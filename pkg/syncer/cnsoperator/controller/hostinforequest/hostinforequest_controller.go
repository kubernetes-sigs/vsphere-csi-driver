/*
Copyright 2026 The Kubernetes Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package hostinforequest

import (
	"context"
	"fmt"
	"sync"
	"time"

	cnstypes "github.com/vmware/govmomi/cns/types"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/kubernetes/scheme"
	typedcorev1 "k8s.io/client-go/kubernetes/typed/core/v1"
	"k8s.io/client-go/tools/record"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller"
	"sigs.k8s.io/controller-runtime/pkg/event"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	"sigs.k8s.io/controller-runtime/pkg/manager"
	"sigs.k8s.io/controller-runtime/pkg/predicate"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
	"sigs.k8s.io/controller-runtime/pkg/source"

	volumes "sigs.k8s.io/vsphere-csi-driver/v3/pkg/common/cns-lib/volume"
	cnsconfig "sigs.k8s.io/vsphere-csi-driver/v3/pkg/common/config"
	"sigs.k8s.io/vsphere-csi-driver/v3/pkg/csi/service/common"
	"sigs.k8s.io/vsphere-csi-driver/v3/pkg/csi/service/common/commonco"
	"sigs.k8s.io/vsphere-csi-driver/v3/pkg/csi/service/logger"
	hostinforequestv1alpha1 "sigs.k8s.io/vsphere-csi-driver/v3/pkg/internalapis/hostinforequest/v1alpha1"
	k8s "sigs.k8s.io/vsphere-csi-driver/v3/pkg/kubernetes"
	"sigs.k8s.io/vsphere-csi-driver/v3/pkg/syncer"
	cnsoperatortypes "sigs.k8s.io/vsphere-csi-driver/v3/pkg/syncer/cnsoperator/types"
)

const defaultMaxWorkerThreads = 1

// resolveESXiHostname is a package-level indirection over
// getESXiHostnameFromNodeList so unit tests can substitute a fake resolver
// without needing a real Kubernetes clientset.
var resolveESXiHostname = getESXiHostnameFromNodeList

// backOffDuration is a map of HostInfoRequest instance name to the time
// after which a request for this instance will be requeued. Initialized to
// 1 second for new instances and for instances whose latest reconcile
// operation succeeded. If the reconcile fails, backoff is incremented
// exponentially.
var (
	backOffDuration         map[types.NamespacedName]time.Duration
	backOffDurationMapMutex = sync.Mutex{}
)

// Add creates a new HostInfoRequest Controller and adds it to the Manager.
// This controller only runs on the Supervisor (Workload flavor), since it
// needs unscoped access to Supervisor Node objects that a guest cluster's
// namespace-scoped ProviderServiceAccount cannot be granted (Nodes are
// cluster-scoped).
func Add(mgr manager.Manager, clusterFlavor cnstypes.CnsClusterFlavor,
	configInfo *cnsconfig.ConfigurationInfo, volumeManager volumes.Manager) error {
	ctx, log := logger.GetNewContextWithLogger()
	if clusterFlavor != cnstypes.CnsClusterFlavorWorkload {
		log.Debug("Not initializing the HostInfoRequest Controller as it is not a Supervisor CSI deployment")
		return nil
	}

	coCommonInterface, err := commonco.GetContainerOrchestratorInterface(ctx,
		common.Kubernetes, clusterFlavor, &syncer.COInitParams)
	if err != nil {
		log.Errorf("failed to create CO agnostic interface. Err: %v", err)
		return err
	}

	if !coCommonInterface.IsFSSEnabled(ctx, common.HostLocalStorageSupport) {
		log.Infof("Not initializing the HostInfoRequest Controller as %s capability is disabled",
			common.HostLocalStorageSupport)
		return nil
	}

	// A plain Supervisor-local clientset - unlike a guest cluster's
	// ProviderServiceAccount, Syncer's own identity is not scoped to a
	// single namespace, so it can list cluster-scoped Node objects.
	k8sclient, err := k8s.NewClient(ctx)
	if err != nil {
		log.Errorf("creating Kubernetes client failed. Err: %v", err)
		return err
	}

	eventBroadcaster := record.NewBroadcaster()
	eventBroadcaster.StartRecordingToSink(
		&typedcorev1.EventSinkImpl{
			Interface: k8sclient.CoreV1().Events(""),
		},
	)
	recorder := eventBroadcaster.NewRecorder(scheme.Scheme,
		corev1.EventSource{Component: hostinforequestv1alpha1.GroupName})
	return add(mgr, newReconciler(mgr, recorder, k8sclient))
}

// newReconciler returns a new `reconcile.Reconciler`.
func newReconciler(mgr manager.Manager, recorder record.EventRecorder,
	k8sClientset kubernetes.Interface) reconcile.Reconciler {
	return &ReconcileHostInfoRequest{
		client:       mgr.GetClient(),
		scheme:       mgr.GetScheme(),
		recorder:     recorder,
		k8sClientset: k8sClientset,
	}
}

// add adds a new Controller to mgr with r as the `reconcile.Reconciler`.
func add(mgr manager.Manager, r reconcile.Reconciler) error {
	log := logger.GetLoggerWithNoContext()

	c, err := controller.New("hostinforequest-controller", mgr, controller.Options{Reconciler: r,
		MaxConcurrentReconciles: defaultMaxWorkerThreads})
	if err != nil {
		log.Errorf("failed to create new HostInfoRequest controller with error: %+v", err)
		return err
	}

	backOffDuration = make(map[types.NamespacedName]time.Duration)

	pred := predicate.TypedFuncs[*hostinforequestv1alpha1.HostInfoRequest]{
		CreateFunc: func(e event.TypedCreateEvent[*hostinforequestv1alpha1.HostInfoRequest]) bool {
			return true
		},
		UpdateFunc: func(e event.TypedUpdateEvent[*hostinforequestv1alpha1.HostInfoRequest]) bool {
			// Skip re-reconciling once successfully resolved - the Node's IP
			// does not change for the lifetime of the guest node. Errors are
			// not final (e.g. the guest node may not have joined Supervisor
			// as a Node yet), so keep retrying via the Reconcile loop's own
			// backoff/RequeueAfter rather than stopping here.
			return e.ObjectNew.Status.Status != hostinforequestv1alpha1.HostInfoRequestSuccess
		},
		DeleteFunc: func(e event.TypedDeleteEvent[*hostinforequestv1alpha1.HostInfoRequest]) bool {
			log.Debug("Ignoring HostInfoRequest reconciliation on delete event")
			return false
		},
	}

	err = c.Watch(source.Kind(mgr.GetCache(),
		&hostinforequestv1alpha1.HostInfoRequest{},
		&handler.TypedEnqueueRequestForObject[*hostinforequestv1alpha1.HostInfoRequest]{}, pred))
	if err != nil {
		log.Errorf("Failed to watch for changes to HostInfoRequest resource with error: %+v", err)
		return err
	}
	log.Info("Started watching on HostInfoRequest resources")
	return nil
}

// blank assignment to verify that ReconcileHostInfoRequest implements
// `reconcile.Reconciler`.
var _ reconcile.Reconciler = &ReconcileHostInfoRequest{}

// ReconcileHostInfoRequest reconciles a HostInfoRequest object.
type ReconcileHostInfoRequest struct {
	client       client.Client
	scheme       *runtime.Scheme
	recorder     record.EventRecorder
	k8sClientset kubernetes.Interface
}

// Reconcile resolves a HostInfoRequest's Spec.NodeIP to a Supervisor Node's
// hostname/moid by listing Node objects directly, and writes the result to
// Status.
func (r *ReconcileHostInfoRequest) Reconcile(ctx context.Context, request reconcile.Request) (
	reconcile.Result, error) {
	log := logger.GetLogger(ctx)
	log.Infof("Reconciling HostInfoRequest %q", request.NamespacedName)

	instance := &hostinforequestv1alpha1.HostInfoRequest{}
	if err := r.client.Get(ctx, request.NamespacedName, instance); err != nil {
		if apierrors.IsNotFound(err) {
			log.Infof("HostInfoRequest %q not found. Ignoring since object must have been deleted.",
				request.NamespacedName)
			return reconcile.Result{}, nil
		}
		log.Errorf("Failed to fetch HostInfoRequest %q. Error: %+v", request.NamespacedName, err)
		return reconcile.Result{}, err
	}

	if instance.Status.Status == hostinforequestv1alpha1.HostInfoRequestSuccess {
		// Already resolved - a guest node's IP does not change for its
		// lifetime, so there is nothing further to reconcile.
		return reconcile.Result{}, nil
	}

	backOffDurationMapMutex.Lock()
	if _, exists := backOffDuration[request.NamespacedName]; !exists {
		backOffDuration[request.NamespacedName] = time.Second
	}
	timeout := backOffDuration[request.NamespacedName]
	backOffDurationMapMutex.Unlock()

	hostname, esxiHostMoid, err := resolveESXiHostname(ctx, r.k8sClientset, instance.Spec.NodeIP)
	if err != nil {
		msg := fmt.Sprintf("failed to resolve ESXi host for node IP %q. Error: %v", instance.Spec.NodeIP, err)
		log.Error(msg)
		if updateErr := r.updateStatus(ctx, instance, hostinforequestv1alpha1.HostInfoRequestError,
			"", "", msg); updateErr != nil {
			log.Errorf("Failed to update HostInfoRequest %q status. Error: %+v", request.NamespacedName, updateErr)
		}
		backOffDurationMapMutex.Lock()
		backOffDuration[request.NamespacedName] = min(backOffDuration[request.NamespacedName]*2,
			cnsoperatortypes.MaxBackOffDurationForReconciler)
		backOffDurationMapMutex.Unlock()
		return reconcile.Result{RequeueAfter: timeout}, nil
	}

	if err := r.updateStatus(ctx, instance, hostinforequestv1alpha1.HostInfoRequestSuccess,
		hostname, esxiHostMoid, ""); err != nil {
		log.Errorf("Failed to update HostInfoRequest %q status. Error: %+v", request.NamespacedName, err)
		return reconcile.Result{RequeueAfter: timeout}, nil
	}

	backOffDurationMapMutex.Lock()
	delete(backOffDuration, request.NamespacedName)
	backOffDurationMapMutex.Unlock()

	log.Infof("Successfully resolved HostInfoRequest %q: hostname=%q esxiHostMoid=%q",
		request.NamespacedName, hostname, esxiHostMoid)
	return reconcile.Result{}, nil
}

func (r *ReconcileHostInfoRequest) updateStatus(ctx context.Context, instance *hostinforequestv1alpha1.HostInfoRequest,
	status hostinforequestv1alpha1.CRDStatus, hostname string, esxiHostMoid string, errorMessage string) error {
	instance.Status.Status = status
	instance.Status.Hostname = hostname
	instance.Status.EsxiHostMoid = esxiHostMoid
	instance.Status.ErrorMessage = errorMessage
	switch status {
	case hostinforequestv1alpha1.HostInfoRequestSuccess:
		r.recorder.Event(instance, corev1.EventTypeNormal, "HostInfoResolutionSucceeded",
			fmt.Sprintf("Resolved node IP %q to hostname %q", instance.Spec.NodeIP, hostname))
	case hostinforequestv1alpha1.HostInfoRequestError:
		r.recorder.Event(instance, corev1.EventTypeWarning, "HostInfoResolutionFailed", errorMessage)
	}
	return r.client.Status().Update(ctx, instance)
}

// getESXiHostnameFromNodeList resolves nodeIP to the Supervisor Node's name
// and ESXi host moid by listing Supervisor Node objects directly and
// matching on status.addresses[type=InternalIP].
func getESXiHostnameFromNodeList(ctx context.Context, k8sClientset kubernetes.Interface,
	nodeIP string) (hostname string, esxiHostMoid string, err error) {
	log := logger.GetLogger(ctx)
	nodeList, err := k8sClientset.CoreV1().Nodes().List(ctx, metav1.ListOptions{})
	if err != nil {
		return "", "", fmt.Errorf("failed to list Supervisor Nodes: %w", err)
	}
	log.Infof("getESXiHostnameFromNodeList: listed %d Supervisor Nodes while resolving InternalIP %q",
		len(nodeList.Items), nodeIP)
	for _, supervisorNode := range nodeList.Items {
		for _, addr := range supervisorNode.Status.Addresses {
			if addr.Type == corev1.NodeInternalIP && addr.Address == nodeIP {
				return supervisorNode.Name, supervisorNode.Annotations[common.HostMoidAnnotationKey], nil
			}
		}
	}
	log.Warnf("getESXiHostnameFromNodeList: no Supervisor Node found with InternalIP %q", nodeIP)
	return "", "", fmt.Errorf("no Supervisor Node found with InternalIP %q", nodeIP)
}
