/*
Copyright 2025 The Kubernetes Authors.

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

package namespace

import (
	"context"
	"fmt"
	"sync"
	"time"

	cnstypes "github.com/vmware/govmomi/cns/types"
	v1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/runtime"
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
	csifault "sigs.k8s.io/vsphere-csi-driver/v3/pkg/common/fault"
	"sigs.k8s.io/vsphere-csi-driver/v3/pkg/syncer"

	snapv1 "github.com/kubernetes-csi/external-snapshotter/client/v8/apis/volumesnapshot/v1"
	cnsoperatorapis "sigs.k8s.io/vsphere-csi-driver/v3/pkg/apis/cnsoperator"
	"sigs.k8s.io/vsphere-csi-driver/v3/pkg/csi/service/logger"
	k8s "sigs.k8s.io/vsphere-csi-driver/v3/pkg/kubernetes"
	cnsoperatortypes "sigs.k8s.io/vsphere-csi-driver/v3/pkg/syncer/cnsoperator/types"
)

const (
	maxWorkerThreads = 10
)

// backOffDuration is a map of namespace name's to the time after
// which a request for this instance will be requeued.
// Initialized to 1 second for new instances and for instances whose latest
// reconcile operation succeeded.
// If the reconcile fails, backoff is incremented exponentially.
var (
	backOffDuration         map[string]time.Duration
	backOffDurationMapMutex = sync.Mutex{}
)

// Add creates a new namespace Controller and adds it to the Manager,
// The Manager will set fields on the Controller and Start it when the Manager is Started.
func Add(mgr manager.Manager, clusterFlavor cnstypes.CnsClusterFlavor) error {
	ctx, log := logger.GetNewContextWithLogger()
	if clusterFlavor != cnstypes.CnsClusterFlavorWorkload {
		log.Debug("Not initializing the K8s Controller as its a non-WCP CSI deployment")
		return nil
	}
	// Initializes kubernetes client.
	k8sclient, err := k8s.NewClient(ctx)
	if err != nil {
		log.Errorf("Creating Kubernetes client failed. Err: %v", err)
		return err
	}
	// eventBroadcaster broadcasts events on namespace instances to
	// the event sink.
	eventBroadcaster := record.NewBroadcaster()
	eventBroadcaster.StartRecordingToSink(
		&typedcorev1.EventSinkImpl{
			Interface: k8sclient.CoreV1().Events(""),
		},
	)

	recorder := eventBroadcaster.NewRecorder(scheme.Scheme, v1.EventSource{Component: cnsoperatorapis.GroupName})
	return add(mgr, newReconciler(mgr, recorder))
}

// newReconciler returns a new reconcile.Reconciler
func newReconciler(mgr manager.Manager, recorder record.EventRecorder) reconcile.Reconciler {
	return &ReconcileNamespace{client: mgr.GetClient(), scheme: mgr.GetScheme(),
		recorder: recorder}
}

// add adds a new Controller to mgr with r as the reconcile.Reconciler.
func add(mgr manager.Manager, r reconcile.Reconciler) error {
	_, log := logger.GetNewContextWithLogger()
	// Create a new controller.
	c, err := controller.New("namespace-controller", mgr,
		controller.Options{Reconciler: r, MaxConcurrentReconciles: maxWorkerThreads})
	if err != nil {
		log.Errorf("failed to create new Namespace controller with error: %+v", err)
		return err
	}

	backOffDuration = make(map[string]time.Duration)
	// Watch for changes to primary resource namespace.
	pred := predicate.TypedFuncs[*v1.Namespace]{
		CreateFunc: func(e event.TypedCreateEvent[*v1.Namespace]) bool {
			log.Info("Ignoring Namespace reconciliation on create event")
			return false
		},
		UpdateFunc: func(e event.TypedUpdateEvent[*v1.Namespace]) bool {
			if e.ObjectOld.DeletionTimestamp == nil &&
				e.ObjectNew.DeletionTimestamp != nil {
				return true
			}
			return false
		},
		DeleteFunc: func(e event.TypedDeleteEvent[*v1.Namespace]) bool {
			log.Info("Ignoring Namespace reconciliation on delete event")
			return false
		},
	}
	err = c.Watch(source.Kind(
		mgr.GetCache(),
		&v1.Namespace{},
		&handler.TypedEnqueueRequestForObject[*v1.Namespace]{}, pred))
	if err != nil {
		log.Errorf("failed to watch for changes to Namespace resource with error: %+v", err)
		return err
	}
	return nil
}

// blank assignment to verify that ReconcileNamespace implements
// reconcile.Reconciler.
var _ reconcile.Reconciler = &ReconcileNamespace{}

// ReconcileNamespace reconciles a Namespace object.
type ReconcileNamespace struct {
	// This client, initialized using mgr.Client() above, is a split client
	// that reads objects from the cache and writes to the apiserver
	client   client.Client
	scheme   *runtime.Scheme
	recorder record.EventRecorder
}

// Reconcile reads that state of the cluster for a Namespace object
// and makes changes based on the state read and what is in the
// Namespace.Spec.
// Note:
// The Controller will requeue the Request to be processed again if the returned
// error is non-nil or Result.Requeue is true. Otherwise, upon completion it
// will remove the work from the queue.
func (r *ReconcileNamespace) Reconcile(ctx context.Context,
	request reconcile.Request) (reconcile.Result, error) {
	ctx = logger.NewContextWithLogger(ctx)
	reconcileLog := logger.GetLogger(ctx)
	reconcileLog.Infof("Received Reconcile for request: %q", request.NamespacedName)
	// Start a goroutine to listen for context cancellation
	go func() {
		<-ctx.Done()
		reconcileLog.Infof("context canceled for reconcile for Namespace request: %q, error: %v",
			request.NamespacedName, ctx.Err())
	}()
	reconcileLog.Infof("Started Reconcile for Namespace request: %q", request.NamespacedName)
	// Fetch the Namespace instance
	instance := &v1.Namespace{}
	err := r.client.Get(ctx, request.NamespacedName, instance)
	if err != nil {
		if apierrors.IsNotFound(err) {
			reconcileLog.Info("Namespace resource not found. Ignoring since object must be deleted.")
			return reconcile.Result{}, nil
		}
		reconcileLog.Errorf("Error reading the Namespace with name: %q. Err: %+v",
			request.Name, err)
		reconcileLog.Errorf("Operation failed, reporting failure status to Prometheus."+
			" Fault Type: %q", csifault.CSIInternalFault)
		// Error reading the object - return with err.
		return reconcile.Result{}, err
	}
	// Initialize backOffDuration for the instance, if required.
	backOffDurationMapMutex.Lock()
	var timeout time.Duration
	if _, exists := backOffDuration[instance.Name]; !exists {
		backOffDuration[instance.Name] = time.Second
	}
	timeout = backOffDuration[instance.Name]
	backOffDurationMapMutex.Unlock()
	reconcileLog.Infof("Reconciling Namespace with Request.Name: %q timeout %q seconds",
		request.Name, timeout)
	// If namespace does not have deletion timestamp, then return
	if instance.DeletionTimestamp.IsZero() {
		return reconcile.Result{}, nil
	}
	// Iterate over all PVCs in deleting namespace and remove the CNS finalizer
	// "cns.vmware.com/pvc-delete-protection" from them, if found
	var pvcList v1.PersistentVolumeClaimList
	err = r.client.List(ctx, &pvcList, &client.ListOptions{Namespace: instance.Name})
	if err != nil {
		msg := fmt.Sprintf("Failed to list PVCs in namespace: %q. Err: %+v",
			instance.Name, err)
		recordEvent(ctx, r, instance, v1.EventTypeWarning, msg)
		reconcileLog.Errorf("Operation failed, reporting failure status to Prometheus."+
			" Fault Type: %q", csifault.CSIApiServerOperationFault)
		return reconcile.Result{RequeueAfter: timeout}, nil
	}
	k8sClient, err := k8s.NewClient(ctx)
	if err != nil {
		reconcileLog.Errorf("Failed to get kubernetes client. Err: %+v", err)
		reconcileLog.Errorf("Operation failed, reporting failure status to Prometheus."+
			" Fault Type: %q", csifault.CSIInternalFault)
		return reconcile.Result{}, err
	}
	for _, pvc := range pvcList.Items {
		reconcileLog.Infof("Processing pvc %s in namespace %q timeout %q seconds",
			pvc.Name, pvc.Namespace, timeout)
		syncer.RemoveCNSFinalizerFromPVCIfTKGClusterDeleted(ctx, k8sClient, &pvc,
			cnsoperatortypes.CNSVolumeFinalizer, true)
	}
	// Iterate over all VolumeSnapshots in deleting namespace and remove the CNS finalizer
	// "cns.vmware.com/volumesnapshot-protection" from them, if found
	var vsList snapv1.VolumeSnapshotList
	err = r.client.List(ctx, &vsList, &client.ListOptions{Namespace: instance.Name})
	if err != nil {
		msg := fmt.Sprintf("Failed to list VolumeSnapshots in namespace: %q. Err: %+v",
			instance.Name, err)
		recordEvent(ctx, r, instance, v1.EventTypeWarning, msg)
		reconcileLog.Errorf("Operation failed, reporting failure status to Prometheus."+
			" Fault Type: %q", csifault.CSIApiServerOperationFault)
		return reconcile.Result{RequeueAfter: timeout}, nil
	}
	snapshotterClient, err := k8s.NewSnapshotterClient(ctx)
	if err != nil {
		reconcileLog.Errorf("Failed to get snapshotterClient. Err: %v", err)
		reconcileLog.Errorf("Operation failed, reporting failure status to Prometheus."+
			" Fault Type: %q", csifault.CSIInternalFault)
		return reconcile.Result{}, err
	}
	for _, vs := range vsList.Items {
		reconcileLog.Infof("Processing snapshot %s in namespace %q timeout %q seconds",
			vs.Name, vs.Namespace, timeout)
		syncer.RemoveCNSFinalizerFromSnapIfTKGClusterDeleted(ctx, snapshotterClient, &vs,
			cnsoperatortypes.CNSSnapshotFinalizer, true)
	}
	// Cleanup instance entry from backOffDuration map.
	backOffDurationMapMutex.Lock()
	delete(backOffDuration, instance.Name)
	backOffDurationMapMutex.Unlock()
	reconcileLog.Infof("Finished Reconcile for Namespace request: %q", request.NamespacedName)
	return reconcile.Result{}, nil
}

// recordEvent records the event, sets the backOffDuration for the instance
// appropriately and logs the message.
// backOffDuration is reset to 1 second on success and doubled on failure.
func recordEvent(ctx context.Context, r *ReconcileNamespace,
	instance *v1.Namespace, eventtype string, msg string) {
	log := logger.GetLogger(ctx)
	switch eventtype {
	case v1.EventTypeWarning:
		// Double backOff duration.
		backOffDurationMapMutex.Lock()
		backOffDuration[instance.Name] = backOffDuration[instance.Name] * 2
		backOffDurationMapMutex.Unlock()
		r.recorder.Event(instance, v1.EventTypeWarning, "NamespaceDeleteNotHandled", msg)
		log.Error(msg)
	case v1.EventTypeNormal:
		// Reset backOff duration to one second.
		backOffDurationMapMutex.Lock()
		backOffDuration[instance.Name] = time.Second
		backOffDurationMapMutex.Unlock()
		r.recorder.Event(instance, v1.EventTypeNormal, "NamespaceDeleteHandled", msg)
		log.Info(msg)
	}
}
