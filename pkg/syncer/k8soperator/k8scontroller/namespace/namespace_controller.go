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
	"slices"
	"strings"
	"sync"
	"time"

	cnstypes "github.com/vmware/govmomi/cns/types"
	v1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	clientset "k8s.io/client-go/kubernetes"
	"k8s.io/client-go/kubernetes/scheme"
	typedcorev1 "k8s.io/client-go/kubernetes/typed/core/v1"
	"k8s.io/client-go/metadata"
	"k8s.io/client-go/tools/record"
	ccV1beta2 "sigs.k8s.io/cluster-api/api/core/v1beta2"
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
	snapshotterclient "github.com/kubernetes-csi/external-snapshotter/client/v8/clientset/versioned"
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
	// Deleting a guest cluster does not change its namespace, so also watch guest clusters (a small number of
	// objects) and reconcile the namespace of a cluster that is deleting or deleted. Skipped if the Cluster CRD
	// (VKS) is not installed.
	if _, err := mgr.GetRESTMapper().RESTMapping(ccV1beta2.GroupVersion.WithKind("Cluster").GroupKind(),
		ccV1beta2.GroupVersion.Version); err != nil {
		if meta.IsNoMatchError(err) {
			log.Infof("Cluster CRD is not installed, not watching guest cluster deletions")
			return nil
		}
		return err
	}
	err = c.Watch(source.Kind(
		mgr.GetCache(),
		&ccV1beta2.Cluster{},
		handler.TypedEnqueueRequestsFromMapFunc(func(_ context.Context, cc *ccV1beta2.Cluster) []reconcile.Request {
			return []reconcile.Request{{NamespacedName: types.NamespacedName{Name: cc.Namespace}}}
		}),
		predicate.TypedFuncs[*ccV1beta2.Cluster]{
			CreateFunc: func(e event.TypedCreateEvent[*ccV1beta2.Cluster]) bool {
				return isClusterDeleting(e.Object)
			},
			UpdateFunc: func(e event.TypedUpdateEvent[*ccV1beta2.Cluster]) bool {
				return isClusterDeleting(e.ObjectNew)
			},
			DeleteFunc: func(event.TypedDeleteEvent[*ccV1beta2.Cluster]) bool { return true },
		}))
	if err != nil {
		log.Errorf("failed to watch for changes to Cluster resource with error: %+v", err)
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
	// If namespace does not have deletion timestamp, only clean up objects of guest clusters being deleted
	if instance.DeletionTimestamp.IsZero() {
		r.cleanupObjectsOfDeletingClusters(ctx, instance.Name)
		return reconcile.Result{}, nil
	}
	// Remove the CNS finalizers "cns.vmware.com/pvc-delete-protection" and
	// "cns.vmware.com/volumesnapshot-protection" from all PVCs and VolumeSnapshots in deleting namespace
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
	k8sClient, err := k8s.NewClient(ctx)
	if err != nil {
		reconcileLog.Errorf("Failed to get kubernetes client. Err: %+v", err)
		reconcileLog.Errorf("Operation failed, reporting failure status to Prometheus."+
			" Fault Type: %q", csifault.CSIInternalFault)
		return reconcile.Result{}, err
	}
	snapshotterClient, err := k8s.NewSnapshotterClient(ctx)
	if err != nil {
		reconcileLog.Errorf("Failed to get snapshotterClient. Err: %v", err)
		reconcileLog.Errorf("Operation failed, reporting failure status to Prometheus."+
			" Fault Type: %q", csifault.CSIInternalFault)
		return reconcile.Result{}, err
	}
	removeCNSFinalizers(ctx, k8sClient, snapshotterClient, pvcList.Items, vsList.Items, true)
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

// isClusterDeleting returns true if the guest cluster is being deleted.
func isClusterDeleting(cc *ccV1beta2.Cluster) bool {
	return !cc.DeletionTimestamp.IsZero() || cc.Status.GetTypedPhase() == ccV1beta2.ClusterPhaseDeleting
}

// ownedByGoneCluster returns true if labels carry a "<cluster name>/TKGService" label and that guest cluster
// is being deleted (clusters[name] is true) or no longer exists in the namespace.
func ownedByGoneCluster(labels map[string]string, clusters map[string]bool) bool {
	for key := range labels {
		if name, ok := strings.CutSuffix(key, "/TKGService"); ok {
			if deleting, exists := clusters[name]; !exists || deleting {
				return true
			}
		}
	}
	return false
}

var (
	pvcResource = v1.SchemeGroupVersion.WithResource("persistentvolumeclaims")
	vsResource  = snapv1.SchemeGroupVersion.WithResource("volumesnapshots")
)

// terminatingObjects returns the objects of resource in namespace that are marked for deletion, still have
// finalizer, and were created from a guest cluster that is deleting or gone. Only object metadata is listed,
// a page at a time, since a namespace may hold a very large number of PVCs and VolumeSnapshots. The full object
// is then fetched, with get, for each match.
func terminatingObjects[T any](ctx context.Context, mdClient metadata.Interface, resource schema.GroupVersionResource,
	namespace, finalizer string, clusters map[string]bool,
	get func(ctx context.Context, name string) (*T, error)) ([]T, error) {
	var objects []T
	opts := metav1.ListOptions{Limit: 500}
	for {
		list, err := mdClient.Resource(resource).Namespace(namespace).List(ctx, opts)
		if err != nil {
			return nil, err
		}
		for _, o := range list.Items {
			if o.DeletionTimestamp == nil || !slices.Contains(o.Finalizers, finalizer) ||
				!ownedByGoneCluster(o.Labels, clusters) {
				continue
			}
			obj, err := get(ctx, o.Name)
			if apierrors.IsNotFound(err) {
				continue
			}
			if err != nil {
				return nil, err
			}
			objects = append(objects, *obj)
		}
		if list.Continue == "" {
			return objects, nil
		}
		opts.Continue = list.Continue
	}
}

// cleanupObjectsOfDeletingClusters removes the CNS finalizers from PVCs and VolumeSnapshots in namespace
// that are marked for deletion and were created from a guest cluster that is being deleted or is gone.
// Objects not marked for deletion are left untouched so that volumes retained after cluster deletion
// stay protected, and so are objects of guest clusters that are running. It is a no-op if there are none.
func (r *ReconcileNamespace) cleanupObjectsOfDeletingClusters(ctx context.Context, namespace string) {
	log := logger.GetLogger(ctx)
	var clusterList ccV1beta2.ClusterList
	if err := r.client.List(ctx, &clusterList, client.InNamespace(namespace)); err != nil {
		log.Errorf("Failed to list guest clusters in namespace %q. Err: %+v", namespace, err)
		return
	}
	clusters := map[string]bool{}
	for i := range clusterList.Items {
		clusters[clusterList.Items[i].Name] = isClusterDeleting(&clusterList.Items[i])
	}
	restClientConfig, err := k8s.GetKubeConfig(ctx)
	if err != nil {
		log.Errorf("Failed to initialize rest clientconfig. Err: %+v", err)
		return
	}
	mdClient, err := metadata.NewForConfig(restClientConfig)
	if err != nil {
		log.Errorf("Failed to get metadata client. Err: %+v", err)
		return
	}
	k8sClient, err := k8s.NewClient(ctx)
	if err != nil {
		log.Errorf("Failed to get kubernetes client. Err: %+v", err)
		return
	}
	snapshotterClient, err := k8s.NewSnapshotterClient(ctx)
	if err != nil {
		log.Errorf("Failed to get snapshotterClient. Err: %v", err)
		return
	}
	pvcs, err := terminatingObjects(ctx, mdClient, pvcResource, namespace, cnsoperatortypes.CNSVolumeFinalizer,
		clusters, func(ctx context.Context, name string) (*v1.PersistentVolumeClaim, error) {
			return k8sClient.CoreV1().PersistentVolumeClaims(namespace).Get(ctx, name, metav1.GetOptions{})
		})
	if err != nil {
		log.Errorf("Failed to get terminating PVCs in namespace: %q. Err: %+v", namespace, err)
		return
	}
	vss, err := terminatingObjects(ctx, mdClient, vsResource, namespace, cnsoperatortypes.CNSSnapshotFinalizer,
		clusters, func(ctx context.Context, name string) (*snapv1.VolumeSnapshot, error) {
			return snapshotterClient.SnapshotV1().VolumeSnapshots(namespace).Get(ctx, name, metav1.GetOptions{})
		})
	if err != nil {
		log.Errorf("Failed to get terminating VolumeSnapshots in namespace: %q. Err: %+v", namespace, err)
		return
	}
	log.Infof("Found %d terminating PVCs and %d terminating VolumeSnapshots of deleting or deleted guest clusters "+
		"in namespace %q", len(pvcs), len(vss), namespace)
	removeCNSFinalizers(ctx, k8sClient, snapshotterClient, pvcs, vss, false)
}

// removeCNSFinalizers removes the CNS finalizers from the given PVCs and VolumeSnapshots. If
// isNamespaceBeingDeleted is false, a finalizer is only removed if the guest cluster that created the
// object is deleted or being deleted.
func removeCNSFinalizers(ctx context.Context, k8sClient clientset.Interface,
	snapshotterClient snapshotterclient.Interface, pvcs []v1.PersistentVolumeClaim,
	vss []snapv1.VolumeSnapshot, isNamespaceBeingDeleted bool) {
	log := logger.GetLogger(ctx)
	for i := range pvcs {
		log.Infof("Processing pvc %s in namespace %q", pvcs[i].Name, pvcs[i].Namespace)
		syncer.RemoveCNSFinalizerFromPVCIfTKGClusterDeleted(ctx, k8sClient, &pvcs[i],
			cnsoperatortypes.CNSVolumeFinalizer, isNamespaceBeingDeleted)
	}
	for i := range vss {
		log.Infof("Processing snapshot %s in namespace %q", vss[i].Name, vss[i].Namespace)
		syncer.RemoveCNSFinalizerFromSnapIfTKGClusterDeleted(ctx, snapshotterClient, &vss[i],
			cnsoperatortypes.CNSSnapshotFinalizer, isNamespaceBeingDeleted)
	}
}
