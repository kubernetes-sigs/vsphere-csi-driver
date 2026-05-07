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

package clusterstoragepolicyinfo

import (
	"context"
	"sync"
	"time"

	cnstypes "github.com/vmware/govmomi/cns/types"
	v1 "k8s.io/api/core/v1"
	storagev1 "k8s.io/api/storage/v1"
	"k8s.io/apimachinery/pkg/api/equality"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	apitypes "k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes/scheme"
	typedcorev1 "k8s.io/client-go/kubernetes/typed/core/v1"
	"k8s.io/client-go/tools/record"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/builder"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller"
	"sigs.k8s.io/controller-runtime/pkg/event"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	"sigs.k8s.io/controller-runtime/pkg/manager"
	"sigs.k8s.io/controller-runtime/pkg/predicate"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
	apis "sigs.k8s.io/vsphere-csi-driver/v3/pkg/apis/cnsoperator"
	clusterspiv1alpha1 "sigs.k8s.io/vsphere-csi-driver/v3/pkg/apis/cnsoperator/clusterstoragepolicyinfo/v1alpha1"
	volumes "sigs.k8s.io/vsphere-csi-driver/v3/pkg/common/cns-lib/volume"
	"sigs.k8s.io/vsphere-csi-driver/v3/pkg/common/config"
	"sigs.k8s.io/vsphere-csi-driver/v3/pkg/csi/service/common"
	"sigs.k8s.io/vsphere-csi-driver/v3/pkg/csi/service/common/commonco"
	"sigs.k8s.io/vsphere-csi-driver/v3/pkg/csi/service/logger"
	k8s "sigs.k8s.io/vsphere-csi-driver/v3/pkg/kubernetes"
	"sigs.k8s.io/vsphere-csi-driver/v3/pkg/syncer/cnsoperator/types"
	"sigs.k8s.io/vsphere-csi-driver/v3/pkg/syncer/cnsoperator/util"
)

// backOffDuration is a map of ClusterStoragePolicyInfo names to the time after
// which a request for this instance will be requeued.
// Initialized to 1 second for new instances and for instances whose latest
// reconcile operation succeeded.
// If the reconcile fails, backoff is incremented exponentially.
var (
	backOffDuration         map[apitypes.NamespacedName]time.Duration
	backOffDurationMapMutex = sync.Mutex{}
)

const (
	workerThreadsEnvVar     = "WORKER_THREADS_CLUSTER_STORAGE_POLICY_INFO"
	defaultMaxWorkerThreads = 4
)

// Add registers the ClusterStoragePolicyInfo controller with the Manager (WCP / Workload only).
func Add(mgr manager.Manager, clusterFlavor cnstypes.CnsClusterFlavor,
	configInfo *config.ConfigurationInfo, _ volumes.Manager) error {
	ctx, log := logger.GetNewContextWithLogger()
	if clusterFlavor != cnstypes.CnsClusterFlavorWorkload {
		log.Debug("Not initializing the ClusterStoragePolicyInfo Controller: unsupported cluster flavor")
		return nil
	}
	if !commonco.ContainerOrchestratorUtility.IsFSSEnabled(ctx, common.SupportsExposingStoragePolicyAttributes) {
		log.Infof("Not initializing the ClusterStoragePolicyInfo Controller: capability %q is not activated",
			common.SupportsExposingStoragePolicyAttributes)
		return nil
	}

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
	recorder := eventBroadcaster.NewRecorder(scheme.Scheme, v1.EventSource{Component: apis.GroupName})
	return add(mgr, newReconciler(mgr, configInfo, recorder))
}

func newReconciler(mgr manager.Manager, configInfo *config.ConfigurationInfo,
	recorder record.EventRecorder) *ReconcileClusterStoragePolicyInfo {
	return &ReconcileClusterStoragePolicyInfo{
		client:     mgr.GetClient(),
		scheme:     mgr.GetScheme(),
		configInfo: configInfo,
		recorder:   recorder,
		mgr:        mgr,
	}
}

func add(mgr manager.Manager, r *ReconcileClusterStoragePolicyInfo) error {
	ctx, log := logger.GetNewContextWithLogger()
	maxWorkerThreads := util.GetMaxWorkerThreads(ctx, workerThreadsEnvVar, defaultMaxWorkerThreads)
	scVacPredicates := predicate.Funcs{
		CreateFunc: func(e event.CreateEvent) bool {
			return e.Object != nil
		},
		UpdateFunc: func(e event.UpdateEvent) bool {
			return false
		},
		DeleteFunc: func(_ event.DeleteEvent) bool {
			return false
		},
	}

	blder := ctrl.NewControllerManagedBy(mgr).Named("clusterstoragepolicyinfo-controller").
		For(&clusterspiv1alpha1.ClusterStoragePolicyInfo{},
			builder.WithPredicates(predicate.GenerationChangedPredicate{})).
		Watches(
			&storagev1.StorageClass{},
			handler.EnqueueRequestsFromMapFunc(r.mapObjectToClusterSPI),
			builder.WithPredicates(scVacPredicates),
		)

	// VolumeAttributesClass API is supported from K8s version 1.34 onwards.
	// Add watch for VolumeAttributesClass only if API is available.
	vacSupported, vacErr := volumeAttributesClassAPIAvailable(mgr)
	if vacErr != nil {
		log.Warnf("Could not discover VolumeAttributesClass API; skipping VAC watch. Err: %v", vacErr)
	} else if !vacSupported {
		log.Infof("VolumeAttributesClass API not registered on this cluster; skipping VAC watch")
	} else {
		log.Infof("VolumeAttributesClass API available; registering VAC watch")
		blder = blder.Watches(
			&storagev1.VolumeAttributesClass{},
			handler.EnqueueRequestsFromMapFunc(r.mapObjectToClusterSPI),
			builder.WithPredicates(scVacPredicates),
		)
	}

	err := blder.WithOptions(controller.Options{MaxConcurrentReconciles: maxWorkerThreads}).
		Complete(r)
	if err != nil {
		log.Errorf("failed to build clusterstoragepolicyinfo controller. Err: %v", err)
		return err
	}

	// Initialize the backoff duration map
	backOffDuration = make(map[apitypes.NamespacedName]time.Duration)
	return nil
}

// mapObjectToClusterSPI maps any client.Object to a ClusterStoragePolicyInfo reconcile request.
// Used for both StorageClass and VolumeAttributesClass watches.
func (r *ReconcileClusterStoragePolicyInfo) mapObjectToClusterSPI(ctx context.Context,
	obj client.Object) []reconcile.Request {
	if obj == nil {
		return nil
	}

	// If this is a StorageClass, skip WFFC StorageClasses
	if sc, ok := obj.(*storagev1.StorageClass); ok {
		if storageClassIsWaitForFirstConsumer(sc) {
			return nil
		}
	}

	return []reconcile.Request{{NamespacedName: apitypes.NamespacedName{Name: obj.GetName()}}}
}

var _ reconcile.Reconciler = &ReconcileClusterStoragePolicyInfo{}

// ReconcileClusterStoragePolicyInfo reconciles ClusterStoragePolicyInfo objects.
type ReconcileClusterStoragePolicyInfo struct {
	client     client.Client
	scheme     *runtime.Scheme
	configInfo *config.ConfigurationInfo
	recorder   record.EventRecorder
	mgr        manager.Manager
}

// Reconcile syncs storage policy attributes from the vCenter.
func (r *ReconcileClusterStoragePolicyInfo) Reconcile(ctx context.Context,
	request reconcile.Request) (reconcile.Result, error) {
	ctx = logger.NewContextWithLogger(ctx)
	log := logger.GetLogger(ctx).With("name", request.NamespacedName)

	log.Infof("Reconciling ClusterStoragePolicyInfo")

	// Initialize backOffDuration for the instance, if required.
	backOffDurationMapMutex.Lock()
	var timeout time.Duration
	if _, exists := backOffDuration[request.NamespacedName]; !exists {
		backOffDuration[request.NamespacedName] = time.Second
	}
	timeout = backOffDuration[request.NamespacedName]
	backOffDurationMapMutex.Unlock()

	instance := &clusterspiv1alpha1.ClusterStoragePolicyInfo{}
	err := r.client.Get(ctx, request.NamespacedName, instance)
	if err != nil {
		if !apierrors.IsNotFound(err) {
			log.Errorf("Failed to get ClusterStoragePolicyInfo %q: %v", request.NamespacedName, err)
			return r.completeReconciliationWithError(ctx, request.NamespacedName, timeout, err)
		}
		instance = nil // Instance not found, will be created later
	}

	if instance != nil && instance.DeletionTimestamp != nil {
		log.Infof("Instance %q is being deleted, skipping reconciliation", request.Name)
		return r.completeReconciliationWithSuccess(ctx, request.NamespacedName, timeout)
	}

	// Handle creation and owner reference management
	instance, wasCreated, err := r.ensureClusterSPIExistsInReconcile(ctx, instance, request.Name)
	if err != nil {
		return r.completeReconciliationWithError(ctx, request.NamespacedName, timeout, err)
	}

	// If no instance returned, it means both SC and VAC are gone - nothing to manage.
	// No need to return an error as in the next reconcile, the CR should get a deletionTimestamp
	// which will anyway trigger a deletion event.
	if instance == nil {
		return r.completeReconciliationWithSuccess(ctx, request.NamespacedName, timeout)
	}

	// If a new instance was created, the creation event will trigger another reconcile automatically
	if wasCreated {
		log.Infof("Instance was created, creation event will trigger next reconcile")
		return r.completeReconciliationWithSuccess(ctx, request.NamespacedName, timeout)
	}

	// TODO: Add storage policy attributes sync logic (vCenter / SPBM) using instance.
	return r.completeReconciliationWithSuccess(ctx, request.NamespacedName, timeout)
}

// ensureClusterSPIExistsInReconcile handles ClusterStoragePolicyInfo creation and owner reference
// management in Reconcile.
// Returns (instance, wasCreated, error).
func (r *ReconcileClusterStoragePolicyInfo) ensureClusterSPIExistsInReconcile(ctx context.Context,
	instance *clusterspiv1alpha1.ClusterStoragePolicyInfo, name string) (
	*clusterspiv1alpha1.ClusterStoragePolicyInfo, bool, error) {
	// Check what resources exist
	sc, err := r.checkStorageClassExists(ctx, name)
	if err != nil {
		return nil, false, err
	}

	vac, err := r.checkVolumeAttributesClassExists(ctx, name)
	if err != nil {
		return nil, false, err
	}

	// Get or create the ClusterStoragePolicyInfo with owner references
	instance, wasCreated, err := r.ensureClusterSPIInstance(ctx, instance, name, sc, vac)
	return instance, wasCreated, err
}

// checkStorageClassExists checks if a StorageClass with the given name exists and is valid.
func (r *ReconcileClusterStoragePolicyInfo) checkStorageClassExists(ctx context.Context,
	name string) (*storagev1.StorageClass, error) {
	sc := &storagev1.StorageClass{}

	if err := r.client.Get(ctx, apitypes.NamespacedName{Name: name}, sc); err == nil {
		return sc, nil
	} else if !apierrors.IsNotFound(err) {
		return nil, err
	}

	return nil, nil
}

// checkVolumeAttributesClassExists checks if a VolumeAttributesClass with the given name exists.
func (r *ReconcileClusterStoragePolicyInfo) checkVolumeAttributesClassExists(ctx context.Context,
	name string) (*storagev1.VolumeAttributesClass, error) {
	log := logger.GetLogger(ctx)
	vac := &storagev1.VolumeAttributesClass{}

	vacSupported, vacErr := volumeAttributesClassAPIAvailable(r.mgr)
	if vacErr != nil {
		log.Warnf("Could not discover VolumeAttributesClass API; skipping VAC lookup. Err: %v", vacErr)
		return nil, nil
	}

	if !vacSupported {
		return nil, nil
	}

	if err := r.client.Get(ctx, apitypes.NamespacedName{Name: name}, vac); err == nil {
		return vac, nil
	} else if !apierrors.IsNotFound(err) {
		return nil, err
	}

	return nil, nil
}

// ensureClusterSPIInstance ensures the ClusterStoragePolicyInfo instance exists when SC or VAC exists.
// Returns (instance, wasCreated, error).
func (r *ReconcileClusterStoragePolicyInfo) ensureClusterSPIInstance(ctx context.Context,
	instance *clusterspiv1alpha1.ClusterStoragePolicyInfo, name string,
	sc *storagev1.StorageClass, vac *storagev1.VolumeAttributesClass) (
	*clusterspiv1alpha1.ClusterStoragePolicyInfo, bool, error) {
	log := logger.GetLogger(ctx)

	// If neither SC nor VAC exists, nothing to manage
	if sc == nil && vac == nil {
		if instance != nil {
			log.Warnf("ClusterStoragePolicyInfo %q exists but no matching StorageClass or VolumeAttributesClass found",
				name)
		} else {
			log.Debugf("No StorageClass or VolumeAttributesClass found for %q, nothing to create", name)
		}
		return nil, false, nil
	}

	// Build owner references
	ownerRefs := buildOwnerReferences(ctx, r.scheme, name, sc, vac)

	// Create SPI if it doesn't exist
	if instance == nil {
		instance = &clusterspiv1alpha1.ClusterStoragePolicyInfo{
			TypeMeta: metav1.TypeMeta{
				APIVersion: apis.SchemeGroupVersion.String(),
				Kind:       "ClusterStoragePolicyInfo",
			},
			ObjectMeta: metav1.ObjectMeta{
				Name:            name,
				OwnerReferences: ownerRefs,
			},
		}
		if err := r.client.Create(ctx, instance); err != nil {
			if !apierrors.IsAlreadyExists(err) {
				log.Errorf("Failed to create ClusterStoragePolicyInfo %q: %v", name, err)
				return nil, false, err
			}
			// Validate and update owner references on the existing instance
			updatedInstance, err := r.validateAndUpdateOwnerReferences(ctx, instance, ownerRefs)
			return updatedInstance, false, err
		} else {
			log.Infof("Created ClusterStoragePolicyInfo %q with owner references", name)
			return instance, true, nil
		}
	}

	// SPI exists, check if owner references need updating
	updatedInstance, err := r.validateAndUpdateOwnerReferences(ctx, instance, ownerRefs)
	return updatedInstance, false, err
}

// validateAndUpdateOwnerReferences validates and updates owner references on existing
// ClusterStoragePolicyInfo instance.
func (r *ReconcileClusterStoragePolicyInfo) validateAndUpdateOwnerReferences(ctx context.Context,
	instance *clusterspiv1alpha1.ClusterStoragePolicyInfo, expectedOwnerRefs []metav1.OwnerReference) (
	*clusterspiv1alpha1.ClusterStoragePolicyInfo, error) {
	log := logger.GetLogger(ctx)

	// Merge expected owner references with existing ones
	currentOwnerRefs := instance.OwnerReferences
	for _, expectedRef := range expectedOwnerRefs {
		currentOwnerRefs = mergeOwnerReference(currentOwnerRefs, expectedRef)
	}

	// Check if update is needed
	if !equality.Semantic.DeepEqual(instance.OwnerReferences, currentOwnerRefs) {
		base := instance.DeepCopy()
		instance.OwnerReferences = currentOwnerRefs
		if err := r.client.Patch(ctx, instance, client.MergeFrom(base)); err != nil {
			log.Errorf("Failed to update ClusterStoragePolicyInfo %q owner references: %v", instance.Name, err)
			return nil, err
		}
		log.Infof("Updated ClusterStoragePolicyInfo %q owner references", instance.Name)
	}

	return instance, nil
}

// completeReconciliationWithSuccess resets the backoff duration for the instance
// and records a successful reconciliation.
func (r *ReconcileClusterStoragePolicyInfo) completeReconciliationWithSuccess(ctx context.Context,
	namespacedName apitypes.NamespacedName, timeout time.Duration) (reconcile.Result, error) {
	log := logger.GetLogger(ctx).With("name", namespacedName)

	// Reset backOff duration to one second on success.
	backOffDurationMapMutex.Lock()
	delete(backOffDuration, namespacedName)
	backOffDurationMapMutex.Unlock()

	log.Infof("Successfully reconciled ClusterStoragePolicyInfo")
	return reconcile.Result{}, nil
}

// completeReconciliationWithError updates the backoff duration for the instance
// and schedules a retry after the backoff timeout.
func (r *ReconcileClusterStoragePolicyInfo) completeReconciliationWithError(ctx context.Context,
	namespacedName apitypes.NamespacedName, timeout time.Duration, err error) (reconcile.Result, error) {
	log := logger.GetLogger(ctx).With("name", namespacedName)

	// Double backOff duration on error, up to the maximum.
	backOffDurationMapMutex.Lock()
	backOffDuration[namespacedName] = min(backOffDuration[namespacedName]*2,
		types.MaxBackOffDurationForReconciler)
	backOffDurationMapMutex.Unlock()

	log.Errorf("Failed to reconcile ClusterStoragePolicyInfo. Err: %v. Will retry after %v",
		namespacedName.Name, err, timeout)

	return reconcile.Result{RequeueAfter: timeout}, nil
}
