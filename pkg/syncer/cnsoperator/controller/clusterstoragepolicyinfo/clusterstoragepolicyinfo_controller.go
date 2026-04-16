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
	"sigs.k8s.io/controller-runtime/pkg/client/apiutil"
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
	"sigs.k8s.io/vsphere-csi-driver/v3/pkg/syncer/cnsoperator/util"
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
		// TODO: update this check when we add support for VKS also.
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
		For(&clusterspiv1alpha1.ClusterStoragePolicyInfo{}, builder.WithPredicates(predicate.GenerationChangedPredicate{})).
		Watches(
			&storagev1.StorageClass{},
			handler.EnqueueRequestsFromMapFunc(r.mapStorageClassToClusterSPI),
			builder.WithPredicates(scVacPredicates),
		)

	vacSupported, vacErr := volumeAttributesClassAPIAvailable(mgr)
	if vacErr != nil {
		log.Warnf("Could not discover VolumeAttributesClass API; skipping VAC watch. Err: %v", vacErr)
	} else if !vacSupported {
		log.Infof("VolumeAttributesClass API not registered on this cluster; skipping VAC watch")
	} else {
		log.Infof("VolumeAttributesClass API available; registering VAC watch")
		blder = blder.Watches(
			&storagev1.VolumeAttributesClass{},
			handler.EnqueueRequestsFromMapFunc(r.mapVolumeAttributesClassToClusterSPI),
			builder.WithPredicates(scVacPredicates),
		)
	}

	err := blder.WithOptions(controller.Options{MaxConcurrentReconciles: maxWorkerThreads}).
		Complete(r)
	if err != nil {
		log.Errorf("failed to build clusterstoragepolicyinfo controller. Err: %v", err)
		return err
	}

	return nil
}

// mapStorageClassToClusterSPI maps a StorageClass to a ClusterStoragePolicyInfo.
func (r *ReconcileClusterStoragePolicyInfo) mapStorageClassToClusterSPI(ctx context.Context,
	obj client.Object) []reconcile.Request {
	sc, ok := obj.(*storagev1.StorageClass)
	if !ok {
		return nil
	}
	if storageClassIsWaitForFirstConsumer(sc) {
		ctx = logger.NewContextWithLogger(ctx)
		logger.GetLogger(ctx).Debugf(
			"skip ClusterStoragePolicyInfo for StorageClass %q: WaitForFirstConsumer volumeBindingMode",
			sc.Name)
		return nil
	}
	return r.ensureClusterSPIExists(ctx, sc.Name, "StorageClass", sc)
}

// mapVolumeAttributesClassToClusterSPI maps a VolumeAttributesClass to a ClusterStoragePolicyInfo.
func (r *ReconcileClusterStoragePolicyInfo) mapVolumeAttributesClassToClusterSPI(ctx context.Context,
	obj client.Object) []reconcile.Request {
	vac, ok := obj.(*storagev1.VolumeAttributesClass)
	if !ok {
		return nil
	}
	return r.ensureClusterSPIExists(ctx, vac.Name, "VolumeAttributesClass", vac)
}

// generateOwnerReference returns an OwnerReference for the given client.Object.
func (r *ReconcileClusterStoragePolicyInfo) generateOwnerReference(owner client.Object) (metav1.OwnerReference, error) {
	gvk, err := apiutil.GVKForObject(owner, r.scheme)
	if err != nil {
		return metav1.OwnerReference{}, err
	}
	controller := false
	block := false
	return metav1.OwnerReference{
		APIVersion:         gvk.GroupVersion().String(),
		Kind:               gvk.Kind,
		Name:               owner.GetName(),
		UID:                owner.GetUID(),
		Controller:         &controller,
		BlockOwnerDeletion: &block,
	}, nil
}

// ensureOwnerReferenceOnClusterSPI ensures ownerReference is present on the given ClusterStoragePolicyInfo.
func (r *ReconcileClusterStoragePolicyInfo) ensureOwnerReferenceOnClusterSPI(ctx context.Context,
	clusterspi *clusterspiv1alpha1.ClusterStoragePolicyInfo, ownerRef metav1.OwnerReference, kind string) error {
	ctx = logger.NewContextWithLogger(ctx)
	log := logger.GetLogger(ctx)
	merged := mergeOwnerReference(clusterspi.OwnerReferences, ownerRef)
	if equality.Semantic.DeepEqual(clusterspi.OwnerReferences, merged) {
		return nil
	}
	base := clusterspi.DeepCopy()
	clusterspi.OwnerReferences = merged
	if err := r.client.Patch(ctx, clusterspi, client.MergeFrom(base)); err != nil {
		return err
	}
	log.Infof("Updated ClusterStoragePolicyInfo %q ownerReferences (added %s/%s)",
		clusterspi.Name, ownerRef.Kind, ownerRef.Name)
	return nil
}

// ensureClusterSPIExists returns a reconcile request for the ClusterStoragePolicyInfo
// named like the StorageClass/VAC (same name by convention). Creates the CR if missing and
// ensures an ownerReference to the triggering StorageClass or VolumeAttributesClass.
func (r *ReconcileClusterStoragePolicyInfo) ensureClusterSPIExists(ctx context.Context,
	name, kind string, owner client.Object) []reconcile.Request {
	ctx = logger.NewContextWithLogger(ctx)
	log := logger.GetLogger(ctx)
	if name == "" || owner == nil {
		return nil
	}
	ownerRef, err := r.generateOwnerReference(owner)
	if err != nil {
		log.Errorf("ownerReference for %s %q: %v", kind, name, err)
		return nil
	}
	namespacedName := apitypes.NamespacedName{Name: name}
	clusterspi := &clusterspiv1alpha1.ClusterStoragePolicyInfo{}
	err = r.client.Get(ctx, namespacedName, clusterspi)
	if err == nil {
		if err := r.ensureOwnerReferenceOnClusterSPI(ctx, clusterspi, ownerRef, kind); err != nil {
			log.Errorf("Failed to patch ownerReferences on ClusterStoragePolicyInfo %q for %s: %v", name, kind, err)
			return nil
		}
		return []reconcile.Request{{NamespacedName: namespacedName}}
	}
	if !apierrors.IsNotFound(err) {
		log.Errorf("Failed to get ClusterStoragePolicyInfo %q for %s. Err %v", name, kind, err)
		return nil
	}
	return r.createClusterSPIWithOwner(ctx, name, kind, ownerRef, namespacedName)
}

// createClusterSPIWithOwner creates a ClusterStoragePolicyInfo with ownerRef, or if it already
// exists loads it and ensures ownerRef is merged into ownerReferences.
func (r *ReconcileClusterStoragePolicyInfo) createClusterSPIWithOwner(ctx context.Context,
	name, kind string, ownerRef metav1.OwnerReference, namespacedName apitypes.NamespacedName) []reconcile.Request {
	log := logger.GetLogger(ctx)
	newClusterSPI := &clusterspiv1alpha1.ClusterStoragePolicyInfo{
		TypeMeta: metav1.TypeMeta{
			APIVersion: apis.SchemeGroupVersion.String(),
			Kind:       "ClusterStoragePolicyInfo",
		},
		ObjectMeta: metav1.ObjectMeta{
			Name:            name,
			OwnerReferences: []metav1.OwnerReference{ownerRef},
		},
		Spec: clusterspiv1alpha1.ClusterStoragePolicyInfoSpec{
			K8sCompliantName: name,
		},
	}
	if err := r.client.Create(ctx, newClusterSPI); err != nil {
		if apierrors.IsAlreadyExists(err) {
			clusterspi := &clusterspiv1alpha1.ClusterStoragePolicyInfo{}
			if err := r.client.Get(ctx, namespacedName, clusterspi); err != nil {
				log.Errorf("get ClusterStoragePolicyInfo %q after AlreadyExists: %v", name, err)
				return nil
			}
			if err := r.ensureOwnerReferenceOnClusterSPI(ctx, clusterspi, ownerRef, kind); err != nil {
				log.Errorf("Failed to patch ownerReferences on ClusterStoragePolicyInfo %q for %s: %v", name, kind, err)
				return nil
			}
			return []reconcile.Request{{NamespacedName: namespacedName}}
		}
		log.Errorf("create ClusterStoragePolicyInfo %q from %s: %v", name, kind, err)
		return nil
	}
	log.Infof("Created ClusterStoragePolicyInfo %q (from %s)", name, kind)
	return []reconcile.Request{{NamespacedName: namespacedName}}
}

var _ reconcile.Reconciler = &ReconcileClusterStoragePolicyInfo{}

// ReconcileClusterStoragePolicyInfo reconciles ClusterStoragePolicyInfo objects.
type ReconcileClusterStoragePolicyInfo struct {
	client     client.Client
	scheme     *runtime.Scheme
	configInfo *config.ConfigurationInfo
	recorder   record.EventRecorder
}

// Reconcile syncs storage policy attributes from the vCenter.
func (r *ReconcileClusterStoragePolicyInfo) Reconcile(ctx context.Context,
	request reconcile.Request) (reconcile.Result, error) {
	ctx = logger.NewContextWithLogger(ctx)
	log := logger.GetLogger(ctx)

	// request always identifies a ClusterStoragePolicyInfo. SC/VAC watches enqueue the same
	// NamespacedName after ensureClusterSPIExists (see mapStorageClassToClusterSPI /
	// mapVolumeAttributesClassToClusterSPI).
	instance := &clusterspiv1alpha1.ClusterStoragePolicyInfo{}
	err := r.client.Get(ctx, request.NamespacedName, instance)
	if err != nil {
		if apierrors.IsNotFound(err) {
			return reconcile.Result{}, nil
		}
		return reconcile.Result{}, err
	}
	if instance.DeletionTimestamp != nil {
		return reconcile.Result{}, nil
	}

	log.Infof("Reconciling ClusterStoragePolicyInfo %q", request.Name)

	// TODO: Add storage policy attributes sync logic (vCenter / SPBM).
	return reconcile.Result{}, nil
}
