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
	"fmt"
	"strings"
	"sync"
	"time"

	cnstypes "github.com/vmware/govmomi/cns/types"
	"github.com/vmware/govmomi/vim25/soap"
	vimtypes "github.com/vmware/govmomi/vim25/types"
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
	cnsvsphere "sigs.k8s.io/vsphere-csi-driver/v3/pkg/common/cns-lib/vsphere"
	"sigs.k8s.io/vsphere-csi-driver/v3/pkg/common/config"
	csicommon "sigs.k8s.io/vsphere-csi-driver/v3/pkg/csi/service/common"
	"sigs.k8s.io/vsphere-csi-driver/v3/pkg/csi/service/common/commonco"
	"sigs.k8s.io/vsphere-csi-driver/v3/pkg/csi/service/logger"
	k8s "sigs.k8s.io/vsphere-csi-driver/v3/pkg/kubernetes"
	"sigs.k8s.io/vsphere-csi-driver/v3/pkg/syncer/cnsoperator/types"
	"sigs.k8s.io/vsphere-csi-driver/v3/pkg/syncer/cnsoperator/util"
)

const (
	workerThreadsEnvVar     = "WORKER_THREADS_CLUSTER_STORAGE_POLICY_INFO"
	defaultMaxWorkerThreads = 4
)

var (
	// backOffDuration is a map of clusterstoragepolicyinfo name's to the time after which
	// a request for this instance will be requeued.
	// Initialized to 1 second for new instances and for instances whose latest
	// reconcile operation succeeded.
	// If the reconcile fails, backoff is incremented exponentially.
	backOffDuration         map[apitypes.NamespacedName]time.Duration
	backOffDurationMapMutex = sync.Mutex{}
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
	if !commonco.ContainerOrchestratorUtility.IsFSSEnabled(ctx, csicommon.SupportsExposingStoragePolicyAttributes) {
		log.Infof("Not initializing the ClusterStoragePolicyInfo Controller: capability %q is not activated",
			csicommon.SupportsExposingStoragePolicyAttributes)
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

	// Initialize backOffDuration map
	backOffDuration = make(map[apitypes.NamespacedName]time.Duration)
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

	// Initialize backOffDuration for the instance, if required.
	backOffDurationMapMutex.Lock()
	var timeout time.Duration
	if _, exists := backOffDuration[request.NamespacedName]; !exists {
		backOffDuration[request.NamespacedName] = time.Second
	}
	timeout = backOffDuration[request.NamespacedName]
	backOffDurationMapMutex.Unlock()

	// Sync storage policy attributes from vCenter
	err = r.syncStoragePolicyAttributes(ctx, instance)
	if err != nil {
		log.Errorf("Failed to sync storage policy attributes for %q: %v.", request.Name, err)

		// Record warning event which will handle backoff duration increase
		r.recordEvent(ctx, instance, v1.EventTypeWarning, fmt.Sprintf("Failed to sync storage policy attributes: %v", err))

		statusErr := r.updateStatus(ctx, instance)
		if statusErr != nil {
			log.Errorf("failed to update status for ClusterStoragePolicyInfo %q: %v", request.Name, statusErr)
		}

		return reconcile.Result{RequeueAfter: timeout}, err
	}

	instance.Status.Error = ""
	statusErr := r.updateStatus(ctx, instance)
	// Handle status update errors (when sync succeeded)
	if statusErr != nil {
		log.Errorf("failed to update status for ClusterStoragePolicyInfo %q: %v", request.Name, statusErr)
		// Apply backoff for status update failures and record warning event
		r.recordEvent(ctx, instance, v1.EventTypeWarning, fmt.Sprintf("Failed to update status: %v", statusErr))
		return reconcile.Result{RequeueAfter: timeout}, statusErr
	}

	// Success - reset backoff duration and record normal event
	r.recordEvent(ctx, instance, v1.EventTypeNormal, "Successfully synced storage policy attributes")
	log.Infof("Successfully synced storage policy attributes for %q", request.Name)

	return reconcile.Result{}, nil
}

// syncStoragePolicyAttributes syncs storage policy attributes from vCenter.
func (r *ReconcileClusterStoragePolicyInfo) syncStoragePolicyAttributes(ctx context.Context,
	instance *clusterspiv1alpha1.ClusterStoragePolicyInfo) error {
	log := logger.GetLogger(ctx)

	// The instance.Name is the K8s compliant name, which we can use directly
	// to search for the storage policy using the new queryProfileDetails API
	k8sCompliantName := instance.Name
	log.Infof("Checking storage policy for K8s compliant name %q (ClusterStoragePolicyInfo %q)", 
		k8sCompliantName, instance.Name)

	// Connect to vCenter
	vc, err := cnsvsphere.GetVirtualCenterInstance(ctx, r.configInfo, false)
	if err != nil {
		return fmt.Errorf("failed to get vCenter instance: %w", err)
	}

	// Use the new API to find profile by K8s compliant name
	profile, err := vc.FindProfileByK8sCompliantName(ctx, k8sCompliantName)
	if err != nil {
		// Profile not found - this is expected when policy is deleted
		log.Warnf("Storage policy with K8s compliant name %q not found in vCenter: %v", k8sCompliantName, err)
		instance.Status.StoragePolicyDeleted = true
		return nil
	}

	// Profile found - policy exists
	log.Infof("Storage policy found with K8s compliant name %q: ID=%s, Name=%s", 
		k8sCompliantName, profile.ID, profile.Name)
	instance.Status.StoragePolicyDeleted = false

	return nil
}

// validateStoragePolicyExistsOnVcenter validates storage policy existence and updates the instance status.
func (r *ReconcileClusterStoragePolicyInfo) validateStoragePolicyExistsOnVcenter(ctx context.Context,
	instance *clusterspiv1alpha1.ClusterStoragePolicyInfo,
	vc *cnsvsphere.VirtualCenter,
	storagePolicyID string) error {
	log := logger.GetLogger(ctx)

	// Check if storage policy exists in vCenter SPBM
	policyExists, err := r.checkStoragePolicyExists(ctx, vc, storagePolicyID)
	if err != nil {
		return fmt.Errorf("failed to check storage policy existence: %w", err)
	}

	// Set status based on policy existence
	instance.Status.StoragePolicyDeleted = !policyExists

	if !policyExists {
		log.Warnf("Storage policy %q not found in vCenter", storagePolicyID)
	} else {
		log.Infof("Storage policy %q exists in vCenter", storagePolicyID)
	}

	return nil
}

// extractStoragePolicyName extracts the storage policy name from the StorageClass with the given name.
func (r *ReconcileClusterStoragePolicyInfo) extractStoragePolicyName(ctx context.Context, 
	scName string) (string, error) {
	log := logger.GetLogger(ctx)

	// Get the StorageClass with the same name as the ClusterStoragePolicyInfo
	storageClass := &storagev1.StorageClass{}
	namespacedName := apitypes.NamespacedName{Name: scName}
	err := r.client.Get(ctx, namespacedName, storageClass)
	if err != nil {
		return "", fmt.Errorf("failed to get StorageClass %q: %w", scName, err)
	}

	// Extract storage policy name from StorageClass parameters
	storagePolicyName := storageClass.Parameters[csicommon.AttributeStoragePolicyName]
	if storagePolicyName == "" {
		return "", fmt.Errorf("no storagepolicyname parameter found in StorageClass %q", scName)
	}

	log.Debugf("Found storage policy name %q in StorageClass %q", storagePolicyName, scName)

	return storagePolicyName, nil
}

// checkStoragePolicyExists checks if the given storage policy ID exists in vCenter SPBM.
func (r *ReconcileClusterStoragePolicyInfo) checkStoragePolicyExists(ctx context.Context,
	vc *cnsvsphere.VirtualCenter, storagePolicyID string) (bool, error) {
	log := logger.GetLogger(ctx)

	// Connect to PBM (Policy-Based Management)
	err := vc.ConnectPbm(ctx)
	if err != nil {
		return false, fmt.Errorf("failed to connect to PBM: %w", err)
	}

	// Try to retrieve the storage policy content to check if it exists
	policyContents, err := vc.PbmRetrieveContent(ctx, []string{storagePolicyID})
	if err != nil {
		// Check if this is an "invalid profile" error (policy doesn't exist)
		if isInvalidProfile, _ := r.isInvalidProfileErr(ctx, err); isInvalidProfile {
			log.Debugf("Storage policy %q not found in vCenter: %v", storagePolicyID, err)
			return false, nil
		}
		// Other errors (network, authentication, etc.) should be propagated
		return false, fmt.Errorf("failed to retrieve storage policy content: %w", err)
	}

	// Check if we got any policy content back
	if len(policyContents) == 0 {
		log.Debugf("No policy content returned for storage policy ID %q", storagePolicyID)
		return false, nil
	}

	// Verify the returned policy ID matches what we requested
	for _, policy := range policyContents {
		if policy.ID == storagePolicyID {
			log.Debugf("Storage policy %q exists in vCenter", storagePolicyID)
			return true, nil
		}
	}

	log.Debugf("Storage policy %q not found in returned policy contents", storagePolicyID)
	return false, nil
}

// isInvalidProfileErr returns whether the given error is an InvalidArgument
// for the profileId returned by SPBM indicating that the policy doesn't exist.
func (r *ReconcileClusterStoragePolicyInfo) isInvalidProfileErr(ctx context.Context, err error) (bool, []string) {
	log := logger.GetLogger(ctx)
	if !soap.IsSoapFault(err) {
		return false, nil
	}
	soapFault := soap.ToSoapFault(err)
	vimFault, isInvalidArgumentErr := soapFault.VimFault().(vimtypes.InvalidArgument)
	if isInvalidArgumentErr && vimFault.InvalidProperty == "profileId" {
		// Parse the profile IDs from the error message.
		log.Debugf("Invalid profile error: %+v", soapFault.String)
		if strings.HasPrefix(soapFault.String, "Profile not found. Id:") {
			profiles := strings.TrimPrefix(soapFault.String, "Profile not found. Id:")
			split := strings.Split(profiles, ",")
			profilesSlice := make([]string, 0)
			for i := range split {
				s := strings.TrimSpace(split[i])
				if s != "" {
					profilesSlice = append(profilesSlice, s)
				}
			}
			log.Debugf("isInvalidProfileErr %v", profilesSlice)
			return true, profilesSlice
		}
		// If the error returned from SPBM does not have profileIDs, return nil.
		return true, nil
	}
	return false, nil
}


// updateStatus updates the ClusterStoragePolicyInfo status using patch to avoid generation changes.
func (r *ReconcileClusterStoragePolicyInfo) updateStatus(ctx context.Context,
	instance *clusterspiv1alpha1.ClusterStoragePolicyInfo) error {

	// Create a base copy for patching
	base := instance.DeepCopy()
	// Clear the status in the base to create a proper patch
	base.Status = clusterspiv1alpha1.ClusterStoragePolicyInfoStatus{}

	// Use the status().Patch() method to update just the status subresource
	err := r.client.Status().Patch(ctx, instance, client.MergeFrom(base))
	if err != nil {
		return fmt.Errorf("failed to update ClusterStoragePolicyInfo status: %w", err)
	}
	return nil
}

// recordEvent records events and handles backoff duration management
func (r *ReconcileClusterStoragePolicyInfo) recordEvent(ctx context.Context,
	instance *clusterspiv1alpha1.ClusterStoragePolicyInfo, eventtype string, msg string) {
	log := logger.GetLogger(ctx)
	log.Debugf("Event type is %s", eventtype)
	namespacedName := apitypes.NamespacedName{
		Name: instance.Name,
	}
	switch eventtype {
	case v1.EventTypeWarning:
		// Double backOff duration.
		backOffDurationMapMutex.Lock()
		backOffDuration[namespacedName] = min(backOffDuration[namespacedName]*2,
			types.MaxBackOffDurationForReconciler)
		r.recorder.Event(instance, v1.EventTypeWarning, "ClusterStoragePolicyInfoFailed", msg)
		backOffDurationMapMutex.Unlock()
	case v1.EventTypeNormal:
		// Reset backOff duration to 1 second on success.
		backOffDurationMapMutex.Lock()
		backOffDuration[namespacedName] = time.Second
		r.recorder.Event(instance, v1.EventTypeNormal, "ClusterStoragePolicyInfoSynced", msg)
		backOffDurationMapMutex.Unlock()
	}
}
