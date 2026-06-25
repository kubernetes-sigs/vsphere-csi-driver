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

package csivolumeinfo

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"time"

	cnstypes "github.com/vmware/govmomi/cns/types"
	vim25types "github.com/vmware/govmomi/vim25/types"
	corev1 "k8s.io/api/core/v1"
	storagev1 "k8s.io/api/storage/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	k8stypes "k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller"
	"sigs.k8s.io/controller-runtime/pkg/manager"
	"sigs.k8s.io/controller-runtime/pkg/predicate"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	csivolumeinfosvc "sigs.k8s.io/vsphere-csi-driver/v3/pkg/apis/cnsoperator/csivolumeinfo"
	csivolumeinfov1alpha1 "sigs.k8s.io/vsphere-csi-driver/v3/pkg/apis/cnsoperator/csivolumeinfo/v1alpha1"
	volumes "sigs.k8s.io/vsphere-csi-driver/v3/pkg/common/cns-lib/volume"
	cnsvsphere "sigs.k8s.io/vsphere-csi-driver/v3/pkg/common/cns-lib/vsphere"
	commonconfig "sigs.k8s.io/vsphere-csi-driver/v3/pkg/common/config"
	"sigs.k8s.io/vsphere-csi-driver/v3/pkg/csi/service/logger"
	cnsoptypes "sigs.k8s.io/vsphere-csi-driver/v3/pkg/syncer/cnsoperator/types"
	"sigs.k8s.io/vsphere-csi-driver/v3/pkg/syncer/cnsoperator/util"
)

const (
	workerThreadEnvVar      = "WORKER_THREADS_CSIVOLUMEINFO"
	defaultMaxWorkerThreads = 10

	// conditionTypeReady is the standard condition type used on CsiVolumeInfo.
	conditionTypeReady = "Ready"

	// reason strings for conditions.
	reasonUnregisterSucceeded = "UnregisterSucceeded"
	reasonRegisterSucceeded   = "RegisterSucceeded"
	reasonReconcileFailed     = "ReconcileFailed"
	reasonInitialCSIManaged   = "InitialCSIManaged"
)

var (
	// backOffDuration is a map of CsiVolumeInfo names to the next requeue delay.
	// Initialised to 1 second and doubled on failure up to MaxBackOffDurationForReconciler.
	backOffDuration         map[k8stypes.NamespacedName]time.Duration
	backOffDurationMapMutex = sync.Mutex{}
)

// newReconciler returns a new reconcile.Reconciler.
func newReconciler(mgr manager.Manager, volumeManager volumes.Manager,
	configInfo *commonconfig.ConfigurationInfo,
	cviSvc csivolumeinfosvc.CsiVolumeInfoService) reconcile.Reconciler {
	return &Reconciler{
		client:        mgr.GetClient(),
		scheme:        mgr.GetScheme(),
		configInfo:    configInfo,
		volumeManager: volumeManager,
		cviSvc:        cviSvc,
	}
}

// add registers a new controller with mgr using r as the reconcile.Reconciler.
func add(mgr manager.Manager, r reconcile.Reconciler) error {
	ctx, log := logger.GetNewContextWithLogger()

	maxWorkerThreads := util.GetMaxWorkerThreads(ctx, workerThreadEnvVar, defaultMaxWorkerThreads)

	err := ctrl.NewControllerManagedBy(mgr).
		Named("csivolumeinfo-controller").
		For(&csivolumeinfov1alpha1.CsiVolumeInfo{}).
		WithEventFilter(predicate.GenerationChangedPredicate{}).
		WithOptions(controller.Options{MaxConcurrentReconciles: maxWorkerThreads}).
		Complete(r)
	if err != nil {
		log.Errorf("Failed to build csivolumeinfo controller. Err: %v", err)
		return err
	}

	backOffDuration = make(map[k8stypes.NamespacedName]time.Duration)
	return nil
}

// blank assignment to verify Reconciler implements reconcile.Reconciler.
var _ reconcile.Reconciler = &Reconciler{}

// Reconciler reconciles CsiVolumeInfo objects.
type Reconciler struct {
	// client is a split client: reads from cache, writes to the API server.
	client        client.Client
	scheme        *runtime.Scheme
	configInfo    *commonconfig.ConfigurationInfo
	volumeManager volumes.Manager
	cviSvc        csivolumeinfosvc.CsiVolumeInfoService
}

// Reconcile reads the state of the CsiVolumeInfo CR and drives the volume
// ownership state machine.  The controller is the sole writer of
// status.ownership and status.phase; vm-operator is the sole writer of
// spec.vms.
//
// Decision table:
//
//	len(spec.vms)>0 ∧ (ownership=="" || ownership=="CSIManaged") → reconcileUnregister
//	len(spec.vms)==0 ∧ ownership=="VMManaged"                    → reconcileRegister
//	otherwise                                                     → idle (no-op)
func (r *Reconciler) Reconcile(ctx context.Context,
	req reconcile.Request) (reconcile.Result, error) {
	ctx = logger.NewContextWithLogger(ctx)
	log := logger.GetLogger(ctx).With("name", req.NamespacedName)
	log.Infof("Reconcile: entry for CsiVolumeInfo %s", req.Name)

	// CVI CRs always live in vmware-system-csi; look up by the fixed namespace.
	cvi := &csivolumeinfov1alpha1.CsiVolumeInfo{}
	nn := k8stypes.NamespacedName{
		Namespace: csivolumeinfov1alpha1.CVINamespace,
		Name:      req.Name,
	}
	if err := r.client.Get(ctx, nn, cvi); err != nil {
		if apierrors.IsNotFound(err) {
			// The CVI is gone; drop its backoff entry so the map does not retain
			// state for deleted volumes.
			deleteBackoffEntry(ctx, nn)
			log.Infof("Reconcile: CsiVolumeInfo %s not found; must be deleted — no action", req.Name)
			return reconcile.Result{}, nil
		}
		log.Errorf("Reconcile: error reading CsiVolumeInfo %s: %v", req.Name, err)
		return reconcile.Result{}, err
	}

	backoff := getBackoffDuration(ctx, nn)
	log.Infof("Reconcile: current backoff duration %s", backoff)

	// Ensure the PV ownerRef is present. This is idempotent and repairs CVIs
	// created before this logic existed (existing CVIs are reconciled on startup
	// via synthetic Create events from the informer cache sync).
	if err := r.ensurePVOwnerRef(ctx, cvi); err != nil {
		log.Errorf("Reconcile: failed to ensure PV ownerRef for %s: %v", req.Name, err)
		return reconcile.Result{}, err
	}

	vmCount := len(cvi.Spec.VMs)
	ownership := cvi.Status.Ownership

	switch {
	case vmCount > 0 && (ownership == "" || ownership == csivolumeinfov1alpha1.OwnershipStateCSIManaged):
		log.Infof("Reconcile: %d VM(s) attached, ownership=%q → reconcileUnregister", vmCount, ownership)
		if err := r.reconcileUnregister(ctx, cvi); err != nil {
			log.Errorf("Reconcile: reconcileUnregister failed: %v", err)
			if statusErr := r.setFailedStatus(ctx, cvi, err.Error()); statusErr != nil {
				log.Warnf("Reconcile: could not write failed status: %v", statusErr)
			}
			doubleBackoffDuration(ctx, nn)
			return reconcile.Result{RequeueAfter: backoff}, nil
		}
		updateBackoffEntry(ctx, nn, time.Second)

	case vmCount == 0 && ownership == csivolumeinfov1alpha1.OwnershipStateVMManaged:
		log.Infof("Reconcile: no VMs attached, ownership=VMManaged → reconcileRegister")
		if err := r.reconcileRegister(ctx, cvi); err != nil {
			log.Errorf("Reconcile: reconcileRegister failed: %v", err)
			if statusErr := r.setFailedStatus(ctx, cvi, err.Error()); statusErr != nil {
				log.Warnf("Reconcile: could not write failed status: %v", statusErr)
			}
			doubleBackoffDuration(ctx, nn)
			return reconcile.Result{RequeueAfter: backoff}, nil
		}
		updateBackoffEntry(ctx, nn, time.Second)

	case vmCount == 0 && ownership == "":
		// Initial state: CR just created, no VMs attached yet. Write the initial
		// CSIManaged status so vm-operator's observedGeneration wait condition is satisfied.
		log.Infof("Reconcile: initial state — writing CSIManaged status for %s", req.Name)
		patch := buildStatusPatch(cvi.Generation,
			csivolumeinfov1alpha1.OwnershipStateCSIManaged,
			csivolumeinfov1alpha1.PhaseSucceeded,
			"", reasonInitialCSIManaged, true)
		if err := r.cviSvc.PatchCsiVolumeInfoStatus(ctx, cvi.Spec.VolumeID, patch); err != nil {
			log.Errorf("Reconcile: failed to set initial CSIManaged status: %v", err)
			return reconcile.Result{}, err
		}

	default:
		log.Infof("Reconcile: idle — vmCount=%d, ownership=%q; no action", vmCount, ownership)
	}

	log.Infof("Reconcile: exit for CsiVolumeInfo %s", req.Name)
	return reconcile.Result{}, nil
}

// reconcileUnregister executes the two-phase CNS unregister protocol and
// transitions the CVI to VMManaged ownership.
//
// Steps:
//  1. Call volumeManager.UnregisterVolumeEx → get backingDiskPath, diskUUID.
//  2. Patch spec.diskPath and spec.diskUUID onto the CVI.
//  3. Add the volume-protection finalizer so GC is blocked while VM-managed.
//  4. Patch status: ownership=VMManaged, phase=Succeeded, observedGeneration, Ready=True.
//  5. Ack the unregister (phase-2 of the two-phase protocol).
func (r *Reconciler) reconcileUnregister(ctx context.Context,
	cvi *csivolumeinfov1alpha1.CsiVolumeInfo) error {
	log := logger.GetLogger(ctx).With("volumeID", cvi.Spec.VolumeID)
	log.Infof("reconcileUnregister: calling UnregisterVolumeEx for volume %q", cvi.Spec.VolumeID)

	backingDiskPath, diskUUID, err := r.volumeManager.UnregisterVolumeEx(ctx, cvi.Spec.VolumeID)
	if err != nil {
		return fmt.Errorf("reconcileUnregister: UnregisterVolumeEx failed for %q: %w",
			cvi.Spec.VolumeID, err)
	}
	log.Infof("reconcileUnregister: UnregisterVolumeEx succeeded — diskPath=%q, diskUUID=%q",
		backingDiskPath, diskUUID)

	// Persist diskPath and diskUUID onto the spec so vm-operator can use them for attachment.
	specPatch, err := json.Marshal(map[string]interface{}{
		"spec": map[string]interface{}{
			"diskPath": backingDiskPath,
			"diskUUID": diskUUID,
		},
	})
	if err != nil {
		return fmt.Errorf("reconcileUnregister: failed to marshal spec patch for %q: %w",
			cvi.Spec.VolumeID, err)
	}
	// The spec patch increments metadata.generation, so capture the post-patch
	// generation. observedGeneration must reflect this latest generation;
	// otherwise vm-operator's green-signal check (observedGeneration >= generation)
	// would never be satisfied, because the controller's own spec write advanced
	// generation beyond the value observed at reconcile entry.
	patchedGen, err := r.cviSvc.PatchCsiVolumeInfo(ctx, cvi.Spec.VolumeID, specPatch)
	if err != nil {
		return fmt.Errorf("reconcileUnregister: failed to patch spec for %q: %w",
			cvi.Spec.VolumeID, err)
	}
	log.Infof("reconcileUnregister: patched spec.diskPath and spec.diskUUID for volume %q (generation=%d)",
		cvi.Spec.VolumeID, patchedGen)

	// Add the volume-protection finalizer before status transition so GC is blocked.
	// Finalizer changes are metadata-only and do not advance generation, so
	// patchedGen remains the generation to record in status.
	if err := r.cviSvc.AddVolumeProtectionFinalizer(ctx, cvi.Spec.VolumeID); err != nil {
		return fmt.Errorf("reconcileUnregister: failed to add protection finalizer for %q: %w",
			cvi.Spec.VolumeID, err)
	}
	log.Infof("reconcileUnregister: volume-protection finalizer added for volume %q", cvi.Spec.VolumeID)

	// Write status: ownership=VMManaged, phase=Succeeded.
	statusPatch := buildStatusPatch(patchedGen,
		csivolumeinfov1alpha1.OwnershipStateVMManaged,
		csivolumeinfov1alpha1.PhaseSucceeded, "", reasonUnregisterSucceeded, true)
	if err := r.cviSvc.PatchCsiVolumeInfoStatus(ctx, cvi.Spec.VolumeID, statusPatch); err != nil {
		return fmt.Errorf("reconcileUnregister: failed to patch status for %q: %w",
			cvi.Spec.VolumeID, err)
	}
	log.Infof("reconcileUnregister: status patched to VMManaged/Succeeded for volume %q",
		cvi.Spec.VolumeID)

	// Phase-2: acknowledge the unregister.  This must happen after all durable
	// state has been written so the controller can recover on restart.
	log.Infof("reconcileUnregister: calling AckUnregister for volume %q", cvi.Spec.VolumeID)
	if err := r.volumeManager.AckUnregister(ctx, cvi.Spec.VolumeID); err != nil {
		return fmt.Errorf("reconcileUnregister: AckUnregister failed for %q: %w",
			cvi.Spec.VolumeID, err)
	}
	log.Infof("reconcileUnregister: AckUnregister completed for volume %q", cvi.Spec.VolumeID)
	return nil
}

// reconcileRegister re-registers a formerly VM-managed VMDK as a first-class
// disk (FCD) with full Kubernetes metadata, and transitions the CVI to
// CSIManaged ownership.
//
// Steps:
//  1. Fetch the PVC and PV referenced by spec.pvcNamespace/pvcName and spec.pvName.
//  2. Reconstruct CNS entity metadata.
//  3. Resolve the SPBM storage policy ID from PV volumeAttributes or StorageClass.
//  4. Call volumeManager.CreateVolume (re-register); CnsVolumeAlreadyExistsFault → success.
//  5. Patch status: ownership=CSIManaged, phase=Succeeded, observedGeneration, Ready=True.
//  6. Remove the volume-protection finalizer.
func (r *Reconciler) reconcileRegister(ctx context.Context,
	cvi *csivolumeinfov1alpha1.CsiVolumeInfo) error {
	log := logger.GetLogger(ctx).With("volumeID", cvi.Spec.VolumeID)
	log.Infof("reconcileRegister: starting for volume %q (diskPath=%q)", cvi.Spec.VolumeID, cvi.Spec.DiskPath)

	// Fetch PVC.
	pvc := &corev1.PersistentVolumeClaim{}
	if err := r.client.Get(ctx, k8stypes.NamespacedName{
		Namespace: cvi.Spec.PVCNamespace,
		Name:      cvi.Spec.PVCName,
	}, pvc); err != nil {
		return fmt.Errorf("reconcileRegister: failed to get PVC %s/%s: %w",
			cvi.Spec.PVCNamespace, cvi.Spec.PVCName, err)
	}

	// Fetch PV.
	pv := &corev1.PersistentVolume{}
	if err := r.client.Get(ctx, k8stypes.NamespacedName{Name: cvi.Spec.PVName}, pv); err != nil {
		return fmt.Errorf("reconcileRegister: failed to get PV %q: %w", cvi.Spec.PVName, err)
	}

	// Resolve cluster ID.
	clusterID := r.configInfo.Cfg.Global.ClusterID
	if clusterID == "" {
		clusterID = r.configInfo.Cfg.Global.SupervisorID
	}

	// Extract vCenter user for containerCluster.
	var vcUser, clusterDist string
	for _, vcCfg := range r.configInfo.Cfg.VirtualCenter {
		vcUser = vcCfg.User
		break
	}
	clusterDist = r.configInfo.Cfg.Global.ClusterDistribution

	containerCluster := cnsvsphere.GetContainerCluster(
		clusterID, vcUser, cnstypes.CnsClusterFlavorWorkload, clusterDist)

	// Reconstruct entity references from the live PVC and PV.
	pvRef := cnsvsphere.CreateCnsKuberenetesEntityReference(
		string(cnstypes.CnsKubernetesEntityTypePV),
		pv.Name, "", clusterID)

	pvcMeta := cnsvsphere.GetCnsKubernetesEntityMetaData(
		pvc.Name, pvc.Labels, false,
		string(cnstypes.CnsKubernetesEntityTypePVC),
		pvc.Namespace, clusterID,
		[]cnstypes.CnsKubernetesEntityReference{pvRef})

	pvMeta := cnsvsphere.GetCnsKubernetesEntityMetaData(
		pv.Name, pv.Labels, false,
		string(cnstypes.CnsKubernetesEntityTypePV),
		"", clusterID, nil)

	// Resolve the SPBM profile ID.
	storagePolicyID := resolveStoragePolicyID(ctx, r.client, pv)

	// Convert the datastore path captured during unregister to the HTTP folder
	// URL that CNS's RegisterVMDKWithUrlAction requires. BackingDiskId cannot be
	// used here because the FCD entry no longer exists after UnregisterVolumeEx.
	diskFolderURL, err := r.volumeManager.GetDiskFolderURL(ctx, cvi.Spec.DiskPath)
	if err != nil {
		return fmt.Errorf("reconcileRegister: failed to resolve disk folder URL for %q: %w",
			cvi.Spec.DiskPath, err)
	}

	// Preserve the original FCD ID by setting VolumeId on the top-level spec.
	// When FCD_TRANSACTION_SUPPORT is enabled (vSphere 9.2+) CNS passes this to
	// FcdSvc()->RegisterDisk so the re-registered FCD retains the same UUID.
	origVolumeID := cnstypes.CnsVolumeId{Id: cvi.Spec.VolumeID}
	createSpec := &cnstypes.CnsVolumeCreateSpec{
		Name:       pv.Name,
		VolumeType: string(cnstypes.CnsVolumeTypeBlock),
		VolumeId:   &origVolumeID,
		Profile:    buildStorageProfileSpec(storagePolicyID),
		Metadata: cnstypes.CnsVolumeMetadata{
			ContainerCluster:      containerCluster,
			ContainerClusterArray: []cnstypes.CnsContainerCluster{containerCluster},
			EntityMetadata:        []cnstypes.BaseCnsEntityMetadata{pvcMeta, pvMeta},
		},
		BackingObjectDetails: &cnstypes.CnsBlockBackingDetails{
			BackingDiskUrlPath: diskFolderURL,
		},
	}

	log.Infof("reconcileRegister: calling CreateVolume for volume %q", cvi.Spec.VolumeID)
	_, faultType, err := r.volumeManager.CreateVolume(ctx, createSpec, nil)
	if err != nil {
		// An already-registered backing disk means the volume is back under CSI
		// management, which is the desired end state — treat it as success. CNS
		// reports this as either CnsAlreadyRegisteredFault (re-register) or
		// CnsVolumeAlreadyExistsFault.
		if volumes.IsCnsAlreadyRegisteredFault(ctx, faultType) ||
			volumes.IsCnsVolumeAlreadyExistsFault(ctx, faultType) {
			log.Infof("reconcileRegister: volume %q already registered as FCD — treating as success",
				cvi.Spec.VolumeID)
		} else {
			return fmt.Errorf("reconcileRegister: CreateVolume failed for %q (fault=%q): %w",
				cvi.Spec.VolumeID, faultType, err)
		}
	} else {
		log.Infof("reconcileRegister: CreateVolume succeeded for volume %q", cvi.Spec.VolumeID)
	}

	// Patch status: ownership=CSIManaged, phase=Succeeded.
	statusPatch := buildStatusPatch(cvi.Generation,
		csivolumeinfov1alpha1.OwnershipStateCSIManaged,
		csivolumeinfov1alpha1.PhaseSucceeded, "", reasonRegisterSucceeded, true)
	if err := r.cviSvc.PatchCsiVolumeInfoStatus(ctx, cvi.Spec.VolumeID, statusPatch); err != nil {
		return fmt.Errorf("reconcileRegister: failed to patch status for %q: %w",
			cvi.Spec.VolumeID, err)
	}
	log.Infof("reconcileRegister: status patched to CSIManaged/Succeeded for volume %q", cvi.Spec.VolumeID)

	// Remove the volume-protection finalizer now the volume is CSI-managed again.
	if err := r.cviSvc.RemoveVolumeProtectionFinalizer(ctx, cvi.Spec.VolumeID); err != nil {
		return fmt.Errorf("reconcileRegister: failed to remove protection finalizer for %q: %w",
			cvi.Spec.VolumeID, err)
	}
	log.Infof("reconcileRegister: volume-protection finalizer removed for volume %q", cvi.Spec.VolumeID)
	return nil
}

// ensurePVOwnerRef sets a PersistentVolume ownerReference on the CsiVolumeInfo
// if one is not already present. This enables Kubernetes GC to cascade-delete
// the CVI when the PV is deleted (CSIManaged path) and blockOwnerDeletion to
// prevent PV deletion while the volume-protection finalizer is present (VMManaged path).
//
// If the PV does not exist yet (race between CreateVolume and PV creation by the CO),
// the error is returned so controller-runtime requeues the reconcile.
func (r *Reconciler) ensurePVOwnerRef(ctx context.Context,
	cvi *csivolumeinfov1alpha1.CsiVolumeInfo) error {
	log := logger.GetLogger(ctx).With("volumeID", cvi.Spec.VolumeID)

	if cvi.Spec.PVName == "" {
		log.Debugf("ensurePVOwnerRef: spec.pvName empty, skipping")
		return nil
	}

	// Already set — nothing to do.
	for _, ref := range cvi.OwnerReferences {
		if ref.Kind == "PersistentVolume" && ref.Name == cvi.Spec.PVName {
			return nil
		}
	}

	pv := &corev1.PersistentVolume{}
	if err := r.client.Get(ctx, k8stypes.NamespacedName{Name: cvi.Spec.PVName}, pv); err != nil {
		if apierrors.IsNotFound(err) {
			// PV not yet created by the CO; requeue so we retry once it exists.
			return fmt.Errorf("ensurePVOwnerRef: PV %q not found yet for CVI %q, will retry",
				cvi.Spec.PVName, cvi.Name)
		}
		return fmt.Errorf("ensurePVOwnerRef: failed to get PV %q: %w", cvi.Spec.PVName, err)
	}

	blockOwnerDeletion := true
	ownerRef := metav1.OwnerReference{
		APIVersion:         "v1",
		Kind:               "PersistentVolume",
		Name:               pv.Name,
		UID:                pv.UID,
		BlockOwnerDeletion: &blockOwnerDeletion,
	}

	patch := map[string]interface{}{
		"metadata": map[string]interface{}{
			"ownerReferences": []metav1.OwnerReference{ownerRef},
		},
	}
	patchBytes, err := json.Marshal(patch)
	if err != nil {
		return fmt.Errorf("ensurePVOwnerRef: failed to marshal patch: %w", err)
	}

	if err := r.client.Patch(ctx, cvi, client.RawPatch(k8stypes.MergePatchType, patchBytes)); err != nil {
		return fmt.Errorf("ensurePVOwnerRef: failed to patch CVI %q: %w", cvi.Name, err)
	}

	log.Infof("ensurePVOwnerRef: set PV ownerRef (pv=%q uid=%q) on CVI %q",
		pv.Name, pv.UID, cvi.Name)
	return nil
}

// setFailedStatus patches status.phase=Failed with an error message and
// sets the Ready condition to False. observedGeneration is also updated.
func (r *Reconciler) setFailedStatus(ctx context.Context,
	cvi *csivolumeinfov1alpha1.CsiVolumeInfo, errMsg string) error {
	patch := buildStatusPatch(cvi.Generation,
		cvi.Status.Ownership, // do not change ownership on failure
		csivolumeinfov1alpha1.PhaseFailed,
		errMsg, reasonReconcileFailed, false)
	return r.cviSvc.PatchCsiVolumeInfoStatus(ctx, cvi.Spec.VolumeID, patch)
}

// buildStatusPatch constructs a JSON merge-patch for the status subresource.
// It always sets observedGeneration to ensure vm-operator's wait condition is met.
func buildStatusPatch(generation int64, ownership csivolumeinfov1alpha1.OwnershipState,
	phase csivolumeinfov1alpha1.PhaseState, errMsg string,
	condReason string, condReady bool) []byte {

	condStatus := metav1.ConditionFalse
	if condReady {
		condStatus = metav1.ConditionTrue
	}
	cond := map[string]interface{}{
		"type":               conditionTypeReady,
		"status":             string(condStatus),
		"reason":             condReason,
		"message":            errMsg,
		"lastTransitionTime": metav1.Now().UTC().Format(time.RFC3339),
	}
	statusMap := map[string]interface{}{
		"status": map[string]interface{}{
			"ownership":          string(ownership),
			"phase":              string(phase),
			"observedGeneration": generation,
			"error":              errMsg,
			"conditions":         []interface{}{cond},
		},
	}
	b, _ := json.Marshal(statusMap)
	return b
}

// resolveStoragePolicyID extracts the SPBM storage policy ID from the PV.
// It first tries pv.spec.csi.volumeAttributes["storagePolicyID"], then falls
// back to the StorageClass parameters["storagePolicyID"] if not found.
func resolveStoragePolicyID(ctx context.Context, c client.Client, pv *corev1.PersistentVolume) string {
	log := logger.GetLogger(ctx).With("pvName", pv.Name)

	if pv.Spec.CSI != nil {
		if id, ok := pv.Spec.CSI.VolumeAttributes["storagePolicyID"]; ok && id != "" {
			log.Infof("resolveStoragePolicyID: found storagePolicyID %q in PV volumeAttributes", id)
			return id
		}
	}

	// Fall back to the StorageClass parameters.
	scName := pv.Spec.StorageClassName
	if scName == "" {
		log.Warnf("resolveStoragePolicyID: PV has no storageClassName; cannot resolve policy ID")
		return ""
	}

	sc := &storagev1.StorageClass{}
	if err := c.Get(ctx, k8stypes.NamespacedName{Name: scName}, sc); err != nil {
		log.Warnf("resolveStoragePolicyID: failed to get StorageClass %q: %v", scName, err)
		return ""
	}

	if id, ok := sc.Parameters["storagePolicyID"]; ok && id != "" {
		log.Infof("resolveStoragePolicyID: found storagePolicyID %q in StorageClass %q", id, scName)
		return id
	}

	log.Warnf("resolveStoragePolicyID: storagePolicyID not found in PV or StorageClass %q", scName)
	return ""
}

// buildStorageProfileSpec returns the CNS storage profile spec for the given policy ID.
// Returns nil if the policy ID is empty.
func buildStorageProfileSpec(storagePolicyID string) []vim25types.BaseVirtualMachineProfileSpec {
	if storagePolicyID == "" {
		return nil
	}
	return []vim25types.BaseVirtualMachineProfileSpec{
		&vim25types.VirtualMachineDefinedProfileSpec{
			ProfileId: storagePolicyID,
		},
	}
}

// getBackoffDuration returns the current backoff for the given name,
// initialising it to 1 second if absent.
func getBackoffDuration(ctx context.Context, name k8stypes.NamespacedName) time.Duration {
	backOffDurationMapMutex.Lock()
	defer backOffDurationMapMutex.Unlock()
	if _, exists := backOffDuration[name]; !exists {
		backOffDuration[name] = time.Second
	}
	return backOffDuration[name]
}

// doubleBackoffDuration doubles the backoff up to MaxBackOffDurationForReconciler.
func doubleBackoffDuration(ctx context.Context, name k8stypes.NamespacedName) {
	d := getBackoffDuration(ctx, name)
	d = min(d*2, cnsoptypes.MaxBackOffDurationForReconciler)
	updateBackoffEntry(ctx, name, d)
}

// updateBackoffEntry sets the backoff for the given name.
func updateBackoffEntry(ctx context.Context, name k8stypes.NamespacedName, duration time.Duration) {
	backOffDurationMapMutex.Lock()
	defer backOffDurationMapMutex.Unlock()
	backOffDuration[name] = duration
}

// deleteBackoffEntry removes the backoff entry for the given name.
func deleteBackoffEntry(ctx context.Context, name k8stypes.NamespacedName) {
	backOffDurationMapMutex.Lock()
	defer backOffDurationMapMutex.Unlock()
	delete(backOffDuration, name)
}
