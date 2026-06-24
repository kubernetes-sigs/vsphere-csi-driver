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

package cnsregistervolume

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	v1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	apitypes "k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	cnsregistervolumev1alpha1 "sigs.k8s.io/vsphere-csi-driver/v3/pkg/apis/cnsoperator/cnsregistervolume/v1alpha1"
	csivolumeinfosvc "sigs.k8s.io/vsphere-csi-driver/v3/pkg/apis/cnsoperator/csivolumeinfo"
	csivolumeinfov1alpha1 "sigs.k8s.io/vsphere-csi-driver/v3/pkg/apis/cnsoperator/csivolumeinfo/v1alpha1"
	"sigs.k8s.io/vsphere-csi-driver/v3/pkg/csi/service/common"
	"sigs.k8s.io/vsphere-csi-driver/v3/pkg/csi/service/common/commonco"
	"sigs.k8s.io/vsphere-csi-driver/v3/pkg/csi/service/logger"
)

const (
	// reasonImportSucceeded is the condition reason set on a CsiVolumeInfo created
	// for an imported VM disk (deferFcdRegistration=true path). The disk starts in
	// VMManaged state with no FCD; the FCD is created lazily at detach time.
	reasonImportSucceeded = "ImportSucceeded"
)

// isPVCBoundFn is a package-level variable so tests can substitute a stub
// without gomonkey. Production code uses the real isPVCBound from util.go.
var isPVCBoundFn = isPVCBound

// reconcileDeferFcdRegistration handles CnsRegisterVolume CRs that carry
// deferFcdRegistration=true. Instead of registering an FCD, it:
//  1. Reads the pre-created PVC (created by vm-operator).
//  2. Derives volumeID = string(pvc.UID) — the synthetic identity used until detach.
//  3. Creates a PV pre-bound to the PVC with volumeHandle = volumeID.
//  4. Waits for PV↔PVC binding.
//  5. Creates a CsiVolumeInfo in VMManaged state with the VM entry and disk metadata.
//  6. Marks the CnsRegisterVolume as registered, recording volumeID and pvName.
//
// No FCD, CNS DB entry, or vCenter call is made. The FCD is created lazily when
// the disk is eventually detached via the CsiVolumeInfo register path.
func (r *ReconcileCnsRegisterVolume) reconcileDeferFcdRegistration(ctx context.Context,
	instance *cnsregistervolumev1alpha1.CnsRegisterVolume,
	request reconcile.Request, timeout time.Duration) (reconcile.Result, error) {

	log := logger.GetLogger(ctx)
	log.Infof("reconcileDeferFcdRegistration: starting for CnsRegisterVolume %q in namespace %q",
		instance.Name, instance.Namespace)

	if !commonco.ContainerOrchestratorUtility.IsFSSEnabled(ctx, common.VMOwnedVolumes) {
		msg := "deferFcdRegistration requires the VMOwnedVolumes feature to be enabled"
		log.Errorf("reconcileDeferFcdRegistration: %s", msg)
		setInstanceError(ctx, r, instance, msg)
		return reconcile.Result{RequeueAfter: timeout}, nil
	}

	if r.csiVolumeInfoService == nil {
		msg := "CsiVolumeInfo service is not initialised; cannot process deferred-FCD import"
		log.Errorf("reconcileDeferFcdRegistration: %s", msg)
		setInstanceError(ctx, r, instance, msg)
		return reconcile.Result{RequeueAfter: timeout}, nil
	}

	// Step 1: read the PVC created by vm-operator.
	pvc, err := r.k8sclient.CoreV1().PersistentVolumeClaims(instance.Namespace).
		Get(ctx, instance.Spec.PvcName, metav1.GetOptions{})
	if err != nil {
		if apierrors.IsNotFound(err) {
			msg := fmt.Sprintf("PVC %q not found in namespace %q; retrying",
				instance.Spec.PvcName, instance.Namespace)
			log.Warnf("reconcileDeferFcdRegistration: %s", msg)
			setInstanceError(ctx, r, instance, msg)
			return reconcile.Result{RequeueAfter: timeout}, nil
		}
		msg := fmt.Sprintf("failed to get PVC %q: %v", instance.Spec.PvcName, err)
		log.Errorf("reconcileDeferFcdRegistration: %s", msg)
		setInstanceError(ctx, r, instance, msg)
		return reconcile.Result{RequeueAfter: timeout}, nil
	}
	log.Infof("reconcileDeferFcdRegistration: read PVC %q (uid=%q) in namespace %q",
		pvc.Name, pvc.UID, pvc.Namespace)

	// Step 2: derive volumeID and pvName from PVC UID.
	volumeID := string(pvc.UID)
	pvName := staticPvNamePrefix + volumeID

	// Step 3: resolve PV parameters from the PVC.
	if pvc.Spec.StorageClassName == nil || *pvc.Spec.StorageClassName == "" {
		msg := fmt.Sprintf("PVC %q has no storageClassName; cannot create PV", pvc.Name)
		log.Errorf("reconcileDeferFcdRegistration: %s", msg)
		setInstanceError(ctx, r, instance, msg)
		return reconcile.Result{RequeueAfter: timeout}, nil
	}
	storageClassName := *pvc.Spec.StorageClassName

	requestStorage, ok := pvc.Spec.Resources.Requests[v1.ResourceStorage]
	if !ok {
		msg := fmt.Sprintf("PVC %q has no storage request; cannot size PV", pvc.Name)
		log.Errorf("reconcileDeferFcdRegistration: %s", msg)
		setInstanceError(ctx, r, instance, msg)
		return reconcile.Result{RequeueAfter: timeout}, nil
	}
	capacity := requestStorage.DeepCopy()

	accessMode := instance.Spec.AccessMode
	if accessMode == "" {
		accessMode = v1.ReadWriteOnce
	}

	volumeMode := instance.Spec.VolumeMode
	if volumeMode == "" && pvc.Spec.VolumeMode != nil {
		volumeMode = *pvc.Spec.VolumeMode
	}

	// Step 4: create the PV if it does not already exist.
	existingPV, err := r.k8sclient.CoreV1().PersistentVolumes().Get(ctx, pvName, metav1.GetOptions{})
	if err != nil && !apierrors.IsNotFound(err) {
		msg := fmt.Sprintf("failed to check for existing PV %q: %v", pvName, err)
		log.Errorf("reconcileDeferFcdRegistration: %s", msg)
		setInstanceError(ctx, r, instance, msg)
		return reconcile.Result{RequeueAfter: timeout}, nil
	}

	if apierrors.IsNotFound(err) {
		claimRef := &v1.ObjectReference{
			Kind:       "PersistentVolumeClaim",
			APIVersion: "v1",
			Namespace:  instance.Namespace,
			Name:       instance.Spec.PvcName,
			UID:        pvc.UID,
		}
		pvSpec := getPersistentVolumeSpec(pvName, volumeID, capacity,
			accessMode, volumeMode, storageClassName, claimRef,
			instance.Namespace, instance.Name)
		createdPV, createErr := r.k8sclient.CoreV1().PersistentVolumes().Create(ctx, pvSpec, metav1.CreateOptions{})
		if createErr != nil {
			msg := fmt.Sprintf("failed to create PV %q: %v", pvName, createErr)
			log.Errorf("reconcileDeferFcdRegistration: %s", msg)
			setInstanceError(ctx, r, instance, msg)
			return reconcile.Result{RequeueAfter: timeout}, nil
		}
		existingPV = createdPV
		log.Infof("reconcileDeferFcdRegistration: created PV %q (volumeHandle=%q)", pvName, volumeID)
	} else {
		log.Infof("reconcileDeferFcdRegistration: PV %q already exists, reusing", pvName)
	}

	// Step 5: wait for PV↔PVC binding.
	bound, bindErr := isPVCBoundFn(ctx, r.k8sclient, pvc, time.Minute)
	if !bound {
		msg := fmt.Sprintf("PVC %q did not bind within timeout: %v", pvc.Name, bindErr)
		log.Errorf("reconcileDeferFcdRegistration: %s", msg)
		setInstanceError(ctx, r, instance, msg)
		return reconcile.Result{RequeueAfter: timeout}, nil
	}
	log.Infof("reconcileDeferFcdRegistration: PVC %q is bound to PV %q", pvc.Name, pvName)

	// Step 6: create CsiVolumeInfo in VMManaged state.
	if err := r.ensureCsiVolumeInfoForImport(ctx, instance, volumeID, pvName, existingPV); err != nil {
		msg := fmt.Sprintf("failed to ensure CsiVolumeInfo for volumeID %q: %v", volumeID, err)
		log.Errorf("reconcileDeferFcdRegistration: %s", msg)
		setInstanceError(ctx, r, instance, msg)
		return reconcile.Result{RequeueAfter: timeout}, nil
	}
	log.Infof("reconcileDeferFcdRegistration: CsiVolumeInfo ready for volumeID %q (VMManaged)", volumeID)

	// Step 7: mark the CnsRegisterVolume as successfully registered.
	setInstanceOwnerRef(instance, instance.Spec.PvcName, pvc.UID)
	if err := updateCnsRegisterVolume(ctx, r.client, instance); err != nil {
		msg := fmt.Sprintf("failed to set ownerRef on CnsRegisterVolume: %v", err)
		log.Errorf("reconcileDeferFcdRegistration: %s", msg)
		setInstanceError(ctx, r, instance, msg)
		return reconcile.Result{RequeueAfter: timeout}, nil
	}

	// Re-fetch after metadata update to avoid stale resourceVersion on status patch.
	if err := r.client.Get(ctx, apitypes.NamespacedName{
		Name:      instance.Name,
		Namespace: instance.Namespace,
	}, instance); err != nil {
		msg := fmt.Sprintf("failed to re-fetch CnsRegisterVolume after ownerRef update: %v", err)
		log.Errorf("reconcileDeferFcdRegistration: %s", msg)
		setInstanceError(ctx, r, instance, msg)
		return reconcile.Result{RequeueAfter: timeout}, nil
	}

	origInstance := instance.DeepCopy()
	instance.Status.Registered = true
	instance.Status.Error = ""
	instance.Status.VolumeID = volumeID
	instance.Status.PvName = pvName
	if err := patchCnsRegisterVolumeStatus(ctx, r.client, origInstance, instance); err != nil {
		msg := fmt.Sprintf("failed to patch CnsRegisterVolume status: %v", err)
		log.Errorf("reconcileDeferFcdRegistration: %s", msg)
		return reconcile.Result{RequeueAfter: timeout}, nil
	}

	backOffDurationMapMutex.Lock()
	delete(backOffDuration, request.NamespacedName)
	backOffDurationMapMutex.Unlock()

	successMsg := fmt.Sprintf("Successfully imported disk as VM-managed volume %q in namespace %q",
		volumeID, instance.Namespace)
	recordEvent(ctx, r, instance, v1.EventTypeNormal, successMsg)
	log.Infof("reconcileDeferFcdRegistration: %s", successMsg)
	return reconcile.Result{}, nil
}

// ensureCsiVolumeInfoForImport creates (or verifies) the CsiVolumeInfo CR for an
// imported disk. The CVI is created directly in VMManaged state — there is no prior
// CSIManaged state for imported disks. If the CVI already exists (idempotent retry),
// its status is patched to VMManaged/Succeeded regardless.
func (r *ReconcileCnsRegisterVolume) ensureCsiVolumeInfoForImport(ctx context.Context,
	instance *cnsregistervolumev1alpha1.CnsRegisterVolume,
	volumeID, pvName string, pv *v1.PersistentVolume) error {

	log := logger.GetLogger(ctx)

	blockOwnerDeletion := true
	cvi := &csivolumeinfov1alpha1.CsiVolumeInfo{
		ObjectMeta: metav1.ObjectMeta{
			Name:      csivolumeinfosvc.GetCsiVolumeInfoCRName(volumeID),
			Namespace: csivolumeinfov1alpha1.CVINamespace,
			Finalizers: []string{
				csivolumeinfov1alpha1.VolumeProtectionFinalizer,
			},
			OwnerReferences: []metav1.OwnerReference{
				{
					APIVersion:         "v1",
					Kind:               "PersistentVolume",
					Name:               pv.Name,
					UID:                pv.UID,
					BlockOwnerDeletion: &blockOwnerDeletion,
				},
			},
		},
		Spec: csivolumeinfov1alpha1.CsiVolumeInfoSpec{
			VolumeID:     volumeID,
			PVCName:      instance.Spec.PvcName,
			PVCNamespace: instance.Namespace,
			PVName:       pvName,
			DiskUUID:     instance.Spec.DiskUUID,
			DiskPath:     instance.Spec.DiskURLPath,
			VMs: []csivolumeinfov1alpha1.VirtualMachineRef{
				{
					VMName:         instance.Spec.VMName,
					VMInstanceUUID: instance.Spec.VMInstanceUUID,
				},
			},
		},
	}

	if err := r.csiVolumeInfoService.CreateCsiVolumeInfo(ctx, cvi); err != nil {
		return fmt.Errorf("CreateCsiVolumeInfo: %w", err)
	}
	log.Infof("ensureCsiVolumeInfoForImport: CsiVolumeInfo %q created or already exists for volumeID %q",
		cvi.Name, volumeID)

	// Patch status to VMManaged/Succeeded. This is idempotent — patching again on
	// a retry is safe because the status subresource is always overwritten.
	statusPatch := buildImportStatusPatch(cvi.Generation)
	if err := r.csiVolumeInfoService.PatchCsiVolumeInfoStatus(ctx, volumeID, statusPatch); err != nil {
		return fmt.Errorf("PatchCsiVolumeInfoStatus: %w", err)
	}
	log.Infof("ensureCsiVolumeInfoForImport: status patched to VMManaged/Succeeded for volumeID %q", volumeID)
	return nil
}

// buildImportStatusPatch returns the JSON merge-patch bytes for the CsiVolumeInfo
// status subresource, setting ownership=VMManaged, phase=Succeeded, and the
// ImportSucceeded condition. generation is the current spec generation of the CVI;
// after creation it is typically 1.
func buildImportStatusPatch(generation int64) []byte {
	cond := map[string]interface{}{
		"type":               "Ready",
		"status":             string(metav1.ConditionTrue),
		"reason":             reasonImportSucceeded,
		"message":            "",
		"lastTransitionTime": metav1.Now().UTC().Format(time.RFC3339),
	}
	statusMap := map[string]interface{}{
		"status": map[string]interface{}{
			"ownership":          string(csivolumeinfov1alpha1.OwnershipStateVMManaged),
			"phase":              string(csivolumeinfov1alpha1.PhaseSucceeded),
			"observedGeneration": generation,
			"error":              "",
			"conditions":         []interface{}{cond},
		},
	}
	b, _ := json.Marshal(statusMap)
	return b
}
