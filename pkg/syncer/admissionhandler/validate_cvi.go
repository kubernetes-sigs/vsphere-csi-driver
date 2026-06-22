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

package admissionhandler

import (
	"context"
	"encoding/json"
	"fmt"

	admissionv1 "k8s.io/api/admission/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	snap "github.com/kubernetes-csi/external-snapshotter/client/v8/apis/volumesnapshot/v1"

	csivolumeinfosvc "sigs.k8s.io/vsphere-csi-driver/v3/pkg/apis/cnsoperator/csivolumeinfo"
	csivolumeinfov1alpha1 "sigs.k8s.io/vsphere-csi-driver/v3/pkg/apis/cnsoperator/csivolumeinfo/v1alpha1"
	"sigs.k8s.io/vsphere-csi-driver/v3/pkg/csi/service/logger"
	csitypes "sigs.k8s.io/vsphere-csi-driver/v3/pkg/csi/types"
)

const (
	// DeleteVMOwnedVolumeErrorMessageFormat is the rejection message returned when
	// a PVC delete is attempted while the volume is VM-managed. It takes the PVC
	// name as its single argument.
	DeleteVMOwnedVolumeErrorMessageFormat = "Cannot delete PVC %s: volume is VM-managed. " +
		"Detach the volume from the VM or delete all retaining snapshots first."

	// SnapshotVMOwnedVolumeErrorMessageFormat is the rejection message returned when
	// a VolumeSnapshot create is attempted while the volume is VM-managed. It takes
	// the PVC name as its single argument.
	SnapshotVMOwnedVolumeErrorMessageFormat = "Cannot create snapshot for PVC %s: volume is VM-managed. " +
		"Detach the volume from the VM or delete all retaining snapshots first."
)

// validatePVCDeletionForVMOwnedVolumes checks whether a PVC being deleted has
// a corresponding CsiVolumeInfo in VMManaged state. If so, the deletion is
// rejected to prevent data loss while a VM holds the disk.
//
// This guard is a no-op when the VMOwnedVolumes feature state is disabled,
// when the CsiVolumeInfo cannot be resolved (fail-open to avoid blocking
// unrelated deletes), or when the volume is already CSI-managed.
func validatePVCDeletionForVMOwnedVolumes(
	ctx context.Context, req *admissionv1.AdmissionRequest) *admissionv1.AdmissionResponse {
	log := logger.GetLogger(ctx)

	if !featureGateVMOwnedVolumesEnabled {
		return &admissionv1.AdmissionResponse{Allowed: true}
	}

	if req.Operation != admissionv1.Delete {
		return &admissionv1.AdmissionResponse{Allowed: true}
	}

	// Decode the PVC being deleted.
	pvc := &corev1.PersistentVolumeClaim{}
	if err := json.Unmarshal(req.OldObject.Raw, pvc); err != nil {
		log.Warnf("validatePVCDeletionForVMOwnedVolumes: failed to decode PVC from request: %v; "+
			"allowing deletion (fail-open)", err)
		return &admissionv1.AdmissionResponse{Allowed: true}
	}

	// Only examine bound PVCs that have a named PV.
	if pvc.Status.Phase != corev1.ClaimBound || pvc.Spec.VolumeName == "" {
		return &admissionv1.AdmissionResponse{Allowed: true}
	}

	k8sclient, err := newK8sClient(ctx)
	if err != nil {
		log.Warnf("validatePVCDeletionForVMOwnedVolumes: failed to create k8s client: %v; "+
			"allowing deletion (fail-open)", err)
		return &admissionv1.AdmissionResponse{Allowed: true}
	}

	// Resolve PV → volumeHandle (O(1) Get by name).
	pv, err := k8sclient.CoreV1().PersistentVolumes().Get(ctx, pvc.Spec.VolumeName, metav1.GetOptions{})
	if err != nil {
		log.Warnf("validatePVCDeletionForVMOwnedVolumes: failed to get PV %q for PVC %s/%s: %v; "+
			"allowing deletion (fail-open)", pvc.Spec.VolumeName, pvc.Namespace, pvc.Name, err)
		return &admissionv1.AdmissionResponse{Allowed: true}
	}

	if pv.Spec.CSI == nil || pv.Spec.CSI.Driver != csitypes.Name {
		return &admissionv1.AdmissionResponse{Allowed: true}
	}

	volumeID := pv.Spec.CSI.VolumeHandle
	if volumeID == "" {
		return &admissionv1.AdmissionResponse{Allowed: true}
	}

	// Resolve CsiVolumeInfo by deterministic name (O(1) Get).
	cviSvc, err := csivolumeinfosvc.InitCsiVolumeInfoService(ctx)
	if err != nil {
		log.Warnf("validatePVCDeletionForVMOwnedVolumes: failed to init CVI service for PVC %s/%s: %v; "+
			"allowing deletion (fail-open)", pvc.Namespace, pvc.Name, err)
		return &admissionv1.AdmissionResponse{Allowed: true}
	}

	cvi, err := cviSvc.GetCsiVolumeInfo(ctx, volumeID)
	if err != nil {
		log.Warnf("validatePVCDeletionForVMOwnedVolumes: failed to get CsiVolumeInfo for volume %q: %v; "+
			"allowing deletion (fail-open)", volumeID, err)
		return &admissionv1.AdmissionResponse{Allowed: true}
	}

	if cvi == nil || cvi.Status.Ownership != csivolumeinfov1alpha1.OwnershipStateVMManaged {
		return &admissionv1.AdmissionResponse{Allowed: true}
	}

	// Volume is VM-managed — reject the deletion.
	log.Infof("validatePVCDeletionForVMOwnedVolumes: rejecting delete of PVC %s/%s "+
		"(volumeID=%q, ownership=VMManaged)", pvc.Namespace, pvc.Name, volumeID)
	msg := fmt.Sprintf(DeleteVMOwnedVolumeErrorMessageFormat, pvc.Name)
	return &admissionv1.AdmissionResponse{
		Allowed: false,
		Result: &metav1.Status{
			Message: msg,
			Reason:  metav1.StatusReasonForbidden,
			Code:    403,
		},
	}
}

// validateSnapshotCreateForVMOwnedVolumes rejects VolumeSnapshot CREATE requests
// when the source PVC's volume has ownership=VMManaged. The disk is a plain VMDK
// at that point — no FCD entry exists in CNS — so an FCD snapshot would fail or
// produce an inconsistent result.
//
// Fail-open on resolution errors to avoid blocking unrelated snapshot creates.
func validateSnapshotCreateForVMOwnedVolumes(
	ctx context.Context, req *admissionv1.AdmissionRequest) *admissionv1.AdmissionResponse {
	log := logger.GetLogger(ctx)

	if !featureGateVMOwnedVolumesEnabled {
		return &admissionv1.AdmissionResponse{Allowed: true}
	}

	if req.Operation != admissionv1.Create {
		return &admissionv1.AdmissionResponse{Allowed: true}
	}

	vs := &snap.VolumeSnapshot{}
	if err := json.Unmarshal(req.Object.Raw, vs); err != nil {
		log.Warnf("validateSnapshotCreateForVMOwnedVolumes: failed to decode VolumeSnapshot: %v; "+
			"allowing (fail-open)", err)
		return &admissionv1.AdmissionResponse{Allowed: true}
	}

	// Only examine snapshots that name an explicit PVC source.
	if vs.Spec.Source.PersistentVolumeClaimName == nil || *vs.Spec.Source.PersistentVolumeClaimName == "" {
		return &admissionv1.AdmissionResponse{Allowed: true}
	}
	pvcName := *vs.Spec.Source.PersistentVolumeClaimName
	pvcNamespace := req.Namespace

	k8sclient, err := newK8sClient(ctx)
	if err != nil {
		log.Warnf("validateSnapshotCreateForVMOwnedVolumes: failed to create k8s client: %v; "+
			"allowing (fail-open)", err)
		return &admissionv1.AdmissionResponse{Allowed: true}
	}

	pvc, err := k8sclient.CoreV1().PersistentVolumeClaims(pvcNamespace).Get(ctx, pvcName, metav1.GetOptions{})
	if err != nil {
		log.Warnf("validateSnapshotCreateForVMOwnedVolumes: failed to get PVC %s/%s: %v; "+
			"allowing (fail-open)", pvcNamespace, pvcName, err)
		return &admissionv1.AdmissionResponse{Allowed: true}
	}

	if pvc.Status.Phase != corev1.ClaimBound || pvc.Spec.VolumeName == "" {
		return &admissionv1.AdmissionResponse{Allowed: true}
	}

	pv, err := k8sclient.CoreV1().PersistentVolumes().Get(ctx, pvc.Spec.VolumeName, metav1.GetOptions{})
	if err != nil {
		log.Warnf("validateSnapshotCreateForVMOwnedVolumes: failed to get PV %q for PVC %s/%s: %v; "+
			"allowing (fail-open)", pvc.Spec.VolumeName, pvcNamespace, pvcName, err)
		return &admissionv1.AdmissionResponse{Allowed: true}
	}

	if pv.Spec.CSI == nil || pv.Spec.CSI.Driver != csitypes.Name {
		return &admissionv1.AdmissionResponse{Allowed: true}
	}

	volumeID := pv.Spec.CSI.VolumeHandle
	if volumeID == "" {
		return &admissionv1.AdmissionResponse{Allowed: true}
	}

	cviSvc, err := csivolumeinfosvc.InitCsiVolumeInfoService(ctx)
	if err != nil {
		log.Warnf("validateSnapshotCreateForVMOwnedVolumes: failed to init CVI service for PVC %s/%s: %v; "+
			"allowing (fail-open)", pvcNamespace, pvcName, err)
		return &admissionv1.AdmissionResponse{Allowed: true}
	}

	cvi, err := cviSvc.GetCsiVolumeInfo(ctx, volumeID)
	if err != nil {
		log.Warnf("validateSnapshotCreateForVMOwnedVolumes: failed to get CsiVolumeInfo for volume %q: %v; "+
			"allowing (fail-open)", volumeID, err)
		return &admissionv1.AdmissionResponse{Allowed: true}
	}

	if cvi == nil || cvi.Status.Ownership != csivolumeinfov1alpha1.OwnershipStateVMManaged {
		return &admissionv1.AdmissionResponse{Allowed: true}
	}

	log.Infof("validateSnapshotCreateForVMOwnedVolumes: rejecting VolumeSnapshot %s/%s for PVC %s/%s "+
		"(volumeID=%q, ownership=VMManaged)", req.Namespace, vs.Name, pvcNamespace, pvcName, volumeID)
	msg := fmt.Sprintf(SnapshotVMOwnedVolumeErrorMessageFormat, pvcName)
	return &admissionv1.AdmissionResponse{
		Allowed: false,
		Result: &metav1.Status{
			Message: msg,
			Reason:  metav1.StatusReasonForbidden,
			Code:    403,
		},
	}
}
