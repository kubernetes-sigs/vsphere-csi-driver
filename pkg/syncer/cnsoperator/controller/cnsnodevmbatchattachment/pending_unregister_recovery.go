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

package cnsnodevmbatchattachment

import (
	"context"
	"fmt"

	corev1 "k8s.io/api/core/v1"
	k8stypes "k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"

	bav1alpha1 "sigs.k8s.io/vsphere-csi-driver/v3/pkg/apis/cnsoperator/cnsnodevmbatchattachment/v1alpha1"
	csivolumeinfo "sigs.k8s.io/vsphere-csi-driver/v3/pkg/apis/cnsoperator/csivolumeinfo"
	csivolumeinfov1alpha1 "sigs.k8s.io/vsphere-csi-driver/v3/pkg/apis/cnsoperator/csivolumeinfo/v1alpha1"
	volumes "sigs.k8s.io/vsphere-csi-driver/v3/pkg/common/cns-lib/volume"
	"sigs.k8s.io/vsphere-csi-driver/v3/pkg/csi/service/logger"
)

// RecoverPendingUnregisters is called once at CSI startup to resume any two-phase
// FCD unregister operations that were interrupted by a crash (Layer-1 recovery).
//
// It calls QueryPendingUnregisters to retrieve all outstanding PENDING_UNREGISTER
// records from the CNS database. For each record, it ensures the CVI is in
// TRANSFERRING_TO_VM state, writes the BA status with AttachMethod=Reconfig, and
// acknowledges the record via AckUnregister so the CNS row is cleaned up.
//
// This function is idempotent: re-running on the same record is safe because
// each step is guarded with state checks. Errors for individual records are
// logged but do not abort recovery for subsequent records.
func RecoverPendingUnregisters(ctx context.Context,
	volumeManager volumes.Manager,
	cviSvc csivolumeinfo.CsiVolumeInfoService,
	k8sClient client.Client,
) error {
	log := logger.GetLogger(ctx)
	log.Infof("RecoverPendingUnregisters: scanning for incomplete FCD unregister operations")

	records, err := volumeManager.QueryPendingUnregisters(ctx)
	if err != nil {
		return fmt.Errorf("RecoverPendingUnregisters: QueryPendingUnregisters failed: %w", err)
	}
	if len(records) == 0 {
		log.Infof("RecoverPendingUnregisters: no pending unregister records found")
		return nil
	}
	log.Infof("RecoverPendingUnregisters: found %d pending unregister record(s)", len(records))

	recovered := 0
	for _, rec := range records {
		recErr := recoverSinglePendingUnregister(ctx, volumeManager, cviSvc, k8sClient, rec)
		if recErr != nil {
			log.Errorf("RecoverPendingUnregisters: failed to recover volume %q: %v",
				rec.VolumeID, recErr)
			// Continue with remaining records.
			continue
		}
		recovered++
	}
	log.Infof("RecoverPendingUnregisters: successfully recovered %d of %d record(s)",
		recovered, len(records))
	return nil
}

// recoverSinglePendingUnregister handles crash recovery for one PENDING_UNREGISTER record.
func recoverSinglePendingUnregister(
	ctx context.Context,
	volumeManager volumes.Manager,
	cviSvc csivolumeinfo.CsiVolumeInfoService,
	k8sClient client.Client,
	rec volumes.PendingUnregisterRecord,
) error {
	log := logger.GetLogger(ctx)
	log.Infof("recoverSinglePendingUnregister: recovering volume %q (diskPath=%q, diskUUID=%q)",
		rec.VolumeID, rec.BackingDiskPath, rec.DiskUUID)

	// Resolve PVC namespace from the PV's ClaimRef.
	pv, pvcNamespace, err := findPVAndNamespaceForVolume(ctx, k8sClient, rec.VolumeID)
	if err != nil {
		// PV not found: volume was deleted while CSI was down. ACK to clean up.
		log.Warnf("recoverSinglePendingUnregister: PV not found for volume %q: %v; "+
			"ACKing pending record to clean up CNS row", rec.VolumeID, err)
		return volumeManager.AckUnregister(ctx, rec.VolumeID)
	}
	_ = pv

	// Look up the CVI.
	cvi, err := cviSvc.GetCsiVolumeInfo(ctx, pvcNamespace, rec.VolumeID)
	if err != nil {
		return fmt.Errorf("recoverSinglePendingUnregister: failed to get CVI for volume %q: %w",
			rec.VolumeID, err)
	}
	if cvi == nil {
		// No CVI: brownfield volume or CVI already cleaned up. ACK the record.
		log.Warnf("recoverSinglePendingUnregister: no CVI found for volume %q; "+
			"ACKing to clean up CNS row", rec.VolumeID)
		return volumeManager.AckUnregister(ctx, rec.VolumeID)
	}

	// If CVI is already TRANSFERRING_TO_VM, the crash happened after the CVI patch.
	// Skip the CVI update and proceed to write BA status and ACK.
	if cvi.Status.OwnershipState == csivolumeinfov1alpha1.OwnershipStateCSIManaged {
		// Crash happened between CNS unregister and CVI patch. Look up the BA to
		// find vmName and vmInstanceUUID, then advance CVI to TRANSFERRING_TO_VM.
		vmName, vmInstanceUUID, baErr := findVMForVolume(ctx, k8sClient, pvcNamespace, cvi.Spec.PVCName)
		if baErr != nil {
			log.Warnf("recoverSinglePendingUnregister: cannot find BA for volume %q "+
				"to determine VM name: %v; skipping CVI advance", rec.VolumeID, baErr)
		} else if vmName != "" {
			cvi.Status.OwnershipState = csivolumeinfov1alpha1.OwnershipStateTransferringToVM
			cvi.Status.VMName = vmName
			cvi.Status.VMInstanceUUID = vmInstanceUUID
			if rec.BackingDiskPath != "" {
				cvi.Status.DiskPath = rec.BackingDiskPath
			}
			if rec.DiskUUID != "" {
				cvi.Status.DiskUUID = rec.DiskUUID
			}
			if updateErr := cviSvc.UpdateCsiVolumeInfoStatus(ctx, cvi); updateErr != nil {
				return fmt.Errorf("recoverSinglePendingUnregister: failed to advance CVI for %q "+
					"to TRANSFERRING_TO_VM: %w", rec.VolumeID, updateErr)
			}
			log.Infof("recoverSinglePendingUnregister: advanced CVI for volume %q to "+
				"TRANSFERRING_TO_VM (vmName=%s)", rec.VolumeID, vmName)
		}
	} else if cvi.Status.OwnershipState != csivolumeinfov1alpha1.OwnershipStateTransferringToVM {
		// CVI is in an unexpected state (e.g., already CSI-managed after a race).
		// ACK the record to clean up the stale CNS row.
		log.Warnf("recoverSinglePendingUnregister: CVI for volume %q is in state %q "+
			"(expected CSI_MANAGED or TRANSFERRING_TO_VM); ACKing to clean up CNS row",
			rec.VolumeID, cvi.Status.OwnershipState)
		return volumeManager.AckUnregister(ctx, rec.VolumeID)
	}

	// Write BA status with AttachMethod=Reconfig so vm-operator knows to proceed.
	if writeErr := writeBAStatusForRecovery(ctx, k8sClient, pvcNamespace, cvi, rec); writeErr != nil {
		log.Warnf("recoverSinglePendingUnregister: failed to write BA status for volume %q: %v; "+
			"Layer-2 reconcile-time check will handle this", rec.VolumeID, writeErr)
		// Do not block ACK on BA status failure.
	}

	// ACK the CNS row to delete the PENDING_UNREGISTER record.
	if ackErr := volumeManager.AckUnregister(ctx, rec.VolumeID); ackErr != nil {
		return fmt.Errorf("recoverSinglePendingUnregister: AckUnregister failed for volume %q: %w",
			rec.VolumeID, ackErr)
	}
	log.Infof("recoverSinglePendingUnregister: completed recovery for volume %q", rec.VolumeID)
	return nil
}

// findPVAndNamespaceForVolume looks up the PV by its CSI volume handle and returns
// the PV object along with the PVC namespace from its ClaimRef.
func findPVAndNamespaceForVolume(ctx context.Context,
	k8sClient client.Client, volumeID string,
) (*corev1.PersistentVolume, string, error) {
	// The deterministic PV name for CSI volumes matches the volume handle pattern.
	// Attempt a direct Get by volume handle as PV name first.
	pv := &corev1.PersistentVolume{}
	if err := k8sClient.Get(ctx, k8stypes.NamespacedName{Name: volumeID}, pv); err == nil {
		if pv.Spec.ClaimRef != nil {
			return pv, pv.Spec.ClaimRef.Namespace, nil
		}
	}

	// The PV name may differ from volumeID (e.g., "pvc-<uuid>"). List is not
	// viable at scale. Use the CO utility's volumeID-to-PVC map as a fallback;
	// that map is populated by the informer at startup.
	_, pvcNS, ok := getPVCNamespaceFromCOUtility(volumeID)
	if ok && pvcNS != "" {
		// Look up PV by volume handle via field selector (server-side indexed).
		pvList := &corev1.PersistentVolumeList{}
		if listErr := k8sClient.List(ctx, pvList,
			client.MatchingFields{"spec.csi.volumeHandle": volumeID}); listErr == nil {
			for i := range pvList.Items {
				pv := &pvList.Items[i]
				if pv.Spec.ClaimRef != nil {
					return pv, pv.Spec.ClaimRef.Namespace, nil
				}
			}
		}
		// Return the namespace from the CO map even without the PV object.
		return nil, pvcNS, nil
	}

	return nil, "", fmt.Errorf("PV not found for volumeID %q", volumeID)
}

// getPVCNamespaceFromCOUtility delegates to the CO utility's GetPVCNameFromCSIVolumeID.
// Defined as a package-level variable so unit tests can substitute a fake.
var getPVCNamespaceFromCOUtility = func(volumeID string) (string, string, bool) {
	return "", "", false
}

// findVMForVolume looks up the BA in the given namespace that contains the given
// PVC and returns the vmName (VM CR name) and vmInstanceUUID.
func findVMForVolume(ctx context.Context,
	k8sClient client.Client, namespace, pvcName string,
) (string, string, error) {
	baList := &bav1alpha1.CnsNodeVMBatchAttachmentList{}
	if err := k8sClient.List(ctx, baList, client.InNamespace(namespace)); err != nil {
		return "", "", fmt.Errorf("failed to list BAs in namespace %q: %w", namespace, err)
	}
	for i := range baList.Items {
		ba := &baList.Items[i]
		for _, vol := range ba.Spec.Volumes {
			if vol.PersistentVolumeClaim.ClaimName == pvcName {
				// The VM CR name is the BA name; instanceUUID is in the spec.
				return ba.Name, ba.Spec.InstanceUUID, nil
			}
		}
	}
	return "", "", nil
}

// writeBAStatusForRecovery finds the BA for this volume and writes the
// AttachMethod=Reconfig condition and disk path/UUID to the BA status.
func writeBAStatusForRecovery(ctx context.Context,
	k8sClient client.Client,
	pvcNamespace string,
	cvi *csivolumeinfov1alpha1.CsiVolumeInfo,
	rec volumes.PendingUnregisterRecord,
) error {
	log := logger.GetLogger(ctx)

	baList := &bav1alpha1.CnsNodeVMBatchAttachmentList{}
	if err := k8sClient.List(ctx, baList, client.InNamespace(pvcNamespace)); err != nil {
		return fmt.Errorf("failed to list BAs: %w", err)
	}

	for i := range baList.Items {
		ba := &baList.Items[i]
		for _, vol := range ba.Spec.Volumes {
			if vol.PersistentVolumeClaim.ClaimName != cvi.Spec.PVCName {
				continue
			}
			if hasAttachMethodReconfig(ba, vol.Name) {
				log.Infof("writeBAStatusForRecovery: BA %s/%s already has AttachMethod=Reconfig "+
					"for volume %q; skipping", ba.Namespace, ba.Name, rec.VolumeID)
				return nil
			}
			diskPath := rec.BackingDiskPath
			if diskPath == "" {
				diskPath = cvi.Status.DiskPath
			}
			diskUUID := rec.DiskUUID
			if diskUUID == "" {
				diskUUID = cvi.Status.DiskUUID
			}
			setBAVolumeAttachMethodReconfig(ctx, ba, vol.Name, cvi.Spec.PVCName,
				rec.VolumeID, diskUUID, diskPath)
			if patchErr := patchBAStatus(ctx, k8sClient, ba); patchErr != nil {
				return fmt.Errorf("failed to patch BA status for %s/%s: %w",
					ba.Namespace, ba.Name, patchErr)
			}
			log.Infof("writeBAStatusForRecovery: wrote AttachMethod=Reconfig to BA %s/%s "+
				"for volume %q", ba.Namespace, ba.Name, rec.VolumeID)
			return nil
		}
	}
	return nil
}
