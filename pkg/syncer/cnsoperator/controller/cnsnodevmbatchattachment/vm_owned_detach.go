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

	"sigs.k8s.io/controller-runtime/pkg/client"

	bav1alpha1 "sigs.k8s.io/vsphere-csi-driver/v3/pkg/apis/cnsoperator/cnsnodevmbatchattachment/v1alpha1"
	csivolumeinfo "sigs.k8s.io/vsphere-csi-driver/v3/pkg/apis/cnsoperator/csivolumeinfo"
	csivolumeinfov1alpha1 "sigs.k8s.io/vsphere-csi-driver/v3/pkg/apis/cnsoperator/csivolumeinfo/v1alpha1"
	volumes "sigs.k8s.io/vsphere-csi-driver/v3/pkg/common/cns-lib/volume"
	cnsvsphere "sigs.k8s.io/vsphere-csi-driver/v3/pkg/common/cns-lib/vsphere"
	"sigs.k8s.io/vsphere-csi-driver/v3/pkg/common/config"
	"sigs.k8s.io/vsphere-csi-driver/v3/pkg/csi/service/logger"
)

// reconcileVMOwnedVolumesDetach handles the ownership-transfer detach path
// for a single volume on a VM with the VMOwnedVolumes annotation. The caller
// is responsible for checking the annotation before calling this function;
// it is never mixed with the legacy DetachVolume path.
func reconcileVMOwnedVolumesDetach(
	ctx context.Context,
	cviSvc csivolumeinfo.CsiVolumeInfoService,
	volumeManager volumes.Manager,
	k8sClient client.Client,
	vm *cnsvsphere.VirtualMachine,
	instance *bav1alpha1.CnsNodeVMBatchAttachment,
	pvcName, volumeID string,
	configInfo *config.ConfigurationInfo,
) error {
	log := logger.GetLogger(ctx)
	log.Infof("reconcileVMOwnedVolumesDetach: starting for PVC %s (volumeID=%s) on VM instanceUUID=%s",
		pvcName, volumeID, instance.Spec.InstanceUUID)

	cvi, err := cviSvc.GetCsiVolumeInfo(ctx, instance.Namespace, volumeID)
	if err != nil {
		return fmt.Errorf("reconcileVMOwnedVolumesDetach: failed to get CVI for %q: %w",
			volumeID, err)
	}
	if cvi == nil {
		// A VM with the VMOwnedVolumes annotation must have a CVI for every
		// volume. Its absence indicates a provisioning inconsistency.
		return fmt.Errorf("reconcileVMOwnedVolumesDetach: CVI not found for volume %q on VM instanceUUID=%s",
			volumeID, instance.Spec.InstanceUUID)
	}

	// If the CVI is already CSI_MANAGED the volume has been fully returned to
	// CSI — treat this as a no-op (idempotent retry).
	if cvi.Status.OwnershipState == csivolumeinfov1alpha1.OwnershipStateCSIManaged {
		log.Infof("reconcileVMOwnedVolumesDetach: CVI for volume %q is already CSI_MANAGED; "+
			"removing from BA status (idempotent)", volumeID)
		deleteVolumeFromStatus(pvcName, instance)
		return nil
	}

	// If the CVI is not yet TRANSFERRING_TO_CSI, advance it to that state.
	// This covers the mid-attach cancellation case (CVI is TRANSFERRING_TO_VM,
	// disk never landed on the VM) and the snapshot-revert case (CVI is
	// VM_MANAGED but vSphere already removed the disk from the VM).
	if cvi.Status.OwnershipState != csivolumeinfov1alpha1.OwnershipStateTransferringToCSI {
		log.Infof("reconcileVMOwnedVolumesDetach: CVI for volume %q is in state %q; "+
			"advancing to TRANSFERRING_TO_CSI",
			volumeID, cvi.Status.OwnershipState)
		cvi.Status.OwnershipState = csivolumeinfov1alpha1.OwnershipStateTransferringToCSI
		if updateErr := cviSvc.UpdateCsiVolumeInfoStatus(ctx, cvi); updateErr != nil {
			return fmt.Errorf("reconcileVMOwnedVolumesDetach: failed to advance CVI for %q "+
				"to TRANSFERRING_TO_CSI: %w", volumeID, updateErr)
		}
	}

	// Query the vCenter snapshot tree to check whether any remaining snapshot
	// still references this disk.
	diskUUID := cvi.Status.DiskUUID
	if diskUUID == "" {
		return fmt.Errorf("reconcileVMOwnedVolumesDetach: CVI for volume %q has empty diskUUID; "+
			"cannot check snapshot retention", volumeID)
	}

	retained, snapErr := IsVolumeRetainedByVCenterSnapshot(ctx, vm, diskUUID)
	if snapErr != nil {
		return fmt.Errorf("reconcileVMOwnedVolumesDetach: snapshot retention check failed for "+
			"volume %q (diskUUID=%s): %w", volumeID, diskUUID, snapErr)
	}

	if retained {
		// At least one snapshot still references the disk; mark the CVI as
		// snapshot-retained: VM_MANAGED state with empty vmName.
		log.Infof("reconcileVMOwnedVolumesDetach: diskUUID %s is retained by a snapshot; "+
			"marking CVI for volume %q as snapshot-retained", diskUUID, volumeID)
		cvi.Status.OwnershipState = csivolumeinfov1alpha1.OwnershipStateVMManaged
		cvi.Status.VMName = ""
		cvi.Status.VMInstanceUUID = ""
		if updateErr := cviSvc.UpdateCsiVolumeInfoStatus(ctx, cvi); updateErr != nil {
			return fmt.Errorf("reconcileVMOwnedVolumesDetach: failed to update CVI for %q "+
				"to snapshot-retained state: %w", volumeID, updateErr)
		}
		if labelErr := PatchPVCOwnershipLabel(ctx, k8sClient, instance.Namespace, pvcName,
			csivolumeinfov1alpha1.OwnershipLabelRetainedBySnapshot); labelErr != nil {
			return fmt.Errorf("reconcileVMOwnedVolumesDetach: failed to label PVC %s/%s "+
				"as retained-by-snapshot: %w", instance.Namespace, pvcName, labelErr)
		}
		// Remove volume from BA status — the disk is now stably snapshot-retained.
		deleteVolumeFromStatus(pvcName, instance)
		log.Infof("reconcileVMOwnedVolumesDetach: volume %q marked snapshot-retained; "+
			"removed from BA status", volumeID)
		return nil
	}

	// No snapshot retains the disk; re-register the VMDK as an FCD to return
	// the volume to full CSI management.
	log.Infof("reconcileVMOwnedVolumesDetach: no snapshot retains diskUUID %s; "+
		"re-registering volume %q as FCD", diskUUID, volumeID)
	if reregErr := ReregisterVolumeAsFCD(ctx, volumeManager, cviSvc, k8sClient,
		cvi, configInfo); reregErr != nil {
		return fmt.Errorf("reconcileVMOwnedVolumesDetach: ReregisterVolumeAsFCD failed "+
			"for volume %q: %w", volumeID, reregErr)
	}
	// ReregisterVolumeAsFCD transitions CVI to CSI_MANAGED, removes the
	// finalizer, and labels the PVC csi-owned.
	deleteVolumeFromStatus(pvcName, instance)
	log.Infof("reconcileVMOwnedVolumesDetach: volume %q re-registered as FCD; "+
		"removed from BA status", volumeID)
	return nil
}
