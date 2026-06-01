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
	"encoding/json"
	"fmt"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	k8stypes "k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"

	bav1alpha1 "sigs.k8s.io/vsphere-csi-driver/v3/pkg/apis/cnsoperator/cnsnodevmbatchattachment/v1alpha1"
	csivolumeinfo "sigs.k8s.io/vsphere-csi-driver/v3/pkg/apis/cnsoperator/csivolumeinfo"
	csivolumeinfov1alpha1 "sigs.k8s.io/vsphere-csi-driver/v3/pkg/apis/cnsoperator/csivolumeinfo/v1alpha1"
	"sigs.k8s.io/vsphere-csi-driver/v3/pkg/common/utils"
	"sigs.k8s.io/vsphere-csi-driver/v3/pkg/csi/service/logger"
)

// processVMOwnedVolumesAttach handles the ownership-transfer volume attach
// path for a single volume on a VM that has the VMOwnedVolumes annotation.
// The caller is responsible for checking the annotation before calling this
// function; it is never mixed with the legacy BatchAttachVolumes path.
//
// Steps:
//  1. Validate the CVI is in CSI_MANAGED state.
//  2. Call CnsUnregisterVolume to deregister the FCD, preserving the VMDK.
//  3. Transition the CVI to TRANSFERRING_TO_VM and record the target VM.
//  4. Add the cvi-protection finalizer to the CVI.
//  5. Write diskPath, diskUUID, and the AttachMethod=Reconfig condition to
//     the BA volume status so vm-operator knows to use ReconfigVM.
func processVMOwnedVolumesAttach(ctx context.Context,
	cviSvc csivolumeinfo.CsiVolumeInfoService,
	vmOperatorClient client.Client,
	volumeManager volumeManagerForAttach,
	instance *bav1alpha1.CnsNodeVMBatchAttachment,
	vmName, vmInstanceUUID string,
	pvcName, volumeID, volumeName string,
) error {
	log := logger.GetLogger(ctx)
	log.Infof("processVMOwnedVolumesAttach: starting for PVC %s (volumeID=%s) on VM %s/%s",
		pvcName, volumeID, instance.Namespace, vmName)

	cvi, err := cviSvc.GetCsiVolumeInfo(ctx, instance.Namespace, volumeID)
	if err != nil {
		return fmt.Errorf("processVMOwnedVolumesAttach: failed to get CVI for %q: %w",
			volumeID, err)
	}
	if cvi == nil {
		// A VM with the VMOwnedVolumes annotation must have a CVI for every
		// volume. Its absence indicates a provisioning inconsistency.
		return fmt.Errorf("processVMOwnedVolumesAttach: CVI not found for volume %q on VM %s/%s",
			volumeID, instance.Namespace, vmName)
	}

	// The CVI must be in the steady CSI_MANAGED state. Any other state means
	// a previous transition is in progress and we must not re-enter the path.
	if cvi.Status.OwnershipState != csivolumeinfov1alpha1.OwnershipStateCSIManaged {
		msg := fmt.Sprintf("volume %q is already in state %q; cannot start attach",
			volumeID, cvi.Status.OwnershipState)
		log.Errorf("processVMOwnedVolumesAttach: %s", msg)
		updateInstanceVolumeStatus(ctx, instance, volumeName, pvcName, volumeID, "",
			fmt.Errorf("%s", msg), bav1alpha1.ConditionAttached, bav1alpha1.ReasonAttachFailed)
		return fmt.Errorf("processVMOwnedVolumesAttach: %s", msg)
	}

	log.Infof("processVMOwnedVolumesAttach: unregistering FCD for volume %q", volumeID)

	// Unregister the FCD, preserving the VMDK file as a plain disk.
	faultType, unregErr := volumeManager.UnregisterVolume(ctx, volumeID, true)
	if unregErr != nil {
		msg := fmt.Sprintf("CnsUnregisterVolume failed for volume %q (fault=%q): %v",
			volumeID, faultType, unregErr)
		log.Errorf("processVMOwnedVolumesAttach: %s", msg)
		updateInstanceVolumeStatus(ctx, instance, volumeName, pvcName, volumeID, "",
			fmt.Errorf("%s", msg), bav1alpha1.ConditionAttached, bav1alpha1.ReasonAttachFailed)
		return fmt.Errorf("processVMOwnedVolumesAttach: %s", msg)
	}
	log.Infof("processVMOwnedVolumesAttach: FCD unregistered for volume %q", volumeID)

	// Transition CVI to TRANSFERRING_TO_VM. diskPath and diskUUID are taken
	// from the CVI (populated at provisioning time) — they remain valid
	// because no storage relocation can occur between provisioning and the
	// first attach.
	cvi.Status.OwnershipState = csivolumeinfov1alpha1.OwnershipStateTransferringToVM
	cvi.Status.VMName = vmName
	cvi.Status.VMInstanceUUID = vmInstanceUUID

	if err := cviSvc.UpdateCsiVolumeInfoStatus(ctx, cvi); err != nil {
		return fmt.Errorf("processVMOwnedVolumesAttach: failed to update CVI status for %q: %w",
			volumeID, err)
	}
	log.Infof("processVMOwnedVolumesAttach: CVI %s/%s transitioned to TRANSFERRING_TO_VM (vmName=%s)",
		cvi.Namespace, cvi.Name, vmName)

	// Add the cvi-protection finalizer to prevent premature CVI GC while the
	// volume transfer is in progress.
	if err := cviSvc.AddCVIProtectionFinalizer(ctx, cvi.Namespace, volumeID); err != nil {
		return fmt.Errorf("processVMOwnedVolumesAttach: failed to add cvi-protection finalizer for %q: %w",
			volumeID, err)
	}
	log.Infof("processVMOwnedVolumesAttach: cvi-protection finalizer added for CVI %s/%s",
		cvi.Namespace, cvi.Name)

	// Update BA status with diskPath, diskUUID, and AttachMethod=Reconfig so
	// vm-operator knows to use ReconfigVM_Task with the plain-disk path.
	setBAVolumeAttachMethodReconfig(ctx, instance, volumeName, pvcName, volumeID,
		cvi.Status.DiskUUID, cvi.Status.DiskPath)

	log.Infof("processVMOwnedVolumesAttach: BA status updated for PVC %s (diskUUID=%s, diskPath=%s)",
		pvcName, cvi.Status.DiskUUID, cvi.Status.DiskPath)
	return nil
}

// setBAVolumeAttachMethodReconfig writes diskPath, diskUUID, and the
// AttachMethod=Reconfig condition to the BA volume status entry for pvcName,
// creating the entry if it does not already exist.
func setBAVolumeAttachMethodReconfig(ctx context.Context,
	instance *bav1alpha1.CnsNodeVMBatchAttachment,
	volumeName, pvcName, volumeID, diskUUID, diskPath string) {
	log := logger.GetLogger(ctx)

	attachMethodCondition := metav1.Condition{
		Type:               bav1alpha1.ConditionAttachMethod,
		Status:             metav1.ConditionTrue,
		Reason:             bav1alpha1.ReasonReconfig,
		LastTransitionTime: metav1.Now(),
		Message:            "FCD unregistered; use diskPath for ReconfigVM",
	}

	// Locate or create the VolumeStatus entry for this PVC.
	var found bool
	for i := range instance.Status.VolumeStatus {
		vs := &instance.Status.VolumeStatus[i]
		if vs.PersistentVolumeClaim.ClaimName == pvcName {
			vs.PersistentVolumeClaim.CnsVolumeID = volumeID
			vs.PersistentVolumeClaim.DiskUUID = diskUUID
			vs.PersistentVolumeClaim.DiskPath = diskPath
			setOrReplaceCondition(&vs.PersistentVolumeClaim.Conditions, attachMethodCondition)
			found = true
			break
		}
	}
	if !found {
		newStatus := bav1alpha1.VolumeStatus{
			Name: volumeName,
			PersistentVolumeClaim: bav1alpha1.PersistentVolumeClaimStatus{
				ClaimName:   pvcName,
				CnsVolumeID: volumeID,
				DiskUUID:    diskUUID,
				DiskPath:    diskPath,
				Conditions:  []metav1.Condition{attachMethodCondition},
			},
		}
		instance.Status.VolumeStatus = append(instance.Status.VolumeStatus, newStatus)
	}
	log.Infof("setBAVolumeAttachMethodReconfig: set AttachMethod=Reconfig for PVC %s (volume %q)",
		pvcName, volumeID)
}

// setOrReplaceCondition adds or replaces the condition with the matching type
// in the given condition slice.
func setOrReplaceCondition(conditions *[]metav1.Condition, c metav1.Condition) {
	for i, existing := range *conditions {
		if existing.Type == c.Type {
			(*conditions)[i] = c
			return
		}
	}
	*conditions = append(*conditions, c)
}

// getVMNameByInstanceUUID returns the VirtualMachine CR name for the given
// instance UUID in the given namespace. Returns empty string if not found.
func getVMNameByInstanceUUID(ctx context.Context, vmOperatorClient client.Client,
	instanceUUID, namespace string) (string, error) {
	log := logger.GetLogger(ctx)

	vmList, err := utils.ListVirtualMachines(ctx, vmOperatorClient, namespace)
	if err != nil {
		return "", fmt.Errorf("getVMNameByInstanceUUID: failed to list VMs in namespace %q: %w",
			namespace, err)
	}
	for _, vm := range vmList.Items {
		if vm.Status.InstanceUUID == instanceUUID {
			log.Infof("getVMNameByInstanceUUID: found VM %q for instanceUUID %q",
				vm.Name, instanceUUID)
			return vm.Name, nil
		}
	}
	log.Infof("getVMNameByInstanceUUID: no VM found with instanceUUID %q in namespace %q",
		instanceUUID, namespace)
	return "", nil
}

// volumeManagerForAttach is the subset of volumes.Manager needed by the
// VMOwnedVolumes attach path. Defined as an interface so tests can provide a
// targeted mock.
type volumeManagerForAttach interface {
	UnregisterVolume(ctx context.Context, volumeID string, unregisterDisk bool) (string, error)
}

// patchBAStatus writes the instance status to the API server using a JSON
// merge-patch.
func patchBAStatus(ctx context.Context, c client.Client,
	instance *bav1alpha1.CnsNodeVMBatchAttachment) error {
	patch, err := json.Marshal(map[string]interface{}{
		"status": instance.Status,
	})
	if err != nil {
		return fmt.Errorf("patchBAStatus: marshal error: %w", err)
	}
	return c.Status().Patch(ctx, instance,
		client.RawPatch(k8stypes.MergePatchType, patch))
}
