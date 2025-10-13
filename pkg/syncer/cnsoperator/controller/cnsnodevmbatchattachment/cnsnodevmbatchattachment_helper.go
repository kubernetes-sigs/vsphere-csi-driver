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

package cnsnodevmbatchattachment

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"strings"
	"sync"

	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes"

	vimtypes "github.com/vmware/govmomi/vim25/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	v1alpha1 "sigs.k8s.io/vsphere-csi-driver/v3/pkg/apis/cnsoperator/cnsnodevmbatchattachment/v1alpha1"
	volumes "sigs.k8s.io/vsphere-csi-driver/v3/pkg/common/cns-lib/volume"
	cnsvsphere "sigs.k8s.io/vsphere-csi-driver/v3/pkg/common/cns-lib/vsphere"
	"sigs.k8s.io/vsphere-csi-driver/v3/pkg/common/config"
	"sigs.k8s.io/vsphere-csi-driver/v3/pkg/csi/service/common/commonco"
	"sigs.k8s.io/vsphere-csi-driver/v3/pkg/csi/service/logger"
	k8s "sigs.k8s.io/vsphere-csi-driver/v3/pkg/kubernetes"
	cnsoperatortypes "sigs.k8s.io/vsphere-csi-driver/v3/pkg/syncer/cnsoperator/types"
	cnsoperatorutil "sigs.k8s.io/vsphere-csi-driver/v3/pkg/syncer/cnsoperator/util"
)

var (
	GetVMFromVcenter = cnsoperatorutil.GetVMFromVcenter
	attachedVmPrefix = "cns.vmware.com/usedby-vm-"
)

const (
	detachSuffix = ":detaching"
)

// removeFinalizerFromCRDInstance will remove the CNS Finalizer, cns.vmware.com,
// from a given nodevmbatchattachment instance.
func removeFinalizerFromCRDInstance(ctx context.Context,
	instance *v1alpha1.CnsNodeVMBatchAttachment,
	c client.Client) error {
	log := logger.GetLogger(ctx)
	finalizersOnInstance := instance.Finalizers
	for i, finalizer := range instance.Finalizers {
		if finalizer == cnsoperatortypes.CNSFinalizer {
			log.Infof("Removing %q finalizer from CnsNodeVMBatchAttachment instance with name: %q on namespace: %q",
				cnsoperatortypes.CNSFinalizer, instance.Name, instance.Namespace)
			finalizersOnInstance = append(instance.Finalizers[:i], instance.Finalizers[i+1:]...)
			break
		}
	}
	return k8s.PatchFinalizers(ctx, c, instance, finalizersOnInstance)
}

// getNamespacedPvcName take namespace and pvcName sends back namespace + "/" + pvcName.
func getNamespacedPvcName(namespace string, pvcName string) string {
	return namespace + "/" + pvcName
}

// getVolumesToDetachFromInstance finds out which are the volumes to detach by finding out which are
// the volumes present in attachedFCDs but not in spec of the instance.
func getVolumesToDetachFromInstance(ctx context.Context,
	instance *v1alpha1.CnsNodeVMBatchAttachment,
	attachedFCDs map[string]bool,
	volumeIdsInSpec map[string]string) (pvcsToDetach map[string]string, err error) {
	log := logger.GetLogger(ctx)

	// Map contains PVCs which need to be detached.
	pvcsToDetach = make(map[string]string)

	/// Add those volumes to to pvcsToDetach
	// which are present in attachedFCDs list but not in
	// instance spec.
	for attachedFcdId := range attachedFCDs {
		if _, ok := volumeIdsInSpec[attachedFcdId]; !ok {
			// Get PVC name for the given FCD.
			pvcName, pvcNs, exists := commonco.ContainerOrchestratorUtility.GetPVCNameFromCSIVolumeID(attachedFcdId)
			if !exists {
				msg := fmt.Sprintf("failed to find PVC for volumeID %s in cluster", attachedFcdId)
				return pvcsToDetach, errors.New(msg)
			}

			// Get PVC object for the given PVC name.
			pvcObj, err := commonco.ContainerOrchestratorUtility.GetPvcObjectByName(ctx, pvcName, pvcNs)
			if err != nil {
				msg := fmt.Sprintf("failed to find PVC obj for PVC name %s in namespace %s", pvcName, pvcNs)
				return pvcsToDetach, errors.New(msg)
			}

			// This check is required only for RWO volumes because before 9.1 we never supported RWX block volumes.
			// If an RWO PVC does not have usedby-vm annotation,
			// it means that it wasn't attached via the CnsNodeVMBatchAttachment CR.
			// So this PVC should not be detached by CnsNodeVMNBatchAttach CR either.
			if !isSharedPvc(*pvcObj) && !pvcHasUsedByAnnotaion(ctx, pvcObj) {
				log.Infof("PVC %s does not have usedby-vm annotation. PVC not attached via CnsNodeVMBatchAttachment.", pvcName)
				continue
			}
			pvcsToDetach[pvcName] = attachedFcdId
		}
	}
	log.Debugf("Obtained volumes to detach %+v for instance %s", pvcsToDetach, instance.Name)
	return pvcsToDetach, nil
}

// isSharedPvc returns true for PVCs which allow multi attach.
func isSharedPvc(pvcObj v1.PersistentVolumeClaim) bool {
	for _, accessMode := range pvcObj.Spec.AccessModes {
		if accessMode == v1.ReadWriteMany || accessMode == v1.ReadOnlyMany {
			return true
		}
	}
	return false
}

// removeStaleEntriesFromInstanceStatus removes the entries in instance status for which there is
// no entry in instance spec and that volume is not being detached also.
//
// Consider the following example:
//
// Volumes in spec: pvc-1, pvc-2
// Volumes in status: pvc-1, pvc-2
// Volumes on vCenter: pvc-1, pvc-2
//
// pvc-2 is removed from spec to trigger detach. Current status:
// Volumes in spec: pvc-1
// Volumes in status: pvc-1, pvc-2
// Volumes on vCenter: pvc-1, pvc-2
//
// Reconciliation 1:
// pvc-2 is detched from the VM but CSI failed to remove this entry from instance status.
// Status after reconciliation 1:
// Volumes in spec: pvc-1
// Volumes in status: pvc-1, pvc-2
// Volumes on vCenter: pvc-1
//
// The request is requeued.
// Reconciliation 2:
// This time spec and vCenter are in sync but status is incorrect.
// So CSI should remove the extra entry.
// Status after reconciliation 2:
// Volumes in spec: pvc-1
// Volumes in status: pvc-1
// Volumes on vCenter: pvc-1
func removeStaleEntriesFromInstanceStatus(ctx context.Context,
	client client.Client,
	k8sClient kubernetes.Interface,
	instance *v1alpha1.CnsNodeVMBatchAttachment,
	pvcsToDetach map[string]string, volumeNamesInSpec map[string]string) error {
	log := logger.GetLogger(ctx)

	// Remove entries them from instance status and update the instance.
	// For each entry in status, find corresponding entry in Spec and in pvcsToDetach.
	for _, volumeStatus := range instance.Status.VolumeStatus {
		if _, existsInSpec := volumeNamesInSpec[volumeStatus.Name]; !existsInSpec {
			// Volume not found in status, check if it is being detached.
			if _, existsInDetachList := pvcsToDetach[volumeStatus.PersistentVolumeClaim.ClaimName]; !existsInDetachList {
				// Volume not getting detached also, it means it is a stale entry.

				// First ensure that the PVC does not have CNS protection finalizer.
				// This kind of situation can happen when detach is successful but finalizer could not be removed
				// because of which the instance is back in queue.
				err := removePvcFinalizer(ctx, client, k8sClient, volumeStatus.PersistentVolumeClaim.ClaimName, instance.Namespace,
					instance.Spec.NodeUUID)
				if err != nil {
					log.Errorf("failed to ensure that PVC finalizers are removed.")
					return err
				}

				log.Infof("Status for a PVC %s found in instance %s but it is not present in Spec. "+
					"Removing it from instance", volumeStatus.PersistentVolumeClaim.ClaimName, instance.Name)
				deleteVolumeFromStatus(volumeStatus.PersistentVolumeClaim.ClaimName, instance)
			}
		}
	}
	return nil
}

// getVolumesToDetach returns list of volumes to detach by taking a diff of
// volumes in spec and in attachedFCDs list.
func getVolumesToDetachForVmFromVC(ctx context.Context,
	instance *v1alpha1.CnsNodeVMBatchAttachment,
	client client.Client,
	k8sClient kubernetes.Interface,
	attachedFCDs map[string]bool) (map[string]string, error) {
	log := logger.GetLogger(ctx)

	pvcsToDetach := make(map[string]string)
	// Get all PVCs and their corresponding volumeID mapping from instance spec.
	volumeIdsInSpec, volumeNamesInSpec, err := getVolumeNameVolumeIdMapsInSpec(ctx, instance)
	if err != nil {
		log.Errorf("failed to get PVCs in spec. Err: %s", err)
		return pvcsToDetach, err
	}

	// Find out the volumes to detach by taking a diff between
	// the instance spec and the FCDs currently attached to the VM on vCenter.
	pvcsToDetach, err = getVolumesToDetachFromInstance(ctx, instance, attachedFCDs, volumeIdsInSpec)
	if err != nil {
		log.Errorf("failed to find volumes to attach and volumes to detach from instance spec. Err: %s", err)
		return pvcsToDetach, err
	}
	log.Debugf("Obtained volumes to detach %+v for instance %s", pvcsToDetach, instance.Name)

	updatePvcStatusEntryName(ctx, instance, pvcsToDetach)

	// Ensure that there are no extra entries in instance status from a previous detach call.
	err = removeStaleEntriesFromInstanceStatus(ctx, client, k8sClient, instance, pvcsToDetach, volumeNamesInSpec)
	if err != nil {
		log.Errorf("failed to remove stale entried from instance spec. Err: %s", err)
		return pvcsToDetach, err
	}
	return pvcsToDetach, nil
}

// updatePvcStatusEntryName goes through each of the PVCs to detach and updates their
// status to have the suffix ":detaching".
// This is required to avoid the case where disk-1 was associated with pvc-1 and got attached.
// disk-1 is then associated with pvc-2.
// This means, PVC-1 should get detached and PVC-2 should get attached to the VM.
// But they both have the same entry in the status which is wrong. By adding the suffix,
// the volume name entry for the PVC getting detached becomes unique.
func updatePvcStatusEntryName(ctx context.Context,
	instance *v1alpha1.CnsNodeVMBatchAttachment, pvcsToDetach map[string]string) {
	log := logger.GetLogger(ctx)

	for i, volume := range instance.Status.VolumeStatus {
		if _, ok := pvcsToDetach[volume.PersistentVolumeClaim.ClaimName]; !ok {
			continue
		}
		newVolumeName := instance.Status.VolumeStatus[i].Name + detachSuffix
		instance.Status.VolumeStatus[i].Name = newVolumeName
		log.Infof("Updating status name entry to %s for detaching PVC %s",
			newVolumeName,
			volume.PersistentVolumeClaim.ClaimName)
	}
}

// updateInstanceStatus updates the given nodevmbatchattachment instance's status.
func updateInstanceStatus(ctx context.Context, cnsoperatorclient client.Client,
	instance *v1alpha1.CnsNodeVMBatchAttachment) error {
	log := logger.GetLogger(ctx)
	err := cnsoperatorclient.Status().Update(ctx, instance)
	if err != nil {
		log.Errorf("failed to update CnsNodeVMBatchAttachment instance: %q on namespace: %q. Error: %+v",
			instance.Name, instance.Namespace, err)
		return err
	}
	return nil
}

// updateInstanceWithAttachVolumeResult finds the given's volumeName's status in the instance status
// and updates it with error.
// It will add a new status for the volume if it does not already exist.
func updateInstanceWithAttachVolumeResult(instance *v1alpha1.CnsNodeVMBatchAttachment,
	volumeName string, pvc string, result volumes.BatchAttachResult) {

	errMsg := ""
	attached := true
	if result.Error != nil {
		attached = false
		errMsg = result.Error.Error()
	}

	newVolumeStatus := v1alpha1.VolumeStatus{
		Name: volumeName,
		PersistentVolumeClaim: v1alpha1.PersistentVolumeClaimStatus{
			ClaimName:   pvc,
			Attached:    attached,
			Error:       errMsg,
			CnsVolumeID: result.VolumeID,
			DiskUUID:    result.DiskUUID,
		},
	}

	for i, volume := range instance.Status.VolumeStatus {
		if volume.Name != volumeName {
			continue
		}
		// Update existing entry
		instance.Status.VolumeStatus[i] = newVolumeStatus
		return
	}

	// Add new entry instatus if it does not already exist.
	instance.Status.VolumeStatus = append(instance.Status.VolumeStatus, newVolumeStatus)
}

// updateInstanceWithErrorVolumeName finds the given's PVC's status in the instance status
// and updates it with error.
func updateInstanceWithErrorForPvc(instance *v1alpha1.CnsNodeVMBatchAttachment,
	pvc string, errMsg string) {
	for i, volume := range instance.Status.VolumeStatus {
		if volume.PersistentVolumeClaim.ClaimName != pvc {
			continue
		}
		instance.Status.VolumeStatus[i].PersistentVolumeClaim.Error = errMsg
		return
	}
}

// deleteVolumeFromStatus finds the status of the given volumeName in an instance and deletes its entry.
func deleteVolumeFromStatus(pvc string, instance *v1alpha1.CnsNodeVMBatchAttachment) {
	instance.Status.VolumeStatus = slices.DeleteFunc(instance.Status.VolumeStatus,
		func(e v1alpha1.VolumeStatus) bool {
			return e.PersistentVolumeClaim.ClaimName == pvc
		})
}

// getVolumeNameVolumeIdMapsInSpec returns the volumes in instance spec.
// It return two maps:
// 1. volumeID to PVC name
// 2. VolumeName to PVC name
func getVolumeNameVolumeIdMapsInSpec(ctx context.Context,
	instance *v1alpha1.CnsNodeVMBatchAttachment) (volumeIdsInSpec map[string]string,
	volumeNamesInSpec map[string]string, err error) {
	log := logger.GetLogger(ctx)

	volumeIdsInSpec = make(map[string]string)
	volumeNamesInSpec = make(map[string]string)
	for _, volume := range instance.Spec.Volumes {
		namespacedPvcName := getNamespacedPvcName(instance.Namespace, volume.PersistentVolumeClaim.ClaimName)
		volumeId, ok := commonco.ContainerOrchestratorUtility.GetVolumeIDFromPVCName(namespacedPvcName)
		if !ok {
			msg := fmt.Sprintf("failed to find volumeID for PVC %s", volume.PersistentVolumeClaim.ClaimName)
			log.Errorf(msg)
			err = errors.New(msg)
			return
		}
		volumeNamesInSpec[volume.Name] = volume.PersistentVolumeClaim.ClaimName
		volumeIdsInSpec[volumeId] = volume.PersistentVolumeClaim.ClaimName
	}
	return
}

// getPvcsInSpec returns map of PVCs and their volumeIDs.
func getPvcsInSpec(instance *v1alpha1.CnsNodeVMBatchAttachment) (map[string]string, error) {
	pvcsInSpec := make(map[string]string)
	for _, volume := range instance.Spec.Volumes {
		namespacedPvcName := getNamespacedPvcName(instance.Namespace, volume.PersistentVolumeClaim.ClaimName)
		volumeId, ok := commonco.ContainerOrchestratorUtility.GetVolumeIDFromPVCName(namespacedPvcName)
		if !ok {
			return pvcsInSpec, fmt.Errorf("failed to find volumeID for PVC %s", volume.PersistentVolumeClaim.ClaimName)
		}
		pvcsInSpec[volume.PersistentVolumeClaim.ClaimName] = volumeId
	}

	return pvcsInSpec, nil

}

// listAttachedFcdsForVM returns list of FCDs (present in the K8s cluster)
// which are attached to given VM on vCenter.
func listAttachedFcdsForVM(ctx context.Context,
	vm *cnsvsphere.VirtualMachine) (map[string]bool, error) {
	log := logger.GetLogger(ctx)
	attachedFCDs := make(map[string]bool)
	// Verify if the volume id is on the VM backing virtual disk devices.
	vmDevices, err := vm.Device(ctx)
	if err != nil {
		log.Errorf("failed to get devices from vm: %s", vm.InventoryPath)
		return attachedFCDs, err
	}
	if len(vmDevices) == 0 {
		return attachedFCDs, nil
	}
	for _, device := range vmDevices {
		if vmDevices.TypeName(device) == "VirtualDisk" {
			if virtualDisk, ok := device.(*vimtypes.VirtualDisk); ok && virtualDisk.VDiskId != nil {
				// If the given volumeID does not exist in K8s cluster,
				// do not add it to attachedFCDs list because it is not being consumed
				// by any PVC.
				_, _, existsOnK8s := commonco.ContainerOrchestratorUtility.GetPVCNameFromCSIVolumeID(virtualDisk.VDiskId.Id)
				if existsOnK8s {
					log.Infof("Adding volume with ID %s to attachedFCDs list", virtualDisk.VDiskId.Id)
					attachedFCDs[virtualDisk.VDiskId.Id] = true
				}

			} else {
				log.Debugf("failed to obtain virtual disk for device %+v", device)
			}
		}
	}
	return attachedFCDs, nil
}

// constructBatchAttachRequest goes through all volumes in instance spec and
// constructs the batchAttach request for each of them.
// It also validates each of the requests to make sure user input is correct.
func constructBatchAttachRequest(ctx context.Context,
	instance *v1alpha1.CnsNodeVMBatchAttachment) (pvcsInSpec map[string]string,
	volumeIdsInSpec map[string]string,
	batchAttachRequest []volumes.BatchAttachRequest, err error) {
	log := logger.GetLogger(ctx)
	log.Infof("Constructing batch attach request for for instance %s", instance.Name)

	batchAttachRequest = make([]volumes.BatchAttachRequest, 0)

	// Initialize these 2 maps which will be required for easy lookup later on.
	// This map has mapping of PVC to VolumeName
	pvcsInSpec = make(map[string]string)
	// This map has mapping of volumeID to PVC.
	volumeIdsInSpec = make(map[string]string)

	for _, volume := range instance.Spec.Volumes {
		pvcName := volume.PersistentVolumeClaim.ClaimName
		volumeName := volume.Name

		// Find volumeID for PVC.
		namespacedPvcName := getNamespacedPvcName(instance.Namespace, pvcName)
		attachVolumeId, ok := commonco.ContainerOrchestratorUtility.GetVolumeIDFromPVCName(namespacedPvcName)
		if !ok {
			err := fmt.Errorf("failed to find volumeID for PVC %s", pvcName)
			log.Error(err)
			return pvcsInSpec, volumeIdsInSpec, batchAttachRequest, err
		}

		// Populate these 2 maps as these values are required later during batch attach.
		pvcsInSpec[volume.PersistentVolumeClaim.ClaimName] = volumeName
		volumeIdsInSpec[attachVolumeId] = volume.PersistentVolumeClaim.ClaimName

		// Populate values for attach request.
		currentBatchAttachRequest := volumes.BatchAttachRequest{
			VolumeID:      attachVolumeId,
			SharingMode:   string(volume.PersistentVolumeClaim.SharingMode),
			DiskMode:      string(volume.PersistentVolumeClaim.DiskMode),
			ControllerKey: volume.PersistentVolumeClaim.ControllerKey,
			UnitNumber:    volume.PersistentVolumeClaim.UnitNumber,
		}
		batchAttachRequest = append(batchAttachRequest, currentBatchAttachRequest)
	}
	return pvcsInSpec, volumeIdsInSpec, batchAttachRequest, nil
}

// getVmObject find the VM object on vCenter.
// If VM retrieval from vCenter fails with NotFound error,
// then it is not considered an error because VM CR is probably being deleted.
func getVmObject(ctx context.Context, client client.Client, configInfo config.ConfigurationInfo,
	instance *v1alpha1.CnsNodeVMBatchAttachment) (*cnsvsphere.VirtualMachine, error) {
	log := logger.GetLogger(ctx)

	// Get vm from vCenter.
	vm, err := GetVMFromVcenter(ctx, instance.Spec.NodeUUID, configInfo)
	if err != nil {
		if err == cnsvsphere.ErrVMNotFound {
			log.Infof("VM %s not found on VC", instance.Spec.NodeUUID)
			return nil, nil
		}
		return nil, err
	}

	log.Infof("Obtained VM object for VM %s from VC", vm.UUID)
	return vm, nil
}

// getVolumesToDetach checks if:
// Instance is being deleted, then it adds all the volumes in the spec for detach.
// If instance is not being deleted then finds the volumes to be detached by querying vCenter.
func getVolumesToDetach(ctx context.Context, instance *v1alpha1.CnsNodeVMBatchAttachment,
	vm *cnsvsphere.VirtualMachine, client client.Client, k8sClient kubernetes.Interface) (map[string]string, error) {
	log := logger.GetLogger(ctx)

	if instance.DeletionTimestamp != nil {
		log.Debugf("Instance %s is being deleted, adding all volumes in spec to volumesToDetach list.", instance.Name)
		volumesToDetach, err := getPvcsInSpec(instance)
		if err != nil {
			log.Errorf("failed to get volumes to detach from instance spec. Err: %s", err)
			return volumesToDetach, err
		}
		log.Debugf("Volumes to detach list %+v for instance %s", volumesToDetach, instance.Name)
		return volumesToDetach, nil
	}

	// Find the volumes to detach from the vCenter.
	volumesToDetach, err := getVolumesToDetachFromVM(ctx, client, k8sClient, instance, vm)
	if err != nil {
		log.Errorf("failed to find volumes to detach from the vCenter for instance %s", instance.Name)
		return volumesToDetach, err
	}
	log.Debugf("Volumes to detach list %+v for instance %s", volumesToDetach, instance.Name)
	return volumesToDetach, nil
}

// getVolumesToDetachFromVM queries vCenter to find the list of FCDs
// which have to be detached from the VM.
func getVolumesToDetachFromVM(ctx context.Context, client client.Client,
	k8sClient kubernetes.Interface,
	instance *v1alpha1.CnsNodeVMBatchAttachment,
	vm *cnsvsphere.VirtualMachine) (map[string]string, error) {
	log := logger.GetLogger(ctx)

	// Query vCenter to find the list of FCDs which are attached to the VM.
	attachedFcdList, err := listAttachedFcdsForVM(ctx, vm)
	if err != nil {
		log.Errorf("failed to find the FCDs attached to VM %s. Err: %s", vm, err)
		return map[string]string{}, err
	}
	log.Infof("List of attached FCDs %+v to VM %s", attachedFcdList, instance.Spec.NodeUUID)

	// Find volumes to be detached from the VM by takinga diff with FCDs attached to VM on vCenter.
	volumesToDetach, err := getVolumesToDetachForVmFromVC(ctx, instance, client, k8sClient, attachedFcdList)
	if err != nil {
		log.Errorf("failed to find volumes to attach and detach. Err: %s", err)
		return map[string]string{}, err
	}

	log.Infof("Volumes to be detached %+v for instance %s", volumesToDetach, instance.Name)

	return volumesToDetach, nil

}

// addPvcAnnotation adds the vmInstanceUUID as an annotation to the given PVC.
func addPvcAnnotation(ctx context.Context, k8sClient kubernetes.Interface,
	vmInstanceUUID string, pvc *v1.PersistentVolumeClaim) error {

	return patchPVCAnnotations(ctx, k8sClient, pvc, attachedVmPrefix+vmInstanceUUID, false)
}

// removePvcAnnotation removes the given vmInstanceUUID from PVC annotations.
func removePvcAnnotation(ctx context.Context, k8sClient kubernetes.Interface,
	vmInstanceUUID string, pvc *v1.PersistentVolumeClaim) error {
	log := logger.GetLogger(ctx)

	if pvc.Annotations == nil {
		log.Infof("PVC %s does not have any annotations, nothing to be done", pvc.Name)
		return nil
	}

	return patchPVCAnnotations(ctx, k8sClient, pvc, attachedVmPrefix+vmInstanceUUID, true)
}

// patchPVCAnnotations patches the list of annotations on the PVC with the newAnnotations.
func patchPVCAnnotations(ctx context.Context, k8sClient kubernetes.Interface,
	pvc *v1.PersistentVolumeClaim, key string, removeAnnotation bool) error {
	log := logger.GetLogger(ctx)

	patchAnnotations := make(map[string]interface{})
	for k, v := range pvc.Annotations {
		patchAnnotations[k] = v
	}

	if removeAnnotation {
		log.Infof("Removing annotation %s from PVC %s", key, pvc.Name)
		patchAnnotations[key] = nil
	} else {
		log.Infof("Adding annotation %s on PVC", key, pvc.Name)
		patchAnnotations[key] = ""
	}

	log.Debugf("Updated list of annotations on PVC %s: %+v", pvc.Name, patchAnnotations)

	// Build patch structure
	patch := map[string]interface{}{
		"metadata": map[string]interface{}{
			"annotations": patchAnnotations,
		},
	}

	patchBytes, err := json.Marshal(patch)
	if err != nil {
		log.Errorf("failed to marshal with annotations for PVC %s. Err: %s", pvc.Name, err)
		return fmt.Errorf("failed to marshal patch: %v", err)
	}

	log.Infof("Patching PVC %s with updated annotation", pvc.Name)

	// Apply the patch
	updatedpvc, err := k8sClient.CoreV1().PersistentVolumeClaims(pvc.Namespace).Patch(
		ctx,
		pvc.Name,
		types.MergePatchType,
		patchBytes,
		metav1.PatchOptions{},
	)
	if err != nil {
		log.Errorf("failed to patch PVC %s with annotations. Err: %s", pvc.Name, err)
		return fmt.Errorf("failed to patch PVC %s: %v", pvc.Name, err)
	}
	log.Debugf("Successfully patched PVC: %s with annotations %+v", pvc.Name, updatedpvc.Annotations)
	return nil
}

// pvcHasUsedByAnnotaion goes through all annotations on the PVC to find out if the PVC is used by any VM or not.
func pvcHasUsedByAnnotaion(ctx context.Context, pvc *v1.PersistentVolumeClaim) bool {
	log := logger.GetLogger(ctx)

	if pvc.Annotations == nil {
		log.Infof("No annotation found on PVC %s", pvc.Name)
		return false
	}

	for key := range pvc.Annotations {
		if strings.HasPrefix(key, attachedVmPrefix) {
			log.Infof("Annotation with prefix %s found on PVC %s", attachedVmPrefix, pvc.Name)
			return true
		}
	}

	log.Infof("PVC %s does not contain any annotations with prefix %s", pvc.Name, attachedVmPrefix)
	return false
}

// addPvcFinalizer adds the given VM as an annotation for the given PVC and
// adds CNS finalizer to the PVC.
func addPvcFinalizer(ctx context.Context, client client.Client,
	k8sClient kubernetes.Interface,
	pvcName string, namespace string, vmInstanceUUID string) error {
	log := logger.GetLogger(ctx)

	// Acquire lock on PVC
	namespacedVolumeName := namespace + "/" + pvcName
	actual, _ := VolumeLock.LoadOrStore(namespacedVolumeName, &sync.Mutex{})
	instanceLock, ok := actual.(*sync.Mutex)
	if !ok {
		return fmt.Errorf("failed to cast lock for PVC: %s", namespacedVolumeName)
	}
	instanceLock.Lock()
	log.Infof("Acquired lock for PVC %s", namespacedVolumeName)
	defer func() {
		instanceLock.Unlock()
		log.Infof("Released lock for PVC %s", namespacedVolumeName)
	}()

	// Get PVC object from informer cache
	pvc, err := commonco.ContainerOrchestratorUtility.GetPvcObjectByName(ctx, pvcName, namespace)
	if err != nil {
		log.Errorf("failed to get PVC object for PVC %s. Err: %s", pvcName, err)
		return err
	}

	// Add annotation indicating that the PVC is being used by this VM.
	log.Infof("PVC %s is shared", pvc.Name)
	err = addPvcAnnotation(ctx, k8sClient, vmInstanceUUID, pvc)
	if err != nil {
		log.Errorf("failed to add annotation %s to PVC %s in namespace %s for VM %s", cnsoperatortypes.CNSPvcFinalizer,
			pvcName, namespace, vmInstanceUUID)
		return err
	}
	log.Infof("Successfully added annotation on PVC %s for VM %s", pvcName, vmInstanceUUID)

	// Obtain the latest PVC again with updated annotations.
	// Cannot rely on informer cache as it may not be updated.
	pvc, err = k8sClient.CoreV1().PersistentVolumeClaims(pvc.Namespace).Get(ctx, pvcName, metav1.GetOptions{})
	if err != nil {
		log.Errorf("failed to get updated PVC %s in namespace %s", pvcName, namespace)
		return err

	}

	// If finalizer already exists, there is nothing to be done.
	if controllerutil.ContainsFinalizer(pvc, cnsoperatortypes.CNSPvcFinalizer) {
		// Finalizer already present on PVC
		log.Infof("Finalizer %s not present on PVC %s", cnsoperatortypes.CNSPvcFinalizer, pvcName)
		return nil
	}

	return k8s.PatchFinalizers(ctx, client, pvc,
		append(pvc.Finalizers, cnsoperatortypes.CNSPvcFinalizer))

}

// removePvcFinalizer removes the given VM from the PVC's used by annotations
// and then removes finalizer from the PVC if it was the last attached VM for the PVC.
func removePvcFinalizer(ctx context.Context, client client.Client,
	k8sClient kubernetes.Interface,
	pvcName string, namespace string, vmInstanceUUID string) error {
	log := logger.GetLogger(ctx)

	namespacedVolumeName := namespace + "/" + pvcName
	// Acquire lock on PVC
	actual, _ := VolumeLock.LoadOrStore(namespacedVolumeName, &sync.Mutex{})
	instanceLock, ok := actual.(*sync.Mutex)
	if !ok {
		return fmt.Errorf("failed to cast lock for PVC: %s", namespacedVolumeName)
	}
	instanceLock.Lock()
	log.Infof("Acquired lock for PVC %s", namespacedVolumeName)
	defer func() {
		instanceLock.Unlock()
		log.Infof("Released lock for PVC %s", namespacedVolumeName)
	}()

	// Get PVC object from informer cache
	pvc, err := commonco.ContainerOrchestratorUtility.GetPvcObjectByName(ctx, pvcName, namespace)
	if err != nil {
		log.Errorf("failed to get PVC object for PVC %s. Err: %s", pvcName, err)
		return err
	}

	// Remove usedby annotation
	log.Infof("PVC %s is shared", pvc.Name)
	err = removePvcAnnotation(ctx, k8sClient, vmInstanceUUID, pvc)
	if err != nil {
		return err
	}
	log.Infof("Successfully updated annotations on PVC %s for VM %s", pvcName, vmInstanceUUID)

	// Obtain the latest PVC again with updated annotations.
	// Cannot rely on informer cache as it may not be updated.
	pvc, err = k8sClient.CoreV1().PersistentVolumeClaims(pvc.Namespace).Get(ctx, pvcName, metav1.GetOptions{})
	if err != nil {
		log.Errorf("failed to get updated PVC %s in namespace %s", pvcName, namespace)
		return err

	}

	if pvcHasUsedByAnnotaion(ctx, pvc) {
		log.Infof("PVC %s is still being use by other VMs. Not removing finalizer.", pvcName)
		return nil
	}

	log.Infof("VM %s was the last attached VM for the PVC %s. Finalizer %s can be safely removed fromt the PVC",
		vmInstanceUUID, pvcName, cnsoperatortypes.CNSPvcFinalizer)

	if !controllerutil.ContainsFinalizer(pvc, cnsoperatortypes.CNSPvcFinalizer) {
		// Finalizer not present on PVC, nothing to be done here.
		log.Infof("Finalizer %s not present on PVC %s", cnsoperatortypes.CNSPvcFinalizer, pvcName)
		return nil
	}

	// Remove finalizer from the PVC if it was the last attached VM.
	finalizersOnPvc := pvc.Finalizers
	for i, finalizer := range pvc.Finalizers {
		if finalizer == cnsoperatortypes.CNSPvcFinalizer {
			log.Infof("Removing %s finalizer from PVC: %s on namespace: %s",
				cnsoperatortypes.CNSPvcFinalizer, pvcName, namespace)
			finalizersOnPvc = append(pvc.Finalizers[:i], pvc.Finalizers[i+1:]...)
			break
		}
	}

	err = k8s.PatchFinalizers(ctx, client, pvc, finalizersOnPvc)
	if err != nil {
		log.Errorf("failed to patch PVC %s with finalizers", pvc.Name)
		return err
	}

	log.Infof("Successfully removed finalizer %s from PVC %s", cnsoperatortypes.CNSPvcFinalizer, pvc.Name)

	// Remove this PVC from volume lock store.
	VolumeLock.Delete(namespacedVolumeName)
	return nil
}
