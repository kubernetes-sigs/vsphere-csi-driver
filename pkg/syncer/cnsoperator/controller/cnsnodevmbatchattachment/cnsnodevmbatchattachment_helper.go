package cnsnodevmbatchattachment

import (
	"context"
	"errors"
	"fmt"
	"slices"

	vimtypes "github.com/vmware/govmomi/vim25/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	cnsnodevmbatchattachmentv1alpha1 "sigs.k8s.io/vsphere-csi-driver/v3/pkg/apis/cnsoperator/cnsnodevmbatchattachment/v1alpha1"
	cnsvsphere "sigs.k8s.io/vsphere-csi-driver/v3/pkg/common/cns-lib/vsphere"
	"sigs.k8s.io/vsphere-csi-driver/v3/pkg/csi/service/common/commonco"
	"sigs.k8s.io/vsphere-csi-driver/v3/pkg/csi/service/logger"
	k8s "sigs.k8s.io/vsphere-csi-driver/v3/pkg/kubernetes"
	cnsoperatortypes "sigs.k8s.io/vsphere-csi-driver/v3/pkg/syncer/cnsoperator/types"
)

// addCnsFinalizerOnCRDInstance patches the given nodeVmbatchAttachment instance with CNS finalizer, cns.vmware.com
func addCnsFinalizerOnCRDInstance(ctx context.Context,
	instance *cnsnodevmbatchattachmentv1alpha1.CnsNodeVmBatchAttachment,
	c client.Client) error {
	return k8s.PatchFinalizers(ctx, c, instance, append(instance.Finalizers, cnsoperatortypes.CNSFinalizer))
}

// removeFinalizerFromCRDInstance will remove the CNS Finalizer, cns.vmware.com,
// from a given nodevmbatchattachment instance.
func removeFinalizerFromCRDInstance(ctx context.Context,
	instance *cnsnodevmbatchattachmentv1alpha1.CnsNodeVmBatchAttachment) {
	log := logger.GetLogger(ctx)
	for i, finalizer := range instance.Finalizers {
		if finalizer == cnsoperatortypes.CNSFinalizer {
			log.Debugf("Removing %q finalizer from CnsNodeVmBatchAttachment instance with name: %q on namespace: %q",
				cnsoperatortypes.CNSFinalizer, instance.Name, instance.Namespace)
			instance.Finalizers = append(instance.Finalizers[:i], instance.Finalizers[i+1:]...)
		}
	}
}

// getVMInstanceFromUUID checks whether VM CR is present in SV namespace
// with given vmuuid and returns the VirtualMachine CR object if it is found.
/*func getVMInstanceFromUUID(ctx context.Context, vmOperatorClient client.Client,
	vmuuid string, namespace string) (*vmoperatorv1alpha4.VirtualMachine, error) {
	log := logger.GetLogger(ctx)
	vmList, err := utils.ListVirtualMachines(ctx, vmOperatorClient, namespace)
	if err != nil {
		msg := fmt.Sprintf("failed to list virtualmachines with error: %+v", err)
		log.Error(msg)
		return nil, err
	}
	for _, vmInstance := range vmList.Items {
		if vmInstance.Status.BiosUUID != vmuuid {
			continue
		}
		msg := fmt.Sprintf("VM CR with BiosUUID: %s found in namespace: %s",
			vmuuid, namespace)
		log.Infof(msg)
		return &vmInstance, nil

	}
	msg := fmt.Sprintf("VM CR with BiosUUID: %s not found in namespace: %s",
		vmuuid, namespace)
	log.Info(msg)
	return nil, nil
}*/

// isVmCrDeleted checks if VM exists on K8s.
// If it does not exist or if it has a deletion timestamp, it returns nil.
// For other cases, it returns the error observed.
/*func isVmCrDeleted(ctx context.Context, client client.Client,
	nodeUUID string, namespace string, instanceName string) error {
	log := logger.GetLogger(ctx)

	vmInstance, err := getVMInstanceFromUUID(ctx, client, nodeUUID, namespace)
	if err != nil {
		log.Errorf("failed to find VM CR for node with UUID", nodeUUID)
		return err
	}

	if vmInstance == nil {
		// This is the case where VirtualMachine is not present on the VC and VM CR
		// is also not found in the API server. The detach will be marked as
		// successful in CnsNodeVmBatchAttachment.
		log.Infof("VM CR is not present with UUID: %s in namespace: %s. "+
			"Removing finalizer on CnsNodeVmBatchAttachment: %s instance.",
			nodeUUID, namespace, instanceName)
		return nil
	}
	if vmInstance.DeletionTimestamp != nil {
		// This is the case where VirtualMachine is not present on the VC and VM CR
		// has the deletionTimestamp set. The CnsNodeVmBatchAttachment
		// can be marked as a success since the VM CR has deletionTimestamp set
		log.Infof("VM on VC not found but VM CR with UUID: %s "+
			"is still present in namespace: %s and is being deleted. "+
			"Hence returning success.", nodeUUID, instanceName)
		return nil
	}
	// This is a case where VirtualMachine is not present on the VC and VM CR
	// does not have the deletionTimestamp set.
	// This is an error and will need to be retried.
	return fmt.Errorf("VM with nodeUUID %s not present on VM but is present in K8s cluster. unexpected failure",
		nodeUUID)

}*/

// getNamespacedPvcName take namespace and pvcName sends back namespace + "/" + pvcName.
func getNamespacedPvcName(namespace string, pvcName string) string {
	return namespace + "/" + pvcName
}

// findVolumesToAttachAndDetachFromInstanceSpec takes the volumes in the instance's spec
// and finds out if they're in sync with the FCDs attached to the VM.
// If there is even 1 volume in spec which is not among FCDs attached to the VM,
// batch attach has to be called for all PVCs in instance spec.
//
// This function also finds out which are the volumes to detach by finding out which are
// the volumes present in attachedFCDs but not in spec of the instance.
func findVolumesToAttachAndDetachFromInstanceSpec(ctx context.Context,
	instance *cnsnodevmbatchattachmentv1alpha1.CnsNodeVmBatchAttachment,
	attachedFCDs map[string]bool, pvcsInSpec map[string]string,
	volumeIdsInSpec map[string]string) (pvcsToAttach map[string]string,
	pvcsToDetach map[string]string, err error) {
	log := logger.GetLogger(ctx)

	// Map of PVC name to volume name (as given in VM spec).
	pvcsToAttach = make(map[string]string)
	// Map contains PVCs which need to be detached.
	// We cannot rely on volume name in this case because
	// the volume's entry has been deleted from the spec.
	pvcsToDetach = make(map[string]string)

	isSpecInconsistentWithVm := false
	// Add those PVCs to volumesToAttach list
	// which are present in the instance's spec
	// but are not present int the attachedFCDs list.
	for _, volume := range instance.Spec.Volumes {
		// Find the PVC's volumeID
		namespacedPvcName := getNamespacedPvcName(instance.Namespace, volume.PersistentVolumeClaim.ClaimName)
		pvcVolumeId, exists := commonco.ContainerOrchestratorUtility.GetVolumeIDFromPVCName(namespacedPvcName)
		if !exists {
			msg := fmt.Sprintf("failed to find volumeID for PVC %s", volume.PersistentVolumeClaim.ClaimName)
			log.Errorf(msg)
			err = errors.New(msg)
			return
		}
		// If the even 1 PVC is not found in attachedFCDs list,
		// call
		if _, ok := attachedFCDs[pvcVolumeId]; !ok {
			isSpecInconsistentWithVm = true
			break
		}
	}

	// Add PVCs in spec to PVCs to attach list.
	if isSpecInconsistentWithVm {
		pvcsToAttach = pvcsInSpec
	}

	/// Add those volumes to to volumesToDetach list
	// which are present in attachedFCDs list but not in
	// instance spec.
	for attachedFcdId := range attachedFCDs {
		if _, ok := volumeIdsInSpec[attachedFcdId]; !ok {
			pvc, _, exists := commonco.ContainerOrchestratorUtility.GetPVCNameFromCSIVolumeID(attachedFcdId)
			if !exists {
				msg := fmt.Sprintf("failed to find PVC for volumeID %s in cluster", attachedFcdId)
				log.Errorf(msg)
				err = errors.New(msg)
				return
			}
			pvcsToDetach[pvc] = attachedFcdId
		}
	}
	return
}

// isInstanceSpecAndStatusInSync finds out the instance's spec and status are in sync.
// If there are volumes in spec for which status is not present or for which
// status is not in healthy state, then batch attach must be retried.
//
// If there are volumes in status for which there are no entries in spec, then
// entry for those volumes can be removed fromt he status.
func isInstanceSpecAndStatusInSync(ctx context.Context,
	instance *cnsnodevmbatchattachmentv1alpha1.CnsNodeVmBatchAttachment,
	client client.Client,
	attachedFCDs map[string]bool, pvcsToAttach map[string]string,
	pvcsInSpec map[string]string) (bool, error) {
	log := logger.GetLogger(ctx)

	for _, volumeSpec := range instance.Spec.Volumes {
		volumeFoundInStatus := false
		for _, volumeStatus := range instance.Status.VolumeStatus {
			if volumeSpec.Name != volumeStatus.Name {
				continue
			}
			volumeFoundInStatus = true
			// If status of the volume is not healthy, batch attach needs to be retried.
			if volumeStatus.PersistentVolumeClaim.Error != "" ||
				!volumeStatus.PersistentVolumeClaim.Attached {
				log.Infof("Status for PVC %s in instance %s is not healthy. Retry attach.",
					volumeSpec.PersistentVolumeClaim.ClaimName, instance.Name)
				return false, nil
			}
		}
		// If status for a volume could not be found, then this means batch attach needs to be retried.
		if !volumeFoundInStatus {
			log.Infof("Status for PVC %s in instance %s not found. Retry attach.",
				volumeSpec.PersistentVolumeClaim.ClaimName, instance.Name)
			return false, nil
		}
	}

	// If status for all volumes in spec is healthy, and number of volumes in spec and status is also the same,
	// nothing to be and the reconciliationc an end.
	if len(instance.Spec.Volumes) == len(instance.Status.VolumeStatus) {
		log.Debugf("Status for every volume in Spec is healthy in Status for instance %s",
			instance.Name)
		return true, nil
	}

	// Remove entried them from instance status and update the instance.
	specAndStatusInSync := true
	for _, volumeStatus := range instance.Status.VolumeStatus {
		if _, existsInSpec := pvcsInSpec[volumeStatus.PersistentVolumeClaim.ClaimName]; !existsInSpec {
			log.Infof("Status for a PVC %s found in instance %s whbut it is not present in Spec. "+
				"Removing it from instance", volumeStatus.PersistentVolumeClaim.ClaimName, instance.Name)
			deleteVolumeFromStatus(volumeStatus.PersistentVolumeClaim.ClaimName, instance)
			specAndStatusInSync = false
		}
	}

	if !specAndStatusInSync {
		err := updateInstance(ctx, client, instance)
		if err != nil {
			log.Errorf("failed to update CnsNodeVmBatchAttachment %s. Err: +%v", instance.Name, err)
			return false, err
		}
	}
	return true, nil
}

// getVolumesToAttachAndDetach returns list of volumes to attach and to detach by taking a diff of
// volumes in spec and in attachedFCDs list.
func getVolumesToAttachAndDetach(ctx context.Context,
	instance *cnsnodevmbatchattachmentv1alpha1.CnsNodeVmBatchAttachment,
	client client.Client,
	attachedFCDs map[string]bool) (pvcsToAttach map[string]string,
	pvcsToDetach map[string]string, err error) {
	log := logger.GetLogger(ctx)

	pvcsInSpec, volumeIdsInSpec, err := getPvcVolumeIdMapsInSpec(ctx, instance)
	if err != nil {
		return
	}

	// First step is to find out the volumes to attach and detach by taking a diff between
	// the instance spec and the FCDs attached to the VM on vCenter.
	pvcsToAttach, pvcsToDetach, err = findVolumesToAttachAndDetachFromInstanceSpec(ctx, instance,
		attachedFCDs, pvcsInSpec, volumeIdsInSpec)
	if err != nil {
		log.Errorf("failed to find volumes to attach and volumes to detach from instance spec. Err: %s", err)
		return
	}

	// If there are PVCs to be attached or detached, then take care of those first
	// before comparing status of the CR.
	// Status may become up to date after this.
	if len(pvcsToAttach) != 0 || len(pvcsToDetach) != 0 {
		return
	}

	// This step determines if reconciliation can be ended for this instance.
	//
	// A CnsNodeVmBatchAttachment instance is considered completed when spec and status are in sync and
	// there are no volumes to be attached or detached.
	//
	// When there are no PVCs to attach and no PVCs to detach because
	// spec reflects the state of the VM on vCenter, then find out of if Status is also in sync.
	// If status has volumes which are in failed state, then add them to pvcsToAttach.
	// If it has volumes which are not present in spec, remove their entry from the status because
	/// it means that that volume already got detached.
	specAndStatusInSync, err := isInstanceSpecAndStatusInSync(ctx, instance, client,
		attachedFCDs, pvcsToAttach, pvcsInSpec)
	if err != nil {
		log.Errorf("failed to check if spec and status of instance %s are in sync", instance.Name)
		return
	}
	// If Spec and Status are in
	if specAndStatusInSync {
		log.Infof("Spec and status for instance %s are in sync. Nothing to be done.", instance.Name)
		return
	}
	// If Spec and status are not in sync, call Batch Attach for all volumes in spec.
	pvcsToAttach = pvcsInSpec
	return
}

// updateInstance updates the given nodevmbatchattachment instance.
func updateInstance(ctx context.Context, client client.Client,
	instance *cnsnodevmbatchattachmentv1alpha1.CnsNodeVmBatchAttachment) error {
	log := logger.GetLogger(ctx)
	err := client.Update(ctx, instance)
	if err != nil {
		log.Errorf("failed to update CnsNodeVmBatchAttachment instance: %q on namespace: %q. Error: %+v",
			instance.Name, instance.Namespace, err)
		return err
	}
	return nil
}

// updateInstanceWithErrorVolumeName finds the given's volumeName's status in the instance status
// and updates it with error.
// It will add a new status for the volume if it does not already exist.
func updateInstanceWithErrorVolumeName(instance *cnsnodevmbatchattachmentv1alpha1.CnsNodeVmBatchAttachment,
	volumeName string, pvc string, errMsg string) {
	for i, volume := range instance.Status.VolumeStatus {
		if volume.Name != volumeName {
			continue
		}
		newVolumeStatus := cnsnodevmbatchattachmentv1alpha1.VolumeStatus{
			Name: volumeName,
			PersistentVolumeClaim: cnsnodevmbatchattachmentv1alpha1.PersistentVolumeClaimStatus{
				Attached:    false,
				ClaimName:   pvc,
				CnsVolumeID: volume.PersistentVolumeClaim.CnsVolumeID,
				Diskuuid:    volume.PersistentVolumeClaim.Diskuuid,
				Error:       errMsg,
			},
		}
		instance.Status.VolumeStatus[i] = newVolumeStatus
		return

	}

	newVolumeStatus := cnsnodevmbatchattachmentv1alpha1.VolumeStatus{
		Name: volumeName,
		PersistentVolumeClaim: cnsnodevmbatchattachmentv1alpha1.PersistentVolumeClaimStatus{
			ClaimName: pvc,
			Attached:  false,
			Error:     errMsg,
		},
	}
	instance.Status.VolumeStatus = append(instance.Status.VolumeStatus, newVolumeStatus)
}

// updateInstanceWithErrorVolumeName finds the given's PVC's status in the instance status
// and updates it with error.
func updateInstanceWithErrorPvc(instance *cnsnodevmbatchattachmentv1alpha1.CnsNodeVmBatchAttachment,
	pvc string, errMsg string) {
	for i, volume := range instance.Status.VolumeStatus {
		if volume.PersistentVolumeClaim.ClaimName != pvc {
			continue
		}
		newVolumeStatus := cnsnodevmbatchattachmentv1alpha1.VolumeStatus{
			Name: volume.Name,
			PersistentVolumeClaim: cnsnodevmbatchattachmentv1alpha1.PersistentVolumeClaimStatus{
				Attached:    false,
				ClaimName:   pvc,
				CnsVolumeID: volume.PersistentVolumeClaim.CnsVolumeID,
				Diskuuid:    volume.PersistentVolumeClaim.Diskuuid,
				Error:       errMsg,
			},
		}
		instance.Status.VolumeStatus[i] = newVolumeStatus
		return
	}
}

// deleteVolumeFromStatus finds the status of the given volumeName in an instance and deletes its entry.
func deleteVolumeFromStatus(pvc string, instance *cnsnodevmbatchattachmentv1alpha1.CnsNodeVmBatchAttachment) {
	instance.Status.VolumeStatus = slices.DeleteFunc(instance.Status.VolumeStatus,
		func(e cnsnodevmbatchattachmentv1alpha1.VolumeStatus) bool {
			return e.PersistentVolumeClaim.ClaimName == pvc
		})
}

// getPvcVolumeIdMapsInSpec returns the PVCs in instance spec.
// It return two maps:
// 1. PVC to VolumeName
// 2. volumeID to PVC name
func getPvcVolumeIdMapsInSpec(ctx context.Context,
	instance *cnsnodevmbatchattachmentv1alpha1.CnsNodeVmBatchAttachment) (pvcsInSpec map[string]string,
	volumeIdsInSpec map[string]string, err error) {
	log := logger.GetLogger(ctx)

	pvcsInSpec = make(map[string]string)
	volumeIdsInSpec = make(map[string]string)
	for _, volume := range instance.Spec.Volumes {
		namespacedPvcName := getNamespacedPvcName(instance.Namespace, volume.PersistentVolumeClaim.ClaimName)
		volumeId, ok := commonco.ContainerOrchestratorUtility.GetVolumeIDFromPVCName(namespacedPvcName)
		if !ok {
			msg := fmt.Sprintf("failed to find volumeID for PVC %s", volume.PersistentVolumeClaim.ClaimName)
			log.Errorf(msg)
			err = errors.New(msg)
			return
		}
		pvcsInSpec[volume.PersistentVolumeClaim.ClaimName] = volume.Name
		volumeIdsInSpec[volumeId] = volume.PersistentVolumeClaim.ClaimName
	}
	return
}

func getPvcsInSpec(instance *cnsnodevmbatchattachmentv1alpha1.CnsNodeVmBatchAttachment) (map[string]string, error) {
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

// getListOfAttachedVolumesForVM returns list of FCDs (present in the K8s cluster)
// which are attached to given VM on vCenter.
func getListOfAttachedVolumesForVM(ctx context.Context,
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
					attachedFCDs[virtualDisk.VDiskId.Id] = true
				}

			} else {
				log.Debugf("failed to obtain virtual disk for device %+v", device)
			}
		}
	}
	return attachedFCDs, nil
}
