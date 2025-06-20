package cnsnodevmbatchattachment

import (
	"context"
	"fmt"
	"slices"

	vmoperatorv1alpha4 "github.com/vmware-tanzu/vm-operator/api/v1alpha4"
	"github.com/vmware/govmomi/object"
	vimtypes "github.com/vmware/govmomi/vim25/types"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	cnsnodevmbatchattachmentv1alpha1 "sigs.k8s.io/vsphere-csi-driver/v3/pkg/apis/cnsoperator/cnsnodevmbatchattachment/v1alpha1"
	cnsvsphere "sigs.k8s.io/vsphere-csi-driver/v3/pkg/common/cns-lib/vsphere"
	"sigs.k8s.io/vsphere-csi-driver/v3/pkg/common/config"
	"sigs.k8s.io/vsphere-csi-driver/v3/pkg/common/utils"
	"sigs.k8s.io/vsphere-csi-driver/v3/pkg/csi/service/logger"
	k8s "sigs.k8s.io/vsphere-csi-driver/v3/pkg/kubernetes"
	cnsoperatortypes "sigs.k8s.io/vsphere-csi-driver/v3/pkg/syncer/cnsoperator/types"
	cnsoperatorutil "sigs.k8s.io/vsphere-csi-driver/v3/pkg/syncer/cnsoperator/util"
)

func cnsFinalizerOnCRDInstanceExists(instance *cnsnodevmbatchattachmentv1alpha1.CnsNodeVmBatchAttachment) bool {
	return controllerutil.ContainsFinalizer(instance, cnsoperatortypes.CNSFinalizer)
}

func addCnsFinalizerOnCRDInstance(ctx context.Context,
	instance *cnsnodevmbatchattachmentv1alpha1.CnsNodeVmBatchAttachment,
	c client.Client) error {
	return k8s.PatchFinalizers(ctx, c, instance, append(instance.Finalizers, cnsoperatortypes.CNSFinalizer))
}

// isVmCrPresent checks whether VM CR is present in SV namespace
// with given vmuuid and returns the VirtualMachine CR object if it is found
func isVmCrPresent(ctx context.Context, vmOperatorClient client.Client,
	vmuuid string, namespace string) (*vmoperatorv1alpha4.VirtualMachine, error) {
	log := logger.GetLogger(ctx)
	vmList, err := utils.ListVirtualMachines(ctx, vmOperatorClient, namespace)
	if err != nil {
		msg := fmt.Sprintf("failed to list virtualmachines with error: %+v", err)
		log.Error(msg)
		return nil, err
	}
	for _, vmInstance := range vmList.Items {
		if vmInstance.Status.BiosUUID == vmuuid {
			msg := fmt.Sprintf("VM CR with BiosUUID: %s found in namespace: %s",
				vmuuid, namespace)
			log.Infof(msg)
			return &vmInstance, nil
		}
	}
	msg := fmt.Sprintf("VM CR with BiosUUID: %s not found in namespace: %s",
		vmuuid, namespace)
	log.Info(msg)
	return nil, nil
}

func validateVmCrWhenNodeVmIsDeleted(ctx context.Context, client client.Client,
	nodeUUID string, namespace string, instanceName string) error {
	log := logger.GetLogger(ctx)

	vmInstance, err := isVmCrPresent(ctx, client, nodeUUID, namespace)
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
	return fmt.Errorf("VM with nodeUUID %s not present on VM but is present in K8s cluster. unexpected failure", nodeUUID)

}

// volumesToAttachAndDetach returns list of volumes to attach and to detach by taking a diff of
// volumes in spec and in attachedFCDs list.
func volumesToAttachAndDetach(ctx context.Context, instance *cnsnodevmbatchattachmentv1alpha1.CnsNodeVmBatchAttachment,
	pvcToVolumeId map[string]string, volumeIdToPvc map[string]string,
	attachedFCDs map[string]bool) (map[string]string, map[string]string, error) {
	log := logger.GetLogger(ctx)

	// Map of PVC name to volume name (as given by VM spec).
	pvcsToAttach := make(map[string]string)
	// Map contains PVCs which need to be detached.
	// We cannot rely on volume name in this case because
	// the volume's entry has been deleted from the spec.
	pvcsToDetach := make(map[string]string)

	// Map of volumeIDs from instance spec for easy lookup.
	volumeIdsInSpec := make(map[string]bool)
	// Map of PVCs from instance spec for easy lookup.
	pvcsInSpec := make(map[string]bool)

	// Add those PVCs to volumesToAttach list
	// which are present in the instance's spec
	// but are not present int the attachedFCDs list.
	for _, volume := range instance.Spec.Volumes {
		// Find the PVC's volumeID
		pvcVolumeId, exists := pvcToVolumeId[volume.PersistentVolumeClaim.ClaimName]
		if !exists {
			log.Errorf("failed to find volumeID for PVC %s in cluster", volume.PersistentVolumeClaim.ClaimName)
			return pvcsToAttach, pvcsToDetach,
				fmt.Errorf("failed to find volumeID for PVC %s in cluster", volume.PersistentVolumeClaim.ClaimName)
		}
		// If the PVC is not found in attachedFCDs list,
		// add it to volumesToAttach list.
		if _, ok := attachedFCDs[pvcVolumeId]; !ok {
			pvcsToAttach[volume.PersistentVolumeClaim.ClaimName] = volume.Name
		}

		// Store PVC's volumeID for easy lookup.
		volumeIdsInSpec[pvcVolumeId] = true
		pvcsInSpec[volume.PersistentVolumeClaim.ClaimName] = true
	}

	/// Add those volumes to to volumesToDetach list
	// which are present in attachedFCDs list but not in
	// instance spec.
	for attachedFcdId := range attachedFCDs {
		if _, ok := volumeIdsInSpec[attachedFcdId]; !ok {
			pvc, exists := volumeIdToPvc[attachedFcdId]
			if !exists {
				log.Errorf("failed to find PVC for volumeID %s in cluster", attachedFcdId)
				return pvcsToAttach, pvcsToDetach, fmt.Errorf("failed to find PVC for volumeID %s in cluster", attachedFcdId)

			}
			pvcsToDetach[pvc] = attachedFcdId
		}
	}

	err := verifySpecAndStatusAreInSync(ctx, instance, pvcsToAttach, pvcsToDetach, pvcsInSpec, pvcToVolumeId)
	if err != nil {
		log.Errorf("failed to find if spec and status are in sync for instance %s. Err: %s ", instance.Name, err)
		return pvcsToAttach, pvcsToDetach, err
	}

	return pvcsToAttach, pvcsToDetach, nil
}

func verifySpecAndStatusAreInSync(ctx context.Context,
	instance *cnsnodevmbatchattachmentv1alpha1.CnsNodeVmBatchAttachment, pvcsToAttach map[string]string,
	pvcsToDetach map[string]string, pvcsInSpec map[string]bool, pvcToVolumeId map[string]string) error {
	log := logger.GetLogger(ctx)

	pvcsInStatus := make(map[string]bool)

	for _, volumeInStatus := range instance.Status.VolumeStatus {
		if _, ok := pvcsInSpec[volumeInStatus.PersistentVolumeClaim.ClaimName]; !ok {
			log.Infof("PVC %s not found in volume spec. Adding to volumesToDeatch list",
				volumeInStatus.PersistentVolumeClaim.ClaimName)
			pvcVolId, exists := pvcToVolumeId[volumeInStatus.PersistentVolumeClaim.ClaimName]
			if !exists {
				return fmt.Errorf("failed to find volumeId for PVC %s", volumeInStatus.PersistentVolumeClaim.ClaimName)
			}
			pvcsToDetach[volumeInStatus.PersistentVolumeClaim.ClaimName] = pvcVolId
		}
		pvcsInStatus[volumeInStatus.PersistentVolumeClaim.ClaimName] = true
	}

	for _, volumeInSpec := range instance.Spec.Volumes {
		if _, ok := pvcsInStatus[volumeInSpec.PersistentVolumeClaim.ClaimName]; !ok {
			log.Infof("PVC %s not found in volume status. Adding to volumesToAttach list",
				volumeInSpec.PersistentVolumeClaim.ClaimName)
			pvcsToAttach[volumeInSpec.PersistentVolumeClaim.ClaimName] = volumeInSpec.Name
		}
	}

	return nil

}

func updateCnsNodeVmBatchAttachment(ctx context.Context, client client.Client,
	instance *cnsnodevmbatchattachmentv1alpha1.CnsNodeVmBatchAttachment) error {
	log := logger.GetLogger(ctx)
	err := client.Update(ctx, instance)
	if err != nil {
		if apierrors.IsConflict(err) {
			log.Infof("Observed conflict while updating CnsNodeVmBatchAttachment instance %q in namespace %q."+
				"Reapplying changes to the latest instance.", instance.Name, instance.Namespace)

			// Fetch the latest instance version from the API server and apply changes on top of it.
			latestInstance := &cnsnodevmbatchattachmentv1alpha1.CnsNodeVmBatchAttachment{}
			err = client.Get(ctx, types.NamespacedName{Name: instance.Name, Namespace: instance.Namespace}, latestInstance)
			if err != nil {
				log.Errorf("Error reading the CnsNodeVmBatchAttachment with name: %q on namespace: %q. Err: %+v",
					instance.Name, instance.Namespace, err)
				// Error reading the object - return error
				return err
			}

			// The callers of updateCnsNodeVMBatchAttachment are either updating the instance finalizers or
			// one of the fields in instance status.
			// Hence we copy only finalizers and Status from the instance passed for update
			// on the latest instance from API server.
			latestInstance.Finalizers = instance.Finalizers
			latestInstance.Status = *instance.Status.DeepCopy()

			err := client.Update(ctx, latestInstance)
			if err != nil {
				log.Errorf("failed to update CnsNodeVmBatchAttachment instance: %q on namespace: %q. Error: %+v",
					instance.Name, instance.Namespace, err)
				return err
			}
			return nil
		} else {
			log.Errorf("failed to update CnsNodeVmBatchAttachment instance: %q on namespace: %q. Error: %+v",
				instance.Name, instance.Namespace, err)
		}
	}
	return err
}

// getDatacenterObject returns the datacenter object for the vCenter.
func getDatacenterObject(ctx context.Context,
	instance *cnsnodevmbatchattachmentv1alpha1.CnsNodeVmBatchAttachment,
	configInfo *config.ConfigurationInfo) (*cnsvsphere.Datacenter, error) {
	log := logger.GetLogger(ctx)

	vcdcMap, err := cnsoperatorutil.GetVCDatacentersFromConfig(configInfo.Cfg)
	if err != nil {
		log.Errorf("failed to find datacenter moref from config for CnsNodeVmBatchAttachment "+
			"request with name: %q on namespace: %q. Err: %+v", instance.Name, instance.Namespace, err)
		return nil, err
	}

	var host, dcMoref string
	for key, value := range vcdcMap {
		host = key
		dcMoref = value[0]
	}

	// Get datacenter object
	var dc *cnsvsphere.Datacenter
	vcenter, err := cnsvsphere.GetVirtualCenterInstance(ctx, configInfo, false)
	if err != nil {
		log.Errorf("failed to get virtual center instance with error: %v", err)
		return nil, err
	}
	err = vcenter.Connect(ctx)
	if err != nil {
		log.Errorf("failed to connect to VC with error: %v", err)
		return nil, err
	}
	dc = &cnsvsphere.Datacenter{
		Datacenter: object.NewDatacenter(vcenter.Client.Client,
			vimtypes.ManagedObjectReference{
				Type:  "Datacenter",
				Value: dcMoref,
			}),
		VirtualCenterHost: host,
	}
	return dc, nil
}

// getNodeVm returns the nodeVM for the given nodeUUID.
func getNodeVm(ctx context.Context,
	instance *cnsnodevmbatchattachmentv1alpha1.CnsNodeVmBatchAttachment,
	configInfo *config.ConfigurationInfo) (*cnsvsphere.VirtualMachine, error) {
	log := logger.GetLogger(ctx)

	nodeUUID := instance.Spec.NodeUUID

	dc, err := getDatacenterObject(ctx, instance, configInfo)
	if err != nil {
		log.Errorf("failed to get datacenter for node: %s. Err: %+q", nodeUUID, err)
		return nil, err
	}

	nodeVM, err := dc.GetVirtualMachineByUUID(ctx, nodeUUID, false)
	if err != nil {
		log.Errorf("failed to find the VM with UUID: %q for CnsNodeVmbatchAttachment "+
			"request with name: %q on namespace: %q. Err: %+v",
			nodeUUID, instance.Name, instance.Namespace, err)
		return nil, err
	}

	return nodeVM, nil
}

// removeFinalizerFromCRDInstance will remove the CNS Finalizer, cns.vmware.com,
// from a given nodevmattachment instance.
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

func updateInstanceWithErrorVolumeName(instance *cnsnodevmbatchattachmentv1alpha1.CnsNodeVmBatchAttachment,
	volumeName string, pvc string, errMsg string) {
	for i, volume := range instance.Status.VolumeStatus {
		if volume.Name == volumeName {
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

func updateInstanceWithErrorPvc(instance *cnsnodevmbatchattachmentv1alpha1.CnsNodeVmBatchAttachment,
	pvc string, errMsg string) {
	for i, volume := range instance.Status.VolumeStatus {
		if volume.PersistentVolumeClaim.ClaimName == pvc {
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
}

func deleteVolumeFromStatus(pvc string, instance *cnsnodevmbatchattachmentv1alpha1.CnsNodeVmBatchAttachment) {
	instance.Status.VolumeStatus = slices.DeleteFunc(instance.Status.VolumeStatus,
		func(e cnsnodevmbatchattachmentv1alpha1.VolumeStatus) bool {
			return e.PersistentVolumeClaim.ClaimName == pvc
		})
}

func getVolumeInInstanceSpec(instance *cnsnodevmbatchattachmentv1alpha1.CnsNodeVmBatchAttachment,
	pvcToVolumeId map[string]string) (map[string]string, error) {
	volumesInSpec := make(map[string]string)
	for _, volume := range instance.Spec.Volumes {
		volumeId, ok := pvcToVolumeId[volume.PersistentVolumeClaim.ClaimName]
		if !ok {
			return volumesInSpec,
				fmt.Errorf("failed to find volumeID for PVC %s", volume.PersistentVolumeClaim.ClaimName)
		}
		volumesInSpec[volume.PersistentVolumeClaim.ClaimName] = volumeId
	}
	return volumesInSpec, nil
}
