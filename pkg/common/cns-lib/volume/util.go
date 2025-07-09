/*
Copyright 2019 The Kubernetes Authors.

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

package volume

import (
	"context"
	"errors"
	"reflect"
	"strings"

	"github.com/davecgh/go-spew/spew"
	uuidlib "github.com/google/uuid"
	"github.com/vmware/govmomi/cns"
	cnstypes "github.com/vmware/govmomi/cns/types"
	"github.com/vmware/govmomi/object"
	"github.com/vmware/govmomi/property"
	"github.com/vmware/govmomi/vim25/mo"
	"github.com/vmware/govmomi/vim25/soap"
	"github.com/vmware/govmomi/vim25/types"

	cnsvsphere "sigs.k8s.io/vsphere-csi-driver/v3/pkg/common/cns-lib/vsphere"
	csifault "sigs.k8s.io/vsphere-csi-driver/v3/pkg/common/fault"
	"sigs.k8s.io/vsphere-csi-driver/v3/pkg/csi/service/logger"
)

func validateManager(ctx context.Context, m *defaultManager) error {
	log := logger.GetLogger(ctx)
	if m.virtualCenter == nil {
		return logger.LogNewError(log, "virtual Center connection not established")
	}
	return nil
}

// IsDiskAttached checks if the volume is attached to the VM.
// If the volume is attached to the VM, return disk uuid of the volume,
// else return empty string.
func IsDiskAttached(ctx context.Context, vm *cnsvsphere.VirtualMachine, volumeID string,
	checkNVMeController bool) (string, error) {
	log := logger.GetLogger(ctx)
	// Verify if the volume id is on the VM backing virtual disk devices.
	vmDevices, err := vm.Device(ctx)
	if err != nil {
		log.Errorf("failed to get devices from vm: %s", vm.InventoryPath)
		return "", err
	}
	if len(vmDevices) == 0 {
		return "", logger.LogNewErrorf(log, "virtual devices list is empty for the vm: %s", vm.InventoryPath)
	}
	// Build a map of NVME Controller key : NVME controller name.
	// This is needed to check if disk in contention is attached to a NVME
	// controller. The virtual disk devices do not contain the controller type
	// information, but only contain the controller key information.
	nvmeControllerKeyToNameMap := make(map[int32]string)
	for _, device := range vmDevices {
		if vmDevices.TypeName(device) == "VirtualNVMEController" {
			var controllerName string
			if device.GetVirtualDevice().DeviceInfo.GetDescription() != nil {
				controllerName = device.GetVirtualDevice().DeviceInfo.GetDescription().Label
			}
			nvmeControllerKeyToNameMap[device.GetVirtualDevice().Key] = controllerName
		}
	}
	// Iterate through all the virtual disk devices and verify if virtual disk
	// is attached to NVME controller if checkNVMeController is enabled and
	// return NVME UUID by converting the backing UUID, else return the backing
	// UUID (SCSI format UUID).
	for _, device := range vmDevices {
		if vmDevices.TypeName(device) == "VirtualDisk" {
			if virtualDisk, ok := device.(*types.VirtualDisk); ok {
				if virtualDisk.VDiskId != nil && virtualDisk.VDiskId.Id == volumeID {
					virtualDevice := device.GetVirtualDevice()
					if checkNVMeController {
						if value, ok := nvmeControllerKeyToNameMap[virtualDevice.ControllerKey]; ok {
							log.Debug("Found that the disk %q is attached to NVMe controller on vm %q", volumeID, vm)
							if strings.Contains(value, "NVME") {
								if backing, ok := virtualDevice.Backing.(*types.VirtualDiskFlatVer2BackingInfo); ok {
									uuid, err := getNvmeUUID(ctx, backing.Uuid)
									if err != nil {
										log.Errorf("failed to convert uuid to  NvmeV13UUID for the vm: %s", vm.InventoryPath)
										return "", err
									}
									log.Debugf("Successfully converted diskUUID %s to NvmeV13UUID %s for volume %s on vm %+v",
										backing.Uuid, uuid, volumeID, vm)
									return uuid, nil
								}
							}
						}
					}
					if backing, ok := virtualDevice.Backing.(*types.VirtualDiskFlatVer2BackingInfo); ok {
						log.Infof("Found diskUUID %s for volume %s on vm %+v", backing.Uuid, volumeID, vm)
						return backing.Uuid, nil
					}
				}
			}
		}
	}

	log.Debugf("Volume %s is not attached to VM: %+v", volumeID, vm)
	return "", nil
}

// getNvmeUUID returns the NVME formatted UUID.
func getNvmeUUID(ctx context.Context, uuid string) (string, error) {
	log := logger.GetLogger(ctx)
	uuidBytes, err := uuidlib.Parse(uuid)
	if err != nil {
		log.Errorf("Error while parsing uuid with err=%v", err)
		return "", err
	}
	var nvmeUUID uuidlib.UUID
	nvmeUUID[0] = uuidBytes[8]
	nvmeUUID[1] = uuidBytes[9]
	nvmeUUID[2] = uuidBytes[10]
	nvmeUUID[3] = uuidBytes[11]
	nvmeUUID[4] = uuidBytes[12]
	nvmeUUID[5] = uuidBytes[13]
	nvmeUUID[6] = uuidBytes[14]
	nvmeUUID[7] = uuidBytes[15]
	nvmeUUID[8] = ((uuidBytes[0] & 0xF) << 4) | ((uuidBytes[1] & 0xF0) >> 4)
	nvmeUUID[9] = ((uuidBytes[1] & 0xF) << 4) | ((uuidBytes[2] & 0xF0) >> 4)
	nvmeUUID[10] = ((uuidBytes[2] & 0xF) << 4) | ((uuidBytes[3] & 0xF0) >> 4)
	nvmeUUID[11] = (uuidBytes[3] & 0xF) | (uuidBytes[0] & 0xF0)
	nvmeUUID[12] = uuidBytes[4]
	nvmeUUID[13] = uuidBytes[5]
	nvmeUUID[14] = uuidBytes[6]
	nvmeUUID[15] = uuidBytes[7]
	return nvmeUUID.String(), nil
}

// IsDiskAttachedToVMs checks if the volume is attached to any of the input VMs.
// If the volume is attached to the VM, return disk uuid of the volume, else
// return empty string.
func IsDiskAttachedToVMs(ctx context.Context, volumeID string, vms []*cnsvsphere.VirtualMachine,
	checkNVMeController bool) (string, error) {
	for _, vm := range vms {
		diskUUID, err := IsDiskAttached(ctx, vm, volumeID, checkNVMeController)
		if diskUUID != "" || err != nil {
			return diskUUID, err
		}
	}
	return "", nil
}

// updateQueryResult helps update CnsQueryResult to populate
// volume.Metadata.EntityMetadata.ClusterID with value from
// volume.Metadata.ContainerCluster.ClusterId. This is required to make
// driver code compatible to vSphere 67 release.
func updateQueryResult(ctx context.Context, m *defaultManager, res *cnstypes.CnsQueryResult) *cnstypes.CnsQueryResult {
	if m.virtualCenter.Client.Version == cns.ReleaseVSAN67u3 {
		log := logger.GetLogger(ctx)
		for volumeIndex, volume := range res.Volumes {
			for metadataIndex, metadata := range volume.Metadata.EntityMetadata {
				if cnsK8sMetaEntityMetadata, ok := metadata.(*cnstypes.CnsKubernetesEntityMetadata); ok {
					cnsK8sMetaEntityMetadata.ClusterID = volume.Metadata.ContainerCluster.ClusterId
					volume.Metadata.EntityMetadata[metadataIndex] = cnsK8sMetaEntityMetadata
				} else {
					log.Debugf("metadata: %v is not of type CnsKubernetesEntityMetadata", metadata)
				}
			}
			res.Volumes[volumeIndex] = volume
		}
	}
	return res
}

// setupConnection connects to CNS and updates VSphereUser to session user.
func setupConnection(ctx context.Context, virtualCenter *cnsvsphere.VirtualCenter,
	spec *cnstypes.CnsVolumeCreateSpec) error {
	log := logger.GetLogger(ctx)
	// Set up the VC connection.
	err := virtualCenter.ConnectCns(ctx)
	if err != nil {
		log.Errorf("ConnectCns failed with err: %+v", err)
		return err
	}
	// If the VSphereUser in the CreateSpec is different from session user,
	// update the CreateSpec.
	s, err := virtualCenter.Client.SessionManager.UserSession(ctx)
	if err != nil {
		log.Errorf("failed to get usersession with err: %v", err)
		return err
	}
	// Refer to this issue - https://github.com/vmware/govmomi/issues/2922
	// Session Manager -> UserSession can return nil user session with nil error
	// so handling the case for nil session.
	if s == nil {
		return errors.New("nil session obtained from session manager")
	}
	if s.UserName != spec.Metadata.ContainerCluster.VSphereUser {
		log.Debugf("Update VSphereUser from %s to %s", spec.Metadata.ContainerCluster.VSphereUser, s.UserName)
		spec.Metadata.ContainerCluster.VSphereUser = s.UserName
	}
	return nil
}

// getPendingCreateVolumeTaskFromMap returns the CreateVolume task for a volume
// stored in the volumeTaskMap.
func getPendingCreateVolumeTaskFromMap(ctx context.Context, volNameFromInputSpec string) *object.Task {
	var task *object.Task
	log := logger.GetLogger(ctx)
	taskDetailsInMap, ok := volumeTaskMap[volNameFromInputSpec]
	if ok {
		task = taskDetailsInMap.task
		log.Infof("CreateVolume task still pending for Volume: %q, with taskInfo: %+v",
			volNameFromInputSpec, task)
	}
	return task
}

// invokeCNSCreateVolume truncates the input volume name and invokes a
// CreateVolume operation for that volume on CNS.
func invokeCNSCreateVolume(ctx context.Context, virtualCenter *cnsvsphere.VirtualCenter,
	spec *cnstypes.CnsVolumeCreateSpec) (*object.Task, error) {
	var cnsCreateSpecList []cnstypes.CnsVolumeCreateSpec
	log := logger.GetLogger(ctx)
	// Truncate the volume name to make sure the name is within 80 characters
	// before calling CNS.
	if len(spec.Name) > maxLengthOfVolumeNameInCNS {
		volNameAfterTruncate := spec.Name[0 : maxLengthOfVolumeNameInCNS-1]
		log.Infof("Create Volume with name %s is too long, truncate it to %s", spec.Name, volNameAfterTruncate)
		spec.Name = volNameAfterTruncate
		log.Debugf("CNS Create Volume is called with %v", spew.Sdump(*spec))
	}
	cnsCreateSpecList = append(cnsCreateSpecList, *spec)
	task, err := virtualCenter.CnsClient.CreateVolume(ctx, cnsCreateSpecList)
	if err != nil {
		log.Errorf("CNS CreateVolume failed from vCenter %q with err: %v", virtualCenter.Config.Host, err)
		return nil, err
	}
	return task, nil
}

// isStaticallyProvisioned returns true if the input spec is for a statically
// provisioned volume.
func isStaticallyProvisioned(spec *cnstypes.CnsVolumeCreateSpec) bool {
	var isStaticallyProvisionedBlockVolume bool
	var isStaticallyProvisionedFileVolume bool
	if spec.VolumeType == string(cnstypes.CnsVolumeTypeBlock) {
		blockBackingDetails, ok := spec.BackingObjectDetails.(*cnstypes.CnsBlockBackingDetails)
		if ok && (blockBackingDetails.BackingDiskId != "" || blockBackingDetails.BackingDiskUrlPath != "") {
			isStaticallyProvisionedBlockVolume = true
		}
	}
	if spec.VolumeType == string(cnstypes.CnsVolumeTypeFile) {
		fileBackingDetails, ok := spec.BackingObjectDetails.(*cnstypes.CnsVsanFileShareBackingDetails)
		if ok && fileBackingDetails.BackingFileId != "" {
			isStaticallyProvisionedFileVolume = true
		}
	}
	return isStaticallyProvisionedBlockVolume || isStaticallyProvisionedFileVolume
}

// getTaskResultFromTaskInfo returns the task result for a given task.
func getTaskResultFromTaskInfo(ctx context.Context, taskInfo *types.TaskInfo) (cnstypes.BaseCnsVolumeOperationResult,
	error) {
	log := logger.GetLogger(ctx)
	// Get the taskResult.
	taskResult, err := cns.GetTaskResult(ctx, taskInfo)
	if err != nil {
		log.Errorf("failed to get task result for task with ID: %q, opId: %q error: %+v",
			taskInfo.Task.Value, taskInfo.ActivationId, err)
		return nil, err
	}
	return taskResult, nil
}

// validateCreateVolumeResponseFault validates if the CreateVolume task fault.
// If it failed with an AlreadyRegistered fault, then it returns the
// CnsVolumeInfo object. Otherwise, it returns an error.
func validateCreateVolumeResponseFault(ctx context.Context, name string,
	resp *cnstypes.CnsVolumeOperationResult) (*CnsVolumeInfo, error) {
	log := logger.GetLogger(ctx)
	fault, ok := resp.Fault.Fault.(*cnstypes.CnsAlreadyRegisteredFault)
	if ok {
		log.Infof("Volume is already registered with CNS. VolumeName: %q, volumeID: %q",
			name, fault.VolumeId.Id)
		return &CnsVolumeInfo{
			DatastoreURL: "",
			VolumeID:     fault.VolumeId,
		}, nil
	}

	return nil, logger.LogNewErrorf(log, "failed to create volume with fault: %q", spew.Sdump(resp.Fault))

}

// getCnsVolumeInfoFromTaskResult retrieves the datastoreURL and returns the
// CnsVolumeInfo object.
func getCnsVolumeInfoFromTaskResult(ctx context.Context, virtualCenter *cnsvsphere.VirtualCenter, volumeName string,
	volumeID cnstypes.CnsVolumeId, taskResult cnstypes.BaseCnsVolumeOperationResult) (*CnsVolumeInfo, string, error) {
	log := logger.GetLogger(ctx)
	var datastoreURL string
	volumeCreateResult := interface{}(taskResult).(*cnstypes.CnsVolumeCreateResult)
	log.Debugf("volumeCreateResult.PlacementResults :%v", volumeCreateResult.PlacementResults)
	if volumeCreateResult.PlacementResults != nil {
		var datastoreMoRef types.ManagedObjectReference
		for _, placementResult := range volumeCreateResult.PlacementResults {
			// For the datastore which the volume is provisioned, placementFaults
			// will not be set.
			if len(placementResult.PlacementFaults) == 0 {
				datastoreMoRef = placementResult.Datastore
				break
			}
		}
		var dsMo mo.Datastore
		pc := property.DefaultCollector(virtualCenter.Client.Client)
		err := pc.RetrieveOne(ctx, datastoreMoRef, []string{"summary"}, &dsMo)
		faultType := ""
		if err != nil {
			faultType = ExtractFaultTypeFromErr(ctx, err)
			return nil, faultType, logger.LogNewErrorf(log, "failed to retrieve datastore summary property: %v", err)
		}
		datastoreURL = dsMo.Summary.Url
	}
	log.Infof("Volume created successfully. VolumeName: %q, volumeID: %q",
		volumeName, volumeID.Id)
	log.Debugf("CreateVolume volumeId %q is placed on datastore %q",
		volumeID, datastoreURL)
	return &CnsVolumeInfo{
		DatastoreURL: datastoreURL,
		VolumeID:     volumeID,
	}, "", nil
}

// ExtractFaultTypeFromErr extracts the fault type from err.
// Return the vim fault type if the input err is a SoapFault, and can exract the fault type of VimFault.
// Otherwise, it returns fault type as "csi.fault.Internal".
func ExtractFaultTypeFromErr(ctx context.Context, err error) string {
	log := logger.GetLogger(ctx)
	var faultType string
	if soap.IsSoapFault(err) {
		soapFault := soap.ToSoapFault(err)
		// faultType has the format like "type.XXX", XXX is the specific VimFault type.
		// For example, when VimFault in the error is NotFound, faultType will be "type.NotFound".
		faultType = reflect.TypeOf(soapFault.VimFault()).String()
		log.Infof("Extract vimfault type: +%v. SoapFault Info: +%v from err +%v", faultType, soapFault, err)
		slice := strings.Split(faultType, ".")
		vimFaultType := csifault.VimFaultPrefix + slice[1]
		return vimFaultType
	}
	log.Infof("err %+v is not a SoapFault\n", err)
	return csifault.CSIInternalFault
}

// ExtractFaultTypeFromVolumeResponseResult extracts the fault type from CnsVolumeOperationResult.
// Return the vim fault type is CnsVolumeOperationResult.Fault is set, and can extract the fault type of VimFault.
// Return "" if CnsVolumeOperationResult.Fault not set.
func ExtractFaultTypeFromVolumeResponseResult(ctx context.Context,
	resp *cnstypes.CnsVolumeOperationResult) string {
	log := logger.GetLogger(ctx)
	log.Debugf("Extracting fault type from response: %+v", resp)
	var faultType string
	fault := resp.Fault
	if fault != nil {
		// faultType has the format like "*type.XXX", XXX is the specific VimFault type.
		// For example, when CnsVolumeOperationrResult failed with ResourceInUse, faultType will be "*type.ResourceInUse".
		if fault.Fault != nil {
			faultType = reflect.TypeOf(fault.Fault).String()
			log.Infof("Extract vimfault type: %+v  vimFault: %+v Fault: %+v from resp: %+v",
				faultType, fault.Fault, fault, resp)
			slice := strings.Split(faultType, ".")
			vimFaultType := csifault.VimFaultPrefix + slice[1]
			log.Infof("returning fault: %q", vimFaultType)
			return vimFaultType
		} else {
			faultType = reflect.TypeOf(fault).String()
			log.Infof("Extract fault: %q from resp: %+v",
				faultType, resp)
			log.Infof("returning fault: %q", faultType)
			return faultType
		}
	}
	log.Info("No fault in resp %+v", resp)
	return ""
}

// invokeCNSCreateSnapshot invokes CreateSnapshot operation for that volume on CNS.
func invokeCNSCreateSnapshot(ctx context.Context, virtualCenter *cnsvsphere.VirtualCenter,
	volumeID string, snapshotName string) (*object.Task, error) {
	log := logger.GetLogger(ctx)
	var cnsSnapshotCreateSpecList []cnstypes.CnsSnapshotCreateSpec
	cnsSnapshotCreateSpec := cnstypes.CnsSnapshotCreateSpec{
		VolumeId: cnstypes.CnsVolumeId{
			Id: volumeID,
		},
		Description: snapshotName,
	}
	cnsSnapshotCreateSpecList = append(cnsSnapshotCreateSpecList, cnsSnapshotCreateSpec)

	log.Infof("Calling CnsClient.CreateSnapshots: VolumeID [%q] Description [%q]"+
		" cnsSnapshotCreateSpecList [%#v]", volumeID, snapshotName, cnsSnapshotCreateSpecList)
	task, err := virtualCenter.CnsClient.CreateSnapshots(ctx, cnsSnapshotCreateSpecList)
	if err != nil {
		log.Errorf("CNS CreateSnapshots failed from vCenter %q with err: %v", virtualCenter.Config.Host, err)
		return nil, err
	}

	return task, err
}

// invokeCNSDeleteSnapshot invokes DeleteSnapshot operation for that volume on CNS.
func invokeCNSDeleteSnapshot(ctx context.Context, virtualCenter *cnsvsphere.VirtualCenter,
	volumeID string, snapshotID string) (*object.Task, error) {
	log := logger.GetLogger(ctx)
	var cnsSnapshotDeleteSpecList []cnstypes.CnsSnapshotDeleteSpec
	cnsSnapshotDeleteSpec := cnstypes.CnsSnapshotDeleteSpec{
		VolumeId: cnstypes.CnsVolumeId{
			Id: volumeID,
		},
		SnapshotId: cnstypes.CnsSnapshotId{
			Id: snapshotID,
		},
	}
	cnsSnapshotDeleteSpecList = append(cnsSnapshotDeleteSpecList, cnsSnapshotDeleteSpec)

	// Call the CNS DeleteSnapshots
	log.Infof("Calling CnsClient.DeleteSnapshots: VolumeID [%q] SnapshotID [%q] "+
		"cnsSnapshotDeleteSpecList [%#v]", volumeID, snapshotID, cnsSnapshotDeleteSpec)
	deleteSnapshotsTask, err := virtualCenter.CnsClient.DeleteSnapshots(ctx, cnsSnapshotDeleteSpecList)
	if err != nil {
		log.Errorf("CNS DeleteSnapshots failed from vCenter %q with err: %v", virtualCenter.Config.Host, err)
		return nil, err
	}

	return deleteSnapshotsTask, err
}

// getPendingCreateSnapshotTaskFromMap returns the CreateSnapshot task for a snapshot
// stored in the snapshotTaskMap.
func getPendingCreateSnapshotTaskFromMap(ctx context.Context, snapshotName string) *object.Task {
	var task *object.Task
	log := logger.GetLogger(ctx)
	taskDetailsInMap, ok := snapshotTaskMap[snapshotName]
	if ok {
		task = taskDetailsInMap.task
		log.Infof("CreateSnapshot task still pending for Snapshot: %q, with taskInfo: %+v",
			snapshotName, task)
	}
	return task
}

// validateVolumeCapacity queries the CNS volume and validates the returned size with
// input size.
// Returns true if the volume capacity is greater than or equal to the input size.
func validateVolumeCapacity(ctx context.Context, m *defaultManager, volumeID string, size int64) bool {
	log := logger.GetLogger(ctx)
	queryFilter := cnstypes.CnsQueryFilter{
		VolumeIds: []cnstypes.CnsVolumeId{{Id: volumeID}},
	}
	querySelection := cnstypes.CnsQuerySelection{
		Names: []string{
			string(cnstypes.QuerySelectionNameTypeBackingObjectDetails),
		},
	}
	queryResult, queryAllVolumeErr := m.QueryAllVolume(ctx, queryFilter, querySelection)
	if queryAllVolumeErr != nil {
		log.Debugf("failed to query CNS for volume %s with error: %v. Cannot "+
			"determine volume capacity.", volumeID, queryAllVolumeErr)
		return false
	}
	return len(queryResult.Volumes) > 0 &&
		queryResult.Volumes[0].BackingObjectDetails.GetCnsBackingObjectDetails().CapacityInMb >= size
}

// validateSnapshotDeleted queries the CNS snapshot and validates whether the specific snapshot is deleted
// returns true if the specific snapshot is deleted
func validateSnapshotDeleted(ctx context.Context, m *defaultManager, volumeID string, snapshotID string) bool {
	log := logger.GetLogger(ctx)

	snapshotQueryFilter := cnstypes.CnsSnapshotQueryFilter{
		SnapshotQuerySpecs: []cnstypes.CnsSnapshotQuerySpec{
			{VolumeId: cnstypes.CnsVolumeId{Id: volumeID}, SnapshotId: &cnstypes.CnsSnapshotId{Id: snapshotID}},
		},
		Cursor: &cnstypes.CnsCursor{Offset: 0, Limit: 1},
	}

	querySnapshotResult, querySnapshotErr := m.QuerySnapshots(ctx, snapshotQueryFilter)
	if querySnapshotErr != nil {
		log.Errorf("failed to validate for snapshot %s on volume %s with error: %v. "+
			"Cannot determine whether the snapshot is deleted or not", snapshotID, volumeID, querySnapshotErr)
		return false
	}

	if len(querySnapshotResult.Entries) == 0 {
		log.Infof("failed to validate for snapshot %s on volume %s as the "+
			"querySnapshotResult.Entries is empty", snapshotID, volumeID)
		return false
	}

	if querySnapshotResult.Entries[0].Error == nil {
		log.Infof("result of CNS query CNS for snapshot %s on volume %s "+
			"does not contain any error", snapshotID, volumeID)
		return false
	}

	faultInQuerySnapshotResult := querySnapshotResult.Entries[0].Error.Fault
	log.Infof("fault in the result of query CNS for snapshot %s on volume %s: %v",
		snapshotID, volumeID, spew.Sdump(querySnapshotResult.Entries[0].Error.Fault))

	return cnsvsphere.IsCnsSnapshotNotFoundError(soap.WrapVimFault(faultInQuerySnapshotResult))
}

func queryCreatedSnapshotByName(ctx context.Context, m *defaultManager, volumeID string,
	snapshotName string) (*cnstypes.CnsSnapshot, bool) {
	log := logger.GetLogger(ctx)

	snapshotQuerySpec := cnstypes.CnsSnapshotQuerySpec{
		VolumeId: cnstypes.CnsVolumeId{
			Id: volumeID,
		},
	}
	snapshotQueryFilter := cnstypes.CnsSnapshotQueryFilter{
		SnapshotQuerySpecs: []cnstypes.CnsSnapshotQuerySpec{snapshotQuerySpec},
		Cursor: &cnstypes.CnsCursor{
			Offset: 0,
			Limit:  int64(128),
		},
	}

	snapshotQueryResult, err := m.QuerySnapshots(ctx, snapshotQueryFilter)
	if err != nil {
		log.Errorf("querySnapshots failed for snapshotQueryFilter: %v. Err=%+v", snapshotQueryFilter, err)
		return nil, false
	}
	if snapshotQueryResult == nil {
		log.Info("Observed empty SnapshotQueryResult")
		return nil, false
	}
	if len(snapshotQueryResult.Entries) == 0 {
		log.Infof("QuerySnapshots retrieved no results for the spec: %+v", snapshotQuerySpec)
		return nil, false
	}
	for _, snapshotInfo := range snapshotQueryResult.Entries {
		if snapshotInfo.Snapshot.Description == snapshotName {
			return &snapshotInfo.Snapshot, true
		}
	}
	return nil, false
}

// IsNotFoundFault returns true if a given faultType value is vim.fault.NotFound
func IsNotFoundFault(ctx context.Context, faultType string) bool {
	log := logger.GetLogger(ctx)
	log.Infof("Checking fault type: %q is vim.fault.NotFound", faultType)
	return faultType == "vim.fault.NotFound"

}

// IsNotSupportedFault returns true if a given fault is NotSupported fault
func IsNotSupportedFault(ctx context.Context, fault *types.LocalizedMethodFault) bool {
	log := logger.GetLogger(ctx)
	if cnsFault, ok := fault.Fault.(*cnstypes.CnsFault); ok {
		if cause := cnsFault.FaultCause; cause != nil {
			if innerfault, ok := cause.Fault.(*types.NotSupported); ok {
				log.Info("observed NotSupported fault")
				return true
			} else {
				log.Infof("observed fault: %T", innerfault)
				return false
			}
		} else {
			log.Errorf("observed fault with nil cause")
		}
	} else {
		log.Errorf("can not typecast fault to CnsFault")
	}
	return false
}

func IsNotSupportedFaultType(ctx context.Context, faultType string) bool {
	log := logger.GetLogger(ctx)
	log.Infof("Checking fault type: %q is vim25:NotSupported", faultType)
	return faultType == "vim25:NotSupported"
}

// IsCnsVolumeAlreadyExistsFault returns true if a given faultType value is vim.fault.CnsVolumeAlreadyExistsFault
func IsCnsVolumeAlreadyExistsFault(ctx context.Context, faultType string) bool {
	log := logger.GetLogger(ctx)
	log.Infof("Checking fault type: %q is vim.fault.CnsVolumeAlreadyExistsFault", faultType)
	return faultType == "vim.fault.CnsVolumeAlreadyExistsFault"
}
