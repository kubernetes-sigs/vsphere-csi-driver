// Copyright 2018 VMware, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//    http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package volume

import (
	"context"
	"errors"
	cnstypes "gitlab.eng.vmware.com/hatchway/govmomi/cns/types"
	"gitlab.eng.vmware.com/hatchway/govmomi/object"
	vimtypes "gitlab.eng.vmware.com/hatchway/govmomi/vim25/types"
	"k8s.io/klog"
	cnsvsphere "sigs.k8s.io/vsphere-csi-driver/pkg/common/cns-lib/vsphere"
)

// version and namespace constants for task client
const (
	// ClientVersion "6.9.0" is mapped to 'vsan.version.version11'
	// This need to be changed if later VMODL version is bumped
	taskClientVersion   = "6.9.0"
	taskClientNamespace = "urn:vsan"

	CNSVolumeResourceInUseFaultMessage = "The resource 'volume' is in use."
	taskInfoEmptyErrMsg                = "TaskInfo is empty"
	getVolOpResErrMsg                  = "Cannot get VolumeOperationResult"
)

// GetTaskResult gets the task result given a task info
func GetTaskResult(ctx context.Context, taskInfo *vimtypes.TaskInfo) (cnstypes.BaseCnsVolumeOperationResult, error) {
	if taskInfo == nil {
		klog.Error(taskInfoEmptyErrMsg)
		return nil, errors.New(taskInfoEmptyErrMsg)
	}
	volumeOperationBatchResult := taskInfo.Result.(cnstypes.CnsVolumeOperationBatchResult)
	if &volumeOperationBatchResult == nil ||
		volumeOperationBatchResult.VolumeResults == nil ||
		len(volumeOperationBatchResult.VolumeResults) == 0 {
		klog.Error(getVolOpResErrMsg)
		return nil, errors.New(getVolOpResErrMsg)
	}
	return volumeOperationBatchResult.VolumeResults[0], nil
}

// GetTaskInfo gets the task info given a task
func GetTaskInfo(ctx context.Context, task *object.Task) (*vimtypes.TaskInfo, error) {
	task.Client().Version = taskClientVersion
	task.Client().Namespace = taskClientNamespace
	taskInfo, err := task.WaitForResult(ctx, nil)
	if err != nil {
		klog.Errorf("Failed to complete task. Task failed on wait with err: %v", err)
		return nil, err
	}
	return taskInfo, nil
}

func validateManager(m *defaultManager) error {
	if m.virtualCenter == nil {
		klog.Error(
			"Virtual Center connection not established")
		return errors.New("Virtual Center connection not established")
	}
	return nil
}

// IsDiskAttached checks if the volume is attached to the VM.
// If the volume is attached to the VM, return disk uuid of the volume, else return empty string
func IsDiskAttached(ctx context.Context, vm *cnsvsphere.VirtualMachine, volumeID string) (string, error) {
	// Verify if the volume id is on the VM backing virtual disk devices
	vmDevices, err := vm.Device(ctx)
	if err != nil {
		klog.Errorf("Failed to get devices from vm: %s", vm.InventoryPath)
		return "", err
	}
	for _, device := range vmDevices {
		if vmDevices.TypeName(device) == "VirtualDisk" {
			if virtualDisk, ok := device.(*vimtypes.VirtualDisk); ok {
				if virtualDisk.VDiskId != nil && virtualDisk.VDiskId.Id == volumeID {
					virtualDevice := device.GetVirtualDevice()
					if backing, ok := virtualDevice.Backing.(*vimtypes.VirtualDiskFlatVer2BackingInfo); ok {
						klog.V(3).Infof("Found diskUUID %s for volume %s on vm %s", backing.Uuid, volumeID, vm.InventoryPath)
						return backing.Uuid, nil
					}
				}
			}
		}
	}
	klog.V(3).Infof("Volume %s is not attached to VM: %s", volumeID, vm.InventoryPath)
	return "", nil
}
