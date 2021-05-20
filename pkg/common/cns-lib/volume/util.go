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
	"strings"

	"github.com/vmware/govmomi/cns"
	cnstypes "github.com/vmware/govmomi/cns/types"

	uuidlib "github.com/google/uuid"
	"github.com/vmware/govmomi/vim25/types"
	cnsvsphere "sigs.k8s.io/vsphere-csi-driver/pkg/common/cns-lib/vsphere"
	"sigs.k8s.io/vsphere-csi-driver/pkg/csi/service/logger"
)

func validateManager(ctx context.Context, m *defaultManager) error {
	log := logger.GetLogger(ctx)
	if m.virtualCenter == nil {
		log.Error(
			"Virtual Center connection not established")
		return errors.New("virtual Center connection not established")
	}
	return nil
}

// IsDiskAttached checks if the volume is attached to the VM.
// If the volume is attached to the VM, return disk uuid of the volume, else return empty string
func IsDiskAttached(ctx context.Context, vm *cnsvsphere.VirtualMachine, volumeID string, checkNVMeController bool) (string, error) {
	log := logger.GetLogger(ctx)
	// Verify if the volume id is on the VM backing virtual disk devices
	vmDevices, err := vm.Device(ctx)
	if err != nil {
		log.Errorf("failed to get devices from vm: %s", vm.InventoryPath)
		return "", err
	}
	// Build a map of NVME Controller key : NVME controller name
	// This is needed to check if disk in contention is attached to a NVME controller
	// The virtual disk devices do not contain the controller type information, but only
	// contain the controller key information.
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
	// is attached to NVME controller if checkNVMeController is enabled and return
	// NVME UUID by converting the backing UUID, else return the backing UUID (SCSI format UUID)
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
									log.Debugf("Successfully converted diskUUID %s to NvmeV13UUID %s for volume %s on vm %+v", backing.Uuid, uuid, volumeID, vm)
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

// getNvmeUUID returns the NVME formatted UUID
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
// If the volume is attached to the VM, return disk uuid of the volume, else return empty string
func IsDiskAttachedToVMs(ctx context.Context, volumeID string, vms []*cnsvsphere.VirtualMachine, checkNVMeController bool) (string, error) {
	for _, vm := range vms {
		diskUUID, err := IsDiskAttached(ctx, vm, volumeID, checkNVMeController)
		if diskUUID != "" || err != nil {
			return diskUUID, err
		}
	}
	return "", nil
}

// updateQueryResult helps update CnsQueryResult to populate volume.Metadata.EntityMetadata.ClusterID
// with value from volume.Metadata.ContainerCluster.ClusterId
// This is required to make driver code compatible to vSphere 67 release
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
