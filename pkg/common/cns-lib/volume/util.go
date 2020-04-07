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

	"sigs.k8s.io/vsphere-csi-driver/pkg/csi/service/logger"

	vimtypes "github.com/vmware/govmomi/vim25/types"
	cnsvsphere "sigs.k8s.io/vsphere-csi-driver/pkg/common/cns-lib/vsphere"
)

func validateManager(ctx context.Context, m *defaultManager) error {
	log := logger.GetLogger(ctx)
	if m.virtualCenter == nil {
		log.Error(
			"Virtual Center connection not established")
		return errors.New("Virtual Center connection not established")
	}
	return nil
}

// IsDiskAttached checks if the volume is attached to the VM.
// If the volume is attached to the VM, return disk uuid of the volume, else return empty string
func IsDiskAttached(ctx context.Context, vm *cnsvsphere.VirtualMachine, volumeID string) (string, error) {
	log := logger.GetLogger(ctx)
	// Verify if the volume id is on the VM backing virtual disk devices
	vmDevices, err := vm.Device(ctx)
	if err != nil {
		log.Errorf("failed to get devices from vm: %s", vm.InventoryPath)
		return "", err
	}
	for _, device := range vmDevices {
		if vmDevices.TypeName(device) == "VirtualDisk" {
			if virtualDisk, ok := device.(*vimtypes.VirtualDisk); ok {
				if virtualDisk.VDiskId != nil && virtualDisk.VDiskId.Id == volumeID {
					virtualDevice := device.GetVirtualDevice()
					if backing, ok := virtualDevice.Backing.(*vimtypes.VirtualDiskFlatVer2BackingInfo); ok {
						log.Debugf("Found diskUUID %s for volume %s on vm %+v", backing.Uuid, volumeID, vm)
						return backing.Uuid, nil
					}
				}
			}
		}
	}
	log.Debugf("Volume %s is not attached to VM: %+v", volumeID, vm)
	return "", nil
}

// IsDiskAttachedToVMs checks if the volume is attached to any of the input VMs.
// If the volume is attached to the VM, return disk uuid of the volume, else return empty string
func IsDiskAttachedToVMs(ctx context.Context, volumeID string, vms []*cnsvsphere.VirtualMachine) (string, error) {
	for _, vm := range vms {
		diskUUID, err := IsDiskAttached(ctx, vm, volumeID)
		if diskUUID != "" || err != nil {
			return diskUUID, err
		}
	}
	return "", nil
}
