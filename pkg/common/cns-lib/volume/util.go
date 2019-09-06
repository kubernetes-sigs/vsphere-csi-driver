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
	vimtypes "gitlab.eng.vmware.com/hatchway/govmomi/vim25/types"
	"k8s.io/klog"
	cnsvsphere "sigs.k8s.io/vsphere-csi-driver/pkg/common/cns-lib/vsphere"
)

const (
	CNSVolumeResourceInUseFaultMessage = "The resource 'volume' is in use."
)

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
