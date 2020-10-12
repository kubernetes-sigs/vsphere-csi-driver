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

	"github.com/vmware/govmomi/cns"
	cnstypes "github.com/vmware/govmomi/cns/types"

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
						log.Debugf("Found diskUUID %s for volume %s on vm %s", backing.Uuid, volumeID, vm.InventoryPath)
						return backing.Uuid, nil
					}
				}
			}
		}
	}
	log.Debugf("Volume %s is not attached to VM: %+v", volumeID, vm)
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
