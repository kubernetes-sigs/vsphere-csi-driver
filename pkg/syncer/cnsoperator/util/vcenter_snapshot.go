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

package util

import (
	"context"
	"fmt"

	"github.com/vmware/govmomi/property"
	"github.com/vmware/govmomi/vim25/mo"
	vimtypes "github.com/vmware/govmomi/vim25/types"

	cnsvsphere "sigs.k8s.io/vsphere-csi-driver/v3/pkg/common/cns-lib/vsphere"
	"sigs.k8s.io/vsphere-csi-driver/v3/pkg/csi/service/logger"
)

// GetRetainedDiskUUIDs queries the vCenter snapshot tree for the given VM and
// returns the set of all disk UUIDs (VirtualDisk.Backing.Uuid) that are still
// referenced by at least one snapshot. The result is empty when the VM has no
// snapshots.
//
// The implementation fetches the snapshot tree in a single API call, then
// batch-retrieves hardware device lists for all snapshot MoRefs in a second
// call, resulting in exactly two vCenter RPCs regardless of snapshot count.
func GetRetainedDiskUUIDs(ctx context.Context,
	vm *cnsvsphere.VirtualMachine,
) (map[string]struct{}, error) {
	log := logger.GetLogger(ctx)
	log.Infof("GetRetainedDiskUUIDs: querying snapshot tree for VM %v",
		vm.VirtualMachine.Reference())

	var vmMo mo.VirtualMachine
	if err := vm.VirtualMachine.Properties(ctx,
		vm.VirtualMachine.Reference(), []string{"snapshot"}, &vmMo); err != nil {
		return nil, fmt.Errorf("GetRetainedDiskUUIDs: failed to get VM properties for %v: %w",
			vm.VirtualMachine.Reference(), err)
	}

	if vmMo.Snapshot == nil || len(vmMo.Snapshot.RootSnapshotList) == 0 {
		log.Infof("GetRetainedDiskUUIDs: VM %v has no snapshots",
			vm.VirtualMachine.Reference())
		return map[string]struct{}{}, nil
	}

	var snapRefs []vimtypes.ManagedObjectReference
	collectSnapshotRefs(vmMo.Snapshot.RootSnapshotList, &snapRefs)
	log.Infof("GetRetainedDiskUUIDs: found %d snapshots in tree for VM %v",
		len(snapRefs), vm.VirtualMachine.Reference())

	pc := property.DefaultCollector(vm.VirtualMachine.Client())
	var snapMos []mo.VirtualMachineSnapshot
	if err := pc.Retrieve(ctx, snapRefs, []string{"config.hardware.device"}, &snapMos); err != nil {
		return nil, fmt.Errorf(
			"GetRetainedDiskUUIDs: failed to retrieve snapshot properties for VM %v: %w",
			vm.VirtualMachine.Reference(), err)
	}

	retained := make(map[string]struct{})
	for i := range snapMos {
		for _, device := range snapMos[i].Config.Hardware.Device {
			disk, ok := device.(*vimtypes.VirtualDisk)
			if !ok {
				continue
			}
			backing, ok := disk.Backing.(*vimtypes.VirtualDiskFlatVer2BackingInfo)
			if !ok {
				continue
			}
			if backing.Uuid != "" {
				retained[backing.Uuid] = struct{}{}
			}
		}
	}

	log.Infof("GetRetainedDiskUUIDs: VM %v has %d disk(s) retained by snapshots: %v",
		vm.VirtualMachine.Reference(), len(retained), retained)
	return retained, nil
}

// IsVolumeRetainedByVCenterSnapshot reports whether the disk identified by
// diskUUID is still referenced by at least one snapshot of the given VM.
// It delegates to GetRetainedDiskUUIDs for the actual vCenter query.
func IsVolumeRetainedByVCenterSnapshot(ctx context.Context,
	vm *cnsvsphere.VirtualMachine, diskUUID string,
) (bool, error) {
	retained, err := GetRetainedDiskUUIDs(ctx, vm)
	if err != nil {
		return false, err
	}
	_, found := retained[diskUUID]
	return found, nil
}

// collectSnapshotRefs recursively walks the snapshot tree rooted at nodes and
// appends each snapshot's ManagedObjectReference to refs.
func collectSnapshotRefs(nodes []vimtypes.VirtualMachineSnapshotTree,
	refs *[]vimtypes.ManagedObjectReference) {
	for i := range nodes {
		*refs = append(*refs, nodes[i].Snapshot)
		if len(nodes[i].ChildSnapshotList) > 0 {
			collectSnapshotRefs(nodes[i].ChildSnapshotList, refs)
		}
	}
}
