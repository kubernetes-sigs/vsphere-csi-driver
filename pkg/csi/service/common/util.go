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

package common

import (
	"fmt"
	"strings"

	"github.com/akutz/gofsutil"
	"github.com/container-storage-interface/spec/lib/go/csi"
	"gitlab.eng.vmware.com/hatchway/govmomi/vim25/types"
	"golang.org/x/net/context"
	"k8s.io/klog"
	cnsvsphere "sigs.k8s.io/vsphere-csi-driver/pkg/common/cns-lib/vsphere"
)

// GetVCenter returns VirtualCenter object from specified Manager object.
// Before returning VirtualCenter object, vcenter connection is established if session doesn't exist.
func GetVCenter(ctx context.Context, manager *Manager) (*cnsvsphere.VirtualCenter, error) {
	var err error
	vcenter, err := manager.VcenterManager.GetVirtualCenter(manager.VcenterConfig.Host)
	if err != nil {
		klog.Errorf("Failed to get VirtualCenter instance for host: %q. err=%v", manager.VcenterConfig.Host, err)
		return nil, err
	}
	err = vcenter.Connect(ctx)
	if err != nil {
		klog.Errorf("Failed to connect to VirtualCenter host: %q. err=%v", manager.VcenterConfig.Host, err)
		return nil, err
	}
	return vcenter, nil
}

// GetUUIDFromProviderID Returns VM UUID from Node's providerID
func GetUUIDFromProviderID(providerID string) string {
	return strings.TrimPrefix(providerID, ProviderPrefix)
}

// FormatDiskUUID removes any spaces and hyphens in UUID
// Example UUID input is 42375390-71f9-43a3-a770-56803bcd7baa and output after format is 4237539071f943a3a77056803bcd7baa
func FormatDiskUUID(uuid string) string {
	uuidwithNoSpace := strings.Replace(uuid, " ", "", -1)
	uuidWithNoHypens := strings.Replace(uuidwithNoSpace, "-", "", -1)
	return strings.ToLower(uuidWithNoHypens)
}

// RoundUpSize calculates how many allocation units are needed to accommodate
// a volume of given size.
func RoundUpSize(volumeSizeBytes int64, allocationUnitBytes int64) int64 {
	roundedUp := volumeSizeBytes / allocationUnitBytes
	if volumeSizeBytes%allocationUnitBytes > 0 {
		roundedUp++
	}
	return roundedUp
}

// GetLabelsMapFromKeyValue creates a  map object from given parameter
func GetLabelsMapFromKeyValue(labels []types.KeyValue) map[string]string {
	labelsMap := make(map[string]string)
	for _, label := range labels {
		labelsMap[label.Key] = label.Value
	}
	return labelsMap
}

// IsFileVolumeRequest checks whether the request is to create a CNS file volume.
func IsFileVolumeRequest(v []*csi.VolumeCapability) bool {
	for _, capability := range v {
		if fstype := strings.ToLower(GetVolumeCapabilityFsType(capability)); fstype == NfsV4FsType || fstype == NfsFsType {
			return true
		}
	}
	return false
}

// GetVolumeCapabilityFsType retrieves fstype from VolumeCapability. Defaults to DefaultFsType when empty for mount volumes.
func GetVolumeCapabilityFsType(capability *csi.VolumeCapability) string {
	fsType := strings.ToLower(capability.GetMount().GetFsType())
	klog.V(4).Infof("FsType received from Volume Capability: %q", fsType)
	if fsType == "" {
		// Defaulting fstype for mount volumes only. Block volumes will still have fstype as empty.
		if _, ok := capability.GetAccessType().(*csi.VolumeCapability_Mount); ok {
			klog.V(2).Infof("No fstype received in Volume Capability for mount volume. Defaulting to: %s",
				DefaultFsType)
			fsType = DefaultFsType
		}
	}
	return fsType
}

// IsVolumeReadOnly checks the access mode in Volume Capability and decides if volume is readonly or not
func IsVolumeReadOnly(capability *csi.VolumeCapability) bool {
	accMode := capability.GetAccessMode().GetMode()
	ro := false
	if accMode == csi.VolumeCapability_AccessMode_SINGLE_NODE_READER_ONLY ||
		accMode == csi.VolumeCapability_AccessMode_MULTI_NODE_READER_ONLY {
		ro = true
	}
	return ro
}

// validateVolumeCapabilities validates the access mode in given volume capabilities in validAccessModes.
func validateVolumeCapabilities(volCaps []*csi.VolumeCapability, validAccessModes []csi.VolumeCapability_AccessMode) bool {
	// Validate if all capabilities of the volume
	// are supported.
	for _, volCap := range volCaps {
		found := false
		for _, validAccessMode := range validAccessModes {
			if volCap.AccessMode.GetMode() == validAccessMode.GetMode() {
				found = true
				break
			}
		}
		if !found {
			return false
		}
	}
	return true
}

// IsValidVolumeCapabilities helps validate the given volume capabilities based on volume type.
func IsValidVolumeCapabilities(volCaps []*csi.VolumeCapability) bool {
	if IsFileVolumeRequest(volCaps) {
		return validateVolumeCapabilities(volCaps, FileVolumeCaps)
	}
	return validateVolumeCapabilities(volCaps, BlockVolumeCaps)
}

// IsFileVolumeMount loops through the list of mount points and
// checks if the target path mount point is a file volume type or not
// Returns an error if the target path is not found in the mount points
func IsFileVolumeMount(target string, mnts []gofsutil.Info) (bool, error) {
	for _, m := range mnts {
		if m.Path == target {
			if m.Type == NfsFsType || m.Type == NfsV4FsType {
				klog.V(4).Info("IsFileVolumeMount: Found file volume")
				return true, nil
			}
			klog.V(4).Info("IsFileVolumeMount: Found block volume")
			return false, nil
		}
	}
	// Target path mount point not found in list of mounts
	return false, fmt.Errorf("could not find target path %q in list of mounts", target)
}

// IsTargetInMounts checks if the given target path is present in list of mount points
func IsTargetInMounts(target string, mnts []gofsutil.Info) bool {
	for _, m := range mnts {
		if m.Path == target {
			klog.V(4).Infof("Found target %q in list of mounts", target)
			return true
		}
	}
	klog.V(4).Infof("Target %q not found in list of mounts", target)
	return false
}
