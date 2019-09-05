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

const (
	// MbInBytes is the number of bytes in one mebibyte.
	MbInBytes = int64(1024 * 1024)

	// GbInBytes is the number of bytes in one gibibyte.
	GbInBytes = int64(1024 * 1024 * 1024)

	// DefaultGbDiskSize is the default disk size in gibibytes.
	DefaultGbDiskSize = int64(10)

	// DiskTypeString is the value for the PersistentVolume's attribute "type"
	DiskTypeString = "vSphere CNS Block Volume"

	// AttributeDiskType is a PersistentVolume's attribute.
	AttributeDiskType = "type"

	// AttributeDatastoreURL represents URL of the datastore in the StorageClass
	// For Example: DatastoreURL: "ds:///vmfs/volumes/5c9bb20e-009c1e46-4b85-0200483b2a97/"
	AttributeDatastoreURL = "datastoreurl"

	// AttributeStoragePolicyName represents name of the Storage Policy in the Storage Class
	// For Example: StoragePolicy: "vSAN Default Storage Policy"
	AttributeStoragePolicyName = "storagepolicyname"

	// AttributeStoragePolicyID represents Storage Policy Id in the Storage Classs
	// For Example: StoragePolicyId: "251bce41-cb24-41df-b46b-7c75aed3c4ee"
	AttributeStoragePolicyID = "storagepolicyid"

	// AttributeFsType represents filesystem type in the Storage Classs
	// For Example: FsType: "ext4"
	AttributeFsType = "fstype"

	// DefaultFsType represents the default filesystem type which will be used to format the volume
	// during mount if user does not specify the filesystem type in the Storage Class
	DefaultFsType = "ext4"

	//ProviderPrefix is the prefix used for the ProviderID set on the node
	// Example: vsphere://4201794a-f26b-8914-d95a-edeb7ecc4a8f
	ProviderPrefix = "vsphere://"

	// AttributeFirstClassDiskUUID is the SCSI Disk Identifier
	AttributeFirstClassDiskUUID = "diskUUID"

	// BlockVolumeType is the VolumeType for CNS Volume
	BlockVolumeType = "BLOCK"

	// MinSupportedVCenterMajor is the minimum, major version of vCenter
	// on which CNS is supported.
	MinSupportedVCenterMajor int = 6

	// MinSupportedVCenterMinor is the minimum, minor version of vCenter
	// on which CNS is supported.
	MinSupportedVCenterMinor int = 7

	// MinSupportedVCenterPatch is the patch version supported with MinSupportedVCenterMajor and MinSupportedVCenterMinor
	MinSupportedVCenterPatch int = 3
)
