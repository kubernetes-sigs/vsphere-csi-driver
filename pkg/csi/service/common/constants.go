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
	// TODO: will make the DefaultGbDiskSize configurable in the future
	DefaultGbDiskSize = int64(10)

	// DiskTypeBlockVolume is the value for the PersistentVolume's attribute "type"
	DiskTypeBlockVolume = "vSphere CNS Block Volume"

	// DiskTypeFileVolume is the value for the PersistentVolume's attribute "type"
	DiskTypeFileVolume = "vSphere CNS File Volume"

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

	// AttributeSupervisorStorageClas represents name of the Storage Class
	// For example: StorageClassName: "silver"
	AttributeSupervisorStorageClass = "svstorageclass"

	// AttributeFsType represents filesystem type in the Storage Classs
	// For Example: FsType: "ext4"
	AttributeFsType = "fstype"

	// AttributeAffineToHost represents the ESX host moid to which this PV should be affinitized
	// For Example: AffineToHost: "host-25"
	AttributeAffineToHost = "affinetohost"

	// DefaultFsType represents the default filesystem type which will be used to format the volume
	// during mount if user does not specify the filesystem type in the Storage Class
	DefaultFsType = "ext4"

	// NfsV4FsType represents nfs4 mount type
	NfsV4FsType = "nfs4"

	// NfsFsType represents nfs mount type
	NfsFsType = "nfs"

	//ProviderPrefix is the prefix used for the ProviderID set on the node
	// Example: vsphere://4201794a-f26b-8914-d95a-edeb7ecc4a8f
	ProviderPrefix = "vsphere://"

	// AttributeFirstClassDiskUUID is the SCSI Disk Identifier
	AttributeFirstClassDiskUUID = "diskUUID"

	// BlockVolumeType is the VolumeType for CNS Volume
	BlockVolumeType = "BLOCK"

	// FileVolumeType is the VolumeType for CNS File Share Volume
	FileVolumeType = "FILE"

	// Key for NFSv4 access point
	Nfsv4AccessPointKey = "NFSv4.1"

	// NFSv4 access point of file volume
	Nfsv4AccessPoint = "Nfsv4AccessPoint"

	// MinSupportedVCenterMajor is the minimum, major version of vCenter
	// on which CNS is supported.
	MinSupportedVCenterMajor int = 6

	// MinSupportedVCenterMinor is the minimum, minor version of vCenter
	// on which CNS is supported.
	MinSupportedVCenterMinor int = 7

	// MinSupportedVCenterMajor is the minimum, major version of vCenter
	// on which CNS is supported.
	MinSupportedVCenterPatch int = 3

	// VsanAffinityKey is the profile param key to indicate which node the FCD should be affinitized to.
	VsanAffinityKey string = "VSAN/affinity/affinity"

	// VsanAffinityMandatory is the profile param key to turn on affinity of the volume to a specific ESX host.
	VsanAffinityMandatory string = "VSAN/affinityMandatory/affinityMandatory"

	// VsanMigrateForDecom is the profile param key to set the migrate mode for the volume.
	VsanMigrateForDecom string = "VSAN/migrateForDecom/migrateForDecom"

	// VsanDatastoreType is the string to identify datastore type as vsan.
	VsanDatastoreType string = "vsan"

	// Whether the root access should be allowed on a file volume
	AllowRoot string = "allowroot"

	// Permission to be set in the file volume
	Permission string = "permission"

	// Client IP address, IP range or IP subnet
	IPs string = "ips"
)
