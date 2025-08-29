/*
Copyright 2025 The Kubernetes Authors.

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

package constants

const (
	Datacenter              = "DATACENTER"
	DestinationDatastoreURL = "DESTINATION_VSPHERE_DATASTORE_URL"
	DisklibUnlinkErr        = "DiskLib_Unlink"
	DiskSize1GB             = "1Gi"
	DiskSize                = "2Gi"
	DiskSizeLarge           = "100Gi"
	DiskSizeInMb            = int64(2048)
	DiskSizeInMinMb         = int64(200)
	E2eTestPassword         = "E2E-test-password!23"
	E2evSphereCSIDriverName = "csi.vsphere.vmware.com"
	Ext3FSType              = "ext3"
	Ext4FSType              = "ext4"
	XfsFSType               = "xfs"
	EvacMModeType           = "evacuateAllData"
	FcdName                 = "BasicStaticFCD"
	FileSizeInMb            = int64(2048)
	FilePathPod             = "/mnt/volume1/Pod.html"
	FilePathPod1            = "/mnt/volume1/Pod1.html"
	FilePathPod2            = "/mnt/volume1/Pod2.html"
	FilePathFsType          = "/mnt/volume1/fstype"
	FullSyncFss             = "trigger-csi-fullsync"
	ThinAllocType           = "Conserve space when possible"
	EztAllocType            = "Fully initialized"
	LztAllocType            = "Reserve space"
	Nfs4FSType              = "nfs4"
	Nfs4Keyword             = "NFSv4.1"
	ObjOrItemNotFoundErr    = "The object or item referred to could not be found"
	ProviderPrefix          = "vsphere://"
)
