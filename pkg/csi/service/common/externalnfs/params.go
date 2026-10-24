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

// Package externalnfs implements minimal, vendor-neutral RWX file-volume provisioning on
// externally managed NFS exports (i.e. NOT vSAN File Service), using a subdir-per-PVC scheme
// modeled on the open-source kubernetes-csi/csi-driver-nfs "subdir" provisioner. It bypasses
// CNS/vCenter entirely: created volumes are plain directories on a pre-existing NFS export.
package externalnfs

// StorageClass parameter keys recognized by this backend.
const (
	// ParamServer is the NFS server address or hostname. Required.
	ParamServer = "nfs.csi.vsphere.vmware.com/server"
	// ParamShare is the path of the pre-existing NFS export on ParamServer. Required.
	ParamShare = "nfs.csi.vsphere.vmware.com/share"
	// ParamSubDir is an optional subdirectory name/template to create under ParamShare for the
	// volume. If unset, the PV/volume name is used.
	ParamSubDir = "nfs.csi.vsphere.vmware.com/subDir"
	// ParamMountOptions is an optional comma-separated list of extra NFS mount options.
	ParamMountOptions = "nfs.csi.vsphere.vmware.com/mountOptions"
)

// IsExternalNFSRequest reports whether the given StorageClass parameters select the
// external-NFS backend, i.e. whether both server and share are set.
func IsExternalNFSRequest(params map[string]string) bool {
	return params[ParamServer] != "" && params[ParamShare] != ""
}
