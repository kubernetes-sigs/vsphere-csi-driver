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
	"github.com/container-storage-interface/spec/lib/go/csi"
	cnsvolume "sigs.k8s.io/vsphere-csi-driver/pkg/common/cns-lib/volume"
	cnsvsphere "sigs.k8s.io/vsphere-csi-driver/pkg/common/cns-lib/vsphere"
	"sigs.k8s.io/vsphere-csi-driver/pkg/common/config"
)

var (
	// BlockVolumeCaps represents how the block volume could be accessed.
	// CNS block volumes support only SINGLE_NODE_WRITER where the volume is
	// attached to a single node at any given time.
	BlockVolumeCaps = []csi.VolumeCapability_AccessMode{
		{
			Mode: csi.VolumeCapability_AccessMode_SINGLE_NODE_WRITER,
		},
	}

	// FileVolumeCaps represents how the file volume could be accessed.
	// CNS file volumes supports MULTI_NODE_READER_ONLY, MULTI_NODE_SINGLE_WRITER
	// and MULTI_NODE_MULTI_WRITER
	FileVolumeCaps = []csi.VolumeCapability_AccessMode{
		{
			Mode: csi.VolumeCapability_AccessMode_MULTI_NODE_READER_ONLY,
		},
		{
			Mode: csi.VolumeCapability_AccessMode_MULTI_NODE_SINGLE_WRITER,
		},
		{
			Mode: csi.VolumeCapability_AccessMode_MULTI_NODE_MULTI_WRITER,
		},
	}
)

// Manager type comprises VirtualCenterConfig, CnsConfig, VolumeManager and VirtualCenterManager
type Manager struct {
	VcenterConfig  *cnsvsphere.VirtualCenterConfig
	CnsConfig      *config.Config
	VolumeManager  cnsvolume.Manager
	VcenterManager cnsvsphere.VirtualCenterManager
}

// CreateVolumeSpec is the Volume Spec used by CSI driver
type CreateVolumeSpec struct {
	Name     string
	ScParams *StorageClassParams
	// TODO: Move this StorageClassParams
	StoragePolicyID string
	CapacityMB      int64
	// TODO: Move this StorageClassParams
	AffineToHost string
	VolumeType   string
}

// StorageClassParams represents the storage class parameterss
type StorageClassParams struct {
	DatastoreURL      string
	StoragePolicyName string
	CSIMigration      string
	Datastore         string
}
