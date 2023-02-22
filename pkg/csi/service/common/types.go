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
	"errors"

	"github.com/container-storage-interface/spec/lib/go/csi"
	cnsvolume "sigs.k8s.io/vsphere-csi-driver/v3/pkg/common/cns-lib/volume"
	cnsvsphere "sigs.k8s.io/vsphere-csi-driver/v3/pkg/common/cns-lib/vsphere"
	"sigs.k8s.io/vsphere-csi-driver/v3/pkg/common/config"
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

	// ErrNotFound represents not found error
	ErrNotFound = errors.New("not found")
)

// Manager type comprises VirtualCenterConfig, CnsConfig, VolumeManager and VirtualCenterManager
type Manager struct {
	VcenterConfig  *cnsvsphere.VirtualCenterConfig
	CnsConfig      *config.Config
	VolumeManager  cnsvolume.Manager
	VcenterManager cnsvsphere.VirtualCenterManager
}

// Managers type comprises VirtualCenterConfigs, CnsConfig, VolumeManagers and VirtualCenterManager
// If k8s cluster is deployed on single vCenter server VcenterConfigs and VolumeManagers will hold single entry
// if k8s cluster is deployed on multi vCenter server, we will have VirtualCenterConfig and VolumeManagers for each
// participating vCenter server.
type Managers struct {
	// map of VC Host to *VirtualCenterConfig
	VcenterConfigs map[string]*cnsvsphere.VirtualCenterConfig
	CnsConfig      *config.Config
	// map of VC Host to Volume Manager
	VolumeManagers map[string]cnsvolume.Manager
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
	AffineToHost            string
	VolumeType              string
	VsanDirectDatastoreURL  string // Datastore URL from vSan direct storage pool
	ContentSourceSnapshotID string // SnapshotID from VolumeContentSource in CreateVolumeRequest
}

// StorageClassParams represents the storage class parameterss
type StorageClassParams struct {
	DatastoreURL      string
	StoragePolicyName string
	CSIMigration      string
	Datastore         string
}
