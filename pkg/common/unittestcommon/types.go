/*
Copyright 2020 The Kubernetes Authors.

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

package unittestcommon

import (
	"context"
	"sync"

	"sigs.k8s.io/vsphere-csi-driver/v3/pkg/apis/migration"
	cnsvolume "sigs.k8s.io/vsphere-csi-driver/v3/pkg/common/cns-lib/volume"
	cnsconfig "sigs.k8s.io/vsphere-csi-driver/v3/pkg/common/config"
	"sigs.k8s.io/vsphere-csi-driver/v3/pkg/internalapis/cnsvolumeoperationrequest"
)

const (
	VCSimDefaultDatacenters     int = 1
	VCSimDefaultClusters        int = 1
	VCSimDefaultHostsPerCluster int = 3
	VCSimDefaultStandalonHosts  int = 1
	VCSimDefaultDatastores      int = 1
	VCSimDefaultVMsPerCluster   int = 2
)

// FakeK8SOrchestrator is used to mock common K8S Orchestrator instance to store FSS values
type FakeK8SOrchestrator struct {
	// RWMutex to synchronize access to 'featureStates' field from multiple callers
	featureStatesLock *sync.RWMutex
	featureStates     map[string]string
}

// volumeMigration holds mocked migrated volume information
type mockVolumeMigration struct {
	// volumePath to volumeId map
	volumePathToVolumeID sync.Map
	// volumeManager helps perform Volume Operations
	volumeManager *cnsvolume.Manager
	// cnsConfig helps retrieve vSphere CSI configuration for RegisterVolume Operation
	cnsConfig *cnsconfig.Config
}

// MockVolumeMigrationService is a mocked VolumeMigrationService needed for CSI migration feature
type MockVolumeMigrationService interface {
	// GetVolumeID returns VolumeID for given migration volumeSpec
	// Returns an error if not able to retrieve VolumeID.
	GetVolumeID(ctx context.Context, volumeSpec *migration.VolumeSpec) (string, error)

	// GetVolumePath returns VolumePath for given VolumeID
	// Returns an error if not able to retrieve VolumePath.
	GetVolumePath(ctx context.Context, volumeID string) (string, error)

	// DeleteVolumeInfo helps delete mapping of volumePath to VolumeID for specified volumeID
	DeleteVolumeInfo(ctx context.Context, volumeID string) error
}

// fakeVolumeOperationRequestInterface implements the VolumeOperationRequest
// interface by storing the operation details in an in-memory map.
type fakeVolumeOperationRequestInterface struct {
	volumeOperationRequestMap map[string]*cnsvolumeoperationrequest.VolumeOperationRequestDetails
}

// mockControllerVolumeTopology is a mock of the k8sorchestrator controllerVolumeTopology type.
type mockControllerVolumeTopology struct {
}

// mockNodeVolumeTopology is a mock of the k8sorchestrator nodeVolumeTopology type.
type mockNodeVolumeTopology struct {
}

type VcsimParams struct {
	Datacenters     int
	Clusters        int
	HostsPerCluster int
	StandaloneHosts int
	VMsPerCluster   int
	// Note that specified number of datastores are created for each datacenter and datastore is accessible from
	// all hosts belonging to a datacenter. Internally each datastore will have temporary local file storage and
	// it will be mounted on every HostSystem of the datacenter.
	Datastores int
	// Version is the dot-separated VC version like 7.0.3
	Version string
	// ApiVersion is the dot-separated API version like 7.0
	ApiVersion string
}
