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
	"fmt"
	"sync"

	cnstypes "github.com/vmware/govmomi/cns/types"
	"github.com/vmware/govmomi/object"
	"github.com/vmware/govmomi/vim25/types"
	"sigs.k8s.io/vsphere-csi-driver/v3/pkg/apis/migration"
	cnsvolume "sigs.k8s.io/vsphere-csi-driver/v3/pkg/common/cns-lib/volume"
	"sigs.k8s.io/vsphere-csi-driver/v3/pkg/common/cns-lib/vsphere"
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

func (c *FakeK8SOrchestrator) HandleLateEnablementOfCapability(
	ctx context.Context, clusterFlavor cnstypes.CnsClusterFlavor, capability, gcPort, gcEndpoint string) {
	//TODO implement me
	panic("implement me")
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

type MockVolumeManager struct {
	createVolumeFunc func(ctx context.Context, spec *cnstypes.CnsVolumeCreateSpec,
		extraParams interface{}) (*cnsvolume.CnsVolumeInfo, string, error)
}

func (m *MockVolumeManager) UnregisterVolume(ctx context.Context, volumeID string,
	unregisterDisk bool) *cnsvolume.Error {
	//TODO implement me
	return nil
}

func (m *MockVolumeManager) CreateVolume(ctx context.Context, spec *cnstypes.CnsVolumeCreateSpec,
	extraParams interface{}) (*cnsvolume.CnsVolumeInfo, string, error) {
	if m.createVolumeFunc != nil {
		return m.createVolumeFunc(ctx, spec, extraParams)
	}
	return nil, "", nil
}

// Implement all other required methods from cnsvolume.Manager interface
func (m *MockVolumeManager) AttachVolume(ctx context.Context, vm *vsphere.VirtualMachine, volumeID string,
	checkNVMeController bool) (string, string, error) {
	return "", "", nil
}
func (m *MockVolumeManager) DetachVolume(ctx context.Context, vm *vsphere.VirtualMachine,
	volumeID string) (string, error) {
	if volumeID == "fail-detach" {
		return "", fmt.Errorf("failed to detach volume")
	}
	return "", nil
}
func (m *MockVolumeManager) DeleteVolume(ctx context.Context, volumeID string, deleteDisk bool) (string, error) {
	return "", nil
}
func (m *MockVolumeManager) UpdateVolumeMetadata(ctx context.Context,
	spec *cnstypes.CnsVolumeMetadataUpdateSpec) error {
	return nil
}
func (m *MockVolumeManager) UpdateVolumeCrypto(ctx context.Context, spec *cnstypes.CnsVolumeCryptoUpdateSpec) error {
	return nil
}
func (m *MockVolumeManager) QueryVolumeInfo(ctx context.Context,
	volumeIDList []cnstypes.CnsVolumeId) (*cnstypes.CnsQueryVolumeInfoResult, error) {
	return nil, nil
}
func (m *MockVolumeManager) QueryAllVolume(ctx context.Context, queryFilter cnstypes.CnsQueryFilter,
	querySelection cnstypes.CnsQuerySelection) (*cnstypes.CnsQueryResult, error) {
	return nil, nil
}
func (m *MockVolumeManager) QueryVolumeAsync(ctx context.Context, queryFilter cnstypes.CnsQueryFilter,
	querySelection *cnstypes.CnsQuerySelection) (*cnstypes.CnsQueryResult, error) {
	return nil, nil
}
func (m *MockVolumeManager) QueryVolume(ctx context.Context,
	queryFilter cnstypes.CnsQueryFilter) (*cnstypes.CnsQueryResult, error) {
	return nil, nil
}
func (m *MockVolumeManager) RelocateVolume(ctx context.Context,
	relocateSpecList ...cnstypes.BaseCnsVolumeRelocateSpec) (*object.Task, error) {
	return nil, nil
}
func (m *MockVolumeManager) ExpandVolume(ctx context.Context, volumeID string, size int64,
	extraParams interface{}) (string, error) {
	return "", nil
}
func (m *MockVolumeManager) ResetManager(ctx context.Context, vcenter *vsphere.VirtualCenter) error {
	return nil
}
func (m *MockVolumeManager) ConfigureVolumeACLs(ctx context.Context, spec cnstypes.CnsVolumeACLConfigureSpec) error {
	return nil
}
func (m *MockVolumeManager) RegisterDisk(ctx context.Context, path string, name string) (string, error) {
	return "", nil
}
func (m *MockVolumeManager) RetrieveVStorageObject(ctx context.Context,
	volumeID string) (*types.VStorageObject, error) {
	return nil, nil
}
func (m *MockVolumeManager) ProtectVolumeFromVMDeletion(ctx context.Context, volumeID string) error {
	return nil
}
func (m *MockVolumeManager) CreateSnapshot(ctx context.Context, volumeID string, desc string,
	extraParams interface{}) (*cnsvolume.CnsSnapshotInfo, error) {
	return nil, nil
}
func (m *MockVolumeManager) DeleteSnapshot(ctx context.Context, volumeID string, snapshotID string,
	extraParams interface{}) (*cnsvolume.CnsSnapshotInfo, error) {
	return nil, nil
}
func (m *MockVolumeManager) QuerySnapshots(ctx context.Context,
	snapshotQueryFilter cnstypes.CnsSnapshotQueryFilter) (*cnstypes.CnsSnapshotQueryResult, error) {
	return nil, nil
}
func (m *MockVolumeManager) IsListViewReady() bool {
	return true
}
func (m *MockVolumeManager) SetListViewNotReady(ctx context.Context) {}
func (m *MockVolumeManager) GetOperationStore() cnsvolumeoperationrequest.VolumeOperationRequest {
	return nil
}
func (m *MockVolumeManager) MonitorCreateVolumeTask(ctx context.Context,
	volumeOperationDetails **cnsvolumeoperationrequest.VolumeOperationRequestDetails, task *object.Task,
	volNameFromInputSpec, clusterID string) (*cnsvolume.CnsVolumeInfo, string, error) {
	return nil, "", nil
}

func (m *MockVolumeManager) BatchAttachVolumes(ctx context.Context,
	vm *vsphere.VirtualMachine,
	batchAttachRequest []cnsvolume.BatchAttachRequest) ([]cnsvolume.BatchAttachResult, string, error) {
	for _, request := range batchAttachRequest {
		if request.VolumeID == "fail-attach" {
			return []cnsvolume.BatchAttachResult{}, "", fmt.Errorf("failed to attach volume")
		}
	}
	return []cnsvolume.BatchAttachResult{}, "", nil
}

func (m *MockVolumeManager) SyncVolume(ctx context.Context,
	syncVolumeSpecs []cnstypes.CnsSyncVolumeSpec) (string, error) {
	return "", nil
}
