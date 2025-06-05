package common

import (
	"context"
	"testing"

	"github.com/agiledragon/gomonkey/v2"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/vmware/govmomi"
	cnstypes "github.com/vmware/govmomi/cns/types"
	"github.com/vmware/govmomi/object"
	"github.com/vmware/govmomi/simulator"
	"github.com/vmware/govmomi/vim25/types"
	cnsvolume "sigs.k8s.io/vsphere-csi-driver/v3/pkg/common/cns-lib/volume"
	"sigs.k8s.io/vsphere-csi-driver/v3/pkg/common/cns-lib/vsphere"
	"sigs.k8s.io/vsphere-csi-driver/v3/pkg/common/config"
	"sigs.k8s.io/vsphere-csi-driver/v3/pkg/common/utils"
	"sigs.k8s.io/vsphere-csi-driver/v3/pkg/internalapis/cnsvolumeoperationrequest"
)

// mockVolumeManager is a mock implementation of cnsvolume.Manager for testing.
type mockVolumeManager struct {
	createVolumeFunc func(ctx context.Context, spec *cnstypes.CnsVolumeCreateSpec,
		extraParams interface{}) (*cnsvolume.CnsVolumeInfo, string, error)
}

func (m *mockVolumeManager) UnregisterVolume(ctx context.Context, volumeID string, unregisterDisk bool) error {
	//TODO implement me
	return nil
}

func (m *mockVolumeManager) CreateVolume(ctx context.Context, spec *cnstypes.CnsVolumeCreateSpec,
	extraParams interface{}) (*cnsvolume.CnsVolumeInfo, string, error) {
	if m.createVolumeFunc != nil {
		return m.createVolumeFunc(ctx, spec, extraParams)
	}
	return nil, "", nil
}

// Implement all other required methods from cnsvolume.Manager interface
func (m *mockVolumeManager) AttachVolume(ctx context.Context, vm *vsphere.VirtualMachine, volumeID string,
	checkNVMeController bool) (string, string, error) {
	return "", "", nil
}
func (m *mockVolumeManager) DetachVolume(ctx context.Context, vm *vsphere.VirtualMachine,
	volumeID string) (string, error) {
	return "", nil
}
func (m *mockVolumeManager) DeleteVolume(ctx context.Context, volumeID string, deleteDisk bool) (string, error) {
	return "", nil
}
func (m *mockVolumeManager) UpdateVolumeMetadata(ctx context.Context,
	spec *cnstypes.CnsVolumeMetadataUpdateSpec) error {
	return nil
}
func (m *mockVolumeManager) UpdateVolumeCrypto(ctx context.Context, spec *cnstypes.CnsVolumeCryptoUpdateSpec) error {
	return nil
}
func (m *mockVolumeManager) QueryVolumeInfo(ctx context.Context,
	volumeIDList []cnstypes.CnsVolumeId) (*cnstypes.CnsQueryVolumeInfoResult, error) {
	return nil, nil
}
func (m *mockVolumeManager) QueryAllVolume(ctx context.Context, queryFilter cnstypes.CnsQueryFilter,
	querySelection cnstypes.CnsQuerySelection) (*cnstypes.CnsQueryResult, error) {
	return nil, nil
}
func (m *mockVolumeManager) QueryVolumeAsync(ctx context.Context, queryFilter cnstypes.CnsQueryFilter,
	querySelection *cnstypes.CnsQuerySelection) (*cnstypes.CnsQueryResult, error) {
	return nil, nil
}
func (m *mockVolumeManager) QueryVolume(ctx context.Context,
	queryFilter cnstypes.CnsQueryFilter) (*cnstypes.CnsQueryResult, error) {
	return nil, nil
}
func (m *mockVolumeManager) RelocateVolume(ctx context.Context,
	relocateSpecList ...cnstypes.BaseCnsVolumeRelocateSpec) (*object.Task, error) {
	return nil, nil
}
func (m *mockVolumeManager) ExpandVolume(ctx context.Context, volumeID string, size int64,
	extraParams interface{}) (string, error) {
	return "", nil
}
func (m *mockVolumeManager) ResetManager(ctx context.Context, vcenter *vsphere.VirtualCenter) error {
	return nil
}
func (m *mockVolumeManager) ConfigureVolumeACLs(ctx context.Context, spec cnstypes.CnsVolumeACLConfigureSpec) error {
	return nil
}
func (m *mockVolumeManager) RegisterDisk(ctx context.Context, path string, name string) (string, error) {
	return "", nil
}
func (m *mockVolumeManager) RetrieveVStorageObject(ctx context.Context,
	volumeID string) (*types.VStorageObject, error) {
	return nil, nil
}
func (m *mockVolumeManager) ProtectVolumeFromVMDeletion(ctx context.Context, volumeID string) error {
	return nil
}
func (m *mockVolumeManager) CreateSnapshot(ctx context.Context, volumeID string, desc string,
	extraParams interface{}) (*cnsvolume.CnsSnapshotInfo, error) {
	return nil, nil
}
func (m *mockVolumeManager) DeleteSnapshot(ctx context.Context, volumeID string, snapshotID string,
	extraParams interface{}) (*cnsvolume.CnsSnapshotInfo, error) {
	return nil, nil
}
func (m *mockVolumeManager) QuerySnapshots(ctx context.Context,
	snapshotQueryFilter cnstypes.CnsSnapshotQueryFilter) (*cnstypes.CnsSnapshotQueryResult, error) {
	return nil, nil
}
func (m *mockVolumeManager) IsListViewReady() bool {
	return true
}
func (m *mockVolumeManager) SetListViewNotReady(ctx context.Context) {}
func (m *mockVolumeManager) GetOperationStore() cnsvolumeoperationrequest.VolumeOperationRequest {
	return nil
}
func (m *mockVolumeManager) MonitorCreateVolumeTask(ctx context.Context,
	volumeOperationDetails **cnsvolumeoperationrequest.VolumeOperationRequestDetails, task *object.Task,
	volNameFromInputSpec, clusterID string) (*cnsvolume.CnsVolumeInfo, string, error) {
	return nil, "", nil
}

func (m *mockVolumeManager) BatchAttachVolumes(ctx context.Context,
	vm *vsphere.VirtualMachine,
	volumeIDs []cnsvolume.BatchAttachRequest) ([]cnsvolume.BatchAttachResult, string, error) {
	return []cnsvolume.BatchAttachResult{}, "", nil
}

func (m *mockVolumeManager) SyncVolume(ctx context.Context,
	syncVolumeSpecs []cnstypes.CnsSyncVolumeSpec) (string, error) {
	return "", nil
}
func TestQueryVolumeSnapshotsByVolumeIDWithQuerySnapshotsCnsVolumeNotFoundFault(t *testing.T) {
	volumeId := "dummy-id"
	patches := gomonkey.ApplyFunc(utils.QuerySnapshotsUtil, func(_ context.Context, _ cnsvolume.Manager,
		_ cnstypes.CnsSnapshotQueryFilter, _ int64) ([]cnstypes.CnsSnapshotQueryResultEntry, string, error) {
		resultEntry := cnstypes.CnsSnapshotQueryResultEntry{
			Snapshot: cnstypes.CnsSnapshot{},
			Error: &types.LocalizedMethodFault{
				Fault: &cnstypes.CnsVolumeNotFoundFault{
					CnsFault: cnstypes.CnsFault{},
					VolumeId: cnstypes.CnsVolumeId{
						Id: volumeId,
					},
				},
				LocalizedMessage: "volume not found",
			},
		}
		return []cnstypes.CnsSnapshotQueryResultEntry{resultEntry}, "", nil
	})
	defer patches.Reset()
	patches.ApplyFunc(utils.QueryVolumeDetailsUtil, func(_ context.Context, _ cnsvolume.Manager,
		_ []cnstypes.CnsVolumeId) (
		map[string]*utils.CnsVolumeDetails, error) {
		return make(map[string]*utils.CnsVolumeDetails), nil
	})
	results, _, err := QueryVolumeSnapshotsByVolumeID(context.TODO(), nil, volumeId, 100)
	assert.Equal(t, nil, err)
	assert.Equal(t, 0, len(results))
}

func TestQueryVolumeSnapshotsByVolumeIDWithQuerySnapshotsUnexpectedFault(t *testing.T) {
	volumeId := "dummy-id"
	patches := gomonkey.ApplyFunc(utils.QuerySnapshotsUtil, func(_ context.Context, _ cnsvolume.Manager,
		_ cnstypes.CnsSnapshotQueryFilter, _ int64) ([]cnstypes.CnsSnapshotQueryResultEntry, string, error) {
		resultEntry := cnstypes.CnsSnapshotQueryResultEntry{
			Snapshot: cnstypes.CnsSnapshot{},
			Error: &types.LocalizedMethodFault{
				Fault: &types.InvalidArgument{
					RuntimeFault:    types.RuntimeFault{},
					InvalidProperty: "unexpected",
				},
				LocalizedMessage: "unknown fault",
			},
		}
		return []cnstypes.CnsSnapshotQueryResultEntry{resultEntry}, "", nil
	})
	defer patches.Reset()
	patches.ApplyFunc(utils.QueryVolumeDetailsUtil, func(_ context.Context, _ cnsvolume.Manager,
		_ []cnstypes.CnsVolumeId) (
		map[string]*utils.CnsVolumeDetails, error) {
		return make(map[string]*utils.CnsVolumeDetails), nil
	})
	_, _, err := QueryVolumeSnapshotsByVolumeID(context.TODO(), nil, volumeId, 100)
	assert.Error(t, err)
}

func TestQueryVolumeSnapshotWithQuerySnapshotsCnsSnapshotNotFoundFault(t *testing.T) {
	volumeId := "dummy-id"
	snapId := "dummy-snap-id"
	patches := gomonkey.ApplyFunc(utils.QuerySnapshotsUtil, func(_ context.Context, _ cnsvolume.Manager,
		_ cnstypes.CnsSnapshotQueryFilter, _ int64) ([]cnstypes.CnsSnapshotQueryResultEntry, string, error) {
		resultEntry := cnstypes.CnsSnapshotQueryResultEntry{
			Snapshot: cnstypes.CnsSnapshot{},
			Error: &types.LocalizedMethodFault{
				Fault: &cnstypes.CnsSnapshotNotFoundFault{
					CnsFault: cnstypes.CnsFault{},
					VolumeId: cnstypes.CnsVolumeId{
						Id: volumeId,
					},
					SnapshotId: cnstypes.CnsSnapshotId{
						DynamicData: types.DynamicData{},
						Id:          snapId,
					},
				},
				LocalizedMessage: "volume snapshot not found",
			},
		}
		return []cnstypes.CnsSnapshotQueryResultEntry{resultEntry}, "", nil
	})
	defer patches.Reset()
	patches.ApplyFunc(utils.QueryVolumeDetailsUtil, func(_ context.Context, _ cnsvolume.Manager,
		_ []cnstypes.CnsVolumeId) (
		map[string]*utils.CnsVolumeDetails, error) {
		return make(map[string]*utils.CnsVolumeDetails), nil
	})
	_, err := QueryVolumeSnapshot(context.TODO(), nil, volumeId, snapId, 100)
	assert.Error(t, err)
}

func TestQueryVolumeSnapshotWithQuerySnapshotsUnexpectedFault(t *testing.T) {
	volumeId := "dummy-id"
	snapId := "dummy-snap-id"
	patches := gomonkey.ApplyFunc(utils.QuerySnapshotsUtil, func(_ context.Context, _ cnsvolume.Manager,
		_ cnstypes.CnsSnapshotQueryFilter, _ int64) ([]cnstypes.CnsSnapshotQueryResultEntry, string, error) {
		resultEntry := cnstypes.CnsSnapshotQueryResultEntry{
			Snapshot: cnstypes.CnsSnapshot{},
			Error: &types.LocalizedMethodFault{
				Fault: &types.InvalidArgument{
					RuntimeFault:    types.RuntimeFault{},
					InvalidProperty: "unexpected",
				},
				LocalizedMessage: "unknown fault",
			},
		}
		return []cnstypes.CnsSnapshotQueryResultEntry{resultEntry}, "", nil
	})
	defer patches.Reset()
	patches.ApplyFunc(utils.QueryVolumeDetailsUtil, func(_ context.Context, _ cnsvolume.Manager,
		_ []cnstypes.CnsVolumeId) (
		map[string]*utils.CnsVolumeDetails, error) {
		return make(map[string]*utils.CnsVolumeDetails), nil
	})
	_, err := QueryVolumeSnapshot(context.TODO(), nil, volumeId, snapId, 100)
	assert.Error(t, err)
}

func TestQueryAllVolumeSnapshotsWithQuerySnapshotsUnexpectedFault(t *testing.T) {
	patches := gomonkey.ApplyFunc(utils.QuerySnapshotsUtil, func(_ context.Context, _ cnsvolume.Manager,
		_ cnstypes.CnsSnapshotQueryFilter, _ int64) ([]cnstypes.CnsSnapshotQueryResultEntry, string, error) {
		resultEntry := cnstypes.CnsSnapshotQueryResultEntry{
			Snapshot: cnstypes.CnsSnapshot{},
			Error: &types.LocalizedMethodFault{
				Fault: &types.InvalidArgument{
					RuntimeFault:    types.RuntimeFault{},
					InvalidProperty: "unexpected",
				},
				LocalizedMessage: "unknown fault",
			},
		}
		return []cnstypes.CnsSnapshotQueryResultEntry{resultEntry}, "", nil
	})
	defer patches.Reset()
	patches.ApplyFunc(utils.QueryVolumeDetailsUtil, func(_ context.Context, _ cnsvolume.Manager,
		_ []cnstypes.CnsVolumeId) (
		map[string]*utils.CnsVolumeDetails, error) {
		return make(map[string]*utils.CnsVolumeDetails), nil
	})
	_, _, err := QueryAllVolumeSnapshots(context.TODO(), nil, "", 100)
	assert.Error(t, err)
}

// TestCreateBlockVolumeFromSnapshotTargetDatastore tests the CreateBlockVolumeUtil function
// to verify the datastore behavior based on the VolFromSnapshotOnTargetDs flag.
func TestCreateBlockVolumeFromSnapshotTargetDatastore(t *testing.T) {
	firstDatastoreMoRef := types.ManagedObjectReference{Type: "Datastore", Value: "datastore-first"}
	secondDatastoreMoRef := types.ManagedObjectReference{Type: "Datastore", Value: "datastore-second"}
	firstDatastoreURL := "ds:///vmfs/volumes/first-datastore/"

	testCases := []struct {
		name                      string
		volFromSnapshotOnTargetDs bool
		expectedDatastoreCount    int
		description               string
		validateFunc              func(t *testing.T, datastores []types.ManagedObjectReference)
	}{
		{
			name:                      "Feature disabled - should overwrite datastores",
			volFromSnapshotOnTargetDs: false,
			expectedDatastoreCount:    1,
			description: "When VolFromSnapshotOnTargetDs is false, datastores should be " +
				"overwritten with source datastore",
			validateFunc: func(t *testing.T, datastores []types.ManagedObjectReference) {
				assert.Equal(t, firstDatastoreMoRef, datastores[0], "Datastore should be the source datastore")
			},
		},
		{
			name:                      "Feature enabled - should preserve datastores",
			volFromSnapshotOnTargetDs: true,
			expectedDatastoreCount:    2,
			description:               "When VolFromSnapshotOnTargetDs is true, original datastores should be preserved",
			validateFunc: func(t *testing.T, datastores []types.ManagedObjectReference) {
				datastoreValues := make([]string, len(datastores))
				for i, ds := range datastores {
					datastoreValues[i] = ds.Value
				}
				assert.Contains(t, datastoreValues, firstDatastoreMoRef.Value, "First datastore should be preserved")
				assert.Contains(t, datastoreValues, secondDatastoreMoRef.Value, "Second datastore should be preserved")
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			// Mock getVCenterInternal to return a mock VirtualCenter
			originalGetVCenter := getVCenterInternal

			// Create a new vcsim instance for use against govmomi client
			model := simulator.VPX()
			defer model.Remove()
			err := model.Create()
			require.NoError(t, err, "failed to create virtual VC for govmomi client")

			// Create a vcsim server
			s := model.Service.NewServer()

			// Create a new client
			client, err := govmomi.NewClient(context.Background(), s.URL, true)
			require.NoError(t, err, "ailed to create new govmomi client")

			getVCenterInternal = func(_ context.Context, _ *Manager) (*vsphere.VirtualCenter, error) {
				return &vsphere.VirtualCenter{
					Config: &vsphere.VirtualCenterConfig{
						Host:            "test-vc",
						DatacenterPaths: []string{},
					},
					Client: client,
				}, nil
			}
			defer func() { getVCenterInternal = originalGetVCenter }()

			// Mock queryVolumeByIDInternal to return the first datastore URL
			originalQueryVolumeByID := queryVolumeByIDInternal
			queryVolumeByIDInternal = func(_ context.Context, _ cnsvolume.Manager, _ string,
				_ *cnstypes.CnsQuerySelection) (*cnstypes.CnsVolume, error) {
				return &cnstypes.CnsVolume{DatastoreUrl: firstDatastoreURL}, nil
			}
			defer func() { queryVolumeByIDInternal = originalQueryVolumeByID }()

			// Track the create spec passed to CreateVolume
			var capturedCreateSpec *cnstypes.CnsVolumeCreateSpec

			// Mock VolumeManager.CreateVolume to capture the create spec
			mockVolumeManager := &mockVolumeManager{
				createVolumeFunc: func(_ context.Context, spec *cnstypes.CnsVolumeCreateSpec,
					_ interface{}) (*cnsvolume.CnsVolumeInfo, string, error) {
					capturedCreateSpec = spec
					return &cnsvolume.CnsVolumeInfo{VolumeID: cnstypes.CnsVolumeId{Id: "test-volume-id"}}, "", nil
				},
			}

			// Create a test manager with minimal configuration
			manager := &Manager{
				VolumeManager: mockVolumeManager,
				CnsConfig:     &config.Config{},
			}
			manager.CnsConfig.Global.ClusterID = "test-cluster"
			manager.CnsConfig.VirtualCenter = map[string]*config.VirtualCenterConfig{
				"test-vc": {User: "test-user"},
			}

			// Create test datastore info objects
			firstDatastoreInfo := &vsphere.DatastoreInfo{
				Datastore: &vsphere.Datastore{Datastore: object.NewDatastore(nil, firstDatastoreMoRef)},
				Info:      &types.DatastoreInfo{Url: firstDatastoreURL},
			}
			secondDatastoreInfo := &vsphere.DatastoreInfo{
				Datastore: &vsphere.Datastore{Datastore: object.NewDatastore(nil, secondDatastoreMoRef)},
				Info:      &types.DatastoreInfo{Url: "ds:///vmfs/volumes/second-datastore/"},
			}

			// Create spec with snapshot
			spec := &CreateVolumeSpec{
				Name:                    "test-volume",
				CapacityMB:              1024,
				VolumeType:              BlockVolumeType,
				ContentSourceSnapshotID: "source-volume-id+snapshot-id",
				ScParams:                &StorageClassParams{},
			}

			// Create options with the test case flag setting
			opts := CreateBlockVolumeOptions{VolFromSnapshotOnTargetDs: tc.volFromSnapshotOnTargetDs}

			// Create shared datastores list
			sharedDatastores := []*vsphere.DatastoreInfo{firstDatastoreInfo, secondDatastoreInfo}

			// Call the actual CreateBlockVolumeUtil function
			// Note that the cluster flavor does not have a significance for this test
			// because it depends on the VolFromSnapshotOnTargetDs flag.
			_, _, err = CreateBlockVolumeUtil(context.Background(), cnstypes.CnsClusterFlavorWorkload,
				manager, spec, sharedDatastores, []string{}, opts, nil)

			// Verify no error occurred
			assert.NoError(t, err, "CreateBlockVolumeUtil should not return an error")

			// Verify the datastore behavior
			assert.NotNil(t, capturedCreateSpec, "Create spec should have been captured")
			assert.Len(t, capturedCreateSpec.Datastores, tc.expectedDatastoreCount, tc.description)

			// Run the test case specific validation
			tc.validateFunc(t, capturedCreateSpec.Datastores)
		})
	}
}
