package common

import (
	"context"
	"testing"

	"github.com/agiledragon/gomonkey/v2"
	"github.com/stretchr/testify/assert"
	cnstypes "github.com/vmware/govmomi/cns/types"
	"github.com/vmware/govmomi/vim25/types"
	cnsvolume "sigs.k8s.io/vsphere-csi-driver/v3/pkg/common/cns-lib/volume"
	"sigs.k8s.io/vsphere-csi-driver/v3/pkg/common/utils"
)

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
