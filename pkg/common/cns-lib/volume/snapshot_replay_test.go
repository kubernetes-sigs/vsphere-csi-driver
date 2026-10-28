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

package volume

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	cnstypes "github.com/vmware/govmomi/cns/types"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"sigs.k8s.io/vsphere-csi-driver/v3/pkg/csi/service/logger"
	"sigs.k8s.io/vsphere-csi-driver/v3/pkg/internalapis/cnsvolumeoperationrequest"
)

// replayOperationStore returns a persisted successful operation for every request.
type replayOperationStore struct {
	cnsvolumeoperationrequest.VolumeOperationRequest
	details *cnsvolumeoperationrequest.VolumeOperationRequestDetails
}

func (s *replayOperationStore) GetRequestDetails(_ context.Context,
	_ string) (*cnsvolumeoperationrequest.VolumeOperationRequestDetails, error) {
	return s.details, nil
}

func (s *replayOperationStore) StoreRequestDetails(_ context.Context,
	_ *cnsvolumeoperationrequest.VolumeOperationRequestDetails) error {
	return nil
}

func successfulSnapshotOperation(volumeID, snapshotID string) *cnsvolumeoperationrequest.VolumeOperationRequestDetails {
	return &cnsvolumeoperationrequest.VolumeOperationRequestDetails{
		VolumeID:   volumeID,
		SnapshotID: snapshotID,
		QuotaDetails: &cnsvolumeoperationrequest.QuotaDetails{
			AggregatedSnapshotSize:              resource.NewQuantity(10*MbInBytes, resource.BinarySI),
			SnapshotLatestOperationCompleteTime: metav1.NewTime(time.Now().Add(-time.Hour)),
		},
		OperationDetails: &cnsvolumeoperationrequest.OperationDetails{
			TaskStatus: taskInvocationStatusSuccess,
			OpID:       "op-1",
		},
	}
}

// stubSnapshotCapacityQuery replaces the CNS aggregated snapshot capacity query for the test.
func stubSnapshotCapacityQuery(t *testing.T, capacityInMb int64, err error) *[]string {
	t.Helper()
	var queried []string
	saved := queryAggregatedSnapshotCapacityInMb
	queryAggregatedSnapshotCapacityInMb = func(_ context.Context, _ Manager, volumeID string) (int64, error) {
		queried = append(queried, volumeID)
		return capacityInMb, err
	}
	t.Cleanup(func() { queryAggregatedSnapshotCapacityInMb = saved })
	return &queried
}

// A replayed operation must not return the size persisted by an earlier attempt: that size may
// predate the caller's read of CnsVolumeInfo. The size is measured again instead.
func TestSnapshotReplayMeasuresAggregatedSnapshotCapacity(t *testing.T) {
	const volumeID, snapshotID = "vol-1", "snap-1"
	newManager := func() *defaultManager {
		return &defaultManager{
			idempotencyHandlingEnabled: true,
			clusterFlavor:              cnstypes.CnsClusterFlavorWorkload,
			operationStore: &replayOperationStore{
				details: successfulSnapshotOperation(volumeID, snapshotID),
			},
		}
	}
	replays := map[string]func(ctx context.Context, m *defaultManager) (*CnsSnapshotInfo, error){
		"create": func(ctx context.Context, m *defaultManager) (*CnsSnapshotInfo, error) {
			return m.createSnapshotWithImprovedIdempotencyCheck(ctx, volumeID, "snapshot-"+snapshotID,
				&CreateSnapshotExtraParams{IsStorageQuotaM2FSSEnabled: true})
		},
		"delete": func(ctx context.Context, m *defaultManager) (*CnsSnapshotInfo, error) {
			return m.deleteSnapshotWithImprovedIdempotencyCheck(ctx, volumeID, snapshotID,
				&DeletesnapshotExtraParams{IsStorageQuotaM2FSSEnabled: true})
		},
	}
	for name, replay := range replays {
		t.Run(name, func(t *testing.T) {
			ctx := logger.NewContextWithLogger(context.Background())
			queried := stubSnapshotCapacityQuery(t, 7, nil)

			info, err := replay(ctx, newManager())

			require.NoError(t, err)
			require.NotNil(t, info)
			assert.Equal(t, snapshotID, info.SnapshotID)
			assert.Equal(t, int64(7), info.AggregatedSnapshotCapacityInMb, "measured now, not replayed")
			assert.Equal(t, []string{volumeID}, *queried)
		})
		t.Run(name+" query failure", func(t *testing.T) {
			ctx := logger.NewContextWithLogger(context.Background())
			stubSnapshotCapacityQuery(t, 0, errors.New("CNS unavailable"))

			_, err := replay(ctx, newManager())

			require.Error(t, err)
		})
	}
}

// Without the StorageQuotaM2 workload path there is no aggregate to report, so nothing is queried.
func TestSnapshotReplayWithoutQuotaDoesNotQuery(t *testing.T) {
	ctx := logger.NewContextWithLogger(context.Background())
	queried := stubSnapshotCapacityQuery(t, 7, nil)
	m := &defaultManager{
		idempotencyHandlingEnabled: true,
		clusterFlavor:              cnstypes.CnsClusterFlavorVanilla,
		operationStore:             &replayOperationStore{details: successfulSnapshotOperation("vol-1", "snap-1")},
	}

	_, err := m.createSnapshotWithImprovedIdempotencyCheck(ctx, "vol-1", "snapshot-snap-1",
		&CreateSnapshotExtraParams{IsStorageQuotaM2FSSEnabled: true})
	require.NoError(t, err)
	_, err = m.deleteSnapshotWithImprovedIdempotencyCheck(ctx, "vol-1", "snap-1",
		&DeletesnapshotExtraParams{IsStorageQuotaM2FSSEnabled: true})
	require.NoError(t, err)

	assert.Empty(t, *queried)
}

// snapshotCapacityQueryManager answers QueryVolume with a fixed result and records the filter.
type snapshotCapacityQueryManager struct {
	Manager
	result *cnstypes.CnsQueryResult
	err    error
	filter cnstypes.CnsQueryFilter
}

func (m *snapshotCapacityQueryManager) QueryVolume(_ context.Context,
	queryFilter cnstypes.CnsQueryFilter) (*cnstypes.CnsQueryResult, error) {
	m.filter = queryFilter
	return m.result, m.err
}

func TestQueryAggregatedSnapshotCapacityInMb(t *testing.T) {
	blockVolume := func(capacityInMb int64) *cnstypes.CnsQueryResult {
		return &cnstypes.CnsQueryResult{Volumes: []cnstypes.CnsVolume{{
			VolumeId: cnstypes.CnsVolumeId{Id: "vol-1"},
			BackingObjectDetails: &cnstypes.CnsBlockBackingDetails{
				AggregatedSnapshotCapacityInMb: capacityInMb,
			},
		}}}
	}
	tests := []struct {
		name      string
		result    *cnstypes.CnsQueryResult
		queryErr  error
		want      int64
		wantError bool
	}{
		{name: "returns CNS capacity", result: blockVolume(42), want: 42},
		{name: "returns invalid capacity as -1", result: blockVolume(-1), want: -1},
		{name: "query error", queryErr: errors.New("CNS unavailable"), wantError: true},
		{name: "nil result", wantError: true},
		{name: "volume not returned", result: &cnstypes.CnsQueryResult{}, wantError: true},
		{
			name: "not a block volume",
			result: &cnstypes.CnsQueryResult{Volumes: []cnstypes.CnsVolume{{
				BackingObjectDetails: &cnstypes.CnsVsanFileShareBackingDetails{},
			}}},
			wantError: true,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			ctx := logger.NewContextWithLogger(context.Background())
			m := &snapshotCapacityQueryManager{result: tc.result, err: tc.queryErr}

			got, err := QueryAggregatedSnapshotCapacityInMb(ctx, m, "vol-1")

			assert.Equal(t, []cnstypes.CnsVolumeId{{Id: "vol-1"}}, m.filter.VolumeIds)
			if tc.wantError {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)
			assert.Equal(t, tc.want, got)
		})
	}
}
