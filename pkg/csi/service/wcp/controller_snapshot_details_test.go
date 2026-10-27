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

package wcp

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	cnsvolume "sigs.k8s.io/vsphere-csi-driver/v3/pkg/common/cns-lib/volume"
	"sigs.k8s.io/vsphere-csi-driver/v3/pkg/csi/service/common"
	"sigs.k8s.io/vsphere-csi-driver/v3/pkg/csi/service/logger"
	"sigs.k8s.io/vsphere-csi-driver/v3/pkg/internalapis/cnsvolumeinfo"
	cnsvolumeinfov1alpha1 "sigs.k8s.io/vsphere-csi-driver/v3/pkg/internalapis/cnsvolumeinfo/v1alpha1"
)

// snapshotDetailsRecorder records UpdateVolumeInfoSnapshotDetails calls.
type snapshotDetailsRecorder struct {
	cnsvolumeinfo.VolumeInfoService
	err error

	calls          int
	volumeID       string
	preReadVersion string
	measured       *cnsvolume.CnsSnapshotInfo
	volumeManager  cnsvolume.Manager
}

func (r *snapshotDetailsRecorder) UpdateVolumeInfoSnapshotDetails(_ context.Context, volumeID,
	preReadResourceVersion string, measured *cnsvolume.CnsSnapshotInfo, volumeManager cnsvolume.Manager) error {
	r.calls++
	r.volumeID = volumeID
	r.preReadVersion = preReadResourceVersion
	r.measured = measured
	r.volumeManager = volumeManager
	return r.err
}

// noopVolumeManager stands in for the CNS volume manager the controller passes along.
type noopVolumeManager struct {
	cnsvolume.Manager
}

func TestUpdateCNSVolumeInfoSnapshotDetails(t *testing.T) {
	const volumeID = "vol-1"
	taskTime := time.Now()
	preReadAt := func(at time.Time) *cnsvolumeinfov1alpha1.CNSVolumeInfo {
		return &cnsvolumeinfov1alpha1.CNSVolumeInfo{
			ObjectMeta: metav1.ObjectMeta{Name: volumeID, ResourceVersion: "41"},
			Spec: cnsvolumeinfov1alpha1.CNSVolumeInfoSpec{
				VolumeID:                            volumeID,
				SnapshotLatestOperationCompleteTime: metav1.NewTime(at),
			},
		}
	}
	snapshotInfo := &cnsvolume.CnsSnapshotInfo{
		SourceVolumeID:                      volumeID,
		AggregatedSnapshotCapacityInMb:      10,
		SnapshotLatestOperationCompleteTime: taskTime,
	}
	tests := []struct {
		name          string
		preRead       *cnsvolumeinfov1alpha1.CNSVolumeInfo
		serviceErr    error
		wantErrorCode codes.Code
	}{
		{
			name:    "the result is written with the pre-read version",
			preRead: preReadAt(taskTime.Add(-time.Minute)),
		},
		{
			name:    "a newer stored timestamp does not skip the write",
			preRead: preReadAt(taskTime.Add(time.Minute)),
		},
		{
			name:          "write failure is an internal error",
			preRead:       preReadAt(taskTime.Add(-time.Minute)),
			serviceErr:    errors.New("conflicts exhausted"),
			wantErrorCode: codes.Internal,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			ctx := logger.NewContextWithLogger(context.Background())
			recorder := &snapshotDetailsRecorder{err: tc.serviceErr}
			saved := volumeInfoService
			volumeInfoService = recorder
			t.Cleanup(func() { volumeInfoService = saved })
			manager := &noopVolumeManager{}
			c := &controller{manager: &common.Manager{VolumeManager: manager}}

			err := c.updateCNSVolumeInfoSnapshotDetails(ctx, volumeID, tc.preRead, snapshotInfo)

			if tc.wantErrorCode != codes.OK {
				require.Error(t, err)
				assert.Equal(t, tc.wantErrorCode, status.Code(err))
			} else {
				require.NoError(t, err)
			}
			require.Equal(t, 1, recorder.calls, "the write is never skipped")
			assert.Equal(t, volumeID, recorder.volumeID)
			assert.Equal(t, "41", recorder.preReadVersion)
			assert.Same(t, snapshotInfo, recorder.measured)
			assert.Same(t, manager, recorder.volumeManager, "CNS is re-measured through the controller's manager")
		})
	}
}
