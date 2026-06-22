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

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	cnstypes "github.com/vmware/govmomi/cns/types"
)

// TestMockManagerImplementsInterface verifies that MockManager satisfies the
// full Manager interface (compilation-level check augmented with a runtime
// nil-interface assignment).
func TestMockManagerImplementsInterface(t *testing.T) {
	var _ Manager = MockManager{}
}

// TestMockUnregisterVolumeExSuccess verifies the success path of the mock.
func TestMockUnregisterVolumeExSuccess(t *testing.T) {
	ctx := context.Background()
	m := NewMockManager(false, nil, "")

	backingDiskPath, diskUUID, err := m.UnregisterVolumeEx(ctx, "test-volume-id")

	require.NoError(t, err)
	assert.Empty(t, backingDiskPath)
	assert.Empty(t, diskUUID)
}

// TestMockUnregisterVolumeExFailure verifies the error path of the mock.
func TestMockUnregisterVolumeExFailure(t *testing.T) {
	ctx := context.Background()
	sentinelErr := errors.New("cns unregister ex failed")
	m := NewMockManager(true, sentinelErr, "vim25:SystemError")

	_, _, err := m.UnregisterVolumeEx(ctx, "test-volume-id")

	require.Error(t, err)
	assert.Equal(t, sentinelErr, err)
}

// TestMockQueryPendingUnregistersSuccess verifies the success path.
func TestMockQueryPendingUnregistersSuccess(t *testing.T) {
	ctx := context.Background()
	m := NewMockManager(false, nil, "")

	results, err := m.QueryPendingUnregisters(ctx)

	require.NoError(t, err)
	assert.Nil(t, results)
}

// TestMockQueryPendingUnregistersFailure verifies the error path.
func TestMockQueryPendingUnregistersFailure(t *testing.T) {
	ctx := context.Background()
	sentinelErr := errors.New("cns query pending unregisters failed")
	m := NewMockManager(true, sentinelErr, "vim25:SystemError")

	results, err := m.QueryPendingUnregisters(ctx)

	require.Error(t, err)
	assert.Equal(t, sentinelErr, err)
	assert.Nil(t, results)
}

// TestMockAckUnregisterSuccess verifies the success path.
func TestMockAckUnregisterSuccess(t *testing.T) {
	ctx := context.Background()
	m := NewMockManager(false, nil, "")

	err := m.AckUnregister(ctx, "test-volume-id")

	require.NoError(t, err)
}

// TestMockAckUnregisterFailure verifies the error path.
func TestMockAckUnregisterFailure(t *testing.T) {
	ctx := context.Background()
	sentinelErr := errors.New("cns ack unregister failed")
	m := NewMockManager(true, sentinelErr, "vim25:SystemError")

	err := m.AckUnregister(ctx, "test-volume-id")

	require.Error(t, err)
	assert.Equal(t, sentinelErr, err)
}

// TestDefaultManagerUnregisterVolumeExNilVC verifies that UnregisterVolumeEx
// returns an error immediately when the virtualCenter is nil (no live VC
// required for this unit test path).
func TestDefaultManagerUnregisterVolumeExNilVC(t *testing.T) {
	ctx := context.Background()
	m := &defaultManager{} // virtualCenter is nil

	_, _, err := m.UnregisterVolumeEx(ctx, "vol-001")

	require.Error(t, err)
	assert.Contains(t, err.Error(), "virtual center connection not established")
}

// TestDefaultManagerQueryPendingUnregistersNilVC verifies that
// QueryPendingUnregisters returns an error when the virtualCenter is nil.
func TestDefaultManagerQueryPendingUnregistersNilVC(t *testing.T) {
	ctx := context.Background()
	m := &defaultManager{}

	_, err := m.QueryPendingUnregisters(ctx)

	require.Error(t, err)
	assert.Contains(t, err.Error(), "virtual center connection not established")
}

// TestDefaultManagerAckUnregisterNilVC verifies that AckUnregister returns
// an error when the virtualCenter is nil.
func TestDefaultManagerAckUnregisterNilVC(t *testing.T) {
	ctx := context.Background()
	m := &defaultManager{}

	err := m.AckUnregister(ctx, "vol-001")

	require.Error(t, err)
	assert.Contains(t, err.Error(), "virtual center connection not established")
}

// TestQueryPendingUnregistersResultType verifies that the CnsUnregisterVolumeResult
// type has the expected BackingDiskPath and DiskUUID fields (structural regression
// guard — if the govmomi type changes shape, this test will not compile).
func TestQueryPendingUnregistersResultType(t *testing.T) {
	r := cnstypes.CnsUnregisterVolumeResult{
		BackingDiskPath: "/vmfs/volumes/ds1/disk.vmdk",
		DiskUUID:        "6000c29a-1234-5678-abcd-ef0123456789",
	}
	assert.Equal(t, "/vmfs/volumes/ds1/disk.vmdk", r.BackingDiskPath)
	assert.Equal(t, "6000c29a-1234-5678-abcd-ef0123456789", r.DiskUUID)
}
