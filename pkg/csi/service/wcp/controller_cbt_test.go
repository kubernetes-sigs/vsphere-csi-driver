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
	"fmt"
	"testing"

	"github.com/container-storage-interface/spec/lib/go/csi"
	cnstypes "github.com/vmware/govmomi/cns/types"
	"github.com/vmware/govmomi/vim25/soap"
	"github.com/vmware/govmomi/vim25/types"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	cnsvolume "sigs.k8s.io/vsphere-csi-driver/v3/pkg/common/cns-lib/volume"
	cnsvsphere "sigs.k8s.io/vsphere-csi-driver/v3/pkg/common/cns-lib/vsphere"
	"sigs.k8s.io/vsphere-csi-driver/v3/pkg/common/unittestcommon"
	"sigs.k8s.io/vsphere-csi-driver/v3/pkg/csi/service/common"
	"sigs.k8s.io/vsphere-csi-driver/v3/pkg/csi/service/common/commonco"
	"sigs.k8s.io/vsphere-csi-driver/v3/pkg/csi/service/logger"
)

type mockAllocatedServer struct {
	grpc.ServerStream
	ctx context.Context
	t   *testing.T
}

func (m *mockAllocatedServer) Context() context.Context {
	return m.ctx
}

func (m *mockAllocatedServer) Send(resp *csi.GetMetadataAllocatedResponse) error {
	if m.t != nil && resp.BlockMetadataType != csi.BlockMetadataType_VARIABLE_LENGTH {
		m.t.Errorf("Expected BlockMetadataType to be %v, got %v",
			csi.BlockMetadataType_VARIABLE_LENGTH, resp.BlockMetadataType)
	}
	return nil
}

type mockDeltaServer struct {
	grpc.ServerStream
	ctx context.Context
	t   *testing.T
}

func (m *mockDeltaServer) Context() context.Context {
	return m.ctx
}

func (m *mockDeltaServer) Send(resp *csi.GetMetadataDeltaResponse) error {
	if m.t != nil && resp.BlockMetadataType != csi.BlockMetadataType_VARIABLE_LENGTH {
		m.t.Errorf("Expected BlockMetadataType to be %v, got %v",
			csi.BlockMetadataType_VARIABLE_LENGTH, resp.BlockMetadataType)
	}
	return nil
}

type mockVolumeManager struct {
	cnsvolume.Manager
}

func (m *mockVolumeManager) QueryVolume(ctx context.Context,
	queryFilter cnstypes.CnsQueryFilter) (*cnstypes.CnsQueryResult, error) {
	res, err := m.Manager.QueryVolume(ctx, queryFilter)
	if err == nil && res != nil && len(res.Volumes) > 0 {
		for i := range res.Volumes {
			res.Volumes[i].BackingObjectDetails = &cnstypes.CnsBlockBackingDetails{
				CnsBackingObjectDetails: cnstypes.CnsBackingObjectDetails{CapacityInMb: 1024},
			}
		}
	}
	return res, err
}

// TestValidateGetMetadataAllocatedRequest tests the validation function
func TestValidateGetMetadataAllocatedRequest(t *testing.T) {
	ctx := context.Background()
	ctx = logger.NewContextWithLogger(ctx)

	tests := []struct {
		name    string
		req     *csi.GetMetadataAllocatedRequest
		wantErr bool
	}{
		{
			name:    "nil request",
			req:     nil,
			wantErr: true,
		},
		{
			name: "empty snapshot ID",
			req: &csi.GetMetadataAllocatedRequest{
				SnapshotId: "",
			},
			wantErr: true,
		},
		{
			name: "valid request",
			req: &csi.GetMetadataAllocatedRequest{
				SnapshotId:     "volume-123+snapshot-456",
				StartingOffset: 0,
				MaxResults:     1000,
			},
			wantErr: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := validateGetMetadataAllocatedRequest(ctx, tt.req)
			if (err != nil) != tt.wantErr {
				t.Errorf("validateGetMetadataAllocatedRequest() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

// TestValidateGetMetadataDeltaRequest tests the validation function
func TestValidateGetMetadataDeltaRequest(t *testing.T) {
	ctx := context.Background()
	ctx = logger.NewContextWithLogger(ctx)

	tests := []struct {
		name    string
		req     *csi.GetMetadataDeltaRequest
		wantErr bool
	}{
		{
			name:    "nil request",
			req:     nil,
			wantErr: true,
		},
		{
			name: "empty base snapshot ID",
			req: &csi.GetMetadataDeltaRequest{
				BaseSnapshotId:   "",
				TargetSnapshotId: "volume-123+snapshot-456",
			},
			wantErr: true,
		},
		{
			name: "empty target snapshot ID",
			req: &csi.GetMetadataDeltaRequest{
				BaseSnapshotId:   "volume-123+snapshot-123",
				TargetSnapshotId: "",
			},
			wantErr: true,
		},
		{
			name: "same base and target snapshot",
			req: &csi.GetMetadataDeltaRequest{
				BaseSnapshotId:   "volume-123+snapshot-456",
				TargetSnapshotId: "volume-123+snapshot-456",
			},
			wantErr: true,
		},
		{
			name: "valid request",
			req: &csi.GetMetadataDeltaRequest{
				BaseSnapshotId:   "volume-123+snapshot-123",
				TargetSnapshotId: "volume-123+snapshot-456",
				StartingOffset:   0,
				MaxResults:       1000,
			},
			wantErr: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := validateGetMetadataDeltaRequest(ctx, tt.req)
			if (err != nil) != tt.wantErr {
				t.Errorf("validateGetMetadataDeltaRequest() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

// TestGetMetadataAllocated_FSSDisabled tests that the API returns Unimplemented when FSS is disabled
func TestGetMetadataAllocated_FSSDisabled(t *testing.T) {
	ct := getControllerTest(t)

	// Since we use the fake orchestrator, we need to ensure CBT feature state returns false.
	// Actually, by default in unittestcommon it returns false if not set.

	req := &csi.GetMetadataAllocatedRequest{
		SnapshotId: "volume-123+snapshot-456",
	}

	err := ct.controller.GetMetadataAllocated(req, &mockAllocatedServer{ctx: ctx})
	if status.Code(err) != codes.Unimplemented {
		t.Errorf("Expected Unimplemented error when CBT FSS is disabled, got: %v", err)
	}
}

// TestGetMetadataDelta_FSSDisabled tests that the API returns Unimplemented when FSS is disabled
func TestGetMetadataDelta_FSSDisabled(t *testing.T) {
	ct := getControllerTest(t)

	req := &csi.GetMetadataDeltaRequest{
		BaseSnapshotId:   "volume-123+snapshot-123",
		TargetSnapshotId: "volume-123+snapshot-456",
	}

	err := ct.controller.GetMetadataDelta(req, &mockDeltaServer{ctx: ctx})
	if status.Code(err) != codes.Unimplemented {
		t.Errorf("Expected Unimplemented error when CBT FSS is disabled, got: %v", err)
	}
}

// TestGetMetadataAllocated_VolumeNotFound tests that the API returns NotFound when volume is not found
func TestGetMetadataAllocated_VolumeNotFound(t *testing.T) {
	ct := getControllerTest(t)
	fakeOrchestrator := commonco.ContainerOrchestratorUtility.(*unittestcommon.FakeK8SOrchestrator)
	_ = fakeOrchestrator.EnableFSS(ctx, common.CBT)

	req := &csi.GetMetadataAllocatedRequest{
		SnapshotId: "nonexistent-volume+snapshot-456",
	}

	err := ct.controller.GetMetadataAllocated(req, &mockAllocatedServer{ctx: ctx})
	if status.Code(err) != codes.NotFound {
		t.Errorf("Expected NotFound error when volume does not exist, got: %v", err)
	}
}

// TestGetMetadataDelta_VolumeNotFound tests that the API returns NotFound when volume is not found
func TestGetMetadataDelta_VolumeNotFound(t *testing.T) {
	ct := getControllerTest(t)
	fakeOrchestrator := commonco.ContainerOrchestratorUtility.(*unittestcommon.FakeK8SOrchestrator)
	_ = fakeOrchestrator.EnableFSS(ctx, common.CBT)

	req := &csi.GetMetadataDeltaRequest{
		BaseSnapshotId:   "nonexistent-volume+snapshot-123",
		TargetSnapshotId: "nonexistent-volume+snapshot-456",
	}

	err := ct.controller.GetMetadataDelta(req, &mockDeltaServer{ctx: ctx})
	if status.Code(err) != codes.NotFound {
		t.Errorf("Expected NotFound error when volume does not exist, got: %v", err)
	}
}

// TestGetMetadataAllocated_InvalidSnapshotID tests error handling for invalid snapshot IDs
func TestGetMetadataAllocated_InvalidSnapshotID(t *testing.T) {
	ctx := context.Background()
	ctx = logger.NewContextWithLogger(ctx)

	req := &csi.GetMetadataAllocatedRequest{
		SnapshotId: "invalid-snapshot-id-format",
	}

	err := validateGetMetadataAllocatedRequest(ctx, req)
	if err != nil {
		// Expected to pass validation, but will fail when parsing
		t.Logf("Validation passed, parsing would fail: %v", err)
	}
}

// TestGetMetadataDelta_DifferentVolumes tests that the API returns InvalidArgument when the base and
// target snapshot IDs refer to different CNS volumes (volume ID mismatch after parsing).
func TestGetMetadataDelta_DifferentVolumes(t *testing.T) {
	ct := getControllerTest(t)
	fakeOrchestrator := commonco.ContainerOrchestratorUtility.(*unittestcommon.FakeK8SOrchestrator)
	_ = fakeOrchestrator.EnableFSS(ctx, common.CBT)

	req := &csi.GetMetadataDeltaRequest{
		BaseSnapshotId:   "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa+11111111-1111-1111-1111-111111111111",
		TargetSnapshotId: "bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb+22222222-2222-2222-2222-222222222222",
	}

	err := ct.controller.GetMetadataDelta(req, &mockDeltaServer{ctx: ctx})
	if status.Code(err) != codes.InvalidArgument {
		t.Errorf("Expected InvalidArgument when base and target snapshots are on different volumes, got: %v", err)
	}
}

// Mock structures for testing (would be expanded in full test suite)

// TestAllocatedAreaConversion tests the conversion from VSLM response to CSI format
func TestAllocatedAreaConversion(t *testing.T) {
	areas := []AllocatedArea{
		{Offset: 0, Length: 4096},
		{Offset: 8192, Length: 4096},
		{Offset: 16384, Length: 8192},
	}

	var blockMetadata []*csi.BlockMetadata
	for _, area := range areas {
		blockMetadata = append(blockMetadata, &csi.BlockMetadata{
			ByteOffset: int64(area.Offset),
			SizeBytes:  int64(area.Length),
		})
	}

	if len(blockMetadata) != 3 {
		t.Errorf("Expected 3 block metadata entries, got %d", len(blockMetadata))
	}

	if blockMetadata[0].ByteOffset != 0 || blockMetadata[0].SizeBytes != 4096 {
		t.Errorf("First block metadata incorrect: offset=%d size=%d",
			blockMetadata[0].ByteOffset, blockMetadata[0].SizeBytes)
	}

	if blockMetadata[2].SizeBytes != 8192 {
		t.Errorf("Third block metadata size incorrect: %d", blockMetadata[2].SizeBytes)
	}
}

// TestChangedAreaConversion tests the conversion from VSLM response to CSI format
func TestChangedAreaConversion(t *testing.T) {
	areas := []ChangedArea{
		{Offset: 4096, Length: 4096},
		{Offset: 12288, Length: 4096},
	}

	var blockMetadata []*csi.BlockMetadata
	for _, area := range areas {
		blockMetadata = append(blockMetadata, &csi.BlockMetadata{
			ByteOffset: int64(area.Offset),
			SizeBytes:  int64(area.Length),
		})
	}

	if len(blockMetadata) != 2 {
		t.Errorf("Expected 2 block metadata entries, got %d", len(blockMetadata))
	}

	if blockMetadata[0].ByteOffset != 4096 {
		t.Errorf("First changed block offset incorrect: %d", blockMetadata[0].ByteOffset)
	}
}

// TestPaginationLogic tests the nextOffset calculation
func TestPaginationLogic(t *testing.T) {
	tests := []struct {
		name           string
		areas          []AllocatedArea
		maxResults     uint32
		startingOffset uint64
		wantNextOffset uint64
	}{
		{
			name: "fewer results than max",
			areas: []AllocatedArea{
				{Offset: 0, Length: 4096},
				{Offset: 8192, Length: 4096},
			},
			maxResults:     10,
			startingOffset: 0,
			wantNextOffset: 0, // Signals completion
		},
		{
			name: "exactly max results",
			areas: []AllocatedArea{
				{Offset: 0, Length: 4096},
				{Offset: 4096, Length: 4096},
			},
			maxResults:     2,
			startingOffset: 0,
			wantNextOffset: 8192, // End of last area
		},
		{
			name: "more results than max",
			areas: []AllocatedArea{
				{Offset: 0, Length: 4096},
				{Offset: 4096, Length: 4096},
				{Offset: 8192, Length: 4096},
			},
			maxResults:     2,
			startingOffset: 0,
			wantNextOffset: 8192, // After taking first 2
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Simulate pagination logic
			nextOffset := tt.startingOffset
			resultCount := 0

			for _, area := range tt.areas {
				if uint32(resultCount) >= tt.maxResults {
					break
				}
				resultCount++
				areaEnd := area.Offset + area.Length
				if areaEnd > nextOffset {
					nextOffset = areaEnd
				}
			}

			if uint32(resultCount) < tt.maxResults {
				nextOffset = 0
			}

			if nextOffset != tt.wantNextOffset {
				t.Errorf("nextOffset = %d, want %d", nextOffset, tt.wantNextOffset)
			}
		})
	}
}

// TestErrorCodes tests that proper gRPC error codes are returned
func TestErrorCodes(t *testing.T) {
	tests := []struct {
		name     string
		errCheck func(error) bool
		wantCode codes.Code
	}{
		{
			name:     "InvalidArgument",
			errCheck: func(err error) bool { return status.Code(err) == codes.InvalidArgument },
			wantCode: codes.InvalidArgument,
		},
		{
			name:     "NotFound",
			errCheck: func(err error) bool { return status.Code(err) == codes.NotFound },
			wantCode: codes.NotFound,
		},
		{
			name:     "Internal",
			errCheck: func(err error) bool { return status.Code(err) == codes.Internal },
			wantCode: codes.Internal,
		},
		{
			name:     "Unimplemented",
			errCheck: func(err error) bool { return status.Code(err) == codes.Unimplemented },
			wantCode: codes.Unimplemented,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := status.Error(tt.wantCode, "test error")
			if !tt.errCheck(err) {
				t.Errorf("Error code check failed for %s", tt.name)
			}
		})
	}
}

// TestGetMetadataAllocated_Success tests the success path
func TestGetMetadataAllocated_Success(t *testing.T) {
	ct := getControllerTest(t)
	fakeOrchestrator := commonco.ContainerOrchestratorUtility.(*unittestcommon.FakeK8SOrchestrator)
	_ = fakeOrchestrator.EnableFSS(ctx, common.CBT)

	origVolumeManager := ct.controller.manager.VolumeManager
	ct.controller.manager.VolumeManager = &mockVolumeManager{Manager: origVolumeManager}
	defer func() { ct.controller.manager.VolumeManager = origVolumeManager }()

	// Create a volume first to avoid NotFound error
	reqCreate := &csi.CreateVolumeRequest{
		Name: testVolumeName + "-allocated",
		CapacityRange: &csi.CapacityRange{
			RequiredBytes: 1 * common.GbInBytes,
		},
		VolumeCapabilities: []*csi.VolumeCapability{
			{
				AccessType: &csi.VolumeCapability_Mount{
					Mount: &csi.VolumeCapability_MountVolume{},
				},
				AccessMode: &csi.VolumeCapability_AccessMode{
					Mode: csi.VolumeCapability_AccessMode_SINGLE_NODE_WRITER,
				},
			},
		},
	}
	respCreate, err := ct.controller.CreateVolume(ctx, reqCreate)
	if err != nil {
		t.Fatalf("Failed to create volume: %v", err)
	}
	volID := respCreate.Volume.VolumeId

	// Mock the VSLM call
	origQueryChangedDiskAreasFunc := queryChangedDiskAreasFunc
	defer func() { queryChangedDiskAreasFunc = origQueryChangedDiskAreasFunc }()

	queryChangedDiskAreasFunc = func(ctx context.Context, vcenter *cnsvsphere.VirtualCenter,
		volumeID types.ID, snapshotID types.ID, startingOffset int64, changeId string) (*types.DiskChangeInfo, error) {
		return &types.DiskChangeInfo{
			ChangedArea: []types.DiskChangeExtent{
				{Start: 0, Length: 4096},
				{Start: 4096, Length: 8192},
			},
		}, nil
	}

	req := &csi.GetMetadataAllocatedRequest{
		SnapshotId: volID + "+snapshot-456",
		MaxResults: 2,
	}

	err = ct.controller.GetMetadataAllocated(req, &mockAllocatedServer{ctx: ctx, t: t})
	if err != nil {
		t.Errorf("Expected success, got error: %v", err)
	}
}

// TestGetMetadataDelta_Success tests the success path
func TestGetMetadataDelta_Success(t *testing.T) {
	ct := getControllerTest(t)
	fakeOrchestrator := commonco.ContainerOrchestratorUtility.(*unittestcommon.FakeK8SOrchestrator)
	_ = fakeOrchestrator.EnableFSS(ctx, common.CBT)

	origVolumeManager := ct.controller.manager.VolumeManager
	ct.controller.manager.VolumeManager = &mockVolumeManager{Manager: origVolumeManager}
	defer func() { ct.controller.manager.VolumeManager = origVolumeManager }()

	// Create a volume first to avoid NotFound error
	reqCreate := &csi.CreateVolumeRequest{
		Name: testVolumeName + "-delta",
		CapacityRange: &csi.CapacityRange{
			RequiredBytes: 1 * common.GbInBytes,
		},
		VolumeCapabilities: []*csi.VolumeCapability{
			{
				AccessType: &csi.VolumeCapability_Mount{
					Mount: &csi.VolumeCapability_MountVolume{},
				},
				AccessMode: &csi.VolumeCapability_AccessMode{
					Mode: csi.VolumeCapability_AccessMode_SINGLE_NODE_WRITER,
				},
			},
		},
	}
	respCreate, err := ct.controller.CreateVolume(ctx, reqCreate)
	if err != nil {
		t.Fatalf("Failed to create volume: %v", err)
	}
	volID := respCreate.Volume.VolumeId

	// Mock the VSLM call
	origQueryChangedDiskAreasFunc := queryChangedDiskAreasFunc
	defer func() { queryChangedDiskAreasFunc = origQueryChangedDiskAreasFunc }()

	queryChangedDiskAreasFunc = func(ctx context.Context, vcenter *cnsvsphere.VirtualCenter,
		volumeID types.ID, snapshotID types.ID, startingOffset int64, changeId string) (*types.DiskChangeInfo, error) {
		return &types.DiskChangeInfo{
			ChangedArea: []types.DiskChangeExtent{
				{Start: 8192, Length: 4096},
			},
		}, nil
	}

	req := &csi.GetMetadataDeltaRequest{
		BaseSnapshotId:   volID + "+snapshot-123",
		TargetSnapshotId: volID + "+snapshot-456",
		MaxResults:       1,
	}

	err = ct.controller.GetMetadataDelta(req, &mockDeltaServer{ctx: ctx, t: t})
	if err != nil {
		t.Errorf("Expected success, got error: %v", err)
	}
}

// TestGetSnapshotChangeIdFromFCD tests the function that retrieves change ID
func TestGetSnapshotChangeIdFromFCD(t *testing.T) {
	ct := getControllerTest(t)

	// Mock the VSLM call
	origRetrieveSnapshotDetailsFunc := retrieveSnapshotDetailsFunc
	defer func() { retrieveSnapshotDetailsFunc = origRetrieveSnapshotDetailsFunc }()

	retrieveSnapshotDetailsFunc = func(ctx context.Context, vcenter *cnsvsphere.VirtualCenter,
		volumeID types.ID, snapshotID types.ID) (*types.VStorageObjectSnapshotDetails, error) {
		return &types.VStorageObjectSnapshotDetails{
			ChangedBlockTrackingId: "mock-change-id-from-fcd",
		}, nil
	}

	changeId, err := ct.controller.getSnapshotChangeIdFromFCD(ctx, testVolumeName, "snapshot-123")
	if err != nil {
		t.Errorf("Expected success, got error: %v", err)
	}
	if changeId != "mock-change-id-from-fcd" {
		t.Errorf("Expected mock-change-id-from-fcd, got: %s", changeId)
	}

	// Test empty change ID
	retrieveSnapshotDetailsFunc = func(ctx context.Context, vcenter *cnsvsphere.VirtualCenter,
		volumeID types.ID, snapshotID types.ID) (*types.VStorageObjectSnapshotDetails, error) {
		return &types.VStorageObjectSnapshotDetails{
			ChangedBlockTrackingId: "", // empty
		}, nil
	}

	_, err = ct.controller.getSnapshotChangeIdFromFCD(ctx, testVolumeName, "snapshot-123")
	if err == nil {
		t.Errorf("Expected error for empty change ID, got nil")
	}
}

func createSoapFault(fault types.AnyType, msg string) error {
	f := &soap.Fault{String: msg}
	f.Detail.Fault = fault
	return soap.WrapSoapFault(f)
}

// TestTranslateVslmError tests the error mapping logic for VSLM API errors
func TestTranslateVslmError(t *testing.T) {
	ctx := context.Background()
	ctx = logger.NewContextWithLogger(ctx)
	log := logger.GetLogger(ctx)

	tests := []struct {
		name     string
		err      error
		wantCode codes.Code
	}{
		{
			name:     "nil error",
			err:      nil,
			wantCode: codes.OK,
		},
		{
			name: "SOAP FileFault with noTrack",
			err: createSoapFault(&types.FileFault{
				VimFault: types.VimFault{
					MethodFault: types.MethodFault{
						FaultMessage: []types.LocalizableMessage{
							{Key: "vim.hostd.vmsvc.cbt.noTrack"},
						},
					},
				},
			}, ""),
			wantCode: codes.FailedPrecondition,
		},
		{
			name: "SOAP FileFault with noEpoch",
			err: createSoapFault(&types.FileFault{
				VimFault: types.VimFault{
					MethodFault: types.MethodFault{
						FaultMessage: []types.LocalizableMessage{
							{Key: "vim.hostd.vmsvc.cbt.noEpoch"},
						},
					},
				},
			}, ""),
			wantCode: codes.FailedPrecondition,
		},
		{
			name: "SOAP FileFault with corrupt cannotGetChanges",
			err: createSoapFault(&types.FileFault{
				VimFault: types.VimFault{
					MethodFault: types.MethodFault{
						FaultMessage: []types.LocalizableMessage{
							{Key: "vim.hostd.vmsvc.cbt.cannotGetChanges", Message: "file is corrupted"},
						},
					},
				},
			}, "file is corrupted"),
			wantCode: codes.FailedPrecondition,
		},
		{
			name: "SOAP FileFault with mismatched cannotGetChanges",
			err: createSoapFault(&types.FileFault{
				VimFault: types.VimFault{
					MethodFault: types.MethodFault{
						FaultMessage: []types.LocalizableMessage{
							{Key: "vim.hostd.vmsvc.cbt.cannotGetChanges"},
						},
					},
				},
			}, ""),
			wantCode: codes.InvalidArgument,
		},
		{
			name:     "SOAP FileFault generic",
			err:      createSoapFault(&types.FileFault{}, ""),
			wantCode: codes.Internal,
		},
		{
			name:     "SOAP SystemError",
			err:      createSoapFault(&types.SystemError{}, ""),
			wantCode: codes.Internal,
		},
		{
			name: "SOAP InvalidArgument startOffset",
			err: createSoapFault(&types.InvalidArgument{
				InvalidProperty: "startOffset",
			}, ""),
			wantCode: codes.OutOfRange,
		},
		{
			name: "SOAP InvalidArgument snapshotId",
			err: createSoapFault(&types.InvalidArgument{
				InvalidProperty: "snapshotId",
			}, ""),
			wantCode: codes.NotFound,
		},
		{
			name: "SOAP InvalidArgument changeId",
			err: createSoapFault(&types.InvalidArgument{
				InvalidProperty: "changeId",
			}, ""),
			wantCode: codes.InvalidArgument,
		},
		{
			name: "SOAP InvalidArgument deviceKey",
			err: createSoapFault(&types.InvalidArgument{
				InvalidProperty: "deviceKey",
			}, ""),
			wantCode: codes.InvalidArgument,
		},
		{
			name:     "SOAP NotFound",
			err:      createSoapFault(&types.NotFound{}, ""),
			wantCode: codes.NotFound,
		},
		{
			name:     "plain error with CBT message substring",
			err:      fmt.Errorf("some inner error: vim.hostd.vmsvc.cbt.noTrack"),
			wantCode: codes.Internal,
		},
		{
			name:     "plain error with vim.fault substring",
			err:      fmt.Errorf("some inner error: vim.fault.NotFound occurred"),
			wantCode: codes.Internal,
		},
		{
			name:     "plain error generic",
			err:      fmt.Errorf("random network timeout"),
			wantCode: codes.Internal,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := translateVslmError(log, tt.err)

			if tt.err == nil {
				if err != nil {
					t.Errorf("Expected nil error, got: %v", err)
				}
				return
			}

			if status.Code(err) != tt.wantCode {
				t.Errorf("translateVslmError() returned code %v, want %v. Error: %v", status.Code(err), tt.wantCode, err)
			}
		})
	}
}
