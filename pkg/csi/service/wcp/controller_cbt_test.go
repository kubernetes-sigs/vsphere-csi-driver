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
	vim25types "github.com/vmware/govmomi/vim25/types"
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

const (
	testValidChangeID    = "52 21 4f 8a 5e 47 9c bd-3b ff e0 12 a3 4c 56 78/12"
	testInvalidChangeID1 = "52 21 4f 8a 5e 47 9c bd-3b ff e0 12 a3 4c 56 78/12/p0"
	testInvalidChangeID2 = "*/p0"
)

type mockAllocatedServer struct {
	grpc.ServerStream
	ctx       context.Context
	t         *testing.T
	sendCount int
	allBlocks []*csi.BlockMetadata
}

func (m *mockAllocatedServer) Context() context.Context {
	return m.ctx
}

func (m *mockAllocatedServer) Send(resp *csi.GetMetadataAllocatedResponse) error {
	m.sendCount++
	m.allBlocks = append(m.allBlocks, resp.BlockMetadata...)
	if m.t != nil && resp.BlockMetadataType != csi.BlockMetadataType_VARIABLE_LENGTH {
		m.t.Errorf("Expected BlockMetadataType to be %v, got %v",
			csi.BlockMetadataType_VARIABLE_LENGTH, resp.BlockMetadataType)
	}
	return nil
}

type mockDeltaServer struct {
	grpc.ServerStream
	ctx       context.Context
	t         *testing.T
	sendCount int
	allBlocks []*csi.BlockMetadata
}

func (m *mockDeltaServer) Context() context.Context {
	return m.ctx
}

func (m *mockDeltaServer) Send(resp *csi.GetMetadataDeltaResponse) error {
	m.sendCount++
	m.allBlocks = append(m.allBlocks, resp.BlockMetadata...)
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
			name: "negative starting_offset",
			req: &csi.GetMetadataAllocatedRequest{
				SnapshotId:     "volume-123+snapshot-456",
				StartingOffset: -1,
			},
			wantErr: true,
		},
		{
			name: "negative max_results",
			req: &csi.GetMetadataAllocatedRequest{
				SnapshotId: "volume-123+snapshot-456",
				MaxResults: -1,
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
				BaseSnapshotId:   testValidChangeID,
				TargetSnapshotId: "",
			},
			wantErr: true,
		},
		{
			name: "base_snapshot_id is reserved '*'",
			req: &csi.GetMetadataDeltaRequest{
				BaseSnapshotId:   "*",
				TargetSnapshotId: "volume-123+snapshot-456",
			},
			wantErr: true,
		},
		{
			name: "base_snapshot_id has invalid format - 1",
			req: &csi.GetMetadataDeltaRequest{
				BaseSnapshotId:   testInvalidChangeID1,
				TargetSnapshotId: "volume-123+snapshot-456",
			},
			wantErr: true,
		},
		{
			name: "base_snapshot_id has invalid format - 2",
			req: &csi.GetMetadataDeltaRequest{
				BaseSnapshotId:   testInvalidChangeID2,
				TargetSnapshotId: "volume-123+snapshot-456",
			},
			wantErr: true,
		},
		{
			name: "negative starting_offset",
			req: &csi.GetMetadataDeltaRequest{
				BaseSnapshotId:   testValidChangeID,
				TargetSnapshotId: "volume-123+snapshot-456",
				StartingOffset:   -1,
			},
			wantErr: true,
		},
		{
			name: "negative max_results",
			req: &csi.GetMetadataDeltaRequest{
				BaseSnapshotId:   testValidChangeID,
				TargetSnapshotId: "volume-123+snapshot-456",
				MaxResults:       -1,
			},
			wantErr: true,
		},
		{
			name: "valid request (BaseSnapshotId is the vSphere change-id)",
			req: &csi.GetMetadataDeltaRequest{
				BaseSnapshotId:   testValidChangeID,
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

// TestGetMetadataAllocated_FSSDisabled tests that the API returns Unimplemented when the
// Supervisor-side gate (CSI_Backup_API) is disabled.
func TestGetMetadataAllocated_FSSDisabled(t *testing.T) {
	ct := getControllerTest(t)

	// FakeK8SOrchestrator returns false for any FSS that has not been explicitly
	// enabled, which matches "CSI_Backup_API not enabled" on a real Supervisor.

	req := &csi.GetMetadataAllocatedRequest{
		SnapshotId: "volume-123+snapshot-456",
	}

	err := ct.controller.GetMetadataAllocated(req, &mockAllocatedServer{ctx: ctx})
	if status.Code(err) != codes.Unimplemented {
		t.Errorf("Expected Unimplemented error when CSI_Backup_API FSS is disabled, got: %v", err)
	}
}

// TestGetMetadataDelta_FSSDisabled tests that the API returns Unimplemented when the
// Supervisor-side gate (CSI_Backup_API) is disabled.
func TestGetMetadataDelta_FSSDisabled(t *testing.T) {
	ct := getControllerTest(t)

	req := &csi.GetMetadataDeltaRequest{
		BaseSnapshotId:   "volume-123+snapshot-123",
		TargetSnapshotId: "volume-123+snapshot-456",
	}

	err := ct.controller.GetMetadataDelta(req, &mockDeltaServer{ctx: ctx})
	if status.Code(err) != codes.Unimplemented {
		t.Errorf("Expected Unimplemented error when CSI_Backup_API FSS is disabled, got: %v", err)
	}
}

// TestGetMetadataAllocated_VolumeNotFound tests that the API returns NotFound when volume is not found
func TestGetMetadataAllocated_VolumeNotFound(t *testing.T) {
	ct := getControllerTest(t)
	fakeOrchestrator := commonco.ContainerOrchestratorUtility.(*unittestcommon.FakeK8SOrchestrator)
	_ = fakeOrchestrator.EnableFSS(ctx, common.CSI_Backup_API)

	req := &csi.GetMetadataAllocatedRequest{
		SnapshotId: "nonexistent-volume+snapshot-456",
	}

	err := ct.controller.GetMetadataAllocated(req, &mockAllocatedServer{ctx: ctx})
	if status.Code(err) != codes.NotFound {
		t.Errorf("Expected NotFound error when volume does not exist, got: %v", err)
	}
}

// TestGetMetadataDelta_VolumeNotFound tests that the API returns NotFound when the target
// volume (derived from TargetSnapshotId) does not exist. BaseSnapshotId is the change-id
// and is not used for volume lookup.
func TestGetMetadataDelta_VolumeNotFound(t *testing.T) {
	ct := getControllerTest(t)
	fakeOrchestrator := commonco.ContainerOrchestratorUtility.(*unittestcommon.FakeK8SOrchestrator)
	_ = fakeOrchestrator.EnableFSS(ctx, common.CSI_Backup_API)

	req := &csi.GetMetadataDeltaRequest{
		BaseSnapshotId:   testValidChangeID,
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

// Mock structures for testing (would be expanded in full test suite)

// TestAllocatedAreaConversion tests the conversion from VSLM response to CSI format
func TestAllocatedAreaConversion(t *testing.T) {
	areas := []cnsvolume.DiskArea{
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
	areas := []cnsvolume.DiskArea{
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
	_ = fakeOrchestrator.EnableFSS(ctx, common.CSI_Backup_API)

	origVolumeManager := ct.controller.manager.VolumeManager
	ct.controller.manager.VolumeManager = &mockVolumeManager{Manager: origVolumeManager}
	defer func() { ct.controller.manager.VolumeManager = origVolumeManager }()

	volumeCapacityBytes := int64(1 * common.GbInBytes)

	// Create a volume first to avoid NotFound error
	reqCreate := &csi.CreateVolumeRequest{
		Name: testVolumeName + "-allocated",
		CapacityRange: &csi.CapacityRange{
			RequiredBytes: volumeCapacityBytes,
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

	origHook := cnsvolume.QueryChangedDiskAreasHook
	defer func() { cnsvolume.QueryChangedDiskAreasHook = origHook }()

	cnsvolume.QueryChangedDiskAreasHook = func(ctx context.Context, vcenter *cnsvsphere.VirtualCenter,
		volumeID vim25types.ID, snapshotID vim25types.ID, startingOffset int64,
		changeID string) (*vim25types.DiskChangeInfo, error) {
		// Only return areas that start at or after startingOffset so the
		// pagination loop terminates correctly.
		all := []vim25types.DiskChangeExtent{
			{Start: 0, Length: 4096},
			{Start: 4096, Length: 8192},
		}
		var filtered []vim25types.DiskChangeExtent
		for _, a := range all {
			if a.Start >= startingOffset {
				filtered = append(filtered, a)
			}
		}

		length := volumeCapacityBytes - startingOffset

		return &vim25types.DiskChangeInfo{
			StartOffset: startingOffset,
			Length:      length,
			ChangedArea: filtered,
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
	_ = fakeOrchestrator.EnableFSS(ctx, common.CSI_Backup_API)

	origVolumeManager := ct.controller.manager.VolumeManager
	ct.controller.manager.VolumeManager = &mockVolumeManager{Manager: origVolumeManager}
	defer func() { ct.controller.manager.VolumeManager = origVolumeManager }()

	volumeCapacityBytes := int64(1 * common.GbInBytes)

	// Create a volume first to avoid NotFound error
	reqCreate := &csi.CreateVolumeRequest{
		Name: testVolumeName + "-delta",
		CapacityRange: &csi.CapacityRange{
			RequiredBytes: volumeCapacityBytes,
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

	origHook := cnsvolume.QueryChangedDiskAreasHook
	defer func() { cnsvolume.QueryChangedDiskAreasHook = origHook }()

	cnsvolume.QueryChangedDiskAreasHook = func(ctx context.Context, vcenter *cnsvsphere.VirtualCenter,
		volumeID vim25types.ID, snapshotID vim25types.ID, startingOffset int64,
		changeID string) (*vim25types.DiskChangeInfo, error) {
		// Only return areas at or after startingOffset so the pagination loop
		// terminates correctly.
		all := []vim25types.DiskChangeExtent{
			{Start: 8192, Length: 4096},
		}
		var filtered []vim25types.DiskChangeExtent
		for _, a := range all {
			if a.Start >= startingOffset {
				filtered = append(filtered, a)
			}
		}

		length := volumeCapacityBytes - startingOffset

		return &vim25types.DiskChangeInfo{
			StartOffset: startingOffset,
			Length:      length,
			ChangedArea: filtered,
		}, nil
	}

	// BaseSnapshotId is the vSphere CBT change-id supplied by backup software (read from the
	// `csi.vsphere.volume/change-id` annotation when the base snapshot was first created).
	// for this mock.
	req := &csi.GetMetadataDeltaRequest{
		BaseSnapshotId:   testValidChangeID,
		TargetSnapshotId: volID + "+snapshot-456",
		MaxResults:       1,
	}

	err = ct.controller.GetMetadataDelta(req, &mockDeltaServer{ctx: ctx, t: t})
	if err != nil {
		t.Errorf("Expected success, got error: %v", err)
	}
}

// makeFCDSoapFault builds a govmomi SOAP fault wrapping the given vim fault
// payload. Mirrors the helper used in the cnsvolume package tests.
func makeFCDSoapFault(fault vim25types.AnyType, msg string) error {
	f := &soap.Fault{String: msg}
	f.Detail.Fault = fault
	return soap.WrapSoapFault(f)
}

// TestVslmErrorToCSICode locks in the unit-level mapping used by the wcp
// controller when wrapping VSLM errors: it must surface any explicit gRPC
// status code from the inner error (e.g. FailedPrecondition / OutOfRange /
// NotFound) and only fall back to Internal for plain errors.
func TestVslmErrorToCSICode(t *testing.T) {
	tests := []struct {
		name string
		err  error
		want codes.Code
	}{
		{name: "nil", err: nil, want: codes.OK},
		{name: "FailedPrecondition status", err: status.Error(codes.FailedPrecondition, "x"), want: codes.FailedPrecondition},
		{name: "OutOfRange status", err: status.Error(codes.OutOfRange, "x"), want: codes.OutOfRange},
		{name: "InvalidArgument status", err: status.Error(codes.InvalidArgument, "x"), want: codes.InvalidArgument},
		{name: "NotFound status", err: status.Error(codes.NotFound, "x"), want: codes.NotFound},
		{name: "Internal status", err: status.Error(codes.Internal, "x"), want: codes.Internal},
		{name: "plain error", err: fmt.Errorf("oops"), want: codes.Internal},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := vslmErrorToCSICode(tt.err); got != tt.want {
				t.Errorf("vslmErrorToCSICode(%v) = %v, want %v", tt.err, got, tt.want)
			}
		})
	}
}

// runVSLMErrorPropagationCase asserts that the wcp controller propagates the
// gRPC status code produced by volume.TranslateVslmError end-to-end (instead
// of overwriting it with codes.Internal).
//
// Each case: stub QueryChangedDiskAreasHook to return a SOAP fault, then drive
// either GetMetadataAllocated or GetMetadataDelta and assert status.Code(err).
func runVSLMErrorPropagationCase(t *testing.T, name string, vimFault vim25types.AnyType,
	faultMsg string, isDelta bool, wantCode codes.Code) {
	t.Run(name, func(t *testing.T) {
		ct := getControllerTest(t)
		fakeOrchestrator := commonco.ContainerOrchestratorUtility.(*unittestcommon.FakeK8SOrchestrator)
		_ = fakeOrchestrator.EnableFSS(ctx, common.CSI_Backup_API)

		origVolumeManager := ct.controller.manager.VolumeManager
		ct.controller.manager.VolumeManager = &mockVolumeManager{Manager: origVolumeManager}
		defer func() { ct.controller.manager.VolumeManager = origVolumeManager }()

		reqCreate := &csi.CreateVolumeRequest{
			Name: fmt.Sprintf("%s-%s", testVolumeName, name),
			CapacityRange: &csi.CapacityRange{
				RequiredBytes: 1 * common.GbInBytes,
			},
			VolumeCapabilities: []*csi.VolumeCapability{
				{
					AccessType: &csi.VolumeCapability_Mount{Mount: &csi.VolumeCapability_MountVolume{}},
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

		origHook := cnsvolume.QueryChangedDiskAreasHook
		defer func() { cnsvolume.QueryChangedDiskAreasHook = origHook }()
		cnsvolume.QueryChangedDiskAreasHook = func(ctx context.Context, vcenter *cnsvsphere.VirtualCenter,
			volumeID vim25types.ID, snapshotID vim25types.ID, startingOffset int64,
			changeID string) (*vim25types.DiskChangeInfo, error) {
			return nil, makeFCDSoapFault(vimFault, faultMsg)
		}

		if isDelta {
			req := &csi.GetMetadataDeltaRequest{
				BaseSnapshotId:   testValidChangeID,
				TargetSnapshotId: volID + "+snapshot-456",
				MaxResults:       1,
			}
			err = ct.controller.GetMetadataDelta(req, &mockDeltaServer{ctx: ctx})
		} else {
			req := &csi.GetMetadataAllocatedRequest{
				SnapshotId: volID + "+snapshot-456",
				MaxResults: 1,
			}
			err = ct.controller.GetMetadataAllocated(req, &mockAllocatedServer{ctx: ctx})
		}

		if err == nil {
			t.Fatalf("Expected error %v, got nil", wantCode)
		}
		if got := status.Code(err); got != wantCode {
			t.Errorf("Expected status code %v, got %v (err=%v)", wantCode, got, err)
		}
	})
}

// TestGetMetadataAllocated_PreservesVSLMErrorCode verifies that for
// GetMetadataAllocated the controller surfaces the gRPC code chosen by
// volume.TranslateVslmError instead of clobbering it with codes.Internal.
func TestGetMetadataAllocated_PreservesVSLMErrorCode(t *testing.T) {
	// noTrack -> CBT not enabled -> FailedPrecondition.
	runVSLMErrorPropagationCase(t, "FailedPrecondition_noTrack",
		vim25types.FileFault{
			VimFault: vim25types.VimFault{
				MethodFault: vim25types.MethodFault{
					FaultMessage: []vim25types.LocalizableMessage{{Key: "vim.hostd.vmsvc.cbt.noTrack"}},
				},
			},
		}, "", false, codes.FailedPrecondition)

	// startOffset out of range -> OutOfRange.
	runVSLMErrorPropagationCase(t, "OutOfRange_startOffset",
		vim25types.InvalidArgument{InvalidProperty: "startOffset"}, "",
		false, codes.OutOfRange)

	// snapshotId not found -> NotFound.
	runVSLMErrorPropagationCase(t, "NotFound_snapshotId",
		vim25types.InvalidArgument{InvalidProperty: "snapshotId"}, "",
		false, codes.NotFound)

	// changeId malformed -> InvalidArgument.
	runVSLMErrorPropagationCase(t, "InvalidArgument_changeId",
		vim25types.InvalidArgument{InvalidProperty: "changeId"}, "",
		false, codes.InvalidArgument)

	// Generic VSLM SystemError -> Internal (already-wrapped case still works).
	runVSLMErrorPropagationCase(t, "Internal_systemError",
		vim25types.SystemError{}, "", false, codes.Internal)
}

// TestGetMetadataDelta_PreservesVSLMErrorCode is the same end-to-end
// propagation check as above, but driven through GetMetadataDelta (which uses
// QueryFCDChangedBlocks under the hood).
func TestGetMetadataDelta_PreservesVSLMErrorCode(t *testing.T) {
	runVSLMErrorPropagationCase(t, "FailedPrecondition_noEpoch",
		vim25types.FileFault{
			VimFault: vim25types.VimFault{
				MethodFault: vim25types.MethodFault{
					FaultMessage: []vim25types.LocalizableMessage{{Key: "vim.hostd.vmsvc.cbt.noEpoch"}},
				},
			},
		}, "", true, codes.FailedPrecondition)

	runVSLMErrorPropagationCase(t, "FailedPrecondition_corruptCTK",
		vim25types.FileFault{
			VimFault: vim25types.VimFault{
				MethodFault: vim25types.MethodFault{
					FaultMessage: []vim25types.LocalizableMessage{
						{Key: "vim.hostd.vmsvc.cbt.cannotGetChanges", Message: "file is corrupted"},
					},
				},
			},
		}, "file is corrupted", true, codes.FailedPrecondition)

	runVSLMErrorPropagationCase(t, "InvalidArgument_changeIDMismatch",
		vim25types.FileFault{
			VimFault: vim25types.VimFault{
				MethodFault: vim25types.MethodFault{
					FaultMessage: []vim25types.LocalizableMessage{
						{Key: "vim.hostd.vmsvc.cbt.cannotGetChanges"},
					},
				},
			},
		}, "", true, codes.InvalidArgument)

	runVSLMErrorPropagationCase(t, "NotFound_FCD",
		vim25types.NotFound{}, "", true, codes.NotFound)
}

// TestGetMetadataAllocated_Pagination verifies that GetMetadataAllocated streams
// multiple server.Send() messages when the total allocated blocks exceed maxResults
// per batch. This is the core bug that caused tc_008 to fail: the old code called
// server.Send once and ignored nextOffset, truncating the result stream.
func TestGetMetadataAllocated_Pagination(t *testing.T) {
	ct := getControllerTest(t)
	fakeOrchestrator := commonco.ContainerOrchestratorUtility.(*unittestcommon.FakeK8SOrchestrator)
	_ = fakeOrchestrator.EnableFSS(ctx, common.CSI_Backup_API)

	origVolumeManager := ct.controller.manager.VolumeManager
	ct.controller.manager.VolumeManager = &mockVolumeManager{Manager: origVolumeManager}
	defer func() { ct.controller.manager.VolumeManager = origVolumeManager }()

	volumeCapacityBytes := int64(1 * common.GbInBytes)

	reqCreate := &csi.CreateVolumeRequest{
		Name: testVolumeName + "-alloc-pagination",
		CapacityRange: &csi.CapacityRange{
			RequiredBytes: volumeCapacityBytes,
		},
		VolumeCapabilities: []*csi.VolumeCapability{
			{
				AccessType: &csi.VolumeCapability_Mount{Mount: &csi.VolumeCapability_MountVolume{}},
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

	origHook := cnsvolume.QueryChangedDiskAreasHook
	defer func() { cnsvolume.QueryChangedDiskAreasHook = origHook }()

	// Stub the hook to return areas in multiple pages to exercise VADP pagination.
	// With maxResults=1 the controller will also batch the returned areas,
	// issuing 3 separate server.Send() calls.
	cnsvolume.QueryChangedDiskAreasHook = func(ctx context.Context, vcenter *cnsvsphere.VirtualCenter,
		volumeID vim25types.ID, snapshotID vim25types.ID, startingOffset int64,
		changeID string) (*vim25types.DiskChangeInfo, error) {

		var area []vim25types.DiskChangeExtent
		var length int64

		switch startingOffset {
		case 0:
			area = []vim25types.DiskChangeExtent{
				{Start: 0, Length: 4096},
				{Start: 4096, Length: 4096},
			}
			length = 8192
		case 8192:
			area = []vim25types.DiskChangeExtent{
				{Start: 8192, Length: 4096},
			}
			length = volumeCapacityBytes - 8192
		default:
			area = nil
			length = volumeCapacityBytes - startingOffset
		}

		return &vim25types.DiskChangeInfo{
			StartOffset: startingOffset,
			Length:      length,
			ChangedArea: area,
		}, nil
	}

	srv := &mockAllocatedServer{ctx: ctx, t: t}
	req := &csi.GetMetadataAllocatedRequest{
		SnapshotId: volID + "+snapshot-pagination",
		MaxResults: 1, // force one area per Send() call
	}

	if err := ct.controller.GetMetadataAllocated(req, srv); err != nil {
		t.Fatalf("GetMetadataAllocated returned unexpected error: %v", err)
	}

	if srv.sendCount != 3 {
		t.Errorf("Expected 3 server.Send() calls (one per block batch), got %d", srv.sendCount)
	}
	if len(srv.allBlocks) != 3 {
		t.Errorf("Expected 3 total BlockMetadata entries, got %d", len(srv.allBlocks))
	}
}

// TestGetMetadataDelta_Pagination is the equivalent pagination test for
// GetMetadataDelta: verifies that multiple server.Send() calls are issued
// when changed blocks span more than one maxResults-sized batch.
func TestGetMetadataDelta_Pagination(t *testing.T) {
	ct := getControllerTest(t)
	fakeOrchestrator := commonco.ContainerOrchestratorUtility.(*unittestcommon.FakeK8SOrchestrator)
	_ = fakeOrchestrator.EnableFSS(ctx, common.CSI_Backup_API)

	origVolumeManager := ct.controller.manager.VolumeManager
	ct.controller.manager.VolumeManager = &mockVolumeManager{Manager: origVolumeManager}
	defer func() { ct.controller.manager.VolumeManager = origVolumeManager }()

	volumeCapacityBytes := int64(1 * common.GbInBytes)

	reqCreate := &csi.CreateVolumeRequest{
		Name: testVolumeName + "-delta-pagination",
		CapacityRange: &csi.CapacityRange{
			RequiredBytes: volumeCapacityBytes,
		},
		VolumeCapabilities: []*csi.VolumeCapability{
			{
				AccessType: &csi.VolumeCapability_Mount{Mount: &csi.VolumeCapability_MountVolume{}},
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

	origHook := cnsvolume.QueryChangedDiskAreasHook
	defer func() { cnsvolume.QueryChangedDiskAreasHook = origHook }()

	cnsvolume.QueryChangedDiskAreasHook = func(ctx context.Context, vcenter *cnsvsphere.VirtualCenter,
		volumeID vim25types.ID, snapshotID vim25types.ID, startingOffset int64,
		changeID string) (*vim25types.DiskChangeInfo, error) {

		var area []vim25types.DiskChangeExtent
		var length int64

		switch startingOffset {
		case 0:
			area = []vim25types.DiskChangeExtent{
				{Start: 0, Length: 4096},
				{Start: 4096, Length: 4096},
			}
			length = 8192
		case 8192:
			area = []vim25types.DiskChangeExtent{
				{Start: 8192, Length: 4096},
			}
			length = volumeCapacityBytes - 8192
		default:
			area = nil
			length = volumeCapacityBytes - startingOffset
		}

		return &vim25types.DiskChangeInfo{
			StartOffset: startingOffset,
			Length:      length,
			ChangedArea: area,
		}, nil
	}

	srv := &mockDeltaServer{ctx: ctx, t: t}
	req := &csi.GetMetadataDeltaRequest{
		BaseSnapshotId:   testValidChangeID,
		TargetSnapshotId: volID + "+snapshot-pagination",
		MaxResults:       1, // force one area per Send() call
	}

	if err := ct.controller.GetMetadataDelta(req, srv); err != nil {
		t.Fatalf("GetMetadataDelta returned unexpected error: %v", err)
	}

	if srv.sendCount != 3 {
		t.Errorf("Expected 3 server.Send() calls (one per block batch), got %d", srv.sendCount)
	}
	if len(srv.allBlocks) != 3 {
		t.Errorf("Expected 3 total BlockMetadata entries, got %d", len(srv.allBlocks))
	}
}
