/*
Copyright 2025 The Kubernetes Authors.

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

package service

import (
	"context"
	"os"
	"strings"
	"testing"

	"github.com/container-storage-interface/spec/lib/go/csi"
	"github.com/stretchr/testify/assert"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestNodeStageVolume_FileVolume(t *testing.T) {
	ctx := context.Background()
	driver := NewDriver().(*vsphereCSIDriver)

	// Create a file volume capability
	volCap := &csi.VolumeCapability{
		AccessType: &csi.VolumeCapability_Mount{
			Mount: &csi.VolumeCapability_MountVolume{
				FsType: "nfs",
			},
		},
		AccessMode: &csi.VolumeCapability_AccessMode{
			Mode: csi.VolumeCapability_AccessMode_MULTI_NODE_MULTI_WRITER,
		},
	}

	req := &csi.NodeStageVolumeRequest{
		VolumeId:          "test-file-volume-id",
		VolumeCapability:  volCap,
		StagingTargetPath: "/tmp/staging",
	}

	resp, err := driver.NodeStageVolume(ctx, req)

	// File volumes should be ignored during staging
	assert.NoError(t, err)
	assert.NotNil(t, resp)
}

func TestNodeStageVolume_InvalidArguments(t *testing.T) {
	ctx := context.Background()
	driver := NewDriver().(*vsphereCSIDriver)

	tests := []struct {
		name        string
		req         *csi.NodeStageVolumeRequest
		expectedErr codes.Code
	}{
		{
			name: "Missing volume capability",
			req: &csi.NodeStageVolumeRequest{
				VolumeId:          "test-volume-id",
				StagingTargetPath: "/tmp/staging",
			},
			expectedErr: codes.InvalidArgument,
		},
		{
			name: "Empty volume ID",
			req: &csi.NodeStageVolumeRequest{
				VolumeId: "",
				VolumeCapability: &csi.VolumeCapability{
					AccessType: &csi.VolumeCapability_Block{
						Block: &csi.VolumeCapability_BlockVolume{},
					},
					AccessMode: &csi.VolumeCapability_AccessMode{
						Mode: csi.VolumeCapability_AccessMode_SINGLE_NODE_WRITER,
					},
				},
				StagingTargetPath: "/tmp/staging",
			},
			expectedErr: codes.InvalidArgument,
		},
		{
			name: "Empty staging target path",
			req: &csi.NodeStageVolumeRequest{
				VolumeId: "test-volume-id",
				VolumeCapability: &csi.VolumeCapability{
					AccessType: &csi.VolumeCapability_Block{
						Block: &csi.VolumeCapability_BlockVolume{},
					},
					AccessMode: &csi.VolumeCapability_AccessMode{
						Mode: csi.VolumeCapability_AccessMode_SINGLE_NODE_WRITER,
					},
				},
				StagingTargetPath: "",
			},
			expectedErr: codes.InvalidArgument,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			resp, err := driver.NodeStageVolume(ctx, tt.req)

			assert.Nil(t, resp)
			assert.Error(t, err)

			st, ok := status.FromError(err)
			assert.True(t, ok)
			assert.Equal(t, tt.expectedErr, st.Code())
		})
	}
}

func TestNodeUnstageVolume_FileVolume(t *testing.T) {
	ctx := context.Background()
	driver := NewDriver().(*vsphereCSIDriver)

	req := &csi.NodeUnstageVolumeRequest{
		VolumeId:          "test-file-volume-id",
		StagingTargetPath: "/tmp/staging",
	}

	resp, err := driver.NodeUnstageVolume(ctx, req)

	// Should succeed for any volume ID
	assert.NoError(t, err)
	assert.NotNil(t, resp)
}

func TestNodeUnstageVolume_InvalidArguments(t *testing.T) {
	ctx := context.Background()
	driver := NewDriver().(*vsphereCSIDriver)

	tests := []struct {
		name        string
		req         *csi.NodeUnstageVolumeRequest
		expectedErr codes.Code
	}{
		{
			name: "Empty volume ID",
			req: &csi.NodeUnstageVolumeRequest{
				VolumeId:          "",
				StagingTargetPath: "/tmp/staging",
			},
			expectedErr: codes.InvalidArgument,
		},
		{
			name: "Empty staging target path",
			req: &csi.NodeUnstageVolumeRequest{
				VolumeId:          "test-volume-id",
				StagingTargetPath: "",
			},
			expectedErr: codes.InvalidArgument,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			resp, err := driver.NodeUnstageVolume(ctx, tt.req)

			assert.Nil(t, resp)
			assert.Error(t, err)

			st, ok := status.FromError(err)
			assert.True(t, ok)
			assert.Equal(t, tt.expectedErr, st.Code())
		})
	}
}

func TestNodePublishVolume_InvalidArguments(t *testing.T) {
	ctx := context.Background()
	driver := NewDriver().(*vsphereCSIDriver)

	tests := []struct {
		name        string
		req         *csi.NodePublishVolumeRequest
		expectedErr codes.Code
	}{
		{
			name: "Empty volume ID",
			req: &csi.NodePublishVolumeRequest{
				VolumeId:   "",
				TargetPath: "/tmp/target",
			},
			expectedErr: codes.InvalidArgument,
		},
		{
			name: "Empty target path",
			req: &csi.NodePublishVolumeRequest{
				VolumeId:   "test-volume-id",
				TargetPath: "",
			},
			expectedErr: codes.FailedPrecondition,
		},
		{
			name: "Missing volume capability",
			req: &csi.NodePublishVolumeRequest{
				VolumeId:   "test-volume-id",
				TargetPath: "/tmp/target",
			},
			expectedErr: codes.FailedPrecondition,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			resp, err := driver.NodePublishVolume(ctx, tt.req)

			assert.Nil(t, resp)
			assert.Error(t, err)

			st, ok := status.FromError(err)
			assert.True(t, ok)
			assert.Equal(t, tt.expectedErr, st.Code())
		})
	}
}

func TestNodeUnpublishVolume_InvalidArguments(t *testing.T) {
	ctx := context.Background()
	driver := NewDriver().(*vsphereCSIDriver)

	tests := []struct {
		name        string
		req         *csi.NodeUnpublishVolumeRequest
		expectedErr codes.Code
	}{
		{
			name: "Empty volume ID",
			req: &csi.NodeUnpublishVolumeRequest{
				VolumeId:   "",
				TargetPath: "/tmp/target",
			},
			expectedErr: codes.InvalidArgument,
		},
		{
			name: "Empty target path",
			req: &csi.NodeUnpublishVolumeRequest{
				VolumeId:   "test-volume-id",
				TargetPath: "",
			},
			expectedErr: codes.FailedPrecondition,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			resp, err := driver.NodeUnpublishVolume(ctx, tt.req)

			assert.Nil(t, resp)
			assert.Error(t, err)

			st, ok := status.FromError(err)
			assert.True(t, ok)
			assert.Equal(t, tt.expectedErr, st.Code())
		})
	}
}

func TestNodeGetVolumeStats_InvalidArguments(t *testing.T) {
	ctx := context.Background()
	driver := NewDriver().(*vsphereCSIDriver)

	tests := []struct {
		name        string
		req         *csi.NodeGetVolumeStatsRequest
		expectedErr codes.Code
	}{
		{
			name: "Empty volume ID",
			req: &csi.NodeGetVolumeStatsRequest{
				VolumeId:   "",
				VolumePath: "/tmp/volume",
			},
			expectedErr: codes.InvalidArgument,
		},
		{
			name: "Empty volume path",
			req: &csi.NodeGetVolumeStatsRequest{
				VolumeId:   "test-volume-id",
				VolumePath: "",
			},
			expectedErr: codes.InvalidArgument,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			resp, err := driver.NodeGetVolumeStats(ctx, tt.req)

			assert.Nil(t, resp)
			assert.Error(t, err)

			st, ok := status.FromError(err)
			assert.True(t, ok)
			assert.Equal(t, tt.expectedErr, st.Code())
		})
	}
}

func TestNodeExpandVolume_InvalidArguments(t *testing.T) {
	ctx := context.Background()
	driver := NewDriver().(*vsphereCSIDriver)

	tests := []struct {
		name        string
		req         *csi.NodeExpandVolumeRequest
		expectedErr codes.Code
	}{
		{
			name: "Empty volume ID",
			req: &csi.NodeExpandVolumeRequest{
				VolumeId:   "",
				VolumePath: "/tmp/volume",
			},
			expectedErr: codes.InvalidArgument,
		},
		{
			name: "Empty volume path",
			req: &csi.NodeExpandVolumeRequest{
				VolumeId:   "test-volume-id",
				VolumePath: "",
			},
			expectedErr: codes.InvalidArgument,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			resp, err := driver.NodeExpandVolume(ctx, tt.req)

			assert.Nil(t, resp)
			assert.Error(t, err)

			st, ok := status.FromError(err)
			assert.True(t, ok)
			assert.Equal(t, tt.expectedErr, st.Code())
		})
	}
}

func TestNodeGetCapabilities(t *testing.T) {
	ctx := context.Background()
	driver := NewDriver().(*vsphereCSIDriver)

	req := &csi.NodeGetCapabilitiesRequest{}
	resp, err := driver.NodeGetCapabilities(ctx, req)

	assert.NoError(t, err)
	assert.NotNil(t, resp)
	assert.NotNil(t, resp.Capabilities)

	// Verify expected capabilities are present
	expectedCaps := []csi.NodeServiceCapability_RPC_Type{
		csi.NodeServiceCapability_RPC_STAGE_UNSTAGE_VOLUME,
		csi.NodeServiceCapability_RPC_GET_VOLUME_STATS,
		csi.NodeServiceCapability_RPC_EXPAND_VOLUME,
	}

	actualCaps := make([]csi.NodeServiceCapability_RPC_Type, 0)
	for _, cap := range resp.Capabilities {
		if rpc := cap.GetRpc(); rpc != nil {
			actualCaps = append(actualCaps, rpc.GetType())
		}
	}

	for _, expectedCap := range expectedCaps {
		assert.Contains(t, actualCaps, expectedCap, "Expected capability %v not found", expectedCap)
	}
}

func TestNodeGetInfo(t *testing.T) {
	ctx := context.Background()
	driver := NewDriver().(*vsphereCSIDriver)

	// Set up required environment variables
	os.Setenv("NODE_NAME", "test-node")
	defer os.Unsetenv("NODE_NAME")

	req := &csi.NodeGetInfoRequest{}
	_, err := driver.NodeGetInfo(ctx, req)

	// In test environment, NodeGetInfo will fail because it can't access system UUID
	// This is expected behavior as the system files don't exist in test environment
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "failed to get system uuid for node VM")
}

func TestNodeConstants(t *testing.T) {
	// Test that constants are properly defined
	assert.Equal(t, 255, maxAllowedBlockVolumesPerNodeInvSphere8)
}

func TestVolumeLockingMechanism(t *testing.T) {
	driver := NewDriver().(*vsphereCSIDriver)

	volumeID := "test-volume-id"

	// Test acquiring lock
	acquired := driver.volumeLocks.TryAcquire(volumeID)
	assert.True(t, acquired, "Should be able to acquire lock for new volume")

	// Test acquiring same lock again (should fail)
	acquired = driver.volumeLocks.TryAcquire(volumeID)
	assert.False(t, acquired, "Should not be able to acquire lock for same volume twice")

	// Test releasing lock
	driver.volumeLocks.Release(volumeID)

	// Test acquiring lock after release (should succeed)
	acquired = driver.volumeLocks.TryAcquire(volumeID)
	assert.True(t, acquired, "Should be able to acquire lock after release")

	// Clean up
	driver.volumeLocks.Release(volumeID)
}

func TestOsUtilsInitialization(t *testing.T) {
	ctx := context.Background()
	driver := NewDriver().(*vsphereCSIDriver)

	// Mock CO init params
	COInitParams = map[string]interface{}{
		"test": "value",
	}

	// Set mode to node to avoid config requirements
	os.Setenv("CSI_MODE", "node")
	defer os.Unsetenv("CSI_MODE")

	err := driver.BeforeServe(ctx)
	// In node mode without Kubernetes cluster, BeforeServe will fail during CO initialization
	// This is expected behavior in a test environment
	assert.Error(t, err)
	// The error can be either missing env vars or missing service account token file
	assert.True(t,
		strings.Contains(err.Error(), "KUBERNETES_SERVICE_HOST and KUBERNETES_SERVICE_PORT must be defined") ||
			strings.Contains(err.Error(), "open /var/run/secrets/kubernetes.io/serviceaccount/token: no such file or directory"),
		"Expected Kubernetes connection error, got: %v", err)
	// OsUtils is not initialized when CO initialization fails because it happens after CO init
	assert.Nil(t, driver.osUtils, "OsUtils should not be initialized when CO initialization fails")
}
