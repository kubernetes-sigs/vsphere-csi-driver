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

package vanilla

import (
	"context"
	"testing"

	"github.com/container-storage-interface/spec/lib/go/csi"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestValidateVanillaControllerExpandVolumeRequest_RejectsFileVolume(t *testing.T) {
	ctx := context.Background()
	req := &csi.ControllerExpandVolumeRequest{
		VolumeId: "test-file-volume-id",
		CapacityRange: &csi.CapacityRange{
			RequiredBytes: 3221225472,
		},
		VolumeCapability: &csi.VolumeCapability{
			AccessMode: &csi.VolumeCapability_AccessMode{
				Mode: csi.VolumeCapability_AccessMode_MULTI_NODE_MULTI_WRITER,
			},
			AccessType: &csi.VolumeCapability_Mount{
				Mount: &csi.VolumeCapability_MountVolume{
					FsType: "nfs4",
				},
			},
		},
	}

	err := validateVanillaControllerExpandVolumeRequest(ctx, req, false, false)
	if err == nil {
		t.Fatalf("expected expansion of a file volume to be rejected, got nil error")
	}
	st, ok := status.FromError(err)
	if !ok {
		t.Fatalf("expected a grpc status error, got %v", err)
	}
	if st.Code() != codes.Unimplemented {
		t.Fatalf("expected code %v, got %v (%v)", codes.Unimplemented, st.Code(), st.Message())
	}
}

func TestValidateVanillaControllerExpandVolumeRequest_AllowsBlockVolume(t *testing.T) {
	ctx := context.Background()
	req := &csi.ControllerExpandVolumeRequest{
		VolumeId: "test-block-volume-id",
		CapacityRange: &csi.CapacityRange{
			RequiredBytes: 3221225472,
		},
		VolumeCapability: &csi.VolumeCapability{
			AccessMode: &csi.VolumeCapability_AccessMode{
				Mode: csi.VolumeCapability_AccessMode_SINGLE_NODE_WRITER,
			},
			AccessType: &csi.VolumeCapability_Mount{
				Mount: &csi.VolumeCapability_MountVolume{
					FsType: "ext4",
				},
			},
		},
	}

	// isOnlineExpansionEnabled=true, isOnlineExpansionSupported=true short-circuits the
	// node-attachment lookup so this exercises only the file-volume gate added by this test.
	err := validateVanillaControllerExpandVolumeRequest(ctx, req, true, true)
	if err != nil {
		t.Fatalf("expected block volume expansion to be allowed, got error: %v", err)
	}
}

func fileVolumeExpandRequest(volumeID string) *csi.ControllerExpandVolumeRequest {
	return &csi.ControllerExpandVolumeRequest{
		VolumeId:      volumeID,
		CapacityRange: &csi.CapacityRange{RequiredBytes: 3221225472},
		VolumeCapability: &csi.VolumeCapability{
			AccessMode: &csi.VolumeCapability_AccessMode{Mode: csi.VolumeCapability_AccessMode_MULTI_NODE_MULTI_WRITER},
			AccessType: &csi.VolumeCapability_Mount{Mount: &csi.VolumeCapability_MountVolume{FsType: "nfs4"}},
		},
	}
}

func TestIsVanillaFileVolume(t *testing.T) {
	ctx := context.Background()
	for volumeID, want := range map[string]bool{
		"file:1234-abcd": true,
		"1234-abcd":      false,
		"":               false,
	} {
		if got := isVanillaFileVolume(ctx, volumeID); got != want {
			t.Errorf("isVanillaFileVolume(%q) = %v, want %v", volumeID, got, want)
		}
	}
}

// TestValidateVanillaControllerExpandFileVolumeRequest verifies a file volume is expanded only for a
// legacy "file:" volume and only when the VsanFileVolumeExpansion FSS is enabled.
func TestValidateVanillaControllerExpandFileVolumeRequest(t *testing.T) {
	ctx := context.Background()

	for _, tt := range []struct {
		name     string
		volumeID string
		fssOn    bool
		want     codes.Code
	}{
		{"FSS off rejects legacy file volume", "file:1234-abcd", false, codes.Unimplemented},
		{"FSS on allows legacy file volume", "file:1234-abcd", true, codes.OK},
		{"FSS on rejects FVS volume id", "fv:ns:fv-name", true, codes.Unimplemented},
		{"FSS off rejects FVS volume id", "fv:ns:fv-name", false, codes.Unimplemented},
	} {
		t.Run(tt.name, func(t *testing.T) {
			err := validateVanillaControllerExpandFileVolumeRequest(ctx, fileVolumeExpandRequest(tt.volumeID), tt.fssOn)
			if got := status.Code(err); got != tt.want {
				t.Fatalf("code = %v, want %v (err: %v)", got, tt.want, err)
			}
		})
	}

	t.Run("required parameters are still validated", func(t *testing.T) {
		noID := fileVolumeExpandRequest("")
		noCap := fileVolumeExpandRequest("file:1234-abcd")
		noCap.VolumeCapability = nil
		noRange := fileVolumeExpandRequest("file:1234-abcd")
		noRange.CapacityRange = nil
		for name, req := range map[string]*csi.ControllerExpandVolumeRequest{
			"missing volume id":      noID,
			"missing capability":     noCap,
			"missing capacity range": noRange,
		} {
			err := validateVanillaControllerExpandFileVolumeRequest(ctx, req, true)
			if got := status.Code(err); got != codes.InvalidArgument {
				t.Errorf("%s: code = %v, want InvalidArgument", name, got)
			}
		}
	})
}

// TestIsNodeExpansionRequiredFileVolume verifies a file volume never asks kubelet for a node-side
// expansion, which would fail on an NFS mount (it looks up a block device from the mount).
func TestIsNodeExpansionRequiredFileVolume(t *testing.T) {
	mount := &csi.VolumeCapability{
		AccessType: &csi.VolumeCapability_Mount{Mount: &csi.VolumeCapability_MountVolume{FsType: "nfs4"}},
	}
	block := &csi.VolumeCapability{
		AccessType: &csi.VolumeCapability_Block{Block: &csi.VolumeCapability_BlockVolume{}},
	}
	if isNodeExpansionRequired(true, mount) {
		t.Error("file volume with mount capability must not require node expansion")
	}
	if isNodeExpansionRequired(true, block) {
		t.Error("file volume with block capability must not require node expansion")
	}
}
