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
