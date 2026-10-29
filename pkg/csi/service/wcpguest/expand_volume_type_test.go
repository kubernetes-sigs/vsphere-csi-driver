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

package wcpguest

import (
	"context"
	"testing"

	"github.com/container-storage-interface/spec/lib/go/csi"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"sigs.k8s.io/vsphere-csi-driver/v3/pkg/csi/service/common"
)

// Tests in this file check that ControllerExpandVolume routes each volume type
// (FVS file, legacy vSAN file service, multi-node raw block) to the right path.

func expandReq(volumeID string, sizeBytes int64, mode csi.VolumeCapability_AccessMode_Mode,
	rawBlock bool) *csi.ControllerExpandVolumeRequest {
	volCap := &csi.VolumeCapability{
		AccessMode: &csi.VolumeCapability_AccessMode{Mode: mode},
		AccessType: &csi.VolumeCapability_Mount{Mount: &csi.VolumeCapability_MountVolume{}},
	}
	if rawBlock {
		volCap.AccessType = &csi.VolumeCapability_Block{Block: &csi.VolumeCapability_BlockVolume{}}
	}
	return &csi.ControllerExpandVolumeRequest{
		VolumeId:         volumeID,
		CapacityRange:    &csi.CapacityRange{RequiredBytes: sizeBytes},
		VolumeCapability: volCap,
	}
}

// TestControllerExpandVolume_FileVolumeModes verifies every multi-node file
// access mode (RWX, ROX, single-writer) on both legacy and FVS file volumes is
// expanded without a node-side step, with the FVS gate on and off.
func TestControllerExpandVolume_FileVolumeModes(t *testing.T) {
	for _, tc := range []struct {
		name string
		sc   string
		gate bool
	}{
		{"legacy, FVS gate off", "standard-sc", false},
		{"legacy, FVS gate on", "standard-sc", true},
		{"FVS, FVS gate on", common.StorageClassVsanFileServicePolicy, true},
		// With the gate off an FVS storage class PVC is treated as a regular file volume.
		{"FVS SC, FVS gate off", common.StorageClassVsanFileServicePolicy, false},
	} {
		for _, mode := range []csi.VolumeCapability_AccessMode_Mode{
			csi.VolumeCapability_AccessMode_MULTI_NODE_MULTI_WRITER,
			csi.VolumeCapability_AccessMode_MULTI_NODE_READER_ONLY,
			csi.VolumeCapability_AccessMode_MULTI_NODE_SINGLE_WRITER,
		} {
			t.Run(tc.name+"/"+mode.String(), func(t *testing.T) {
				setVsanFileVolumeServiceEnabledForTest(t, tc.gate)
				pvc := newPVCForExpand(fvsExpandPVC, tc.sc, "10Gi", "10Gi")
				c := fvsExpandController(t, pvc, pvc)

				resp, err := c.ControllerExpandVolume(context.Background(),
					expandReq(fvsExpandPVC, gi10, mode, false))
				if err != nil {
					t.Fatalf("unexpected error: %v", err)
				}
				if resp.NodeExpansionRequired {
					t.Error("NodeExpansionRequired must be false for file volumes")
				}
				if resp.CapacityBytes != gi10 {
					t.Errorf("CapacityBytes=%d want %d", resp.CapacityBytes, gi10)
				}
			})
		}
	}
}

// TestControllerExpandVolume_MultiNodeRawBlock_Rejected verifies multi-node raw
// block requests are not mistaken for file volumes and remain unsupported.
func TestControllerExpandVolume_MultiNodeRawBlock_Rejected(t *testing.T) {
	for _, gate := range []bool{false, true} {
		setVsanFileVolumeServiceEnabledForTest(t, gate)
		pvc := newPVCForExpand(fvsExpandPVC, "standard-sc", "10Gi", "10Gi")
		c := fvsExpandController(t, pvc, pvc)

		_, err := c.ControllerExpandVolume(context.Background(),
			expandReq(fvsExpandPVC, gi10, csi.VolumeCapability_AccessMode_MULTI_NODE_MULTI_WRITER, true))
		if status.Code(err) != codes.Unimplemented {
			t.Fatalf("gate=%v: expected Unimplemented, got %v", gate, err)
		}
	}
}
