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

package service

import (
	"context"
	"testing"

	"github.com/container-storage-interface/spec/lib/go/csi"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"sigs.k8s.io/vsphere-csi-driver/v3/pkg/common/unittestcommon"
	"sigs.k8s.io/vsphere-csi-driver/v3/pkg/csi/service/common"
	"sigs.k8s.io/vsphere-csi-driver/v3/pkg/csi/service/common/commonco"
	"sigs.k8s.io/vsphere-csi-driver/v3/pkg/csi/service/wcpguest/nfsdriver"
)

const guestNFSTestVolumeID = nfsdriver.VolumeIDPrefix + "10.0.0.1#export#pvc-1##"

// useFakeOrchestrator installs a fake container orchestrator with the guest-nfs-volume FSS
// enabled or disabled for the duration of the test.
func useFakeOrchestrator(t *testing.T, enableGuestNFS bool) {
	t.Helper()
	co, err := unittestcommon.GetFakeContainerOrchestratorInterface(common.Kubernetes)
	require.NoError(t, err)
	if enableGuestNFS {
		require.NoError(t, co.EnableFSS(context.Background(), common.GuestNFSVolume))
	}
	old := commonco.ContainerOrchestratorUtility
	commonco.ContainerOrchestratorUtility = co
	t.Cleanup(func() { commonco.ContainerOrchestratorUtility = old })
}

func TestIsGuestNFSVolume(t *testing.T) {
	require.True(t, isGuestNFSVolume(guestNFSTestVolumeID))
	require.True(t, isGuestNFSVolume(nfsdriver.VolumeIDPrefix))
	require.False(t, isGuestNFSVolume(""))
	require.False(t, isGuestNFSVolume("0a45639e-28c3-407e-a709-e7cc90264276")) // CNS volume
	require.False(t, isGuestNFSVolume("file:0a45639e-28c3-407e-a709-e7cc90264276"))
	require.False(t, isGuestNFSVolume("fv:ns:fv-1"))
	require.False(t, isGuestNFSVolume("x"+guestNFSTestVolumeID))
}

// For any other volume NodeStageVolume rejects a request without a capability, so success here
// shows the guest NFS volume was acknowledged before any of that handling.
func TestNodeStageVolumeGuestNFSVolumeIsANoOp(t *testing.T) {
	driver := NewDriver().(*vsphereCSIDriver)

	resp, err := driver.NodeStageVolume(context.Background(), &csi.NodeStageVolumeRequest{
		VolumeId:          guestNFSTestVolumeID,
		StagingTargetPath: "/tmp/staging",
	})
	require.NoError(t, err)
	require.NotNil(t, resp)

	// Not tied to the feature state: kubelet must always be able to finish staging.
	useFakeOrchestrator(t, false)
	_, err = driver.NodeStageVolume(context.Background(), &csi.NodeStageVolumeRequest{
		VolumeId: guestNFSTestVolumeID, StagingTargetPath: "/tmp/staging",
	})
	require.NoError(t, err)
}

// For any other volume NodeUnstageVolume rejects an empty staging path.
func TestNodeUnstageVolumeGuestNFSVolumeIsANoOp(t *testing.T) {
	driver := NewDriver().(*vsphereCSIDriver)

	resp, err := driver.NodeUnstageVolume(context.Background(), &csi.NodeUnstageVolumeRequest{
		VolumeId: guestNFSTestVolumeID,
	})
	require.NoError(t, err)
	require.NotNil(t, resp)
}

func TestNodeStageAndUnstageStillRejectBadRequestsForOtherVolumes(t *testing.T) {
	driver := NewDriver().(*vsphereCSIDriver)

	_, err := driver.NodeStageVolume(context.Background(), &csi.NodeStageVolumeRequest{
		VolumeId: "0a45639e-28c3-407e-a709-e7cc90264276", StagingTargetPath: "/tmp/staging",
	})
	require.Equal(t, codes.InvalidArgument, status.Code(err))

	_, err = driver.NodeUnstageVolume(context.Background(), &csi.NodeUnstageVolumeRequest{
		VolumeId: "0a45639e-28c3-407e-a709-e7cc90264276",
	})
	require.Equal(t, codes.InvalidArgument, status.Code(err))
}

// The request is answered by the vendored NFS driver, whose own argument checks produce the
// wording below, not the block volume path. This holds with the feature state on or off: the FSS
// gates provisioning only, so a volume that exists can always be mounted (an FSS reset during an
// upgrade must not stop pods starting on another node or re-mounting after a kubelet restart).
func TestNodePublishVolumeGuestNFSVolumeReachesNFSDriverRegardlessOfFeatureState(t *testing.T) {
	for _, enabled := range []bool{true, false} {
		useFakeOrchestrator(t, enabled)
		driver := NewDriver().(*vsphereCSIDriver)
		mountCap := &csi.VolumeCapability{
			AccessType: &csi.VolumeCapability_Mount{Mount: &csi.VolumeCapability_MountVolume{}},
		}

		_, err := driver.NodePublishVolume(context.Background(), &csi.NodePublishVolumeRequest{
			VolumeId: guestNFSTestVolumeID, VolumeCapability: mountCap,
		})
		require.Equal(t, codes.InvalidArgument, status.Code(err), "enabled=%v: %v", enabled, err)
		require.Contains(t, err.Error(), "Target path not provided")

		_, err = driver.NodePublishVolume(context.Background(), &csi.NodePublishVolumeRequest{
			VolumeId: guestNFSTestVolumeID, TargetPath: t.TempDir(),
		})
		require.Equal(t, codes.InvalidArgument, status.Code(err), "enabled=%v: %v", enabled, err)
		require.Contains(t, err.Error(), "Volume capability missing")

		// The parameters the controller put in the volume context are validated by the driver too.
		_, err = driver.NodePublishVolume(context.Background(), &csi.NodePublishVolumeRequest{
			VolumeId: guestNFSTestVolumeID, TargetPath: t.TempDir(), VolumeCapability: mountCap,
			VolumeContext: map[string]string{"share": "/export"},
		})
		require.Equal(t, codes.InvalidArgument, status.Code(err), "enabled=%v: %v", enabled, err)
		require.Contains(t, err.Error(), "server is a required parameter")
	}
}

// Unmounting must work whether or not the feature is on, so a pod can always be torn down.
func TestNodeUnpublishVolumeGuestNFSVolumeIsNotGatedOnFeatureState(t *testing.T) {
	for _, enabled := range []bool{true, false} {
		useFakeOrchestrator(t, enabled)
		driver := NewDriver().(*vsphereCSIDriver)

		_, err := driver.NodeUnpublishVolume(context.Background(), &csi.NodeUnpublishVolumeRequest{
			VolumeId: guestNFSTestVolumeID,
		})
		// Answered by the vendored driver (its own wording), not refused for the feature state.
		require.Equal(t, codes.InvalidArgument, status.Code(err), "enabled=%v: %v", enabled, err)
		require.Contains(t, err.Error(), "Target path missing in request")
	}
}

// Unmounting a target that is not mounted is a success, so a repeated call after a lost
// response does not leave kubelet stuck.
func TestNodeUnpublishVolumeGuestNFSVolumeIsIdempotent(t *testing.T) {
	useFakeOrchestrator(t, true)
	driver := NewDriver().(*vsphereCSIDriver)
	target := t.TempDir() + "/not-mounted"

	for i := 0; i < 2; i++ {
		resp, err := driver.NodeUnpublishVolume(context.Background(), &csi.NodeUnpublishVolumeRequest{
			VolumeId: guestNFSTestVolumeID, TargetPath: target,
		})
		require.NoError(t, err, "call %d", i)
		require.NotNil(t, resp)
	}
}

func TestNodeGetVolumeStatsGuestNFSVolume(t *testing.T) {
	for _, enabled := range []bool{true, false} {
		useFakeOrchestrator(t, enabled)
		driver := NewDriver().(*vsphereCSIDriver)

		_, err := driver.NodeGetVolumeStats(context.Background(), &csi.NodeGetVolumeStatsRequest{
			VolumeId: guestNFSTestVolumeID,
		})
		require.Equal(t, codes.InvalidArgument, status.Code(err), "enabled=%v: %v", enabled, err)
		require.Contains(t, err.Error(), "volume path was empty", "answered by the NFS driver")

		_, err = driver.NodeGetVolumeStats(context.Background(), &csi.NodeGetVolumeStatsRequest{
			VolumeId: guestNFSTestVolumeID, VolumePath: t.TempDir() + "/missing",
		})
		require.Equal(t, codes.NotFound, status.Code(err), "enabled=%v: %v", enabled, err)
	}
}

func TestNodeGetVolumeStatsGuestNFSVolumeReportsUsage(t *testing.T) {
	useFakeOrchestrator(t, true)
	driver := NewDriver().(*vsphereCSIDriver)

	resp, err := driver.NodeGetVolumeStats(context.Background(), &csi.NodeGetVolumeStatsRequest{
		VolumeId: guestNFSTestVolumeID, VolumePath: t.TempDir(),
	})
	require.NoError(t, err)
	require.NotEmpty(t, resp.GetUsage())
	require.Equal(t, csi.VolumeUsage_BYTES, resp.GetUsage()[0].GetUnit())
}

func TestNodeExpandVolumeGuestNFSVolumeIsANoOp(t *testing.T) {
	driver := NewDriver().(*vsphereCSIDriver)

	resp, err := driver.NodeExpandVolume(context.Background(), &csi.NodeExpandVolumeRequest{
		VolumeId:      guestNFSTestVolumeID,
		VolumePath:    "/does/not/matter",
		CapacityRange: &csi.CapacityRange{RequiredBytes: 2 << 30},
	})
	require.NoError(t, err)
	require.Equal(t, int64(2<<30), resp.GetCapacityBytes())
}
