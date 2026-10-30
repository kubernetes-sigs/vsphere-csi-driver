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
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"sigs.k8s.io/vsphere-csi-driver/v3/pkg/common/unittestcommon"
	"sigs.k8s.io/vsphere-csi-driver/v3/pkg/csi/service/common"
	"sigs.k8s.io/vsphere-csi-driver/v3/pkg/csi/service/common/commonco"
	"sigs.k8s.io/vsphere-csi-driver/v3/pkg/csi/service/wcpguest/nfsdriver"
)

// useFakeOrchestrator installs a fake container orchestrator for the test, with the
// guest-nfs-volume FSS enabled or disabled.
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

func TestIsGuestNFSEnabled(t *testing.T) {
	ctx := context.Background()

	old := commonco.ContainerOrchestratorUtility
	commonco.ContainerOrchestratorUtility = nil
	require.False(t, isGuestNFSEnabled(ctx), "no orchestrator must read as disabled")
	commonco.ContainerOrchestratorUtility = old

	useFakeOrchestrator(t, false)
	require.False(t, isGuestNFSEnabled(ctx))

	useFakeOrchestrator(t, true)
	require.True(t, isGuestNFSEnabled(ctx))
}

func TestCreateGuestNFSVolumeRejectedWhenFSSDisabled(t *testing.T) {
	useFakeOrchestrator(t, false)
	c := &controller{}

	resp, _, err := c.createGuestNFSVolume(context.Background(), &csi.CreateVolumeRequest{
		Name:       "pvc-1",
		Parameters: map[string]string{"server": "10.0.0.1", "share": "/export"},
	})
	require.Nil(t, resp)
	require.Error(t, err)
	require.Equal(t, codes.InvalidArgument, status.Code(err))
}

func TestDeleteGuestNFSVolumeRejectedWhenFSSDisabled(t *testing.T) {
	useFakeOrchestrator(t, false)
	c := &controller{}

	req := &csi.DeleteVolumeRequest{VolumeId: nfsdriver.VolumeIDPrefix + "10.0.0.1#export#pvc-1##"}
	resp, _, err := c.deleteGuestNFSVolume(context.Background(), req)
	require.Nil(t, resp)
	require.Error(t, err)
	require.Equal(t, codes.FailedPrecondition, status.Code(err))
	require.Equal(t, nfsdriver.VolumeIDPrefix+"10.0.0.1#export#pvc-1##", req.VolumeId,
		"the volume ID must be left untouched when the call is rejected")
}

const testGuestNFSVolumeID = nfsdriver.VolumeIDPrefix + "10.0.0.1#export#pvc-1##"

func TestControllerPublishGuestNFSVolumeNotGatedByFSS(t *testing.T) {
	useFakeOrchestrator(t, false)
	c := &controller{}

	resp, err := c.ControllerPublishVolume(context.Background(), &csi.ControllerPublishVolumeRequest{
		VolumeId: testGuestNFSVolumeID,
		NodeId:   "node-1",
		VolumeCapability: &csi.VolumeCapability{
			AccessType: &csi.VolumeCapability_Mount{Mount: &csi.VolumeCapability_MountVolume{FsType: "nfs4"}},
			AccessMode: &csi.VolumeCapability_AccessMode{
				Mode: csi.VolumeCapability_AccessMode_MULTI_NODE_MULTI_WRITER,
			},
		},
	})
	require.NoError(t, err, "publish of an existing volume must not be blocked by a disabled FSS")
	require.Empty(t, resp.GetPublishContext(), "guest NFS volumes need no publish context")
}

func TestControllerUnpublishGuestNFSVolumeNotGatedByFSS(t *testing.T) {
	useFakeOrchestrator(t, false)
	c := &controller{}

	resp, err := c.ControllerUnpublishVolume(context.Background(), &csi.ControllerUnpublishVolumeRequest{
		VolumeId: testGuestNFSVolumeID,
		NodeId:   "node-1",
	})
	require.NoError(t, err, "unpublish must not be blocked by a disabled FSS")
	require.NotNil(t, resp)
}
