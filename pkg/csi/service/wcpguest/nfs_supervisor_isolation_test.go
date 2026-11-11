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

// Tests that a guest NFS volume never reaches the Supervisor: it is not used to seed a Supervisor
// PVC, not modified through a Supervisor PVC, and ListVolumes reports it published on the nodes
// of its VolumeAttachments rather than looking for it in Supervisor objects.

import (
	"context"
	"errors"
	"reflect"
	"testing"

	"github.com/container-storage-interface/spec/lib/go/csi"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/runtime"
	testclient "k8s.io/client-go/kubernetes/fake"
	ktesting "k8s.io/client-go/testing"
	ctrlclient "sigs.k8s.io/controller-runtime/pkg/client"

	"sigs.k8s.io/vsphere-csi-driver/v3/pkg/csi/service/common"
	"sigs.k8s.io/vsphere-csi-driver/v3/pkg/csi/service/common/commonco"
)

func TestGuestNFSContentSourceID(t *testing.T) {
	snapshot := func(id string) *csi.VolumeContentSource {
		return &csi.VolumeContentSource{Type: &csi.VolumeContentSource_Snapshot{
			Snapshot: &csi.VolumeContentSource_SnapshotSource{SnapshotId: id}}}
	}
	volume := func(id string) *csi.VolumeContentSource {
		return &csi.VolumeContentSource{Type: &csi.VolumeContentSource_Volume{
			Volume: &csi.VolumeContentSource_VolumeSource{VolumeId: id}}}
	}
	require.Equal(t, testGuestNFSSnapshotID, guestNFSContentSourceID(snapshot(testGuestNFSSnapshotID)))
	require.Equal(t, testGuestNFSVolumeID, guestNFSContentSourceID(volume(testGuestNFSVolumeID)))
	require.Empty(t, guestNFSContentSourceID(snapshot("cluster-uid-1234")))
	require.Empty(t, guestNFSContentSourceID(volume("cluster-uid-1234")))
	require.Empty(t, guestNFSContentSourceID(nil))
	require.Empty(t, guestNFSContentSourceID(&csi.VolumeContentSource{}))
}

// A PVC on a Supervisor-backed StorageClass whose data source is a guest NFS snapshot or volume
// must be rejected before any Supervisor object is created: the Supervisor PVC would reference a
// VolumeSnapshot that can never exist and stay Pending.
func TestCreateVolumeRejectsGuestNFSContentSourceOnSupervisorStorageClass(t *testing.T) {
	sources := map[string]*csi.VolumeContentSource{
		"snapshot": {Type: &csi.VolumeContentSource_Snapshot{
			Snapshot: &csi.VolumeContentSource_SnapshotSource{SnapshotId: testGuestNFSSnapshotID}}},
		"volume": {Type: &csi.VolumeContentSource_Volume{
			Volume: &csi.VolumeContentSource_VolumeSource{VolumeId: testGuestNFSVolumeID}}},
	}
	for name, src := range sources {
		t.Run(name, func(t *testing.T) {
			useFakeOrchestrator(t, true)
			supervisorClient := testclient.NewClientset()
			// Fail every Supervisor call, so that without the guard the request fails here at once
			// instead of waiting for a Supervisor PVC to bind.
			supervisorClient.PrependReactor("*", "*", func(ktesting.Action) (bool, runtime.Object, error) {
				return true, nil, errors.New("unexpected Supervisor call")
			})
			c := &controller{supervisorClient: supervisorClient, supervisorNamespace: testNamespace}

			_, err := c.CreateVolume(context.Background(), &csi.CreateVolumeRequest{
				Name:                "pvc-12345678",
				Parameters:          map[string]string{common.AttributeSupervisorStorageClass: "gold"},
				VolumeCapabilities:  []*csi.VolumeCapability{blockMountCap()},
				VolumeContentSource: src,
			})
			require.Equal(t, codes.InvalidArgument, status.Code(err), "err: %v", err)
			require.Contains(t, err.Error(), "guest NFS")
			require.Empty(t, supervisorClient.Actions(), "no Supervisor object may be read or created")
		})
	}
}

func blockMountCap() *csi.VolumeCapability {
	return &csi.VolumeCapability{
		AccessType: &csi.VolumeCapability_Mount{Mount: &csi.VolumeCapability_MountVolume{FsType: "ext4"}},
		AccessMode: &csi.VolumeCapability_AccessMode{Mode: csi.VolumeCapability_AccessMode_SINGLE_NODE_WRITER},
	}
}

// A guest NFS volume has no Supervisor PVC to change the VolumeAttributesClass of.
func TestControllerModifyVolumeRejectsGuestNFSVolume(t *testing.T) {
	useFakeOrchestrator(t, true)
	require.NoError(t, commonco.ContainerOrchestratorUtility.(interface {
		EnableFSS(ctx context.Context, featureName string) error
	}).EnableFSS(context.Background(), common.VMPVCStoragePolicyMutabilityFSS))
	supervisorClient := testclient.NewClientset()
	guestClient := testclient.NewClientset()
	c := &controller{supervisorClient: supervisorClient, guestClient: guestClient, supervisorNamespace: testNamespace}

	_, err := c.ControllerModifyVolume(context.Background(), &csi.ControllerModifyVolumeRequest{
		VolumeId: testGuestNFSVolumeID, MutableParameters: map[string]string{},
	})
	require.Equal(t, codes.InvalidArgument, status.Code(err), "err: %v", err)
	require.Empty(t, supervisorClient.Actions())
	require.Empty(t, guestClient.Actions())
}

// A guest NFS volume's ControllerPublishVolume is a no-op, so ListVolumes must report it
// published on the node of each attached VolumeAttachment, whatever its access mode and whether
// or not the FVS capability is on. Reporting it unpublished would make external-attacher mark
// every guest NFS VolumeAttachment detached on each reconcile.
func TestListVolumesGuestNFSVolumeEchoesVolumeAttachment(t *testing.T) {
	for _, tc := range []struct {
		name       string
		accessMode v1.PersistentVolumeAccessMode
		fvs        bool
	}{
		{"RWX, FVS on", v1.ReadWriteMany, true},
		{"RWX, FVS off", v1.ReadWriteMany, false},
		{"RWO, FVS on", v1.ReadWriteOnce, true},
		{"RWO, FVS off", v1.ReadWriteOnce, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			setVsanFileVolumeServiceEnabledForTest(t, tc.fvs)
			const pvName = "pvc-nfs-1"
			c := newListVolumesControllerWithSupervisorObjs(t,
				[]ctrlclient.Object{newVM("node-b")},
				[]runtime.Object{
					newGuestPVWithName(pvName, testGuestNFSVolumeID, tc.accessMode),
					newGuestVolumeAttachmentForPV(pvName, testGuestNFSVolumeID, "node-a"),
					newGuestVolumeAttachmentForPV(pvName, testGuestNFSVolumeID, "node-b"),
				},
				nil, nil)
			// Nothing about a guest NFS volume is in the Supervisor: neither the classification
			// pass nor the legacy file pass may run, so their list failures must not matter.
			failFileVolumeLists(t, c, true, true, false)

			resp, err := c.ListVolumes(context.Background(), &csi.ListVolumesRequest{})
			require.NoError(t, err)
			e := entryForHandle(resp, testGuestNFSVolumeID)
			require.NotNil(t, e)
			got := e.GetStatus().GetPublishedNodeIds()
			if want := []string{"node-a", "node-b"}; !reflect.DeepEqual(got, want) {
				t.Errorf("PublishedNodeIds = %v, want %v", got, want)
			}
		})
	}
}

// As for FVS, only an attach that completed is echoed.
func TestListVolumesGuestNFSVolumeNotYetAttachedUnpublished(t *testing.T) {
	const pvName = "pvc-nfs-1"
	va := newGuestVolumeAttachmentForPV(pvName, testGuestNFSVolumeID, "node-a")
	va.Status.Attached = false
	c := newListVolumesControllerWithSupervisorObjs(t, []ctrlclient.Object{newVM("node-a")},
		[]runtime.Object{newGuestPVWithName(pvName, testGuestNFSVolumeID, v1.ReadWriteMany), va}, nil, nil)

	resp, err := c.ListVolumes(context.Background(), &csi.ListVolumesRequest{})
	require.NoError(t, err)
	e := entryForHandle(resp, testGuestNFSVolumeID)
	require.NotNil(t, e)
	require.Empty(t, e.GetStatus().GetPublishedNodeIds())
}

// Guest NFS volumes are not block volumes: on their own they must not trip the guard that refuses
// an empty VirtualMachine list while block volumes are owned.
func TestListVolumesGuestNFSVolumeIsNotCountedAsBlock(t *testing.T) {
	const pvName = "pvc-nfs-1"
	c := newListVolumesControllerWithSupervisorObjs(t, nil,
		[]runtime.Object{
			newGuestPVWithName(pvName, testGuestNFSVolumeID, v1.ReadWriteOnce),
			newGuestVolumeAttachmentForPV(pvName, testGuestNFSVolumeID, "node-a"),
		}, nil, nil)

	resp, err := c.ListVolumes(context.Background(), &csi.ListVolumesRequest{})
	require.NoError(t, err)
	require.NotNil(t, entryForHandle(resp, testGuestNFSVolumeID))
}
