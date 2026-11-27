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
	"fmt"
	"testing"

	"github.com/container-storage-interface/spec/lib/go/csi"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	testclient "k8s.io/client-go/kubernetes/fake"
	clienttesting "k8s.io/client-go/testing"

	csifault "sigs.k8s.io/vsphere-csi-driver/v3/pkg/common/fault"
	"sigs.k8s.io/vsphere-csi-driver/v3/pkg/common/unittestcommon"
	"sigs.k8s.io/vsphere-csi-driver/v3/pkg/csi/service/common"
	"sigs.k8s.io/vsphere-csi-driver/v3/pkg/csi/service/common/commonco"
	"sigs.k8s.io/vsphere-csi-driver/v3/pkg/csi/service/wcpguest/nfsdriver"
	csitypes "sigs.k8s.io/vsphere-csi-driver/v3/pkg/csi/types"
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

func guestNFSCreateRequest(params map[string]string) *csi.CreateVolumeRequest {
	return &csi.CreateVolumeRequest{
		Name:       "pvc-1",
		Parameters: params,
		VolumeCapabilities: []*csi.VolumeCapability{{
			AccessType: &csi.VolumeCapability_Mount{Mount: &csi.VolumeCapability_MountVolume{}},
			AccessMode: &csi.VolumeCapability_AccessMode{
				Mode: csi.VolumeCapability_AccessMode_MULTI_NODE_MULTI_WRITER},
		}},
	}
}

// A "server" key alone routes a request here, so the whole parameter set is validated before
// anything is created: a stray key on an ordinary vSphere StorageClass must fail, not provision
// NFS on whatever host the key names.
func TestCreateGuestNFSVolumeValidatesStorageClassParameters(t *testing.T) {
	useFakeOrchestrator(t, true)
	c := &controller{}

	tests := map[string]map[string]string{
		"vSphere storage policy next to server": {"server": "10.0.0.1", "storagePolicyName": "gold"},
		"supervisor storage class":              {"server": "10.0.0.1", "svStorageClass": "gold"},
		"server that is not a host":             {"server": "10.0.0.1/export"},
		"server with whitespace":                {"server": "nas 1"},
	}
	for name, params := range tests {
		t.Run(name, func(t *testing.T) {
			resp, fault, err := c.createGuestNFSVolume(context.Background(), guestNFSCreateRequest(params))
			require.Nil(t, resp)
			require.Equal(t, codes.InvalidArgument, status.Code(err), "got: %v", err)
			require.Equal(t, csifault.CSIInvalidArgumentFault, fault)
		})
	}
}

// Requests that pass the wrapper's checks reach the vendored driver, which rejects unsafe ones
// before it mounts anything.
func TestCreateGuestNFSVolumeReachesDriverValidation(t *testing.T) {
	useFakeOrchestrator(t, true)
	c := &controller{}

	_, fault, err := c.createGuestNFSVolume(context.Background(), guestNFSCreateRequest(
		map[string]string{"server": "10.0.0.1", "share": "/export", "subDir": "."}))
	require.Equal(t, codes.InvalidArgument, status.Code(err), "got: %v", err)
	require.Equal(t, csifault.CSIInvalidArgumentFault, fault, "an invalid argument is reported as one")
	require.Contains(t, err.Error(), "share root")
}

// A Supervisor-backed snapshot or volume cannot seed a guest NFS volume; that is refused before the
// vendored driver mounts anything. The share-root subDir makes the driver itself refuse the request
// too, with a different message, should it ever be reached.
func TestCreateGuestNFSVolumeRejectsSupervisorContentSource(t *testing.T) {
	useFakeOrchestrator(t, true)
	c := &controller{}

	sources := map[string]*csi.VolumeContentSource{
		"snapshot": {Type: &csi.VolumeContentSource_Snapshot{
			Snapshot: &csi.VolumeContentSource_SnapshotSource{SnapshotId: "cluster-uid-1234"}}},
		"volume": {Type: &csi.VolumeContentSource_Volume{
			Volume: &csi.VolumeContentSource_VolumeSource{VolumeId: "cluster-uid-1234"}}},
	}
	for name, src := range sources {
		t.Run(name, func(t *testing.T) {
			req := guestNFSCreateRequest(map[string]string{"server": "10.0.0.1", "share": "/export", "subDir": "."})
			req.VolumeContentSource = src
			resp, fault, err := c.createGuestNFSVolume(context.Background(), req)
			require.Nil(t, resp)
			require.Equal(t, codes.InvalidArgument, status.Code(err), "got: %v", err)
			require.Equal(t, csifault.CSIInvalidArgumentFault, fault)
			require.Contains(t, err.Error(), "from a guest NFS snapshot or volume")
		})
	}
}

// DeleteVolume hands the ID to the vendored driver exactly as it received it, prefix included;
// the driver is the only place that understands the prefix. An ID that points at the share root
// is treated as already gone and nothing is deleted.
func TestDeleteGuestNFSVolumeLeavesIDUntouchedAndIgnoresShareRootIDs(t *testing.T) {
	useFakeOrchestrator(t, true)
	c := &controller{guestClient: testclient.NewClientset()}

	id := nfsdriver.VolumeIDPrefix + "10.0.0.1#export#.#pvc-1#"
	req := &csi.DeleteVolumeRequest{VolumeId: id}
	resp, fault, err := c.deleteGuestNFSVolume(context.Background(), req)
	require.NoError(t, err)
	require.Empty(t, fault)
	require.NotNil(t, resp)
	require.Equal(t, id, req.VolumeId, "the wrapper must not rewrite the volume ID")
}

// guestNFSPV returns a guest PersistentVolume of this driver with the given volume handle. A PV
// provisioned by the external-provisioner carries its provisioned-by annotation; a static one,
// written by hand, does not.
func guestNFSPV(name, handle, provisionedBy string) *corev1.PersistentVolume {
	pv := &corev1.PersistentVolume{
		ObjectMeta: metav1.ObjectMeta{Name: name},
		Spec: corev1.PersistentVolumeSpec{
			PersistentVolumeReclaimPolicy: corev1.PersistentVolumeReclaimDelete,
			PersistentVolumeSource: corev1.PersistentVolumeSource{CSI: &corev1.CSIPersistentVolumeSource{
				Driver:       csitypes.Name,
				VolumeHandle: handle,
			}},
		},
		Status: corev1.PersistentVolumeStatus{Phase: corev1.VolumeReleased},
	}
	if provisionedBy != "" {
		pv.Annotations = map[string]string{common.AnnDynamicallyProvisioned: provisionedBy}
	}
	return pv
}

// The danger case: a static PersistentVolume with reclaim policy Delete, whose handle is in exactly
// the form CreateVolume mints, so the vendored driver would remove the directory it names. The
// external-provisioner calls DeleteVolume for it all the same, and nothing in the request tells it
// apart from a volume this driver created; the PersistentVolume does.
func TestDeleteGuestNFSVolumeRefusesStaticPersistentVolumes(t *testing.T) {
	useFakeOrchestrator(t, true)
	// Should the guard ever be bypassed, the driver's mount of this server fails to resolve.
	handle := nfsdriver.VolumeIDPrefix + "nfs.invalid#export#team-a##"

	tests := map[string][]runtime.Object{
		"static PersistentVolume": {guestNFSPV("pv-nfs-static", handle, "")},
		"PersistentVolume provisioned by another provisioner": {
			guestNFSPV("pv-nfs-static", handle, "nfs.csi.k8s.io")},
		// A static PersistentVolume reusing a dynamic volume's handle, e.g. to share its directory
		// with another namespace: the directory is still in use.
		"static PersistentVolume sharing a dynamic volume's handle": {
			guestNFSPV("pvc-1", handle, csitypes.Name), guestNFSPV("pv-nfs-static", handle, "")},
	}
	for name, objects := range tests {
		t.Run(name, func(t *testing.T) {
			c := &controller{guestClient: testclient.NewClientset(objects...)}

			resp, fault, err := c.deleteGuestNFSVolume(context.Background(), &csi.DeleteVolumeRequest{VolumeId: handle})
			require.Nil(t, resp)
			require.Equal(t, codes.FailedPrecondition, status.Code(err), "got: %v", err)
			require.Equal(t, csifault.CSIInvalidArgumentFault, fault)
			require.Contains(t, err.Error(), "pv-nfs-static")
			require.Contains(t, err.Error(), "Retain")
		})
	}
}

// A volume this driver created is still deleted: its PersistentVolume has the provisioned-by
// annotation, or there is none yet because the provisioner is cleaning up a volume whose
// PersistentVolume it failed to create. A handle naming the share root lets the call reach the
// driver without mounting anything.
func TestDeleteGuestNFSVolumeStillDeletesDynamicVolumes(t *testing.T) {
	useFakeOrchestrator(t, true)
	handle := nfsdriver.VolumeIDPrefix + "10.0.0.1#export#.#pvc-1#"

	for name, objects := range map[string][]runtime.Object{
		"dynamically provisioned PersistentVolume": {guestNFSPV("pvc-1", handle, csitypes.Name)},
		"no PersistentVolume":                      nil,
		"static PersistentVolume with another handle": {
			guestNFSPV("pv-nfs-static", nfsdriver.VolumeIDPrefix+"pv-nfs-static", "")},
	} {
		t.Run(name, func(t *testing.T) {
			c := &controller{guestClient: testclient.NewClientset(objects...)}

			resp, fault, err := c.deleteGuestNFSVolume(context.Background(), &csi.DeleteVolumeRequest{VolumeId: handle})
			require.NoError(t, err)
			require.Empty(t, fault)
			require.NotNil(t, resp)
		})
	}
}

// If the PersistentVolumes cannot be read, nothing is deleted and the provisioner retries.
func TestDeleteGuestNFSVolumeFailsClosedWhenPersistentVolumesCannotBeListed(t *testing.T) {
	useFakeOrchestrator(t, true)
	guestClient := testclient.NewClientset()
	guestClient.PrependReactor("list", "persistentvolumes",
		func(clienttesting.Action) (bool, runtime.Object, error) {
			return true, nil, fmt.Errorf("apiserver unavailable")
		})
	c := &controller{guestClient: guestClient}

	resp, fault, err := c.deleteGuestNFSVolume(context.Background(),
		&csi.DeleteVolumeRequest{VolumeId: nfsdriver.VolumeIDPrefix + "nfs.invalid#export#team-a##"})
	require.Nil(t, resp)
	require.Equal(t, codes.Internal, status.Code(err), "got: %v", err)
	require.Equal(t, csifault.CSIInternalFault, fault)
	require.Contains(t, err.Error(), "apiserver unavailable")
}

// The external-attacher publishes a static PersistentVolume with the handle, access mode,
// mountOptions and volumeAttributes written on it (and an empty fsType: pvcsi.yaml does not set
// --default-fstype). Nothing is attached, whatever the handle looks like and whether or not the
// feature state is on, and the Supervisor, whose clients are not set up here, is never consulted.
func TestControllerPublishUnpublishStaticGuestNFSVolume(t *testing.T) {
	attributes := map[string]string{"server": "10.0.0.1", "share": "/exports", "subdir": "team-a"}
	for _, enabled := range []bool{true, false} {
		for _, mode := range []csi.VolumeCapability_AccessMode_Mode{
			csi.VolumeCapability_AccessMode_MULTI_NODE_MULTI_WRITER,
			csi.VolumeCapability_AccessMode_MULTI_NODE_READER_ONLY,
		} {
			useFakeOrchestrator(t, enabled)
			c := &controller{}
			volCap := &csi.VolumeCapability{
				AccessType: &csi.VolumeCapability_Mount{Mount: &csi.VolumeCapability_MountVolume{
					MountFlags: []string{"nfsvers=4.1"}}},
				AccessMode: &csi.VolumeCapability_AccessMode{Mode: mode},
			}
			for _, handle := range []string{
				nfsdriver.VolumeIDPrefix + "pv-nfs-static",
				nfsdriver.VolumeIDPrefix + "10.0.0.1#exports#team-a##",
			} {
				pub, err := c.ControllerPublishVolume(context.Background(), &csi.ControllerPublishVolumeRequest{
					VolumeId: handle, NodeId: "node-1", VolumeCapability: volCap, VolumeContext: attributes,
					Readonly: mode == csi.VolumeCapability_AccessMode_MULTI_NODE_READER_ONLY,
				})
				require.NoError(t, err, "enabled=%v mode=%v handle=%q", enabled, mode, handle)
				require.Empty(t, pub.GetPublishContext())

				unpub, err := c.ControllerUnpublishVolume(context.Background(), &csi.ControllerUnpublishVolumeRequest{
					VolumeId: handle, NodeId: "node-1",
				})
				require.NoError(t, err, "enabled=%v mode=%v handle=%q", enabled, mode, handle)
				require.NotNil(t, unpub)
			}
		}
	}
}

// A handle without the prefix is the name of a Supervisor PVC to pvCSI. When the volume carries
// the NFS attributes of a guest NFS volume, the prefix was forgotten: publishing is refused
// before the Supervisor (whose clients are not set up here) is asked to attach anything.
func TestControllerPublishRejectsGuestNFSVolumeWithoutPrefix(t *testing.T) {
	useFakeOrchestrator(t, true)
	c := &controller{}

	_, err := c.ControllerPublishVolume(context.Background(), &csi.ControllerPublishVolumeRequest{
		VolumeId: "pv-nfs-static", NodeId: "node-1", VolumeCapability: guestNFSMountCap(),
		VolumeContext: map[string]string{"Server": "10.0.0.1", "share": "/exports"},
	})
	require.Equal(t, codes.InvalidArgument, status.Code(err), "got: %v", err)
	require.Contains(t, err.Error(), common.GuestNFSVolumeIDPrefix)

	require.NoError(t, unprefixedGuestNFSVolumeError(context.Background(), "pv-nfs-static", nil))
	require.NoError(t, unprefixedGuestNFSVolumeError(context.Background(), "pv-nfs-static",
		map[string]string{common.AttributeDiskType: common.DiskTypeFileVolume}))
}

const testGuestNFSVolumeID = nfsdriver.VolumeIDPrefix + "10.0.0.1#export#pvc-1##"
const testGuestNFSSnapshotID = nfsdriver.VolumeIDPrefix + "10.0.0.1#export#snap-1#snap-1#pvc-1"

func guestNFSMountCap() *csi.VolumeCapability {
	return &csi.VolumeCapability{
		AccessType: &csi.VolumeCapability_Mount{Mount: &csi.VolumeCapability_MountVolume{FsType: "nfs4"}},
		AccessMode: &csi.VolumeCapability_AccessMode{
			Mode: csi.VolumeCapability_AccessMode_MULTI_NODE_MULTI_WRITER,
		},
	}
}

// Everything that serves a volume that already exists keeps working with the feature state off:
// see isGuestNFSEnabled for why. Each of these would otherwise reach the Supervisor, whose
// client is not even set up on the controller used here.
func TestControllerPublishGuestNFSVolumeNotGatedByFSS(t *testing.T) {
	for _, enabled := range []bool{true, false} {
		useFakeOrchestrator(t, enabled)
		c := &controller{}

		resp, err := c.ControllerPublishVolume(context.Background(), &csi.ControllerPublishVolumeRequest{
			VolumeId: testGuestNFSVolumeID, NodeId: "node-1", VolumeCapability: guestNFSMountCap(),
		})
		require.NoError(t, err, "enabled=%v: publish of an existing volume must not be blocked", enabled)
		require.Empty(t, resp.GetPublishContext(), "guest NFS volumes need no publish context")
	}
}

func TestControllerUnpublishGuestNFSVolumeNotGatedByFSS(t *testing.T) {
	for _, enabled := range []bool{true, false} {
		useFakeOrchestrator(t, enabled)
		c := &controller{}

		resp, err := c.ControllerUnpublishVolume(context.Background(), &csi.ControllerUnpublishVolumeRequest{
			VolumeId: testGuestNFSVolumeID, NodeId: "node-1",
		})
		require.NoError(t, err, "enabled=%v: unpublish must not be blocked", enabled)
		require.NotNil(t, resp)
	}
}

func TestControllerExpandGuestNFSVolume(t *testing.T) {
	for _, enabled := range []bool{true, false} {
		useFakeOrchestrator(t, enabled)
		c := &controller{}

		resp, err := c.ControllerExpandVolume(context.Background(), &csi.ControllerExpandVolumeRequest{
			VolumeId:         testGuestNFSVolumeID,
			CapacityRange:    &csi.CapacityRange{RequiredBytes: 5 << 30},
			VolumeCapability: guestNFSMountCap(),
		})
		require.NoError(t, err, "enabled=%v", enabled)
		require.Equal(t, int64(5<<30), resp.GetCapacityBytes())
		require.False(t, resp.GetNodeExpansionRequired(), "an NFS volume has no filesystem to grow on the node")
	}
}

// external-provisioner refuses a PVC data source unless CLONE_VOLUME is advertised, so cloning a
// guest NFS volume needs it; with the feature off, nothing can be cloned and it stays hidden.
func TestControllerGetCapabilitiesCloneVolumeFollowsGuestNFSFeatureState(t *testing.T) {
	before := len(controllerCaps)
	for _, enabled := range []bool{true, false} {
		useFakeOrchestrator(t, enabled)
		resp, err := (&controller{}).ControllerGetCapabilities(context.Background(),
			&csi.ControllerGetCapabilitiesRequest{})
		require.NoError(t, err)
		require.Equal(t, enabled, hasCap(resp, csi.ControllerServiceCapability_RPC_CLONE_VOLUME), "enabled=%v", enabled)
		require.True(t, hasCap(resp, csi.ControllerServiceCapability_RPC_CREATE_DELETE_SNAPSHOT))
	}
	require.Len(t, controllerCaps, before, "the package-level capability list must not grow")
}

func TestValidateVolumeCapabilitiesGuestNFSVolume(t *testing.T) {
	useFakeOrchestrator(t, true)
	c := &controller{}

	resp, err := c.ValidateVolumeCapabilities(context.Background(), &csi.ValidateVolumeCapabilitiesRequest{
		VolumeId:           testGuestNFSVolumeID,
		VolumeCapabilities: []*csi.VolumeCapability{guestNFSMountCap()},
	})
	require.NoError(t, err)
	require.NotNil(t, resp.GetConfirmed())

	// The vendored driver's stricter validation applies, which the Supervisor path does not know about.
	_, err = c.ValidateVolumeCapabilities(context.Background(), &csi.ValidateVolumeCapabilitiesRequest{
		VolumeId:           testGuestNFSVolumeID,
		VolumeCapabilities: []*csi.VolumeCapability{nil},
	})
	require.Equal(t, codes.InvalidArgument, status.Code(err), "got: %v", err)
}

func TestCreateSnapshotGuestNFSVolumeNeedsFeatureState(t *testing.T) {
	useFakeOrchestrator(t, false)
	c := &controller{}

	resp, err := c.CreateSnapshot(context.Background(), &csi.CreateSnapshotRequest{
		Name: "snapshot-1", SourceVolumeId: testGuestNFSVolumeID,
	})
	require.Nil(t, resp)
	require.Equal(t, codes.FailedPrecondition, status.Code(err), "got: %v", err)
	require.Contains(t, err.Error(), common.GuestNFSVolume)
}

// With the feature on, the snapshot is taken by the vendored driver, not on the Supervisor and
// not subject to the Supervisor's block volume snapshot feature (which the fake leaves off, so
// reaching it would answer Unimplemented). A source ID that decodes to the share root is refused
// by the driver before it mounts anything.
func TestCreateSnapshotGuestNFSVolumeReachesNFSDriver(t *testing.T) {
	useFakeOrchestrator(t, true)
	c := &controller{}

	_, err := c.CreateSnapshot(context.Background(), &csi.CreateSnapshotRequest{
		Name:           "snapshot-1",
		SourceVolumeId: nfsdriver.VolumeIDPrefix + "10.0.0.1#export#.#pvc-1#",
	})
	require.Equal(t, codes.NotFound, status.Code(err), "got: %v", err)
	require.Contains(t, err.Error(), "share root")
}

func TestWithoutExternalMetadata(t *testing.T) {
	in := map[string]string{
		"csi.storage.k8s.io/volumesnapshot/name":        "snap",
		"csi.storage.k8s.io/volumesnapshot/namespace":   "ns",
		"CSI.storage.k8s.io/volumesnapshotcontent/name": "content",
		"server": "10.0.0.2",
		"share":  "/other",
	}
	out := withoutExternalMetadata(in)
	require.Equal(t, map[string]string{"server": "10.0.0.2", "share": "/other"}, out)
	require.Len(t, in, 5, "the request's own parameters must not be modified")
	require.Empty(t, withoutExternalMetadata(nil))
}

func TestDeleteSnapshotGuestNFSSnapshotNotGatedByFSS(t *testing.T) {
	useFakeOrchestrator(t, false)
	c := &controller{}

	// An ID the driver cannot decode is treated as already deleted, which succeeds; the point is that
	// the call is answered by the driver rather than refused for the feature state or sent to the
	// Supervisor, whose client is not set up here.
	resp, err := c.DeleteSnapshot(context.Background(), &csi.DeleteSnapshotRequest{
		SnapshotId: nfsdriver.VolumeIDPrefix + "not-a-snapshot-id",
	})
	require.NoError(t, err)
	require.NotNil(t, resp)
}

// Listing the snapshots of a guest NFS volume, or by an ID that names no guest NFS snapshot, is
// answered without the Supervisor (whose client is not set up here) and without mounting anything.
// Looking up an existing snapshot by ID is covered by the nfsdriver package's GetSnapshot tests.
func TestListSnapshotsGuestNFSIDsReturnEmptyList(t *testing.T) {
	for _, enabled := range []bool{true, false} {
		useFakeOrchestrator(t, enabled)
		c := &controller{}

		for _, req := range []*csi.ListSnapshotsRequest{
			{SnapshotId: nfsdriver.VolumeIDPrefix + "not-a-snapshot-id"},
			{SnapshotId: nfsdriver.VolumeIDPrefix + "10.0.0.1#export#.#.#pvc-1"},
			{SourceVolumeId: testGuestNFSVolumeID},
		} {
			resp, err := c.ListSnapshots(context.Background(), req)
			require.NoError(t, err, "enabled=%v", enabled)
			require.Empty(t, resp.GetEntries())
		}
	}
}
