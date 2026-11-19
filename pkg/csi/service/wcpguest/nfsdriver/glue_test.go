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

package nfsdriver

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/container-storage-interface/spec/lib/go/csi"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// Every PVC directory is created with the controller's own mode unless a StorageClass opts in,
// so one workload cannot read or write another PVC's data by being able to mount the export.
func TestDefaultMountPermissionsIsNotWorldWritable(t *testing.T) {
	require.Equal(t, 0, defaultMountPermissions,
		"the default must stay 0 (no chmod); workloads that need a writable volume set mountPermissions")
	require.Equal(t, uint64(0), GetOrCreateDriver().mountPermissions)
}

func TestValidateCreateVolumeParameters(t *testing.T) {
	ok := func(extra map[string]string) map[string]string {
		p := map[string]string{"server": "10.0.0.1", "share": "/export"}
		for k, v := range extra {
			p[k] = v
		}
		return p
	}

	good := []map[string]string{
		ok(nil),
		ok(map[string]string{"subDir": "tenant", "onDelete": "retain", "mountPermissions": "0775"}),
		ok(map[string]string{"SERVER": "nas-1.example.com"}), // keys are case-insensitive
		ok(map[string]string{
			"csi.storage.k8s.io/pvc/name": "claim", "csi.storage.k8s.io/pvc/namespace": "ns",
			"csi.storage.k8s.io/pv/name": "pvc-1",
		}),
		{"server": "fd00::1", "share": "/export"},
		{"server": "NAS.Example.COM"},
	}
	for _, p := range good {
		require.NoError(t, ValidateCreateVolumeParameters(p), "%v", p)
	}

	bad := map[string]map[string]string{
		// A stray "server" on an ordinary vSphere StorageClass must not provision NFS silently.
		"vSphere storage policy":    ok(map[string]string{"storagePolicyName": "gold"}),
		"supervisor storage class":  ok(map[string]string{"svStorageClass": "gold"}),
		"unknown key":               ok(map[string]string{"foo": "bar"}),
		"other external metadata":   ok(map[string]string{"csi.storage.k8s.io/other": "x"}),
		"empty server":              {"server": ""},
		"server with a path":        {"server": "10.0.0.1/export"},
		"server with a port":        {"server": "10.0.0.1:2049"},
		"server with whitespace":    {"server": "nas 1"},
		"server with the separator": {"server": "nas#1"},
		"server with a scheme":      {"server": "nfs://nas"},
		"server with a quote":       {"server": "nas'1"},
	}
	for name, p := range bad {
		t.Run(name, func(t *testing.T) {
			require.Error(t, ValidateCreateVolumeParameters(p), "%v", p)
		})
	}
	err := ValidateCreateVolumeParameters(ok(map[string]string{"storagePolicyName": "gold"}))
	require.Contains(t, err.Error(), "storagePolicyName")
	require.Contains(t, err.Error(), "supported parameters")
}

// The prefix is added when an ID is minted and removed when it is decoded, so IDs pass through
// every caller unchanged in both directions.
func TestIDPrefixIsAddedAndRemovedInOnePlace(t *testing.T) {
	params := map[string]string{"server": "10.0.0.1", "share": "/export"}

	vol, err := newNFSVolume("pvc-1", 0, params, "")
	require.NoError(t, err)
	require.True(t, strings.HasPrefix(vol.id, VolumeIDPrefix))
	require.True(t, IsGuestVolumeID(vol.id))
	require.Equal(t, 1, strings.Count(vol.id, VolumeIDPrefix), "the prefix must not be doubled")

	// The decoded volume reports the ID it was given, prefix included, and parses the rest.
	decoded, err := getNfsVolFromID(vol.id)
	require.NoError(t, err)
	require.Equal(t, vol.id, decoded.id)
	require.Equal(t, "10.0.0.1", decoded.server)
	require.Equal(t, "pvc-1", decoded.subDir)

	// An ID without the prefix still decodes (IDs minted before the prefix, and the legacy form).
	bare, err := getNfsVolFromID(strings.TrimPrefix(vol.id, VolumeIDPrefix))
	require.NoError(t, err)
	require.Equal(t, "10.0.0.1", bare.server)

	snap, err := newNFSSnapshot("snap-1", nil, vol)
	require.NoError(t, err)
	require.True(t, IsGuestVolumeID(snap.id))
	decodedSnap, err := getNfsSnapFromID(snap.id)
	require.NoError(t, err)
	require.Equal(t, snap.id, decodedSnap.id)
	require.Equal(t, "10.0.0.1", decodedSnap.server)
	require.Equal(t, "snap-1", decodedSnap.uuid)
	require.Equal(t, "pvc-1", decodedSnap.src)
	bareSnap, err := getNfsSnapFromID(strings.TrimPrefix(snap.id, VolumeIDPrefix))
	require.NoError(t, err)
	require.Equal(t, "10.0.0.1", bareSnap.server)
}

// A volume cloned from another volume is passed the prefixed source ID by the provisioner.
func TestCreateVolumeCloneFromPrefixedSourceID(t *testing.T) {
	cs, workDir := newTestControllerServer(t)
	params := map[string]string{"server": "10.0.0.1", "share": "/export"}
	src, err := newNFSVolume("pvc-src", 0, params, "")
	require.NoError(t, err)
	seedVolume(t, workDir, "pvc-src", "pvc-src")

	resp, err := cs.CreateVolume(context.Background(), &csi.CreateVolumeRequest{
		Name:               "pvc-clone",
		Parameters:         params,
		VolumeCapabilities: []*csi.VolumeCapability{mountCap(csi.VolumeCapability_AccessMode_MULTI_NODE_MULTI_WRITER)},
		VolumeContentSource: &csi.VolumeContentSource{
			Type: &csi.VolumeContentSource_Volume{Volume: &csi.VolumeContentSource_VolumeSource{VolumeId: src.id}},
		},
	})
	require.NoError(t, err)
	require.True(t, IsGuestVolumeID(resp.Volume.VolumeId))
	require.Equal(t, src.id, resp.Volume.ContentSource.GetVolume().GetVolumeId(),
		"the content source is reported back as it was given")
	require.FileExists(t, filepath.Join(workDir, "pvc-clone", "pvc-clone", "data.txt"),
		"the clone must contain the source's data")
}

func TestCreateSnapshotFromPrefixedSourceID(t *testing.T) {
	cs, workDir := newTestControllerServer(t)
	params := map[string]string{"server": "10.0.0.1", "share": "/export"}
	src, err := newNFSVolume("pvc-src", 0, params, "")
	require.NoError(t, err)
	seedVolume(t, workDir, "pvc-src", "pvc-src")

	resp, err := cs.CreateSnapshot(context.Background(), &csi.CreateSnapshotRequest{
		Name: "snap-1", SourceVolumeId: src.id,
	})
	require.NoError(t, err)
	require.True(t, IsGuestVolumeID(resp.Snapshot.SnapshotId))
	require.Equal(t, src.id, resp.Snapshot.SourceVolumeId)
	require.True(t, resp.Snapshot.ReadyToUse)
}

// A clone that fails part-way must not leave a half-populated directory behind: a retry would
// find it, copy again over the partial result and report success.
func TestCreateVolumeRemovesDirectoryWhenCloneFails(t *testing.T) {
	cs, workDir := newTestControllerServer(t)
	params := map[string]string{"server": "10.0.0.1", "share": "/export"}
	missing, err := newNFSVolume("pvc-missing", 0, params, "")
	require.NoError(t, err) // no data was ever created for this source, so the copy fails

	req := &csi.CreateVolumeRequest{
		Name:               "pvc-clone",
		Parameters:         params,
		VolumeCapabilities: []*csi.VolumeCapability{mountCap(csi.VolumeCapability_AccessMode_MULTI_NODE_MULTI_WRITER)},
		VolumeContentSource: &csi.VolumeContentSource{
			Type: &csi.VolumeContentSource_Volume{Volume: &csi.VolumeContentSource_VolumeSource{VolumeId: missing.id}},
		},
	}
	resp, err := cs.CreateVolume(context.Background(), req)
	require.Error(t, err)
	require.Nil(t, resp)
	require.NoDirExists(t, filepath.Join(workDir, "pvc-clone", "pvc-clone"),
		"the directory created for the failed clone must be removed")

	// And the retry fails the same way instead of succeeding against what was left.
	_, err = cs.CreateVolume(context.Background(), req)
	require.Error(t, err)
}

// A directory that was already there belongs to an earlier attempt of the same volume (for
// example one whose response was lost), so a failed retry must not delete its data.
func TestCreateVolumeKeepsPreexistingDirectoryWhenCloneFails(t *testing.T) {
	cs, workDir := newTestControllerServer(t)
	params := map[string]string{"server": "10.0.0.1", "share": "/export"}
	missing, err := newNFSVolume("pvc-missing", 0, params, "")
	require.NoError(t, err)
	existing := seedVolume(t, workDir, "pvc-clone", "pvc-clone")

	_, err = cs.CreateVolume(context.Background(), &csi.CreateVolumeRequest{
		Name:               "pvc-clone",
		Parameters:         params,
		VolumeCapabilities: []*csi.VolumeCapability{mountCap(csi.VolumeCapability_AccessMode_MULTI_NODE_MULTI_WRITER)},
		VolumeContentSource: &csi.VolumeContentSource{
			Type: &csi.VolumeContentSource_Volume{Volume: &csi.VolumeContentSource_VolumeSource{VolumeId: missing.id}},
		},
	})
	require.Error(t, err)
	require.FileExists(t, filepath.Join(existing, "data.txt"))
}

func TestCreateVolumeWithoutContentSourceIsUnaffectedByCleanup(t *testing.T) {
	cs, workDir := newTestControllerServer(t)
	params := map[string]string{"server": "10.0.0.1", "share": "/export"}

	resp, err := cs.CreateVolume(context.Background(), &csi.CreateVolumeRequest{
		Name:               "pvc-plain",
		Parameters:         params,
		VolumeCapabilities: []*csi.VolumeCapability{mountCap(csi.VolumeCapability_AccessMode_MULTI_NODE_MULTI_WRITER)},
	})
	require.NoError(t, err)
	require.True(t, IsGuestVolumeID(resp.Volume.VolumeId))
	require.DirExists(t, filepath.Join(workDir, "pvc-plain", "pvc-plain"))
	// The context handed back for the node carries the resolved subdirectory.
	require.Equal(t, "pvc-plain", resp.Volume.VolumeContext[paramSubDir])
}

// staticPublishRequest is the NodePublishVolume request kubelet sends for a statically provisioned
// PersistentVolume with the given handle and volumeAttributes, mountOptions [nfsvers=4.1] and
// access mode ReadWriteMany.
func staticPublishRequest(target, handle string, attributes map[string]string) *csi.NodePublishVolumeRequest {
	volCap := mountCap(csi.VolumeCapability_AccessMode_MULTI_NODE_MULTI_WRITER)
	volCap.GetMount().MountFlags = []string{"nfsvers=4.1"}
	return &csi.NodePublishVolumeRequest{
		VolumeId:         handle,
		TargetPath:       target,
		VolumeCapability: volCap,
		VolumeContext:    attributes,
	}
}

// mountedPath is the path the fake mounter records a mount of target under.
func mountedPath(t *testing.T, target string) string {
	t.Helper()
	p, err := filepath.EvalSymlinks(target)
	require.NoError(t, err)
	return p
}

func TestNodePublishGuestVolumeMountsStaticPV(t *testing.T) {
	m := newFakeNFSMounter()
	ns := newTestNodeServer(t, m)
	target := filepath.Join(t.TempDir(), "target")
	req := staticPublishRequest(target, VolumeIDPrefix+"pv-nfs-static",
		map[string]string{"server": "nas-1.example.com", "share": "/exports", "subdir": "team-a"})
	req.Readonly = true

	_, err := ns.NodePublishGuestVolume(context.Background(), req)
	require.NoError(t, err)
	require.Len(t, m.MountPoints, 1)
	require.Equal(t, "nas-1.example.com:/exports/team-a", m.MountPoints[0].Device)
	require.Equal(t, mountedPath(t, target), m.MountPoints[0].Path)
	require.Equal(t, "nfs", m.MountPoints[0].Type)
	require.Equal(t, []string{"nfsvers=4.1", "ro"}, m.MountPoints[0].Opts)

	// Without a subdir the share itself is mounted.
	other := filepath.Join(t.TempDir(), "target")
	_, err = ns.NodePublishGuestVolume(context.Background(), staticPublishRequest(other, VolumeIDPrefix+"pv-2",
		map[string]string{"server": "fd00::1", "share": "/exports"}))
	require.NoError(t, err)
	require.Equal(t, "[fd00::1]:/exports", m.MountPoints[1].Device)
}

// A static PersistentVolume's attributes are checked like a StorageClass's before anything is
// mounted, and before the target path is even created.
func TestNodePublishGuestVolumeRejectsInvalidLocation(t *testing.T) {
	loc := func(server, share, subDir string) map[string]string {
		return map[string]string{"server": server, "share": share, "subdir": subDir}
	}
	tests := []struct {
		name       string
		handle     string
		attributes map[string]string
		errPart    string
	}{
		{"missing server", "", map[string]string{"share": "/exports"}, "server is a required parameter"},
		{"missing share", "", map[string]string{"server": "10.0.0.1"}, "share is a required parameter"},
		{"server with a path", "", loc("10.0.0.1:/other", "/exports", ""), "invalid server"},
		{"server with whitespace", "", loc("nas 1", "/exports", ""), "invalid server"},
		{"server that reads as an option", "", loc("-oremount", "/exports", ""), "invalid server"},
		{"share traversal", "", loc("10.0.0.1", "/exports/../etc", ""), "traversal"},
		{"subdir traversal", "", loc("10.0.0.1", "/exports", "../other-team"), "traversal"},
		{"nested subdir traversal", "", loc("10.0.0.1", "/exports", "team-a/../../etc"), "traversal"},
		{"share with the ID separator", "", loc("10.0.0.1", "/ex#ports", ""), "must not contain"},
		{"subdir with the ID separator", "", loc("10.0.0.1", "/exports", "team#a"), "must not contain"},
		{"no location and a free-form handle", VolumeIDPrefix + "pv-nfs-static", nil, "no NFS location"},
		{"no location and a handle naming the share root", VolumeIDPrefix + "10.0.0.1#exports#.##", nil,
			"share root"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			m := newFakeNFSMounter()
			ns := newTestNodeServer(t, m)
			target := filepath.Join(t.TempDir(), "target")
			handle := tc.handle
			if handle == "" {
				handle = VolumeIDPrefix + "pv-nfs-static"
			}

			_, err := ns.NodePublishGuestVolume(context.Background(), staticPublishRequest(target, handle, tc.attributes))
			require.Equal(t, codes.InvalidArgument, status.Code(err), "got: %v", err)
			require.Contains(t, err.Error(), tc.errPart)
			require.Empty(t, m.MountPoints)
			require.NoDirExists(t, target)
		})
	}
}

// A static PersistentVolume that sets no location is mounted from its handle, when the handle is
// in the form CreateVolume mints. Attributes, when given, win, as they do upstream.
func TestNodePublishGuestVolumeDerivesLocationFromHandle(t *testing.T) {
	m := newFakeNFSMounter()
	ns := newTestNodeServer(t, m)
	handle := VolumeIDPrefix + "10.0.0.1#exports#team-a/data##"

	_, err := ns.NodePublishGuestVolume(context.Background(),
		staticPublishRequest(filepath.Join(t.TempDir(), "target"), handle, nil))
	require.NoError(t, err)
	require.Equal(t, "10.0.0.1:/exports/team-a/data", m.MountPoints[0].Device)
	require.Equal(t, []string{"nfsvers=4.1"}, m.MountPoints[0].Opts)

	// Other attributes, or empty location ones, do not stop the location being taken from the handle.
	_, err = ns.NodePublishGuestVolume(context.Background(), staticPublishRequest(
		filepath.Join(t.TempDir(), "target"), handle, map[string]string{"mountOptions": "hard", "Server": ""}))
	require.NoError(t, err)
	require.Equal(t, "10.0.0.1:/exports/team-a/data", m.MountPoints[1].Device)
	require.Equal(t, []string{"nfsvers=4.1", "hard"}, m.MountPoints[1].Opts)

	_, err = ns.NodePublishGuestVolume(context.Background(), staticPublishRequest(
		filepath.Join(t.TempDir(), "target"), handle, map[string]string{"server": "10.0.0.2", "share": "/other"}))
	require.NoError(t, err)
	require.Equal(t, "10.0.0.2:/other", m.MountPoints[2].Device)
}

// A dynamically provisioned volume is mounted exactly as before: from the context CreateVolume
// returned, which the PersistentVolume carries along with the provisioner's own attribute. Its
// handle names the same location.
func TestNodePublishGuestVolumeDynamicVolumeIsUnchanged(t *testing.T) {
	cs, _ := newTestControllerServer(t)
	resp, err := cs.CreateVolume(context.Background(), &csi.CreateVolumeRequest{
		Name: "pvc-1",
		Parameters: map[string]string{
			"server": "10.0.0.1", "share": "/exports", "subDir": "${pvc.metadata.namespace}/${pvc.metadata.name}",
			pvcNamespaceKey: "ns", pvcNameKey: "claim", pvNameKey: "pvc-1",
		},
		VolumeCapabilities: []*csi.VolumeCapability{mountCap(csi.VolumeCapability_AccessMode_MULTI_NODE_MULTI_WRITER)},
	})
	require.NoError(t, err)
	attributes := map[string]string{
		"storage.kubernetes.io/csiProvisionerIdentity": "1700000000000-1-csi.vsphere.vmware.com",
	}
	for k, v := range resp.Volume.VolumeContext {
		attributes[k] = v
	}

	viaVendored := newFakeNFSMounter()
	_, err = newTestNodeServer(t, viaVendored).NodePublishVolume(context.Background(),
		staticPublishRequest(filepath.Join(t.TempDir(), "target"), resp.Volume.VolumeId, attributes))
	require.NoError(t, err)

	for _, attrs := range []map[string]string{attributes, nil} {
		m := newFakeNFSMounter()
		_, err = newTestNodeServer(t, m).NodePublishGuestVolume(context.Background(),
			staticPublishRequest(filepath.Join(t.TempDir(), "target"), resp.Volume.VolumeId, attrs))
		require.NoError(t, err)
		require.Equal(t, "10.0.0.1:/exports/ns/claim", m.MountPoints[0].Device)
		require.Equal(t, viaVendored.MountPoints[0].Device, m.MountPoints[0].Device)
		require.Equal(t, viaVendored.MountPoints[0].Opts, m.MountPoints[0].Opts)
	}
}

// Unknown attributes are logged, not rejected: refusing one would turn any attribute this code does
// not know about into a failure to mount.
func TestNodePublishGuestVolumeIgnoresUnknownAttributes(t *testing.T) {
	m := newFakeNFSMounter()
	ns := newTestNodeServer(t, m)

	_, err := ns.NodePublishGuestVolume(context.Background(), staticPublishRequest(
		filepath.Join(t.TempDir(), "target"), VolumeIDPrefix+"pv-nfs-static",
		map[string]string{"server": "10.0.0.1", "share": "/exports", "subDirectory": "team-a"}))
	require.NoError(t, err)
	require.Equal(t, "10.0.0.1:/exports", m.MountPoints[0].Device)

	require.True(t, isKnownVolumeContextKey("storage.kubernetes.io/csiprovisioneridentity"))
	require.True(t, isKnownVolumeContextKey(pvcNameKey))
	require.True(t, isKnownVolumeContextKey(mountPermissionsField))
	require.False(t, isKnownVolumeContextKey("subdirectory"))
}

// Each poll is an Lstat over NFS, so the interval must be human-scale, not microseconds.
func TestWaitForPathNotExist(t *testing.T) {
	require.GreaterOrEqual(t, pathPollInterval, 100*time.Millisecond)

	missing := filepath.Join(t.TempDir(), "gone")
	require.NoError(t, waitForPathNotExistWithTimeout(missing, time.Second))

	existing := t.TempDir()
	start := time.Now()
	err := waitForPathNotExistWithTimeout(existing, 200*time.Millisecond)
	require.Error(t, err)
	require.Contains(t, err.Error(), "time out")
	require.Less(t, time.Since(start), 3*time.Second)

	// A path that disappears while waiting is noticed.
	dir := t.TempDir()
	target := filepath.Join(dir, "x")
	require.NoError(t, os.Mkdir(target, 0755))
	go func() {
		time.Sleep(50 * time.Millisecond)
		_ = os.Remove(target)
	}()
	require.NoError(t, waitForPathNotExistWithTimeout(target, 5*time.Second))
}

// The external-snapshotter decides whether a pre-provisioned snapshot is ready by listing it by
// ID, so an existing archive must be found, and a missing one or an invalid ID reported as absent.
func TestGetSnapshot(t *testing.T) {
	cs, workDir := newTestControllerServer(t)
	seedVolume(t, workDir, "pvc-src", "pvc-src")
	created, err := cs.CreateSnapshot(context.Background(),
		&csi.CreateSnapshotRequest{Name: "snap-1", SourceVolumeId: testSourceVolumeID})
	require.NoError(t, err)

	got, err := cs.GetSnapshot(context.Background(), created.Snapshot.SnapshotId, nil)
	require.NoError(t, err)
	require.NotNil(t, got)
	require.Equal(t, created.Snapshot.SnapshotId, got.SnapshotId)
	require.Equal(t, created.Snapshot.SizeBytes, got.SizeBytes)
	require.True(t, created.Snapshot.CreationTime.AsTime().Equal(got.CreationTime.AsTime()))
	require.True(t, got.ReadyToUse)

	missing := VolumeIDPrefix + "10.0.0.1#export#snap-2#snap-2#pvc-src"
	got, err = cs.GetSnapshot(context.Background(), missing, nil)
	require.NoError(t, err)
	require.Nil(t, got)

	mounter := cs.Driver.ns.mounter.(*fakeNFSMounter)
	mounter.ResetLog()
	got, err = cs.GetSnapshot(context.Background(), VolumeIDPrefix+"10.0.0.1#export#.#.#pvc-src", nil)
	require.NoError(t, err)
	require.Nil(t, got)
	require.Empty(t, mounter.GetLog(), "an invalid ID must not be mounted")
}
