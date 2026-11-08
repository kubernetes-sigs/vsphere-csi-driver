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
