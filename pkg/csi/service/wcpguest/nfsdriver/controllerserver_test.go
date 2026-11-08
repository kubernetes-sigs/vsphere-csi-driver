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

func mountCap(mode csi.VolumeCapability_AccessMode_Mode) *csi.VolumeCapability {
	return &csi.VolumeCapability{
		AccessType: &csi.VolumeCapability_Mount{Mount: &csi.VolumeCapability_MountVolume{}},
		AccessMode: &csi.VolumeCapability_AccessMode{Mode: mode},
	}
}

// newTestControllerServer returns a ControllerServer whose internal mounts go to a fake
// mounter, with scratch space under a per-test temp dir. Nothing is really mounted, so tests
// pre-create the directories that a mount would have exposed.
func newTestControllerServer(t *testing.T) (*ControllerServer, string) {
	t.Helper()
	workDir := t.TempDir()
	d := NewDriver(&DriverOptions{
		DriverName:       DefaultDriverName,
		WorkingMountDir:  workDir,
		MountPermissions: 0755,
	})
	d.ns = NewNodeServer(d, newFakeNFSMounter())
	return d.ControllerServer(), workDir
}

func TestNewNFSVolumeValidation(t *testing.T) {
	base := func(extra map[string]string) map[string]string {
		p := map[string]string{paramServer: "10.0.0.1", paramShare: "/export"}
		for k, v := range extra {
			p[k] = v
		}
		return p
	}

	// withSubDir builds parameters with the given subDir and, optionally, one more key/value.
	withSubDir := func(subDir string, more ...string) map[string]string {
		extra := map[string]string{paramSubDir: subDir}
		for i := 0; i+1 < len(more); i += 2 {
			extra[more[i]] = more[i+1]
		}
		return base(extra)
	}

	tests := []struct {
		name    string
		volName string
		params  map[string]string
		errPart string // empty means success
	}{
		{name: "defaults subDir to the volume name", volName: "pvc-1", params: base(nil)},
		{name: "explicit subDir", volName: "pvc-1", params: withSubDir("tenant")},
		{name: "nested subDir", volName: "pvc-1", params: withSubDir("tenant/data")},
		{
			name: "template expands to a real directory", volName: "pvc-1",
			params: base(map[string]string{
				paramSubDir:     "${pvc.metadata.namespace}/${pvc.metadata.name}",
				pvcNamespaceKey: "ns", pvcNameKey: "claim",
			}),
		},
		{name: "subDir dot is the share root", volName: "pvc-1", params: withSubDir("."), errPart: "share root"},
		{name: "subDir slash is the share root", volName: "pvc-1", params: withSubDir("/"), errPart: "share root"},
		{name: "subDir dot-slash is the share root", volName: "pvc-1", params: withSubDir("./"), errPart: "share root"},
		{name: "subDir of repeated slashes is the share root", volName: "pvc-1", params: withSubDir("//"),
			errPart: "share root"},
		{name: "subDir traversal", volName: "pvc-1", params: withSubDir("../x"), errPart: "traversal"},
		{
			name: "template that expands to nothing", volName: "pvc-1",
			params:  withSubDir("${pvc.metadata.name}", pvcNameKey, ""),
			errPart: "share root",
		},
		{
			name: "template whose value is a root alias", volName: "pvc-1",
			params:  withSubDir("${pvc.metadata.name}", pvcNameKey, "."),
			errPart: "share root",
		},
		{name: "hash in subDir", volName: "pvc-1", params: withSubDir("tenant#data"), errPart: "must not contain"},
		{
			name: "hash in subDir via template", volName: "pvc-1",
			params:  withSubDir("${pvc.metadata.name}", pvcNameKey, "a#b"),
			errPart: "must not contain",
		},
		{name: "hash in share", volName: "pvc-1", params: base(map[string]string{paramShare: "/ex#port"}),
			errPart: "must not contain"},
		{name: "hash in server", volName: "pvc-1", params: base(map[string]string{paramServer: "10.0.0.1#x"}),
			errPart: "must not contain"},
		{name: "hash in the volume name", volName: "pvc#1", params: base(nil), errPart: "must not contain"},
		{name: "volume name that is a root alias", volName: ".", params: base(nil), errPart: "share root"},
		{name: "missing server", volName: "pvc-1", params: map[string]string{paramShare: "/export"},
			errPart: "required"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			vol, err := newNFSVolume(tc.volName, 0, tc.params, "")
			if tc.errPart != "" {
				require.Error(t, err)
				require.Contains(t, err.Error(), tc.errPart)
				require.Nil(t, vol)
				return
			}
			require.NoError(t, err)

			// Whatever was accepted must decode back to the same directories on delete.
			decoded, err := getNfsVolFromID(vol.id)
			require.NoError(t, err)
			// IDs carry the slash-trimmed form of each component.
			require.Equal(t, strings.Trim(vol.server, "/"), decoded.server)
			require.Equal(t, strings.Trim(vol.baseDir, "/"), decoded.baseDir)
			require.Equal(t, strings.Trim(vol.subDir, "/"), decoded.subDir)
			require.Equal(t, vol.uuid, decoded.uuid)
		})
	}
}

func TestGetNfsVolFromID(t *testing.T) {
	tests := []struct {
		name    string
		id      string
		want    nfsVolume
		errPart string
	}{
		{name: "subDir defaulted to the volume name", id: "10.0.0.1#export#pvc-1##",
			want: nfsVolume{server: "10.0.0.1", baseDir: "export", subDir: "pvc-1"}},
		{name: "explicit subDir with uuid and onDelete", id: "10.0.0.1#export#tenant#pvc-1#archive",
			want: nfsVolume{server: "10.0.0.1", baseDir: "export", subDir: "tenant", uuid: "pvc-1", onDelete: "archive"}},
		{name: "legacy slash separated id", id: "10.0.0.1/export/pvc-1",
			want: nfsVolume{server: "10.0.0.1", baseDir: "export", subDir: "pvc-1"}},
		{name: "pvCSI prefix is not part of the server", id: VolumeIDPrefix + "10.0.0.1#export#pvc-1##",
			want: nfsVolume{server: "10.0.0.1", baseDir: "export", subDir: "pvc-1"}},
		{name: "subDir dot", id: "10.0.0.1#export#.#pvc-1", errPart: "share root"},
		{name: "empty subDir", id: "10.0.0.1#export##pvc-1#", errPart: "share root"},
		{name: "subDir slash", id: "10.0.0.1#export#/#pvc-1", errPart: "share root"},
		{name: "uuid dot", id: "10.0.0.1#export#tenant#.#", errPart: "invalid uuid"},
		{name: "subDir traversal", id: "10.0.0.1#export#../x#pvc-1", errPart: "traversal"},
		{name: "baseDir traversal", id: "10.0.0.1#../export#pvc-1##", errPart: "traversal"},
		{name: "too short", id: "pvc-1", errPart: "could not split"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got, err := getNfsVolFromID(tc.id)
			if tc.errPart != "" {
				require.Error(t, err)
				require.Contains(t, err.Error(), tc.errPart)
				return
			}
			require.NoError(t, err)
			require.Equal(t, tc.want.server, got.server)
			require.Equal(t, tc.want.baseDir, got.baseDir)
			require.Equal(t, tc.want.subDir, got.subDir)
			require.Equal(t, tc.want.uuid, got.uuid)
			require.Equal(t, tc.want.onDelete, got.onDelete)
		})
	}
}

func TestNewNFSSnapshotRejectsHash(t *testing.T) {
	vol := &nfsVolume{server: "10.0.0.1", baseDir: "export", subDir: "pvc-1"}

	snap, err := newNFSSnapshot("snap-1", nil, vol)
	require.NoError(t, err)
	require.NotEmpty(t, snap.id)

	_, err = newNFSSnapshot("snap#1", nil, vol)
	require.Error(t, err)
	require.Contains(t, err.Error(), "must not contain")

	_, err = newNFSSnapshot("snap-1", map[string]string{paramShare: "ex#port"}, vol)
	require.Error(t, err)
}

func TestIsValidVolumeCapabilities(t *testing.T) {
	rwx := csi.VolumeCapability_AccessMode_MULTI_NODE_MULTI_WRITER

	tests := []struct {
		name string
		caps []*csi.VolumeCapability
		ok   bool
	}{
		{name: "valid mount capability", caps: []*csi.VolumeCapability{mountCap(rwx)}, ok: true},
		{name: "several valid capabilities", ok: true, caps: []*csi.VolumeCapability{
			mountCap(rwx), mountCap(csi.VolumeCapability_AccessMode_SINGLE_NODE_WRITER)}},
		{name: "empty list", caps: nil},
		{name: "nil entry", caps: []*csi.VolumeCapability{nil}},
		{name: "empty entry", caps: []*csi.VolumeCapability{{}}},
		{name: "block access type", caps: []*csi.VolumeCapability{{
			AccessType: &csi.VolumeCapability_Block{Block: &csi.VolumeCapability_BlockVolume{}},
			AccessMode: &csi.VolumeCapability_AccessMode{Mode: rwx},
		}}},
		{name: "no access type", caps: []*csi.VolumeCapability{{
			AccessMode: &csi.VolumeCapability_AccessMode{Mode: rwx},
		}}},
		{name: "no access mode", caps: []*csi.VolumeCapability{{
			AccessType: &csi.VolumeCapability_Mount{Mount: &csi.VolumeCapability_MountVolume{}},
		}}},
		{name: "unknown access mode", caps: []*csi.VolumeCapability{
			mountCap(csi.VolumeCapability_AccessMode_UNKNOWN)}},
		{name: "one bad entry among good ones", caps: []*csi.VolumeCapability{mountCap(rwx), nil}},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			err := isValidVolumeCapabilities(tc.caps)
			if tc.ok {
				require.NoError(t, err)
			} else {
				require.Error(t, err)
			}
		})
	}
}

// Requests that must be refused before anything is mounted or created on the NFS server.
func TestCreateVolumeRejectsUnsafeRequests(t *testing.T) {
	cs, workDir := newTestControllerServer(t)
	rwx := csi.VolumeCapability_AccessMode_MULTI_NODE_MULTI_WRITER
	params := func(subDir string) map[string]string {
		return map[string]string{paramServer: "10.0.0.1", paramShare: "/export", paramSubDir: subDir}
	}

	tests := []struct {
		name string
		req  *csi.CreateVolumeRequest
	}{
		{"subDir is the share root", &csi.CreateVolumeRequest{
			Name: "pvc-1", Parameters: params("."), VolumeCapabilities: []*csi.VolumeCapability{mountCap(rwx)}}},
		{"hash in subDir", &csi.CreateVolumeRequest{
			Name: "pvc-1", Parameters: params("tenant#data"), VolumeCapabilities: []*csi.VolumeCapability{mountCap(rwx)}}},
		{"nil capability", &csi.CreateVolumeRequest{
			Name: "pvc-1", Parameters: params("tenant"), VolumeCapabilities: []*csi.VolumeCapability{nil}}},
		{"no access mode", &csi.CreateVolumeRequest{
			Name: "pvc-1", Parameters: params("tenant"), VolumeCapabilities: []*csi.VolumeCapability{{
				AccessType: &csi.VolumeCapability_Mount{Mount: &csi.VolumeCapability_MountVolume{}}}}}},
		{"unknown access mode", &csi.CreateVolumeRequest{
			Name: "pvc-1", Parameters: params("tenant"),
			VolumeCapabilities: []*csi.VolumeCapability{mountCap(csi.VolumeCapability_AccessMode_UNKNOWN)}}},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			resp, err := cs.CreateVolume(context.Background(), tc.req)
			require.Nil(t, resp)
			require.Equal(t, codes.InvalidArgument, status.Code(err), "got: %v", err)
		})
	}

	entries, err := os.ReadDir(workDir)
	require.NoError(t, err)
	require.Empty(t, entries, "a rejected request must not touch the working directory")
}

func TestValidateVolumeCapabilitiesRejectsMalformedCaps(t *testing.T) {
	cs, _ := newTestControllerServer(t)

	_, err := cs.ValidateVolumeCapabilities(context.Background(), &csi.ValidateVolumeCapabilitiesRequest{
		VolumeId:           "10.0.0.1#export#pvc-1##",
		VolumeCapabilities: []*csi.VolumeCapability{nil},
	})
	require.Equal(t, codes.InvalidArgument, status.Code(err))

	resp, err := cs.ValidateVolumeCapabilities(context.Background(), &csi.ValidateVolumeCapabilitiesRequest{
		VolumeId: "10.0.0.1#export#pvc-1##",
		VolumeCapabilities: []*csi.VolumeCapability{
			mountCap(csi.VolumeCapability_AccessMode_MULTI_NODE_MULTI_WRITER)},
	})
	require.NoError(t, err)
	require.NotNil(t, resp.Confirmed)
}

// seedVolume creates the directory a mounted export would expose for the volume at the path
// DeleteVolume works on (workingDir/<mountDir>/<subDir>) and puts a file in it.
func seedVolume(t *testing.T, workDir, mountDir, subDir string) string {
	t.Helper()
	dir := filepath.Join(workDir, mountDir, subDir)
	require.NoError(t, os.MkdirAll(dir, 0755))
	require.NoError(t, os.WriteFile(filepath.Join(dir, "data.txt"), []byte("x"), 0644))
	return dir
}

func resetDeletionCache(t *testing.T, cs *ControllerServer) {
	t.Helper()
	c, err := NewTimedCache(time.Minute, func(string) (interface{}, error) { return nil, nil }, false)
	require.NoError(t, err)
	cs.Driver.volDeletionCache = c
}

func TestDeleteVolumeRemovesDirectoryAndIsRepeatable(t *testing.T) {
	cs, workDir := newTestControllerServer(t)
	volDir := seedVolume(t, workDir, "pvc-1", "pvc-1")
	id := "10.0.0.1#export#pvc-1##"

	_, err := cs.DeleteVolume(context.Background(), &csi.DeleteVolumeRequest{VolumeId: id})
	require.NoError(t, err)
	require.NoDirExists(t, volDir)

	// Retry served from the deletion cache.
	_, err = cs.DeleteVolume(context.Background(), &csi.DeleteVolumeRequest{VolumeId: id})
	require.NoError(t, err)

	// Retry after the cache expired or the controller restarted: the directory is already gone.
	resetDeletionCache(t, cs)
	_, err = cs.DeleteVolume(context.Background(), &csi.DeleteVolumeRequest{VolumeId: id})
	require.NoError(t, err)
}

func TestDeleteVolumeNestedSubDirIsRepeatable(t *testing.T) {
	cs, workDir := newTestControllerServer(t)
	// Volume ID for subDir "ns/claim": the scratch mount dir is named after the uuid.
	volDir := seedVolume(t, workDir, "pvc-1", "ns/claim")
	id := "10.0.0.1#export#ns/claim#pvc-1#"

	_, err := cs.DeleteVolume(context.Background(), &csi.DeleteVolumeRequest{VolumeId: id})
	require.NoError(t, err)
	require.NoDirExists(t, volDir)
	// The now-empty parent "ns" is cleaned up too.
	require.NoDirExists(t, filepath.Join(workDir, "pvc-1", "ns"))

	// With the parent already removed, a repeated delete must still succeed.
	resetDeletionCache(t, cs)
	_, err = cs.DeleteVolume(context.Background(), &csi.DeleteVolumeRequest{VolumeId: id})
	require.NoError(t, err)
}

func TestDeleteVolumeArchiveIsIdempotent(t *testing.T) {
	cs, workDir := newTestControllerServer(t)
	volDir := seedVolume(t, workDir, "pvc-1", "pvc-1")
	archived := filepath.Join(workDir, "pvc-1", "archived-pvc-1")
	id := "10.0.0.1#export#pvc-1##archive"

	_, err := cs.DeleteVolume(context.Background(), &csi.DeleteVolumeRequest{VolumeId: id})
	require.NoError(t, err)
	require.NoDirExists(t, volDir)
	require.FileExists(t, filepath.Join(archived, "data.txt"))

	// First attempt succeeded but its response was lost; the retry arrives with an empty
	// cache. It must succeed and leave the archive alone, even with removeArchivedVolumePath
	// set, which would otherwise delete the archive and then fail renaming a missing source.
	resetDeletionCache(t, cs)
	cs.Driver.removeArchivedVolumePath = true
	_, err = cs.DeleteVolume(context.Background(), &csi.DeleteVolumeRequest{VolumeId: id})
	require.NoError(t, err)
	require.FileExists(t, filepath.Join(archived, "data.txt"))
}

func TestDeleteVolumeRetainLeavesData(t *testing.T) {
	cs, workDir := newTestControllerServer(t)
	volDir := seedVolume(t, workDir, "pvc-1", "pvc-1")

	_, err := cs.DeleteVolume(context.Background(),
		&csi.DeleteVolumeRequest{VolumeId: "10.0.0.1#export#pvc-1##retain"})
	require.NoError(t, err)
	require.FileExists(t, filepath.Join(volDir, "data.txt"))
}

// An ID that decodes to the share root must be treated as "does not exist" without deleting
// anything: with subDir "." the volume path equals the whole mounted export.
func TestDeleteVolumeIgnoresShareRootIDs(t *testing.T) {
	for _, id := range []string{
		"10.0.0.1#export#.#pvc-1",
		"10.0.0.1#export##pvc-1#",
		"10.0.0.1#export#/#pvc-1",
		VolumeIDPrefix + "10.0.0.1#export#.#pvc-1",
	} {
		t.Run(id, func(t *testing.T) {
			cs, workDir := newTestControllerServer(t)
			// Another volume's data, on the same export.
			other := seedVolume(t, workDir, "pvc-1", "other-volume")

			resp, err := cs.DeleteVolume(context.Background(), &csi.DeleteVolumeRequest{VolumeId: id})
			require.NoError(t, err)
			require.NotNil(t, resp)
			require.FileExists(t, filepath.Join(other, "data.txt"))
		})
	}
}

func TestArchiveVolumeDirMissingSourceKeepsArchive(t *testing.T) {
	cs, workDir := newTestControllerServer(t)
	cs.Driver.removeArchivedVolumePath = true
	archived := seedVolume(t, workDir, "pvc-1", "archived-pvc-1")
	vol := &nfsVolume{server: "10.0.0.1", baseDir: "export", subDir: "pvc-1", uuid: "pvc-1"}

	require.NoError(t, archiveVolumeDir(cs.Driver, vol, getInternalVolumePath(workDir, vol)))
	require.FileExists(t, filepath.Join(archived, "data.txt"))
}

func TestArchiveVolumeDirMovesSource(t *testing.T) {
	cs, workDir := newTestControllerServer(t)
	cs.Driver.removeArchivedVolumePath = true
	src := seedVolume(t, workDir, "pvc-1", "pvc-1")
	// A stale archive from an earlier volume of the same name is replaced.
	stale := seedVolume(t, workDir, "pvc-1", "archived-pvc-1")
	require.NoError(t, os.WriteFile(filepath.Join(stale, "stale.txt"), []byte("old"), 0644))
	vol := &nfsVolume{server: "10.0.0.1", baseDir: "export", subDir: "pvc-1", uuid: "pvc-1"}

	require.NoError(t, archiveVolumeDir(cs.Driver, vol, src))
	require.NoDirExists(t, src)
	require.FileExists(t, filepath.Join(workDir, "pvc-1", "archived-pvc-1", "data.txt"))
	require.NoFileExists(t, filepath.Join(workDir, "pvc-1", "archived-pvc-1", "stale.txt"))
}
