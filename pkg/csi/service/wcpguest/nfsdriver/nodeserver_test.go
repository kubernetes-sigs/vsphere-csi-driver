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
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/container-storage-interface/spec/lib/go/csi"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	mount "k8s.io/mount-utils"
)

// fakeNFSMounter wraps mount.FakeMounter. A real NFS mount would cover the mkdir-ed target
// with the export's own permissions, so Mount opens the target up the same way; mountHook
// lets a test hold the mount in flight; unmounted records Unmount calls.
type fakeNFSMounter struct {
	*mount.FakeMounter
	mountHook func(source, target string) error
	unmounted chan string
}

func newFakeNFSMounter() *fakeNFSMounter {
	return &fakeNFSMounter{FakeMounter: mount.NewFakeMounter(nil), unmounted: make(chan string, 8)}
}

func (m *fakeNFSMounter) Mount(source, target, fstype string, options []string) error {
	if m.mountHook != nil {
		if err := m.mountHook(source, target); err != nil {
			return err
		}
	}
	_ = os.Chmod(target, 0755)
	return m.FakeMounter.Mount(source, target, fstype, options)
}

func (m *fakeNFSMounter) Unmount(target string) error {
	select {
	case m.unmounted <- target:
	default:
	}
	return m.FakeMounter.Unmount(target)
}

func newTestNodeServer(t *testing.T, m mount.Interface) *NodeServer {
	t.Helper()
	d := NewDriver(&DriverOptions{
		DriverName:       DefaultDriverName,
		WorkingMountDir:  t.TempDir(),
		MountPermissions: 0755,
	})
	d.ns = NewNodeServer(d, m)
	return d.ns
}

func publishRequest(target string) *csi.NodePublishVolumeRequest {
	return &csi.NodePublishVolumeRequest{
		VolumeId:         "vol-1",
		TargetPath:       target,
		VolumeCapability: mountCap(csi.VolumeCapability_AccessMode_MULTI_NODE_MULTI_WRITER),
		VolumeContext:    map[string]string{paramServer: "10.0.0.1", paramShare: "/export"},
	}
}

func setMountTimeout(t *testing.T, d time.Duration) {
	t.Helper()
	old := mountTimeout
	mountTimeout = d
	t.Cleanup(func() { mountTimeout = old })
}

func TestNodePublishVolumeMounts(t *testing.T) {
	m := newFakeNFSMounter()
	ns := newTestNodeServer(t, m)
	target := t.TempDir() + "/target"

	_, err := ns.NodePublishVolume(context.Background(), publishRequest(target))
	require.NoError(t, err)
	require.Len(t, m.MountPoints, 1)
	require.Equal(t, "10.0.0.1:/export", m.MountPoints[0].Device)

	// The lock is released after a normal publish, so another call is not Aborted.
	_, err = ns.NodePublishVolume(context.Background(), publishRequest(target))
	require.NoError(t, err)
}

// A mount that outlives the timeout cannot be cancelled. The volume lock must stay held until
// it really finishes, so a retry cannot race it, and a late success must be unmounted because
// the caller was told the mount failed.
func TestNodePublishVolumeLateMountIsUnmountedAndLockHeld(t *testing.T) {
	setMountTimeout(t, 50*time.Millisecond)
	release := make(chan struct{})
	m := newFakeNFSMounter()
	m.mountHook = func(_, _ string) error {
		<-release
		return nil
	}
	ns := newTestNodeServer(t, m)
	target := t.TempDir() + "/target"
	lockKey := fmt.Sprintf("%s-%s", "vol-1", target)

	_, err := ns.NodePublishVolume(context.Background(), publishRequest(target))
	require.Equal(t, codes.Internal, status.Code(err), "got: %v", err)
	require.Contains(t, err.Error(), "timeout")

	// Still mounting: the lock is held, so a retry is turned away instead of racing.
	_, err = ns.NodePublishVolume(context.Background(), publishRequest(target))
	require.Equal(t, codes.Aborted, status.Code(err), "got: %v", err)
	require.False(t, ns.Driver.volumeLocks.TryAcquire(lockKey))

	close(release)

	select {
	case got := <-m.unmounted:
		require.Equal(t, target, got)
	case <-time.After(5 * time.Second):
		t.Fatal("late mount was never unmounted")
	}
	require.Eventually(t, func() bool {
		if ns.Driver.volumeLocks.TryAcquire(lockKey) {
			ns.Driver.volumeLocks.Release(lockKey)
			return true
		}
		return false
	}, 5*time.Second, 10*time.Millisecond, "lock must be released once the mount has finished")
}

func TestNodePublishVolumeLateMountFailureReleasesLock(t *testing.T) {
	setMountTimeout(t, 50*time.Millisecond)
	release := make(chan struct{})
	m := newFakeNFSMounter()
	m.mountHook = func(_, _ string) error {
		<-release
		return fmt.Errorf("mount failed")
	}
	ns := newTestNodeServer(t, m)
	target := t.TempDir() + "/target"
	lockKey := fmt.Sprintf("%s-%s", "vol-1", target)

	_, err := ns.NodePublishVolume(context.Background(), publishRequest(target))
	require.Equal(t, codes.Internal, status.Code(err))

	close(release)
	require.Eventually(t, func() bool {
		if ns.Driver.volumeLocks.TryAcquire(lockKey) {
			ns.Driver.volumeLocks.Release(lockKey)
			return true
		}
		return false
	}, 5*time.Second, 10*time.Millisecond)
	require.Empty(t, m.unmounted, "nothing was mounted, so nothing should be unmounted")
}

func TestNodePublishVolumeMountErrorReleasesLock(t *testing.T) {
	m := newFakeNFSMounter()
	m.mountHook = func(_, _ string) error { return fmt.Errorf("boom") }
	ns := newTestNodeServer(t, m)
	target := t.TempDir() + "/target"

	_, err := ns.NodePublishVolume(context.Background(), publishRequest(target))
	require.Equal(t, codes.Internal, status.Code(err))

	// A prompt (non-timeout) failure releases the lock straight away.
	require.True(t, ns.Driver.volumeLocks.TryAcquire(fmt.Sprintf("%s-%s", "vol-1", target)))
}
