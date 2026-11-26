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

package volume

import (
	"context"
	"crypto/tls"
	"os"
	"path/filepath"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/vmware/govmomi/find"
	"github.com/vmware/govmomi/object"
	"github.com/vmware/govmomi/session"
	"github.com/vmware/govmomi/simulator"
	"github.com/vmware/govmomi/vim25/types"

	cnsvsphere "sigs.k8s.io/vsphere-csi-driver/v3/pkg/common/cns-lib/vsphere"
	commontypes "sigs.k8s.io/vsphere-csi-driver/v3/pkg/common/types"
	"sigs.k8s.io/vsphere-csi-driver/v3/pkg/csi/service/logger"
)

// newSimulatorVirtualCenter connects a VirtualCenter to a fresh vcsim.
func newSimulatorVirtualCenter(t *testing.T) *cnsvsphere.VirtualCenter {
	t.Helper()
	confPath := filepath.Join(t.TempDir(), "csi-vsphere.conf")
	require.NoError(t, os.WriteFile(confPath, []byte(
		"[Global]\ncluster-id = \"listview-session-test\"\n\n"+
			"[VirtualCenter \"127.0.0.1\"]\nuser = \"user@vsphere.local\"\npassword = \"pass\"\n"+
			"datacenters = \"DC0\"\ninsecure-flag = \"true\"\n"), 0600))
	t.Setenv("VSPHERE_CSI_CONFIG", confPath)

	model := simulator.VPX()
	t.Cleanup(model.Remove)
	require.NoError(t, model.Create())
	model.Service.TLS = new(tls.Config)
	server := model.Service.NewServer()
	// vcsim ends a WaitForUpdatesEx only when its collector is cancelled or
	// destroyed, and the destroy of a collector whose session was logged out
	// fails, so its long poll outlives the client. Close would wait for it
	// forever; let it finish in the background instead.
	t.Cleanup(func() { go server.Close() })

	port, err := strconv.Atoi(server.URL.Port())
	require.NoError(t, err)
	vc := &cnsvsphere.VirtualCenter{
		Config: &cnsvsphere.VirtualCenterConfig{
			Host:     commontypes.NewFQDN(server.URL.Hostname()),
			Port:     port,
			Insecure: true,
			Username: "user", // simulator.DefaultLogin
			Password: "pass",
		},
		ClientMutex: &sync.Mutex{},
	}
	require.NoError(t, vc.Connect(context.Background()))
	return vc
}

// newReconfigureTask starts a task on the simulator that always succeeds. It
// uses the VirtualCenter's current client, which a re-login replaces.
func newReconfigureTask(ctx context.Context, t *testing.T, vc *cnsvsphere.VirtualCenter,
	vm types.ManagedObjectReference) *object.Task {
	t.Helper()
	task, err := object.NewVirtualMachine(vc.VimClient(), vm).Reconfigure(ctx,
		types.VirtualMachineConfigSpec{Annotation: time.Now().String()})
	require.NoError(t, err)
	return task
}

func requireTaskSucceeded(t *testing.T, ch chan TaskResult) {
	t.Helper()
	select {
	case result := <-ch:
		require.NoError(t, result.Err)
		require.NotNil(t, result.TaskInfo)
		require.Equal(t, types.TaskInfoStateSuccess, result.TaskInfo.State)
	case <-time.After(30 * time.Second):
		t.Fatal("no task update arrived through the listview")
	}
}

func requireEventually(t *testing.T, cond func() bool, msg string) {
	t.Helper()
	require.Eventually(t, cond, 30*time.Second, 50*time.Millisecond, msg)
}

// TestExpiredSessionRebuildsListView is the end-to-end check for dropping the
// per-call session check: with no UserSession read before ModifyListView, an
// expired session must still fail the call, mark the view for rebuild, and
// leave a listview that watches new tasks once the listener has logged in
// again -- for both registration and removal.
func TestExpiredSessionRebuildsListView(t *testing.T) {
	vc := newSimulatorVirtualCenter(t)
	ctx, cancel := context.WithCancel(logger.NewContextWithLogger(context.Background()))
	l, err := NewListViewImpl(ctx, vc)
	require.NoError(t, err)
	// Stop the listener before the simulator closes (cleanups run last in,
	// first out): its WaitForUpdatesEx long poll would otherwise keep a
	// connection open and server.Close waits for it. With its context ended,
	// the listener returns once its wait is cancelled.
	t.Cleanup(func() {
		cancel()
		l.SetListViewNotReady(ctx)
	})
	requireEventually(t, l.IsListViewReady, "listview did not become ready")

	vms, err := find.NewFinder(vc.VimClient()).VirtualMachineList(ctx, "*")
	require.NoError(t, err)
	require.NotEmpty(t, vms)
	vm := vms[0].Reference()

	// Baseline: a task is watched and its completion pushed.
	first := newReconfigureTask(ctx, t, vc, vm)
	ch := make(chan TaskResult, 1)
	require.NoError(t, l.AddTask(ctx, first.Reference(), ch))
	requireTaskSucceeded(t, ch)
	require.NoError(t, l.RemoveTask(ctx, first.Reference()))

	// expire logs the session out while holding one shared slot of viewMu.
	// The listener notices the logout on its own, but its recovery needs viewMu
	// exclusively, so until release is called it can neither mark the view not
	// ready nor log in again. The call under test therefore really reaches
	// vCenter with the expired session, instead of racing the listener.
	expire := func() (release func()) {
		t.Helper()
		require.NoError(t, l.viewMu.RLock(ctx))
		require.NoError(t, session.NewManager(vc.VimClient()).Logout(ctx))
		return l.viewMu.RUnlock
	}

	// An expired session fails the Add itself.
	pending := newReconfigureTask(ctx, t, vc, vm)
	release := expire()
	require.True(t, l.IsListViewReady(), "the view must still look usable when the call is made")
	err = l.AddTask(ctx, pending.Reference(), make(chan TaskResult, 1))
	require.ErrorIs(t, err, ErrListViewTaskAddition)
	require.False(t, l.IsListViewReady(), "the failed Add itself must mark the view for rebuild")
	_, ok := l.taskMap.Get(pending.Reference())
	require.False(t, ok, "a failed registration must not stay in the task map")
	release()

	// The listener logs in again and rebuilds the view; new tasks are
	// watched again, including the one whose registration failed.
	requireEventually(t, l.IsListViewReady, "listview was not rebuilt after the session expired")
	ch = make(chan TaskResult, 1)
	require.NoError(t, l.AddTask(ctx, pending.Reference(), ch))
	requireTaskSucceeded(t, ch)

	// An expired session fails the Remove too, and the view recovers again.
	release = expire()
	require.True(t, l.IsListViewReady())
	require.Error(t, l.RemoveTask(ctx, pending.Reference()))
	require.False(t, l.IsListViewReady(), "the failed Remove itself must mark the view for rebuild")
	release()
	requireEventually(t, l.IsListViewReady, "listview was not rebuilt after the session expired on removal")
	next := newReconfigureTask(ctx, t, vc, vm)
	ch = make(chan TaskResult, 1)
	require.NoError(t, l.AddTask(ctx, next.Reference(), ch))
	requireTaskSucceeded(t, ch)
	require.NoError(t, l.RemoveTask(ctx, next.Reference()))
}
