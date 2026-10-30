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

package cnsregistervolume

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	cnstypes "github.com/vmware/govmomi/cns/types"
	vim25types "github.com/vmware/govmomi/vim25/types"
	apitypes "k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/event"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	volumes "sigs.k8s.io/vsphere-csi-driver/v3/pkg/common/cns-lib/volume"
	cnsvsphere "sigs.k8s.io/vsphere-csi-driver/v3/pkg/common/cns-lib/vsphere"
)

// fakeListView records registrations and lets a test deliver a task result the
// same way the real listview's WaitForUpdates loop would.
type fakeListView struct {
	mu        sync.Mutex
	channels  map[string]chan volumes.TaskResult
	removed   []string
	marked    []string
	addErr    error
	removeErr error
	addCalls  int
}

func newFakeListView() *fakeListView {
	return &fakeListView{channels: make(map[string]chan volumes.TaskResult)}
}

func (f *fakeListView) AddTask(ctx context.Context, taskMoRef vim25types.ManagedObjectReference,
	ch chan volumes.TaskResult) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.addCalls++
	if f.addErr != nil {
		return f.addErr
	}
	f.channels[taskMoRef.Value] = ch
	return nil
}

func (f *fakeListView) RemoveTask(ctx context.Context, taskMoRef vim25types.ManagedObjectReference) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.removed = append(f.removed, taskMoRef.Value)
	return f.removeErr
}

func (f *fakeListView) MarkTaskForDeletion(ctx context.Context,
	taskMoRef vim25types.ManagedObjectReference) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.marked = append(f.marked, taskMoRef.Value)
	return nil
}

func (f *fakeListView) ResetVirtualCenter(ctx context.Context, vc *cnsvsphere.VirtualCenter) {}
func (f *fakeListView) IsListViewReady() bool                                                { return true }
func (f *fakeListView) SetListViewNotReady(ctx context.Context)                              {}

// deliver pushes a result onto a registered task's channel, imitating
// processTaskUpdate.
func (f *fakeListView) deliver(t *testing.T, taskID string, result volumes.TaskResult) {
	t.Helper()
	f.mu.Lock()
	ch, ok := f.channels[taskID]
	f.mu.Unlock()
	require.True(t, ok, "task %s was never registered with the listview", taskID)
	ch <- result
}

func (f *fakeListView) removedTasks() []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]string{}, f.removed...)
}

func taskRef(id string) vim25types.ManagedObjectReference {
	return vim25types.ManagedObjectReference{Type: "Task", Value: id}
}

func crKey(name string) apitypes.NamespacedName {
	return apitypes.NamespacedName{Name: name, Namespace: "test-ns"}
}

// waitFor polls cond until it holds or the deadline passes. Used instead of a
// fixed sleep so the tests do not depend on goroutine scheduling speed.
func waitFor(t *testing.T, cond func() bool, msg string) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatalf("timed out waiting for %s", msg)
}

// TestTrackerDoesNotBlockCaller is the whole point of the type: registering a
// task must return immediately rather than waiting for the task to finish, so
// the reconciler worker is released.
func TestTrackerDoesNotBlockCaller(t *testing.T) {
	lv := newFakeListView()
	tracker := newCnsTaskTracker(lv, nil, make(chan event.GenericEvent, 10), 40)

	done := make(chan struct{})
	go func() {
		defer close(done)
		assert.NoError(t, tracker.watch(context.Background(), crKey("cr-1"), taskRef("task-1"), "vol-1"))
	}()

	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("watch blocked waiting for the task; the reconciler worker would be held")
	}

	// The task has not completed, so there is no outcome yet and the check for
	// one must not make any call.
	_, ok := tracker.outcome("task-1")
	assert.False(t, ok, "a running task must not report an outcome")
	assert.True(t, tracker.isWatching("task-1"))
}

// TestTrackerRecordsResultAndWakesCR verifies the push path end to end: the
// listview delivers, the tracker decodes and stores, and the CR is woken.
func TestTrackerRecordsResultAndWakesCR(t *testing.T) {
	lv := newFakeListView()
	events := make(chan event.GenericEvent, 10)
	tracker := newCnsTaskTracker(lv, nil, events, 40)

	require.NoError(t, tracker.watch(context.Background(), crKey("cr-1"), taskRef("task-1"), "vol-1"))

	// A session-style failure: TaskInfo nil with an error.
	lv.deliver(t, "task-1", volumes.TaskResult{TaskInfo: nil, Err: errors.New("vc session lost")})

	waitFor(t, func() bool {
		_, ok := tracker.outcome("task-1")
		return ok
	}, "the task outcome to be recorded")

	out, ok := tracker.outcome("task-1")
	require.True(t, ok)
	assert.Equal(t, volumes.CreateVolumeTaskUnknown, out.state)

	select {
	case ev := <-events:
		assert.Equal(t, "cr-1", ev.Object.GetName())
		assert.Equal(t, "test-ns", ev.Object.GetNamespace())
	case <-time.After(2 * time.Second):
		t.Fatal("the CR was never woken after its task completed")
	}
}

// TestTrackerNeverReportsUnknownAsFailed guards the rule that a lost vCenter
// session is not a failed task. If this regressed, a transient blip would send
// every in-flight CR back to resubmit a second CreateVolume against a disk
// whose first task is still running.
func TestTrackerNeverReportsUnknownAsFailed(t *testing.T) {
	lv := newFakeListView()
	tracker := newCnsTaskTracker(lv, nil, make(chan event.GenericEvent, 10), 40)

	require.NoError(t, tracker.watch(context.Background(), crKey("cr-1"), taskRef("task-1"), "vol-1"))
	// This is exactly what reportErrorOnAllPendingTasks pushes to every pending
	// task when the listview loses its session.
	lv.deliver(t, "task-1", volumes.TaskResult{TaskInfo: nil, Err: errors.New("connection reset")})

	waitFor(t, func() bool { _, ok := tracker.outcome("task-1"); return ok }, "outcome")
	out, _ := tracker.outcome("task-1")
	assert.Equal(t, volumes.CreateVolumeTaskUnknown, out.state)
	assert.NotEqual(t, volumes.CreateVolumeTaskFailed, out.state,
		"a transport failure must never be reported as a failed CNS task")
}

// TestTrackerRemovesTaskOnlyAfterResult verifies the ordering the listview
// requires: processTaskUpdate drops updates for unregistered tasks, so the
// registration must outlive the wait.
func TestTrackerRemovesTaskOnlyAfterResult(t *testing.T) {
	lv := newFakeListView()
	tracker := newCnsTaskTracker(lv, nil, make(chan event.GenericEvent, 10), 40)

	require.NoError(t, tracker.watch(context.Background(), crKey("cr-1"), taskRef("task-1"), "vol-1"))
	assert.Empty(t, lv.removedTasks(), "the task must stay registered while it is running")

	lv.deliver(t, "task-1", volumes.TaskResult{TaskInfo: nil, Err: errors.New("done")})
	waitFor(t, func() bool { return len(lv.removedTasks()) == 1 }, "the task to be removed after its result")
	assert.Equal(t, []string{"task-1"}, lv.removedTasks())
}

// TestTrackerWatchIsIdempotent verifies a repeated reconcile cannot start a
// second watcher, which would race two goroutines on one result.
func TestTrackerWatchIsIdempotent(t *testing.T) {
	lv := newFakeListView()
	tracker := newCnsTaskTracker(lv, nil, make(chan event.GenericEvent, 10), 40)

	for i := 0; i < 5; i++ {
		require.NoError(t, tracker.watch(context.Background(), crKey("cr-1"), taskRef("task-1"), "vol-1"))
	}

	lv.mu.Lock()
	calls := lv.addCalls
	lv.mu.Unlock()
	assert.Equal(t, 1, calls, "the task must be registered with the listview exactly once")
}

// TestTrackerWatchReleasesWatchFlagOnAddFailure verifies that a listview that
// rejects the task does not leave the tracker believing it is being watched,
// which would wedge the CR forever.
func TestTrackerWatchReleasesWatchFlagOnAddFailure(t *testing.T) {
	lv := newFakeListView()
	lv.addErr = errors.New("listview not ready")
	tracker := newCnsTaskTracker(lv, nil, make(chan event.GenericEvent, 10), 40)

	err := tracker.watch(context.Background(), crKey("cr-1"), taskRef("task-1"), "vol-1")
	assert.Error(t, err)
	assert.False(t, tracker.isWatching("task-1"),
		"a failed registration must not leave the task marked as watched")
}

// TestTrackerCapLimitsInFlightTasks verifies the replacement for the implicit
// throttle the 40 blocking workers provide today.
func TestTrackerCapLimitsInFlightTasks(t *testing.T) {
	tracker := newCnsTaskTracker(newFakeListView(), nil, make(chan event.GenericEvent, 10), 3)

	assert.True(t, tracker.tryAcquireSlot(crKey("cr-1")))
	assert.True(t, tracker.tryAcquireSlot(crKey("cr-2")))
	assert.True(t, tracker.tryAcquireSlot(crKey("cr-3")))
	assert.False(t, tracker.tryAcquireSlot(crKey("cr-4")), "the cap must be enforced")
	assert.Equal(t, 3, tracker.inFlight())

	// Releasing one admits the next.
	tracker.releaseSlot(crKey("cr-2"))
	assert.Equal(t, 2, tracker.inFlight())
	assert.True(t, tracker.tryAcquireSlot(crKey("cr-4")))
}

// TestTrackerSlotIsPerCRNotPerCall verifies a CR reconciled repeatedly reuses
// its slot instead of consuming the cap.
func TestTrackerSlotIsPerCRNotPerCall(t *testing.T) {
	tracker := newCnsTaskTracker(newFakeListView(), nil, make(chan event.GenericEvent, 10), 2)

	for i := 0; i < 10; i++ {
		assert.True(t, tracker.tryAcquireSlot(crKey("cr-1")))
	}
	assert.Equal(t, 1, tracker.inFlight(), "one CR must never hold more than one slot")
	assert.True(t, tracker.tryAcquireSlot(crKey("cr-2")))
	assert.False(t, tracker.tryAcquireSlot(crKey("cr-3")))
}

// TestTrackerForgetFreesSlotAndResult verifies the reconciler can consume an
// outcome and return the capacity.
func TestTrackerForgetFreesSlotAndResult(t *testing.T) {
	lv := newFakeListView()
	tracker := newCnsTaskTracker(lv, nil, make(chan event.GenericEvent, 10), 2)

	require.NoError(t, tracker.watch(context.Background(), crKey("cr-1"), taskRef("task-1"), "vol-1"))
	lv.deliver(t, "task-1", volumes.TaskResult{TaskInfo: nil, Err: errors.New("done")})
	waitFor(t, func() bool { _, ok := tracker.outcome("task-1"); return ok }, "outcome")

	assert.Equal(t, 1, tracker.inFlight())
	tracker.forget(crKey("cr-1"), "task-1")

	_, ok := tracker.outcome("task-1")
	assert.False(t, ok, "a consumed outcome must be dropped")
	assert.Equal(t, 0, tracker.inFlight(), "forget must release the capacity slot")
}

// TestTrackerConcurrentWatchersAreSafe exercises the tracker the way a burst
// would, to catch lock misuse under -race.
func TestTrackerConcurrentWatchersAreSafe(t *testing.T) {
	lv := newFakeListView()
	tracker := newCnsTaskTracker(lv, nil, make(chan event.GenericEvent, 200), 100)

	var wg sync.WaitGroup
	for i := 0; i < 50; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			cr := crKey("cr-" + string(rune('a'+i%26)) + string(rune('0'+i/26)))
			id := "task-" + string(rune('a'+i%26)) + string(rune('0'+i/26))
			if tracker.tryAcquireSlot(cr) {
				_ = tracker.watch(context.Background(), cr, taskRef(id), "vol")
			}
		}(i)
	}
	wg.Wait()
	assert.LessOrEqual(t, tracker.inFlight(), 100)
}

// seedOutcome injects a completed task outcome. Same package, so tests can
// stage a result without driving a real listview.
func seedOutcome(tr *cnsTaskTracker, taskID string, out *cnsTaskOutcome) {
	tr.mu.Lock()
	defer tr.mu.Unlock()
	tr.results[taskID] = out
}

// fakeCnsTaskID is the task reference handed out by wireAsyncCnsTaskSuccess.
const fakeCnsTaskID = "task-fake-0001"

// wireAsyncCnsTaskSuccess makes the async CNS path resolve through the test's
// own mock volume manager instead of a real vCenter.
//
// Registration is now a multi-pass flow: one reconcile submits the CNS task and
// releases the worker, a later one picks up the result. This helper keeps the
// existing mockVolumeManager.createVolumeFunc as the source of truth for what
// CNS "returns" -- it calls that mock at submit time and stages the result for
// the next pass -- so tests keep asserting on their own fixtures rather than on
// anything invented here.
//
// Returns a restore function; call it with defer.
func wireAsyncCnsTaskSuccess(r *ReconcileCnsRegisterVolume) func() {
	r.taskTracker = newCnsTaskTracker(newFakeListView(), nil,
		make(chan event.GenericEvent, 16), 40)

	orig := submitCreateVolumeTaskFn
	submitCreateVolumeTaskFn = func(ctx context.Context, vc *cnsvsphere.VirtualCenter,
		spec *cnstypes.CnsVolumeCreateSpec) (vim25types.ManagedObjectReference, string, error) {
		volInfo, faultType, err := r.volumeManager.CreateVolume(ctx, spec, nil)
		if err != nil {
			return vim25types.ManagedObjectReference{}, faultType, err
		}
		seedOutcome(r.taskTracker, fakeCnsTaskID, &cnsTaskOutcome{
			state:   volumes.CreateVolumeTaskSucceeded,
			volInfo: volInfo,
		})
		return taskRef(fakeCnsTaskID), "", nil
	}
	return func() { submitCreateVolumeTaskFn = orig }
}

// reconcileUntilSettled drives Reconcile the way the work queue would, until it
// stops asking to be requeued. A single call no longer takes a CR from created
// to registered, because the CNS task wait and the PVC bind wait both release
// the worker and resume on a later pass.
func reconcileUntilSettled(ctx context.Context, r *ReconcileCnsRegisterVolume,
	req reconcile.Request) (reconcile.Result, error) {
	var (
		result reconcile.Result
		err    error
	)
	for i := 0; i < 12; i++ {
		result, err = r.Reconcile(ctx, req)
		if err != nil || result.RequeueAfter == 0 {
			return result, err
		}
	}
	return result, err
}
