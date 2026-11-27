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
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	apitypes "k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/record"
	"sigs.k8s.io/controller-runtime/pkg/event"

	cnsregistervolumev1alpha1 "sigs.k8s.io/vsphere-csi-driver/v3/pkg/apis/cnsoperator/cnsregistervolume/v1alpha1"
	volumes "sigs.k8s.io/vsphere-csi-driver/v3/pkg/common/cns-lib/volume"
	"sigs.k8s.io/vsphere-csi-driver/v3/pkg/common/conditions"
)

// TestTerminalOutcomeFreesCapacityBeforeConsumption is the first half of the
// fix: a slot stops counting against the cap as soon as CNS is done with the
// task, not when the owning CR's next reconcile gets around to it. That gap is
// what kept every slot busy while vCenter sat idle.
func TestTerminalOutcomeFreesCapacityBeforeConsumption(t *testing.T) {
	tracker := newCnsTaskTracker(newFakeListView(), nil, make(chan event.GenericEvent, 10), 1)
	require.True(t, tracker.tryAcquireSlot(crKey("cr-1")))
	tracker.mu.Lock()
	tracker.slots[crKey("cr-1")] = "task-1"
	tracker.mu.Unlock()
	require.False(t, tracker.tryAcquireSlot(crKey("cr-2")), "the cap is full while task-1 runs")

	seedOutcome(tracker, "task-1", &cnsTaskOutcome{state: volumes.CreateVolumeTaskSucceeded})

	assert.Equal(t, 0, tracker.inFlight(), "a finished task is no longer load on vCenter")
	assert.True(t, tracker.tryAcquireSlot(crKey("cr-2")), "the next CR gets the capacity at once")
	tracker.mu.Lock()
	assert.Equal(t, "task-1", tracker.slots[crKey("cr-1")],
		"cr-1 keeps its task reference until it consumes the result")
	tracker.mu.Unlock()
}

// TestUnknownOutcomeKeepsCapacity: Unknown means the task may still be running,
// so its slot must keep counting.
func TestUnknownOutcomeKeepsCapacity(t *testing.T) {
	lv := newFakeListView()
	tracker := newCnsTaskTracker(lv, nil, make(chan event.GenericEvent, 10), 1)
	require.NoError(t, tracker.watch(context.Background(), crKey("cr-1"), taskRef("task-1"), "vol-1"))
	lv.deliver(t, "task-1", volumes.TaskResult{Err: fmt.Errorf("%w: connection reset", volumes.ErrTaskMonitoring)})
	waitFor(t, func() bool { _, ok := tracker.outcome("task-1"); return ok }, "outcome")

	out, _ := tracker.outcome("task-1")
	require.Equal(t, volumes.CreateVolumeTaskUnknown, out.state)
	assert.Equal(t, 1, tracker.inFlight())
	assert.False(t, tracker.tryAcquireSlot(crKey("cr-2")))
}

// TestListViewTaskFailureIsFailed: when vCenter reports the task in the error
// state the listview sends ErrTaskFailed with no TaskInfo. That is a real
// failure, unlike the ErrTaskMonitoring the listview sends for a lost session,
// and is recorded as Failed rather than re-read as Unknown.
func TestListViewTaskFailureIsFailed(t *testing.T) {
	lv := newFakeListView()
	events := make(chan event.GenericEvent, 10)
	tracker := newCnsTaskTracker(lv, nil, events, 1)
	require.NoError(t, tracker.watch(context.Background(), crKey("cr-1"), taskRef("task-1"), "vol-1"))
	lv.deliver(t, "task-1", volumes.TaskResult{
		Err: fmt.Errorf("%w: The operation is not allowed in the current state. (vim.fault.InvalidState)", volumes.ErrTaskFailed)})
	waitFor(t, func() bool { _, ok := tracker.outcome("task-1"); return ok }, "outcome")

	out, _ := tracker.outcome("task-1")
	assert.Equal(t, volumes.CreateVolumeTaskFailed, out.state)
	assert.ErrorIs(t, out.err, volumes.ErrTaskFailed)
	assert.Equal(t, 0, tracker.inFlight(), "a failed task frees its slot")
}

// TestWaitingCRPollsForASlot: a CR refused a slot is told why and tries again
// after cnsSlotRetryInterval. Nothing has been sent to CNS.
func TestWaitingCRPollsForASlot(t *testing.T) {
	instance := newCondTestInstance(cnsregistervolumev1alpha1.CnsRegisterVolumeStatus{})
	r, _, _ := newAsyncTestReconciler(t, instance, 1)
	require.True(t, r.taskTracker.tryAcquireSlot(apitypes.NamespacedName{Name: "other", Namespace: "test-ns"}))

	_, result, err := r.submitOrPollCreateVolume(context.Background(), instance, nil, asyncTestSpec(), time.Second)
	require.NoError(t, err)
	require.NotNil(t, result)
	assert.Equal(t, cnsSlotRetryInterval, result.RequeueAfter)
	assert.Empty(t, instance.Status.CnsTaskID)
	assert.Equal(t, cnsregistervolumev1alpha1.ReasonWaitingForTaskSlot,
		conditions.GetReason(instance, cnsregistervolumev1alpha1.ConditionCnsVolumeRegistered))
}

// TestCapIsNeverExceededUnderContention hammers admission and release from
// many goroutines (run with -race).
func TestCapIsNeverExceededUnderContention(t *testing.T) {
	const capacity = 5
	tracker := newCnsTaskTracker(newFakeListView(), nil, make(chan event.GenericEvent, 1000), capacity)
	var holders, peak atomic.Int32
	var wg sync.WaitGroup
	for i := 0; i < 50; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			cr := crKey(fmt.Sprintf("cr-%d", i))
			for !tracker.tryAcquireSlot(cr) {
				time.Sleep(time.Millisecond)
			}
			n := holders.Add(1)
			for {
				p := peak.Load()
				if n <= p || peak.CompareAndSwap(p, n) {
					break
				}
			}
			time.Sleep(time.Millisecond)
			holders.Add(-1)
			tracker.forget(cr, "")
		}(i)
	}
	wg.Wait()
	assert.LessOrEqual(t, peak.Load(), int32(capacity))
	assert.Equal(t, 0, tracker.inFlight())
}

// TestConsumingResultTakesNoSlot: once a finished task's capacity has gone to
// the next CR, the owner consuming its result must not take it back.
func TestConsumingResultTakesNoSlot(t *testing.T) {
	instance := newCondTestInstance(cnsregistervolumev1alpha1.CnsRegisterVolumeStatus{CnsTaskID: "task-done"})
	r, _, _ := newAsyncTestReconciler(t, instance, 1)
	r.taskTracker.mu.Lock()
	r.taskTracker.slots[instanceKey(instance)] = "task-done"
	r.taskTracker.mu.Unlock()
	seedOutcome(r.taskTracker, "task-done", &cnsTaskOutcome{
		state: volumes.CreateVolumeTaskFailed, err: errors.New("CnsFault: InvalidState")})
	other := apitypes.NamespacedName{Name: "other", Namespace: "test-ns"}
	require.True(t, r.taskTracker.tryAcquireSlot(other))

	_, result, err := r.submitOrPollCreateVolume(context.Background(), instance, nil, asyncTestSpec(), time.Second)
	require.NoError(t, err)
	require.NotNil(t, result)
	assert.Equal(t, 1, r.taskTracker.inFlight(), "only the other CR holds capacity")
	r.taskTracker.mu.Lock()
	_, held := r.taskTracker.slots[instanceKey(instance)]
	r.taskTracker.mu.Unlock()
	assert.False(t, held)
}

// TestFailedTaskRecordsWarning: every failed CreateVolume leaves a Warning on
// the CR. In the 1000-volume run none did, which hid 276 host-side failures.
func TestFailedTaskRecordsWarning(t *testing.T) {
	instance := newCondTestInstance(cnsregistervolumev1alpha1.CnsRegisterVolumeStatus{CnsTaskID: "task-1"})
	r, lv, _ := newAsyncTestReconciler(t, instance, 1)
	recorder := record.NewFakeRecorder(8)
	r.recorder = recorder
	require.NoError(t, r.taskTracker.watch(context.Background(), instanceKey(instance), taskRef("task-1"), "vol"))
	lv.deliver(t, "task-1", volumes.TaskResult{
		Err: fmt.Errorf("%w: The operation is not allowed in the current state. (vim.fault.InvalidState)", volumes.ErrTaskFailed)})
	waitFor(t, func() bool { _, ok := r.taskTracker.outcome("task-1"); return ok }, "outcome")

	_, result, err := r.submitOrPollCreateVolume(context.Background(), instance, nil, asyncTestSpec(), time.Second)
	require.NoError(t, err)
	require.NotNil(t, result)
	assert.Empty(t, instance.Status.CnsTaskID, "the failed task is cleared so the CR resubmits")
	select {
	case ev := <-recorder.Events:
		assert.True(t, strings.HasPrefix(ev, "Warning CnsRegisterVolumeFailed"), ev)
	default:
		t.Fatal("a failed CNS task recorded no event")
	}
}

// TestRepeatedTaskFailuresStayWithinBudget bounds what the per-CR backoff
// costs a CR that keeps hitting a transient host fault (the duplicate-ddb.uuid
// InvalidState seen in the field, where no disk failed more than 4 times).
// The requeue after each failure is the backoff in force for that pass, which
// doubles per failure from 1s; four failures cost 1+2+4+8 = 15s.
func TestRepeatedTaskFailuresStayWithinBudget(t *testing.T) {
	instance := newCondTestInstance(cnsregistervolumev1alpha1.CnsRegisterVolumeStatus{})
	r, _, _ := newAsyncTestReconciler(t, instance, 1)
	key := instanceKey(instance)
	backOffDuration[key] = time.Second
	var waited time.Duration
	for i := 1; i <= 4; i++ {
		taskID := fmt.Sprintf("task-%d", i)
		require.NoError(t, setInstanceCnsTaskID(context.Background(), r, instance, taskID))
		r.taskTracker.mu.Lock()
		r.taskTracker.slots[key] = taskID
		r.taskTracker.mu.Unlock()
		seedOutcome(r.taskTracker, taskID, &cnsTaskOutcome{
			state: volumes.CreateVolumeTaskFailed, err: errors.New("CnsFault: InvalidState")})
		timeout := backOffDuration[key]
		_, result, err := r.submitOrPollCreateVolume(context.Background(), instance, nil, asyncTestSpec(), timeout)
		require.NoError(t, err)
		require.NotNil(t, result)
		waited += result.RequeueAfter
	}
	assert.LessOrEqual(t, waited, 30*time.Second)
	assert.Equal(t, cnsregistervolumev1alpha1.ReasonCnsTaskFailed,
		conditions.GetReason(instance, cnsregistervolumev1alpha1.ConditionCnsVolumeRegistered))
}
