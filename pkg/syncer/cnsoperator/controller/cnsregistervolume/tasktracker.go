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
	"sync"
	"time"

	vim25types "github.com/vmware/govmomi/vim25/types"
	apitypes "k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/event"

	cnsregistervolumev1alpha1 "sigs.k8s.io/vsphere-csi-driver/v3/pkg/apis/cnsoperator/cnsregistervolume/v1alpha1"
	volumes "sigs.k8s.io/vsphere-csi-driver/v3/pkg/common/cns-lib/volume"
	cnsvsphere "sigs.k8s.io/vsphere-csi-driver/v3/pkg/common/cns-lib/vsphere"
	"sigs.k8s.io/vsphere-csi-driver/v3/pkg/common/prometheus"
	"sigs.k8s.io/vsphere-csi-driver/v3/pkg/csi/service/logger"
)

// cnsTaskTracker watches in-flight CNS CreateVolume tasks on behalf of the
// reconciler.
//
// Why this exists: Manager.CreateVolume parks the calling goroutine in
// waitOnTask until CNS responds. The caller is one of the controller's
// MaxConcurrentReconciles workers, so a registration that takes minutes holds a
// worker for minutes and no other CnsRegisterVolume can start. That is the
// head-of-line blocking this type removes.
//
// What replaces it is deliberately NOT polling. The listview multiplexes every
// registered task onto one WaitForUpdates session against vCenter and pushes
// completions, so watching N tasks costs one connection and no periodic reads.
// This type keeps that and changes only who owns the result channel: instead of
// a reconciler worker blocking on a per-call channel, a bare goroutine parks on
// it, records the result, and wakes the CR through a GenericEvent. Such a
// goroutine costs a few KB of stack and blocks nothing; the reconciler worker
// returns as soon as the task is submitted.
//
// Two properties of the listview shape this type:
//
//   - processTaskUpdate drops an update when no channel is registered for the
//     task. So the registration must survive from submit until the result has
//     been consumed, and RemoveTask cannot be deferred at submit time the way
//     waitOnTask does it.
//   - The send to the result channel is non-blocking, so a full channel
//     silently discards the result. Each task therefore gets its own channel
//     with capacity 1, which a single result always fits into.
type cnsTaskTracker struct {
	mu sync.Mutex
	// results holds completed task outcomes keyed by task MoRef value, until
	// the owning CR's reconcile consumes them.
	results map[string]*cnsTaskOutcome
	// watching records which task MoRefs already have a goroutine, so that a
	// repeated reconcile cannot start a second watcher for one task.
	watching map[string]bool
	// submittedAt is when each watched task was dispatched, so the
	// CreateVolume latency metric can still span submit to completion now that
	// no single function call covers that span.
	submittedAt map[string]time.Time
	// slots maps a CR to the task MoRef it currently holds a capacity slot
	// for. Keyed by CR rather than by task so that a CR which loses its task
	// reference cannot leak a slot forever.
	slots map[apitypes.NamespacedName]string
	// maxInFlight caps concurrent CNS tasks. The 40 reconciler workers are
	// today an implicit ceiling on how many CreateVolume calls vCenter sees at
	// once; removing the blocking wait removes that ceiling, so it has to be
	// replaced explicitly or a burst would hit CNS far harder than it does now.
	maxInFlight int

	listView volumes.ListViewIf
	vc       *cnsvsphere.VirtualCenter
	// events wakes a CR's reconcile when its task completes.
	events chan event.GenericEvent
}

// cnsTaskOutcome is a decoded, terminal CreateVolume result.
type cnsTaskOutcome struct {
	state     volumes.CreateVolumeTaskState
	volInfo   *volumes.CnsVolumeInfo
	faultType string
	err       error
}

// newCnsTaskTracker builds a tracker. events must be the same channel the
// controller registered as a source.Channel watch.
func newCnsTaskTracker(listView volumes.ListViewIf, vc *cnsvsphere.VirtualCenter,
	events chan event.GenericEvent, maxInFlight int) *cnsTaskTracker {
	return &cnsTaskTracker{
		results:     make(map[string]*cnsTaskOutcome),
		watching:    make(map[string]bool),
		submittedAt: make(map[string]time.Time),
		slots:       make(map[apitypes.NamespacedName]string),
		maxInFlight: maxInFlight,
		listView:    listView,
		vc:          vc,
		events:      events,
	}
}

// tryAcquireSlot reserves one of the in-flight CNS task slots for a CR.
//
// A CR that already holds a slot gets it back rather than taking a second one,
// so a repeated reconcile of the same CR cannot consume the cap.
func (t *cnsTaskTracker) tryAcquireSlot(cr apitypes.NamespacedName) bool {
	t.mu.Lock()
	defer t.mu.Unlock()
	if _, held := t.slots[cr]; held {
		return true
	}
	if len(t.slots) >= t.maxInFlight {
		return false
	}
	t.slots[cr] = ""
	return true
}

// releaseSlot returns a CR's slot to the pool. Safe to call when none is held.
func (t *cnsTaskTracker) releaseSlot(cr apitypes.NamespacedName) {
	t.mu.Lock()
	defer t.mu.Unlock()
	delete(t.slots, cr)
}

// inFlight reports how many slots are currently taken.
func (t *cnsTaskTracker) inFlight() int {
	t.mu.Lock()
	defer t.mu.Unlock()
	return len(t.slots)
}

// watch registers a submitted task with the listview and starts the goroutine
// that waits for its completion.
//
// It is idempotent per task MoRef: calling it again for a task already being
// watched does nothing, which is what makes it safe to call from a reconcile
// that may run repeatedly for the same CR, and from restart recovery.
func (t *cnsTaskTracker) watch(ctx context.Context, cr apitypes.NamespacedName,
	taskMoRef vim25types.ManagedObjectReference, volumeName string) error {
	log := logger.GetLogger(ctx)

	t.mu.Lock()
	if t.watching[taskMoRef.Value] {
		t.mu.Unlock()
		return nil
	}
	if _, done := t.results[taskMoRef.Value]; done {
		// Already completed and waiting to be consumed.
		t.mu.Unlock()
		return nil
	}
	t.watching[taskMoRef.Value] = true
	t.submittedAt[taskMoRef.Value] = time.Now()
	t.slots[cr] = taskMoRef.Value
	t.mu.Unlock()

	// Capacity 1 so that the listview's non-blocking send always lands.
	ch := make(chan volumes.TaskResult, 1)
	if err := t.listView.AddTask(ctx, taskMoRef, ch); err != nil {
		t.mu.Lock()
		delete(t.watching, taskMoRef.Value)
		delete(t.submittedAt, taskMoRef.Value)
		t.mu.Unlock()
		// The caller distinguishes "task is gone from vCenter" from "listview
		// is unavailable"; both arrive here as an error.
		return err
	}

	go t.awaitResult(cr, taskMoRef, volumeName, ch)
	log.Infof("Watching CNS CreateVolume task %q for CnsRegisterVolume %s/%s",
		taskMoRef.Value, cr.Namespace, cr.Name)
	return nil
}

// awaitResult parks on one task's result channel. This goroutine is not a
// reconciler worker: it holds no queue slot and blocks no other CR.
func (t *cnsTaskTracker) awaitResult(cr apitypes.NamespacedName,
	taskMoRef vim25types.ManagedObjectReference, volumeName string, ch chan volumes.TaskResult) {
	ctx, log := logger.GetNewContextWithLogger()

	result := <-ch

	state, volInfo, faultType, err := volumes.DecodeCreateVolumeTaskResult(ctx, t.vc, result, volumeName)
	log.Infof("CNS CreateVolume task %q for CnsRegisterVolume %s/%s completed with state %q",
		taskMoRef.Value, cr.Namespace, cr.Name, state)

	t.mu.Lock()
	delete(t.watching, taskMoRef.Value)
	submittedAt, timed := t.submittedAt[taskMoRef.Value]
	delete(t.submittedAt, taskMoRef.Value)
	t.results[taskMoRef.Value] = &cnsTaskOutcome{
		state:     state,
		volInfo:   volInfo,
		faultType: faultType,
		err:       err,
	}
	t.mu.Unlock()

	// Manager.CreateVolume records this histogram around its blocking call.
	// That call no longer exists on this path, so record the equivalent span
	// here -- submit to completion -- or CreateVolume latency would silently
	// stop being reported for registrations.
	if timed {
		status := prometheus.PrometheusPassStatus
		if state != volumes.CreateVolumeTaskSucceeded {
			status = prometheus.PrometheusFailStatus
		}
		prometheus.CnsControlOpsHistVec.WithLabelValues(
			prometheus.PrometheusCnsCreateVolumeOpType, status).Observe(time.Since(submittedAt).Seconds())
	}

	// Remove from the listview only now: the update has been consumed, so
	// there is no window where a late update would be dropped.
	if removeErr := t.listView.RemoveTask(ctx, taskMoRef); removeErr != nil {
		log.Errorf("failed to remove task %q from listview: %v", taskMoRef.Value, removeErr)
		if markErr := t.listView.MarkTaskForDeletion(ctx, taskMoRef); markErr != nil {
			log.Errorf("failed to mark task %q for deletion: %v", taskMoRef.Value, markErr)
		}
	}

	t.wake(cr)
}

// wake nudges the owning CR's reconcile. The send is non-blocking: the
// controller also sets a safety RequeueAfter, so a dropped wake-up costs
// latency rather than progress.
func (t *cnsTaskTracker) wake(cr apitypes.NamespacedName) {
	ev := event.GenericEvent{
		Object: &cnsregistervolumev1alpha1.CnsRegisterVolume{},
	}
	ev.Object.SetName(cr.Name)
	ev.Object.SetNamespace(cr.Namespace)
	select {
	case t.events <- ev:
	default:
	}
}

// outcome returns a completed task's result, if it has arrived. This is a map
// lookup: checking on a running task costs no vCenter call at all.
func (t *cnsTaskTracker) outcome(taskID string) (*cnsTaskOutcome, bool) {
	t.mu.Lock()
	defer t.mu.Unlock()
	out, ok := t.results[taskID]
	return out, ok
}

// isWatching reports whether a goroutine is currently waiting on this task.
func (t *cnsTaskTracker) isWatching(taskID string) bool {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.watching[taskID]
}

// forget drops a consumed result and releases the CR's slot. Called once the
// reconciler has acted on the outcome.
func (t *cnsTaskTracker) forget(cr apitypes.NamespacedName, taskID string) {
	t.mu.Lock()
	defer t.mu.Unlock()
	delete(t.results, taskID)
	delete(t.slots, cr)
}

// forgetResult drops a consumed outcome but keeps the CR's capacity slot.
// Used for an Unknown outcome, where the CR must stay in CnsRegistering and
// keep its slot while the task's real state is re-established.
func (t *cnsTaskTracker) forgetResult(taskID string) {
	t.mu.Lock()
	defer t.mu.Unlock()
	delete(t.results, taskID)
}

// ensureInit lazily binds the tracker to the manager's listview and the
// vCenter. Done on first use rather than at Add() time because the listview
// needs a live VC connection, which is not guaranteed when controllers are
// registered.
func (t *cnsTaskTracker) ensureInit(ctx context.Context, mgr volumes.Manager,
	vc *cnsvsphere.VirtualCenter) error {
	t.mu.Lock()
	needsListView := t.listView == nil
	t.vc = vc
	t.mu.Unlock()

	if !needsListView {
		return nil
	}
	lv, err := volumes.TaskListView(ctx, mgr)
	if err != nil {
		return err
	}
	t.mu.Lock()
	if t.listView == nil {
		t.listView = lv
	}
	t.mu.Unlock()
	return nil
}
