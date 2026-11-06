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
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	cnstypes "github.com/vmware/govmomi/cns/types"
	"github.com/vmware/govmomi/vim25"
	"github.com/vmware/govmomi/vim25/methods"
	"github.com/vmware/govmomi/vim25/soap"
	"github.com/vmware/govmomi/vim25/types"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	commontypes "sigs.k8s.io/vsphere-csi-driver/v3/pkg/common/types"
	"sigs.k8s.io/vsphere-csi-driver/v3/pkg/csi/service/logger"
	"sigs.k8s.io/vsphere-csi-driver/v3/pkg/internalapis/cnsvolumeoperationrequest"
)

const placementDatastoreURL = "ds:///vmfs/volumes/5e8d4f/"

// datastoreLookupTransport answers the datastore summary retrieval that
// getCnsVolumeInfoFromTaskResult makes after a CreateVolume task succeeds with
// a placement result that names a datastore rather than clusters. err, when
// set, stands in for that retrieval failing.
type datastoreLookupTransport struct {
	err   error
	calls chan struct{}
}

func (d *datastoreLookupTransport) RoundTrip(ctx context.Context, req, res soap.HasFault) error {
	body, ok := req.(*methods.RetrievePropertiesExBody)
	if !ok {
		return fmt.Errorf("unexpected request %T", req)
	}
	select {
	case d.calls <- struct{}{}:
	default:
	}
	if d.err != nil {
		return d.err
	}
	out := res.(*methods.RetrievePropertiesExBody)
	out.Res = &types.RetrievePropertiesExResponse{Returnval: &types.RetrieveResult{
		Objects: []types.ObjectContent{{
			Obj: body.Req.SpecSet[0].ObjectSet[0].Obj,
			PropSet: []types.DynamicProperty{{
				Name: "summary",
				Val:  types.DatastoreSummary{Url: placementDatastoreURL},
			}},
		}},
	}}
	return nil
}

func newLookupClient(transport *datastoreLookupTransport) *vim25.Client {
	return &vim25.Client{RoundTripper: transport, ServiceContent: types.ServiceContent{
		PropertyCollector: types.ManagedObjectReference{Type: "PropertyCollector", Value: "collector"},
	}}
}

// newPlacementTestManager is newDeleteVolumeTestManagerWithListView with a
// caller supplied vim25 client, so the datastore lookup can be driven.
func newPlacementTestManager(store *fakeOperationStore, listView ListViewIf,
	client *vim25.Client) *defaultManager {
	m := newDeleteVolumeTestManagerWithListView(store, listView)
	m.virtualCenter = newTestVirtualCenter(client)
	return m
}

// datastorePlacementUpdate is the terminal task update CNS sends for a create
// that landed on a specific datastore: the volume exists, but its URL still
// has to be read from vCenter before the driver can answer the caller.
func datastorePlacementUpdate(ref types.ManagedObjectReference, volumeID string) types.PropertyChange {
	return types.PropertyChange{Val: types.TaskInfo{
		Task: ref, State: types.TaskInfoStateSuccess,
		Result: cnstypes.CnsVolumeOperationBatchResult{VolumeResults: []cnstypes.BaseCnsVolumeOperationResult{
			&cnstypes.CnsVolumeCreateResult{
				CnsVolumeOperationResult: cnstypes.CnsVolumeOperationResult{
					VolumeId: cnstypes.CnsVolumeId{Id: volumeID},
				},
				PlacementResults: []cnstypes.CnsPlacementResult{{
					Datastore: types.ManagedObjectReference{Type: "Datastore", Value: "datastore-1"},
				}},
			},
		}},
	}}
}

type createOutcome struct {
	info *CnsVolumeInfo
	err  error
}

// runCreate starts createVolumeWithImprovedIdempotency and waits until the
// task has been registered with the listview, so the caller can drive the
// terminal update deterministically.
func runCreate(t *testing.T, ctx context.Context, m *defaultManager, view *taskUpdateView,
	spec *cnstypes.CnsVolumeCreateSpec, ref types.ManagedObjectReference) <-chan createOutcome {
	t.Helper()
	done := make(chan createOutcome, 1)
	go func() {
		info, _, err := m.createVolumeWithImprovedIdempotency(ctx, spec, nil)
		done <- createOutcome{info, err}
	}()
	select {
	case got := <-view.registered:
		require.Equal(t, ref, got)
	case outcome := <-done:
		t.Fatalf("create returned before registering the task: %v", outcome.err)
	case <-ctx.Done():
		t.Fatal("create did not register the task")
	}
	return done
}

// TestPlacementLookupTimeoutKeepsCreateResumable covers the case where the CNS
// task succeeded but the datastore lookup that follows it ran out of context.
// Recording that as a task error would drop the task ID, and the retry would
// create a second volume for the same PVC. The record must stay resumable, and
// the retry must recover the volume together with its placement.
func TestPlacementLookupTimeoutKeepsCreateResumable(t *testing.T) {
	ctx, cancel := context.WithTimeout(logger.NewContextWithLogger(context.Background()), 10*time.Second)
	defer cancel()
	const volumeName, taskID, volumeID = "pvc-placement", "task-77", "volume-77"
	ref := types.ManagedObjectReference{Type: "Task", Value: taskID}
	store := newFakeOperationStore()
	store.records[volumeName] = cnsvolumeoperationrequest.CreateVolumeOperationRequestDetails(
		volumeName, "", "", 0, nil, metav1.Now(), taskID, "", "",
		taskInvocationStatusInProgress, "", "")
	spec := &cnstypes.CnsVolumeCreateSpec{Name: volumeName, Metadata: cnstypes.CnsVolumeMetadata{
		ContainerClusterArray: []cnstypes.CnsContainerCluster{{ClusterId: "cluster"}},
	}}

	// First attempt: the lookup times out after the task already succeeded.
	first := newTaskUpdateView()
	firstTransport := &datastoreLookupTransport{err: context.DeadlineExceeded, calls: make(chan struct{}, 1)}
	firstDone := runCreate(t, ctx, newPlacementTestManager(store, first, newLookupClient(firstTransport)),
		first, spec, ref)
	first.view.processTaskUpdate(datastorePlacementUpdate(ref, volumeID))
	require.ErrorIs(t, (<-firstDone).err, ErrTaskMonitoring)
	require.Len(t, firstTransport.calls, 1, "the datastore lookup must have been attempted")
	record := store.records[volumeName]
	require.Equal(t, taskInvocationStatusInProgress, record.OperationDetails.TaskStatus,
		"a lookup that ran out of context says nothing about the task and must not mark it failed")
	require.Equal(t, taskID, record.OperationDetails.TaskID)
	require.True(t, IsTaskPending(record))

	// Retry: the same task is resumed and the placement is read this time.
	second := newTaskUpdateView()
	secondManager := newPlacementTestManager(store, second,
		newLookupClient(&datastoreLookupTransport{calls: make(chan struct{}, 1)}))
	secondDone := runCreate(t, ctx, secondManager, second, spec, ref)
	second.view.processTaskUpdate(datastorePlacementUpdate(ref, volumeID))
	outcome := <-secondDone
	require.NoError(t, outcome.err)
	info := outcome.info
	require.Equal(t, volumeID, info.VolumeID.Id)
	require.Equal(t, placementDatastoreURL, info.DatastoreURL,
		"the retry must recover the placement the first attempt could not read")
	record = store.records[volumeName]
	require.Equal(t, taskInvocationStatusSuccess, record.OperationDetails.TaskStatus)
	require.Equal(t, volumeID, record.VolumeID)
}

// TestPlacementLookupFailureRecordsVolumeID covers a lookup failure that is not
// a context expiry. That is a real failure and still gets recorded as one, but
// the volume CNS created exists, and this record is the only place its ID is
// written down.
func TestPlacementLookupFailureRecordsVolumeID(t *testing.T) {
	ctx, cancel := context.WithTimeout(logger.NewContextWithLogger(context.Background()), 10*time.Second)
	defer cancel()
	const volumeName, taskID, volumeID = "pvc-placement-failed", "task-78", "volume-78"
	ref := types.ManagedObjectReference{Type: "Task", Value: taskID}
	store := newFakeOperationStore()
	store.records[volumeName] = cnsvolumeoperationrequest.CreateVolumeOperationRequestDetails(
		volumeName, "", "", 0, nil, metav1.Now(), taskID, "", "",
		taskInvocationStatusInProgress, "", "")
	spec := &cnstypes.CnsVolumeCreateSpec{Name: volumeName, Metadata: cnstypes.CnsVolumeMetadata{
		ContainerClusterArray: []cnstypes.CnsContainerCluster{{ClusterId: "cluster"}},
	}}

	view := newTaskUpdateView()
	transport := &datastoreLookupTransport{err: errors.New("datastore no longer accessible"),
		calls: make(chan struct{}, 1)}
	done := runCreate(t, ctx, newPlacementTestManager(store, view, newLookupClient(transport)), view, spec, ref)
	view.view.processTaskUpdate(datastorePlacementUpdate(ref, volumeID))
	err := (<-done).err
	require.Error(t, err)
	require.NotErrorIs(t, err, ErrTaskMonitoring)
	record := store.records[volumeName]
	require.Equal(t, taskInvocationStatusError, record.OperationDetails.TaskStatus)
	require.Equal(t, volumeID, record.VolumeID,
		"the created volume's ID must be recorded even though the task is marked failed")
}

// blockingRemoveListView holds the post-result cleanup open until released.
type blockingRemoveListView struct {
	taskUpdateView
	removing chan struct{}
	release  chan struct{}
}

func (b *blockingRemoveListView) RemoveTask(_ context.Context, ref types.ManagedObjectReference) error {
	close(b.removing)
	<-b.release
	b.view.taskMap.Delete(ref)
	return nil
}

// TestSuccessfulCreateDoesNotWaitForListViewCleanup covers the regression where
// cleanup ran before waitOnTask returned, so a slow RemoveTask spent what was
// left of the request's deadline and the placement lookup that follows it
// failed on an expired context. Cleanup must not be on the caller's path --
// but it must still gate a retry for the same task.
func TestSuccessfulCreateDoesNotWaitForListViewCleanup(t *testing.T) {
	ctx, cancel := context.WithTimeout(logger.NewContextWithLogger(context.Background()), 10*time.Second)
	defer cancel()
	const volumeName, taskID, volumeID = "pvc-cleanup", "task-79", "volume-79"
	ref := types.ManagedObjectReference{Type: "Task", Value: taskID}
	store := newFakeOperationStore()
	store.records[volumeName] = cnsvolumeoperationrequest.CreateVolumeOperationRequestDetails(
		volumeName, "", "", 0, nil, metav1.Now(), taskID, "", "",
		taskInvocationStatusInProgress, "", "")
	spec := &cnstypes.CnsVolumeCreateSpec{Name: volumeName, Metadata: cnstypes.CnsVolumeMetadata{
		ContainerClusterArray: []cnstypes.CnsContainerCluster{{ClusterId: "cluster"}},
	}}

	view := &blockingRemoveListView{taskUpdateView: *newTaskUpdateView(),
		removing: make(chan struct{}), release: make(chan struct{})}
	defer close(view.release)
	m := newPlacementTestManager(store, view, newLookupClient(&datastoreLookupTransport{calls: make(chan struct{}, 1)}))
	done := runCreate(t, ctx, m, &view.taskUpdateView, spec, ref)
	view.view.processTaskUpdate(datastorePlacementUpdate(ref, volumeID))

	select {
	case outcome := <-done:
		require.NoError(t, outcome.err, "the create must return its result without waiting for cleanup")
		require.Equal(t, placementDatastoreURL, outcome.info.DatastoreURL)
	case <-time.After(5 * time.Second):
		t.Fatal("create blocked on listview cleanup")
	}
	<-view.removing

	// Cleanup still owns the task wait lock, so a retry for the same task
	// queues behind it rather than racing it.
	key := taskWaitKey{manager: m, task: ref}
	acquired := make(chan func(), 1)
	go func() {
		if release, err := acquireTaskWait(context.Background(), key); err == nil {
			acquired <- release
		}
	}()
	select {
	case release := <-acquired:
		release()
		t.Fatal("a retry acquired the task while cleanup was still running")
	case <-time.After(100 * time.Millisecond):
	}
	view.release <- struct{}{}
	select {
	case release := <-acquired:
		release()
	case <-time.After(5 * time.Second):
		t.Fatal("the retry never acquired the task after cleanup finished")
	}
}

// TestPendingTaskOnAnotherVCenterIsNotMonitored covers the multi-vCenter case.
// The operation record is keyed by volume name alone, so after a monitoring
// interruption on one vCenter the create loop can hand the record to a manager
// belonging to a different one. Monitoring that task ID with the wrong client
// would either lose a still-running create or read an unrelated task, so the
// manager must refuse and leave the record alone.
func TestPendingTaskOnAnotherVCenterIsNotMonitored(t *testing.T) {
	ctx := logger.NewContextWithLogger(context.Background())
	const volumeName, taskID = "pvc-multivc", "task-80"
	store := newFakeOperationStore()
	store.records[volumeName] = cnsvolumeoperationrequest.CreateVolumeOperationRequestDetails(
		volumeName, "", "", 0, nil, metav1.Now(), taskID, "vc-a.example.com", "",
		taskInvocationStatusInProgress, "", "")
	spec := &cnstypes.CnsVolumeCreateSpec{Name: volumeName, Metadata: cnstypes.CnsVolumeMetadata{
		ContainerClusterArray: []cnstypes.CnsContainerCluster{{ClusterId: "cluster"}},
	}}

	view := newTaskUpdateView()
	m := newPlacementTestManager(store, view, newLookupClient(&datastoreLookupTransport{calls: make(chan struct{}, 1)}))
	m.virtualCenter.Config.Host = commontypes.NewFQDN("vc-b.example.com")
	m.multivCenterTopologyDeployment = true

	_, _, err := m.createVolumeWithImprovedIdempotency(ctx, spec, nil)
	require.ErrorIs(t, err, ErrTaskMonitoring)
	require.Contains(t, err.Error(), "vc-a.example.com")
	require.Empty(t, view.registered, "the task must not be registered against the wrong vCenter")
	record := store.records[volumeName]
	require.Equal(t, taskInvocationStatusInProgress, record.OperationDetails.TaskStatus,
		"the record belongs to the other vCenter and must be left untouched")
	require.Equal(t, taskID, record.OperationDetails.TaskID)
	require.Equal(t, "vc-a.example.com", record.OperationDetails.VCenterServer)
}

// TestPendingTaskOnSameVCenterIsMonitored is the companion to the test above:
// a record naming this manager's own vCenter is resumed as before.
func TestPendingTaskOnSameVCenterIsMonitored(t *testing.T) {
	ctx, cancel := context.WithTimeout(logger.NewContextWithLogger(context.Background()), 10*time.Second)
	defer cancel()
	const volumeName, taskID, volumeID = "pvc-samevc", "task-81", "volume-81"
	ref := types.ManagedObjectReference{Type: "Task", Value: taskID}
	store := newFakeOperationStore()
	store.records[volumeName] = cnsvolumeoperationrequest.CreateVolumeOperationRequestDetails(
		volumeName, "", "", 0, nil, metav1.Now(), taskID, "vc-a.example.com", "",
		taskInvocationStatusInProgress, "", "")
	spec := &cnstypes.CnsVolumeCreateSpec{Name: volumeName, Metadata: cnstypes.CnsVolumeMetadata{
		ContainerClusterArray: []cnstypes.CnsContainerCluster{{ClusterId: "cluster"}},
	}}

	view := newTaskUpdateView()
	m := newPlacementTestManager(store, view, newLookupClient(&datastoreLookupTransport{calls: make(chan struct{}, 1)}))
	m.virtualCenter.Config.Host = commontypes.NewFQDN("vc-a.example.com")
	m.multivCenterTopologyDeployment = true

	done := runCreate(t, ctx, m, view, spec, ref)
	view.view.processTaskUpdate(datastorePlacementUpdate(ref, volumeID))
	require.NoError(t, (<-done).err)
	require.Equal(t, taskInvocationStatusSuccess, store.records[volumeName].OperationDetails.TaskStatus)
}
