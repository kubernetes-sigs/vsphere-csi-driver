package volume

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/vmware/govmomi"
	cnstypes "github.com/vmware/govmomi/cns/types"
	"github.com/vmware/govmomi/object"
	"github.com/vmware/govmomi/view"
	"github.com/vmware/govmomi/vim25"
	"github.com/vmware/govmomi/vim25/methods"
	"github.com/vmware/govmomi/vim25/soap"
	"github.com/vmware/govmomi/vim25/types"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	cnsvsphere "sigs.k8s.io/vsphere-csi-driver/v3/pkg/common/cns-lib/vsphere"
	"sigs.k8s.io/vsphere-csi-driver/v3/pkg/csi/service/logger"
	"sigs.k8s.io/vsphere-csi-driver/v3/pkg/internalapis/cnsvolumeoperationrequest"
)

type blockingSessionTransport struct{ started chan struct{} }

type blockingListViewTransport struct {
	started  chan struct{}
	deadline chan time.Time
	unblock  chan struct{}
}

func (b *blockingListViewTransport) RoundTrip(ctx context.Context, req, res soap.HasFault) error {
	switch req := req.(type) {
	case *methods.RetrievePropertiesExBody:
		out := res.(*methods.RetrievePropertiesExBody)
		out.Res = &types.RetrievePropertiesExResponse{Returnval: &types.RetrieveResult{Objects: []types.ObjectContent{{
			Obj:     req.Req.SpecSet[0].ObjectSet[0].Obj,
			PropSet: []types.DynamicProperty{{Name: "currentSession", Val: &types.UserSession{Key: "session"}}},
		}}}}
		return nil
	case *methods.ModifyListViewBody:
		deadline, _ := ctx.Deadline()
		b.deadline <- deadline
		close(b.started)
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-b.unblock:
			return context.DeadlineExceeded
		}
	default:
		return fmt.Errorf("unexpected request %T", req)
	}
}

// taskUpdateView uses the real ListView task map, broadcast and update
// processing while isolating the test from a live vCenter connection.
type taskUpdateView struct {
	fakeListView
	view       *ListViewImpl
	registered chan types.ManagedObjectReference
}

func newTaskUpdateView() *taskUpdateView {
	return &taskUpdateView{
		view:       &ListViewImpl{ctx: logger.NewContextWithLogger(context.Background()), taskMap: NewTaskMap()},
		registered: make(chan types.ManagedObjectReference, 1),
	}
}

func (v *taskUpdateView) AddTask(_ context.Context, ref types.ManagedObjectReference, ch chan TaskResult) error {
	v.view.taskMap.Upsert(ref, TaskDetails{Reference: ref, ResultCh: ch})
	v.registered <- ref
	return nil
}

func (v *taskUpdateView) RemoveTask(_ context.Context, ref types.ManagedObjectReference) error {
	v.view.taskMap.Delete(ref)
	return nil
}

func (b *blockingSessionTransport) RoundTrip(ctx context.Context, _, _ soap.HasFault) error {
	close(b.started)
	<-ctx.Done()
	return ctx.Err()
}

func TestCreateVolumeMonitoringInterruptionPreservesTask(t *testing.T) {
	for _, tc := range []struct {
		name string
		err  error
	}{
		{"collector canceled", fmt.Errorf("%w: %w", ErrTaskMonitoring, context.Canceled)},
		{"collector transport failure", fmt.Errorf("%w: connection reset", ErrTaskMonitoring)},
		{"registration failure", ErrListViewTaskAddition},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx := logger.NewContextWithLogger(context.Background())
			store := newFakeOperationStore()
			m := newDeleteVolumeTestManager(store, tc.err)
			ref := types.ManagedObjectReference{Type: "Task", Value: "task-42"}
			details := cnsvolumeoperationrequest.CreateVolumeOperationRequestDetails(
				"pvc-test", "", "", 0, nil, metav1.Now(), ref.Value, "", "",
				cnsvolumeoperationrequest.TaskInvocationStatusInProgress, "", "")
			_, _, err := m.MonitorCreateVolumeTask(ctx, &details,
				object.NewTask(m.virtualCenter.Client.Client, ref), "pvc-test", "cluster")
			require.ErrorIs(t, err, ErrTaskMonitoring)
			require.Equal(t, cnsvolumeoperationrequest.TaskInvocationStatusInProgress, details.OperationDetails.TaskStatus)
			require.Equal(t, ref.Value, details.OperationDetails.TaskID)
			require.True(t, IsTaskPending(details))
		})
	}
}

func TestCreateVolumeTerminalTaskFailureIsRecorded(t *testing.T) {
	ctx := logger.NewContextWithLogger(context.Background())
	m := newDeleteVolumeTestManager(newFakeOperationStore(), fmt.Errorf("%w: failure", ErrTaskFailed))
	ref := types.ManagedObjectReference{Type: "Task", Value: "task-43"}
	details := cnsvolumeoperationrequest.CreateVolumeOperationRequestDetails(
		"pvc-test", "", "", 0, nil, metav1.Now(), ref.Value, "", "",
		cnsvolumeoperationrequest.TaskInvocationStatusInProgress, "", "")
	_, _, err := m.MonitorCreateVolumeTask(ctx, &details,
		object.NewTask(m.virtualCenter.Client.Client, ref), "pvc-test", "cluster")
	require.Error(t, err)
	require.Equal(t, cnsvolumeoperationrequest.TaskInvocationStatusError, details.OperationDetails.TaskStatus)
}

func TestCreateVolumeRetryOnAnotherManagerKeepsPendingTask(t *testing.T) {
	ctx := logger.NewContextWithLogger(context.Background())
	store := newFakeOperationStore()
	store.records["pvc-test"] = cnsvolumeoperationrequest.CreateVolumeOperationRequestDetails(
		"pvc-test", "", "", 0, nil, metav1.Now(), "task-42", "", "",
		cnsvolumeoperationrequest.TaskInvocationStatusInProgress, "", "")
	spec := &cnstypes.CnsVolumeCreateSpec{Name: "pvc-test", Metadata: cnstypes.CnsVolumeMetadata{
		ContainerClusterArray: []cnstypes.CnsContainerCluster{{ClusterId: "cluster"}},
	}}
	for i := 0; i < 2; i++ {
		m := newDeleteVolumeTestManager(store, fmt.Errorf("%w: collector unavailable", ErrTaskMonitoring))
		_, _, err := m.createVolumeWithImprovedIdempotency(ctx, spec, nil)
		require.ErrorIs(t, err, ErrTaskMonitoring)
		require.Equal(t, "task-42", store.records["pvc-test"].OperationDetails.TaskID)
		require.Equal(t, cnsvolumeoperationrequest.TaskInvocationStatusInProgress,
			store.records["pvc-test"].OperationDetails.TaskStatus)
	}
}

func TestCreateVolumeRecoversSameTaskAfterCollectorFailure(t *testing.T) {
	ctx, cancel := context.WithTimeout(logger.NewContextWithLogger(context.Background()), 5*time.Second)
	defer cancel()
	const volumeName, taskID, volumeID = "pvc-test", "task-42", "volume-42"
	ref := types.ManagedObjectReference{Type: "Task", Value: taskID}
	store := newFakeOperationStore()
	store.records[volumeName] = cnsvolumeoperationrequest.CreateVolumeOperationRequestDetails(
		volumeName, "", "", 0, nil, metav1.Now(), taskID, "", "",
		cnsvolumeoperationrequest.TaskInvocationStatusInProgress, "", "")
	spec := &cnstypes.CnsVolumeCreateSpec{Name: volumeName, Metadata: cnstypes.CnsVolumeMetadata{
		ContainerClusterArray: []cnstypes.CnsContainerCluster{{ClusterId: "cluster"}},
	}}
	type response struct {
		info *CnsVolumeInfo
		err  error
	}
	run := func(view *taskUpdateView) <-chan response {
		m := newDeleteVolumeTestManagerWithListView(store, view)
		result := make(chan response, 1)
		go func() {
			info, _, err := m.createVolumeWithImprovedIdempotency(ctx, spec, nil)
			result <- response{info, err}
		}()
		return result
	}

	first := newTaskUpdateView()
	firstResult := run(first)
	select {
	case got := <-first.registered:
		require.Equal(t, ref, got)
	case resp := <-firstResult:
		t.Fatalf("first monitor returned before registration: %v", resp.err)
	case <-ctx.Done():
		t.Fatal("first monitor did not register the task")
	}
	first.view.reportErrorOnAllPendingTasks(context.Canceled)
	resp := <-firstResult
	require.ErrorIs(t, resp.err, ErrTaskMonitoring)
	require.Equal(t, taskID, store.records[volumeName].OperationDetails.TaskID)
	require.Equal(t, taskInvocationStatusInProgress, store.records[volumeName].OperationDetails.TaskStatus)

	// A second manager represents the next controller replica. A new CNS
	// submission would fail here because the test has no CNS client.
	second := newTaskUpdateView()
	secondResult := run(second)
	select {
	case got := <-second.registered:
		require.Equal(t, ref, got)
	case resp := <-secondResult:
		t.Fatalf("retry returned before registering the original task: %v", resp.err)
	case <-ctx.Done():
		t.Fatal("retry did not register the task")
	}
	second.view.processTaskUpdate(types.PropertyChange{Val: types.TaskInfo{
		Task: ref, State: types.TaskInfoStateSuccess,
		Result: cnstypes.CnsVolumeOperationBatchResult{VolumeResults: []cnstypes.BaseCnsVolumeOperationResult{
			&cnstypes.CnsVolumeCreateResult{CnsVolumeOperationResult: cnstypes.CnsVolumeOperationResult{
				VolumeId: cnstypes.CnsVolumeId{Id: volumeID},
			}},
		}},
	}})
	resp = <-secondResult
	require.NoError(t, resp.err)
	require.Equal(t, volumeID, resp.info.VolumeID.Id)
	require.Equal(t, taskInvocationStatusSuccess, store.records[volumeName].OperationDetails.TaskStatus)
	require.Equal(t, taskID, store.records[volumeName].OperationDetails.TaskID)
	require.Equal(t, volumeID, store.records[volumeName].VolumeID)
}

func TestSupervisorCreateVolumePreservesTaskAndQuotaAcrossMonitoringFailure(t *testing.T) {
	for _, quotaEnabled := range []bool{false, true} {
		t.Run(fmt.Sprintf("quota_enabled=%t", quotaEnabled), func(t *testing.T) {
			ctx, cancel := context.WithTimeout(logger.NewContextWithLogger(context.Background()), 5*time.Second)
			defer cancel()
			const name, taskID, volumeID = "pvc-supervisor", "task-supervisor", "volume-supervisor"
			const volumeSize = int64(1024 * 1024 * 1024)
			ref := types.ManagedObjectReference{Type: "Task", Value: taskID}
			var quota *cnsvolumeoperationrequest.QuotaDetails
			var extra *CreateVolumeExtraParams
			if quotaEnabled {
				quota = &cnsvolumeoperationrequest.QuotaDetails{Reserved: resource.NewQuantity(volumeSize, resource.BinarySI)}
				extra = &CreateVolumeExtraParams{VolSizeBytes: volumeSize, IsPodVMOnStretchSupervisorFSSEnabled: true}
			}
			store := newFakeOperationStore()
			store.records[name] = cnsvolumeoperationrequest.CreateVolumeOperationRequestDetails(
				name, "", "", 0, quota, metav1.Now(), taskID, "", "", taskInvocationStatusInProgress, "", "")
			spec := &cnstypes.CnsVolumeCreateSpec{Name: name, Metadata: cnstypes.CnsVolumeMetadata{
				ContainerClusterArray: []cnstypes.CnsContainerCluster{{ClusterId: "supervisor"}},
			}}
			first := newDeleteVolumeTestManager(store, fmt.Errorf("%w: collector canceled", ErrTaskMonitoring))
			first.clusterFlavor = cnstypes.CnsClusterFlavorWorkload
			var params interface{}
			if extra != nil {
				params = extra
			}
			_, _, err := first.createVolumeWithImprovedIdempotency(ctx, spec, params)
			require.ErrorIs(t, err, ErrTaskMonitoring)
			require.Equal(t, taskInvocationStatusInProgress, store.records[name].OperationDetails.TaskStatus)
			require.Equal(t, taskID, store.records[name].OperationDetails.TaskID)
			if quotaEnabled {
				require.Equal(t, volumeSize, store.records[name].QuotaDetails.Reserved.Value())
			}

			view := newTaskUpdateView()
			second := newDeleteVolumeTestManagerWithListView(store, view)
			second.clusterFlavor = cnstypes.CnsClusterFlavorWorkload
			result := make(chan error, 1)
			go func() {
				info, _, err := second.createVolumeWithImprovedIdempotency(ctx, spec, params)
				if err == nil && (info == nil || info.VolumeID.Id != volumeID) {
					err = fmt.Errorf("unexpected volume returned: %+v", info)
				}
				result <- err
			}()
			select {
			case got := <-view.registered:
				require.Equal(t, ref, got)
			case err := <-result:
				t.Fatalf("retry returned before monitoring original task: %v", err)
			case <-ctx.Done():
				t.Fatal("retry did not register the original task")
			}
			view.view.processTaskUpdate(types.PropertyChange{Val: types.TaskInfo{Task: ref, State: types.TaskInfoStateSuccess,
				Result: cnstypes.CnsVolumeOperationBatchResult{VolumeResults: []cnstypes.BaseCnsVolumeOperationResult{
					&cnstypes.CnsVolumeCreateResult{CnsVolumeOperationResult: cnstypes.CnsVolumeOperationResult{
						VolumeId: cnstypes.CnsVolumeId{Id: volumeID},
					}},
				}}}})
			require.NoError(t, <-result)
			require.Equal(t, taskInvocationStatusSuccess, store.records[name].OperationDetails.TaskStatus)
			require.Equal(t, taskID, store.records[name].OperationDetails.TaskID)
			if quotaEnabled {
				require.Zero(t, store.records[name].QuotaDetails.Reserved.Value())
			}
		})
	}
}

func TestSupervisorCreateVolumeRecordsTerminalTaskFailure(t *testing.T) {
	ctx := logger.NewContextWithLogger(context.Background())
	const name, taskID = "pvc-supervisor-failed", "task-supervisor-failed"
	const volumeSize = int64(1024 * 1024 * 1024)
	store := newFakeOperationStore()
	store.records[name] = cnsvolumeoperationrequest.CreateVolumeOperationRequestDetails(
		name, "", "", 0, &cnsvolumeoperationrequest.QuotaDetails{
			Reserved: resource.NewQuantity(volumeSize, resource.BinarySI),
		}, metav1.Now(), taskID, "", "", taskInvocationStatusInProgress, "", "")
	m := newDeleteVolumeTestManager(store, fmt.Errorf("%w: actual vCenter failure", ErrTaskFailed))
	m.clusterFlavor = cnstypes.CnsClusterFlavorWorkload
	spec := &cnstypes.CnsVolumeCreateSpec{Name: name, Metadata: cnstypes.CnsVolumeMetadata{
		ContainerClusterArray: []cnstypes.CnsContainerCluster{{ClusterId: "supervisor"}},
	}}
	_, _, err := m.createVolumeWithImprovedIdempotency(ctx, spec, &CreateVolumeExtraParams{
		VolSizeBytes: volumeSize, IsPodVMOnStretchSupervisorFSSEnabled: true,
	})
	require.ErrorIs(t, err, ErrTaskFailed)
	require.Equal(t, taskInvocationStatusError, store.records[name].OperationDetails.TaskStatus)
	require.Equal(t, taskID, store.records[name].OperationDetails.TaskID)
	require.Zero(t, store.records[name].QuotaDetails.Reserved.Value())
}

func TestWaitForResultCanceledIsMonitoringError(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, err := waitForResultOrTimeout(ctx, types.ManagedObjectReference{Type: "Task", Value: "task-1"},
		make(chan TaskResult))
	require.ErrorIs(t, err, ErrTaskMonitoring)
	require.ErrorIs(t, err, context.Canceled)
	require.Contains(t, err.Error(), "canceled while waiting")
}

func TestWaitForResultDeadlineIsMonitoringError(t *testing.T) {
	ctx, cancel := context.WithDeadline(context.Background(), time.Now().Add(-time.Second))
	defer cancel()
	_, err := waitForResultOrTimeout(ctx, types.ManagedObjectReference{Type: "Task", Value: "task-1"},
		make(chan TaskResult))
	require.ErrorIs(t, err, ErrTaskMonitoring)
	require.ErrorIs(t, err, context.DeadlineExceeded)
	require.Contains(t, err.Error(), "time out for task")
}

func TestCollectorFailureBroadcastIsNotTaskFailure(t *testing.T) {
	l := &ListViewImpl{taskMap: NewTaskMap()}
	channels := []chan TaskResult{make(chan TaskResult, 1), make(chan TaskResult, 1)}
	for i, ch := range channels {
		ref := types.ManagedObjectReference{Type: "Task", Value: fmt.Sprintf("task-%d", i)}
		l.taskMap.Upsert(ref, TaskDetails{Reference: ref, ResultCh: ch})
	}
	l.reportErrorOnAllPendingTasks(context.Canceled)
	for _, ch := range channels {
		result := <-ch
		require.ErrorIs(t, result.Err, ErrTaskMonitoring)
		require.ErrorIs(t, result.Err, context.Canceled)
		require.NotErrorIs(t, result.Err, ErrTaskFailed)
	}
}

func TestCanceledAddDoesNotInvalidateSharedListView(t *testing.T) {
	sharedCtx, sharedCancel := context.WithCancel(context.Background())
	defer sharedCancel()
	l := &ListViewImpl{isReady: true, waitForUpdatesContext: sharedCtx, waitForUpdatesCancelFunc: sharedCancel}
	ctx, cancel := context.WithCancel(logger.NewContextWithLogger(context.Background()))
	cancel()
	err := l.AddTask(ctx, types.ManagedObjectReference{Type: "Task", Value: "task-1"}, make(chan TaskResult, 1))
	require.ErrorIs(t, err, ErrTaskMonitoring)
	require.True(t, l.IsListViewReady())
	require.NoError(t, sharedCtx.Err())
}

func TestCanceledAddDoesNotWaitForBusyView(t *testing.T) {
	sharedCtx, sharedCancel := context.WithCancel(context.Background())
	defer sharedCancel()
	l := &ListViewImpl{isReady: true, waitForUpdatesContext: sharedCtx, waitForUpdatesCancelFunc: sharedCancel}
	require.NoError(t, l.viewMu.Lock(context.Background()))
	defer l.viewMu.Unlock()
	ctx, cancel := context.WithCancel(logger.NewContextWithLogger(context.Background()))
	cancel()
	err := l.AddTask(ctx, types.ManagedObjectReference{Type: "Task", Value: "task-1"}, make(chan TaskResult, 1))
	require.ErrorIs(t, err, ErrTaskMonitoring)
	require.ErrorIs(t, err, context.Canceled)
	require.NoError(t, sharedCtx.Err())
}

func TestViewLockWaitHonorsDeadline(t *testing.T) {
	var lock contextMutex
	require.NoError(t, lock.Lock(context.Background()))
	defer lock.Unlock()
	ctx, cancel := context.WithDeadline(context.Background(), time.Now().Add(-time.Second))
	defer cancel()
	require.ErrorIs(t, lock.Lock(ctx), context.DeadlineExceeded)
}

func TestStalledSessionCheckDoesNotTrapCanceledRequests(t *testing.T) {
	sessionRef := types.ManagedObjectReference{Type: "SessionManager", Value: "session"}
	collectorRef := types.ManagedObjectReference{Type: "PropertyCollector", Value: "collector"}
	transport := &blockingSessionTransport{started: make(chan struct{})}
	client := &vim25.Client{RoundTripper: transport, ServiceContent: types.ServiceContent{
		SessionManager: &sessionRef, PropertyCollector: collectorRef,
	}}
	sharedCtx, sharedCancel := context.WithCancel(context.Background())
	defer sharedCancel()
	l := &ListViewImpl{isReady: true, virtualCenter: &cnsvsphere.VirtualCenter{Client: &govmomi.Client{Client: client}},
		waitForUpdatesContext: sharedCtx, waitForUpdatesCancelFunc: sharedCancel}
	firstCtx, cancelFirst := context.WithCancel(logger.NewContextWithLogger(context.Background()))
	firstDone := make(chan error, 1)
	go func() {
		firstDone <- l.AddTask(firstCtx, types.ManagedObjectReference{Type: "Task", Value: "task-1"},
			make(chan TaskResult, 1))
	}()
	<-transport.started
	secondCtx, cancelSecond := context.WithCancel(logger.NewContextWithLogger(context.Background()))
	cancelSecond()
	err := l.AddTask(secondCtx, types.ManagedObjectReference{Type: "Task", Value: "task-2"}, make(chan TaskResult, 1))
	require.ErrorIs(t, err, context.Canceled)
	cancelFirst()
	require.ErrorIs(t, <-firstDone, context.Canceled)
	require.True(t, l.IsListViewReady())
	require.NoError(t, sharedCtx.Err())
}

func TestStalledListViewAddHonorsCancellation(t *testing.T) {
	sessionRef := types.ManagedObjectReference{Type: "SessionManager", Value: "session"}
	collectorRef := types.ManagedObjectReference{Type: "PropertyCollector", Value: "collector"}
	transport := &blockingListViewTransport{started: make(chan struct{}), deadline: make(chan time.Time, 1)}
	client := &vim25.Client{RoundTripper: transport, ServiceContent: types.ServiceContent{
		SessionManager: &sessionRef, PropertyCollector: collectorRef,
	}}
	sharedCtx, sharedCancel := context.WithCancel(context.Background())
	defer sharedCancel()
	l := &ListViewImpl{isReady: true, taskMap: NewTaskMap(),
		virtualCenter:         &cnsvsphere.VirtualCenter{Client: &govmomi.Client{Client: client}},
		listView:              view.NewListView(client, types.ManagedObjectReference{Type: "ListView", Value: "view"}),
		waitForUpdatesContext: sharedCtx, waitForUpdatesCancelFunc: sharedCancel}
	ctx, cancel := context.WithCancel(logger.NewContextWithLogger(context.Background()))
	result := make(chan error, 1)
	go func() {
		result <- l.AddTask(ctx, types.ManagedObjectReference{Type: "Task", Value: "task-1"}, make(chan TaskResult, 1))
	}()
	select {
	case <-transport.started:
	case err := <-result:
		t.Fatalf("Add failed before reaching the ListView: %v", err)
	case <-time.After(5 * time.Second):
		t.Fatal("Add did not reach the ListView")
	}
	deadline := <-transport.deadline
	require.False(t, deadline.IsZero(), "ListView Add must have a deadline")
	otherCtx, cancelOther := context.WithCancel(context.Background())
	cancelOther()
	require.ErrorIs(t, l.viewMu.Lock(otherCtx), context.Canceled)
	cancel()
	select {
	case err := <-result:
		require.ErrorIs(t, err, ErrListViewTaskAddition)
	case <-time.After(5 * time.Second):
		t.Fatal("canceled Add did not release the ListView lock")
	}
	require.NoError(t, l.viewMu.Lock(context.Background()))
	l.viewMu.Unlock()
	// Add may have reached vCenter before cancellation, so the view is
	// invalidated and existing waiters will receive a monitoring error.
	require.ErrorIs(t, sharedCtx.Err(), context.Canceled)
}

func TestStalledListViewRemoveUsesBoundedCleanup(t *testing.T) {
	sessionRef := types.ManagedObjectReference{Type: "SessionManager", Value: "session"}
	collectorRef := types.ManagedObjectReference{Type: "PropertyCollector", Value: "collector"}
	transport := &blockingListViewTransport{started: make(chan struct{}), deadline: make(chan time.Time, 1),
		unblock: make(chan struct{})}
	client := &vim25.Client{RoundTripper: transport, ServiceContent: types.ServiceContent{
		SessionManager: &sessionRef, PropertyCollector: collectorRef,
	}}
	l := &ListViewImpl{isReady: true, taskMap: NewTaskMap(),
		virtualCenter: &cnsvsphere.VirtualCenter{Client: &govmomi.Client{Client: client}},
		listView:      view.NewListView(client, types.ManagedObjectReference{Type: "ListView", Value: "view"})}
	ctx, cancel := context.WithCancel(logger.NewContextWithLogger(context.Background()))
	cancel()
	result := make(chan error, 1)
	go func() {
		result <- l.RemoveTask(ctx, types.ManagedObjectReference{Type: "Task", Value: "task-1"})
	}()
	select {
	case <-transport.started:
	case err := <-result:
		t.Fatalf("cleanup failed before reaching the ListView: %v", err)
	case <-time.After(5 * time.Second):
		t.Fatal("cleanup did not reach the ListView")
	}
	deadline := <-transport.deadline
	require.WithinDuration(t, time.Now().Add(listViewCallTimeout), deadline, time.Second)
	otherCtx, cancelOther := context.WithCancel(context.Background())
	cancelOther()
	require.ErrorIs(t, l.viewMu.Lock(otherCtx), context.Canceled)
	close(transport.unblock)
	select {
	case err := <-result:
		require.Error(t, err)
	case <-time.After(5 * time.Second):
		t.Fatal("Remove did not release the ListView lock")
	}
	require.NoError(t, l.viewMu.Lock(context.Background()))
	l.viewMu.Unlock()
}

func TestTerminalTaskUpdateIsTaskFailure(t *testing.T) {
	ref := types.ManagedObjectReference{Type: "Task", Value: "task-failed"}
	ch := make(chan TaskResult, 1)
	l := &ListViewImpl{ctx: logger.NewContextWithLogger(context.Background()), taskMap: NewTaskMap()}
	l.taskMap.Upsert(ref, TaskDetails{Reference: ref, ResultCh: ch})
	l.processTaskUpdate(types.PropertyChange{Val: types.TaskInfo{Task: ref, State: types.TaskInfoStateError,
		Error: &types.LocalizedMethodFault{LocalizedMessage: "task failed"}}})
	require.ErrorIs(t, (<-ch).Err, ErrTaskFailed)
}

func TestTaskWaitLockDoesNotReplaceExistingWaiter(t *testing.T) {
	key := taskWaitKey{task: types.ManagedObjectReference{Type: "Task", Value: "task-1"}}
	release, err := acquireTaskWait(context.Background(), key)
	require.NoError(t, err)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, err = acquireTaskWait(ctx, key)
	require.ErrorIs(t, err, ErrTaskMonitoring)
	release()
	release, err = acquireTaskWait(context.Background(), key)
	require.NoError(t, err)
	release()
}

func TestTaskWaitLockSerializesConcurrentMonitors(t *testing.T) {
	key := taskWaitKey{task: types.ManagedObjectReference{Type: "Task", Value: "task-concurrent"}}
	releaseFirst, err := acquireTaskWait(context.Background(), key)
	require.NoError(t, err)
	started := make(chan struct{})
	acquired := make(chan func(), 1)
	go func() {
		close(started)
		release, err := acquireTaskWait(context.Background(), key)
		if err == nil {
			acquired <- release
		}
	}()
	<-started
	select {
	case release := <-acquired:
		release()
		t.Fatal("second monitor acquired the task while the first was active")
	default:
	}
	releaseFirst()
	releaseSecond := <-acquired
	releaseSecond()
}
