package volume

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/vmware/govmomi"
	cnstypes "github.com/vmware/govmomi/cns/types"
	"github.com/vmware/govmomi/object"
	"github.com/vmware/govmomi/vim25"
	vim25types "github.com/vmware/govmomi/vim25/types"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	cnsvsphere "sigs.k8s.io/vsphere-csi-driver/v3/pkg/common/cns-lib/vsphere"
	csifault "sigs.k8s.io/vsphere-csi-driver/v3/pkg/common/fault"
	"sigs.k8s.io/vsphere-csi-driver/v3/pkg/csi/service/logger"
	"sigs.k8s.io/vsphere-csi-driver/v3/pkg/internalapis/cnsvolumeoperationrequest"
	cnsvolumeoprequestv1alpha1 "sigs.k8s.io/vsphere-csi-driver/v3/pkg/internalapis/cnsvolumeoperationrequest/v1alpha1"
)

// fakeOperationStore is an in-memory VolumeOperationRequest implementation. The
// fake in unittestcommon cannot be used here because that package imports this
// one.
// newTestVirtualCenter builds a VirtualCenter the way the driver's own
// constructors do. ClientMutex in particular is not optional: every read of
// vc.Client goes through it, and Connect dereferences it unconditionally.
func newTestVirtualCenter(client *vim25.Client) *cnsvsphere.VirtualCenter {
	return &cnsvsphere.VirtualCenter{
		Config:      &cnsvsphere.VirtualCenterConfig{},
		Client:      &govmomi.Client{Client: client},
		ClientMutex: &sync.Mutex{},
	}
}

type fakeOperationStore struct {
	records map[string]*cnsvolumeoperationrequest.VolumeOperationRequestDetails
}

func newFakeOperationStore() *fakeOperationStore {
	return &fakeOperationStore{
		records: make(map[string]*cnsvolumeoperationrequest.VolumeOperationRequestDetails),
	}
}

func (f *fakeOperationStore) GetRequestDetails(ctx context.Context, name string) (
	*cnsvolumeoperationrequest.VolumeOperationRequestDetails, error) {
	record, ok := f.records[name]
	if !ok {
		return nil, apierrors.NewNotFound(
			cnsvolumeoprequestv1alpha1.Resource("cnsvolumeoperationrequests"), name)
	}
	return record, nil
}

func (f *fakeOperationStore) StoreRequestDetails(ctx context.Context,
	instance *cnsvolumeoperationrequest.VolumeOperationRequestDetails) error {
	f.records[instance.Name] = instance
	return nil
}

func (f *fakeOperationStore) DeleteRequestDetails(ctx context.Context, name string) error {
	delete(f.records, name)
	return nil
}

func (f *fakeOperationStore) HasPriorSuccessfulCreate(ctx context.Context, name string) bool {
	return false
}

// fakeListView fails AddTask with a caller supplied error, which is how
// waitOnTask surfaces both terminally failed tasks and unobservable ones. When
// deliverEmptyResult is set, AddTask succeeds and delivers an empty TaskResult,
// so waitOnTask returns a nil TaskInfo together with a nil error.
type fakeListView struct {
	addTaskErr         error
	deliverEmptyResult bool
	// taskInfo, when set, is delivered as the terminal result of the task.
	taskInfo *vim25types.TaskInfo
}

func (f *fakeListView) AddTask(ctx context.Context, taskMoRef vim25types.ManagedObjectReference,
	ch chan TaskResult) error {
	if f.addTaskErr != nil {
		return f.addTaskErr
	}
	if f.deliverEmptyResult {
		ch <- TaskResult{}
	}
	if f.taskInfo != nil {
		ch <- TaskResult{TaskInfo: f.taskInfo}
	}
	return nil
}

func (f *fakeListView) RemoveTask(ctx context.Context, taskMoRef vim25types.ManagedObjectReference) error {
	return nil
}

func (f *fakeListView) ResetVirtualCenter(ctx context.Context, virtualCenter *cnsvsphere.VirtualCenter) {
}

func (f *fakeListView) MarkTaskForDeletion(ctx context.Context,
	taskMoRef vim25types.ManagedObjectReference) error {
	return nil
}

func (f *fakeListView) IsListViewReady() bool { return true }

func (f *fakeListView) SetListViewNotReady(ctx context.Context) {}

// newDeleteVolumeTestManager builds a defaultManager whose DeleteVolume task is
// resumed from the operation store and whose task polling fails with waitErr.
func newDeleteVolumeTestManager(store *fakeOperationStore, waitErr error) *defaultManager {
	return newDeleteVolumeTestManagerWithListView(store, &fakeListView{addTaskErr: waitErr})
}

// newDeleteVolumeTestManagerWithListView is newDeleteVolumeTestManager with a
// caller supplied listview.
func newDeleteVolumeTestManagerWithListView(store *fakeOperationStore,
	listView ListViewIf) *defaultManager {
	return &defaultManager{
		virtualCenter:              newTestVirtualCenter(&vim25.Client{}),
		operationStore:             store,
		listViewIf:                 listView,
		idempotencyHandlingEnabled: true,
	}
}

// seedPendingDelete stores an "InProgress" record carrying taskID, which is what
// the driver persists after creating the CNS DeleteVolume task but before the
// task result is known.
func seedPendingDelete(store *fakeOperationStore, instanceName, taskID string) {
	store.records[instanceName] = cnsvolumeoperationrequest.CreateVolumeOperationRequestDetails(
		instanceName, "", "", 0, nil, metav1.Now(), taskID, "", "test-op-id",
		cnsvolumeoperationrequest.TaskInvocationStatusInProgress, "", "")
}

// TestDeleteVolumeRecordsErrorWhenTaskPollingFails covers the regression where a
// CNS DeleteVolume task that has already terminally failed was left recorded as
// "InProgress", so every retry re-read the same dead task instead of invoking
// CNS again.
func TestDeleteVolumeRecordsErrorWhenTaskPollingFails(t *testing.T) {
	const (
		volumeID     = "test-volume-id"
		instanceName = "delete-" + volumeID
		taskID       = "task-55858"
	)
	taskMoRef := vim25types.ManagedObjectReference{Type: "Task", Value: taskID}

	tests := []struct {
		name    string
		waitErr error
	}{
		{
			// vsanvcmgmtd fails pending CNS tasks at startup. The listview
			// reports this as a terminal task error.
			name:    "task terminally failed on CNS",
			waitErr: fmt.Errorf("%w: Failing pending CNS tasks during startup", ErrTaskFailed),
		},
		{
			name: "task missing from vCenter",
			waitErr: createFCDSoapFault(vim25types.ManagedObjectNotFound{Obj: taskMoRef},
				"The object has already been deleted or has not been completely created"),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx := logger.NewContextWithLogger(context.Background())
			store := newFakeOperationStore()
			seedPendingDelete(store, instanceName, taskID)
			m := newDeleteVolumeTestManager(store, tt.waitErr)

			_, err := m.deleteVolumeWithImprovedIdempotency(ctx, volumeID, true, false)
			assert.Error(t, err, "a failed DeleteVolume task must surface an error to the caller")

			record := store.records[instanceName]
			assert.NotNil(t, record)
			assert.Equal(t, cnsvolumeoperationrequest.TaskInvocationStatusError,
				record.OperationDetails.TaskStatus,
				"the failed task must be recorded as Error so the next retry creates a new task")
			assert.Equal(t, taskID, record.OperationDetails.TaskID,
				"the failed task ID must be retained for traceability")

			// The retry contract: an Error record must not be resumed.
			assert.False(t, IsTaskPending(record),
				"an Error record must not be treated as pending, otherwise the retry "+
					"re-reads the dead task instead of invoking CNS DeleteVolume again")
		})
	}
}

// TestDeleteVolumeRecordsErrorWhenTaskInfoIsEmpty covers waitOnTask returning a
// nil TaskInfo together with a nil error. That must not be reported to CSI as a
// successful delete, and the record must leave "InProgress" so the next retry
// invokes CNS again.
func TestDeleteVolumeRecordsErrorWhenTaskInfoIsEmpty(t *testing.T) {
	const (
		volumeID     = "test-volume-id"
		instanceName = "delete-" + volumeID
		taskID       = "task-55858"
	)
	ctx := logger.NewContextWithLogger(context.Background())
	store := newFakeOperationStore()
	seedPendingDelete(store, instanceName, taskID)
	m := newDeleteVolumeTestManagerWithListView(store, &fakeListView{deliverEmptyResult: true})

	faultType, err := m.deleteVolumeWithImprovedIdempotency(ctx, volumeID, true, false)
	assert.Error(t, err, "an empty task info must not be reported as a successful delete")
	assert.Equal(t, csifault.CSITaskInfoEmptyFault, faultType)

	record := store.records[instanceName]
	assert.NotNil(t, record)
	assert.Equal(t, cnsvolumeoperationrequest.TaskInvocationStatusError,
		record.OperationDetails.TaskStatus,
		"the record must be marked Error so the next retry creates a new task")
	assert.Equal(t, taskID, record.OperationDetails.TaskID,
		"the task ID must be retained for traceability")
	assert.False(t, IsTaskPending(record),
		"an Error record must not be treated as pending")
}

// TestDeleteVolumeResumesTaskOnListViewAdditionFailure pins the deliberate
// exception to the above: when the task could not be added to the listview the
// state of the operation on CNS is unknown, so the same task must be resumed
// rather than a second DeleteVolume issued.
func TestDeleteVolumeResumesTaskOnListViewAdditionFailure(t *testing.T) {
	const (
		volumeID     = "test-volume-id"
		instanceName = "delete-" + volumeID
		taskID       = "task-55858"
	)
	ctx := logger.NewContextWithLogger(context.Background())
	store := newFakeOperationStore()
	seedPendingDelete(store, instanceName, taskID)
	m := newDeleteVolumeTestManager(store,
		errors.New(listviewAdditionError+". task: task-55858, err: listview not ready"))

	_, err := m.deleteVolumeWithImprovedIdempotency(ctx, volumeID, true, false)
	assert.Error(t, err)

	record := store.records[instanceName]
	assert.NotNil(t, record)
	assert.True(t, IsTaskPending(record),
		"a listview addition failure must stay pending so the same task is resumed")
}

// seedSuccessfulDelete stores the record left behind by a completed delete.
func seedSuccessfulDelete(store *fakeOperationStore, instanceName string) {
	store.records[instanceName] = cnsvolumeoperationrequest.CreateVolumeOperationRequestDetails(
		instanceName, "", "", 0, nil, metav1.Now(), "old-task", "", "old-op-id",
		cnsvolumeoperationrequest.TaskInvocationStatusSuccess, "", "")
}

// cnsDeleteCall is one CNS DeleteVolume request seen by recordingCNS.
type cnsDeleteCall struct {
	volumeIDs  []string
	deleteDisk bool
}

// recordingCNS stands in for the CNS client's DeleteVolume. It records every
// request and answers with a fixed task or error.
type recordingCNS struct {
	calls   []cnsDeleteCall
	taskID  string
	callErr error
}

func (r *recordingCNS) deleteVolume(ctx context.Context, ids []cnstypes.CnsVolumeId,
	deleteDisk bool) (*object.Task, error) {
	call := cnsDeleteCall{deleteDisk: deleteDisk}
	for _, id := range ids {
		call.volumeIDs = append(call.volumeIDs, id.Id)
	}
	r.calls = append(r.calls, call)
	if r.callErr != nil {
		return nil, r.callErr
	}
	return object.NewTask(&vim25.Client{},
		vim25types.ManagedObjectReference{Type: "Task", Value: r.taskID}), nil
}

// cnsTaskInfo builds the terminal task state CNS reports for a DeleteVolume
// task, with fault set for a failed one.
func cnsTaskInfo(taskID, opID string, fault *vim25types.LocalizedMethodFault) *vim25types.TaskInfo {
	return &vim25types.TaskInfo{
		Task:         vim25types.ManagedObjectReference{Type: "Task", Value: taskID},
		ActivationId: opID,
		Result: cnstypes.CnsVolumeOperationBatchResult{
			VolumeResults: []cnstypes.BaseCnsVolumeOperationResult{
				&cnstypes.CnsVolumeOperationResult{Fault: fault},
			},
		},
	}
}

// TestDeleteVolumeWithSuccessRecord covers a prior Success delete record. The
// record must never stand in for a CNS delete, because the volume ID may have
// been registered again since (a restore recreates volumes under their original
// IDs) and CNS having nothing for the ID does not prove the backing disk is gone.
// The delete therefore always reaches CNS, and the outcome is whatever CNS says.
func TestDeleteVolumeWithSuccessRecord(t *testing.T) {
	const (
		volumeID     = "test-volume-id"
		instanceName = "delete-" + volumeID
		newTaskID    = "task-new"
		newOpID      = "op-new"
	)
	notFound := &vim25types.LocalizedMethodFault{Fault: &vim25types.NotFound{}}
	inUse := &vim25types.LocalizedMethodFault{Fault: &vim25types.ResourceInUse{}}

	tests := []struct {
		name         string
		callErr      error
		taskInfo     *vim25types.TaskInfo
		expectErr    bool
		expectStatus string
		expectTaskID string
	}{
		{
			name:         "volume registered again is deleted",
			taskInfo:     cnsTaskInfo(newTaskID, newOpID, nil),
			expectStatus: cnsvolumeoperationrequest.TaskInvocationStatusSuccess,
			expectTaskID: newTaskID,
		},
		{
			// A retry of a delete that really did complete: CNS has nothing left.
			name:         "volume already gone is reported as success",
			callErr:      createFCDSoapFault(vim25types.NotFound{}, "not found"),
			expectStatus: cnsvolumeoperationrequest.TaskInvocationStatusSuccess,
			expectTaskID: "",
		},
		{
			name:         "volume already gone reported on the task is success",
			taskInfo:     cnsTaskInfo(newTaskID, newOpID, notFound),
			expectStatus: cnsvolumeoperationrequest.TaskInvocationStatusSuccess,
			expectTaskID: newTaskID,
		},
		{
			name:         "CNS delete failure replaces the stale record with Error",
			taskInfo:     cnsTaskInfo(newTaskID, newOpID, inUse),
			expectErr:    true,
			expectStatus: cnsvolumeoperationrequest.TaskInvocationStatusError,
			expectTaskID: newTaskID,
		},
		{
			name:         "CNS call failure replaces the stale record with Error",
			callErr:      errors.New("vc unreachable"),
			expectErr:    true,
			expectStatus: cnsvolumeoperationrequest.TaskInvocationStatusError,
			expectTaskID: "",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx := logger.NewContextWithLogger(context.Background())
			store := newFakeOperationStore()
			seedSuccessfulDelete(store, instanceName)
			cns := &recordingCNS{taskID: newTaskID, callErr: tt.callErr}
			m := newDeleteVolumeTestManagerWithListView(store, &fakeListView{taskInfo: tt.taskInfo})
			m.cnsDeleteVolumeFunc = cns.deleteVolume

			_, err := m.deleteVolumeWithImprovedIdempotency(ctx, volumeID, true, false)
			if tt.expectErr {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
			}

			assert.Equal(t, []cnsDeleteCall{{volumeIDs: []string{volumeID}, deleteDisk: true}}, cns.calls,
				"a Success record must not stand in for a CNS DeleteVolume of the volume")
			record := store.records[instanceName]
			assert.NotNil(t, record)
			assert.Equal(t, tt.expectStatus, record.OperationDetails.TaskStatus)
			assert.Equal(t, tt.expectTaskID, record.OperationDetails.TaskID)
		})
	}
}

// TestDeleteVolumeWithoutRecordInvokesCNS is the baseline: with no record the
// delete reaches CNS exactly once and is recorded as a success.
func TestDeleteVolumeWithoutRecordInvokesCNS(t *testing.T) {
	const (
		volumeID     = "test-volume-id"
		instanceName = "delete-" + volumeID
	)
	ctx := logger.NewContextWithLogger(context.Background())
	store := newFakeOperationStore()
	cns := &recordingCNS{taskID: "task-1"}
	m := newDeleteVolumeTestManagerWithListView(store, &fakeListView{taskInfo: cnsTaskInfo("task-1", "op-1", nil)})
	m.cnsDeleteVolumeFunc = cns.deleteVolume

	_, err := m.deleteVolumeWithImprovedIdempotency(ctx, volumeID, false, false)
	assert.NoError(t, err)
	assert.Equal(t, []cnsDeleteCall{{volumeIDs: []string{volumeID}, deleteDisk: false}}, cns.calls)
	assert.Equal(t, cnsvolumeoperationrequest.TaskInvocationStatusSuccess,
		store.records[instanceName].OperationDetails.TaskStatus)
}

// TestDeleteVolumeResumesPendingTaskWithoutNewCNSCall guards the idempotency the
// record still provides: a retry that finds an in-flight task resumes it rather
// than issuing a second CNS DeleteVolume.
func TestDeleteVolumeResumesPendingTaskWithoutNewCNSCall(t *testing.T) {
	const (
		volumeID     = "test-volume-id"
		instanceName = "delete-" + volumeID
		taskID       = "task-55858"
	)
	ctx := logger.NewContextWithLogger(context.Background())
	store := newFakeOperationStore()
	seedPendingDelete(store, instanceName, taskID)
	cns := &recordingCNS{taskID: "must-not-be-used"}
	m := newDeleteVolumeTestManagerWithListView(store, &fakeListView{taskInfo: cnsTaskInfo(taskID, "op-1", nil)})
	m.cnsDeleteVolumeFunc = cns.deleteVolume

	_, err := m.deleteVolumeWithImprovedIdempotency(ctx, volumeID, true, false)
	assert.NoError(t, err)
	assert.Empty(t, cns.calls, "a pending task must be resumed, not replaced")
	assert.Equal(t, cnsvolumeoperationrequest.TaskInvocationStatusSuccess,
		store.records[instanceName].OperationDetails.TaskStatus)
}

// TestIsTaskPendingRetryContract covers IsTaskPending directly, since the choice
// between resuming a task and issuing a new one hangs entirely off it.
func TestIsTaskPendingRetryContract(t *testing.T) {
	tests := []struct {
		name          string
		taskStatus    string
		taskID        string
		errMsg        string
		expectPending bool
	}{
		{
			name:          "in progress with task ID resumes",
			taskStatus:    cnsvolumeoperationrequest.TaskInvocationStatusInProgress,
			taskID:        "task-55858",
			expectPending: true,
		},
		{
			name:          "in progress without task ID cannot be resumed",
			taskStatus:    cnsvolumeoperationrequest.TaskInvocationStatusInProgress,
			expectPending: false,
		},
		{
			name:          "error creates a new task",
			taskStatus:    cnsvolumeoperationrequest.TaskInvocationStatusError,
			taskID:        "task-55858",
			errMsg:        "Failing pending CNS tasks during startup",
			expectPending: false,
		},
		{
			name:          "listview addition error resumes the same task",
			taskStatus:    cnsvolumeoperationrequest.TaskInvocationStatusError,
			taskID:        "task-55858",
			errMsg:        listviewAdditionError + ". task: task-55858",
			expectPending: true,
		},
		{
			name:          "success is not pending",
			taskStatus:    cnsvolumeoperationrequest.TaskInvocationStatusSuccess,
			taskID:        "task-55858",
			expectPending: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			details := cnsvolumeoperationrequest.CreateVolumeOperationRequestDetails(
				"delete-test-volume-id", "", "", 0, nil, metav1.Now(), tt.taskID, "", "",
				tt.taskStatus, tt.errMsg, "")
			assert.Equal(t, tt.expectPending, IsTaskPending(details))
		})
	}
}
