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
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	cnstypes "github.com/vmware/govmomi/cns/types"
	"github.com/vmware/govmomi/vim25/soap"
	vim25types "github.com/vmware/govmomi/vim25/types"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	apitypes "k8s.io/apimachinery/pkg/types"
	k8sfake "k8s.io/client-go/kubernetes/fake"
	"k8s.io/client-go/tools/record"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/event"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	cnsregistervolumev1alpha1 "sigs.k8s.io/vsphere-csi-driver/v3/pkg/apis/cnsoperator/cnsregistervolume/v1alpha1"
	volumes "sigs.k8s.io/vsphere-csi-driver/v3/pkg/common/cns-lib/volume"
	cnsvsphere "sigs.k8s.io/vsphere-csi-driver/v3/pkg/common/cns-lib/vsphere"
	"sigs.k8s.io/vsphere-csi-driver/v3/pkg/common/conditions"
	commonconfig "sigs.k8s.io/vsphere-csi-driver/v3/pkg/common/config"
)

// newAsyncTestReconciler builds a reconciler wired for the async CNS path with
// a fake listview, so the submit/poll flow can be driven without a vCenter.
func newAsyncTestReconciler(t *testing.T, instance *cnsregistervolumev1alpha1.CnsRegisterVolume,
	maxInFlight int) (*ReconcileCnsRegisterVolume, *fakeListView, chan event.GenericEvent) {
	t.Helper()
	// recordEvent writes into this package-global map; tests that reach an
	// error path panic without it.
	backOffDuration = make(map[apitypes.NamespacedName]time.Duration)
	scheme := condTestScheme()
	c := fake.NewClientBuilder().WithScheme(scheme).
		WithObjects(instance).WithStatusSubresource(instance).Build()
	lv := newFakeListView()
	events := make(chan event.GenericEvent, 16)
	r := &ReconcileCnsRegisterVolume{
		client:      c,
		scheme:      scheme,
		k8sclient:   k8sfake.NewClientset(),
		recorder:    record.NewFakeRecorder(32),
		taskTracker: newCnsTaskTracker(lv, nil, events, maxInFlight),
		taskEvents:  events,
	}
	if err := c.Get(context.Background(), instanceKey(instance), instance); err != nil {
		t.Fatal(err)
	}
	return r, lv, events
}

func reconcileRequestFor(i *cnsregistervolumev1alpha1.CnsRegisterVolume) reconcile.Request {
	return reconcile.Request{NamespacedName: apitypes.NamespacedName{Name: i.Name, Namespace: i.Namespace}}
}

func asyncTestSpec() *cnstypes.CnsVolumeCreateSpec {
	return &cnstypes.CnsVolumeCreateSpec{Name: "static-pv-test", VolumeType: "BLOCK"}
}

// TestSubmitReleasesWorkerWithoutWaiting is the central guarantee of this
// change: dispatching the CNS task must return a requeue rather than block, so
// the reconciler worker is free to pick up another CnsRegisterVolume while this
// one's task runs on vCenter.
func TestSubmitReleasesWorkerWithoutWaiting(t *testing.T) {
	ctx := context.Background()
	instance := newCondTestInstance(cnsregistervolumev1alpha1.CnsRegisterVolumeStatus{})
	r, _, _ := newAsyncTestReconciler(t, instance, 40)

	orig := submitCreateVolumeTaskFn
	defer func() { submitCreateVolumeTaskFn = orig }()
	submitCreateVolumeTaskFn = func(_ context.Context, _ *cnsvsphere.VirtualCenter,
		_ *cnstypes.CnsVolumeCreateSpec) (vim25types.ManagedObjectReference, string, error) {
		// Deliberately does not complete: the task is "still running on CNS".
		return taskRef("task-running"), "", nil
	}

	done := make(chan struct{})
	var res *reconcileResultCapture
	go func() {
		defer close(done)
		volInfo, result, err := r.submitOrPollCreateVolume(ctx, instance, nil, asyncTestSpec(), time.Second)
		res = &reconcileResultCapture{volInfo: volInfo, requeueAfter: result.RequeueAfter, err: err}
	}()

	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("submit blocked on the CNS task; the reconciler worker would be held")
	}

	require.NotNil(t, res)
	assert.NoError(t, res.err)
	assert.Nil(t, res.volInfo, "no volume is available while the task is still running")
	assert.Greater(t, res.requeueAfter, time.Duration(0), "the CR must be requeued, not completed")
	assert.Equal(t, "task-running", instance.Status.CnsTaskID, "the task reference must be persisted")
	cond := conditions.Get(instance, cnsregistervolumev1alpha1.ConditionCnsVolumeRegistered)
	assert.NotNil(t, cond, "the submitted task must be reported")
	assert.Equal(t, cnsregistervolumev1alpha1.ReasonCnsTaskRunning, cond.Reason)
}

type reconcileResultCapture struct {
	volInfo      *volumes.CnsVolumeInfo
	requeueAfter time.Duration
	err          error
}

// TestPollReturnsVolumeOnceTaskCompletes verifies the resume half: a later pass
// picks up the pushed result and hands the volume back to the caller.
func TestPollReturnsVolumeOnceTaskCompletes(t *testing.T) {
	ctx := context.Background()
	instance := newCondTestInstance(cnsregistervolumev1alpha1.CnsRegisterVolumeStatus{
		Conditions: []metav1.Condition{{
			Type:   cnsregistervolumev1alpha1.ConditionCnsVolumeRegistered,
			Status: metav1.ConditionFalse,
			Reason: cnsregistervolumev1alpha1.ReasonCnsTaskRunning,
		}},
		CnsTaskID: "task-done",
	})
	r, _, _ := newAsyncTestReconciler(t, instance, 40)
	seedOutcome(r.taskTracker, "task-done", &cnsTaskOutcome{
		state:   volumes.CreateVolumeTaskSucceeded,
		volInfo: &volumes.CnsVolumeInfo{VolumeID: cnstypes.CnsVolumeId{Id: "vol-123"}},
	})

	volInfo, result, err := r.submitOrPollCreateVolume(ctx, instance, nil, asyncTestSpec(), time.Second)

	assert.NoError(t, err)
	assert.Nil(t, result, "a completed task must let the reconcile carry on, not requeue")
	require.NotNil(t, volInfo)
	assert.Equal(t, "vol-123", volInfo.VolumeID.Id)
	assert.Empty(t, instance.Status.CnsTaskID, "a settled task reference must be cleared")
	cond := conditions.Get(instance, cnsregistervolumev1alpha1.ConditionCnsVolumeRegistered)
	assert.NotNil(t, cond)
	assert.Equal(t, metav1.ConditionTrue, cond.Status,
		"a completed task must mark the volume as registered")
}

// TestUnknownTaskStateNeverResubmits is the safety property that matters most.
// A lost vCenter session surfaces as an Unknown outcome; treating it as a
// failure would resubmit CreateVolume against a disk whose first task may still
// be running.
func TestUnknownTaskStateNeverResubmits(t *testing.T) {
	ctx := context.Background()
	instance := newCondTestInstance(cnsregistervolumev1alpha1.CnsRegisterVolumeStatus{
		Conditions: []metav1.Condition{{
			Type:   cnsregistervolumev1alpha1.ConditionCnsVolumeRegistered,
			Status: metav1.ConditionFalse,
			Reason: cnsregistervolumev1alpha1.ReasonCnsTaskRunning,
		}},
		CnsTaskID: "task-unknown",
	})
	r, _, _ := newAsyncTestReconciler(t, instance, 40)
	oldReader := getCreateVolumeTaskResultFn
	t.Cleanup(func() { getCreateVolumeTaskResultFn = oldReader })
	getCreateVolumeTaskResultFn = func(context.Context, *cnsvsphere.VirtualCenter,
		vim25types.ManagedObjectReference, string) (volumes.CreateVolumeTaskState, *volumes.CnsVolumeInfo, string, error) {
		return volumes.CreateVolumeTaskUnknown, nil, "", errors.New("vc unavailable")
	}
	seedOutcome(r.taskTracker, "task-unknown", &cnsTaskOutcome{
		state: volumes.CreateVolumeTaskUnknown,
		err:   errors.New("vc session lost"),
	})

	submitted := false
	orig := submitCreateVolumeTaskFn
	defer func() { submitCreateVolumeTaskFn = orig }()
	submitCreateVolumeTaskFn = func(_ context.Context, _ *cnsvsphere.VirtualCenter,
		_ *cnstypes.CnsVolumeCreateSpec) (vim25types.ManagedObjectReference, string, error) {
		submitted = true
		return taskRef("task-second"), "", nil
	}

	volInfo, result, err := r.submitOrPollCreateVolume(ctx, instance, nil, asyncTestSpec(), time.Second)

	assert.NoError(t, err)
	assert.Nil(t, volInfo)
	require.NotNil(t, result)
	assert.Greater(t, result.RequeueAfter, time.Duration(0))
	assert.False(t, submitted, "an unknown task state must never trigger a second CreateVolume")
	assert.Equal(t, "task-unknown", instance.Status.CnsTaskID,
		"the task reference must be kept so the real state can be re-established")
	assert.Equal(t, 1, r.taskTracker.inFlight(), "the capacity slot must be held while the state is unknown")
}

// TestFailedTaskClearsReferenceAndRetries verifies that a genuine CNS fault is
// retried rather than becoming terminal, matching the behaviour before this
// change.
func TestFailedTaskClearsReferenceAndRetries(t *testing.T) {
	ctx := context.Background()
	instance := newCondTestInstance(cnsregistervolumev1alpha1.CnsRegisterVolumeStatus{
		Conditions: []metav1.Condition{{
			Type:   cnsregistervolumev1alpha1.ConditionCnsVolumeRegistered,
			Status: metav1.ConditionFalse,
			Reason: cnsregistervolumev1alpha1.ReasonCnsTaskRunning,
		}},
		CnsTaskID: "task-failed",
	})
	r, _, _ := newAsyncTestReconciler(t, instance, 40)
	seedOutcome(r.taskTracker, "task-failed", &cnsTaskOutcome{
		state: volumes.CreateVolumeTaskFailed,
		err:   errors.New("CnsFault: disk is locked"),
	})

	volInfo, result, err := r.submitOrPollCreateVolume(ctx, instance, nil, asyncTestSpec(), time.Second)

	assert.NoError(t, err)
	assert.Nil(t, volInfo)
	require.NotNil(t, result)
	assert.Greater(t, result.RequeueAfter, time.Duration(0), "a failed task must be retried, not made terminal")
	assert.Empty(t, instance.Status.CnsTaskID, "a spent task reference must not be reused")
	assert.NotEqual(t, cnsregistervolumev1alpha1.ReasonVolumeCleanedUp,
		conditions.GetReason(instance, cnsregistervolumev1alpha1.ConditionCnsVolumeRegistered),
		"a CreateVolume fault must not become a terminal Failed phase")
	assert.Equal(t, 0, r.taskTracker.inFlight(), "a failed task must return its capacity slot")
}

func TestUnknownNotificationReadsTaskState(t *testing.T) {
	for _, state := range []volumes.CreateVolumeTaskState{
		volumes.CreateVolumeTaskFailed, volumes.CreateVolumeTaskRunning, volumes.CreateVolumeTaskSucceeded,
	} {
		t.Run(string(state), func(t *testing.T) {
			cr := newCondTestInstance(cnsregistervolumev1alpha1.CnsRegisterVolumeStatus{CnsTaskID: "task-1"})
			cr.Labels = map[string]string{labelRestoreVolumeID: "restore-id"}
			r, _, _ := newAsyncTestReconciler(t, cr, 40)
			seedOutcome(r.taskTracker, "task-1", &cnsTaskOutcome{
				state: volumes.CreateVolumeTaskUnknown, err: errors.New("listview error")})
			old := getCreateVolumeTaskResultFn
			t.Cleanup(func() { getCreateVolumeTaskResultFn = old })
			reads := 0
			getCreateVolumeTaskResultFn = func(context.Context, *cnsvsphere.VirtualCenter,
				vim25types.ManagedObjectReference, string) (
				volumes.CreateVolumeTaskState, *volumes.CnsVolumeInfo, string, error) {
				reads++
				return state, &volumes.CnsVolumeInfo{VolumeID: cnstypes.CnsVolumeId{Id: "vol-1"}}, "", errors.New("task error")
			}
			_, retry, err := r.pollCreateVolume(context.Background(), cr, nil, asyncTestSpec(), time.Second)
			require.NoError(t, err)
			require.Equal(t, 1, reads)
			switch state {
			case volumes.CreateVolumeTaskRunning:
				require.NotNil(t, retry)
				require.Equal(t, "task-1", cr.Status.CnsTaskID)
				require.Equal(t, "restore-id", cr.Labels[labelRestoreVolumeID])
			case volumes.CreateVolumeTaskFailed:
				require.NotNil(t, retry)
				require.Empty(t, cr.Status.CnsTaskID)
				require.Empty(t, cr.Labels[labelRestoreVolumeID])
			case volumes.CreateVolumeTaskSucceeded:
				require.Nil(t, retry)
				require.Equal(t, "vol-1", cr.Status.CnsVolumeID)
				require.Equal(t, "restore-id", cr.Labels[labelRestoreVolumeID])
			}
		})
	}
}

type missingTaskQueryManager struct {
	volumes.Manager
	result       *cnstypes.CnsQueryResult
	err          error
	filter       cnstypes.CnsQueryFilter
	unregistered string
}

func (m *missingTaskQueryManager) QueryAllVolume(_ context.Context, filter cnstypes.CnsQueryFilter,
	_ cnstypes.CnsQuerySelection) (*cnstypes.CnsQueryResult, error) {
	m.filter = filter
	return m.result, m.err
}

func (m *missingTaskQueryManager) UnregisterVolume(_ context.Context, volumeID string, unregDisk bool) (string, error) {
	if !unregDisk {
		return "", errors.New("expected disk unregister")
	}
	m.unregistered = volumeID
	return "", nil
}

func TestMissingTaskRecovery(t *testing.T) {
	for _, tc := range []struct {
		name                 string
		deleting             bool
		result               *cnstypes.CnsQueryResult
		queryErr             error
		wantTask, wantVolume string
	}{
		{name: "retry keeps reservation", result: &cnstypes.CnsQueryResult{}, wantTask: ""},
		{name: "retry recovers volume", result: &cnstypes.CnsQueryResult{
			Volumes: []cnstypes.CnsVolume{{VolumeId: cnstypes.CnsVolumeId{Id: "vol-1"}}}}, wantVolume: "vol-1"},
		{name: "retry waits for query", queryErr: errors.New("unavailable"), wantTask: "task-1"},
		{name: "delete recovers volume", deleting: true, result: &cnstypes.CnsQueryResult{
			Volumes: []cnstypes.CnsVolume{{VolumeId: cnstypes.CnsVolumeId{Id: "vol-1"}}}}, wantVolume: "vol-1"},
		{name: "delete no volume", deleting: true, result: &cnstypes.CnsQueryResult{}},
		{name: "delete query fails", deleting: true, queryErr: errors.New("unavailable"), wantTask: "task-1"},
		{name: "delete ambiguous", deleting: true,
			result: &cnstypes.CnsQueryResult{Volumes: []cnstypes.CnsVolume{{}, {}}}, wantTask: "task-1"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			cr := newCondTestInstance(cnsregistervolumev1alpha1.CnsRegisterVolumeStatus{CnsTaskID: "task-1"})
			cr.Spec.VolumeID = ""
			cr.Labels = map[string]string{labelRestoreVolumeID: "restore-id"}
			if tc.deleting {
				now := metav1.Now()
				cr.DeletionTimestamp = &now
				cr.Finalizers = []string{"test"}
			}
			r, _, _ := newAsyncTestReconciler(t, cr, 40)
			cfg := &commonconfig.Config{}
			cfg.Global.ClusterID = "cluster-1"
			r.configInfo = commonconfig.NewConfigurationInfo(cfg)
			mgr := &missingTaskQueryManager{result: tc.result, err: tc.queryErr}
			r.volumeManager = mgr
			old := getCreateVolumeTaskResultFn
			t.Cleanup(func() { getCreateVolumeTaskResultFn = old })
			getCreateVolumeTaskResultFn = func(_ context.Context, _ *cnsvsphere.VirtualCenter,
				ref vim25types.ManagedObjectReference, _ string) (
				volumes.CreateVolumeTaskState, *volumes.CnsVolumeInfo, string, error) {
				fault := &soap.Fault{Code: "ServerFaultCode"}
				fault.Detail.Fault = vim25types.ManagedObjectNotFound{Obj: ref}
				return volumes.CreateVolumeTaskUnknown, nil, "", soap.WrapSoapFault(fault)
			}
			_, retry, err := r.resolveTaskDirectly(context.Background(), cr, nil, asyncTestSpec(), time.Second)
			require.NoError(t, err)
			require.Equal(t, tc.wantTask, cr.Status.CnsTaskID)
			require.Equal(t, tc.wantVolume, cr.Status.CnsVolumeID)
			require.Equal(t, "restore-id", cr.Labels[labelRestoreVolumeID])
			if tc.wantTask != "" || (!tc.deleting && tc.wantVolume == "") {
				require.NotNil(t, retry)
			} else {
				require.Nil(t, retry)
			}
			if tc.deleting {
				require.Equal(t, []string{"static-pv-test"}, mgr.filter.Names)
			}
		})
	}
}

func TestCompletedTaskSettlesBeforeStorageClassAdmission(t *testing.T) {
	ctx := context.Background()
	cr := newCondTestInstance(cnsregistervolumev1alpha1.CnsRegisterVolumeStatus{CnsTaskID: "task-1"})
	cr.Spec.VolumeID = ""
	cr.Spec.DiskURLPath = "disk.vmdk"
	cr.Spec.StorageClassName = "removed-class"
	r, _, _ := newAsyncTestReconciler(t, cr, 1)
	require.True(t, r.taskTracker.tryAcquireSlot(instanceKey(cr)))
	seedOutcome(r.taskTracker, "task-1", &cnsTaskOutcome{state: volumes.CreateVolumeTaskSucceeded,
		volInfo: &volumes.CnsVolumeInfo{VolumeID: cnstypes.CnsVolumeId{Id: "vol-1"}}})
	oldVC := taskVirtualCenterFn
	t.Cleanup(func() { taskVirtualCenterFn = oldVC })
	taskVirtualCenterFn = func(*ReconcileCnsRegisterVolume, context.Context) (*cnsvsphere.VirtualCenter, error) {
		return &cnsvsphere.VirtualCenter{}, nil
	}
	result, err := r.Reconcile(ctx, reconcileRequestFor(cr))
	require.NoError(t, err)
	require.Positive(t, result.RequeueAfter)
	stored := getCondTestInstance(t, r.client, cr)
	require.Empty(t, stored.Status.CnsTaskID)
	require.Equal(t, "vol-1", stored.Status.CnsVolumeID)
	require.False(t, stored.Status.Registered)
	require.Equal(t, 0, r.taskTracker.inFlight())
	require.Equal(t, cnsregistervolumev1alpha1.ReasonStoragePolicyLookupFailed,
		conditions.GetReason(stored, cnsregistervolumev1alpha1.ConditionSpecValidated))
	pvs, err := r.k8sclient.CoreV1().PersistentVolumes().List(ctx, metav1.ListOptions{})
	require.NoError(t, err)
	require.Empty(t, pvs.Items)
}

func TestMissingTaskLookupUsesVolumeIDForRestore(t *testing.T) {
	ctx := context.Background()
	cr := newCondTestInstance(cnsregistervolumev1alpha1.CnsRegisterVolumeStatus{CnsTaskID: "expired"})
	r, _, _ := newAsyncTestReconciler(t, cr, 40)
	cfg := &commonconfig.Config{}
	cfg.Global.ClusterID = "cluster-1"
	r.configInfo = commonconfig.NewConfigurationInfo(cfg)
	mgr := &missingTaskQueryManager{result: &cnstypes.CnsQueryResult{Volumes: []cnstypes.CnsVolume{
		{VolumeId: cnstypes.CnsVolumeId{Id: cr.Spec.VolumeID}},
	}}}
	r.volumeManager = mgr
	old := getCreateVolumeTaskResultFn
	t.Cleanup(func() { getCreateVolumeTaskResultFn = old })
	getCreateVolumeTaskResultFn = func(_ context.Context, _ *cnsvsphere.VirtualCenter,
		ref vim25types.ManagedObjectReference, _ string) (
		volumes.CreateVolumeTaskState, *volumes.CnsVolumeInfo, string, error) {
		fault := &soap.Fault{Code: "ServerFaultCode"}
		fault.Detail.Fault = vim25types.ManagedObjectNotFound{Obj: ref}
		return volumes.CreateVolumeTaskUnknown, nil, "", soap.WrapSoapFault(fault)
	}
	info, retry, err := r.resolveTaskDirectly(ctx, cr, nil, asyncTestSpec(), time.Second)
	require.NoError(t, err)
	require.Nil(t, retry)
	require.Equal(t, cr.Spec.VolumeID, info.VolumeID.Id)
	require.Equal(t, []cnstypes.CnsVolumeId{{Id: cr.Spec.VolumeID}}, mgr.filter.VolumeIds)
	require.Empty(t, mgr.filter.Names)
	require.Equal(t, []string{"cluster-1"}, mgr.filter.ContainerClusterIds)
}

func TestDeleteRecoversAndUnregistersMissingTaskVolume(t *testing.T) {
	ctx := context.Background()
	cr := newCondTestInstance(cnsregistervolumev1alpha1.CnsRegisterVolumeStatus{CnsTaskID: "expired"})
	cr.Spec.VolumeID = ""
	cr.Spec.DiskURLPath = "disk.vmdk"
	now := metav1.Now()
	cr.DeletionTimestamp, cr.Finalizers = &now, []string{"test"}
	r, _, _ := newAsyncTestReconciler(t, cr, 40)
	cfg := &commonconfig.Config{}
	cfg.Global.ClusterID = "cluster-1"
	r.configInfo = commonconfig.NewConfigurationInfo(cfg)
	mgr := &missingTaskQueryManager{result: &cnstypes.CnsQueryResult{Volumes: []cnstypes.CnsVolume{
		{VolumeId: cnstypes.CnsVolumeId{Id: "vol-1"}},
	}}}
	r.volumeManager = mgr
	seedOutcome(r.taskTracker, "expired", &cnsTaskOutcome{state: volumes.CreateVolumeTaskUnknown})
	oldReader, oldVC := getCreateVolumeTaskResultFn, taskVirtualCenterFn
	t.Cleanup(func() { getCreateVolumeTaskResultFn, taskVirtualCenterFn = oldReader, oldVC })
	taskVirtualCenterFn = func(*ReconcileCnsRegisterVolume, context.Context) (*cnsvsphere.VirtualCenter, error) {
		return &cnsvsphere.VirtualCenter{}, nil
	}
	getCreateVolumeTaskResultFn = func(_ context.Context, _ *cnsvsphere.VirtualCenter,
		ref vim25types.ManagedObjectReference, _ string) (
		volumes.CreateVolumeTaskState, *volumes.CnsVolumeInfo, string, error) {
		fault := &soap.Fault{Code: "ServerFaultCode"}
		fault.Detail.Fault = vim25types.ManagedObjectNotFound{Obj: ref}
		return volumes.CreateVolumeTaskUnknown, nil, "", soap.WrapSoapFault(fault)
	}
	retry := r.resolveInFlightTaskForDelete(ctx, cr, reconcileRequestFor(cr), time.Second)
	require.Nil(t, retry)
	require.Equal(t, "vol-1", mgr.unregistered)
	require.Empty(t, cr.Status.CnsTaskID)
	require.Empty(t, cr.Status.CnsVolumeID)
}

// TestCapHoldsBackSubmissions verifies the explicit replacement for the
// implicit throttle the blocking workers used to provide.
func TestCapHoldsBackSubmissions(t *testing.T) {
	ctx := context.Background()
	instance := newCondTestInstance(cnsregistervolumev1alpha1.CnsRegisterVolumeStatus{})
	r, _, _ := newAsyncTestReconciler(t, instance, 1)
	// Another CR already holds the only slot.
	require.True(t, r.taskTracker.tryAcquireSlot(apitypes.NamespacedName{Name: "other", Namespace: "test-ns"}))

	submitted := false
	orig := submitCreateVolumeTaskFn
	defer func() { submitCreateVolumeTaskFn = orig }()
	submitCreateVolumeTaskFn = func(_ context.Context, _ *cnsvsphere.VirtualCenter,
		_ *cnstypes.CnsVolumeCreateSpec) (vim25types.ManagedObjectReference, string, error) {
		submitted = true
		return taskRef("task-x"), "", nil
	}

	volInfo, result, err := r.submitOrPollCreateVolume(ctx, instance, nil, asyncTestSpec(), time.Second)

	assert.NoError(t, err)
	assert.Nil(t, volInfo)
	require.NotNil(t, result)
	assert.False(t, submitted, "no CNS task may be dispatched once the in-flight cap is reached")
	assert.Equal(t, cnsregistervolumev1alpha1.ReasonWaitingForTaskSlot,
		conditions.GetReason(instance, cnsregistervolumev1alpha1.ConditionCnsVolumeRegistered),
		"a CR waiting for a slot stays Pending and holds no worker")
	assert.Empty(t, instance.Status.CnsTaskID)
}

// TestRestartResubscribesToTask verifies recovery after a syncer restart: the
// listview is in-process and starts empty, so a CR resumed from its stored task
// ID must re-register rather than give up or resubmit.
func TestRestartResubscribesToTask(t *testing.T) {
	ctx := context.Background()
	instance := newCondTestInstance(cnsregistervolumev1alpha1.CnsRegisterVolumeStatus{
		Conditions: []metav1.Condition{{
			Type:   cnsregistervolumev1alpha1.ConditionCnsVolumeRegistered,
			Status: metav1.ConditionFalse,
			Reason: cnsregistervolumev1alpha1.ReasonCnsTaskRunning,
		}},
		CnsTaskID: "task-resumed",
	})
	// Fresh tracker with no knowledge of the task, as after a restart.
	r, lv, _ := newAsyncTestReconciler(t, instance, 40)

	submitted := false
	orig := submitCreateVolumeTaskFn
	defer func() { submitCreateVolumeTaskFn = orig }()
	submitCreateVolumeTaskFn = func(_ context.Context, _ *cnsvsphere.VirtualCenter,
		_ *cnstypes.CnsVolumeCreateSpec) (vim25types.ManagedObjectReference, string, error) {
		submitted = true
		return taskRef("task-second"), "", nil
	}

	_, result, err := r.submitOrPollCreateVolume(ctx, instance, nil, asyncTestSpec(), time.Second)

	assert.NoError(t, err)
	require.NotNil(t, result)
	assert.False(t, submitted, "a resumed CR must re-subscribe to its existing task, not submit a new one")
	assert.True(t, r.taskTracker.isWatching("task-resumed"), "the resumed task must be watched again")

	lv.mu.Lock()
	_, registered := lv.channels["task-resumed"]
	lv.mu.Unlock()
	assert.True(t, registered, "the resumed task must be re-registered with the listview")
}

// TestDeleteWaitsForInFlightTask verifies the finalizer is held while a CNS
// task is running, so the CR cannot be removed leaving an orphaned volume.
func TestDeleteWaitsForInFlightTask(t *testing.T) {
	ctx := context.Background()
	instance := newCondTestInstance(cnsregistervolumev1alpha1.CnsRegisterVolumeStatus{
		Conditions: []metav1.Condition{{
			Type:   cnsregistervolumev1alpha1.ConditionCnsVolumeRegistered,
			Status: metav1.ConditionFalse,
			Reason: cnsregistervolumev1alpha1.ReasonCnsTaskRunning,
		}},
		CnsTaskID: "task-inflight",
	})
	r, _, _ := newAsyncTestReconciler(t, instance, 40)
	req := reconcileRequestFor(instance)

	// No outcome recorded: the task is still running.
	result := r.resolveInFlightTaskForDelete(ctx, instance, req, time.Second)

	require.NotNil(t, result, "deletion must wait while the CNS task is in flight")
	assert.Greater(t, result.RequeueAfter, time.Duration(0))
	assert.Equal(t, "task-inflight", instance.Status.CnsTaskID,
		"the task reference must be kept while waiting")
}

// TestDeleteProceedsOnceTaskSettles verifies the other half: once the task is
// done the reference is cleared and the normal delete path is allowed to run.
func TestDeleteProceedsOnceTaskSettles(t *testing.T) {
	ctx := context.Background()
	instance := newCondTestInstance(cnsregistervolumev1alpha1.CnsRegisterVolumeStatus{
		Conditions: []metav1.Condition{{
			Type:   cnsregistervolumev1alpha1.ConditionCnsVolumeRegistered,
			Status: metav1.ConditionFalse,
			Reason: cnsregistervolumev1alpha1.ReasonCnsTaskRunning,
		}},
		CnsTaskID: "task-settled",
	})
	r, _, _ := newAsyncTestReconciler(t, instance, 40)
	// Failed task: nothing was created, so there is nothing to clean up.
	seedOutcome(r.taskTracker, "task-settled", &cnsTaskOutcome{
		state: volumes.CreateVolumeTaskFailed,
		err:   errors.New("create failed"),
	})

	result := r.resolveInFlightTaskForDelete(ctx, instance, reconcileRequestFor(instance), time.Second)

	assert.Nil(t, result, "a settled task must let deletion continue")
	assert.Empty(t, instance.Status.CnsTaskID)
	assert.Equal(t, 0, r.taskTracker.inFlight(), "deletion must release the capacity slot")
}

// TestIsPVCBoundNowDoesNotWatch verifies the bind check is a plain read of
// current state, replacing a 60s per-CR watch that blocked the worker.
func TestIsPVCBoundNowDoesNotWatch(t *testing.T) {
	ctx := context.Background()
	bound := &corev1.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{Name: "bound-pvc", Namespace: "test-ns"},
		Status:     corev1.PersistentVolumeClaimStatus{Phase: corev1.ClaimBound},
	}
	pending := &corev1.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{Name: "pending-pvc", Namespace: "test-ns"},
		Status:     corev1.PersistentVolumeClaimStatus{Phase: corev1.ClaimPending},
	}
	client := k8sfake.NewClientset(bound, pending)

	start := time.Now()
	isBound, err := isPVCBoundNow(ctx, client, bound)
	assert.NoError(t, err)
	assert.True(t, isBound)

	isBound, err = isPVCBoundNow(ctx, client, pending)
	assert.NoError(t, err)
	assert.False(t, isBound)
	assert.Less(t, time.Since(start), time.Second,
		"the bind check must return immediately rather than waiting for the PVC to bind")

	// A PVC that does not exist yet is simply not bound, not an error.
	missing := &corev1.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{Name: "nope", Namespace: "test-ns"},
	}
	isBound, err = isPVCBoundNow(ctx, client, missing)
	assert.NoError(t, err)
	assert.False(t, isBound)
}
