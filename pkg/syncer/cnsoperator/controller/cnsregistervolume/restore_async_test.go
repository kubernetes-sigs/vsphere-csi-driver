// Copyright (c) 2026 Broadcom. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

package cnsregistervolume

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	cnstypes "github.com/vmware/govmomi/cns/types"
	vimtypes "github.com/vmware/govmomi/vim25/types"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/record"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"

	api "sigs.k8s.io/vsphere-csi-driver/v3/pkg/apis/cnsoperator/cnsregistervolume/v1alpha1"
	volumes "sigs.k8s.io/vsphere-csi-driver/v3/pkg/common/cns-lib/volume"
	vsphere "sigs.k8s.io/vsphere-csi-driver/v3/pkg/common/cns-lib/vsphere"
	"sigs.k8s.io/vsphere-csi-driver/v3/pkg/internalapis/cnsvolumeinfo"
	infoapi "sigs.k8s.io/vsphere-csi-driver/v3/pkg/internalapis/cnsvolumeinfo/v1alpha1"
)

// A fresh reconciler must see the reservation even before the first PV exists.
// The reservation blocks work, but only the PV/CVI stamp makes the loser terminal.
func TestAsyncRestoreReservationAcrossRequeuesAndRestart(t *testing.T) {
	withSnapServiceLocalProtection(t, true)
	ctx := context.Background()
	owner := testRestoreInstance("true")
	r := newRestoreReconcileHarness(t, owner, &fakeRestoreVolumeInfoService{})
	require.NoError(t, r.client.Get(ctx, instanceKey(owner), owner))
	unlock, _, result, proceed := r.startRestore(ctx, owner, reconcileRequestFor(owner), time.Second)
	require.True(t, proceed)
	require.NoError(t, r.reserveRestore(ctx, owner))
	unlock()
	assertRestoreUnlocked(t)

	competitor := testRestoreInstance("true")
	competitor.Name, competitor.UID = "competitor", types.UID("competitor-uid")
	require.NoError(t, r.client.Create(ctx, competitor))
	restarted := &ReconcileCnsRegisterVolume{client: r.client, apiReader: r.client, k8sclient: r.k8sclient,
		recorder: record.NewFakeRecorder(30), volumeInfoService: &fakeRestoreVolumeInfoService{}}
	for i := 0; i < 2; i++ {
		unlock, _, result, proceed = restarted.startRestore(ctx, competitor, reconcileRequestFor(competitor), time.Second)
		require.False(t, proceed)
		require.Nil(t, unlock)
		require.Positive(t, result.RequeueAfter)
		require.Empty(t, competitor.Status.Error)
		require.Empty(t, competitor.Annotations[infoapi.AnnRestoreNotProcessed])
		require.Equal(t, api.ReasonWaitingForRestoreOwner, condOf(competitor, api.ConditionReady).Reason)
	}
	// First stamp decides ownership, retaining #4402's not-processed semantics.
	_, err := r.k8sclient.CoreV1().PersistentVolumes().Create(ctx,
		testStampedPV(string(owner.UID), false), metav1.CreateOptions{})
	require.NoError(t, err)
	_, _, result, proceed = restarted.startRestore(ctx, competitor, reconcileRequestFor(competitor), time.Second)
	require.False(t, proceed)
	require.Zero(t, result.RequeueAfter)
	require.Equal(t, string(owner.UID), competitor.Annotations[infoapi.AnnRestoreNotProcessed])
	require.Equal(t, api.ReasonRestoreNotProcessed, condOf(competitor, api.ConditionReady).Reason)
}

func TestAsyncRestoreFailedAttemptReleasesReservation(t *testing.T) {
	withSnapServiceLocalProtection(t, true)
	ctx := context.Background()
	owner := testRestoreInstance("true")
	owner.Labels = map[string]string{labelRestoreVolumeID: owner.Spec.VolumeID}
	owner.Status.CnsTaskID = "task-failed"
	r, _, _ := newAsyncTestReconciler(t, owner, 40)
	r.volumeInfoService = &fakeRestoreVolumeInfoService{}
	seedOutcome(r.taskTracker, "task-failed", &cnsTaskOutcome{
		state: volumes.CreateVolumeTaskFailed, err: errors.New("disk locked")})
	_, retry, err := r.submitOrPollCreateVolume(ctx, owner, nil, asyncTestSpec(), time.Second)
	require.NoError(t, err)
	require.NotNil(t, retry)
	require.Empty(t, owner.Labels[labelRestoreVolumeID])
	require.Empty(t, owner.Status.CnsTaskID)
}

// Resume from the successful task result even when the result's task and all
// in-memory tracking have disappeared. Conditions alone must not authorize this.
func TestAsyncRestoreTaskResultSurvivesRestart(t *testing.T) {
	ctx := context.Background()
	cr := testRestoreInstance("true")
	cr.Status.CnsTaskID = "task-completed"
	r, _, _ := newAsyncTestReconciler(t, cr, 40)
	seedOutcome(r.taskTracker, cr.Status.CnsTaskID, &cnsTaskOutcome{state: volumes.CreateVolumeTaskSucceeded,
		volInfo: &volumes.CnsVolumeInfo{VolumeID: cnstypes.CnsVolumeId{Id: testRestoreVolumeID}}})
	_, retry, err := r.submitOrPollCreateVolume(ctx, cr, nil, asyncTestSpec(), time.Second)
	require.NoError(t, err)
	require.Nil(t, retry)
	require.Empty(t, cr.Status.CnsTaskID)
	require.Equal(t, testRestoreVolumeID, cr.Status.CnsVolumeID)
	stored := &api.CnsRegisterVolume{}
	require.NoError(t, r.client.Get(ctx, instanceKey(cr), stored))
	require.Equal(t, testRestoreVolumeID, stored.Status.CnsVolumeID)
	restarted := &ReconcileCnsRegisterVolume{client: r.client}
	old := submitCreateVolumeTaskFn
	t.Cleanup(func() { submitCreateVolumeTaskFn = old })
	submits := 0
	submitCreateVolumeTaskFn = func(context.Context, *vsphere.VirtualCenter, *cnstypes.CnsVolumeCreateSpec) (
		vimtypes.ManagedObjectReference, string, error) {
		submits++
		return taskRef("unexpected"), "", nil
	}
	for i := 0; i < 3; i++ {
		info, retry, err := restarted.submitOrPollCreateVolume(ctx, stored, nil, asyncTestSpec(), time.Second)
		require.NoError(t, err)
		require.Nil(t, retry)
		require.Equal(t, testRestoreVolumeID, info.VolumeID.Id)
	}
	require.Zero(t, submits, "bind/finalization retries must not submit again")
}

func TestAsyncRestoreResultWriteFailureKeepsTask(t *testing.T) {
	ctx := context.Background()
	cr := testRestoreInstance("true")
	cr.Status.CnsTaskID = "task-done"
	r, _, _ := newAsyncTestReconciler(t, cr, 40)
	failing := true
	r.client = interceptor.NewClient(r.client.(client.WithWatch), interceptor.Funcs{
		SubResourcePatch: func(ctx context.Context, c client.Client,
			sub string, obj client.Object, p client.Patch, opts ...client.SubResourcePatchOption) error {
			if failing {
				return errors.New("unavailable")
			}
			return c.SubResource(sub).Patch(ctx, obj, p, opts...)
		},
	})
	seedOutcome(r.taskTracker, cr.Status.CnsTaskID, &cnsTaskOutcome{state: volumes.CreateVolumeTaskSucceeded,
		volInfo: &volumes.CnsVolumeInfo{VolumeID: cnstypes.CnsVolumeId{Id: testRestoreVolumeID}}})
	_, retry, err := r.submitOrPollCreateVolume(ctx, cr, nil, asyncTestSpec(), time.Second)
	require.NoError(t, err)
	require.NotNil(t, retry)
	require.Equal(t, "task-done", cr.Status.CnsTaskID)
	require.Empty(t, cr.Status.CnsVolumeID)
	_, retained := r.taskTracker.outcome("task-done")
	require.True(t, retained)
	failing = false
	_, retry, err = r.submitOrPollCreateVolume(ctx, cr, nil, asyncTestSpec(), time.Second)
	require.NoError(t, err)
	require.Nil(t, retry)
	require.Equal(t, testRestoreVolumeID, cr.Status.CnsVolumeID)
}

func TestAsyncRestoreMetadataRetryAndWarning(t *testing.T) {
	withSnapServiceLocalProtection(t, true)
	ctx := context.Background()
	cr := testRestoreInstance("true")
	service := &fakeRestoreVolumeInfoService{handoverErr: errors.New("previous CVI terminating")}
	r := newRestoreReconcileHarness(t, cr, service, testStampedPV(string(cr.UID), false))
	require.NoError(t, r.client.Get(ctx, instanceKey(cr), cr))
	info := cnsvolumeinfo.RestoredVolumeInfo{
		VolumeID: testRestoreVolumeID, Namespace: cr.Namespace, RestoredBy: string(cr.UID)}
	require.Error(t, r.reconcileRestoreMetadata(ctx, cr, testRestorePVName, info))
	failed := condOf(cr, api.ConditionRestoreMetadataConfigured)
	transition := failed.LastTransitionTime
	require.Equal(t, api.ReasonRestoreMetadataFailed, condOf(cr, api.ConditionReady).Reason)
	require.Error(t, r.reconcileRestoreMetadata(ctx, cr, testRestorePVName, info))
	require.Equal(t, transition, condOf(cr, api.ConditionRestoreMetadataConfigured).LastTransitionTime)
	require.False(t, cr.Status.Registered)
	// DVI can disappear after the precheck. #4402's completed warning outcome
	// must not turn into a registration failure or a wait for full sync.
	service.handoverErr = nil
	service.handoverResult = seededResult(metav1.ConditionFalse, infoapi.ReasonDeletedVolumeInfoNotFound)
	require.NoError(t, r.reconcileRestoreMetadata(ctx, cr, testRestorePVName, info))
	require.Equal(t, metav1.ConditionTrue, condOf(cr, api.ConditionRestoreMetadataConfigured).Status)
	require.Equal(t, api.ReasonFinalizing, condOf(cr, api.ConditionReady).Reason)
	require.Nil(t, condOf(cr, infoapi.ConditionSnapshotSizeRestored), "CVI owns snapshot-size reporting")
}

func TestAsyncRestoreDeleteUnknownTaskPreservesReference(t *testing.T) {
	ctx := context.Background()
	cr := testRestoreInstance("true")
	cr.Status.CnsTaskID = "task-unknown"
	r, _, _ := newAsyncTestReconciler(t, cr, 40)
	seedOutcome(r.taskTracker, cr.Status.CnsTaskID, &cnsTaskOutcome{
		state: volumes.CreateVolumeTaskUnknown, err: errors.New("session lost")})
	require.NotNil(t, r.resolveInFlightTaskForDelete(ctx, cr, reconcileRequestFor(cr), time.Second))
	require.Equal(t, "task-unknown", cr.Status.CnsTaskID)
}

func TestAsyncRestoreDeleteCompletedTaskProtectsOtherPV(t *testing.T) {
	withSnapServiceLocalProtection(t, true)
	ctx := context.Background()
	cr := testRestoreInstance("true")
	cr.Status.CnsTaskID = "task-old"
	r, _, _ := newAsyncTestReconciler(t, cr, 40)
	r.volumeInfoService = &fakeRestoreVolumeInfoService{}
	_, err := r.k8sclient.CoreV1().PersistentVolumes().Create(ctx,
		testStampedPV("other-uid", false), metav1.CreateOptions{})
	require.NoError(t, err)
	seedOutcome(r.taskTracker, cr.Status.CnsTaskID, &cnsTaskOutcome{state: volumes.CreateVolumeTaskSucceeded,
		volInfo: &volumes.CnsVolumeInfo{VolumeID: cnstypes.CnsVolumeId{Id: testRestoreVolumeID}}})
	// No volume manager: an attempted CNS cleanup would panic.
	result, err := r.reconcileDelete(ctx, cr, reconcileRequestFor(cr), time.Second)
	require.NoError(t, err)
	require.Zero(t, result.RequeueAfter)
	_, err = r.k8sclient.CoreV1().PersistentVolumes().Get(ctx, testRestorePVName, metav1.GetOptions{})
	require.NoError(t, err)
}

func TestAsyncRestoreExecutionPatchRejectsStaleWriter(t *testing.T) {
	ctx := context.Background()
	cr := testRestoreInstance("true")
	c := fake.NewClientBuilder().WithScheme(condTestScheme()).WithObjects(cr).WithStatusSubresource(cr).Build()
	r := &ReconcileCnsRegisterVolume{client: c}
	require.NoError(t, c.Get(ctx, instanceKey(cr), cr))
	stale := cr.DeepCopy()
	require.NoError(t, r.persistCnsExecution(ctx, cr, "task-first", ""))
	err := r.persistCnsExecution(ctx, stale, "task-second", "")
	require.True(t, apierrors.IsConflict(err), "stale execution writes must conflict: %v", err)
	got := &api.CnsRegisterVolume{}
	require.NoError(t, c.Get(ctx, instanceKey(cr), got))
	require.Equal(t, "task-first", got.Status.CnsTaskID)
}
