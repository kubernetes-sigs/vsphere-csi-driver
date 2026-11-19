// Copyright (c) 2026 Broadcom. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

package cnsregistervolume

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/agiledragon/gomonkey/v2"
	"github.com/stretchr/testify/require"
	vmoperator "github.com/vmware-tanzu/vm-operator/api/v1alpha5"
	cnstypes "github.com/vmware/govmomi/cns/types"
	v1 "k8s.io/api/core/v1"
	storagev1 "k8s.io/api/storage/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	k8sfake "k8s.io/client-go/kubernetes/fake"
	"k8s.io/client-go/tools/record"
	ctrlclient "sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
	api "sigs.k8s.io/vsphere-csi-driver/v3/pkg/apis/cnsoperator/cnsregistervolume/v1alpha1"
	cnsvsphere "sigs.k8s.io/vsphere-csi-driver/v3/pkg/common/cns-lib/vsphere"
	"sigs.k8s.io/vsphere-csi-driver/v3/pkg/csi/service/common"
	"sigs.k8s.io/vsphere-csi-driver/v3/pkg/csi/service/common/commonco"
	"sigs.k8s.io/vsphere-csi-driver/v3/pkg/syncer"
)

func TestVMDeleteProtectionRetryAndRestart(t *testing.T) {
	ctx := context.Background()
	cr := newCondTestInstance(api.CnsRegisterVolumeStatus{})
	applyConditions(cr, trueCondition(api.ConditionPvcBound, api.ReasonBound, "PVC is bound"))
	pvc := vmImportPVC("test-pvc", cr.Namespace, "test-vm")
	removable := false
	vm := vmWithVolume("test-vm", cr.Namespace, pvc.Name, &removable)
	scheme := condTestScheme()
	require.NoError(t, vmoperator.AddToScheme(scheme))
	patchFails := false
	c := fake.NewClientBuilder().WithScheme(scheme).WithObjects(cr, pvc, vm).
		WithStatusSubresource(cr).WithInterceptorFuncs(interceptor.Funcs{
		Patch: func(ctx context.Context, client ctrlclient.WithWatch, obj ctrlclient.Object,
			patch ctrlclient.Patch, opts ...ctrlclient.PatchOption) error {
			if _, ok := obj.(*v1.PersistentVolumeClaim); ok && patchFails {
				return errors.New("annotation write unavailable")
			}
			return client.Patch(ctx, obj, patch, opts...)
		},
	}).Build()
	calls := 0
	clearErr := errors.New("operation cannot be performed in current state")
	manager := &mockVolumeManager{unprotectVolumeFromVMDeletionFunc: func(ctx context.Context, _ string) error {
		calls++
		deadline, ok := ctx.Deadline()
		require.True(t, ok)
		require.InDelta(t, vmDeleteProtectionClearTimeout.Seconds(), time.Until(deadline).Seconds(), 1)
		return clearErr
	}}
	r := &ReconcileCnsRegisterVolume{client: c, volumeManager: manager, recorder: record.NewFakeRecorder(20)}
	key := types.NamespacedName{Name: cr.Name, Namespace: cr.Namespace}
	backOffDuration = map[types.NamespacedName]time.Duration{key: time.Second}
	_, retry := r.reconcileVMDeleteProtection(ctx, cr, pvc, "vol-1")
	require.Equal(t, vmDeleteProtectionRetryInterval, retry.RequeueAfter)
	got := getCondTestInstance(t, c, cr)
	require.False(t, got.Status.Registered)
	failure := condOf(got, api.ConditionVMDeleteProtectionConfigured)
	require.Equal(t, api.ReasonClearKeepAfterDeleteVmFailed, failure.Reason)
	require.Equal(t, failure.Reason, condOf(got, api.ConditionReady).Reason)
	require.Equal(t, metav1.ConditionTrue, condOf(got, api.ConditionPvcBound).Status)
	require.Equal(t, time.Second, backOffDuration[key], "fixed retries must not inflate shared backoff")

	// A new reconciler and freshly fetched CR simulate a restart, retaining no
	// in-memory completion state. Identical failures preserve transition time.
	r = &ReconcileCnsRegisterVolume{client: c, volumeManager: manager, recorder: record.NewFakeRecorder(20)}
	_, retry = r.reconcileVMDeleteProtection(ctx, got, pvc, "vol-1")
	require.NotNil(t, retry)
	require.Equal(t, failure.LastTransitionTime, condOf(getCondTestInstance(t, c, cr),
		api.ConditionVMDeleteProtectionConfigured).LastTransitionTime)

	clearErr = context.DeadlineExceeded
	_, retry = r.reconcileVMDeleteProtection(ctx, got, pvc, "vol-1")
	require.NotNil(t, retry)
	require.Equal(t, api.ReasonClearKeepAfterDeleteVmTimedOut,
		condOf(getCondTestInstance(t, c, cr), api.ConditionVMDeleteProtectionConfigured).Reason)

	clearErr, patchFails = nil, true
	_, retry = r.reconcileVMDeleteProtection(ctx, got, pvc, "vol-1")
	require.NotNil(t, retry)
	require.Equal(t, api.ReasonPersistClearResultFailed,
		condOf(getCondTestInstance(t, c, cr), api.ConditionVMDeleteProtectionConfigured).Reason)
	require.NoError(t, c.Get(ctx, ctrlclient.ObjectKeyFromObject(pvc), pvc))
	require.Empty(t, pvc.Annotations[common.AnnVMDeleteProtectionCleared])

	patchFails = false
	updated, retry := r.reconcileVMDeleteProtection(ctx, got, pvc, "vol-1")
	require.Nil(t, retry)
	require.Equal(t, "true", updated.Annotations[common.AnnVMDeleteProtectionCleared])
	require.False(t, getCondTestInstance(t, c, cr).Status.Registered, "only finalization may declare success")
	callsBefore := calls
	// Even with no VM available, the durable annotation skips the remote call.
	require.NoError(t, c.Delete(ctx, vm))
	got = getCondTestInstance(t, c, cr)
	_, retry = r.reconcileVMDeleteProtection(ctx, got, updated, "vol-1")
	require.Nil(t, retry)
	require.Equal(t, callsBefore, calls)
	require.Equal(t, api.ReasonKeepAfterDeleteVmCleared,
		condOf(getCondTestInstance(t, c, cr), api.ConditionVMDeleteProtectionConfigured).Reason)
	require.NoError(t, setInstanceSuccess(ctx, r, got, updated.Name, updated.UID, "registered"))
	got = getCondTestInstance(t, c, cr)
	require.True(t, got.Status.Registered)
	require.Empty(t, got.Status.Error)
	require.Equal(t, metav1.ConditionTrue, condOf(got, api.ConditionReady).Status)
}

func TestVMDeleteProtectionNotRequired(t *testing.T) {
	cr := newCondTestInstance(api.CnsRegisterVolumeStatus{})
	c, _ := newCapturingClient(cr, nil)
	r := &ReconcileCnsRegisterVolume{client: c}
	_, retry := r.reconcileVMDeleteProtection(context.Background(), cr, &v1.PersistentVolumeClaim{}, "vol-1")
	require.Nil(t, retry)
	require.Equal(t, api.ReasonNotRequired,
		condOf(getCondTestInstance(t, c, cr), api.ConditionVMDeleteProtectionConfigured).Reason)
}

// Exercise the real Reconcile ordering: an imported PVC can bind while the
// VSLM operation fails, and only a later successful clear permits registration.
func TestReconcileBindsBeforeVMDeleteProtection(t *testing.T) {
	ctx := context.Background()
	patches := gomonkey.NewPatches()
	defer patches.Reset()
	oldCO, oldFSS, oldStretch, oldShared := commonco.ContainerOrchestratorUtility,
		fssEnabledOverride, syncer.IsPodVMOnStretchSupervisorFSSEnabled, isSharedDiskEnabled
	defer func() {
		commonco.ContainerOrchestratorUtility, fssEnabledOverride = oldCO, oldFSS
		syncer.IsPodVMOnStretchSupervisorFSSEnabled, isSharedDiskEnabled = oldStretch, oldShared
	}()
	r, _, _ := newHostLocalReconcileFixture(patches, "none")
	fssEnabledOverride = func(string) bool { return false }
	syncer.IsPodVMOnStretchSupervisorFSSEnabled, isSharedDiskEnabled = false, false
	require.NoError(t, vmoperator.AddToScheme(r.scheme))
	removable := false
	pvc := vmImportPVC("test-pvc", "test-ns", "test-vm")
	scName := "test-sc"
	pvc.Spec.StorageClassName = &scName
	pvc.Spec.Resources.Requests = v1.ResourceList{v1.ResourceStorage: resource.MustParse("1Gi")}
	require.NoError(t, r.client.Create(ctx, pvc))
	require.NoError(t, r.client.Create(ctx, vmWithVolume("test-vm", "test-ns", pvc.Name, &removable)))
	sc := &storagev1.StorageClass{ObjectMeta: metav1.ObjectMeta{Name: scName},
		Parameters: map[string]string{"storagePolicyID": "dummy-storage-policy-id"}}
	quota := &v1.ResourceQuota{ObjectMeta: metav1.ObjectMeta{Name: "quota", Namespace: pvc.Namespace},
		Spec: v1.ResourceQuotaSpec{Hard: v1.ResourceList{
			v1.ResourceName(scName + scResourceNameSuffix): resource.MustParse("10Gi"),
		}}}
	r.k8sclient = k8sfake.NewClientset(sc, quota, pvc)
	patches.ApplyFunc(isDatastoreAccessibleToCluster,
		func(context.Context, *cnsvsphere.VirtualCenter, string, string) bool {
			return true
		})
	calls := 0
	fail := true
	r.volumeManager.(*mockVolumeManager).unprotectVolumeFromVMDeletionFunc = func(context.Context, string) error {
		calls++
		actual, err := r.k8sclient.CoreV1().PersistentVolumeClaims(pvc.Namespace).Get(ctx, pvc.Name, metav1.GetOptions{})
		require.NoError(t, err)
		require.Equal(t, v1.ClaimBound, actual.Status.Phase, "clear must run after binding")
		if fail {
			return errors.New("transient VSLM state")
		}
		return nil
	}
	r.volumeManager = &protectionTestVolumeManager{r.volumeManager.(*mockVolumeManager)}
	defer wireAsyncCnsTaskSuccess(r)()
	req := reconcile.Request{NamespacedName: types.NamespacedName{Name: "test-volume", Namespace: pvc.Namespace}}
	result, err := reconcileUntilSettled(ctx, r, req)
	require.NoError(t, err)
	require.Equal(t, pvcBindPollInterval, result.RequeueAfter)
	require.Zero(t, calls, "an unbound PVC must not attempt clearing")
	pvName := staticPvNamePrefix + "dummy-volume-id"
	pv, err := r.k8sclient.CoreV1().PersistentVolumes().Get(ctx, pvName, metav1.GetOptions{})
	require.NoError(t, err, "PV must exist even while clearing would fail")
	pvc, err = r.k8sclient.CoreV1().PersistentVolumeClaims(pvc.Namespace).Get(ctx, pvc.Name, metav1.GetOptions{})
	require.NoError(t, err)
	pvc.Spec.VolumeName, pvc.Status.Phase = pv.Name, v1.ClaimBound
	_, err = r.k8sclient.CoreV1().PersistentVolumeClaims(pvc.Namespace).Update(ctx, pvc, metav1.UpdateOptions{})
	require.NoError(t, err)
	_, err = r.k8sclient.CoreV1().PersistentVolumeClaims(pvc.Namespace).UpdateStatus(ctx, pvc, metav1.UpdateOptions{})
	require.NoError(t, err)
	result, err = reconcileUntilSettled(ctx, r, req)
	require.NoError(t, err)
	require.Equal(t, vmDeleteProtectionRetryInterval, result.RequeueAfter)
	cr := &api.CnsRegisterVolume{}
	require.NoError(t, r.client.Get(ctx, req.NamespacedName, cr))
	require.False(t, cr.Status.Registered)
	require.Equal(t, metav1.ConditionTrue, condOf(cr, api.ConditionPvcBound).Status)
	require.Equal(t, api.ReasonClearKeepAfterDeleteVmFailed, condOf(cr, api.ConditionReady).Reason)
	require.Positive(t, calls)
	fail = false
	result, err = reconcileUntilSettled(ctx, r, req)
	require.NoError(t, err)
	require.Zero(t, result.RequeueAfter)
	require.NoError(t, r.client.Get(ctx, req.NamespacedName, cr))
	require.True(t, cr.Status.Registered)
	require.Equal(t, metav1.ConditionTrue, condOf(cr, api.ConditionVMDeleteProtectionConfigured).Status)
}

// Supply backend capacity through the interface as well as the fixture's
// QueryVolumeByID patch, so this test also works with compiler inlining enabled.
type protectionTestVolumeManager struct{ *mockVolumeManager }

func (m *protectionTestVolumeManager) QueryVolumeAsync(context.Context,
	cnstypes.CnsQueryFilter, *cnstypes.CnsQuerySelection) (*cnstypes.CnsQueryResult, error) {
	return &cnstypes.CnsQueryResult{Volumes: []cnstypes.CnsVolume{{
		VolumeId:     cnstypes.CnsVolumeId{Id: "dummy-volume-id"},
		DatastoreUrl: "dummy-ds-url", StoragePolicyId: "dummy-storage-policy-id",
		BackingObjectDetails: &cnstypes.CnsBlockBackingDetails{
			CnsBackingObjectDetails: cnstypes.CnsBackingObjectDetails{CapacityInMb: 1024},
		},
	}}}, nil
}

func TestDeleteWhileVMDeleteProtectionPendingPreservesBoundVolume(t *testing.T) {
	ctx := context.Background()
	cr := newCondTestInstance(api.CnsRegisterVolumeStatus{})
	cr.Finalizers = []string{"cns.vmware.com/register-volume"}
	cr.Spec.PvcName = "test-pvc"
	applyConditions(cr,
		trueCondition(api.ConditionPvcBound, api.ReasonBound, "PVC is bound"),
		falseCondition(api.ConditionVMDeleteProtectionConfigured,
			api.ReasonClearKeepAfterDeleteVmFailed, "Waiting to retry"))
	pv := &v1.PersistentVolume{
		ObjectMeta: metav1.ObjectMeta{Name: staticPvNamePrefix + cr.Spec.VolumeID},
		Status:     v1.PersistentVolumeStatus{Phase: v1.VolumeBound},
	}
	pvc := &v1.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{Name: cr.Spec.PvcName, Namespace: cr.Namespace},
		Spec:       v1.PersistentVolumeClaimSpec{VolumeName: pv.Name},
		Status:     v1.PersistentVolumeClaimStatus{Phase: v1.ClaimBound},
	}
	c, _ := newCapturingClient(cr, nil)
	r := &ReconcileCnsRegisterVolume{client: c, k8sclient: k8sfake.NewClientset(pv, pvc)}
	key := types.NamespacedName{Name: cr.Name, Namespace: cr.Namespace}
	result, err := r.reconcileDelete(ctx, cr, reconcile.Request{NamespacedName: key}, time.Second)
	require.NoError(t, err)
	require.Zero(t, result.RequeueAfter)
	require.Empty(t, getCondTestInstance(t, c, cr).Finalizers)
	_, err = r.k8sclient.CoreV1().PersistentVolumes().Get(ctx, pv.Name, metav1.GetOptions{})
	require.NoError(t, err)
	_, err = r.k8sclient.CoreV1().PersistentVolumeClaims(pvc.Namespace).Get(ctx, pvc.Name, metav1.GetOptions{})
	require.NoError(t, err)
}

// protectionTestEnv builds the VM-import fixture shared by the abandon tests:
// a bound CnsRegisterVolume, a VM-owned PVC marked non-removable, and a
// reconciler whose VSLM clear behaves as the caller specifies. seedPVC=false
// models garbage collection having already removed the PVC.
func protectionTestEnv(t *testing.T, seedPVC bool, pvcMutate func(*v1.PersistentVolumeClaim),
	clearErr error, intercept interceptor.Funcs) (*ReconcileCnsRegisterVolume, ctrlclient.Client,
	*api.CnsRegisterVolume, *v1.PersistentVolumeClaim, *int) {
	t.Helper()
	cr := newCondTestInstance(api.CnsRegisterVolumeStatus{})
	cr.Spec.PvcName = "test-pvc"
	applyConditions(cr, trueCondition(api.ConditionPvcBound, api.ReasonBound, "PVC is bound"))
	pvc := vmImportPVC("test-pvc", cr.Namespace, "test-vm")
	if pvcMutate != nil {
		pvcMutate(pvc)
	}
	removable := false
	vm := vmWithVolume("test-vm", cr.Namespace, pvc.Name, &removable)
	scheme := condTestScheme()
	require.NoError(t, vmoperator.AddToScheme(scheme))
	objs := []ctrlclient.Object{cr, vm}
	if seedPVC {
		objs = append(objs, pvc)
	}
	c := fake.NewClientBuilder().WithScheme(scheme).WithObjects(objs...).
		WithStatusSubresource(cr).WithInterceptorFuncs(intercept).Build()
	calls := 0
	manager := &mockVolumeManager{unprotectVolumeFromVMDeletionFunc: func(context.Context, string) error {
		calls++
		return clearErr
	}}
	r := &ReconcileCnsRegisterVolume{client: c, volumeManager: manager, recorder: record.NewFakeRecorder(20)}
	backOffDuration = map[types.NamespacedName]time.Duration{}
	return r, c, cr, pvc, &calls
}

// TestVMDeleteProtectionAbandonedWhenPvcTerminating covers the 1767699 shape:
// the VSLM clear keeps failing while the VM is being destroyed. Retrying can no
// longer change what vSphere leaves on the datastore, so the step must stop
// rather than issue a clear every vmDeleteProtectionRetryInterval forever.
func TestVMDeleteProtectionAbandonedWhenPvcTerminating(t *testing.T) {
	ctx := context.Background()
	deleting := func(pvc *v1.PersistentVolumeClaim) {
		// The fake client only accepts a deletion timestamp with a finalizer.
		pvc.Finalizers = []string{"kubernetes.io/pvc-protection"}
		now := metav1.Now()
		pvc.DeletionTimestamp = &now
	}
	r, c, cr, pvc, calls := protectionTestEnv(t, true, deleting,
		errors.New("operation cannot be performed in current state"), interceptor.Funcs{})

	_, retry := r.reconcileVMDeleteProtection(ctx, cr, pvc, "vol-1")

	require.NotNil(t, retry)
	require.Zero(t, retry.RequeueAfter, "a terminating PVC must not keep retrying the clear")
	require.Equal(t, 1, *calls, "the attempt in flight still runs; only the retry is abandoned")
	got := getCondTestInstance(t, c, cr)
	require.Equal(t, api.ReasonPvcTerminating, condOf(got, api.ConditionVMDeleteProtectionConfigured).Reason)
	require.Equal(t, api.ReasonPvcTerminating, condOf(got, api.ConditionReady).Reason,
		"Ready must surface why the registration stopped")
	require.False(t, got.Status.Registered)
	requireAbandonRecorded(t, r, cr)
}

// requireAbandonRecorded checks that an abandoned clear is recorded as terminal,
// so a requeue scheduled before it cannot retry the clear or recreate the PVC.
func requireAbandonRecorded(t *testing.T, r *ReconcileCnsRegisterVolume, cr *api.CnsRegisterVolume) {
	t.Helper()
	got, ok := r.terminalFailures.Load(instanceKey(cr))
	require.True(t, ok, "the abandon must be recorded in terminalFailures")
	require.Equal(t, terminalFailure{uid: cr.UID, generation: cr.Generation}, got)
}

// TestVMDeleteProtectionAbandonedWhenPvcGone covers the 1769149 shape: the clear
// succeeds in CNS but its completion annotation cannot be written because the
// PVC is already gone. The flag is cleared, so there is nothing left to retry.
func TestVMDeleteProtectionAbandonedWhenPvcGone(t *testing.T) {
	ctx := context.Background()
	r, c, cr, pvc, _ := protectionTestEnv(t, false, nil, nil, interceptor.Funcs{})

	_, retry := r.reconcileVMDeleteProtection(ctx, cr, pvc, "vol-1")

	require.NotNil(t, retry)
	require.Zero(t, retry.RequeueAfter, "a deleted PVC must not keep retrying the clear")
	require.Equal(t, api.ReasonPvcTerminating,
		condOf(getCondTestInstance(t, c, cr), api.ConditionVMDeleteProtectionConfigured).Reason)
	requireAbandonRecorded(t, r, cr)
}

// TestVMDeleteProtectionRetriesWhenPvcLookupFails guards the other side: only
// NotFound means the PVC is gone. A transient API failure must not be mistaken
// for a destroyed VM and silently abandon a clear that could still land.
func TestVMDeleteProtectionRetriesWhenPvcLookupFails(t *testing.T) {
	ctx := context.Background()
	blocked := interceptor.Funcs{
		Get: func(ctx context.Context, client ctrlclient.WithWatch, key ctrlclient.ObjectKey,
			obj ctrlclient.Object, opts ...ctrlclient.GetOption) error {
			if _, ok := obj.(*v1.PersistentVolumeClaim); ok {
				return errors.New("api server unavailable")
			}
			return client.Get(ctx, key, obj, opts...)
		},
	}
	r, c, cr, pvc, _ := protectionTestEnv(t, true, nil,
		errors.New("operation cannot be performed in current state"), blocked)

	_, retry := r.reconcileVMDeleteProtection(ctx, cr, pvc, "vol-1")

	require.NotNil(t, retry)
	require.Equal(t, vmDeleteProtectionRetryInterval, retry.RequeueAfter,
		"an API blip must not be read as a terminating PVC")
	require.Equal(t, api.ReasonClearKeepAfterDeleteVmFailed,
		condOf(getCondTestInstance(t, c, cr), api.ConditionVMDeleteProtectionConfigured).Reason)
	_, recorded := r.terminalFailures.Load(instanceKey(cr))
	require.False(t, recorded, "a clear that is still retried must not be recorded as terminal")
}
