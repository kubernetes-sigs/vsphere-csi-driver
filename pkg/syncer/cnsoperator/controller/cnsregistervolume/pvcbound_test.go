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
	"strings"
	"testing"

	"github.com/agiledragon/gomonkey/v2"
	"github.com/stretchr/testify/require"
	vmoperator "github.com/vmware-tanzu/vm-operator/api/v1alpha5"
	v1 "k8s.io/api/core/v1"
	storagev1 "k8s.io/api/storage/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	k8sfake "k8s.io/client-go/kubernetes/fake"
	ctrlclient "sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
	api "sigs.k8s.io/vsphere-csi-driver/v3/pkg/apis/cnsoperator/cnsregistervolume/v1alpha1"
	cnsvsphere "sigs.k8s.io/vsphere-csi-driver/v3/pkg/common/cns-lib/vsphere"
	"sigs.k8s.io/vsphere-csi-driver/v3/pkg/csi/service/common/commonco"
	"sigs.k8s.io/vsphere-csi-driver/v3/pkg/syncer"
)

// bindFixture drives a VM-import registration through the real Reconcile up to
// the bind, with a VSLM clear that fails until the test allows it. It is the
// setup of TestReconcileBindsBeforeVMDeleteProtection, shared by the tests below.
type bindFixture struct {
	r         *ReconcileCnsRegisterVolume
	req       reconcile.Request
	pvcName   string
	ns        string
	calls     *int
	clearFail *bool
	// blockBound makes status patches that would record PvcBound=True fail.
	blockBound *bool
}

func newBindFixture(t *testing.T) *bindFixture {
	t.Helper()
	ctx := context.Background()
	patches := gomonkey.NewPatches()
	t.Cleanup(patches.Reset)
	oldCO, oldFSS, oldStretch, oldShared := commonco.ContainerOrchestratorUtility,
		fssEnabledOverride, syncer.IsPodVMOnStretchSupervisorFSSEnabled, isSharedDiskEnabled
	t.Cleanup(func() {
		commonco.ContainerOrchestratorUtility, fssEnabledOverride = oldCO, oldFSS
		syncer.IsPodVMOnStretchSupervisorFSSEnabled, isSharedDiskEnabled = oldStretch, oldShared
	})
	r, _, _ := newHostLocalReconcileFixture(patches, "none")
	fssEnabledOverride = func(string) bool { return false }
	syncer.IsPodVMOnStretchSupervisorFSSEnabled, isSharedDiskEnabled = false, false
	require.NoError(t, vmoperator.AddToScheme(r.scheme))

	blockBound := false
	r.client = interceptor.NewClient(r.client.(ctrlclient.WithWatch), interceptor.Funcs{
		SubResourcePatch: func(ctx context.Context, c ctrlclient.Client, subResourceName string,
			obj ctrlclient.Object, patch ctrlclient.Patch, opts ...ctrlclient.SubResourcePatchOption) error {
			if blockBound {
				data, err := patch.Data(obj)
				require.NoError(t, err)
				if strings.Contains(string(data), `"type":"PvcBound","status":"True"`) {
					return errors.New("injected status patch failure")
				}
			}
			return c.Status().Patch(ctx, obj, patch, opts...)
		},
	})

	removable := false
	pvc := vmImportPVC("test-pvc", "test-ns", "test-vm")
	scName := "test-sc"
	pvc.Spec.StorageClassName = &scName
	pvc.Spec.Resources.Requests = v1.ResourceList{v1.ResourceStorage: resource.MustParse("1Gi")}
	// The finalizer lets a test give this copy a deletion timestamp.
	pvc.Finalizers = []string{"kubernetes.io/pvc-protection"}
	require.NoError(t, r.client.Create(ctx, pvc))
	require.NoError(t, r.client.Create(ctx, vmWithVolume("test-vm", "test-ns", pvc.Name, &removable)))
	sc := &storagev1.StorageClass{ObjectMeta: metav1.ObjectMeta{Name: scName},
		Parameters: map[string]string{"storagePolicyID": "dummy-storage-policy-id"}}
	quota := &v1.ResourceQuota{ObjectMeta: metav1.ObjectMeta{Name: "quota", Namespace: pvc.Namespace},
		Spec: v1.ResourceQuotaSpec{Hard: v1.ResourceList{
			v1.ResourceName(scName + scResourceNameSuffix): resource.MustParse("10Gi"),
		}}}
	r.k8sclient = k8sfake.NewClientset(sc, quota, pvc.DeepCopy())
	patches.ApplyFunc(isDatastoreAccessibleToCluster,
		func(context.Context, *cnsvsphere.VirtualCenter, string, string) bool {
			return true
		})
	calls := 0
	clearFail := true
	r.volumeManager.(*mockVolumeManager).unprotectVolumeFromVMDeletionFunc = func(context.Context, string) error {
		calls++
		if clearFail {
			return errors.New("transient VSLM state")
		}
		return nil
	}
	r.volumeManager = &protectionTestVolumeManager{r.volumeManager.(*mockVolumeManager)}
	t.Cleanup(wireAsyncCnsTaskSuccess(r))
	return &bindFixture{
		r:          r,
		req:        reconcile.Request{NamespacedName: types.NamespacedName{Name: "test-volume", Namespace: pvc.Namespace}},
		pvcName:    pvc.Name,
		ns:         pvc.Namespace,
		calls:      &calls,
		clearFail:  &clearFail,
		blockBound: &blockBound,
	}
}

// createPVAndPVC runs the pipeline until it waits for the bind.
func (f *bindFixture) createPVAndPVC(t *testing.T) {
	t.Helper()
	result, err := reconcileUntilSettled(context.Background(), f.r, f.req)
	require.NoError(t, err)
	require.Equal(t, pvcBindPollInterval, result.RequeueAfter)
	require.Zero(t, *f.calls)
}

// setPVCPhase binds or unbinds the PVC the way the PV controller would.
func (f *bindFixture) setPVCPhase(t *testing.T, phase v1.PersistentVolumeClaimPhase) {
	t.Helper()
	ctx := context.Background()
	pvc, err := f.r.k8sclient.CoreV1().PersistentVolumeClaims(f.ns).Get(ctx, f.pvcName, metav1.GetOptions{})
	require.NoError(t, err)
	pvc.Spec.VolumeName = staticPvNamePrefix + "dummy-volume-id"
	pvc.Status.Phase = phase
	_, err = f.r.k8sclient.CoreV1().PersistentVolumeClaims(f.ns).Update(ctx, pvc, metav1.UpdateOptions{})
	require.NoError(t, err)
	_, err = f.r.k8sclient.CoreV1().PersistentVolumeClaims(f.ns).UpdateStatus(ctx, pvc, metav1.UpdateOptions{})
	require.NoError(t, err)
}

// bindWithFailingClear binds the PVC and runs one pass, which records
// PvcBound=True and then fails the clear: the state the VM-delete race starts from.
func (f *bindFixture) bindWithFailingClear(t *testing.T) {
	t.Helper()
	f.createPVAndPVC(t)
	f.setPVCPhase(t, v1.ClaimBound)
	result, err := reconcileUntilSettled(context.Background(), f.r, f.req)
	require.NoError(t, err)
	require.Equal(t, vmDeleteProtectionRetryInterval, result.RequeueAfter)
	require.Positive(t, *f.calls)
	require.Equal(t, metav1.ConditionTrue, condOf(f.cr(t), api.ConditionPvcBound).Status)
}

func (f *bindFixture) cr(t *testing.T) *api.CnsRegisterVolume {
	t.Helper()
	cr := &api.CnsRegisterVolume{}
	require.NoError(t, f.r.client.Get(context.Background(), f.req.NamespacedName, cr))
	return cr
}

func (f *bindFixture) pvcExists(t *testing.T) bool {
	t.Helper()
	_, err := f.r.k8sclient.CoreV1().PersistentVolumeClaims(f.ns).Get(context.Background(), f.pvcName,
		metav1.GetOptions{})
	if apierrors.IsNotFound(err) {
		return false
	}
	require.NoError(t, err)
	return true
}

func (f *bindFixture) deletePVC(t *testing.T) {
	t.Helper()
	require.NoError(t, f.r.k8sclient.CoreV1().PersistentVolumeClaims(f.ns).Delete(context.Background(),
		f.pvcName, metav1.DeleteOptions{}))
}

func (f *bindFixture) requireTerminalRecorded(t *testing.T) {
	t.Helper()
	cr := f.cr(t)
	got, ok := f.r.terminalFailures.Load(f.req.NamespacedName)
	require.True(t, ok, "the outcome must be recorded so earlier requeues are absorbed")
	require.Equal(t, terminalFailure{uid: cr.UID, generation: cr.Generation}, got)
}

// TestPvcBoundPatchFailureBlocksPostBindWork covers the first half of the review
// finding: if PvcBound=True cannot be persisted, no post-bind step may run,
// because a VM delete during that step would find PvcBound unset and unregister
// a disk the workload owns.
func TestPvcBoundPatchFailureBlocksPostBindWork(t *testing.T) {
	ctx := context.Background()
	f := newBindFixture(t)
	f.createPVAndPVC(t)
	f.setPVCPhase(t, v1.ClaimBound)
	*f.blockBound = true

	result, err := f.r.Reconcile(ctx, f.req)

	require.NoError(t, err)
	require.Positive(t, result.RequeueAfter, "a failed checkpoint must be retried")
	require.NotEqual(t, vmDeleteProtectionRetryInterval, result.RequeueAfter,
		"the requeue must come from the checkpoint, not from a clear attempt")
	require.Zero(t, *f.calls, "the clear must not run before PvcBound=True is persisted")
	cr := f.cr(t)
	require.False(t, cr.Status.Registered)
	require.NotEqual(t, metav1.ConditionTrue, condOf(cr, api.ConditionPvcBound).Status)

	*f.blockBound = false
	*f.clearFail = false
	result, err = reconcileUntilSettled(ctx, f.r, f.req)

	require.NoError(t, err)
	require.Zero(t, result.RequeueAfter)
	cr = f.cr(t)
	require.Equal(t, metav1.ConditionTrue, condOf(cr, api.ConditionPvcBound).Status)
	require.Positive(t, *f.calls)
	require.True(t, cr.Status.Registered)
}

// TestPvcBoundNotDowngradedWhenPvcUnbound covers the second half: a later pass
// that sees the PVC unbound must not move a recorded PvcBound=True back.
func TestPvcBoundNotDowngradedWhenPvcUnbound(t *testing.T) {
	f := newBindFixture(t)
	f.bindWithFailingClear(t)
	f.setPVCPhase(t, v1.ClaimPending)

	result, err := f.r.Reconcile(context.Background(), f.req)

	require.NoError(t, err)
	require.Equal(t, pvcBindPollInterval, result.RequeueAfter)
	cond := condOf(f.cr(t), api.ConditionPvcBound)
	require.Equal(t, metav1.ConditionTrue, cond.Status)
	require.Equal(t, api.ReasonBound, cond.Reason)
}

// TestBoundPvcGoneIsNotRecreated covers the path that bypassed the delete guard:
// once the bound PVC is deleted with its VM, a reconcile must not create a new,
// unbound PVC, which would leak and would move PvcBound back to False. A second
// reconcile, as a requeue scheduled earlier would cause, must do nothing.
func TestBoundPvcGoneIsNotRecreated(t *testing.T) {
	ctx := context.Background()
	f := newBindFixture(t)
	f.bindWithFailingClear(t)
	callsBefore := *f.calls
	f.deletePVC(t)

	result, err := f.r.Reconcile(ctx, f.req)

	require.NoError(t, err)
	require.Zero(t, result.RequeueAfter)
	require.False(t, f.pvcExists(t), "a deleted bound PVC must not be recreated")
	cr := f.cr(t)
	require.Equal(t, metav1.ConditionTrue, condOf(cr, api.ConditionPvcBound).Status)
	require.Equal(t, api.ReasonPvcTerminating, condOf(cr, api.ConditionReady).Reason)
	require.False(t, cr.Status.Registered)
	require.Equal(t, callsBefore, *f.calls, "the clear must not be retried")
	f.requireTerminalRecorded(t)

	result, err = f.r.Reconcile(ctx, f.req)

	require.NoError(t, err)
	require.Zero(t, result.RequeueAfter)
	require.False(t, f.pvcExists(t))
	require.Equal(t, callsBefore, *f.calls)
}

// TestBoundPvcGoneAfterRestart checks the same outcome without the in-memory
// terminal record, as after a syncer restart: the persisted PvcBound=True alone
// must stop the PVC from being recreated.
func TestBoundPvcGoneAfterRestart(t *testing.T) {
	f := newBindFixture(t)
	f.bindWithFailingClear(t)
	f.deletePVC(t)
	_, err := f.r.Reconcile(context.Background(), f.req)
	require.NoError(t, err)
	f.r.terminalFailures.Delete(f.req.NamespacedName)

	result, err := f.r.Reconcile(context.Background(), f.req)

	require.NoError(t, err)
	require.Zero(t, result.RequeueAfter)
	require.False(t, f.pvcExists(t), "a restarted syncer must not recreate a deleted bound PVC")
	f.requireTerminalRecorded(t)
}

// TestPvcTerminatingAbandonAbsorbsScheduledRequeue covers the second review
// finding: abandoning the clear returns without a requeue, but a requeue
// scheduled earlier is still delivered. It must not retry the clear while the
// CR has not yet been marked for deletion.
func TestPvcTerminatingAbandonAbsorbsScheduledRequeue(t *testing.T) {
	ctx := context.Background()
	f := newBindFixture(t)
	f.bindWithFailingClear(t)
	// The VM is being destroyed: its PVC carries a deletion timestamp as seen by
	// the clear's own check, while the pipeline's copy still reads as bound.
	pvc := &v1.PersistentVolumeClaim{}
	require.NoError(t, f.r.client.Get(ctx, types.NamespacedName{Name: f.pvcName, Namespace: f.ns}, pvc))
	require.NoError(t, f.r.client.Delete(ctx, pvc))

	result, err := f.r.Reconcile(ctx, f.req)

	require.NoError(t, err)
	require.Zero(t, result.RequeueAfter)
	require.Equal(t, api.ReasonPvcTerminating,
		condOf(f.cr(t), api.ConditionVMDeleteProtectionConfigured).Reason)
	f.requireTerminalRecorded(t)
	callsAfterAbandon := *f.calls

	result, err = f.r.Reconcile(ctx, f.req)

	require.NoError(t, err)
	require.Zero(t, result.RequeueAfter)
	require.Equal(t, callsAfterAbandon, *f.calls, "a requeue scheduled before the abandon must not retry the clear")
	require.True(t, f.pvcExists(t))
}
