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
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	k8sfake "k8s.io/client-go/kubernetes/fake"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	k8stesting "k8s.io/client-go/testing"
	"k8s.io/client-go/tools/record"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	cnsregistervolumev1alpha1 "sigs.k8s.io/vsphere-csi-driver/v3/pkg/apis/cnsoperator/cnsregistervolume/v1alpha1"
	"sigs.k8s.io/vsphere-csi-driver/v3/pkg/common/config"
	"sigs.k8s.io/vsphere-csi-driver/v3/pkg/csi/service/common/commonco"
	"sigs.k8s.io/vsphere-csi-driver/v3/pkg/internalapis/cnsvolumeinfo"
	cnsvolumeinfov1alpha1 "sigs.k8s.io/vsphere-csi-driver/v3/pkg/internalapis/cnsvolumeinfo/v1alpha1"
	cnsoperatortypes "sigs.k8s.io/vsphere-csi-driver/v3/pkg/syncer/cnsoperator/types"
)

const (
	testRestoreVolumeID = "3c6b9a1e-5d2f-4f4e-9b8a-2a71c0e5d9f4"
	testRestoreUID      = types.UID("9b1f0c3d-4e5a-4b6c-8d7e-0f1a2b3c7a2e")
	testRestorePVName   = staticPvNamePrefix + testRestoreVolumeID
)

// fakeRestoreVolumeInfoService records the restore calls of the register controller.
type fakeRestoreVolumeInfoService struct {
	cnsvolumeinfo.VolumeInfoService
	precheckErr     error
	handoverErr     error
	handoverResult  cnsvolumeinfo.RestoreResult
	prechecks       []string
	handovers       []cnsvolumeinfo.RestoredVolumeInfo
	precheckRestore []string
	restoredBy      string
	restoredByErr   error
	restoredByReads int
}

func (f *fakeRestoreVolumeInfoService) GetRestoredBy(_ context.Context, _ string) (string, error) {
	f.restoredByReads++
	return f.restoredBy, f.restoredByErr
}

func (f *fakeRestoreVolumeInfoService) PrecheckRestore(_ context.Context, volumeID, namespace,
	restoredBy string) error {
	f.prechecks = append(f.prechecks, volumeID+"|"+namespace)
	f.precheckRestore = append(f.precheckRestore, restoredBy)
	return f.precheckErr
}

func (f *fakeRestoreVolumeInfoService) CreateOrUpdateRestoredVolumeInfo(_ context.Context,
	info cnsvolumeinfo.RestoredVolumeInfo) (cnsvolumeinfo.RestoreResult, error) {
	f.handovers = append(f.handovers, info)
	return f.handoverResult, f.handoverErr
}

func withSnapServiceLocalProtection(t *testing.T, enabled bool) {
	t.Helper()
	orig := isSnapServiceLocalProtectionEnabled
	isSnapServiceLocalProtectionEnabled = enabled
	t.Cleanup(func() { isSnapServiceLocalProtectionEnabled = orig })
}

func testRestoreInstance(annotation string) *cnsregistervolumev1alpha1.CnsRegisterVolume {
	instance := &cnsregistervolumev1alpha1.CnsRegisterVolume{
		ObjectMeta: metav1.ObjectMeta{Name: "restore", Namespace: "ns1", UID: testRestoreUID},
		Spec: cnsregistervolumev1alpha1.CnsRegisterVolumeSpec{
			PvcName:    "restored-pvc",
			VolumeID:   testRestoreVolumeID,
			AccessMode: corev1.ReadWriteOnce,
		},
	}
	if annotation != "" {
		instance.Annotations = map[string]string{cnsvolumeinfov1alpha1.AnnSnapServiceRestore: annotation}
	}
	return instance
}

func seededResult(status metav1.ConditionStatus, reason string) cnsvolumeinfo.RestoreResult {
	return cnsvolumeinfo.RestoreResult{
		Seeded: status == metav1.ConditionTrue,
		Condition: metav1.Condition{Type: cnsvolumeinfov1alpha1.ConditionSnapshotSizeRestored,
			Status: status, Reason: reason, Message: "details"},
	}
}

func TestIsSnapServiceRestore(t *testing.T) {
	ctx := context.Background()
	for _, tc := range []struct {
		name       string
		annotation string
		capability bool
		noService  bool
		want       bool
	}{
		{name: "annotated, capability enabled", annotation: "true", capability: true, want: true},
		{name: "no annotation", capability: true},
		{name: "annotation not true", annotation: "false", capability: true},
		{name: "annotation with other casing", annotation: "True", capability: true},
		{name: "capability disabled", annotation: "true"},
		{name: "volume info service not running", annotation: "true", capability: true, noService: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			withSnapServiceLocalProtection(t, tc.capability)
			r := &ReconcileCnsRegisterVolume{volumeInfoService: &fakeRestoreVolumeInfoService{}}
			if tc.noService {
				r.volumeInfoService = nil
			}
			assert.Equal(t, tc.want, r.isSnapServiceRestore(ctx, testRestoreInstance(tc.annotation)))
		})
	}
}

func TestSetRestoreAnnotations(t *testing.T) {
	pv := getPersistentVolumeSpec(testRestorePVName, testRestoreVolumeID, resource.MustParse("1Gi"),
		corev1.ReadWriteOnce, "", "sc-gold", nil, "ns1", "restore")
	setRestoreAnnotations(pv, testRestoreInstance("true"))
	assert.Equal(t, "true", pv.Annotations[cnsvolumeinfov1alpha1.AnnSnapServiceRestore])
	assert.Equal(t, string(testRestoreUID), pv.Annotations[cnsvolumeinfov1alpha1.AnnRestoredByCnsRegisterVolume])
	assert.Equal(t, labelCnsRegisterVolumeCreatedByValue, pv.Labels[labelCnsRegisterVolumeCreatedBy],
		"the restore annotations must not replace the registration labels")
}

func TestEnsureRestoreAnnotationsOnPV(t *testing.T) {
	ctx := context.Background()
	t.Run("adds missing annotations", func(t *testing.T) {
		pv := &corev1.PersistentVolume{ObjectMeta: metav1.ObjectMeta{Name: testRestorePVName,
			Annotations: map[string]string{"other": "kept"}}}
		k8sclient := k8sfake.NewClientset(pv)
		patched, err := ensureRestoreAnnotationsOnPV(ctx, k8sclient, pv, testRestoreInstance("true"))
		require.NoError(t, err)
		assert.Equal(t, "true", patched.Annotations[cnsvolumeinfov1alpha1.AnnSnapServiceRestore])
		assert.Equal(t, string(testRestoreUID),
			patched.Annotations[cnsvolumeinfov1alpha1.AnnRestoredByCnsRegisterVolume])
		assert.Equal(t, "kept", patched.Annotations["other"])
	})
	t.Run("adds a missing stamp guarded by the resourceVersion", func(t *testing.T) {
		pv := &corev1.PersistentVolume{ObjectMeta: metav1.ObjectMeta{Name: testRestorePVName,
			ResourceVersion: "7",
			Annotations:     map[string]string{cnsvolumeinfov1alpha1.AnnSnapServiceRestore: "true"}}}
		k8sclient := k8sfake.NewClientset(pv)
		patched, err := ensureRestoreAnnotationsOnPV(ctx, k8sclient, pv, testRestoreInstance("true"))
		require.NoError(t, err)
		assert.Equal(t, string(testRestoreUID),
			patched.Annotations[cnsvolumeinfov1alpha1.AnnRestoredByCnsRegisterVolume])
		var patches []string
		for _, action := range k8sclient.Actions() {
			if patch, ok := action.(k8stesting.PatchAction); ok {
				patches = append(patches, string(patch.GetPatch()))
			}
		}
		require.Len(t, patches, 1)
		assert.Contains(t, patches[0], `"resourceVersion":"7"`)
		assert.NotContains(t, patches[0], cnsvolumeinfov1alpha1.AnnSnapServiceRestore,
			"only the missing annotation is patched")
	})
	t.Run("never replaces the stamp of another CnsRegisterVolume", func(t *testing.T) {
		pv := &corev1.PersistentVolume{ObjectMeta: metav1.ObjectMeta{Name: testRestorePVName,
			Annotations: map[string]string{cnsvolumeinfov1alpha1.AnnRestoredByCnsRegisterVolume: "other-uid"}}}
		k8sclient := k8sfake.NewClientset(pv)
		_, err := ensureRestoreAnnotationsOnPV(ctx, k8sclient, pv, testRestoreInstance("true"))
		require.ErrorContains(t, err, "was restored by CnsRegisterVolume other-uid")
		for _, action := range k8sclient.Actions() {
			assert.NotEqual(t, "patch", action.GetVerb())
		}
	})
	t.Run("leaves an annotated PV alone", func(t *testing.T) {
		pv := &corev1.PersistentVolume{ObjectMeta: metav1.ObjectMeta{Name: testRestorePVName}}
		setRestoreAnnotations(pv, testRestoreInstance("true"))
		k8sclient := k8sfake.NewClientset(pv)
		_, err := ensureRestoreAnnotationsOnPV(ctx, k8sclient, pv, testRestoreInstance("true"))
		require.NoError(t, err)
		for _, action := range k8sclient.Actions() {
			assert.NotEqual(t, "patch", action.GetVerb())
		}
	})
}

func TestHandoverRestoredVolume(t *testing.T) {
	ctx := context.Background()
	now := metav1.Now()
	livePV := &corev1.PersistentVolume{ObjectMeta: metav1.ObjectMeta{Name: testRestorePVName}}
	deletingPV := &corev1.PersistentVolume{ObjectMeta: metav1.ObjectMeta{Name: testRestorePVName,
		DeletionTimestamp: &now, Finalizers: []string{"kubernetes.io/pv-protection"}}}
	for _, tc := range []struct {
		name          string
		pv            *corev1.PersistentVolume
		result        cnsvolumeinfo.RestoreResult
		handoverErr   error
		wantErr       string
		wantHandovers int
		wantWarning   bool
	}{
		{name: "seeded", pv: livePV, wantHandovers: 1,
			result: seededResult(metav1.ConditionTrue, cnsvolumeinfov1alpha1.ReasonSeededFromDeletedVolumeInfo)},
		{name: "no matching DVI emits a warning", pv: livePV, wantHandovers: 1, wantWarning: true,
			result: seededResult(metav1.ConditionFalse, cnsvolumeinfov1alpha1.ReasonDeletedVolumeInfoNotFound)},
		{name: "handover error is returned", pv: livePV, wantHandovers: 1,
			handoverErr: errors.New("CRD not served"), wantErr: "CRD not served"},
		{name: "PV missing stops", wantErr: "is gone"},
		{name: "PV being deleted stops", pv: deletingPV, wantErr: "is being deleted"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			k8sclient := k8sfake.NewClientset()
			if tc.pv != nil {
				k8sclient = k8sfake.NewClientset(tc.pv)
			}
			service := &fakeRestoreVolumeInfoService{handoverResult: tc.result, handoverErr: tc.handoverErr}
			recorder := record.NewFakeRecorder(10)
			r := &ReconcileCnsRegisterVolume{k8sclient: k8sclient, volumeInfoService: service, recorder: recorder}
			info := cnsvolumeinfo.RestoredVolumeInfo{VolumeID: testRestoreVolumeID, Namespace: "ns1",
				RestoredBy: string(testRestoreUID)}
			err := r.handoverRestoredVolume(ctx, testRestoreInstance("true"), testRestorePVName, info)
			if tc.wantErr != "" {
				require.ErrorContains(t, err, tc.wantErr)
			} else {
				require.NoError(t, err)
			}
			require.Len(t, service.handovers, tc.wantHandovers)
			if tc.wantHandovers > 0 {
				assert.Equal(t, info, service.handovers[0])
			}
			if tc.wantWarning {
				require.Len(t, recorder.Events, 1)
				event := <-recorder.Events
				assert.True(t, strings.HasPrefix(event, corev1.EventTypeWarning+" SnapshotSizeNotRestored"), event)
			} else {
				assert.Empty(t, recorder.Events)
			}
		})
	}
}

// newRestoreReconcileHarness returns a reconciler that has no vCenter configured, so a Reconcile that
// gets past the precheck stops at the vCenter connection.
func newRestoreReconcileHarness(t *testing.T, instance *cnsregistervolumev1alpha1.CnsRegisterVolume,
	service *fakeRestoreVolumeInfoService, pvs ...runtime.Object) *ReconcileCnsRegisterVolume {
	t.Helper()
	backOffDuration = make(map[types.NamespacedName]time.Duration)
	scheme := runtime.NewScheme()
	require.NoError(t, clientgoscheme.AddToScheme(scheme))
	scheme.AddKnownTypes(schema.GroupVersion{Group: "cnsoperator.vmware.com", Version: "v1alpha1"},
		&cnsregistervolumev1alpha1.CnsRegisterVolume{}, &cnsregistervolumev1alpha1.CnsRegisterVolumeList{})
	fakeClient := fake.NewClientBuilder().WithScheme(scheme).WithObjects(instance).
		WithStatusSubresource(&cnsregistervolumev1alpha1.CnsRegisterVolume{}).Build()
	origCO := commonco.ContainerOrchestratorUtility
	commonco.ContainerOrchestratorUtility = &mockCOCommon{}
	t.Cleanup(func() { commonco.ContainerOrchestratorUtility = origCO })
	return &ReconcileCnsRegisterVolume{
		client:            fakeClient,
		scheme:            scheme,
		k8sclient:         k8sfake.NewClientset(pvs...),
		recorder:          record.NewFakeRecorder(100),
		volumeInfoService: service,
		configInfo:        config.NewConfigurationInfo(&config.Config{}),
	}
}

func reconcileAndGetStatus(t *testing.T, r *ReconcileCnsRegisterVolume,
	instance *cnsregistervolumev1alpha1.CnsRegisterVolume) (reconcile.Result,
	*cnsregistervolumev1alpha1.CnsRegisterVolume) {
	t.Helper()
	key := types.NamespacedName{Name: instance.Name, Namespace: instance.Namespace}
	result, err := r.Reconcile(context.Background(), reconcile.Request{NamespacedName: key})
	require.NoError(t, err)
	got := &cnsregistervolumev1alpha1.CnsRegisterVolume{}
	require.NoError(t, r.client.Get(context.Background(), key, got))
	return result, got
}

func TestReconcileRestorePrecheckFailureStopsBeforeCNS(t *testing.T) {
	withSnapServiceLocalProtection(t, true)
	instance := testRestoreInstance("true")
	msg := "restore prerequisite not met: CnsDeletedVolumeInfo vmware-system-csi/" + testRestoreVolumeID +
		" not found"
	service := &fakeRestoreVolumeInfoService{precheckErr: errors.New(msg)}
	r := newRestoreReconcileHarness(t, instance, service)

	result, got := reconcileAndGetStatus(t, r, instance)
	assert.Equal(t, reconcile.Result{RequeueAfter: time.Second}, result, "a failed precheck must retry")
	assert.Equal(t, msg, got.Status.Error)
	assert.False(t, got.Status.Registered)
	assert.Equal(t, []string{testRestoreVolumeID + "|ns1"}, service.prechecks)
	assert.Equal(t, []string{string(testRestoreUID)}, service.precheckRestore)
	assert.Empty(t, service.handovers)
	assert.NotContains(t, got.Status.Error, "Unable to connect to VC",
		"the precheck must run before vCenter is contacted")
}

func TestReconcileRestorePrecheckPassContinuesRegistration(t *testing.T) {
	withSnapServiceLocalProtection(t, true)
	instance := testRestoreInstance("true")
	service := &fakeRestoreVolumeInfoService{}
	r := newRestoreReconcileHarness(t, instance, service)

	_, got := reconcileAndGetStatus(t, r, instance)
	assert.Len(t, service.prechecks, 1)
	assert.Equal(t, "Unable to connect to VC for volume registration", got.Status.Error,
		"a passed precheck must continue to the CNS registration")
}

func TestReconcileWithoutRestoreSkipsPrecheck(t *testing.T) {
	for _, tc := range []struct {
		name       string
		annotation string
		capability bool
	}{
		{name: "no annotation", capability: true},
		{name: "annotation not true", annotation: "yes", capability: true},
		{name: "capability disabled", annotation: "true"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			withSnapServiceLocalProtection(t, tc.capability)
			instance := testRestoreInstance(tc.annotation)
			service := &fakeRestoreVolumeInfoService{precheckErr: errors.New("must not be called")}
			r := newRestoreReconcileHarness(t, instance, service)

			_, got := reconcileAndGetStatus(t, r, instance)
			assert.Empty(t, service.prechecks, "an ordinary registration must not read a DVI")
			assert.Equal(t, "Unable to connect to VC for volume registration", got.Status.Error)
		})
	}
}

func TestRestoreLock(t *testing.T) {
	const otherUID = types.UID("other-uid")
	t.Cleanup(func() {
		unlockRestore(testRestoreVolumeID, testRestoreUID)
		unlockRestore(testRestoreVolumeID, otherUID)
	})
	locked, _ := tryLockRestore(testRestoreVolumeID, testRestoreUID)
	require.True(t, locked)
	locked, holder := tryLockRestore(testRestoreVolumeID, otherUID)
	assert.False(t, locked, "another CnsRegisterVolume must not get the lock")
	assert.Equal(t, testRestoreUID, holder)
	locked, _ = tryLockRestore("another-volume", otherUID)
	assert.True(t, locked, "the lock is per volume")
	unlockRestore("another-volume", otherUID)

	unlockRestore(testRestoreVolumeID, otherUID)
	locked, _ = tryLockRestore(testRestoreVolumeID, otherUID)
	assert.False(t, locked, "only the holder releases the lock")

	unlockRestore(testRestoreVolumeID, testRestoreUID)
	locked, _ = tryLockRestore(testRestoreVolumeID, otherUID)
	assert.True(t, locked)
}

func assertRestoreUnlocked(t *testing.T) {
	t.Helper()
	restoreLocksMutex.Lock()
	defer restoreLocksMutex.Unlock()
	assert.NotContains(t, restoreLocks, testRestoreVolumeID, "the restore lock must be released")
}

func testStampedPV(stamp string, deleting bool) *corev1.PersistentVolume {
	pv := &corev1.PersistentVolume{ObjectMeta: metav1.ObjectMeta{Name: testRestorePVName,
		Annotations: map[string]string{
			cnsvolumeinfov1alpha1.AnnSnapServiceRestore:          "true",
			cnsvolumeinfov1alpha1.AnnRestoredByCnsRegisterVolume: stamp,
		}}}
	if deleting {
		now := metav1.Now()
		pv.DeletionTimestamp = &now
		pv.Finalizers = []string{"kubernetes.io/pv-protection"}
	}
	return pv
}

func TestReconcileRestoreOfOtherOwnerIsNotProcessed(t *testing.T) {
	for _, tc := range []struct {
		name    string
		pv      *corev1.PersistentVolume
		cviOwns bool
	}{
		{name: "PV stamped by another CnsRegisterVolume", pv: testStampedPV("other-uid", false)},
		{name: "CnsVolumeInfo stamped by another CnsRegisterVolume", cviOwns: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			withSnapServiceLocalProtection(t, true)
			instance := testRestoreInstance("true")
			service := &fakeRestoreVolumeInfoService{}
			if tc.cviOwns {
				service.restoredBy = "other-uid"
			}
			var pvs []runtime.Object
			if tc.pv != nil {
				pvs = append(pvs, tc.pv)
			}
			r := newRestoreReconcileHarness(t, instance, service, pvs...)

			result, got := reconcileAndGetStatus(t, r, instance)
			assert.Equal(t, reconcile.Result{}, result, "a CnsRegisterVolume that is not processed is not retried")
			assert.Equal(t, "volume "+testRestoreVolumeID+" was restored by CnsRegisterVolume other-uid; this "+
				"CnsRegisterVolume is not processed", got.Status.Error)
			assert.Equal(t, "other-uid", got.Annotations[cnsvolumeinfov1alpha1.AnnRestoreNotProcessed])
			assert.Empty(t, got.Finalizers, "the decision is made before the finalizer is added")
			assert.Empty(t, service.prechecks)
			assertRestoreUnlocked(t)

			// The marker keeps the decision without reading the stamps again.
			reads := service.restoredByReads
			result, got = reconcileAndGetStatus(t, r, instance)
			assert.Equal(t, reconcile.Result{}, result)
			assert.Equal(t, reads, service.restoredByReads)
			assert.Empty(t, service.prechecks)
			assert.Empty(t, got.Finalizers)
		})
	}
}

func TestReconcileRestoreProceedsWhenNotOwnedByOther(t *testing.T) {
	for _, tc := range []struct {
		name         string
		pv           *corev1.PersistentVolume
		restoredBy   string
		wantPrecheck bool
	}{
		{name: "no stamps", wantPrecheck: true},
		{name: "PV stamped by this CnsRegisterVolume skips the precheck",
			pv: testStampedPV(string(testRestoreUID), false)},
		{name: "CnsVolumeInfo stamped by this CnsRegisterVolume skips the precheck",
			restoredBy: string(testRestoreUID)},
		{name: "PV of another CnsRegisterVolume being deleted", pv: testStampedPV("other-uid", true),
			wantPrecheck: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			withSnapServiceLocalProtection(t, true)
			instance := testRestoreInstance("true")
			// A precheck would fail, as it does once DPO or a purge removed the CnsDeletedVolumeInfo.
			service := &fakeRestoreVolumeInfoService{restoredBy: tc.restoredBy,
				precheckErr: errors.New("restore prerequisite not met")}
			var pvs []runtime.Object
			if tc.pv != nil {
				pvs = append(pvs, tc.pv)
			}
			r := newRestoreReconcileHarness(t, instance, service, pvs...)

			_, got := reconcileAndGetStatus(t, r, instance)
			if tc.wantPrecheck {
				assert.Len(t, service.prechecks, 1, "the restore must run the precheck")
				assert.Equal(t, "restore prerequisite not met", got.Status.Error)
			} else {
				assert.Empty(t, service.prechecks, "a restore that already stamped the volume skips the precheck")
				assert.Equal(t, "Unable to connect to VC for volume registration", got.Status.Error,
					"the restore must continue to the CNS registration")
			}
			assert.Empty(t, got.Annotations[cnsvolumeinfov1alpha1.AnnRestoreNotProcessed])
			assertRestoreUnlocked(t)
		})
	}
}

func TestReconcileRestoreWaitsForTheLockHolder(t *testing.T) {
	withSnapServiceLocalProtection(t, true)
	locked, _ := tryLockRestore(testRestoreVolumeID, "holder-uid")
	require.True(t, locked)
	t.Cleanup(func() { unlockRestore(testRestoreVolumeID, "holder-uid") })
	instance := testRestoreInstance("true")
	service := &fakeRestoreVolumeInfoService{}
	r := newRestoreReconcileHarness(t, instance, service)

	result, got := reconcileAndGetStatus(t, r, instance)
	assert.Equal(t, reconcile.Result{RequeueAfter: time.Second}, result, "a blocked restore retries")
	assert.Empty(t, got.Status.Error, "a blocked restore is not an error")
	assert.Zero(t, service.restoredByReads)
	assert.Empty(t, service.prechecks)
	assert.Empty(t, got.Finalizers)
}

func TestReconcileRestoreOwnerReadErrorRetries(t *testing.T) {
	withSnapServiceLocalProtection(t, true)
	instance := testRestoreInstance("true")
	service := &fakeRestoreVolumeInfoService{restoredByErr: errors.New("failed to read CnsVolumeInfo")}
	r := newRestoreReconcileHarness(t, instance, service)

	result, got := reconcileAndGetStatus(t, r, instance)
	assert.Equal(t, reconcile.Result{RequeueAfter: time.Second}, result)
	assert.Equal(t, "failed to read CnsVolumeInfo", got.Status.Error)
	assert.Empty(t, got.Annotations[cnsvolumeinfov1alpha1.AnnRestoreNotProcessed])
	assert.Empty(t, service.prechecks)
	assertRestoreUnlocked(t)
}

func TestReconcileDeleteLeavesPVOfAnotherRestore(t *testing.T) {
	withSnapServiceLocalProtection(t, true)
	instance := testRestoreInstance("true")
	now := metav1.Now()
	instance.DeletionTimestamp = &now
	instance.Finalizers = []string{cnsoperatortypes.CNSRegisterVolumeFinalizer}
	pv := testStampedPV("other-uid", false)
	r := newRestoreReconcileHarness(t, instance, &fakeRestoreVolumeInfoService{}, pv)

	key := types.NamespacedName{Name: instance.Name, Namespace: instance.Namespace}
	result, err := r.Reconcile(context.Background(), reconcile.Request{NamespacedName: key})
	require.NoError(t, err)
	assert.Equal(t, reconcile.Result{}, result)
	err = r.client.Get(context.Background(), key, &cnsregistervolumev1alpha1.CnsRegisterVolume{})
	assert.True(t, apierrors.IsNotFound(err), "the finalizer must be removed: %v", err)
	_, err = r.k8sclient.CoreV1().PersistentVolumes().Get(context.Background(), testRestorePVName,
		metav1.GetOptions{})
	assert.NoError(t, err, "the PV of the other restore must not be deleted")
}
