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

package cbtconfig

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	cnstypes "github.com/vmware/govmomi/cns/types"
	v1 "k8s.io/api/core/v1"
	storagev1 "k8s.io/api/storage/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	clientset "k8s.io/client-go/kubernetes"
	fakekube "k8s.io/client-go/kubernetes/fake"
	corelisters "k8s.io/client-go/listers/core/v1"
	storagelistersv1 "k8s.io/client-go/listers/storage/v1"
	"k8s.io/client-go/tools/cache"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	cbtconfigv1alpha1 "sigs.k8s.io/vsphere-csi-driver/v3/pkg/apis/cbtconfig/v1alpha1"
	cnsvolume "sigs.k8s.io/vsphere-csi-driver/v3/pkg/common/cns-lib/volume"
	"sigs.k8s.io/vsphere-csi-driver/v3/pkg/csi/service/common"
	"sigs.k8s.io/vsphere-csi-driver/v3/pkg/csi/service/logger"
	"sigs.k8s.io/vsphere-csi-driver/v3/pkg/syncer"
)

// fakeVolumeManager records SetVolumeControlFlags / ClearVolumeControlFlags calls and returns
// configurable errors. It embeds volume.MockManager to satisfy the rest of the volume.Manager
// interface (other methods will panic if accidentally invoked, which is desirable - any test
// that calls them is exercising untested behavior).
type fakeVolumeManager struct {
	cnsvolume.MockManager

	mu          sync.Mutex
	setCalls    []string
	clearCalls  []string
	setErr      error
	clearErr    error
	queryResult *cnstypes.CnsQueryResult
	queryErr    error
	// queryFn, if set, takes precedence over queryResult/queryErr.
	queryFn func(filter cnstypes.CnsQueryFilter) (*cnstypes.CnsQueryResult, error)
	// syncCalls records each volume ID phase 4's SyncVolume() invocation targeted.
	syncCalls []string
	syncErr   error
}

func (f *fakeVolumeManager) SetVolumeControlFlags(_ context.Context, volumeID string,
	_ []string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.setCalls = append(f.setCalls, volumeID)
	return f.setErr
}

func (f *fakeVolumeManager) ClearVolumeControlFlags(_ context.Context, volumeID string,
	_ []string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.clearCalls = append(f.clearCalls, volumeID)
	return f.clearErr
}

func (f *fakeVolumeManager) QueryAllVolume(_ context.Context, filter cnstypes.CnsQueryFilter,
	_ cnstypes.CnsQuerySelection) (*cnstypes.CnsQueryResult, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.queryFn != nil {
		return f.queryFn(filter)
	}
	return f.queryResult, f.queryErr
}

func (f *fakeVolumeManager) SyncVolume(_ context.Context,
	specs []cnstypes.CnsSyncVolumeSpec) (string, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	for _, s := range specs {
		f.syncCalls = append(f.syncCalls, s.VolumeId.Id)
	}
	return "", f.syncErr
}

func (f *fakeVolumeManager) setCallsCopy() []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	out := make([]string, len(f.setCalls))
	copy(out, f.setCalls)
	return out
}

func (f *fakeVolumeManager) clearCallsCopy() []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	out := make([]string, len(f.clearCalls))
	copy(out, f.clearCalls)
	return out
}

var _ cnsvolume.Manager = (*fakeVolumeManager)(nil)

func newTestScheme(t *testing.T) *runtime.Scheme {
	t.Helper()
	scheme := runtime.NewScheme()
	require.NoError(t, v1.AddToScheme(scheme))
	require.NoError(t, storagev1.AddToScheme(scheme))
	require.NoError(t, cbtconfigv1alpha1.AddToScheme(scheme))
	return scheme
}

func newTestClientBuilder(t *testing.T) *fake.ClientBuilder {
	t.Helper()
	return fake.NewClientBuilder().WithScheme(newTestScheme(t))
}

func newCBTBlockPV(name, volumeHandle string) *v1.PersistentVolume {
	return &v1.PersistentVolume{
		ObjectMeta: metav1.ObjectMeta{Name: name},
		Spec: v1.PersistentVolumeSpec{
			PersistentVolumeSource: v1.PersistentVolumeSource{
				CSI: &v1.CSIPersistentVolumeSource{
					Driver:       common.VSphereCSIDriverName,
					VolumeHandle: volumeHandle,
					VolumeAttributes: map[string]string{
						common.AttributeDiskType: common.DiskTypeBlockVolume,
					},
				},
			},
		},
	}
}

func newPVC(namespace, name, pvName string, labels map[string]string,
	annotations map[string]string) *v1.PersistentVolumeClaim {
	return &v1.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{
			Name:        name,
			Namespace:   namespace,
			Labels:      labels,
			Annotations: annotations,
		},
		Spec:   v1.PersistentVolumeClaimSpec{VolumeName: pvName},
		Status: v1.PersistentVolumeClaimStatus{Phase: v1.ClaimBound},
	}
}

func newPVCWithPhase(namespace, name, pvName string,
	phase v1.PersistentVolumeClaimPhase) *v1.PersistentVolumeClaim {
	pvc := newPVC(namespace, name, pvName, nil, nil)
	pvc.Status.Phase = phase
	return pvc
}

func newVolumeAttachmentForPV(name, pvName string) *storagev1.VolumeAttachment {
	pvNameRef := pvName
	return &storagev1.VolumeAttachment{
		ObjectMeta: metav1.ObjectMeta{Name: name},
		Spec: storagev1.VolumeAttachmentSpec{
			Source: storagev1.VolumeAttachmentSource{PersistentVolumeName: &pvNameRef},
		},
	}
}

func newCBTConfig(namespace, name string, statusState *cbtconfigv1alpha1.CBTState) *cbtconfigv1alpha1.CBTConfig {
	return &cbtconfigv1alpha1.CBTConfig{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: namespace},
		Status:     &cbtconfigv1alpha1.CBTConfigStatus{State: statusState},
	}
}

func cnsVolumeWithCBT(id string, status cnstypes.CnsVolumeCBTStatus) cnstypes.CnsVolume {
	return cnstypes.CnsVolume{
		VolumeId:             cnstypes.CnsVolumeId{Id: id},
		ChangedBlockTracking: status,
	}
}

func statePtr(s cbtconfigv1alpha1.CBTState) *cbtconfigv1alpha1.CBTState { return &s }

// newTestListers builds in-memory PV / PVC / VolumeAttachment listers seeded with the given objects.
func newTestListers(t *testing.T, objs ...client.Object) (corelisters.PersistentVolumeLister,
	corelisters.PersistentVolumeClaimLister, storagelistersv1.VolumeAttachmentLister) {
	t.Helper()
	pvIdx := cache.NewIndexer(cache.MetaNamespaceKeyFunc,
		cache.Indexers{cache.NamespaceIndex: cache.MetaNamespaceIndexFunc})
	pvcIdx := cache.NewIndexer(cache.MetaNamespaceKeyFunc,
		cache.Indexers{cache.NamespaceIndex: cache.MetaNamespaceIndexFunc})
	vaIdx := cache.NewIndexer(cache.MetaNamespaceKeyFunc,
		cache.Indexers{cache.NamespaceIndex: cache.MetaNamespaceIndexFunc})
	for _, o := range objs {
		switch obj := o.(type) {
		case *v1.PersistentVolume:
			require.NoError(t, pvIdx.Add(obj))
		case *v1.PersistentVolumeClaim:
			require.NoError(t, pvcIdx.Add(obj))
		case *storagev1.VolumeAttachment:
			require.NoError(t, vaIdx.Add(obj))
		}
	}
	return corelisters.NewPersistentVolumeLister(pvIdx),
		corelisters.NewPersistentVolumeClaimLister(pvcIdx),
		storagelistersv1.NewVolumeAttachmentLister(vaIdx)
}

// newTestReconciler builds a *ReconcileCBTConfig directly (bypassing newReconciler) so tests
// don't have to satisfy the controller-runtime manager.Manager interface.
func newTestReconciler(c client.Client, kubeClient clientset.Interface, vm cnsvolume.Manager,
	pvLister corelisters.PersistentVolumeLister,
	pvcLister corelisters.PersistentVolumeClaimLister,
	vaLister storagelistersv1.VolumeAttachmentLister) *ReconcileCBTConfig {
	return &ReconcileCBTConfig{
		client:    c,
		scheme:    c.Scheme(),
		cbtSyncer: syncer.NewCBTSyncer(kubeClient, vm, pvLister, pvcLister, vaLister),
	}
}

// newSeededReconciler builds a reconciler whose r.client, kubeClient, and listers are
// seeded with the same objs. PVCs are added to the fake clientset so CBT label patches
// succeed; non-PVC objects are filtered out.
func newSeededReconciler(t *testing.T, vm cnsvolume.Manager,
	objs ...client.Object) (*ReconcileCBTConfig, client.Client) {
	t.Helper()
	c := newTestClientBuilder(t).WithObjects(objs...).Build()
	pvLister, pvcLister, vaLister := newTestListers(t, objs...)
	pvcObjs := make([]runtime.Object, 0, len(objs))
	for _, o := range objs {
		if pvc, ok := o.(*v1.PersistentVolumeClaim); ok {
			pvcObjs = append(pvcObjs, pvc)
		}
	}
	kube := fakekube.NewSimpleClientset(pvcObjs...)
	return newTestReconciler(c, kube, vm, pvLister, pvcLister, vaLister), c
}

func TestCbtStatusState(t *testing.T) {
	tests := []struct {
		name           string
		status         *cbtconfigv1alpha1.CBTConfigStatus
		wantActive     bool
		wantConfigured bool
	}{
		{name: "StatusNil", status: nil,
			wantActive: false, wantConfigured: false},
		{name: "StateNil", status: &cbtconfigv1alpha1.CBTConfigStatus{State: nil},
			wantActive: false, wantConfigured: false},
		{name: "StateInactive", status: &cbtconfigv1alpha1.CBTConfigStatus{
			State: statePtr(cbtconfigv1alpha1.CBTStateInactive)},
			wantActive: false, wantConfigured: true},
		{name: "StateActive", status: &cbtconfigv1alpha1.CBTConfigStatus{
			State: statePtr(cbtconfigv1alpha1.CBTStateActive)},
			wantActive: true, wantConfigured: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			active, configured := cbtStatusState(tt.status)
			assert.Equal(t, tt.wantActive, active, "active")
			assert.Equal(t, tt.wantConfigured, configured, "configured")
		})
	}
}

// eventuallyEqual polls fn until it returns the expected slice or the timeout elapses,
// then asserts equality. Background CBT work runs on a goroutine launched by
// ReconcileCBTForNamespace, so observable side effects (Set/Clear calls on the fake
// volume manager) are not visible the moment Reconcile returns.
func eventuallyEqual(t *testing.T, fn func() []string, want []string, msg string) {
	t.Helper()
	assert.Eventually(t, func() bool {
		got := fn()
		if len(got) != len(want) {
			return false
		}
		for i := range got {
			if got[i] != want[i] {
				return false
			}
		}
		return true
	}, time.Second*5, time.Millisecond*10, msg)
}

// neverHasCalls polls fn for the given duration and asserts the slice stays empty.
// Used by tests that expect the background goroutine to short-circuit before issuing
// any Set/Clear call (e.g. when phase 2 fails or all volumes are already in state).
func neverHasCalls(t *testing.T, fn func() []string, msg string) {
	t.Helper()
	assert.Never(t, func() bool {
		return len(fn()) > 0
	}, time.Millisecond*200, time.Millisecond*10, msg)
}

func TestReconcile(t *testing.T) {
	ctx, _ := logger.GetNewContextWithLogger()

	inactiveQueryResult := func(volumeIDs ...string) *cnstypes.CnsQueryResult {
		vols := make([]cnstypes.CnsVolume, 0, len(volumeIDs))
		for _, id := range volumeIDs {
			vols = append(vols, cnsVolumeWithCBT(id, cnstypes.CnsVolumeCBTStatusDisabled))
		}
		return &cnstypes.CnsQueryResult{Volumes: vols}
	}
	activeQueryResult := func(volumeIDs ...string) *cnstypes.CnsQueryResult {
		vols := make([]cnstypes.CnsVolume, 0, len(volumeIDs))
		for _, id := range volumeIDs {
			vols = append(vols, cnsVolumeWithCBT(id, cnstypes.CnsVolumeCBTStatusEnabled))
		}
		return &cnstypes.CnsQueryResult{Volumes: vols}
	}

	t.Run("CBTConfigNotFoundReturnsNil", func(t *testing.T) {
		r, _ := newSeededReconciler(t, &fakeVolumeManager{})
		res, err := r.Reconcile(ctx, reconcile.Request{
			NamespacedName: types.NamespacedName{Namespace: "ns", Name: "absent"},
		})
		require.NoError(t, err)
		assert.Equal(t, reconcile.Result{}, res)
	})

	t.Run("CBTConfigBeingDeletedIsNoop", func(t *testing.T) {
		now := metav1.Now()
		cbt := newCBTConfig("ns", "cbt", statePtr(cbtconfigv1alpha1.CBTStateActive))
		cbt.DeletionTimestamp = &now
		cbt.Finalizers = []string{"keep-me"}
		vm := &fakeVolumeManager{}
		r, _ := newSeededReconciler(t, vm, cbt)
		res, err := r.Reconcile(ctx, reconcile.Request{
			NamespacedName: types.NamespacedName{Namespace: "ns", Name: "cbt"},
		})
		require.NoError(t, err)
		assert.Equal(t, reconcile.Result{}, res)
		assert.Empty(t, vm.setCallsCopy())
		assert.Empty(t, vm.clearCallsCopy())
	})

	t.Run("ActiveAppliesSetToUnattachedBlockPVCsOnly", func(t *testing.T) {
		cbt := newCBTConfig("ns", "cbt", statePtr(cbtconfigv1alpha1.CBTStateActive))
		pvAttached := newCBTBlockPV("pv-attached", "vol-attached")
		pvUnattached := newCBTBlockPV("pv-unattached", "vol-unattached")
		pvcAttached := newPVC("ns", "pvc-attached", "pv-attached", nil, nil)
		pvcUnattached := newPVC("ns", "pvc-unattached", "pv-unattached", nil, nil)
		va := newVolumeAttachmentForPV("va-1", "pv-attached")

		vm := &fakeVolumeManager{queryResult: inactiveQueryResult("vol-unattached")}
		r, _ := newSeededReconciler(t, vm, cbt, pvAttached, pvUnattached,
			pvcAttached, pvcUnattached, va)

		res, err := r.Reconcile(ctx, reconcile.Request{
			NamespacedName: types.NamespacedName{Namespace: "ns", Name: "cbt"},
		})
		require.NoError(t, err)
		assert.Equal(t, reconcile.Result{}, res)
		eventuallyEqual(t, vm.setCallsCopy, []string{"vol-unattached"},
			"background goroutine must call Set on the unattached volume only")
		neverHasCalls(t, vm.clearCallsCopy, "Clear must not be called on active")
	})

	t.Run("InactiveClearsCBTOnPreviouslyActiveUnattachedPVCs", func(t *testing.T) {
		cbt := newCBTConfig("ns", "cbt", statePtr(cbtconfigv1alpha1.CBTStateInactive))
		pv := newCBTBlockPV("pv-1", "vol-1")
		pvc := newPVC("ns", "pvc-1", "pv-1", map[string]string{"cbt": "true"}, nil)

		vm := &fakeVolumeManager{queryResult: activeQueryResult("vol-1")}
		r, _ := newSeededReconciler(t, vm, cbt, pv, pvc)

		res, err := r.Reconcile(ctx, reconcile.Request{
			NamespacedName: types.NamespacedName{Namespace: "ns", Name: "cbt"},
		})
		require.NoError(t, err)
		assert.Equal(t, reconcile.Result{}, res)
		eventuallyEqual(t, vm.clearCallsCopy, []string{"vol-1"},
			"background goroutine must call Clear when setting to inactive")
		neverHasCalls(t, vm.setCallsCopy, "Set must not be called on inactive")
	})

	t.Run("ActiveSkipsNonBoundPVCs", func(t *testing.T) {
		cbt := newCBTConfig("ns", "cbt", statePtr(cbtconfigv1alpha1.CBTStateActive))
		pvBound := newCBTBlockPV("pv-bound", "vol-bound")
		pvLost := newCBTBlockPV("pv-lost", "vol-lost")
		pvcBound := newPVC("ns", "pvc-bound", "pv-bound", nil, nil)
		pvcPending := newPVCWithPhase("ns", "pvc-pending", "", v1.ClaimPending)
		pvcLost := newPVCWithPhase("ns", "pvc-lost", "pv-lost", v1.ClaimLost)

		vm := &fakeVolumeManager{queryResult: inactiveQueryResult("vol-bound")}
		r, _ := newSeededReconciler(t, vm, cbt, pvBound, pvLost, pvcBound, pvcPending, pvcLost)

		res, err := r.Reconcile(ctx, reconcile.Request{
			NamespacedName: types.NamespacedName{Namespace: "ns", Name: "cbt"},
		})
		require.NoError(t, err)
		assert.Equal(t, reconcile.Result{}, res)
		eventuallyEqual(t, vm.setCallsCopy, []string{"vol-bound"},
			"only the Bound PVC's volume should have CBT active")
	})

	t.Run("ActiveSkipsPVCsAlreadyInTargetCBTState", func(t *testing.T) {
		cbt := newCBTConfig("ns", "cbt", statePtr(cbtconfigv1alpha1.CBTStateActive))
		pv := newCBTBlockPV("pv-1", "vol-1")
		pvc := newPVC("ns", "pvc-1", "pv-1", map[string]string{"cbt": "true"}, nil)
		vm := &fakeVolumeManager{queryResult: &cnstypes.CnsQueryResult{}}
		r, _ := newSeededReconciler(t, vm, cbt, pv, pvc)

		res, err := r.Reconcile(ctx, reconcile.Request{
			NamespacedName: types.NamespacedName{Namespace: "ns", Name: "cbt"},
		})
		require.NoError(t, err)
		assert.Equal(t, reconcile.Result{}, res)
		neverHasCalls(t, vm.setCallsCopy, "Set must not be called when CBT already active")
		neverHasCalls(t, vm.clearCallsCopy, "Clear must not be called on active")
	})

	// Phase 2 (CNS query) failures used to bubble back through ReconcileCBTForNamespace and
	// requeue at the controller layer. Now that the work runs on a bounded background pool,
	// query failures are logged and absorbed by the goroutine — re-convergence happens via
	// the periodic sync. The controller's only requeue trigger is "queue full".
	t.Run("QueryErrorIsAbsorbedByBackgroundGoroutine", func(t *testing.T) {
		cbt := newCBTConfig("ns", "cbt", statePtr(cbtconfigv1alpha1.CBTStateActive))
		pv := newCBTBlockPV("pv-1", "vol-1")
		pvc := newPVC("ns", "pvc-1", "pv-1", nil, nil)
		vm := &fakeVolumeManager{queryErr: errors.New("cns boom")}
		r, _ := newSeededReconciler(t, vm, cbt, pv, pvc)

		res, err := r.Reconcile(ctx, reconcile.Request{
			NamespacedName: types.NamespacedName{Namespace: "ns", Name: "cbt"},
		})
		require.NoError(t, err)
		assert.Equal(t, reconcile.Result{}, res,
			"phase failures are absorbed by the background goroutine; controller does not requeue")
		neverHasCalls(t, vm.setCallsCopy, "Set must not be called when phase 2 fails")
	})

	t.Run("VolumeOpFailureIsLogged", func(t *testing.T) {
		cbt := newCBTConfig("ns", "cbt", statePtr(cbtconfigv1alpha1.CBTStateActive))
		pv := newCBTBlockPV("pv-1", "vol-1")
		pvc := newPVC("ns", "pvc-1", "pv-1", nil, nil)
		vm := &fakeVolumeManager{
			setErr: errors.New("cns set failed"),
			queryResult: &cnstypes.CnsQueryResult{
				Volumes: []cnstypes.CnsVolume{cnsVolumeWithCBT("vol-1", cnstypes.CnsVolumeCBTStatusDisabled)},
			},
		}
		r, _ := newSeededReconciler(t, vm, cbt, pv, pvc)

		res, err := r.Reconcile(ctx, reconcile.Request{
			NamespacedName: types.NamespacedName{Namespace: "ns", Name: "cbt"},
		})
		require.NoError(t, err)
		assert.Equal(t, reconcile.Result{}, res, "per-volume set failure is best-effort; no requeue expected")
		eventuallyEqual(t, vm.setCallsCopy, []string{"vol-1"},
			"background goroutine should still attempt the per-volume Set even on failure")
	})
}
