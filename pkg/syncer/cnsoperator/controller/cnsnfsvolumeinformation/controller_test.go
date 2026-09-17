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

package cnsnfsvolumeinformation

import (
	"context"
	"errors"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	v1a1 "sigs.k8s.io/vsphere-csi-driver/v3/pkg/apis/cnsoperator/cnsnfsvolumeinformation/v1alpha1"
	cnsvolume "sigs.k8s.io/vsphere-csi-driver/v3/pkg/common/cns-lib/volume"
)

const (
	testCRName       = "vks1-cluster-uid-1-nfsvolumes"
	testNamespace    = "sv-ns"
	testVKSName      = "vks1"
	testVKSID        = "cluster-uid-1"
	testPVCUID       = "pvc-uid-a"
	testFullVolumeID = testVKSName + "-" + testVKSID + "-" + testPVCUID
)

// fakeVolumeManager is a configurable cnsvolume.Manager for testing the reconciler's
// Register/Update dispatch, embedding cnsvolume.MockManager to satisfy the rest of the
// interface (every other method panics if called, which is deliberate: this reconciler
// must never call anything but RegisterNfsVolumeInfo/UpdateNfsVolumeInfo).
type fakeVolumeManager struct {
	cnsvolume.MockManager

	mu            sync.Mutex
	registerCalls []string
	updateCalls   []string
	deleteCalls   []string
	registerErr   error
	updateErr     error
	deleteErr     error
}

func (f *fakeVolumeManager) RegisterNfsVolumeInfo(_ context.Context, volumeID string, _ v1a1.NfsVolumeEntry) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.registerCalls = append(f.registerCalls, volumeID)
	return f.registerErr
}

func (f *fakeVolumeManager) UpdateNfsVolumeInfo(_ context.Context, volumeID string, _ v1a1.NfsVolumeEntry) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.updateCalls = append(f.updateCalls, volumeID)
	return f.updateErr
}

func (f *fakeVolumeManager) DeleteNfsVolumeInfo(_ context.Context, volumeID string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.deleteCalls = append(f.deleteCalls, volumeID)
	return f.deleteErr
}

func buildScheme(t *testing.T) *runtime.Scheme {
	t.Helper()
	s := runtime.NewScheme()
	gv := schema.GroupVersion{Group: "cns.vmware.com", Version: "v1alpha1"}
	s.AddKnownTypes(gv, &v1a1.CnsNfsVolumeInformation{}, &v1a1.CnsNfsVolumeInformationList{})
	metav1.AddToGroupVersion(s, gv)
	return s
}

func newReconciler(t *testing.T, fvm *fakeVolumeManager, objs ...client.Object) (*Reconciler, client.Client) {
	t.Helper()
	c := fake.NewClientBuilder().
		WithScheme(buildScheme(t)).
		WithStatusSubresource(&v1a1.CnsNfsVolumeInformation{}).
		WithObjects(objs...).
		Build()
	return &Reconciler{client: c, volumeManager: fvm}, c
}

func baseCR(volumes map[string]v1a1.NfsVolumeEntry) *v1a1.CnsNfsVolumeInformation {
	return &v1a1.CnsNfsVolumeInformation{
		ObjectMeta: metav1.ObjectMeta{Name: testCRName, Namespace: testNamespace},
		Spec: v1a1.CnsNfsVolumeInformationSpec{
			VKSClusterName: testVKSName,
			VKSClusterID:   testVKSID,
			Volumes:        volumes,
		},
	}
}

func reconcileRequest() reconcile.Request {
	return reconcile.Request{NamespacedName: client.ObjectKey{Name: testCRName, Namespace: testNamespace}}
}

func getCR(t *testing.T, c client.Client) *v1a1.CnsNfsVolumeInformation {
	t.Helper()
	obj := &v1a1.CnsNfsVolumeInformation{}
	require.NoError(t, c.Get(context.Background(), client.ObjectKey{Name: testCRName, Namespace: testNamespace}, obj))
	return obj
}

func TestReconcile_NewVolumeRegistersOnce(t *testing.T) {
	fvm := &fakeVolumeManager{}
	entry := v1a1.NfsVolumeEntry{ClaimName: "pvc-a", Health: "accessible"}
	r, c := newReconciler(t, fvm, baseCR(map[string]v1a1.NfsVolumeEntry{testPVCUID: entry}))

	_, err := r.Reconcile(context.Background(), reconcileRequest())
	require.NoError(t, err)

	assert.Equal(t, []string{testFullVolumeID}, fvm.registerCalls)

	got := getCR(t, c)
	assert.Equal(t, []string{testFullVolumeID}, got.Status.SyncedVolumeIDs)
}

func TestReconcile_AlreadySyncedVolumeSkipped(t *testing.T) {
	fvm := &fakeVolumeManager{}
	entry := v1a1.NfsVolumeEntry{ClaimName: "pvc-a"}
	r, c := newReconciler(t, fvm, baseCR(map[string]v1a1.NfsVolumeEntry{testPVCUID: entry}))

	_, err := r.Reconcile(context.Background(), reconcileRequest())
	require.NoError(t, err)
	rvAfterFirst := getCR(t, c).ResourceVersion

	// Reconcile again with no change to Spec.Volumes.
	_, err = r.Reconcile(context.Background(), reconcileRequest())
	require.NoError(t, err)

	assert.Len(t, fvm.registerCalls, 1, "must not re-register an already-synced entry")
	assert.Equal(t, rvAfterFirst, getCR(t, c).ResourceVersion, "no-op reconcile must not write Status again")
}

func TestReconcile_ChangedEntryCallsUpdateNotRegister(t *testing.T) {
	fvm := &fakeVolumeManager{}
	r, c := newReconciler(t, fvm,
		baseCR(map[string]v1a1.NfsVolumeEntry{testPVCUID: {ClaimName: "pvc-a", Health: "accessible"}}))

	_, err := r.Reconcile(context.Background(), reconcileRequest())
	require.NoError(t, err)
	require.Equal(t, []string{testFullVolumeID}, fvm.registerCalls)

	// Simulate pvCSI patching health on the CR (as PatchVolumeEntryHealthAndPods would).
	current := getCR(t, c)
	entry := current.Spec.Volumes[testPVCUID]
	entry.Health = "inaccessible"
	current.Spec.Volumes[testPVCUID] = entry
	require.NoError(t, c.Update(context.Background(), current))

	_, err = r.Reconcile(context.Background(), reconcileRequest())
	require.NoError(t, err)

	assert.Len(t, fvm.registerCalls, 1, "must not re-register an already-registered volume")
	assert.Equal(t, []string{testFullVolumeID}, fvm.updateCalls,
		"a changed, already-registered volume must be Updated")
	assert.Equal(t, []string{testFullVolumeID}, getCR(t, c).Status.SyncedVolumeIDs)
}

func TestReconcile_SecondNewVolumeOnlyRegistersTheNewOne(t *testing.T) {
	fvm := &fakeVolumeManager{}
	r, c := newReconciler(t, fvm, baseCR(map[string]v1a1.NfsVolumeEntry{testPVCUID: {ClaimName: "pvc-a"}}))

	_, err := r.Reconcile(context.Background(), reconcileRequest())
	require.NoError(t, err)
	require.Equal(t, []string{testFullVolumeID}, fvm.registerCalls)

	current := getCR(t, c)
	current.Spec.Volumes["pvc-uid-b"] = v1a1.NfsVolumeEntry{ClaimName: "pvc-b"}
	require.NoError(t, c.Update(context.Background(), current))

	_, err = r.Reconcile(context.Background(), reconcileRequest())
	require.NoError(t, err)

	secondVolumeID := testVKSName + "-" + testVKSID + "-pvc-uid-b"
	assert.ElementsMatch(t, []string{testFullVolumeID, secondVolumeID}, fvm.registerCalls)
	assert.ElementsMatch(t, []string{testFullVolumeID, secondVolumeID}, getCR(t, c).Status.SyncedVolumeIDs)
}

func TestReconcile_RegisterFailureRequeuesAndDoesNotMarkSynced(t *testing.T) {
	wantErr := errors.New("CNS unreachable")
	fvm := &fakeVolumeManager{registerErr: wantErr}
	r, c := newReconciler(t, fvm, baseCR(map[string]v1a1.NfsVolumeEntry{testPVCUID: {ClaimName: "pvc-a"}}))

	_, err := r.Reconcile(context.Background(), reconcileRequest())
	require.Error(t, err, "a relay failure must be returned so controller-runtime requeues with backoff")

	got := getCR(t, c)
	assert.Empty(t, got.Status.SyncedVolumeIDs, "a failed register must not be recorded as synced")
}

func TestReconcile_MissingCRIsIgnored(t *testing.T) {
	fvm := &fakeVolumeManager{}
	r, _ := newReconciler(t, fvm) // no objects seeded

	_, err := r.Reconcile(context.Background(), reconcileRequest())
	assert.NoError(t, err)
	assert.Empty(t, fvm.registerCalls)
}

func TestReconcile_RelaySucceededConditionReflectsOutcome(t *testing.T) {
	fvm := &fakeVolumeManager{}
	r, c := newReconciler(t, fvm, baseCR(map[string]v1a1.NfsVolumeEntry{testPVCUID: {ClaimName: "pvc-a"}}))

	_, err := r.Reconcile(context.Background(), reconcileRequest())
	require.NoError(t, err)

	cond := findCondition(getCR(t, c).Status.Conditions, relaySucceededCondition)
	require.NotNil(t, cond)
	assert.Equal(t, metav1.ConditionTrue, cond.Status)

	fvm.registerErr = errors.New("CNS unreachable")
	current := getCR(t, c)
	current.Spec.Volumes["pvc-uid-b"] = v1a1.NfsVolumeEntry{ClaimName: "pvc-b"}
	require.NoError(t, c.Update(context.Background(), current))

	_, err = r.Reconcile(context.Background(), reconcileRequest())
	require.Error(t, err)

	cond = findCondition(getCR(t, c).Status.Conditions, relaySucceededCondition)
	require.NotNil(t, cond)
	assert.Equal(t, metav1.ConditionFalse, cond.Status)
	assert.Equal(t, "RelayFailed", cond.Reason)
}

func TestReconcile_PartialRemovalCallsDeleteNfsVolumeInfo(t *testing.T) {
	// Covers steps 12-14 for the case where the CR itself stays alive (another entry
	// remains) - only the removed key must be deleted from CNS.
	fvm := &fakeVolumeManager{}
	r, c := newReconciler(t, fvm, baseCR(map[string]v1a1.NfsVolumeEntry{
		testPVCUID:  {ClaimName: "pvc-a"},
		"pvc-uid-b": {ClaimName: "pvc-b"},
	}))

	_, err := r.Reconcile(context.Background(), reconcileRequest())
	require.NoError(t, err)
	require.Contains(t, getCR(t, c).Status.SyncedVolumeHashes, testFullVolumeID)

	current := getCR(t, c)
	delete(current.Spec.Volumes, testPVCUID)
	require.NoError(t, c.Update(context.Background(), current))

	_, err = r.Reconcile(context.Background(), reconcileRequest())
	require.NoError(t, err)

	assert.Equal(t, []string{testFullVolumeID}, fvm.deleteCalls)
	got := getCR(t, c)
	assert.NotContains(t, got.Status.SyncedVolumeHashes, testFullVolumeID)
	assert.NotContains(t, got.Status.SyncedVolumeIDs, testFullVolumeID)
	assert.Contains(t, got.Status.SyncedVolumeHashes, testVKSName+"-"+testVKSID+"-pvc-uid-b",
		"the remaining entry must be untouched")
}

func TestReconcile_PartialRemovalDeleteFailureKeepsHash(t *testing.T) {
	wantErr := errors.New("CNS unreachable")
	fvm := &fakeVolumeManager{}
	r, c := newReconciler(t, fvm, baseCR(map[string]v1a1.NfsVolumeEntry{testPVCUID: {ClaimName: "pvc-a"}}))

	_, err := r.Reconcile(context.Background(), reconcileRequest())
	require.NoError(t, err)

	current := getCR(t, c)
	current.Spec.Volumes = map[string]v1a1.NfsVolumeEntry{}
	require.NoError(t, c.Update(context.Background(), current))

	fvm.deleteErr = wantErr
	_, err = r.Reconcile(context.Background(), reconcileRequest())
	require.Error(t, err)

	got := getCR(t, c)
	assert.Contains(t, got.Status.SyncedVolumeHashes, testFullVolumeID,
		"a failed delete must not be dropped from bookkeeping - it isn't confirmed gone from CNS")
}

func TestReconcileDelete_DeletesAllSyncedVolumesThenRemovesFinalizer(t *testing.T) {
	fvm := &fakeVolumeManager{}
	cr := baseCR(map[string]v1a1.NfsVolumeEntry{testPVCUID: {ClaimName: "pvc-a"}})
	r, c := newReconciler(t, fvm, cr)

	// First reconcile: registers the volume and adds the finalizer.
	_, err := r.Reconcile(context.Background(), reconcileRequest())
	require.NoError(t, err)
	require.Contains(t, getCR(t, c).Finalizers, finalizerName)

	// pvCSI removes the last entry and deletes the whole CR (RemoveVolumeEntry's
	// behavior) - simulated here directly, since Spec.Volumes is already empty by the
	// time Delete is called in the real flow.
	current := getCR(t, c)
	current.Spec.Volumes = map[string]v1a1.NfsVolumeEntry{}
	require.NoError(t, c.Update(context.Background(), current))
	require.NoError(t, c.Delete(context.Background(), current))

	// The finalizer keeps the object present with DeletionTimestamp set.
	pending := getCR(t, c)
	require.NotNil(t, pending.DeletionTimestamp)
	require.Contains(t, pending.Status.SyncedVolumeHashes, testFullVolumeID,
		"Status must still be readable while the finalizer blocks actual deletion")

	_, err = r.Reconcile(context.Background(), reconcileRequest())
	require.NoError(t, err)

	assert.Equal(t, []string{testFullVolumeID}, fvm.deleteCalls)
	err = c.Get(context.Background(), client.ObjectKey{Name: testCRName, Namespace: testNamespace},
		&v1a1.CnsNfsVolumeInformation{})
	assert.True(t, apierrors.IsNotFound(err), "CR must be fully deleted once the finalizer is removed, got: %v", err)
}

func TestReconcileDelete_KeepsFinalizerOnPartialFailure(t *testing.T) {
	wantErr := errors.New("CNS unreachable")
	fvm := &fakeVolumeManager{deleteErr: wantErr}
	cr := baseCR(map[string]v1a1.NfsVolumeEntry{testPVCUID: {ClaimName: "pvc-a"}})
	r, c := newReconciler(t, fvm, cr)

	_, err := r.Reconcile(context.Background(), reconcileRequest())
	require.NoError(t, err)

	current := getCR(t, c)
	current.Spec.Volumes = map[string]v1a1.NfsVolumeEntry{}
	require.NoError(t, c.Update(context.Background(), current))
	require.NoError(t, c.Delete(context.Background(), current))

	_, err = r.Reconcile(context.Background(), reconcileRequest())
	require.Error(t, err, "a failed CNS delete must requeue rather than let the CR disappear")

	got := &v1a1.CnsNfsVolumeInformation{}
	require.NoError(t, c.Get(context.Background(),
		client.ObjectKey{Name: testCRName, Namespace: testNamespace}, got))
	assert.Contains(t, got.Finalizers, finalizerName, "finalizer must remain until CNS confirms deletion")
	assert.Contains(t, got.Status.SyncedVolumeHashes, testFullVolumeID)
}

func TestReconcileDelete_NoFinalizerIsNoOp(t *testing.T) {
	fvm := &fakeVolumeManager{}
	cr := baseCR(nil)
	now := metav1.Now()
	cr.DeletionTimestamp = &now
	cr.Finalizers = []string{"some-other-finalizer"} // keeps the fake client from hard-deleting on create/get
	r, _ := newReconciler(t, fvm, cr)

	_, err := r.Reconcile(context.Background(), reconcileRequest())
	require.NoError(t, err)
	assert.Empty(t, fvm.deleteCalls, "a CR that never got our finalizer has nothing recorded to clean up")
}

// findCondition finds a condition by type without pulling in the full
// k8s.io/apimachinery/pkg/api/meta helper surface just for a test assertion.
func findCondition(conditions []metav1.Condition, condType string) *metav1.Condition {
	for i := range conditions {
		if conditions[i].Type == condType {
			return &conditions[i]
		}
	}
	return nil
}
