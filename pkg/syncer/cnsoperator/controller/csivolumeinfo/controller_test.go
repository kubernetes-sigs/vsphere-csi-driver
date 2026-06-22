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

package csivolumeinfo

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	cnstypes "github.com/vmware/govmomi/cns/types"
	"github.com/vmware/govmomi/object"
	vim25types "github.com/vmware/govmomi/vim25/types"
	corev1 "k8s.io/api/core/v1"
	storagev1 "k8s.io/api/storage/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	k8stypes "k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	csivolumeinfosvc "sigs.k8s.io/vsphere-csi-driver/v3/pkg/apis/cnsoperator/csivolumeinfo"
	csivolumeinfov1alpha1 "sigs.k8s.io/vsphere-csi-driver/v3/pkg/apis/cnsoperator/csivolumeinfo/v1alpha1"
	cnsvolumes "sigs.k8s.io/vsphere-csi-driver/v3/pkg/common/cns-lib/volume"
	cnsvsphere "sigs.k8s.io/vsphere-csi-driver/v3/pkg/common/cns-lib/vsphere"
	commonconfig "sigs.k8s.io/vsphere-csi-driver/v3/pkg/common/config"
	cnsvolumeoperationrequest "sigs.k8s.io/vsphere-csi-driver/v3/pkg/internalapis/cnsvolumeoperationrequest"
)

// ---------------------------------------------------------------------------
// fakeCsiVolumeInfoService — in-process service backed by a fake client.
// ---------------------------------------------------------------------------

// fakeCsiVolumeInfoService wraps a controller-runtime fake client and provides
// the CsiVolumeInfoService interface for tests.  Its methods are thin delegates
// to the concrete service implementation so we exercise real service logic.
type fakeCsiVolumeInfoService struct {
	inner csivolumeinfosvc.CsiVolumeInfoService
}

func newFakeCviService(c client.Client) csivolumeinfosvc.CsiVolumeInfoService {
	return &fakeCsiVolumeInfoService{inner: csivolumeinfosvc.NewCsiVolumeInfoService(c)}
}

func (f *fakeCsiVolumeInfoService) CreateCsiVolumeInfo(
	ctx context.Context, cvi *csivolumeinfov1alpha1.CsiVolumeInfo) error {
	return f.inner.CreateCsiVolumeInfo(ctx, cvi)
}

func (f *fakeCsiVolumeInfoService) GetCsiVolumeInfo(
	ctx context.Context, volumeID string) (*csivolumeinfov1alpha1.CsiVolumeInfo, error) {
	return f.inner.GetCsiVolumeInfo(ctx, volumeID)
}

func (f *fakeCsiVolumeInfoService) UpdateCsiVolumeInfoStatus(
	ctx context.Context, cvi *csivolumeinfov1alpha1.CsiVolumeInfo) error {
	return f.inner.UpdateCsiVolumeInfoStatus(ctx, cvi)
}

func (f *fakeCsiVolumeInfoService) PatchCsiVolumeInfo(
	ctx context.Context, volumeID string, patchBytes []byte) (int64, error) {
	return f.inner.PatchCsiVolumeInfo(ctx, volumeID, patchBytes)
}

func (f *fakeCsiVolumeInfoService) PatchCsiVolumeInfoStatus(
	ctx context.Context, volumeID string, patchBytes []byte) error {
	return f.inner.PatchCsiVolumeInfoStatus(ctx, volumeID, patchBytes)
}

func (f *fakeCsiVolumeInfoService) DeleteCsiVolumeInfo(
	ctx context.Context, volumeID string) error {
	return f.inner.DeleteCsiVolumeInfo(ctx, volumeID)
}

func (f *fakeCsiVolumeInfoService) CsiVolumeInfoExists(
	ctx context.Context, volumeID string) (bool, error) {
	return f.inner.CsiVolumeInfoExists(ctx, volumeID)
}

func (f *fakeCsiVolumeInfoService) AddVolumeProtectionFinalizer(
	ctx context.Context, volumeID string) error {
	return f.inner.AddVolumeProtectionFinalizer(ctx, volumeID)
}

func (f *fakeCsiVolumeInfoService) RemoveVolumeProtectionFinalizer(
	ctx context.Context, volumeID string) error {
	return f.inner.RemoveVolumeProtectionFinalizer(ctx, volumeID)
}

// genBumpingCviService simulates the API server incrementing metadata.generation
// on a spec patch.  It applies the patch through the wrapped service but reports a
// higher generation than the fake store actually records (the controller-runtime
// fake client does not bump generation on a patch).  This lets a test verify that
// the reconciler records observedGeneration from the patch result rather than from
// the generation observed at reconcile entry.
type genBumpingCviService struct {
	csivolumeinfosvc.CsiVolumeInfoService
	bumpedGen int64
}

func (g *genBumpingCviService) PatchCsiVolumeInfo(
	ctx context.Context, volumeID string, patchBytes []byte) (int64, error) {
	if _, err := g.CsiVolumeInfoService.PatchCsiVolumeInfo(ctx, volumeID, patchBytes); err != nil {
		return 0, err
	}
	return g.bumpedGen, nil
}

// ---------------------------------------------------------------------------
// testVolumeManager — fully configurable mock for the Manager interface.
// ---------------------------------------------------------------------------

// testVolumeManager is a test-scoped implementation of volumes.Manager that
// lets each test control the behaviour of UnregisterVolumeEx, AckUnregister,
// and CreateVolume independently.
type testVolumeManager struct {
	unregisterVolumeExFn func(ctx context.Context, volumeID string) (string, string, error)
	ackUnregisterFn      func(ctx context.Context, volumeID string) error
	createVolumeFn       func(ctx context.Context, spec *cnstypes.CnsVolumeCreateSpec,
		extraParams interface{}) (*cnsvolumes.CnsVolumeInfo, string, error)
}

func (m *testVolumeManager) UnregisterVolumeEx(ctx context.Context, volumeID string) (string, string, error) {
	if m.unregisterVolumeExFn != nil {
		return m.unregisterVolumeExFn(ctx, volumeID)
	}
	return "test-disk-path", "test-disk-uuid", nil
}

func (m *testVolumeManager) AckUnregister(ctx context.Context, volumeID string) error {
	if m.ackUnregisterFn != nil {
		return m.ackUnregisterFn(ctx, volumeID)
	}
	return nil
}

func (m *testVolumeManager) GetDiskFolderURL(ctx context.Context, datastorePath string) (string, error) {
	return "", nil
}

func (m *testVolumeManager) CreateVolume(ctx context.Context, spec *cnstypes.CnsVolumeCreateSpec,
	extraParams interface{}) (*cnsvolumes.CnsVolumeInfo, string, error) {
	if m.createVolumeFn != nil {
		return m.createVolumeFn(ctx, spec, extraParams)
	}
	return &cnsvolumes.CnsVolumeInfo{}, "", nil
}

// Remaining Manager interface methods — not exercised by these tests.
func (m *testVolumeManager) AttachVolume(ctx context.Context, vm *cnsvsphere.VirtualMachine,
	volumeID string, checkNVMeController bool) (string, string, error) {
	panic("not implemented")
}
func (m *testVolumeManager) DetachVolume(ctx context.Context, vm *cnsvsphere.VirtualMachine,
	volumeID string) (string, error) {
	panic("not implemented")
}
func (m *testVolumeManager) DeleteVolume(ctx context.Context, volumeID string,
	deleteDisk bool) (string, error) {
	panic("not implemented")
}
func (m *testVolumeManager) UpdateVolumeMetadata(ctx context.Context,
	spec *cnstypes.CnsVolumeMetadataUpdateSpec) error {
	panic("not implemented")
}
func (m *testVolumeManager) UpdateVolumeCrypto(ctx context.Context,
	spec *cnstypes.CnsVolumeCryptoUpdateSpec) error {
	panic("not implemented")
}
func (m *testVolumeManager) QueryVolumeInfo(ctx context.Context,
	volumeIDList []cnstypes.CnsVolumeId) (*cnstypes.CnsQueryVolumeInfoResult, error) {
	panic("not implemented")
}
func (m *testVolumeManager) QueryAllVolume(ctx context.Context, queryFilter cnstypes.CnsQueryFilter,
	querySelection cnstypes.CnsQuerySelection) (*cnstypes.CnsQueryResult, error) {
	panic("not implemented")
}
func (m *testVolumeManager) QueryVolumeAsync(ctx context.Context, queryFilter cnstypes.CnsQueryFilter,
	querySelection *cnstypes.CnsQuerySelection) (*cnstypes.CnsQueryResult, error) {
	panic("not implemented")
}
func (m *testVolumeManager) QueryVolume(ctx context.Context,
	queryFilter cnstypes.CnsQueryFilter) (*cnstypes.CnsQueryResult, error) {
	panic("not implemented")
}
func (m *testVolumeManager) RelocateVolume(ctx context.Context,
	relocateSpecList ...cnstypes.BaseCnsVolumeRelocateSpec) (*object.Task, error) {
	panic("not implemented")
}
func (m *testVolumeManager) ExpandVolume(ctx context.Context, volumeID string, size int64,
	extraParams interface{}) (string, error) {
	panic("not implemented")
}
func (m *testVolumeManager) ResetManager(ctx context.Context, vcenter *cnsvsphere.VirtualCenter) error {
	panic("not implemented")
}
func (m *testVolumeManager) ConfigureVolumeACLs(ctx context.Context,
	spec cnstypes.CnsVolumeACLConfigureSpec) error {
	panic("not implemented")
}
func (m *testVolumeManager) RegisterDisk(ctx context.Context, path string, name string) (string, error) {
	panic("not implemented")
}
func (m *testVolumeManager) RetrieveVStorageObject(ctx context.Context,
	volumeID string) (*vim25types.VStorageObject, error) {
	panic("not implemented")
}
func (m *testVolumeManager) ProtectVolumeFromVMDeletion(ctx context.Context, volumeID string) error {
	panic("not implemented")
}
func (m *testVolumeManager) UnprotectVolumeFromVMDeletion(ctx context.Context, volumeID string) error {
	panic("not implemented")
}
func (m *testVolumeManager) SetVolumeControlFlags(ctx context.Context, volumeID string,
	controlFlags []string) error {
	panic("not implemented")
}
func (m *testVolumeManager) ClearVolumeControlFlags(ctx context.Context, volumeID string,
	controlFlags []string) error {
	panic("not implemented")
}
func (m *testVolumeManager) CreateSnapshot(ctx context.Context, volumeID string, desc string,
	extraParams interface{}) (*cnsvolumes.CnsSnapshotInfo, error) {
	panic("not implemented")
}
func (m *testVolumeManager) DeleteSnapshot(ctx context.Context, volumeID string, snapshotID string,
	extraParams interface{}) (*cnsvolumes.CnsSnapshotInfo, error) {
	panic("not implemented")
}
func (m *testVolumeManager) QuerySnapshots(ctx context.Context,
	snapshotQueryFilter cnstypes.CnsSnapshotQueryFilter) (*cnstypes.CnsSnapshotQueryResult, error) {
	panic("not implemented")
}
func (m *testVolumeManager) MonitorCreateVolumeTask(ctx context.Context,
	details **cnsvolumeoperationrequest.VolumeOperationRequestDetails, task *object.Task,
	volNameFromInputSpec, clusterID string) (*cnsvolumes.CnsVolumeInfo, string, error) {
	panic("not implemented")
}
func (m *testVolumeManager) GetOperationStore() cnsvolumeoperationrequest.VolumeOperationRequest {
	panic("not implemented")
}
func (m *testVolumeManager) IsListViewReady() bool                   { return true }
func (m *testVolumeManager) SetListViewNotReady(ctx context.Context) {}
func (m *testVolumeManager) BatchAttachVolumes(ctx context.Context, vm *cnsvsphere.VirtualMachine,
	batchAttachRequest []cnsvolumes.BatchAttachRequest) ([]cnsvolumes.BatchAttachResult, string, error) {
	panic("not implemented")
}
func (m *testVolumeManager) UnregisterVolume(ctx context.Context, volumeID string,
	unregisterDisk bool) (string, error) {
	panic("not implemented")
}
func (m *testVolumeManager) SyncVolume(ctx context.Context,
	syncVolumeSpecs []cnstypes.CnsSyncVolumeSpec) (string, error) {
	panic("not implemented")
}
func (m *testVolumeManager) ReRegisterVolume(ctx context.Context, volumeID string) error {
	panic("not implemented")
}
func (m *testVolumeManager) QueryFCDAllocatedBlocks(ctx context.Context,
	volumeID, snapshotID string, startingOffset uint64) (
	[]cnsvolumes.DiskArea, uint64, error) {
	panic("not implemented")
}
func (m *testVolumeManager) QueryFCDChangedBlocks(ctx context.Context,
	volumeID, targetSnapshotID, baseChangeID string, startingOffset uint64) (
	[]cnsvolumes.DiskArea, uint64, error) {
	panic("not implemented")
}
func (m *testVolumeManager) QueryPendingUnregisters(ctx context.Context) (
	[]cnsvolumes.PendingUnregisterRecord, error) {
	return nil, nil
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

func newScheme(t *testing.T) *runtime.Scheme {
	t.Helper()
	s := runtime.NewScheme()
	require.NoError(t, csivolumeinfov1alpha1.AddToScheme(s))
	require.NoError(t, corev1.AddToScheme(s))
	require.NoError(t, storagev1.AddToScheme(s))
	return s
}

func newFakeClient(t *testing.T, s *runtime.Scheme, objs []client.Object,
	ifuncs interceptor.Funcs) client.Client {
	t.Helper()
	cb := fake.NewClientBuilder().
		WithScheme(s).
		WithObjects(objs...).
		WithStatusSubresource(&csivolumeinfov1alpha1.CsiVolumeInfo{}).
		WithInterceptorFuncs(ifuncs)
	return cb.Build()
}

func minimalConfigInfo() *commonconfig.ConfigurationInfo {
	cfg := &commonconfig.Config{
		VirtualCenter: map[string]*commonconfig.VirtualCenterConfig{
			"vcenter": {User: "test-user"},
		},
	}
	cfg.Global.ClusterID = "test-cluster"
	return &commonconfig.ConfigurationInfo{Cfg: cfg}
}

// newCVI creates a CsiVolumeInfo for testing.  The object is created in the
// vmware-system-csi namespace which is the fixed namespace for all CVIs.
func newCVI(volumeID string, vms []csivolumeinfov1alpha1.VirtualMachineRef,
	ownership csivolumeinfov1alpha1.OwnershipState,
	diskPath, diskUUID string,
	generation int64,
	finalizers []string) *csivolumeinfov1alpha1.CsiVolumeInfo {
	cvi := &csivolumeinfov1alpha1.CsiVolumeInfo{
		ObjectMeta: metav1.ObjectMeta{
			Name:       csivolumeinfosvc.GetCsiVolumeInfoCRName(volumeID),
			Namespace:  csivolumeinfov1alpha1.CVINamespace,
			Generation: generation,
			Finalizers: finalizers,
		},
		Spec: csivolumeinfov1alpha1.CsiVolumeInfoSpec{
			VolumeID:     volumeID,
			PVCName:      "test-pvc",
			PVCNamespace: "test-ns",
			PVName:       "test-pv",
			DiskPath:     diskPath,
			DiskUUID:     diskUUID,
			VMs:          vms,
		},
		Status: csivolumeinfov1alpha1.CsiVolumeInfoStatus{
			Ownership: ownership,
		},
	}
	return cvi
}

func makeRequest(volumeID string) reconcile.Request {
	return reconcile.Request{
		NamespacedName: k8stypes.NamespacedName{
			Namespace: csivolumeinfov1alpha1.CVINamespace,
			Name:      csivolumeinfosvc.GetCsiVolumeInfoCRName(volumeID),
		},
	}
}

func newTestPV(name string) *corev1.PersistentVolume {
	return &corev1.PersistentVolume{
		ObjectMeta: metav1.ObjectMeta{Name: name},
		Spec: corev1.PersistentVolumeSpec{
			StorageClassName: "test-sc",
			PersistentVolumeSource: corev1.PersistentVolumeSource{
				CSI: &corev1.CSIPersistentVolumeSource{
					Driver:           "csi.vsphere.volume",
					VolumeHandle:     "test-volume-id",
					VolumeAttributes: map[string]string{"storagePolicyID": "test-policy"},
				},
			},
		},
	}
}

func newTestPVC(name, ns string) *corev1.PersistentVolumeClaim {
	return &corev1.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: ns},
	}
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

// TestReconcile_NotFound verifies that a missing CVI is silently ignored
// (no requeue, no error).
func TestReconcile_NotFound(t *testing.T) {
	s := newScheme(t)
	c := newFakeClient(t, s, nil, interceptor.Funcs{
		Get: func(ctx context.Context, cl client.WithWatch, key client.ObjectKey,
			obj client.Object, opts ...client.GetOption) error {
			return apierrors.NewNotFound(schema.GroupResource{
				Group:    csivolumeinfov1alpha1.GroupName,
				Resource: csivolumeinfov1alpha1.CRDSingular,
			}, key.Name)
		},
	})
	r := &Reconciler{
		client:        c,
		scheme:        s,
		configInfo:    minimalConfigInfo(),
		volumeManager: &testVolumeManager{},
		cviSvc:        newFakeCviService(c),
	}
	backOffDuration = make(map[k8stypes.NamespacedName]time.Duration)

	res, err := r.Reconcile(context.Background(), makeRequest("vol-001"))
	assert.NoError(t, err)
	assert.True(t, res.IsZero())
}

// TestReconcile_GetError verifies that a transient Get error is returned so the
// controller-runtime framework requeues it.
func TestReconcile_GetError(t *testing.T) {
	s := newScheme(t)
	c := newFakeClient(t, s, nil, interceptor.Funcs{
		Get: func(ctx context.Context, cl client.WithWatch, key client.ObjectKey,
			obj client.Object, opts ...client.GetOption) error {
			return errors.New("transient error")
		},
	})
	r := &Reconciler{
		client:        c,
		scheme:        s,
		configInfo:    minimalConfigInfo(),
		volumeManager: &testVolumeManager{},
		cviSvc:        newFakeCviService(c),
	}
	backOffDuration = make(map[k8stypes.NamespacedName]time.Duration)

	_, err := r.Reconcile(context.Background(), makeRequest("vol-001"))
	assert.Error(t, err)
}

// TestReconcile_UnregisterTransition verifies the full unregister path:
// spec.vms=[vmA], ownership="" → UnregisterVolumeEx called, spec.diskPath/diskUUID
// patched, protection finalizer added, status.ownership=VMManaged, phase=Succeeded,
// observedGeneration matches, AckUnregister called.
func TestReconcile_UnregisterTransition(t *testing.T) {
	const volID = "vol-unregister"
	vms := []csivolumeinfov1alpha1.VirtualMachineRef{{VMName: "vm-a"}}
	cvi := newCVI(volID, vms, "", "", "", 3, nil)

	s := newScheme(t)
	c := newFakeClient(t, s, []client.Object{cvi, newTestPV("test-pv")}, interceptor.Funcs{})

	ackCalled := false
	mgr := &testVolumeManager{
		unregisterVolumeExFn: func(_ context.Context, id string) (string, string, error) {
			assert.Equal(t, volID, id)
			return "/vmfs/volumes/ds/disk.vmdk", "disk-uuid-123", nil
		},
		ackUnregisterFn: func(_ context.Context, id string) error {
			assert.Equal(t, volID, id)
			ackCalled = true
			return nil
		},
	}

	r := &Reconciler{
		client:        c,
		scheme:        s,
		configInfo:    minimalConfigInfo(),
		volumeManager: mgr,
		cviSvc:        newFakeCviService(c),
	}
	backOffDuration = make(map[k8stypes.NamespacedName]time.Duration)

	res, err := r.Reconcile(context.Background(), makeRequest(volID))
	require.NoError(t, err)
	assert.True(t, res.IsZero())
	assert.True(t, ackCalled, "AckUnregister must be called")

	// Read updated CVI from fake store.
	updated := &csivolumeinfov1alpha1.CsiVolumeInfo{}
	require.NoError(t, c.Get(context.Background(), k8stypes.NamespacedName{
		Namespace: csivolumeinfov1alpha1.CVINamespace,
		Name:      csivolumeinfosvc.GetCsiVolumeInfoCRName(volID),
	}, updated))

	assert.Equal(t, "/vmfs/volumes/ds/disk.vmdk", updated.Spec.DiskPath)
	assert.Equal(t, "disk-uuid-123", updated.Spec.DiskUUID)
	assert.Contains(t, updated.Finalizers, csivolumeinfov1alpha1.VolumeProtectionFinalizer)
	assert.Equal(t, csivolumeinfov1alpha1.OwnershipStateVMManaged, updated.Status.Ownership)
	assert.Equal(t, csivolumeinfov1alpha1.PhaseSucceeded, updated.Status.Phase)
	// observedGeneration must match the live generation (including any bump from the
	// controller's own diskPath/diskUUID spec write) so the green signal is satisfiable.
	assert.Equal(t, updated.Generation, updated.Status.ObservedGeneration)
	assert.Empty(t, updated.Status.Error)
}

// TestReconcile_UnregisterObservedGenerationTracksSpecWrite verifies that the
// unregister path records observedGeneration from the generation returned by the
// spec patch (which the API server bumps), not the generation observed at reconcile
// entry.  Without this, the controller's own diskPath/diskUUID write would leave
// observedGeneration permanently behind generation and the green signal would never
// be satisfied.
func TestReconcile_UnregisterObservedGenerationTracksSpecWrite(t *testing.T) {
	const volID = "vol-gen-track"
	const bumpedGen = int64(5)
	vms := []csivolumeinfov1alpha1.VirtualMachineRef{{VMName: "vm-a"}}
	// Entry generation is 4; the simulated spec write reports generation 5.
	cvi := newCVI(volID, vms, "", "", "", 4, nil)

	s := newScheme(t)
	c := newFakeClient(t, s, []client.Object{cvi, newTestPV("test-pv")}, interceptor.Funcs{})

	r := &Reconciler{
		client:        c,
		scheme:        s,
		configInfo:    minimalConfigInfo(),
		volumeManager: &testVolumeManager{},
		cviSvc: &genBumpingCviService{
			CsiVolumeInfoService: newFakeCviService(c),
			bumpedGen:            bumpedGen,
		},
	}
	backOffDuration = make(map[k8stypes.NamespacedName]time.Duration)

	res, err := r.Reconcile(context.Background(), makeRequest(volID))
	require.NoError(t, err)
	assert.True(t, res.IsZero())

	updated := &csivolumeinfov1alpha1.CsiVolumeInfo{}
	require.NoError(t, c.Get(context.Background(), k8stypes.NamespacedName{
		Namespace: csivolumeinfov1alpha1.CVINamespace,
		Name:      csivolumeinfosvc.GetCsiVolumeInfoCRName(volID),
	}, updated))
	assert.Equal(t, bumpedGen, updated.Status.ObservedGeneration,
		"observedGeneration must follow the post-spec-write generation")
}

// TestReconcile_BrownfieldLazy verifies that a CVI with spec.vms set but
// ownership="" (brownfield/lazy) is treated identically to CSIManaged and
// triggers reconcileUnregister.
func TestReconcile_BrownfieldLazy(t *testing.T) {
	const volID = "vol-brownfield"
	vms := []csivolumeinfov1alpha1.VirtualMachineRef{{VMName: "vm-a"}}
	// ownership="" is the brownfield case
	cvi := newCVI(volID, vms, "", "", "", 1, nil)

	s := newScheme(t)
	c := newFakeClient(t, s, []client.Object{cvi, newTestPV("test-pv")}, interceptor.Funcs{})

	called := false
	mgr := &testVolumeManager{
		unregisterVolumeExFn: func(_ context.Context, _ string) (string, string, error) {
			called = true
			return "path", "uuid", nil
		},
	}

	r := &Reconciler{
		client:        c,
		scheme:        s,
		configInfo:    minimalConfigInfo(),
		volumeManager: mgr,
		cviSvc:        newFakeCviService(c),
	}
	backOffDuration = make(map[k8stypes.NamespacedName]time.Duration)

	res, err := r.Reconcile(context.Background(), makeRequest(volID))
	require.NoError(t, err)
	assert.True(t, res.IsZero())
	assert.True(t, called, "UnregisterVolumeEx must be called for brownfield-lazy CVI")
}

// TestReconcile_RegisterTransition verifies the full re-register path:
// spec.vms=[], ownership=VMManaged → CreateVolume called, status.ownership=CSIManaged,
// phase=Succeeded, protection finalizer removed.
func TestReconcile_RegisterTransition(t *testing.T) {
	const volID = "vol-register"
	cvi := newCVI(volID, nil, csivolumeinfov1alpha1.OwnershipStateVMManaged,
		"/vmfs/volumes/ds/disk.vmdk", "disk-uuid-123", 2,
		[]string{csivolumeinfov1alpha1.VolumeProtectionFinalizer})

	pv := newTestPV("test-pv")
	pvc := newTestPVC("test-pvc", "test-ns")

	s := newScheme(t)
	c := newFakeClient(t, s, []client.Object{cvi, pv, pvc}, interceptor.Funcs{})

	createCalled := false
	mgr := &testVolumeManager{
		createVolumeFn: func(_ context.Context, spec *cnstypes.CnsVolumeCreateSpec,
			_ interface{}) (*cnsvolumes.CnsVolumeInfo, string, error) {
			createCalled = true
			return &cnsvolumes.CnsVolumeInfo{}, "", nil
		},
	}

	r := &Reconciler{
		client:        c,
		scheme:        s,
		configInfo:    minimalConfigInfo(),
		volumeManager: mgr,
		cviSvc:        newFakeCviService(c),
	}
	backOffDuration = make(map[k8stypes.NamespacedName]time.Duration)

	res, err := r.Reconcile(context.Background(), makeRequest(volID))
	require.NoError(t, err)
	assert.True(t, res.IsZero())
	assert.True(t, createCalled, "CreateVolume must be called for register transition")

	updated := &csivolumeinfov1alpha1.CsiVolumeInfo{}
	require.NoError(t, c.Get(context.Background(), k8stypes.NamespacedName{
		Namespace: csivolumeinfov1alpha1.CVINamespace,
		Name:      csivolumeinfosvc.GetCsiVolumeInfoCRName(volID),
	}, updated))

	assert.Equal(t, csivolumeinfov1alpha1.OwnershipStateCSIManaged, updated.Status.Ownership)
	assert.Equal(t, csivolumeinfov1alpha1.PhaseSucceeded, updated.Status.Phase)
	assert.Equal(t, int64(2), updated.Status.ObservedGeneration)
	assert.NotContains(t, updated.Finalizers, csivolumeinfov1alpha1.VolumeProtectionFinalizer)
}

// TestReconcile_RegisterAlreadyExists verifies that a CnsVolumeAlreadyExistsFault
// from CreateVolume is treated as success (idempotent re-register).
func TestReconcile_RegisterAlreadyExists(t *testing.T) {
	const volID = "vol-already-exists"
	cvi := newCVI(volID, nil, csivolumeinfov1alpha1.OwnershipStateVMManaged,
		"/vmfs/volumes/ds/disk.vmdk", "disk-uuid", 1,
		[]string{csivolumeinfov1alpha1.VolumeProtectionFinalizer})

	pv := newTestPV("test-pv")
	pvc := newTestPVC("test-pvc", "test-ns")

	s := newScheme(t)
	c := newFakeClient(t, s, []client.Object{cvi, pv, pvc}, interceptor.Funcs{})

	mgr := &testVolumeManager{
		createVolumeFn: func(_ context.Context, _ *cnstypes.CnsVolumeCreateSpec,
			_ interface{}) (*cnsvolumes.CnsVolumeInfo, string, error) {
			// Simulate CnsVolumeAlreadyExistsFault
			return nil, "vim.fault.CnsVolumeAlreadyExistsFault",
				errors.New("volume already exists")
		},
	}

	r := &Reconciler{
		client:        c,
		scheme:        s,
		configInfo:    minimalConfigInfo(),
		volumeManager: mgr,
		cviSvc:        newFakeCviService(c),
	}
	backOffDuration = make(map[k8stypes.NamespacedName]time.Duration)

	res, err := r.Reconcile(context.Background(), makeRequest(volID))
	require.NoError(t, err)
	assert.True(t, res.IsZero())

	updated := &csivolumeinfov1alpha1.CsiVolumeInfo{}
	require.NoError(t, c.Get(context.Background(), k8stypes.NamespacedName{
		Namespace: csivolumeinfov1alpha1.CVINamespace,
		Name:      csivolumeinfosvc.GetCsiVolumeInfoCRName(volID),
	}, updated))
	assert.Equal(t, csivolumeinfov1alpha1.OwnershipStateCSIManaged, updated.Status.Ownership)
	assert.Equal(t, csivolumeinfov1alpha1.PhaseSucceeded, updated.Status.Phase)
}

// TestReconcile_IdleVMsPresent_VMManaged verifies that a CVI with VMs and
// ownership=VMManaged is idle (the VM already owns the disk, nothing to do).
func TestReconcile_IdleVMsPresent_VMManaged(t *testing.T) {
	const volID = "vol-idle-vm"
	vms := []csivolumeinfov1alpha1.VirtualMachineRef{{VMName: "vm-a"}, {VMName: "vm-b"}}
	cvi := newCVI(volID, vms, csivolumeinfov1alpha1.OwnershipStateVMManaged, "", "", 1, nil)

	s := newScheme(t)
	c := newFakeClient(t, s, []client.Object{cvi, newTestPV("test-pv")}, interceptor.Funcs{})

	callCount := 0
	mgr := &testVolumeManager{
		unregisterVolumeExFn: func(_ context.Context, _ string) (string, string, error) {
			callCount++
			return "", "", nil
		},
	}

	r := &Reconciler{
		client:        c,
		scheme:        s,
		configInfo:    minimalConfigInfo(),
		volumeManager: mgr,
		cviSvc:        newFakeCviService(c),
	}
	backOffDuration = make(map[k8stypes.NamespacedName]time.Duration)

	res, err := r.Reconcile(context.Background(), makeRequest(volID))
	require.NoError(t, err)
	assert.True(t, res.IsZero())
	assert.Equal(t, 0, callCount, "UnregisterVolumeEx must NOT be called for idle CVI")
}

// TestReconcile_IdleNoVMs_CSIManaged verifies that a CVI with no VMs and
// ownership=CSIManaged is idle (already in CSI-managed steady state).
func TestReconcile_IdleNoVMs_CSIManaged(t *testing.T) {
	const volID = "vol-idle-csi"
	cvi := newCVI(volID, nil, csivolumeinfov1alpha1.OwnershipStateCSIManaged, "", "", 1, nil)

	s := newScheme(t)
	c := newFakeClient(t, s, []client.Object{cvi, newTestPV("test-pv")}, interceptor.Funcs{})

	createCalled := false
	mgr := &testVolumeManager{
		createVolumeFn: func(_ context.Context, _ *cnstypes.CnsVolumeCreateSpec,
			_ interface{}) (*cnsvolumes.CnsVolumeInfo, string, error) {
			createCalled = true
			return &cnsvolumes.CnsVolumeInfo{}, "", nil
		},
	}

	r := &Reconciler{
		client:        c,
		scheme:        s,
		configInfo:    minimalConfigInfo(),
		volumeManager: mgr,
		cviSvc:        newFakeCviService(c),
	}
	backOffDuration = make(map[k8stypes.NamespacedName]time.Duration)

	res, err := r.Reconcile(context.Background(), makeRequest(volID))
	require.NoError(t, err)
	assert.True(t, res.IsZero())
	assert.False(t, createCalled, "CreateVolume must NOT be called for idle CVI")
}

// TestReconcile_UnregisterFault verifies that a CNS fault during UnregisterVolumeEx
// sets status.phase=Failed with an error message and conditions[Ready=False].
func TestReconcile_UnregisterFault(t *testing.T) {
	const volID = "vol-unregister-fault"
	vms := []csivolumeinfov1alpha1.VirtualMachineRef{{VMName: "vm-a"}}
	cvi := newCVI(volID, vms, csivolumeinfov1alpha1.OwnershipStateCSIManaged, "", "", 5, nil)

	s := newScheme(t)
	c := newFakeClient(t, s, []client.Object{cvi, newTestPV("test-pv")}, interceptor.Funcs{})

	mgr := &testVolumeManager{
		unregisterVolumeExFn: func(_ context.Context, _ string) (string, string, error) {
			return "", "", errors.New("CNS unregister fault")
		},
	}

	r := &Reconciler{
		client:        c,
		scheme:        s,
		configInfo:    minimalConfigInfo(),
		volumeManager: mgr,
		cviSvc:        newFakeCviService(c),
	}
	backOffDuration = make(map[k8stypes.NamespacedName]time.Duration)

	res, err := r.Reconcile(context.Background(), makeRequest(volID))
	require.NoError(t, err) // reconciler swallows error into status, returns nil+requeue
	assert.Greater(t, res.RequeueAfter, time.Duration(0), "should requeue after backoff")

	updated := &csivolumeinfov1alpha1.CsiVolumeInfo{}
	require.NoError(t, c.Get(context.Background(), k8stypes.NamespacedName{
		Namespace: csivolumeinfov1alpha1.CVINamespace,
		Name:      csivolumeinfosvc.GetCsiVolumeInfoCRName(volID),
	}, updated))
	assert.Equal(t, csivolumeinfov1alpha1.PhaseFailed, updated.Status.Phase)
	assert.NotEmpty(t, updated.Status.Error)
	assert.Equal(t, int64(5), updated.Status.ObservedGeneration)
	require.NotEmpty(t, updated.Status.Conditions)
	assert.Equal(t, conditionTypeReady, updated.Status.Conditions[0].Type)
	assert.Equal(t, string(metav1.ConditionFalse), string(updated.Status.Conditions[0].Status))
}

// TestReconcile_RegisterFault verifies that a CNS fault during CreateVolume
// sets status.phase=Failed and requeueing with backoff.
func TestReconcile_RegisterFault(t *testing.T) {
	const volID = "vol-register-fault"
	cvi := newCVI(volID, nil, csivolumeinfov1alpha1.OwnershipStateVMManaged,
		"/ds/disk.vmdk", "uuid", 2, []string{csivolumeinfov1alpha1.VolumeProtectionFinalizer})

	pv := newTestPV("test-pv")
	pvc := newTestPVC("test-pvc", "test-ns")

	s := newScheme(t)
	c := newFakeClient(t, s, []client.Object{cvi, pv, pvc}, interceptor.Funcs{})

	mgr := &testVolumeManager{
		createVolumeFn: func(_ context.Context, _ *cnstypes.CnsVolumeCreateSpec,
			_ interface{}) (*cnsvolumes.CnsVolumeInfo, string, error) {
			return nil, "vim.fault.CnsFault", errors.New("create volume fault")
		},
	}

	r := &Reconciler{
		client:        c,
		scheme:        s,
		configInfo:    minimalConfigInfo(),
		volumeManager: mgr,
		cviSvc:        newFakeCviService(c),
	}
	backOffDuration = make(map[k8stypes.NamespacedName]time.Duration)

	res, err := r.Reconcile(context.Background(), makeRequest(volID))
	require.NoError(t, err)
	assert.Greater(t, res.RequeueAfter, time.Duration(0))

	updated := &csivolumeinfov1alpha1.CsiVolumeInfo{}
	require.NoError(t, c.Get(context.Background(), k8stypes.NamespacedName{
		Namespace: csivolumeinfov1alpha1.CVINamespace,
		Name:      csivolumeinfosvc.GetCsiVolumeInfoCRName(volID),
	}, updated))
	assert.Equal(t, csivolumeinfov1alpha1.PhaseFailed, updated.Status.Phase)
	assert.Equal(t, int64(2), updated.Status.ObservedGeneration)
}

// TestReconcile_ObservedGenerationAlwaysSet verifies that observedGeneration is
// written to status on every reconcile path (success and failure).
func TestReconcile_ObservedGenerationAlwaysSet(t *testing.T) {
	tests := []struct {
		name      string
		ownership csivolumeinfov1alpha1.OwnershipState
		vms       []csivolumeinfov1alpha1.VirtualMachineRef
		gen       int64
		fail      bool
	}{
		{
			name: "unregister success",
			vms:  []csivolumeinfov1alpha1.VirtualMachineRef{{VMName: "vm-a"}},
			gen:  7,
			fail: false,
		},
		{
			name: "unregister failure",
			vms:  []csivolumeinfov1alpha1.VirtualMachineRef{{VMName: "vm-a"}},
			gen:  8,
			fail: true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			volID := "vol-gen-" + tc.name
			cvi := newCVI(volID, tc.vms, tc.ownership, "", "", tc.gen, nil)

			s := newScheme(t)
			c := newFakeClient(t, s, []client.Object{cvi, newTestPV("test-pv")}, interceptor.Funcs{})

			mgr := &testVolumeManager{
				unregisterVolumeExFn: func(_ context.Context, _ string) (string, string, error) {
					if tc.fail {
						return "", "", errors.New("forced error")
					}
					return "path", "uuid", nil
				},
			}
			r := &Reconciler{
				client:        c,
				scheme:        s,
				configInfo:    minimalConfigInfo(),
				volumeManager: mgr,
				cviSvc:        newFakeCviService(c),
			}
			backOffDuration = make(map[k8stypes.NamespacedName]time.Duration)

			r.Reconcile(context.Background(), makeRequest(volID)) //nolint:errcheck

			updated := &csivolumeinfov1alpha1.CsiVolumeInfo{}
			require.NoError(t, c.Get(context.Background(), k8stypes.NamespacedName{
				Namespace: csivolumeinfov1alpha1.CVINamespace,
				Name:      csivolumeinfosvc.GetCsiVolumeInfoCRName(volID),
			}, updated))
			assert.Equal(t, updated.Generation, updated.Status.ObservedGeneration,
				"observedGeneration must equal the live spec.generation")
		})
	}
}

// ---------------------------------------------------------------------------
// buildStatusPatch unit test
// ---------------------------------------------------------------------------

// TestBuildStatusPatch verifies that buildStatusPatch produces a valid JSON
// merge-patch with all required fields.
func TestBuildStatusPatch(t *testing.T) {
	patch := buildStatusPatch(42,
		csivolumeinfov1alpha1.OwnershipStateVMManaged,
		csivolumeinfov1alpha1.PhaseSucceeded,
		"", reasonUnregisterSucceeded, true)
	require.NotNil(t, patch)

	var m map[string]interface{}
	require.NoError(t, json.Unmarshal(patch, &m))

	status, ok := m["status"].(map[string]interface{})
	require.True(t, ok)
	assert.Equal(t, string(csivolumeinfov1alpha1.OwnershipStateVMManaged), status["ownership"])
	assert.Equal(t, string(csivolumeinfov1alpha1.PhaseSucceeded), status["phase"])
	assert.Equal(t, float64(42), status["observedGeneration"])

	conds, ok := status["conditions"].([]interface{})
	require.True(t, ok)
	require.Len(t, conds, 1)
	cond := conds[0].(map[string]interface{})
	assert.Equal(t, conditionTypeReady, cond["type"])
	assert.Equal(t, string(metav1.ConditionTrue), cond["status"])
	assert.Equal(t, reasonUnregisterSucceeded, cond["reason"])
}

// ---------------------------------------------------------------------------
// resolveStoragePolicyID unit tests
// ---------------------------------------------------------------------------

func TestResolveStoragePolicyID_FromPVAttributes(t *testing.T) {
	pv := newTestPV("test-pv")
	s := newScheme(t)
	c := newFakeClient(t, s, nil, interceptor.Funcs{})

	id := resolveStoragePolicyID(context.Background(), c, pv)
	assert.Equal(t, "test-policy", id)
}

func TestResolveStoragePolicyID_FromStorageClass(t *testing.T) {
	pv := &corev1.PersistentVolume{
		ObjectMeta: metav1.ObjectMeta{Name: "test-pv"},
		Spec: corev1.PersistentVolumeSpec{
			StorageClassName: "my-sc",
			PersistentVolumeSource: corev1.PersistentVolumeSource{
				CSI: &corev1.CSIPersistentVolumeSource{
					// No storagePolicyID in volumeAttributes
					VolumeAttributes: map[string]string{},
				},
			},
		},
	}
	sc := &storagev1.StorageClass{
		ObjectMeta: metav1.ObjectMeta{Name: "my-sc"},
		Parameters: map[string]string{"storagePolicyID": "sc-policy-id"},
	}

	s := newScheme(t)
	c := newFakeClient(t, s, []client.Object{sc}, interceptor.Funcs{})

	id := resolveStoragePolicyID(context.Background(), c, pv)
	assert.Equal(t, "sc-policy-id", id)
}

func TestResolveStoragePolicyID_NotFound(t *testing.T) {
	pv := &corev1.PersistentVolume{
		ObjectMeta: metav1.ObjectMeta{Name: "test-pv"},
		Spec: corev1.PersistentVolumeSpec{
			StorageClassName: "missing-sc",
			PersistentVolumeSource: corev1.PersistentVolumeSource{
				CSI: &corev1.CSIPersistentVolumeSource{
					VolumeAttributes: map[string]string{},
				},
			},
		},
	}

	s := newScheme(t)
	c := newFakeClient(t, s, nil, interceptor.Funcs{})

	id := resolveStoragePolicyID(context.Background(), c, pv)
	assert.Empty(t, id)
}

// ---------------------------------------------------------------------------
// ensurePVOwnerRef unit tests
// ---------------------------------------------------------------------------

// TestEnsurePVOwnerRef_SetsOwnerRef verifies that when the CVI has no PV
// ownerReference, ensurePVOwnerRef patches it with blockOwnerDeletion=true.
func TestEnsurePVOwnerRef_SetsOwnerRef(t *testing.T) {
	const volID = "vol-ownerref-set"
	cvi := newCVI(volID, nil, "", "", "", 1, nil)

	pv := &corev1.PersistentVolume{
		ObjectMeta: metav1.ObjectMeta{
			Name: "test-pv",
			UID:  k8stypes.UID("pv-uid-abc123"),
		},
	}

	s := newScheme(t)
	c := newFakeClient(t, s, []client.Object{cvi, pv}, interceptor.Funcs{})
	r := &Reconciler{client: c, scheme: s}

	err := r.ensurePVOwnerRef(context.Background(), cvi)
	require.NoError(t, err)

	// Reload CVI and verify the ownerRef was written.
	got := &csivolumeinfov1alpha1.CsiVolumeInfo{}
	require.NoError(t, c.Get(context.Background(), k8stypes.NamespacedName{
		Namespace: csivolumeinfov1alpha1.CVINamespace,
		Name:      cvi.Name,
	}, got))

	require.Len(t, got.OwnerReferences, 1)
	ref := got.OwnerReferences[0]
	assert.Equal(t, "v1", ref.APIVersion)
	assert.Equal(t, "PersistentVolume", ref.Kind)
	assert.Equal(t, "test-pv", ref.Name)
	assert.Equal(t, k8stypes.UID("pv-uid-abc123"), ref.UID)
	require.NotNil(t, ref.BlockOwnerDeletion)
	assert.True(t, *ref.BlockOwnerDeletion)
	assert.Nil(t, ref.Controller, "controller should not be set — PV is owner for GC, not managing controller")
}

// TestEnsurePVOwnerRef_AlreadySet verifies that ensurePVOwnerRef is a no-op
// when the ownerReference is already present and no Patch call is made.
func TestEnsurePVOwnerRef_AlreadySet(t *testing.T) {
	const volID = "vol-ownerref-already"
	cvi := newCVI(volID, nil, "", "", "", 1, nil)
	cvi.OwnerReferences = []metav1.OwnerReference{
		{APIVersion: "v1", Kind: "PersistentVolume", Name: "test-pv", UID: "existing-uid"},
	}

	s := newScheme(t)
	patchCalled := false
	c := newFakeClient(t, s, []client.Object{cvi}, interceptor.Funcs{
		Patch: func(ctx context.Context, cl client.WithWatch, obj client.Object,
			patch client.Patch, opts ...client.PatchOption) error {
			patchCalled = true
			return nil
		},
	})
	r := &Reconciler{client: c, scheme: s}

	err := r.ensurePVOwnerRef(context.Background(), cvi)
	require.NoError(t, err)
	assert.False(t, patchCalled, "Patch must not be called when ownerRef is already set")
}

// TestEnsurePVOwnerRef_PVNotFound verifies that a missing PV returns an error
// so controller-runtime requeues the reconcile.
func TestEnsurePVOwnerRef_PVNotFound(t *testing.T) {
	const volID = "vol-ownerref-nopv"
	cvi := newCVI(volID, nil, "", "", "", 1, nil)

	s := newScheme(t)
	c := newFakeClient(t, s, []client.Object{cvi}, interceptor.Funcs{})
	r := &Reconciler{client: c, scheme: s}

	err := r.ensurePVOwnerRef(context.Background(), cvi)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "not found")
}

// TestEnsurePVOwnerRef_EmptyPVName verifies that a CVI with no spec.pvName
// is silently skipped.
func TestEnsurePVOwnerRef_EmptyPVName(t *testing.T) {
	const volID = "vol-ownerref-nopvname"
	cvi := newCVI(volID, nil, "", "", "", 1, nil)
	cvi.Spec.PVName = ""

	s := newScheme(t)
	patchCalled := false
	c := newFakeClient(t, s, []client.Object{cvi}, interceptor.Funcs{
		Patch: func(ctx context.Context, cl client.WithWatch, obj client.Object,
			patch client.Patch, opts ...client.PatchOption) error {
			patchCalled = true
			return nil
		},
	})
	r := &Reconciler{client: c, scheme: s}

	err := r.ensurePVOwnerRef(context.Background(), cvi)
	require.NoError(t, err)
	assert.False(t, patchCalled, "Patch must not be called when pvName is empty")
}

// TestReconcile_SetsOwnerRefOnInitialState verifies that a full Reconcile on a
// freshly-created CVI (no VMs, no ownership) sets the PV ownerRef and writes
// the initial CSIManaged status.
func TestReconcile_SetsOwnerRefOnInitialState(t *testing.T) {
	const volID = "vol-ownerref-initial"
	cvi := newCVI(volID, nil, "", "", "", 1, nil)

	pv := &corev1.PersistentVolume{
		ObjectMeta: metav1.ObjectMeta{
			Name: "test-pv",
			UID:  k8stypes.UID("pv-uid-initial"),
		},
	}

	s := newScheme(t)
	c := newFakeClient(t, s, []client.Object{cvi, pv}, interceptor.Funcs{})
	r := &Reconciler{
		client:        c,
		scheme:        s,
		configInfo:    minimalConfigInfo(),
		volumeManager: &testVolumeManager{},
		cviSvc:        newFakeCviService(c),
	}
	backOffDuration = make(map[k8stypes.NamespacedName]time.Duration)

	res, err := r.Reconcile(context.Background(), makeRequest(volID))
	require.NoError(t, err)
	assert.True(t, res.IsZero())

	got := &csivolumeinfov1alpha1.CsiVolumeInfo{}
	require.NoError(t, c.Get(context.Background(), k8stypes.NamespacedName{
		Namespace: csivolumeinfov1alpha1.CVINamespace,
		Name:      csivolumeinfosvc.GetCsiVolumeInfoCRName(volID),
	}, got))

	// ownerRef must be set.
	require.Len(t, got.OwnerReferences, 1)
	assert.Equal(t, k8stypes.UID("pv-uid-initial"), got.OwnerReferences[0].UID)
}

// ---------------------------------------------------------------------------
// Backoff helper unit tests
// ---------------------------------------------------------------------------

func TestBackoffHelpers(t *testing.T) {
	backOffDuration = make(map[k8stypes.NamespacedName]time.Duration)
	ctx := context.Background()
	nn := k8stypes.NamespacedName{Name: "test"}

	// Initial backoff should be 1s.
	d := getBackoffDuration(ctx, nn)
	assert.Equal(t, time.Second, d)

	// Double it.
	doubleBackoffDuration(ctx, nn)
	d = getBackoffDuration(ctx, nn)
	assert.Equal(t, 2*time.Second, d)

	// Delete it.
	deleteBackoffEntry(ctx, nn)
	assert.NotContains(t, backOffDuration, nn)
}
