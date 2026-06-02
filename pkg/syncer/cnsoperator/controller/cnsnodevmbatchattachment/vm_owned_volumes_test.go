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

package cnsnodevmbatchattachment

import (
	"context"
	"errors"
	"testing"

	vmoperatortypes "github.com/vmware-tanzu/vm-operator/api/v1alpha2"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	k8stypes "k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	bav1alpha1 "sigs.k8s.io/vsphere-csi-driver/v3/pkg/apis/cnsoperator/cnsnodevmbatchattachment/v1alpha1"
	csivolumeinfov1alpha1 "sigs.k8s.io/vsphere-csi-driver/v3/pkg/apis/cnsoperator/csivolumeinfo/v1alpha1"
)

// ---------------------------------------------------------------------------
// Fake CsiVolumeInfoService
// ---------------------------------------------------------------------------

// fakeCVISvc is a minimal in-memory fake of CsiVolumeInfoService for unit tests.
type fakeCVISvc struct {
	cviBydiskUUID map[string]*csivolumeinfov1alpha1.CsiVolumeInfo // diskUUID → CVI
	cviByVolID    map[string]*csivolumeinfov1alpha1.CsiVolumeInfo // volumeID → CVI
	cviByPVCName  map[string]*csivolumeinfov1alpha1.CsiVolumeInfo // pvcName → CVI
	addFinErr     error
	removeFinErr  error
	updateStatErr error
}

func (f *fakeCVISvc) CreateCsiVolumeInfo(_ context.Context, cvi *csivolumeinfov1alpha1.CsiVolumeInfo) error {
	f.cviByVolID[cvi.Spec.VolumeID] = cvi
	return nil
}

func (f *fakeCVISvc) GetCsiVolumeInfo(
	_ context.Context, _, volumeID string) (*csivolumeinfov1alpha1.CsiVolumeInfo, error) {
	return f.cviByVolID[volumeID], nil
}

func (f *fakeCVISvc) GetCsiVolumeInfoByDiskUUID(
	_ context.Context, _, diskUUID string) (*csivolumeinfov1alpha1.CsiVolumeInfo, error) {
	return f.cviBydiskUUID[diskUUID], nil
}

func (f *fakeCVISvc) UpdateCsiVolumeInfoStatus(_ context.Context, cvi *csivolumeinfov1alpha1.CsiVolumeInfo) error {
	if f.updateStatErr != nil {
		return f.updateStatErr
	}
	f.cviByVolID[cvi.Spec.VolumeID] = cvi
	if cvi.Status.DiskUUID != "" {
		f.cviBydiskUUID[cvi.Status.DiskUUID] = cvi
	}
	return nil
}

func (f *fakeCVISvc) PatchCsiVolumeInfo(_ context.Context, _, _ string, _ []byte) error {
	return nil
}

func (f *fakeCVISvc) PatchCsiVolumeInfoStatus(_ context.Context, _, _ string, _ []byte) error {
	return nil
}

func (f *fakeCVISvc) DeleteCsiVolumeInfo(_ context.Context, _, _ string) error {
	return nil
}

func (f *fakeCVISvc) CsiVolumeInfoExists(_ context.Context, _, _ string) (bool, error) {
	return false, nil
}

func (f *fakeCVISvc) GetCsiVolumeInfoByPVCName(
	_ context.Context, _, pvcName string) (*csivolumeinfov1alpha1.CsiVolumeInfo, error) {
	if f.cviByPVCName != nil {
		return f.cviByPVCName[pvcName], nil
	}
	return nil, nil
}

func (f *fakeCVISvc) AddCVIProtectionFinalizer(_ context.Context, _, _ string) error {
	return f.addFinErr
}

func (f *fakeCVISvc) RemoveCVIProtectionFinalizer(_ context.Context, _, _ string) error {
	return f.removeFinErr
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

func makeVMScheme() *runtime.Scheme {
	s := runtime.NewScheme()
	_ = vmoperatortypes.AddToScheme(s)
	_ = corev1.AddToScheme(s)
	return s
}

func makeVM(namespace, name, instanceUUID string, annotations map[string]string) *vmoperatortypes.VirtualMachine {
	return &vmoperatortypes.VirtualMachine{
		ObjectMeta: metav1.ObjectMeta{
			Name:        name,
			Namespace:   namespace,
			Annotations: annotations,
		},
		Status: vmoperatortypes.VirtualMachineStatus{
			InstanceUUID: instanceUUID,
		},
	}
}

func makePVC(namespace, name string, labels map[string]string) *corev1.PersistentVolumeClaim {
	return &corev1.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: namespace,
			Labels:    labels,
		},
	}
}

func makeCVI(namespace, volumeID, pvcName, diskUUID string,
	state csivolumeinfov1alpha1.OwnershipState) *csivolumeinfov1alpha1.CsiVolumeInfo {
	return &csivolumeinfov1alpha1.CsiVolumeInfo{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "csi-volume-info-" + volumeID,
			Namespace: namespace,
		},
		Spec: csivolumeinfov1alpha1.CsiVolumeInfoSpec{
			VolumeID: volumeID,
			PVCName:  pvcName,
		},
		Status: csivolumeinfov1alpha1.CsiVolumeInfoStatus{
			OwnershipState: state,
			DiskUUID:       diskUUID,
			DiskPath:       "[ds] vm/" + diskUUID + ".vmdk",
		},
	}
}

// ---------------------------------------------------------------------------
// IsVMOwnedVolumesVM tests
// ---------------------------------------------------------------------------

func TestIsVMOwnedVolumesVM_WithAnnotation(t *testing.T) {
	ctx := context.Background()
	vm := makeVM("ns", "vm1", "uuid1", map[string]string{
		VMOwnedVolumesAnnotation: "true",
	})
	s := makeVMScheme()
	c := fake.NewClientBuilder().WithScheme(s).WithObjects(vm).Build()

	got, err := IsVMOwnedVolumesVM(ctx, c, "ns", "vm1")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !got {
		t.Error("expected true for VM with VMOwnedVolumes annotation")
	}
}

func TestIsVMOwnedVolumesVM_WithoutAnnotation(t *testing.T) {
	ctx := context.Background()
	vm := makeVM("ns", "vm1", "uuid1", nil)
	s := makeVMScheme()
	c := fake.NewClientBuilder().WithScheme(s).WithObjects(vm).Build()

	got, err := IsVMOwnedVolumesVM(ctx, c, "ns", "vm1")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if got {
		t.Error("expected false for VM without annotation")
	}
}

func TestIsVMOwnedVolumesVM_AnnotationWrongValue(t *testing.T) {
	ctx := context.Background()
	vm := makeVM("ns", "vm1", "uuid1", map[string]string{
		VMOwnedVolumesAnnotation: "false",
	})
	s := makeVMScheme()
	c := fake.NewClientBuilder().WithScheme(s).WithObjects(vm).Build()

	got, err := IsVMOwnedVolumesVM(ctx, c, "ns", "vm1")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if got {
		t.Error("expected false for VM with annotation value 'false'")
	}
}

func TestIsVMOwnedVolumesVM_VMNotFound(t *testing.T) {
	ctx := context.Background()
	s := makeVMScheme()
	c := fake.NewClientBuilder().WithScheme(s).Build()

	got, err := IsVMOwnedVolumesVM(ctx, c, "ns", "nonexistent")
	if err != nil {
		t.Fatalf("expected nil error for NotFound, got: %v", err)
	}
	if got {
		t.Error("expected false for absent VM")
	}
}

// ---------------------------------------------------------------------------
// PatchPVCOwnershipLabel tests
// ---------------------------------------------------------------------------

func TestPatchPVCOwnershipLabel_SetsLabel(t *testing.T) {
	ctx := context.Background()
	s := makeVMScheme()
	pvc := makePVC("ns", "pvc1", nil)
	c := fake.NewClientBuilder().WithScheme(s).WithObjects(pvc).Build()

	if err := PatchPVCOwnershipLabel(ctx, c, "ns", "pvc1",
		csivolumeinfov1alpha1.OwnershipLabelVMOwned); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	got := &corev1.PersistentVolumeClaim{}
	if err := c.Get(ctx, k8stypes.NamespacedName{Namespace: "ns", Name: "pvc1"}, got); err != nil {
		t.Fatalf("could not get PVC: %v", err)
	}
	if got.Labels[csivolumeinfov1alpha1.LabelVolumeOwnership] != csivolumeinfov1alpha1.OwnershipLabelVMOwned {
		t.Errorf("label not set: got %q", got.Labels[csivolumeinfov1alpha1.LabelVolumeOwnership])
	}
}

func TestPatchPVCOwnershipLabel_Idempotent(t *testing.T) {
	ctx := context.Background()
	s := makeVMScheme()
	pvc := makePVC("ns", "pvc1", map[string]string{
		csivolumeinfov1alpha1.LabelVolumeOwnership: csivolumeinfov1alpha1.OwnershipLabelVMOwned,
	})
	c := fake.NewClientBuilder().WithScheme(s).WithObjects(pvc).Build()

	if err := PatchPVCOwnershipLabel(ctx, c, "ns", "pvc1",
		csivolumeinfov1alpha1.OwnershipLabelVMOwned); err != nil {
		t.Fatalf("unexpected error on idempotent call: %v", err)
	}
}

func TestPatchPVCOwnershipLabel_PVCNotFound(t *testing.T) {
	ctx := context.Background()
	s := makeVMScheme()
	c := fake.NewClientBuilder().WithScheme(s).Build()

	if err := PatchPVCOwnershipLabel(ctx, c, "ns", "missing",
		csivolumeinfov1alpha1.OwnershipLabelCSIOwned); err != nil {
		t.Fatalf("unexpected error for missing PVC: %v", err)
	}
}

// ---------------------------------------------------------------------------
// processVMOwnedVolumesAttach tests
// ---------------------------------------------------------------------------

// fakeUnregisterManager satisfies the volumeManagerForAttach interface.
type fakeUnregisterManager struct {
	unregisterErr    error
	unregisterCalled bool
	ackErr           error
	ackCalled        bool
	// returnDiskPath and returnDiskUUID are returned by UnregisterVolumeEx.
	returnDiskPath string
	returnDiskUUID string
}

func (f *fakeUnregisterManager) UnregisterVolumeEx(_ context.Context, _ string) (string, string, error) {
	f.unregisterCalled = true
	return f.returnDiskPath, f.returnDiskUUID, f.unregisterErr
}

func (f *fakeUnregisterManager) AckUnregister(_ context.Context, _ string) error {
	f.ackCalled = true
	return f.ackErr
}

func TestProcessVMOwnedVolumesAttach_Success(t *testing.T) {
	ctx := context.Background()
	const (
		ns          = "ns"
		vmName      = "vm1"
		instanceID  = "instance-1"
		pvcName     = "pvc-1"
		volumeID    = "vol-1"
		volName     = "disk-1"
		diskUUID    = "uuid-disk-1"
		diskPath    = "[ds] foo.vmdk"
		cnsDiskPath = "[ds] foo-fresh.vmdk"
		cnsDiskUUID = "uuid-disk-1"
	)

	cvi := makeCVI(ns, volumeID, pvcName, diskUUID, csivolumeinfov1alpha1.OwnershipStateCSIManaged)
	cvi.Status.DiskPath = diskPath

	svc := &fakeCVISvc{
		cviByVolID:    map[string]*csivolumeinfov1alpha1.CsiVolumeInfo{volumeID: cvi},
		cviBydiskUUID: map[string]*csivolumeinfov1alpha1.CsiVolumeInfo{},
	}
	// UnregisterVolumeEx returns fresh diskPath and diskUUID from CNS.
	mgr := &fakeUnregisterManager{returnDiskPath: cnsDiskPath, returnDiskUUID: cnsDiskUUID}

	s := makeVMScheme()
	c := fake.NewClientBuilder().WithScheme(s).Build()

	instance := &bav1alpha1.CnsNodeVMBatchAttachment{
		ObjectMeta: metav1.ObjectMeta{Name: "ba1", Namespace: ns},
		Spec:       bav1alpha1.CnsNodeVMBatchAttachmentSpec{InstanceUUID: instanceID},
	}

	if err := processVMOwnedVolumesAttach(ctx, svc, c, mgr, instance,
		vmName, instanceID, pvcName, volumeID, volName); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !mgr.unregisterCalled {
		t.Error("expected UnregisterVolumeEx to be called")
	}
	if !mgr.ackCalled {
		t.Error("expected AckUnregister to be called")
	}
	updatedCVI := svc.cviByVolID[volumeID]
	if updatedCVI.Status.OwnershipState != csivolumeinfov1alpha1.OwnershipStateTransferringToVM {
		t.Errorf("expected TRANSFERRING_TO_VM, got %q", updatedCVI.Status.OwnershipState)
	}
	if updatedCVI.Status.VMName != vmName {
		t.Errorf("expected vmName=%q, got %q", vmName, updatedCVI.Status.VMName)
	}
	// diskPath should be refreshed to the CNS-returned value.
	if updatedCVI.Status.DiskPath != cnsDiskPath {
		t.Errorf("expected diskPath %q from CNS result, got %q", cnsDiskPath, updatedCVI.Status.DiskPath)
	}
}

func TestProcessVMOwnedVolumesAttach_AckFails_NonFatal(t *testing.T) {
	ctx := context.Background()
	const (
		ns       = "ns"
		volumeID = "vol-ack-fail"
		pvcName  = "pvc-ack-fail"
	)
	cvi := makeCVI(ns, volumeID, pvcName, "duuid", csivolumeinfov1alpha1.OwnershipStateCSIManaged)
	svc := &fakeCVISvc{
		cviByVolID:    map[string]*csivolumeinfov1alpha1.CsiVolumeInfo{volumeID: cvi},
		cviBydiskUUID: map[string]*csivolumeinfov1alpha1.CsiVolumeInfo{},
	}
	// ACK failure must not cause the overall attach to fail.
	mgr := &fakeUnregisterManager{
		returnDiskPath: "[ds] d.vmdk",
		returnDiskUUID: "duuid",
		ackErr:         errors.New("ack timeout"),
	}
	instance := &bav1alpha1.CnsNodeVMBatchAttachment{
		ObjectMeta: metav1.ObjectMeta{Name: "ba1", Namespace: ns},
	}
	c := fake.NewClientBuilder().WithScheme(makeVMScheme()).Build()

	err := processVMOwnedVolumesAttach(ctx, svc, c, mgr, instance,
		"vm1", "inst1", pvcName, volumeID, "disk1")

	if err != nil {
		t.Errorf("ACK failure must be non-fatal; got error: %v", err)
	}
	if !mgr.ackCalled {
		t.Error("expected AckUnregister to be called even on failure")
	}
	// CVI should still be transitioned.
	if svc.cviByVolID[volumeID].Status.OwnershipState != csivolumeinfov1alpha1.OwnershipStateTransferringToVM {
		t.Errorf("expected TRANSFERRING_TO_VM even when ACK fails")
	}
}

func TestProcessVMOwnedVolumesAttach_DiskUUIDRefreshedFromCNS(t *testing.T) {
	ctx := context.Background()
	const (
		ns               = "ns"
		volumeID         = "vol-uuid-refresh"
		pvcName          = "pvc-uuid-refresh"
		originalDiskUUID = "old-uuid"
		cnsDiskUUID      = "new-uuid-from-cns"
		cnsDiskPath      = "[ds2] moved.vmdk"
	)
	cvi := makeCVI(ns, volumeID, pvcName, originalDiskUUID, csivolumeinfov1alpha1.OwnershipStateCSIManaged)
	svc := &fakeCVISvc{
		cviByVolID:    map[string]*csivolumeinfov1alpha1.CsiVolumeInfo{volumeID: cvi},
		cviBydiskUUID: map[string]*csivolumeinfov1alpha1.CsiVolumeInfo{},
	}
	mgr := &fakeUnregisterManager{returnDiskPath: cnsDiskPath, returnDiskUUID: cnsDiskUUID}
	instance := &bav1alpha1.CnsNodeVMBatchAttachment{
		ObjectMeta: metav1.ObjectMeta{Name: "ba1", Namespace: ns},
	}
	c := fake.NewClientBuilder().WithScheme(makeVMScheme()).Build()

	if err := processVMOwnedVolumesAttach(ctx, svc, c, mgr, instance,
		"vm1", "inst1", pvcName, volumeID, "disk1"); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	// CVI diskUUID must be updated to the CNS-returned value.
	if svc.cviByVolID[volumeID].Status.DiskUUID != cnsDiskUUID {
		t.Errorf("expected DiskUUID %q from CNS, got %q", cnsDiskUUID, svc.cviByVolID[volumeID].Status.DiskUUID)
	}
	if svc.cviByVolID[volumeID].Status.DiskPath != cnsDiskPath {
		t.Errorf("expected DiskPath %q from CNS, got %q", cnsDiskPath, svc.cviByVolID[volumeID].Status.DiskPath)
	}
}

func TestProcessVMOwnedVolumesAttach_NoCVI_Error(t *testing.T) {
	ctx := context.Background()
	svc := &fakeCVISvc{
		cviByVolID:    map[string]*csivolumeinfov1alpha1.CsiVolumeInfo{},
		cviBydiskUUID: map[string]*csivolumeinfov1alpha1.CsiVolumeInfo{},
	}
	mgr := &fakeUnregisterManager{}
	s := makeVMScheme()
	c := fake.NewClientBuilder().WithScheme(s).Build()
	instance := &bav1alpha1.CnsNodeVMBatchAttachment{
		ObjectMeta: metav1.ObjectMeta{Name: "ba1", Namespace: "ns"},
		Spec:       bav1alpha1.CnsNodeVMBatchAttachmentSpec{InstanceUUID: "inst1"},
	}

	err := processVMOwnedVolumesAttach(ctx, svc, c, mgr, instance,
		"vm1", "inst1", "pvc1", "vol1", "disk1")

	if err == nil {
		t.Fatal("expected error when CVI is absent on a VMOwnedVolumes VM")
	}
	if mgr.unregisterCalled {
		t.Error("expected UnregisterVolumeEx NOT to be called")
	}
}

func TestProcessVMOwnedVolumesAttach_CVINotCSIManaged_Error(t *testing.T) {
	ctx := context.Background()
	const volumeID = "vol-1"
	cvi := makeCVI("ns", volumeID, "pvc1", "duuid", csivolumeinfov1alpha1.OwnershipStateTransferringToVM)
	svc := &fakeCVISvc{
		cviByVolID:    map[string]*csivolumeinfov1alpha1.CsiVolumeInfo{volumeID: cvi},
		cviBydiskUUID: map[string]*csivolumeinfov1alpha1.CsiVolumeInfo{},
	}
	mgr := &fakeUnregisterManager{}
	s := makeVMScheme()
	c := fake.NewClientBuilder().WithScheme(s).Build()
	instance := &bav1alpha1.CnsNodeVMBatchAttachment{
		ObjectMeta: metav1.ObjectMeta{Name: "ba1", Namespace: "ns"},
		Spec:       bav1alpha1.CnsNodeVMBatchAttachmentSpec{InstanceUUID: "inst1"},
	}

	err := processVMOwnedVolumesAttach(ctx, svc, c, mgr, instance,
		"vm1", "inst1", "pvc1", volumeID, "disk1")

	if err == nil {
		t.Fatal("expected error when CVI is not CSI_MANAGED")
	}
	if mgr.unregisterCalled {
		t.Error("expected UnregisterVolumeEx NOT called when CVI in wrong state")
	}
}

func TestProcessVMOwnedVolumesAttach_UnregisterFails(t *testing.T) {
	ctx := context.Background()
	const volumeID = "vol-1"
	cvi := makeCVI("ns", volumeID, "pvc1", "duuid", csivolumeinfov1alpha1.OwnershipStateCSIManaged)
	svc := &fakeCVISvc{
		cviByVolID:    map[string]*csivolumeinfov1alpha1.CsiVolumeInfo{volumeID: cvi},
		cviBydiskUUID: map[string]*csivolumeinfov1alpha1.CsiVolumeInfo{},
	}
	mgr := &fakeUnregisterManager{unregisterErr: errors.New("cns fault")}
	s := makeVMScheme()
	c := fake.NewClientBuilder().WithScheme(s).Build()
	instance := &bav1alpha1.CnsNodeVMBatchAttachment{
		ObjectMeta: metav1.ObjectMeta{Name: "ba1", Namespace: "ns"},
		Spec:       bav1alpha1.CnsNodeVMBatchAttachmentSpec{InstanceUUID: "inst1"},
	}

	err := processVMOwnedVolumesAttach(ctx, svc, c, mgr, instance,
		"vm1", "inst1", "pvc1", volumeID, "disk1")

	if err == nil {
		t.Fatal("expected error when UnregisterVolumeEx fails")
	}
	if svc.cviByVolID[volumeID].Status.OwnershipState != csivolumeinfov1alpha1.OwnershipStateCSIManaged {
		t.Errorf("CVI should remain CSI_MANAGED on error, got %q",
			svc.cviByVolID[volumeID].Status.OwnershipState)
	}
}

// ---------------------------------------------------------------------------
// reconcileVMOwnedVolumesDetach tests
// ---------------------------------------------------------------------------

func TestReconcileVMOwnedVolumesDetach_NoCVI_Error(t *testing.T) {
	ctx := context.Background()
	svc := &fakeCVISvc{
		cviByVolID:    map[string]*csivolumeinfov1alpha1.CsiVolumeInfo{},
		cviBydiskUUID: map[string]*csivolumeinfov1alpha1.CsiVolumeInfo{},
	}
	s := makeVMScheme()
	c := fake.NewClientBuilder().WithScheme(s).Build()
	instance := &bav1alpha1.CnsNodeVMBatchAttachment{
		ObjectMeta: metav1.ObjectMeta{Name: "ba1", Namespace: "ns"},
		Spec:       bav1alpha1.CnsNodeVMBatchAttachmentSpec{InstanceUUID: "inst1"},
	}

	err := reconcileVMOwnedVolumesDetach(ctx, svc, nil, c, nil, instance,
		"pvc1", "vol1", nil)

	if err == nil {
		t.Fatal("expected error when CVI is absent on a VMOwnedVolumes VM")
	}
}

func TestReconcileVMOwnedVolumesDetach_CSIManaged_Idempotent(t *testing.T) {
	ctx := context.Background()
	const volumeID = "vol-1"
	cvi := makeCVI("ns", volumeID, "pvc1", "duuid", csivolumeinfov1alpha1.OwnershipStateCSIManaged)
	svc := &fakeCVISvc{
		cviByVolID:    map[string]*csivolumeinfov1alpha1.CsiVolumeInfo{volumeID: cvi},
		cviBydiskUUID: map[string]*csivolumeinfov1alpha1.CsiVolumeInfo{},
	}
	s := makeVMScheme()
	c := fake.NewClientBuilder().WithScheme(s).Build()
	instance := &bav1alpha1.CnsNodeVMBatchAttachment{
		ObjectMeta: metav1.ObjectMeta{Name: "ba1", Namespace: "ns"},
		Spec:       bav1alpha1.CnsNodeVMBatchAttachmentSpec{InstanceUUID: "inst1"},
	}

	// A CSI_MANAGED CVI means the volume was already returned — should be a no-op.
	if err := reconcileVMOwnedVolumesDetach(ctx, svc, nil, c, nil, instance,
		"pvc1", volumeID, nil); err != nil {
		t.Fatalf("unexpected error for already-CSI-managed volume: %v", err)
	}
}

// ---------------------------------------------------------------------------
// setOrReplaceCondition tests
// ---------------------------------------------------------------------------

func TestSetOrReplaceCondition_AddsNew(t *testing.T) {
	conds := []metav1.Condition{}
	c := metav1.Condition{Type: "Foo", Status: metav1.ConditionTrue, Reason: "Bar"}
	setOrReplaceCondition(&conds, c)
	if len(conds) != 1 {
		t.Fatalf("expected 1 condition, got %d", len(conds))
	}
	if conds[0].Type != "Foo" {
		t.Errorf("wrong type: %q", conds[0].Type)
	}
}

func TestSetOrReplaceCondition_ReplacesExisting(t *testing.T) {
	old := metav1.Condition{Type: "Foo", Status: metav1.ConditionFalse, Reason: "Old"}
	conds := []metav1.Condition{old}
	newCond := metav1.Condition{Type: "Foo", Status: metav1.ConditionTrue, Reason: "New"}
	setOrReplaceCondition(&conds, newCond)
	if len(conds) != 1 {
		t.Fatalf("expected 1 condition, got %d", len(conds))
	}
	if conds[0].Status != metav1.ConditionTrue {
		t.Errorf("condition not replaced: status=%q", conds[0].Status)
	}
}

// ---------------------------------------------------------------------------
// setBAVolumeAttachMethodReconfig tests
// ---------------------------------------------------------------------------

func TestSetBAVolumeAttachMethodReconfig_NewEntry(t *testing.T) {
	ctx := context.Background()
	instance := &bav1alpha1.CnsNodeVMBatchAttachment{}

	setBAVolumeAttachMethodReconfig(ctx, instance, "vol1", "pvc1", "volid1", "uuid1", "/path/to.vmdk")

	if len(instance.Status.VolumeStatus) != 1 {
		t.Fatalf("expected 1 VolumeStatus, got %d", len(instance.Status.VolumeStatus))
	}
	vs := instance.Status.VolumeStatus[0]
	if vs.PersistentVolumeClaim.DiskPath != "/path/to.vmdk" {
		t.Errorf("diskPath not set: %q", vs.PersistentVolumeClaim.DiskPath)
	}
	if vs.PersistentVolumeClaim.DiskUUID != "uuid1" {
		t.Errorf("diskUUID not set: %q", vs.PersistentVolumeClaim.DiskUUID)
	}
	var foundCond bool
	for _, c := range vs.PersistentVolumeClaim.Conditions {
		if c.Type == bav1alpha1.ConditionAttachMethod && c.Reason == bav1alpha1.ReasonReconfig {
			foundCond = true
		}
	}
	if !foundCond {
		t.Error("AttachMethod=Reconfig condition not set")
	}
}

func TestSetBAVolumeAttachMethodReconfig_UpdatesExisting(t *testing.T) {
	ctx := context.Background()
	instance := &bav1alpha1.CnsNodeVMBatchAttachment{
		Status: bav1alpha1.CnsNodeVMBatchAttachmentStatus{
			VolumeStatus: []bav1alpha1.VolumeStatus{
				{
					Name: "vol1",
					PersistentVolumeClaim: bav1alpha1.PersistentVolumeClaimStatus{
						ClaimName: "pvc1",
					},
				},
			},
		},
	}

	setBAVolumeAttachMethodReconfig(ctx, instance, "vol1", "pvc1", "volid1", "uuid1", "/new/path.vmdk")

	if len(instance.Status.VolumeStatus) != 1 {
		t.Fatalf("expected 1 VolumeStatus, got %d", len(instance.Status.VolumeStatus))
	}
	vs := instance.Status.VolumeStatus[0]
	if vs.PersistentVolumeClaim.DiskPath != "/new/path.vmdk" {
		t.Errorf("diskPath not updated: %q", vs.PersistentVolumeClaim.DiskPath)
	}
}

// ---------------------------------------------------------------------------
// Task 3.3a -- Hold PVC protection finalizer for non-CSI_MANAGED volumes
// ---------------------------------------------------------------------------

// TestRemovePvcProtectionFinalizer_HoldsForNonCSIManaged verifies that
// removePvcProtectionFinalizersForTrackedPVCs skips removal when the CVI is
// in a non-CSI_MANAGED state.
func TestRemovePvcProtectionFinalizer_HoldsForNonCSIManaged(t *testing.T) {
	ctx := context.Background()
	const (
		ns      = "test-ns"
		pvcName = "pvc-vm-owned"
		volName = "vol1"
	)

	instance := &bav1alpha1.CnsNodeVMBatchAttachment{
		Spec: bav1alpha1.CnsNodeVMBatchAttachmentSpec{
			Volumes: []bav1alpha1.VolumeSpec{
				{
					Name: volName,
					PersistentVolumeClaim: bav1alpha1.PersistentVolumeClaimSpec{
						ClaimName: pvcName,
					},
				},
			},
		},
	}
	instance.Namespace = ns

	cviSvc := &fakeCVISvc{
		cviByVolID:    make(map[string]*csivolumeinfov1alpha1.CsiVolumeInfo),
		cviBydiskUUID: make(map[string]*csivolumeinfov1alpha1.CsiVolumeInfo),
		cviByPVCName: map[string]*csivolumeinfov1alpha1.CsiVolumeInfo{
			pvcName: {
				Spec: csivolumeinfov1alpha1.CsiVolumeInfoSpec{PVCName: pvcName},
				Status: csivolumeinfov1alpha1.CsiVolumeInfoStatus{
					OwnershipState: csivolumeinfov1alpha1.OwnershipStateVMManaged,
				},
			},
		},
	}

	// The function should return nil (loop skips the PVC, no removePvcFinalizerFn called).
	err := removePvcProtectionFinalizersForTrackedPVCs(ctx, instance, nil, nil, nil, cviSvc)
	if err != nil {
		t.Fatalf("expected nil error, got: %v", err)
	}
}

// TestRemovePvcProtectionFinalizer_AllowsCSIManaged verifies that the finalizer
// skip does NOT apply when CVI is CSI_MANAGED.
func TestRemovePvcProtectionFinalizer_AllowsCSIManaged(t *testing.T) {
	ctx := context.Background()
	const (
		ns      = "test-ns"
		pvcName = "pvc-csi-owned"
		volName = "vol1"
	)

	instance := &bav1alpha1.CnsNodeVMBatchAttachment{
		Spec: bav1alpha1.CnsNodeVMBatchAttachmentSpec{
			Volumes: []bav1alpha1.VolumeSpec{
				{
					Name: volName,
					PersistentVolumeClaim: bav1alpha1.PersistentVolumeClaimSpec{
						ClaimName: pvcName,
					},
				},
			},
		},
	}
	instance.Namespace = ns

	cviSvc := &fakeCVISvc{
		cviByVolID:    make(map[string]*csivolumeinfov1alpha1.CsiVolumeInfo),
		cviBydiskUUID: make(map[string]*csivolumeinfov1alpha1.CsiVolumeInfo),
		cviByPVCName: map[string]*csivolumeinfov1alpha1.CsiVolumeInfo{
			pvcName: {
				Spec: csivolumeinfov1alpha1.CsiVolumeInfoSpec{PVCName: pvcName},
				Status: csivolumeinfov1alpha1.CsiVolumeInfoStatus{
					OwnershipState: csivolumeinfov1alpha1.OwnershipStateCSIManaged,
				},
			},
		},
	}

	// removePvcFinalizerFn is a package-level var; it will be called here.
	// The real implementation would try to remove a finalizer from a PVC via
	// client calls which we cannot do in this unit test. Replace it with a no-op.
	origFn := removePvcFinalizerFn
	defer func() { removePvcFinalizerFn = origFn }()
	removePvcFinalizerFn = func(_ context.Context, _ client.Client,
		_ kubernetes.Interface, _ client.Client, _, _, _ string) error {
		return nil
	}

	err := removePvcProtectionFinalizersForTrackedPVCs(ctx, instance, nil, nil, nil, cviSvc)
	if err != nil {
		t.Fatalf("expected nil error, got: %v", err)
	}
}

// TestRemovePvcProtectionFinalizer_NilSvcAllowsAll verifies that when cviSvc is nil
// (FSS disabled), the function does not skip any PVC.
func TestRemovePvcProtectionFinalizer_NilSvcAllowsAll(t *testing.T) {
	ctx := context.Background()
	instance := &bav1alpha1.CnsNodeVMBatchAttachment{
		Spec: bav1alpha1.CnsNodeVMBatchAttachmentSpec{
			Volumes: []bav1alpha1.VolumeSpec{
				{
					Name: "vol1",
					PersistentVolumeClaim: bav1alpha1.PersistentVolumeClaimSpec{
						ClaimName: "pvc1",
					},
				},
			},
		},
	}
	instance.Namespace = "test-ns"

	origFn := removePvcFinalizerFn
	defer func() { removePvcFinalizerFn = origFn }()
	called := false
	removePvcFinalizerFn = func(_ context.Context, _ client.Client,
		_ kubernetes.Interface, _ client.Client, _, _, _ string) error {
		called = true
		return nil
	}

	// nil cviSvc simulates FSS disabled.
	err := removePvcProtectionFinalizersForTrackedPVCs(ctx, instance, nil, nil, nil, nil)
	if err != nil {
		t.Fatalf("expected nil error, got: %v", err)
	}
	if !called {
		t.Error("expected removePvcFinalizerFn to be called when cviSvc is nil")
	}
}

// ---------------------------------------------------------------------------
// Task 4.3 -- hasAttachMethodReconfig helper tests
// ---------------------------------------------------------------------------

func TestHasAttachMethodReconfig_TrueWhenSet(t *testing.T) {
	instance := &bav1alpha1.CnsNodeVMBatchAttachment{
		Status: bav1alpha1.CnsNodeVMBatchAttachmentStatus{
			VolumeStatus: []bav1alpha1.VolumeStatus{
				{
					Name: "vol1",
					PersistentVolumeClaim: bav1alpha1.PersistentVolumeClaimStatus{
						Conditions: []metav1.Condition{
							{
								Type:   bav1alpha1.ConditionAttachMethod,
								Reason: bav1alpha1.ReasonReconfig,
								Status: metav1.ConditionTrue,
							},
						},
					},
				},
			},
		},
	}
	if !hasAttachMethodReconfig(instance, "vol1") {
		t.Error("expected hasAttachMethodReconfig to return true when condition is set")
	}
}

func TestHasAttachMethodReconfig_FalseWhenNotSet(t *testing.T) {
	instance := &bav1alpha1.CnsNodeVMBatchAttachment{}
	if hasAttachMethodReconfig(instance, "vol1") {
		t.Error("expected hasAttachMethodReconfig to return false for empty instance")
	}
}

func TestHasAttachMethodReconfig_FalseWhenDifferentReason(t *testing.T) {
	instance := &bav1alpha1.CnsNodeVMBatchAttachment{
		Status: bav1alpha1.CnsNodeVMBatchAttachmentStatus{
			VolumeStatus: []bav1alpha1.VolumeStatus{
				{
					Name: "vol1",
					PersistentVolumeClaim: bav1alpha1.PersistentVolumeClaimStatus{
						Conditions: []metav1.Condition{
							{
								Type:   bav1alpha1.ConditionAttachMethod,
								Reason: bav1alpha1.ReasonCnsAttach,
								Status: metav1.ConditionTrue,
							},
						},
					},
				},
			},
		},
	}
	if hasAttachMethodReconfig(instance, "vol1") {
		t.Error("expected false when reason is CnsAttach, not Reconfig")
	}
}

func TestHasAttachMethodReconfig_FalseForUnknownVolume(t *testing.T) {
	instance := &bav1alpha1.CnsNodeVMBatchAttachment{
		Status: bav1alpha1.CnsNodeVMBatchAttachmentStatus{
			VolumeStatus: []bav1alpha1.VolumeStatus{
				{
					Name: "vol1",
					PersistentVolumeClaim: bav1alpha1.PersistentVolumeClaimStatus{
						Conditions: []metav1.Condition{
							{
								Type:   bav1alpha1.ConditionAttachMethod,
								Reason: bav1alpha1.ReasonReconfig,
								Status: metav1.ConditionTrue,
							},
						},
					},
				},
			},
		},
	}
	// "vol2" is not in the status.
	if hasAttachMethodReconfig(instance, "vol2") {
		t.Error("expected false for volume not present in status")
	}
}

// ---------------------------------------------------------------------------
// Task 5.10 -- isIndependentDiskMode tests
// ---------------------------------------------------------------------------

func TestIsIndependentDiskMode_IndependentPersistent(t *testing.T) {
	if !isIndependentDiskMode(bav1alpha1.IndependentPersistent) {
		t.Error("expected independent_persistent to be independent")
	}
}

func TestIsIndependentDiskMode_IndependentNonPersistent(t *testing.T) {
	if !isIndependentDiskMode(bav1alpha1.DiskMode(bav1alpha1.IndependentNonPersistent)) {
		t.Error("expected independent_nonpersistent to be independent")
	}
}

func TestIsIndependentDiskMode_Persistent(t *testing.T) {
	if isIndependentDiskMode(bav1alpha1.Persistent) {
		t.Error("expected persistent to NOT be independent")
	}
}

func TestIsIndependentDiskMode_Empty(t *testing.T) {
	if isIndependentDiskMode("") {
		t.Error("expected empty disk mode to NOT be independent")
	}
}

// Verify the fakes satisfy their interfaces at compile time.
var _ interface {
	GetCsiVolumeInfo(context.Context, string, string) (*csivolumeinfov1alpha1.CsiVolumeInfo, error)
	GetCsiVolumeInfoByDiskUUID(context.Context, string, string) (*csivolumeinfov1alpha1.CsiVolumeInfo, error)
	UpdateCsiVolumeInfoStatus(context.Context, *csivolumeinfov1alpha1.CsiVolumeInfo) error
	AddCVIProtectionFinalizer(context.Context, string, string) error
	RemoveCVIProtectionFinalizer(context.Context, string, string) error
} = (*fakeCVISvc)(nil)

var _ volumeManagerForAttach = (*fakeUnregisterManager)(nil)

// Suppress unused import linting.
var _ client.Client = (client.Client)(nil)

// ---------------------------------------------------------------------------
// Task 5.9 -- reconcileStaleCVIs tests
// ---------------------------------------------------------------------------

// makeBAWithStatusAndSpec creates a CnsNodeVMBatchAttachment with the given
// PVC names in status and (optionally) in spec.
func makeBAWithStatusAndSpec(ns string,
	statusPVCs []struct{ name, volID string },
	specPVCs []string,
) *bav1alpha1.CnsNodeVMBatchAttachment {
	ba := &bav1alpha1.CnsNodeVMBatchAttachment{
		ObjectMeta: metav1.ObjectMeta{Name: "ba-stale", Namespace: ns},
	}
	for _, pvc := range statusPVCs {
		ba.Status.VolumeStatus = append(ba.Status.VolumeStatus,
			bav1alpha1.VolumeStatus{
				Name: "vol-" + pvc.name,
				PersistentVolumeClaim: bav1alpha1.PersistentVolumeClaimStatus{
					ClaimName:   pvc.name,
					CnsVolumeID: pvc.volID,
				},
			})
	}
	for _, pvcName := range specPVCs {
		ba.Spec.Volumes = append(ba.Spec.Volumes,
			bav1alpha1.VolumeSpec{
				Name: "vol-" + pvcName,
				PersistentVolumeClaim: bav1alpha1.PersistentVolumeClaimSpec{
					ClaimName: pvcName,
				},
			})
	}
	return ba
}

// TestReconcileStaleCVIs_CSIManagedIsNoop verifies that a CSI_MANAGED CVI whose
// volume is no longer in BA spec does not trigger any state change.
func TestReconcileStaleCVIs_CSIManagedIsNoop(t *testing.T) {
	ctx := context.Background()
	const (
		ns      = "ns-stale"
		pvcName = "pvc-csi"
		volID   = "vol-csi"
	)
	cvi := makeCVI(ns, volID, pvcName, "uuid", csivolumeinfov1alpha1.OwnershipStateCSIManaged)
	svc := &fakeCVISvc{
		cviByVolID:    map[string]*csivolumeinfov1alpha1.CsiVolumeInfo{volID: cvi},
		cviBydiskUUID: map[string]*csivolumeinfov1alpha1.CsiVolumeInfo{},
	}

	ba := makeBAWithStatusAndSpec(ns,
		[]struct{ name, volID string }{{pvcName, volID}},
		nil) // volume NOT in spec

	scheme := makeVMScheme()
	c := fake.NewClientBuilder().WithScheme(scheme).Build()

	err := reconcileStaleCVIs(ctx, svc, nil, c, c, ba)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	// CVI must remain CSI_MANAGED (no update should have been called).
	if cvi.Status.OwnershipState != csivolumeinfov1alpha1.OwnershipStateCSIManaged {
		t.Errorf("expected CSI_MANAGED to be unchanged, got %q", cvi.Status.OwnershipState)
	}
}

// TestReconcileStaleCVIs_VolumeInSpecIsNoop verifies that a volume still present
// in BA spec is not processed by the stale reconciler.
func TestReconcileStaleCVIs_VolumeInSpecIsNoop(t *testing.T) {
	ctx := context.Background()
	const (
		ns      = "ns-stale-inspec"
		pvcName = "pvc-inspec"
		volID   = "vol-inspec"
	)
	cvi := makeCVI(ns, volID, pvcName, "uuid", csivolumeinfov1alpha1.OwnershipStateTransferringToVM)
	svc := &fakeCVISvc{
		cviByVolID:    map[string]*csivolumeinfov1alpha1.CsiVolumeInfo{volID: cvi},
		cviBydiskUUID: map[string]*csivolumeinfov1alpha1.CsiVolumeInfo{},
	}

	ba := makeBAWithStatusAndSpec(ns,
		[]struct{ name, volID string }{{pvcName, volID}},
		[]string{pvcName}) // volume IS in spec

	scheme := makeVMScheme()
	c := fake.NewClientBuilder().WithScheme(scheme).Build()

	err := reconcileStaleCVIs(ctx, svc, nil, c, c, ba)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	// TRANSFERRING_TO_VM must remain unchanged (volume is in spec; stale reconciler must not touch it).
	if cvi.Status.OwnershipState != csivolumeinfov1alpha1.OwnershipStateTransferringToVM {
		t.Errorf("expected TRANSFERRING_TO_VM unchanged, got %q", cvi.Status.OwnershipState)
	}
}

// TestReconcileStaleCVIs_VMDeletedAdvancesToTransferringToCSI verifies that a
// VM_MANAGED CVI whose volume is not in spec and whose VM no longer exists is
// advanced to TRANSFERRING_TO_CSI.
func TestReconcileStaleCVIs_VMDeletedAdvancesToTransferringToCSI(t *testing.T) {
	ctx := context.Background()
	const (
		ns      = "ns-stale-vmdel"
		pvcName = "pvc-vmdel"
		volID   = "vol-vmdel"
		vmName  = "deleted-vm"
	)
	cvi := makeCVI(ns, volID, pvcName, "uuid", csivolumeinfov1alpha1.OwnershipStateVMManaged)
	cvi.Status.VMName = vmName

	svc := &fakeCVISvc{
		cviByVolID:    map[string]*csivolumeinfov1alpha1.CsiVolumeInfo{volID: cvi},
		cviBydiskUUID: map[string]*csivolumeinfov1alpha1.CsiVolumeInfo{},
	}

	ba := makeBAWithStatusAndSpec(ns,
		[]struct{ name, volID string }{{pvcName, volID}},
		nil) // volume NOT in spec

	// VM does NOT exist in K8s.
	scheme := makeVMScheme()
	c := fake.NewClientBuilder().WithScheme(scheme).Build()

	err := reconcileStaleCVIs(ctx, svc, nil, c, c, ba)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	// CVI should be advanced to TRANSFERRING_TO_CSI.
	if cvi.Status.OwnershipState != csivolumeinfov1alpha1.OwnershipStateTransferringToCSI {
		t.Errorf("expected TRANSFERRING_TO_CSI, got %q", cvi.Status.OwnershipState)
	}
}

// TestReconcileStaleCVIs_SnapshotRetainedIsNoop verifies that a snapshot-retained
// CVI (VM_MANAGED + vmName="") is not touched.
func TestReconcileStaleCVIs_SnapshotRetainedIsNoop(t *testing.T) {
	ctx := context.Background()
	const (
		ns      = "ns-stale-snap"
		pvcName = "pvc-snap"
		volID   = "vol-snap"
	)
	cvi := makeCVI(ns, volID, pvcName, "uuid", csivolumeinfov1alpha1.OwnershipStateVMManaged)
	cvi.Status.VMName = "" // snapshot-retained

	svc := &fakeCVISvc{
		cviByVolID:    map[string]*csivolumeinfov1alpha1.CsiVolumeInfo{volID: cvi},
		cviBydiskUUID: map[string]*csivolumeinfov1alpha1.CsiVolumeInfo{},
	}

	ba := makeBAWithStatusAndSpec(ns,
		[]struct{ name, volID string }{{pvcName, volID}},
		nil)

	scheme := makeVMScheme()
	c := fake.NewClientBuilder().WithScheme(scheme).Build()

	err := reconcileStaleCVIs(ctx, svc, nil, c, c, ba)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	// VM_MANAGED with empty vmName must remain unchanged.
	if cvi.Status.OwnershipState != csivolumeinfov1alpha1.OwnershipStateVMManaged {
		t.Errorf("expected VM_MANAGED unchanged, got %q", cvi.Status.OwnershipState)
	}
	if cvi.Status.VMName != "" {
		t.Errorf("expected empty vmName, got %q", cvi.Status.VMName)
	}
}
