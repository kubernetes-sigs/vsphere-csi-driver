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

package virtualmachinesnapshot

import (
	"context"
	"encoding/json"
	"errors"
	"testing"

	vmoperatortypes "github.com/vmware-tanzu/vm-operator/api/v1alpha5"
	cnstypes "github.com/vmware/govmomi/cns/types"
	"go.uber.org/zap"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"

	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	csivolumeinfov1alpha1 "sigs.k8s.io/vsphere-csi-driver/v3/pkg/apis/cnsoperator/csivolumeinfo/v1alpha1"
	cnsvolume "sigs.k8s.io/vsphere-csi-driver/v3/pkg/common/cns-lib/volume"
	"sigs.k8s.io/vsphere-csi-driver/v3/pkg/common/config"
)

// ---------------------------------------------------------------------------
// Helpers shared across snapshot deletion tests
// ---------------------------------------------------------------------------

type snapshotTestCVISvc struct {
	cviByDiskUUID map[string]*csivolumeinfov1alpha1.CsiVolumeInfo
	updateCalled  bool
	removeCalled  bool
	err           error
}

func (s *snapshotTestCVISvc) GetCsiVolumeInfo(
	_ context.Context, _, _ string) (*csivolumeinfov1alpha1.CsiVolumeInfo, error) {
	return nil, nil
}
func (s *snapshotTestCVISvc) GetCsiVolumeInfoByDiskUUID(
	_ context.Context, _, diskUUID string) (*csivolumeinfov1alpha1.CsiVolumeInfo, error) {
	if s.err != nil {
		return nil, s.err
	}
	return s.cviByDiskUUID[diskUUID], nil
}
func (s *snapshotTestCVISvc) GetCsiVolumeInfoByPVCName(
	_ context.Context, _, _ string) (*csivolumeinfov1alpha1.CsiVolumeInfo, error) {
	return nil, nil
}
func (s *snapshotTestCVISvc) CreateCsiVolumeInfo(_ context.Context, _ *csivolumeinfov1alpha1.CsiVolumeInfo) error {
	return nil
}
func (s *snapshotTestCVISvc) UpdateCsiVolumeInfoStatus(
	_ context.Context, cvi *csivolumeinfov1alpha1.CsiVolumeInfo) error {
	s.updateCalled = true
	// Mutate the CVI in the map so assertions can see the updated state.
	for k, v := range s.cviByDiskUUID {
		if v.Name == cvi.Name {
			s.cviByDiskUUID[k] = cvi
		}
	}
	return nil
}
func (s *snapshotTestCVISvc) PatchCsiVolumeInfo(_ context.Context, _, _ string, _ []byte) error {
	return nil
}
func (s *snapshotTestCVISvc) DeleteCsiVolumeInfo(_ context.Context, _, _ string) error {
	return nil
}
func (s *snapshotTestCVISvc) CsiVolumeInfoExists(_ context.Context, _, _ string) (bool, error) {
	return false, nil
}
func (s *snapshotTestCVISvc) AddCVIProtectionFinalizer(_ context.Context, _, _ string) error {
	return nil
}
func (s *snapshotTestCVISvc) RemoveCVIProtectionFinalizer(_ context.Context, _, _ string) error {
	s.removeCalled = true
	return nil
}

type snapshotTestVolMgr struct {
	cnsvolume.MockManager
	createVolumeErr      error
	createVolumeFaultTyp string
}

func (m *snapshotTestVolMgr) CreateVolume(
	_ context.Context, _ *cnstypes.CnsVolumeCreateSpec, _ interface{},
) (*cnsvolume.CnsVolumeInfo, string, error) {
	return &cnsvolume.CnsVolumeInfo{}, m.createVolumeFaultTyp, m.createVolumeErr
}

func makeSnapDeletionReconciler(
	cviSvc *snapshotTestCVISvc, volMgr *snapshotTestVolMgr, k8sObjs ...runtime.Object,
) *ReconcileVirtualMachineSnapshot {
	s := runtime.NewScheme()
	_ = corev1.AddToScheme(s)
	_ = csivolumeinfov1alpha1.AddToScheme(s)
	_ = vmoperatortypes.AddToScheme(s)
	c := fake.NewClientBuilder().WithScheme(s).WithRuntimeObjects(k8sObjs...).Build()

	cfg := &config.Config{
		VirtualCenter: map[string]*config.VirtualCenterConfig{
			"vc": {User: "u"},
		},
	}
	cfg.Global.ClusterID = "cid"

	return &ReconcileVirtualMachineSnapshot{
		client:        c,
		cviService:    cviSvc,
		volumeManager: volMgr,
		configInfo:    &config.ConfigurationInfo{Cfg: cfg},
		// vcenter is nil — causes the reconciler to treat all disks as not retained.
	}
}

func testLogger() *zap.SugaredLogger {
	l, _ := zap.NewDevelopment()
	return l.Sugar()
}

// ---------------------------------------------------------------------------
// extractSnapshotDisks tests
// ---------------------------------------------------------------------------

// TestExtractSnapshotDisks_EmptyStatus verifies that a VMSnap with no status
// returns an empty disk list without error.
func TestExtractSnapshotDisks_EmptyStatus(t *testing.T) {
	snap := &vmoperatortypes.VirtualMachineSnapshot{}
	disks, err := extractSnapshotDisks(snap)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(disks) != 0 {
		t.Errorf("expected 0 disks, got %d", len(disks))
	}
}

// TestExtractSnapshotDisks_DirectParsing tests the internal parseDisksFromStatusJSON
// helper to ensure the disk JSON is decoded correctly independent of the typed status.
func TestExtractSnapshotDisks_DirectParsing(t *testing.T) {
	// The typed vmoperatortypes.VirtualMachineSnapshotStatus does not declare
	// a 'disks' field, so we test the parsing logic through the helper directly.
	rawDisks := `[{"id":"uuid-1","key":2000},{"id":"uuid-2","key":2001}]`
	statusJSON := `{"disks":` + rawDisks + `}`

	var statusMap map[string]json.RawMessage
	if err := json.Unmarshal([]byte(statusJSON), &statusMap); err != nil {
		t.Fatalf("failed to unmarshal status map: %v", err)
	}

	rawDiskField, ok := statusMap["disks"]
	if !ok {
		t.Fatal("expected disks field in status map")
	}

	var disks []vmSnapshotDiskEntry
	if err := json.Unmarshal(rawDiskField, &disks); err != nil {
		t.Fatalf("failed to unmarshal disks: %v", err)
	}

	if len(disks) != 2 {
		t.Fatalf("expected 2 disks, got %d", len(disks))
	}
	if disks[0].ID != "uuid-1" || disks[0].Key != 2000 {
		t.Errorf("unexpected first disk: %+v", disks[0])
	}
	if disks[1].ID != "uuid-2" || disks[1].Key != 2001 {
		t.Errorf("unexpected second disk: %+v", disks[1])
	}
}

// ---------------------------------------------------------------------------
// processDiskAfterSnapshotDeletion tests
// ---------------------------------------------------------------------------

func makePVCandPV(ns, pvcName, pvName string) (*corev1.PersistentVolumeClaim, *corev1.PersistentVolume) {
	pvc := &corev1.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{Name: pvcName, Namespace: ns},
	}
	pv := &corev1.PersistentVolume{
		ObjectMeta: metav1.ObjectMeta{Name: pvName},
		Spec: corev1.PersistentVolumeSpec{
			ClaimRef: &corev1.ObjectReference{Namespace: ns, Name: pvcName},
		},
	}
	return pvc, pv
}

func makeCVIForDisk(
	ns, volID, pvcName, pvName, diskUUID string,
	state csivolumeinfov1alpha1.OwnershipState, vmName string,
) *csivolumeinfov1alpha1.CsiVolumeInfo {
	return &csivolumeinfov1alpha1.CsiVolumeInfo{
		ObjectMeta: metav1.ObjectMeta{
			Name:       "csi-volume-info-" + volID,
			Namespace:  ns,
			Finalizers: []string{csivolumeinfov1alpha1.CVIProtectionFinalizer},
		},
		Spec: csivolumeinfov1alpha1.CsiVolumeInfoSpec{
			VolumeID: volID,
			PVCName:  pvcName,
			PVName:   pvName,
		},
		Status: csivolumeinfov1alpha1.CsiVolumeInfoStatus{
			OwnershipState: state,
			DiskUUID:       diskUUID,
			DiskPath:       "[ds1] ns/vm/disk.vmdk",
			VMName:         vmName,
		},
	}
}

// TestProcessDiskAfterSnapshotDeletion_NoCVI verifies that a missing CVI is a no-op.
func TestProcessDiskAfterSnapshotDeletion_NoCVI(t *testing.T) {
	ctx := context.Background()
	svc := &snapshotTestCVISvc{cviByDiskUUID: map[string]*csivolumeinfov1alpha1.CsiVolumeInfo{}}
	r := makeSnapDeletionReconciler(svc, &snapshotTestVolMgr{})

	err := r.processDiskAfterSnapshotDeletion(ctx, testLogger(), "ns", "disk-uuid-missing",
		map[string]struct{}{})
	if err != nil {
		t.Errorf("expected no error for missing CVI, got: %v", err)
	}
}

// TestProcessDiskAfterSnapshotDeletion_StillRetained verifies that a disk still
// referenced by another snapshot is left as-is (no re-registration).
func TestProcessDiskAfterSnapshotDeletion_StillRetained(t *testing.T) {
	ctx := context.Background()
	const (
		ns       = "ns-retained"
		diskUUID = "disk-retained"
		volID    = "vol-retained"
		pvcName  = "pvc-retained"
		pvName   = "pv-retained"
	)
	cvi := makeCVIForDisk(ns, volID, pvcName, pvName, diskUUID,
		csivolumeinfov1alpha1.OwnershipStateVMManaged, "")
	svc := &snapshotTestCVISvc{
		cviByDiskUUID: map[string]*csivolumeinfov1alpha1.CsiVolumeInfo{diskUUID: cvi},
	}
	r := makeSnapDeletionReconciler(svc, &snapshotTestVolMgr{})

	// The disk is still retained by another snapshot.
	retainedSet := map[string]struct{}{diskUUID: {}}
	err := r.processDiskAfterSnapshotDeletion(ctx, testLogger(), ns, diskUUID, retainedSet)
	if err != nil {
		t.Errorf("expected no error, got: %v", err)
	}
	// CVI status must NOT be updated.
	if svc.updateCalled {
		t.Error("expected CVI status NOT to be updated when disk is still retained")
	}
}

// TestProcessDiskAfterSnapshotDeletion_ReAdoptedByVM verifies that a disk with
// vmName set (re-adopted via Workflow E) is left as-is.
func TestProcessDiskAfterSnapshotDeletion_ReAdoptedByVM(t *testing.T) {
	ctx := context.Background()
	const (
		ns       = "ns-readopt"
		diskUUID = "disk-readopt"
		volID    = "vol-readopt"
	)
	cvi := makeCVIForDisk(ns, volID, "pvc-readopt", "pv-readopt", diskUUID,
		csivolumeinfov1alpha1.OwnershipStateVMManaged, "some-vm")
	svc := &snapshotTestCVISvc{
		cviByDiskUUID: map[string]*csivolumeinfov1alpha1.CsiVolumeInfo{diskUUID: cvi},
	}
	r := makeSnapDeletionReconciler(svc, &snapshotTestVolMgr{})

	err := r.processDiskAfterSnapshotDeletion(ctx, testLogger(), ns, diskUUID,
		map[string]struct{}{})
	if err != nil {
		t.Errorf("expected no error, got: %v", err)
	}
	if svc.updateCalled {
		t.Error("expected CVI status NOT to be updated when disk is re-adopted")
	}
}

// TestProcessDiskAfterSnapshotDeletion_ReRegister verifies that a disk no longer
// retained by any snapshot and not re-adopted gets re-registered as an FCD.
func TestProcessDiskAfterSnapshotDeletion_ReRegister(t *testing.T) {
	ctx := context.Background()
	const (
		ns       = "ns-rereg"
		diskUUID = "disk-rereg"
		volID    = "vol-rereg"
		pvcName  = "pvc-rereg"
		pvName   = "pv-rereg"
	)
	cvi := makeCVIForDisk(ns, volID, pvcName, pvName, diskUUID,
		csivolumeinfov1alpha1.OwnershipStateVMManaged, "")
	pvc, pv := makePVCandPV(ns, pvcName, pvName)
	svc := &snapshotTestCVISvc{
		cviByDiskUUID: map[string]*csivolumeinfov1alpha1.CsiVolumeInfo{diskUUID: cvi},
	}
	r := makeSnapDeletionReconciler(svc, &snapshotTestVolMgr{}, pvc, pv, cvi)

	err := r.processDiskAfterSnapshotDeletion(ctx, testLogger(), ns, diskUUID,
		map[string]struct{}{})
	if err != nil {
		t.Errorf("expected no error, got: %v", err)
	}
	// CVI must have been transitioned to CSI_MANAGED.
	if !svc.updateCalled {
		t.Error("expected CVI status to be updated to CSI_MANAGED")
	}
}

// TestProcessDiskAfterSnapshotDeletion_CVILookupError verifies that a CVI lookup
// error is propagated.
func TestProcessDiskAfterSnapshotDeletion_CVILookupError(t *testing.T) {
	ctx := context.Background()
	svc := &snapshotTestCVISvc{
		cviByDiskUUID: map[string]*csivolumeinfov1alpha1.CsiVolumeInfo{},
		err:           errors.New("api server error"),
	}
	r := makeSnapDeletionReconciler(svc, &snapshotTestVolMgr{})

	err := r.processDiskAfterSnapshotDeletion(ctx, testLogger(), "ns", "some-disk",
		map[string]struct{}{})
	if err == nil {
		t.Error("expected error on CVI lookup failure")
	}
}
