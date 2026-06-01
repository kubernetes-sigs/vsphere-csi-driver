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
	"testing"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	csivolumeinfov1alpha1 "sigs.k8s.io/vsphere-csi-driver/v3/pkg/apis/cnsoperator/csivolumeinfo/v1alpha1"
)

// newTestScheme builds a runtime.Scheme with CsiVolumeInfo types registered.
func newTestScheme(t *testing.T) *runtime.Scheme {
	t.Helper()
	s := runtime.NewScheme()
	if err := csivolumeinfov1alpha1.AddToScheme(s); err != nil {
		t.Fatalf("failed to add CsiVolumeInfo to scheme: %v", err)
	}
	return s
}

// newTestService creates a CsiVolumeInfoService backed by a fake client.
func newTestService(t *testing.T, objs ...runtime.Object) *csiVolumeInfoSvc {
	t.Helper()
	s := newTestScheme(t)
	b := fake.NewClientBuilder().WithScheme(s).WithStatusSubresource(&csivolumeinfov1alpha1.CsiVolumeInfo{})
	if len(objs) > 0 {
		b = b.WithRuntimeObjects(objs...)
	}
	return &csiVolumeInfoSvc{k8sClient: b.Build()}
}

// buildCVI is a helper that constructs a minimal CsiVolumeInfo for tests.
func buildCVI(namespace, volumeID, diskUUID, pvcName, pvName string) *csivolumeinfov1alpha1.CsiVolumeInfo {
	return &csivolumeinfov1alpha1.CsiVolumeInfo{
		ObjectMeta: metav1.ObjectMeta{
			Name:      GetCsiVolumeInfoCRName(volumeID),
			Namespace: namespace,
			Labels: map[string]string{
				csivolumeinfov1alpha1.LabelDiskUUID: diskUUID,
			},
		},
		Spec: csivolumeinfov1alpha1.CsiVolumeInfoSpec{
			VolumeID: volumeID,
			PVCName:  pvcName,
			PVName:   pvName,
		},
		Status: csivolumeinfov1alpha1.CsiVolumeInfoStatus{
			OwnershipState: csivolumeinfov1alpha1.OwnershipStateCSIManaged,
			DiskUUID:       diskUUID,
		},
	}
}

// TestCsiVolumeInfoCRName verifies the deterministic naming convention.
func TestCsiVolumeInfoCRName(t *testing.T) {
	cases := []struct {
		volumeID string
		want     string
	}{
		{"abc-d3", "csi-volume-info-abc-d3"},
		{"some-long-volume-id-1234", "csi-volume-info-some-long-volume-id-1234"},
		{"", "csi-volume-info-"},
	}
	for _, tc := range cases {
		got := GetCsiVolumeInfoCRName(tc.volumeID)
		if got != tc.want {
			t.Errorf("GetCsiVolumeInfoCRName(%q) = %q, want %q", tc.volumeID, got, tc.want)
		}
	}
}

// TestOwnershipStateEnum verifies all four OwnershipState constants are non-empty.
func TestOwnershipStateEnum(t *testing.T) {
	states := []csivolumeinfov1alpha1.OwnershipState{
		csivolumeinfov1alpha1.OwnershipStateCSIManaged,
		csivolumeinfov1alpha1.OwnershipStateTransferringToVM,
		csivolumeinfov1alpha1.OwnershipStateVMManaged,
		csivolumeinfov1alpha1.OwnershipStateTransferringToCSI,
	}
	for _, s := range states {
		if string(s) == "" {
			t.Errorf("OwnershipState constant is empty string")
		}
	}
}

// TestCreateCsiVolumeInfo creates a CVI and verifies it is retrievable.
func TestCreateCsiVolumeInfo(t *testing.T) {
	ctx := context.Background()
	svc := newTestService(t)
	cvi := buildCVI("testns", "vol-1", "disk-uuid-1", "pvc-1", "pv-1")

	if err := svc.CreateCsiVolumeInfo(ctx, cvi); err != nil {
		t.Fatalf("CreateCsiVolumeInfo: %v", err)
	}

	got, err := svc.GetCsiVolumeInfo(ctx, "testns", "vol-1")
	if err != nil {
		t.Fatalf("GetCsiVolumeInfo: %v", err)
	}
	if got == nil {
		t.Fatal("expected CVI to exist, got nil")
	}
	if got.Spec.VolumeID != "vol-1" {
		t.Errorf("Spec.VolumeID = %q, want %q", got.Spec.VolumeID, "vol-1")
	}
	if got.Spec.PVCName != "pvc-1" {
		t.Errorf("Spec.PVCName = %q, want %q", got.Spec.PVCName, "pvc-1")
	}
}

// TestCreateCsiVolumeInfo_Idempotent verifies that creating a CVI that already
// exists returns no error (AlreadyExists is silently swallowed).
func TestCreateCsiVolumeInfo_Idempotent(t *testing.T) {
	ctx := context.Background()
	// Pre-populate the fake store with an existing CVI.
	existing := buildCVI("testns", "vol-2", "disk-uuid-2", "pvc-2", "pv-2")
	svc := newTestService(t, existing)

	// Build a fresh object (no ResourceVersion) that represents a second create
	// attempt for the same volume.
	duplicate := buildCVI("testns", "vol-2", "disk-uuid-2", "pvc-2", "pv-2")
	if err := svc.CreateCsiVolumeInfo(ctx, duplicate); err != nil {
		t.Errorf("second CreateCsiVolumeInfo should be a no-op, got: %v", err)
	}
}

// TestGetCsiVolumeInfo_NotFound verifies nil is returned for a missing CVI.
func TestGetCsiVolumeInfo_NotFound(t *testing.T) {
	ctx := context.Background()
	svc := newTestService(t)

	got, err := svc.GetCsiVolumeInfo(ctx, "testns", "nonexistent")
	if err != nil {
		t.Fatalf("GetCsiVolumeInfo for missing CVI returned error: %v", err)
	}
	if got != nil {
		t.Errorf("expected nil for missing CVI, got %+v", got)
	}
}

// TestGetCsiVolumeInfoByDiskUUID locates a CVI by disk-uuid label.
func TestGetCsiVolumeInfoByDiskUUID(t *testing.T) {
	ctx := context.Background()
	cvi := buildCVI("testns", "vol-3", "6000C29-d3", "pvc-3", "pv-3")
	svc := newTestService(t, cvi)

	got, err := svc.GetCsiVolumeInfoByDiskUUID(ctx, "testns", "6000C29-d3")
	if err != nil {
		t.Fatalf("GetCsiVolumeInfoByDiskUUID: %v", err)
	}
	if got == nil {
		t.Fatal("expected CVI, got nil")
	}
	if got.Spec.VolumeID != "vol-3" {
		t.Errorf("VolumeID = %q, want %q", got.Spec.VolumeID, "vol-3")
	}
}

// TestGetCsiVolumeInfoByDiskUUID_NotFound returns nil without error.
func TestGetCsiVolumeInfoByDiskUUID_NotFound(t *testing.T) {
	ctx := context.Background()
	svc := newTestService(t)

	got, err := svc.GetCsiVolumeInfoByDiskUUID(ctx, "testns", "no-such-uuid")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if got != nil {
		t.Errorf("expected nil for missing diskUUID, got %+v", got)
	}
}

// TestUpdateCsiVolumeInfoStatus transitions ownershipState and verifies spec
// is unchanged.
func TestUpdateCsiVolumeInfoStatus(t *testing.T) {
	ctx := context.Background()
	cvi := buildCVI("testns", "vol-4", "disk-uuid-4", "pvc-4", "pv-4")
	svc := newTestService(t, cvi)

	// Fetch fresh copy (needed for ResourceVersion).
	fresh, err := svc.GetCsiVolumeInfo(ctx, "testns", "vol-4")
	if err != nil || fresh == nil {
		t.Fatalf("GetCsiVolumeInfo: %v, got %v", err, fresh)
	}

	fresh.Status.OwnershipState = csivolumeinfov1alpha1.OwnershipStateTransferringToVM
	fresh.Status.VMName = "my-vm"
	fresh.Status.VMInstanceUUID = "502e71fa"

	if err := svc.UpdateCsiVolumeInfoStatus(ctx, fresh); err != nil {
		t.Fatalf("UpdateCsiVolumeInfoStatus: %v", err)
	}

	updated, _ := svc.GetCsiVolumeInfo(ctx, "testns", "vol-4")
	if updated.Status.OwnershipState != csivolumeinfov1alpha1.OwnershipStateTransferringToVM {
		t.Errorf("OwnershipState = %q, want TRANSFERRING_TO_VM", updated.Status.OwnershipState)
	}
	if updated.Status.VMName != "my-vm" {
		t.Errorf("VMName = %q, want my-vm", updated.Status.VMName)
	}
	// Spec must not have changed.
	if updated.Spec.VolumeID != "vol-4" {
		t.Errorf("Spec.VolumeID changed: %q", updated.Spec.VolumeID)
	}
}

// TestDeleteCsiVolumeInfo creates, deletes, and verifies the CVI is gone.
func TestDeleteCsiVolumeInfo(t *testing.T) {
	ctx := context.Background()
	cvi := buildCVI("testns", "vol-5", "disk-uuid-5", "pvc-5", "pv-5")
	svc := newTestService(t, cvi)

	if err := svc.DeleteCsiVolumeInfo(ctx, "testns", "vol-5"); err != nil {
		t.Fatalf("DeleteCsiVolumeInfo: %v", err)
	}

	got, _ := svc.GetCsiVolumeInfo(ctx, "testns", "vol-5")
	if got != nil {
		t.Error("expected CVI to be deleted, but still found")
	}
}

// TestDeleteCsiVolumeInfo_Idempotent verifies that deleting a non-existent
// CVI returns no error.
func TestDeleteCsiVolumeInfo_Idempotent(t *testing.T) {
	ctx := context.Background()
	svc := newTestService(t)

	if err := svc.DeleteCsiVolumeInfo(ctx, "testns", "nonexistent"); err != nil {
		t.Errorf("DeleteCsiVolumeInfo on missing CVI should be no-op, got: %v", err)
	}
}

// TestCsiVolumeInfoExists verifies exists/not-exists semantics.
func TestCsiVolumeInfoExists(t *testing.T) {
	ctx := context.Background()
	cvi := buildCVI("testns", "vol-6", "disk-uuid-6", "pvc-6", "pv-6")
	svc := newTestService(t, cvi)

	exists, err := svc.CsiVolumeInfoExists(ctx, "testns", "vol-6")
	if err != nil {
		t.Fatalf("CsiVolumeInfoExists: %v", err)
	}
	if !exists {
		t.Error("expected CVI to exist")
	}

	if err := svc.DeleteCsiVolumeInfo(ctx, "testns", "vol-6"); err != nil {
		t.Fatalf("DeleteCsiVolumeInfo: %v", err)
	}
	exists, err = svc.CsiVolumeInfoExists(ctx, "testns", "vol-6")
	if err != nil {
		t.Fatalf("CsiVolumeInfoExists after delete: %v", err)
	}
	if exists {
		t.Error("expected CVI to be gone after delete")
	}
}

// TestCsiVolumeInfoDeepCopy verifies that DeepCopy produces an independent
// copy (no shared slice pointers).
func TestCsiVolumeInfoDeepCopy(t *testing.T) {
	cvi := buildCVI("testns", "vol-7", "disk-uuid-7", "pvc-7", "pv-7")
	cvi.Status.Conditions = []metav1.Condition{
		{
			Type:               "Ready",
			Status:             metav1.ConditionTrue,
			Reason:             "OK",
			Message:            "all good",
			LastTransitionTime: metav1.Now(),
		},
	}

	copy := cvi.DeepCopy()

	// Mutating the copy's Conditions must not affect the original.
	copy.Status.Conditions[0].Message = "mutated"
	if cvi.Status.Conditions[0].Message == "mutated" {
		t.Error("DeepCopy shares Conditions slice with original")
	}

	// Mutating the copy's spec must not affect original.
	copy.Spec.VolumeID = "different"
	if cvi.Spec.VolumeID == "different" {
		t.Error("DeepCopy shares Spec with original")
	}
}

// TestCsiVolumeInfoLabels verifies that the disk-uuid label is set correctly
// and the volume-ownership PVC label constants are defined.
func TestCsiVolumeInfoLabels(t *testing.T) {
	cvi := buildCVI("testns", "vol-8", "6000C29-abc", "pvc-8", "pv-8")
	if got := cvi.Labels[csivolumeinfov1alpha1.LabelDiskUUID]; got != "6000C29-abc" {
		t.Errorf("LabelDiskUUID = %q, want %q", got, "6000C29-abc")
	}

	ownershipLabels := []string{
		csivolumeinfov1alpha1.OwnershipLabelVMOwned,
		csivolumeinfov1alpha1.OwnershipLabelCSIOwned,
		csivolumeinfov1alpha1.OwnershipLabelRetainedBySnapshot,
	}
	for _, l := range ownershipLabels {
		if l == "" {
			t.Errorf("ownership label constant is empty string")
		}
	}
}

// TestCsiVolumeInfoOwnerReference verifies that the PV ownerReference fields
// can be set correctly on a CVI.
func TestCsiVolumeInfoOwnerReference(t *testing.T) {
	cvi := buildCVI("testns", "vol-9", "disk-uuid-9", "pvc-9", "pv-9")
	controller := true
	blockOwnerDeletion := true
	cvi.OwnerReferences = []metav1.OwnerReference{
		{
			APIVersion:         "v1",
			Kind:               "PersistentVolume",
			Name:               "pv-9",
			UID:                "some-uid",
			Controller:         &controller,
			BlockOwnerDeletion: &blockOwnerDeletion,
		},
	}

	if len(cvi.OwnerReferences) != 1 {
		t.Fatalf("expected 1 ownerRef, got %d", len(cvi.OwnerReferences))
	}
	ref := cvi.OwnerReferences[0]
	if ref.Kind != "PersistentVolume" {
		t.Errorf("Kind = %q, want PersistentVolume", ref.Kind)
	}
	if ref.Controller == nil || !*ref.Controller {
		t.Error("Controller must be true")
	}
	if ref.BlockOwnerDeletion == nil || !*ref.BlockOwnerDeletion {
		t.Error("BlockOwnerDeletion must be true")
	}
}

// TestCsiVolumeInfoFinalizer verifies that the CVIProtectionFinalizer can be
// added to and removed from a CVI.
func TestCsiVolumeInfoFinalizer(t *testing.T) {
	cvi := buildCVI("testns", "vol-10", "disk-uuid-10", "pvc-10", "pv-10")

	// Add finalizer.
	cvi.Finalizers = append(cvi.Finalizers, csivolumeinfov1alpha1.CVIProtectionFinalizer)
	found := false
	for _, f := range cvi.Finalizers {
		if f == csivolumeinfov1alpha1.CVIProtectionFinalizer {
			found = true
			break
		}
	}
	if !found {
		t.Error("CVIProtectionFinalizer not found after add")
	}

	// Remove finalizer.
	updated := make([]string, 0, len(cvi.Finalizers))
	for _, f := range cvi.Finalizers {
		if f != csivolumeinfov1alpha1.CVIProtectionFinalizer {
			updated = append(updated, f)
		}
	}
	cvi.Finalizers = updated
	for _, f := range cvi.Finalizers {
		if f == csivolumeinfov1alpha1.CVIProtectionFinalizer {
			t.Error("CVIProtectionFinalizer still present after remove")
		}
	}
}

// TestCsiVolumeInfoSpec_Immutability documents that the spec fields are treated
// as immutable by convention (enforcement is in the admission webhook, Phase 5).
func TestCsiVolumeInfoSpec_Immutability(t *testing.T) {
	cvi := buildCVI("testns", "vol-11", "disk-uuid-11", "pvc-11", "pv-11")
	originalVolumeID := cvi.Spec.VolumeID

	// Ensure DeepCopy preserves spec fields correctly.
	copy := cvi.DeepCopy()
	if copy.Spec.VolumeID != originalVolumeID {
		t.Errorf("DeepCopy changed VolumeID: got %q want %q",
			copy.Spec.VolumeID, originalVolumeID)
	}
	if copy.Spec.PVCName != cvi.Spec.PVCName {
		t.Errorf("DeepCopy changed PVCName")
	}
	if copy.Spec.PVName != cvi.Spec.PVName {
		t.Errorf("DeepCopy changed PVName")
	}
}
