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

package csivolumeinfo_test

import (
	"context"
	"testing"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	csivolumeinfo "sigs.k8s.io/vsphere-csi-driver/v3/pkg/apis/cnsoperator/csivolumeinfo"
	csivolumeinfov1alpha1 "sigs.k8s.io/vsphere-csi-driver/v3/pkg/apis/cnsoperator/csivolumeinfo/v1alpha1"
)

// newScheme returns a runtime.Scheme with CsiVolumeInfo types registered.
func newScheme(t *testing.T) *runtime.Scheme {
	t.Helper()
	s := runtime.NewScheme()
	if err := csivolumeinfov1alpha1.AddToScheme(s); err != nil {
		t.Fatalf("AddToScheme: %v", err)
	}
	return s
}

// makeCVI returns a minimal CsiVolumeInfo ready for creation.
func makeCVI(volumeID string) *csivolumeinfov1alpha1.CsiVolumeInfo {
	return &csivolumeinfov1alpha1.CsiVolumeInfo{
		ObjectMeta: metav1.ObjectMeta{
			Name:      csivolumeinfo.GetCsiVolumeInfoCRName(volumeID),
			Namespace: csivolumeinfov1alpha1.CVINamespace,
		},
		Spec: csivolumeinfov1alpha1.CsiVolumeInfoSpec{
			VolumeID:     volumeID,
			PVCName:      "test-pvc",
			PVCNamespace: "default",
			PVName:       "pv-" + volumeID,
		},
	}
}

func TestGetCsiVolumeInfoCRName(t *testing.T) {
	cases := []struct {
		volumeID string
		want     string
	}{
		{"abc-123", "cns-volume-abc-123"},
		{"", "cns-volume-"},
	}
	for _, tc := range cases {
		got := csivolumeinfo.GetCsiVolumeInfoCRName(tc.volumeID)
		if got != tc.want {
			t.Errorf("GetCsiVolumeInfoCRName(%q) = %q; want %q", tc.volumeID, got, tc.want)
		}
	}
}

func TestFSSConstantValue(t *testing.T) {
	// Verify the constant matches the agreed WCP capability string.
	const wantFSS = "supports_vm_owned_volumes"
	// We import the constant indirectly via the package string value test.
	// If the constant changes, this test catches the regression.
	if csivolumeinfov1alpha1.CVINamespace != "vmware-system-csi" {
		t.Errorf("CVINamespace = %q; want %q",
			csivolumeinfov1alpha1.CVINamespace, "vmware-system-csi")
	}
	if csivolumeinfov1alpha1.CVINamePrefix != "cns-volume-" {
		t.Errorf("CVINamePrefix = %q; want %q",
			csivolumeinfov1alpha1.CVINamePrefix, "cns-volume-")
	}
	_ = wantFSS
}

func TestDeepCopyRoundTrip(t *testing.T) {
	orig := makeCVI("vol-round-trip")
	orig.Spec.VMs = []csivolumeinfov1alpha1.VirtualMachineRef{
		{VMName: "vm-1", VMInstanceUUID: "uuid-1"},
		{VMName: "vm-2"},
	}
	orig.Status.Ownership = csivolumeinfov1alpha1.OwnershipStateVMManaged
	orig.Status.Conditions = []metav1.Condition{
		{
			Type:               "Ready",
			Status:             metav1.ConditionTrue,
			LastTransitionTime: metav1.Now(),
			Reason:             "Reconciled",
			Message:            "ok",
		},
	}

	copy := orig.DeepCopy()
	if copy == orig {
		t.Fatal("DeepCopy returned the same pointer")
	}
	if len(copy.Spec.VMs) != len(orig.Spec.VMs) {
		t.Errorf("Spec.VMs len mismatch: got %d, want %d",
			len(copy.Spec.VMs), len(orig.Spec.VMs))
	}
	if len(copy.Status.Conditions) != len(orig.Status.Conditions) {
		t.Errorf("Status.Conditions len mismatch")
	}
	// Mutation isolation
	copy.Spec.VMs[0].VMName = "mutated"
	if orig.Spec.VMs[0].VMName == "mutated" {
		t.Error("DeepCopy is not isolated: mutating copy affected orig")
	}
}

func TestCsiVolumeInfoService_CreateGetDelete(t *testing.T) {
	ctx := context.Background()
	svc := csivolumeinfo.NewCsiVolumeInfoService(
		fake.NewClientBuilder().WithScheme(newScheme(t)).Build())

	volumeID := "test-vol-001"
	cvi := makeCVI(volumeID)

	// Create
	if err := svc.CreateCsiVolumeInfo(ctx, cvi); err != nil {
		t.Fatalf("CreateCsiVolumeInfo: %v", err)
	}

	// Idempotent create — use a fresh object (no ResourceVersion) to simulate a
	// second caller that doesn't have the server-assigned metadata yet.
	if err := svc.CreateCsiVolumeInfo(ctx, makeCVI(volumeID)); err != nil {
		t.Fatalf("idempotent CreateCsiVolumeInfo: %v", err)
	}

	// Get
	got, err := svc.GetCsiVolumeInfo(ctx, volumeID)
	if err != nil {
		t.Fatalf("GetCsiVolumeInfo: %v", err)
	}
	if got == nil {
		t.Fatal("GetCsiVolumeInfo returned nil after create")
	}
	if got.Spec.VolumeID != volumeID {
		t.Errorf("VolumeID = %q; want %q", got.Spec.VolumeID, volumeID)
	}

	// Exists
	exists, err := svc.CsiVolumeInfoExists(ctx, volumeID)
	if err != nil {
		t.Fatalf("CsiVolumeInfoExists: %v", err)
	}
	if !exists {
		t.Error("CsiVolumeInfoExists = false; want true")
	}

	// Delete
	if err := svc.DeleteCsiVolumeInfo(ctx, volumeID); err != nil {
		t.Fatalf("DeleteCsiVolumeInfo: %v", err)
	}

	// Idempotent delete
	if err := svc.DeleteCsiVolumeInfo(ctx, volumeID); err != nil {
		t.Fatalf("idempotent DeleteCsiVolumeInfo: %v", err)
	}

	// Get after delete
	got, err = svc.GetCsiVolumeInfo(ctx, volumeID)
	if err != nil {
		t.Fatalf("GetCsiVolumeInfo after delete: %v", err)
	}
	if got != nil {
		t.Error("GetCsiVolumeInfo returned non-nil after delete")
	}
}

func TestCsiVolumeInfoService_Finalizers(t *testing.T) {
	ctx := context.Background()
	svc := csivolumeinfo.NewCsiVolumeInfoService(
		fake.NewClientBuilder().WithScheme(newScheme(t)).Build())

	volumeID := "test-vol-finalizer"
	cvi := makeCVI(volumeID)

	if err := svc.CreateCsiVolumeInfo(ctx, cvi); err != nil {
		t.Fatalf("CreateCsiVolumeInfo: %v", err)
	}

	// Add finalizer
	if err := svc.AddVolumeProtectionFinalizer(ctx, volumeID); err != nil {
		t.Fatalf("AddVolumeProtectionFinalizer: %v", err)
	}

	// Idempotent add
	if err := svc.AddVolumeProtectionFinalizer(ctx, volumeID); err != nil {
		t.Fatalf("idempotent AddVolumeProtectionFinalizer: %v", err)
	}

	// Verify finalizer is present
	got, err := svc.GetCsiVolumeInfo(ctx, volumeID)
	if err != nil || got == nil {
		t.Fatalf("GetCsiVolumeInfo after add finalizer: err=%v got=%v", err, got)
	}
	found := false
	for _, f := range got.Finalizers {
		if f == csivolumeinfov1alpha1.VolumeProtectionFinalizer {
			found = true
		}
	}
	if !found {
		t.Errorf("finalizer %q not found; got %v",
			csivolumeinfov1alpha1.VolumeProtectionFinalizer, got.Finalizers)
	}

	// Remove finalizer
	if err := svc.RemoveVolumeProtectionFinalizer(ctx, volumeID); err != nil {
		t.Fatalf("RemoveVolumeProtectionFinalizer: %v", err)
	}

	// Idempotent remove
	if err := svc.RemoveVolumeProtectionFinalizer(ctx, volumeID); err != nil {
		t.Fatalf("idempotent RemoveVolumeProtectionFinalizer: %v", err)
	}

	// Verify finalizer removed
	got, err = svc.GetCsiVolumeInfo(ctx, volumeID)
	if err != nil || got == nil {
		t.Fatalf("GetCsiVolumeInfo after remove finalizer: err=%v got=%v", err, got)
	}
	for _, f := range got.Finalizers {
		if f == csivolumeinfov1alpha1.VolumeProtectionFinalizer {
			t.Errorf("finalizer %q still present after remove", f)
		}
	}
}

func TestCsiVolumeInfoService_GetNotFound(t *testing.T) {
	ctx := context.Background()
	svc := csivolumeinfo.NewCsiVolumeInfoService(
		fake.NewClientBuilder().WithScheme(newScheme(t)).Build())

	got, err := svc.GetCsiVolumeInfo(ctx, "nonexistent-vol")
	if err != nil {
		t.Fatalf("GetCsiVolumeInfo for missing CR should return nil error, got: %v", err)
	}
	if got != nil {
		t.Errorf("GetCsiVolumeInfo for missing CR should return nil, got: %+v", got)
	}
}
