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

package v1alpha1

import (
	"testing"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// TestPhaseConstants verifies phase constant string values so a rename is caught immediately.
func TestPhaseConstants(t *testing.T) {
	cases := []struct {
		name string
		got  VKSRegisterVolumePhase
		want VKSRegisterVolumePhase
	}{
		{"Pending", VKSRegisterVolumePhasePending, "Pending"},
		{
			"WaitingForSupervisorRegistration",
			VKSRegisterVolumePhaseWaitingForSupervisorRegistration,
			"WaitingForSupervisorRegistration",
		},
		{"WaitingForSupervisorBinding", VKSRegisterVolumePhaseWaitingForSupervisorBinding, "WaitingForSupervisorBinding"},
		{"CreatingGuestPV", VKSRegisterVolumePhaseCreatingGuestPV, "CreatingGuestPV"},
		{"WaitingForGuestPVCBound", VKSRegisterVolumePhaseWaitingForGuestPVCBound, "WaitingForGuestPVCBound"},
		{"Registered", VKSRegisterVolumePhaseRegistered, "Registered"},
		{"Failed", VKSRegisterVolumePhaseFailed, "Failed"},
	}
	for _, tc := range cases {
		if tc.got != tc.want {
			t.Errorf("phase %s: got %q, want %q", tc.name, tc.got, tc.want)
		}
	}
}

// TestDeepCopySpec verifies VKSRegisterVolumeSpec.DeepCopy produces an independent copy.
func TestDeepCopySpec(t *testing.T) {
	orig := &VKSRegisterVolumeSpec{
		PVCName:               "my-pvc",
		CnsRegisterVolumeName: "sv-cr-name",
	}
	cp := orig.DeepCopy()
	if cp == orig {
		t.Fatal("DeepCopy returned the same pointer")
	}
	if *cp != *orig {
		t.Fatalf("DeepCopy content mismatch: got %+v, want %+v", *cp, *orig)
	}
	// Mutating the copy must not affect the original.
	cp.PVCName = "changed"
	if orig.PVCName != "my-pvc" {
		t.Errorf("mutation of copy affected original: orig.PVCName = %q", orig.PVCName)
	}
}

// TestDeepCopySpecNil verifies that DeepCopy on a nil pointer returns nil.
func TestDeepCopySpecNil(t *testing.T) {
	var s *VKSRegisterVolumeSpec
	if s.DeepCopy() != nil {
		t.Error("expected nil from DeepCopy on nil VKSRegisterVolumeSpec")
	}
}

// TestDeepCopyStatus verifies VKSRegisterVolumeStatus.DeepCopy produces an independent copy.
func TestDeepCopyStatus(t *testing.T) {
	orig := &VKSRegisterVolumeStatus{
		Phase:      VKSRegisterVolumePhaseRegistered,
		Registered: true,
		Error:      "",
	}
	cp := orig.DeepCopy()
	if cp == orig {
		t.Fatal("DeepCopy returned the same pointer")
	}
	if *cp != *orig {
		t.Fatalf("DeepCopy content mismatch: got %+v, want %+v", *cp, *orig)
	}
	cp.Registered = false
	if !orig.Registered {
		t.Error("mutation of copy affected original: orig.Registered")
	}
}

// TestDeepCopyStatusNil verifies that DeepCopy on a nil pointer returns nil.
func TestDeepCopyStatusNil(t *testing.T) {
	var s *VKSRegisterVolumeStatus
	if s.DeepCopy() != nil {
		t.Error("expected nil from DeepCopy on nil VKSRegisterVolumeStatus")
	}
}

// TestDeepCopyVKSRegisterVolume verifies full object deep copy including ObjectMeta.
func TestDeepCopyVKSRegisterVolume(t *testing.T) {
	orig := &VKSRegisterVolume{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "restore-db-vol",
			Namespace: "my-app",
			Labels:    map[string]string{"env": "test"},
		},
		Spec: VKSRegisterVolumeSpec{
			PVCName:               "db-data-pvc",
			CnsRegisterVolumeName: "abc-reg-deadbeef12345678",
		},
		Status: VKSRegisterVolumeStatus{
			Phase:      VKSRegisterVolumePhaseRegistered,
			Registered: true,
		},
	}

	cp := orig.DeepCopy()
	if cp == orig {
		t.Fatal("DeepCopy returned the same pointer")
	}
	if cp.Name != orig.Name || cp.Namespace != orig.Namespace {
		t.Errorf("ObjectMeta mismatch: got %s/%s, want %s/%s", cp.Namespace, cp.Name, orig.Namespace, orig.Name)
	}
	if cp.Spec != orig.Spec {
		t.Errorf("Spec mismatch: got %+v, want %+v", cp.Spec, orig.Spec)
	}
	if cp.Status != orig.Status {
		t.Errorf("Status mismatch: got %+v, want %+v", cp.Status, orig.Status)
	}

	// Labels map must be an independent copy.
	cp.Labels["env"] = "prod"
	if orig.Labels["env"] != "test" {
		t.Errorf("mutating copy Labels affected original: orig.Labels[env] = %q", orig.Labels["env"])
	}
}

// TestDeepCopyVKSRegisterVolumeNil verifies nil input.
func TestDeepCopyVKSRegisterVolumeNil(t *testing.T) {
	var v *VKSRegisterVolume
	if v.DeepCopy() != nil {
		t.Error("expected nil from DeepCopy on nil VKSRegisterVolume")
	}
}

// TestDeepCopyVKSRegisterVolumeList verifies list deep copy with multiple items.
func TestDeepCopyVKSRegisterVolumeList(t *testing.T) {
	orig := &VKSRegisterVolumeList{
		Items: []VKSRegisterVolume{
			{
				ObjectMeta: metav1.ObjectMeta{Name: "vol-1", Namespace: "ns-a"},
				Spec:       VKSRegisterVolumeSpec{PVCName: "pvc-1"},
			},
			{
				ObjectMeta: metav1.ObjectMeta{Name: "vol-2", Namespace: "ns-b"},
				Spec:       VKSRegisterVolumeSpec{PVCName: "pvc-2"},
			},
		},
	}

	cp := orig.DeepCopy()
	if cp == orig {
		t.Fatal("DeepCopy returned the same pointer")
	}
	if len(cp.Items) != len(orig.Items) {
		t.Fatalf("Items length mismatch: got %d, want %d", len(cp.Items), len(orig.Items))
	}

	// Mutating an item in the copy must not affect the original.
	cp.Items[0].Spec.PVCName = "changed"
	if orig.Items[0].Spec.PVCName != "pvc-1" {
		t.Errorf("mutation of copy Items[0] affected original: %q", orig.Items[0].Spec.PVCName)
	}
}

// TestDeepCopyVKSRegisterVolumeListNilItems verifies that a list with nil Items is handled.
func TestDeepCopyVKSRegisterVolumeListNilItems(t *testing.T) {
	orig := &VKSRegisterVolumeList{}
	cp := orig.DeepCopy()
	if cp == nil {
		t.Fatal("expected non-nil copy")
	}
	if cp.Items != nil {
		t.Errorf("expected nil Items in copy, got %v", cp.Items)
	}
}

// TestDeepCopyVKSRegisterVolumeListNil verifies nil input.
func TestDeepCopyVKSRegisterVolumeListNil(t *testing.T) {
	var l *VKSRegisterVolumeList
	if l.DeepCopy() != nil {
		t.Error("expected nil from DeepCopy on nil VKSRegisterVolumeList")
	}
}

// TestDeepCopyObjectInterface verifies that DeepCopyObject returns a runtime.Object.
func TestDeepCopyObjectInterface(t *testing.T) {
	v := &VKSRegisterVolume{
		ObjectMeta: metav1.ObjectMeta{Name: "test"},
	}
	obj := v.DeepCopyObject()
	if obj == nil {
		t.Fatal("DeepCopyObject returned nil")
	}
	got, ok := obj.(*VKSRegisterVolume)
	if !ok {
		t.Fatalf("DeepCopyObject type: got %T, want *VKSRegisterVolume", obj)
	}
	if got.Name != v.Name {
		t.Errorf("DeepCopyObject Name: got %q, want %q", got.Name, v.Name)
	}
}
