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
	"testing"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	bav1alpha1 "sigs.k8s.io/vsphere-csi-driver/v3/pkg/apis/cnsoperator/cnsnodevmbatchattachment/v1alpha1"
)

// TestIsSnapshotRevertInducedDetach_True confirms that the
// DroppedBySnapshotRevert reason triggers a true result.
func TestIsSnapshotRevertInducedDetach_True(t *testing.T) {
	conditions := []metav1.Condition{
		{
			Type:   bav1alpha1.ConditionDetached,
			Status: "True",
			Reason: bav1alpha1.ReasonDroppedBySnapshotRevert,
		},
	}
	if !IsSnapshotRevertInducedDetach(conditions) {
		t.Error("expected true for DroppedBySnapshotRevert condition")
	}
}

// TestIsSnapshotRevertInducedDetach_False confirms that other detach reasons
// return false.
func TestIsSnapshotRevertInducedDetach_False(t *testing.T) {
	conditions := []metav1.Condition{
		{
			Type:   bav1alpha1.ConditionDetached,
			Status: "True",
			Reason: bav1alpha1.ReasonDetachFailed,
		},
	}
	if IsSnapshotRevertInducedDetach(conditions) {
		t.Error("expected false for DetachFailed condition")
	}
}

// TestIsSnapshotRevertInducedDetach_Empty returns false for an empty
// condition slice.
func TestIsSnapshotRevertInducedDetach_Empty(t *testing.T) {
	if IsSnapshotRevertInducedDetach(nil) {
		t.Error("expected false for nil conditions")
	}
}

// TestIsSnapshotRevertInducedDetach_WrongType ensures that a condition with the
// DroppedBySnapshotRevert reason but the wrong type is not matched.
func TestIsSnapshotRevertInducedDetach_WrongType(t *testing.T) {
	conditions := []metav1.Condition{
		{
			Type:   "SomeOtherCondition",
			Status: "True",
			Reason: bav1alpha1.ReasonDroppedBySnapshotRevert,
		},
	}
	if IsSnapshotRevertInducedDetach(conditions) {
		t.Error("expected false when condition type does not match")
	}
}

// TestIsSnapshotRevertInducedDetach_StatusFalse ensures that a matching type and
// reason with Status=False is not treated as a revert-induced detach.
func TestIsSnapshotRevertInducedDetach_StatusFalse(t *testing.T) {
	conditions := []metav1.Condition{
		{
			Type:   bav1alpha1.ConditionDetached,
			Status: "False",
			Reason: bav1alpha1.ReasonDroppedBySnapshotRevert,
		},
	}
	if IsSnapshotRevertInducedDetach(conditions) {
		t.Error("expected false when Status is False")
	}
}

// TestIsSnapshotRevertInducedDetach_MultipleConditions verifies correct detection
// when the revert condition is mixed with other conditions.
func TestIsSnapshotRevertInducedDetach_MultipleConditions(t *testing.T) {
	conditions := []metav1.Condition{
		{
			Type:   bav1alpha1.ConditionAttached,
			Status: "True",
			Reason: bav1alpha1.ReasonAttachFailed, // using a real constant
		},
		{
			Type:   bav1alpha1.ConditionDetached,
			Status: "True",
			Reason: bav1alpha1.ReasonDroppedBySnapshotRevert,
		},
	}
	if !IsSnapshotRevertInducedDetach(conditions) {
		t.Error("expected true when DroppedBySnapshotRevert condition is present alongside others")
	}
}
