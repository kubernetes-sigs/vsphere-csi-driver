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

package syncer

import (
	"testing"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	k8stypes "k8s.io/apimachinery/pkg/types"

	csivolumeinfov1alpha1 "sigs.k8s.io/vsphere-csi-driver/v3/pkg/apis/cnsoperator/csivolumeinfo/v1alpha1"
)

// TestHasPVOwnerReference verifies the helper that checks whether a CVI already
// carries a PersistentVolume ownerReference with the expected UID.
func TestHasPVOwnerReference_Present(t *testing.T) {
	cvi := &csivolumeinfov1alpha1.CsiVolumeInfo{
		ObjectMeta: metav1.ObjectMeta{
			OwnerReferences: []metav1.OwnerReference{
				{
					Kind: "PersistentVolume",
					UID:  k8stypes.UID("pv-uid-123"),
				},
			},
		},
	}
	if !hasPVOwnerReference(cvi, "pv-uid-123") {
		t.Error("expected hasPVOwnerReference to return true when ownerRef is present")
	}
}

func TestHasPVOwnerReference_Absent(t *testing.T) {
	cvi := &csivolumeinfov1alpha1.CsiVolumeInfo{
		ObjectMeta: metav1.ObjectMeta{},
	}
	if hasPVOwnerReference(cvi, "pv-uid-123") {
		t.Error("expected hasPVOwnerReference to return false when no ownerRefs exist")
	}
}

func TestHasPVOwnerReference_WrongKind(t *testing.T) {
	cvi := &csivolumeinfov1alpha1.CsiVolumeInfo{
		ObjectMeta: metav1.ObjectMeta{
			OwnerReferences: []metav1.OwnerReference{
				{
					Kind: "PersistentVolumeClaim",
					UID:  k8stypes.UID("pv-uid-123"),
				},
			},
		},
	}
	if hasPVOwnerReference(cvi, "pv-uid-123") {
		t.Error("expected hasPVOwnerReference to return false when kind is not PersistentVolume")
	}
}

func TestHasPVOwnerReference_WrongUID(t *testing.T) {
	cvi := &csivolumeinfov1alpha1.CsiVolumeInfo{
		ObjectMeta: metav1.ObjectMeta{
			OwnerReferences: []metav1.OwnerReference{
				{
					Kind: "PersistentVolume",
					UID:  k8stypes.UID("different-uid"),
				},
			},
		},
	}
	if hasPVOwnerReference(cvi, "pv-uid-123") {
		t.Error("expected hasPVOwnerReference to return false when UID does not match")
	}
}

func TestHasPVOwnerReference_MultipleRefs_MatchFound(t *testing.T) {
	cvi := &csivolumeinfov1alpha1.CsiVolumeInfo{
		ObjectMeta: metav1.ObjectMeta{
			OwnerReferences: []metav1.OwnerReference{
				{Kind: "PersistentVolumeClaim", UID: k8stypes.UID("pvc-uid")},
				{Kind: "PersistentVolume", UID: k8stypes.UID("pv-uid-123")},
			},
		},
	}
	if !hasPVOwnerReference(cvi, "pv-uid-123") {
		t.Error("expected hasPVOwnerReference to return true when one of the refs matches")
	}
}

func TestBoolPtr(t *testing.T) {
	if boolPtr(true) == nil || !*boolPtr(true) {
		t.Error("boolPtr(true) should return non-nil pointer to true")
	}
	if boolPtr(false) == nil || *boolPtr(false) {
		t.Error("boolPtr(false) should return non-nil pointer to false")
	}
}
