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

package wcp

import (
	"context"
	"testing"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	csivolumeinfosvc "sigs.k8s.io/vsphere-csi-driver/v3/pkg/apis/cnsoperator/csivolumeinfo"
	csivolumeinfov1alpha1 "sigs.k8s.io/vsphere-csi-driver/v3/pkg/apis/cnsoperator/csivolumeinfo/v1alpha1"
	"k8s.io/apimachinery/pkg/runtime"
)

func newCVIScheme() *runtime.Scheme {
	s := runtime.NewScheme()
	_ = csivolumeinfov1alpha1.AddToScheme(s)
	return s
}

func cviSvcWith(cvis ...*csivolumeinfov1alpha1.CsiVolumeInfo) csivolumeinfosvc.CsiVolumeInfoService {
	objs := make([]client.Object, len(cvis))
	for i, c := range cvis {
		objs[i] = c
	}
	c := fake.NewClientBuilder().
		WithScheme(newCVIScheme()).
		WithObjects(objs...).
		WithStatusSubresource(&csivolumeinfov1alpha1.CsiVolumeInfo{}).
		Build()
	return csivolumeinfosvc.NewCsiVolumeInfoService(c)
}

func buildTestCVI(volumeID string, ownership csivolumeinfov1alpha1.OwnershipState) *csivolumeinfov1alpha1.CsiVolumeInfo {
	cvi := &csivolumeinfov1alpha1.CsiVolumeInfo{
		ObjectMeta: metav1.ObjectMeta{
			Name:      csivolumeinfov1alpha1.CVINamePrefix + volumeID,
			Namespace: csivolumeinfov1alpha1.CVINamespace,
		},
		Spec: csivolumeinfov1alpha1.CsiVolumeInfoSpec{VolumeID: volumeID},
	}
	cvi.Status.Ownership = ownership
	return cvi
}

func TestRejectSnapshotIfVMManaged_VMManaged_Rejects(t *testing.T) {
	svc := cviSvcWith(buildTestCVI("vol-001", csivolumeinfov1alpha1.OwnershipStateVMManaged))
	if err := rejectSnapshotIfVMManaged(context.Background(), svc, "vol-001"); err == nil {
		t.Fatal("expected error for VMManaged volume, got nil")
	}
}

func TestRejectSnapshotIfVMManaged_CSIManaged_Allows(t *testing.T) {
	svc := cviSvcWith(buildTestCVI("vol-002", csivolumeinfov1alpha1.OwnershipStateCSIManaged))
	if err := rejectSnapshotIfVMManaged(context.Background(), svc, "vol-002"); err != nil {
		t.Fatalf("expected nil for CSIManaged volume, got: %v", err)
	}
}

func TestRejectSnapshotIfVMManaged_NoCVI_Allows(t *testing.T) {
	svc := cviSvcWith() // empty
	if err := rejectSnapshotIfVMManaged(context.Background(), svc, "vol-003"); err != nil {
		t.Fatalf("expected nil (fail-open) when no CVI, got: %v", err)
	}
}
