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

package vksregistervolume

import (
	"reflect"
	"testing"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	vksregistervolumev1alpha1 "sigs.k8s.io/vsphere-csi-driver/v3/pkg/apis/cnsoperator/vksregistervolume/v1alpha1"
	cnsoperatortypes "sigs.k8s.io/vsphere-csi-driver/v3/pkg/syncer/cnsoperator/types"
)

const (
	testNamespace         = "my-app"
	testCRName            = "restore-db-vol"
	testPVCName           = "db-data-pvc"
	testGuestPVName       = "pv-restore-db-vol"
	testSupervisorPVCName = "tkc-uid-reg-deadbeef12345678"
)

func testInstance() *vksregistervolumev1alpha1.VKSRegisterVolume {
	return &vksregistervolumev1alpha1.VKSRegisterVolume{
		ObjectMeta: metav1.ObjectMeta{Namespace: testNamespace, Name: testCRName},
		Spec: vksregistervolumev1alpha1.VKSRegisterVolumeSpec{
			PVCName:               testPVCName,
			CnsRegisterVolumeName: "tkc-uid-reg-deadbeef12345678",
		},
	}
}

// ── TestBuildGuestPV ──────────────────────────────────────────────────────────────────────────

func TestBuildGuestPV(t *testing.T) {
	instance := testInstance()

	cases := []struct {
		name                string
		pvc                 *corev1.PersistentVolumeClaim
		volumeMode          corev1.PersistentVolumeMode
		accessibleTopology  []map[string]string
		wantFSType          string
		wantNodeAffinityNil bool
	}{
		{
			name:                "Filesystem volume, no topology → nodeAffinity nil, fsType ext4",
			pvc:                 pendingPVC(testNamespace, testPVCName, "guest-sc"),
			volumeMode:          corev1.PersistentVolumeFilesystem,
			accessibleTopology:  nil,
			wantFSType:          "ext4",
			wantNodeAffinityNil: true,
		},
		{
			name:       "Filesystem volume, with topology → nodeAffinity populated",
			pvc:        pendingPVC(testNamespace, testPVCName, "guest-sc"),
			volumeMode: corev1.PersistentVolumeFilesystem,
			accessibleTopology: []map[string]string{
				{"topology.kubernetes.io/zone": "zone-a"},
			},
			wantFSType:          "ext4",
			wantNodeAffinityNil: false,
		},
		{
			name:                "Block volume → no fsType, nodeAffinity nil",
			pvc:                 pendingPVC(testNamespace, testPVCName, "guest-sc"),
			volumeMode:          corev1.PersistentVolumeBlock,
			accessibleTopology:  nil,
			wantFSType:          "",
			wantNodeAffinityNil: true,
		},
	}

	for _, tc := range cases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			pv := buildGuestPV(tc.pvc, testGuestPVName, testSupervisorPVCName, tc.volumeMode,
				tc.accessibleTopology, instance)

			if pv.Name != testGuestPVName {
				t.Errorf("PV name = %q, want %q", pv.Name, testGuestPVName)
			}
			if pv.Spec.CSI == nil {
				t.Fatal("expected non-nil CSI source")
			}
			if pv.Spec.CSI.Driver != cnsoperatortypes.VSphereCSIDriverName {
				t.Errorf("CSI.Driver = %q, want %q", pv.Spec.CSI.Driver, cnsoperatortypes.VSphereCSIDriverName)
			}
			// The most important invariant: volumeHandle is the Supervisor PVC name, NOT the FCD
			// UUID and NOT the guest PV name — this is what the pvCSI syncer resolves guest volume
			// ops against (pkg/syncer/pvcsi_fullsync.go:365).
			if pv.Spec.CSI.VolumeHandle != testSupervisorPVCName {
				t.Errorf("CSI.VolumeHandle = %q, want %q", pv.Spec.CSI.VolumeHandle, testSupervisorPVCName)
			}
			if pv.Spec.CSI.FSType != tc.wantFSType {
				t.Errorf("CSI.FSType = %q, want %q", pv.Spec.CSI.FSType, tc.wantFSType)
			}
			if pv.Spec.ClaimRef == nil {
				t.Fatal("expected non-nil claimRef")
			}
			if pv.Spec.ClaimRef.Namespace != instance.Namespace || pv.Spec.ClaimRef.Name != instance.Spec.PVCName {
				t.Errorf("claimRef = %s/%s, want %s/%s",
					pv.Spec.ClaimRef.Namespace, pv.Spec.ClaimRef.Name, instance.Namespace, instance.Spec.PVCName)
			}
			if pv.Spec.VolumeMode == nil || *pv.Spec.VolumeMode != tc.volumeMode {
				t.Errorf("volumeMode = %v, want %v", pv.Spec.VolumeMode, tc.volumeMode)
			}
			if !reflect.DeepEqual(pv.Spec.AccessModes, tc.pvc.Spec.AccessModes) {
				t.Errorf("accessModes = %v, want %v", pv.Spec.AccessModes, tc.pvc.Spec.AccessModes)
			}
			wantCapacity := tc.pvc.Spec.Resources.Requests[corev1.ResourceStorage]
			if gotCapacity := pv.Spec.Capacity[corev1.ResourceStorage]; gotCapacity.Cmp(wantCapacity) != 0 {
				t.Errorf("capacity = %s, want %s", gotCapacity.String(), wantCapacity.String())
			}
			if pv.Spec.StorageClassName != *tc.pvc.Spec.StorageClassName {
				t.Errorf("storageClassName = %q, want %q", pv.Spec.StorageClassName, *tc.pvc.Spec.StorageClassName)
			}
			if pv.Spec.PersistentVolumeReclaimPolicy != corev1.PersistentVolumeReclaimDelete {
				t.Errorf("reclaimPolicy = %q, want %q", pv.Spec.PersistentVolumeReclaimPolicy, corev1.PersistentVolumeReclaimDelete)
			}
			if pv.Annotations["pv.kubernetes.io/provisioned-by"] != cnsoperatortypes.VSphereCSIDriverName {
				t.Errorf("provisioned-by annotation = %q, want %q",
					pv.Annotations["pv.kubernetes.io/provisioned-by"], cnsoperatortypes.VSphereCSIDriverName)
			}
			if pv.Labels[labelVKSRegVolCreatedBy] != labelVKSRegVolCreatedByValue {
				t.Errorf("created-by label = %q, want %q", pv.Labels[labelVKSRegVolCreatedBy], labelVKSRegVolCreatedByValue)
			}
			if pv.Labels[labelVKSRegVolCRNamespace] != instance.Namespace {
				t.Errorf("CR namespace label = %q, want %q", pv.Labels[labelVKSRegVolCRNamespace], instance.Namespace)
			}
			if pv.Labels[labelVKSRegVolCRName] != instance.Name {
				t.Errorf("CR name label = %q, want %q", pv.Labels[labelVKSRegVolCRName], instance.Name)
			}

			gotNil := pv.Spec.NodeAffinity == nil
			if gotNil != tc.wantNodeAffinityNil {
				t.Errorf("nodeAffinity nil = %v, want %v (nodeAffinity=%+v)",
					gotNil, tc.wantNodeAffinityNil, pv.Spec.NodeAffinity)
			}
		})
	}
}

// ── TestToCSITopology ─────────────────────────────────────────────────────────────────────────

func TestToCSITopology(t *testing.T) {
	if got := toCSITopology(nil); got != nil {
		t.Errorf("toCSITopology(nil) = %v, want nil", got)
	}
	if got := toCSITopology([]map[string]string{}); got != nil {
		t.Errorf("toCSITopology(empty) = %v, want nil", got)
	}

	segments := []map[string]string{
		{"topology.kubernetes.io/zone": "zone-a"},
		{"topology.kubernetes.io/zone": "zone-b"},
	}
	got := toCSITopology(segments)
	if len(got) != 2 {
		t.Fatalf("len(toCSITopology(segments)) = %d, want 2", len(got))
	}
	for i, top := range got {
		if !reflect.DeepEqual(top.Segments, segments[i]) {
			t.Errorf("topology[%d].Segments = %v, want %v", i, top.Segments, segments[i])
		}
	}
}

// ── TestGuestPVMatchesExpected ────────────────────────────────────────────────────────────────

func TestGuestPVMatchesExpected(t *testing.T) {
	instance := testInstance()

	matchingPV := &corev1.PersistentVolume{
		Spec: corev1.PersistentVolumeSpec{
			PersistentVolumeSource: corev1.PersistentVolumeSource{
				CSI: &corev1.CSIPersistentVolumeSource{VolumeHandle: testSupervisorPVCName},
			},
			ClaimRef: &corev1.ObjectReference{Namespace: instance.Namespace, Name: instance.Spec.PVCName},
		},
	}

	cases := []struct {
		name string
		pv   *corev1.PersistentVolume
		want bool
	}{
		{"nil PV", nil, false},
		{"matching PV", matchingPV, true},
		{
			name: "wrong volumeHandle (e.g. FCD UUID leaked in) → mismatch",
			pv: &corev1.PersistentVolume{
				Spec: corev1.PersistentVolumeSpec{
					PersistentVolumeSource: corev1.PersistentVolumeSource{
						CSI: &corev1.CSIPersistentVolumeSource{VolumeHandle: "42-fake-fcd-uuid"},
					},
					ClaimRef: &corev1.ObjectReference{Namespace: instance.Namespace, Name: instance.Spec.PVCName},
				},
			},
			want: false,
		},
		{
			name: "claimRef points at a different PVC → mismatch",
			pv: &corev1.PersistentVolume{
				Spec: corev1.PersistentVolumeSpec{
					PersistentVolumeSource: corev1.PersistentVolumeSource{
						CSI: &corev1.CSIPersistentVolumeSource{VolumeHandle: testSupervisorPVCName},
					},
					ClaimRef: &corev1.ObjectReference{Namespace: instance.Namespace, Name: "some-other-pvc"},
				},
			},
			want: false,
		},
		{
			name: "nil claimRef → mismatch",
			pv: &corev1.PersistentVolume{
				Spec: corev1.PersistentVolumeSpec{
					PersistentVolumeSource: corev1.PersistentVolumeSource{
						CSI: &corev1.CSIPersistentVolumeSource{VolumeHandle: testSupervisorPVCName},
					},
				},
			},
			want: false,
		},
		{
			name: "nil CSI source → mismatch",
			pv: &corev1.PersistentVolume{
				Spec: corev1.PersistentVolumeSpec{
					ClaimRef: &corev1.ObjectReference{Namespace: instance.Namespace, Name: instance.Spec.PVCName},
				},
			},
			want: false,
		},
	}

	for _, tc := range cases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			got := guestPVMatchesExpected(tc.pv, testSupervisorPVCName, instance)
			if got != tc.want {
				t.Errorf("guestPVMatchesExpected() = %v, want %v", got, tc.want)
			}
		})
	}
}
