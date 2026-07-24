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
	"context"
	"testing"
	"time"

	"github.com/container-storage-interface/spec/lib/go/csi"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	k8sfake "k8s.io/client-go/kubernetes/fake"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	apis "sigs.k8s.io/vsphere-csi-driver/v3/pkg/apis/cnsoperator"
	cnsregistervolumev1alpha1 "sigs.k8s.io/vsphere-csi-driver/v3/pkg/apis/cnsoperator/cnsregistervolume/v1alpha1"
	"sigs.k8s.io/vsphere-csi-driver/v3/pkg/csi/service/common"
)

// makeSupervisorScheme returns a runtime.Scheme with the cns.vmware.com group registered
// (needed to GET a CnsRegisterVolume through the fake controller-runtime client).
func makeSupervisorScheme(t *testing.T) *runtime.Scheme {
	t.Helper()
	s := runtime.NewScheme()
	if err := clientgoscheme.AddToScheme(s); err != nil {
		t.Fatalf("failed to add clientgoscheme: %v", err)
	}
	if err := apis.SchemeBuilder.AddToScheme(s); err != nil {
		t.Fatalf("failed to add cnsoperator scheme: %v", err)
	}
	return s
}

// ── TestWaitForSupervisorRegistration ────────────────────────────────────────────────────────────

func TestWaitForSupervisorRegistration(t *testing.T) {
	const supervisorNS = "sv-ns"
	const crName = "abc-reg-deadbeef12345678"

	cases := []struct {
		name         string
		supervisorCR *cnsregistervolumev1alpha1.CnsRegisterVolume
		instanceAge  time.Duration
		wantPVCName  string
		wantTerminal bool
		wantErr      bool
	}{
		{
			name: "registered",
			supervisorCR: &cnsregistervolumev1alpha1.CnsRegisterVolume{
				ObjectMeta: metav1.ObjectMeta{Name: crName, Namespace: supervisorNS},
				Spec:       cnsregistervolumev1alpha1.CnsRegisterVolumeSpec{PvcName: "sv-pvc-1"},
				Status:     cnsregistervolumev1alpha1.CnsRegisterVolumeStatus{Registered: true},
			},
			wantPVCName:  "sv-pvc-1",
			wantTerminal: false,
			wantErr:      false,
		},
		{
			name: "not yet registered",
			supervisorCR: &cnsregistervolumev1alpha1.CnsRegisterVolume{
				ObjectMeta: metav1.ObjectMeta{Name: crName, Namespace: supervisorNS},
				Status:     cnsregistervolumev1alpha1.CnsRegisterVolumeStatus{Registered: false},
			},
			wantTerminal: false,
			wantErr:      true,
		},
		{
			name: "supervisor CR reports error",
			supervisorCR: &cnsregistervolumev1alpha1.CnsRegisterVolume{
				ObjectMeta: metav1.ObjectMeta{Name: crName, Namespace: supervisorNS},
				Status: cnsregistervolumev1alpha1.CnsRegisterVolumeStatus{
					Registered: false,
					Error:      "backing disk not found",
				},
			},
			wantTerminal: true,
			wantErr:      true,
		},
		{
			name: "registered but empty pvcName is terminal",
			supervisorCR: &cnsregistervolumev1alpha1.CnsRegisterVolume{
				ObjectMeta: metav1.ObjectMeta{Name: crName, Namespace: supervisorNS},
				Status:     cnsregistervolumev1alpha1.CnsRegisterVolumeStatus{Registered: true},
			},
			wantTerminal: true,
			wantErr:      true,
		},
		{
			name:         "missing within timeout is transient",
			supervisorCR: nil,
			instanceAge:  time.Minute,
			wantTerminal: false,
			wantErr:      true,
		},
		{
			name:         "missing past timeout is terminal",
			supervisorCR: nil,
			instanceAge:  supervisorCRMissingTimeout + time.Minute,
			wantTerminal: true,
			wantErr:      true,
		},
	}

	for _, tc := range cases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			s := makeSupervisorScheme(t)
			var objs []runtime.Object
			if tc.supervisorCR != nil {
				objs = append(objs, tc.supervisorCR)
			}
			fakeClient := fake.NewClientBuilder().WithScheme(s).WithRuntimeObjects(objs...).Build()

			instance := instanceWithAge("guest-ns", "restore-db-vol", "db-data-pvc", tc.instanceAge)
			instance.Spec.CnsRegisterVolumeName = crName

			pvcName, terminal, err := waitForSupervisorRegistration(context.Background(),
				fakeClient, supervisorNS, instance)

			if tc.wantErr && err == nil {
				t.Errorf("expected error, got nil (terminal=%v, pvcName=%q)", terminal, pvcName)
			}
			if !tc.wantErr && err != nil {
				t.Errorf("unexpected error: %v", err)
			}
			if tc.wantTerminal != terminal {
				t.Errorf("terminal: got %v, want %v (err=%v)", terminal, tc.wantTerminal, err)
			}
			if pvcName != tc.wantPVCName {
				t.Errorf("pvcName: got %q, want %q", pvcName, tc.wantPVCName)
			}
		})
	}
}

// ── TestWaitForSupervisorBinding ─────────────────────────────────────────────────────────────────

func TestWaitForSupervisorBinding(t *testing.T) {
	const supervisorNS = "sv-ns"
	const pvcName = "sv-pvc-1"
	const pvName = "sv-pv-1"

	topologyAnnotation := `[{"topology.kubernetes.io/zone":"zone-a"}]`

	cases := []struct {
		name         string
		pvc          *corev1.PersistentVolumeClaim
		pv           *corev1.PersistentVolume
		wantTopology bool
		wantTerminal bool
		wantErr      bool
	}{
		{
			name: "bound with topology annotation",
			pvc: &corev1.PersistentVolumeClaim{
				ObjectMeta: metav1.ObjectMeta{
					Name:      pvcName,
					Namespace: supervisorNS,
					Annotations: map[string]string{
						common.AnnVolumeAccessibleTopology: topologyAnnotation,
					},
				},
				Spec:   corev1.PersistentVolumeClaimSpec{VolumeName: pvName},
				Status: corev1.PersistentVolumeClaimStatus{Phase: corev1.ClaimBound},
			},
			wantTopology: true,
			wantTerminal: false,
			wantErr:      false,
		},
		{
			name: "bound, no annotation, falls back to PV node affinity",
			pvc: &corev1.PersistentVolumeClaim{
				ObjectMeta: metav1.ObjectMeta{Name: pvcName, Namespace: supervisorNS},
				Spec:       corev1.PersistentVolumeClaimSpec{VolumeName: pvName},
				Status:     corev1.PersistentVolumeClaimStatus{Phase: corev1.ClaimBound},
			},
			pv: &corev1.PersistentVolume{
				ObjectMeta: metav1.ObjectMeta{Name: pvName},
				Spec: corev1.PersistentVolumeSpec{
					NodeAffinity: &corev1.VolumeNodeAffinity{
						Required: &corev1.NodeSelector{
							NodeSelectorTerms: []corev1.NodeSelectorTerm{
								{
									MatchExpressions: []corev1.NodeSelectorRequirement{
										{
											Key:      "topology.kubernetes.io/zone",
											Operator: corev1.NodeSelectorOpIn,
											Values:   []string{"zone-b"},
										},
									},
								},
							},
						},
					},
				},
			},
			wantTopology: true,
			wantTerminal: false,
			wantErr:      false,
		},
		{
			name: "bound, no annotation, no PV affinity — nil topology is not an error",
			pvc: &corev1.PersistentVolumeClaim{
				ObjectMeta: metav1.ObjectMeta{Name: pvcName, Namespace: supervisorNS},
				Spec:       corev1.PersistentVolumeClaimSpec{VolumeName: pvName},
				Status:     corev1.PersistentVolumeClaimStatus{Phase: corev1.ClaimBound},
			},
			pv: &corev1.PersistentVolume{
				ObjectMeta: metav1.ObjectMeta{Name: pvName},
			},
			wantTopology: false,
			wantTerminal: false,
			wantErr:      false,
		},
		{
			name: "not yet bound is transient",
			pvc: &corev1.PersistentVolumeClaim{
				ObjectMeta: metav1.ObjectMeta{Name: pvcName, Namespace: supervisorNS},
				Spec:       corev1.PersistentVolumeClaimSpec{VolumeName: pvName},
				Status:     corev1.PersistentVolumeClaimStatus{Phase: corev1.ClaimPending},
			},
			wantTerminal: false,
			wantErr:      true,
		},
		{
			name:         "pvc missing after supervisor CR registered is terminal",
			pvc:          nil,
			wantTerminal: true,
			wantErr:      true,
		},
	}

	for _, tc := range cases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			var objs []runtime.Object
			if tc.pvc != nil {
				objs = append(objs, tc.pvc)
			}
			if tc.pv != nil {
				objs = append(objs, tc.pv)
			}
			fakeK8s := k8sfake.NewSimpleClientset(objs...)

			topology, terminal, err := waitForSupervisorBinding(context.Background(),
				fakeK8s, supervisorNS, pvcName)

			if tc.wantErr && err == nil {
				t.Errorf("expected error, got nil (terminal=%v, topology=%v)", terminal, topology)
			}
			if !tc.wantErr && err != nil {
				t.Errorf("unexpected error: %v", err)
			}
			if tc.wantTerminal != terminal {
				t.Errorf("terminal: got %v, want %v (err=%v)", terminal, tc.wantTerminal, err)
			}
			gotTopology := len(topology) > 0
			if gotTopology != tc.wantTopology {
				t.Errorf("topology presence: got %v (%v), want %v", gotTopology, topology, tc.wantTopology)
			}
		})
	}
}

// ── TestParseVolumeAccessibleTopologyAnnotation ──────────────────────────────────────────────────

func TestParseVolumeAccessibleTopologyAnnotation(t *testing.T) {
	cases := []struct {
		name       string
		annotation string
		wantLen    int
		wantErr    bool
	}{
		{
			name:       "single zone",
			annotation: `[{"topology.kubernetes.io/zone":"zone-a"}]`,
			wantLen:    1,
			wantErr:    false,
		},
		{
			name:       "multiple zones",
			annotation: `[{"topology.kubernetes.io/zone":"zone-a"},{"topology.kubernetes.io/zone":"zone-b"}]`,
			wantLen:    2,
			wantErr:    false,
		},
		{
			name:       "malformed json",
			annotation: `not-json`,
			wantErr:    true,
		},
	}

	for _, tc := range cases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			segments, err := parseVolumeAccessibleTopologyAnnotation(tc.annotation)
			if tc.wantErr && err == nil {
				t.Errorf("expected error, got nil (segments=%v)", segments)
			}
			if !tc.wantErr && err != nil {
				t.Errorf("unexpected error: %v", err)
			}
			if !tc.wantErr && len(segments) != tc.wantLen {
				t.Errorf("segments length: got %d, want %d", len(segments), tc.wantLen)
			}
		})
	}
}

// ── TestToCSITopology ─────────────────────────────────────────────────────────────────────────────

func TestToCSITopology(t *testing.T) {
	segments := []map[string]string{
		{"topology.kubernetes.io/zone": "zone-a"},
		{"topology.kubernetes.io/zone": "zone-b"},
	}
	got := toCSITopology(segments)
	if len(got) != 2 {
		t.Fatalf("expected 2 topology entries, got %d", len(got))
	}
	want := []*csi.Topology{
		{Segments: map[string]string{"topology.kubernetes.io/zone": "zone-a"}},
		{Segments: map[string]string{"topology.kubernetes.io/zone": "zone-b"}},
	}
	for i := range want {
		if got[i].Segments["topology.kubernetes.io/zone"] != want[i].Segments["topology.kubernetes.io/zone"] {
			t.Errorf("entry %d: got %v, want %v", i, got[i], want[i])
		}
	}
}
