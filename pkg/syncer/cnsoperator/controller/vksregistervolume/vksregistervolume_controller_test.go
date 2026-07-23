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

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	apitypes "k8s.io/apimachinery/pkg/types"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	"k8s.io/client-go/tools/record"

	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	vksregistervolumev1alpha1 "sigs.k8s.io/vsphere-csi-driver/v3/pkg/apis/cnsoperator/vksregistervolume/v1alpha1"
)

// vksRegisterVolumeTestScheme returns a runtime.Scheme with core/v1 and VKSRegisterVolume registered,
// for tests that need a controller-runtime fake client backing a ReconcileVKSRegisterVolume.
func vksRegisterVolumeTestScheme(t *testing.T) *runtime.Scheme {
	t.Helper()
	s := runtime.NewScheme()
	if err := clientgoscheme.AddToScheme(s); err != nil {
		t.Fatalf("failed to add clientgoscheme: %v", err)
	}
	s.AddKnownTypes(schema.GroupVersion{Group: "cns.vmware.com", Version: "v1alpha1"},
		&vksregistervolumev1alpha1.VKSRegisterVolume{}, &vksregistervolumev1alpha1.VKSRegisterVolumeList{})
	return s
}

// ── TestSetStatusRegistered ──────────────────────────────────────────────────────────────────────
//
// setStatusRegistered implements the plan's Registered terminal phase (Part 4d step 8): once the
// guest PVC↔PV direct bind completes, it sets Phase=Registered, Registered=true, clears any prior
// Error, and records a Normal event. This test exercises the status-setting logic directly.
func TestSetStatusRegistered(t *testing.T) {
	instance := &vksregistervolumev1alpha1.VKSRegisterVolume{
		ObjectMeta: metav1.ObjectMeta{Namespace: testNamespace, Name: testCRName},
		Spec: vksregistervolumev1alpha1.VKSRegisterVolumeSpec{
			PVCName:               testPVCName,
			CnsRegisterVolumeName: testSupervisorPVCName,
		},
		Status: vksregistervolumev1alpha1.VKSRegisterVolumeStatus{
			Phase: vksregistervolumev1alpha1.VKSRegisterVolumePhaseWaitingForGuestPVCBound,
			Error: "PVC not yet Bound",
		},
	}

	s := vksRegisterVolumeTestScheme(t)
	fakeClient := fake.NewClientBuilder().
		WithScheme(s).
		WithObjects(instance).
		WithStatusSubresource(instance).
		Build()

	backOffDuration = make(map[apitypes.NamespacedName]time.Duration)
	recorder := record.NewFakeRecorder(10)
	r := &ReconcileVKSRegisterVolume{client: fakeClient, recorder: recorder}

	if err := r.setStatusRegistered(context.Background(), instance); err != nil {
		t.Fatalf("setStatusRegistered returned unexpected error: %v", err)
	}

	got := &vksregistervolumev1alpha1.VKSRegisterVolume{}
	if err := fakeClient.Get(context.Background(),
		apitypes.NamespacedName{Namespace: testNamespace, Name: testCRName}, got); err != nil {
		t.Fatalf("failed to GET VKSRegisterVolume after setStatusRegistered: %v", err)
	}

	if got.Status.Phase != vksregistervolumev1alpha1.VKSRegisterVolumePhaseRegistered {
		t.Errorf("Status.Phase = %q, want %q", got.Status.Phase, vksregistervolumev1alpha1.VKSRegisterVolumePhaseRegistered)
	}
	if !got.Status.Registered {
		t.Error("Status.Registered = false, want true")
	}
	if got.Status.Error != "" {
		t.Errorf("Status.Error = %q, want empty (cleared on success)", got.Status.Error)
	}

	select {
	case event := <-recorder.Events:
		if event == "" {
			t.Error("expected a non-empty Normal event to be recorded")
		}
	default:
		t.Error("expected a Normal event to be recorded, got none")

	}
}

// ── TestHasPassedPhase ────────────────────────────────────────────────────────────────────────────

func TestHasPassedPhase(t *testing.T) {
	cases := []struct {
		name    string
		current vksregistervolumev1alpha1.VKSRegisterVolumePhase
		target  vksregistervolumev1alpha1.VKSRegisterVolumePhase
		want    bool
	}{
		{
			name:    "unset current is treated as Pending — never past anything",
			current: "",
			target:  vksregistervolumev1alpha1.VKSRegisterVolumePhaseWaitingForSupervisorRegistration,
			want:    false,
		},
		{
			name:    "current equals target is not past",
			current: vksregistervolumev1alpha1.VKSRegisterVolumePhaseWaitingForSupervisorRegistration,
			target:  vksregistervolumev1alpha1.VKSRegisterVolumePhaseWaitingForSupervisorRegistration,
			want:    false,
		},
		{
			name:    "current earlier than target is not past",
			current: vksregistervolumev1alpha1.VKSRegisterVolumePhasePending,
			target:  vksregistervolumev1alpha1.VKSRegisterVolumePhaseWaitingForSupervisorBinding,
			want:    false,
		},
		{
			name:    "current later than target is past — the regression case",
			current: vksregistervolumev1alpha1.VKSRegisterVolumePhaseCreatingGuestPV,
			target:  vksregistervolumev1alpha1.VKSRegisterVolumePhaseWaitingForSupervisorRegistration,
			want:    true,
		},
		{
			name:    "current one step later than target is past",
			current: vksregistervolumev1alpha1.VKSRegisterVolumePhaseWaitingForSupervisorBinding,
			target:  vksregistervolumev1alpha1.VKSRegisterVolumePhaseWaitingForSupervisorRegistration,
			want:    true,
		},
	}

	for _, tc := range cases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			got := hasPassedPhase(tc.current, tc.target)
			if got != tc.want {
				t.Errorf("hasPassedPhase(%q, %q): got %v, want %v", tc.current, tc.target, got, tc.want)
			}
		})
	}
}

// ── TestSetStatusPhaseDoesNotRegress ─────────────────────────────────────────────────────────────

// TestSetStatusPhaseDoesNotRegress guards against the phase-regression bug Copilot flagged on
// vksregistervolume PR #4180: Reconcile always walks its steps in order and, before this fix,
// unconditionally called setStatusPhase for each step's target phase — so a CR already at a later
// phase (e.g. CreatingGuestPV, once T7 exists) got patched backward to an earlier one
// (WaitingForSupervisorRegistration) on every subsequent reconcile.
func TestSetStatusPhaseDoesNotRegress(t *testing.T) {
	cases := []struct {
		name          string
		startingPhase vksregistervolumev1alpha1.VKSRegisterVolumePhase
		setPhase      vksregistervolumev1alpha1.VKSRegisterVolumePhase
		wantPhase     vksregistervolumev1alpha1.VKSRegisterVolumePhase
	}{
		{
			name:          "advancing forward patches normally",
			startingPhase: vksregistervolumev1alpha1.VKSRegisterVolumePhasePending,
			setPhase:      vksregistervolumev1alpha1.VKSRegisterVolumePhaseWaitingForSupervisorRegistration,
			wantPhase:     vksregistervolumev1alpha1.VKSRegisterVolumePhaseWaitingForSupervisorRegistration,
		},
		{
			name:          "re-setting the same phase is a no-op",
			startingPhase: vksregistervolumev1alpha1.VKSRegisterVolumePhaseWaitingForSupervisorBinding,
			setPhase:      vksregistervolumev1alpha1.VKSRegisterVolumePhaseWaitingForSupervisorBinding,
			wantPhase:     vksregistervolumev1alpha1.VKSRegisterVolumePhaseWaitingForSupervisorBinding,
		},
		{
			name:          "setting an earlier phase than current does not regress",
			startingPhase: vksregistervolumev1alpha1.VKSRegisterVolumePhaseCreatingGuestPV,
			setPhase:      vksregistervolumev1alpha1.VKSRegisterVolumePhaseWaitingForSupervisorRegistration,
			wantPhase:     vksregistervolumev1alpha1.VKSRegisterVolumePhaseCreatingGuestPV,
		},
		{
			name:          "setting an earlier phase one step back does not regress",
			startingPhase: vksregistervolumev1alpha1.VKSRegisterVolumePhaseWaitingForSupervisorBinding,
			setPhase:      vksregistervolumev1alpha1.VKSRegisterVolumePhaseWaitingForSupervisorRegistration,
			wantPhase:     vksregistervolumev1alpha1.VKSRegisterVolumePhaseWaitingForSupervisorBinding,
		},
	}

	for _, tc := range cases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			s := makeSupervisorScheme(t)
			instance := &vksregistervolumev1alpha1.VKSRegisterVolume{
				ObjectMeta: metav1.ObjectMeta{Name: "restore-db-vol", Namespace: "my-app"},
				Status:     vksregistervolumev1alpha1.VKSRegisterVolumeStatus{Phase: tc.startingPhase},
			}
			fakeClient := fake.NewClientBuilder().WithScheme(s).
				WithRuntimeObjects(instance).WithStatusSubresource(instance).Build()

			r := &ReconcileVKSRegisterVolume{client: fakeClient}

			if err := r.setStatusPhase(context.Background(), instance, tc.setPhase); err != nil {
				t.Fatalf("setStatusPhase returned unexpected error: %v", err)
			}
			if instance.Status.Phase != tc.wantPhase {
				t.Errorf("in-memory instance phase: got %q, want %q", instance.Status.Phase, tc.wantPhase)
			}

			persisted := &vksregistervolumev1alpha1.VKSRegisterVolume{}
			if err := fakeClient.Get(context.Background(),
				client.ObjectKeyFromObject(instance), persisted); err != nil {
				t.Fatalf("failed to GET persisted instance: %v", err)
			}
			if persisted.Status.Phase != tc.wantPhase {
				t.Errorf("persisted phase: got %q, want %q", persisted.Status.Phase, tc.wantPhase)
			}
		})
	}
}
