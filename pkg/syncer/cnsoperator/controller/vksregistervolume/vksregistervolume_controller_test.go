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
// Error, and records a Normal event. Wiring the call into Reconcile()'s WaitingForGuestPVCBound step
// is TODO(T7 wiring) (see the pseudo-code in Reconcile's doc comment) because it depends on T6
// supplying the real supervisorPVCName/accessibleTopology; the status-setting logic itself has no
// such dependency and is exercised directly here.
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
