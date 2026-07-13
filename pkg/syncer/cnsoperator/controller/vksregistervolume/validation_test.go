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

	corev1 "k8s.io/api/core/v1"
	storagev1 "k8s.io/api/storage/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	k8sfake "k8s.io/client-go/kubernetes/fake"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	vksregistervolumev1alpha1 "sigs.k8s.io/vsphere-csi-driver/v3/pkg/apis/cnsoperator/vksregistervolume/v1alpha1"
	"sigs.k8s.io/vsphere-csi-driver/v3/pkg/csi/service/common"
)

// makeScheme returns a runtime.Scheme with core/v1 and storage/v1 registered.
// VKSRegisterVolume is not needed in the scheme for these pure-helper tests because
// resolveGuestPVC never GETs the CR itself from the fake client.
func makeScheme(t *testing.T) *runtime.Scheme {
	t.Helper()
	s := runtime.NewScheme()
	if err := clientgoscheme.AddToScheme(s); err != nil {
		t.Fatalf("failed to add clientgoscheme: %v", err)
	}
	return s
}

// storageClassWithSVSC builds a StorageClass that carries the svstorageclass parameter.
func storageClassWithSVSC(name string) *storagev1.StorageClass {
	return &storagev1.StorageClass{
		ObjectMeta:  metav1.ObjectMeta{Name: name},
		Provisioner: "csi.vsphere.vmware.com",
		Parameters: map[string]string{
			common.AttributeSupervisorStorageClass: "silver",
		},
	}
}

// storageClassWithoutSVSC builds a StorageClass that does NOT carry the svstorageclass parameter.
func storageClassWithoutSVSC(name string) *storagev1.StorageClass {
	return &storagev1.StorageClass{
		ObjectMeta:  metav1.ObjectMeta{Name: name},
		Provisioner: "csi.vsphere.vmware.com",
		Parameters:  map[string]string{"other-param": "value"},
	}
}

// pendingPVC returns a well-formed Pending PVC accepted by resolveGuestPVC.
func pendingPVC(namespace, name, scName string) *corev1.PersistentVolumeClaim {
	sc := scName
	mode := corev1.PersistentVolumeFilesystem
	return &corev1.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: namespace,
		},
		Spec: corev1.PersistentVolumeClaimSpec{
			VolumeName:       "pv-restore-db-vol",
			StorageClassName: &sc,
			AccessModes:      []corev1.PersistentVolumeAccessMode{corev1.ReadWriteOnce},
			VolumeMode:       &mode,
			Resources: corev1.VolumeResourceRequirements{
				Requests: corev1.ResourceList{
					corev1.ResourceStorage: resource.MustParse("50Gi"),
				},
			},
		},
		Status: corev1.PersistentVolumeClaimStatus{
			Phase: corev1.ClaimPending,
		},
	}
}

// instanceWithAge builds a minimal VKSRegisterVolume CR with the given age offset.
// Use a positive offset for "older than timeout" and a negative or zero offset for "fresh".
func instanceWithAge(namespace, name, pvcName string,
	ageFromNow time.Duration) *vksregistervolumev1alpha1.VKSRegisterVolume {
	ts := metav1.NewTime(time.Now().Add(-ageFromNow))
	return &vksregistervolumev1alpha1.VKSRegisterVolume{
		ObjectMeta: metav1.ObjectMeta{
			Name:              name,
			Namespace:         namespace,
			CreationTimestamp: ts,
		},
		Spec: vksregistervolumev1alpha1.VKSRegisterVolumeSpec{
			PVCName:                    pvcName,
			CnsRegisterVolumeNamespace: "sv-ns",
			CnsRegisterVolumeName:      "abc-reg-deadbeef12345678",
		},
	}
}

// ── TestValidateSpec ──────────────────────────────────────────────────────────────────────────

func TestValidateSpec(t *testing.T) {
	validSpec := vksregistervolumev1alpha1.VKSRegisterVolumeSpec{
		PVCName:                    "db-data-pvc",
		CnsRegisterVolumeNamespace: "sv-ns",
		CnsRegisterVolumeName:      "abc-reg-deadbeef12345678",
	}

	cases := []struct {
		name    string
		spec    vksregistervolumev1alpha1.VKSRegisterVolumeSpec
		wantErr bool
	}{
		{
			name:    "all fields set — valid",
			spec:    validSpec,
			wantErr: false,
		},
		{
			name: "pvcName empty — invalid",
			spec: vksregistervolumev1alpha1.VKSRegisterVolumeSpec{
				PVCName:                    "",
				CnsRegisterVolumeNamespace: "sv-ns",
				CnsRegisterVolumeName:      "abc-reg-deadbeef12345678",
			},
			wantErr: true,
		},
		{
			name: "cnsRegisterVolumeName empty — invalid",
			spec: vksregistervolumev1alpha1.VKSRegisterVolumeSpec{
				PVCName:                    "db-data-pvc",
				CnsRegisterVolumeNamespace: "sv-ns",
				CnsRegisterVolumeName:      "",
			},
			wantErr: true,
		},
		{
			name: "cnsRegisterVolumeNamespace empty — invalid",
			spec: vksregistervolumev1alpha1.VKSRegisterVolumeSpec{
				PVCName:                    "db-data-pvc",
				CnsRegisterVolumeNamespace: "",
				CnsRegisterVolumeName:      "abc-reg-deadbeef12345678",
			},
			wantErr: true,
		},
		{
			name:    "all fields empty — invalid",
			spec:    vksregistervolumev1alpha1.VKSRegisterVolumeSpec{},
			wantErr: true,
		},
	}

	for _, tc := range cases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			err := validateSpec(context.Background(), &tc.spec)
			if tc.wantErr && err == nil {
				t.Error("expected error, got nil")
			}
			if !tc.wantErr && err != nil {
				t.Errorf("unexpected error: %v", err)
			}
		})
	}
}

// ── TestResolveGuestPVC ───────────────────────────────────────────────────────────────────────

func TestResolveGuestPVC(t *testing.T) {
	const (
		ns      = "my-app"
		pvcName = "db-data-pvc"
		scName  = "guest-sc"
	)

	freshInstance := instanceWithAge(ns, "restore-db-vol", pvcName, 0)
	oldInstance := instanceWithAge(ns, "restore-db-vol", pvcName, pvcMissingTimeout+time.Minute)

	cases := []struct {
		name         string
		instance     *vksregistervolumev1alpha1.VKSRegisterVolume
		pvcInStore   *corev1.PersistentVolumeClaim // nil → PVC not found
		scInStore    *storagev1.StorageClass        // nil → StorageClass not found
		wantPVC      bool  // expect non-nil PVC returned
		wantTerminal bool  // expect terminal=true
		wantErr      bool  // expect err != nil
	}{
		// ── PVC missing cases ─────────────────────────────────────────────────────────────────
		{
			name:         "PVC not found, CR fresh → transient (not terminal)",
			instance:     freshInstance,
			pvcInStore:   nil,
			scInStore:    storageClassWithSVSC(scName),
			wantPVC:      false,
			wantTerminal: false,
			wantErr:      true,
		},
		{
			name:         "PVC not found, CR older than timeout → terminal",
			instance:     oldInstance,
			pvcInStore:   nil,
			scInStore:    storageClassWithSVSC(scName),
			wantPVC:      false,
			wantTerminal: true,
			wantErr:      true,
		},

		// ── PVC state validation ──────────────────────────────────────────────────────────────
		{
			name: "PVC already Bound → terminal",
			instance: freshInstance,
			pvcInStore: func() *corev1.PersistentVolumeClaim {
				pvc := pendingPVC(ns, pvcName, scName)
				pvc.Status.Phase = corev1.ClaimBound
				pvc.Spec.VolumeName = "existing-pv"
				return pvc
			}(),
			scInStore:    storageClassWithSVSC(scName),
			wantPVC:      false,
			wantTerminal: true,
			wantErr:      true,
		},
		{
			name: "PVC spec.volumeName empty → terminal",
			instance: freshInstance,
			pvcInStore: func() *corev1.PersistentVolumeClaim {
				pvc := pendingPVC(ns, pvcName, scName)
				pvc.Spec.VolumeName = ""
				return pvc
			}(),
			scInStore:    storageClassWithSVSC(scName),
			wantPVC:      false,
			wantTerminal: true,
			wantErr:      true,
		},
		{
			name: "PVC no accessModes → terminal",
			instance: freshInstance,
			pvcInStore: func() *corev1.PersistentVolumeClaim {
				pvc := pendingPVC(ns, pvcName, scName)
				pvc.Spec.AccessModes = nil
				return pvc
			}(),
			scInStore:    storageClassWithSVSC(scName),
			wantPVC:      false,
			wantTerminal: true,
			wantErr:      true,
		},
		{
			name: "PVC no storage request → terminal",
			instance: freshInstance,
			pvcInStore: func() *corev1.PersistentVolumeClaim {
				pvc := pendingPVC(ns, pvcName, scName)
				pvc.Spec.Resources.Requests = corev1.ResourceList{}
				return pvc
			}(),
			scInStore:    storageClassWithSVSC(scName),
			wantPVC:      false,
			wantTerminal: true,
			wantErr:      true,
		},
		{
			name: "PVC zero storage request → terminal",
			instance: freshInstance,
			pvcInStore: func() *corev1.PersistentVolumeClaim {
				pvc := pendingPVC(ns, pvcName, scName)
				pvc.Spec.Resources.Requests[corev1.ResourceStorage] = resource.MustParse("0")
				return pvc
			}(),
			scInStore:    storageClassWithSVSC(scName),
			wantPVC:      false,
			wantTerminal: true,
			wantErr:      true,
		},
		{
			name: "PVC nil storageClassName → terminal",
			instance: freshInstance,
			pvcInStore: func() *corev1.PersistentVolumeClaim {
				pvc := pendingPVC(ns, pvcName, scName)
				pvc.Spec.StorageClassName = nil
				return pvc
			}(),
			scInStore:    storageClassWithSVSC(scName),
			wantPVC:      false,
			wantTerminal: true,
			wantErr:      true,
		},
		{
			name: "PVC empty storageClassName → terminal",
			instance: freshInstance,
			pvcInStore: func() *corev1.PersistentVolumeClaim {
				pvc := pendingPVC(ns, pvcName, scName)
				empty := ""
				pvc.Spec.StorageClassName = &empty
				return pvc
			}(),
			scInStore:    storageClassWithSVSC(scName),
			wantPVC:      false,
			wantTerminal: true,
			wantErr:      true,
		},

		// ── StorageClass validation ───────────────────────────────────────────────────────────
		{
			name:         "StorageClass not found → terminal",
			instance:     freshInstance,
			pvcInStore:   pendingPVC(ns, pvcName, scName),
			scInStore:    nil,
			wantPVC:      false,
			wantTerminal: true,
			wantErr:      true,
		},
		{
			name:         "StorageClass missing svstorageclass param → terminal",
			instance:     freshInstance,
			pvcInStore:   pendingPVC(ns, pvcName, scName),
			scInStore:    storageClassWithoutSVSC(scName),
			wantPVC:      false,
			wantTerminal: true,
			wantErr:      true,
		},

		// ── Happy path ────────────────────────────────────────────────────────────────────────
		{
			name:         "valid PVC with svstorageclass StorageClass → success",
			instance:     freshInstance,
			pvcInStore:   pendingPVC(ns, pvcName, scName),
			scInStore:    storageClassWithSVSC(scName),
			wantPVC:      true,
			wantTerminal: false,
			wantErr:      false,
		},
		{
			name: "valid PVC with nil VolumeMode (Filesystem implied) → success",
			instance: freshInstance,
			pvcInStore: func() *corev1.PersistentVolumeClaim {
				pvc := pendingPVC(ns, pvcName, scName)
				pvc.Spec.VolumeMode = nil
				return pvc
			}(),
			scInStore:    storageClassWithSVSC(scName),
			wantPVC:      true,
			wantTerminal: false,
			wantErr:      false,
		},
	}

	for _, tc := range cases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			s := makeScheme(t)
			var clientObjs []runtime.Object
			if tc.pvcInStore != nil {
				clientObjs = append(clientObjs, tc.pvcInStore)
			}
			fakeClient := fake.NewClientBuilder().WithScheme(s).WithRuntimeObjects(clientObjs...).Build()

			var k8sObjs []runtime.Object
			if tc.scInStore != nil {
				k8sObjs = append(k8sObjs, tc.scInStore)
			}
			fakeK8s := k8sfake.NewSimpleClientset(k8sObjs...)

			pvc, terminal, err := resolveGuestPVC(context.Background(), fakeClient, fakeK8s, tc.instance)

			if tc.wantErr && err == nil {
				t.Errorf("expected error, got nil (terminal=%v, pvc=%v)", terminal, pvc)
			}
			if !tc.wantErr && err != nil {
				t.Errorf("unexpected error: %v", err)
			}
			if tc.wantTerminal != terminal {
				t.Errorf("terminal: got %v, want %v (err=%v)", terminal, tc.wantTerminal, err)
			}
			if tc.wantPVC && pvc == nil {
				t.Error("expected non-nil PVC, got nil")
			}
			if !tc.wantPVC && pvc != nil {
				t.Errorf("expected nil PVC, got %s/%s", pvc.Namespace, pvc.Name)
			}
		})
	}
}

// ── TestDefaultVolumeMode ─────────────────────────────────────────────────────────────────────

func TestDefaultVolumeMode(t *testing.T) {
	block := corev1.PersistentVolumeBlock
	fs := corev1.PersistentVolumeFilesystem
	empty := corev1.PersistentVolumeMode("")

	cases := []struct {
		name string
		mode *corev1.PersistentVolumeMode
		want corev1.PersistentVolumeMode
	}{
		{"nil → Filesystem", nil, corev1.PersistentVolumeFilesystem},
		{"empty string → Filesystem", &empty, corev1.PersistentVolumeFilesystem},
		{"Filesystem → Filesystem", &fs, corev1.PersistentVolumeFilesystem},
		{"Block → Block", &block, corev1.PersistentVolumeBlock},
	}

	for _, tc := range cases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			got := defaultVolumeMode(tc.mode)
			if got != tc.want {
				t.Errorf("defaultVolumeMode(%v) = %q, want %q", tc.mode, got, tc.want)
			}
		})
	}
}

// ── TODO(T4+): Reconcile-level tests (see T10) ───────────────────────────────────────────────
//
// The following test cases require a fully wired Reconcile() loop (T4 complete) and fake
// Supervisor clients (T6). Add them in validation_test.go or a dedicated
// vksregistervolume_controller_test.go once T4 is done:
//
//  1. Happy path: Pending PVC + Registered Supervisor CR + Bound Supervisor PVC →
//     controller creates only the guest PV, PVC binds, Phase=Registered.
//  2. Idempotency: re-Reconcile an already-Registered CR → no duplicate PV, still Registered.
//  3. Finalizer add: first reconcile adds vksRegisterVolumeFinalizer.
//  4. Finalizer remove (reconcileDelete): deleting the CR removes only the finalizer;
//     guest PV/PVC and Supervisor CR are retained.
//  5. Spec validation failures in Reconcile(): missing pvcName etc. → Phase=Failed, no requeue.
//  6. PVC not found within timeout → requeue (not Failed).
//  7. PVC not found past timeout → Phase=Failed, no requeue.
//  8. PVC already Bound to a different PV → Phase=Failed.
//  9. Supervisor CR not yet Registered → requeue, phase WaitingForSupervisorRegistration.
// 10. Supervisor PVC not Bound → requeue, phase WaitingForSupervisorBinding.
// 11. B-missing after Registered: Supervisor CR deleted after CR reached Registered → no regression.
// 12. nodeAffinity: topology annotation on Supervisor PVC → guest PV gets nodeAffinity; nil otherwise.
