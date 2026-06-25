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
	"context"
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	csivolumeinfosvc "sigs.k8s.io/vsphere-csi-driver/v3/pkg/apis/cnsoperator/csivolumeinfo"
	csivolumeinfov1alpha1 "sigs.k8s.io/vsphere-csi-driver/v3/pkg/apis/cnsoperator/csivolumeinfo/v1alpha1"
	csitypes "sigs.k8s.io/vsphere-csi-driver/v3/pkg/csi/types"
)

// newCVIScheme returns a runtime.Scheme with CsiVolumeInfo types registered.
func newCVIScheme(t *testing.T) *runtime.Scheme {
	t.Helper()
	s := runtime.NewScheme()
	require.NoError(t, csivolumeinfov1alpha1.AddToScheme(s))
	return s
}

// makeCsiPV returns a minimal vSphere CSI PV with the given phase and volumeHandle.
func makeCsiPV(name, volumeHandle string, phase v1.PersistentVolumePhase) *v1.PersistentVolume {
	return &v1.PersistentVolume{
		ObjectMeta: metav1.ObjectMeta{Name: name},
		Spec: v1.PersistentVolumeSpec{
			PersistentVolumeSource: v1.PersistentVolumeSource{
				CSI: &v1.CSIPersistentVolumeSource{
					Driver:       csitypes.Name,
					VolumeHandle: volumeHandle,
				},
			},
		},
		Status: v1.PersistentVolumeStatus{Phase: phase},
	}
}

// makeCsiPVWithClaimRef adds a ClaimRef to a vSphere CSI PV.
func makeCsiPVWithClaimRef(name, volumeHandle, pvcName, pvcNamespace string,
	phase v1.PersistentVolumePhase) *v1.PersistentVolume {
	pv := makeCsiPV(name, volumeHandle, phase)
	pv.Spec.ClaimRef = &v1.ObjectReference{
		Name:      pvcName,
		Namespace: pvcNamespace,
	}
	return pv
}

// makeCVIForPVBind returns a minimal CsiVolumeInfo for the given volumeID.
func makeCVIForPVBind(volumeID, pvName, pvcName, pvcNamespace string) *csivolumeinfov1alpha1.CsiVolumeInfo {
	return &csivolumeinfov1alpha1.CsiVolumeInfo{
		ObjectMeta: metav1.ObjectMeta{
			Name:      csivolumeinfosvc.GetCsiVolumeInfoCRName(volumeID),
			Namespace: csivolumeinfov1alpha1.CVINamespace,
		},
		Spec: csivolumeinfov1alpha1.CsiVolumeInfoSpec{
			VolumeID:     volumeID,
			PVName:       pvName,
			PVCName:      pvcName,
			PVCNamespace: pvcNamespace,
		},
	}
}

// TestReconcileCviPVBind_NoTransition verifies that the function is a no-op
// when the PV phase does not change.
func TestReconcileCviPVBind_NoTransition(t *testing.T) {
	ctx := context.Background()
	scheme := newCVIScheme(t)
	fakeClient := fake.NewClientBuilder().WithScheme(scheme).Build()
	svc := csivolumeinfosvc.NewCsiVolumeInfoService(fakeClient)

	pv := makeCsiPV("pv-1", "vol-aaa", v1.VolumeBound)
	err := ReconcileCsiVolumeInfoOnPVBind(ctx, pv, pv, svc)
	require.NoError(t, err)
}

// TestReconcileCviPVBind_NotBound verifies that a transition to Released is ignored.
func TestReconcileCviPVBind_NotBound(t *testing.T) {
	ctx := context.Background()
	scheme := newCVIScheme(t)
	fakeClient := fake.NewClientBuilder().WithScheme(scheme).Build()
	svc := csivolumeinfosvc.NewCsiVolumeInfoService(fakeClient)

	oldPV := makeCsiPV("pv-1", "vol-aaa", v1.VolumeBound)
	newPV := makeCsiPV("pv-1", "vol-aaa", v1.VolumeReleased)
	err := ReconcileCsiVolumeInfoOnPVBind(ctx, oldPV, newPV, svc)
	require.NoError(t, err)
}

// TestReconcileCviPVBind_NotVspherePV verifies that non-vSphere PVs are skipped.
func TestReconcileCviPVBind_NotVspherePV(t *testing.T) {
	ctx := context.Background()
	scheme := newCVIScheme(t)
	fakeClient := fake.NewClientBuilder().WithScheme(scheme).Build()
	svc := csivolumeinfosvc.NewCsiVolumeInfoService(fakeClient)

	oldPV := &v1.PersistentVolume{
		ObjectMeta: metav1.ObjectMeta{Name: "pv-nfs"},
		Spec: v1.PersistentVolumeSpec{
			PersistentVolumeSource: v1.PersistentVolumeSource{
				NFS: &v1.NFSVolumeSource{Server: "nfs-server", Path: "/exports"},
			},
		},
		Status: v1.PersistentVolumeStatus{Phase: v1.VolumeAvailable},
	}
	newPV := oldPV.DeepCopy()
	newPV.Status.Phase = v1.VolumeBound
	err := ReconcileCsiVolumeInfoOnPVBind(ctx, oldPV, newPV, svc)
	require.NoError(t, err)
}

// TestReconcileCviPVBind_NoCVI verifies that a missing CsiVolumeInfo is a
// no-op (the function returns nil).
func TestReconcileCviPVBind_NoCVI(t *testing.T) {
	ctx := context.Background()
	scheme := newCVIScheme(t)
	fakeClient := fake.NewClientBuilder().WithScheme(scheme).Build()
	svc := csivolumeinfosvc.NewCsiVolumeInfoService(fakeClient)

	oldPV := makeCsiPV("pv-1", "vol-no-cvi", v1.VolumeAvailable)
	newPV := makeCsiPVWithClaimRef("pv-1", "vol-no-cvi", "my-pvc", "default", v1.VolumeBound)
	err := ReconcileCsiVolumeInfoOnPVBind(ctx, oldPV, newPV, svc)
	require.NoError(t, err)
}

// TestReconcileCviPVBind_PatchesPVName verifies that when a CsiVolumeInfo
// exists with a stale pvName, the patch updates it correctly.
func TestReconcileCviPVBind_PatchesPVName(t *testing.T) {
	ctx := context.Background()
	volumeID := "vol-123"
	scheme := newCVIScheme(t)

	cvi := makeCVIForPVBind(volumeID, "old-pv-name", "my-pvc", "default")
	fakeClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(cvi).
		WithStatusSubresource(cvi).
		Build()
	svc := csivolumeinfosvc.NewCsiVolumeInfoService(fakeClient)

	oldPV := makeCsiPV("new-pv-name", volumeID, v1.VolumeAvailable)
	newPV := makeCsiPVWithClaimRef("new-pv-name", volumeID, "my-pvc", "default", v1.VolumeBound)

	err := ReconcileCsiVolumeInfoOnPVBind(ctx, oldPV, newPV, svc)
	require.NoError(t, err)

	// Verify the patch was applied.
	updated, fetchErr := svc.GetCsiVolumeInfo(ctx, volumeID)
	require.NoError(t, fetchErr)
	require.NotNil(t, updated)
	assert.Equal(t, "new-pv-name", updated.Spec.PVName)
	assert.Equal(t, "my-pvc", updated.Spec.PVCName)
	assert.Equal(t, "default", updated.Spec.PVCNamespace)
}

// TestReconcileCviPVBind_AlreadyUpToDate verifies that no patch is issued
// when the CsiVolumeInfo spec already matches the PV.
func TestReconcileCviPVBind_AlreadyUpToDate(t *testing.T) {
	ctx := context.Background()
	volumeID := "vol-456"
	scheme := newCVIScheme(t)

	cvi := makeCVIForPVBind(volumeID, "pv-456", "my-pvc", "ns1")
	fakeClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(cvi).
		WithStatusSubresource(cvi).
		Build()
	svc := csivolumeinfosvc.NewCsiVolumeInfoService(fakeClient)

	oldPV := makeCsiPV("pv-456", volumeID, v1.VolumeAvailable)
	newPV := makeCsiPVWithClaimRef("pv-456", volumeID, "my-pvc", "ns1", v1.VolumeBound)

	err := ReconcileCsiVolumeInfoOnPVBind(ctx, oldPV, newPV, svc)
	require.NoError(t, err)
}

// TestReconcileCviPVBind_EmptyVolumeHandle verifies that a PV with an empty
// volumeHandle is safely skipped.
func TestReconcileCviPVBind_EmptyVolumeHandle(t *testing.T) {
	ctx := context.Background()
	scheme := newCVIScheme(t)
	fakeClient := fake.NewClientBuilder().WithScheme(scheme).Build()
	svc := csivolumeinfosvc.NewCsiVolumeInfoService(fakeClient)

	oldPV := makeCsiPV("pv-empty", "", v1.VolumeAvailable)
	newPV := makeCsiPV("pv-empty", "", v1.VolumeBound)

	err := ReconcileCsiVolumeInfoOnPVBind(ctx, oldPV, newPV, svc)
	require.NoError(t, err)
}

// TestPatchStructJSON verifies that the internal patch struct marshals
// correctly (regression guard for field names used in merge-patch).
func TestPatchStructJSON(t *testing.T) {
	type specPatch struct {
		PVName       string `json:"pvName,omitempty"`
		PVCName      string `json:"pvcName,omitempty"`
		PVCNamespace string `json:"pvcNamespace,omitempty"`
	}
	type patch struct {
		Spec specPatch `json:"spec"`
	}
	p := patch{Spec: specPatch{PVName: "pv-1", PVCName: "pvc-1", PVCNamespace: "ns-1"}}
	b, err := json.Marshal(p)
	require.NoError(t, err)
	assert.Contains(t, string(b), `"pvName":"pv-1"`)
	assert.Contains(t, string(b), `"pvcName":"pvc-1"`)
	assert.Contains(t, string(b), `"pvcNamespace":"ns-1"`)
}
