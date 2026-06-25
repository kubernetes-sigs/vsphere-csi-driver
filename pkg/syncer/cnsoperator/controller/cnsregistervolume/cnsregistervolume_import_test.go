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

package cnsregistervolume

import (
	"context"
	"testing"
	"time"

	cnstypes "github.com/vmware/govmomi/cns/types"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	clientset "k8s.io/client-go/kubernetes"
	k8sfake "k8s.io/client-go/kubernetes/fake"
	"k8s.io/client-go/tools/record"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
	cnsvolume "sigs.k8s.io/vsphere-csi-driver/v3/pkg/common/cns-lib/volume"

	apis "sigs.k8s.io/vsphere-csi-driver/v3/pkg/apis/cnsoperator"
	cnsregistervolumev1alpha1 "sigs.k8s.io/vsphere-csi-driver/v3/pkg/apis/cnsoperator/cnsregistervolume/v1alpha1"
	csivolumeinfosvc "sigs.k8s.io/vsphere-csi-driver/v3/pkg/apis/cnsoperator/csivolumeinfo"
	csivolumeinfov1alpha1 "sigs.k8s.io/vsphere-csi-driver/v3/pkg/apis/cnsoperator/csivolumeinfo/v1alpha1"
	"sigs.k8s.io/vsphere-csi-driver/v3/pkg/csi/service/common"
	"sigs.k8s.io/vsphere-csi-driver/v3/pkg/csi/service/common/commonco"
)

// importTestCO is a minimal CO interface for import tests that controls only the
// VMOwnedVolumes FSS. Unneeded methods are delegated to the shared mockCOCommon.
type importTestCO struct {
	mockCOCommon
	vmOwnedVolumesEnabled bool
}

func (c *importTestCO) IsFSSEnabled(_ context.Context, featureName string) bool {
	if featureName == common.VMOwnedVolumes {
		return c.vmOwnedVolumesEnabled
	}
	return false
}

// newImportScheme creates a runtime.Scheme with all cns operator types plus
// CsiVolumeInfo and core types registered.
func newImportScheme(t *testing.T) *runtime.Scheme {
	t.Helper()
	s := runtime.NewScheme()
	if err := apis.AddToScheme(s); err != nil {
		t.Fatalf("AddToScheme cns operator: %v", err)
	}
	if err := csivolumeinfov1alpha1.AddToScheme(s); err != nil {
		t.Fatalf("AddToScheme CsiVolumeInfo: %v", err)
	}
	if err := corev1.AddToScheme(s); err != nil {
		t.Fatalf("AddToScheme core: %v", err)
	}
	return s
}

// importTestPVC returns a PVC resembling one created by vm-operator before the CRV.
func importTestPVC(name, namespace string) *corev1.PersistentVolumeClaim {
	sc := "vsan-default-storage-policy"
	fs := corev1.PersistentVolumeFilesystem
	return &corev1.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: namespace,
			UID:       types.UID("f47ac10b-58cc-4372-a567-0e02b2c3d479"),
		},
		Spec: corev1.PersistentVolumeClaimSpec{
			AccessModes:      []corev1.PersistentVolumeAccessMode{corev1.ReadWriteOnce},
			StorageClassName: &sc,
			VolumeMode:       &fs,
			Resources: corev1.VolumeResourceRequirements{
				Requests: corev1.ResourceList{
					corev1.ResourceStorage: resource.MustParse("100Gi"),
				},
			},
		},
	}
}

// importTestCRV returns a CnsRegisterVolume with deferFcdRegistration=true.
func importTestCRV(name, namespace, pvcName string) *cnsregistervolumev1alpha1.CnsRegisterVolume {
	return &cnsregistervolumev1alpha1.CnsRegisterVolume{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: namespace,
			Finalizers: []string{
				"cns.vmware.com/register-volume",
			},
		},
		Spec: cnsregistervolumev1alpha1.CnsRegisterVolumeSpec{
			PvcName:              pvcName,
			DeferFcdRegistration: true,
			DiskURLPath:          "[vsanDatastore] testns/imported-vm/disk-0.vmdk",
			AccessMode:           corev1.ReadWriteOnce,
			VMName:               "imported-vm",
			VMInstanceUUID:       "502e71fa-1234-5678-9abc-def012345678",
			DiskUUID:             "6000C29a-bcde-1234-5678-9abcdef01234",
		},
	}
}

// newImportReconciler builds a ReconcileCnsRegisterVolume for deferred-FCD tests.
// The k8s fake client is seeded with pvc (nil = absent); the controller-runtime
// client holds the CRV and serves as the CVI backend.
func newImportReconciler(t *testing.T,
	pvc *corev1.PersistentVolumeClaim,
	crv *cnsregistervolumev1alpha1.CnsRegisterVolume,
	fssEnabled bool,
) *ReconcileCnsRegisterVolume {
	t.Helper()

	s := newImportScheme(t)

	crCtrl := fake.NewClientBuilder().
		WithScheme(s).
		WithObjects(crv).
		WithStatusSubresource(
			crv,
			&csivolumeinfov1alpha1.CsiVolumeInfo{},
		).
		Build()

	var k8sObjs []runtime.Object
	if pvc != nil {
		k8sObjs = append(k8sObjs, pvc)
	}
	k8sClient := k8sfake.NewSimpleClientset(k8sObjs...)

	broadcaster := record.NewBroadcaster()
	recorder := broadcaster.NewRecorder(runtime.NewScheme(), corev1.EventSource{Component: "test"})

	commonco.ContainerOrchestratorUtility = &importTestCO{vmOwnedVolumesEnabled: fssEnabled}
	backOffDuration = make(map[types.NamespacedName]time.Duration)

	return &ReconcileCnsRegisterVolume{
		client:               crCtrl,
		scheme:               s,
		volumeManager:        &mockVolumeManager{},
		recorder:             recorder,
		k8sclient:            k8sClient,
		csiVolumeInfoService: csivolumeinfosvc.NewCsiVolumeInfoService(crCtrl),
	}
}

// stubIsPVCBound sets isPVCBoundFn to return (true, nil) and restores the
// original on test cleanup.
func stubIsPVCBound(t *testing.T) {
	t.Helper()
	origFn := isPVCBoundFn
	isPVCBoundFn = func(_ context.Context, _ clientset.Interface,
		_ *corev1.PersistentVolumeClaim, _ time.Duration) (bool, error) {
		return true, nil
	}
	t.Cleanup(func() { isPVCBoundFn = origFn })
}

// TestReconcileDeferFcd_HappyPath verifies that a successful import creates the
// PV with the correct volumeHandle, the CVI in VMManaged state, and marks the
// CRV as registered with volumeID and pvName set.
func TestReconcileDeferFcd_HappyPath(t *testing.T) {
	ctx := context.Background()
	pvc := importTestPVC("imported-vm-1a2b", "testns")
	crv := importTestCRV("imported-vm-1a2b", "testns", "imported-vm-1a2b")
	stubIsPVCBound(t)
	r := newImportReconciler(t, pvc, crv, true)

	res, err := r.reconcileDeferFcdRegistration(ctx, crv,
		reconcile.Request{NamespacedName: types.NamespacedName{Name: crv.Name, Namespace: crv.Namespace}},
		time.Second)

	if err != nil {
		t.Fatalf("expected no error, got: %v", err)
	}
	if res.RequeueAfter != 0 {
		t.Errorf("expected zero result, got: %+v", res)
	}

	expectedVolumeID := string(pvc.UID)
	expectedPVName := staticPvNamePrefix + expectedVolumeID

	// PV must exist with volumeHandle == PVC UID and reclaim policy Delete.
	pv, pvErr := r.k8sclient.CoreV1().PersistentVolumes().Get(ctx, expectedPVName, metav1.GetOptions{})
	if pvErr != nil {
		t.Fatalf("PV not found: %v", pvErr)
	}
	if pv.Spec.CSI == nil || pv.Spec.CSI.VolumeHandle != expectedVolumeID {
		t.Errorf("PV volumeHandle = %q; want %q", pv.Spec.CSI.VolumeHandle, expectedVolumeID)
	}
	if pv.Spec.PersistentVolumeReclaimPolicy != corev1.PersistentVolumeReclaimDelete {
		t.Errorf("PV reclaimPolicy = %q; want Delete", pv.Spec.PersistentVolumeReclaimPolicy)
	}
	if pv.Spec.ClaimRef == nil || pv.Spec.ClaimRef.Name != pvc.Name {
		t.Errorf("PV claimRef.Name = %q; want %q", pv.Spec.ClaimRef.Name, pvc.Name)
	}

	// CVI must exist in VMManaged/Succeeded state with correct metadata.
	cviSvc := csivolumeinfosvc.NewCsiVolumeInfoService(r.client)
	cvi, cviErr := cviSvc.GetCsiVolumeInfo(ctx, expectedVolumeID)
	if cviErr != nil {
		t.Fatalf("CsiVolumeInfo not found: %v", cviErr)
	}
	if cvi.Status.Ownership != csivolumeinfov1alpha1.OwnershipStateVMManaged {
		t.Errorf("CVI ownership = %q; want VMManaged", cvi.Status.Ownership)
	}
	if cvi.Status.Phase != csivolumeinfov1alpha1.PhaseSucceeded {
		t.Errorf("CVI phase = %q; want Succeeded", cvi.Status.Phase)
	}
	if len(cvi.Spec.VMs) != 1 || cvi.Spec.VMs[0].VMName != crv.Spec.VMName {
		t.Errorf("CVI VMs = %+v; want one entry with vmName=%q", cvi.Spec.VMs, crv.Spec.VMName)
	}
	hasFinalizer := false
	for _, f := range cvi.Finalizers {
		if f == csivolumeinfov1alpha1.VolumeProtectionFinalizer {
			hasFinalizer = true
		}
	}
	if !hasFinalizer {
		t.Errorf("CVI missing finalizer %q", csivolumeinfov1alpha1.VolumeProtectionFinalizer)
	}

	// CRV status: registered=true, volumeID and pvName set.
	updated := &cnsregistervolumev1alpha1.CnsRegisterVolume{}
	if getErr := r.client.Get(ctx,
		types.NamespacedName{Name: crv.Name, Namespace: crv.Namespace}, updated); getErr != nil {
		t.Fatalf("failed to get CRV: %v", getErr)
	}
	if !updated.Status.Registered {
		t.Errorf("CRV registered = false; want true")
	}
	if updated.Status.VolumeID != expectedVolumeID {
		t.Errorf("CRV volumeID = %q; want %q", updated.Status.VolumeID, expectedVolumeID)
	}
	if updated.Status.PvName != expectedPVName {
		t.Errorf("CRV pvName = %q; want %q", updated.Status.PvName, expectedPVName)
	}
}

// TestReconcileDeferFcd_Idempotent verifies that reconcileDeferFcdRegistration
// is safe to call multiple times: PV and CVI already exist on the second run.
func TestReconcileDeferFcd_Idempotent(t *testing.T) {
	ctx := context.Background()
	pvc := importTestPVC("imported-vm-idem", "testns")
	crv := importTestCRV("imported-vm-idem", "testns", "imported-vm-idem")
	stubIsPVCBound(t)
	r := newImportReconciler(t, pvc, crv, true)

	req := reconcile.Request{NamespacedName: types.NamespacedName{Name: crv.Name, Namespace: crv.Namespace}}

	// First run.
	if _, err := r.reconcileDeferFcdRegistration(ctx, crv, req, time.Second); err != nil {
		t.Fatalf("first run: %v", err)
	}

	// Re-fetch CRV to get updated resourceVersion.
	if err := r.client.Get(ctx, req.NamespacedName, crv); err != nil {
		t.Fatalf("re-fetch crv: %v", err)
	}

	// Second run must succeed without errors.
	res, err := r.reconcileDeferFcdRegistration(ctx, crv, req, time.Second)
	if err != nil {
		t.Fatalf("second run: %v", err)
	}
	if res.RequeueAfter != 0 {
		t.Errorf("expected zero result on second run, got: %+v", res)
	}
}

// TestReconcileDeferFcd_PVCNotFound verifies that a missing PVC causes a requeue
// with an error recorded on the CRV.
func TestReconcileDeferFcd_PVCNotFound(t *testing.T) {
	ctx := context.Background()
	crv := importTestCRV("imported-vm-nopvc", "testns", "imported-vm-nopvc")
	r := newImportReconciler(t, nil /* no PVC */, crv, true)

	res, err := r.reconcileDeferFcdRegistration(ctx, crv,
		reconcile.Request{NamespacedName: types.NamespacedName{Name: crv.Name, Namespace: crv.Namespace}},
		time.Second)

	if err != nil {
		t.Fatalf("unexpected returned error (should be set on CRV status only): %v", err)
	}
	if res.RequeueAfter == 0 {
		t.Errorf("expected requeue when PVC missing, got zero result")
	}

	updated := &cnsregistervolumev1alpha1.CnsRegisterVolume{}
	if getErr := r.client.Get(ctx,
		types.NamespacedName{Name: crv.Name, Namespace: crv.Namespace}, updated); getErr != nil {
		t.Fatalf("failed to get CRV: %v", getErr)
	}
	if updated.Status.Registered {
		t.Errorf("CRV registered = true; want false when PVC missing")
	}
	if updated.Status.Error == "" {
		t.Errorf("CRV error empty; want a descriptive error when PVC missing")
	}
}

// TestReconcileDeferFcd_FSSDisabled verifies that when VMOwnedVolumes is
// disabled, the handler requeues without creating any CVI or PV.
func TestReconcileDeferFcd_FSSDisabled(t *testing.T) {
	ctx := context.Background()
	pvc := importTestPVC("imported-vm-nofss", "testns")
	crv := importTestCRV("imported-vm-nofss", "testns", "imported-vm-nofss")
	r := newImportReconciler(t, pvc, crv, false /* FSS off */)

	res, err := r.reconcileDeferFcdRegistration(ctx, crv,
		reconcile.Request{NamespacedName: types.NamespacedName{Name: crv.Name, Namespace: crv.Namespace}},
		time.Second)

	if err != nil {
		t.Fatalf("unexpected returned error: %v", err)
	}
	if res.RequeueAfter == 0 {
		t.Errorf("expected requeue when FSS disabled, got zero result")
	}

	updated := &cnsregistervolumev1alpha1.CnsRegisterVolume{}
	if getErr := r.client.Get(ctx,
		types.NamespacedName{Name: crv.Name, Namespace: crv.Namespace}, updated); getErr != nil {
		t.Fatalf("get CRV: %v", getErr)
	}
	if updated.Status.Registered {
		t.Errorf("CRV registered = true; want false when FSS disabled")
	}

	// No CVI should have been created.
	cvi := &csivolumeinfov1alpha1.CsiVolumeInfo{}
	getErr := r.client.Get(ctx, types.NamespacedName{
		Name:      csivolumeinfosvc.GetCsiVolumeInfoCRName(string(pvc.UID)),
		Namespace: csivolumeinfov1alpha1.CVINamespace,
	}, cvi)
	if getErr == nil {
		t.Errorf("CsiVolumeInfo should not exist when FSS disabled, but was found")
	}
}

// TestReconcileDeferFcd_DiskMetadataPropagated verifies that diskUUID,
// diskPath, vmName, and vmInstanceUUID are correctly propagated from the
// CRV spec to the CsiVolumeInfo spec.
func TestReconcileDeferFcd_DiskMetadataPropagated(t *testing.T) {
	ctx := context.Background()
	pvc := importTestPVC("imported-vm-diskmd", "testns")
	crv := importTestCRV("imported-vm-diskmd", "testns", "imported-vm-diskmd")
	stubIsPVCBound(t)
	r := newImportReconciler(t, pvc, crv, true)

	if _, err := r.reconcileDeferFcdRegistration(ctx, crv,
		reconcile.Request{NamespacedName: types.NamespacedName{Name: crv.Name, Namespace: crv.Namespace}},
		time.Second); err != nil {
		t.Fatalf("reconcile: %v", err)
	}

	cviSvc := csivolumeinfosvc.NewCsiVolumeInfoService(r.client)
	cvi, err := cviSvc.GetCsiVolumeInfo(ctx, string(pvc.UID))
	if err != nil {
		t.Fatalf("CVI not found: %v", err)
	}
	if cvi.Spec.DiskUUID != crv.Spec.DiskUUID {
		t.Errorf("CVI.spec.diskUUID = %q; want %q", cvi.Spec.DiskUUID, crv.Spec.DiskUUID)
	}
	if cvi.Spec.DiskPath != crv.Spec.DiskURLPath {
		t.Errorf("CVI.spec.diskPath = %q; want %q", cvi.Spec.DiskPath, crv.Spec.DiskURLPath)
	}
	if len(cvi.Spec.VMs) != 1 {
		t.Fatalf("CVI.spec.vms len = %d; want 1", len(cvi.Spec.VMs))
	}
	if cvi.Spec.VMs[0].VMName != crv.Spec.VMName {
		t.Errorf("CVI.spec.vms[0].vmName = %q; want %q", cvi.Spec.VMs[0].VMName, crv.Spec.VMName)
	}
	if cvi.Spec.VMs[0].VMInstanceUUID != crv.Spec.VMInstanceUUID {
		t.Errorf("CVI.spec.vms[0].vmInstanceUUID = %q; want %q",
			cvi.Spec.VMs[0].VMInstanceUUID, crv.Spec.VMInstanceUUID)
	}
}

// TestReconcileDeferFcd_NoFCDCall verifies that no CreateVolume call is issued
// during a successful import (no FCD registration).
func TestReconcileDeferFcd_NoFCDCall(t *testing.T) {
	ctx := context.Background()
	pvc := importTestPVC("imported-vm-nofcd", "testns")
	crv := importTestCRV("imported-vm-nofcd", "testns", "imported-vm-nofcd")
	stubIsPVCBound(t)

	createVolumeCalled := false
	r := newImportReconciler(t, pvc, crv, true)
	r.volumeManager = &mockVolumeManager{
		createVolumeFunc: func(_ context.Context, _ *cnstypes.CnsVolumeCreateSpec,
			_ interface{}) (*cnsvolume.CnsVolumeInfo, string, error) {
			createVolumeCalled = true
			return nil, "", nil
		},
	}

	if _, err := r.reconcileDeferFcdRegistration(ctx, crv,
		reconcile.Request{NamespacedName: types.NamespacedName{Name: crv.Name, Namespace: crv.Namespace}},
		time.Second); err != nil {
		t.Fatalf("reconcile: %v", err)
	}

	if createVolumeCalled {
		t.Errorf("CreateVolume was called; deferred-FCD import must not touch the FCD API")
	}
}
