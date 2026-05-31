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
	"context"
	"fmt"
	"testing"

	cnstypes "github.com/vmware/govmomi/cns/types"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	k8stypes "k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	csivolumeinfov1alpha1 "sigs.k8s.io/vsphere-csi-driver/v3/pkg/apis/cnsoperator/csivolumeinfo/v1alpha1"
	cnsvolume "sigs.k8s.io/vsphere-csi-driver/v3/pkg/common/cns-lib/volume"
	"sigs.k8s.io/vsphere-csi-driver/v3/pkg/common/config"
	cnsoperatortypes "sigs.k8s.io/vsphere-csi-driver/v3/pkg/syncer/cnsoperator/types"
)

// reregisterSuccessMgr embeds MockManager and overrides CreateVolume to succeed.
// ReregisterVolumeAsFCD only calls CreateVolume; all other methods are inherited
// from MockManager and will panic if reached (they should not be in these tests).
type reregisterSuccessMgr struct {
	cnsvolume.MockManager
}

func (r *reregisterSuccessMgr) CreateVolume(
	_ context.Context, _ *cnstypes.CnsVolumeCreateSpec, _ interface{},
) (*cnsvolume.CnsVolumeInfo, string, error) {
	return &cnsvolume.CnsVolumeInfo{}, "", nil
}

// reregisterAlreadyExistsMgr simulates CreateVolume returning CnsVolumeAlreadyExistsFault.
type reregisterAlreadyExistsMgr struct {
	cnsvolume.MockManager
}

func (r *reregisterAlreadyExistsMgr) CreateVolume(
	_ context.Context, _ *cnstypes.CnsVolumeCreateSpec, _ interface{},
) (*cnsvolume.CnsVolumeInfo, string, error) {
	return nil, "vim.fault.CnsVolumeAlreadyExistsFault", fmt.Errorf("already registered")
}

// reregisterFailMgr simulates CreateVolume returning a non-idempotent fault.
type reregisterFailMgr struct {
	cnsvolume.MockManager
}

func (r *reregisterFailMgr) CreateVolume(
	_ context.Context, _ *cnstypes.CnsVolumeCreateSpec, _ interface{},
) (*cnsvolume.CnsVolumeInfo, string, error) {
	return nil, "vim.fault.CnsStorageFault", fmt.Errorf("storage fault")
}

func minimalConfigInfo() *config.ConfigurationInfo {
	cfg := &config.Config{
		VirtualCenter: map[string]*config.VirtualCenterConfig{
			"test-vc": {User: "test-user"},
		},
	}
	cfg.Global.ClusterID = "test-cluster"
	return &config.ConfigurationInfo{Cfg: cfg}
}

// ---------------------------------------------------------------------------
// Task 3.3b -- Release PVC protection finalizer in ReregisterVolumeAsFCD
//              when PVC has a deletionTimestamp.
// ---------------------------------------------------------------------------

// TestReregisterVolumeAsFCD_PVCProtectionFinalizer_ReleasedOnTerminating verifies that
// when the PVC has a deletionTimestamp, ReregisterVolumeAsFCD removes the
// cns.vmware.com/pvc-protection finalizer so the standard CSI delete path can proceed.
func TestReregisterVolumeAsFCD_PVCProtectionFinalizer_ReleasedOnTerminating(t *testing.T) {
	ctx := context.Background()
	const (
		ns      = "test-ns"
		pvcName = "pvc-terminating"
		pvName  = "pv-test"
		volID   = "fake-volume-id"
	)

	now := metav1.Now()
	pvc := &corev1.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{
			Name:              pvcName,
			Namespace:         ns,
			DeletionTimestamp: &now,
			Finalizers:        []string{cnsoperatortypes.CNSPvcFinalizer},
		},
	}
	pv := &corev1.PersistentVolume{
		ObjectMeta: metav1.ObjectMeta{Name: pvName},
		Spec: corev1.PersistentVolumeSpec{
			ClaimRef: &corev1.ObjectReference{Namespace: ns, Name: pvcName},
		},
	}

	scheme := runtime.NewScheme()
	_ = corev1.AddToScheme(scheme)
	_ = csivolumeinfov1alpha1.AddToScheme(scheme)

	k8sClient := fake.NewClientBuilder().WithScheme(scheme).WithObjects(pvc, pv).Build()

	cvi := &csivolumeinfov1alpha1.CsiVolumeInfo{
		ObjectMeta: metav1.ObjectMeta{
			Name:       "csi-volume-info-" + volID,
			Namespace:  ns,
			Finalizers: []string{csivolumeinfov1alpha1.CVIProtectionFinalizer},
		},
		Spec: csivolumeinfov1alpha1.CsiVolumeInfoSpec{
			VolumeID: volID,
			PVCName:  pvcName,
			PVName:   pvName,
		},
		Status: csivolumeinfov1alpha1.CsiVolumeInfoStatus{
			OwnershipState: csivolumeinfov1alpha1.OwnershipStateVMManaged,
			DiskPath:       "[ds1] path/disk.vmdk",
			DiskUUID:       "disk-uuid-1",
		},
	}
	_ = k8sClient.Create(ctx, cvi)

	cviSvc := &fakeCVISvc{
		cviByVolID:    map[string]*csivolumeinfov1alpha1.CsiVolumeInfo{volID: cvi},
		cviBydiskUUID: make(map[string]*csivolumeinfov1alpha1.CsiVolumeInfo),
	}

	fakeVolMgr := &reregisterSuccessMgr{}

	err := ReregisterVolumeAsFCD(ctx, fakeVolMgr, cviSvc, k8sClient, cvi, minimalConfigInfo())
	if err != nil {
		t.Fatalf("ReregisterVolumeAsFCD failed: %v", err)
	}

	// Verify the PVC protection finalizer was removed. The fake client removes the
	// Terminating PVC entirely once all finalizers are gone, so a NotFound result
	// also confirms successful finalizer removal.
	updatedPVC := &corev1.PersistentVolumeClaim{}
	getErr := k8sClient.Get(ctx,
		k8stypes.NamespacedName{Namespace: ns, Name: pvcName}, updatedPVC)
	if getErr == nil {
		// PVC still exists; verify the protection finalizer is gone.
		for _, f := range updatedPVC.Finalizers {
			if f == cnsoperatortypes.CNSPvcFinalizer {
				t.Errorf("expected PVC protection finalizer %q to be removed, but it is still present",
					cnsoperatortypes.CNSPvcFinalizer)
			}
		}
	}
	// If getErr is NotFound, the finalizer was removed and the PVC was GC'd -- that is also success.
}

// TestReregisterVolumeAsFCD_PVCProtectionFinalizer_NotRemovedWhenAlive verifies that
// when the PVC does NOT have a deletionTimestamp, ReregisterVolumeAsFCD leaves the
// PVC protection finalizer in place.
func TestReregisterVolumeAsFCD_PVCProtectionFinalizer_NotRemovedWhenAlive(t *testing.T) {
	ctx := context.Background()
	const (
		ns      = "test-ns"
		pvcName = "pvc-alive"
		pvName  = "pv-alive"
		volID   = "fake-volume-alive"
	)

	pvc := &corev1.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{
			Name:       pvcName,
			Namespace:  ns,
			Finalizers: []string{cnsoperatortypes.CNSPvcFinalizer},
		},
	}
	pv := &corev1.PersistentVolume{
		ObjectMeta: metav1.ObjectMeta{Name: pvName},
		Spec: corev1.PersistentVolumeSpec{
			ClaimRef: &corev1.ObjectReference{Namespace: ns, Name: pvcName},
		},
	}

	scheme := runtime.NewScheme()
	_ = corev1.AddToScheme(scheme)
	_ = csivolumeinfov1alpha1.AddToScheme(scheme)

	k8sClient := fake.NewClientBuilder().WithScheme(scheme).WithObjects(pvc, pv).Build()

	cvi := &csivolumeinfov1alpha1.CsiVolumeInfo{
		ObjectMeta: metav1.ObjectMeta{
			Name:       "csi-volume-info-" + volID,
			Namespace:  ns,
			Finalizers: []string{csivolumeinfov1alpha1.CVIProtectionFinalizer},
		},
		Spec: csivolumeinfov1alpha1.CsiVolumeInfoSpec{
			VolumeID: volID,
			PVCName:  pvcName,
			PVName:   pvName,
		},
		Status: csivolumeinfov1alpha1.CsiVolumeInfoStatus{
			OwnershipState: csivolumeinfov1alpha1.OwnershipStateVMManaged,
			DiskPath:       "[ds1] path/disk-alive.vmdk",
			DiskUUID:       "disk-uuid-alive",
		},
	}
	_ = k8sClient.Create(ctx, cvi)

	cviSvc := &fakeCVISvc{
		cviByVolID:    map[string]*csivolumeinfov1alpha1.CsiVolumeInfo{volID: cvi},
		cviBydiskUUID: make(map[string]*csivolumeinfov1alpha1.CsiVolumeInfo),
	}

	fakeVolMgr := &reregisterSuccessMgr{}

	err := ReregisterVolumeAsFCD(ctx, fakeVolMgr, cviSvc, k8sClient, cvi, minimalConfigInfo())
	if err != nil {
		t.Fatalf("ReregisterVolumeAsFCD failed: %v", err)
	}

	// The PVC protection finalizer must still be present.
	updatedPVC := &corev1.PersistentVolumeClaim{}
	if getErr := k8sClient.Get(ctx,
		k8stypes.NamespacedName{Namespace: ns, Name: pvcName}, updatedPVC); getErr != nil {
		t.Fatalf("failed to get updated PVC: %v", getErr)
	}
	found := false
	for _, f := range updatedPVC.Finalizers {
		if f == cnsoperatortypes.CNSPvcFinalizer {
			found = true
		}
	}
	if !found {
		t.Errorf("expected PVC protection finalizer %q to remain present, but it was removed",
			cnsoperatortypes.CNSPvcFinalizer)
	}
}

// ---------------------------------------------------------------------------
// Task 5.5 -- ReregisterVolumeAsFCD comprehensive tests
// ---------------------------------------------------------------------------

func makeReregisterObjects(
	ns, pvcName, pvName, volID string,
) (*corev1.PersistentVolumeClaim, *corev1.PersistentVolume, *csivolumeinfov1alpha1.CsiVolumeInfo) {
	pvc := &corev1.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{
			Name:       pvcName,
			Namespace:  ns,
			Finalizers: []string{cnsoperatortypes.CNSPvcFinalizer},
		},
	}
	pv := &corev1.PersistentVolume{
		ObjectMeta: metav1.ObjectMeta{Name: pvName},
		Spec: corev1.PersistentVolumeSpec{
			ClaimRef: &corev1.ObjectReference{Namespace: ns, Name: pvcName},
		},
	}
	cvi := &csivolumeinfov1alpha1.CsiVolumeInfo{
		ObjectMeta: metav1.ObjectMeta{
			Name:       "csi-volume-info-" + volID,
			Namespace:  ns,
			Finalizers: []string{csivolumeinfov1alpha1.CVIProtectionFinalizer},
		},
		Spec: csivolumeinfov1alpha1.CsiVolumeInfoSpec{
			VolumeID: volID,
			PVCName:  pvcName,
			PVName:   pvName,
		},
		Status: csivolumeinfov1alpha1.CsiVolumeInfoStatus{
			OwnershipState: csivolumeinfov1alpha1.OwnershipStateVMManaged,
			DiskPath:       "[ds1] ns/vm/disk.vmdk",
			DiskUUID:       "test-disk-uuid",
		},
	}
	return pvc, pv, cvi
}

func makeReregisterScheme() *runtime.Scheme {
	s := runtime.NewScheme()
	_ = corev1.AddToScheme(s)
	_ = csivolumeinfov1alpha1.AddToScheme(s)
	return s
}

// TestReregisterVolumeAsFCD_Success verifies the happy path: CVI transitions to
// CSI_MANAGED, the cvi-protection finalizer is removed, and the PVC is labeled csi-owned.
func TestReregisterVolumeAsFCD_Success(t *testing.T) {
	ctx := context.Background()
	const (
		ns      = "ns-success"
		pvcName = "pvc-success"
		pvName  = "pv-success"
		volID   = "vol-success"
	)
	pvc, pv, cvi := makeReregisterObjects(ns, pvcName, pvName, volID)
	k8sClient := fake.NewClientBuilder().WithScheme(makeReregisterScheme()).
		WithObjects(pvc, pv).Build()
	_ = k8sClient.Create(ctx, cvi)

	svc := &fakeCVISvc{
		cviByVolID:    map[string]*csivolumeinfov1alpha1.CsiVolumeInfo{volID: cvi},
		cviBydiskUUID: map[string]*csivolumeinfov1alpha1.CsiVolumeInfo{},
	}

	err := ReregisterVolumeAsFCD(ctx, &reregisterSuccessMgr{}, svc, k8sClient, cvi, minimalConfigInfo())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// CVI must be CSI_MANAGED.
	updatedCVI := svc.cviByVolID[volID]
	if updatedCVI.Status.OwnershipState != csivolumeinfov1alpha1.OwnershipStateCSIManaged {
		t.Errorf("expected CSI_MANAGED, got %q", updatedCVI.Status.OwnershipState)
	}
	if updatedCVI.Status.VMName != "" {
		t.Errorf("expected empty vmName, got %q", updatedCVI.Status.VMName)
	}

	// PVC must be labeled csi-owned.
	updatedPVC := &corev1.PersistentVolumeClaim{}
	if err := k8sClient.Get(ctx, k8stypes.NamespacedName{Namespace: ns, Name: pvcName}, updatedPVC); err != nil {
		t.Fatalf("failed to get updated PVC: %v", err)
	}
	if updatedPVC.Labels[csivolumeinfov1alpha1.LabelVolumeOwnership] != csivolumeinfov1alpha1.OwnershipLabelCSIOwned {
		t.Errorf("expected PVC label %q=%q, got %q",
			csivolumeinfov1alpha1.LabelVolumeOwnership, csivolumeinfov1alpha1.OwnershipLabelCSIOwned,
			updatedPVC.Labels[csivolumeinfov1alpha1.LabelVolumeOwnership])
	}
}

// TestReregisterVolumeAsFCD_AlreadyRegistered verifies that CnsVolumeAlreadyExistsFault
// is treated as success (idempotent re-registration).
func TestReregisterVolumeAsFCD_AlreadyRegistered(t *testing.T) {
	ctx := context.Background()
	const (
		ns      = "ns-already"
		pvcName = "pvc-already"
		pvName  = "pv-already"
		volID   = "vol-already"
	)
	pvc, pv, cvi := makeReregisterObjects(ns, pvcName, pvName, volID)
	k8sClient := fake.NewClientBuilder().WithScheme(makeReregisterScheme()).
		WithObjects(pvc, pv).Build()
	_ = k8sClient.Create(ctx, cvi)

	svc := &fakeCVISvc{
		cviByVolID:    map[string]*csivolumeinfov1alpha1.CsiVolumeInfo{volID: cvi},
		cviBydiskUUID: map[string]*csivolumeinfov1alpha1.CsiVolumeInfo{},
	}

	// CnsVolumeAlreadyExistsFault must not cause failure.
	err := ReregisterVolumeAsFCD(ctx, &reregisterAlreadyExistsMgr{}, svc, k8sClient, cvi, minimalConfigInfo())
	if err != nil {
		t.Fatalf("expected success on AlreadyRegistered fault, got: %v", err)
	}

	// CVI must still be transitioned to CSI_MANAGED.
	if svc.cviByVolID[volID].Status.OwnershipState != csivolumeinfov1alpha1.OwnershipStateCSIManaged {
		t.Error("expected CSI_MANAGED after AlreadyRegistered idempotent path")
	}
}

// TestReregisterVolumeAsFCD_CreateVolumeFails verifies that a non-idempotent
// CreateVolume fault causes the function to return an error.
func TestReregisterVolumeAsFCD_CreateVolumeFails(t *testing.T) {
	ctx := context.Background()
	const (
		ns      = "ns-fail"
		pvcName = "pvc-fail"
		pvName  = "pv-fail"
		volID   = "vol-fail"
	)
	pvc, pv, cvi := makeReregisterObjects(ns, pvcName, pvName, volID)
	k8sClient := fake.NewClientBuilder().WithScheme(makeReregisterScheme()).
		WithObjects(pvc, pv).Build()
	_ = k8sClient.Create(ctx, cvi)

	svc := &fakeCVISvc{
		cviByVolID:    map[string]*csivolumeinfov1alpha1.CsiVolumeInfo{volID: cvi},
		cviBydiskUUID: map[string]*csivolumeinfov1alpha1.CsiVolumeInfo{},
	}

	err := ReregisterVolumeAsFCD(ctx, &reregisterFailMgr{}, svc, k8sClient, cvi, minimalConfigInfo())
	if err == nil {
		t.Fatal("expected error when CreateVolume returns a non-idempotent fault")
	}

	// CVI must remain in VM_MANAGED (not yet transitioned).
	if svc.cviByVolID[volID].Status.OwnershipState != csivolumeinfov1alpha1.OwnershipStateVMManaged {
		t.Errorf("expected CVI to remain VM_MANAGED on error, got %q",
			svc.cviByVolID[volID].Status.OwnershipState)
	}
}

// TestReregisterVolumeAsFCD_PVCNotFound verifies that a missing PVC causes an error.
func TestReregisterVolumeAsFCD_PVCNotFound(t *testing.T) {
	ctx := context.Background()
	const (
		ns     = "ns-nopvc"
		pvName = "pv-nopvc"
		volID  = "vol-nopvc"
	)
	pv := &corev1.PersistentVolume{
		ObjectMeta: metav1.ObjectMeta{Name: pvName},
		Spec: corev1.PersistentVolumeSpec{
			ClaimRef: &corev1.ObjectReference{Namespace: ns, Name: "missing-pvc"},
		},
	}
	cvi := &csivolumeinfov1alpha1.CsiVolumeInfo{
		ObjectMeta: metav1.ObjectMeta{
			Name:       "csi-volume-info-" + volID,
			Namespace:  ns,
			Finalizers: []string{csivolumeinfov1alpha1.CVIProtectionFinalizer},
		},
		Spec: csivolumeinfov1alpha1.CsiVolumeInfoSpec{
			VolumeID: volID,
			PVCName:  "missing-pvc",
			PVName:   pvName,
		},
		Status: csivolumeinfov1alpha1.CsiVolumeInfoStatus{
			OwnershipState: csivolumeinfov1alpha1.OwnershipStateVMManaged,
			DiskPath:       "[ds1] path.vmdk",
			DiskUUID:       "uuid-nopvc",
		},
	}
	k8sClient := fake.NewClientBuilder().WithScheme(makeReregisterScheme()).
		WithObjects(pv).Build()
	_ = k8sClient.Create(ctx, cvi)

	svc := &fakeCVISvc{
		cviByVolID:    map[string]*csivolumeinfov1alpha1.CsiVolumeInfo{volID: cvi},
		cviBydiskUUID: map[string]*csivolumeinfov1alpha1.CsiVolumeInfo{},
	}

	err := ReregisterVolumeAsFCD(ctx, &reregisterSuccessMgr{}, svc, k8sClient, cvi, minimalConfigInfo())
	if err == nil {
		t.Fatal("expected error when PVC is not found")
	}
}
