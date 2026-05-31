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
	"errors"
	"testing"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	cnsopapis "sigs.k8s.io/vsphere-csi-driver/v3/pkg/apis/cnsoperator"
	bav1alpha1 "sigs.k8s.io/vsphere-csi-driver/v3/pkg/apis/cnsoperator/cnsnodevmbatchattachment/v1alpha1"
	csivolumeinfov1alpha1 "sigs.k8s.io/vsphere-csi-driver/v3/pkg/apis/cnsoperator/csivolumeinfo/v1alpha1"
	cnsvolume "sigs.k8s.io/vsphere-csi-driver/v3/pkg/common/cns-lib/volume"
)

// ---------------------------------------------------------------------------
// Task 4.4a -- RecoverPendingUnregisters unit tests
// ---------------------------------------------------------------------------

// fakeQueryMgr wraps MockManager to control QueryPendingUnregisters output.
type fakeQueryMgr struct {
	cnsvolume.MockManager
	records  []cnsvolume.PendingUnregisterRecord
	queryErr error
	ackCalls []string
}

func (f *fakeQueryMgr) QueryPendingUnregisters(_ context.Context) ([]cnsvolume.PendingUnregisterRecord, error) {
	return f.records, f.queryErr
}

func (f *fakeQueryMgr) AckUnregister(_ context.Context, volumeID string) error {
	f.ackCalls = append(f.ackCalls, volumeID)
	return nil
}

func makeRecoveryScheme() *runtime.Scheme {
	s := runtime.NewScheme()
	_ = corev1.AddToScheme(s)
	_ = csivolumeinfov1alpha1.AddToScheme(s)
	_ = cnsopapis.AddToScheme(s)
	return s
}

// TestRecoverPendingUnregisters_EmptyList verifies that an empty pending list
// results in a no-op.
func TestRecoverPendingUnregisters_EmptyList(t *testing.T) {
	ctx := context.Background()
	mgr := &fakeQueryMgr{records: nil}
	cviSvc := &fakeCVISvc{
		cviByVolID:    make(map[string]*csivolumeinfov1alpha1.CsiVolumeInfo),
		cviBydiskUUID: make(map[string]*csivolumeinfov1alpha1.CsiVolumeInfo),
	}
	k8sClient := fake.NewClientBuilder().WithScheme(makeRecoveryScheme()).Build()

	err := RecoverPendingUnregisters(ctx, mgr, cviSvc, k8sClient)
	if err != nil {
		t.Fatalf("expected nil error, got: %v", err)
	}
	if len(mgr.ackCalls) != 0 {
		t.Errorf("expected 0 ACK calls, got %d", len(mgr.ackCalls))
	}
}

// TestRecoverPendingUnregisters_QueryError verifies that a query error is returned.
func TestRecoverPendingUnregisters_QueryError(t *testing.T) {
	ctx := context.Background()
	wantErr := errors.New("vCenter connectivity failure")
	mgr := &fakeQueryMgr{queryErr: wantErr}
	cviSvc := &fakeCVISvc{
		cviByVolID:    make(map[string]*csivolumeinfov1alpha1.CsiVolumeInfo),
		cviBydiskUUID: make(map[string]*csivolumeinfov1alpha1.CsiVolumeInfo),
	}
	k8sClient := fake.NewClientBuilder().WithScheme(makeRecoveryScheme()).Build()

	err := RecoverPendingUnregisters(ctx, mgr, cviSvc, k8sClient)
	if err == nil {
		t.Fatal("expected error, got nil")
	}
	if !errors.Is(err, wantErr) {
		t.Errorf("expected error to wrap %v, got: %v", wantErr, err)
	}
}

// TestRecoverPendingUnregisters_NoPVFound verifies that when the PV is not found
// for a pending record, the record is ACKed and recovery continues.
func TestRecoverPendingUnregisters_NoPVFound(t *testing.T) {
	ctx := context.Background()
	const volID = "vol-no-pv"
	mgr := &fakeQueryMgr{
		records: []cnsvolume.PendingUnregisterRecord{
			{VolumeID: volID, BackingDiskPath: "/ds/disk.vmdk", DiskUUID: "uuid-1"},
		},
	}
	cviSvc := &fakeCVISvc{
		cviByVolID:    make(map[string]*csivolumeinfov1alpha1.CsiVolumeInfo),
		cviBydiskUUID: make(map[string]*csivolumeinfov1alpha1.CsiVolumeInfo),
	}
	// k8sClient has no PVs registered.
	k8sClient := fake.NewClientBuilder().WithScheme(makeRecoveryScheme()).Build()

	err := RecoverPendingUnregisters(ctx, mgr, cviSvc, k8sClient)
	if err != nil {
		t.Fatalf("expected nil error, got: %v", err)
	}
	// The record should be ACKed even when PV is not found.
	if len(mgr.ackCalls) != 1 || mgr.ackCalls[0] != volID {
		t.Errorf("expected ACK for %q, got: %v", volID, mgr.ackCalls)
	}
}

// TestRecoverPendingUnregisters_CVIAlreadyTransferring verifies that when the CVI
// is already TRANSFERRING_TO_VM, it is not re-patched and the ACK is still called.
func TestRecoverPendingUnregisters_CVIAlreadyTransferring(t *testing.T) {
	ctx := context.Background()
	const (
		ns      = "test-ns"
		volID   = "vol-already-transferring"
		pvName  = "pv-" + volID
		pvcName = "pvc-" + volID
	)

	pv := &corev1.PersistentVolume{
		ObjectMeta: metav1.ObjectMeta{Name: volID},
		Spec: corev1.PersistentVolumeSpec{
			ClaimRef: &corev1.ObjectReference{Namespace: ns, Name: pvcName},
		},
	}
	ba := &bav1alpha1.CnsNodeVMBatchAttachment{
		ObjectMeta: metav1.ObjectMeta{Name: "my-vm", Namespace: ns},
		Spec: bav1alpha1.CnsNodeVMBatchAttachmentSpec{
			InstanceUUID: "vm-uuid-1",
			Volumes: []bav1alpha1.VolumeSpec{
				{
					Name: "vol-1",
					PersistentVolumeClaim: bav1alpha1.PersistentVolumeClaimSpec{
						ClaimName: pvcName,
					},
				},
			},
		},
	}
	existingCVI := &csivolumeinfov1alpha1.CsiVolumeInfo{
		ObjectMeta: metav1.ObjectMeta{Name: "csi-volume-info-" + volID, Namespace: ns},
		Spec:       csivolumeinfov1alpha1.CsiVolumeInfoSpec{VolumeID: volID, PVCName: pvcName, PVName: pvName},
		Status: csivolumeinfov1alpha1.CsiVolumeInfoStatus{
			OwnershipState: csivolumeinfov1alpha1.OwnershipStateTransferringToVM,
			DiskPath:       "/ds/old.vmdk",
			DiskUUID:       "uuid-existing",
		},
	}

	scheme := makeRecoveryScheme()
	k8sClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(pv, ba, existingCVI).
		Build()

	cviSvc := &fakeCVISvc{
		cviByVolID: map[string]*csivolumeinfov1alpha1.CsiVolumeInfo{
			volID: existingCVI,
		},
		cviBydiskUUID: make(map[string]*csivolumeinfov1alpha1.CsiVolumeInfo),
	}

	mgr := &fakeQueryMgr{
		records: []cnsvolume.PendingUnregisterRecord{
			{VolumeID: volID, BackingDiskPath: "/ds/new.vmdk", DiskUUID: "uuid-new"},
		},
	}

	err := RecoverPendingUnregisters(ctx, mgr, cviSvc, k8sClient)
	if err != nil {
		t.Fatalf("expected nil error, got: %v", err)
	}
	// ACK should be called.
	if len(mgr.ackCalls) != 1 || mgr.ackCalls[0] != volID {
		t.Errorf("expected ACK for %q, got: %v", volID, mgr.ackCalls)
	}
	// CVI should still be TRANSFERRING_TO_VM (not reset).
	cvi := cviSvc.cviByVolID[volID]
	if cvi.Status.OwnershipState != csivolumeinfov1alpha1.OwnershipStateTransferringToVM {
		t.Errorf("expected CVI to remain TRANSFERRING_TO_VM, got: %q", cvi.Status.OwnershipState)
	}
}

// ---------------------------------------------------------------------------
// Task 4.4b -- hasAttachMethodReconfig is already tested in vm_owned_volumes_test.go
// ---------------------------------------------------------------------------

// ---------------------------------------------------------------------------
// Task 4.4c -- reconcileStaleCVIs unit tests
// ---------------------------------------------------------------------------

// TestReconcileStaleCVIs_CSIManagedNoop verifies that CSI_MANAGED CVIs are skipped.
func TestReconcileStaleCVIs_CSIManagedNoop(t *testing.T) {
	ctx := context.Background()
	const (
		ns      = "test-ns"
		pvcName = "pvc-csi"
		volID   = "vol-csi"
	)

	cvi := &csivolumeinfov1alpha1.CsiVolumeInfo{
		Spec: csivolumeinfov1alpha1.CsiVolumeInfoSpec{VolumeID: volID},
		Status: csivolumeinfov1alpha1.CsiVolumeInfoStatus{
			OwnershipState: csivolumeinfov1alpha1.OwnershipStateCSIManaged,
		},
	}
	cviSvc := &fakeCVISvc{
		cviByVolID:    map[string]*csivolumeinfov1alpha1.CsiVolumeInfo{volID: cvi},
		cviBydiskUUID: make(map[string]*csivolumeinfov1alpha1.CsiVolumeInfo),
	}

	// Volume is in BA status but not in BA spec (detach delta).
	instance := &bav1alpha1.CnsNodeVMBatchAttachment{
		Spec: bav1alpha1.CnsNodeVMBatchAttachmentSpec{
			Volumes: []bav1alpha1.VolumeSpec{},
		},
		Status: bav1alpha1.CnsNodeVMBatchAttachmentStatus{
			VolumeStatus: []bav1alpha1.VolumeStatus{
				{
					Name: "vol1",
					PersistentVolumeClaim: bav1alpha1.PersistentVolumeClaimStatus{
						ClaimName:   pvcName,
						CnsVolumeID: volID,
					},
				},
			},
		},
	}
	instance.Namespace = ns

	k8sClient := fake.NewClientBuilder().WithScheme(makeRecoveryScheme()).Build()
	mgr := &fakeQueryMgr{}

	err := reconcileStaleCVIs(ctx, cviSvc, mgr, nil, k8sClient, instance)
	if err != nil {
		t.Fatalf("expected nil error, got: %v", err)
	}
	// CVI should remain CSI_MANAGED.
	if cvi.Status.OwnershipState != csivolumeinfov1alpha1.OwnershipStateCSIManaged {
		t.Errorf("expected CSI_MANAGED, got %q", cvi.Status.OwnershipState)
	}
}

// TestReconcileStaleCVIs_SnapshotRetainedNoop verifies that snapshot-retained CVIs
// (VM_MANAGED + vmName="") are skipped.
func TestReconcileStaleCVIs_SnapshotRetainedNoop(t *testing.T) {
	ctx := context.Background()
	const (
		ns      = "test-ns"
		pvcName = "pvc-snap-retained"
		volID   = "vol-snap-retained"
	)

	cvi := &csivolumeinfov1alpha1.CsiVolumeInfo{
		Spec: csivolumeinfov1alpha1.CsiVolumeInfoSpec{VolumeID: volID},
		Status: csivolumeinfov1alpha1.CsiVolumeInfoStatus{
			OwnershipState: csivolumeinfov1alpha1.OwnershipStateVMManaged,
			VMName:         "",
		},
	}
	cviSvc := &fakeCVISvc{
		cviByVolID:    map[string]*csivolumeinfov1alpha1.CsiVolumeInfo{volID: cvi},
		cviBydiskUUID: make(map[string]*csivolumeinfov1alpha1.CsiVolumeInfo),
	}

	instance := &bav1alpha1.CnsNodeVMBatchAttachment{
		Spec: bav1alpha1.CnsNodeVMBatchAttachmentSpec{Volumes: nil},
		Status: bav1alpha1.CnsNodeVMBatchAttachmentStatus{
			VolumeStatus: []bav1alpha1.VolumeStatus{
				{
					Name: "vol1",
					PersistentVolumeClaim: bav1alpha1.PersistentVolumeClaimStatus{
						ClaimName:   pvcName,
						CnsVolumeID: volID,
					},
				},
			},
		},
	}
	instance.Namespace = ns

	k8sClient := fake.NewClientBuilder().WithScheme(makeRecoveryScheme()).Build()
	mgr := &fakeQueryMgr{}

	err := reconcileStaleCVIs(ctx, cviSvc, mgr, nil, k8sClient, instance)
	if err != nil {
		t.Fatalf("expected nil error, got: %v", err)
	}
	// CVI should remain unchanged (snapshot-retained is not stale).
	if cvi.Status.OwnershipState != csivolumeinfov1alpha1.OwnershipStateVMManaged {
		t.Errorf("expected VM_MANAGED, got %q", cvi.Status.OwnershipState)
	}
}

// TestReconcileStaleCVIs_VolumeInSpec_Skipped verifies that volumes present in
// BA spec are not processed by the stale reconciler.
func TestReconcileStaleCVIs_VolumeInSpec_Skipped(t *testing.T) {
	ctx := context.Background()
	const (
		ns      = "test-ns"
		pvcName = "pvc-in-spec"
		volID   = "vol-in-spec"
	)

	cvi := &csivolumeinfov1alpha1.CsiVolumeInfo{
		Spec: csivolumeinfov1alpha1.CsiVolumeInfoSpec{VolumeID: volID},
		Status: csivolumeinfov1alpha1.CsiVolumeInfoStatus{
			OwnershipState: csivolumeinfov1alpha1.OwnershipStateTransferringToVM,
		},
	}
	cviSvc := &fakeCVISvc{
		cviByVolID:    map[string]*csivolumeinfov1alpha1.CsiVolumeInfo{volID: cvi},
		cviBydiskUUID: make(map[string]*csivolumeinfov1alpha1.CsiVolumeInfo),
	}

	// Volume IS in BA spec.
	instance := &bav1alpha1.CnsNodeVMBatchAttachment{
		Spec: bav1alpha1.CnsNodeVMBatchAttachmentSpec{
			Volumes: []bav1alpha1.VolumeSpec{
				{
					Name: "vol1",
					PersistentVolumeClaim: bav1alpha1.PersistentVolumeClaimSpec{
						ClaimName: pvcName,
					},
				},
			},
		},
		Status: bav1alpha1.CnsNodeVMBatchAttachmentStatus{
			VolumeStatus: []bav1alpha1.VolumeStatus{
				{
					Name: "vol1",
					PersistentVolumeClaim: bav1alpha1.PersistentVolumeClaimStatus{
						ClaimName:   pvcName,
						CnsVolumeID: volID,
					},
				},
			},
		},
	}
	instance.Namespace = ns

	k8sClient := fake.NewClientBuilder().WithScheme(makeRecoveryScheme()).Build()
	mgr := &fakeQueryMgr{}

	err := reconcileStaleCVIs(ctx, cviSvc, mgr, nil, k8sClient, instance)
	if err != nil {
		t.Fatalf("expected nil error, got: %v", err)
	}
	// CVI should still be TRANSFERRING_TO_VM (skipped because in spec).
	if cvi.Status.OwnershipState != csivolumeinfov1alpha1.OwnershipStateTransferringToVM {
		t.Errorf("expected TRANSFERRING_TO_VM (unchanged), got %q", cvi.Status.OwnershipState)
	}
}
