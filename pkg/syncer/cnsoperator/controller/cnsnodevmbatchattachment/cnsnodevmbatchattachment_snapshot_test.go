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

// Tests for UBMCNS-2764: detaching a volume whose disk is part of a VM snapshot must not
// loop forever, flood vCenter with tasks, or corrupt the instance status.
package cnsnodevmbatchattachment

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/vmware/govmomi/object"
	"github.com/vmware/govmomi/vim25/soap"
	vim25types "github.com/vmware/govmomi/vim25/types"
	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes"
	k8sFake "k8s.io/client-go/kubernetes/fake"
	"k8s.io/client-go/tools/record"

	"sigs.k8s.io/vsphere-csi-driver/v3/pkg/apis/cnsoperator/cnsnodevmbatchattachment/v1alpha1"
	volumes "sigs.k8s.io/vsphere-csi-driver/v3/pkg/common/cns-lib/volume"
	cnsvsphere "sigs.k8s.io/vsphere-csi-driver/v3/pkg/common/cns-lib/vsphere"
	"sigs.k8s.io/vsphere-csi-driver/v3/pkg/common/unittestcommon"
	"sigs.k8s.io/vsphere-csi-driver/v3/pkg/csi/service/common/commonco"
	cnsoperatortypes "sigs.k8s.io/vsphere-csi-driver/v3/pkg/syncer/cnsoperator/types"
)

const (
	// The fake orchestrator maps every PVC name to this volume ID, and this volume ID back
	// to snapshotTestPvc in snapshotTestPvcNamespace.
	snapshotTestVolumeID     = "12345-6789"
	snapshotTestPvc          = "mock-pvc"
	snapshotTestPvcNamespace = "mock-namespace"
	snapshotTestVolumeName   = "disk-1"
	genericDetachFailureID   = "generic-detach-failure"
)

// snapshotFaultVolumeManager is a volume manager whose DetachVolume reports the
// disk-in-VM-snapshot fault for the configured volume IDs, and which records every
// DetachVolume and BatchAttachVolumes call.
type snapshotFaultVolumeManager struct {
	unittestcommon.MockVolumeManager
	snapshotBlocked map[string]bool
	failAllAttaches bool
	detachCalls     []string
	attachRequests  []volumes.BatchAttachRequest
}

func (m *snapshotFaultVolumeManager) DetachVolume(ctx context.Context, vm *cnsvsphere.VirtualMachine,
	volumeID string) (string, error) {
	m.detachCalls = append(m.detachCalls, volumeID)
	if m.snapshotBlocked[volumeID] {
		return "vim.fault.CnsFault", fmt.Errorf("failed to detach cns volume: %q, Error: Cannot remove virtual "+
			"disk from the virtual machine because it or one of its parent disks is part of a snapshot: %w",
			volumeID, volumes.ErrDiskInVMSnapshot)
	}
	if volumeID == genericDetachFailureID {
		return "vim.fault.CnsFault", errors.New("failed to detach cns volume: some transient failure")
	}
	return "", nil
}

func (m *snapshotFaultVolumeManager) BatchAttachVolumes(ctx context.Context, vm *cnsvsphere.VirtualMachine,
	batchAttachRequest []volumes.BatchAttachRequest) ([]volumes.BatchAttachResult, string, error) {
	m.attachRequests = append(m.attachRequests, batchAttachRequest...)
	results := make([]volumes.BatchAttachResult, 0, len(batchAttachRequest))
	for _, req := range batchAttachRequest {
		if m.failAllAttaches {
			results = append(results, volumes.BatchAttachResult{VolumeID: req.VolumeID,
				Error: errors.New("fault: vim.fault.InvalidPowerState")})
			continue
		}
		results = append(results, volumes.BatchAttachResult{VolumeID: req.VolumeID, DiskUUID: "uuid-" + req.VolumeID})
	}
	if m.failAllAttaches {
		return results, "csi.fault.BatchAttach", errors.New("failed to attach all volumes")
	}
	return results, "", nil
}

// stubSnapshotSeams replaces the vCenter lookups with fakes and restores them when the test ends.
func stubSnapshotSeams(t *testing.T, hasSnapshot bool, attached map[string]FCDBackingDetails) *int {
	t.Helper()
	origSnapshot, origList := vmHasSnapshotFn, listAttachedFcdsForVMFn
	t.Cleanup(func() { vmHasSnapshotFn, listAttachedFcdsForVMFn = origSnapshot, origList })

	snapshotChecks := 0
	vmHasSnapshotFn = func(ctx context.Context, vm *cnsvsphere.VirtualMachine) (bool, error) {
		snapshotChecks++
		return hasSnapshot, nil
	}
	listAttachedFcdsForVMFn = func(ctx context.Context,
		vm *cnsvsphere.VirtualMachine) (map[string]FCDBackingDetails, error) {
		return attached, nil
	}
	return &snapshotChecks
}

func int32Ptr(v int32) *int32 { return &v }

// newSnapshotTestInstance returns an instance with one volume in spec and an Attached entry
// in status, matching the VM registered by setTestEnvironment.
func newSnapshotTestInstance() v1alpha1.CnsNodeVMBatchAttachment {
	instance := setupTestCnsNodeVMBatchAttachment()
	instance.Spec.Volumes = []v1alpha1.VolumeSpec{{
		Name: snapshotTestVolumeName,
		PersistentVolumeClaim: v1alpha1.PersistentVolumeClaimSpec{
			ClaimName:     snapshotTestPvc,
			DiskMode:      v1alpha1.Persistent,
			SharingMode:   v1alpha1.SharingNone,
			ControllerKey: int32Ptr(1000),
			UnitNumber:    int32Ptr(1),
		},
	}}
	instance.Status.VolumeStatus = []v1alpha1.VolumeStatus{{
		Name: snapshotTestVolumeName,
		PersistentVolumeClaim: v1alpha1.PersistentVolumeClaimStatus{
			ClaimName:   snapshotTestPvc,
			CnsVolumeID: snapshotTestVolumeID,
			Conditions: []metav1.Condition{{
				Type: v1alpha1.ConditionAttached, Status: metav1.ConditionTrue,
				LastTransitionTime: metav1.NewTime(time.Now().Add(-time.Hour)),
			}},
		},
	}}
	return instance
}

// useOrchestratorWithAttachedPvc makes the fake orchestrator return a PVC that was
// attached through CnsNodeVMBatchAttachment, so it is eligible for detach.
func useOrchestratorWithAttachedPvc() {
	orchestrator := &unittestcommon.FakeK8SOrchestrator{}
	orchestrator.SetPVCs([]*v1.PersistentVolumeClaim{{
		ObjectMeta: metav1.ObjectMeta{
			Name:        snapshotTestPvc,
			Namespace:   snapshotTestPvcNamespace,
			Annotations: map[string]string{cnsoperatortypes.UsedByVMAnnotationPrefix + "vm": "test-uuid"},
			Finalizers:  []string{cnsoperatortypes.CNSPvcFinalizer},
		},
		Spec: v1.PersistentVolumeClaimSpec{AccessModes: []v1.PersistentVolumeAccessMode{v1.ReadWriteOnce}},
	}})
	commonco.ContainerOrchestratorUtility = orchestrator
}

// clientSetWithPvcs returns a clientset with RWO PVCs in testNamespace that already carry
// the CNS PVC finalizer, so a successful attach does not need to patch them.
func clientSetWithPvcs(names ...string) kubernetes.Interface {
	objects := make([]runtime.Object, 0, len(names))
	for _, name := range names {
		objects = append(objects, &v1.PersistentVolumeClaim{
			ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: testNamespace,
				Finalizers: []string{cnsoperatortypes.CNSPvcFinalizer}},
			Spec: v1.PersistentVolumeClaimSpec{AccessModes: []v1.PersistentVolumeAccessMode{v1.ReadWriteOnce}},
		})
	}
	return k8sFake.NewClientset(objects...)
}

func clientSetWithSnapshotTestPvc() kubernetes.Interface {
	return clientSetWithPvcs(snapshotTestPvc)
}

// runReconcilePass runs the same steps as one reconcile of an instance without a deletion
// timestamp: compute the volumes to attach and detach, then detach and attach them.
func runReconcilePass(t *testing.T, r *Reconciler, k8sClient kubernetes.Interface,
	instance *v1alpha1.CnsNodeVMBatchAttachment) error {
	t.Helper()
	ctx := context.TODO()
	vm := testVM()
	dedupeVolumeStatus(ctx, instance)
	toAttach, toDetach, err := getVolumesToAttachAndDetach(ctx, instance, vm, r.client, k8sClient,
		r.cnsOperatorClient)
	require.NoError(t, err)
	return r.reconcileInstanceWithoutDeletionTimestamp(ctx, k8sClient, instance, toDetach, toAttach, vm)
}

func assertUniqueStatusNames(t *testing.T, instance *v1alpha1.CnsNodeVMBatchAttachment) {
	t.Helper()
	seen := make(map[string]bool)
	for _, volume := range instance.Status.VolumeStatus {
		assert.False(t, seen[volume.Name], "duplicate status.volumes name %q: %+v", volume.Name,
			instance.Status.VolumeStatus)
		seen[volume.Name] = true
	}
}

func findCondition(volume v1alpha1.VolumeStatus, conditionType string) *metav1.Condition {
	for i := range volume.PersistentVolumeClaim.Conditions {
		if volume.PersistentVolumeClaim.Conditions[i].Type == conditionType {
			return &volume.PersistentVolumeClaim.Conditions[i]
		}
	}
	return nil
}

func statusEntriesForPvc(instance *v1alpha1.CnsNodeVMBatchAttachment, pvc string) []v1alpha1.VolumeStatus {
	var entries []v1alpha1.VolumeStatus
	for _, volume := range instance.Status.VolumeStatus {
		if volume.PersistentVolumeClaim.ClaimName == pvc {
			entries = append(entries, volume)
		}
	}
	return entries
}

func blockedStatusEntry(pvc, name string) v1alpha1.VolumeStatus {
	return v1alpha1.VolumeStatus{
		Name: name,
		PersistentVolumeClaim: v1alpha1.PersistentVolumeClaimStatus{
			ClaimName: pvc,
			Conditions: []metav1.Condition{{
				Type: v1alpha1.ConditionDetached, Status: metav1.ConditionFalse,
				Reason: v1alpha1.ReasonDetachBlockedBySnapshot, LastTransitionTime: metav1.Now(),
			}},
		},
	}
}

// N1: the exact loop from the ticket. The PVC is in spec, attached with a different unit
// number, and its detach is refused because of a VM snapshot. Before the fix, the second
// reconcile produced two "disk-1:detaching" entries, which the API server rejects.
func TestReconcile_BackingMismatchDetachFails_NoDuplicateStatusAcrossReconciles(t *testing.T) {
	instance := newSnapshotTestInstance()
	r := setTestEnvironment(&instance, false)
	useOrchestratorWithAttachedPvc()
	checks := stubSnapshotSeams(t, true, map[string]FCDBackingDetails{
		snapshotTestVolumeID: {ControllerKey: 1000, UnitNumber: 2, DiskMode: "persistent", SharingMode: "sharingNone"},
	})
	vm := &snapshotFaultVolumeManager{snapshotBlocked: map[string]bool{snapshotTestVolumeID: true},
		failAllAttaches: true}
	r.volumeManager = vm
	k8sClient := clientSetWithSnapshotTestPvc()

	for pass := 1; pass <= 3; pass++ {
		err := runReconcilePass(t, r, k8sClient, &instance)
		require.Error(t, err, "pass %d", pass)
		assert.True(t, isOnlyDetachBlockedBySnapshot(err), "pass %d: %v", pass, err)
		assertUniqueStatusNames(t, &instance)
		require.NoError(t, updateInstanceStatus(context.TODO(), r.client, &instance), "pass %d", pass)
	}

	// vCenter saw exactly one detach. Every later pass was gated by the VM snapshot check.
	assert.Equal(t, []string{snapshotTestVolumeID}, vm.detachCalls)
	assert.Empty(t, vm.attachRequests, "re-attach must wait for the detach")
	assert.Equal(t, 2, *checks, "one snapshot property read per gated pass")

	entries := statusEntriesForPvc(&instance, snapshotTestPvc)
	require.Len(t, entries, 1)
	assert.Equal(t, snapshotTestVolumeName+detachSuffix, entries[0].Name)
	assert.Equal(t, v1alpha1.ReasonDetachBlockedBySnapshot,
		findCondition(entries[0], v1alpha1.ConditionDetached).Reason)
	assert.Equal(t, v1alpha1.ReasonAttachPendingDetach,
		findCondition(entries[0], v1alpha1.ConditionAttached).Reason)
}

// N2: an instance that is already wedged with the ticket's status shape heals.
func TestDedupeVolumeStatus_TicketShape(t *testing.T) {
	older := metav1.NewTime(time.Now().Add(-time.Hour))
	newer := metav1.NewTime(time.Now())
	entry := func(name, msg string, at metav1.Time) v1alpha1.VolumeStatus {
		return v1alpha1.VolumeStatus{Name: name, PersistentVolumeClaim: v1alpha1.PersistentVolumeClaimStatus{
			ClaimName: "pvc-1",
			Conditions: []metav1.Condition{{Type: v1alpha1.ConditionDetached, Status: metav1.ConditionFalse,
				Reason: v1alpha1.ReasonDetachFailed, Message: msg, LastTransitionTime: at}},
		}}
	}
	instance := &v1alpha1.CnsNodeVMBatchAttachment{Status: v1alpha1.CnsNodeVMBatchAttachmentStatus{
		VolumeStatus: []v1alpha1.VolumeStatus{
			entry("vol-1"+detachSuffix, "old", older),
			entry("vol-2", "other", older),
			entry("vol-1"+detachSuffix, "new", newer),
			entry("vol-1", "twin", older),
		},
	}}

	dedupeVolumeStatus(context.TODO(), instance)

	require.Len(t, instance.Status.VolumeStatus, 3)
	assertUniqueStatusNames(t, instance)
	assert.Equal(t, "vol-1"+detachSuffix, instance.Status.VolumeStatus[0].Name)
	assert.Equal(t, "new", instance.Status.VolumeStatus[0].PersistentVolumeClaim.Conditions[0].Message,
		"the duplicate with the newest condition is kept")
	assert.Equal(t, "vol-2", instance.Status.VolumeStatus[1].Name)
	assert.Equal(t, "vol-1", instance.Status.VolumeStatus[2].Name)
}

// N3: a valid status is not modified.
func TestDedupeVolumeStatus_NoDuplicates_NoOp(t *testing.T) {
	instance := setupTestCnsNodeVMBatchAttachment()
	before := instance.DeepCopy()

	dedupeVolumeStatus(context.TODO(), &instance)

	assert.Equal(t, before.Status.VolumeStatus, instance.Status.VolumeStatus)
}

// N4: renaming for detach removes the un-suffixed twin instead of creating a duplicate.
func TestUpdatePvcStatusEntryName_DropsTwinWhenSuffixedExists(t *testing.T) {
	instance := &v1alpha1.CnsNodeVMBatchAttachment{Status: v1alpha1.CnsNodeVMBatchAttachmentStatus{
		VolumeStatus: []v1alpha1.VolumeStatus{
			{Name: "vol1" + detachSuffix, PersistentVolumeClaim: v1alpha1.PersistentVolumeClaimStatus{ClaimName: "pvc-1"}},
			{Name: "vol1", PersistentVolumeClaim: v1alpha1.PersistentVolumeClaimStatus{ClaimName: "pvc-1"}},
			{Name: "vol2", PersistentVolumeClaim: v1alpha1.PersistentVolumeClaimStatus{ClaimName: "pvc-2"}},
		},
	}}

	updatePvcStatusEntryName(context.TODO(), instance, map[string]string{"pvc-1": "vol-id-1"})

	require.Len(t, instance.Status.VolumeStatus, 2)
	assert.Equal(t, "vol1"+detachSuffix, instance.Status.VolumeStatus[0].Name)
	assert.Equal(t, "vol2", instance.Status.VolumeStatus[1].Name)
}

// N5: a failed attach records its condition on the detaching entry instead of adding a twin.
func TestUpdateInstanceVolumeStatusByVolumeName_NoTwinOnFailedAttach(t *testing.T) {
	instance := &v1alpha1.CnsNodeVMBatchAttachment{Status: v1alpha1.CnsNodeVMBatchAttachmentStatus{
		VolumeStatus: []v1alpha1.VolumeStatus{
			{Name: "vol1" + detachSuffix, PersistentVolumeClaim: v1alpha1.PersistentVolumeClaimStatus{ClaimName: "pvc-1"}},
		},
	}}

	updateInstanceVolumeStatus(context.TODO(), instance, "vol1", "pvc-1", "vol-id-1", "",
		errors.New("InvalidPowerState"), v1alpha1.ConditionAttached, v1alpha1.ReasonAttachFailed)

	require.Len(t, instance.Status.VolumeStatus, 1)
	cond := findCondition(instance.Status.VolumeStatus[0], v1alpha1.ConditionAttached)
	require.NotNil(t, cond)
	assert.Equal(t, v1alpha1.ReasonAttachFailed, cond.Reason)
}

// N5 (counterpart): a successful attach still creates a fresh entry, as before.
func TestUpdateInstanceVolumeStatusByVolumeName_SuccessfulAttachStillAddsEntry(t *testing.T) {
	instance := &v1alpha1.CnsNodeVMBatchAttachment{Status: v1alpha1.CnsNodeVMBatchAttachmentStatus{
		VolumeStatus: []v1alpha1.VolumeStatus{
			{Name: "vol1" + detachSuffix, PersistentVolumeClaim: v1alpha1.PersistentVolumeClaimStatus{ClaimName: "pvc-1"}},
		},
	}}

	updateInstanceVolumeStatus(context.TODO(), instance, "vol1", "pvc-2", "vol-id-2", "uuid",
		nil, v1alpha1.ConditionAttached, "")

	require.Len(t, instance.Status.VolumeStatus, 2)
	assert.Equal(t, "vol1", instance.Status.VolumeStatus[1].Name)
	assert.Equal(t, "pvc-2", instance.Status.VolumeStatus[1].PersistentVolumeClaim.ClaimName)
}

// N6: the snapshot fault sets DetachBlockedBySnapshot and emits a warning event.
func TestDetachVolumes_SnapshotFault_SetsBlockedReason(t *testing.T) {
	instance := setupTestCnsNodeVMBatchAttachment()
	r := setTestEnvironment(&instance, false)
	stubSnapshotSeams(t, true, nil)
	vm := &snapshotFaultVolumeManager{snapshotBlocked: map[string]bool{"vol-id-1": true}}
	r.volumeManager = vm

	failed, err := r.processDetach(context.TODO(), getClientSetWithPvc(), testVM(),
		&instance, map[string]string{"pvc-1": "vol-id-1"})

	assert.Equal(t, []string{"pvc-1"}, failed)
	assert.True(t, isOnlyDetachBlockedBySnapshot(err))
	assert.ErrorIs(t, err, volumes.ErrDiskInVMSnapshot)
	assert.True(t, isDetachBlockedBySnapshot(&instance, "pvc-1"))
	cond := findCondition(statusEntriesForPvc(&instance, "pvc-1")[0], v1alpha1.ConditionDetached)
	assert.Contains(t, cond.Message, "part of a VM snapshot")

	events := r.recorder.(*record.FakeRecorder).Events
	require.Len(t, events, 1)
	assert.Contains(t, <-events, v1alpha1.ReasonDetachBlockedBySnapshot)
}

// N7: a blocked volume on a VM that still has snapshots issues no vCenter call.
func TestDetachVolumes_BlockedAndVmHasSnapshot_SkipsVcCall(t *testing.T) {
	instance := setupTestCnsNodeVMBatchAttachment()
	instance.Status.VolumeStatus = []v1alpha1.VolumeStatus{
		blockedStatusEntry("pvc-1", "disk-1"+detachSuffix),
		blockedStatusEntry("pvc-2", "disk-2"+detachSuffix),
	}
	r := setTestEnvironment(&instance, false)
	checks := stubSnapshotSeams(t, true, nil)
	vm := &snapshotFaultVolumeManager{}
	r.volumeManager = vm

	failed, err := r.processDetach(context.TODO(), getClientSetWithPvc(), testVM(),
		&instance, map[string]string{"pvc-1": "vol-id-1", "pvc-2": "vol-id-2"})

	assert.Empty(t, vm.detachCalls, "DetachVolume must not be called while the VM has snapshots")
	assert.ElementsMatch(t, []string{"pvc-1", "pvc-2"}, failed)
	assert.True(t, isOnlyDetachBlockedBySnapshot(err))
	assert.Equal(t, 1, *checks, "the snapshot property is read once per reconcile")
	assert.Empty(t, r.recorder.(*record.FakeRecorder).Events, "no repeated event while gated")
}

// N8: once the VM snapshots are deleted, the gated detach is attempted and succeeds.
func TestDetachVolumes_BlockedAndSnapshotDeleted_RetriesAndSucceeds(t *testing.T) {
	instance := setupTestCnsNodeVMBatchAttachment()
	instance.Status.VolumeStatus = []v1alpha1.VolumeStatus{blockedStatusEntry("pvc-1", "disk-1"+detachSuffix)}
	r := setTestEnvironment(&instance, false)
	stubSnapshotSeams(t, false, nil)
	commonco.ContainerOrchestratorUtility = &unittestcommon.FakeK8SOrchestrator{}
	vm := &snapshotFaultVolumeManager{}
	r.volumeManager = vm

	failed, err := r.processDetach(context.TODO(), getClientSetWithPvc(), testVM(),
		&instance, map[string]string{"pvc-1": "vol-id-1"})

	require.NoError(t, err)
	assert.Empty(t, failed)
	assert.Equal(t, []string{"vol-id-1"}, vm.detachCalls)
	assert.Empty(t, statusEntriesForPvc(&instance, "pvc-1"), "the detached volume's entry is removed")
}

// N9: a disk without the blocked reason is detached even if the VM has snapshots, so
// independent disks, which are not part of a snapshot, are never gated.
func TestDetachVolumes_IndependentDiskNotGatedWithoutFault(t *testing.T) {
	instance := setupTestCnsNodeVMBatchAttachment()
	r := setTestEnvironment(&instance, false)
	checks := stubSnapshotSeams(t, true, nil)
	commonco.ContainerOrchestratorUtility = &unittestcommon.FakeK8SOrchestrator{}
	vm := &snapshotFaultVolumeManager{}
	r.volumeManager = vm

	failed, err := r.processDetach(context.TODO(), getClientSetWithPvc(), testVM(),
		&instance, map[string]string{"pvc-1": "vol-id-1"})

	require.NoError(t, err)
	assert.Empty(t, failed)
	assert.Equal(t, []string{"vol-id-1"}, vm.detachCalls)
	assert.Equal(t, 0, *checks, "the snapshot property is only read for volumes already blocked")
}

// N10: a PVC in both lists whose detach failed is not sent to BatchAttachVolumes.
func TestReconcileWithoutDeletionTimestamp_FailedDetachExcludedFromAttach(t *testing.T) {
	instance := newSnapshotTestInstance()
	instance.Status.VolumeStatus[0].Name = snapshotTestVolumeName + detachSuffix
	r := setTestEnvironment(&instance, false)
	stubSnapshotSeams(t, true, nil)
	vm := &snapshotFaultVolumeManager{snapshotBlocked: map[string]bool{snapshotTestVolumeID: true}}
	r.volumeManager = vm

	err := r.reconcileInstanceWithoutDeletionTimestamp(context.TODO(), clientSetWithSnapshotTestPvc(), &instance,
		map[string]string{snapshotTestPvc: snapshotTestVolumeID},
		map[string]string{snapshotTestPvc: snapshotTestVolumeID}, testVM())

	assert.True(t, isOnlyDetachBlockedBySnapshot(err), "%v", err)
	assert.Empty(t, vm.attachRequests)
	entries := statusEntriesForPvc(&instance, snapshotTestPvc)
	require.Len(t, entries, 1)
	assert.Equal(t, v1alpha1.ReasonAttachPendingDetach, findCondition(entries[0], v1alpha1.ConditionAttached).Reason)
}

// N11: a blocked detach of one volume does not hold back the attach of another.
func TestReconcileWithoutDeletionTimestamp_OtherVolumesStillAttach(t *testing.T) {
	instance := setupTestCnsNodeVMBatchAttachment()
	for i := range instance.Spec.Volumes {
		instance.Spec.Volumes[i].PersistentVolumeClaim.DiskMode = v1alpha1.Persistent
		instance.Spec.Volumes[i].PersistentVolumeClaim.SharingMode = v1alpha1.SharingNone
		instance.Spec.Volumes[i].PersistentVolumeClaim.ControllerKey = int32Ptr(1000)
		instance.Spec.Volumes[i].PersistentVolumeClaim.UnitNumber = int32Ptr(int32(i))
	}
	r := setTestEnvironment(&instance, false)
	stubSnapshotSeams(t, true, nil)
	commonco.ContainerOrchestratorUtility = &unittestcommon.FakeK8SOrchestrator{}
	vm := &snapshotFaultVolumeManager{snapshotBlocked: map[string]bool{"vol-id-1": true}}
	r.volumeManager = vm

	err := r.reconcileInstanceWithoutDeletionTimestamp(context.TODO(), clientSetWithPvcs("pvc-1", "pvc-2"), &instance,
		map[string]string{"pvc-1": "vol-id-1"},
		map[string]string{"pvc-1": "vol-id-1", "pvc-2": "vol-id-2"}, testVM())

	assert.True(t, isOnlyDetachBlockedBySnapshot(err), "%v", err)
	require.Len(t, vm.attachRequests, 1)
	assert.Equal(t, "vol-id-2", vm.attachRequests[0].VolumeID)
}

// N12: disk and sharing modes are read from every disk backing type.
func TestDiskBackingModes(t *testing.T) {
	tests := []struct {
		name        string
		backing     vim25types.BaseVirtualDeviceBackingInfo
		wantMode    string
		wantSharing string
	}{
		{"FlatVer2", &vim25types.VirtualDiskFlatVer2BackingInfo{DiskMode: "persistent", Sharing: "sharingMultiWriter"},
			"persistent", "sharingMultiWriter"},
		{"FlatVer2 without sharing is unchanged", &vim25types.VirtualDiskFlatVer2BackingInfo{DiskMode: "persistent"},
			"persistent", ""},
		{"SeSparse", &vim25types.VirtualDiskSeSparseBackingInfo{DiskMode: "persistent"}, "persistent", "sharingNone"},
		{"SparseVer2", &vim25types.VirtualDiskSparseVer2BackingInfo{DiskMode: "independent_persistent"},
			"independent_persistent", "sharingNone"},
		{"SparseVer1", &vim25types.VirtualDiskSparseVer1BackingInfo{DiskMode: "persistent"}, "persistent", "sharingNone"},
		{"FlatVer1", &vim25types.VirtualDiskFlatVer1BackingInfo{DiskMode: "persistent"}, "persistent", "sharingNone"},
		{"RDM", &vim25types.VirtualDiskRawDiskMappingVer1BackingInfo{DiskMode: "persistent", Sharing: "sharingNone"},
			"persistent", "sharingNone"},
		{"LocalPMem", &vim25types.VirtualDiskLocalPMemBackingInfo{DiskMode: "persistent"}, "persistent", "sharingNone"},
		{"unknown backing", &vim25types.VirtualDiskPartitionedRawDiskVer2BackingInfo{}, "", ""},
		{"nil backing", nil, "", ""},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			disk := &vim25types.VirtualDisk{VirtualDevice: vim25types.VirtualDevice{Backing: tc.backing}}
			mode, sharing := diskBackingModes(disk)
			assert.Equal(t, tc.wantMode, mode)
			assert.Equal(t, tc.wantSharing, sharing)
		})
	}
}

// N13: a SeSparse disk that matches the spec is not planned for detach. Before the fix its
// modes were read as empty strings, so it was always detached and re-attached.
func TestGetVolumesToDetachFromInstance_SeSparseMatchingSpec_NoDetach(t *testing.T) {
	instance := setupTestCnsNodeVMBatchAttachment()
	commonco.ContainerOrchestratorUtility = &unittestcommon.FakeK8SOrchestrator{}
	disk := &vim25types.VirtualDisk{VirtualDevice: vim25types.VirtualDevice{
		Backing: &vim25types.VirtualDiskSeSparseBackingInfo{DiskMode: "persistent"}}}
	mode, sharing := diskBackingModes(disk)
	spec := FCDBackingDetails{ControllerKey: 1000, UnitNumber: 1, DiskMode: "persistent", SharingMode: "sharingNone"}

	attached := map[string]FCDBackingDetails{"with-used-by-annotation-1": {ControllerKey: 1000, UnitNumber: 1,
		DiskMode: mode, SharingMode: sharing}}
	pvcsToDetach, err := getVolumesToDetachFromInstance(context.TODO(), &instance, attached,
		map[string]FCDBackingDetails{"with-used-by-annotation-1": spec})
	require.NoError(t, err)
	assert.Empty(t, pvcsToDetach)

	// The comparison is still sensitive: a real disk mode change is detected.
	spec.DiskMode = "independent_persistent"
	pvcsToDetach, err = getVolumesToDetachFromInstance(context.TODO(), &instance, attached,
		map[string]FCDBackingDetails{"with-used-by-annotation-1": spec})
	require.NoError(t, err)
	assert.Equal(t, map[string]string{"with-used-by-annotation": "with-used-by-annotation-1"}, pvcsToDetach)
}

// N15: when the status update fails, the event still carries the reconcile error.
func TestCompleteReconciliationWithError_StatusUpdateFails_EventHasRootCause(t *testing.T) {
	instance := setupTestCnsNodeVMBatchAttachment()
	r := setTestEnvironment(&instance, false)
	missing := instance.DeepCopy()
	missing.Name = "not-in-the-api-server"
	reconcileErr := &detachBlockedBySnapshotError{pvcs: []string{"pvc-1"}}

	_, err := r.completeReconciliationWithError(context.TODO(), missing,
		types.NamespacedName{Namespace: missing.Namespace, Name: missing.Name}, time.Second, reconcileErr)
	require.NoError(t, err)

	events := r.recorder.(*record.FakeRecorder).Events
	require.Len(t, events, 1)
	event := <-events
	assert.Contains(t, event, "part of a VM snapshot", "root cause must be reported")
	assert.Contains(t, event, "Failed to update the instance status")
}

// N16: when every failure is a snapshot-blocked detach, the requeue is at the maximum backoff.
func TestReconcile_OnlySnapshotBlockedFailures_RequeueAtMaxBackoff(t *testing.T) {
	blocked := &detachBlockedBySnapshotError{pvcs: []string{"pvc-1"}}
	tests := []struct {
		name string
		err  error
		want time.Duration
	}{
		{"only blocked", blocked, cnsoperatortypes.MaxBackOffDurationForReconciler},
		{"blocked joined with nil attach error", errors.Join(nil, blocked),
			cnsoperatortypes.MaxBackOffDurationForReconciler},
		{"blocked and attach failure", errors.Join(errors.New("attach failed"), blocked), time.Second},
		{"generic failure", errors.New("failed to detach volumes: pvc-1"), time.Second},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			instance := setupTestCnsNodeVMBatchAttachment()
			r := setTestEnvironment(&instance, false)
			res, err := r.completeReconciliationWithError(context.TODO(), &instance,
				types.NamespacedName{Namespace: instance.Namespace, Name: instance.Name}, time.Second, tc.err)
			require.NoError(t, err)
			assert.Equal(t, tc.want, res.RequeueAfter)
		})
	}
}

// R1: a normal backing change on a VM without snapshots still detaches and re-attaches.
func TestReconcile_BackingChange_DetachSucceeds_ThenReattach(t *testing.T) {
	instance := newSnapshotTestInstance()
	r := setTestEnvironment(&instance, false)
	useOrchestratorWithAttachedPvc()
	stubSnapshotSeams(t, false, map[string]FCDBackingDetails{
		snapshotTestVolumeID: {ControllerKey: 1000, UnitNumber: 2, DiskMode: "persistent", SharingMode: "sharingNone"},
	})
	vm := &snapshotFaultVolumeManager{}
	r.volumeManager = vm
	// The PVC finalizer is patched through the controller-runtime client after the
	// re-attach, so the PVC must exist there with the same resource version.
	require.NoError(t, r.client.Create(context.TODO(), &v1.PersistentVolumeClaim{ObjectMeta: metav1.ObjectMeta{
		Name: snapshotTestPvc, Namespace: testNamespace}}))
	k8sClient := k8sFake.NewClientset(&v1.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{Name: snapshotTestPvc, Namespace: testNamespace, ResourceVersion: "1"},
		Spec:       v1.PersistentVolumeClaimSpec{AccessModes: []v1.PersistentVolumeAccessMode{v1.ReadWriteOnce}},
	})

	err := runReconcilePass(t, r, k8sClient, &instance)

	require.NoError(t, err)
	assert.Equal(t, []string{snapshotTestVolumeID}, vm.detachCalls)
	require.Len(t, vm.attachRequests, 1)
	assert.Equal(t, int32(1), *vm.attachRequests[0].UnitNumber, "re-attached with the spec's unit number")
	entries := statusEntriesForPvc(&instance, snapshotTestPvc)
	require.Len(t, entries, 1)
	assert.Equal(t, snapshotTestVolumeName, entries[0].Name)
	assert.Equal(t, metav1.ConditionTrue, findCondition(entries[0], v1alpha1.ConditionAttached).Status)
}

// R2: a ManagedObjectNotFound for the VM is still treated as a successful detach.
func TestDetachVolumes_ManagedObjectNotFound_TreatedAsDetached(t *testing.T) {
	instance := setupTestCnsNodeVMBatchAttachment()
	r := setTestEnvironment(&instance, false)
	stubSnapshotSeams(t, true, nil)
	commonco.ContainerOrchestratorUtility = &unittestcommon.FakeK8SOrchestrator{}
	vmRef := vim25types.ManagedObjectReference{Type: "VirtualMachine", Value: "vm-1"}
	r.volumeManager = &monfVolumeManager{ref: vmRef}

	failed, err := r.processDetach(context.TODO(), getClientSetWithPvc(), newVMWithRef(vmRef),
		&instance, map[string]string{"pvc-1": "vol-id-1"})

	require.NoError(t, err)
	assert.Empty(t, failed)
	assert.Empty(t, statusEntriesForPvc(&instance, "pvc-1"))
}

// R3: other faults are not gated and are not reported as snapshot-blocked.
func TestDetachVolumes_GenericFault_StillRetriedNextReconcile(t *testing.T) {
	instance := setupTestCnsNodeVMBatchAttachment()
	r := setTestEnvironment(&instance, false)
	checks := stubSnapshotSeams(t, true, nil)
	vm := &snapshotFaultVolumeManager{}
	r.volumeManager = vm
	toDetach := map[string]string{"pvc-1": genericDetachFailureID}

	for pass := 1; pass <= 2; pass++ {
		failed, err := r.processDetach(context.TODO(), getClientSetWithPvc(), testVM(),
			&instance, toDetach)
		assert.Equal(t, []string{"pvc-1"}, failed)
		assert.EqualError(t, err, "failed to detach volumes: pvc-1")
		assert.False(t, isOnlyDetachBlockedBySnapshot(err))
	}
	assert.Equal(t, []string{genericDetachFailureID, genericDetachFailureID}, vm.detachCalls)
	assert.Equal(t, 0, *checks)
	assert.Equal(t, v1alpha1.ReasonDetachFailed,
		findCondition(statusEntriesForPvc(&instance, "pvc-1")[0], v1alpha1.ConditionDetached).Reason)
}

// R4: an instance being deleted keeps its finalizer while the detach is blocked, and does
// not call vCenter again once the block is recorded.
func TestReconcileWithDeletionTimestamp_SnapshotBlocked_FinalizerKept(t *testing.T) {
	instance := setupTestCnsNodeVMBatchAttachment()
	instance.Finalizers = []string{cnsoperatortypes.CNSFinalizer}
	r := setTestEnvironment(&instance, true)
	stubSnapshotSeams(t, true, nil)
	vm := &snapshotFaultVolumeManager{snapshotBlocked: map[string]bool{"vol-id-1": true}}
	r.volumeManager = vm
	toDetach := map[string]string{"pvc-1": "vol-id-1"}

	for pass := 1; pass <= 2; pass++ {
		err := r.reconcileInstanceWithDeletionTimestamp(context.TODO(), getClientSetWithPvc(), &instance,
			toDetach, testVM())
		assert.True(t, isOnlyDetachBlockedBySnapshot(err), "pass %d: %v", pass, err)
	}
	assert.Equal(t, []string{"vol-id-1"}, vm.detachCalls)
	assert.Contains(t, instance.Finalizers, cnsoperatortypes.CNSFinalizer)
}

// R5: the original purpose of the ":detaching" suffix still works. disk-1 moves from
// pvc-1 to pvc-2, so pvc-1 is detached and pvc-2 is attached under the same volume name.
func TestUpdatePvcStatusEntryName_DiskReassignedToDifferentPvc(t *testing.T) {
	instance := &v1alpha1.CnsNodeVMBatchAttachment{Status: v1alpha1.CnsNodeVMBatchAttachmentStatus{
		VolumeStatus: []v1alpha1.VolumeStatus{
			{Name: "disk-1", PersistentVolumeClaim: v1alpha1.PersistentVolumeClaimStatus{ClaimName: "pvc-1"}},
		},
	}}

	updatePvcStatusEntryName(context.TODO(), instance, map[string]string{"pvc-1": "vol-id-1"})
	updateInstanceVolumeStatus(context.TODO(), instance, "disk-1", "pvc-2", "vol-id-2", "uuid-2", nil,
		v1alpha1.ConditionAttached, "")

	require.Len(t, instance.Status.VolumeStatus, 2)
	assertUniqueStatusNames(t, instance)
	assert.Equal(t, "disk-1"+detachSuffix, statusEntriesForPvc(instance, "pvc-1")[0].Name)
	assert.Equal(t, "disk-1", statusEntriesForPvc(instance, "pvc-2")[0].Name)
}

// R6: stale entries are still removed once spec and vCenter agree, with dedup in place.
func TestRemoveStaleEntriesFromInstanceStatus_Unchanged(t *testing.T) {
	instance := setupTestCnsNodeVMBatchAttachment()
	instance.Spec.Volumes = instance.Spec.Volumes[:1]
	r := setTestEnvironment(&instance, false)
	commonco.ContainerOrchestratorUtility = &unittestcommon.FakeK8SOrchestrator{}

	dedupeVolumeStatus(context.TODO(), &instance)
	err := removeStaleEntriesFromInstanceStatus(context.TODO(), r.client, getClientSetWithPvc(),
		r.cnsOperatorClient, &instance, map[string]string{}, map[string]string{"disk-1": "pvc-1"})

	require.NoError(t, err)
	require.Len(t, instance.Status.VolumeStatus, 1)
	assert.Equal(t, "pvc-1", instance.Status.VolumeStatus[0].PersistentVolumeClaim.ClaimName)
}

// monfVolumeManager reports that the VM no longer exists on vCenter.
type monfVolumeManager struct {
	unittestcommon.MockVolumeManager
	ref vim25types.ManagedObjectReference
}

func (m *monfVolumeManager) DetachVolume(ctx context.Context, vm *cnsvsphere.VirtualMachine,
	volumeID string) (string, error) {
	return "vim.fault.ManagedObjectNotFound", monfError(m.ref)
}

// monfError returns the SOAP fault vCenter reports when the VM does not exist.
func monfError(ref vim25types.ManagedObjectReference) error {
	fault := &soap.Fault{}
	fault.Detail.Fault = vim25types.ManagedObjectNotFound{Obj: ref}
	return soap.WrapSoapFault(fault)
}

func newVMWithRef(ref vim25types.ManagedObjectReference) *cnsvsphere.VirtualMachine {
	return &cnsvsphere.VirtualMachine{VirtualMachine: object.NewVirtualMachine(nil, ref)}
}

// testVM returns a VM whose managed object reference can be read, as detachVolumes does
// on every detach error.
func testVM() *cnsvsphere.VirtualMachine {
	return newVMWithRef(vim25types.ManagedObjectReference{Type: "VirtualMachine", Value: "vm-test"})
}
