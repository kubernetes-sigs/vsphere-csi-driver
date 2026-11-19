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
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	cnsregistervolumev1alpha1 "sigs.k8s.io/vsphere-csi-driver/v3/pkg/apis/cnsoperator/cnsregistervolume/v1alpha1"
	"sigs.k8s.io/vsphere-csi-driver/v3/pkg/common/fault"
	cnsoperatortypes "sigs.k8s.io/vsphere-csi-driver/v3/pkg/syncer/cnsoperator/types"
)

// newBoundButUnregisteredInstance builds the CnsRegisterVolume the VM-delete race
// leaves behind: the PVC bound, so PvcBound is True, but the post-bind
// keepAfterDeleteVm clear has not finished, so Registered is still false and the
// CNS volume is recorded in the execution checkpoint. Owner-reference garbage
// collection has already stamped it for deletion.
func newBoundButUnregisteredInstance(
	protectionReason string, deletedAgo time.Duration,
) *cnsregistervolumev1alpha1.CnsRegisterVolume {
	deletedAt := metav1.NewTime(time.Now().Add(-deletedAgo))
	instance := newCondTestInstance(cnsregistervolumev1alpha1.CnsRegisterVolumeStatus{
		Registered:  false,
		CnsVolumeID: "fcd-owned-by-vm",
		Conditions: []metav1.Condition{
			{
				Type:               cnsregistervolumev1alpha1.ConditionPvcBound,
				Status:             metav1.ConditionTrue,
				Reason:             cnsregistervolumev1alpha1.ReasonBound,
				Message:            "PVC is bound",
				LastTransitionTime: deletedAt,
			},
			{
				Type:               cnsregistervolumev1alpha1.ConditionVMDeleteProtectionConfigured,
				Status:             metav1.ConditionFalse,
				Reason:             protectionReason,
				Message:            "Waiting to retry VM-delete-protection configuration",
				LastTransitionTime: deletedAt,
			},
		},
	})
	instance.Spec = cnsregistervolumev1alpha1.CnsRegisterVolumeSpec{
		DiskURLPath: "https://vc/disk.vmdk",
		PvcName:     "batch-import-poweroff-assigned",
	}
	instance.DeletionTimestamp = &deletedAt
	instance.Finalizers = []string{cnsoperatortypes.CNSRegisterVolumeFinalizer}
	return instance
}

// TestReconcileDelete_BoundPvcIsNotUnregistered reproduces the stuck-namespace
// race behind bugs 1767699 and 1769149.
//
// Mobility-operator imports a VM, vm-operator creates a CnsRegisterVolume for its
// disk, and the test deletes the VM about a second after the PVC binds. Garbage
// collection removes the PVC and the CnsRegisterVolume while the keepAfterDeleteVm
// clear is still running, so the CR is deleted bound but not yet Registered.
//
// The delete must not try to unregister that disk: a live VM and a bound PVC own
// it, and CNS answering with a fault used to requeue roughly once a second
// forever, holding the finalizer and stranding the namespace in Terminating.
func TestReconcileDelete_BoundPvcIsNotUnregistered(t *testing.T) {
	// Both bugs reach the same state by different routes: the clear itself
	// failing, and the clear succeeding but its PVC annotation write failing
	// because garbage collection already removed the PVC.
	for _, reason := range []string{
		cnsregistervolumev1alpha1.ReasonClearKeepAfterDeleteVmFailed,
		cnsregistervolumev1alpha1.ReasonPersistClearResultFailed,
	} {
		t.Run(reason, func(t *testing.T) {
			ctx := context.Background()
			instance := newBoundButUnregisteredInstance(reason, time.Second)
			r, _, _ := newAsyncTestReconciler(t, instance, 40)

			unregisterCalls := 0
			r.volumeManager = &mockVolumeManager{
				unregisterVolume: func(_ context.Context, _ string, _ bool) (string, error) {
					unregisterCalls++
					return "vim.fault.CnsFault", errors.New("volume operation result contains fault")
				},
			}

			result, err := r.reconcileDelete(ctx, instance, reconcileRequestFor(instance), time.Second)

			require.NoError(t, err)
			assert.Zero(t, unregisterCalls,
				"a disk whose PVC bound belongs to the workload and must not be unregistered")
			assert.Zero(t, result.RequeueAfter, "the delete must complete rather than requeue")
			err = r.client.Get(ctx, instanceKey(instance), &cnsregistervolumev1alpha1.CnsRegisterVolume{})
			assert.True(t, apierrors.IsNotFound(err),
				"the finalizer must be released so the namespace can finish deleting, got: %v", err)
		})
	}
}

// TestIsPvcBound covers the trigger for the skip above. Only a True condition
// counts: a CnsRegisterVolume still working towards the bind, or one that never
// reached it, must still clean up the volume it registered.
func TestIsPvcBound(t *testing.T) {
	tests := []struct {
		name string
		cond *metav1.Condition
		want bool
	}{
		{name: "no condition yet", cond: nil, want: false},
		{
			name: "bound",
			cond: &metav1.Condition{Type: cnsregistervolumev1alpha1.ConditionPvcBound,
				Status: metav1.ConditionTrue, Reason: cnsregistervolumev1alpha1.ReasonBound},
			want: true,
		},
		{
			name: "still waiting for the bind",
			cond: &metav1.Condition{Type: cnsregistervolumev1alpha1.ConditionPvcBound,
				Status: metav1.ConditionFalse, Reason: cnsregistervolumev1alpha1.ReasonWaitingForBind},
			want: false,
		},
		{
			name: "creating the PV and PVC",
			cond: &metav1.Condition{Type: cnsregistervolumev1alpha1.ConditionPvcBound,
				Status: metav1.ConditionFalse, Reason: cnsregistervolumev1alpha1.ReasonConfiguringK8sResources},
			want: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			instance := newCondTestInstance(cnsregistervolumev1alpha1.CnsRegisterVolumeStatus{})
			if tt.cond != nil {
				instance.Status.Conditions = []metav1.Condition{*tt.cond}
			}
			assert.Equal(t, tt.want, isPvcBound(instance))
		})
	}
}

// TestReconcileDelete_ReleasesFinalizerAfterCleanupDeadline guards the general
// defect behind the same bugs: a CNS fault that never clears must not hold the
// finalizer indefinitely. An FCD left registered can be reclaimed afterwards; a
// namespace wedged in Terminating cannot be, short of editing the object by hand.
func TestReconcileDelete_ReleasesFinalizerAfterCleanupDeadline(t *testing.T) {
	ctx := context.Background()
	instance := newDeletingDiskURLPathInstance()
	// Deleted well before the deadline, with cleanup failing the whole time.
	deletedAt := metav1.NewTime(time.Now().Add(-cnsCleanupDeadline - time.Minute))
	instance.DeletionTimestamp = &deletedAt
	r, _, _ := newAsyncTestReconciler(t, instance, 40)

	unregisterCalls := 0
	r.volumeManager = &mockVolumeManager{
		unregisterVolume: func(_ context.Context, _ string, _ bool) (string, error) {
			unregisterCalls++
			return fault.VimFaultPrefix + "InvalidState", errors.New("persistent vCenter failure")
		},
	}

	result, err := r.reconcileDelete(ctx, instance, reconcileRequestFor(instance), time.Second)

	require.NoError(t, err)
	assert.Equal(t, 1, unregisterCalls, "the cleanup is still attempted once more before giving up")
	assert.Zero(t, result.RequeueAfter, "past the deadline the delete must stop retrying")
	err = r.client.Get(ctx, instanceKey(instance), &cnsregistervolumev1alpha1.CnsRegisterVolume{})
	assert.True(t, apierrors.IsNotFound(err),
		"the finalizer must be released once the deadline passes, got: %v", err)
}

// TestGiveUpOnCleanup pins the deadline boundary itself, independently of which
// caller hits it: the delete path in reconcileDelete and the one in
// resolveInFlightTaskForDelete share this decision.
func TestGiveUpOnCleanup(t *testing.T) {
	cause := errors.New("volume operation result contains fault")
	tests := []struct {
		name       string
		deletedAgo time.Duration
		want       bool
	}{
		{name: "just deleted", deletedAgo: time.Second, want: false},
		{name: "inside the deadline", deletedAgo: cnsCleanupDeadline - time.Minute, want: false},
		{name: "past the deadline", deletedAgo: cnsCleanupDeadline + time.Minute, want: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx := context.Background()
			instance := newBoundButUnregisteredInstance(
				cnsregistervolumev1alpha1.ReasonClearKeepAfterDeleteVmFailed, tt.deletedAgo)
			r, _, _ := newAsyncTestReconciler(t, instance, 40)

			assert.Equal(t, tt.want, r.giveUpOnCleanup(ctx, instance, "fcd-owned-by-vm", cause))
		})
	}
}

// TestGiveUpOnCleanupRecordsWhyItGaveUp: the volume may still be registered in
// CNS at that point, so the reason has to survive on the object rather than only
// in the logs. This is the signal the original bug lacked, where 572 and 856
// consecutive passes logged nothing but "Handling deletion".
func TestGiveUpOnCleanupRecordsWhyItGaveUp(t *testing.T) {
	ctx := context.Background()
	instance := newBoundButUnregisteredInstance(
		cnsregistervolumev1alpha1.ReasonClearKeepAfterDeleteVmFailed, cnsCleanupDeadline+time.Minute)
	r, _, _ := newAsyncTestReconciler(t, instance, 40)

	require.True(t, r.giveUpOnCleanup(ctx, instance, "fcd-owned-by-vm",
		errors.New("volume operation result contains fault")))

	got := getCondTestInstance(t, r.client, instance)
	assert.Contains(t, got.Status.Error, "fcd-owned-by-vm")
	assert.Contains(t, got.Status.Error, "may still be registered in CNS")
}

// TestGrowDeleteBackoff: a delete that keeps failing must back off instead of
// re-issuing a CNS call every second, which is what the original bug did for
// 572 and 856 consecutive passes.
func TestGrowDeleteBackoff(t *testing.T) {
	instance := newBoundButUnregisteredInstance(
		cnsregistervolumev1alpha1.ReasonClearKeepAfterDeleteVmFailed, time.Second)
	// newAsyncTestReconciler resets the package-global backoff map.
	newAsyncTestReconciler(t, instance, 40)
	request := reconcileRequestFor(instance)

	backOffDuration[request.NamespacedName] = time.Second
	growDeleteBackoff(request)
	assert.Equal(t, 2*time.Second, backOffDuration[request.NamespacedName])
	growDeleteBackoff(request)
	assert.Equal(t, 4*time.Second, backOffDuration[request.NamespacedName])

	backOffDuration[request.NamespacedName] = cnsoperatortypes.MaxBackOffDurationForReconciler
	growDeleteBackoff(request)
	assert.Equal(t, cnsoperatortypes.MaxBackOffDurationForReconciler,
		backOffDuration[request.NamespacedName], "the backoff must stay capped")
}
