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

// TestUnregisterFCD_NotFound verifies that a vim.fault.NotFound from CNS is
// treated as "already unregistered", while every other failure is still
// returned so the caller retries.
func TestUnregisterFCD_NotFound(t *testing.T) {
	ctx := context.Background()
	unregErr := errors.New("CNS UnregisterVolume failed")

	tests := []struct {
		name      string
		faultType string
		err       error
		wantErr   bool
	}{
		{name: "success", faultType: "", err: nil, wantErr: false},
		{name: "NotFound is ignored", faultType: fault.VimFaultNotFound, err: unregErr, wantErr: false},
		{name: "other vim fault is returned", faultType: fault.VimFaultPrefix + "InvalidState",
			err: unregErr, wantErr: true},
		{name: "non-vim fault is returned", faultType: fault.CSIInternalFault, err: unregErr, wantErr: true},
		{name: "empty fault type with error is returned", faultType: "", err: unregErr, wantErr: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r := &ReconcileCnsRegisterVolume{volumeManager: &mockVolumeManager{
				unregisterVolume: func(_ context.Context, volumeID string, unregDisk bool) (string, error) {
					assert.Equal(t, "vol-1", volumeID)
					assert.True(t, unregDisk)
					return tt.faultType, tt.err
				},
			}}

			err := r.unregisterFCD(ctx, "vol-1")

			if tt.wantErr {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
			}
		})
	}
}

// newDeletingDiskURLPathInstance returns a DiskURLPath CnsRegisterVolume that
// is being deleted before registration finished: Registered is false but the
// CNS volume was already recorded in the execution checkpoint.
func newDeletingDiskURLPathInstance() *cnsregistervolumev1alpha1.CnsRegisterVolume {
	now := metav1.Now()
	instance := newCondTestInstance(cnsregistervolumev1alpha1.CnsRegisterVolumeStatus{
		Registered:  false,
		CnsVolumeID: "fcd-already-deleted",
	})
	instance.Spec = cnsregistervolumev1alpha1.CnsRegisterVolumeSpec{
		DiskURLPath: "https://vc/disk.vmdk",
		PvcName:     "test-pvc",
	}
	instance.DeletionTimestamp = &now
	instance.Finalizers = []string{cnsoperatortypes.CNSRegisterVolumeFinalizer}
	return instance
}

// TestReconcileDelete_FCDAlreadyDeleted reproduces the stuck-namespace bug.
// DeleteVolume removed the FCD and its PV when the PVC was garbage collected,
// before the CnsRegisterVolume was finalized. CNS then answers UnregisterVolume
// with vim.fault.NotFound. The finalizer must still be removed.
func TestReconcileDelete_FCDAlreadyDeleted(t *testing.T) {
	ctx := context.Background()
	instance := newDeletingDiskURLPathInstance()
	r, _, _ := newAsyncTestReconciler(t, instance, 40)

	unregisterCalls := 0
	r.volumeManager = &mockVolumeManager{
		unregisterVolume: func(_ context.Context, volumeID string, unregDisk bool) (string, error) {
			unregisterCalls++
			assert.Equal(t, "fcd-already-deleted", volumeID)
			assert.True(t, unregDisk)
			return fault.VimFaultNotFound,
				errors.New("ServerFaultCode: The object or item referred to could not be found.")
		},
	}

	result, err := r.reconcileDelete(ctx, instance, reconcileRequestFor(instance), time.Second)

	require.NoError(t, err)
	assert.Zero(t, result.RequeueAfter, "a NotFound FCD must not requeue the delete")
	assert.Equal(t, 1, unregisterCalls, "the unregister must be attempted exactly once")
	// The finalizer was the last blocker, so the object is gone.
	err = r.client.Get(ctx, instanceKey(instance), &cnsregistervolumev1alpha1.CnsRegisterVolume{})
	assert.True(t, apierrors.IsNotFound(err), "the CnsRegisterVolume must be deleted, got: %v", err)
}

// TestReconcileDelete_UnregisterFailureKeepsFinalizer guards the other side: a
// failure that is not NotFound must still hold the finalizer and requeue.
func TestReconcileDelete_UnregisterFailureKeepsFinalizer(t *testing.T) {
	ctx := context.Background()
	instance := newDeletingDiskURLPathInstance()
	r, _, _ := newAsyncTestReconciler(t, instance, 40)

	r.volumeManager = &mockVolumeManager{
		unregisterVolume: func(_ context.Context, _ string, _ bool) (string, error) {
			return fault.VimFaultPrefix + "InvalidState", errors.New("transient vCenter failure")
		},
	}

	result, err := r.reconcileDelete(ctx, instance, reconcileRequestFor(instance), time.Second)

	require.NoError(t, err)
	assert.Equal(t, time.Second, result.RequeueAfter, "a real failure must requeue")
	got := &cnsregistervolumev1alpha1.CnsRegisterVolume{}
	require.NoError(t, r.client.Get(ctx, instanceKey(instance), got))
	assert.Contains(t, got.Finalizers, cnsoperatortypes.CNSRegisterVolumeFinalizer,
		"the finalizer must stay until the unregister succeeds")
}
