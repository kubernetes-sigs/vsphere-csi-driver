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

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	apitypes "k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/record"
	ctrlclient "sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"

	cnsregistervolumev1alpha1 "sigs.k8s.io/vsphere-csi-driver/v3/pkg/apis/cnsoperator/cnsregistervolume/v1alpha1"
)

const (
	ownerTestPVCName = "test-vm-4efc1a1e"
	ownerTestPVCUID  = apitypes.UID("461e1b93-5bd8-4b46-8137-82b1d2a36312")
)

// vmOwnerRef is the reference VM Operator puts on the CnsRegisterVolume it
// creates for each disk of a VirtualMachine.
func vmOwnerRef() metav1.OwnerReference {
	controller, block := true, true
	return metav1.OwnerReference{
		APIVersion:         "vmoperator.vmware.com/v1alpha6",
		Kind:               "VirtualMachine",
		Name:               "test-vm",
		UID:                "a5d69056-7a9a-4254-9a9a-81137e76d682",
		Controller:         &controller,
		BlockOwnerDeletion: &block,
	}
}

func ownerTestInstance(owners ...metav1.OwnerReference) *cnsregistervolumev1alpha1.CnsRegisterVolume {
	cr := newCondTestInstance(cnsregistervolumev1alpha1.CnsRegisterVolumeStatus{})
	cr.OwnerReferences = owners
	return cr
}

func controllerCount(refs []metav1.OwnerReference) int {
	n := 0
	for _, ref := range refs {
		if ref.Controller != nil && *ref.Controller {
			n++
		}
	}
	return n
}

// TestSetInstanceOwnerRefPreservesVMController is the reported bug: a CR that
// VM Operator created with the VirtualMachine as controller owner must keep it
// after registration, rather than having it replaced by the PVC.
func TestSetInstanceOwnerRefPreservesVMController(t *testing.T) {
	cr := ownerTestInstance(vmOwnerRef())

	changed := setInstanceOwnerRef(cr, ownerTestPVCName, ownerTestPVCUID)

	assert.False(t, changed, "a CR another controller owns must not be modified")
	require.Len(t, cr.OwnerReferences, 1)
	assert.Equal(t, vmOwnerRef(), cr.OwnerReferences[0], "the VirtualMachine owner must be kept as it was")
}

// TestSetInstanceOwnerRefDoesNotAddPVCAlongsideController guards the
// alternative fix that was rejected: adding the PVC as a second owner would
// stop the CR being garbage collected with the VM, because collection waits
// for every owner to be gone.
func TestSetInstanceOwnerRefDoesNotAddPVCAlongsideController(t *testing.T) {
	cr := ownerTestInstance(vmOwnerRef())

	setInstanceOwnerRef(cr, ownerTestPVCName, ownerTestPVCUID)

	for _, ref := range cr.OwnerReferences {
		assert.NotEqual(t, ownerTestPVCUID, ref.UID, "the PVC must not be added next to an existing controller")
	}
}

// TestSetInstanceOwnerRefAddsPVCWhenUnowned keeps the existing behaviour for a
// CR that nothing owns: the PVC becomes its controller owner.
func TestSetInstanceOwnerRefAddsPVCWhenUnowned(t *testing.T) {
	cr := ownerTestInstance()

	changed := setInstanceOwnerRef(cr, ownerTestPVCName, ownerTestPVCUID)

	assert.True(t, changed)
	require.Len(t, cr.OwnerReferences, 1)
	ref := cr.OwnerReferences[0]
	assert.Equal(t, "v1", ref.APIVersion)
	assert.Equal(t, "PersistentVolumeClaim", ref.Kind)
	assert.Equal(t, ownerTestPVCName, ref.Name)
	assert.Equal(t, ownerTestPVCUID, ref.UID)
	require.NotNil(t, ref.Controller)
	assert.True(t, *ref.Controller)
	require.NotNil(t, ref.BlockOwnerDeletion)
	assert.True(t, *ref.BlockOwnerDeletion)
}

// TestSetInstanceOwnerRefKeepsNonControllerOwners verifies an owner that is not
// a controller is no longer wiped when the PVC is added.
func TestSetInstanceOwnerRefKeepsNonControllerOwners(t *testing.T) {
	notController := false
	other := metav1.OwnerReference{
		APIVersion: "example.com/v1", Kind: "Backup", Name: "nightly",
		UID: "11111111-2222-3333-4444-555555555555", Controller: &notController,
	}
	cr := ownerTestInstance(other)

	changed := setInstanceOwnerRef(cr, ownerTestPVCName, ownerTestPVCUID)

	assert.True(t, changed)
	require.Len(t, cr.OwnerReferences, 2)
	assert.Equal(t, other, cr.OwnerReferences[0], "the existing owner must be kept")
	assert.Equal(t, ownerTestPVCUID, cr.OwnerReferences[1].UID)
}

// TestSetInstanceOwnerRefIsIdempotent verifies a repeated call, e.g. a retry
// after the status write failed, neither duplicates the PVC nor reports a
// change.
func TestSetInstanceOwnerRefIsIdempotent(t *testing.T) {
	cr := ownerTestInstance()

	require.True(t, setInstanceOwnerRef(cr, ownerTestPVCName, ownerTestPVCUID))
	assert.False(t, setInstanceOwnerRef(cr, ownerTestPVCName, ownerTestPVCUID))
	assert.Len(t, cr.OwnerReferences, 1)
}

// TestSetInstanceOwnerRefNeverTwoControllers checks the API server rule that at
// most one owner reference may be a controller, across every starting state.
func TestSetInstanceOwnerRefNeverTwoControllers(t *testing.T) {
	notController := false
	for name, owners := range map[string][]metav1.OwnerReference{
		"none":           nil,
		"vm controller":  {vmOwnerRef()},
		"non-controller": {{APIVersion: "example.com/v1", Kind: "Backup", Name: "b", UID: "u", Controller: &notController}},
	} {
		t.Run(name, func(t *testing.T) {
			cr := ownerTestInstance(owners...)
			setInstanceOwnerRef(cr, ownerTestPVCName, ownerTestPVCUID)
			assert.LessOrEqual(t, controllerCount(cr.OwnerReferences), 1)
		})
	}
}

// newOwnerTestReconciler returns a reconciler whose client counts metadata
// updates, so a test can assert that none is issued.
func newOwnerTestReconciler(t *testing.T, cr *cnsregistervolumev1alpha1.CnsRegisterVolume) (
	*ReconcileCnsRegisterVolume, *int) {
	t.Helper()
	backOffDuration = make(map[apitypes.NamespacedName]time.Duration)
	updates := 0
	c := fake.NewClientBuilder().WithScheme(condTestScheme()).
		WithObjects(cr).WithStatusSubresource(cr).
		WithInterceptorFuncs(interceptor.Funcs{
			Update: func(ctx context.Context, client ctrlclient.WithWatch, obj ctrlclient.Object,
				opts ...ctrlclient.UpdateOption) error {
				updates++
				return client.Update(ctx, obj, opts...)
			},
		}).Build()
	r := &ReconcileCnsRegisterVolume{
		client:     c,
		scheme:     condTestScheme(),
		configInfo: testConfigInfo(),
		recorder:   record.NewFakeRecorder(8),
	}
	require.NoError(t, c.Get(context.Background(), instanceKey(cr), cr))
	return r, &updates
}

// TestSetInstanceSuccessKeepsVMOwner exercises the path from the bug report end
// to end: registration finishes, the CR is marked registered, and the stored
// object still belongs to the VirtualMachine.
func TestSetInstanceSuccessKeepsVMOwner(t *testing.T) {
	ctx := context.Background()
	cr := ownerTestInstance(vmOwnerRef())
	r, updates := newOwnerTestReconciler(t, cr)

	require.NoError(t, setInstanceSuccess(ctx, r, cr, ownerTestPVCName, ownerTestPVCUID, "registered"))

	stored := getCondTestInstance(t, r.client, cr)
	assert.True(t, stored.Status.Registered, "registration must still complete")
	require.Len(t, stored.OwnerReferences, 1)
	assert.Equal(t, "VirtualMachine", stored.OwnerReferences[0].Kind)
	assert.Equal(t, vmOwnerRef().UID, stored.OwnerReferences[0].UID)
	assert.Equal(t, 0, *updates, "there is nothing to write, so no metadata update may be issued")
}

// TestSetInstanceSuccessStillOwnsUnownedCR verifies the unchanged half: a CR
// with no owner still ends up owned by its PVC.
func TestSetInstanceSuccessStillOwnsUnownedCR(t *testing.T) {
	ctx := context.Background()
	cr := ownerTestInstance()
	r, updates := newOwnerTestReconciler(t, cr)

	require.NoError(t, setInstanceSuccess(ctx, r, cr, ownerTestPVCName, ownerTestPVCUID, "registered"))

	stored := getCondTestInstance(t, r.client, cr)
	assert.True(t, stored.Status.Registered)
	require.Len(t, stored.OwnerReferences, 1)
	assert.Equal(t, ownerTestPVCUID, stored.OwnerReferences[0].UID)
	assert.Equal(t, 1, *updates)
}
