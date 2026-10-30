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
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/agiledragon/gomonkey/v2"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	"k8s.io/client-go/tools/record"
	ctrlclient "sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
	cnsregistervolumev1alpha1 "sigs.k8s.io/vsphere-csi-driver/v3/pkg/apis/cnsoperator/cnsregistervolume/v1alpha1"
	cnsvsphere "sigs.k8s.io/vsphere-csi-driver/v3/pkg/common/cns-lib/vsphere"
	"sigs.k8s.io/vsphere-csi-driver/v3/pkg/common/conditions"
	"sigs.k8s.io/vsphere-csi-driver/v3/pkg/csi/service/common/commonco"
)

// condTestScheme returns a scheme that knows the CnsRegisterVolume types.
func condTestScheme() *runtime.Scheme {
	scheme := runtime.NewScheme()
	_ = clientgoscheme.AddToScheme(scheme)
	scheme.AddKnownTypes(schema.GroupVersion{Group: "cnsoperator.vmware.com", Version: "v1alpha1"},
		&cnsregistervolumev1alpha1.CnsRegisterVolume{}, &cnsregistervolumev1alpha1.CnsRegisterVolumeList{})
	return scheme
}

// newCondTestInstance returns a CnsRegisterVolume with the given starting status.
func newCondTestInstance(
	status cnsregistervolumev1alpha1.CnsRegisterVolumeStatus,
) *cnsregistervolumev1alpha1.CnsRegisterVolume {
	return &cnsregistervolumev1alpha1.CnsRegisterVolume{
		ObjectMeta: metav1.ObjectMeta{Name: "test-volume", Namespace: "test-ns", UID: "test-uid"},
		Spec:       cnsregistervolumev1alpha1.CnsRegisterVolumeSpec{VolumeID: "test-volume-id"},
		Status:     status,
	}
}

// getCondTestInstance re-reads the instance from the client.
func getCondTestInstance(t *testing.T, c ctrlclient.Client,
	instance *cnsregistervolumev1alpha1.CnsRegisterVolume) *cnsregistervolumev1alpha1.CnsRegisterVolume {
	t.Helper()
	got := &cnsregistervolumev1alpha1.CnsRegisterVolume{}
	err := c.Get(context.Background(),
		types.NamespacedName{Name: instance.Name, Namespace: instance.Namespace}, got)
	assert.NoError(t, err)
	return got
}

// condOf returns the condition of the given type, or nil.
func condOf(instance *cnsregistervolumev1alpha1.CnsRegisterVolume,
	conditionType string) *metav1.Condition {
	return conditions.Get(instance, conditionType)
}

// capturedPatch records the status patches a test issues, so assertions can be made
// about which keys were sent rather than only about the resulting object. The
// distinction matters: a merge patch that omits a key leaves the stored value alone,
// which is how "no condition changed" is expressed on the wire.
type capturedPatch struct {
	patches []map[string]interface{}
}

func (c *capturedPatch) count() int { return len(c.patches) }

func (c *capturedPatch) last() map[string]interface{} {
	if len(c.patches) == 0 {
		return nil
	}
	return c.patches[len(c.patches)-1]
}

// newCapturingClient returns a client that records the status subresource patches
// it is given before applying them. failWith, when non-nil, is returned instead of
// applying the patch.
func newCapturingClient(instance *cnsregistervolumev1alpha1.CnsRegisterVolume,
	failWith error) (ctrlclient.Client, *capturedPatch) {
	rec := &capturedPatch{}
	c := fake.NewClientBuilder().
		WithScheme(condTestScheme()).
		WithObjects(instance).
		WithStatusSubresource(instance).
		WithInterceptorFuncs(interceptor.Funcs{
			SubResourcePatch: func(ctx context.Context, client ctrlclient.Client, subResourceName string,
				obj ctrlclient.Object, patch ctrlclient.Patch, opts ...ctrlclient.SubResourcePatchOption) error {
				data, err := patch.Data(obj)
				if err != nil {
					return err
				}
				var decoded map[string]interface{}
				if err := json.Unmarshal(data, &decoded); err != nil {
					return err
				}
				if status, ok := decoded["status"].(map[string]interface{}); ok {
					rec.patches = append(rec.patches, status)
				}
				if failWith != nil {
					return failWith
				}
				return client.Status().Patch(ctx, obj, patch, opts...)
			},
		}).
		Build()
	return c, rec
}

// TestPatchStatusCarriesConditionsWhenChanged verifies that a condition change is
// sent as a whole array, because a JSON merge patch replaces arrays wholesale.
func TestPatchStatusCarriesConditionsWhenChanged(t *testing.T) {
	ctx := context.Background()
	instance := newCondTestInstance(cnsregistervolumev1alpha1.CnsRegisterVolumeStatus{})
	c, rec := newCapturingClient(instance, nil)

	newInstance := instance.DeepCopy()
	applyConditions(newInstance, trueCondition(
		cnsregistervolumev1alpha1.ConditionSpecValidated,
		cnsregistervolumev1alpha1.ReasonValidated, "ok"))

	assert.NoError(t, patchCnsRegisterVolumeStatus(ctx, c, instance, newInstance))
	assert.Equal(t, 1, rec.count())
	conds, ok := rec.last()["conditions"].([]interface{})
	assert.True(t, ok, "the conditions key must be present when a condition changed")
	assert.Len(t, conds, 1)

	got := getCondTestInstance(t, c, instance)
	assert.Equal(t, metav1.ConditionTrue,
		condOf(got, cnsregistervolumev1alpha1.ConditionSpecValidated).Status)
}

// TestPatchStatusOmitsConditionsWhenUnchanged verifies that a write which is only
// about registered or error does not carry the conditions array at all. Omitting
// the key is what leaves stored conditions untouched.
func TestPatchStatusOmitsConditionsWhenUnchanged(t *testing.T) {
	ctx := context.Background()
	instance := newCondTestInstance(cnsregistervolumev1alpha1.CnsRegisterVolumeStatus{})
	applyConditions(instance, falseCondition(
		cnsregistervolumev1alpha1.ConditionPvcBound,
		cnsregistervolumev1alpha1.ReasonWaitingForBind, "waiting"))
	c, rec := newCapturingClient(instance, nil)

	newInstance := instance.DeepCopy()
	newInstance.Status.Error = "something failed"

	assert.NoError(t, patchCnsRegisterVolumeStatus(ctx, c, instance, newInstance))
	assert.Equal(t, 1, rec.count())
	_, present := rec.last()["conditions"]
	assert.False(t, present,
		"an error-only write must not carry conditions, or it would replace the stored array")
	_, registeredPresent := rec.last()["registered"]
	assert.True(t, registeredPresent, "registered is required by the CRD and must always be sent")
}

// TestPatchStatusPreservesStoredConditionsOnErrorOnlyWrite is the stored-object
// counterpart: a write about error must not wipe conditions published earlier.
func TestPatchStatusPreservesStoredConditionsOnErrorOnlyWrite(t *testing.T) {
	ctx := context.Background()
	instance := newCondTestInstance(cnsregistervolumev1alpha1.CnsRegisterVolumeStatus{})
	applyConditions(instance, falseCondition(
		cnsregistervolumev1alpha1.ConditionPvcBound,
		cnsregistervolumev1alpha1.ReasonWaitingForBind, "waiting"))
	c, _ := newCapturingClient(instance, nil)

	newInstance := instance.DeepCopy()
	newInstance.Status.Error = "transient failure"
	assert.NoError(t, patchCnsRegisterVolumeStatus(ctx, c, instance, newInstance))

	got := getCondTestInstance(t, c, instance)
	assert.NotNil(t, condOf(got, cnsregistervolumev1alpha1.ConditionPvcBound),
		"a status write that is only about error must leave conditions alone")
	assert.Equal(t, "transient failure", got.Status.Error)
}

// TestSetInstanceConditionsPublishes verifies the happy path of the helper.
func TestSetInstanceConditionsPublishes(t *testing.T) {
	ctx := context.Background()
	instance := newCondTestInstance(cnsregistervolumev1alpha1.CnsRegisterVolumeStatus{})
	c, rec := newCapturingClient(instance, nil)
	r := &ReconcileCnsRegisterVolume{client: c, recorder: record.NewFakeRecorder(10)}

	setInstanceConditions(ctx, r, instance, falseCondition(
		cnsregistervolumev1alpha1.ConditionCnsVolumeRegistered,
		cnsregistervolumev1alpha1.ReasonCnsTaskRunning, "running"))

	assert.Equal(t, 1, rec.count())
	got := getCondTestInstance(t, c, instance)
	cond := condOf(got, cnsregistervolumev1alpha1.ConditionCnsVolumeRegistered)
	assert.NotNil(t, cond)
	assert.Equal(t, cnsregistervolumev1alpha1.ReasonCnsTaskRunning, cond.Reason)
}

// TestSetInstanceConditionsIsNoOpWhenUnchanged verifies that re-stating the same
// condition issues no API write at all. This is what keeps a repeated reconcile
// from churning the status and from moving lastTransitionTime.
func TestSetInstanceConditionsIsNoOpWhenUnchanged(t *testing.T) {
	ctx := context.Background()
	instance := newCondTestInstance(cnsregistervolumev1alpha1.CnsRegisterVolumeStatus{})
	c, rec := newCapturingClient(instance, nil)
	r := &ReconcileCnsRegisterVolume{client: c, recorder: record.NewFakeRecorder(10)}

	cond := func() *metav1.Condition {
		return falseCondition(cnsregistervolumev1alpha1.ConditionPvcBound,
			cnsregistervolumev1alpha1.ReasonWaitingForBind, "waiting")
	}
	setInstanceConditions(ctx, r, instance, cond())
	assert.Equal(t, 1, rec.count())
	setInstanceConditions(ctx, r, instance, cond())
	assert.Equal(t, 1, rec.count(), "re-stating an identical condition must not write")
}

// TestConditionsPreserveLastTransitionTimeWhenRestated verifies that the age of a
// condition measures time in that state, not time since the last reconcile.
func TestConditionsPreserveLastTransitionTimeWhenRestated(t *testing.T) {
	ctx := context.Background()
	instance := newCondTestInstance(cnsregistervolumev1alpha1.CnsRegisterVolumeStatus{})
	c, _ := newCapturingClient(instance, nil)
	r := &ReconcileCnsRegisterVolume{client: c, recorder: record.NewFakeRecorder(10)}

	setInstanceConditions(ctx, r, instance, falseCondition(
		cnsregistervolumev1alpha1.ConditionCnsVolumeRegistered,
		cnsregistervolumev1alpha1.ReasonCnsTaskRunning, "running"))
	first := condOf(instance, cnsregistervolumev1alpha1.ConditionCnsVolumeRegistered).LastTransitionTime

	setInstanceConditions(ctx, r, instance, falseCondition(
		cnsregistervolumev1alpha1.ConditionCnsVolumeRegistered,
		cnsregistervolumev1alpha1.ReasonCnsTaskRunning, "running"))
	again := condOf(instance, cnsregistervolumev1alpha1.ConditionCnsVolumeRegistered).LastTransitionTime
	assert.Equal(t, first, again, "an unchanged condition must not move lastTransitionTime")
}

// TestConditionsMoveLastTransitionTimeOnReasonChange verifies the other half: a
// change of reason is a real change of state and does move the timestamp. This is
// why condition messages must be stable for a given state.
func TestConditionsMoveLastTransitionTimeOnReasonChange(t *testing.T) {
	instance := newCondTestInstance(cnsregistervolumev1alpha1.CnsRegisterVolumeStatus{})
	applyConditions(instance, falseCondition(
		cnsregistervolumev1alpha1.ConditionCnsVolumeRegistered,
		cnsregistervolumev1alpha1.ReasonWaitingForTaskSlot, "queued"))
	first := condOf(instance, cnsregistervolumev1alpha1.ConditionCnsVolumeRegistered).LastTransitionTime

	// conditions.Set truncates to the second, so move the stored stamp back to
	// make a change observable without sleeping.
	instance.Status.Conditions[0].LastTransitionTime = metav1.NewTime(first.Add(-time.Minute))
	before := instance.Status.Conditions[0].LastTransitionTime

	applyConditions(instance, falseCondition(
		cnsregistervolumev1alpha1.ConditionCnsVolumeRegistered,
		cnsregistervolumev1alpha1.ReasonCnsTaskRunning, "running"))
	after := condOf(instance, cnsregistervolumev1alpha1.ConditionCnsVolumeRegistered).LastTransitionTime
	assert.True(t, after.After(before.Time), "a reason change must move lastTransitionTime")
}

// TestSetInstanceConditionsLeavesRegisteredAndErrorAlone verifies that publishing
// progress is observational and cannot disturb the completion signal.
func TestSetInstanceConditionsLeavesRegisteredAndErrorAlone(t *testing.T) {
	ctx := context.Background()
	instance := newCondTestInstance(cnsregistervolumev1alpha1.CnsRegisterVolumeStatus{
		Registered: true,
		Error:      "previous error",
	})
	c, _ := newCapturingClient(instance, nil)
	r := &ReconcileCnsRegisterVolume{client: c, recorder: record.NewFakeRecorder(10)}

	setInstanceConditions(ctx, r, instance, falseCondition(
		cnsregistervolumev1alpha1.ConditionPvcBound,
		cnsregistervolumev1alpha1.ReasonWaitingForBind, "waiting"))

	got := getCondTestInstance(t, c, instance)
	assert.True(t, got.Status.Registered)
	assert.Equal(t, "previous error", got.Status.Error)
}

// TestSetInstanceConditionsRollsBackOnPatchFailure verifies that a failed publish
// leaves the in-memory conditions matching the server, so the next transition
// tries again rather than silently believing it already published.
func TestSetInstanceConditionsRollsBackOnPatchFailure(t *testing.T) {
	ctx := context.Background()
	instance := newCondTestInstance(cnsregistervolumev1alpha1.CnsRegisterVolumeStatus{})
	c, _ := newCapturingClient(instance, errors.New("patch rejected"))
	r := &ReconcileCnsRegisterVolume{client: c, recorder: record.NewFakeRecorder(10)}

	setInstanceConditions(ctx, r, instance, falseCondition(
		cnsregistervolumev1alpha1.ConditionPvcBound,
		cnsregistervolumev1alpha1.ReasonWaitingForBind, "waiting"))

	assert.Empty(t, instance.Status.Conditions,
		"a failed publish must not leave the in-memory conditions ahead of the server")
}

// TestTrueConditionCarriesMeaningfulReason guards the reason conditions.MarkTrue
// would have produced. MarkTrue hard-codes reason to the literal "True", which
// carries no information, so True conditions are built explicitly instead.
func TestTrueConditionCarriesMeaningfulReason(t *testing.T) {
	c := trueCondition(cnsregistervolumev1alpha1.ConditionPvcBound,
		cnsregistervolumev1alpha1.ReasonBound, "PVC is bound")
	assert.Equal(t, metav1.ConditionTrue, c.Status)
	assert.Equal(t, cnsregistervolumev1alpha1.ReasonBound, c.Reason)
	assert.NotEqual(t, string(metav1.ConditionTrue), c.Reason,
		"a True condition must say why, not just repeat its status")
}

// TestConditionsSortReadyFirst documents the stored order, which is why consumers
// must look conditions up by type rather than by index.
func TestConditionsSortReadyFirst(t *testing.T) {
	instance := newCondTestInstance(cnsregistervolumev1alpha1.CnsRegisterVolumeStatus{})
	applyConditions(instance,
		trueCondition(cnsregistervolumev1alpha1.ConditionSpecValidated,
			cnsregistervolumev1alpha1.ReasonValidated, ""),
		trueCondition(cnsregistervolumev1alpha1.ConditionCnsVolumeRegistered,
			cnsregistervolumev1alpha1.ReasonRegistered, ""),
		falseCondition(cnsregistervolumev1alpha1.ConditionReady,
			cnsregistervolumev1alpha1.ReasonFinalizing, ""),
	)
	assert.Equal(t, cnsregistervolumev1alpha1.ConditionReady, instance.Status.Conditions[0].Type,
		"Ready sorts first, so the array order is not the progression order")
}

// TestConditionsCarryObservedGeneration verifies a stale condition is identifiable.
func TestConditionsCarryObservedGeneration(t *testing.T) {
	instance := newCondTestInstance(cnsregistervolumev1alpha1.CnsRegisterVolumeStatus{})
	instance.Generation = 7
	applyConditions(instance, trueCondition(
		cnsregistervolumev1alpha1.ConditionSpecValidated,
		cnsregistervolumev1alpha1.ReasonValidated, ""))
	assert.EqualValues(t, 7,
		condOf(instance, cnsregistervolumev1alpha1.ConditionSpecValidated).ObservedGeneration)
}

// TestStatusDeepCopyIsIndependent verifies the generated deepcopy covers the
// conditions slice, which a shallow copy would alias.
func TestStatusDeepCopyIsIndependent(t *testing.T) {
	original := cnsregistervolumev1alpha1.CnsRegisterVolumeStatus{
		Registered: true,
		CnsTaskID:  "task-1",
		Conditions: []metav1.Condition{{
			Type:   cnsregistervolumev1alpha1.ConditionReady,
			Status: metav1.ConditionTrue,
			Reason: cnsregistervolumev1alpha1.ReasonRegistered,
		}},
	}
	copied := original.DeepCopy()
	copied.Conditions[0].Reason = cnsregistervolumev1alpha1.ReasonFinalizing
	copied.CnsTaskID = "task-2"

	assert.Equal(t, cnsregistervolumev1alpha1.ReasonRegistered, original.Conditions[0].Reason,
		"mutating the copy must not affect the original's conditions")
	assert.Equal(t, "task-1", original.CnsTaskID)
}

// TestCnsRegisterVolumeDeepCopyIsIndependent covers the object-level deepcopy.
func TestCnsRegisterVolumeDeepCopyIsIndependent(t *testing.T) {
	original := newCondTestInstance(cnsregistervolumev1alpha1.CnsRegisterVolumeStatus{
		Conditions: []metav1.Condition{{
			Type:   cnsregistervolumev1alpha1.ConditionPvcBound,
			Status: metav1.ConditionFalse,
			Reason: cnsregistervolumev1alpha1.ReasonWaitingForBind,
		}},
	})
	copied := original.DeepCopy()
	copied.Status.Conditions[0].Status = metav1.ConditionTrue

	assert.Equal(t, metav1.ConditionFalse, original.Status.Conditions[0].Status)
}

// TestStatusJSONOmitsNewFieldsWhenUnset verifies the wire form stays byte-identical
// to the pre-change one for a CR that has never had conditions or a task ID, which
// is what keeps older consumers unaffected.
func TestStatusJSONOmitsNewFieldsWhenUnset(t *testing.T) {
	data, err := json.Marshal(cnsregistervolumev1alpha1.CnsRegisterVolumeStatus{Registered: false})
	assert.NoError(t, err)
	assert.JSONEq(t, `{"registered":false}`, string(data),
		"an unset conditions list and task ID must not appear on the wire")
}

// TestStatusJSONRoundTrip verifies the new fields survive serialization.
func TestStatusJSONRoundTrip(t *testing.T) {
	original := cnsregistervolumev1alpha1.CnsRegisterVolumeStatus{
		Registered: true,
		CnsTaskID:  "task-42",
		Conditions: []metav1.Condition{{
			Type:               cnsregistervolumev1alpha1.ConditionReady,
			Status:             metav1.ConditionTrue,
			Reason:             cnsregistervolumev1alpha1.ReasonRegistered,
			Message:            "Volume is registered",
			LastTransitionTime: metav1.NewTime(time.Now().UTC().Truncate(time.Second)),
		}},
	}
	data, err := json.Marshal(original)
	assert.NoError(t, err)

	var decoded cnsregistervolumev1alpha1.CnsRegisterVolumeStatus
	assert.NoError(t, json.Unmarshal(data, &decoded))
	assert.Equal(t, original.CnsTaskID, decoded.CnsTaskID)
	assert.Equal(t, original.Conditions[0].Reason, decoded.Conditions[0].Reason)
	// Compare instants, not struct fields: unmarshalling yields a time in the
	// local zone, so the structs differ even when they name the same moment.
	assert.True(t, original.Conditions[0].LastTransitionTime.Time.Equal(
		decoded.Conditions[0].LastTransitionTime.Time),
		"lastTransitionTime must survive the round trip")
}

// TestConditionAndReasonConstants pins the published strings. These are API: a
// consumer keys off them, so a rename is a breaking change and should fail here.
func TestConditionAndReasonConstants(t *testing.T) {
	assert.Equal(t, "Ready", cnsregistervolumev1alpha1.ConditionReady)
	assert.Equal(t, "SpecValidated", cnsregistervolumev1alpha1.ConditionSpecValidated)
	assert.Equal(t, "CnsVolumeRegistered", cnsregistervolumev1alpha1.ConditionCnsVolumeRegistered)
	assert.Equal(t, "VolumeAccessible", cnsregistervolumev1alpha1.ConditionVolumeAccessible)
	assert.Equal(t, "PvcBound", cnsregistervolumev1alpha1.ConditionPvcBound)
	assert.Equal(t, "VMDeleteProtectionConfigured", cnsregistervolumev1alpha1.ConditionVMDeleteProtectionConfigured)

	assert.Equal(t, "WaitingForTaskSlot", cnsregistervolumev1alpha1.ReasonWaitingForTaskSlot)
	assert.Equal(t, "CnsTaskRunning", cnsregistervolumev1alpha1.ReasonCnsTaskRunning)
	assert.Equal(t, "CnsTaskFailed", cnsregistervolumev1alpha1.ReasonCnsTaskFailed)
	assert.Equal(t, "CnsTaskStateUnknown", cnsregistervolumev1alpha1.ReasonCnsTaskStateUnknown)
	assert.Equal(t, "WaitingForBind", cnsregistervolumev1alpha1.ReasonWaitingForBind)
	assert.Equal(t, "DatastoreNotAccessibleToCluster",
		cnsregistervolumev1alpha1.ReasonDatastoreNotAccessibleToCluster)

	// The CRD constrains reason to CamelCase with no spaces or hyphens.
	for _, reason := range []string{
		cnsregistervolumev1alpha1.ReasonValidated,
		cnsregistervolumev1alpha1.ReasonInvalidSpec,
		cnsregistervolumev1alpha1.ReasonStoragePolicyNotAssignedToNamespace,
		cnsregistervolumev1alpha1.ReasonStoragePolicyLookupFailed,
		cnsregistervolumev1alpha1.ReasonRegistered,
		cnsregistervolumev1alpha1.ReasonWaitingForTaskSlot,
		cnsregistervolumev1alpha1.ReasonCnsTaskRunning,
		cnsregistervolumev1alpha1.ReasonCnsTaskFailed,
		cnsregistervolumev1alpha1.ReasonCnsTaskStateUnknown,
		cnsregistervolumev1alpha1.ReasonVolumeCleanedUp,
		cnsregistervolumev1alpha1.ReasonAccessible,
		cnsregistervolumev1alpha1.ReasonDatastoreNotFound,
		cnsregistervolumev1alpha1.ReasonHostLocalPolicyIncompatible,
		cnsregistervolumev1alpha1.ReasonTopologyUnresolved,
		cnsregistervolumev1alpha1.ReasonDatastoreNotAccessibleToAZClusters,
		cnsregistervolumev1alpha1.ReasonDatastoreNotAccessibleToCluster,
		cnsregistervolumev1alpha1.ReasonStoragePolicyMissing,
		cnsregistervolumev1alpha1.ReasonHostLocalCheckFailed,
		cnsregistervolumev1alpha1.ReasonCleanupPending,
		cnsregistervolumev1alpha1.ReasonBound,
		cnsregistervolumev1alpha1.ReasonConfiguringK8sResources,
		cnsregistervolumev1alpha1.ReasonWaitingForBind,
		cnsregistervolumev1alpha1.ReasonFinalizing,
		cnsregistervolumev1alpha1.ReasonClearingKeepAfterDeleteVm,
		cnsregistervolumev1alpha1.ReasonClearKeepAfterDeleteVmFailed,
		cnsregistervolumev1alpha1.ReasonClearKeepAfterDeleteVmTimedOut,
		cnsregistervolumev1alpha1.ReasonPersistClearResultFailed,
		cnsregistervolumev1alpha1.ReasonKeepAfterDeleteVmCleared,
		cnsregistervolumev1alpha1.ReasonNotRequired,
	} {
		assert.Regexp(t, `^[A-Za-z]([A-Za-z0-9_,:]*[A-Za-z0-9_])?$`, reason,
			"reason must satisfy the CRD pattern")
	}
}

// TestReconcilePublishesSpecValidatedOnValidationFailure verifies that a CR
// rejected during spec validation says which step failed and why, rather than only
// carrying a free-text error.
func TestReconcilePublishesSpecValidatedOnValidationFailure(t *testing.T) {
	backOffDuration = make(map[types.NamespacedName]time.Duration)
	ctx := context.Background()

	// VolumeID without AccessMode fails validateCnsRegisterVolumeSpec, before
	// vCenter is contacted.
	crv := &cnsregistervolumev1alpha1.CnsRegisterVolume{
		ObjectMeta: metav1.ObjectMeta{Name: "test-volume", Namespace: "test-ns"},
		Spec: cnsregistervolumev1alpha1.CnsRegisterVolumeSpec{
			PvcName:  "test-pvc",
			VolumeID: "dummy-volume-id",
		},
	}
	scheme := condTestScheme()
	fakeClient := fake.NewClientBuilder().WithScheme(scheme).
		WithObjects(crv).WithStatusSubresource(crv).Build()

	commonco.ContainerOrchestratorUtility = &mockCOCommon{}
	r := &ReconcileCnsRegisterVolume{
		client:   fakeClient,
		scheme:   scheme,
		recorder: record.NewFakeRecorder(10),
	}

	_, err := r.Reconcile(ctx, reconcile.Request{
		NamespacedName: types.NamespacedName{Name: "test-volume", Namespace: "test-ns"}})
	assert.NoError(t, err)

	got := getCondTestInstance(t, fakeClient, crv)
	cond := condOf(got, cnsregistervolumev1alpha1.ConditionSpecValidated)
	assert.NotNil(t, cond, "spec validation failure must be reported as a condition")
	assert.Equal(t, metav1.ConditionFalse, cond.Status)
	assert.Equal(t, cnsregistervolumev1alpha1.ReasonInvalidSpec, cond.Reason)
	assert.False(t, got.Status.Registered, "registered must be untouched")
	assert.Contains(t, got.Status.Error, "AccessMode",
		"the existing error reporting must be unaffected")

	// Nothing downstream has been evaluated, so those conditions must be absent
	// rather than False. Absent means "not yet evaluated".
	assert.Nil(t, condOf(got, cnsregistervolumev1alpha1.ConditionCnsVolumeRegistered))
	assert.Nil(t, condOf(got, cnsregistervolumev1alpha1.ConditionPvcBound))
}

// TestConditionsDoNotRegressAcrossReconciles is the regression guard for the bug
// a single mutable phase had: an early unconditional write that reset progress on
// every pass. Conditions are per-milestone, so a repeated reconcile of a CR that
// has already registered its volume must not take that condition back to False.
func TestConditionsDoNotRegressAcrossReconciles(t *testing.T) {
	backOffDuration = make(map[types.NamespacedName]time.Duration)
	ctx := context.Background()

	crv := &cnsregistervolumev1alpha1.CnsRegisterVolume{
		ObjectMeta: metav1.ObjectMeta{Name: "test-volume", Namespace: "test-ns"},
		Spec: cnsregistervolumev1alpha1.CnsRegisterVolumeSpec{
			PvcName:  "test-pvc",
			VolumeID: "dummy-volume-id",
		},
		Status: cnsregistervolumev1alpha1.CnsRegisterVolumeStatus{
			Conditions: []metav1.Condition{
				{
					Type:               cnsregistervolumev1alpha1.ConditionSpecValidated,
					Status:             metav1.ConditionTrue,
					Reason:             cnsregistervolumev1alpha1.ReasonValidated,
					LastTransitionTime: metav1.NewTime(time.Now().Add(-time.Hour)),
				},
				{
					Type:               cnsregistervolumev1alpha1.ConditionCnsVolumeRegistered,
					Status:             metav1.ConditionTrue,
					Reason:             cnsregistervolumev1alpha1.ReasonRegistered,
					LastTransitionTime: metav1.NewTime(time.Now().Add(-time.Hour)),
				},
			},
		},
	}
	scheme := condTestScheme()
	fakeClient := fake.NewClientBuilder().WithScheme(scheme).
		WithObjects(crv).WithStatusSubresource(crv).Build()

	commonco.ContainerOrchestratorUtility = &mockCOCommon{}
	r := &ReconcileCnsRegisterVolume{
		client:   fakeClient,
		scheme:   scheme,
		recorder: record.NewFakeRecorder(10),
	}

	// This pass fails spec validation, which is the earliest possible exit. Even
	// so, a step already recorded as done must not be taken back.
	_, err := r.Reconcile(ctx, reconcile.Request{
		NamespacedName: types.NamespacedName{Name: "test-volume", Namespace: "test-ns"}})
	assert.NoError(t, err)

	got := getCondTestInstance(t, fakeClient, crv)
	registered := condOf(got, cnsregistervolumev1alpha1.ConditionCnsVolumeRegistered)
	assert.NotNil(t, registered)
	assert.Equal(t, metav1.ConditionTrue, registered.Status,
		"an already-registered volume must not be reported as unregistered by a later pass")
}

// TestReconcilePublishesTerminalAccessibilityCondition verifies that the paths
// which already stopped requeuing before this change now say so on the CR, and
// that the undone registration is reported rather than left claiming a volume
// that has been deleted.
func TestReconcilePublishesTerminalAccessibilityCondition(t *testing.T) {
	backOffDuration = make(map[types.NamespacedName]time.Duration)
	ctx := context.Background()
	patches := gomonkey.NewPatches()
	defer patches.Reset()

	r, _, cleanupCalled := newHostLocalReconcileFixture(patches, "HostLocalStorage")
	defer wireAsyncCnsTaskSuccess(r)()
	defer func() { fssEnabledOverride = nil }()

	checkDatastorePolicyCompatibilityFn = func(_ context.Context, _ *cnsvsphere.VirtualCenter,
		_ *cnsvsphere.DatastoreInfo, _ string) (bool, error) {
		return false, nil
	}
	defer func() { checkDatastorePolicyCompatibilityFn = checkDatastorePolicyCompatibility }()

	result, err := reconcileUntilSettled(ctx, r, reconcile.Request{
		NamespacedName: types.NamespacedName{Name: "test-volume", Namespace: "test-ns"}})

	assert.NoError(t, err)
	assert.Equal(t, reconcile.Result{}, result, "this path must still not requeue")
	assert.True(t, *cleanupCalled, "the CNS volume must still be cleaned up")

	got := &cnsregistervolumev1alpha1.CnsRegisterVolume{}
	assert.NoError(t, r.client.Get(ctx,
		types.NamespacedName{Name: "test-volume", Namespace: "test-ns"}, got))

	accessible := condOf(got, cnsregistervolumev1alpha1.ConditionVolumeAccessible)
	assert.NotNil(t, accessible, "a terminal accessibility failure must be reported")
	assert.Equal(t, metav1.ConditionFalse, accessible.Status)
	assert.Equal(t, cnsregistervolumev1alpha1.ReasonHostLocalPolicyIncompatible, accessible.Reason)

	registered := condOf(got, cnsregistervolumev1alpha1.ConditionCnsVolumeRegistered)
	assert.NotNil(t, registered)
	assert.Equal(t, metav1.ConditionFalse, registered.Status,
		"the CNS volume was deleted, so the registration must be reported as undone")
	assert.Equal(t, cnsregistervolumev1alpha1.ReasonVolumeCleanedUp, registered.Reason)

	assert.False(t, got.Status.Registered,
		"a terminal failure must never be published together with registered=true")
}

// TestReadyMirrorsBlockingCondition verifies that Ready answers "why is this not
// registered yet" without the reader walking the array, which is what the kubectl
// Reason column depends on.
func TestReadyMirrorsBlockingCondition(t *testing.T) {
	instance := newCondTestInstance(cnsregistervolumev1alpha1.CnsRegisterVolumeStatus{})

	applyConditions(instance, falseCondition(
		cnsregistervolumev1alpha1.ConditionCnsVolumeRegistered,
		cnsregistervolumev1alpha1.ReasonWaitingForTaskSlot, "Waiting for a CNS task slot"))
	ready := condOf(instance, cnsregistervolumev1alpha1.ConditionReady)
	require.NotNil(t, ready, "Ready must be published alongside the blocking step")
	assert.Equal(t, metav1.ConditionFalse, ready.Status)
	assert.Equal(t, cnsregistervolumev1alpha1.ReasonWaitingForTaskSlot, ready.Reason)

	// Once that step completes, Ready follows the next blocking step rather than
	// staying on the stale reason.
	applyConditions(instance,
		trueCondition(cnsregistervolumev1alpha1.ConditionCnsVolumeRegistered,
			cnsregistervolumev1alpha1.ReasonRegistered, "registered"),
		trueCondition(cnsregistervolumev1alpha1.ConditionVolumeAccessible,
			cnsregistervolumev1alpha1.ReasonAccessible, "accessible"),
		falseCondition(cnsregistervolumev1alpha1.ConditionPvcBound,
			cnsregistervolumev1alpha1.ReasonWaitingForBind, "Waiting for the PVC to be bound"))
	ready = condOf(instance, cnsregistervolumev1alpha1.ConditionReady)
	assert.Equal(t, cnsregistervolumev1alpha1.ReasonWaitingForBind, ready.Reason)
}

// TestExplicitReadyIsNotOverwritten verifies that the Finalizing reason survives.
// Every step is satisfied at that point, so a derived Ready would have nothing to
// mirror and must leave the explicit one alone.
func TestExplicitReadyIsNotOverwritten(t *testing.T) {
	instance := newCondTestInstance(cnsregistervolumev1alpha1.CnsRegisterVolumeStatus{})
	applyConditions(instance,
		trueCondition(cnsregistervolumev1alpha1.ConditionSpecValidated,
			cnsregistervolumev1alpha1.ReasonValidated, ""),
		trueCondition(cnsregistervolumev1alpha1.ConditionCnsVolumeRegistered,
			cnsregistervolumev1alpha1.ReasonRegistered, ""),
		trueCondition(cnsregistervolumev1alpha1.ConditionVolumeAccessible,
			cnsregistervolumev1alpha1.ReasonAccessible, ""),
		trueCondition(cnsregistervolumev1alpha1.ConditionPvcBound,
			cnsregistervolumev1alpha1.ReasonBound, ""),
		falseCondition(cnsregistervolumev1alpha1.ConditionReady,
			cnsregistervolumev1alpha1.ReasonFinalizing, "Applying bookkeeping"))

	ready := condOf(instance, cnsregistervolumev1alpha1.ConditionReady)
	require.NotNil(t, ready)
	assert.Equal(t, cnsregistervolumev1alpha1.ReasonFinalizing, ready.Reason)
}

// TestReadyIsNotDowngradedAfterSuccess verifies that a registered CR keeps
// Ready=True. Nothing else may publish progress once registration is complete.
func TestReadyIsNotDowngradedAfterSuccess(t *testing.T) {
	instance := newCondTestInstance(cnsregistervolumev1alpha1.CnsRegisterVolumeStatus{
		Registered: true,
	})
	applyConditions(instance, trueCondition(cnsregistervolumev1alpha1.ConditionReady,
		cnsregistervolumev1alpha1.ReasonRegistered, "Volume is registered"))

	// A late, stale step write must not take Ready back to False.
	applyConditions(instance, falseCondition(
		cnsregistervolumev1alpha1.ConditionPvcBound,
		cnsregistervolumev1alpha1.ReasonWaitingForBind, "Waiting for the PVC to be bound"))

	ready := condOf(instance, cnsregistervolumev1alpha1.ConditionReady)
	assert.Equal(t, metav1.ConditionTrue, ready.Status,
		"a registered CR must keep Ready=True")
}

// TestTerminalFailureReadyReportsTheCause verifies Ready carries the accessibility
// reason, not VolumeCleanedUp. CnsVolumeRegistered is an earlier step, so plain
// mirroring would surface the consequence of the failure instead of its cause.
func TestTerminalFailureReadyReportsTheCause(t *testing.T) {
	instance := newCondTestInstance(cnsregistervolumev1alpha1.CnsRegisterVolumeStatus{})
	applyConditions(instance,
		trueCondition(cnsregistervolumev1alpha1.ConditionSpecValidated,
			cnsregistervolumev1alpha1.ReasonValidated, ""),
		falseCondition(cnsregistervolumev1alpha1.ConditionVolumeAccessible,
			cnsregistervolumev1alpha1.ReasonDatastoreNotAccessibleToCluster, "not reachable"),
		falseCondition(cnsregistervolumev1alpha1.ConditionCnsVolumeRegistered,
			cnsregistervolumev1alpha1.ReasonVolumeCleanedUp, "deleted"),
		falseCondition(cnsregistervolumev1alpha1.ConditionReady,
			cnsregistervolumev1alpha1.ReasonDatastoreNotAccessibleToCluster, "not reachable"))

	ready := condOf(instance, cnsregistervolumev1alpha1.ConditionReady)
	require.NotNil(t, ready)
	assert.Equal(t, cnsregistervolumev1alpha1.ReasonDatastoreNotAccessibleToCluster, ready.Reason,
		"Ready must report why the volume is unusable, not that it was cleaned up")
}
