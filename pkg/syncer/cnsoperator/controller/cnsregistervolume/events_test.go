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
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	k8sfake "k8s.io/client-go/kubernetes/fake"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	"k8s.io/client-go/tools/reference"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
	cnsregistervolumev1alpha1 "sigs.k8s.io/vsphere-csi-driver/v3/pkg/apis/cnsoperator/cnsregistervolume/v1alpha1"
	"sigs.k8s.io/vsphere-csi-driver/v3/pkg/csi/service/common/commonco"
)

const (
	eventTestName      = "cns-register-volume-events"
	eventTestNamespace = "events-ns"
)

// newEventTestRecorder returns the production recorder wired to a fake
// clientset, so tests observe what would reach the API server.
func newEventTestRecorder(t *testing.T) (*ReconcileCnsRegisterVolume, *k8sfake.Clientset,
	*cnsregistervolumev1alpha1.CnsRegisterVolume) {
	t.Helper()
	backOffDuration = make(map[types.NamespacedName]time.Duration)
	crv := &cnsregistervolumev1alpha1.CnsRegisterVolume{
		ObjectMeta: metav1.ObjectMeta{Name: eventTestName, Namespace: eventTestNamespace},
		Spec: cnsregistervolumev1alpha1.CnsRegisterVolumeSpec{
			PvcName:    "events-pvc",
			VolumeID:   "events-volume-id",
			AccessMode: corev1.ReadWriteMany,
		},
	}
	s := condTestScheme()
	c := fake.NewClientBuilder().WithScheme(s).WithObjects(crv).WithStatusSubresource(crv).Build()
	k8sclient := k8sfake.NewSimpleClientset()
	recorder, broadcaster := newEventRecorder(k8sclient, s)
	t.Cleanup(broadcaster.Shutdown)
	r := &ReconcileCnsRegisterVolume{
		client:     c,
		scheme:     s,
		configInfo: testConfigInfo(),
		k8sclient:  k8sclient,
		recorder:   recorder,
	}
	return r, k8sclient, crv
}

// waitForEvent waits for the broadcaster to deliver an event with the given
// type and reason for the test CnsRegisterVolume, and returns it.
func waitForEvent(t *testing.T, k8sclient *k8sfake.Clientset, eventType, reason string) *corev1.Event {
	t.Helper()
	var found *corev1.Event
	require.Eventually(t, func() bool {
		events, err := k8sclient.CoreV1().Events(eventTestNamespace).List(context.Background(),
			metav1.ListOptions{})
		if err != nil {
			return false
		}
		for i := range events.Items {
			if events.Items[i].Type == eventType && events.Items[i].Reason == reason {
				found = &events.Items[i]
				return true
			}
		}
		return false
	}, 5*time.Second, 20*time.Millisecond, "expected a %s %s event to be recorded", eventType, reason)
	return found
}

// TestClientGoSchemeCannotReferenceCnsRegisterVolume documents why the recorder
// must use the manager's scheme. The controller-runtime client leaves TypeMeta
// empty on objects it decodes, so the recorder has to resolve the kind from its
// scheme, and client-go's kubernetes scheme does not know CnsRegisterVolume.
func TestClientGoSchemeCannotReferenceCnsRegisterVolume(t *testing.T) {
	crv := &cnsregistervolumev1alpha1.CnsRegisterVolume{
		ObjectMeta: metav1.ObjectMeta{Name: eventTestName, Namespace: eventTestNamespace},
	}

	_, err := reference.GetReference(clientgoscheme.Scheme, crv)
	require.Error(t, err, "client-go's scheme must not be able to reference a CnsRegisterVolume")
	assert.Contains(t, err.Error(), "no kind is registered")

	ref, err := reference.GetReference(condTestScheme(), crv)
	require.NoError(t, err)
	assert.Equal(t, "CnsRegisterVolume", ref.Kind)
	assert.Equal(t, "cnsoperator.vmware.com/v1alpha1", ref.APIVersion)
	assert.Equal(t, eventTestName, ref.Name)
	assert.Equal(t, eventTestNamespace, ref.Namespace)
}

// TestEventRecordedAfterStatusPatchWithoutTypeMeta covers setInstanceError,
// which patches status and then records a Warning. The patch adopts the decoded
// object into the instance, so TypeMeta is empty when the event is recorded.
func TestEventRecordedAfterStatusPatchWithoutTypeMeta(t *testing.T) {
	ctx := context.Background()
	r, k8sclient, crv := newEventTestRecorder(t)
	instance := getCondTestInstance(t, r.client, crv)
	instance.TypeMeta = metav1.TypeMeta{}

	setInstanceError(ctx, r, instance, "registration failed for a test reason")

	event := waitForEvent(t, k8sclient, corev1.EventTypeWarning, "CnsRegisterVolumeFailed")
	assert.Equal(t, "CnsRegisterVolume", event.InvolvedObject.Kind)
	assert.Equal(t, eventTestName, event.InvolvedObject.Name)
	assert.Equal(t, eventTestNamespace, event.InvolvedObject.Namespace)
	assert.Equal(t, "registration failed for a test reason", event.Message)
}

// TestEventRecordedOnSuccessWithoutTypeMeta covers the Normal event path, which
// shares the recorder with the Warning path.
func TestEventRecordedOnSuccessWithoutTypeMeta(t *testing.T) {
	r, k8sclient, crv := newEventTestRecorder(t)
	instance := getCondTestInstance(t, r.client, crv)
	instance.TypeMeta = metav1.TypeMeta{}

	recordEvent(context.Background(), r, instance, corev1.EventTypeNormal, "registered")

	event := waitForEvent(t, k8sclient, corev1.EventTypeNormal, "CnsRegisterVolumeSucceeded")
	assert.Equal(t, "CnsRegisterVolume", event.InvolvedObject.Kind)
}

// TestReconcileAccessModeMismatchRecordsEvent reproduces the reported failure
// end to end: a ReadWriteMany registration with no volumeMode is rejected on its
// first reconcile, after the finalizer update and status patches, and the
// rejection must reach the API server as a Warning event on the CR.
func TestReconcileAccessModeMismatchRecordsEvent(t *testing.T) {
	commonco.ContainerOrchestratorUtility = &mockCOCommon{}
	r, k8sclient, crv := newEventTestRecorder(t)

	_, err := r.Reconcile(context.Background(), reconcile.Request{
		NamespacedName: types.NamespacedName{Name: eventTestName, Namespace: eventTestNamespace}})
	require.NoError(t, err)

	got := getCondTestInstance(t, r.client, crv)
	cond := condOf(got, cnsregistervolumev1alpha1.ConditionSpecValidated)
	require.NotNil(t, cond)
	assert.Equal(t, metav1.ConditionFalse, cond.Status)
	assert.Equal(t, cnsregistervolumev1alpha1.ReasonInvalidSpec, cond.Reason)
	assert.False(t, got.Status.Registered)

	event := waitForEvent(t, k8sclient, corev1.EventTypeWarning, "CnsRegisterVolumeFailed")
	assert.Equal(t, "CnsRegisterVolume", event.InvolvedObject.Kind)
	assert.Equal(t, eventTestName, event.InvolvedObject.Name)
	assert.True(t, strings.HasPrefix(event.Message, "AccessMode: ReadWriteMany is not supported"),
		event.Message)
}

// TestClientGoSchemeRecorderDropsEvent is the negative control for the tests
// above: built with client-go's scheme, as the controller used to be, the same
// setInstanceError call publishes no event.
func TestClientGoSchemeRecorderDropsEvent(t *testing.T) {
	r, _, crv := newEventTestRecorder(t)
	k8sclient := k8sfake.NewSimpleClientset()
	recorder, broadcaster := newEventRecorder(k8sclient, clientgoscheme.Scheme)
	t.Cleanup(broadcaster.Shutdown)
	r.recorder = recorder
	instance := getCondTestInstance(t, r.client, crv)
	instance.TypeMeta = metav1.TypeMeta{}

	setInstanceError(context.Background(), r, instance, "registration failed for a test reason")

	assert.Never(t, func() bool {
		events, err := k8sclient.CoreV1().Events(eventTestNamespace).List(context.Background(),
			metav1.ListOptions{})
		return err == nil && len(events.Items) > 0
	}, 300*time.Millisecond, 20*time.Millisecond, "client-go's scheme cannot reference a CnsRegisterVolume")
}
