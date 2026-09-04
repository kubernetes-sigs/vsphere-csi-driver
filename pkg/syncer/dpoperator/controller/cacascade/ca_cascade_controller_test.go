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

package cacascade

import (
	"context"
	"testing"

	certmanagerv1 "github.com/cert-manager/cert-manager/pkg/apis/certmanager/v1"
	cmmeta "github.com/cert-manager/cert-manager/pkg/apis/meta/v1"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	k8stypes "k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
)

func newTestScheme(t *testing.T) *runtime.Scheme {
	t.Helper()
	s := runtime.NewScheme()
	require.NoError(t, certmanagerv1.AddToScheme(s))
	return s
}

func newCA(revision *int) *certmanagerv1.Certificate {
	return &certmanagerv1.Certificate{
		ObjectMeta: metav1.ObjectMeta{Name: caCertName, Namespace: targetNamespace},
		Status:     certmanagerv1.CertificateStatus{Revision: revision},
	}
}

func newLeaf(name string, annotations map[string]string) *certmanagerv1.Certificate {
	return &certmanagerv1.Certificate{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: targetNamespace, Annotations: annotations},
	}
}

func intPtr(i int) *int { return &i }

func TestRevisionEqual(t *testing.T) {
	assert.True(t, revisionEqual(nil, nil))
	assert.False(t, revisionEqual(nil, intPtr(1)))
	assert.False(t, revisionEqual(intPtr(1), nil))
	assert.False(t, revisionEqual(intPtr(1), intPtr(2)))
	assert.True(t, revisionEqual(intPtr(1), intPtr(1)))
}

func TestIsTargetCA(t *testing.T) {
	assert.True(t, isTargetCA(newCA(nil)))
	assert.False(t, isTargetCA(newLeaf(serverCertName, nil)))

	other := newCA(nil)
	other.Namespace = "some-other-namespace"
	assert.False(t, isTargetCA(other))
}

func TestReconcile_CANotFound_NoOp(t *testing.T) {
	scheme := newTestScheme(t)
	fakeClient := fake.NewClientBuilder().WithScheme(scheme).Build()
	r := &ReconcileCACascade{client: fakeClient}

	res, err := r.Reconcile(context.Background(),
		reconcile.Request{NamespacedName: k8stypes.NamespacedName{Name: caCertName, Namespace: targetNamespace}})
	require.NoError(t, err)
	assert.Equal(t, reconcile.Result{}, res)
}

func TestReconcile_CANeverIssued_NoOp(t *testing.T) {
	scheme := newTestScheme(t)
	ca := newCA(nil) // Status.Revision is nil: never issued yet
	fakeClient := fake.NewClientBuilder().WithScheme(scheme).WithObjects(ca).Build()
	r := &ReconcileCACascade{client: fakeClient}

	_, err := r.Reconcile(context.Background(),
		reconcile.Request{NamespacedName: k8stypes.NamespacedName{Name: caCertName, Namespace: targetNamespace}})
	require.NoError(t, err)

	// Neither leaf exists; nothing should have been created or errored on.
	server := &certmanagerv1.Certificate{}
	err = fakeClient.Get(context.Background(),
		k8stypes.NamespacedName{Name: serverCertName, Namespace: targetNamespace}, server)
	assert.Error(t, err, "leaf should not have been created")
}

func TestReconcile_ForcesReissueOnNewCARevision(t *testing.T) {
	scheme := newTestScheme(t)
	ca := newCA(intPtr(5))
	server := newLeaf(serverCertName, nil)
	client := newLeaf(clientCertName, nil)
	fakeClient := fake.NewClientBuilder().WithScheme(scheme).
		WithStatusSubresource(&certmanagerv1.Certificate{}).
		WithObjects(ca, server, client).Build()
	r := &ReconcileCACascade{client: fakeClient}

	_, err := r.Reconcile(context.Background(),
		reconcile.Request{NamespacedName: k8stypes.NamespacedName{Name: caCertName, Namespace: targetNamespace}})
	require.NoError(t, err)

	for _, name := range []string{serverCertName, clientCertName} {
		got := &certmanagerv1.Certificate{}
		require.NoError(t, fakeClient.Get(context.Background(),
			k8stypes.NamespacedName{Name: name, Namespace: targetNamespace}, got))

		assert.Equal(t, "5", got.Annotations[lastCascadedCARevisionAnnotation],
			"leaf %s should be annotated with the cascaded CA revision", name)

		require.Len(t, got.Status.Conditions, 1)
		cond := got.Status.Conditions[0]
		assert.Equal(t, certmanagerv1.CertificateConditionIssuing, cond.Type)
		assert.Equal(t, cmmeta.ConditionTrue, cond.Status)
		assert.Equal(t, "CACascade", cond.Reason)
	}
}

func TestReconcile_AlreadyCascaded_IsNoOp(t *testing.T) {
	scheme := newTestScheme(t)
	ca := newCA(intPtr(5))
	// Both leaves already record revision 5: nothing further should happen.
	server := newLeaf(serverCertName, map[string]string{lastCascadedCARevisionAnnotation: "5"})
	client := newLeaf(clientCertName, map[string]string{lastCascadedCARevisionAnnotation: "5"})
	fakeClient := fake.NewClientBuilder().WithScheme(scheme).
		WithStatusSubresource(&certmanagerv1.Certificate{}).
		WithObjects(ca, server, client).Build()
	r := &ReconcileCACascade{client: fakeClient}

	_, err := r.Reconcile(context.Background(),
		reconcile.Request{NamespacedName: k8stypes.NamespacedName{Name: caCertName, Namespace: targetNamespace}})
	require.NoError(t, err)

	for _, name := range []string{serverCertName, clientCertName} {
		got := &certmanagerv1.Certificate{}
		require.NoError(t, fakeClient.Get(context.Background(),
			k8stypes.NamespacedName{Name: name, Namespace: targetNamespace}, got))
		assert.Empty(t, got.Status.Conditions, "an already-cascaded leaf should not be touched again")
	}
}

func TestReconcile_NewerCARevision_ReissuesAgain(t *testing.T) {
	scheme := newTestScheme(t)
	ca := newCA(intPtr(6)) // CA has moved on since the leaf was last cascaded
	server := newLeaf(serverCertName, map[string]string{lastCascadedCARevisionAnnotation: "5"})
	client := newLeaf(clientCertName, map[string]string{lastCascadedCARevisionAnnotation: "5"})
	fakeClient := fake.NewClientBuilder().WithScheme(scheme).
		WithStatusSubresource(&certmanagerv1.Certificate{}).
		WithObjects(ca, server, client).Build()
	r := &ReconcileCACascade{client: fakeClient}

	_, err := r.Reconcile(context.Background(),
		reconcile.Request{NamespacedName: k8stypes.NamespacedName{Name: caCertName, Namespace: targetNamespace}})
	require.NoError(t, err)

	got := &certmanagerv1.Certificate{}
	require.NoError(t, fakeClient.Get(context.Background(),
		k8stypes.NamespacedName{Name: serverCertName, Namespace: targetNamespace}, got))
	assert.Equal(t, "6", got.Annotations[lastCascadedCARevisionAnnotation])
	require.Len(t, got.Status.Conditions, 1)
}

func TestReconcile_LeafNotFound_SkipsWithoutError(t *testing.T) {
	scheme := newTestScheme(t)
	ca := newCA(intPtr(5))
	// Neither leaf exists yet (e.g. not applied yet on this manifest version).
	fakeClient := fake.NewClientBuilder().WithScheme(scheme).
		WithStatusSubresource(&certmanagerv1.Certificate{}).
		WithObjects(ca).Build()
	r := &ReconcileCACascade{client: fakeClient}

	_, err := r.Reconcile(context.Background(),
		reconcile.Request{NamespacedName: k8stypes.NamespacedName{Name: caCertName, Namespace: targetNamespace}})
	require.NoError(t, err)
}
