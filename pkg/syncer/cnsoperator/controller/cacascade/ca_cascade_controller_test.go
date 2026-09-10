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
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	k8stypes "k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	cnsconfig "sigs.k8s.io/vsphere-csi-driver/v3/pkg/common/config"
	"sigs.k8s.io/vsphere-csi-driver/v3/pkg/syncer/k8scloudoperator"
)

func newTestScheme(t *testing.T) *runtime.Scheme {
	t.Helper()
	s := runtime.NewScheme()
	require.NoError(t, certmanagerv1.AddToScheme(s))
	require.NoError(t, corev1.AddToScheme(s))
	return s
}

func newCA(revision *int) *certmanagerv1.Certificate {
	return &certmanagerv1.Certificate{
		ObjectMeta: metav1.ObjectMeta{
			Name:      k8scloudoperator.K8sCloudOperatorCACertName,
			Namespace: cnsconfig.DefaultCSINamespace,
		},
		Status: certmanagerv1.CertificateStatus{Revision: revision},
	}
}

func newLeaf(name string, annotations map[string]string) *certmanagerv1.Certificate {
	return &certmanagerv1.Certificate{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: cnsconfig.DefaultCSINamespace, Annotations: annotations},
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
	assert.False(t, isTargetCA(newLeaf(k8scloudoperator.K8sCloudOperatorServerCertName, nil)))

	other := newCA(nil)
	other.Namespace = "some-other-namespace"
	assert.False(t, isTargetCA(other))
}

func TestReconcile_CANotFound_NoOp(t *testing.T) {
	scheme := newTestScheme(t)
	fakeClient := fake.NewClientBuilder().WithScheme(scheme).Build()
	r := &ReconcileCACascade{client: fakeClient}

	res, err := r.Reconcile(context.Background(),
		reconcile.Request{NamespacedName: k8stypes.NamespacedName{
			Name:      k8scloudoperator.K8sCloudOperatorCACertName,
			Namespace: cnsconfig.DefaultCSINamespace,
		}})
	require.NoError(t, err)
	assert.Equal(t, reconcile.Result{}, res)
}

func TestReconcile_CANeverIssued_NoOp(t *testing.T) {
	scheme := newTestScheme(t)
	ca := newCA(nil) // Status.Revision is nil: never issued yet
	fakeClient := fake.NewClientBuilder().WithScheme(scheme).WithObjects(ca).Build()
	r := &ReconcileCACascade{client: fakeClient}

	_, err := r.Reconcile(context.Background(),
		reconcile.Request{NamespacedName: k8stypes.NamespacedName{
			Name:      k8scloudoperator.K8sCloudOperatorCACertName,
			Namespace: cnsconfig.DefaultCSINamespace,
		}})
	require.NoError(t, err)

	// Neither leaf exists; nothing should have been created or errored on.
	server := &certmanagerv1.Certificate{}
	err = fakeClient.Get(context.Background(),
		k8stypes.NamespacedName{
			Name:      k8scloudoperator.K8sCloudOperatorServerCertName,
			Namespace: cnsconfig.DefaultCSINamespace,
		}, server)
	assert.Error(t, err, "leaf should not have been created")
}

func TestReconcile_ForcesReissueOnNewCARevision(t *testing.T) {
	scheme := newTestScheme(t)
	ca := newCA(intPtr(5))
	ca.Spec.SecretName = "ca-secret"
	server := newLeaf(k8scloudoperator.K8sCloudOperatorServerCertName, nil)
	server.Spec.SecretName = "server-secret"
	client := newLeaf(k8scloudoperator.K8sCloudOperatorClientCertName, nil)
	client.Spec.SecretName = "client-secret"
	// No Secret objects seeded: leafTrustsLiveCA has nothing to compare yet
	// (known=false), so this deliberately exercises the "unknown - fall back
	// to reissuing" branch of forceReissueIfStale, not a confirmed mismatch.
	fakeClient := fake.NewClientBuilder().WithScheme(scheme).
		WithStatusSubresource(&certmanagerv1.Certificate{}).
		WithObjects(ca, server, client).Build()
	r := &ReconcileCACascade{client: fakeClient}

	_, err := r.Reconcile(context.Background(),
		reconcile.Request{NamespacedName: k8stypes.NamespacedName{
			Name:      k8scloudoperator.K8sCloudOperatorCACertName,
			Namespace: cnsconfig.DefaultCSINamespace,
		}})
	require.NoError(t, err)

	certNames := []string{k8scloudoperator.K8sCloudOperatorServerCertName, k8scloudoperator.K8sCloudOperatorClientCertName}
	for _, name := range certNames {
		got := &certmanagerv1.Certificate{}
		require.NoError(t, fakeClient.Get(context.Background(),
			k8stypes.NamespacedName{Name: name, Namespace: cnsconfig.DefaultCSINamespace}, got))

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
	ca.Spec.SecretName = "ca-secret"
	// Both leaves already record revision 5: nothing further should happen.
	annotations := map[string]string{lastCascadedCARevisionAnnotation: "5"}
	server := newLeaf(k8scloudoperator.K8sCloudOperatorServerCertName, annotations)
	server.Spec.SecretName = "server-secret"
	client := newLeaf(k8scloudoperator.K8sCloudOperatorClientCertName, annotations)
	client.Spec.SecretName = "client-secret"
	// No Secret objects seeded: the periodic drift check that runs after the
	// annotation short-circuit has nothing to compare (known=false), so it
	// deliberately stays a no-op rather than happening to skip by accident.
	fakeClient := fake.NewClientBuilder().WithScheme(scheme).
		WithStatusSubresource(&certmanagerv1.Certificate{}).
		WithObjects(ca, server, client).Build()
	r := &ReconcileCACascade{client: fakeClient}

	_, err := r.Reconcile(context.Background(),
		reconcile.Request{NamespacedName: k8stypes.NamespacedName{
			Name:      k8scloudoperator.K8sCloudOperatorCACertName,
			Namespace: cnsconfig.DefaultCSINamespace,
		}})
	require.NoError(t, err)

	certNames := []string{k8scloudoperator.K8sCloudOperatorServerCertName, k8scloudoperator.K8sCloudOperatorClientCertName}
	for _, name := range certNames {
		got := &certmanagerv1.Certificate{}
		require.NoError(t, fakeClient.Get(context.Background(),
			k8stypes.NamespacedName{Name: name, Namespace: cnsconfig.DefaultCSINamespace}, got))
		assert.Empty(t, got.Status.Conditions, "an already-cascaded leaf should not be touched again")
	}
}

func TestReconcile_NewerCARevision_ReissuesAgain(t *testing.T) {
	scheme := newTestScheme(t)
	ca := newCA(intPtr(6)) // CA has moved on since the leaf was last cascaded
	ca.Spec.SecretName = "ca-secret"
	annotations := map[string]string{lastCascadedCARevisionAnnotation: "5"}
	server := newLeaf(k8scloudoperator.K8sCloudOperatorServerCertName, annotations)
	server.Spec.SecretName = "server-secret"
	client := newLeaf(k8scloudoperator.K8sCloudOperatorClientCertName, annotations)
	client.Spec.SecretName = "client-secret"
	// No Secret objects seeded: deliberately exercises the "unknown" branch
	// of forceReissueIfStale, same as TestReconcile_ForcesReissueOnNewCARevision.
	fakeClient := fake.NewClientBuilder().WithScheme(scheme).
		WithStatusSubresource(&certmanagerv1.Certificate{}).
		WithObjects(ca, server, client).Build()
	r := &ReconcileCACascade{client: fakeClient}

	_, err := r.Reconcile(context.Background(),
		reconcile.Request{NamespacedName: k8stypes.NamespacedName{
			Name:      k8scloudoperator.K8sCloudOperatorCACertName,
			Namespace: cnsconfig.DefaultCSINamespace,
		}})
	require.NoError(t, err)

	got := &certmanagerv1.Certificate{}
	require.NoError(t, fakeClient.Get(context.Background(),
		k8stypes.NamespacedName{Name: k8scloudoperator.K8sCloudOperatorServerCertName,
			Namespace: cnsconfig.DefaultCSINamespace}, got))
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
		reconcile.Request{NamespacedName: k8stypes.NamespacedName{
			Name:      k8scloudoperator.K8sCloudOperatorCACertName,
			Namespace: cnsconfig.DefaultCSINamespace,
		}})
	require.NoError(t, err)
}

func newSecret(name string, data map[string][]byte) *corev1.Secret {
	return &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: cnsconfig.DefaultCSINamespace},
		Data:       data,
	}
}

func TestReconcile_ExistingIssuingCondition_ReplacedInPlace(t *testing.T) {
	scheme := newTestScheme(t)
	ca := newCA(intPtr(5))
	server := newLeaf(k8scloudoperator.K8sCloudOperatorServerCertName, nil)
	// Simulate a stale Issuing condition left over from a prior cascade (or
	// from cert-manager's own processing). Appending a second Issuing entry
	// would violate the listType=map contract on status.conditions and the
	// real API server would reject the whole update.
	server.Status.Conditions = []certmanagerv1.CertificateCondition{{
		Type:   certmanagerv1.CertificateConditionIssuing,
		Status: cmmeta.ConditionTrue,
		Reason: "PriorCascade",
	}}
	client := newLeaf(k8scloudoperator.K8sCloudOperatorClientCertName, nil)
	fakeClient := fake.NewClientBuilder().WithScheme(scheme).
		WithStatusSubresource(&certmanagerv1.Certificate{}).
		WithObjects(ca, server, client).Build()
	r := &ReconcileCACascade{client: fakeClient}

	_, err := r.Reconcile(context.Background(),
		reconcile.Request{NamespacedName: k8stypes.NamespacedName{
			Name:      k8scloudoperator.K8sCloudOperatorCACertName,
			Namespace: cnsconfig.DefaultCSINamespace,
		}})
	require.NoError(t, err)

	got := &certmanagerv1.Certificate{}
	require.NoError(t, fakeClient.Get(context.Background(),
		k8stypes.NamespacedName{Name: k8scloudoperator.K8sCloudOperatorServerCertName,
			Namespace: cnsconfig.DefaultCSINamespace}, got))
	require.Len(t, got.Status.Conditions, 1, "the stale Issuing condition should be replaced, not duplicated")
	assert.Equal(t, "CACascade", got.Status.Conditions[0].Reason)
}

func TestReconcile_DriftDetected_ForcesReissueDespiteMatchingAnnotation(t *testing.T) {
	scheme := newTestScheme(t)
	ca := newCA(intPtr(5))
	ca.Spec.SecretName = "ca-secret"
	annotations := map[string]string{lastCascadedCARevisionAnnotation: "5"}
	server := newLeaf(k8scloudoperator.K8sCloudOperatorServerCertName, annotations)
	server.Spec.SecretName = "server-secret"
	client := newLeaf(k8scloudoperator.K8sCloudOperatorClientCertName, annotations)
	client.Spec.SecretName = "client-secret"

	// The annotation claims both leaves are already cascaded to revision 5,
	// but their cached CA (ca.crt) no longer matches the CA's actual live
	// Secret content - e.g. an out-of-band CA rotation that didn't advance
	// status.revision the way expected.
	caSecret := newSecret("ca-secret", map[string][]byte{"tls.crt": []byte("live-ca-v2")})
	serverSecret := newSecret("server-secret", map[string][]byte{"ca.crt": []byte("stale-ca-v1")})
	clientSecret := newSecret("client-secret", map[string][]byte{"ca.crt": []byte("stale-ca-v1")})

	fakeClient := fake.NewClientBuilder().WithScheme(scheme).
		WithStatusSubresource(&certmanagerv1.Certificate{}).
		WithObjects(ca, server, client, caSecret, serverSecret, clientSecret).Build()
	r := &ReconcileCACascade{client: fakeClient}

	_, err := r.Reconcile(context.Background(),
		reconcile.Request{NamespacedName: k8stypes.NamespacedName{
			Name:      k8scloudoperator.K8sCloudOperatorCACertName,
			Namespace: cnsconfig.DefaultCSINamespace,
		}})
	require.NoError(t, err)

	certNames := []string{k8scloudoperator.K8sCloudOperatorServerCertName, k8scloudoperator.K8sCloudOperatorClientCertName}
	for _, name := range certNames {
		got := &certmanagerv1.Certificate{}
		require.NoError(t, fakeClient.Get(context.Background(),
			k8stypes.NamespacedName{Name: name, Namespace: cnsconfig.DefaultCSINamespace}, got))
		require.Len(t, got.Status.Conditions, 1,
			"leaf %s should be force-reissued despite a matching cascade annotation", name)
		assert.Equal(t, "CADriftDetected", got.Status.Conditions[0].Reason)
	}
}

func TestReconcile_LeafAlreadyTrustsLiveCA_BackfillsAnnotationWithoutReissue(t *testing.T) {
	scheme := newTestScheme(t)
	ca := newCA(intPtr(5))
	ca.Spec.SecretName = "ca-secret"
	// No cascade annotation at all - e.g. the leaf's own independent renewal
	// happened to pick up the current CA before this controller ever acted.
	server := newLeaf(k8scloudoperator.K8sCloudOperatorServerCertName, nil)
	server.Spec.SecretName = "server-secret"
	client := newLeaf(k8scloudoperator.K8sCloudOperatorClientCertName, nil)
	client.Spec.SecretName = "client-secret"

	caSecret := newSecret("ca-secret", map[string][]byte{"tls.crt": []byte("live-ca-v1")})
	serverSecret := newSecret("server-secret", map[string][]byte{"ca.crt": []byte("live-ca-v1")})
	clientSecret := newSecret("client-secret", map[string][]byte{"ca.crt": []byte("live-ca-v1")})

	fakeClient := fake.NewClientBuilder().WithScheme(scheme).
		WithStatusSubresource(&certmanagerv1.Certificate{}).
		WithObjects(ca, server, client, caSecret, serverSecret, clientSecret).Build()
	r := &ReconcileCACascade{client: fakeClient}

	_, err := r.Reconcile(context.Background(),
		reconcile.Request{NamespacedName: k8stypes.NamespacedName{
			Name:      k8scloudoperator.K8sCloudOperatorCACertName,
			Namespace: cnsconfig.DefaultCSINamespace,
		}})
	require.NoError(t, err)

	certNames := []string{k8scloudoperator.K8sCloudOperatorServerCertName, k8scloudoperator.K8sCloudOperatorClientCertName}
	for _, name := range certNames {
		got := &certmanagerv1.Certificate{}
		require.NoError(t, fakeClient.Get(context.Background(),
			k8stypes.NamespacedName{Name: name, Namespace: cnsconfig.DefaultCSINamespace}, got))
		assert.Equal(t, "5", got.Annotations[lastCascadedCARevisionAnnotation],
			"leaf %s should have its cascade annotation backfilled", name)
		assert.Empty(t, got.Status.Conditions,
			"leaf %s already trusts the live CA, so no reissue should be forced", name)
	}
}

func TestReconcile_SecretsExistButDataMissing_ForcesReissueRatherThanBackfill(t *testing.T) {
	scheme := newTestScheme(t)
	ca := newCA(intPtr(5))
	ca.Spec.SecretName = "ca-secret"
	// No cascade annotation, same as the backfill case above - but here
	// neither Secret has its expected key populated yet (e.g. still being
	// written by cert-manager). bytes.Equal(nil, nil) would report two
	// absent byte slices as "equal", so without treating this as unknown,
	// the controller would incorrectly conclude trust and skip reissuing.
	server := newLeaf(k8scloudoperator.K8sCloudOperatorServerCertName, nil)
	server.Spec.SecretName = "server-secret"
	client := newLeaf(k8scloudoperator.K8sCloudOperatorClientCertName, nil)
	client.Spec.SecretName = "client-secret"

	caSecret := newSecret("ca-secret", map[string][]byte{})
	serverSecret := newSecret("server-secret", map[string][]byte{})
	clientSecret := newSecret("client-secret", map[string][]byte{})

	fakeClient := fake.NewClientBuilder().WithScheme(scheme).
		WithStatusSubresource(&certmanagerv1.Certificate{}).
		WithObjects(ca, server, client, caSecret, serverSecret, clientSecret).Build()
	r := &ReconcileCACascade{client: fakeClient}

	_, err := r.Reconcile(context.Background(),
		reconcile.Request{NamespacedName: k8stypes.NamespacedName{
			Name:      k8scloudoperator.K8sCloudOperatorCACertName,
			Namespace: cnsconfig.DefaultCSINamespace,
		}})
	require.NoError(t, err)

	certNames := []string{k8scloudoperator.K8sCloudOperatorServerCertName, k8scloudoperator.K8sCloudOperatorClientCertName}
	for _, name := range certNames {
		got := &certmanagerv1.Certificate{}
		require.NoError(t, fakeClient.Get(context.Background(),
			k8stypes.NamespacedName{Name: name, Namespace: cnsconfig.DefaultCSINamespace}, got))
		require.Len(t, got.Status.Conditions, 1,
			"leaf %s should be force-reissued, not backfilled, when Secret data can't be compared", name)
		assert.Equal(t, "CACascade", got.Status.Conditions[0].Reason)
	}
}
