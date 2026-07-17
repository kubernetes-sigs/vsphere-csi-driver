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

package snapshotmetadataservice

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"errors"
	"math/big"
	"net"
	"sync"
	"testing"
	"time"

	certmanagerv1 "github.com/cert-manager/cert-manager/pkg/apis/certmanager/v1"
	cmmeta "github.com/cert-manager/cert-manager/pkg/apis/meta/v1"
	snapshotmetadatav1beta1 "github.com/kubernetes-csi/external-snapshot-metadata/client/apis/snapshotmetadataservice/v1beta1"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	cnstypes "github.com/vmware/govmomi/cns/types"
	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	k8stypes "k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/record"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"
	"sigs.k8s.io/controller-runtime/pkg/manager"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	"sigs.k8s.io/vsphere-csi-driver/v3/pkg/common/unittestcommon"
	"sigs.k8s.io/vsphere-csi-driver/v3/pkg/csi/service/common"
	"sigs.k8s.io/vsphere-csi-driver/v3/pkg/csi/service/common/commonco"
)

// ---------------------------------------------------------------------------
// shared test fixtures / helpers
// ---------------------------------------------------------------------------

// newTestScheme creates a runtime scheme with all types required for the
// SnapshotMetadataService controller tests.
func newTestScheme(t *testing.T) *runtime.Scheme {
	t.Helper()
	s := runtime.NewScheme()
	require.NoError(t, v1.AddToScheme(s))
	require.NoError(t, certmanagerv1.AddToScheme(s))
	require.NoError(t, snapshotmetadatav1beta1.AddToScheme(s))
	return s
}

// resetBackOffDuration ensures each test starts with a clean backOffDuration map.
func resetBackOffDuration() {
	backOffDurationMapMutex.Lock()
	defer backOffDurationMapMutex.Unlock()
	backOffDuration = make(map[string]time.Duration)
}

// readyCertificate returns a Certificate object that is in Ready=True state.
func readyCertificate(name, namespace, secretName string, revision int) *certmanagerv1.Certificate {
	rev := revision
	return &certmanagerv1.Certificate{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: namespace,
		},
		Spec: certmanagerv1.CertificateSpec{
			SecretName: secretName,
		},
		Status: certmanagerv1.CertificateStatus{
			Revision: &rev,
			Conditions: []certmanagerv1.CertificateCondition{
				{
					Type:   certmanagerv1.CertificateConditionReady,
					Status: cmmeta.ConditionTrue,
				},
			},
		},
	}
}

// notReadyCertificate returns a Certificate object that is not in Ready=True
// state.
func notReadyCertificate(name, namespace, secretName string) *certmanagerv1.Certificate {
	return &certmanagerv1.Certificate{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: namespace,
		},
		Spec: certmanagerv1.CertificateSpec{
			SecretName: secretName,
		},
		Status: certmanagerv1.CertificateStatus{
			Conditions: []certmanagerv1.CertificateCondition{
				{
					Type:   certmanagerv1.CertificateConditionReady,
					Status: cmmeta.ConditionFalse,
				},
			},
		},
	}
}

// makeLBService creates a Service with a LoadBalancer ingress entry for ip and
// a default gRPC service port. If ip is empty the Ingress slice is empty
// (simulating an unassigned LB IP).
func makeLBService(ip string) *v1.Service {
	return makeLBServiceWithPort(ip, 8100)
}

func makeLBServiceWithPort(ip string, port int32) *v1.Service {
	svc := &v1.Service{
		ObjectMeta: metav1.ObjectMeta{
			Name:      targetServiceName,
			Namespace: targetNamespace,
		},
		Spec: v1.ServiceSpec{
			Ports: []v1.ServicePort{{Port: port}},
		},
	}
	if ip != "" {
		svc.Status.LoadBalancer.Ingress = []v1.LoadBalancerIngress{{IP: ip}}
	}
	return svc
}

// makeSMSCR returns a SnapshotMetadataService CR fixture with the given caCert
// and address already set (as if a previous reconcile had synced them).
func makeSMSCR(caCert []byte, address string) *snapshotmetadatav1beta1.SnapshotMetadataService {
	return &snapshotmetadatav1beta1.SnapshotMetadataService{
		ObjectMeta: metav1.ObjectMeta{Name: targetSMSName},
		Spec: snapshotmetadatav1beta1.SnapshotMetadataServiceSpec{
			CACert:   caCert,
			Address:  address,
			Audience: "test-audience",
		},
	}
}

// generateSelfSignedCertPEM returns a PEM-encoded self-signed certificate
// (the "ca.crt" shape produced by a selfSigned cert-manager Issuer) whose IP
// SANs are ips. Used to build realistic Secret data for the certPEMCoversIP
// gate tests without depending on a real cert-manager instance.
func generateSelfSignedCertPEM(t *testing.T, ips ...net.IP) []byte {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	require.NoError(t, err)

	tmpl := &x509.Certificate{
		SerialNumber: big.NewInt(1),
		Subject:      pkix.Name{CommonName: "vsphere-csi-snapshot-metadata-service"},
		NotBefore:    time.Now().Add(-time.Hour),
		NotAfter:     time.Now().Add(time.Hour),
		IPAddresses:  ips,
		IsCA:         true,
		KeyUsage:     x509.KeyUsageCertSign | x509.KeyUsageDigitalSignature,
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, &key.PublicKey, key)
	require.NoError(t, err)
	return pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der})
}

// withInjectedCO swaps the package-level getContainerOrchestratorInterface
// indirection with one that returns the supplied CO and error, and returns a
// cleanup function to restore the original implementation.
func withInjectedCO(co commonco.COCommonInterface, err error) func() {
	original := getContainerOrchestratorInterface
	getContainerOrchestratorInterface = func(ctx context.Context, _ int,
		_ cnstypes.CnsClusterFlavor, _ interface{}) (commonco.COCommonInterface, error) {
		return co, err
	}
	return func() {
		getContainerOrchestratorInterface = original
	}
}

// fssCallTracker wraps a COCommonInterface to record which feature names had
// their state queried. It lets us assert that Add only consults the
// CSI_Backup_API gate before bailing out.
type fssCallTracker struct {
	commonco.COCommonInterface
	mu         sync.Mutex
	fssQueries []string
}

func (f *fssCallTracker) IsFSSEnabled(ctx context.Context, featureName string) bool {
	f.mu.Lock()
	f.fssQueries = append(f.fssQueries, featureName)
	f.mu.Unlock()
	return f.COCommonInterface.IsFSSEnabled(ctx, featureName)
}

// Reference manager.Manager so the import is recognised as used. Add expects
// a manager.Manager argument; the FSS-disabled tests pass nil because Add
// short-circuits before touching it.
var _ manager.Manager = (manager.Manager)(nil)

// smsReconcileRequest is the reconcile.Request every watch in this package
// maps onto: the single, cluster-scoped SnapshotMetadataService CR.
func smsReconcileRequest() reconcile.Request {
	return reconcile.Request{NamespacedName: k8stypes.NamespacedName{Name: targetSMSName}}
}

// ---------------------------------------------------------------------------
// pure function tests
// ---------------------------------------------------------------------------

// TestShouldProcessCertificate verifies the predicate matcher only accepts
// certificates that match the controller's target name and namespace.
func TestShouldProcessCertificate(t *testing.T) {
	tests := []struct {
		name     string
		cert     *certmanagerv1.Certificate
		expected bool
	}{
		{
			name: "matching name and namespace returns true",
			cert: &certmanagerv1.Certificate{
				ObjectMeta: metav1.ObjectMeta{
					Name:      targetCertificateName,
					Namespace: targetNamespace,
				},
			},
			expected: true,
		},
		{
			name: "different name returns false",
			cert: &certmanagerv1.Certificate{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "some-other-cert",
					Namespace: targetNamespace,
				},
			},
			expected: false,
		},
		{
			name: "different namespace returns false",
			cert: &certmanagerv1.Certificate{
				ObjectMeta: metav1.ObjectMeta{
					Name:      targetCertificateName,
					Namespace: "some-other-namespace",
				},
			},
			expected: false,
		},
		{
			name: "different name and namespace returns false",
			cert: &certmanagerv1.Certificate{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "some-other-cert",
					Namespace: "some-other-namespace",
				},
			},
			expected: false,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			assert.Equal(t, tc.expected, shouldProcessCertificate(tc.cert))
		})
	}
}

// TestIsCertificateReady verifies the helper that checks the certificate's
// readiness condition.
func TestIsCertificateReady(t *testing.T) {
	tests := []struct {
		name     string
		cert     *certmanagerv1.Certificate
		expected bool
	}{
		{
			name: "ready condition true returns true",
			cert: &certmanagerv1.Certificate{
				Status: certmanagerv1.CertificateStatus{
					Conditions: []certmanagerv1.CertificateCondition{
						{Type: certmanagerv1.CertificateConditionReady, Status: cmmeta.ConditionTrue},
					},
				},
			},
			expected: true,
		},
		{
			name: "ready condition false returns false",
			cert: &certmanagerv1.Certificate{
				Status: certmanagerv1.CertificateStatus{
					Conditions: []certmanagerv1.CertificateCondition{
						{Type: certmanagerv1.CertificateConditionReady, Status: cmmeta.ConditionFalse},
					},
				},
			},
			expected: false,
		},
		{
			name: "ready condition unknown returns false",
			cert: &certmanagerv1.Certificate{
				Status: certmanagerv1.CertificateStatus{
					Conditions: []certmanagerv1.CertificateCondition{
						{Type: certmanagerv1.CertificateConditionReady, Status: cmmeta.ConditionUnknown},
					},
				},
			},
			expected: false,
		},
		{
			name: "no conditions returns false",
			cert: &certmanagerv1.Certificate{
				Status: certmanagerv1.CertificateStatus{Conditions: []certmanagerv1.CertificateCondition{}},
			},
			expected: false,
		},
		{
			name: "only non-ready conditions returns false",
			cert: &certmanagerv1.Certificate{
				Status: certmanagerv1.CertificateStatus{
					Conditions: []certmanagerv1.CertificateCondition{
						{Type: certmanagerv1.CertificateConditionIssuing, Status: cmmeta.ConditionTrue},
					},
				},
			},
			expected: false,
		},
		{
			name: "ready condition first match wins",
			cert: &certmanagerv1.Certificate{
				Status: certmanagerv1.CertificateStatus{
					Conditions: []certmanagerv1.CertificateCondition{
						{Type: certmanagerv1.CertificateConditionIssuing, Status: cmmeta.ConditionFalse},
						{Type: certmanagerv1.CertificateConditionReady, Status: cmmeta.ConditionTrue},
					},
				},
			},
			expected: true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			assert.Equal(t, tc.expected, isCertificateReady(tc.cert))
		})
	}
}

// TestSMSServiceLBIP verifies the LoadBalancer-ingress-IP extraction helper.
func TestSMSServiceLBIP(t *testing.T) {
	t.Run("nil service returns empty", func(t *testing.T) {
		assert.Equal(t, "", smsServiceLBIP(nil))
	})
	t.Run("no ingress entries returns empty", func(t *testing.T) {
		assert.Equal(t, "", smsServiceLBIP(makeLBService("")))
	})
	t.Run("ingress IP is returned", func(t *testing.T) {
		assert.Equal(t, "192.168.130.1", smsServiceLBIP(makeLBService("192.168.130.1")))
	})
}

// TestCertPEMCoversIP verifies the x509-based gate used to decide whether the
// served certificate already covers a given LoadBalancer IP.
func TestCertPEMCoversIP(t *testing.T) {
	t.Run("cert with matching IP SAN covers it", func(t *testing.T) {
		pemBytes := generateSelfSignedCertPEM(t, net.ParseIP("192.168.130.1"))
		assert.True(t, certPEMCoversIP(pemBytes, "192.168.130.1"))
	})
	t.Run("cert without the IP SAN does not cover it", func(t *testing.T) {
		pemBytes := generateSelfSignedCertPEM(t, net.ParseIP("10.0.0.1"))
		assert.False(t, certPEMCoversIP(pemBytes, "192.168.130.1"))
	})
	t.Run("cert with no IP SANs does not cover any IP", func(t *testing.T) {
		pemBytes := generateSelfSignedCertPEM(t)
		assert.False(t, certPEMCoversIP(pemBytes, "192.168.130.1"))
	})
	t.Run("garbage PEM bytes do not cover any IP", func(t *testing.T) {
		assert.False(t, certPEMCoversIP([]byte("not a certificate"), "192.168.130.1"))
	})
	t.Run("invalid target IP never matches", func(t *testing.T) {
		pemBytes := generateSelfSignedCertPEM(t, net.ParseIP("192.168.130.1"))
		assert.False(t, certPEMCoversIP(pemBytes, "not-an-ip"))
	})
}

// ---------------------------------------------------------------------------
// recordEvent tests
// ---------------------------------------------------------------------------

func TestRecordEvent(t *testing.T) {
	t.Run("warning event doubles backoff and emits warning", func(t *testing.T) {
		resetBackOffDuration()
		instance := &snapshotmetadatav1beta1.SnapshotMetadataService{
			ObjectMeta: metav1.ObjectMeta{Name: targetSMSName},
		}
		backOffDuration[instance.Name] = 2 * time.Second

		recorder := record.NewFakeRecorder(10)
		r := &ReconcileSnapshotMetadataService{recorder: recorder}

		recordEvent(context.Background(), r, instance, v1.EventTypeWarning, "something failed")

		backOffDurationMapMutex.Lock()
		got := backOffDuration[instance.Name]
		backOffDurationMapMutex.Unlock()
		assert.Equal(t, 4*time.Second, got)

		select {
		case ev := <-recorder.Events:
			assert.Contains(t, ev, "Warning")
			assert.Contains(t, ev, "SnapshotMetadataServiceSyncFailed")
			assert.Contains(t, ev, "something failed")
		case <-time.After(time.Second):
			t.Fatal("expected a warning event to be recorded")
		}
	})

	t.Run("normal event resets backoff and emits normal", func(t *testing.T) {
		resetBackOffDuration()
		instance := &snapshotmetadatav1beta1.SnapshotMetadataService{
			ObjectMeta: metav1.ObjectMeta{Name: targetSMSName},
		}
		backOffDuration[instance.Name] = 16 * time.Second

		recorder := record.NewFakeRecorder(10)
		r := &ReconcileSnapshotMetadataService{recorder: recorder}

		recordEvent(context.Background(), r, instance, v1.EventTypeNormal, "all good")

		backOffDurationMapMutex.Lock()
		got := backOffDuration[instance.Name]
		backOffDurationMapMutex.Unlock()
		assert.Equal(t, time.Second, got)

		select {
		case ev := <-recorder.Events:
			assert.Contains(t, ev, "Normal")
			assert.Contains(t, ev, "SnapshotMetadataServiceSyncSucceeded")
			assert.Contains(t, ev, "all good")
		case <-time.After(time.Second):
			t.Fatal("expected a normal event to be recorded")
		}
	})

	t.Run("unknown event type is a no-op", func(t *testing.T) {
		resetBackOffDuration()
		instance := &snapshotmetadatav1beta1.SnapshotMetadataService{
			ObjectMeta: metav1.ObjectMeta{Name: targetSMSName},
		}
		backOffDuration[instance.Name] = 5 * time.Second

		recorder := record.NewFakeRecorder(10)
		r := &ReconcileSnapshotMetadataService{recorder: recorder}

		recordEvent(context.Background(), r, instance, "SomeUnknownType", "ignored")

		backOffDurationMapMutex.Lock()
		got := backOffDuration[instance.Name]
		backOffDurationMapMutex.Unlock()
		assert.Equal(t, 5*time.Second, got, "backoff should be untouched")

		select {
		case ev := <-recorder.Events:
			t.Fatalf("did not expect any event, got: %s", ev)
		case <-time.After(100 * time.Millisecond):
			// expected: no event recorded
		}
	})
}

// ---------------------------------------------------------------------------
// ensureLeafCoversLBIP unit tests
// ---------------------------------------------------------------------------

func TestEnsureLeafCoversLBIP_AddsMissingIP(t *testing.T) {
	scheme := newTestScheme(t)
	cert := readyCertificate(targetCertificateName, targetNamespace, "sec", 1)

	fakeClient := fake.NewClientBuilder().WithScheme(scheme).WithObjects(cert).Build()
	r := &ReconcileSnapshotMetadataService{client: fakeClient, scheme: scheme}

	added, err := r.ensureLeafCoversLBIP(context.Background(), cert, "192.168.130.1")
	require.NoError(t, err)
	assert.True(t, added)

	updated := &certmanagerv1.Certificate{}
	require.NoError(t, fakeClient.Get(context.Background(),
		k8stypes.NamespacedName{Name: targetCertificateName, Namespace: targetNamespace}, updated))
	assert.Contains(t, updated.Spec.IPAddresses, "192.168.130.1")
}

func TestEnsureLeafCoversLBIP_NoOpWhenAlreadyPresent(t *testing.T) {
	scheme := newTestScheme(t)
	cert := readyCertificate(targetCertificateName, targetNamespace, "sec", 1)
	cert.Spec.IPAddresses = []string{"192.168.130.1"}

	patchCalled := false
	fakeClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(cert).
		WithInterceptorFuncs(interceptor.Funcs{
			Patch: func(ctx context.Context, c client.WithWatch, obj client.Object,
				patch client.Patch, opts ...client.PatchOption) error {
				patchCalled = true
				return c.Patch(ctx, obj, patch, opts...)
			},
		}).
		Build()

	r := &ReconcileSnapshotMetadataService{client: fakeClient, scheme: scheme}
	added, err := r.ensureLeafCoversLBIP(context.Background(), cert, "192.168.130.1")
	require.NoError(t, err)
	assert.False(t, added)
	assert.False(t, patchCalled, "no patch should be issued when IP is already present")
}

// TestEnsureLeafCoversLBIP_ReplacesStaleIP verifies that a stale IP left over
// from a previous LoadBalancer assignment is replaced by the current LB IP,
// rather than accumulated alongside it.
func TestEnsureLeafCoversLBIP_ReplacesStaleIP(t *testing.T) {
	scheme := newTestScheme(t)
	cert := readyCertificate(targetCertificateName, targetNamespace, "sec", 1)
	cert.Spec.IPAddresses = []string{"10.0.0.99"} // stale IP from a previous LB assignment

	fakeClient := fake.NewClientBuilder().WithScheme(scheme).WithObjects(cert).Build()
	r := &ReconcileSnapshotMetadataService{client: fakeClient, scheme: scheme}

	changed, err := r.ensureLeafCoversLBIP(context.Background(), cert, "192.168.130.1")
	require.NoError(t, err)
	assert.True(t, changed)

	updated := &certmanagerv1.Certificate{}
	require.NoError(t, fakeClient.Get(context.Background(),
		k8stypes.NamespacedName{Name: targetCertificateName, Namespace: targetNamespace}, updated))
	assert.Equal(t, []string{"192.168.130.1"}, updated.Spec.IPAddresses,
		"stale IP should be replaced, not accumulated alongside the current LB IP")
}

func TestEnsureLeafCoversLBIP_PatchFailure(t *testing.T) {
	scheme := newTestScheme(t)
	cert := readyCertificate(targetCertificateName, targetNamespace, "sec", 1)

	fakeClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(cert).
		WithInterceptorFuncs(interceptor.Funcs{
			Patch: func(ctx context.Context, c client.WithWatch, obj client.Object,
				patch client.Patch, opts ...client.PatchOption) error {
				return errors.New("patch failed")
			},
		}).
		Build()

	r := &ReconcileSnapshotMetadataService{client: fakeClient, scheme: scheme}
	_, err := r.ensureLeafCoversLBIP(context.Background(), cert, "192.168.130.1")
	assert.Error(t, err)
}

// ---------------------------------------------------------------------------
// Reconcile end-to-end tests
// ---------------------------------------------------------------------------

// TestReconcileSMSNotFound verifies that a missing SnapshotMetadataService CR
// is treated as a no-op (nothing to reconcile).
func TestReconcileSMSNotFound(t *testing.T) {
	resetBackOffDuration()
	scheme := newTestScheme(t)
	fakeClient := fake.NewClientBuilder().WithScheme(scheme).Build()

	r := &ReconcileSnapshotMetadataService{
		client:   fakeClient,
		scheme:   scheme,
		recorder: record.NewFakeRecorder(10),
	}

	res, err := r.Reconcile(context.Background(), smsReconcileRequest())
	assert.NoError(t, err)
	assert.Equal(t, reconcile.Result{}, res)
}

// TestReconcileSMSGetError verifies that an error other than NotFound when
// fetching the SnapshotMetadataService CR is returned to the caller.
func TestReconcileSMSGetError(t *testing.T) {
	resetBackOffDuration()
	scheme := newTestScheme(t)
	expectedErr := errors.New("api server is down")

	fakeClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithInterceptorFuncs(interceptor.Funcs{
			Get: func(ctx context.Context, c client.WithWatch, key client.ObjectKey,
				obj client.Object, opts ...client.GetOption) error {
				if _, ok := obj.(*snapshotmetadatav1beta1.SnapshotMetadataService); ok {
					return expectedErr
				}
				return c.Get(ctx, key, obj, opts...)
			},
		}).
		Build()

	r := &ReconcileSnapshotMetadataService{
		client:   fakeClient,
		scheme:   scheme,
		recorder: record.NewFakeRecorder(10),
	}

	res, err := r.Reconcile(context.Background(), smsReconcileRequest())
	assert.Error(t, err)
	assert.Equal(t, expectedErr, err)
	assert.Equal(t, reconcile.Result{}, res)
}

// TestReconcileCertificateNotFound verifies that the controller requeues when
// the served Certificate does not exist yet.
func TestReconcileCertificateNotFound(t *testing.T) {
	resetBackOffDuration()
	scheme := newTestScheme(t)
	sms := makeSMSCR(nil, "")

	fakeClient := fake.NewClientBuilder().WithScheme(scheme).WithObjects(sms).Build()
	r := &ReconcileSnapshotMetadataService{
		client:   fakeClient,
		scheme:   scheme,
		recorder: record.NewFakeRecorder(10),
	}

	res, err := r.Reconcile(context.Background(), smsReconcileRequest())
	assert.NoError(t, err)
	assert.True(t, res.RequeueAfter > 0)
}

// TestReconcileCertificateGetError verifies that an unexpected Get error on
// the Certificate leads to a requeue and a warning event.
func TestReconcileCertificateGetError(t *testing.T) {
	resetBackOffDuration()
	scheme := newTestScheme(t)
	sms := makeSMSCR(nil, "")

	fakeClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(sms).
		WithInterceptorFuncs(interceptor.Funcs{
			Get: func(ctx context.Context, c client.WithWatch, key client.ObjectKey,
				obj client.Object, opts ...client.GetOption) error {
				if _, ok := obj.(*certmanagerv1.Certificate); ok {
					return errors.New("transient cert get failure")
				}
				return c.Get(ctx, key, obj, opts...)
			},
		}).
		Build()

	recorder := record.NewFakeRecorder(10)
	r := &ReconcileSnapshotMetadataService{client: fakeClient, scheme: scheme, recorder: recorder}

	res, err := r.Reconcile(context.Background(), smsReconcileRequest())
	assert.NoError(t, err)
	assert.True(t, res.RequeueAfter > 0)

	select {
	case ev := <-recorder.Events:
		assert.Contains(t, ev, "Warning")
		assert.Contains(t, ev, "Failed to get Certificate")
	case <-time.After(time.Second):
		t.Fatal("expected a warning event to be recorded")
	}
}

// TestReconcileCertificateNotReady verifies that the controller requeues when
// the Certificate is not yet Ready.
func TestReconcileCertificateNotReady(t *testing.T) {
	resetBackOffDuration()
	scheme := newTestScheme(t)
	sms := makeSMSCR(nil, "")
	cert := notReadyCertificate(targetCertificateName, targetNamespace, "secret-name")

	fakeClient := fake.NewClientBuilder().WithScheme(scheme).WithObjects(sms, cert).Build()
	r := &ReconcileSnapshotMetadataService{
		client:   fakeClient,
		scheme:   scheme,
		recorder: record.NewFakeRecorder(10),
	}

	res, err := r.Reconcile(context.Background(), smsReconcileRequest())
	assert.NoError(t, err)
	assert.True(t, res.RequeueAfter > 0)

	backOffDurationMapMutex.Lock()
	got := backOffDuration[targetSMSName]
	backOffDurationMapMutex.Unlock()
	assert.Equal(t, time.Second, got)
}

// TestReconcileCertificateMissingSecretName verifies controller behavior when
// the Certificate has no secretName defined in its spec.
func TestReconcileCertificateMissingSecretName(t *testing.T) {
	resetBackOffDuration()
	scheme := newTestScheme(t)
	sms := makeSMSCR(nil, "")
	cert := readyCertificate(targetCertificateName, targetNamespace, "", 1)

	fakeClient := fake.NewClientBuilder().WithScheme(scheme).WithObjects(sms, cert).Build()
	recorder := record.NewFakeRecorder(10)
	r := &ReconcileSnapshotMetadataService{client: fakeClient, scheme: scheme, recorder: recorder}

	res, err := r.Reconcile(context.Background(), smsReconcileRequest())
	assert.NoError(t, err)
	assert.True(t, res.RequeueAfter > 0)

	backOffDurationMapMutex.Lock()
	got := backOffDuration[targetSMSName]
	backOffDurationMapMutex.Unlock()
	assert.Equal(t, 2*time.Second, got)

	select {
	case ev := <-recorder.Events:
		assert.Contains(t, ev, "Warning")
		assert.Contains(t, ev, "no secretName")
	case <-time.After(time.Second):
		t.Fatal("expected a warning event to be recorded")
	}
}

// TestReconcileSecretNotFound verifies that a missing Secret leads to a
// requeue and a Warning event.
func TestReconcileSecretNotFound(t *testing.T) {
	resetBackOffDuration()
	scheme := newTestScheme(t)
	sms := makeSMSCR(nil, "")
	cert := readyCertificate(targetCertificateName, targetNamespace, "missing-secret", 1)

	fakeClient := fake.NewClientBuilder().WithScheme(scheme).WithObjects(sms, cert).Build()
	recorder := record.NewFakeRecorder(10)
	r := &ReconcileSnapshotMetadataService{client: fakeClient, scheme: scheme, recorder: recorder}

	res, err := r.Reconcile(context.Background(), smsReconcileRequest())
	assert.NoError(t, err)
	assert.True(t, res.RequeueAfter > 0)

	select {
	case ev := <-recorder.Events:
		assert.Contains(t, ev, "Warning")
		assert.Contains(t, ev, "Secret")
	case <-time.After(time.Second):
		t.Fatal("expected a warning event to be recorded")
	}
}

// TestReconcileSecretGetError verifies that an unexpected Get error on the
// Secret leads to a requeue and a warning event being recorded.
func TestReconcileSecretGetError(t *testing.T) {
	resetBackOffDuration()
	scheme := newTestScheme(t)
	sms := makeSMSCR(nil, "")
	cert := readyCertificate(targetCertificateName, targetNamespace, "some-secret", 1)

	fakeClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(sms, cert).
		WithInterceptorFuncs(interceptor.Funcs{
			Get: func(ctx context.Context, c client.WithWatch, key client.ObjectKey,
				obj client.Object, opts ...client.GetOption) error {
				if _, ok := obj.(*v1.Secret); ok {
					return errors.New("transient secret get failure")
				}
				return c.Get(ctx, key, obj, opts...)
			},
		}).
		Build()

	recorder := record.NewFakeRecorder(10)
	r := &ReconcileSnapshotMetadataService{client: fakeClient, scheme: scheme, recorder: recorder}

	res, err := r.Reconcile(context.Background(), smsReconcileRequest())
	assert.NoError(t, err)
	assert.True(t, res.RequeueAfter > 0)

	select {
	case ev := <-recorder.Events:
		assert.Contains(t, ev, "Warning")
		assert.Contains(t, ev, "Failed to get secret")
	case <-time.After(time.Second):
		t.Fatal("expected a warning event to be recorded")
	}
}

// TestReconcileSecretMissingCAField verifies behavior when the secret has no
// ca.crt field or it is empty.
func TestReconcileSecretMissingCAField(t *testing.T) {
	cases := []struct {
		name string
		data map[string][]byte
	}{
		{name: "missing ca.crt key", data: map[string][]byte{"tls.crt": []byte("garbage")}},
		{name: "empty ca.crt", data: map[string][]byte{"ca.crt": {}}},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			resetBackOffDuration()
			scheme := newTestScheme(t)
			sms := makeSMSCR(nil, "")
			cert := readyCertificate(targetCertificateName, targetNamespace, "ca-secret", 1)
			secret := &v1.Secret{
				ObjectMeta: metav1.ObjectMeta{Name: "ca-secret", Namespace: targetNamespace},
				Data:       tc.data,
			}

			fakeClient := fake.NewClientBuilder().WithScheme(scheme).WithObjects(sms, cert, secret).Build()
			recorder := record.NewFakeRecorder(10)
			r := &ReconcileSnapshotMetadataService{client: fakeClient, scheme: scheme, recorder: recorder}

			res, err := r.Reconcile(context.Background(), smsReconcileRequest())
			assert.NoError(t, err)
			assert.True(t, res.RequeueAfter > 0)

			select {
			case ev := <-recorder.Events:
				assert.Contains(t, ev, "Warning")
				assert.Contains(t, ev, "ca.crt")
			case <-time.After(time.Second):
				t.Fatal("expected a warning event to be recorded")
			}
		})
	}
}

// TestReconcileServiceGetError verifies that an unexpected Get error on the
// SMS Service leads to a requeue.
func TestReconcileServiceGetError(t *testing.T) {
	resetBackOffDuration()
	scheme := newTestScheme(t)
	caBytes := generateSelfSignedCertPEM(t)
	sms := makeSMSCR(caBytes, "")
	cert := readyCertificate(targetCertificateName, targetNamespace, "ca-secret", 1)
	secret := &v1.Secret{
		ObjectMeta: metav1.ObjectMeta{Name: "ca-secret", Namespace: targetNamespace},
		Data:       map[string][]byte{"ca.crt": caBytes},
	}

	fakeClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(sms, cert, secret).
		WithInterceptorFuncs(interceptor.Funcs{
			Get: func(ctx context.Context, c client.WithWatch, key client.ObjectKey,
				obj client.Object, opts ...client.GetOption) error {
				if _, ok := obj.(*v1.Service); ok {
					return errors.New("transient service get failure")
				}
				return c.Get(ctx, key, obj, opts...)
			},
		}).
		Build()

	r := &ReconcileSnapshotMetadataService{client: fakeClient, scheme: scheme, recorder: record.NewFakeRecorder(10)}

	res, err := r.Reconcile(context.Background(), smsReconcileRequest())
	assert.NoError(t, err)
	assert.True(t, res.RequeueAfter > 0)

	// SMS CR must remain untouched.
	updated := &snapshotmetadatav1beta1.SnapshotMetadataService{}
	require.NoError(t, fakeClient.Get(context.Background(), k8stypes.NamespacedName{Name: targetSMSName}, updated))
	assert.Equal(t, caBytes, updated.Spec.CACert)
	assert.Equal(t, "", updated.Spec.Address)
}

// TestReconcileNoLBIP_SyncsCACertOnly verifies that when the LB Service has no
// IP yet, the caCert is still synced (so DNS-based access keeps working) but
// the address and IP SANs are left untouched.
func TestReconcileNoLBIP_SyncsCACertOnly(t *testing.T) {
	resetBackOffDuration()
	scheme := newTestScheme(t)
	newCA := generateSelfSignedCertPEM(t)
	oldCA := []byte("old-ca-bytes")

	sms := makeSMSCR(oldCA, "existing-address:8100")
	cert := readyCertificate(targetCertificateName, targetNamespace, "ca-secret", 1)
	secret := &v1.Secret{
		ObjectMeta: metav1.ObjectMeta{Name: "ca-secret", Namespace: targetNamespace},
		Data:       map[string][]byte{"ca.crt": newCA},
	}
	svc := makeLBService("") // LB IP not assigned yet

	fakeClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(sms, cert, secret, svc).
		WithStatusSubresource(svc).
		Build()

	recorder := record.NewFakeRecorder(10)
	r := &ReconcileSnapshotMetadataService{client: fakeClient, scheme: scheme, recorder: recorder}

	res, err := r.Reconcile(context.Background(), smsReconcileRequest())
	require.NoError(t, err)
	assert.Equal(t, reconcile.Result{}, res)

	updatedSMS := &snapshotmetadatav1beta1.SnapshotMetadataService{}
	require.NoError(t, fakeClient.Get(context.Background(), k8stypes.NamespacedName{Name: targetSMSName}, updatedSMS))
	assert.Equal(t, newCA, updatedSMS.Spec.CACert, "caCert should be synced even without an LB IP")
	assert.Equal(t, "existing-address:8100", updatedSMS.Spec.Address, "address should be untouched without an LB IP")

	updatedCert := &certmanagerv1.Certificate{}
	require.NoError(t, fakeClient.Get(context.Background(),
		k8stypes.NamespacedName{Name: targetCertificateName, Namespace: targetNamespace}, updatedCert))
	assert.Empty(t, updatedCert.Spec.IPAddresses, "no IP SAN should be added without an LB IP")

	select {
	case ev := <-recorder.Events:
		assert.Contains(t, ev, "Normal")
	case <-time.After(time.Second):
		t.Fatal("expected a normal event to be recorded")
	}
}

// TestReconcileLBIPPresent_AddsMissingIPSANAndRequeues verifies that when the
// LB IP is not yet in the Certificate's IP SANs, the controller patches the
// Certificate and requeues without touching the SMS CR (cert-manager has not
// reissued yet, so caCert/address must not be published).
func TestReconcileLBIPPresent_AddsMissingIPSANAndRequeues(t *testing.T) {
	resetBackOffDuration()
	scheme := newTestScheme(t)
	caBytes := generateSelfSignedCertPEM(t) // does not yet cover the LB IP

	sms := makeSMSCR(caBytes, "")
	cert := readyCertificate(targetCertificateName, targetNamespace, "ca-secret", 1)
	secret := &v1.Secret{
		ObjectMeta: metav1.ObjectMeta{Name: "ca-secret", Namespace: targetNamespace},
		Data:       map[string][]byte{"ca.crt": caBytes},
	}
	svc := makeLBServiceWithPort("192.168.130.1", 8100)

	fakeClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(sms, cert, secret, svc).
		WithStatusSubresource(svc).
		Build()

	r := &ReconcileSnapshotMetadataService{client: fakeClient, scheme: scheme, recorder: record.NewFakeRecorder(10)}

	res, err := r.Reconcile(context.Background(), smsReconcileRequest())
	require.NoError(t, err)
	assert.True(t, res.RequeueAfter > 0)

	updatedCert := &certmanagerv1.Certificate{}
	require.NoError(t, fakeClient.Get(context.Background(),
		k8stypes.NamespacedName{Name: targetCertificateName, Namespace: targetNamespace}, updatedCert))
	assert.Contains(t, updatedCert.Spec.IPAddresses, "192.168.130.1")

	// SMS CR must NOT have been touched yet — the served cert has not been
	// reissued to cover the new IP.
	updatedSMS := &snapshotmetadatav1beta1.SnapshotMetadataService{}
	require.NoError(t, fakeClient.Get(context.Background(), k8stypes.NamespacedName{Name: targetSMSName}, updatedSMS))
	assert.Equal(t, caBytes, updatedSMS.Spec.CACert)
	assert.Equal(t, "", updatedSMS.Spec.Address)
}

// TestReconcileLBIPPresent_ReissuePending_Requeues verifies that when the
// Certificate's IP SANs already list the LB IP (added by a previous reconcile)
// but the served ca.crt has not been reissued to cover it yet, the controller
// requeues and does not publish caCert/address.
func TestReconcileLBIPPresent_ReissuePending_Requeues(t *testing.T) {
	resetBackOffDuration()
	scheme := newTestScheme(t)
	staleCA := generateSelfSignedCertPEM(t) // still does not cover the LB IP

	sms := makeSMSCR(staleCA, "")
	cert := readyCertificate(targetCertificateName, targetNamespace, "ca-secret", 2)
	cert.Spec.IPAddresses = []string{"192.168.130.1"} // added, reissue not yet reflected in Secret
	secret := &v1.Secret{
		ObjectMeta: metav1.ObjectMeta{Name: "ca-secret", Namespace: targetNamespace},
		Data:       map[string][]byte{"ca.crt": staleCA},
	}
	svc := makeLBServiceWithPort("192.168.130.1", 8100)

	patchCalled := false
	fakeClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(sms, cert, secret, svc).
		WithStatusSubresource(svc).
		WithInterceptorFuncs(interceptor.Funcs{
			Patch: func(ctx context.Context, c client.WithWatch, obj client.Object,
				patch client.Patch, opts ...client.PatchOption) error {
				if _, ok := obj.(*snapshotmetadatav1beta1.SnapshotMetadataService); ok {
					patchCalled = true
				}
				return c.Patch(ctx, obj, patch, opts...)
			},
		}).
		Build()

	r := &ReconcileSnapshotMetadataService{client: fakeClient, scheme: scheme, recorder: record.NewFakeRecorder(10)}

	res, err := r.Reconcile(context.Background(), smsReconcileRequest())
	require.NoError(t, err)
	assert.True(t, res.RequeueAfter > 0)
	assert.False(t, patchCalled, "SMS CR must not be patched while the served cert does not cover the LB IP")
}

// TestReconcileLBIPPresent_NoServicePorts_Requeues verifies that a Service
// with no ports (address cannot be computed) causes a requeue without
// publishing to the SMS CR, even though the certificate already covers the IP.
func TestReconcileLBIPPresent_NoServicePorts_Requeues(t *testing.T) {
	resetBackOffDuration()
	scheme := newTestScheme(t)
	caBytes := generateSelfSignedCertPEM(t, net.ParseIP("192.168.130.1"))

	sms := makeSMSCR(nil, "")
	cert := readyCertificate(targetCertificateName, targetNamespace, "ca-secret", 2)
	cert.Spec.IPAddresses = []string{"192.168.130.1"}
	secret := &v1.Secret{
		ObjectMeta: metav1.ObjectMeta{Name: "ca-secret", Namespace: targetNamespace},
		Data:       map[string][]byte{"ca.crt": caBytes},
	}
	svc := &v1.Service{
		ObjectMeta: metav1.ObjectMeta{Name: targetServiceName, Namespace: targetNamespace},
		Status: v1.ServiceStatus{
			LoadBalancer: v1.LoadBalancerStatus{Ingress: []v1.LoadBalancerIngress{{IP: "192.168.130.1"}}},
		},
	}

	fakeClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(sms, cert, secret, svc).
		WithStatusSubresource(svc).
		Build()

	r := &ReconcileSnapshotMetadataService{client: fakeClient, scheme: scheme, recorder: record.NewFakeRecorder(10)}

	res, err := r.Reconcile(context.Background(), smsReconcileRequest())
	require.NoError(t, err)
	assert.True(t, res.RequeueAfter > 0)

	updatedSMS := &snapshotmetadatav1beta1.SnapshotMetadataService{}
	require.NoError(t, fakeClient.Get(context.Background(), k8stypes.NamespacedName{Name: targetSMSName}, updatedSMS))
	assert.Equal(t, "", updatedSMS.Spec.Address)
}

// TestReconcileLBIPPresent_FullySynced verifies the end state: once the
// Certificate's IP SANs and the served ca.crt both cover the LB IP, the
// controller publishes caCert and address together in a single patch.
func TestReconcileLBIPPresent_FullySynced(t *testing.T) {
	resetBackOffDuration()
	scheme := newTestScheme(t)
	newCA := generateSelfSignedCertPEM(t, net.ParseIP("192.168.130.1"))
	oldCA := []byte("old-ca-bytes")

	sms := makeSMSCR(oldCA, "old-dns-name.svc.cluster.local:8100")
	cert := readyCertificate(targetCertificateName, targetNamespace, "ca-secret", 2)
	cert.Spec.IPAddresses = []string{"192.168.130.1"} // already added by a previous reconcile
	secret := &v1.Secret{
		ObjectMeta: metav1.ObjectMeta{Name: "ca-secret", Namespace: targetNamespace},
		Data:       map[string][]byte{"ca.crt": newCA},
	}
	svc := makeLBServiceWithPort("192.168.130.1", 8100)

	fakeClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(sms, cert, secret, svc).
		WithStatusSubresource(svc).
		Build()

	recorder := record.NewFakeRecorder(10)
	r := &ReconcileSnapshotMetadataService{client: fakeClient, scheme: scheme, recorder: recorder}

	res, err := r.Reconcile(context.Background(), smsReconcileRequest())
	require.NoError(t, err)
	assert.Equal(t, reconcile.Result{}, res)

	updatedSMS := &snapshotmetadatav1beta1.SnapshotMetadataService{}
	require.NoError(t, fakeClient.Get(context.Background(), k8stypes.NamespacedName{Name: targetSMSName}, updatedSMS))
	assert.Equal(t, newCA, updatedSMS.Spec.CACert)
	assert.Equal(t, "192.168.130.1:8100", updatedSMS.Spec.Address)

	backOffDurationMapMutex.Lock()
	_, exists := backOffDuration[targetSMSName]
	backOffDurationMapMutex.Unlock()
	assert.False(t, exists, "expected backoff entry to be deleted on success")

	select {
	case ev := <-recorder.Events:
		assert.Contains(t, ev, "Normal")
		assert.Contains(t, ev, "Successfully synced")
	case <-time.After(time.Second):
		t.Fatal("expected a normal event to be recorded")
	}
}

// TestReconcileAlreadyUpToDate_NoOp verifies that no patch is performed when
// caCert and address are already correct.
func TestReconcileAlreadyUpToDate_NoOp(t *testing.T) {
	resetBackOffDuration()
	scheme := newTestScheme(t)
	caBytes := generateSelfSignedCertPEM(t, net.ParseIP("192.168.130.1"))

	sms := makeSMSCR(caBytes, "192.168.130.1:8100")
	cert := readyCertificate(targetCertificateName, targetNamespace, "ca-secret", 2)
	cert.Spec.IPAddresses = []string{"192.168.130.1"}
	secret := &v1.Secret{
		ObjectMeta: metav1.ObjectMeta{Name: "ca-secret", Namespace: targetNamespace},
		Data:       map[string][]byte{"ca.crt": caBytes},
	}
	svc := makeLBServiceWithPort("192.168.130.1", 8100)

	patchCalled := false
	fakeClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(sms, cert, secret, svc).
		WithStatusSubresource(svc).
		WithInterceptorFuncs(interceptor.Funcs{
			Patch: func(ctx context.Context, c client.WithWatch, obj client.Object,
				patch client.Patch, opts ...client.PatchOption) error {
				if _, ok := obj.(*snapshotmetadatav1beta1.SnapshotMetadataService); ok {
					patchCalled = true
				}
				return c.Patch(ctx, obj, patch, opts...)
			},
		}).
		Build()

	recorder := record.NewFakeRecorder(10)
	r := &ReconcileSnapshotMetadataService{client: fakeClient, scheme: scheme, recorder: recorder}

	// Pre-seed a backoff entry to verify it is cleaned up.
	backOffDurationMapMutex.Lock()
	backOffDuration[targetSMSName] = 8 * time.Second
	backOffDurationMapMutex.Unlock()

	res, err := r.Reconcile(context.Background(), smsReconcileRequest())
	require.NoError(t, err)
	assert.Equal(t, reconcile.Result{}, res)
	assert.False(t, patchCalled, "no patch should be issued when caCert and address are already correct")

	backOffDurationMapMutex.Lock()
	_, exists := backOffDuration[targetSMSName]
	backOffDurationMapMutex.Unlock()
	assert.False(t, exists)

	// The up-to-date, no-op branch only logs; it does not record an event.
	select {
	case ev := <-recorder.Events:
		t.Fatalf("did not expect any event, got: %s", ev)
	case <-time.After(100 * time.Millisecond):
		// expected: no event recorded
	}
}

// TestReconcileSMSPatchFailure verifies that a failed SMS CR patch leads to a
// requeue and a warning event, with the backoff doubled.
func TestReconcileSMSPatchFailure(t *testing.T) {
	resetBackOffDuration()
	scheme := newTestScheme(t)
	newCA := generateSelfSignedCertPEM(t)
	oldCA := []byte("old-ca-bytes")

	sms := makeSMSCR(oldCA, "")
	cert := readyCertificate(targetCertificateName, targetNamespace, "ca-secret", 3)
	secret := &v1.Secret{
		ObjectMeta: metav1.ObjectMeta{Name: "ca-secret", Namespace: targetNamespace},
		Data:       map[string][]byte{"ca.crt": newCA},
	}
	svc := makeLBService("") // no LB IP -> exercise the caCert-only publish path

	fakeClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(sms, cert, secret, svc).
		WithStatusSubresource(svc).
		WithInterceptorFuncs(interceptor.Funcs{
			Patch: func(ctx context.Context, c client.WithWatch, obj client.Object,
				patch client.Patch, opts ...client.PatchOption) error {
				if _, ok := obj.(*snapshotmetadatav1beta1.SnapshotMetadataService); ok {
					return errors.New("patch failed")
				}
				return c.Patch(ctx, obj, patch, opts...)
			},
		}).
		Build()

	recorder := record.NewFakeRecorder(10)
	r := &ReconcileSnapshotMetadataService{client: fakeClient, scheme: scheme, recorder: recorder}

	res, err := r.Reconcile(context.Background(), smsReconcileRequest())
	assert.NoError(t, err)
	assert.True(t, res.RequeueAfter > 0)

	// Verify no patch was actually applied.
	updatedSMS := &snapshotmetadatav1beta1.SnapshotMetadataService{}
	require.NoError(t, fakeClient.Get(context.Background(), k8stypes.NamespacedName{Name: targetSMSName}, updatedSMS))
	assert.Equal(t, oldCA, updatedSMS.Spec.CACert)

	backOffDurationMapMutex.Lock()
	got := backOffDuration[targetSMSName]
	backOffDurationMapMutex.Unlock()
	assert.Equal(t, 2*time.Second, got)

	select {
	case ev := <-recorder.Events:
		assert.Contains(t, ev, "Warning")
		assert.Contains(t, ev, "Failed to patch")
	case <-time.After(time.Second):
		t.Fatal("expected a warning event to be recorded")
	}
}

// TestReconcileBackoffInitialization verifies that the backoff duration map
// is lazily initialized for new instances.
func TestReconcileBackoffInitialization(t *testing.T) {
	resetBackOffDuration()
	scheme := newTestScheme(t)
	sms := makeSMSCR(nil, "")
	cert := notReadyCertificate(targetCertificateName, targetNamespace, "ca-secret")

	fakeClient := fake.NewClientBuilder().WithScheme(scheme).WithObjects(sms, cert).Build()
	r := &ReconcileSnapshotMetadataService{
		client:   fakeClient,
		scheme:   scheme,
		recorder: record.NewFakeRecorder(10),
	}

	backOffDurationMapMutex.Lock()
	_, existsBefore := backOffDuration[targetSMSName]
	backOffDurationMapMutex.Unlock()
	assert.False(t, existsBefore)

	_, err := r.Reconcile(context.Background(), smsReconcileRequest())
	assert.NoError(t, err)

	backOffDurationMapMutex.Lock()
	got, existsAfter := backOffDuration[targetSMSName]
	backOffDurationMapMutex.Unlock()
	assert.True(t, existsAfter, "backoff entry should be created after reconcile")
	assert.Equal(t, time.Second, got)
}

// ---------------------------------------------------------------------------
// Add() / FSS gating tests
// ---------------------------------------------------------------------------

// TestAddSkipsInitForNonWorkloadFlavor verifies that Add returns immediately
// without consulting the CO interface or initializing any resources when the
// cluster flavor is not Workload (WCP).
func TestAddSkipsInitForNonWorkloadFlavor(t *testing.T) {
	restore := withInjectedCO(nil, errors.New("CO factory must not be called for non-WCP flavor"))
	defer restore()

	flavors := []cnstypes.CnsClusterFlavor{
		cnstypes.CnsClusterFlavorVanilla,
		cnstypes.CnsClusterFlavorGuest,
	}
	for _, flavor := range flavors {
		flavor := flavor
		t.Run(string(flavor), func(t *testing.T) {
			err := Add(nil, flavor)
			assert.NoError(t, err, "Add should be a no-op for non-WCP flavors")
		})
	}
}

// TestAddSkipsInitWhenCSIBackupAPIFSSDisabled verifies the controller is not
// initialized (i.e., Add returns nil and never reaches manager-related setup)
// when the CSI_Backup_API capability/FSS is disabled on the WCP cluster.
func TestAddSkipsInitWhenCSIBackupAPIFSSDisabled(t *testing.T) {
	fakeCO, err := unittestcommon.GetFakeContainerOrchestratorInterface(common.Kubernetes)
	require.NoError(t, err)

	require.NoError(t, fakeCO.DisableFSS(context.Background(), common.CSI_Backup_API))
	require.False(t, fakeCO.IsFSSEnabled(context.Background(), common.CSI_Backup_API),
		"precondition: CSI_Backup_API FSS must be disabled in the fake CO")

	tracker := &fssCallTracker{COCommonInterface: fakeCO}
	restore := withInjectedCO(tracker, nil)
	defer restore()

	// A nil manager is intentional: Add should bail out before any manager
	// method is invoked. A panic here would be a strong regression signal.
	err = Add(nil, cnstypes.CnsClusterFlavorWorkload)
	assert.NoError(t, err, "Add should return nil when CSI_Backup_API FSS is disabled")

	assert.Equal(t, []string{common.CSI_Backup_API}, tracker.fssQueries,
		"Add should query IsFSSEnabled exactly once for CSI_Backup_API")
}

// TestAddPropagatesCOInterfaceError verifies that Add returns the error from
// the CO interface factory (and does not initialize the controller) when CO
// initialization fails on the WCP cluster.
func TestAddPropagatesCOInterfaceError(t *testing.T) {
	expectedErr := errors.New("co init failure")
	restore := withInjectedCO(nil, expectedErr)
	defer restore()

	err := Add(nil, cnstypes.CnsClusterFlavorWorkload)
	assert.ErrorIs(t, err, expectedErr,
		"Add should propagate the CO factory error and not initialize the controller")
}
