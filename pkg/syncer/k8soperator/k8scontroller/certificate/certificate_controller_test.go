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

package certificate

import (
	"context"
	"errors"
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
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
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

// newTestScheme creates a runtime scheme with all types required for the
// certificate controller tests.
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
						{
							Type:   certmanagerv1.CertificateConditionReady,
							Status: cmmeta.ConditionTrue,
						},
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
						{
							Type:   certmanagerv1.CertificateConditionReady,
							Status: cmmeta.ConditionFalse,
						},
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
						{
							Type:   certmanagerv1.CertificateConditionReady,
							Status: cmmeta.ConditionUnknown,
						},
					},
				},
			},
			expected: false,
		},
		{
			name: "no conditions returns false",
			cert: &certmanagerv1.Certificate{
				Status: certmanagerv1.CertificateStatus{
					Conditions: []certmanagerv1.CertificateCondition{},
				},
			},
			expected: false,
		},
		{
			name: "only non-ready conditions returns false",
			cert: &certmanagerv1.Certificate{
				Status: certmanagerv1.CertificateStatus{
					Conditions: []certmanagerv1.CertificateCondition{
						{
							Type:   certmanagerv1.CertificateConditionIssuing,
							Status: cmmeta.ConditionTrue,
						},
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
						{
							Type:   certmanagerv1.CertificateConditionIssuing,
							Status: cmmeta.ConditionFalse,
						},
						{
							Type:   certmanagerv1.CertificateConditionReady,
							Status: cmmeta.ConditionTrue,
						},
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

// TestRecordEvent verifies that the helper updates backOffDuration correctly
// based on event type and emits the expected events to the recorder.
func TestRecordEvent(t *testing.T) {
	t.Run("warning event doubles backoff and emits warning", func(t *testing.T) {
		resetBackOffDuration()
		instance := &certmanagerv1.Certificate{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "cert-1",
				Namespace: "ns-1",
			},
		}
		backOffDuration[instance.Name] = 2 * time.Second

		recorder := record.NewFakeRecorder(10)
		r := &ReconcileCertificate{recorder: recorder}

		recordEvent(context.Background(), r, instance, v1.EventTypeWarning, "something failed")

		// backoff should have doubled.
		backOffDurationMapMutex.Lock()
		got := backOffDuration[instance.Name]
		backOffDurationMapMutex.Unlock()
		assert.Equal(t, 4*time.Second, got)

		select {
		case ev := <-recorder.Events:
			assert.Contains(t, ev, "Warning")
			assert.Contains(t, ev, "CertificateSyncFailed")
			assert.Contains(t, ev, "something failed")
		case <-time.After(time.Second):
			t.Fatal("expected a warning event to be recorded")
		}
	})

	t.Run("normal event resets backoff and emits normal", func(t *testing.T) {
		resetBackOffDuration()
		instance := &certmanagerv1.Certificate{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "cert-2",
				Namespace: "ns-1",
			},
		}
		backOffDuration[instance.Name] = 16 * time.Second

		recorder := record.NewFakeRecorder(10)
		r := &ReconcileCertificate{recorder: recorder}

		recordEvent(context.Background(), r, instance, v1.EventTypeNormal, "all good")

		backOffDurationMapMutex.Lock()
		got := backOffDuration[instance.Name]
		backOffDurationMapMutex.Unlock()
		assert.Equal(t, time.Second, got)

		select {
		case ev := <-recorder.Events:
			assert.Contains(t, ev, "Normal")
			assert.Contains(t, ev, "CertificateSyncSucceeded")
			assert.Contains(t, ev, "all good")
		case <-time.After(time.Second):
			t.Fatal("expected a normal event to be recorded")
		}
	})

	t.Run("unknown event type is a no-op", func(t *testing.T) {
		resetBackOffDuration()
		instance := &certmanagerv1.Certificate{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "cert-3",
				Namespace: "ns-1",
			},
		}
		backOffDuration[instance.Name] = 5 * time.Second

		recorder := record.NewFakeRecorder(10)
		r := &ReconcileCertificate{recorder: recorder}

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

// TestReconcileCertificateNotFound verifies that a missing Certificate is
// treated as a no-op.
func TestReconcileCertificateNotFound(t *testing.T) {
	resetBackOffDuration()
	scheme := newTestScheme(t)
	fakeClient := fake.NewClientBuilder().WithScheme(scheme).Build()

	r := &ReconcileCertificate{
		client:   fakeClient,
		scheme:   scheme,
		recorder: record.NewFakeRecorder(10),
	}

	req := reconcile.Request{NamespacedName: k8stypes.NamespacedName{
		Name:      targetCertificateName,
		Namespace: targetNamespace,
	}}

	res, err := r.Reconcile(context.Background(), req)
	assert.NoError(t, err)
	assert.Equal(t, reconcile.Result{}, res)
}

// TestReconcileCertificateGetError verifies that an error other than NotFound
// when fetching the Certificate is returned to the caller.
func TestReconcileCertificateGetError(t *testing.T) {
	resetBackOffDuration()
	scheme := newTestScheme(t)
	expectedErr := errors.New("api server is down")

	fakeClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithInterceptorFuncs(interceptor.Funcs{
			Get: func(ctx context.Context, c client.WithWatch, key client.ObjectKey,
				obj client.Object, opts ...client.GetOption) error {
				if _, ok := obj.(*certmanagerv1.Certificate); ok {
					return expectedErr
				}
				return c.Get(ctx, key, obj, opts...)
			},
		}).
		Build()

	r := &ReconcileCertificate{
		client:   fakeClient,
		scheme:   scheme,
		recorder: record.NewFakeRecorder(10),
	}

	req := reconcile.Request{NamespacedName: k8stypes.NamespacedName{
		Name:      targetCertificateName,
		Namespace: targetNamespace,
	}}

	res, err := r.Reconcile(context.Background(), req)
	assert.Error(t, err)
	assert.Equal(t, expectedErr, err)
	assert.Equal(t, reconcile.Result{}, res)
}

// TestReconcileCertificateNotReady verifies that the controller requeues when
// the Certificate is not yet Ready.
func TestReconcileCertificateNotReady(t *testing.T) {
	resetBackOffDuration()
	scheme := newTestScheme(t)
	cert := notReadyCertificate(targetCertificateName, targetNamespace, "secret-name")

	fakeClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(cert).
		Build()

	r := &ReconcileCertificate{
		client:   fakeClient,
		scheme:   scheme,
		recorder: record.NewFakeRecorder(10),
	}

	req := reconcile.Request{NamespacedName: k8stypes.NamespacedName{
		Name:      cert.Name,
		Namespace: cert.Namespace,
	}}

	res, err := r.Reconcile(context.Background(), req)
	assert.NoError(t, err)
	// Should be requeued using the (initialized) backoff duration.
	assert.True(t, res.RequeueAfter > 0)

	// backoff entry should have been seeded to time.Second.
	backOffDurationMapMutex.Lock()
	got := backOffDuration[cert.Name]
	backOffDurationMapMutex.Unlock()
	assert.Equal(t, time.Second, got)
}

// TestReconcileCertificateMissingSecretName verifies controller behavior when
// the Certificate has no secretName defined in its spec.
func TestReconcileCertificateMissingSecretName(t *testing.T) {
	resetBackOffDuration()
	scheme := newTestScheme(t)
	cert := readyCertificate(targetCertificateName, targetNamespace, "", 1)

	fakeClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(cert).
		Build()

	recorder := record.NewFakeRecorder(10)
	r := &ReconcileCertificate{
		client:   fakeClient,
		scheme:   scheme,
		recorder: recorder,
	}

	req := reconcile.Request{NamespacedName: k8stypes.NamespacedName{
		Name:      cert.Name,
		Namespace: cert.Namespace,
	}}

	res, err := r.Reconcile(context.Background(), req)
	assert.NoError(t, err)
	assert.True(t, res.RequeueAfter > 0)

	// A warning event should have been recorded which doubles the backoff.
	backOffDurationMapMutex.Lock()
	got := backOffDuration[cert.Name]
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
	cert := readyCertificate(targetCertificateName, targetNamespace, "missing-secret", 1)

	fakeClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(cert).
		Build()

	recorder := record.NewFakeRecorder(10)
	r := &ReconcileCertificate{
		client:   fakeClient,
		scheme:   scheme,
		recorder: recorder,
	}

	req := reconcile.Request{NamespacedName: k8stypes.NamespacedName{
		Name:      cert.Name,
		Namespace: cert.Namespace,
	}}

	res, err := r.Reconcile(context.Background(), req)
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
	cert := readyCertificate(targetCertificateName, targetNamespace, "some-secret", 1)

	fakeClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(cert).
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
	r := &ReconcileCertificate{
		client:   fakeClient,
		scheme:   scheme,
		recorder: recorder,
	}

	req := reconcile.Request{NamespacedName: k8stypes.NamespacedName{
		Name:      cert.Name,
		Namespace: cert.Namespace,
	}}

	res, err := r.Reconcile(context.Background(), req)
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
		{
			name: "missing ca.crt key",
			data: map[string][]byte{"tls.crt": []byte("garbage")},
		},
		{
			name: "empty ca.crt",
			data: map[string][]byte{"ca.crt": {}},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			resetBackOffDuration()
			scheme := newTestScheme(t)
			cert := readyCertificate(targetCertificateName, targetNamespace, "ca-secret", 1)
			secret := &v1.Secret{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "ca-secret",
					Namespace: targetNamespace,
				},
				Data: tc.data,
			}

			fakeClient := fake.NewClientBuilder().
				WithScheme(scheme).
				WithObjects(cert, secret).
				Build()

			recorder := record.NewFakeRecorder(10)
			r := &ReconcileCertificate{
				client:   fakeClient,
				scheme:   scheme,
				recorder: recorder,
			}

			req := reconcile.Request{NamespacedName: k8stypes.NamespacedName{
				Name:      cert.Name,
				Namespace: cert.Namespace,
			}}

			res, err := r.Reconcile(context.Background(), req)
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

// TestReconcileSnapshotMetadataServiceNotFound verifies that the controller
// gracefully exits without requeue when the SnapshotMetadataService CR is
// absent.
func TestReconcileSnapshotMetadataServiceNotFound(t *testing.T) {
	resetBackOffDuration()
	scheme := newTestScheme(t)
	cert := readyCertificate(targetCertificateName, targetNamespace, "ca-secret", 1)
	secret := &v1.Secret{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "ca-secret",
			Namespace: targetNamespace,
		},
		Data: map[string][]byte{"ca.crt": []byte("some-ca-data")},
	}

	fakeClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(cert, secret).
		Build()

	r := &ReconcileCertificate{
		client:   fakeClient,
		scheme:   scheme,
		recorder: record.NewFakeRecorder(10),
	}

	req := reconcile.Request{NamespacedName: k8stypes.NamespacedName{
		Name:      cert.Name,
		Namespace: cert.Namespace,
	}}

	res, err := r.Reconcile(context.Background(), req)
	assert.NoError(t, err)
	assert.Equal(t, reconcile.Result{}, res, "should not requeue when SMS CR is missing")
}

// TestReconcileSnapshotMetadataServiceGetError verifies that an unexpected
// Get error on the SMS CR is handled by requeueing and emitting a warning.
func TestReconcileSnapshotMetadataServiceGetError(t *testing.T) {
	resetBackOffDuration()
	scheme := newTestScheme(t)
	cert := readyCertificate(targetCertificateName, targetNamespace, "ca-secret", 1)
	secret := &v1.Secret{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "ca-secret",
			Namespace: targetNamespace,
		},
		Data: map[string][]byte{"ca.crt": []byte("some-ca-data")},
	}

	fakeClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(cert, secret).
		WithInterceptorFuncs(interceptor.Funcs{
			Get: func(ctx context.Context, c client.WithWatch, key client.ObjectKey,
				obj client.Object, opts ...client.GetOption) error {
				if _, ok := obj.(*snapshotmetadatav1beta1.SnapshotMetadataService); ok {
					return errors.New("sms api error")
				}
				return c.Get(ctx, key, obj, opts...)
			},
		}).
		Build()

	recorder := record.NewFakeRecorder(10)
	r := &ReconcileCertificate{
		client:   fakeClient,
		scheme:   scheme,
		recorder: recorder,
	}

	req := reconcile.Request{NamespacedName: k8stypes.NamespacedName{
		Name:      cert.Name,
		Namespace: cert.Namespace,
	}}

	res, err := r.Reconcile(context.Background(), req)
	assert.NoError(t, err)
	assert.True(t, res.RequeueAfter > 0)

	select {
	case ev := <-recorder.Events:
		assert.Contains(t, ev, "Warning")
		assert.Contains(t, ev, "SnapshotMetadataService")
	case <-time.After(time.Second):
		t.Fatal("expected a warning event to be recorded")
	}
}

// TestReconcileCAUpToDate verifies that no patch is performed when the CA
// certificate is already up-to-date in the SnapshotMetadataService CR.
func TestReconcileCAUpToDate(t *testing.T) {
	resetBackOffDuration()
	scheme := newTestScheme(t)
	caBytes := []byte("up-to-date-ca")

	cert := readyCertificate(targetCertificateName, targetNamespace, "ca-secret", 1)
	secret := &v1.Secret{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "ca-secret",
			Namespace: targetNamespace,
		},
		Data: map[string][]byte{"ca.crt": caBytes},
	}
	sms := &snapshotmetadatav1beta1.SnapshotMetadataService{
		ObjectMeta: metav1.ObjectMeta{
			Name: targetSMSName,
		},
		Spec: snapshotmetadatav1beta1.SnapshotMetadataServiceSpec{
			CACert: caBytes,
		},
	}
	// Pre-seed an entry in the backoff map so we can verify it gets cleaned up.
	resetBackOffDuration()
	backOffDurationMapMutex.Lock()
	backOffDuration[cert.Name] = 8 * time.Second
	backOffDurationMapMutex.Unlock()

	fakeClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(cert, secret, sms).
		Build()

	recorder := record.NewFakeRecorder(10)
	r := &ReconcileCertificate{
		client:   fakeClient,
		scheme:   scheme,
		recorder: recorder,
	}

	req := reconcile.Request{NamespacedName: k8stypes.NamespacedName{
		Name:      cert.Name,
		Namespace: cert.Namespace,
	}}

	res, err := r.Reconcile(context.Background(), req)
	assert.NoError(t, err)
	assert.Equal(t, reconcile.Result{}, res)

	// SMS CR's CA cert remains unchanged.
	updatedSMS := &snapshotmetadatav1beta1.SnapshotMetadataService{}
	require.NoError(t, fakeClient.Get(context.Background(),
		k8stypes.NamespacedName{Name: targetSMSName}, updatedSMS))
	assert.Equal(t, caBytes, updatedSMS.Spec.CACert)

	// backoff entry is cleaned up.
	backOffDurationMapMutex.Lock()
	_, exists := backOffDuration[cert.Name]
	backOffDurationMapMutex.Unlock()
	assert.False(t, exists, "expected backoff entry to be deleted on success")

	select {
	case ev := <-recorder.Events:
		assert.Contains(t, ev, "Normal")
		assert.Contains(t, ev, "up-to-date")
	case <-time.After(time.Second):
		t.Fatal("expected a normal event to be recorded")
	}
}

// TestReconcileCAPatchSuccess verifies that the SMS CR is patched with the
// new CA bytes when they differ from the current SMS state.
func TestReconcileCAPatchSuccess(t *testing.T) {
	resetBackOffDuration()
	scheme := newTestScheme(t)

	newCA := []byte("new-ca-bytes")
	oldCA := []byte("old-ca-bytes")

	cert := readyCertificate(targetCertificateName, targetNamespace, "ca-secret", 5)
	secret := &v1.Secret{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "ca-secret",
			Namespace: targetNamespace,
		},
		Data: map[string][]byte{"ca.crt": newCA},
	}
	sms := &snapshotmetadatav1beta1.SnapshotMetadataService{
		ObjectMeta: metav1.ObjectMeta{
			Name: targetSMSName,
		},
		Spec: snapshotmetadatav1beta1.SnapshotMetadataServiceSpec{
			CACert: oldCA,
		},
	}

	fakeClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(cert, secret, sms).
		Build()

	recorder := record.NewFakeRecorder(10)
	r := &ReconcileCertificate{
		client:   fakeClient,
		scheme:   scheme,
		recorder: recorder,
	}

	req := reconcile.Request{NamespacedName: k8stypes.NamespacedName{
		Name:      cert.Name,
		Namespace: cert.Namespace,
	}}

	res, err := r.Reconcile(context.Background(), req)
	assert.NoError(t, err)
	assert.Equal(t, reconcile.Result{}, res)

	updatedSMS := &snapshotmetadatav1beta1.SnapshotMetadataService{}
	require.NoError(t, fakeClient.Get(context.Background(),
		k8stypes.NamespacedName{Name: targetSMSName}, updatedSMS))
	assert.Equal(t, newCA, updatedSMS.Spec.CACert)

	// backoff entry should be removed on success.
	backOffDurationMapMutex.Lock()
	_, exists := backOffDuration[cert.Name]
	backOffDurationMapMutex.Unlock()
	assert.False(t, exists, "expected backoff entry to be deleted on success")

	select {
	case ev := <-recorder.Events:
		assert.Contains(t, ev, "Normal")
		assert.Contains(t, ev, "Successfully patched")
	case <-time.After(time.Second):
		t.Fatal("expected a normal event to be recorded")
	}
}

// TestReconcileCAPatchFailure verifies that a failed patch leads to a
// requeue and a warning event.
func TestReconcileCAPatchFailure(t *testing.T) {
	resetBackOffDuration()
	scheme := newTestScheme(t)

	newCA := []byte("new-ca-bytes")
	oldCA := []byte("old-ca-bytes")

	cert := readyCertificate(targetCertificateName, targetNamespace, "ca-secret", 7)
	secret := &v1.Secret{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "ca-secret",
			Namespace: targetNamespace,
		},
		Data: map[string][]byte{"ca.crt": newCA},
	}
	sms := &snapshotmetadatav1beta1.SnapshotMetadataService{
		ObjectMeta: metav1.ObjectMeta{
			Name: targetSMSName,
		},
		Spec: snapshotmetadatav1beta1.SnapshotMetadataServiceSpec{
			CACert: oldCA,
		},
	}

	fakeClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(cert, secret, sms).
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
	r := &ReconcileCertificate{
		client:   fakeClient,
		scheme:   scheme,
		recorder: recorder,
	}

	req := reconcile.Request{NamespacedName: k8stypes.NamespacedName{
		Name:      cert.Name,
		Namespace: cert.Namespace,
	}}

	res, err := r.Reconcile(context.Background(), req)
	assert.NoError(t, err)
	assert.True(t, res.RequeueAfter > 0)

	// Verify that no patch was applied (because we intercepted the call).
	updatedSMS := &snapshotmetadatav1beta1.SnapshotMetadataService{}
	require.NoError(t, fakeClient.Get(context.Background(),
		k8stypes.NamespacedName{Name: targetSMSName}, updatedSMS))
	assert.Equal(t, oldCA, updatedSMS.Spec.CACert)

	// backoff entry should have been doubled by the warning event.
	backOffDurationMapMutex.Lock()
	got := backOffDuration[cert.Name]
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
	cert := notReadyCertificate(targetCertificateName, targetNamespace, "ca-secret")

	fakeClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(cert).
		Build()

	r := &ReconcileCertificate{
		client:   fakeClient,
		scheme:   scheme,
		recorder: record.NewFakeRecorder(10),
	}

	// Confirm map starts empty.
	backOffDurationMapMutex.Lock()
	_, existsBefore := backOffDuration[cert.Name]
	backOffDurationMapMutex.Unlock()
	assert.False(t, existsBefore)

	req := reconcile.Request{NamespacedName: k8stypes.NamespacedName{
		Name:      cert.Name,
		Namespace: cert.Namespace,
	}}

	_, err := r.Reconcile(context.Background(), req)
	assert.NoError(t, err)

	backOffDurationMapMutex.Lock()
	got, existsAfter := backOffDuration[cert.Name]
	backOffDurationMapMutex.Unlock()
	assert.True(t, existsAfter, "backoff entry should be created after reconcile")
	assert.Equal(t, time.Second, got)
}

// TestReconcileNotFoundIsApiError verifies that the apierrors helper used by
// the controller treats an injected NotFound error correctly. This guards
// against accidental regressions in the NotFound branch of Reconcile.
func TestReconcileCertificateNotFoundViaInterceptor(t *testing.T) {
	resetBackOffDuration()
	scheme := newTestScheme(t)

	fakeClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithInterceptorFuncs(interceptor.Funcs{
			Get: func(ctx context.Context, c client.WithWatch, key client.ObjectKey,
				obj client.Object, opts ...client.GetOption) error {
				if _, ok := obj.(*certmanagerv1.Certificate); ok {
					return apierrors.NewNotFound(
						schema.GroupResource{
							Group:    certmanagerv1.SchemeGroupVersion.Group,
							Resource: "certificates",
						},
						key.Name)
				}
				return c.Get(ctx, key, obj, opts...)
			},
		}).
		Build()

	r := &ReconcileCertificate{
		client:   fakeClient,
		scheme:   scheme,
		recorder: record.NewFakeRecorder(10),
	}

	req := reconcile.Request{NamespacedName: k8stypes.NamespacedName{
		Name:      targetCertificateName,
		Namespace: targetNamespace,
	}}

	res, err := r.Reconcile(context.Background(), req)
	assert.NoError(t, err)
	assert.Equal(t, reconcile.Result{}, res)
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

// TestAddSkipsInitForNonWorkloadFlavor verifies that Add returns immediately
// without consulting the CO interface or initializing any resources when the
// cluster flavor is not Workload (WCP).
func TestAddSkipsInitForNonWorkloadFlavor(t *testing.T) {
	// If the CO factory is invoked, the test should fail because the early
	// flavor check should short-circuit before reaching the FSS gate.
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
// The test relies on the package-level getContainerOrchestratorInterface
// indirection to inject a fake CO with the FSS turned off, and asserts that
// IsFSSEnabled was queried for exactly the CSI_Backup_API key.
func TestAddSkipsInitWhenCSIBackupAPIFSSDisabled(t *testing.T) {
	fakeCO, err := unittestcommon.GetFakeContainerOrchestratorInterface(common.Kubernetes)
	require.NoError(t, err)

	// Be explicit about the precondition rather than relying on default fixture.
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

	// Verify the gate was actually consulted, and only for the expected key.
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
