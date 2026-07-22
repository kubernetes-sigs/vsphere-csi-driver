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

package snapshotmetadataservicegc

import (
	"context"
	"errors"
	"testing"
	"time"

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
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
)

// ---------------------------------------------------------------------------
// shared test fixtures / helpers
// ---------------------------------------------------------------------------

// newTestScheme creates a runtime scheme with all types required for the
// SnapshotMetadataServiceGC controller tests.
func newTestScheme(t *testing.T) *runtime.Scheme {
	t.Helper()
	s := runtime.NewScheme()
	require.NoError(t, v1.AddToScheme(s))
	require.NoError(t, snapshotmetadatav1beta1.AddToScheme(s))
	return s
}

// resetBackOffDuration ensures each test starts with a clean backOffDuration map.
func resetBackOffDuration() {
	backOffDurationMapMutex.Lock()
	defer backOffDurationMapMutex.Unlock()
	backOffDuration = make(map[string]time.Duration)
}

// makeSMSCR returns a SnapshotMetadataService CR fixture with the given caCert.
func makeSMSCR(caCert []byte) *snapshotmetadatav1beta1.SnapshotMetadataService {
	return &snapshotmetadatav1beta1.SnapshotMetadataService{
		ObjectMeta: metav1.ObjectMeta{Name: targetSMSName},
		Spec: snapshotmetadatav1beta1.SnapshotMetadataServiceSpec{
			CACert:   caCert,
			Address:  "vmware-system-csi-snapshot-metadata-service.vmware-system-csi.svc.cluster.local:8100",
			Audience: "csi.vsphere.vmware.com",
		},
	}
}

// makeGCSecret returns a "-gc" TLS Secret fixture with the given ca.crt data.
// If data is nil, the Secret has no ca.crt field at all.
func makeGCSecret(caCert []byte) *v1.Secret {
	secret := &v1.Secret{
		ObjectMeta: metav1.ObjectMeta{Name: targetSecretName, Namespace: targetNamespace},
		Type:       v1.SecretTypeTLS,
		Data:       map[string][]byte{},
	}
	if caCert != nil {
		secret.Data["ca.crt"] = caCert
	}
	return secret
}

func smsReconcileRequest() reconcile.Request {
	return reconcile.Request{NamespacedName: k8stypes.NamespacedName{Name: targetSMSName}}
}

// ---------------------------------------------------------------------------
// pure function tests
// ---------------------------------------------------------------------------

func TestShouldProcessSecret(t *testing.T) {
	tests := []struct {
		name     string
		secret   *v1.Secret
		expected bool
	}{
		{
			name: "matching name and namespace returns true",
			secret: &v1.Secret{ObjectMeta: metav1.ObjectMeta{
				Name: targetSecretName, Namespace: targetNamespace,
			}},
			expected: true,
		},
		{
			name: "different name returns false",
			secret: &v1.Secret{ObjectMeta: metav1.ObjectMeta{
				Name: "some-other-secret", Namespace: targetNamespace,
			}},
			expected: false,
		},
		{
			name: "different namespace returns false",
			secret: &v1.Secret{ObjectMeta: metav1.ObjectMeta{
				Name: targetSecretName, Namespace: "some-other-namespace",
			}},
			expected: false,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			assert.Equal(t, tc.expected, shouldProcessSecret(tc.secret))
		})
	}
}

// ---------------------------------------------------------------------------
// recordEvent tests
// ---------------------------------------------------------------------------

func TestRecordEvent(t *testing.T) {
	t.Run("warning event doubles backoff and emits warning", func(t *testing.T) {
		resetBackOffDuration()
		instance := makeSMSCR(nil)
		backOffDuration[instance.Name] = 2 * time.Second

		recorder := record.NewFakeRecorder(10)
		r := &ReconcileSnapshotMetadataServiceGC{recorder: recorder}

		recordEvent(context.Background(), r, instance, v1.EventTypeWarning, "something failed")

		backOffDurationMapMutex.Lock()
		got := backOffDuration[instance.Name]
		backOffDurationMapMutex.Unlock()
		assert.Equal(t, 4*time.Second, got)

		select {
		case ev := <-recorder.Events:
			assert.Contains(t, ev, "Warning")
			assert.Contains(t, ev, "SnapshotMetadataServiceGCSyncFailed")
			assert.Contains(t, ev, "something failed")
		case <-time.After(time.Second):
			t.Fatal("expected a warning event to be recorded")
		}
	})

	t.Run("normal event resets backoff and emits normal", func(t *testing.T) {
		resetBackOffDuration()
		instance := makeSMSCR(nil)
		backOffDuration[instance.Name] = 16 * time.Second

		recorder := record.NewFakeRecorder(10)
		r := &ReconcileSnapshotMetadataServiceGC{recorder: recorder}

		recordEvent(context.Background(), r, instance, v1.EventTypeNormal, "all good")

		backOffDurationMapMutex.Lock()
		got := backOffDuration[instance.Name]
		backOffDurationMapMutex.Unlock()
		assert.Equal(t, time.Second, got)

		select {
		case ev := <-recorder.Events:
			assert.Contains(t, ev, "Normal")
			assert.Contains(t, ev, "SnapshotMetadataServiceGCSyncSucceeded")
			assert.Contains(t, ev, "all good")
		case <-time.After(time.Second):
			t.Fatal("expected a normal event to be recorded")
		}
	})
}

// ---------------------------------------------------------------------------
// Reconcile end-to-end tests
// ---------------------------------------------------------------------------

func TestReconcileSMSNotFound(t *testing.T) {
	resetBackOffDuration()
	scheme := newTestScheme(t)
	fakeClient := fake.NewClientBuilder().WithScheme(scheme).Build()

	r := &ReconcileSnapshotMetadataServiceGC{
		client:   fakeClient,
		scheme:   scheme,
		recorder: record.NewFakeRecorder(10),
	}

	res, err := r.Reconcile(context.Background(), smsReconcileRequest())
	assert.NoError(t, err)
	assert.Equal(t, reconcile.Result{}, res)
}

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

	r := &ReconcileSnapshotMetadataServiceGC{
		client:   fakeClient,
		scheme:   scheme,
		recorder: record.NewFakeRecorder(10),
	}

	res, err := r.Reconcile(context.Background(), smsReconcileRequest())
	assert.Error(t, err)
	assert.Equal(t, expectedErr, err)
	assert.Equal(t, reconcile.Result{}, res)
}

func TestReconcileSecretNotFound(t *testing.T) {
	resetBackOffDuration()
	scheme := newTestScheme(t)
	sms := makeSMSCR(nil)

	fakeClient := fake.NewClientBuilder().WithScheme(scheme).WithObjects(sms).Build()
	recorder := record.NewFakeRecorder(10)
	r := &ReconcileSnapshotMetadataServiceGC{client: fakeClient, scheme: scheme, recorder: recorder}

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

func TestReconcileSecretGetError(t *testing.T) {
	resetBackOffDuration()
	scheme := newTestScheme(t)
	sms := makeSMSCR(nil)

	fakeClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(sms).
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
	r := &ReconcileSnapshotMetadataServiceGC{client: fakeClient, scheme: scheme, recorder: recorder}

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

func TestReconcileSecretMissingCAField(t *testing.T) {
	cases := []struct {
		name   string
		secret *v1.Secret
	}{
		{name: "missing ca.crt key", secret: makeGCSecret(nil)},
		{name: "empty ca.crt", secret: makeGCSecret([]byte{})},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			resetBackOffDuration()
			scheme := newTestScheme(t)
			sms := makeSMSCR(nil)

			fakeClient := fake.NewClientBuilder().WithScheme(scheme).WithObjects(sms, tc.secret).Build()
			recorder := record.NewFakeRecorder(10)
			r := &ReconcileSnapshotMetadataServiceGC{client: fakeClient, scheme: scheme, recorder: recorder}

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

func TestReconcileCAPatchSuccess(t *testing.T) {
	resetBackOffDuration()
	scheme := newTestScheme(t)
	newCA := []byte("new-ca-bytes")
	oldCA := []byte("old-ca-bytes")

	sms := makeSMSCR(oldCA)
	secret := makeGCSecret(newCA)

	fakeClient := fake.NewClientBuilder().WithScheme(scheme).WithObjects(sms, secret).Build()
	recorder := record.NewFakeRecorder(10)
	r := &ReconcileSnapshotMetadataServiceGC{client: fakeClient, scheme: scheme, recorder: recorder}

	// Pre-seed a backoff entry to verify it is cleaned up.
	backOffDurationMapMutex.Lock()
	backOffDuration[targetSMSName] = 8 * time.Second
	backOffDurationMapMutex.Unlock()

	res, err := r.Reconcile(context.Background(), smsReconcileRequest())
	require.NoError(t, err)
	assert.Equal(t, reconcile.Result{}, res)

	updated := &snapshotmetadatav1beta1.SnapshotMetadataService{}
	require.NoError(t, fakeClient.Get(context.Background(), k8stypes.NamespacedName{Name: targetSMSName}, updated))
	assert.Equal(t, newCA, updated.Spec.CACert)

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

func TestReconcileCAPatchFailure(t *testing.T) {
	resetBackOffDuration()
	scheme := newTestScheme(t)
	newCA := []byte("new-ca-bytes")
	oldCA := []byte("old-ca-bytes")

	sms := makeSMSCR(oldCA)
	secret := makeGCSecret(newCA)

	fakeClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(sms, secret).
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
	r := &ReconcileSnapshotMetadataServiceGC{client: fakeClient, scheme: scheme, recorder: recorder}

	res, err := r.Reconcile(context.Background(), smsReconcileRequest())
	assert.NoError(t, err)
	assert.True(t, res.RequeueAfter > 0)

	updated := &snapshotmetadatav1beta1.SnapshotMetadataService{}
	require.NoError(t, fakeClient.Get(context.Background(), k8stypes.NamespacedName{Name: targetSMSName}, updated))
	assert.Equal(t, oldCA, updated.Spec.CACert)

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

// TestReconcileAlreadyUpToDate_NoOp verifies that no patch and no event occur
// when the SMS CR's caCert already matches the Secret's ca.crt.
func TestReconcileAlreadyUpToDate_NoOp(t *testing.T) {
	resetBackOffDuration()
	scheme := newTestScheme(t)
	caBytes := []byte("current-ca")

	sms := makeSMSCR(caBytes)
	secret := makeGCSecret(caBytes)

	patchCalled := false
	fakeClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(sms, secret).
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
	r := &ReconcileSnapshotMetadataServiceGC{client: fakeClient, scheme: scheme, recorder: recorder}

	backOffDurationMapMutex.Lock()
	backOffDuration[targetSMSName] = 8 * time.Second
	backOffDurationMapMutex.Unlock()

	res, err := r.Reconcile(context.Background(), smsReconcileRequest())
	require.NoError(t, err)
	assert.Equal(t, reconcile.Result{}, res)
	assert.False(t, patchCalled, "no patch should be issued when caCert is already correct")

	backOffDurationMapMutex.Lock()
	_, exists := backOffDuration[targetSMSName]
	backOffDurationMapMutex.Unlock()
	assert.False(t, exists)

	select {
	case ev := <-recorder.Events:
		t.Fatalf("did not expect any event, got: %s", ev)
	case <-time.After(100 * time.Millisecond):
		// expected: no event recorded
	}
}

func TestReconcileBackoffInitialization(t *testing.T) {
	resetBackOffDuration()
	scheme := newTestScheme(t)
	sms := makeSMSCR(nil)

	fakeClient := fake.NewClientBuilder().WithScheme(scheme).WithObjects(sms).Build()
	r := &ReconcileSnapshotMetadataServiceGC{
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

	// The map is lazily seeded to 1s, but since the Secret is missing, the
	// same Reconcile call also hits the "Secret not found" Warning path, which
	// doubles it to 2s before returning — there's no intermediate no-op branch
	// in this controller to observe the seed value in isolation.
	backOffDurationMapMutex.Lock()
	got, existsAfter := backOffDuration[targetSMSName]
	backOffDurationMapMutex.Unlock()
	assert.True(t, existsAfter, "backoff entry should be created after reconcile")
	assert.Equal(t, 2*time.Second, got)
}

// ---------------------------------------------------------------------------
// Add() flavor gating tests
// ---------------------------------------------------------------------------

// TestAddSkipsInitForNonGuestFlavor verifies that Add returns immediately
// without initializing any resources when the cluster flavor is not Guest
// (VKS). The CSI_Backup_API_FSS gate that used to live inside Add is now
// enforced once, by the caller, before Add is ever invoked.
func TestAddSkipsInitForNonGuestFlavor(t *testing.T) {
	flavors := []cnstypes.CnsClusterFlavor{
		cnstypes.CnsClusterFlavorVanilla,
		cnstypes.CnsClusterFlavorWorkload,
	}
	for _, flavor := range flavors {
		flavor := flavor
		t.Run(string(flavor), func(t *testing.T) {
			err := Add(nil, flavor, nil)
			assert.NoError(t, err, "Add should be a no-op for non-Guest flavors")
		})
	}
}
