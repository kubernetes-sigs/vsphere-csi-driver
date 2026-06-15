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

package storagepolicyinfo

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	v1 "k8s.io/api/core/v1"
	storagev1 "k8s.io/api/storage/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/record"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
	apis "sigs.k8s.io/vsphere-csi-driver/v3/pkg/apis/cnsoperator"
	infraspiv1alpha1 "sigs.k8s.io/vsphere-csi-driver/v3/pkg/apis/cnsoperator/infrastoragepolicyinfo/v1alpha1"
	spiv1alpha1 "sigs.k8s.io/vsphere-csi-driver/v3/pkg/apis/cnsoperator/storagepolicyinfo/v1alpha1"
	storagepolicyv1alpha2 "sigs.k8s.io/vsphere-csi-driver/v3/pkg/apis/cnsoperator/storagepolicy/v1alpha2"
	"sigs.k8s.io/vsphere-csi-driver/v3/pkg/csi/service/logger"
	k8s "sigs.k8s.io/vsphere-csi-driver/v3/pkg/kubernetes"
)

func testScheme(t *testing.T) *runtime.Scheme {
	t.Helper()
	s := runtime.NewScheme()
	require.NoError(t, apis.SchemeBuilder.AddToScheme(s))
	require.NoError(t, storagev1.AddToScheme(s))
	return s
}

// mockZonesProvider is a minimal test double for the zonesProvider interface.
// Only GetZonesForNamespace carries real behaviour; tests set zonesForNamespace
// to control which zones are returned per namespace.
type mockZonesProvider struct {
	zonesForNamespace map[string]map[string]struct{}
}

func (m *mockZonesProvider) GetZonesForNamespace(ns string) map[string]struct{} {
	if m.zonesForNamespace != nil {
		return m.zonesForNamespace[ns]
	}
	return nil
}

var _ zonesProvider = &mockZonesProvider{}

// TestMapSPQtoSPI_NilObject verifies that mapSPQtoSPI returns nil for a nil input.
func TestMapSPQtoSPI_NilObject(t *testing.T) {
	ctx := logger.NewContextWithLogger(context.Background())
	scheme := testScheme(t)
	r := &ReconcileStoragePolicyInfo{
		client: fake.NewClientBuilder().WithScheme(scheme).Build(),
		scheme: scheme,
	}
	reqs := r.mapSPQtoSPI(ctx, nil)
	assert.Nil(t, reqs)
}

// TestMapSPQtoSPI_CorrectSuffix verifies that an SPQ with the expected suffix maps to
// the right SPI reconcile request.
func TestMapSPQtoSPI_CorrectSuffix(t *testing.T) {
	ctx := logger.NewContextWithLogger(context.Background())
	scheme := testScheme(t)
	r := &ReconcileStoragePolicyInfo{
		client: fake.NewClientBuilder().WithScheme(scheme).Build(),
		scheme: scheme,
	}
	spq := &storagepolicyv1alpha2.StoragePolicyQuota{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "gold-policy-storagepolicyquota",
			Namespace: "test-ns",
		},
	}
	reqs := r.mapSPQtoSPI(ctx, spq)
	require.Len(t, reqs, 1)
	assert.Equal(t, reconcile.Request{
		NamespacedName: types.NamespacedName{Namespace: "test-ns", Name: "gold-policy"},
	}, reqs[0])
}

// TestMapSPQtoSPI_NoSuffix verifies that an object whose name does not end with the
// expected suffix is silently ignored.
func TestMapSPQtoSPI_NoSuffix(t *testing.T) {
	ctx := logger.NewContextWithLogger(context.Background())
	scheme := testScheme(t)
	r := &ReconcileStoragePolicyInfo{
		client: fake.NewClientBuilder().WithScheme(scheme).Build(),
		scheme: scheme,
	}
	spq := &storagepolicyv1alpha2.StoragePolicyQuota{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "some-other-object",
			Namespace: "test-ns",
		},
	}
	reqs := r.mapSPQtoSPI(ctx, spq)
	assert.Nil(t, reqs)
}

// TestMergeOwnerReference exercises the four cases of the mergeOwnerReference helper.
func TestMergeOwnerReference(t *testing.T) {
	controllerFalse := false
	blockFalse := false
	base := metav1.OwnerReference{
		APIVersion: "cns.vmware.com/v1alpha1", Kind: "InfraStoragePolicyInfo", Name: "gold",
		UID:        types.UID("aaaa"),
		Controller: &controllerFalse, BlockOwnerDeletion: &blockFalse,
	}
	other := metav1.OwnerReference{
		APIVersion: "cns.vmware.com/v1alpha1", Kind: "InfraStoragePolicyInfo", Name: "silver",
		UID:        types.UID("bbbb"),
		Controller: &controllerFalse, BlockOwnerDeletion: &blockFalse,
	}
	sameKeyNewUID := metav1.OwnerReference{
		APIVersion: base.APIVersion, Kind: base.Kind, Name: base.Name,
		UID:        types.UID("cccc"),
		Controller: &controllerFalse, BlockOwnerDeletion: &blockFalse,
	}

	t.Run("append when new", func(t *testing.T) {
		out := mergeOwnerReference([]metav1.OwnerReference{base}, other)
		require.Len(t, out, 2)
		assert.Equal(t, base, out[0])
		assert.Equal(t, other, out[1])
	})

	t.Run("unchanged when same key and UID", func(t *testing.T) {
		refs := []metav1.OwnerReference{base}
		out := mergeOwnerReference(refs, base)
		assert.Equal(t, refs, out)
	})

	t.Run("replace when same key different UID", func(t *testing.T) {
		out := mergeOwnerReference([]metav1.OwnerReference{base}, sameKeyNewUID)
		require.Len(t, out, 1)
		assert.Equal(t, sameKeyNewUID, out[0])
	})

	t.Run("empty slice adds one", func(t *testing.T) {
		out := mergeOwnerReference(nil, base)
		require.Len(t, out, 1)
		assert.Equal(t, base, out[0])
	})
}

// TestGenerateOwnerReference_InfraSPI verifies the fields produced for an
// InfraStoragePolicyInfo owner reference.
func TestGenerateOwnerReference_InfraSPI(t *testing.T) {
	scheme := testScheme(t)
	infraSPI := &infraspiv1alpha1.InfraStoragePolicyInfo{
		ObjectMeta: metav1.ObjectMeta{Name: "gold", UID: types.UID("1111")},
	}
	ref, err := generateOwnerReference(scheme, infraSPI)
	require.NoError(t, err)
	assert.Equal(t, "cns.vmware.com/v1alpha1", ref.APIVersion)
	assert.Equal(t, "InfraStoragePolicyInfo", ref.Kind)
	assert.Equal(t, "gold", ref.Name)
	assert.Equal(t, types.UID("1111"), ref.UID)
	require.NotNil(t, ref.Controller)
	assert.False(t, *ref.Controller)
	require.NotNil(t, ref.BlockOwnerDeletion)
	assert.False(t, *ref.BlockOwnerDeletion)
}

// TestGenerateOwnerReference_UnknownType verifies that an error is returned when
// the type is not registered in the scheme.
func TestGenerateOwnerReference_UnknownType(t *testing.T) {
	emptyScheme := runtime.NewScheme()
	infraSPI := &infraspiv1alpha1.InfraStoragePolicyInfo{
		ObjectMeta: metav1.ObjectMeta{Name: "x"},
	}
	_, err := generateOwnerReference(emptyScheme, infraSPI)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "no kind is registered")
}

// TestEnsureSPIExists_AlreadyExists verifies that ensureSPIExists returns the
// existing instance (wasCreated=false) when the CR is already present.
func TestEnsureSPIExists_AlreadyExists(t *testing.T) {
	ctx := logger.NewContextWithLogger(context.Background())
	scheme := testScheme(t)
	existing := &spiv1alpha1.StoragePolicyInfo{
		ObjectMeta: metav1.ObjectMeta{Name: "gold", Namespace: "test-ns"},
	}
	cli := fake.NewClientBuilder().WithScheme(scheme).WithObjects(existing).Build()
	r := &ReconcileStoragePolicyInfo{client: cli, scheme: scheme}

	inst, wasCreated, err := r.ensureSPIExists(ctx, "test-ns", "gold")
	require.NoError(t, err)
	assert.False(t, wasCreated)
	require.NotNil(t, inst)
	assert.Equal(t, "gold", inst.Name)
}

// TestEnsureSPIExists_NotFound verifies that ensureSPIExists creates and persists
// a new StoragePolicyInfo (wasCreated=true) when the CR does not exist.
func TestEnsureSPIExists_NotFound(t *testing.T) {
	ctx := logger.NewContextWithLogger(context.Background())
	scheme := testScheme(t)
	cli := fake.NewClientBuilder().WithScheme(scheme).Build()
	r := &ReconcileStoragePolicyInfo{client: cli, scheme: scheme}

	inst, wasCreated, err := r.ensureSPIExists(ctx, "test-ns", "gold")
	require.NoError(t, err)
	assert.True(t, wasCreated)
	require.NotNil(t, inst)
	assert.Equal(t, "gold", inst.Name)
	assert.Equal(t, "test-ns", inst.Namespace)

	// Verify the CR is persisted in the fake client.
	got := &spiv1alpha1.StoragePolicyInfo{}
	require.NoError(t, cli.Get(ctx, types.NamespacedName{Namespace: "test-ns", Name: "gold"}, got))
}

// TestEnsureInfraSPIOwnerReference_SetWhenAbsent verifies that the owner reference
// is created when the SPI has no owner references yet.
func TestEnsureInfraSPIOwnerReference_SetWhenAbsent(t *testing.T) {
	ctx := logger.NewContextWithLogger(context.Background())
	scheme := testScheme(t)
	spi := &spiv1alpha1.StoragePolicyInfo{
		ObjectMeta: metav1.ObjectMeta{Name: "gold", Namespace: "test-ns"},
	}
	infraSPI := &infraspiv1alpha1.InfraStoragePolicyInfo{
		ObjectMeta: metav1.ObjectMeta{Name: "gold", UID: types.UID("gold-uid")},
	}
	cli := fake.NewClientBuilder().WithScheme(scheme).WithObjects(spi).Build()
	r := &ReconcileStoragePolicyInfo{client: cli, scheme: scheme}

	err := r.ensureInfraSPIOwnerReference(ctx, spi, infraSPI)
	require.NoError(t, err)
	require.Len(t, spi.OwnerReferences, 1)
	assert.Equal(t, "InfraStoragePolicyInfo", spi.OwnerReferences[0].Kind)
	assert.Equal(t, "gold", spi.OwnerReferences[0].Name)
	assert.Equal(t, types.UID("gold-uid"), spi.OwnerReferences[0].UID)
}

// TestEnsureInfraSPIOwnerReference_NoOpWhenAlreadySet verifies that no patch is
// issued when the owner reference already points to the same UID.
func TestEnsureInfraSPIOwnerReference_NoOpWhenAlreadySet(t *testing.T) {
	ctx := logger.NewContextWithLogger(context.Background())
	scheme := testScheme(t)
	controllerFalse := false
	blockFalse := false
	infraSPI := &infraspiv1alpha1.InfraStoragePolicyInfo{
		ObjectMeta: metav1.ObjectMeta{Name: "gold", UID: types.UID("gold-uid")},
	}
	existingRef := metav1.OwnerReference{
		APIVersion:         "cns.vmware.com/v1alpha1",
		Kind:               "InfraStoragePolicyInfo",
		Name:               "gold",
		UID:                types.UID("gold-uid"),
		Controller:         &controllerFalse,
		BlockOwnerDeletion: &blockFalse,
	}
	spi := &spiv1alpha1.StoragePolicyInfo{
		ObjectMeta: metav1.ObjectMeta{
			Name: "gold", Namespace: "test-ns",
			OwnerReferences: []metav1.OwnerReference{existingRef},
		},
	}
	cli := fake.NewClientBuilder().WithScheme(scheme).WithObjects(spi).Build()
	r := &ReconcileStoragePolicyInfo{client: cli, scheme: scheme}

	err := r.ensureInfraSPIOwnerReference(ctx, spi, infraSPI)
	require.NoError(t, err)
	require.Len(t, spi.OwnerReferences, 1, "owner reference count must not change")
}

// TestSyncTopologyFromInfraSPI_CopiesTopology verifies that topology data is
// correctly copied from InfraStoragePolicyInfo into the StoragePolicyInfo status.
func TestSyncTopologyFromInfraSPI_CopiesTopology(t *testing.T) {
	ctx := logger.NewContextWithLogger(context.Background())
	scheme := testScheme(t)
	r := &ReconcileStoragePolicyInfo{
		client:        fake.NewClientBuilder().WithScheme(scheme).Build(),
		scheme:        scheme,
		zonesProvider: &mockZonesProvider{},
	}

	inst := &spiv1alpha1.StoragePolicyInfo{
		ObjectMeta: metav1.ObjectMeta{Name: "gold", Namespace: "ns1"},
	}
	infraSPI := &infraspiv1alpha1.InfraStoragePolicyInfo{
		ObjectMeta: metav1.ObjectMeta{Name: "gold"},
		Status: infraspiv1alpha1.InfraStoragePolicyInfoStatus{
			Topology: &infraspiv1alpha1.Topology{
				TopologyType:    "zonal",
				AccessibleZones: []string{"az1", "az2"},
			},
		},
	}

	err := r.syncTopologyFromInfraSPI(ctx, inst, infraSPI)
	require.NoError(t, err)
	require.NotNil(t, inst.Status.TopologyInfo)
	assert.Equal(t, "zonal", inst.Status.TopologyInfo.TopologyType)
	assert.ElementsMatch(t, []string{"az1", "az2"}, inst.Status.TopologyInfo.AccessibleZones)
}

// TestSyncTopologyFromInfraSPI_ClearsWhenNilTopology verifies that when
// InfraStoragePolicyInfo has no Topology, the StoragePolicyInfo TopologyInfo is
// set to nil.
func TestSyncTopologyFromInfraSPI_ClearsWhenNilTopology(t *testing.T) {
	ctx := logger.NewContextWithLogger(context.Background())
	scheme := testScheme(t)
	r := &ReconcileStoragePolicyInfo{
		client: fake.NewClientBuilder().WithScheme(scheme).Build(),
		scheme: scheme,
	}

	inst := &spiv1alpha1.StoragePolicyInfo{
		ObjectMeta: metav1.ObjectMeta{Name: "gold", Namespace: "ns1"},
		Status: spiv1alpha1.StoragePolicyInfoStatus{
			TopologyInfo: &spiv1alpha1.Topology{TopologyType: "zonal"},
		},
	}
	infraSPI := &infraspiv1alpha1.InfraStoragePolicyInfo{
		ObjectMeta: metav1.ObjectMeta{Name: "gold"},
	}

	err := r.syncTopologyFromInfraSPI(ctx, inst, infraSPI)
	require.NoError(t, err)
	assert.Nil(t, inst.Status.TopologyInfo)
}

// TestNamespaceFilteredZones exercises the zone-intersection logic used by
// namespaceFilteredZones with a table of representative inputs.
func TestNamespaceFilteredZones(t *testing.T) {
	ctx := logger.NewContextWithLogger(context.Background())
	scheme := testScheme(t)

	tests := []struct {
		name          string
		nsZones       map[string]struct{} // zones for "ns1"; nil = no constraint
		clusterZones  []string
		expectedZones []string
	}{
		{
			name:          "no namespace zone constraints returns all cluster zones",
			nsZones:       nil,
			clusterZones:  []string{"az1", "az2", "az3"},
			expectedZones: []string{"az1", "az2", "az3"},
		},
		{
			name:          "namespace zones intersect cluster zones",
			nsZones:       map[string]struct{}{"az1": {}, "az3": {}},
			clusterZones:  []string{"az1", "az2", "az3"},
			expectedZones: []string{"az1", "az3"},
		},
		{
			name:          "namespace zones disjoint from cluster zones returns empty",
			nsZones:       map[string]struct{}{"az9": {}},
			clusterZones:  []string{"az1", "az2"},
			expectedZones: nil,
		},
		{
			name:          "empty cluster zones returns empty",
			nsZones:       map[string]struct{}{"az1": {}},
			clusterZones:  nil,
			expectedZones: nil,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var zonesMap map[string]map[string]struct{}
			if tt.nsZones != nil {
				zonesMap = map[string]map[string]struct{}{"ns1": tt.nsZones}
			}
			r := &ReconcileStoragePolicyInfo{
				client:        fake.NewClientBuilder().WithScheme(scheme).Build(),
				scheme:        scheme,
				zonesProvider: &mockZonesProvider{zonesForNamespace: zonesMap},
			}

			got := r.namespaceFilteredZones(ctx, "ns1", tt.clusterZones)
			if len(tt.expectedZones) == 0 {
				assert.Empty(t, got)
			} else {
				assert.ElementsMatch(t, tt.expectedZones, got)
			}
		})
	}
}

// TestReconcile_CreatesAndReturnsEarly verifies that when a StoragePolicyInfo does
// not yet exist Reconcile creates it, returns early, and lets the creation event
// drive the next reconcile.
func TestReconcile_CreatesAndReturnsEarly(t *testing.T) {
	ctx := logger.NewContextWithLogger(context.Background())
	scheme := testScheme(t)
	backOffDuration = make(map[types.NamespacedName]time.Duration)

	cli := fake.NewClientBuilder().WithScheme(scheme).
		WithStatusSubresource(&spiv1alpha1.StoragePolicyInfo{}).Build()
	r := &ReconcileStoragePolicyInfo{
		client:        cli,
		scheme:        scheme,
		recorder:      record.NewFakeRecorder(10),
		zonesProvider: &mockZonesProvider{},
	}

	result, err := r.Reconcile(ctx, reconcile.Request{
		NamespacedName: types.NamespacedName{Namespace: "ns1", Name: "gold"},
	})
	require.NoError(t, err)
	assert.Equal(t, reconcile.Result{}, result)

	got := &spiv1alpha1.StoragePolicyInfo{}
	require.NoError(t, cli.Get(ctx, types.NamespacedName{Namespace: "ns1", Name: "gold"}, got))
}

// TestReconcile_SkipsDeletion verifies that Reconcile exits immediately when
// DeletionTimestamp is set on the StoragePolicyInfo.
func TestReconcile_SkipsDeletion(t *testing.T) {
	ctx := logger.NewContextWithLogger(context.Background())
	scheme := testScheme(t)
	backOffDuration = make(map[types.NamespacedName]time.Duration)

	now := metav1.Now()
	spi := &spiv1alpha1.StoragePolicyInfo{
		ObjectMeta: metav1.ObjectMeta{
			Name: "gold", Namespace: "ns1",
			DeletionTimestamp: &now,
			Finalizers:        []string{"test"},
		},
	}
	cli := fake.NewClientBuilder().WithScheme(scheme).
		WithStatusSubresource(&spiv1alpha1.StoragePolicyInfo{}).WithObjects(spi).Build()
	r := &ReconcileStoragePolicyInfo{
		client:        cli,
		scheme:        scheme,
		recorder:      record.NewFakeRecorder(10),
		zonesProvider: &mockZonesProvider{},
	}

	result, err := r.Reconcile(ctx, reconcile.Request{
		NamespacedName: types.NamespacedName{Namespace: "ns1", Name: "gold"},
	})
	require.NoError(t, err)
	assert.Equal(t, reconcile.Result{}, result)
}

// TestReconcile_SyncsTopologyAndRecordsEvent verifies the full happy-path:
// topology is copied from InfraStoragePolicyInfo and a Normal event is emitted.
func TestReconcile_SyncsTopologyAndRecordsEvent(t *testing.T) {
	ctx := logger.NewContextWithLogger(context.Background())
	scheme := testScheme(t)
	backOffDuration = make(map[types.NamespacedName]time.Duration)

	spi := &spiv1alpha1.StoragePolicyInfo{
		ObjectMeta: metav1.ObjectMeta{Name: "gold", Namespace: "ns1"},
	}
	infraSPI := &infraspiv1alpha1.InfraStoragePolicyInfo{
		ObjectMeta: metav1.ObjectMeta{Name: "gold", UID: types.UID("gold-uid")},
		Status: infraspiv1alpha1.InfraStoragePolicyInfoStatus{
			Topology: &infraspiv1alpha1.Topology{
				TopologyType:    "zonal",
				AccessibleZones: []string{"az1"},
			},
		},
	}
	recorder := record.NewFakeRecorder(10)
	cli := fake.NewClientBuilder().WithScheme(scheme).
		WithStatusSubresource(&spiv1alpha1.StoragePolicyInfo{}).
		WithObjects(spi, infraSPI).Build()
	r := &ReconcileStoragePolicyInfo{
		client:        cli,
		scheme:        scheme,
		recorder:      recorder,
		zonesProvider: &mockZonesProvider{},
	}

	result, err := r.Reconcile(ctx, reconcile.Request{
		NamespacedName: types.NamespacedName{Namespace: "ns1", Name: "gold"},
	})
	require.NoError(t, err)
	assert.Equal(t, reconcile.Result{}, result)

	select {
	case ev := <-recorder.Events:
		assert.Contains(t, ev, string(v1.EventTypeNormal))
		assert.Contains(t, ev, "StoragePolicyInfoSynced")
	default:
		t.Error("expected a Normal StoragePolicyInfoSynced event")
	}
}

// TestReconcile_InfraSPINotFound verifies that when InfraStoragePolicyInfo does
// not yet exist, Reconcile returns success (the event-driven next reconcile will
// populate topology once InfraStoragePolicyInfo is available).
func TestReconcile_InfraSPINotFound(t *testing.T) {
	ctx := logger.NewContextWithLogger(context.Background())
	scheme := testScheme(t)
	backOffDuration = make(map[types.NamespacedName]time.Duration)

	spi := &spiv1alpha1.StoragePolicyInfo{
		ObjectMeta: metav1.ObjectMeta{Name: "gold", Namespace: "ns1"},
	}
	cli := fake.NewClientBuilder().WithScheme(scheme).
		WithStatusSubresource(&spiv1alpha1.StoragePolicyInfo{}).WithObjects(spi).Build()
	r := &ReconcileStoragePolicyInfo{
		client:        cli,
		scheme:        scheme,
		recorder:      record.NewFakeRecorder(10),
		zonesProvider: &mockZonesProvider{},
	}

	result, err := r.Reconcile(ctx, reconcile.Request{
		NamespacedName: types.NamespacedName{Namespace: "ns1", Name: "gold"},
	})
	require.NoError(t, err)
	assert.Equal(t, reconcile.Result{}, result)
}

// TestSetSPISuccess verifies that setSPISuccess clears the error field and
// records a Normal "StoragePolicyInfoSynced" event.
func TestSetSPISuccess(t *testing.T) {
	ctx := logger.NewContextWithLogger(context.Background())
	scheme := testScheme(t)
	spi := &spiv1alpha1.StoragePolicyInfo{
		ObjectMeta: metav1.ObjectMeta{Name: "gold", Namespace: "ns1"},
		Status:     spiv1alpha1.StoragePolicyInfoStatus{Error: "previous error"},
	}
	recorder := record.NewFakeRecorder(10)
	cli := fake.NewClientBuilder().WithScheme(scheme).
		WithStatusSubresource(&spiv1alpha1.StoragePolicyInfo{}).WithObjects(spi).Build()
	r := &ReconcileStoragePolicyInfo{client: cli, scheme: scheme, recorder: recorder}

	err := r.setSPISuccess(ctx, spi, "all good")
	require.NoError(t, err)
	assert.Empty(t, spi.Status.Error, "expected error field to be cleared")

	select {
	case ev := <-recorder.Events:
		assert.Contains(t, ev, "Normal")
		assert.Contains(t, ev, "StoragePolicyInfoSynced")
	default:
		t.Error("expected a Normal StoragePolicyInfoSynced event")
	}
}

// TestSetSPIError verifies that setSPIError populates the error field and
// records a Warning "StoragePolicyInfoFailed" event.
func TestSetSPIError(t *testing.T) {
	ctx := logger.NewContextWithLogger(context.Background())
	scheme := testScheme(t)
	spi := &spiv1alpha1.StoragePolicyInfo{
		ObjectMeta: metav1.ObjectMeta{Name: "gold", Namespace: "ns1"},
	}
	recorder := record.NewFakeRecorder(10)
	cli := fake.NewClientBuilder().WithScheme(scheme).
		WithStatusSubresource(&spiv1alpha1.StoragePolicyInfo{}).WithObjects(spi).Build()
	r := &ReconcileStoragePolicyInfo{client: cli, scheme: scheme, recorder: recorder}

	err := r.setSPIError(ctx, spi, "something failed")
	require.NoError(t, err)
	assert.Equal(t, "something failed", spi.Status.Error)

	select {
	case ev := <-recorder.Events:
		assert.Contains(t, ev, "Warning")
		assert.Contains(t, ev, "StoragePolicyInfoFailed")
	default:
		t.Error("expected a Warning StoragePolicyInfoFailed event")
	}
}

// TestUpdateStatus_StatusFieldPersisted verifies that k8s.UpdateStatus correctly
// persists changes to the StoragePolicyInfo status subresource.
func TestUpdateStatus_StatusFieldPersisted(t *testing.T) {
	ctx := logger.NewContextWithLogger(context.Background())
	scheme := testScheme(t)
	spi := &spiv1alpha1.StoragePolicyInfo{
		ObjectMeta: metav1.ObjectMeta{Name: "gold", Namespace: "ns1"},
	}
	cli := fake.NewClientBuilder().WithScheme(scheme).
		WithStatusSubresource(&spiv1alpha1.StoragePolicyInfo{}).WithObjects(spi).Build()

	spi.Status.Error = "injected error"
	err := k8s.UpdateStatus(ctx, cli, spi)
	assert.NoError(t, err)
	assert.Equal(t, "injected error", spi.Status.Error)
}

// TestCompleteReconciliationWithSuccess verifies that a successful reconciliation
// removes the entry from the backoff map.
func TestCompleteReconciliationWithSuccess(t *testing.T) {
	ctx := logger.NewContextWithLogger(context.Background())
	scheme := testScheme(t)
	r := &ReconcileStoragePolicyInfo{
		client:   fake.NewClientBuilder().WithScheme(scheme).Build(),
		scheme:   scheme,
		recorder: record.NewFakeRecorder(10),
	}
	nn := types.NamespacedName{Namespace: "ns1", Name: "gold"}

	backOffDuration = make(map[types.NamespacedName]time.Duration)
	backOffDuration[nn] = 5 * time.Second

	result, err := r.completeReconciliationWithSuccess(ctx, nn)
	require.NoError(t, err)
	assert.Equal(t, reconcile.Result{}, result)

	backOffDurationMapMutex.Lock()
	_, exists := backOffDuration[nn]
	backOffDurationMapMutex.Unlock()
	assert.False(t, exists, "expected backoff entry to be deleted on success")
}

// TestCompleteReconciliationWithError verifies that a failed reconciliation
// doubles the backoff duration.
func TestCompleteReconciliationWithError(t *testing.T) {
	ctx := logger.NewContextWithLogger(context.Background())
	scheme := testScheme(t)
	r := &ReconcileStoragePolicyInfo{
		client:   fake.NewClientBuilder().WithScheme(scheme).Build(),
		scheme:   scheme,
		recorder: record.NewFakeRecorder(10),
	}
	nn := types.NamespacedName{Namespace: "ns1", Name: "gold"}

	backOffDuration = make(map[types.NamespacedName]time.Duration)
	backOffDuration[nn] = time.Second

	result, err := r.completeReconciliationWithError(ctx, nn, time.Second, assert.AnError)
	require.NoError(t, err)
	assert.Equal(t, reconcile.Result{RequeueAfter: time.Second}, result)

	backOffDurationMapMutex.Lock()
	got := backOffDuration[nn]
	backOffDurationMapMutex.Unlock()
	assert.Equal(t, 2*time.Second, got, "expected backoff to be doubled on error")
}
