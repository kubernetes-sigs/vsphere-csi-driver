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
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/record"
	ctrlcache "sigs.k8s.io/controller-runtime/pkg/cache"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"
	"sigs.k8s.io/controller-runtime/pkg/event"
	"sigs.k8s.io/controller-runtime/pkg/manager"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
	infraspiv1alpha1 "sigs.k8s.io/vsphere-csi-driver/v3/pkg/apis/cnsoperator/infrastoragepolicyinfo/v1alpha1"
	spiv1alpha1 "sigs.k8s.io/vsphere-csi-driver/v3/pkg/apis/cnsoperator/storagepolicyinfo/v1alpha1"
	"sigs.k8s.io/vsphere-csi-driver/v3/pkg/csi/service/logger"
)

// zoneTestScheme returns testScheme with the Zone CR registered as unstructured, so the fake
// client can store and list Zones the way the manager's cache serves them.
func zoneTestScheme(t *testing.T) *runtime.Scheme {
	t.Helper()
	s := testScheme(t)
	s.AddKnownTypeWithName(zoneGVK, &unstructured.Unstructured{})
	s.AddKnownTypeWithName(zoneGVK.GroupVersion().WithKind(zoneGVK.Kind+"List"), &unstructured.UnstructuredList{})
	return s
}

// testZone builds a Zone CR in namespace. clusters, when non-nil, is set as
// spec.namespace.clusterMoIDs; deleting sets a deletionTimestamp (with a finalizer, which the
// fake client requires for objects being deleted).
func testZone(t *testing.T, namespace, name string, clusters []string, deleting bool) *unstructured.Unstructured {
	t.Helper()
	z := newZone(namespace, name)
	if clusters != nil {
		require.NoError(t, unstructured.SetNestedStringSlice(z.Object, clusters, "spec", "namespace", "clusterMoIDs"))
	}
	if deleting {
		now := metav1.Now()
		z.SetDeletionTimestamp(&now)
		z.SetFinalizers([]string{"tanzu-topology.vmware.com/zone"})
	}
	return z
}

// TestCacheZonesProvider verifies that cacheZonesProvider reports zones and active clusters from
// the Zone CRs it reads, with the same semantics as the commonco implementation it replaces.
func TestCacheZonesProvider(t *testing.T) {
	ctx := logger.NewContextWithLogger(context.Background())
	cli := fake.NewClientBuilder().WithScheme(zoneTestScheme(t)).WithObjects(
		testZone(t, "ns1", "az1", []string{"domain-c9"}, false),
		testZone(t, "ns1", "az2", []string{"domain-c10", "domain-c11"}, false),
		testZone(t, "ns1", "az3", nil, false),                   // clusterMoIDs not filled in yet
		testZone(t, "ns1", "az4", []string{"domain-c12"}, true), // being unassigned
		testZone(t, "ns2", "az5", []string{"domain-c13"}, false),
	).Build()
	p := newCacheZonesProvider(cli)

	zones, err := p.GetZonesForNamespace(ctx, "ns1")
	require.NoError(t, err)
	assert.Equal(t, map[string]struct{}{"az1": {}, "az2": {}, "az3": {}}, zones,
		"zones being deleted and zones of other namespaces must be excluded")

	zones, err = p.GetZonesForNamespace(ctx, "ns3")
	require.NoError(t, err)
	assert.Empty(t, zones)

	for _, tc := range []struct {
		name    string
		zones   []string
		want    []string
		wantErr string
	}{
		{name: "single zone", zones: []string{"az1"}, want: []string{"domain-c9"}},
		{name: "multiple zones", zones: []string{"az1", "az2"}, want: []string{"domain-c9", "domain-c10", "domain-c11"}},
		{name: "clusterMoIDs not filled in yet", zones: []string{"az3"},
			wantErr: "spec.namespace.clusterMoIDs not found in Zone ns1/az3"},
		{name: "zone being deleted", zones: []string{"az4"}, wantErr: "could not find active cluster"},
		{name: "zone of another namespace", zones: []string{"az5"}, wantErr: "could not find active cluster"},
		{name: "zone not assigned", zones: []string{"az9"}, wantErr: "could not find active cluster"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got, err := p.GetActiveClustersForNamespaceInRequestedZones(ctx, "ns1", tc.zones)
			if tc.wantErr != "" {
				require.ErrorContains(t, err, tc.wantErr)
				return
			}
			require.NoError(t, err)
			assert.ElementsMatch(t, tc.want, got)
		})
	}
}

// TestCacheZonesProvider_ListError verifies that a failure to read Zone CRs is returned rather
// than reported as the namespace having no zones.
func TestCacheZonesProvider_ListError(t *testing.T) {
	ctx := logger.NewContextWithLogger(context.Background())
	cli := fake.NewClientBuilder().WithScheme(zoneTestScheme(t)).WithInterceptorFuncs(interceptor.Funcs{
		List: func(_ context.Context, _ client.WithWatch, _ client.ObjectList, _ ...client.ListOption) error {
			return errors.New("cache not synced")
		},
	}).Build()
	p := newCacheZonesProvider(cli)

	_, err := p.GetZonesForNamespace(ctx, "ns1")
	require.ErrorContains(t, err, "cache not synced")
	_, err = p.GetActiveClustersForNamespaceInRequestedZones(ctx, "ns1", []string{"az1"})
	require.ErrorContains(t, err, "cache not synced")
}

// fakeManager provides just what newReconciler reads from a manager.
type fakeManager struct {
	manager.Manager
	client client.Client
	scheme *runtime.Scheme
	cache  ctrlcache.Cache
}

func (m *fakeManager) GetClient() client.Client   { return m.client }
func (m *fakeManager) GetScheme() *runtime.Scheme { return m.scheme }
func (m *fakeManager) GetCache() ctrlcache.Cache  { return m.cache }

// fakeCache is a distinct ctrlcache.Cache instance; only its identity is checked.
type fakeCache struct {
	ctrlcache.Cache
}

// TestNewReconciler_ReadsZonesFromManagerCache verifies that the reconciler reads Zone CRs from
// the manager's cache, which the Zone watch is served from, rather than from any other informer.
func TestNewReconciler_ReadsZonesFromManagerCache(t *testing.T) {
	mgr := &fakeManager{
		client: fake.NewClientBuilder().WithScheme(testScheme(t)).Build(),
		scheme: testScheme(t),
		cache:  &fakeCache{},
	}
	r := newReconciler(mgr, nil, record.NewFakeRecorder(1), nil, nil, false)

	zp, ok := r.zonesProvider.(*cacheZonesProvider)
	require.True(t, ok, "zonesProvider must be a cacheZonesProvider, got %T", r.zonesProvider)
	assert.Same(t, mgr.cache, zp.reader, "Zone CRs must be read from mgr.GetCache()")
}

// TestReconcile_ZoneClusterMoIDsUpdateWithLaggingTopologyInformer reproduces a namespace created
// with storage policies in the same request (e.g. dcli `namespaces instances create
// --storage-specs`): the SPI is reconciled when the namespace's Zone CR is created, before WCP
// fills in its spec.namespace.clusterMoIDs in a second update. The reconcile triggered by that
// update must see the clusterMoIDs even though a separate Zone informer (commonco's) still holds
// the Zone without them; reading from that informer would drop the zone with nothing left to
// re-trigger the reconcile until the periodic resync.
func TestReconcile_ZoneClusterMoIDsUpdateWithLaggingTopologyInformer(t *testing.T) {
	ctx := logger.NewContextWithLogger(context.Background())
	scheme := zoneTestScheme(t)
	key := types.NamespacedName{Namespace: "ns1", Name: "gold"}

	infraSPI := &infraspiv1alpha1.InfraStoragePolicyInfo{
		ObjectMeta: metav1.ObjectMeta{Name: "gold", UID: types.UID("gold-uid")},
		Status: infraspiv1alpha1.InfraStoragePolicyInfoStatus{
			Topology:           &infraspiv1alpha1.Topology{AccessibleZones: []string{"az1"}},
			VolumeCapabilities: determinedInfraVolumeCapabilities(true, false),
		},
	}
	createdZone := testZone(t, "ns1", "az1", nil, false)

	// watchCache stands in for the manager's cache: it serves the Zone watch, so it holds every
	// version of the Zone the watch has delivered. laggingInformer stands in for the separate
	// commonco Zone informer, which has not yet seen the clusterMoIDs update.
	watchCache := fake.NewClientBuilder().WithScheme(scheme).
		WithStatusSubresource(&spiv1alpha1.StoragePolicyInfo{}).
		WithObjects(&spiv1alpha1.StoragePolicyInfo{ObjectMeta: metav1.ObjectMeta{Name: "gold", Namespace: "ns1"}},
			infraSPI, testSPQ("ns1", "gold"), testCanonicalClusterSPI("gold"), createdZone.DeepCopy()).Build()
	laggingInformer := fake.NewClientBuilder().WithScheme(scheme).WithObjects(createdZone.DeepCopy()).Build()

	newTestReconciler := func(zp zonesProvider) *ReconcileStoragePolicyInfo {
		return &ReconcileStoragePolicyInfo{
			client: watchCache, scheme: scheme, recorder: record.NewFakeRecorder(10),
			zonesProvider:   zp,
			backOffDuration: make(map[types.NamespacedName]time.Duration),
		}
	}
	reconcileAndGetZones := func(r *ReconcileStoragePolicyInfo) []string {
		t.Helper()
		result, err := r.Reconcile(ctx, reconcile.Request{NamespacedName: key})
		require.NoError(t, err)
		require.Equal(t, reconcile.Result{}, result)
		got := &spiv1alpha1.StoragePolicyInfo{}
		require.NoError(t, watchCache.Get(ctx, key, got))
		require.NotNil(t, got.Status.Topology)
		return got.Status.Topology.AccessibleZones
	}
	r := newTestReconciler(newCacheZonesProvider(watchCache))

	// Reconcile triggered by the Zone create: no active cluster in az1 yet.
	assert.Empty(t, reconcileAndGetZones(r))

	// WCP fills in spec.namespace.clusterMoIDs. The watch delivers this update, which must
	// enqueue the SPI.
	updatedZone := &unstructured.Unstructured{}
	updatedZone.SetGroupVersionKind(zoneGVK)
	require.NoError(t, watchCache.Get(ctx, types.NamespacedName{Namespace: "ns1", Name: "az1"}, updatedZone))
	oldZone := updatedZone.DeepCopy()
	require.NoError(t, unstructured.SetNestedStringSlice(updatedZone.Object, []string{"domain-c9"},
		"spec", "namespace", "clusterMoIDs"))
	require.NoError(t, watchCache.Update(ctx, updatedZone))
	require.True(t, zonePredicates().Update(event.UpdateEvent{ObjectOld: oldZone, ObjectNew: updatedZone}),
		"the clusterMoIDs update must enqueue the SPI")

	// Reading zones from the lagging informer for that reconcile drops az1 (the original bug).
	assert.Empty(t, reconcileAndGetZones(newTestReconciler(newCacheZonesProvider(laggingInformer))),
		"precondition: a reconcile reading the lagging informer misses the update")

	// Reading from the cache that delivered the update, as the controller does, picks it up.
	assert.Equal(t, []string{"az1"}, reconcileAndGetZones(r))
}

// TestReconcile_ZoneReadErrorFailsReconcile verifies that a failure to read the namespace's Zone
// CRs fails and requeues the reconcile instead of publishing empty accessible zones.
func TestReconcile_ZoneReadErrorFailsReconcile(t *testing.T) {
	ctx := logger.NewContextWithLogger(context.Background())
	scheme := zoneTestScheme(t)
	key := types.NamespacedName{Namespace: "ns1", Name: "gold"}

	cli := fake.NewClientBuilder().WithScheme(scheme).
		WithStatusSubresource(&spiv1alpha1.StoragePolicyInfo{}).
		WithObjects(&spiv1alpha1.StoragePolicyInfo{ObjectMeta: metav1.ObjectMeta{Name: "gold", Namespace: "ns1"}},
			&infraspiv1alpha1.InfraStoragePolicyInfo{
				ObjectMeta: metav1.ObjectMeta{Name: "gold", UID: types.UID("gold-uid")},
				Status: infraspiv1alpha1.InfraStoragePolicyInfoStatus{
					Topology:           &infraspiv1alpha1.Topology{AccessibleZones: []string{"az1"}},
					VolumeCapabilities: determinedInfraVolumeCapabilities(true, false),
				},
			}, testSPQ("ns1", "gold"), testCanonicalClusterSPI("gold")).Build()
	failingZones := fake.NewClientBuilder().WithScheme(scheme).WithInterceptorFuncs(interceptor.Funcs{
		List: func(_ context.Context, _ client.WithWatch, _ client.ObjectList, _ ...client.ListOption) error {
			return errors.New("cache not synced")
		},
	}).Build()
	r := &ReconcileStoragePolicyInfo{
		client: cli, scheme: scheme, recorder: record.NewFakeRecorder(10),
		zonesProvider:   newCacheZonesProvider(failingZones),
		backOffDuration: make(map[types.NamespacedName]time.Duration),
	}

	result, err := r.Reconcile(ctx, reconcile.Request{NamespacedName: key})
	require.NoError(t, err)
	assert.Positive(t, result.RequeueAfter, "a zone read failure must requeue")

	got := &spiv1alpha1.StoragePolicyInfo{}
	require.NoError(t, cli.Get(ctx, key, got))
	assert.Contains(t, got.Status.Error, "failed to get zones for namespace \"ns1\"")
	assert.Nil(t, got.Status.Topology, "topology must not be published from a failed zone read")
}
