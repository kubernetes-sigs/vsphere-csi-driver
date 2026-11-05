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

package clusterstoragepolicyinfo

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/vmware/govmomi"
	vimtypes "github.com/vmware/govmomi/vim25/types"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	apitypes "k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	infraspiv1alpha1 "sigs.k8s.io/vsphere-csi-driver/v3/pkg/apis/cnsoperator/infrastoragepolicyinfo/v1alpha1"
	cnsvsphere "sigs.k8s.io/vsphere-csi-driver/v3/pkg/common/cns-lib/vsphere"
	"sigs.k8s.io/vsphere-csi-driver/v3/pkg/csi/service/logger"
	"sigs.k8s.io/vsphere-csi-driver/v3/pkg/syncer/cnsoperator/vsphereinfra"
)

func dsSet(ids ...string) map[string]struct{} {
	m := make(map[string]struct{}, len(ids))
	for _, id := range ids {
		m[id] = struct{}{}
	}
	return m
}

func TestNewDatastoreSummary(t *testing.T) {
	normal := string(vimtypes.DatastoreSummaryMaintenanceModeStateNormal)
	tests := []struct {
		name       string
		accessible bool
		mm         string
		usable     bool
	}{
		{"accessible and normal", true, normal, true},
		{"accessible, maintenance mode unset", true, "", true},
		{"inaccessible", false, normal, false},
		{"entering maintenance", true, string(vimtypes.DatastoreSummaryMaintenanceModeStateEnteringMaintenance), false},
		{"in maintenance", true, string(vimtypes.DatastoreSummaryMaintenanceModeStateInMaintenance), false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := newDatastoreSummary(vimtypes.DatastoreSummary{
				Capacity: 100, FreeSpace: 40, Accessible: tt.accessible, MaintenanceMode: tt.mm})
			assert.Equal(t, tt.usable, s.usable)
			assert.EqualValues(t, 100, s.capacity)
			assert.EqualValues(t, 40, s.freeSpace)
		})
	}
}

func TestComputeZoneCapacity(t *testing.T) {
	summaries := map[string]datastoreSummary{
		"ds-1":      {capacity: 100, freeSpace: 40, usable: true},
		"ds-2":      {capacity: 200, freeSpace: 50, usable: true},
		"ds-shared": {capacity: 1000, freeSpace: 500, usable: true},
		"ds-down":   {capacity: 999, freeSpace: 999, usable: false},
	}

	t.Run("sums per zone and counts a shared datastore in each zone", func(t *testing.T) {
		got := computeZoneCapacity(map[string]map[string]struct{}{
			"zone-b": dsSet("ds-2", "ds-shared"),
			"zone-a": dsSet("ds-1", "ds-shared"),
		}, summaries)
		require.Len(t, got, 2)
		assert.Equal(t, "zone-a", got[0].Zone, "result must be sorted by zone")
		assert.EqualValues(t, 1100, got[0].Capacity.Value())
		assert.EqualValues(t, 540, got[0].FreeSpace.Value())
		assert.Equal(t, "zone-b", got[1].Zone)
		assert.EqualValues(t, 1200, got[1].Capacity.Value())
		assert.EqualValues(t, 550, got[1].FreeSpace.Value())
	})

	t.Run("unusable and missing datastores are ignored", func(t *testing.T) {
		got := computeZoneCapacity(map[string]map[string]struct{}{
			"zone-a": dsSet("ds-1", "ds-down", "ds-deleted"),
		}, summaries)
		require.Len(t, got, 1)
		assert.EqualValues(t, 100, got[0].Capacity.Value())
		assert.EqualValues(t, 40, got[0].FreeSpace.Value())
	})

	t.Run("zones without a usable datastore are omitted", func(t *testing.T) {
		got := computeZoneCapacity(map[string]map[string]struct{}{
			"zone-empty":   dsSet(),
			"zone-down":    dsSet("ds-down"),
			"zone-deleted": dsSet("ds-deleted"),
		}, summaries)
		assert.NotNil(t, got)
		assert.Empty(t, got)
	})

	t.Run("nil input yields empty non-nil result", func(t *testing.T) {
		got := computeZoneCapacity(nil, nil)
		assert.NotNil(t, got)
		assert.Empty(t, got)
	})
}

func TestZoneCapacityForPolicy_NotInCache(t *testing.T) {
	ctx, _ := logger.GetNewContextWithLogger()
	// A nil vCenter client is fine: a policy absent from the cache must return before any vCenter call.
	zc, found, err := zoneCapacityForPolicy(ctx, nil, "capacity-test-never-cached")
	assert.NoError(t, err)
	assert.False(t, found, "an uncached policy must be reported as not found, not as empty")
	assert.Nil(t, zc)
}

func TestZoneCapacityForPolicy_CachedWithNoDatastores(t *testing.T) {
	ctx, _ := logger.GetNewContextWithLogger()
	const policy = "capacity-test-cached-empty"
	vsphereinfra.GetCache().SetDatastoresForPolicyZones(policy, map[string][]string{})
	defer vsphereinfra.GetCache().SetDatastoresForPolicyZones(policy, nil)

	// No datastores means no vCenter call, so a nil client is safe here too.
	zc, found, err := zoneCapacityForPolicy(ctx, nil, policy)
	assert.NoError(t, err)
	assert.True(t, found, "a cached policy with no datastores must be reported as found")
	assert.NotNil(t, zc)
	assert.Empty(t, zc)
}

func TestZoneCapacityForPolicy_NilClientWithDatastores(t *testing.T) {
	ctx, _ := logger.GetNewContextWithLogger()
	const policy = "capacity-test-nil-client"
	vsphereinfra.GetCache().SetDatastoresForPolicyZones(policy, map[string][]string{"zone-a": {"ds-1"}})
	defer vsphereinfra.GetCache().SetDatastoresForPolicyZones(policy, nil)

	zc, found, err := zoneCapacityForPolicy(ctx, nil, policy)
	assert.Error(t, err, "datastores need reading but there is no client: must error, not panic")
	assert.True(t, found)
	assert.Nil(t, zc)
}

func TestPopulateCapacity_NotInCacheLeavesStatusUntouched(t *testing.T) {
	ctx, _ := logger.GetNewContextWithLogger()
	observed := metav1.NewTime(time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC))
	existing := []infraspiv1alpha1.ZoneCapacity{{Zone: "zone-a"}}
	infraSPI := &infraspiv1alpha1.InfraStoragePolicyInfo{
		ObjectMeta: metav1.ObjectMeta{Name: "capacity-test-not-cached"},
		Status: infraspiv1alpha1.InfraStoragePolicyInfoStatus{
			ZoneCapacity:         existing,
			CapacityObservedTime: &observed,
		},
	}
	r := &ReconcileClusterStoragePolicyInfo{}

	// The policy isn't cached, so no vCenter call is made. populateCapacity does read vc.Client.Client
	// to pass along, so vc needs a non-nil Client, but that inner client can stay nil.
	vc := &cnsvsphere.VirtualCenter{Client: &govmomi.Client{}}
	require.NoError(t, r.populateCapacity(ctx, infraSPI, vc))

	assert.Equal(t, existing, infraSPI.Status.ZoneCapacity)
	assert.True(t, observed.Equal(infraSPI.Status.CapacityObservedTime))
}

func TestPublishCapacity(t *testing.T) {
	ctx, _ := logger.GetNewContextWithLogger()
	scheme := testScheme(t)
	zc := computeZoneCapacity(map[string]map[string]struct{}{"zone-a": dsSet("ds-1")},
		map[string]datastoreSummary{"ds-1": {capacity: 100, freeSpace: 40, usable: true}})
	observed := metav1.NewTime(time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC))

	t.Run("patches only capacity fields and preserves the rest of the status", func(t *testing.T) {
		existing := &infraspiv1alpha1.InfraStoragePolicyInfo{
			ObjectMeta: metav1.ObjectMeta{Name: "policy-1"},
			Status: infraspiv1alpha1.InfraStoragePolicyInfoStatus{
				Topology: &infraspiv1alpha1.Topology{TopologyType: "zonal", AccessibleZones: []string{"zone-a"}},
				Error:    "keep me",
			},
		}
		cli := fake.NewClientBuilder().WithScheme(scheme).WithObjects(existing).
			WithStatusSubresource(existing).Build()
		r := &ReconcileClusterStoragePolicyInfo{client: cli}

		require.NoError(t, r.publishCapacity(ctx, "policy-1", zc, observed))

		got := &infraspiv1alpha1.InfraStoragePolicyInfo{}
		require.NoError(t, cli.Get(ctx, apitypes.NamespacedName{Name: "policy-1"}, got))
		require.Len(t, got.Status.ZoneCapacity, 1)
		assert.Equal(t, "zone-a", got.Status.ZoneCapacity[0].Zone)
		assert.EqualValues(t, 100, got.Status.ZoneCapacity[0].Capacity.Value())
		require.NotNil(t, got.Status.CapacityObservedTime)
		assert.True(t, observed.Equal(got.Status.CapacityObservedTime))
		assert.Equal(t, []string{"zone-a"}, got.Status.Topology.AccessibleZones)
		assert.Equal(t, "keep me", got.Status.Error)
	})

	t.Run("an empty result clears previously published zones", func(t *testing.T) {
		existing := &infraspiv1alpha1.InfraStoragePolicyInfo{
			ObjectMeta: metav1.ObjectMeta{Name: "policy-1"},
			Status:     infraspiv1alpha1.InfraStoragePolicyInfoStatus{ZoneCapacity: zc},
		}
		cli := fake.NewClientBuilder().WithScheme(scheme).WithObjects(existing).
			WithStatusSubresource(existing).Build()
		r := &ReconcileClusterStoragePolicyInfo{client: cli}

		require.NoError(t, r.publishCapacity(ctx, "policy-1", []infraspiv1alpha1.ZoneCapacity{}, observed))

		got := &infraspiv1alpha1.InfraStoragePolicyInfo{}
		require.NoError(t, cli.Get(ctx, apitypes.NamespacedName{Name: "policy-1"}, got))
		assert.Empty(t, got.Status.ZoneCapacity)
		require.NotNil(t, got.Status.CapacityObservedTime)
	})

	t.Run("does not replace capacity observed at the same time or later", func(t *testing.T) {
		newer := []infraspiv1alpha1.ZoneCapacity{{Zone: "zone-newer"}}
		for _, publishedAt := range []metav1.Time{observed, metav1.NewTime(observed.Add(time.Minute))} {
			existing := &infraspiv1alpha1.InfraStoragePolicyInfo{
				ObjectMeta: metav1.ObjectMeta{Name: "policy-1"},
				Status: infraspiv1alpha1.InfraStoragePolicyInfoStatus{
					ZoneCapacity:         newer,
					CapacityObservedTime: &publishedAt,
				},
			}
			cli := fake.NewClientBuilder().WithScheme(scheme).WithObjects(existing).
				WithStatusSubresource(existing).Build()
			r := &ReconcileClusterStoragePolicyInfo{client: cli}

			require.NoError(t, r.publishCapacity(ctx, "policy-1", zc, observed))

			got := &infraspiv1alpha1.InfraStoragePolicyInfo{}
			require.NoError(t, cli.Get(ctx, apitypes.NamespacedName{Name: "policy-1"}, got))
			require.Len(t, got.Status.ZoneCapacity, 1)
			assert.Equal(t, "zone-newer", got.Status.ZoneCapacity[0].Zone)
			assert.True(t, publishedAt.Equal(got.Status.CapacityObservedTime))
		}
	})

	t.Run("replaces capacity observed earlier", func(t *testing.T) {
		earlier := metav1.NewTime(observed.Add(-time.Hour))
		existing := &infraspiv1alpha1.InfraStoragePolicyInfo{
			ObjectMeta: metav1.ObjectMeta{Name: "policy-1"},
			Status: infraspiv1alpha1.InfraStoragePolicyInfoStatus{
				ZoneCapacity:         []infraspiv1alpha1.ZoneCapacity{{Zone: "zone-old"}},
				CapacityObservedTime: &earlier,
			},
		}
		cli := fake.NewClientBuilder().WithScheme(scheme).WithObjects(existing).
			WithStatusSubresource(existing).Build()
		r := &ReconcileClusterStoragePolicyInfo{client: cli}

		require.NoError(t, r.publishCapacity(ctx, "policy-1", zc, observed))

		got := &infraspiv1alpha1.InfraStoragePolicyInfo{}
		require.NoError(t, cli.Get(ctx, apitypes.NamespacedName{Name: "policy-1"}, got))
		require.Len(t, got.Status.ZoneCapacity, 1)
		assert.Equal(t, "zone-a", got.Status.ZoneCapacity[0].Zone)
		assert.True(t, observed.Equal(got.Status.CapacityObservedTime))
	})

	t.Run("a deleted policy is not resurrected", func(t *testing.T) {
		cli := fake.NewClientBuilder().WithScheme(scheme).Build()
		r := &ReconcileClusterStoragePolicyInfo{client: cli}

		require.NoError(t, r.publishCapacity(ctx, "gone", zc, observed))

		got := &infraspiv1alpha1.InfraStoragePolicyInfo{}
		assert.Error(t, cli.Get(ctx, apitypes.NamespacedName{Name: "gone"}, got))
	})
}
