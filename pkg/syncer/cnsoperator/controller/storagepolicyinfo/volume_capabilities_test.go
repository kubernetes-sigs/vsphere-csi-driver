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

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	infraspiv1alpha1 "sigs.k8s.io/vsphere-csi-driver/v3/pkg/apis/cnsoperator/infrastoragepolicyinfo/v1alpha1"
	spiv1alpha1 "sigs.k8s.io/vsphere-csi-driver/v3/pkg/apis/cnsoperator/storagepolicyinfo/v1alpha1"
	"sigs.k8s.io/vsphere-csi-driver/v3/pkg/csi/service/logger"
	"sigs.k8s.io/vsphere-csi-driver/v3/pkg/syncer/cnsoperator/vsphereinfra"
)

// clearPolicyZoneCache clears the process-wide vsphereinfra cache entries this test wrote,
// so cases in this file can't leak state into each other or into other packages' tests
// sharing the same singleton.
func clearPolicyZoneCache(t *testing.T, policyName string) {
	t.Helper()
	t.Cleanup(func() { vsphereinfra.GetCache().SetDatastoresForPolicyZones(policyName, nil) })
}

// zoneAActiveClusters returns an activeClustersByZone map where zone-a's active cluster is
// "cluster-for-zone-a" (mirroring mockZonesProvider's naming convention).
func zoneAActiveClusters() map[string]map[string]bool {
	return map[string]map[string]bool{"zone-a": {"cluster-for-zone-a": true}}
}

// zoneAZonesProvider is a mockZonesProvider where the given namespace is assigned and active
// on zone-a, with active cluster moref "cluster-for-zone-a" (mockZonesProvider's convention).
func zoneAZonesProvider(namespace string) *mockZonesProvider {
	return &mockZonesProvider{zonesForNamespace: map[string]map[string]struct{}{
		namespace: {"zone-a": {}},
	}}
}

func TestAnyQualifyingHostForNamespace_NoZones(t *testing.T) {
	ctx := logger.NewContextWithLogger(context.Background())
	ok, err := anyQualifyingHostForNamespace(ctx, nil, "test-op", "policy-1", nil,
		func(context.Context, string) (bool, error) { return true, nil })
	require.NoError(t, err)
	assert.False(t, ok)
}

func TestAnyQualifyingHostForNamespace_NoActiveClusterInZone(t *testing.T) {
	ctx := logger.NewContextWithLogger(context.Background())
	const policyName = "policy-anqhfn-noactivecluster"
	clearPolicyZoneCache(t, policyName)

	// No entry for zone-a in activeClustersByZone means the namespace has no active cluster
	// there, so the zone is skipped before datastores are even consulted.
	ok, err := anyQualifyingHostForNamespace(ctx, map[string]map[string]bool{}, "test-op",
		policyName, []string{"zone-a"},
		func(context.Context, string) (bool, error) { return true, nil })
	require.NoError(t, err)
	assert.False(t, ok)
}

func TestAnyQualifyingHostForNamespace_ZoneNotCached(t *testing.T) {
	ctx := logger.NewContextWithLogger(context.Background())
	ok, err := anyQualifyingHostForNamespace(ctx, zoneAActiveClusters(), "test-op",
		"policy-never-cached", []string{"zone-a"},
		func(context.Context, string) (bool, error) { return true, nil })
	require.NoError(t, err)
	assert.False(t, ok)
}

func TestAnyQualifyingHostForNamespace_DatastoreNotInDsToHosts(t *testing.T) {
	ctx := logger.NewContextWithLogger(context.Background())
	const policyName = "policy-anqhfn-1"
	clearPolicyZoneCache(t, policyName)

	vsphereinfra.GetCache().SetDatastoresForPolicyZones(policyName, map[string][]string{"zone-a": {"ds-1"}})
	// Deliberately not populating DsToHosts for ds-1.

	ok, err := anyQualifyingHostForNamespace(ctx, zoneAActiveClusters(), "test-op",
		policyName, []string{"zone-a"},
		func(context.Context, string) (bool, error) { return true, nil })
	require.NoError(t, err)
	assert.False(t, ok)
}

func TestAnyQualifyingHostForNamespace_QualifyingHostFound(t *testing.T) {
	ctx := logger.NewContextWithLogger(context.Background())
	const policyName = "policy-anqhfn-2"
	const clusterID = "cluster-for-zone-a"
	clearPolicyZoneCache(t, policyName)
	t.Cleanup(func() { vsphereinfra.GetCache().UpdateDsHosts("ds-anqhfn-2", map[string]struct{}{}) })
	t.Cleanup(func() { vsphereinfra.GetCache().InvalidateCluster(clusterID) })

	vsphereinfra.GetCache().SetDatastoresForPolicyZones(policyName, map[string][]string{"zone-a": {"ds-anqhfn-2"}})
	vsphereinfra.GetCache().UpdateDsHosts("ds-anqhfn-2", map[string]struct{}{"host-1": {}, "host-2": {}})
	vsphereinfra.GetCache().UpdateClusterHosts(clusterID, map[string]struct{}{"host-1": {}, "host-2": {}})

	ok, err := anyQualifyingHostForNamespace(ctx, zoneAActiveClusters(), "test-op",
		policyName, []string{"zone-a"},
		func(_ context.Context, hostID string) (bool, error) { return hostID == "host-2", nil })
	require.NoError(t, err)
	assert.True(t, ok)
}

func TestAnyQualifyingHostForNamespace_HostClusterNotActiveForNamespace(t *testing.T) {
	ctx := logger.NewContextWithLogger(context.Background())
	const policyName = "policy-anqhfn-inactivecluster"
	const clusterID = "cluster-not-active"
	clearPolicyZoneCache(t, policyName)
	t.Cleanup(func() { vsphereinfra.GetCache().UpdateDsHosts("ds-anqhfn-inactivecluster", map[string]struct{}{}) })
	t.Cleanup(func() { vsphereinfra.GetCache().InvalidateCluster(clusterID) })

	vsphereinfra.GetCache().SetDatastoresForPolicyZones(policyName,
		map[string][]string{"zone-a": {"ds-anqhfn-inactivecluster"}})
	vsphereinfra.GetCache().UpdateDsHosts("ds-anqhfn-inactivecluster", map[string]struct{}{"host-1": {}})
	// host-1 belongs to a cluster that is not the namespace's active cluster for zone-a
	// ("cluster-for-zone-a"), so it must be skipped even though qualifies would otherwise
	// return true.
	vsphereinfra.GetCache().UpdateClusterHosts(clusterID, map[string]struct{}{"host-1": {}})

	ok, err := anyQualifyingHostForNamespace(ctx, zoneAActiveClusters(), "test-op",
		policyName, []string{"zone-a"},
		func(context.Context, string) (bool, error) { return true, nil })
	require.NoError(t, err)
	assert.False(t, ok)
}

func TestAnyQualifyingHostForNamespace_NoQualifyingHost(t *testing.T) {
	ctx := logger.NewContextWithLogger(context.Background())
	const policyName = "policy-anqhfn-3"
	const clusterID = "cluster-for-zone-a"
	clearPolicyZoneCache(t, policyName)
	t.Cleanup(func() { vsphereinfra.GetCache().UpdateDsHosts("ds-anqhfn-3", map[string]struct{}{}) })
	t.Cleanup(func() { vsphereinfra.GetCache().InvalidateCluster(clusterID) })

	vsphereinfra.GetCache().SetDatastoresForPolicyZones(policyName, map[string][]string{"zone-a": {"ds-anqhfn-3"}})
	vsphereinfra.GetCache().UpdateDsHosts("ds-anqhfn-3", map[string]struct{}{"host-1": {}})
	vsphereinfra.GetCache().UpdateClusterHosts(clusterID, map[string]struct{}{"host-1": {}})

	ok, err := anyQualifyingHostForNamespace(ctx, zoneAActiveClusters(), "test-op",
		policyName, []string{"zone-a"},
		func(context.Context, string) (bool, error) { return false, nil })
	require.NoError(t, err)
	assert.False(t, ok)
}

func TestAnyQualifyingHostForNamespace_QualifiesErrorPropagates(t *testing.T) {
	ctx := logger.NewContextWithLogger(context.Background())
	const policyName = "policy-anqhfn-4"
	const clusterID = "cluster-for-zone-a"
	clearPolicyZoneCache(t, policyName)
	t.Cleanup(func() { vsphereinfra.GetCache().UpdateDsHosts("ds-anqhfn-4", map[string]struct{}{}) })
	t.Cleanup(func() { vsphereinfra.GetCache().InvalidateCluster(clusterID) })

	vsphereinfra.GetCache().SetDatastoresForPolicyZones(policyName, map[string][]string{"zone-a": {"ds-anqhfn-4"}})
	vsphereinfra.GetCache().UpdateDsHosts("ds-anqhfn-4", map[string]struct{}{"host-1": {}})
	vsphereinfra.GetCache().UpdateClusterHosts(clusterID, map[string]struct{}{"host-1": {}})

	wantErr := errors.New("boom")
	ok, err := anyQualifyingHostForNamespace(ctx, zoneAActiveClusters(), "test-op",
		policyName, []string{"zone-a"},
		func(context.Context, string) (bool, error) { return false, wantErr })
	assert.ErrorIs(t, err, wantErr)
	assert.False(t, ok)
}

func TestHostSupportsLinkedClone_NotObserved(t *testing.T) {
	ctx := logger.NewContextWithLogger(context.Background())
	ok, err := hostSupportsLinkedClone(ctx, "host-never-observed")
	require.NoError(t, err)
	assert.False(t, ok)
}

func TestHostSupportsLinkedClone_VersionBelow91(t *testing.T) {
	ctx := logger.NewContextWithLogger(context.Background())
	t.Cleanup(func() { vsphereinfra.GetCache().InvalidateHostVersion("host-hslc-old") })
	vsphereinfra.GetCache().UpdateHostVersion("host-hslc-old", "8.0.3")

	ok, err := hostSupportsLinkedClone(ctx, "host-hslc-old")
	require.NoError(t, err)
	assert.False(t, ok)
}

func TestHostSupportsLinkedClone_VersionAtOrAbove91(t *testing.T) {
	ctx := logger.NewContextWithLogger(context.Background())
	t.Cleanup(func() { vsphereinfra.GetCache().InvalidateHostVersion("host-hslc-new") })
	vsphereinfra.GetCache().UpdateHostVersion("host-hslc-new", "9.1.0")

	ok, err := hostSupportsLinkedClone(ctx, "host-hslc-new")
	require.NoError(t, err)
	assert.True(t, ok)
}

func TestHostSupportsHighPerformanceLinkedClone_RequiresLinkedCloneFirst(t *testing.T) {
	ctx := logger.NewContextWithLogger(context.Background())
	t.Cleanup(func() { vsphereinfra.GetCache().InvalidateHostVersion("host-hphlc-old") })
	// Below 9.1: hostSupportsLinkedClone is false, so HPLC must short-circuit to false without
	// ever needing ClusterForHost/GetClusterESAEnabled to be populated.
	vsphereinfra.GetCache().UpdateHostVersion("host-hphlc-old", "8.0.3")

	ok, err := hostSupportsHighPerformanceLinkedClone(ctx, "host-hphlc-old")
	require.NoError(t, err)
	assert.False(t, ok)
}

func TestHostSupportsHighPerformanceLinkedClone_NoClusterMapping(t *testing.T) {
	ctx := logger.NewContextWithLogger(context.Background())
	t.Cleanup(func() { vsphereinfra.GetCache().InvalidateHostVersion("host-hphlc-nocluster") })
	vsphereinfra.GetCache().UpdateHostVersion("host-hphlc-nocluster", "9.1.0")
	// Deliberately not populating ClusterToHosts for this host.

	ok, err := hostSupportsHighPerformanceLinkedClone(ctx, "host-hphlc-nocluster")
	require.NoError(t, err)
	assert.False(t, ok)
}

func TestHostSupportsHighPerformanceLinkedClone_ESANotEnabled(t *testing.T) {
	ctx := logger.NewContextWithLogger(context.Background())
	const hostID, clusterID = "host-hphlc-noesa", "cluster-hphlc-noesa"
	t.Cleanup(func() {
		vsphereinfra.GetCache().InvalidateHostVersion(hostID)
		vsphereinfra.GetCache().InvalidateCluster(clusterID)
	})
	vsphereinfra.GetCache().UpdateHostVersion(hostID, "9.1.0")
	vsphereinfra.GetCache().UpdateClusterHosts(clusterID, map[string]struct{}{hostID: {}})
	vsphereinfra.GetCache().SetClusterESAEnabled(clusterID, false)

	ok, err := hostSupportsHighPerformanceLinkedClone(ctx, hostID)
	require.NoError(t, err)
	assert.False(t, ok)
}

func TestHostSupportsHighPerformanceLinkedClone_ESAEnabled(t *testing.T) {
	ctx := logger.NewContextWithLogger(context.Background())
	const hostID, clusterID = "host-hphlc-esa", "cluster-hphlc-esa"
	t.Cleanup(func() {
		vsphereinfra.GetCache().InvalidateHostVersion(hostID)
		vsphereinfra.GetCache().InvalidateCluster(clusterID)
	})
	vsphereinfra.GetCache().UpdateHostVersion(hostID, "9.1.0")
	vsphereinfra.GetCache().UpdateClusterHosts(clusterID, map[string]struct{}{hostID: {}})
	vsphereinfra.GetCache().SetClusterESAEnabled(clusterID, true)

	ok, err := hostSupportsHighPerformanceLinkedClone(ctx, hostID)
	require.NoError(t, err)
	assert.True(t, ok)
}

// TestLinkedCloneCapabilitiesForNamespace_NilTopologyInfoReturnsError verifies that a nil
// TopologyInfo — which only happens when InfraStoragePolicyInfo failed to resolve its own
// topology, never as a legitimate non-zonal state — is surfaced as an error rather than
// silently falling back to (equally untrustworthy) infraCaps.
func TestLinkedCloneCapabilitiesForNamespace_NilTopologyInfoReturnsError(t *testing.T) {
	ctx := logger.NewContextWithLogger(context.Background())
	instance := &spiv1alpha1.StoragePolicyInfo{
		ObjectMeta: metav1.ObjectMeta{Name: "policy-lccfn-notopology"},
		// TopologyInfo is deliberately nil.
	}
	infraSPI := &infraspiv1alpha1.InfraStoragePolicyInfo{ObjectMeta: metav1.ObjectMeta{Name: "policy-lccfn-notopology"}}

	lc, hplc, err := linkedCloneCapabilitiesForNamespace(ctx, &mockZonesProvider{}, nil, instance, infraSPI)
	assert.Error(t, err)
	assert.False(t, lc)
	assert.False(t, hplc)
}

func TestLinkedCloneCapabilitiesForNamespace_ZonalRecomputesFromCache(t *testing.T) {
	ctx := logger.NewContextWithLogger(context.Background())
	const policyName = "policy-lccfn-zonal"
	const namespace = "ns-lccfn-zonal"
	const clusterID = "cluster-for-zone-a"
	clearPolicyZoneCache(t, policyName)
	t.Cleanup(func() { vsphereinfra.GetCache().UpdateDsHosts("ds-lccfn-zonal", map[string]struct{}{}) })
	t.Cleanup(func() { vsphereinfra.GetCache().InvalidateHostVersion("host-lccfn-zonal") })
	t.Cleanup(func() { vsphereinfra.GetCache().InvalidateCluster(clusterID) })

	vsphereinfra.GetCache().SetDatastoresForPolicyZones(policyName, map[string][]string{"zone-a": {"ds-lccfn-zonal"}})
	vsphereinfra.GetCache().UpdateDsHosts("ds-lccfn-zonal", map[string]struct{}{"host-lccfn-zonal": {}})
	vsphereinfra.GetCache().UpdateHostVersion("host-lccfn-zonal", "9.1.0")
	vsphereinfra.GetCache().UpdateClusterHosts(clusterID, map[string]struct{}{"host-lccfn-zonal": {}})

	instance := &spiv1alpha1.StoragePolicyInfo{
		ObjectMeta: metav1.ObjectMeta{Name: policyName, Namespace: namespace},
		Status: spiv1alpha1.StoragePolicyInfoStatus{
			TopologyInfo: &spiv1alpha1.Topology{TopologyType: "zonal", AccessibleZones: []string{"zone-a"}},
		},
	}
	infraSPI := &infraspiv1alpha1.InfraStoragePolicyInfo{ObjectMeta: metav1.ObjectMeta{Name: policyName}}

	// activeClustersByZone is nil here to exercise the fresh-lookup fallback path (as marker
	// policies use), rather than a precomputed map from namespaceFilteredZones.
	lc, hplc, err := linkedCloneCapabilitiesForNamespace(ctx, zoneAZonesProvider(namespace), nil, instance, infraSPI)
	require.NoError(t, err)
	assert.True(t, lc, "zone-a has a compatible datastore mounted by an ESXi 9.1+ host in the namespace's active cluster")
	assert.False(t, hplc, "no vSAN-ESA cluster was configured for this host")
}

func TestSyncVolumeCapabilitiesFromInfraSPI_CopiesBlockAndFilesystemCapabilities(t *testing.T) {
	ctx := logger.NewContextWithLogger(context.Background())
	instance := &spiv1alpha1.StoragePolicyInfo{
		ObjectMeta: metav1.ObjectMeta{Name: "policy-svcfi"},
		Status: spiv1alpha1.StoragePolicyInfoStatus{
			// Non-nil but zoneless, as syncTopologyFromInfraSPI would set for a non-zonal
			// policy; only nil TopologyInfo (an unresolved upstream topology) is an error.
			TopologyInfo: &spiv1alpha1.Topology{},
		},
	}
	infraSPI := &infraspiv1alpha1.InfraStoragePolicyInfo{
		ObjectMeta: metav1.ObjectMeta{Name: "policy-svcfi"},
		Status: infraspiv1alpha1.InfraStoragePolicyInfoStatus{
			VolumeCapabilities: map[infraspiv1alpha1.VolumeCapability]bool{
				infraspiv1alpha1.SupportsVolumeModeBlock: true,
			},
		},
	}

	r := &ReconcileStoragePolicyInfo{zonesProvider: &mockZonesProvider{}}
	err := r.syncVolumeCapabilitiesFromInfraSPI(ctx, instance, infraSPI, nil)
	require.NoError(t, err)
	assert.True(t, instance.Status.VolumeCapabilities[spiv1alpha1.SupportsVolumeModeFilesystem],
		"SupportsVolumeModeFilesystem is always true, independent of InfraSPI")
	assert.True(t, instance.Status.VolumeCapabilities[spiv1alpha1.SupportsVolumeModeBlock])
	assert.False(t, instance.Status.VolumeCapabilities[spiv1alpha1.SupportsLinkedClone])
	assert.False(t, instance.Status.VolumeCapabilities[spiv1alpha1.SupportsHighPerformanceLinkedClone])
}
