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
	"sigs.k8s.io/vsphere-csi-driver/v3/pkg/csi/service/common"
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

func TestQualifyingZonesForNamespace_NoZones(t *testing.T) {
	ctx := logger.NewContextWithLogger(context.Background())
	zones, err := qualifyingZonesForNamespace(ctx, nil, "test-op", "policy-1", nil,
		func(context.Context, string) (bool, error) { return true, nil })
	require.NoError(t, err)
	assert.Empty(t, zones)
}

func TestQualifyingZonesForNamespace_NoActiveClusterInZone(t *testing.T) {
	ctx := logger.NewContextWithLogger(context.Background())
	const policyName = "policy-anqhfn-noactivecluster"
	clearPolicyZoneCache(t, policyName)

	// No entry for zone-a in activeClustersByZone means the namespace has no active cluster
	// there, so the zone is skipped before datastores are even consulted.
	zones, err := qualifyingZonesForNamespace(ctx, map[string]map[string]bool{}, "test-op",
		policyName, []string{"zone-a"},
		func(context.Context, string) (bool, error) { return true, nil })
	require.NoError(t, err)
	assert.Empty(t, zones)
}

func TestQualifyingZonesForNamespace_ZoneNotCached(t *testing.T) {
	ctx := logger.NewContextWithLogger(context.Background())
	zones, err := qualifyingZonesForNamespace(ctx, zoneAActiveClusters(), "test-op",
		"policy-never-cached", []string{"zone-a"},
		func(context.Context, string) (bool, error) { return true, nil })
	require.NoError(t, err)
	assert.Empty(t, zones)
}

func TestQualifyingZonesForNamespace_DatastoreNotInDsToHosts(t *testing.T) {
	ctx := logger.NewContextWithLogger(context.Background())
	const policyName = "policy-anqhfn-1"
	clearPolicyZoneCache(t, policyName)

	vsphereinfra.GetCache().SetDatastoresForPolicyZones(policyName, map[string][]string{"zone-a": {"ds-1"}})
	// Deliberately not populating DsToHosts for ds-1.

	zones, err := qualifyingZonesForNamespace(ctx, zoneAActiveClusters(), "test-op",
		policyName, []string{"zone-a"},
		func(context.Context, string) (bool, error) { return true, nil })
	require.NoError(t, err)
	assert.Empty(t, zones)
}

func TestQualifyingZonesForNamespace_QualifyingHostFound(t *testing.T) {
	ctx := logger.NewContextWithLogger(context.Background())
	const policyName = "policy-anqhfn-2"
	const clusterID = "cluster-for-zone-a"
	clearPolicyZoneCache(t, policyName)
	t.Cleanup(func() { vsphereinfra.GetCache().UpdateDsHosts("ds-anqhfn-2", map[string]struct{}{}) })
	t.Cleanup(func() { vsphereinfra.GetCache().InvalidateCluster(clusterID) })

	vsphereinfra.GetCache().SetDatastoresForPolicyZones(policyName, map[string][]string{"zone-a": {"ds-anqhfn-2"}})
	vsphereinfra.GetCache().UpdateDsHosts("ds-anqhfn-2", map[string]struct{}{"host-1": {}, "host-2": {}})
	vsphereinfra.GetCache().UpdateClusterHosts(clusterID, map[string]struct{}{"host-1": {}, "host-2": {}})

	zones, err := qualifyingZonesForNamespace(ctx, zoneAActiveClusters(), "test-op",
		policyName, []string{"zone-a"},
		func(_ context.Context, hostID string) (bool, error) { return hostID == "host-2", nil })
	require.NoError(t, err)
	assert.Equal(t, []string{"zone-a"}, zones)
}

func TestQualifyingZonesForNamespace_HostClusterNotActiveForNamespace(t *testing.T) {
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

	zones, err := qualifyingZonesForNamespace(ctx, zoneAActiveClusters(), "test-op",
		policyName, []string{"zone-a"},
		func(context.Context, string) (bool, error) { return true, nil })
	require.NoError(t, err)
	assert.Empty(t, zones)
}

func TestQualifyingZonesForNamespace_NoQualifyingHost(t *testing.T) {
	ctx := logger.NewContextWithLogger(context.Background())
	const policyName = "policy-anqhfn-3"
	const clusterID = "cluster-for-zone-a"
	clearPolicyZoneCache(t, policyName)
	t.Cleanup(func() { vsphereinfra.GetCache().UpdateDsHosts("ds-anqhfn-3", map[string]struct{}{}) })
	t.Cleanup(func() { vsphereinfra.GetCache().InvalidateCluster(clusterID) })

	vsphereinfra.GetCache().SetDatastoresForPolicyZones(policyName, map[string][]string{"zone-a": {"ds-anqhfn-3"}})
	vsphereinfra.GetCache().UpdateDsHosts("ds-anqhfn-3", map[string]struct{}{"host-1": {}})
	vsphereinfra.GetCache().UpdateClusterHosts(clusterID, map[string]struct{}{"host-1": {}})

	zones, err := qualifyingZonesForNamespace(ctx, zoneAActiveClusters(), "test-op",
		policyName, []string{"zone-a"},
		func(context.Context, string) (bool, error) { return false, nil })
	require.NoError(t, err)
	assert.Empty(t, zones)
}

func TestQualifyingZonesForNamespace_QualifiesErrorPropagates(t *testing.T) {
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
	zones, err := qualifyingZonesForNamespace(ctx, zoneAActiveClusters(), "test-op",
		policyName, []string{"zone-a"},
		func(context.Context, string) (bool, error) { return false, wantErr })
	assert.ErrorIs(t, err, wantErr)
	assert.Empty(t, zones)
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

// TestLinkedCloneZonesForNamespace_NilTopologyInfoReturnsError verifies that a nil
// TopologyInfo — which only happens when InfraStoragePolicyInfo failed to resolve its own
// topology, never as a legitimate non-zonal state — is surfaced as an error rather than
// silently falling back to InfraSPI's (equally untrustworthy) zonal capabilities.
func TestLinkedCloneZonesForNamespace_NilTopologyInfoReturnsError(t *testing.T) {
	ctx := logger.NewContextWithLogger(context.Background())
	instance := &spiv1alpha1.StoragePolicyInfo{
		ObjectMeta: metav1.ObjectMeta{Name: "policy-lccfn-notopology"},
		// TopologyInfo is deliberately nil.
	}
	infraSPI := &infraspiv1alpha1.InfraStoragePolicyInfo{ObjectMeta: metav1.ObjectMeta{Name: "policy-lccfn-notopology"}}

	lcZones, hplcZones, err := linkedCloneZonesForNamespace(ctx, &mockZonesProvider{}, nil, instance, infraSPI)
	assert.Error(t, err)
	assert.Empty(t, lcZones)
	assert.Empty(t, hplcZones)
}

func TestLinkedCloneZonesForNamespace_ZonalRecomputesFromCache(t *testing.T) {
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
	lcZones, hplcZones, err := linkedCloneZonesForNamespace(ctx, zoneAZonesProvider(namespace), nil, instance, infraSPI)
	require.NoError(t, err)
	assert.Equal(t, []string{"zone-a"}, lcZones,
		"zone-a has a compatible datastore mounted by an ESXi 9.1+ host in the namespace's active cluster")
	assert.Empty(t, hplcZones, "no vSAN-ESA cluster was configured for this host")
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
	assert.Equal(t, emptyZonalVolumeCapabilities(), instance.Status.ZonalVolumeCapabilities,
		"a zoneless policy reports every zonal capability with no zones")
	assert.False(t, instance.Status.VolumeCapabilities[spiv1alpha1.SupportsHostLocal],
		"SupportsHostLocal is copied as-is from InfraSPI, which did not set it here")
}

// TestSyncVolumeCapabilitiesFromInfraSPI_CopiesHostLocalCapability verifies that
// SupportsHostLocal is copied directly from InfraSPI, with no per-namespace recomputation.
func TestSyncVolumeCapabilitiesFromInfraSPI_CopiesHostLocalCapability(t *testing.T) {
	ctx := logger.NewContextWithLogger(context.Background())
	instance := &spiv1alpha1.StoragePolicyInfo{
		ObjectMeta: metav1.ObjectMeta{Name: "policy-svcfi-hostlocal"},
		Status: spiv1alpha1.StoragePolicyInfoStatus{
			TopologyInfo: &spiv1alpha1.Topology{},
		},
	}
	infraSPI := &infraspiv1alpha1.InfraStoragePolicyInfo{
		ObjectMeta: metav1.ObjectMeta{Name: "policy-svcfi-hostlocal"},
		Status: infraspiv1alpha1.InfraStoragePolicyInfoStatus{
			VolumeCapabilities: map[infraspiv1alpha1.VolumeCapability]bool{
				infraspiv1alpha1.SupportsHostLocal: true,
			},
		},
	}

	r := &ReconcileStoragePolicyInfo{zonesProvider: &mockZonesProvider{}}
	err := r.syncVolumeCapabilitiesFromInfraSPI(ctx, instance, infraSPI, nil)
	require.NoError(t, err)
	assert.True(t, instance.Status.VolumeCapabilities[spiv1alpha1.SupportsHostLocal])
}

// setupPolicyZoneWithHPLCHost primes the cache so LC/HPLC over zone-a would compute true.
func setupPolicyZoneWithHPLCHost(t *testing.T, policyName, dsID, hostID, clusterID string) {
	t.Helper()
	clearPolicyZoneCache(t, policyName)
	t.Cleanup(func() {
		vsphereinfra.GetCache().UpdateDsHosts(dsID, map[string]struct{}{})
		vsphereinfra.GetCache().InvalidateHostVersion(hostID)
		vsphereinfra.GetCache().InvalidateCluster(clusterID)
	})
	vsphereinfra.GetCache().SetDatastoresForPolicyZones(policyName, map[string][]string{"zone-a": {dsID}})
	vsphereinfra.GetCache().UpdateDsHosts(dsID, map[string]struct{}{hostID: {}})
	vsphereinfra.GetCache().UpdateHostVersion(hostID, "9.1.0")
	vsphereinfra.GetCache().UpdateClusterHosts(clusterID, map[string]struct{}{hostID: {}})
	vsphereinfra.GetCache().SetClusterESAEnabled(clusterID, true)
}

// TestSyncVolumeCapabilitiesFromInfraSPI_MarkerPolicyReportsNoZonalCapabilities verifies Block mode
// is forced false and no LC/HPLC zones are reported for the marker policy, even when InfraSPI reports Block
// mode support and the cache would otherwise compute LC/HPLC true.
func TestSyncVolumeCapabilitiesFromInfraSPI_MarkerPolicyReportsNoZonalCapabilities(t *testing.T) {
	ctx := logger.NewContextWithLogger(context.Background())
	markerPolicy := common.StorageClassVsanFileServicePolicy
	setupPolicyZoneWithHPLCHost(t, markerPolicy, "ds-marker-force", "host-marker-force", "cluster-marker-force")

	instance := &spiv1alpha1.StoragePolicyInfo{
		ObjectMeta: metav1.ObjectMeta{Name: markerPolicy, Namespace: "consumer-ns"},
		Status: spiv1alpha1.StoragePolicyInfoStatus{
			TopologyInfo: &spiv1alpha1.Topology{TopologyType: "zonal", AccessibleZones: []string{"zone-a"}},
		},
	}
	infraSPI := &infraspiv1alpha1.InfraStoragePolicyInfo{
		ObjectMeta: metav1.ObjectMeta{Name: markerPolicy},
		Status: infraspiv1alpha1.InfraStoragePolicyInfoStatus{
			VolumeCapabilities: map[infraspiv1alpha1.VolumeCapability]bool{
				infraspiv1alpha1.SupportsVolumeModeBlock: true,
			},
		},
	}

	r := &ReconcileStoragePolicyInfo{IsVsanFileVolumeService: true}
	err := r.syncVolumeCapabilitiesFromInfraSPI(ctx, instance, infraSPI, nil)
	require.NoError(t, err)
	assert.True(t, instance.Status.VolumeCapabilities[spiv1alpha1.SupportsVolumeModeFilesystem])
	assert.False(t, instance.Status.VolumeCapabilities[spiv1alpha1.SupportsVolumeModeBlock],
		"marker policy must have Block mode forced false even when InfraSPI reports it true")
	assert.Equal(t, emptyZonalVolumeCapabilities(), instance.Status.ZonalVolumeCapabilities,
		"marker policy must report no LinkedClone/HighPerformanceLinkedClone zones even when the cache would compute them")
}

// emptyZonalVolumeCapabilities is the ZonalVolumeCapabilities expected when no zone supports any
// zonal capability: every capability is present with an empty zone list.
func emptyZonalVolumeCapabilities() map[spiv1alpha1.ZonalVolumeCapability]spiv1alpha1.ZoneList {
	return map[spiv1alpha1.ZonalVolumeCapability]spiv1alpha1.ZoneList{
		spiv1alpha1.ZonesSupportingLinkedClone:                {},
		spiv1alpha1.ZonesSupportingHighPerformanceLinkedClone: {},
	}
}

// TestSyncVolumeCapabilitiesFromInfraSPI_MarkerPolicyFSSDisabledComputes verifies LC/HPLC are
// computed normally for the marker policy when the marker FSS is disabled.
func TestSyncVolumeCapabilitiesFromInfraSPI_MarkerPolicyFSSDisabledComputes(t *testing.T) {
	ctx := logger.NewContextWithLogger(context.Background())
	markerPolicy := common.StorageClassVsanFileServicePolicy
	setupPolicyZoneWithHPLCHost(t, markerPolicy, "ds-marker-fssoff", "host-marker-fssoff", "cluster-marker-fssoff")

	instance := &spiv1alpha1.StoragePolicyInfo{
		ObjectMeta: metav1.ObjectMeta{Name: markerPolicy, Namespace: "consumer-ns"},
		Status: spiv1alpha1.StoragePolicyInfoStatus{
			TopologyInfo: &spiv1alpha1.Topology{TopologyType: "zonal", AccessibleZones: []string{"zone-a"}},
		},
	}
	infraSPI := &infraspiv1alpha1.InfraStoragePolicyInfo{ObjectMeta: metav1.ObjectMeta{Name: markerPolicy}}

	r := &ReconcileStoragePolicyInfo{IsVsanFileVolumeService: false}
	activeClustersByZone := map[string]map[string]bool{"zone-a": {"cluster-marker-fssoff": true}}
	err := r.syncVolumeCapabilitiesFromInfraSPI(ctx, instance, infraSPI, activeClustersByZone)
	require.NoError(t, err)
	assert.Equal(t, spiv1alpha1.ZoneList{"zone-a"},
		instance.Status.ZonalVolumeCapabilities[spiv1alpha1.ZonesSupportingLinkedClone],
		"with the marker FSS off, LinkedClone is computed normally (zone-a here)")
	assert.Equal(t, spiv1alpha1.ZoneList{"zone-a"},
		instance.Status.ZonalVolumeCapabilities[spiv1alpha1.ZonesSupportingHighPerformanceLinkedClone],
		"with the marker FSS off, HighPerformanceLinkedClone is computed normally (zone-a here)")
}

// TestSyncVolumeCapabilitiesFromInfraSPI_NonMarkerPolicyComputes verifies the marker
// short-circuit doesn't affect ordinary policies.
func TestSyncVolumeCapabilitiesFromInfraSPI_NonMarkerPolicyComputes(t *testing.T) {
	ctx := logger.NewContextWithLogger(context.Background())
	const policyName = "policy-svcfi-nonmarker"
	setupPolicyZoneWithHPLCHost(t, policyName, "ds-nonmarker", "host-nonmarker", "cluster-nonmarker")

	instance := &spiv1alpha1.StoragePolicyInfo{
		ObjectMeta: metav1.ObjectMeta{Name: policyName, Namespace: "consumer-ns"},
		Status: spiv1alpha1.StoragePolicyInfoStatus{
			TopologyInfo: &spiv1alpha1.Topology{TopologyType: "zonal", AccessibleZones: []string{"zone-a"}},
		},
	}
	infraSPI := &infraspiv1alpha1.InfraStoragePolicyInfo{ObjectMeta: metav1.ObjectMeta{Name: policyName}}

	r := &ReconcileStoragePolicyInfo{IsVsanFileVolumeService: true}
	activeClustersByZone := map[string]map[string]bool{"zone-a": {"cluster-nonmarker": true}}
	err := r.syncVolumeCapabilitiesFromInfraSPI(ctx, instance, infraSPI, activeClustersByZone)
	require.NoError(t, err)
	assert.Equal(t, spiv1alpha1.ZoneList{"zone-a"},
		instance.Status.ZonalVolumeCapabilities[spiv1alpha1.ZonesSupportingLinkedClone],
		"non-marker policy is unaffected by the marker short-circuit")
	assert.Equal(t, spiv1alpha1.ZoneList{"zone-a"},
		instance.Status.ZonalVolumeCapabilities[spiv1alpha1.ZonesSupportingHighPerformanceLinkedClone],
		"non-marker policy is unaffected by the marker short-circuit")
}

// setupPolicyZoneHost primes the cache with datastore dsID mounted by hostID
// running version in clusterID, with vSAN-ESA set to esa.
func setupPolicyZoneHost(t *testing.T, dsID, hostID, version, clusterID string,
	esa bool) {
	t.Helper()
	t.Cleanup(func() {
		vsphereinfra.GetCache().UpdateDsHosts(dsID, map[string]struct{}{})
		vsphereinfra.GetCache().InvalidateHostVersion(hostID)
		vsphereinfra.GetCache().InvalidateCluster(clusterID)
	})
	vsphereinfra.GetCache().UpdateDsHosts(dsID, map[string]struct{}{hostID: {}})
	vsphereinfra.GetCache().UpdateHostVersion(hostID, version)
	vsphereinfra.GetCache().UpdateClusterHosts(clusterID, map[string]struct{}{hostID: {}})
	vsphereinfra.GetCache().SetClusterESAEnabled(clusterID, esa)
}

// TestQualifyingZonesForNamespace_ReturnsEveryQualifyingZoneSorted verifies that the walk does
// not stop at the first qualifying zone, and that the result is sorted regardless of nsZones
// order so the SPI status is stable across reconciles.
func TestQualifyingZonesForNamespace_ReturnsEveryQualifyingZoneSorted(t *testing.T) {
	ctx := logger.NewContextWithLogger(context.Background())
	const policyName = "policy-qzfn-multi"
	clearPolicyZoneCache(t, policyName)
	zoneDS := map[string][]string{"zone-a": {"ds-qzfn-a"}, "zone-b": {"ds-qzfn-b"}, "zone-c": {"ds-qzfn-c"}}
	vsphereinfra.GetCache().SetDatastoresForPolicyZones(policyName, zoneDS)
	setupPolicyZoneHost(t, "ds-qzfn-a", "host-qzfn-a", "9.1.0", "cluster-qzfn-a", false)
	setupPolicyZoneHost(t, "ds-qzfn-b", "host-qzfn-b", "8.0.3", "cluster-qzfn-b", false)
	setupPolicyZoneHost(t, "ds-qzfn-c", "host-qzfn-c", "9.1.0", "cluster-qzfn-c", false)
	activeClustersByZone := map[string]map[string]bool{
		"zone-a": {"cluster-qzfn-a": true},
		"zone-b": {"cluster-qzfn-b": true},
		"zone-c": {"cluster-qzfn-c": true},
	}

	zones, err := qualifyingZonesForNamespace(ctx, activeClustersByZone, "test-op", policyName,
		[]string{"zone-c", "zone-b", "zone-a"}, hostSupportsLinkedClone)
	require.NoError(t, err)
	assert.Equal(t, []string{"zone-a", "zone-c"}, zones)
}

// TestSyncVolumeCapabilitiesFromInfraSPI_MixedZones mirrors the motivating scenario: the
// namespace spans three zones, only zone-b and zone-c have ESXi 9.1+ hosts, and only zone-c's
// cluster has vSAN-ESA. The SPI must list exactly those zones per capability, with HPLC zones a
// subset of LC zones.
func TestSyncVolumeCapabilitiesFromInfraSPI_MixedZones(t *testing.T) {
	ctx := logger.NewContextWithLogger(context.Background())
	const policyName = "policy-svcfi-mixed"
	clearPolicyZoneCache(t, policyName)
	zoneDS := map[string][]string{"zone-a": {"ds-mixed-a"}, "zone-b": {"ds-mixed-b"}, "zone-c": {"ds-mixed-c"}}
	vsphereinfra.GetCache().SetDatastoresForPolicyZones(policyName, zoneDS)
	setupPolicyZoneHost(t, "ds-mixed-a", "host-mixed-a", "8.0.3", "cluster-mixed-a", true)
	setupPolicyZoneHost(t, "ds-mixed-b", "host-mixed-b", "9.1.0", "cluster-mixed-b", false)
	setupPolicyZoneHost(t, "ds-mixed-c", "host-mixed-c", "9.1.0", "cluster-mixed-c", true)

	instance := &spiv1alpha1.StoragePolicyInfo{
		ObjectMeta: metav1.ObjectMeta{Name: policyName, Namespace: "consumer-ns"},
		Status: spiv1alpha1.StoragePolicyInfoStatus{
			TopologyInfo: &spiv1alpha1.Topology{TopologyType: "zonal",
				AccessibleZones: []string{"zone-a", "zone-b", "zone-c"}},
		},
	}
	infraSPI := &infraspiv1alpha1.InfraStoragePolicyInfo{ObjectMeta: metav1.ObjectMeta{Name: policyName}}
	activeClustersByZone := map[string]map[string]bool{
		"zone-a": {"cluster-mixed-a": true},
		"zone-b": {"cluster-mixed-b": true},
		"zone-c": {"cluster-mixed-c": true},
	}

	r := &ReconcileStoragePolicyInfo{}
	err := r.syncVolumeCapabilitiesFromInfraSPI(ctx, instance, infraSPI, activeClustersByZone)
	require.NoError(t, err)
	assert.Equal(t, map[spiv1alpha1.ZonalVolumeCapability]spiv1alpha1.ZoneList{
		spiv1alpha1.ZonesSupportingLinkedClone:                {"zone-b", "zone-c"},
		spiv1alpha1.ZonesSupportingHighPerformanceLinkedClone: {"zone-c"},
	}, instance.Status.ZonalVolumeCapabilities)
}

// TestSyncVolumeCapabilitiesFromInfraSPI_ZoneNotActiveForNamespaceExcluded verifies that a zone
// with a qualifying host is not listed when the namespace has no active cluster in it.
func TestSyncVolumeCapabilitiesFromInfraSPI_ZoneNotActiveForNamespaceExcluded(t *testing.T) {
	ctx := logger.NewContextWithLogger(context.Background())
	const policyName = "policy-svcfi-inactive"
	clearPolicyZoneCache(t, policyName)
	zoneDS := map[string][]string{"zone-a": {"ds-inactive-a"}, "zone-b": {"ds-inactive-b"}}
	vsphereinfra.GetCache().SetDatastoresForPolicyZones(policyName, zoneDS)
	setupPolicyZoneHost(t, "ds-inactive-a", "host-inactive-a", "9.1.0", "cluster-inactive-a", true)
	setupPolicyZoneHost(t, "ds-inactive-b", "host-inactive-b", "9.1.0", "cluster-inactive-b", true)

	instance := &spiv1alpha1.StoragePolicyInfo{
		ObjectMeta: metav1.ObjectMeta{Name: policyName, Namespace: "consumer-ns"},
		Status: spiv1alpha1.StoragePolicyInfoStatus{
			TopologyInfo: &spiv1alpha1.Topology{TopologyType: "zonal",
				AccessibleZones: []string{"zone-a", "zone-b"}},
		},
	}
	infraSPI := &infraspiv1alpha1.InfraStoragePolicyInfo{ObjectMeta: metav1.ObjectMeta{Name: policyName}}
	// The namespace is only active on zone-a.
	activeClustersByZone := map[string]map[string]bool{"zone-a": {"cluster-inactive-a": true}}

	r := &ReconcileStoragePolicyInfo{}
	err := r.syncVolumeCapabilitiesFromInfraSPI(ctx, instance, infraSPI, activeClustersByZone)
	require.NoError(t, err)
	assert.Equal(t, map[spiv1alpha1.ZonalVolumeCapability]spiv1alpha1.ZoneList{
		spiv1alpha1.ZonesSupportingLinkedClone:                {"zone-a"},
		spiv1alpha1.ZonesSupportingHighPerformanceLinkedClone: {"zone-a"},
	}, instance.Status.ZonalVolumeCapabilities)
}
