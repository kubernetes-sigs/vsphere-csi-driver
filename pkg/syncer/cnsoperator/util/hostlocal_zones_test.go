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

package util

import (
	"context"
	"errors"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/vmware/govmomi"
	pbmtypes "github.com/vmware/govmomi/pbm/types"
	"github.com/vmware/govmomi/simulator"
	vimtypes "github.com/vmware/govmomi/vim25/types"

	cnsvsphere "sigs.k8s.io/vsphere-csi-driver/v3/pkg/common/cns-lib/vsphere"
	commontypes "sigs.k8s.io/vsphere-csi-driver/v3/pkg/common/types"
)

// --- helpers for GetHostLocalAccessibleZones --------------------------------------------

// setupHostLocalSim creates a VPX model with numClusters clusters, one host each, and one
// datastore exclusively mounted by that cluster's single host — mirroring a host-local vSAN
// datastore, which is by definition mounted on exactly one host. Returns, per cluster in order,
// the cluster moref, its single host moref, and its exclusively-mounted datastore moref, plus the
// model itself so tests can mutate simulator state directly (e.g. clearing a datastore's mounts,
// or mounting an extra datastore on an existing host).
func setupHostLocalSim(t *testing.T, numClusters int) (
	ctx context.Context,
	vc *cnsvsphere.VirtualCenter,
	model *simulator.Model,
	clusterRefs []vimtypes.ManagedObjectReference,
	hostRefs []vimtypes.ManagedObjectReference,
	dsRefs []vimtypes.ManagedObjectReference,
	stop func(),
) {
	t.Helper()
	ctx = context.Background()
	model = simulator.VPX()
	model.Datacenter = 1
	model.Cluster = numClusters
	model.ClusterHost = 1
	model.Host = 0
	model.Machine = 0
	model.Datastore = numClusters
	require.NoError(t, model.Create())
	s := model.Service.NewServer()
	c, err := govmomi.NewClient(ctx, s.URL, true)
	if err != nil {
		s.Close()
		model.Remove()
		t.Fatalf("failed to create govmomi client: %v", err)
	}
	vc = &cnsvsphere.VirtualCenter{
		Config:      &cnsvsphere.VirtualCenterConfig{Host: commontypes.NewFQDN("127.0.0.1")},
		Client:      c,
		ClientMutex: &sync.Mutex{},
	}
	stop = func() { s.Close(); model.Remove() }

	clusters := model.Map().All("ClusterComputeResource")
	require.Len(t, clusters, numClusters)
	dsObjs := model.Map().All("Datastore")
	require.Len(t, dsObjs, numClusters)

	clusterRefs = make([]vimtypes.ManagedObjectReference, numClusters)
	hostRefs = make([]vimtypes.ManagedObjectReference, numClusters)
	dsRefs = make([]vimtypes.ManagedObjectReference, numClusters)
	for i, cl := range clusters {
		clusterRef := cl.Reference()
		clusterRefs[i] = clusterRef

		clusterObj := model.Map().Get(clusterRef).(*simulator.ClusterComputeResource)
		require.Len(t, clusterObj.Host, 1, "setupHostLocalSim expects exactly one host per cluster")
		hostRefs[i] = clusterObj.Host[0]

		// Restrict this datastore's mount list to only this cluster's single host; by default
		// vcsim mounts every datastore on every host in the datacenter.
		dsObj := dsObjs[i].(*simulator.Datastore)
		dsObj.Host = []vimtypes.DatastoreHostMount{{Key: hostRefs[i]}}
		dsRefs[i] = dsObj.Reference()
	}
	return
}

// hubFor builds a PbmPlacementHub for a datastore moref, as PbmQueryMatchingHub would return.
func hubFor(dsRef vimtypes.ManagedObjectReference) pbmtypes.PbmPlacementHub {
	return pbmtypes.PbmPlacementHub{HubType: "Datastore", HubId: dsRef.Value}
}

// withFakeMatchingHubs replaces PbmQueryMatchingHubFn for the duration of fn, returning
// hubs/err without a real PBM connection. If called is non-nil, it's set to true iff the fake
// was actually invoked.
func withFakeMatchingHubs(hubs []pbmtypes.PbmPlacementHub, err error, called *bool, fn func()) {
	orig := PbmQueryMatchingHubFn
	PbmQueryMatchingHubFn = func(_ context.Context, _ *cnsvsphere.VirtualCenter,
		_ string) ([]pbmtypes.PbmPlacementHub, error) {
		if called != nil {
			*called = true
		}
		return hubs, err
	}
	defer func() { PbmQueryMatchingHubFn = orig }()
	fn()
}

// --- GetHostLocalAccessibleZones tests ---------------------------------------------------

// TestGetHostLocalAccessibleZones_NilTopologyManager verifies that a nil topology manager
// returns an error immediately, without ever calling PBM.
func TestGetHostLocalAccessibleZones_NilTopologyManager(t *testing.T) {
	ctx := context.Background()
	var called bool
	withFakeMatchingHubs(nil, nil, &called, func() {
		zones, zoneCompatibleDS, dsIDs, err := GetHostLocalAccessibleZones(ctx, nil, nil, "policy-1")
		assert.Error(t, err)
		assert.Nil(t, zones)
		assert.Nil(t, zoneCompatibleDS)
		assert.Nil(t, dsIDs)
	})
	assert.False(t, called, "PBM should not be queried when the topology manager is nil")
}

// TestGetHostLocalAccessibleZones_NilVirtualCenter verifies that a nil (or not-yet-connected)
// vCenter returns an error instead of panicking.
func TestGetHostLocalAccessibleZones_NilVirtualCenter(t *testing.T) {
	ctx := context.Background()
	topo := &mockTopologyService{azClustersMap: map[string][]string{"zone-a": {"cluster-1"}}}

	zones, zoneCompatibleDS, dsIDs, err := GetHostLocalAccessibleZones(ctx, topo, nil, "policy-1")
	assert.Error(t, err)
	assert.Nil(t, zones)
	assert.Nil(t, zoneCompatibleDS)
	assert.Nil(t, dsIDs)

	zones, zoneCompatibleDS, dsIDs, err = GetHostLocalAccessibleZones(ctx, topo, &cnsvsphere.VirtualCenter{}, "policy-1")
	assert.Error(t, err)
	assert.Nil(t, zones)
	assert.Nil(t, zoneCompatibleDS)
	assert.Nil(t, dsIDs)
}

// TestGetHostLocalAccessibleZones_PbmError verifies that a PBM error is propagated.
func TestGetHostLocalAccessibleZones_PbmError(t *testing.T) {
	ctx := context.Background()
	topo := &mockTopologyService{azClustersMap: map[string][]string{"zone-a": {"cluster-1"}}}

	withFakeMatchingHubs(nil, errors.New("pbm unavailable"), nil, func() {
		zones, zoneCompatibleDS, dsIDs, err := GetHostLocalAccessibleZones(ctx, topo, fakeVC(), "policy-1")
		assert.Error(t, err)
		assert.Nil(t, zones)
		assert.Nil(t, zoneCompatibleDS)
		assert.Nil(t, dsIDs)
	})
}

// TestGetHostLocalAccessibleZones_NoHubs verifies that no compatible datastores yields an empty,
// non-nil result without error.
func TestGetHostLocalAccessibleZones_NoHubs(t *testing.T) {
	_, vc, _, _, _, _, stop := setupHostLocalSim(t, 1)
	defer stop()
	ctx := context.Background()
	topo := &mockTopologyService{azClustersMap: map[string][]string{"zone-a": {"cluster-1"}}}

	withFakeMatchingHubs(nil, nil, nil, func() {
		zones, zoneCompatibleDS, dsIDs, err := GetHostLocalAccessibleZones(ctx, topo, vc, "policy-1")
		require.NoError(t, err)
		assert.NotNil(t, zones, "zones must be a non-nil empty slice, not nil: it's assigned straight into "+
			"InfraStoragePolicyInfo.Status.Topology.AccessibleZones, which the CRD requires to be present")
		assert.Empty(t, zones)
		assert.Empty(t, zoneCompatibleDS)
		assert.Empty(t, dsIDs)
	})
}

// TestGetHostLocalAccessibleZones_SingleZoneSingleHost verifies the basic success path: one
// host-local datastore, mounted on one host, whose cluster is part of one zone.
func TestGetHostLocalAccessibleZones_SingleZoneSingleHost(t *testing.T) {
	ctx, vc, _, clusterRefs, _, dsRefs, stop := setupHostLocalSim(t, 1)
	defer stop()
	topo := &mockTopologyService{azClustersMap: map[string][]string{"zone-a": {clusterRefs[0].Value}}}

	withFakeMatchingHubs([]pbmtypes.PbmPlacementHub{hubFor(dsRefs[0])}, nil, nil, func() {
		zones, zoneCompatibleDS, dsIDs, err := GetHostLocalAccessibleZones(ctx, topo, vc, "policy-1")
		require.NoError(t, err)
		assert.Equal(t, []string{"zone-a"}, zones)
		require.Contains(t, zoneCompatibleDS, "zone-a")
		require.Len(t, zoneCompatibleDS["zone-a"], 1)
		assert.Equal(t, dsRefs[0].Value, zoneCompatibleDS["zone-a"][0].Reference().Value)
		assert.Equal(t, []string{dsRefs[0].Value}, dsIDs)
	})
}

// TestGetHostLocalAccessibleZones_MultipleZonesUnioned verifies that host-local datastores in
// different zones are all attributed and unioned into the result.
func TestGetHostLocalAccessibleZones_MultipleZonesUnioned(t *testing.T) {
	ctx, vc, _, clusterRefs, _, dsRefs, stop := setupHostLocalSim(t, 2)
	defer stop()
	topo := &mockTopologyService{azClustersMap: map[string][]string{
		"zone-a": {clusterRefs[0].Value},
		"zone-b": {clusterRefs[1].Value},
	}}

	withFakeMatchingHubs([]pbmtypes.PbmPlacementHub{hubFor(dsRefs[0]), hubFor(dsRefs[1])}, nil, nil, func() {
		zones, zoneCompatibleDS, dsIDs, err := GetHostLocalAccessibleZones(ctx, topo, vc, "policy-1")
		require.NoError(t, err)
		assert.ElementsMatch(t, []string{"zone-a", "zone-b"}, zones)
		require.Len(t, zoneCompatibleDS["zone-a"], 1)
		require.Len(t, zoneCompatibleDS["zone-b"], 1)
		assert.Equal(t, dsRefs[0].Value, zoneCompatibleDS["zone-a"][0].Reference().Value)
		assert.Equal(t, dsRefs[1].Value, zoneCompatibleDS["zone-b"][0].Reference().Value)
		assert.ElementsMatch(t, []string{dsRefs[0].Value, dsRefs[1].Value}, dsIDs)
	})
}

// TestGetHostLocalAccessibleZones_InaccessibleDatastore_SkippedNotFatal verifies that a
// PBM-compatible datastore the property collector can't read (e.g. NoPermission, or here a
// moref vCenter doesn't recognize) is skipped rather than failing the whole policy's zone
// computation — other, readable datastores are still processed normally. Regression test for a
// live failure where PbmQueryMatchingHub returned a datastore outside the service account's
// permitted inventory subtree and aborted accessible-zone computation for the entire policy.
func TestGetHostLocalAccessibleZones_InaccessibleDatastore_SkippedNotFatal(t *testing.T) {
	ctx, vc, _, clusterRefs, _, dsRefs, stop := setupHostLocalSim(t, 1)
	defer stop()
	topo := &mockTopologyService{azClustersMap: map[string][]string{"zone-a": {clusterRefs[0].Value}}}

	unreadableDS := vimtypes.ManagedObjectReference{Type: "Datastore", Value: "datastore-does-not-exist"}
	withFakeMatchingHubs([]pbmtypes.PbmPlacementHub{hubFor(unreadableDS), hubFor(dsRefs[0])}, nil, nil, func() {
		zones, zoneCompatibleDS, dsIDs, err := GetHostLocalAccessibleZones(ctx, topo, vc, "policy-1")
		require.NoError(t, err, "an unreadable datastore must be skipped, not surfaced as an error")
		assert.Equal(t, []string{"zone-a"}, zones)
		require.Len(t, zoneCompatibleDS["zone-a"], 1)
		assert.Equal(t, dsRefs[0].Value, zoneCompatibleDS["zone-a"][0].Reference().Value)
		assert.ElementsMatch(t, []string{unreadableDS.Value, dsRefs[0].Value}, dsIDs,
			"dsIDs still records the unreadable datastore as policy-compatible")
	})
}

// TestGetHostLocalAccessibleZones_ClusterNotInAnyZone_Skipped verifies that a host-local
// datastore whose host's cluster isn't part of any zone is skipped: it's still recorded in
// dsIDs (it's still policy-compatible) but contributes no zone.
func TestGetHostLocalAccessibleZones_ClusterNotInAnyZone_Skipped(t *testing.T) {
	ctx, vc, _, _, _, dsRefs, stop := setupHostLocalSim(t, 1)
	defer stop()
	// No zone maps to this cluster at all.
	topo := &mockTopologyService{azClustersMap: map[string][]string{"zone-a": {"some-other-cluster"}}}

	withFakeMatchingHubs([]pbmtypes.PbmPlacementHub{hubFor(dsRefs[0])}, nil, nil, func() {
		zones, zoneCompatibleDS, dsIDs, err := GetHostLocalAccessibleZones(ctx, topo, vc, "policy-1")
		require.NoError(t, err)
		assert.Empty(t, zones)
		assert.Empty(t, zoneCompatibleDS)
		assert.Equal(t, []string{dsRefs[0].Value}, dsIDs)
	})
}

// TestGetHostLocalAccessibleZones_NoHostMounts_Skipped verifies that a compatible datastore with
// no host mounts (e.g. detached) is skipped for zone purposes but still recorded in dsIDs.
func TestGetHostLocalAccessibleZones_NoHostMounts_Skipped(t *testing.T) {
	ctx, vc, model, clusterRefs, _, dsRefs, stop := setupHostLocalSim(t, 1)
	defer stop()
	topo := &mockTopologyService{azClustersMap: map[string][]string{"zone-a": {clusterRefs[0].Value}}}

	// Clear the datastore's host mounts entirely.
	dsObj := model.Map().Get(dsRefs[0]).(*simulator.Datastore)
	dsObj.Host = nil

	withFakeMatchingHubs([]pbmtypes.PbmPlacementHub{hubFor(dsRefs[0])}, nil, nil, func() {
		zones, zoneCompatibleDS, dsIDs, err := GetHostLocalAccessibleZones(ctx, topo, vc, "policy-1")
		require.NoError(t, err)
		assert.Empty(t, zones)
		assert.Empty(t, zoneCompatibleDS)
		assert.Equal(t, []string{dsRefs[0].Value}, dsIDs, "an unmounted datastore is still policy-compatible")
	})
}

// TestGetHostLocalAccessibleZones_SameHostMultipleDatastores verifies that two host-local
// datastores mounted on the same host are both attributed to that host's zone — deduped once in
// zones, but each recorded separately in zoneCompatibleDS and dsIDs — exercising the
// hostClusterCache reuse path (the host's parent cluster is only looked up once).
func TestGetHostLocalAccessibleZones_SameHostMultipleDatastores(t *testing.T) {
	// setupHostLocalSim(t, 2) gives two independent clusters/hosts/datastores; remount the second
	// cluster's datastore onto the first cluster's host so both datastores end up mounted on the
	// same single host, as if that host had two local disk groups.
	ctx, vc, model, clusterRefs, hostRefs, dsRefs, stop := setupHostLocalSim(t, 2)
	defer stop()
	topo := &mockTopologyService{azClustersMap: map[string][]string{"zone-a": {clusterRefs[0].Value}}}

	secondDSObj := model.Map().Get(dsRefs[1]).(*simulator.Datastore)
	secondDSObj.Host = []vimtypes.DatastoreHostMount{{Key: hostRefs[0]}}

	withFakeMatchingHubs([]pbmtypes.PbmPlacementHub{hubFor(dsRefs[0]), hubFor(dsRefs[1])}, nil, nil, func() {
		zones, zoneCompatibleDS, dsIDs, err := GetHostLocalAccessibleZones(ctx, topo, vc, "policy-1")
		require.NoError(t, err)
		assert.Equal(t, []string{"zone-a"}, zones, "the zone must appear once even though two hubs map to it")
		require.Len(t, zoneCompatibleDS["zone-a"], 2, "both hub entries should still be recorded")
		assert.ElementsMatch(t, []string{dsRefs[0].Value, dsRefs[1].Value}, dsIDs)
	})
}
