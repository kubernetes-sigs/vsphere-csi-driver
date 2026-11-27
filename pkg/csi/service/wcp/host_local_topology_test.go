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

package wcp

import (
	"context"
	"errors"
	"maps"
	"testing"

	"github.com/container-storage-interface/spec/lib/go/csi"
	"github.com/stretchr/testify/require"
	vimtypes "github.com/vmware/govmomi/vim25/types"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	v1 "k8s.io/api/core/v1"

	cnsvsphere "sigs.k8s.io/vsphere-csi-driver/v3/pkg/common/cns-lib/vsphere"
	"sigs.k8s.io/vsphere-csi-driver/v3/pkg/csi/service/common"
	"sigs.k8s.io/vsphere-csi-driver/v3/pkg/csi/service/common/commonco"
	commoncotypes "sigs.k8s.io/vsphere-csi-driver/v3/pkg/csi/service/common/commonco/types"
)

// hostLocalTestCO serves the orchestrator calls made by HostLocalStorage + PodVM v2 host-local
// provisioning, and counts them so tests can assert which path was taken. Any other call goes to
// the embedded orchestrator.
type hostLocalTestCO struct {
	commonco.COCommonInterface
	podVMV2 bool
	// owners is the host MoID -> owning Node snapshot returned by GetNodeIDtoNameMap.
	owners    map[string]string
	ownersErr error
	// zoneClusters is the namespace's active clusters per zone.
	zoneClusters      map[string][]string
	activeClustersErr error
	// nodeNameToHostMoID is the legacy dedicated-Node map.
	nodeNameToHostMoID map[string]string

	ownerReads, activeClusterReads, legacyMapReads int
}

func (f *hostLocalTestCO) IsPodVMV2EnabledAtStartup(context.Context) bool { return f.podVMV2 }

func (f *hostLocalTestCO) IsFSSEnabled(ctx context.Context, featureName string) bool {
	if featureName == common.PodVMV2Support {
		return f.podVMV2
	}
	return f.COCommonInterface.IsFSSEnabled(ctx, featureName)
}

func (f *hostLocalTestCO) GetNodeIDtoNameMap(context.Context) (map[string]string, error) {
	f.ownerReads++
	if f.ownersErr != nil {
		return nil, f.ownersErr
	}
	return maps.Clone(f.owners), nil
}

func (f *hostLocalTestCO) GetNodeNameToHostMoIDMap(context.Context) map[string]string {
	f.legacyMapReads++
	return maps.Clone(f.nodeNameToHostMoID)
}

func (f *hostLocalTestCO) GetActiveClustersForNamespaceInRequestedZones(_ context.Context, _ string,
	zones []string) ([]string, error) {
	f.activeClusterReads++
	if f.activeClustersErr != nil {
		return nil, f.activeClustersErr
	}
	var clusters []string
	for _, zone := range zones {
		clusters = append(clusters, f.zoneClusters[zone]...)
	}
	if len(clusters) == 0 {
		return nil, commoncotypes.ErrNoActiveClusters
	}
	return clusters, nil
}

// upgradeOwners is the ownership snapshot of a Supervisor partway through the ESXi upgrade
// (design section 8.1): host-11 and host-12 still have dedicated Nodes, host-13 and host-14 are
// represented by VirtualNode vn-c8, host-15 is mid-upgrade with no owner, and zone-2's hosts are
// represented by vn-c9.
func upgradeOwners() map[string]string {
	return map[string]string{
		"host-11": "esx-11", "host-12": "esx-12",
		"host-13": "vn-c8", "host-14": "vn-c8",
		"host-21": "vn-c9", "host-22": "vn-c9",
	}
}

// upgradeClusterHosts is the vCenter inventory of the same Supervisor.
func upgradeClusterHosts() map[string][]string {
	return map[string][]string{
		"domain-c8": {"host-11", "host-12", "host-13", "host-14", "host-15"},
		"domain-c9": {"host-21", "host-22"},
	}
}

// upgradeInventory is the hostLocalInventory result for zone-1 and zone-2.
func upgradeInventory() map[string]map[string]string {
	inventory := make(map[string]map[string]string)
	for host, owner := range upgradeOwners() {
		zone := "zone-1"
		if host == "host-21" || host == "host-22" {
			zone = "zone-2"
		}
		inventory[host] = map[string]string{v1.LabelTopologyZone: zone, v1.LabelHostname: owner}
	}
	return inventory
}

func topologies(segments ...map[string]string) []*csi.Topology {
	var result []*csi.Topology
	for _, segment := range segments {
		result = append(result, &csi.Topology{Segments: segment})
	}
	return result
}

func zoneSegment(zone string) map[string]string { return map[string]string{v1.LabelTopologyZone: zone} }

func hostSegment(zone, host string) map[string]string {
	return map[string]string{v1.LabelTopologyZone: zone, common.GuestClusterTopologyLabelHost: host}
}

func nodeSegment(zone, node string) map[string]string {
	return map[string]string{v1.LabelTopologyZone: zone, v1.LabelHostname: node}
}

func hostRefs(hosts ...string) []vimtypes.ManagedObjectReference {
	refs := make([]vimtypes.ManagedObjectReference, 0, len(hosts))
	for _, host := range hosts {
		refs = append(refs, vimtypes.ManagedObjectReference{Type: "HostSystem", Value: host})
	}
	return refs
}

func refValues(refs []vimtypes.ManagedObjectReference) []string {
	values := make([]string, 0, len(refs))
	for _, ref := range refs {
		values = append(values, ref.Value)
	}
	return values
}

func TestParseHostLocalTopology(t *testing.T) {
	ctx := context.Background()
	for _, tc := range []struct {
		name string
		raw  string
		want []map[string]string
	}{
		{name: "physical host pair", raw: `[{"topology.kubernetes.io/zone":"zone-1",` +
			`"topology.csi.vmware.com/host":"host-12"}]`, want: []map[string]string{hostSegment("zone-1", "host-12")}},
		{name: "mixed alternatives keep their order and pairing", raw: `[{"topology.kubernetes.io/zone":"zone-1",` +
			`"topology.csi.vmware.com/host":"host-21"},{"topology.kubernetes.io/zone":"zone-1"},` +
			`{"topology.kubernetes.io/zone":"zone-2","kubernetes.io/hostname":"vn-c9"}]`,
			want: []map[string]string{hostSegment("zone-1", "host-21"), zoneSegment("zone-1"),
				nodeSegment("zone-2", "vn-c9")}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got, err := parseHostLocalTopology(ctx, tc.raw)
			require.NoError(t, err)
			require.Equal(t, tc.want, got)
		})
	}
	for _, tc := range []struct{ name, raw string }{
		{name: "not JSON", raw: `zone-1/host-12`},
		{name: "not a list", raw: `{"topology.kubernetes.io/zone":"zone-1"}`},
		{name: "empty list", raw: `[]`},
		{name: "null alternative", raw: `[null]`},
		{name: "host without zone", raw: `[{"topology.csi.vmware.com/host":"host-12"}]`},
		{name: "empty zone", raw: `[{"topology.kubernetes.io/zone":""}]`},
		{name: "empty host", raw: `[{"topology.kubernetes.io/zone":"zone-1","topology.csi.vmware.com/host":""}]`},
		{name: "empty Node", raw: `[{"topology.kubernetes.io/zone":"zone-1","kubernetes.io/hostname":""}]`},
		{name: "both host keys", raw: `[{"topology.kubernetes.io/zone":"zone-1",` +
			`"topology.csi.vmware.com/host":"host-12","kubernetes.io/hostname":"esx-12"}]`},
		{name: "unsupported key", raw: `[{"topology.kubernetes.io/zone":"zone-1","rack":"r1"}]`},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, err := parseHostLocalTopology(ctx, tc.raw)
			require.Error(t, err)
			require.Equal(t, codes.InvalidArgument, status.Code(err), "%v", err)
		})
	}
}

func TestHostLocalCandidates(t *testing.T) {
	ctx := context.Background()
	bothZones := &csi.TopologyRequirement{
		Requisite: topologies(zoneSegment("zone-1"), zoneSegment("zone-2")),
		Preferred: topologies(zoneSegment("zone-1"), zoneSegment("zone-2")),
	}
	zone1 := &csi.TopologyRequirement{
		Requisite: topologies(zoneSegment("zone-1")), Preferred: topologies(zoneSegment("zone-1")),
	}
	for _, tc := range []struct {
		name         string
		requirement  *csi.TopologyRequirement
		alternatives []map[string]string
		want         []string
		code         codes.Code
	}{
		{name: "physical host", requirement: bothZones,
			alternatives: []map[string]string{hostSegment("zone-1", "host-12")}, want: []string{"host-12"}},
		{name: "physical hosts owned by a dedicated Node and a VirtualNode", requirement: zone1,
			alternatives: []map[string]string{hostSegment("zone-1", "host-12"), hostSegment("zone-1", "host-13")},
			want:         []string{"host-12", "host-13"}},
		{name: "zone-only alternative keeps its OR semantics", requirement: zone1,
			alternatives: []map[string]string{hostSegment("zone-1", "host-12"), zoneSegment("zone-1")},
			want:         []string{"host-11", "host-12", "host-13", "host-14"}},
		{name: "pairs across zones are never flattened into a cross product", requirement: bothZones,
			alternatives: []map[string]string{hostSegment("zone-1", "host-12"), hostSegment("zone-2", "host-21")},
			want:         []string{"host-12", "host-21"}},
		{name: "VirtualNode resolves to the hosts it owns", requirement: zone1,
			alternatives: []map[string]string{nodeSegment("zone-1", "vn-c8")}, want: []string{"host-13", "host-14"}},
		{name: "dedicated Node resolves to its host, case-insensitively", requirement: zone1,
			alternatives: []map[string]string{nodeSegment("zone-1", "ESX-12")}, want: []string{"host-12"}},
		{name: "zone-only normal topology selects every owned host in the zone", requirement: zone1,
			want: []string{"host-11", "host-12", "host-13", "host-14"}},
		{name: "direct PodVM Immediate aggregated per CSINode", requirement: &csi.TopologyRequirement{
			Requisite: topologies(nodeSegment("zone-1", "esx-11"), nodeSegment("zone-1", "esx-12"),
				zoneSegment("zone-1"), zoneSegment("zone-2")),
			Preferred: topologies(nodeSegment("zone-1", "esx-11"), nodeSegment("zone-1", "esx-12"),
				zoneSegment("zone-1"), zoneSegment("zone-2")),
		}, want: []string{"host-11", "host-12", "host-13", "host-14", "host-21", "host-22"}},
		{name: "direct PodVM WFFC on a dedicated Node", requirement: &csi.TopologyRequirement{
			Requisite: topologies(nodeSegment("zone-1", "esx-12")), Preferred: topologies(nodeSegment("zone-1", "esx-12")),
		}, want: []string{"host-12"}},
		{name: "normal Node constraint still restricts a zone-only alternative", requirement: &csi.TopologyRequirement{
			Requisite: topologies(nodeSegment("zone-1", "esx-12")), Preferred: topologies(nodeSegment("zone-1", "esx-12")),
		}, alternatives: []map[string]string{zoneSegment("zone-1")}, want: []string{"host-12"}},
		{name: "requisite falls back to preferred",
			requirement: &csi.TopologyRequirement{Preferred: topologies(zoneSegment("zone-2"))},
			want:        []string{"host-21", "host-22"}},
		{name: "unowned host is never a candidate", requirement: zone1,
			alternatives: []map[string]string{hostSegment("zone-1", "host-15")}, code: codes.FailedPrecondition},
		{name: "host outside its alternative's zone", requirement: bothZones,
			alternatives: []map[string]string{hostSegment("zone-2", "host-11")}, code: codes.FailedPrecondition},
		{name: "physical alternative cannot escape the requisite zones", requirement: zone1,
			alternatives: []map[string]string{hostSegment("zone-2", "host-21")}, code: codes.FailedPrecondition},
		{name: "unknown Node", requirement: zone1,
			alternatives: []map[string]string{nodeSegment("zone-1", "esx-99")}, code: codes.FailedPrecondition},
		{name: "host not admitted by the normal Node constraint", requirement: &csi.TopologyRequirement{
			Requisite: topologies(nodeSegment("zone-1", "esx-12")), Preferred: topologies(nodeSegment("zone-1", "esx-12")),
		}, alternatives: []map[string]string{hostSegment("zone-1", "host-13")}, code: codes.FailedPrecondition},
		{name: "unsupported key in normal topology", requirement: &csi.TopologyRequirement{
			Preferred: topologies(map[string]string{v1.LabelTopologyZone: "zone-1", "rack": "r1"}),
		}, code: codes.InvalidArgument},
		{name: "no topology", requirement: &csi.TopologyRequirement{}, code: codes.InvalidArgument},
	} {
		t.Run(tc.name, func(t *testing.T) {
			inventory := upgradeInventory()
			refs, candidateTopology, err := hostLocalCandidates(ctx, tc.requirement, tc.alternatives, inventory)
			if tc.code != codes.OK {
				require.Error(t, err)
				require.Equal(t, tc.code, status.Code(err), "%v", err)
				return
			}
			require.NoError(t, err)
			require.Equal(t, tc.want, refValues(refs))
			for _, ref := range refs {
				require.Equal(t, "HostSystem", ref.Type)
			}
			require.Len(t, candidateTopology, len(tc.want))
			for host, topology := range candidateTopology {
				require.Equal(t, inventory[host], topology)
				topology[v1.LabelHostname] = "mutated"
				require.NotEqual(t, "mutated", inventory[host][v1.LabelHostname],
					"candidate topology must not alias the inventory")
			}
		})
	}
}

func TestHostLocalInventory(t *testing.T) {
	ctx := context.Background()
	originalCO := commonco.ContainerOrchestratorUtility
	originalClusterHosts := getClusterHostMoIDs
	t.Cleanup(func() {
		commonco.ContainerOrchestratorUtility = originalCO
		getClusterHostMoIDs = originalClusterHosts
	})
	bothZones := &csi.TopologyRequirement{
		Requisite: topologies(zoneSegment("zone-1"), zoneSegment("zone-2")),
		Preferred: topologies(zoneSegment("zone-1"), zoneSegment("zone-2")),
	}
	zoneClusters := map[string][]string{"zone-1": {"domain-c8"}, "zone-2": {"domain-c9"}}

	type setup struct {
		co          *hostLocalTestCO
		c           *controller
		hostReads   map[string]int
		clusterHost map[string][]string
		hostErr     error
	}
	newSetup := func() *setup {
		s := &setup{
			co: &hostLocalTestCO{COCommonInterface: originalCO, podVMV2: true, owners: upgradeOwners(),
				zoneClusters: zoneClusters},
			c:           &controller{},
			hostReads:   map[string]int{},
			clusterHost: upgradeClusterHosts(),
		}
		commonco.ContainerOrchestratorUtility = s.co
		getClusterHostMoIDs = func(_ context.Context, _ *cnsvsphere.VirtualCenter, cluster string) ([]string, error) {
			s.hostReads[cluster]++
			if s.hostErr != nil {
				return nil, s.hostErr
			}
			return s.clusterHost[cluster], nil
		}
		return s
	}

	t.Run("namespace active clusters with ownership from the snapshot", func(t *testing.T) {
		s := newSetup()
		inventory, err := s.c.hostLocalInventory(ctx, nil, "tenant-a", bothZones)
		require.NoError(t, err)
		require.Equal(t, upgradeInventory(), inventory, "host-15 has no owner and must be left out")
		require.Equal(t, 1, s.co.ownerReads)
		require.Equal(t, 2, s.co.activeClusterReads)
		require.Zero(t, s.co.legacyMapReads, "the dedicated-only map must not be used")
	})

	t.Run("only requested zones are read", func(t *testing.T) {
		s := newSetup()
		inventory, err := s.c.hostLocalInventory(ctx, nil, "tenant-a", &csi.TopologyRequirement{
			Requisite: topologies(nodeSegment("zone-1", "esx-11"), zoneSegment("zone-1")),
			Preferred: topologies(nodeSegment("zone-1", "esx-11"), zoneSegment("zone-1")),
		})
		require.NoError(t, err)
		require.Len(t, inventory, 4)
		require.Equal(t, map[string]int{"domain-c8": 1}, s.hostReads, "each zone is read once")
	})

	t.Run("ownership that cannot be resolved fails the attempt", func(t *testing.T) {
		s := newSetup()
		s.co.ownersErr = errors.New("cannot list NodeMapping resources")
		_, err := s.c.hostLocalInventory(ctx, nil, "tenant-a", bothZones)
		require.Error(t, err)
		require.Equal(t, codes.Unavailable, status.Code(err), "%v", err)
		require.Empty(t, s.hostReads, "no candidate may be derived from an incomplete ownership view")
	})

	for _, tc := range []struct {
		name   string
		mutate func(*setup)
		ns     string
		req    *csi.TopologyRequirement
		code   codes.Code
	}{
		{name: "namespace is required for active clusters", ns: "", req: bothZones, code: codes.InvalidArgument},
		{name: "no zones", ns: "tenant-a", req: &csi.TopologyRequirement{
			Preferred: topologies(map[string]string{v1.LabelHostname: "esx-11"})}, code: codes.InvalidArgument},
		{name: "active cluster lookup fails", ns: "tenant-a", req: bothZones, code: codes.Internal,
			mutate: func(s *setup) { s.co.activeClustersErr = errors.New("zone CR unavailable") }},
		{name: "cluster host lookup fails", ns: "tenant-a", req: bothZones, code: codes.Internal,
			mutate: func(s *setup) { s.hostErr = errors.New("vCenter unavailable") }},
		{name: "host in two zones", ns: "tenant-a", req: bothZones, code: codes.Internal,
			mutate: func(s *setup) { s.clusterHost["domain-c9"] = append(s.clusterHost["domain-c9"], "host-11") }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			s := newSetup()
			if tc.mutate != nil {
				tc.mutate(s)
			}
			_, err := s.c.hostLocalInventory(ctx, nil, tc.ns, tc.req)
			require.Error(t, err)
			require.Equal(t, tc.code, status.Code(err), "%v", err)
		})
	}
}

func TestHostLocalVolumeResult(t *testing.T) {
	ctx := context.Background()
	inventory := upgradeInventory()
	selected := func(host string) *vimtypes.ManagedObjectReference {
		return &vimtypes.ManagedObjectReference{Type: "HostSystem", Value: host}
	}
	for _, tc := range []struct {
		name       string
		candidates []vimtypes.ManagedObjectReference
		selected   *vimtypes.ManagedObjectReference
		topology   map[string]map[string]string
		wantZone   string
		wantHost   string
	}{
		{name: "CNS selects a host owned by a VirtualNode", candidates: hostRefs("host-13", "host-14"),
			selected: selected("host-13"), topology: inventory, wantZone: "zone-1", wantHost: "host-13"},
		{name: "CNS selects a host owned by a dedicated Node", candidates: hostRefs("host-12"),
			selected: selected("host-12"), topology: inventory, wantZone: "zone-1", wantHost: "host-12"},
		{name: "no placement host", candidates: hostRefs("host-13"), topology: inventory},
		{name: "empty placement host", candidates: hostRefs("host-13"), selected: selected(""), topology: inventory},
		{name: "placement host outside the candidates", candidates: hostRefs("host-13"),
			selected: selected("host-14"), topology: inventory},
		{name: "unknown zone", candidates: hostRefs("host-13"), selected: selected("host-13"),
			topology: map[string]map[string]string{"host-13": {v1.LabelHostname: "vn-c8"}}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			segments, host, err := hostLocalVolumeResult(ctx, tc.candidates, tc.topology, tc.selected, "vol-1")
			if tc.wantHost == "" {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)
			require.Equal(t, tc.wantHost, host)
			require.Equal(t, map[string]string{v1.LabelTopologyZone: tc.wantZone}, segments,
				"the result must be zone only, with no kubernetes.io/hostname affinity")
			require.Contains(t, tc.topology[host], v1.LabelHostname,
				"the internal candidate topology keeps the owner separate from the returned topology")
		})
	}
}

func TestValidateCreateReqParamHostLocalTopology(t *testing.T) {
	require.True(t, validateCreateBlockReqParam(common.AttributeHostLocalTopology, "[]"))
	require.False(t, validateCreateFileReqParam(common.AttributeHostLocalTopology, "[]"))
}
