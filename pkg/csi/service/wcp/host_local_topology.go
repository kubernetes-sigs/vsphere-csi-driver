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
	"encoding/json"
	"errors"
	"fmt"
	"maps"
	"slices"
	"strings"

	"github.com/container-storage-interface/spec/lib/go/csi"
	vimtypes "github.com/vmware/govmomi/vim25/types"
	"google.golang.org/grpc/codes"
	v1 "k8s.io/api/core/v1"

	cnsvolume "sigs.k8s.io/vsphere-csi-driver/v3/pkg/common/cns-lib/volume"
	cnsvsphere "sigs.k8s.io/vsphere-csi-driver/v3/pkg/common/cns-lib/vsphere"
	"sigs.k8s.io/vsphere-csi-driver/v3/pkg/csi/service/common"
	"sigs.k8s.io/vsphere-csi-driver/v3/pkg/csi/service/common/commonco"
	commoncotypes "sigs.k8s.io/vsphere-csi-driver/v3/pkg/csi/service/common/commonco/types"
	"sigs.k8s.io/vsphere-csi-driver/v3/pkg/csi/service/logger"
)

// isHostLocalStorageAndPodVMV2Enabled reports whether host-local volumes use the HostLocalStorage +
// PodVM v2 contract.
func isHostLocalStorageAndPodVMV2Enabled(ctx context.Context) bool {
	return isHostLocalStorageSupportEnabled && commonco.ContainerOrchestratorUtility.IsPodVMV2EnabledAtStartup(ctx)
}

// getClusterHostMoIDs returns the MoIDs of the hosts in a vCenter cluster
// (ClusterComputeResource.host).
var getClusterHostMoIDs = func(ctx context.Context, vc *cnsvsphere.VirtualCenter,
	clusterMoID string) ([]string, error) {
	hosts, err := vc.GetHostsByCluster(ctx, clusterMoID)
	if err != nil {
		return nil, err
	}
	ids := make([]string, 0, len(hosts))
	for _, host := range hosts {
		ids = append(ids, host.Reference().Value)
	}
	return ids, nil
}

// parseHostLocalTopology decodes the cns.vmware.com/host-local-topology parameter: a JSON list of
// paired alternatives.
func parseHostLocalTopology(ctx context.Context, raw string) ([]map[string]string, error) {
	log := logger.GetLogger(ctx)
	var alternatives []map[string]string
	if err := json.Unmarshal([]byte(raw), &alternatives); err != nil {
		return nil, logger.LogNewErrorCodef(log, codes.InvalidArgument,
			"invalid %s parameter %q: %v", common.AttributeHostLocalTopology, raw, err)
	}
	if len(alternatives) == 0 {
		return nil, logger.LogNewErrorCodef(log, codes.InvalidArgument,
			"%s parameter has no topology alternatives", common.AttributeHostLocalTopology)
	}
	for _, segment := range alternatives {
		if err := validateHostLocalSegment(ctx, segment); err != nil {
			return nil, err
		}
	}
	return alternatives, nil
}

// validateHostLocalSegment checks one host-local topology alternative. It must name a zone, and
// may additionally name either a physical host (topology.csi.vmware.com/host, a host MoID) or a
// Supervisor Node (kubernetes.io/hostname), but not both. Any other key is a constraint this
// driver cannot honor, so it is rejected rather than ignored.
func validateHostLocalSegment(ctx context.Context, segment map[string]string) error {
	log := logger.GetLogger(ctx)
	if segment[v1.LabelTopologyZone] == "" {
		return logger.LogNewErrorCodef(log, codes.InvalidArgument,
			"host-local topology alternative %v has no %s", segment, v1.LabelTopologyZone)
	}
	host, hasHost := segment[common.GuestClusterTopologyLabelHost]
	node, hasNode := segment[v1.LabelHostname]
	if hasHost && hasNode {
		return logger.LogNewErrorCodef(log, codes.InvalidArgument,
			"host-local topology alternative %v names both %s and %s", segment,
			common.GuestClusterTopologyLabelHost, v1.LabelHostname)
	}
	if (hasHost && host == "") || (hasNode && node == "") {
		return logger.LogNewErrorCodef(log, codes.InvalidArgument,
			"host-local topology alternative %v has an empty host constraint", segment)
	}
	for key := range segment {
		if key != v1.LabelTopologyZone && key != common.GuestClusterTopologyLabelHost && key != v1.LabelHostname {
			return logger.LogNewErrorCodef(log, codes.InvalidArgument,
				"host-local topology alternative %v has unsupported key %q", segment, key)
		}
	}
	return nil
}

// requestedZones returns the distinct zones named by the normal CSI topology requirement.
func requestedZones(requirement *csi.TopologyRequirement) []string {
	var zones []string
	for _, topology := range append(slices.Clone(requirement.GetRequisite()), requirement.GetPreferred()...) {
		if zone := topology.GetSegments()[v1.LabelTopologyZone]; zone != "" && !slices.Contains(zones, zone) {
			zones = append(zones, zone)
		}
	}
	return zones
}

// hostLocalInventory returns host MoID -> {zone, kubernetes.io/hostname: owning Node} for every
// ESXi host that can back a host-local volume in the requested zones.
func (c *controller) hostLocalInventory(ctx context.Context, vc *cnsvsphere.VirtualCenter, namespace string,
	requirement *csi.TopologyRequirement) (map[string]map[string]string, error) {
	log := logger.GetLogger(ctx)
	zones := requestedZones(requirement)
	if len(zones) == 0 {
		return nil, logger.LogNewErrorCodef(log, codes.InvalidArgument,
			"host-local volume provisioning requires zones in the accessibility requirement: %+v", requirement)
	}
	if namespace == "" {
		return nil, logger.LogNewErrorCodef(log, codes.InvalidArgument,
			"PVC namespace is required to resolve the namespace's active clusters for host-local provisioning")
	}
	owners, err := commonco.ContainerOrchestratorUtility.GetNodeIDtoNameMap(ctx)
	if err != nil {
		return nil, logger.LogNewErrorCodef(log, codes.Unavailable,
			"cannot resolve the Supervisor Node that owns each ESXi host; retrying host-local provisioning "+
				"rather than treating hosts as unowned. Error: %v", err)
	}
	inventory := make(map[string]map[string]string)
	for _, zone := range zones {
		clusters, err := commonco.ContainerOrchestratorUtility.GetActiveClustersForNamespaceInRequestedZones(
			ctx, namespace, []string{zone})
		if errors.Is(err, commoncotypes.ErrNoActiveClusters) {
			continue
		}
		if err != nil {
			return nil, logger.LogNewErrorCodef(log, codes.Internal,
				"failed to find active clusters in zone %q for namespace %q: %v", zone, namespace, err)
		}
		for _, cluster := range clusters {
			hosts, err := getClusterHostMoIDs(ctx, vc, cluster)
			if err != nil {
				return nil, logger.LogNewErrorCodef(log, codes.Internal,
					"failed to list hosts of cluster %q in zone %q: %v", cluster, zone, err)
			}
			for _, host := range hosts {
				owner := owners[host]
				if owner == "" {
					log.Infof("Host %q in cluster %q (zone %q) has no current owning Supervisor Node; "+
						"it is not a host-local candidate", host, cluster, zone)
					continue
				}
				if existing, ok := inventory[host]; ok && existing[v1.LabelTopologyZone] != zone {
					return nil, logger.LogNewErrorCodef(log, codes.Internal,
						"host %q belongs to conflicting zones %q and %q", host, existing[v1.LabelTopologyZone], zone)
				}
				inventory[host] = map[string]string{v1.LabelTopologyZone: zone, v1.LabelHostname: owner}
			}
		}
	}
	log.Debugf("Host-local inventory for zones %v: %v", zones, inventory)
	return inventory, nil
}

// segmentSelectsHost reports whether a topology segment admits the host:
//   - the zone must match;
//   - topology.csi.vmware.com/host selects exactly that host MoID;
//   - kubernetes.io/hostname selects the hosts owned by that Supervisor Node (one for a dedicated
//     Node, every host in its NodeMapping for a VirtualNode);
//   - a zone-only segment admits every owned host in the zone.
func segmentSelectsHost(segment map[string]string, host string, topology map[string]string) bool {
	if segment[v1.LabelTopologyZone] != topology[v1.LabelTopologyZone] {
		return false
	}
	if want, ok := segment[common.GuestClusterTopologyLabelHost]; ok && want != host {
		return false
	}
	if want, ok := segment[v1.LabelHostname]; ok && !strings.EqualFold(want, topology[v1.LabelHostname]) {
		return false
	}
	return true
}

// hostLocalCandidates intersects each host-local alternative with the inventory and with the
// normal CSI requirement, and returns the candidate hosts to supply to CNS together with each
// candidate's {zone, owning Node}.
//
// The alternatives come from cns.vmware.com/host-local-topology when the provisioner supplied it,
// otherwise from the normal requirement's own segments. Each alternative is evaluated as a whole,
// so a zone-only or VirtualNode alternative can admit several hosts while an explicit
// physical-host alternative is never widened beyond that host. A candidate must also be admitted
// by a normal requisite segment, so a host-local alternative never escapes the requested zones.
func hostLocalCandidates(ctx context.Context, requirement *csi.TopologyRequirement,
	alternatives []map[string]string, inventory map[string]map[string]string) (
	[]vimtypes.ManagedObjectReference, map[string]map[string]string, error) {
	log := logger.GetLogger(ctx)
	allowed := requirement.GetRequisite()
	if len(allowed) == 0 {
		allowed = requirement.GetPreferred()
	}
	if len(alternatives) == 0 {
		topologies := requirement.GetPreferred()
		if len(topologies) == 0 {
			topologies = requirement.GetRequisite()
		}
		for _, topology := range topologies {
			if err := validateHostLocalSegment(ctx, topology.GetSegments()); err != nil {
				return nil, nil, err
			}
			alternatives = append(alternatives, topology.GetSegments())
		}
	}
	if len(alternatives) == 0 || len(allowed) == 0 {
		return nil, nil, logger.LogNewErrorCodef(log, codes.InvalidArgument,
			"host-local volume provisioning requires topology alternatives; requirement: %+v", requirement)
	}
	selected := make(map[string]map[string]string)
	for _, alternative := range alternatives {
		for host, topology := range inventory {
			if _, done := selected[host]; done || !segmentSelectsHost(alternative, host, topology) {
				continue
			}
			for _, normal := range allowed {
				if segmentSelectsHost(normal.GetSegments(), host, topology) {
					selected[host] = maps.Clone(topology)
					break
				}
			}
		}
	}
	if len(selected) == 0 {
		return nil, nil, logger.LogNewErrorCodef(log, codes.FailedPrecondition,
			"no ESXi host with a current owning Supervisor Node in the namespace's clusters satisfies the "+
				"host-local topology %v within requirement %+v", alternatives, requirement)
	}
	hosts := slices.Sorted(maps.Keys(selected))
	refs := make([]vimtypes.ManagedObjectReference, 0, len(hosts))
	for _, host := range hosts {
		refs = append(refs, vimtypes.ManagedObjectReference{Type: "HostSystem", Value: host})
	}
	log.Infof("Host-local candidate hosts: %v", hosts)
	return refs, selected, nil
}

// recoverHostLocalVolumeHost derives placement from a host-exclusive datastore when the create result lacks a host.
func (c *controller) recoverHostLocalVolumeHost(ctx context.Context, vc *cnsvsphere.VirtualCenter,
	volume *cnsvolume.CnsVolumeInfo) (*vimtypes.ManagedObjectReference, error) {
	datastore, err := c.getDatastoreForVolume(ctx, vc, volume.VolumeID.Id, volume.DatastoreURL)
	if err != nil {
		return nil, err
	}
	mounts, err := datastoreHostMounts(ctx, datastore)
	if err != nil {
		return nil, fmt.Errorf("failed to get host mounts for datastore %q: %w", datastore.Info.Url, err)
	}
	if len(mounts) != 1 || mounts[0].Key.Value == "" {
		return nil, fmt.Errorf("datastore %q does not identify a single backing host (%d host mounts)",
			datastore.Info.Url, len(mounts))
	}
	return &mounts[0].Key, nil
}

// hostLocalVolumeResult returns the HostLocalStorage + PodVM v2 result of a provisioned host-local
// volume: zone-only accessible topology.
// The backing host must be one of the candidates supplied to CNS.
func hostLocalVolumeResult(ctx context.Context, candidates []vimtypes.ManagedObjectReference,
	topologies map[string]map[string]string, selectedHost *vimtypes.ManagedObjectReference,
	volumeID string) (map[string]string, string, error) {
	log := logger.GetLogger(ctx)
	if selectedHost == nil || selectedHost.Value == "" {
		return nil, "", logger.LogNewErrorf(log, "CNS did not return a host in the placement result "+
			"for host-local volume %q", volumeID)
	}
	if !slices.ContainsFunc(candidates, func(c vimtypes.ManagedObjectReference) bool {
		return c.Value == selectedHost.Value
	}) {
		return nil, "", logger.LogNewErrorf(log, "host %q selected by CNS for volume %q is not among the "+
			"candidate hosts %v", selectedHost.Value, volumeID, candidates)
	}
	host := selectedHost.Value
	zone := topologies[host][v1.LabelTopologyZone]
	if zone == "" {
		return nil, "", logger.LogNewErrorf(log, "no zone is known for host %q backing volume %q", host, volumeID)
	}
	return map[string]string{v1.LabelTopologyZone: zone}, host, nil
}
