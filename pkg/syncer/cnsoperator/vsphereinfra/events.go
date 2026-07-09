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

package vsphereinfra

import (
	"context"

	"sigs.k8s.io/vsphere-csi-driver/v3/pkg/csi/service/logger"
)

// EventKind classifies a vSphere inventory change detected by the PropertyCollector.
type EventKind int

const (
	// EventDatastoreRemoved fires when a Datastore is deleted from vCenter entirely.
	EventDatastoreRemoved EventKind = iota

	// EventDatastoreHostChanged fires when the set of hosts mounting a
	// datastore changes — a host added or removed from Datastore.host.
	// Matches InfraSPI's own accessible-datastore algorithm, which checks
	// HostSystem.datastore list membership only; a mount's Accessible/Mounted
	// flag flipping without an add/remove isn't a change InfraSPI would
	// notice, so it isn't tracked as one here either.
	EventDatastoreHostChanged

	// EventHostClusterChanged fires when a host's cluster association changes
	// in any way this session can observe: it moved to a different cluster
	// (also visible to this session), its cluster is no longer visible at all
	// — deleted from vCenter, or moved out of this session's scope. Detected
	// via ClusterComputeResource.host (see collectEvents doc), not per-host
	// HostSystem.parent. The MoRef field carries the host moref; callers can
	// look up affected datastores via DsToHosts.
	EventHostClusterChanged

	// EventHostVersionChanged fires when a host's ESXi version changes (e.g.
	// an upgrade). The MoRef field carries the host moref.
	EventHostVersionChanged
)

// InventoryEvent describes a change detected by the PropertyCollector watcher.
type InventoryEvent struct {
	Kind  EventKind
	MoRef string // moref of the changed object — see EventKind docs for which object type
}

// OnInventoryChange is called whenever the PropertyCollector watcher detects a topology change.
func OnInventoryChange(ctx context.Context, events []InventoryEvent) {
	log := logger.GetLogger(ctx)
	log.Infof("vsphereinfra: OnInventoryChange not yet implemented, received %d event(s)", len(events))
	// TODO: coalesce events over a short window (e.g. a couple minutes) before
	// reconciling. A single real-world change (e.g. a host moving clusters) can
	// cascade into many individual PropertyCollector updates — e.g. several
	// datastores that host mounts each getting their own Datastore.host update
	// in the same burst — so without coalescing, the same affected storage
	// policy gets reconciled once per update instead of once for the whole
	// burst. Resolve events to affected policies (via DsToPolicy/DsToHosts)
	// and dedupe by policy ID, not by raw event, since several different
	// events can resolve to the same policy.
	// TODO: implement — look up affected datastores via DsToHosts and enqueue for reconciliation.
}
