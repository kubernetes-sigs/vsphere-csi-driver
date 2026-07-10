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
	"sync"
	"time"

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

	// Policies carries pre-resolved policy names for EventDatastoreRemoved,
	// since collectEvents invalidates the cache's MoRef -> policy mapping
	// before this event is ever seen by OnInventoryChange — by then,
	// PoliciesForDatastore(MoRef) would just return nothing. Unused (nil) for
	// every other EventKind, which resolve to policies the normal way, via
	// the cache, in OnInventoryChange.
	Policies []string
}

// policyReconcileDelay is how long an affected storage policy sits in the
// coalescing queue, from the first event that names it, before being handed
// off for reconciliation. A single real-world change (e.g. a host moving
// clusters) can cascade into many individual PropertyCollector updates in a
// short burst — e.g. several datastores the host mounts each getting their
// own Datastore.host update — so this window absorbs the whole burst into a
// single reconcile per policy instead of one per update. It is a fixed
// window measured from the first event, not reset by later events in the
// same burst: real bursts complete in well under two minutes, and a fixed
// window keeps worst-case reconcile latency bounded.
//
// Not a const so tests can shrink it instead of waiting out the real delay.
var policyReconcileDelay = 2 * time.Minute

var (
	pendingPoliciesMu sync.Mutex
	// pendingPolicies is the set of storage policy names currently waiting out
	// policyReconcileDelay in the coalescing queue. Membership here is what
	// dedupes repeated events for the same policy within one burst down to a
	// single reconcile.
	pendingPolicies = make(map[string]struct{})
)

var (
	policyChangeChOnce sync.Once
	policyChangeCh     chan string
)

// PolicyChanges returns the channel on which storage policy names are
// delivered once they have cleared the coalescing queue, so controllers can
// re-reconcile the corresponding ClusterStoragePolicyInfo/InfraStoragePolicyInfo
// against the vCenter. Safe to call from multiple goroutines; the channel is
// created once and shared.
func PolicyChanges() <-chan string {
	return getPolicyChangeCh()
}

func getPolicyChangeCh() chan string {
	policyChangeChOnce.Do(func() {
		// Buffered so a slow or not-yet-started consumer doesn't stall the
		// PropertyCollector callback goroutine; see queuePolicyReconcile.
		policyChangeCh = make(chan string, 256)
	})
	return policyChangeCh
}

// OnInventoryChange is called whenever the PropertyCollector watcher detects a topology change.
// It resolves each event to the storage policies it can affect and queues
// those policies for coalesced reconciliation.
//
// DsToPolicy is in-memory only, so events arriving before it's been populated
// resolve to no policies and are silently dropped here — periodic slow-sync
// is what eventually catches those up, not this function.
func OnInventoryChange(ctx context.Context, events []InventoryEvent) {
	log := logger.GetLogger(ctx)
	cache := GetCache()

	// Resolve events to affected datastores. Datastore-scoped events already
	// name the datastore directly; host-scoped events are resolved via the
	// cache's reverse index, since a host change can affect every datastore
	// it mounts. EventDatastoreRemoved is handled separately below: by the
	// time this runs, collectEvents has already invalidated that datastore's
	// cache entry, so there's nothing left here to resolve it through.
	affectedDatastores := make(map[string]struct{})
	affectedPolicies := make(map[string]struct{})
	for _, ev := range events {
		switch ev.Kind {
		case EventDatastoreRemoved:
			for _, policy := range ev.Policies {
				affectedPolicies[policy] = struct{}{}
			}
		case EventDatastoreHostChanged:
			affectedDatastores[ev.MoRef] = struct{}{}
		case EventHostClusterChanged, EventHostVersionChanged:
			for _, dsID := range cache.DatastoresForHost(ev.MoRef) {
				affectedDatastores[dsID] = struct{}{}
			}
		}
	}

	// Resolve affected datastores to affected policies, deduping by policy:
	// several different events (and several different datastores) can resolve
	// to the same policy, and it must only be queued once per burst.
	for dsID := range affectedDatastores {
		for _, policy := range cache.PoliciesForDatastore(dsID) {
			affectedPolicies[policy] = struct{}{}
		}
	}

	log.Infof("vsphereinfra: %d inventory event(s) resolved to %d affected datastore(s) and %d affected policy(ies)",
		len(events), len(affectedDatastores), len(affectedPolicies))

	for policy := range affectedPolicies {
		queuePolicyReconcile(ctx, policy)
	}
}

// queuePolicyReconcile adds policyName to the coalescing queue if it isn't
// already waiting there, and schedules it to be sent on the PolicyChanges
// channel after policyReconcileDelay. Calling this again for a policy already
// in the queue is a no-op: that's the coalescing behavior — the whole burst
// of events for a policy is collapsed into the single reconcile already
// scheduled for it.
func queuePolicyReconcile(ctx context.Context, policyName string) {
	log := logger.GetLogger(ctx)

	pendingPoliciesMu.Lock()
	if _, alreadyQueued := pendingPolicies[policyName]; alreadyQueued {
		pendingPoliciesMu.Unlock()
		log.Debugf("vsphereinfra: policy %q already queued for reconciliation, coalescing", policyName)
		return
	}
	pendingPolicies[policyName] = struct{}{}
	pendingPoliciesMu.Unlock()

	log.Infof("vsphereinfra: policy %q queued for reconciliation in %s", policyName, policyReconcileDelay)
	time.AfterFunc(policyReconcileDelay, func() {
		pendingPoliciesMu.Lock()
		delete(pendingPolicies, policyName)
		pendingPoliciesMu.Unlock()

		select {
		case getPolicyChangeCh() <- policyName:
			log.Infof("vsphereinfra: policy %q pushed onto policy change channel", policyName)
		default:
			// The consumer isn't keeping up or hasn't started yet. Drop rather
			// than block this timer goroutine; the policy will be re-queued by
			// the next inventory event that affects it, or picked up by the
			// periodic slow-sync in the meantime.
			log.Warnf("vsphereinfra: policy change channel full, dropping reconcile for %q", policyName)
		}
	})
}
