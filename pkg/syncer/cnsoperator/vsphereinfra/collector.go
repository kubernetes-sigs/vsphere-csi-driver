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

// Package vsphereinfra provides a shared PropertyCollector watcher for the
// vSphere inventory.  It fires events when hosts or datastores are deleted from
// vCenter or when their topology relationships change (host leaves a cluster,
// datastore host list changes), allowing higher-level controllers to react
// without individually polling vCenter.
package vsphereinfra

import (
	"context"
	"time"

	"github.com/vmware/govmomi/property"
	"github.com/vmware/govmomi/view"
	"github.com/vmware/govmomi/vim25/types"
	cnsvsphere "sigs.k8s.io/vsphere-csi-driver/v3/pkg/common/cns-lib/vsphere"
	"sigs.k8s.io/vsphere-csi-driver/v3/pkg/csi/service/logger"
)

// restartDelay is how long manageWatcher waits before restarting the watcher
// after the session goroutine exits, to avoid tight reconnect loops against a
// temporarily unhealthy vCenter.
const restartDelay = time.Minute

// StartInventoryWatcher starts a long-running PropertyCollector listener that
// watches HostSystem, Datastore, and ClusterComputeResource objects across the
// entire vCenter inventory. It returns immediately; the watcher runs in the
// background.
func StartInventoryWatcher(ctx context.Context, vc *cnsvsphere.VirtualCenter) {
	go manageWatcher(ctx, vc)
}

// manageWatcher is the single long-lived goroutine that owns the watcher
// lifecycle.  It loops: run a session to completion, then either stop
// (ctx cancelled) or wait restartDelay and try again.
func manageWatcher(ctx context.Context, vc *cnsvsphere.VirtualCenter) {
	log := logger.GetLogger(ctx)
	for {
		runWatcherSession(ctx, vc)

		select {
		case <-ctx.Done():
			log.Infof("vsphereinfra: context cancelled, stopping inventory watcher")
			return
		case <-time.After(restartDelay):
			log.Infof("vsphereinfra: restarting inventory watcher after session exit")
		}
	}
}

// runWatcherSession runs the PropertyCollector session loop.  On each
// WaitForUpdatesEx error it immediately attempts to reconnect; only when the
// reconnect itself fails (or ctx is cancelled) does it return so manageWatcher
// can decide whether to restart.
func runWatcherSession(ctx context.Context, vc *cnsvsphere.VirtualCenter) {
	log := logger.GetLogger(ctx)
	// This runs in its own goroutine (see StartInventoryWatcher), so an
	// unrecovered panic here — e.g. from unexpected vCenter data — would crash
	// the entire syncer process instead of just this watcher. Recovering lets
	// manageWatcher's loop restart the session on the next iteration.
	defer func() {
		if r := recover(); r != nil {
			log.Infof("vsphereinfra: recovered panic in inventory watcher: %v", r)
		}
	}()

	if err := vc.Connect(ctx); err != nil {
		log.Errorf("vsphereinfra: cannot connect to vCenter: %v", err)
		return
	}

	log.Infof("vsphereinfra: starting PropertyCollector inventory watcher")

	for {
		// runPCSession blocks for the entire life of one WaitForUpdatesEx
		// long-poll session — it only returns when that session itself ends
		// (e.g. vCenter session expiry, network blip), not after each batch
		// of updates. So the reconnect below runs once per session teardown,
		// not once per callback fire.
		err := runPCSession(ctx, vc)

		if ctx.Err() != nil {
			log.Infof("vsphereinfra: context cancelled, stopping inventory watcher")
			return
		}

		if err != nil {
			log.Infof("vsphereinfra: PropertyCollector session ended (%v); reconnecting to vCenter", err)
		} else {
			log.Infof("vsphereinfra: PropertyCollector session ended cleanly; reconnecting to vCenter")
		}

		// Connect() no-ops if the existing session is still valid (checks
		// sessionMgr.UserSession first), so this is cheap even if the
		// session didn't actually need re-establishing.
		if connErr := vc.Connect(ctx); connErr != nil {
			log.Errorf("vsphereinfra: cannot reconnect to vCenter (%v); stopping watcher", connErr)
			return
		}
		log.Infof("vsphereinfra: reconnected to vCenter; resuming inventory watcher")
	}
}

// runPCSession creates a PropertyCollector, runs WaitForUpdatesEx until the
// session ends, and cleans up.
func runPCSession(ctx context.Context, vc *cnsvsphere.VirtualCenter) error {
	log := logger.GetLogger(ctx)

	pc, err := property.DefaultCollector(vc.Client.Client).Create(ctx)
	if err != nil {
		log.Errorf("vsphereinfra: cannot create PropertyCollector: %v", err)
		return err
	}
	// Use context.Background() instead of ctx: by the time this runs (e.g. on
	// shutdown), ctx may already be cancelled, which would make the Destroy
	// call fail before it ever reaches vCenter, leaking the PropertyCollector
	// session server-side. Same reasoning as storagepool/listener.go.
	defer func() { _ = pc.Destroy(context.Background()) }()

	filter, cv, err := buildWaitFilter(ctx, vc)
	if err != nil {
		log.Errorf("vsphereinfra: cannot build wait filter: %v", err)
		return err
	}
	defer func() { _ = cv.Destroy(context.Background()) }()

	return property.WaitForUpdatesEx(ctx, pc, filter, func(updates []types.ObjectUpdate) bool {
		events := collectEvents(ctx, updates)
		if len(events) > 0 {
			OnInventoryChange(ctx, events)
		}
		return false
	})
}

// buildWaitFilter creates a ContainerView over the entire vCenter inventory and
// returns a WaitFilter that tracks Datastore, HostSystem, and
// ClusterComputeResource objects.
func buildWaitFilter(
	ctx context.Context, vc *cnsvsphere.VirtualCenter) (*property.WaitFilter, *view.ContainerView, error) {
	log := logger.GetLogger(ctx)
	client := vc.Client.Client

	rootFolder := client.ServiceContent.RootFolder
	log.Infof("vsphereinfra: creating ContainerView rooted at %s", rootFolder.Value)

	viewMgr := view.NewManager(client)
	cv, err := viewMgr.CreateContainerView(ctx, rootFolder,
		[]string{"Datastore", "HostSystem", "ClusterComputeResource"},
		true,
	)
	if err != nil {
		return nil, nil, err
	}

	filter := new(property.WaitFilter)
	filter.Add(cv.Reference(), "ContainerView", nil,
		&types.TraversalSpec{
			Type: "ContainerView",
			Path: "view",
			Skip: types.NewBool(false),
		},
	)

	// Datastore.host — detect a host being added to or removed from a
	// datastore's mount list.
	// HostSystem — Leave (deletion or moving out of scope) is always tracked
	// implicitly regardless of PathSet. summary.config.product.version detects
	// ESXi version changes.
	// ClusterComputeResource.host — detect a host being added to or removed
	// from a cluster (see collectEvents doc for why this is watched instead
	// of HostSystem.parent).
	filter.Spec.PropSet = append(filter.Spec.PropSet,
		types.PropertySpec{Type: "Datastore", PathSet: []string{"host"}},
		types.PropertySpec{Type: "HostSystem", PathSet: []string{"summary.config.product.version"}},
		types.PropertySpec{Type: "ClusterComputeResource", PathSet: []string{"host"}},
	)

	log.Infof("vsphereinfra: ContainerView created, watching Datastore.host, " +
		"HostSystem version, and ClusterComputeResource.host")
	return filter, cv, nil
}

// collectEvents translates a batch of ObjectUpdates from WaitForUpdatesEx into
// InventoryEvents.
//
// Datastore updates:
//   - Enter: populates DsToHosts baseline (no event).
//   - Leave: clears cached state and emits EventDatastoreRemoved, carrying the
//     just-cleared policy names in its Policies field — the cache no longer
//     has them by the time OnInventoryChange sees this event, so it can't
//     resolve them the normal way.
//   - Modify on host: emits EventDatastoreHostChanged only when the set of
//     mounting hosts actually changed (added/removed), matching how InfraSPI
//     itself determines datastore accessibility — it checks HostSystem.datastore
//     list membership, not the per-host Mounted/Accessible flags, so a mount
//     flipping accessible without being added/removed isn't tracked as a
//     change here either. This also suppresses no-op re-publications from
//     DRS/HA.
//
// HostSystem updates:
//   - Leave: cleans up HostToVersion for the departed host. Cluster-membership
//     changes (including a host leaving visibility entirely) are detected via
//     ClusterComputeResource.host instead — see below — so Leave here doesn't
//     emit EventHostClusterChanged itself anymore.
//   - Enter/Modify on version: emits EventHostVersionChanged only when it
//     actually differs from the cached one; vCenter can re-publish this
//     property unchanged during unrelated churn (observed in practice), so
//     update.Kind alone isn't sufficient here.
//
// ClusterComputeResource updates:
//   - Enter: populates ClusterToHosts baseline (no event).
//   - Leave: the cluster is gone (deleted or moved out of scope). Every host
//     it last contained is treated as having left its cluster — emits
//     EventHostClusterChanged for each.
//   - Modify on host: diffs the new host array against the cached one and
//     emits EventHostClusterChanged for each host added or removed. This is
//     watched instead of HostSystem.parent because a host moving between two
//     clusters both visible to this session doesn't reliably fire any
//     particular update on the host's own parent — vCenter delivers that
//     non-deterministically depending on move direction. Watching the
//     cluster's own host array instead also gives the complete added/removed
//     set from a single Modify, rather than inferring it from scattered
//     per-host updates.
func collectEvents(ctx context.Context, updates []types.ObjectUpdate) []InventoryEvent {
	log := logger.GetLogger(ctx)
	cache := GetCache()
	var events []InventoryEvent

	for _, update := range updates {
		moref := update.Obj.Value

		switch update.Obj.Type {
		case "Datastore":
			switch update.Kind {
			case types.ObjectUpdateKindEnter:
				for _, change := range update.ChangeSet {
					if change.Name == "host" && isAssignOp(ctx, update.Obj.Type, moref, change) {
						cache.UpdateDsHosts(moref, extractHostSet(ctx, change.Val))
					}
				}

			case types.ObjectUpdateKindLeave:
				log.Infof("vsphereinfra: datastore %s removed from vCenter inventory", moref)
				removedPolicies := cache.InvalidateDatastore(moref)
				events = append(events, InventoryEvent{Kind: EventDatastoreRemoved, MoRef: moref, Policies: removedPolicies})

			case types.ObjectUpdateKindModify:
				for _, change := range update.ChangeSet {
					if change.Name == "host" && isAssignOp(ctx, update.Obj.Type, moref, change) {
						newHosts := extractHostSet(ctx, change.Val)
						if cache.UpdateDsHosts(moref, newHosts) {
							log.Infof("vsphereinfra: datastore %s host list changed", moref)
							events = append(events, InventoryEvent{Kind: EventDatastoreHostChanged, MoRef: moref})
						} else {
							log.Debugf("vsphereinfra: datastore %s host list re-published without change (DRS/HA noise), skipping", moref)
						}
					}
				}
			}

		case "HostSystem":
			switch update.Kind {
			case types.ObjectUpdateKindLeave:
				log.Infof("vsphereinfra: host %s removed from inventory or moved out of scope", moref)
				cache.InvalidateHostVersion(moref)

			case types.ObjectUpdateKindEnter, types.ObjectUpdateKindModify:
				for _, change := range update.ChangeSet {
					if change.Name != "summary.config.product.version" {
						continue
					}
					newVersion, ok := change.Val.(string)
					if !ok {
						log.Errorf("vsphereinfra: HostSystem.summary.config.product.version has unexpected type %T, want string",
							change.Val)
						continue
					}
					if _, changed := cache.UpdateHostVersion(moref, newVersion); changed {
						log.Infof("vsphereinfra: host %s ESXi version changed", moref)
						events = append(events, InventoryEvent{Kind: EventHostVersionChanged, MoRef: moref})
					}
				}
			}

		case "ClusterComputeResource":
			switch update.Kind {
			case types.ObjectUpdateKindEnter:
				for _, change := range update.ChangeSet {
					if change.Name == "host" && isAssignOp(ctx, update.Obj.Type, moref, change) {
						cache.UpdateClusterHosts(moref, extractHostRefSet(ctx, change.Val))
					}
				}

			case types.ObjectUpdateKindLeave:
				log.Infof("vsphereinfra: cluster %s removed from vCenter inventory or moved out of scope", moref)
				for _, hostID := range cache.InvalidateCluster(moref) {
					events = append(events, InventoryEvent{Kind: EventHostClusterChanged, MoRef: hostID})
				}

			case types.ObjectUpdateKindModify:
				for _, change := range update.ChangeSet {
					if change.Name != "host" || !isAssignOp(ctx, update.Obj.Type, moref, change) {
						continue
					}
					newHosts := extractHostRefSet(ctx, change.Val)
					added, removed := cache.UpdateClusterHosts(moref, newHosts)
					for _, hostID := range added {
						log.Infof("vsphereinfra: host %s added to cluster %s", hostID, moref)
						events = append(events, InventoryEvent{Kind: EventHostClusterChanged, MoRef: hostID})
					}
					for _, hostID := range removed {
						log.Infof("vsphereinfra: host %s removed from cluster %s", hostID, moref)
						events = append(events, InventoryEvent{Kind: EventHostClusterChanged, MoRef: hostID})
					}
					if len(added) == 0 && len(removed) == 0 {
						log.Debugf("vsphereinfra: cluster %s host list re-published without change (DRS/HA noise), skipping", moref)
					}
				}
			}
		}
	}

	return events
}

// isAssignOp reports whether change.Op is "assign".
func isAssignOp(ctx context.Context, objType, moref string, change types.PropertyChange) bool {
	log := logger.GetLogger(ctx)
	if change.Op != types.PropertyChangeOpAssign {
		log.Errorf("vsphereinfra: %s %s property %q change has unexpected op %q, want assign",
			objType, moref, change.Name, change.Op)
		return false
	}
	return true
}

// extractHostSet converts a Datastore.host PropertyChange value into the set
// of host morefs mounting it. The per-host Mounted/Accessible flags are
// deliberately ignored — InfraSPI's own accessible-datastore algorithm only
// checks HostSystem.datastore list membership, not those flags, so a mount
// flipping accessible without being added or removed isn't a change worth
// reporting here.
func extractHostSet(ctx context.Context, val types.AnyType) map[string]struct{} {
	log := logger.GetLogger(ctx)
	arr, ok := val.(types.ArrayOfDatastoreHostMount)
	if !ok {
		log.Errorf("vsphereinfra: Datastore.host value has unexpected type %T, want types.ArrayOfDatastoreHostMount", val)
		return nil
	}
	hosts := make(map[string]struct{}, len(arr.DatastoreHostMount))
	for _, m := range arr.DatastoreHostMount {
		hosts[m.Key.Value] = struct{}{}
	}
	return hosts
}

// extractHostRefSet converts a ClusterComputeResource.host PropertyChange
// value into the set of host morefs in that cluster.
func extractHostRefSet(ctx context.Context, val types.AnyType) map[string]struct{} {
	log := logger.GetLogger(ctx)
	arr, ok := val.(types.ArrayOfManagedObjectReference)
	if !ok {
		log.Errorf("vsphereinfra: ClusterComputeResource.host value has unexpected type %T, "+
			"want types.ArrayOfManagedObjectReference", val)
		return nil
	}
	hosts := make(map[string]struct{}, len(arr.ManagedObjectReference))
	for _, ref := range arr.ManagedObjectReference {
		hosts[ref.Value] = struct{}{}
	}
	return hosts
}
