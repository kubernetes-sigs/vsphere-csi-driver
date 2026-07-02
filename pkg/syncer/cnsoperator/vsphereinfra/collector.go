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
	"go.uber.org/zap"
	cnsvsphere "sigs.k8s.io/vsphere-csi-driver/v3/pkg/common/cns-lib/vsphere"
	"sigs.k8s.io/vsphere-csi-driver/v3/pkg/csi/service/logger"
)

// restartDelay is how long manageWatcher waits before restarting the watcher
// after the session goroutine exits, to avoid tight reconnect loops against a
// temporarily unhealthy vCenter.
const restartDelay = time.Minute

// StartInventoryWatcher starts a long-running PropertyCollector listener that
// watches HostSystem and Datastore objects across the entire vCenter inventory.
// It returns immediately; the watcher runs in the background.
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
	defer func() { _ = pc.Destroy(ctx) }()

	filter, cv, err := buildWaitFilter(ctx, vc)
	if err != nil {
		log.Errorf("vsphereinfra: cannot build wait filter: %v", err)
		return err
	}
	defer func() { _ = cv.Destroy(ctx) }()

	return property.WaitForUpdatesEx(ctx, pc, filter, func(updates []types.ObjectUpdate) bool {
		events := collectEvents(ctx, updates)
		if len(events) > 0 {
			OnInventoryChange(ctx, events)
		}
		return false
	})
}

// buildWaitFilter creates a ContainerView over the entire vCenter inventory and
// returns a WaitFilter that tracks Datastore and HostSystem objects.
func buildWaitFilter(ctx context.Context, vc *cnsvsphere.VirtualCenter) (*property.WaitFilter, *view.ContainerView, error) {
	log := logger.GetLogger(ctx)
	client := vc.Client.Client

	rootFolder := client.ServiceContent.RootFolder
	log.Infof("vsphereinfra: creating ContainerView rooted at %s", rootFolder.Value)

	viewMgr := view.NewManager(client)
	cv, err := viewMgr.CreateContainerView(ctx, rootFolder,
		[]string{"Datastore", "HostSystem"},
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

	// Datastore.host — detect mount/unmount and host accessibility changes.
	// HostSystem — Leave (deletion or moving out of scope) is always tracked
	// implicitly regardless of PathSet. summary.config.product.version
	// detects ESXi version changes on a host that stays in view.
	filter.Spec.PropSet = append(filter.Spec.PropSet,
		types.PropertySpec{Type: "Datastore", PathSet: []string{"host"}},
		types.PropertySpec{Type: "HostSystem", PathSet: []string{"summary.config.product.version"}},
	)

	log.Infof("vsphereinfra: ContainerView created, watching Datastore.host and HostSystem version/Leave")
	return filter, cv, nil
}

// collectEvents translates a batch of ObjectUpdates from WaitForUpdatesEx into
// InventoryEvents.
//
// Datastore updates:
//   - Enter: populates DsToHosts baseline (no event).
//   - Leave: emits EventDatastoreRemoved and clears cached state.
//   - Modify on host: emits EventDatastoreHostChanged only when content changed;
//     suppresses no-op re-publications from DRS/HA.
//
// HostSystem updates:
//   - Leave: the host is no longer visible to this session — deleted, or
//     moved to a cluster outside our scope. Either way it has left the
//     cluster(s) we can see, so emit EventHostClusterChanged.
//   - Enter: populates HostToVersion baseline (no event) — there is nothing
//     to compare a first sighting against.
//   - Modify on version: emits EventHostVersionChanged only when it actually
//     differs from the cached one; vCenter can re-publish this property
//     unchanged during unrelated churn (observed in practice), same as
//     HostSystem.parent and Datastore.host, so update.Kind alone isn't
//     sufficient here.
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
					if change.Name == "host" {
						cache.UpdateDsHosts(moref, extractHostMap(log, change.Val))
					}
				}

			case types.ObjectUpdateKindLeave:
				log.Infof("vsphereinfra: datastore %s removed from vCenter inventory", moref)
				cache.InvalidateDatastore(moref)
				events = append(events, InventoryEvent{Kind: EventDatastoreRemoved, MoRef: moref})

			case types.ObjectUpdateKindModify:
				for _, change := range update.ChangeSet {
					if change.Name == "host" {
						newHosts := extractHostMap(log, change.Val)
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
				log.Infof("vsphereinfra: host %s left cluster (removed from inventory or moved out of scope)", moref)
				cache.InvalidateHostVersion(moref)
				events = append(events, InventoryEvent{Kind: EventHostClusterChanged, MoRef: moref})

			case types.ObjectUpdateKindEnter, types.ObjectUpdateKindModify:
				for _, change := range update.ChangeSet {
					if change.Name != "summary.config.product.version" {
						continue
					}
					newVersion, ok := change.Val.(string)
					if !ok {
						log.Errorf("vsphereinfra: HostSystem.summary.config.product.version has unexpected type %T, want string", change.Val)
						continue
					}
					if _, changed := cache.UpdateHostVersion(moref, newVersion); changed {
						log.Infof("vsphereinfra: host %s ESXi version changed", moref)
						events = append(events, InventoryEvent{Kind: EventHostVersionChanged, MoRef: moref})
					}
				}
			}
		}
	}

	return events
}

// extractHostMap converts a Datastore.host PropertyChange value into a map of
// host moref → accessible flag.
func extractHostMap(log *zap.SugaredLogger, val types.AnyType) map[string]bool {
	arr, ok := val.(types.ArrayOfDatastoreHostMount)
	if !ok {
		log.Errorf("vsphereinfra: Datastore.host value has unexpected type %T, want types.ArrayOfDatastoreHostMount", val)
		return nil
	}
	hosts := make(map[string]bool, len(arr.DatastoreHostMount))
	for _, m := range arr.DatastoreHostMount {
		accessible := m.MountInfo.Accessible == nil || *m.MountInfo.Accessible
		hosts[m.Key.Value] = accessible
	}
	return hosts
}
