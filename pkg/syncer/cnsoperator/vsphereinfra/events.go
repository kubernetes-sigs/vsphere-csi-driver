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

	// EventDatastoreHostChanged fires when Datastore.host changes — on datastore
	// mount/unmount and when a host's accessible flag changes (e.g. host reboot
	// during an upgrade).
	EventDatastoreHostChanged

	// EventHostClusterChanged fires when a HostSystem is no longer visible to
	// this session — deleted from vCenter, or moved to a cluster outside this
	// session's scope.
	EventHostClusterChanged
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
	// TODO: implement — look up affected datastores via DsToHosts and enqueue for reconciliation.
}
