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
	"fmt"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/vmware/govmomi/vim25/types"
)

// collectEvents reads/writes the process-wide GetCache() singleton, so tests
// in this file use morefs unique to the calling test (via uniqueID) to avoid
// cross-test state collisions when run in the same binary.
func uniqueID(t *testing.T, base string) string {
	t.Helper()
	return fmt.Sprintf("%s-%s", base, strings.NewReplacer("/", "_", " ", "_").Replace(t.Name()))
}

func datastoreHostMountVal(hosts map[string]bool) types.ArrayOfDatastoreHostMount {
	mounts := make([]types.DatastoreHostMount, 0, len(hosts))
	for host, accessible := range hosts {
		accessible := accessible
		mounts = append(mounts, types.DatastoreHostMount{
			Key: types.ManagedObjectReference{Type: "HostSystem", Value: host},
			MountInfo: types.HostMountInfo{
				AccessMode: "readWrite",
				Mounted:    &accessible,
				Accessible: &accessible,
			},
		})
	}
	return types.ArrayOfDatastoreHostMount{DatastoreHostMount: mounts}
}

func clusterHostRefsVal(hosts ...string) types.ArrayOfManagedObjectReference {
	refs := make([]types.ManagedObjectReference, 0, len(hosts))
	for _, h := range hosts {
		refs = append(refs, types.ManagedObjectReference{Type: "HostSystem", Value: h})
	}
	return types.ArrayOfManagedObjectReference{ManagedObjectReference: refs}
}

func datastoreUpdate(moref string, kind types.ObjectUpdateKind, hosts map[string]bool) types.ObjectUpdate {
	return types.ObjectUpdate{
		Obj:  types.ManagedObjectReference{Type: "Datastore", Value: moref},
		Kind: kind,
		ChangeSet: []types.PropertyChange{
			// Op: assign matches real vCenter behavior for Datastore.host,
			// confirmed against live vCenter 9.2.0 — see isAssignOp.
			{Name: "host", Op: types.PropertyChangeOpAssign, Val: datastoreHostMountVal(hosts)},
		},
	}
}

func clusterUpdate(moref string, kind types.ObjectUpdateKind, hosts ...string) types.ObjectUpdate {
	return types.ObjectUpdate{
		Obj:  types.ManagedObjectReference{Type: "ClusterComputeResource", Value: moref},
		Kind: kind,
		ChangeSet: []types.PropertyChange{
			// Op: assign matches real vCenter behavior for
			// ClusterComputeResource.host, confirmed against live vCenter
			// 9.2.0 — see isAssignOp.
			{Name: "host", Op: types.PropertyChangeOpAssign, Val: clusterHostRefsVal(hosts...)},
		},
	}
}

func clusterLeaveUpdate(moref string) types.ObjectUpdate {
	return types.ObjectUpdate{
		Obj:  types.ManagedObjectReference{Type: "ClusterComputeResource", Value: moref},
		Kind: types.ObjectUpdateKindLeave,
	}
}

func hostVersionUpdate(moref string, kind types.ObjectUpdateKind, version string) types.ObjectUpdate {
	return types.ObjectUpdate{
		Obj:  types.ManagedObjectReference{Type: "HostSystem", Value: moref},
		Kind: kind,
		ChangeSet: []types.PropertyChange{
			{Name: "summary.config.product.version", Val: version},
		},
	}
}

func TestCollectEvents_DatastoreModify_NonAssignOp_Ignored(t *testing.T) {
	ds := uniqueID(t, "ds")
	// Establish a valid baseline via Enter (Op: assign).
	collectEvents(context.Background(), []types.ObjectUpdate{
		datastoreUpdate(ds, types.ObjectUpdateKindEnter, map[string]bool{"host-1": true}),
	})

	// A Modify with a non-assign Op must not be treated as the complete host
	// set — the cache must stay untouched and no event should fire.
	events := collectEvents(context.Background(), []types.ObjectUpdate{
		{
			Obj:  types.ManagedObjectReference{Type: "Datastore", Value: ds},
			Kind: types.ObjectUpdateKindModify,
			ChangeSet: []types.PropertyChange{
				{Name: "host", Op: types.PropertyChangeOpRemove, Val: datastoreHostMountVal(map[string]bool{"host-2": true})},
			},
		},
	})
	assert.Empty(t, events)

	hosts, ok := GetCache().GetDsHosts(ds)
	assert.True(t, ok)
	assert.Equal(t, hostSet("host-1"), hosts, "cache must be untouched by a non-assign op")
}

func TestCollectEvents_ClusterModify_NonAssignOp_Ignored(t *testing.T) {
	cluster := uniqueID(t, "cluster")
	// Establish a valid baseline via Enter (Op: assign).
	collectEvents(context.Background(), []types.ObjectUpdate{
		clusterUpdate(cluster, types.ObjectUpdateKindEnter, "host-1", "host-2"),
	})

	// A Modify with a non-assign Op must not be treated as the complete host
	// set — the cache must stay untouched and no event should fire (in
	// particular, it must not report host-1/host-2 as removed).
	events := collectEvents(context.Background(), []types.ObjectUpdate{
		{
			Obj:  types.ManagedObjectReference{Type: "ClusterComputeResource", Value: cluster},
			Kind: types.ObjectUpdateKindModify,
			ChangeSet: []types.PropertyChange{
				{Name: "host", Op: types.PropertyChangeOpAdd, Val: clusterHostRefsVal("host-3")},
			},
		},
	})
	assert.Empty(t, events)
	assert.Equal(t, hostSet("host-1", "host-2"), GetCache().ClusterToHosts[cluster],
		"cache must be untouched by a non-assign op")
}

func TestCollectEvents_DatastoreEnter_NoEvent(t *testing.T) {
	ds := uniqueID(t, "ds")
	events := collectEvents(context.Background(), []types.ObjectUpdate{
		datastoreUpdate(ds, types.ObjectUpdateKindEnter, map[string]bool{"host-1": true}),
	})
	assert.Empty(t, events)

	hosts, ok := GetCache().GetDsHosts(ds)
	assert.True(t, ok)
	assert.Equal(t, hostSet("host-1"), hosts)
}

func TestCollectEvents_DatastoreModify_HostAdded(t *testing.T) {
	ds := uniqueID(t, "ds")
	collectEvents(context.Background(), []types.ObjectUpdate{
		datastoreUpdate(ds, types.ObjectUpdateKindEnter, map[string]bool{"host-1": true}),
	})

	events := collectEvents(context.Background(), []types.ObjectUpdate{
		datastoreUpdate(ds, types.ObjectUpdateKindModify, map[string]bool{"host-1": true, "host-2": true}),
	})

	assert.Equal(t, []InventoryEvent{{Kind: EventDatastoreHostChanged, MoRef: ds}}, events)
}

func TestCollectEvents_DatastoreModify_AccessibleFlagOnly_NoEvent(t *testing.T) {
	// Regression test: unmounting a datastore from a host flips
	// Mounted/Accessible without removing it from the host list. InfraSPI's
	// own algorithm never looks at those flags, so this must not fire
	// EventDatastoreHostChanged.
	ds := uniqueID(t, "ds")
	collectEvents(context.Background(), []types.ObjectUpdate{
		datastoreUpdate(ds, types.ObjectUpdateKindEnter, map[string]bool{"host-1": true}),
	})

	events := collectEvents(context.Background(), []types.ObjectUpdate{
		datastoreUpdate(ds, types.ObjectUpdateKindModify, map[string]bool{"host-1": false}),
	})

	assert.Empty(t, events)
}

func TestCollectEvents_DatastoreModify_NoChange_DRSNoise(t *testing.T) {
	ds := uniqueID(t, "ds")
	collectEvents(context.Background(), []types.ObjectUpdate{
		datastoreUpdate(ds, types.ObjectUpdateKindEnter, map[string]bool{"host-1": true}),
	})

	events := collectEvents(context.Background(), []types.ObjectUpdate{
		datastoreUpdate(ds, types.ObjectUpdateKindModify, map[string]bool{"host-1": true}),
	})

	assert.Empty(t, events)
}

func TestCollectEvents_DatastoreLeave(t *testing.T) {
	ds := uniqueID(t, "ds")
	collectEvents(context.Background(), []types.ObjectUpdate{
		datastoreUpdate(ds, types.ObjectUpdateKindEnter, map[string]bool{"host-1": true}),
	})

	events := collectEvents(context.Background(), []types.ObjectUpdate{
		{Obj: types.ManagedObjectReference{Type: "Datastore", Value: ds}, Kind: types.ObjectUpdateKindLeave},
	})

	assert.Equal(t, []InventoryEvent{{Kind: EventDatastoreRemoved, MoRef: ds}}, events)
	_, ok := GetCache().GetDsHosts(ds)
	assert.False(t, ok)
}

func TestCollectEvents_HostVersion_Changed(t *testing.T) {
	host := uniqueID(t, "host")
	collectEvents(context.Background(), []types.ObjectUpdate{
		hostVersionUpdate(host, types.ObjectUpdateKindEnter, "9.0.0"),
	})

	events := collectEvents(context.Background(), []types.ObjectUpdate{
		hostVersionUpdate(host, types.ObjectUpdateKindModify, "9.1.0"),
	})

	assert.Equal(t, []InventoryEvent{{Kind: EventHostVersionChanged, MoRef: host}}, events)
}

func TestCollectEvents_HostVersion_Unchanged_NoEvent(t *testing.T) {
	// Regression test: vCenter can re-publish summary.config.product.version
	// unchanged during unrelated churn (observed: multiple sibling hosts
	// firing Modify with identical old/new version at the same instant).
	host := uniqueID(t, "host")
	collectEvents(context.Background(), []types.ObjectUpdate{
		hostVersionUpdate(host, types.ObjectUpdateKindEnter, "9.2.0"),
	})

	events := collectEvents(context.Background(), []types.ObjectUpdate{
		hostVersionUpdate(host, types.ObjectUpdateKindModify, "9.2.0"),
	})

	assert.Empty(t, events)
}

func TestCollectEvents_HostVersion_UnexpectedType_NoEventNoPanic(t *testing.T) {
	host := uniqueID(t, "host")
	events := collectEvents(context.Background(), []types.ObjectUpdate{
		{
			Obj:  types.ManagedObjectReference{Type: "HostSystem", Value: host},
			Kind: types.ObjectUpdateKindEnter,
			ChangeSet: []types.PropertyChange{
				{Name: "summary.config.product.version", Val: 42}, // wrong type: int, not string
			},
		},
	})
	assert.Empty(t, events)
}

func TestCollectEvents_HostSystemModify_IgnoresUnrelatedChangeSetEntry(t *testing.T) {
	host := uniqueID(t, "host")
	events := collectEvents(context.Background(), []types.ObjectUpdate{
		{
			Obj:  types.ManagedObjectReference{Type: "HostSystem", Value: host},
			Kind: types.ObjectUpdateKindModify,
			ChangeSet: []types.PropertyChange{
				{Name: "runtime.connectionState", Val: "connected"},
			},
		},
	})
	assert.Empty(t, events)
}

func TestCollectEvents_ClusterModify_IgnoresUnrelatedChangeSetEntry(t *testing.T) {
	cluster := uniqueID(t, "cluster")
	events := collectEvents(context.Background(), []types.ObjectUpdate{
		{
			Obj:  types.ManagedObjectReference{Type: "ClusterComputeResource", Value: cluster},
			Kind: types.ObjectUpdateKindModify,
			ChangeSet: []types.PropertyChange{
				{Name: "name", Val: "some-cluster-name"},
			},
		},
	})
	assert.Empty(t, events)
}

func TestCollectEvents_HostSystemLeave_NoLongerEmitsClusterEvent(t *testing.T) {
	// HostSystem Leave used to emit EventHostClusterChanged directly; cluster
	// membership is now tracked via ClusterComputeResource.host instead, so
	// Leave should only clean up HostToVersion and emit nothing.
	host := uniqueID(t, "host")
	collectEvents(context.Background(), []types.ObjectUpdate{
		hostVersionUpdate(host, types.ObjectUpdateKindEnter, "9.2.0"),
	})

	events := collectEvents(context.Background(), []types.ObjectUpdate{
		{Obj: types.ManagedObjectReference{Type: "HostSystem", Value: host}, Kind: types.ObjectUpdateKindLeave},
	})

	assert.Empty(t, events)

	// version cache was cleared, so a re-appearance with any version is
	// treated as a fresh baseline (no event), not a "change" from "9.2.0".
	events = collectEvents(context.Background(), []types.ObjectUpdate{
		hostVersionUpdate(host, types.ObjectUpdateKindModify, "1.0.0"),
	})
	assert.Empty(t, events)
}

func TestCollectEvents_ClusterEnter_NoEvent(t *testing.T) {
	cluster := uniqueID(t, "cluster")
	events := collectEvents(context.Background(), []types.ObjectUpdate{
		clusterUpdate(cluster, types.ObjectUpdateKindEnter, "host-1", "host-2"),
	})
	assert.Empty(t, events)
}

func TestCollectEvents_ClusterModify_HostAdded(t *testing.T) {
	cluster := uniqueID(t, "cluster")
	collectEvents(context.Background(), []types.ObjectUpdate{
		clusterUpdate(cluster, types.ObjectUpdateKindEnter, "host-1"),
	})

	events := collectEvents(context.Background(), []types.ObjectUpdate{
		clusterUpdate(cluster, types.ObjectUpdateKindModify, "host-1", "host-2"),
	})

	assert.Equal(t, []InventoryEvent{{Kind: EventHostClusterChanged, MoRef: "host-2"}}, events)
}

func TestCollectEvents_ClusterModify_HostRemoved(t *testing.T) {
	cluster := uniqueID(t, "cluster")
	collectEvents(context.Background(), []types.ObjectUpdate{
		clusterUpdate(cluster, types.ObjectUpdateKindEnter, "host-1", "host-2"),
	})

	events := collectEvents(context.Background(), []types.ObjectUpdate{
		clusterUpdate(cluster, types.ObjectUpdateKindModify, "host-1"),
	})

	assert.Equal(t, []InventoryEvent{{Kind: EventHostClusterChanged, MoRef: "host-2"}}, events)
}

func TestCollectEvents_ClusterModify_HostSwap_EmitsBoth(t *testing.T) {
	cluster := uniqueID(t, "cluster")
	collectEvents(context.Background(), []types.ObjectUpdate{
		clusterUpdate(cluster, types.ObjectUpdateKindEnter, "host-1", "host-2"),
	})

	events := collectEvents(context.Background(), []types.ObjectUpdate{
		clusterUpdate(cluster, types.ObjectUpdateKindModify, "host-1", "host-3"),
	})

	morefs := make([]string, 0, len(events))
	for _, e := range events {
		assert.Equal(t, EventHostClusterChanged, e.Kind)
		morefs = append(morefs, e.MoRef)
	}
	assert.ElementsMatch(t, []string{"host-2", "host-3"}, morefs)
}

func TestCollectEvents_ClusterModify_NoChange_DRSNoise(t *testing.T) {
	cluster := uniqueID(t, "cluster")
	collectEvents(context.Background(), []types.ObjectUpdate{
		clusterUpdate(cluster, types.ObjectUpdateKindEnter, "host-1", "host-2"),
	})

	events := collectEvents(context.Background(), []types.ObjectUpdate{
		clusterUpdate(cluster, types.ObjectUpdateKindModify, "host-1", "host-2"),
	})

	assert.Empty(t, events)
}

func TestCollectEvents_ClusterLeave_EmitsForEveryLastKnownHost(t *testing.T) {
	cluster := uniqueID(t, "cluster")
	collectEvents(context.Background(), []types.ObjectUpdate{
		clusterUpdate(cluster, types.ObjectUpdateKindEnter, "host-1", "host-2"),
	})

	events := collectEvents(context.Background(), []types.ObjectUpdate{
		clusterLeaveUpdate(cluster),
	})

	morefs := make([]string, 0, len(events))
	for _, e := range events {
		assert.Equal(t, EventHostClusterChanged, e.Kind)
		morefs = append(morefs, e.MoRef)
	}
	assert.ElementsMatch(t, []string{"host-1", "host-2"}, morefs)
}

func TestCollectEvents_ClusterLeave_UnknownCluster_NoEvent(t *testing.T) {
	cluster := uniqueID(t, "cluster")
	events := collectEvents(context.Background(), []types.ObjectUpdate{
		clusterLeaveUpdate(cluster),
	})
	assert.Empty(t, events)
}

func TestExtractHostSet(t *testing.T) {
	ctx := context.Background()

	t.Run("valid type", func(t *testing.T) {
		val := datastoreHostMountVal(map[string]bool{"host-1": true, "host-2": false})
		hosts := extractHostSet(ctx, val)
		assert.Equal(t, hostSet("host-1", "host-2"), hosts)
	})

	t.Run("unexpected type returns nil", func(t *testing.T) {
		hosts := extractHostSet(ctx, "not-the-right-type")
		assert.Nil(t, hosts)
	})
}

func TestExtractHostRefSet(t *testing.T) {
	ctx := context.Background()

	t.Run("valid type", func(t *testing.T) {
		val := clusterHostRefsVal("host-1", "host-2")
		hosts := extractHostRefSet(ctx, val)
		assert.Equal(t, hostSet("host-1", "host-2"), hosts)
	})

	t.Run("unexpected type returns nil", func(t *testing.T) {
		hosts := extractHostRefSet(ctx, "not-the-right-type")
		assert.Nil(t, hosts)
	})
}
