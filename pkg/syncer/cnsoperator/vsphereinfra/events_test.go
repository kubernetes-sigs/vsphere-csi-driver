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
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// withShortReconcileDelay shrinks policyReconcileDelay for the duration of a
// test, so coalescing tests don't have to wait out the real two minutes.
func withShortReconcileDelay(t *testing.T, d time.Duration) {
	t.Helper()
	orig := policyReconcileDelay
	policyReconcileDelay = d
	t.Cleanup(func() { policyReconcileDelay = orig })
}

// drainPolicyChanges reads exactly n values off PolicyChanges() within
// timeout, failing the test if fewer arrive in time.
func drainPolicyChanges(t *testing.T, n int, timeout time.Duration) []string {
	t.Helper()
	var got []string
	deadline := time.After(timeout)
	for i := 0; i < n; i++ {
		select {
		case p := <-PolicyChanges():
			got = append(got, p)
		case <-deadline:
			t.Fatalf("timed out waiting for policy change %d/%d, got so far: %v", i+1, n, got)
		}
	}
	return got
}

// assertNoPolicyChange fails the test if a policy change arrives within d.
func assertNoPolicyChange(t *testing.T, d time.Duration) {
	t.Helper()
	select {
	case p := <-PolicyChanges():
		t.Fatalf("unexpected policy change for %q before the coalescing window elapsed", p)
	case <-time.After(d):
	}
}

func TestOnInventoryChange_DatastoreEventResolvesToPolicy(t *testing.T) {
	withShortReconcileDelay(t, 20*time.Millisecond)
	ctx := context.Background()
	cache := GetCache()

	dsID := uniqueID(t, "ds")
	policy := uniqueID(t, "policy")
	cache.SetDatastoresForPolicy(policy, []string{dsID})

	OnInventoryChange(ctx, []InventoryEvent{{Kind: EventDatastoreHostChanged, MoRef: dsID}})

	got := drainPolicyChanges(t, 1, time.Second)
	assert.Equal(t, []string{policy}, got)
}

func TestOnInventoryChange_HostEventResolvesToPolicyViaDatastores(t *testing.T) {
	withShortReconcileDelay(t, 20*time.Millisecond)
	ctx := context.Background()
	cache := GetCache()

	hostID := uniqueID(t, "host")
	dsID := uniqueID(t, "ds")
	policy := uniqueID(t, "policy")
	cache.UpdateDsHosts(dsID, hostSet(hostID))
	cache.SetDatastoresForPolicy(policy, []string{dsID})

	OnInventoryChange(ctx, []InventoryEvent{{Kind: EventHostClusterChanged, MoRef: hostID}})

	got := drainPolicyChanges(t, 1, time.Second)
	assert.Equal(t, []string{policy}, got)
}

func TestOnInventoryChange_UnresolvedEventQueuesNothing(t *testing.T) {
	withShortReconcileDelay(t, 20*time.Millisecond)
	ctx := context.Background()

	// A moref with no cache entries (unknown datastore/host) must not panic
	// and must not produce a policy change.
	OnInventoryChange(ctx, []InventoryEvent{{Kind: EventHostVersionChanged, MoRef: uniqueID(t, "host")}})

	assertNoPolicyChange(t, 50*time.Millisecond)
}

func TestOnInventoryChange_MultipleEventsForSamePolicyCoalesceIntoOneReconcile(t *testing.T) {
	withShortReconcileDelay(t, 100*time.Millisecond)
	ctx := context.Background()
	cache := GetCache()

	policy := uniqueID(t, "policy")
	ds1, ds2 := uniqueID(t, "ds1"), uniqueID(t, "ds2")
	host := uniqueID(t, "host")
	cache.SetDatastoresForPolicy(policy, []string{ds1, ds2})
	cache.UpdateDsHosts(ds2, hostSet(host))

	// Simulate a burst: a host leaving a cluster fires events for both the
	// host and the datastores it mounts, all resolving to the same policy.
	OnInventoryChange(ctx, []InventoryEvent{{Kind: EventDatastoreHostChanged, MoRef: ds1}})
	time.Sleep(10 * time.Millisecond)
	OnInventoryChange(ctx, []InventoryEvent{{Kind: EventHostClusterChanged, MoRef: host}})
	time.Sleep(10 * time.Millisecond)
	OnInventoryChange(ctx, []InventoryEvent{{Kind: EventDatastoreHostChanged, MoRef: ds1}})

	// Nothing should be reconciled before the window (measured from the first
	// event) elapses.
	assertNoPolicyChange(t, 50*time.Millisecond)

	got := drainPolicyChanges(t, 1, time.Second)
	assert.Equal(t, []string{policy}, got, "the whole burst must collapse into a single reconcile")

	// And no second reconcile should follow for this burst.
	assertNoPolicyChange(t, 100*time.Millisecond)
}

func TestOnInventoryChange_DifferentPoliciesEachQueuedIndependently(t *testing.T) {
	withShortReconcileDelay(t, 20*time.Millisecond)
	ctx := context.Background()
	cache := GetCache()

	ds1, ds2 := uniqueID(t, "ds1"), uniqueID(t, "ds2")
	policy1, policy2 := uniqueID(t, "policy1"), uniqueID(t, "policy2")
	cache.SetDatastoresForPolicy(policy1, []string{ds1})
	cache.SetDatastoresForPolicy(policy2, []string{ds2})

	OnInventoryChange(ctx, []InventoryEvent{
		{Kind: EventDatastoreHostChanged, MoRef: ds1},
		{Kind: EventDatastoreHostChanged, MoRef: ds2},
	})

	got := drainPolicyChanges(t, 2, time.Second)
	assert.ElementsMatch(t, []string{policy1, policy2}, got)
}

func TestOnInventoryChange_DatastoreRemovedResolvesToPolicy(t *testing.T) {
	withShortReconcileDelay(t, 20*time.Millisecond)
	ctx := context.Background()

	dsID := uniqueID(t, "ds")
	policy := uniqueID(t, "policy")
	// By the time OnInventoryChange sees an EventDatastoreRemoved,
	// collectEvents has already invalidated DsToPolicy for this datastore —
	// PoliciesForDatastore(dsID) would return nothing. The event must carry
	// the pre-resolved policy names itself, in its Policies field, exactly as
	// collectEvents populates it from InvalidateDatastore's return value.
	OnInventoryChange(ctx, []InventoryEvent{{Kind: EventDatastoreRemoved, MoRef: dsID, Policies: []string{policy}}})

	got := drainPolicyChanges(t, 1, time.Second)
	assert.Equal(t, []string{policy}, got)
}

func TestOnInventoryChange_DatastoreRemovedWithNoRecordedPoliciesQueuesNothing(t *testing.T) {
	withShortReconcileDelay(t, 20*time.Millisecond)
	ctx := context.Background()

	// A datastore that had no recorded policies (Policies is nil, matching
	// InvalidateDatastore's return when nothing was recorded) must not queue
	// anything, and must not panic on a nil Policies slice.
	OnInventoryChange(ctx, []InventoryEvent{{Kind: EventDatastoreRemoved, MoRef: uniqueID(t, "ds")}})

	assertNoPolicyChange(t, 50*time.Millisecond)
}

func TestQueuePolicyReconcile_DropsWhenChannelFull(t *testing.T) {
	withShortReconcileDelay(t, 10*time.Millisecond)
	ctx := context.Background()

	ch := getPolicyChangeCh()
	require.Greater(t, cap(ch), 0)
	// Fill the channel so the send in queuePolicyReconcile's timer callback
	// hits its default (drop) branch instead of blocking forever.
	for len(ch) < cap(ch) {
		ch <- uniqueID(t, "filler")
	}
	defer func() {
		for len(ch) > 0 {
			<-ch
		}
	}()

	// Must not panic or hang even though the channel is full.
	queuePolicyReconcile(ctx, uniqueID(t, "policy"))
	time.Sleep(50 * time.Millisecond)
}
