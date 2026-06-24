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

import "sync"

// StoragePolicyInfoCache is a shared in-memory cache used by the
// PropertyCollector watcher and the storage-policy-info
// controllers.
type StoragePolicyInfoCache struct {
	mu sync.RWMutex

	// DsToHosts tracks the last-known set of hosts mounting each datastore.
	// Written by the PropertyCollector on Datastore.host changes.
	DsToHosts map[string]map[string]struct{}

	// TODO: not yet written or read by anything. Intended for the
	// clusterstoragepolicyinfo/storagepolicyinfo controllers to record which
	// storage policies a datastore is compatible with (after a PBM query), so
	// EventDatastoreHostChanged/EventHostClusterChanged handlers can look up
	// affected policies without a separate vCenter round trip. Wire this up
	// (and add real accessors) once those controllers consume this cache.
	DsToPolicy map[string][]string

	// HostToVersion tracks the last-known ESXi version string per host
	// (e.g. "9.2.0"). key: host moref.
	// Written by the PropertyCollector on HostSystem.summary.config.product.version
	// changes. Needed because vCenter can re-publish this property with an
	// unchanged value during unrelated churn.
	HostToVersion map[string]string

	// ClusterToHosts tracks the last-known set of hosts per cluster.
	// key: cluster moref; inner key: host moref.
	// Written by the PropertyCollector on ClusterComputeResource.host changes.
	// Diffing this (rather than watching HostSystem.parent per host) gives the
	// full added/removed host set for a cluster from a single Modify, and
	// matches how InfraSPI itself queries hosts (GetHostsByCluster, per known
	// zone-registered cluster).
	ClusterToHosts map[string]map[string]struct{}
}

var (
	defaultCache *StoragePolicyInfoCache
	cacheOnce    sync.Once
)

// GetCache returns the process-wide singleton StoragePolicyInfoCache.
// Safe to call from multiple goroutines; initialised exactly once.
func GetCache() *StoragePolicyInfoCache {
	cacheOnce.Do(func() {
		defaultCache = &StoragePolicyInfoCache{
			DsToHosts:      make(map[string]map[string]struct{}),
			DsToPolicy:     make(map[string][]string),
			HostToVersion:  make(map[string]string),
			ClusterToHosts: make(map[string]map[string]struct{}),
		}
	})
	return defaultCache
}

// InvalidateDatastore removes all cache entries associated with a deleted datastore.
func (c *StoragePolicyInfoCache) InvalidateDatastore(dsID string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	delete(c.DsToHosts, dsID)
	delete(c.DsToPolicy, dsID)
}

// UpdateDsHosts compares newHosts (the current set of host morefs mounting
// this datastore) against the cached state.  Returns true if the set of hosts
// changed, and updates the cache.  Returns false for identical
// re-publications (e.g. from DRS rebalancing or HA reconfiguration) so
// callers can suppress spurious events.  A nil newHosts (e.g. extractHostSet
// hitting an unexpected PropertyCollector type) is treated as an invalid
// update: the cache is left unchanged and false is returned, rather than
// caching the nil and causing a spurious "changed" event on the next valid
// update.
func (c *StoragePolicyInfoCache) UpdateDsHosts(dsID string, newHosts map[string]struct{}) bool {
	if newHosts == nil {
		return false
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	prev, ok := c.DsToHosts[dsID]
	if ok && hostSetsEqual(prev, newHosts) {
		return false
	}
	c.DsToHosts[dsID] = newHosts
	return true
}

// GetDsHosts returns a copy of the cached host set for a datastore and
// whether the entry exists. A copy is returned (rather than the cache's
// internal map) so a caller mutating the result can't race with the cache's
// own locked reads/writes on that same map, and can't mistake a retained
// reference for a live view that updates as the cache does.
func (c *StoragePolicyInfoCache) GetDsHosts(dsID string) (map[string]struct{}, bool) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	hosts, ok := c.DsToHosts[dsID]
	if !ok {
		return nil, false
	}
	cp := make(map[string]struct{}, len(hosts))
	for h := range hosts {
		cp[h] = struct{}{}
	}
	return cp, true
}

// UpdateHostVersion compares newVersion against the cached ESXi version for a
// host. Returns the previous value and whether it changed. On the first
// sighting for a host (no previous entry) changed is false — there is nothing
// yet to compare against.
func (c *StoragePolicyInfoCache) UpdateHostVersion(hostID, newVersion string) (oldVersion string, changed bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	prev, ok := c.HostToVersion[hostID]
	c.HostToVersion[hostID] = newVersion
	if !ok || prev == newVersion {
		return prev, false
	}
	return prev, true
}

// InvalidateHostVersion removes cached version info for a host that has left
// the inventory, so a moref reused later doesn't compare against stale data.
func (c *StoragePolicyInfoCache) InvalidateHostVersion(hostID string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	delete(c.HostToVersion, hostID)
}

// UpdateClusterHosts compares newHosts (the current set of host morefs in
// this cluster) against the cached state, and updates the cache. Returns the
// host morefs added and removed since the last known state. Returns nil, nil
// on the first sighting for a cluster (no previous entry) — that's the
// baseline, not a change — and also when the set is unchanged (e.g. a DRS/HA
// re-publication of the same host list). A nil newHosts (e.g. extractHostRefSet
// hitting an unexpected PropertyCollector type) is treated as an invalid
// update: the cache is left unchanged and nil, nil is returned, rather than
// clobbering the cached host list and reporting every previously-known host
// as removed.
func (c *StoragePolicyInfoCache) UpdateClusterHosts(
	clusterID string, newHosts map[string]struct{}) (added, removed []string) {
	if newHosts == nil {
		return nil, nil
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	prev, ok := c.ClusterToHosts[clusterID]
	c.ClusterToHosts[clusterID] = newHosts
	if !ok {
		return nil, nil
	}
	for h := range newHosts {
		if _, existed := prev[h]; !existed {
			added = append(added, h)
		}
	}
	for h := range prev {
		if _, still := newHosts[h]; !still {
			removed = append(removed, h)
		}
	}
	return added, removed
}

// InvalidateCluster removes cached host-list state for a cluster that has
// been deleted or moved out of scope, returning the host morefs it last
// contained so callers can treat each as having left its cluster.
func (c *StoragePolicyInfoCache) InvalidateCluster(clusterID string) []string {
	c.mu.Lock()
	defer c.mu.Unlock()
	hosts := c.ClusterToHosts[clusterID]
	delete(c.ClusterToHosts, clusterID)
	result := make([]string, 0, len(hosts))
	for h := range hosts {
		result = append(result, h)
	}
	return result
}

// hostSetsEqual returns true if both sets contain the same host morefs.
func hostSetsEqual(a, b map[string]struct{}) bool {
	if len(a) != len(b) {
		return false
	}
	for k := range a {
		if _, ok := b[k]; !ok {
			return false
		}
	}
	return true
}
