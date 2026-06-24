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

	// DsToHosts tracks the last-known host set per datastore.
	// key: datastore moref; inner key: host moref; inner value: accessible flag.
	// Written by the PropertyCollector on Datastore.host changes.
	DsToHosts map[string]map[string]bool

	// TODO: not yet written or read by anything. Intended for the
	// clusterstoragepolicyinfo/storagepolicyinfo controllers to record which
	// storage policies a datastore is compatible with (after a PBM query), so
	// EventDatastoreHostChanged/EventHostClusterChanged handlers can look up
	// affected policies without a separate vCenter round trip. Wire this up
	// (and add real accessors) once those controllers consume this cache.
	DsToPolicy map[string][]string
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
			DsToHosts:  make(map[string]map[string]bool),
			DsToPolicy: make(map[string][]string),
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

// UpdateDsHosts compares newHosts (moref → accessible) against the cached state
// for the given datastore.  Returns true if the host set or any accessible flag
// changed, and updates the cache.  Returns false for identical re-publications
// (e.g. from DRS rebalancing or HA reconfiguration) so callers can suppress
// spurious events.
func (c *StoragePolicyInfoCache) UpdateDsHosts(dsID string, newHosts map[string]bool) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	prev, ok := c.DsToHosts[dsID]
	if ok && hostMapsEqual(prev, newHosts) {
		return false
	}
	c.DsToHosts[dsID] = newHosts
	return true
}

// GetDsHosts returns the cached host map for a datastore and whether the entry
// exists.
func (c *StoragePolicyInfoCache) GetDsHosts(dsID string) (map[string]bool, bool) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	hosts, ok := c.DsToHosts[dsID]
	return hosts, ok
}

// hostMapsEqual returns true if both maps have the same keys and values.
func hostMapsEqual(a, b map[string]bool) bool {
	if len(a) != len(b) {
		return false
	}
	for k, v := range a {
		if bv, ok := b[k]; !ok || bv != v {
			return false
		}
	}
	return true
}
