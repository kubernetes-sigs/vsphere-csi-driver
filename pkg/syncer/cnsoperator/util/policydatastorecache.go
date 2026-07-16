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

package util

import "sync"

// PolicyDatastoreCache is a shared in-memory cache of storage-policy and zone
// datastore compatibility, populated by the clusterstoragepolicyinfo controller
// as a byproduct of reconciling InfraStoragePolicyInfo.
//
// This is kept separate from vsphereinfra.StoragePolicyInfoCache, which the
// PropertyCollector watcher keeps continuously up to date from live vCenter
// events: entries here are only as fresh as the last InfraStoragePolicyInfo
// reconcile. Keeping the two caches distinct avoids treating reconcile-cadence
// data as if it had the watcher's real-time freshness guarantees.
//
// It lets the namespace-scoped storagepolicyinfo controller re-derive
// namespace-scoped volume capabilities (e.g. SupportsLinkedClone restricted to
// the zones accessible to a given namespace) without any additional vCenter calls.
type PolicyDatastoreCache struct {
	mu sync.RWMutex

	// policyToDatastores tracks the last-known set of datastores compatible with
	// a storage policy, keyed by the policy's K8s-compliant name (the same name
	// used for the InfraStoragePolicyInfo/StoragePolicyInfo CRs). Inner key:
	// datastore moref.
	policyToDatastores map[string]map[string]struct{}

	// zoneToDatastores tracks the last-known set of datastores in each zone,
	// independent of any storage policy. Key: zone name; inner key: datastore
	// moref.
	zoneToDatastores map[string]map[string]struct{}
}

var (
	defaultPolicyDatastoreCache *PolicyDatastoreCache
	policyDatastoreCacheOnce    sync.Once
)

// GetPolicyDatastoreCache returns the process-wide singleton PolicyDatastoreCache.
// Safe to call from multiple goroutines; initialised exactly once.
func GetPolicyDatastoreCache() *PolicyDatastoreCache {
	policyDatastoreCacheOnce.Do(func() {
		defaultPolicyDatastoreCache = &PolicyDatastoreCache{
			policyToDatastores: make(map[string]map[string]struct{}),
			zoneToDatastores:   make(map[string]map[string]struct{}),
		}
	})
	return defaultPolicyDatastoreCache
}

// UpdatePolicyDatastores records the set of datastore morefs compatible with
// the storage policy identified by its K8s-compliant name.
func (c *PolicyDatastoreCache) UpdatePolicyDatastores(policyName string, dsIDs map[string]struct{}) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.policyToDatastores[policyName] = copyStringSet(dsIDs)
}

// GetPolicyDatastores returns a copy of the cached compatible-datastore set for
// a storage policy and whether the entry exists.
func (c *PolicyDatastoreCache) GetPolicyDatastores(policyName string) (map[string]struct{}, bool) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	ds, ok := c.policyToDatastores[policyName]
	if !ok {
		return nil, false
	}
	return copyStringSet(ds), true
}

// UpdateZoneDatastores records the set of datastore morefs present in the
// given zone, independent of any storage policy.
func (c *PolicyDatastoreCache) UpdateZoneDatastores(zone string, dsIDs map[string]struct{}) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.zoneToDatastores[zone] = copyStringSet(dsIDs)
}

// GetZoneDatastores returns a copy of the cached datastore set for a zone and
// whether the entry exists.
func (c *PolicyDatastoreCache) GetZoneDatastores(zone string) (map[string]struct{}, bool) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	ds, ok := c.zoneToDatastores[zone]
	if !ok {
		return nil, false
	}
	return copyStringSet(ds), true
}

// copyStringSet returns a copy of s so a caller mutating the result can't race
// with the cache's own locked reads/writes on the original map.
func copyStringSet(s map[string]struct{}) map[string]struct{} {
	cp := make(map[string]struct{}, len(s))
	for k := range s {
		cp[k] = struct{}{}
	}
	return cp
}
