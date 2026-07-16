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
	"slices"
	"sync"
)

// StoragePolicyInfoCache is a shared in-memory cache used by the
// PropertyCollector watcher and the storage-policy-info
// controllers.
type StoragePolicyInfoCache struct {
	mu sync.RWMutex

	// DsToHosts tracks the last-known set of hosts mounting each datastore.
	// Written by the PropertyCollector on Datastore.host changes.
	DsToHosts map[string]map[string]struct{}

	// DsToPolicy records which storage policies a datastore is compatible
	// with, keyed by datastore moref. Written by the clusterstoragepolicyinfo
	// controller (SetDatastoresForPolicy) after each policy's PBM compatibility
	// query, and read by OnInventoryChange (via PoliciesForDatastore) to
	// resolve an inventory change to the policies it affects, without a
	// separate vCenter round trip.
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

	// ZoneToDatastores tracks the last-known set of datastores in each zone,
	// independent of any storage policy. key: zone name; inner key: datastore
	// moref. Written by util.GetPolicyCompatibleDatastoresPerZone as a byproduct
	// of its cluster→datastore enumeration, which runs regardless of which
	// policy triggered it, so the namespace-scoped storagepolicyinfo controller
	// can intersect a policy's compatible datastores (DsToPolicy) against a
	// namespace's accessible zones without any additional vCenter call.
	ZoneToDatastores map[string]map[string]struct{}

	// ClusterToESAEnabled records whether a cluster has vSAN-ESA enabled, keyed
	// by cluster moref. Written by the clusterstoragepolicyinfo controller
	// (isClusterESAEnabled) as a byproduct of its per-reconcile vSAN-ESA check,
	// so the namespace-scoped storagepolicyinfo controller can determine
	// SupportsHighPerformanceLinkedClone without any additional vCenter call.
	ClusterToESAEnabled map[string]bool
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
			DsToHosts:           make(map[string]map[string]struct{}),
			DsToPolicy:          make(map[string][]string),
			HostToVersion:       make(map[string]string),
			ClusterToHosts:      make(map[string]map[string]struct{}),
			ZoneToDatastores:    make(map[string]map[string]struct{}),
			ClusterToESAEnabled: make(map[string]bool),
		}
	})
	return defaultCache
}

// InvalidateDatastore removes all cache entries associated with a deleted
// datastore, returning the policy names it was last recorded as compatible
// with. A deleted datastore no longer resolves to anything once this returns
// (PoliciesForDatastore(dsID) goes back to empty), so callers that still need
// to notify those policies about the removal must use the returned list
// instead of trying to look it up afterwards.
func (c *StoragePolicyInfoCache) InvalidateDatastore(dsID string) []string {
	c.mu.Lock()
	defer c.mu.Unlock()
	policies := c.DsToPolicy[dsID]
	delete(c.DsToHosts, dsID)
	delete(c.DsToPolicy, dsID)
	if len(policies) == 0 {
		return nil
	}
	cp := make([]string, len(policies))
	copy(cp, policies)
	return cp
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

// DatastoresForHost returns the morefs of all datastores currently known to
// mount the given host, by scanning DsToHosts for membership.
func (c *StoragePolicyInfoCache) DatastoresForHost(hostID string) []string {
	c.mu.RLock()
	defer c.mu.RUnlock()
	var datastores []string
	for dsID, hosts := range c.DsToHosts {
		if _, ok := hosts[hostID]; ok {
			datastores = append(datastores, dsID)
		}
	}
	return datastores
}

// PoliciesForDatastore returns a copy of the K8s compliant names
// compatible with the given datastore.
func (c *StoragePolicyInfoCache) PoliciesForDatastore(dsID string) []string {
	c.mu.RLock()
	defer c.mu.RUnlock()
	policies := c.DsToPolicy[dsID]
	if len(policies) == 0 {
		return nil
	}
	cp := make([]string, len(policies))
	copy(cp, policies)
	return cp
}

// SetDatastoresForPolicy replaces the set of datastores recorded as
// compatible with policyName with dsIDs, so a datastore the policy is no
// longer compatible with doesn't keep a stale entry pointing back to it.
func (c *StoragePolicyInfoCache) SetDatastoresForPolicy(policyName string, dsIDs []string) {
	c.mu.Lock()
	defer c.mu.Unlock()

	stillCompatible := make(map[string]struct{}, len(dsIDs))
	for _, dsID := range dsIDs {
		stillCompatible[dsID] = struct{}{}
	}

	for dsID, policies := range c.DsToPolicy {
		if _, ok := stillCompatible[dsID]; ok {
			continue
		}
		policies = slices.DeleteFunc(policies, func(p string) bool { return p == policyName })
		if len(policies) == 0 {
			delete(c.DsToPolicy, dsID)
		} else {
			c.DsToPolicy[dsID] = policies
		}
	}

	for dsID := range stillCompatible {
		if !slices.Contains(c.DsToPolicy[dsID], policyName) {
			c.DsToPolicy[dsID] = append(c.DsToPolicy[dsID], policyName)
		}
	}
}

// SetZoneDatastores replaces the set of datastores recorded for zone.
func (c *StoragePolicyInfoCache) SetZoneDatastores(zone string, dsIDs []string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	set := make(map[string]struct{}, len(dsIDs))
	for _, dsID := range dsIDs {
		set[dsID] = struct{}{}
	}
	c.ZoneToDatastores[zone] = set
}

// GetZoneDatastores returns a copy of the cached datastore set for a zone and
// whether the entry exists.
func (c *StoragePolicyInfoCache) GetZoneDatastores(zone string) (map[string]struct{}, bool) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	ds, ok := c.ZoneToDatastores[zone]
	if !ok {
		return nil, false
	}
	cp := make(map[string]struct{}, len(ds))
	for id := range ds {
		cp[id] = struct{}{}
	}
	return cp, true
}

// GetDatastoresForPolicy returns the morefs of all datastores currently
// recorded (via DsToPolicy) as compatible with policyName, by scanning
// DsToPolicy for membership (mirrors DatastoresForHost).
func (c *StoragePolicyInfoCache) GetDatastoresForPolicy(policyName string) []string {
	c.mu.RLock()
	defer c.mu.RUnlock()
	var datastores []string
	for dsID, policies := range c.DsToPolicy {
		if slices.Contains(policies, policyName) {
			datastores = append(datastores, dsID)
		}
	}
	return datastores
}

// GetHostVersion returns the last-known ESXi version string for a host and
// whether it has been observed yet.
func (c *StoragePolicyInfoCache) GetHostVersion(hostID string) (version string, found bool) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	version, found = c.HostToVersion[hostID]
	return version, found
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
	delete(c.ClusterToESAEnabled, clusterID)
	result := make([]string, 0, len(hosts))
	for h := range hosts {
		result = append(result, h)
	}
	return result
}

// ClusterForHost returns the moref of the cluster the given host currently
// belongs to, by scanning ClusterToHosts for membership (mirrors
// DatastoresForHost). A host belongs to at most one cluster at steady state.
func (c *StoragePolicyInfoCache) ClusterForHost(hostID string) (string, bool) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	for clusterID, hosts := range c.ClusterToHosts {
		if _, ok := hosts[hostID]; ok {
			return clusterID, true
		}
	}
	return "", false
}

// SetClusterESAEnabled records whether the given cluster has vSAN-ESA enabled.
func (c *StoragePolicyInfoCache) SetClusterESAEnabled(clusterID string, esa bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.ClusterToESAEnabled[clusterID] = esa
}

// GetClusterESAEnabled returns the last-known vSAN-ESA state for a cluster and
// whether it has been observed/computed yet.
func (c *StoragePolicyInfoCache) GetClusterESAEnabled(clusterID string) (esa bool, found bool) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	esa, found = c.ClusterToESAEnabled[clusterID]
	return esa, found
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
