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
	"testing"

	"github.com/stretchr/testify/assert"
)

// newTestCache returns a fresh, independent StoragePolicyInfoCache instead of
// the process-wide GetCache() singleton, so cache_test.go cases never share
// state with each other or with collector_test.go.
func newTestCache() *StoragePolicyInfoCache {
	return &StoragePolicyInfoCache{
		DsToHosts:      make(map[string]map[string]struct{}),
		DsToPolicy:     make(map[string][]string),
		HostToVersion:  make(map[string]string),
		ClusterToHosts: make(map[string]map[string]struct{}),
	}
}

func hostSet(hosts ...string) map[string]struct{} {
	set := make(map[string]struct{}, len(hosts))
	for _, h := range hosts {
		set[h] = struct{}{}
	}
	return set
}

func TestUpdateDsHosts(t *testing.T) {
	t.Run("first sighting reports changed", func(t *testing.T) {
		c := newTestCache()
		// Note: unlike UpdateHostVersion/UpdateClusterHosts, UpdateDsHosts
		// reports changed=true on first sighting. This is harmless in
		// practice: collectEvents only checks the return value on Modify,
		// and Enter (which always precedes Modify for a newly-viewed object)
		// discards it.
		changed := c.UpdateDsHosts("ds-1", hostSet("host-1"))
		assert.True(t, changed)
	})

	t.Run("identical host set reports unchanged", func(t *testing.T) {
		c := newTestCache()
		c.UpdateDsHosts("ds-1", hostSet("host-1", "host-2"))
		changed := c.UpdateDsHosts("ds-1", hostSet("host-1", "host-2"))
		assert.False(t, changed)
	})

	t.Run("host added reports changed", func(t *testing.T) {
		c := newTestCache()
		c.UpdateDsHosts("ds-1", hostSet("host-1"))
		changed := c.UpdateDsHosts("ds-1", hostSet("host-1", "host-2"))
		assert.True(t, changed)
	})

	t.Run("host removed reports changed", func(t *testing.T) {
		c := newTestCache()
		c.UpdateDsHosts("ds-1", hostSet("host-1", "host-2"))
		changed := c.UpdateDsHosts("ds-1", hostSet("host-1"))
		assert.True(t, changed)
	})

	t.Run("same size different members reports changed", func(t *testing.T) {
		c := newTestCache()
		c.UpdateDsHosts("ds-1", hostSet("host-1"))
		changed := c.UpdateDsHosts("ds-1", hostSet("host-2"))
		assert.True(t, changed)
	})

	t.Run("nil host set is an invalid update and leaves an unpopulated entry absent", func(t *testing.T) {
		c := newTestCache()
		changed := c.UpdateDsHosts("ds-1", nil)
		assert.False(t, changed)
		_, ok := c.GetDsHosts("ds-1")
		assert.False(t, ok, "a nil update must not fabricate a cache entry")
	})

	t.Run("nil host set is an invalid update and leaves a populated entry untouched", func(t *testing.T) {
		c := newTestCache()
		c.UpdateDsHosts("ds-1", hostSet("host-1", "host-2"))
		changed := c.UpdateDsHosts("ds-1", nil)
		assert.False(t, changed)
		hosts, ok := c.GetDsHosts("ds-1")
		assert.True(t, ok)
		assert.Equal(t, hostSet("host-1", "host-2"), hosts)
	})
}

func TestGetDsHosts(t *testing.T) {
	c := newTestCache()

	_, ok := c.GetDsHosts("ds-1")
	assert.False(t, ok, "unpopulated datastore should not be found")

	c.UpdateDsHosts("ds-1", hostSet("host-1"))
	hosts, ok := c.GetDsHosts("ds-1")
	assert.True(t, ok)
	assert.Equal(t, hostSet("host-1"), hosts)

	// The returned map must be a copy: mutating it must not affect the cache's
	// internal state.
	hosts["host-2"] = struct{}{}
	internal, _ := c.GetDsHosts("ds-1")
	assert.Equal(t, hostSet("host-1"), internal, "mutating the returned map must not leak into the cache")
}

func TestInvalidateDatastore(t *testing.T) {
	c := newTestCache()
	c.UpdateDsHosts("ds-1", hostSet("host-1"))
	c.DsToPolicy["ds-1"] = []string{"policy-1"}

	c.InvalidateDatastore("ds-1")

	_, ok := c.GetDsHosts("ds-1")
	assert.False(t, ok)
	_, ok = c.DsToPolicy["ds-1"]
	assert.False(t, ok)
}

func TestUpdateHostVersion(t *testing.T) {
	t.Run("first sighting is baseline, not a change", func(t *testing.T) {
		c := newTestCache()
		old, changed := c.UpdateHostVersion("host-1", "9.2.0")
		assert.False(t, changed)
		assert.Equal(t, "", old)
	})

	t.Run("identical version reports unchanged", func(t *testing.T) {
		c := newTestCache()
		c.UpdateHostVersion("host-1", "9.2.0")
		_, changed := c.UpdateHostVersion("host-1", "9.2.0")
		assert.False(t, changed)
	})

	t.Run("different version reports changed with old value", func(t *testing.T) {
		c := newTestCache()
		c.UpdateHostVersion("host-1", "9.0.0")
		old, changed := c.UpdateHostVersion("host-1", "9.1.0")
		assert.True(t, changed)
		assert.Equal(t, "9.0.0", old)
	})
}

func TestInvalidateHostVersion(t *testing.T) {
	c := newTestCache()
	c.UpdateHostVersion("host-1", "9.2.0")

	c.InvalidateHostVersion("host-1")

	_, changed := c.UpdateHostVersion("host-1", "9.9.9")
	assert.False(t, changed, "host should be treated as never-seen after invalidation")
}

func TestUpdateClusterHosts(t *testing.T) {
	t.Run("first sighting is baseline, not a change", func(t *testing.T) {
		c := newTestCache()
		added, removed := c.UpdateClusterHosts("cluster-1", hostSet("host-1", "host-2"))
		assert.Nil(t, added)
		assert.Nil(t, removed)
	})

	t.Run("identical host set reports no added or removed hosts", func(t *testing.T) {
		c := newTestCache()
		c.UpdateClusterHosts("cluster-1", hostSet("host-1", "host-2"))
		added, removed := c.UpdateClusterHosts("cluster-1", hostSet("host-1", "host-2"))
		assert.Empty(t, added)
		assert.Empty(t, removed)
	})

	t.Run("host added to cluster", func(t *testing.T) {
		c := newTestCache()
		c.UpdateClusterHosts("cluster-1", hostSet("host-1"))
		added, removed := c.UpdateClusterHosts("cluster-1", hostSet("host-1", "host-2"))
		assert.ElementsMatch(t, []string{"host-2"}, added)
		assert.Empty(t, removed)
	})

	t.Run("host removed from cluster", func(t *testing.T) {
		c := newTestCache()
		c.UpdateClusterHosts("cluster-1", hostSet("host-1", "host-2"))
		added, removed := c.UpdateClusterHosts("cluster-1", hostSet("host-1"))
		assert.Empty(t, added)
		assert.ElementsMatch(t, []string{"host-2"}, removed)
	})

	t.Run("host swap reports both added and removed", func(t *testing.T) {
		c := newTestCache()
		c.UpdateClusterHosts("cluster-1", hostSet("host-1", "host-2"))
		added, removed := c.UpdateClusterHosts("cluster-1", hostSet("host-1", "host-3"))
		assert.ElementsMatch(t, []string{"host-3"}, added)
		assert.ElementsMatch(t, []string{"host-2"}, removed)
	})

	t.Run("nil host set is an invalid update and leaves an unpopulated entry absent", func(t *testing.T) {
		c := newTestCache()
		added, removed := c.UpdateClusterHosts("cluster-1", nil)
		assert.Nil(t, added)
		assert.Nil(t, removed)
		_, ok := c.ClusterToHosts["cluster-1"]
		assert.False(t, ok, "a nil update must not fabricate a cache entry")
	})

	t.Run("nil host set is an invalid update and does not report every prior host as removed", func(t *testing.T) {
		c := newTestCache()
		c.UpdateClusterHosts("cluster-1", hostSet("host-1", "host-2"))
		added, removed := c.UpdateClusterHosts("cluster-1", nil)
		assert.Nil(t, added)
		assert.Nil(t, removed)
		assert.Equal(t, hostSet("host-1", "host-2"), c.ClusterToHosts["cluster-1"],
			"cached host list must be untouched by an invalid update")
	})
}

func TestInvalidateCluster(t *testing.T) {
	c := newTestCache()
	c.UpdateClusterHosts("cluster-1", hostSet("host-1", "host-2"))

	lastHosts := c.InvalidateCluster("cluster-1")

	assert.ElementsMatch(t, []string{"host-1", "host-2"}, lastHosts)

	added, _ := c.UpdateClusterHosts("cluster-1", hostSet("host-1"))
	assert.Nil(t, added, "cluster should be treated as never-seen after invalidation")
}

func TestInvalidateCluster_UnknownCluster(t *testing.T) {
	c := newTestCache()
	lastHosts := c.InvalidateCluster("cluster-never-seen")
	assert.Empty(t, lastHosts)
}

func TestHostSetsEqual(t *testing.T) {
	tests := []struct {
		name     string
		a, b     map[string]struct{}
		expected bool
	}{
		{"both empty", hostSet(), hostSet(), true},
		{"identical single", hostSet("host-1"), hostSet("host-1"), true},
		{"identical multiple", hostSet("host-1", "host-2"), hostSet("host-2", "host-1"), true},
		{"different sizes", hostSet("host-1"), hostSet("host-1", "host-2"), false},
		{"same size different members", hostSet("host-1"), hostSet("host-2"), false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.expected, hostSetsEqual(tt.a, tt.b))
		})
	}
}
