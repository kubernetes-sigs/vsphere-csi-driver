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

package nfsdriver

import (
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func nilGetter(string) (interface{}, error) { return nil, nil }

func newTestTimedCache(t *testing.T, ttl time.Duration) *TimedCache {
	t.Helper()
	c, err := NewTimedCache(ttl, nilGetter, false)
	require.NoError(t, err)
	return c.(*TimedCache)
}

func TestNewTimedCache(t *testing.T) {
	_, err := NewTimedCache(time.Minute, nil, false)
	require.Error(t, err)

	c, err := NewTimedCache(time.Minute, nilGetter, true)
	require.NoError(t, err)
	require.IsType(t, &DisabledCache{}, c)
}

func TestTimedCacheGetSet(t *testing.T) {
	c := newTestTimedCache(t, time.Minute)

	v, err := c.Get("k", CacheReadTypeDefault)
	require.NoError(t, err)
	require.Nil(t, v)

	c.Set("k", "v")
	v, err = c.Get("k", CacheReadTypeDefault)
	require.NoError(t, err)
	require.Equal(t, "v", v)
}

func TestTimedCacheGetRefreshesExpiredEntry(t *testing.T) {
	calls := 0
	c, err := NewTimedCache(20*time.Millisecond, func(string) (interface{}, error) {
		calls++
		return "fresh", nil
	}, false)
	require.NoError(t, err)

	v, err := c.Get("k", CacheReadTypeDefault)
	require.NoError(t, err)
	require.Equal(t, "fresh", v)
	_, _ = c.Get("k", CacheReadTypeDefault)
	require.Equal(t, 1, calls, "a fresh entry is served from the cache")

	time.Sleep(40 * time.Millisecond)
	_, _ = c.Get("k", CacheReadTypeDefault)
	require.Equal(t, 2, calls, "an expired entry is refreshed through the getter")
}

// Entries are never read again once a volume is deleted, so without eviction the store would
// hold one entry per volume ever deleted for the life of the process.
func TestTimedCacheEvictsExpiredEntries(t *testing.T) {
	ttl := 30 * time.Millisecond
	c := newTestTimedCache(t, ttl)

	for i := 0; i < 100; i++ {
		c.Set(fmt.Sprintf("deleted-%d", i), "")
	}
	require.Len(t, c.Store.ListKeys(), 100)

	time.Sleep(2 * ttl)
	c.Set("recent", "")

	require.Equal(t, []string{"recent"}, c.Store.ListKeys(),
		"expired entries must be dropped, and the fresh one kept")
	v, err := c.Get("recent", CacheReadTypeDefault)
	require.NoError(t, err)
	require.Equal(t, "", v)
}

func TestTimedCacheEvictionOnLookupOfMissingKey(t *testing.T) {
	// Looking up an unknown key adds a placeholder entry; those must not pile up either.
	ttl := 30 * time.Millisecond
	c := newTestTimedCache(t, ttl)

	for i := 0; i < 50; i++ {
		_, err := c.Get(fmt.Sprintf("never-set-%d", i), CacheReadTypeDefault)
		require.NoError(t, err)
	}
	time.Sleep(2 * ttl)
	_, err := c.Get("another", CacheReadTypeDefault)
	require.NoError(t, err)

	require.Equal(t, []string{"another"}, c.Store.ListKeys())
}

func TestTimedCacheSweepIsThrottled(t *testing.T) {
	c := newTestTimedCache(t, time.Hour)
	c.Set("a", "")
	c.lastSweep = time.Now().UTC().Add(-2 * time.Hour)
	c.Set("b", "")
	first := c.lastSweep

	// A second Set within the TTL must not scan again.
	c.Set("c", "")
	require.Equal(t, first, c.lastSweep)
	require.Len(t, c.Store.ListKeys(), 3, "nothing has expired yet")
}

func TestTimedCacheKeepsEntryThatIsBeingRead(t *testing.T) {
	ttl := 20 * time.Millisecond
	c := newTestTimedCache(t, ttl)
	c.Set("busy", "")
	time.Sleep(2 * ttl)

	obj, exists, err := c.Store.GetByKey("busy")
	require.NoError(t, err)
	require.True(t, exists)
	entry := obj.(*CacheEntry)

	entry.Lock.Lock() // simulates a Get that is in the middle of refreshing this entry
	c.Set("other", "")
	entry.Lock.Unlock()

	_, exists, _ = c.Store.GetByKey("busy")
	require.True(t, exists, "a locked entry must be skipped, not evicted from under its reader")

	time.Sleep(2 * ttl)
	c.Set("again", "")
	_, exists, _ = c.Store.GetByKey("busy")
	require.False(t, exists, "and it is evicted by a later sweep once it is free")
}

func TestTimedCacheConcurrentUse(t *testing.T) {
	c := newTestTimedCache(t, 5*time.Millisecond)
	var wg sync.WaitGroup
	for g := 0; g < 8; g++ {
		wg.Add(1)
		go func(g int) {
			defer wg.Done()
			for i := 0; i < 200; i++ {
				key := fmt.Sprintf("k-%d", (g*200+i)%40)
				c.Set(key, "")
				_, _ = c.Get(key, CacheReadTypeDefault)
				if i%50 == 0 {
					time.Sleep(6 * time.Millisecond)
				}
			}
		}(g)
	}
	wg.Wait()
}

func TestDisabledCache(t *testing.T) {
	calls := 0
	c, err := NewTimedCache(time.Minute, func(string) (interface{}, error) {
		calls++
		return "v", nil
	}, true)
	require.NoError(t, err)
	c.Set("k", "ignored")
	v, err := c.Get("k", CacheReadTypeDefault)
	require.NoError(t, err)
	require.Equal(t, "v", v)
	require.Equal(t, 1, calls)
}
