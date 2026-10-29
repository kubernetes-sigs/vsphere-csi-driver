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

package storagepool

import (
	"context"
	"fmt"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"sigs.k8s.io/vsphere-csi-driver/v3/pkg/apis/storagepool/cns/v1alpha1"
	csitypes "sigs.k8s.io/vsphere-csi-driver/v3/pkg/csi/types"
)

func newTestStoragePool(name, driver string, params, diskDecommStatus map[string]string) v1alpha1.StoragePool {
	return v1alpha1.StoragePool{
		ObjectMeta: metav1.ObjectMeta{Name: name},
		Spec: v1alpha1.StoragePoolSpec{
			Driver:     driver,
			Parameters: params,
		},
		Status: v1alpha1.StoragePoolStatus{
			DiskDecomm: diskDecommStatus,
		},
	}
}

func TestShouldEnterDiskDecommission(t *testing.T) {
	ctx := context.Background()
	w := &DiskDecommController{diskDecommMode: make(map[string]string)}

	// Different driver: ignored and not recorded.
	sp := newTestStoragePool("sp1", "other.driver", map[string]string{drainModeField: ensureAccessibilityMM}, nil)
	ok, mode := w.shouldEnterDiskDecommission(ctx, sp)
	assert.False(t, ok)
	assert.Empty(t, mode)
	assert.NotContains(t, w.diskDecommMode, "sp1")

	// No decomm mode: does not enter.
	sp = newTestStoragePool("sp1", csitypes.Name, nil, nil)
	ok, mode = w.shouldEnterDiskDecommission(ctx, sp)
	assert.False(t, ok)
	assert.Empty(t, mode)
	assert.NotContains(t, w.diskDecommMode, "sp1")

	// New decomm mode: enters and records the mode.
	sp = newTestStoragePool("sp1", csitypes.Name, map[string]string{drainModeField: ensureAccessibilityMM}, nil)
	ok, mode = w.shouldEnterDiskDecommission(ctx, sp)
	assert.True(t, ok)
	assert.Equal(t, ensureAccessibilityMM, mode)
	assert.Equal(t, ensureAccessibilityMM, w.diskDecommMode["sp1"])

	// Same decomm mode seen again (e.g. replayed watch event): does not enter again.
	ok, mode = w.shouldEnterDiskDecommission(ctx, sp)
	assert.False(t, ok)
	assert.Empty(t, mode)

	// Decomm mode changes: enters with the new mode.
	sp = newTestStoragePool("sp1", csitypes.Name, map[string]string{drainModeField: fullDataEvacuationMM}, nil)
	ok, mode = w.shouldEnterDiskDecommission(ctx, sp)
	assert.True(t, ok)
	assert.Equal(t, fullDataEvacuationMM, mode)

	// Decomm mode removed: entry is cleared.
	sp = newTestStoragePool("sp1", csitypes.Name, nil, nil)
	ok, _ = w.shouldEnterDiskDecommission(ctx, sp)
	assert.False(t, ok)
	assert.NotContains(t, w.diskDecommMode, "sp1")

	// Decomm already finished: does not enter, but mode is recorded.
	sp = newTestStoragePool("sp2", csitypes.Name, map[string]string{drainModeField: noMigrationMM},
		map[string]string{drainStatusField: drainSuccessStatus})
	ok, _ = w.shouldEnterDiskDecommission(ctx, sp)
	assert.False(t, ok)
	assert.Equal(t, noMigrationMM, w.diskDecommMode["sp2"])

	// Unknown decomm mode: does not enter.
	sp = newTestStoragePool("sp3", csitypes.Name, map[string]string{drainModeField: "bogus"}, nil)
	ok, _ = w.shouldEnterDiskDecommission(ctx, sp)
	assert.False(t, ok)
}

// TestShouldEnterDiskDecommissionConcurrent simulates the init loop and the
// StoragePool watch goroutine processing the same StoragePools at the same
// time. Run with -race to detect unsynchronized access to diskDecommMode.
// Each StoragePool must enter disk decommission exactly once.
func TestShouldEnterDiskDecommissionConcurrent(t *testing.T) {
	ctx := context.Background()
	w := &DiskDecommController{diskDecommMode: make(map[string]string)}

	const numSPs = 50
	const numWorkers = 8
	spList := make([]v1alpha1.StoragePool, 0, numSPs)
	for i := 0; i < numSPs; i++ {
		spList = append(spList, newTestStoragePool(fmt.Sprintf("sp-%d", i), csitypes.Name,
			map[string]string{drainModeField: ensureAccessibilityMM}, nil))
	}

	var mu sync.Mutex
	entered := make(map[string]int)
	var wg sync.WaitGroup
	for i := 0; i < numWorkers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for _, sp := range spList {
				if ok, mode := w.shouldEnterDiskDecommission(ctx, sp); ok {
					assert.Equal(t, ensureAccessibilityMM, mode)
					mu.Lock()
					entered[sp.Name]++
					mu.Unlock()
				}
			}
		}()
	}
	wg.Wait()

	assert.Len(t, entered, numSPs)
	for name, count := range entered {
		assert.Equal(t, 1, count, "StoragePool %s entered disk decommission %d times", name, count)
	}
}
