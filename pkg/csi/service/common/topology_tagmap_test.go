/*
Copyright 2025 The Kubernetes Authors.

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

package common

import (
	"fmt"
	"sync"
	"testing"

	"github.com/vmware/govmomi/vim25/mo"
	vim25types "github.com/vmware/govmomi/vim25/types"

	commontypes "sigs.k8s.io/vsphere-csi-driver/v3/pkg/common/types"
)

// resetTagVCEntityMoRefMap clears the package-level cache and restores it on cleanup.
func resetTagVCEntityMoRefMap(t *testing.T) {
	t.Helper()
	tagVCEntityMoRefMapLock.Lock()
	saved := tagVCEntityMoRefMap
	tagVCEntityMoRefMap = make(map[string]map[commontypes.FQDN][]mo.Reference)
	tagVCEntityMoRefMapLock.Unlock()
	t.Cleanup(func() {
		tagVCEntityMoRefMapLock.Lock()
		tagVCEntityMoRefMap = saved
		tagVCEntityMoRefMapLock.Unlock()
	})
}

func TestTagVCEntityMoRefMapSetAndGet(t *testing.T) {
	resetTagVCEntityMoRefMap(t)
	vc1, vc2 := commontypes.NewFQDN("vc1"), commontypes.NewFQDN("vc2")
	mor := func(v string) []mo.Reference {
		return []mo.Reference{vim25types.ManagedObjectReference{Type: "ClusterComputeResource", Value: v}}
	}

	if _, exists := getVCsForTag("zone-1"); exists {
		t.Fatal("expected tag to be absent")
	}
	if _, exists := areEntityMorefsPresentForTag("zone-1", vc1); exists {
		t.Fatal("expected tag to be absent")
	}

	setTagVCEntityMoRefs("zone-1", vc1, mor("domain-c1"))
	setTagVCEntityMoRefs("zone-1", vc2, mor("domain-c2"))
	// Overwriting an existing VC entry must keep the other VC's entry.
	setTagVCEntityMoRefs("zone-1", vc1, mor("domain-c3"))

	vcs, exists := getVCsForTag("zone-1")
	if !exists || len(vcs) != 2 {
		t.Fatalf("expected 2 VCs for tag, got %v (exists=%v)", vcs, exists)
	}
	morefs, exists := areEntityMorefsPresentForTag("zone-1", vc1)
	if !exists || len(morefs) != 1 || morefs[0].Reference().Value != "domain-c3" {
		t.Fatalf("unexpected morefs for vc1: %v (exists=%v)", morefs, exists)
	}
	morefs, exists = areEntityMorefsPresentForTag("zone-1", vc2)
	if !exists || len(morefs) != 1 || morefs[0].Reference().Value != "domain-c2" {
		t.Fatalf("unexpected morefs for vc2: %v (exists=%v)", morefs, exists)
	}
}

// TestTagVCEntityMoRefMapConcurrentAccess exercises concurrent writers and readers.
// It is meaningful when run with -race; without the lock the runtime also aborts with
// "concurrent map read and map write".
func TestTagVCEntityMoRefMapConcurrentAccess(t *testing.T) {
	resetTagVCEntityMoRefMap(t)
	vcs := []commontypes.FQDN{commontypes.NewFQDN("vc1"), commontypes.NewFQDN("vc2")}
	const iterations = 500

	var wg sync.WaitGroup
	for w := 0; w < 4; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			for i := 0; i < iterations; i++ {
				mors := []mo.Reference{vim25types.ManagedObjectReference{Type: "Datacenter", Value: fmt.Sprintf("dc-%d", i)}}
				setTagVCEntityMoRefs(fmt.Sprintf("tag-%d", i%5), vcs[w%len(vcs)], mors)
			}
		}(w)
	}
	for r := 0; r < 8; r++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < iterations; i++ {
				tag := fmt.Sprintf("tag-%d", i%5)
				if got, exists := getVCsForTag(tag); exists {
					for range got {
					}
				}
				areEntityMorefsPresentForTag(tag, vcs[i%len(vcs)])
			}
		}()
	}
	wg.Wait()

	for i := 0; i < 5; i++ {
		if _, exists := getVCsForTag(fmt.Sprintf("tag-%d", i)); !exists {
			t.Errorf("tag-%d missing after concurrent writes", i)
		}
	}
}
