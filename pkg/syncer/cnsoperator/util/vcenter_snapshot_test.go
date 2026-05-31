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

import (
	"testing"

	vimtypes "github.com/vmware/govmomi/vim25/types"
)

// ---------------------------------------------------------------------------
// collectSnapshotRefs tests
// ---------------------------------------------------------------------------

// TestCollectSnapshotRefs_Empty verifies that an empty tree produces no refs.
func TestCollectSnapshotRefs_Empty(t *testing.T) {
	var refs []vimtypes.ManagedObjectReference
	collectSnapshotRefs(nil, &refs)
	if len(refs) != 0 {
		t.Errorf("expected 0 refs, got %d", len(refs))
	}
}

// TestCollectSnapshotRefs_SingleSnapshot verifies that a single-node tree
// returns exactly one ref.
func TestCollectSnapshotRefs_SingleSnapshot(t *testing.T) {
	nodes := []vimtypes.VirtualMachineSnapshotTree{
		{Snapshot: vimtypes.ManagedObjectReference{Type: "VirtualMachineSnapshot", Value: "snap-1"}},
	}
	var refs []vimtypes.ManagedObjectReference
	collectSnapshotRefs(nodes, &refs)
	if len(refs) != 1 {
		t.Fatalf("expected 1 ref, got %d", len(refs))
	}
	if refs[0].Value != "snap-1" {
		t.Errorf("expected snap-1, got %q", refs[0].Value)
	}
}

// TestCollectSnapshotRefs_NestedTree verifies that all nodes in a deep tree are
// collected, including children and grandchildren.
func TestCollectSnapshotRefs_NestedTree(t *testing.T) {
	// Tree structure:
	//  snap-root
	//    snap-child1
	//      snap-grandchild
	//    snap-child2
	nodes := []vimtypes.VirtualMachineSnapshotTree{
		{
			Snapshot: vimtypes.ManagedObjectReference{Value: "snap-root"},
			ChildSnapshotList: []vimtypes.VirtualMachineSnapshotTree{
				{
					Snapshot: vimtypes.ManagedObjectReference{Value: "snap-child1"},
					ChildSnapshotList: []vimtypes.VirtualMachineSnapshotTree{
						{Snapshot: vimtypes.ManagedObjectReference{Value: "snap-grandchild"}},
					},
				},
				{Snapshot: vimtypes.ManagedObjectReference{Value: "snap-child2"}},
			},
		},
	}
	var refs []vimtypes.ManagedObjectReference
	collectSnapshotRefs(nodes, &refs)
	if len(refs) != 4 {
		t.Fatalf("expected 4 refs, got %d: %v", len(refs), refs)
	}

	// All four values must be present.
	expected := map[string]bool{
		"snap-root":       false,
		"snap-child1":     false,
		"snap-child2":     false,
		"snap-grandchild": false,
	}
	for _, ref := range refs {
		if _, ok := expected[ref.Value]; ok {
			expected[ref.Value] = true
		}
	}
	for k, found := range expected {
		if !found {
			t.Errorf("expected ref %q to be present but it was missing", k)
		}
	}
}

// TestCollectSnapshotRefs_MultipleRoots verifies that a forest (multiple root
// snapshots) is collected correctly.
func TestCollectSnapshotRefs_MultipleRoots(t *testing.T) {
	nodes := []vimtypes.VirtualMachineSnapshotTree{
		{Snapshot: vimtypes.ManagedObjectReference{Value: "snap-a"}},
		{Snapshot: vimtypes.ManagedObjectReference{Value: "snap-b"}},
		{Snapshot: vimtypes.ManagedObjectReference{Value: "snap-c"}},
	}
	var refs []vimtypes.ManagedObjectReference
	collectSnapshotRefs(nodes, &refs)
	if len(refs) != 3 {
		t.Fatalf("expected 3 refs, got %d", len(refs))
	}
}
