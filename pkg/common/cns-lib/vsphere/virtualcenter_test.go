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

package vsphere

import (
	"context"
	"strings"
	"sync"
	"testing"

	"github.com/vmware/govmomi"
	"github.com/vmware/govmomi/find"
	"github.com/vmware/govmomi/simulator"
)

func TestGetDatacentersWithMoRef(t *testing.T) {
	ctx := context.Background()

	// 1. Setup vcsim
	model := simulator.VPX()
	model.Datacenter = 1
	err := model.Create()
	if err != nil {
		t.Fatal(err)
	}
	defer model.Remove()

	s := model.Service.NewServer()
	defer s.Close()

	client, err := govmomi.NewClient(ctx, s.URL, true)
	if err != nil {
		t.Fatal(err)
	}

	// 2. Create VirtualCenter instance
	vc := &VirtualCenter{
		Config: &VirtualCenterConfig{
			Host: s.URL.Host,
		},
		Client:      client,
		ClientMutex: &sync.Mutex{},
	}

	// Get the initial datacenter to find its MoRef and Path
	finder := find.NewFinder(client.Client, false)
	dcs, err := finder.DatacenterList(ctx, "*")
	if err != nil || len(dcs) == 0 {
		t.Fatalf("failed to list datacenters: %v", err)
	}
	dcObj := dcs[0]
	dcMoRef := dcObj.Reference().String() // e.g. "Datacenter:datacenter-2"
	dcMoID := dcObj.Reference().Value     // e.g. "datacenter-2"
	dcPath := dcObj.InventoryPath         // e.g. "/DC0"

	t.Logf("Initial Datacenter: Path=%s, MoRef=%s, MoID=%s", dcPath, dcMoRef, dcMoID)

	// 3. Test with Path (should succeed initially)
	vc.Config.DatacenterPaths = []string{dcPath}
	resolvedDCs, err := vc.getDatacenters(ctx, vc.Config.DatacenterPaths)
	if err != nil {
		t.Errorf("getDatacenters with path failed: %v", err)
	}
	if len(resolvedDCs) != 1 || resolvedDCs[0].InventoryPath != dcPath {
		t.Errorf("Unexpected resolved datacenter with path: %+v", resolvedDCs)
	}

	// 4. Rename the Datacenter
	newName := "RenamedDC"
	task, err := dcObj.Rename(ctx, newName)
	if err != nil {
		t.Fatalf("failed to rename datacenter: %v", err)
	}
	err = task.Wait(ctx)
	if err != nil {
		t.Fatalf("failed to wait for rename task: %v", err)
	}
	newPath := strings.Replace(dcPath, "DC0", newName, 1)
	t.Logf("Renamed Datacenter to %s, New Path=%s", newName, newPath)

	// 5. Test with OLD Path (should fail now)
	vc.Config.DatacenterPaths = []string{dcPath}
	_, err = vc.getDatacenters(ctx, vc.Config.DatacenterPaths)
	if err == nil {
		t.Error("getDatacenters with old path should have failed but succeeded")
	} else {
		t.Logf("Expected failure with old path: %v", err)
	}

	// 6. Test with MoRef (should succeed despite rename)
	vc.Config.DatacenterPaths = []string{dcMoRef}
	resolvedDCs, err = vc.getDatacenters(ctx, vc.Config.DatacenterPaths)
	if err != nil {
		t.Errorf("getDatacenters with MoRef failed after rename: %v", err)
	} else if len(resolvedDCs) != 1 {
		t.Errorf("Expected 1 datacenter with MoRef, got %d", len(resolvedDCs))
	} else {
		// Verify the name is correct
		name, err := resolvedDCs[0].ObjectName(ctx)
		if err != nil {
			t.Errorf("failed to get name for resolved DC: %v", err)
		} else {
			t.Logf("Resolved DC name via MoRef: %s", name)
			if name != newName {
				t.Errorf("Expected name to be %s, got %s", newName, name)
			}
		}
	}

	// 7. Test with raw MoID (Supervisor format)
	vc.Config.DatacenterPaths = []string{dcMoID}
	resolvedDCs, err = vc.getDatacenters(ctx, vc.Config.DatacenterPaths)
	if err != nil {
		t.Errorf("getDatacenters with raw MoID failed after rename: %v", err)
	} else if len(resolvedDCs) != 1 {
		t.Errorf("Expected 1 datacenter with raw MoID, got %d", len(resolvedDCs))
	} else {
		name, err := resolvedDCs[0].ObjectName(ctx)
		if err != nil {
			t.Errorf("failed to get name for resolved DC: %v", err)
		} else {
			t.Logf("Resolved DC name via raw MoID: %s", name)
			if name != newName {
				t.Errorf("Expected name to be %s, got %s", newName, name)
			}
		}
	}

	// 8. Test with a path that starts with "datacenter-" but isn't a MoID
	fakePath := "datacenter-not-a-real-id/something"
	vc.Config.DatacenterPaths = []string{fakePath}
	_, err = vc.getDatacenters(ctx, vc.Config.DatacenterPaths)
	if err == nil {
		t.Error("getDatacenters with fake path starting with 'datacenter-' should have failed")
	} else {
		t.Logf("Expected failure with fake path: %v", err)
		if !strings.Contains(err.Error(), "not found") {
			t.Errorf("Expected 'not found' error for fake path, got: %v", err)
		}
	}
}
