/*
Copyright 2024 The Kubernetes Authors.

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
	"crypto/tls"
	"fmt"
	"sync"
	"testing"

	"github.com/vmware/govmomi"
	"github.com/vmware/govmomi/object"
	"github.com/vmware/govmomi/simulator"
	"github.com/vmware/govmomi/vapi/rest"
	_ "github.com/vmware/govmomi/vapi/simulator"
	"github.com/vmware/govmomi/vapi/tags"
)

var (
	onceForVMTest sync.Once
	ctx           context.Context
	tagManager    *tags.Manager
	vm            *VirtualMachine
)

func getClientFromVCSimAndAttachTags(t *testing.T) {
	var client *govmomi.Client

	onceForVMTest.Do(func() {
		ctx = context.Background()
		tlsConfig := new(tls.Config)

		// Create a new vcsim instance
		model := simulator.VPX()
		// Update model values as per requirement
		model.Datacenter = 1
		model.Cluster = 1
		model.Host = 0

		defer model.Remove()
		err := model.Create()
		if err != nil {
			t.Fatalf("Failed to create simulator model, err: %v\n", err)
		}

		model.Service.TLS = tlsConfig
		// Requird for tag manager to work
		model.Service.RegisterEndpoints = true

		// Create a vcsim server
		s := model.Service.NewServer()

		// Create a new client
		client, err = govmomi.NewClient(ctx, s.URL, true)
		if err != nil {
			t.Fatalf("Failed to create new govmomi client, err: %v\n", err)
		}

		userinfo := simulator.DefaultLogin
		// Create a new rest client and a tag manager
		restClient := rest.NewClient(client.Client)
		err = restClient.Login(ctx, userinfo)
		if err != nil {
			t.Fatalf("failed to login to the rest client, err: %v", err)
		}

		tagManager = tags.NewManager(restClient)
		if tagManager == nil {
			t.Fatalf("failed to create a new tagManager")
		}
		fmt.Printf("New tag manager created with useragent %s\n", tagManager.UserAgent)

		// Create a new region category
		regionCategoryId, err := CreateNewCategory(ctx, regionCategoryName, "SINGLE", tagManager)
		if err != nil {
			t.Fatalf("Error creating a new category %s, err: %v\n", regionCategoryName, err)
		}
		// Create a new tag within the region category
		description := "region-tag"
		regionTagId, err := CreateNewTag(ctx, regionCategoryId, regionTagName, description, tagManager)
		if err != nil {
			t.Fatalf("Error creating a new tag %s, err: %v\n", regionTagName, err)
		}

		// Add region tag to the datacenter
		dcMoRef := model.Map().Any("Datacenter").(*simulator.Datacenter).Reference()
		err = AttachTag(ctx, regionTagId, dcMoRef, tagManager)
		if err != nil {
			t.Fatalf("Error adding region tag to datacenter, err: %v", err)
		}

		// Create a new zone category
		zoneCategoryId, err := CreateNewCategory(ctx, zoneCategoryName, "SINGLE", tagManager)
		if err != nil {
			t.Fatalf("Error creating a new category %s, err: %v\n", zoneCategoryName, err)
		}
		// Create a new tag within the zone category
		description = "zone-tag"
		zoneTagId, err := CreateNewTag(ctx, zoneCategoryId, zoneTagName, description, tagManager)
		if err != nil {
			t.Fatalf("Error creating a new tag %s, err: %v\n", zoneTagName, err)
		}

		// Add zone tag to the cluster
		clusterMoRef := model.Map().Any("ClusterComputeResource").(*simulator.ClusterComputeResource).Reference()
		err = AttachTag(ctx, zoneTagId, clusterMoRef, tagManager)
		if err != nil {
			t.Fatalf("Error adding region tag to datacenter, err: %v", err)
		}

		// Get any nodeVM instance which belongs to above datacenter and cluster and check GetZoneRegion of that VM
		dcobj := object.NewDatacenter(client.Client, dcMoRef)
		dc := &Datacenter{
			Datacenter: dcobj,
		}
		obj := model.Map().Any("VirtualMachine").(*simulator.VirtualMachine)
		vm = &VirtualMachine{
			Datacenter:     dc,
			VirtualMachine: object.NewVirtualMachine(client.Client, obj.Reference()),
		}
	})
}

// TestGetZoneRegion creates new category and tag using tag manager for region
// and zone respectively. It attaches region tag to datacenter and zone tag to cluster moref.
// After that it calls existing function GetZoneRegion for one of the VMs belonging to this datacenter
// and cluster and checks if we can successfully fetch tags from the ancestors of this VM that we added
// using tagManager.
func TestGetZoneRegion(t *testing.T) {
	getClientFromVCSimAndAttachTags(t)

	// GetZoneRegion function returns zone and region tag values of the nodeVM for the
	// given zone and region category names.
	zone, region, err := vm.GetZoneRegion(ctx, zoneCategoryName, regionCategoryName, tagManager)
	if err != nil {
		t.Fatalf("Error occurred while getting zone and region of VM, err: %v\n", err)
	}
	if zone == zoneTagName && region == regionTagName {
		fmt.Printf("Expected zone and region tag values received for the VM, region: %s and zone: %s\n",
			region, zone)
	} else {
		t.Fatalf("Didn't get expected values for zone and region for VM. Got region: %s and zone: %s. "+
			"Expected values are - region: %s and zone: %s\n",
			region, zone, regionTagName, zoneTagName)
	}
}

// TestIsInZoneRegion creates new category and tag using tag manager for region
// and zone respectively. It attaches region tag to datacenter and zone tag to cluster moref.
// After that it calls existing function IsInZoneRegion for one of the VMs belonging to this datacenter
// and cluster and checks if vm belongs to the specified zone and region.
func TestIsInZoneRegion(t *testing.T) {
	getClientFromVCSimAndAttachTags(t)

	// IsInZoneRegion function checks if VM belongs to specified zone and region tag values.
	// This function returns true if yes, false otherwise.
	isInZoneRegion, err := vm.IsInZoneRegion(ctx, zoneCategoryName, regionCategoryName, zoneTagName,
		regionTagName, tagManager)
	if err != nil {
		t.Fatalf("Error occurred while checking if VM is in specified zone and region, err: %v\n", err)
	}
	if isInZoneRegion == true {
		fmt.Printf("VM belongs to specified zone and region as expected.\n")
	} else {
		t.Fatalf("VM should belong to specified zone and region")
	}
}
