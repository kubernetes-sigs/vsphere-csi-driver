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
	"context"
	"sync"
	"testing"

	"github.com/vmware/govmomi"
	"github.com/vmware/govmomi/object"
	"github.com/vmware/govmomi/simulator"
	"github.com/vmware/govmomi/vim25/mo"
	vim25types "github.com/vmware/govmomi/vim25/types"

	cnsvsphere "sigs.k8s.io/vsphere-csi-driver/v3/pkg/common/cns-lib/vsphere"
)

// buildVirtualCenter constructs a minimal cnsvsphere.VirtualCenter backed by
// a running govmomi client so that fetchHosts can make real property-collector
// calls against the vcsim server.
func buildVirtualCenter(client *govmomi.Client) *cnsvsphere.VirtualCenter {
	return &cnsvsphere.VirtualCenter{
		Config:      &cnsvsphere.VirtualCenterConfig{Host: "127.0.0.1"},
		Client:      client,
		ClientMutex: &sync.Mutex{},
	}
}

// setupFetchHostsSim creates a vcsim environment with one cluster containing
// the specified number of standalone (non-cluster) hosts for ComputeResource
// tests, or uses the cluster for ClusterComputeResource tests.
func setupFetchHostsSim(t *testing.T, numHosts int) (
	ctx context.Context,
	client *govmomi.Client,
	model *simulator.Model,
	stop func(),
) {
	t.Helper()
	ctx = context.Background()
	model = simulator.VPX()
	model.Datacenter = 1
	model.Cluster = 1
	model.Host = numHosts
	model.Machine = 0

	if err := model.Create(); err != nil {
		t.Fatalf("failed to create vcsim model: %v", err)
	}
	s := model.Service.NewServer()

	var err error
	client, err = govmomi.NewClient(ctx, s.URL, true)
	if err != nil {
		s.Close()
		model.Remove()
		t.Fatalf("failed to create govmomi client: %v", err)
	}
	stop = func() {
		s.Close()
		model.Remove()
	}
	return ctx, client, model, stop
}

// --- ClusterComputeResource cases (covered via fetchHosts indirectly) -------

// TestFetchHostsClusterExcludesMMHost verifies that when fetchHosts is called
// with a ClusterComputeResource entity, hosts in Maintenance Mode are excluded.
func TestFetchHostsClusterExcludesMMHost(t *testing.T) {
	ctx, client, model, stop := setupFetchHostsSim(t, 2)
	defer stop()

	allSimHosts := model.Map().All("HostSystem")
	if len(allSimHosts) < 2 {
		t.Skip("need at least 2 hosts in vcsim model")
	}
	// Put one host into Maintenance Mode.
	allSimHosts[0].(*simulator.HostSystem).Runtime.InMaintenanceMode = true

	vc := buildVirtualCenter(client)
	simCluster := model.Map().Any("ClusterComputeResource").(*simulator.ClusterComputeResource)
	entity := simCluster // implements mo.Reference

	hosts, err := fetchHosts(ctx, entity, vc)
	if err != nil {
		t.Fatalf("fetchHosts returned unexpected error: %v", err)
	}

	mmHostRef := allSimHosts[0].(*simulator.HostSystem).Reference().Value
	for _, h := range hosts {
		if h.Reference().Value == mmHostRef {
			t.Errorf("host %q is in Maintenance Mode but was returned by fetchHosts", mmHostRef)
		}
	}
	t.Logf("fetchHosts (ClusterComputeResource) returned %d active host(s), MM host excluded", len(hosts))
}

// --- ComputeResource cases ---------------------------------------------------

// getStandaloneComputeResourceRef finds a ComputeResource that is NOT a
// ClusterComputeResource in the vcsim model. vcsim represents a standalone
// host as a ComputeResource with a single host inside.
func getStandaloneComputeResourceRef(
	t *testing.T,
	client *govmomi.Client,
	model *simulator.Model,
) (mo.Reference, *simulator.HostSystem) {
	t.Helper()
	for _, obj := range model.Map().All("ComputeResource") {
		cr := object.NewComputeResource(client.Client, obj.Reference())
		hostList, err := cr.Hosts(context.Background())
		if err != nil || len(hostList) != 1 {
			continue
		}
		hostObj := model.Map().Get(hostList[0].Reference())
		if hostObj == nil {
			continue
		}
		simHost, ok := hostObj.(*simulator.HostSystem)
		if !ok {
			continue
		}
		return obj.(mo.Reference), simHost
	}
	return nil, nil
}

// TestFetchHostsComputeResourceActiveHost verifies that fetchHosts returns the
// standalone host when it is NOT in Maintenance Mode.
func TestFetchHostsComputeResourceActiveHost(t *testing.T) {
	// Use standalone hosts (Host outside of a cluster).
	ctx := context.Background()
	model := simulator.VPX()
	model.Datacenter = 1
	model.Cluster = 0
	model.Host = 1
	model.Machine = 0
	if err := model.Create(); err != nil {
		t.Fatalf("failed to create vcsim model: %v", err)
	}
	s := model.Service.NewServer()
	defer s.Close()
	defer model.Remove()

	client, err := govmomi.NewClient(ctx, s.URL, true)
	if err != nil {
		t.Fatalf("failed to create govmomi client: %v", err)
	}

	crRef, simHost := getStandaloneComputeResourceRef(t, client, model)
	if crRef == nil {
		t.Skip("no standalone ComputeResource found in vcsim model")
	}
	simHost.Runtime.InMaintenanceMode = false

	vc := buildVirtualCenter(client)
	hosts, err := fetchHosts(ctx, crRef, vc)
	if err != nil {
		t.Fatalf("fetchHosts returned unexpected error: %v", err)
	}
	if len(hosts) != 1 {
		t.Fatalf("expected 1 active host, got %d", len(hosts))
	}
	t.Log("fetchHosts (ComputeResource) correctly returned the active host")
}

// TestFetchHostsComputeResourceMMHost verifies that fetchHosts skips the
// standalone host when it is in Maintenance Mode.
func TestFetchHostsComputeResourceMMHost(t *testing.T) {
	ctx := context.Background()
	model := simulator.VPX()
	model.Datacenter = 1
	model.Cluster = 0
	model.Host = 1
	model.Machine = 0
	if err := model.Create(); err != nil {
		t.Fatalf("failed to create vcsim model: %v", err)
	}
	s := model.Service.NewServer()
	defer s.Close()
	defer model.Remove()

	client, err := govmomi.NewClient(ctx, s.URL, true)
	if err != nil {
		t.Fatalf("failed to create govmomi client: %v", err)
	}

	crRef, simHost := getStandaloneComputeResourceRef(t, client, model)
	if crRef == nil {
		t.Skip("no standalone ComputeResource found in vcsim model")
	}
	simHost.Runtime.InMaintenanceMode = true

	vc := buildVirtualCenter(client)
	hosts, err := fetchHosts(ctx, crRef, vc)
	if err != nil {
		t.Fatalf("fetchHosts returned unexpected error: %v", err)
	}
	if len(hosts) != 0 {
		t.Fatalf("expected 0 hosts when ComputeResource host is in Maintenance Mode, got %d", len(hosts))
	}
	t.Log("fetchHosts (ComputeResource) correctly excluded the Maintenance Mode host")
}

// --- HostSystem cases --------------------------------------------------------

// TestFetchHostsHostSystemActiveHost verifies that fetchHosts returns the host
// when a HostSystem entity is NOT in Maintenance Mode.
func TestFetchHostsHostSystemActiveHost(t *testing.T) {
	ctx := context.Background()
	model := simulator.VPX()
	model.Datacenter = 1
	model.Cluster = 0
	model.Host = 1
	model.Machine = 0
	if err := model.Create(); err != nil {
		t.Fatalf("failed to create vcsim model: %v", err)
	}
	s := model.Service.NewServer()
	defer s.Close()
	defer model.Remove()

	client, err := govmomi.NewClient(ctx, s.URL, true)
	if err != nil {
		t.Fatalf("failed to create govmomi client: %v", err)
	}

	allSimHosts := model.Map().All("HostSystem")
	if len(allSimHosts) == 0 {
		t.Skip("no HostSystem in vcsim model")
	}
	simHost := allSimHosts[0].(*simulator.HostSystem)
	simHost.Runtime.InMaintenanceMode = false

	vc := buildVirtualCenter(client)
	// Pass the HostSystem directly as the entity.
	hosts, err := fetchHosts(ctx, simHost, vc)
	if err != nil {
		t.Fatalf("fetchHosts returned unexpected error: %v", err)
	}
	if len(hosts) != 1 {
		t.Fatalf("expected 1 active host, got %d", len(hosts))
	}
	t.Log("fetchHosts (HostSystem) correctly returned the active host")
}

// TestFetchHostsHostSystemMMHost verifies that fetchHosts returns an empty
// list when the HostSystem entity is in Maintenance Mode.
func TestFetchHostsHostSystemMMHost(t *testing.T) {
	ctx := context.Background()
	model := simulator.VPX()
	model.Datacenter = 1
	model.Cluster = 0
	model.Host = 1
	model.Machine = 0
	if err := model.Create(); err != nil {
		t.Fatalf("failed to create vcsim model: %v", err)
	}
	s := model.Service.NewServer()
	defer s.Close()
	defer model.Remove()

	client, err := govmomi.NewClient(ctx, s.URL, true)
	if err != nil {
		t.Fatalf("failed to create govmomi client: %v", err)
	}

	allSimHosts := model.Map().All("HostSystem")
	if len(allSimHosts) == 0 {
		t.Skip("no HostSystem in vcsim model")
	}
	simHost := allSimHosts[0].(*simulator.HostSystem)
	simHost.Runtime.InMaintenanceMode = true

	vc := buildVirtualCenter(client)
	hosts, err := fetchHosts(ctx, simHost, vc)
	if err != nil {
		t.Fatalf("fetchHosts returned unexpected error: %v", err)
	}
	if len(hosts) != 0 {
		t.Fatalf("expected 0 hosts when HostSystem is in Maintenance Mode, got %d", len(hosts))
	}
	t.Log("fetchHosts (HostSystem) correctly excluded the Maintenance Mode host")
}

// --- filterMaintenanceModeHosts unit tests -----------------------------------

// buildHostList constructs a []*cnsvsphere.HostSystem from all HostSystem
// objects in the given simulator model.
func buildHostList(client *govmomi.Client, model *simulator.Model) []*cnsvsphere.HostSystem {
	var hostList []*cnsvsphere.HostSystem
	for _, obj := range model.Map().All("HostSystem") {
		simHost := obj.(*simulator.HostSystem)
		hostList = append(hostList, &cnsvsphere.HostSystem{
			HostSystem: object.NewHostSystem(client.Client, simHost.Reference()),
		})
	}
	return hostList
}

// TestFilterMMHostsEmptyList verifies that filterMaintenanceModeHosts returns
// nil without error when given an empty host list.
func TestFilterMMHostsEmptyList(t *testing.T) {
	ctx := context.Background()
	result, err := filterMaintenanceModeHosts(ctx, nil, nil, vim25types.ManagedObjectReference{})
	if err != nil {
		t.Fatalf("expected no error for empty host list, got: %v", err)
	}
	if result != nil {
		t.Fatalf("expected nil result for empty host list, got %d hosts", len(result))
	}
	t.Log("filterMaintenanceModeHosts correctly returned nil for empty input")
}

// TestFilterMMHostsAllActive verifies that all hosts are returned when none
// are in Maintenance Mode.
func TestFilterMMHostsAllActive(t *testing.T) {
	ctx, client, model, stop := setupFetchHostsSim(t, 3)
	defer stop()

	allSimHosts := model.Map().All("HostSystem")
	if len(allSimHosts) == 0 {
		t.Skip("no HostSystem objects in vcsim model")
	}
	for _, obj := range allSimHosts {
		obj.(*simulator.HostSystem).Runtime.InMaintenanceMode = false
	}

	vc := buildVirtualCenter(client)
	hostList := buildHostList(client, model)
	entityRef := model.Map().Any("ClusterComputeResource").(*simulator.ClusterComputeResource).Reference()

	result, err := filterMaintenanceModeHosts(ctx, hostList, vc, entityRef)
	if err != nil {
		t.Fatalf("filterMaintenanceModeHosts returned unexpected error: %v", err)
	}
	if len(result) != len(hostList) {
		t.Fatalf("expected all %d hosts returned, got %d", len(hostList), len(result))
	}
	t.Logf("filterMaintenanceModeHosts correctly returned all %d active hosts", len(result))
}

// TestFilterMMHostsSomeInMM verifies that only active hosts are returned when
// some hosts are in Maintenance Mode.
func TestFilterMMHostsSomeInMM(t *testing.T) {
	ctx, client, model, stop := setupFetchHostsSim(t, 3)
	defer stop()

	allSimHosts := model.Map().All("HostSystem")
	if len(allSimHosts) < 2 {
		t.Skip("need at least 2 hosts in vcsim model")
	}
	// Put the first host into Maintenance Mode.
	allSimHosts[0].(*simulator.HostSystem).Runtime.InMaintenanceMode = true
	mmHostRef := allSimHosts[0].(*simulator.HostSystem).Reference().Value

	vc := buildVirtualCenter(client)
	hostList := buildHostList(client, model)
	entityRef := model.Map().Any("ClusterComputeResource").(*simulator.ClusterComputeResource).Reference()

	result, err := filterMaintenanceModeHosts(ctx, hostList, vc, entityRef)
	if err != nil {
		t.Fatalf("filterMaintenanceModeHosts returned unexpected error: %v", err)
	}
	expectedCount := len(hostList) - 1
	if len(result) != expectedCount {
		t.Fatalf("expected %d active hosts, got %d", expectedCount, len(result))
	}
	for _, h := range result {
		if h.Reference().Value == mmHostRef {
			t.Errorf("host %q is in Maintenance Mode but was included in result", mmHostRef)
		}
	}
	t.Logf("filterMaintenanceModeHosts correctly returned %d active hosts, excluded MM host", len(result))
}

// TestFilterMMHostsAllInMM verifies that an empty list is returned when all
// hosts are in Maintenance Mode.
func TestFilterMMHostsAllInMM(t *testing.T) {
	ctx, client, model, stop := setupFetchHostsSim(t, 2)
	defer stop()

	allSimHosts := model.Map().All("HostSystem")
	if len(allSimHosts) == 0 {
		t.Skip("no HostSystem objects in vcsim model")
	}
	for _, obj := range allSimHosts {
		obj.(*simulator.HostSystem).Runtime.InMaintenanceMode = true
	}

	vc := buildVirtualCenter(client)
	hostList := buildHostList(client, model)
	entityRef := model.Map().Any("ClusterComputeResource").(*simulator.ClusterComputeResource).Reference()

	result, err := filterMaintenanceModeHosts(ctx, hostList, vc, entityRef)
	if err != nil {
		t.Fatalf("filterMaintenanceModeHosts returned unexpected error: %v", err)
	}
	if len(result) != 0 {
		t.Fatalf("expected 0 hosts when all are in Maintenance Mode, got %d", len(result))
	}
	t.Log("filterMaintenanceModeHosts correctly returned empty list when all hosts are in Maintenance Mode")
}
