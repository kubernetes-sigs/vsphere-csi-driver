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
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/vmware/govmomi"
	"github.com/vmware/govmomi/simulator"
)

// buildTestVC creates a minimal VirtualCenter backed by a running vcsim server.
// The returned stop function must be deferred by the caller.
func buildTestVC(t *testing.T) (
	ctx context.Context,
	vc *VirtualCenter,
	model *simulator.Model,
	stop func(),
) {
	t.Helper()
	ctx = context.Background()
	model = simulator.VPX()
	model.Datacenter = 1
	if err := model.Create(); err != nil {
		t.Fatalf("failed to create vcsim model: %v", err)
	}
	s := model.Service.NewServer()
	client, err := govmomi.NewClient(ctx, s.URL, true)
	if err != nil {
		s.Close()
		model.Remove()
		t.Fatalf("failed to create govmomi client: %v", err)
	}
	vc = &VirtualCenter{
		Config:      &VirtualCenterConfig{Host: s.URL.Hostname()},
		Client:      client,
		ClientMutex: &sync.Mutex{},
	}
	stop = func() {
		_ = client.Logout(ctx)
		s.Close()
		model.Remove()
	}
	return
}

// TestGetDatacentersWithPathFormat verifies that entries starting with "/" are resolved
// via the govmomi finder (inventory-path lookup) and the returned Datacenter has its
// InventoryPath populated.
func TestGetDatacentersWithPathFormat(t *testing.T) {
	ctx, vc, _, stop := buildTestVC(t)
	defer stop()

	dcs, err := vc.getDatacenters(ctx, []string{"/DC0"})
	require.NoError(t, err)
	require.Len(t, dcs, 1)
	assert.Equal(t, "/DC0", dcs[0].InventoryPath)
	assert.Equal(t, vc.Config.Host, dcs[0].VirtualCenterHost)
}

// TestGetDatacentersWithMoRefFormat verifies that entries NOT starting with "/" are treated
// as MoRef values and resolved via RetrieveOne. This lookup is rename-resilient because
// MoRef identifiers are stable across datacenter renames.
// It also confirms that resolving via MoRef returns the same datacenter as path-based lookup.
func TestGetDatacentersWithMoRefFormat(t *testing.T) {
	ctx, vc, model, stop := buildTestVC(t)
	defer stop()

	// Obtain the datacenter MoRef from the simulator.
	morefValue := model.Map().Any("Datacenter").Reference().Value // e.g. "datacenter-2"

	dcs, err := vc.getDatacenters(ctx, []string{morefValue})
	require.NoError(t, err)
	require.Len(t, dcs, 1)
	assert.Equal(t, morefValue, dcs[0].Reference().Value)
	assert.Equal(t, vc.Config.Host, dcs[0].VirtualCenterHost)
	// InventoryPath is set to the datacenter name (not a full path) so log messages remain useful.
	assert.NotEmpty(t, dcs[0].InventoryPath)
}

// TestGetDatacentersMoRefMatchesPath verifies that path-based and MoRef-based lookups
// resolve to the same underlying datacenter object (same Reference).
func TestGetDatacentersMoRefMatchesPath(t *testing.T) {
	ctx, vc, model, stop := buildTestVC(t)
	defer stop()

	morefValue := model.Map().Any("Datacenter").Reference().Value

	byPath, err := vc.getDatacenters(ctx, []string{"/DC0"})
	require.NoError(t, err)
	require.Len(t, byPath, 1)

	byMoRef, err := vc.getDatacenters(ctx, []string{morefValue})
	require.NoError(t, err)
	require.Len(t, byMoRef, 1)

	assert.Equal(t, byPath[0].Reference().Value, byMoRef[0].Reference().Value,
		"path and MoRef lookup should resolve to the same datacenter")
}

// TestGetDatacentersWithUnknownPathReturnsError verifies that a non-existent inventory
// path returns an error.
func TestGetDatacentersWithUnknownPathReturnsError(t *testing.T) {
	ctx, vc, _, stop := buildTestVC(t)
	defer stop()

	_, err := vc.getDatacenters(ctx, []string{"/NoSuchDC"})
	require.Error(t, err)
}

// TestGetDatacentersWithInvalidMoRefReturnsError verifies that an unknown MoRef value
// returns an error.
func TestGetDatacentersWithInvalidMoRefReturnsError(t *testing.T) {
	ctx, vc, _, stop := buildTestVC(t)
	defer stop()

	_, err := vc.getDatacenters(ctx, []string{"datacenter-99999"})
	require.Error(t, err)
}
