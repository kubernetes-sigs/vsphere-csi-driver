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

package k8sorchestrator

import (
	"context"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"
	cnstypes "github.com/vmware/govmomi/cns/types"
	"k8s.io/client-go/kubernetes/fake"
	"sigs.k8s.io/vsphere-csi-driver/v3/pkg/internalapis/podvmv2"
)

// gatedOrchestrator returns a Supervisor orchestrator whose informer map holds
// dedicated ESXi Nodes and whose host inventory, if selected at startup,
// records every NodeMapping read.
func gatedOrchestrator(t *testing.T) (*K8sOrchestrator, *fake.Clientset, *int) {
	t.Helper()
	client := fake.NewSimpleClientset(inventoryNode("compute-a", "", true))
	mappingReads := 0
	c := &K8sOrchestrator{
		clusterFlavor: cnstypes.CnsClusterFlavorWorkload,
		k8sClient:     client,
		nodeIDToNameMap: &nodeIDToNameMap{
			RWMutex:  &sync.RWMutex{},
			items:    map[string]string{"host-1": "esx-1", "host-2": "esx-2"},
			nameToID: map[string]string{"esx-1": "host-1", "esx-2": "host-2"},
		},
	}
	if inventory := c.newHostInventory(context.Background()); inventory != nil {
		inventory.nodeMappings = func(context.Context) ([]podvmv2.NodeMapping, error) {
			mappingReads++
			return nil, nil
		}
		c.hostInventory = inventory
	}
	client.ClearActions()
	return c, client, &mappingReads
}

func TestNewHostInventoryFollowsCapabilityAtStartup(t *testing.T) {
	ctx := context.Background()
	for _, enabled := range []bool{false, true} {
		setPodVMV2Capability(t, enabled)
		c := &K8sOrchestrator{clusterFlavor: cnstypes.CnsClusterFlavorWorkload, k8sClient: fake.NewSimpleClientset()}
		inventory := c.newHostInventory(ctx)
		require.Equal(t, enabled, inventory != nil, "capability=%t", enabled)
		if inventory != nil {
			require.NotNil(t, inventory.client)
			require.NotNil(t, inventory.nodeMappings)
		}
	}
}

// Model "disabled" as an absent capability: an explicit false is in the
// late-enablement set and would re-read the capabilities CR from a live
// API server. Enabling the capability later restarts the container through
// HandleLateEnablementOfCapability, which re-runs this startup selection.
func TestHostOwnershipUsesLegacyMapWithoutPodVMV2(t *testing.T) {
	ctx := context.Background()
	setPodVMV2Capability(t, false)
	c, client, mappingReads := gatedOrchestrator(t)
	require.False(t, c.usesHostInventory())
	owners, err := c.GetNodeIDtoNameMap(ctx)
	require.NoError(t, err, "the legacy path never fails")
	require.Equal(t, map[string]string{"host-1": "esx-1", "host-2": "esx-2"}, owners)
	require.Equal(t, map[string]string{"esx-1": "host-1", "esx-2": "host-2"}, c.GetNodeNameToHostMoIDMap(ctx))
	require.Zero(t, *mappingReads, "NodeMapping must not be read while PodVM v2 is disabled")
	require.Empty(t, client.Actions(), "Nodes must not be listed while PodVM v2 is disabled")

	// The legacy map is returned by copy.
	copied, err := c.GetNodeIDtoNameMap(ctx)
	require.NoError(t, err)
	copied["host-1"] = "mutated"
	owners, err = c.GetNodeIDtoNameMap(ctx)
	require.NoError(t, err)
	require.Equal(t, "esx-1", owners["host-1"])
}

func TestHostOwnershipUsesInventoryWithPodVMV2(t *testing.T) {
	ctx := context.Background()
	setPodVMV2Capability(t, true)
	c, client, mappingReads := gatedOrchestrator(t)
	require.True(t, c.usesHostInventory())
	// Live Nodes hold only a VirtualNode with no NodeMapping, so no host has an
	// owner; the informer map's dedicated entries are not consulted.
	owners, err := c.GetNodeIDtoNameMap(ctx)
	require.NoError(t, err)
	require.Empty(t, owners)
	// The node name -> single host accessor keeps reading the informer map whatever the
	// capability state: only a dedicated ESXi Node carries the host annotation it is built
	// from, and its host-local callers cannot place a disk on a cluster-backed Node.
	require.Equal(t, map[string]string{"esx-1": "host-1", "esx-2": "host-2"}, c.GetNodeNameToHostMoIDMap(ctx))
	require.Equal(t, 1, *mappingReads)
	require.NotEmpty(t, client.Actions(), "inventory reads live Nodes")
}

func TestHostOwnershipWithoutInformerMap(t *testing.T) {
	setPodVMV2Capability(t, false)
	c := &K8sOrchestrator{clusterFlavor: cnstypes.CnsClusterFlavorWorkload}
	owners, err := c.GetNodeIDtoNameMap(context.Background())
	require.NoError(t, err)
	require.Empty(t, owners)
	require.Empty(t, c.GetNodeNameToHostMoIDMap(context.Background()))
}
