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
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
	cnstypes "github.com/vmware/govmomi/cns/types"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes/fake"
	clienttesting "k8s.io/client-go/testing"
	"sigs.k8s.io/vsphere-csi-driver/v3/pkg/csi/service/common"
	"sigs.k8s.io/vsphere-csi-driver/v3/pkg/internalapis/podvmv2"
)

func inventoryNode(name, host string, virtual bool) *corev1.Node {
	node := &corev1.Node{ObjectMeta: metav1.ObjectMeta{
		Name:        name,
		UID:         types.UID(name + "-uid"),
		Labels:      map[string]string{corev1.LabelTopologyZone: "zone-1"},
		Annotations: map[string]string{},
	}}
	if host != "" {
		node.Annotations[common.HostMoidAnnotationKey] = host
	}
	if virtual {
		node.Annotations[podvmv2.NodeTypeAnnotation] = podvmv2.VirtualNodeType
	}
	return node
}

// nodeMapping builds a mapping the resolver should accept. The platform scopes
// hosts to those without a dedicated Node, so tests list only handed-off hosts.
func nodeMapping(node *corev1.Node, hosts ...string) podvmv2.NodeMapping {
	controller := true
	mapping := podvmv2.NodeMapping{
		ObjectMeta: metav1.ObjectMeta{
			Name:       node.Name,
			Generation: 1,
			OwnerReferences: []metav1.OwnerReference{{
				APIVersion: "v1", Kind: "Node", Name: node.Name, UID: node.UID, Controller: &controller,
			}},
		},
		Status: podvmv2.NodeMappingStatus{Conditions: []metav1.Condition{{
			Type: podvmv2.NodeMappingConditionReady, Status: metav1.ConditionTrue,
			ObservedGeneration: 1, Reason: "Synced", LastTransitionTime: metav1.Now(),
		}}},
	}
	for _, host := range hosts {
		mapping.Status.Hosts = append(mapping.Status.Hosts, podvmv2.NodeMappingHost{MOID: host, Name: host + ".example"})
	}
	return mapping
}

func TestVirtualNodeInventoryNodeMappingHandoff(t *testing.T) {
	ctx := context.Background()
	client := fake.NewSimpleClientset()
	virtual := inventoryNode("compute-a", "host-masquerade", true)
	var mappings []podvmv2.NodeMapping
	var listErr error
	h := &hostInventory{
		client: client,
		nodeMappings: func(context.Context) ([]podvmv2.NodeMapping, error) {
			return mappings, listErr
		},
	}
	setPodVMV2Capability(t, true)
	c := &K8sOrchestrator{clusterFlavor: cnstypes.CnsClusterFlavorWorkload, hostInventory: h}
	create := func(node *corev1.Node) {
		t.Helper()
		_, err := client.CoreV1().Nodes().Create(ctx, node, metav1.CreateOptions{})
		require.NoError(t, err)
	}
	check := func(want map[string]string) {
		t.Helper()
		owners, err := c.GetNodeIDtoNameMap(ctx)
		require.NoError(t, err)
		require.Equal(t, want, owners)
	}

	// Before any ESXi upgrade every host still has a dedicated Node.
	for i := 1; i <= 3; i++ {
		create(inventoryNode(fmt.Sprintf("esx-%d", i), fmt.Sprintf("host-%d", i), false))
	}
	legacy := map[string]string{"host-1": "esx-1", "host-2": "esx-2", "host-3": "esx-3"}
	check(legacy)

	// A VirtualNode appears owning nothing yet. An empty host list is the
	// normal pre-upgrade state, not missing data, and the Node's masqueraded
	// host annotation carries no identity.
	create(virtual)
	mappings = []podvmv2.NodeMapping{nodeMapping(virtual)}
	check(legacy)

	// The platform hands off host-1: the dedicated Node goes away and the
	// mapping starts publishing the host.
	require.NoError(t, client.CoreV1().Nodes().Delete(ctx, "esx-1", metav1.DeleteOptions{}))
	mappings = []podvmv2.NodeMapping{nodeMapping(virtual, "host-1")}
	check(map[string]string{"host-1": "compute-a", "host-2": "esx-2", "host-3": "esx-3"})

	// A mapping that claims a host still held by a dedicated Node means the two
	// producers disagree. Publish no owner for it rather than picking one.
	mappings = []podvmv2.NodeMapping{nodeMapping(virtual, "host-1", "host-2")}
	check(map[string]string{"host-1": "compute-a", "host-3": "esx-3"})

	for i := 2; i <= 3; i++ {
		require.NoError(t, client.CoreV1().Nodes().Delete(ctx, fmt.Sprintf("esx-%d", i), metav1.DeleteOptions{}))
	}
	mappings = []podvmv2.NodeMapping{nodeMapping(virtual, "host-1", "host-2", "host-3")}
	check(map[string]string{"host-1": "compute-a", "host-2": "compute-a", "host-3": "compute-a"})
	require.Empty(t, c.GetNodeNameToHostMoIDMap(ctx),
		"the node name -> single host accessor never names a cluster-backed Node")

	// Callers cannot corrupt the next snapshot, and host removal is reflected.
	mutated, err := c.GetNodeIDtoNameMap(ctx)
	require.NoError(t, err)
	mutated["host-1"] = "wrong"
	mappings = []podvmv2.NodeMapping{nodeMapping(virtual, "host-2")}
	check(map[string]string{"host-2": "compute-a"})

	// A failed mapping read never reuses previous membership, and never answers from the
	// dedicated Nodes alone: a caller cannot tell a withheld host from an idle one.
	listErr = fmt.Errorf("API server unavailable")
	owners, err := c.GetNodeIDtoNameMap(ctx)
	require.Error(t, err)
	require.Nil(t, owners)
	listErr = nil
	check(map[string]string{"host-2": "compute-a"})

	require.NoError(t, client.CoreV1().Nodes().Delete(ctx, "compute-a", metav1.DeleteOptions{}))
	check(map[string]string{})
}

func TestVirtualNodeInventoryRejectsUntrustworthyMapping(t *testing.T) {
	scenarios := []string{
		"no mapping", "not ready", "wrong owner uid", "not controller owned", "deleting mapping",
		"missing zone", "deleting node", "unknown node type", "node list fails",
		"mapping list fails", "conflicting host claim",
	}
	for _, scenario := range scenarios {
		t.Run(scenario, func(t *testing.T) {
			node := inventoryNode("compute-a", "masquerade", true)
			objects := []runtime.Object{node}
			mapping := nodeMapping(node, "host-1")
			mappings := []podvmv2.NodeMapping{mapping}
			var listErr, nodeListErr error
			// Only a failed lookup is an error. An untrustworthy or absent mapping
			// leaves the VirtualNode owning nothing, which is a valid answer.
			wantErr := false

			switch scenario {
			case "no mapping":
				mappings = nil
			case "not ready":
				mappings[0].Status.Conditions[0].Status = metav1.ConditionFalse
			case "wrong owner uid":
				mappings[0].OwnerReferences[0].UID = "replaced-node-uid"
			case "not controller owned":
				mappings[0].OwnerReferences[0].Controller = nil
			case "deleting mapping":
				now := metav1.Now()
				mappings[0].DeletionTimestamp = &now
			case "missing zone":
				delete(node.Labels, corev1.LabelTopologyZone)
			case "deleting node":
				now := metav1.Now()
				node.DeletionTimestamp = &now
			case "unknown node type":
				node.Annotations[podvmv2.NodeTypeAnnotation] = "entity-group"
			case "node list fails":
				wantErr = true
				nodeListErr = fmt.Errorf("unavailable")
			case "mapping list fails":
				wantErr = true
				listErr = fmt.Errorf("unavailable")
			case "conflicting host claim":
				// Two VirtualNodes list the same host. NodeMapping carries no
				// cluster identity, so this per-host check is what rejects it.
				other := inventoryNode("compute-b", "", true)
				objects = append(objects, other)
				mappings = append(mappings, nodeMapping(other, "host-1"))
			}

			client := fake.NewSimpleClientset(objects...)
			if nodeListErr != nil {
				client.PrependReactor("list", "nodes", func(clienttesting.Action) (bool, runtime.Object, error) {
					return true, nil, nodeListErr
				})
			}
			h := &hostInventory{
				client: client,
				nodeMappings: func(context.Context) ([]podvmv2.NodeMapping, error) {
					return mappings, listErr
				},
			}
			owners, err := h.snapshot(context.Background())
			if wantErr {
				// Host ownership could not be resolved at all. Returning the dedicated
				// owners alone would look like a complete answer to ListVolumes.
				require.Error(t, err)
				require.Nil(t, owners)
			} else {
				// A VirtualNode CSI cannot trust simply owns no hosts, which is a normal
				// state and not a failure.
				require.NoError(t, err)
				require.Empty(t, owners)
			}
		})
	}
}

func TestDedicatedNodesReserveHostsDuringHandoff(t *testing.T) {
	for _, scenario := range []string{"dedicated", "deleting", "unknown type", "conflicting dedicated"} {
		t.Run(scenario, func(t *testing.T) {
			node := inventoryNode("virtual-node-legacy-name", "host-1", false)
			virtual := inventoryNode("compute-a", "", true)
			objects := []runtime.Object{node, virtual}
			switch scenario {
			case "deleting":
				now := metav1.Now()
				node.DeletionTimestamp = &now
			case "unknown type":
				node.Annotations[podvmv2.NodeTypeAnnotation] = "entity-group"
			case "conflicting dedicated":
				objects = append(objects, inventoryNode("duplicate", "host-1", false))
			}
			// Correct producer behavior: the mapping excludes host-1 because a
			// dedicated Node still holds it.
			mappings := []podvmv2.NodeMapping{nodeMapping(virtual, "host-2")}
			h := &hostInventory{
				client: fake.NewSimpleClientset(objects...),
				nodeMappings: func(context.Context) ([]podvmv2.NodeMapping, error) {
					return mappings, nil
				},
			}
			want := map[string]string{"host-2": "compute-a"}
			if scenario == "dedicated" {
				want["host-1"] = node.Name
			}
			owners, err := h.snapshot(context.Background())
			require.NoError(t, err)
			require.Equal(t, want, owners)
		})
	}
}

// TestNodeMappingContradictsDedicatedNode covers the producers disagreeing.
// status.hosts is scoped to hosts without a dedicated Node, so an overlap means
// producer lag, a reverse transition or a bug. Neither claimant wins.
func TestNodeMappingContradictsDedicatedNode(t *testing.T) {
	dedicated := inventoryNode("esx-1", "host-1", false)
	virtual := inventoryNode("compute-a", "", true)
	mappings := []podvmv2.NodeMapping{nodeMapping(virtual, "host-1", "host-2")}
	h := &hostInventory{
		client: fake.NewSimpleClientset(dedicated, virtual),
		nodeMappings: func(context.Context) ([]podvmv2.NodeMapping, error) {
			return mappings, nil
		},
	}
	owners, err := h.snapshot(context.Background())
	require.NoError(t, err)
	require.Equal(t, map[string]string{"host-2": "compute-a"}, owners)
}
