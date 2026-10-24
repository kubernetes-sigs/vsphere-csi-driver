/*
Copyright 2020 The Kubernetes Authors.

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
	"crypto/tls"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"
	cnstypes "github.com/vmware/govmomi/cns/types"
	"github.com/vmware/govmomi/simulator"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes/fake"
	clienttesting "k8s.io/client-go/testing"
	"sigs.k8s.io/vsphere-csi-driver/v3/pkg/common/cns-lib/vsphere"
	commontypes "sigs.k8s.io/vsphere-csi-driver/v3/pkg/common/types"
	"sigs.k8s.io/vsphere-csi-driver/v3/pkg/csi/service/common"
	"sigs.k8s.io/vsphere-csi-driver/v3/pkg/internalapis/podvmv2"
)

const testVCUUID = "42305b1e-8f1a-4a3b-9c2d-1234567890ab"

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

// hostMapping builds a mapping the resolver should accept. The platform scopes
// hosts to those without a dedicated Node, so tests list only handed-off hosts.
func hostMapping(node *corev1.Node, cluster string, hosts ...string) podvmv2.HostMapping {
	controller := true
	mapping := podvmv2.HostMapping{
		ObjectMeta: metav1.ObjectMeta{
			Name:       node.Name,
			Generation: 1,
			OwnerReferences: []metav1.OwnerReference{{
				APIVersion: "v1", Kind: "Node", Name: node.Name, UID: node.UID, Controller: &controller,
			}},
		},
		Spec: podvmv2.HostMappingSpec{VCenterUUID: testVCUUID, ComputeClusterMOID: cluster},
		Status: podvmv2.HostMappingStatus{Conditions: []metav1.Condition{{
			Type: podvmv2.HostMappingConditionReady, Status: metav1.ConditionTrue,
			ObservedGeneration: 1, Reason: "Synced", LastTransitionTime: metav1.Now(),
		}}},
	}
	for _, host := range hosts {
		mapping.Spec.Hosts = append(mapping.Spec.Hosts, podvmv2.HostMappingHost{MOID: host, Name: host + ".example"})
	}
	return mapping
}

func TestVirtualNodeInventoryHostMappingHandoff(t *testing.T) {
	ctx := context.Background()
	client := fake.NewSimpleClientset()
	virtual := inventoryNode("compute-a", "host-masquerade", true)
	var mappings []podvmv2.HostMapping
	var listErr error
	h := &hostInventory{
		client: client,
		hostMappings: func(context.Context) ([]podvmv2.HostMapping, error) {
			return mappings, listErr
		},
		vcenterUUID: func(context.Context) (string, error) { return testVCUUID, nil },
	}
	setPodVMV2Capability(t, true)
	c := &K8sOrchestrator{clusterFlavor: cnstypes.CnsClusterFlavorWorkload, hostInventory: h}
	create := func(node *corev1.Node) {
		t.Helper()
		_, err := client.CoreV1().Nodes().Create(ctx, node, metav1.CreateOptions{})
		require.NoError(t, err)
	}
	check := func(want map[string]string) { t.Helper(); require.Equal(t, want, c.GetNodeIDtoNameMap(ctx)) }

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
	mappings = []podvmv2.HostMapping{hostMapping(virtual, "domain-c1")}
	check(legacy)

	// The platform hands off host-1: the dedicated Node goes away and the
	// mapping starts publishing the host.
	require.NoError(t, client.CoreV1().Nodes().Delete(ctx, "esx-1", metav1.DeleteOptions{}))
	mappings = []podvmv2.HostMapping{hostMapping(virtual, "domain-c1", "host-1")}
	check(map[string]string{"host-1": "compute-a", "host-2": "esx-2", "host-3": "esx-3"})

	// A mapping that claims a host still held by a dedicated Node means the two
	// producers disagree. Publish no owner for it rather than picking one.
	mappings = []podvmv2.HostMapping{hostMapping(virtual, "domain-c1", "host-1", "host-2")}
	check(map[string]string{"host-1": "compute-a", "host-3": "esx-3"})

	for i := 2; i <= 3; i++ {
		require.NoError(t, client.CoreV1().Nodes().Delete(ctx, fmt.Sprintf("esx-%d", i), metav1.DeleteOptions{}))
	}
	mappings = []podvmv2.HostMapping{hostMapping(virtual, "domain-c1", "host-1", "host-2", "host-3")}
	check(map[string]string{"host-1": "compute-a", "host-2": "compute-a", "host-3": "compute-a"})
	require.Empty(t, c.GetNodeNameToHostMoIDMap(ctx), "singleton accessor must not choose a cluster member")

	// Callers cannot corrupt the next snapshot, and host removal is reflected.
	mutated := c.GetNodeIDtoNameMap(ctx)
	mutated["host-1"] = "wrong"
	mappings = []podvmv2.HostMapping{hostMapping(virtual, "domain-c1", "host-2")}
	check(map[string]string{"host-2": "compute-a"})

	// A failed mapping read never reuses previous membership.
	listErr = fmt.Errorf("API server unavailable")
	check(map[string]string{})
	listErr = nil
	check(map[string]string{"host-2": "compute-a"})

	require.NoError(t, client.CoreV1().Nodes().Delete(ctx, "compute-a", metav1.DeleteOptions{}))
	check(map[string]string{})
}

func TestVirtualNodeInventoryRejectsUntrustworthyMapping(t *testing.T) {
	scenarios := []string{
		"no mapping", "not ready", "stale generation", "zero generation", "wrong owner uid",
		"not controller owned", "foreign vcenter", "missing cluster moid", "deleting mapping",
		"missing zone", "deleting node", "unknown node type", "node list fails",
		"mapping list fails", "vcenter unresolved", "duplicate cluster claim", "conflicting host claim",
	}
	for _, scenario := range scenarios {
		t.Run(scenario, func(t *testing.T) {
			node := inventoryNode("compute-a", "masquerade", true)
			objects := []runtime.Object{node}
			mapping := hostMapping(node, "domain-c1", "host-1")
			mappings := []podvmv2.HostMapping{mapping}
			vcUUID := testVCUUID
			var listErr, nodeListErr, vcErr error

			switch scenario {
			case "no mapping":
				mappings = nil
			case "not ready":
				mappings[0].Status.Conditions[0].Status = metav1.ConditionFalse
			case "stale generation":
				mappings[0].Generation = 2
			case "zero generation":
				mappings[0].Generation = 0
				mappings[0].Status.Conditions[0].ObservedGeneration = 0
			case "wrong owner uid":
				mappings[0].OwnerReferences[0].UID = "replaced-node-uid"
			case "not controller owned":
				mappings[0].OwnerReferences[0].Controller = nil
			case "foreign vcenter":
				mappings[0].Spec.VCenterUUID = "99999999-8f1a-4a3b-9c2d-1234567890ab"
			case "missing cluster moid":
				mappings[0].Spec.ComputeClusterMOID = ""
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
				nodeListErr = fmt.Errorf("unavailable")
			case "mapping list fails":
				listErr = fmt.Errorf("unavailable")
			case "vcenter unresolved":
				vcErr = fmt.Errorf("no configured vCenter")
			case "duplicate cluster claim":
				other := inventoryNode("compute-b", "", true)
				objects = append(objects, other)
				mappings = append(mappings, hostMapping(other, "domain-c1", "host-1"))
			case "conflicting host claim":
				other := inventoryNode("compute-b", "", true)
				objects = append(objects, other)
				mappings = append(mappings, hostMapping(other, "domain-c2", "host-1"))
			}

			client := fake.NewSimpleClientset(objects...)
			if nodeListErr != nil {
				client.PrependReactor("list", "nodes", func(clienttesting.Action) (bool, runtime.Object, error) {
					return true, nil, nodeListErr
				})
			}
			h := &hostInventory{
				client: client,
				hostMappings: func(context.Context) ([]podvmv2.HostMapping, error) {
					return mappings, listErr
				},
				vcenterUUID: func(context.Context) (string, error) { return vcUUID, vcErr },
			}
			require.Empty(t, h.snapshot(context.Background()))
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
			mappings := []podvmv2.HostMapping{hostMapping(virtual, "domain-c1", "host-2")}
			h := &hostInventory{
				client: fake.NewSimpleClientset(objects...),
				hostMappings: func(context.Context) ([]podvmv2.HostMapping, error) {
					return mappings, nil
				},
				vcenterUUID: func(context.Context) (string, error) { return testVCUUID, nil },
			}
			want := map[string]string{"host-2": "compute-a"}
			if scenario == "dedicated" {
				want["host-1"] = node.Name
			}
			require.Equal(t, want, h.snapshot(context.Background()))
		})
	}
}

// TestHostMappingContradictsDedicatedNode covers the producers disagreeing.
// spec.hosts is scoped to hosts without a dedicated Node, so an overlap means
// producer lag, a reverse transition or a bug. Neither claimant wins.
func TestHostMappingContradictsDedicatedNode(t *testing.T) {
	dedicated := inventoryNode("esx-1", "host-1", false)
	virtual := inventoryNode("compute-a", "", true)
	mappings := []podvmv2.HostMapping{hostMapping(virtual, "domain-c1", "host-1", "host-2")}
	h := &hostInventory{
		client: fake.NewSimpleClientset(dedicated, virtual),
		hostMappings: func(context.Context) ([]podvmv2.HostMapping, error) {
			return mappings, nil
		},
		vcenterUUID: func(context.Context) (string, error) { return testVCUUID, nil },
	}
	require.Equal(t, map[string]string{"host-2": "compute-a"}, h.snapshot(context.Background()))
}

func TestConfiguredVCenterUUID(t *testing.T) {
	ctx := context.Background()
	model := simulator.VPX()
	model.ClusterHost = 3
	require.NoError(t, model.Create())
	defer model.Remove()
	model.Service.TLS = new(tls.Config)
	model.Service.RegisterEndpoints = true
	server := model.Service.NewServer()
	defer server.Close()
	confPath := filepath.Join(t.TempDir(), "csi.conf")
	require.NoError(t, os.WriteFile(confPath, []byte("[Global]\ncluster-id = \"test\"\n"+
		"[VirtualCenter \"127.0.0.1\"]\nuser = \"user@vsphere.local\"\npassword = \"pass\"\n"+
		"datacenters = \"DC0\"\ninsecure-flag = \"true\"\n"), 0600))
	t.Setenv("VSPHERE_CSI_CONFIG", confPath)
	t.Setenv("CLUSTER_FLAVOR", "VANILLA")
	port, err := strconv.Atoi(server.URL.Port())
	require.NoError(t, err)
	vc := &vsphere.VirtualCenter{ClientMutex: &sync.Mutex{}, Config: &vsphere.VirtualCenterConfig{
		Host: commontypes.NewFQDN(server.URL.Hostname()), Port: port, Username: "user", Password: "pass", Insecure: true,
	}}
	require.NoError(t, vc.Connect(ctx))
	defer func() { _ = vc.Disconnect(context.Background()) }()

	uuid, err := vcenterUUIDFrom(ctx, []*vsphere.VirtualCenter{vc})
	require.NoError(t, err)
	require.Equal(t, vc.Client.ServiceContent.About.InstanceUuid, uuid)
	require.NotEmpty(t, uuid)

	// Host MoIDs are only unique within a vCenter, so ownership must refuse to
	// resolve until host identity is vCenter-qualified.
	for _, tc := range []struct {
		name string
		vcs  []*vsphere.VirtualCenter
	}{
		{"not initialized", nil},
		{"multiple vcenters", []*vsphere.VirtualCenter{vc, vc}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got, err := vcenterUUIDFrom(ctx, tc.vcs)
			require.Error(t, err)
			require.Empty(t, got)
		})
	}
}
