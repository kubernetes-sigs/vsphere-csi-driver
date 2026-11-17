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

package podvmv2

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	dynamicfake "k8s.io/client-go/dynamic/fake"
)

func testNode() *corev1.Node {
	return &corev1.Node{ObjectMeta: metav1.ObjectMeta{Name: "compute-a", UID: "uid-1"}}
}

func validMapping(node *corev1.Node) *NodeMapping {
	controller := true
	return &NodeMapping{
		ObjectMeta: metav1.ObjectMeta{
			Name:       node.Name,
			Generation: 1,
			OwnerReferences: []metav1.OwnerReference{{
				APIVersion: "v1", Kind: "Node", Name: node.Name, UID: node.UID, Controller: &controller,
			}},
		},
		Status: NodeMappingStatus{
			Conditions: []metav1.Condition{{Type: NodeMappingConditionReady, Status: metav1.ConditionTrue}},
			Hosts:      []NodeMappingHost{{MOID: "host-1", Name: "esx-1"}, {MOID: "host-2", Name: "esx-2"}},
		},
	}
}

func TestNodeMappingValidateFor(t *testing.T) {
	notController := false
	for _, tc := range []struct {
		name   string
		mutate func(*NodeMapping, *corev1.Node) (*NodeMapping, *corev1.Node)
		valid  bool
	}{
		{name: "valid", valid: true,
			mutate: func(m *NodeMapping, n *corev1.Node) (*NodeMapping, *corev1.Node) { return m, n }},
		{name: "valid without observedGeneration: the spec is empty", valid: true,
			mutate: func(m *NodeMapping, n *corev1.Node) (*NodeMapping, *corev1.Node) {
				m.Generation = 0
				m.Status.Conditions[0].ObservedGeneration = 0
				return m, n
			}},
		{name: "valid with no hosts yet", valid: true,
			mutate: func(m *NodeMapping, n *corev1.Node) (*NodeMapping, *corev1.Node) {
				m.Status.Hosts = nil
				return m, n
			}},
		{name: "nil mapping", mutate: func(_ *NodeMapping, n *corev1.Node) (*NodeMapping, *corev1.Node) {
			return nil, n
		}},
		{name: "nil node", mutate: func(m *NodeMapping, _ *corev1.Node) (*NodeMapping, *corev1.Node) {
			return m, nil
		}},
		{name: "node without UID", mutate: func(m *NodeMapping, n *corev1.Node) (*NodeMapping, *corev1.Node) {
			n.UID = ""
			return m, n
		}},
		{name: "name mismatch", mutate: func(m *NodeMapping, n *corev1.Node) (*NodeMapping, *corev1.Node) {
			m.Name = "other"
			return m, n
		}},
		{name: "deleting", mutate: func(m *NodeMapping, n *corev1.Node) (*NodeMapping, *corev1.Node) {
			now := metav1.Now()
			m.DeletionTimestamp = &now
			return m, n
		}},
		{name: "owned by earlier Node incarnation",
			mutate: func(m *NodeMapping, n *corev1.Node) (*NodeMapping, *corev1.Node) {
				m.OwnerReferences[0].UID = "uid-0"
				return m, n
			}},
		{name: "owner reference is not controller",
			mutate: func(m *NodeMapping, n *corev1.Node) (*NodeMapping, *corev1.Node) {
				m.OwnerReferences[0].Controller = &notController
				return m, n
			}},
		{name: "owner reference without controller flag",
			mutate: func(m *NodeMapping, n *corev1.Node) (*NodeMapping, *corev1.Node) {
				m.OwnerReferences[0].Controller = nil
				return m, n
			}},
		{name: "owner of a different kind",
			mutate: func(m *NodeMapping, n *corev1.Node) (*NodeMapping, *corev1.Node) {
				m.OwnerReferences[0].Kind = "Pod"
				return m, n
			}},
		{name: "no owner reference", mutate: func(m *NodeMapping, n *corev1.Node) (*NodeMapping, *corev1.Node) {
			m.OwnerReferences = nil
			return m, n
		}},
		{name: "no Ready condition", mutate: func(m *NodeMapping, n *corev1.Node) (*NodeMapping, *corev1.Node) {
			m.Status.Conditions = nil
			return m, n
		}},
		{name: "Ready=False", mutate: func(m *NodeMapping, n *corev1.Node) (*NodeMapping, *corev1.Node) {
			m.Status.Conditions[0].Status = metav1.ConditionFalse
			return m, n
		}},
		{name: "Ready=Unknown", mutate: func(m *NodeMapping, n *corev1.Node) (*NodeMapping, *corev1.Node) {
			m.Status.Conditions[0].Status = metav1.ConditionUnknown
			return m, n
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			node := testNode()
			mapping, node := tc.mutate(validMapping(node), node)
			err := mapping.ValidateFor(node)
			if tc.valid {
				require.NoError(t, err)
			} else {
				require.Error(t, err)
			}
		})
	}
}

func TestNodeMappingHostMoIDs(t *testing.T) {
	var none *NodeMapping
	require.Nil(t, none.HostMoIDs())

	m := validMapping(testNode())
	m.Status.Hosts = append(m.Status.Hosts, NodeMappingHost{Name: "no-moid"})
	require.Equal(t, []string{"host-1", "host-2"}, m.HostMoIDs(), "entries without a MoID are skipped")

	// An empty host list is the normal pre-upgrade state.
	m.Status.Hosts = nil
	require.NotNil(t, m.HostMoIDs())
	require.Empty(t, m.HostMoIDs())
}

func TestNodeMappingClientReads(t *testing.T) {
	ctx := context.Background()
	scheme := runtime.NewScheme()
	listKinds := map[schema.GroupVersionResource]string{NodeMappingGVR: "NodeMappingList"}

	t.Run("empty collection", func(t *testing.T) {
		client := dynamicfake.NewSimpleDynamicClientWithCustomListKinds(scheme, listKinds)
		mappings, err := ListNodeMappings(ctx, client)
		require.NoError(t, err)
		require.Empty(t, mappings)
	})

	t.Run("decodes stored objects", func(t *testing.T) {
		node := testNode()
		want := validMapping(node)
		want.APIVersion = NodeMappingGVR.GroupVersion().String()
		want.Kind = "NodeMapping"
		raw, err := runtime.DefaultUnstructuredConverter.ToUnstructured(want)
		require.NoError(t, err)
		client := dynamicfake.NewSimpleDynamicClientWithCustomListKinds(scheme, listKinds,
			&unstructured.Unstructured{Object: raw})

		mappings, err := ListNodeMappings(ctx, client)
		require.NoError(t, err)
		require.Len(t, mappings, 1)
		require.Equal(t, want.Status.Hosts, mappings[0].Status.Hosts)
		require.NoError(t, mappings[0].ValidateFor(node))
		require.Equal(t, []string{"host-1", "host-2"}, mappings[0].HostMoIDs())
	})

	t.Run("hosts are read from status, as published by the platform", func(t *testing.T) {
		obj := &unstructured.Unstructured{Object: map[string]interface{}{
			"apiVersion": NodeMappingGVR.GroupVersion().String(),
			"kind":       "NodeMapping",
			"metadata":   map[string]interface{}{"name": "compute-a"},
			"spec":       map[string]interface{}{},
			"status": map[string]interface{}{
				"hosts": []interface{}{map[string]interface{}{"moid": "host-42", "name": "esx-42"}},
			},
		}}
		client := dynamicfake.NewSimpleDynamicClientWithCustomListKinds(scheme, listKinds, obj)
		mappings, err := ListNodeMappings(ctx, client)
		require.NoError(t, err)
		require.Len(t, mappings, 1)
		require.Equal(t, []string{"host-42"}, mappings[0].HostMoIDs())
	})
}

func TestNodeClassification(t *testing.T) {
	withType := func(value string, set bool) *corev1.Node {
		node := &corev1.Node{ObjectMeta: metav1.ObjectMeta{Annotations: map[string]string{}}}
		if set {
			node.Annotations[NodeTypeAnnotation] = value
		}
		return node
	}
	for _, tc := range []struct {
		name               string
		node               *corev1.Node
		virtual, dedicated bool
	}{
		{name: "nil", node: nil},
		{name: "annotation absent is a dedicated ESXi Node", node: withType("", false), dedicated: true},
		{name: "empty annotation is a dedicated ESXi Node", node: withType("", true), dedicated: true},
		{name: "virtual node", node: withType(VirtualNodeType, true), virtual: true},
		{name: "unknown type is neither", node: withType("entity-group", true)},
		{name: "case differs is neither", node: withType("Virtual-Node", true)},
	} {
		t.Run(tc.name, func(t *testing.T) {
			require.Equal(t, tc.virtual, IsVirtualNode(tc.node))
			require.Equal(t, tc.dedicated, IsDedicatedHostNode(tc.node))
		})
	}
}
