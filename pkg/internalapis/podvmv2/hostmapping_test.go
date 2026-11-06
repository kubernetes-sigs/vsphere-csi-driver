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

const testVC = "42305B1E-8F1A-4A3B-9C2D-1234567890AB"

func testNode() *corev1.Node {
	return &corev1.Node{ObjectMeta: metav1.ObjectMeta{Name: "compute-a", UID: "uid-1"}}
}

func validMapping(node *corev1.Node) *HostMapping {
	controller := true
	return &HostMapping{
		ObjectMeta: metav1.ObjectMeta{
			Name:       node.Name,
			Generation: 2,
			OwnerReferences: []metav1.OwnerReference{{
				APIVersion: "v1", Kind: "Node", Name: node.Name, UID: node.UID, Controller: &controller,
			}},
		},
		Spec: HostMappingSpec{
			VCenterUUID:        testVC,
			ComputeClusterMOID: "domain-c8",
			Hosts:              []HostMappingHost{{MOID: "host-1", Name: "esx-1"}, {MOID: "host-2"}},
		},
		Status: HostMappingStatus{Conditions: []metav1.Condition{{
			Type: HostMappingConditionReady, Status: metav1.ConditionTrue, ObservedGeneration: 2,
		}}},
	}
}

func TestHostMappingValidateFor(t *testing.T) {
	notController := false
	for _, tc := range []struct {
		name   string
		mutate func(*HostMapping, *corev1.Node) (*HostMapping, *corev1.Node, string)
		valid  bool
	}{
		{name: "valid; UUID compared case-insensitively", valid: true,
			mutate: func(m *HostMapping, n *corev1.Node) (*HostMapping, *corev1.Node, string) {
				return m, n, "42305b1e-8f1a-4a3b-9c2d-1234567890ab"
			}},
		{name: "nil mapping", mutate: func(_ *HostMapping, n *corev1.Node) (*HostMapping, *corev1.Node, string) {
			return nil, n, testVC
		}},
		{name: "nil node", mutate: func(m *HostMapping, _ *corev1.Node) (*HostMapping, *corev1.Node, string) {
			return m, nil, testVC
		}},
		{name: "node without UID", mutate: func(m *HostMapping, n *corev1.Node) (*HostMapping, *corev1.Node, string) {
			n.UID = ""
			return m, n, testVC
		}},
		{name: "name mismatch", mutate: func(m *HostMapping, n *corev1.Node) (*HostMapping, *corev1.Node, string) {
			m.Name = "other"
			return m, n, testVC
		}},
		{name: "deleting", mutate: func(m *HostMapping, n *corev1.Node) (*HostMapping, *corev1.Node, string) {
			now := metav1.Now()
			m.DeletionTimestamp = &now
			return m, n, testVC
		}},
		{name: "owned by earlier Node incarnation",
			mutate: func(m *HostMapping, n *corev1.Node) (*HostMapping, *corev1.Node, string) {
				m.OwnerReferences[0].UID = "uid-0"
				return m, n, testVC
			}},
		{name: "owner reference is not controller",
			mutate: func(m *HostMapping, n *corev1.Node) (*HostMapping, *corev1.Node, string) {
				m.OwnerReferences[0].Controller = &notController
				return m, n, testVC
			}},
		{name: "owner reference without controller flag",
			mutate: func(m *HostMapping, n *corev1.Node) (*HostMapping, *corev1.Node, string) {
				m.OwnerReferences[0].Controller = nil
				return m, n, testVC
			}},
		{name: "owner of a different kind",
			mutate: func(m *HostMapping, n *corev1.Node) (*HostMapping, *corev1.Node, string) {
				m.OwnerReferences[0].Kind = "Pod"
				return m, n, testVC
			}},
		{name: "no Ready condition", mutate: func(m *HostMapping, n *corev1.Node) (*HostMapping, *corev1.Node, string) {
			m.Status.Conditions = nil
			return m, n, testVC
		}},
		{name: "Ready=False", mutate: func(m *HostMapping, n *corev1.Node) (*HostMapping, *corev1.Node, string) {
			m.Status.Conditions[0].Status = metav1.ConditionFalse
			return m, n, testVC
		}},
		{name: "stale Ready", mutate: func(m *HostMapping, n *corev1.Node) (*HostMapping, *corev1.Node, string) {
			m.Status.Conditions[0].ObservedGeneration = 1
			return m, n, testVC
		}},
		{name: "generation unset", mutate: func(m *HostMapping, n *corev1.Node) (*HostMapping, *corev1.Node, string) {
			m.Generation, m.Status.Conditions[0].ObservedGeneration = 0, 0
			return m, n, testVC
		}},
		{name: "no cluster MoID", mutate: func(m *HostMapping, n *corev1.Node) (*HostMapping, *corev1.Node, string) {
			m.Spec.ComputeClusterMOID = ""
			return m, n, testVC
		}},
		{name: "foreign vCenter", mutate: func(m *HostMapping, n *corev1.Node) (*HostMapping, *corev1.Node, string) {
			return m, n, "11111111-2222-3333-4444-555555555555"
		}},
		{name: "configured vCenter unknown",
			mutate: func(m *HostMapping, n *corev1.Node) (*HostMapping, *corev1.Node, string) {
				return m, n, ""
			}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			node := testNode()
			mapping, node, vc := tc.mutate(validMapping(node), node)
			err := mapping.ValidateFor(node, vc)
			if tc.valid {
				require.NoError(t, err)
			} else {
				require.Error(t, err)
			}
		})
	}
}

func TestHostMappingAccessors(t *testing.T) {
	var none *HostMapping
	vc, cluster := none.ClusterIdentity()
	require.Empty(t, vc)
	require.Empty(t, cluster)
	require.Nil(t, none.HostMoIDs())

	m := validMapping(testNode())
	vc, cluster = m.ClusterIdentity()
	require.Equal(t, "42305b1e-8f1a-4a3b-9c2d-1234567890ab", vc, "vCenter UUID is normalized to lower case")
	require.Equal(t, "domain-c8", cluster)

	m.Spec.Hosts = append(m.Spec.Hosts, HostMappingHost{Name: "no-moid"})
	require.Equal(t, []string{"host-1", "host-2"}, m.HostMoIDs(), "entries without a MoID are skipped")

	// An empty host list is the normal pre-upgrade state.
	m.Spec.Hosts = nil
	require.NotNil(t, m.HostMoIDs())
	require.Empty(t, m.HostMoIDs())
}

func TestHostMappingClientReads(t *testing.T) {
	ctx := context.Background()
	scheme := runtime.NewScheme()
	listKinds := map[schema.GroupVersionResource]string{HostMappingGVR: "HostMappingList"}

	t.Run("empty collection", func(t *testing.T) {
		client := dynamicfake.NewSimpleDynamicClientWithCustomListKinds(scheme, listKinds)
		mappings, err := ListHostMappings(ctx, client)
		require.NoError(t, err)
		require.Empty(t, mappings)
		_, err = GetHostMapping(ctx, client, "compute-a")
		require.Error(t, err)
	})

	t.Run("decodes stored objects", func(t *testing.T) {
		node := testNode()
		want := validMapping(node)
		want.APIVersion = HostMappingGVR.GroupVersion().String()
		want.Kind = "HostMapping"
		raw, err := runtime.DefaultUnstructuredConverter.ToUnstructured(want)
		require.NoError(t, err)
		client := dynamicfake.NewSimpleDynamicClientWithCustomListKinds(scheme, listKinds,
			&unstructured.Unstructured{Object: raw})

		got, err := GetHostMapping(ctx, client, node.Name)
		require.NoError(t, err)
		require.Equal(t, want.Spec, got.Spec)
		require.NoError(t, got.ValidateFor(node, testVC))

		mappings, err := ListHostMappings(ctx, client)
		require.NoError(t, err)
		require.Len(t, mappings, 1)
		require.Equal(t, []string{"host-1", "host-2"}, mappings[0].HostMoIDs())
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
