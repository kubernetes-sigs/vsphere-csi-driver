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
	"fmt"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/dynamic"
)

// NodeMappingGVR identifies the platform resource that records which ESXi
// hosts a VirtualNode represents. The platform creates one cluster-scoped
// NodeMapping per VirtualNode, named after the Node and owned by it, so
// Kubernetes garbage collection removes it when the Node goes away. The CRD
// is registered only when the PodVM v2 feature and capability are enabled.
//
// These types mirror virtualnode.vmware.com/v1alpha1 NodeMapping from the
// Supervisor controller-managers APIs; keep them in sync with that definition.
var NodeMappingGVR = schema.GroupVersionResource{
	Group:    "virtualnode.vmware.com",
	Version:  "v1alpha1",
	Resource: "nodemappings",
}

// NodeMappingConditionReady reports whether status.hosts reflects the current
// set of ESXi hosts represented by the Node.
const NodeMappingConditionReady = "Ready"

// NodeMapping records the ESXi hosts a VirtualNode represents. The host list
// contains only hosts that have no dedicated Kubernetes Node, so it states
// ownership rather than raw vCenter cluster membership, and CSI does not infer
// that a host has completed its upgrade handoff.
type NodeMapping struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`
	Spec              NodeMappingSpec   `json:"spec"`
	Status            NodeMappingStatus `json:"status,omitempty"`
}

// NodeMappingSpec has no fields; it is reserved for future use.
type NodeMappingSpec struct{}

// NodeMappingHost identifies one ESXi host represented by the VirtualNode.
type NodeMappingHost struct {
	MOID string `json:"moid,omitempty"`
	Name string `json:"name,omitempty"`
}

// NodeMappingStatus reports the observed state of a NodeMapping, including
// the hosts the VirtualNode represents.
type NodeMappingStatus struct {
	Conditions []metav1.Condition `json:"conditions,omitempty"`
	Hosts      []NodeMappingHost  `json:"hosts,omitempty"`
}

// NodeMappingList is the list form returned by a collection read.
type NodeMappingList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []NodeMapping `json:"items"`
}

// HostMoIDs returns the represented host MoIDs. An empty result is the normal
// state before the first ESXi host upgrade: the VirtualNode represents no
// hosts yet. It is not missing data and must not trigger a fallback.
func (m *NodeMapping) HostMoIDs() []string {
	if m == nil {
		return nil
	}
	ids := make([]string, 0, len(m.Status.Hosts))
	for _, host := range m.Status.Hosts {
		if host.MOID != "" {
			ids = append(ids, host.MOID)
		}
	}
	return ids
}

// ValidateFor reports why this mapping cannot be trusted as the ownership
// record for node. Every failure must leave the VirtualNode's hosts
// unpublished rather than fall back to a previous mapping or to raw cluster
// membership.
//
// Ready is not compared with metadata.generation: the spec is empty, so the
// generation never changes and observedGeneration carries no information.
func (m *NodeMapping) ValidateFor(node *corev1.Node) error {
	if m == nil {
		return fmt.Errorf("no NodeMapping")
	}
	if node == nil || node.UID == "" {
		return fmt.Errorf("node identity is required to validate a NodeMapping")
	}
	if m.Name != node.Name {
		return fmt.Errorf("NodeMapping %q does not match Node %q", m.Name, node.Name)
	}
	if m.DeletionTimestamp != nil {
		return fmt.Errorf("NodeMapping %q is being deleted", m.Name)
	}
	if !ownedByNode(m, node) {
		// A same-named mapping left by an earlier Node incarnation describes
		// inventory that the current Node never claimed.
		return fmt.Errorf("NodeMapping %q is not owned by Node %q with UID %q", m.Name, node.Name, node.UID)
	}
	ready := meta.FindStatusCondition(m.Status.Conditions, NodeMappingConditionReady)
	if ready == nil || ready.Status != metav1.ConditionTrue {
		return fmt.Errorf("NodeMapping %q is not Ready", m.Name)
	}
	return nil
}

// ownedByNode requires a controller reference to this exact Node incarnation.
func ownedByNode(m metav1.Object, node *corev1.Node) bool {
	for _, ref := range m.GetOwnerReferences() {
		if ref.Kind != "Node" || ref.APIVersion != "v1" {
			continue
		}
		if ref.Controller == nil || !*ref.Controller {
			continue
		}
		if ref.Name == node.Name && ref.UID == node.UID {
			return true
		}
	}
	return false
}

// ListNodeMappings reads every mapping so one collection read can serve an
// ownership snapshot covering all VirtualNodes.
func ListNodeMappings(ctx context.Context, client dynamic.Interface) ([]NodeMapping, error) {
	list, err := client.Resource(NodeMappingGVR).List(ctx, metav1.ListOptions{})
	if err != nil {
		return nil, err
	}
	mappings := make([]NodeMapping, 0, len(list.Items))
	for i := range list.Items {
		var mapping NodeMapping
		if err := runtime.DefaultUnstructuredConverter.FromUnstructured(list.Items[i].Object, &mapping); err != nil {
			return nil, err
		}
		mappings = append(mappings, mapping)
	}
	return mappings, nil
}
