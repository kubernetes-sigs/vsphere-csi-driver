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

package podvmv2

import (
	"context"
	"fmt"
	"strings"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/dynamic"
)

// HostMappingGVR identifies the platform resource that publishes which ESXi
// hosts a VirtualNode owns. Objects exist only for VirtualNodes, are named
// after the Node and are owned by it, so Kubernetes garbage collection removes
// a mapping when its Node goes away.
// TODO: Confirm against the platform's published CRD once it ships.
var HostMappingGVR = schema.GroupVersionResource{
	Group:    "virtualnode.vmware.com",
	Version:  "v1alpha1",
	Resource: "hostmappings",
}

// HostMappingConditionReady reports whether the host list is current.
const HostMappingConditionReady = "Ready"

// HostMapping records the ESXi hosts a VirtualNode owns. The platform scopes
// spec.hosts to hosts that have no dedicated Kubernetes Node, so this resource
// states ownership rather than raw vCenter cluster membership. CSI therefore
// does not infer that a host has completed its upgrade handoff.
type HostMapping struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`
	Spec              HostMappingSpec   `json:"spec"`
	Status            HostMappingStatus `json:"status,omitempty"`
}

// HostMappingSpec identifies the compute cluster and the hosts owned in it.
type HostMappingSpec struct {
	VCenterUUID        string            `json:"vCenterUUID,omitempty"`
	ComputeClusterMOID string            `json:"computeClusterMOID,omitempty"`
	Hosts              []HostMappingHost `json:"hosts,omitempty"`
}

// HostMappingHost identifies one ESXi host owned by the VirtualNode.
type HostMappingHost struct {
	MOID string `json:"moid,omitempty"`
	Name string `json:"name,omitempty"`
}

// HostMappingStatus reports the observed state of a HostMapping.
type HostMappingStatus struct {
	Conditions []metav1.Condition `json:"conditions,omitempty"`
}

// HostMappingList is the list form returned by a collection read.
type HostMappingList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []HostMapping `json:"items"`
}

// ClusterIdentity returns the (vCenter UUID, cluster MoID) pair this mapping
// describes. Cluster MoIDs are scoped to a vCenter, so the pair is the key.
func (m *HostMapping) ClusterIdentity() (string, string) {
	if m == nil {
		return "", ""
	}
	return strings.ToLower(m.Spec.VCenterUUID), m.Spec.ComputeClusterMOID
}

// HostMoIDs returns the owned host MoIDs. An empty result is the normal state
// before the first ESXi host upgrade: the VirtualNode owns no hosts yet. It is
// not missing data and must not trigger a fallback.
func (m *HostMapping) HostMoIDs() []string {
	if m == nil {
		return nil
	}
	ids := make([]string, 0, len(m.Spec.Hosts))
	for _, host := range m.Spec.Hosts {
		if host.MOID != "" {
			ids = append(ids, host.MOID)
		}
	}
	return ids
}

// ValidateFor reports why this mapping cannot be trusted as the ownership
// record for node in the vCenter identified by vcUUID. Every failure must
// leave the VirtualNode's hosts unpublished rather than fall back to a
// previous mapping or to raw cluster membership.
func (m *HostMapping) ValidateFor(node *corev1.Node, vcUUID string) error {
	if m == nil {
		return fmt.Errorf("no HostMapping")
	}
	if node == nil || node.UID == "" {
		return fmt.Errorf("node identity is required to validate a HostMapping")
	}
	if m.Name != node.Name {
		return fmt.Errorf("HostMapping %q does not match Node %q", m.Name, node.Name)
	}
	if m.DeletionTimestamp != nil {
		return fmt.Errorf("HostMapping %q is being deleted", m.Name)
	}
	if !ownedByNode(m, node) {
		// A same-named mapping left by an earlier Node incarnation describes
		// inventory that the current Node never claimed.
		return fmt.Errorf("HostMapping %q is not owned by Node %q with UID %q", m.Name, node.Name, node.UID)
	}
	ready := meta.FindStatusCondition(m.Status.Conditions, HostMappingConditionReady)
	if ready == nil || ready.Status != metav1.ConditionTrue {
		return fmt.Errorf("HostMapping %q is not Ready", m.Name)
	}
	// TODO: Confirm the producer always stamps observedGeneration on Ready.
	if m.Generation == 0 || ready.ObservedGeneration != m.Generation {
		return fmt.Errorf("HostMapping %q Ready condition is stale: observed %d, current %d",
			m.Name, ready.ObservedGeneration, m.Generation)
	}
	if m.Spec.ComputeClusterMOID == "" {
		return fmt.Errorf("HostMapping %q has no compute cluster MoID", m.Name)
	}
	// Host MoIDs are unique per vCenter but not globally. This equality check is
	// what makes the current single-vCenter limitation fail closed rather than
	// silently resolving a colliding MoID against the wrong physical host.
	if vcUUID == "" || !strings.EqualFold(m.Spec.VCenterUUID, vcUUID) {
		return fmt.Errorf("HostMapping %q vCenter UUID %q does not match the configured vCenter %q",
			m.Name, m.Spec.VCenterUUID, vcUUID)
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

// GetHostMapping reads the mapping named after a VirtualNode.
func GetHostMapping(ctx context.Context, client dynamic.Interface, name string) (*HostMapping, error) {
	obj, err := client.Resource(HostMappingGVR).Get(ctx, name, metav1.GetOptions{})
	if err != nil {
		return nil, err
	}
	var mapping HostMapping
	if err := runtime.DefaultUnstructuredConverter.FromUnstructured(obj.Object, &mapping); err != nil {
		return nil, err
	}
	return &mapping, nil
}

// ListHostMappings reads every mapping so one collection read can serve an
// ownership snapshot covering all VirtualNodes.
func ListHostMappings(ctx context.Context, client dynamic.Interface) ([]HostMapping, error) {
	list, err := client.Resource(HostMappingGVR).List(ctx, metav1.ListOptions{})
	if err != nil {
		return nil, err
	}
	mappings := make([]HostMapping, 0, len(list.Items))
	for i := range list.Items {
		var mapping HostMapping
		if err := runtime.DefaultUnstructuredConverter.FromUnstructured(list.Items[i].Object, &mapping); err != nil {
			return nil, err
		}
		mappings = append(mappings, mapping)
	}
	return mappings, nil
}
