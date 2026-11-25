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

// Package podvmv2 holds read-only integration contracts for cluster-backed Nodes.
package podvmv2

import corev1 "k8s.io/api/core/v1"

const (
	// VirtualNodeProviderLabel is the Node label the virtual node controller sets on every
	// VirtualNode it registers. The Supervisor scheduler extender identifies VirtualNodes by
	// the same label. The pod.vmware.com/node-type annotation is a Pod annotation for the
	// network operator and is never set on Nodes, so CSI does not read it.
	VirtualNodeProviderLabel = "virtual-kubelet.io/provider"
	// VirtualNodeProviderValue is the VirtualNodeProviderLabel value of a VirtualNode.
	VirtualNodeProviderValue = "wcp-virtual-node"
)

// IsVirtualNode uses explicit identity, never a Node name or masqueraded host ID.
func IsVirtualNode(node *corev1.Node) bool {
	return node != nil && node.Labels[VirtualNodeProviderLabel] == VirtualNodeProviderValue
}

// IsDedicatedHostNode recognizes the existing ESXi Node contract: a Node with no
// virtual-kubelet provider. A Node served by any other provider is not a dedicated
// physical host and must not silently be treated as one.
func IsDedicatedHostNode(node *corev1.Node) bool {
	return node != nil && node.Labels[VirtualNodeProviderLabel] == ""
}

// NodeTypeOf returns the provider that identifies node's type, for log messages.
// It is empty for a dedicated ESXi Node.
func NodeTypeOf(node *corev1.Node) string {
	if node == nil {
		return ""
	}
	return node.Labels[VirtualNodeProviderLabel]
}
