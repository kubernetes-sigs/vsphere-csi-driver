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

// Package podvmv2 holds read-only integration contracts for cluster-backed Nodes.
package podvmv2

import corev1 "k8s.io/api/core/v1"

const (
	// TODO: Confirm the platform's Node identity annotation and supported values.
	NodeTypeAnnotation = "pod.vmware.com/node-type"
	VirtualNodeType    = "virtual-node"
)

// IsVirtualNode uses explicit identity, never a Node name or masqueraded host ID.
func IsVirtualNode(node *corev1.Node) bool {
	return node != nil && node.Annotations[NodeTypeAnnotation] == VirtualNodeType
}

// IsDedicatedHostNode recognizes the existing ESXi Node contract. Unknown future
// node types must not silently be treated as dedicated physical hosts.
func IsDedicatedHostNode(node *corev1.Node) bool {
	return node != nil && node.Annotations[NodeTypeAnnotation] == ""
}
