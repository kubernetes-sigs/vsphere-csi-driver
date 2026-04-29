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

package v1alpha1

import (
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// Topology describes topology accessibility for the storage policy within the cluster.
type Topology struct {
	// AccessibleZones lists zones where the policy is accessible for this cluster.
	// +optional
	AccessibleZones []string `json:"accessibleZones,omitempty"`

	// Error describes a failure condition when resolving topology for this storage policy.
	// +optional
	Error string `json:"error,omitempty"`
}

// InfraStoragePolicyInfoStatus defines the observed state of InfraStoragePolicyInfo.
// +k8s:openapi-gen=true
type InfraStoragePolicyInfoStatus struct {
	// Topology contains observed topology for this storage policy in the cluster.
	// +optional
	Topology *Topology `json:"topology,omitempty"`

	// Error describes a failure condition when observing or reconciling this resource.
	// +optional
	Error string `json:"error,omitempty"`
}

// +genclient:nonNamespaced
// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object

// +k8s:openapi-gen=true
// +kubebuilder:subresource:status
// +kubebuilder:object:root=true
// +kubebuilder:resource:shortName=infraspi,scope=Cluster,path=infrastoragepolicyinfoes

// InfraStoragePolicyInfo is the Schema for the infrastoragepolicyinfoes API.
type InfraStoragePolicyInfo struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Status InfraStoragePolicyInfoStatus `json:"status,omitempty"`
}

// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object

// InfraStoragePolicyInfoList contains a list of InfraStoragePolicyInfo.
type InfraStoragePolicyInfoList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []InfraStoragePolicyInfo `json:"items"`
}
