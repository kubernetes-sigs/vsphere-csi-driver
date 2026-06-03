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

// VolumeCapability describes capabilities of the volume created with the given policy.
// The supported capabilities are:
//   - SupportsPersistentVolumeBlock: Volume Mode Block is supported.
//   - SupportsPersistentVolumeFilesystem: Volume Mode Filesystem is supported.
//   - SupportsHighPerformanceLinkedClone: LinkedClone on vSAN ESA is supported.
//   - SupportsLinkedClone: LinkedClone is supported.
//
// +kubebuilder:validation:Enum=SupportsPersistentVolumeBlock;SupportsPersistentVolumeFilesystem;SupportsHighPerformanceLinkedClone;SupportsLinkedClone
type VolumeCapability string

// Topology describes topology accessibility for the storage policy within a namespace.
type Topology struct {
	// TopologyType describes the type of topology for the storage policy.
	// Valid values are empty string (no topology) or "zonal" (zone-based topology).
	// +kubebuilder:validation:Enum="";zonal
	// +optional
	TopologyType string `json:"topologyType"`

	// AccessibleZones lists zones where the policy is accessible for this namespace.
	// For a marker policy (e.g. vSAN File Service), zones are shown even if they are
	// not assigned to the namespace.
	// +listType=set
	// +kubebuilder:validation:items:MinLength=1
	// +optional
	AccessibleZones []string `json:"accessibleZones,omitempty"`
}

// StoragePolicyInfoStatus defines the observed state of StoragePolicyInfo.
// +k8s:openapi-gen=true
type StoragePolicyInfoStatus struct {
	// TopologyInfo contains observed topology for this storage policy filtered
	// to the zones accessible within this namespace.
	// +optional
	TopologyInfo *Topology `json:"topologyInfo,omitempty"`

	// VolumeCapabilities describes the supported volume capabilities.
	// +optional
	VolumeCapabilities map[VolumeCapability]bool `json:"volumeCapabilities,omitempty"`

	// Error describes a failure condition when resolving topology for this
	// storage policy. Empty string indicates no error.
	// +optional
	Error string `json:"error,omitempty"`
}

// +genclient
// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object

// +k8s:openapi-gen=true
// +kubebuilder:subresource:status
// +kubebuilder:object:root=true
// +kubebuilder:resource:scope=Namespaced,shortName=spi,path=storagepolicyinfos

// StoragePolicyInfo is the Schema for the storagepolicyinfos API.
// Name of this CR is the same as the unique and immutable K8sCompliantName of the
// storage policy. One instance is created per namespace per storage policy that is
// assigned to that namespace.
type StoragePolicyInfo struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Status StoragePolicyInfoStatus `json:"status,omitempty"`
}

// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object

// +kubebuilder:object:root=true

// StoragePolicyInfoList contains a list of StoragePolicyInfo.
type StoragePolicyInfoList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []StoragePolicyInfo `json:"items"`
}
