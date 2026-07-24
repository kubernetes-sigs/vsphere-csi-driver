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
//   - SupportsHostLocal: the policy is a host-local storage policy.
//
// +kubebuilder:validation:Enum=SupportsPersistentVolumeBlock;SupportsPersistentVolumeFilesystem;SupportsHighPerformanceLinkedClone;SupportsLinkedClone;SupportsHostLocal
type VolumeCapability string

const (
	// SupportsVolumeModeBlock indicates that the policy supports PersistentVolume with Block volume mode.
	SupportsVolumeModeBlock VolumeCapability = "SupportsPersistentVolumeBlock"
	// SupportsVolumeModeFilesystem indicates that the policy supports PersistentVolume with Filesystem volume mode.
	SupportsVolumeModeFilesystem VolumeCapability = "SupportsPersistentVolumeFilesystem"
	// SupportsHighPerformanceLinkedClone indicates that the policy supports high-performance linked clones
	// on vSAN ESA clusters with ESXi 9.1 or above hosts.
	SupportsHighPerformanceLinkedClone VolumeCapability = "SupportsHighPerformanceLinkedClone"
	// SupportsLinkedClone indicates that the policy supports linked clones with at least one ESXi 9.1+ host.
	SupportsLinkedClone VolumeCapability = "SupportsLinkedClone"
	// SupportsHostLocal indicates that the policy carries the host-local storage capability.
	SupportsHostLocal VolumeCapability = "SupportsHostLocal"
)

// Topology describes topology accessibility for the storage policy within a namespace.
type Topology struct {
	// TopologyType describes the type of topology for the storage policy.
	// Valid values are empty string (no topology) or "zonal" (zone-based topology).
	// +kubebuilder:validation:Enum="";zonal
	TopologyType string `json:"topologyType"`

	// AccessibleZones lists zones where the policy is accessible for this namespace.
	// For a marker policy (e.g. vSAN File Service), zones are shown even if they are
	// not assigned to the namespace.
	// +listType=set
	// +kubebuilder:validation:items:MinLength=1
	AccessibleZones []string `json:"accessibleZones"`
}

// ClusterStoragePolicyInfoReference identifies the cluster-scoped ClusterStoragePolicyInfo
// that carries the non-topology attributes (encryption, performance, volume capabilities)
// for the same storage policy.
type ClusterStoragePolicyInfoReference struct {
	// Name is the name of the referenced ClusterStoragePolicyInfo. It is always equal to
	// this object's own name, since both share the same K8sCompliantName of the storage policy.
	Name string `json:"name"`

	// Kind is the kind of the referenced object, i.e. "ClusterStoragePolicyInfo".
	Kind string `json:"kind"`

	// APIGroup is the API group and version of the referenced object, i.e. "cns.vmware.com/v1alpha1".
	APIGroup string `json:"apiGroup"`
}

// StoragePolicyInfoSpec defines the desired state of StoragePolicyInfo.
type StoragePolicyInfoSpec struct {
	// ClusterStoragePolicyInfoRef points to the corresponding cluster-scoped ClusterStoragePolicyInfo.
	ClusterStoragePolicyInfoRef ClusterStoragePolicyInfoReference `json:"clusterStoragePolicyInfoRef"`
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
	Spec              StoragePolicyInfoSpec `json:"spec,omitempty"`
	metav1.TypeMeta   `json:",inline"`
	Status            StoragePolicyInfoStatus `json:"status,omitempty"`
	metav1.ObjectMeta `json:"metadata,omitempty"`
}

// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object

// +kubebuilder:object:root=true

// StoragePolicyInfoList contains a list of StoragePolicyInfo.
type StoragePolicyInfoList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []StoragePolicyInfo `json:"items"`
}
