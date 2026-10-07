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

// VolumeCapabilities describes the capabilities of volumes created with the given policy.
type VolumeCapabilities struct {
	// SupportsVolumeModeBlock indicates that the policy supports PersistentVolume with Block volume mode.
	SupportsVolumeModeBlock bool `json:"supportsVolumeModeBlock"`

	// SupportsVolumeModeFilesystem indicates that the policy supports PersistentVolume with Filesystem
	// volume mode.
	SupportsVolumeModeFilesystem bool `json:"supportsVolumeModeFilesystem"`

	// SupportsHostLocal indicates that the policy carries the host-local storage capability.
	SupportsHostLocal bool `json:"supportsHostLocal"`

	// ZonesSupportingLinkedClone is the set of zones in the cluster where linked
	// clones are supported (zones having at least one ESXi 9.1+ host that mounts a datastore
	// compatible with the policy). An empty list means no zone supports it.
	// +listType=set
	// +kubebuilder:validation:items:MinLength=1
	ZonesSupportingLinkedClone []string `json:"zonesSupportingLinkedClone"`

	// ZonesSupportingHighPerformanceLinkedClone is the set of zones in the cluster
	// where high-performance linked clones are supported (zones having at least one ESXi 9.1+ host
	// in a vSAN-ESA enabled cluster). An empty list means no zone supports it.
	// +listType=set
	// +kubebuilder:validation:items:MinLength=1
	ZonesSupportingHighPerformanceLinkedClone []string `json:"zonesSupportingHighPerformanceLinkedClone"`
}

// Topology describes topology accessibility for the storage policy within the cluster.
type Topology struct {

	// TopologyType describes the type of topology for the storage policy.
	// The only valid value today is "zonal" (zone-based topology). If the storage
	// policy does not have a zonal topology, this field is omitted.
	// +optional
	// +kubebuilder:validation:Enum=zonal
	TopologyType string `json:"topologyType,omitempty"`

	// AccessibleZones lists zones where the policy is accessible for this cluster.
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

// InfraStoragePolicyInfoSpec defines the desired state of InfraStoragePolicyInfo.
type InfraStoragePolicyInfoSpec struct {
	// ClusterStoragePolicyInfoRef points to the corresponding cluster-scoped ClusterStoragePolicyInfo.
	ClusterStoragePolicyInfoRef ClusterStoragePolicyInfoReference `json:"clusterStoragePolicyInfoRef"`
}

// InfraStoragePolicyInfoStatus defines the observed state of InfraStoragePolicyInfo.
// +k8s:openapi-gen=true
type InfraStoragePolicyInfoStatus struct {
	// Topology contains observed topology for this storage policy in the cluster.
	// +optional
	Topology *Topology `json:"topology,omitempty"`

	// VolumeCapabilities describes the supported volume capabilities.
	// +optional
	VolumeCapabilities *VolumeCapabilities `json:"volumeCapabilities,omitempty"`

	// Error describes a failure condition when observing or reconciling this resource.
	// +optional
	Error string `json:"error,omitempty"`
}

// +genclient:nonNamespaced
// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object

// +k8s:openapi-gen=true
// +kubebuilder:subresource:status
// +kubebuilder:object:root=true
// +kubebuilder:resource:shortName=infraspi,scope=Cluster,path=infrastoragepolicyinfos

// InfraStoragePolicyInfo is the Schema for the infrastoragepolicyinfos API.
// Name of this CR is same as the unique and immutable K8sCompliantName of the storage policy.
type InfraStoragePolicyInfo struct {
	Spec              InfraStoragePolicyInfoSpec `json:"spec,omitempty"`
	metav1.TypeMeta   `json:",inline"`
	Status            InfraStoragePolicyInfoStatus `json:"status,omitempty"`
	metav1.ObjectMeta `json:"metadata,omitempty"`
}

// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object

// +kubebuilder:object:root=true
// InfraStoragePolicyInfoList contains a list of InfraStoragePolicyInfo.
type InfraStoragePolicyInfoList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []InfraStoragePolicyInfo `json:"items"`
}
