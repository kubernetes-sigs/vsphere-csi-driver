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
// +kubebuilder:validation:Enum=PersistentVolumeBlock;PersistentVolumeFilesystem;HighPerformanceLinkedClone
type VolumeCapability string

const (
	// VolumeCapabilityPersistentVolumeBlock indicates block volumes are supported.
	VolumeCapabilityPersistentVolumeBlock VolumeCapability = "PersistentVolumeBlock"
	// VolumeCapabilityPersistentVolumeFilesystem indicates file volumes are supported.
	VolumeCapabilityPersistentVolumeFilesystem VolumeCapability = "PersistentVolumeFilesystem"
	// VolumeCapabilityHighPerformanceLinkedClone indicates linked-clone / fast provisioning style volumes.
	VolumeCapabilityHighPerformanceLinkedClone VolumeCapability = "HighPerformanceLinkedClone"
)

// ClusterStoragePolicyInfoSpec defines the desired state of ClusterStoragePolicyInfo.
// It is used to store the information about the storage policy that is exposed to Devops users.
// +k8s:openapi-gen=true
type ClusterStoragePolicyInfoSpec struct {
	// K8sCompliantName is the Kubernetes-compliant name the VI admin chose when creating the policy.
	// +required
	K8sCompliantName string `json:"k8sCompliantName"`
}

// ClusterStoragePolicyInfoStatus defines the observed state of ClusterStoragePolicyInfo.
// +k8s:openapi-gen=true
type ClusterStoragePolicyInfoStatus struct {
	// StoragePolicyDeleted indicates whether the underlying storagepolicy is deleted or not on the VC.
	// +optional
	StoragePolicyDeleted bool `json:"storagePolicyDeleted,omitempty"`

	// VolumeCapabilities describes the supported volume capabilities.
	// +optional
	VolumeCapabilities []VolumeCapability `json:"volumeCapabilities,omitempty"`

	// Performance describes performance characteristics (vSAN only).
	// +optional
	Performance *Performance `json:"performance,omitempty"`

	// Encryption describes encryption-related status for the storage policy.
	// +optional
	Encryption *Encryption `json:"encryption,omitempty"`

	// Error describes the error encountered while retrieving the storage policy info.
	// +optional
	Error string `json:"error,omitempty"`
}

// Encryption describes encryption capabilities.
type Encryption struct {
	// SupportsEncryption indicates whether the storage policy supports encryption.
	// +optional
	SupportsEncryption bool `json:"supportsEncryption,omitempty"`

	// EncryptionType indicates the type of encryption.
	// +optional
	// +kubebuilder:validation:items:Enum=vm-encryption;vsan-encryption
	EncryptionType []string `json:"encryptionType,omitempty"`

	// Error describes the error encountered while retrieving the encryption info.
	// +optional
	Error string `json:"error,omitempty"`
}

// Performance describes performance characteristics (vSAN only).
type Performance struct {
	// IopsLimit is the IOPS limit for volumes created from this storage class (vSAN ESA only).
	// +optional
	IopsLimit *int64 `json:"iopsLimit"`

	// Error describes the error encountered while retrieving the performance info.
	// +optional
	Error string `json:"error,omitempty"`
}

// +genclient:nonNamespaced
// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object

// +k8s:openapi-gen=true
// +kubebuilder:subresource:status
// +kubebuilder:resource:path=clusterstoragepolicyinfoes,scope=Cluster,shortName=clusterspi
// +kubebuilder:object:root=true

// ClusterStoragePolicyInfo is the Schema for the clusterstoragepolicyinfoes API.
type ClusterStoragePolicyInfo struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec   ClusterStoragePolicyInfoSpec   `json:"spec,omitempty"`
	Status ClusterStoragePolicyInfoStatus `json:"status,omitempty"`
}

// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object

// ClusterStoragePolicyInfoList contains a list of ClusterStoragePolicyInfo.
type ClusterStoragePolicyInfoList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []ClusterStoragePolicyInfo `json:"items"`
}
