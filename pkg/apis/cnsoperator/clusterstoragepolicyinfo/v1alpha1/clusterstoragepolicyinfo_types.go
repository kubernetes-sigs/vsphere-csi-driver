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

// EncryptionType describes the type of encryption supported by the storage policy.
// +kubebuilder:validation:Enum=vm-encryption;vsan-encryption
type EncryptionType string

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

	// Error describes a failure observed while reconciling the ClusterStoragePolicyInfo resource.
	// Empty string indicates no error.
	// +optional
	Error string `json:"error,omitempty"`
}

// Encryption describes encryption capabilities.
type Encryption struct {
	// SupportsEncryption indicates whether the storage policy supports encryption.
	// +optional
	SupportsEncryption bool `json:"supportsEncryption,omitempty"`

	// EncryptionTypes indicates the types of encryption.
	// +optional
	EncryptionTypes []EncryptionType `json:"encryptionTypes,omitempty"`
}

// Performance describes performance characteristics (vSAN only).
type Performance struct {
	// IopsLimit is the IOPS limit for volumes created from this storage policy.
	// +optional
	IopsLimit *int64 `json:"iopsLimit,omitempty"`
}

// +genclient:nonNamespaced
// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object

// +k8s:openapi-gen=true
// +kubebuilder:subresource:status
// +kubebuilder:resource:path=clusterstoragepolicyinfos,scope=Cluster,shortName=clusterspi
// +kubebuilder:object:root=true

// ClusterStoragePolicyInfo is the Schema for the clusterstoragepolicyinfos API.
// Name of this CR is same as the unique and immutable K8sCompliantName of the storage policy.
type ClusterStoragePolicyInfo struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Status ClusterStoragePolicyInfoStatus `json:"status,omitempty"`
}

// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object
// +kubebuilder:object:root=true

// ClusterStoragePolicyInfoList contains a list of ClusterStoragePolicyInfo.
type ClusterStoragePolicyInfoList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []ClusterStoragePolicyInfo `json:"items"`
}
