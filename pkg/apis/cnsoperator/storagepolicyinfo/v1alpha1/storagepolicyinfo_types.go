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

// StoragePolicyInfoSpec defines the desired state of StoragePolicyInfo
// +k8s:openapi-gen=true
type StoragePolicyInfoSpec struct {
	// StoragePolicyID is the vSphere storage policy ID
	// +required
	StoragePolicyID string `json:"storagePolicyID"`

	// K8sCompliantName is the k8sCompliantName that the VI admin chose while creating the policy
	// +required
	K8sCompliantName string `json:"k8sCompliantName"`
}

// StoragePolicyInfoStatus defines the observed state of StoragePolicyInfo
// +k8s:openapi-gen=true
type StoragePolicyInfoStatus struct {
	// StoragePolicyDeleted indicates whether the underlying storagepolicy is deleted or not on the VC.
	// +optional
	StoragePolicyDeleted bool `json:"storagePolicyDeleted,omitempty"`

	// StorageTypeInfo describes the underlying storage type
	// +optional
	StorageTypeInfo *[]StorageTypeInfo `json:"storageTypeInfo,omitempty"`

	// VolumeTypeInfo describes supported volume types and access modes
	// +optional
	VolumeTypeInfo *VolumeTypeInfo `json:"volumeTypeInfo,omitempty"`

	// TopologyInfo describes the topology characteristics of the storage
	// +optional
	TopologyInfo *TopologyInfo `json:"topologyInfo,omitempty"`

	// EncryptionInfo describes encryption capabilities
	// +optional
	EncryptionInfo *EncryptionInfo `json:"encryptionInfo,omitempty"`

	// PerformanceInfo describes performance characteristics
	// +optional
	PerformanceInfo *PerformanceInfo `json:"performanceInfo,omitempty"`

	// Error describes the error encountered while retrieving the storage policy info
	// +optional
	Error string `json:"error,omitempty"`
}

// StorageTypeInfo describes the underlying storage type
type StorageTypeInfo struct {
	// DatastoreType indicates the type of datastore
	// Valid values: "vsan", "vmfs", "nfs", "vvol"
	// +required
	// +kubebuilder:validation:Enum=vsan;vmfs;nfs;vvol
	DatastoreType string `json:"datastoreType"`

	// VsanArchitecture indicates the vSAN architecture (only for vsan datastoreType)
	// Valid values: "esa", "osa"
	// +optional
	// +kubebuilder:validation:Enum=esa;osa
	VsanArchitecture string `json:"vsanArchitecture,omitempty"`

	// Version is the version of the datastore.
	// +required
	Version string `json:"version"`
}

// VolumeTypeInfo describes supported volume types and access modes
type VolumeTypeInfo struct {
	// SupportedBlockVolumeModes gives the supported combinations of access modes and volume modes for a block volume.
	// Valid values: "RWO_FileSystem", "RWX_Block"
	// +kubebuilder:validation:Enum=RWO_FileSystem;RWX_Block
	// +optional
	SupportedBlockVolumeModes []string `json:"supportedBlockVolumeModes,omitempty"`

	// SupportedFileVolumeModes gives the supported combinations of access modes and volume modes for a file volume.
	// Valid value: "RWX_FileSystem"
	// +kubebuilder:validation:Enum=RWX_FileSystem
	// +optional
	SupportedFileVolumeModes []string `json:"supportedFileVolumeModes,omitempty"`
}

// TopologyInfo describes the topology characteristics of the storage
type TopologyInfo struct {
	// Type indicates whether the storage is zonal or cross-zonal
	// Valid values: "zonal", "non-zonal"
	// +required
	// +kubebuilder:validation:Enum=zonal;non-zonal
	Type string `json:"type"`

	// AccessibleZones lists the zones where this storage class can be accessed
	// +optional
	AccessibleZones []string `json:"accessibleZones,omitempty"`
}

// EncryptionInfo describes encryption capabilities
type EncryptionInfo struct {
	// Supported indicates whether encryption is supported or not.
	// +required
	Supported bool `json:"supported"`

	// EncryptionType indicates the type of encryption
	// Valid values: "vm-crypt", "storage-level", "none"
	// +optional
	// +kubebuilder:validation:Enum=vm-encryption;vsan-encryption
	EncryptionType []string `json:"encryptionType,omitempty"`
}

// PerformanceInfo describes performance characteristics (vSAN only)
type PerformanceInfo struct {
	// IopsLimit is the IOPS limit for volumes created from this storage class (vSAN ESA only)
	// +required
	IopsLimit *int64 `json:"iopsLimit"`
}

// +genclient
// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object

// +k8s:openapi-gen=true
// +kubebuilder:subresource:status
// +kubebuilder:resource:shortName=spi
// +kubebuilder:object:root=true

// StoragePolicyInfo is the Schema for the storagepolicyinfoes API
type StoragePolicyInfo struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec   StoragePolicyInfoSpec   `json:"spec,omitempty"`
	Status StoragePolicyInfoStatus `json:"status,omitempty"`
}

// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object

// StoragePolicyInfoList contains a list of StoragePolicyInfo
type StoragePolicyInfoList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []StoragePolicyInfo `json:"items"`
}
