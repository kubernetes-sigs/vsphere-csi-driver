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

const (
	// CnsDeletedVolumeInfoKind is the kind of the CnsDeletedVolumeInfo CRD.
	CnsDeletedVolumeInfoKind = "CnsDeletedVolumeInfo"

	// CnsDeletedVolumeInfoPlural is plural of CnsDeletedVolumeInfo.
	CnsDeletedVolumeInfoPlural = "cnsdeletedvolumeinfoes"

	// CnsDeletedVolumeInfoSingular is singular of CnsDeletedVolumeInfo.
	CnsDeletedVolumeInfoSingular = "cnsdeletedvolumeinfo"
)

// CnsDeletedVolumeInfoSpec records a volume that was deleted while SnapService still holds its
// snapshots. The Data Protection Operator creates, sizes and deletes it; CSI only reads it.
type CnsDeletedVolumeInfoSpec struct {
	// VolumeID is the FCD ID of the deleted volume.
	// +kubebuilder:validation:MinLength=1
	VolumeID string `json:"volumeID"`

	// Namespace is the tenant namespace charged for the snapshots of the deleted volume.
	// +kubebuilder:validation:MinLength=1
	Namespace string `json:"namespace"`

	// StorageClassName is the storage class the deleted volume was charged to.
	// +kubebuilder:validation:MinLength=1
	StorageClassName string `json:"storageClassName"`

	// VolumeAttributeClassName is the volume attributes class the deleted volume was charged to, if any.
	VolumeAttributeClassName string `json:"volumeAttributeClassName,omitempty"`

	// StoragePolicyID is the ID of the storage policy of the deleted volume.
	// +kubebuilder:validation:MinLength=1
	StoragePolicyID string `json:"storagePolicyID"`

	// SizeBytes is the snapshot size, in bytes, still held for the deleted volume.
	// +kubebuilder:validation:Minimum=0
	SizeBytes int64 `json:"sizeBytes"`
}

//+kubebuilder:object:root=true

// CnsDeletedVolumeInfo is the Schema for the cnsdeletedvolumeinfoes API.
type CnsDeletedVolumeInfo struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec CnsDeletedVolumeInfoSpec `json:"spec"`
}

//+kubebuilder:object:root=true

// CnsDeletedVolumeInfoList contains a list of CnsDeletedVolumeInfo.
type CnsDeletedVolumeInfoList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []CnsDeletedVolumeInfo `json:"items"`
}
