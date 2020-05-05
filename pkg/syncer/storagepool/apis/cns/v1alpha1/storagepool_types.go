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

package v1alpha1

import (
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// StoragePoolSpec defines the desired state of StoragePool
type StoragePoolSpec struct {
	// Name of the driver
	Driver string `json:"driver"`

	// Opaque parameters describing attributes of the storage pool
	// +optional
	Parameters map[string]string `json:"parameters,omitempty"`
}

// StoragePoolStatus defines the observed state of StoragePool
type StoragePoolStatus struct {
	// Nodes the storage pool has access to
	// +optional
	AccessibleNodes []string `json:"accessibleNodes,omitempty"`
	// StorageClasses that can be used with this storage pool
	// +optional
	CompatibleStorageClasses []string `json:"compatibleStorageClasses,omitempty"`
	// Total Capacity of the storage pool
	// +optional
	Capacity *PoolCapacity `json:"capacity,omitempty"`
	// Last errors happened on the pool
	// +optional
	Errors []StoragePoolError `json:"errors,omitempty"`
}

// PoolCapacity is the storage capacity of the storage pool
type PoolCapacity struct {
	// Total capacity of the storage pool
	// +optional
	Total *resource.Quantity `json:"total,omitempty"`
	// Free Space of the storage pool
	// +optional
	FreeSpace *resource.Quantity `json:"freeSpace,omitempty"`
}

// StoragePoolError describes an error encountered on the pool
type StoragePoolError struct {
	// Time is the timestamp when the error was encountered.
	// +optional
	Time *metav1.Time `json:"time,omitempty"`

	// Message details the encountered error
	// +optional
	Message *string `json:"message,omitempty"`
}

// +genclient:nonNamespaced
// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object

// StoragePool is the Schema for the storagepools API
// +k8s:openapi-gen=true
type StoragePool struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec   StoragePoolSpec   `json:"spec,omitempty"`
	Status StoragePoolStatus `json:"status,omitempty"`
}

// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object

// StoragePoolList contains a list of StoragePool
type StoragePoolList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []StoragePool `json:"items"`
}

func init() {
	SchemeBuilder.Register(&StoragePool{}, &StoragePoolList{})
}
