/*
Copyright 2025 The Kubernetes Authors.

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

package v1alpha2

import (
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// StoragePolicyReservationSpec defines the desired state of StoragePolicyReservation
type StoragePolicyReservationSpec struct {
	// +kubebuilder:validation:Required
	Requested []Requested `json:"requested"`
}

// Requested defines a storage reservation request for a given object
type Requested struct {
	// APIGroup is the group for the resource being referenced.
	// If APIGroup is not specified, the specified Kind must be in the core API group.
	// For any other third-party types, APIGroup is required.
	// +optional
	APIGroup *string `json:"apiGroup,omitempty"`
	// Kind of the requested storage object.
	// More info: https://git.k8s.io/community/contributors/devel/sig-architecture/api-conventions.md#types-kinds
	Kind string `json:"kind"`
	// Name of the requested storage object such as a VM service VM API object.
	// More info: https://kubernetes.io/docs/concepts/overview/working-with-objects/names/#names
	Name string `json:"name"`
	// +listType=map
	// +listMapKey=storageClassName
	// ReservationRequests define request for storage quota associated with a StorageClass
	ReservationRequests []ReservationRequest `json:"reservationRequests"`
}

// ReservationRequest defines request for storage quota associated with a StorageClass
type ReservationRequest struct {
	// Name of the Kubernetes StorageClass
	StorageClassName string `json:"storageClassName"`
	// Requested capacity for a storage resource to be provisioned at a later time
	Request *resource.Quantity `json:"request,omitempty"`
}

// StoragePolicyReservationStatus defines the observed state of StoragePolicyReservation
type StoragePolicyReservationStatus struct {
	Approved []StorageObject `json:"approved,omitempty"`
	Denied   []StorageObject `json:"denied,omitempty"`
}

// StorageObject defines a storage reservation for a given object
type StorageObject struct {
	// APIGroup is the group for the resource being referenced.
	// If APIGroup is not specified, the specified Kind must be in the core API group.
	// For any other third-party types, APIGroup is required.
	// +optional
	APIGroup *string `json:"apiGroup,omitempty"`
	// Kind of the requested storage object.
	// More info: https://git.k8s.io/community/contributors/devel/sig-architecture/api-conventions.md#types-kinds
	Kind string `json:"kind"`
	// Name of the requested storage object.
	// More info: https://kubernetes.io/docs/concepts/overview/working-with-objects/names/#names
	Name string `json:"name"`
	// Name of the Kubernetes StorageClass
	StorageClassName string `json:"storageClassName"`
	// Requested capacity for a storage resource being provisioned
	Request *resource.Quantity `json:"request,omitempty"`
	// Indicates the reason why the reservation for an object is being denied
	// +optional
	Reason *string `json:"reason,omitempty"`
}

//+kubebuilder:object:root=true
//+kubebuilder:subresource:status
//+kubebuilder:storageversion

// StoragePolicyReservation is the Schema for the storagepolicyreservations API
type StoragePolicyReservation struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec   StoragePolicyReservationSpec   `json:"spec"`
	Status StoragePolicyReservationStatus `json:"status,omitempty"`
}

//+kubebuilder:object:root=true

// StoragePolicyReservationList contains a list of StoragePolicyReservation
type StoragePolicyReservationList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []StoragePolicyReservation `json:"items"`
}
