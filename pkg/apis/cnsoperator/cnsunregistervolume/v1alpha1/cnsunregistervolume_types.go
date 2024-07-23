/*
Copyright 2024 The Kubernetes Authors.

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

// CnsUnregisterVolumeSpec defines the desired state of CnsUnregisterVolume
// +k8s:openapi-gen=true
type CnsUnregisterVolumeSpec struct {
	// Name of the PVC to be unregistered
	PvcName string `json:"pvcName"`
}

// CnsUnregisterVolumeStatus defines the observed state of CnsUnregisterVolume
// +k8s:openapi-gen=true
type CnsUnregisterVolumeStatus struct {
	// Indicates the volume is successfully unregistered.
	// This field must only be set by the entity completing the unregister
	// operation, i.e. the CNS Operator.
	Unregistered bool `json:"unregistered"`

	// The last error encountered during export operation, if any.
	// This field must only be set by the entity completing the export
	// operation, i.e. the CNS Operator.
	Error string `json:"error,omitempty"`
}

// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object

// CnsUnregisterVolume is the Schema for the cnsunregistervolumes API
// +k8s:openapi-gen=true
// +kubebuilder:subresource:status
type CnsUnregisterVolume struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec   CnsUnregisterVolumeSpec   `json:"spec,omitempty"`
	Status CnsUnregisterVolumeStatus `json:"status,omitempty"`
}

// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object

// CnsUnregisterVolumeList contains a list of CnsUnregisterVolume
type CnsUnregisterVolumeList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []CnsUnregisterVolume `json:"items"`
}
