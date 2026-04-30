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

// CBTConfigSpec defines the desired state of CBTConfig
type CBTConfigSpec struct {
	// Cbt is requested to be set true/false in the namespace.
	Enabled bool `json:"enabled"`
}

// CBTConfigStatus defines the observed state of CBTConfig.
type CBTConfigStatus struct {
	// The cbt status of the namespace.
	Enabled *bool `json:"enabled,omitempty"`

	// The last error encountered from the last reconcile, if any.
	Error string `json:"error,omitempty"`
}

// +kubebuilder:object:root=true
// +kubebuilder:subresource:status
// +kubebuilder:resource:scope=Namespaced

// CBTConfig is the Schema for the namespace level CBT enablement API.
type CBTConfig struct {
	metav1.TypeMeta `json:",inline"`

	// metadata is a standard object metadata
	// +optional
	metav1.ObjectMeta `json:"metadata,omitempty"`

	// spec defines the desired state of CBTConfig
	// +required
	Spec CBTConfigSpec `json:"spec"`

	// status defines the observed state of CBTConfig
	// +optional
	Status CBTConfigStatus `json:"status,omitempty"`
}

// +kubebuilder:object:root=true

// CBTConfigList contains a list of CBTConfig
type CBTConfigList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []CBTConfig `json:"items"`
}
