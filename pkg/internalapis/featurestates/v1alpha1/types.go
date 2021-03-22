/*
Copyright 2021 The Kubernetes Authors.

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

// CnsCsiSvFeatureStatesSpec defines the desired state of CnsCsiSvFeatureStates
type CnsCsiSvFeatureStatesSpec struct {
	FeatureStates []FeatureState `json:"featureStates,omitempty"`
}

// FeatureState defines the feature name and its state
type FeatureState struct {
	// Name is the unique identifier of the feature, this must be unique
	Name string `json:"name"`
	// Enabled is set to true when feature is enabled, else it is set to false
	Enabled bool `json:"enabled"`
}

// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object

// CnsCsiSvFeatureStates is the Schema for the cnscsisvfeaturestates API
// +kubebuilder:subresource:status
// +kubebuilder:resource:path=cnscsisvfeaturestates,scope=Namespaced
type CnsCsiSvFeatureStates struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec CnsCsiSvFeatureStatesSpec `json:"spec,omitempty"`
}

// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object

// CnsCsiSvFeatureStatesList contains a list of CnsCsiSvFeatureStates
type CnsCsiSvFeatureStatesList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []CnsCsiSvFeatureStates `json:"items"`
}
