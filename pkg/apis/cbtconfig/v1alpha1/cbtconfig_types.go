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
	"k8s.io/apimachinery/pkg/runtime"
)

var (
	// SchemeBuilder is used to add go types to the GroupVersionKind scheme.
	SchemeBuilder = runtime.NewSchemeBuilder(addKnownTypes)

	// AddToScheme adds the types in this group-version to the given scheme.
	AddToScheme = SchemeBuilder.AddToScheme
)

// addKnownTypes adds the set of types defined in this package to the supplied scheme.
func addKnownTypes(scheme *runtime.Scheme) error {
	scheme.AddKnownTypes(GroupVersion,
		&CBTConfig{},
		&CBTConfigList{},
	)
	metav1.AddToGroupVersion(scheme, GroupVersion)
	return nil
}

// CBTState is a string enum that represents whether Change Block Tracking
// is enabled or not.
// +kubebuilder:validation:Enum=Active;Inactive
type CBTState string

const (
	// CBTStateActive requests that Change Block Tracking be turned on.
	CBTStateActive CBTState = "Active"
	// CBTStateInactive requests that Change Block Tracking be turned off.
	CBTStateInactive CBTState = "Inactive"

	// CBTConfigConditionConfigured is True when every VirtualMachine in the
	// namespace has its CBT flag aligned with Spec.State.
	CBTConfigConditionConfigured = "Configured"

	// Condition reasons for CBTConfig.
	// Pattern: {CRPrefix}Reason{ConditionType}{Success|Error|Semantic}.

	CBTConfigReasonConfiguredInProgress = "InProgress"
	CBTConfigReasonConfiguredSuccess    = "Succeeded"
	CBTConfigReasonConfiguredFailed     = "Failed"
)

// CBTConfigSpec defines the desired state of CBTConfig.
type CBTConfigSpec struct {
	// state requests that Change Block Tracking be turned on or off
	// in this namespace.
	// +required
	State CBTState `json:"state,omitempty"`
}

// CBTConfigStatus defines the observed state of CBTConfig.
type CBTConfigStatus struct {
	// conditions describes the observed conditions of the CBTConfig.
	// +optional
	// +listType=map
	// +listMapKey=type
	// +kubebuilder:validation:MaxItems=32
	Conditions []metav1.Condition `json:"conditions,omitempty"`

	// observedGeneration is the metadata.generation of the CBTConfig that the
	// controller last finished reconciling on the success path.
	// +optional
	ObservedGeneration *int64 `json:"observedGeneration,omitempty"`

	// state reflects the current CBT state of this namespace.
	// +optional
	State *CBTState `json:"state,omitempty"`
}

// +kubebuilder:object:root=true
// +kubebuilder:subresource:status
// +kubebuilder:resource:scope=Namespaced

// CBTConfig is the Schema for the namespace level CBT enablement API.
type CBTConfig struct {
	metav1.TypeMeta `json:",inline"`

	// metadata is a standard object metadata.
	// +optional
	metav1.ObjectMeta `json:"metadata,omitempty"`

	// spec defines the desired state of CBTConfig.
	// +required
	Spec CBTConfigSpec `json:"spec,omitzero"`

	// status defines the observed state of CBTConfig.
	// +optional
	Status *CBTConfigStatus `json:"status,omitempty"`
}

func (c *CBTConfig) GetConditions() []metav1.Condition {
	if c.Status == nil {
		return nil
	}
	return c.Status.Conditions
}

func (c *CBTConfig) SetConditions(conditions []metav1.Condition) {
	if c.Status == nil {
		c.Status = &CBTConfigStatus{}
	}
	c.Status.Conditions = conditions
}

// +kubebuilder:object:root=true

// CBTConfigList contains a list of CBTConfig
type CBTConfigList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []CBTConfig `json:"items"`
}
