// Copyright (c) 2019 VMware, Inc. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

package v1alpha1

import (
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// VirtualMachineClassHardware describes a virtual hardware resource specification.
type VirtualMachineClassHardware struct {
	Cpus   int64             `json:"cpus,omitempty"`
	Memory resource.Quantity `json:"memory,omitempty"`
}

// VirtualMachineResourceSpec describes a virtual hardware policy specification.
type VirtualMachineResourceSpec struct {
	Cpu    resource.Quantity `json:"cpu,omitempty"`
	Memory resource.Quantity `json:"memory,omitempty"`
}

// VirtualMachineClassResources describes the virtual hardware resource reservations and limits configuration to be used
// by a VirtualMachineClass.
type VirtualMachineClassResources struct {
	Requests VirtualMachineResourceSpec `json:"requests,omitempty"`
	Limits   VirtualMachineResourceSpec `json:"limits,omitempty"`
}

// VirtualMachineClassPolicies describes the policy configuration to be used by a VirtualMachineClass.
type VirtualMachineClassPolicies struct {
	Resources VirtualMachineClassResources `json:"resources,omitempty"`
}

// VirtualMachineClassSpec defines the desired state of VirtualMachineClass
type VirtualMachineClassSpec struct {
	// Hardware describes the configuration of the VirtualMachineClass attributes related to virtual hardware.  The
	// configuration specified in this field is used to customize the virtual hardware characteristics of any VirtualMachine
	// associated with this VirtualMachineClass.
	Hardware VirtualMachineClassHardware `json:"hardware,omitempty"`

	// Policies describes the configuration of the VirtualMachineClass attributes related to virtual infrastructure
	// policy.  The configuration specified in this field is used to customize various policies related to
	// infrastructure resource consumption.
	Policies VirtualMachineClassPolicies `json:"policies,omitempty"`
}

// VirtualMachineClassStatus defines the observed state of VirtualMachineClass.  VirtualMachineClasses are immutable,
// non-dynamic resources, so this status is currently unused.
type VirtualMachineClassStatus struct {
}

// +genclient
// +genclient:nonNamespaced
// +kubebuilder:object:root=true
// +kubebuilder:resource:scope=Cluster,shortName=vmclass
// +kubebuilder:storageversion
// +kubebuilder:subresource:status

// VirtualMachineClass is the Schema for the virtualmachineclasses API.
// A VirtualMachineClass represents the desired specification and the observed status of a VirtualMachineClass
// instance.  A VirtualMachineClass represents a policy and configuration resource which defines a set of attributes to
// be used in the configuration of a VirtualMachine instance.  A VirtualMachine resource references a
// VirtualMachineClass as a required input.
type VirtualMachineClass struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec   VirtualMachineClassSpec   `json:"spec,omitempty"`
	Status VirtualMachineClassStatus `json:"status,omitempty"`
}

// +kubebuilder:object:root=true

// VirtualMachineClassList contains a list of VirtualMachineClass.
type VirtualMachineClassList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []VirtualMachineClass `json:"items"`
}

func init() {
	RegisterTypeWithScheme(&VirtualMachineClass{}, &VirtualMachineClassList{})
}
