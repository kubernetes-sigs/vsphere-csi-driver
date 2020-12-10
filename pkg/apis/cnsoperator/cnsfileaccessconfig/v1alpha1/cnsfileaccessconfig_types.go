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
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// CnsFileAccessConfigSpec defines the desired state of CnsFileAccessConfig
// +k8s:openapi-gen=true
type CnsFileAccessConfigSpec struct {
	// PvcName indicates the name of the PVC on the supervisor Cluster.
	// This is guaranteed to be unique in Supervisor cluster.
	PvcName string `json:"pvcName"`

	// VmName is the name of VirtualMachine instance on SV cluster
	// Support for PodVm will be added in the near future and
	// either VMName or PodVMName needs to be set.
	VMName string `json:"vmName,omitempty"`
}

// CnsFileAccessConfigStatus defines the observed state of CnsFileAccessConfig
// +k8s:openapi-gen=true
type CnsFileAccessConfigStatus struct {
	// Done indicates whether the ACL has been
	// configured on file volume. This field must only be set by
	// the entity completing the register operation, i.e. the CNS Operator.
	Done bool `json:"done,omitempty"`

	// Access points per protocol supported by the volume.
	// This field must only be set by the entity completing the config
	// operation, i.e. the CNS Operator.
	// AccessPoints field will only be set when the CnsFileAccessConfig.Status.Done
	// field is set to true.
	AccessPoints map[string]string `json:"accessPoints,omitempty"`

	// The last error encountered during file volume config operation, if any
	// This field must only be set by the entity completing the config
	// operation, i.e. the CNS Operator.
	Error string `json:"error,omitempty"`
}

// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object

// CnsFileAccessConfig is the Schema for the CnsFileAccessConfig API
// +k8s:openapi-gen=true
// +kubebuilder:subresource:status
type CnsFileAccessConfig struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec   CnsFileAccessConfigSpec   `json:"spec,omitempty"`
	Status CnsFileAccessConfigStatus `json:"status,omitempty"`
}

// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object

// CnsFileAccessConfigList contains a list of CnsFileAccessConfig
type CnsFileAccessConfigList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []CnsFileAccessConfig `json:"items"`
}
