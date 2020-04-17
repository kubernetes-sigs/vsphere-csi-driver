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
	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// CnsRegisterVolumeSpec defines the desired state of CnsRegisterVolume
// +k8s:openapi-gen=true
type CnsRegisterVolumeSpec struct {
	// Name of the PVC
	PvcName string `json:"pvcName"`

	// VolumeID indicates an existing vsphere volume to be imported into Project Pacific cluster
	// If the AccessMode is "ReadWriteMany" or "ReadOnlyMany", then this VolumeID can be either an existing FileShare (or) CNS file volume backed FileShare.
	// If the AccessMode is "ReadWriteOnce", then this VolumeID can be either an existing FCD (or) a CNS backed FCD.
	// VolumeID and DiskUrlPath cannot be specified together.
	VolumeID string `json:"volumeID,omitempty"`

	// AccessMode contains the actual access mode the volume backing the CnsRegisterVolume has.
	// AccessMode must be specified if VolumeID is specified.
	AccessMode v1.PersistentVolumeAccessMode `json:"accessMode,omitempty"`

	// DiskUrlPath is URL path to an existing block volume to be imported into Project Pacific cluster.
	// VolumeID and DiskUrlPath cannot be specified together.
	// DiskUrlPath is explicitly used for block volumes. AccessMode need not be specified and will be defaulted to "ReadWriteOnce".
	// This field must be in the following format:
	// Format:
	// https://<vc_ip>/folder/<vm_vmdk_path>?dcPath=<datacenterName>&dsName=<datastoreName>
	// Ex: https://10.192.255.221/folder/34a9c05d-5f03-e254-e692-02004479cb91/vm2_1.vmdk?dcPath=Datacenter-1&dsName=vsanDatastore
	// This is for a 34a9c05d-5f03-e254-e692-02004479cb91/vm2_1.vmdk
	// file under datacenter "Datacenter-1" and datastore "vsanDatastore".
	DiskURLPath string `json:"diskURLPath,omitempty"`
}

// CnsRegisterVolumeStatus defines the observed state of CnsRegisterVolume
// +k8s:openapi-gen=true
type CnsRegisterVolumeStatus struct {
	// Indicates the volume is successfully registered.
	// This field must only be set by the entity completing the register
	// operation, i.e. the CNS Operator.
	Registered bool `json:"registered"`

	// The last error encountered during import operation, if any.
	// This field must only be set by the entity completing the import
	// operation, i.e. the CNS Operator.
	Error string `json:"error,omitempty"`
}

// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object

// CnsRegisterVolume is the Schema for the cnsregistervolumes API
// +k8s:openapi-gen=true
// +kubebuilder:subresource:status
type CnsRegisterVolume struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec   CnsRegisterVolumeSpec   `json:"spec,omitempty"`
	Status CnsRegisterVolumeStatus `json:"status,omitempty"`
}

// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object

// CnsRegisterVolumeList contains a list of CnsRegisterVolume
type CnsRegisterVolumeList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []CnsRegisterVolume `json:"items"`
}
