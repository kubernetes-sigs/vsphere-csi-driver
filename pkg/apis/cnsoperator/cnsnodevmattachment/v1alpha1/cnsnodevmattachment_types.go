/*
Copyright 2019 The Kubernetes Authors.

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
	// AttributeFirstClassDiskUUID is the SCSI Disk Identifier
	AttributeFirstClassDiskUUID = "diskUUID"

	// AttributeCnsVolumeID represents the volume ID in CNS.
	AttributeCnsVolumeID = "cnsVolumeId"
)

// CnsNodeVmAttachmentSpec defines the desired state of CnsNodeVmAttachment
// +k8s:openapi-gen=true
type CnsNodeVmAttachmentSpec struct {
	// NodeUUID indicates the UUID of the node where the volume needs to be attached to.
	// Here NodeUUID is the bios UUID of the node.
	NodeUUID string `json:"nodeuuid"`

	// VolumeName indicates the name of the volume on the supervisor Cluster.
	// This is guaranteed to be unique in Supervisor cluster.
	VolumeName string `json:"volumename"`
}

// CnsNodeVmAttachmentStatus defines the observed state of CnsNodeVmAttachment
// +k8s:openapi-gen=true
type CnsNodeVmAttachmentStatus struct {
	// Indicates the volume is successfully attached.
	// This field must only be set by the entity completing the attach
	// operation, i.e. the CNS Operator.
	Attached bool `json:"attached"`

	// Before successful attach, this field is populated with CNS volume ID.
	// Upon successful attach, this field is populated with any information
	// returned by the attach operation. This field must only be set by the entity
	// completing the attach operation, i.e. the CNS Operator
	AttachmentMetadata map[string]string `json:"metadata,omitempty"`

	// The last error encountered during attach/detach operation, if any.
	// This field must only be set by the entity completing the attach
	// operation, i.e. the CNS Operator.
	// +optional
	Error string `json:"error,omitempty"`
}

// +genclient
// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object

// +k8s:openapi-gen=true
// +kubebuilder:subresource:status

// CnsNodeVmAttachment is the Schema for the cnsnodevmattachments API
type CnsNodeVmAttachment struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec   CnsNodeVmAttachmentSpec   `json:"spec,omitempty"`
	Status CnsNodeVmAttachmentStatus `json:"status,omitempty"`
}

// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object

// CnsNodeVmAttachmentList contains a list of CnsNodeVmAttachment
type CnsNodeVmAttachmentList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []CnsNodeVmAttachment `json:"items"`
}
