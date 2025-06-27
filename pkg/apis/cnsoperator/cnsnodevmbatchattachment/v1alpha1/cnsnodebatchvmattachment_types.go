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

package v1alpha1

import (
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

type AttachState string

const (
	AttachCompleted  AttachState = "ATTACH_COMPLETED"
	AttachFailed     AttachState = "ATTACH_FAILED"
	DetachInProgress AttachState = "DETACH_IN_PROGRESS"
	DetachFailed     AttachState = "DETACH_FAILED"
)

type VolumeStatus struct {
	AttachState AttachState `json:"attachState,omitempty"`
	Error       string      `json:"error,omitempty"`
	CnsVolumeID string      `json:"cnsVolumeId,omitEmpty"`
	Diskuuid    string      `json:"diskuuid,omitEmpty"`
}

// CnsNodeVmBatchAttachmentSpec defines the desired state of CnsNodeVmBatchAttachment
// +k8s:openapi-gen=true
type CnsNodeVmBatchAttachmentSpec struct {
	// NodeUUID indicates the UUID of the node where the volume needs to be attached to.
	// Here NodeUUID is the bios UUID of the node.
	NodeUUID string `json:"nodeuuid"`

	// VolumeName indicates the name of the volume on the supervisor Cluster.
	// This is guaranteed to be unique in Supervisor cluster.
	Volumes map[string]VolumeSpec `json:"volumes"`
}

type DiskMode string

const (
	IndependentNonpersistent DiskMode = "independent_nonpersistent"
	IndependentPersistent    DiskMode = "independent_persistent"
	NonPersistent            DiskMode = "nonpersistent"
	Persistent               DiskMode = "persistent"
)

type SharingMode string

const (
	SharingMultiWriter SharingMode = "sharingMultiWriter"
	SharingNone        SharingMode = "sharingNone"
)

type VolumeSpec struct {
	DiskMode      DiskMode    `json:"diskMode,omitempty"`
	SharingMode   SharingMode `json:"sharingMode,omitempty"`
	ControllerKey int64       `json:"controllerKey,omitempty"`
	UnitNumber    int64       `json:"unitNumber,omitempty"`
}

// CnsNodeVmBatchAttachmentStatus defines the observed state of CnsNodeVmBatchAttachment
// +k8s:openapi-gen=true
type CnsNodeVmBatchAttachmentStatus struct {
	VolumeStatus map[string]VolumeStatus `json:"volumeStatus,omitEmpty"`
}

// +genclient
// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object

// +k8s:openapi-gen=true
// +kubebuilder:subresource:status

// CnsNodeVmBatchAttachment is the Schema for the cnsnodevmbatchattachments API
type CnsNodeVmBatchAttachment struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec   CnsNodeVmBatchAttachmentSpec   `json:"spec,omitempty"`
	Status CnsNodeVmBatchAttachmentStatus `json:"status,omitempty"`
}

// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object

// CnsNodeVmBatchAttachmentList contains a list of CnsNodeVmBatchAttachment
type CnsNodeVmBatchAttachmentList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []CnsNodeVmBatchAttachment `json:"items"`
}
