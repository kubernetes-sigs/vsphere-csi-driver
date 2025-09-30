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

// DiskMode describes the desired mode to use when attaching the volume.
// +kubebuilder:validation:Enum=independent_persistent;persistent;independent_nonpersistent
type DiskMode string

const (
	// IndependentPersistent is the diskMode in which a virtual machine's disk is not captured in snapshots and
	// changes are permanently written to the disk, regardless of snapshot operations.
	IndependentPersistent DiskMode = "independent_persistent"
	// Persistent diskMode changes are immediately and permanently written to the virtual disk.
	Persistent DiskMode = "persistent"
	// IndependentNonPersistent is the diskMode in which changes to virtual disk are made to a redo log
	// and discarded at power off.
	// It is not affected by snapshots.
	IndependentNonPersistent = "independent_nonpersistent"
)

// SharingMode is the sharing mode of the virtual disk.
// +kubebuilder:validation:Enum=sharingMultiWriter;sharingNone
type SharingMode string

const (
	// SharingMultiWriter: The virtual disk is shared between multiple virtual machines.
	SharingMultiWriter SharingMode = "sharingMultiWriter"
	// SharingNone: The virtual disk is not shared.
	SharingNone SharingMode = "sharingNone"
)

// CnsNodeVMBatchAttachmentSpec defines the desired state of CnsNodeVMBatchAttachment
// +k8s:openapi-gen=true
type CnsNodeVMBatchAttachmentSpec struct {
	// +required

	// NodeUUID indicates the UUID of the node where the volume needs to be attached to.
	// Here NodeUUID is the instance UUID of the node.
	NodeUUID string `json:"nodeUUID"`

	// +listType=map
	// +listMapKey=name
	// VolumeSpec reflects the desired state for each volume.
	Volumes []VolumeSpec `json:"volumes"`
}

type VolumeSpec struct {
	// Name of the volume as given by the user.
	Name string `json:"name"`
	// PersistentVolumeClaim contains details about the volume's desired state.
	PersistentVolumeClaim PersistentVolumeClaimSpec `json:"persistentVolumeClaim"`
}

type PersistentVolumeClaimSpec struct {
	// +required

	// ClaimName is the PVC name.
	ClaimName string `json:"claimName"`
	// +optional

	// DiskMode is the desired mode to use when attaching the volume
	DiskMode DiskMode `json:"diskMode,omitempty"`
	// +optional

	// SharingMode indicates the sharing mode if the virtual disk while attaching.
	SharingMode SharingMode `json:"sharingMode,omitempty"`
	// +optional

	// ControllerKey is the object key for the controller object for this device.
	ControllerKey *int32 `json:"controllerKey,omitempty"`
	// +optional

	// UnitNumber of this device on its controller.
	UnitNumber *int32 `json:"unitNumber,omitempty"`
}

// CnsNodeVMBatchAttachmentStatus defines the observed state of CnsNodeVMBatchAttachment
// +k8s:openapi-gen=true
type CnsNodeVMBatchAttachmentStatus struct {
	// Error is the overall error status for the instance.
	Error string `json:"error,omitempty"`
	// +listType=map
	// +listMapKey=name
	// VolumeStatus reflects the status for each volume.
	VolumeStatus []VolumeStatus `json:"volumes,omitempty"`
}

type VolumeStatus struct {
	// Name of the volume as given by the user.
	Name string `json:"name"`
	// PersistentVolumeClaim contains details about the volume's current state.
	PersistentVolumeClaim PersistentVolumeClaimStatus `json:"persistentVolumeClaim"`
}

type PersistentVolumeClaimStatus struct {
	// ClaimName is the PVC name.
	ClaimName string `json:"claimName"`
	// Attached indicates the attach status of a PVC.
	// If volume is not attached, Attached will be set to false.
	// If volume is attached, Attached will be set to true.
	// If volume is detached successfully, its entry will be removed from VolumeStatus.
	Attached bool `json:"attached"`
	// Error indicates the error which may have occurred during attach/detach.
	Error string `json:"error,omitempty"`
	// CnsVolumeID is the volume ID for the PVC.
	CnsVolumeID string `json:"cnsVolumeId,omitempty"`
	// DiskUUID is the ID obtained when volume is attached to a VM.
	DiskUUID string `json:"DiskUUID,omitempty"`
}

// +genclient
// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object

// +k8s:openapi-gen=true
// +kubebuilder:subresource:status
// +kubebuilder:object:root=true
// +kubebuilder:resource:shortName=batchattach
// +kubebuilder:printcolumn:name="NodeUUID",type="string",JSONPath=".spec.nodeUUID"

// CnsNodeVMBatchAttachment is the Schema for the cnsnodevmbatchattachments API
type CnsNodeVMBatchAttachment struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec   CnsNodeVMBatchAttachmentSpec   `json:"spec,omitempty"`
	Status CnsNodeVMBatchAttachmentStatus `json:"status,omitempty"`
}

// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object

// CnsNodeVMBatchAttachmentList contains a list of CnsNodeVMBatchAttachment
type CnsNodeVMBatchAttachmentList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []CnsNodeVMBatchAttachment `json:"items"`
}
