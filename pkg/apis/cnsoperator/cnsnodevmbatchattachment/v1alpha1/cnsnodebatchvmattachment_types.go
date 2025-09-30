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
type DiskMode string

const (
	// By setting DiskMode to independent_persistent, a virtual machine's disk is not captured in snapshots and
	// changes are permanently written to the disk, regardless of snapshot operations.
	IndependentPersistent DiskMode = "independent_persistent"
	// Changes are immediately and permanently written to the virtual disk.
	Persistent DiskMode = "persistent"
	// Changes to virtual disk are made to a redo log and discarded at power off.
	// It is not affected by snapshots.
	IndependentNonPersistent = "independent_nonpersistent"
)

// The sharing mode of the virtual disk.
type SharingMode string

const (
	// The virtual disk is shared between multiple virtual machines.
	SharingMultiWriter SharingMode = "sharingMultiWriter"
	// The virtual disk is not shared.
	SharingNone SharingMode = "sharingNone"
)

// CnsNodeVmBatchAttachmentSpec defines the desired state of CnsNodeVmBatchAttachment
// +k8s:openapi-gen=true
type CnsNodeVmBatchAttachmentSpec struct {
	// NodeUUID indicates the UUID of the node where the volume needs to be attached to.
	// Here NodeUUID is the instance UUID of the node.
	NodeUUID string `json:"nodeuuid"`

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
	// ClaimName is the PVC name.
	ClaimName string `json:"claimName"`
	// DiskMode is the desired mode to use when attaching the volume
	DiskMode DiskMode `json:"diskMode,omitempty"`
	// SharingMode indicates the shraring mode if the virtual disk while attaching.
	SharingMode SharingMode `json:"sharingMode,omitempty"`
	// ControllerKey is the object key for the controller object for this device.
	ControllerKey string `json:"controllerKey,omitempty"`
	// UnitNumber of this device on its controller.
	UnitNumber string `json:"unitNumber,omitempty"`
}

// CnsNodeVmBatchAttachmentStatus defines the observed state of CnsNodeVmBatchAttachment
// +k8s:openapi-gen=true
type CnsNodeVmBatchAttachmentStatus struct {
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
	// If volume is not attached, Attached will be set to false.
	// If volume is attached, Attached will be set to true.
	// If volume is detached successfully, its entry will be removed from VolumeStatus.
	Attached bool `json:"attached"`
	// Error indicates the error which may have occurred during attach/detach.
	Error string `json:"error,omitempty"`
	// CnsVolumeID is the volume ID for the PVC.
	CnsVolumeID string `json:"cnsVolumeId,omitempty"`
	// Diskuuid is the ID obtained when volume is attached to a VM.
	Diskuuid string `json:"diskuuid,omitempty"`
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
