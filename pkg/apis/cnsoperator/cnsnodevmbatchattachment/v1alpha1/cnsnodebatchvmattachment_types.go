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
// +kubebuilder:validation:Enum=independent_persistent;persistent;independent_nonpersistent;nonpersistent
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
	// Changes to virtual disk are made to a redo log and discarded at power off.
	NonPersistent = "nonpersistent"

	// This section defines the different conditions that CnsNodeVMBatchAttachment CR can take.
	// ConditionReady reflects the overall status of the CR.
	ConditionReady = "Ready"
	// ConditionAttached reflects whether the given volume was attached successfully.
	ConditionAttached = "VolumeAttached"
	// ConditionDetached reflects whether the given volume was detached successfully.
	ConditionDetached = "VolumeDetached"

	// This section defines the different reasons for different conditions in the CnsNodeVMBatchAttachment CR.
	// ReasonAttachFailed reflects that the volume failed to get attached.
	// In case of successful attachment, reason is set to True.
	ReasonAttachFailed = "AttachFailed"
	// ReasonDetachFailed reflects that the volume failed to get detached.
	// In case of successful detach, the volume's entry is removed from the CR.
	ReasonDetachFailed = "DetachFailed"
	// ReasonFailed reflects that the CR instance is not yet ready.
	ReasonFailed = "Failed"
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

	// InstanceUUID indicates the instance UUID of the node where the volume needs to be attached to.
	InstanceUUID string `json:"instanceUUID"`

	// +listType=map
	// +listMapKey=name
	// VolumeSpec reflects the desired state for each volume.
	Volumes []VolumeSpec `json:"volumes,omitempty"`
}

type VolumeSpec struct {
	// +required

	// Name of the volume as given by the user.
	Name string `json:"name"`
	// +required

	// PersistentVolumeClaim contains details about the volume's desired state.
	PersistentVolumeClaim PersistentVolumeClaimSpec `json:"persistentVolumeClaim"`
}

type PersistentVolumeClaimSpec struct {
	// +required

	// ClaimName is the PVC name.
	ClaimName string `json:"claimName"`
	// +required

	// DiskMode is the desired mode to use when attaching the volume
	DiskMode DiskMode `json:"diskMode"`
	// +required

	// SharingMode indicates the sharing mode if the virtual disk while attaching.
	SharingMode SharingMode `json:"sharingMode"`
	// +required

	// ControllerKey is the object key for the controller object for this device.
	ControllerKey *int32 `json:"controllerKey"`
	// +required

	// UnitNumber of this device on its controller.
	UnitNumber *int32 `json:"unitNumber"`
}

// CnsNodeVMBatchAttachmentStatus defines the observed state of CnsNodeVMBatchAttachment
// +k8s:openapi-gen=true
type CnsNodeVMBatchAttachmentStatus struct {
	// +optional
	// +listType=map
	// +listMapKey=name
	// VolumeStatus reflects the status for each volume.
	VolumeStatus []VolumeStatus `json:"volumes,omitempty"`
	// +optional

	// Conditions describes any conditions associated with this CnsNodeVMBatchAttachment instance.
	Conditions []metav1.Condition `json:"conditions,omitempty"`
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
	// +optional

	// CnsVolumeID is the volume ID for the PVC.
	CnsVolumeID string `json:"cnsVolumeId,omitempty"`
	// +optional

	// DiskUUID is the ID obtained when volume is attached to a VM.
	DiskUUID string `json:"diskUUID,omitempty"`
	// +optional

	// Conditions describes any conditions associated with this volume.
	Conditions []metav1.Condition `json:"conditions,omitempty"`
}

// +genclient
// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object

// +k8s:openapi-gen=true
// +kubebuilder:subresource:status
// +kubebuilder:object:root=true
// +kubebuilder:resource:shortName=batchattach
// +kubebuilder:printcolumn:name="InstanceUUID",type="string",JSONPath=".spec.instanceUUID"

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

func (in *CnsNodeVMBatchAttachment) GetConditions() []metav1.Condition {
	return in.Status.Conditions
}

func (in *CnsNodeVMBatchAttachment) SetConditions(conditions []metav1.Condition) {
	in.Status.Conditions = conditions
}

func (p *PersistentVolumeClaimStatus) GetConditions() []metav1.Condition {
	return p.Conditions
}

func (p *PersistentVolumeClaimStatus) SetConditions(conditions []metav1.Condition) {
	p.Conditions = conditions
}
