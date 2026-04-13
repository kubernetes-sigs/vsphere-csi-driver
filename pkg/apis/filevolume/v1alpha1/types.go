/*
Copyright (c) 2025 Broadcom. All Rights Reserved.
Broadcom Confidential. The term "Broadcom" refers to Broadcom Inc.
and/or its subsidiaries.

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
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// FileVolumePhase defines the high-level lifecycle phase of a FileVolume.
type FileVolumePhase string

const (
	FileVolumePhasePending      FileVolumePhase = "Pending"
	FileVolumePhaseProvisioning FileVolumePhase = "Provisioning"
	FileVolumePhaseReady        FileVolumePhase = "Ready"
	FileVolumePhaseDeleting     FileVolumePhase = "Deleting"
	FileVolumePhaseError        FileVolumePhase = "Error"
)

// FileVolumeProtocol defines the file sharing protocol supported by a FileVolume (e.g., nfs3, nfs4)
type FileVolumeProtocol string

const (
	FileVolumeProtocolNFSv3 FileVolumeProtocol = "nfs3"
	FileVolumeProtocolNFSv4 FileVolumeProtocol = "nfs4"
)

// FileVolumeSpec defines the desired state of FileVolume.
type FileVolumeSpec struct {
	// *Required*
	// Size of the file volume.
	// +required
	Size resource.Quantity `json:"size"`

	// *Optional*
	// StoragePolicyID specifies the vSAN storage policy to be used
	// for provisioning the backend volume. Immutable after creation.
	// +optional
	StoragePolicyID string `json:"storagePolicyID,omitempty"`

	// *Optional*
	// ExistingVdfsVolumeUUID is used for fileshare -> filevolume migration.
	// Immutable after creation.
	//
	// This field is only supported for migration/adoption workflows and must refer to a VDFS volume
	// on the same vSAN cluster as the target FVS instance.
	// +optional
	ExistingVdfsVolumeUUID string `json:"existingVdfsVolumeUUID,omitempty"`

	// *Optional*
	// Protocols defines the file sharing protocols supported by this volume.
	// If not specified, defaults to nfs4 (set by mutating webhook). Immutable after creation.
	// +kubebuilder:validation:MinItems=1
	// +kubebuilder:validation:Items:Enum=nfs3;nfs4
	// +optional
	Protocols []FileVolumeProtocol `json:"protocols,omitempty"`

	// *Required*
	// Kubernetes PVC UID for identity fencing and preemption.
	// VDFS uses this to verify ownership when Attach() is called.
	// Immutable after creation.
	// +required
	PvcUID string `json:"pvcUID"`
}

// FileVolumeStatus defines the observed state of FileVolume.
type FileVolumeStatus struct {
	// *Required*
	// High-level lifecycle phase.
	// +kubebuilder:validation:Enum=Pending;Provisioning;Ready;Deleting;Error
	// +required
	Phase FileVolumePhase `json:"phase"`

	// *Optional*
	// UUID of the underlying vdfs volume.
	// +optional
	VdfsVolumeUUID string `json:"vdfsVolumeUUID,omitempty"`

	// *Optional*
	// NFS export path (e.g. "/exports/pv-123").
	// +optional
	ExportPath string `json:"exportPath,omitempty"`

	// *Optional*
	// Endpoint (IP/Service DNS name) of the NFS server.
	// +optional
	Endpoint string `json:"endpoint,omitempty"`

	// *Optional*
	// Standard Kubernetes conditions (e.g. Ready).
	// +optional
	Conditions []metav1.Condition `json:"conditions,omitempty"`

	// *Optional*
	// ObservedGeneration is the most recent generation observed for this FileVolume.
	// +optional
	ObservedGeneration int64 `json:"observedGeneration,omitempty"`

	// *Optional*
	// LastAppliedSize is the last FileVolume spec size that was applied to the VolumeAssignment.
	// Used to detect size expansion (spec.size increased compared to this value).
	// +optional
	LastAppliedSize resource.Quantity `json:"lastAppliedSize,omitempty"`
}

// +kubebuilder:object:root=true
// +kubebuilder:subresource:status
// +kubebuilder:resource:path=filevolumes,scope=Namespaced,shortName=fv
// +kubebuilder:printcolumn:name="Phase",type=string,JSONPath=`.status.phase`,description="FileVolume phase"
// +kubebuilder:printcolumn:name="Size",type=string,JSONPath=`.spec.size`,description="FileVolume size"
// +kubebuilder:printcolumn:name="ObservedGen",type=integer,JSONPath=`.status.observedGeneration`,priority=1
// +kubebuilder:printcolumn:name="Age",type=date,JSONPath=`.metadata.creationTimestamp`
// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object
// +genclient
// FileVolume Schema.
type FileVolume struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec   FileVolumeSpec   `json:"spec,omitempty"`
	Status FileVolumeStatus `json:"status,omitempty"`
}

// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object
// +kubebuilder:object:root=true
// FileVolumeList contains a list of FileVolume.
type FileVolumeList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []FileVolume `json:"items"`
}
