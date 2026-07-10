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
)

// VKSRegisterVolumeSpec defines the desired state of VKSRegisterVolume.
// +k8s:openapi-gen=true
//
// VKSRegisterVolume is a guest (VKS) cluster CR that binds a pre-created guest
// PersistentVolumeClaim to an already-registered Supervisor volume. It intentionally
// does NOT expose any VC-relative disk identity (no VolumeID / DiskURLPath) in the
// guest cluster: that identity lives only on the referenced Supervisor
// CnsRegisterVolume. This CR only references the pre-created guest PVC and that
// Supervisor CnsRegisterVolume; it does not itself create either.
//
// The supported workflow is the data-protection restore path, which drives all
// three objects (the guest PVC, the Supervisor CnsRegisterVolume, and this CR) in
// the correct order. The controller has no way to check who created the referenced
// objects, so a client with sufficient RBAC could in principle pre-create them by
// hand and create this CR directly for static provisioning; this is not a
// supported or documented usage and is not validated against — treat it as
// unsupported, not as an alternate API contract.
type VKSRegisterVolumeSpec struct {
	// PVCName is the name of the pre-created guest PersistentVolumeClaim, in this
	// CR's namespace, that must already exist in the Pending state with
	// spec.volumeName set to the future guest PV name. The controller binds this
	// PVC by creating the matching PV; it never creates or deletes the PVC.
	// +kubebuilder:validation:Required
	PVCName string `json:"pvcName"`

	// CnsRegisterVolumeName is the name of the CnsRegisterVolume CR in the Supervisor
	// cluster, in the Supervisor namespace this VKS cluster is deployed in (see
	// config.GetSupervisorNamespace; a VKS cluster can only ever access resources in
	// that namespace, so it is not repeated here). The controller watches this CR for
	// registration/binding and uses its spec.pvcName (the Supervisor PVC name) to
	// locate the Supervisor PVC/PV and obtain the CSI volumeHandle for the guest PV.
	// +kubebuilder:validation:Required
	CnsRegisterVolumeName string `json:"cnsRegisterVolumeName"`
}

// VKSRegisterVolumePhase is the lifecycle phase of a VKSRegisterVolume.
type VKSRegisterVolumePhase string

const (
	// VKSRegisterVolumePhasePending is the initial phase before any work.
	VKSRegisterVolumePhasePending VKSRegisterVolumePhase = "Pending"
	// VKSRegisterVolumePhaseWaitingForSupervisorRegistration waits for the
	// referenced Supervisor CnsRegisterVolume to report Registered==true.
	VKSRegisterVolumePhaseWaitingForSupervisorRegistration VKSRegisterVolumePhase = "WaitingForSupervisorRegistration"
	// VKSRegisterVolumePhaseWaitingForSupervisorBinding waits for the Supervisor
	// PVC (== CnsRegisterVolume.spec.pvcName) to reach Bound.
	VKSRegisterVolumePhaseWaitingForSupervisorBinding VKSRegisterVolumePhase = "WaitingForSupervisorBinding"
	// VKSRegisterVolumePhaseCreatingGuestPV creates the guest PV (name from the
	// referenced PVC's spec.volumeName) with a claimRef back to that PVC.
	VKSRegisterVolumePhaseCreatingGuestPV VKSRegisterVolumePhase = "CreatingGuestPV"
	// VKSRegisterVolumePhaseWaitingForGuestPVCBound waits for the pre-created guest
	// PVC and the new guest PV to bind directly.
	VKSRegisterVolumePhaseWaitingForGuestPVCBound VKSRegisterVolumePhase = "WaitingForGuestPVCBound"
	// VKSRegisterVolumePhaseRegistered is the terminal success phase.
	VKSRegisterVolumePhaseRegistered VKSRegisterVolumePhase = "Registered"
	// VKSRegisterVolumePhaseFailed is the terminal failure phase.
	VKSRegisterVolumePhaseFailed VKSRegisterVolumePhase = "Failed"
)

// VKSRegisterVolumeStatus defines the observed state of VKSRegisterVolume.
// +k8s:openapi-gen=true
type VKSRegisterVolumeStatus struct {
	// Phase is the current lifecycle phase.
	// +optional
	Phase VKSRegisterVolumePhase `json:"phase,omitempty"`

	// Registered is true once the guest PVC is Bound to the guest PV created by the
	// controller. Callers can watch this field to detect completion.
	// +optional
	Registered bool `json:"registered,omitempty"`

	// Error is a human-readable message describing a terminal failure, if any.
	// +optional
	Error string `json:"error,omitempty"`
}

// +genclient
// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object
// +kubebuilder:object:root=true
// +k8s:openapi-gen=true
// +kubebuilder:subresource:status
// +kubebuilder:resource:scope=Namespaced,shortName=vksregvol
// +kubebuilder:printcolumn:name="PVCName",type=string,JSONPath=`.spec.pvcName`
// +kubebuilder:printcolumn:name="Phase",type=string,JSONPath=`.status.phase`
// +kubebuilder:printcolumn:name="Registered",type=boolean,JSONPath=`.status.registered`
// +kubebuilder:printcolumn:name="Age",type=date,JSONPath=`.metadata.creationTimestamp`

// VKSRegisterVolume is the Schema for the vksregistervolumes API.
type VKSRegisterVolume struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec   VKSRegisterVolumeSpec   `json:"spec,omitempty"`
	Status VKSRegisterVolumeStatus `json:"status,omitempty"`
}

// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object
// +kubebuilder:object:root=true

// VKSRegisterVolumeList contains a list of VKSRegisterVolume.
type VKSRegisterVolumeList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []VKSRegisterVolume `json:"items"`
}
