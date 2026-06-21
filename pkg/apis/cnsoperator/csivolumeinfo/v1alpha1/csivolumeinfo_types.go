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

const (
	// CRDSingular represents the singular name of CsiVolumeInfo CRD.
	CRDSingular = "csivolumeinfo"
	// CRDPlural represents the plural name of CsiVolumeInfo CRD.
	CRDPlural = "csivolumeinfos"

	// VolumeProtectionFinalizer prevents GC while ownership is VMManaged.
	VolumeProtectionFinalizer = "csi.vsphere.vmware.com/volume-protection"

	// CVINamespace is the namespace where all CsiVolumeInfo CRs live.
	CVINamespace = "vmware-system-csi"

	// CVINamePrefix is prepended to the volumeID to form the CR name.
	CVINamePrefix = "cns-volume-"
)

// OwnershipState is the current ownership of the volume.
// +kubebuilder:validation:Enum=CSIManaged;VMManaged
type OwnershipState string

const (
	// OwnershipStateCSIManaged is the steady state when the volume is a
	// registered FCD managed by CSI.
	OwnershipStateCSIManaged OwnershipState = "CSIManaged"

	// OwnershipStateVMManaged is the steady state when the disk is a plain
	// VMDK managed by a greenfield VM.
	OwnershipStateVMManaged OwnershipState = "VMManaged"
)

// PhaseState represents the reconcile phase of a CsiVolumeInfo.
// +kubebuilder:validation:Enum=Pending;Succeeded;Failed
type PhaseState string

const (
	// PhasePending indicates the controller has not yet acted on the current spec generation.
	PhasePending PhaseState = "Pending"

	// PhaseSucceeded indicates the last reconcile completed successfully.
	PhaseSucceeded PhaseState = "Succeeded"

	// PhaseFailed indicates the last reconcile encountered an error.
	PhaseFailed PhaseState = "Failed"
)

// VirtualMachineRef identifies a VM attached to the volume.
type VirtualMachineRef struct {
	// VMName is the VirtualMachine CR name.
	VMName string `json:"vmName"`
	// VMInstanceUUID is the instance UUID of the VM.
	// +optional
	VMInstanceUUID string `json:"vmInstanceUUID,omitempty"`
}

// CsiVolumeInfoSpec defines the desired state of CsiVolumeInfo.
type CsiVolumeInfoSpec struct {
	// VolumeID is the CNS volume ID / PV volumeHandle. Immutable after creation.
	// +kubebuilder:validation:Required
	// +kubebuilder:validation:MinLength=1
	VolumeID string `json:"volumeID"`

	// PVCName is the name of the bound PVC. Updated in place on Retain-reclaim rebind.
	// +kubebuilder:validation:Required
	PVCName string `json:"pvcName"`

	// PVCNamespace is the namespace of the bound PVC.
	// +kubebuilder:validation:Required
	PVCNamespace string `json:"pvcNamespace"`

	// PVName is the name of the bound PersistentVolume.
	// +kubebuilder:validation:Required
	PVName string `json:"pvName"`

	// DiskUUID is the stable identifier of the virtual disk. Written by CSI after
	// UnregisterVolumeEx completes and remains set for the lifetime of the VM-managed
	// phase. Cleared when re-registered as FCD.
	// +optional
	DiskUUID string `json:"diskUUID,omitempty"`

	// DiskPath is the datastore path to the VMDK file. Written by CSI alongside DiskUUID
	// after UnregisterVolumeEx. May be refreshed JIT by vm-operator before attachment.
	// +optional
	DiskPath string `json:"diskPath,omitempty"`

	// VMs lists the VirtualMachine CRs that have this volume attached.
	// An empty slice means no VM currently owns the disk (CSI-managed steady state).
	// vm-operator is the sole writer of this field.
	// +optional
	// +listType=map
	// +listMapKey=vmName
	VMs []VirtualMachineRef `json:"vms,omitempty"`
}

// CsiVolumeInfoStatus defines the observed state of CsiVolumeInfo.
// Written exclusively via the /status subresource by the CsiVolumeInfo controller.
type CsiVolumeInfoStatus struct {
	// Ownership is the current ownership state of the volume.
	// +optional
	Ownership OwnershipState `json:"ownership,omitempty"`

	// Phase reflects the last reconcile outcome.
	// +optional
	Phase PhaseState `json:"phase,omitempty"`

	// ObservedGeneration is the spec.generation last acted on by the controller.
	// vm-operator waits until observedGeneration >= spec.generation before proceeding.
	// +optional
	ObservedGeneration int64 `json:"observedGeneration,omitempty"`

	// Error holds the last reconcile error message, if any.
	// +optional
	Error string `json:"error,omitempty"`

	// Conditions is a standard K8s condition array.
	// +optional
	// +listType=map
	// +listMapKey=type
	Conditions []metav1.Condition `json:"conditions,omitempty"`
}

// +kubebuilder:object:root=true
// +kubebuilder:subresource:status
// +kubebuilder:resource:shortName=cvi,scope=Namespaced
// +kubebuilder:printcolumn:name="Ownership",type=string,JSONPath=`.status.ownership`
// +kubebuilder:printcolumn:name="Phase",type=string,JSONPath=`.status.phase`
// +kubebuilder:printcolumn:name="VMs",type=integer,JSONPath=`.spec.vms`
// +kubebuilder:printcolumn:name="Age",type=date,JSONPath=`.metadata.creationTimestamp`

// CsiVolumeInfo tracks the ownership lifecycle of a CSI volume for the VM-owned
// volume attach/detach model. One CR exists per PV in the vmware-system-csi namespace.
type CsiVolumeInfo struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec   CsiVolumeInfoSpec   `json:"spec"`
	Status CsiVolumeInfoStatus `json:"status,omitempty"`
}

// GetConditions returns the conditions for controller-runtime conditions interface.
func (in *CsiVolumeInfo) GetConditions() []metav1.Condition {
	return in.Status.Conditions
}

// SetConditions sets the conditions for controller-runtime conditions interface.
func (in *CsiVolumeInfo) SetConditions(conditions []metav1.Condition) {
	in.Status.Conditions = conditions
}

// +kubebuilder:object:root=true

// CsiVolumeInfoList contains a list of CsiVolumeInfo.
type CsiVolumeInfoList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []CsiVolumeInfo `json:"items"`
}
