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

	// LabelDiskUUID is the label key set on CsiVolumeInfo CRs to the
	// VirtualDisk.Backing.Uuid value. The label enables O(1) API-server-indexed
	// lookup of a CVI by diskUUID.
	LabelDiskUUID = "cns.vmware.com/disk-uuid"

	// LabelVolumeOwnership is the label key set on PVC objects to surface the
	// human-friendly ownership state.
	LabelVolumeOwnership = "cns.vmware.com/volume-ownership"

	// OwnershipLabelVMOwned is the PVC label value when the volume is attached
	// to a greenfield VM (CVI ownershipState=VM_MANAGED, vmName set).
	OwnershipLabelVMOwned = "vm-owned"

	// OwnershipLabelCSIOwned is the PVC label value when the volume is
	// detached and registered as an FCD (CVI ownershipState=CSI_MANAGED).
	OwnershipLabelCSIOwned = "csi-owned"

	// OwnershipLabelRetainedBySnapshot is the PVC label value when the volume
	// is snapshot-retained (CVI ownershipState=VM_MANAGED, vmName="").
	OwnershipLabelRetainedBySnapshot = "retained-by-snapshot"

	// CVIProtectionFinalizer is added to a CsiVolumeInfo CR when the volume
	// leaves the CSI_MANAGED steady state. It prevents premature GC while the
	// volume is in-flight, attached to a VM, or pinned by a snapshot.
	CVIProtectionFinalizer = "csi.vsphere.vmware.com/cvi-protection"

	// SnapshotFinalizer is added by CSI to VirtualMachineSnapshot CRs to gate
	// phase-2 PVC re-evaluation during snapshot deletion.
	SnapshotFinalizer = "csi.vsphere.vmware.com/snapshot"
)

// OwnershipState represents the ownership lifecycle state of a volume.
//
// +kubebuilder:validation:Enum=CSI_MANAGED;TRANSFERRING_TO_VM;VM_MANAGED;TRANSFERRING_TO_CSI
type OwnershipState string

const (
	// OwnershipStateCSIManaged is the steady state when the volume is a
	// registered FCD managed by CSI. vmName is empty. No cvi-protection
	// finalizer is held in this state.
	OwnershipStateCSIManaged OwnershipState = "CSI_MANAGED"

	// OwnershipStateTransferringToVM is the transient state between
	// CnsUnregisterVolume (A.3) and vm-operator confirming the disk is on the
	// VM (A.6). vmName is set to the target VM.
	OwnershipStateTransferringToVM OwnershipState = "TRANSFERRING_TO_VM"

	// OwnershipStateVMManaged is the steady state when the disk is a plain
	// VMDK on a greenfield VM (vmName set), or snapshot-retained (vmName="").
	OwnershipStateVMManaged OwnershipState = "VM_MANAGED"

	// OwnershipStateTransferringToCSI is the transient state after
	// vm-operator removes the disk from the VM (C.3) and before CSI
	// re-registers the FCD or marks the volume snapshot-retained. vmName is
	// set to the source VM.
	OwnershipStateTransferringToCSI OwnershipState = "TRANSFERRING_TO_CSI"
)

// CsiVolumeInfoSpec defines the desired (immutable) state of CsiVolumeInfo.
// All fields are set at creation time and must not change except pvcName on
// Retain-reclaim rebind.
type CsiVolumeInfoSpec struct {
	// VolumeID is the CNS volume ID. Immutable after creation.
	// Matches PV.spec.csi.volumeHandle.
	// +kubebuilder:validation:Required
	// +kubebuilder:validation:MinLength=1
	VolumeID string `json:"volumeID"`

	// PVCName is the bound PVC name at CVI creation (or last bind update on
	// Retain-reclaim rebind). Together with metadata.namespace, uniquely
	// identifies the PVC.
	// +kubebuilder:validation:Required
	PVCName string `json:"pvcName"`

	// PVName is the bound PV name. The CVI carries a PV ownerReference so
	// PV deletion cascades CVI deletion via K8s GC.
	// +kubebuilder:validation:Required
	PVName string `json:"pvName"`
}

// CsiVolumeInfoStatus defines the observed state of CsiVolumeInfo.
// All writes go through the /status subresource endpoint.
type CsiVolumeInfoStatus struct {
	// OwnershipState is the current ownership lifecycle state of the volume.
	// One of CSI_MANAGED, TRANSFERRING_TO_VM, VM_MANAGED, TRANSFERRING_TO_CSI.
	// +kubebuilder:validation:Required
	OwnershipState OwnershipState `json:"ownershipState"`

	// VMName is the name of the VirtualMachine CR this volume is attached to
	// (or being attached to / detached from). Empty when CSI-owned or
	// snapshot-retained (ownershipState=VM_MANAGED, vmName="").
	// +optional
	VMName string `json:"vmName,omitempty"`

	// VMInstanceUUID is the instance UUID of the VM identified by VMName.
	// Empty when CSI-owned or snapshot-retained.
	// +optional
	VMInstanceUUID string `json:"vmInstanceUUID,omitempty"`

	// DiskUUID is the stable identifier for the virtual disk
	// (VirtualDisk.Backing.Uuid). Populated at CVI creation from the FCD's
	// backing VMDK. Immutable after creation; also surfaced as the
	// cns.vmware.com/disk-uuid label on this CR.
	// +kubebuilder:validation:Required
	DiskUUID string `json:"diskUUID"`

	// DiskPath is the datastore path to the VMDK file. An informational JIT
	// cache — may be stale at rest. Refreshed at each consumption point
	// (attach, detach, snapshot-delete, revert).
	// +optional
	DiskPath string `json:"diskPath,omitempty"`

	// Conditions is a standard K8s condition array for extensible status.
	// +optional
	// +listType=map
	// +listMapKey=type
	// +kubebuilder:validation:MaxItems=8
	Conditions []metav1.Condition `json:"conditions,omitempty"`
}

// +kubebuilder:object:root=true
// +kubebuilder:subresource:status
// +kubebuilder:resource:shortName=cvi,scope=Namespaced
// +kubebuilder:printcolumn:name="OwnershipState",type=string,JSONPath=`.status.ownershipState`
// +kubebuilder:printcolumn:name="VMName",type=string,JSONPath=`.status.vmName`
// +kubebuilder:printcolumn:name="diskUUID",type=string,JSONPath=`.status.diskUUID`
// +kubebuilder:printcolumn:name="diskPath",type=string,JSONPath=`.status.diskPath`
// +kubebuilder:printcolumn:name="Age",type=date,JSONPath=`.metadata.creationTimestamp`

// CsiVolumeInfo tracks the per-volume ownership lifecycle for the VM-owned
// volume attach/detach model. One CR exists per PVC (in the PVC's namespace)
// from the moment it is provisioned (while VMOwnedVolumes FSS is enabled)
// until its owning PV is deleted.
type CsiVolumeInfo struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec   CsiVolumeInfoSpec   `json:"spec"`
	Status CsiVolumeInfoStatus `json:"status,omitempty"`
}

// +kubebuilder:object:root=true

// CsiVolumeInfoList contains a list of CsiVolumeInfo.
type CsiVolumeInfoList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []CsiVolumeInfo `json:"items"`
}

// GetConditions returns the condition slice for CsiVolumeInfo.
// Implements the conditions.Getter interface.
func (in *CsiVolumeInfo) GetConditions() []metav1.Condition {
	return in.Status.Conditions
}

// SetConditions sets the condition slice for CsiVolumeInfo.
// Implements the conditions.Setter interface.
func (in *CsiVolumeInfo) SetConditions(conditions []metav1.Condition) {
	in.Status.Conditions = conditions
}
