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

	// VolumeID indicates an existing vsphere volume to be imported into Project
	// Pacific cluster.
	// If the AccessMode is "ReadWriteMany" or "ReadOnlyMany", then this VolumeID
	// can be either an existing FileShare (or) CNS file volume backed FileShare.
	// If the AccessMode is "ReadWriteOnce", then this VolumeID can be either an
	// existing FCD (or) a CNS backed FCD.
	// VolumeID and DiskUrlPath cannot be specified together.
	VolumeID string `json:"volumeID,omitempty"`

	// AccessMode contains the actual access mode the volume backing the
	// CnsRegisterVolume has.
	// AccessMode must be specified if VolumeID is specified.
	AccessMode v1.PersistentVolumeAccessMode `json:"accessMode,omitempty"`

	// DiskUrlPath is URL path to an existing block volume to be imported into
	// Project Pacific cluster.
	// VolumeID and DiskUrlPath cannot be specified together.
	// DiskUrlPath is explicitly used for block volumes. AccessMode need not be
	// specified and will be defaulted to "ReadWriteOnce".
	// This field must be in the following format:
	// Format:
	// https://<vc_ip>/folder/<vm_vmdk_path>?dcPath=<datacenterName>&dsName=<datastoreName>
	// Ex: https://10.192.255.221/folder/34a9c05d-5f03-e254-e692-02004479cb91/
	//        vm2_1.vmdk?dcPath=Datacenter-1&dsName=vsanDatastore
	// This is for a 34a9c05d-5f03-e254-e692-02004479cb91/vm2_1.vmdk
	// file under datacenter "Datacenter-1" and datastore "vsanDatastore".
	DiskURLPath string `json:"diskURLPath,omitempty"`

	// VolumeMode can either be Block (for raw block volume) or
	// Filesystem. Default values is Filesystem.
	VolumeMode v1.PersistentVolumeMode `json:"volumeMode,omitempty"`

	// BackingType enumerates types of backing for batch attach operations.
	// The values correspond to the cns.VolumeBackingType enum.
	// Valid values: FlatVer1BackingInfo, FlatVer2BackingInfo, SparseVer1BackingInfo,
	// SparseVer2BackingInfo, RawDiskMappingVer1BackingInfo, SeSparseBackingInfo,
	// LocalPMemBackingInfo, or empty string.
	BackingType string `json:"backingType,omitempty"`

	// StorageClassName is the name of the Kubernetes StorageClass whose associated
	// vSphere storage policy should be assigned to the volume being registered.
	// This is optional. If set, the storage policy is applied unconditionally,
	// regardless of whether the volume already has a storage policy associated
	// with it in CNS.
	StorageClassName string `json:"storageClassName,omitempty"`
}

// Condition types reported on a CnsRegisterVolume. Each is a durable fact
// about the registration: True means that step is done. Conditions are set
// lazily as each step is first evaluated, so an absent condition means "not yet
// evaluated", which is distinct from False.
//
// Registered, not ConditionReady, remains the completion signal that callers
// (e.g. VM Operator, and the VKSRegisterVolume controller's Supervisor watch)
// key off. Conditions must not be used to infer completion.
const (
	// ConditionReady reflects the overall state of the registration. It is True
	// exactly when Registered is true, and is published in the same write.
	ConditionReady = "Ready"
	// ConditionSpecValidated reflects whether the spec's own fields are well
	// formed and, when a storageClassName is given, whether its storage policy
	// is assigned to this namespace. Evaluated before any CNS call.
	ConditionSpecValidated = "SpecValidated"
	// ConditionRestoreValidated reports restore prerequisites and admission.
	ConditionRestoreValidated = "RestoreValidated"
	// ConditionRestoreMetadataConfigured reports completion of the restored CVI
	// handover, including a warning-only outcome without a snapshot-size seed.
	// SnapshotSizeRestored remains a condition on CnsVolumeInfo, owned by its writers.
	ConditionRestoreMetadataConfigured = "RestoreMetadataConfigured"
	// ConditionCnsVolumeRegistered reflects whether CNS has registered the disk
	// and the volume exists.
	ConditionCnsVolumeRegistered = "CnsVolumeRegistered"
	// ConditionVolumeAccessible reflects whether the registered volume is
	// reachable and usable from this cluster: its datastore is accessible, its
	// storage policy is present and compatible, and any host-local topology has
	// been resolved. These are checked against the volume CNS returned, so they
	// can only run once it exists.
	ConditionVolumeAccessible = "VolumeAccessible"
	// ConditionPvcBound reflects whether the PV and PVC exist and the PVC has
	// reached Bound.
	ConditionPvcBound = "PvcBound"
	// ConditionVMDeleteProtectionConfigured reports that keepAfterDeleteVm was
	// cleared and recorded on the PVC, or that this registration does not require it.
	ConditionVMDeleteProtectionConfigured = "VMDeleteProtectionConfigured"
)

// Reasons reported on CnsRegisterVolume conditions.
//
// A False condition is not by itself terminal: terminality is carried by the
// reason. Only the reasons marked terminal below stop the controller retrying,
// and those are exactly the paths that returned without requeuing before
// conditions existed. Everything else, including ReasonCnsTaskFailed, retries.
const (
	// ReasonValidated is set on ConditionSpecValidated when the checks pass.
	ReasonValidated                  = "Validated"
	ReasonWaitingForRestoreOwner     = "WaitingForRestoreOwner"
	ReasonRestorePrecheckFailed      = "RestorePrecheckFailed"
	ReasonRestoreNotProcessed        = "RestoreNotProcessed"
	ReasonConfiguringRestoreMetadata = "ConfiguringRestoreMetadata"
	ReasonRestoreMetadataFailed      = "RestoreMetadataFailed"
	ReasonRestoreMetadataConfigured  = "RestoreMetadataConfigured"
	// ReasonInvalidSpec means the spec is malformed or incomplete.
	ReasonInvalidSpec = "InvalidSpec"
	// ReasonStoragePolicyNotAssignedToNamespace means the storageClassName's
	// policy is not assigned to this namespace. Checked before any CNS call so
	// a namespace cannot register a volume with a policy it is not entitled to.
	ReasonStoragePolicyNotAssignedToNamespace = "StoragePolicyNotAssignedToNamespace"
	// ReasonStoragePolicyLookupFailed means the policy for the StorageClass
	// could not be resolved, or its namespace assignment not verified.
	ReasonStoragePolicyLookupFailed = "StoragePolicyLookupFailed"

	// ReasonRegistered is set on ConditionCnsVolumeRegistered when the CNS
	// volume exists, and on ConditionReady on success.
	ReasonRegistered = "Registered"
	// ReasonWaitingForTaskSlot means the in-flight CNS task cap is full.
	// Nothing has been sent to CNS yet.
	ReasonWaitingForTaskSlot = "WaitingForTaskSlot"
	// ReasonCnsTaskRunning means the CreateVolume task is submitted and
	// running. No reconciler worker is held while it runs.
	ReasonCnsTaskRunning = "CnsTaskRunning"
	// ReasonCnsTaskFailed means CNS reported a terminal fault for the task.
	// This is retried, not terminal: classifying CNS faults as permanent needs
	// agreement with VM Operator first.
	ReasonCnsTaskFailed = "CnsTaskFailed"
	// ReasonCnsTaskStateUnknown means the task's state could not be
	// determined. The task ID and its capacity slot are kept and the task is
	// never resubmitted, because it may still be running on CNS.
	ReasonCnsTaskStateUnknown = "CnsTaskStateUnknown"
	// ReasonVolumeCleanedUp means the CNS volume this condition reported was
	// deleted by a terminal accessibility failure, so the registration has
	// been undone.
	ReasonVolumeCleanedUp = "VolumeCleanedUp"

	// ReasonAccessible is set on ConditionVolumeAccessible when the registered
	// volume is usable from this cluster.
	ReasonAccessible = "Accessible"
	// ReasonDatastoreNotFound means a host-local volume's datastore is not
	// present in the namespace's clusters. Terminal.
	ReasonDatastoreNotFound = "DatastoreNotFound"
	// ReasonHostLocalPolicyIncompatible means the host-local storage policy is
	// not compatible with the datastore the disk lives on. Terminal.
	ReasonHostLocalPolicyIncompatible = "HostLocalPolicyIncompatible"
	// ReasonTopologyUnresolved means the accessible topology for a host-local
	// volume could not be resolved. Terminal.
	ReasonTopologyUnresolved = "TopologyUnresolved"
	// ReasonDatastoreNotAccessibleToAZClusters means that, on a stretched
	// supervisor, the datastore is not accessible to any active AZ cluster.
	// Terminal.
	ReasonDatastoreNotAccessibleToAZClusters = "DatastoreNotAccessibleToAZClusters"
	// ReasonDatastoreNotAccessibleToCluster means the datastore is not
	// accessible to all nodes in the cluster. Terminal.
	ReasonDatastoreNotAccessibleToCluster = "DatastoreNotAccessibleToCluster"
	// ReasonStoragePolicyMissing means the registered volume has no storage
	// policy associated with it. Retried, not terminal.
	ReasonStoragePolicyMissing = "StoragePolicyMissing"
	// ReasonHostLocalCheckFailed means whether the policy is host-local could
	// not be determined. Retried.
	ReasonHostLocalCheckFailed = "HostLocalCheckFailed"
	// ReasonCleanupPending means a terminal check failed but deleting the CNS
	// volume did not succeed, so the outcome is retried until the volume is
	// gone and the terminal reason can be published.
	ReasonCleanupPending = "CleanupPending"

	// ReasonBound is set on ConditionPvcBound when the PVC is bound.
	ReasonBound = "Bound"
	// ReasonConfiguringK8sResources means the PV and PVC are being created and
	// topology and volumeMode fixes applied.
	ReasonConfiguringK8sResources = "ConfiguringK8sResources"
	// ReasonWaitingForBind means the PV and PVC exist and the controller is
	// re-checking the PVC with a single Get until it binds.
	ReasonWaitingForBind = "WaitingForBind"

	// ReasonFinalizing is set on ConditionReady while the PVC is bound and the
	// remaining bookkeeping (CnsVolumeInfo, storage quota accounting) is being
	// applied.
	ReasonFinalizing = "Finalizing"

	// VM-delete-protection failures are retryable and do not undo PVC binding.
	ReasonClearingKeepAfterDeleteVm      = "ClearingKeepAfterDeleteVm"
	ReasonClearKeepAfterDeleteVmFailed   = "ClearKeepAfterDeleteVmFailed"
	ReasonClearKeepAfterDeleteVmTimedOut = "ClearKeepAfterDeleteVmTimedOut"
	ReasonPersistClearResultFailed       = "PersistClearResultFailed"
	ReasonKeepAfterDeleteVmCleared       = "KeepAfterDeleteVmCleared"
	ReasonNotRequired                    = "NotRequired"
)

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

	// CnsTaskID is the vCenter managed object ID of the in-flight CNS
	// CreateVolume task for this CR, while one is running.
	//
	// It exists because the controller submits the task and returns rather than
	// holding a worker until CNS responds, so the task reference has to survive
	// across reconciles. There is no other durable copy: this controller writes
	// no CnsVolumeOperationRequest, and the volume manager skips its in-memory
	// task map for static provisioning specs.
	//
	// Set immediately after a successful submit and cleared once the task has
	// reached a terminal state. A cleared or failed task ID is never reused.
	// +optional
	CnsTaskID string `json:"cnsTaskID,omitempty"`

	// CnsVolumeID is the successful task result. It is persisted in the same
	// write that clears CnsTaskID, before releasing the task result. Later bind
	// and finalization retries query this volume instead of submitting again.
	// Cleanup clears it only after undoing CNS registration. Unlike conditions,
	// this field controls recovery and its writes must succeed.
	// +optional
	CnsVolumeID string `json:"cnsVolumeID,omitempty"`

	// Conditions describes the observed progress of this registration, and the
	// reason it is not yet ready.
	//
	// Conditions are informational: Registered, not the Ready condition, is the
	// completion signal. Absent on CRs last written by a controller that
	// predates this field; consumers must treat an absent condition as unknown
	// rather than as False, and must look conditions up by type rather than by
	// index, because the stored order is Ready first and then alphabetical.
	// +optional
	// +patchMergeKey=type
	// +patchStrategy=merge
	// +listType=map
	// +listMapKey=type
	Conditions []metav1.Condition `json:"conditions,omitempty" patchStrategy:"merge" patchMergeKey:"type"`
}

// GetConditions returns the conditions on this instance, implementing the
// Getter interface the pkg/common/conditions helpers require.
func (in *CnsRegisterVolume) GetConditions() []metav1.Condition {
	return in.Status.Conditions
}

// SetConditions sets the conditions on this instance, implementing the Setter
// interface the pkg/common/conditions helpers require.
func (in *CnsRegisterVolume) SetConditions(conditions []metav1.Condition) {
	in.Status.Conditions = conditions
}

// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object

// CnsRegisterVolume is the Schema for the cnsregistervolumes API
// +k8s:openapi-gen=true
// +kubebuilder:subresource:status
// +kubebuilder:printcolumn:name="PVCName",type=string,JSONPath=`.spec.pvcName`
// +kubebuilder:printcolumn:name="Registered",type=boolean,JSONPath=`.status.registered`
// +kubebuilder:printcolumn:name="Ready",type=string,JSONPath=`.status.conditions[?(@.type=="Ready")].status`
// +kubebuilder:printcolumn:name="Reason",type=string,JSONPath=`.status.conditions[?(@.type=="Ready")].reason`
// +kubebuilder:printcolumn:name="Age",type=date,JSONPath=`.metadata.creationTimestamp`
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
