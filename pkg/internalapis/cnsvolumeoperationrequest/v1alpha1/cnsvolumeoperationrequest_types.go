/*
Copyright 2021 The Kubernetes Authors.

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

// CnsVolumeOperationRequestSpec defines the desired state of CnsVolumeOperationRequest
type CnsVolumeOperationRequestSpec struct {
	// Name represents the name of the instance.
	// There is no strict naming convention for instances; it is dependent on the caller.
	Name string `json:"name"`
}

// CnsVolumeOperationRequestStatus defines the observed state of CnsVolumeOperationRequest
type CnsVolumeOperationRequestStatus struct {
	// VolumeID is the unique ID of the backend volume.
	// Populated during successful CreateVolume calls.
	VolumeID string `json:"volumeID,omitempty"`
	// SnapshotID is the unique ID of the backend snapshot.
	// Populated during successful CreateSnapshot calls.
	SnapshotID string `json:"snapshotID,omitempty"`
	// Populated with the latest capacity on every successful ExtendVolume call for a volume.
	Capacity int64 `json:"capacity,omitempty"`
	// ErrorCount is the number of times this operation failed for this volume.
	// Incremented by clients when new OperationDetails are added with error set.
	ErrorCount int `json:"errorCount,omitempty"`
	// StorageQuotaDetails stores the details required by the CSI driver and syncer to
	// access the quota custom resources.
	// +optional
	StorageQuotaDetails *QuotaDetails `json:"quotaDetails,omitempty"`
	// FirstOperationDetails stores the details of the first operation performed on the volume.
	// For debugging purposes, clients should ensure that this information is never overwritten.
	// More recent operation details should be stored in the LatestOperationDetails field.
	FirstOperationDetails OperationDetails `json:"firstOperationDetails,omitempty"`
	// LatestOperationDetails stores the details of the latest operations performed
	// on the volume. Should have a maximum of 10 entries.
	LatestOperationDetails []OperationDetails `json:"latestOperationDetails,omitempty"`
}
type QuotaDetails struct {
	// ReservationId is the unique ID added as annotation on the resource by storage quota webhook.
	// +optional
	ReservationId string `json:"reservationId,omitempty"`
	// Reserved keeps a track of the quantity that should be reserved in
	// storage quota during a create volume/snapshot operation.
	// +optional
	Reserved *resource.Quantity `json:"reserved,omitempty"`
	// StoragePolicyId is the ID associated with the storage policy.
	StoragePolicyId string `json:"storagePolicyId,omitempty"`
	// StorageClassName is the name of K8s storage class associated with the given storage policy.
	StorageClassName string `json:"storageClassName,omitempty"`
	// Namespace of the PersistentVolumeClaim.
	Namespace string `json:"namespace,omitempty"`
	// AggregatedSnapshotSize stores the aggregate snapshot size for volume.
	AggregatedSnapshotSize *resource.Quantity `json:"aggregatedsnapshotsize,omitempty"`
	// Associated time stamp of the create or delete snapshot task completion.
	// This is used to ordering concurrent snapshots on same volume.
	SnapshotLatestOperationCompleteTime metav1.Time `json:"snapshotlatestoperationcompletetime,omitempty"`
}

// OperationDetails stores the details of the operation performed on a volume.
type OperationDetails struct {
	// TaskInvocationTimestamp represents the time at which the task was invoked.
	// This timestamp is derived from the cluster and may not correspond to the
	// task invocation timestamp on CNS.
	TaskInvocationTimestamp metav1.Time `json:"taskInvocationTimestamp"`
	// TaskID stores the task for an operation that was invoked on CNS for a volume.
	TaskID string `json:"taskId"`
	// vCenter server on which the task is created
	VCenterServer string `json:"vCenterServer,omitempty"`
	// OpID stores the OpID for a task that was invoked on CNS for a volume.
	OpID string `json:"opId,omitempty"`
	// TaskStatus describes the current status of the task invoked on CNS.
	// Valid strings are "In Progress", "Successful", "PartiallyFailed" and "Failed".
	TaskStatus string `json:"taskStatus,omitempty"`
	// Error represents the error returned if the task fails on CNS.
	// Defaults to empty string.
	Error string `json:"error,omitempty"`
}

//+kubebuilder:object:root=true
//+kubebuilder:subresource:status

// CnsVolumeOperationRequest is the Schema for the cnsvolumeoperationrequests API
type CnsVolumeOperationRequest struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec   CnsVolumeOperationRequestSpec   `json:"spec,omitempty"`
	Status CnsVolumeOperationRequestStatus `json:"status,omitempty"`
}

//+kubebuilder:object:root=true

// CnsVolumeOperationRequestList contains a list of CnsVolumeOperationRequest
type CnsVolumeOperationRequestList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []CnsVolumeOperationRequest `json:"items"`
}
