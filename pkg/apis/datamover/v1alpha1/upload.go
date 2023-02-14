/*
Copyright 2020 the Velero contributors.

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
	meta_v1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// UploadSpec is the specification for Upload resource
type UploadSpec struct {
	// SnapshotID is the identifier for the snapshot of the volume.
	SnapshotID string `json:"snapshotID,omitempty"`

	// BackupTimestamp records the time the backup was called.
	// The server's time is used for SnapshotTimestamp
	BackupTimestamp *meta_v1.Time `json:"backupTimestamp,omitempty"`

	// UploadCancel indicates request to cancel ongoing upload.
	UploadCancel bool `json:"uploadCancel,omitempty"`

	// BackupRepository provides backup repository info for upload. Used for
	// multiple backup repository.
	BackupRepositoryName string `json:"backupRepository,omitempty"`

	// SnapshotReference is the namespace and snapshot name for this upload request.
	// The format is SnapshotNamespace/SnapshotCRName
	// It is used to update the upload status in the snapshot.
	// +optional
	SnapshotReference string `json:"snapshotReference,omitempty"`
}

// UploadPhase represents the lifecycle phase of a Upload.
// +kubebuilder:validation:Enum=New;InProgress;Completed;UploadError;CleanupFailed;Canceled;Canceling;
type UploadPhase string

const (
	UploadPhaseNew           UploadPhase = "New"
	UploadPhaseInProgress    UploadPhase = "InProgress"
	UploadPhaseCompleted     UploadPhase = "Completed"
	UploadPhaseUploadError   UploadPhase = "UploadError"
	UploadPhaseCleanupFailed UploadPhase = "CleanupFailed"
	UploadPhaseCanceling     UploadPhase = "Canceling"
	UploadPhaseCanceled      UploadPhase = "Canceled"
)

// UploadStatus is the current status of a Upload.
type UploadStatus struct {
	// Phase is the current state of the Upload.
	// +optional
	Phase UploadPhase `json:"phase,omitempty"`

	// Message is a message about the upload's status.
	// +optional
	Message string `json:"message,omitempty"`

	// StartTimestamp records the time an upload was started.
	// The server's time is used for StartTimestamps
	// +optional
	// +nullable
	StartTimestamp *meta_v1.Time `json:"startTimestamp,omitempty"`

	// CompletionTimestamp records the time an upload was completed.
	// Completion time is recorded even on failed uploads.
	// The server's time is used for CompletionTimestamps
	// +optional
	// +nullable
	CompletionTimestamp *meta_v1.Time `json:"completionTimestamp,omitempty"`

	// Progress holds the total number of bytes of the volume and the current
	// number of backed up bytes. This can be used to display progress information
	// about the backup operation.
	// +optional
	Progress UploadOperationProgress `json:"progress,omitempty"`

	// The DataManager node that has picked up the Upload for processing.
	// This will be updated as soon as the Upload is picked up for processing.
	// If the DataManager couldn't process Upload for some reason it will be picked up by another
	// node.
	ProcessingNode string `json:"processingNode,omitempty"`

	// RetryCount records the number of retry times for adding a failed Upload which failed due to
	// network issue back to queue. Used for user tracking and debugging.
	// +optional
	RetryCount int32 `json:"retryCount,omitempty"`

	// NextRetryTimestamp should be the timestamp that indicate the next retry for failed upload CR.
	// Used to filter out the upload request which comes in before next retry time.
	// +optional
	// +nullable
	NextRetryTimestamp *meta_v1.Time `json:"nextRetryTimestamp,omitempty"`

	// CurrentBackOff records the backoff on retry for failed upload. Retry on upload should obey
	// exponential backoff mechanism.
	// +optional
	CurrentBackOff int32 `json:"currentBackOff,omitempty"`
}

// UploadOperationProgress represents the progress of a
// Upload operation
type UploadOperationProgress struct {
	// +optional
	TotalBytes int64 `json:"totalBytes,omitempty"`

	// +optional
	BytesDone int64 `json:"bytesDone,omitempty"`
}

// +genclient
// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object

// Upload describe a velero-plugin backup
type Upload struct {
	// TypeMeta is the metadata for the resource, like kind and apiversion
	meta_v1.TypeMeta `json:",inline"`

	// +optional
	meta_v1.ObjectMeta `json:"metadata,omitempty"`

	// Spec is the custom resource spec
	Spec UploadSpec `json:"spec"`

	// +optional
	Status UploadStatus `json:"status,omitempty"`
}

// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object

// UploadList is a list of Upload resources
type UploadList struct {
	meta_v1.TypeMeta `json:",inline"`

	// +optional
	meta_v1.ListMeta `json:"metadata,omitempty"`

	Items []Upload `json:"items"`
}
