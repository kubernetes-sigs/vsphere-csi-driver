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

package datamover

import (
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	velerov1api "sigs.k8s.io/vsphere-csi-driver/v2/pkg/apis/datamover/v1alpha1"
	"time"
)

// UploadBuilder builds Upload objects
type UploadBuilder struct {
	object *velerov1api.Upload
}

// ForUpload is the constructor for a UploadBuilder.
func ForUpload(ns, name string) *UploadBuilder {
	return &UploadBuilder{
		object: &velerov1api.Upload{
			TypeMeta: metav1.TypeMeta{
				APIVersion: velerov1api.SchemeGroupVersion.String(),
				Kind:       "Upload",
			},
			ObjectMeta: metav1.ObjectMeta{
				Namespace: ns,
				Name:      name,
				Labels:    AppendVeleroExcludeLabels(nil),
			},
		},
	}
}

// Result returns the built Upload.
func (b *UploadBuilder) Result() *velerov1api.Upload {
	return b.object
}

// ObjectMeta applies functional options to the Upload's ObjectMeta.
func (b *UploadBuilder) ObjectMeta(opts ...ObjectMetaOpt) *UploadBuilder {
	for _, opt := range opts {
		opt(b.object)
	}

	return b
}

// BackupTimestamp sets the start time of upload creation.
func (b *UploadBuilder) BackupTimestamp(val time.Time) *UploadBuilder {
	b.object.Spec.BackupTimestamp = &metav1.Time{Time: val}
	return b
}

// Phase sets the Upload's phase.
func (b *UploadBuilder) Phase(phase velerov1api.UploadPhase) *UploadBuilder {
	b.object.Status.Phase = phase
	return b
}

// SnapshotID sets the Upload's snapshot ID.
func (b *UploadBuilder) SnapshotID(snapshotID string) *UploadBuilder {
	b.object.Spec.SnapshotID = snapshotID
	return b
}

// StartTimestamp sets the Upload's start timestamp.
func (b *UploadBuilder) StartTimestamp(val time.Time) *UploadBuilder {
	b.object.Status.StartTimestamp = &metav1.Time{Time: val}
	return b
}

// CompletionTimestamp sets the Upload's end timestamp.
func (b *UploadBuilder) CompletionTimestamp(val time.Time) *UploadBuilder {
	b.object.Status.CompletionTimestamp = &metav1.Time{Time: val}
	return b
}

// ProcessingNode sets the DataManager node that has
// picked up the Upload for processing.
func (b *UploadBuilder) ProcessingNode(node string) *UploadBuilder {
	b.object.Status.ProcessingNode = node
	return b
}

// Retry sets the number of retry time.
func (b *UploadBuilder) Retry(cnt int32) *UploadBuilder {
	b.object.Status.RetryCount = cnt
	return b
}

// NextRetryTimestamp sets the timestamp for next retry.
func (b *UploadBuilder) NextRetryTimestamp(val time.Time) *UploadBuilder {
	b.object.Status.NextRetryTimestamp = &metav1.Time{Time: val}
	return b
}

// CurrentBackOff sets the current backoff for upload retry.
func (b *UploadBuilder) CurrentBackOff(backoff int32) *UploadBuilder {
	b.object.Status.CurrentBackOff = backoff
	return b
}

// BackupRepository sets the backup repository for upload.
func (b *UploadBuilder) BackupRepositoryName(backuprepo string) *UploadBuilder {
	b.object.Spec.BackupRepositoryName = backuprepo
	return b
}

// SnapshotReference sets the reference to the snapshot.
func (b *UploadBuilder) SnapshotReference(snapshotRef string) *UploadBuilder {
	b.object.Spec.SnapshotReference = snapshotRef
	return b
}
