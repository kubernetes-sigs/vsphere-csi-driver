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

// DownloadBuilder builds Download objects
type DownloadBuilder struct {
	object *velerov1api.Download
}

// ForDownload is the constructor for a DownloadBuilder.
func ForDownload(ns, name string) *DownloadBuilder {
	return &DownloadBuilder{
		object: &velerov1api.Download{
			TypeMeta: metav1.TypeMeta{
				APIVersion: velerov1api.SchemeGroupVersion.String(),
				Kind:       "Download",
			},
			ObjectMeta: metav1.ObjectMeta{
				Namespace: ns,
				Name:      name,
				Labels:    AppendVeleroExcludeLabels(nil),
			},
		},
	}
}

// Result returns the built Download.
func (b *DownloadBuilder) Result() *velerov1api.Download {
	return b.object
}

// ObjectMeta applies functional options to the Download's ObjectMeta.
func (b *DownloadBuilder) ObjectMeta(opts ...ObjectMetaOpt) *DownloadBuilder {
	for _, opt := range opts {
		opt(b.object)
	}

	return b
}

// RestoreTimestamp sets the start time of download creation.
func (b *DownloadBuilder) RestoreTimestamp(val time.Time) *DownloadBuilder {
	b.object.Spec.RestoreTimestamp = &metav1.Time{Time: val}
	return b
}

// Phase sets the Download's phase.
func (b *DownloadBuilder) Phase(phase velerov1api.DownloadPhase) *DownloadBuilder {
	b.object.Status.Phase = phase
	return b
}

// VolumeID sets the identifier for the restored volume in the Status.
func (b *DownloadBuilder) VolumeID(id string) *DownloadBuilder {
	b.object.Status.VolumeID = id
	return b
}

// ProtectedEntityID sets the identifier for the to be restored
// protected entity in the Spec.
func (b *DownloadBuilder) ProtectedEntityID(id string) *DownloadBuilder {
	b.object.Spec.ProtectedEntityID = id
	return b
}

// BackupRepositoryName sets the backuprepository name for the to be restored volume.
func (b *DownloadBuilder) BackupRepositoryName(brName string) *DownloadBuilder {
	b.object.Spec.BackupRepositoryName = brName
	return b
}

// SnapshotID sets the Download's snapshot ID.
func (b *DownloadBuilder) SnapshotID(snapshotID string) *DownloadBuilder {
	b.object.Spec.SnapshotID = snapshotID
	return b
}

// StartTimestamp sets the Download's start timestamp.
func (b *DownloadBuilder) StartTimestamp(val time.Time) *DownloadBuilder {
	b.object.Status.StartTimestamp = &metav1.Time{Time: val}
	return b
}

// CompletionTimestamp sets the Download's end timestamp.
func (b *DownloadBuilder) CompletionTimestamp(val time.Time) *DownloadBuilder {
	b.object.Status.CompletionTimestamp = &metav1.Time{Time: val}
	return b
}

// ProcessingNode sets the DataManager node that has
// picked up the Download for processing.
func (b *DownloadBuilder) ProcessingNode(node string) *DownloadBuilder {
	b.object.Status.ProcessingNode = node
	return b
}

// Retry sets the number of retry time.
func (b *DownloadBuilder) Retry(cnt int32) *DownloadBuilder {
	b.object.Status.RetryCount = cnt
	return b
}

// NextRetryTimestamp sets the timestamp for next retry.
func (b *DownloadBuilder) NextRetryTimestamp(val time.Time) *DownloadBuilder {
	b.object.Status.NextRetryTimestamp = &metav1.Time{Time: val}
	return b
}

// CloneFromSnapshotReference sets the reference to the clonefromsnapshot.
func (b *DownloadBuilder) CloneFromSnapshotReference(cloneFromSnapshotRef string) *DownloadBuilder {
	b.object.Spec.CloneFromSnapshotReference = cloneFromSnapshotRef
	return b
}
