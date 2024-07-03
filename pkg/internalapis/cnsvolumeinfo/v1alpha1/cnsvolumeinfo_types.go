/*
Copyright 2022 The Kubernetes Authors.

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

const (
	// CRDSingular represents the singular name of CNSVolumeInfo CRD.
	CRDSingular = "cnsvolumeinfo"
)

// CNSVolumeInfoSpec defines the desired state of CNSVolumeInfo
type CNSVolumeInfoSpec struct {

	// VolumeID is the FCD ID obtained from creating volume using CNS API.
	VolumeID string `json:"volumeID"`

	// Namespace of the PersistentVolumeClaim.
	Namespace string `json:"namespace,omitempty"`

	// vCenterServer is the IP/FQDN of the vCenter host on which the CNS volume is accessible.
	VCenterServer string `json:"vCenterServer"`

	// ID of the storage policy
	StoragePolicyID string `json:"storagePolicyID,omitempty"`

	// Name of the storage class
	StorageClassName string `json:"storageClassName,omitempty"`

	// Capacity stores the current capacity of the PersistentVolume this volume represents.
	Capacity *resource.Quantity `json:"capacity,omitempty"`

	// ValidAggregatedSnapshotSize defines if the presented AggregatedSnapshotSize is valid.
	ValidAggregatedSnapshotSize bool `json:"validaggregatedsnapshotsize"`

	// AggregatedSnapshotSize stores the aggregate snapshot size for volume.
	AggregatedSnapshotSize *resource.Quantity `json:"aggregatedsnapshotsize,omitempty"`

	// These are Zones of the volume post provisioning.
	Zones []string `json:"zones"`

	// Associated time stamp of the create or delete snapshot task completion.
	// This is used to ordering concurrent snapshots on same volume.
	SnapshotLatestOperationCompleteTime metav1.Time `json:"snapshotlatestoperationcompletetime"`
}

//+kubebuilder:object:root=true
//+kubebuilder:subresource:status

// CNSVolumeInfo is the Schema for the cnsvolumeinfoes API
type CNSVolumeInfo struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec CNSVolumeInfoSpec `json:"spec"`
}

//+kubebuilder:object:root=true

// CNSVolumeInfoList contains a list of CNSVolumeInfo
type CNSVolumeInfoList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []CNSVolumeInfo `json:"items"`
}
