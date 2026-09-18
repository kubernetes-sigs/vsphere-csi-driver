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
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// CnsNfsVolumeInformationSpec defines the desired state of CnsNfsVolumeInformation.
// +k8s:openapi-gen=true
type CnsNfsVolumeInformationSpec struct {
	// VKSClusterName is the display name of the owning VKS cluster.
	VKSClusterName string `json:"vksClusterName"`

	// VKSClusterID is the stable UID of the owning VKS cluster (guest cluster UID).
	VKSClusterID string `json:"vksClusterID"`

	// SupervisorNamespace is the vSphere Namespace this CR (and the VKS cluster) lives in.
	SupervisorNamespace string `json:"supervisorNamespace"`

	// Volumes is keyed by the guest PVC's UID (stable across renames; the CNS table's
	// VolumeID is derived from this key - see "Table schema alignment" below).
	// +kubebuilder:validation:Type=object
	// +kubebuilder:pruning:PreserveUnknownFields
	Volumes map[string]NfsVolumeEntry `json:"volumes"`
}

// NfsVolumeEntry describes a single NFS-backed guest PVC tracked by a
// CnsNfsVolumeInformation CR.
type NfsVolumeEntry struct {
	// VKSNamespace is the namespace of the PVC/Pod inside the VKS cluster (distinct from
	// SupervisorNamespace, which is set once at the Spec level, not per-entry).
	VKSNamespace string `json:"vksNamespace"`

	// ClaimName is the guest PVC's name.
	ClaimName string `json:"claimName"`

	// PodNames lists every pod currently mounting this volume. A list, not a single name -
	// RWX volumes are routinely mounted by multiple pods on multiple nodes simultaneously.
	// +optional
	PodNames []string `json:"podNames,omitempty"`

	// Health mirrors the volumehealth.storage.kubernetes.io/health annotation value
	// (see the Volume Health proposal) - "accessible" / "inaccessible".
	Health string `json:"health"`

	// NFSSharePath is "server:/share/subdirectory" - everything needed to identify the
	// backing export, already derivable from the volume handle at CreateVolume time.
	NFSSharePath string `json:"nfsSharePath"`

	// Labels are copied from the PVC's own labels at sync time.
	// +optional
	Labels map[string]string `json:"labels,omitempty"`

	// Capacity is the PVC's requested/bound size, so the CNS table doesn't need a
	// separate lookup to answer "how big is this volume."
	// +optional
	Capacity *resource.Quantity `json:"capacity,omitempty"`

	// NFSVersion is read from the StorageClass's mountOptions (e.g. "4.1"); left empty
	// if the StorageClass never set nfsvers explicitly, rather than guessing a default.
	// +optional
	NFSVersion string `json:"nfsVersion,omitempty"`

	// ClaimStatus is the PVC phase (Bound/Pending/Terminating) - kept distinct from
	// Health, which is reachability, not Kubernetes binding state. Conflating the two
	// was flagged earlier as a schema ambiguity worth avoiding.
	ClaimStatus string `json:"claimStatus"`

	// LastUpdatedTime lets a stale entry (not touched in N hours) be treated as a
	// staleness/health signal by the relay controller, independent of the Health field.
	LastUpdatedTime metav1.Time `json:"lastUpdatedTime"`
}

// CnsNfsVolumeInformationStatus defines the observed state of CnsNfsVolumeInformation.
// +k8s:openapi-gen=true
type CnsNfsVolumeInformationStatus struct {
	// SyncedVolumeIDs records which entries the relay controller has confirmed written to
	// the CNS table, so pvCSI (or an observer) can tell sync lag from sync failure.
	// +optional
	SyncedVolumeIDs []string `json:"syncedVolumeIDs,omitempty"`

	// SyncedVolumeHashes is keyed by volumeID and records a content hash of the entry as
	// of the last successful relay. The reconciler compares this against the current
	// entry's hash to decide RegisterNfsVolumeInfo (key absent) vs UpdateNfsVolumeInfo
	// (key present but hash differs) vs no-op (hash unchanged) - a plain
	// "already in SyncedVolumeIDs" check can't tell "registered once" from "registered
	// but now stale," which the Health/pod-attach-changes flow requires.
	// +optional
	SyncedVolumeHashes map[string]string `json:"syncedVolumeHashes,omitempty"`

	// Conditions surfaces relay failures (e.g. CNS API unreachable) without pvCSI needing
	// to know anything about the CNS API itself.
	// +optional
	Conditions []metav1.Condition `json:"conditions,omitempty"`
}

// +genclient
// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object
// +kubebuilder:object:root=true
// +k8s:openapi-gen=true
// +kubebuilder:subresource:status
// +kubebuilder:resource:scope=Namespaced,shortName=cnsnfsvolinfo,path=cnsnfsvolumeinformations,singular=cnsnfsvolumeinformation
// +kubebuilder:printcolumn:name="VKSCluster",type=string,JSONPath=`.spec.vksClusterName`
// +kubebuilder:printcolumn:name="Age",type=date,JSONPath=`.metadata.creationTimestamp`

// CnsNfsVolumeInformation is the Schema for the cnsnfsvolumeinformations API.
// Group: cns.vmware.com, Version: v1alpha1, Kind: CnsNfsVolumeInformation.
//
// Naming convention: <vksClusterName>-<vksClusterID>-nfsvolumes, one per VKS cluster,
// created in that cluster's Supervisor Namespace.
type CnsNfsVolumeInformation struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec   CnsNfsVolumeInformationSpec   `json:"spec"`
	Status CnsNfsVolumeInformationStatus `json:"status,omitempty"`
}

// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object
// +kubebuilder:object:root=true

// CnsNfsVolumeInformationList contains a list of CnsNfsVolumeInformation.
type CnsNfsVolumeInformationList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []CnsNfsVolumeInformation `json:"items"`
}
