// Copyright (c) 2023 VMware, Inc. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

package v1alpha1

import (
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

const (
	CRDSingular = "storagepolicyusage"
	// NameSuffixForPVC is the suffix used to name instances of StoragePolicyUsage created for PVCs.
	NameSuffixForPVC = "pvc-usage"
	// NameSuffixForSnapshot is the suffix used to name instances of StoragePolicyUsage created for VolumeSnapshots.
	NameSuffixForSnapshot = "snapshot-usage"
)

// StoragePolicyUsageSpec defines the desired state of StoragePolicyUsage
type StoragePolicyUsageSpec struct {
	// +kubebuilder:validation:Required
	// +kubebuilder:validation:XValidation:rule="self == oldSelf",message="StoragePolicyId is immutable"
	// +kubebuilder:validation:MaxLength=128

	// ID of the storage policy
	StoragePolicyId string `json:"storagePolicyId"`

	// +kubebuilder:validation:Required
	// +kubebuilder:validation:XValidation:rule="self == oldSelf",message="StorageClassName is immutable"
	// +kubebuilder:validation:MaxLength=64

	// name of K8S storage class associated with given storage policy
	StorageClassName string `json:"storageClassName"`

	// +kubebuilder:validation:Optional
	// +kubebuilder:validation:XValidation:rule="self == oldSelf",message="ResourceAPIgroup is immutable"

	// APIGroup is the group for the resource being referenced.
	// If it is not specified, the specified ResourceKind must be in the core API group.
	// For resources not in the core API group, this field is required.
	// +optional
	ResourceAPIgroup *string `json:"resourceApiGroup,omitempty"`

	// +kubebuilder:validation:Required
	// +kubebuilder:validation:XValidation:rule="self == oldSelf",message="ResourceKind is immutable"
	// +kubebuilder:validation:MaxLength=64

	// Type of resource being referenced
	ResourceKind string `json:"resourceKind"`

	// +kubebuilder:validation:Required
	// +kubebuilder:validation:XValidation:rule="self == oldSelf",message="ResourceExtensionName is immutable"

	// Name of service extension for given storage resource type
	ResourceExtensionName string `json:"resourceExtensionName"`
}

// StoragePolicyUsageStatus defines the observed state of StoragePolicyUsage
type StoragePolicyUsageStatus struct {
	// Storage usage details per storage object type for given storage policy
	// +optional
	ResourceTypeLevelQuotaUsage *QuotaUsageDetails `json:"quotaUsage"`
}

//+kubebuilder:object:root=true
//+kubebuilder:subresource:status

// StoragePolicyUsage is the Schema for the storagepolicyusages API
type StoragePolicyUsage struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec   StoragePolicyUsageSpec   `json:"spec,omitempty"`
	Status StoragePolicyUsageStatus `json:"status,omitempty"`
}

//+kubebuilder:object:root=true

// StoragePolicyUsageList contains a list of StoragePolicyUsage
type StoragePolicyUsageList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []StoragePolicyUsage `json:"items"`
}
