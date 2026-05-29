// Copyright (c) 2026 Broadcom. All Rights Reserved.
// Broadcom Confidential. The term "Broadcom" refers to Broadcom Inc.
// and/or its subsidiaries.
// SPDX-License-Identifier: Apache-2.0

package v1alpha3

import (
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// QuotaUsageDetails gives reserved and used quota details.
type QuotaUsageDetails struct {
	// reserved is the storage quota held for in-flight provisioning (bytes).
	// +optional
	Reserved *resource.Quantity `json:"reserved,omitempty"`

	// used is the storage quota already consumed (bytes).
	// +optional
	Used *resource.Quantity `json:"used,omitempty"`
}

// +kubebuilder:validation:MaxItems=256
type StoragePolicyLevelQuotaStatusList []StoragePolicyLevelQuotaStatus

// StoragePolicyLevelQuotaStatus reports usage for one storage policy in a namespace.
type StoragePolicyLevelQuotaStatus struct {
	// storagePolicyId is the vSphere storage policy identifier.
	// +required
	// +kubebuilder:validation:MinLength=1
	// +kubebuilder:validation:MaxLength=128
	StoragePolicyId string `json:"storagePolicyId,omitempty"`

	// volumeClassName is the K8s-compliant name derived from storage policy name
	// to ensure unique identification across multiple vCenters.
	// +optional
	// +kubebuilder:validation:MaxLength=253
	//nolint:kubeapilinter
	VolumeClassName string `json:"volumeClassName,omitempty"`

	// policyQuotaUsage reports reserved and used quota for this storage policy.
	// +optional
	//nolint:kubeapilinter
	StoragePolicyLevelQuotaUsage QuotaUsageDetails `json:"policyQuotaUsage,omitempty"`
}

// StorageQuotaSpec defines the desired state of StorageQuota.
type StorageQuotaSpec struct {
	// limit is the desired storage limit (bytes) across vSphere storage resource types in this namespace.
	// +optional
	Limit *resource.Quantity `json:"limit,omitempty"`
	// storagePolicyLevelLimits maps storage policy IDs to per-policy quota limits (bytes).
	// +optional
	//nolint:kubeapilinter
	StoragePolicyLevelLimits map[string]*resource.Quantity `json:"storagePolicyLevelLimits,omitempty"`
	// parentStorageResourceEnvelopeId links this namespace quota to a logical storage
	// resource envelope (tenant/project id).
	// +optional
	// +kubebuilder:validation:MaxLength=256
	//nolint:kubeapilinter // empty string means unset; API uses value type for compatibility
	ParentStorageResourceEnvelopeId string `json:"parentStorageResourceEnvelopeId,omitempty"`
}

// StorageQuotaStatus defines the observed state of StorageQuota.
type StorageQuotaStatus struct {
	// total reports quota usage per storage policy in this namespace.
	// +optional
	//nolint:kubeapilinter // match v1alpha1/v1alpha2: no x-kubernetes-list-type on status.total
	StoragePolicyLevelQuotaStatuses StoragePolicyLevelQuotaStatusList `json:"total,omitempty"`
	// appliedLimit is the applied storage limit (bytes) across vSphere storage resource types.
	// +optional
	AppliedLimit *resource.Quantity `json:"appliedLimit,omitempty"`
	// appliedStoragePolicyLevelLimits maps storage policy IDs to applied per-policy limits (bytes).
	// +optional
	//nolint:kubeapilinter
	AppliedStoragePolicyLevelLimits map[string]*resource.Quantity `json:"appliedStoragePolicyLevelLimits,omitempty"`
}

// +kubebuilder:object:root=true
// +kubebuilder:subresource:status
// +kubebuilder:storageversion

// StorageQuota is the Schema for the storagequotas API.
type StorageQuota struct {
	metav1.TypeMeta `json:",inline"`
	// metadata is the standard Kubernetes object metadata.
	// +optional
	metav1.ObjectMeta `json:"metadata,omitempty"`

	// spec defines the desired state of this StorageQuota.
	// +optional
	//nolint:kubeapilinter // Kubernetes CRD convention: embedded spec, not a pointer
	Spec StorageQuotaSpec `json:"spec,omitempty"`
	// status defines the observed state of this StorageQuota.
	// +optional
	//nolint:kubeapilinter // Kubernetes CRD convention: embedded status, not a pointer
	Status StorageQuotaStatus `json:"status,omitempty"`
}

// +kubebuilder:object:root=true

// StorageQuotaList contains a list of StorageQuota.
type StorageQuotaList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []StorageQuota `json:"items"`
}

func init() {
	objectTypes = append(objectTypes, &StorageQuota{}, &StorageQuotaList{})
}
