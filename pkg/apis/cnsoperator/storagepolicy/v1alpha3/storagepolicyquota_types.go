// Copyright (c) 2026 Broadcom. All Rights Reserved.
// Broadcom Confidential. The term "Broadcom" refers to Broadcom Inc.
// and/or its subsidiaries.
// SPDX-License-Identifier: Apache-2.0

package v1alpha3

import (
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// +kubebuilder:validation:MaxItems=256
type SCLevelQuotaStatusList []SCLevelQuotaStatus

// SCLevelQuotaStatus gives storage quota usage per Kubernetes storage class.
type SCLevelQuotaStatus struct {
	// storageClassName is the Kubernetes StorageClass name for the SPBM policy.
	// +optional
	// +kubebuilder:validation:MaxLength=253
	//nolint:kubeapilinter
	StorageClassName string `json:"storageClassName,omitempty"`

	// scQuotaUsage reports quota usage for this storage class.
	// +optional
	SCLevelQuotaUsage *QuotaUsageDetails `json:"scQuotaUsage,omitempty"`
}

// +kubebuilder:validation:MaxItems=256
type VACLevelQuotaStatusList []VACLevelQuotaStatus

// VACLevelQuotaStatus gives storage quota usage per Kubernetes volume attributes
// class.
type VACLevelQuotaStatus struct {
	// volumeAttributesClassName is the Kubernetes VolumeAttributesClass name for
	// the SPBM policy.
	// +optional
	// +kubebuilder:validation:MaxLength=253
	//nolint:kubeapilinter
	VolumeAttributesClassName string `json:"volumeAttributesClassName,omitempty"`

	// vacQuotaUsage reports quota usage for this volume attributes class.
	// +optional
	VACLevelQuotaUsage *QuotaUsageDetails `json:"vacQuotaUsage,omitempty"`
}

// +kubebuilder:validation:MaxItems=256
type ResourceTypeLevelQuotaStatusList []ResourceTypeLevelQuotaStatus

type ResourceTypeLevelQuotaStatus struct {
	// extensionName is the service extension name for the storage resource type.
	// +required
	// +kubebuilder:validation:MinLength=1
	// +kubebuilder:validation:MaxLength=253
	ResourceExtensionName string `json:"extensionName,omitempty"`

	// extensionQuotaUsage reports usage per storage class for this extension.
	// +optional
	//nolint:kubeapilinter // match prior CRDs: no x-kubernetes-list-type on these lists
	ResourceTypeSCLevelQuotaStatuses SCLevelQuotaStatusList `json:"extensionQuotaUsage,omitempty"`

	// extensionVACQuotaUsage reports usage per volume attributes class for this
	// extension.
	// +optional
	//nolint:kubeapilinter // match prior CRDs: no x-kubernetes-list-type on these lists
	ResourceTypeVACLevelQuotaStatuses VACLevelQuotaStatusList `json:"extensionVACQuotaUsage,omitempty"`
}

// StoragePolicyQuotaSpec defines the desired state of StoragePolicyQuota.
type StoragePolicyQuotaSpec struct {
	// storagePolicyId is the vSphere storage policy identifier.
	// +required
	// +kubebuilder:validation:MinLength=1
	// +kubebuilder:validation:XValidation:rule="self == oldSelf",message="StoragePolicyId is immutable"
	// +kubebuilder:validation:MaxLength=128
	StoragePolicyId string `json:"storagePolicyId,omitempty"`

	// volumeClassName is the K8s-compliant name derived from storage policy name
	// to ensure unique identification across multiple vCenters.
	// +optional
	// +kubebuilder:validation:XValidation:rule="oldSelf == '' || self == oldSelf",message="VolumeClassName is immutable"
	// +kubebuilder:validation:MaxLength=253
	//nolint:kubeapilinter
	VolumeClassName string `json:"volumeClassName,omitempty"`

	// limit is the desired storage quota limit (bytes) across resource types for
	// this policy in the namespace.
	// +optional
	Limit *resource.Quantity `json:"limit,omitempty"`
}

// StoragePolicyQuotaStatus defines the observed state of StoragePolicyQuota.
type StoragePolicyQuotaStatus struct {
	// total reports quota usage per Kubernetes storage class.
	// +optional
	//nolint:kubeapilinter // match prior CRDs: no x-kubernetes-list-type on these lists
	SCLevelQuotaStatuses SCLevelQuotaStatusList `json:"total,omitempty"`

	// totalVAC reports quota usage per Kubernetes volume attributes class.
	// +optional
	//nolint:kubeapilinter // match prior CRDs: no x-kubernetes-list-type on these lists
	VACLevelQuotaStatuses VACLevelQuotaStatusList `json:"totalVAC,omitempty"`

	// extensions reports quota usage per resource extension.
	// +optional
	//nolint:kubeapilinter // match prior CRDs: no x-kubernetes-list-type on these lists
	ResourceTypeLevelQuotaStatuses ResourceTypeLevelQuotaStatusList `json:"extensions,omitempty"`

	// appliedLimit is the limit the controller has applied (bytes).
	// +optional
	AppliedLimit *resource.Quantity `json:"appliedLimit,omitempty"`
}

// +kubebuilder:object:root=true
// +kubebuilder:subresource:status
// +kubebuilder:storageversion

// StoragePolicyQuota is the Schema for the storagepolicyquotas API.
type StoragePolicyQuota struct {
	metav1.TypeMeta `json:",inline"`
	// metadata is the standard Kubernetes object metadata.
	// +optional
	metav1.ObjectMeta `json:"metadata,omitempty"`

	// spec defines the desired state of this StoragePolicyQuota.
	// +optional
	//nolint:kubeapilinter // Kubernetes CRD convention: embedded spec, not a pointer
	Spec StoragePolicyQuotaSpec `json:"spec,omitempty"`
	// status defines the observed state of this StoragePolicyQuota.
	// +optional
	//nolint:kubeapilinter // Kubernetes CRD convention: embedded status, not a pointer
	Status StoragePolicyQuotaStatus `json:"status,omitempty"`
}

// +kubebuilder:object:root=true

// StoragePolicyQuotaList contains a list of StoragePolicyQuota.
type StoragePolicyQuotaList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []StoragePolicyQuota `json:"items"`
}

func init() {
	objectTypes = append(objectTypes, &StoragePolicyQuota{}, &StoragePolicyQuotaList{})
}
