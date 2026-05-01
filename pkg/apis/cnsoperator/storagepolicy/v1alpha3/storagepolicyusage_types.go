// Copyright (c) 2026 Broadcom. All Rights Reserved.
// Broadcom Confidential. The term "Broadcom" refers to Broadcom Inc.
// and/or its subsidiaries.
// SPDX-License-Identifier: Apache-2.0

package v1alpha3

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

// StoragePolicyUsageSpec defines the desired state of StoragePolicyUsage.
type StoragePolicyUsageSpec struct {
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
	//nolint:kubeapilinter // empty string means unset; API uses value type for compatibility
	VolumeClassName string `json:"volumeClassName,omitempty"`

	// storageClassName is the Kubernetes StorageClass name for the SPBM policy.
	// Can be set to empty string when using volumeAttributesClassName instead.
	// +required
	// +kubebuilder:validation:XValidation:rule="self == oldSelf",message="StorageClassName is immutable"
	// +kubebuilder:validation:MaxLength=64
	//nolint:kubeapilinter // empty string is valid when using volumeAttributesClassName
	// API uses value type for compatibility
	StorageClassName string `json:"storageClassName"`

	// volumeAttributesClassName is the Kubernetes VolumeAttributesClass name for
	// the SPBM policy.
	// VolumeAttributesClassName is mutable.
	// +optional
	// +kubebuilder:validation:MaxLength=253
	//nolint:kubeapilinter // empty string means unset; API uses value type for compatibility
	VolumeAttributesClassName string `json:"volumeAttributesClassName,omitempty"`

	// resourceApiGroup is the API group of the referenced resource, if not core.
	// +optional
	// +kubebuilder:validation:XValidation:rule="self == oldSelf",message="ResourceAPIgroup is immutable"
	// +kubebuilder:validation:MaxLength=253
	ResourceAPIgroup *string `json:"resourceApiGroup,omitempty"`

	// resourceKind is the kind of the referenced resource.
	// +required
	// +kubebuilder:validation:MinLength=1
	// +kubebuilder:validation:XValidation:rule="self == oldSelf",message="ResourceKind is immutable"
	// +kubebuilder:validation:MaxLength=64
	ResourceKind string `json:"resourceKind,omitempty"`

	// resourceExtensionName is the service extension name for this usage record.
	// +required
	// +kubebuilder:validation:MinLength=1
	// +kubebuilder:validation:XValidation:rule="self == oldSelf",message="ResourceExtensionName is immutable"
	// +kubebuilder:validation:MaxLength=253
	ResourceExtensionName string `json:"resourceExtensionName,omitempty"`

	// resourceExtensionNamespace is the namespace of the referenced extension resource, if any.
	// +optional
	// +kubebuilder:validation:XValidation:rule="self == oldSelf",message="ResourceExtensionNamespace is immutable"
	// +kubebuilder:validation:MaxLength=253
	//nolint:kubeapilinter // empty string means unset; API uses value type for compatibility
	ResourceExtensionNamespace string `json:"resourceExtensionNamespace,omitempty"`

	// caBundle holds PEM-encoded CA certificates for extension admission.
	// +optional
	// +kubebuilder:validation:MaxLength=1048576
	CABundle []byte `json:"caBundle,omitempty"`
}

// StoragePolicyUsageStatus defines the observed state of StoragePolicyUsage.
type StoragePolicyUsageStatus struct {
	// quotaUsage reports usage for this storage policy usage object.
	// +optional
	ResourceTypeLevelQuotaUsage *QuotaUsageDetails `json:"quotaUsage,omitempty"`
}

// +kubebuilder:object:root=true
// +kubebuilder:storageversion
// +kubebuilder:subresource:status

// StoragePolicyUsage is the Schema for the storagepolicyusages API.
// Each resource in a namespace has two StoragePolicyUsage objects: one for
// StorageClass (storageClassName set) and one for VolumeAttributesClass
// (storageClassName empty, volumeAttributesClassName set).
type StoragePolicyUsage struct {
	metav1.TypeMeta `json:",inline"`
	// metadata is the standard Kubernetes object metadata.
	// +optional
	metav1.ObjectMeta `json:"metadata,omitempty"`

	// spec defines the desired state of this StoragePolicyUsage.
	// +optional
	//nolint:kubeapilinter // Kubernetes CRD convention: embedded spec, not a pointer
	Spec StoragePolicyUsageSpec `json:"spec,omitempty"`
	// status defines the observed state of this StoragePolicyUsage.
	// +optional
	//nolint:kubeapilinter // Kubernetes CRD convention: embedded status, not a pointer
	Status StoragePolicyUsageStatus `json:"status,omitempty"`
}

// +kubebuilder:object:root=true

// StoragePolicyUsageList contains a list of StoragePolicyUsage.
type StoragePolicyUsageList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []StoragePolicyUsage `json:"items"`
}

func init() {
	objectTypes = append(objectTypes, &StoragePolicyUsage{}, &StoragePolicyUsageList{})
}
