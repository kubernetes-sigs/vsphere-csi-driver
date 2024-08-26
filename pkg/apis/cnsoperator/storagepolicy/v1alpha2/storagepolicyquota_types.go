/*
Copyright 2019 The Kubernetes Authors.

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

package v1alpha2

import (
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// QuotaUsageDetails gives reserved and used quota details
type QuotaUsageDetails struct {
	// Storage quota that is reserved for storage resource(s) that are being provisioned
	Reserved *resource.Quantity `json:"reserved,omitempty"`

	// Storage quota that is already used by storage resource(s) that have been provisioned
	Used *resource.Quantity `json:"used,omitempty"`
}

type SCLevelQuotaStatusList []SCLevelQuotaStatus

// SCLevelQuotaStatus gives storage quota usage per Kubernetes storage class
type SCLevelQuotaStatus struct {
	// +kubebuilder:validation:MaxLength=64

	// Name of the Kubernetes StorageClass
	StorageClassName string `json:"storageClassName"`

	// Storage quota usage details for given Kubernetes storage class
	// +optional
	SCLevelQuotaUsage *QuotaUsageDetails `json:"scQuotaUsage,omitempty"`
}

type ResourceTypeLevelQuotaStatusList []ResourceTypeLevelQuotaStatus
type ResourceTypeLevelQuotaStatus struct {
	// +kubebuilder:validation:MaxLength=64

	// Name of service extension associated with resource kind to be provisioned
	ResourceExtensionName string `json:"extensionName"`

	// Storage usage details per storage class level for given object kind
	ResourceTypeSCLevelQuotaStatuses SCLevelQuotaStatusList `json:"extensionQuotaUsage,omitempty"`
}

// StoragePolicyQuotaSpec defines the desired state of StoragePolicyQuota
type StoragePolicyQuotaSpec struct {
	// +kubebuilder:validation:Required
	// +kubebuilder:validation:XValidation:rule="self == oldSelf",message="StoragePolicyId is immutable"
	// +kubebuilder:validation:MaxLength=128

	// ID of the storage policy
	StoragePolicyId string `json:"storagePolicyId"`

	// Total limit of storage across all types of storage resources
	// for given storage policy within given namespace
	// +optional
	Limit *resource.Quantity `json:"limit,omitempty"`
}

// StoragePolicyQuotaStatus defines the observed state of StoragePolicyQuota
type StoragePolicyQuotaStatus struct {
	// Storage quota usage details per storage class level for given storage policy
	// +optional
	SCLevelQuotaStatuses SCLevelQuotaStatusList `json:"total,omitempty"`

	// Storage quota usage details per storage object type for given storage policy
	// +optional
	ResourceTypeLevelQuotaStatuses ResourceTypeLevelQuotaStatusList `json:"extensions,omitempty"`
}

//+kubebuilder:object:root=true
//+kubebuilder:subresource:status
//+kubebuilder:storageversion

// StoragePolicyQuota is the Schema for the storagepolicyquotas API
type StoragePolicyQuota struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec   StoragePolicyQuotaSpec   `json:"spec,omitempty"`
	Status StoragePolicyQuotaStatus `json:"status,omitempty"`
}

//+kubebuilder:object:root=true

// StoragePolicyQuotaList contains a list of StoragePolicyQuota
type StoragePolicyQuotaList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []StoragePolicyQuota `json:"items"`
}
