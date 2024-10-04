// Copyright (c) 2024 VMware, Inc. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

package v1alpha1

import (
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// StorageQuotaPeriodicSyncSpec defines the desired state of StorageQuotaPeriodicSync
type StorageQuotaPeriodicSyncSpec struct {
	// SyncIntervalInMinutes is the time in minutes after which StorageQuota CR will be
	// synced periodically
	SyncIntervalInMinutes int `json:"syncIntervalInMinutes,omitempty"`
}

// StorageQuotaPeriodicSyncStatus defines the observed state of StorageQuotaPeriodicSync
type StorageQuotaPeriodicSyncStatus struct {
	// LastSyncTimestamp defines the timestamp when last sync operation executed
	LastSyncTimestamp metav1.Time `json:"lastSyncTimestamp,omitempty"`

	// ExpectedReservedValues indicates the expected "reserved" value for each namespace
	// during each execution cycle
	ExpectedReservedValues []ExpectedReservedValues `json:"expectedReservedValues,omitempty"`
}

type ExpectedReservedValues struct {
	// Namespace is the namespace for which reserved values are calculated
	Namespace string `json:"namespace,omitempty"`

	// Reserved indicates the map of storagePolicyId to expected reserved value for that policy.
	// It is calculated by combining the sizes of pending PVC objects, pending PVC expands and not ready
	// VolumeSnapshots etc. for each storagePolicyId.
	Reserved map[string]*resource.Quantity `json:"reserved,omitempty"`
}

//+kubebuilder:object:root=true
//+kubebuilder:subresource:status
//+kubebuilder:storageversion

// StorageQuotaPeriodicSync is the Schema for the storagequotaperiodicsync API
type StorageQuotaPeriodicSync struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec   StorageQuotaPeriodicSyncSpec   `json:"spec,omitempty"`
	Status StorageQuotaPeriodicSyncStatus `json:"status,omitempty"`
}

//+kubebuilder:object:root=true

// StorageQuotaPeriodicSyncList contains a list of StorageQuotaPeriodicSync
type StorageQuotaPeriodicSyncList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []StorageQuotaPeriodicSync `json:"items"`
}

func init() {
	objectTypes = append(objectTypes, &StorageQuotaPeriodicSync{}, &StorageQuotaPeriodicSyncList{})
}
