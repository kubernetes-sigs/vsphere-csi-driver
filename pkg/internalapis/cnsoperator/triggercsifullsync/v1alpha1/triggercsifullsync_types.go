/*
Copyright 2021 The Kubernetes authors.

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
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// TriggerCsiFullSyncCRName is the name of the instance
// created to trigger full sync on demand.
const TriggerCsiFullSyncCRName = "csifullsync"

// TriggerCsiFullSyncSpec is the spec for TriggerCsiFullSync
type TriggerCsiFullSyncSpec struct {
	// TriggerSyncID gives an option to trigger full sync on demand.
	// Initial value will be 0. In order to trigger a full sync, user
	// has to set a number that is 1 greater than the previous one.
	TriggerSyncID uint64 `json:"triggerSyncID"`
}

// TriggerCsiFullSyncStatus contains the status for a TriggerCsiFullSync
type TriggerCsiFullSyncStatus struct {
	// InProgress indicates whether a CSI full sync is in progress.
	// If full sync is completed this field will be unset.
	InProgress bool `json:"inProgress"`

	// LastTriggerSyncID indicates the last trigger sync Id.
	LastTriggerSyncID uint64 `json:"lastTriggerSyncID"`

	// LastSuccessfulStartTimeStamp indicates last successful full sync start timestamp.
	LastSuccessfulStartTimeStamp *metav1.Time `json:"lastSuccessfulStartTimeStamp,omitempty"`

	// LastSuccessfulEndTimeStamp indicates last successful full sync end timestamp.
	LastSuccessfulEndTimeStamp *metav1.Time `json:"lastSuccessfulEndTimeStamp,omitempty"`

	// LastRunStartTimeStamp indicates last run full sync start timestamp.
	// This timestamp can be either the successful or failed full sync start timestamp.
	LastRunStartTimeStamp *metav1.Time `json:"lastRunStartTimeStamp,omitempty"`

	// LastRunEndTimeStamp indicates last run full sync end timestamp.
	// This timestamp can be either the successful or failed full sync end timestamp.
	LastRunEndTimeStamp *metav1.Time `json:"lastRunEndTimeStamp,omitempty"`

	// The last error encountered during CSI full sync operation, if any.
	// Previous error will be cleared when a new full sync is in progress.
	Error string `json:"error,omitempty"`
}

// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object

// TriggerCsiFullSync is the Schema for the TriggerCsiFullSync API
// +kubebuilder:subresource:status
type TriggerCsiFullSync struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	// Spec defines a specification of the TriggerCsiFullSync.
	Spec TriggerCsiFullSyncSpec `json:"spec,omitempty"`

	// Status represents the current information/status for the TriggerCsiFullSync request.
	Status TriggerCsiFullSyncStatus `json:"status,omitempty"`
}

// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object

// TriggerCsiFullSyncList contains a list of TriggerCsiFullSync
type TriggerCsiFullSyncList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []TriggerCsiFullSync `json:"items"`
}

// CreateTriggerCsiFullSyncInstance creates default CreateTriggerCsiFullSync CR instance
func CreateTriggerCsiFullSyncInstance() *TriggerCsiFullSync {
	return &TriggerCsiFullSync{
		ObjectMeta: metav1.ObjectMeta{
			Name: TriggerCsiFullSyncCRName,
		},
		Spec: TriggerCsiFullSyncSpec{
			TriggerSyncID: 0,
		},
		Status: TriggerCsiFullSyncStatus{
			InProgress:        false,
			LastTriggerSyncID: 0,
		},
	}
}
