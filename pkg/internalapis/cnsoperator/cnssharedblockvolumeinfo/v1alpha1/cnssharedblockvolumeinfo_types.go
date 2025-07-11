/*
Copyright 2025 The Kubernetes authors.

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

// CnsSharedBlockVolumeInfoSpec contains the list of VM UUIDs attached
// to the given volume
type CnsSharedBlockVolumeInfoSpec struct {
	AttachedVms []string `json:"attachedVms,omitempty"`
}

// +kubebuilder:object:root=true
// +kubebuilder:subresource:status

// CnsSharedBlockVolumeInfo is the Schema for the cnssharedblockvolumeinfoes CRD. This CRD is used by
// CNS-CSI for internal bookkeeping purposes only and is not an API.
type CnsSharedBlockVolumeInfo struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec CnsSharedBlockVolumeInfoSpec `json:"spec,omitempty"`
}

// +kubebuilder:object:root=true

// CnsSharedBlockVolumeInfoList contains a list of CnsSharedBlockVolumeInfo
type CnsSharedBlockVolumeInfoList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []CnsSharedBlockVolumeInfo `json:"items"`
}
