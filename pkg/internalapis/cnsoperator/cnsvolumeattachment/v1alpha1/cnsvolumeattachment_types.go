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

// CnsVolumeAttachmentSpec contains the list of VM UUIDs attached
// to the given volume
type CnsVolumeAttachmentSpec struct {
	AttachedVms []string `json:"attachedVms,omitempty"`
}

// +kubebuilder:object:root=true
// +kubebuilder:subresource:status

// CnsVolumeAttachment is the Schema for the cnsvolumeattachment CRD. This CRD is used by
// CNS-CSI for internal bookkeeping purposes only and is not an API.
type CnsVolumeAttachment struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec CnsVolumeAttachmentSpec `json:"spec,omitempty"`
}

// +kubebuilder:object:root=true

// CnsVolumeAttachmentList contains a list of CnsVolumeAttachment
type CnsVolumeAttachmentList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []CnsVolumeAttachment `json:"items"`
}
