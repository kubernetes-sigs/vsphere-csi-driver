/*
Copyright 2022 The Kubernetes Authors.

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

// CNSVolumeInfoSpec defines the desired state of CNSVolumeInfo
type CNSVolumeInfoSpec struct {

	// VolumeID is the FCD ID obtained from creating volume using CNS API.
	VolumeID string `json:"volumeID"`

	// vCenterServer is the IP/FQDN of the vCenter host on which the CNS volume is accessible.
	VCenterServer string `json:"vCenterServer"`
}

//+kubebuilder:object:root=true
//+kubebuilder:subresource:status

// CNSVolumeInfo is the Schema for the cnsvolumeinfoes API
type CNSVolumeInfo struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec CNSVolumeInfoSpec `json:"spec"`
}

//+kubebuilder:object:root=true

// CNSVolumeInfoList contains a list of CNSVolumeInfo
type CNSVolumeInfoList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []CNSVolumeInfo `json:"items"`
}
