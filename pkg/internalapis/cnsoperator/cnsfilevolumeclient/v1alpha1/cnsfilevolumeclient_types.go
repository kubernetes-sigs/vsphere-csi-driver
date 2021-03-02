/*
Copyright 2020 The Kubernetes authors.

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

// CnsFileVolumeClientSpec defines the desired state of CnsFileVolumeClient.
type CnsFileVolumeClientSpec struct {
	// ExternalIPtoClientVms maintains a mapping of External IP address to list of names of ClientVms.
	// ClientsVms can be TKC Vms for TKC cluster (or) PodVms for SV.
	// Keys are External IP Addresses that have access to a volume.
	// Values are list of names of ClientVms. Each ClientVm in the list mounts this volume and
	// is exposed to the external network by this IP address.
	// PodVMs names will be prefixed with "POD_VM" and guest VMs will be prefix with "GUEST_VM".
	ExternalIPtoClientVms map[string][]string `json:"externalIPtoClientVms,omitempty"`
}

// +kubebuilder:object:root=true
// +kubebuilder:subresource:status

// CnsFileVolumeClient is the Schema for the cnsfilevolumeclients CRD. This CRD is used by
// CNS-CSI for internal bookkeeping purposes only and is not an API.
type CnsFileVolumeClient struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec CnsFileVolumeClientSpec `json:"spec,omitempty"`
}

// +kubebuilder:object:root=true

// CnsFileVolumeClientList contains a list of CnsFileVolumeClient
type CnsFileVolumeClientList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []CnsFileVolumeClient `json:"items"`
}
