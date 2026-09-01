/*
Copyright 2026 The Kubernetes Authors.

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
	storagepolicyv1alpha1 "sigs.k8s.io/vsphere-csi-driver/v3/pkg/apis/cnsoperator/storagepolicyinfo/v1alpha1"
)

// +genclient:nonNamespaced
// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object

// +k8s:openapi-gen=true
// +kubebuilder:subresource:status
// +kubebuilder:object:root=true
// +kubebuilder:resource:scope=Cluster,shortName=vksspi,path=vksstoragepolicyinfos

// VKSStoragePolicyInfo is the Schema for the vksstoragepolicyinfos API. It mirrors the
// Supervisor's namespaced StoragePolicyInfo into a guest cluster. A guest cluster is
// provisioned inside a single Supervisor namespace and is therefore single-tenant, so
// unlike the Supervisor (one StoragePolicyInfo per tenant namespace per policy), the
// guest cluster carries a single Cluster-scoped VKSStoragePolicyInfo per policy. Name of
// this CR is the same as the unique and immutable K8sCompliantName of the storage policy.
type VKSStoragePolicyInfo struct {
	Spec              storagepolicyv1alpha1.StoragePolicyInfoSpec `json:"spec,omitempty"`
	metav1.TypeMeta   `json:",inline"`
	Status            storagepolicyv1alpha1.StoragePolicyInfoStatus `json:"status,omitempty"`
	metav1.ObjectMeta `json:"metadata,omitempty"`
}

// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object

// +kubebuilder:object:root=true

// VKSStoragePolicyInfoList contains a list of VKSStoragePolicyInfo.
type VKSStoragePolicyInfoList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []VKSStoragePolicyInfo `json:"items"`
}
