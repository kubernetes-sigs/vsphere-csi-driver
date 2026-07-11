/*
Copyright 2026.

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

// HostInfoRequestSpec defines the desired state of HostInfoRequest.
type HostInfoRequestSpec struct {
	// NodeIP is the Supervisor VM's InternalIP that needs to be resolved to
	// an ESXi hostname/moid. Sourced from the guest node's
	// VirtualMachine.Status.NodeName.
	NodeIP string `json:"nodeIP"`
}

// CRDStatus reflects the state of resolving a HostInfoRequest.
type CRDStatus string

const (
	// HostInfoRequestSuccess is used to imply that Node resolution succeeded.
	HostInfoRequestSuccess CRDStatus = "Success"
	// HostInfoRequestError is used to imply that Node resolution failed.
	HostInfoRequestError CRDStatus = "Error"
)

// HostInfoRequestStatus defines the observed state of HostInfoRequest.
type HostInfoRequestStatus struct {
	// Status can have the following values: "Success", "Error". Empty means
	// the request has not been reconciled yet.
	Status CRDStatus `json:"status,omitempty"`

	// Hostname is the resolved Supervisor Node's name. Read this only after
	// `Status` is set to "Success".
	Hostname string `json:"hostname,omitempty"`

	// EsxiHostMoid is the resolved Node's "vmware-system-esxi-node-moid"
	// annotation value. Read this only after `Status` is set to "Success".
	EsxiHostMoid string `json:"esxiHostMoid,omitempty"`

	// ErrorMessage is set when `Status` is "Error".
	ErrorMessage string `json:"errorMessage,omitempty"`
}

//+kubebuilder:object:root=true
//+kubebuilder:subresource:status

// HostInfoRequest lets a guest cluster ask the Supervisor's CNS Syncer to
// resolve a Supervisor VM's IP to the ESXi hostname/moid it is running on,
// by having Syncer (which has unscoped Supervisor cluster access) list
// Node objects directly - something pvCSI's own namespace-scoped
// ProviderServiceAccount cannot do, since Nodes are cluster-scoped.
// Instances live in the guest cluster's Supervisor namespace, named after
// the CSINodeTopology instance (guest node name) that requested them.
type HostInfoRequest struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec   HostInfoRequestSpec   `json:"spec"`
	Status HostInfoRequestStatus `json:"status,omitempty"`
}

//+kubebuilder:object:root=true

// HostInfoRequestList contains a list of HostInfoRequest.
type HostInfoRequestList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []HostInfoRequest `json:"items"`
}
