/*
Copyright 2021.

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

// CSINodeTopologySpec defines the desired state of CSINodeTopology.
type CSINodeTopologySpec struct {

	// NodeID refers to the node name by which a Node is recognised.
	NodeID string `json:"nodeID"`

	// NodeUUID refers to the unique VM UUID by which a Node is recognised.
	NodeUUID string `json:"nodeuuid,omitempty"`
}

type CRDStatus string

const (
	// CSINodeTopologySuccess is used to imply that node topology retrieval is successful.
	CSINodeTopologySuccess CRDStatus = "Success"
	// CSINodeTopologyError is used to imply that node topology retrieval resulted in an error.
	CSINodeTopologyError CRDStatus = "Error"
)

// CSINodeTopologyStatus defines the observed state of CSINodeTopology.
type CSINodeTopologyStatus struct {
	// Status can have the following values: "Success", "Error".
	Status CRDStatus `json:"status,omitempty"`

	// TopologyLabels consists of all the topology-related labels
	// applied to the NodeVM or its ancestors in the VC.
	// Read this parameter only after `Status` is set to "Success".
	// TopologyLabels will be empty when `Status` is set to "Error".
	//+optional
	TopologyLabels []TopologyLabel `json:"topologyLabels,omitempty"`

	// ErrorMessage will contain the error string when `Status` field is set to "Error".
	// It will be empty when the `Status` field is set to "Success".
	ErrorMessage string `json:"errorMessage,omitempty"`
}

// TopologyLabel will consist of a key-value pair.
// The entries in `key` field must be a part of the `Labels` struct in the vSphere config secret.
// For example: User might choose to assign a tag of `us-east` under the `k8s-zone` to a NodeVM on the VC.
// In such cases this struct will hold `k8s-zone` as the key and `us-east` as a value for that NodeVM.
type TopologyLabel struct {
	Key   string `json:"key"`
	Value string `json:"value"`
}

//+kubebuilder:object:root=true
//+kubebuilder:subresource:status

// CSINodeTopology is the Schema for the csinodetopologies API.
type CSINodeTopology struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec   CSINodeTopologySpec   `json:"spec"`
	Status CSINodeTopologyStatus `json:"status,omitempty"`
}

//+kubebuilder:object:root=true

// CSINodeTopologyList contains a list of CSINodeTopology.
type CSINodeTopologyList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []CSINodeTopology `json:"items"`
}
