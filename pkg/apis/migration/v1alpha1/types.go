/*
Copyright 2020 The Kubernetes Authors.

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

// +genclient
// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object

// CnsVSphereVolumeMigration is the Schema for the cnsvspherevolumemigrations API
type CnsVSphereVolumeMigration struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec CnsVSphereVolumeMigrationSpec `json:"spec,omitempty"`
}

// CnsVSphereVolumeMigrationSpec defines the desired state of CnsVSphereVolumeMigration
type CnsVSphereVolumeMigrationSpec struct {
	// VolumePath is the vmdk path of the vSphere Volume
	VolumePath string `json:"volumepath"`
	// VolumeID is the FCD ID obtained after register volume with CNS.
	VolumeID string `json:"volumeid"`
}

// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object

// CnsvSphereVolumeMigrationList contains a list of CnsVSphereVolumeMigration
type CnsvSphereVolumeMigrationList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []CnsVSphereVolumeMigration `json:"items"`
}
