/*
Copyright 2024.

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

// EDIT THIS FILE!  THIS IS SCAFFOLDING FOR YOU TO OWN!
// NOTE: json tags are required.  Any new fields you add must have json tags for the fields to be serialized.

// CnsMigrateVolumeSpec defines the desired state of CnsMigrateVolume
type CnsMigrateVolumeSpec struct {
        // VolumeName indicates the name of the volume on the supervisor Cluster.
	// This is guaranteed to be unique in Supervisor cluster.
	VolumeName string `json:"volumename"`

        // Datastore url indicates the destination datastore to be migrated.
        DatastoreUrl string `json:"datastoreurl"`

	// Storage policy name of destination datastore, the default policy
        // of the destination datastore is applied if this is unset.
        // +optional
	StoragePolicyName string `json:"storagepolicyname,omitempty"`

        // Storage class to be migrated, DatastoreUrl should be within
        // the storage class of volume if this is unset.
        // +optional
        StorageClass string `json:"storageclass,omitempty"`

        // Namespace to be migrated.
        // +optional
        Namespace string `json:"namespace,omitempty"`
}

// CnsMigrateVolumeStatus defines the observed state of CnsMigrateVolume
type CnsMigrateVolumeStatus struct {
	// Indicates the volume is successfully migrated.
	// This field must only be set by the entity completing the migrate
	// operation, i.e. the CNS Operator.
	Migrated bool `json:"migrated"`

	// The last error encountered during migrate operation, if any.
	// This field must only be set by the entity completing the migrate
	// operation, i.e. the CNS Operator.
	// +optional
	Error string `json:"error,omitempty"`
}

//+kubebuilder:object:root=true
//+kubebuilder:subresource:status

// CnsMigrateVolume is the Schema for the cnsmigratevolumes API
type CnsMigrateVolume struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec   CnsMigrateVolumeSpec   `json:"spec,omitempty"`
	Status CnsMigrateVolumeStatus `json:"status,omitempty"`
}

//+kubebuilder:object:root=true

// CnsMigrateVolumeList contains a list of CnsMigrateVolume
type CnsMigrateVolumeList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []CnsMigrateVolume `json:"items"`
}

func init() {
	SchemeBuilder.Register(&CnsMigrateVolume{}, &CnsMigrateVolumeList{})
}
