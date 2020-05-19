package v1alpha1

import (
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// CnsvSphereVolumeMigrationSpec defines the desired state of CnsvSphereVolumeMigration
// +k8s:openapi-gen=true
type CnsvSphereVolumeMigrationSpec struct {
	// VolumePath is the vmdk path of the vSphere Volume
	VolumePath string `json:"volumepath"`
	// VolumeName is the name of the volume, generally this will be set as PV name
	VolumeName string `json:"volumename"`
}

// CnsvSphereVolumeMigrationStatus defines the observed state of CnsvSphereVolumeMigration
// +k8s:openapi-gen=true
type CnsvSphereVolumeMigrationStatus struct {
	// VolumeID is the FCD ID obtained after register volume with CNS.
	VolumeID string `json:"volumeid"`
	// Registered is set to true if volume is successfully registered with CNS and VolumeID is obtained.
	Registered bool `json:"registered"`
	// Error represent the error observed while registering volume with CNS.
	Error string `json:"error,omitempty"`
}

// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object

// CnsvSphereVolumeMigration is the Schema for the cnsvspherevolumemigrations API
// +k8s:openapi-gen=true
// +kubebuilder:subresource:status
// +kubebuilder:resource:path=cnsvspherevolumemigrations,scope=Namespaced
type CnsvSphereVolumeMigration struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec   CnsvSphereVolumeMigrationSpec   `json:"spec,omitempty"`
	Status CnsvSphereVolumeMigrationStatus `json:"status,omitempty"`
}

// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object

// CnsvSphereVolumeMigrationList contains a list of CnsvSphereVolumeMigration
type CnsvSphereVolumeMigrationList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []CnsvSphereVolumeMigration `json:"items"`
}
