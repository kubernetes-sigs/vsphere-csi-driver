/*
Copyright 2019 The Kubernetes Authors.

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
	cnstypes "github.com/vmware/govmomi/cns/types"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/vsphere-csi-driver/v2/pkg/common/config"
	cnsoperatortypes "sigs.k8s.io/vsphere-csi-driver/v2/pkg/syncer/cnsoperator/types"
)

// CnsVolumeMetadataSpec defines the desired state of CnsVolumeMetadata
// +k8s:openapi-gen=true
type CnsVolumeMetadataSpec struct {
	// VolumeNames indicates the unique ID of the volume.
	// For volumes created in a guest cluster, this will be
	// “<guestcluster-ID>-<UID>” where UID is the pvc.UUID in supervisor cluster.
	// Instances of POD entity type can have multiple volume names.
	// Instances of PV and PVC entity types will have only one volume name.
	VolumeNames []string `json:"volumenames"`

	// GuestClusterID indicates the guest cluster ID in which this volume
	// is referenced.
	GuestClusterID string `json:"guestclusterid"`

	// EntityType indicates type of entity whose metadata
	// this instance represents.
	EntityType CnsOperatorEntityType `json:"entitytype"`

	// EntityName indicates name of the entity in the guest cluster.
	EntityName string `json:"entityname"`

	// EntityReferences indicate the other entities that this entity refers to.
	// A Pod in the guest cluster can refers to one or more PVCs in the guest cluster.
	// A PVC in the guest cluster refers to a PV in the guest cluster.
	// A PV in the guest cluster refers to a PVC in the supervisor cluster.
	// A Pod/PVC in the guest cluster referring to a PVC/PV respectively in the
	// guest cluster must set the ClusterID field.
	// A PV in the guest cluster referring to a PVC in the supervisor cluster
	// must leave the ClusterID field unset.
	// This field is mandatory.
	EntityReferences []CnsOperatorEntityReference `json:"entityreferences"`

	// Labels indicates user labels assigned to the entity in the guest cluster.
	// Should only be populated if EntityType is PERSISTENT_VOLUME OR
	// PERSISTENT_VOLUME_CLAIM. CNS Operator will return a failure to the client
	// if labels are set for objects whose EntityType is POD.
	//+optional
	Labels map[string]string `json:"labels,omitempty"`

	// Namespace indicates namespace of entity in guest cluster.
	// Should only be populated if EntityType is PERSISTENT_VOLUME_CLAIM or POD.
	// CNS Operator will return a failure to the client if namespace is set for
	// objects whose EntityType is PERSISTENT_VOLUME.
	//+optional
	Namespace string `json:"namespace,omitempty"`

	// ClusterDistribution indicates the cluster distribution where the
	// PVCSI driver is deployed
	//+optional
	ClusterDistribution string `json:"clusterdistribution,omitempty"`
}

// CnsVolumeMetadataStatus defines the observed state of CnsVolumeMetadata
// +k8s:openapi-gen=true
type CnsVolumeMetadataStatus struct {
	// The last error encountered, per volume, during update operation.
	// This field must only be set by the entity completing the update
	// operation, i.e. the CNS Operator.
	// This string may be logged, so it should not contain sensitive
	// information.
	// +optional
	VolumeStatus []CnsVolumeMetadataVolumeStatus `json:"volumestatus,omitempty"`
}

// +genclient
// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object

// CnsVolumeMetadata is the Schema for the cnsvolumemetadata API
// +k8s:openapi-gen=true
// +kubebuilder:subresource:status
// +kubebuilder:resource:path=cnsvolumemetadata,scope=Namespaced
type CnsVolumeMetadata struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec   CnsVolumeMetadataSpec   `json:"spec,omitempty"`
	Status CnsVolumeMetadataStatus `json:"status,omitempty"`
}

// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object

// CnsVolumeMetadataList contains a list of CnsVolumeMetadata.
type CnsVolumeMetadataList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []CnsVolumeMetadata `json:"items"`
}

// CnsOperatorEntityType defines the type for entitytype parameter in
// cnsvolumemetadata API.
type CnsOperatorEntityType cnstypes.CnsKubernetesEntityType

// CnsVolumeMetadataVolumeStatus defines the status of the last update operation
// on CNS for the given volume.
// Error message will be empty if no error was encountered.
type CnsVolumeMetadataVolumeStatus struct {
	VolumeName   string `json:"volumename"`
	Updated      bool   `json:"updated"`
	ErrorMessage string `json:"errormessage,omitempty"`
}

// Allowed CnsOperatorEntityTypes for cnsvolumemetadata API.
const (
	CnsOperatorEntityTypePV  = CnsOperatorEntityType(cnstypes.CnsKubernetesEntityTypePV)
	CnsOperatorEntityTypePVC = CnsOperatorEntityType(cnstypes.CnsKubernetesEntityTypePVC)
	CnsOperatorEntityTypePOD = CnsOperatorEntityType(cnstypes.CnsKubernetesEntityTypePOD)
)

// CnsOperatorEntityReference defines the type for entityreference
// parameter in cnsvolumemetadata API.
type CnsOperatorEntityReference cnstypes.CnsKubernetesEntityReference

// CreateCnsVolumeMetadataSpec returns a cnsvolumemetadata object from the
// input parameters.
func CreateCnsVolumeMetadataSpec(volumeHandle []string, gcConfig config.GCConfig, uid string, name string,
	entityType CnsOperatorEntityType, labels map[string]string, namespace string,
	reference []CnsOperatorEntityReference) *CnsVolumeMetadata {
	return &CnsVolumeMetadata{
		ObjectMeta: metav1.ObjectMeta{
			Name: GetCnsVolumeMetadataName(gcConfig.TanzuKubernetesClusterUID, uid),
			OwnerReferences: []metav1.OwnerReference{GetCnsVolumeMetadataOwnerReference(cnsoperatortypes.GCAPIVersion,
				cnsoperatortypes.GCKind, gcConfig.TanzuKubernetesClusterName, gcConfig.TanzuKubernetesClusterUID)},
		},
		Spec: CnsVolumeMetadataSpec{
			VolumeNames:         volumeHandle,
			GuestClusterID:      gcConfig.TanzuKubernetesClusterUID,
			EntityType:          entityType,
			EntityName:          name,
			Labels:              labels,
			Namespace:           namespace,
			EntityReferences:    reference,
			ClusterDistribution: gcConfig.ClusterDistribution,
		},
	}
}

// GetCnsVolumeMetadataName returns the concatenated string of guestclusterid
// and entity id.
func GetCnsVolumeMetadataName(guestClusterID string, entityUID string) string {
	return guestClusterID + "-" + entityUID
}

// GetCnsOperatorEntityReference returns the entityReference object from the
// input parameters.
func GetCnsOperatorEntityReference(name string, namespace string, entitytype CnsOperatorEntityType,
	clusterid string) CnsOperatorEntityReference {
	return CnsOperatorEntityReference{
		EntityType: string(entitytype),
		EntityName: name,
		Namespace:  namespace,
		ClusterID:  clusterid,
	}
}

// GetCnsOperatorVolumeStatus returns a CnsVolumeMetadataVolumeStatus object
// from input parameters.
func GetCnsOperatorVolumeStatus(volumeName string, errorMessage string) CnsVolumeMetadataVolumeStatus {
	return CnsVolumeMetadataVolumeStatus{
		VolumeName:   volumeName,
		Updated:      false,
		ErrorMessage: errorMessage,
	}
}

// GetCnsVolumeMetadataOwnerReference returns the owner reference object from
// the input parameters.
func GetCnsVolumeMetadataOwnerReference(apiVersion string, kind string, clusterName string,
	clusterUID string) metav1.OwnerReference {
	bController := true
	bOwnerDeletion := true
	return metav1.OwnerReference{
		APIVersion:         apiVersion,
		Controller:         &bController,
		BlockOwnerDeletion: &bOwnerDeletion,
		Kind:               kind,
		Name:               clusterName,
		UID:                types.UID(clusterUID),
	}
}
