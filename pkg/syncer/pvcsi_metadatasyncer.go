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

package syncer

import (
	"github.com/davecgh/go-spew/spew"
	v1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/klog"
	cnsconfig "sigs.k8s.io/vsphere-csi-driver/pkg/common/config"
	cnsvolumemetadatav1alpha1 "sigs.k8s.io/vsphere-csi-driver/pkg/syncer/cnsoperator/apis/cnsvolumemetadata/v1alpha1"
)

// pvcsiVolumeUpdated updates persistent volume claim and persistent volume CnsVolumeMetadata on supervisor cluster when pvc/pv labels on K8S cluster have been updated
func pvcsiVolumeUpdated(resourceType interface{}, volumeHandle string, metadataSyncer *metadataSyncInformer) {
	supervisorNamespace, err := cnsconfig.GetSupervisorNamespace()
	if err != nil {
		klog.Warningf("pvCSI VolumeUpdated: Unable to fetch supervisor namespace. Err: %v", err)
		return
	}
	var newMetadata *cnsvolumemetadatav1alpha1.CnsVolumeMetadata
	// Create CnsVolumeMetaDataSpec based on the resource type
	switch resource := resourceType.(type) {
	case *v1.PersistentVolume:
		entityReference := cnsvolumemetadatav1alpha1.CnsOperatorEntityReference(createEntityReference(string(cnsvolumemetadatav1alpha1.CnsOperatorEntityTypePVC), resource.Spec.CSI.VolumeHandle, supervisorNamespace))
		newMetadata = createCnsVolumeMetaDataSpec(metadataSyncer.configInfo.Cfg.GC.ManagedClusterUID, string(resource.GetUID()), []string{volumeHandle}, resource.Name, resource.Labels, string(cnsvolumemetadatav1alpha1.CnsOperatorEntityTypePV), "", []cnsvolumemetadatav1alpha1.CnsOperatorEntityReference{entityReference})
	case *v1.PersistentVolumeClaim:
		entityReference := cnsvolumemetadatav1alpha1.CnsOperatorEntityReference(createEntityReference(string(cnsvolumemetadatav1alpha1.CnsOperatorEntityTypePV), resource.Spec.VolumeName, resource.Namespace))
		newMetadata = createCnsVolumeMetaDataSpec(metadataSyncer.configInfo.Cfg.GC.ManagedClusterUID, string(resource.GetUID()), []string{volumeHandle}, resource.Name, resource.Labels, string(cnsvolumemetadatav1alpha1.CnsOperatorEntityTypePVC), resource.Namespace, []cnsvolumemetadatav1alpha1.CnsOperatorEntityReference{entityReference})
	default:
	}
	// Check if cnsvolumemetadata object exists for this entity in the supervisor cluster
	currentMetadata, err := metadataSyncer.cnsOperatorClient.CnsVolumeMetadatas(supervisorNamespace).Get(newMetadata.Name, metav1.GetOptions{})
	if err != nil {
		if apierrors.IsNotFound(err) {
			_, err = metadataSyncer.cnsOperatorClient.CnsVolumeMetadatas(supervisorNamespace).Create(newMetadata)
			if err != nil {
				klog.Errorf("pvCSI VolumeUpdated: Failed to create CnsVolumeMetadata: %v. Error: %v", newMetadata.Name, err)
			}
			return
		}
		klog.Errorf("pvCSI VolumeUpdated: Unable to fetch CnsVolumeMetadata: %v. Error: %v", newMetadata.Name, err)
		return
	}
	newMetadata.ResourceVersion = currentMetadata.ResourceVersion
	klog.V(4).Infof("pvCSI VolumeUpdated: Invoking update on CnsVolumeMetadata : %+v", spew.Sdump(currentMetadata))
	_, err = metadataSyncer.cnsOperatorClient.CnsVolumeMetadatas(supervisorNamespace).Update(newMetadata)
	if err != nil {
		klog.Errorf("pvCSI VolumeUpdated: Failed to update CnsVolumeMetadata: %v. Error: %v", newMetadata.Name, err)
		return
	}
	klog.V(2).Infof("pvCSI VolumeUpdated: Successfully updated CnsVolumeMetadata: %v", currentMetadata.Name)
}

// pvcsiVolumeDeleted deletes pvc/pv CnsVolumeMetadata on supervisor cluster when pvc/pv has been deleted on K8s cluster
func pvcsiVolumeDeleted(uID string, metadataSyncer *metadataSyncInformer) {
	supervisorNamespace, err := cnsconfig.GetSupervisorNamespace()
	if err != nil {
		klog.Warningf("pvCSI VolumeDeleted: Unable to fetch supervisor namespace. Err: %v", err)
		return
	}
	volumeMetadataName := getCnsVolMetadataName(metadataSyncer.configInfo.Cfg.GC.ManagedClusterUID, uID)
	klog.V(4).Infof("pvCSI VolumeDeleted: Invoking delete on CnsVolumeMetadata : %v", volumeMetadataName)
	err = metadataSyncer.cnsOperatorClient.CnsVolumeMetadatas(supervisorNamespace).Delete(volumeMetadataName, &metav1.DeleteOptions{})
	if err != nil {
		klog.Errorf("pvCSI VolumeDeleted: Failed to delete CnsVolumeMetadata: %v. Error: %v", volumeMetadataName, err)
		return
	}
	klog.V(2).Infof("pvCSI VolumeDeleted: Successfully deleted CnsVolumeMetadata: %v", volumeMetadataName)
}

// createCnsVolumeMetaDataSpec creates a CnsVolumeEntityMetaData object from given parameters
func createCnsVolumeMetaDataSpec(guestClusterID string, uID string, volumeNames []string, entityName string, labels map[string]string, entityType string, namespace string, entityReferences []cnsvolumemetadatav1alpha1.CnsOperatorEntityReference) *cnsvolumemetadatav1alpha1.CnsVolumeMetadata {
	// Create CnsVolumeMetadata spec
	return &cnsvolumemetadatav1alpha1.CnsVolumeMetadata{
		ObjectMeta: metav1.ObjectMeta{
			Name:            getCnsVolMetadataName(guestClusterID, uID),
			OwnerReferences: nil,
			Finalizers:      nil,
		},
		Spec: cnsvolumemetadatav1alpha1.CnsVolumeMetadataSpec{
			VolumeNames:      volumeNames,
			GuestClusterID:   guestClusterID,
			EntityType:       cnsvolumemetadatav1alpha1.CnsOperatorEntityType(entityType),
			EntityName:       entityName,
			Labels:           labels,
			Namespace:        namespace,
			EntityReferences: entityReferences,
		},
	}
}

//getCnsVolMetadataName returns the concatenated string of guestclusterid and entity id
func getCnsVolMetadataName(guestClusterID string, entityUID string) string {
	return guestClusterID + "-" + entityUID
}
