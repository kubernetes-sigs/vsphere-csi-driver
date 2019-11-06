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
	"k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	clientset "k8s.io/client-go/kubernetes"
	"k8s.io/klog"
	"reflect"
	cnsconfig "sigs.k8s.io/vsphere-csi-driver/pkg/common/config"
	cnsvolumemetadatav1alpha1 "sigs.k8s.io/vsphere-csi-driver/pkg/syncer/cnsoperator/apis/cnsvolumemetadata/v1alpha1"
)

// pvcsiFullSync reconciles PV/PVC/Pod metadata on the guest cluster
// with cnsvolumemetadata objects on the supervisor cluster for the guest cluster
func pvcsiFullSync(k8sclient clientset.Interface, metadataSyncer *metadataSyncInformer) {
	klog.V(2).Infof("FullSync: Start")

	// guestCnsVolumeMetadataList is an in-memory list of cnsvolumemetadata
	// objects that represents PV/PVC/Pod objects in the guest cluster API server.
	// These objects are compared to actual objects on the supervisor
	// cluster to make reconciliation decisions.
	guestCnsVolumeMetadataList := cnsvolumemetadatav1alpha1.CnsVolumeMetadataList{}

	// Get the supervisor namespace in which the guest cluster is deployed
	supervisorNamespace, err := cnsconfig.GetSupervisorNamespace()
	if err != nil {
		klog.Errorf("FullSync: could not get supervisor namespace in which guest cluster was deployed. Err: %v", err)
		return
	}

	// Populate guestCnsVolumeMetadataList with cnsvolumemetadata objects created from the guest cluster
	err = createCnsVolumeMetadataList(k8sclient, metadataSyncer, supervisorNamespace, &guestCnsVolumeMetadataList)
	if err != nil {
		klog.Errorf("FullSync: Failed to create CnsVolumeMetadataList from guest cluster. Err: %v", err)
		return
	}

	// Get list of cnsvolumemetadata objects that exist in the given supervisor cluster namespace
	supervisorNamespaceList, err := metadataSyncer.cnsOperatorClient.CnsVolumeMetadatas(supervisorNamespace).List(metav1.ListOptions{})
	if err != nil {
		klog.Warningf("FullSync: Failed to get CnsVolumeMetadatas from supervisor cluster. Err: %v", err)
		return
	}

	supervisorCnsVolumeMetadataList := cnsvolumemetadatav1alpha1.CnsVolumeMetadataList{}
	// Remove cnsvolumemetadata object from supervisorCnsVolumeMetadataList that do not belong to this guest cluster
	for _, object := range supervisorNamespaceList.Items {
		if object.Spec.GuestClusterID == metadataSyncer.configInfo.Cfg.GC.ManagedClusterUID {
			supervisorCnsVolumeMetadataList.Items = append(supervisorCnsVolumeMetadataList.Items, object)
		}
	}

	// guestObjectsMap maintains a mapping of cnsvolumemetadata objects names to their spec.
	// Used by pvcsi full sync for quick look-up to check existence in the guest cluster.
	guestObjectsMap := make(map[string]*cnsvolumemetadatav1alpha1.CnsVolumeMetadata)
	for index, object := range guestCnsVolumeMetadataList.Items {
		guestObjectsMap[object.Name] = &guestCnsVolumeMetadataList.Items[index]
	}

	// supervisorObjectsMap maintains a mapping of cnsvolumemetadata objects names
	// to their spec.
	// Used by pvcsi full sync for quick look-up to check existence in the supervisor cluster.
	supervisorObjectsMap := make(map[string]*cnsvolumemetadatav1alpha1.CnsVolumeMetadata)
	for index, object := range supervisorCnsVolumeMetadataList.Items {
		supervisorObjectsMap[object.Name] = &supervisorCnsVolumeMetadataList.Items[index]
	}

	// Identify cnsvolumemetadata objects that need to be updated or created
	// on the supervisor cluster API server.
	for _, guestObject := range guestCnsVolumeMetadataList.Items {
		if supervisorObject, exists := supervisorObjectsMap[guestObject.Name]; !exists {
			// Create objects that do not exist
			klog.V(2).Infof("FullSync: Creating CnsVolumeMetadata %v on the supervisor cluster", guestObject.Name)
			if _, err = metadataSyncer.cnsOperatorClient.CnsVolumeMetadatas(supervisorNamespace).Create(&guestObject); err != nil {
				klog.Warningf("FullSync: Failed to create CnsVolumeMetadata %v. Err: %v", supervisorObject.Name, err)
			}
		} else {
			// Compare objects between the guest cluster and supervisor cluster.
			// Update the supervisor cluster API server if an object is stale.
			if !compareCnsVolumeMetadatas(&guestObject.Spec, &supervisorObject.Spec) {
				klog.V(2).Infof("FullSync: Updating CnsVolumeMetadata %v on the supervisor cluster", guestObject.Name)
				if _, err = metadataSyncer.cnsOperatorClient.CnsVolumeMetadatas(supervisorNamespace).Update(supervisorObject); err != nil {
					klog.Warningf("FullSync: Failed to update CnsVolumeMetadata %v. Err: %v", supervisorObject.Name, err)
				}
			}
		}
	}

	// Delete outdated cnsvolumemetadata objects present in
	// the supervisor cluster API server that shouldn't exist.
	for _, supervisorObject := range supervisorCnsVolumeMetadataList.Items {
		if _, exists := guestObjectsMap[supervisorObject.Name]; !exists {
			klog.V(2).Infof("FullSync: Deleting CnsVolumeMetadata %v on the supervisor cluster", supervisorObject.Name)
			if err = metadataSyncer.cnsOperatorClient.CnsVolumeMetadatas(supervisorNamespace).Delete(supervisorObject.Name, &metav1.DeleteOptions{}); err != nil {
				klog.Warningf("FullSync: Failed to delete CnsVolumeMetadata %v. Err: %v", supervisorObject.Name, err)
			}
		}
	}

	klog.V(2).Infof("FullSync: End")

}

// createCnsVolumeMetadataList creates cnsvolumemetadata objects from the API server
// using the input k8s client.
// All objects that can be created are added to returnList. This includes
// PERSISTENT_VOLUME, PERSISTENT_VOLUME_CLAIM and POD entity types.
func createCnsVolumeMetadataList(k8sclient clientset.Interface, metadataSyncer *metadataSyncInformer, supervisorNamespace string, returnList *cnsvolumemetadatav1alpha1.CnsVolumeMetadataList) error {
	klog.V(4).Infof("FullSync: Querying guest cluster API server for all PV objects.")
	pvList, err := getPVsInBoundAvailableOrReleased(k8sclient)
	if err != nil {
		klog.Errorf("FullSync: Failed to get PVs from guest cluster. Err: %v", err)
		return err
	}

	klog.V(4).Infof("FullSync: Creating CnsVolumeMetadata objects for each volume on the guest cluster.")
	for _, pv := range pvList {
		var volumeNames []string
		volumeNames = append(volumeNames, pv.Spec.CSI.VolumeHandle)

		// Get the cnsvolumemetadata object for this pv and add it to the return list
		entityReference := cnsvolumemetadatav1alpha1.GetCnsOperatorEntityReference(pv.Spec.CSI.VolumeHandle, supervisorNamespace, cnsvolumemetadatav1alpha1.CnsOperatorEntityTypePVC)
		pvObject := cnsvolumemetadatav1alpha1.CreateCnsVolumeMetadataSpec(volumeNames, metadataSyncer.configInfo.Cfg.GC.ManagedClusterUID, string(pv.UID), pv.Name, cnsvolumemetadatav1alpha1.CnsOperatorEntityTypePV, pv.Labels, "", []cnsvolumemetadatav1alpha1.CnsOperatorEntityReference{entityReference})
		returnList.Items = append(returnList.Items, *pvObject)

		// Get the cnsvolumemetadata object for pvc bound to this pv and add it to the return list
		if pv.Spec.ClaimRef != nil && pv.Status.Phase == v1.VolumeBound {
			pvc, err := k8sclient.CoreV1().PersistentVolumeClaims(pv.Spec.ClaimRef.Namespace).Get(pv.Spec.ClaimRef.Name, metav1.GetOptions{})
			if err != nil {
				klog.Errorf("FullSync: Failed to get PVC %q from guest cluster. Err: %v", pvc.Name, err)
				return err
			}
			entityReference := cnsvolumemetadatav1alpha1.GetCnsOperatorEntityReference(pvc.Spec.VolumeName, "", cnsvolumemetadatav1alpha1.CnsOperatorEntityTypePV)
			pvcObject := cnsvolumemetadatav1alpha1.CreateCnsVolumeMetadataSpec(volumeNames, metadataSyncer.configInfo.Cfg.GC.ManagedClusterUID, string(pvc.UID), pvc.Name, cnsvolumemetadatav1alpha1.CnsOperatorEntityTypePVC, pvc.GetLabels(), pvc.Namespace, []cnsvolumemetadatav1alpha1.CnsOperatorEntityReference{entityReference})
			returnList.Items = append(returnList.Items, *pvcObject)
		}
	}

	// TODO: Create CnsVolumeMetadata objects for POD entity types.
	return nil
}

// compareCnsVolumeMetadatas compares input cnsvolumemetadata objects
// and returns false if their labels are not deeply equal
func compareCnsVolumeMetadatas(guestObject *cnsvolumemetadatav1alpha1.CnsVolumeMetadataSpec, supervisorObject *cnsvolumemetadatav1alpha1.CnsVolumeMetadataSpec) bool {
	if !reflect.DeepEqual(guestObject.Labels, supervisorObject.Labels) {
		supervisorObject.Labels = guestObject.Labels
		return false
	}
	return true
}
