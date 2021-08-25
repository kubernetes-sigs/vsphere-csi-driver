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
	"context"
	"reflect"

	v1 "k8s.io/api/core/v1"

	"k8s.io/apimachinery/pkg/labels"
	"sigs.k8s.io/controller-runtime/pkg/client"

	cnsvolumemetadatav1alpha1 "sigs.k8s.io/vsphere-csi-driver/v2/pkg/apis/cnsoperator/cnsvolumemetadata/v1alpha1"
	cnsconfig "sigs.k8s.io/vsphere-csi-driver/v2/pkg/common/config"
	"sigs.k8s.io/vsphere-csi-driver/v2/pkg/csi/service/logger"
)

// PvcsiFullSync reconciles PV/PVC/Pod metadata on the guest cluster with
// cnsvolumemetadata objects on the supervisor cluster for the guest cluster.
func PvcsiFullSync(ctx context.Context, metadataSyncer *metadataSyncInformer) error {
	log := logger.GetLogger(ctx)
	log.Infof("FullSync: Start")

	// guestCnsVolumeMetadataList is an in-memory list of cnsvolumemetadata
	// objects that represents PV/PVC/Pod objects in the guest cluster API server.
	// These objects are compared to actual objects on the supervisor
	// cluster to make reconciliation decisions.
	guestCnsVolumeMetadataList := cnsvolumemetadatav1alpha1.CnsVolumeMetadataList{}

	// Get the supervisor namespace in which the guest cluster is deployed.
	supervisorNamespace, err := cnsconfig.GetSupervisorNamespace(ctx)
	if err != nil {
		log.Errorf("FullSync: could not get supervisor namespace in which guest cluster was deployed. Err: %v", err)
		return err
	}

	// Populate guestCnsVolumeMetadataList with cnsvolumemetadata objects created
	// from the guest cluster.
	err = createCnsVolumeMetadataList(ctx, metadataSyncer, supervisorNamespace, &guestCnsVolumeMetadataList)
	if err != nil {
		log.Errorf("FullSync: Failed to create CnsVolumeMetadataList from guest cluster. Err: %v", err)
		return err
	}

	// Get list of cnsvolumemetadata objects that exist in the given supervisor
	// cluster namespace.
	supervisorNamespaceList := &cnsvolumemetadatav1alpha1.CnsVolumeMetadataList{}
	err = metadataSyncer.cnsOperatorClient.List(ctx, supervisorNamespaceList, client.InNamespace(supervisorNamespace))
	if err != nil {
		log.Warnf("FullSync: Failed to get CnsVolumeMetadatas from supervisor cluster. Err: %v", err)
		return err
	}

	supervisorCnsVolumeMetadataList := cnsvolumemetadatav1alpha1.CnsVolumeMetadataList{}
	// Remove cnsvolumemetadata object from supervisorCnsVolumeMetadataList that
	// do not belong to this guest cluster.
	for _, object := range supervisorNamespaceList.Items {
		if object.Spec.GuestClusterID == metadataSyncer.configInfo.Cfg.GC.TanzuKubernetesClusterUID {
			supervisorCnsVolumeMetadataList.Items = append(supervisorCnsVolumeMetadataList.Items, object)
		}
	}

	// guestObjectsMap maintains a mapping of cnsvolumemetadata objects names to
	// their spec. Used by pvcsi full sync for quick look-up to check existence
	// in the guest cluster.
	guestObjectsMap := make(map[string]*cnsvolumemetadatav1alpha1.CnsVolumeMetadata)
	for index, object := range guestCnsVolumeMetadataList.Items {
		guestObjectsMap[object.Name] = &guestCnsVolumeMetadataList.Items[index]
	}

	// supervisorObjectsMap maintains a mapping of cnsvolumemetadata objects names
	// to their spec. Used by pvcsi full sync for quick look-up to check existence
	// in the supervisor cluster.
	supervisorObjectsMap := make(map[string]*cnsvolumemetadatav1alpha1.CnsVolumeMetadata)
	for index, object := range supervisorCnsVolumeMetadataList.Items {
		supervisorObjectsMap[object.Name] = &supervisorCnsVolumeMetadataList.Items[index]
	}

	// Identify cnsvolumemetadata objects that need to be updated or created
	// on the supervisor cluster API server.
	for _, guestObject := range guestCnsVolumeMetadataList.Items {
		if supervisorObject, exists := supervisorObjectsMap[guestObject.Name]; !exists {
			// Create objects that do not exist.
			log.Infof("FullSync: Creating CnsVolumeMetadata %v on the supervisor cluster for entity type %q",
				guestObject.Name, guestObject.Spec.EntityType)
			guestObject.Namespace = supervisorNamespace
			if err := metadataSyncer.cnsOperatorClient.Create(ctx, &guestObject); err != nil {
				log.Warnf("FullSync: Failed to create CnsVolumeMetadata %v. Err: %v", guestObject.Name, err)
			}
		} else {
			// Compare objects between the guest cluster and supervisor cluster.
			// Update the supervisor cluster API server if an object is stale.
			if guestObject.Spec.EntityType != cnsvolumemetadatav1alpha1.CnsOperatorEntityTypePOD &&
				!compareCnsVolumeMetadatas(&guestObject.Spec, &supervisorObject.Spec) {
				log.Infof("FullSync: Updating CnsVolumeMetadata %v on the supervisor cluster", guestObject.Name)
				if err := metadataSyncer.cnsOperatorClient.Update(ctx, supervisorObject); err != nil {
					log.Warnf("FullSync: Failed to update CnsVolumeMetadata %v. Err: %v", supervisorObject.Name, err)
				}
			}
		}
	}

	// Delete outdated cnsvolumemetadata objects present in
	// the supervisor cluster API server that shouldn't exist.
	for _, supervisorObject := range supervisorCnsVolumeMetadataList.Items {
		if _, exists := guestObjectsMap[supervisorObject.Name]; !exists {
			log.Infof("FullSync: Deleting CnsVolumeMetadata %v on the supervisor cluster for entity type %q",
				supervisorObject.Name, supervisorObject.Spec.EntityType)
			if err := metadataSyncer.cnsOperatorClient.Delete(ctx, &supervisorObject); err != nil {
				log.Warnf("FullSync: Failed to delete CnsVolumeMetadata %v. Err: %v", supervisorObject.Name, err)
			}
		}
	}

	log.Infof("FullSync: End")
	return nil
}

// createCnsVolumeMetadataList creates cnsvolumemetadata objects from the API
// server using the input k8s client.
// All objects that can be created are added to returnList. This includes
// PERSISTENT_VOLUME, PERSISTENT_VOLUME_CLAIM and POD entity types.
func createCnsVolumeMetadataList(ctx context.Context, metadataSyncer *metadataSyncInformer,
	supervisorNamespace string, returnList *cnsvolumemetadatav1alpha1.CnsVolumeMetadataList) error {
	log := logger.GetLogger(ctx)
	log.Debugf("FullSync: Querying guest cluster API server for all PV objects.")
	pvList, err := getPVsInBoundAvailableOrReleased(ctx, metadataSyncer)
	if err != nil {
		log.Errorf("FullSync: Failed to get PVs from guest cluster. Err: %v", err)
		return err
	}

	// Structure to map PVC names to corresponding volume handles.
	pvcToVolumeName := make(map[string]string)

	// Create cnsvolumemetadata objects for PV and PVC entity types.
	for _, pv := range pvList {
		var volumeNames []string
		volumeNames = append(volumeNames, pv.Spec.CSI.VolumeHandle)

		// Get the cnsvolumemetadata object for this pv and add it to the return
		// list.
		entityReference := cnsvolumemetadatav1alpha1.GetCnsOperatorEntityReference(
			pv.Spec.CSI.VolumeHandle, supervisorNamespace, cnsvolumemetadatav1alpha1.CnsOperatorEntityTypePVC, "")
		pvObject := cnsvolumemetadatav1alpha1.CreateCnsVolumeMetadataSpec(
			volumeNames, metadataSyncer.configInfo.Cfg.GC, string(pv.UID), pv.Name,
			cnsvolumemetadatav1alpha1.CnsOperatorEntityTypePV, pv.Labels, "",
			[]cnsvolumemetadatav1alpha1.CnsOperatorEntityReference{entityReference})
		returnList.Items = append(returnList.Items, *pvObject)

		// Get the cnsvolumemetadata object for pvc bound to this pv and add it
		// to the return list.
		if pv.Spec.ClaimRef != nil && pv.Status.Phase == v1.VolumeBound {
			pvc, err := metadataSyncer.pvcLister.PersistentVolumeClaims(pv.Spec.ClaimRef.Namespace).Get(
				pv.Spec.ClaimRef.Name)
			if err != nil {
				log.Errorf("FullSync: Failed to get PVC %q from guest cluster. Err: %v", pvc.Name, err)
				return err
			}
			entityReference := cnsvolumemetadatav1alpha1.GetCnsOperatorEntityReference(
				pvc.Spec.VolumeName, "", cnsvolumemetadatav1alpha1.CnsOperatorEntityTypePV,
				metadataSyncer.configInfo.Cfg.GC.TanzuKubernetesClusterUID)
			pvcObject := cnsvolumemetadatav1alpha1.CreateCnsVolumeMetadataSpec(
				volumeNames, metadataSyncer.configInfo.Cfg.GC, string(pvc.UID), pvc.Name,
				cnsvolumemetadatav1alpha1.CnsOperatorEntityTypePVC, pvc.GetLabels(), pvc.Namespace,
				[]cnsvolumemetadatav1alpha1.CnsOperatorEntityReference{entityReference})
			returnList.Items = append(returnList.Items, *pvcObject)
			pvcToVolumeName[pvc.Name] = pv.Spec.CSI.VolumeHandle
		}
	}

	log.Debugf("FullSync: Querying guest cluster API server for all POD objects.")
	pods, err := metadataSyncer.podLister.Pods(v1.NamespaceAll).List(labels.Everything())
	if err != nil {
		log.Errorf("FullSync: Failed to get all PODs from the guest cluster. Err: %v", err)
		return err
	}
	// Create cnsvolumemetadata objects for POD entity types.
	for _, pod := range pods {
		var entityReferences []cnsvolumemetadatav1alpha1.CnsOperatorEntityReference
		var volumeNames []string
		for _, volume := range pod.Spec.Volumes {
			if volume.VolumeSource.PersistentVolumeClaim == nil {
				continue
			}
			volumeName, ok := pvcToVolumeName[volume.VolumeSource.PersistentVolumeClaim.ClaimName]
			if !ok {
				log.Debugf("FullSync: PVC %q claimed by Pod %q is not a CSI vSphere Volume",
					volume.VolumeSource.PersistentVolumeClaim.ClaimName, pod.Name)
				continue
			}
			entityReferences = append(entityReferences,
				cnsvolumemetadatav1alpha1.GetCnsOperatorEntityReference(
					volume.VolumeSource.PersistentVolumeClaim.ClaimName, pod.Namespace,
					cnsvolumemetadatav1alpha1.CnsOperatorEntityTypePVC,
					metadataSyncer.configInfo.Cfg.GC.TanzuKubernetesClusterUID))
			volumeNames = append(volumeNames, volumeName)
		}
		if len(volumeNames) > 0 {
			log.Debugf("Pod %q claims vsphere volumes %v", pod.Name, volumeNames)
			podObject := cnsvolumemetadatav1alpha1.CreateCnsVolumeMetadataSpec(
				volumeNames, metadataSyncer.configInfo.Cfg.GC, string(pod.UID), pod.Name,
				cnsvolumemetadatav1alpha1.CnsOperatorEntityTypePOD, nil, pod.Namespace, entityReferences)
			returnList.Items = append(returnList.Items, *podObject)
		}
	}
	return nil
}

// compareCnsVolumeMetadatas compares input cnsvolumemetadata objects
// and returns false if their labels are not deeply equal.
func compareCnsVolumeMetadatas(guestObject *cnsvolumemetadatav1alpha1.CnsVolumeMetadataSpec,
	supervisorObject *cnsvolumemetadatav1alpha1.CnsVolumeMetadataSpec) bool {
	if !reflect.DeepEqual(guestObject.Labels, supervisorObject.Labels) ||
		!reflect.DeepEqual(guestObject.ClusterDistribution, supervisorObject.ClusterDistribution) {
		supervisorObject.Labels = guestObject.Labels
		supervisorObject.ClusterDistribution = guestObject.ClusterDistribution
		return false
	}
	return true
}
