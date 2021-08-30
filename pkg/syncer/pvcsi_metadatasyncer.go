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

	"github.com/davecgh/go-spew/spew"
	v1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	cnsvolumemetadatav1alpha1 "sigs.k8s.io/vsphere-csi-driver/v2/pkg/apis/cnsoperator/cnsvolumemetadata/v1alpha1"
	cnsconfig "sigs.k8s.io/vsphere-csi-driver/v2/pkg/common/config"
	"sigs.k8s.io/vsphere-csi-driver/v2/pkg/csi/service/logger"
)

// pvcsiVolumeUpdated updates persistent volume claim and persistent volume
// CnsVolumeMetadata on supervisor cluster when pvc/pv labels on K8S cluster
// have been updated.
func pvcsiVolumeUpdated(ctx context.Context, resourceType interface{},
	volumeHandle string, metadataSyncer *metadataSyncInformer) {
	log := logger.GetLogger(ctx)
	supervisorNamespace, err := cnsconfig.GetSupervisorNamespace(ctx)
	if err != nil {
		log.Errorf("pvCSI VolumeUpdated: Unable to fetch supervisor namespace. Err: %v", err)
		return
	}
	var newMetadata *cnsvolumemetadatav1alpha1.CnsVolumeMetadata
	// Create CnsVolumeMetaDataSpec based on the resource type.
	switch resource := resourceType.(type) {
	case *v1.PersistentVolume:
		entityReference := cnsvolumemetadatav1alpha1.GetCnsOperatorEntityReference(
			resource.Spec.CSI.VolumeHandle, supervisorNamespace, cnsvolumemetadatav1alpha1.CnsOperatorEntityTypePVC, "")
		newMetadata = cnsvolumemetadatav1alpha1.CreateCnsVolumeMetadataSpec([]string{volumeHandle},
			metadataSyncer.configInfo.Cfg.GC, string(resource.GetUID()), resource.Name,
			cnsvolumemetadatav1alpha1.CnsOperatorEntityTypePV, resource.Labels, "",
			[]cnsvolumemetadatav1alpha1.CnsOperatorEntityReference{entityReference})
	case *v1.PersistentVolumeClaim:
		entityReference := cnsvolumemetadatav1alpha1.GetCnsOperatorEntityReference(
			resource.Spec.VolumeName, "", cnsvolumemetadatav1alpha1.CnsOperatorEntityTypePV,
			metadataSyncer.configInfo.Cfg.GC.TanzuKubernetesClusterUID)
		newMetadata = cnsvolumemetadatav1alpha1.CreateCnsVolumeMetadataSpec([]string{volumeHandle},
			metadataSyncer.configInfo.Cfg.GC, string(resource.GetUID()), resource.Name,
			cnsvolumemetadatav1alpha1.CnsOperatorEntityTypePVC, resource.Labels, resource.Namespace,
			[]cnsvolumemetadatav1alpha1.CnsOperatorEntityReference{entityReference})
	default:
	}
	// Check if cnsvolumemetadata object exists for this entity in the supervisor
	// cluster.
	currentMetadata := &cnsvolumemetadatav1alpha1.CnsVolumeMetadata{}
	key := types.NamespacedName{Namespace: supervisorNamespace, Name: newMetadata.Name}
	if err := metadataSyncer.cnsOperatorClient.Get(ctx, key, currentMetadata); err != nil {
		if apierrors.IsNotFound(err) {
			newMetadata.Namespace = supervisorNamespace
			if err := metadataSyncer.cnsOperatorClient.Create(ctx, newMetadata); err != nil {
				log.Errorf("pvCSI VolumeUpdated: Failed to create CnsVolumeMetadata: %v. Error: %v", newMetadata.Name, err)
			}
			return
		}
		log.Errorf("pvCSI VolumeUpdated: Unable to fetch CnsVolumeMetadata: %v. Error: %v", newMetadata.Name, err)
		return
	}
	newMetadata.ResourceVersion = currentMetadata.ResourceVersion
	newMetadata.Namespace = supervisorNamespace
	log.Debugf("pvCSI VolumeUpdated: Invoking update on CnsVolumeMetadata with spec: %+v", spew.Sdump(newMetadata))
	if err := metadataSyncer.cnsOperatorClient.Update(ctx, newMetadata); err != nil {
		log.Errorf("pvCSI VolumeUpdated: Failed to update CnsVolumeMetadata: %v. Error: %v", newMetadata.Name, err)
		return
	}
	log.Infof("pvCSI VolumeUpdated: Successfully updated CnsVolumeMetadata: %v", currentMetadata.Name)
}

// pvcsiVolumeDeleted deletes pvc/pv CnsVolumeMetadata on supervisor cluster
// when pvc/pv has been deleted on K8s cluster.
func pvcsiVolumeDeleted(ctx context.Context, uID string, metadataSyncer *metadataSyncInformer) {
	log := logger.GetLogger(ctx)
	supervisorNamespace, err := cnsconfig.GetSupervisorNamespace(ctx)
	if err != nil {
		log.Errorf("pvCSI VolumeDeleted: Unable to fetch supervisor namespace. Err: %v", err)
		return
	}
	volumeMetadataName := cnsvolumemetadatav1alpha1.GetCnsVolumeMetadataName(
		metadataSyncer.configInfo.Cfg.GC.TanzuKubernetesClusterUID, uID)
	log.Debugf("pvCSI VolumeDeleted: Invoking delete on CnsVolumeMetadata : %v", volumeMetadataName)
	err = metadataSyncer.cnsOperatorClient.Delete(ctx, &cnsvolumemetadatav1alpha1.CnsVolumeMetadata{
		ObjectMeta: metav1.ObjectMeta{
			Name:      volumeMetadataName,
			Namespace: supervisorNamespace,
		},
	})
	if err != nil {
		log.Errorf("pvCSI VolumeDeleted: Failed to delete CnsVolumeMetadata: %v. Error: %v", volumeMetadataName, err)
		return
	}
	log.Infof("pvCSI VolumeDeleted: Successfully deleted CnsVolumeMetadata: %v", volumeMetadataName)
}

// pvcsiUpdatePod creates/deletes cnsvolumemetadata for POD entities on the
// supervisor cluster when pod has been created/deleted on the guest cluster.
func pvcsiUpdatePod(ctx context.Context, pod *v1.Pod, metadataSyncer *metadataSyncInformer, deleteFlag bool) {
	log := logger.GetLogger(ctx)
	supervisorNamespace, err := cnsconfig.GetSupervisorNamespace(ctx)
	if err != nil {
		log.Errorf("pvCSI PODUpdatedDeleted: Unable to fetch supervisor namespace. Err: %v", err)
		return
	}
	var entityReferences []cnsvolumemetadatav1alpha1.CnsOperatorEntityReference
	var volumes []string
	// Iterate through volumes attached to pod.
	for _, volume := range pod.Spec.Volumes {
		if volume.PersistentVolumeClaim != nil {
			valid, pv, pvc := IsValidVolume(ctx, volume, pod, metadataSyncer)
			if valid {
				entityReferences = append(entityReferences,
					cnsvolumemetadatav1alpha1.GetCnsOperatorEntityReference(pvc.Name, pvc.Namespace,
						cnsvolumemetadatav1alpha1.CnsOperatorEntityTypePVC,
						metadataSyncer.configInfo.Cfg.GC.TanzuKubernetesClusterUID))
				volumes = append(volumes, pv.Spec.CSI.VolumeHandle)
			}
		}
	}
	if len(volumes) > 0 {
		if !deleteFlag {
			newMetadata := cnsvolumemetadatav1alpha1.CreateCnsVolumeMetadataSpec(volumes,
				metadataSyncer.configInfo.Cfg.GC, string(pod.GetUID()), pod.Name,
				cnsvolumemetadatav1alpha1.CnsOperatorEntityTypePOD, nil, pod.Namespace, entityReferences)
			log.Debugf("pvCSI PodUpdated: Invoking create CnsVolumeMetadata : %v", newMetadata)
			newMetadata.Namespace = supervisorNamespace
			if err := metadataSyncer.cnsOperatorClient.Create(ctx, newMetadata); err != nil {
				log.Errorf("pvCSI PodUpdated: Failed to create CnsVolumeMetadata: %v. Error: %v", newMetadata.Name, err)
				return
			}
			log.Infof("pvCSI PodUpdated: Successfully created CnsVolumeMetadata: %v", newMetadata.Name)
		} else {
			volumeMetadataName := cnsvolumemetadatav1alpha1.GetCnsVolumeMetadataName(
				metadataSyncer.configInfo.Cfg.GC.TanzuKubernetesClusterUID, string(pod.GetUID()))
			log.Debugf("pvCSI PodDeleted: Invoking delete on CnsVolumeMetadata : %v", volumeMetadataName)
			err = metadataSyncer.cnsOperatorClient.Delete(ctx, &cnsvolumemetadatav1alpha1.CnsVolumeMetadata{
				ObjectMeta: metav1.ObjectMeta{
					Name:      volumeMetadataName,
					Namespace: supervisorNamespace,
				},
			})
			if err != nil {
				log.Errorf("pvCSI PodDeleted: Failed to delete CnsVolumeMetadata: %v. Error: %v", volumeMetadataName, err)
				return
			}
			log.Infof("pvCSI PodDeleted: Successfully deleted CnsVolumeMetadata: %v", volumeMetadataName)
		}
	}
}
