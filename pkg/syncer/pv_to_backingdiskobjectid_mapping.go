/*
Copyright 2022 The Kubernetes Authors.

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
	"strings"

	cnstypes "github.com/vmware/govmomi/cns/types"
	v1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	clientset "k8s.io/client-go/kubernetes"

	"sigs.k8s.io/vsphere-csi-driver/v3/pkg/csi/service/logger"
)

// TODO: refactor pv to backingdiskobjectid and volume health code to reduce duplicated code
func csiGetPVtoBackingDiskObjectIdMapping(ctx context.Context, k8sclient clientset.Interface,
	metadataSyncer *metadataSyncInformer) {
	log := logger.GetLogger(ctx)
	log.Debug("csiGetPVtoBackingDiskObjectIdMapping: start")
	// Call CNS QueryAll to get container volumes by cluster ID.
	queryFilter := cnstypes.CnsQueryFilter{
		ContainerClusterIds: []string{
			clusterIDforVolumeMetadata,
		},
	}

	querySelection := cnstypes.CnsQuerySelection{
		Names: []string{
			string(cnstypes.QuerySelectionNameTypeBackingObjectDetails),
			string(cnstypes.QuerySelectionNameTypeVolumeType),
		},
	}
	queryAllResult, err := metadataSyncer.volumeManager.QueryAllVolume(ctx, queryFilter, querySelection)
	if err != nil {
		log.Errorf("csiGetPVtoBackingDiskObjectIdMapping: failed to QueryAllVolume with err=%+v", err.Error())
		return
	}

	// Get K8s PVs in State "Bound".
	k8sPVs, err := getBoundPVs(ctx, metadataSyncer)
	if err != nil {
		log.Errorf("csiGetPVtoBackingDiskObjectIdMapping: Failed to get PVs from kubernetes. Err: %+v", err)
		return
	}

	// volumeHandleToPvcMap maps pv.Spec.CSI.VolumeHandle to the pvc object which
	// bounded to the pv.
	volumeHandleToPvcMap := make(volumeHandlePVCMap, len(k8sPVs))
	// volumeHandleToPvUidMap maps volumehandle to pv uid
	volumeHandleToPvUidMap := make(volumeIdToPvUidMap, len(k8sPVs))

	for _, pv := range k8sPVs {
		if pv.Spec.ClaimRef != nil && pv.Status.Phase == v1.VolumeBound {
			pvc, err := metadataSyncer.pvcLister.PersistentVolumeClaims(
				pv.Spec.ClaimRef.Namespace).Get(pv.Spec.ClaimRef.Name)
			if err != nil {
				log.Warnf("csiGetPVtoBackingDiskObjectIdMapping: Failed to get pvc for namespace %s and name %s. err=%+v",
					pv.Spec.ClaimRef.Namespace, pv.Spec.ClaimRef.Name, err)
				continue
			}
			volumeHandleToPvcMap[pv.Spec.CSI.VolumeHandle] = pvc
			log.Debugf("csiGetPVtoBackingDiskObjectIdMapping: pvc %s/%s is backed by pv %s volumeHandle %s, pv uid is %s",
				pvc.Namespace, pvc.Name, pv.Name, pv.Spec.CSI.VolumeHandle, string(pv.UID))
			volumeHandleToPvUidMap[pv.Spec.CSI.VolumeHandle] = string(pv.UID)
		}
	}

	// pv to backingDiskObjectId Map maps vol.VolumeId.Id to backingobjectId.
	volumeIdToBackingObjectIdMap := make(volumeIdToPvUidMap, len(queryAllResult.Volumes))

	for _, vol := range queryAllResult.Volumes {
		// NOTE: BackingDiskObjectId is the id of vvol or vSan; BackingDiskId is the same as VolumeId.
		// This is only supported for block volumes.
		if vol.VolumeType != string(cnstypes.CnsVolumeTypeBlock) {
			continue
		}
		val, ok := vol.BackingObjectDetails.(*cnstypes.CnsBlockBackingDetails)
		if ok {
			volumeIdToBackingObjectIdMap[vol.VolumeId.Id] = val.BackingDiskObjectId
		}
	}

	for volID, pvc := range volumeHandleToPvcMap {
		pvToBackingDiskObjectIdPair := volumeHandleToPvUidMap[volID] + ":" + volumeIdToBackingObjectIdMap[volID]
		updated, err := updatePVtoBackingDiskObjectIdMappingStatus(ctx, k8sclient, pvc, pvToBackingDiskObjectIdPair)
		if err != nil {
			log.Error("csiGetPVtoBackingDiskObjectIdMapping: Failed to update pv to backingDiskObjectId mapping")
			return
		}
		if updated {
			log.Infof("csiGetPVtoBackingDiskObjectIdMapping: pvc %s is updated with pv to backingDiskObjectId mapping %s",
				pvc.Name, pvToBackingDiskObjectIdPair)
		}
	}

	log.Debug("csiGetPVtoBackingDiskObjectIdMapping: end")
}

func validatePvToBackingDiskObjectIdPair(pvToBackingDiskObjectIdPair string) bool {
	split := strings.Split(pvToBackingDiskObjectIdPair, ":")
	if len(split) < 2 {
		return false
	}
	if split[0] == "" || split[1] == "" {
		return false
	}
	return true
}

func updatePVtoBackingDiskObjectIdMappingStatus(ctx context.Context, k8sclient clientset.Interface,
	pvc *v1.PersistentVolumeClaim, pvToBackingDiskObjectIdPair string) (bool, error) {
	log := logger.GetLogger(ctx)

	val, found := pvc.Annotations[annPVtoBackingDiskObjectId]
	isValidate := validatePvToBackingDiskObjectIdPair(pvToBackingDiskObjectIdPair)

	if !isValidate {
		if !found {
			log.Debugf("updatePVtoBackingDiskObjectIdMappingStatus: no previous mapping found in annotation. No need to update.")
			return false, nil
		} else {
			// If there is any previous pv to backingdiskobjectid mapping found,
			// set it as empty as queryAll returns nothing in this cycle
			pvToBackingDiskObjectIdPair = ""
		}
	}

	if !found || val != pvToBackingDiskObjectIdPair {
		// PVToBackingDiskObjectId annotation on pvc is changed, set it to new value.
		metav1.SetMetaDataAnnotation(&pvc.ObjectMeta, annPVtoBackingDiskObjectId, pvToBackingDiskObjectIdPair)

		log.Infof("updatePVtoBackingDiskObjectIdMappingStatus: set pv to backingdiskobjectid annotation for "+
			"pvc %s/%s from old value %s to new value %s",
			pvc.Namespace, pvc.Name, val, pvToBackingDiskObjectIdPair)
		_, err := k8sclient.CoreV1().PersistentVolumeClaims(pvc.Namespace).Update(ctx, pvc, metav1.UpdateOptions{})
		if err != nil {
			if apierrors.IsConflict(err) {
				log.Debugf("updatePVtoBackingDiskObjectIdMappingStatus: Failed to update pvc %s/%s with err:%+v, "+
					"will retry the update", pvc.Namespace, pvc.Name, err)
				// pvc get from pvcLister may be stale, try to get updated pvc which
				// bound to pv from API server.
				newPvc, err := k8sclient.CoreV1().PersistentVolumeClaims(pvc.Namespace).Get(
					ctx, pvc.Name, metav1.GetOptions{})
				if err != nil {
					log.Errorf("updatePVtoBackingDiskObjectIdMappingStatus: pv to backingdiskobjectid annotation for "+
						"pvc %s/%s is not updated because failed to get pvc from API server. err=%+v",
						pvc.Namespace, pvc.Name, err)
					return false, err
				}

				log.Infof("updatePVtoBackingDiskObjectIdMappingStatus: updating pv to backingdiskobjectid annotation "+
					"for pvc %s/%s which get from API server from old value %s to new value %s",
					newPvc.Namespace, newPvc.Name, val, pvToBackingDiskObjectIdPair)
				metav1.SetMetaDataAnnotation(&newPvc.ObjectMeta, annPVtoBackingDiskObjectId, pvToBackingDiskObjectIdPair)
				_, err = k8sclient.CoreV1().PersistentVolumeClaims(newPvc.Namespace).Update(ctx,
					newPvc, metav1.UpdateOptions{})
				if err != nil {
					log.Errorf("updatePVtoBackingDiskObjectIdMappingStatus: Failed to update pvc %s/%s with err:%+v",
						newPvc.Namespace, newPvc.Name, err)
					return false, err
				} else {
					return true, nil
				}
			} else {
				log.Errorf("updatePVtoBackingDiskObjectIdMappingStatus: pv to backingdiskobjectid annotation for "+
					"pvc %s/%s is not updated because failed to get pvc from API server. err=%+v",
					pvc.Namespace, pvc.Name, err)
				return false, err
			}

		}
		log.Debug("updatePVtoBackingDiskObjectIdMappingStatus: annotation on pvc updated")
		return true, nil
	}
	log.Debug("updatePVtoBackingDiskObjectIdMappingStatus: no change to annotation on pvc, skip update")
	return false, nil
}
