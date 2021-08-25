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

package syncer

import (
	"context"
	"time"

	cnstypes "github.com/vmware/govmomi/cns/types"
	pbmtypes "github.com/vmware/govmomi/pbm/types"
	v1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	clientset "k8s.io/client-go/kubernetes"
	"sigs.k8s.io/vsphere-csi-driver/v2/pkg/common/utils"
	"sigs.k8s.io/vsphere-csi-driver/v2/pkg/csi/service/common"
	"sigs.k8s.io/vsphere-csi-driver/v2/pkg/csi/service/logger"
)

func csiGetVolumeHealthStatus(ctx context.Context, k8sclient clientset.Interface,
	metadataSyncer *metadataSyncInformer) {
	log := logger.GetLogger(ctx)
	log.Infof("csiGetVolumeHealthStatus: start")

	// Call CNS QueryAll to get container volumes by cluster ID.
	queryFilter := cnstypes.CnsQueryFilter{
		ContainerClusterIds: []string{
			metadataSyncer.configInfo.Cfg.Global.ClusterID,
		},
	}

	querySelection := cnstypes.CnsQuerySelection{
		Names: []string{
			string(cnstypes.QuerySelectionNameTypeHealthStatus),
		},
	}
	queryResult, err := utils.QueryAllVolumeUtil(ctx, metadataSyncer.volumeManager, queryFilter,
		querySelection, metadataSyncer.coCommonInterface.IsFSSEnabled(ctx, common.AsyncQueryVolume))
	if err != nil {
		log.Errorf("csiGetVolumeHealthStatus: QueryVolume failed with err=%+v", err.Error())
		return
	}

	// Get K8s PVs in State "Bound".
	k8sPVs, err := getBoundPVs(ctx, metadataSyncer)
	if err != nil {
		log.Errorf("csiGetVolumeHealthStatus: Failed to get PVs from kubernetes. Err: %+v", err)
		return
	}

	// volumeHandleToPvcMap maps pv.Spec.CSI.VolumeHandle to the pvc object which
	// bounded to the pv.
	volumeHandleToPvcMap := make(volumeHandlePVCMap, len(k8sPVs))

	for _, pv := range k8sPVs {
		if pv.Spec.ClaimRef != nil && pv.Status.Phase == v1.VolumeBound {
			pvc, err := metadataSyncer.pvcLister.PersistentVolumeClaims(
				pv.Spec.ClaimRef.Namespace).Get(pv.Spec.ClaimRef.Name)
			if err != nil {
				log.Warnf("csiGetVolumeHealthStatus: Failed to get pvc for namespace %s and name %s. err=%+v",
					pv.Spec.ClaimRef.Namespace, pv.Spec.ClaimRef.Name, err)
				continue
			}
			volumeHandleToPvcMap[pv.Spec.CSI.VolumeHandle] = pvc
			log.Debugf("csiGetVolumeHealthStatus: pvc %s/%s is backed by pv %s volumeHandle %s",
				pvc.Namespace, pvc.Name, pv.Name, pv.Spec.CSI.VolumeHandle)
		}
	}

	// volumeIdToHealthStatusMap maps vol.VolumeId.Id to vol.HealthStatus.
	volumeIdToHealthStatusMap := make(volumeIdHealthStatusMap, len(queryResult.Volumes))

	for _, vol := range queryResult.Volumes {
		volumeIdToHealthStatusMap[vol.VolumeId.Id] = vol.HealthStatus
	}

	for volID, pvc := range volumeHandleToPvcMap {
		if volHealthStatus, ok := volumeIdToHealthStatusMap[volID]; ok {
			// Only update PVC health annotation if the HealthStatus of volume is
			// not "unknown".
			if volHealthStatus != string(pbmtypes.PbmHealthStatusForEntityUnknown) {
				volHealthStatusAnn, err := common.ConvertVolumeHealthStatus(ctx, volID, volHealthStatus)
				if err != nil {
					log.Errorf("csiGetVolumeHealthStatus: invalid health status %q for volume %q", volHealthStatus, volID)
				}
				updateVolumeHealthStatus(ctx, k8sclient, pvc, volHealthStatusAnn)
			}
		} else {
			// Set volume health status as "Inaccessible" when PVC is not found in
			// CNS. When a Datastore is removed from VC (like vSAN direct disk
			// decommisson with noAction does), the CNS Volumes on that Datastore
			// are eventually removed from CNS DB, but the PVCs still remain in
			// the K8S cluster. We are making the design choice of reflecting the
			// CNS cached status of health on the PVC's health annotation at any
			// given point of time. The vDPp operators are advised to look at the
			// health change timestamp and wait "long enough" (like an hour) before
			// taking any corrective actions. So if the PVC health is getting
			// updated every 5 mins, wait for an hour or so before taking any
			// corrective actions. This is an acceptable level of eventual
			// consistency.
			updateVolumeHealthStatus(ctx, k8sclient, pvc, common.VolHealthStatusInaccessible)
		}
	}
	log.Infof("GetVolumeHealthStatus: end")
}

func updateVolumeHealthStatus(ctx context.Context, k8sclient clientset.Interface,
	pvc *v1.PersistentVolumeClaim, volHealthStatus string) {
	log := logger.GetLogger(ctx)

	val, found := pvc.Annotations[annVolumeHealth]
	_, foundAnnHealthTS := pvc.Annotations[annVolumeHealthTS]
	if !found || val != volHealthStatus || !foundAnnHealthTS {
		// VolumeHealth annotation on pvc is changed, set it to new value.
		metav1.SetMetaDataAnnotation(&pvc.ObjectMeta, annVolumeHealth, volHealthStatus)
		timeNow := time.Now().Format(time.UnixDate)
		metav1.SetMetaDataAnnotation(&pvc.ObjectMeta, annVolumeHealthTS, timeNow)
		log.Infof("updateVolumeHealthStatus: set volumehealth annotation for pvc %s/%s from old "+
			"value %s to new value %s and volumehealthTS annotation to %s",
			pvc.Namespace, pvc.Name, val, volHealthStatus, timeNow)
		_, err := k8sclient.CoreV1().PersistentVolumeClaims(pvc.Namespace).Update(ctx, pvc, metav1.UpdateOptions{})
		if err != nil {
			if apierrors.IsConflict(err) {
				log.Debugf("updateVolumeHealthStatus: Failed to update pvc %s/%s with err:%+v, will retry the update",
					pvc.Namespace, pvc.Name, err)
				// pvc get from pvcLister may be stale, try to get updated pvc which
				// bound to pv from API server.
				newPvc, err := k8sclient.CoreV1().PersistentVolumeClaims(pvc.Namespace).Get(
					ctx, pvc.Name, metav1.GetOptions{})
				if err == nil {
					timeUpdate := time.Now().Format(time.UnixDate)
					log.Infof("updateVolumeHealthStatus: updating volume health annotation for pvc %s/%s which "+
						"get from API server from old value %s to new value %s and volumehealthTS annotation to %s",
						newPvc.Namespace, newPvc.Name, val, volHealthStatus, timeUpdate)
					metav1.SetMetaDataAnnotation(&newPvc.ObjectMeta, annVolumeHealth, volHealthStatus)
					metav1.SetMetaDataAnnotation(&newPvc.ObjectMeta, annVolumeHealthTS, timeUpdate)
					_, err := k8sclient.CoreV1().PersistentVolumeClaims(newPvc.Namespace).Update(ctx,
						newPvc, metav1.UpdateOptions{})
					if err != nil {
						log.Errorf("updateVolumeHealthStatus: Failed to update pvc %s/%s with err:%+v",
							newPvc.Namespace, newPvc.Name, err)
					}
				} else {
					log.Errorf("updateVolumeHealthStatus: volume health annotation for pvc %s/%s is not updated because "+
						"failed to get pvc from API server. err=%+v",
						pvc.Namespace, pvc.Name, err)
				}
			} else {
				log.Errorf("updateVolumeHealthStatus: Failed to update pvc %s/%s with err:%+v",
					pvc.Namespace, pvc.Name, err)
			}
		}
	}
}
