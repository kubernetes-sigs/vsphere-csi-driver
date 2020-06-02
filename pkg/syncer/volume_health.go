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
	"fmt"

	cnstypes "github.com/vmware/govmomi/cns/types"
	pbmtypes "github.com/vmware/govmomi/pbm/types"
	v1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	clientset "k8s.io/client-go/kubernetes"
	"sigs.k8s.io/vsphere-csi-driver/pkg/csi/service/logger"
)

func csiGetVolumeHealthStatus(ctx context.Context, k8sclient clientset.Interface, metadataSyncer *metadataSyncInformer) {
	log := logger.GetLogger(ctx)
	log.Infof("csiGetVolumeHealthStatus: start")

	//Call CNS QueryAll to get container volumes by cluster ID
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

	queryAllResult, err := metadataSyncer.volumeManager.QueryAllVolume(ctx, queryFilter, querySelection)
	if err != nil {
		log.Errorf("csiGetVolumeHealthStatus: failed to queryAllVolume with err %+v", err)
		return
	}

	// Get K8s PVs in State "Bound"
	k8sPVs, err := getBoundPVs(ctx, metadataSyncer)
	if err != nil {
		log.Errorf("csiGetVolumeHealthStatus: Failed to get PVs from kubernetes. Err: %+v", err)
		return
	}

	// volumeHandleToPvcMap maps pv.Spec.CSI.VolumeHandle to the pvc object which bounded to the pv
	volumeHandleToPvcMap := make(volumeHandlePVCMap, len(k8sPVs))

	for _, pv := range k8sPVs {
		if pv.Spec.ClaimRef != nil && pv.Status.Phase == v1.VolumeBound {
			pvc, err := metadataSyncer.pvcLister.PersistentVolumeClaims(pv.Spec.ClaimRef.Namespace).Get(pv.Spec.ClaimRef.Name)
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

	for _, vol := range queryAllResult.Volumes {
		log.Debugf("Volume %q Health Status %q", vol.VolumeId.Id, vol.HealthStatus)

		if pvc, ok := volumeHandleToPvcMap[vol.VolumeId.Id]; ok {
			log.Debugf("csiGetVolumeHealthStatus: Found pvc %q for volume %q", pvc, vol.VolumeId.Id)

			// only update PVC health annotation if the HealthStatus of volume is not "unknown"
			if vol.HealthStatus != string(pbmtypes.PbmHealthStatusForEntityUnknown) {
				volHealthStatus, err := convertVolumeHealthStatus(vol.HealthStatus)
				if err != nil {
					log.Errorf("csiGetVolumeHealthStatus: invalid health status %q for volume %q", vol.HealthStatus, vol.VolumeId.Id)
				}
				if val, found := pvc.Annotations[annVolumeHealth]; !found || val != volHealthStatus {
					// VolumeHealth annotation on pvc is changed, set it to new value
					log.Debugf("csiGetVolumeHealthStatus: update volume health annotation for pvc %s/%s from old value %s to new value %s",
					pvc.Namespace, pvc.Name, val, volHealthStatus)
					metav1.SetMetaDataAnnotation(&pvc.ObjectMeta, annVolumeHealth, volHealthStatus)
					_, err := k8sclient.CoreV1().PersistentVolumeClaims(pvc.Namespace).Update(pvc)
					if err != nil {
						if apierrors.IsConflict(err) {
							log.Debugf("csiGetVolumeHealthStatus: Failed to update pvc %s/%s with err:%+v, will retry the update",
							pvc.Namespace, pvc.Name, err)
							// pvc get from pvcLister may be stale, try to get updated pvc which bound to pv from API server
							newPvc, err := k8sclient.CoreV1().PersistentVolumeClaims(pvc.Namespace).Get(pvc.Name, metav1.GetOptions{})
							if err == nil {
								log.Debugf("csiGetVolumeHealthStatus: update volume health annotation for pvc %s/%s which "+
								"get from API server from old value %s to new value %s",
								newPvc.Namespace, newPvc.Name, val, volHealthStatus)
								metav1.SetMetaDataAnnotation(&newPvc.ObjectMeta, annVolumeHealth, volHealthStatus)
								_, err := k8sclient.CoreV1().PersistentVolumeClaims(newPvc.Namespace).Update(newPvc)
								if err != nil {
									log.Errorf("csiGetVolumeHealthStatus: Failed to update pvc %s/%s with err:%+v",
									newPvc.Namespace, newPvc.Name, err)
								}
							} else {
								log.Errorf("csiGetVolumeHealthStatus: volume health annotation for pvc %s/%s is not updated because "+
								"failed to get pvc from API server. err=%+v",
								pvc.Namespace, pvc.Name, err)
							}
						} else {
							log.Errorf("csiGetVolumeHealthStatus: Failed to update pvc %s/%s with err:%+v",
							pvc.Namespace, pvc.Name, err)
						}
					}
				}
			}
		}
	}
	log.Infof("GetVolumeHealthStatus: end")
}

// convertVolumeHealthStatus convert the volume health status into accessible/inaccessible status
func convertVolumeHealthStatus(volHealthStatus string) (string, error) {
	switch volHealthStatus {
	case string(pbmtypes.PbmHealthStatusForEntityRed):
		return volHealthStatusInAccessible, nil
	case string(pbmtypes.PbmHealthStatusForEntityGreen):
		return volHealthStatusAccessible, nil
	case string(pbmtypes.PbmHealthStatusForEntityYellow):
		return volHealthStatusAccessible, nil
	case string(pbmtypes.PbmHealthStatusForEntityUnknown):
		return string(pbmtypes.PbmHealthStatusForEntityUnknown), nil
	default:
		return "", fmt.Errorf("cannot convert invalid volume health status %s", volHealthStatus)
	}
}
