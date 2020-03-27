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
	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	clientset "k8s.io/client-go/kubernetes"
	"sigs.k8s.io/vsphere-csi-driver/pkg/csi/service/logger"
)

func csiGetVolumeHealthStatus(ctx context.Context, k8sclient clientset.Interface, metadataSyncer *metadataSyncInformer) {
	log := logger.GetLogger(ctx)
	log.Infof("csiGetVolumeHealthStatus: start")

	// Get K8s PVs in State "Bound"
	k8sPVs, err := getBoundPVs(ctx, k8sclient)
	if err != nil {
		log.Errorf("csiGetVolumeHealthStatus: Failed to get PVs from kubernetes. Err: %+v", err)
		return
	}

	// volumeHandleToPvcMap maps pv.Spec.CSI.VolumeHandle to the pvc object which bounded to the pv
	volumeHandleToPvcMap := make(volumeHandleMap)

	for _, pv := range k8sPVs {
		if pv.Spec.ClaimRef != nil && pv.Status.Phase == v1.VolumeBound {
			pvc, err := k8sclient.CoreV1().PersistentVolumeClaims(pv.Spec.ClaimRef.Namespace).Get(pv.Spec.ClaimRef.Name, metav1.GetOptions{})
			if err != nil {
				log.Warnf("csiGetVolumeHealthStatus: Failed to get pvc for namespace %s and name %s. err=%+v", pv.Spec.ClaimRef.Namespace, pv.Spec.ClaimRef.Name, err)
				continue
			}
			volumeHandleToPvcMap[pv.Spec.CSI.VolumeHandle] = pvc
			log.Debugf("csiGetVolumeHealthStatus: pvc %s/%s is backed by pv %s volumeHandle %s", pvc.Namespace, pvc.Name, pv.Name, pv.Spec.CSI.VolumeHandle)
		}
	}

	//Call CNS QueryAll to get container volumes by cluster ID
	queryFilter := cnstypes.CnsQueryFilter{
		ContainerClusterIds: []string{
			metadataSyncer.configInfo.Cfg.Global.ClusterID,
		},
	}

	querySelection := cnstypes.CnsQuerySelection{
		Names: []string{
			"HEALTH_STATUS",
		},
	}

	queryAllResult, err := metadataSyncer.volumeManager.QueryAllVolume(ctx, queryFilter, querySelection)
	if err != nil {
		log.Errorf("csiGetVolumeHealthStatus: failed to queryAllVolume with err %+v", err)
		return
	}

	for _, vol := range queryAllResult.Volumes {
		log.Infof("Volume %q Health Status %q", vol.VolumeId.Id, vol.HealthStatus)

		if pvc, ok := volumeHandleToPvcMap[vol.VolumeId.Id]; ok {
			log.Debugf("csiGetVolumeHealthStatus: Found pvc %q for volume %q", pvc, vol.VolumeId.Id)

			// only update PVC health annotation if the HealthStatus of volume is not "unknown"
			if vol.HealthStatus != volHealthStatusUnknown {
				volHealthStatus, err := convertVolumeHealthStatus(vol.HealthStatus)
				if err != nil {
					log.Errorf("csiGetVolumeHealthStatus: invalid health status %q for volume %q", vol.HealthStatus, vol.VolumeId.Id)
				}
				if val, found := pvc.Annotations[annVolumeHealth]; !found || val != volHealthStatus {
					// VolumeHealth annotation on pvc is changed, set it to new value
					log.Infof("csiGetVolumeHealthStatus: update volume health annotation for pvc %s/%s from old value %s to new value %s", pvc.Namespace, pvc.Name, val, volHealthStatus)
					metav1.SetMetaDataAnnotation(&pvc.ObjectMeta, annVolumeHealth, volHealthStatus)
					_, err := k8sclient.CoreV1().PersistentVolumeClaims(pvc.Namespace).Update(pvc)
					if err != nil {
						log.Errorf("csiGetVolumeHealthStatus: Failed to update pvc %s/%s with err:%+v", pvc.Namespace, pvc.Name, err)
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
	case volHealthStatusRed:
		return volHealthStatusInAccessible, nil
	case volHealthStatusGreen:
		return volHealthStatusAccessible, nil
	case volHealthStatusYellow:
		return volHealthStatusAccessible, nil
	case volHealthStatusUnknown:
		return volHealthStatusUnknown, nil
	default:
		return "", fmt.Errorf("cannot convert invalid volume health status %s", volHealthStatus)
	}
}
