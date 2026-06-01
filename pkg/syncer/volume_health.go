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
	ctrlclient "sigs.k8s.io/controller-runtime/pkg/client"
	fvv1alpha1 "sigs.k8s.io/vsphere-csi-driver/v3/pkg/apis/filevolume/v1alpha1"
	"sigs.k8s.io/vsphere-csi-driver/v3/pkg/common/utils"

	"sigs.k8s.io/vsphere-csi-driver/v3/pkg/common/prometheus"
	"sigs.k8s.io/vsphere-csi-driver/v3/pkg/csi/service/common"
	"sigs.k8s.io/vsphere-csi-driver/v3/pkg/csi/service/logger"
	k8s "sigs.k8s.io/vsphere-csi-driver/v3/pkg/kubernetes"
)

func csiGetVolumeHealthStatus(ctx context.Context, k8sclient clientset.Interface,
	metadataSyncer *metadataSyncInformer) {
	log := logger.GetLogger(ctx)
	log.Infof("csiGetVolumeHealthStatus: start")

	// Get K8s PVs in State "Bound".
	k8sPVs, err := getBoundPVs(ctx, metadataSyncer)
	if err != nil {
		log.Errorf("csiGetVolumeHealthStatus: Failed to get PVs from kubernetes. Err: %+v", err)
		return
	}

	// volumeHandleToPvcMap maps pv.Spec.CSI.VolumeHandle to the pvc object which
	// bounded to the pv.
	volumeHandleToPvcMap := make(volumeHandlePVCMap, len(k8sPVs))
	// fvsVolumeHandleToPvcMap holds FVS-backed PVs separately when the FSS is enabled.
	var fvsVolumeHandleToPvcMap volumeHandlePVCMap

	if IsVsanFileVolumeServiceEnabled {
		fvsVolumeHandleToPvcMap = make(volumeHandlePVCMap)
	}

	for _, pv := range k8sPVs {
		if pv.Spec.ClaimRef != nil && pv.Status.Phase == v1.VolumeBound {
			pvc, err := metadataSyncer.pvcLister.PersistentVolumeClaims(
				pv.Spec.ClaimRef.Namespace).Get(pv.Spec.ClaimRef.Name)
			if err != nil {
				log.Warnf("csiGetVolumeHealthStatus: Failed to get pvc for namespace %s and name %s. err=%+v",
					pv.Spec.ClaimRef.Namespace, pv.Spec.ClaimRef.Name, err)
				continue
			}
			if IsVsanFileVolumeServiceEnabled && common.IsFVSStorageClassName(pv.Spec.StorageClassName) {
				fvsVolumeHandleToPvcMap[pv.Spec.CSI.VolumeHandle] = pvc
				log.Debugf("csiGetVolumeHealthStatus: FVS pvc %s/%s is backed by pv %s volumeHandle %s",
					pvc.Namespace, pvc.Name, pv.Name, pv.Spec.CSI.VolumeHandle)
			} else {
				volumeHandleToPvcMap[pv.Spec.CSI.VolumeHandle] = pvc
				log.Debugf("csiGetVolumeHealthStatus: pvc %s/%s is backed by pv %s volumeHandle %s",
					pvc.Namespace, pvc.Name, pv.Name, pv.Spec.CSI.VolumeHandle)
			}
		}
	}

	accessibleVolumeCount := 0
	inaccessibleVolumeCount := 0

	// Process FVS-backed volumes by reading FileVolume CR conditions.
	if IsVsanFileVolumeServiceEnabled && len(fvsVolumeHandleToPvcMap) > 0 {
		fvsAccessible, fvsInaccessible := getFileVolumeHealthStatus(ctx, k8sclient,
			metadataSyncer.fileVolumeClient, fvsVolumeHandleToPvcMap)
		accessibleVolumeCount += fvsAccessible
		inaccessibleVolumeCount += fvsInaccessible
	}

	// Process non-FVS volumes via CNS query (legacy path).
	if len(volumeHandleToPvcMap) > 0 {
		querySelection := cnstypes.CnsQuerySelection{
			Names: []string{
				string(cnstypes.QuerySelectionNameTypeHealthStatus),
			},
		}
		queryAllResult, err := utils.QueryAllVolumesForCluster(ctx, metadataSyncer.volumeManager,
			clusterIDforVolumeMetadata, querySelection)
		if err != nil {
			log.Errorf("csiGetVolumeHealthStatus: failed to QueryAllVolume with err=%+v", err.Error())
		} else {
			volumeIdToHealthStatusMap := make(volumeIdHealthStatusMap, len(queryAllResult.Volumes))
			for _, vol := range queryAllResult.Volumes {
				volumeIdToHealthStatusMap[vol.VolumeId.Id] = vol.HealthStatus
			}

			for volID, pvc := range volumeHandleToPvcMap {
				var volHealthStatusAnn string
				if volHealthStatus, ok := volumeIdToHealthStatusMap[volID]; ok {
					if volHealthStatus != string(pbmtypes.PbmHealthStatusForEntityUnknown) {
						volHealthStatusAnn, err = common.ConvertVolumeHealthStatus(ctx, volID, volHealthStatus)
						if err != nil {
							log.Errorf("csiGetVolumeHealthStatus: invalid health status %q for volume %q",
								volHealthStatus, volID)
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
					volHealthStatusAnn = common.VolHealthStatusInaccessible
					updateVolumeHealthStatus(ctx, k8sclient, pvc, volHealthStatusAnn)
				}
				switch volHealthStatusAnn {
				case common.VolHealthStatusAccessible:
					accessibleVolumeCount++
				case common.VolHealthStatusInaccessible:
					inaccessibleVolumeCount++
				}
			}
		}
	}

	prometheus.VolumeHealthGaugeVec.WithLabelValues(
		prometheus.PrometheusAccessibleVolumes).Set(float64(accessibleVolumeCount))
	prometheus.VolumeHealthGaugeVec.WithLabelValues(
		prometheus.PrometheusInaccessibleVolumes).Set(float64(inaccessibleVolumeCount))

	log.Infof("csiGetVolumeHealthStatus: end")
}

// getFileVolumeHealthStatus derives volume health from FileVolume CR conditions
// for FVS-backed PVCs. A volume is "accessible" only when both BackendReady and
// ExportReady conditions are True; any other state (False, absent, or still
// provisioning) is "inaccessible".
// Returns the count of accessible and inaccessible volumes processed.
func getFileVolumeHealthStatus(ctx context.Context, k8sclient clientset.Interface,
	fvClient ctrlclient.Client, fvsVolumes volumeHandlePVCMap) (int, int) {
	log := logger.GetLogger(ctx)
	accessibleCount := 0
	inaccessibleCount := 0

	for volHandle, pvc := range fvsVolumes {
		instanceNS, fvName, err := common.ParseFVSVolumeHandle(volHandle)
		if err != nil {
			log.Errorf("getFileVolumeHealthStatus: %v, marking pvc %s/%s inaccessible",
				err, pvc.Namespace, pvc.Name)
			updateVolumeHealthStatus(ctx, k8sclient, pvc, common.VolHealthStatusInaccessible)
			inaccessibleCount++
			continue
		}

		fv := &fvv1alpha1.FileVolume{}
		if err := fvClient.Get(ctx, ctrlclient.ObjectKey{Namespace: instanceNS, Name: fvName}, fv); err != nil {
			log.Errorf("getFileVolumeHealthStatus: failed to get FileVolume %s/%s for pvc %s/%s: %v",
				instanceNS, fvName, pvc.Namespace, pvc.Name, err)
			updateVolumeHealthStatus(ctx, k8sclient, pvc, common.VolHealthStatusInaccessible)
			inaccessibleCount++
			continue
		}

		healthStatus := deriveHealthFromFileVolumeConditions(fv.Status.Conditions)
		log.Infof("getFileVolumeHealthStatus: FileVolume %s/%s health=%s for pvc %s/%s",
			instanceNS, fvName, healthStatus, pvc.Namespace, pvc.Name)
		updateVolumeHealthStatus(ctx, k8sclient, pvc, healthStatus)

		switch healthStatus {
		case common.VolHealthStatusAccessible:
			accessibleCount++
		case common.VolHealthStatusInaccessible:
			inaccessibleCount++
		}
	}
	return accessibleCount, inaccessibleCount
}

// deriveHealthFromFileVolumeConditions examines the FileVolume CR conditions and
// returns "accessible" only when both BackendReady and ExportReady are True.
func deriveHealthFromFileVolumeConditions(conditions []metav1.Condition) string {
	backendReady := false
	exportReady := false
	for _, c := range conditions {
		if c.Type == common.FileVolumeConditionBackendReady && c.Status == metav1.ConditionTrue {
			backendReady = true
		}
		if c.Type == common.FileVolumeConditionExportReady && c.Status == metav1.ConditionTrue {
			exportReady = true
		}
	}
	if backendReady && exportReady {
		return common.VolHealthStatusAccessible
	}
	return common.VolHealthStatusInaccessible
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
		
		// Patch PVC with annotations
		restClientConfig, err := k8s.GetKubeConfig(ctx)
		if err != nil {
			log.Errorf("updateVolumeHealthStatus: Failed to get kubeconfig. Err: %v", err)
			return
		}
		coreV1Client, err := k8s.NewClientForGroup(ctx, restClientConfig, v1.SchemeGroupVersion.Group)
		if err != nil {
			log.Errorf("updateVolumeHealthStatus: Failed to create client. Err: %v", err)
			return
		}
		originalPVC := pvc.DeepCopy()
		// Reset annotations to original for proper patch
		originalPVC.Annotations = make(map[string]string)
		for k, v := range pvc.Annotations {
			originalPVC.Annotations[k] = v
		}
		delete(originalPVC.Annotations, annVolumeHealth)
		delete(originalPVC.Annotations, annVolumeHealthTS)
		
		err = k8s.PatchObject(ctx, coreV1Client, originalPVC, pvc)
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
					
					originalNewPVC := newPvc.DeepCopy()
					metav1.SetMetaDataAnnotation(&newPvc.ObjectMeta, annVolumeHealth, volHealthStatus)
					metav1.SetMetaDataAnnotation(&newPvc.ObjectMeta, annVolumeHealthTS, timeUpdate)
					
					err := k8s.PatchObject(ctx, coreV1Client, originalNewPVC, newPvc)
					if err != nil {
						log.Errorf("updateVolumeHealthStatus: Failed to patch pvc %s/%s with err:%+v",
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
