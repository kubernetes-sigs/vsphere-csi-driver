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
	"sync"

	"github.com/davecgh/go-spew/spew"
	cnstypes "gitlab.eng.vmware.com/hatchway/govmomi/cns/types"
	"k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/fields"
	clientset "k8s.io/client-go/kubernetes"
	"k8s.io/klog"
	api "k8s.io/kubernetes/pkg/apis/core"
	cnsvsphere "sigs.k8s.io/vsphere-csi-driver/pkg/common/cns-lib/vsphere"
	"sigs.k8s.io/vsphere-csi-driver/pkg/csi/service/common"
)

// csiFullSync reconciles volume metadata on a vanilla k8s cluster
// with volume metadata on CNS
func csiFullSync(k8sclient clientset.Interface, metadataSyncer *metadataSyncInformer) {
	klog.V(2).Infof("FullSync: start")

	// Get K8s PVs in State "Bound", "Available" or "Released"
	k8sPVs, err := getPVsInBoundAvailableOrReleased(k8sclient)
	if err != nil {
		klog.Warningf("FullSync: Failed to get PVs from kubernetes. Err: %v", err)
		return
	}
	// Converting k8sPVs slice to Map for clean and quicker look up.
	k8sPVMap := make(map[string]string)
	for _, pv := range k8sPVs {
		k8sPVMap[pv.Spec.CSI.VolumeHandle] = ""
	}

	// pvToPVCMap maps pv name to corresponding PVC
	// pvcToPodMap maps pvc to the mounted Pod
	pvToPVCMap, pvcToPodMap := buildPVCMapPodMap(k8sclient, k8sPVs)
	klog.V(4).Infof("FullSync: pvToPVCMap %v", pvToPVCMap)
	klog.V(4).Infof("FullSync: pvcToPodMap %v", pvcToPodMap)

	//Call CNS QueryAll to get container volumes by cluster ID
	queryFilter := cnstypes.CnsQueryFilter{
		ContainerClusterIds: []string{
			metadataSyncer.configInfo.Cfg.Global.ClusterID,
		},
	}
	querySelection := cnstypes.CnsQuerySelection{}
	queryAllResult, err := metadataSyncer.volumeManager.QueryAllVolume(queryFilter, querySelection)
	if err != nil {
		klog.Warningf("FullSync: failed to queryAllVolume with err %+v", err)
		return
	}
	// get map of "pv to EntityMetadata" in CNS and "pv to EntityMetadata" in kubernetes
	pvToCnsEntityMetadataMap, pvToK8sEntityMetadataMap, err := getEntityMetadata(k8sPVs, queryAllResult.Volumes, pvToPVCMap, pvcToPodMap, metadataSyncer)
	if err != nil {
		klog.Warningf("FullSync: getEntityMetadata failed with err %+v", err)
		return
	}
	klog.V(5).Infof("FullSync: pvToCnsEntityMetadataMap %+v \n pvToK8sEntityMetadataMap: %+v \n", spew.Sdump(pvToCnsEntityMetadataMap), spew.Sdump(pvToK8sEntityMetadataMap))
	// Get specs for create and update volume calls
	containerCluster := cnsvsphere.GetContainerCluster(metadataSyncer.configInfo.Cfg.Global.ClusterID, metadataSyncer.configInfo.Cfg.VirtualCenter[metadataSyncer.host].User, metadataSyncer.clusterFlavor)
	createSpecArray, updateSpecArray := getVolumeSpecs(k8sPVs, pvToCnsEntityMetadataMap, pvToK8sEntityMetadataMap, containerCluster)
	volToBeDeleted := getVolumesToBeDeleted(queryAllResult.Volumes, k8sPVMap)

	wg := sync.WaitGroup{}
	wg.Add(3)
	// Perform operations
	go fullSyncCreateVolumes(createSpecArray, metadataSyncer, k8sclient, &wg)
	go fullSyncUpdateVolumes(updateSpecArray, metadataSyncer, &wg)
	go fullSyncDeleteVolumes(volToBeDeleted, metadataSyncer, k8sclient, &wg)
	wg.Wait()

	cleanupCnsMaps(k8sPVMap)
	klog.V(4).Infof("FullSync: cnsDeletionMap at end of cycle: %v", cnsDeletionMap)
	klog.V(4).Infof("FullSync: cnsCreationMap at end of cycle: %v", cnsCreationMap)
	klog.V(2).Infof("FullSync: end")
}

// fullSyncCreateVolumes create volumes with given array of createSpec
// Before creating a volume, all current K8s volumes are retrieved
// If the volume is successfully created, it is removed from cnsCreationMap
func fullSyncCreateVolumes(createSpecArray []cnstypes.CnsVolumeCreateSpec, metadataSyncer *metadataSyncInformer, k8sclient clientset.Interface, wg *sync.WaitGroup) {
	defer wg.Done()
	currentK8sPVMap := make(map[string]bool)
	volumeOperationsLock.Lock()
	defer volumeOperationsLock.Unlock()
	// Get all K8s PVs
	currentK8sPV, err := getPVsInBoundAvailableOrReleased(k8sclient)
	if err != nil {
		klog.Errorf("FullSync: fullSyncCreateVolumes failed to get PVs from kubernetes. Err: %v", err)
		return
	}
	// Create map for easy lookup
	for _, pv := range currentK8sPV {
		currentK8sPVMap[pv.Spec.CSI.VolumeHandle] = true
	}
	for _, createSpec := range createSpecArray {
		// Create volume if present in currentK8sPVMap
		var volumeId string
		if createSpec.VolumeType == common.BlockVolumeType && createSpec.BackingObjectDetails != nil && createSpec.BackingObjectDetails.(*cnstypes.CnsBlockBackingDetails) != nil {
			volumeId = createSpec.BackingObjectDetails.(*cnstypes.CnsBlockBackingDetails).BackingDiskId
		} else if createSpec.VolumeType == common.FileVolumeType && createSpec.BackingObjectDetails != nil && createSpec.BackingObjectDetails.(*cnstypes.CnsVsanFileShareBackingDetails) != nil {
			volumeId = createSpec.BackingObjectDetails.(*cnstypes.CnsVsanFileShareBackingDetails).BackingFileId
		} else {
			klog.Warningf("Skipping createSpec: %+v as VolumeType is not known or BackingObjectDetails is either nil or not typecastable  ", spew.Sdump(createSpec))
			continue
		}
		if _, existsInK8s := currentK8sPVMap[volumeId]; existsInK8s {
			klog.V(4).Infof("FullSync: Calling CreateVolume for volume id: %q with createSpec %+v", volumeId, spew.Sdump(createSpec))
			_, err := metadataSyncer.volumeManager.CreateVolume(&createSpec)
			if err != nil {
				klog.Warningf("FullSync: Failed to create volume with the spec: %+v. Err: %+v", spew.Sdump(createSpec), err)
				continue
			}
		}
		delete(cnsCreationMap, volumeId)
	}

}

// fullSyncDeleteVolumes delete volumes with given array of volumeId
// Before deleting a volume, all current K8s volumes are retrieved
// If the volume is successfully deleted, it is removed from cnsDeletionMap
func fullSyncDeleteVolumes(volumeIDDeleteArray []cnstypes.CnsVolumeId, metadataSyncer *metadataSyncInformer, k8sclient clientset.Interface, wg *sync.WaitGroup) {
	defer wg.Done()
	deleteDisk := false
	currentK8sPVMap := make(map[string]bool)
	volumeOperationsLock.Lock()
	defer volumeOperationsLock.Unlock()
	// Get all K8s PVs
	currentK8sPV, err := getPVsInBoundAvailableOrReleased(k8sclient)
	if err != nil {
		klog.Errorf("FullSync: fullSyncDeleteVolumes failed to get PVs from kubernetes. Err: %v", err)
		return
	}
	// Create map for easy lookup
	for _, pv := range currentK8sPV {
		currentK8sPVMap[pv.Spec.CSI.VolumeHandle] = true
	}
	for _, volID := range volumeIDDeleteArray {
		// Delete volume if not present in currentK8sPVMap
		if _, existsInK8s := currentK8sPVMap[volID.Id]; !existsInK8s {
			queryFilter := cnstypes.CnsQueryFilter{
				VolumeIds: []cnstypes.CnsVolumeId{
					{
						Id: volID.Id,
					},
				},
			}
			// Verify if Volume is not in use by any other Cluster before removing CNS tag
			queryResult, err := metadataSyncer.volumeManager.QueryVolume(queryFilter)
			if err != nil {
				klog.Errorf("FullSync: fullSyncDeleteVolumes: Failed to QueryVolume using filter: %+v, err: %+v", queryFilter, err)
				continue
			}
			inUsebyOtherK8SCluster := false
			if queryResult != nil && len(queryResult.Volumes) == 1 {
				for _, metadata := range queryResult.Volumes[0].Metadata.EntityMetadata {
					if metadata.(*cnstypes.CnsKubernetesEntityMetadata).ClusterID != metadataSyncer.configInfo.Cfg.Global.ClusterID {
						inUsebyOtherK8SCluster = true
						klog.V(4).Infof("FullSync: fullSyncDeleteVolumes: Volume: %q is in use by other cluster.", volID.Id)
						break
					}
				}
			}
			if !inUsebyOtherK8SCluster {
				klog.V(4).Infof("FullSync: fullSyncDeleteVolumes: Calling DeleteVolume for volume %v with delete disk %v", volID, deleteDisk)
				err := metadataSyncer.volumeManager.DeleteVolume(volID.Id, deleteDisk)
				if err != nil {
					klog.Warningf("FullSync: fullSyncDeleteVolumes: Failed to delete volume %s with error %+v", volID, err)
					continue
				}
			}
		}
		delete(cnsDeletionMap, volID.Id)
	}
}

// fullSyncUpdateVolumes update metadata for volumes with given array of createSpec
func fullSyncUpdateVolumes(updateSpecArray []cnstypes.CnsVolumeMetadataUpdateSpec, metadataSyncer *metadataSyncInformer, wg *sync.WaitGroup) {
	defer wg.Done()
	for _, updateSpec := range updateSpecArray {
		klog.V(4).Infof("FullSync: Calling UpdateVolumeMetadata for volume %s with updateSpec: %+v", updateSpec.VolumeId.Id, spew.Sdump(updateSpec))
		if err := metadataSyncer.volumeManager.UpdateVolumeMetadata(&updateSpec); err != nil {
			klog.Warningf("FullSync:UpdateVolumeMetadata failed with err %v", err)
		}
	}
}

// buildCnsMetadataList build metadata list for given PV
// metadata list may include PV metadata, PVC metadata and POD metadata
func buildCnsMetadataList(pv *v1.PersistentVolume, pvToPVCMap pvcMap, pvcToPodMap podMap, clusterID string) []cnstypes.BaseCnsEntityMetadata {
	var metadataList []cnstypes.BaseCnsEntityMetadata

	// get pv metadata
	pvMetadata := cnsvsphere.GetCnsKubernetesEntityMetaData(pv.Name, pv.GetLabels(), false, string(cnstypes.CnsKubernetesEntityTypePV), "", clusterID, nil)
	metadataList = append(metadataList, pvMetadata)
	if pvc, ok := pvToPVCMap[pv.Name]; ok {
		// get pvc metadata
		pvEntityReference := cnsvsphere.CreateCnsKuberenetesEntityReference(string(cnstypes.CnsKubernetesEntityTypePV), pv.Name, "", clusterID)
		pvcMetadata := cnsvsphere.GetCnsKubernetesEntityMetaData(pvc.Name, pvc.GetLabels(), false, string(cnstypes.CnsKubernetesEntityTypePVC), pvc.Namespace, clusterID, []cnstypes.CnsKubernetesEntityReference{pvEntityReference})
		metadataList = append(metadataList, cnstypes.BaseCnsEntityMetadata(pvcMetadata))

		key := pvc.Namespace + "/" + pvc.Name
		if pods, ok := pvcToPodMap[key]; ok {
			for _, pod := range pods {
				// get pod metadata
				pvcEntityReference := cnsvsphere.CreateCnsKuberenetesEntityReference(string(cnstypes.CnsKubernetesEntityTypePVC), pvc.Name, pvc.Namespace, clusterID)
				podMetadata := cnsvsphere.GetCnsKubernetesEntityMetaData(pod.Name, nil, false, string(cnstypes.CnsKubernetesEntityTypePOD), pod.Namespace, clusterID, []cnstypes.CnsKubernetesEntityReference{pvcEntityReference})
				metadataList = append(metadataList, cnstypes.BaseCnsEntityMetadata(podMetadata))
			}
		}
	}
	klog.V(4).Infof("FullSync: buildMetadataList=%+v \n", spew.Sdump(metadataList))
	return metadataList
}

// getEntityMetadata builds and return map of pv to EntityMetadata in CNS (pvToCnsEntityMetadataMap) and
// map of pv to EntityMetadata in kubernetes (pvToK8sEntityMetadataMap)
func getEntityMetadata(pvList []*v1.PersistentVolume, cnsVolumeList []cnstypes.CnsVolume, pvToPVCMap pvcMap, pvcToPodMap podMap, metadataSyncer *metadataSyncInformer) (map[string][]cnstypes.BaseCnsEntityMetadata, map[string][]cnstypes.BaseCnsEntityMetadata, error) {
	pvToCnsEntityMetadataMap := make(map[string][]cnstypes.BaseCnsEntityMetadata)
	pvToK8sEntityMetadataMap := make(map[string][]cnstypes.BaseCnsEntityMetadata)
	cnsVolumeMap := make(map[string]bool)

	for _, vol := range cnsVolumeList {
		cnsVolumeMap[vol.VolumeId.Id] = true
	}
	for _, pv := range pvList {
		k8sMetadata := buildCnsMetadataList(pv, pvToPVCMap, pvcToPodMap, metadataSyncer.configInfo.Cfg.Global.ClusterID)
		pvToK8sEntityMetadataMap[pv.Spec.CSI.VolumeHandle] = k8sMetadata
		if cnsVolumeMap[pv.Spec.CSI.VolumeHandle] {
			// PV exist in both K8S and CNS cache, check metadata has been changed or not
			queryFilter := cnstypes.CnsQueryFilter{
				VolumeIds: []cnstypes.CnsVolumeId{
					{
						Id: pv.Spec.CSI.VolumeHandle,
					},
				},
				ContainerClusterIds: []string{
					metadataSyncer.configInfo.Cfg.Global.ClusterID,
				},
			}
			queryResult, err := metadataSyncer.volumeManager.QueryVolume(queryFilter)
			if err != nil {
				klog.Errorf("FullSync: Failed to QueryVolume using filter: %+v", queryFilter)
				return nil, nil, err
			}
			var cnsMetadata []cnstypes.BaseCnsEntityMetadata
			if err == nil && queryResult != nil && len(queryResult.Volumes) > 0 {
				if &queryResult.Volumes[0].Metadata != nil {
					allEntityMetadata := queryResult.Volumes[0].Metadata.EntityMetadata
					for _, metadata := range allEntityMetadata {
						if metadata.(*cnstypes.CnsKubernetesEntityMetadata).ClusterID == metadataSyncer.configInfo.Cfg.Global.ClusterID {
							cnsMetadata = append(cnsMetadata, metadata)
						}
					}
					pvToCnsEntityMetadataMap[pv.Spec.CSI.VolumeHandle] = cnsMetadata
				}
			}
		}
	}
	return pvToCnsEntityMetadataMap, pvToK8sEntityMetadataMap, nil
}

// getVolumeSpecs return list of CnsVolumeCreateSpec for volumes which needs to be created in CNS and a list of
// CnsVolumeMetadataUpdateSpec for volumes which needs to be updated in CNS.
func getVolumeSpecs(pvList []*v1.PersistentVolume, pvToCnsEntityMetadataMap map[string][]cnstypes.BaseCnsEntityMetadata, pvToK8sEntityMetadataMap map[string][]cnstypes.BaseCnsEntityMetadata, containerCluster cnstypes.CnsContainerCluster) ([]cnstypes.CnsVolumeCreateSpec, []cnstypes.CnsVolumeMetadataUpdateSpec) {
	var createSpecArray []cnstypes.CnsVolumeCreateSpec
	var updateSpecArray []cnstypes.CnsVolumeMetadataUpdateSpec

	var operationType string
	for _, pv := range pvList {
		pvToCnsEntityMetadata, presentInCNS := pvToCnsEntityMetadataMap[pv.Spec.CSI.VolumeHandle]
		pvToK8sEntityMetadata, presentInK8S := pvToK8sEntityMetadataMap[pv.Spec.CSI.VolumeHandle]

		if !presentInK8S {
			klog.V(2).Infof("FullSync: getVolumeSpecs. skipping volume: %q as volume is not present in the k8s", pv.Spec.CSI.VolumeHandle)
			continue
		}
		if !presentInCNS {
			// PV exist in K8S but not in CNS cache, need to create
			if _, existsInCnsCreationMap := cnsCreationMap[pv.Spec.CSI.VolumeHandle]; existsInCnsCreationMap {
				klog.V(2).Infof("FullSync: create is required for volume: %q, as volume was present in cnsCreationMap across two full-sync cycles", pv.Spec.CSI.VolumeHandle)
				operationType = "createVolume"
			} else {
				klog.V(2).Infof("FullSync: Volume with id: %q and name: %q is added to cnsCreationMap", pv.Spec.CSI.VolumeHandle, pv.Name)
				cnsCreationMap[pv.Spec.CSI.VolumeHandle] = true
			}
		} else {
			// volume exist in K8S and CNS, Check if update is required.
			if isUpdateRequired(pvToK8sEntityMetadata, pvToCnsEntityMetadata) {
				klog.V(2).Infof("FullSync: update is required for volume: %q", pv.Spec.CSI.VolumeHandle)
				operationType = "updateVolume"
			} else {
				klog.V(2).Infof("FullSync: update is not required for volume: %q", pv.Spec.CSI.VolumeHandle)
			}
		}
		switch operationType {
		case "createVolume":
			var volumeType string
			if pv.Spec.CSI.FSType == common.NfsV4FsType || pv.Spec.CSI.FSType == common.NfsFsType {
				volumeType = common.FileVolumeType
			} else {
				volumeType = common.BlockVolumeType
			}
			createSpec := cnstypes.CnsVolumeCreateSpec{
				Name:       pv.Name,
				VolumeType: volumeType,
				Metadata: cnstypes.CnsVolumeMetadata{
					ContainerCluster:      containerCluster,
					ContainerClusterArray: []cnstypes.CnsContainerCluster{containerCluster},
					EntityMetadata:        pvToK8sEntityMetadataMap[pv.Spec.CSI.VolumeHandle],
				},
			}
			if volumeType == common.BlockVolumeType {
				createSpec.BackingObjectDetails = &cnstypes.CnsBlockBackingDetails{
					CnsBackingObjectDetails: cnstypes.CnsBackingObjectDetails{},
					BackingDiskId:           pv.Spec.CSI.VolumeHandle,
				}
			} else {
				createSpec.BackingObjectDetails = &cnstypes.CnsVsanFileShareBackingDetails{
					CnsFileBackingDetails: cnstypes.CnsFileBackingDetails{
						BackingFileId: pv.Spec.CSI.VolumeHandle,
					},
				}
			}
			createSpecArray = append(createSpecArray, createSpec)
		case "updateVolume":
			// volume exist in K8S and CNS cache, but metadata is different, need to update this volume
			klog.V(4).Infof("FullSync: Volume with id %q added to volume update list", pv.Spec.CSI.VolumeHandle)
			updateSpec := cnstypes.CnsVolumeMetadataUpdateSpec{
				VolumeId: cnstypes.CnsVolumeId{
					Id: pv.Spec.CSI.VolumeHandle,
				},
				Metadata: cnstypes.CnsVolumeMetadata{
					ContainerCluster:      containerCluster,
					ContainerClusterArray: []cnstypes.CnsContainerCluster{containerCluster},
					// Update metadata in CNS with the new metadata present in K8S
					EntityMetadata: pvToK8sEntityMetadataMap[pv.Spec.CSI.VolumeHandle],
				},
			}
			// Delete metadata in CNS which is not present in K8S
			for _, oldMetadata := range pvToCnsEntityMetadataMap[pv.Spec.CSI.VolumeHandle] {
				oldEntityName := oldMetadata.(*cnstypes.CnsKubernetesEntityMetadata).EntityName
				oldEntityNameSpace := oldMetadata.(*cnstypes.CnsKubernetesEntityMetadata).Namespace
				oldEntityType := oldMetadata.(*cnstypes.CnsKubernetesEntityMetadata).EntityType
				found := false
				for _, newMetadata := range updateSpec.Metadata.EntityMetadata {
					newEntityName := newMetadata.(*cnstypes.CnsKubernetesEntityMetadata).EntityName
					newEntityNameSpace := newMetadata.(*cnstypes.CnsKubernetesEntityMetadata).Namespace
					newEntityType := newMetadata.(*cnstypes.CnsKubernetesEntityMetadata).EntityType
					if oldEntityName == newEntityName && oldEntityNameSpace == newEntityNameSpace && oldEntityType == newEntityType {
						found = true
						break
					}
				}
				if !found {
					oldMetadata.(*cnstypes.CnsKubernetesEntityMetadata).Delete = true
					updateSpec.Metadata.EntityMetadata = append(updateSpec.Metadata.EntityMetadata, oldMetadata)
				}
			}
			updateSpecArray = append(updateSpecArray, updateSpec)
		}
	}
	return createSpecArray, updateSpecArray
}

// getVolumesToBeDeleted return list of volumeIds that need to be deleted
// A volumeId is added to this list only if it was present in cnsDeletionMap across two cycles of full sync
func getVolumesToBeDeleted(cnsVolumeList []cnstypes.CnsVolume, k8sPVMap map[string]string) []cnstypes.CnsVolumeId {
	var volToBeDeleted []cnstypes.CnsVolumeId
	for _, vol := range cnsVolumeList {
		if _, existsInK8s := k8sPVMap[vol.VolumeId.Id]; !existsInK8s {
			if _, existsInCnsDeletionMap := cnsDeletionMap[vol.VolumeId.Id]; existsInCnsDeletionMap {
				// Volume does not exist in K8s across two fullsync cycles - add to delete list
				klog.V(4).Infof("FullSync: Volume with id %s added to delete list as it was present in cnsDeletionMap across two fullsync cycles", vol.VolumeId.Id)
				volToBeDeleted = append(volToBeDeleted, vol.VolumeId)
			} else {
				// Add to cnsDeletionMap
				klog.V(4).Infof("FullSync: Volume with id %s added to cnsDeletionMap", vol.VolumeId.Id)
				cnsDeletionMap[vol.VolumeId.Id] = true
			}
		}
	}
	return volToBeDeleted
}

// buildPVCMapPodMap build two maps to help
//  1. find PVC for given PV
//  2. find POD mounted to given PVC
// pvToPVCMap maps PV name to corresponding PVC, key is pv name
// pvcToPodMap maps PVC to the array of PODs using the PVC, key is "pod.Namespace/pvc.Name"
func buildPVCMapPodMap(k8sclient clientset.Interface, pvList []*v1.PersistentVolume) (pvcMap, podMap) {
	pvToPVCMap := make(pvcMap)
	pvcToPodMap := make(podMap)
	for _, pv := range pvList {
		if pv.Spec.ClaimRef != nil && pv.Status.Phase == v1.VolumeBound {
			pvc, err := k8sclient.CoreV1().PersistentVolumeClaims(pv.Spec.ClaimRef.Namespace).Get(pv.Spec.ClaimRef.Name, metav1.GetOptions{})
			if err != nil {
				klog.Warningf("FullSync: Failed to get pvc for namespace %v and name %v. err=%v", pv.Spec.ClaimRef.Namespace, pv.Spec.ClaimRef.Name, err)
				continue
			}
			pvToPVCMap[pv.Name] = pvc
			klog.V(4).Infof("FullSync: pvc %v is backed by pv %v", pvc.Name, pv.Name)
			pods, err := k8sclient.CoreV1().Pods(pvc.Namespace).List(metav1.ListOptions{
				FieldSelector: fields.AndSelectors(fields.SelectorFromSet(fields.Set{"status.phase": string(api.PodRunning)})).String(),
			})
			if err != nil {
				klog.Warningf("FullSync: Failed to get pods for namespace %v. err=%v", pvc.Namespace, err)
				continue
			}
			for index, pod := range pods.Items {
				if pod.Spec.Volumes != nil {
					for _, volume := range pod.Spec.Volumes {
						pvClaim := volume.VolumeSource.PersistentVolumeClaim
						if pvClaim != nil && pvClaim.ClaimName == pvc.Name {
							key := pod.Namespace + "/" + pvClaim.ClaimName
							pvcToPodMap[key] = append(pvcToPodMap[key], &pods.Items[index])
							klog.V(4).Infof("FullSync: pvc %v is mounted by pod %v", key, pod.Name)
							break
						}
					}
				}
			}

		}
	}
	return pvToPVCMap, pvcToPodMap
}

// isUpdateRequired compares the input metadata list from K8S and metadata list from CNS and
// returns true if update operation is required else returns false.
func isUpdateRequired(k8sMetadataList []cnstypes.BaseCnsEntityMetadata, cnsMetadataList []cnstypes.BaseCnsEntityMetadata) bool {
	klog.V(4).Infof("FullSync: isUpdateRequired called with k8sMetadataList: %+v \n", spew.Sdump(k8sMetadataList))
	klog.V(4).Infof("FullSync: isUpdateRequired called with cnsMetadataList: %+v \n", spew.Sdump(cnsMetadataList))
	if len(k8sMetadataList) == len(cnsMetadataList) {
		// Same number of entries for volume in K8s and CNS
		// Need to check if entries match
		cnsEntityTypeMetadataMap := make(map[string]*cnstypes.CnsKubernetesEntityMetadata)
		for _, cnsMetadata := range cnsMetadataList {
			metadata := cnsMetadata.(*cnstypes.CnsKubernetesEntityMetadata)
			// Here key is required to retrieve specific entity metadata from cnsEntityTypeMetadataMap,
			// while traversing through k8sMetadataList, to compare metadata in k8s and CNS.
			key := metadata.EntityType + ":" + metadata.EntityName + ":" + metadata.Namespace
			cnsEntityTypeMetadataMap[key] = metadata
		}
		klog.V(4).Infof("cnsEntityTypeMetadataMap :%+v", spew.Sdump(cnsEntityTypeMetadataMap))
		for _, k8sMetadata := range k8sMetadataList {
			metadata := k8sMetadata.(*cnstypes.CnsKubernetesEntityMetadata)
			key := metadata.EntityType + ":" + metadata.EntityName + ":" + metadata.Namespace
			cnsMetadata, ok := cnsEntityTypeMetadataMap[key]
			if !ok {
				klog.V(4).Infof("key: %q is not found in the cnsEntityTypeMetadataMap", key)
				return true
			}
			if cnsvsphere.CompareKubernetesMetadata(metadata, cnsMetadata) == false {
				return true
			}
		}
	} else {
		// K8s metadata entries and CNS metadata entries does not match need to update
		return true
	}
	return false
}

// cleanupCnsMaps performs cleanup on cnsCreationMap and cnsDeletionMap
// Removes volume entries from cnsCreationMap that do not exist in K8s
// and volume entries from cnsDeletionMap that exist in K8s
// An entry could have been added to cnsCreationMap (or cnsDeletionMap)
// because full sync was triggered in between the delete (or create)
// operation of a volume
func cleanupCnsMaps(k8sPVs map[string]string) {
	// Cleanup cnsCreationMap
	for volID := range cnsCreationMap {
		if _, existsInK8s := k8sPVs[volID]; !existsInK8s {
			delete(cnsCreationMap, volID)
		}
	}
	// Cleanup cnsDeletionMap
	for volID := range cnsDeletionMap {
		if _, existsInK8s := k8sPVs[volID]; existsInK8s {
			delete(cnsDeletionMap, volID)
		}
	}
}
