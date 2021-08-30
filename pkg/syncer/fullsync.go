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
	"sync"

	"github.com/davecgh/go-spew/spew"
	"github.com/vmware/govmomi/cns"
	cnstypes "github.com/vmware/govmomi/cns/types"
	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/labels"

	"sigs.k8s.io/vsphere-csi-driver/v2/pkg/apis/migration"
	cnsvsphere "sigs.k8s.io/vsphere-csi-driver/v2/pkg/common/cns-lib/vsphere"
	"sigs.k8s.io/vsphere-csi-driver/v2/pkg/common/utils"
	"sigs.k8s.io/vsphere-csi-driver/v2/pkg/csi/service/common"
	"sigs.k8s.io/vsphere-csi-driver/v2/pkg/csi/service/logger"
)

// CsiFullSync reconciles volume metadata on a vanilla k8s cluster with volume
// metadata on CNS.
func CsiFullSync(ctx context.Context, metadataSyncer *metadataSyncInformer) error {
	log := logger.GetLogger(ctx)
	log.Infof("FullSync: start")

	var migrationFeatureStateForFullSync bool
	// Fetch CSI migration feature state, before performing full sync operations.
	if metadataSyncer.clusterFlavor == cnstypes.CnsClusterFlavorVanilla {
		migrationFeatureStateForFullSync = metadataSyncer.coCommonInterface.IsFSSEnabled(ctx, common.CSIMigration)
	}
	// Get K8s PVs in State "Bound", "Available" or "Released".
	k8sPVs, err := getPVsInBoundAvailableOrReleased(ctx, metadataSyncer)
	if err != nil {
		log.Errorf("FullSync: Failed to get PVs from kubernetes. Err: %v", err)
		return err
	}

	// k8sPVMap is useful for clean and quicker look up.
	k8sPVMap := make(map[string]string)
	// Instantiate volumeMigrationService when migration feature state is True.
	if migrationFeatureStateForFullSync {
		// In case if feature state switch is enabled after syncer is deployed,
		// we need to initialize the volumeMigrationService.
		if err := initVolumeMigrationService(ctx, metadataSyncer); err != nil {
			log.Errorf("FullSync: Failed to get migration service. Err: %v", err)
			return err
		}
	}

	// Iterate through all the k8sPVs and use volume id as the key for k8sPVMap
	// items. For migrated volumes, invoke GetVolumeID from migration service.
	for _, pv := range k8sPVs {
		// k8sPVs contains valid CSI volumes or migrated vSphere volumes
		if pv.Spec.CSI != nil {
			k8sPVMap[pv.Spec.CSI.VolumeHandle] = ""
		} else if migrationFeatureStateForFullSync && pv.Spec.VsphereVolume != nil {
			// For vSphere volumes, migration service will register volumes in CNS.
			migrationVolumeSpec := &migration.VolumeSpec{
				VolumePath:        pv.Spec.VsphereVolume.VolumePath,
				StoragePolicyName: pv.Spec.VsphereVolume.StoragePolicyName}
			volumeHandle, err := volumeMigrationService.GetVolumeID(ctx, migrationVolumeSpec)
			if err != nil {
				log.Errorf("FullSync: Failed to get VolumeID from volumeMigrationService for spec: %v. Err: %+v",
					migrationVolumeSpec, err)
				return err
			}
			k8sPVMap[volumeHandle] = ""
		}
	}
	// pvToPVCMap maps pv name to corresponding PVC.
	// pvcToPodMap maps pvc to the mounted Pod.
	pvToPVCMap, pvcToPodMap, err := buildPVCMapPodMap(ctx, k8sPVs, metadataSyncer)
	if err != nil {
		log.Errorf("FullSync: Failed to build PVCMap and PodMap. Err: %v", err)
		return err
	}
	log.Debugf("FullSync: pvToPVCMap %v", pvToPVCMap)
	log.Debugf("FullSync: pvcToPodMap %v", pvcToPodMap)

	// Call CNS QueryAll to get container volumes by cluster ID.
	queryFilter := cnstypes.CnsQueryFilter{
		ContainerClusterIds: []string{
			metadataSyncer.configInfo.Cfg.Global.ClusterID,
		},
	}
	queryResult, err := utils.QueryAllVolumeUtil(ctx, metadataSyncer.volumeManager, queryFilter,
		cnstypes.CnsQuerySelection{}, metadataSyncer.coCommonInterface.IsFSSEnabled(ctx, common.AsyncQueryVolume))
	if err != nil {
		log.Errorf("PVCUpdated: QueryVolume failed with err=%+v", err.Error())
		return err
	}

	volumeToCnsEntityMetadataMap, volumeToK8sEntityMetadataMap, volumeClusterDistributionMap, err :=
		fullSyncConstructVolumeMaps(ctx, k8sPVs, queryResult.Volumes, pvToPVCMap,
			pvcToPodMap, metadataSyncer, migrationFeatureStateForFullSync)
	if err != nil {
		log.Errorf("FullSync: fullSyncGetEntityMetadata failed with err %+v", err)
		return err
	}
	log.Debugf("FullSync: pvToCnsEntityMetadataMap %+v \n pvToK8sEntityMetadataMap: %+v \n",
		spew.Sdump(volumeToCnsEntityMetadataMap), spew.Sdump(volumeToK8sEntityMetadataMap))
	log.Debugf("FullSync: volumes where clusterDistribution is set: %+v", volumeClusterDistributionMap)

	vcenter, err := cnsvsphere.GetVirtualCenterInstance(ctx, metadataSyncer.configInfo, false)
	if err != nil {
		log.Errorf("FullSync: failed to get vcenter with error %+v", err)
		return err
	}
	// Get specs for create and update volume calls.
	containerCluster := cnsvsphere.GetContainerCluster(metadataSyncer.configInfo.Cfg.Global.ClusterID,
		metadataSyncer.configInfo.Cfg.VirtualCenter[metadataSyncer.host].User, metadataSyncer.clusterFlavor,
		metadataSyncer.configInfo.Cfg.Global.ClusterDistribution)
	createSpecArray, updateSpecArray := fullSyncGetVolumeSpecs(ctx, vcenter.Client.Version, k8sPVs,
		volumeToCnsEntityMetadataMap, volumeToK8sEntityMetadataMap, volumeClusterDistributionMap,
		containerCluster, metadataSyncer, migrationFeatureStateForFullSync)
	volToBeDeleted, err := getVolumesToBeDeleted(ctx, queryResult.Volumes, k8sPVMap, metadataSyncer,
		migrationFeatureStateForFullSync)
	if err != nil {
		log.Errorf("FullSync: failed to get list of volumes to be deleted with err %+v", err)
		return err
	}

	wg := sync.WaitGroup{}
	wg.Add(3)
	// Perform operations.
	go fullSyncCreateVolumes(ctx, createSpecArray, metadataSyncer, &wg, migrationFeatureStateForFullSync)
	go fullSyncUpdateVolumes(ctx, updateSpecArray, metadataSyncer, &wg)
	go fullSyncDeleteVolumes(ctx, volToBeDeleted, metadataSyncer, &wg, migrationFeatureStateForFullSync)
	wg.Wait()

	cleanupCnsMaps(k8sPVMap)
	log.Debugf("FullSync: cnsDeletionMap at end of cycle: %v", cnsDeletionMap)
	log.Debugf("FullSync: cnsCreationMap at end of cycle: %v", cnsCreationMap)
	log.Infof("FullSync: end")
	return nil
}

// fullSyncCreateVolumes creates volumes with given array of createSpec.
// Before creating a volume, all current K8s volumes are retrieved.
// If the volume is successfully created, it is removed from cnsCreationMap.
func fullSyncCreateVolumes(ctx context.Context, createSpecArray []cnstypes.CnsVolumeCreateSpec,
	metadataSyncer *metadataSyncInformer, wg *sync.WaitGroup, migrationFeatureStateForFullSync bool) {
	log := logger.GetLogger(ctx)
	defer wg.Done()
	currentK8sPVMap := make(map[string]bool)
	volumeOperationsLock.Lock()
	defer volumeOperationsLock.Unlock()
	// Get all K8s PVs.
	currentK8sPV, err := getPVsInBoundAvailableOrReleased(ctx, metadataSyncer)
	if err != nil {
		log.Errorf("FullSync: fullSyncCreateVolumes failed to get PVs from kubernetes. Err: %v", err)
		return
	}

	// Create map for easy lookup.
	for _, pv := range currentK8sPV {
		if pv.Spec.CSI != nil {
			currentK8sPVMap[pv.Spec.CSI.VolumeHandle] = true
		} else if migrationFeatureStateForFullSync && pv.Spec.VsphereVolume != nil {
			migrationVolumeSpec := &migration.VolumeSpec{
				VolumePath:        pv.Spec.VsphereVolume.VolumePath,
				StoragePolicyName: pv.Spec.VsphereVolume.StoragePolicyName}
			volumeHandle, err := volumeMigrationService.GetVolumeID(ctx, migrationVolumeSpec)
			if err != nil {
				log.Errorf("FullSync: Failed to get VolumeID from volumeMigrationService for spec: %v. Err: %+v",
					migrationVolumeSpec, err)
				return
			}
			currentK8sPVMap[volumeHandle] = true
		}
	}
	for _, createSpec := range createSpecArray {
		// Create volume if present in currentK8sPVMap.
		var volumeID string
		if createSpec.VolumeType == common.BlockVolumeType && createSpec.BackingObjectDetails != nil &&
			createSpec.BackingObjectDetails.(*cnstypes.CnsBlockBackingDetails) != nil {
			volumeID = createSpec.BackingObjectDetails.(*cnstypes.CnsBlockBackingDetails).BackingDiskId
		} else if createSpec.VolumeType == common.FileVolumeType && createSpec.BackingObjectDetails != nil &&
			createSpec.BackingObjectDetails.(*cnstypes.CnsVsanFileShareBackingDetails) != nil {
			volumeID = createSpec.BackingObjectDetails.(*cnstypes.CnsVsanFileShareBackingDetails).BackingFileId
		} else {
			log.Warnf("Skipping createSpec: %+v as VolumeType is unknown or BackingObjectDetails is not valid",
				spew.Sdump(createSpec))
			continue
		}
		if _, existsInK8s := currentK8sPVMap[volumeID]; existsInK8s {
			log.Debugf("FullSync: Calling CreateVolume for volume id: %q with createSpec %+v",
				volumeID, spew.Sdump(createSpec))
			_, err := metadataSyncer.volumeManager.CreateVolume(ctx, &createSpec)
			if err != nil {
				log.Warnf("FullSync: Failed to create volume with the spec: %+v. Err: %+v", spew.Sdump(createSpec), err)
				continue
			}
		} else {
			log.Debugf("FullSync: volumeID %s does not exist in Kubernetes, no need to create volume in CNS", volumeID)
		}
		delete(cnsCreationMap, volumeID)
	}

}

// fullSyncDeleteVolumes deletes volumes with given array of volumeId.
// Before deleting a volume, all current K8s volumes are retrieved.
// If the volume is successfully deleted, it is removed from cnsDeletionMap.
func fullSyncDeleteVolumes(ctx context.Context, volumeIDDeleteArray []cnstypes.CnsVolumeId,
	metadataSyncer *metadataSyncInformer, wg *sync.WaitGroup, migrationFeatureStateForFullSync bool) {
	defer wg.Done()
	log := logger.GetLogger(ctx)
	deleteDisk := false
	currentK8sPVMap := make(map[string]bool)
	volumeOperationsLock.Lock()
	defer volumeOperationsLock.Unlock()
	// Get all K8s PVs.
	currentK8sPV, err := getPVsInBoundAvailableOrReleased(ctx, metadataSyncer)
	if err != nil {
		log.Errorf("FullSync: fullSyncDeleteVolumes failed to get PVs from kubernetes. Err: %v", err)
		return
	}

	// Create map for easy lookup.
	for _, pv := range currentK8sPV {
		if pv.Spec.CSI != nil {
			currentK8sPVMap[pv.Spec.CSI.VolumeHandle] = true
		} else if migrationFeatureStateForFullSync && pv.Spec.VsphereVolume != nil {
			migrationVolumeSpec := &migration.VolumeSpec{
				VolumePath:        pv.Spec.VsphereVolume.VolumePath,
				StoragePolicyName: pv.Spec.VsphereVolume.StoragePolicyName}
			volumeHandle, err := volumeMigrationService.GetVolumeID(ctx, migrationVolumeSpec)
			if err != nil {
				log.Errorf("FullSync: Failed to get VolumeID from volumeMigrationService for spec: %v. Err: %+v",
					migrationVolumeSpec, err)
				return
			}
			currentK8sPVMap[volumeHandle] = true
		}
	}
	var queryVolumeIds []cnstypes.CnsVolumeId
	for _, volID := range volumeIDDeleteArray {
		// Delete volume if not present in currentK8sPVMap.
		if _, existsInK8s := currentK8sPVMap[volID.Id]; !existsInK8s {
			queryVolumeIds = append(queryVolumeIds, cnstypes.CnsVolumeId{Id: volID.Id})
		}
	}
	// This check is needed to prevent querying all CNS volumes when
	// queryFilter.VolumeIds does not have any volumes. volumes in the
	// queryFilter.VolumeIds should be one which is not present in the k8s,
	// but needs to be verified that it is not in use by any other k8s cluster.
	if len(queryVolumeIds) == 0 {
		log.Info("FullSync: fullSyncDeleteVolumes could not find any volume " +
			"which is not present in k8s and needs to be checked for volume deletion.")
		return
	}
	allQueryResults, err := fullSyncGetQueryResults(ctx, queryVolumeIds, "",
		metadataSyncer.volumeManager, metadataSyncer)
	if err != nil {
		log.Errorf("FullSync: fullSyncGetQueryResults failed to query volume metadata from vc. Err: %v", err)
		return
	}
	// Verify if Volume is not in use by any other Cluster before removing CNS tag
	for _, queryResult := range allQueryResults {
		for _, volume := range queryResult.Volumes {
			inUsebyOtherK8SCluster := false
			for _, metadata := range volume.Metadata.EntityMetadata {
				if metadata.(*cnstypes.CnsKubernetesEntityMetadata).ClusterID !=
					metadataSyncer.configInfo.Cfg.Global.ClusterID {
					inUsebyOtherK8SCluster = true
					log.Debugf("FullSync: fullSyncDeleteVolumes: Volume: %q is in use by other cluster.", volume.VolumeId.Id)
					break
				}
			}
			if !inUsebyOtherK8SCluster {
				log.Infof("FullSync: fullSyncDeleteVolumes: Calling DeleteVolume for volume %v with delete disk %v",
					volume.VolumeId.Id, deleteDisk)
				err := metadataSyncer.volumeManager.DeleteVolume(ctx, volume.VolumeId.Id, deleteDisk)
				if err != nil {
					log.Warnf("FullSync: fullSyncDeleteVolumes: Failed to delete volume %s with error %+v",
						volume.VolumeId.Id, err)
					continue
				}
				if migrationFeatureStateForFullSync {
					err = volumeMigrationService.DeleteVolumeInfo(ctx, volume.VolumeId.Id)
					// For non-migrated volumes DeleteVolumeInfo will not return
					// error. So, the volume id will be deleted from cnsDeletionMap.
					if err != nil {
						log.Warnf("FullSync: fullSyncDeleteVolumes: Failed to delete volume mapping CR for %s. Err: %+v",
							volume.VolumeId.Id, err)
						continue
					}
				}
			}
			// Delete volume from cnsDeletionMap which is successfully deleted from
			// CNS.
			delete(cnsDeletionMap, volume.VolumeId.Id)
		}
	}
}

// fullSyncUpdateVolumes update metadata for volumes with given array of
// createSpec.
func fullSyncUpdateVolumes(ctx context.Context, updateSpecArray []cnstypes.CnsVolumeMetadataUpdateSpec,
	metadataSyncer *metadataSyncInformer, wg *sync.WaitGroup) {
	defer wg.Done()
	log := logger.GetLogger(ctx)
	for _, updateSpec := range updateSpecArray {
		log.Debugf("FullSync: Calling UpdateVolumeMetadata for volume %s with updateSpec: %+v",
			updateSpec.VolumeId.Id, spew.Sdump(updateSpec))
		if err := metadataSyncer.volumeManager.UpdateVolumeMetadata(ctx, &updateSpec); err != nil {
			log.Warnf("FullSync:UpdateVolumeMetadata failed with err %v", err)
		}
	}
}

// buildCnsMetadataList build metadata list for given PV.
// Metadata list may include PV metadata, PVC metadata and POD metadata.
func buildCnsMetadataList(ctx context.Context, pv *v1.PersistentVolume, pvToPVCMap pvcMap,
	pvcToPodMap podMap, clusterID string) []cnstypes.BaseCnsEntityMetadata {
	log := logger.GetLogger(ctx)
	var metadataList []cnstypes.BaseCnsEntityMetadata
	// Get pv metadata.
	pvMetadata := cnsvsphere.GetCnsKubernetesEntityMetaData(pv.Name, pv.GetLabels(),
		false, string(cnstypes.CnsKubernetesEntityTypePV), "", clusterID, nil)
	metadataList = append(metadataList, pvMetadata)
	if pvc, ok := pvToPVCMap[pv.Name]; ok {
		// Get pvc metadata.
		pvEntityReference := cnsvsphere.CreateCnsKuberenetesEntityReference(
			string(cnstypes.CnsKubernetesEntityTypePV), pv.Name, "", clusterID)
		pvcMetadata := cnsvsphere.GetCnsKubernetesEntityMetaData(pvc.Name, pvc.GetLabels(),
			false, string(cnstypes.CnsKubernetesEntityTypePVC), pvc.Namespace, clusterID,
			[]cnstypes.CnsKubernetesEntityReference{pvEntityReference})
		metadataList = append(metadataList, cnstypes.BaseCnsEntityMetadata(pvcMetadata))

		key := pvc.Namespace + "/" + pvc.Name
		if pods, ok := pvcToPodMap[key]; ok {
			for _, pod := range pods {
				// Get pod metadata.
				pvcEntityReference := cnsvsphere.CreateCnsKuberenetesEntityReference(
					string(cnstypes.CnsKubernetesEntityTypePVC), pvc.Name, pvc.Namespace, clusterID)
				podMetadata := cnsvsphere.GetCnsKubernetesEntityMetaData(pod.Name,
					nil, false, string(cnstypes.CnsKubernetesEntityTypePOD), pod.Namespace,
					clusterID, []cnstypes.CnsKubernetesEntityReference{pvcEntityReference})
				metadataList = append(metadataList, cnstypes.BaseCnsEntityMetadata(podMetadata))
			}
		}
	}
	log.Debugf("FullSync: buildMetadataList=%+v \n", spew.Sdump(metadataList))
	return metadataList
}

// fullSyncConstructVolumeMaps builds and returns map of volume to
// EntityMetadata in CNS (volumeToCnsEntityMetadataMap), map of volume to
// EntityMetadata in kubernetes (volumeToK8sEntityMetadataMap) and set of
// volumes where ClusterDistribution is populated (volumeClusterDistributionMap).
func fullSyncConstructVolumeMaps(ctx context.Context, pvList []*v1.PersistentVolume,
	cnsVolumeList []cnstypes.CnsVolume, pvToPVCMap pvcMap, pvcToPodMap podMap,
	metadataSyncer *metadataSyncInformer, migrationFeatureStateForFullSync bool) (
	map[string][]cnstypes.BaseCnsEntityMetadata, map[string][]cnstypes.BaseCnsEntityMetadata,
	map[string]bool, error) {
	log := logger.GetLogger(ctx)
	volumeToCnsEntityMetadataMap := make(map[string][]cnstypes.BaseCnsEntityMetadata)
	volumeToK8sEntityMetadataMap := make(map[string][]cnstypes.BaseCnsEntityMetadata)
	volumeClusterDistributionMap := make(map[string]bool)

	cnsVolumeMap := make(map[string]bool)

	for _, vol := range cnsVolumeList {
		cnsVolumeMap[vol.VolumeId.Id] = true
	}
	var err error
	var queryVolumeIds []cnstypes.CnsVolumeId
	for _, pv := range pvList {
		k8sMetadata := buildCnsMetadataList(ctx, pv, pvToPVCMap, pvcToPodMap,
			metadataSyncer.configInfo.Cfg.Global.ClusterID)
		var volumeHandle string
		if pv.Spec.CSI != nil {
			volumeHandle = pv.Spec.CSI.VolumeHandle
		} else if migrationFeatureStateForFullSync && pv.Spec.VsphereVolume != nil {
			migrationVolumeSpec := &migration.VolumeSpec{
				VolumePath:        pv.Spec.VsphereVolume.VolumePath,
				StoragePolicyName: pv.Spec.VsphereVolume.StoragePolicyName}
			volumeHandle, err = volumeMigrationService.GetVolumeID(ctx, migrationVolumeSpec)
			if err != nil {
				log.Errorf("FullSync: Failed to get VolumeID from volumeMigrationService for spec: %v. Err: %+v",
					migrationVolumeSpec, err)
				return nil, nil, nil, err
			}
		} else {
			// Do nothing for other cases.
			continue
		}
		if metadata, ok := volumeToK8sEntityMetadataMap[volumeHandle]; ok {
			volumeToK8sEntityMetadataMap[volumeHandle] = append(k8sMetadata, metadata...)
		} else {
			volumeToK8sEntityMetadataMap[volumeHandle] = k8sMetadata
		}
		if cnsVolumeMap[volumeHandle] {
			// PV exist in both K8S and CNS cache, add to queryVolumeIds list to
			// check if metadata has been changed or not.
			queryVolumeIds = append(queryVolumeIds, cnstypes.CnsVolumeId{Id: volumeHandle})
		}
	}
	// This check is needed to prevent querying all CNS volumes when
	// queryFilter.VolumeIds does not have any volumes. Volumes in the
	// queryFilter.VolumeIds should be one which is present in both k8s
	// and in CNS.
	if len(queryVolumeIds) == 0 {
		log.Warn("could not find any volume which is present in both k8s and in CNS")
		return volumeToCnsEntityMetadataMap, volumeToK8sEntityMetadataMap, volumeClusterDistributionMap, nil
	}
	allQueryResults, err := fullSyncGetQueryResults(ctx, queryVolumeIds,
		metadataSyncer.configInfo.Cfg.Global.ClusterID, metadataSyncer.volumeManager, metadataSyncer)
	if err != nil {
		log.Errorf("FullSync: fullSyncGetQueryResults failed to query volume metadata from vc. Err: %v", err)
		return nil, nil, nil, err
	}

	for _, queryResult := range allQueryResults {
		for _, volume := range queryResult.Volumes {
			var cnsMetadata []cnstypes.BaseCnsEntityMetadata
			allEntityMetadata := volume.Metadata.EntityMetadata
			for _, metadata := range allEntityMetadata {
				if metadata.(*cnstypes.CnsKubernetesEntityMetadata).ClusterID ==
					metadataSyncer.configInfo.Cfg.Global.ClusterID {
					cnsMetadata = append(cnsMetadata, metadata)
				}
			}
			volumeToCnsEntityMetadataMap[volume.VolumeId.Id] = cnsMetadata
			if len(volume.Metadata.ContainerClusterArray) == 1 &&
				metadataSyncer.configInfo.Cfg.Global.ClusterID == volume.Metadata.ContainerClusterArray[0].ClusterId &&
				metadataSyncer.configInfo.Cfg.Global.ClusterDistribution ==
					volume.Metadata.ContainerClusterArray[0].ClusterDistribution {
				log.Debugf("Volume %s has cluster distribution set to %s",
					volume.Name, volume.Metadata.ContainerClusterArray[0].ClusterDistribution)
				volumeClusterDistributionMap[volume.VolumeId.Id] = true
			}

		}
	}
	return volumeToCnsEntityMetadataMap, volumeToK8sEntityMetadataMap, volumeClusterDistributionMap, nil
}

// fullSyncGetVolumeSpecs return list of CnsVolumeCreateSpec for volumes which
// needs to be created in CNS and a list of CnsVolumeMetadataUpdateSpec for
// volumes which needs to be updated in CNS.
func fullSyncGetVolumeSpecs(ctx context.Context, vCenterVersion string, pvList []*v1.PersistentVolume,
	volumeToCnsEntityMetadataMap map[string][]cnstypes.BaseCnsEntityMetadata,
	volumeToK8sEntityMetadataMap map[string][]cnstypes.BaseCnsEntityMetadata,
	volumeClusterDistributionMap map[string]bool, containerCluster cnstypes.CnsContainerCluster,
	metadataSyncer *metadataSyncInformer, migrationFeatureStateForFullSync bool) (
	[]cnstypes.CnsVolumeCreateSpec, []cnstypes.CnsVolumeMetadataUpdateSpec) {
	log := logger.GetLogger(ctx)
	var createSpecArray []cnstypes.CnsVolumeCreateSpec
	var updateSpecArray []cnstypes.CnsVolumeMetadataUpdateSpec

	for _, pv := range pvList {
		var operationType string
		var volumeHandle string
		if pv.Spec.VsphereVolume != nil && migrationFeatureStateForFullSync {
			var err error
			migrationVolumeSpec := &migration.VolumeSpec{
				VolumePath:        pv.Spec.VsphereVolume.VolumePath,
				StoragePolicyName: pv.Spec.VsphereVolume.StoragePolicyName}
			volumeHandle, err = volumeMigrationService.GetVolumeID(ctx, migrationVolumeSpec)
			if err != nil {
				log.Warnf("FullSync: Failed to get VolumeID from volumeMigrationService for spec: %v. Err: %+v",
					migrationVolumeSpec, err)
				continue
			}
		} else {
			volumeHandle = pv.Spec.CSI.VolumeHandle
		}
		volumeToCnsEntityMetadata, presentInCNS := volumeToCnsEntityMetadataMap[volumeHandle]
		volumeToK8sEntityMetadata, presentInK8S := volumeToK8sEntityMetadataMap[volumeHandle]
		_, volumeClusterDistributionSet := volumeClusterDistributionMap[volumeHandle]
		if !presentInK8S {
			log.Infof("FullSync: Skipping volume: %s with VolumeId %q. Volume is not present in the k8s",
				pv.Name, volumeHandle)
			continue
		}
		if !presentInCNS {
			// PV exist in K8S but not in CNS cache, need to create
			if _, existsInCnsCreationMap := cnsCreationMap[volumeHandle]; existsInCnsCreationMap {
				// Volume was present in cnsCreationMap across two full-sync cycles.
				log.Infof("FullSync: create is required for volume: %q", volumeHandle)
				operationType = "createVolume"
			} else {
				log.Infof("FullSync: Volume with id: %q and name: %q is added to cnsCreationMap", volumeHandle, pv.Name)
				cnsCreationMap[volumeHandle] = true
			}
		} else {
			// volume exist in K8S and CNS, Check if update is required.
			if isUpdateRequired(ctx, vCenterVersion, volumeToK8sEntityMetadata,
				volumeToCnsEntityMetadata, volumeClusterDistributionSet) {
				log.Infof("FullSync: update is required for volume: %q", volumeHandle)
				operationType = "updateVolume"
			} else {
				log.Infof("FullSync: update is not required for volume: %q", volumeHandle)
			}
		}
		switch operationType {
		case "createVolume":
			var volumeType string
			if IsMultiAttachAllowed(pv) {
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
					EntityMetadata:        volumeToK8sEntityMetadataMap[volumeHandle],
				},
			}
			if volumeType == common.BlockVolumeType {
				createSpec.BackingObjectDetails = &cnstypes.CnsBlockBackingDetails{
					CnsBackingObjectDetails: cnstypes.CnsBackingObjectDetails{},
					BackingDiskId:           volumeHandle,
				}
			} else {
				createSpec.BackingObjectDetails = &cnstypes.CnsVsanFileShareBackingDetails{
					CnsFileBackingDetails: cnstypes.CnsFileBackingDetails{
						BackingFileId: volumeHandle,
					},
				}
			}
			createSpecArray = append(createSpecArray, createSpec)
		case "updateVolume":
			// Volume exist in K8S and CNS cache, but metadata is different, need
			// to update this volume.
			log.Debugf("FullSync: Volume with id %q added to volume update list", volumeHandle)
			var volumeType string
			if IsMultiAttachAllowed(pv) {
				volumeType = common.FileVolumeType
			} else {
				volumeType = common.BlockVolumeType
			}
			updateSpec := cnstypes.CnsVolumeMetadataUpdateSpec{
				VolumeId: cnstypes.CnsVolumeId{
					Id: volumeHandle,
				},
				Metadata: cnstypes.CnsVolumeMetadata{
					ContainerCluster:      containerCluster,
					ContainerClusterArray: []cnstypes.CnsContainerCluster{containerCluster},
					// Update metadata in CNS with the new metadata present in K8S.
					EntityMetadata: volumeToK8sEntityMetadataMap[volumeHandle],
				},
			}
			// Delete metadata in CNS which is not present in K8S.
			for _, oldMetadata := range volumeToCnsEntityMetadataMap[volumeHandle] {
				oldEntityName := oldMetadata.(*cnstypes.CnsKubernetesEntityMetadata).EntityName
				oldEntityNameSpace := oldMetadata.(*cnstypes.CnsKubernetesEntityMetadata).Namespace
				oldEntityType := oldMetadata.(*cnstypes.CnsKubernetesEntityMetadata).EntityType
				found := false
				for _, newMetadata := range updateSpec.Metadata.EntityMetadata {
					newEntityName := newMetadata.(*cnstypes.CnsKubernetesEntityMetadata).EntityName
					newEntityNameSpace := newMetadata.(*cnstypes.CnsKubernetesEntityMetadata).Namespace
					newEntityType := newMetadata.(*cnstypes.CnsKubernetesEntityMetadata).EntityType
					if oldEntityName == newEntityName && oldEntityNameSpace == newEntityNameSpace &&
						oldEntityType == newEntityType {
						found = true
						break
					}
				}
				if !found {
					oldMetadata.(*cnstypes.CnsKubernetesEntityMetadata).Delete = true
					updateSpec.Metadata.EntityMetadata = append(updateSpec.Metadata.EntityMetadata, oldMetadata)
				}
			}

			if volumeType == common.BlockVolumeType {
				// For Block volume, CNS only allow one instances for one
				// EntityMetadata type in UpdateVolumeMetadataSpec. If there are
				// more than one pod instances in the UpdateVolumeMetadataSpec,
				// need to invoke multiple UpdateVolumeMetadata call.
				var metadataList []cnstypes.BaseCnsEntityMetadata
				var podMetadataList []cnstypes.BaseCnsEntityMetadata
				for _, metadata := range updateSpec.Metadata.EntityMetadata {
					entityType := metadata.(*cnstypes.CnsKubernetesEntityMetadata).EntityType
					if entityType == string(cnstypes.CnsKubernetesEntityTypePOD) {
						podMetadataList = append(podMetadataList, metadata)
					} else {
						metadataList = append(metadataList, metadata)
					}
				}
				if len(podMetadataList) > 0 {
					for _, podMetadata := range podMetadataList {
						updateSpecNew := cnstypes.CnsVolumeMetadataUpdateSpec{
							VolumeId: cnstypes.CnsVolumeId{
								Id: volumeHandle,
							},
							Metadata: cnstypes.CnsVolumeMetadata{
								ContainerCluster:      containerCluster,
								ContainerClusterArray: []cnstypes.CnsContainerCluster{containerCluster},
								// Update metadata in CNS with the new metadata present in K8S.
								EntityMetadata: append(metadataList, podMetadata),
							},
						}
						log.Debugf("FullSync: updateSpec %+v is added to updateSpecArray\n", spew.Sdump(updateSpecNew))
						updateSpecArray = append(updateSpecArray, updateSpecNew)
					}
				} else {
					updateSpecArray = append(updateSpecArray, updateSpec)
				}
			} else {
				updateSpecArray = append(updateSpecArray, updateSpec)
			}
		}
	}
	return createSpecArray, updateSpecArray
}

// getVolumesToBeDeleted return list of volumeIds that need to be deleted.
// A volumeId is added to this list only if it was present in cnsDeletionMap
// across two cycles of full sync.
func getVolumesToBeDeleted(ctx context.Context, cnsVolumeList []cnstypes.CnsVolume, k8sPVMap map[string]string,
	metadataSyncer *metadataSyncInformer, migrationFeatureStateForFullSync bool) ([]cnstypes.CnsVolumeId, error) {
	log := logger.GetLogger(ctx)
	var volToBeDeleted []cnstypes.CnsVolumeId
	// inlineVolumeMap holds the volume path information for migrated volumes
	// which are used by Pods.
	inlineVolumeMap := make(map[string]string)
	var err error
	if migrationFeatureStateForFullSync {
		inlineVolumeMap, err = fullSyncGetInlineMigratedVolumesInfo(ctx, metadataSyncer, migrationFeatureStateForFullSync)
		if err != nil {
			log.Errorf("FullSync: Failed to get inline migrated volumes. Err: %v", err)
			return volToBeDeleted, err
		}
	}
	for _, vol := range cnsVolumeList {
		if _, existsInK8s := k8sPVMap[vol.VolumeId.Id]; !existsInK8s {
			if _, existsInCnsDeletionMap := cnsDeletionMap[vol.VolumeId.Id]; existsInCnsDeletionMap {
				// Volume does not exist in K8s across two fullsync cycles, because
				// it was present in cnsDeletionMap across two full sync cycles.
				// Add it to delete list.
				log.Debugf("FullSync: Volume with id %s added to delete list", vol.VolumeId.Id)
				volToBeDeleted = append(volToBeDeleted, vol.VolumeId)
			} else {
				// Add to cnsDeletionMap.
				if migrationFeatureStateForFullSync {
					// If migration is ON, verify if the volume is present in inlineVolumeMap.
					if _, existsInInlineVolumeMap := inlineVolumeMap[vol.VolumeId.Id]; !existsInInlineVolumeMap {
						log.Infof("FullSync: Volume with id %q added to cnsDeletionMap", vol.VolumeId.Id)
						cnsDeletionMap[vol.VolumeId.Id] = true
					} else {
						log.Debugf("FullSync: Inline migrated volume with id %s is in use. Skipping for deletion",
							vol.VolumeId.Id)
					}
				} else {
					log.Debugf("FullSync: Volume with id %s added to cnsDeletionMap", vol.VolumeId.Id)
					cnsDeletionMap[vol.VolumeId.Id] = true
				}
			}
		}
	}
	return volToBeDeleted, nil
}

// buildPVCMapPodMap build two maps to help find
// 1) PVC for given PV, and 2) POD mounted to given PVC.
// pvToPVCMap maps PV name to corresponding PVC, key is pv name.
// pvcToPodMap maps PVC to the array of PODs using the PVC, key is
// "pod.Namespace/pvc.Name".
func buildPVCMapPodMap(ctx context.Context, pvList []*v1.PersistentVolume,
	metadataSyncer *metadataSyncInformer) (pvcMap, podMap, error) {
	log := logger.GetLogger(ctx)
	pvToPVCMap := make(pvcMap)
	pvcToPodMap := make(podMap)
	pods, err := metadataSyncer.podLister.Pods(v1.NamespaceAll).List(labels.Everything())
	if err != nil {
		log.Warnf("FullSync: Failed to get pods in all namespaces. err=%v", err)
		return nil, nil, err
	}
	for _, pv := range pvList {
		if pv.Spec.ClaimRef != nil && pv.Status.Phase == v1.VolumeBound {
			pvc, err := metadataSyncer.pvcLister.PersistentVolumeClaims(
				pv.Spec.ClaimRef.Namespace).Get(pv.Spec.ClaimRef.Name)
			if err != nil {
				log.Warnf("FullSync: Failed to get pvc for namespace %s and name %s. err=%v",
					pv.Spec.ClaimRef.Namespace, pv.Spec.ClaimRef.Name, err)
				return nil, nil, err
			}
			pvToPVCMap[pv.Name] = pvc
			log.Debugf("FullSync: pvc %s/%s is backed by pv %s", pvc.Namespace, pvc.Name, pv.Name)
			for _, pod := range pods {
				if pod.Status.Phase == v1.PodRunning && pod.Spec.Volumes != nil {
					for _, volume := range pod.Spec.Volumes {
						pvClaim := volume.VolumeSource.PersistentVolumeClaim
						if pvClaim != nil && pvClaim.ClaimName == pvc.Name && pod.Namespace == pvc.Namespace {
							key := pod.Namespace + "/" + pvClaim.ClaimName
							pvcToPodMap[key] = append(pvcToPodMap[key], pod)
							log.Debugf("FullSync: pvc %s is mounted by pod %s/%s", key, pod.Namespace, pod.Name)
							break
						}
					}
				}
			}
		}
	}
	return pvToPVCMap, pvcToPodMap, nil
}

// isUpdateRequired compares the input metadata list from K8S and metadata
// list from CNS and returns true if update operation is required. Otherwise,
// returns false.
func isUpdateRequired(ctx context.Context, vCenterVersion string, k8sMetadataList []cnstypes.BaseCnsEntityMetadata,
	cnsMetadataList []cnstypes.BaseCnsEntityMetadata, volumeClusterDistributionSet bool) bool {
	log := logger.GetLogger(ctx)
	log.Debugf("FullSync: isUpdateRequired called with k8sMetadataList: %+v \n", spew.Sdump(k8sMetadataList))
	log.Debugf("FullSync: isUpdateRequired called with cnsMetadataList: %+v \n", spew.Sdump(cnsMetadataList))
	if vCenterVersion != cns.ReleaseVSAN67u3 && vCenterVersion != cns.ReleaseVSAN70 &&
		vCenterVersion != cns.ReleaseVSAN70u1 {
		// Update is required if cluster distribution is not set on volume on
		// vSphere 7.0u2 and above.
		if !volumeClusterDistributionSet {
			return true
		}
	}

	if len(k8sMetadataList) == len(cnsMetadataList) {
		// Same number of entries for volume in K8s and CNS.
		// Need to check if entries match.
		cnsEntityTypeMetadataMap := make(map[string]*cnstypes.CnsKubernetesEntityMetadata)
		for _, cnsMetadata := range cnsMetadataList {
			metadata := cnsMetadata.(*cnstypes.CnsKubernetesEntityMetadata)
			// Here key is required to retrieve specific entity metadata from
			// cnsEntityTypeMetadataMap, while traversing through k8sMetadataList,
			// to compare metadata in k8s and CNS.
			key := metadata.EntityType + ":" + metadata.EntityName + ":" + metadata.Namespace
			cnsEntityTypeMetadataMap[key] = metadata
		}
		log.Debugf("cnsEntityTypeMetadataMap :%+v", spew.Sdump(cnsEntityTypeMetadataMap))
		for _, k8sMetadata := range k8sMetadataList {
			metadata := k8sMetadata.(*cnstypes.CnsKubernetesEntityMetadata)
			key := metadata.EntityType + ":" + metadata.EntityName + ":" + metadata.Namespace
			cnsMetadata, ok := cnsEntityTypeMetadataMap[key]
			if !ok {
				log.Debugf("key: %q is not found in the cnsEntityTypeMetadataMap", key)
				return true
			}
			if !cnsvsphere.CompareKubernetesMetadata(ctx, metadata, cnsMetadata) {
				return true
			}
		}
	} else {
		// K8s metadata entries and CNS metadata entries does not match.
		// Need to update.
		return true
	}
	return false
}

// cleanupCnsMaps performs cleanup on cnsCreationMap and cnsDeletionMap.
// Removes volume entries from cnsCreationMap that do not exist in K8s
// and volume entries from cnsDeletionMap that exist in K8s.
// An entry could have been added to cnsCreationMap (or cnsDeletionMap),
// because full sync was triggered in between the delete (or create)
// operation of a volume.
func cleanupCnsMaps(k8sPVs map[string]string) {
	// Cleanup cnsCreationMap.
	for volID := range cnsCreationMap {
		if _, existsInK8s := k8sPVs[volID]; !existsInK8s {
			delete(cnsCreationMap, volID)
		}
	}
	// Cleanup cnsDeletionMap.
	for volID := range cnsDeletionMap {
		if _, existsInK8s := k8sPVs[volID]; existsInK8s {
			// Delete volume from cnsDeletionMap which is present in kubernetes.
			delete(cnsDeletionMap, volID)
		}
	}
}
