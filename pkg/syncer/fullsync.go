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
	"errors"
	"sync"
	"time"

	"github.com/davecgh/go-spew/spew"
	"github.com/vmware/govmomi/cns"
	cnstypes "github.com/vmware/govmomi/cns/types"
	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/vsphere-csi-driver/v3/pkg/apis/migration"
	volumes "sigs.k8s.io/vsphere-csi-driver/v3/pkg/common/cns-lib/volume"
	cnsvsphere "sigs.k8s.io/vsphere-csi-driver/v3/pkg/common/cns-lib/vsphere"
	"sigs.k8s.io/vsphere-csi-driver/v3/pkg/common/prometheus"
	"sigs.k8s.io/vsphere-csi-driver/v3/pkg/csi/service/common"
	"sigs.k8s.io/vsphere-csi-driver/v3/pkg/csi/service/common/commonco"
	"sigs.k8s.io/vsphere-csi-driver/v3/pkg/csi/service/logger"
	cnsvolumeinfov1alpha1 "sigs.k8s.io/vsphere-csi-driver/v3/pkg/internalapis/cnsvolumeinfo/v1alpha1"
)

// CsiFullSync reconciles volume metadata on a vanilla k8s cluster with volume
// metadata on CNS.
func CsiFullSync(ctx context.Context, metadataSyncer *metadataSyncInformer, vc string) error {
	log := logger.GetLogger(ctx)
	log.Infof("FullSync for VC %s: start", vc)
	fullSyncStartTime := time.Now()
	var migrationFeatureStateForFullSync bool
	var err error
	// Fetch CSI migration feature state, before performing full sync operations.
	if metadataSyncer.clusterFlavor == cnstypes.CnsClusterFlavorVanilla {
		migrationFeatureStateForFullSync = metadataSyncer.coCommonInterface.IsFSSEnabled(ctx, common.CSIMigration)
	}
	defer func() {
		fullSyncStatus := prometheus.PrometheusPassStatus
		if err != nil {
			fullSyncStatus = prometheus.PrometheusFailStatus
		}
		prometheus.FullSyncOpsHistVec.WithLabelValues(fullSyncStatus).Observe(
			(time.Since(fullSyncStartTime)).Seconds())
	}()

	// Get K8s PVs in State "Bound", "Available" or "Released" for the given VC.
	k8sPVs, err := getPVsInBoundAvailableOrReleasedForVc(ctx, metadataSyncer, vc)
	if err != nil {
		log.Errorf("FullSync for VC %s: Failed to get PVs from kubernetes. Err: %v", vc, err)
		return err
	}

	// k8sPVMap is useful for clean and quicker look up.
	k8sPVMap := make(map[string]string)

	// Iterate through all the k8sPVs and use volume id as the key for k8sPVMap
	// items. For migrated volumes, invoke GetVolumeID from migration service.
	for _, pv := range k8sPVs {
		// k8sPVs contains valid CSI volumes or migrated vSphere volumes
		if pv.Spec.CSI != nil {
			k8sPVMap[pv.Spec.CSI.VolumeHandle] = ""
		} else if migrationFeatureStateForFullSync && pv.Spec.VsphereVolume != nil {
			// For vSphere volumes, migration service will register volumes in CNS.

			// Note that we can never reach here in case of a multi VC setup
			// as we have already filtered out in-tree volumes.
			if volumeMigrationService == nil {
				// Instantiate volumeMigrationService when migration feature state is True.
				if err = initVolumeMigrationService(ctx, metadataSyncer); err != nil {
					log.Errorf("FullSync for VC %s: Failed to get migration service. Err: %v", vc, err)
					return err
				}
			}

			migrationVolumeSpec := &migration.VolumeSpec{
				VolumePath:        pv.Spec.VsphereVolume.VolumePath,
				StoragePolicyName: pv.Spec.VsphereVolume.StoragePolicyName}
			var volumeHandle string
			volumeHandle, err = volumeMigrationService.GetVolumeID(ctx, migrationVolumeSpec, true)
			if err != nil {
				log.Errorf("FullSync for VC %s: Failed to get VolumeID from volumeMigrationService for spec: %v. Err: %+v",
					vc, migrationVolumeSpec, err)
				return err
			}
			k8sPVMap[volumeHandle] = ""
		}
	}
	// pvToPVCMap maps pv name to corresponding PVC.
	// pvcToPodMap maps pvc to the mounted Pod.
	pvToPVCMap, pvcToPodMap, err := buildPVCMapPodMap(ctx, k8sPVs, metadataSyncer, vc)
	if err != nil {
		log.Errorf("FullSync for VC %s: Failed to build PVCMap and PodMap. Err: %v", vc, err)
		return err
	}
	log.Debugf("FullSync for VC %s: pvToPVCMap %v", vc, pvToPVCMap)
	log.Debugf("FullSyncfor VC %s: pvcToPodMap %v", vc, pvcToPodMap)
	// Call CNS QueryAll to get container volumes by cluster ID.
	queryFilter := cnstypes.CnsQueryFilter{
		ContainerClusterIds: []string{
			metadataSyncer.configInfo.Cfg.Global.ClusterID,
		},
	}

	volManager, err := getVolManagerForVcHost(ctx, vc, metadataSyncer)
	if err != nil {
		log.Errorf("FullSync for VC %s: Failed to get volume manager. Err: %v", vc, err)
		return err
	}

	queryAllResult, err := volManager.QueryAllVolume(ctx, queryFilter, cnstypes.CnsQuerySelection{})
	if err != nil {
		log.Errorf("FullSync for VC %s: QueryVolume failed with err=%+v", vc, err.Error())
		return err
	}

	if metadataSyncer.clusterFlavor == cnstypes.CnsClusterFlavorWorkload &&
		commonco.ContainerOrchestratorUtility.IsFSSEnabled(ctx, common.TKGsHA) {
		// Replace Volume Metadata using old cluster ID and replace with the new SupervisorID
		if len(queryAllResult.Volumes) > 0 {
			var updateMetadataSpecArray []cnstypes.CnsVolumeMetadataUpdateSpec
			for _, volume := range queryAllResult.Volumes {
				var updatedContainerClusterArray []cnstypes.CnsContainerCluster
				var updatedContainerCluster cnstypes.CnsContainerCluster
				for _, containercluster := range volume.Metadata.ContainerClusterArray {
					if containercluster.ClusterId == metadataSyncer.configInfo.Cfg.Global.ClusterID {
						containercluster.ClusterId = metadataSyncer.configInfo.Cfg.Global.SupervisorID
						updatedContainerCluster = containercluster
					}
					updatedContainerClusterArray = append(updatedContainerClusterArray, containercluster)
				}
				updateSpecToDeleteMetadata := cnstypes.CnsVolumeMetadataUpdateSpec{
					VolumeId: cnstypes.CnsVolumeId{
						Id: volume.VolumeId.Id,
					},
					Metadata: cnstypes.CnsVolumeMetadata{
						ContainerCluster:      volume.Metadata.ContainerCluster,
						ContainerClusterArray: volume.Metadata.ContainerClusterArray,
					},
				}
				updateSpecToAddMetadata := cnstypes.CnsVolumeMetadataUpdateSpec{
					VolumeId: cnstypes.CnsVolumeId{
						Id: volume.VolumeId.Id,
					},
					Metadata: cnstypes.CnsVolumeMetadata{
						ContainerCluster:      updatedContainerCluster,
						ContainerClusterArray: updatedContainerClusterArray,
					},
				}
				for _, entityMetadata := range volume.Metadata.EntityMetadata {
					if entityMetadata.GetCnsEntityMetadata().ClusterID ==
						metadataSyncer.configInfo.Cfg.Global.ClusterID {
						// Delete metadata for associated with old cluster ID
						oldk8sEntityMetadata := *entityMetadata.(*cnstypes.CnsKubernetesEntityMetadata)
						oldk8sEntityMetadata.Delete = true
						// add metadata for new supervisor id
						newk8sEntityMetadata := *entityMetadata.(*cnstypes.CnsKubernetesEntityMetadata)
						newk8sEntityMetadata.ClusterID = metadataSyncer.configInfo.Cfg.Global.SupervisorID
						for index, referredEntity := range newk8sEntityMetadata.ReferredEntity {
							if referredEntity.ClusterID == metadataSyncer.configInfo.Cfg.Global.ClusterID {
								referredEntity.ClusterID = metadataSyncer.configInfo.Cfg.Global.SupervisorID
							}
							newk8sEntityMetadata.ReferredEntity[index] = referredEntity
						}
						updateSpecToDeleteMetadata.Metadata.EntityMetadata =
							append(updateSpecToDeleteMetadata.Metadata.EntityMetadata, &oldk8sEntityMetadata)
						updateSpecToAddMetadata.Metadata.EntityMetadata =
							append(updateSpecToAddMetadata.Metadata.EntityMetadata, &newk8sEntityMetadata)
					}
				}
				if len(updateSpecToDeleteMetadata.Metadata.EntityMetadata) > 0 {
					updateMetadataSpecArray = append(updateMetadataSpecArray, updateSpecToDeleteMetadata)
				}
				// TODO: Remove this check after CNS resolve issue regarding removal of old cluster-id
				// from ContainerClusterArray. This will allow replacing cluster-id for volume which does not have any
				// entity metadata.
				if len(updateSpecToAddMetadata.Metadata.EntityMetadata) > 0 {
					updateMetadataSpecArray = append(updateMetadataSpecArray, updateSpecToAddMetadata)
				}
			}
			if len(updateMetadataSpecArray) > 0 {
				log.Infof("FullSync for VC %s: Replacing ClusterID: %q with new SupervisorID: %q",
					vc, metadataSyncer.configInfo.Cfg.Global.ClusterID,
					metadataSyncer.configInfo.Cfg.Global.SupervisorID)
			}
			for _, updateSpec := range updateMetadataSpecArray {
				log.Debugf("Calling UpdateVolumeMetadata for volume %s with updateSpec: %+v",
					updateSpec.VolumeId.Id, spew.Sdump(updateSpec))
				if err := volManager.UpdateVolumeMetadata(ctx, &updateSpec); err != nil {
					log.Warnf("FullSync for VC %s: UpdateVolumeMetadata failed while replacing clusterID "+
						"with supervisorID. Error: %+v", vc, err)
				}
			}
		}
		// Call CNS QueryAll to get container volumes by cluster ID.
		queryFilter = cnstypes.CnsQueryFilter{
			ContainerClusterIds: []string{
				metadataSyncer.configInfo.Cfg.Global.SupervisorID,
			},
		}
		// get queryAllResult using new Supervisor ID for rest of full sync operations
		queryAllResult, err = volManager.QueryAllVolume(ctx, queryFilter, cnstypes.CnsQuerySelection{})
		if err != nil {
			log.Errorf("FullSync for VC %s: QueryVolume failed with err=%+v", vc, err.Error())
			return err
		}
	}

	vcHostObj, vcHostObjFound := metadataSyncer.configInfo.Cfg.VirtualCenter[vc]
	if !vcHostObjFound {
		log.Errorf("FullSync for VC %s: Failed to get VC host object.", vc)
		return errors.New("failed to get VC host object")
	}

	volumeToCnsEntityMetadataMap, volumeToK8sEntityMetadataMap, volumeClusterDistributionMap, err :=
		fullSyncConstructVolumeMaps(ctx, k8sPVs, queryAllResult.Volumes, pvToPVCMap,
			pvcToPodMap, metadataSyncer, migrationFeatureStateForFullSync, volManager, vc)
	if err != nil {
		log.Errorf("FullSync for VC %s: fullSyncGetEntityMetadata failed with err %+v", vc, err)
		return err
	}
	log.Debugf("FullSync for VC %s: pvToCnsEntityMetadataMap %+v \n pvToK8sEntityMetadataMap: %+v \n",
		vc, spew.Sdump(volumeToCnsEntityMetadataMap), spew.Sdump(volumeToK8sEntityMetadataMap))
	log.Debugf("FullSync for VC %s: volumes where clusterDistribution is set: %+v", vc, volumeClusterDistributionMap)

	var vcenter *cnsvsphere.VirtualCenter
	// Get VC instance.
	if isMultiVCenterFssEnabled {
		vcenter, err = cnsvsphere.GetVirtualCenterInstanceForVCenterHost(ctx, vc, true)
		if err != nil {
			log.Errorf("failed to get virtual center instance for VC: %s. Error: %v", vc, err)
			return err
		}
	} else {
		vcenter, err = cnsvsphere.GetVirtualCenterInstance(ctx, metadataSyncer.configInfo, false)
		if err != nil {
			log.Errorf("failed to get virtual center instance with error: %v", err)
			return err
		}
	}

	containerCluster := cnsvsphere.GetContainerCluster(clusterIDforVolumeMetadata,
		vcHostObj.User, metadataSyncer.clusterFlavor,
		metadataSyncer.configInfo.Cfg.Global.ClusterDistribution)
	createSpecArray, updateSpecArray := fullSyncGetVolumeSpecs(ctx, vcenter.Client.Version, k8sPVs,
		volumeToCnsEntityMetadataMap, volumeToK8sEntityMetadataMap, volumeClusterDistributionMap,
		containerCluster, migrationFeatureStateForFullSync, vc)
	volToBeDeleted, err := getVolumesToBeDeleted(ctx, queryAllResult.Volumes, k8sPVMap, metadataSyncer,
		migrationFeatureStateForFullSync, vc)
	if err != nil {
		log.Errorf("FullSync for VC %s: failed to get list of volumes to be deleted with err %+v", vc, err)
		return err
	}

	wg := sync.WaitGroup{}
	wg.Add(3)
	// Perform operations.
	go fullSyncCreateVolumes(ctx, createSpecArray, metadataSyncer, &wg, migrationFeatureStateForFullSync, volManager, vc)
	go fullSyncUpdateVolumes(ctx, updateSpecArray, metadataSyncer, &wg, volManager, vc)
	go fullSyncDeleteVolumes(ctx, volToBeDeleted, metadataSyncer, &wg, migrationFeatureStateForFullSync, volManager, vc)
	wg.Wait()

	// Sync VolumeInfo CRs
	if isMultiVCenterFssEnabled && len(metadataSyncer.configInfo.Cfg.VirtualCenter) > 1 {
		volumeInfoCRFullSync(ctx, k8sPVMap, vc)
	}

	cleanupCnsMaps(k8sPVMap, vc)
	log.Debugf("FullSync for VC %s: cnsDeletionMap at end of cycle: %v", vc, cnsDeletionMap)
	log.Debugf("FullSync for VC %s: cnsCreationMap at end of cycle: %v", vc, cnsCreationMap)
	log.Infof("FullSync for VC %s: end", vc)
	return nil
}

// volumeInfoCRFullSync creates VolumeInfo CR if it does not already exist for a volume.
// It also deletes VolumeInfo CR if its corresponding PV does not exist.
func volumeInfoCRFullSync(ctx context.Context, k8sPvs map[string]string, vc string) {
	log := logger.GetLogger(ctx)

	log.Debugf("Starting volumeInfo CR full sync.")

	for volumeID := range k8sPvs {
		crExists, err := volumeInfoService.VolumeInfoCrExistsForVolume(ctx, volumeID)
		if err != nil {
			log.Errorf("FullSync for VC %s: failed to find VolumeInfo CR for volume %s."+
				"Error: %+v", vc, volumeID, err)
			continue
		}

		// Create VolumeInfo CR if not found.
		if !crExists {
			err := volumeInfoService.CreateVolumeInfo(ctx, volumeID, vc)
			if err != nil {
				log.Errorf("FullSync for VC %s: failed to create VolumeInfo CR for volume %s."+
					"Error: %+v", vc, volumeID, err)
				continue
			}
		}
	}

	volumeInfoCRList := volumeInfoService.ListAllVolumeInfos()
	for _, volumeInfo := range volumeInfoCRList {
		cnsvolumeinfo := &cnsvolumeinfov1alpha1.CNSVolumeInfo{}
		err := runtime.DefaultUnstructuredConverter.FromUnstructured(volumeInfo.(*unstructured.Unstructured).Object,
			&cnsvolumeinfo)
		if err != nil {
			log.Errorf("FullSync for VC %s: failed to parse cnsvolumeinfo object: %v, err: %v", vc, cnsvolumeinfo, err)
			continue
		}

		if cnsvolumeinfo.Spec.VCenterServer == vc {
			// Delete this CR if corresponding PV is not found
			if _, exists := k8sPvs[cnsvolumeinfo.Spec.VolumeID]; !exists {
				err := volumeInfoService.DeleteVolumeInfo(ctx, cnsvolumeinfo.Spec.VolumeID)
				if err != nil {
					log.Errorf("FullSync for VC %s: failed to delete VolumeInfo CR for volume %s."+
						"Error: %+v", vc, cnsvolumeinfo.Spec.VolumeID, err)
					continue
				}
			}
		}
	}

}

// fullSyncCreateVolumes creates volumes with given array of createSpec.
// Before creating a volume, all current K8s volumes are retrieved.
// If the volume is successfully created, it is removed from cnsCreationMap.
func fullSyncCreateVolumes(ctx context.Context, createSpecArray []cnstypes.CnsVolumeCreateSpec,
	metadataSyncer *metadataSyncInformer, wg *sync.WaitGroup, migrationFeatureStateForFullSync bool,
	volManager volumes.Manager, vc string) {
	log := logger.GetLogger(ctx)
	defer wg.Done()
	currentK8sPVMap := make(map[string]bool)
	volumeOperationsLock[vc].Lock()
	defer volumeOperationsLock[vc].Unlock()
	// Get all K8s PVs in the given VC.
	currentK8sPV, err := getPVsInBoundAvailableOrReleasedForVc(ctx, metadataSyncer, vc)
	if err != nil {
		log.Errorf("FullSync for VC %s: fullSyncCreateVolumes failed to get PVs from kubernetes. Err: %v", vc, err)
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
			var volumeHandle string
			volumeHandle, err = volumeMigrationService.GetVolumeID(ctx, migrationVolumeSpec, true)
			if err != nil {
				log.Errorf("FullSync for VC %s: Failed to get VolumeID from volumeMigrationService for spec: %v. Err: %+v",
					vc, migrationVolumeSpec, err)
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
			// We should never reach here in case of multi VC deployment as file share volumes are already filtered out.
			volumeID = createSpec.BackingObjectDetails.(*cnstypes.CnsVsanFileShareBackingDetails).BackingFileId
		} else {
			log.Warnf("Skipping createSpec: %+v as VolumeType is unknown or BackingObjectDetails is not valid",
				spew.Sdump(createSpec))
			continue
		}
		if _, existsInK8s := currentK8sPVMap[volumeID]; existsInK8s {
			log.Debugf("FullSync for VC %s: Calling CreateVolume for volume id: %q with createSpec %+v",
				vc, volumeID, spew.Sdump(createSpec))
			_, _, err := volManager.CreateVolume(ctx, &createSpec)
			if err != nil {
				log.Warnf("FullSync for VC %s: Failed to create volume with the spec: %+v. "+
					"Err: %+v", vc, spew.Sdump(createSpec), err)
				continue
			}

			if isMultiVCenterFssEnabled && len(metadataSyncer.configInfo.Cfg.VirtualCenter) > 1 {
				// Create CNSVolumeInfo CR for the volume ID.
				err = volumeInfoService.CreateVolumeInfo(ctx, volumeID, vc)
				if err != nil {
					log.Errorf("FullSync for VC %s: failed to store volumeID %q in CNSVolumeInfo CR. Error: %+v",
						vc, volumeID, err)
				}
			}

		} else {
			log.Debugf("FullSync for VC %s: volumeID %s does not exist in Kubernetes, "+
				"no need to create volume in CNS", vc, volumeID)
		}
		delete(cnsCreationMap[vc], volumeID)
	}

}

// fullSyncDeleteVolumes deletes volumes with given array of volumeId.
// Before deleting a volume, all current K8s volumes are retrieved.
// If the volume is successfully deleted, it is removed from cnsDeletionMap.
func fullSyncDeleteVolumes(ctx context.Context, volumeIDDeleteArray []cnstypes.CnsVolumeId,
	metadataSyncer *metadataSyncInformer, wg *sync.WaitGroup,
	migrationFeatureStateForFullSync bool, volManager volumes.Manager, vc string) {
	defer wg.Done()
	log := logger.GetLogger(ctx)
	deleteDisk := false
	currentK8sPVMap := make(map[string]bool)
	volumeOperationsLock[vc].Lock()
	defer volumeOperationsLock[vc].Unlock()
	// Get all K8s PVs.
	currentK8sPV, err := getPVsInBoundAvailableOrReleasedForVc(ctx, metadataSyncer, vc)
	if err != nil {
		log.Errorf("FullSync for VC %s: fullSyncDeleteVolumes failed to get PVs from kubernetes. Err: %v", vc, err)
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
			volumeHandle, err := volumeMigrationService.GetVolumeID(ctx, migrationVolumeSpec, true)
			if err != nil {
				log.Errorf("FullSync for VC %s: Failed to get VolumeID from volumeMigrationService for spec: %v. Err: %+v",
					vc, migrationVolumeSpec, err)
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
		log.Infof("FullSync for VC %s: fullSyncDeleteVolumes could not find any volume "+
			"which is not present in k8s and needs to be checked for volume deletion.", vc)
		return
	}
	allQueryResults, err := fullSyncGetQueryResults(ctx, queryVolumeIds, "", volManager, metadataSyncer)
	if err != nil {
		log.Errorf("FullSync for VC %s: fullSyncGetQueryResults failed to query volume metadata from vc. Err: %v", vc, err)
		return
	}
	// Verify if Volume is not in use by any other Cluster before removing CNS tag
	for _, queryResult := range allQueryResults {
		for _, volume := range queryResult.Volumes {
			inUsebyOtherK8SCluster := false
			for _, metadata := range volume.Metadata.EntityMetadata {
				if metadata.(*cnstypes.CnsKubernetesEntityMetadata).ClusterID != clusterIDforVolumeMetadata {
					inUsebyOtherK8SCluster = true
					log.Debugf("FullSync for VC %s: fullSyncDeleteVolumes: Volume: %q is "+
						"in use by other cluster.", vc, volume.VolumeId.Id)
					break
				}
			}
			if !inUsebyOtherK8SCluster {
				log.Infof("FullSync for VC %s: fullSyncDeleteVolumes: Calling DeleteVolume for volume %v with delete disk %v",
					vc, volume.VolumeId.Id, deleteDisk)
				_, err := volManager.DeleteVolume(ctx, volume.VolumeId.Id, deleteDisk)
				if err != nil {
					log.Warnf("FullSync for VC %s: fullSyncDeleteVolumes: Failed to delete volume %s with error %+v",
						vc, volume.VolumeId.Id, err)
					continue
				}

				if isMultiVCenterFssEnabled && len(metadataSyncer.configInfo.Cfg.VirtualCenter) > 1 {
					// Delete CNSVolumeInfo CR for the volume ID.
					err = volumeInfoService.DeleteVolumeInfo(ctx, volume.VolumeId.Id)
					if err != nil {
						log.Errorf("failed to remove volumeID %q for vCenter %q from CNSVolumeInfo CR. Error: %+v",
							volume.VolumeId.Id, vc, err)
					}
				}

				if migrationFeatureStateForFullSync {
					err = volumeMigrationService.DeleteVolumeInfo(ctx, volume.VolumeId.Id)
					// For non-migrated volumes DeleteVolumeInfo will not return
					// error. So, the volume id will be deleted from cnsDeletionMap.
					if err != nil {
						log.Warnf("FullSync for VC %s: fullSyncDeleteVolumes: Failed to delete volume mapping CR for %s. Err: %+v",
							vc, volume.VolumeId.Id, err)
						continue
					}
				}
			}
			// Delete volume from cnsDeletionMap which is successfully deleted from
			// CNS.
			delete(cnsDeletionMap[vc], volume.VolumeId.Id)
		}
	}
}

// fullSyncUpdateVolumes update metadata for volumes with given array of
// createSpec.
func fullSyncUpdateVolumes(ctx context.Context, updateSpecArray []cnstypes.CnsVolumeMetadataUpdateSpec,
	metadataSyncer *metadataSyncInformer, wg *sync.WaitGroup, volManager volumes.Manager,
	vc string) {
	defer wg.Done()
	log := logger.GetLogger(ctx)
	for _, updateSpec := range updateSpecArray {
		log.Debugf("FullSync for VC %s: Calling UpdateVolumeMetadata for volume %s with updateSpec: %+v",
			vc, updateSpec.VolumeId.Id, spew.Sdump(updateSpec))
		if err := volManager.UpdateVolumeMetadata(ctx, &updateSpec); err != nil {
			log.Warnf("FullSync for VC %s: UpdateVolumeMetadata failed with err %v", vc, err)
		}
	}
}

// buildCnsMetadataList build metadata list for given PV.
// Metadata list may include PV metadata, PVC metadata and POD metadata.
func buildCnsMetadataList(ctx context.Context, pv *v1.PersistentVolume, pvToPVCMap pvcMap,
	pvcToPodMap podMap, clusterID string, vc string) []cnstypes.BaseCnsEntityMetadata {
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
	log.Debugf("FullSync for VC %s: buildMetadataList=%+v \n", vc, spew.Sdump(metadataList))
	return metadataList
}

// fullSyncConstructVolumeMaps builds and returns map of volume to
// EntityMetadata in CNS (volumeToCnsEntityMetadataMap), map of volume to
// EntityMetadata in kubernetes (volumeToK8sEntityMetadataMap) and set of
// volumes where ClusterDistribution is populated (volumeClusterDistributionMap).
func fullSyncConstructVolumeMaps(ctx context.Context, pvList []*v1.PersistentVolume,
	cnsVolumeList []cnstypes.CnsVolume, pvToPVCMap pvcMap, pvcToPodMap podMap,
	metadataSyncer *metadataSyncInformer, migrationFeatureStateForFullSync bool,
	volManager volumes.Manager, vc string) (
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
		k8sMetadata := buildCnsMetadataList(ctx, pv, pvToPVCMap, pvcToPodMap, clusterIDforVolumeMetadata, vc)
		var volumeHandle string
		if pv.Spec.CSI != nil {
			volumeHandle = pv.Spec.CSI.VolumeHandle
		} else if migrationFeatureStateForFullSync && pv.Spec.VsphereVolume != nil {
			// For Multi VC setup, we should never reach here as in-tree volumes are already filtered out.
			migrationVolumeSpec := &migration.VolumeSpec{
				VolumePath:        pv.Spec.VsphereVolume.VolumePath,
				StoragePolicyName: pv.Spec.VsphereVolume.StoragePolicyName}
			volumeHandle, err = volumeMigrationService.GetVolumeID(ctx, migrationVolumeSpec, true)
			if err != nil {
				log.Errorf("FullSync for VC %s: Failed to get VolumeID from volumeMigrationService for spec: %v. Err: %+v",
					vc, migrationVolumeSpec, err)
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
		clusterIDforVolumeMetadata, volManager, metadataSyncer)
	if err != nil {
		log.Errorf("FullSync for VC %s: fullSyncGetQueryResults failed to query volume metadata from vc. Err: %v", vc, err)
		return nil, nil, nil, err
	}

	for _, queryResult := range allQueryResults {
		for _, volume := range queryResult.Volumes {
			var cnsMetadata []cnstypes.BaseCnsEntityMetadata
			allEntityMetadata := volume.Metadata.EntityMetadata
			for _, metadata := range allEntityMetadata {
				if metadata.(*cnstypes.CnsKubernetesEntityMetadata).ClusterID ==
					clusterIDforVolumeMetadata {
					cnsMetadata = append(cnsMetadata, metadata)
				}
			}
			volumeToCnsEntityMetadataMap[volume.VolumeId.Id] = cnsMetadata
			if len(volume.Metadata.ContainerClusterArray) == 1 &&
				clusterIDforVolumeMetadata == volume.Metadata.ContainerClusterArray[0].ClusterId &&
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
	migrationFeatureStateForFullSync bool, vc string) (
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
			volumeHandle, err = volumeMigrationService.GetVolumeID(ctx, migrationVolumeSpec, true)
			if err != nil {
				log.Warnf("FullSync for VC %s: Failed to get VolumeID from volumeMigrationService for spec: %v. Err: %+v",
					vc, migrationVolumeSpec, err)
				continue
			}
		} else {
			volumeHandle = pv.Spec.CSI.VolumeHandle
		}
		volumeToCnsEntityMetadata, presentInCNS := volumeToCnsEntityMetadataMap[volumeHandle]
		volumeToK8sEntityMetadata, presentInK8S := volumeToK8sEntityMetadataMap[volumeHandle]
		_, volumeClusterDistributionSet := volumeClusterDistributionMap[volumeHandle]
		if !presentInK8S {
			log.Infof("FullSync for VC %s: Skipping volume: %s with VolumeId %q. Volume is not present in the k8s",
				vc, pv.Name, volumeHandle)
			continue
		}
		if !presentInCNS {
			// PV exist in K8S but not in CNS cache, need to create
			if _, existsInCnsCreationMap := cnsCreationMap[vc][volumeHandle]; existsInCnsCreationMap {
				// Volume was present in cnsCreationMap across two full-sync cycles.
				log.Infof("FullSync for VC %s: create is required for volume: %q", vc, volumeHandle)
				operationType = "createVolume"
			} else {
				log.Infof("FullSync for VC %s: Volume with id: %q and name: %q is added "+
					"to cnsCreationMap", vc, volumeHandle, pv.Name)
				cnsCreationMap[vc][volumeHandle] = true
			}
		} else {
			// volume exist in K8S and CNS, Check if update is required.
			if isUpdateRequired(ctx, vCenterVersion, volumeToK8sEntityMetadata,
				volumeToCnsEntityMetadata, volumeClusterDistributionSet, vc) {
				log.Infof("FullSync for VC %s: update is required for volume: %q", vc, volumeHandle)
				operationType = "updateVolume"
			} else {
				log.Infof("FullSync for VC %s: update is not required for volume: %q", vc, volumeHandle)
			}
		}
		switch operationType {
		case "createVolume":
			var volumeType string
			if IsMultiAttachAllowed(pv) {
				// We should never reach here in case of multi VC deployment as file share volumes are already filtered out.
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
			log.Debugf("FullSync for VC %s: Volume with id %q added to volume update list", vc, volumeHandle)
			var volumeType string
			if IsMultiAttachAllowed(pv) {
				// We should never reach here in case of multi VC deployment as file share volumes are already filtered out.
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
						log.Debugf("FullSync for VC %s: updateSpec %+v is added to updateSpecArray\n", vc, spew.Sdump(updateSpecNew))
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
	metadataSyncer *metadataSyncInformer, migrationFeatureStateForFullSync bool,
	vc string) ([]cnstypes.CnsVolumeId, error) {
	log := logger.GetLogger(ctx)
	var volToBeDeleted []cnstypes.CnsVolumeId
	// inlineVolumeMap holds the volume path information for migrated volumes
	// which are used by Pods.
	inlineVolumeMap := make(map[string]string)
	var err error
	if migrationFeatureStateForFullSync {
		inlineVolumeMap, err = fullSyncGetInlineMigratedVolumesInfo(ctx, metadataSyncer, migrationFeatureStateForFullSync)
		if err != nil {
			log.Errorf("FullSync for VC %s: Failed to get inline migrated volumes. Err: %v", vc, err)
			return volToBeDeleted, err
		}
	}
	for _, vol := range cnsVolumeList {
		if _, existsInK8s := k8sPVMap[vol.VolumeId.Id]; !existsInK8s {
			if _, existsInCnsDeletionMap := cnsDeletionMap[vc][vol.VolumeId.Id]; existsInCnsDeletionMap {
				// Volume does not exist in K8s across two fullsync cycles, because
				// it was present in cnsDeletionMap across two full sync cycles.
				// Add it to delete list.
				log.Debugf("FullSync for VC %s: Volume with id %s added to delete list", vc, vol.VolumeId.Id)
				volToBeDeleted = append(volToBeDeleted, vol.VolumeId)
			} else {
				// Add to cnsDeletionMap.
				if migrationFeatureStateForFullSync {
					// If migration is ON, verify if the volume is present in inlineVolumeMap.
					if _, existsInInlineVolumeMap := inlineVolumeMap[vol.VolumeId.Id]; !existsInInlineVolumeMap {
						log.Infof("FullSync for VC %s: Volume with id %q added to cnsDeletionMap", vc, vol.VolumeId.Id)
						cnsDeletionMap[vc][vol.VolumeId.Id] = true
					} else {
						log.Debugf("FullSync for VC %s: Inline migrated volume with id %s is in use. Skipping for deletion",
							vc, vol.VolumeId.Id)
					}
				} else {
					log.Debugf("FullSync for VC %s: Volume with id %s added to cnsDeletionMap", vc, vol.VolumeId.Id)
					cnsDeletionMap[vc][vol.VolumeId.Id] = true
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
	metadataSyncer *metadataSyncInformer, vc string) (pvcMap, podMap, error) {
	log := logger.GetLogger(ctx)
	pvToPVCMap := make(pvcMap)
	pvcToPodMap := make(podMap)
	pods, err := metadataSyncer.podLister.Pods(v1.NamespaceAll).List(labels.Everything())
	if err != nil {
		log.Warnf("FullSync for VC %s: Failed to get pods in all namespaces. err=%v", vc, err)
		return nil, nil, err
	}
	for _, pv := range pvList {
		if pv.Spec.ClaimRef != nil && pv.Status.Phase == v1.VolumeBound {
			pvc, err := metadataSyncer.pvcLister.PersistentVolumeClaims(
				pv.Spec.ClaimRef.Namespace).Get(pv.Spec.ClaimRef.Name)
			if err != nil {
				log.Warnf("FullSync for VC %s: Failed to get pvc for namespace %s and name %s. err=%v",
					pv.Spec.ClaimRef.Namespace, pv.Spec.ClaimRef.Name, vc, err)
				return nil, nil, err
			}
			pvToPVCMap[pv.Name] = pvc
			log.Debugf("FullSync for VC %s: pvc %s/%s is backed by pv %s", vc, pvc.Namespace, pvc.Name, pv.Name)
			for _, pod := range pods {
				if pod.Status.Phase == v1.PodRunning && pod.Spec.Volumes != nil {
					for _, volume := range pod.Spec.Volumes {
						pvClaim := volume.VolumeSource.PersistentVolumeClaim
						if pvClaim != nil && pvClaim.ClaimName == pvc.Name && pod.Namespace == pvc.Namespace {
							key := pod.Namespace + "/" + pvClaim.ClaimName
							pvcToPodMap[key] = append(pvcToPodMap[key], pod)
							log.Debugf("FullSync for VC %s: pvc %s is mounted by pod %s/%s", vc, key, pod.Namespace, pod.Name)
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
	cnsMetadataList []cnstypes.BaseCnsEntityMetadata, volumeClusterDistributionSet bool, vc string) bool {
	log := logger.GetLogger(ctx)
	log.Debugf("FullSync for VC %s: isUpdateRequired called with k8sMetadataList: %+v \n", vc, spew.Sdump(k8sMetadataList))
	log.Debugf("FullSync for VC %s: isUpdateRequired called with cnsMetadataList: %+v \n", vc, spew.Sdump(cnsMetadataList))
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
func cleanupCnsMaps(k8sPVs map[string]string, vc string) {
	// Cleanup cnsCreationMap.
	cnsCreationMapForVc := cnsCreationMap[vc]
	for volID := range cnsCreationMapForVc {
		if _, existsInK8s := k8sPVs[volID]; !existsInK8s {
			delete(cnsCreationMap[vc], volID)
		}
	}
	// Cleanup cnsDeletionMap.
	cnsDeletionMapForVc := cnsDeletionMap[vc]
	for volID := range cnsDeletionMapForVc {
		if _, existsInK8s := k8sPVs[volID]; existsInK8s {
			// Delete volume from cnsDeletionMap which is present in kubernetes.
			delete(cnsDeletionMap[vc], volID)
		}
	}
}
