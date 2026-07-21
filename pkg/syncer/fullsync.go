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
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"strings"
	"sync"
	"time"

	"github.com/container-storage-interface/spec/lib/go/csi"
	"github.com/davecgh/go-spew/spew"
	snapv1 "github.com/kubernetes-csi/external-snapshotter/client/v8/apis/volumesnapshot/v1"
	versioned "github.com/kubernetes-csi/external-snapshotter/client/v8/clientset/versioned"
	"github.com/vmware/govmomi/cns"
	cnstypes "github.com/vmware/govmomi/cns/types"
	"google.golang.org/grpc/codes"
	v1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/strategicpatch"
	clientset "k8s.io/client-go/kubernetes"
	ccV1beta2 "sigs.k8s.io/cluster-api/api/core/v1beta2"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	"sigs.k8s.io/vsphere-csi-driver/v3/pkg/apis/migration"
	volumes "sigs.k8s.io/vsphere-csi-driver/v3/pkg/common/cns-lib/volume"
	cnsvsphere "sigs.k8s.io/vsphere-csi-driver/v3/pkg/common/cns-lib/vsphere"
	"sigs.k8s.io/vsphere-csi-driver/v3/pkg/common/prometheus"
	"sigs.k8s.io/vsphere-csi-driver/v3/pkg/common/utils"
	"sigs.k8s.io/vsphere-csi-driver/v3/pkg/csi/service/common"
	"sigs.k8s.io/vsphere-csi-driver/v3/pkg/csi/service/common/commonco"
	commoncotypes "sigs.k8s.io/vsphere-csi-driver/v3/pkg/csi/service/common/commonco/types"
	"sigs.k8s.io/vsphere-csi-driver/v3/pkg/csi/service/logger"
	csitypes "sigs.k8s.io/vsphere-csi-driver/v3/pkg/csi/types"
	cnsvolumeinfov1alpha1 "sigs.k8s.io/vsphere-csi-driver/v3/pkg/internalapis/cnsvolumeinfo/v1alpha1"
	k8s "sigs.k8s.io/vsphere-csi-driver/v3/pkg/kubernetes"
	cnsoperatortypes "sigs.k8s.io/vsphere-csi-driver/v3/pkg/syncer/cnsoperator/types"
)

const (
	allowedRetriesToPatchCNSVolumeInfo    = 5
	annCSIvSphereVolumeAccessibleTopology = "csi.vsphere.volume-accessible-topology"

	// querySelectionNameTypeVolumeMetadata is the CNS query selection name for
	// volume metadata. The vendored cnstypes package does not define this
	// constant, so it is declared here.
	querySelectionNameTypeVolumeMetadata = cnstypes.QuerySelectionNameType("VOLUME_METADATA")
)

// queryVolumeByIDFn is the function used to query a CNS volume by its ID.
// It is a variable so that unit tests can substitute a fake implementation
// without relying on gomonkey (which is unreliable on arm64).
var queryVolumeByIDFn = common.QueryVolumeByID

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
		if len(metadataSyncer.configInfo.Cfg.VirtualCenter) == 1 {
			migrationFeatureStateForFullSync = true
		}
	}
	// Attempt to create StoragePolicyUsage CRs.
	if metadataSyncer.clusterFlavor == cnstypes.CnsClusterFlavorWorkload {
		if IsPodVMOnStretchSupervisorFSSEnabled {
			createStoragePolicyUsageCRS(ctx, metadataSyncer)
		}
	}

	// Sync VolumeInfo CRs for the below conditions:
	// Either it is a Vanilla k8s deployment with Multi-VC configuration or, it's a StretchSupervisor cluster
	if len(metadataSyncer.configInfo.Cfg.VirtualCenter) > 1 ||
		(metadataSyncer.clusterFlavor == cnstypes.CnsClusterFlavorWorkload && IsPodVMOnStretchSupervisorFSSEnabled) {
		volumeInfoCRFullSync(ctx, metadataSyncer, vc)
		cleanUpVolumeInfoCrDeletionMap(ctx, metadataSyncer, vc)
	}
	// Attempt to patch StoragePolicyUsage CRs. For storagePolicyUsageCRSync to work,
	// we need CNSVolumeInfo CRs to be present for all existing volumes.
	if metadataSyncer.clusterFlavor == cnstypes.CnsClusterFlavorWorkload {
		if IsPodVMOnStretchSupervisorFSSEnabled {
			storagePolicyUsageCRSync(ctx, metadataSyncer)
		}
	}

	// On Supervisor cluster, if SVPVCSnapshotProtectionFinalizer FSS is enabled,
	// Iterate over all PVCs & VolumeSnapshots and find ones with DeletionTimestamp set but
	// CNS specific finalizer present.
	// For all such PVCs/Snapshots, attempt to remove CNS finalizer if corresponding guest cluster does not exist.
	// This code handles cases where namespace deletion causes guest cluster and its corresponding components
	// to be deleted but associated objects on supervisor remain stuck in Terminating state.
	if metadataSyncer.clusterFlavor == cnstypes.CnsClusterFlavorWorkload {
		if metadataSyncer.coCommonInterface.IsFSSEnabled(ctx, common.SVPVCSnapshotProtectionFinalizer) {
			cleanupUnusedPVCsAndSnapshotsFromGuestCluster(ctx)
		}
	}

	// On Supervisor cluster, if CSI_Backup_API FSS is enabled, reconcile snapshot change-id annotations
	// from CNS snapshot metadata. This ensures all supervisor snapshots have the change-id annotation
	// which can then be mirrored to guest cluster snapshots.
	if metadataSyncer.clusterFlavor == cnstypes.CnsClusterFlavorWorkload {
		if metadataSyncer.coCommonInterface.IsFSSEnabled(ctx, common.CSI_Backup_API) {
			setChangeIDAnnotationOnSupervisorSnapshots(ctx, metadataSyncer, vc)
		}
	}

	// On Supervisor cluster, if ImprovedVolumeVisibility FSS is enabled,
	// classify every PVC according to the consumer workload-type signals
	// (VKS Node VM ownerRef, VKS guest cluster TKGService label, or none)
	// and reconcile the matching csi.vsphere.volume.type/* annotations. The
	// annotations are advisory metadata for VI admins, operators, and other
	// tooling that needs to distinguish VKS-attributable PVCs from purely
	// supervisor-side ones; they have no functional effect on CSI provisioning.
	if metadataSyncer.clusterFlavor == cnstypes.CnsClusterFlavorWorkload {
		if metadataSyncer.coCommonInterface.IsFSSEnabled(ctx, common.ImprovedVolumeVisibility) {
			annotateSupervisorPVCsWithWorkloadType(ctx, metadataSyncer)
		}
	}

	// On Supervisor cluster, reconcile keepAfterDeleteVm flags for PVCs with VM
	// ownerRefs that may have been missed by pvcAdded (upgrade, informer gaps,
	// transient failures). The annotation check ensures idempotency.
	if metadataSyncer.clusterFlavor == cnstypes.CnsClusterFlavorWorkload {
		reconcileKeepAfterDeleteVmFlags(ctx, metadataSyncer)
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

	// Filter out PVs provisioned by the new vSAN FileVolumeService (FVS) on the supervisor.
	// CNS is not the source of truth for these volumes and CNS QueryVolume will not return
	// them, so we must skip pushing/creating/updating any volume metadata for them.
	// See pkg/csi/service/wcp/fvs_filevolume.go for the provisioning workflow.
	if metadataSyncer.clusterFlavor == cnstypes.CnsClusterFlavorWorkload && IsVsanFileVolumeServiceEnabled {
		filteredPVs := make([]*v1.PersistentVolume, 0, len(k8sPVs))
		skipped := 0
		for _, pv := range k8sPVs {
			if isFVSPersistentVolume(pv) {
				log.Debugf("FullSync for VC %s: skipping FVS-backed PV %q (volumeHandle=%q) for CNS metadata sync",
					vc, pv.Name, pv.Spec.CSI.VolumeHandle)
				skipped++
				continue
			}
			filteredPVs = append(filteredPVs, pv)
		}
		if skipped > 0 {
			log.Infof("FullSync for VC %s: filtered out %d FVS-backed PV(s) from CNS metadata reconciliation",
				vc, skipped)
		}
		k8sPVs = filteredPVs
	}

	// k8sPVMap is useful for clean and quicker look up.
	k8sPVMap := make(map[string]string)
	// Instantiate volumeMigrationService when migration feature state is True.
	if migrationFeatureStateForFullSync {
		// Instantiate volumeMigrationService when migration feature state is True.
		if err = initVolumeMigrationService(ctx, metadataSyncer); err != nil {
			log.Errorf("FullSync for VC %s: Failed to initialize migration service. Err: %v", vc, err)
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
			// Note that we can never reach here in case of a multi VC setup
			// as we have already filtered out in-tree volumes.
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

	volManager, err := getVolManagerForVcHost(ctx, vc, metadataSyncer)
	if err != nil {
		log.Errorf("FullSync for VC %s: Failed to get volume manager. Err: %v", vc, err)
		return err
	}

	var vcenter *cnsvsphere.VirtualCenter
	// Get VC instance.
	vcenter, err = cnsvsphere.GetVirtualCenterInstanceForVCenterHost(ctx, vc, true)
	if err != nil {
		log.Errorf("failed to get virtual center instance for VC: %s. Error: %v", vc, err)
		return err
	}

	// Ensure a ClusterStoragePolicyInfo CR exists for every storage policy on the VC, even if no
	// StorageClass/VolumeAttributesClass references it yet. Reuses the VC instance fetched above.
	if metadataSyncer.clusterFlavor == cnstypes.CnsClusterFlavorWorkload {
		clusterStoragePolicyInfoFullSync(ctx, metadataSyncer, vc, vcenter)
	}

	// Iterate through all the k8sPVs to find all PVs with node affinity missing and
	// patch such PVs and their corresponding PVCs with topology discovered
	if metadataSyncer.clusterFlavor == cnstypes.CnsClusterFlavorWorkload &&
		IsWorkloadDomainIsolationSupported {
		k8sClient, err := k8s.NewClient(ctx)
		if err != nil {
			log.Errorf("FullSync for VC %s: Failed to create kubernetes client. Err: %+v", vc, err)
			return err
		}
		var pvWithMissingNodeAffinityList [](*v1.PersistentVolume)
		for _, pv := range k8sPVs {
			if pv.Spec.NodeAffinity == nil {
				pvWithMissingNodeAffinityList = append(pvWithMissingNodeAffinityList, pv)
			}
		}
		// For PVs missing node affinity info, discover and patch the topology information as node affinity.
		// Also add "csi.vsphere.volume-accessible-topology" annotation to associated PVC(s)
		if len(pvWithMissingNodeAffinityList) != 0 {
			patchNodeAffinityToPVAndPVC(ctx, k8sClient, metadataSyncer, vcenter,
				pvWithMissingNodeAffinityList, pvToPVCMap)
		}
		// Add "csi.vsphere.volume-accessible-topology" annotation to all PVCs,
		// if missed to get patched in patchNodeAffinityToPVAndPVC()
		for _, pvc := range pvToPVCMap {
			if pvc.ObjectMeta.Annotations[annCSIvSphereVolumeAccessibleTopology] == "" {
				err = setVolumeAccessibleTopologyForPVC(ctx, k8sClient, metadataSyncer, vcenter,
					pvc.Namespace, pvc)
				if err != nil {
					log.Errorf("FullSync for VC %s: Failed to add %s annotation for PVC %s. Err: %v",
						vc, annCSIvSphereVolumeAccessibleTopology, pvc.Name, err)
					continue
				}
			}
		}
	}

	// Iterate over all the file volume PVCs and check if file share export paths are added as annotations
	// on it. If not added, then add file share export path annotations on such PVCs.
	if metadataSyncer.clusterFlavor == cnstypes.CnsClusterFlavorWorkload {
		k8sClient, err := k8sNewClient(ctx)
		if err != nil {
			log.Errorf("FullSync for VC %s: Failed to create kubernetes client. Err: %+v", vc, err)
			return err
		}
		for _, pv := range k8sPVs {
			if IsFileVolume(pv) {
				if pvc, ok := pvToPVCMap[pv.Name]; ok {
					// Check if file share export path is available as annotation on PVC
					if pvc.ObjectMeta.Annotations[common.Nfsv3ExportPathAnnotationKey] == "" ||
						pvc.ObjectMeta.Annotations[common.Nfsv4ExportPathAnnotationKey] == "" {
						err = setFileShareAnnotationsOnPVC(ctx, k8sClient, metadataSyncer.volumeManager, pvc)
						if err != nil {
							log.Errorf("FullSync for VC %s: Failed to add file share export path annotations "+
								"for PVC %s, Err: %v. Continuing for other PVCs.", vc, pvc.Name, err)
							continue
						}
					}
				}
			}
		}
	}

	var queryAllResult *cnstypes.CnsQueryResult
	if metadataSyncer.configInfo.Cfg.Global.ClusterID != "" {
		// Cluster ID is removed from vSphere Config Secret post 9.0 release in Supervisor
		queryAllResult, err = utils.QueryAllVolumesForCluster(ctx, volManager,
			metadataSyncer.configInfo.Cfg.Global.ClusterID, cnstypes.CnsQuerySelection{})
		if err != nil {
			log.Errorf("FullSync for VC %s: QueryVolume failed with err=%+v", vc, err.Error())
			return err
		}
	} else {
		log.Infof("observed emptry string cluster-id in the vSphere Config secret. " +
			"Skipping to replace volume metadata with older cluster-id to new supervisor-id")
	}
	if metadataSyncer.clusterFlavor == cnstypes.CnsClusterFlavorWorkload &&
		commonco.ContainerOrchestratorUtility.IsFSSEnabled(ctx, common.TKGsHA) {
		// Replace the Cluster-ID in the volume metadata with the new Supervisor-ID.
		//
		// Note:
		// - In vSphere 9.1 and later, this replacement is not required because volume metadata already contains
		//   the correct Supervisor-ID.
		// - In vSphere 9.0, the Cluster ID field in the metadata stores the Supervisor-ID. volume metadata created
		//   before 9.0 has already been updated to use the new Supervisor-ID.

		var volumeIDsWithOldClusterID []cnstypes.CnsVolumeId
		if queryAllResult != nil && len(queryAllResult.Volumes) > 0 {
			for _, volume := range queryAllResult.Volumes {
				volumeIDsWithOldClusterID = append(volumeIDsWithOldClusterID, volume.VolumeId)
			}
			queryAllResult, err := fullSyncGetQueryResults(ctx, volumeIDsWithOldClusterID,
				metadataSyncer.configInfo.Cfg.Global.ClusterID, volManager, metadataSyncer, nil)
			if err != nil {
				log.Errorf("FullSync for VC %s: fullSyncGetQueryResults failed to query volume metadata from vc. Err: %v", vc, err)
				return err
			}
			var updateMetadataSpecArray []cnstypes.CnsVolumeMetadataUpdateSpec
			for _, queryResult := range queryAllResult {
				for _, volume := range queryResult.Volumes {
					log.Infof("observed volume %q with old cluster Id: %q", volume.VolumeId,
						metadataSyncer.configInfo.Cfg.Global.ClusterID)
					var containerClusterArrayToAdd []cnstypes.CnsContainerCluster
					var containerClusterArrayToDelete []cnstypes.CnsContainerCluster
					var updatedContainerCluster cnstypes.CnsContainerCluster
					for _, containerCluster := range volume.Metadata.ContainerClusterArray {
						if containerCluster.ClusterId == metadataSyncer.configInfo.Cfg.Global.ClusterID {
							updatedContainerCluster = containerCluster
							updatedContainerCluster.ClusterId = metadataSyncer.configInfo.Cfg.Global.SupervisorID
							containerClusterArrayToAdd = append(containerClusterArrayToAdd, updatedContainerCluster)
							containerCluster.Delete = true
							containerClusterArrayToDelete = append(containerClusterArrayToDelete, containerCluster)
						}
					}
					updateSpecToDeleteMetadata := cnstypes.CnsVolumeMetadataUpdateSpec{
						VolumeId: cnstypes.CnsVolumeId{
							Id: volume.VolumeId.Id,
						},
						Metadata: cnstypes.CnsVolumeMetadata{
							ContainerCluster:      volume.Metadata.ContainerCluster,
							ContainerClusterArray: containerClusterArrayToDelete,
						},
					}
					updateSpecToDeleteMetadata.Metadata.ContainerCluster.Delete = true
					updateSpecToAddMetadata := cnstypes.CnsVolumeMetadataUpdateSpec{
						VolumeId: cnstypes.CnsVolumeId{
							Id: volume.VolumeId.Id,
						},
						Metadata: cnstypes.CnsVolumeMetadata{
							ContainerCluster:      updatedContainerCluster,
							ContainerClusterArray: containerClusterArrayToAdd,
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
					updateMetadataSpecArray = append(updateMetadataSpecArray, updateSpecToAddMetadata)
					updateMetadataSpecArray = append(updateMetadataSpecArray, updateSpecToDeleteMetadata)
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
				// Reset the array for the next queryResult iteration to prevent duplicate operations
				updateMetadataSpecArray = updateMetadataSpecArray[:0]
			}
		}
		querySelection := cnstypes.CnsQuerySelection{
			Names: []string{
				string(cnstypes.QuerySelectionNameTypeVolumeType),
				string(cnstypes.QuerySelectionNameTypeBackingObjectDetails),
				string(cnstypes.QuerySelectionNameTypeVolumeName),
				//string("VOLUME_METADATA"),
			},
		}
		// get queryAllResult using new Supervisor ID for rest of full sync operations
		queryAllResult, err = utils.QueryAllVolumesForCluster(ctx, volManager,
			metadataSyncer.configInfo.Cfg.Global.SupervisorID, querySelection)
		if err != nil {
			log.Errorf("FullSync for VC %s: QueryVolume failed with err=%+v", vc, err.Error())
			return err
		}
	}
	if metadataSyncer.clusterFlavor == cnstypes.CnsClusterFlavorWorkload && isStorageQuotaM2FSSEnabled {
		cnsBlockVolumeMap := make(map[string]cnstypes.CnsVolume)
		for _, vol := range queryAllResult.Volumes {
			// We do not support file volume snapshot, filtering out block volume only.
			// cnsvolumeinfo snapshot details will be validated for volumes in cnsBlockVolumeMap
			if vol.VolumeType == common.BlockVolumeType {
				cnsBlockVolumeMap[vol.VolumeId.Id] = vol
			}
		}
		log.Infof("calling validateAndCorrectVolumeInfoSnapshotDetails with %d volumes",
			len(cnsBlockVolumeMap))
		validateAndCorrectVolumeInfoSnapshotDetails(ctx, cnsBlockVolumeMap)
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

	containerCluster := cnsvsphere.GetContainerCluster(clusterIDforVolumeMetadata,
		vcHostObj.User, metadataSyncer.clusterFlavor,
		metadataSyncer.configInfo.Cfg.Global.ClusterDistribution)
	createSpecArray, updateSpecArray := fullSyncGetVolumeSpecs(ctx, vcenter.Client.Version, k8sPVs,
		volumeToCnsEntityMetadataMap, volumeToK8sEntityMetadataMap, volumeClusterDistributionMap,
		containerCluster, migrationFeatureStateForFullSync, vc)

	// Re-query the same volume set with VOLUME_METADATA included so that
	// getMissingPVVolumeUpdateSpecs and getRetainedPVVolumeUpdateSpecs can
	// read vol.Metadata.EntityMetadata. The main queryAllResult intentionally
	// omits VOLUME_METADATA to keep the payload small; this secondary query
	// fetches only what the labeling path needs.
	var volumeIDsForLabelQuery []cnstypes.CnsVolumeId
	for _, vol := range queryAllResult.Volumes {
		volumeIDsForLabelQuery = append(volumeIDsForLabelQuery, vol.VolumeId)
	}
	labelQuerySelection := &cnstypes.CnsQuerySelection{
		Names: []string{
			string(cnstypes.QuerySelectionNameTypeVolumeType),
			string(cnstypes.QuerySelectionNameTypeBackingObjectDetails),
			string(cnstypes.QuerySelectionNameTypeVolumeName),
			string(querySelectionNameTypeVolumeMetadata),
		},
	}
	labelQueryResults, err := fullSyncGetQueryResults(ctx, volumeIDsForLabelQuery,
		clusterIDforVolumeMetadata, volManager, metadataSyncer, labelQuerySelection)
	if err != nil {
		log.Errorf("FullSync for VC %s: fullSyncGetQueryResults for label query failed with err %+v", vc, err)
		return err
	}
	var volumesWithMetadata []cnstypes.CnsVolume
	for _, qr := range labelQueryResults {
		volumesWithMetadata = append(volumesWithMetadata, qr.Volumes...)
	}

	// Per the cns-health-initiative design (docs/mermaid/cns-health-initiative/
	// 03-syncer-no-unregister.mmd), CNS volumes whose matching K8s PV cannot
	// be found are no longer unregistered (the legacy fullSyncDeleteVolumes
	// flow that issued DeleteVolume(deleteDisk=false) has been removed).
	// Instead we label them with `pv_missing=true` on their existing PV-type
	// CnsKubernetesEntityMetadata, after a two-cycle grace period to absorb
	// transient races between PV deletion and full-sync execution.
	missingPVUpdateSpecs, missingPVCount, err := getMissingPVVolumeUpdateSpecs(ctx, volumesWithMetadata,
		k8sPVMap, metadataSyncer, migrationFeatureStateForFullSync, containerCluster, vc)
	if err != nil {
		log.Errorf("FullSync for VC %s: failed to compute pv_missing update specs with err %+v", vc, err)
		return err
	}
	prometheus.CnsVolumePVMissingGaugeVec.WithLabelValues(vc).Set(float64(missingPVCount))
	if len(missingPVUpdateSpecs) > 0 {
		log.Infof("FullSync for VC %s: applying pv_missing label to %d volume(s)",
			vc, len(missingPVUpdateSpecs))
		updateSpecArray = append(updateSpecArray, missingPVUpdateSpecs...)
	}

	// On Supervisor clusters, label CNS volumes whose Kubernetes PV has
	// ReclaimPolicy=Retain and is in Released or Available phase (no active
	// PVC binding, PodVM, or VMService VM consumer). These volumes are safe
	// to mark as pv_retained=true for VI Admin visibility.
	if metadataSyncer.clusterFlavor == cnstypes.CnsClusterFlavorWorkload {
		retainedPVUpdateSpecs, retainedPVCount, err := getRetainedPVVolumeUpdateSpecs(ctx,
			volumesWithMetadata, k8sPVs, containerCluster, vc)
		if err != nil {
			log.Errorf("FullSync for VC %s: failed to compute pv_retained update specs with err %+v", vc, err)
			return err
		}
		prometheus.CnsVolumePVRetainedGaugeVec.WithLabelValues(vc).Set(float64(retainedPVCount))
		if len(retainedPVUpdateSpecs) > 0 {
			log.Infof("FullSync for VC %s: applying pv_retained label to %d volume(s)",
				vc, len(retainedPVUpdateSpecs))
			updateSpecArray = append(updateSpecArray, retainedPVUpdateSpecs...)
		}
	}

	wg := sync.WaitGroup{}
	wg.Add(2)
	// Perform operations.
	go fullSyncCreateVolumes(ctx, createSpecArray, metadataSyncer, &wg, migrationFeatureStateForFullSync, volManager, vc)
	go fullSyncUpdateVolumes(ctx, updateSpecArray, metadataSyncer, &wg, volManager, vc)
	wg.Wait()

	cleanupCnsMaps(k8sPVMap, vc)
	log.Debugf("FullSync for VC %s: cnsDeletionMap at end of cycle: %v", vc, cnsDeletionMap)
	log.Debugf("FullSync for VC %s: cnsCreationMap at end of cycle: %v", vc, cnsCreationMap)
	log.Infof("FullSync for VC %s: end", vc)
	return nil
}

// getPVNodeAffinity finds topology associated with given PV and returns the same
func getPVNodeAffinity(ctx context.Context, metadataSyncer *metadataSyncInformer,
	vc *cnsvsphere.VirtualCenter, pv *v1.PersistentVolume) ([]*csi.Topology, error) {
	log := logger.GetLogger(ctx)
	var pvTopology []*csi.Topology
	var singleZoneTopologyToadd string

	// Check availability zones present in supervisor to determine the node
	// affinity information to be patched on pv
	azClusterMap := volumeTopologyService.GetAZClustersMap(ctx)
	if len(azClusterMap) == 1 {
		// In case of only single zone present, return same zone as PV topology
		for zoneName := range azClusterMap {
			singleZoneTopologyToadd = zoneName
			break
		}
		log.Infof("getPVNodeAffinity: Found single zone supervisor cluster with zone %+v",
			singleZoneTopologyToadd)
		pvTopology = append(pvTopology, &csi.Topology{
			Segments: map[string]string{
				v1.LabelTopologyZone: singleZoneTopologyToadd,
			},
		})
	} else {
		// Find zones associated with datastore on which volume is created
		volManager, err := getVolManagerForVcHost(ctx, vc.Config.Host, metadataSyncer)
		if err != nil {
			log.Errorf("getPVNodeAffinity: Failed to get volume manager for VC %s. Err: %v", vc, err)
			return nil, err
		}
		queryFilter := cnstypes.CnsQueryFilter{
			VolumeIds: []cnstypes.CnsVolumeId{
				{
					Id: pv.Spec.CSI.VolumeHandle,
				},
			},
		}
		querySelection := cnstypes.CnsQuerySelection{
			Names: []string{string(cnstypes.QuerySelectionNameTypeDataStoreUrl)},
		}
		queryResult, err := utils.QueryVolumeUtil(ctx, volManager, queryFilter, &querySelection)
		if err != nil || queryResult == nil || len(queryResult.Volumes) != 1 {
			return nil, logger.LogNewErrorCodef(log, codes.Internal,
				"failed to find the datastore on which volume %q is provisioned. "+
					"Error: %+v", pv.Spec.CSI.VolumeHandle, err)
		}
		selectedDatastore := queryResult.Volumes[0].DatastoreUrl
		// Calculate accessible topology for the provisioned volume.
		topoSegToDatastoresMap := make(map[string][]*cnsvsphere.DatastoreInfo)
		datastoreAccessibleTopology, err := volumeTopologyService.GetTopologyInfoFromNodes(ctx,
			commoncotypes.WCPRetrieveTopologyInfoParams{
				DatastoreURL:           selectedDatastore,
				StorageTopologyType:    "",
				TopologyRequirement:    nil,
				Vc:                     vc,
				TopoSegToDatastoresMap: topoSegToDatastoresMap})
		if err != nil {
			return nil, logger.LogNewErrorCodef(log, codes.Internal,
				"failed to get topology of datastore on which volume %q is provisioned."+
					" Error: %+v", pv.Spec.CSI.VolumeHandle, err)
		}
		// Add topology segments to output.
		for _, topoSegments := range datastoreAccessibleTopology {
			volumeTopology := &csi.Topology{
				Segments: topoSegments,
			}
			pvTopology = append(pvTopology, volumeTopology)
		}
		log.Infof("getPVNodeAffinity: Found multi zone supervisor cluster with datastore attached to "+
			"zones %+v", pvTopology)
	}
	return pvTopology, nil
}

// generateVolumeAccessibleTopologyJSON returns JSON string from AccessibleTopology given as input.
// This value will be set on the PVC for annotation - "csi.vsphere.volume-accessible-topology"
func generateVolumeAccessibleTopologyJSON(topologies []*csi.Topology) (string, error) {
	segmentsArray := make([]string, 0)
	for _, topology := range topologies {
		jsonSegment, err := json.Marshal(topology.Segments)
		if err != nil {
			return "", fmt.Errorf("failed to marshal topology segment: %v to json. Err: %v",
				topology.Segments, err)
		}
		segmentsArray = append(segmentsArray, string(jsonSegment))
	}
	return "[" + strings.Join(segmentsArray, ",") + "]", nil
}

// patchNodeAffinityToPVAndPVC finds the topology associated with PV and patches that info as
// node affinity to PV objects
// This also adds "csi.vsphere.volume-accessible-topology" annotation to associated PVC, if any
func patchNodeAffinityToPVAndPVC(ctx context.Context, k8sClient clientset.Interface,
	metadataSyncer *metadataSyncInformer,
	vc *cnsvsphere.VirtualCenter, pvWithoutNodeAffinity []*v1.PersistentVolume,
	pvToPVCMap map[string]*v1.PersistentVolumeClaim) {
	log := logger.GetLogger(ctx)
	// Iterate over all PVs missing node affinity info, discover the topology and patch to both PV & PVC
	for _, pv := range pvWithoutNodeAffinity {
		log.Infof("patchNodeAffinityToPVAndPVC: Setting node affinity for pv: %q", pv.Name)
		oldData, err := json.Marshal(pv)
		if err != nil {
			log.Errorf("patchNodeAffinityToPVAndPVC: Failed to marshal pv: %v, Error: %v", pv, err)
			continue
		}
		pvCSITopology, err := getPVNodeAffinity(ctx, metadataSyncer, vc, pv)
		if err != nil {
			log.Errorf("patchNodeAffinityToPVAndPVC: Unable to get node affinity for PV %q. Error: %+v",
				pv.Name, err)
			continue
		}
		newPV := pv.DeepCopy()
		newPV.Spec.NodeAffinity = GenerateVolumeNodeAffinity(pvCSITopology)
		newData, err := json.Marshal(newPV)
		if err != nil {
			log.Errorf("patchNodeAffinityToPVAndPVC: Failed to marshal updated PV with "+
				"node affinity rules: %v, Error: %v", newPV, err)
			continue
		}
		patchBytes, err := strategicpatch.CreateTwoWayMergePatch(oldData, newData, pv)
		if err != nil {
			log.Errorf("patchNodeAffinityToPVAndPVC: Error creating two way merge patch for PV %q"+
				" with error : %v", pv.Name, err)
			continue
		}
		// Patch node affinity to PV
		_, err = k8sClient.CoreV1().PersistentVolumes().Patch(ctx, pv.Name, types.StrategicMergePatchType,
			patchBytes, metav1.PatchOptions{})
		if err != nil {
			log.Errorf("patchNodeAffinityToPVAndPVC: Failed to patch the PV %q", pv.Name)
			continue
		}
		log.Infof("patchNodeAffinityToPVAndPVC: Updated PV %s with node affinity details successfully "+
			"for volume %q", pv.Name, pv.Spec.CSI.VolumeHandle)
		// Add "csi.vsphere.volume-accessible-topology" annotation to associated PVC
		if pvc, ok := pvToPVCMap[pv.Name]; ok {
			log.Infof("patchNodeAffinityToPVAndPVC: Setting claim annotation: %q for pvc: %q, namespace: %q",
				annCSIvSphereVolumeAccessibleTopology, pvc.Name, pvc.Namespace)
			err = patchVolumeAccessibleTopologyToPVC(ctx, k8sClient, pvc, pvCSITopology)
			if err != nil {
				log.Warnf("patchNodeAffinityToPVAndPVC: Failed to generate VolumeAccessibleTopology Json."+
					" Err: %v", err)
				continue
			}
		}
	}
}

// setVolumeAccessibleTopologyForPVC sets the "csi.vsphere.volume-accessible-topology" annotation on the PVC
// for DevOps user to know which zone volume is provisioned in
func setVolumeAccessibleTopologyForPVC(ctx context.Context, k8sClient clientset.Interface,
	metadataSyncer *metadataSyncInformer, vc *cnsvsphere.VirtualCenter, namespace string,
	pvc *v1.PersistentVolumeClaim) error {
	log := logger.GetLogger(ctx)
	log.Infof("setVolumeAccessibleTopologyForPVC: Setting claim annotation: %q for pvc: %q, namespace: %q",
		annCSIvSphereVolumeAccessibleTopology, pvc.Name, namespace)
	if pvc.ObjectMeta.Annotations[annCSIvSphereVolumeAccessibleTopology] == "" {
		pv, err := k8sClient.CoreV1().PersistentVolumes().Get(ctx, pvc.Spec.VolumeName, metav1.GetOptions{})
		if err != nil {
			log.Warnf("setVolumeAccessibleTopologyForPVC: Failed to get PV: %q to fetch "+
				"node topology %q annotation. Err: %v.",
				pvc.Spec.VolumeName, annCSIvSphereVolumeAccessibleTopology, err)
			return err
		}
		if pv.Spec.NodeAffinity != nil {
			pvCSITopology, err := getPVNodeAffinity(ctx, metadataSyncer, vc, pv)
			if err != nil {
				log.Errorf("setVolumeAccessibleTopologyForPVC: Unable to get node affinity for PV %q. "+
					"Error: %+v", pv.Name, err)
				return err
			}
			err = patchVolumeAccessibleTopologyToPVC(ctx, k8sClient, pvc, pvCSITopology)
			if err != nil {
				log.Warnf("setVolumeAccessibleTopologyForPVC: Failed to generate VolumeAccessibleTopology Json."+
					" Err: %v", err)
				return err
			}
		}
	}
	return nil
}

// patchVolumeAccessibleTopologyToPVC sets the "csi.vsphere.volume-accessible-topology" annotation on the PVC
// for DevOps user to know which zone volume is provisioned in
func patchVolumeAccessibleTopologyToPVC(ctx context.Context, k8sClient clientset.Interface,
	pvc *v1.PersistentVolumeClaim, accessibleTopology []*csi.Topology) error {
	log := logger.GetLogger(ctx)
	annCSIvSphereVolumeAccessibleTopologyValue, err := generateVolumeAccessibleTopologyJSON(accessibleTopology)
	if err != nil {
		log.Warnf("patchVolumeAccessibleTopologyToPVC: Failed to generate VolumeAccessibleTopology Json. "+
			"Err: %v", err)
		return err
	}
	oldData, err := json.Marshal(pvc)
	if err != nil {
		log.Errorf("patchVolumeAccessibleTopologyToPVC: Failed to marshal pvc: %v, Error: %v", pvc, err)
		return err
	}
	newPVC := pvc.DeepCopy()
	newPVC.Annotations[annCSIvSphereVolumeAccessibleTopology] = annCSIvSphereVolumeAccessibleTopologyValue
	newData, err := json.Marshal(newPVC)
	if err != nil {
		log.Errorf("patchVolumeAccessibleTopologyToPVC: Failed to marshal updated PV with "+
			"node affinity rules: %v, Error: %v", newPVC, err)
		return err
	}
	patchBytes, err := strategicpatch.CreateTwoWayMergePatch(oldData, newData, pvc)
	if err != nil {
		log.Errorf("patchVolumeAccessibleTopologyToPVC: Error creating two way merge patch for PV %q"+
			" with error : %v", pvc.Name, err)
		return err
	}
	pvc, err = k8sClient.CoreV1().PersistentVolumeClaims(pvc.Namespace).Patch(ctx, pvc.Name,
		types.StrategicMergePatchType, patchBytes, metav1.PatchOptions{})
	if err != nil {
		log.Errorf("patchVolumeAccessibleTopologyToPVC: Failed to update PVC %q with %q annotation. Err: %v",
			pvc.Name, annCSIvSphereVolumeAccessibleTopology, err)
		return err

	}
	log.Infof("patchVolumeAccessibleTopologyToPVC: Added annotation %q successfully to PVC %q",
		annCSIvSphereVolumeAccessibleTopology, pvc.Name)
	return nil
}

// setFileShareAnnotationsOnPVC sets file share export paths as annotation on file volume PVC if it
// is not already added. In case of upgrade from old version to VC 9.1 compatible CSI driver, this
// will add these annotations on all existing file volume PVCs.
func setFileShareAnnotationsOnPVC(ctx context.Context, k8sClient clientset.Interface,
	volumeManager volumes.Manager, pvc *v1.PersistentVolumeClaim) error {
	log := logger.GetLogger(ctx)
	log.Infof("setFileShareAnnotationsOnPVC: Setting file share annotation for PVC: %q, namespace: %q",
		pvc.Name, pvc.Namespace)

	pv, err := k8sClient.CoreV1().PersistentVolumes().Get(ctx, pvc.Spec.VolumeName, metav1.GetOptions{})
	if err != nil {
		log.Errorf("setFileShareAnnotationsOnPVC: failed to get PV for PVC: %q, namespace: %q",
			pvc.Name, pvc.Namespace)
		return err
	}

	// Make a QueryVolume call to fetch file share export paths.
	querySelection := cnstypes.CnsQuerySelection{
		Names: []string{
			string(cnstypes.QuerySelectionNameTypeBackingObjectDetails),
		},
	}
	volume, err := queryVolumeByIDFn(ctx, volumeManager, pv.Spec.CSI.VolumeHandle, &querySelection)
	if err != nil {
		log.Errorf("setFileShareAnnotationsOnPVC: Error while performing QueryVolume on volume %s, Err: %+v",
			pv.Spec.CSI.VolumeHandle, err)
		return err
	}
	vSANFileBackingDetails := volume.BackingObjectDetails.(*cnstypes.CnsVsanFileShareBackingDetails)
	accessPoints := make(map[string]string)
	for _, kv := range vSANFileBackingDetails.AccessPoints {
		if kv.Key == common.Nfsv3AccessPointKey {
			pvc.Annotations[common.Nfsv3ExportPathAnnotationKey] = kv.Value
		} else if kv.Key == common.Nfsv4AccessPointKey {
			pvc.Annotations[common.Nfsv4ExportPathAnnotationKey] = kv.Value
		}
		accessPoints[kv.Key] = kv.Value
	}
	log.Debugf("setFileShareAnnotationsOnPVC: Access point details for PVC: %q, namespace: %q are %+v",
		pvc.Name, pvc.Namespace, accessPoints)

	// Update PVC to add annotation on it
	pvc, err = k8sClient.CoreV1().PersistentVolumeClaims(pvc.Namespace).Update(ctx, pvc,
		metav1.UpdateOptions{})
	if err != nil {
		log.Errorf("setFileShareAnnotationsOnPVC: Error updating PVC %q in namespace %q, Err: %v",
			pvc.Name, pvc.Namespace, err)
		return err
	}
	log.Infof("setFileShareAnnotationsOnPVC: Added file share export paths annotation successfully on PVC %q, "+
		"namespce %q", pvc.Name, pvc.Namespace)
	return nil
}

// cleanUpVolumeInfoCrDeletionMap removes volumes from the VolumeInfo CR deletion map
// if the volume is present in the K8s cluster.
// This may happen if is not yet created PV by the time full sync
// starts. So the k8sPVMap has stale values. This means that the volumeInfo CR which
// may have got created during the full sync, might be added to the deletion map.
// This entry will be removed from the deletion map in the next full sync cycle.
func cleanUpVolumeInfoCrDeletionMap(ctx context.Context, metadataSyncer *metadataSyncInformer, vc string) {
	log := logger.GetLogger(ctx)
	log.Debugf("Cleaning up VolumeInfo CRs.")

	// Get all K8s PVs in the given VC.
	currentK8sPV, err := getPVsInBoundAvailableOrReleasedForVc(ctx, metadataSyncer, vc)
	if err != nil {
		log.Errorf("FullSync for VC %s: cleanUpVolumeInfoCrDeletionMap failed to get PVs from kubernetes. Err: %v", vc, err)
		return
	}

	currentK8sPVMap := make(map[string]bool)
	// Create map for easy lookup.
	for _, pv := range currentK8sPV {
		if pv.Spec.CSI != nil {
			currentK8sPVMap[pv.Spec.CSI.VolumeHandle] = true
		} else {
			log.Errorf("FullSync for VC %s: failed to find volumeHandle for volume %s", vc, pv.Name)
		}
	}

	volumeInfoCrForVc := volumeInfoCrDeletionMap[vc]
	for volID := range volumeInfoCrForVc {
		if _, pvExists := currentK8sPVMap[volID]; pvExists {
			delete(volumeInfoCrDeletionMap[vc], volID)
		}
	}
	log.Debugf("Full sync for VC %s: volumeInfoCrDeletionMap after clean up: %v ",
		vc, volumeInfoCrDeletionMap[vc])
}

// volumeInfoCRFullSync creates VolumeInfo CR if it does not already exist for a volume.
// It also deletes VolumeInfo CR if its corresponding PV does not exist.
func volumeInfoCRFullSync(ctx context.Context, metadataSyncer *metadataSyncInformer, vc string) {
	log := logger.GetLogger(ctx)

	log.Debugf("Starting volumeInfo CR full sync.")

	// Get all K8s PVs in the given VC.
	currentK8sPV, err := getPVsInBoundAvailableOrReleasedForVc(ctx, metadataSyncer, vc)
	if err != nil {
		log.Errorf("FullSync for VC %s: volumeInfoCRFullSync failed to get PVs from kubernetes. Err: %v", vc, err)
		return
	}

	currentK8sPVMap := make(map[string]bool)
	// Create map for easy lookup.
	for _, pv := range currentK8sPV {
		if pv.Spec.CSI != nil {
			currentK8sPVMap[pv.Spec.CSI.VolumeHandle] = true
		} else {
			log.Errorf("FullSync for VC %s: failed to find volumeHandle for volume %s", vc, pv.Name)
		}
	}

	volumeIdTok8sPVMap := make(map[string]*v1.PersistentVolume)
	scNameToPolicyIdMap := make(map[string]string)
	if metadataSyncer.clusterFlavor == cnstypes.CnsClusterFlavorWorkload && IsPodVMOnStretchSupervisorFSSEnabled {
		// Create volumeIdTok8sPVMap map for easy lookup of PVs
		for _, pv := range currentK8sPV {
			if pv.Spec.CSI != nil {
				volumeIdTok8sPVMap[pv.Spec.CSI.VolumeHandle] = pv
			} else {
				log.Errorf("PV %s: is not a CSI Volume", pv.Name)
			}
		}
		config, err := k8s.GetKubeConfig(ctx)
		if err != nil {
			log.Errorf("volumeInfoCRFullSync: Failed to get KubeConfig. err: %v", err)
			return
		}
		k8sClient, err := clientset.NewForConfig(config)
		if err != nil {
			log.Errorf("volumeInfoCRFullSync: Failed to create kubernetes client. Err: %+v", err)
			return
		}
		storageClassList, err := k8sClient.StorageV1().StorageClasses().List(ctx, metav1.ListOptions{})
		if err != nil {
			log.Errorf("volumeInfoCRFullSync: Failed to list storageclasses. Err: %+v", err)
			return
		}
		// Create scNameToPolicyIdMap map for easy lookup of PolicyIds for a given storageclass name
		for _, sc := range storageClassList.Items {
			if _, ok := scNameToPolicyIdMap[sc.Parameters[scParamStoragePolicyID]]; !ok {
				scNameToPolicyIdMap[sc.Name] =
					sc.Parameters[scParamStoragePolicyID]
			}
		}
	}

	for volumeID := range currentK8sPVMap {
		crExists, err := volumeInfoService.VolumeInfoCrExistsForVolume(ctx, volumeID)
		if err != nil {
			log.Errorf("FullSync for VC %s: failed to find VolumeInfo CR for volume %s."+
				"Error: %+v", vc, volumeID, err)
			continue
		}
		// Create VolumeInfo CR if not found.
		if !crExists {
			if len(metadataSyncer.configInfo.Cfg.VirtualCenter) > 1 {
				err := volumeInfoService.CreateVolumeInfo(ctx, volumeID, vc)
				if err != nil {
					log.Errorf("FullSync for VC %s: failed to create VolumeInfo CR for volume %s."+
						"Error: %+v", vc, volumeID, err)
					continue
				}
			} else if metadataSyncer.clusterFlavor == cnstypes.CnsClusterFlavorWorkload &&
				IsPodVMOnStretchSupervisorFSSEnabled {
				isLinkedCloneVolume := false
				pv := volumeIdTok8sPVMap[volumeID]
				// claimref will be nil when volume is static provisioned or any available/released pv
				// which are not claimed by pvc. added a check to handle such cases.
				if pv.Spec.ClaimRef == nil {
					log.Warnf("Claimref is not available for pv %s", pv.Name)
					continue
				} else {
					pvc, err := metadataSyncer.pvcLister.PersistentVolumeClaims(
						pv.Spec.ClaimRef.Namespace).Get(pv.Spec.ClaimRef.Name)
					if err != nil {
						log.Warnf("Failed to get pvc for namespace %s and name %s. err=%+v",
							pv.Spec.ClaimRef.Namespace, pv.Spec.ClaimRef.Name, err)
						continue
					}
					if IsLinkedCloneSupportFSSEnabled && metav1.HasAnnotation(pvc.ObjectMeta, common.AnnKeyLinkedClone) {
						isLinkedCloneVolume = true
					}
					pvcCapacity := pvc.Status.Capacity[v1.ResourceStorage]
					if pvc.Spec.StorageClassName != nil {
						err = volumeInfoService.CreateVolumeInfoWithPolicyInfo(ctx, volumeID, pvc.Namespace,
							scNameToPolicyIdMap[*pvc.Spec.StorageClassName], *pvc.Spec.StorageClassName, vc,
							&pvcCapacity, isLinkedCloneVolume)
						if err != nil {
							log.Warnf("FullSync for VC %s: failed to create VolumeInfo CR for volume %s."+
								"Error: %+v", vc, volumeID, err)
							continue
						}
					} else {
						log.Warnf("FullSync for VC %s: failed to create VolumeInfo CR for volume %s."+
							"StorageClassName not found in the PVC spec %v.", vc, volumeID, pvc.Spec)
						continue
					}
				}
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
			if _, exists := currentK8sPVMap[cnsvolumeinfo.Spec.VolumeID]; !exists {
				// If a PV is not present in the cluster for two full sync cycles, delete its VolumeInfo CR.
				if _, existsInCrDeletionMap := volumeInfoCrDeletionMap[vc][cnsvolumeinfo.Spec.VolumeID]; existsInCrDeletionMap {
					err := volumeInfoService.DeleteVolumeInfo(ctx, cnsvolumeinfo.Spec.VolumeID)
					if err != nil {
						log.Errorf("FullSync for VC %s: failed to delete VolumeInfo CR for volume %s."+
							"Error: %+v", vc, cnsvolumeinfo.Spec.VolumeID, err)
						continue
					}
					delete(volumeInfoCrDeletionMap[vc], cnsvolumeinfo.Spec.VolumeID)
				} else {
					// Add volume to deletion map.
					volumeInfoCrDeletionMap[vc][cnsvolumeinfo.Spec.VolumeID] = true
				}
			}
		}
	}
	log.Debugf("FullSync for VC %s: volumeInfoCrDeletionMap: %v", vc, volumeInfoCrDeletionMap)
}

// validateAndCorrectVolumeInfoSnapshotDetails sync cnsvolumeinfo snapshot details with by comparing
// the aggregatedSnapshotSize of CNS volume.
// validate aggregated snapshot size: compare aggregated snapshot size of individual volume
// in cns with the size in cnsvolumeinfo. if found discrepancy in order to correct the values
// update the cnsvolumeinfo.
func validateAndCorrectVolumeInfoSnapshotDetails(ctx context.Context,
	cnsBlockVolumeMap map[string]cnstypes.CnsVolume) {
	log := logger.GetLogger(ctx)
	volumeInfoCRList := volumeInfoService.ListAllVolumeInfos()
	alreadySyncedVolumeCount := 0
	outOfSyncVolumeCount := 0
	for _, volumeInfo := range volumeInfoCRList {
		cnsvolumeinfo := &cnsvolumeinfov1alpha1.CNSVolumeInfo{}
		err := runtime.DefaultUnstructuredConverter.FromUnstructured(volumeInfo.(*unstructured.Unstructured).Object,
			&cnsvolumeinfo)
		if err != nil {
			log.Errorf("Failed to parse cnsvolumeinfo object: %v, err: %v", cnsvolumeinfo, err)
			continue
		}
		if cnsVol, ok := cnsBlockVolumeMap[cnsvolumeinfo.Spec.VolumeID]; ok {
			log.Debugf("validate volume info for storage details for volume %s", cnsVol.VolumeId.Id)
			var aggregatedSnapshotCapacityInMB int64
			if cnsVol.BackingObjectDetails != nil &&
				cnsVol.BackingObjectDetails.(*cnstypes.CnsBlockBackingDetails) != nil {
				val, ok := cnsVol.BackingObjectDetails.(*cnstypes.CnsBlockBackingDetails)
				if ok {
					aggregatedSnapshotCapacityInMB = val.AggregatedSnapshotCapacityInMb
				}
				log.Debugf("Received aggregatedSnapshotCapacity %dMB for volume %s from CNS",
					aggregatedSnapshotCapacityInMB, cnsVol.VolumeId.Id)
				if cnsvolumeinfo.Spec.AggregatedSnapshotSize != nil && cnsvolumeinfo.Spec.ValidAggregatedSnapshotSize {
					aggregatedSnapshotCapacity := resource.NewQuantity(aggregatedSnapshotCapacityInMB*common.MbInBytes,
						resource.BinarySI)
					if aggregatedSnapshotCapacity.Value() == cnsvolumeinfo.Spec.AggregatedSnapshotSize.Value() {
						log.Debugf("volume %s Aggregated Snapshot capacity %s in CnsVolumeInfo is in sync with CNS",
							cnsVol.VolumeId.Id, aggregatedSnapshotCapacity.String())
						alreadySyncedVolumeCount++
						continue
					}
					log.Infof("Aggregated Snapshot size mismatch for volume %s, %s in CnsVolumeInfo and %dMB in CNS",
						cnsVol.VolumeId.Id, cnsvolumeinfo.Spec.AggregatedSnapshotSize.String(),
						aggregatedSnapshotCapacityInMB)
				}
				// Nothing to do if it's invalid in CNS and CNSVolumeInfo
				if !cnsvolumeinfo.Spec.ValidAggregatedSnapshotSize && aggregatedSnapshotCapacityInMB == -1 {
					log.Infof("volume %s aggregated snapshot capacity not present in CNS and CNSVolumeInfo",
						cnsVol.VolumeId.Id)
					continue
				}
				// implies the cnsvolumeinfo has capacity mismatch with CNS queryVolume result OR
				// existing cnsvolumeinfo aggregated snapshot is not set or invalid
				log.Infof("Update aggregatedSnapshotCapacity for volume %s to %dMB",
					cnsVol.VolumeId.Id, aggregatedSnapshotCapacityInMB)
				// use current time as snapshot completion time is not available in fullsync.
				currentTime := time.Now()
				cnsSnapInfo := &volumes.CnsSnapshotInfo{
					SourceVolumeID:                      cnsvolumeinfo.Spec.VolumeID,
					SnapshotLatestOperationCompleteTime: time.Now(),
					AggregatedSnapshotCapacityInMb:      aggregatedSnapshotCapacityInMB,
				}
				log.Infof("snapshot operation completion time unavailable for volumeID %s, will"+
					" use current time %v instead", cnsvolumeinfo.Spec.VolumeID, currentTime.String())
				patch, err := common.GetValidatedCNSVolumeInfoPatch(ctx, cnsSnapInfo)
				if err != nil {
					log.Errorf("unable to get VolumeInfo patch for %s. Error: %+v. Continuing..",
						cnsvolumeinfo.Spec.VolumeID, err)
					continue
				}
				patchBytes, err := json.Marshal(patch)
				if err != nil {
					log.Errorf("error while create VolumeInfo patch for volume %s. Error while marshaling: %+v. Continuing..",
						cnsvolumeinfo.Spec.VolumeID, err)
					continue
				}
				err = volumeInfoService.PatchVolumeInfo(ctx, cnsvolumeinfo.Spec.VolumeID, patchBytes,
					allowedRetriesToPatchCNSVolumeInfo)
				if err != nil {
					log.Errorf("failed to patch CNSVolumeInfo instance to update snapshot details."+
						"for volume %s. Error: %+v. Continuing..", cnsvolumeinfo.Spec.VolumeID, err)
					continue
				}
				log.Infof("Updated CNSvolumeInfo with Snapshot details successfully for volume %s",
					cnsvolumeinfo.Spec.VolumeID)
				outOfSyncVolumeCount++
			}
		}
	}
	log.Infof("Number of volumes with synced aggregated snapshot size with CNS %d", alreadySyncedVolumeCount)
	log.Infof("Number of volumes with out-of-sync aggregated snapshot size with CNS %d", outOfSyncVolumeCount)
}

// fullSyncCreateVolumes creates volumes with given array of createSpec.
// Before creating a volume, all current K8s volumes are retrieved.
// If the volume is successfully created, it is removed from `cnsCreationMap`.
func fullSyncCreateVolumes(ctx context.Context, createSpecArray []cnstypes.CnsVolumeCreateSpec,
	metadataSyncer *metadataSyncInformer, wg *sync.WaitGroup, migrationFeatureStateForFullSync bool,
	volManager volumes.Manager, vc string) {
	log := logger.GetLogger(ctx)
	defer wg.Done()
	currentK8sPVMap := make(map[string]*v1.PersistentVolume)
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
			currentK8sPVMap[pv.Spec.CSI.VolumeHandle] = pv
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
			currentK8sPVMap[volumeHandle] = pv
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
		if pv, existsInK8s := currentK8sPVMap[volumeID]; existsInK8s {
			log.Debugf("FullSync for VC %s: Calling CreateVolume for volume id: %q with createSpec %+v",
				vc, volumeID, spew.Sdump(createSpec))
			_, _, err := volManager.CreateVolume(ctx, &createSpec, nil)
			if err != nil {
				log.Warnf("FullSync for VC %s: Failed to create volume with the spec: %+v. "+
					"Err: %+v", vc, spew.Sdump(createSpec), err)
				continue
			}

			if !isDynamicallyCreatedVolume(ctx, pv) {
				generateEventOnPv(ctx, pv, v1.EventTypeNormal,
					staticVolumeProvisioningSuccessReason, staticVolumeProvisioningSuccessMessage)
			}

			if len(metadataSyncer.configInfo.Cfg.VirtualCenter) > 1 {
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

// healthLabelKeys are labels applied to CNS volume PV entity metadata
// out-of-band by full sync's own health-labeling routines, which have no
// counterpart on the actual k8s PV object, so k8s-truth metadata built from
// pv.GetLabels() never carries them. See preserveHealthLabelsOnK8sPVMetadata.
//
// pv_missing is deliberately NOT included here. preserveHealthLabelsOnK8sPVMetadata
// only ever runs for volumes that have a live k8s PV (fullSyncConstructVolumeMaps
// iterates pvList), so by construction the volume is not missing at that
// point — any pv_missing label still present in CNS is stale and must be
// allowed to drop via the normal k8s-truth diff/overwrite. That overwrite is
// what clears pv_missing when a matching PV reappears (see
// getMissingPVVolumeUpdateSpecs and tests/e2e/fullsync_pv_missing.go
// "pv-missing-cleared"). Only pv_retained belongs here, since a volume can
// remain legitimately retained-and-unconsumed across many cycles while still
// having a live PV, and getRetainedPVVolumeUpdateSpecs re-derives eligibility
// independently every cycle.
var healthLabelKeys = []string{
	prometheus.PrometheusPVRetainedLabelKey,
}

// preserveHealthLabelsOnK8sPVMetadata copies any healthLabelKeys found on the
// CNS-side PV entity metadata onto the corresponding k8s-side PV entity
// metadata, in place.
//
// Without this, isUpdateRequired's exact label-map comparison (via
// CompareKubernetesMetadata) sees a health label present only on the CNS
// side as a difference and triggers a metadata "updateVolume" that rebuilds
// CNS entity metadata purely from k8s-truth data, silently stripping the
// label. getRetainedPVVolumeUpdateSpecs only re-adds pv_retained when it
// isn't already present, based on a CNS query taken earlier in the same full
// sync cycle, so it doesn't observe the strip happening later in that same
// cycle — the label only reappears on the next cycle. The net effect,
// without this preservation, is the label visibly flipping on/off on
// alternating full sync cycles even though the volume's retained/unconsumed
// state never changed.
func preserveHealthLabelsOnK8sPVMetadata(k8sMetadataList []cnstypes.BaseCnsEntityMetadata,
	cnsMetadataList []cnstypes.BaseCnsEntityMetadata) {
	var cnsPV *cnstypes.CnsKubernetesEntityMetadata
	for _, m := range cnsMetadataList {
		if km, ok := m.(*cnstypes.CnsKubernetesEntityMetadata); ok &&
			km.EntityType == string(cnstypes.CnsKubernetesEntityTypePV) {
			cnsPV = km
			break
		}
	}
	if cnsPV == nil {
		return
	}
	cnsLabels := cnsvsphere.GetLabelsMapFromKeyValue(cnsPV.Labels)
	for i, m := range k8sMetadataList {
		km, ok := m.(*cnstypes.CnsKubernetesEntityMetadata)
		if !ok || km.EntityType != string(cnstypes.CnsKubernetesEntityTypePV) {
			continue
		}
		k8sLabels := cnsvsphere.GetLabelsMapFromKeyValue(km.Labels)
		if k8sLabels == nil {
			k8sLabels = make(map[string]string)
		}
		changed := false
		for _, key := range healthLabelKeys {
			if val, ok := cnsLabels[key]; ok && k8sLabels[key] != val {
				k8sLabels[key] = val
				changed = true
			}
		}
		if changed {
			k8sMetadataList[i] = cnsvsphere.GetCnsKubernetesEntityMetaData(km.EntityName, k8sLabels,
				km.Delete, km.EntityType, km.Namespace, km.ClusterID, km.ReferredEntity)
		}
	}
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
		clusterIDforVolumeMetadata, volManager, metadataSyncer, nil)
	if err != nil {
		log.Errorf("FullSync for VC %s: fullSyncGetQueryResults failed to query volume metadata from vc. Err: %v", vc, err)
		return nil, nil, nil, err
	}

	for _, queryResult := range allQueryResults {
		for _, volume := range queryResult.Volumes {
			var cnsMetadata []cnstypes.BaseCnsEntityMetadata
			allEntityMetadata := volume.Metadata.EntityMetadata
			for _, metadata := range allEntityMetadata {
				if metadata.(*cnstypes.CnsKubernetesEntityMetadata).ClusterID == clusterIDforVolumeMetadata {
					cnsMetadata = append(cnsMetadata, metadata)
				}
			}
			volumeToCnsEntityMetadataMap[volume.VolumeId.Id] = cnsMetadata
			preserveHealthLabelsOnK8sPVMetadata(volumeToK8sEntityMetadataMap[volume.VolumeId.Id], cnsMetadata)

			volumeClusterDistributionMap[volume.VolumeId.Id] =
				hasClusterDistributionSet(ctx, volume, clusterIDforVolumeMetadata,
					metadataSyncer.configInfo.Cfg.Global.ClusterDistribution)
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
				log.Debugf("FullSync for VC %s: update is not required for volume: %q", vc, volumeHandle)
			}
		}
		switch operationType {
		case "createVolume":
			var volumeType string
			if IsFileVolume(pv) {
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
			if IsFileVolume(pv) {
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

// getMissingPVVolumeUpdateSpecs scans the CNS volume list for volumes whose
// corresponding Kubernetes PV cannot be found in k8sPVMap and returns the set
// of CnsVolumeMetadataUpdateSpec needed to set the `pv_missing=true` label on
// the existing PV-type CnsKubernetesEntityMetadata of each such volume.
//
// A two-cycle grace period is observed: a volume only becomes a candidate for
// labeling on the second consecutive full-sync where its PV is absent. The
// per-vCenter cnsDeletionMap (kept under its legacy name to avoid touching
// unrelated state) is reused as the grace tracker — entries are added on the
// first miss and act as the "seen before" flag on the second miss.
//
// Volumes are skipped if:
//   - the PVC for the volume is still cached (transient race),
//   - the volume is an in-use inline-migrated volume (when CSI migration is on),
//   - the volume's PV-type entity is already labeled `pv_missing=true`,
//   - the volume has no PV-type entity scoped to this cluster (legacy/foreign).
//
// This function replaces the legacy getVolumesToBeDeleted + fullSyncDeleteVolumes
// pair, which used to call DeleteVolume(deleteDisk=false) to unregister the
// CNS volume — stranding the underlying FCD invisibly. With the new flow the
// volume stays registered and remains visible in the vCenter Container Volumes
// view so a VI admin can inspect and remediate, while observability is exposed
// through the prometheus.CnsVolumePVMissingGaugeVec metric.
//
// The returned count is the number of volumes for which an UpdateSpec was
// generated and corresponds to what the caller should publish to the
// pv_missing gauge for this VC.
func getMissingPVVolumeUpdateSpecs(ctx context.Context, cnsVolumeList []cnstypes.CnsVolume,
	k8sPVMap map[string]string, metadataSyncer *metadataSyncInformer,
	migrationFeatureStateForFullSync bool, containerCluster cnstypes.CnsContainerCluster,
	vc string) ([]cnstypes.CnsVolumeMetadataUpdateSpec, int, error) {
	log := logger.GetLogger(ctx)
	var updateSpecArray []cnstypes.CnsVolumeMetadataUpdateSpec

	// inlineVolumeMap holds the volume path information for migrated volumes
	// which are used by Pods.
	inlineVolumeMap := make(map[string]string)
	var err error
	if migrationFeatureStateForFullSync {
		inlineVolumeMap, err = fullSyncGetInlineMigratedVolumesInfo(ctx, metadataSyncer, migrationFeatureStateForFullSync)
		if err != nil {
			log.Errorf("FullSync for VC %s: Failed to get inline migrated volumes. Err: %v", vc, err)
			return nil, 0, err
		}
	}

	for _, vol := range cnsVolumeList {
		if _, existsInK8s := k8sPVMap[vol.VolumeId.Id]; existsInK8s {
			continue
		}

		// Already labeled by a previous full sync cycle: skip re-evaluating
		// this volume's CNS metadata and reissuing UpdateVolumeMetadata. This
		// short-circuit is purely local (independent of what the current CNS
		// query returns), so a volume that remains PV-missing indefinitely is
		// only ever labeled once instead of every cycle.
		if pvMissingLabeledMap[vc][vol.VolumeId.Id] {
			continue
		}

		// PVC is still cached: the PV likely has not surfaced in the lister
		// yet. Defer to the next full-sync cycle rather than racing the API
		// server's eventual-consistency.
		if nsNameStr, ok := metadataSyncer.coCommonInterface.GetPVCNamespacedNameByUID(vol.VolumeId.Id); ok {
			log.Infof(
				"FullSync for VC %s: Skipping pv_missing labeling for volume %q: PVC found in cache at %s",
				vc, vol.VolumeId.Id, nsNameStr,
			)
			continue
		}

		// Migrated inline-volumes that are still referenced by a running Pod
		// must not be labeled as PV-missing; the in-tree volume path is the
		// authoritative reference, not a PV object.
		if migrationFeatureStateForFullSync {
			if _, inUse := inlineVolumeMap[vol.VolumeId.Id]; inUse {
				log.Debugf("FullSync for VC %s: Skipping pv_missing labeling for in-use inline-migrated volume %q",
					vc, vol.VolumeId.Id)
				continue
			}
		}

		// Grace period: first time we observe the volume without its PV we
		// only record it; only on the next cycle do we act on it.
		if _, seenBefore := cnsDeletionMap[vc][vol.VolumeId.Id]; !seenBefore {
			cnsDeletionMap[vc][vol.VolumeId.Id] = true
			log.Infof("FullSync for VC %s: Volume %q has no matching K8s PV; "+
				"deferring pv_missing label to next cycle (grace period)",
				vc, vol.VolumeId.Id)
			continue
		}

		// The volume may already carry the label from before this process
		// started (e.g. syncer restart), in which case pvMissingLabeledMap
		// would not know about it yet. Detect that from the live query so we
		// don't keep re-deriving an update spec for it every cycle.
		if isPVEntityLabeled(vol, prometheus.PrometheusPVMissingLabelKey, prometheus.PrometheusPVMissingLabelValue) {
			pvMissingLabeledMap[vc][vol.VolumeId.Id] = true
			continue
		}

		updateSpec, ok := buildPVMissingUpdateSpec(ctx, vol, containerCluster, vc)
		if !ok {
			// Either the volume has no in-cluster PV-type entity or it is
			// already labeled. Keep it in the grace tracker so subsequent
			// cycles continue to no-op cheaply.
			continue
		}
		// Mark labeled now so subsequent cycles short-circuit above,
		// regardless of whether this cycle's CNS query reflects the label
		// yet on a later read.
		pvMissingLabeledMap[vc][vol.VolumeId.Id] = true
		updateSpecArray = append(updateSpecArray, updateSpec)
	}
	return updateSpecArray, len(updateSpecArray), nil
}

// isPVEntityLabeled reports whether the volume's PV-type CNS entity
// metadata, scoped to the current cluster, already carries the given label.
func isPVEntityLabeled(vol cnstypes.CnsVolume, labelKey, labelValue string) bool {
	for _, em := range vol.Metadata.EntityMetadata {
		k8sEm, ok := em.(*cnstypes.CnsKubernetesEntityMetadata)
		if !ok || k8sEm.ClusterID != clusterIDforVolumeMetadata ||
			k8sEm.EntityType != string(cnstypes.CnsKubernetesEntityTypePV) {
			continue
		}
		if cnsvsphere.GetLabelsMapFromKeyValue(k8sEm.Labels)[labelKey] == labelValue {
			return true
		}
	}
	return false
}

// buildPVMissingUpdateSpec returns an UpdateSpec that sets pv_missing=true on
// the CNS volume's PV-type CnsKubernetesEntityMetadata, preserving any
// pre-existing labels on those entries.
//
// If the volume has one or more in-cluster PV-type entities, each is reissued
// with the pv_missing label appended.
//
// If no in-cluster PV-type entity is present (legacy volumes, or volumes
// whose CNS metadata never carried a PV entity), a synthetic PV-type entity
// is generated using the CNS volume's `Name` as the entity name so the label
// has somewhere to live. Volumes that lack a usable name AND have no
// in-cluster PV entity are skipped — there is no safe key to attach the
// label under.
//
// Returns (spec, false) if there is nothing to update (every PV entity is
// already labeled, or no candidate entity could be synthesized).
func buildPVMissingUpdateSpec(ctx context.Context, vol cnstypes.CnsVolume,
	containerCluster cnstypes.CnsContainerCluster, vc string) (cnstypes.CnsVolumeMetadataUpdateSpec, bool) {
	log := logger.GetLogger(ctx)
	var entityMetadata []cnstypes.BaseCnsEntityMetadata
	foundInClusterPV := false
	log.Infof("FullSync for VC %s: buildPVMissingUpdateSpec: volume %q with name %q has %d entity metadata entries",
		vc, vol.VolumeId.Id, vol.Name, len(vol.Metadata.EntityMetadata))
	for _, em := range vol.Metadata.EntityMetadata {
		k8sEm, ok := em.(*cnstypes.CnsKubernetesEntityMetadata)
		if !ok {
			log.Infof("FullSync for VC %s: buildPVMissingUpdateSpec: volume %q skipping non-K8s entity metadata (type %T)",
				vc, vol.VolumeId.Id, em)
			continue
		}
		if k8sEm.ClusterID != clusterIDforVolumeMetadata {
			// Entity belongs to a different K8s cluster — leave it alone.
			log.Infof("FullSync for VC %s: buildPVMissingUpdateSpec: volume %q skipping entity with clusterID %q"+
				" (expected %q) entityType %s entityName %s",
				vc, vol.VolumeId.Id, k8sEm.ClusterID, clusterIDforVolumeMetadata, k8sEm.EntityType, k8sEm.EntityName)
			continue
		}
		if k8sEm.EntityType != string(cnstypes.CnsKubernetesEntityTypePV) {
			log.Infof("FullSync for VC %s: buildPVMissingUpdateSpec: volume %q skipping entity type %s (name %s)",
				vc, vol.VolumeId.Id, k8sEm.EntityType, k8sEm.EntityName)
			continue
		}
		foundInClusterPV = true
		labels := cnsvsphere.GetLabelsMapFromKeyValue(k8sEm.Labels)
		if labels == nil {
			labels = make(map[string]string)
		}
		if labels[prometheus.PrometheusPVMissingLabelKey] == prometheus.PrometheusPVMissingLabelValue {
			// Already labeled in CNS; nothing to do for this entity.
			log.Infof("FullSync for VC %s: buildPVMissingUpdateSpec: volume %q PV entity %q"+
				" already has pv_missing=true label, skipping", vc, vol.VolumeId.Id, k8sEm.EntityName)
			continue
		}
		labels[prometheus.PrometheusPVMissingLabelKey] = prometheus.PrometheusPVMissingLabelValue
		entityMetadata = append(entityMetadata,
			cnsvsphere.GetCnsKubernetesEntityMetaData(k8sEm.EntityName, labels, false,
				k8sEm.EntityType, k8sEm.Namespace, k8sEm.ClusterID, k8sEm.ReferredEntity))
	}

	if !foundInClusterPV && vol.Name != "" {
		// Synthesize a PV-type entity so the pv_missing label has a home on
		// volumes whose CNS metadata never carried a PV entry.
		log.Infof("FullSync for VC %s: buildPVMissingUpdateSpec: volume %q has no in-cluster PV entity;"+
			" synthesizing one using vol.Name %q", vc, vol.VolumeId.Id, vol.Name)
		labels := map[string]string{
			prometheus.PrometheusPVMissingLabelKey: prometheus.PrometheusPVMissingLabelValue,
		}
		entityMetadata = append(entityMetadata,
			cnsvsphere.GetCnsKubernetesEntityMetaData(vol.Name, labels, false,
				string(cnstypes.CnsKubernetesEntityTypePV), "", clusterIDforVolumeMetadata, nil))
	}

	if len(entityMetadata) == 0 {
		log.Infof("FullSync for VC %s: buildPVMissingUpdateSpec: volume %q produced no update specs"+
			" (foundInClusterPV=%v vol.Name=%q); skipping",
			vc, vol.VolumeId.Id, foundInClusterPV, vol.Name)
		return cnstypes.CnsVolumeMetadataUpdateSpec{}, false
	}
	log.Infof("FullSync for VC %s: Adding pv_missing label to volume %q "+
		"(no matching K8s PV after grace period)", vc, vol.VolumeId.Id)
	return cnstypes.CnsVolumeMetadataUpdateSpec{
		VolumeId: cnstypes.CnsVolumeId{Id: vol.VolumeId.Id},
		Metadata: cnstypes.CnsVolumeMetadata{
			ContainerCluster:      containerCluster,
			ContainerClusterArray: []cnstypes.CnsContainerCluster{containerCluster},
			EntityMetadata:        entityMetadata,
		},
	}, true
}

// getRetainedPVVolumeUpdateSpecs returns UpdateSpecs for CNS volumes whose
// matching Kubernetes PV has ReclaimPolicy=Retain and is in Released or
// Available phase. A PV in either of these phases has no active PVC binding
// and therefore no active PodVM or VMService VM consumer — the volume data
// is retained on the datastore but is no longer serving any workload.
//
// The returned count corresponds to what the caller should publish to the
// pv_retained gauge for this VC.
func getRetainedPVVolumeUpdateSpecs(ctx context.Context, cnsVolumeList []cnstypes.CnsVolume,
	k8sPVs []*v1.PersistentVolume, containerCluster cnstypes.CnsContainerCluster,
	vc string) ([]cnstypes.CnsVolumeMetadataUpdateSpec, int, error) {
	log := logger.GetLogger(ctx)
	var updateSpecArray []cnstypes.CnsVolumeMetadataUpdateSpec

	// Build a set of volume IDs for Retain-policy PVs with no active consumer.
	// Released = was bound, PVC deleted, PV kept by Retain policy.
	// Available = never bound (or manually reclaimed), no consumer.
	retainedVolumeIDs := make(map[string]struct{})
	for _, pv := range k8sPVs {
		if pv.Spec.PersistentVolumeReclaimPolicy != v1.PersistentVolumeReclaimRetain {
			continue
		}
		if pv.Status.Phase != v1.VolumeReleased && pv.Status.Phase != v1.VolumeAvailable {
			continue
		}
		if pv.Spec.CSI == nil || pv.Spec.CSI.VolumeHandle == "" {
			continue
		}
		retainedVolumeIDs[pv.Spec.CSI.VolumeHandle] = struct{}{}
		log.Debugf("FullSync for VC %s: PV %q (volume %q) has ReclaimPolicy=Retain and phase=%s",
			vc, pv.Name, pv.Spec.CSI.VolumeHandle, pv.Status.Phase)
	}

	if len(retainedVolumeIDs) == 0 {
		log.Debugf("FullSync for VC %s: no Retain-policy Released/Available PVs found; skipping pv_retained labeling", vc)
		return nil, 0, nil
	}

	for _, vol := range cnsVolumeList {
		if _, isRetained := retainedVolumeIDs[vol.VolumeId.Id]; !isRetained {
			continue
		}
		updateSpec, ok := buildPVRetainedUpdateSpec(ctx, vol, containerCluster, vc)
		if !ok {
			continue
		}
		updateSpecArray = append(updateSpecArray, updateSpec)
	}
	return updateSpecArray, len(updateSpecArray), nil
}

// buildPVRetainedUpdateSpec returns an UpdateSpec that sets pv_retained=true
// on the CNS volume's PV-type CnsKubernetesEntityMetadata, preserving any
// pre-existing labels on those entries.
//
// It mirrors buildPVMissingUpdateSpec but applies the pv_retained label to
// volumes whose K8s PV has ReclaimPolicy=Retain and no active consumer.
//
// Returns (spec, false) if there is nothing to update (every PV entity is
// already labeled, or no candidate entity could be synthesized).
func buildPVRetainedUpdateSpec(ctx context.Context, vol cnstypes.CnsVolume,
	containerCluster cnstypes.CnsContainerCluster, vc string) (cnstypes.CnsVolumeMetadataUpdateSpec, bool) {
	log := logger.GetLogger(ctx)
	var entityMetadata []cnstypes.BaseCnsEntityMetadata
	foundInClusterPV := false
	log.Infof("FullSync for VC %s: buildPVRetainedUpdateSpec: volume %q with name %q has %d entity metadata entries",
		vc, vol.VolumeId.Id, vol.Name, len(vol.Metadata.EntityMetadata))
	for _, em := range vol.Metadata.EntityMetadata {
		k8sEm, ok := em.(*cnstypes.CnsKubernetesEntityMetadata)
		if !ok {
			continue
		}
		if k8sEm.ClusterID != clusterIDforVolumeMetadata {
			continue
		}
		if k8sEm.EntityType != string(cnstypes.CnsKubernetesEntityTypePV) {
			continue
		}
		foundInClusterPV = true
		labels := cnsvsphere.GetLabelsMapFromKeyValue(k8sEm.Labels)
		if labels == nil {
			labels = make(map[string]string)
		}
		if labels[prometheus.PrometheusPVRetainedLabelKey] == prometheus.PrometheusPVRetainedLabelValue {
			log.Infof("FullSync for VC %s: buildPVRetainedUpdateSpec: volume %q PV entity %q"+
				" already has pv_retained=true label, skipping", vc, vol.VolumeId.Id, k8sEm.EntityName)
			continue
		}
		labels[prometheus.PrometheusPVRetainedLabelKey] = prometheus.PrometheusPVRetainedLabelValue
		entityMetadata = append(entityMetadata,
			cnsvsphere.GetCnsKubernetesEntityMetaData(k8sEm.EntityName, labels, false,
				k8sEm.EntityType, k8sEm.Namespace, k8sEm.ClusterID, k8sEm.ReferredEntity))
	}

	if !foundInClusterPV && vol.Name != "" {
		log.Infof("FullSync for VC %s: buildPVRetainedUpdateSpec: volume %q has no in-cluster PV entity;"+
			" synthesizing one using vol.Name %q", vc, vol.VolumeId.Id, vol.Name)
		labels := map[string]string{
			prometheus.PrometheusPVRetainedLabelKey: prometheus.PrometheusPVRetainedLabelValue,
		}
		entityMetadata = append(entityMetadata,
			cnsvsphere.GetCnsKubernetesEntityMetaData(vol.Name, labels, false,
				string(cnstypes.CnsKubernetesEntityTypePV), "", clusterIDforVolumeMetadata, nil))
	}

	if len(entityMetadata) == 0 {
		log.Infof("FullSync for VC %s: buildPVRetainedUpdateSpec: volume %q produced no update specs"+
			" (foundInClusterPV=%v vol.Name=%q); skipping",
			vc, vol.VolumeId.Id, foundInClusterPV, vol.Name)
		return cnstypes.CnsVolumeMetadataUpdateSpec{}, false
	}
	log.Infof("FullSync for VC %s: Adding pv_retained label to volume %q "+
		"(Retain-policy PV with no active consumer)", vc, vol.VolumeId.Id)
	return cnstypes.CnsVolumeMetadataUpdateSpec{
		VolumeId: cnstypes.CnsVolumeId{Id: vol.VolumeId.Id},
		Metadata: cnstypes.CnsVolumeMetadata{
			ContainerCluster:      containerCluster,
			ContainerClusterArray: []cnstypes.CnsContainerCluster{containerCluster},
			EntityMetadata:        entityMetadata,
		},
	}, true
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
//
//   - cnsCreationMap: tracks volumes seen in K8s but not yet in CNS. Entries
//     whose volume is no longer in K8s are removed (the create candidacy
//     lapsed before the second cycle could act).
//   - cnsDeletionMap: now serves as the pv_missing grace tracker (see
//     getMissingPVVolumeUpdateSpecs). Entries whose volume reappeared in K8s
//     are removed so the volume is no longer considered "PV missing" and the
//     grace period resets for any future absence.
//
// The map names are retained for historical/test compatibility; their meaning
// for cnsDeletionMap has shifted from "to be deleted" to "missing PV, awaiting
// label".
func cleanupCnsMaps(k8sPVs map[string]string, vc string) {
	// Cleanup cnsCreationMap.
	cnsCreationMapForVc := cnsCreationMap[vc]
	for volID := range cnsCreationMapForVc {
		if _, existsInK8s := k8sPVs[volID]; !existsInK8s {
			delete(cnsCreationMap[vc], volID)
		}
	}
	// Cleanup cnsDeletionMap (pv_missing grace tracker).
	cnsDeletionMapForVc := cnsDeletionMap[vc]
	for volID := range cnsDeletionMapForVc {
		if _, existsInK8s := k8sPVs[volID]; existsInK8s {
			// PV reappeared — clear the grace flag.
			delete(cnsDeletionMap[vc], volID)
		}
	}
	// Cleanup pvMissingLabeledMap: if the PV reappeared in K8s, forget that
	// the volume was labeled so it is re-evaluated (and re-labeled) from
	// scratch if it goes missing again later.
	pvMissingLabeledMapForVc := pvMissingLabeledMap[vc]
	for volID := range pvMissingLabeledMapForVc {
		if _, existsInK8s := k8sPVs[volID]; existsInK8s {
			delete(pvMissingLabeledMap[vc], volID)
		}
	}
}

func RemoveCNSFinalizerFromPVCIfTKGClusterDeleted(ctx context.Context, k8sClient clientset.Interface,
	pvc *v1.PersistentVolumeClaim, finalizerToRemove string, isNamespaceBeingDeleted bool) {
	log := logger.GetLogger(ctx)
	// Create client to operator on Cluster object
	restClientConfig, err := k8s.GetKubeConfig(ctx)
	if err != nil {
		msg := fmt.Sprintf("RemoveCNSFinalizerFromPVCIfTKGClusterDeleted: Failed to initialize rest clientconfig. "+
			"Err: %+v", err)
		log.Error(msg)
		return
	}
	ccClient, err := k8s.NewClientForGroup(ctx, restClientConfig, ccV1beta2.GroupVersion.Group)
	if err != nil {
		msg := fmt.Sprintf("RemoveCNSFinalizerFromPVCIfTKGClusterDeleted: Failed to get vmOperatorClient. "+
			"Err: %+v", err)
		log.Error(msg)
		return
	}
	log.Debugf("RemoveCNSFinalizerFromPVCIfTKGClusterDeleted called for pvc %s/%s finalizer %d", pvc.Namespace, pvc.Name,
		len(pvc.ObjectMeta.Finalizers))
	okToRemoveFinalizer := false
	if len(pvc.ObjectMeta.Finalizers) != 0 &&
		(slices.Contains(pvc.ObjectMeta.Finalizers, finalizerToRemove)) {
		if !isNamespaceBeingDeleted {
			// Fetch guest cluster name from PVC label and check if that guest cluster,
			// where associated PVC was created, is running or not.
			// NOTE: This is to be executed only when not called on namespace deletion.
			var tkcClusterName string
			for key := range pvc.ObjectMeta.Labels {
				if strings.Contains(key, "TKGService") {
					tkcDetails := strings.Split(key, "/")
					tkcClusterName = tkcDetails[0]
					break
				}
			}
			if tkcClusterName == "" {
				if finalizerToRemove == cnsoperatortypes.CNSVolumeFinalizer {
					msg := fmt.Sprintf("RemoveCNSFinalizerFromPVCIfTKGClusterDeleted: failed to get Cluster Name "+
						"from PVC %q in %q namespace. Err: %+v", pvc.Name, pvc.Namespace, err)
					log.Error(msg)
				} else {
					okToRemoveFinalizer = true
				}
			} else {
				cc := &ccV1beta2.Cluster{}
				err := ccClient.Get(ctx, client.ObjectKey{
					Namespace: pvc.Namespace,
					Name:      tkcClusterName,
				}, cc)
				if err != nil && !apierrors.IsNotFound(err) {
					msg := fmt.Sprintf("RemoveCNSFinalizerFromPVCIfTKGClusterDeleted: failed to get Cluster %q "+
						"in %q namespace. Err: %+v", tkcClusterName, pvc.Namespace, err)
					log.Error(msg)
				} else if ((err == nil) && (cc.Status.Phase != "Running")) || apierrors.IsNotFound(err) {
					okToRemoveFinalizer = true
				}
			}
		} else {
			// In case of namespace deletion, do not check for guest cluster.
			// Directly delete the CNS finalizer added from guest cluster.
			okToRemoveFinalizer = true
		}
		if okToRemoveFinalizer {
			// Remove finalizer if associated guest cluster is not-running/deleted or Namespace being deleted
			log.Infof("RemoveCNSFinalizerFromPVCIfTKGClusterDeleted: Removing %q finalizer from PVC "+
				"with name: %q on namespace: %q in Terminating state",
				cnsoperatortypes.CNSVolumeFinalizer, pvc.Name, pvc.Namespace)
			controllerutil.RemoveFinalizer(pvc, finalizerToRemove)
			_, err = k8sClient.CoreV1().PersistentVolumeClaims(pvc.Namespace).Update(ctx,
				pvc, metav1.UpdateOptions{})
			if err != nil {
				msg := fmt.Sprintf("RemoveCNSFinalizerFromPVCIfTKGClusterDeleted: failed to update "+
					"supervisor PVC %q in %q namespace. Err: %+v", pvc.Name, pvc.Namespace, err)
				log.Error(msg)
			}
		}
	}
}

func RemoveCNSFinalizerFromSnapIfTKGClusterDeleted(ctx context.Context, snapshotterClient versioned.Interface,
	vs *snapv1.VolumeSnapshot, finalizerToRemove string, isNamespaceBeingDeleted bool) {
	log := logger.GetLogger(ctx)
	log.Infof("RemoveCNSFinalizerFromSnapIfTKGClusterDeleted called for vs %s/%s", vs.Namespace, vs.Name)
	// Create client to operator on Cluster object
	restClientConfig, err := k8s.GetKubeConfig(ctx)
	if err != nil {
		msg := fmt.Sprintf("RemoveCNSFinalizerFromSnapIfTKGClusterDeleted: Failed to initialize rest clientconfig. "+
			"Err: %+v", err)
		log.Error(msg)
		return
	}
	ccClient, err := k8s.NewClientForGroup(ctx, restClientConfig, ccV1beta2.GroupVersion.Group)
	if err != nil {
		msg := fmt.Sprintf("RemoveCNSFinalizerFromSnapIfTKGClusterDeleted: Failed to get vmOperatorClient. "+
			"Err: %+v", err)
		log.Error(msg)
		return
	}
	log.Debugf("RemoveCNSFinalizerFromSnapIfTKGClusterDeleted called for vs %s/%s finalizer %d", vs.Namespace, vs.Name,
		len(vs.ObjectMeta.Finalizers))
	okToRemoveFinalizer := false
	if (len(vs.ObjectMeta.Finalizers) != 0) &&
		(slices.Contains(vs.ObjectMeta.Finalizers, finalizerToRemove)) {
		if !isNamespaceBeingDeleted {
			// Fetch guest cluster name from snapshot label and check if that guest cluster,
			// where associated snapshot was created, is running or not.
			// NOTE: This is to be executed only when not called on namespace deletion.
			var tkcClusterName string
			for key := range vs.ObjectMeta.Labels {
				if strings.Contains(key, "TKGService") {
					tkcDetails := strings.Split(key, "/")
					tkcClusterName = tkcDetails[0]
					break
				}
			}
			if tkcClusterName == "" {
				if finalizerToRemove == cnsoperatortypes.CNSSnapshotFinalizer {
					msg := fmt.Sprintf("RemoveCNSFinalizerFromPVCIfTKGClusterDeleted: failed to get Cluster Name "+
						"from VolumeSnapshot %q in %q namespace. Err: %+v", vs.Name, vs.Namespace, err)
					log.Error(msg)
				} else {
					okToRemoveFinalizer = true
				}
			} else {
				cc := &ccV1beta2.Cluster{}
				err := ccClient.Get(ctx, client.ObjectKey{
					Namespace: vs.Namespace,
					Name:      tkcClusterName,
				}, cc)
				if err != nil && !apierrors.IsNotFound(err) {
					msg := fmt.Sprintf("RemoveCNSFinalizerFromSnapIfTKGClusterDeleted: failed to get Cluster %q "+
						"in %q namespace. Err: %+v", tkcClusterName, vs.Namespace, err)
					log.Error(msg)
				} else if ((err == nil) && (cc.Status.Phase != "Running") && (cc.Status.Phase != "Provisioned")) ||
					apierrors.IsNotFound(err) {
					okToRemoveFinalizer = true
				}
			}
		} else {
			// In case of namespace deletion, do not check for guest cluster.
			// Directly delete the CNS finalizer added from guest cluster.
			okToRemoveFinalizer = true
		}
		if okToRemoveFinalizer {
			// Remove finalizer if associated guest cluster is not-running/deleted or Namespace being deleted
			log.Infof("RemoveCNSFinalizerFromSnapIfTKGClusterDeleted: Removing %q finalizer from VolumeSnapshot "+
				"with name: %q on namespace: %q in Terminating state",
				cnsoperatortypes.CNSSnapshotFinalizer, vs.Name, vs.Namespace)
			controllerutil.RemoveFinalizer(vs, finalizerToRemove)
			_, err = snapshotterClient.SnapshotV1().VolumeSnapshots(vs.Namespace).Update(ctx,
				vs, metav1.UpdateOptions{})
			if err != nil {
				msg := fmt.Sprintf("RemoveCNSFinalizerFromSnapIfTKGClusterDeleted: failed to update "+
					"supervisor VolumeSnapshot %q in %q namespace. Err: %+v", vs.Name, vs.Namespace, err)
				log.Error(msg)
			}
		}
	}
}

// classifySupervisorPVC examines the labels and ownerReferences of a
// supervisor PVC and returns the set of csi.vsphere.volume.type/* annotation
// keys that should be present on it. All returned keys map to AnnValueTrue;
// the value is the same for every category so callers only need the keys.
//
// Classification rules (boolean tags, not mutually exclusive):
//
//   - AnnKeyVKSNode is included if any OwnerReference has
//     Kind == OwnerKindVirtualMachine or Kind == OwnerKindVSphereMachine.
//     This signals that the PVC is consumed as a Node-VM disk in a VKS
//     guest cluster (lifetime tied to a specific Node VM).
//
//   - AnnKeyVKSWorkload is included if any label key contains
//     TKGServiceLabelMarker. VKS guest-cluster Pod PVCs carry a label of
//     the form "<gc-name>/TKGService" — the presence of that marker is
//     authoritative even when the rest of the key has been redacted.
//
//   - AnnKeySupervisorPodVM is included if neither of the above applies and
//     pvc.Spec.VolumeName is referenced by a VolumeAttachment object —
//     i.e., by exclusion, the PV must be attached to a podVM as it is
//     attached AND it is not attached to a VKSNode.
//
//   - AnnKeySupervisorVMServiceVM is included if neither of the first two
//     applies and any PVC annotation key has prefix
//     cnsoperatortypes.UsedByVMAnnotationPrefix — i.e., the PVC is attached
//     as a data disk to a standalone VM Service VM via
//     CnsNodeVMBatchAttachment rather than through the Kubernetes attacher
//     flow.
//
//   - AnnKeySupervisorWorkload is included only when none of the above
//     applies. The annotation is set explicitly rather than implied by
//     absence so that "no csi.vsphere.volume.type/* annotation at all"
//     reliably means "fullsync has not yet evaluated this PVC".
//
// A PVC that satisfies both Node and Workload signals (rare but possible
// if, for example, a guest-cluster Pod volume gains a VM owner reference
// through an out-of-band edit) is double-tagged with vks-node AND
// vks-workload but is NOT tagged supervisor-workload. The same holds for
// any other combination of signals: all matching tags are set, and
// supervisor-workload is set only when none match.
func classifySupervisorPVC(pvc *v1.PersistentVolumeClaim, attachedPVs map[string]struct{}) map[string]string {
	desired := make(map[string]string, 2)

	hasVKSNode := false
	for _, owner := range pvc.OwnerReferences {
		if owner.Kind == common.OwnerKindVirtualMachine ||
			owner.Kind == common.OwnerKindVSphereMachine {
			hasVKSNode = true
			break
		}
	}

	hasVKSWorkload := false
	for key := range pvc.Labels {
		if strings.Contains(key, common.TKGServiceLabelMarker) {
			hasVKSWorkload = true
			break
		}
	}

	hasPodVMAttachment := false
	if pvc.Spec.VolumeName != "" {
		if _, ok := attachedPVs[pvc.Spec.VolumeName]; ok {
			hasPodVMAttachment = true
		}
	}

	hasVMServiceAttachment := false
	for key := range pvc.Annotations {
		if strings.HasPrefix(key, cnsoperatortypes.UsedByVMAnnotationPrefix) {
			hasVMServiceAttachment = true
			break
		}
	}

	if hasVKSNode {
		desired[common.AnnKeyVKSNode] = common.AnnValueTrue
	}
	if hasVKSWorkload {
		desired[common.AnnKeyVKSWorkload] = common.AnnValueTrue
	}
	if hasPodVMAttachment {
		desired[common.AnnKeySupervisorPodVM] = common.AnnValueTrue
	}
	if hasVMServiceAttachment {
		desired[common.AnnKeySupervisorVMServiceVM] = common.AnnValueTrue
	}
	if !hasVKSNode && !hasVKSWorkload && !hasPodVMAttachment && !hasVMServiceAttachment {
		desired[common.AnnKeySupervisorWorkload] = common.AnnValueTrue
	}
	return desired
}

// reconcilePVCWorkloadTypeAnnotations returns a Strategic-Merge-Patch byte
// payload that, when applied to pvc, adds every annotation in desired and
// removes every csi.vsphere.volume.type/* annotation that is NOT in desired.
// Annotations outside the AnnPrefixVKSWorkloadType namespace are left
// untouched. Returns (nil, false, nil) if no change is needed; the caller
// can then skip the API patch entirely.
//
// The patch is built via strategic-merge so that:
//   - other PVC fields (Spec, Status, other annotations) are never touched,
//   - stale tags are explicitly deleted via the JSON "null" value (the SMP
//     convention for removing a map entry).
func reconcilePVCWorkloadTypeAnnotations(pvc *v1.PersistentVolumeClaim,
	desired map[string]string) ([]byte, bool, error) {
	annotationsPatch := map[string]interface{}{}

	// 1) Add or update the desired tags.
	for key, val := range desired {
		if existing, ok := pvc.Annotations[key]; !ok || existing != val {
			annotationsPatch[key] = val
		}
	}

	// 2) Remove any pre-existing csi.vsphere.volume.type/* tag that is not
	//    in the desired set. In Strategic Merge Patch, setting a map key
	//    to JSON null deletes that key.
	for key := range pvc.Annotations {
		if !strings.HasPrefix(key, common.AnnPrefixVKSWorkloadType) {
			continue
		}
		if _, keep := desired[key]; !keep {
			annotationsPatch[key] = nil
		}
	}

	if len(annotationsPatch) == 0 {
		return nil, false, nil
	}

	patch := map[string]interface{}{
		"metadata": map[string]interface{}{
			"annotations": annotationsPatch,
		},
	}
	body, err := json.Marshal(patch)
	if err != nil {
		return nil, false, err
	}
	return body, true, nil
}

// annotateSupervisorPVCsWithWorkloadType iterates over every PVC in every
// supervisor namespace and reconciles the csi.vsphere.volume.type/*
// annotations that classify the PVC's consumer workload type (VKS Node VM,
// VKS guest-cluster Pod, Supervisor PodVM, or Supervisor VM Service VM). See
// classifySupervisorPVC for the rule set.
//
// PVCs being deleted (DeletionTimestamp set) are skipped — there is no
// value in updating a tombstone and doing so can race with the deletion
// flow. Errors are logged per-PVC but do NOT abort the loop: classification
// is idempotent and the next full-sync cycle will reattempt.
//
// This helper is gated behind the ImprovedVolumeVisibility FSS
// and only runs in CnsClusterFlavorWorkload (supervisor) deployments.
func annotateSupervisorPVCsWithWorkloadType(ctx context.Context,
	metadataSyncer *metadataSyncInformer) {
	log := logger.GetLogger(ctx)
	log.Infof("annotateSupervisorPVCsWithWorkloadType: start")
	startTime := time.Now()

	k8sClient, err := k8s.NewClient(ctx)
	if err != nil {
		log.Errorf("annotateSupervisorPVCsWithWorkloadType: failed to get kubernetes client. Err: %v", err)
		return
	}

	// Use the informer cache rather than a fresh API list — supervisors with
	// thousands of namespaces would otherwise pay a multi-second round-trip
	// on every full-sync cycle.
	pvcs, err := metadataSyncer.pvcLister.PersistentVolumeClaims(v1.NamespaceAll).List(labels.Everything())
	if err != nil {
		log.Errorf("annotateSupervisorPVCsWithWorkloadType: failed to list supervisor PVCs. Err: %v", err)
		return
	}

	attachedPVs, err := loadAttachedPVNames(metadataSyncer.vaLister)
	if err != nil {
		log.Errorf("annotateSupervisorPVCsWithWorkloadType: failed to list VolumeAttachment objects. Err: %v", err)
		return
	}

	var patched, skipped, failed int
	for _, pvc := range pvcs {
		if pvc.ObjectMeta.DeletionTimestamp != nil {
			skipped++
			continue
		}
		desired := classifySupervisorPVC(pvc, attachedPVs)
		patchBytes, needsPatch, err := reconcilePVCWorkloadTypeAnnotations(pvc, desired)
		if err != nil {
			log.Errorf("annotateSupervisorPVCsWithWorkloadType: failed to build patch for PVC %s/%s. Err: %v",
				pvc.Namespace, pvc.Name, err)
			failed++
			continue
		}
		if !needsPatch {
			skipped++
			continue
		}
		_, err = k8sClient.CoreV1().PersistentVolumeClaims(pvc.Namespace).Patch(ctx,
			pvc.Name, types.StrategicMergePatchType, patchBytes, metav1.PatchOptions{})
		if err != nil {
			// IsNotFound: the PVC was deleted between List and Patch. Treat
			// as transient — the next full-sync cycle will re-evaluate the
			// rest of the cluster.
			if apierrors.IsNotFound(err) {
				skipped++
				continue
			}
			log.Warnf("annotateSupervisorPVCsWithWorkloadType: failed to patch PVC %s/%s. Err: %v",
				pvc.Namespace, pvc.Name, err)
			failed++
			continue
		}
		log.Infof("annotateSupervisorPVCsWithWorkloadType: reconciled workload-type annotations on PVC %s/%s",
			pvc.Namespace, pvc.Name)
		patched++
	}
	log.Infof("annotateSupervisorPVCsWithWorkloadType: end. total=%d patched=%d skipped=%d failed=%d elapsed=%s",
		len(pvcs), patched, skipped, failed, time.Since(startTime))
}

// cleanupUnusedPVCsAndSnapshotsFromGuestCluster iterates over all PVCs and VolumeSnapshots and
// filter ones with DeletionTimestamp set but CNS specific finalizer present. These finalizers are added to
// PVCs and VolumeSnapshots from guest cluster during their creation.
// If such objects found, remove the finalizer from those objects if corresponding guest cluster is not-running/deleted.
// This code handles cases where namespace deletion causes guest cluster and its corresponding components
// to be deleted but associated objects on supervisor remain stuck in Terminating state.
func cleanupUnusedPVCsAndSnapshotsFromGuestCluster(ctx context.Context) {
	log := logger.GetLogger(ctx)
	// Create K8S client and snapshotter client
	k8sClient, err := k8s.NewClient(ctx)
	if err != nil {
		log.Errorf("cleanupUnusedPVCsAndSnapshotsFromGuestCluster: Failed to get kubernetes client. Err: %+v", err)
		return
	}
	snapshotterClient, err := k8s.NewSnapshotterClient(ctx)
	if err != nil {
		log.Errorf("cleanupUnusedPVCsAndSnapshotsFromGuestCluster: failed to get snapshotterClient. Err: %v", err)
		return
	}

	// Get all VolumeSnapshots and check if any marked for deletion but has finalizer
	// "cns.vmware.com/volumesnapshot-protection", set while creation from guest cluster.
	// If found, remove the finalizer "cns.vmware.com/volumesnapshot-protection" from that snapshot.
	vsList, err := snapshotterClient.SnapshotV1().VolumeSnapshots("").List(ctx, metav1.ListOptions{})
	if err != nil {
		log.Errorf("cleanupUnusedPVCsAndSnapshotsFromGuestCluster: failed to list VolumeSnapshot."+
			" Err: %v", err)
		return
	}
	for _, vs := range vsList.Items {
		// Check if snapshot is being deleted and has "cns.vmware.com/volumesnapshot-protection" finalizer
		// Note: Above finalizer will only be removed in 2 cases
		//       1. Namespace is deleted
		//       2. Guest cluster, from which this smapshot is created, is deleted.
		if vs.ObjectMeta.DeletionTimestamp != nil {
			RemoveCNSFinalizerFromSnapIfTKGClusterDeleted(ctx, snapshotterClient, &vs,
				cnsoperatortypes.CNSSnapshotFinalizer, false)
		}
	}

	// Get all PVCs and check if any marked for deletion but have finalizer "cns.vmware.com/pvc-delete-protection",
	// set while creation from guest cluster.
	// If found, remove the finalizer "cns.vmware.com/pvc-delete-protection" from that PVC.
	pvcList, err := k8sClient.CoreV1().PersistentVolumeClaims("").List(ctx, metav1.ListOptions{})
	if err != nil {
		log.Errorf("cleanupUnusedPVCsAndSnapshotsFromGuestCluster: failed to list PersistentVolumeClaims."+
			" Err: %v", err)
		return
	}
	for _, pvc := range pvcList.Items {
		// Check if PVC is being deleted and has "cns.vmware.com/pvc-delete-protection" finalizer
		// Note: Above finalizer will only be removed in 2 cases
		//       1. Namespace is deleted
		//       2. Guest cluster, from which this PVC is created, is deleted.
		if pvc.ObjectMeta.DeletionTimestamp != nil {
			RemoveCNSFinalizerFromPVCIfTKGClusterDeleted(ctx, k8sClient, &pvc,
				cnsoperatortypes.CNSVolumeFinalizer, false)
		}
	}
}

// getSnapshotsWithoutChangeIDAnnotation lists supervisor VolumeSnapshots, filters those needing change-id annotation,
// and returns them grouped by volumeID for selective CNS querying.
// This is separated to enable testability with sample k8s objects.
func getSnapshotsWithoutChangeIDAnnotation(ctx context.Context, snapshotterClient versioned.Interface) (
	snapshotMap map[string]*snapv1.VolumeSnapshot,
	volumeSnapMap map[string]map[string]struct{},
	err error,
) {
	log := logger.GetLogger(ctx)

	snapshotMap = make(map[string]*snapv1.VolumeSnapshot)
	volumeSnapMap = make(map[string]map[string]struct{})

	// Use pagination to avoid loading all snapshots at once
	const pageSize = 500
	listOptions := metav1.ListOptions{
		Limit: pageSize,
	}

	for {
		// List VolumeSnapshots across all namespaces with pagination
		vsList, err := snapshotterClient.SnapshotV1().VolumeSnapshots("").List(ctx, listOptions)
		if err != nil {
			log.Errorf(
				"getSnapshotsWithoutChangeIDAnnotation: failed to list supervisor VolumeSnapshots. Error: %v",
				err)
			return nil, nil, err
		}

		if len(vsList.Items) == 0 {
			log.Debugf("getSnapshotsWithoutChangeIDAnnotation: No VolumeSnapshots found in current page")
			break
		}

		log.Debugf(
			"getSnapshotsWithoutChangeIDAnnotation: Processing page with %d VolumeSnapshots",
			len(vsList.Items))

		// Process each snapshot in this page
		for i := range vsList.Items {
			vs := &vsList.Items[i]
			if changeID, exists := vs.Annotations[common.VolumeSnapshotChangeIDKey]; exists && changeID != "" {
				log.Debugf(
					"getSnapshotsWithoutChangeIDAnnotation: VolumeSnapshot %q/%q already has change-id annotation",
					vs.Namespace, vs.Name)
				continue
			}
			if vs.Status == nil {
				log.Debugf("getSnapshotsWithoutChangeIDAnnotation: VolumeSnapshot %q/%q has no status",
					vs.Namespace, vs.Name)
				continue
			}
			if vs.Status.ReadyToUse != nil && !*vs.Status.ReadyToUse {
				log.Debugf(
					"getSnapshotsWithoutChangeIDAnnotation: VolumeSnapshot %q/%q is not ready to use",
					vs.Namespace, vs.Name)
				continue
			}
			if vs.Status.BoundVolumeSnapshotContentName == nil ||
				*vs.Status.BoundVolumeSnapshotContentName == "" {
				log.Debugf(
					"getSnapshotsWithoutChangeIDAnnotation: VolumeSnapshot %q/%q has no bound VSC",
					vs.Namespace, vs.Name)
				continue
			}
			vsc, err := snapshotterClient.SnapshotV1().VolumeSnapshotContents().Get(ctx,
				*vs.Status.BoundVolumeSnapshotContentName, metav1.GetOptions{})
			if err != nil {
				log.Errorf(
					"getSnapshotsWithoutChangeIDAnnotation: failed to get VolumeSnapshotContent for snapshot %q/%q. Error: %v",
					vs.Namespace, vs.Name, err)
				continue
			}
			if vsc.Status == nil || vsc.Status.SnapshotHandle == nil || *vsc.Status.SnapshotHandle == "" {
				log.Debugf(
					"getSnapshotsWithoutChangeIDAnnotation: VolumeSnapshotContent %q has no snapshot handle",
					vsc.Name)
				continue
			}
			volumeID, snapID, err := common.ParseCSISnapshotID(*vsc.Status.SnapshotHandle)
			if err != nil {
				log.Errorf(
					"getSnapshotsWithoutChangeIDAnnotation: failed to parse snapshot handle %q. Error: %v",
					*vsc.Status.SnapshotHandle, err)
				continue
			}
			snapshotMap[snapID] = vs
			if _, ok := volumeSnapMap[volumeID]; !ok {
				volumeSnapMap[volumeID] = make(map[string]struct{})
			}
			volumeSnapMap[volumeID][snapID] = struct{}{}
		}

		// Check if there are more pages
		if vsList.Continue == "" {
			// No more pages
			break
		}
		// Set continue token for next page
		listOptions.Continue = vsList.Continue
	}

	if len(volumeSnapMap) == 0 {
		log.Debugf("getSnapshotsWithoutChangeIDAnnotation: No snapshots need change-id annotation")
	}

	return snapshotMap, volumeSnapMap, nil
}

// annotateSnapshotsFromCNSResults processes CNS QuerySnapshots results and annotates matching VolumeSnapshots.
// This is separated to enable testability with mock volumeManager and coCommonInterface.
// It delegates pagination to utils.QuerySnapshotsUtil which handles cursor management internally.
func annotateSnapshotsFromCNSResults(
	ctx context.Context,
	volumeID string,
	wantedSnapIDs map[string]struct{},
	snapshotMap map[string]*snapv1.VolumeSnapshot,
	volumeManager volumes.Manager,
	coCommonInterface commonco.COCommonInterface,
) int {
	log := logger.GetLogger(ctx)

	log.Debugf("annotateSnapshotsFromCNSResults: Processing volume %q with %d snapshots needing annotation",
		volumeID, len(wantedSnapIDs))

	// Build CnsSnapshotQueryFilter for the volume.
	// utils.QuerySnapshotsUtil handles cursor-based pagination internally.
	snapshotQueryFilter := cnstypes.CnsSnapshotQueryFilter{
		SnapshotQuerySpecs: []cnstypes.CnsSnapshotQuerySpec{
			{
				VolumeId: cnstypes.CnsVolumeId{Id: volumeID},
			},
		},
	}

	// Query CNS for all snapshots of this volume; util handles pagination.
	queryEntries, _, err := utils.QuerySnapshotsUtil(ctx, volumeManager, snapshotQueryFilter, common.QuerySnapshotLimit)
	if err != nil {
		log.Warnf("annotateSnapshotsFromCNSResults: QuerySnapshots failed for volume %q. Error: %v",
			volumeID, err)
		return 0
	}

	annotatedCount := 0
	// Reuse single annotation map to reduce allocation pressure in loop
	annotations := make(map[string]string, 1)

	for _, entry := range queryEntries {
		if entry.Error != nil {
			log.Debugf("annotateSnapshotsFromCNSResults: skipping entry with fault for volume %q: %+v",
				volumeID, entry.Error.Fault)
			continue
		}
		snapID := entry.Snapshot.SnapshotId.Id
		// Only process snapshots that need change-id annotation
		if _, want := wantedSnapIDs[snapID]; !want {
			continue
		}
		changeID := entry.Snapshot.ChangedBlockTrackingId
		if changeID == "" {
			log.Debugf("annotateSnapshotsFromCNSResults: No change-id found for snapshot %q in volume %q",
				snapID, volumeID)
			continue
		}

		vs, ok := snapshotMap[snapID]
		if !ok {
			log.Debugf("annotateSnapshotsFromCNSResults: VolumeSnapshot not found for snapshot %q", snapID)
			continue
		}

		// Annotate the VolumeSnapshot - reuse annotations map
		annotations[common.VolumeSnapshotChangeIDKey] = changeID
		annotated, annotErr := coCommonInterface.AnnotateVolumeSnapshot(ctx, vs.Name, vs.Namespace, annotations)
		if annotErr != nil || !annotated {
			log.Warnf(
				"annotateSnapshotsFromCNSResults: Failed to annotate VolumeSnapshot %q/%q with change-id. Error: %v",
				vs.Namespace, vs.Name, annotErr)
			continue
		}
		log.Infof(
			"annotateSnapshotsFromCNSResults: Successfully set change-id annotation on "+
				"VolumeSnapshot %q/%q with value: %q",
			vs.Namespace, vs.Name, changeID)
		annotatedCount++
	}

	return annotatedCount
}

// setChangeIDAnnotationOnSupervisorSnapshots reconciles the VolumeSnapshotChangeIDKey annotation on supervisor
// VolumeSnapshots by fetching change-id from CNS snapshots using QuerySnapshots call.
// It:
// 1. Lists all supervisor VolumeSnapshots across all namespaces
// 2. Resolves volumeID/snapshotID from each VolumeSnapshot's bound VolumeSnapshotContent
// 3. Groups snapshots needing annotation by volumeID (selective - only volumes we care about)
// 4. Queries CNS sequentially, one volume at a time (CNS does not parallelize requests)
// 5. For each CNS query result, immediately annotates matching VolumeSnapshots
// This ensures supervisor snapshots have complete metadata that can be mirrored to guest cluster snapshots.
func setChangeIDAnnotationOnSupervisorSnapshots(ctx context.Context, metadataSyncer *metadataSyncInformer, vc string) {
	log := logger.GetLogger(ctx)
	log.Debugf("setChangeIDAnnotationOnSupervisorSnapshots: Start for VC: %s", vc)

	// Create snapshotter client for supervisor cluster
	snapshotterClient, err := k8s.NewSnapshotterClient(ctx)
	if err != nil {
		log.Errorf("setChangeIDAnnotationOnSupervisorSnapshots: failed to get snapshotterClient. Error: %v", err)
		return
	}

	// Build groupings of snapshots needing annotation
	snapshotMap, volumeSnapMap, err := getSnapshotsWithoutChangeIDAnnotation(ctx, snapshotterClient)
	if err != nil {
		return
	}

	if len(volumeSnapMap) == 0 {
		log.Debugf("setChangeIDAnnotationOnSupervisorSnapshots: No supervisor VolumeSnapshots need change-id annotation")
		return
	}

	log.Infof(
		"setChangeIDAnnotationOnSupervisorSnapshots: Found %d VolumeSnapshots across %d volumes needing change-id annotation",
		len(snapshotMap), len(volumeSnapMap))

	// Get volume manager and common interface for CNS queries and annotations
	volumeManager := metadataSyncer.volumeManager
	coCommonInterface := metadataSyncer.coCommonInterface

	// Process each volume sequentially. CNS does not parallelize requests, so we
	// query one volume at a time and annotate snapshots as results arrive.
	totalAnnotatedCount := 0
	for volumeID, wantedSnapIDs := range volumeSnapMap {
		annotatedCount := annotateSnapshotsFromCNSResults(
			ctx, volumeID, wantedSnapIDs, snapshotMap, volumeManager, coCommonInterface)
		totalAnnotatedCount += annotatedCount
	}

	log.Infof(
		"setChangeIDAnnotationOnSupervisorSnapshots: Completed. Annotated %d VolumeSnapshots with change-id",
		totalAnnotatedCount)
	log.Debugf("setChangeIDAnnotationOnSupervisorSnapshots: End for VC: %s", vc)
}

// reconcileKeepAfterDeleteVmFlags performs bidirectional reconciliation of the
// keepAfterDeleteVm control flag for PVCs on Supervisor cluster:
//
//  1. Clear flag: PVCs with VM ownerRef but missing annotation → clear flag + set annotation
//     (handles missed pvcAdded events during upgrade, informer gaps, transient failures)
//
//  2. Restore flag: PVCs with annotation but no VM ownerRef → restore flag + remove annotation
//     (handles missed pvcUpdated events when VM ownerRef was removed)
//
// This function only runs on Supervisor cluster (CnsClusterFlavorWorkload).
func reconcileKeepAfterDeleteVmFlags(ctx context.Context, metadataSyncer *metadataSyncInformer) {
	log := logger.GetLogger(ctx)
	log.Debugf("reconcileKeepAfterDeleteVmFlags: Start")

	// List all PVCs
	allPVCs, err := metadataSyncer.pvcLister.List(labels.Everything())
	if err != nil {
		log.Errorf("reconcileKeepAfterDeleteVmFlags: Failed to list PVCs: %v", err)
		return
	}

	clearedCount := 0
	restoredCount := 0
	for _, pvc := range allPVCs {
		// Skip if PVC is not bound
		if pvc.Status.Phase != v1.ClaimBound {
			continue
		}

		// Get PV to retrieve volumeID (needed for both directions)
		pv, err := metadataSyncer.pvLister.Get(pvc.Spec.VolumeName)
		if err != nil {
			log.Warnf("reconcileKeepAfterDeleteVmFlags: Failed to get PV %s for PVC %s/%s: %v",
				pvc.Spec.VolumeName, pvc.Namespace, pvc.Name, err)
			continue
		}

		// Only process vSphere CSI volumes
		if pv.Spec.CSI == nil || pv.Spec.CSI.Driver != csitypes.Name {
			continue
		}

		volumeID := pv.Spec.CSI.VolumeHandle
		hasAnnotation := pvc.Annotations != nil && pvc.Annotations[common.AnnVMDeleteProtectionCleared] == "true"
		hasVMOwner := hasVMOwnerRef(pvc)

		// Case 1: PVC has VM ownerRef but no annotation → clear flag + set annotation
		if hasVMOwner && !hasAnnotation {
			if clearKeepAfterDeleteVmForExistingPVC(ctx, pvc, volumeID,
				metadataSyncer.volumeManager, metadataSyncer.supervisorClient) {
				clearedCount++
			}
			continue
		}

		// Case 2: PVC has annotation but no VM ownerRef → restore flag + remove annotation
		if hasAnnotation && !hasVMOwner {
			if restoreKeepAfterDeleteVmForDetachedPVC(ctx, pvc, volumeID,
				metadataSyncer.volumeManager, metadataSyncer.supervisorClient) {
				restoredCount++
			}
		}
	}

	if clearedCount > 0 {
		log.Infof("reconcileKeepAfterDeleteVmFlags: Cleared keepAfterDeleteVm flag for %d PVCs", clearedCount)
	}
	if restoredCount > 0 {
		log.Infof("reconcileKeepAfterDeleteVmFlags: Restored keepAfterDeleteVm flag for %d PVCs", restoredCount)
	}
	log.Debugf("reconcileKeepAfterDeleteVmFlags: End")
}
