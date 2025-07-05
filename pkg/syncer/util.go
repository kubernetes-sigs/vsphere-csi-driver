package syncer

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"google.golang.org/grpc/codes"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/client-go/kubernetes/scheme"
	"k8s.io/client-go/tools/cache"
	"k8s.io/client-go/tools/record"
	"sigs.k8s.io/controller-runtime/pkg/client"
	cnsconfig "sigs.k8s.io/vsphere-csi-driver/v3/pkg/common/config"
	"sigs.k8s.io/vsphere-csi-driver/v3/pkg/csi/service/common/commonco"

	"github.com/davecgh/go-spew/spew"
	jsonpatch "github.com/evanphx/json-patch/v5"
	cnstypes "github.com/vmware/govmomi/cns/types"
	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/labels"
	apitypes "k8s.io/apimachinery/pkg/types"
	typedcorev1 "k8s.io/client-go/kubernetes/typed/core/v1"
	storagepolicyusagev1alpha2 "sigs.k8s.io/vsphere-csi-driver/v3/pkg/apis/cnsoperator/storagepolicy/v1alpha2"
	"sigs.k8s.io/vsphere-csi-driver/v3/pkg/apis/migration"
	volumes "sigs.k8s.io/vsphere-csi-driver/v3/pkg/common/cns-lib/volume"
	cnsvsphere "sigs.k8s.io/vsphere-csi-driver/v3/pkg/common/cns-lib/vsphere"
	"sigs.k8s.io/vsphere-csi-driver/v3/pkg/common/utils"
	"sigs.k8s.io/vsphere-csi-driver/v3/pkg/csi/service/common"
	"sigs.k8s.io/vsphere-csi-driver/v3/pkg/csi/service/logger"
	csitypes "sigs.k8s.io/vsphere-csi-driver/v3/pkg/csi/types"
	k8s "sigs.k8s.io/vsphere-csi-driver/v3/pkg/kubernetes"
)

const (
	// These are the values required for geenrating events on a K8s object

	// Syncer component name
	syncerComponent = "VSphere CSI Syncer"
	// reason for PV creation failure when static volume provioining fails
	staticVolumeProvisioningFailure = "static volume provisioning failed"
	// reason for successful PV creation for static volumes
	staticVolumeProvisioningSuccessReason = "static volume provisioning succeeded"
	// message for successful PV creation for static volumes
	staticVolumeProvisioningSuccessMessage = "Successfully created container volume"

	// allowedRetriesToPatchStoragePolicyUsage indicates number of retries allowed for patching StoragePolicyUsage CR
	allowedRetriesToPatchStoragePolicyUsage = 5
	// volumdIDLimitPerQuery is set to 1000
	volumdIDLimitPerQuery = 1000
)

// getPVsInBoundAvailableOrReleased return PVs in Bound, Available or Released
// state.
func getPVsInBoundAvailableOrReleased(ctx context.Context,
	metadataSyncer *metadataSyncInformer) ([]*v1.PersistentVolume, error) {
	log := logger.GetLogger(ctx)
	var pvsInDesiredState []*v1.PersistentVolume
	log.Debugf("FullSync: Getting all PVs in Bound, Available or Released state")
	// Get all PVs from kubernetes.
	allPVs, err := metadataSyncer.pvLister.List(labels.Everything())
	if err != nil {
		return nil, err
	}
	for _, pv := range allPVs {
		if (pv.Spec.CSI != nil && pv.Spec.CSI.Driver == csitypes.Name) ||
			(metadataSyncer.coCommonInterface.IsFSSEnabled(ctx, common.CSIMigration) && pv.Spec.VsphereVolume != nil &&
				isValidvSphereVolume(ctx, pv)) {
			log.Debugf("FullSync: pv %v is in state %v", pv.Name, pv.Status.Phase)
			if pv.Status.Phase == v1.VolumeBound || pv.Status.Phase == v1.VolumeAvailable ||
				pv.Status.Phase == v1.VolumeReleased {
				pvsInDesiredState = append(pvsInDesiredState, pv)
			}
		}
	}
	return pvsInDesiredState, nil
}

// getBoundPVs is a helper function for VolumeHealthStatus feature and returns
// PVs in Bound state.
func getBoundPVs(ctx context.Context, metadataSyncer *metadataSyncInformer) ([]*v1.PersistentVolume, error) {
	log := logger.GetLogger(ctx)
	var boundPVs []*v1.PersistentVolume
	// Get all PVs from kubernetes.
	allPVs, err := metadataSyncer.pvLister.List(labels.Everything())
	if err != nil {
		return nil, err
	}
	for _, pv := range allPVs {
		if pv.Spec.CSI != nil && pv.Spec.CSI.Driver == csitypes.Name {
			log.Debugf("getBoundPVs: pv %s with volumeHandle %s is in state %v",
				pv.Name, pv.Spec.CSI.VolumeHandle, pv.Status.Phase)
			if pv.Status.Phase == v1.VolumeBound {
				boundPVs = append(boundPVs, pv)
			}
		}
	}
	return boundPVs, nil
}

// fullSyncGetInlineMigratedVolumesInfo is a helper function for retrieving
// inline PV information from Pods.
func fullSyncGetInlineMigratedVolumesInfo(ctx context.Context,
	metadataSyncer *metadataSyncInformer, migrationFeatureState bool) (map[string]string, error) {
	log := logger.GetLogger(ctx)
	inlineVolumes := make(map[string]string)
	// Get all Pods from kubernetes.
	allPods, err := metadataSyncer.podLister.List(labels.Everything())
	if err != nil {
		log.Errorf("FullSync: failed to fetch the list of pods with err: %+v", err)
		return nil, err
	}
	for _, pod := range allPods {
		for _, volume := range pod.Spec.Volumes {
			// Check if migration is ON and volumes if of type vSphereVolume.
			if migrationFeatureState && volume.VsphereVolume != nil {
				volumeHandle, err := volumeMigrationService.GetVolumeID(ctx,
					&migration.VolumeSpec{VolumePath: volume.VsphereVolume.VolumePath,
						StoragePolicyName: volume.VsphereVolume.StoragePolicyName}, true)
				if err != nil {
					log.Warnf(
						"FullSync: Failed to get VolumeID from volumeMigrationService for volumePath: %s with error %+v",
						volume.VsphereVolume.VolumePath, err)
					continue
				}
				inlineVolumes[volumeHandle] = volume.VsphereVolume.VolumePath
			}
		}
	}
	return inlineVolumes, nil
}

// IsValidVolume determines if the given volume mounted by a POD is a valid
// vsphere volume. Returns the pv and pvc object if true.
func IsValidVolume(ctx context.Context, volume v1.Volume, pod *v1.Pod,
	metadataSyncer *metadataSyncInformer) (bool, *v1.PersistentVolume, *v1.PersistentVolumeClaim) {
	log := logger.GetLogger(ctx)
	pvcName := volume.PersistentVolumeClaim.ClaimName
	// Get pvc attached to pod.
	pvc, err := metadataSyncer.pvcLister.PersistentVolumeClaims(pod.Namespace).Get(pvcName)
	if err != nil {
		log.Errorf("Error getting Persistent Volume Claim for volume %s with err: %v", volume.Name, err)
		return false, nil, nil
	}

	// Get pv object attached to pvc.
	pv, err := metadataSyncer.pvLister.Get(pvc.Spec.VolumeName)
	if err != nil {
		log.Errorf("Error getting Persistent Volume for PVC %s in volume %s with err: %v", pvc.Name, volume.Name, err)
		return false, nil, nil
	}
	if pv.Spec.CSI == nil {
		// Verify volume is a in-tree VCP volume.
		if pv.Spec.VsphereVolume != nil {
			// Check if migration feature switch is enabled.
			if metadataSyncer.coCommonInterface.IsFSSEnabled(ctx, common.CSIMigration) {
				if !isValidvSphereVolume(ctx, pv) {
					return false, nil, nil
				}
			} else {
				log.Debugf(
					"%s feature switch is disabled. Cannot update vSphere volume metadata %s for the pod %s in namespace %s",
					common.CSIMigration, pv.Name, pod.Name, pod.Namespace)
				return false, nil, nil
			}
		} else {
			log.Debugf("Volume %q is not a valid vSphere volume", pv.Name)
			return false, nil, nil
		}
	} else {
		if pv.Spec.CSI.Driver != csitypes.Name {
			log.Debugf("Pod %s in namespace %s has a volume %s which is not provisioned by vSphere CSI driver",
				pod.Name, pod.Namespace, pv.Name)
			return false, nil, nil
		}
	}
	return true, pv, pvc
}

// fullSyncGetQueryResults returns list of CnsQueryResult retrieved using
// queryFilter with offset and limit to query volumes using pagination
// if volumeIds is empty, then all volumes from CNS will be retrieved by
// pagination.
func fullSyncGetQueryResults(ctx context.Context, volumeIds []cnstypes.CnsVolumeId, clusterID string,
	volumeManager volumes.Manager, metadataSyncer *metadataSyncInformer) ([]*cnstypes.CnsQueryResult, error) {
	log := logger.GetLogger(ctx)
	log.Debugf("FullSync: fullSyncGetQueryResults is called with volumeIds %v for clusterID %s",
		volumeIds, clusterID)
	var volumeIdsBatchesToFilter [][]cnstypes.CnsVolumeId
	for i := 0; i < len(volumeIds); i += volumdIDLimitPerQuery {
		end := i + volumdIDLimitPerQuery
		if end > len(volumeIds) {
			end = len(volumeIds)
		}
		volumeIdsBatchesToFilter = append(volumeIdsBatchesToFilter, volumeIds[i:end])
	}

	var allQueryResults []*cnstypes.CnsQueryResult
	for _, volumeIdsBatch := range volumeIdsBatchesToFilter {
		queryFilter := cnstypes.CnsQueryFilter{
			VolumeIds: volumeIdsBatch,
		}
		if clusterID != "" {
			queryFilter.ContainerClusterIds = []string{clusterID}
		}
		queryResult, err := utils.QueryVolumeUtil(ctx, volumeManager, queryFilter, nil)
		if err != nil {
			return nil, logger.LogNewErrorCodef(log, codes.Internal,
				"queryVolumeUtil failed with err=%+v", err.Error())
		}
		if queryResult == nil {
			log.Info("Observed empty queryResult")
			continue
		}
		allQueryResults = append(allQueryResults, queryResult)
	}
	return allQueryResults, nil
}

// getPVCKey helps to get the PVC name from PVC object.
func getPVCKey(ctx context.Context, obj interface{}) (string, error) {
	log := logger.GetLogger(ctx)

	if unknown, ok := obj.(cache.DeletedFinalStateUnknown); ok && unknown.Obj != nil {
		obj = unknown.Obj
	}
	objKey, err := cache.DeletionHandlingMetaNamespaceKeyFunc(obj)
	if err != nil {
		log.Errorf("Failed to get key from object: %v", err)
		return "", err
	}
	log.Infof("getPVCKey: PVC key %s", objKey)
	return objKey, nil
}

// HasMigratedToAnnotationUpdate returns true if the migrated-to annotation
// is found in the newer object.
func HasMigratedToAnnotationUpdate(ctx context.Context, prevAnnotations map[string]string,
	newAnnotations map[string]string, objectName string) bool {
	log := logger.GetLogger(ctx)
	// Checking if the migrated-to annotation is found in the newer object.
	if _, annMigratedToFound := newAnnotations[common.AnnMigratedTo]; annMigratedToFound {
		if _, annMigratedToFound = prevAnnotations[common.AnnMigratedTo]; !annMigratedToFound {
			log.Debugf("Received %v annotation update for %q", common.AnnMigratedTo, objectName)
			return true
		}
	}
	log.Debugf("%v annotation not found for %q", common.AnnMigratedTo, objectName)
	return false
}

// isValidvSphereVolumeClaim returns true if the given PVC metadata of a vSphere
// Volume (in-tree volume) has migrated-to annotation on the PVC, or if the PVC
// was provisioned by CSI driver using in-tree storage class.
func isValidvSphereVolumeClaim(ctx context.Context, pvcMetadata metav1.ObjectMeta) bool {
	log := logger.GetLogger(ctx)
	// Checking if the migrated-to annotation is found in the PVC metadata.
	if annotation, annMigratedToFound := pvcMetadata.Annotations[common.AnnMigratedTo]; annMigratedToFound {
		if annotation == csitypes.Name &&
			(pvcMetadata.Annotations[common.AnnBetaStorageProvisioner] == common.InTreePluginName ||
				pvcMetadata.Annotations[common.AnnStorageProvisioner] == common.InTreePluginName) {
			log.Debugf("%v annotation found with value %q for PVC: %q",
				common.AnnMigratedTo, csitypes.Name, pvcMetadata.Name)
			return true
		}
	} else { // Checking if the PVC was provisioned by CSI.
		if pvcMetadata.Annotations[common.AnnBetaStorageProvisioner] == csitypes.Name ||
			pvcMetadata.Annotations[common.AnnStorageProvisioner] == csitypes.Name {
			log.Debugf("%v or %v annotation found with value %q for PVC: %q",
				common.AnnBetaStorageProvisioner, common.AnnStorageProvisioner, csitypes.Name, pvcMetadata.Name)
			return true
		}
	}
	return false
}

// isValidvSphereVolume returns true if the given PV is of a vSphere
// Volume (in-tree volume),
// or if the PV was provisioned by CSI driver using in-tree storage class.
func isValidvSphereVolume(ctx context.Context, pv *v1.PersistentVolume) bool {
	log := logger.GetLogger(ctx)
	// Check if PV is in-tree Static vSphere PV
	if pv.Spec.VsphereVolume != nil {
		_, ok := pv.Annotations[common.AnnDynamicallyProvisioned]
		if !ok {
			// when pv.Spec.VsphereVolume is not nil and "pv.kubernetes.io/provisioned-by" annotation is not present on the PV
			// PV is in-tree static vSphere PV
			log.Debugf("PV: %q is statically created in-tree vSphere volume", pv.Name)
			return true
		}
	}
	// Check if the migrated-to annotation is found on the PV.
	if annotation, annMigratedToFound := pv.ObjectMeta.Annotations[common.AnnMigratedTo]; annMigratedToFound {
		if annotation == csitypes.Name &&
			pv.ObjectMeta.Annotations[common.AnnDynamicallyProvisioned] == common.InTreePluginName {
			log.Debugf("%v annotation found with value %q for PV: %q",
				common.AnnMigratedTo, csitypes.Name, pv.Name)
			return true
		}
	}
	if pv.ObjectMeta.Annotations[common.AnnDynamicallyProvisioned] == csitypes.Name {
		log.Debugf("%v annotation found with value %q for PV: %q",
			common.AnnDynamicallyProvisioned, csitypes.Name, pv.Name)
		return true
	}
	return false
}

// IsFileVolume returns true for PVs that have accessMode as RWX or ROM
// and volumeMode as FileSystem.
func IsFileVolume(pv *v1.PersistentVolume) bool {
	if pv == nil {
		return false
	}
	if len(pv.Spec.AccessModes) == 0 {
		return false
	}
	for _, accessMode := range pv.Spec.AccessModes {
		if accessMode == v1.ReadWriteMany || accessMode == v1.ReadOnlyMany {
			if isSharedDiskEabled {
				if *pv.Spec.VolumeMode != v1.PersistentVolumeBlock {
					return true
				}
			} else {
				return true
			}
		}
	}
	return false
}

// initVolumeMigrationService is a helper method to initialize
// volumeMigrationService in Syncer.
func initVolumeMigrationService(ctx context.Context, metadataSyncer *metadataSyncInformer) error {
	log := logger.GetLogger(ctx)
	// This check prevents unnecessary RLocks on the volumeMigration instance.
	if volumeMigrationService != nil {
		return nil
	}
	var err error
	var volManager volumes.Manager

	if len(metadataSyncer.configInfo.Cfg.VirtualCenter) > 1 {
		// Migration feature switch is enabled and multi vCenter feature is enabled, and
		// Kubernetes Cluster is spread on multiple vCenter Servers.
		return logger.LogNewErrorf(log,
			"volume-migration feature is not supported on Multi-vCenter deployment")
	}

	// It is a single VC setup with Multi VC FSS enabled, we need to pick up the one and only volume manager in inventory.
	vCenter := metadataSyncer.configInfo.Cfg.Global.VCenterIP
	cnsVolumeMgr, volMgrFound := metadataSyncer.volumeManagers[vCenter]
	if !volMgrFound {
		return logger.LogNewErrorf(log, "could not get volume manager for the vCenter: %q", vCenter)
	}
	volManager = cnsVolumeMgr

	volumeMigrationService, err = migration.GetVolumeMigrationService(ctx,
		&volManager, metadataSyncer.configInfo.Cfg, true)
	if err != nil {
		log.Errorf("failed to get migration service. Err: %v", err)
		return err
	}
	return nil
}

// getConfig is a wrapper function in syncer container to get the
// config from vSphere Config Secret. If cluster ID is not provided
// in the secret, then we read it from the immutable ConfigMap
// which was created during csi controller initialization.
func getConfig(ctx context.Context) (*cnsconfig.Config, error) {
	var clusterID string
	log := logger.GetLogger(ctx)

	cfg, err := cnsconfig.GetConfig(ctx)
	if err != nil {
		log.Errorf("failed to read config. Error: %+v", err)
		return nil, err
	}
	CSINamespace := common.GetCSINamespace()
	if cfg.Global.ClusterID == "" {
		cmData, err := commonco.ContainerOrchestratorUtility.GetConfigMap(ctx,
			cnsconfig.ClusterIDConfigMapName, CSINamespace)
		if err == nil {
			// Get the clusterID value stored in the existing immutable ConfigMap.
			clusterID = cmData["clusterID"]
			log.Infof("Cluster ID value read from the ConfigMap is %s", clusterID)
		} else {
			return nil, logger.LogNewErrorf(log, "cluster ID is not available in "+
				"vSphere config secret and in immutable ConfigMap")
		}
		cfg.Global.ClusterID = clusterID
		cnsconfig.GeneratedVanillaClusterID = clusterID
	} else {
		if _, err := commonco.ContainerOrchestratorUtility.GetConfigMap(ctx,
			cnsconfig.ClusterIDConfigMapName, CSINamespace); err == nil {
			return nil, logger.LogNewErrorf(log, "Cluster ID is present in vSphere Config Secret "+
				"as well as in %s ConfigMap. Please remove the cluster ID from vSphere Config "+
				"Secret.", cnsconfig.ClusterIDConfigMapName)
		}
	}
	return cfg, nil
}

// SyncerInitConfigInfo initializes the ConfigurationInfo struct
func SyncerInitConfigInfo(ctx context.Context) (*cnsconfig.ConfigurationInfo, error) {
	log := logger.GetLogger(ctx)
	cfg, err := getConfig(ctx)
	if err != nil {
		log.Errorf("failed to read config. Error: %+v", err)
		return nil, err
	}
	configInfo := &cnsconfig.ConfigurationInfo{
		Cfg: cfg,
	}
	return configInfo, nil
}

// getVcHostAndVolumeManagerForVolumeID returns VC host and the corresponding
// volume manager that can access the given volume on VC.
// In case of a single VC setup, we simply return
// the metadataSyncer's host and volumeManager fields.
func getVcHostAndVolumeManagerForVolumeID(ctx context.Context,
	metadataSyncer *metadataSyncInformer,
	volumeID string) (string, volumes.Manager, error) {
	log := logger.GetLogger(ctx)
	log.Debugf("Getting VC from in-memory map for volume %s", volumeID)

	if len(metadataSyncer.configInfo.Cfg.VirtualCenter) == 1 {
		vCenter := metadataSyncer.configInfo.Cfg.Global.VCenterIP
		cnsVolumeMgr, volMgrFound := metadataSyncer.volumeManagers[vCenter]
		if !volMgrFound {
			return "", nil, logger.LogNewErrorf(log,
				"could not get volume manager for the vCenter: %q", vCenter)
		}
		log.Debugf("Identified VC %s for single VC setup for volume %s", vCenter, volumeID)

		return vCenter, cnsVolumeMgr, nil
	}

	if volumeInfoService != nil {
		vCenter, err := volumeInfoService.GetvCenterForVolumeID(ctx, volumeID)
		if err != nil {
			log.Errorf("failed to get vCenter for the volumeID: %q with err=%+v", volumeID, err)
			return "", nil, logger.LogNewErrorf(log,
				"failed to get vCenter for the volumeID: %q with err=%+v", volumeID, err)
		}
		volumeManager, volumeManagerfound := metadataSyncer.volumeManagers[vCenter]
		if !volumeManagerfound {
			return "", nil, logger.LogNewErrorf(log,
				"could not get volume manager for the vCenter: %q", vCenter)
		}

		log.Debugf("Identified VC %s for multi VC setup for volume %s", vCenter, volumeID)
		return vCenter, volumeManager, nil
	}

	return "", nil, logger.LogNewErrorf(log,
		"failed to get VC host and volume manager. VolumeInfoService is not initialized.")
}

// getTopologySegmentsFromNodeAffinityRules prepares a list of topology segments from the
// nodeAffinity rules defined on the PV.
func getTopologySegmentsFromNodeAffinityRules(ctx context.Context,
	pv *v1.PersistentVolume) []map[string][]string {
	log := logger.GetLogger(ctx)
	topologySegments := make([]map[string][]string, 0)

	if pv.Spec.NodeAffinity != nil {
		if pv.Spec.NodeAffinity.Required != nil {
			if pv.Spec.NodeAffinity.Required.NodeSelectorTerms != nil {
				for _, nodeSelector := range pv.Spec.NodeAffinity.Required.NodeSelectorTerms {
					if nodeSelector.MatchExpressions == nil {
						continue
					}
					// Get topology segments on PV
					currentTopoSegments := make(map[string][]string)
					for _, topology := range nodeSelector.MatchExpressions {
						currentTopoSegments[topology.Key] = append(currentTopoSegments[topology.Key], topology.Values...)
					}
					topologySegments = append(topologySegments, currentTopoSegments)
				}
			}
		}
	}

	log.Debugf("Consolidated topology segments: %+v", topologySegments)
	return topologySegments
}

// Based on the topology segments, this function locates
// the right VC for the given volume. If overlapping topology segments are found in nodeAffinity rules,
// error is returned.
func getVcHostFromTopologySegments(ctx context.Context, topologySegments []map[string][]string,
	volumeName string) (string, error) {
	log := logger.GetLogger(ctx)
	var vcHost string

	if len(topologySegments) == 0 {
		return "", logger.LogNewErrorf(log,
			"Invalid volume %s as it does not have node affinity rules", volumeName)
	}

	for _, topology := range topologySegments {

		vc, err := getVCForTopologySegments(ctx, topology)
		if err != nil {
			return "", logger.LogNewErrorf(log,
				"failed to get VC host and volume manager. Error %+v.", err)
		}

		if vcHost != "" && vcHost != vc {
			return "", logger.LogNewErrorf(log,
				"Found topology segments from 2 different VCs %s and %s."+
					"Error %+v.", vcHost, vc, err)
		}
		vcHost = vc
	}

	log.Debugf("Identified VC %s from topology segments for volume %s", vcHost, volumeName)

	return vcHost, nil
}

// Given a VC, this method returns the volume manager for it to invoke CNS APIs.
func getVolManagerForVcHost(ctx context.Context, vc string,
	metadataSyncer *metadataSyncInformer) (volumes.Manager, error) {
	log := logger.GetLogger(ctx)

	cnsVolumeMgr, volMgrFound := metadataSyncer.volumeManagers[vc]
	if !volMgrFound {
		return nil, logger.LogNewErrorf(log,
			"could not get volume manager for the vCenter: %q", vc)
	}

	return cnsVolumeMgr, nil
}

// getVcHostAndVolumeManagerFromPvNodeAffinity returns VC host and the corresponding
// volume manager that can access the given volume on VC based on the nodeAffinity rules.
func getVcHostAndVolumeManagerFromPvNodeAffinity(ctx context.Context, pv *v1.PersistentVolume,
	metadataSyncer *metadataSyncInformer) (string, volumes.Manager, error) {
	log := logger.GetLogger(ctx)

	log.Debugf("Getting VC from topology segments for volume %s", pv.Name)

	topologySegments := getTopologySegmentsFromNodeAffinityRules(ctx, pv)
	vcHost, err := getVcHostFromTopologySegments(ctx, topologySegments, pv.Name)
	if err != nil {
		return "", nil, err
	}

	volManager, err := getVolManagerForVcHost(ctx, vcHost, metadataSyncer)
	if err != nil {
		return "", nil, err
	}

	return vcHost, volManager, nil
}

// getPVsInBoundAvailableOrReleasedForVc sends back all K8s volumes in "Bound", "Available"
// or "Released" states, associated with the given VC.
// In case of a multi VC setup, it will also filter out all the
// in-tree PVs as well as file share volumes.
// For all K8s volumes, the corresponding VC is looked up from the in-memory map.
// In case this info is not available, it is obtained from PV's nodeAffinity rules.
func getPVsInBoundAvailableOrReleasedForVc(ctx context.Context, metadataSyncer *metadataSyncInformer,
	vc string) ([]*v1.PersistentVolume, error) {

	log := logger.GetLogger(ctx)
	k8svolumes := make([]*v1.PersistentVolume, 0)

	// get all K8s volumes in "Bound", "Available" or "Released" states.
	allPvs, err := getPVsInBoundAvailableOrReleased(ctx, metadataSyncer)
	if err != nil {
		return nil, err
	}

	// For a single VC setup, send back all volumes.
	if len(metadataSyncer.configInfo.Cfg.VirtualCenter) == 1 {
		return allPvs, nil
	}

	// PVs for which VC could not be found from in-memory map.
	leftOutPvs := make([]*v1.PersistentVolume, 0)

	for _, pv := range allPvs {
		// Check if the PV is an in-tree volume.
		if pv.Spec.CSI == nil {
			if metadataSyncer.coCommonInterface.IsFSSEnabled(ctx, common.CSIMigration) &&
				pv.Spec.VsphereVolume != nil {
				return nil, logger.LogNewErrorf(log,
					"In-tree volumes are not supported on a multi VC set up."+
						"Found in-tree volume %q.", pv.Name)
			}
			return nil, logger.LogNewErrorf(log,
				"Invalid PV %s with empty volume handle.", pv.Name)
		}

		// Check if the PV is a file share volume.
		if IsFileVolume(pv) {
			isTopologyAwareFileVolumeEnabled := metadataSyncer.coCommonInterface.IsFSSEnabled(ctx,
				common.TopologyAwareFileVolume)
			if !isTopologyAwareFileVolumeEnabled {
				return nil, logger.LogNewErrorf(log,
					"File share volumes are not supported on a multi VC set up."+
						"Found file share volume %s.", pv.Name)
			}
		}

		if volumeInfoService == nil {
			return nil, logger.LogNewErrorf(log, "VolumeInfoService is not initialized.")
		}

		volumeID := pv.Spec.CSI.VolumeHandle
		// Look up VC from in-memory map.
		vCenter, err := volumeInfoService.GetvCenterForVolumeID(ctx, volumeID)
		if err != nil {
			log.Errorf("failed to get vCenter for the volumeID: %q with err=%+v", volumeID, err)
			leftOutPvs = append(leftOutPvs, pv)
			continue
		}

		if vCenter == vc {
			k8svolumes = append(k8svolumes, pv)
		}
	}

	if len(leftOutPvs) != 0 {
		for _, volume := range leftOutPvs {
			if !IsFileVolume(volume) {
				// Try to locate the VC for all the left out PVs from their nodeAffinity rules.
				topologySegments := getTopologySegmentsFromNodeAffinityRules(ctx, volume)
				vCenter, err := getVcHostFromTopologySegments(ctx, topologySegments, volume.Name)
				if err != nil {
					log.Debugf("Failed to find which VC volume %+v belongs to from nodeAffinityRules",
						volume.Spec.CSI.VolumeHandle)
					continue
				}

				if vCenter == vc {
					k8svolumes = append(k8svolumes, volume)
				}
			}

		}
	}

	k8svolumeIDs := make([]string, 0)
	for _, volume := range k8svolumes {
		k8svolumeIDs = append(k8svolumeIDs, volume.Spec.CSI.VolumeHandle)
	}
	log.Debugf("List of K8s volumes for VC %s: %+v", vc, k8svolumeIDs)

	return k8svolumes, nil
}

// createVolumeOnMultiVc attempts to create a static volume on each VC until it gets SUCCESS.
// If while creating the volume, CNS returns CnsAlreadyRegisteredFault,
// it means that the volume does not need to be re-created.
func createVolumeOnMultiVc(ctx context.Context, pv *v1.PersistentVolume,
	metadataSyncer *metadataSyncInformer, volumeType string, metadataList []cnstypes.BaseCnsEntityMetadata,
	volumeHandle string) (string, volumes.Manager, error) {
	log := logger.GetLogger(ctx)

	vcconfigs, err := cnsvsphere.GetVirtualCenterConfigs(ctx, metadataSyncer.configInfo.Cfg)
	if err != nil {
		return "", nil, logger.LogNewErrorf(log, "failed to get VirtualCenterConfigs. err: %v", err)
	}

	for _, vcconfig := range vcconfigs {
		log.Debugf("Attempting to create volume on VC %s", vcconfig.Host)
		volManager, err := getVolManagerForVcHost(ctx, vcconfig.Host, metadataSyncer)
		if err != nil {
			continue
		}
		err = createCnsVolume(ctx, pv, metadataSyncer, volManager, volumeType, vcconfig.Host, metadataList, volumeHandle)
		if err == nil {
			return vcconfig.Host, volManager, nil
		}
		log.Debugf("Failed to create volume %q on VC %s", volumeHandle, vcconfig.Host)
	}
	return "", nil, logger.LogNewErrorf(log,
		"Failed to create volume %s on any of the VCs", volumeHandle)
}

func generateEventOnPv(ctx context.Context, pv *v1.PersistentVolume,
	eventType string, failureReason string, errorMsg string) {
	log := logger.GetLogger(ctx)

	eventBroadcaster := record.NewBroadcaster()
	k8sClient, err := k8s.NewClient(ctx)
	if err != nil {
		log.Errorf("Failed to create k8s client. Err: %v", err)
		return
	}

	eventBroadcaster.StartRecordingToSink(&typedcorev1.EventSinkImpl{Interface: k8sClient.CoreV1().Events("")})
	eventRecorder := eventBroadcaster.NewRecorder(scheme.Scheme, v1.EventSource{Component: syncerComponent})
	eventRecorder.Event(pv, eventType, failureReason, errorMsg)
}

func createCnsVolume(ctx context.Context, pv *v1.PersistentVolume,
	metadataSyncer *metadataSyncInformer, cnsVolumeMgr volumes.Manager, volumeType string,
	vcHost string, metadataList []cnstypes.BaseCnsEntityMetadata, volumeHandle string) error {
	log := logger.GetLogger(ctx)

	vcHostObj, vcHostObjFound := metadataSyncer.configInfo.Cfg.VirtualCenter[vcHost]
	if !vcHostObjFound {
		return logger.LogNewErrorf(log,
			"Failed to find VC host for given volume: %q.", volumeHandle)
	}

	containerCluster := cnsvsphere.GetContainerCluster(clusterIDforVolumeMetadata,
		vcHostObj.User, metadataSyncer.clusterFlavor,
		metadataSyncer.configInfo.Cfg.Global.ClusterDistribution)

	createSpec := &cnstypes.CnsVolumeCreateSpec{
		Name:       pv.Name,
		VolumeType: volumeType,
		Metadata: cnstypes.CnsVolumeMetadata{
			ContainerCluster:      containerCluster,
			ContainerClusterArray: []cnstypes.CnsContainerCluster{containerCluster},
			EntityMetadata:        metadataList,
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
	log.Debugf("vSphere CSI Driver is creating volume %q with create spec %+v",
		pv.Name, spew.Sdump(createSpec))
	_, _, err := cnsVolumeMgr.CreateVolume(ctx, createSpec, nil)
	if err != nil {
		log.Errorf("Failed to create disk %s with error %+v", pv.Name, err)
		return err
	} else {
		log.Infof("vSphere CSI Driver has successfully marked volume: %q as the container volume.",
			pv.Spec.CSI.VolumeHandle)
		if len(metadataSyncer.configInfo.Cfg.VirtualCenter) > 1 {
			// Create CNSVolumeInfo CR for the volume ID.
			err = volumeInfoService.CreateVolumeInfo(ctx, pv.Spec.CSI.VolumeHandle, vcHost)
			if err != nil {
				log.Errorf("Failed to store volumeID %q for vCenter %q in CNSVolumeInfo CR. Error: %+v",
					pv.Spec.CSI.VolumeHandle, vcHost, err)
				return err
			}
		}
	}
	return nil
}

func createMissingFileVolumeInfoCrs(ctx context.Context, metadataSyncer *metadataSyncInformer) {

	log := logger.GetLogger(ctx)

	// Get all K8s volumes in "Bound", "Available" or "Released" states.
	allPvs, err := getPVsInBoundAvailableOrReleased(ctx, metadataSyncer)
	if err != nil {
		log.Errorf("Failed to get PVs in the cluster. Err: %v", err)
		return
	}

	fileVolumes := make([]*v1.PersistentVolume, 0)
	for _, pv := range allPvs {
		if pv.Spec.CSI == nil {
			if metadataSyncer.coCommonInterface.IsFSSEnabled(ctx, common.CSIMigration) &&
				pv.Spec.VsphereVolume != nil {
				log.Errorf("In-tree volumes are not supported on a multi VC set up."+
					"Found in-tree volume %s.", pv.Name)
				return
			}
			log.Errorf("Invalid PV %s with empty volume handle.", pv.Name)
			return
		}
		// Check if the PV is a file volume.
		if IsFileVolume(pv) {
			fileVolumes = append(fileVolumes, pv)
		}
	}

	if len(fileVolumes) == 0 {
		log.Debugf("There are no file volumes on the cluster")
		return
	}

	fileVolumesWithMissingCrs := make([]*v1.PersistentVolume, 0)
	if volumeInfoService == nil {
		log.Errorf("VolumeInfoService is not initialized")
		return
	}

	for _, pv := range fileVolumes {
		crExists, err := volumeInfoService.VolumeInfoCrExistsForVolume(ctx, pv.Spec.CSI.VolumeHandle)
		if err != nil {
			log.Errorf("Failed to find VolumeInfo CR for volume %s."+
				"Error: %+v", pv.Spec.CSI.VolumeHandle, err)
			continue
		}
		// Create VolumeInfo CR if not found.
		if !crExists {
			fileVolumesWithMissingCrs = append(fileVolumesWithMissingCrs, pv)
		}
	}

	if len(fileVolumesWithMissingCrs) == 0 {
		log.Debugf("There are no missing volume info CRs for file volumes")
		return
	}

	// This is a best effort attempt.
	// If some of the PVs could not be created, it can be attempted in the next cycle.
	for _, pv := range fileVolumesWithMissingCrs {

		var metadataList []cnstypes.BaseCnsEntityMetadata
		pvMetadata := cnsvsphere.GetCnsKubernetesEntityMetaData(pv.Name, pv.GetLabels(), false,
			string(cnstypes.CnsKubernetesEntityTypePV), "", clusterIDforVolumeMetadata, nil)
		metadataList = append(metadataList, cnstypes.BaseCnsEntityMetadata(pvMetadata))

		_, _, err = createVolumeOnMultiVc(ctx, pv, metadataSyncer,
			common.FileVolumeType, metadataList, pv.Spec.CSI.VolumeHandle)
		if err == nil {
			if !isDynamicallyCreatedVolume(ctx, pv) {
				generateEventOnPv(ctx, pv, v1.EventTypeNormal,
					staticVolumeProvisioningSuccessReason, staticVolumeProvisioningSuccessMessage)
			}
		}
	}
}

func isDynamicallyCreatedVolume(ctx context.Context, pv *v1.PersistentVolume) bool {
	isdynamicCSIPV := false
	if pv.Spec.CSI != nil {
		_, isdynamicCSIPV = pv.Spec.CSI.VolumeAttributes[attribCSIProvisionerID]
	}
	return isdynamicCSIPV
}

func getPatchData(oldObj, newObj interface{}) ([]byte, error) {
	oldData, err := json.Marshal(oldObj)
	if err != nil {
		return nil, fmt.Errorf("marshal old object failed: %v", err)
	}
	newData, err := json.Marshal(newObj)
	if err != nil {
		return nil, fmt.Errorf("marshal new object failed: %v", err)
	}
	patchBytes, err := jsonpatch.CreateMergePatch(oldData, newData)
	if err != nil {
		return nil, fmt.Errorf("CreateMergePatch failed: %v", err)
	}
	return patchBytes, nil
}

// PatchStoragePolicyUsage patches the StoragePolicyUsage CR based on old and new objects
func PatchStoragePolicyUsage(ctx context.Context, cnsOperatorClient client.Client,
	oldObj *storagepolicyusagev1alpha2.StoragePolicyUsage,
	newObj *storagepolicyusagev1alpha2.StoragePolicyUsage) error {
	log := logger.GetLogger(ctx)
	patch, err := getPatchData(oldObj, newObj)
	if err != nil {
		log.Errorf("error fetching PatchData StoragePolicyUsage CR. err: %v", err)
		return err
	}
	rawPatch := client.RawPatch(apitypes.MergePatchType, patch)
	// Try to patch StoragePolicyUsage CR for allowedRetries times
	allowedRetries := allowedRetriesToPatchStoragePolicyUsage
	attempt := 0
	for {
		attempt++
		err = cnsOperatorClient.Status().Patch(ctx, oldObj, rawPatch)
		if err != nil && attempt >= allowedRetries {
			log.Errorf("failed to patch StoragePolicyUsage instance %q on namespace %q, Error: %+v",
				oldObj.Name, oldObj.Namespace, err)
			return err
		} else if err == nil {
			log.Debugf("Successfully patched StoragePolicyUsage instance %q on namespace %q",
				oldObj.Name, oldObj.Namespace)
			return nil
		}
		log.Warnf("attempt %d, failed to patch StoragePolicyUsage instance %q on namespace %q with error %+v, "+
			"will retry...", attempt, oldObj.Name, oldObj.Namespace, err)
		time.Sleep(100 * time.Millisecond)
	}
}

// addResourceVersion sets the resource version for the patch obj
func addResourceVersion(patchBytes []byte, resourceVersion string) ([]byte, error) {
	var patchMap map[string]interface{}
	err := json.Unmarshal(patchBytes, &patchMap)
	if err != nil {
		return nil, fmt.Errorf("error unmarshalling patch: %v", err)
	}
	u := unstructured.Unstructured{Object: patchMap}
	a, err := meta.Accessor(&u)
	if err != nil {
		return nil, fmt.Errorf("error creating accessor: %v", err)
	}
	a.SetResourceVersion(resourceVersion)
	versionBytes, err := json.Marshal(patchMap)
	if err != nil {
		return nil, fmt.Errorf("error marshalling json patch: %v", err)
	}
	return versionBytes, nil
}

// hasClusterDistributionSet checks volume information obtained from CNS queryvolume API and check
// expected cluster distribution is set for specific clusterID in ContainerClusterArray associated with volume
// for Volumes Created from TKG Cluster, QueryVolume returns two entries for containerClusterArray
// one for clusterFlavor: "WORKLOAD" and clusterDistribution "SupervisorCluster" and
// another for clusterFlavor: "GUEST_CLUSTER" clusterDistribution: "TKGService"
func hasClusterDistributionSet(ctx context.Context, volume cnstypes.CnsVolume,
	clusterIDforVolumeMetadata string, expectedClusterDistribution string) bool {
	log := logger.GetLogger(ctx)
	for _, containerCluster := range volume.Metadata.ContainerClusterArray {
		if clusterIDforVolumeMetadata == containerCluster.ClusterId &&
			containerCluster.ClusterDistribution == expectedClusterDistribution {
			log.Debugf("Volume %s has cluster distribution set to %s",
				volume.Name, containerCluster.ClusterDistribution)
			return true
		}
	}
	return false
}

// generateVolumeAccessibleTopologyFromPVCAnnotation returns accessible topologies generated using
// PVC annotation "csi.vsphere.volume-accessible-topology".
func generateVolumeAccessibleTopologyFromPVCAnnotation(claim *v1.PersistentVolumeClaim) (
	[]map[string]string, error) {
	volumeAccessibleTopology := claim.Annotations[common.AnnVolumeAccessibleTopology]
	if volumeAccessibleTopology == "" {
		return nil, fmt.Errorf("annotation %q is not set for the claim: %q, namespace: %q",
			common.AnnVolumeAccessibleTopology, claim.Name, claim.Namespace)
	}
	volumeAccessibleTopologyArray := make([]map[string]string, 0)
	err := json.Unmarshal([]byte(volumeAccessibleTopology), &volumeAccessibleTopologyArray)
	if err != nil {
		return nil, fmt.Errorf("failed to parse annotation: %q value %v from the claim: %q, namespace: %q. "+
			"err: %v", common.AnnVolumeAccessibleTopology, volumeAccessibleTopology,
			claim.Name, claim.Namespace, err)
	}
	return volumeAccessibleTopologyArray, nil
}
