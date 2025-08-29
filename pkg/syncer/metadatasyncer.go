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
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/davecgh/go-spew/spew"
	"github.com/fsnotify/fsnotify"
	"github.com/go-co-op/gocron"
	cnstypes "github.com/vmware/govmomi/cns/types"
	v1 "k8s.io/api/core/v1"
	apiextensionsclientset "k8s.io/apiextensions-apiserver/pkg/client/clientset/clientset"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/runtime"
	k8stypes "k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/client-go/informers"
	clientset "k8s.io/client-go/kubernetes"
	restclient "k8s.io/client-go/rest"
	"k8s.io/client-go/tools/cache"
	"k8s.io/client-go/util/workqueue"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/config"

	"github.com/go-logr/zapr"
	cr_log "sigs.k8s.io/controller-runtime/pkg/log"
	cnsoperatorv1alpha1 "sigs.k8s.io/vsphere-csi-driver/v3/pkg/apis/cnsoperator"
	storagepolicyv1alpha2 "sigs.k8s.io/vsphere-csi-driver/v3/pkg/apis/cnsoperator/storagepolicy/v1alpha2"
	sqperiodicsyncv1alpha1 "sigs.k8s.io/vsphere-csi-driver/v3/pkg/apis/cnsoperator/storagequotaperiodicsync/v1alpha1"
	"sigs.k8s.io/vsphere-csi-driver/v3/pkg/apis/migration"
	"sigs.k8s.io/vsphere-csi-driver/v3/pkg/common/cns-lib/node"
	volumes "sigs.k8s.io/vsphere-csi-driver/v3/pkg/common/cns-lib/volume"
	cnsvsphere "sigs.k8s.io/vsphere-csi-driver/v3/pkg/common/cns-lib/vsphere"
	cnsconfig "sigs.k8s.io/vsphere-csi-driver/v3/pkg/common/config"
	"sigs.k8s.io/vsphere-csi-driver/v3/pkg/common/utils"
	"sigs.k8s.io/vsphere-csi-driver/v3/pkg/csi/service/common"
	"sigs.k8s.io/vsphere-csi-driver/v3/pkg/csi/service/common/commonco"
	"sigs.k8s.io/vsphere-csi-driver/v3/pkg/csi/service/common/commonco/k8sorchestrator"
	commoncotypes "sigs.k8s.io/vsphere-csi-driver/v3/pkg/csi/service/common/commonco/types"
	"sigs.k8s.io/vsphere-csi-driver/v3/pkg/csi/service/logger"
	csitypes "sigs.k8s.io/vsphere-csi-driver/v3/pkg/csi/types"
	cnsfilevolumeclientv1alpha1 "sigs.k8s.io/vsphere-csi-driver/v3/pkg/internalapis/cnsoperator/cnsfilevolumeclient/v1alpha1"
	triggercsifullsyncv1alpha1 "sigs.k8s.io/vsphere-csi-driver/v3/pkg/internalapis/cnsoperator/triggercsifullsync/v1alpha1"
	"sigs.k8s.io/vsphere-csi-driver/v3/pkg/internalapis/cnsvolumeinfo"
	cnsvolumeinfov1alpha1 "sigs.k8s.io/vsphere-csi-driver/v3/pkg/internalapis/cnsvolumeinfo/v1alpha1"
	"sigs.k8s.io/vsphere-csi-driver/v3/pkg/internalapis/cnsvolumeoperationrequest"
	cnsvolumeoperationrequestv1alpha1 "sigs.k8s.io/vsphere-csi-driver/v3/pkg/internalapis/cnsvolumeoperationrequest/v1alpha1"
	"sigs.k8s.io/vsphere-csi-driver/v3/pkg/internalapis/csinodetopology"
	csinodetopologyv1alpha1 "sigs.k8s.io/vsphere-csi-driver/v3/pkg/internalapis/csinodetopology/v1alpha1"
	"sigs.k8s.io/vsphere-csi-driver/v3/pkg/internalapis/featurestates"
	k8s "sigs.k8s.io/vsphere-csi-driver/v3/pkg/kubernetes"
	cnsoperatortypes "sigs.k8s.io/vsphere-csi-driver/v3/pkg/syncer/cnsoperator/types"
	"sigs.k8s.io/vsphere-csi-driver/v3/pkg/syncer/storagepool"
)

var (
	// volumeMigrationService holds the pointer to VolumeMigration instance.
	volumeMigrationService migration.VolumeMigrationService
	// volumeInfoService holds the pointer to VolumeInfo instance.
	volumeInfoService cnsvolumeinfo.VolumeInfoService
	// volumeTopologyService holds the pointer to ControllerTopologyService instance.
	volumeTopologyService commoncotypes.ControllerTopologyService
	// COInitParams stores the input params required for initiating the
	// CO agnostic orchestrator for the syncer container.
	COInitParams interface{}

	// MetadataSyncer instance for the syncer container.
	MetadataSyncer *metadataSyncInformer

	// Contains list of clusterComputeResourceMoIds on which supervisor cluster is deployed.
	clusterComputeResourceMoIds = make([]string, 0)
	clusterIDforVolumeMetadata  string

	// isSharedDiskEabled is true if shared disks are supported on the supervisor cluster
	isSharedDiskEabled bool

	//IsMigrationEnabled is true when in-tree to CSI Migration FSS is enabled for the driver, false otherwise.
	IsMigrationEnabled bool
	// nodeMgr stores the manager to interact with nodeVMs.
	nodeMgr node.Manager
	// IsPodVMOnStretchSupervisorFSSEnabled is true when PodVMOnStretchedSupervisor FSS is enabled.
	IsPodVMOnStretchSupervisorFSSEnabled bool
	// IsLinkedCloneSupportFSSEnabled is true when linked-clone-support FSS is enabled.
	IsLinkedCloneSupportFSSEnabled bool
	// IsMultipleClustersPerVsphereZoneFSSEnabled is true when supports_multiple_clusters_per_zone FSS is enabled
	IsMultipleClustersPerVsphereZoneFSSEnabled bool
	// ResourceAPIgroupPVC is an empty string as PVC belongs to the core resource group denoted by `""`.
	ResourceAPIgroupPVC = ""

	// ResourceAPIgroupSnapshot is API group for volume snapshot
	ResourceAPIgroupSnapshot = "snapshot.storage.k8s.io"

	// isStorageQuotaM2FSSEnabled is true if the Snapshot Storage Quota feature is enabled, false otherwise.
	isStorageQuotaM2FSSEnabled bool

	// IsWorkloadDomainIsolationSupported is true when Workload_Domain_Isolation_Supported FSS is enabled.
	IsWorkloadDomainIsolationSupported bool

	// PeriodicSyncIntervalInMin the time interval to run sync for
	PeriodicSyncIntervalInMin time.Duration
)

const (
	ResourceKindPVC                      = "PersistentVolumeClaim"
	ResourceKindSnapshot                 = "VolumeSnapshot"
	PVCQuotaExtensionServiceName         = "volume.cns.vsphere.vmware.com"
	SnapQuotaExtensionServiceName        = "snapshot.cns.vsphere.vmware.com"
	VMServiceExtensionServiceName        = "vmware-system-vmop-webhook-service"
	scParamStoragePolicyID               = "storagePolicyID"
	StorageQuotaPeriodicSyncInstanceName = "storage-quota-periodic-sync"
	FileVolumePrefix                     = "file:"
)

// newInformer returns uninitialized metadataSyncInformer.
func newInformer() *metadataSyncInformer {
	return &metadataSyncInformer{}
}

// getFullSyncIntervalInMin returns the FullSyncInterval.
// If environment variable FULL_SYNC_INTERVAL_MINUTES is set and valid,
// return the interval value read from environment variable.
// Otherwise, use the default value 30 minutes.
func getFullSyncIntervalInMin(ctx context.Context) int {
	log := logger.GetLogger(ctx)
	fullSyncIntervalInMin := defaultFullSyncIntervalInMin
	if v := os.Getenv("FULL_SYNC_INTERVAL_MINUTES"); v != "" {
		if value, err := strconv.Atoi(v); err == nil {
			if value <= 0 {
				log.Warnf("FullSync: fullSync interval set in env variable FULL_SYNC_INTERVAL_MINUTES %s "+
					"is equal or less than 0, will use the default interval", v)
			} else if value > defaultFullSyncIntervalInMin {
				log.Warnf("FullSync: fullSync interval set in env variable FULL_SYNC_INTERVAL_MINUTES %s "+
					"is larger than max value can be set, will use the default interval", v)
			} else {
				fullSyncIntervalInMin = value
				log.Infof("FullSync: fullSync interval is set to %d minutes", fullSyncIntervalInMin)
			}
		} else {
			log.Warnf("FullSync: fullSync interval set in env variable FULL_SYNC_INTERVAL_MINUTES %s "+
				"is invalid, will use the default interval", v)
		}
	}
	return fullSyncIntervalInMin
}

// getVolumeHealthIntervalInMin returns the VolumeHealthInterval.
// If environment variable VOLUME_HEALTH_STATUS_INTERVAL_MINUTES is set and valid,
// return the interval value read from environment variable.
// Otherwise, use the default value 5 minutes.
func getVolumeHealthIntervalInMin(ctx context.Context) int {
	log := logger.GetLogger(ctx)
	volumeHealthIntervalInMin := defaultVolumeHealthIntervalInMin
	if v := os.Getenv("VOLUME_HEALTH_INTERVAL_MINUTES"); v != "" {
		if value, err := strconv.Atoi(v); err == nil {
			if value <= 0 {
				log.Warnf("VolumeHealth: VolumeHealth interval set in env variable VOLUME_HEALTH_INTERVAL_MINUTES %s "+
					"is equal or less than 0, will use the default interval", v)
			} else {
				volumeHealthIntervalInMin = value
				log.Infof("VolumeHealth: VolumeHealth interval is set to %d minutes", volumeHealthIntervalInMin)
			}
		} else {
			log.Warnf("VolumeHealth: VolumeHealth interval set in env variable VOLUME_HEALTH_INTERVAL_MINUTES %s "+
				"is invalid, will use the default interval", v)
		}
	}
	return volumeHealthIntervalInMin
}

// getPVtoBackingDiskObjectIdIntervalInMin returns pv to backingdiskobjectid interval.
func getPVtoBackingDiskObjectIdIntervalInMin(ctx context.Context) int {
	log := logger.GetLogger(ctx)
	pvtoBackingDiskObjectIdIntervalInMin := defaultPVtoBackingDiskObjectIdIntervalInMin
	if v := os.Getenv("PV_TO_BACKINGDISKOBJECTID_INTERVAL_MINUTES"); v != "" {
		if value, err := strconv.Atoi(v); err == nil {
			if value <= 0 {
				log.Warnf("PVtoBackingDiskObjectId: PVtoBackingDiskObjectId interval set in env variable "+
					"PV_TO_BACKINGDISKOBJECTID_INTERVAL_MINUTES %s is equal or less than 0, will use the "+
					"default interval", v)
			} else {
				pvtoBackingDiskObjectIdIntervalInMin = value
				log.Infof("PVtoBackingDiskObjectId: PVtoBackingDiskObjectId interval is set to %d minutes",
					pvtoBackingDiskObjectIdIntervalInMin)
			}
		} else {
			log.Warnf("PVtoBackingDiskObjectId: PVtoBackingDiskObjectId interval set in env variable "+
				"PV_TO_BACKINGDISKOBJECTID_INTERVAL_MINUTES %s is invalid, will use the default interval", v)
		}
	}
	return pvtoBackingDiskObjectIdIntervalInMin
}

// InitMetadataSyncer initializes the Metadata Sync Informer.
func InitMetadataSyncer(ctx context.Context, clusterFlavor cnstypes.CnsClusterFlavor,
	configInfo *cnsconfig.ConfigurationInfo) error {
	log := logger.GetLogger(ctx)
	cr_log.SetLogger(zapr.NewLogger(log.Desugar()))
	var err error
	log.Infof("Initializing MetadataSyncer")
	metadataSyncer := newInformer()
	MetadataSyncer = metadataSyncer
	metadataSyncer.configInfo = configInfo

	if clusterFlavor == cnstypes.CnsClusterFlavorVanilla {
		IsMigrationEnabled = commonco.ContainerOrchestratorUtility.IsFSSEnabled(ctx, common.CSIMigration)
	}
	isStorageQuotaM2FSSEnabled = commonco.ContainerOrchestratorUtility.IsFSSEnabled(ctx, common.StorageQuotaM2)
	// Create the kubernetes client from config.
	k8sClient, err := k8s.NewClient(ctx)
	if err != nil {
		log.Errorf("Creating Kubernetes client failed. Err: %v", err)
		return err
	}

	// Initialize the k8s orchestrator interface.
	metadataSyncer.coCommonInterface, err = commonco.GetContainerOrchestratorInterface(ctx,
		common.Kubernetes, clusterFlavor, COInitParams)
	if err != nil {
		log.Errorf("Failed to create CO agnostic interface. Error: %v", err)
		return err
	}
	metadataSyncer.clusterFlavor = clusterFlavor
	clusterIDforVolumeMetadata = configInfo.Cfg.Global.ClusterID
	if metadataSyncer.clusterFlavor == cnstypes.CnsClusterFlavorWorkload {
		isSharedDiskEabled = commonco.ContainerOrchestratorUtility.IsFSSEnabled(ctx, common.SharedDiskFss)
		if !configInfo.Cfg.Global.InsecureFlag && configInfo.Cfg.Global.CAFile != cnsconfig.SupervisorCAFilePath {
			log.Warnf("Invalid CA file: %q is set in the vSphere Config Secret. "+
				"Setting correct CA file: %q", configInfo.Cfg.Global.CAFile, cnsconfig.SupervisorCAFilePath)
			configInfo.Cfg.Global.CAFile = cnsconfig.SupervisorCAFilePath
		}
		if commonco.ContainerOrchestratorUtility.IsFSSEnabled(ctx, common.TKGsHA) {
			clusterComputeResourceMoIds, _, err = common.GetClusterComputeResourceMoIds(ctx)
			if err != nil {
				log.Errorf("failed to get clusterComputeResourceMoIds. err: %v", err)
				return err
			}
			if len(clusterComputeResourceMoIds) > 0 {
				if configInfo.Cfg.Global.SupervisorID == "" {
					return logger.LogNewError(log, "supervisor-id is not set in the vsphere-config-secret")
				}
			}
			clusterIDforVolumeMetadata = configInfo.Cfg.Global.SupervisorID
		}
		IsPodVMOnStretchSupervisorFSSEnabled = commonco.ContainerOrchestratorUtility.IsFSSEnabled(ctx,
			common.PodVMOnStretchedSupervisor)
		if IsPodVMOnStretchSupervisorFSSEnabled {
			// Start watching on nodes to create CSINodes, if not already present.
			err = commonco.ContainerOrchestratorUtility.InitializeCSINodes(ctx)
			if err != nil {
				return logger.LogNewErrorf(log, "failed to initialize CSINodes creation. Error: %+v", err)
			}
		}

		// Check if finalizer is added on CnsFileVolumeClient CRs, if not then add a finalizer.
		// We want to protect CnsFileVolumeClient from getting abruptly deleted, as it is being used
		// in CnsFileAccessConfig CR. So, in case of upgrade we will add finalizer if it is missing.
		err = addFinalizerOnCnsFileVolumeClientCRs(ctx)
		if err != nil {
			log.Errorf("Failed to add finalizer on CnsFileVolumeClient CRs. Error: %v", err)
			return err
		}

		// Currently we are checking if capability workload-domain-isolation is enabled or not.
		// If it is not enabled, then we are checking its value in capabilities CR after every 2 mins
		// and once it gets enabled, we restart the CSI syncer container on supervisor.
		// NOTE: We can add other capabilities here when similar functionality is required. For
		// workload-domain-isolation feature we are restarting the container when capability changes
		// dynamically from false to true, but for other features instead of restarting CSI container,
		// if possible we can implement some init() function which can initialize required things when
		// capability value changes from false to true.
		IsWorkloadDomainIsolationSupported = commonco.ContainerOrchestratorUtility.IsFSSEnabled(ctx,
			common.WorkloadDomainIsolation)
		if !IsWorkloadDomainIsolationSupported {
			// Workload_Domain_Isolation_Supported Capability is disabled
			go commonco.ContainerOrchestratorUtility.HandleLateEnablementOfCapability(ctx, clusterFlavor,
				common.WorkloadDomainIsolation,
				"", "")
		} else {
			// Workload_Domain_Isolation_Supported Capability is enabled, checking if FSS is set as enabled
			// This is required for backward compatibility of released TKR versions on newer version of supervisor
			// we are already enabling FSS in HandleLateEnablementOfCapability, this code block is required to cover a case
			// when capability gets enabled but container is not running or driver is installed with capability
			// already enabled on supervisor cluster
			IsWorkloadDomainIsolationFSSEnabled :=
				commonco.ContainerOrchestratorUtility.IsFSSEnabled(ctx, common.WorkloadDomainIsolationFSS)
			if !IsWorkloadDomainIsolationFSSEnabled {
				// if workload-domain-isolation FSS is not enabled in config-map, update config-map is set this FSS
				// as true
				err = commonco.ContainerOrchestratorUtility.EnableFSS(ctx, common.WorkloadDomainIsolationFSS)
				if err != nil {
					log.Errorf("failed to enable CNS-CSI FSS %q, err: %+v",
						common.WorkloadDomainIsolationFSS, err)
					os.Exit(1)
				}
			}
			volumeTopologyService, err = commonco.ContainerOrchestratorUtility.InitTopologyServiceInController(ctx)
			if err != nil {
				log.Errorf("failed to init topology manager. err: %v", err)
				return err
			}
			log.Infof("Successfully initialized Topology service in syncer")
		}
		IsLinkedCloneSupportFSSEnabled = commonco.ContainerOrchestratorUtility.IsFSSEnabled(ctx,
			common.LinkedCloneSupport)
		if !IsLinkedCloneSupportFSSEnabled {
			go commonco.ContainerOrchestratorUtility.HandleLateEnablementOfCapability(ctx,
				clusterFlavor, common.LinkedCloneSupport, "", "")
		}
		IsMultipleClustersPerVsphereZoneFSSEnabled = commonco.ContainerOrchestratorUtility.IsFSSEnabled(ctx,
			common.MultipleClustersPerVsphereZone)
		if !IsMultipleClustersPerVsphereZoneFSSEnabled {
			go commonco.ContainerOrchestratorUtility.HandleLateEnablementOfCapability(ctx, clusterFlavor,
				common.MultipleClustersPerVsphereZone, "", "")
		}
		if !commonco.ContainerOrchestratorUtility.IsFSSEnabled(ctx,
			common.StoragePolicyReservationSupport) {
			go commonco.ContainerOrchestratorUtility.HandleLateEnablementOfCapability(ctx, clusterFlavor,
				common.StoragePolicyReservationSupport, "", "")
		}
	}

	if metadataSyncer.clusterFlavor == cnstypes.CnsClusterFlavorGuest {
		// If workload-domain-isolation FSS is not enabled on guest cluster, then check the capabilities CR in
		// supervisor cluster every 2 mins to check if there is a change in Workload_Domain_Isolation_Supported
		// capability value from false to true. If so, restart the CSI controller container on guest.
		// NOTE: We can add other capabilities here when similar functionality is required. For
		// workload-isolation-domain feature we are restarting the container when capability changes dynamically from
		// false to true, but for other features instead of restarting CSI container, if possible we can implement
		// some init() function which can initialize required things when capability value changes from false to true.
		if !commonco.ContainerOrchestratorUtility.IsFSSEnabled(ctx, common.WorkloadDomainIsolationFSS) {
			go commonco.ContainerOrchestratorUtility.HandleLateEnablementOfCapability(ctx, clusterFlavor,
				common.WorkloadDomainIsolation,
				metadataSyncer.configInfo.Cfg.GC.Port, metadataSyncer.configInfo.Cfg.GC.Endpoint)
		}
		if !commonco.ContainerOrchestratorUtility.IsFSSEnabled(ctx, common.LinkedCloneSupportFSS) {
			go commonco.ContainerOrchestratorUtility.HandleLateEnablementOfCapability(ctx,
				clusterFlavor, common.LinkedCloneSupport,
				metadataSyncer.configInfo.Cfg.GC.Port, metadataSyncer.configInfo.Cfg.GC.Endpoint)
		}
	}

	// Initialize cnsDeletionMap used by Full Sync.
	cnsDeletionMap = make(map[string]map[string]bool)
	// Initialize cnsCreationMap used by Full Sync.
	cnsCreationMap = make(map[string]map[string]bool)
	// Initialize volumeOperationsLock map
	volumeOperationsLock = make(map[string]*sync.Mutex)
	// Initialize volumeInfoCrDeletionMap used by Full Sync.
	volumeInfoCrDeletionMap = make(map[string]map[string]bool)

	if metadataSyncer.clusterFlavor == cnstypes.CnsClusterFlavorGuest {
		// Initialize client to supervisor cluster, if metadata syncer is being
		// initialized for guest clusters.
		restClientConfig := k8s.GetRestClientConfigForSupervisor(ctx,
			metadataSyncer.configInfo.Cfg.GC.Endpoint, metadataSyncer.configInfo.Cfg.GC.Port)
		metadataSyncer.cnsOperatorClient, err = k8s.NewClientForGroup(ctx,
			restClientConfig, cnsoperatorv1alpha1.GroupName)
		if err != nil {
			log.Errorf("Creating Cns Operator client failed. Err: %v", err)
			return err
		}

		// Initialize supervisor cluser client.
		metadataSyncer.supervisorClient, err = k8s.NewSupervisorClient(ctx, restClientConfig)
		if err != nil {
			log.Errorf("Failed to create supervisorClient. Error: %+v", err)
			return err
		}
	} else if metadataSyncer.clusterFlavor == cnstypes.CnsClusterFlavorWorkload {
		// Initialize volume manager with vcenter credentials
		vCenter, err := cnsvsphere.GetVirtualCenterInstance(ctx, configInfo, false)
		if err != nil {
			return err
		}
		err = vCenter.ConnectCns(ctx)
		if err != nil {
			return logger.LogNewErrorf(log, "error while connecting cns. err=%v", err)
		}
		vCenter.Config.ReloadVCConfigForNewClient = true
		metadataSyncer.host = vCenter.Config.Host

		cnsDeletionMap[metadataSyncer.host] = make(map[string]bool)
		cnsCreationMap[metadataSyncer.host] = make(map[string]bool)
		volumeInfoCrDeletionMap[metadataSyncer.host] = make(map[string]bool)
		volumeOperationsLock[metadataSyncer.host] = &sync.Mutex{}

		volumeManager, err := volumes.GetManager(ctx, vCenter,
			nil, false, false, false, metadataSyncer.clusterFlavor)
		if err != nil {
			return logger.LogNewErrorf(log, "failed to create an instance of volume manager. err=%v", err)
		}
		metadataSyncer.volumeManager = volumeManager
		if metadataSyncer.coCommonInterface.IsFSSEnabled(ctx, common.CSISVFeatureStateReplication) {
			svParams, ok := COInitParams.(k8sorchestrator.K8sSupervisorInitParams)
			if !ok {
				return fmt.Errorf("expected orchestrator params of type K8sSupervisorInitParams, got %T instead",
					COInitParams)
			}
			go func() {
				err := featurestates.StartSvFSSReplicationService(ctx, svParams.SupervisorFeatureStatesConfigInfo.Name,
					svParams.SupervisorFeatureStatesConfigInfo.Namespace)
				if err != nil {
					log.Errorf("error starting supervisor FSS ReplicationService. Error: %+v", err)
					os.Exit(1)
				}
			}()
		}
		if IsPodVMOnStretchSupervisorFSSEnabled {
			log.Info("Loading CnsVolumeInfo Service to persist mapping for VolumeID to storage policy info")
			volumeInfoService, err = cnsvolumeinfo.InitVolumeInfoService(ctx)
			if err != nil {
				return logger.LogNewErrorf(log, "error initializing volumeInfoService. Error: %+v", err)
			}
			if volumeInfoService != nil {
				log.Infof("Successfully initialized VolumeInfoService")
			}
			k8sConfig, err := k8s.GetKubeConfig(ctx)
			if err != nil {
				return logger.LogNewErrorf(log, "failed to get kubeconfig with error: %v", err)
			}
			err = initCnsVolumeOperationRequestCRInformer(ctx, k8sConfig)
			if err != nil {
				return logger.LogNewErrorf(log, "failed to start CnsVolumeOperationRequest informer on %q instances. Error: %v",
					cnsvolumeoperationrequest.CRDSingular, err)
			}
			isStorageQuotaM2Enabled := metadataSyncer.coCommonInterface.IsFSSEnabled(ctx, common.StorageQuotaM2)
			if isStorageQuotaM2Enabled {
				// Create informer to watch on update events of CnsVolumeInfo CRs
				cnsVolumeInfoCRInformerErr := startCnsVolumeInfoCRInformer(ctx, k8sConfig, metadataSyncer)
				if cnsVolumeInfoCRInformerErr != nil {
					log.Errorf("failed to start informer on %q instances. Error: %v",
						cnsvolumeinfov1alpha1.CnsVolumeInfoSingular, cnsVolumeInfoCRInformerErr)
					os.Exit(1)
				}
				// start periodic sync for storage quota
				err := initStorageQuotaPeriodicSync(ctx, metadataSyncer)
				if err != nil {
					log.Errorf("initStorageQuotaPeriodicSync: Failed to initialize the storagequota "+
						"periodic sync, with error Error: %v", err)
					os.Exit(1)
				}
			}
		}

	} else {
		// code block only applicable to Vanilla
		// Initialize volume manager with vcenter credentials for Vanilla flavor
		vcconfigs, err := cnsvsphere.GetVirtualCenterConfigs(ctx, configInfo.Cfg)
		if err != nil {
			return logger.LogNewErrorf(log, "failed to get VirtualCenterConfigs. err: %v", err)
		}
		metadataSyncer.volumeManagers = make(map[string]volumes.Manager)
		var multivCenterTopologyDeployment bool
		if len(vcconfigs) > 1 {
			multivCenterTopologyDeployment = true
		}
		for _, vcconfig := range vcconfigs {
			vcconfig.ReloadVCConfigForNewClient = true
			vCenter, err := cnsvsphere.GetVirtualCenterInstanceForVCenterConfig(ctx, vcconfig, false)
			if err != nil {
				return logger.LogNewErrorf(log, "failed to get vCenterInstance for vCenter Host: %q, err: %v",
					vcconfig.Host, err)
			}
			volumeManager, err := volumes.GetManager(ctx, vCenter,
				nil, false, true,
				multivCenterTopologyDeployment, metadataSyncer.clusterFlavor)
			if err != nil {
				return logger.LogNewErrorf(log, "failed to create an instance of volume manager. err=%v", err)
			}

			metadataSyncer.volumeManagers[vcconfig.Host] = volumeManager
			cnsDeletionMap[vcconfig.Host] = make(map[string]bool)
			cnsCreationMap[vcconfig.Host] = make(map[string]bool)
			volumeInfoCrDeletionMap[vcconfig.Host] = make(map[string]bool)
			volumeOperationsLock[vcconfig.Host] = &sync.Mutex{}
		}
		// If it is a multi VC deployment, initialize volumeInfoService
		if len(vcconfigs) > 1 && volumeInfoService == nil {
			volumeInfoService, err = cnsvolumeinfo.InitVolumeInfoService(ctx)
			if err != nil {
				return logger.LogNewErrorf(log, "error initializing volumeInfoService. Error: %+v", err)
			}
		}
		// Add informer on CSINodeTopology instances and update metadataSyncer.topologyVCMap parameter.
		nodeMgr = node.GetManager(ctx)
		k8sConfig, err := k8s.GetKubeConfig(ctx)
		if err != nil {
			return logger.LogNewErrorf(log, "failed to get kubeconfig with error: %v", err)
		}
		metadataSyncer.topologyVCMap = make(map[string]map[string]struct{})
		err = startTopologyCRInformer(ctx, k8sConfig)
		if err != nil {
			return logger.LogNewErrorf(log, "failed to start informer on %q instances. Error: %v",
				csinodetopology.CRDSingular, err)
		}
	}

	cfgPath := cnsconfig.GetConfigPath(ctx)
	watcher, err := fsnotify.NewWatcher()
	if err != nil {
		log.Errorf("failed to create fsnotify watcher. err=%v", err)
		return err
	}
	go func() {
		for {
			log.Debugf("Waiting for event on fsnotify watcher")
			select {
			case event, ok := <-watcher.Events:
				if !ok {
					return
				}
				log.Debugf("fsnotify event: %q", event.String())
				if event.Op&fsnotify.Remove == fsnotify.Remove {
					if clusterFlavor != cnstypes.CnsClusterFlavorWorkload &&
						clusterFlavor != cnstypes.CnsClusterFlavorGuest {
						// Only Vanilla Code block
						reloadConfigErr := ReloadConfiguration(metadataSyncer, false)
						if reloadConfigErr == nil {
							log.Infof("Successfully reloaded configuration from: %q", cfgPath)
						} else {
							log.Errorf("failed to reload configuration will retry again in 5 seconds. err: %+v", reloadConfigErr)
						}
					} else {
						for {
							reloadConfigErr := ReloadConfiguration(metadataSyncer, false)
							if reloadConfigErr == nil {
								log.Infof("Successfully reloaded configuration from: %q", cfgPath)
								break
							}
							log.Errorf("failed to reload configuration will retry again in 5 seconds. err: %+v", reloadConfigErr)
							time.Sleep(5 * time.Second)
						}
					}
				}
				// Handling create event for reconnecting to VC when ca file is
				// rotated. In Supervisor cluster, ca file gets rotated at the path
				// /etc/vmware/wcp/tls/vmca.pem. WCP handles ca file rotation by
				// creating a /etc/vmware/wcp/tls/vmca.pem.tmp file with new
				// contents, then rename it back to /etc/vmware/wcp/tls/vmca.pem.
				// For such operations, fsnotify handles the event as a CREATE
				// event. The conditions below also ensures that the event is for
				// the expected ca file path.
				if event.Op&fsnotify.Create == fsnotify.Create && event.Name == cnsconfig.SupervisorCAFilePath {
					for {
						reconnectVCErr := ReloadConfiguration(metadataSyncer, true)
						if reconnectVCErr == nil {
							log.Infof("Successfully re-established connection with VC from: %q",
								cnsconfig.SupervisorCAFilePath)
							break
						}
						log.Errorf("failed to re-establish VC connection. Will retry again in 60 seconds. err: %+v",
							reconnectVCErr)
						time.Sleep(60 * time.Second)
					}
				}
			case err, ok := <-watcher.Errors:
				if !ok {
					log.Errorf("fsnotify error: %+v", err)
					return
				}
			}
			log.Debugf("fsnotify event processed")
		}
	}()
	cfgDirPath := filepath.Dir(cfgPath)
	log.Infof("Adding watch on path: %q", cfgDirPath)
	err = watcher.Add(cfgDirPath)
	if err != nil {
		log.Errorf("failed to watch on path: %q. err=%v", cfgDirPath, err)
		return err
	}
	if metadataSyncer.clusterFlavor == cnstypes.CnsClusterFlavorWorkload {
		caFileDirPath := filepath.Dir(cnsconfig.SupervisorCAFilePath)
		log.Infof("Adding watch on path: %q", caFileDirPath)
		err = watcher.Add(caFileDirPath)
		if err != nil {
			log.Errorf("failed to watch on path: %q. err=%v", caFileDirPath, err)
			return err
		}
	}

	if metadataSyncer.clusterFlavor == cnstypes.CnsClusterFlavorGuest {
		log.Infof("Adding watch on path: %q", cnsconfig.DefaultpvCSIProviderPath)
		err = watcher.Add(cnsconfig.DefaultpvCSIProviderPath)
		if err != nil {
			log.Errorf("failed to watch on path: %q. err=%v", cnsconfig.DefaultpvCSIProviderPath, err)
			return err
		}
	}

	// Set up kubernetes resource listeners for metadata syncer.
	metadataSyncer.k8sInformerManager = k8s.NewInformer(ctx, k8sClient, true)
	err = metadataSyncer.k8sInformerManager.AddPVCListener(
		ctx,
		nil, // Add.
		func(oldObj interface{}, newObj interface{}) { // Update.
			pvcUpdated(oldObj, newObj, metadataSyncer)
		},
		func(obj interface{}) { // Delete.
			pvcDeleted(obj, metadataSyncer)
		})
	if err != nil {
		return logger.LogNewErrorf(log, "failed to listen on PVCs. Error: %v", err)
	}
	err = metadataSyncer.k8sInformerManager.AddPVListener(
		ctx,
		func(obj interface{}) {
			pvAdded(obj, metadataSyncer)
		}, // Add.
		func(oldObj interface{}, newObj interface{}) { // Update.
			pvUpdated(oldObj, newObj, metadataSyncer)
		},
		func(obj interface{}) { // Delete.
			pvDeleted(obj, metadataSyncer)
		})
	if err != nil {
		return logger.LogNewErrorf(log, "failed to listen on PVs. Error: %v", err)
	}
	err = metadataSyncer.k8sInformerManager.AddPodListener(
		ctx,
		func(obj interface{}) { // Add.
			podAdded(obj, metadataSyncer)
		},
		func(oldObj interface{}, newObj interface{}) { // Update.
			podUpdated(oldObj, newObj, metadataSyncer)
		},
		func(obj interface{}) { // Delete.
			podDeleted(obj, metadataSyncer)
		})
	if err != nil {
		return logger.LogNewErrorf(log, "failed to listen on pods. Error: %v", err)
	}

	metadataSyncer.pvLister = metadataSyncer.k8sInformerManager.GetPVLister()
	metadataSyncer.pvcLister = metadataSyncer.k8sInformerManager.GetPVCLister()
	metadataSyncer.podLister = metadataSyncer.k8sInformerManager.GetPodLister()
	stopCh := metadataSyncer.k8sInformerManager.Listen()
	if stopCh == nil {
		return logger.LogNewError(log, "Failed to sync informer caches")
	}
	log.Infof("Initialized metadata syncer")

	fullSyncTicker := time.NewTicker(time.Duration(getFullSyncIntervalInMin(ctx)) * time.Minute)
	defer fullSyncTicker.Stop()
	// Trigger full sync.
	// If TriggerCsiFullSync feature gate is enabled, use TriggerCsiFullSync to
	// trigger full sync. If not, directly invoke full sync methods.
	if metadataSyncer.coCommonInterface.IsFSSEnabled(ctx, common.TriggerCsiFullSync) {
		log.Infof("%q feature flag is enabled. Using TriggerCsiFullSync API to trigger full sync",
			common.TriggerCsiFullSync)
		// Get a config to talk to the apiserver.
		restConfig, err := config.GetConfig()
		if err != nil {
			log.Errorf("failed to get Kubernetes config. Err: %+v", err)
			return err
		}

		cnsOperatorClient, err := k8s.NewClientForGroup(ctx, restConfig, cnsoperatorv1alpha1.GroupName)
		if err != nil {
			log.Errorf("Failed to create CnsOperator client. Err: %+v", err)
			return err
		}
		go func() {
			for ; true; <-fullSyncTicker.C {
				ctx, log = logger.GetNewContextWithLogger()
				log.Infof("periodic fullSync is triggered")
				triggerCsiFullSyncInstance, err := getTriggerCsiFullSyncInstance(ctx, cnsOperatorClient)
				if err != nil {
					log.Warnf("Unable to get the trigger full sync instance. Err: %+v", err)
					continue
				}

				// Update TriggerCsiFullSync instance if full sync is not already in progress
				if triggerCsiFullSyncInstance.Status.InProgress {
					log.Info("There is a full sync already in progress. Ignoring this current cycle of periodic full sync")
				} else if !triggerCsiFullSyncInstance.Status.InProgress &&
					triggerCsiFullSyncInstance.Spec.TriggerSyncID != triggerCsiFullSyncInstance.Status.LastTriggerSyncID {
					log.Info("FullSync is already triggered. Ignoring this current cycle of periodic full sync")
				} else {
					triggerCsiFullSyncInstance.Spec.TriggerSyncID = triggerCsiFullSyncInstance.Spec.TriggerSyncID + 1
					err = updateTriggerCsiFullSyncInstance(ctx, cnsOperatorClient, triggerCsiFullSyncInstance)
					if err != nil {
						log.Errorf("Failed to update TriggerCsiFullSync instance: %+v to increment the TriggerFullSyncId. "+
							"Error: %v", triggerCsiFullSyncInstance, err)
					} else {
						log.Infof("Incremented TriggerSyncID from %d to %d as part of periodic run to trigger full sync",
							triggerCsiFullSyncInstance.Spec.TriggerSyncID-1, triggerCsiFullSyncInstance.Spec.TriggerSyncID)
					}
				}
			}
		}()
	} else {
		log.Infof("%q feature flag is not enabled. Using the traditional way to directly invoke full sync",
			common.TriggerCsiFullSync)

		go func() {
			for ; true; <-fullSyncTicker.C {
				log.Infof("fullSync is triggered")
				if metadataSyncer.clusterFlavor == cnstypes.CnsClusterFlavorGuest {
					err := PvcsiFullSync(ctx, metadataSyncer)
					if err != nil {
						log.Infof("pvCSI full sync failed with error: %+v", err)
					}
				} else if metadataSyncer.clusterFlavor == cnstypes.CnsClusterFlavorWorkload {
					err := CsiFullSync(ctx, metadataSyncer, metadataSyncer.configInfo.Cfg.Global.VCenterIP)
					if err != nil {
						log.Infof("CSI full sync failed with error: %+v", err)
					}
				} else {
					if len(metadataSyncer.configInfo.Cfg.VirtualCenter) == 1 {
						err := CsiFullSync(ctx, metadataSyncer, metadataSyncer.configInfo.Cfg.Global.VCenterIP)
						if err != nil {
							log.Infof("CSI full sync failed with error: %+v", err)
						}
					} else {
						vcconfigs, err := cnsvsphere.GetVirtualCenterConfigs(ctx, metadataSyncer.configInfo.Cfg)
						if err != nil {
							log.Errorf("Failed to get all virtual configs for CSI full sync. Error: %+v", err)
						}

						log.Debugf("Starting full sync for Multi VC setup with %d VCs", len(vcconfigs))

						isTopologyAwareFileVolumeEnabled := commonco.ContainerOrchestratorUtility.IsFSSEnabled(ctx,
							common.TopologyAwareFileVolume)
						if isTopologyAwareFileVolumeEnabled {
							createMissingFileVolumeInfoCrs(ctx, metadataSyncer)
						}

						var csiFulSyncWg sync.WaitGroup
						for _, vc := range vcconfigs {
							csiFulSyncWg.Add(1)
							vCenter := vc
							go func() {
								defer csiFulSyncWg.Done()
								// TODO: Create/delete volumeInfo CRs if it was missed by metadatasyncer.
								err := CsiFullSync(ctx, metadataSyncer, vCenter.Host)
								if err != nil {
									log.Infof("CSI full sync failed with error: %+v for VC %s", err, vCenter.Host)
								}
							}()
						}
						csiFulSyncWg.Wait()
					}
				}
			}
		}()
	}

	// Trigger get pv to backingDiskObjectId mapping on vanilla cluster
	pvToBackingDiskObjectIdFSSEnabled := metadataSyncer.coCommonInterface.IsFSSEnabled(ctx,
		common.PVtoBackingDiskObjectIdMapping)
	if metadataSyncer.clusterFlavor == cnstypes.CnsClusterFlavorVanilla && pvToBackingDiskObjectIdFSSEnabled {
		pvToBackingDiskObjectIdMappingTicker := time.NewTicker(time.Duration(
			getPVtoBackingDiskObjectIdIntervalInMin(ctx)) * time.Minute)
		defer pvToBackingDiskObjectIdMappingTicker.Stop()

		var pvToBackingDiskObjectIdSupportCheck bool
		vcconfigs, err := cnsvsphere.GetVirtualCenterConfigs(ctx, configInfo.Cfg)
		if err != nil {
			return logger.LogNewErrorf(log, "failed to get VirtualCenterConfigs. err: %v", err)
		}
		var pvToBackingDiskObjectIdSupportedByVc bool
		for _, vcconfig := range vcconfigs {
			vCenter, err := cnsvsphere.GetVirtualCenterInstanceForVCenterConfig(ctx, vcconfig, false)
			if err != nil {
				return logger.LogNewErrorf(log, "failed to get vCenterInstance for vCenter Host: %q, err: %v", vcconfig.Host, err)
			}
			// All VCs in the deployment must support PV to Backing Disk Object ID feature.
			pvToBackingDiskObjectIdSupportedByVc = common.CheckPVtoBackingDiskObjectIdSupport(ctx, vCenter)
			if !pvToBackingDiskObjectIdSupportedByVc {
				pvToBackingDiskObjectIdSupportCheck = false
				break
			} else {
				pvToBackingDiskObjectIdSupportCheck = true
			}
		}

		if pvToBackingDiskObjectIdSupportCheck {
			go func() {
				for ; true; <-pvToBackingDiskObjectIdMappingTicker.C {
					ctx, log = logger.GetNewContextWithLogger()
					log.Info("get pv to backingDiskObjectId mapping is triggered")

					vcconfigs, err := cnsvsphere.GetVirtualCenterConfigs(ctx, configInfo.Cfg)
					if err != nil {
						log.Error(log, "failed to get VirtualCenterConfigs. err: %v", err)
						return
					}
					// Update mapping for all VCs.
					for _, vcconfig := range vcconfigs {
						csiGetPVtoBackingDiskObjectIdMapping(ctx, k8sClient, metadataSyncer, vcconfig.Host)
					}

				}
			}()
		}
	}

	volumeHealthTicker := time.NewTicker(time.Duration(getVolumeHealthIntervalInMin(ctx)) * time.Minute)
	defer volumeHealthTicker.Stop()

	// Trigger get volume health status.
	if metadataSyncer.clusterFlavor == cnstypes.CnsClusterFlavorWorkload {
		go func() {
			for ; true; <-volumeHealthTicker.C {
				ctx, log = logger.GetNewContextWithLogger()
				log.Infof("getVolumeHealthStatus is triggered")
				csiGetVolumeHealthStatus(ctx, k8sClient, metadataSyncer)
			}
		}()
		if IsPodVMOnStretchSupervisorFSSEnabled {
			// Trigger StoragePolicyQuota reconciler to handle add/delete event on StoragePolicyQuota.
			storageQuotaEnablementTicker := time.NewTicker(common.DefaultFeatureEnablementCheckInterval)
			defer storageQuotaEnablementTicker.Stop()
			go func() {
				for ; true; <-storageQuotaEnablementTicker.C {
					ctx, log = logger.GetNewContextWithLogger()
					if err := initStoragePolicyQuotaReconciler(ctx, metadataSyncer); err != nil {
						log.Warnf("Error while initializing StoragePolicyQuota reconciler. Err:%+v. "+
							"Retry will be triggered at %v",
							err, time.Now().Add(common.DefaultFeatureEnablementCheckInterval))
						continue
					}
					break
				}
			}()
		}
	}
	if metadataSyncer.clusterFlavor == cnstypes.CnsClusterFlavorGuest {
		volumeHealthEnablementTicker := time.NewTicker(common.DefaultFeatureEnablementCheckInterval)
		defer volumeHealthEnablementTicker.Stop()
		// Trigger volume health reconciler.
		go func() {
			for ; true; <-volumeHealthEnablementTicker.C {
				ctx, log = logger.GetNewContextWithLogger()
				if err := initVolumeHealthReconciler(ctx, k8sClient, metadataSyncer.supervisorClient); err != nil {
					log.Warnf("Error while initializing volume health reconciler. Err:%+v. Retry will be triggered at %v",
						err, time.Now().Add(common.DefaultFeatureEnablementCheckInterval))
					continue
				}
				break
			}
		}()

		volumeResizeEnablementTicker := time.NewTicker(common.DefaultFeatureEnablementCheckInterval)
		defer volumeResizeEnablementTicker.Stop()
		// Trigger resize reconciler.
		go func() {
			for ; true; <-volumeResizeEnablementTicker.C {
				ctx, log = logger.GetNewContextWithLogger()
				if err := initResizeReconciler(ctx, k8sClient, metadataSyncer.supervisorClient); err != nil {
					log.Warnf("Error while initializing volume resize reconciler. Err:%+v. Retry will be triggered at %v",
						err, time.Now().Add(common.DefaultFeatureEnablementCheckInterval))
					continue
				}
				break
			}
		}()
	}

	<-stopCh
	return nil
}

// addFinalizerOnCnsFileVolumeClientCRs checks and adds finalizer on CnsFileVolumeClient CRs if it is missing.
func addFinalizerOnCnsFileVolumeClientCRs(ctx context.Context) error {
	log := logger.GetLogger(ctx).WithOptions()
	cfg, err := config.GetConfig()
	if err != nil {
		msg := fmt.Sprintf("Failed to get config. Err: %+v", err)
		log.Error(msg)
		return err
	}
	apiextensionsClientSet, err := apiextensionsclientset.NewForConfig(cfg)
	if err != nil {
		log.Errorf("failed to create Kubernetes client using config. Err: %+v", err)
		return err
	}
	_, err = apiextensionsClientSet.ApiextensionsV1().CustomResourceDefinitions().Get(ctx,
		"cnsfilevolumeclients.cns.vmware.com", metav1.GetOptions{})
	if err != nil {
		if apierrors.IsNotFound(err) {
			log.Infof("CR instance cnsfilevolumeclients.cns.vmware.com is not registered. " +
				"skipping to add finalizer on CNSFileVolumeClient Instances")
			return nil
		} else {
			log.Errorf("failed to check if CnsFileAccessConfig CR is registered. Err: %v", err)
			return err
		}
	}
	cnsOperatorClient, err := k8s.NewClientForGroup(ctx, cfg, cnsoperatorv1alpha1.GroupName)
	if err != nil {
		log.Errorf("failed to create CnsOperator client. Err: %+v", err)
		return err
	}
	// Get the list of all CnsFileVolumeClient CRs from all supervisor namespaces.
	cnsFileVolumeClientList := &cnsfilevolumeclientv1alpha1.CnsFileVolumeClientList{}
	err = cnsOperatorClient.List(ctx, cnsFileVolumeClientList)
	if err != nil {
		log.Errorf("failed to list CnsFileVolumeClient CRs from all supervisor namespaces. Error: %+v",
			err)
		return err
	}

	for _, cnsFileVolumeClient := range cnsFileVolumeClientList.Items {
		// If cnsFileVolumeClient instance is marked for deletion, then no need to add finalizer
		if cnsFileVolumeClient.DeletionTimestamp == nil {
			cnsFinalizerExists := false
			// Check if finalizer already exists.
			for _, finalizer := range cnsFileVolumeClient.Finalizers {
				if finalizer == cnsoperatortypes.CNSFinalizer {
					cnsFinalizerExists = true
					break
				}
			}
			if !cnsFinalizerExists {
				// Add finalizer.
				cnsFileVolumeClient.Finalizers = append(cnsFileVolumeClient.Finalizers,
					cnsoperatortypes.CNSFinalizer)
				err = cnsOperatorClient.Update(ctx, &cnsFileVolumeClient)
				if err != nil {
					log.Errorf("failed to update CnsFileVolumeClient instance: %q on namespace: %q. Error: %+v",
						cnsFileVolumeClient.Name, cnsFileVolumeClient.Namespace, err)
					return err
				}
				log.Infof("successfully added finalizer on CnsFileVolumeClient instance: %q on namespace: %q",
					cnsFileVolumeClient.Name, cnsFileVolumeClient.Namespace)
			}
		}
	}
	return nil
}

func initStorageQuotaPeriodicSync(ctx context.Context, metadataSyncer *metadataSyncInformer) error {
	log := logger.GetLogger(ctx).WithOptions()
	if int(PeriodicSyncIntervalInMin.Minutes()) == 0 {
		log.Info("initStorageQuotaPeriodicSync: sync interval is set to 0, " +
			"will skip the Periodic Sync for storage quota")
		return nil
	}
	// create storagequotaperiodicsync CR

	log.Info("initStorageQuotaPeriodicSync: Initialize the storage quota periodic sync")
	restConfig, err := config.GetConfig()
	if err != nil {
		log.Errorf("initStorageQuotaPeriodicSync: failed to get Kubernetes config. Err: %+v", err)
		return err
	}
	cnsOperatorClient, err := k8s.NewClientForGroup(ctx, restConfig, cnsoperatorv1alpha1.GroupName)
	if err != nil {
		log.Errorf("initStorageQuotaPeriodicSync: failed to create CnsOperator client. Err: %+v", err)
		return err
	}
	sqPeriodicSync := &sqperiodicsyncv1alpha1.StorageQuotaPeriodicSync{
		ObjectMeta: metav1.ObjectMeta{
			Name:      StorageQuotaPeriodicSyncInstanceName,
			Namespace: common.GetCSINamespace(),
		},
		Spec: sqperiodicsyncv1alpha1.StorageQuotaPeriodicSyncSpec{
			SyncIntervalInMinutes: int(PeriodicSyncIntervalInMin.Minutes()),
		},
	}

	err = cnsOperatorClient.Get(ctx, k8stypes.NamespacedName{
		Namespace: sqPeriodicSync.Namespace,
		Name:      sqPeriodicSync.Name},
		sqPeriodicSync)
	if err != nil {
		if apierrors.IsNotFound(err) {
			log.Info("initStorageQuotaPeriodicSync: StorageQuotaPeriodicSync CR not found, create new instance")
			if err := cnsOperatorClient.Create(ctx, sqPeriodicSync); err != nil {
				msg := fmt.Sprintf("initStorageQuotaPeriodicSync: failed to create StorageQuotaPeriodicSync:"+
					" %q/%q. Error: %v",
					sqPeriodicSync.Namespace, sqPeriodicSync.Name, err)
				log.Error(msg)
				return err
			}
			err = cnsOperatorClient.Get(ctx, k8stypes.NamespacedName{
				Namespace: sqPeriodicSync.Namespace,
				Name:      sqPeriodicSync.Name},
				sqPeriodicSync)
			if err != nil {
				log.Errorf("initStorageQuotaPeriodicSync: failed to fetch StorageQuotaPeriodicSync instance "+
					" with name %q Error: %+v",
					StorageQuotaPeriodicSyncInstanceName, err)
				return err
			}
		} else {
			log.Errorf("initStorageQuotaPeriodicSync: failed to fetch StorageQuotaPeriodicSync instance"+
				" with name %q Error: %+v",
				StorageQuotaPeriodicSyncInstanceName, err)
			return err
		}
	}
	if sqPeriodicSync.Spec.SyncIntervalInMinutes != int(PeriodicSyncIntervalInMin.Minutes()) {
		rawPatch := client.MergeFromWithOptions(
			sqPeriodicSync.DeepCopy(),
			client.MergeFromWithOptimisticLock{})
		oldInterval := sqPeriodicSync.Spec.SyncIntervalInMinutes

		sqPeriodicSync.Spec.SyncIntervalInMinutes = int(PeriodicSyncIntervalInMin.Minutes())
		err = cnsOperatorClient.Patch(ctx, sqPeriodicSync, rawPatch)
		if err != nil {
			log.Errorf("initStorageQuotaPeriodicSync: failed to patch StorageQuotaPeriodicSync instance"+
				" with name %q Error: %+v",
				StorageQuotaPeriodicSyncInstanceName, err)
			sqPeriodicSync.Spec.SyncIntervalInMinutes = oldInterval
			log.Infof("initStorageQuotaPeriodicSync: Initializing periodic sync with previous interval value, %d ",
				oldInterval)
		}
	}

	// start cron job
	s := gocron.NewScheduler(time.UTC)
	job, err := s.Every(sqPeriodicSync.Spec.SyncIntervalInMinutes).Minute().Do(func() {
		go syncStorageQuotaReserved(ctx, cnsOperatorClient, metadataSyncer)
	})
	if err != nil {
		log.Errorf("initStorageQuotaPeriodicSync: cannot start periodic sync for StorageQuota instance %q, Error: %+v",
			sqPeriodicSync.Name, err)
		return err
	}
	// Make sure a single job runs(no overlapping jobs)
	job.SingletonMode()
	s.StartAsync()
	log.Info("initStorageQuotaPeriodicSync: Cron is setup for periodic storage quota sync")
	return nil
}

func syncStorageQuotaReserved(ctx context.Context,
	cnsOperatorClient client.Client, metadataSyncer *metadataSyncInformer) {
	log := logger.GetLogger(ctx)
	lastSyncTime := metav1.NewTime(time.Now())
	// fetching storagepolicyquota
	log.Info("syncStorageQuotaReserved: Sync started for storage quota")
	spqList := &storagepolicyv1alpha2.StoragePolicyQuotaList{}
	err := cnsOperatorClient.List(ctx, spqList, &client.ListOptions{})
	if err != nil {
		log.Errorf("syncStorageQuotaReserved: failed to fetch storage policy quota instances, Error: %+v", err)
		return
	}
	log.Debugf("syncStorageQuotaReserved: Retrieved StoragePolicyQuota instances for syncing", "Count: %d",
		len(spqList.Items))
	namespaces := make(map[string]string)
	// create namespace map to be able to calculate reserved namespace wise
	for _, spq := range spqList.Items {
		namespaces[spq.Namespace] = ""
	}
	// fetch pv list
	volumeMap, err := fetchPVs(ctx, metadataSyncer)
	if err != nil {
		log.Errorf("syncStorageQuotaReserved: failed to fetch persistentvolume instances, Error: %+v", err)
		return
	}
	var totalStoragePolicyReserved map[string]*resource.Quantity
	expectedReservedValues := []sqperiodicsyncv1alpha1.ExpectedReservedValues{}
	log.Debug("syncStorageQuotaReserved: iterate through namespaces and calcuate reserved values")
	// loop over namespaces and calcuate reserved values
	for ns := range namespaces {
		totalStoragePolicyReserved = make(map[string]*resource.Quantity)
		log.Debugf("syncStorageQuotaReserved: processing storage quota sync for namespace %q", ns)
		// calculate VM service's storagepolicyusage reserved values for given namespace
		spuVmServiceReserved, err := calculateVMServiceStoragePolicyUsageReservedForNamespace(ctx,
			cnsOperatorClient, ns)
		if err != nil {
			log.Errorf("syncStorageQuotaReserved: error while calculating expected VmService StoragePolicyUsage"+
				" reserved value, Error: %v", err)
			continue
		}
		if spuVmServiceReserved != nil {
			totalStoragePolicyReserved = mergeStoragePolicyReserved(spuVmServiceReserved, totalStoragePolicyReserved)
		}

		// calculate pending and under expansion PVC's reserved values for given namespace
		pvcReserved, err := calculatePVCReservedForNamespace(ctx, volumeMap, ns, metadataSyncer)
		if err != nil {
			log.Errorf("syncStorageQuotaReserved: error while calculating expected PVC reserved value for"+
				" given namespace %q, Error: %v", ns, err)
			continue
		}
		if pvcReserved != nil {
			totalStoragePolicyReserved = mergeStoragePolicyReserved(pvcReserved, totalStoragePolicyReserved)
		}

		// calculate reserved values for not ready snapshots
		vsReserved, err := calculateVolumeSnapshotReservedForNamespace(ctx, ns, metadataSyncer)
		if err != nil {
			log.Errorf("syncStorageQuotaReserved: error while calculating expected VolumeSnapshot reserved value for"+
				" given namespace %q, Error: %v", ns, err)
			continue
		}
		if vsReserved != nil {
			totalStoragePolicyReserved = mergeStoragePolicyReserved(vsReserved, totalStoragePolicyReserved)
		}

		// Check if storage policy reservation related FSS is enabled
		if commonco.ContainerOrchestratorUtility.IsFSSEnabled(ctx, common.StoragePolicyReservationSupport) {
			// calculate expected reserved values for StoragePolicyReservation CRs for given namespace
			sprReserved, err := calculateSPRReservedForNamespace(ctx, cnsOperatorClient, ns)
			if err != nil {
				log.Errorf("syncStorageQuotaReserved: error while calculating expected reserved value for"+
					" StoragePolicyReservation in namespace %q, Error: %v", ns, err)
				continue
			}
			if sprReserved != nil {
				totalStoragePolicyReserved = mergeStoragePolicyReserved(sprReserved, totalStoragePolicyReserved)
			}
		}

		expectedReservedValues = append(expectedReservedValues, sqperiodicsyncv1alpha1.ExpectedReservedValues{
			Namespace: ns,
			Reserved:  totalStoragePolicyReserved,
		})
	}
	updateStorageQuotaPeriodicSyncCR(ctx, cnsOperatorClient, expectedReservedValues, lastSyncTime)
	log.Debug("syncStorageQuotaReserved: updated the StorageQuotaPeriodicSync CR")
}

// mergeStoragePolicyReserved will sum up  and return the total reserved value
func mergeStoragePolicyReserved(storagePolicyReserved,
	totalStoragepolicyReserved map[string]*resource.Quantity) map[string]*resource.Quantity {
	for spid, reserved := range storagePolicyReserved {
		if _, ok := totalStoragepolicyReserved[spid]; !ok {
			totalStoragepolicyReserved[spid] = resource.NewQuantity(0, resource.BinarySI)
		}
		totalStoragepolicyReserved[spid].Add(*reserved)
	}
	return totalStoragepolicyReserved
}

// calculateVMServiceStoragePolicyUsageReservedForNamespace calculate the expected
// reserved for StoragePolicyUsage for VMService
func calculateVMServiceStoragePolicyUsageReservedForNamespace(ctx context.Context, cnsOperatorClient client.Client,
	namespace string) (map[string]*resource.Quantity, error) {
	log := logger.GetLogger(ctx)
	log.Debugf("calculateVMServiceStoragePolicyUsageReservedForNamespace: Fetching StoragePolicyUsage for namespace %q",
		namespace)
	supList := &storagepolicyv1alpha2.StoragePolicyUsageList{}
	err := cnsOperatorClient.List(ctx, supList, &client.ListOptions{Namespace: namespace})
	if err != nil {
		return nil, err
	}
	storagePolicyToReservedMap := make(map[string]*resource.Quantity)
	for _, storagePolicyUsage := range supList.Items {
		if storagePolicyUsage.Spec.ResourceExtensionName == VMServiceExtensionServiceName {
			log.Debugf("calculateVMServiceStoragePolicyUsageReservedForNamespace: Processing StoragePolicyUsage"+
				" Name: %q, Namespace: %q", storagePolicyUsage.Name, storagePolicyUsage.Namespace)
			if storagePolicyUsage.DeletionTimestamp != nil {
				log.Debugf("calculateVMServiceStoragePolicyUsageReservedForNamespace:"+
					" StoragePolicyUsage is marked for deletion, ignoring Name: %q, Namespace: %q",
					storagePolicyUsage.Name, storagePolicyUsage.Namespace)
				continue
			}
			if storagePolicyUsage.Status.ResourceTypeLevelQuotaUsage == nil {
				log.Debugf("calculateVMServiceStoragePolicyUsageReservedForNamespace: StoragePolicyUsage"+
					" status not populated, continue processing other PVCs Name: %q, Namespace: %q",
					storagePolicyUsage.Name, storagePolicyUsage.Namespace)
				continue
			}
			if storagePolicyToReservedMap[storagePolicyUsage.Spec.StoragePolicyId] == nil {
				storagePolicyToReservedMap[storagePolicyUsage.Spec.StoragePolicyId] = resource.NewQuantity(0,
					resource.BinarySI)
			}
			storagePolicyToReservedMap[storagePolicyUsage.Spec.StoragePolicyId].Add(
				*storagePolicyUsage.Status.ResourceTypeLevelQuotaUsage.Reserved)
		}
	}
	return storagePolicyToReservedMap, nil
}

// calculatePendingPVCReservedForNamespace calculate the expected reserved value for PVC for namespace
func calculatePVCReservedForNamespace(ctx context.Context, volumeMap map[string]*v1.PersistentVolume,
	namespace string, metadataSyncer *metadataSyncInformer) (map[string]*resource.Quantity, error) {
	log := logger.GetLogger(ctx)
	// get storageclass and storage policy ID mappings.
	scToStoragePolicyIDMap, err := fetchStorageClassToStoragePolicyMapping(ctx)
	if err != nil {
		return nil, err
	}
	// fetch pvc for given namespace
	log.Debugf("calculatePVCReservedForNamespace: Fetching PersistentVolumeClaim for namespace %q", namespace)
	pvcList, err := metadataSyncer.pvcLister.PersistentVolumeClaims(namespace).List(labels.NewSelector())
	if err != nil {
		log.Errorf("calculatePVCReservedForNamespace: unable to fetch all pvc from namespace %q, Error %v",
			namespace, err)
		return nil, err
	}
	storagePolicyToReservedMap := make(map[string]*resource.Quantity)
	for _, pvc := range pvcList {
		log.Debugf("calculatePVCReservedForNamespace: Processing PersistentVolumeClaim Name: %q, Namespace: %q",
			pvc.Name, pvc.Namespace)
		if pvc.DeletionTimestamp != nil {
			log.Debugf("calculatePVCReservedForNamespace: pvc is marked for deletion, ignoring Name: %q, Namespace: %q",
				pvc.Name, pvc.Namespace)
			continue
		}
		if pvc.Status.Phase == "" {
			log.Debugf("calculatePVCReservedForNamespace: pvc status not populated, continuing processing others"+
				" Name: %q, Namespace: %q", pvc.Name, pvc.Namespace)
			continue
		}
		if pvc.Spec.StorageClassName == nil {
			log.Debugf("calculatePVCReservedForNamespace: storageclass is not provided for pvc,"+
				" continue processing other PVCs Name: %q, Namespace: %q", pvc.Name, pvc.Namespace)
			continue
		}
		if storagePolicyID, ok := scToStoragePolicyIDMap[*pvc.Spec.StorageClassName]; ok {
			if storagePolicyToReservedMap[storagePolicyID] == nil {
				storagePolicyToReservedMap[storagePolicyID] = resource.NewQuantity(0, resource.BinarySI)
			}
		}
		// check if pvc is in pending state
		pvcSize := pvc.Spec.Resources.Requests[v1.ResourceStorage]
		if pvc.Status.Phase == v1.ClaimPending {
			if storagePolicyID, ok := scToStoragePolicyIDMap[*pvc.Spec.StorageClassName]; ok {
				log.Debugf("calculatePVCReservedForNamespace: pvc is in pending state,"+
					" Name: %q, Namespace: %q adding pvc capacity to reserved", pvc.Name, pvc.Namespace)
				storagePolicyToReservedMap[storagePolicyID].Add(pvcSize)
			}
		} else if pv, ok := volumeMap[pvc.Name]; ok {
			pvSize := pv.Spec.Capacity[v1.ResourceStorage]
			// check if pvc is under expansion
			if !pvcSize.Equal(pvSize) {
				log.Debugf("calculatePVCReservedForNamespace: pvc size being expanded"+
					" Name: %q, Namespace: %q adding pvc capacity to reserved", pvc.Name, pvc.Namespace)
				if storagePolicyID, ok := scToStoragePolicyIDMap[*pvc.Spec.StorageClassName]; ok {
					// get accurate expected reserved value
					pvcSize.Sub(pvSize)
					storagePolicyToReservedMap[storagePolicyID].Add(pvcSize)
				}
			}
		}
	}
	return storagePolicyToReservedMap, nil
}

// calculateVolumeSnapshotReservedForNamespace calculate the expected reserved value for volumesnapshot for namespace
func calculateVolumeSnapshotReservedForNamespace(ctx context.Context,
	namespace string, metadataSyncer *metadataSyncInformer) (map[string]*resource.Quantity, error) {
	log := logger.GetLogger(ctx)
	scToStoragPolicyIdMap, err := fetchStorageClassToStoragePolicyMapping(ctx)
	if err != nil {
		return nil, err
	}
	log.Debugf("calculateVolumeSnapshotReservedForNamespace: Fetching VolumeSnapshots for namespace %q", namespace)
	snapshotterClient, err := k8s.NewSnapshotterClient(ctx)
	if err != nil {
		log.Errorf("calculateVolumeSnapshotReservedForNamespace: failed to get snapshotterClient with error: %v", err)
		return nil, err
	}
	vsList, err := snapshotterClient.SnapshotV1().VolumeSnapshots(namespace).List(ctx, metav1.ListOptions{})
	if err != nil {
		log.Errorf("calculateVolumeSnapshotReservedForNamespace: failed to list VolumeSnapshot with"+
			" error: %v in namespace: %s", err, namespace)
		return nil, err
	}
	pvcList := &v1.PersistentVolumeClaimList{}
	volumeClaim := &v1.PersistentVolumeClaim{}
	volumeMap := make(map[string]*v1.PersistentVolumeClaim)
	storagePolicyIdToReservedMap := make(map[string]*resource.Quantity)
	for _, vs := range vsList.Items {
		if vs.DeletionTimestamp != nil {
			log.Debugf("calculateVolumeSnapshotReservedForNamespace: volumesnapshot is marked for deletion,"+
				" ignoring Name: %q, Namespace: %q",
				vs.Name, vs.Namespace)
			continue
		}
		if vs.Status != nil && vs.Status.ReadyToUse != nil && *vs.Status.ReadyToUse {
			log.Debugf("calculateVolumeSnapshotReservedForNamespace: skipping volumesnapshot"+
				" as it is ready to use, ignoring Name: %q, Namespace: %q", vs.Name, vs.Namespace)
			continue
		} else {
			log.Debugf("calculateVolumeSnapshotReservedForNamespace: Processing VolumeSnapshot"+
				" Name: %q, Namespace: %q", vs.Name, vs.Namespace)
			if len(pvcList.Items) == 0 {
				log.Debugf("calculateVolumeSnapshotReservedForNamespace: Fetching PersistentVolumeClaim "+
					" for Namespace: %q", vs.Namespace)
				pvcList, err := metadataSyncer.pvcLister.PersistentVolumeClaims(namespace).List(labels.NewSelector())
				if err != nil {
					log.Errorf("calculateVolumeSnapshotReservedForNamespace: unable to fetch pvc"+
						" from namespace %q, Error %v", namespace, err)
					return nil, err
				}
				for _, pvc := range pvcList {
					volumeMap[pvc.Name] = pvc
				}
			}
			if vs.Spec.Source.PersistentVolumeClaimName != nil {
				volumeClaim = volumeMap[*vs.Spec.Source.PersistentVolumeClaimName]
				if spu, ok := scToStoragPolicyIdMap[*volumeClaim.Spec.StorageClassName]; ok {
					if storagePolicyIdToReservedMap[spu] == nil {
						storagePolicyIdToReservedMap[spu] = resource.NewQuantity(0, resource.BinarySI)
					}
					storagePolicyIdToReservedMap[spu].Add(volumeClaim.Spec.Resources.Requests[v1.ResourceStorage])
				}
			}
		}
	}
	return storagePolicyIdToReservedMap, nil
}

// calculateSPRReservedForNamespace calculates the expected reserved capacity values for StoragePolicyReservation
// CRs in the given namespace and returns map of storage policy ID to expected reserved capacity for that policy.
func calculateSPRReservedForNamespace(ctx context.Context, cnsOperatorClient client.Client,
	namespace string) (map[string]*resource.Quantity, error) {
	log := logger.GetLogger(ctx)
	log.Debugf("calculateSPRReservedForNamespace: Fetching StoragePolicyReservation CRs for namespace %q",
		namespace)
	sprList := &storagepolicyv1alpha2.StoragePolicyReservationList{}
	err := cnsOperatorClient.List(ctx, sprList, &client.ListOptions{Namespace: namespace})
	if err != nil {
		return nil, err
	}
	if len(sprList.Items) == 0 {
		log.Debugf("calculateSPRReservedForNamespace: There are no StoragePolicyReservation CRs in namespace %q",
			namespace)
		return nil, nil
	}

	storagePolicyIdToReservedMap := make(map[string]*resource.Quantity)
	for _, storagePolicyReservation := range sprList.Items {
		log.Debugf("calculateSPRReservedForNamespace: Processing StoragePolicyReservation CR, "+
			"Name: %q, Namespace: %q", storagePolicyReservation.Name, storagePolicyReservation.Namespace)
		expectedReservedMap, err := getExpectedReservedCapacityForSPR(ctx, storagePolicyReservation)
		if err != nil {
			return nil, err
		}
		for storagePolicyId, capacity := range expectedReservedMap {
			if storagePolicyIdToReservedMap[storagePolicyId] == nil {
				storagePolicyIdToReservedMap[storagePolicyId] = resource.NewQuantity(0,
					resource.BinarySI)
			}
			storagePolicyIdToReservedMap[storagePolicyId].Add(*capacity)
		}
	}
	return storagePolicyIdToReservedMap, nil
}

// getExpectedReservedCapacityForSPR gets the expected reserved capacity for given StoragePolicyReservation.
func getExpectedReservedCapacityForSPR(ctx context.Context,
	spr storagepolicyv1alpha2.StoragePolicyReservation) (map[string]*resource.Quantity, error) {
	log := logger.GetLogger(ctx)
	log.Infof("Fetching expected reserved capacity for StoragePolicyReservation, Name: %q, Namespace: %q",
		spr.Name, spr.Namespace)

	// Calculate requested capacity by StorageClass from Spec
	requestedCapacityMap := calculateRequestedCapacityByStorageClass(spr)

	// Calculate approved capacity by StorageClass from Status
	approvedCapacityMap := calculateApprovedCapacityByStorageClass(spr)

	// Calculate expected reserved capacity by StorageClass based on requested and approved capacity map
	expectedReservedCapacityMap := calculateExpectedReservedCapacity(ctx, requestedCapacityMap,
		approvedCapacityMap)

	// Get storageclass to storage policy ID mapping
	scToStoragePolicyIDMap, err := fetchStorageClassToStoragePolicyMapping(ctx)
	if err != nil {
		return nil, err
	}

	// Convert StorageClass capacity map to StoragePolicyID capacity map
	storagePolicyExpectedReservedMap := make(map[string]*resource.Quantity)
	for storageClassName, expectedReservedCapacity := range expectedReservedCapacityMap {
		if expectedReservedCapacity.IsZero() {
			continue
		}

		log.Infof("Calculated expected reserved capacity for StorageClass: %s, "+
			"ExpectedReservedCapacity: %s", storageClassName, expectedReservedCapacity.String())
		// Get storage policy ID from StorageClass
		if storagePolicyID, ok := scToStoragePolicyIDMap[storageClassName]; ok {
			// Create storage policy ID to capacity map
			capacity := expectedReservedCapacity.DeepCopy()
			storagePolicyExpectedReservedMap[storagePolicyID] = &capacity
		} else {
			log.Errorf("Couldn't find storage policy ID for StorageClass %s in the map, continuing for "+
				"other storage classes", storageClassName)
			continue
		}
	}
	log.Infof("Successfully calculated expected reserved capacity values for StoragePolicyReservation, "+
		"Name: %q, Namespace: %q", spr.Name, spr.Namespace)
	return storagePolicyExpectedReservedMap, nil
}

// calculateRequestedCapacityByStorageClass aggregates requested capacity by StorageClass from Spec
func calculateRequestedCapacityByStorageClass(
	spr storagepolicyv1alpha2.StoragePolicyReservation) map[string]resource.Quantity {
	capacityMap := make(map[string]resource.Quantity)

	for _, requested := range spr.Spec.Requested {
		for _, reservationRequest := range requested.ReservationRequests {
			if reservationRequest.Request == nil || reservationRequest.Request.IsZero() {
				continue
			}

			if existingCapacity, exists := capacityMap[reservationRequest.StorageClassName]; exists {
				existingCapacity.Add(*reservationRequest.Request)
				capacityMap[reservationRequest.StorageClassName] = existingCapacity
			} else {
				capacityMap[reservationRequest.StorageClassName] = *reservationRequest.Request
			}
		}
	}

	return capacityMap
}

// calculateApprovedCapacityByStorageClass aggregates approved capacity by StorageClass from Status
func calculateApprovedCapacityByStorageClass(
	spr storagepolicyv1alpha2.StoragePolicyReservation) map[string]resource.Quantity {
	capacityMap := make(map[string]resource.Quantity)

	for _, approved := range spr.Status.Approved {
		// Check if Request is nil or zero
		if approved.Request == nil || approved.Request.IsZero() {
			continue
		}

		capacity := *approved.Request

		if existingCapacity, exists := capacityMap[approved.StorageClassName]; exists {
			existingCapacity.Add(capacity)
			capacityMap[approved.StorageClassName] = existingCapacity
		} else {
			capacityMap[approved.StorageClassName] = capacity
		}
	}

	return capacityMap
}

// calculateExpectedReservedCapacity calculates the difference between requested and approved capacity
// for each StorageClass and returns the map of StorageClass to expected reserved capacity
func calculateExpectedReservedCapacity(ctx context.Context, requestedMap,
	approvedMap map[string]resource.Quantity) map[string]resource.Quantity {
	log := logger.GetLogger(ctx)
	expectedReservedCapacityMap := make(map[string]resource.Quantity)

	for storageClassName, requestedCapacity := range requestedMap {
		approvedCapacity, exists := approvedMap[storageClassName]
		if !exists {
			// All requested capacity is extra if nothing is approved
			expectedReservedCapacityMap[storageClassName] = requestedCapacity
			continue
		}

		// Calculate the difference
		extraCapacity := requestedCapacity.DeepCopy()
		extraCapacity.Sub(approvedCapacity)

		// Only include positive differences (extra reserved quota)
		if extraCapacity.Sign() > 0 {
			expectedReservedCapacityMap[storageClassName] = extraCapacity
		} else if extraCapacity.Sign() < 0 {
			log.Infof("WARNING: Negative extra reserved quota found for StorageClass: %s, "+
				"ExtraCapacity: %s", storageClassName, extraCapacity.String())
		}
	}

	return expectedReservedCapacityMap
}

// updateStorageQuotaPeriodicSyncCR update the StorageQuotaPeriodSync CR status
func updateStorageQuotaPeriodicSyncCR(ctx context.Context, cnsOperatorClient client.Client,
	expectedReservedvalues []sqperiodicsyncv1alpha1.ExpectedReservedValues, lastSyncTime metav1.Time) {
	log := logger.GetLogger(ctx)
	log.Debugf("updateStorageQuotaPeriodicSyncCR: Updating StorageQuotaPeriodicSyncCR %s",
		StorageQuotaPeriodicSyncInstanceName)
	sqPeriodicSync := &sqperiodicsyncv1alpha1.StorageQuotaPeriodicSync{}
	err := cnsOperatorClient.Get(ctx,
		k8stypes.NamespacedName{
			Name:      StorageQuotaPeriodicSyncInstanceName,
			Namespace: common.GetCSINamespace()},
		sqPeriodicSync)
	if err != nil {
		log.Errorf("updateStorageQuotaPeriodicSyncCR: failed to fetch StorageQuotaPeriodicSync"+
			" instance with name %q Error: %+v", StorageQuotaPeriodicSyncInstanceName, err)
		return
	}
	oldSqPeriodicSync := sqPeriodicSync.DeepCopy()

	sqPeriodicSync.Status.ExpectedReservedValues = expectedReservedvalues
	sqPeriodicSync.Status.LastSyncTimestamp = lastSyncTime
	patch, err := getPatchData(oldSqPeriodicSync, sqPeriodicSync)
	if err != nil {
		log.Errorf("updateStorageQuotaPeriodicSyncCR: error fetching PatchData StorageQuotaPeriodicSync CR. err: %v",
			err)
		return
	}
	rawPatch := client.RawPatch(k8stypes.MergePatchType, patch)
	err = cnsOperatorClient.Status().Patch(ctx, oldSqPeriodicSync, rawPatch)
	if err != nil {
		log.Errorf("updateStorageQuotaPeriodicSyncCR: failed to update StorageQuotaPeriodicSync instance"+
			" with name %q Error: %+v", StorageQuotaPeriodicSyncInstanceName, err)
		return
	}
}

// fetchPVs will fetch pvs with pv lister
func fetchPVs(ctx context.Context, metadataSyncer *metadataSyncInformer) (map[string]*v1.PersistentVolume, error) {
	log := logger.GetLogger(ctx)
	log.Info("fetchPVs: Fetching PersistentVolumes")
	pvList, err := metadataSyncer.pvLister.List(labels.NewSelector())
	if err != nil {
		log.Errorf("fetchPVs: unable to fetch all PV, Error %v", err)
		return nil, err
	}

	volumeMap := make(map[string]*v1.PersistentVolume)
	for _, pv := range pvList {
		if pv.Spec.ClaimRef != nil && pv.Status.Phase != v1.VolumePending {
			volumeMap[pv.Spec.ClaimRef.Name] = pv
		}
	}
	return volumeMap, nil
}

// fetchStorageClassToStoragePolicyMapping will fetch storageclass and returns mapping with storagepolicy id
func fetchStorageClassToStoragePolicyMapping(ctx context.Context) (map[string]string, error) {
	log := logger.GetLogger(ctx)
	log.Debug("fetchStorageClassToStoragePolicyMapping: Fetching StorageClass and create mapping for storageclass" +
		"and storagepolicyid")
	// Prepare map of storagepolicyID to storageclassnames.
	k8sconfig, err := k8s.GetKubeConfig(ctx)
	if err != nil {
		log.Errorf("fetchStorageClassToStoragePolicyMapping: Failed to get KubeConfig. err: %v", err)
		return nil, err
	}
	k8sClient, err := clientset.NewForConfig(k8sconfig)
	if err != nil {
		log.Errorf("fetchStorageClassToStoragePolicyMapping: Failed to create kubernetes client. Err: %+v", err)
		return nil, err
	}
	storageClassList, err := k8sClient.StorageV1().StorageClasses().List(ctx, metav1.ListOptions{})
	if err != nil {
		log.Errorf("fetchStorageClassToStoragePolicyMapping: Failed to list storageclasses. Err: %+v", err)
		return nil, err
	}
	storageClassToStoragePolicy := make(map[string]string)
	for _, sc := range storageClassList.Items {
		storageClassToStoragePolicy[sc.Name] = sc.Parameters["storagePolicyID"]
	}
	return storageClassToStoragePolicy, nil
}

// initCnsVolumeOperationRequestCRInformer creates and starts an informer for CnsVolumeOperationRequest custom resource.
func initCnsVolumeOperationRequestCRInformer(ctx context.Context, cfg *restclient.Config) error {
	log := logger.GetLogger(ctx)
	// Create an informer for CnsVolumeOperationRequest instances.
	dynInformer, err := k8s.GetDynamicInformer(ctx, cnsvolumeoperationrequestv1alpha1.SchemeGroupVersion.Group,
		cnsvolumeoperationrequestv1alpha1.SchemeGroupVersion.Version, cnsvolumeoperationrequest.CRDPlural,
		metav1.NamespaceAll, cfg, true)
	if err != nil {
		return logger.LogNewErrorf(log, "failed to create dynamic informer for %s CR. Error: %+v",
			cnsvolumeoperationrequest.CRDSingular, err)
	}
	cnsvolumeoperationrequestInformer := dynInformer.Informer()
	_, err = cnsvolumeoperationrequestInformer.AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc: func(obj interface{}) {
			cnsvolumeoperationrequestCRAdded(obj)
		},
		UpdateFunc: func(oldObj interface{}, newObj interface{}) {
			cnsvolumeoperationrequestCRUpdated(oldObj, newObj)
		},
		DeleteFunc: func(obj interface{}) {
			cnsvolumeoperationrequestCRDeleted(obj)
		},
	})
	if err != nil {
		return logger.LogNewErrorf(log, "failed to add event handler on informer for %q CR. Error: %v",
			cnsvolumeoperationrequest.CRDPlural, err)
	}
	// Start informer.
	go func() {
		log.Infof("Informer to watch on %s CR starting..", cnsvolumeoperationrequest.CRDSingular)
		cnsvolumeoperationrequestInformer.Run(make(chan struct{}))
	}()
	return nil
}

// startTopologyCRInformer creates and starts an informer for CSINodeTopology custom resource.
func startTopologyCRInformer(ctx context.Context, cfg *restclient.Config) error {
	log := logger.GetLogger(ctx)
	// Create an informer for CSINodeTopology instances.
	dynInformer, err := k8s.GetDynamicInformer(ctx, csinodetopologyv1alpha1.GroupName,
		csinodetopologyv1alpha1.Version, csinodetopology.CRDPlural, metav1.NamespaceAll, cfg, true)
	if err != nil {
		return logger.LogNewErrorf(log, "failed to create dynamic informer for %s CR. Error: %+v",
			csinodetopology.CRDSingular, err)
	}
	csiNodeTopologyInformer := dynInformer.Informer()
	// TODO: Multi-VC: Use a RWLock to guard simultaneous updates to topologyVCMap
	_, err = csiNodeTopologyInformer.AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc: func(obj interface{}) {
			topoCRAdded(obj)
		},
		UpdateFunc: func(oldObj interface{}, newObj interface{}) {
			topoCRUpdated(oldObj, newObj)
		},
		DeleteFunc: func(obj interface{}) {
			topoCRDeleted(obj)
		},
	})
	if err != nil {
		return logger.LogNewErrorf(log, "failed to add event handler on informer for %q CR. Error: %v",
			csinodetopology.CRDPlural, err)
	}
	// Start informer.
	go func() {
		log.Infof("Informer to watch on %s CR starting..", csinodetopology.CRDSingular)
		csiNodeTopologyInformer.Run(make(chan struct{}))
	}()
	return nil
}

// addLabelsToTopologyVCMap adds topology label to VC mapping for given CSINodeTopology instance
// in the MetadataSyncer.topologyVCMap parameter.
func addLabelsToTopologyVCMap(ctx context.Context, nodeTopoObj csinodetopologyv1alpha1.CSINodeTopology) {
	log := logger.GetLogger(ctx)
	nodeVM, err := nodeMgr.GetNodeVMAndUpdateCache(ctx, nodeTopoObj.Spec.NodeUUID, nil)
	if err != nil {
		log.Errorf("Node %q is not yet registered in the node manager. Error: %+v",
			nodeTopoObj.Spec.NodeUUID, err)
		return
	}
	log.Infof("Topology labels %+v belong to %q VC", nodeTopoObj.Status.TopologyLabels,
		nodeVM.VirtualCenterHost)
	// Update MetadataSyncer.topologyVCMap with topology label and associated VC host.
	for _, label := range nodeTopoObj.Status.TopologyLabels {
		if _, exists := MetadataSyncer.topologyVCMap[label.Value]; !exists {
			MetadataSyncer.topologyVCMap[label.Value] = map[string]struct{}{nodeVM.VirtualCenterHost: {}}
		} else {
			MetadataSyncer.topologyVCMap[label.Value][nodeVM.VirtualCenterHost] = struct{}{}
		}
	}
}

// cnsvolumeoperationrequestCRAdded checks if the cnsvolumeoperationrequest instance reserved field is present
// and updates the reserved field for StoragePolicyUsage CR
func cnsvolumeoperationrequestCRAdded(obj interface{}) {
	ctx, log := logger.GetNewContextWithLogger()
	// Verify objects received.
	var (
		cnsvolumeoperationrequestObj cnsvolumeoperationrequestv1alpha1.CnsVolumeOperationRequest
	)
	err := runtime.DefaultUnstructuredConverter.FromUnstructured(
		obj.(*unstructured.Unstructured).Object, &cnsvolumeoperationrequestObj)
	if err != nil {
		log.Errorf("cnsvolumeoperationrequestCRAdded: failed to cast object %+v to %s. Error: %+v", obj,
			cnsvolumeoperationrequest.CRDSingular, err)
		return
	}
	log.Debugf("cnsvolumeoperationrequestCRAdded: Received a CR added event for cnsvolumeoperationrequestObj %+v",
		cnsvolumeoperationrequestObj)
	// Check for the below set of conditions:
	// 1. Cnsvolumeoperationrequest object's StorageQuotaDetails should not be nil
	// 2. Cnsvolumeoperationrequest object's StorageQuotaDetails.Reserved should not be nil
	if cnsvolumeoperationrequestObj.Status.StorageQuotaDetails != nil &&
		cnsvolumeoperationrequestObj.Status.StorageQuotaDetails.Reserved != nil {
		// Check if the value is zero. This is needed because, during Syncer restart
		// an informer add event is received for all the CRs(already created).
		// For CRs mapping to Volumes which are in Bound state, the Reserved Value will be zero.
		// So, we can safely skip the CRAdded event.
		if cnsvolumeoperationrequestObj.Status.StorageQuotaDetails.Reserved.Value() == 0 {
			log.Debugf("cnsvolumeoperationrequestCRAdded: Received a cnsvolumeoperationrequest CR added event "+
				"for %q, with the reserved capacity as zero(0). Skipping the informer event",
				cnsvolumeoperationrequestObj.Name)
			return
		}
		cnsVolumeOperationRequestName := cnsvolumeoperationrequestObj.Name
		isSnapshot := checkOperationRequestCRForSnapshot(ctx, cnsVolumeOperationRequestName)
		storagePolicyUsageInstanceName := ""
		if isSnapshot {
			// Fetch StoragePolicyUsage instance for storageClass associated with the snapshot.
			log.Infof("Received a CR added event for snapshot operation on volumeID: %s CnsVolumeOperationRequest: %s",
				cnsvolumeoperationrequestObj.Status.VolumeID, cnsVolumeOperationRequestName)
			storagePolicyUsageInstanceName = cnsvolumeoperationrequestObj.Status.StorageQuotaDetails.
				StorageClassName + "-" + storagepolicyv1alpha2.NameSuffixForSnapshot
		} else {
			// Fetch StoragePolicyUsage instance for storageClass associated with the volume.
			log.Infof("Received a CR added event for pvc operation with volumeID: %s CnsVolumeOperationRequest: %s",
				cnsvolumeoperationrequestObj.Status.VolumeID, cnsVolumeOperationRequestName)
			storagePolicyUsageInstanceName = cnsvolumeoperationrequestObj.Status.StorageQuotaDetails.
				StorageClassName + "-" + storagepolicyv1alpha2.NameSuffixForPVC
		}
		restConfig, err := config.GetConfig()
		if err != nil {
			log.Errorf("cnsvolumeoperationrequestCRAdded: failed to get Kubernetes config. Err: %+v", err)
			return
		}
		cnsOperatorClient, err := k8s.NewClientForGroup(ctx, restConfig, cnsoperatorv1alpha1.GroupName)
		if err != nil {
			log.Errorf("cnsvolumeoperationrequestCRAdded: Failed to create CnsOperator client. Err: %+v", err)
			return
		}

		storagePolicyUsageCR := &storagepolicyv1alpha2.StoragePolicyUsage{}
		err = cnsOperatorClient.Get(ctx, k8stypes.NamespacedName{
			Namespace: cnsvolumeoperationrequestObj.Status.StorageQuotaDetails.Namespace,
			Name:      storagePolicyUsageInstanceName},
			storagePolicyUsageCR)
		if err != nil {
			log.Errorf("failed to fetch %s instance with name %q from supervisor namespace %q. Error: %+v",
				storagepolicyv1alpha2.CRDSingular, storagePolicyUsageInstanceName,
				cnsvolumeoperationrequestObj.Status.StorageQuotaDetails.Namespace, err)
			return
		}
		patchedStoragePolicyUsageCR := storagePolicyUsageCR.DeepCopy()
		if storagePolicyUsageCR.Status.ResourceTypeLevelQuotaUsage != nil &&
			storagePolicyUsageCR.Status.ResourceTypeLevelQuotaUsage.Reserved != nil {
			// If StoragePolicyUsage CR has Status.QuotaUsage fields not nil update StoragePolicyUsage reserved field
			patchedStoragePolicyUsageCR.Status.ResourceTypeLevelQuotaUsage.Reserved.Add(
				*cnsvolumeoperationrequestObj.Status.StorageQuotaDetails.Reserved)
			err := PatchStoragePolicyUsage(ctx, cnsOperatorClient, storagePolicyUsageCR,
				patchedStoragePolicyUsageCR)
			if err != nil {
				log.Errorf("updateStoragePolicyUsage failed. err: %v", err)
				return
			}
			log.Infof("cnsvolumeoperationrequestCRAdded: Successfully increased the reserved field by %s "+
				"for storagepolicyusage CR: %s for CnsVolumeOperationRequest: %s",
				cnsvolumeoperationrequestObj.Status.StorageQuotaDetails.Reserved.String(),
				patchedStoragePolicyUsageCR.Name, cnsVolumeOperationRequestName)
			return
		} else {
			// This is a case where, the storagePolicyUsage CR does not have Status field.
			// The else{} block is usually executed for the 1st CreateVolume call after the
			// podVMOnStretchedSupervisor FSS is enabled
			var (
				usedQty     resource.Quantity
				reservedQty resource.Quantity
			)
			reservedQty = *cnsvolumeoperationrequestObj.Status.StorageQuotaDetails.Reserved
			patchedStoragePolicyUsageCR.Status = storagepolicyv1alpha2.StoragePolicyUsageStatus{
				ResourceTypeLevelQuotaUsage: &storagepolicyv1alpha2.QuotaUsageDetails{
					Reserved: &reservedQty,
					Used:     &usedQty,
				},
			}
			err := PatchStoragePolicyUsage(ctx, cnsOperatorClient, storagePolicyUsageCR,
				patchedStoragePolicyUsageCR)
			if err != nil {
				log.Errorf("updateStoragePolicyUsage failed. err: %v", err)
				return
			}
			log.Infof("cnsvolumeoperationrequestCRAdded: Successfully increased the reserved field by %s "+
				"for storagepolicyusage CR: %q", cnsvolumeoperationrequestObj.Status.StorageQuotaDetails.Reserved.String(),
				patchedStoragePolicyUsageCR.Name)
			return
		}
	}
}

// cnsvolumeoperationrequestCRDeleted is invoked when the the cnsvolumeoperationrequest instance is deleted
func cnsvolumeoperationrequestCRDeleted(obj interface{}) {
	ctx, log := logger.GetNewContextWithLogger()
	var cnsvolumeoperationrequestObj cnsvolumeoperationrequestv1alpha1.CnsVolumeOperationRequest
	err := runtime.DefaultUnstructuredConverter.FromUnstructured(
		obj.(*unstructured.Unstructured).Object, &cnsvolumeoperationrequestObj)
	if err != nil {
		log.Errorf("cnsvolumeoperationrequestCRDeleted: failed to cast object %+v to %s. Error: %+v", obj,
			cnsvolumeoperationrequest.CRDSingular, err)
		return
	}

	if cnsvolumeoperationrequestObj.Status.StorageQuotaDetails != nil &&
		cnsvolumeoperationrequestObj.Status.StorageQuotaDetails.Reserved != nil {
		// Update StoragePolicyUsage reserved field
		restConfig, err := config.GetConfig()
		if err != nil {
			log.Errorf("failed to get Kubernetes config. Err: %+v", err)
			return
		}
		cnsOperatorClient, err := k8s.NewClientForGroup(ctx, restConfig, cnsoperatorv1alpha1.GroupName)
		if err != nil {
			log.Errorf("Failed to create CnsOperator client. Err: %+v", err)
			return
		}
		cnsVolumeOperationRequestName := cnsvolumeoperationrequestObj.Name
		isSnapshot := checkOperationRequestCRForSnapshot(ctx, cnsVolumeOperationRequestName)
		storagePolicyUsageInstanceName := ""
		if isSnapshot {
			// Fetch StoragePolicyUsage instance for storageClass associated with the snapshot.
			log.Infof("cnsvolumeoperationrequestCRDeleted: Delete event receieved for snapshot operation "+
				"with snapshoID %s CnsVolumeOperationRequest: %s",
				cnsvolumeoperationrequestObj.Status.SnapshotID, cnsVolumeOperationRequestName)
			storagePolicyUsageInstanceName = cnsvolumeoperationrequestObj.Status.StorageQuotaDetails.
				StorageClassName + "-" + storagepolicyv1alpha2.NameSuffixForSnapshot
		} else {
			log.Infof("cnsvolumeoperationrequestCRDeleted: Delete event receieved for PVC operation "+
				"with volumeID %s CnsVolumeOperationRequest: %s",
				cnsvolumeoperationrequestObj.Status.VolumeID, cnsVolumeOperationRequestName)
			// Fetch StoragePolicyUsage instance for storageClass associated with the volume.
			storagePolicyUsageInstanceName = cnsvolumeoperationrequestObj.Status.StorageQuotaDetails.StorageClassName + "-" +
				storagepolicyv1alpha2.NameSuffixForPVC
		}
		storagePolicyUsageCR := &storagepolicyv1alpha2.StoragePolicyUsage{}
		err = cnsOperatorClient.Get(ctx, k8stypes.NamespacedName{
			Namespace: cnsvolumeoperationrequestObj.Status.StorageQuotaDetails.Namespace,
			Name:      storagePolicyUsageInstanceName},
			storagePolicyUsageCR)
		if err != nil {
			log.Errorf("failed to fetch %s instance with name %s from supervisor namespace %s. Error: %+v",
				storagepolicyv1alpha2.CRDSingular, storagePolicyUsageInstanceName,
				cnsvolumeoperationrequestObj.Status.StorageQuotaDetails.Namespace, err)
			return
		}
		patchedStoragePolicyUsageCR := storagePolicyUsageCR.DeepCopy()
		// This is a case where CnsVolumeOperationRequest is cleaned up due to CreateVolume failure.
		// Hence, the "reserved" field in StoragePolicyUsage needs to be decreased based on the
		// deleted CnsVolumeOperationRequest object's "reserved" field.
		patchedStoragePolicyUsageCR.Status.ResourceTypeLevelQuotaUsage.Reserved.Sub(
			*cnsvolumeoperationrequestObj.Status.StorageQuotaDetails.Reserved)
		err = PatchStoragePolicyUsage(ctx, cnsOperatorClient, storagePolicyUsageCR,
			patchedStoragePolicyUsageCR)
		if err != nil {
			log.Errorf("updateStoragePolicyUsage failed. err: %v", err)
			return
		}
		log.Infof("cnsvolumeoperationrequestCRDeleted: Successfully decreased the reserved field by %s "+
			"for storagepolicyusage CR: %s CnsVolumeOperationRequest: %s",
			cnsvolumeoperationrequestObj.Status.StorageQuotaDetails.Reserved.String(),
			patchedStoragePolicyUsageCR.Name, cnsVolumeOperationRequestName)
	}
}

// cnsvolumeoperationrequestCRUpdated checks if the cnsvolumeoperationrequest instance reserved field is updated
func cnsvolumeoperationrequestCRUpdated(oldObj interface{}, newObj interface{}) {
	ctx, log := logger.GetNewContextWithLogger()
	// Verify both objects received.
	var (
		oldcnsvolumeoperationrequestObj cnsvolumeoperationrequestv1alpha1.CnsVolumeOperationRequest
		newcnsvolumeoperationrequestObj cnsvolumeoperationrequestv1alpha1.CnsVolumeOperationRequest
	)
	err := runtime.DefaultUnstructuredConverter.FromUnstructured(
		newObj.(*unstructured.Unstructured).Object, &newcnsvolumeoperationrequestObj)
	if err != nil {
		log.Errorf("cnsvolumeoperationrequestCRUpdated: failed to cast new object %+v to %s. Error: %+v", newObj,
			cnsvolumeoperationrequest.CRDSingular, err)
		return
	}
	err = runtime.DefaultUnstructuredConverter.FromUnstructured(
		oldObj.(*unstructured.Unstructured).Object, &oldcnsvolumeoperationrequestObj)
	if err != nil {
		log.Errorf("cnsvolumeoperationrequestCRUpdated: failed to cast old object %+v to %s. Error: %+v", oldObj,
			cnsvolumeoperationrequest.CRDSingular, err)
		return
	}
	// Check for the below set of conditions:
	// 1. Previous Cnsvolumeoperationrequest object's StorageQuotaDetails should not be nil
	// 2. Previous Cnsvolumeoperationrequest object's StorageQuotaDetails.Reserved should not be nil
	// 3. Current Cnsvolumeoperationrequest object's StorageQuotaDetails should not be nil
	// 4. Current Cnsvolumeoperationrequest object's StorageQuotaDetails.Reserved should not be nil
	// 5. Current Cnsvolumeoperationrequest object's StorageQuotaDetails.Reserved value has changed
	if oldcnsvolumeoperationrequestObj.Status.StorageQuotaDetails != nil &&
		oldcnsvolumeoperationrequestObj.Status.StorageQuotaDetails.Reserved != nil &&
		newcnsvolumeoperationrequestObj.Status.StorageQuotaDetails != nil &&
		newcnsvolumeoperationrequestObj.Status.StorageQuotaDetails.Reserved != nil &&
		(oldcnsvolumeoperationrequestObj.Status.StorageQuotaDetails.Reserved.Value() !=
			newcnsvolumeoperationrequestObj.Status.StorageQuotaDetails.Reserved.Value()) {
		// Update StoragePolicyUsage reserved field
		restConfig, err := config.GetConfig()
		if err != nil {
			log.Errorf("failed to get Kubernetes config. Err: %+v", err)
			return
		}
		cnsOperatorClient, err := k8s.NewClientForGroup(ctx, restConfig, cnsoperatorv1alpha1.GroupName)
		if err != nil {
			log.Errorf("Failed to create CnsOperator client. Err: %+v", err)
			return
		}
		cnsVolumeOperationRequestName := newcnsvolumeoperationrequestObj.Name
		isSnapshot := checkOperationRequestCRForSnapshot(ctx, cnsVolumeOperationRequestName)
		storagePolicyUsageInstanceName := ""
		if isSnapshot {
			log.Infof("Update event receieved for snapshot operation on volumeID %s CnsVolumeOperationRequest: %s",
				newcnsvolumeoperationrequestObj.Status.VolumeID, cnsVolumeOperationRequestName)
			// Fetch StoragePolicyUsage instance for storageClass associated with the snapshot.
			storagePolicyUsageInstanceName = newcnsvolumeoperationrequestObj.Status.StorageQuotaDetails.
				StorageClassName + "-" + storagepolicyv1alpha2.NameSuffixForSnapshot

		} else {
			log.Infof("Update event receieved for volume operation on volumeID %s CnsVolumeOperationRequest: %s",
				newcnsvolumeoperationrequestObj.Status.VolumeID, cnsVolumeOperationRequestName)
			// Fetch StoragePolicyUsage instance for storageClass associated with the volume.
			storagePolicyUsageInstanceName = newcnsvolumeoperationrequestObj.Status.StorageQuotaDetails.StorageClassName + "-" +
				storagepolicyv1alpha2.NameSuffixForPVC
		}

		storagePolicyUsageCR := &storagepolicyv1alpha2.StoragePolicyUsage{}
		err = cnsOperatorClient.Get(ctx, k8stypes.NamespacedName{
			Namespace: newcnsvolumeoperationrequestObj.Status.StorageQuotaDetails.Namespace,
			Name:      storagePolicyUsageInstanceName},
			storagePolicyUsageCR)
		if err != nil {
			log.Errorf("failed to fetch %s instance with name %s from supervisor namespace %s. Error: %+v",
				storagepolicyv1alpha2.CRDSingular, storagePolicyUsageInstanceName,
				newcnsvolumeoperationrequestObj.Status.StorageQuotaDetails.Namespace, err)
			return
		}
		log.Infof("Fetched storagePolicyUsage CR %s", storagePolicyUsageCR.Name)
		patchedStoragePolicyUsageCR := storagePolicyUsageCR.DeepCopy()
		log.Debugf("old cnsvolumeoperationrequestObj %+v and new cnsvolumeoperationrequestObj %+v",
			oldcnsvolumeoperationrequestObj, newcnsvolumeoperationrequestObj)
		if newcnsvolumeoperationrequestObj.Status.StorageQuotaDetails.Reserved.Value() >
			oldcnsvolumeoperationrequestObj.Status.StorageQuotaDetails.Reserved.Value() {
			// This is a case where CSI Driver container increases the value of "reserved" field in
			// CnsVolumeOperationRequest during in-flight CreateVolume/CreateSnapshot operation. And subsequently,
			// the "reserved" field in StoragePolicyUsage needs to be increased based on the
			// CnsVolumeOperationRequest "reserved" field
			if storagePolicyUsageCR.Status.ResourceTypeLevelQuotaUsage != nil {
				// Move forward only if StoragePolicyUsage CR has Status.QuotaUsage fields not nil
				log.Infof("Increase the reserved value in storagePolicyUsage CR %s by %d",
					storagePolicyUsageCR.Name, newcnsvolumeoperationrequestObj.Status.StorageQuotaDetails.Reserved.Value())
				patchedStoragePolicyUsageCR.Status.ResourceTypeLevelQuotaUsage.Reserved.Add(
					*newcnsvolumeoperationrequestObj.Status.StorageQuotaDetails.Reserved)
				err := PatchStoragePolicyUsage(ctx, cnsOperatorClient, storagePolicyUsageCR,
					patchedStoragePolicyUsageCR)
				if err != nil {
					log.Errorf("updateStoragePolicyUsage failed. err: %v", err)
					return
				}
				log.Infof("cnsvolumeoperationrequestCRUpdated: Successfully increased the reserved field by %s "+
					"for storagepolicyusage CR: %s CnsVolumeOperationRequest: %s",
					newcnsvolumeoperationrequestObj.Status.StorageQuotaDetails.Reserved.String(),
					patchedStoragePolicyUsageCR.Name, cnsVolumeOperationRequestName)
			}
		} else {
			// This is a case where CSI Driver container decreases the value of "reserved" value in
			// CnsVolumeOperationRequest. This change can be done in 2 scenarios:
			// 1. after a successful Create/Expand volume operation: subsequently, the "reserved" field
			// needs to be decreased and the "used" field needs to be increased in StoragePolicyUsage CR.
			// 2. when the latest CNS task tracked by the CNSVolumeOperationRequest errors out: in this case,
			// just the "reserved" field in StoragePolicyUsage needs to be decreased.
			log.Infof("Decrease the reserved value in storagePolicyUsage CR %s by %s CnsVolumeOperationRequest: %s",
				storagePolicyUsageCR.Name, oldcnsvolumeoperationrequestObj.Status.StorageQuotaDetails.Reserved.String(),
				cnsVolumeOperationRequestName)
			patchedStoragePolicyUsageCR.Status.ResourceTypeLevelQuotaUsage.Reserved.Sub(
				*oldcnsvolumeoperationrequestObj.Status.StorageQuotaDetails.Reserved)
			var increaseUsed bool
			if !isSnapshot {
				// Fetch the latest task of the CNSVolumeOperationRequest instance and increment
				// "used" only when the task is successful.
				latestOps := newcnsvolumeoperationrequestObj.Status.LatestOperationDetails
				latestOp := latestOps[len(latestOps)-1]
				if latestOp.TaskStatus == cnsvolumeoperationrequest.TaskInvocationStatusSuccess {
					log.Debugf("Latest task %s in %s instance %s succeeded. Incrementing \"used\" "+
						"field in storagepolicyusage CR", latestOp.TaskID,
						cnsvolumeoperationrequest.CRDSingular, newcnsvolumeoperationrequestObj.Name)
					log.Infof("Increase the used value in storagePolicyUsage CR %s by %s "+
						" since the operation was successful CnsVolumeOperationRequest: %s",
						storagePolicyUsageCR.Name,
						oldcnsvolumeoperationrequestObj.Status.StorageQuotaDetails.Reserved.String(),
						cnsVolumeOperationRequestName)
					patchedStoragePolicyUsageCR.Status.ResourceTypeLevelQuotaUsage.Used.Add(
						*oldcnsvolumeoperationrequestObj.Status.StorageQuotaDetails.Reserved)
					increaseUsed = true
				}
			} else {
				log.Debug("skip increase `used` capacity, for snapshot operation cnsvolumeinfo informer will increase it")
			}
			err := PatchStoragePolicyUsage(ctx, cnsOperatorClient, storagePolicyUsageCR,
				patchedStoragePolicyUsageCR)
			if err != nil {
				log.Errorf("patching operation failed for StoragePolicyUsage CR: %s in namespace: %s. err: %v",
					patchedStoragePolicyUsageCR.Name, patchedStoragePolicyUsageCR.Namespace, err)
				return
			}
			log.Infof("cnsvolumeoperationrequestCRUpdated: Successfully decreased the reserved field "+
				"by %s for storagepolicyusage CR: %s in namespace: %s CnsVolumeOperationRequest: %s",
				oldcnsvolumeoperationrequestObj.Status.StorageQuotaDetails.Reserved.String(),
				patchedStoragePolicyUsageCR.Name, patchedStoragePolicyUsageCR.Namespace,
				cnsVolumeOperationRequestName)
			if increaseUsed {
				log.Infof("cnsvolumeoperationrequestCRUpdated: Successfully increased the used field "+
					"by %s for storagepolicyusage CR: %s in namespace: %s CnsVolumeOperationRequest: %s",
					oldcnsvolumeoperationrequestObj.Status.StorageQuotaDetails.Reserved.String(),
					patchedStoragePolicyUsageCR.Name, patchedStoragePolicyUsageCR.Namespace,
					cnsVolumeOperationRequestName)
			}
		}
	}
}

// checkOperationRequestCRForSnapshot will verify if the cnsvolumeopeationrequest CR event is generated
// for snapshot operation
func checkOperationRequestCRForSnapshot(ctx context.Context, operationReqCRName string) bool {
	cnsvolopreqInitial := ""
	if operationReqCRName != "" {
		opreqnameArr := strings.Split(operationReqCRName, "-")
		if len(opreqnameArr) > 0 {
			cnsvolopreqInitial = opreqnameArr[0]
			if cnsvolopreqInitial == "snapshot" || cnsvolopreqInitial == "deletesnapshot" {
				return true
			}
		}
	}
	return false
}

// topoCRAdded checks if the CSINodeTopology instance Status is set to Success
// and populates the MetadataSyncer.topologyVCMap with appropriate values.
func topoCRAdded(obj interface{}) {
	ctx, log := logger.GetNewContextWithLogger()
	// Verify object received.
	var nodeTopoObj csinodetopologyv1alpha1.CSINodeTopology
	err := runtime.DefaultUnstructuredConverter.FromUnstructured(obj.(*unstructured.Unstructured).Object, &nodeTopoObj)
	if err != nil {
		log.Errorf("topoCRAdded: failed to cast object %+v to %s. Error: %v", obj,
			csinodetopology.CRDSingular, err)
		return
	}
	// Check if Status is set to Success.
	if nodeTopoObj.Status.Status != csinodetopologyv1alpha1.CSINodeTopologySuccess {
		log.Infof("topoCRAdded: CSINodeTopology instance %q not yet ready. Status: %q",
			nodeTopoObj.Name, nodeTopoObj.Status.Status)
		return
	}
	addLabelsToTopologyVCMap(ctx, nodeTopoObj)
}

// topoCRUpdated checks if the CSINodeTopology instance Status is set to Success
// and populates the MetadataSyncer.topologyVCMap with appropriate values.
func topoCRUpdated(oldObj interface{}, newObj interface{}) {
	ctx, log := logger.GetNewContextWithLogger()
	// Verify both objects received.
	var (
		oldNodeTopoObj csinodetopologyv1alpha1.CSINodeTopology
		newNodeTopoObj csinodetopologyv1alpha1.CSINodeTopology
	)
	err := runtime.DefaultUnstructuredConverter.FromUnstructured(
		newObj.(*unstructured.Unstructured).Object, &newNodeTopoObj)
	if err != nil {
		log.Errorf("topoCRUpdated: failed to cast new object %+v to %s. Error: %+v", newObj,
			csinodetopology.CRDSingular, err)
		return
	}
	err = runtime.DefaultUnstructuredConverter.FromUnstructured(
		oldObj.(*unstructured.Unstructured).Object, &oldNodeTopoObj)
	if err != nil {
		log.Errorf("topoCRUpdated: failed to cast old object %+v to %s. Error: %+v", oldObj,
			csinodetopology.CRDSingular, err)
		return
	}
	// Check if there is any change in the topology labels.
	oldTopoLabelsMap := make(map[string]string)
	for _, label := range oldNodeTopoObj.Status.TopologyLabels {
		oldTopoLabelsMap[label.Key] = label.Value
	}
	newTopoLabelsMap := make(map[string]string)
	for _, label := range newNodeTopoObj.Status.TopologyLabels {
		newTopoLabelsMap[label.Key] = label.Value
	}
	// Check if there are updates to the topology labels in the Status.
	if reflect.DeepEqual(oldTopoLabelsMap, newTopoLabelsMap) {
		log.Debugf("topoCRUpdated: No change in %s CR topology labels. Ignoring the event",
			csinodetopology.CRDSingular)
		return
	}
	// Ideally a CSINodeTopology CR should never be updated after the status is set to Success but
	// in cases where this does happen, in order to maintain the correctness of domainNodeMap, we
	// will first remove the node name from previous topology labels before adding the new values.
	if oldNodeTopoObj.Status.Status == csinodetopologyv1alpha1.CSINodeTopologySuccess {
		log.Warnf("topoCRUpdated: %q instance with name %q has been updated after the Status was set to "+
			"Success. Old object - %+v. New object - %+v", csinodetopology.CRDSingular, oldNodeTopoObj.Name,
			oldNodeTopoObj, newNodeTopoObj)
		removeLabelsFromTopologyVCMap(ctx, oldNodeTopoObj)
	}
	if newNodeTopoObj.Status.Status == csinodetopologyv1alpha1.CSINodeTopologySuccess {
		addLabelsToTopologyVCMap(ctx, newNodeTopoObj)
	}
}

// topoCRDeleted removes the topology to VC mapping for the deleted CSINodeTopology
// instance from the MetadataSyncer.topologyVCMap.
func topoCRDeleted(obj interface{}) {
	ctx, log := logger.GetNewContextWithLogger()
	// Verify object received.
	if unknown, ok := obj.(cache.DeletedFinalStateUnknown); ok {
		if unknown.Obj == nil {
			log.Errorf("topoCRDeleted: received empty DeletedFinalStateUnknown object, ignoring")
			return
		}
		obj = unknown.Obj
	}
	unstruct, ok := obj.(*unstructured.Unstructured)
	if !ok {
		log.Errorf("topoCRDeleted: received non-unstructured object %T, ignoring", obj)
		return
	}
	var nodeTopoObj csinodetopologyv1alpha1.CSINodeTopology
	err := runtime.DefaultUnstructuredConverter.FromUnstructured(unstruct.Object, &nodeTopoObj)
	if err != nil {
		log.Errorf("topoCRDeleted: failed to cast object %+v to %s type. Error: %+v",
			obj, csinodetopology.CRDSingular, err)
		return
	}
	// Delete topology labels from MetadataSyncer.topologyVCMap if the status of the CR was set to Success.
	if nodeTopoObj.Status.Status == csinodetopologyv1alpha1.CSINodeTopologySuccess {
		removeLabelsFromTopologyVCMap(ctx, nodeTopoObj)
	} else {
		log.Debugf("topoCRDeleted: %q instance with name %q and status %q deleted. "+
			"Ignoring update to topologyVCMap", csinodetopology.CRDSingular, nodeTopoObj.Name,
			nodeTopoObj.Status.Status)
	}
}

// removeLabelsFromTopologyVCMap removes the topology label to VC mapping for given CSINodeTopology
// instance in the MetadataSyncer.topologyVCMap parameter.
func removeLabelsFromTopologyVCMap(ctx context.Context, nodeTopoObj csinodetopologyv1alpha1.CSINodeTopology) {
	log := logger.GetLogger(ctx)
	nodeVM, err := nodeMgr.GetNodeVMAndUpdateCache(ctx, nodeTopoObj.Spec.NodeUUID, nil)
	if err != nil {
		log.Errorf("Node %q is not yet registered in the node manager. Error: %+v",
			nodeTopoObj.Spec.NodeUUID, err)
		return
	}
	log.Infof("Removing VC %q mapping for TopologyLabels %+v.", nodeVM.VirtualCenterHost,
		nodeTopoObj.Status.TopologyLabels)
	for _, label := range nodeTopoObj.Status.TopologyLabels {
		delete(MetadataSyncer.topologyVCMap[label.Value], nodeVM.VirtualCenterHost)
	}
}

// getVCForTopologySegments uses the MetadataSyncer.topologyVCMap parameter to
// retrieve the VC instance for the given topology segments map.
func getVCForTopologySegments(ctx context.Context, topologySegments map[string][]string) (string, error) {
	log := logger.GetLogger(ctx)
	// vcCountMap keeps a cumulative count of the occurrences of
	// VCs across all labels in the given topology segment.
	vcCountMap := make(map[string]int)

	// Find the VC which contains all the labels given in the topologySegments.
	// For example, if topologyVCMap looks like
	// {"region-1": {"vc1": struct{}{}, "vc2": struct{}{} },
	// "zone-1": {"vc1": struct{}{} },
	// "zone-2": {"vc2": struct{}{} },}
	// For a given topologySegment, we will end up with a vcCountMap as follows: {"vc1": 1, "vc2": 2}
	// We go over the vcCountMap to check which VC has a count equal to the len(topologySegment).
	// If we get a single VC match, we return this VC.
	numTopoLabels := 0
	for topologyKey, labelList := range topologySegments {
		for _, label := range labelList {
			if vcList, exists := MetadataSyncer.topologyVCMap[label]; exists {
				numTopoLabels++
				for vc := range vcList {
					vcCountMap[vc] = vcCountMap[vc] + 1
				}
			} else {
				return "", logger.LogNewErrorf(log, "Topology label %q not found in topology to VC mapping.",
					topologyKey+":"+label)
			}
		}
	}
	var commonVCList []string
	for vc, count := range vcCountMap {
		// Add VCs to the commonVCList if they satisfied all the labels in the topology segment.
		if count == numTopoLabels {
			commonVCList = append(commonVCList, vc)
		}
	}
	switch {
	case len(commonVCList) > 1:
		return "", logger.LogNewErrorf(log, "Topology segment(s) %+v belong to more than one VC: %+v",
			topologySegments, commonVCList)
	case len(commonVCList) == 1:
		log.Infof("Topology segment(s) %+v belong to VC: %q", topologySegments, commonVCList[0])
		return commonVCList[0], nil
	}
	return "", logger.LogNewErrorf(log, "failed to find the VC associated with topology segments %+v",
		topologySegments)
}

// getTriggerCsiFullSyncInstance gets the full sync instance with name
// "csifullsync".
func getTriggerCsiFullSyncInstance(ctx context.Context,
	client client.Client) (*triggercsifullsyncv1alpha1.TriggerCsiFullSync, error) {
	log := logger.GetLogger(ctx)
	log.Info("get triggercsifullsync instance")
	triggerCsiFullSyncInstance := &triggercsifullsyncv1alpha1.TriggerCsiFullSync{}
	key := k8stypes.NamespacedName{Namespace: "", Name: common.TriggerCsiFullSyncCRName}
	if err := client.Get(ctx, key, triggerCsiFullSyncInstance); err != nil {
		log.Errorf("error get triggercsifullsync instance %+v", err)
		return nil, err
	}
	return triggerCsiFullSyncInstance, nil
}

// updateTriggerCsiFullSyncInstance updates the full sync instance with
// name "csifullsync".
func updateTriggerCsiFullSyncInstance(ctx context.Context,
	client client.Client, instance *triggercsifullsyncv1alpha1.TriggerCsiFullSync) error {
	log := logger.GetLogger(ctx)
	log.Info("updating trigger csi fullsync instance")
	if err := client.Update(ctx, instance); err != nil {
		return err
	}
	log.Info("successfully update triggercsifullsync instance")
	return nil
}

// ReloadConfiguration reloads configuration from the secret, and update
// controller's cached configs. The function takes metadatasyncerInformer and
// reconnectToVCFromNewConfig as parameters. If reconnectToVCFromNewConfig
// is set to true, the function re-establishes connection with VC. Otherwise,
// based on the configuration data changed during reload, the function resets
// config, reloads VC connection when credentials are changed and returns
// appropriate error.
func ReloadConfiguration(metadataSyncer *metadataSyncInformer, reconnectToVCFromNewConfig bool) error {
	ctx, log := logger.GetNewContextWithLogger()
	log.Info("Reloading Configuration")
	var cfg *cnsconfig.Config
	var err error
	if metadataSyncer.clusterFlavor == cnstypes.CnsClusterFlavorVanilla &&
		commonco.ContainerOrchestratorUtility.IsFSSEnabled(ctx, common.CSIInternalGeneratedClusterID) {
		cfg, err = getConfig(ctx)
	} else {
		cfg, err = cnsconfig.GetConfig(ctx)
	}

	if err != nil {
		return logger.LogNewErrorf(log, "failed to read config. Error: %+v", err)
	}
	if metadataSyncer.clusterFlavor == cnstypes.CnsClusterFlavorWorkload {
		if !cfg.Global.InsecureFlag && cfg.Global.CAFile != cnsconfig.SupervisorCAFilePath {
			log.Warnf("Invalid CA file: %q is set in the vSphere Config Secret. "+
				"Setting correct CA file: %q", cfg.Global.CAFile, cnsconfig.SupervisorCAFilePath)
			cfg.Global.CAFile = cnsconfig.SupervisorCAFilePath
		}
	}

	if metadataSyncer.clusterFlavor == cnstypes.CnsClusterFlavorGuest {
		var err error
		restClientConfig := k8s.GetRestClientConfigForSupervisor(ctx,
			cfg.GC.Endpoint, metadataSyncer.configInfo.Cfg.GC.Port)
		metadataSyncer.cnsOperatorClient, err = k8s.NewClientForGroup(ctx,
			restClientConfig, cnsoperatorv1alpha1.GroupName)
		if err != nil {
			return logger.LogNewErrorf(log, "failed to create cns operator client. Err: %v", err)
		}

		metadataSyncer.supervisorClient, err = k8s.NewSupervisorClient(ctx, restClientConfig)
		if err != nil {
			return logger.LogNewErrorf(log, "failed to create supervisorClient. Error: %+v", err)
		}
	}

	if metadataSyncer.clusterFlavor != cnstypes.CnsClusterFlavorWorkload &&
		metadataSyncer.clusterFlavor != cnstypes.CnsClusterFlavorGuest {
		isStorageQuotaM2FSSEnabled = commonco.ContainerOrchestratorUtility.IsFSSEnabled(ctx, common.StorageQuotaM2)
		// Vanilla ReloadConfiguration
		newVcenterConfigs, err := cnsvsphere.GetVirtualCenterConfigs(ctx, cfg)
		if err != nil {
			return logger.LogNewErrorf(log, "failed to get VirtualCenterConfigs. err=%v", err)
		}
		if newVcenterConfigs != nil {
			for _, newVCConfig := range newVcenterConfigs {
				newVCConfig.ReloadVCConfigForNewClient = true
				if metadataSyncer.volumeManagers[newVCConfig.Host] == nil {
					log.Infof("Observed new vCenter server: %q in the config secret. "+
						"Existing syncer Container for re-initialization", newVCConfig.Host)
					unregisterAllvCenterErr := cnsvsphere.UnregisterAllVirtualCenters(ctx)
					if unregisterAllvCenterErr != nil {
						log.Warnf("failed to Unregister all vCenter servers. Error: %v. "+
							"Proceeding to exit the syncer container for re-initialization", unregisterAllvCenterErr)
					}
					os.Exit(1)
				}
				var vcenter *cnsvsphere.VirtualCenter
				vcenter, err = cnsvsphere.GetVirtualCenterInstanceForVCenterHost(ctx, newVCConfig.Host, false)
				if err != nil {
					return logger.LogNewErrorf(log, "failed to get VirtualCenter. err=%v", err)
				}
				vcenter.Config = newVCConfig
				err := metadataSyncer.volumeManagers[newVCConfig.Host].ResetManager(ctx, vcenter)
				if err != nil {
					return logger.LogNewErrorf(log, "failed to reset updated VC object in volumemanager for vCenter: %q "+
						"err=%v", newVCConfig.Host, err)
				}
			}
			if cfg != nil {
				metadataSyncer.configInfo = &cnsconfig.ConfigurationInfo{Cfg: cfg}
				log.Infof("updated metadataSyncer.configInfo")
			}
		}
	}

	if metadataSyncer.clusterFlavor == cnstypes.CnsClusterFlavorWorkload {
		newVCConfig, err := cnsvsphere.GetVirtualCenterConfig(ctx, cfg)
		if err != nil {
			return logger.LogNewErrorf(log, "failed to get VirtualCenterConfig. err=%v", err)
		}
		isStorageQuotaM2FSSEnabled = commonco.ContainerOrchestratorUtility.IsFSSEnabled(ctx, common.StorageQuotaM2)
		if newVCConfig != nil {
			var vcenter *cnsvsphere.VirtualCenter
			newVCConfig.ReloadVCConfigForNewClient = true
			vcConfig := metadataSyncer.configInfo.Cfg.VirtualCenter[metadataSyncer.host]
			if metadataSyncer.host != newVCConfig.Host ||
				vcConfig.User != newVCConfig.Username ||
				vcConfig.Password != newVCConfig.Password ||
				vcConfig.VCSessionManagerURL != newVCConfig.VCSessionManagerURL ||
				vcConfig.VCSessionManagerToken != newVCConfig.VCSessionManagerToken ||
				reconnectToVCFromNewConfig {
				// Verify if new configuration has valid credentials by connecting
				// to vCenter. Proceed only if the connection succeeds, else return
				// error.
				newVC := &cnsvsphere.VirtualCenter{Config: newVCConfig, ClientMutex: &sync.Mutex{}}
				if err = newVC.Connect(ctx); err != nil {
					return logger.LogNewErrorf(log,
						"failed to connect to VirtualCenter host: %s using new credentials, Err: %+v",
						newVCConfig.Host, err)
				}

				// Reset virtual center singleton instance by passing reload flag
				// as true.
				log.Info("Obtaining new vCenterInstance using new credentials")
				vcenter, err = cnsvsphere.GetVirtualCenterInstance(ctx, &cnsconfig.ConfigurationInfo{Cfg: cfg}, true)
				if err != nil {
					return logger.LogNewErrorf(log, "failed to get VirtualCenter. err=%v", err)
				}
			} else {
				// If it's not a VC host or VC credentials update, same singleton
				// instance can be used and it's Config field can be updated.
				vcenter, err = cnsvsphere.GetVirtualCenterInstance(ctx, &cnsconfig.ConfigurationInfo{Cfg: cfg}, false)
				if err != nil {
					return logger.LogNewErrorf(log, "failed to get VirtualCenter. err=%v", err)
				}
				vcenter.Config = newVCConfig
			}
			err := metadataSyncer.volumeManager.ResetManager(ctx, vcenter)
			if err != nil {
				return logger.LogNewErrorf(log, "failed to reset volume manager. err=%v", err)
			}
			volumeManager, err := volumes.GetManager(ctx, vcenter, nil, false, false, false,
				metadataSyncer.clusterFlavor)
			if err != nil {
				return logger.LogNewErrorf(log, "failed to create an instance of volume manager. err=%v", err)
			}
			metadataSyncer.volumeManager = volumeManager
			storagepool.ResetVC(ctx, vcenter)

			metadataSyncer.host = newVCConfig.Host
		}
		if cfg != nil {
			metadataSyncer.configInfo = &cnsconfig.ConfigurationInfo{Cfg: cfg}
			log.Infof("updated metadataSyncer.configInfo")
		}
	}
	return nil
}

// pvcUpdated updates persistent volume claim metadata on VC when pvc labels
// on K8S cluster have been updated.
func pvcUpdated(oldObj, newObj interface{}, metadataSyncer *metadataSyncInformer) {
	ctx, log := logger.GetNewContextWithLogger()
	// Get old and new pvc objects.
	oldPvc, ok := oldObj.(*v1.PersistentVolumeClaim)
	if oldPvc == nil || !ok {
		return
	}
	newPvc, ok := newObj.(*v1.PersistentVolumeClaim)
	if newPvc == nil || !ok {
		return
	}
	log.Debugf("PVCUpdated: PVC Updated from %+v to %+v", oldPvc, newPvc)
	if newPvc.Status.Phase != v1.ClaimBound {
		log.Debugf("PVCUpdated: New PVC not in Bound phase")
		return
	}

	// Get pv object attached to pvc.
	pv, err := metadataSyncer.pvLister.Get(newPvc.Spec.VolumeName)
	if pv == nil || err != nil {
		if !apierrors.IsNotFound(err) {
			log.Errorf("PVCUpdated: Error getting Persistent Volume for pvc %s in namespace %s with err: %v",
				newPvc.Name, newPvc.Namespace, err)
			return
		}
		log.Infof("PVCUpdated: PV with name %s not found using PV Lister. Querying API server to get PV Info",
			newPvc.Spec.VolumeName)
		// Create the kubernetes client from config.
		k8sClient, err := k8s.NewClient(ctx)
		if err != nil {
			log.Errorf("PVCUpdated: Creating Kubernetes client failed. Err: %v", err)
			return
		}
		pv, err = k8sClient.CoreV1().PersistentVolumes().Get(ctx, newPvc.Spec.VolumeName, metav1.GetOptions{})
		if err != nil {
			log.Errorf("PVCUpdated: Error getting Persistent Volume %s from API server with err: %v",
				newPvc.Spec.VolumeName, err)
			return
		}
		log.Debugf("PVCUpdated: Found Persistent Volume %s from API server", newPvc.Spec.VolumeName)
	}

	// Verify if csi migration is ON and check if there is any label update or
	// migrated-to annotation was received for the PVC.
	if IsMigrationEnabled && pv.Spec.VsphereVolume != nil {
		// If it is a multi VC setup, then skip this volume as we do not support vSphere to CSI migrated volumes
		// on a multi VC deployment.
		if len(metadataSyncer.configInfo.Cfg.VirtualCenter) > 1 {
			log.Infof("PVCUpdated: %q is a vSphere volume claim in namespace %q."+
				"In-tree vSphere volume are not supported in a multi VC setup. Skipping update",
				newPvc.Name, newPvc.Namespace)
			return
		}

		if !isValidvSphereVolumeClaim(ctx, newPvc.ObjectMeta) {
			if !isValidvSphereVolume(ctx, pv) {
				log.Debugf("PVCUpdated: %q is not a valid vSphere volume claim in namespace %q. Skipping update",
					newPvc.Name, newPvc.Namespace)
				return
			}
		}
		if oldPvc.Status.Phase == v1.ClaimBound &&
			reflect.DeepEqual(newPvc.GetAnnotations(), oldPvc.GetAnnotations()) &&
			reflect.DeepEqual(newPvc.Labels, oldPvc.Labels) {
			log.Debugf("PVCUpdated: PVC labels and annotations have not changed for %s in namespace %s",
				newPvc.Name, newPvc.Namespace)
			return
		}
		// Verify if there is an annotation update
		if !reflect.DeepEqual(newPvc.GetAnnotations(), oldPvc.GetAnnotations()) {
			// Verify if the annotation update is related to migration. If not,
			// return.
			if !HasMigratedToAnnotationUpdate(ctx, oldPvc.GetAnnotations(), newPvc.GetAnnotations(), newPvc.Name) {
				log.Debugf("PVCUpdated: Migrated-to annotation is not added for %s in namespace %s. "+
					"Ignoring other annotation updates", newPvc.Name, newPvc.Namespace)
				// Check if there are no label update, then return.
				if !reflect.DeepEqual(newPvc.Labels, oldPvc.Labels) {
					return
				}
			}
		}
	} else {
		if pv.Spec.VsphereVolume != nil {
			// Volume is in-tree VCP volume.
			log.Warnf("PVCUpdated: %q feature state is disabled. Skipping the PVC update", common.CSIMigration)
			return
		}
		// Verify if pv is vsphere csi volume.
		if pv.Spec.CSI == nil || pv.Spec.CSI.Driver != csitypes.Name {
			log.Debugf("PVCUpdated: Not a vSphere CSI Volume")
			return
		}
		// For volumes provisioned by CSI driver, verify if old and new labels are not equal.
		if oldPvc.Status.Phase == v1.ClaimBound && reflect.DeepEqual(newPvc.Labels, oldPvc.Labels) {
			log.Debugf("PVCUpdated: Old PVC and New PVC labels equal")
			return
		}
	}

	if metadataSyncer.clusterFlavor == cnstypes.CnsClusterFlavorGuest {
		// Invoke volume updated method for pvCSI.
		pvcsiVolumeUpdated(ctx, newPvc, pv.Spec.CSI.VolumeHandle, metadataSyncer)
	} else {
		csiPVCUpdated(ctx, newPvc, pv, metadataSyncer)
	}
}

// pvcDeleted deletes pvc metadata on VC when pvc has been deleted on K8s
// cluster.
func pvcDeleted(obj interface{}, metadataSyncer *metadataSyncInformer) {
	ctx, log := logger.GetNewContextWithLogger()
	pvc, ok := obj.(*v1.PersistentVolumeClaim)
	if pvc == nil || !ok {
		log.Warnf("PVCDeleted: unrecognized object %+v", obj)
		return
	}
	log.Debugf("PVCDeleted: %+v", pvc)
	if pvc.Status.Phase != v1.ClaimBound {
		return
	}
	// Get pv object attached to pvc.
	pv, err := metadataSyncer.pvLister.Get(pvc.Spec.VolumeName)
	if pv == nil || err != nil {
		log.Errorf("PVCDeleted: Error getting Persistent Volume for pvc %s in namespace %s with err: %v",
			pvc.Name, pvc.Namespace, err)
		return
	}

	if IsMigrationEnabled && pv.Spec.VsphereVolume != nil {
		// If it is a multi VC setup, then skip this volume as we do not support vSphere to CSI migrated volumes
		// on a multi VC deployment.
		if len(metadataSyncer.configInfo.Cfg.VirtualCenter) > 1 {
			log.Infof("PVCDeleted: %q is a vSphere volume claim in namespace %q."+
				"In-tree vSphere volume are not supported in a multi VC setup. Skipping delettion of PVC metadata.",
				pvc.Name, pvc.Namespace)
			return
		}

		if !isValidvSphereVolumeClaim(ctx, pvc.ObjectMeta) {
			if !isValidvSphereVolume(ctx, pv) {
				log.Debugf("PVCDeleted: %q is not a valid vSphere volume claim in namespace %q. "+
					"Skipping deletion of PVC metadata.", pvc.Name, pvc.Namespace)
				return
			}
		}
	} else {
		if pv.Spec.VsphereVolume != nil {
			// Volume is in-tree VCP volume.
			log.Warnf("PVCDeleted: %q feature state is disabled. Skipping the PVC delete", common.CSIMigration)
			return
		}
		// Verify if pv is vSphere csi volume.
		if pv.Spec.CSI == nil || pv.Spec.CSI.Driver != csitypes.Name {
			log.Debugf("PVCDeleted: Not a vSphere CSI Volume")
			return
		}
	}
	if metadataSyncer.clusterFlavor == cnstypes.CnsClusterFlavorGuest {
		// Invoke volume deleted method for pvCSI.
		pvcsiVolumeDeleted(ctx, string(pvc.GetUID()), metadataSyncer, pv)
	} else {
		csiPVCDeleted(ctx, pvc, pv, metadataSyncer)
	}
}

// pvAdded updates the PV labels with linkedclone's volumesnapshot uuid
func pvAdded(obj interface{}, metadataSyncer *metadataSyncInformer) {
	if !IsLinkedCloneSupportFSSEnabled {
		return
	}
	if metadataSyncer.clusterFlavor != cnstypes.CnsClusterFlavorGuest {
		return
	}
	ctx, log := logger.GetNewContextWithLogger()
	pv, ok := obj.(*v1.PersistentVolume)
	if pv == nil || !ok {
		log.Warnf("pvAdded: unrecognized object %+v", obj)
		return
	}
	log.Debugf("pvAdded: PV: %+v", pv)
	// Verify if pv is a vSphere csi volume.
	if pv.Spec.CSI == nil || pv.Spec.CSI.Driver != csitypes.Name {
		log.Debugf("pvAdded: Not a vSphere CSI Volume. PV: %+v", pv)
		return
	}

	vsUID, ok := pv.Spec.CSI.VolumeAttributes[common.VolumeContextAttributeLinkedCloneVolumeSnapshotSourceUID]
	if !ok {
		// Not a linked clone volume, nothing to do
		return
	}
	err := metadataSyncer.coCommonInterface.UpdatePersistentVolumeLabel(ctx, pv.Name,
		common.VolumeContextAttributeLinkedCloneVolumeSnapshotSourceUID, vsUID)
	if err != nil {
		log.Errorf("PVAdded: Error updating PV label with key: %s, value: %s error: %v",
			common.VolumeContextAttributeLinkedCloneVolumeSnapshotSourceUID, vsUID, err)
		return
	}
}

// pvUpdated updates volume metadata on VC when volume labels on K8S cluster
// have been updated.
func pvUpdated(oldObj, newObj interface{}, metadataSyncer *metadataSyncInformer) {
	ctx, log := logger.GetNewContextWithLogger()
	// Get old and new PV objects.
	oldPv, ok := oldObj.(*v1.PersistentVolume)
	if oldPv == nil || !ok {
		log.Warnf("PVUpdated: unrecognized old object %+v", oldObj)
		return
	}

	newPv, ok := newObj.(*v1.PersistentVolume)
	if newPv == nil || !ok {
		log.Warnf("PVUpdated: unrecognized new object %+v", newObj)
		return
	}
	log.Debugf("PVUpdated: PV Updated from %+v to %+v", oldPv, newPv)

	// Return if new PV status is Pending or Failed.
	if newPv.Status.Phase == v1.VolumePending || newPv.Status.Phase == v1.VolumeFailed {
		log.Debugf("PVUpdated: PV %s metadata is not updated since updated PV is in phase %s",
			newPv.Name, newPv.Status.Phase)
		return
	}
	if IsMigrationEnabled && newPv.Spec.VsphereVolume != nil {

		// If it is a multi VC setup, then skip this volume as we do not support vSphere to CSI migrated volumes
		// on a multi VC deployment.
		if len(metadataSyncer.configInfo.Cfg.VirtualCenter) > 1 {
			log.Infof("PVUpdated: %q is a vSphere volume claim in namespace %q."+
				"In-tree vSphere volume are not supported in a multi VC setup."+
				"Skipping PV update.", newPv.Name, newPv.Namespace)
			return
		}

		if !isValidvSphereVolume(ctx, newPv) {
			log.Debugf("PVUpdated: PV %q is not a valid vSphere volume. Skipping update of PV metadata.", newPv.Name)
			return
		}
		if (oldPv.Status.Phase == v1.VolumeAvailable || oldPv.Status.Phase == v1.VolumeBound) &&
			reflect.DeepEqual(newPv.GetAnnotations(), oldPv.GetAnnotations()) &&
			reflect.DeepEqual(newPv.Labels, oldPv.Labels) {
			log.Debug("PVUpdated: PV labels and annotations have not changed")
			return
		}
		// Verify if migration annotation is getting removed.
		if !reflect.DeepEqual(newPv.GetAnnotations(), oldPv.GetAnnotations()) {
			// Verify if the annotation update is related to migration.
			// If not, return.
			if !HasMigratedToAnnotationUpdate(ctx, oldPv.GetAnnotations(), newPv.GetAnnotations(), newPv.Name) {
				log.Debugf("PVUpdated: Migrated-to annotation is not added for %q. Ignoring other annotation updates",
					newPv.Name)
				// Check if there are no label update, then return.
				if !reflect.DeepEqual(newPv.Labels, oldPv.Labels) {
					return
				}
			}
		}
	} else {
		if newPv.Spec.VsphereVolume != nil {
			// Volume is in-tree VCP volume.
			log.Warnf("PVUpdated: %q feature state is disabled. Skipping the PV update", common.CSIMigration)
			return
		}
		// Verify if pv is a vSphere csi volume.
		if newPv.Spec.CSI == nil || newPv.Spec.CSI.Driver != csitypes.Name {
			log.Debugf("PVUpdated: PV is not a vSphere CSI Volume: %+v", newPv)
			return
		}
		// Return if labels are unchanged.
		if (oldPv.Status.Phase == v1.VolumeAvailable || oldPv.Status.Phase == v1.VolumeBound) &&
			reflect.DeepEqual(newPv.GetLabels(), oldPv.GetLabels()) {
			log.Debugf("PVUpdated: PV labels have not changed")
			return
		}
	}
	if oldPv.Status.Phase == v1.VolumeBound && newPv.Status.Phase == v1.VolumeReleased &&
		oldPv.Spec.PersistentVolumeReclaimPolicy == v1.PersistentVolumeReclaimDelete {
		log.Debugf("PVUpdated: Volume will be deleted by controller")
		return
	}
	if newPv.DeletionTimestamp != nil {
		log.Debugf("PVUpdated: PV already deleted")
		return
	}
	if metadataSyncer.clusterFlavor == cnstypes.CnsClusterFlavorGuest {
		// Invoke volume updated method for pvCSI.
		pvcsiVolumeUpdated(ctx, newPv, newPv.Spec.CSI.VolumeHandle, metadataSyncer)
	} else {
		csiPVUpdated(ctx, newPv, oldPv, metadataSyncer)
	}
}

// pvDeleted deletes volume metadata on VC when volume has been deleted on
// K8s cluster.
func pvDeleted(obj interface{}, metadataSyncer *metadataSyncInformer) {
	ctx, log := logger.GetNewContextWithLogger()
	pv, ok := obj.(*v1.PersistentVolume)
	if pv == nil || !ok {
		log.Warnf("PVDeleted: unrecognized object %+v", obj)
		return
	}
	log.Debugf("PVDeleted: PV: %+v", pv)

	if IsMigrationEnabled && pv.Spec.VsphereVolume != nil {

		// If it is a multi VC setup, then skip this volume as we do not support vSphere to CSI migrated volumes
		// on a multi VC deployment.
		if len(metadataSyncer.configInfo.Cfg.VirtualCenter) > 1 {
			log.Infof("PVUpdated: %q is a vSphere volume claim in namespace %q."+
				"In-tree vSphere volume are not supported in a multi VC setup."+
				"Skipping deletion of PV metadata.", pv.Name, pv.Namespace)
			return
		}

		if !isValidvSphereVolume(ctx, pv) {
			log.Debugf("PVDeleted: PV %q is not a valid vSphereVolume. Skipping deletion of PV metadata.", pv.Name)
			return
		}
	} else {
		if pv.Spec.VsphereVolume != nil {
			// Volume is in-tree VCP volume.
			log.Warnf("PVDeleted: %q feature state is disabled. Skipping the PVC update", common.CSIMigration)
			return
		}
		// Verify if pv is a vSphere csi volume.
		if pv.Spec.CSI == nil || pv.Spec.CSI.Driver != csitypes.Name {
			log.Debugf("PVDeleted: Not a vSphere CSI Volume. PV: %+v", pv)
			return
		}
	}
	if metadataSyncer.clusterFlavor == cnstypes.CnsClusterFlavorGuest {
		// Invoke volume deleted method for pvCSI.
		pvcsiVolumeDeleted(ctx, string(pv.GetUID()), metadataSyncer, pv)
	} else {
		csiPVDeleted(ctx, pv, metadataSyncer)
	}
}

// podAdded helps register inline vSphere in-tree volumes.
// NOTE: This functionality will be skipped if it is called in a multi-VC environment.
func podAdded(obj interface{}, metadataSyncer *metadataSyncInformer) {
	ctx, log := logger.GetNewContextWithLogger()
	if metadataSyncer.clusterFlavor == cnstypes.CnsClusterFlavorVanilla && IsMigrationEnabled {
		// Get pod object.
		pod, ok := obj.(*v1.Pod)
		if pod == nil || !ok {
			log.Warnf("podAdded: unrecognized new object %+v", obj)
			return
		}
		if len(metadataSyncer.configInfo.Cfg.VirtualCenter) > 1 {
			log.Info("podAdded: In-tree vSphere volume are not supported in a multi VC setup. " +
				"Skipping addition of Pod metadata.")
			return
		}
		// In case if feature state switch is enabled after syncer is
		// deployed, we need to initialize the volumeMigrationService.
		if err := initVolumeMigrationService(ctx, metadataSyncer); err != nil {
			log.Errorf("podAdded: failed to get migration service. Err: %v", err)
			return
		}
		for _, volume := range pod.Spec.Volumes {
			// Migrated in-line in-tree vSphere volumes
			if volume.VsphereVolume != nil {
				log.Infof("Registering in-tree vSphere inline volume: %q", volume.VsphereVolume.VolumePath)
				volumeHandle, err := volumeMigrationService.GetVolumeID(ctx,
					&migration.VolumeSpec{VolumePath: volume.VsphereVolume.VolumePath}, true)
				if err != nil {
					log.Warnf("podAdded: Failed to get VolumeID from "+
						"volumeMigrationService for volumePath: %q with error %+v", volume.VsphereVolume.VolumePath, err)
					continue
				}
				log.Infof("Successfully registered in-tree vSphere inline volume: %q with "+
					"volume Id: %q", volume.VsphereVolume.VolumePath, volumeHandle)
			}
			// Migrated in-tree static vSphere volumes
			if volume.PersistentVolumeClaim != nil {
				pvc, err := metadataSyncer.pvcLister.PersistentVolumeClaims(pod.Namespace).
					Get(volume.PersistentVolumeClaim.ClaimName)
				if err != nil {
					log.Errorf("Error getting Persistent Volume Claim for volume %s with err: %v", volume.Name, err)
					continue
				}
				// Get pv object attached to pvc.
				pv, err := metadataSyncer.pvLister.Get(pvc.Spec.VolumeName)
				if err != nil {
					log.Errorf("Error getting Persistent Volume for PVC %s in volume %s with err: %v", pvc.Name, volume.Name, err)
					continue
				}
				if pv.Spec.VsphereVolume != nil {
					_, ok := pv.Annotations[common.AnnDynamicallyProvisioned]
					if !ok {
						// in-tree statically created vSphere volume
						log.Infof("Registering in-tree Static vSphere PV: %q, Volume Path: %q",
							pv.Name, pv.Spec.VsphereVolume.VolumePath)
						volumeHandle, err := volumeMigrationService.GetVolumeID(ctx,
							&migration.VolumeSpec{VolumePath: pv.Spec.VsphereVolume.VolumePath}, true)
						if err != nil {
							log.Warnf("podAdded: Failed to get VolumeID from "+
								"volumeMigrationService for static PV: %q volumePath: %q with error %+v",
								pv.Name, pv.Spec.VsphereVolume.VolumePath, err)
							continue
						}
						log.Infof("Successfully registered in-tree vSphere inline volume: %q with "+
							"volume Id: %q", pv.Spec.VsphereVolume.VolumePath, volumeHandle)
					}
				}
			}
		}
	}
}

// podUpdated updates pod metadata on VC when pod labels have been updated on
// K8s cluster.
func podUpdated(oldObj, newObj interface{}, metadataSyncer *metadataSyncInformer) {
	ctx, log := logger.GetNewContextWithLogger()
	// Get old and new pod objects.
	oldPod, ok := oldObj.(*v1.Pod)
	if oldPod == nil || !ok {
		log.Warnf("PodUpdated: unrecognized old object %+v", oldObj)
		return
	}
	newPod, ok := newObj.(*v1.Pod)
	if newPod == nil || !ok {
		log.Warnf("PodUpdated: unrecognized new object %+v", newObj)
		return
	}

	// If old pod is in pending state and new pod is running, update metadata.
	if oldPod.Status.Phase == v1.PodPending && newPod.Status.Phase == v1.PodRunning {
		log.Debugf("PodUpdated: Pod %s calling updatePodMetadata", newPod.Name)
		// Update pod metadata.
		updatePodMetadata(ctx, newPod, metadataSyncer, false)
	}
}

// podDeleted deletes pod metadata on VC when pod has been deleted on
// K8s cluster.
func podDeleted(obj interface{}, metadataSyncer *metadataSyncInformer) {
	ctx, log := logger.GetNewContextWithLogger()
	// Get pod object.
	pod, ok := obj.(*v1.Pod)
	if pod == nil || !ok {
		log.Warnf("PodDeleted: unrecognized new object %+v", obj)
		return
	}

	log.Debugf("PodDeleted: Pod %s calling updatePodMetadata", pod.Name)
	// Update pod metadata.
	updatePodMetadata(ctx, pod, metadataSyncer, true)
}

// updatePodMetadata updates metadata for volumes attached to the pod.
func updatePodMetadata(ctx context.Context, pod *v1.Pod, metadataSyncer *metadataSyncInformer, deleteFlag bool) {
	if metadataSyncer.clusterFlavor == cnstypes.CnsClusterFlavorGuest {
		pvcsiUpdatePod(ctx, pod, metadataSyncer, deleteFlag)
	} else {
		csiUpdatePod(ctx, pod, metadataSyncer, deleteFlag)
	}

}

// csiPVCUpdated updates volume metadata for PVC objects on the VC in Vanilla
// k8s and supervisor cluster.
func csiPVCUpdated(ctx context.Context, pvc *v1.PersistentVolumeClaim,
	pv *v1.PersistentVolume, metadataSyncer *metadataSyncInformer) {
	log := logger.GetLogger(ctx)
	var (
		volumeHandle string
		err          error
		vcHost       string
		cnsVolumeMgr volumes.Manager
	)
	if IsMigrationEnabled && pv.Spec.VsphereVolume != nil {
		// In case if feature state switch is enabled after syncer is deployed,
		// we need to initialize the volumeMigrationService.
		if err = initVolumeMigrationService(ctx, metadataSyncer); err != nil {
			log.Errorf("PVC Updated: Failed to get migration service. Err: %v", err)
			return
		}
		migrationVolumeSpec := &migration.VolumeSpec{VolumePath: pv.Spec.VsphereVolume.VolumePath,
			StoragePolicyName: pv.Spec.VsphereVolume.StoragePolicyName}
		volumeHandle, err = volumeMigrationService.GetVolumeID(ctx, migrationVolumeSpec, true)
		if err != nil {
			log.Errorf("PVC Updated: Failed to get VolumeID from volumeMigrationService for migration VolumeSpec: %v "+
				"with error %+v", migrationVolumeSpec, err)
			return
		}
		vcHost, cnsVolumeMgr, err = getVcHostAndVolumeManagerForVolumeID(ctx, metadataSyncer, volumeHandle)
		if err != nil {
			log.Errorf("PVCUpdated: Failed to get VC host and volume manager for the given volume: %v. "+
				"Error occoured: %+v", volumeHandle, err)
			return
		}
	} else {
		volumeFound := false
		volumeHandle = pv.Spec.CSI.VolumeHandle

		vcHost, cnsVolumeMgr, err = getVcHostAndVolumeManagerForVolumeID(ctx, metadataSyncer, volumeHandle)
		if err != nil {
			log.Errorf("PVCUpdated: Failed to get VC host and volume manager for the given volume: %v. "+
				"Error occoured: %+v", volumeHandle, err)
			return
		}
		// Following wait poll is required to avoid race condition between
		// pvcUpdated and pvUpdated. This helps avoid race condition between
		// pvUpdated and pvcUpdated handlers when static PV and PVC is created
		// almost at the same time using single YAML file.
		err = wait.PollUntilContextTimeout(ctx, 5*time.Second, time.Minute, false,
			func(ctx context.Context) (bool, error) {
				queryFilter := cnstypes.CnsQueryFilter{
					VolumeIds: []cnstypes.CnsVolumeId{{Id: volumeHandle}},
				}
				// Query with empty selection. CNS returns only the volume ID from
				// its cache.
				queryResult, err := cnsVolumeMgr.QueryAllVolume(ctx, queryFilter, cnstypes.CnsQuerySelection{})
				if err != nil {
					log.Errorf("PVCUpdated: QueryVolume failed for volume %q with err=%+v", volumeHandle, err.Error())
					return false, err
				}
				if queryResult != nil && len(queryResult.Volumes) == 1 && queryResult.Volumes[0].VolumeId.Id == volumeHandle {
					log.Infof("PVCUpdated: volume %q found", volumeHandle)
					volumeFound = true
				}
				return volumeFound, nil
			})
		if err != nil {
			log.Errorf("PVCUpdated: Error occurred while polling to check if volume is marked as container volume. "+
				"err: %+v", err)
			return
		}

		if !volumeFound {
			// volumeFound will be false when wait poll times out.
			log.Errorf("PVCUpdated: volume: %q is not marked as the container volume. Skipping PVC entity metadata update",
				volumeHandle)
			return
		}
	}

	vcHostObj, vcHostObjFound := metadataSyncer.configInfo.Cfg.VirtualCenter[vcHost]
	if !vcHostObjFound {
		log.Errorf("PVCUpdated: failed to find VC host for given volume: %q.", volumeHandle)
		return
	}

	// Create updateSpec.
	var metadataList []cnstypes.BaseCnsEntityMetadata
	entityReference := cnsvsphere.CreateCnsKuberenetesEntityReference(string(cnstypes.CnsKubernetesEntityTypePV),
		pv.Name, "", clusterIDforVolumeMetadata)
	pvcMetadata := cnsvsphere.GetCnsKubernetesEntityMetaData(pvc.Name, pvc.Labels, false,
		string(cnstypes.CnsKubernetesEntityTypePVC), pvc.Namespace, clusterIDforVolumeMetadata,
		[]cnstypes.CnsKubernetesEntityReference{entityReference})

	metadataList = append(metadataList, cnstypes.BaseCnsEntityMetadata(pvcMetadata))
	containerCluster := cnsvsphere.GetContainerCluster(clusterIDforVolumeMetadata,
		vcHostObj.User, metadataSyncer.clusterFlavor,
		metadataSyncer.configInfo.Cfg.Global.ClusterDistribution)

	updateSpec := &cnstypes.CnsVolumeMetadataUpdateSpec{
		VolumeId: cnstypes.CnsVolumeId{
			Id: volumeHandle,
		},
		Metadata: cnstypes.CnsVolumeMetadata{
			ContainerCluster:      containerCluster,
			ContainerClusterArray: []cnstypes.CnsContainerCluster{containerCluster},
			EntityMetadata:        metadataList,
		},
	}

	log.Debugf("PVCUpdated: Calling UpdateVolumeMetadata with updateSpec: %+v", spew.Sdump(updateSpec))
	if err := cnsVolumeMgr.UpdateVolumeMetadata(ctx, updateSpec); err != nil {
		log.Errorf("PVCUpdated: UpdateVolumeMetadata failed with err %v", err)
	}
}

// csiPVCDeleted deletes volume metadata on VC when volume has been deleted
// on Vanilla k8s and supervisor cluster.
func csiPVCDeleted(ctx context.Context, pvc *v1.PersistentVolumeClaim,
	pv *v1.PersistentVolume, metadataSyncer *metadataSyncInformer) {
	log := logger.GetLogger(ctx)
	// Volume will be deleted by controller when reclaim policy is delete.
	if pv.Spec.PersistentVolumeReclaimPolicy == v1.PersistentVolumeReclaimDelete {
		log.Debugf("PVCDeleted: Reclaim policy is delete")
		return
	}

	// If the PV reclaim policy is retain we need to delete PVC labels.
	var metadataList []cnstypes.BaseCnsEntityMetadata
	pvcMetadata := cnsvsphere.GetCnsKubernetesEntityMetaData(pvc.Name, nil, true,
		string(cnstypes.CnsKubernetesEntityTypePVC), pvc.Namespace,
		clusterIDforVolumeMetadata, nil)
	metadataList = append(metadataList, cnstypes.BaseCnsEntityMetadata(pvcMetadata))

	var volumeHandle string
	var err error
	if IsMigrationEnabled && pv.Spec.VsphereVolume != nil {
		// In case if feature state switch is enabled after syncer is deployed,
		// we need to initialize the volumeMigrationService.
		if err = initVolumeMigrationService(ctx, metadataSyncer); err != nil {
			log.Errorf("PVC Deleted: Failed to get migration service. Err: %v", err)
			return
		}
		migrationVolumeSpec := &migration.VolumeSpec{VolumePath: pv.Spec.VsphereVolume.VolumePath,
			StoragePolicyName: pv.Spec.VsphereVolume.StoragePolicyName}
		volumeHandle, err = volumeMigrationService.GetVolumeID(ctx, migrationVolumeSpec, true)
		if err != nil {
			log.Errorf("PVC Deleted: Failed to get VolumeID from volumeMigrationService for migration VolumeSpec: %v "+
				"with error %+v", migrationVolumeSpec, err)
			return
		}
	} else {
		volumeHandle = pv.Spec.CSI.VolumeHandle
	}

	vcHost, cnsVolumeMgr, err := getVcHostAndVolumeManagerForVolumeID(ctx, metadataSyncer, volumeHandle)
	if err != nil {
		log.Errorf("PVC Deleted: Failed to get VC host and volume manager for the given volume: %v. "+
			"Error occoured: %+v", volumeHandle, err)
		return
	}

	vcHostObj, vcHostObjFound := metadataSyncer.configInfo.Cfg.VirtualCenter[vcHost]
	if !vcHostObjFound {
		log.Errorf("PVCDeleted: failed to find VC host for given volume: %q.", volumeHandle)
		return
	}

	containerCluster := cnsvsphere.GetContainerCluster(clusterIDforVolumeMetadata,
		vcHostObj.User, metadataSyncer.clusterFlavor, metadataSyncer.configInfo.Cfg.Global.ClusterDistribution)
	updateSpec := &cnstypes.CnsVolumeMetadataUpdateSpec{
		VolumeId: cnstypes.CnsVolumeId{
			Id: volumeHandle,
		},
		Metadata: cnstypes.CnsVolumeMetadata{
			ContainerCluster:      containerCluster,
			ContainerClusterArray: []cnstypes.CnsContainerCluster{containerCluster},
			EntityMetadata:        metadataList,
		},
	}

	log.Debugf("PVCDeleted: Calling UpdateVolumeMetadata for volume %s with updateSpec: %+v",
		updateSpec.VolumeId.Id, spew.Sdump(updateSpec))

	if err := cnsVolumeMgr.UpdateVolumeMetadata(ctx, updateSpec); err != nil {
		log.Errorf("PVCDeleted: UpdateVolumeMetadata failed with err %v", err)
	}
}

// csiPVUpdated updates volume metadata on VC when volume labels on Vanilla
// k8s and supervisor cluster have been updated.
func csiPVUpdated(ctx context.Context, newPv *v1.PersistentVolume, oldPv *v1.PersistentVolume,
	metadataSyncer *metadataSyncInformer) {
	log := logger.GetLogger(ctx)
	var metadataList []cnstypes.BaseCnsEntityMetadata
	pvMetadata := cnsvsphere.GetCnsKubernetesEntityMetaData(newPv.Name, newPv.GetLabels(), false,
		string(cnstypes.CnsKubernetesEntityTypePV), "", clusterIDforVolumeMetadata, nil)
	metadataList = append(metadataList, cnstypes.BaseCnsEntityMetadata(pvMetadata))
	var (
		volumeHandle                     string
		err                              error
		containerCluster                 cnstypes.CnsContainerCluster
		cnsVolumeMgr                     volumes.Manager
		vcHost                           string
		isTopologyAwareFileVolumeEnabled bool
	)
	if IsMigrationEnabled && newPv.Spec.VsphereVolume != nil {
		// In case if feature state switch is enabled after syncer is deployed,
		// we need to initialize the volumeMigrationService.
		if err = initVolumeMigrationService(ctx, metadataSyncer); err != nil {
			log.Errorf("PVUpdated: Failed to get migration service. Err: %v", err)
			return
		}
		volumeHandle, err = volumeMigrationService.GetVolumeID(ctx,
			&migration.VolumeSpec{VolumePath: newPv.Spec.VsphereVolume.VolumePath,
				StoragePolicyName: newPv.Spec.VsphereVolume.StoragePolicyName}, true)
		if err != nil {
			log.Errorf("PVUpdated: Failed to get VolumeID from volumeMigrationService for volumePath: %s with error %+v",
				newPv.Spec.VsphereVolume.VolumePath, err)
			return
		}
	} else {
		volumeHandle = newPv.Spec.CSI.VolumeHandle
	}

	// TODO: Revisit the logic for static PV update once we have a specific
	// return code from CNS for UpdateVolumeMetadata if the volume is not
	// registered as CNS volume. The issue is being tracked here:
	// https://github.com/kubernetes-sigs/vsphere-csi-driver/issues/579

	// Dynamically provisioned PVs have a volume attribute called
	// 'storage.kubernetes.io/csiProvisionerIdentity' in their CSI spec, which
	// is set by external-provisioner.
	var isdynamicCSIPV bool
	if newPv.Spec.CSI != nil {
		_, isdynamicCSIPV = newPv.Spec.CSI.VolumeAttributes[attribCSIProvisionerID]
	}

	isTopologyAwareFileVolumeEnabled = metadataSyncer.coCommonInterface.IsFSSEnabled(ctx,
		common.TopologyAwareFileVolume)

	if oldPv.Status.Phase == v1.VolumePending &&
		(newPv.Status.Phase == v1.VolumeAvailable || newPv.Status.Phase == v1.VolumeBound) &&
		!isdynamicCSIPV && newPv.Spec.CSI != nil {
		// Static PV is Created.
		var volumeType string
		if IsFileVolume(oldPv) {

			if len(metadataSyncer.configInfo.Cfg.VirtualCenter) > 1 {
				// If it is a multi VC setup, then skip this volume as we do not support file share volumes
				// on a multi VC deployment if TopologyAwareFileVolume FSS is not enabled.
				if !isTopologyAwareFileVolumeEnabled {
					log.Infof("PVUpdated: %q is a vSphere volume claim in namespace %q."+
						"File share volumes are not supported in a multi VC setup."+
						"Skipping PV update.", newPv.Name, newPv.Namespace)
					return
				}
			}
			volumeType = common.FileVolumeType
		} else {
			volumeType = common.BlockVolumeType
		}
		log.Debugf("PVUpdated: observed static volume provisioning for the PV: %q with volumeType: %q",
			newPv.Name, volumeType)
		queryFilter := cnstypes.CnsQueryFilter{
			VolumeIds: []cnstypes.CnsVolumeId{{Id: oldPv.Spec.CSI.VolumeHandle}},
		}

		if len(metadataSyncer.configInfo.Cfg.VirtualCenter) > 1 {
			if volumeType == common.BlockVolumeType {
				// If it is a multi VC deployment, figure out FCD's location based on PV's nodeAffinity rules.
				vcHost, cnsVolumeMgr, err = getVcHostAndVolumeManagerFromPvNodeAffinity(ctx, newPv, metadataSyncer)
				if err != nil {
					log.Errorf("PVUpdated: Failed to get VC host and volume manager for multi VC setup. "+
						"Error occurred: %+v", err)
					generateEventOnPv(ctx, oldPv, v1.EventTypeWarning,
						staticVolumeProvisioningFailure, "Failed to identify VC for volume.")
					return
				}
			} else {
				// File Volume in Multi VC
				if !isTopologyAwareFileVolumeEnabled {
					log.Infof("PVUpdated: %q is a vSphere volume claim in namespace %q."+
						"File share volumes are not supported in a multi VC setup as TopologyAwareFileVolume FSS is disabled."+
						"Skipping PV update.", newPv.Name, newPv.Namespace)
					return
				}

				// If VolumeID to VC mappping is found, volume is already created.
				// PV metadata needs to be updated
				vcHost, cnsVolumeMgr, err = getVcHostAndVolumeManagerForVolumeID(ctx, metadataSyncer, volumeHandle)
				if err != nil {
					log.Errorf("PVUpdated: Failed to get VC host and volume manager. "+
						"Error occurred: %+v", err)

					// Could not find vcHost mapping, attempt to create the volume on all VCs utill successful
					vcHost, _, err = createVolumeOnMultiVc(ctx, oldPv, metadataSyncer,
						volumeType, metadataList, volumeHandle)
					if err == nil {
						log.Infof("PVUpdated: Successfully created static file volume %q on VC %s", newPv.Name, vcHost)
					} else {
						// Failed to create static PV
						log.Errorf("PVUpdated: Failed to create static file volume %q. Error: %+v", newPv.Name, err)
						generateEventOnPv(ctx, oldPv, v1.EventTypeWarning,
							staticVolumeProvisioningFailure, "Failed to create volume on any of the VCs")
						return
					}
					return
				}
				// Volume to VC mapping already exists.
				// PV is required to be updated.
			}
		} else {
			// In case of a single VC set up, no need to look up topology segments.
			vcHost, cnsVolumeMgr, err = getVcHostAndVolumeManagerForVolumeID(ctx, metadataSyncer, volumeHandle)
			if err != nil {
				log.Errorf("PVUpdated: Failed to get VC host and volume manager for single VC setup. "+
					"Error occoured: %+v", err)
				generateEventOnPv(ctx, oldPv, v1.EventTypeWarning,
					staticVolumeProvisioningFailure, "Failed to identify VC for volume")
				return
			}
		}

		volumeOperationsLock[vcHost].Lock()
		defer volumeOperationsLock[vcHost].Unlock()

		vcHostObj, vcHostObjFound := metadataSyncer.configInfo.Cfg.VirtualCenter[vcHost]
		if !vcHostObjFound {
			log.Errorf("PVUpdated: failed to find VC host for given volume: %q.", volumeHandle)
			return
		}

		containerCluster = cnsvsphere.GetContainerCluster(clusterIDforVolumeMetadata,
			vcHostObj.User, metadataSyncer.clusterFlavor,
			metadataSyncer.configInfo.Cfg.Global.ClusterDistribution)

		if volumeType == common.BlockVolumeType || len(metadataSyncer.configInfo.Cfg.VirtualCenter) == 1 {
			// QueryAll with no selection will return only the volume ID.
			queryResult, err := cnsVolumeMgr.QueryAllVolume(ctx, queryFilter, cnstypes.CnsQuerySelection{})
			if err != nil {
				log.Errorf("PVUpdated: QueryVolume failed for volume %q with err=%+v", oldPv.Spec.CSI.VolumeHandle, err.Error())
				return
			}
			if len(queryResult.Volumes) == 0 {
				log.Infof("PVUpdated: Verified volume: %q is not marked as container volume in CNS. "+
					"Calling CreateVolume with BackingID to mark volume as Container Volume.", oldPv.Spec.CSI.VolumeHandle)
				// Call CreateVolume for Static Volume Provisioning.
				err = createCnsVolume(ctx, oldPv, metadataSyncer, cnsVolumeMgr, volumeType, vcHost, metadataList, volumeHandle)
				if err != nil {
					errMsg := fmt.Sprintf("Failed to create volume on VC %s", vcHost)
					log.Errorf(errMsg)
					generateEventOnPv(ctx, oldPv, v1.EventTypeWarning,
						staticVolumeProvisioningFailure, errMsg)
				}
				return
			} else if queryResult.Volumes[0].VolumeId.Id == oldPv.Spec.CSI.VolumeHandle {
				log.Infof("PVUpdated: Verified volume: %q is already marked as container volume in CNS.",
					oldPv.Spec.CSI.VolumeHandle)
				// Volume is already present in the CNS, so continue with the
				// UpdateVolumeMetadata.
			} else {
				log.Infof("PVUpdated: Queried volume: %q is other than requested volume: %q.",
					oldPv.Spec.CSI.VolumeHandle, queryResult.Volumes[0].VolumeId.Id)
				// unknown Volume is returned from the CNS, so returning from here.
				return
			}
		}
	} else {
		// This is the case where updates are detected on an existing PV.
		// Look up VC for the given volume from in-memory map.
		vcHost, cnsVolumeMgr, err = getVcHostAndVolumeManagerForVolumeID(ctx, metadataSyncer, volumeHandle)
		if err != nil {
			log.Errorf("PVUpdated: Failed to get VC host and volume manager for single VC setup. "+
				"Error occoured: %+v", err)
			return
		}

		vcHostObj, vcHostObjFound := metadataSyncer.configInfo.Cfg.VirtualCenter[vcHost]
		if !vcHostObjFound {
			log.Errorf("PVUpdated: failed to find VC host for given volume: %q.", volumeHandle)
			return
		}

		containerCluster = cnsvsphere.GetContainerCluster(clusterIDforVolumeMetadata,
			vcHostObj.User, metadataSyncer.clusterFlavor,
			metadataSyncer.configInfo.Cfg.Global.ClusterDistribution)
	}
	// Call UpdateVolumeMetadata for all other cases.
	updateSpec := &cnstypes.CnsVolumeMetadataUpdateSpec{
		VolumeId: cnstypes.CnsVolumeId{
			Id: volumeHandle,
		},
		Metadata: cnstypes.CnsVolumeMetadata{
			ContainerCluster:      containerCluster,
			ContainerClusterArray: []cnstypes.CnsContainerCluster{containerCluster},
			EntityMetadata:        metadataList,
		},
	}

	log.Debugf("PVUpdated: Calling UpdateVolumeMetadata for volume %q with updateSpec: %+v",
		updateSpec.VolumeId.Id, spew.Sdump(updateSpec))
	if err := cnsVolumeMgr.UpdateVolumeMetadata(ctx, updateSpec); err != nil {
		log.Errorf("PVUpdated: UpdateVolumeMetadata failed with err %v", err)
		return
	}
	log.Debugf("PVUpdated: UpdateVolumeMetadata succeed for the volume %q with updateSpec: %+v",
		updateSpec.VolumeId.Id, spew.Sdump(updateSpec))
}

// csiPVDeleted deletes volume metadata on VC when volume has been deleted on
// Vanills k8s and supervisor cluster.
func csiPVDeleted(ctx context.Context, pv *v1.PersistentVolume, metadataSyncer *metadataSyncInformer) {
	log := logger.GetLogger(ctx)
	if IsPodVMOnStretchSupervisorFSSEnabled {
		volumeInfo, err := volumeInfoService.GetVolumeInfoForVolumeID(ctx, pv.Spec.CSI.VolumeHandle)
		if err != nil {
			log.Errorf("failed to fetch CnsVolumeInfo CR. Error: %+v", err)
			return
		}
		restConfig, err := config.GetConfig()
		if err != nil {
			log.Errorf("failed to fetch kubernetes config. Error: %+v", err)
			return
		}
		cnsOperatorClient, err := k8s.NewClientForGroup(ctx, restConfig, cnsoperatorv1alpha1.GroupName)
		if err != nil {
			log.Errorf("failed to create CNSOperator client. Error: %+v", err)
			return
		}

		// Fetch StoragePolicyUsage instance for storageClass associated with the volume.
		storagePolicyUsageInstanceName := volumeInfo.Spec.StorageClassName + "-" +
			storagepolicyv1alpha2.NameSuffixForPVC
		storagePolicyUsageCR := &storagepolicyv1alpha2.StoragePolicyUsage{}
		err = cnsOperatorClient.Get(ctx, k8stypes.NamespacedName{
			Namespace: volumeInfo.Spec.Namespace,
			Name:      storagePolicyUsageInstanceName},
			storagePolicyUsageCR)
		if err != nil {
			log.Errorf("failed to fetch %s instance with name %q from supervisor namespace %q. Error: %+v",
				storagepolicyv1alpha2.CRDSingular, storagePolicyUsageInstanceName,
				volumeInfo.Spec.Namespace, err)
			return
		}

		// Decrease the used capacity in StoragePolicyUsage instance as we are deleting the volume.
		patchedStoragePolicyUsageCR := storagePolicyUsageCR.DeepCopy()
		if storagePolicyUsageCR.Status.ResourceTypeLevelQuotaUsage.Used.Value() < volumeInfo.Spec.Capacity.Value() {
			log.Infof("Failed to update used capacity in StoragePolicyUsage: %q in namespace: %q "+
				"StoragePolicyUsage has used capacity as: %v Mb and is lesser than the capacity of the volume "+
				"getting deleted: %v Mb. Usage field computation will be deferred to CSI full sync.",
				storagePolicyUsageCR.Name, storagePolicyUsageCR.Namespace,
				storagePolicyUsageCR.Status.ResourceTypeLevelQuotaUsage.Used.ScaledValue(resource.Mega),
				volumeInfo.Spec.Capacity.ScaledValue(resource.Mega))
		} else {
			patchedStoragePolicyUsageCR.Status.ResourceTypeLevelQuotaUsage.Used.Sub(*volumeInfo.Spec.Capacity)
			err = PatchStoragePolicyUsage(ctx, cnsOperatorClient, storagePolicyUsageCR,
				patchedStoragePolicyUsageCR)
			if err != nil {
				log.Errorf("updateStoragePolicyUsage failed. err: %v", err)
				return
			}
			log.Infof("Successfully decreased the used capacity by %v Mb for StoragePolicyUsage: %q in namespace: %q",
				volumeInfo.Spec.Capacity.ScaledValue(resource.Mega), storagePolicyUsageCR.Name, storagePolicyUsageCR.Namespace)
		}
	}
	// Delete the CNSVolumeInfo instance for this volume.
	if pv.Spec.CSI != nil && volumeInfoService != nil {
		err := volumeInfoService.DeleteVolumeInfo(ctx, pv.Spec.CSI.VolumeHandle)
		if err != nil {
			log.Errorf("failed to delete cnsVolumeInfo CR for volume: %q. Error: %+v", pv.Spec.CSI.VolumeHandle, err)
			return
		}
	}
	if pv.Spec.ClaimRef != nil && pv.Status.Phase == v1.VolumeReleased &&
		pv.Spec.PersistentVolumeReclaimPolicy == v1.PersistentVolumeReclaimDelete {
		log.Debugf("PVDeleted: Volume deletion will be handled by Controller")
		return
	}

	if IsFileVolume(pv) {
		// If PV is file share volume.

		// If TopologyAwareFileVolume FSS is false and it is a multi VC setup, then skip this volume
		if len(metadataSyncer.configInfo.Cfg.VirtualCenter) > 1 &&
			!metadataSyncer.coCommonInterface.IsFSSEnabled(ctx,
				common.TopologyAwareFileVolume) {
			log.Debugf("PVDeleted: %q is a vSphere volume claim in namespace %q."+
				"File share volumes are not supported in a multi VC setup."+
				"Skipping deletion of PV metadata.", pv.Name, pv.Namespace)
			return
		}

		vcHost, cnsVolumeMgr, err := getVcHostAndVolumeManagerForVolumeID(ctx, metadataSyncer, pv.Spec.CSI.VolumeHandle)
		if err != nil {
			log.Errorf("PVDeleted: Failed to get VC host and volume manager for single VC setup. "+
				"Error occoured: %+v", err)
			return
		}

		volumeOperationsLock[vcHost].Lock()
		defer volumeOperationsLock[vcHost].Unlock()

		vcHostObj, vcHostObjFound := metadataSyncer.configInfo.Cfg.VirtualCenter[vcHost]
		if !vcHostObjFound {
			log.Errorf("PVDeleted: failed to find VC host for given file volume: %q.", pv.Name)
			return
		}

		log.Debugf("PVDeleted: vSphere CSI Driver is calling UpdateVolumeMetadata to "+
			"delete volume metadata references for PV: %q", pv.Name)
		var metadataList []cnstypes.BaseCnsEntityMetadata
		pvMetadata := cnsvsphere.GetCnsKubernetesEntityMetaData(pv.Name, nil, true,
			string(cnstypes.CnsKubernetesEntityTypePV), "", clusterIDforVolumeMetadata, nil)
		metadataList = append(metadataList, cnstypes.BaseCnsEntityMetadata(pvMetadata))

		containerCluster := cnsvsphere.GetContainerCluster(clusterIDforVolumeMetadata,
			vcHostObj.User, metadataSyncer.clusterFlavor, metadataSyncer.configInfo.Cfg.Global.ClusterDistribution)
		updateSpec := &cnstypes.CnsVolumeMetadataUpdateSpec{
			VolumeId: cnstypes.CnsVolumeId{
				Id: pv.Spec.CSI.VolumeHandle,
			},
			Metadata: cnstypes.CnsVolumeMetadata{
				ContainerCluster:      containerCluster,
				ContainerClusterArray: []cnstypes.CnsContainerCluster{containerCluster},
				EntityMetadata:        metadataList,
			},
		}

		log.Debugf("PVDeleted: Calling UpdateVolumeMetadata for volume %s with updateSpec: %+v",
			updateSpec.VolumeId.Id, spew.Sdump(updateSpec))
		if err := cnsVolumeMgr.UpdateVolumeMetadata(ctx, updateSpec); err != nil {
			log.Errorf("PVDeleted: UpdateVolumeMetadata failed with err %v", err)
			return
		}
		queryFilter := cnstypes.CnsQueryFilter{
			VolumeIds: []cnstypes.CnsVolumeId{
				{
					Id: pv.Spec.CSI.VolumeHandle,
				},
			},
		}
		queryResult, err := utils.QueryVolumeUtil(ctx, cnsVolumeMgr, queryFilter, nil)
		if err != nil {
			log.Error("PVDeleted: QueryVolumeUtil failed with err=%+v", err.Error())
			return
		}
		if queryResult != nil && len(queryResult.Volumes) == 1 &&
			len(queryResult.Volumes[0].Metadata.EntityMetadata) == 0 {
			log.Infof("PVDeleted: Volume: %q is not in use by any other entity. Removing CNS tag.",
				pv.Spec.CSI.VolumeHandle)
			_, err := cnsVolumeMgr.DeleteVolume(ctx, pv.Spec.CSI.VolumeHandle, false)
			if err != nil {
				log.Errorf("PVDeleted: Failed to delete volume %q with error %+v", pv.Spec.CSI.VolumeHandle, err)
				return
			}
		}

	} else {
		var (
			volumeHandle string
			vcHost       string
			err          error
			cnsVolumeMgr volumes.Manager
		)

		// Fetch FSS value for CSI migration once.
		if IsMigrationEnabled && pv.Spec.VsphereVolume != nil {
			// In case if feature state switch is enabled after syncer is deployed,
			// we need to initialize the volumeMigrationService.
			if err = initVolumeMigrationService(ctx, metadataSyncer); err != nil {
				log.Errorf("PVDeleted: Failed to get migration service. Err: %v", err)
				return
			}
			migrationVolumeSpec := &migration.VolumeSpec{VolumePath: pv.Spec.VsphereVolume.VolumePath,
				StoragePolicyName: pv.Spec.VsphereVolume.StoragePolicyName}
			volumeHandle, err = volumeMigrationService.GetVolumeID(ctx, migrationVolumeSpec, true)
			if err != nil {
				log.Errorf("PVDeleted: Failed to get VolumeID from volumeMigrationService for migration VolumeSpec: %v "+
					"with error %+v", migrationVolumeSpec, err)
				return
			}
		} else {
			volumeHandle = pv.Spec.CSI.VolumeHandle
		}

		vcHost, cnsVolumeMgr, err = getVcHostAndVolumeManagerForVolumeID(ctx, metadataSyncer, volumeHandle)
		if err != nil {
			log.Errorf("PVDeleted: Failed to get VC host and volume manager for single VC setup. "+
				"Error occoured: %+v", err)
			return
		}

		volumeOperationsLock[vcHost].Lock()
		defer volumeOperationsLock[vcHost].Unlock()

		log.Debugf("PVDeleted: vSphere CSI Driver is deleting volume %v", pv)

		if _, err := cnsVolumeMgr.DeleteVolume(ctx, volumeHandle, false); err != nil {
			log.Errorf("PVDeleted: Failed to delete disk %s with error %+v", volumeHandle, err)
		}
		if IsMigrationEnabled && pv.Spec.VsphereVolume != nil {
			// Delete the cnsvspherevolumemigration crd instance when PV is deleted.
			err = volumeMigrationService.DeleteVolumeInfo(ctx, volumeHandle)
			if err != nil {
				log.Errorf("PVDeleted: failed to delete volumeInfo CR for volume: %q. Error: %+v", volumeHandle, err)
				return
			}
		}
		if metadataSyncer.clusterFlavor == cnstypes.CnsClusterFlavorVanilla {
			if len(metadataSyncer.configInfo.Cfg.VirtualCenter) > 1 {
				// Delete CNSVolumeInfo CR for the volume ID.
				err = volumeInfoService.DeleteVolumeInfo(ctx, volumeHandle)
				if err != nil {
					log.Errorf("failed to remove CNSVolumeInfo CR for volumeID %q. Error: %+v",
						volumeHandle, err)
				}
			}
		} else if metadataSyncer.clusterFlavor == cnstypes.CnsClusterFlavorWorkload &&
			IsPodVMOnStretchSupervisorFSSEnabled {
			// Delete CNSVolumeInfo CR for the volume ID.
			err = volumeInfoService.DeleteVolumeInfo(ctx, volumeHandle)
			if err != nil {
				log.Errorf("failed to remove CNSVolumeInfo CR for volumeID %q. Error: %+v",
					volumeHandle, err)
			}
		}
	}
}

// csiUpdatePod update/deletes pod CnsVolumeMetadata when pod has been
// created/deleted on Vanilla k8s and supervisor cluster have been updated.
func csiUpdatePod(ctx context.Context, pod *v1.Pod, metadataSyncer *metadataSyncInformer, deleteFlag bool) {
	log := logger.GetLogger(ctx)
	// Iterate through volumes attached to pod.
	for _, volume := range pod.Spec.Volumes {
		var (
			volumeHandle string
			metadataList []cnstypes.BaseCnsEntityMetadata
			podMetadata  *cnstypes.CnsKubernetesEntityMetadata
			err          error = nil
		)
		if volume.PersistentVolumeClaim != nil {
			valid, pv, pvc := IsValidVolume(ctx, volume, pod, metadataSyncer)
			if valid {
				if !deleteFlag {
					// We need to update metadata for pods having corresponding PVC
					// as an entity reference.
					entityReference := cnsvsphere.CreateCnsKuberenetesEntityReference(
						string(cnstypes.CnsKubernetesEntityTypePVC), pvc.Name, pvc.Namespace,
						clusterIDforVolumeMetadata)
					podMetadata = cnsvsphere.GetCnsKubernetesEntityMetaData(pod.Name, nil,
						deleteFlag, string(cnstypes.CnsKubernetesEntityTypePOD), pod.Namespace,
						clusterIDforVolumeMetadata,
						[]cnstypes.CnsKubernetesEntityReference{entityReference})
				} else {
					// Deleting the pod metadata.
					podMetadata = cnsvsphere.GetCnsKubernetesEntityMetaData(pod.Name, nil, deleteFlag,
						string(cnstypes.CnsKubernetesEntityTypePOD), pod.Namespace,
						clusterIDforVolumeMetadata, nil)
				}
				metadataList = append(metadataList, cnstypes.BaseCnsEntityMetadata(podMetadata))
				if IsMigrationEnabled && pv.Spec.VsphereVolume != nil {
					// In case if feature state switch is enabled after syncer is
					// deployed, we need to initialize the volumeMigrationService.
					if err = initVolumeMigrationService(ctx, metadataSyncer); err != nil {
						log.Errorf("PodUpdated: Failed to get migration service. Err: %v", err)
						return
					}
					migrationVolumeSpec := &migration.VolumeSpec{VolumePath: pv.Spec.VsphereVolume.VolumePath,
						StoragePolicyName: pv.Spec.VsphereVolume.StoragePolicyName}
					volumeHandle, err = volumeMigrationService.GetVolumeID(ctx, migrationVolumeSpec, true)
					if err != nil {
						log.Errorf("Failed to get VolumeID from volumeMigrationService for migration VolumeSpec: %v "+
							"with error %+v", migrationVolumeSpec, err)
						return
					}
				} else {
					volumeHandle = pv.Spec.CSI.VolumeHandle
				}
				if err != nil {
					log.Errorf("failed to get volume id for volume name: %q with err=%v", pv.Name, err)
					continue
				}
			} else {
				log.Debugf("Volume %q is not a valid vSphere volume for the pod %q",
					volume.PersistentVolumeClaim.ClaimName, pod.Name)
				return
			}
		} else {
			// Inline migrated volumes with no PVC.
			if IsMigrationEnabled {
				if volume.VsphereVolume != nil {
					// No entity reference is supplied for inline volumes.
					podMetadata = cnsvsphere.GetCnsKubernetesEntityMetaData(pod.Name, nil, deleteFlag,
						string(cnstypes.CnsKubernetesEntityTypePOD), pod.Namespace,
						clusterIDforVolumeMetadata, nil)
					metadataList = append(metadataList, cnstypes.BaseCnsEntityMetadata(podMetadata))
					var err error
					// In case if feature state switch is enabled after syncer is
					// deployed, we need to initialize the volumeMigrationService.
					if err = initVolumeMigrationService(ctx, metadataSyncer); err != nil {
						log.Errorf("PodUpdated: Failed to get migration service. Err: %v", err)
						return
					}
					migrationVolumeSpec := &migration.VolumeSpec{VolumePath: volume.VsphereVolume.VolumePath}
					volumeHandle, err = volumeMigrationService.GetVolumeID(ctx, migrationVolumeSpec, true)
					if err != nil {
						log.Warnf("Failed to get VolumeID from volumeMigrationService for migration VolumeSpec: %v "+
							"with error %+v", migrationVolumeSpec, err)
						return
					}
				} else {
					log.Debugf("Volume %q is not an inline migrated vSphere volume", volume.Name)
					continue
				}
			} else {
				// For vSphere volumes we need to log the message that CSI
				// migration feature state is disabled.
				if volume.VsphereVolume != nil {
					log.Debug("CSI migration feature state is disabled")
					continue
				}
				// For non vSphere volumes, do nothing and move to next volume
				// iteration.
				log.Debugf("Ignoring the update for inline volume %q for the pod %q", volume.Name, pod.Name)
				continue
			}
		}

		// Fetch vCenterHost & volumeManager for given volume, based on VC configuration
		vcHost, cnsVolumeMgr, err := getVcHostAndVolumeManagerForVolumeID(ctx, metadataSyncer, volumeHandle)
		if err != nil {
			log.Errorf("csiUpdatePod: Failed to get VC host and volume manager for the given volume: %v. "+
				"Error occoured: %+v", volumeHandle, err)
			return
		}
		vcHostObj, vcHostObjFound := metadataSyncer.configInfo.Cfg.VirtualCenter[vcHost]
		if !vcHostObjFound {
			log.Errorf("csiUpdatePod: failed to find VC host for given volume: %q.", volumeHandle)
			return
		}

		containerCluster := cnsvsphere.GetContainerCluster(clusterIDforVolumeMetadata,
			vcHostObj.User, metadataSyncer.clusterFlavor,
			metadataSyncer.configInfo.Cfg.Global.ClusterDistribution)
		updateSpec := &cnstypes.CnsVolumeMetadataUpdateSpec{
			VolumeId: cnstypes.CnsVolumeId{
				Id: volumeHandle,
			},
			Metadata: cnstypes.CnsVolumeMetadata{
				ContainerCluster:      containerCluster,
				ContainerClusterArray: []cnstypes.CnsContainerCluster{containerCluster},
				EntityMetadata:        metadataList,
			},
		}

		log.Debugf("Calling UpdateVolumeMetadata for volume %s with updateSpec: %+v",
			updateSpec.VolumeId.Id, spew.Sdump(updateSpec))
		if err := cnsVolumeMgr.UpdateVolumeMetadata(ctx, updateSpec); err != nil {
			log.Errorf("UpdateVolumeMetadata failed for volume %s with err: %v", volume.Name, err)
		}

	}
}

func initVolumeHealthReconciler(ctx context.Context, tkgKubeClient clientset.Interface,
	svcKubeClient clientset.Interface) error {
	log := logger.GetLogger(ctx)
	// Get the supervisor namespace in which the guest cluster is deployed.
	supervisorNamespace, err := cnsconfig.GetSupervisorNamespace(ctx)
	if err != nil {
		log.Errorf("could not get supervisor namespace in which guest cluster was deployed. Err: %v", err)
		return err
	}
	log.Infof("supervisorNamespace %s", supervisorNamespace)
	log.Infof("initVolumeHealthReconciler is triggered")
	tkgInformerFactory := informers.NewSharedInformerFactory(tkgKubeClient, volumeHealthResyncPeriod)
	svcInformerFactory := informers.NewSharedInformerFactoryWithOptions(svcKubeClient,
		volumeHealthResyncPeriod, informers.WithNamespace(supervisorNamespace))
	stopCh := make(chan struct{})
	defer close(stopCh)
	rc, err := NewVolumeHealthReconciler(tkgKubeClient, svcKubeClient, volumeHealthResyncPeriod,
		tkgInformerFactory, svcInformerFactory,
		workqueue.NewTypedItemExponentialFailureRateLimiter[any](volumeHealthRetryIntervalStart,
			volumeHealthRetryIntervalMax),
		supervisorNamespace, stopCh,
	)
	if err != nil {
		return err
	}
	rc.Run(ctx, volumeHealthWorkers)
	return nil
}

func initResizeReconciler(ctx context.Context, tkgClient clientset.Interface,
	supervisorClient clientset.Interface) error {
	log := logger.GetLogger(ctx)
	supervisorNamespace, err := cnsconfig.GetSupervisorNamespace(ctx)
	if err != nil {
		log.Errorf("resize: could not get supervisor namespace in which Tanzu Kubernetes Grid was deployed. "+
			"Resize reconciler is not running for err: %v", err)
		return err
	}
	stopCh := make(chan struct{})
	defer close(stopCh)
	log.Infof("initResizeReconciler is triggered")
	// TODO: Refactor the code to use existing NewInformer function to get informerFactory
	// https://github.com/kubernetes-sigs/vsphere-csi-driver/issues/585
	informerFactory := informers.NewSharedInformerFactory(tkgClient, resizeResyncPeriod)

	rc, err := newResizeReconciler(tkgClient, supervisorClient, supervisorNamespace,
		resizeResyncPeriod, informerFactory, workqueue.NewTypedItemExponentialFailureRateLimiter[any](
			resizeRetryIntervalStart, resizeRetryIntervalMax), stopCh)
	if err != nil {
		return err
	}
	rc.Run(ctx, resizeWorkers)
	return nil
}

func initStoragePolicyQuotaReconciler(ctx context.Context, metadataSyncInformer *metadataSyncInformer) error {
	log := logger.GetLogger(ctx)
	stopCh := make(chan struct{})
	defer close(stopCh)
	log.Infof("initStoragePolicyQuotaReconciler is triggered")
	// TODO: Refactor the code to use existing NewInformer function to get informerFactory
	// https://github.com/kubernetes-sigs/vsphere-csi-driver/issues/585
	rc, err := newStoragePolicyQuotaReconciler(ctx, metadataSyncInformer,
		workqueue.NewTypedItemExponentialFailureRateLimiter[any](storagePolicyQuotaRetryIntervalStart,
			storagePolicyQuotaRetryIntervalMax), stopCh)
	if err != nil {
		log.Errorf("initStoragePolicyQuotaReconciler: err received %v", err)
		return err
	}
	rc.Run(ctx, storagePolicyQuotaWorkers)
	return nil
}

// createStoragePolicyUsageCR creates StoragePolicyUsage CR with given parameters
func createStoragePolicyUsageCR(ctx context.Context, quotaClient client.Client, name, namespace,
	storagePolicyId, storageClassName, resourceKind, resourceApiGroup,
	extensionName string) (*storagepolicyv1alpha2.StoragePolicyUsage, error) {
	log := logger.GetLogger(ctx)
	newUsageInstance := storagepolicyv1alpha2.StoragePolicyUsage{
		TypeMeta: metav1.TypeMeta{
			Kind:       cnsoperatorv1alpha1.GroupName,
			APIVersion: cnsoperatorv1alpha1.Version,
		},
		ObjectMeta: metav1.ObjectMeta{
			Name:              name,
			Namespace:         namespace,
			Generation:        0,
			CreationTimestamp: metav1.Time{},
		},
		Spec: storagepolicyv1alpha2.StoragePolicyUsageSpec{
			StoragePolicyId:       storagePolicyId,
			StorageClassName:      storageClassName,
			ResourceKind:          resourceKind,
			ResourceAPIgroup:      &resourceApiGroup,
			ResourceExtensionName: extensionName,
		},
	}
	err := quotaClient.Create(ctx, &newUsageInstance, &client.CreateOptions{})
	if err != nil {
		log.Errorf("Failed to create StoragePolicyUsage for policyID %v storageclass %v resourceKind %v. Err: %+v",
			storagePolicyId, storageClassName, resourceKind, err)
		return nil, err
	}
	log.Infof("Successfully created StoragePolicyUsage %q/%q for policyID %v storageclass %v resourceKind %v.",
		name, namespace, storagePolicyId, storageClassName, resourceKind)
	return &newUsageInstance, nil
}

// getOrCreateStoragePolicyUsageCR creates StoragePolicyUsage CR for given storagePolicyID & namespace,
// if not found already.
func getOrCreateStoragePolicyUsageCR(ctx context.Context, storagePolicyId string,
	namespace string, metadataSyncer *metadataSyncInformer) (*storagepolicyv1alpha2.StoragePolicyUsage,
	error) {
	var foundPvcUsageInstance, foundSnapUsageInstance bool
	log := logger.GetLogger(ctx)
	// Get storage classes associated with storage policy id of StoragePolicyQuota CR added
	config, err := k8s.GetKubeConfig(ctx)
	if err != nil {
		log.Errorf("getOrCreateStoragePolicyUsageCR: Failed to get KubeConfig. err: %v", err)
		return nil, err
	}
	k8sClient, err := clientset.NewForConfig(config)
	if err != nil {
		log.Errorf("getOrCreateStoragePolicyUsageCR: Failed to create kubernetes client. Err: %+v", err)
		return nil, err
	}
	storageQuotaClient, err := k8s.NewClientForGroup(ctx, config, cnsoperatorv1alpha1.GroupName)
	if err != nil {
		log.Errorf("getOrCreateStoragePolicyUsageCR: Failed to create CnsOperator client. Err: %+v", err)
		return nil, err
	}
	storageClassList, err := k8sClient.StorageV1().StorageClasses().List(ctx, metav1.ListOptions{})
	if err != nil {
		log.Errorf("getOrCreateStoragePolicyUsageCR: Failed to list storageclasses. Err: %+v", err)
		return nil, err
	}
	isStorageQuotaM2Enabled := metadataSyncer.coCommonInterface.IsFSSEnabled(ctx, common.StorageQuotaM2)
	usageCR := &storagepolicyv1alpha2.StoragePolicyUsage{}
	// For each storage class associated with storage policy id of StoragePolicyQuota CR,
	// check if StoragePolicyUsage CR with resource type PVC or Snapshot exists.
	// If not, create one with all parameters specified.
	for _, sc := range storageClassList.Items {
		if sc.Parameters[scParamStoragePolicyID] == storagePolicyId {
			policyUsageList := &storagepolicyv1alpha2.StoragePolicyUsageList{}
			err := storageQuotaClient.List(ctx, policyUsageList, &client.ListOptions{
				Namespace: namespace,
			})
			if err != nil {
				log.Errorf("getOrCreateStoragePolicyUsageCR: Failed to list %v CRs in namespace %v. Err: %+v",
					cnsoperatorv1alpha1.CnsStoragePolicyUsageSingular, namespace, err)
				return nil, err
			}
			foundPvcUsageInstance = false
			foundSnapUsageInstance = false
			for _, usage := range policyUsageList.Items {
				if usage.Spec.StoragePolicyId == storagePolicyId &&
					usage.Spec.StorageClassName == sc.Name {
					if usage.Spec.ResourceKind == ResourceKindPVC {
						foundPvcUsageInstance = true
					}
					if usage.Spec.ResourceKind == ResourceKindSnapshot && isStorageQuotaM2Enabled {
						foundSnapUsageInstance = true
					}
				}
			}
			if !foundPvcUsageInstance {
				pvcQuotaUsageInstanceName := sc.Name + "-" + storagepolicyv1alpha2.NameSuffixForPVC
				usageCR, err = createStoragePolicyUsageCR(ctx, storageQuotaClient, pvcQuotaUsageInstanceName,
					namespace, storagePolicyId, sc.Name, ResourceKindPVC, ResourceAPIgroupPVC,
					PVCQuotaExtensionServiceName)
				if err != nil {
					log.Errorf("getOrCreateStoragePolicyUsageCR: Failed to create %v CR for %v kind. Err: %+v",
						cnsoperatorv1alpha1.CnsStoragePolicyUsageSingular, ResourceKindPVC, err)
					return nil, err
				}
				log.Infof("getOrCreateStoragePolicyUsageCR: Created %v in namespace %v for policy %v "+
					"storageclass %v resourceKind %v", cnsoperatorv1alpha1.CnsStoragePolicyUsageSingular,
					namespace, storagePolicyId, sc.Name, ResourceKindPVC)
			}
			if isStorageQuotaM2Enabled && !foundSnapUsageInstance {
				snapQuotaUsageInstanceName := sc.Name + "-" + storagepolicyv1alpha2.NameSuffixForSnapshot
				usageCR, err = createStoragePolicyUsageCR(ctx, storageQuotaClient, snapQuotaUsageInstanceName,
					namespace, storagePolicyId, sc.Name, ResourceKindSnapshot, ResourceAPIgroupSnapshot,
					SnapQuotaExtensionServiceName)
				if err != nil {
					log.Errorf("getOrCreateStoragePolicyUsageCR: Failed to create %v CR for %v kind. Err: %+v",
						cnsoperatorv1alpha1.CnsStoragePolicyUsageSingular, ResourceKindSnapshot, err)
					return nil, err
				}
				log.Infof("getOrCreateStoragePolicyUsageCR: Created %v in namespace %vfor policy %v "+
					"storageclass %v resourceKind %v", cnsoperatorv1alpha1.CnsStoragePolicyUsageSingular,
					namespace, storagePolicyId, sc.Name, ResourceKindSnapshot)
			}
		}
	}
	return usageCR, nil
}

// deleteStoragePolicyUsageCR deletes StoragePolicyUsage CR for given storagePolicyId and namespace
func deleteStoragePolicyUsageCR(ctx context.Context, storagePolicyId string,
	namespace string, metadataSyncer *metadataSyncInformer) error {
	log := logger.GetLogger(ctx)
	// Get storage classes associated with storage policy id of StoragePolicyQuota CR added
	config, err := k8s.GetKubeConfig(ctx)
	if err != nil {
		log.Errorf("deleteStoragePolicyUsageCR: Failed to get KubeConfig. err: %v", err)
		return err
	}
	storageQuotaClient, err := k8s.NewClientForGroup(ctx, config, cnsoperatorv1alpha1.GroupName)
	if err != nil {
		log.Errorf("deleteStoragePolicyUsageCR: Failed to create CnsOperator client. Err: %+v", err)
		return err
	}
	policyUsageList := &storagepolicyv1alpha2.StoragePolicyUsageList{}
	err = storageQuotaClient.List(ctx, policyUsageList, &client.ListOptions{
		Namespace: namespace,
	})
	if err != nil {
		log.Errorf("deleteStoragePolicyUsageCR: Failed to list %v CRs associated in namespace %v. Err: %+v",
			cnsoperatorv1alpha1.CnsStoragePolicyUsageSingular, namespace, err)
		return err
	}
	isStorageQuotaM2Enabled := metadataSyncer.coCommonInterface.IsFSSEnabled(ctx, common.StorageQuotaM2)
	// For each storagepolicyusage matching with the storage policy id, delete the usage CR.
	for _, usage := range policyUsageList.Items {
		if usage.Spec.StoragePolicyId == storagePolicyId &&
			(usage.Spec.ResourceKind == ResourceKindPVC ||
				(isStorageQuotaM2Enabled && usage.Spec.ResourceKind == ResourceKindSnapshot)) {
			policyUsageCR := storagepolicyv1alpha2.StoragePolicyUsage{
				ObjectMeta: metav1.ObjectMeta{
					Name:      usage.Name,
					Namespace: namespace,
				},
			}
			err := storageQuotaClient.Delete(ctx, &policyUsageCR)
			if err != nil {
				if apierrors.IsNotFound(err) {
					log.Infof("deleteStoragePolicyUsageCR: StoragePolicyUsageCR instance %q/%q already "+
						"deleted. Returning success for the delete operation", namespace, usage.Name)
					continue
				}
				log.Errorf("deleteStoragePolicyUsageCR: Failed to delete StoragePolicyUsageCR for "+
					"policyID %v storageclass %v resourceKind %v. Err: %+v",
					storagePolicyId, usage.Spec.StorageClassName, ResourceAPIgroupPVC, err)
				continue
			}
			log.Infof("deleteStoragePolicyUsageCR: StoragePolicyUsageCR instance %q/%q successfully deleted.",
				namespace, usage.Name)
			continue
		}
		log.Infof("deleteStoragePolicyUsageCR: StoragePolicyUsageCR instance %q/%q does not have matching "+
			"storagepolicyid %v with quota object getting deleted or has invalid resourceKind %s associated."+
			"Ignoring this usage CR object.", namespace, usage.Name, storagePolicyId, usage.Spec.ResourceKind)
	}
	return nil
}

func createStoragePolicyUsageCRS(ctx context.Context, metadataSyncer *metadataSyncInformer) {
	log := logger.GetLogger(ctx)

	// Prepare map of storagepolicyID to storageclassnames.
	k8sconfig, err := k8s.GetKubeConfig(ctx)
	if err != nil {
		log.Errorf("createStoragePolicyUsageCRS: Failed to get KubeConfig. err: %v", err)
		return
	}
	k8sClient, err := clientset.NewForConfig(k8sconfig)
	if err != nil {
		log.Errorf("createStoragePolicyUsageCRS: Failed to create kubernetes client. Err: %+v", err)
		return
	}
	storageClassList, err := k8sClient.StorageV1().StorageClasses().List(ctx, metav1.ListOptions{})
	if err != nil {
		log.Errorf("createStoragePolicyUsageCRS: Failed to list storageclasses. Err: %+v", err)
		return
	}
	scPolicyIdToNameMap := make(map[string][]string)
	for _, sc := range storageClassList.Items {
		policyID := sc.Parameters[scParamStoragePolicyID]
		scPolicyIdToNameMap[policyID] = append(scPolicyIdToNameMap[policyID], sc.Name)
	}

	// Prepare Config and NewClientForGroup for cnsOperatorClient
	restConfig, err := config.GetConfig()
	if err != nil {
		log.Errorf("createStoragePolicyUsageCRS: Failed to get Kubernetes k8sconfig. Err: %+v", err)
		return
	}
	cnsOperatorClient, err := k8s.NewClientForGroup(ctx, restConfig, cnsoperatorv1alpha1.GroupName)
	if err != nil {
		log.Errorf("createStoragePolicyUsageCRS: Failed to create CnsOperator client. Err: %+v", err)
		return
	}

	// List storagePolicyQuota in all namespaces.
	spqList := &storagepolicyv1alpha2.StoragePolicyQuotaList{}
	err = cnsOperatorClient.List(ctx, spqList)
	if err != nil {
		log.Errorf("createStoragePolicyUsageCRS: failed to list %q CR from all "+
			"supervisor namespaces. Error: %+v", cnsoperatorv1alpha1.CnsStoragePolicyQuotaSingular, err)
		return
	}
	isStorageQuotaM2Enabled := metadataSyncer.coCommonInterface.IsFSSEnabled(ctx, common.StorageQuotaM2)
	for _, spq := range spqList.Items {
		// Make sure storagePolicyQuota instance is not getting deleted.
		if spq.DeletionTimestamp != nil {
			log.Infof("createStoragePolicyUsageCRS: Deletion timestamp set on StoragePolicyQuota %q in "+
				"namespace %q. Ignoring creation of StoragePolicyUsage for this instance.", spq.Name, spq.Namespace)
			continue
		}
		policyID := spq.Spec.StoragePolicyId
		// For each StoragePolicyQuota, verify if a StoragePolicyUsage CR exists. If not, create one.
		for _, scName := range scPolicyIdToNameMap[policyID] {
			// Create StoragePolicyUsage CR for PVC resource kind if not present already.
			storagePolicyUsageInstanceName := scName + "-" + storagepolicyv1alpha2.NameSuffixForPVC
			storagePolicyUsageCR := &storagepolicyv1alpha2.StoragePolicyUsage{}
			err = cnsOperatorClient.Get(ctx, k8stypes.NamespacedName{
				Namespace: spq.Namespace,
				Name:      storagePolicyUsageInstanceName},
				storagePolicyUsageCR)
			if err == nil {
				// Found storagePolicyUsageCR for PVC kind. Check for storagePolicyUsageCR for snapshot kind next.
				log.Debugf("createStoragePolicyUsageCRS: Found storagePolicyUsage CR %q for "+
					"StoragePolicyQuota %q in namespace %q", storagePolicyUsageInstanceName, spq.Name, spq.Namespace)
			} else {
				if apierrors.IsNotFound(err) {
					_, err = createStoragePolicyUsageCR(ctx, cnsOperatorClient, storagePolicyUsageInstanceName,
						spq.Namespace, policyID, scName, ResourceKindPVC, ResourceAPIgroupPVC,
						PVCQuotaExtensionServiceName)
					if err != nil {
						log.Errorf("createStoragePolicyUsageCRS: Failed to create %q CR with name %q in "+
							"namespace %q for %q kind. Err: %+v", cnsoperatorv1alpha1.CnsStoragePolicyUsageSingular,
							storagePolicyUsageInstanceName, spq.Namespace, ResourceKindPVC, err)
					} else {
						log.Infof("createStoragePolicyUsageCRS: Successfully created the storagePolicyUsage CR %q "+
							"in namespace %q", storagePolicyUsageInstanceName, spq.Namespace)
					}
				} else {
					log.Errorf("createStoragePolicyUsageCRS: Failed to fetch storagePolicyUsage CR %q for "+
						"StoragePolicyQuota %q in namespace %q. Error: %+v", storagePolicyUsageInstanceName, spq.Name,
						spq.Namespace, err)
				}
			}
			if isStorageQuotaM2Enabled {
				// Create StoragePolicyUsage CR for snapshot resource kind if not present already.
				storagePolicyUsageInstanceName = scName + "-" + storagepolicyv1alpha2.NameSuffixForSnapshot
				storagePolicyUsageCR = &storagepolicyv1alpha2.StoragePolicyUsage{}
				err = cnsOperatorClient.Get(ctx, k8stypes.NamespacedName{
					Namespace: spq.Namespace,
					Name:      storagePolicyUsageInstanceName},
					storagePolicyUsageCR)
				if err == nil {
					// Found storagePolicyUsageCR for snapshot kind. Move to the next SC.
					log.Debugf("createStoragePolicyUsageCRS: Found storagePolicyUsage CR %q for "+
						"StoragePolicyQuota %q in namespace %q", storagePolicyUsageInstanceName, spq.Name, spq.Namespace)
					continue
				} else {
					if apierrors.IsNotFound(err) {
						_, err = createStoragePolicyUsageCR(ctx, cnsOperatorClient, storagePolicyUsageInstanceName,
							spq.Namespace, policyID, scName, ResourceKindSnapshot, ResourceAPIgroupSnapshot,
							SnapQuotaExtensionServiceName)
						if err != nil {
							log.Errorf("createStoragePolicyUsageCRS: Failed to create %q CR with name %q in "+
								"namespace %q for %q kind. Err: %+v", cnsoperatorv1alpha1.CnsStoragePolicyUsageSingular,
								storagePolicyUsageInstanceName, spq.Namespace, ResourceKindSnapshot, err)
							continue
						}
						log.Infof("createStoragePolicyUsageCRS: Successfully created the storagePolicyUsage CR %q "+
							"in namespace %q", storagePolicyUsageInstanceName, spq.Namespace)
					} else {
						log.Errorf("createStoragePolicyUsageCRS: Failed to fetch storagePolicyUsage CR %q for "+
							"StoragePolicyQuota %q in namespace %q. Error: %+v", storagePolicyUsageInstanceName, spq.Name,
							spq.Namespace, err)
						continue
					}
				}
			}
		}
	}
}

// storagePolicyUsageCRSync patches StoragePolicyUsage CRs for k8s PVs in Bound state.
// This method also creates StoragePolicyUsage CRs when the CR is not found for PVCs in Pending state.
func storagePolicyUsageCRSync(ctx context.Context, metadataSyncer *metadataSyncInformer) {
	log := logger.GetLogger(ctx)
	log.Infof("storagePolicyUsageCRSync: Starting storage policy usage CR sync")
	// Prepare Config and NewClientForGroup for cnsOperatorClient
	restConfig, err := config.GetConfig()
	if err != nil {
		log.Errorf("storagePolicyUsageCRSync: Failed to get Kubernetes k8sconfig. Err: %+v", err)
		return
	}
	cnsOperatorClient, err := k8s.NewClientForGroup(ctx, restConfig, cnsoperatorv1alpha1.GroupName)
	if err != nil {
		log.Errorf("storagePolicyUsageCRSync: Failed to create CnsOperator client. Err: %+v", err)
		return
	}
	// Get K8s PVs in "Bound" State.
	k8sPVsInBoundState, err := getBoundPVs(ctx, metadataSyncer)
	if err != nil {
		log.Errorf("Failed to get PVs from kubernetes. Err: %+v", err)
		return
	}
	namespaceToK8sVolumesMap := make(map[string][]*v1.PersistentVolume)
	for _, pv := range k8sPVsInBoundState {
		namespaceToK8sVolumesMap[pv.Spec.ClaimRef.Namespace] =
			append(namespaceToK8sVolumesMap[pv.Spec.ClaimRef.Namespace], pv.DeepCopy())
	}
	// Get the list of all StoragePolicyUsage CRs from all supervisor namespaces.
	storagePolicyUsageList := &storagepolicyv1alpha2.StoragePolicyUsageList{}
	err = cnsOperatorClient.List(ctx, storagePolicyUsageList)
	if err != nil {
		log.Errorf("storagePolicyUsageCRSync: failed to list %s CR from all supervisor namespaces. Error: %+v",
			cnsoperatorv1alpha1.CnsStoragePolicyUsageSingular, err)
		return
	}
	volumeInfoCRList := volumeInfoService.ListAllVolumeInfos()
	cnsVolumeInfoMap := make(map[string]*cnsvolumeinfov1alpha1.CNSVolumeInfo)
	spuAggregatedSumMap := make(map[string]*resource.Quantity)
	for _, volumeInfo := range volumeInfoCRList {
		cnsVolumeInfoObj := &cnsvolumeinfov1alpha1.CNSVolumeInfo{}
		err = runtime.DefaultUnstructuredConverter.FromUnstructured(volumeInfo.(*unstructured.Unstructured).Object,
			&cnsVolumeInfoObj)
		if err != nil {
			log.Errorf("storagePolicyUsageCRSync: Failed to parse CNSVolumeInfo object: %v. Error: %+v",
				cnsVolumeInfoObj, err)
			continue
		}
		cnsVolumeInfoMap[cnsVolumeInfoObj.Name] = cnsVolumeInfoObj.DeepCopy()
		if isStorageQuotaM2FSSEnabled && cnsVolumeInfoObj.Spec.AggregatedSnapshotSize != nil {
			spuKey := generateSPUKey(cnsVolumeInfoObj)
			if usedQty := spuAggregatedSumMap[spuKey]; usedQty == nil {
				spuAggregatedSumMap[spuKey] = cnsVolumeInfoObj.Spec.AggregatedSnapshotSize
			} else {
				spuAggregatedSumMap[spuKey].Add(*cnsVolumeInfoObj.Spec.AggregatedSnapshotSize)
			}
		}
	}
	// Check if volumeInfoCRList is not empty
	if len(volumeInfoCRList) > 0 {
		// Iterate through storagePolicyUsageList
		for _, storagePolicyUsage := range storagePolicyUsageList.Items {
			totalUsedQty := resource.NewQuantity(int64(0), resource.BinarySI)
			updateSpu := false
			if storagePolicyUsage.Spec.ResourceKind == ResourceKindPVC {
				// For every storagePolicyUsage, fetch "Bound" PVs in that namespace from k8sVolumesToNamespaceMap
				if volumes, ok := namespaceToK8sVolumesMap[storagePolicyUsage.Namespace]; ok {
					for _, pv := range volumes {
						// Verify the StorageClass, StoragePolicyId match with the storagePolicyUsage spec
						// using the cnsVolumeInfo CR for the volume
						volumeHandle := pv.Spec.CSI.VolumeHandle
						// For file volumes replace prefix "file:" with "file-", since CnsVolumeInfo CR
						// is created as "file-<uuid>". For example, see below.
						// cnsvolumeinfo name: file-e6a32a53-2783-42cd-a854-4df28582f04c
						// pv.Spec.volumeHandle: file:e6a32a53-2783-42cd-a854-4df28582f04c
						if strings.HasPrefix(pv.Spec.CSI.VolumeHandle, FileVolumePrefix) {
							volumeHandle = strings.Replace(pv.Spec.CSI.VolumeHandle, ":", "-", 1)
						}
						if cnsVolumeInfo, ok := cnsVolumeInfoMap[volumeHandle]; ok {
							if cnsVolumeInfo.Spec.StorageClassName == storagePolicyUsage.Spec.StorageClassName &&
								cnsVolumeInfo.Spec.StoragePolicyID == storagePolicyUsage.Spec.StoragePolicyId &&
								cnsVolumeInfo.Spec.Namespace == storagePolicyUsage.Namespace {
								// Compute the total used capacity for the voluems in the current namespace(iteration)
								totalUsedQty.Add(*cnsVolumeInfo.Spec.Capacity)
							} else {
								continue
							}
						}
					}
					updateSpu = true
				}
			} else if isStorageQuotaM2FSSEnabled && storagePolicyUsage.Spec.ResourceKind == ResourceKindSnapshot {
				spuKey := strings.Join([]string{storagePolicyUsage.Spec.StorageClassName,
					storagePolicyUsage.Spec.StoragePolicyId, storagePolicyUsage.Namespace}, "-")
				if usedQty, ok := spuAggregatedSumMap[spuKey]; ok {
					log.Infof("storagePolicyUsageCRSync: The used capacity field for StoragepolicyUsage CR: %s "+
						"in namespace: %s Total AggregatedSnapshotSize Sum is: %s", storagePolicyUsage.Name,
						storagePolicyUsage.Namespace, usedQty.String())
					totalUsedQty = usedQty
					updateSpu = true
				}
			}
			if updateSpu {
				patchedStoragePolicyUsage := *storagePolicyUsage.DeepCopy()
				if patchedStoragePolicyUsage.Status.ResourceTypeLevelQuotaUsage != nil {
					// Compare the expected total used capacity vs the actual used capacity value in storagePolicyUsage CR
					patchedStoragePolicyUsage.Status.ResourceTypeLevelQuotaUsage.Used = totalUsedQty
					currentUsedCapacity := storagePolicyUsage.Status.ResourceTypeLevelQuotaUsage.Used
					if currentUsedCapacity.Value() != totalUsedQty.Value() {
						log.Infof("storagePolicyUsageCRSync: The used capacity field for StoragepolicyUsage CR: %s in namespace: %s "+
							"is not matching with the total capacity of all the k8s volumes in Bound state. Current: %s , "+
							"Expected: %s", storagePolicyUsage.Name, storagePolicyUsage.Namespace,
							currentUsedCapacity.String(), totalUsedQty.String())
						err := PatchStoragePolicyUsage(ctx, cnsOperatorClient, &storagePolicyUsage,
							&patchedStoragePolicyUsage)
						if err != nil {
							log.Errorf("storagePolicyUsageCRSync: Patching operation failed for StoragePolicyUsage CR: %s in "+
								"namespace: %s. err: %v. Continuing..", storagePolicyUsage.Name, patchedStoragePolicyUsage.Namespace, err)
							continue
						}
						log.Infof("storagePolicyUsageCRSync: Successfully updated the used field from %s to %s for StoragepolicyUsage "+
							"CR: %s in namespace: %s", currentUsedCapacity.String(),
							totalUsedQty.String(), patchedStoragePolicyUsage.Name, patchedStoragePolicyUsage.Namespace)
					} else {
						log.Infof("storagePolicyUsageCRSync: The used capacity field for StoragepolicyUsage CR: %s in namespace: %s "+
							"field is matching with the total capacity. Used: %s Skipping the Patch operation",
							storagePolicyUsage.Name, storagePolicyUsage.Namespace,
							storagePolicyUsage.Status.ResourceTypeLevelQuotaUsage.Used.String())
					}
				} else {
					patchedStoragePolicyUsage.Status = storagepolicyv1alpha2.StoragePolicyUsageStatus{
						ResourceTypeLevelQuotaUsage: &storagepolicyv1alpha2.QuotaUsageDetails{
							Used: totalUsedQty,
						},
					}
					err := PatchStoragePolicyUsage(ctx, cnsOperatorClient, &storagePolicyUsage,
						&patchedStoragePolicyUsage)
					if err != nil {
						log.Errorf("storagePolicyUsageCRSync: Patching operation failed for StoragePolicyUsage CR: %s in "+
							"namespace: %s. err: %v. Continuing..", storagePolicyUsage.Name, patchedStoragePolicyUsage.Namespace, err)
						continue
					}
					log.Infof("storagePolicyUsageCRSync: Successfully updated the used field to %s for StoragepolicyUsage "+
						"CR: %s in namespace: %s", totalUsedQty.String(), patchedStoragePolicyUsage.Name,
						patchedStoragePolicyUsage.Namespace)
				}
			}
		}
	}
}

func generateSPUKey(cnsVolumeInfoObj *cnsvolumeinfov1alpha1.CNSVolumeInfo) string {
	return strings.Join([]string{cnsVolumeInfoObj.Spec.StorageClassName, cnsVolumeInfoObj.Spec.StoragePolicyID,
		cnsVolumeInfoObj.Spec.Namespace}, "-")
}

// startCnsVolumeInfoCRInformer creates and starts an informer for CnsVolumeInfo custom resource.
func startCnsVolumeInfoCRInformer(ctx context.Context, cfg *restclient.Config,
	metadataSyncer *metadataSyncInformer) error {
	log := logger.GetLogger(ctx)
	// Create an informer for CnsVolumeInfo instances.
	dynInformer, err := k8s.GetDynamicInformer(ctx, cnsvolumeinfov1alpha1.GroupName,
		cnsvolumeinfov1alpha1.Version, cnsvolumeinfov1alpha1.CnsVolumeInfoPlural, metav1.NamespaceAll, cfg, true)
	if err != nil {
		return logger.LogNewErrorf(log, "failed to create dynamic informer for %s CR. Error: %+v",
			cnsvolumeinfov1alpha1.CnsVolumeInfoSingular, err)
	}
	cnsVolumeInformer := dynInformer.Informer()
	_, err = cnsVolumeInformer.AddEventHandler(cache.ResourceEventHandlerFuncs{
		UpdateFunc: func(oldObj, newObj interface{}) {
			cnsVolumeInfoCRUpdated(oldObj, newObj, metadataSyncer)
		},
	})
	if err != nil {
		return logger.LogNewErrorf(log, "failed to add event handler on informer for %q CR. Error: %v",
			cnsvolumeinfov1alpha1.CnsVolumeInfoSingular, err)
	}
	// Start informer.
	go func() {
		log.Infof("Informer to watch on %s CR starting..", cnsvolumeinfov1alpha1.CnsVolumeInfoSingular)
		cnsVolumeInformer.Run(make(chan struct{}))
	}()
	return nil
}

// cnsVolumeInfoCRUpdated updates the VolumeSnapshot StoragePolicyUsage "used" capacity. This is used to track
// snapshot aggregated capacity usage per volume basis.
func cnsVolumeInfoCRUpdated(oldObj interface{}, newObj interface{}, metadataSyncer *metadataSyncInformer) {
	ctx, log := logger.GetNewContextWithLogger()
	// Verify both objects received.
	var (
		oldCnsVolumeInfoObj cnsvolumeinfov1alpha1.CNSVolumeInfo
		newCnsVolumeInfoObj cnsvolumeinfov1alpha1.CNSVolumeInfo
	)
	err := runtime.DefaultUnstructuredConverter.FromUnstructured(
		newObj.(*unstructured.Unstructured).Object, &newCnsVolumeInfoObj)
	if err != nil {
		log.Errorf("cnsVolumeInfoCRUpdated: failed to cast new object %+v to %s. Error: %+v", newObj,
			cnsvolumeinfov1alpha1.CnsVolumeInfoSingular, err)
		return
	}
	err = runtime.DefaultUnstructuredConverter.FromUnstructured(
		oldObj.(*unstructured.Unstructured).Object, &oldCnsVolumeInfoObj)
	if err != nil {
		log.Errorf("cnsVolumeInfoCRUpdated: failed to cast old object %+v to %s. Error: %+v", oldObj,
			cnsvolumeinfov1alpha1.CnsVolumeInfoSingular, err)
		return
	}
	log.Debugf("cnsVolumeInfoCRUpdated: Old CnsVolumeInfo: +%v, Updated CnsVolumeInfo: +%+v",
		oldCnsVolumeInfoObj, newCnsVolumeInfoObj)

	// Fetch StoragePolicyUsage instance for StorageClass associated with the snapshot.
	restConfig, err := config.GetConfig()
	if err != nil {
		log.Errorf("cnsVolumeInfoCRUpdated: failed to get Kubernetes config. Err: %+v", err)
		return
	}
	cnsOperatorClient, err := k8s.NewClientForGroup(ctx, restConfig, cnsoperatorv1alpha1.GroupName)
	if err != nil {
		log.Errorf("cnsVolumeInfoCRUpdated: Failed to create CnsOperator client. Err: %+v", err)
		return
	}

	storagePolicyUsageInstanceName := newCnsVolumeInfoObj.Spec.StorageClassName + "-" +
		storagepolicyv1alpha2.NameSuffixForSnapshot
	storagePolicyUsageCR := &storagepolicyv1alpha2.StoragePolicyUsage{}
	err = cnsOperatorClient.Get(ctx, k8stypes.NamespacedName{
		Namespace: newCnsVolumeInfoObj.Spec.Namespace,
		Name:      storagePolicyUsageInstanceName},
		storagePolicyUsageCR)
	if err != nil {
		log.Errorf("failed to fetch %s instance with name %q from supervisor namespace %q. Error: %+v",
			storagepolicyv1alpha2.CRDSingular, storagePolicyUsageInstanceName,
			newCnsVolumeInfoObj.Spec.Namespace, err)
		return
	}
	var diffSnapshotSize resource.Quantity
	patchedStoragePolicyUsageCR := storagePolicyUsageCR.DeepCopy()
	if !newCnsVolumeInfoObj.Spec.ValidAggregatedSnapshotSize &&
		!newCnsVolumeInfoObj.Spec.SnapshotLatestOperationCompleteTime.IsZero() {
		// If ValidAggregatedSnapshotSize field of new CnsVolumeInfo CR is false and volume has snapshot(s), then add
		// annotation "csi.vsphere.missing-snapshot-aggregated-capacity: true" on StoragePolicyUsage CR.
		log.Infof("Couldn't retrieve aggregated snapshot size for volume %q, adding annotation %s on "+
			"StoragePolicyUsage CR", common.MissingSnapshotAggregatedCapacity, newCnsVolumeInfoObj.Spec.VolumeID)
		patchAnnotation := common.MergeMaps(newCnsVolumeInfoObj.Annotations,
			map[string]string{common.MissingSnapshotAggregatedCapacity: "true"})
		patchedStoragePolicyUsageCR.Annotations = patchAnnotation
	} else if newCnsVolumeInfoObj.Spec.ValidAggregatedSnapshotSize &&
		newCnsVolumeInfoObj.Spec.AggregatedSnapshotSize != nil {
		// Delete annotation "csi.vsphere.missing-snapshot-aggregated-capacity: true" on StoragePolicyUsage CR
		// if it was present, since ValidAggregatedSnapshotSize is true.
		delete(patchedStoragePolicyUsageCR.Annotations, common.MissingSnapshotAggregatedCapacity)

		var oldAggregatedSnapshotSize resource.Quantity
		if oldCnsVolumeInfoObj.Spec.AggregatedSnapshotSize != nil {
			oldAggregatedSnapshotSize = *oldCnsVolumeInfoObj.Spec.AggregatedSnapshotSize
		} else {
			oldAggregatedSnapshotSize = *resource.NewQuantity(0, resource.BinarySI)
		}

		if oldAggregatedSnapshotSize.Value() != newCnsVolumeInfoObj.Spec.AggregatedSnapshotSize.Value() {
			if storagePolicyUsageCR.Status.ResourceTypeLevelQuotaUsage != nil &&
				storagePolicyUsageCR.Status.ResourceTypeLevelQuotaUsage.Used != nil {
				// Patch StoragePolicyUsage Used field when Status->QuotaUsage->Used are not nil
				diffSnapshotSize = *newCnsVolumeInfoObj.Spec.AggregatedSnapshotSize
				diffSnapshotSize.Sub(oldAggregatedSnapshotSize)
				patchedStoragePolicyUsageCR.Status.ResourceTypeLevelQuotaUsage.Used.Add(diffSnapshotSize)
			} else {
				// This is a case where, the StoragePolicyUsage CR does not have Status->QuotaUsage field.
				// The block is usually executed for the 1st CreateSnapshot call.
				usedQty := *newCnsVolumeInfoObj.Spec.AggregatedSnapshotSize
				patchedStoragePolicyUsageCR.Status = storagepolicyv1alpha2.StoragePolicyUsageStatus{
					ResourceTypeLevelQuotaUsage: &storagepolicyv1alpha2.QuotaUsageDetails{
						Used: &usedQty,
					},
				}
			}
		} else {
			log.Infof("cnsVolumeInfoCRUpdated: there is no difference in aggregated snapshot size, skipping "+
				"update of StoragePolicyUsage CR: %q", patchedStoragePolicyUsageCR.Name)
			return
		}
	}

	// Patch StoragePolicyUsage CR
	err = PatchStoragePolicyUsage(ctx, cnsOperatorClient, storagePolicyUsageCR,
		patchedStoragePolicyUsageCR)
	if err != nil {
		log.Errorf("error occurred while patching StoragePolicyUsage CR %q, err: %v",
			patchedStoragePolicyUsageCR.Name, err)
		return
	}
	if diffSnapshotSize.Value() > 0 {
		log.Infof("cnsVolumeInfoCRUpdated: aggregated snapshot size increased by %s, increased Used "+
			"field for storagepolicyusage CR: %s", diffSnapshotSize.String(), patchedStoragePolicyUsageCR.Name)
	} else if diffSnapshotSize.Value() < 0 {
		log.Infof("cnsVolumeInfoCRUpdated: aggregated snapshot size decreased by %s, decreased Used "+
			"field for storagepolicyusage CR: %s", diffSnapshotSize.String(), patchedStoragePolicyUsageCR.Name)
	}
}
