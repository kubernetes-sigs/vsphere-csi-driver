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
	"sync"
	"time"

	"github.com/davecgh/go-spew/spew"
	"github.com/fsnotify/fsnotify"
	cnstypes "github.com/vmware/govmomi/cns/types"
	v1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
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

	cnsoperatorv1alpha1 "sigs.k8s.io/vsphere-csi-driver/v3/pkg/apis/cnsoperator"
	"sigs.k8s.io/vsphere-csi-driver/v3/pkg/apis/migration"
	"sigs.k8s.io/vsphere-csi-driver/v3/pkg/common/cns-lib/node"
	volumes "sigs.k8s.io/vsphere-csi-driver/v3/pkg/common/cns-lib/volume"
	cnsvsphere "sigs.k8s.io/vsphere-csi-driver/v3/pkg/common/cns-lib/vsphere"
	cnsconfig "sigs.k8s.io/vsphere-csi-driver/v3/pkg/common/config"
	"sigs.k8s.io/vsphere-csi-driver/v3/pkg/common/utils"
	"sigs.k8s.io/vsphere-csi-driver/v3/pkg/csi/service/common"
	"sigs.k8s.io/vsphere-csi-driver/v3/pkg/csi/service/common/commonco"
	"sigs.k8s.io/vsphere-csi-driver/v3/pkg/csi/service/common/commonco/k8sorchestrator"
	"sigs.k8s.io/vsphere-csi-driver/v3/pkg/csi/service/logger"
	csitypes "sigs.k8s.io/vsphere-csi-driver/v3/pkg/csi/types"
	triggercsifullsyncv1alpha1 "sigs.k8s.io/vsphere-csi-driver/v3/pkg/internalapis/cnsoperator/triggercsifullsync/v1alpha1"
	"sigs.k8s.io/vsphere-csi-driver/v3/pkg/internalapis/cnsvolumeinfo"
	"sigs.k8s.io/vsphere-csi-driver/v3/pkg/internalapis/csinodetopology"
	csinodetopologyv1alpha1 "sigs.k8s.io/vsphere-csi-driver/v3/pkg/internalapis/csinodetopology/v1alpha1"
	"sigs.k8s.io/vsphere-csi-driver/v3/pkg/internalapis/featurestates"
	k8s "sigs.k8s.io/vsphere-csi-driver/v3/pkg/kubernetes"
	"sigs.k8s.io/vsphere-csi-driver/v3/pkg/syncer/storagepool"
)

var (
	// volumeMigrationService holds the pointer to VolumeMigration instance.
	volumeMigrationService migration.VolumeMigrationService
	// volumeInfoService holds the pointer to VolumeInfo instance.
	volumeInfoService cnsvolumeinfo.VolumeInfoService
	// COInitParams stores the input params required for initiating the
	// CO agnostic orchestrator for the syncer container.
	COInitParams interface{}

	// MetadataSyncer instance for the syncer container.
	MetadataSyncer *metadataSyncInformer

	// Contains list of clusterComputeResourceMoIds on which supervisor cluster is deployed.
	clusterComputeResourceMoIds = make([]string, 0)
	clusterIDforVolumeMetadata  string

	// isMultiVCenterFssEnabled is true if the Multi VC support FSS is enabled, false otherwise.
	isMultiVCenterFssEnabled bool
	// nodeMgr stores the manager to interact with nodeVMs.
	nodeMgr node.Manager
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
	var err error
	log.Infof("Initializing MetadataSyncer")
	metadataSyncer := newInformer()
	MetadataSyncer = metadataSyncer
	metadataSyncer.configInfo = configInfo

	isMultiVCenterFssEnabled = commonco.ContainerOrchestratorUtility.IsFSSEnabled(ctx, common.MultiVCenterCSITopology)

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
		if !configInfo.Cfg.Global.InsecureFlag && configInfo.Cfg.Global.CAFile != cnsconfig.SupervisorCAFilePath {
			log.Warnf("Invalid CA file: %q is set in the vSphere Config Secret. "+
				"Setting correct CA file: %q", configInfo.Cfg.Global.CAFile, cnsconfig.SupervisorCAFilePath)
			configInfo.Cfg.Global.CAFile = cnsconfig.SupervisorCAFilePath
		}
		if commonco.ContainerOrchestratorUtility.IsFSSEnabled(ctx, common.TKGsHA) {
			clusterComputeResourceMoIds, err = common.GetClusterComputeResourceMoIds(ctx)
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
	}

	// Initialize cnsDeletionMap used by Full Sync.
	cnsDeletionMap = make(map[string]map[string]bool)
	// Initialize cnsCreationMap used by Full Sync.
	cnsCreationMap = make(map[string]map[string]bool)
	// Initialize volumeOperationsLock map
	volumeOperationsLock = make(map[string]*sync.Mutex)

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
		metadataSyncer.host = vCenter.Config.Host

		cnsDeletionMap[metadataSyncer.host] = make(map[string]bool)
		cnsCreationMap[metadataSyncer.host] = make(map[string]bool)
		volumeOperationsLock[metadataSyncer.host] = &sync.Mutex{}

		volumeManager, err := volumes.GetManager(ctx, vCenter, nil,
			false, false, false,
			metadataSyncer.coCommonInterface.IsFSSEnabled(ctx, common.ListViewPerf))
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
	} else {
		// code block only applicable to Vanilla
		tasksListViewEnabled := metadataSyncer.coCommonInterface.IsFSSEnabled(ctx, common.ListViewPerf)
		// Initialize volume manager with vcenter credentials for Vanilla flavor
		if !isMultiVCenterFssEnabled {
			// Initialize volume manager with vcenter credentials
			vCenter, err := cnsvsphere.GetVirtualCenterInstance(ctx, configInfo, false)
			vCenter.Config.ReloadVCConfigForNewClient = true
			if err != nil {
				return err
			}
			metadataSyncer.host = vCenter.Config.Host

			cnsDeletionMap[metadataSyncer.host] = make(map[string]bool)
			cnsCreationMap[metadataSyncer.host] = make(map[string]bool)
			volumeOperationsLock[metadataSyncer.host] = &sync.Mutex{}

			volumeManager, err := volumes.GetManager(ctx, vCenter, nil, false, false, false, tasksListViewEnabled)
			if err != nil {
				return logger.LogNewErrorf(log, "failed to create an instance of volume manager. err=%v", err)
			}
			metadataSyncer.volumeManager = volumeManager
		} else {
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
					return logger.LogNewErrorf(log, "failed to get vCenterInstance for vCenter Host: %q, err: %v", vcconfig.Host, err)
				}
				volumeManager, err := volumes.GetManager(ctx, vCenter, nil,
					false, true,
					multivCenterTopologyDeployment, tasksListViewEnabled)
				if err != nil {
					return logger.LogNewErrorf(log, "failed to create an instance of volume manager. err=%v", err)
				}

				metadataSyncer.volumeManagers[vcconfig.Host] = volumeManager
				cnsDeletionMap[vcconfig.Host] = make(map[string]bool)
				cnsCreationMap[vcconfig.Host] = make(map[string]bool)
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
	metadataSyncer.k8sInformerManager.AddPVCListener(
		nil, // Add.
		func(oldObj interface{}, newObj interface{}) { // Update.
			pvcUpdated(oldObj, newObj, metadataSyncer)
		},
		func(obj interface{}) { // Delete.
			pvcDeleted(obj, metadataSyncer)
		})
	metadataSyncer.k8sInformerManager.AddPVListener(
		nil, // Add.
		func(oldObj interface{}, newObj interface{}) { // Update.
			pvUpdated(oldObj, newObj, metadataSyncer)
		},
		func(obj interface{}) { // Delete.
			pvDeleted(obj, metadataSyncer)
		})
	metadataSyncer.k8sInformerManager.AddPodListener(
		func(obj interface{}) { // Add.
			podAdded(obj, metadataSyncer)
		},
		func(oldObj interface{}, newObj interface{}) { // Update.
			podUpdated(oldObj, newObj, metadataSyncer)
		},
		func(obj interface{}) { // Delete.
			podDeleted(obj, metadataSyncer)
		})
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
					log.Infof("There is a full sync already in progress. Ignoring this current cycle of periodic full sync")
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
					if !isMultiVCenterFssEnabled || len(metadataSyncer.configInfo.Cfg.VirtualCenter) == 1 {
						err := CsiFullSync(ctx, metadataSyncer, metadataSyncer.configInfo.Cfg.Global.VCenterIP)
						if err != nil {
							log.Infof("CSI full sync failed with error: %+v", err)
						}
					} else {
						vcconfigs, err := cnsvsphere.GetVirtualCenterConfigs(ctx, configInfo.Cfg)
						if err != nil {
							log.Errorf("Failed to get all virtual configs for CSI full sync. Error: %+v", err)
						}

						log.Debugf("Starting full sync for Multi VC setup with %d VCs", len(vcconfigs))

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
		vCenter, err := cnsvsphere.GetVirtualCenterInstance(ctx, configInfo, false)
		if err != nil {
			return err
		}
		pvToBackingDiskObjectIdSupportCheck = common.CheckPVtoBackingDiskObjectIdSupport(ctx, vCenter)

		if pvToBackingDiskObjectIdSupportCheck {
			go func() {
				for ; true; <-pvToBackingDiskObjectIdMappingTicker.C {
					ctx, log = logger.GetNewContextWithLogger()
					log.Info("get pv to backingDiskObjectId mapping is triggered")
					csiGetPVtoBackingDiskObjectIdMapping(ctx, k8sClient, metadataSyncer)
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
				if !metadataSyncer.coCommonInterface.IsFSSEnabled(ctx, common.VolumeHealth) {
					log.Warnf("VolumeHealth feature is disabled on the cluster")
				} else {
					log.Infof("getVolumeHealthStatus is triggered")
					csiGetVolumeHealthStatus(ctx, k8sClient, metadataSyncer)
				}
			}
		}()
	}
	if metadataSyncer.clusterFlavor == cnstypes.CnsClusterFlavorGuest {
		volumeHealthEnablementTicker := time.NewTicker(common.DefaultFeatureEnablementCheckInterval)
		defer volumeHealthEnablementTicker.Stop()
		// Trigger volume health reconciler.
		go func() {
			for ; true; <-volumeHealthEnablementTicker.C {
				ctx, log = logger.GetNewContextWithLogger()
				if !metadataSyncer.coCommonInterface.IsFSSEnabled(ctx, common.VolumeHealth) {
					log.Debugf("VolumeHealth feature is disabled on the cluster")
				} else {
					if err := initVolumeHealthReconciler(ctx, k8sClient, metadataSyncer.supervisorClient); err != nil {
						log.Warnf("Error while initializing volume health reconciler. Err:%+v. Retry will be triggered at %v",
							err, time.Now().Add(common.DefaultFeatureEnablementCheckInterval))
						continue
					}
					break
				}
			}
		}()

		volumeResizeEnablementTicker := time.NewTicker(common.DefaultFeatureEnablementCheckInterval)
		defer volumeResizeEnablementTicker.Stop()
		// Trigger resize reconciler.
		go func() {
			for ; true; <-volumeResizeEnablementTicker.C {
				ctx, log = logger.GetNewContextWithLogger()
				if !metadataSyncer.coCommonInterface.IsFSSEnabled(ctx, common.VolumeExtend) {
					log.Debugf("ExpandVolume feature is disabled on the cluster")
				} else {
					if err := initResizeReconciler(ctx, k8sClient, metadataSyncer.supervisorClient); err != nil {
						log.Warnf("Error while initializing volume resize reconciler. Err:%+v. Retry will be triggered at %v",
							err, time.Now().Add(common.DefaultFeatureEnablementCheckInterval))
						continue
					}
					break
				}
			}
		}()
	}

	<-stopCh
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
	csiNodeTopologyInformer.AddEventHandler(cache.ResourceEventHandlerFuncs{
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
	nodeVM, err := nodeMgr.GetNode(ctx, nodeTopoObj.Spec.NodeUUID, nil)
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
	var nodeTopoObj csinodetopologyv1alpha1.CSINodeTopology
	err := runtime.DefaultUnstructuredConverter.FromUnstructured(obj.(*unstructured.Unstructured).Object, &nodeTopoObj)
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
	nodeVM, err := nodeMgr.GetNode(ctx, nodeTopoObj.Spec.NodeUUID, nil)
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
	triggerCsiFullSyncInstance := &triggercsifullsyncv1alpha1.TriggerCsiFullSync{}
	key := k8stypes.NamespacedName{Namespace: "", Name: common.TriggerCsiFullSyncCRName}
	if err := client.Get(ctx, key, triggerCsiFullSyncInstance); err != nil {
		return nil, err
	}
	return triggerCsiFullSyncInstance, nil
}

// updateTriggerCsiFullSyncInstance updates the full sync instance with
// name "csifullsync".
func updateTriggerCsiFullSyncInstance(ctx context.Context,
	client client.Client, instance *triggercsifullsyncv1alpha1.TriggerCsiFullSync) error {
	if err := client.Update(ctx, instance); err != nil {
		return err
	}
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
	tasksListViewEnabled := metadataSyncer.coCommonInterface.IsFSSEnabled(ctx, common.ListViewPerf)
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
		// Vanilla ReloadConfiguration
		if isMultiVCenterFssEnabled {
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
		} else {
			oldvCenter := metadataSyncer.configInfo.Cfg.Global.VCenterIP
			newVCConfig, err := cnsvsphere.GetVirtualCenterConfig(ctx, cfg)
			newVCConfig.ReloadVCConfigForNewClient = true
			if err != nil {
				return logger.LogNewErrorf(log, "failed to get VirtualCenterConfig. err=%v", err)
			}
			if newVCConfig != nil {
				if oldvCenter != newVCConfig.Host {
					log.Infof("Observed new vCenter server: %q in the config secret. "+
						"Exiting syncer Container for re-initialization", newVCConfig.Host)
					unregisterAllvCenterErr := cnsvsphere.UnregisterAllVirtualCenters(ctx)
					if unregisterAllvCenterErr != nil {
						log.Warnf("failed to Unregister all vCenter servers. Error: %v. "+
							"Proceeding to exit the syncer container for re-initialization", unregisterAllvCenterErr)
					}
					os.Exit(1)
				}
				var vcenter *cnsvsphere.VirtualCenter
				vcenter, err = cnsvsphere.GetVirtualCenterInstance(ctx, &cnsconfig.ConfigurationInfo{Cfg: cfg}, false)
				if err != nil {
					return logger.LogNewErrorf(log, "failed to get VirtualCenter. err=%v", err)
				}
				vcenter.Config = newVCConfig
				err := metadataSyncer.volumeManager.ResetManager(ctx, vcenter)
				if err != nil {
					return logger.LogNewErrorf(log, "failed to reset volume manager. err=%v", err)
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
		if newVCConfig != nil {
			var vcenter *cnsvsphere.VirtualCenter
			newVCConfig.ReloadVCConfigForNewClient = true
			if metadataSyncer.host != newVCConfig.Host ||
				metadataSyncer.configInfo.Cfg.VirtualCenter[metadataSyncer.host].User != newVCConfig.Username ||
				metadataSyncer.configInfo.Cfg.VirtualCenter[metadataSyncer.host].Password != newVCConfig.Password ||
				reconnectToVCFromNewConfig {
				// Verify if new configuration has valid credentials by connecting
				// to vCenter. Proceed only if the connection succeeds, else return
				// error.
				newVC := &cnsvsphere.VirtualCenter{Config: newVCConfig}
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
			volumeManager, err := volumes.GetManager(ctx, vcenter, nil, false, false, false, tasksListViewEnabled)
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
	migrationEnabled := metadataSyncer.coCommonInterface.IsFSSEnabled(ctx, common.CSIMigration)
	// Verify if csi migration is ON and check if there is any label update or
	// migrated-to annotation was received for the PVC.
	if migrationEnabled && pv.Spec.VsphereVolume != nil {

		// If it is a multi VC setup, then skip this volume as we do not support vSphere to CSI migrated volumes
		// on a multi VC deployment.
		if isMultiVCenterFssEnabled && len(metadataSyncer.configInfo.Cfg.VirtualCenter) > 1 {
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
	migrationEnabled := metadataSyncer.coCommonInterface.IsFSSEnabled(ctx, common.CSIMigration)
	if migrationEnabled && pv.Spec.VsphereVolume != nil {

		// If it is a multi VC setup, then skip this volume as we do not support vSphere to CSI migrated volumes
		// on a multi VC deployment.
		if isMultiVCenterFssEnabled && len(metadataSyncer.configInfo.Cfg.VirtualCenter) > 1 {
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
		pvcsiVolumeDeleted(ctx, string(pvc.GetUID()), metadataSyncer)
	} else {
		csiPVCDeleted(ctx, pvc, pv, metadataSyncer)
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
	migrationEnabled := metadataSyncer.coCommonInterface.IsFSSEnabled(ctx, common.CSIMigration)
	if migrationEnabled && newPv.Spec.VsphereVolume != nil {

		// If it is a multi VC setup, then skip this volume as we do not support vSphere to CSI migrated volumes
		// on a multi VC deployment.
		if isMultiVCenterFssEnabled && len(metadataSyncer.configInfo.Cfg.VirtualCenter) > 1 {
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

	migrationEnabled := metadataSyncer.coCommonInterface.IsFSSEnabled(ctx, common.CSIMigration)
	if migrationEnabled && pv.Spec.VsphereVolume != nil {

		// If it is a multi VC setup, then skip this volume as we do not support vSphere to CSI migrated volumes
		// on a multi VC deployment.
		if isMultiVCenterFssEnabled && len(metadataSyncer.configInfo.Cfg.VirtualCenter) > 1 {
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
		pvcsiVolumeDeleted(ctx, string(pv.GetUID()), metadataSyncer)
	} else {
		csiPVDeleted(ctx, pv, metadataSyncer)
	}
}

// podAdded helps register inline vSphere in-tree volumes
func podAdded(obj interface{}, metadataSyncer *metadataSyncInformer) {
	ctx, log := logger.GetNewContextWithLogger()
	if metadataSyncer.coCommonInterface.IsFSSEnabled(ctx, common.CSIMigration) &&
		metadataSyncer.clusterFlavor != cnstypes.CnsClusterFlavorWorkload &&
		metadataSyncer.clusterFlavor != cnstypes.CnsClusterFlavorGuest {
		// Get pod object.
		pod, ok := obj.(*v1.Pod)
		if pod == nil || !ok {
			log.Warnf("podAdded: unrecognized new object %+v", obj)
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

	if metadataSyncer.coCommonInterface.IsFSSEnabled(ctx, common.CSIMigration) && pv.Spec.VsphereVolume != nil {
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
		err := wait.Poll(5*time.Second, time.Minute, func() (bool, error) {
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
	if metadataSyncer.coCommonInterface.IsFSSEnabled(ctx, common.CSIMigration) && pv.Spec.VsphereVolume != nil {
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
		volumeHandle     string
		err              error
		containerCluster cnstypes.CnsContainerCluster
		cnsVolumeMgr     volumes.Manager
		vcHost           string
	)

	if metadataSyncer.coCommonInterface.IsFSSEnabled(ctx, common.CSIMigration) && newPv.Spec.VsphereVolume != nil {
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
	if oldPv.Status.Phase == v1.VolumePending &&
		(newPv.Status.Phase == v1.VolumeAvailable || newPv.Status.Phase == v1.VolumeBound) &&
		!isdynamicCSIPV && newPv.Spec.CSI != nil {
		// Static PV is Created.
		var volumeType string
		if IsMultiAttachAllowed(oldPv) {
			// If it is a multi VC setup, then skip this volume as we do not support file share volumes
			// on a multi VC deployment.
			if isMultiVCenterFssEnabled && len(metadataSyncer.configInfo.Cfg.VirtualCenter) > 1 {
				log.Infof("PVUpdated: %q is a vSphere volume claim in namespace %q."+
					"File share volumes are not supported in a multi VC setup."+
					"Skipping PV update.", newPv.Name, newPv.Namespace)
				return
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

		// If it is a multi VC deployment, figure out FCD's location based on PV's nodeAffinity rules.
		if isMultiVCenterFssEnabled && len(metadataSyncer.configInfo.Cfg.VirtualCenter) > 1 {
			vcHost, cnsVolumeMgr, err = getVcHostAndVolumeManagerFromPvNodeAffinity(ctx, newPv, metadataSyncer)
			if err != nil {
				log.Errorf("PVUpdated: Failed to get VC host and volume manager for multi VC setup. "+
					"Error occoured: %+v", err)
				return
			}
		} else {
			// In case of a single VC set up, no need to look up topology segments.
			vcHost, cnsVolumeMgr, err = getVcHostAndVolumeManagerForVolumeID(ctx, metadataSyncer, volumeHandle)
			if err != nil {
				log.Errorf("PVUpdated: Failed to get VC host and volume manager for single VC setup. "+
					"Error occoured: %+v", err)
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
			createSpec := &cnstypes.CnsVolumeCreateSpec{
				Name:       oldPv.Name,
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
					BackingDiskId:           oldPv.Spec.CSI.VolumeHandle,
				}
			} else {
				createSpec.BackingObjectDetails = &cnstypes.CnsVsanFileShareBackingDetails{
					CnsFileBackingDetails: cnstypes.CnsFileBackingDetails{
						BackingFileId: oldPv.Spec.CSI.VolumeHandle,
					},
				}
			}
			log.Debugf("PVUpdated: vSphere CSI Driver is creating volume %q with create spec %+v",
				oldPv.Name, spew.Sdump(createSpec))
			_, _, err := cnsVolumeMgr.CreateVolume(ctx, createSpec)
			if err != nil {
				log.Errorf("PVUpdated: Failed to create disk %s with error %+v", oldPv.Name, err)
			} else {
				log.Infof("PVUpdated: vSphere CSI Driver has successfully marked volume: %q as the container volume.",
					oldPv.Spec.CSI.VolumeHandle)

				if isMultiVCenterFssEnabled && len(metadataSyncer.configInfo.Cfg.VirtualCenter) > 1 {
					// Create CNSVolumeInfo CR for the volume ID.
					err = volumeInfoService.CreateVolumeInfo(ctx, oldPv.Spec.CSI.VolumeHandle, vcHost)
					if err != nil {
						log.Errorf("failed to store volumeID %q for vCenter %q in CNSVolumeInfo CR. Error: %+v",
							oldPv.Spec.CSI.VolumeHandle, vcHost, err)
					}
				}
			}
			// Volume is successfully created so returning from here.
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
	} else {
		// This is the case where updates are detcted on an existing PV.
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
	if pv.Spec.ClaimRef != nil && pv.Status.Phase == v1.VolumeReleased &&
		pv.Spec.PersistentVolumeReclaimPolicy == v1.PersistentVolumeReclaimDelete {
		log.Debugf("PVDeleted: Volume deletion will be handled by Controller")
		return
	}

	if IsMultiAttachAllowed(pv) {
		// If PV is file share volume.

		// If it is a multi VC setup, then skip this volume as we do not support file share volumes
		// on a multi VC deployment
		if isMultiVCenterFssEnabled && len(metadataSyncer.configInfo.Cfg.VirtualCenter) > 1 {
			log.Debugf("PVDeleted: %q is a vSphere volume claim in namespace %q."+
				"File share volumes are not supported in a multi VC setup."+
				"Skipping deletion of PV metadata.", pv.Name, pv.Namespace)
			return
		}

		// Setting volumeHandle as empty as there is only 1 VC so volumeID does not matter.
		vcHost, cnsVolumeMgr, err := getVcHostAndVolumeManagerForVolumeID(ctx, metadataSyncer, "")
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
		queryResult, err := utils.QueryVolumeUtil(ctx, cnsVolumeMgr, queryFilter,
			nil, metadataSyncer.coCommonInterface.IsFSSEnabled(ctx, common.AsyncQueryVolume))
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
		migrationFeatureEnabled := metadataSyncer.coCommonInterface.IsFSSEnabled(ctx, common.CSIMigration)
		if migrationFeatureEnabled && pv.Spec.VsphereVolume != nil {
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
		if migrationFeatureEnabled && pv.Spec.VsphereVolume != nil {
			// Delete the cnsvspherevolumemigration crd instance when PV is deleted.
			err = volumeMigrationService.DeleteVolumeInfo(ctx, volumeHandle)
			if err != nil {
				log.Errorf("PVDeleted: failed to delete volumeInfo CR for volume: %q. Error: %+v", volumeHandle, err)
				return
			}
		}
		if isMultiVCenterFssEnabled && len(metadataSyncer.configInfo.Cfg.VirtualCenter) > 1 {
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
				if metadataSyncer.coCommonInterface.IsFSSEnabled(ctx, common.CSIMigration) && pv.Spec.VsphereVolume != nil {
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
			if metadataSyncer.coCommonInterface.IsFSSEnabled(ctx, common.CSIMigration) {
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
		workqueue.NewItemExponentialFailureRateLimiter(volumeHealthRetryIntervalStart, volumeHealthRetryIntervalMax),
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

	rc, err := newResizeReconciler(tkgClient, supervisorClient, supervisorNamespace, resizeResyncPeriod, informerFactory,
		workqueue.NewItemExponentialFailureRateLimiter(resizeRetryIntervalStart, resizeRetryIntervalMax),
		stopCh,
	)
	if err != nil {
		return err
	}
	rc.Run(ctx, resizeWorkers)
	return nil
}
