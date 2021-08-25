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
	"time"

	"github.com/davecgh/go-spew/spew"
	"github.com/fsnotify/fsnotify"
	cnstypes "github.com/vmware/govmomi/cns/types"
	v1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	k8stypes "k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/client-go/informers"
	clientset "k8s.io/client-go/kubernetes"
	"k8s.io/client-go/util/workqueue"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/config"

	cnsoperatorv1alpha1 "sigs.k8s.io/vsphere-csi-driver/v2/pkg/apis/cnsoperator"
	"sigs.k8s.io/vsphere-csi-driver/v2/pkg/apis/migration"
	volumes "sigs.k8s.io/vsphere-csi-driver/v2/pkg/common/cns-lib/volume"
	cnsvsphere "sigs.k8s.io/vsphere-csi-driver/v2/pkg/common/cns-lib/vsphere"
	cnsconfig "sigs.k8s.io/vsphere-csi-driver/v2/pkg/common/config"
	"sigs.k8s.io/vsphere-csi-driver/v2/pkg/common/utils"
	"sigs.k8s.io/vsphere-csi-driver/v2/pkg/csi/service/common"
	"sigs.k8s.io/vsphere-csi-driver/v2/pkg/csi/service/common/commonco"
	"sigs.k8s.io/vsphere-csi-driver/v2/pkg/csi/service/common/commonco/k8sorchestrator"
	"sigs.k8s.io/vsphere-csi-driver/v2/pkg/csi/service/logger"
	csitypes "sigs.k8s.io/vsphere-csi-driver/v2/pkg/csi/types"
	triggercsifullsyncv1alpha1 "sigs.k8s.io/vsphere-csi-driver/v2/pkg/internalapis/cnsoperator/triggercsifullsync/v1alpha1"
	"sigs.k8s.io/vsphere-csi-driver/v2/pkg/internalapis/featurestates"
	k8s "sigs.k8s.io/vsphere-csi-driver/v2/pkg/kubernetes"
	"sigs.k8s.io/vsphere-csi-driver/v2/pkg/syncer/storagepool"
)

var (
	// volumeMigrationService holds the pointer to VolumeMigration instance.
	volumeMigrationService migration.VolumeMigrationService
	// COInitParams stores the input params required for initiating the
	// CO agnostic orchestrator for the syncer container.
	COInitParams interface{}

	// MetadataSyncer instance for the syncer container.
	MetadataSyncer *metadataSyncInformer
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

// InitMetadataSyncer initializes the Metadata Sync Informer.
func InitMetadataSyncer(ctx context.Context, clusterFlavor cnstypes.CnsClusterFlavor,
	configInfo *cnsconfig.ConfigurationInfo) error {
	log := logger.GetLogger(ctx)
	var err error
	log.Infof("Initializing MetadataSyncer")
	metadataSyncer := newInformer()
	MetadataSyncer = metadataSyncer
	metadataSyncer.configInfo = configInfo

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
	} else {
		// Initialize volume manager with vcenter credentials, if metadata syncer
		// is being intialized for Vanilla or Supervisor clusters.
		vCenter, err := cnsvsphere.GetVirtualCenterInstance(ctx, configInfo, false)
		if err != nil {
			return err
		}
		metadataSyncer.host = vCenter.Config.Host
		metadataSyncer.volumeManager = volumes.GetManager(ctx, vCenter, nil, false)
	}

	if metadataSyncer.clusterFlavor == cnstypes.CnsClusterFlavorWorkload {
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
	}

	// Initialize cnsDeletionMap used by Full Sync.
	cnsDeletionMap = make(map[string]bool)
	// Initialize cnsCreationMap used by Full Sync.
	cnsCreationMap = make(map[string]bool)

	cfgPath := common.GetConfigPath(ctx)
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
	metadataSyncer.k8sInformerManager = k8s.NewInformer(k8sClient)
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
		nil, // Add.
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
				} else {
					err := CsiFullSync(ctx, metadataSyncer)
					if err != nil {
						log.Infof("CSI full sync failed with error: %+v", err)
					}
				}
			}
		}()
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
	cfg, err := common.GetConfig(ctx)
	if err != nil {
		return logger.LogNewErrorf(log, "failed to read config. Error: %+v", err)
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
	} else {
		newVCConfig, err := cnsvsphere.GetVirtualCenterConfig(ctx, cfg)
		if err != nil {
			return logger.LogNewErrorf(log, "failed to get VirtualCenterConfig. err=%v", err)
		}
		if newVCConfig != nil {
			var vcenter *cnsvsphere.VirtualCenter
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
			metadataSyncer.volumeManager.ResetManager(ctx, vcenter)
			metadataSyncer.volumeManager = volumes.GetManager(ctx, vcenter, nil, false)
			if metadataSyncer.clusterFlavor == cnstypes.CnsClusterFlavorWorkload {
				storagepool.ResetVC(ctx, vcenter)
			}
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
		if !isValidvSphereVolumeClaim(ctx, newPvc.ObjectMeta) {
			log.Debugf("PVCUpdated: %q is not a valid vSphere volume claim in namespace %q. Skipping update",
				newPvc.Name, newPvc.Namespace)
			return
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
		if !isValidvSphereVolumeClaim(ctx, pvc.ObjectMeta) {
			log.Debugf("PVCDeleted: %q is not a valid vSphere volume claim in namespace %q. "+
				"Skipping deletion of PVC metadata.", pvc.Name, pvc.Namespace)
			return
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
		if !isValidvSphereVolume(ctx, newPv.ObjectMeta) {
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
		if !isValidvSphereVolume(ctx, pv.ObjectMeta) {
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
	var volumeHandle string
	var err error
	if metadataSyncer.coCommonInterface.IsFSSEnabled(ctx, common.CSIMigration) && pv.Spec.VsphereVolume != nil {
		// In case if feature state switch is enabled after syncer is deployed,
		// we need to initialize the volumeMigrationService.
		if err = initVolumeMigrationService(ctx, metadataSyncer); err != nil {
			log.Errorf("PVC Updated: Failed to get migration service. Err: %v", err)
			return
		}
		migrationVolumeSpec := &migration.VolumeSpec{VolumePath: pv.Spec.VsphereVolume.VolumePath,
			StoragePolicyName: pv.Spec.VsphereVolume.StoragePolicyName}
		volumeHandle, err = volumeMigrationService.GetVolumeID(ctx, migrationVolumeSpec)
		if err != nil {
			log.Errorf("PVC Updated: Failed to get VolumeID from volumeMigrationService for migration VolumeSpec: %v "+
				"with error %+v", migrationVolumeSpec, err)
			return
		}
	} else {
		volumeFound := false
		volumeHandle = pv.Spec.CSI.VolumeHandle
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
			queryResult, err := utils.QueryAllVolumeUtil(ctx, metadataSyncer.volumeManager, queryFilter,
				cnstypes.CnsQuerySelection{}, metadataSyncer.coCommonInterface.IsFSSEnabled(ctx, common.AsyncQueryVolume))
			if err != nil {
				log.Errorf("PVCUpdated: QueryVolume failed with err=%+v", err.Error())
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

	// Create updateSpec.
	var metadataList []cnstypes.BaseCnsEntityMetadata
	entityReference := cnsvsphere.CreateCnsKuberenetesEntityReference(string(cnstypes.CnsKubernetesEntityTypePV),
		pv.Name, "", metadataSyncer.configInfo.Cfg.Global.ClusterID)
	pvcMetadata := cnsvsphere.GetCnsKubernetesEntityMetaData(pvc.Name, pvc.Labels, false,
		string(cnstypes.CnsKubernetesEntityTypePVC), pvc.Namespace, metadataSyncer.configInfo.Cfg.Global.ClusterID,
		[]cnstypes.CnsKubernetesEntityReference{entityReference})

	metadataList = append(metadataList, cnstypes.BaseCnsEntityMetadata(pvcMetadata))
	containerCluster := cnsvsphere.GetContainerCluster(metadataSyncer.configInfo.Cfg.Global.ClusterID,
		metadataSyncer.configInfo.Cfg.VirtualCenter[metadataSyncer.host].User, metadataSyncer.clusterFlavor,
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
	if err := metadataSyncer.volumeManager.UpdateVolumeMetadata(ctx, updateSpec); err != nil {
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
		metadataSyncer.configInfo.Cfg.Global.ClusterID, nil)
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
		volumeHandle, err = volumeMigrationService.GetVolumeID(ctx, migrationVolumeSpec)
		if err != nil {
			log.Errorf("PVC Deleted: Failed to get VolumeID from volumeMigrationService for migration VolumeSpec: %v "+
				"with error %+v", migrationVolumeSpec, err)
			return
		}
	} else {
		volumeHandle = pv.Spec.CSI.VolumeHandle
	}
	containerCluster := cnsvsphere.GetContainerCluster(metadataSyncer.configInfo.Cfg.Global.ClusterID,
		metadataSyncer.configInfo.Cfg.VirtualCenter[metadataSyncer.host].User,
		metadataSyncer.clusterFlavor, metadataSyncer.configInfo.Cfg.Global.ClusterDistribution)
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
	if err := metadataSyncer.volumeManager.UpdateVolumeMetadata(ctx, updateSpec); err != nil {
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
		string(cnstypes.CnsKubernetesEntityTypePV), "", metadataSyncer.configInfo.Cfg.Global.ClusterID, nil)
	metadataList = append(metadataList, cnstypes.BaseCnsEntityMetadata(pvMetadata))
	var volumeHandle string
	var err error
	containerCluster := cnsvsphere.GetContainerCluster(metadataSyncer.configInfo.Cfg.Global.ClusterID,
		metadataSyncer.configInfo.Cfg.VirtualCenter[metadataSyncer.host].User, metadataSyncer.clusterFlavor,
		metadataSyncer.configInfo.Cfg.Global.ClusterDistribution)
	if metadataSyncer.coCommonInterface.IsFSSEnabled(ctx, common.CSIMigration) && newPv.Spec.VsphereVolume != nil {
		// In case if feature state switch is enabled after syncer is deployed,
		// we need to initialize the volumeMigrationService.
		if err = initVolumeMigrationService(ctx, metadataSyncer); err != nil {
			log.Errorf("PVUpdated: Failed to get migration service. Err: %v", err)
			return
		}
		volumeHandle, err = volumeMigrationService.GetVolumeID(ctx,
			&migration.VolumeSpec{VolumePath: newPv.Spec.VsphereVolume.VolumePath,
				StoragePolicyName: newPv.Spec.VsphereVolume.StoragePolicyName})
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
	if oldPv.Status.Phase == v1.VolumePending && newPv.Status.Phase == v1.VolumeAvailable &&
		!isdynamicCSIPV && newPv.Spec.CSI != nil {
		// Static PV is Created.
		var volumeType string
		if IsMultiAttachAllowed(oldPv) {
			volumeType = common.FileVolumeType
		} else {
			volumeType = common.BlockVolumeType
		}
		log.Debugf("PVUpdated: observed static volume provisioning for the PV: %q with volumeType: %q",
			newPv.Name, volumeType)
		queryFilter := cnstypes.CnsQueryFilter{
			VolumeIds: []cnstypes.CnsVolumeId{{Id: oldPv.Spec.CSI.VolumeHandle}},
		}
		volumeOperationsLock.Lock()
		defer volumeOperationsLock.Unlock()
		// QueryAll with no selection will return only the volume ID.
		queryResult, err := utils.QueryAllVolumeUtil(ctx, metadataSyncer.volumeManager, queryFilter,
			cnstypes.CnsQuerySelection{}, metadataSyncer.coCommonInterface.IsFSSEnabled(ctx, common.AsyncQueryVolume))
		if err != nil {
			log.Errorf("PVUpdated: QueryVolume failed with err=%+v", err.Error())
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
			_, err := metadataSyncer.volumeManager.CreateVolume(ctx, createSpec)
			if err != nil {
				log.Errorf("PVUpdated: Failed to create disk %s with error %+v", oldPv.Name, err)
			} else {
				log.Infof("PVUpdated: vSphere CSI Driver has successfully marked volume: %q as the container volume.",
					oldPv.Spec.CSI.VolumeHandle)
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
	if err := metadataSyncer.volumeManager.UpdateVolumeMetadata(ctx, updateSpec); err != nil {
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
	volumeOperationsLock.Lock()
	defer volumeOperationsLock.Unlock()

	if IsMultiAttachAllowed(pv) {
		// If PV is file share volume.
		log.Debugf("PVDeleted: vSphere CSI Driver is calling UpdateVolumeMetadata to "+
			"delete volume metadata references for PV: %q", pv.Name)
		var metadataList []cnstypes.BaseCnsEntityMetadata
		pvMetadata := cnsvsphere.GetCnsKubernetesEntityMetaData(pv.Name, nil, true,
			string(cnstypes.CnsKubernetesEntityTypePV), "", metadataSyncer.configInfo.Cfg.Global.ClusterID, nil)
		metadataList = append(metadataList, cnstypes.BaseCnsEntityMetadata(pvMetadata))

		containerCluster := cnsvsphere.GetContainerCluster(metadataSyncer.configInfo.Cfg.Global.ClusterID,
			metadataSyncer.configInfo.Cfg.VirtualCenter[metadataSyncer.host].User,
			metadataSyncer.clusterFlavor, metadataSyncer.configInfo.Cfg.Global.ClusterDistribution)
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
		if err := metadataSyncer.volumeManager.UpdateVolumeMetadata(ctx, updateSpec); err != nil {
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
		queryResult, err := utils.QueryVolumeUtil(ctx, metadataSyncer.volumeManager, queryFilter,
			cnstypes.CnsQuerySelection{}, metadataSyncer.coCommonInterface.IsFSSEnabled(ctx, common.AsyncQueryVolume))
		if err != nil {
			log.Error("PVDeleted: QueryVolume failed with err=%+v", err.Error())
			return
		}
		if queryResult != nil && len(queryResult.Volumes) == 1 &&
			len(queryResult.Volumes[0].Metadata.EntityMetadata) == 0 {
			log.Infof("PVDeleted: Volume: %q is not in use by any other entity. Removing CNS tag.",
				pv.Spec.CSI.VolumeHandle)
			err := metadataSyncer.volumeManager.DeleteVolume(ctx, pv.Spec.CSI.VolumeHandle, false)
			if err != nil {
				log.Errorf("PVDeleted: Failed to delete volume %q with error %+v", pv.Spec.CSI.VolumeHandle, err)
				return
			}
		}

	} else {
		var volumeHandle string
		var err error
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
			volumeHandle, err = volumeMigrationService.GetVolumeID(ctx, migrationVolumeSpec)
			if err != nil {
				log.Errorf("PVDeleted: Failed to get VolumeID from volumeMigrationService for migration VolumeSpec: %v "+
					"with error %+v", migrationVolumeSpec, err)
				return
			}
		} else {
			volumeHandle = pv.Spec.CSI.VolumeHandle
		}

		log.Debugf("PVDeleted: vSphere CSI Driver is deleting volume %v", pv)

		if err := metadataSyncer.volumeManager.DeleteVolume(ctx, volumeHandle, false); err != nil {
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
	}
}

// csiUpdatePod update/deletes pod CnsVolumeMetadata when pod has been
// created/deleted on Vanilla k8s and supervisor cluster have been updated.
func csiUpdatePod(ctx context.Context, pod *v1.Pod, metadataSyncer *metadataSyncInformer, deleteFlag bool) {
	log := logger.GetLogger(ctx)
	// Iterate through volumes attached to pod.
	for _, volume := range pod.Spec.Volumes {
		var volumeHandle string
		var metadataList []cnstypes.BaseCnsEntityMetadata
		var podMetadata *cnstypes.CnsKubernetesEntityMetadata
		if volume.PersistentVolumeClaim != nil {
			valid, pv, pvc := IsValidVolume(ctx, volume, pod, metadataSyncer)
			if valid {
				if !deleteFlag {
					// We need to update metadata for pods having corresponding PVC
					// as an entity reference.
					entityReference := cnsvsphere.CreateCnsKuberenetesEntityReference(
						string(cnstypes.CnsKubernetesEntityTypePVC), pvc.Name, pvc.Namespace,
						metadataSyncer.configInfo.Cfg.Global.ClusterID)
					podMetadata = cnsvsphere.GetCnsKubernetesEntityMetaData(pod.Name, nil,
						deleteFlag, string(cnstypes.CnsKubernetesEntityTypePOD), pod.Namespace,
						metadataSyncer.configInfo.Cfg.Global.ClusterID,
						[]cnstypes.CnsKubernetesEntityReference{entityReference})
				} else {
					// Deleting the pod metadata.
					podMetadata = cnsvsphere.GetCnsKubernetesEntityMetaData(pod.Name, nil, deleteFlag,
						string(cnstypes.CnsKubernetesEntityTypePOD), pod.Namespace,
						metadataSyncer.configInfo.Cfg.Global.ClusterID, nil)
				}
				metadataList = append(metadataList, cnstypes.BaseCnsEntityMetadata(podMetadata))
				var err error
				if metadataSyncer.coCommonInterface.IsFSSEnabled(ctx, common.CSIMigration) && pv.Spec.VsphereVolume != nil {
					// In case if feature state switch is enabled after syncer is
					// deployed, we need to initialize the volumeMigrationService.
					if err = initVolumeMigrationService(ctx, metadataSyncer); err != nil {
						log.Errorf("PodUpdated: Failed to get migration service. Err: %v", err)
						return
					}
					migrationVolumeSpec := &migration.VolumeSpec{VolumePath: pv.Spec.VsphereVolume.VolumePath,
						StoragePolicyName: pv.Spec.VsphereVolume.StoragePolicyName}
					volumeHandle, err = volumeMigrationService.GetVolumeID(ctx, migrationVolumeSpec)
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
						metadataSyncer.configInfo.Cfg.Global.ClusterID, nil)
					metadataList = append(metadataList, cnstypes.BaseCnsEntityMetadata(podMetadata))
					var err error
					// In case if feature state switch is enabled after syncer is
					// deployed, we need to initialize the volumeMigrationService.
					if err = initVolumeMigrationService(ctx, metadataSyncer); err != nil {
						log.Errorf("PodUpdated: Failed to get migration service. Err: %v", err)
						return
					}
					migrationVolumeSpec := &migration.VolumeSpec{VolumePath: volume.VsphereVolume.VolumePath}
					volumeHandle, err = volumeMigrationService.GetVolumeID(ctx, migrationVolumeSpec)
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
		containerCluster := cnsvsphere.GetContainerCluster(metadataSyncer.configInfo.Cfg.Global.ClusterID,
			metadataSyncer.configInfo.Cfg.VirtualCenter[metadataSyncer.host].User,
			metadataSyncer.clusterFlavor, metadataSyncer.configInfo.Cfg.Global.ClusterDistribution)
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
		if err := metadataSyncer.volumeManager.UpdateVolumeMetadata(ctx, updateSpec); err != nil {
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
