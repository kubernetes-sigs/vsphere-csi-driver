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

package manager

import (
	"context"
	"fmt"
	"path/filepath"
	"reflect"
	"time"

	"github.com/fsnotify/fsnotify"
	cnstypes "github.com/vmware/govmomi/cns/types"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	k8stypes "k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client/config"
	"sigs.k8s.io/controller-runtime/pkg/manager"

	vmoperatortypes "github.com/vmware-tanzu/vm-operator/api/v1alpha5"
	metricsserver "sigs.k8s.io/controller-runtime/pkg/metrics/server"
	cnsoperatorv1alpha1 "sigs.k8s.io/vsphere-csi-driver/v3/pkg/apis/cnsoperator"
	cnsvolumemetadatav1alpha1 "sigs.k8s.io/vsphere-csi-driver/v3/pkg/apis/cnsoperator/cnsvolumemetadata/v1alpha1"
	cnsoperatorconfig "sigs.k8s.io/vsphere-csi-driver/v3/pkg/apis/cnsoperator/config"
	wcpcapapis "sigs.k8s.io/vsphere-csi-driver/v3/pkg/apis/wcpcapabilities"
	volumes "sigs.k8s.io/vsphere-csi-driver/v3/pkg/common/cns-lib/volume"
	cnsvsphere "sigs.k8s.io/vsphere-csi-driver/v3/pkg/common/cns-lib/vsphere"
	commonconfig "sigs.k8s.io/vsphere-csi-driver/v3/pkg/common/config"
	"sigs.k8s.io/vsphere-csi-driver/v3/pkg/csi/service/common"
	"sigs.k8s.io/vsphere-csi-driver/v3/pkg/csi/service/common/commonco"
	"sigs.k8s.io/vsphere-csi-driver/v3/pkg/csi/service/logger"
	"sigs.k8s.io/vsphere-csi-driver/v3/pkg/internalapis"
	internalapiscnsoperatorconfig "sigs.k8s.io/vsphere-csi-driver/v3/pkg/internalapis/cnsoperator/config"
	triggercsifullsyncv1alpha1 "sigs.k8s.io/vsphere-csi-driver/v3/pkg/internalapis/cnsoperator/triggercsifullsync/v1alpha1"
	"sigs.k8s.io/vsphere-csi-driver/v3/pkg/internalapis/csinodetopology"
	csinodetopologyconfig "sigs.k8s.io/vsphere-csi-driver/v3/pkg/internalapis/csinodetopology/config"
	csinodetopologyv1alpha1 "sigs.k8s.io/vsphere-csi-driver/v3/pkg/internalapis/csinodetopology/v1alpha1"
	k8s "sigs.k8s.io/vsphere-csi-driver/v3/pkg/kubernetes"
	"sigs.k8s.io/vsphere-csi-driver/v3/pkg/syncer"
	"sigs.k8s.io/vsphere-csi-driver/v3/pkg/syncer/cnsoperator/controller"
)

var (
	// Use localhost and port for metrics
	metricsHost       = "0.0.0.0"
	metricsPort int32 = 8383
)

type cnsOperatorInfo struct {
	configInfo        *commonconfig.ConfigurationInfo
	coCommonInterface commonco.COCommonInterface
}

// InitCnsOperator initializes the Cns Operator.
func InitCnsOperator(ctx context.Context, clusterFlavor cnstypes.CnsClusterFlavor,
	configInfo *commonconfig.ConfigurationInfo, coInitParams *interface{}) error {
	log := logger.GetLogger(ctx)
	log.Infof("Initializing CNS Operator")
	cnsOperator := &cnsOperatorInfo{}
	cnsOperator.configInfo = configInfo

	var volumeManager volumes.Manager
	if clusterFlavor == cnstypes.CnsClusterFlavorWorkload || clusterFlavor == cnstypes.CnsClusterFlavorVanilla {
		var (
			vCenter  *cnsvsphere.VirtualCenter
			err      error
			vcconfig *cnsvsphere.VirtualCenterConfig
		)
		vcconfig, err = cnsvsphere.GetVirtualCenterConfig(ctx, cnsOperator.configInfo.Cfg)
		if err != nil {
			log.Errorf("failed to get VirtualCenterConfig. Err: %+v", err)
			return err
		}
		vCenter, err = cnsvsphere.GetVirtualCenterInstanceForVCenterConfig(ctx, vcconfig, false)
		if err != nil {
			return err
		}

		volumeManager, err = volumes.GetManager(ctx, vCenter, nil, false, false, false, clusterFlavor)
		if err != nil {
			return logger.LogNewErrorf(log, "failed to create an instance of volume manager. err=%v", err)
		}
	}

	// Get a config to talk to the apiserver
	restConfig, err := config.GetConfig()
	if err != nil {
		log.Errorf("failed to get Kubernetes config. Err: %+v", err)
		return err
	}

	// Initialize the k8s orchestrator interface.
	cnsOperator.coCommonInterface, err = commonco.GetContainerOrchestratorInterface(ctx, common.Kubernetes,
		clusterFlavor, coInitParams)
	if err != nil {
		log.Errorf("failed to create CO agnostic interface. Err: %v", err)
		return err
	}

	// TODO: Verify leader election for CNS Operator in multi-master mode
	// Create CRD's for WCP flavor.
	if clusterFlavor == cnstypes.CnsClusterFlavorWorkload {
		syncer.IsPodVMOnStretchSupervisorFSSEnabled = cnsOperator.coCommonInterface.IsFSSEnabled(ctx,
			common.PodVMOnStretchedSupervisor)
		// Create CnsNodeVmAttachment CRD
		err = k8s.CreateCustomResourceDefinitionFromManifest(ctx, cnsoperatorconfig.EmbedCnsNodeVmAttachmentCRFile,
			cnsoperatorconfig.EmbedCnsNodeVmAttachmentCRFileName)
		if err != nil {
			crdNameNodeVMAttachment := cnsoperatorv1alpha1.CnsNodeVMAttachmentPlural +
				"." + cnsoperatorv1alpha1.SchemeGroupVersion.Group
			log.Errorf("failed to create %q CRD. Err: %+v", crdNameNodeVMAttachment, err)
			return err
		}

		if cnsOperator.coCommonInterface.IsFSSEnabled(ctx,
			common.SharedDiskFss) {
			// Create CnsNodeVmBatchAttachment CRD
			err = k8s.CreateCustomResourceDefinitionFromManifest(ctx,
				cnsoperatorconfig.EmbedCnsNodeVmBatchAttachmentCRFile,
				cnsoperatorconfig.EmbedCnsNodeVmABatchttachmentCRFileName)
			if err != nil {
				crdNameNodeVmBatchAttachment := cnsoperatorv1alpha1.CnsNodeVmBatchAttachmentPlural +
					"." + cnsoperatorv1alpha1.SchemeGroupVersion.Group
				log.Errorf("failed to create %q CRD. Err: %+v", crdNameNodeVmBatchAttachment, err)
				return err
			}
		}

		// Create CnsVolumeMetadata CRD
		err = k8s.CreateCustomResourceDefinitionFromManifest(ctx, cnsoperatorconfig.EmbedCnsVolumeMetadataCRFile,
			cnsoperatorconfig.EmbedCnsVolumeMetadataCRFileName)
		if err != nil {
			crdKindVolumeMetadata := reflect.TypeOf(cnsvolumemetadatav1alpha1.CnsVolumeMetadata{}).Name()
			log.Errorf("failed to create %q CRD. Err: %+v", crdKindVolumeMetadata, err)
			return err
		}

		var stretchedSupervisor bool
		if commonco.ContainerOrchestratorUtility.IsFSSEnabled(ctx, common.TKGsHA) {
			clusterComputeResourceMoIds, _, err := common.GetClusterComputeResourceMoIds(ctx)
			if err != nil {
				log.Errorf("failed to get clusterComputeResourceMoIds. err: %v", err)
				return err
			}
			if len(clusterComputeResourceMoIds) > 1 {
				stretchedSupervisor = true
			}
		}
		if stretchedSupervisor {
			log.Info("Observed stretchedSupervisor setup")
		}
		if !stretchedSupervisor || (stretchedSupervisor && syncer.IsPodVMOnStretchSupervisorFSSEnabled) {
			// Create CnsRegisterVolume CRD from manifest.
			log.Infof("Creating %q CRD", cnsoperatorv1alpha1.CnsRegisterVolumePlural)
			err = k8s.CreateCustomResourceDefinitionFromManifest(ctx, cnsoperatorconfig.EmbedCnsRegisterVolumeCRFile,
				cnsoperatorconfig.EmbedCnsRegisterVolumeCRFileName)
			if err != nil {
				log.Errorf("Failed to create %q CRD. Err: %+v", cnsoperatorv1alpha1.CnsRegisterVolumePlural, err)
				return err
			}
			log.Infof("%q CRD is created successfully", cnsoperatorv1alpha1.CnsRegisterVolumePlural)

			// Clean up routine to cleanup successful CnsRegisterVolume instances.
			log.Info("Starting go routine to cleanup successful CnsRegisterVolume instances.")
			err = watcher(ctx, cnsOperator)
			if err != nil {
				log.Error("Failed to watch on config file for changes to "+
					"CnsRegisterVolumesCleanupIntervalInMin. Error: %+v", err)
				return err
			}
			go func() {
				for {
					ctx, log = logger.GetNewContextWithLogger()
					log.Infof("Triggering CnsRegisterVolume cleanup routine")
					cleanUpCnsRegisterVolumeInstances(ctx, restConfig,
						cnsOperator.configInfo.Cfg.Global.CnsRegisterVolumesCleanupIntervalInMin)
					log.Infof("Completed CnsRegisterVolume cleanup")
					for i := 1; i <= cnsOperator.configInfo.Cfg.Global.CnsRegisterVolumesCleanupIntervalInMin; i++ {
						time.Sleep(time.Duration(1 * time.Minute))
					}
				}
			}()

			if commonco.ContainerOrchestratorUtility.IsFSSEnabled(ctx, common.CnsUnregisterVolume) {
				// Create CnsUnregisterVolume CRD from manifest.
				log.Infof("Creating %q CRD", cnsoperatorv1alpha1.CnsUnregisterVolumePlural)
				err = k8s.CreateCustomResourceDefinitionFromManifest(ctx, cnsoperatorconfig.EmbedCnsUnregisterVolumeCRFile,
					cnsoperatorconfig.EmbedCnsUnregisterVolumeCRFileName)
				if err != nil {
					log.Errorf("Failed to create %q CRD. Err: %+v", cnsoperatorv1alpha1.CnsUnregisterVolumePlural, err)
					return err
				}
				log.Infof("%q CRD is created successfully", cnsoperatorv1alpha1.CnsUnregisterVolumePlural)

				// Clean up routine to cleanup successful CnsUnregisterVolume instances.
				log.Info("Starting go routine to cleanup successful CnsUnregisterVolume instances.")
				err = watcher(ctx, cnsOperator)
				if err != nil {
					log.Error("Failed to watch on config file for changes to "+
						"CnsRegisterVolumesCleanupIntervalInMin. Error: %+v", err)
					return err
				}
				go func() {
					for {
						ctx, log = logger.GetNewContextWithLogger()
						log.Infof("Triggering CnsUnregisterVolume cleanup routine")
						cleanUpCnsUnregisterVolumeInstances(ctx, restConfig,
							cnsOperator.configInfo.Cfg.Global.CnsRegisterVolumesCleanupIntervalInMin)
						log.Infof("Completed CnsUnregisterVolume cleanup")
						for i := 1; i <= cnsOperator.configInfo.Cfg.Global.CnsRegisterVolumesCleanupIntervalInMin; i++ {
							time.Sleep(time.Duration(1 * time.Minute))
						}
					}
				}()
			}
		}

		if !stretchedSupervisor || (stretchedSupervisor && syncer.IsWorkloadDomainIsolationSupported) {
			if cnsOperator.coCommonInterface.IsFSSEnabled(ctx, common.FileVolume) {
				// Create CnsFileAccessConfig CRD from manifest if file volume feature
				// is enabled.
				err = k8s.CreateCustomResourceDefinitionFromManifest(ctx, cnsoperatorconfig.EmbedCnsFileAccessConfigCRFile,
					cnsoperatorconfig.EmbedCnsFileAccessConfigCRFileName)
				if err != nil {
					log.Errorf("Failed to create %q CRD. Err: %+v", cnsoperatorv1alpha1.CnsFileAccessConfigPlural, err)
					return err
				}
				// Create FileVolumeClients CRD from manifest if file volume feature
				// is enabled.
				err = k8s.CreateCustomResourceDefinitionFromManifest(ctx,
					internalapiscnsoperatorconfig.EmbedCnsFileVolumeClientFile,
					internalapiscnsoperatorconfig.EmbedCnsFileVolumeClientFileName)
				if err != nil {
					log.Errorf("Failed to create %q CRD. Err: %+v", internalapis.CnsFileVolumeClientPlural, err)
					return err
				}
			}
		}
	} else if clusterFlavor == cnstypes.CnsClusterFlavorVanilla {
		// Create CSINodeTopology CRD.
		err = k8s.CreateCustomResourceDefinitionFromManifest(ctx, csinodetopologyconfig.EmbedCSINodeTopologyFile,
			csinodetopologyconfig.EmbedCSINodeTopologyFileName)
		if err != nil {
			log.Errorf("Failed to create %q CRD. Error: %+v", csinodetopology.CRDSingular, err)
			return err
		}
	} else if clusterFlavor == cnstypes.CnsClusterFlavorGuest {
		if cnsOperator.coCommonInterface.IsFSSEnabled(ctx, common.TKGsHA) {
			// Create CSINodeTopology CRD.
			err = k8s.CreateCustomResourceDefinitionFromManifest(ctx, csinodetopologyconfig.EmbedCSINodeTopologyFile,
				csinodetopologyconfig.EmbedCSINodeTopologyFileName)
			if err != nil {
				log.Errorf("Failed to create %q CRD. Error: %+v", csinodetopology.CRDSingular, err)
				return err
			}
		}
	}

	// Create a new operator to provide shared dependencies and start components
	// Setting namespace to empty would let operator watch all namespaces.
	mgr, err := manager.New(restConfig, manager.Options{
		Metrics: metricsserver.Options{
			BindAddress: fmt.Sprintf("%s:%d", metricsHost, metricsPort),
		},
	})
	if err != nil {
		log.Errorf("failed to create new Cns operator instance. Err: %+v", err)
		return err
	}

	log.Info("Registering Components for Cns Operator")

	// Setup Scheme for all resources for external APIs.
	if err := cnsoperatorv1alpha1.AddToScheme(mgr.GetScheme()); err != nil {
		log.Errorf("failed to set the scheme for Cns operator. Err: %+v", err)
		return err
	}
	if err = csinodetopologyv1alpha1.AddToScheme(mgr.GetScheme()); err != nil {
		log.Errorf("failed to add CSINodeTopology to scheme with error: %+v", err)
		return err
	}
	if err = internalapis.AddToScheme(mgr.GetScheme()); err != nil {
		log.Errorf("failed to add internalapis to scheme with error: %+v", err)
		return err
	}
	if err := wcpcapapis.AddToScheme(mgr.GetScheme()); err != nil {
		log.Errorf("failed to set the scheme for Cns operator. Err: %+v", err)
		return err
	}
	if err := vmoperatortypes.AddToScheme(mgr.GetScheme()); err != nil {
		log.Errorf("failed to set the scheme for vm operator. Err: %+v", err)
		return err
	}

	// Setup all Controllers.
	if err := controller.AddToManager(mgr, clusterFlavor, cnsOperator.configInfo, volumeManager); err != nil {
		log.Errorf("failed to setup the controller for Cns operator. Err: %+v", err)
		return err
	}

	log.Info("Starting Cns Operator")

	// Start the operator.
	if err := mgr.Start(ctx); err != nil {
		log.Errorf("failed to start Cns operator. Err: %+v", err)
		return err
	}
	return nil
}

// InitCommonModules initializes the common modules for all flavors.
func InitCommonModules(ctx context.Context, clusterFlavor cnstypes.CnsClusterFlavor,
	coInitParams *interface{}) error {
	ctx = logger.NewContextWithLogger(ctx)
	log := logger.GetLogger(ctx)
	var err error
	if clusterFlavor == cnstypes.CnsClusterFlavorWorkload {
		commonco.ContainerOrchestratorUtility, err = commonco.GetContainerOrchestratorInterface(ctx,
			common.Kubernetes, cnstypes.CnsClusterFlavorWorkload, *coInitParams)
	} else if clusterFlavor == cnstypes.CnsClusterFlavorVanilla {
		commonco.ContainerOrchestratorUtility, err = commonco.GetContainerOrchestratorInterface(ctx,
			common.Kubernetes, cnstypes.CnsClusterFlavorVanilla, *coInitParams)
	} else if clusterFlavor == cnstypes.CnsClusterFlavorGuest {
		commonco.ContainerOrchestratorUtility, err = commonco.GetContainerOrchestratorInterface(ctx,
			common.Kubernetes, cnstypes.CnsClusterFlavorGuest, *coInitParams)
	}
	if err != nil {
		log.Errorf("failed to create CO agnostic interface. Err: %v", err)
		return err
	}

	if commonco.ContainerOrchestratorUtility.IsFSSEnabled(ctx, common.TriggerCsiFullSync) {
		log.Infof("Triggerfullsync feature enabled")
		err := k8s.CreateCustomResourceDefinitionFromManifest(ctx, internalapiscnsoperatorconfig.EmbedTriggerCsiFullSync,
			internalapiscnsoperatorconfig.EmbedTriggerCsiFullSyncName)
		if err != nil {
			log.Errorf("Failed to create %q CRD. Err: %+v", internalapis.TriggerCsiFullSyncPlural, err)
			return err
		}
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
		// Check if TriggerCsiFullSync instance is present. If not present,
		// create the TriggerCsiFullSync instance with name "csifullsync".
		// If present, update the TriggerCsiFullSync.Status.InProgress to false if
		// a full sync is already running.
		triggerCsiFullSyncInstance := &triggercsifullsyncv1alpha1.TriggerCsiFullSync{}
		key := k8stypes.NamespacedName{Namespace: "", Name: common.TriggerCsiFullSyncCRName}
		if err := cnsOperatorClient.Get(ctx, key, triggerCsiFullSyncInstance); err != nil {
			if apierrors.IsNotFound(err) {
				newtriggerCsiFullSyncInstance := triggercsifullsyncv1alpha1.CreateTriggerCsiFullSyncInstance()
				if err := cnsOperatorClient.Create(ctx, newtriggerCsiFullSyncInstance); err != nil {
					log.Errorf("Failed to create TriggerCsiFullSync instance: %q. Error: %v",
						common.TriggerCsiFullSyncCRName, err)
					return err
				}
				log.Infof("Created the a new instance of %q TriggerCsiFullSync instance as it was not found.",
					common.TriggerCsiFullSyncCRName)
			} else {
				log.Errorf("Failed to get TriggerCsiFullSync instance: %q. Error: %v",
					common.TriggerCsiFullSyncCRName, err)
				return err
			}
		}
		if triggerCsiFullSyncInstance.Status.InProgress {
			log.Infof("Found %q instance with InProgress set to true on syncer startup. "+
				"Resetting InProgress field to false as no full sync is currently running",
				common.TriggerCsiFullSyncCRName)
			triggerCsiFullSyncInstance.Status.InProgress = false
			if err := cnsOperatorClient.Update(ctx, triggerCsiFullSyncInstance); err != nil {
				log.Errorf("Failed to update TriggerCsiFullSync instance: %q with Status.InProgress set to false. "+
					"Error: %v", common.TriggerCsiFullSyncCRName, err)
				return err
			}
		}
	}
	return nil
}

// watcher watches on the vsphere.conf file mounted as secret within the syncer
// container.
func watcher(ctx context.Context, cnsOperator *cnsOperatorInfo) error {
	log := logger.GetLogger(ctx)
	cfgPath := commonconfig.GetConfigPath(ctx)
	watcher, err := fsnotify.NewWatcher()
	if err != nil {
		log.Errorf("Failed to create fsnotify watcher. Err: %+v", err)
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
					log.Infof("Reloading Configuration")
					err := reloadConfiguration(ctx, cnsOperator)
					if err != nil {
						log.Infof("Failed to reload configuration from: %q. Err: %+v", cfgPath, err)
					} else {
						log.Infof("Successfully reloaded configuration from: %q", cfgPath)
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
		log.Errorf("Failed to watch on path: %q. err=%v", cfgDirPath, err)
	}
	return err
}

// reloadConfiguration reloads configuration from the secret, and cnsOperatorInfo
// with the latest configInfo.
func reloadConfiguration(ctx context.Context, cnsOperator *cnsOperatorInfo) error {
	log := logger.GetLogger(ctx)
	cfg, err := commonconfig.GetConfig(ctx)
	if err != nil {
		log.Errorf("Failed to read config. Error: %+v", err)
		return err
	}
	cnsOperator.configInfo = &commonconfig.ConfigurationInfo{Cfg: cfg}
	log.Infof("Reloaded the value for CnsRegisterVolumesCleanupIntervalInMin to %d",
		cnsOperator.configInfo.Cfg.Global.CnsRegisterVolumesCleanupIntervalInMin)
	return nil
}
