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
	apiextensionsv1beta1 "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions/v1beta1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	k8stypes "k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client/config"
	"sigs.k8s.io/controller-runtime/pkg/manager"
	"sigs.k8s.io/controller-runtime/pkg/manager/signals"

	cnsoperatorv1alpha1 "sigs.k8s.io/vsphere-csi-driver/v2/pkg/apis/cnsoperator"
	cnsnodevmattachmentv1alpha1 "sigs.k8s.io/vsphere-csi-driver/v2/pkg/apis/cnsoperator/cnsnodevmattachment/v1alpha1"
	cnsvolumemetadatav1alpha1 "sigs.k8s.io/vsphere-csi-driver/v2/pkg/apis/cnsoperator/cnsvolumemetadata/v1alpha1"
	"sigs.k8s.io/vsphere-csi-driver/v2/pkg/common/cns-lib/node"
	volumes "sigs.k8s.io/vsphere-csi-driver/v2/pkg/common/cns-lib/volume"
	cnsvsphere "sigs.k8s.io/vsphere-csi-driver/v2/pkg/common/cns-lib/vsphere"
	commonconfig "sigs.k8s.io/vsphere-csi-driver/v2/pkg/common/config"
	"sigs.k8s.io/vsphere-csi-driver/v2/pkg/csi/service/common"
	"sigs.k8s.io/vsphere-csi-driver/v2/pkg/csi/service/common/commonco"
	"sigs.k8s.io/vsphere-csi-driver/v2/pkg/csi/service/logger"
	"sigs.k8s.io/vsphere-csi-driver/v2/pkg/internalapis"
	triggercsifullsyncv1alpha1 "sigs.k8s.io/vsphere-csi-driver/v2/pkg/internalapis/cnsoperator/triggercsifullsync/v1alpha1"
	"sigs.k8s.io/vsphere-csi-driver/v2/pkg/internalapis/csinodetopology"
	csinodetopologyv1alpha1 "sigs.k8s.io/vsphere-csi-driver/v2/pkg/internalapis/csinodetopology/v1alpha1"
	k8s "sigs.k8s.io/vsphere-csi-driver/v2/pkg/kubernetes"
	"sigs.k8s.io/vsphere-csi-driver/v2/pkg/syncer/cnsoperator/controller"
)

var (
	// Use localhost and port for metrics
	metricsHost       = "0.0.0.0"
	metricsPort int32 = 8383
)

type cnsOperator struct {
	configInfo        *commonconfig.ConfigurationInfo
	coCommonInterface commonco.COCommonInterface
}

// InitCnsOperator initializes the Cns Operator.
func InitCnsOperator(ctx context.Context, clusterFlavor cnstypes.CnsClusterFlavor,
	configInfo *commonconfig.ConfigurationInfo, coInitParams *interface{}) error {
	log := logger.GetLogger(ctx)
	log.Infof("Initializing CNS Operator")
	cnsOperator := &cnsOperator{}
	cnsOperator.configInfo = configInfo

	var volumeManager volumes.Manager
	if clusterFlavor == cnstypes.CnsClusterFlavorWorkload || clusterFlavor == cnstypes.CnsClusterFlavorVanilla {
		vCenter, err := cnsvsphere.GetVirtualCenterInstance(ctx, cnsOperator.configInfo, false)
		if err != nil {
			return err
		}
		volumeManager = volumes.GetManager(ctx, vCenter, nil, false)
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
		// Create CnsNodeVmAttachment CRD.
		crdKindNodeVMAttachment := reflect.TypeOf(cnsnodevmattachmentv1alpha1.CnsNodeVmAttachment{}).Name()
		crdNameNodeVMAttachment := cnsoperatorv1alpha1.CnsNodeVMAttachmentPlural + "." +
			cnsoperatorv1alpha1.SchemeGroupVersion.Group
		err = k8s.CreateCustomResourceDefinitionFromSpec(ctx, crdNameNodeVMAttachment,
			cnsoperatorv1alpha1.CnsNodeVMAttachmentSingular, cnsoperatorv1alpha1.CnsNodeVMAttachmentPlural,
			crdKindNodeVMAttachment, cnsoperatorv1alpha1.SchemeGroupVersion.Group,
			cnsoperatorv1alpha1.SchemeGroupVersion.Version, apiextensionsv1beta1.NamespaceScoped)
		if err != nil {
			log.Errorf("failed to create %q CRD. Err: %+v", crdNameNodeVMAttachment, err)
			return err
		}

		// Create CnsVolumeMetadata CRD.
		crdKindVolumeMetadata := reflect.TypeOf(cnsvolumemetadatav1alpha1.CnsVolumeMetadata{}).Name()
		crdNameVolumeMetadata := cnsoperatorv1alpha1.CnsVolumeMetadataPlural + "." +
			cnsoperatorv1alpha1.SchemeGroupVersion.Group

		err = k8s.CreateCustomResourceDefinitionFromSpec(ctx, crdNameVolumeMetadata,
			cnsoperatorv1alpha1.CnsVolumeMetadataSingular, cnsoperatorv1alpha1.CnsVolumeMetadataPlural,
			crdKindVolumeMetadata, cnsoperatorv1alpha1.SchemeGroupVersion.Group,
			cnsoperatorv1alpha1.SchemeGroupVersion.Version, apiextensionsv1beta1.NamespaceScoped)
		if err != nil {
			log.Errorf("failed to create %q CRD. Err: %+v", crdKindVolumeMetadata, err)
			return err
		}

		// Create CnsRegisterVolume CRD from manifest.
		err = k8s.CreateCustomResourceDefinitionFromManifest(ctx, "cnsregistervolume_crd.yaml")
		if err != nil {
			log.Errorf("Failed to create %q CRD. Err: %+v", cnsoperatorv1alpha1.CnsRegisterVolumePlural, err)
			return err
		}

		if cnsOperator.coCommonInterface.IsFSSEnabled(ctx, common.FileVolume) {
			// Create CnsFileAccessConfig CRD from manifest if file volume feature
			// is enabled.
			err = k8s.CreateCustomResourceDefinitionFromManifest(ctx, "cnsfileaccessconfig_crd.yaml")
			if err != nil {
				log.Errorf("Failed to create %q CRD. Err: %+v", cnsoperatorv1alpha1.CnsFileAccessConfigPlural, err)
				return err
			}
			// Create FileVolumeClients CRD from manifest if file volume feature
			// is enabled.
			err = k8s.CreateCustomResourceDefinitionFromManifest(ctx, "cnsfilevolumeclient_crd.yaml")
			if err != nil {
				log.Errorf("Failed to create %q CRD. Err: %+v", internalapis.CnsFileVolumeClientPlural, err)
				return err
			}
		}

		// Clean up routine to cleanup successful CnsRegisterVolume instances.
		err = watcher(ctx, cnsOperator)
		if err != nil {
			log.Error("Failed to watch on config file for changes to CnsRegisterVolumesCleanupIntervalInMin. Error: %+v",
				err)
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
	} else if clusterFlavor == cnstypes.CnsClusterFlavorVanilla {
		if cnsOperator.coCommonInterface.IsFSSEnabled(ctx, common.ImprovedVolumeTopology) {
			// Create CSINodeTopology CRD.
			csiNodeTopologyCRDName := csinodetopology.CRDPlural + "." + csinodetopologyv1alpha1.GroupName
			err := k8s.CreateCustomResourceDefinitionFromSpec(ctx, csiNodeTopologyCRDName, csinodetopology.CRDSingular,
				csinodetopology.CRDPlural, reflect.TypeOf(csinodetopologyv1alpha1.CSINodeTopology{}).Name(),
				csinodetopologyv1alpha1.GroupName, csinodetopologyv1alpha1.Version, apiextensionsv1beta1.ClusterScoped)
			if err != nil {
				log.Errorf("Failed to create %q CRD. Error: %+v", csinodetopology.CRDSingular, err)
				return err
			}
			// Initialize node manager so that CSINodeTopology controller can
			// retrieve NodeVM using the NodeID in the spec.
			nodeMgr := &node.Nodes{}
			err = nodeMgr.Initialize(ctx)
			if err != nil {
				log.Errorf("failed to initialize nodeManager. Error: %+v", err)
				return err
			}
		}
	}

	// Create a new operator to provide shared dependencies and start components
	// Setting namespace to empty would let operator watch all namespaces.
	mgr, err := manager.New(restConfig, manager.Options{
		Namespace:          "",
		MetricsBindAddress: fmt.Sprintf("%s:%d", metricsHost, metricsPort),
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

	// Setup all Controllers.
	if err := controller.AddToManager(mgr, clusterFlavor, cnsOperator.configInfo, volumeManager); err != nil {
		log.Errorf("failed to setup the controller for Cns operator. Err: %+v", err)
		return err
	}

	log.Info("Starting Cns Operator")

	// Start the operator.
	if err := mgr.Start(signals.SetupSignalHandler()); err != nil {
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
	var coCommonInterface commonco.COCommonInterface
	var err error
	if clusterFlavor == cnstypes.CnsClusterFlavorWorkload {
		coCommonInterface, err = commonco.GetContainerOrchestratorInterface(ctx,
			common.Kubernetes, cnstypes.CnsClusterFlavorWorkload, *coInitParams)
	} else if clusterFlavor == cnstypes.CnsClusterFlavorVanilla {
		coCommonInterface, err = commonco.GetContainerOrchestratorInterface(ctx,
			common.Kubernetes, cnstypes.CnsClusterFlavorVanilla, *coInitParams)
	} else if clusterFlavor == cnstypes.CnsClusterFlavorGuest {
		coCommonInterface, err = commonco.GetContainerOrchestratorInterface(ctx,
			common.Kubernetes, cnstypes.CnsClusterFlavorGuest, *coInitParams)
	}
	if err != nil {
		log.Errorf("failed to create CO agnostic interface. Err: %v", err)
		return err
	}
	if coCommonInterface.IsFSSEnabled(ctx, common.TriggerCsiFullSync) {
		log.Infof("Triggerfullsync feature enabled")
		err := k8s.CreateCustomResourceDefinitionFromManifest(ctx, "triggercsifullsync_crd.yaml")
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
func watcher(ctx context.Context, cnsOperator *cnsOperator) error {
	log := logger.GetLogger(ctx)
	cfgPath := common.GetConfigPath(ctx)
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

// reloadConfiguration reloads configuration from the secret, and cnsOperator
// with the latest configInfo.
func reloadConfiguration(ctx context.Context, cnsOperator *cnsOperator) error {
	log := logger.GetLogger(ctx)
	cfg, err := common.GetConfig(ctx)
	if err != nil {
		log.Errorf("Failed to read config. Error: %+v", err)
		return err
	}
	cnsOperator.configInfo = &commonconfig.ConfigurationInfo{Cfg: cfg}
	log.Infof("Reloaded the value for CnsRegisterVolumesCleanupIntervalInMin to %d",
		cnsOperator.configInfo.Cfg.Global.CnsRegisterVolumesCleanupIntervalInMin)
	return nil
}
