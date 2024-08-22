/*
Copyright 2020 The Kubernetes Authors.

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

package storagepool

import (
	"context"
	"reflect"
	"sync"
	"time"

	"sigs.k8s.io/controller-runtime/pkg/client/config"

	storagepoolconfig "sigs.k8s.io/vsphere-csi-driver/v3/pkg/apis/storagepool/config"

	spv1alpha1 "sigs.k8s.io/vsphere-csi-driver/v3/pkg/apis/storagepool/cns/v1alpha1"
	cnsvsphere "sigs.k8s.io/vsphere-csi-driver/v3/pkg/common/cns-lib/vsphere"
	commonconfig "sigs.k8s.io/vsphere-csi-driver/v3/pkg/common/config"
	"sigs.k8s.io/vsphere-csi-driver/v3/pkg/csi/service/common"
	"sigs.k8s.io/vsphere-csi-driver/v3/pkg/csi/service/common/commonco"
	"sigs.k8s.io/vsphere-csi-driver/v3/pkg/csi/service/logger"
	k8s "sigs.k8s.io/vsphere-csi-driver/v3/pkg/kubernetes"
)

// Service holds the controllers needed to manage StoragePools.
type Service struct {
	spController   *SpController
	scWatchCntlr   *StorageClassWatch
	migrationCntlr *migrationController
	clusterIDs     []string
}

var (
	defaultStoragePoolService     = new(Service)
	defaultStoragePoolServiceLock sync.Mutex
)

// InitStoragePoolService initializes the StoragePool service that updates
// vSphere Datastore information into corresponding k8s StoragePool resources.
// TODO: handle the scenario when there's a change in the availability zones
func InitStoragePoolService(ctx context.Context,
	configInfo *commonconfig.ConfigurationInfo, coInitParams *interface{}) error {
	log := logger.GetLogger(ctx)
	clusterIDs := []string{configInfo.Cfg.Global.ClusterID}

	if commonco.ContainerOrchestratorUtility.IsFSSEnabled(ctx, common.TKGsHA) {
		clusterComputeResourceMoIds, err := common.GetClusterComputeResourceMoIds(ctx)
		if err != nil {
			log.Errorf("failed to get clusterComputeResourceMoIds. err: %v", err)
			return err
		}

		clusterIDs = clusterComputeResourceMoIds
		if len(clusterIDs) > 1 &&
			(!commonco.ContainerOrchestratorUtility.IsFSSEnabled(ctx, common.PodVMOnStretchedSupervisor) ||
				!commonco.ContainerOrchestratorUtility.IsFSSEnabled(ctx, common.VdppOnStretchedSupervisor)) {
			log.Infof("`%s` and `%s` should be enabled for storage pool service on Stretched Supervisor. Exiting...",
				common.PodVMOnStretchedSupervisor, common.VdppOnStretchedSupervisor)
			return nil
		}
	}

	log.Infof("Initializing Storage Pool Service")

	// Create StoragePool CRD.
	err := k8s.CreateCustomResourceDefinitionFromManifest(ctx, storagepoolconfig.EmbedStoragePoolCRFile,
		storagepoolconfig.EmbedStoragePoolCRFileName)
	if err != nil {
		crdKind := reflect.TypeOf(spv1alpha1.StoragePool{}).Name()
		log.Errorf("Failed to create %q CRD. Err: %+v", crdKind, err)
		return err
	}

	// Get VC connection.
	vc, err := cnsvsphere.GetVirtualCenterInstance(ctx, configInfo, false)
	if err != nil {
		log.Errorf("Failed to get vCenter from vSphereSecretConfigInfo. Err: %+v", err)
		return err
	}

	err = vc.ConnectPbm(ctx)
	if err != nil {
		log.Errorf("Failed to connect to SPBM service. Err: %+v", err)
		return err
	}

	// Start the services.
	spController, err := newSPController(vc, clusterIDs)
	if err != nil {
		log.Errorf("Failed starting StoragePool controller. Err: %+v", err)
		return err
	}

	// Get a config to talk to the apiserver.
	cfg, err := config.GetConfig()
	if err != nil {
		log.Errorf("Failed to get Kubernetes config. Err: %+v", err)
		return err
	}

	scWatchCntlr, err := startStorageClassWatch(ctx, spController, cfg)
	if err != nil {
		log.Errorf("Failed starting the Storageclass watch. Err: %+v", err)
		return err
	}

	// Trigger NodeAnnotationListener in StoragePool.
	go func() {
		// Create the kubernetes client from config.
		k8sClient, err := k8s.NewClient(ctx)
		if err != nil {
			log.Errorf("Creating Kubernetes client failed. Err: %v", err)
			return
		}
		k8sInformerManager := k8s.NewInformer(ctx, k8sClient, true)
		err = InitNodeAnnotationListener(ctx, k8sInformerManager, scWatchCntlr, spController)
		if err != nil {
			log.Errorf("InitNodeAnnotationListener failed. err: %v", err)
		}
	}()

	migrationController := initMigrationController(vc, clusterIDs)
	go func() {
		diskDecommEnablementTicker := time.NewTicker(common.DefaultFeatureEnablementCheckInterval)
		defer diskDecommEnablementTicker.Stop()
		for ; true; <-diskDecommEnablementTicker.C {
			_, err := initDiskDecommController(ctx, migrationController)
			if err != nil {
				log.Warnf("Error while initializing disk decommission controller. Error: %+v. "+
					"Retry will be triggered at %v",
					err, time.Now().Add(common.DefaultFeatureEnablementCheckInterval))
				continue
			}
			break
		}
	}()

	storagePoolService := new(Service)
	storagePoolService.spController = spController
	storagePoolService.scWatchCntlr = scWatchCntlr
	storagePoolService.migrationCntlr = migrationController
	storagePoolService.clusterIDs = clusterIDs

	// Create the default Service.
	defaultStoragePoolServiceLock.Lock()
	defaultStoragePoolService = storagePoolService
	defaultStoragePoolServiceLock.Unlock()

	startPropertyCollectorListener(ctx)

	log.Infof("Done initializing Storage Pool Service")
	return nil
}

// GetScWatch returns the active StorageClassWatch initialized in this service.
func (sps *Service) GetScWatch() *StorageClassWatch {
	return sps.scWatchCntlr
}

// GetSPController returns the single SpController intialized in this service.
func (sps *Service) GetSPController() *SpController {
	return sps.spController
}

// ResetVC will be called whenever the connection to vCenter is recycled. This
// will renew the PropertyCollector listener of StoragePool as well as update
// the controllers with the new refreshed VC connection.
func ResetVC(ctx context.Context, vc *cnsvsphere.VirtualCenter) {
	log := logger.GetLogger(ctx)
	if vc == nil {
		log.Errorf("VirtualCenter not given to Reset")
		return
	}
	err := vc.ConnectPbm(ctx)
	if err != nil {
		log.Errorf("Failed to connect to SPBM service. Err: %+v", err)
		return
	}
	// TODO remove code to add version to CNS API, once CNS releases the next version.
	if commonco.ContainerOrchestratorUtility.IsFSSEnabled(ctx, common.StorageQuotaM2) {
		cnsDevVersion := "dev.version"
		log.Infof("use version %s for vCenter client", cnsDevVersion)
		if vc.Client != nil {
			log.Infof("Setting version %s to vCenter: %q vim25 client",
				cnsDevVersion, vc.Config.Host)
			vc.Client.Version = cnsDevVersion
		}
		if vc.CnsClient != nil {
			log.Infof("Setting version %s to vCenter: %q cns client",
				cnsDevVersion, vc.Config.Host)
			vc.CnsClient.Version = cnsDevVersion
			vc.CnsClient.Client.Version = cnsDevVersion
		}
		log.Infof("using CNS API version %s for vCenters %q client ",
			cnsDevVersion, vc.Config.Host)
	}
	log.Info("Resetting VC connection in StoragePool service")
	defaultStoragePoolServiceLock.Lock()
	defer defaultStoragePoolServiceLock.Unlock()
	resetspControllerVC := false
	resetscwatchControllerVC := false
	resetmigrationControllerVC := false
	if defaultStoragePoolService.spController != nil {
		defaultStoragePoolService.spController.vc = vc
		resetspControllerVC = true
	} else {
		log.Info("StoragePool service controller is not yet initialized. " +
			"Skip resetting new VC connection to spController.")
	}
	if defaultStoragePoolService.scWatchCntlr != nil {
		defaultStoragePoolService.scWatchCntlr.vc = vc
		resetscwatchControllerVC = true
	} else {
		log.Info("StoragePool service watch controller is not yet initialized. " +
			"Skip resetting new VC connection to scWatchCntlr.")
	}
	if defaultStoragePoolService.migrationCntlr != nil {
		defaultStoragePoolService.migrationCntlr.vc = vc
		resetmigrationControllerVC = true
	} else {
		log.Info("StoragePool service migration controller is not yet initialized. " +
			"Skip resetting new VC connection to migrationCntlr.")
	}
	if resetspControllerVC && resetscwatchControllerVC && resetmigrationControllerVC {
		// PC listener will automatically reestablish its session with VC.
		log.Info("Successfully reset VC connection in StoragePool service")
	}
}
