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

package vanilla

import (
	"context"
	"fmt"
	"math/rand"
	"net/http"
	"path/filepath"
	"strings"
	"time"

	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/container-storage-interface/spec/lib/go/csi"
	"github.com/fsnotify/fsnotify"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	cnstypes "github.com/vmware/govmomi/cns/types"
	"github.com/vmware/govmomi/units"
	"github.com/vmware/govmomi/vapi/tags"
	"google.golang.org/grpc/codes"

	"github.com/vmware/govmomi/vim25/types"
	"sigs.k8s.io/vsphere-csi-driver/v2/pkg/apis/migration"
	"sigs.k8s.io/vsphere-csi-driver/v2/pkg/common/cns-lib/node"
	cnsvolume "sigs.k8s.io/vsphere-csi-driver/v2/pkg/common/cns-lib/volume"
	cnsvsphere "sigs.k8s.io/vsphere-csi-driver/v2/pkg/common/cns-lib/vsphere"
	cnsconfig "sigs.k8s.io/vsphere-csi-driver/v2/pkg/common/config"
	"sigs.k8s.io/vsphere-csi-driver/v2/pkg/common/prometheus"
	"sigs.k8s.io/vsphere-csi-driver/v2/pkg/common/utils"
	"sigs.k8s.io/vsphere-csi-driver/v2/pkg/csi/service/common"
	"sigs.k8s.io/vsphere-csi-driver/v2/pkg/csi/service/common/commonco"
	commoncotypes "sigs.k8s.io/vsphere-csi-driver/v2/pkg/csi/service/common/commonco/types"
	"sigs.k8s.io/vsphere-csi-driver/v2/pkg/csi/service/logger"
	csitypes "sigs.k8s.io/vsphere-csi-driver/v2/pkg/csi/types"
	"sigs.k8s.io/vsphere-csi-driver/v2/pkg/internalapis/cnsvolumeoperationrequest"
)

// NodeManagerInterface provides functionality to manage (VM) nodes.
type NodeManagerInterface interface {
	Initialize(ctx context.Context) error
	GetSharedDatastoresInK8SCluster(ctx context.Context) ([]*cnsvsphere.DatastoreInfo, error)
	GetSharedDatastoresInTopology(ctx context.Context, topologyRequirement *csi.TopologyRequirement,
		tagManager *tags.Manager, zoneKey string, regionKey string) ([]*cnsvsphere.DatastoreInfo,
		map[string][]map[string]string, error)
	GetNodeByName(ctx context.Context, nodeName string) (*cnsvsphere.VirtualMachine, error)
	GetAllNodes(ctx context.Context) ([]*cnsvsphere.VirtualMachine, error)
}

type controller struct {
	manager     *common.Manager
	nodeMgr     NodeManagerInterface
	authMgr     common.AuthorizationService
	topologyMgr commoncotypes.ControllerTopologyService
}

// volumeMigrationService holds the pointer to VolumeMigration instance.
var volumeMigrationService migration.VolumeMigrationService

// New creates a CNS controller.
func New() csitypes.CnsController {
	return &controller{}
}

// Init is initializing controller struct.
func (c *controller) Init(config *cnsconfig.Config, version string) error {
	ctx, log := logger.GetNewContextWithLogger()
	log.Infof("Initializing CNS controller")
	var err error
	// Get VirtualCenterManager instance and validate version.
	vcenterconfig, err := cnsvsphere.GetVirtualCenterConfig(ctx, config)
	if err != nil {
		log.Errorf("failed to get VirtualCenterConfig. err=%v", err)
		return err
	}
	vcManager := cnsvsphere.GetVirtualCenterManager(ctx)
	vcenter, err := vcManager.RegisterVirtualCenter(ctx, vcenterconfig)
	if err != nil {
		log.Errorf("failed to register VC with virtualCenterManager. err=%v", err)
		return err
	}
	var operationStore cnsvolumeoperationrequest.VolumeOperationRequest
	idempotencyHandlingEnabled := commonco.ContainerOrchestratorUtility.IsFSSEnabled(ctx,
		common.CSIVolumeManagerIdempotency)
	if idempotencyHandlingEnabled {
		log.Info("CSI Volume manager idempotency handling feature flag is enabled.")
		operationStore, err = cnsvolumeoperationrequest.InitVolumeOperationRequestInterface(ctx,
			config.Global.CnsVolumeOperationRequestCleanupIntervalInMin)
		if err != nil {
			log.Errorf("failed to initialize VolumeOperationRequestInterface with error: %v", err)
			return err
		}
	}
	c.manager = &common.Manager{
		VcenterConfig:  vcenterconfig,
		CnsConfig:      config,
		VolumeManager:  cnsvolume.GetManager(ctx, vcenter, operationStore, idempotencyHandlingEnabled),
		VcenterManager: vcManager,
	}

	vc, err := common.GetVCenter(ctx, c.manager)
	if err != nil {
		log.Errorf("failed to get vcenter. err=%v", err)
		return err
	}

	isAuthCheckFSSEnabled := commonco.ContainerOrchestratorUtility.IsFSSEnabled(ctx, common.CSIAuthCheck)
	// Check if vSAN FS is enabled for TargetvSANFileShareDatastoreURLs only if
	// CSIAuthCheck FSS is not enabled.
	if !isAuthCheckFSSEnabled && len(c.manager.VcenterConfig.TargetvSANFileShareDatastoreURLs) > 0 {
		datacenters, err := vc.ListDatacenters(ctx)
		if err != nil {
			return logger.LogNewErrorf(log, "failed to find datacenters from VC: %q, Error: %+v", vc.Config.Host, err)
		}
		// Check if file service is enabled on datastore present in
		// targetvSANFileShareDatastoreURLs.
		dsToFileServiceEnabledMap, err := common.IsFileServiceEnabled(ctx,
			c.manager.VcenterConfig.TargetvSANFileShareDatastoreURLs, vc, datacenters)
		if err != nil {
			return logger.LogNewErrorf(log, "file service enablement check failed for datastore specified in "+
				"TargetvSANFileShareDatastoreURLs. err=%v", err)
		}
		for _, targetFSDatastore := range c.manager.VcenterConfig.TargetvSANFileShareDatastoreURLs {
			isFSEnabled := dsToFileServiceEnabledMap[targetFSDatastore]
			if !isFSEnabled {
				return logger.LogNewErrorf(log, "file service is not enabled on datastore %s specified in "+
					"TargetvSANFileShareDatastoreURLs", targetFSDatastore)
			}
		}
	}

	// Check vCenter API Version against 6.7.3.
	err = common.CheckAPI(vc.Client.ServiceContent.About.ApiVersion, common.MinSupportedVCenterMajor,
		common.MinSupportedVCenterMinor, common.MinSupportedVCenterPatch)
	if err != nil {
		log.Errorf("checkAPI failed for vcenter API version: %s, err=%v", vc.Client.ServiceContent.About.ApiVersion, err)
		return err
	}
	c.nodeMgr = &node.Nodes{}
	err = c.nodeMgr.Initialize(ctx)
	if err != nil {
		log.Errorf("failed to initialize nodeMgr. err=%v", err)
		return err
	}

	go cnsvolume.ClearTaskInfoObjects()
	cfgPath := common.GetConfigPath(ctx)

	if isAuthCheckFSSEnabled {
		log.Info("CSIAuthCheck feature is enabled, loading AuthorizationService")
		authMgr, err := common.GetAuthorizationService(ctx, vc)
		if err != nil {
			log.Errorf("failed to initialize authMgr. err=%v", err)
			return err
		}
		c.authMgr = authMgr
		go common.ComputeDatastoreMapForBlockVolumes(authMgr.(*common.AuthManager),
			config.Global.CSIAuthCheckIntervalInMin)
		isvSANFileServicesSupported, err := c.manager.VcenterManager.IsvSANFileServicesSupported(ctx,
			c.manager.VcenterConfig.Host)
		if err != nil {
			log.Errorf("failed to verify if vSAN file services is supported or not. Error:%+v", err)
			return err
		}
		if isvSANFileServicesSupported {
			go common.ComputeFSEnabledClustersToDsMap(authMgr.(*common.AuthManager),
				config.Global.CSIAuthCheckIntervalInMin)
		}
	}

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
						reloadConfigErr := c.ReloadConfiguration()
						if reloadConfigErr == nil {
							log.Infof("Successfully reloaded configuration from: %q", cfgPath)
							break
						}
						log.Errorf("failed to reload configuration. will retry again in 5 seconds. err: %+v", reloadConfigErr)
						time.Sleep(5 * time.Second)
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
	if commonco.ContainerOrchestratorUtility.IsFSSEnabled(ctx, common.CSIMigration) {
		log.Info("CSI Migration Feature is Enabled. Loading Volume Migration Service")
		volumeMigrationService, err = migration.GetVolumeMigrationService(ctx, &c.manager.VolumeManager, config, false)
		if err != nil {
			log.Errorf("failed to get migration service. Err: %v", err)
			return err
		}
	}
	// Create dynamic informer for CSINodeTopology instance if FSS is enabled.
	if commonco.ContainerOrchestratorUtility.IsFSSEnabled(ctx, common.ImprovedVolumeTopology) {
		// Initialize volume topology service.
		c.topologyMgr, err = commonco.ContainerOrchestratorUtility.InitTopologyServiceInController(ctx)
		if err != nil {
			log.Errorf("failed to initialize topology service. Error: %+v", err)
			return err
		}
	}
	// Go module to keep the metrics http server running all the time.
	go func() {
		prometheus.CsiInfo.WithLabelValues(version).Set(1)
		for {
			log.Info("Starting the http server to expose Prometheus metrics..")
			http.Handle("/metrics", promhttp.Handler())
			err = http.ListenAndServe(":2112", nil)
			if err != nil {
				log.Warnf("Http server that exposes the Prometheus exited with err: %+v", err)
			}
			log.Info("Restarting http server to expose Prometheus metrics..")
		}
	}()
	return nil
}

// ReloadConfiguration reloads configuration from the secret, and update
// controller's config cache and VolumeManager's VC Config cache.
func (c *controller) ReloadConfiguration() error {
	ctx, log := logger.GetNewContextWithLogger()
	log.Info("Reloading Configuration")
	cfg, err := common.GetConfig(ctx)
	if err != nil {
		return logger.LogNewErrorf(log, "failed to read config. Error: %+v", err)
	}
	newVCConfig, err := cnsvsphere.GetVirtualCenterConfig(ctx, cfg)
	if err != nil {
		log.Errorf("failed to get VirtualCenterConfig. err=%v", err)
		return err
	}
	if newVCConfig != nil {
		var vcenter *cnsvsphere.VirtualCenter
		if c.manager.VcenterConfig.Host != newVCConfig.Host ||
			c.manager.VcenterConfig.Username != newVCConfig.Username ||
			c.manager.VcenterConfig.Password != newVCConfig.Password {

			// Verify if new configuration has valid credentials by connecting to
			// vCenter. Proceed only if the connection succeeds, else return error.
			newVC := &cnsvsphere.VirtualCenter{Config: newVCConfig}
			if err = newVC.Connect(ctx); err != nil {
				return logger.LogNewErrorf(log, "failed to connect to VirtualCenter host: %q, Err: %+v",
					newVCConfig.Host, err)
			}

			// Reset vCenter singleton instance by passing reload flag as true.
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
		var operationStore cnsvolumeoperationrequest.VolumeOperationRequest
		idempotencyHandlingEnabled := commonco.ContainerOrchestratorUtility.IsFSSEnabled(ctx,
			common.CSIVolumeManagerIdempotency)
		if idempotencyHandlingEnabled {
			log.Info("CSI Volume manager idempotency handling feature flag is enabled.")
			operationStore, err = cnsvolumeoperationrequest.InitVolumeOperationRequestInterface(ctx,
				c.manager.CnsConfig.Global.CnsVolumeOperationRequestCleanupIntervalInMin)
			if err != nil {
				log.Errorf("failed to initialize VolumeOperationRequestInterface with error: %v", err)
				return err
			}
		}
		c.manager.VolumeManager.ResetManager(ctx, vcenter)
		c.manager.VcenterConfig = newVCConfig
		c.manager.VolumeManager = cnsvolume.GetManager(ctx, vcenter, operationStore, idempotencyHandlingEnabled)
		// Re-Initialize Node Manager to cache latest vCenter config.
		c.nodeMgr = &node.Nodes{}
		err = c.nodeMgr.Initialize(ctx)
		if err != nil {
			log.Errorf("failed to re-initialize nodeMgr. err=%v", err)
			return err
		}
		if c.authMgr != nil {
			c.authMgr.ResetvCenterInstance(ctx, vcenter)
			log.Debugf("Updated vCenter in auth manager")
		}
	}
	if cfg != nil {
		c.manager.CnsConfig = cfg
		log.Debugf("Updated manager.CnsConfig")
	}
	return nil
}

func (c *controller) filterDatastores(ctx context.Context,
	sharedDatastores []*cnsvsphere.DatastoreInfo) []*cnsvsphere.DatastoreInfo {
	log := logger.GetLogger(ctx)
	dsMap := c.authMgr.GetDatastoreMapForBlockVolumes(ctx)
	log.Debugf("filterDatastores: dsMap %v sharedDatastores %v", dsMap, sharedDatastores)
	var filteredDatastores []*cnsvsphere.DatastoreInfo
	for _, sharedDatastore := range sharedDatastores {
		if _, existsInDsMap := dsMap[sharedDatastore.Info.Url]; existsInDsMap {
			filteredDatastores = append(filteredDatastores, sharedDatastore)
		} else {
			log.Debugf("filter out datastore %v from create volume spec", sharedDatastore)
		}
	}
	log.Debugf("filterDatastores: filteredDatastores %v", filteredDatastores)
	return filteredDatastores
}

// createBlockVolume creates a block volume based on the CreateVolumeRequest.
func (c *controller) createBlockVolume(ctx context.Context, req *csi.CreateVolumeRequest) (
	*csi.CreateVolumeResponse, error) {
	log := logger.GetLogger(ctx)
	// Volume Size - Default is 10 GiB.
	volSizeBytes := int64(common.DefaultGbDiskSize * common.GbInBytes)
	if req.GetCapacityRange() != nil && req.GetCapacityRange().RequiredBytes != 0 {
		volSizeBytes = int64(req.GetCapacityRange().GetRequiredBytes())
	}
	volSizeMB := int64(common.RoundUpSize(volSizeBytes, common.MbInBytes))

	// Check if the feature state of block-volume-snapshot is enabled
	isBlockVolumeSnapshotEnabled := commonco.ContainerOrchestratorUtility.IsFSSEnabled(ctx, common.BlockVolumeSnapshot)
	// Check if requested volume size and source snapshot size matches
	volumeSource := req.GetVolumeContentSource()
	var contentSourceSnapshotID string
	if isBlockVolumeSnapshotEnabled && volumeSource != nil {
		sourceSnapshot := volumeSource.GetSnapshot()
		if sourceSnapshot == nil {
			return nil, logger.LogNewErrorCode(log, codes.InvalidArgument, "unsupported VolumeContentSource type")
		}
		contentSourceSnapshotID = sourceSnapshot.GetSnapshotId()

		cnsVolumeID, _, err := common.ParseCSISnapshotID(contentSourceSnapshotID)
		if err != nil {
			return nil, logger.LogNewErrorCode(log, codes.InvalidArgument, err.Error())
		}

		snapshotSizeInMB, _, err := utils.QueryCapacityAndDsUrlForVolumeUtil(
			ctx, c.manager.VolumeManager, cnsVolumeID, common.BlockVolumeType)
		if err != nil {
			return nil, err
		}

		snapshotSizeInBytes := snapshotSizeInMB * common.MbInBytes
		if volSizeBytes != snapshotSizeInBytes {
			return nil, logger.LogNewErrorCodef(log, codes.InvalidArgument,
				"size mismatches, requested volume size %d and source snapshot size %d",
				volSizeBytes, snapshotSizeInBytes)
		}
	}

	// Fetching the feature state for csi-migration before parsing storage class
	// params.
	csiMigrationFeatureState := commonco.ContainerOrchestratorUtility.IsFSSEnabled(ctx, common.CSIMigration)
	scParams, err := common.ParseStorageClassParams(ctx, req.Parameters, csiMigrationFeatureState)
	if err != nil {
		return nil, logger.LogNewErrorCodef(log, codes.InvalidArgument,
			"parsing storage class parameters failed with error: %+v", err)
	}

	if csiMigrationFeatureState && scParams.CSIMigration == "true" {
		if len(scParams.Datastore) != 0 {
			log.Infof("Converting datastore name: %q to Datastore URL", scParams.Datastore)
			// Get vCenter.
			vCenter, err := cnsvsphere.GetVirtualCenterManager(ctx).GetVirtualCenter(ctx, c.manager.VcenterConfig.Host)
			if err != nil {
				return nil, logger.LogNewErrorCodef(log, codes.Internal,
					"failed to get vCenter. err: %+v", err)
			}
			dcList, err := vCenter.GetDatacenters(ctx)
			if err != nil {
				return nil, logger.LogNewErrorCodef(log, codes.Internal,
					"failed to get datacenter list. err: %+v", err)
			}
			foundDatastoreURL := false
			for _, dc := range dcList {
				dsURLTodsInfoMap, err := dc.GetAllDatastores(ctx)
				if err != nil {
					return nil, logger.LogNewErrorCodef(log, codes.Internal,
						"failed to get dsURLTodsInfoMap. err: %+v", err)
				}
				for dsURL, dsInfo := range dsURLTodsInfoMap {
					if dsInfo.Info.Name == scParams.Datastore {
						scParams.DatastoreURL = dsURL
						log.Infof("Found datastoreURL: %q for datastore name: %q", scParams.DatastoreURL, scParams.Datastore)
						foundDatastoreURL = true
						break
					}
				}
				if foundDatastoreURL {
					break
				}
			}
			if !foundDatastoreURL {
				return nil, logger.LogNewErrorCodef(log, codes.Internal,
					"failed to find datastoreURL for datastore name: %q", scParams.Datastore)
			}
		}
	}
	var createVolumeSpec = common.CreateVolumeSpec{
		CapacityMB:              volSizeMB,
		Name:                    req.Name,
		ScParams:                scParams,
		VolumeType:              common.BlockVolumeType,
		ContentSourceSnapshotID: contentSourceSnapshotID,
	}

	var sharedDatastores []*cnsvsphere.DatastoreInfo
	var datastoreTopologyMap = make(map[string][]map[string]string)

	// Get accessibility.
	topologyRequirement := req.GetAccessibilityRequirements()
	if topologyRequirement != nil {
		if commonco.ContainerOrchestratorUtility.IsFSSEnabled(ctx, common.ImprovedVolumeTopology) {

			// Check if topology domains have been provided in the vSphere CSI config secret.
			// NOTE: We do not support kubernetes.io/hostname as a topology label.
			if c.manager.CnsConfig.Labels.TopologyCategories == "" && c.manager.CnsConfig.Labels.Zone == "" &&
				c.manager.CnsConfig.Labels.Region == "" {
				return nil, logger.LogNewErrorCode(log, codes.InvalidArgument,
					"topology category names not specified in the vsphere config secret")
			}
			// Get shared accessible datastores for matching topology requirement.
			sharedDatastores, datastoreTopologyMap, err = c.topologyMgr.GetSharedDatastoresInTopology(ctx,
				topologyRequirement)
			if err != nil || len(sharedDatastores) == 0 {
				return nil, logger.LogNewErrorCodef(log, codes.Internal,
					"failed to get shared datastores for topology requirement: %+v. Error: %+v",
					topologyRequirement, err)
			}
		} else {
			if c.manager.CnsConfig.Labels.Zone == "" || c.manager.CnsConfig.Labels.Region == "" {
				// If zone and region label (vSphere category names) not specified in
				// the config secret, then return NotFound error.
				return nil, logger.LogNewErrorCode(log, codes.Internal,
					"zone/region vsphere category names not specified in the vsphere config secret")
			}
			vcenter, err := c.manager.VcenterManager.GetVirtualCenter(ctx, c.manager.VcenterConfig.Host)
			if err != nil {
				return nil, logger.LogNewErrorCodef(log, codes.Internal,
					"failed to get vCenter. Err: %v", err)
			}
			tagManager, err := cnsvsphere.GetTagManager(ctx, vcenter)
			if err != nil {
				return nil, logger.LogNewErrorCodef(log, codes.Internal,
					"failed to get tagManager. Err: %v", err)
			}
			defer func() {
				err := tagManager.Logout(ctx)
				if err != nil {
					log.Errorf("failed to logout tagManager. err: %v", err)
				}
			}()
			sharedDatastores, datastoreTopologyMap, err = c.nodeMgr.GetSharedDatastoresInTopology(ctx,
				topologyRequirement, tagManager, c.manager.CnsConfig.Labels.Zone, c.manager.CnsConfig.Labels.Region)
			if err != nil || len(sharedDatastores) == 0 {
				return nil, logger.LogNewErrorCodef(log, codes.Internal,
					"failed to get shared datastores in topology: %+v. Error: %+v", topologyRequirement, err)
			}
		}
		log.Debugf("Shared datastores [%+v] retrieved for topologyRequirement [%+v] with "+
			"datastoreTopologyMap [+%v]", sharedDatastores, topologyRequirement, datastoreTopologyMap)
		if createVolumeSpec.ScParams.DatastoreURL != "" {
			// Check datastoreURL specified in the storageclass is accessible from
			// topology.
			isDataStoreAccessible := false
			for _, sharedDatastore := range sharedDatastores {
				if sharedDatastore.Info.Url == createVolumeSpec.ScParams.DatastoreURL {
					isDataStoreAccessible = true
					break
				}
			}
			if !isDataStoreAccessible {
				return nil, logger.LogNewErrorCodef(log, codes.InvalidArgument,
					"datastore URL: %s specified in the storage class is not accessible in the topology:[+%v]",
					createVolumeSpec.ScParams.DatastoreURL, topologyRequirement)
			}
		}

	} else {
		sharedDatastores, err = c.nodeMgr.GetSharedDatastoresInK8SCluster(ctx)
		if err != nil || len(sharedDatastores) == 0 {
			return nil, logger.LogNewErrorCodef(log, codes.Internal,
				"failed to get shared datastores in kubernetes cluster. Error: %+v", err)
		}
	}

	if commonco.ContainerOrchestratorUtility.IsFSSEnabled(ctx, common.CSIAuthCheck) {
		// Filter datastores which in datastoreMap from sharedDatastores.
		sharedDatastores = c.filterDatastores(ctx, sharedDatastores)
	}
	volumeInfo, err := common.CreateBlockVolumeUtil(ctx, cnstypes.CnsClusterFlavorVanilla,
		c.manager, &createVolumeSpec, sharedDatastores)
	if err != nil {
		return nil, logger.LogNewErrorCodef(log, codes.Internal,
			"failed to create volume. Error: %+v", err)
	}

	attributes := make(map[string]string)
	attributes[common.AttributeDiskType] = common.DiskTypeBlockVolume
	if csiMigrationFeatureState && scParams.CSIMigration == "true" {
		// In case if feature state switch is enabled after controller is
		// deployed, we need to initialize the volumeMigrationService.
		if err := initVolumeMigrationService(ctx, c); err != nil {
			// Error is already wrapped in CSI error code.
			return nil, err
		}
		// Return InitialVolumeFilepath in the response for TranslateCSIPVToInTree.
		volumePath, err := volumeMigrationService.GetVolumePath(ctx, volumeInfo.VolumeID.Id)
		if err != nil {
			return nil, logger.LogNewErrorCodef(log, codes.Internal,
				"failed to get volume path for volume id: %q. Error: %+v", volumeInfo.VolumeID.Id, err)
		}
		attributes[common.AttributeInitialVolumeFilepath] = volumePath
	}

	resp := &csi.CreateVolumeResponse{
		Volume: &csi.Volume{
			VolumeId:      volumeInfo.VolumeID.Id,
			CapacityBytes: int64(units.FileSize(volSizeMB * common.MbInBytes)),
			VolumeContext: attributes,
		},
	}

	// Retrieve the datastoreURL of the Provisioned Volume. If CNS CreateVolume
	// API does not return datastoreURL, retrieve this by calling QueryVolume.
	// Otherwise, retrieve this from PlacementResults from the response of
	// CreateVolume API.
	var volumeAccessibleTopology = make(map[string]string)
	var datastoreAccessibleTopology []map[string]string
	var datastoreURL string
	if len(datastoreTopologyMap) > 0 {
		if volumeInfo.DatastoreURL == "" {
			volumeIds := []cnstypes.CnsVolumeId{{Id: volumeInfo.VolumeID.Id}}
			queryFilter := cnstypes.CnsQueryFilter{
				VolumeIds: volumeIds,
			}
			queryResult, err := c.manager.VolumeManager.QueryVolume(ctx, queryFilter)
			if err != nil {
				return nil, logger.LogNewErrorCodef(log, codes.Internal,
					"queryVolume failed for volumeID: %s, err: %+v", volumeInfo.VolumeID.Id, err)
			}
			if len(queryResult.Volumes) > 0 {
				// Find datastore topology from the retrieved datastoreURL.
				if queryResult.Volumes[0].DatastoreUrl == "" {
					return nil, logger.LogNewErrorCodef(log, codes.Internal,
						"could not retrieve datastore of volume: %q", volumeInfo.VolumeID.Id)
				}
				datastoreAccessibleTopology = datastoreTopologyMap[queryResult.Volumes[0].DatastoreUrl]
				datastoreURL = queryResult.Volumes[0].DatastoreUrl
				log.Debugf("Volume: %s is provisioned on the datastore: %s ", volumeInfo.VolumeID.Id, datastoreURL)
			} else {
				return nil, logger.LogNewErrorCodef(log, codes.Internal,
					"queryVolume could not retrieve volume information for volume: %q", volumeInfo.VolumeID.Id)
			}
		} else {
			// Retrieve datastoreURL from placementResults.
			datastoreAccessibleTopology = datastoreTopologyMap[volumeInfo.DatastoreURL]
			datastoreURL = volumeInfo.DatastoreURL
			log.Debugf("Volume: %s is provisioned on the datastore: %s ", volumeInfo.VolumeID.Id, datastoreURL)
		}
		if len(datastoreAccessibleTopology) > 0 {
			rand.Seed(time.Now().Unix())
			volumeAccessibleTopology = datastoreAccessibleTopology[rand.Intn(len(datastoreAccessibleTopology))]
			log.Debugf("volumeAccessibleTopology: [%+v] is selected for datastore: %s ",
				volumeAccessibleTopology, datastoreURL)
		}
	}
	if len(volumeAccessibleTopology) != 0 {
		volumeTopology := &csi.Topology{
			Segments: volumeAccessibleTopology,
		}
		resp.Volume.AccessibleTopology = append(resp.Volume.AccessibleTopology, volumeTopology)
	}

	// Set the Snapshot VolumeContentSource in the CreateVolumeResponse
	if contentSourceSnapshotID != "" {
		resp.Volume.ContentSource = &csi.VolumeContentSource{
			Type: &csi.VolumeContentSource_Snapshot{
				Snapshot: &csi.VolumeContentSource_SnapshotSource{
					SnapshotId: contentSourceSnapshotID,
				},
			},
		}
	}

	return resp, nil
}

// createFileVolume creates a file volume based on the CreateVolumeRequest.
func (c *controller) createFileVolume(ctx context.Context, req *csi.CreateVolumeRequest) (
	*csi.CreateVolumeResponse, error) {
	log := logger.GetLogger(ctx)
	// Ignore TopologyRequirement for file volume provisioning.
	if req.GetAccessibilityRequirements() != nil {
		log.Info("Ignoring TopologyRequirement for file volume")
	}

	// Volume Size - Default is 10 GiB.
	volSizeBytes := int64(common.DefaultGbDiskSize * common.GbInBytes)
	if req.GetCapacityRange() != nil && req.GetCapacityRange().RequiredBytes != 0 {
		volSizeBytes = int64(req.GetCapacityRange().GetRequiredBytes())
	}
	volSizeMB := int64(common.RoundUpSize(volSizeBytes, common.MbInBytes))

	// Fetching the feature state for csi-migration before parsing storage class
	// params.
	csiMigrationFeatureState := commonco.ContainerOrchestratorUtility.IsFSSEnabled(ctx, common.CSIMigration)
	scParams, err := common.ParseStorageClassParams(ctx, req.Parameters, csiMigrationFeatureState)
	if err != nil {
		return nil, logger.LogNewErrorCodef(log, codes.InvalidArgument,
			"parsing storage class parameters failed with error: %+v", err)
	}

	var createVolumeSpec = common.CreateVolumeSpec{
		CapacityMB: volSizeMB,
		Name:       req.Name,
		ScParams:   scParams,
		VolumeType: common.FileVolumeType,
	}
	var volumeID string
	if commonco.ContainerOrchestratorUtility.IsFSSEnabled(ctx, common.CSIAuthCheck) {
		fsEnabledClusterToDsInfoMap := c.authMgr.GetFsEnabledClusterToDsMap(ctx)

		var filteredDatastores []*cnsvsphere.DatastoreInfo
		for _, datastores := range fsEnabledClusterToDsInfoMap {
			filteredDatastores = append(filteredDatastores, datastores...)
		}

		if len(filteredDatastores) == 0 {
			return nil, logger.LogNewErrorCode(log, codes.Internal,
				"no datastores found to create file volume")
		}
		volumeID, err = common.CreateFileVolumeUtil(ctx, cnstypes.CnsClusterFlavorVanilla,
			c.manager, &createVolumeSpec, filteredDatastores)
		if err != nil {
			return nil, logger.LogNewErrorCodef(log, codes.Internal,
				"failed to create volume. Error: %+v", err)
		}
	} else {
		volumeID, err = common.CreateFileVolumeUtilOld(ctx, cnstypes.CnsClusterFlavorVanilla,
			c.manager, &createVolumeSpec)
		if err != nil {
			return nil, logger.LogNewErrorCodef(log, codes.Internal,
				"failed to create volume. Error: %+v", err)
		}
	}

	attributes := make(map[string]string)
	attributes[common.AttributeDiskType] = common.DiskTypeFileVolume

	resp := &csi.CreateVolumeResponse{
		Volume: &csi.Volume{
			VolumeId:      volumeID,
			CapacityBytes: int64(units.FileSize(volSizeMB * common.MbInBytes)),
			VolumeContext: attributes,
		},
	}
	return resp, nil
}

// CreateVolume is creating CNS Volume using volume request specified in
// CreateVolumeRequest.
func (c *controller) CreateVolume(ctx context.Context, req *csi.CreateVolumeRequest) (
	*csi.CreateVolumeResponse, error) {
	start := time.Now()
	volumeType := prometheus.PrometheusUnknownVolumeType
	createVolumeInternal := func() (
		*csi.CreateVolumeResponse, error) {

		ctx = logger.NewContextWithLogger(ctx)
		log := logger.GetLogger(ctx)
		log.Infof("CreateVolume: called with args %+v", *req)
		volumeCapabilities := req.GetVolumeCapabilities()
		if err := common.IsValidVolumeCapabilities(ctx, volumeCapabilities); err != nil {
			return nil, logger.LogNewErrorCodef(log, codes.InvalidArgument,
				"volume capability not supported. Err: %+v", err)
		}
		if common.IsFileVolumeRequest(ctx, volumeCapabilities) {
			volumeType = prometheus.PrometheusFileVolumeType
			isvSANFileServicesSupported, err := c.manager.VcenterManager.IsvSANFileServicesSupported(ctx,
				c.manager.VcenterConfig.Host)
			if err != nil {
				return nil, logger.LogNewErrorCodef(log, codes.Internal,
					"failed to verify if vSAN file services is supported or not. Error:%+v", err)
			}
			if !isvSANFileServicesSupported {
				return nil, logger.LogNewErrorCode(log, codes.FailedPrecondition,
					"fileshare volume creation is not supported on vSAN 67u3 release")
			}
			return c.createFileVolume(ctx, req)
		}
		volumeType = prometheus.PrometheusBlockVolumeType
		return c.createBlockVolume(ctx, req)
	}
	resp, err := createVolumeInternal()
	if err != nil {
		prometheus.CsiControlOpsHistVec.WithLabelValues(volumeType, prometheus.PrometheusCreateVolumeOpType,
			prometheus.PrometheusFailStatus).Observe(time.Since(start).Seconds())
	} else {
		prometheus.CsiControlOpsHistVec.WithLabelValues(volumeType, prometheus.PrometheusCreateVolumeOpType,
			prometheus.PrometheusPassStatus).Observe(time.Since(start).Seconds())
	}
	return resp, err
}

// DeleteVolume is deleting CNS Volume specified in DeleteVolumeRequest.
func (c *controller) DeleteVolume(ctx context.Context, req *csi.DeleteVolumeRequest) (
	*csi.DeleteVolumeResponse, error) {
	start := time.Now()
	volumeType := prometheus.PrometheusUnknownVolumeType

	deleteVolumeInternal := func() (
		*csi.DeleteVolumeResponse, error) {
		ctx = logger.NewContextWithLogger(ctx)
		log := logger.GetLogger(ctx)
		log.Infof("DeleteVolume: called with args: %+v", *req)
		var err error
		err = validateVanillaDeleteVolumeRequest(ctx, req)
		if err != nil {
			return nil, err
		}
		var volumePath string
		if strings.Contains(req.VolumeId, ".vmdk") {
			volumeType = prometheus.PrometheusBlockVolumeType
			// In-tree volume support.
			if !commonco.ContainerOrchestratorUtility.IsFSSEnabled(ctx, common.CSIMigration) {
				// Migration feature switch is disabled.
				return nil, logger.LogNewErrorCodef(log, codes.Internal,
					"volume-migration feature switch is disabled. Cannot use volume with vmdk path :%q", req.VolumeId)
			}
			// Migration feature switch is enabled.
			volumePath = req.VolumeId
			// In case if feature state switch is enabled after controller is
			// deployed, we need to initialize the volumeMigrationService.
			if err := initVolumeMigrationService(ctx, c); err != nil {
				// Error is already wrapped in CSI error code.
				return nil, err
			}
			req.VolumeId, err = volumeMigrationService.GetVolumeID(ctx, &migration.VolumeSpec{VolumePath: req.VolumeId})
			if err != nil {
				return nil, logger.LogNewErrorCodef(log, codes.Internal,
					"failed to get VolumeID from volumeMigrationService for volumePath: %q", volumePath)
			}
		}
		// Check if the volume contains CNS snapshots.
		if commonco.ContainerOrchestratorUtility.IsFSSEnabled(ctx, common.BlockVolumeSnapshot) {
			snapshots, _, err := common.QueryVolumeSnapshotsByVolumeID(ctx, c.manager.VolumeManager, req.VolumeId,
				common.QuerySnapshotLimit)
			if err != nil {
				return nil, logger.LogNewErrorCodef(log, codes.Internal,
					"failed to retrieve snapshots for volume: %s. Error: %+v", req.VolumeId, err)
			}
			if len(snapshots) == 0 {
				log.Infof("no CNS snapshots found for volume: %s, the volume can be safely deleted",
					req.VolumeId)
			} else {
				return nil, logger.LogNewErrorCodef(log, codes.FailedPrecondition,
					"volume: %s with existing snapshots %v cannot be deleted, "+
						"please delete snapshots before deleting the volume", req.VolumeId, snapshots)
			}
		}
		// TODO: Add code to determine the volume type and set volumeType for
		// Prometheus metric accordingly.
		err = common.DeleteVolumeUtil(ctx, c.manager.VolumeManager, req.VolumeId, true)
		if err != nil {
			return nil, logger.LogNewErrorCodef(log, codes.Internal,
				"failed to delete volume: %q. Error: %+v", req.VolumeId, err)
		}
		// Migration feature switch is enabled and volumePath is set.
		if volumePath != "" {
			// Delete VolumePath to VolumeID mapping.
			err = volumeMigrationService.DeleteVolumeInfo(ctx, req.VolumeId)
			if err != nil {
				return nil, logger.LogNewErrorCodef(log, codes.Internal,
					"failed to delete volumeInfo CR for volume: %q. Error: %+v", req.VolumeId, err)
			}
		}
		return &csi.DeleteVolumeResponse{}, nil
	}
	resp, err := deleteVolumeInternal()
	if err != nil {
		prometheus.CsiControlOpsHistVec.WithLabelValues(volumeType, prometheus.PrometheusDeleteVolumeOpType,
			prometheus.PrometheusFailStatus).Observe(time.Since(start).Seconds())
	} else {
		prometheus.CsiControlOpsHistVec.WithLabelValues(volumeType, prometheus.PrometheusDeleteVolumeOpType,
			prometheus.PrometheusPassStatus).Observe(time.Since(start).Seconds())
	}
	return resp, err
}

// ControllerPublishVolume attaches a volume to the Node VM.
// Volume id and node name is retrieved from ControllerPublishVolumeRequest.
func (c *controller) ControllerPublishVolume(ctx context.Context, req *csi.ControllerPublishVolumeRequest) (
	*csi.ControllerPublishVolumeResponse, error) {
	start := time.Now()
	volumeType := prometheus.PrometheusUnknownVolumeType

	controllerPublishVolumeInternal := func() (
		*csi.ControllerPublishVolumeResponse, error) {

		ctx = logger.NewContextWithLogger(ctx)
		log := logger.GetLogger(ctx)
		log.Infof("ControllerPublishVolume: called with args %+v", *req)
		err := validateVanillaControllerPublishVolumeRequest(ctx, req)
		if err != nil {
			return nil, logger.LogNewErrorCodef(log, codes.Internal,
				"validation for PublishVolume Request: %+v has failed. Error: %v", *req, err)
		}
		publishInfo := make(map[string]string)
		// Check whether its a block or file volume.
		if common.IsFileVolumeRequest(ctx, []*csi.VolumeCapability{req.GetVolumeCapability()}) {
			volumeType = prometheus.PrometheusFileVolumeType
			// File Volume.
			queryFilter := cnstypes.CnsQueryFilter{
				VolumeIds: []cnstypes.CnsVolumeId{{Id: req.VolumeId}},
			}
			querySelection := cnstypes.CnsQuerySelection{
				Names: []string{
					string(cnstypes.QuerySelectionNameTypeBackingObjectDetails),
				},
			}
			// Select only the backing object details.
			queryResult, err := utils.QueryAllVolumeUtil(ctx, c.manager.VolumeManager, queryFilter, querySelection,
				commonco.ContainerOrchestratorUtility.IsFSSEnabled(ctx, common.AsyncQueryVolume))
			if err != nil {
				return nil, logger.LogNewErrorCodef(log, codes.Internal,
					"queryVolume failed with err=%+v", err)
			}
			if len(queryResult.Volumes) == 0 {
				return nil, logger.LogNewErrorCodef(log, codes.Internal,
					"volumeID %s not found in QueryVolume", req.VolumeId)
			}

			vSANFileBackingDetails :=
				queryResult.Volumes[0].BackingObjectDetails.(*cnstypes.CnsVsanFileShareBackingDetails)
			publishInfo[common.AttributeDiskType] = common.DiskTypeFileVolume
			nfsv4AccessPointFound := false
			for _, kv := range vSANFileBackingDetails.AccessPoints {
				if kv.Key == common.Nfsv4AccessPointKey {
					publishInfo[common.Nfsv4AccessPoint] = kv.Value
					nfsv4AccessPointFound = true
					break
				}
			}
			if !nfsv4AccessPointFound {
				return nil, logger.LogNewErrorCodef(log, codes.Internal,
					"failed to get NFSv4 access point for volume: %q. Returned vSAN file backing details: %+v",
					req.VolumeId, vSANFileBackingDetails)
			}
		} else {
			// Block Volume.
			volumeType = prometheus.PrometheusBlockVolumeType
			if strings.Contains(req.VolumeId, ".vmdk") {
				// In-tree volume support.
				if !commonco.ContainerOrchestratorUtility.IsFSSEnabled(ctx, common.CSIMigration) {
					// Migration feature switch is disabled.
					return nil, logger.LogNewErrorCodef(log, codes.Internal,
						"volume-migration feature switch is disabled. Cannot use volume with vmdk path :%q", req.VolumeId)
				}
				// Migration feature switch is enabled.
				storagePolicyName := req.VolumeContext[common.AttributeStoragePolicyName]
				volumePath := req.VolumeId
				// In case if feature state switch is enabled after controller is
				// deployed, we need to initialize the volumeMigrationService.
				if err := initVolumeMigrationService(ctx, c); err != nil {
					// Error is already wrapped in CSI error code.
					return nil, err
				}
				req.VolumeId, err = volumeMigrationService.GetVolumeID(ctx,
					&migration.VolumeSpec{VolumePath: volumePath, StoragePolicyName: storagePolicyName})
				if err != nil {
					return nil, logger.LogNewErrorCodef(log, codes.Internal,
						"failed to get VolumeID from volumeMigrationService for volumePath: %q", volumePath)
				}
			}
			node, err := c.nodeMgr.GetNodeByName(ctx, req.NodeId)
			if err != nil {
				return nil, logger.LogNewErrorCodef(log, codes.Internal,
					"failed to find VirtualMachine for node:%q. Error: %v", req.NodeId, err)
			}
			log.Debugf("Found VirtualMachine for node:%q.", req.NodeId)
			diskUUID, err := common.AttachVolumeUtil(ctx, c.manager, node, req.VolumeId, false)
			if err != nil {
				return nil, logger.LogNewErrorCodef(log, codes.Internal,
					"failed to attach disk: %+q with node: %q err %+v", req.VolumeId, req.NodeId, err)
			}
			publishInfo[common.AttributeDiskType] = common.DiskTypeBlockVolume
			publishInfo[common.AttributeFirstClassDiskUUID] = common.FormatDiskUUID(diskUUID)
		}
		log.Infof("ControllerPublishVolume successful with publish context: %v", publishInfo)
		return &csi.ControllerPublishVolumeResponse{
			PublishContext: publishInfo,
		}, nil
	}
	resp, err := controllerPublishVolumeInternal()
	if err != nil {
		prometheus.CsiControlOpsHistVec.WithLabelValues(volumeType, prometheus.PrometheusAttachVolumeOpType,
			prometheus.PrometheusFailStatus).Observe(time.Since(start).Seconds())
	} else {
		prometheus.CsiControlOpsHistVec.WithLabelValues(volumeType, prometheus.PrometheusAttachVolumeOpType,
			prometheus.PrometheusPassStatus).Observe(time.Since(start).Seconds())
	}
	return resp, err
}

// ControllerUnpublishVolume detaches a volume from the Node VM. Volume id and
// node name is retrieved from ControllerUnpublishVolumeRequest.
func (c *controller) ControllerUnpublishVolume(ctx context.Context, req *csi.ControllerUnpublishVolumeRequest) (
	*csi.ControllerUnpublishVolumeResponse, error) {
	start := time.Now()
	volumeType := prometheus.PrometheusUnknownVolumeType

	controllerUnpublishVolumeInternal := func() (
		*csi.ControllerUnpublishVolumeResponse, error) {
		ctx = logger.NewContextWithLogger(ctx)
		log := logger.GetLogger(ctx)
		log.Infof("ControllerUnpublishVolume: called with args %+v", *req)
		err := validateVanillaControllerUnpublishVolumeRequest(ctx, req)
		if err != nil {
			return nil, logger.LogNewErrorCodef(log, codes.Internal,
				"validation for UnpublishVolume Request: %+v has failed. Error: %v", *req, err)
		}
		if !strings.Contains(req.VolumeId, ".vmdk") {
			// Check if volume is block or file, skip detach for file volume.
			queryFilter := cnstypes.CnsQueryFilter{
				VolumeIds: []cnstypes.CnsVolumeId{{Id: req.VolumeId}},
			}
			querySelection := cnstypes.CnsQuerySelection{
				Names: []string{
					string(cnstypes.QuerySelectionNameTypeVolumeType),
				},
			}
			// Select only the volume type.
			queryResult, err := utils.QueryAllVolumeUtil(ctx, c.manager.VolumeManager, queryFilter, querySelection,
				commonco.ContainerOrchestratorUtility.IsFSSEnabled(ctx, common.AsyncQueryVolume))
			if err != nil {
				return nil, logger.LogNewErrorCodef(log, codes.Internal,
					"queryVolume failed with err=%+v", err)
			}

			if len(queryResult.Volumes) == 0 {
				return nil, logger.LogNewErrorCodef(log, codes.Internal,
					"volumeID %q not found in QueryVolume", req.VolumeId)
			}
			if queryResult.Volumes[0].VolumeType == common.FileVolumeType {
				volumeType = prometheus.PrometheusFileVolumeType
				log.Infof("Skipping ControllerUnpublish for file volume %q", req.VolumeId)
				return &csi.ControllerUnpublishVolumeResponse{}, nil
			}
		} else {
			// In-tree volume support.
			volumeType = prometheus.PrometheusBlockVolumeType
			if !commonco.ContainerOrchestratorUtility.IsFSSEnabled(ctx, common.CSIMigration) {
				// Migration feature switch is disabled.
				return nil, logger.LogNewErrorCodef(log, codes.Internal,
					"volume-migration feature switch is disabled. Cannot use volume with vmdk path: %q", req.VolumeId)
			}
			// Migration feature switch is enabled.
			//
			// ControllerUnpublishVolume will never be the first call back for vmdk
			// registration with CNS. Here in the migration.VolumeSpec, we do not
			// supply SPBM Policy name. Node drain is the pre-requisite for volume
			// migration, so volume will be registered with SPBM policy during
			// ControllerPublish if metadata-syncer fails to register volume using
			// associated SPBM Policy. For ControllerUnpublishVolume, we anticipate
			// volume is already registered with CNS, and volumeMigrationService
			// should return volumeID for requested VolumePath.
			volumePath := req.VolumeId
			// In case if feature state switch is enabled after controller is
			// deployed, we need to initialize the volumeMigrationService.
			if err := initVolumeMigrationService(ctx, c); err != nil {
				// Error is already wrapped in CSI error code.
				return nil, err
			}
			req.VolumeId, err = volumeMigrationService.GetVolumeID(ctx, &migration.VolumeSpec{VolumePath: volumePath})
			if err != nil {
				return nil, logger.LogNewErrorCodef(log, codes.Internal,
					"failed to get VolumeID from volumeMigrationService for volumePath: %q", volumePath)
			}
		}
		// Block Volume.
		volumeType = prometheus.PrometheusBlockVolumeType
		node, err := c.nodeMgr.GetNodeByName(ctx, req.NodeId)
		if err != nil {
			return nil, logger.LogNewErrorCodef(log, codes.Internal,
				"failed to find VirtualMachine for node:%q. Error: %v", req.NodeId, err)
		}
		err = common.DetachVolumeUtil(ctx, c.manager, node, req.VolumeId)
		if err != nil {
			return nil, logger.LogNewErrorCodef(log, codes.Internal,
				"failed to detach disk: %+q from node: %q err %+v", req.VolumeId, req.NodeId, err)
		}
		log.Infof("ControllerUnpublishVolume successful for volume ID: %s", req.VolumeId)
		return &csi.ControllerUnpublishVolumeResponse{}, nil
	}
	resp, err := controllerUnpublishVolumeInternal()
	if err != nil {
		prometheus.CsiControlOpsHistVec.WithLabelValues(volumeType, prometheus.PrometheusDetachVolumeOpType,
			prometheus.PrometheusFailStatus).Observe(time.Since(start).Seconds())
	} else {
		prometheus.CsiControlOpsHistVec.WithLabelValues(volumeType, prometheus.PrometheusDetachVolumeOpType,
			prometheus.PrometheusPassStatus).Observe(time.Since(start).Seconds())
	}
	return resp, err
}

// ControllerExpandVolume expands a volume.
// Volume id and size is retrieved from ControllerExpandVolumeRequest.
func (c *controller) ControllerExpandVolume(ctx context.Context, req *csi.ControllerExpandVolumeRequest) (
	*csi.ControllerExpandVolumeResponse, error) {
	ctx = logger.NewContextWithLogger(ctx)
	log := logger.GetLogger(ctx)
	log.Infof("ControllerExpandVolume: called with args %+v", *req)

	if strings.Contains(req.VolumeId, ".vmdk") {
		return nil, logger.LogNewErrorCodef(log, codes.Unimplemented,
			"cannot expand migrated vSphere volume. :%q", req.VolumeId)
	}

	isExtendSupported, err := c.manager.VcenterManager.IsExtendVolumeSupported(ctx, c.manager.VcenterConfig.Host)
	if err != nil {
		return nil, logger.LogNewErrorCodef(log, codes.Internal,
			"failed to verify if extend volume is supported or not. Error: %+v", err)
	}
	if !isExtendSupported {
		return nil, logger.LogNewErrorCode(log, codes.Internal,
			"volume Expansion is not supported in this vSphere release. "+
				"Upgrade to vSphere 7.0 for offline expansion and vSphere 7.0U2 for online expansion support.")
	}

	isOnlineExpansionSupported, err := c.manager.VcenterManager.IsOnlineExtendVolumeSupported(ctx,
		c.manager.VcenterConfig.Host)
	if err != nil {
		return nil, logger.LogNewErrorCodef(log, codes.Internal,
			"failed to check if online expansion is supported due to error: %v", err)
	}
	isOnlineExpansionEnabled := commonco.ContainerOrchestratorUtility.IsFSSEnabled(ctx, common.OnlineVolumeExtend)
	err = validateVanillaControllerExpandVolumeRequest(ctx, req, isOnlineExpansionEnabled, isOnlineExpansionSupported)
	if err != nil {
		msg := fmt.Sprintf("validation for ExpandVolume Request: %+v has failed. Error: %v", *req, err)
		log.Error(msg)
		return nil, err
	}

	volumeID := req.GetVolumeId()
	volSizeBytes := int64(req.GetCapacityRange().GetRequiredBytes())
	volSizeMB := int64(common.RoundUpSize(volSizeBytes, common.MbInBytes))

	// Check if the volume contains CNS snapshots.
	if commonco.ContainerOrchestratorUtility.IsFSSEnabled(ctx, common.BlockVolumeSnapshot) {
		snapshots, _, err := common.QueryVolumeSnapshotsByVolumeID(ctx, c.manager.VolumeManager, volumeID,
			common.QuerySnapshotLimit)
		if err != nil {
			return nil, logger.LogNewErrorCodef(log, codes.Internal,
				"failed to retrieve snapshots for volume: %s. Error: %+v", volumeID, err)
		}
		if len(snapshots) == 0 {
			log.Infof("The volume %s can be safely expanded as no CNS snapshots were found.",
				req.VolumeId)
		} else {
			return nil, logger.LogNewErrorCodef(log, codes.FailedPrecondition,
				"volume: %s with existing snapshots %v cannot be expanded. "+
					"Please delete snapshots before expanding the volume", req.VolumeId, snapshots)
		}
	}

	err = common.ExpandVolumeUtil(ctx, c.manager, volumeID, volSizeMB,
		commonco.ContainerOrchestratorUtility.IsFSSEnabled(ctx, common.AsyncQueryVolume),
		commonco.ContainerOrchestratorUtility.IsFSSEnabled(ctx, common.CSIVolumeManagerIdempotency))
	if err != nil {
		return nil, logger.LogNewErrorCodef(log, codes.Internal,
			"failed to expand volume: %q to size: %d with error: %+v", volumeID, volSizeMB, err)
	}

	// Always set nodeExpansionRequired to true, even if requested size is equal
	// to current size. Volume expansion may succeed on CNS but external-resizer
	// may fail to update API server. Requests are requeued in this case. Setting
	// nodeExpandsionRequired to false marks PVC resize as finished which
	// prevents kubelet from expanding the filesystem.
	// Ref: https://github.com/kubernetes-csi/external-resizer/blob/master/pkg/controller/controller.go#L335
	nodeExpansionRequired := true
	// Node expansion is not required for raw block volumes.
	if _, ok := req.GetVolumeCapability().GetAccessType().(*csi.VolumeCapability_Block); ok {
		nodeExpansionRequired = false
	}
	resp := &csi.ControllerExpandVolumeResponse{
		CapacityBytes:         int64(units.FileSize(volSizeMB * common.MbInBytes)),
		NodeExpansionRequired: nodeExpansionRequired,
	}
	return resp, nil
}

// ValidateVolumeCapabilities returns the capabilities of the volume.
func (c *controller) ValidateVolumeCapabilities(ctx context.Context, req *csi.ValidateVolumeCapabilitiesRequest) (
	*csi.ValidateVolumeCapabilitiesResponse, error) {
	ctx = logger.NewContextWithLogger(ctx)
	log := logger.GetLogger(ctx)
	log.Infof("ControllerGetCapabilities: called with args %+v", *req)
	volCaps := req.GetVolumeCapabilities()
	var confirmed *csi.ValidateVolumeCapabilitiesResponse_Confirmed
	if err := common.IsValidVolumeCapabilities(ctx, volCaps); err == nil {
		confirmed = &csi.ValidateVolumeCapabilitiesResponse_Confirmed{VolumeCapabilities: volCaps}
	}
	return &csi.ValidateVolumeCapabilitiesResponse{
		Confirmed: confirmed,
	}, nil
}

func (c *controller) ListVolumes(ctx context.Context, req *csi.ListVolumesRequest) (
	*csi.ListVolumesResponse, error) {
	ctx = logger.NewContextWithLogger(ctx)
	log := logger.GetLogger(ctx)
	log.Infof("ListVolumes: called with args %+v", *req)
	return nil, logger.LogNewErrorCode(log, codes.Unimplemented, "listVolumes")
}

func (c *controller) GetCapacity(ctx context.Context, req *csi.GetCapacityRequest) (
	*csi.GetCapacityResponse, error) {
	ctx = logger.NewContextWithLogger(ctx)
	log := logger.GetLogger(ctx)
	log.Infof("GetCapacity: called with args %+v", *req)
	return nil, logger.LogNewErrorCode(log, codes.Unimplemented, "getCapacity")
}

// initVolumeMigrationService is a helper method to initialize
// volumeMigrationService in controller.
func initVolumeMigrationService(ctx context.Context, c *controller) error {
	log := logger.GetLogger(ctx)
	// This check prevents unnecessary RLocks on the volumeMigration instance.
	if volumeMigrationService != nil {
		return nil
	}
	// In case if feature state switch is enabled after controller is deployed,
	// we need to initialize the volumeMigrationService.
	var err error
	volumeMigrationService, err = migration.GetVolumeMigrationService(ctx,
		&c.manager.VolumeManager, c.manager.CnsConfig, false)
	if err != nil {
		return logger.LogNewErrorCodef(log, codes.Internal,
			"failed to get migration service. Err: %v", err)
	}
	return nil
}

func (c *controller) ControllerGetCapabilities(ctx context.Context, req *csi.ControllerGetCapabilitiesRequest) (
	*csi.ControllerGetCapabilitiesResponse, error) {
	ctx = logger.NewContextWithLogger(ctx)
	log := logger.GetLogger(ctx)
	log.Infof("ControllerGetCapabilities: called with args %+v", *req)

	controllerCaps := []csi.ControllerServiceCapability_RPC_Type{
		csi.ControllerServiceCapability_RPC_CREATE_DELETE_VOLUME,
		csi.ControllerServiceCapability_RPC_PUBLISH_UNPUBLISH_VOLUME,
		csi.ControllerServiceCapability_RPC_EXPAND_VOLUME,
	}
	var vcSnapshotSupportCheck, csiSnapshotFSSEnabled bool
	// Report capability only if CSI Snapshot FSS is enabled and VC supports
	// snapshot feature.
	csiSnapshotFSSEnabled = commonco.ContainerOrchestratorUtility.IsFSSEnabled(ctx, common.BlockVolumeSnapshot)
	if csiSnapshotFSSEnabled {
		// Check VC support only if internal FSS is enabled.
		vcSnapshotSupportCheck = common.CheckSnapshotSupport(ctx, c.manager)
	}

	if csiSnapshotFSSEnabled && vcSnapshotSupportCheck {
		log.Infof("ControllerGetCapabilities: reporting Snapshot capabilities as snapshot FSS is enabled.")
		controllerCaps = append(controllerCaps, csi.ControllerServiceCapability_RPC_CREATE_DELETE_SNAPSHOT)
	}

	var caps []*csi.ControllerServiceCapability
	for _, cap := range controllerCaps {
		c := &csi.ControllerServiceCapability{
			Type: &csi.ControllerServiceCapability_Rpc{
				Rpc: &csi.ControllerServiceCapability_RPC{
					Type: cap,
				},
			},
		}
		caps = append(caps, c)
	}
	return &csi.ControllerGetCapabilitiesResponse{Capabilities: caps}, nil
}

func (c *controller) CreateSnapshot(ctx context.Context, req *csi.CreateSnapshotRequest) (
	*csi.CreateSnapshotResponse, error) {
	ctx = logger.NewContextWithLogger(ctx)
	log := logger.GetLogger(ctx)
	log.Infof("CreateSnapshot: called with args %+v", *req)

	isBlockVolumeSnapshotEnabled := commonco.ContainerOrchestratorUtility.IsFSSEnabled(ctx, common.BlockVolumeSnapshot)
	if !isBlockVolumeSnapshotEnabled {
		return nil, logger.LogNewErrorCode(log, codes.Unimplemented, "createSnapshot")
	}

	volumeType := prometheus.PrometheusUnknownVolumeType
	createSnapshotInternal := func() (*csi.CreateSnapshotResponse, error) {
		// Validate CreateSnapshotRequest
		if err := validateVanillaCreateSnapshotRequestRequest(ctx, req); err != nil {
			return nil, logger.LogNewErrorCodef(log, codes.Internal,
				"validation for CreateSnapshot Request: %+v has failed. Error: %v", *req, err)
		}
		volumeID := req.GetSourceVolumeId()

		// Check if the source volume is migrated vSphere volume
		if strings.Contains(volumeID, ".vmdk") {
			return nil, logger.LogNewErrorCodef(log, codes.Unimplemented,
				"cannot snapshot migrated vSphere volume. :%q", volumeID)
		}

		// Query capacity in MB and datastore url for block volume snapshot
		snapshotSizeInMB, datastoreUrl, err := utils.QueryCapacityAndDsUrlForVolumeUtil(
			ctx, c.manager.VolumeManager, volumeID, common.BlockVolumeType)
		if err != nil {
			return nil, err
		}
		volumeType = prometheus.PrometheusBlockVolumeType

		// Check if snapshots number of this volume reaches the granular limit on VSAN/VVOL
		maxSnapshotsPerBlockVolume := c.manager.CnsConfig.Snapshot.GlobalMaxSnapshotsPerBlockVolume
		log.Infof("The limit of the maximum number of snapshots per block volume is "+
			"set to the global maximum (%v) by default.", maxSnapshotsPerBlockVolume)
		if c.manager.CnsConfig.Snapshot.GranularMaxSnapshotsPerBlockVolumeInVSAN > 0 ||
			c.manager.CnsConfig.Snapshot.GranularMaxSnapshotsPerBlockVolumeInVVOL > 0 {

			var isGranularMaxEnabled bool
			if strings.Contains(datastoreUrl, strings.ToLower(string(types.HostFileSystemVolumeFileSystemTypeVsan))) {
				if c.manager.CnsConfig.Snapshot.GranularMaxSnapshotsPerBlockVolumeInVSAN > 0 {
					maxSnapshotsPerBlockVolume = c.manager.CnsConfig.Snapshot.GranularMaxSnapshotsPerBlockVolumeInVSAN
					isGranularMaxEnabled = true

				}
			} else if strings.Contains(datastoreUrl, strings.ToLower(string(types.HostFileSystemVolumeFileSystemTypeVVOL))) {
				if c.manager.CnsConfig.Snapshot.GranularMaxSnapshotsPerBlockVolumeInVVOL > 0 {
					maxSnapshotsPerBlockVolume = c.manager.CnsConfig.Snapshot.GranularMaxSnapshotsPerBlockVolumeInVVOL
					isGranularMaxEnabled = true
				}
			}

			if isGranularMaxEnabled {
				log.Infof("The limit of the maximum number of snapshots per block volume on datastore %q is "+
					"overridden by the granular maximum (%v).", datastoreUrl, maxSnapshotsPerBlockVolume)
			}
		}

		// Check if snapshots number of this volume reaches the limit
		snapshotList, _, err := common.QueryVolumeSnapshotsByVolumeID(
			ctx, c.manager.VolumeManager, volumeID, common.QuerySnapshotLimit)
		if err != nil {
			return nil, logger.LogNewErrorCodef(log, codes.Internal,
				"failed to query snapshots of volume %s for the limit check. Error: %v", volumeID, err)
		}

		if len(snapshotList) >= maxSnapshotsPerBlockVolume {
			return nil, logger.LogNewErrorCodef(log, codes.FailedPrecondition,
				"the number of snapshots on the source volume %s reaches the configured maximum (%v)",
				volumeID, c.manager.CnsConfig.Snapshot.GlobalMaxSnapshotsPerBlockVolume)
		}

		// the returned snapshotID below is a combination of CNS VolumeID and CNS SnapshotID concatenated by the "+"
		// sign. That is, a string of "<UUID>+<UUID>". Because, all other CNS snapshot APIs still require both
		// VolumeID and SnapshotID as the input, while corresponding snapshot APIs in upstream CSI require SnapshotID.
		// So, we need to bridge the gap in vSphere CSI driver and return a combined SnapshotID to CSI Snapshotter.
		snapshotID, snapshotCreateTimePtr, err := common.CreateSnapshotUtil(ctx, c.manager, volumeID, req.Name)
		if err != nil {
			return nil, logger.LogNewErrorCodef(log, codes.Internal,
				"failed to create snapshot on volume %q: %v", volumeID, err)
		}
		snapshotCreateTimeInProto := timestamppb.New(*snapshotCreateTimePtr)

		createSnapshotResponse := &csi.CreateSnapshotResponse{
			Snapshot: &csi.Snapshot{
				SizeBytes:      snapshotSizeInMB * common.MbInBytes,
				SnapshotId:     snapshotID,
				SourceVolumeId: volumeID,
				CreationTime:   snapshotCreateTimeInProto,
				ReadyToUse:     true,
			},
		}

		log.Infof("CreateSnapshot succeeded for snapshot %s "+
			"on volume %s size %d Time proto %+v Timestamp %+v Response: %+v",
			snapshotID, volumeID, snapshotSizeInMB*common.MbInBytes, snapshotCreateTimeInProto,
			*snapshotCreateTimePtr, createSnapshotResponse)
		return createSnapshotResponse, nil
	}

	start := time.Now()
	resp, err := createSnapshotInternal()
	if err != nil {
		prometheus.CsiControlOpsHistVec.WithLabelValues(volumeType, prometheus.PrometheusCreateSnapshotOpType,
			prometheus.PrometheusFailStatus).Observe(time.Since(start).Seconds())
	} else {
		prometheus.CsiControlOpsHistVec.WithLabelValues(volumeType, prometheus.PrometheusCreateSnapshotOpType,
			prometheus.PrometheusPassStatus).Observe(time.Since(start).Seconds())
	}
	return resp, err
}

func (c *controller) DeleteSnapshot(ctx context.Context, req *csi.DeleteSnapshotRequest) (
	*csi.DeleteSnapshotResponse, error) {
	ctx = logger.NewContextWithLogger(ctx)
	log := logger.GetLogger(ctx)
	log.Infof("DeleteSnapshot: called with args %+v", *req)

	isBlockVolumeSnapshotEnabled :=
		commonco.ContainerOrchestratorUtility.IsFSSEnabled(ctx, common.BlockVolumeSnapshot)
	if !isBlockVolumeSnapshotEnabled {
		return nil, logger.LogNewErrorCode(log, codes.Unimplemented, "deleteSnapshot")
	}

	deleteSnapshotInternal := func() (*csi.DeleteSnapshotResponse, error) {
		csiSnapshotID := req.GetSnapshotId()
		err := common.DeleteSnapshotUtil(ctx, c.manager, csiSnapshotID)
		if err != nil {
			return nil, logger.LogNewErrorCodef(log, codes.Internal,
				"Failed to delete snapshot %q. Error: %+v",
				csiSnapshotID, err)
		}

		log.Infof("DeleteSnapshot: successfully deleted snapshot %q", csiSnapshotID)
		return &csi.DeleteSnapshotResponse{}, nil
	}

	volumeType := prometheus.PrometheusBlockVolumeType
	start := time.Now()
	resp, err := deleteSnapshotInternal()
	if err != nil {
		prometheus.CsiControlOpsHistVec.WithLabelValues(volumeType, prometheus.PrometheusDeleteSnapshotOpType,
			prometheus.PrometheusFailStatus).Observe(time.Since(start).Seconds())
	} else {
		prometheus.CsiControlOpsHistVec.WithLabelValues(volumeType, prometheus.PrometheusDeleteSnapshotOpType,
			prometheus.PrometheusPassStatus).Observe(time.Since(start).Seconds())
	}
	return resp, err

}

func (c *controller) ListSnapshots(ctx context.Context, req *csi.ListSnapshotsRequest) (
	*csi.ListSnapshotsResponse, error) {
	start := time.Now()
	volumeType := prometheus.PrometheusBlockVolumeType
	listSnapshotsInternal := func() (*csi.ListSnapshotsResponse, error) {
		ctx = logger.NewContextWithLogger(ctx)
		log := logger.GetLogger(ctx)
		log.Infof("ListSnapshots: called with args %+v", *req)
		err := validateVanillaListSnapshotRequest(ctx, req)
		if err != nil {
			return nil, err
		}
		maxEntries := common.QuerySnapshotLimit
		if req.MaxEntries != 0 {
			maxEntries = int64(req.MaxEntries)
		}
		snapshots, nextToken, err := common.ListSnapshotsUtil(ctx, c.manager.VolumeManager, req.SourceVolumeId,
			req.SnapshotId, req.StartingToken, maxEntries)
		if err != nil {
			return nil, logger.LogNewErrorCodef(log, codes.Internal, " failed to retrieve the snapshots, err: %+v", err)
		}
		var entries []*csi.ListSnapshotsResponse_Entry
		for _, snapshot := range snapshots {
			entry := &csi.ListSnapshotsResponse_Entry{
				Snapshot: snapshot,
			}
			entries = append(entries, entry)
		}
		resp := &csi.ListSnapshotsResponse{
			Entries:   entries,
			NextToken: nextToken,
		}
		log.Infof("ListSnapshot served %d results, token for next set: %s", len(entries), nextToken)
		return resp, nil
	}
	resp, err := listSnapshotsInternal()
	if err != nil {
		prometheus.CsiControlOpsHistVec.WithLabelValues(volumeType, prometheus.PrometheusListSnapshotsOpType,
			prometheus.PrometheusFailStatus).Observe(time.Since(start).Seconds())
	} else {
		prometheus.CsiControlOpsHistVec.WithLabelValues(volumeType, prometheus.PrometheusListSnapshotsOpType,
			prometheus.PrometheusPassStatus).Observe(time.Since(start).Seconds())
	}
	return resp, err
}

func (c *controller) ControllerGetVolume(ctx context.Context, req *csi.ControllerGetVolumeRequest) (
	*csi.ControllerGetVolumeResponse, error) {
	ctx = logger.NewContextWithLogger(ctx)
	log := logger.GetLogger(ctx)
	log.Infof("ControllerGetVolume: called with args %+v", *req)
	return nil, logger.LogNewErrorCode(log, codes.Unimplemented, "controllerGetVolume")
}
