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
	"errors"
	"fmt"
	"math/rand"
	"net/http"
	"path/filepath"
	"strings"
	"time"

	"github.com/container-storage-interface/spec/lib/go/csi"
	"github.com/fsnotify/fsnotify"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	cnstypes "github.com/vmware/govmomi/cns/types"
	"github.com/vmware/govmomi/units"
	"github.com/vmware/govmomi/vapi/tags"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"sigs.k8s.io/vsphere-csi-driver/pkg/apis/migration"
	cnsvolume "sigs.k8s.io/vsphere-csi-driver/pkg/common/cns-lib/volume"
	cnsvsphere "sigs.k8s.io/vsphere-csi-driver/pkg/common/cns-lib/vsphere"
	cnsconfig "sigs.k8s.io/vsphere-csi-driver/pkg/common/config"
	"sigs.k8s.io/vsphere-csi-driver/pkg/common/prometheus"
	"sigs.k8s.io/vsphere-csi-driver/pkg/csi/service/common"
	"sigs.k8s.io/vsphere-csi-driver/pkg/csi/service/common/commonco"
	"sigs.k8s.io/vsphere-csi-driver/pkg/csi/service/logger"
	csitypes "sigs.k8s.io/vsphere-csi-driver/pkg/csi/types"
)

// NodeManagerInterface provides functionality to manage nodes.
type NodeManagerInterface interface {
	Initialize(ctx context.Context) error
	GetSharedDatastoresInK8SCluster(ctx context.Context) ([]*cnsvsphere.DatastoreInfo, error)
	GetSharedDatastoresInTopology(ctx context.Context, topologyRequirement *csi.TopologyRequirement, tagManager *tags.Manager, zoneKey string, regionKey string) ([]*cnsvsphere.DatastoreInfo, map[string][]map[string]string, error)
	GetNodeByName(ctx context.Context, nodeName string) (*cnsvsphere.VirtualMachine, error)
	GetAllNodes(ctx context.Context) ([]*cnsvsphere.VirtualMachine, error)
}

type controller struct {
	manager *common.Manager
	nodeMgr NodeManagerInterface
	authMgr common.AuthorizationService
}

// volumeMigrationService holds the pointer to VolumeMigration instance
var volumeMigrationService migration.VolumeMigrationService

// New creates a CNS controller
func New() csitypes.CnsController {
	return &controller{}
}

// Init is initializing controller struct
func (c *controller) Init(config *cnsconfig.Config, version string) error {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	ctx = logger.NewContextWithLogger(ctx)
	log := logger.GetLogger(ctx)

	log.Infof("Initializing CNS controller")
	var err error
	// Get VirtualCenterManager instance and validate version
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
	c.manager = &common.Manager{
		VcenterConfig:  vcenterconfig,
		CnsConfig:      config,
		VolumeManager:  cnsvolume.GetManager(ctx, vcenter),
		VcenterManager: vcManager,
	}

	vc, err := common.GetVCenter(ctx, c.manager)
	if err != nil {
		log.Errorf("failed to get vcenter. err=%v", err)
		return err
	}

	isAuthCheckFSSEnabled := commonco.ContainerOrchestratorUtility.IsFSSEnabled(ctx, common.CSIAuthCheck)
	// Check if vSAN FS is enabled for TargetvSANFileShareDatastoreURLs only if CSIAuthCheck FSS is not enabled
	if !isAuthCheckFSSEnabled && len(c.manager.VcenterConfig.TargetvSANFileShareDatastoreURLs) > 0 {
		// Check if file service is enabled on datastore present in targetvSANFileShareDatastoreURLs.
		dsToFileServiceEnabledMap, err := common.IsFileServiceEnabled(ctx, c.manager.VcenterConfig.TargetvSANFileShareDatastoreURLs, vc)
		if err != nil {
			msg := fmt.Sprintf("file service enablement check failed for datastore specified in TargetvSANFileShareDatastoreURLs. err=%v", err)
			log.Error(msg)
			return errors.New(msg)
		}
		for _, targetFSDatastore := range c.manager.VcenterConfig.TargetvSANFileShareDatastoreURLs {
			isFSEnabled := dsToFileServiceEnabledMap[targetFSDatastore]
			if !isFSEnabled {
				msg := fmt.Sprintf("file service is not enabled on datastore %s specified in TargetvSANFileShareDatastoreURLs", targetFSDatastore)
				log.Error(msg)
				return errors.New(msg)
			}
		}
	}

	// Check vCenter API Version
	if err = common.CheckAPI(vc.Client.ServiceContent.About.ApiVersion); err != nil {
		log.Errorf("checkAPI failed for vcenter API version: %s, err=%v", vc.Client.ServiceContent.About.ApiVersion, err)
		return err
	}
	c.nodeMgr = &Nodes{}
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
		isvSANFileServicesSupported, err := c.manager.VcenterManager.IsvSANFileServicesSupported(ctx, c.manager.VcenterConfig.Host)
		if err != nil {
			log.Errorf("failed to verify if vSAN file services is supported or not. Error:%+v", err)
			return err
		}
		if isvSANFileServicesSupported {
			go common.ComputeDatastoreMapForFileVolumes(authMgr.(*common.AuthManager),
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

// ReloadConfiguration reloads configuration from the secret, and update controller's config cache
// and VolumeManager's VC Config cache.
func (c *controller) ReloadConfiguration() error {
	ctx, log := logger.GetNewContextWithLogger()
	log.Info("Reloading Configuration")
	cfg, err := common.GetConfig(ctx)
	if err != nil {
		msg := fmt.Sprintf("failed to read config. Error: %+v", err)
		log.Error(msg)
		return errors.New(msg)
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

			// Verify if new configuration has valid credentials by connecting to vCenter.
			// Proceed only if the connection succeeds, else return error.
			newVC := &cnsvsphere.VirtualCenter{Config: newVCConfig}
			if err = newVC.Connect(ctx); err != nil {
				msg := fmt.Sprintf("failed to connect to VirtualCenter host: %q, Err: %+v", newVCConfig.Host, err)
				log.Error(msg)
				return errors.New(msg)
			}

			// Reset virtual center singleton instance by passing reload flag as true
			log.Info("Obtaining new vCenterInstance using new credentials")
			vcenter, err = cnsvsphere.GetVirtualCenterInstance(ctx, &cnsconfig.ConfigurationInfo{Cfg: cfg}, true)
			if err != nil {
				msg := fmt.Sprintf("failed to get VirtualCenter. err=%v", err)
				log.Error(msg)
				return errors.New(msg)
			}
		} else {
			// If it's not a VC host or VC credentials update, same singleton instance can be used
			// and it's Config field can be updated
			vcenter, err = cnsvsphere.GetVirtualCenterInstance(ctx, &cnsconfig.ConfigurationInfo{Cfg: cfg}, false)
			if err != nil {
				msg := fmt.Sprintf("failed to get VirtualCenter. err=%v", err)
				log.Error(msg)
				return errors.New(msg)
			}
			vcenter.Config = newVCConfig
		}
		c.manager.VolumeManager.ResetManager(ctx, vcenter)
		c.manager.VcenterConfig = newVCConfig
		c.manager.VolumeManager = cnsvolume.GetManager(ctx, vcenter)
		// Re-Initialize Node Manager to cache latest vCenter config
		c.nodeMgr = &Nodes{}
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

func (c *controller) filterDatastores(ctx context.Context, sharedDatastores []*cnsvsphere.DatastoreInfo) []*cnsvsphere.DatastoreInfo {
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
	// Volume Size - Default is 10 GiB
	volSizeBytes := int64(common.DefaultGbDiskSize * common.GbInBytes)
	if req.GetCapacityRange() != nil && req.GetCapacityRange().RequiredBytes != 0 {
		volSizeBytes = int64(req.GetCapacityRange().GetRequiredBytes())
	}
	volSizeMB := int64(common.RoundUpSize(volSizeBytes, common.MbInBytes))

	// Fetching the feature state for csi-migration before parsing storage class params
	csiMigrationFeatureState := commonco.ContainerOrchestratorUtility.IsFSSEnabled(ctx, common.CSIMigration)
	scParams, err := common.ParseStorageClassParams(ctx, req.Parameters, csiMigrationFeatureState)
	if err != nil {
		msg := fmt.Sprintf("Parsing storage class parameters failed with error: %+v", err)
		log.Error(msg)
		return nil, status.Errorf(codes.InvalidArgument, msg)
	}

	if csiMigrationFeatureState && scParams.CSIMigration == "true" {
		if len(scParams.Datastore) != 0 {
			log.Infof("Converting datastore name: %q to Datastore URL", scParams.Datastore)
			// Get vCenter
			vCenter, err := cnsvsphere.GetVirtualCenterManager(ctx).GetVirtualCenter(ctx, c.manager.VcenterConfig.Host)
			if err != nil {
				msg := fmt.Sprintf("failed to get vCenter. err: %+v", err)
				log.Error(msg)
				return nil, status.Errorf(codes.Internal, msg)
			}
			dcList, err := vCenter.GetDatacenters(ctx)
			if err != nil {
				msg := fmt.Sprintf("failed to get datacenter list. err: %+v", err)
				log.Error(msg)
				return nil, status.Errorf(codes.Internal, msg)
			}
			foundDatastoreURL := false
			for _, dc := range dcList {
				dsURLTodsInfoMap, err := dc.GetAllDatastores(ctx)
				if err != nil {
					msg := fmt.Sprintf("failed to get dsURLTodsInfoMap. err: %+v", err)
					log.Error(msg)
					return nil, status.Errorf(codes.Internal, msg)
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
				msg := fmt.Sprintf("failed to find datastoreURL for datastore name: %q", scParams.Datastore)
				log.Error(msg)
				return nil, status.Errorf(codes.Internal, msg)
			}
		}
	}
	var createVolumeSpec = common.CreateVolumeSpec{
		CapacityMB: volSizeMB,
		Name:       req.Name,
		ScParams:   scParams,
		VolumeType: common.BlockVolumeType,
	}

	var sharedDatastores []*cnsvsphere.DatastoreInfo
	var datastoreTopologyMap = make(map[string][]map[string]string)

	// Get accessibility
	topologyRequirement := req.GetAccessibilityRequirements()
	if topologyRequirement != nil {
		// Get shared accessible datastores for matching topology requirement
		if c.manager.CnsConfig.Labels.Zone == "" || c.manager.CnsConfig.Labels.Region == "" {
			// if zone and region label (vSphere category names) not specified in the config secret, then return
			// NotFound error.
			errMsg := "Zone/Region vsphere category names not specified in the vsphere config secret"
			log.Errorf(errMsg)
			return nil, status.Error(codes.NotFound, errMsg)
		}
		vcenter, err := c.manager.VcenterManager.GetVirtualCenter(ctx, c.manager.VcenterConfig.Host)
		if err != nil {
			errMsg := fmt.Sprintf("Failed to get vCenter. Err: %v", err)
			log.Errorf(errMsg)
			return nil, status.Error(codes.NotFound, errMsg)
		}
		tagManager, err := cnsvsphere.GetTagManager(ctx, vcenter)
		if err != nil {
			errMsg := fmt.Sprintf("Failed to get tagManager. Err: %v", err)
			log.Errorf(errMsg)
			return nil, status.Error(codes.NotFound, errMsg)
		}
		defer func() {
			err := tagManager.Logout(ctx)
			if err != nil {
				log.Errorf("failed to logout tagManager. err: %v", err)
			}
		}()
		sharedDatastores, datastoreTopologyMap, err = c.nodeMgr.GetSharedDatastoresInTopology(ctx, topologyRequirement, tagManager, c.manager.CnsConfig.Labels.Zone, c.manager.CnsConfig.Labels.Region)
		if err != nil || len(sharedDatastores) == 0 {
			msg := fmt.Sprintf("failed to get shared datastores in topology: %+v. Error: %+v", topologyRequirement, err)
			log.Error(msg)
			return nil, status.Error(codes.NotFound, msg)
		}
		log.Debugf("Shared datastores [%+v] retrieved for topologyRequirement [%+v] with datastoreTopologyMap [+%v]", sharedDatastores, topologyRequirement, datastoreTopologyMap)
		if createVolumeSpec.ScParams.DatastoreURL != "" {
			// Check datastoreURL specified in the storageclass is accessible from topology
			isDataStoreAccessible := false
			for _, sharedDatastore := range sharedDatastores {
				if sharedDatastore.Info.Url == createVolumeSpec.ScParams.DatastoreURL {
					isDataStoreAccessible = true
					break
				}
			}
			if !isDataStoreAccessible {
				errMsg := fmt.Sprintf("DatastoreURL: %s specified in the storage class is not accessible in the topology:[+%v]",
					createVolumeSpec.ScParams.DatastoreURL, topologyRequirement)
				log.Errorf(errMsg)
				return nil, status.Error(codes.InvalidArgument, errMsg)
			}
		}

	} else {
		sharedDatastores, err = c.nodeMgr.GetSharedDatastoresInK8SCluster(ctx)
		if err != nil || len(sharedDatastores) == 0 {
			msg := fmt.Sprintf("failed to get shared datastores in kubernetes cluster. Error: %+v", err)
			log.Error(msg)
			return nil, status.Errorf(codes.Internal, msg)
		}
	}

	if commonco.ContainerOrchestratorUtility.IsFSSEnabled(ctx, common.CSIAuthCheck) {
		// filter datastores which in datastoreMap from sharedDatastores
		sharedDatastores = c.filterDatastores(ctx, sharedDatastores)
	}
	volumeInfo, err := common.CreateBlockVolumeUtil(ctx, cnstypes.CnsClusterFlavorVanilla, c.manager, &createVolumeSpec, sharedDatastores)
	if err != nil {
		msg := fmt.Sprintf("failed to create volume. Error: %+v", err)
		log.Error(msg)
		return nil, status.Errorf(codes.Internal, msg)
	}

	attributes := make(map[string]string)
	attributes[common.AttributeDiskType] = common.DiskTypeBlockVolume
	if csiMigrationFeatureState && scParams.CSIMigration == "true" {
		// In case if feature state switch is enabled after controller is deployed, we need to initialize the volumeMigrationService
		if err := initVolumeMigrationService(ctx, c); err != nil {
			// Error is already wrapped in CSI error code
			return nil, err
		}
		// Return InitialVolumeFilepath in the response for TranslateCSIPVToInTree
		volumePath, err := volumeMigrationService.GetVolumePath(ctx, volumeInfo.VolumeID.Id)
		if err != nil {
			msg := fmt.Sprintf("failed to get volume path for volume id: %q. Error: %+v", volumeInfo.VolumeID.Id, err)
			log.Error(msg)
			return nil, status.Errorf(codes.Internal, msg)
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

	// Retrieve the datastoreURL of the Provisioned Volume
	// If CNS CreateVolume API does not return datastoreURL, retrieve this by calling QueryVolume
	// otherwise, retrieve this from PlacementResults from the response of CreateVolume API
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
				log.Errorf("QueryVolume failed for volumeID: %s", volumeInfo.VolumeID.Id)
				return nil, status.Error(codes.Internal, err.Error())
			}
			if len(queryResult.Volumes) > 0 {
				// Find datastore topology from the retrieved datastoreURL
				if queryResult.Volumes[0].DatastoreUrl == "" {
					msg := fmt.Sprintf("could not retrieve datastore of volume: %q", volumeInfo.VolumeID.Id)
					log.Error(msg)
					return nil, status.Error(codes.Internal, msg)
				}
				datastoreAccessibleTopology = datastoreTopologyMap[queryResult.Volumes[0].DatastoreUrl]
				datastoreURL = queryResult.Volumes[0].DatastoreUrl
				log.Debugf("Volume: %s is provisioned on the datastore: %s ", volumeInfo.VolumeID.Id, datastoreURL)
			} else {
				msg := fmt.Sprintf("QueryVolume could not retrieve volume information for volume: %q", volumeInfo.VolumeID.Id)
				log.Error(msg)
				return nil, status.Error(codes.Internal, msg)
			}
		} else {
			// retrieve datastoreURL from placementResults
			datastoreAccessibleTopology = datastoreTopologyMap[volumeInfo.DatastoreURL]
			datastoreURL = volumeInfo.DatastoreURL
			log.Debugf("Volume: %s is provisioned on the datastore: %s ", volumeInfo.VolumeID.Id, datastoreURL)
		}
		if len(datastoreAccessibleTopology) > 0 {
			rand.Seed(time.Now().Unix())
			volumeAccessibleTopology = datastoreAccessibleTopology[rand.Intn(len(datastoreAccessibleTopology))]
			log.Debugf("volumeAccessibleTopology: [%+v] is selected for datastore: %s ", volumeAccessibleTopology, datastoreURL)
		}
	}
	if len(volumeAccessibleTopology) != 0 {
		volumeTopology := &csi.Topology{
			Segments: volumeAccessibleTopology,
		}
		resp.Volume.AccessibleTopology = append(resp.Volume.AccessibleTopology, volumeTopology)
	}
	return resp, nil
}

// createFileVolume creates a file volume based on the CreateVolumeRequest.
func (c *controller) createFileVolume(ctx context.Context, req *csi.CreateVolumeRequest) (
	*csi.CreateVolumeResponse, error) {
	log := logger.GetLogger(ctx)
	// ignore TopologyRequirement for file volume provisioning
	if req.GetAccessibilityRequirements() != nil {
		log.Info("Ignoring TopologyRequirement for file volume")
	}

	// Volume Size - Default is 10 GiB
	volSizeBytes := int64(common.DefaultGbDiskSize * common.GbInBytes)
	if req.GetCapacityRange() != nil && req.GetCapacityRange().RequiredBytes != 0 {
		volSizeBytes = int64(req.GetCapacityRange().GetRequiredBytes())
	}
	volSizeMB := int64(common.RoundUpSize(volSizeBytes, common.MbInBytes))

	// Fetching the feature state for csi-migration before parsing storage class params
	csiMigrationFeatureState := commonco.ContainerOrchestratorUtility.IsFSSEnabled(ctx, common.CSIMigration)
	scParams, err := common.ParseStorageClassParams(ctx, req.Parameters, csiMigrationFeatureState)
	if err != nil {
		msg := fmt.Sprintf("Parsing storage class parameters failed with error: %+v", err)
		log.Error(msg)
		return nil, status.Errorf(codes.InvalidArgument, msg)
	}

	var createVolumeSpec = common.CreateVolumeSpec{
		CapacityMB: volSizeMB,
		Name:       req.Name,
		ScParams:   scParams,
		VolumeType: common.FileVolumeType,
	}
	var volumeID string
	if commonco.ContainerOrchestratorUtility.IsFSSEnabled(ctx, common.CSIAuthCheck) {
		dsURLToInfoMap := c.authMgr.GetDatastoreMapForFileVolumes(ctx)
		log.Debugf("Filtered Datastores: %+v", dsURLToInfoMap)
		var filteredDatastores []*cnsvsphere.DatastoreInfo
		for _, datastore := range dsURLToInfoMap {
			filteredDatastores = append(filteredDatastores, datastore)
		}
		if len(filteredDatastores) == 0 {
			msg := "no datastores found to create file volume"
			log.Error(msg)
			return nil, status.Errorf(codes.Internal, msg)
		}
		volumeID, err = common.CreateFileVolumeUtil(ctx, cnstypes.CnsClusterFlavorVanilla,
			c.manager, &createVolumeSpec, filteredDatastores)
		if err != nil {
			msg := fmt.Sprintf("failed to create volume. Error: %+v", err)
			log.Error(msg)
			return nil, status.Errorf(codes.Internal, msg)
		}
	} else {
		volumeID, err = common.CreateFileVolumeUtilOld(ctx, cnstypes.CnsClusterFlavorVanilla, c.manager, &createVolumeSpec)
		if err != nil {
			msg := fmt.Sprintf("failed to create volume. Error: %+v", err)
			log.Error(msg)
			return nil, status.Errorf(codes.Internal, msg)
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

// CreateVolume is creating CNS Volume using volume request specified
// in CreateVolumeRequest
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
			return nil, status.Errorf(codes.InvalidArgument, "Volume capability not supported. Err: %+v", err)
		}
		if common.IsFileVolumeRequest(ctx, volumeCapabilities) {
			volumeType = prometheus.PrometheusFileVolumeType
			isvSANFileServicesSupported, err := c.manager.VcenterManager.IsvSANFileServicesSupported(ctx, c.manager.VcenterConfig.Host)
			if err != nil {
				log.Errorf("failed to verify if vSAN file services is supported or not. Error:%+v", err)
				return nil, status.Error(codes.Internal, err.Error())
			}
			if !isvSANFileServicesSupported {
				msg := "fileshare volume creation is not supported on vSAN 67u3 release"
				log.Error(msg)
				return nil, status.Error(codes.FailedPrecondition, msg)
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

// DeleteVolume is deleting CNS Volume specified in DeleteVolumeRequest
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
			// in-tree volume support
			if !commonco.ContainerOrchestratorUtility.IsFSSEnabled(ctx, common.CSIMigration) {
				// Migration feature switch is disabled
				msg := fmt.Sprintf("volume-migration feature switch is disabled. Cannot use volume with vmdk path :%q", req.VolumeId)
				log.Error(msg)
				return nil, status.Errorf(codes.Internal, msg)
			}
			// Migration feature switch is enabled
			volumePath = req.VolumeId
			// In case if feature state switch is enabled after controller is deployed, we need to initialize the volumeMigrationService
			if err := initVolumeMigrationService(ctx, c); err != nil {
				// Error is already wrapped in CSI error code
				return nil, err
			}
			req.VolumeId, err = volumeMigrationService.GetVolumeID(ctx, &migration.VolumeSpec{VolumePath: req.VolumeId}, false)
			if err != nil {
				msg := fmt.Sprintf("failed to get VolumeID from volumeMigrationService for volumePath: %q", volumePath)
				log.Error(msg)
				return nil, status.Errorf(codes.Internal, msg)
			}
		}
		// TODO: Add code to determine the volume type and set volumeType for Prometheus metric accordingly.
		err = common.DeleteVolumeUtil(ctx, c.manager.VolumeManager, req.VolumeId, true)
		if err != nil {
			msg := fmt.Sprintf("failed to delete volume: %q. Error: %+v", req.VolumeId, err)
			log.Error(msg)
			return nil, status.Errorf(codes.Internal, msg)
		}
		// Migration feature switch is enabled and volumePath is set
		if volumePath != "" {
			// Delete VolumePath to VolumeID mapping
			err = volumeMigrationService.DeleteVolumeInfo(ctx, req.VolumeId)
			if err != nil {
				msg := fmt.Sprintf("failed to delete volumeInfo CR for volume: %q. Error: %+v", req.VolumeId, err)
				log.Error(msg)
				return nil, status.Errorf(codes.Internal, msg)
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
// volume id and node name is retrieved from ControllerPublishVolumeRequest
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
			msg := fmt.Sprintf("Validation for PublishVolume Request: %+v has failed. Error: %v", *req, err)
			log.Error(msg)
			return nil, status.Errorf(codes.Internal, msg)
		}
		publishInfo := make(map[string]string)
		// Check whether its a block or file volume
		if common.IsFileVolumeRequest(ctx, []*csi.VolumeCapability{req.GetVolumeCapability()}) {
			volumeType = prometheus.PrometheusFileVolumeType
			// File Volume
			queryFilter := cnstypes.CnsQueryFilter{
				VolumeIds: []cnstypes.CnsVolumeId{{Id: req.VolumeId}},
			}
			querySelection := cnstypes.CnsQuerySelection{
				Names: []string{
					string(cnstypes.QuerySelectionNameTypeBackingObjectDetails),
				},
			}
			// Select only the backing object details.
			queryResult, err := c.manager.VolumeManager.QueryAllVolume(ctx, queryFilter, querySelection)
			if err != nil {
				msg := fmt.Sprintf("QueryVolume failed for volumeID: %q. %+v", req.VolumeId, err.Error())
				log.Error(msg)
				return nil, status.Error(codes.Internal, msg)
			}
			if len(queryResult.Volumes) == 0 {
				msg := fmt.Sprintf("volumeID %s not found in QueryVolume", req.VolumeId)
				log.Error(msg)
				return nil, status.Error(codes.Internal, msg)
			}

			vSANFileBackingDetails := queryResult.Volumes[0].BackingObjectDetails.(*cnstypes.CnsVsanFileShareBackingDetails)
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
				msg := fmt.Sprintf("failed to get NFSv4 access point for volume: %q."+
					" Returned vSAN file backing details : %+v", req.VolumeId, vSANFileBackingDetails)
				log.Error(msg)
				return nil, status.Errorf(codes.Internal, msg)
			}
		} else {
			// Block Volume
			volumeType = prometheus.PrometheusBlockVolumeType
			if strings.Contains(req.VolumeId, ".vmdk") {
				// in-tree volume support
				if !commonco.ContainerOrchestratorUtility.IsFSSEnabled(ctx, common.CSIMigration) {
					// Migration feature switch is disabled
					msg := fmt.Sprintf("volume-migration feature switch is disabled. Cannot use volume with vmdk path :%q", req.VolumeId)
					log.Error(msg)
					return nil, status.Errorf(codes.Internal, msg)
				}
				// Migration feature switch is enabled
				storagePolicyName := req.VolumeContext[common.AttributeStoragePolicyName]
				volumePath := req.VolumeId
				// In case if feature state switch is enabled after controller is deployed, we need to initialize the volumeMigrationService
				if err := initVolumeMigrationService(ctx, c); err != nil {
					// Error is already wrapped in CSI error code
					return nil, err
				}

				req.VolumeId, err = volumeMigrationService.GetVolumeID(ctx,
					&migration.VolumeSpec{VolumePath: volumePath, StoragePolicyName: storagePolicyName}, false)

				if err != nil {
					msg := fmt.Sprintf("failed to get VolumeID from volumeMigrationService for volumePath: %q", volumePath)
					log.Error(msg)
					return nil, status.Errorf(codes.Internal, msg)
				}
				err = volumeMigrationService.ProtectVolumeFromVMDeletion(ctx, req.VolumeId)
				if err != nil {
					msg := fmt.Sprintf("failed to set keepAfterDeleteVm control flag for VolumeID %q", req.VolumeId)
					return nil, status.Errorf(codes.Internal, msg)
				}
			}
			node, err := c.nodeMgr.GetNodeByName(ctx, req.NodeId)
			if err != nil {
				msg := fmt.Sprintf("failed to find VirtualMachine for node:%q. Error: %v", req.NodeId, err)
				log.Error(msg)
				return nil, status.Errorf(codes.Internal, msg)
			}
			log.Debugf("Found VirtualMachine for node:%q.", req.NodeId)
			diskUUID, err := common.AttachVolumeUtil(ctx, c.manager, node, req.VolumeId)
			if err != nil {
				msg := fmt.Sprintf("failed to attach disk: %+q with node: %q err %+v", req.VolumeId, req.NodeId, err)
				log.Error(msg)
				return nil, status.Errorf(codes.Internal, msg)
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

// ControllerUnpublishVolume detaches a volume from the Node VM.
// volume id and node name is retrieved from ControllerUnpublishVolumeRequest
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
			msg := fmt.Sprintf("Validation for UnpublishVolume Request: %+v has failed. Error: %v", *req, err)
			log.Error(msg)
			return nil, status.Errorf(codes.Internal, msg)
		}
		if !strings.Contains(req.VolumeId, ".vmdk") {
			// check if volume is block or file, skip detach for file volume
			queryFilter := cnstypes.CnsQueryFilter{
				VolumeIds: []cnstypes.CnsVolumeId{{Id: req.VolumeId}},
			}
			querySelection := cnstypes.CnsQuerySelection{
				Names: []string{
					string(cnstypes.QuerySelectionNameTypeVolumeType),
				},
			}
			// Select only the volume type.
			queryResult, err := c.manager.VolumeManager.QueryAllVolume(ctx, queryFilter, querySelection)
			if err != nil {
				msg := fmt.Sprintf("QueryVolume failed for volumeID: %q. %+v", req.VolumeId, err.Error())
				log.Error(msg)
				return nil, status.Error(codes.Internal, msg)
			}
			if len(queryResult.Volumes) == 0 {
				msg := fmt.Sprintf("volumeID %q not found in QueryVolume", req.VolumeId)
				log.Error(msg)
				return nil, status.Error(codes.Internal, msg)
			}
			if queryResult.Volumes[0].VolumeType == common.FileVolumeType {
				volumeType = prometheus.PrometheusFileVolumeType
				log.Infof("Skipping ControllerUnpublish for file volume %q", req.VolumeId)
				return &csi.ControllerUnpublishVolumeResponse{}, nil
			}
		} else {
			// in-tree volume support
			volumeType = prometheus.PrometheusBlockVolumeType
			if !commonco.ContainerOrchestratorUtility.IsFSSEnabled(ctx, common.CSIMigration) {
				// Migration feature switch is disabled
				msg := fmt.Sprintf("volume-migration feature switch is disabled. Cannot use volume with vmdk path :%q", req.VolumeId)
				log.Error(msg)
				return nil, status.Errorf(codes.Internal, msg)
			}
			// Migration feature switch is enabled
			// ControllerUnpublishVolume will never be the first call back for vmdk registration with CNS.
			// Here in the migration.VolumeSpec, we do not supply SPBM Policy name.
			// Node drain is the pre-requisite for volume migration, so volume will be registered with SPBM policy
			// during ControllerPublish if metadata-syncer fails to register volume using associated SPBM Policy.
			// for ControllerUnpublishVolume we anticipate volume is already registered with CNS, and volumeMigrationService
			// should return volumeID for requested VolumePath
			volumePath := req.VolumeId
			// In case if feature state switch is enabled after controller is deployed, we need to initialize the volumeMigrationService
			if err := initVolumeMigrationService(ctx, c); err != nil {
				// Error is already wrapped in CSI error code
				return nil, err
			}
			req.VolumeId, err = volumeMigrationService.GetVolumeID(ctx, &migration.VolumeSpec{VolumePath: volumePath}, false)
			if err != nil {
				msg := fmt.Sprintf("failed to get VolumeID from volumeMigrationService for volumePath: %q", volumePath)
				log.Error(msg)
				return nil, status.Errorf(codes.Internal, msg)
			}
		}
		// Block Volume
		volumeType = prometheus.PrometheusBlockVolumeType
		node, err := c.nodeMgr.GetNodeByName(ctx, req.NodeId)
		if err != nil {
			msg := fmt.Sprintf("failed to find VirtualMachine for node:%q. Error: %v", req.NodeId, err)
			log.Error(msg)
			return nil, status.Error(codes.Internal, msg)
		}
		err = common.DetachVolumeUtil(ctx, c.manager, node, req.VolumeId)
		if err != nil {
			msg := fmt.Sprintf("failed to detach disk: %+q from node: %q err %+v", req.VolumeId, req.NodeId, err)
			log.Error(msg)
			return nil, status.Error(codes.Internal, msg)
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
// volume id and size is retrieved from ControllerExpandVolumeRequest
func (c *controller) ControllerExpandVolume(ctx context.Context, req *csi.ControllerExpandVolumeRequest) (
	*csi.ControllerExpandVolumeResponse, error) {
	ctx = logger.NewContextWithLogger(ctx)
	log := logger.GetLogger(ctx)
	log.Infof("ControllerExpandVolume: called with args %+v", *req)

	if strings.Contains(req.VolumeId, ".vmdk") {
		msg := fmt.Sprintf("Cannot expand migrated vSphere volume. :%q", req.VolumeId)
		log.Error(msg)
		return nil, status.Errorf(codes.Unimplemented, msg)
	}

	isExtendSupported, err := c.manager.VcenterManager.IsExtendVolumeSupported(ctx, c.manager.VcenterConfig.Host)
	if err != nil {
		log.Errorf("failed to verify if extend volume is supported or not. Error: %+v", err)
		return nil, status.Error(codes.Internal, err.Error())
	}
	if !isExtendSupported {
		msg := "Volume Expansion is not supported in this vSphere release. Kindly upgrade to vSphere 7.0 for offline expansion and vSphere 7.0U2 for online expansion support."
		log.Error(msg)
		return nil, status.Error(codes.Internal, msg)
	}

	isOnlineExpansionSupported, err := c.manager.VcenterManager.IsOnlineExtendVolumeSupported(ctx, c.manager.VcenterConfig.Host)
	if err != nil {
		msg := fmt.Sprintf("failed to check if online expansion is supported due to error: %v", err)
		log.Error(msg)
		return nil, status.Errorf(codes.Internal, msg)
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

	err = common.ExpandVolumeUtil(ctx, c.manager, volumeID, volSizeMB)
	if err != nil {
		msg := fmt.Sprintf("failed to expand volume: %q to size: %d with error: %+v", volumeID, volSizeMB, err)
		log.Error(msg)
		return nil, status.Errorf(codes.Internal, msg)
	}

	// Always set nodeExpansionRequired to true, even if requested size is equal to current size.
	// Volume expansion may succeed on CNS but external-resizer may fail to update API server.
	// Requests are requeued in this case. Setting nodeExpandsionRequired to false marks PVC
	// resize as finished which prevents kubelet from expanding the filesystem.
	// Ref: https://github.com/kubernetes-csi/external-resizer/blob/master/pkg/controller/controller.go#L335
	nodeExpansionRequired := true
	// Node expansion is not required for raw block volumes
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
	return nil, status.Error(codes.Unimplemented, "")
}

func (c *controller) GetCapacity(ctx context.Context, req *csi.GetCapacityRequest) (
	*csi.GetCapacityResponse, error) {
	ctx = logger.NewContextWithLogger(ctx)
	log := logger.GetLogger(ctx)
	log.Infof("GetCapacity: called with args %+v", *req)
	return nil, status.Error(codes.Unimplemented, "")
}

// initVolumeMigrationService is a helper method to initialize volumeMigrationService in controller
func initVolumeMigrationService(ctx context.Context, c *controller) error {
	log := logger.GetLogger(ctx)
	// This check is to prevent unnecessary RLocks on the volumeMigration instance
	if volumeMigrationService != nil {
		return nil
	}
	// In case if feature state switch is enabled after controller is deployed, we need to initialize the volumeMigrationService
	var err error
	volumeMigrationService, err = migration.GetVolumeMigrationService(ctx, &c.manager.VolumeManager, c.manager.CnsConfig, false)
	if err != nil {
		msg := fmt.Sprintf("failed to get migration service. Err: %v", err)
		log.Error(msg)
		return status.Errorf(codes.Internal, msg)
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
	return nil, status.Error(codes.Unimplemented, "")
}

func (c *controller) DeleteSnapshot(ctx context.Context, req *csi.DeleteSnapshotRequest) (
	*csi.DeleteSnapshotResponse, error) {
	ctx = logger.NewContextWithLogger(ctx)
	log := logger.GetLogger(ctx)
	log.Infof("DeleteSnapshot: called with args %+v", *req)
	return nil, status.Error(codes.Unimplemented, "")
}

func (c *controller) ListSnapshots(ctx context.Context, req *csi.ListSnapshotsRequest) (
	*csi.ListSnapshotsResponse, error) {
	ctx = logger.NewContextWithLogger(ctx)
	log := logger.GetLogger(ctx)
	log.Infof("ListSnapshots: called with args %+v", *req)
	return nil, status.Error(codes.Unimplemented, "")
}
