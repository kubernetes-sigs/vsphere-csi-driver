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
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/container-storage-interface/spec/lib/go/csi"
	"github.com/fsnotify/fsnotify"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	cnstypes "github.com/vmware/govmomi/cns/types"
	"github.com/vmware/govmomi/object"
	"github.com/vmware/govmomi/units"
	"github.com/vmware/govmomi/vim25/types"
	"google.golang.org/grpc/codes"
	"google.golang.org/protobuf/types/known/timestamppb"
	apierrors "k8s.io/apimachinery/pkg/api/errors"

	"sigs.k8s.io/vsphere-csi-driver/v3/pkg/apis/migration"
	"sigs.k8s.io/vsphere-csi-driver/v3/pkg/common/cns-lib/node"
	cnsvolume "sigs.k8s.io/vsphere-csi-driver/v3/pkg/common/cns-lib/volume"
	cnsvsphere "sigs.k8s.io/vsphere-csi-driver/v3/pkg/common/cns-lib/vsphere"
	cnsconfig "sigs.k8s.io/vsphere-csi-driver/v3/pkg/common/config"
	csifault "sigs.k8s.io/vsphere-csi-driver/v3/pkg/common/fault"
	"sigs.k8s.io/vsphere-csi-driver/v3/pkg/common/prometheus"
	"sigs.k8s.io/vsphere-csi-driver/v3/pkg/common/utils"
	"sigs.k8s.io/vsphere-csi-driver/v3/pkg/csi/service/common"
	"sigs.k8s.io/vsphere-csi-driver/v3/pkg/csi/service/common/commonco"
	commoncotypes "sigs.k8s.io/vsphere-csi-driver/v3/pkg/csi/service/common/commonco/types"
	"sigs.k8s.io/vsphere-csi-driver/v3/pkg/csi/service/common/placementengine"
	"sigs.k8s.io/vsphere-csi-driver/v3/pkg/csi/service/logger"
	csitypes "sigs.k8s.io/vsphere-csi-driver/v3/pkg/csi/types"
	"sigs.k8s.io/vsphere-csi-driver/v3/pkg/internalapis/cnsvolumeinfo"
	"sigs.k8s.io/vsphere-csi-driver/v3/pkg/internalapis/cnsvolumeoperationrequest"
)

// NodeManagerInterface provides functionality to manage (VM) nodes.
type NodeManagerInterface interface {
	Initialize(ctx context.Context) error
	GetSharedDatastoresInK8SCluster(ctx context.Context) ([]*cnsvsphere.DatastoreInfo, error)
	GetNodeVMByNameAndUpdateCache(ctx context.Context, nodeName string) (*cnsvsphere.VirtualMachine, error)
	GetNodeVMByNameOrUUID(ctx context.Context, nodeName string) (*cnsvsphere.VirtualMachine, error)
	GetNodeNameByUUID(ctx context.Context, nodeUUID string) (string, error)
	GetNodeVMByUuid(ctx context.Context, nodeUuid string) (*cnsvsphere.VirtualMachine, error)
	GetAllNodes(ctx context.Context) ([]*cnsvsphere.VirtualMachine, error)
	GetAllNodesByVC(ctx context.Context, vcHost string) ([]*cnsvsphere.VirtualMachine, error)
}

type controller struct {
	// Deprecated
	// To be removed after multi vCenter support is added
	manager  *common.Manager
	managers *common.Managers
	nodeMgr  NodeManagerInterface
	// Deprecated
	// To be removed after multi vCenter support is added
	authMgr     common.AuthorizationService
	authMgrs    map[string]*common.AuthManager
	topologyMgr commoncotypes.ControllerTopologyService
}

var (
	// volumeMigrationService holds the pointer to VolumeMigration instance.
	volumeMigrationService migration.VolumeMigrationService

	// volumeInfoService holds the pointer to VolumeInfo service instance
	// This will hold mapping for VolumeID to vCenter for multi vCenter CSI topology deployment
	volumeInfoService cnsvolumeinfo.VolumeInfoService

	// The following variables hold feature states for multi-vcenter-csi-topology, CSI Migration
	// and authorisation check.
	multivCenterCSITopologyEnabled, csiMigrationEnabled, filterSuspendedDatastores,
	isTopologyAwareFileVolumeEnabled, isCSITransactionSupportEnabled bool

	// variables for list volumes
	volIDsInK8s             = make([]string, 0)
	CNSVolumesforListVolume = make([]cnstypes.CnsVolume, 0)

	// errAllDSFilteredOut is an error thrown by auth service when it
	// filters out all the potential shared datastores in a volume provisioning call.
	errAllDSFilteredOut = errors.New("auth service could not find datastore for block volume provisioning")

	// variable for list snapshots
	CNSSnapshotsForListSnapshots = make([]cnstypes.CnsSnapshotQueryResultEntry, 0)
	CNSVolumeDetailsMap          = make([]map[string]*utils.CnsVolumeDetails, 0)
	volumeIDToNodeUUIDMap        = make(map[string]string)
)

// New creates a CNS controller.
func New() csitypes.CnsController {
	return &controller{}
}

// Init is initializing controller struct.
func (c *controller) Init(config *cnsconfig.Config, version string) error {
	ctx, log := logger.GetNewContextWithLogger()
	log.Infof("Initializing CNS controller")
	var err error
	var operationStore cnsvolumeoperationrequest.VolumeOperationRequest
	operationStore, err = cnsvolumeoperationrequest.InitVolumeOperationRequestInterface(ctx,
		config.Global.CnsVolumeOperationRequestCleanupIntervalInMin,
		func() bool {
			return commonco.ContainerOrchestratorUtility.IsFSSEnabled(ctx, common.BlockVolumeSnapshot)
		}, false)
	if err != nil {
		log.Errorf("failed to initialize VolumeOperationRequestInterface with error: %v", err)
		return err
	}
	// Check if the feature states are enabled.
	multivCenterCSITopologyEnabled = commonco.ContainerOrchestratorUtility.IsFSSEnabled(ctx,
		common.MultiVCenterCSITopology)
	csiMigrationEnabled = commonco.ContainerOrchestratorUtility.IsFSSEnabled(ctx, common.CSIMigration)
	filterSuspendedDatastores = commonco.ContainerOrchestratorUtility.IsFSSEnabled(ctx,
		common.CnsMgrSuspendCreateVolume)
	isTopologyAwareFileVolumeEnabled = commonco.ContainerOrchestratorUtility.IsFSSEnabled(ctx,
		common.TopologyAwareFileVolume)
	isCSITransactionSupportEnabled = commonco.ContainerOrchestratorUtility.IsFSSEnabled(ctx, common.CSITranSactionSupport)

	vcManager := cnsvsphere.GetVirtualCenterManager(ctx)
	if !multivCenterCSITopologyEnabled {
		// Get VirtualCenterInstance and validate version.
		vc, err := cnsvsphere.GetVirtualCenterInstance(ctx, &cnsconfig.ConfigurationInfo{Cfg: config}, false)
		if err != nil {
			log.Errorf("failed to get vCenter Instance. err=%v", err)
			return err
		}
		vcenterconfig, err := cnsvsphere.GetVirtualCenterConfig(ctx, config)
		// force loading latest vCenter config at the time of creating new VC client
		vcenterconfig.ReloadVCConfigForNewClient = true
		if err != nil {
			log.Errorf("failed to get VirtualCenterConfig. err=%v", err)
			return err
		}
		vc.Config = vcenterconfig
		volumeManager, err := cnsvolume.GetManager(ctx, vc, operationStore,
			true, false,
			false, cnstypes.CnsClusterFlavorVanilla)
		if err != nil {
			return logger.LogNewErrorf(log, "failed to create an instance of volume manager. err=%v", err)
		}

		c.manager = &common.Manager{
			VcenterConfig:  vcenterconfig,
			CnsConfig:      config,
			VolumeManager:  volumeManager,
			VcenterManager: vcManager,
		}
		// Check vCenter API Version
		err = common.CheckAPI(ctx, vc.Client.ServiceContent.About.ApiVersion, common.MinSupportedVCenterMajor,
			common.MinSupportedVCenterMinor, common.MinSupportedVCenterPatch)
		if err != nil {
			log.Errorf("checkAPI failed for vcenter API version: %s, err=%v",
				vc.Client.ServiceContent.About.ApiVersion, err)
			return err
		}
		log.Info("loading AuthorizationService")
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
	} else {
		// Multi vCenter feature enabled
		c.managers = &common.Managers{
			CnsConfig:      config,
			VcenterManager: vcManager,
		}
		c.managers.VcenterConfigs = make(map[string]*cnsvsphere.VirtualCenterConfig)
		c.managers.VolumeManagers = make(map[string]cnsvolume.Manager)
		// Get VirtualCenterManager instance and validate version.
		vcenterconfigs, err := cnsvsphere.GetVirtualCenterConfigs(ctx, config)
		if err != nil {
			return logger.LogNewErrorf(log, "failed to get VirtualCenterConfigs. err=%v", err)
		}
		var multivCenterTopologyDeployment bool
		if len(vcenterconfigs) > 1 {
			multivCenterTopologyDeployment = true
		}
		for _, vcenterconfig := range vcenterconfigs {
			vcenterconfig.ReloadVCConfigForNewClient = true
			vcenter, err := cnsvsphere.GetVirtualCenterInstanceForVCenterConfig(ctx, vcenterconfig, false)
			if err != nil {
				return logger.LogNewErrorf(log, "failed to get vCenterInstance for vCenter %q"+
					"err=%v", vcenterconfig.Host, err)
			}
			c.managers.VcenterConfigs[vcenterconfig.Host] = vcenterconfig
			volumeManager, err := cnsvolume.GetManager(ctx, vcenter,
				operationStore, true, true,
				multivCenterTopologyDeployment, cnstypes.CnsClusterFlavorVanilla)
			if err != nil {
				return logger.LogNewErrorf(log, "failed to create an instance of volume manager. err=%v", err)
			}
			c.managers.VolumeManagers[vcenterconfig.Host] = volumeManager
		}
		vCenters, err := common.GetVCenters(ctx, c.managers)
		if err != nil {
			return logger.LogNewErrorf(log, "failed to get vcenters. err=%v", err)
		}
		// Check vCenter API Version
		for _, vc := range vCenters {
			err = common.CheckAPI(ctx, vc.Client.ServiceContent.About.ApiVersion, common.MinSupportedVCenterMajor,
				common.MinSupportedVCenterMinor, common.MinSupportedVCenterPatch)
			if err != nil {
				return logger.LogNewErrorf(log, "checkAPI failed for vcenter API version: %s for vCenter %s, err=%v",
					vc.Client.ServiceContent.About.ApiVersion, vc.Config.Host, err)
			}
		}

		log.Info("loading AuthorizationService")
		authMgrs, err := common.GetAuthorizationServices(ctx, vCenters)
		if err != nil {
			return logger.LogNewErrorf(log, "failed to initialize authMgr. err=%v", err)
		}
		c.authMgrs = authMgrs
		for _, authMgr := range authMgrs {
			go common.ComputeDatastoreMapForBlockVolumes(authMgr, config.Global.CSIAuthCheckIntervalInMin)
		}
		var vsanFileServiceNotSupported bool
		for _, vcconfig := range c.managers.VcenterConfigs {
			isvSANFileServicesSupported, err := c.managers.VcenterManager.IsvSANFileServicesSupported(ctx,
				vcconfig.Host)
			if err != nil {
				return logger.LogNewErrorf(log, "failed to verify if vSAN file services is supported or not for vCenter: %s. "+
					"Error:%+v", vcconfig.Host, err)
			}
			if !isvSANFileServicesSupported {
				vsanFileServiceNotSupported = true
				break
			}
		}
		if vsanFileServiceNotSupported {
			return logger.LogNewErrorf(log, "vSAN file service is not supported in one or more vCenter(s)")
		}
		for _, vcconfig := range c.managers.VcenterConfigs {
			go common.ComputeFSEnabledClustersToDsMap(authMgrs[vcconfig.Host], config.Global.CSIAuthCheckIntervalInMin)
		}
		if multivCenterTopologyDeployment {
			log.Info("Loading CnsVolumeInfo Service to persist mapping for VolumeID to vCenter")
			volumeInfoService, err = cnsvolumeinfo.InitVolumeInfoService(ctx)
			if err != nil {
				return logger.LogNewErrorf(log, "failed to load volumeInfoService service. Err: %v", err)
			}
			if volumeInfoService != nil {
				log.Infof("Successfully initialized VolumeInfoService")
			}
		}
	}

	c.nodeMgr = &node.Nodes{}
	err = c.nodeMgr.Initialize(ctx)
	if err != nil {
		log.Errorf("failed to initialize nodeMgr. err=%v", err)
		return err
	}

	go cnsvolume.ClearInvalidTasksFromListView(multivCenterCSITopologyEnabled)
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
					reloadConfigErr := c.ReloadConfiguration()
					if reloadConfigErr == nil {
						log.Infof("Successfully reloaded configuration from: %q", cfgPath)
					} else {
						log.Errorf("failed to reload configuration. err: %+v", reloadConfigErr)
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
		if !multivCenterCSITopologyEnabled {
			log.Info("CSI Migration Feature is Enabled. Loading Volume Migration Service")
			volumeMigrationService, err = migration.GetVolumeMigrationService(ctx, &c.manager.VolumeManager, config, false)
			if err != nil {
				log.Errorf("failed to get migration service. Err: %v", err)
				return err
			}
		} else {
			if len(c.managers.VcenterConfigs) == 1 {
				log.Info("CSI Migration Feature is Enabled. Loading Volume Migration Service")
				volumeManager := c.managers.VolumeManagers[c.managers.CnsConfig.Global.VCenterIP]
				volumeMigrationService, err = migration.GetVolumeMigrationService(ctx, &volumeManager, config, false)
				if err != nil {
					log.Errorf("failed to get migration service. Err: %v", err)
					return err
				}
			} else {
				log.Infof("vSphere CSI Migration is not supported on the multi vCenter setup")
			}
		}
	}

	// Create dynamic informer for CSINodeTopology instance if FSS is enabled.
	// Initialize volume topology service.
	c.topologyMgr, err = commonco.ContainerOrchestratorUtility.InitTopologyServiceInController(ctx)
	if err != nil {
		log.Errorf("failed to initialize topology service. Error: %+v", err)
		return err
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
	newCfg, err := cnsconfig.GetConfig(ctx)
	if err != nil {
		return logger.LogNewErrorf(log, "failed to read config. Error: %+v", err)
	}
	if multivCenterCSITopologyEnabled {
		newVcenterConfigs, err := cnsvsphere.GetVirtualCenterConfigs(ctx, newCfg)
		if err != nil {
			return logger.LogNewErrorf(log, "failed to get VirtualCenterConfigs. err=%v", err)
		}
		for _, newVCConfig := range newVcenterConfigs {
			newVCConfig.ReloadVCConfigForNewClient = true
			if c.managers.VolumeManagers[newVCConfig.Host] == nil {
				log.Infof("Observed new vCenter server: %q in the config secret. "+
					"Exiting vSphere CSI Controller Container for re-initialization", newVCConfig.Host)
				unregisterAllvCenterErr := c.managers.VcenterManager.UnregisterAllVirtualCenters(ctx)
				if unregisterAllvCenterErr != nil {
					log.Warnf("failed to Unregister all vCenter servers. Error: %v. "+
						"Proceeding to exit the vSphere CSI Controller Container for re-initialization",
						unregisterAllvCenterErr)
				}
				os.Exit(1)
			}
			var vcenter *cnsvsphere.VirtualCenter
			vcenter, err = cnsvsphere.GetVirtualCenterInstanceForVCenterHost(ctx, newVCConfig.Host, false)
			if err != nil {
				return logger.LogNewErrorf(log, "failed to get VirtualCenter. err=%v", err)
			}
			vcenter.Config = newVCConfig
			err := c.managers.VolumeManagers[newVCConfig.Host].ResetManager(ctx, vcenter)
			if err != nil {
				return logger.LogNewErrorf(log, "failed to reset updated VC object in volumemanager for vCenter: %q "+
					"err=%v", newVCConfig.Host, err)
			}
			c.managers.VcenterConfigs[newVCConfig.Host] = newVCConfig
			c.authMgrs[newVCConfig.Host].ResetvCenterInstance(ctx, vcenter)
		}
		if newCfg != nil {
			c.managers.CnsConfig = newCfg
			log.Debugf("Updated managers.CnsConfig")
		}
		// Re-Initialize Node Manager to cache latest vCenter config.
		log.Debug("Re-Initializing node manager")
		c.nodeMgr = &node.Nodes{}
		err = c.nodeMgr.Initialize(ctx)
		if err != nil {
			log.Errorf("failed to re-initialize nodeMgr. err=%v", err)
			return err
		}
	} else {
		// multivCenterCSITopology feature is disabled
		oldvCenter := c.manager.VcenterConfig.Host
		newVCConfig, err := cnsvsphere.GetVirtualCenterConfig(ctx, newCfg)
		if err != nil {
			log.Errorf("failed to get VirtualCenterConfig. err=%v", err)
			return err
		}
		if newVCConfig != nil {
			newVCConfig.ReloadVCConfigForNewClient = true
			if oldvCenter != newVCConfig.Host {
				log.Infof("Observed new vCenter server: %q in the config secret. "+
					"Exiting vSphere CSI Controller Container for re-initialization", newVCConfig.Host)
				unregisterAllvCenterErr := c.managers.VcenterManager.UnregisterAllVirtualCenters(ctx)
				if unregisterAllvCenterErr != nil {
					log.Warnf("failed to Unregister all vCenter servers. Error: %v. "+
						"Proceeding to exit the vSphere CSI Controller Container for re-initialization",
						unregisterAllvCenterErr)
				}
				os.Exit(1)
			}

			var vcenter *cnsvsphere.VirtualCenter
			vcenter, err = cnsvsphere.GetVirtualCenterInstance(ctx, &cnsconfig.ConfigurationInfo{Cfg: newCfg}, false)
			if err != nil {
				return logger.LogNewErrorf(log, "failed to get VirtualCenter. err=%v", err)
			}
			vcenter.Config = newVCConfig
			if c.authMgr != nil {
				c.authMgr.ResetvCenterInstance(ctx, vcenter)
				log.Info("Updated vCenter in auth manager")
			}
			err = c.manager.VolumeManager.ResetManager(ctx, vcenter)
			if err != nil {
				return logger.LogNewErrorf(log, "failed to reset volume manager. err=%v", err)
			}
			c.manager.VcenterConfig = newVCConfig
			// Re-Initialize Node Manager to cache latest vCenter config.
			c.nodeMgr = &node.Nodes{}
			err = c.nodeMgr.Initialize(ctx)
			if err != nil {
				log.Errorf("failed to re-initialize nodeMgr. err=%v", err)
				return err
			}
		}
		if newCfg != nil {
			c.manager.CnsConfig = newCfg
			log.Debugf("Updated manager.CnsConfig")
		}
	}
	return nil
}

func (c *controller) filterDatastores(ctx context.Context, sharedDatastores []*cnsvsphere.DatastoreInfo,
	vcHost string) ([]*cnsvsphere.DatastoreInfo, error) {
	log := logger.GetLogger(ctx)
	var dsMap map[string]*cnsvsphere.DatastoreInfo
	if multivCenterCSITopologyEnabled {
		dsMap = c.authMgrs[vcHost].GetDatastoreMapForBlockVolumes(ctx)
	} else {
		dsMap = c.authMgr.GetDatastoreMapForBlockVolumes(ctx)
	}
	if len(dsMap) == 0 {
		return nil, logger.LogNewError(log,
			"auth service: no shared datastore found for block volume provisioning")
	}
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
	if len(filteredDatastores) == 0 {
		return nil, errAllDSFilteredOut
	}
	return filteredDatastores, nil
}

// createBlockVolume creates a block volume based on the CreateVolumeRequest.
func (c *controller) createBlockVolume(ctx context.Context, req *csi.CreateVolumeRequest) (
	*csi.CreateVolumeResponse, string, error) {
	log := logger.GetLogger(ctx)
	// Volume Size - Default is 10 GiB.
	volSizeBytes := int64(common.DefaultGbDiskSize * common.GbInBytes)
	if req.GetCapacityRange() != nil && req.GetCapacityRange().RequiredBytes != 0 {
		volSizeBytes = int64(req.GetCapacityRange().GetRequiredBytes())
	}
	volSizeMB := int64(common.RoundUpSize(volSizeBytes, common.MbInBytes))

	// Check if the feature states are enabled.
	isBlockVolumeSnapshotEnabled := commonco.ContainerOrchestratorUtility.IsFSSEnabled(ctx, common.BlockVolumeSnapshot)
	csiMigrationFeatureState := commonco.ContainerOrchestratorUtility.IsFSSEnabled(ctx, common.CSIMigration)

	// Check if requested volume size and source snapshot size matches
	volumeSource := req.GetVolumeContentSource()
	var contentSourceSnapshotID string
	if isBlockVolumeSnapshotEnabled && volumeSource != nil {
		isCnsSnapshotSupported, err := c.manager.VcenterManager.IsCnsSnapshotSupported(ctx,
			c.manager.VcenterConfig.Host)
		if err != nil {
			return nil, csifault.CSIInternalFault, logger.LogNewErrorCodef(log, codes.Internal,
				"failed to check if cns snapshot operations are supported on VC due to error: %v", err)
		}
		if !isCnsSnapshotSupported {
			return nil, csifault.CSIUnimplementedFault, logger.LogNewErrorCode(log, codes.Unimplemented,
				"VC version does not support snapshot operations")
		}
		sourceSnapshot := volumeSource.GetSnapshot()
		if sourceSnapshot == nil {
			return nil, csifault.CSIInvalidArgumentFault,
				logger.LogNewErrorCode(log, codes.InvalidArgument, "unsupported VolumeContentSource type")
		}
		contentSourceSnapshotID = sourceSnapshot.GetSnapshotId()

		cnsVolumeID, _, err := common.ParseCSISnapshotID(contentSourceSnapshotID)
		if err != nil {
			return nil, csifault.CSIInvalidArgumentFault,
				logger.LogNewErrorCode(log, codes.InvalidArgument, err.Error())
		}
		// Query capacity in MB and datastore url for block volume snapshot
		volumeIds := []cnstypes.CnsVolumeId{{Id: cnsVolumeID}}
		cnsVolumeDetailsMap, err := utils.QueryVolumeDetailsUtil(ctx, c.manager.VolumeManager, volumeIds)
		if err != nil {
			log.Errorf("failed to retrieve the volume: %s details. err: %+v", cnsVolumeID, err)
			return nil, csifault.CSIInternalFault, err
		}
		if _, ok := cnsVolumeDetailsMap[cnsVolumeID]; !ok {
			return nil, csifault.CSIInternalFault, logger.LogNewErrorCodef(log, codes.Internal,
				"cns query volume did not return the volume: %s", cnsVolumeID)
		}
		snapshotSizeInMB := cnsVolumeDetailsMap[cnsVolumeID].SizeInMB
		snapshotSizeInBytes := snapshotSizeInMB * common.MbInBytes
		if volSizeBytes != snapshotSizeInBytes {
			return nil, csifault.CSIInvalidArgumentFault, logger.LogNewErrorCodef(log, codes.InvalidArgument,
				"size mismatches, requested volume size %d and source snapshot size %d."+
					"Volume resizing while restoring from snapshot is currently unsupported.",
				volSizeBytes, snapshotSizeInBytes)
		}
	}
	// Fetching the feature state for csi-migration before parsing storage class
	// params.
	scParams, err := common.ParseStorageClassParams(ctx, req.Parameters, csiMigrationFeatureState)
	// TODO: Need to figure out the fault returned by ParseStorageClassParams.
	// Currently, just return "csi.fault.Internal".
	if err != nil {
		return nil, csifault.CSIInternalFault, logger.LogNewErrorCodef(log, codes.InvalidArgument,
			"parsing storage class parameters failed with error: %+v", err)
	}

	if csiMigrationFeatureState && scParams.CSIMigration == "true" {
		if len(scParams.Datastore) != 0 {
			log.Infof("Converting datastore name: %q to Datastore URL", scParams.Datastore)
			// Get vCenter.
			// Need to extract fault from err returned by GetVirtualCenter.
			// Currently, just return "csi.fault.Internal".
			vCenter, err := cnsvsphere.GetVirtualCenterManager(ctx).GetVirtualCenter(ctx, c.manager.VcenterConfig.Host)
			if err != nil {
				return nil, csifault.CSIInternalFault, logger.LogNewErrorCodef(log, codes.Internal,
					"failed to get vCenter. err: %+v", err)
			}
			dcList, err := vCenter.GetDatacenters(ctx)
			// Need to extract fault from err returned by GetDatacenters.
			// Currently, just return "csi.fault.Internal".
			if err != nil {
				return nil, csifault.CSIInternalFault, logger.LogNewErrorCodef(log, codes.Internal,
					"failed to get datacenter list. err: %+v", err)
			}
			foundDatastoreURL := false
			for _, dc := range dcList {
				dsURLTodsInfoMap, err := dc.GetAllDatastores(ctx)
				// Need to extract fault from err returned by GetAllDatastores.
				// Currently, just return "csi.fault.Internal".
				if err != nil {
					return nil, csifault.CSIInternalFault, logger.LogNewErrorCodef(log, codes.Internal,
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
				return nil, csifault.CSIInternalFault, logger.LogNewErrorCodef(log, codes.Internal,
					"failed to find datastoreURL for datastore name: %q", scParams.Datastore)
			}
		} else if c.manager.VcenterConfig.MigrationDataStoreURL != "" {
			scParams.DatastoreURL = c.manager.VcenterConfig.MigrationDataStoreURL
		}
	}

	var createVolumeSpec = common.CreateVolumeSpec{
		CapacityMB:              volSizeMB,
		Name:                    req.Name,
		ScParams:                scParams,
		VolumeType:              common.BlockVolumeType,
		ContentSourceSnapshotID: contentSourceSnapshotID,
	}

	// Check if vCenter task for this volume is already registered as part of
	// improved idempotency CR
	log.Debugf("Checking if vCenter task for volume %s is already registered.", req.Name)
	var (
		volTaskAlreadyRegistered bool
		volumeInfo               *cnsvolume.CnsVolumeInfo
		faultType                string
		vcenter                  *cnsvsphere.VirtualCenter
	)
	// Get VirtualCenter instance
	vcenter, err = c.manager.VcenterManager.GetVirtualCenter(ctx, c.manager.VcenterConfig.Host)
	if err != nil {
		return nil, csifault.CSIInternalFault, logger.LogNewErrorCodef(log, codes.Internal,
			"failed to get vCenter. Err: %v", err)
	}
	// Get operation store
	operationStore := c.manager.VolumeManager.GetOperationStore()
	if operationStore == nil {
		return nil, csifault.CSIInternalFault, logger.LogNewErrorCodef(log, codes.Internal,
			"Operation store cannot be nil")
	}

	volumeOperationDetails, err := operationStore.GetRequestDetails(ctx, req.Name)
	if err != nil {
		if apierrors.IsNotFound(err) {
			log.Debugf("CreateVolume task details for block volume %s are not found.", req.Name)
		} else {
			return nil, csifault.CSIInternalFault, logger.LogNewErrorCodef(log, codes.Internal,
				"Error occurred while getting CreateVolume task details for block volume %s, err: %+v",
				req.Name, err)
		}
	} else if volumeOperationDetails.OperationDetails != nil {
		if volumeOperationDetails.OperationDetails.TaskStatus ==
			cnsvolumeoperationrequest.TaskInvocationStatusSuccess &&
			volumeOperationDetails.VolumeID != "" {
			// If task status is successful for this volume, then it means that volume is
			// already created and there is no need to create it again.
			log.Infof("Volume with name %q and id %q is already created on CNS with opId: %q.",
				req.Name, volumeOperationDetails.VolumeID, volumeOperationDetails.OperationDetails.OpID)

			volumeInfo = &cnsvolume.CnsVolumeInfo{
				DatastoreURL: "",
				VolumeID: cnstypes.CnsVolumeId{
					Id: volumeOperationDetails.VolumeID,
				},
			}
			volTaskAlreadyRegistered = true
		} else if cnsvolume.IsTaskPending(volumeOperationDetails) {
			// If task is created in CNS for this volume but task is in progress, then
			// we need to monitor the task to check if volume creation is completed or not.
			log.Infof("Volume with name %s has CreateVolume task %s pending on CNS.",
				req.Name, volumeOperationDetails.OperationDetails.TaskID)

			taskMoRef := types.ManagedObjectReference{
				Type:  "Task",
				Value: volumeOperationDetails.OperationDetails.TaskID,
			}
			task := object.NewTask(vcenter.Client.Client, taskMoRef)

			defer func() {
				// Persist the operation details before returning. Only success or error
				// needs to be stored as InProgress details are stored when the task is
				// created on CNS.
				if volumeOperationDetails != nil && volumeOperationDetails.OperationDetails != nil &&
					volumeOperationDetails.OperationDetails.TaskStatus !=
						cnsvolumeoperationrequest.TaskInvocationStatusInProgress {
					err := operationStore.StoreRequestDetails(ctx, volumeOperationDetails)
					if err != nil {
						log.Warnf("failed to store CreateVolume details with error: %v", err)
					}
				}
			}()

			volumeInfo, faultType, err = c.manager.VolumeManager.MonitorCreateVolumeTask(ctx,
				&volumeOperationDetails, task, req.Name, c.manager.CnsConfig.Global.ClusterID)
			if err != nil {
				return nil, faultType, logger.LogNewErrorCodef(log, codes.Internal,
					"failed to monitor task for volume %s. Error: %+v", req.Name, err)
			}
			volTaskAlreadyRegistered = true
		}
	}

	var (
		sharedDatastores    []*cnsvsphere.DatastoreInfo
		topologyRequirement *csi.TopologyRequirement
	)
	// Get accessibility.
	topologyRequirement = req.GetAccessibilityRequirements()
	if !volTaskAlreadyRegistered {
		if topologyRequirement != nil {
			// Check if topology domains have been provided in the vSphere CSI config secret.
			// NOTE: We do not support kubernetes.io/hostname as a topology label.
			if c.manager.CnsConfig.Labels.TopologyCategories == "" && c.manager.CnsConfig.Labels.Zone == "" &&
				c.manager.CnsConfig.Labels.Region == "" {
				return nil, csifault.CSIInvalidArgumentFault, logger.LogNewErrorCode(log, codes.InvalidArgument,
					"topology category names not specified in the vsphere config secret")
			}

			// Get shared accessible datastores for matching topology requirement.
			if commonco.ContainerOrchestratorUtility.IsFSSEnabled(ctx, common.TopologyPreferentialDatastores) {
				sharedDatastores, err = c.topologyMgr.GetSharedDatastoresInTopology(ctx,
					commoncotypes.VanillaTopologyFetchDSParams{
						TopologyRequirement: topologyRequirement,
						Vc:                  vcenter,
						StoragePolicyName:   scParams.StoragePolicyName,
					})
				if err != nil || len(sharedDatastores) == 0 {
					return nil, csifault.CSIInternalFault, logger.LogNewErrorCodef(log, codes.Internal,
						"failed to get shared datastores for topology requirement: %+v. Error: %+v",
						topologyRequirement, err)
				}
			} else {
				sharedDatastores, err = c.topologyMgr.GetSharedDatastoresInTopology(ctx,
					commoncotypes.VanillaTopologyFetchDSParams{TopologyRequirement: topologyRequirement})
				if err != nil || len(sharedDatastores) == 0 {
					return nil, csifault.CSIInternalFault, logger.LogNewErrorCodef(log, codes.Internal,
						"failed to get shared datastores for topology requirement: %+v. Error: %+v",
						topologyRequirement, err)
				}
			}
			log.Debugf("Shared datastores [%+v] retrieved for topologyRequirement [%+v]", sharedDatastores,
				topologyRequirement)
		} else {
			sharedDatastores, err = c.nodeMgr.GetSharedDatastoresInK8SCluster(ctx)
			if err != nil || len(sharedDatastores) == 0 {
				return nil, csifault.CSIInternalFault, logger.LogNewErrorCodef(log, codes.Internal,
					"failed to get shared datastores in kubernetes cluster. Error: %+v", err)
			}
			if len(sharedDatastores) == 0 {
				return nil, csifault.CSIInternalFault, logger.LogNewErrorCode(log, codes.Internal,
					"No datastore found for volume provisioning.")
			}
		}

		// Filter datastores which in datastoreMap from sharedDatastores.
		sharedDatastores, err = c.filterDatastores(ctx, sharedDatastores, c.manager.VcenterConfig.Host)
		if err != nil {
			return nil, csifault.CSIInternalFault, logger.LogNewErrorCodef(log, codes.Internal,
				"failed to create volume. Error: %+v", err)
		}

		volumeInfo, faultType, err = common.CreateBlockVolumeUtil(ctx, cnstypes.CnsClusterFlavorVanilla,
			c.manager, &createVolumeSpec, sharedDatastores,
			common.CreateBlockVolumeOptions{
				FilterSuspendedDatastores: filterSuspendedDatastores,
			},
			nil)
		if err != nil {
			return nil, faultType, logger.LogNewErrorCodef(log, codes.Internal,
				"failed to create volume. Error: %+v", err)
		}
	}

	attributes := make(map[string]string)
	attributes[common.AttributeDiskType] = common.DiskTypeBlockVolume
	if csiMigrationFeatureState && scParams.CSIMigration == "true" {
		// In case if feature state switch is enabled after controller is
		// deployed, we need to initialize the volumeMigrationService.
		if err := initVolumeMigrationService(ctx, c); err != nil {
			// Error is already wrapped in CSI error code.
			return nil, csifault.CSIInternalFault, err
		}
		// Return InitialVolumeFilepath in the response for TranslateCSIPVToInTree.
		volumePath, err := volumeMigrationService.GetVolumePath(ctx, volumeInfo.VolumeID.Id)
		if err != nil {
			return nil, csifault.CSIInternalFault, logger.LogNewErrorCodef(log, codes.Internal,
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

	// For topology aware provisioning, populate the topology segments parameter
	// in the CreateVolumeResponse struct.
	if topologyRequirement != nil {
		var (
			datastoreAccessibleTopology []map[string]string
			allNodeVMs                  []*cnsvsphere.VirtualMachine
		)

		// Retrieve the datastoreURL of the Provisioned Volume. If CNS CreateVolume
		// API does not return datastoreURL, retrieve this by calling QueryVolume.
		// Otherwise, retrieve this from PlacementResults in the response of
		// CreateVolume API.
		datastoreURL := volumeInfo.DatastoreURL
		if datastoreURL == "" {
			volumeIds := []cnstypes.CnsVolumeId{{Id: volumeInfo.VolumeID.Id}}
			queryFilter := cnstypes.CnsQueryFilter{
				VolumeIds: volumeIds,
			}

			querySelection := cnstypes.CnsQuerySelection{
				Names: []string{string(cnstypes.QuerySelectionNameTypeDataStoreUrl)},
			}

			queryResult, err := utils.QueryVolumeUtil(ctx, c.manager.VolumeManager, queryFilter, &querySelection, true)
			if err != nil {
				// TODO: QueryVolume need to return faultType.
				// Need to return faultType which is returned from QueryVolume.
				// Currently, just return "csi.fault.Internal".
				return nil, csifault.CSIInternalFault, logger.LogNewErrorCodef(log, codes.Internal,
					"queryVolumeUtil failed for volumeID: %s, err: %+v", volumeInfo.VolumeID.Id, err)
			}
			if len(queryResult.Volumes) == 0 || queryResult.Volumes[0].DatastoreUrl == "" {
				return nil, csifault.CSIInternalFault, logger.LogNewErrorCodef(log, codes.Internal,
					"queryVolumeUtil could not retrieve volume information for volume ID: %q",
					volumeInfo.VolumeID.Id)
			}
			datastoreURL = queryResult.Volumes[0].DatastoreUrl
		}
		// If improved topology FSS is enabled, retrieve datastore topology information
		// from CSINodeTopology CRs.
		// Get all nodeVMs in cluster.
		allNodeVMs, err = c.nodeMgr.GetAllNodes(ctx)
		if err != nil {
			return nil, csifault.CSIInternalFault, logger.LogNewErrorCodef(log, codes.Internal,
				"failed to find VirtualMachines for the registered nodes in the cluster. Error: %v", err)
		}
		// Find datastore topology from the retrieved datastoreURL.
		datastoreAccessibleTopology, err = c.getAccessibleTopologiesForDatastore(ctx, vcenter, topologyRequirement,
			allNodeVMs, datastoreURL)
		if err != nil {
			return nil, csifault.CSIInternalFault, logger.LogNewErrorCodef(log, codes.Internal,
				"failed to calculate accessible topologies for the datastore %q", datastoreURL)
		}

		// Add topology segments to the CreateVolumeResponse.
		for _, topoSegments := range datastoreAccessibleTopology {
			volumeTopology := &csi.Topology{
				Segments: topoSegments,
			}
			resp.Volume.AccessibleTopology = append(resp.Volume.AccessibleTopology, volumeTopology)
		}
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
	return resp, "", nil
}

// getAccessibleTopologiesForDatastore figures out the list of topologies from
// which the given datastore is accessible.
func (c *controller) getAccessibleTopologiesForDatastore(ctx context.Context, vcenter *cnsvsphere.VirtualCenter,
	topologyRequirement *csi.TopologyRequirement, allNodeVMs []*cnsvsphere.VirtualMachine, datastoreURL string) (
	[]map[string]string, error) {
	log := logger.GetLogger(ctx)
	var datastoreAccessibleTopology []map[string]string

	// Find out all nodes which have access to the chosen datastore.
	accessibleNodes, err := common.GetNodeVMsWithAccessToDatastore(ctx, vcenter, datastoreURL, allNodeVMs)
	if err != nil || len(accessibleNodes) == 0 {
		return nil, logger.LogNewErrorCodef(log, codes.Internal,
			"failed to find all the nodes from which the datastore %q is accessible", datastoreURL)
	}

	// Get node names for the accessible nodeVMs so that we can query CSINodeTopology CRs.
	var accessibleNodeNames []string
	for _, vmref := range accessibleNodes {
		// Get UUID from VM reference.
		vmUUID, err := cnsvsphere.GetUUIDFromVMReference(ctx, vcenter, vmref.Reference())
		if err != nil {
			return nil, logger.LogNewErrorCode(log, codes.Internal,
				err.Error())
		}
		// Get NodeVM name from VM UUID.
		nodeName, err := c.nodeMgr.GetNodeNameByUUID(ctx, vmUUID)
		if err != nil {
			return nil, logger.LogNewErrorCode(log, codes.Internal,
				err.Error())
		}
		accessibleNodeNames = append(accessibleNodeNames, nodeName)
	}

	datastoreAccessibleTopology, err = c.topologyMgr.GetTopologyInfoFromNodes(ctx,
		commoncotypes.VanillaRetrieveTopologyInfoParams{
			NodeNames:           accessibleNodeNames,
			DatastoreURL:        datastoreURL,
			TopologyRequirement: topologyRequirement,
		})
	if err != nil {
		return nil, logger.LogNewErrorCodef(log, codes.Internal,
			"failed to find accessible topologies for the remaining nodes %v. Error: %+v",
			accessibleNodeNames, err)
	}
	return datastoreAccessibleTopology, nil
}

// createBlockVolumeWithPlacementEngineForMultiVC creates a block volume based on the CreateVolumeRequest
// using the placement engine interface on a multi-VC environment.
func (c *controller) createBlockVolumeWithPlacementEngineForMultiVC(ctx context.Context, req *csi.CreateVolumeRequest) (
	*csi.CreateVolumeResponse, string, error) {
	log := logger.GetLogger(ctx)
	// Volume Size - Default is 10 GiB.
	volSizeBytes := int64(common.DefaultGbDiskSize * common.GbInBytes)
	if req.GetCapacityRange() != nil && req.GetCapacityRange().RequiredBytes != 0 {
		volSizeBytes = int64(req.GetCapacityRange().GetRequiredBytes())
	}
	volSizeMB := int64(common.RoundUpSize(volSizeBytes, common.MbInBytes))

	scParams, err := common.ParseStorageClassParams(ctx, req.Parameters, csiMigrationEnabled)
	// TODO: Need to figure out the fault returned by ParseStorageClassParams.
	// Currently, just return "csi.fault.Internal".
	if err != nil {
		return nil, csifault.CSIInternalFault, logger.LogNewErrorCodef(log, codes.InvalidArgument,
			"parsing storage class parameters failed with error: %+v", err)
	}

	if scParams.CSIMigration == "true" {
		if len(c.managers.VcenterConfigs) > 1 {
			return nil, csifault.CSIInternalFault, logger.LogNewErrorCode(log, codes.InvalidArgument,
				"vSphere CSI Migration is not supported on multi vCenter deployment")
		} else {
			if len(scParams.Datastore) != 0 {
				log.Infof("Converting datastore name: %q to Datastore URL", scParams.Datastore)
				// Get vCenter.
				// Need to extract fault from err returned by GetVirtualCenter.
				// Currently, just return "csi.fault.Internal".
				vCenter, err := common.GetVCenterFromVCHost(ctx, c.managers.VcenterManager, c.managers.CnsConfig.Global.VCenterIP)
				if err != nil {
					return nil, csifault.CSIInternalFault, logger.LogNewErrorCodef(log, codes.Internal,
						"failed to get vCenter. err: %+v", err)
				}
				dcList, err := vCenter.GetDatacenters(ctx)
				// Need to extract fault from err returned by GetDatacenters.
				// Currently, just return "csi.fault.Internal".
				if err != nil {
					return nil, csifault.CSIInternalFault, logger.LogNewErrorCodef(log, codes.Internal,
						"failed to get datacenter list. err: %+v", err)
				}
				foundDatastoreURL := false
				for _, dc := range dcList {
					dsURLTodsInfoMap, err := dc.GetAllDatastores(ctx)
					// Need to extract fault from err returned by GetAllDatastores.
					// Currently, just return "csi.fault.Internal".
					if err != nil {
						return nil, csifault.CSIInternalFault, logger.LogNewErrorCodef(log, codes.Internal,
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
					return nil, csifault.CSIInternalFault, logger.LogNewErrorCodef(log, codes.Internal,
						"failed to find datastoreURL for datastore name: %q", scParams.Datastore)
				}
			} else if c.managers.VcenterConfigs[c.managers.CnsConfig.Global.VCenterIP].MigrationDataStoreURL != "" {
				scParams.DatastoreURL = c.managers.VcenterConfigs[c.managers.CnsConfig.Global.VCenterIP].MigrationDataStoreURL
			}
		}
	}

	// Check if requested volume size and source snapshot size matches.
	volumeSource := req.GetVolumeContentSource()
	var contentSourceSnapshotID, snapshotDatastoreURL string
	if volumeSource != nil {
		sourceSnapshot := volumeSource.GetSnapshot()
		if sourceSnapshot == nil {
			return nil, csifault.CSIInvalidArgumentFault, logger.LogNewErrorCode(log, codes.InvalidArgument,
				"unsupported VolumeContentSource type")
		}
		contentSourceSnapshotID = sourceSnapshot.GetSnapshotId()
		cnsVolumeID, _, err := common.ParseCSISnapshotID(contentSourceSnapshotID)
		if err != nil {
			return nil, csifault.CSIInvalidArgumentFault, logger.LogNewErrorCode(log,
				codes.InvalidArgument, err.Error())
		}
		// Get VC, volumeManager for given volumeID.
		vCenterHost, volumeManager, err := getVCenterAndVolumeManagerForVolumeID(ctx, c, cnsVolumeID,
			volumeInfoService)
		if err != nil {
			return nil, csifault.CSIInternalFault, logger.LogNewErrorCodef(log, codes.Internal,
				"failed to get vCenter/volume manager for volumeID: %q. Error: %+v", cnsVolumeID, err)
		}
		isCnsSnapshotSupported, err := c.managers.VcenterManager.IsCnsSnapshotSupported(ctx, vCenterHost)
		if err != nil {
			return nil, csifault.CSIInternalFault, logger.LogNewErrorCodef(log, codes.Internal,
				"failed to check if cns snapshot operations are supported on VC %q due to error: %v",
				vCenterHost, err)
		}
		if !isCnsSnapshotSupported {
			return nil, csifault.CSIUnimplementedFault, logger.LogNewErrorCodef(log, codes.Unimplemented,
				"VC %q does not support snapshot operations", vCenterHost)
		}
		// Query capacity in MB and datastore url for block volume snapshot.
		volumeIds := []cnstypes.CnsVolumeId{{Id: cnsVolumeID}}
		cnsVolumeDetailsMap, err := utils.QueryVolumeDetailsUtil(ctx, volumeManager, volumeIds)
		if err != nil {
			return nil, csifault.CSIInternalFault, logger.LogNewErrorCodef(log, codes.Internal,
				"failed to retrieve volume details for ID %q. Error: %+v", cnsVolumeID, err)
		}
		if _, ok := cnsVolumeDetailsMap[cnsVolumeID]; !ok {
			return nil, csifault.CSIInternalFault, logger.LogNewErrorCodef(log, codes.Internal,
				"CNS query volume failed to find the volume: %q", cnsVolumeID)
		}
		snapshotSizeInMB := cnsVolumeDetailsMap[cnsVolumeID].SizeInMB
		snapshotSizeInBytes := snapshotSizeInMB * common.MbInBytes
		if volSizeBytes != snapshotSizeInBytes {
			return nil, csifault.CSIInvalidArgumentFault, logger.LogNewErrorCodef(log, codes.InvalidArgument,
				"snapshot size mismatch, requested volume size: %d but source snapshot size: %d",
				volSizeBytes, snapshotSizeInBytes)
		}
		// Store the datastoreURL of snapshot for future use.
		snapshotDatastoreURL = cnsVolumeDetailsMap[cnsVolumeID].DatastoreUrl
		// If DatastoreURL parameter is given in StorageClass, check if
		// snapshot datastore URL is same as DatastoreURL.
		if scParams.DatastoreURL != "" {
			if strings.TrimSpace(snapshotDatastoreURL) != strings.TrimSpace(scParams.DatastoreURL) {
				return nil, csifault.CSIInvalidArgumentFault, logger.LogNewErrorCodef(log, codes.InvalidArgument,
					"datastore URL %q given in storage class does not match the snapshot datastore URL %q.",
					scParams.DatastoreURL, snapshotDatastoreURL)
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
	// Check if vCenter task for this volume is already registered as part of
	// improved idempotency CR.
	log.Debugf("Checking if vCenter task for volume %s is already registered.", req.Name)
	var (
		volTaskAlreadyRegistered bool
		volumeInfo               *cnsvolume.CnsVolumeInfo
		faultType                string
		vcenter                  *cnsvsphere.VirtualCenter
		vcHost                   string
		volumeMgr                cnsvolume.Manager
	)
	// Get operation store.
	// NOTE: Operation store is common to all volume managers, so
	// we pick the operation store from any VC under volume managers list.
	var operationStore cnsvolumeoperationrequest.VolumeOperationRequest
	for _, volMgr := range c.managers.VolumeManagers {
		operationStore = volMgr.GetOperationStore()
		if operationStore == nil {
			return nil, csifault.CSIInternalFault, logger.LogNewErrorCodef(log, codes.Internal,
				"failed to get operation store in volume managers")
		}
		break
	}

	volumeOperationDetails, err := operationStore.GetRequestDetails(ctx, req.Name)
	if err != nil {
		if apierrors.IsNotFound(err) {
			log.Debugf("CreateVolume task details for block volume %s are not found.", req.Name)
		} else {
			return nil, csifault.CSIInternalFault, logger.LogNewErrorCodef(log, codes.Internal,
				"error occurred while getting CreateVolume task details for block volume %q. Error: %+v",
				req.Name, err)
		}
	} else if !isCSITransactionSupportEnabled && volumeOperationDetails.OperationDetails != nil {
		if volumeOperationDetails.OperationDetails.TaskStatus ==
			cnsvolumeoperationrequest.TaskInvocationStatusSuccess &&
			volumeOperationDetails.VolumeID != "" {
			// If task status is successful for this volume, then it means that volume is
			// already created and there is no need to create it again.
			log.Infof("Volume with name %q and id %q is already created on VC %q with opId: %q.",
				req.Name, volumeOperationDetails.VolumeID, volumeOperationDetails.OperationDetails.VCenterServer,
				volumeOperationDetails.OperationDetails.OpID)

			// Get vCenter instance.
			if volumeOperationDetails.OperationDetails.VCenterServer != "" {
				vcHost = volumeOperationDetails.OperationDetails.VCenterServer
			} else {
				vcHost = c.managers.CnsConfig.Global.VCenterIP
			}
			vcenter, err = common.GetVCenterFromVCHost(ctx, c.managers.VcenterManager, vcHost)
			if err != nil {
				return nil, csifault.CSIInternalFault, logger.LogNewErrorCodef(log, codes.Internal,
					"failed to get vCenter instance for host %q. Error: %+v", vcHost, err)
			}
			volumeInfo = &cnsvolume.CnsVolumeInfo{
				DatastoreURL: "",
				VolumeID: cnstypes.CnsVolumeId{
					Id: volumeOperationDetails.VolumeID,
				},
			}
			volTaskAlreadyRegistered = true
		} else if !isCSITransactionSupportEnabled && cnsvolume.IsTaskPending(volumeOperationDetails) {
			// If task is already created in CNS for this volume but task is in progress,
			// we need to monitor the task to check if volume creation is complete or not.
			log.Infof("Volume with name %s has CreateVolume task %s pending on VC %q.",
				req.Name, volumeOperationDetails.OperationDetails.TaskID,
				volumeOperationDetails.OperationDetails.VCenterServer)

			// Get vCenter instance.
			if volumeOperationDetails.OperationDetails.VCenterServer != "" {
				vcHost = volumeOperationDetails.OperationDetails.VCenterServer
			} else {
				vcHost = c.managers.CnsConfig.Global.VCenterIP
			}
			vcenter, err = common.GetVCenterFromVCHost(ctx, c.managers.VcenterManager, vcHost)
			if err != nil {
				return nil, csifault.CSIInternalFault, logger.LogNewErrorCodef(log, codes.Internal,
					"failed to fetch vCenter instance %q. Error: %+v", vcHost, err)
			}
			// Create task object.
			taskMoRef := types.ManagedObjectReference{
				Type:  "Task",
				Value: volumeOperationDetails.OperationDetails.TaskID,
			}
			task := object.NewTask(vcenter.Client.Client, taskMoRef)

			defer func() {
				// Persist the operation details before returning. Only success or error
				// needs to be stored as InProgress details are stored when the task is
				// created on CNS.
				if volumeOperationDetails != nil && volumeOperationDetails.OperationDetails != nil &&
					volumeOperationDetails.OperationDetails.TaskStatus !=
						cnsvolumeoperationrequest.TaskInvocationStatusInProgress {
					err := operationStore.StoreRequestDetails(ctx, volumeOperationDetails)
					if err != nil {
						log.Warnf("failed to store CreateVolume operation request details with error: %v", err)
					}
				}
			}()

			// Monitor CreateVolume task.
			volumeMgr, err = GetVolumeManagerFromVCHost(ctx, c.managers, vcHost)
			if err != nil {
				return nil, csifault.CSIInternalFault, logger.LogNewErrorCode(log, codes.Internal, err.Error())
			}
			volumeInfo, faultType, err = volumeMgr.MonitorCreateVolumeTask(ctx,
				&volumeOperationDetails, task, req.Name, c.managers.CnsConfig.Global.ClusterID)
			if err != nil {
				return nil, faultType, logger.LogNewErrorCodef(log, codes.Internal,
					"failed to monitor task for volume %s on VC %q. Error: %+v", req.Name, vcHost, err)
			}
			volTaskAlreadyRegistered = true
		}
	}

	var (
		sharedDatastores               []*cnsvsphere.DatastoreInfo
		topologyRequirement            *csi.TopologyRequirement
		combinedErrMssgs               []string
		multivCenterTopologyDeployment bool
	)

	if len(c.managers.VcenterConfigs) > 1 {
		multivCenterTopologyDeployment = true
	}
	// Get accessibility requirements.
	topologyRequirement = req.GetAccessibilityRequirements()

	vcTopologySegmentsMap := make(map[string][]map[string]string)
	if multivCenterTopologyDeployment {
		if topologyRequirement == nil {
			return nil, csifault.CSIInvalidArgumentFault, logger.LogNewErrorCode(log, codes.InvalidArgument,
				"accessibility requirements cannot be nil for a multi-VC environment")
		}
		// Get the accessibility requirements according to the VC they belong to.
		vcTopologySegmentsMap, err = common.GetAccessibilityRequirementsByVC(ctx, topologyRequirement)
		if err != nil {
			return nil, csifault.CSIInternalFault, logger.LogNewErrorCodef(log, codes.Internal,
				"failed to get accessibility requirements by VC. Error: %+v", err)
		}
		log.Debugf("Topology accessibility requirements per VC are %+v", vcTopologySegmentsMap)
	} else {
		if topologyRequirement != nil {
			// Get accessibility requirements.
			for _, topology := range topologyRequirement.Preferred {
				vcTopologySegmentsMap[c.managers.CnsConfig.Global.VCenterIP] = append(
					vcTopologySegmentsMap[c.managers.CnsConfig.Global.VCenterIP],
					topology.GetSegments())
			}
			log.Debugf("Topology accessibility requirements per VC are %+v", vcTopologySegmentsMap)
		}
	}

	if topologyRequirement != nil {
		// Check if topology domains have been provided in the vSphere CSI config secret.
		// NOTE: We do not support kubernetes.io/hostname as a topology label.
		if c.managers.CnsConfig.Labels.TopologyCategories == "" && c.managers.CnsConfig.Labels.Zone == "" &&
			c.managers.CnsConfig.Labels.Region == "" {
			return nil, csifault.CSIInvalidArgumentFault, logger.LogNewErrorCode(log, codes.InvalidArgument,
				"topology category names not specified in the vsphere config secret")
		}
	}

	if !volTaskAlreadyRegistered {
		// Iterate through each VC and its accessibility requirements to try and create a volume.
		// If it fails for any reason, move to the next VC in list.
		if topologyRequirement != nil {
			var topologySegmentsList []map[string]string
			for vcHost, topologySegmentsList = range vcTopologySegmentsMap {
				// Get VC instance.
				vcenter, err = common.GetVCenterFromVCHost(ctx, c.managers.VcenterManager, vcHost)
				if err != nil {
					return nil, csifault.CSIInternalFault, logger.LogNewErrorCodef(log, codes.Internal,
						"failed to get vCenter instance for host %q. Error: %+v", vcHost, err)
				}

				// If Storage policy is given, check if it exists in the VC. If not found, continue to next VC.
				var storagePolicyID string
				if scParams.StoragePolicyName != "" {
					storagePolicyID, err = vcenter.GetStoragePolicyIDByName(ctx, scParams.StoragePolicyName)
					if err != nil {
						// TODO: As govmomi doesn't support locale and all error messages are
						// in English, we are temporarily resorting to a error message check.
						// In future, we need to change govmomi to throw a NotFound error and catch that instead.
						errMssgFromPBM := fmt.Sprintf("no pbm profile found with name: %q",
							scParams.StoragePolicyName)
						if err.Error() == errMssgFromPBM {
							errMsg := fmt.Sprintf("Storage policy name %q not found in VC %q",
								scParams.StoragePolicyName, vcHost)
							log.Warn(errMsg)
							combinedErrMssgs = append(combinedErrMssgs, errMsg)
							continue
						}
						return nil, csifault.CSIInternalFault, logger.LogNewErrorCodef(log, codes.Internal,
							"failed to get policy ID for storage policy name %q. Error: %+v",
							scParams.StoragePolicyName, err)
					}
					log.Infof("Found ID %q for storage policy name %q in vCenter %q", storagePolicyID,
						scParams.StoragePolicyName, vcHost)
				}

				// Get shared accessible datastores for topology segments associated with the vcHost.
				sharedDatastores, err = placementengine.GetSharedDatastores(ctx,
					placementengine.VanillaSharedDatastoresParams{
						Vcenter:              vcenter,
						TopologySegmentsList: topologySegmentsList,
						StoragePolicyID:      storagePolicyID,
					})
				if err != nil {
					return nil, csifault.CSIInternalFault, logger.LogNewErrorCodef(log, codes.Internal,
						"failed to get shared datastores for topology segments %+v in vCenter %q. Error: %+v",
						topologySegmentsList, vcHost, err)
				}
				if len(sharedDatastores) == 0 {
					errMsg := fmt.Sprintf("No compatible datastores found for accessibility requirements %+v "+
						"pertaining to vCenter %q", topologySegmentsList, vcHost)
					log.Warn(errMsg)
					combinedErrMssgs = append(combinedErrMssgs, errMsg)
					continue
				}
				// Filter datastores based on user access.
				sharedDatastores, err = c.filterDatastores(ctx, sharedDatastores, vcHost)
				if err != nil {
					if err == errAllDSFilteredOut {
						errMsg := fmt.Sprintf("authorization service filtered out all the compatible "+
							"datastores found for accessibility requirements %+v associated with vCenter %q",
							topologySegmentsList, vcHost)
						log.Warn(errMsg)
						combinedErrMssgs = append(combinedErrMssgs, errMsg)
						continue
					}
					return nil, csifault.CSIInternalFault, logger.LogNewErrorCodef(log, codes.Internal,
						"failed to filter datastores based on authorisation check in vCenter %q. Error: %+v",
						vcHost, err)
				}
				volumeMgr, err = GetVolumeManagerFromVCHost(ctx, c.managers, vcHost)
				if err != nil {
					return nil, csifault.CSIInternalFault, logger.LogNewErrorCode(log, codes.Internal, err.Error())
				}
				// Call CreateVolume.
				// TODO: Few errors encountered  in CreateBlockVolumeUtilForMultiVC can be
				// retried instead of moving unto next VC. Need to throw a custom error for such scenarios.

				volumeInfo, faultType, err = common.CreateBlockVolumeUtilForMultiVC(ctx,
					common.VanillaCreateBlockVolParamsForMultiVC{
						Vcenter:              vcenter,
						VolumeManager:        volumeMgr,
						CNSConfig:            c.managers.CnsConfig,
						StoragePolicyID:      storagePolicyID,
						Spec:                 &createVolumeSpec,
						SharedDatastores:     sharedDatastores,
						SnapshotDatastoreURL: snapshotDatastoreURL,
						ClusterFlavor:        cnstypes.CnsClusterFlavorVanilla,
					},
					common.CreateBlockVolumeOptions{
						IsCSITransactionSupportEnabled: isCSITransactionSupportEnabled,
					})
				if err != nil {
					if cnsvolume.IsNotSupportedFaultType(ctx, faultType) {
						log.Warnf("NotSupported fault is detected: retrying CreateVolume without VolumeID in spec.")
						volumeInfo, faultType, err = common.CreateBlockVolumeUtilForMultiVC(ctx,
							common.VanillaCreateBlockVolParamsForMultiVC{
								Vcenter:              vcenter,
								VolumeManager:        volumeMgr,
								CNSConfig:            c.managers.CnsConfig,
								StoragePolicyID:      storagePolicyID,
								Spec:                 &createVolumeSpec,
								SharedDatastores:     sharedDatastores,
								SnapshotDatastoreURL: snapshotDatastoreURL,
								ClusterFlavor:        cnstypes.CnsClusterFlavorVanilla,
							},
							common.CreateBlockVolumeOptions{
								IsCSITransactionSupportEnabled: false,
							})
					}
					if err != nil {
						log.Error(err)
						combinedErrMssgs = append(combinedErrMssgs, err.Error())
						continue
					}
				}
				log.Infof("volume %q created in vCenter %q. Proceeding to calculate "+
					"accessible topology for the volume", volumeInfo.VolumeID.Id, vcHost)
				break
			}
		} else {
			// Get VC instance.
			vcHost = c.managers.CnsConfig.Global.VCenterIP
			vcenter, err = common.GetVCenterFromVCHost(ctx, c.managers.VcenterManager, vcHost)
			if err != nil {
				return nil, csifault.CSIInternalFault, logger.LogNewErrorCodef(log, codes.Internal,
					"failed to get vCenter instance for host %q. Error: %+v", vcHost, err)
			}

			volumeMgr, err = GetVolumeManagerFromVCHost(ctx, c.managers, vcHost)
			if err != nil {
				return nil, csifault.CSIInternalFault, logger.LogNewErrorCode(log, codes.Internal, err.Error())
			}

			// If Storage policy is given, check if it exists in the VC.
			// If not found, fail Volume Creation
			var storagePolicyID string
			if scParams.StoragePolicyName != "" {
				storagePolicyID, err = vcenter.GetStoragePolicyIDByName(ctx, scParams.StoragePolicyName)
				if err != nil {
					return nil, csifault.CSIInternalFault, logger.LogNewErrorCodef(log, codes.Internal,
						"failed to get policy ID for storage policy name %q. Error: %+v",
						scParams.StoragePolicyName, err.Error())
				}
				log.Infof("Found ID %q for storage policy name %q in vCenter %q", storagePolicyID,
					scParams.StoragePolicyName, vcHost)
			}
			sharedDatastores, err = c.nodeMgr.GetSharedDatastoresInK8SCluster(ctx)
			if err != nil || len(sharedDatastores) == 0 {
				return nil, csifault.CSIInternalFault, logger.LogNewErrorCodef(log, codes.Internal,
					"failed to get shared datastores in kubernetes cluster. Error: %+v", err)
			}
			if len(sharedDatastores) == 0 {
				return nil, csifault.CSIInternalFault, logger.LogNewErrorCode(log, codes.Internal,
					"No datastore found for volume provisioning.")
			}

			// Filter datastores which in datastoreMap from sharedDatastores.
			sharedDatastores, err = c.filterDatastores(ctx, sharedDatastores, vcHost)
			if err != nil {
				return nil, csifault.CSIInternalFault, logger.LogNewErrorCodef(log, codes.Internal,
					"failed to create volume. Error: %+v", err)
			}
			volumeInfo, faultType, err = common.CreateBlockVolumeUtilForMultiVC(ctx,
				common.VanillaCreateBlockVolParamsForMultiVC{
					Vcenter:              vcenter,
					VolumeManager:        volumeMgr,
					CNSConfig:            c.managers.CnsConfig,
					StoragePolicyID:      storagePolicyID,
					Spec:                 &createVolumeSpec,
					SharedDatastores:     sharedDatastores,
					SnapshotDatastoreURL: snapshotDatastoreURL,
					ClusterFlavor:        cnstypes.CnsClusterFlavorVanilla,
				},
				common.CreateBlockVolumeOptions{
					IsCSITransactionSupportEnabled: isCSITransactionSupportEnabled,
				})
			if err != nil {
				if cnsvolume.IsNotSupportedFaultType(ctx, faultType) {
					log.Warnf("NotSupported fault is detected: retrying CreateVolume without VolumeID in spec.")
					volumeInfo, faultType, err = common.CreateBlockVolumeUtilForMultiVC(ctx,
						common.VanillaCreateBlockVolParamsForMultiVC{
							Vcenter:              vcenter,
							VolumeManager:        volumeMgr,
							CNSConfig:            c.managers.CnsConfig,
							StoragePolicyID:      storagePolicyID,
							Spec:                 &createVolumeSpec,
							SharedDatastores:     sharedDatastores,
							SnapshotDatastoreURL: snapshotDatastoreURL,
							ClusterFlavor:        cnstypes.CnsClusterFlavorVanilla,
						},
						common.CreateBlockVolumeOptions{
							IsCSITransactionSupportEnabled: false,
						})
					if err != nil {
						return nil, faultType, logger.LogNewErrorCodef(log, codes.Internal,
							"failed to create volume without supplying VolumeID in the Spec. Error: %+v", err)
					}
				} else {
					return nil, faultType, logger.LogNewErrorCodef(log, codes.Internal,
						"failed to create volume. Error: %+v", err)
				}
			}
			log.Infof("volume %q created in vCenter %q", volumeInfo.VolumeID.Id, vcHost)
		}
	}
	if volumeInfo == nil {
		if faultType == "" {
			faultType = csifault.CSIInternalFault
		}
		return nil, faultType, logger.LogNewErrorCodef(log, codes.Internal,
			"failed to create volume. Errors encountered: %+v", combinedErrMssgs)
	}

	attributes := make(map[string]string)
	attributes[common.AttributeDiskType] = common.DiskTypeBlockVolume

	if scParams.CSIMigration == "true" {
		volumePath, err := volumeMigrationService.GetVolumePath(ctx, volumeInfo.VolumeID.Id)
		if err != nil {
			return nil, csifault.CSIInternalFault, logger.LogNewErrorCodef(log, codes.Internal,
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

	// For topology aware provisioning, populate the topology segments parameter
	// in the CreateVolumeResponse struct.
	if topologyRequirement != nil {
		var (
			datastoreAccessibleTopology []map[string]string
			allNodeVMs                  []*cnsvsphere.VirtualMachine
		)
		// Retrieve the datastoreURL of the Provisioned Volume. If CNS CreateVolume
		// API does not return datastoreURL, retrieve this by calling QueryVolume.
		// Otherwise, retrieve this from PlacementResults in the response of
		// CreateVolume API.
		datastoreURL := volumeInfo.DatastoreURL
		if datastoreURL == "" {
			if volumeMgr == nil {
				volumeMgr, err = GetVolumeManagerFromVCHost(ctx, c.managers, vcHost)
				if err != nil {
					return nil, csifault.CSIInternalFault, logger.LogNewErrorCode(log, codes.Internal, err.Error())
				}
			}

			volumeIds := []cnstypes.CnsVolumeId{{Id: volumeInfo.VolumeID.Id}}
			queryFilter := cnstypes.CnsQueryFilter{
				VolumeIds: volumeIds,
			}

			querySelection := cnstypes.CnsQuerySelection{
				Names: []string{string(cnstypes.QuerySelectionNameTypeDataStoreUrl)},
			}
			queryResult, err := utils.QueryVolumeUtil(ctx, volumeMgr, queryFilter, &querySelection, true)
			if err != nil {
				// TODO: QueryVolume need to return faultType.
				// Need to return faultType which is returned from QueryVolume.
				// Currently, just return "csi.fault.Internal".
				return nil, csifault.CSIInternalFault, logger.LogNewErrorCodef(log, codes.Internal,
					"queryVolumeUtil failed for volumeID: %s in vCenter %q. Error: %+v",
					volumeInfo.VolumeID.Id, vcHost, err)
			}
			if len(queryResult.Volumes) == 0 || queryResult.Volumes[0].DatastoreUrl == "" {
				return nil, csifault.CSIInternalFault, logger.LogNewErrorCodef(log, codes.Internal,
					"queryVolumeUtil could not retrieve volume information for volume ID: %q in vCenter %q",
					volumeInfo.VolumeID.Id, vcHost)
			}
			datastoreURL = queryResult.Volumes[0].DatastoreUrl
		}

		// Retrieve datastore topology information from CSINodeTopology CRs.
		allNodeVMs, err = c.nodeMgr.GetAllNodesByVC(ctx, vcHost)
		if err != nil {
			return nil, csifault.CSIInternalFault, logger.LogNewErrorCodef(log, codes.Internal,
				"failed to fetch VirtualMachines for the registered nodes in VC %q. Error: %v", vcHost, err)
		}
		// Find datastore topology from the retrieved datastoreURL.
		datastoreAccessibleTopology, err = c.calculateAccessibleTopologiesForDatastore(ctx, vcenter,
			vcTopologySegmentsMap[vcHost], allNodeVMs, datastoreURL)
		if err != nil {
			return nil, csifault.CSIInternalFault, logger.LogNewErrorCodef(log, codes.Internal,
				"failed to calculate accessible topologies for the datastore %q", datastoreURL)
		}

		// Add topology segments to the CreateVolumeResponse.
		for _, topoSegments := range datastoreAccessibleTopology {
			volumeTopology := &csi.Topology{
				Segments: topoSegments,
			}
			resp.Volume.AccessibleTopology = append(resp.Volume.AccessibleTopology, volumeTopology)
		}
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
	if len(c.managers.VcenterConfigs) > 1 {
		// Create CNSVolumeInfo CR for the volume ID.
		err = volumeInfoService.CreateVolumeInfo(ctx, volumeInfo.VolumeID.Id, vcHost)
		if err != nil {
			return nil, csifault.CSIInternalFault, logger.LogNewErrorCodef(log, codes.Internal,
				"failed to store volumeID %q for vCenter %q in CNSVolumeInfo CR. Error: %+v",
				volumeInfo.VolumeID.Id, vcHost, err)
		}
	}
	return resp, "", nil
}

// calculateAccessibleTopologiesForDatastore figures out the list of topologies from
// which the given datastore is accessible when multi-VC FSS is enabled.
func (c *controller) calculateAccessibleTopologiesForDatastore(ctx context.Context, vcenter *cnsvsphere.VirtualCenter,
	topologySegments []map[string]string, allNodeVMs []*cnsvsphere.VirtualMachine, datastoreURL string) (
	[]map[string]string, error) {
	log := logger.GetLogger(ctx)
	var datastoreAccessibleTopology []map[string]string

	// Find out all nodeVMs which have access to the chosen datastore among all the nodes in k8s cluster.
	accessibleNodes, err := common.GetNodeVMsWithAccessToDatastore(ctx, vcenter, datastoreURL, allNodeVMs)
	if err != nil || len(accessibleNodes) == 0 {
		return nil, logger.LogNewErrorCodef(log, codes.Internal,
			"failed to find all the nodes from which the datastore %q is accessible", datastoreURL)
	}

	// Get node names for the accessible nodeVMs so that we can query CSINodeTopology CRs.
	var accessibleNodeNames []string
	for _, vmref := range accessibleNodes {
		// Get UUID from VM reference.
		vmUUID, err := cnsvsphere.GetUUIDFromVMReference(ctx, vcenter, vmref.Reference())
		if err != nil {
			return nil, logger.LogNewErrorCode(log, codes.Internal,
				err.Error())
		}
		// Get NodeVM name from VM UUID.
		nodeName, err := c.nodeMgr.GetNodeNameByUUID(ctx, vmUUID)
		if err != nil {
			return nil, logger.LogNewErrorCode(log, codes.Internal,
				err.Error())
		}
		accessibleNodeNames = append(accessibleNodeNames, nodeName)
	}

	datastoreAccessibleTopology, err = placementengine.GetTopologyInfoFromNodes(ctx,
		placementengine.VanillaRetrieveTopologyInfoParams{
			VCHost:                    vcenter.Config.Host,
			NodeNames:                 accessibleNodeNames,
			DatastoreURL:              datastoreURL,
			RequestedTopologySegments: topologySegments,
		})
	if err != nil {
		return nil, logger.LogNewErrorCodef(log, codes.Internal,
			"failed to find accessible topologies for the nodes %v. Error: %+v",
			accessibleNodeNames, err)
	}
	return datastoreAccessibleTopology, nil
}

// createFileVolume creates a file volume based on the CreateVolumeRequest.
func (c *controller) createFileVolume(ctx context.Context, req *csi.CreateVolumeRequest) (
	*csi.CreateVolumeResponse, string, error) {
	log := logger.GetLogger(ctx)

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
	// TODO: Need to figure out the fault returned by ParseStorageClassParams.
	// Currently, just return "csi.fault.Internal".
	if err != nil {
		return nil, csifault.CSIInternalFault, logger.LogNewErrorCodef(log, codes.InvalidArgument,
			"parsing storage class parameters failed with error: %+v", err)
	}

	var (
		volTaskAlreadyRegistered bool
		volumeInfo               *cnsvolume.CnsVolumeInfo
		faultType                string
		volumeID                 string
		vcenter                  *cnsvsphere.VirtualCenter
		vcHost                   string
	)
	// Get operation store
	var operationStore cnsvolumeoperationrequest.VolumeOperationRequest
	if multivCenterCSITopologyEnabled {
		for _, volMgr := range c.managers.VolumeManagers {
			// NOTE: Operation store is common to all volume managers, so
			// we pick the operation store from any VC under volume managers list.
			operationStore = volMgr.GetOperationStore()
			if operationStore == nil {
				return nil, csifault.CSIInternalFault, logger.LogNewErrorCodef(log, codes.Internal,
					"failed to get operation store in volume managers")
			}
			break
		}
	} else {
		operationStore = c.manager.VolumeManager.GetOperationStore()
	}
	if operationStore == nil {
		return nil, csifault.CSIInternalFault, logger.LogNewErrorCodef(log, codes.Internal,
			"Operation store cannot be nil")
	}

	// Check if vCenter task for this volume is already registered as part of
	// improved idempotency CR
	log.Debugf("Checking if vCenter task for file volume %s is already registered.", req.Name)
	volumeOperationDetails, err := operationStore.GetRequestDetails(ctx, req.Name)
	if err != nil {
		if apierrors.IsNotFound(err) {
			log.Debugf("CreateVolume task details for file volume %s are not found.", req.Name)
		} else {
			return nil, csifault.CSIInternalFault, logger.LogNewErrorCodef(log, codes.Internal,
				"Error occurred while getting CreateVolume task details for file volume %s, err: %+v",
				req.Name, err)
		}
	} else if volumeOperationDetails.OperationDetails != nil {
		if volumeOperationDetails.OperationDetails.TaskStatus ==
			cnsvolumeoperationrequest.TaskInvocationStatusSuccess && volumeOperationDetails.VolumeID != "" {
			// If task status is successful for this volume, then it means that volume is
			// already created and there is no need to create it again.
			log.Infof("File volume with name %q and id %q is already created on CNS with opId: %q.",
				req.Name, volumeOperationDetails.VolumeID, volumeOperationDetails.OperationDetails.OpID)

			volumeID = volumeOperationDetails.VolumeID
			volTaskAlreadyRegistered = true
		} else if cnsvolume.IsTaskPending(volumeOperationDetails) {
			volTaskAlreadyRegistered = true
			if volumeOperationDetails.OperationDetails.VCenterServer != "" {
				vcHost = volumeOperationDetails.OperationDetails.VCenterServer
			} else {
				vcHost = c.managers.CnsConfig.Global.VCenterIP
			}
			vcenter, err = common.GetVCenterFromVCHost(ctx, c.managers.VcenterManager, vcHost)
			if err != nil {
				return nil, csifault.CSIInternalFault, logger.LogNewErrorCodef(log, codes.Internal,
					"failed to fetch vCenter instance %q. Error: %+v", vcHost, err)
			}
			// If task is created in CNS for this volume but task is in progress, then
			// we need to monitor the task to check if volume creation is completed or not.
			log.Infof("File volume with name %s has CreateVolume task %s pending on CNS.",
				req.Name, volumeOperationDetails.OperationDetails.TaskID)
			taskMoRef := types.ManagedObjectReference{
				Type:  "Task",
				Value: volumeOperationDetails.OperationDetails.TaskID,
			}
			task := object.NewTask(vcenter.Client.Client, taskMoRef)

			defer func() {
				// Persist the operation details before returning. Only success or error
				// needs to be stored as InProgress details are stored when the task is
				// created on CNS.
				if volumeOperationDetails != nil && volumeOperationDetails.OperationDetails != nil &&
					volumeOperationDetails.OperationDetails.TaskStatus !=
						cnsvolumeoperationrequest.TaskInvocationStatusInProgress {
					err := operationStore.StoreRequestDetails(ctx, volumeOperationDetails)
					if err != nil {
						log.Warnf("failed to store CreateVolume details with error: %v", err)
					}
				}
			}()
			if multivCenterCSITopologyEnabled {
				volumeManager := c.managers.VolumeManagers[vcHost]
				volumeInfo, faultType, err = volumeManager.MonitorCreateVolumeTask(ctx,
					&volumeOperationDetails, task, req.Name, c.managers.CnsConfig.Global.ClusterID)
			} else {
				volumeInfo, faultType, err = c.manager.VolumeManager.MonitorCreateVolumeTask(ctx,
					&volumeOperationDetails, task, req.Name, c.manager.CnsConfig.Global.ClusterID)
			}
			if err != nil {
				return nil, faultType, logger.LogNewErrorCodef(log, codes.Internal,
					"failed to monitor task for file volume %s. Error: %+v", req.Name, err)
			}
			volumeID = volumeInfo.VolumeID.Id
		}
	}

	if !volTaskAlreadyRegistered {
		// Get accessibility requirements.
		topologyRequirement := req.GetAccessibilityRequirements()
		if topologyRequirement != nil {
			// Check if topology domains have been provided in the vSphere CSI config secret.
			// NOTE: We do not support kubernetes.io/hostname as a topology label.
			if c.managers.CnsConfig.Labels.TopologyCategories == "" && c.managers.CnsConfig.Labels.Zone == "" &&
				c.managers.CnsConfig.Labels.Region == "" {
				return nil, csifault.CSIInvalidArgumentFault, logger.LogNewErrorCode(log, codes.InvalidArgument,
					"topology category names not specified in the vsphere config secret")
			}
		}

		if len(c.managers.VcenterConfigs) > 1 {
			if topologyRequirement == nil {
				return nil, csifault.CSIInvalidArgumentFault, logger.LogNewErrorCode(log, codes.InvalidArgument,
					"accessibility requirements cannot be nil for a multi-VC environment")
			}
		}
		vcTopologySegmentsMap := make(map[string][]map[string]string)
		if topologyRequirement != nil {
			// Get accessibility requirements.
			vcTopologySegmentsMap, err = common.GetAccessibilityRequirementsByVC(ctx, topologyRequirement)
			if err != nil {
				return nil, csifault.CSIInternalFault, logger.LogNewErrorCodef(log, codes.Internal,
					"failed to get accessibility requirements by VC. Error: %+v", err)
			}
			log.Debugf("Topology accessibility requirements per VC are %+v", vcTopologySegmentsMap)
		}
		var createVolumeSpec = common.CreateVolumeSpec{
			CapacityMB: volSizeMB,
			Name:       req.Name,
			ScParams:   scParams,
			VolumeType: common.FileVolumeType,
		}
		var combinedErrMssgs []string
		if topologyRequirement != nil {
			if !isTopologyAwareFileVolumeEnabled {
				// Error out if TopologyRequirement is provided during file
				// volume provisioning when FSS is turned off.
				return nil, csifault.CSIInvalidArgumentFault, logger.LogNewErrorCode(log, codes.InvalidArgument,
					"volume topology feature for file volumes is not supported.")
			}
			var topologySegmentsList []map[string]string
			for vcHost, topologySegmentsList = range vcTopologySegmentsMap {
				// Get VC instance.
				vcenter, err = common.GetVCenterFromVCHost(ctx, c.managers.VcenterManager, vcHost)
				if err != nil {
					return nil, csifault.CSIInternalFault, logger.LogNewErrorCodef(log, codes.Internal,
						"failed to get vCenter instance for host %q. Error: %+v", vcHost, err)
				}
				// If Storage policy is given, check if it exists in the VC. If not found, continue to next VC.
				var storagePolicyID string
				if scParams.StoragePolicyName != "" {
					storagePolicyID, err = vcenter.GetStoragePolicyIDByName(ctx, scParams.StoragePolicyName)
					if err != nil {
						// TODO: As govmomi doesn't support locale and all error messages are
						// in English, we are temporarily resorting to a error message check.
						// In future, we need to change govmomi to throw a NotFound error and catch that instead.
						errMssgFromPBM := fmt.Sprintf("no pbm profile found with name: %q",
							scParams.StoragePolicyName)
						if err.Error() == errMssgFromPBM {
							errMsg := fmt.Sprintf("Storage policy name %q not found in VC %q",
								scParams.StoragePolicyName, vcenter.Config.Host)
							log.Warn(errMsg)
							combinedErrMssgs = append(combinedErrMssgs, errMsg)
							continue
						}
						return nil, csifault.CSIInternalFault, logger.LogNewErrorCodef(log, codes.Internal,
							"failed to get policy ID for storage policy name %q. Error: %+v",
							scParams.StoragePolicyName, err)
					}
					log.Infof("Found ID %q for storage policy name %q in vCenter %q", storagePolicyID,
						scParams.StoragePolicyName, vcenter.Config.Host)
				}

				// Get accessible datastores in the VC's topology requirement.
				candidateDatastores, err := placementengine.GetAllAccessibleDSInTopology(ctx,
					topologySegmentsList, vcenter)
				if err != nil {
					return nil, csifault.CSIInternalFault, logger.LogNewErrorCodef(log, codes.Internal,
						"error finding candidate datastores in topology %+v for VC: %q",
						topologySegmentsList, vcHost)
				}
				if len(candidateDatastores) == 0 {
					errMsg := fmt.Sprintf("No candidate datastores available for file volume provisioning "+
						"in topology: %+v in VC: %q", topologySegmentsList, vcenter.Config.Host)
					log.Warn(errMsg)
					combinedErrMssgs = append(combinedErrMssgs, errMsg)
					continue
				}

				// Filter File service enabled DS from candidate datastores.
				var fsEnabledCandidateDatastores []*cnsvsphere.DatastoreInfo
				fsEnabledClusterToDsInfoMap := c.authMgrs[vcHost].GetFsEnabledClusterToDsMap(ctx)
				for _, datastores := range fsEnabledClusterToDsInfoMap {
					for _, fsEnabledDatastore := range datastores {
						for _, ds := range candidateDatastores {
							if ds.Info.Url == fsEnabledDatastore.Info.Url {
								fsEnabledCandidateDatastores = append(fsEnabledCandidateDatastores, ds)
							}
						}
					}
				}
				if len(fsEnabledCandidateDatastores) == 0 {
					// Possibly vsan file service is not enabled on any vsan cluster.
					errMsg := fmt.Sprintf("No datastores found to create file volume on VC %q. vSAN file "+
						"service may be disabled", vcHost)
					log.Warn(errMsg)
					combinedErrMssgs = append(combinedErrMssgs, errMsg)
					continue
				}
				log.Infof("fsEnabledCandidateDatastores %v", fsEnabledCandidateDatastores)

				// Filter Storage policy compatible datastores from candidate datastores list.
				if scParams.StoragePolicyName != "" {
					// Check storage policy compatibility.
					var candidateDSMoRef []types.ManagedObjectReference
					for _, ds := range fsEnabledCandidateDatastores {
						candidateDSMoRef = append(candidateDSMoRef, ds.Reference())
					}
					compat, err := vcenter.PbmCheckCompatibility(ctx, candidateDSMoRef, storagePolicyID)
					if err != nil {
						return nil, csifault.CSIInternalFault, logger.LogNewErrorf(log,
							"failed to find datastore compatibility with storage policy ID %q. Error: %+v",
							storagePolicyID, err)
					}
					compatibleDsMoids := make(map[string]struct{})
					for _, ds := range compat.CompatibleDatastores() {
						compatibleDsMoids[ds.HubId] = struct{}{}
					}
					log.Infof("Datastores compatible with storage policy %q are %+v",
						scParams.StoragePolicyName, compatibleDsMoids)

					// Filter compatible datastores from candidate datastores list.
					var compatibleDatastores []*cnsvsphere.DatastoreInfo
					for _, ds := range fsEnabledCandidateDatastores {
						if _, exists := compatibleDsMoids[ds.Reference().Value]; exists {
							compatibleDatastores = append(compatibleDatastores, ds)
						}
					}
					if len(compatibleDatastores) == 0 {
						errMsg := fmt.Sprintf("No compatible datastores found for storage policy %q on VC %q",
							scParams.StoragePolicyName, vcHost)
						log.Warn(errMsg)
						combinedErrMssgs = append(combinedErrMssgs, errMsg)
						continue
					}
					fsEnabledCandidateDatastores = compatibleDatastores
				}
				// TODO: Few errors encountered in CreateFileVolumeUtil can be retried instead of
				// moving unto next VC. Need to throw a custom error for such scenarios.
				volumeInfo, faultType, err = common.CreateFileVolumeUtil(ctx, cnstypes.CnsClusterFlavorVanilla,
					vcenter, c.managers.VolumeManagers[vcHost], c.managers.CnsConfig, &createVolumeSpec,
					fsEnabledCandidateDatastores, filterSuspendedDatastores, false, nil)
				if err != nil {
					log.Error(err)
					combinedErrMssgs = append(combinedErrMssgs, err.Error())
					continue
				}
				volumeID = volumeInfo.VolumeID.Id
				log.Infof("volume %q created in vCenter %q.", volumeID, vcHost)

				// In case task was successful from previous run but the CR was not created.
				if multivCenterCSITopologyEnabled && len(c.managers.VcenterConfigs) > 1 {
					// Create CNSVolumeInfo CR for the volume ID in case of multi VC deployment.
					err = volumeInfoService.CreateVolumeInfo(ctx, volumeID, vcHost)
					if err != nil {
						return nil, csifault.CSIInternalFault, logger.LogNewErrorCodef(log, codes.Internal,
							"failed to store volumeID %q for vCenter %q in CNSVolumeInfo CR. Error: %+v",
							volumeInfo.VolumeID.Id, vcHost, err)
					}
				}
				break
			}
			// After iterating over all VCs, if volumeID is still empty, we error out.
			if volumeID == "" {
				if faultType == "" {
					faultType = csifault.CSIInternalFault
				}
				return nil, faultType, logger.LogNewErrorCodef(log, codes.Internal,
					"failed to create file volume. Errors encountered: %+v", combinedErrMssgs)
			}
		} else {
			// When topologyRequirement is nil, below code is invoked
			var fsEnabledClusterToDsInfoMap map[string][]*cnsvsphere.DatastoreInfo
			if multivCenterCSITopologyEnabled {
				vcHost = c.managers.CnsConfig.Global.VCenterIP
				fsEnabledClusterToDsInfoMap = c.authMgrs[vcHost].GetFsEnabledClusterToDsMap(ctx)
			} else {
				fsEnabledClusterToDsInfoMap = c.authMgr.GetFsEnabledClusterToDsMap(ctx)
			}

			var filteredDatastores []*cnsvsphere.DatastoreInfo
			for _, datastores := range fsEnabledClusterToDsInfoMap {
				filteredDatastores = append(filteredDatastores, datastores...)
			}
			if len(filteredDatastores) == 0 {
				// when len(filteredDatastore)==0, it means vsan file service is not enabled on any vsan cluster
				return nil, csifault.CSIVSanFileServiceDisabledFault, logger.LogNewErrorCode(log, codes.FailedPrecondition,
					"no datastores found to create file volume, vSAN file service may be disabled")
			}

			if multivCenterCSITopologyEnabled {
				// Get VC instance.
				vcenter, err = common.GetVCenterFromVCHost(ctx, c.managers.VcenterManager, vcHost)
				if err != nil {
					return nil, csifault.CSIInternalFault, logger.LogNewErrorCodef(log, codes.Internal,
						"failed to get vCenter instance for host %q. Error: %+v", vcHost, err)
				}
				volumeInfo, faultType, err = common.CreateFileVolumeUtil(ctx, cnstypes.CnsClusterFlavorVanilla,
					vcenter, c.managers.VolumeManagers[vcHost], c.managers.CnsConfig, &createVolumeSpec,
					filteredDatastores, filterSuspendedDatastores, false, nil)
				if err != nil {
					return nil, faultType, logger.LogNewErrorCodef(log, codes.Internal,
						"failed to create volume. Error: %+v", err)
				}
				volumeID = volumeInfo.VolumeID.Id
			} else {
				vcenter, err = c.manager.VcenterManager.GetVirtualCenter(ctx, c.manager.VcenterConfig.Host)
				if err != nil {
					return nil, faultType, logger.LogNewErrorCodef(log, codes.Internal,
						"failed to get vCenter. Error: %+v", err)
				}
				volumeInfo, faultType, err = common.CreateFileVolumeUtil(ctx, cnstypes.CnsClusterFlavorVanilla,
					vcenter, c.manager.VolumeManager, c.manager.CnsConfig, &createVolumeSpec,
					filteredDatastores, filterSuspendedDatastores, false, nil)
				if err != nil {
					return nil, faultType, logger.LogNewErrorCodef(log, codes.Internal,
						"failed to create volume. Error: %+v", err)
				}
				volumeID = volumeInfo.VolumeID.Id
			}
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
	return resp, "", nil
}

// CreateVolume is creating CNS Volume using volume request specified in
// CreateVolumeRequest.
func (c *controller) CreateVolume(ctx context.Context, req *csi.CreateVolumeRequest) (
	*csi.CreateVolumeResponse, error) {
	start := time.Now()
	ctx = logger.NewContextWithLogger(ctx)
	log := logger.GetLogger(ctx)

	volumeType := prometheus.PrometheusUnknownVolumeType
	createVolumeInternal := func() (
		*csi.CreateVolumeResponse, string, error) {
		log.Infof("CreateVolume: called with args %+v", *req)
		// TODO: If the err is returned by invoking CNS API, then faultType should be
		// populated by the underlying layer.
		// If the request failed due to validate the request, "csi.fault.InvalidArgument" will be return.
		// If thr reqeust failed due to object not found, "csi.fault.NotFound" will be return.
		// For all other cases, the faultType will be set to "csi.fault.Internal" for now.
		// Later we may need to define different csi faults.
		volumeCapabilities := req.GetVolumeCapabilities()
		if err := common.IsValidVolumeCapabilities(ctx, volumeCapabilities); err != nil {
			return nil, csifault.CSIInternalFault, logger.LogNewErrorCodef(log, codes.InvalidArgument,
				"volume capability not supported. Err: %+v", err)
		}
		if common.IsFileVolumeRequest(ctx, volumeCapabilities) {
			// Error out if TopologyRequirement is provided during file volume provisioning
			// as this is not supported yet.
			if !commonco.ContainerOrchestratorUtility.IsFSSEnabled(ctx, common.TopologyAwareFileVolume) {
				if req.GetAccessibilityRequirements() != nil {
					return nil, csifault.CSIInvalidArgumentFault, logger.LogNewErrorCode(log, codes.InvalidArgument,
						"volume topology feature for file volumes is not supported.")
				}
			}
			volumeType = prometheus.PrometheusFileVolumeType
			if multivCenterCSITopologyEnabled && len(c.managers.VcenterConfigs) > 1 {
				isvSANFileServicesDisabledInAllVCs := true
				for _, vcconfig := range c.managers.VcenterConfigs {
					isvSANFileServicesSupported, err := c.managers.VcenterManager.IsvSANFileServicesSupported(ctx,
						vcconfig.Host)
					if err != nil {
						return nil, csifault.CSIInternalFault, logger.LogNewErrorf(log,
							"failed to verify if vSAN file services is supported or not for vCenter: %s. "+
								"Error:%+v", vcconfig.Host, err)
					}
					if isvSANFileServicesSupported {
						isvSANFileServicesDisabledInAllVCs = false
					}
				}

				if isvSANFileServicesDisabledInAllVCs {
					return nil, csifault.CSIInternalFault, logger.LogNewErrorCode(log, codes.FailedPrecondition,
						"fileshare volume creation is not supported on vSAN 67u3 release")
				}
				return c.createFileVolume(ctx, req)
			} else {
				isvSANFileServicesSupported, err := c.managers.VcenterManager.IsvSANFileServicesSupported(ctx,
					c.managers.CnsConfig.Global.VCenterIP)
				if err != nil {
					return nil, csifault.CSIInternalFault, logger.LogNewErrorCodef(log, codes.Internal,
						"failed to verify if vSAN file services is supported or not. Error:%+v", err)
				}
				if !isvSANFileServicesSupported {
					return nil, csifault.CSIInternalFault, logger.LogNewErrorCode(log, codes.FailedPrecondition,
						"fileshare volume creation is not supported on vSAN 67u3 release")
				}
				return c.createFileVolume(ctx, req)
			}
		}
		volumeType = prometheus.PrometheusBlockVolumeType
		if multivCenterCSITopologyEnabled {
			return c.createBlockVolumeWithPlacementEngineForMultiVC(ctx, req)
		} else {
			return c.createBlockVolume(ctx, req)
		}
	}
	resp, faultType, err := createVolumeInternal()
	log.Debugf("createVolumeInternal: returns fault %q", faultType)
	if err != nil {
		if csifault.IsNonStorageFault(faultType) {
			faultType = csifault.AddCsiNonStoragePrefix(ctx, faultType)
		}
		log.Errorf("Operation failed, reporting failure status to Prometheus."+
			" Operation Type: %q, Volume Type: %q, Fault Type: %q",
			prometheus.PrometheusCreateVolumeOpType, volumeType, faultType)
		prometheus.CsiControlOpsHistVec.WithLabelValues(volumeType, prometheus.PrometheusCreateVolumeOpType,
			prometheus.PrometheusFailStatus, faultType).Observe(time.Since(start).Seconds())
	} else {
		log.Infof("Volume created successfully. Volume Handle: %q, PV Name: %q", resp.Volume.VolumeId, req.Name)
		prometheus.CsiControlOpsHistVec.WithLabelValues(volumeType, prometheus.PrometheusCreateVolumeOpType,
			prometheus.PrometheusPassStatus, faultType).Observe(time.Since(start).Seconds())
	}
	return resp, err
}

// DeleteVolume is deleting CNS Volume specified in DeleteVolumeRequest.
func (c *controller) DeleteVolume(ctx context.Context, req *csi.DeleteVolumeRequest) (
	*csi.DeleteVolumeResponse, error) {
	start := time.Now()
	ctx = logger.NewContextWithLogger(ctx)
	log := logger.GetLogger(ctx)
	volumeType := prometheus.PrometheusUnknownVolumeType
	cnsVolumeType := common.UnknownVolumeType

	deleteVolumeInternal := func() (
		*csi.DeleteVolumeResponse, string, error) {
		log.Infof("DeleteVolume: called with args: %+v", *req)
		// TODO: If the err is returned by invoking CNS API, then faultType should be
		// populated by the underlying layer.
		// If the request failed due to validate the request, "csi.fault.InvalidArgument" will be return.
		// If thr reqeust failed due to object not found, "csi.fault.NotFound" will be return.
		// For all other cases, the faultType will be set to "csi.fault.Internal" for now.
		// Later we may need to define different csi faults.
		var (
			faultType      string
			err            error
			volumePath     string
			volumeManager  cnsvolume.Manager
			vCenterHost    string
			vCenterManager cnsvsphere.VirtualCenterManager
		)

		err = validateVanillaDeleteVolumeRequest(ctx, req)
		if err != nil {
			return nil, csifault.CSIInvalidArgumentFault, err
		}
		if strings.Contains(req.VolumeId, ".vmdk") {
			volumeType = prometheus.PrometheusBlockVolumeType
			cnsVolumeType = common.BlockVolumeType
			// In-tree volume support.
			if !commonco.ContainerOrchestratorUtility.IsFSSEnabled(ctx, common.CSIMigration) {
				// Migration feature switch is disabled.
				return nil, csifault.CSIInternalFault, logger.LogNewErrorCodef(log, codes.Internal,
					"volume-migration feature switch is disabled. Cannot use volume with vmdk path :%q", req.VolumeId)
			}
			// Migration feature switch is enabled.
			volumePath = req.VolumeId
			// In case if feature state switch is enabled after controller is
			// deployed, we need to initialize the volumeMigrationService.
			if err := initVolumeMigrationService(ctx, c); err != nil {
				// Error is already wrapped in CSI error code.
				return nil, csifault.CSIInternalFault, err
			}
			req.VolumeId, err = volumeMigrationService.GetVolumeID(ctx, &migration.VolumeSpec{VolumePath: req.VolumeId}, false)
			if err != nil {
				return nil, csifault.CSIInternalFault, logger.LogNewErrorCodef(log, codes.Internal,
					"failed to get VolumeID from volumeMigrationService for volumePath: %q", volumePath)
			}
		}
		// Fetch vCenterHost, vCenterManager & volumeManager for given volume, based on VC configuration
		vCenterManager = getVCenterManagerForVCenter(ctx, c)
		vCenterHost, volumeManager, err = getVCenterAndVolumeManagerForVolumeID(ctx, c, req.VolumeId, volumeInfoService)
		if err != nil {
			return nil, csifault.CSIInternalFault, logger.LogNewErrorCodef(log, codes.Internal,
				"failed to get vCenter/volume manager for volume Id: %q. Error: %v", req.VolumeId, err)
		}

		if cnsVolumeType == common.UnknownVolumeType {
			cnsVolumeType, err = common.GetCnsVolumeType(ctx, volumeManager, req.VolumeId)
			if err != nil {
				if err.Error() == common.ErrNotFound.Error() {
					// The volume couldn't be found during query, assuming the delete operation as success
					return &csi.DeleteVolumeResponse{}, "", nil
				} else {
					return nil, csifault.CSIInternalFault, logger.LogNewErrorCodef(log, codes.Internal,
						"failed to determine CNS volume type for volume: %q. Error: %+v", req.VolumeId, err)
				}
			}
			volumeType = convertCnsVolumeType(ctx, cnsVolumeType)
		}
		// Check if the volume contains CNS snapshots only for block volumes.
		if cnsVolumeType == common.BlockVolumeType &&
			commonco.ContainerOrchestratorUtility.IsFSSEnabled(ctx, common.BlockVolumeSnapshot) {
			isCnsSnapshotSupported, err := vCenterManager.IsCnsSnapshotSupported(ctx, vCenterHost)
			if err != nil {
				return nil, csifault.CSIInternalFault, logger.LogNewErrorCodef(log, codes.Internal,
					"failed to check if cns snapshot operations are supported on VC due to error: %v", err)
			}
			if isCnsSnapshotSupported {
				snapshots, _, err := common.QueryVolumeSnapshotsByVolumeID(ctx, volumeManager, req.VolumeId,
					common.QuerySnapshotLimit)
				if err != nil {
					return nil, csifault.CSIInternalFault, logger.LogNewErrorCodef(log, codes.Internal,
						"failed to retrieve snapshots for volume: %s. Error: %+v", req.VolumeId, err)
				}
				if len(snapshots) == 0 {
					log.Infof("no CNS snapshots found for volume: %s, the volume can be safely deleted",
						req.VolumeId)
				} else {
					return nil, csifault.CSIInternalFault, logger.LogNewErrorCodef(log, codes.FailedPrecondition,
						"volume: %s with existing snapshots %v cannot be deleted, "+
							"please delete snapshots before deleting the volume", req.VolumeId, snapshots)
				}
			}
		}
		faultType, err = common.DeleteVolumeUtil(ctx, volumeManager, req.VolumeId, true)
		if err != nil {
			return nil, faultType, logger.LogNewErrorCodef(log, codes.Internal,
				"failed to delete volume: %q. Error: %+v", req.VolumeId, err)
		}
		// Migration feature switch is enabled and volumePath is set.
		if volumePath != "" {
			// Delete VolumePath to VolumeID mapping.
			err = volumeMigrationService.DeleteVolumeInfo(ctx, req.VolumeId)
			if err != nil {
				return nil, csifault.CSIInternalFault, logger.LogNewErrorCodef(log, codes.Internal,
					"failed to delete volumeInfo CR for volume: %q. Error: %+v", req.VolumeId, err)
			}
		}
		// If this is multi-VC configuration, delete CnsVolumeInfo CR
		if multivCenterCSITopologyEnabled && len(c.managers.VcenterConfigs) > 1 {
			err = volumeInfoService.DeleteVolumeInfo(ctx, req.VolumeId)
			if err != nil {
				return nil, csifault.CSIInternalFault, logger.LogNewErrorCodef(log, codes.Internal,
					"failed to delete cnsvolumeInfo CR for volume: %q. Error: %+v", req.VolumeId, err)
			}
		}
		return &csi.DeleteVolumeResponse{}, "", nil
	}
	resp, faultType, err := deleteVolumeInternal()
	log.Debugf("deleteVolumeInternal: returns fault %q for volume %q", faultType, req.VolumeId)
	if err != nil {
		if csifault.IsNonStorageFault(faultType) {
			faultType = csifault.AddCsiNonStoragePrefix(ctx, faultType)
		}
		log.Errorf("Operation failed, reporting failure status to Prometheus."+
			" Operation Type: %q, Volume Type: %q, Fault Type: %q",
			prometheus.PrometheusDeleteVolumeOpType, volumeType, faultType)
		prometheus.CsiControlOpsHistVec.WithLabelValues(volumeType, prometheus.PrometheusDeleteVolumeOpType,
			prometheus.PrometheusFailStatus, faultType).Observe(time.Since(start).Seconds())
	} else {
		log.Infof("Volume %q deleted successfully.", req.VolumeId)
		prometheus.CsiControlOpsHistVec.WithLabelValues(volumeType, prometheus.PrometheusDeleteVolumeOpType,
			prometheus.PrometheusPassStatus, faultType).Observe(time.Since(start).Seconds())
	}
	return resp, err
}

// ControllerPublishVolume attaches a volume to the Node VM.
// Volume id and node name is retrieved from ControllerPublishVolumeRequest.
func (c *controller) ControllerPublishVolume(ctx context.Context, req *csi.ControllerPublishVolumeRequest) (
	*csi.ControllerPublishVolumeResponse, error) {
	start := time.Now()
	ctx = logger.NewContextWithLogger(ctx)
	log := logger.GetLogger(ctx)
	volumeType := prometheus.PrometheusUnknownVolumeType

	controllerPublishVolumeInternal := func() (
		*csi.ControllerPublishVolumeResponse, string, error) {
		log.Infof("ControllerPublishVolume: called with args %+v", *req)
		// TODO: If the err is returned by invoking CNS API, then faultType should be
		// populated by the underlying layer.
		// If the request failed due to validate the request, "csi.fault.InvalidArgument" will be return.
		// If thr reqeust failed due to object not found, "csi.fault.NotFound" will be return.
		// For all other cases, the faultType will be set to "csi.fault.Internal" for now.
		// Later we may need to define different csi faults.
		err := validateVanillaControllerPublishVolumeRequest(ctx, req)
		if err != nil {

			return nil, csifault.CSIInvalidArgumentFault, logger.LogNewErrorCodef(log, codes.Internal,
				"validation for PublishVolume Request: %+v has failed. Error: %v", *req, err)
		}
		publishInfo := make(map[string]string)
		_, volumeManager, err := getVCenterAndVolumeManagerForVolumeID(ctx, c, req.VolumeId, volumeInfoService)
		if err != nil {
			return nil, csifault.CSIInternalFault, logger.LogNewErrorCodef(log, codes.Internal,
				"failed to get volume manager for volume Id: %q. Error: %v", req.VolumeId, err)
		}
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
			queryResult, err := volumeManager.QueryAllVolume(ctx, queryFilter, querySelection)
			if err != nil {
				return nil, csifault.CSIInternalFault, logger.LogNewErrorCodef(log, codes.Internal,
					"queryVolume failed for volumeID: %q with err=%+v", req.VolumeId, err)
			}
			if len(queryResult.Volumes) == 0 {
				return nil, csifault.CSIInternalFault, logger.LogNewErrorCodef(log, codes.Internal,
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
				return nil, csifault.CSIInternalFault, logger.LogNewErrorCodef(log, codes.Internal,
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
					return nil, csifault.CSIInternalFault, logger.LogNewErrorCodef(log, codes.Internal,
						"volume-migration feature switch is disabled. Cannot use volume with vmdk path :%q", req.VolumeId)
				}
				// Migration feature switch is enabled.
				storagePolicyName := req.VolumeContext[common.AttributeStoragePolicyName]
				volumePath := req.VolumeId
				// In case if feature state switch is enabled after controller is
				// deployed, we need to initialize the volumeMigrationService.
				if err := initVolumeMigrationService(ctx, c); err != nil {
					// Error is already wrapped in CSI error code.
					return nil, csifault.CSIInternalFault, err
				}
				req.VolumeId, err = volumeMigrationService.GetVolumeID(ctx,
					&migration.VolumeSpec{VolumePath: volumePath, StoragePolicyName: storagePolicyName}, false)
				if err != nil {
					return nil, csifault.CSIInternalFault, logger.LogNewErrorCodef(log, codes.Internal,
						"failed to get VolumeID from volumeMigrationService for volumePath: %q", volumePath)
				}
				err = volumeMigrationService.ProtectVolumeFromVMDeletion(ctx, req.VolumeId)
				if err != nil {
					return nil, csifault.CSIInternalFault, logger.LogNewErrorCodef(log, codes.Internal,
						"failed to set keepAfterDeleteVm control flag for VolumeID %q", req.VolumeId)
				}
			}
			var nodevm *cnsvsphere.VirtualMachine
			// if node is not yet updated to run the release of the driver publishing Node VM UUID as Node ID
			// look up Node by name
			nodevm, err = c.nodeMgr.GetNodeVMByNameOrUUID(ctx, req.NodeId)
			if err == node.ErrNodeNotFound {
				log.Infof("Performing node VM lookup using node VM UUID: %q", req.NodeId)
				nodevm, err = c.nodeMgr.GetNodeVMByUuid(ctx, req.NodeId)
			}
			if err != nil {
				return nil, csifault.CSIInternalFault, logger.LogNewErrorCodef(log, codes.Internal,
					"failed to find VirtualMachine for node:%q. Error: %v", req.NodeId, err)
			}
			log.Debugf("Found VirtualMachine for node:%q.", req.NodeId)
			// faultType is returned from manager.AttachVolume.
			diskUUID, faultType, err := common.AttachVolumeUtil(ctx, volumeManager, nodevm, req.VolumeId,
				false)
			if err != nil {
				return nil, faultType, logger.LogNewErrorCodef(log, codes.Internal,
					"failed to attach disk: %+q with node: %q err %+v", req.VolumeId, req.NodeId, err)
			}
			publishInfo[common.AttributeDiskType] = common.DiskTypeBlockVolume
			publishInfo[common.AttributeFirstClassDiskUUID] = common.FormatDiskUUID(diskUUID)
		}
		log.Infof("ControllerPublishVolume successful with publish context: %v", publishInfo)
		return &csi.ControllerPublishVolumeResponse{
			PublishContext: publishInfo,
		}, "", nil
	}
	resp, faultType, err := controllerPublishVolumeInternal()
	log.Debugf("controllerPublishVolumeInternal: returns fault %q for volume %q", faultType, req.VolumeId)
	if err != nil {
		if csifault.IsNonStorageFault(faultType) {
			faultType = csifault.AddCsiNonStoragePrefix(ctx, faultType)
		}
		log.Errorf("Operation failed, reporting failure status to Prometheus."+
			" Operation Type: %q, Volume Type: %q, Fault Type: %q",
			prometheus.PrometheusAttachVolumeOpType, volumeType, faultType)
		prometheus.CsiControlOpsHistVec.WithLabelValues(volumeType, prometheus.PrometheusAttachVolumeOpType,
			prometheus.PrometheusFailStatus, faultType).Observe(time.Since(start).Seconds())
	} else {
		log.Infof("Volume %q attached successfully to node %q.", req.VolumeId, req.NodeId)
		prometheus.CsiControlOpsHistVec.WithLabelValues(volumeType, prometheus.PrometheusAttachVolumeOpType,
			prometheus.PrometheusPassStatus, faultType).Observe(time.Since(start).Seconds())
	}
	return resp, err
}

// ControllerUnpublishVolume detaches a volume from the Node VM. Volume id and
// node name is retrieved from ControllerUnpublishVolumeRequest.
func (c *controller) ControllerUnpublishVolume(ctx context.Context, req *csi.ControllerUnpublishVolumeRequest) (
	*csi.ControllerUnpublishVolumeResponse, error) {
	start := time.Now()
	ctx = logger.NewContextWithLogger(ctx)
	log := logger.GetLogger(ctx)
	volumeType := prometheus.PrometheusUnknownVolumeType

	controllerUnpublishVolumeInternal := func() (
		*csi.ControllerUnpublishVolumeResponse, string, error) {
		var faultType string
		log.Infof("ControllerUnpublishVolume: called with args %+v", *req)
		// TODO: If the err is returned by invoking CNS API, then faultType should be
		// populated by the underlying layer.
		// If the request failed due to validate the request, "csi.fault.InvalidArgument" will be return.
		// If thr reqeust failed due to object not found, "csi.fault.NotFound" will be return.
		// For all other cases, the faultType will be set to "csi.fault.Internal" for now.
		// Later we may need to define different csi faults.
		err := validateVanillaControllerUnpublishVolumeRequest(ctx, req)
		if err != nil {
			return nil, csifault.CSIInvalidArgumentFault, logger.LogNewErrorCodef(log, codes.Internal,
				"validation for UnpublishVolume Request: %+v has failed. Error: %v", *req, err)
		}

		_, volumeManager, err := getVCenterAndVolumeManagerForVolumeID(ctx, c, req.VolumeId, volumeInfoService)
		if err != nil {
			return nil, csifault.CSIInternalFault, logger.LogNewErrorCodef(log, codes.Internal,
				"failed to get volume manager for volume Id: %q. Error: %v", req.VolumeId, err)
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
			queryResult, err := volumeManager.QueryAllVolume(ctx, queryFilter, querySelection)
			// TODO: QueryAllVolumeUtil need return faultType
			//	and we should return the faultType.
			// Currently, just return "csi.fault.Internal"
			if err != nil {
				return nil, csifault.CSIInternalFault, logger.LogNewErrorCodef(log, codes.Internal,
					"queryVolume failed for volumeID: %q with err=%+v", req.VolumeId, err)
			}

			if len(queryResult.Volumes) == 0 {
				return nil, csifault.CSIInternalFault, logger.LogNewErrorCodef(log, codes.Internal,
					"volumeID %q not found in QueryVolume", req.VolumeId)
			}
			if queryResult.Volumes[0].VolumeType == common.FileVolumeType {
				volumeType = prometheus.PrometheusFileVolumeType
				log.Infof("Skipping ControllerUnpublish for file volume %q", req.VolumeId)
				return &csi.ControllerUnpublishVolumeResponse{}, "", nil
			}
		} else {
			// In-tree volume support.
			volumeType = prometheus.PrometheusBlockVolumeType
			if !commonco.ContainerOrchestratorUtility.IsFSSEnabled(ctx, common.CSIMigration) {
				// Migration feature switch is disabled.
				return nil, csifault.CSIInternalFault, logger.LogNewErrorCodef(log, codes.Internal,
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
				return nil, csifault.CSIInternalFault, err
			}
			req.VolumeId, err = volumeMigrationService.GetVolumeID(ctx, &migration.VolumeSpec{VolumePath: volumePath}, false)
			if err != nil {
				return nil, csifault.CSIInternalFault, logger.LogNewErrorCodef(log, codes.Internal,
					"failed to get VolumeID from volumeMigrationService for volumePath: %q", volumePath)
			}
		}
		// Block Volume.
		volumeType = prometheus.PrometheusBlockVolumeType
		var nodevm *cnsvsphere.VirtualMachine
		// if node is not yet updated to run the release of the driver publishing Node VM UUID as Node ID
		// look up Node by name
		nodevm, err = c.nodeMgr.GetNodeVMByNameOrUUID(ctx, req.NodeId)
		if err == node.ErrNodeNotFound {
			log.Infof("Performing node VM lookup using node VM UUID: %q", req.NodeId)
			nodevm, err = c.nodeMgr.GetNodeVMByUuid(ctx, req.NodeId)
		}
		if err != nil {
			if err == cnsvsphere.ErrVMNotFound {
				log.Infof("Virtual Machine for Node ID: %v is not present in the VC Inventory. "+
					"Marking ControllerUnpublishVolume for Volume: %q as successful.", req.NodeId, req.VolumeId)
				return &csi.ControllerUnpublishVolumeResponse{}, "", nil
			} else {
				return nil, csifault.CSIInternalFault, logger.LogNewErrorCodef(log, codes.Internal,
					"failed to find VirtualMachine for node:%q. Error: %v", req.NodeId, err)
			}
		}
		faultType, err = common.DetachVolumeUtil(ctx, volumeManager, nodevm, req.VolumeId)
		if err != nil {
			return nil, faultType, logger.LogNewErrorCodef(log, codes.Internal,
				"failed to detach disk: %+q from node: %q err %+v", req.VolumeId, req.NodeId, err)
		}
		log.Infof("ControllerUnpublishVolume successful for volume ID: %s", req.VolumeId)
		return &csi.ControllerUnpublishVolumeResponse{}, "", nil
	}
	resp, faultType, err := controllerUnpublishVolumeInternal()
	log.Debugf("controllerUnpublishVolumeInternal: returns fault %q for volume %q", faultType, req.VolumeId)
	if err != nil {
		if csifault.IsNonStorageFault(faultType) {
			faultType = csifault.AddCsiNonStoragePrefix(ctx, faultType)
		}
		log.Errorf("Operation failed, reporting failure status to Prometheus."+
			" Operation Type: %q, Volume Type: %q, Fault Type: %q",
			prometheus.PrometheusDetachVolumeOpType, volumeType, faultType)
		prometheus.CsiControlOpsHistVec.WithLabelValues(volumeType, prometheus.PrometheusDetachVolumeOpType,
			prometheus.PrometheusFailStatus, faultType).Observe(time.Since(start).Seconds())
	} else {
		log.Infof("Volume %q detached successfully from node %q.", req.VolumeId, req.NodeId)
		prometheus.CsiControlOpsHistVec.WithLabelValues(volumeType, prometheus.PrometheusDetachVolumeOpType,
			prometheus.PrometheusPassStatus, faultType).Observe(time.Since(start).Seconds())
	}
	return resp, err
}

// ControllerExpandVolume expands a volume.
// Volume id and size is retrieved from ControllerExpandVolumeRequest.
func (c *controller) ControllerExpandVolume(ctx context.Context, req *csi.ControllerExpandVolumeRequest) (
	*csi.ControllerExpandVolumeResponse, error) {
	start := time.Now()
	ctx = logger.NewContextWithLogger(ctx)
	log := logger.GetLogger(ctx)
	volumeType := prometheus.PrometheusUnknownVolumeType
	controllerExpandVolumeInternal := func() (
		*csi.ControllerExpandVolumeResponse, string, error) {
		var (
			vCenterHost    string
			vCenterManager cnsvsphere.VirtualCenterManager
			volumeManager  cnsvolume.Manager
			err            error
			faultType      string
		)

		log.Infof("ControllerExpandVolume: called with args %+v", *req)
		// TODO: If the err is returned by invoking CNS API, then faultType should be
		// populated by the underlying layer.
		// If the request failed due to validate the request, "csi.fault.InvalidArgument" will be return.
		// If thr reqeust failed due to object not found, "csi.fault.NotFound" will be return.
		// For all other cases, the faultType will be set to "csi.fault.Internal" for now.
		// Later we may need to define different csi faults.

		// csifault.CSIInternalFault csifault.CSIUnimplementedFault csifault.CSIInvalidArgumentFault
		if strings.Contains(req.VolumeId, ".vmdk") {
			if err := initVolumeMigrationService(ctx, c); err != nil {
				// Error is already wrapped in CSI error code.
				return nil, csifault.CSIInternalFault, err
			}
			req.VolumeId, err = volumeMigrationService.GetVolumeID(ctx, &migration.VolumeSpec{VolumePath: req.VolumeId}, false)
			if err != nil {
				return nil, csifault.CSIInternalFault, logger.LogNewErrorCodef(log, codes.Internal,
					"failed to get VolumeID from volumeMigrationService for volumePath: %q", req.VolumeId)
			}
		}

		// Fetch vCenterHost, vCenterManager & volumeManager for given volume, based on VC configuration
		vCenterManager = getVCenterManagerForVCenter(ctx, c)
		vCenterHost, volumeManager, err = getVCenterAndVolumeManagerForVolumeID(ctx, c, req.VolumeId, volumeInfoService)
		if err != nil {
			return nil, csifault.CSIInternalFault, logger.LogNewErrorCodef(log, codes.Internal,
				"failed to get vCenter/volume manager for volume Id: %q. Error: %v", req.VolumeId, err)
		}

		isOnlineExpansionSupported, err := vCenterManager.IsOnlineExtendVolumeSupported(ctx, vCenterHost)
		if err != nil {
			return nil, csifault.CSIInternalFault, logger.LogNewErrorCodef(log, codes.Internal,
				"failed to check if online expansion is supported due to error: %v", err)
		}
		isOnlineExpansionEnabled := commonco.ContainerOrchestratorUtility.IsFSSEnabled(ctx, common.OnlineVolumeExtend)
		err = validateVanillaControllerExpandVolumeRequest(ctx, req, isOnlineExpansionEnabled, isOnlineExpansionSupported)
		if err != nil {
			msg := fmt.Sprintf("validation for ExpandVolume Request: %+v has failed. Error: %v", *req, err)
			log.Error(msg)
			return nil, csifault.CSIInternalFault, err
		}
		volumeType = prometheus.PrometheusBlockVolumeType

		volumeID := req.GetVolumeId()
		volSizeBytes := int64(req.GetCapacityRange().GetRequiredBytes())
		volSizeMB := int64(common.RoundUpSize(volSizeBytes, common.MbInBytes))
		// Check if the volume contains CNS snapshots.
		if commonco.ContainerOrchestratorUtility.IsFSSEnabled(ctx, common.BlockVolumeSnapshot) {
			isCnsSnapshotSupported, err := vCenterManager.IsCnsSnapshotSupported(ctx, vCenterHost)
			if err != nil {
				return nil, csifault.CSIInternalFault, logger.LogNewErrorCodef(log, codes.Internal,
					"failed to check if cns snapshot is supported on VC due to error: %v", err)
			}
			if isCnsSnapshotSupported {
				snapshots, _, err := common.QueryVolumeSnapshotsByVolumeID(ctx, volumeManager, volumeID,
					common.QuerySnapshotLimit)
				if err != nil {
					return nil, csifault.CSIInternalFault, logger.LogNewErrorCodef(log, codes.Internal,
						"failed to retrieve snapshots for volume: %s. Error: %+v", volumeID, err)
				}
				if len(snapshots) == 0 {
					log.Infof("The volume %s can be safely expanded as no CNS snapshots were found.",
						req.VolumeId)
				} else {
					return nil, csifault.CSIInvalidArgumentFault, logger.LogNewErrorCodef(log, codes.FailedPrecondition,
						"volume: %s with existing snapshots %v cannot be expanded. "+
							"Please delete snapshots before expanding the volume", req.VolumeId, snapshots)
				}
			}
		}

		faultType, err = common.ExpandVolumeUtil(ctx, vCenterManager, vCenterHost, volumeManager, volumeID,
			volSizeMB, commonco.ContainerOrchestratorUtility.IsFSSEnabled(ctx, common.AsyncQueryVolume), nil)
		if err != nil {
			return nil, faultType, logger.LogNewErrorCodef(log, codes.Internal,
				"failed to expand volume: %q to size: %d with error: %+v", "df", volSizeMB, err)
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
		log.Debugf("ControllerExpandVolumeInternal: returns %v as capacity and %v as NodeExpansionRequired",
			int64(units.FileSize(volSizeMB*common.MbInBytes)), nodeExpansionRequired)
		resp := &csi.ControllerExpandVolumeResponse{
			CapacityBytes:         int64(units.FileSize(volSizeMB * common.MbInBytes)),
			NodeExpansionRequired: nodeExpansionRequired,
		}
		return resp, "", nil
	}

	resp, faultType, err := controllerExpandVolumeInternal()
	if err != nil {
		log.Debugf("controllerExpandVolumeInternal: returns fault %q for volume %q", faultType, req.VolumeId)
		if csifault.IsNonStorageFault(faultType) {
			faultType = csifault.AddCsiNonStoragePrefix(ctx, faultType)
		}
		log.Errorf("Operation failed, reporting failure status to Prometheus."+
			" Operation Type: %q, Volume Type: %q, Fault Type: %q",
			prometheus.PrometheusExpandVolumeOpType, volumeType, faultType)
		prometheus.CsiControlOpsHistVec.WithLabelValues(volumeType, prometheus.PrometheusExpandVolumeOpType,
			prometheus.PrometheusFailStatus, faultType).Observe(time.Since(start).Seconds())
	} else {
		log.Infof("Volume %q expanded successfully.", req.VolumeId)
		prometheus.CsiControlOpsHistVec.WithLabelValues(volumeType, prometheus.PrometheusExpandVolumeOpType,
			prometheus.PrometheusPassStatus, faultType).Observe(time.Since(start).Seconds())
	}
	return resp, err
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
	start := time.Now()
	ctx = logger.NewContextWithLogger(ctx)
	log := logger.GetLogger(ctx)
	volumeType := prometheus.PrometheusUnknownVolumeType
	if !commonco.ContainerOrchestratorUtility.IsFSSEnabled(ctx, common.ListVolumes) {
		return nil, logger.LogNewErrorCode(log, codes.Unimplemented, "List Volumes")
	}
	cfg, err := cnsconfig.GetConfig(ctx)
	if err != nil {
		return nil, logger.LogNewErrorf(log, "failed to read config. Error: %+v", err)
	}

	// Get the global query limit
	maxEntries := cfg.Global.QueryLimit
	if req.MaxEntries != 0 {
		maxEntries = int(req.MaxEntries)
	}

	listVolumesInternal := func() (*csi.ListVolumesResponse, string, error) {
		log.Debugf("ListVolumes: called with args %+v", *req)

		startingToken := 0
		if req.StartingToken != "" {
			startingToken, err = strconv.Atoi(req.StartingToken)
			if err != nil {
				log.Errorf("Unable to convert startingToken from string to int err=%v", err)
				return nil, csifault.CSIInvalidArgumentFault, logger.LogNewErrorCode(log, codes.InvalidArgument,
					"startingToken not a valid integer")
			}
		}

		// Step 1: Get all the volume IDs of PVs, from K8s cluster
		// If startingToken is 0, then listVolume request is a new one and not part of a previous request.
		// Therefore, fetch all the volumes from K8s and CNS.
		if startingToken == 0 || len(CNSVolumesforListVolume) == 0 {
			volIDsInK8s = commonco.ContainerOrchestratorUtility.GetAllK8sVolumes()
			log.Debugf("Number of Volume IDs of PVs from K8s cluster %v, list of volumes %v", len(volIDsInK8s),
				volIDsInK8s)
			querySelection := cnstypes.CnsQuerySelection{
				Names: []string{
					string(cnstypes.QuerySelectionNameTypeVolumeType),
					string(cnstypes.QuerySelectionNameTypeVolumeName),
				},
			}
			// For multi-VC configuration, query volumes from all vCenters
			if multivCenterCSITopologyEnabled {
				var cnsVolumes = make([]cnstypes.CnsVolume, 0)
				for vcHost, volumeManager := range c.managers.VolumeManagers {
					cnsQueryResult, err := utils.QueryAllVolumesForCluster(ctx, volumeManager,
						cfg.Global.ClusterID, querySelection)
					if err != nil {
						return nil, csifault.CSIInternalFault, logger.LogNewErrorCodef(log, codes.Internal,
							"queryVolume failed on Cluster ID %q for vCenter %s with err = %+v ",
							cfg.Global.ClusterID, vcHost, err)
					}
					cnsVolumes = append(cnsVolumes, cnsQueryResult.Volumes...)
				}
				CNSVolumesforListVolume = cnsVolumes
			} else {
				queryFilter := cnstypes.CnsQueryFilter{
					ContainerClusterIds: []string{cfg.Global.ClusterID},
				}
				cnsQueryResult, err := c.manager.VolumeManager.QueryAllVolume(ctx, queryFilter, querySelection)
				if err != nil {
					return nil, csifault.CSIInternalFault, logger.LogNewErrorCodef(log, codes.Internal,
						"queryVolume failed on Cluster ID %q with err = %+v ", cfg.Global.ClusterID, err)
				}
				CNSVolumesforListVolume = cnsQueryResult.Volumes
			}

			// Get all nodes from the vanilla K8s cluster from the node manager
			allNodeVMs, err := c.nodeMgr.GetAllNodes(ctx)
			if err != nil {
				return nil, csifault.CSIInternalFault, logger.LogNewErrorCodef(log, codes.Internal,
					"failed to get nodes(node vms) in the vanilla cluster. Error: %v", err)
			}

			// Fetching below map once per resync cycle to be used later while processing the volumes
			volumeIDToNodeUUIDMap, err = getBlockVolumeIDToNodeUUIDMap(ctx, c, allNodeVMs)
			if err != nil {
				return nil, csifault.CSIInternalFault, logger.LogNewErrorCodef(log, codes.Internal,
					"get block volumeIDToNodeUUIDMap failed with err = %+v ", err)
			}
		}

		// Step 3: If the difference between number of K8s volumes and CNS volumes is greater than threshold,
		// fail the operation, as it can result in too many attach calls.
		if len(volIDsInK8s)-len(CNSVolumesforListVolume) > cfg.Global.ListVolumeThreshold {
			log.Errorf("difference between number of K8s volumes: %d, and CNS volumes: %d, is greater than "+
				"threshold: %d, and completely out of sync.", len(volIDsInK8s), len(CNSVolumesforListVolume),
				cfg.Global.ListVolumeThreshold)
			return nil, csifault.CSIInternalFault, logger.LogNewErrorCodef(log, codes.FailedPrecondition,
				"difference between number of K8s volumes and CNS volumes is greater than threshold.")
		}

		if maxEntries > len(CNSVolumesforListVolume) {
			maxEntries = len(CNSVolumesforListVolume)
		}
		// Step 4: process queryLimit number of items starting from ListVolumeRequest.start_token
		var entries []*csi.ListVolumesResponse_Entry

		nextToken := ""
		log.Debugf("Starting token: %d, Length of Query volume result: %d, Max entries: %d ",
			startingToken, len(CNSVolumesforListVolume), maxEntries)
		entries, nextToken, volumeType, err = c.processQueryResultsListVolumes(ctx, startingToken, maxEntries,
			CNSVolumesforListVolume)
		if err != nil {
			return nil, csifault.CSIInternalFault, fmt.Errorf("error while processing query results for list "+
				" volumes, err: %v", err)
		}
		resp := &csi.ListVolumesResponse{
			Entries:   entries,
			NextToken: nextToken,
		}

		log.Debugf("ListVolumes served %d results, token for next set: %s", len(entries), nextToken)
		return resp, "", nil
	}
	listVolResponse, faultType, err := listVolumesInternal()
	log.Debugf("List volume response: %+v", listVolResponse)
	if err != nil {
		if csifault.IsNonStorageFault(faultType) {
			faultType = csifault.AddCsiNonStoragePrefix(ctx, faultType)
		}
		log.Errorf("Operation failed, reporting failure status to Prometheus."+
			" Operation Type: %q, Volume Type: %q, Fault Type: %q",
			prometheus.PrometheusListVolumeOpType, volumeType, faultType)
		prometheus.CsiControlOpsHistVec.WithLabelValues(volumeType, prometheus.PrometheusListVolumeOpType,
			prometheus.PrometheusFailStatus, faultType).Observe(time.Since(start).Seconds())
	} else {
		prometheus.CsiControlOpsHistVec.WithLabelValues(volumeType, prometheus.PrometheusListVolumeOpType,
			prometheus.PrometheusPassStatus, faultType).Observe(time.Since(start).Seconds())
	}
	return listVolResponse, err
}

func (c *controller) processQueryResultsListVolumes(ctx context.Context, startingToken int, maxEntries int,
	cnsVolumes []cnstypes.CnsVolume) ([]*csi.ListVolumesResponse_Entry,
	string, string, error) {

	volumeType := ""
	nextToken := ""
	volCounter := 0
	nextTokenCounter := 0
	log := logger.GetLogger(ctx)
	var entries []*csi.ListVolumesResponse_Entry

	for i := startingToken; i < len(cnsVolumes); i++ {
		if cnsVolumes[i].VolumeType == common.FileVolumeType {
			// If this is multi-VC configuration, then
			// skip processing query results for file volumes
			if multivCenterCSITopologyEnabled && len(c.managers.VcenterConfigs) > 1 {
				isTopologyAwareFileVolumeEnabled := commonco.ContainerOrchestratorUtility.IsFSSEnabled(ctx,
					common.TopologyAwareFileVolume)
				if !isTopologyAwareFileVolumeEnabled {
					log.Debugf("Skipping processing for file volume %v in multi-VC configuration", cnsVolumes[i].Name)
					continue
				}
			}

			volumeType = prometheus.PrometheusFileVolumeType
			fileVolID := cnsVolumes[i].VolumeId.Id

			// Populate csi.Volume info for the given volume
			fileVolumeInfo := &csi.Volume{
				VolumeId: fileVolID,
			}
			// Getting published nodes
			publishedNodeIds := commonco.ContainerOrchestratorUtility.GetNodesForVolumes(ctx, []string{fileVolID})
			for volID, nodeName := range publishedNodeIds {
				if volID == fileVolID && len(nodeName) != 0 {
					nodeVMObj, err := c.nodeMgr.GetNodeVMByNameAndUpdateCache(ctx, publishedNodeIds[fileVolID][0])
					if err != nil {
						log.Errorf("Failed to get node vm object from the node name, err:%v", err)
						return entries, nextToken, volumeType, err
					}
					volCounter += 1
					nodeVMUUID := nodeVMObj.UUID

					// Populate published node
					volStatus := &csi.ListVolumesResponse_VolumeStatus{
						PublishedNodeIds: []string{nodeVMUUID},
					}

					// Populate List Volumes Entry Response
					entry := &csi.ListVolumesResponse_Entry{
						Volume: fileVolumeInfo,
						Status: volStatus,
					}

					entries = append(entries, entry)
				}
			}
			if volCounter == maxEntries {
				nextTokenCounter = i + 1
				break
			}
		} else {
			volumeType = prometheus.PrometheusBlockVolumeType
			blockVolID := cnsVolumes[i].VolumeId.Id
			nodeVMUUID, found := volumeIDToNodeUUIDMap[blockVolID]
			if found {
				volCounter += 1
				volumeId := blockVolID
				// this check is required as volumeMigrationService is not initialized
				// when multi-vc is enabled and there is more than 1 vc
				if volumeMigrationService != nil {
					migratedVolumePath, err := volumeMigrationService.GetVolumePathFromMigrationServiceCache(ctx, blockVolID)
					if err != nil && err == common.ErrNotFound {
						log.Debugf("volumeID: %v not found in migration service in-memory cache "+
							"so it's not a migrated in-tree volume", blockVolID)
					} else if migratedVolumePath != "" {
						volumeId = migratedVolumePath
					}
				}
				// Populate csi.Volume info for the given volume
				blockVolumeInfo := &csi.Volume{
					VolumeId: volumeId,
				}
				// Getting published nodes
				volStatus := &csi.ListVolumesResponse_VolumeStatus{
					PublishedNodeIds: []string{nodeVMUUID},
				}
				entry := &csi.ListVolumesResponse_Entry{
					Volume: blockVolumeInfo,
					Status: volStatus,
				}
				// Populate List Volumes Entry Response
				entries = append(entries, entry)
				if volCounter == maxEntries {
					nextTokenCounter = i + 1
					break
				}
			}
		}
	}

	// if nextTokenCounter = 0, it means all cns volumes were processed(i = len(cnsVolumes)), and
	// fewer than 'maxEntries' no. of volumes were found. So nextToken is empty.
	// If nextTokenCounter is not 0 and there are more volumes to process, then set the nextToken
	if nextTokenCounter != 0 && len(cnsVolumes) > nextTokenCounter {
		nextToken = strconv.Itoa(nextTokenCounter)
	}

	return entries, nextToken, volumeType, nil
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

	// If this is multi-VC config, return without initializing service
	// currently CSI migration is not supported in multi-VC config
	if multivCenterCSITopologyEnabled {
		if len(c.managers.VcenterConfigs) > 1 {
			// Multi-VC case
			return logger.LogNewErrorf(log,
				"volume-migration feature is not supported on Multi-vCenter deployment")
		} else {
			// Single-VC case
			volumeManager := c.managers.VolumeManagers[c.managers.CnsConfig.Global.VCenterIP]
			volumeMigrationService, err = migration.GetVolumeMigrationService(ctx,
				&volumeManager, c.managers.CnsConfig, false)
		}
	} else {
		volumeMigrationService, err = migration.GetVolumeMigrationService(ctx,
			&c.manager.VolumeManager, c.manager.CnsConfig, false)
	}
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
		csi.ControllerServiceCapability_RPC_CREATE_DELETE_SNAPSHOT,
		csi.ControllerServiceCapability_RPC_LIST_SNAPSHOTS,
	}

	if commonco.ContainerOrchestratorUtility.IsFSSEnabled(ctx, common.ListVolumes) {
		controllerCaps = append(controllerCaps, csi.ControllerServiceCapability_RPC_LIST_VOLUMES,
			csi.ControllerServiceCapability_RPC_LIST_VOLUMES_PUBLISHED_NODES)
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
	var (
		vCenterHost                              string
		vCenterManager                           cnsvsphere.VirtualCenterManager
		volumeManager                            cnsvolume.Manager
		err                                      error
		maxSnapshotsPerBlockVolume               int
		granularMaxSnapshotsPerBlockVolumeInVSAN int
		granularMaxSnapshotsPerBlockVolumeInVVOL int
	)
	log.Infof("CreateSnapshot: called with args %+v", *req)

	isBlockVolumeSnapshotEnabled := commonco.ContainerOrchestratorUtility.IsFSSEnabled(ctx, common.BlockVolumeSnapshot)
	if !isBlockVolumeSnapshotEnabled {
		return nil, logger.LogNewErrorCode(log, codes.Unimplemented, "createSnapshot")
	}

	volumeID := req.GetSourceVolumeId()
	// Fetch vCenterHost, vCenterManager & volumeManager for given snapshot, based on VC configuration
	vCenterManager = getVCenterManagerForVCenter(ctx, c)
	vCenterHost, volumeManager, err = getVCenterAndVolumeManagerForVolumeID(ctx, c, volumeID, volumeInfoService)
	if err != nil {
		return nil, logger.LogNewErrorCodef(log, codes.Internal,
			"failed to get vCenter/volume manager for volume Id: %q. Error: %v", volumeID, err)
	}

	isCnsSnapshotSupported, err := vCenterManager.IsCnsSnapshotSupported(ctx, vCenterHost)
	if err != nil {
		return nil, logger.LogNewErrorCodef(log, codes.Internal,
			"failed to check if cns snapshot is supported on VC due to error: %v", err)
	}
	if !isCnsSnapshotSupported {
		return nil, logger.LogNewErrorCode(log, codes.Unimplemented,
			"VC version does not support snapshot operations")
	}
	volumeType := prometheus.PrometheusUnknownVolumeType
	createSnapshotInternal := func() (*csi.CreateSnapshotResponse, error) {
		// Validate CreateSnapshotRequest
		if err := validateVanillaCreateSnapshotRequestRequest(ctx, req); err != nil {
			return nil, logger.LogNewErrorCodef(log, codes.Internal,
				"validation for CreateSnapshot Request: %+v has failed. Error: %v", *req, err)
		}

		// Check if the source volume is migrated vSphere volume
		if strings.Contains(volumeID, ".vmdk") {
			return nil, logger.LogNewErrorCodef(log, codes.Unimplemented,
				"cannot snapshot migrated vSphere volume. :%q", volumeID)
		}
		volumeType = prometheus.PrometheusBlockVolumeType
		// Query capacity in MB and datastore url for block volume snapshot
		volumeIds := []cnstypes.CnsVolumeId{{Id: volumeID}}
		cnsVolumeDetailsMap, err := utils.QueryVolumeDetailsUtil(ctx, volumeManager, volumeIds)
		if err != nil {
			return nil, err
		}
		if _, ok := cnsVolumeDetailsMap[volumeID]; !ok {
			return nil, logger.LogNewErrorCodef(log, codes.Internal,
				"cns query volume did not return the volume: %s", volumeID)
		}
		snapshotSizeInMB := cnsVolumeDetailsMap[volumeID].SizeInMB
		datastoreUrl := cnsVolumeDetailsMap[volumeID].DatastoreUrl
		if cnsVolumeDetailsMap[volumeID].VolumeType != common.BlockVolumeType {
			return nil, logger.LogNewErrorCodef(log, codes.FailedPrecondition,
				"queried volume doesn't have the expected volume type. Expected VolumeType: %v. "+
					"Queried VolumeType: %v", volumeType, cnsVolumeDetailsMap[volumeID].VolumeType)
		}
		// Check if snapshots number of this volume reaches the granular limit on VSAN/VVOL
		if multivCenterCSITopologyEnabled {
			maxSnapshotsPerBlockVolume = c.managers.CnsConfig.Snapshot.GlobalMaxSnapshotsPerBlockVolume
			granularMaxSnapshotsPerBlockVolumeInVSAN =
				c.managers.CnsConfig.Snapshot.GranularMaxSnapshotsPerBlockVolumeInVSAN
			granularMaxSnapshotsPerBlockVolumeInVVOL =
				c.managers.CnsConfig.Snapshot.GranularMaxSnapshotsPerBlockVolumeInVVOL
		} else {
			maxSnapshotsPerBlockVolume = c.manager.CnsConfig.Snapshot.GlobalMaxSnapshotsPerBlockVolume
			granularMaxSnapshotsPerBlockVolumeInVSAN =
				c.manager.CnsConfig.Snapshot.GranularMaxSnapshotsPerBlockVolumeInVSAN
			granularMaxSnapshotsPerBlockVolumeInVVOL =
				c.manager.CnsConfig.Snapshot.GranularMaxSnapshotsPerBlockVolumeInVVOL
		}
		log.Infof("The limit of the maximum number of snapshots per block volume is "+
			"set to the global maximum (%v) by default.", maxSnapshotsPerBlockVolume)

		if granularMaxSnapshotsPerBlockVolumeInVSAN > 0 || granularMaxSnapshotsPerBlockVolumeInVVOL > 0 {
			var isGranularMaxEnabled bool
			if strings.Contains(datastoreUrl, strings.ToLower(string(types.HostFileSystemVolumeFileSystemTypeVsan))) {
				if granularMaxSnapshotsPerBlockVolumeInVSAN > 0 {
					maxSnapshotsPerBlockVolume = granularMaxSnapshotsPerBlockVolumeInVSAN
					isGranularMaxEnabled = true
				}
			} else if strings.Contains(datastoreUrl, strings.ToLower(string(types.HostFileSystemVolumeFileSystemTypeVVOL))) {
				if granularMaxSnapshotsPerBlockVolumeInVVOL > 0 {
					maxSnapshotsPerBlockVolume = granularMaxSnapshotsPerBlockVolumeInVVOL
					isGranularMaxEnabled = true
				}
			}

			if isGranularMaxEnabled {
				log.Infof("The limit of the maximum number of snapshots per block volume on datastore %q is "+
					"overridden by the granular maximum (%v).", datastoreUrl, maxSnapshotsPerBlockVolume)
			}
		}

		// Check if snapshots number of this volume reaches the limit
		snapshotList, _, err := common.QueryVolumeSnapshotsByVolumeID(ctx, volumeManager, volumeID,
			common.QuerySnapshotLimit)
		if err != nil {
			return nil, logger.LogNewErrorCodef(log, codes.Internal,
				"failed to query snapshots of volume %s for the limit check. Error: %v", volumeID, err)
		}

		if len(snapshotList) >= maxSnapshotsPerBlockVolume {
			return nil, logger.LogNewErrorCodef(log, codes.FailedPrecondition,
				"the number of snapshots on the source volume %s reaches the configured maximum (%v)",
				volumeID, maxSnapshotsPerBlockVolume)
		}

		// the returned snapshotID below is a combination of CNS VolumeID and CNS SnapshotID concatenated by the "+"
		// sign. That is, a string of "<UUID>+<UUID>". Because, all other CNS snapshot APIs still require both
		// VolumeID and SnapshotID as the input, while corresponding snapshot APIs in upstream CSI require SnapshotID.
		// So, we need to bridge the gap in vSphere CSI driver and return a combined SnapshotID to CSI Snapshotter.
		snapshotID, cnsSnapshotInfo, err := common.CreateSnapshotUtil(ctx, volumeManager, volumeID, req.Name, nil)
		if err != nil {
			return nil, logger.LogNewErrorCodef(log, codes.Internal,
				"failed to create snapshot on volume %q with error: %v", volumeID, err)
		}
		snapshotCreateTimeInProto := timestamppb.New(cnsSnapshotInfo.SnapshotLatestOperationCompleteTime)

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
			cnsSnapshotInfo.SnapshotLatestOperationCompleteTime, createSnapshotResponse)
		return createSnapshotResponse, nil
	}

	start := time.Now()
	resp, err := createSnapshotInternal()
	if err != nil {
		log.Errorf("Operation failed, reporting failure status to Prometheus."+
			" Operation Type: %q, Volume Type: %q, Fault Type: %q",
			prometheus.PrometheusCreateSnapshotOpType, volumeType, "NotComputed")
		prometheus.CsiControlOpsHistVec.WithLabelValues(volumeType, prometheus.PrometheusCreateSnapshotOpType,
			prometheus.PrometheusFailStatus, "NotComputed").Observe(time.Since(start).Seconds())
	} else {
		log.Infof("Snapshot for volume %q created successfully.", req.GetSourceVolumeId())
		prometheus.CsiControlOpsHistVec.WithLabelValues(volumeType, prometheus.PrometheusCreateSnapshotOpType,
			prometheus.PrometheusPassStatus, "").Observe(time.Since(start).Seconds())
	}
	return resp, err
}

func (c *controller) DeleteSnapshot(ctx context.Context, req *csi.DeleteSnapshotRequest) (
	*csi.DeleteSnapshotResponse, error) {
	ctx = logger.NewContextWithLogger(ctx)
	log := logger.GetLogger(ctx)
	var (
		vCenterHost    string
		vCenterManager cnsvsphere.VirtualCenterManager
		volumeManager  cnsvolume.Manager
		err            error
	)
	log.Infof("DeleteSnapshot: called with args %+v", *req)

	isBlockVolumeSnapshotEnabled :=
		commonco.ContainerOrchestratorUtility.IsFSSEnabled(ctx, common.BlockVolumeSnapshot)
	if !isBlockVolumeSnapshotEnabled {
		return nil, logger.LogNewErrorCode(log, codes.Unimplemented, "deleteSnapshot")
	}

	volumeID, _, err := common.ParseCSISnapshotID(req.SnapshotId)
	if err != nil {
		return nil, logger.LogNewErrorCode(log, codes.InvalidArgument, err.Error())
	}
	// Fetch vCenterHost, vCenterManager & volumeManager for given snapshot, based on VC configuration
	vCenterManager = getVCenterManagerForVCenter(ctx, c)
	vCenterHost, volumeManager, err = getVCenterAndVolumeManagerForVolumeID(ctx, c, volumeID, volumeInfoService)
	if err != nil {
		return nil, logger.LogNewErrorCodef(log, codes.Internal,
			"failed to get vCenter/volume manager for snapshot Id: %q. Error: %v", req.SnapshotId, err)
	}

	isCnsSnapshotSupported, err := vCenterManager.IsCnsSnapshotSupported(ctx, vCenterHost)
	if err != nil {
		return nil, logger.LogNewErrorCodef(log, codes.Internal,
			"failed to check if cns snapshot is supported on VC due to error: %v", err)
	}
	if !isCnsSnapshotSupported {
		return nil, logger.LogNewErrorCode(log, codes.Unimplemented,
			"VC version does not support snapshot operations")
	}

	deleteSnapshotInternal := func() (*csi.DeleteSnapshotResponse, error) {
		csiSnapshotID := req.GetSnapshotId()
		_, err := common.DeleteSnapshotUtil(ctx, volumeManager, csiSnapshotID, nil)
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
		log.Errorf("Operation failed, reporting failure status to Prometheus."+
			" Operation Type: %q, Volume Type: %q, Fault Type: %q",
			prometheus.PrometheusDeleteSnapshotOpType, volumeType, "NotComputed")
		prometheus.CsiControlOpsHistVec.WithLabelValues(volumeType, prometheus.PrometheusDeleteSnapshotOpType,
			prometheus.PrometheusFailStatus, "NotComputed").Observe(time.Since(start).Seconds())
	} else {
		log.Infof("Snapshot %q deleted successfully.", req.SnapshotId)
		prometheus.CsiControlOpsHistVec.WithLabelValues(volumeType, prometheus.PrometheusDeleteSnapshotOpType,
			prometheus.PrometheusPassStatus, "").Observe(time.Since(start).Seconds())
	}
	return resp, err
}

func (c *controller) ListSnapshots(ctx context.Context, req *csi.ListSnapshotsRequest) (
	*csi.ListSnapshotsResponse, error) {
	start := time.Now()
	ctx = logger.NewContextWithLogger(ctx)
	log := logger.GetLogger(ctx)
	volumeType := prometheus.PrometheusBlockVolumeType

	listSnapshotsInternal := func() (*csi.ListSnapshotsResponse, error) {
		var (
			vCenterManager cnsvsphere.VirtualCenterManager
			volManager     cnsvolume.Manager
			vCenterHost    string
			snapshots      []*csi.Snapshot
			nextToken      string
			err            error
		)
		log.Infof("ListSnapshots: called with args %+v", *req)
		err = validateVanillaListSnapshotRequest(ctx, req)
		if err != nil {
			return nil, err
		}
		maxEntries := common.QuerySnapshotLimit
		if req.MaxEntries != 0 {
			maxEntries = int64(req.MaxEntries)
		}
		// Fetch vCenterHost, vCenterManager & volumeManager for given snapshot, based on input given and
		// query snapshot records from respective VC
		if req.SnapshotId != "" {
			// Retrieve specific snapshot information.
			// The snapshotID is of format: <volume-id>+<snapshot-id>
			volID, _, err := common.ParseCSISnapshotID(req.SnapshotId)
			if err != nil {
				log.Errorf("Unable to determine the volume-id and snapshot-id")
				return nil, err
			}
			// Fetch vCenterHost & volumeManager for source volume, based on VC configuration
			vCenterManager = getVCenterManagerForVCenter(ctx, c)
			vCenterHost, volManager, err = getVCenterAndVolumeManagerForVolumeID(ctx, c, volID, volumeInfoService)
			if err != nil {
				return nil, logger.LogNewErrorCodef(log, codes.Internal,
					"failed to get vCenter/volume manager for volume Id: %q in VC %s. Error: %v", volID, vCenterHost, err)
			}
			// Check for snapshot support
			isCnsSnapshotSupported, err := vCenterManager.IsCnsSnapshotSupported(ctx, vCenterHost)
			if err != nil {
				return nil, logger.LogNewErrorCodef(log, codes.Internal,
					"failed to check if cns snapshot is supported on VC %s due to error: %v", vCenterHost, err)
			}
			if !isCnsSnapshotSupported {
				return nil, logger.LogNewErrorCodef(log, codes.Unimplemented,
					"VC %s version does not support snapshot operations", vCenterHost)
			}
			snapshots, nextToken, err = common.ListSnapshotsUtil(ctx, volManager, req.SourceVolumeId,
				req.SnapshotId, req.StartingToken, maxEntries)
			if err != nil {
				return nil, logger.LogNewErrorCodef(log, codes.Internal, " failed to retrieve the snapshots, err: %+v", err)
			}
		} else if req.SourceVolumeId != "" {
			// Fetch vCenterHost & volumeManager for source volume, based on VC configuration
			vCenterManager = getVCenterManagerForVCenter(ctx, c)
			vCenterHost, volManager, err = getVCenterAndVolumeManagerForVolumeID(ctx, c, req.SourceVolumeId, volumeInfoService)
			if err != nil {
				return nil, logger.LogNewErrorCodef(log, codes.Internal,
					"failed to get vCenter/volume manager for volume Id: %q in VC %s. Error: %v", req.SourceVolumeId, vCenterHost, err)
			}
			// Check for snapshot support
			isCnsSnapshotSupported, err := vCenterManager.IsCnsSnapshotSupported(ctx, vCenterHost)
			if err != nil {
				return nil, logger.LogNewErrorCodef(log, codes.Internal,
					"failed to check if cns snapshot is supported on VC %s due to error: %v", vCenterHost, err)
			}
			if !isCnsSnapshotSupported {
				return nil, logger.LogNewErrorCodef(log, codes.Unimplemented,
					"VC %s version does not support snapshot operations", vCenterHost)
			}
			snapshots, nextToken, err = common.ListSnapshotsUtil(ctx, volManager, req.SourceVolumeId,
				req.SnapshotId, req.StartingToken, maxEntries)
			if err != nil {
				return nil, logger.LogNewErrorCodef(log, codes.Internal, " failed to retrieve the snapshots, err: %+v", err)
			}
		} else {
			snapshots, nextToken, err = queryAllVolumeSnapshotsForMultiVC(ctx, c, req.StartingToken, maxEntries)
			if err != nil {
				return nil, logger.LogNewErrorCodef(log, codes.Internal, " failed to retrieve the snapshots, err: %+v", err)
			}
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
		log.Errorf("Operation failed, reporting failure status to Prometheus."+
			" Operation Type: %q, Volume Type: %q, Fault Type: %q",
			prometheus.PrometheusListSnapshotsOpType, volumeType, "NotComputed")
		prometheus.CsiControlOpsHistVec.WithLabelValues(volumeType, prometheus.PrometheusListSnapshotsOpType,
			prometheus.PrometheusFailStatus, "NotComputed").Observe(time.Since(start).Seconds())
	} else {
		prometheus.CsiControlOpsHistVec.WithLabelValues(volumeType, prometheus.PrometheusListSnapshotsOpType,
			prometheus.PrometheusPassStatus, "").Observe(time.Since(start).Seconds())
	}
	return resp, err
}

// getSnapshots(): This func queries snapshot for given VC and returns the query results along with corresponding
// source volume details
func getSnapshotsAndSourceVolumeDetails(ctx context.Context, vCenterManager cnsvsphere.VirtualCenterManager,
	volManager cnsvolume.Manager, vcHost string) ([]cnstypes.CnsSnapshotQueryResultEntry,
	map[string]*utils.CnsVolumeDetails, error) {
	var (
		err                 error
		snapshotQueryFilter cnstypes.CnsSnapshotQueryFilter
		snapQueryEntries    = make([]cnstypes.CnsSnapshotQueryResultEntry, 0)
		volumeIds           []cnstypes.CnsVolumeId
	)
	log := logger.GetLogger(ctx)
	// Check for snapshot support
	isCnsSnapshotSupported, err := vCenterManager.IsCnsSnapshotSupported(ctx, vcHost)
	if err != nil {
		return nil, nil, logger.LogNewErrorCodef(log, codes.Internal,
			"failed to check if cns snapshot is supported on VC %s due to error: %v", vcHost, err)
	}
	if !isCnsSnapshotSupported {
		return nil, nil, logger.LogNewErrorCodef(log, codes.Unimplemented,
			"VC %s version does not support snapshot operations", vcHost)
	}
	// fetch all snapshot records for each VC
	snapshotQueryFilter = cnstypes.CnsSnapshotQueryFilter{
		Cursor: &cnstypes.CnsCursor{
			Offset: 0,
			Limit:  utils.DefaultQuerySnapshotLimit,
		},
	}
	for {
		queryResult, err := volManager.QuerySnapshots(ctx, snapshotQueryFilter)
		if err != nil {
			log.Errorf("getSnapshots failed for vCenter %s with snapfilter %v. Err=%+v",
				vcHost, snapshotQueryFilter, err)
			return nil, nil, err
		}
		log.Debugf("querySnapshots returned %d entries for VC %s", len(queryResult.Entries), vcHost)
		snapQueryEntries = append(snapQueryEntries, queryResult.Entries...)
		// If cursor offset < total snapshot records found, then there are more snapshots
		// to be queried, so continue the loop to fetch remaining records
		if queryResult.Cursor.Offset >= queryResult.Cursor.TotalRecords {
			break
		}
		// Assign cursor for next iteration based on previous query result
		snapshotQueryFilter.Cursor = &queryResult.Cursor
	}
	// populate list of volume-ids to retrieve the volume size.
	for _, queryResult := range snapQueryEntries {
		if queryResult.Error != nil {
			return nil, nil, logger.LogNewErrorCodef(log, codes.Internal,
				"faults are not expected when invoking QuerySnapshots without volume-id and snapshot-id, fault: %+v",
				queryResult.Error.Fault)
		}
		volumeIds = append(volumeIds, queryResult.Snapshot.VolumeId)
	}
	// TODO: Retrieve Snapshot size directly from CnsQuerySnapshot once supported.
	volumeDetails, err := utils.QueryVolumeDetailsUtil(ctx, volManager, volumeIds)
	if err != nil {
		log.Errorf("failed to retrieve volume details for volume-ids: %v for vCenter %s, err: %+v",
			volumeIds, vcHost, err)
		return nil, nil, err
	}
	return snapQueryEntries, volumeDetails, nil
}

// queryAllVolumeSnapshotsForMultiVC(): This func fetches all volume snapshots from all vCenters configured
// and filters the result as per token provided
func queryAllVolumeSnapshotsForMultiVC(ctx context.Context, c *controller, token string,
	maxEntries int64) ([]*csi.Snapshot, string, error) {
	var (
		err              error
		csiSnapshots     []*csi.Snapshot
		nextToken        string
		finalSnapEntries []cnstypes.CnsSnapshotQueryResultEntry
		snapQueryEntries = make([]cnstypes.CnsSnapshotQueryResultEntry, 0)
	)
	log := logger.GetLogger(ctx)
	startingToken := 0
	if token != "" {
		startingToken, err = strconv.Atoi(token)
		if err != nil {
			log.Errorf("failed to parse the token: %s err: %v", token, err)
			return nil, "", err
		}
	}
	// Get all snapshot entries from available vCenters
	// startingToken = 0 indicates listSnapshots called as a new request and not part of
	// previous request. Hence fetch all snapshots from CNS
	if startingToken == 0 || len(CNSSnapshotsForListSnapshots) == 0 {
		// For multi-VC configuration, query volumes from all vCenters
		vCenterManager := getVCenterManagerForVCenter(ctx, c)
		if multivCenterCSITopologyEnabled {
			var cnsVolumeDetailsMap = make([]map[string]*utils.CnsVolumeDetails, 0)
			for vcHost, volManager := range c.managers.VolumeManagers {
				// fetch snapshots and associated source volume details
				perVCSnapQueryEntries, volumeDetails, err := getSnapshotsAndSourceVolumeDetails(ctx, vCenterManager,
					volManager, vcHost)
				if err != nil {
					log.Errorf("failed to retrieve snapshots/source volume info for vCenter %s, err: %+v", vcHost, err)
					return nil, "", err
				}
				snapQueryEntries = append(snapQueryEntries, perVCSnapQueryEntries...)
				cnsVolumeDetailsMap = append(cnsVolumeDetailsMap, volumeDetails)
			}
			CNSSnapshotsForListSnapshots = snapQueryEntries
			CNSVolumeDetailsMap = cnsVolumeDetailsMap
		} else {
			// fetch snapshots
			snapQueryEntries, volumeDetails, err := getSnapshotsAndSourceVolumeDetails(ctx, vCenterManager,
				c.manager.VolumeManager, c.manager.VcenterConfig.Host)
			if err != nil {
				log.Errorf("failed to retrieve snapshots/source volume info for vCenter %s, err: %+v",
					c.manager.VcenterConfig.Host, err)
				return nil, "", err
			}
			CNSVolumeDetailsMap = append(CNSVolumeDetailsMap, volumeDetails)
			CNSSnapshotsForListSnapshots = snapQueryEntries
		}
	}
	// Process snapshot query result obtained from configured vCenter(s) and return final response
	finalSnapEntries, nextToken, err = processQueryResultsListSnapshots(ctx, startingToken,
		maxEntries, CNSSnapshotsForListSnapshots)
	if err != nil {
		return nil, csifault.CSIInternalFault, fmt.Errorf("error while processing query results for list "+
			" snapshots, err: %v", err)
	}
	for _, queryResult := range finalSnapEntries {
		snapshotCreateTimeInProto := timestamppb.New(queryResult.Snapshot.CreateTime)
		csiSnapshotId := queryResult.Snapshot.VolumeId.Id +
			common.VSphereCSISnapshotIdDelimiter + queryResult.Snapshot.SnapshotId.Id
		// For every snapshot, find corresponding source volume details and fill the CSISnapshot response entry
		for _, cnsVolumeDetailsMap := range CNSVolumeDetailsMap {
			if _, ok := cnsVolumeDetailsMap[queryResult.Snapshot.VolumeId.Id]; !ok {
				continue
			}
			csiSnapshotSize := cnsVolumeDetailsMap[queryResult.Snapshot.VolumeId.Id].SizeInMB * common.MbInBytes
			csiSnapshotInfo := &csi.Snapshot{
				SnapshotId:     csiSnapshotId,
				SourceVolumeId: queryResult.Snapshot.VolumeId.Id,
				CreationTime:   snapshotCreateTimeInProto,
				SizeBytes:      csiSnapshotSize,
				ReadyToUse:     true,
			}
			csiSnapshots = append(csiSnapshots, csiSnapshotInfo)
			break
		}
	}
	log.Debugf("queryAllVolumeSnapshotsForMultiVC served %d results out of total %d entries, token for next set: %s",
		len(csiSnapshots), len(CNSSnapshotsForListSnapshots), nextToken)
	return csiSnapshots, nextToken, nil
}

// processQueryResultsListSnapshots(): This func filters the list of snapshots as per token given
func processQueryResultsListSnapshots(ctx context.Context, startingToken int,
	maxEntries int64,
	snapshotQueryResultEntries []cnstypes.CnsSnapshotQueryResultEntry) ([]cnstypes.CnsSnapshotQueryResultEntry,
	string, error) {
	nextToken := ""
	nextTokenCounter := 0
	var snapEntries = make([]cnstypes.CnsSnapshotQueryResultEntry, 0)
	for i := startingToken; i < len(snapshotQueryResultEntries); i++ {
		if len(snapEntries) == int(maxEntries) {
			nextTokenCounter = i
			break
		}
		snapEntries = append(snapEntries, snapshotQueryResultEntries[i])
	}
	if nextTokenCounter != 0 && len(snapshotQueryResultEntries) > nextTokenCounter {
		nextToken = strconv.Itoa(nextTokenCounter)
	}
	return snapEntries, nextToken, nil
}

func (c *controller) ControllerGetVolume(ctx context.Context, req *csi.ControllerGetVolumeRequest) (
	*csi.ControllerGetVolumeResponse, error) {
	ctx = logger.NewContextWithLogger(ctx)
	log := logger.GetLogger(ctx)
	log.Infof("ControllerGetVolume: called with args %+v", *req)
	return nil, logger.LogNewErrorCode(log, codes.Unimplemented, "controllerGetVolume")
}

func (c *controller) ControllerModifyVolume(ctx context.Context, req *csi.ControllerModifyVolumeRequest) (
	*csi.ControllerModifyVolumeResponse, error) {
	ctx = logger.NewContextWithLogger(ctx)
	log := logger.GetLogger(ctx)
	log.Infof("ControllerModifyVolume: called with args %+v", *req)
	return nil, logger.LogNewErrorCode(log, codes.Unimplemented, "ControllerModifyVolume")
}
