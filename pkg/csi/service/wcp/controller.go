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

package wcp

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/container-storage-interface/spec/lib/go/csi"
	"github.com/davecgh/go-spew/spew"
	"github.com/fsnotify/fsnotify"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	cnstypes "github.com/vmware/govmomi/cns/types"
	"github.com/vmware/govmomi/units"
	"github.com/vmware/govmomi/vim25/types"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"
	v1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	"k8s.io/apimachinery/pkg/util/wait"

	"sigs.k8s.io/vsphere-csi-driver/v3/pkg/common/cns-lib/crypto"
	cnsvolume "sigs.k8s.io/vsphere-csi-driver/v3/pkg/common/cns-lib/volume"
	cnsvsphere "sigs.k8s.io/vsphere-csi-driver/v3/pkg/common/cns-lib/vsphere"
	cnsconfig "sigs.k8s.io/vsphere-csi-driver/v3/pkg/common/config"
	csifault "sigs.k8s.io/vsphere-csi-driver/v3/pkg/common/fault"
	"sigs.k8s.io/vsphere-csi-driver/v3/pkg/common/prometheus"
	"sigs.k8s.io/vsphere-csi-driver/v3/pkg/common/utils"
	"sigs.k8s.io/vsphere-csi-driver/v3/pkg/csi/service/common"
	"sigs.k8s.io/vsphere-csi-driver/v3/pkg/csi/service/common/commonco"
	commoncotypes "sigs.k8s.io/vsphere-csi-driver/v3/pkg/csi/service/common/commonco/types"
	"sigs.k8s.io/vsphere-csi-driver/v3/pkg/csi/service/logger"
	csitypes "sigs.k8s.io/vsphere-csi-driver/v3/pkg/csi/types"
	"sigs.k8s.io/vsphere-csi-driver/v3/pkg/internalapis/cnsvolumeinfo"
	cnsvolumeinfov1alpha1 "sigs.k8s.io/vsphere-csi-driver/v3/pkg/internalapis/cnsvolumeinfo/v1alpha1"
	"sigs.k8s.io/vsphere-csi-driver/v3/pkg/internalapis/cnsvolumeoperationrequest"
)

const (
	vsanDirect = "vsanD"
	vsanSna    = "vsan-sna"
	// allowedRetriesToPatchCNSVolumeInfo retry allowed for patching CNSVolumeInfo with snapshot details
	allowedRetriesToPatchCNSVolumeInfo = 5
)

var (
	// operationStore represents the client by which we can
	// interact with VolumeOperationRequest interface.
	operationStore cnsvolumeoperationrequest.VolumeOperationRequest
	// controllerCaps represents the capability of controller service.
	controllerCaps = []csi.ControllerServiceCapability_RPC_Type{
		csi.ControllerServiceCapability_RPC_CREATE_DELETE_VOLUME,
		csi.ControllerServiceCapability_RPC_PUBLISH_UNPUBLISH_VOLUME,
		csi.ControllerServiceCapability_RPC_EXPAND_VOLUME,
		csi.ControllerServiceCapability_RPC_CREATE_DELETE_SNAPSHOT,
		csi.ControllerServiceCapability_RPC_LIST_SNAPSHOTS,
	}
	// volumeInfoService holds the pointer to VolumeInfo service instance
	// This will hold mapping for VolumeID to Storage policy info for PodVMOnStretchedSupervisor deployments
	volumeInfoService cnsvolumeinfo.VolumeInfoService
	// isPodVMOnStretchSupervisorFSSEnabled is true when PodVMOnStretchedSupervisor FSS is enabled.
	isPodVMOnStretchSupervisorFSSEnabled bool
)

var getCandidateDatastores = cnsvsphere.GetCandidateDatastoresInCluster

// Contains list of clusterComputeResourceMoIds on which supervisor cluster is deployed.
var clusterComputeResourceMoIds = make([]string, 0)

var (
	expectedStartingIndex             = 0
	cnsVolumeIDs                      = make([]string, 0)
	vmMoidToHostMoid, volumeIDToVMMap map[string]string
)

type controller struct {
	manager     *common.Manager
	authMgr     common.AuthorizationService
	topologyMgr commoncotypes.ControllerTopologyService
}

// New creates a CNS controller.
func New() csitypes.CnsController {
	return &controller{}
}

// Init is initializing controller struct.
func (c *controller) Init(config *cnsconfig.Config, version string) error {
	ctx, log := logger.GetNewContextWithLogger()
	log.Infof("Initializing WCP CSI controller")
	var err error
	if !config.Global.InsecureFlag && config.Global.CAFile != cnsconfig.SupervisorCAFilePath {
		log.Warnf("Invalid CA file: %q is set in the vSphere Config Secret. "+
			"Setting correct CA file: %q", config.Global.CAFile, cnsconfig.SupervisorCAFilePath)
		config.Global.CAFile = cnsconfig.SupervisorCAFilePath
	}

	if commonco.ContainerOrchestratorUtility.IsFSSEnabled(ctx, common.TKGsHA) {
		clusterComputeResourceMoIds, err = common.GetClusterComputeResourceMoIds(ctx)
		if err != nil {
			log.Errorf("failed to get clusterComputeResourceMoIds. err: %v", err)
			return err
		}
		if len(clusterComputeResourceMoIds) > 0 {
			if config.Global.SupervisorID == "" {
				return logger.LogNewError(log, "supervisor-id is not set in the vsphere-config-secret")
			}
		} else {
			// This will cover the case when VC is upgraded to 8.0+ but any of the existing pre-8.0 supervisor clusters
			// are not upgraded along with it. Such supervisor clusters will not have a AZ CR in it.
			clusterComputeResourceMoIds = append(clusterComputeResourceMoIds, config.Global.ClusterID)
		}
	}

	// Get VirtualCenterManager instance and validate version.
	vcenterconfig, err := cnsvsphere.GetVirtualCenterConfig(ctx, config)
	if err != nil {
		log.Errorf("failed to get VirtualCenterConfig. err=%v", err)
		return err
	}
	vcenterconfig.ReloadVCConfigForNewClient = true
	vcManager := cnsvsphere.GetVirtualCenterManager(ctx)
	vcenter, err := vcManager.RegisterVirtualCenter(ctx, vcenterconfig)
	if err != nil {
		log.Errorf("failed to register VC with virtualCenterManager. err=%v", err)
		return err
	}

	isPodVMOnStretchSupervisorFSSEnabled = commonco.ContainerOrchestratorUtility.IsFSSEnabled(ctx,
		common.PodVMOnStretchedSupervisor)
	idempotencyHandlingEnabled := commonco.ContainerOrchestratorUtility.IsFSSEnabled(ctx,
		common.CSIVolumeManagerIdempotency)
	if idempotencyHandlingEnabled {
		log.Info("CSI Volume manager idempotency handling feature flag is enabled.")
		operationStore, err = cnsvolumeoperationrequest.InitVolumeOperationRequestInterface(ctx,
			config.Global.CnsVolumeOperationRequestCleanupIntervalInMin,
			func() bool {
				return commonco.ContainerOrchestratorUtility.IsFSSEnabled(ctx, common.BlockVolumeSnapshot)
			}, isPodVMOnStretchSupervisorFSSEnabled)
		if err != nil {
			log.Errorf("failed to initialize VolumeOperationRequestInterface with error: %v", err)
			return err
		}
	}

	volumeManager, err := cnsvolume.GetManager(ctx, vcenter, operationStore,
		idempotencyHandlingEnabled, false,
		false, cnstypes.CnsClusterFlavorWorkload)
	if err != nil {
		return logger.LogNewErrorf(log, "failed to create an instance of volume manager. err=%v", err)
	}

	var cryptoClient crypto.Client

	if commonco.ContainerOrchestratorUtility.IsFSSEnabled(ctx, common.WCP_VMService_BYOK) {
		var err error
		if cryptoClient, err = crypto.NewClientWithDefaultConfig(ctx); err != nil {
			return logger.LogNewErrorf(log, "failed to create an instance of crypto client. err=%v", err)
		}
	}

	c.manager = &common.Manager{
		VcenterConfig:  vcenterconfig,
		CnsConfig:      config,
		VolumeManager:  volumeManager,
		VcenterManager: cnsvsphere.GetVirtualCenterManager(ctx),
		CryptoClient:   cryptoClient,
	}

	vc, err := common.GetVCenter(ctx, c.manager)
	if err != nil {
		log.Errorf("failed to get vcenter. err=%v", err)
		return err
	}

	// Check vCenter API Version against 6.7.3.
	err = common.CheckAPI(ctx, vc.Client.ServiceContent.About.ApiVersion, common.MinSupportedVCenterMajor,
		common.MinSupportedVCenterMinor, common.MinSupportedVCenterPatch)
	if err != nil {
		log.Errorf("checkAPI failed for vcenter API version: %s, err=%v", vc.Client.ServiceContent.About.ApiVersion, err)
		return err
	}

	go cnsvolume.ClearTaskInfoObjects()
	go cnsvolume.ClearInvalidTasksFromListView(false)
	cfgPath := cnsconfig.GetConfigPath(ctx)
	watcher, err := fsnotify.NewWatcher()
	if err != nil {
		log.Errorf("failed to create fsnotify watcher. err=%v", err)
		return err
	}

	log.Info("loading AuthorizationService")
	authMgr, err := common.GetAuthorizationService(ctx, vc)
	if err != nil {
		log.Errorf("failed to initialize authMgr. err=%v", err)
		return err
	}
	c.authMgr = authMgr
	// TODO: Invoke similar method for block volumes.
	go common.ComputeFSEnabledClustersToDsMap(authMgr.(*common.AuthManager), config.Global.CSIAuthCheckIntervalInMin)
	// Create dynamic informer for AvailabilityZone instance if FSS is enabled
	// and CR is present in environment.
	if commonco.ContainerOrchestratorUtility.IsFSSEnabled(ctx, common.TKGsHA) {
		// Initialize volume topology service.
		c.topologyMgr, err = commonco.ContainerOrchestratorUtility.InitTopologyServiceInController(ctx)
		if err != nil {
			log.Errorf("failed to initialize topology service. Error: %+v", err)
			return err
		}
	}
	if isPodVMOnStretchSupervisorFSSEnabled {
		log.Info("Loading CnsVolumeInfo Service to persist mapping for VolumeID to storage policy info")
		volumeInfoService, err = cnsvolumeinfo.InitVolumeInfoService(ctx)
		if err != nil {
			return logger.LogNewErrorf(log, "error initializing volumeInfoService. Error: %+v", err)
		}
		log.Infof("Successfully initialized VolumeInfoService")
	}

	cfgDirPath := filepath.Dir(cfgPath)
	log.Infof("Adding watch on path: %q", cfgDirPath)
	err = watcher.Add(cfgDirPath)
	if err != nil {
		log.Errorf("failed to watch on path: %q. err=%v", cfgDirPath, err)
		return err
	}
	caFileDirPath := filepath.Dir(cnsconfig.SupervisorCAFilePath)
	log.Infof("Adding watch on path: %q", caFileDirPath)
	err = watcher.Add(caFileDirPath)
	if err != nil {
		log.Errorf("failed to watch on path: %q. err=%v", caFileDirPath, err)
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
				expectedEvent :=
					strings.Contains(event.Name, cfgDirPath) && !strings.Contains(event.Name, caFileDirPath)
				if event.Op&fsnotify.Remove == fsnotify.Remove && expectedEvent {
					for {
						reloadConfigErr := c.ReloadConfiguration(false)
						if reloadConfigErr == nil {
							log.Infof("Successfully reloaded configuration from: %q", cfgPath)
							break
						}
						log.Errorf("failed to reload configuration. will retry again in 60 seconds. err: %+v", reloadConfigErr)
						time.Sleep(60 * time.Second)
					}
				}
				// Handling create event for reconnecting to VC when ca file is
				// rotated. In Supervisor cluster, ca file gets rotated at the path
				// /etc/vmware/wcp/tls/vmca.pem. WCP is handling ca file rotation by
				// creating a /etc/vmware/wcp/tls/vmca.pem.tmp file with new
				// contents, and then renaming the file back to
				// /etc/vmware/wcp/tls/vmca.pem. For such operations, fsnotify
				// handles the event as a CREATE event. The condition below also
				// ensures that the event is for the expected ca file path.
				if event.Op&fsnotify.Create == fsnotify.Create && event.Name == cnsconfig.SupervisorCAFilePath {
					log.Infof("Observed ca file rotation at: %q", cnsconfig.SupervisorCAFilePath)
					for {
						reconnectVCErr := c.ReloadConfiguration(true)
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
// The function takes a boolean reconnectToVCFromNewConfig as ainputs.
// If reconnectToVCFromNewConfig is set to true, the function re-establishes
// connection with VC, else based on the configuration data changed during
// reload, the function resets config, reloads VC connection when credentials
// are changed and returns appropriate error.
func (c *controller) ReloadConfiguration(reconnectToVCFromNewConfig bool) error {
	ctx, log := logger.GetNewContextWithLogger()
	log.Info("Reloading Configuration")
	cfg, err := cnsconfig.GetConfig(ctx)
	if err != nil {
		log.Errorf("failed to read config. Error: %+v", err)
		return err
	}

	if !cfg.Global.InsecureFlag && cfg.Global.CAFile != cnsconfig.SupervisorCAFilePath {
		log.Warnf("Invalid CA file: %q is set in the vSphere Config Secret. "+
			"Setting correct CA file: %q", cfg.Global.CAFile, cnsconfig.SupervisorCAFilePath)
		cfg.Global.CAFile = cnsconfig.SupervisorCAFilePath
	}
	newVCConfig, err := cnsvsphere.GetVirtualCenterConfig(ctx, cfg)
	if err != nil {
		log.Errorf("failed to get VirtualCenterConfig. err=%v", err)
		return err
	}
	if newVCConfig != nil {
		newVCConfig.ReloadVCConfigForNewClient = true
		var vcenter *cnsvsphere.VirtualCenter
		if c.manager.VcenterConfig.Host != newVCConfig.Host ||
			c.manager.VcenterConfig.Username != newVCConfig.Username ||
			c.manager.VcenterConfig.Password != newVCConfig.Password || reconnectToVCFromNewConfig {

			// Verify if new configuration has valid credentials by connecting to
			// vCenter. Proceed only if the connection succeeds, else return error.
			newVC := &cnsvsphere.VirtualCenter{Config: newVCConfig, ClientMutex: &sync.Mutex{}}
			if err = newVC.Connect(ctx); err != nil {
				return logger.LogNewErrorf(log, "failed to connect to VirtualCenter host: %q, Err: %+v",
					newVCConfig.Host, err)
			}

			// Reset virtual center singleton instance by passing reload flag as
			// true.
			log.Info("Obtaining new vCenterInstance")
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
		idempotencyHandlingEnabled := commonco.ContainerOrchestratorUtility.IsFSSEnabled(ctx,
			common.CSIVolumeManagerIdempotency)
		if idempotencyHandlingEnabled {
			log.Info("CSI Volume manager idempotency handling feature flag is enabled.")
			operationStore, err = cnsvolumeoperationrequest.InitVolumeOperationRequestInterface(ctx,
				c.manager.CnsConfig.Global.CnsVolumeOperationRequestCleanupIntervalInMin,
				func() bool {
					return commonco.ContainerOrchestratorUtility.IsFSSEnabled(ctx, common.BlockVolumeSnapshot)
				}, isPodVMOnStretchSupervisorFSSEnabled)
			if err != nil {
				log.Errorf("failed to initialize VolumeOperationRequestInterface with error: %v", err)
				return err
			}
		}
		err := c.manager.VolumeManager.ResetManager(ctx, vcenter)
		if err != nil {
			return logger.LogNewErrorf(log, "failed to reset volume manager. err=%v", err)
		}
		c.manager.VcenterConfig = newVCConfig

		volumeManager, err := cnsvolume.GetManager(ctx, vcenter, operationStore,
			idempotencyHandlingEnabled, false,
			false, cnstypes.CnsClusterFlavorWorkload)
		if err != nil {
			return logger.LogNewErrorf(log, "failed to create an instance of volume manager. err=%v", err)
		}
		c.manager.VolumeManager = volumeManager
		if c.authMgr != nil {
			c.authMgr.ResetvCenterInstance(ctx, vcenter)
			log.Debugf("Updated vCenter in auth manager")
		}
	}
	if cfg != nil {
		c.manager.CnsConfig = cfg
		log.Debugf("Updated manager.CnsConfig")
	}
	log.Info("Successfully reloaded configuration")
	return nil
}

// createBlockVolume creates a block volume based on the CreateVolumeRequest.
func (c *controller) createBlockVolume(ctx context.Context, req *csi.CreateVolumeRequest,
	isWorkloadDomainIsolationEnabled bool) (
	*csi.CreateVolumeResponse, string, error) {
	log := logger.GetLogger(ctx)
	var (
		storagePolicyID      string
		affineToHost         string
		storagePool          string
		selectedDatastoreURL string
		storageTopologyType  string
		pvcName              string
		pvcNamespace         string
		topologyRequirement  *csi.TopologyRequirement
		// accessibleNodes will be used to populate volumeAccessTopology.
		accessibleNodes      []string
		sharedDatastores     []*cnsvsphere.DatastoreInfo
		vsanDirectDatastores []*cnsvsphere.DatastoreInfo
		hostnameLabelPresent bool
		zoneLabelPresent     bool
		err                  error
	)
	isVdppOnStretchedSVEnabled := commonco.ContainerOrchestratorUtility.IsFSSEnabled(ctx, common.VdppOnStretchedSupervisor)
	// Support case insensitive parameters.
	for paramName := range req.Parameters {
		param := strings.ToLower(paramName)
		switch param {
		case common.AttributeStoragePolicyID:
			storagePolicyID = req.Parameters[paramName]
		case common.AttributeStoragePool:
			storagePool = req.Parameters[paramName]
		case common.AttributeStorageTopologyType:
			// This case will never be reached if StorageTopologyType
			// is not present in the list of parameters.
			// TKGS-HA: validate storageTopologyType.
			storageTopologyType = req.Parameters[paramName]
			val := strings.ToLower(storageTopologyType)
			if val != "zonal" {
				if isVdppOnStretchedSVEnabled && val == "hostlocal" {
					log.Debugf("StorageTopologyType HostLocal is accepted.")
				} else {
					return nil, csifault.CSIInvalidArgumentFault, logger.LogNewErrorCodef(log, codes.InvalidArgument,
						"invalid value found for StorageClass parameter `storagetopologytype`: %q.",
						storageTopologyType)
				}
			}
		case common.AttributePvcName:
			pvcName = req.Parameters[paramName]
		case common.AttributePvcNamespace:
			pvcNamespace = req.Parameters[paramName]
		}
	}

	// Get VC instance.
	vc, err := common.GetVCenter(ctx, c.manager)
	// TODO: Need to extract fault from err returned by GetVirtualCenter.
	// Currently, just return "csi.fault.Internal".
	if err != nil {
		return nil, csifault.CSIInternalFault, logger.LogNewErrorCodef(log, codes.Internal,
			"failed to get vCenter from Manager. Error: %v", err)
	}
	// Fetch the accessibility requirements from the request.
	topologyRequirement = req.GetAccessibilityRequirements()
	filterSuspendedDatastores := commonco.ContainerOrchestratorUtility.IsFSSEnabled(ctx, common.CnsMgrSuspendCreateVolume)
	isTKGSHAEnabled := commonco.ContainerOrchestratorUtility.IsFSSEnabled(ctx, common.TKGsHA)
	isCSITransactionSupportEnabled := commonco.ContainerOrchestratorUtility.IsFSSEnabled(ctx, common.CSITranSactionSupport)

	topoSegToDatastoresMap := make(map[string][]*cnsvsphere.DatastoreInfo)
	if isTKGSHAEnabled {
		// TKGS-HA feature is enabled
		// Identify the topology keys in Accessibility requirements and infer the environment type based on these keys.

		// If both zone and hostname labels are present in the request -
		// it's host local volume provisioning on a stretched supervisor.

		// If only zone label is present in the request -
		// it's non-host local volume provisioning on a stretched supervisor.

		// If only hostname label is present in the request -
		// it's host local volume provisioning on a non-stretched supervisor.

		// If neither zone nor hostname label is present in the request -
		// In VC 8.x, it's non-host local volume provisioning on a non-stretched supervisor.
		// In 9.x, it is not a supported use case. We require all supervisor clusters to use zone
		// keys as topology requirement during volume provisioning.
		hostnameLabelPresent, zoneLabelPresent = checkTopologyKeysFromAccessibilityReqs(topologyRequirement)
		if zoneLabelPresent && hostnameLabelPresent {
			if isVdppOnStretchedSVEnabled {
				log.Infof("Host Local volume provisioning with requirement: %+v", topologyRequirement)
			} else {
				return nil, csifault.CSIUnimplementedFault, logger.LogNewErrorCodef(log, codes.Unimplemented,
					"support for topology requirement with both zone and hostname labels is not yet implemented.")
			}
		} else if zoneLabelPresent {
			if !isWorkloadDomainIsolationEnabled {
				if storageTopologyType == "" {
					return nil, csifault.CSIInvalidArgumentFault, logger.LogNewErrorCode(log, codes.InvalidArgument,
						"StorageTopologyType is unset while topology label is present")
				}
			}
			// topologyMgr can be nil if the AZ CR was not registered
			// at the time of controller init. Handling that case in CreateVolume calls.
			if c.topologyMgr == nil {
				return nil, csifault.CSIInternalFault, logger.LogNewErrorCode(log, codes.Internal,
					"topology manager not initialized.")
			}
			// Initiate TKGs HA workflow when the topology requirement contains zone labels only.
			log.Infof("Topology aware environment detected with requirement: %+v", topologyRequirement)
			sharedDatastores, err = c.topologyMgr.GetSharedDatastoresInTopology(ctx,
				commoncotypes.WCPTopologyFetchDSParams{
					TopologyRequirement:    topologyRequirement,
					Vc:                     vc,
					TopoSegToDatastoresMap: topoSegToDatastoresMap})
			if err != nil {
				return nil, csifault.CSIInternalFault, logger.LogNewErrorCodef(log, codes.Internal,
					"failed to find shared datastores for given topology requirement. Error: %v", err)
			}
		} else if hostnameLabelPresent && isVdppOnStretchedSVEnabled {
			log.Infof("Host Local volume provisioning with requirement: %+v", topologyRequirement)
		} else {
			// No topology labels present in the topologyRequirement
			if isWorkloadDomainIsolationEnabled {
				return nil, csifault.CSIInternalFault, logger.LogNewErrorCode(log, codes.Internal,
					"volume provisioning request received without topologyRequirement.")
			}
			if len(clusterComputeResourceMoIds) > 1 {
				return nil, csifault.CSIInternalFault, logger.LogNewErrorCodef(log, codes.FailedPrecondition,
					"stretched supervisor cluster does not support creating volumes "+
						"without zone keys in the topologyRequirement.")
			}
			sharedDatastores, vsanDirectDatastores, err = getCandidateDatastores(ctx, vc,
				clusterComputeResourceMoIds[0], true)
			if err != nil {
				return nil, csifault.CSIInternalFault, logger.LogNewErrorCodef(log, codes.Internal,
					"failed finding candidate datastores to place volume. Error: %v", err)
			}
		}
	} else {
		// TKGS-HA feature is disabled
		sharedDatastores, vsanDirectDatastores, err = getCandidateDatastores(ctx, vc,
			c.manager.CnsConfig.Global.ClusterID, true)
		if err != nil {
			return nil, csifault.CSIInternalFault, logger.LogNewErrorCodef(log, codes.Internal,
				"failed finding candidate datastores to place volume. Error: %v", err)
		}
	}
	candidateDatastores := append(sharedDatastores, vsanDirectDatastores...)
	if storagePool != "" {
		if !isValidAccessibilityRequirement(topologyRequirement) {
			return nil, csifault.CSIInvalidArgumentFault, logger.LogNewErrorCode(log, codes.InvalidArgument,
				"invalid accessibility requirements")
		}
		spAccessibleNodes, storagePoolType, err := getStoragePoolInfo(ctx, storagePool)
		if err != nil {
			return nil, csifault.CSIInternalFault, logger.LogNewErrorCodef(log, codes.Internal,
				"error in specified StoragePool %s. Error: %+v", storagePool, err)
		}

		overlappingNodes, err := getOverlappingNodes(spAccessibleNodes, topologyRequirement)
		if err != nil || len(overlappingNodes) == 0 {
			return nil, csifault.CSIInternalFault, logger.LogNewErrorCodef(log, codes.Internal,
				"getOverlappingNodes failed: %v", err)
		}
		accessibleNodes = append(accessibleNodes, overlappingNodes...)
		log.Infof("Storage pool Accessible nodes for volume topology: %+v", accessibleNodes)

		if storagePoolType == vsanDirect {
			selectedDatastoreURL, err = getDatastoreURLFromStoragePool(ctx, storagePool)
			if err != nil {
				return nil, csifault.CSIInternalFault, logger.LogNewErrorCodef(log, codes.Internal,
					"error in specified StoragePool %s. Error: %+v", storagePool, err)
			}
			log.Infof("Will select datastore %s as per the provided storage pool %s", selectedDatastoreURL, storagePool)
		} else if storagePoolType == vsanSna {
			// Query API server to get ESX Host Moid from the hostLocalNodeName.
			if len(accessibleNodes) != 1 {
				return nil, csifault.CSIInternalFault, logger.LogNewErrorCode(log, codes.Internal,
					"too many accessible nodes")
			}

			if isVdppOnStretchedSVEnabled {
				selectedDatastoreURL, err = getDatastoreURLFromStoragePool(ctx, storagePool)
				if err != nil {
					return nil, csifault.CSIInternalFault, logger.LogNewErrorCodef(log, codes.Internal,
						"error in specified StoragePool %s. Error: %+v", storagePool, err)
				}
				log.Infof("Will select datastore %s as per the provided storage pool %s", selectedDatastoreURL, storagePool)
			}

			hostMoid, err := getHostMOIDFromK8sCloudOperatorService(ctx, accessibleNodes[0])
			if err != nil {
				return nil, csifault.CSIInternalFault, logger.LogNewErrorCodef(log, codes.Internal,
					"failed to get ESX Host Moid from API server. Error: %+v", err)
			}

			affineToHost = hostMoid
			log.Debugf("Setting the affineToHost value as %s", affineToHost)
		}

		if isVdppOnStretchedSVEnabled {
			datastore, err := cnsvsphere.GetDatastoreInfoByURL(ctx, vc, clusterComputeResourceMoIds, selectedDatastoreURL)
			if err != nil {
				return nil, csifault.CSIInternalFault, logger.LogNewErrorCodef(log, codes.Internal,
					"failed to find the datastore from the selected datastore URL %s. Error: %v", selectedDatastoreURL, err)
			}

			candidateDatastores = []*cnsvsphere.DatastoreInfo{datastore}
		}
	}

	// Volume Size - Default is 10 GiB.
	volSizeBytes := int64(common.DefaultGbDiskSize * common.GbInBytes)
	if req.GetCapacityRange() != nil && req.GetCapacityRange().RequiredBytes != 0 {
		volSizeBytes = int64(req.GetCapacityRange().GetRequiredBytes())
	}
	volSizeMB := int64(common.RoundUpSize(volSizeBytes, common.MbInBytes))
	isBlockVolumeSnapshotEnabled := commonco.ContainerOrchestratorUtility.IsFSSEnabled(ctx, common.BlockVolumeSnapshot)
	// Check if requested volume size and source snapshot size matches
	volumeSource := req.GetVolumeContentSource()
	var contentSourceSnapshotID string
	if isBlockVolumeSnapshotEnabled && volumeSource != nil {
		sourceSnapshot := volumeSource.GetSnapshot()
		if sourceSnapshot == nil {
			return nil, csifault.CSIInvalidArgumentFault,
				logger.LogNewErrorCode(log, codes.InvalidArgument, "unsupported VolumeContentSource type")
		}
		contentSourceSnapshotID = sourceSnapshot.GetSnapshotId()
		// Retrieving the original source CNS volume-id from the snapshot-id
		cnsVolumeID, _, err := common.ParseCSISnapshotID(contentSourceSnapshotID)
		if err != nil {
			return nil, csifault.CSIInvalidArgumentFault,
				logger.LogNewErrorCode(log, codes.InvalidArgument, err.Error())
		}
		// The requested volume size when creating a volume from snapshot should be the same as the
		// snapshot size, since CNS does not support querying the exact snapshot size, we approximate
		// it to the original source volume size, the check ensures that sufficient space is allocated
		// for the restore.
		// Query capacity in MB for block volume snapshot
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
				"requested volume size: %d must be the same as source snapshot size: %d",
				volSizeBytes, snapshotSizeInBytes)
		}
	}

	var cryptoKeyID *common.CryptoKeyID
	isByokEnabled := commonco.ContainerOrchestratorUtility.IsFSSEnabled(ctx, common.WCP_VMService_BYOK)
	if isByokEnabled {
		if encClass, err := c.manager.CryptoClient.GetEncryptionClassForPVC(
			ctx,
			pvcName,
			pvcNamespace); err != nil {

			return nil, csifault.CSIInternalFault, logger.LogNewErrorCodef(log, codes.Internal,
				"failed to get encryption class for PVC. Error: %+v", err)
		} else if encClass != nil {
			cryptoKeyID = &common.CryptoKeyID{
				KeyID:       encClass.Spec.KeyID,
				KeyProvider: encClass.Spec.KeyProvider,
			}
		}
	}

	// Create CreateVolumeSpec and populate values.
	var createVolumeSpec = common.CreateVolumeSpec{
		CapacityMB:              volSizeMB,
		Name:                    req.Name,
		StoragePolicyID:         storagePolicyID,
		ScParams:                &common.StorageClassParams{},
		AffineToHost:            affineToHost,
		VolumeType:              common.BlockVolumeType,
		VsanDatastoreURL:        selectedDatastoreURL,
		ContentSourceSnapshotID: contentSourceSnapshotID,
		CryptoKeyID:             cryptoKeyID,
	}

	createVolumeOpts := common.CreateBlockVolumeOptions{
		FilterSuspendedDatastores:      filterSuspendedDatastores,
		UseSupervisorId:                isTKGSHAEnabled,
		IsVdppOnStretchedSvFssEnabled:  isVdppOnStretchedSVEnabled,
		IsByokEnabled:                  isByokEnabled,
		IsCSITransactionSupportEnabled: isCSITransactionSupportEnabled,
	}

	var (
		volumeInfo *cnsvolume.CnsVolumeInfo
		faultType  string
	)
	if isPodVMOnStretchSupervisorFSSEnabled {
		volumeInfo, faultType, err = common.CreateBlockVolumeUtil(ctx, cnstypes.CnsClusterFlavorWorkload,
			c.manager, &createVolumeSpec, candidateDatastores, createVolumeOpts,
			&cnsvolume.CreateVolumeExtraParams{
				VolSizeBytes:                         volSizeBytes,
				StorageClassName:                     req.Parameters[common.AttributeStorageClassName],
				Namespace:                            req.Parameters[common.AttributePvcNamespace],
				IsPodVMOnStretchSupervisorFSSEnabled: isPodVMOnStretchSupervisorFSSEnabled,
			})
		if err != nil {
			if cnsvolume.IsNotSupportedFaultType(ctx, faultType) {
				log.Warnf("NotSupported fault is detected: retrying CreateVolume without VolumeID in spec.")
				// Disable CSI transaction support for retry
				createVolumeOpts.IsCSITransactionSupportEnabled = false
				volumeInfo, faultType, err = common.CreateBlockVolumeUtil(ctx, cnstypes.CnsClusterFlavorWorkload,
					c.manager, &createVolumeSpec, candidateDatastores, createVolumeOpts,
					&cnsvolume.CreateVolumeExtraParams{
						VolSizeBytes:                         volSizeBytes,
						StorageClassName:                     req.Parameters[common.AttributeStorageClassName],
						Namespace:                            req.Parameters[common.AttributePvcNamespace],
						IsPodVMOnStretchSupervisorFSSEnabled: isPodVMOnStretchSupervisorFSSEnabled,
					})
			}
		}
	} else {
		volumeInfo, faultType, err = common.CreateBlockVolumeUtil(ctx, cnstypes.CnsClusterFlavorWorkload,
			c.manager, &createVolumeSpec, candidateDatastores, createVolumeOpts, nil)
	}
	if err != nil {
		return nil, faultType, logger.LogNewErrorCodef(log, codes.Internal,
			"failed to create volume. Error: %+v", err)
	}

	// CreateVolume response.
	attributes := make(map[string]string)
	attributes[common.AttributeDiskType] = common.DiskTypeBlockVolume
	resp := &csi.CreateVolumeResponse{
		Volume: &csi.Volume{
			VolumeId:      volumeInfo.VolumeID.Id,
			CapacityBytes: int64(units.FileSize(volSizeMB * common.MbInBytes)),
			VolumeContext: attributes,
		},
	}

	// Calculate accessible topology for the provisioned volume in case of topology aware environment.
	if isTKGSHAEnabled {
		if hostnameLabelPresent {
			// Configure the volumeTopology in the response so that the external
			// provisioner will properly sets up the nodeAffinity for this volume.
			if isVdppOnStretchedSVEnabled {
				resp.Volume.AccessibleTopology = topologyRequirement.GetPreferred()
			} else {
				for _, hostName := range accessibleNodes {
					volumeTopology := &csi.Topology{
						Segments: map[string]string{
							v1.LabelHostname: hostName,
						},
					}
					resp.Volume.AccessibleTopology = append(resp.Volume.AccessibleTopology, volumeTopology)
				}
			}
		} else if zoneLabelPresent {
			selectedDatastore := volumeInfo.DatastoreURL
			// CreateBlockVolumeUtil with idempotency enabled does not return datastore
			// information when it uses the cached information from CR. In such cases,
			// querying the volume to retrieve the datastore URL.
			if selectedDatastore == "" {
				queryFilter := cnstypes.CnsQueryFilter{
					VolumeIds: []cnstypes.CnsVolumeId{
						{
							Id: volumeInfo.VolumeID.Id,
						},
					},
				}
				querySelection := cnstypes.CnsQuerySelection{
					Names: []string{string(cnstypes.QuerySelectionNameTypeDataStoreUrl)},
				}
				queryResult, err := utils.QueryVolumeUtil(ctx, c.manager.VolumeManager, queryFilter, &querySelection,
					true)
				if err != nil || queryResult == nil || len(queryResult.Volumes) != 1 {
					return nil, csifault.CSIInternalFault, logger.LogNewErrorCodef(log, codes.Internal,
						"failed to find the datastore on which volume %q is provisioned. Error: %+v",
						volumeInfo.VolumeID.Id, err)
				}
				selectedDatastore = queryResult.Volumes[0].DatastoreUrl
			}

			// Calculate accessible topology for the provisioned volume.
			datastoreAccessibleTopology, err := c.topologyMgr.GetTopologyInfoFromNodes(ctx,
				commoncotypes.WCPRetrieveTopologyInfoParams{
					DatastoreURL:           selectedDatastore,
					StorageTopologyType:    storageTopologyType,
					TopologyRequirement:    topologyRequirement,
					Vc:                     vc,
					TopoSegToDatastoresMap: topoSegToDatastoresMap})
			if err != nil {
				// If the error is of InvalidTopologyProvisioningError type, it means we cannot
				// recover from this error with a retry, so cleanup the volume created above.
				if _, ok := err.(*common.InvalidTopologyProvisioningError); ok {
					log.Errorf("Encountered error after creating volume. Cleaning up...")
					// Delete the CnsVolumeOperationRequest created for CreateVolume call above.
					deleteOpReqError := operationStore.DeleteRequestDetails(ctx, req.Name)
					if deleteOpReqError != nil {
						log.Warnf("failed to cleanup CnsVolumeOperationRequest instance before erroring "+
							"out. Error received: %+v", deleteOpReqError)
					} else {
						// As the CnsVolumeOperationRequest for this CreateVolume call is deleted
						// successfully, we can go ahead and delete the volume created above.
						_, deleteVolumeError := common.DeleteVolumeUtil(ctx, c.manager.VolumeManager,
							volumeInfo.VolumeID.Id, true)
						if deleteVolumeError != nil {
							// This is a best effort deletion. We do not propagate the delete volume error to K8s.
							// NOTE: This might leave behind an orphan volume.
							log.Warnf("failed to delete volume: %q while cleaning up after CreateVolume failure. "+
								"Error: %+v", volumeInfo.VolumeID.Id, deleteVolumeError)
						}
					}
					return nil, csifault.CSIInternalFault, logger.LogNewErrorCodef(log, codes.Internal,
						"encountered an error while fetching accessible topologies for volume %q. Error: %+v",
						volumeInfo.VolumeID.Id, err)
				}
				// If error is not of InvalidTopologyProvisioningError type, do not delete volume created as idempotency
				// feature will ensure we retry with the same volume.
				return nil, csifault.CSIInternalFault, logger.LogNewErrorCodef(log, codes.Internal,
					"failed to find accessible topologies for volume %q. Error: %+v",
					volumeInfo.VolumeID.Id, err)
			}

			// Add topology segments to the CreateVolumeResponse.
			for _, topoSegments := range datastoreAccessibleTopology {
				volumeTopology := &csi.Topology{
					Segments: topoSegments,
				}
				resp.Volume.AccessibleTopology = append(resp.Volume.AccessibleTopology, volumeTopology)
			}
		}
	} else {
		// Configure the volumeTopology in the response so that the external
		// provisioner will properly set up the nodeAffinity for this volume.
		if isValidAccessibilityRequirement(topologyRequirement) {
			for _, hostName := range accessibleNodes {
				volumeTopology := &csi.Topology{
					Segments: map[string]string{
						v1.LabelHostname: hostName,
					},
				}
				resp.Volume.AccessibleTopology = append(resp.Volume.AccessibleTopology, volumeTopology)
			}
		}
	}
	log.Debugf("Volume Accessible Topology: %+v", resp.Volume.AccessibleTopology)
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
	if isPodVMOnStretchSupervisorFSSEnabled {
		if pvcNamespace, ok := req.Parameters[common.AttributePvcNamespace]; ok {
			if scName, ok := req.Parameters[common.AttributeStorageClassName]; ok {
				// Create CNSVolumeInfo CR for the volume ID.
				capacity := resource.NewQuantity(volSizeBytes, resource.BinarySI)
				err = volumeInfoService.CreateVolumeInfoWithPolicyInfo(ctx, volumeInfo.VolumeID.Id, pvcNamespace,
					storagePolicyID, scName, vc.Config.Host, capacity)
				if err != nil {
					return nil, csifault.CSIInternalFault, logger.LogNewErrorCodef(log, codes.Internal,
						"failed to store volumeID %q pvcNamespace %q StoragePolicyID %q StorageClassName %q "+
							"and vCenter %q in CNSVolumeInfo CR. Error: %+v",
						volumeInfo.VolumeID.Id, pvcNamespace, storagePolicyID, scName, vc.Config.Host, err)
				}
			} else {
				return nil, csifault.CSIInternalFault, logger.LogNewErrorCodef(log, codes.Internal,
					"failed to create CnsVolumeInfo CR for volumeID %q due to missing storage class name "+
						"in the CreateVolume request parameters", volumeInfo.VolumeID.Id)
			}
		} else {
			return nil, csifault.CSIInternalFault, logger.LogNewErrorCodef(log, codes.Internal,
				"failed to create CnsVolumeInfo CR for volumeID %q due to missing pvc namespace "+
					"in the CreateVolume request parameters", volumeInfo.VolumeID.Id)
		}
	}
	return resp, "", nil
}

// createFileVolume creates a file volume based on the CreateVolumeRequest.
func (c *controller) createFileVolume(ctx context.Context, req *csi.CreateVolumeRequest,
	isWorkloadDomainIsolationEnabled bool) (
	*csi.CreateVolumeResponse, string, error) {
	log := logger.GetLogger(ctx)
	var (
		storagePolicyID      string
		storageTopologyType  string
		topologyRequirement  *csi.TopologyRequirement
		candidateDatastores  []*cnsvsphere.DatastoreInfo
		hostnameLabelPresent bool
		zoneLabelPresent     bool
		err                  error
		volumeInfo           *cnsvolume.CnsVolumeInfo
		faultType            string
	)
	topologyRequirement = req.AccessibilityRequirements
	// Volume Size - Default is 10 GiB.
	volSizeBytes := int64(common.DefaultGbDiskSize * common.GbInBytes)
	if req.GetCapacityRange() != nil && req.GetCapacityRange().RequiredBytes != 0 {
		volSizeBytes = int64(req.GetCapacityRange().GetRequiredBytes())
	}
	volSizeMB := int64(common.RoundUpSize(volSizeBytes, common.MbInBytes))

	for paramName := range req.Parameters {
		param := strings.ToLower(paramName)
		if param == common.AttributeStoragePolicyID {
			storagePolicyID = req.Parameters[paramName]
		}
	}

	var createVolumeSpec = common.CreateVolumeSpec{
		CapacityMB:      volSizeMB,
		Name:            req.Name,
		StoragePolicyID: storagePolicyID,
		ScParams:        &common.StorageClassParams{},
		VolumeType:      common.FileVolumeType,
	}

	filterSuspendedDatastores := commonco.ContainerOrchestratorUtility.IsFSSEnabled(ctx, common.CnsMgrSuspendCreateVolume)
	isTKGSHAEnabled := commonco.ContainerOrchestratorUtility.IsFSSEnabled(ctx, common.TKGsHA)
	topoSegToDatastoresMap := make(map[string][]*cnsvsphere.DatastoreInfo)

	vc, err := c.manager.VcenterManager.GetVirtualCenter(ctx, c.manager.VcenterConfig.Host)
	if err != nil {
		return nil, faultType, logger.LogNewErrorCodef(log, codes.Internal,
			"failed to get vCenter. Error: %+v", err)
	}

	// If FSS Workload_Domain_Isolation_Supported is enabled, find the shared datastores associated with
	// topology requirements provided in the request if any and pass those to CNS for further processing.
	if isWorkloadDomainIsolationEnabled {
		// Check if topology requirements are specified in the request and accordingly filter the vSAN datastores
		// to be sent to CNS for volume provisioning.
		hostnameLabelPresent, zoneLabelPresent = checkTopologyKeysFromAccessibilityReqs(req.GetAccessibilityRequirements())
		if zoneLabelPresent && hostnameLabelPresent {
			// zone and host labels are present in the topologyRequirement meaning
			// it's host local volume provisioning on a stretched/multi-zone supervisor cluster.
			// Fail the request since we do not support this configuration.

			return nil, csifault.CSIUnimplementedFault, logger.LogNewErrorCodef(log, codes.Unimplemented,
				"support for topology requirement with both zone and hostname labels is not yet implemented.")
		} else if zoneLabelPresent {
			// zone labels present in the topologyRequirement meaning
			// it's non-host/zonal volume provisioning on a stretched/multi-zone supervisor cluster.
			// Continue volume provisioning with candidate vSAN datastores accessible to provided topology

			// topologyMgr can be nil if the AZ CR was not registered
			// at the time of controller init. Handling that case in CreateVolume calls.
			if c.topologyMgr == nil {
				return nil, csifault.CSIInternalFault, logger.LogNewErrorCode(log, codes.Internal,
					"topology manager not initialized.")
			}
			// Initiate TKGs HA workflow when the topology requirement contains zone labels only.
			log.Infof("Topology aware environment detected with requirement: %+v", topologyRequirement)
			sharedDatastores, err := c.topologyMgr.GetSharedDatastoresInTopology(ctx,
				commoncotypes.WCPTopologyFetchDSParams{
					TopologyRequirement:    topologyRequirement,
					Vc:                     vc,
					TopoSegToDatastoresMap: topoSegToDatastoresMap})
			if err != nil {
				return nil, csifault.CSIInternalFault, logger.LogNewErrorCodef(log, codes.Internal,
					"failed to find shared datastores for given topology requirement. Error: %v", err)
			}
			// Fetch all vSAN datastores in vCenter
			datacenters, err := vc.ListDatacenters(ctx)
			if err != nil {
				log.Errorf("failed to find datacenters from vCenter: %q, Error: %+v", vc.Config.Host, err)
				return nil, csifault.CSIInternalFault, logger.LogNewErrorCode(log, codes.Internal,
					"failed to find datacenters from vCenter")
			}
			// Get all vSAN datastores from VC.
			vsanDsURLToInfoMap, err := vc.GetVsanDatastores(ctx, datacenters)
			if err != nil {
				log.Errorf("failed to get vSAN datastores for vCenter %q, error %+v", vc.Config.Host, err)
				return nil, csifault.CSIInternalFault, logger.LogNewErrorCode(log, codes.Internal,
					"failed to get vSAN datacenters from vCenter")
			}
			// Return empty map if no vSAN datastores are found.
			if len(vsanDsURLToInfoMap) == 0 {
				log.Infof("No vSAN datastores found for vCenter %q", vc.Config.Host)
				return nil, csifault.CSIInternalFault, logger.LogNewErrorCode(log, codes.Internal,
					"no vSAN datastores found to create file volume")
			}
			// Filter vSAN datastores from shared datastores for given topology requirements
			for _, sharedDSInfo := range sharedDatastores {
				for _, vSANDSInfo := range vsanDsURLToInfoMap {
					if sharedDSInfo.Info.Url == vSANDSInfo.Info.Url {
						log.Debugf("Adding datastore %q to filtered datastores", vSANDSInfo.Info.Url)
						candidateDatastores = append(candidateDatastores, vSANDSInfo)
					}
				}
			}
		} else if hostnameLabelPresent {
			// host label present but zone labels not present in the topologyRequirement meaning
			// it's host local volume provisioning on a non-stretched/single-zone supervisor cluster.
			// Fail the request since we do not support this configuration.

			return nil, csifault.CSIInternalFault, logger.LogNewErrorCodef(log, codes.Unimplemented,
				"support for topology requirement with hostname labels is not yet implemented ")
		} else {
			// no label present in the topologyRequirement meaning
			// it's non-host local volume provisioning on a non-stretched/single-zone supervisor cluster.
			// Fail the request since we expect topology requirements to be provided always when
			// FSS Workload_Domain_Isolation_Supported is enabled.

			return nil, csifault.CSIInternalFault, logger.LogNewErrorCodef(log, codes.FailedPrecondition,
				"volume provisioning request received without topologyRequirement.")
		}
	} else {
		// Workload domain isolation feature is disabled, continue volume provisioning with original logic

		// Ignore TopologyRequirement for file volume provisioning.
		if req.GetAccessibilityRequirements() != nil {
			log.Info("Ignoring TopologyRequirement for file volume")
		}

		fsEnabledClusterToDsMap := c.authMgr.GetFsEnabledClusterToDsMap(ctx)
		// targetvSANFileShareClusters is set in CSI secret when file volume feature
		// is enabled on WCP. So we get datastores with privileges to create file
		// volumes for each specified vSAN cluster, and use those datastores to
		// create file volumes.
		for _, targetvSANcluster := range c.manager.VcenterConfig.TargetvSANFileShareClusters {
			if datastores, ok := fsEnabledClusterToDsMap[targetvSANcluster]; ok {
				for _, dsInfo := range datastores {
					log.Debugf("Adding datastore %q to filtered datastores", dsInfo.Info.Url)
					candidateDatastores = append(candidateDatastores, dsInfo)
				}
			}
		}
		if len(candidateDatastores) == 0 {
			// when len(sharedDatastores)==0, it means vsan file service is not enabled on any vsan cluster specfified
			// by VcenterConfig.TargetvSANFileShareClusters
			return nil, csifault.CSIVSanFileServiceDisabledFault, logger.LogNewErrorCode(log, codes.FailedPrecondition,
				"no datastores found to create file volume, vsan file service may be disabled")
		}
	}

	if isPodVMOnStretchSupervisorFSSEnabled {
		volumeInfo, faultType, err = common.CreateFileVolumeUtil(ctx, cnstypes.CnsClusterFlavorWorkload, vc,
			c.manager.VolumeManager, c.manager.CnsConfig, &createVolumeSpec, candidateDatastores,
			filterSuspendedDatastores, isTKGSHAEnabled, &cnsvolume.CreateVolumeExtraParams{
				VolSizeBytes:                         volSizeBytes,
				StorageClassName:                     req.Parameters[common.AttributeStorageClassName],
				Namespace:                            req.Parameters[common.AttributePvcNamespace],
				IsPodVMOnStretchSupervisorFSSEnabled: isPodVMOnStretchSupervisorFSSEnabled,
			})
	} else {
		volumeInfo, faultType, err = common.CreateFileVolumeUtil(ctx, cnstypes.CnsClusterFlavorWorkload, vc,
			c.manager.VolumeManager, c.manager.CnsConfig, &createVolumeSpec, candidateDatastores,
			filterSuspendedDatastores, isTKGSHAEnabled, nil)
	}
	if err != nil {
		return nil, faultType, logger.LogNewErrorCodef(log, codes.Internal,
			"failed to create volume. Error: %+v", err)
	}

	if volumeInfo == nil {
		return nil, faultType, logger.LogNewErrorCodef(log, codes.Internal,
			"nil response for volumeInfo")
	}

	attributes := make(map[string]string)
	attributes[common.AttributeDiskType] = common.DiskTypeFileVolume

	resp := &csi.CreateVolumeResponse{
		Volume: &csi.Volume{
			VolumeId:      volumeInfo.VolumeID.Id,
			CapacityBytes: int64(units.FileSize(volSizeMB * common.MbInBytes)),
			VolumeContext: attributes,
		},
	}

	// Calculate accessible topology for the provisioned volume in case of topology aware environment.
	if isWorkloadDomainIsolationEnabled {
		if zoneLabelPresent {
			// Note: with Workload domain isolation feature enabled, volumeInfo will always
			// 			return URL of the datastore that volume is allocated on.
			selectedDatastore := volumeInfo.DatastoreURL
			// Calculate accessible topology for the provisioned volume.
			datastoreAccessibleTopology, err := c.topologyMgr.GetTopologyInfoFromNodes(ctx,
				commoncotypes.WCPRetrieveTopologyInfoParams{
					DatastoreURL:           selectedDatastore,
					StorageTopologyType:    storageTopologyType,
					TopologyRequirement:    topologyRequirement,
					Vc:                     vc,
					TopoSegToDatastoresMap: topoSegToDatastoresMap})
			if err != nil {
				// If the error is of InvalidTopologyProvisioningError type, it means we cannot
				// recover from this error with a retry, so cleanup the volume created above.
				if _, ok := err.(*common.InvalidTopologyProvisioningError); ok {
					log.Errorf("Encountered error after creating volume. Cleaning up...")
					// Delete the CnsVolumeOperationRequest created for CreateVolume call above.
					deleteOpReqError := operationStore.DeleteRequestDetails(ctx, req.Name)
					if deleteOpReqError != nil {
						log.Warnf("failed to cleanup CnsVolumeOperationRequest instance before erroring "+
							"out. Error received: %+v", deleteOpReqError)
					} else {
						// As the CnsVolumeOperationRequest for this CreateVolume call is deleted
						// successfully, we can go ahead and delete the volume created above.
						_, deleteVolumeError := common.DeleteVolumeUtil(ctx, c.manager.VolumeManager,
							volumeInfo.VolumeID.Id, true)
						if deleteVolumeError != nil {
							// This is a best effort deletion. We do not propagate the delete volume error to K8s.
							// NOTE: This might leave behind an orphan volume.
							log.Warnf("failed to delete volume: %q while cleaning up after CreateVolume failure. "+
								"Error: %+v", volumeInfo.VolumeID.Id, deleteVolumeError)
						}
					}
					return nil, csifault.CSIInternalFault, logger.LogNewErrorCodef(log, codes.Internal,
						"encountered an error while fetching accessible topologies for volume %q. Error: %+v",
						volumeInfo.VolumeID.Id, err)
				}
				// If error is not of InvalidTopologyProvisioningError type, do not delete volume created as idempotency
				// feature will ensure we retry with the same volume.
				return nil, csifault.CSIInternalFault, logger.LogNewErrorCodef(log, codes.Internal,
					"failed to find accessible topologies for volume %q. Error: %+v",
					volumeInfo.VolumeID.Id, err)
			}

			// Add topology segments to the CreateVolumeResponse.
			for _, topoSegments := range datastoreAccessibleTopology {
				volumeTopology := &csi.Topology{
					Segments: topoSegments,
				}
				resp.Volume.AccessibleTopology = append(resp.Volume.AccessibleTopology, volumeTopology)
			}
		}
	}

	return resp, "", nil
}

// CreateVolume is creating CNS Volume using volume request specified
// in CreateVolumeRequest.
func (c *controller) CreateVolume(ctx context.Context, req *csi.CreateVolumeRequest) (
	*csi.CreateVolumeResponse, error) {

	start := time.Now()
	ctx = logger.NewContextWithLogger(ctx)
	log := logger.GetLogger(ctx)

	volumeType := prometheus.PrometheusUnknownVolumeType
	createVolumeInternal := func() (
		*csi.CreateVolumeResponse, string, error) {
		log.Infof("CreateVolume: called with args %+v", *req)
		isWorkloadDomainIsolationEnabled := commonco.ContainerOrchestratorUtility.
			IsFSSEnabled(ctx, common.WorkloadDomainIsolation)
		// TODO: If the err is returned by invoking CNS API, then faultType should be
		// populated by the underlying layer.
		// If the request failed due to validate the request, "csi.fault.InvalidArgument" will be return.
		// If thr reqeust failed due to object not found, "csi.fault.NotFound" will be return.
		// For all other cases, the faultType will be set to "csi.fault.Internal" for now.
		// Later we may need to define different csi faults.

		isBlockRequest := !isFileVolumeRequestInWcp(ctx, req.GetVolumeCapabilities())
		if isBlockRequest {
			volumeType = prometheus.PrometheusBlockVolumeType
		} else {
			volumeType = prometheus.PrometheusFileVolumeType
		}
		// Validate create request.
		err := validateWCPCreateVolumeRequest(ctx, req, isBlockRequest)
		if err != nil {
			msg := fmt.Sprintf("Validation for CreateVolume Request: %+v has failed. Error: %+v", *req, err)
			log.Error(msg)
			return nil, csifault.CSIInvalidArgumentFault, err
		}

		if !isBlockRequest {
			if !commonco.ContainerOrchestratorUtility.IsFSSEnabled(ctx, common.FileVolume) {
				return nil, csifault.CSIUnimplementedFault, logger.LogNewErrorCode(log, codes.Unimplemented,
					"file volume feature is disabled on the cluster")
			}
			// Block file volume provisioning if FSS Workload_Domain_Isolation_Supported is enabled but
			// 'fileVolumeActivated' field is set to false in vSphere config secret.
			if isWorkloadDomainIsolationEnabled &&
				!c.manager.VcenterConfig.FileVolumeActivated {
				return nil, csifault.CSIUnimplementedFault, logger.LogNewErrorCode(log, codes.Unimplemented,
					"file services are disabled on supervisor cluster")
			}
			// Block file volume provisioning on stretched supervisor cluster unless
			// FSS Workload_Domain_Isolation_Supported is enabled, where we allow file volume provisioning
			// with multiple vSphere clusters.
			if commonco.ContainerOrchestratorUtility.IsFSSEnabled(ctx, common.TKGsHA) {
				if len(clusterComputeResourceMoIds) > 1 &&
					!isWorkloadDomainIsolationEnabled {
					return nil, csifault.CSIUnimplementedFault, logger.LogNewErrorCode(log, codes.Unimplemented,
						"file volume provisioning is not supported on a stretched supervisor cluster")
				}
			}
			return c.createFileVolume(ctx, req, isWorkloadDomainIsolationEnabled)
		}
		return c.createBlockVolume(ctx, req, isWorkloadDomainIsolationEnabled)
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

	deleteVolumeInternal := func() (*csi.DeleteVolumeResponse, string, error) {
		log.Infof("DeleteVolume: called with args: %+v", *req)
		// TODO: If the err is returned by invoking CNS API, then faultType should be
		// populated by the underlying layer.
		// For all other cases, the faultType will be set to "csi.fault.Internal" for now.
		// Later we may need to define different csi faults.
		err := validateWCPDeleteVolumeRequest(ctx, req)
		if err != nil {
			msg := fmt.Sprintf("Validation for DeleteVolume Request: %+v has failed. Error: %+v", *req, err)
			log.Error(msg)
			return nil, csifault.CSIInvalidArgumentFault, err
		}
		if cnsVolumeType == common.UnknownVolumeType {
			cnsVolumeType, err = common.GetCnsVolumeType(ctx, c.manager.VolumeManager, req.VolumeId)
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
			snapshots, _, err := common.QueryVolumeSnapshotsByVolumeID(ctx, c.manager.VolumeManager, req.VolumeId,
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
		faultType, err := common.DeleteVolumeUtil(ctx, c.manager.VolumeManager, req.VolumeId, true)
		if err != nil {
			log.Debugf("DeleteVolumeUtil returns fault %s:", faultType)
			return nil, faultType, logger.LogNewErrorCodef(log, codes.Internal,
				"failed to delete volume: %q. Error: %+v", req.VolumeId, err)
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

func convertCnsVolumeType(ctx context.Context, cnsVolumeType string) string {
	volumeType := prometheus.PrometheusUnknownVolumeType
	if cnsVolumeType == common.BlockVolumeType {
		volumeType = prometheus.PrometheusBlockVolumeType
	} else if cnsVolumeType == common.FileVolumeType {
		volumeType = prometheus.PrometheusFileVolumeType
	}
	return volumeType
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
		err := validateWCPControllerPublishVolumeRequest(ctx, req)
		if err != nil {
			msg := fmt.Sprintf("Validation for PublishVolume Request: %+v has failed. Error: %v", *req, err)
			log.Errorf(msg)
			return nil, csifault.CSIInvalidArgumentFault, err
		}
		volumeType = prometheus.PrometheusBlockVolumeType
		volumeAttachment, err := commonco.ContainerOrchestratorUtility.GetVolumeAttachment(ctx, req.VolumeId, req.NodeId)
		if err != nil {
			log.Warnf("failed to retrieve volumeAttachment from API server Err: %v", err)
		} else {
			if volumeAttachment != nil && volumeAttachment.Status.Attached {
				return nil, csifault.CSIInternalFault, logger.LogNewErrorCodef(log, codes.Internal,
					"volumeAttachment %v already has the attached status as true. "+
						"Assuming the attach volume is due to incorrect force sync Attach from CSI Attacher",
					volumeAttachment)
			}
		}

		vmuuid, err := getVMUUIDFromK8sCloudOperatorService(ctx, req.VolumeId, req.NodeId)
		if err != nil {
			if e, ok := status.FromError(err); ok {
				switch e.Code() {
				case codes.NotFound:
					return nil, csifault.CSIVmUuidNotFoundFault, logger.LogNewErrorCodef(log, codes.Internal,
						"failed to find the pod vmuuid annotation from the k8sCloudOperator service "+
							"when processing attach for volumeID: %s on node: %s. Error: %+v",
						req.VolumeId, req.NodeId, err)
				}
			}
			return nil, csifault.CSIInternalFault, logger.LogNewErrorCodef(log, codes.Internal,
				"failed to get the pod vmuuid annotation from the k8sCloudOperator service "+
					"when processing attach for volumeID: %s on node: %s. Error: %+v",
				req.VolumeId, req.NodeId, err)
		}

		vcdcMap, err := getDatacenterFromConfig(c.manager.CnsConfig)
		if err != nil {
			return nil, csifault.CSIInternalFault, logger.LogNewErrorCodef(log, codes.Internal,
				"failed to get datacenter from config with error: %+v", err)
		}
		var vCenterHost, dcMorefValue string
		for key, value := range vcdcMap {
			vCenterHost = key
			dcMorefValue = value
		}
		vc, err := c.manager.VcenterManager.GetVirtualCenter(ctx, vCenterHost)
		if err != nil {
			return nil, csifault.CSIInternalFault, logger.LogNewErrorCodef(log, codes.Internal,
				"cannot get virtual center %s from virtualcentermanager while attaching disk with error %+v",
				vc.Config.Host, err)
		}

		// Connect to VC.
		err = vc.Connect(ctx)
		if err != nil {
			return nil, csifault.CSIInternalFault, logger.LogNewErrorCodef(log, codes.Internal,
				"failed to connect to Virtual Center: %s", vc.Config.Host)
		}

		podVM, err := getVMByInstanceUUIDInDatacenter(ctx, vc, dcMorefValue, vmuuid)
		if err != nil {
			return nil, csifault.CSIInternalFault, logger.LogNewErrorCodef(log, codes.Internal,
				"failed to get the PodVM Moref from the PodVM UUID: %s in datacenter: %s with err: %+v",
				vmuuid, dcMorefValue, err)
		}

		// Attach the volume to the node.
		// faultType is returned from manager.AttachVolume.
		diskUUID, faultType, err := common.AttachVolumeUtil(ctx, c.manager.VolumeManager, podVM,
			req.VolumeId, true)
		if err != nil {
			if commonco.ContainerOrchestratorUtility.IsFSSEnabled(ctx, common.FakeAttach) {
				log.Infof("Volume attachment failed. Checking if it can be fake attached")
				var capabilities []*csi.VolumeCapability
				capabilities = append(capabilities, req.VolumeCapability)
				if !isFileVolumeRequestInWcp(ctx, capabilities) { // Block volume.
					allowed, err := commonco.ContainerOrchestratorUtility.IsFakeAttachAllowed(ctx,
						req.VolumeId, c.manager.VolumeManager)
					if err != nil {
						return nil, csifault.CSIInternalFault, logger.LogNewErrorCodef(log, codes.Internal,
							"failed to determine if volume: %s can be fake attached. Error: %+v", req.VolumeId, err)
					}

					if allowed {
						// Mark the volume as fake attached before returning response.
						err := commonco.ContainerOrchestratorUtility.MarkFakeAttached(ctx, req.VolumeId)
						if err != nil {
							return nil, csifault.CSIInternalFault, logger.LogNewErrorCodef(log, codes.Internal,
								"failed to mark volume: %s as fake attached. Error: %+v", req.VolumeId, err)
						}

						publishInfo := make(map[string]string)
						publishInfo[common.AttributeDiskType] = common.DiskTypeBlockVolume
						publishInfo[common.AttributeFakeAttached] = "true"

						resp := &csi.ControllerPublishVolumeResponse{
							PublishContext: publishInfo,
						}
						log.Infof("Volume %s has been fake attached", req.VolumeId)
						return resp, "", nil
					}
				}

				log.Infof("Volume %s is not eligible to be fake attached", req.VolumeId)
			}
			log.Debugf("AttachVolumeUtil returns fault %s:", faultType)
			return nil, faultType, logger.LogNewErrorCodef(log, codes.Internal,
				"failed to attach volume with volumeID: %s. Error: %+v", req.VolumeId, err)
		}

		publishInfo := make(map[string]string)
		publishInfo[common.AttributeDiskType] = common.DiskTypeBlockVolume
		publishInfo[common.AttributeFirstClassDiskUUID] = common.FormatDiskUUID(diskUUID)
		publishInfo[common.AttributeVmUUID] = vmuuid
		resp := &csi.ControllerPublishVolumeResponse{
			PublishContext: publishInfo,
		}

		return resp, "", nil
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
		log.Infof("Volume %q attached successfully.", req.VolumeId)
		prometheus.CsiControlOpsHistVec.WithLabelValues(volumeType, prometheus.PrometheusAttachVolumeOpType,
			prometheus.PrometheusPassStatus, faultType).Observe(time.Since(start).Seconds())
	}
	return resp, err
}

// ControllerUnpublishVolume detaches a volume from the Node VM.
// Volume id and node name is retrieved from ControllerUnpublishVolumeRequest.
func (c *controller) ControllerUnpublishVolume(ctx context.Context, req *csi.ControllerUnpublishVolumeRequest) (
	*csi.ControllerUnpublishVolumeResponse, error) {
	start := time.Now()
	ctx = logger.NewContextWithLogger(ctx)
	log := logger.GetLogger(ctx)
	volumeType := prometheus.PrometheusUnknownVolumeType
	controllerUnpublishVolumeInternal := func() (
		*csi.ControllerUnpublishVolumeResponse, string, error) {
		log.Infof("ControllerUnpublishVolume: called with args %+v", *req)
		// TODO: If the err is returned by invoking CNS API, then faultType should be
		// populated by the underlying layer.
		// If the request failed due to validate the request, "csi.fault.InvalidArgument" will be return.
		// If thr reqeust failed due to object not found, "csi.fault.NotFound" will be return.
		// For all other cases, the faultType will be set to "csi.fault.Internal" for now.
		// Later we may need to define different csi faults.
		err := validateWCPControllerUnpublishVolumeRequest(ctx, req)
		if err != nil {
			msg := fmt.Sprintf("Validation for UnpublishVolume Request: %+v has failed. Error: %v", *req, err)
			log.Error(msg)
			return nil, csifault.CSIInvalidArgumentFault, err
		}
		volumeType = prometheus.PrometheusBlockVolumeType

		csiDetachEnabled := commonco.ContainerOrchestratorUtility.IsFSSEnabled(ctx,
			common.CSIDetachOnSupervisor)

		if csiDetachEnabled {
			var isVADeleted bool
			var vmuuid string
			volumeAttachment, err := commonco.ContainerOrchestratorUtility.GetVolumeAttachment(ctx, req.VolumeId, req.NodeId)
			if err != nil {
				if apierrors.IsNotFound(err) {
					log.Infof("VolumeAttachments object not found for volume %q & node %q. "+
						"Thus, assuming the volume is detached.", req.VolumeId, req.NodeId)
					isVADeleted = true
				} else {
					log.Errorf("failed to retrieve volumeAttachment from API server Err: %v", err)
					return nil, csifault.CSIInternalFault, err
				}
			}
			if !isVADeleted {
				log.Debugf("controllerUnpublishVolumeInternal: VA : %v", spew.Sdump(volumeAttachment))
				for k, v := range volumeAttachment.Status.AttachmentMetadata {
					if k == common.AttributeVmUUID {
						log.Debugf("controllerUnpublishVolumeInternal: vmuuid value: %q", v)
						vmuuid = v
						break
					}
				}
				vcdcMap, err := getDatacenterFromConfig(c.manager.CnsConfig)
				if err != nil {
					return nil, csifault.CSIInternalFault, logger.LogNewErrorCodef(log, codes.Internal,
						"failed to get datacenter from config with error: %+v", err)
				}
				var vCenterHost, dcMorefValue string
				for key, value := range vcdcMap {
					vCenterHost = key
					dcMorefValue = value
				}
				vc, err := c.manager.VcenterManager.GetVirtualCenter(ctx, vCenterHost)
				if err != nil {
					return nil, csifault.CSIInternalFault, logger.LogNewErrorCodef(log, codes.Internal,
						"cannot get virtual center %s from virtualcentermanager while attaching disk with error %+v",
						vc.Config.Host, err)
				}
				// Connect to VC.
				err = vc.Connect(ctx)
				if err != nil {
					return nil, csifault.CSIInternalFault, logger.LogNewErrorCodef(log, codes.Internal,
						"failed to connect to Virtual Center: %s", vc.Config.Host)
				}
				podVM, err := getVMByInstanceUUIDInDatacenter(ctx, vc, dcMorefValue, vmuuid)
				if err != nil {
					if err != cnsvsphere.ErrVMNotFound {
						return nil, csifault.CSIInternalFault, logger.LogNewErrorCodef(log, codes.Internal,
							"failed to get the PodVM Moref from the PodVM UUID: %s in datacenter: %s with err: %+v",
							vmuuid, dcMorefValue, err)
					}
					log.Infof("virtual machine not found for vmUUID %q. "+
						"Thus, assuming the volume is detached.", vmuuid)
				}

				if podVM != nil {
					faultType, err := common.DetachVolumeUtil(ctx, c.manager.VolumeManager, podVM, req.VolumeId)
					if err != nil {
						return nil, faultType, logger.LogNewErrorCodef(log, codes.Internal,
							"failed to detach disk: %+q from node: %q err %+v", req.VolumeId, req.NodeId, err)
					}
				}
			}

			if commonco.ContainerOrchestratorUtility.IsFSSEnabled(ctx, common.FakeAttach) {
				// Check if the volume was fake attached and unmark it as not fake
				// attached.
				if err := commonco.ContainerOrchestratorUtility.ClearFakeAttached(ctx, req.VolumeId); err != nil {
					msg := fmt.Sprintf("Failed to unmark volume as not fake attached. Error: %v", err)
					log.Error(msg)
					return nil, csifault.CSIInternalFault, err
				}
			}
			return &csi.ControllerUnpublishVolumeResponse{}, "", nil
		} else {
			var isVADeleted bool
			volumeAttachment, err := commonco.ContainerOrchestratorUtility.GetVolumeAttachment(ctx, req.VolumeId, req.NodeId)
			if err != nil {
				if apierrors.IsNotFound(err) {
					log.Infof("VolumeAttachments object not found for volume %q & node %q. "+
						"Thus, assuming the volume is detached.", req.VolumeId, req.NodeId)
					isVADeleted = true
				} else {
					log.Errorf("failed to retrieve volumeAttachment from API server Err: %v", err)
					return nil, csifault.CSIInternalFault, err
				}
			}
			if !isVADeleted {
				log.Debugf("controllerUnpublishVolumeInternal: VA : %v", spew.Sdump(volumeAttachment))
				for k, v := range volumeAttachment.Status.AttachmentMetadata {
					if k == common.AttributeVmUUID {
						log.Debugf("controllerUnpublishVolumeInternal: vmuuid value: %q", v)
						vcdcMap, err := getDatacenterFromConfig(c.manager.CnsConfig)
						if err != nil {
							return nil, csifault.CSIInternalFault, logger.LogNewErrorCodef(log, codes.Internal,
								"failed to get datacenter from config with error: %+v", err)
						}
						var vCenterHost, dcMorefValue string
						for key, value := range vcdcMap {
							vCenterHost = key
							dcMorefValue = value
						}
						vc, err := c.manager.VcenterManager.GetVirtualCenter(ctx, vCenterHost)
						if err != nil {
							return nil, csifault.CSIInternalFault, logger.LogNewErrorCodef(log, codes.Internal,
								"cannot get virtual center %s from virtualcentermanager while attaching disk with error %+v",
								vc.Config.Host, err)
						}
						// Connect to VC.
						err = vc.Connect(ctx)
						if err != nil {
							return nil, csifault.CSIInternalFault, logger.LogNewErrorCodef(log, codes.Internal,
								"failed to connect to Virtual Center: %s", vc.Config.Host)
						}
						isStillAttached := false
						timeout := 4 * time.Minute
						pollTime := time.Duration(5) * time.Second
						var podVM *cnsvsphere.VirtualMachine
						err = wait.PollUntilContextTimeout(ctx, pollTime, timeout, true,
							func(ctx context.Context) (bool, error) {
								podVM, err = getVMByInstanceUUIDInDatacenter(ctx, vc, dcMorefValue, v)
								if err != nil {
									if err == cnsvsphere.ErrVMNotFound {
										log.Infof("virtual machine not found for vmUUID %q. "+
											"Thus, assuming the volume is detached.", v)
										return true, err
									}
									if err == cnsvsphere.ErrInvalidVC {
										log.Errorf("failed to get the PodVM Moref from the PodVM UUID: %s in datacenter: "+
											"%s with err: %+v", v, dcMorefValue, err)
										return false, err
									}
									log.Errorf("failed to get the PodVM Moref from the PodVM UUID: %s in datacenter: "+
										"%s with err: %+v. Will Retry after 5 seconds", v, dcMorefValue, err)
									return false, nil
								}
								diskUUID, err := cnsvolume.IsDiskAttached(ctx, podVM, req.VolumeId, true)
								if err != nil {
									log.Infof("retrying the IsDiskAttached check again for volumeId %q. Err: %+v", req.VolumeId, err)
									return false, nil
								}
								if diskUUID != "" {
									log.Infof("diskUUID: %q is still attached to podVM %q with moId %q. Retrying in 5 seconds.",
										diskUUID, podVM.Reference().String(), podVM.Reference().Value)
									isStillAttached = true
									return false, nil
								}
								return true, nil
							})
						if err != nil {
							if err == cnsvsphere.ErrVMNotFound {
								// If VirtualMachine is not found, return success assuming volume is already detached
								break
							}
							if err == cnsvsphere.ErrInvalidVC {
								return nil, csifault.CSIInternalFault, fmt.Errorf(
									"failed to get the PodVM Moref from the PodVM UUID: %s in datacenter: "+
										"%s with err: %+v", v, dcMorefValue, err)
							}
							if isStillAttached {
								// Since the disk is still attached, we need to check if the volumeId in contention is attached
								// to the Pod on a different node. We can get the node name information for the volumeID
								// from GetNodesForVolumes method. If the nodeName is same, it signifies that the Pod is in the
								// process of getting deleted. If the nodeName is different, it signifies that the Pod got
								// rescheduled onto another node and hence we can break out of the isDiskAttached loop
								volumeId := []string{req.VolumeId}
								nodesForVolume := commonco.ContainerOrchestratorUtility.GetNodesForVolumes(ctx, volumeId)
								if len(nodesForVolume) == 0 {
									log.Errorf("error while fetching the node names for volumeId %q", req.VolumeId)
									return nil, csifault.CSIInternalFault, err
								}
								nodeNames := nodesForVolume[req.VolumeId]
								if len(nodeNames) == 1 && nodeNames[0] == req.NodeId {
									log.Errorf("volume %q is still attached to node %q and podVM %q", req.VolumeId,
										req.NodeId, podVM.Reference().String())
									podvmpowerstate, powerstateErr := podVM.PowerState(ctx)
									if powerstateErr != nil {
										log.Errorf("failed to check the power state of pod vm: %q, error: %v",
											podVM.Reference(), powerstateErr)
										return nil, csifault.CSIInternalFault, fmt.Errorf("volume %q is still attached to "+
											"node %q and podVM %q Error while checking power state of Pod VM. Error: %v",
											req.VolumeId, req.NodeId, podVM.Reference().String(), powerstateErr)
									}
									log.Infof("power state of pod vm: %q is %q", podVM.Reference(), podvmpowerstate)
									if podvmpowerstate == types.VirtualMachinePowerStatePoweredOff {
										log.Debugf("attempting to detach volume %q from "+
											"powered off Pod VM %q", req.VolumeId, podVM.Reference().String())
										detachFault, detachErr := c.manager.VolumeManager.DetachVolume(ctx, podVM, req.VolumeId)
										if detachErr == nil {
											log.Infof("successfully detached volume %q from Pod VM %q", req.VolumeId, podVM.Reference().String())
											return &csi.ControllerUnpublishVolumeResponse{}, "", nil
										} else {
											log.Errorf("failed to detach volume %q from Pod VM %q", req.VolumeId, podVM.Reference().String())
											return nil, detachFault, detachErr
										}
									}
									return nil, csifault.CSIDiskNotDetachedFault, err
								}
								log.Infof("Found another VolumeAttachment for volumeId %q. Assuming that the pod using the "+
									"volume is scheduled on different node. Returning success for ControllerUnPublishVolume for "+
									"volumeId: %q and VA: %q", req.VolumeId, req.VolumeId, volumeAttachment.Name)
								break
							}
							return nil, csifault.CSIInternalFault, err
						}
					}
				}
			}
			if commonco.ContainerOrchestratorUtility.IsFSSEnabled(ctx, common.FakeAttach) {
				// Check if the volume was fake attached and unmark it as not fake
				// attached.
				if err := commonco.ContainerOrchestratorUtility.ClearFakeAttached(ctx, req.VolumeId); err != nil {
					msg := fmt.Sprintf("Failed to unmark volume as not fake attached. Error: %v", err)
					log.Error(msg)
					return nil, csifault.CSIInternalFault, err
				}
			}
			return &csi.ControllerUnpublishVolumeResponse{}, "", nil
		}
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
		log.Infof("Volume %q detached successfully.", req.VolumeId)
		prometheus.CsiControlOpsHistVec.WithLabelValues(volumeType, prometheus.PrometheusDetachVolumeOpType,
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
	if err := isValidVolumeCapabilitiesInWcp(ctx, volCaps); err == nil {
		confirmed = &csi.ValidateVolumeCapabilitiesResponse_Confirmed{VolumeCapabilities: volCaps}
	}
	return &csi.ValidateVolumeCapabilitiesResponse{
		Confirmed: confirmed,
	}, nil
}

// ListVolumes returns the mapping of the volumes and corresponding published nodes.
func (c *controller) ListVolumes(ctx context.Context, req *csi.ListVolumesRequest) (
	*csi.ListVolumesResponse, error) {
	start := time.Now()
	volumeType := prometheus.PrometheusBlockVolumeType
	ctx = logger.NewContextWithLogger(ctx)
	log := logger.GetLogger(ctx)
	var err error
	// ListVolumes FSS is disabled so, return nil and an unimplemented error
	if !commonco.ContainerOrchestratorUtility.IsFSSEnabled(ctx, common.ListVolumes) {
		return nil, status.Error(codes.Unimplemented, "list volumes FSS disabled")
	}
	controllerListVolumeInternal := func() (*csi.ListVolumesResponse, string, error) {
		log.Debugf("ListVolumes called with args %+v, expectedStartingIndex %v", *req, expectedStartingIndex)
		k8sVolumeIDs := commonco.ContainerOrchestratorUtility.GetAllVolumes()

		startingIdx := 0
		if req.StartingToken != "" {
			startingIdx, err = strconv.Atoi(req.StartingToken)
			if err != nil {
				log.Errorf("Unable to convert startingToken from string to int err=%v", err)
				return nil, csifault.CSIInvalidArgumentFault, status.Error(codes.InvalidArgument,
					"startingToken not a valid integer")
			}
		}

		// If the startingIdx is zero or not equal to the expectedStartingIndex, then
		// it means the listVolume request is a new one and not part of a previous
		// request, so fetch the volumes from CNS again
		if startingIdx == 0 || startingIdx != expectedStartingIndex {
			querySelection := cnstypes.CnsQuerySelection{
				Names: []string{
					string(cnstypes.QuerySelectionNameTypeVolumeType),
				},
			}
			cnsQueryVolumes, err := utils.QueryAllVolumesForCluster(ctx, c.manager.VolumeManager,
				c.manager.CnsConfig.Global.SupervisorID, querySelection)
			if err != nil {
				log.Errorf("Error while querying volumes from CNS %v", err)
				return nil, csifault.CSIInternalFault, status.Error(codes.Internal, "Error while querying volumes from CNS")
			}

			cnsVolumeIDs = nil
			for _, cnsVolume := range cnsQueryVolumes.Volumes {
				cnsVolumeIDs = append(cnsVolumeIDs, cnsVolume.VolumeId.Id)
			}

			// Get volume ID to VMMap and vmMoidToHostMoid map
			vmMoidToHostMoid, volumeIDToVMMap, err = c.GetVolumeToHostMapping(ctx)
			if err != nil {
				log.Errorf("failed to get VM MoID to Host MoID map, err:%v", err)
				return nil, csifault.CSIInternalFault, status.Error(codes.Internal, "failed to get VM MoID to Host MoID map")
			}
		}

		// If the difference between the volumes reported by Kubernetes and CNS
		// is greater than the listVolumeThreshold, it might mean that the CNS
		// cache is stale. So, fail the request and return an error
		listVolumeThreshold := c.manager.VcenterConfig.ListVolumeThreshold
		if len(k8sVolumeIDs)-len(cnsVolumeIDs) > listVolumeThreshold {
			log.Errorf("Kubernetes and CNS volumes completely out of sync and exceeds the threshold: %d-%d=%d",
				len(k8sVolumeIDs), len(cnsVolumeIDs), listVolumeThreshold)
			return nil, csifault.CSIInternalFault, status.Error(codes.FailedPrecondition,
				"Kubernetes and CNS volumes completely out of sync")
		}

		queryLimit := c.manager.VcenterConfig.QueryLimit
		if req.MaxEntries != 0 {
			queryLimit = int(req.MaxEntries)
		}

		if queryLimit > len(cnsVolumeIDs) {
			queryLimit = len(cnsVolumeIDs)
		}

		endingIdx := queryLimit + startingIdx
		if endingIdx > len(cnsVolumeIDs) {
			endingIdx = len(cnsVolumeIDs)
		}
		log.Debugf("ListVolumes: cnsVolumeIDs %+v, startingIdx %v, queryLimit %v", cnsVolumeIDs, startingIdx, queryLimit)
		volumeIDs := make([]string, 0)
		for i := startingIdx; i < endingIdx; i++ {
			volumeIDs = append(volumeIDs, cnsVolumeIDs[i])
		}

		response, err := getVolumeIDToVMMap(ctx, volumeIDs, vmMoidToHostMoid, volumeIDToVMMap)
		if err != nil {
			log.Errorf("Error while generating ListVolume response, err:%v", err)
			return nil, csifault.CSIInternalFault, status.Error(codes.Internal, "Error while generating ListVolume response")
		}

		// Correctly set response nextToken value for the paginated response
		if len(cnsVolumeIDs) > endingIdx {
			expectedStartingIndex = endingIdx
			response.NextToken = strconv.Itoa(endingIdx)
		} else {
			// All entries have been returned, so reset expectedStartingIndex and the nextToken
			expectedStartingIndex = 0
			response.NextToken = ""
		}
		log.Debugf("ListVolumes: Response entries %+v", response)
		return response, "", nil
	}
	resp, faultType, err := controllerListVolumeInternal()
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
	return resp, err
}

func (c *controller) GetCapacity(ctx context.Context, req *csi.GetCapacityRequest) (
	*csi.GetCapacityResponse, error) {
	ctx = logger.NewContextWithLogger(ctx)
	log := logger.GetLogger(ctx)
	log.Infof("GetCapacity: called with args %+v", *req)
	return nil, status.Error(codes.Unimplemented, "")
}

func (c *controller) ControllerGetCapabilities(ctx context.Context, req *csi.ControllerGetCapabilitiesRequest) (
	*csi.ControllerGetCapabilitiesResponse, error) {

	ctx = logger.NewContextWithLogger(ctx)
	log := logger.GetLogger(ctx)
	log.Infof("ControllerGetCapabilities: called with args %+v", *req)
	var caps []*csi.ControllerServiceCapability
	if commonco.ContainerOrchestratorUtility.IsFSSEnabled(ctx, common.ListVolumes) {
		controllerCaps = append(controllerCaps, csi.ControllerServiceCapability_RPC_LIST_VOLUMES,
			csi.ControllerServiceCapability_RPC_LIST_VOLUMES_PUBLISHED_NODES)
	}

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
	log.Infof("WCP CreateSnapshot: called with args %+v", *req)
	isBlockVolumeSnapshotWCPEnabled := commonco.ContainerOrchestratorUtility.IsFSSEnabled(ctx, common.BlockVolumeSnapshot)
	if !isBlockVolumeSnapshotWCPEnabled {
		return nil, logger.LogNewErrorCode(log, codes.Unimplemented, "createSnapshot")
	}
	volumeType := prometheus.PrometheusUnknownVolumeType
	createSnapshotInternal := func() (*csi.CreateSnapshotResponse, error) {
		// Validate CreateSnapshotRequest
		if err := validateWCPCreateSnapshotRequest(ctx, req); err != nil {
			return nil, logger.LogNewErrorCodef(log, codes.Internal,
				"validation for CreateSnapshot Request: %+v has failed. Error: %v", *req, err)
		}
		volumeID := req.GetSourceVolumeId()

		// Check if the source volume is migrated vSphere volume
		if strings.Contains(volumeID, ".vmdk") {
			return nil, logger.LogNewErrorCodef(log, codes.Unimplemented,
				"cannot snapshot migrated vSphere volume. :%q", volumeID)
		}
		volumeType = prometheus.PrometheusBlockVolumeType
		// Query capacity in MB for block volume snapshot
		volumeIds := []cnstypes.CnsVolumeId{{Id: volumeID}}
		cnsVolumeDetailsMap, err := utils.QueryVolumeDetailsUtil(ctx, c.manager.VolumeManager, volumeIds)
		if err != nil {
			return nil, err
		}
		if _, ok := cnsVolumeDetailsMap[volumeID]; !ok {
			return nil, logger.LogNewErrorCodef(log, codes.Internal,
				"cns query volume did not return the volume: %s", volumeID)
		}
		snapshotSizeInMB := cnsVolumeDetailsMap[volumeID].SizeInMB

		if cnsVolumeDetailsMap[volumeID].VolumeType != common.BlockVolumeType {
			return nil, logger.LogNewErrorCodef(log, codes.FailedPrecondition,
				"queried volume doesn't have the expected volume type. Expected VolumeType: %v. "+
					"Queried VolumeType: %v", volumeType, cnsVolumeDetailsMap[volumeID].VolumeType)
		}

		// TODO: We may need to add logic to check the limit of max number of snapshots by using
		// GlobalMaxSnapshotsPerBlockVolume etc. variables in the future.

		// the returned snapshotID below is a combination of CNS VolumeID and CNS SnapshotID concatenated by the "+"
		// sign. That is, a string of "<UUID>+<UUID>". Because, all other CNS snapshot APIs still require both
		// VolumeID and SnapshotID as the input, while corresponding snapshot APIs in upstream CSI require SnapshotID.
		// So, we need to bridge the gap in vSphere CSI driver and return a combined SnapshotID to CSI Snapshotter.
		var snapshotID string
		var cnsSnapshotInfo *cnsvolume.CnsSnapshotInfo
		var cnsVolumeInfo *cnsvolumeinfov1alpha1.CNSVolumeInfo
		isStorageQuotaM2FSSEnabled := commonco.ContainerOrchestratorUtility.IsFSSEnabled(ctx,
			common.StorageQuotaM2)
		if isStorageQuotaM2FSSEnabled {
			cnsVolumeInfo, err = volumeInfoService.GetVolumeInfoForVolumeID(ctx, volumeID)
			if err != nil {
				return nil, logger.LogNewErrorCodef(log, codes.Internal,
					"failed to retrieve cnsVolumeInfo for volume: %s Error: %+v", volumeID, err)
			}
			snapshotID, cnsSnapshotInfo, err = common.CreateSnapshotUtil(ctx, c.manager.VolumeManager,
				volumeID, req.Name, &cnsvolume.CreateSnapshotExtraParams{
					StorageClassName:           cnsVolumeInfo.Spec.StorageClassName,
					StoragePolicyID:            cnsVolumeInfo.Spec.StoragePolicyID,
					Namespace:                  cnsVolumeInfo.Spec.Namespace,
					Capacity:                   cnsVolumeInfo.Spec.Capacity,
					IsStorageQuotaM2FSSEnabled: isStorageQuotaM2FSSEnabled,
				})
			if err != nil {
				return nil, logger.LogNewErrorCodef(log, codes.Internal,
					"failed to create snapshot on volume %q with error: %v", volumeID, err)
			}
			cnsVolumeInfo, err := volumeInfoService.GetVolumeInfoForVolumeID(ctx, cnsSnapshotInfo.SourceVolumeID)
			if err != nil {
				return nil, logger.LogNewErrorCodef(log, codes.Internal,
					"failed to retrieve cnsVolumeInfo for volume: %s Error: %+v", cnsVolumeInfo.Spec.VolumeID, err)
			}
			if cnsVolumeInfo.Spec.SnapshotLatestOperationCompleteTime.Time.Before(
				cnsSnapshotInfo.SnapshotLatestOperationCompleteTime) {
				patch, err := common.GetValidatedCNSVolumeInfoPatch(ctx, cnsSnapshotInfo)
				if err != nil {
					return nil, err
				}
				err = c.UpdateCNSVolumeInfo(ctx, patch, volumeID)
				if err != nil {
					return nil, err
				}
			} else {
				log.Infof("CNSVolumeInfo is already updated, skipping update for volume %q and snapshot %q",
					volumeID, snapshotID)
			}
			log.Infof("successfully updated aggregated snapshot capacity: %d for volume %q and snapshot %q",
				cnsSnapshotInfo.AggregatedSnapshotCapacityInMb, volumeID, snapshotID)
		} else {
			snapshotID, cnsSnapshotInfo, err = common.CreateSnapshotUtil(ctx, c.manager.VolumeManager,
				volumeID, req.Name, nil)
			if err != nil {
				return nil, logger.LogNewErrorCodef(log, codes.Internal,
					"failed to create snapshot on volume %q with error: %v", volumeID, err)
			}
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

		volumeSnapshotName := req.Parameters[common.VolumeSnapshotNameKey]
		volumeSnapshotNamespace := req.Parameters[common.VolumeSnapshotNamespaceKey]

		log.Infof("Attempting to annotate volumesnapshot %s/%s with annotation %s:%s",
			volumeSnapshotNamespace, volumeSnapshotName, common.VolumeSnapshotInfoKey, snapshotID)
		annotated, err := commonco.ContainerOrchestratorUtility.AnnotateVolumeSnapshot(ctx, volumeSnapshotName,
			volumeSnapshotNamespace, map[string]string{common.VolumeSnapshotInfoKey: snapshotID})
		if err != nil || !annotated {
			log.Warnf("The snapshot: %s was created successfully, but failed to annotate volumesnapshot %s/%s"+
				"with annotation %s:%s. Error: %v", snapshotID, volumeSnapshotNamespace,
				volumeSnapshotName, common.VolumeSnapshotInfoKey, snapshotID, err)
		}
		return createSnapshotResponse, nil
	}

	start := time.Now()
	resp, err := createSnapshotInternal()
	if err != nil {
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
	log.Infof("DeleteSnapshot: called with args %+v", *req)
	volumeType := prometheus.PrometheusBlockVolumeType
	start := time.Now()
	isBlockVolumeSnapshotWCPEnabled := commonco.ContainerOrchestratorUtility.IsFSSEnabled(ctx, common.BlockVolumeSnapshot)
	if !isBlockVolumeSnapshotWCPEnabled {
		return nil, logger.LogNewErrorCode(log, codes.Unimplemented, "deleteSnapshot")
	}
	deleteSnapshotInternal := func() (*csi.DeleteSnapshotResponse, error) {
		csiSnapshotID := req.GetSnapshotId()
		isStorageQuotaM2FSSEnabled := commonco.ContainerOrchestratorUtility.IsFSSEnabled(ctx,
			common.StorageQuotaM2)
		if isStorageQuotaM2FSSEnabled {
			volumeID, _, err := common.ParseCSISnapshotID(csiSnapshotID)
			if err != nil {
				return nil, err
			}
			cnsVolumeInfo, err := volumeInfoService.GetVolumeInfoForVolumeID(ctx, volumeID)
			if err != nil {
				return nil, logger.LogNewErrorCodef(log, codes.Internal,
					"failed to retrieve cnsVolumeInfo for volume: %s Error: %+v", volumeID, err)
			}
			cnsSnapshotInfo, err := common.DeleteSnapshotUtil(ctx, c.manager.VolumeManager, csiSnapshotID,
				&cnsvolume.DeletesnapshotExtraParams{
					StorageClassName:           cnsVolumeInfo.Spec.StorageClassName,
					StoragePolicyID:            cnsVolumeInfo.Spec.StoragePolicyID,
					Capacity:                   resource.NewQuantity(0, resource.BinarySI),
					Namespace:                  cnsVolumeInfo.Namespace,
					IsStorageQuotaM2FSSEnabled: isStorageQuotaM2FSSEnabled,
				})
			if err != nil {
				return nil, logger.LogNewErrorCodef(log, codes.Internal,
					"Failed to delete WCP snapshot %q. Error: %+v",
					csiSnapshotID, err)
			}
			if cnsVolumeInfo.Spec.SnapshotLatestOperationCompleteTime.Time.Before(
				cnsSnapshotInfo.SnapshotLatestOperationCompleteTime) {
				patch, err := common.GetValidatedCNSVolumeInfoPatch(ctx, cnsSnapshotInfo)
				if err != nil {
					return nil, err
				}
				err = c.UpdateCNSVolumeInfo(ctx, patch, cnsVolumeInfo.Spec.VolumeID)
				if err != nil {
					return nil, err
				}
			} else {
				log.Infof("CNSVolumeInfo is already updated, skiping update for volume %d and snapshot %d",
					cnsVolumeInfo.Spec.VolumeID, cnsSnapshotInfo.SnapshotID)
			}
		} else {
			_, err := common.DeleteSnapshotUtil(ctx, c.manager.VolumeManager, csiSnapshotID, nil)
			if err != nil {
				return nil, logger.LogNewErrorCodef(log, codes.Internal,
					"Failed to delete WCP snapshot %q. Error: %+v",
					csiSnapshotID, err)
			}
		}

		log.Infof("DeleteSnapshot: successfully deleted snapshot %q", csiSnapshotID)
		return &csi.DeleteSnapshotResponse{}, nil
	}
	resp, err := deleteSnapshotInternal()
	if err != nil {
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
	ctx = logger.NewContextWithLogger(ctx)
	log := logger.GetLogger(ctx)
	volumeType := prometheus.PrometheusBlockVolumeType
	log.Infof("ListSnapshots: called with args %+v", *req)
	isBlockVolumeSnapshotWCPEnabled := commonco.ContainerOrchestratorUtility.IsFSSEnabled(ctx, common.BlockVolumeSnapshot)
	if !isBlockVolumeSnapshotWCPEnabled {
		return nil, logger.LogNewErrorCode(log, codes.Unimplemented, "listSnapshot")
	}
	listSnapshotsInternal := func() (*csi.ListSnapshotsResponse, error) {
		err := validateWCPListSnapshotRequest(ctx, req)
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
			return nil, logger.LogNewErrorCodef(log, codes.Internal, "failed to retrieve the snapshots, err: %+v", err)
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
	start := time.Now()
	resp, err := listSnapshotsInternal()
	if err != nil {
		prometheus.CsiControlOpsHistVec.WithLabelValues(volumeType, prometheus.PrometheusListSnapshotsOpType,
			prometheus.PrometheusFailStatus, "NotComputed").Observe(time.Since(start).Seconds())
	} else {
		prometheus.CsiControlOpsHistVec.WithLabelValues(volumeType, prometheus.PrometheusListSnapshotsOpType,
			prometheus.PrometheusPassStatus, "").Observe(time.Since(start).Seconds())
	}
	return resp, err
}

// ControllerExpandVolume expands a volume.
func (c *controller) ControllerExpandVolume(ctx context.Context, req *csi.ControllerExpandVolumeRequest) (
	*csi.ControllerExpandVolumeResponse, error) {
	start := time.Now()
	ctx = logger.NewContextWithLogger(ctx)
	log := logger.GetLogger(ctx)
	volumeType := prometheus.PrometheusUnknownVolumeType
	cnsVolumeType := common.UnknownVolumeType
	controllerExpandVolumeInternal := func() (
		*csi.ControllerExpandVolumeResponse, string, error) {
		var err error
		if !commonco.ContainerOrchestratorUtility.IsFSSEnabled(ctx, common.VolumeExtend) {
			return nil, csifault.CSIUnimplementedFault, logger.LogNewErrorCode(log, codes.Unimplemented,
				"expandVolume feature is disabled on the cluster")
		}
		log.Infof("ControllerExpandVolume: called with args %+v", *req)
		// TODO: If the err is returned by invoking CNS API, then faultType should be
		// populated by the underlying layer.
		// If the request failed due to validate the request, "csi.fault.InvalidArgument" will be return.
		// If thr reqeust failed due to object not found, "csi.fault.NotFound" will be return.
		// For all other cases, the faultType will be set to "csi.fault.Internal" for now.
		// Later we may need to define different csi faults.
		// Check if the volume contains CNS snapshots only for block volumes.
		if cnsVolumeType == common.UnknownVolumeType {
			cnsVolumeType, err = common.GetCnsVolumeType(ctx, c.manager.VolumeManager, req.VolumeId)
			if err != nil {
				return nil, csifault.CSIInternalFault, logger.LogNewErrorCodef(log, codes.Internal,
					"failed to determine CNS volume type for volume: %q. Error: %+v", req.VolumeId, err)
			}
			volumeType = convertCnsVolumeType(ctx, cnsVolumeType)
		}
		if cnsVolumeType == common.BlockVolumeType &&
			commonco.ContainerOrchestratorUtility.IsFSSEnabled(ctx, common.BlockVolumeSnapshot) {
			snapshots, _, err := common.QueryVolumeSnapshotsByVolumeID(ctx, c.manager.VolumeManager, req.VolumeId,
				common.QuerySnapshotLimit)
			if err != nil {
				return nil, csifault.CSIInternalFault, logger.LogNewErrorCodef(log, codes.Internal,
					"failed to retrieve snapshots for volume: %s. Error: %+v", req.VolumeId, err)
			}
			if len(snapshots) == 0 {
				log.Infof("no CNS snapshots found for volume: %s, the volume can be safely expanded",
					req.VolumeId)
			} else {
				return nil, csifault.CSIInternalFault, logger.LogNewErrorCodef(log, codes.FailedPrecondition,
					"volume: %s with existing snapshots %v cannot be expanded, "+
						"please delete snapshots before deleting the volume", req.VolumeId, snapshots)
			}

		}
		isOnlineExpansionEnabled := commonco.ContainerOrchestratorUtility.IsFSSEnabled(ctx, common.OnlineVolumeExtend)
		err = validateWCPControllerExpandVolumeRequest(ctx, req, c.manager, isOnlineExpansionEnabled)
		if err != nil {
			log.Errorf("validation for ExpandVolume Request: %+v has failed. Error: %v", *req, err)
			return nil, csifault.CSIInvalidArgumentFault, err
		}
		volumeType = prometheus.PrometheusBlockVolumeType
		volumeID := req.GetVolumeId()
		volSizeBytes := int64(req.GetCapacityRange().GetRequiredBytes())
		volSizeMB := int64(common.RoundUpSize(volSizeBytes, common.MbInBytes))
		var (
			faultType     string
			cnsVolumeInfo *cnsvolumeinfov1alpha1.CNSVolumeInfo
		)
		if isPodVMOnStretchSupervisorFSSEnabled {
			cnsVolumeInfo, err = volumeInfoService.GetVolumeInfoForVolumeID(ctx, volumeID)
			if err != nil {
				return nil, csifault.CSIInternalFault, logger.LogNewErrorCodef(log, codes.Internal,
					"failed to retrieve cnsVolumeInfo for volume: %s Error: %+v", req.VolumeId, err)
			}
			faultType, err = common.ExpandVolumeUtil(ctx, c.manager.VcenterManager,
				c.manager.VcenterConfig.Host, c.manager.VolumeManager, volumeID, volSizeMB,
				commonco.ContainerOrchestratorUtility.IsFSSEnabled(ctx, common.AsyncQueryVolume),
				&cnsvolume.ExpandVolumeExtraParams{
					StorageClassName:                     cnsVolumeInfo.Spec.StorageClassName,
					StoragePolicyID:                      cnsVolumeInfo.Spec.StoragePolicyID,
					Namespace:                            cnsVolumeInfo.Spec.Namespace,
					Capacity:                             cnsVolumeInfo.Spec.Capacity,
					IsPodVMOnStretchSupervisorFSSEnabled: isPodVMOnStretchSupervisorFSSEnabled,
				})
		} else {
			faultType, err = common.ExpandVolumeUtil(ctx, c.manager.VcenterManager,
				c.manager.VcenterConfig.Host, c.manager.VolumeManager, volumeID, volSizeMB,
				commonco.ContainerOrchestratorUtility.IsFSSEnabled(ctx, common.AsyncQueryVolume), nil)
		}
		if err != nil {
			return nil, faultType, logger.LogNewErrorCodef(log, codes.Internal,
				"failed to expand volume: %+q to size: %d err %+v", volumeID, volSizeMB, err)
		}

		// Always set nodeExpansionRequired to true, even if requested size is
		// equal to current size. Volume expansion may succeed on CNS but
		// external-resizer may fail to update API server. Requests are requeued
		// in this case. Setting nodeExpandsionRequired to false marks PVC
		// resize as finished which prevents kubelet from expanding the filesystem.
		// Ref: https://github.com/kubernetes-csi/external-resizer/blob/master/pkg/controller/controller.go#L335
		nodeExpansionRequired := true
		// Set NodeExpansionRequired to false for raw block volumes.
		if _, ok := req.GetVolumeCapability().GetAccessType().(*csi.VolumeCapability_Block); ok {
			nodeExpansionRequired = false
		}
		if isPodVMOnStretchSupervisorFSSEnabled {
			// Increase capacity in CNSVolumeInfo instance.
			patch := map[string]interface{}{
				"spec": map[string]interface{}{
					"capacity": volSizeBytes,
				},
			}
			patchBytes, err := json.Marshal(patch)
			if err != nil {
				return nil, csifault.CSIInternalFault, logger.LogNewErrorCodef(log, codes.Internal,
					"failed to create patch for CNSVolumeInfo instance. Error: %+v", err)
			}
			err = volumeInfoService.PatchVolumeInfo(ctx, volumeID, patchBytes, allowedRetriesToPatchCNSVolumeInfo)
			if err != nil {
				return nil, csifault.CSIInternalFault, logger.LogNewErrorCodef(log, codes.Internal,
					"failed to patch CNSVolumeInfo instance to increase capacity from %q to %d."+
						" Error: %+v", cnsVolumeInfo.Spec.Capacity.String(), volSizeMB, err)
			}
		}

		resp := &csi.ControllerExpandVolumeResponse{
			CapacityBytes:         int64(units.FileSize(volSizeMB * common.MbInBytes)),
			NodeExpansionRequired: nodeExpansionRequired,
		}
		return resp, "", nil
	}
	resp, faultType, err := controllerExpandVolumeInternal()
	log.Debugf("controllerExpandVolumeInternal: returns fault %q for volume %q", faultType, req.VolumeId)

	if err != nil {
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

func (c *controller) ControllerGetVolume(ctx context.Context, req *csi.ControllerGetVolumeRequest) (
	*csi.ControllerGetVolumeResponse, error) {
	return nil, status.Error(codes.Unimplemented, "")
}

func (c *controller) ControllerModifyVolume(ctx context.Context, req *csi.ControllerModifyVolumeRequest) (
	*csi.ControllerModifyVolumeResponse, error) {
	return nil, status.Error(codes.Unimplemented, "")
}

func (c *controller) UpdateCNSVolumeInfo(ctx context.Context, patch map[string]interface{}, volumeID string) error {
	log := logger.GetLogger(ctx)
	patchBytes, err := json.Marshal(patch)
	if err != nil {
		return logger.LogNewErrorCodef(log, codes.Internal,
			"failed to create patch for CNSVolumeInfo instance. Error: %+v", err)
	}
	err = volumeInfoService.PatchVolumeInfo(ctx, volumeID, patchBytes, allowedRetriesToPatchCNSVolumeInfo)
	if err != nil {
		return logger.LogNewErrorCodef(log, codes.Internal,
			"failed to patch CNSVolumeInfo instance to update snapshot details."+
				" Error: %+v", err)
	}
	return nil
}
