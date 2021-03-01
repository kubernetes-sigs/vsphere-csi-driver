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
	"fmt"
	"net/http"
	"path/filepath"
	"strings"
	"time"

	"github.com/prometheus/client_golang/prometheus/promhttp"
	"sigs.k8s.io/vsphere-csi-driver/pkg/common/prometheus"

	"github.com/container-storage-interface/spec/lib/go/csi"
	"github.com/fsnotify/fsnotify"
	cnstypes "github.com/vmware/govmomi/cns/types"
	"github.com/vmware/govmomi/units"
	"golang.org/x/net/context"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	v1 "k8s.io/api/core/v1"

	cnsvolume "sigs.k8s.io/vsphere-csi-driver/pkg/common/cns-lib/volume"
	cnsvsphere "sigs.k8s.io/vsphere-csi-driver/pkg/common/cns-lib/vsphere"
	cnsconfig "sigs.k8s.io/vsphere-csi-driver/pkg/common/config"
	"sigs.k8s.io/vsphere-csi-driver/pkg/csi/service/common"
	"sigs.k8s.io/vsphere-csi-driver/pkg/csi/service/common/commonco"
	"sigs.k8s.io/vsphere-csi-driver/pkg/csi/service/logger"
	csitypes "sigs.k8s.io/vsphere-csi-driver/pkg/csi/types"
)

const (
	vsanDirect = "vsanD"
	vsanSna    = "vsan-sna"
)

var (
	// controllerCaps represents the capability of controller service
	controllerCaps = []csi.ControllerServiceCapability_RPC_Type{
		csi.ControllerServiceCapability_RPC_CREATE_DELETE_VOLUME,
		csi.ControllerServiceCapability_RPC_PUBLISH_UNPUBLISH_VOLUME,
		csi.ControllerServiceCapability_RPC_EXPAND_VOLUME,
	}
)

var getCandidateDatastores = cnsvsphere.GetCandidateDatastoresInCluster

type controller struct {
	manager *common.Manager
	authMgr common.AuthorizationService
}

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

	log.Infof("Initializing WCP CSI controller")
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
		VcenterManager: cnsvsphere.GetVirtualCenterManager(ctx),
	}

	vc, err := common.GetVCenter(ctx, c.manager)
	if err != nil {
		log.Errorf("failed to get vcenter. err=%v", err)
		return err
	}
	// Check vCenter API Version
	if err = common.CheckAPI(vc.Client.ServiceContent.About.ApiVersion); err != nil {
		log.Errorf("checkAPI failed for vcenter API version: %s, err=%v", vc.Client.ServiceContent.About.ApiVersion, err)
		return err
	}
	go cnsvolume.ClearTaskInfoObjects()
	cfgPath := common.GetConfigPath(ctx)
	watcher, err := fsnotify.NewWatcher()
	if err != nil {
		log.Errorf("failed to create fsnotify watcher. err=%v", err)
		return err
	}

	if commonco.ContainerOrchestratorUtility.IsFSSEnabled(ctx, common.CSIAuthCheck) {
		log.Info("CSIAuthCheck feature is enabled, loading AuthorizationService")
		authMgr, err := common.GetAuthorizationService(ctx, vc)
		if err != nil {
			log.Errorf("failed to initialize authMgr. err=%v", err)
			return err
		}
		c.authMgr = authMgr
		// TODO: Invoke similar method for block volumes
		go common.ComputeDatastoreMapForFileVolumes(authMgr.(*common.AuthManager), config.Global.CSIAuthCheckIntervalInMin)
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
					c.ReloadConfiguration()
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
func (c *controller) ReloadConfiguration() {
	ctx, log := logger.GetNewContextWithLogger()
	log.Info("Reloading Configuration")
	cfg, err := common.GetConfig(ctx)
	if err != nil {
		log.Errorf("failed to read config. Error: %+v", err)
		return
	}
	newVCConfig, err := cnsvsphere.GetVirtualCenterConfig(ctx, cfg)
	if err != nil {
		log.Errorf("failed to get VirtualCenterConfig. err=%v", err)
		return
	}
	if newVCConfig != nil {
		var vcenter *cnsvsphere.VirtualCenter
		if c.manager.VcenterConfig.Host != newVCConfig.Host ||
			c.manager.VcenterConfig.Username != newVCConfig.Username ||
			c.manager.VcenterConfig.Password != newVCConfig.Password {
			log.Debugf("Unregistering virtual center: %q from virtualCenterManager", c.manager.VcenterConfig.Host)
			err = c.manager.VcenterManager.UnregisterAllVirtualCenters(ctx)
			if err != nil {
				log.Errorf("failed to unregister vcenter with virtualCenterManager.")
				return
			}
			log.Debugf("Registering virtual center: %q with virtualCenterManager", newVCConfig.Host)
			vcenter, err = c.manager.VcenterManager.RegisterVirtualCenter(ctx, newVCConfig)
			if err != nil {
				log.Errorf("failed to register VC with virtualCenterManager. err=%v", err)
				return
			}
			c.manager.VcenterManager = cnsvsphere.GetVirtualCenterManager(ctx)
		} else {
			vcenter, err = c.manager.VcenterManager.GetVirtualCenter(ctx, newVCConfig.Host)
			if err != nil {
				log.Errorf("failed to get VirtualCenter. err=%v", err)
				return
			}
			vcenter.Config = newVCConfig
		}
		c.manager.VolumeManager.ResetManager(ctx, vcenter)
		c.manager.VolumeManager = cnsvolume.GetManager(ctx, vcenter)
		c.manager.VcenterConfig = newVCConfig
	}
	if cfg != nil {
		log.Debugf("updating manager.CnsConfig")
		c.manager.CnsConfig = cfg
	}
	log.Info("Successfully reloaded configuration")
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

	var (
		storagePolicyID     string
		affineToHost        string
		storagePool         string
		topologyRequirement *csi.TopologyRequirement
		accessibleNodes     []string // This will be used to populate volumeAccessTopology
		err                 error
	)
	// Fetch the accessibility requirements from the request
	topologyRequirement = req.GetAccessibilityRequirements()
	var selectedDatastoreURL string
	// Support case insensitive parameters
	for paramName := range req.Parameters {
		param := strings.ToLower(paramName)
		if param == common.AttributeStoragePolicyID {
			storagePolicyID = req.Parameters[paramName]
		} else if param == common.AttributeStoragePool {
			storagePool = req.Parameters[paramName]
			if !isValidAccessibilityRequirement(topologyRequirement) {
				return nil, status.Errorf(codes.InvalidArgument, "invalid accessibility requirements")
			}
			spAccessibleNodes, storagePoolType, err := getStoragePoolInfo(ctx, storagePool)
			if err != nil {
				msg := fmt.Sprintf("Error in specified StoragePool %s. Error: %+v", storagePool, err)
				return nil, status.Errorf(codes.Internal, msg)
			}
			overlappingNodes, err := getOverlappingNodes(spAccessibleNodes, topologyRequirement)
			if err != nil || len(overlappingNodes) == 0 {
				msg := fmt.Sprintf("getOverlappingNodes failed: %v", err)
				return nil, status.Errorf(codes.Internal, msg)
			}
			accessibleNodes = append(accessibleNodes, overlappingNodes...)
			log.Infof("Storage pool Accessible nodes for volume topology: %+v", accessibleNodes)

			if storagePoolType == vsanDirect {
				selectedDatastoreURL, err = getDatastoreURLFromStoragePool(ctx, storagePool)
				if err != nil {
					msg := fmt.Sprintf("Error in specified StoragePool %s. Error: %+v", storagePool, err)
					log.Error(msg)
					return nil, status.Errorf(codes.Internal, msg)
				}
				log.Infof("Will select datastore %s as per the provided storage pool %s", selectedDatastoreURL, storagePool)
			} else if storagePoolType == vsanSna {
				// Query API server to get ESX Host Moid from the hostLocalNodeName
				if len(accessibleNodes) != 1 {
					return nil, status.Errorf(codes.Internal, "too many accessible nodes")
				}
				hostMoid, err := getHostMOIDFromK8sCloudOperatorService(ctx, accessibleNodes[0])
				if err != nil {
					log.Error(err)
					return nil, status.Errorf(codes.Internal, "failed to get ESX Host Moid from API server")
				}
				affineToHost = hostMoid
				log.Debugf("Setting the affineToHost value as %s", affineToHost)
			}
		}
	}

	var createVolumeSpec = common.CreateVolumeSpec{
		CapacityMB:             volSizeMB,
		Name:                   req.Name,
		StoragePolicyID:        storagePolicyID,
		ScParams:               &common.StorageClassParams{},
		AffineToHost:           affineToHost,
		VolumeType:             common.BlockVolumeType,
		VsanDirectDatastoreURL: selectedDatastoreURL,
	}
	// Get candidate datastores for the Kubernetes cluster
	vc, err := common.GetVCenter(ctx, c.manager)
	if err != nil {
		msg := fmt.Sprintf("Failed to get vCenter from Manager. Error: %v", err)
		log.Error(msg)
		return nil, status.Errorf(codes.Internal, msg)
	}
	sharedDatastores, vsanDirectDatastores, err := getCandidateDatastores(ctx, vc, c.manager.CnsConfig.Global.ClusterID)
	if err != nil {
		msg := fmt.Sprintf("Failed finding candidate datastores to place volume. Error: %v", err)
		log.Error(msg)
		return nil, status.Errorf(codes.Internal, msg)
	}

	candidateDatastores := append(sharedDatastores, vsanDirectDatastores...)
	volumeInfo, err := common.CreateBlockVolumeUtil(ctx, cnstypes.CnsClusterFlavorWorkload, c.manager, &createVolumeSpec, candidateDatastores)
	if err != nil {
		msg := fmt.Sprintf("failed to create volume. Error: %+v", err)
		log.Error(msg)
		return nil, status.Errorf(codes.Internal, msg)
	}

	attributes := make(map[string]string)
	attributes[common.AttributeDiskType] = common.DiskTypeBlockVolume
	resp := &csi.CreateVolumeResponse{
		Volume: &csi.Volume{
			VolumeId:      volumeInfo.VolumeID.Id,
			CapacityBytes: int64(units.FileSize(volSizeMB * common.MbInBytes)),
			VolumeContext: attributes,
		},
	}
	// Configure the volumeTopology in the response so that the external provisioner will properly sets up the
	// nodeAffinity for this volume
	if isValidAccessibilityRequirement(topologyRequirement) {
		for _, hostName := range accessibleNodes {
			volumeTopology := &csi.Topology{
				Segments: map[string]string{
					v1.LabelHostname: hostName,
				},
			}
			resp.Volume.AccessibleTopology = append(resp.Volume.AccessibleTopology, volumeTopology)
		}
		log.Debugf("Volume Accessible Topology: %+v", resp.Volume.AccessibleTopology)
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

	var storagePolicyID string
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

	var volumeID string
	var err error

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
	volumeID, err = common.CreateFileVolumeUtil(ctx, cnstypes.CnsClusterFlavorWorkload,
		c.manager, &createVolumeSpec, filteredDatastores)
	if err != nil {
		msg := fmt.Sprintf("failed to create volume. Error: %+v", err)
		log.Error(msg)
		return nil, status.Errorf(codes.Internal, msg)
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

		isBlockRequest := !common.IsFileVolumeRequest(ctx, req.GetVolumeCapabilities())
		if isBlockRequest {
			volumeType = prometheus.PrometheusBlockVolumeType
		} else {
			volumeType = prometheus.PrometheusFileVolumeType
		}
		// Validate create request
		err := validateWCPCreateVolumeRequest(ctx, req, isBlockRequest)
		if err != nil {
			msg := fmt.Sprintf("Validation for CreateVolume Request: %+v has failed. Error: %+v", *req, err)
			log.Error(msg)
			return nil, err
		}

		if !isBlockRequest {
			if !commonco.ContainerOrchestratorUtility.IsFSSEnabled(ctx, common.FileVolume) || !commonco.ContainerOrchestratorUtility.IsFSSEnabled(ctx, common.CSIAuthCheck) {
				msg := "File volume feature is disabled on the cluster"
				log.Warn(msg)
				return nil, status.Errorf(codes.Unimplemented, msg)
			}
			return c.createFileVolume(ctx, req)
		}
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
		err = validateWCPDeleteVolumeRequest(ctx, req)
		if err != nil {
			msg := fmt.Sprintf("Validation for DeleteVolume Request: %+v has failed. Error: %+v", *req, err)
			log.Error(msg)
			return nil, err
		}
		// TODO: Add code to determine the volume type and set volumeType for Prometheus metric accordingly.
		err = common.DeleteVolumeUtil(ctx, c.manager.VolumeManager, req.VolumeId, true)
		if err != nil {
			msg := fmt.Sprintf("failed to delete volume: %q. Error: %+v", req.VolumeId, err)
			log.Error(msg)
			return nil, status.Errorf(codes.Internal, msg)
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
	ctx = logger.NewContextWithLogger(ctx)
	log := logger.GetLogger(ctx)
	log.Infof("ControllerPublishVolume: called with args %+v", *req)
	err := validateWCPControllerPublishVolumeRequest(ctx, req)
	if err != nil {
		msg := fmt.Sprintf("Validation for PublishVolume Request: %+v has failed. Error: %v", *req, err)
		log.Errorf(msg)
		return nil, err
	}

	vmuuid, err := getVMUUIDFromK8sCloudOperatorService(ctx, req.VolumeId, req.NodeId)
	if err != nil {
		msg := fmt.Sprintf("Failed to get the pod vmuuid annotation from the k8sCloudOperator service when processing attach for volumeID: %s on node: %s. Error: %+v", req.VolumeId, req.NodeId, err)
		log.Error(msg)
		return nil, status.Errorf(codes.Internal, msg)
	}

	vcdcMap, err := getDatacenterFromConfig(c.manager.CnsConfig)
	if err != nil {
		msg := fmt.Sprintf("failed to get datacenter from config with error: %+v", err)
		log.Error(msg)
		return nil, status.Errorf(codes.Internal, msg)
	}
	var vCenterHost, dcMorefValue string
	for key, value := range vcdcMap {
		vCenterHost = key
		dcMorefValue = value
	}
	vc, err := c.manager.VcenterManager.GetVirtualCenter(ctx, vCenterHost)
	if err != nil {
		msg := fmt.Sprintf("Cannot get virtual center %s from virtualcentermanager while attaching disk with error %+v",
			vc.Config.Host, err)
		log.Error(msg)
		return nil, status.Errorf(codes.Internal, msg)
	}

	// Connect to VC
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	err = vc.Connect(ctx)
	if err != nil {
		msg := fmt.Sprintf("failed to connect to Virtual Center: %s", vc.Config.Host)
		log.Error(msg)
		return nil, status.Errorf(codes.Internal, msg)
	}

	podVM, err := getVMByInstanceUUIDInDatacenter(ctx, vc, dcMorefValue, vmuuid)
	if err != nil {
		msg := fmt.Sprintf("failed to the PodVM Moref from the PodVM UUID: %s in datacenter: %s with err: %+v", vmuuid, dcMorefValue, err)
		log.Error(msg)
		return nil, status.Errorf(codes.Internal, msg)
	}

	// Attach the volume to the node
	diskUUID, err := common.AttachVolumeUtil(ctx, c.manager, podVM, req.VolumeId)
	if err != nil {
		if commonco.ContainerOrchestratorUtility.IsFSSEnabled(ctx, common.FakeAttach) {
			log.Infof("Volume attachment failed. Checking if it can be fake attached")
			var capabilities []*csi.VolumeCapability
			capabilities = append(capabilities, req.VolumeCapability)
			if !common.IsFileVolumeRequest(ctx, capabilities) { //Block volume
				allowed, err := commonco.ContainerOrchestratorUtility.IsFakeAttachAllowed(ctx, req.VolumeId, c.manager.VolumeManager)
				if err != nil {
					msg := fmt.Sprintf("failed to determine if volume: %s can be fake attached. Error: %+v", req.VolumeId, err)
					log.Error(msg)
					return nil, status.Errorf(codes.Internal, msg)
				}

				if allowed {
					// Mark the volume as fake attached before returning response
					err := commonco.ContainerOrchestratorUtility.MarkFakeAttached(ctx, req.VolumeId)
					if err != nil {
						msg := fmt.Sprintf("failed to mark volume: %s as fake attached. Error: %+v", req.VolumeId, err)
						log.Error(msg)
						return nil, status.Errorf(codes.Internal, msg)
					}

					publishInfo := make(map[string]string)
					publishInfo[common.AttributeDiskType] = common.DiskTypeBlockVolume
					publishInfo[common.AttributeFakeAttached] = "true"

					resp := &csi.ControllerPublishVolumeResponse{
						PublishContext: publishInfo,
					}
					log.Infof("Volume %s has been fake attached", req.VolumeId)
					return resp, nil
				}
			}

			log.Infof("Volume %s is not eligible to be fake attached", req.VolumeId)
		}
		msg := fmt.Sprintf("failed to attach volume with volumeID: %s. Error: %+v", req.VolumeId, err)
		log.Error(msg)
		return nil, status.Errorf(codes.Internal, msg)
	}

	publishInfo := make(map[string]string)
	publishInfo[common.AttributeDiskType] = common.DiskTypeBlockVolume
	publishInfo[common.AttributeFirstClassDiskUUID] = common.FormatDiskUUID(diskUUID)
	resp := &csi.ControllerPublishVolumeResponse{
		PublishContext: publishInfo,
	}

	return resp, nil
}

// ControllerUnpublishVolume detaches a volume from the Node VM.
// volume id and node name is retrieved from ControllerUnpublishVolumeRequest
func (c *controller) ControllerUnpublishVolume(ctx context.Context, req *csi.ControllerUnpublishVolumeRequest) (
	*csi.ControllerUnpublishVolumeResponse, error) {
	ctx = logger.NewContextWithLogger(ctx)
	log := logger.GetLogger(ctx)
	log.Infof("ControllerUnpublishVolume: called with args %+v", *req)
	err := validateWCPControllerUnpublishVolumeRequest(ctx, req)
	if err != nil {
		msg := fmt.Sprintf("Validation for UnpublishVolume Request: %+v has failed. Error: %v", *req, err)
		log.Error(msg)
		return nil, err
	}

	if commonco.ContainerOrchestratorUtility.IsFSSEnabled(ctx, common.FakeAttach) {
		// Check if the volume was fake attached and unmark it as not fake attached
		if err := commonco.ContainerOrchestratorUtility.ClearFakeAttached(ctx, req.VolumeId); err != nil {
			msg := fmt.Sprintf("Failed to unmark volume as not fake attached. Error: %v", err)
			log.Error(msg)
			return nil, err
		}
	}
	return &csi.ControllerUnpublishVolumeResponse{}, nil
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

func (c *controller) ControllerGetCapabilities(ctx context.Context, req *csi.ControllerGetCapabilitiesRequest) (
	*csi.ControllerGetCapabilitiesResponse, error) {

	ctx = logger.NewContextWithLogger(ctx)
	log := logger.GetLogger(ctx)
	log.Infof("ControllerGetCapabilities: called with args %+v", *req)
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

// ControllerExpandVolume expands a volume.
func (c *controller) ControllerExpandVolume(ctx context.Context, req *csi.ControllerExpandVolumeRequest) (
	*csi.ControllerExpandVolumeResponse, error) {
	ctx = logger.NewContextWithLogger(ctx)
	log := logger.GetLogger(ctx)
	if !commonco.ContainerOrchestratorUtility.IsFSSEnabled(ctx, common.VolumeExtend) {
		msg := "ExpandVolume feature is disabled on the cluster"
		log.Warn(msg)
		return nil, status.Errorf(codes.Unimplemented, msg)
	}
	log.Infof("ControllerExpandVolume: called with args %+v", *req)

	isOnlineExpansionEnabled := commonco.ContainerOrchestratorUtility.IsFSSEnabled(ctx, common.OnlineVolumeExtend)
	err := validateWCPControllerExpandVolumeRequest(ctx, req, c.manager, isOnlineExpansionEnabled)
	if err != nil {
		log.Errorf("validation for ExpandVolume Request: %+v has failed. Error: %v", *req, err)
		return nil, err
	}
	volumeID := req.GetVolumeId()
	volSizeBytes := int64(req.GetCapacityRange().GetRequiredBytes())
	volSizeMB := int64(common.RoundUpSize(volSizeBytes, common.MbInBytes))

	err = common.ExpandVolumeUtil(ctx, c.manager, volumeID, volSizeMB)
	if err != nil {
		msg := fmt.Sprintf("failed to expand volume: %+q to size: %d err %+v", volumeID, volSizeMB, err)
		log.Error(msg)
		return nil, status.Errorf(codes.Internal, msg)
	}

	// Always set nodeExpansionRequired to true, even if requested size is equal to current size.
	// Volume expansion may succeed on CNS but external-resizer may fail to update API server.
	// Requests are requeued in this case. Setting nodeExpandsionRequired to false marks PVC
	// resize as finished which prevents kubelet from expanding the filesystem.
	// Ref: https://github.com/kubernetes-csi/external-resizer/blob/master/pkg/controller/controller.go#L335
	nodeExpansionRequired := true
	// Set NodeExpansionRequired to false for raw block volumes
	if _, ok := req.GetVolumeCapability().GetAccessType().(*csi.VolumeCapability_Block); ok {
		nodeExpansionRequired = false
	}
	resp := &csi.ControllerExpandVolumeResponse{
		CapacityBytes:         int64(units.FileSize(volSizeMB * common.MbInBytes)),
		NodeExpansionRequired: nodeExpansionRequired,
	}
	return resp, nil
}
