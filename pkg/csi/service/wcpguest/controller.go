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

package wcpguest

import (
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
	vmoperatortypes "github.com/vmware-tanzu/vm-operator-api/api/v1alpha1"
	"golang.org/x/net/context"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/fields"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/watch"
	clientset "k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/cache"
	"sigs.k8s.io/controller-runtime/pkg/client"

	cnsoperatorv1alpha1 "sigs.k8s.io/vsphere-csi-driver/v2/pkg/apis/cnsoperator"
	cnsfileaccessconfigv1alpha1 "sigs.k8s.io/vsphere-csi-driver/v2/pkg/apis/cnsoperator/cnsfileaccessconfig/v1alpha1"
	cnsconfig "sigs.k8s.io/vsphere-csi-driver/v2/pkg/common/config"
	"sigs.k8s.io/vsphere-csi-driver/v2/pkg/common/prometheus"
	"sigs.k8s.io/vsphere-csi-driver/v2/pkg/csi/service/common"
	"sigs.k8s.io/vsphere-csi-driver/v2/pkg/csi/service/common/commonco"
	"sigs.k8s.io/vsphere-csi-driver/v2/pkg/csi/service/logger"
	csitypes "sigs.k8s.io/vsphere-csi-driver/v2/pkg/csi/types"
	k8s "sigs.k8s.io/vsphere-csi-driver/v2/pkg/kubernetes"
)

var (
	// controllerCaps represents the capability of controller service
	controllerCaps = []csi.ControllerServiceCapability_RPC_Type{
		csi.ControllerServiceCapability_RPC_CREATE_DELETE_VOLUME,
		csi.ControllerServiceCapability_RPC_PUBLISH_UNPUBLISH_VOLUME,
		csi.ControllerServiceCapability_RPC_EXPAND_VOLUME,
	}
	// virtualMachineLock is used for handling race conditions during concurrent Attach/Detach calls
	virtualMachineLock = &sync.Mutex{}
)

type controller struct {
	supervisorClient          clientset.Interface
	restClientConfig          *rest.Config
	vmOperatorClient          client.Client
	cnsOperatorClient         client.Client
	vmWatcher                 *cache.ListWatch
	supervisorNamespace       string
	tanzukubernetesClusterUID string
}

// New creates a CNS controller
func New() csitypes.CnsController {
	return &controller{}
}

// Init is initializing controller struct
func (c *controller) Init(config *cnsconfig.Config, version string) error {
	ctx, log := logger.GetNewContextWithLogger()
	log.Infof("Initializing WCPGC CSI controller")
	var err error
	// connect to the CSI controller in supervisor cluster
	c.supervisorNamespace, err = cnsconfig.GetSupervisorNamespace(ctx)
	if err != nil {
		return err
	}
	c.tanzukubernetesClusterUID = config.GC.TanzuKubernetesClusterUID
	c.restClientConfig = k8s.GetRestClientConfigForSupervisor(ctx, config.GC.Endpoint, config.GC.Port)
	c.supervisorClient, err = k8s.NewSupervisorClient(ctx, c.restClientConfig)
	if err != nil {
		log.Errorf("failed to create supervisorClient. Error: %+v", err)
		return err
	}

	c.vmOperatorClient, err = k8s.NewClientForGroup(ctx, c.restClientConfig, vmoperatortypes.GroupName)
	if err != nil {
		log.Errorf("failed to create vmOperatorClient. Error: %+v", err)
		return err
	}

	c.cnsOperatorClient, err = k8s.NewClientForGroup(ctx, c.restClientConfig, cnsoperatorv1alpha1.GroupName)
	if err != nil {
		log.Errorf("failed to create cnsOperatorClient. Error: %+v", err)
		return err
	}
	c.vmWatcher, err = k8s.NewVirtualMachineWatcher(ctx, c.restClientConfig, c.supervisorNamespace)
	if err != nil {
		log.Errorf("failed to create vmWatcher. Error: %+v", err)
		return err
	}

	pvcsiConfigPath := common.GetConfigPath(ctx)
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
							log.Infof("Successfully reloaded configuration from: %q", pvcsiConfigPath)
							break
						}
						log.Errorf("failed to reload configuration. will retry again in 5 seconds. err: %+v", reloadConfigErr)
						time.Sleep(5 * time.Second)
					}
				}
			case err, ok := <-watcher.Errors:
				if !ok {
					return
				}
				log.Errorf("fsnotify error: %+v", err)
			}
			log.Debugf("fsnotify event processed")
		}
	}()
	cfgDirPath := filepath.Dir(pvcsiConfigPath)
	log.Infof("Adding watch on path: %q", cfgDirPath)
	err = watcher.Add(cfgDirPath)
	if err != nil {
		log.Errorf("failed to watch on path: %q. err=%v", cfgDirPath, err)
		return err
	}
	log.Infof("Adding watch on path: %q", cnsconfig.DefaultpvCSIProviderPath)
	err = watcher.Add(cnsconfig.DefaultpvCSIProviderPath)
	if err != nil {
		log.Errorf("failed to watch on path: %q. err=%v", cnsconfig.DefaultpvCSIProviderPath, err)
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

// ReloadConfiguration reloads configuration from the secret, and reset restClientConfig, supervisorClient
// and re-create vmOperatorClient using new config
func (c *controller) ReloadConfiguration() error {
	ctx, log := logger.GetNewContextWithLogger()
	log.Info("Reloading Configuration")
	cfg, err := common.GetConfig(ctx)
	if err != nil {
		log.Errorf("failed to read config. Error: %+v", err)
		return err
	}
	if cfg != nil {
		c.restClientConfig = k8s.GetRestClientConfigForSupervisor(ctx, cfg.GC.Endpoint, cfg.GC.Port)
		c.supervisorClient, err = k8s.NewSupervisorClient(ctx, c.restClientConfig)
		if err != nil {
			log.Errorf("failed to create supervisorClient. Error: %+v", err)
			return err
		}
		log.Infof("successfully re-created supervisorClient using updated configuration")
		c.vmOperatorClient, err = k8s.NewClientForGroup(ctx, c.restClientConfig, vmoperatortypes.GroupName)
		if err != nil {
			log.Errorf("failed to create vmOperatorClient. Error: %+v", err)
			return err
		}
		c.vmWatcher, err = k8s.NewVirtualMachineWatcher(ctx, c.restClientConfig, c.supervisorNamespace)
		if err != nil {
			log.Errorf("failed to create vmWatcher. Error: %+v", err)
			return err
		}
		log.Infof("successfully re-created vmOperatorClient using updated configuration")
		c.cnsOperatorClient, err = k8s.NewClientForGroup(ctx, c.restClientConfig, cnsoperatorv1alpha1.GroupName)
		if err != nil {
			log.Errorf("failed to create cnsOperatorClient. Error: %+v", err)
			return err
		}
	}
	return nil
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
		err := validateGuestClusterCreateVolumeRequest(ctx, req)
		if err != nil {
			msg := fmt.Sprintf("Validation for CreateVolume Request: %+v has failed. Error: %+v", *req, err)
			log.Error(msg)
			return nil, err
		}
		isFileVolumeRequest := common.IsFileVolumeRequest(ctx, req.GetVolumeCapabilities())
		if isFileVolumeRequest {
			volumeType = prometheus.PrometheusFileVolumeType
		} else {
			volumeType = prometheus.PrometheusBlockVolumeType
		}

		// Get PVC name and disk size for the supervisor cluster
		// We use default prefix 'pvc-' for pvc created in the guest cluster, it is mandatory.
		supervisorPVCName := c.tanzukubernetesClusterUID + "-" + req.Name[4:]

		// Volume Size - Default is 10 GiB
		volSizeBytes := int64(common.DefaultGbDiskSize * common.GbInBytes)
		if req.GetCapacityRange() != nil && req.GetCapacityRange().RequiredBytes != 0 {
			volSizeBytes = int64(req.GetCapacityRange().GetRequiredBytes())
		}
		volSizeMB := int64(common.RoundUpSize(volSizeBytes, common.MbInBytes))

		// Get supervisorStorageClass and accessMode
		var supervisorStorageClass string
		for param := range req.Parameters {
			paramName := strings.ToLower(param)
			if paramName == common.AttributeSupervisorStorageClass {
				supervisorStorageClass = req.Parameters[param]
			}
		}
		accessMode := req.GetVolumeCapabilities()[0].GetAccessMode().GetMode()
		pvc, err := c.supervisorClient.CoreV1().PersistentVolumeClaims(c.supervisorNamespace).Get(
			ctx, supervisorPVCName, metav1.GetOptions{})
		if err != nil {
			if errors.IsNotFound(err) {
				diskSize := strconv.FormatInt(volSizeMB, 10) + "Mi"
				claim := getPersistentVolumeClaimSpecWithStorageClass(supervisorPVCName,
					c.supervisorNamespace, diskSize, supervisorStorageClass, getAccessMode(accessMode))
				log.Debugf("PVC claim spec is %+v", spew.Sdump(claim))
				pvc, err = c.supervisorClient.CoreV1().PersistentVolumeClaims(c.supervisorNamespace).Create(
					ctx, claim, metav1.CreateOptions{})
				if err != nil {
					msg := fmt.Sprintf("failed to create pvc with name: %s on namespace: %s in supervisorCluster. Error: %+v",
						supervisorPVCName, c.supervisorNamespace, err)
					log.Error(msg)
					return nil, status.Errorf(codes.Internal, msg)
				}
			} else {
				msg := fmt.Sprintf("failed to get pvc with name: %s on namespace: %s from supervisorCluster. Error: %+v",
					supervisorPVCName, c.supervisorNamespace, err)
				log.Error(msg)
				return nil, status.Errorf(codes.Internal, msg)
			}
		}
		isBound, err := isPVCInSupervisorClusterBound(ctx, c.supervisorClient,
			pvc, time.Duration(getProvisionTimeoutInMin(ctx))*time.Minute)
		if !isBound {
			msg := fmt.Sprintf("failed to create volume on namespace: %s in supervisor cluster. Error: %+v",
				c.supervisorNamespace, err)
			log.Error(msg)
			eventList, err := c.supervisorClient.CoreV1().Events(c.supervisorNamespace).List(ctx,
				metav1.ListOptions{FieldSelector: "involvedObject.name=" + pvc.Name})
			if err != nil {
				log.Errorf("Unable to fetch events for pvc %q/%q from supervisor cluster with err: %+v",
					c.supervisorNamespace, pvc.Name, err)
				return nil, status.Errorf(codes.Internal, msg)
			}
			log.Errorf("Last observed events on the pvc %q/%q in supervisor cluster: %+v",
				c.supervisorNamespace, pvc.Name, spew.Sdump(eventList.Items))
			return nil, status.Errorf(codes.Internal, msg)
		}
		attributes := make(map[string]string)
		if commonco.ContainerOrchestratorUtility.IsFSSEnabled(ctx, common.FileVolume) && isFileVolumeRequest {
			attributes[common.AttributeDiskType] = common.DiskTypeFileVolume
		} else {
			attributes[common.AttributeDiskType] = common.DiskTypeBlockVolume
		}
		resp := &csi.CreateVolumeResponse{
			Volume: &csi.Volume{
				VolumeId:      supervisorPVCName,
				CapacityBytes: int64(volSizeMB * common.MbInBytes),
				VolumeContext: attributes,
			},
		}
		return resp, nil
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
		err = validateGuestClusterDeleteVolumeRequest(ctx, req)
		if err != nil {
			msg := fmt.Sprintf("Validation for Delete Volume Request: %+v has failed. Error: %+v", *req, err)
			log.Error(msg)
			return nil, err
		}
		// Retrieve Supervisor PVC
		svPVC, err := c.supervisorClient.CoreV1().PersistentVolumeClaims(c.supervisorNamespace).Get(
			ctx, req.VolumeId, metav1.GetOptions{})
		if err != nil {
			if errors.IsNotFound(err) {
				log.Debugf("PVC: %q not found in the Supervisor cluster. Assuming the volume is already deleted.",
					req.VolumeId)
				return &csi.DeleteVolumeResponse{}, nil
			}
			msg := fmt.Sprintf("failed to retrieve supervisor PVC %q in %q namespace. Error: %+v",
				req.VolumeId, c.supervisorNamespace, err)
			log.Error(msg)
			return nil, status.Error(codes.Internal, msg)
		}
		volumeType = prometheus.PrometheusBlockVolumeType
		for _, accessMode := range svPVC.Spec.AccessModes {
			if accessMode == corev1.ReadWriteMany || accessMode == corev1.ReadOnlyMany {
				volumeType = prometheus.PrometheusFileVolumeType
			}
		}
		err = c.supervisorClient.CoreV1().PersistentVolumeClaims(c.supervisorNamespace).Delete(
			ctx, req.VolumeId, *metav1.NewDeleteOptions(0))
		if err != nil {
			if errors.IsNotFound(err) {
				log.Debugf("PVC: %q not found in the Supervisor cluster. Assuming this volume to be deleted.", req.VolumeId)
				return &csi.DeleteVolumeResponse{}, nil
			}
			msg := fmt.Sprintf("DeleteVolume Request: %+v has failed. Error: %+v", *req, err)
			log.Error(msg)
			return nil, status.Errorf(codes.Internal, msg)
		}
		log.Infof("DeleteVolume: Volume deleted successfully. VolumeID: %q", req.VolumeId)
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
		// Check whether the request is for a block or file volume
		isFileVolumeRequest := common.IsFileVolumeRequest(ctx, []*csi.VolumeCapability{req.GetVolumeCapability()})

		err := validateGuestClusterControllerPublishVolumeRequest(ctx, req)
		if err != nil {
			msg := fmt.Sprintf("Validation for PublishVolume Request: %+v has failed. Error: %v", *req, err)
			log.Error(msg)
			return nil, status.Errorf(codes.Internal, msg)
		}

		// File volumes support
		if isFileVolumeRequest {
			volumeType = prometheus.PrometheusFileVolumeType
			// Check the feature state for file volume support
			if !commonco.ContainerOrchestratorUtility.IsFSSEnabled(ctx, common.FileVolume) {
				// Feature is disabled on the cluster
				return nil, status.Error(codes.InvalidArgument, "File volume not supported.")
			}
			return controllerPublishForFileVolume(ctx, req, c)
		}
		volumeType = prometheus.PrometheusBlockVolumeType
		// Block volumes support
		return controllerPublishForBlockVolume(ctx, req, c)
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

// controllerPublishForBlockVolume is a helper mthod for handling ControllerPublishVolume request for Block volumes
func controllerPublishForBlockVolume(ctx context.Context, req *csi.ControllerPublishVolumeRequest, c *controller) (
	*csi.ControllerPublishVolumeResponse, error) {
	log := logger.GetLogger(ctx)
	virtualMachine := &vmoperatortypes.VirtualMachine{}
	vmKey := types.NamespacedName{
		Namespace: c.supervisorNamespace,
		Name:      req.NodeId,
	}
	var err error
	if err = c.vmOperatorClient.Get(ctx, vmKey, virtualMachine); err != nil {
		msg := fmt.Sprintf("failed to get VirtualMachines for the node: %q. Error: %+v", req.NodeId, err)
		log.Error(msg)
		return nil, status.Errorf(codes.Internal, msg)
	}
	// Check if volume is already present in the virtualMachine.Spec.Volumes
	var isVolumePresentInSpec, isVolumeAttached bool
	var diskUUID string
	for _, volume := range virtualMachine.Spec.Volumes {
		if volume.PersistentVolumeClaim != nil && volume.Name == req.VolumeId {
			log.Infof("Volume %q is already present in the virtualMachine.Spec.Volumes", volume.Name)
			isVolumePresentInSpec = true
			break
		}
	}
	timeoutSeconds := int64(getAttacherTimeoutInMin(ctx) * 60)
	// if volume is present in the virtualMachine.Spec.Volumes check if volume's status is attached and DiskUuid is set
	if isVolumePresentInSpec {
		for _, volume := range virtualMachine.Status.Volumes {
			if volume.Name == req.VolumeId && volume.Attached && volume.DiskUuid != "" {
				diskUUID = volume.DiskUuid
				isVolumeAttached = true
				log.Infof("Volume %q is already attached in the virtualMachine.Spec.Volumes. Disk UUID: %q",
					volume.Name, volume.DiskUuid)
				break
			}
		}
	} else {
		timeout := time.Now().Add(time.Duration(timeoutSeconds) * time.Second)
		for {
			// Volume is not present in the virtualMachine.Spec.Volumes, so adding
			// volume in the spec and patching virtualMachine instance.
			vmvolumes := vmoperatortypes.VirtualMachineVolume{
				Name: req.VolumeId,
				PersistentVolumeClaim: &corev1.PersistentVolumeClaimVolumeSource{
					ClaimName: req.VolumeId,
				},
			}
			virtualMachineLock.Lock()
			virtualMachine.Spec.Volumes = append(virtualMachine.Spec.Volumes, vmvolumes)
			err := c.vmOperatorClient.Update(ctx, virtualMachine)
			virtualMachineLock.Unlock()
			if err == nil || time.Now().After(timeout) {
				break
			}
			if err := c.vmOperatorClient.Get(ctx, vmKey, virtualMachine); err != nil {
				msg := fmt.Sprintf("failed to get VirtualMachines for the node: %q. Error: %+v", req.NodeId, err)
				log.Error(msg)
				return nil, status.Errorf(codes.Internal, msg)
			}
			log.Debugf("Found virtualMachine instance for node: %q", req.NodeId)
		}
		if err != nil {
			msg := fmt.Sprintf("Time out to update VirtualMachines %q with Error: %+v", virtualMachine.Name, err)
			log.Error(msg)
			return nil, status.Errorf(codes.Internal, msg)
		}
	}

	// volume is not attached, so wait until volume is attached and DiskUuid is set
	if !isVolumeAttached {
		watchVirtualMachine, err := c.vmWatcher.Watch(metav1.ListOptions{
			FieldSelector:   fields.SelectorFromSet(fields.Set{"metadata.name": string(virtualMachine.Name)}).String(),
			ResourceVersion: virtualMachine.ResourceVersion,
			TimeoutSeconds:  &timeoutSeconds,
		})
		if err != nil {
			msg := fmt.Sprintf("failed to watch virtualMachine %q with Error: %v", virtualMachine.Name, err)
			log.Error(msg)
			return nil, status.Errorf(codes.Internal, msg)
		}
		defer watchVirtualMachine.Stop()

		// Watch all update events made on VirtualMachine instance until volume.DiskUuid is set
		for diskUUID == "" {
			// blocking wait for update event
			log.Debugf("waiting for update on virtualmachine: %q", virtualMachine.Name)
			event := <-watchVirtualMachine.ResultChan()
			vm, ok := event.Object.(*vmoperatortypes.VirtualMachine)
			if !ok {
				msg := fmt.Sprintf("Watch on virtualmachine %q timed out", virtualMachine.Name)
				log.Error(msg)
				return nil, status.Errorf(codes.Internal, msg)
			}
			if vm.Name != virtualMachine.Name {
				log.Debugf("Observed vm name: %q, expecting vm name: %q, volumeID: %q",
					vm.Name, virtualMachine.Name, req.VolumeId)
				continue
			}
			log.Debugf("observed update on virtualmachine: %q. checking if disk UUID is set for volume: %q ",
				virtualMachine.Name, req.VolumeId)
			for _, volume := range vm.Status.Volumes {
				if volume.Name == req.VolumeId {
					if volume.Attached && volume.DiskUuid != "" && volume.Error == "" {
						diskUUID = volume.DiskUuid
						log.Infof("observed disk UUID %q is set for the volume %q on virtualmachine %q",
							volume.DiskUuid, volume.Name, vm.Name)
					} else {
						if volume.Error != "" {
							msg := fmt.Sprintf("observed Error: %q is set on the volume %q on virtualmachine %q",
								volume.Error, volume.Name, vm.Name)
							log.Error(msg)
							return nil, status.Errorf(codes.Internal, msg)
						}
					}
					break
				}
			}
			if diskUUID == "" {
				log.Debugf("disk UUID is not set for volume: %q ", req.VolumeId)
			}
		}
		log.Debugf("disk UUID %v is set for the volume: %q ", diskUUID, req.VolumeId)
	}

	//return PublishContext with diskUUID of the volume attached to node.
	publishInfo := make(map[string]string)
	publishInfo[common.AttributeDiskType] = common.DiskTypeBlockVolume
	publishInfo[common.AttributeFirstClassDiskUUID] = common.FormatDiskUUID(diskUUID)
	resp := &csi.ControllerPublishVolumeResponse{
		PublishContext: publishInfo,
	}
	log.Infof("ControllerPublishVolume: Volume attached successfully %q", req.VolumeId)
	return resp, nil
}

// controllerPublishForFileVolume is a helper mthod for handling ControllerPublishVolume request for File volumes
func controllerPublishForFileVolume(ctx context.Context, req *csi.ControllerPublishVolumeRequest, c *controller) (
	*csi.ControllerPublishVolumeResponse, error) {
	log := logger.GetLogger(ctx)
	// Build the CnsFileAccessConfig instance name and namespace
	cnsFileAccessConfigInstance := &cnsfileaccessconfigv1alpha1.CnsFileAccessConfig{}
	cnsFileAccessConfigInstanceName := req.NodeId + "-" + req.VolumeId
	cnsFileAccessConfigInstanceKey := types.NamespacedName{
		Namespace: c.supervisorNamespace,
		Name:      cnsFileAccessConfigInstanceName,
	}
	// Check whether the CnsFileAccessConfig instance exist in the supervisor cluster
	if err := c.cnsOperatorClient.Get(ctx, cnsFileAccessConfigInstanceKey, cnsFileAccessConfigInstance); err != nil {
		if !errors.IsNotFound(err) {
			// Get() on the CnsFileAccessConfig instance failed with different error
			msg := fmt.Sprintf("failed to get CnsFileAccessConfig instance: %q/%q. Error: %+v",
				c.supervisorNamespace, cnsFileAccessConfigInstance.Name, err)
			log.Error(msg)
			return nil, status.Errorf(codes.Internal, msg)
		}
		// Create the CnsFileAccessConfig instance since it is not found
		cnsFileAccessConfigInstance = &cnsfileaccessconfigv1alpha1.CnsFileAccessConfig{
			ObjectMeta: metav1.ObjectMeta{
				Name:      cnsFileAccessConfigInstanceName,
				Namespace: c.supervisorNamespace},
			Spec: cnsfileaccessconfigv1alpha1.CnsFileAccessConfigSpec{
				VMName:  req.NodeId,
				PvcName: req.VolumeId,
			},
		}
		log.Debugf("Creating CnsFileAccessConfig instance: %+v", cnsFileAccessConfigInstance)
		log.Infof("Creating CnsFileAccessConfig instance with name: %q", cnsFileAccessConfigInstance.Name)
		if err := c.cnsOperatorClient.Create(ctx, cnsFileAccessConfigInstance); err != nil {
			msg := fmt.Sprintf("failed to create cnsFileAccessConfig: %q/%q. Error: %v",
				c.supervisorNamespace, cnsFileAccessConfigInstance.Name, err)
			log.Error(msg)
			return nil, status.Errorf(codes.Internal, msg)
		}
	}
	log.Debugf("Found CnsFileAccessConfig: %q/%q", c.supervisorNamespace, cnsFileAccessConfigInstance.Name)
	if cnsFileAccessConfigInstance.DeletionTimestamp != nil {
		// When deletionTimestamp is set, CnsOperator is in the process of
		// removing access for this IP. When that operation is successful, the
		// instance will be deleted. In a subsequent retry, a new instance will
		// be created.
		msg := fmt.Sprintf("cnsFileAccessConfigInstance %q/%q is getting deleted. "+
			"A new instance will be created in the subsequent ControllerPublishVolume request",
			c.supervisorNamespace, cnsFileAccessConfigInstance.Name)
		log.Error(msg)
		return nil, status.Errorf(codes.Internal, msg)
	}
	publishInfo := make(map[string]string)
	// Verify if the CnsFileAccessConfig instance has status with done set to true and error is empty
	if cnsFileAccessConfigInstance.Status.Done && cnsFileAccessConfigInstance.Status.Error == "" {
		for key, value := range cnsFileAccessConfigInstance.Status.AccessPoints {
			if key == common.Nfsv4AccessPointKey {
				publishInfo[common.Nfsv4AccessPoint] = value
				break
			}
		}
		publishInfo[common.AttributeDiskType] = common.DiskTypeFileVolume
		resp := &csi.ControllerPublishVolumeResponse{
			PublishContext: publishInfo,
		}
		log.Infof("ControllerPublishVolume: Volume %q attached successfully on the node: %q", req.VolumeId, req.NodeId)
		return resp, nil
	}
	cnsFileAccessConfigWatcher, err := k8s.NewCnsFileAccessConfigWatcher(ctx, c.restClientConfig, c.supervisorNamespace)
	if err != nil {
		msg := fmt.Sprintf("failed to create cnsFileAccessConfigWatcher. Error: %+v", err)
		log.Error(msg)
		return nil, status.Errorf(codes.Internal, msg)
	}
	// Attacher timeout, default is set to 4 minutes
	timeoutSeconds := int64(getAttacherTimeoutInMin(ctx) * 60)
	// Adding watch on the CnsFileAccessConfig instance to register for updates
	watchCnsFileAccessConfig, err := cnsFileAccessConfigWatcher.Watch(metav1.ListOptions{
		FieldSelector:   fields.SelectorFromSet(fields.Set{"metadata.name": cnsFileAccessConfigInstance.Name}).String(),
		ResourceVersion: cnsFileAccessConfigInstance.ResourceVersion,
		TimeoutSeconds:  &timeoutSeconds,
	})
	if err != nil {
		msg := fmt.Sprintf("failed to watch cnsfileaccessconfig %q with Error: %v", cnsFileAccessConfigInstance.Name, err)
		log.Error(msg)
		return nil, status.Errorf(codes.Internal, msg)
	}
	defer watchCnsFileAccessConfig.Stop()
	var cnsFileAccessConfigInstanceErr string
	// Watch all update events made on CnsFileAccessConfig instance until accessPoints is set
	for {
		log.Debugf("Waiting for update on cnsfileaccessconfigs: %q", cnsFileAccessConfigInstance.Name)
		event := <-watchCnsFileAccessConfig.ResultChan()
		cnsfileaccessconfig, ok := event.Object.(*cnsfileaccessconfigv1alpha1.CnsFileAccessConfig)
		if !ok {
			msg := fmt.Sprintf("Watch on cnsfileaccessconfig instance %q timed out. Last seen error on the instance=%q",
				cnsFileAccessConfigInstance.Name, cnsFileAccessConfigInstanceErr)
			log.Error(msg)
			return nil, status.Errorf(codes.Internal, msg)
		}
		if cnsfileaccessconfig.Name != cnsFileAccessConfigInstanceName {
			log.Debugf("Observed cnsFileAccessConfig instance name: %q, expecting cnsFileAccessConfig instance name: %q",
				cnsfileaccessconfig.Name, cnsFileAccessConfigInstanceName)
			continue
		}
		// Check if SV PVC Name match with VolumeId from the request
		if cnsfileaccessconfig.Spec.PvcName != req.VolumeId {
			log.Debugf("Observed SV PVC Name: %q, expecting SV PVC Name: %q",
				cnsfileaccessconfig.Spec.PvcName, req.VolumeId)
			continue
		}
		// Check if VM name in the cnsfileaccessconfig instance match with NodeId from the request
		if cnsfileaccessconfig.Spec.VMName != req.NodeId {
			log.Debugf("Observed vm name: %q, expecting vm name: %q", cnsfileaccessconfig.Spec.VMName, req.NodeId)
			continue
		}
		log.Debugf("Observed an update on cnsfileaccessconfig: %+v", cnsfileaccessconfig)
		if cnsfileaccessconfig.Status.Done && cnsfileaccessconfig.Status.Error == "" &&
			cnsfileaccessconfig.DeletionTimestamp == nil {
			// Check if the updated instance has the AccessPoints
			for key, value := range cnsfileaccessconfig.Status.AccessPoints {
				if key == common.Nfsv4AccessPointKey {
					publishInfo[common.AttributeDiskType] = common.DiskTypeFileVolume
					publishInfo[common.Nfsv4AccessPoint] = value
					break
				}
			}
			if _, ok := publishInfo[common.Nfsv4AccessPoint]; ok {
				log.Debugf("Found Nfsv4AccessPoint in publishInfo. publishInfo=%+v", publishInfo)
				break
			}
		}
		cnsFileAccessConfigInstanceErr = cnsfileaccessconfig.Status.Error
	}
	resp := &csi.ControllerPublishVolumeResponse{
		PublishContext: publishInfo,
	}
	log.Infof("ControllerPublishVolume: Volume %q attached successfully on the node: %q", req.VolumeId, req.NodeId)
	return resp, nil
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
		err := validateGuestClusterControllerUnpublishVolumeRequest(ctx, req)
		if err != nil {
			msg := fmt.Sprintf("Validation for UnpublishVolume Request: %+v has failed. Error: %v", *req, err)
			log.Error(msg)
			return nil, err
		}

		// Retrieve Supervisor PVC
		svPVC, err := c.supervisorClient.CoreV1().PersistentVolumeClaims(c.supervisorNamespace).Get(
			ctx, req.VolumeId, metav1.GetOptions{})
		if err != nil {
			msg := fmt.Sprintf("failed to retrieve supervisor PVC %q in %q namespace. Error: %+v",
				req.VolumeId, c.supervisorNamespace, err)
			log.Error(msg)
			return nil, status.Error(codes.Internal, msg)
		}
		var isFileVolume bool
		for _, accessMode := range svPVC.Spec.AccessModes {
			if accessMode == corev1.ReadWriteMany || accessMode == corev1.ReadOnlyMany {
				isFileVolume = true
			}
		}
		if isFileVolume {
			volumeType = prometheus.PrometheusFileVolumeType
			if commonco.ContainerOrchestratorUtility.IsFSSEnabled(ctx, common.FileVolume) {
				return controllerUnpublishForFileVolume(ctx, req, c)
			}
			// Feature is disabled on the cluster
			return nil, status.Error(codes.InvalidArgument, "File volume not supported.")
		}
		volumeType = prometheus.PrometheusBlockVolumeType
		return controllerUnpublishForBlockVolume(ctx, req, c)
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

// controllerUnpublishForBlockVolume is helper method to handle ControllerPublishVolume for Block volumes
func controllerUnpublishForBlockVolume(ctx context.Context, req *csi.ControllerUnpublishVolumeRequest, c *controller) (
	*csi.ControllerUnpublishVolumeResponse, error) {

	log := logger.GetLogger(ctx)

	// TODO: Investigate if a race condition can exist here between multiple detach calls to the same volume.
	// 	If yes, implement some locking mechanism
	virtualMachine := &vmoperatortypes.VirtualMachine{}
	vmKey := types.NamespacedName{
		Namespace: c.supervisorNamespace,
		Name:      req.NodeId,
	}
	var err error
	if err := c.vmOperatorClient.Get(ctx, vmKey, virtualMachine); err != nil {
		if errors.IsNotFound(err) {
			log.Infof("VirtualMachine %s/%s not found. Assuming volume %s was detached.",
				c.supervisorNamespace, req.NodeId, req.VolumeId)
			return &csi.ControllerUnpublishVolumeResponse{}, nil
		}
		msg := fmt.Sprintf("failed to get VirtualMachines for node: %q. Error: %+v", req.NodeId, err)
		log.Error(msg)
		return nil, status.Errorf(codes.Internal, msg)
	}
	log.Debugf("Found VirtualMachine for node: %q.", req.NodeId)
	timeoutSeconds := int64(getAttacherTimeoutInMin(ctx) * 60)
	timeout := time.Now().Add(time.Duration(timeoutSeconds) * time.Second)
	for {
		for index, volume := range virtualMachine.Spec.Volumes {
			if volume.Name == req.VolumeId {
				log.Debugf("Removing volume %q from VirtualMachine %q", volume.Name, virtualMachine.Name)
				virtualMachineLock.Lock()
				virtualMachine.Spec.Volumes = append(virtualMachine.Spec.Volumes[:index],
					virtualMachine.Spec.Volumes[index+1:]...)
				err = c.vmOperatorClient.Update(ctx, virtualMachine)
				virtualMachineLock.Unlock()
				break
			}
		}
		if err == nil || time.Now().After(timeout) {
			break
		}
		if err := c.vmOperatorClient.Get(ctx, vmKey, virtualMachine); err != nil {
			if errors.IsNotFound(err) {
				log.Infof("VirtualMachine %s/%s not found. Assuming volume %s was detached.",
					c.supervisorNamespace, req.NodeId, req.VolumeId)
				return &csi.ControllerUnpublishVolumeResponse{}, nil
			}
			msg := fmt.Sprintf("failed to get VirtualMachines for node: %q. Error: %+v", req.NodeId, err)
			log.Error(msg)
			return nil, status.Errorf(codes.Internal, msg)
		}
		log.Debugf("Found VirtualMachine for node: %q.", req.NodeId)
	}
	if err != nil {
		msg := fmt.Sprintf("Time out to update VirtualMachines %q with Error: %+v", virtualMachine.Name, err)
		log.Error(msg)
		return nil, status.Errorf(codes.Internal, msg)
	}

	// Watch virtual machine object and wait for volume name to be removed from the status field.
	watchVirtualMachine, err := c.vmWatcher.Watch(metav1.ListOptions{
		FieldSelector:   fields.SelectorFromSet(fields.Set{"metadata.name": string(virtualMachine.Name)}).String(),
		ResourceVersion: virtualMachine.ResourceVersion,
		TimeoutSeconds:  &timeoutSeconds,
	})
	if err != nil {
		msg := fmt.Sprintf("failed to watch VirtualMachine %q with Error: %v", virtualMachine.Name, err)
		log.Error(msg)
		return nil, status.Errorf(codes.Internal, msg)
	}
	if watchVirtualMachine == nil {
		msg := fmt.Sprintf("watchVirtualMachine for %q is nil", virtualMachine.Name)
		log.Error(msg)
		return nil, status.Errorf(codes.Internal, msg)

	}
	defer watchVirtualMachine.Stop()

	// Loop until the volume is removed from virtualmachine status
	isVolumeDetached := false
	for !isVolumeDetached {
		log.Debugf("Waiting for update on VirtualMachine: %q", virtualMachine.Name)
		// Block on update events
		event := <-watchVirtualMachine.ResultChan()
		vm, ok := event.Object.(*vmoperatortypes.VirtualMachine)
		if !ok {
			msg := fmt.Sprintf("Watch on virtualmachine %q timed out", virtualMachine.Name)
			log.Error(msg)
			return nil, status.Errorf(codes.Internal, msg)
		}
		if vm.Name != virtualMachine.Name {
			log.Debugf("Observed vm name: %q, expecting vm name: %q, volumeID: %q",
				vm.Name, virtualMachine.Name, req.VolumeId)
			continue
		}
		switch event.Type {
		case watch.Added, watch.Modified:
			isVolumeDetached = true
			for _, volume := range vm.Status.Volumes {
				if volume.Name == req.VolumeId {
					log.Debugf("Volume %q still exists in VirtualMachine %q status", volume.Name, virtualMachine.Name)
					isVolumeDetached = false
					if volume.Attached && volume.Error != "" {
						msg := fmt.Sprintf("failed to detach volume %q from VirtualMachine %q with Error: %v",
							volume.Name, virtualMachine.Name, volume.Error)
						log.Error(msg)
						return nil, status.Errorf(codes.Internal, msg)
					}
					break
				}
			}
		case watch.Deleted:
			log.Infof("VirtualMachine %s/%s deleted. Assuming volume %s was detached.",
				c.supervisorNamespace, req.NodeId, req.VolumeId)
			isVolumeDetached = true
		}
	}
	log.Infof("ControllerUnpublishVolume: Volume detached successfully %q", req.VolumeId)
	return &csi.ControllerUnpublishVolumeResponse{}, nil
}

// controllerUnpublishForFileVolume is helper method to handle ControllerPublishVolume for File volumes
func controllerUnpublishForFileVolume(ctx context.Context, req *csi.ControllerUnpublishVolumeRequest, c *controller) (
	*csi.ControllerUnpublishVolumeResponse, error) {
	log := logger.GetLogger(ctx)
	// Adding watch on the CnsFileAccessConfig instance to register for updates
	cnsFileAccessConfigWatcher, err := k8s.NewCnsFileAccessConfigWatcher(ctx, c.restClientConfig, c.supervisorNamespace)
	if err != nil {
		msg := fmt.Sprintf("failed to create cnsFileAccessConfigWatcher. Error: %+v", err)
		log.Error(msg)
		return nil, status.Errorf(codes.Internal, msg)
	}
	cnsFileAccessConfigInstance := &cnsfileaccessconfigv1alpha1.CnsFileAccessConfig{}
	cnsFileAccessConfigInstanceName := req.NodeId + "-" + req.VolumeId
	cnsFileAccessConfigInstanceKey := types.NamespacedName{
		Namespace: c.supervisorNamespace,
		Name:      cnsFileAccessConfigInstanceName,
	}
	if err := c.cnsOperatorClient.Get(ctx, cnsFileAccessConfigInstanceKey, cnsFileAccessConfigInstance); err != nil {
		if errors.IsNotFound(err) {
			log.Infof("ControllerUnpublishVolume: CnsFileAccessConfig instance %q/%q not found in supervisor cluster. "+
				"Returning success for the detach operation", c.supervisorNamespace, cnsFileAccessConfigInstanceName)
			return &csi.ControllerUnpublishVolumeResponse{}, nil
		}
		msg := fmt.Sprintf("failed to get CnsFileAccessConfig instance: %q/%q. Error: %+v",
			c.supervisorNamespace, cnsFileAccessConfigInstanceName, err)
		log.Error(msg)
		return nil, status.Errorf(codes.Internal, msg)
	}
	// Attach/Detach timeout, default is set to 4 minutes
	timeoutSeconds := int64(getAttacherTimeoutInMin(ctx) * 60)
	watchCnsFileAccessConfig, err := cnsFileAccessConfigWatcher.Watch(metav1.ListOptions{
		FieldSelector:   fields.SelectorFromSet(fields.Set{"metadata.name": cnsFileAccessConfigInstanceName}).String(),
		ResourceVersion: cnsFileAccessConfigInstance.ResourceVersion,
		TimeoutSeconds:  &timeoutSeconds,
	})
	if err != nil {
		msg := fmt.Sprintf("failed to watch cnsFileAccessConfig instance %q/%q with Error: %v",
			c.supervisorNamespace, cnsFileAccessConfigInstanceName, err)
		log.Error(msg)
		return nil, status.Errorf(codes.Internal, msg)
	}
	if err := c.cnsOperatorClient.Delete(ctx, &cnsfileaccessconfigv1alpha1.CnsFileAccessConfig{
		ObjectMeta: metav1.ObjectMeta{
			Name:      cnsFileAccessConfigInstanceName,
			Namespace: c.supervisorNamespace,
		},
	}); err != nil {
		if errors.IsNotFound(err) {
			log.Infof("ControllerUnpublishVolume: CnsFileAccessConfig instance %q/%q already deleted. "+
				"Returning success for the detach operation", c.supervisorNamespace, cnsFileAccessConfigInstanceName)
			return &csi.ControllerUnpublishVolumeResponse{}, nil
		}
		msg := fmt.Sprintf("failed to delete CnsFileAccessConfig instance: %q/%q. Error: %+v",
			c.supervisorNamespace, cnsFileAccessConfigInstanceName, err)
		log.Error(msg)
		return nil, status.Errorf(codes.Internal, msg)
	}
	defer watchCnsFileAccessConfig.Stop()
	var cnsFileAccessConfigInstanceErr string
	isCnsFileAccessConfigInstanceDeleted := false
	// Watch all update events made on CnsFileAccessConfig instance until Deleted
	// event or a timeout occurs on the cnsfileaccessconfig instance.
	for !isCnsFileAccessConfigInstanceDeleted {
		log.Debugf("waiting for update on cnsfileaccessconfigs: %q", cnsFileAccessConfigInstanceName)
		event := <-watchCnsFileAccessConfig.ResultChan()
		cnsfileaccessconfig, ok := event.Object.(*cnsfileaccessconfigv1alpha1.CnsFileAccessConfig)
		if !ok {
			msg := fmt.Sprintf("Watch on cnsfileaccessconfig instance %q/%q timed out. Last seen error on the instance=%q",
				c.supervisorNamespace, cnsFileAccessConfigInstanceName, cnsFileAccessConfigInstanceErr)
			log.Error(msg)
			return nil, status.Errorf(codes.Internal, msg)
		}
		if cnsfileaccessconfig.Name != cnsFileAccessConfigInstanceName {
			log.Debugf("Observed CnsFileAccessConfig instance name: %q, expecting CnsFileAccessConfig instance name: %q",
				cnsfileaccessconfig.Name, cnsFileAccessConfigInstanceName)
			continue
		}
		// Check if SV PVC Name ain the cnsfileaccessconfig instance match with VolumeId from the request
		if cnsfileaccessconfig.Spec.PvcName != req.VolumeId {
			log.Debugf("Observed SV PVC Name: %q, expecting SV PVC Name: %q",
				cnsfileaccessconfig.Spec.PvcName, req.VolumeId)
			continue
		}
		// Check if VM name in the cnsfileaccessconfig instance match with NodeId from the request
		if cnsfileaccessconfig.Spec.VMName != req.NodeId {
			log.Debugf("Observed vm name: %q, expecting vm name: %q", cnsfileaccessconfig.Spec.VMName, req.NodeId)
			continue
		}
		if event.Type == "DELETED" {
			isCnsFileAccessConfigInstanceDeleted = true
		}
		cnsFileAccessConfigInstanceErr = cnsfileaccessconfig.Status.Error
	}
	log.Infof("ControllerUnpublishVolume: Volume detached successfully %q", req.VolumeId)
	return &csi.ControllerUnpublishVolumeResponse{}, nil
}

// ControllerExpandVolume expands a volume.
// volume id and size is retrieved from ControllerExpandVolumeRequest
func (c *controller) ControllerExpandVolume(ctx context.Context, req *csi.ControllerExpandVolumeRequest) (
	*csi.ControllerExpandVolumeResponse, error) {
	start := time.Now()
	volumeType := prometheus.PrometheusUnknownVolumeType

	controllerExpandVolumeInternal := func() (
		*csi.ControllerExpandVolumeResponse, error) {
		ctx = logger.NewContextWithLogger(ctx)
		log := logger.GetLogger(ctx)
		if !commonco.ContainerOrchestratorUtility.IsFSSEnabled(ctx, common.VolumeExtend) {
			msg := "ExpandVolume feature is disabled on the cluster."
			log.Warn(msg)
			return nil, status.Error(codes.Unimplemented, msg)
		}
		log.Infof("ControllerExpandVolume: called with args %+v", *req)

		err := validateGuestClusterControllerExpandVolumeRequest(ctx, req)
		if err != nil {
			return nil, err
		}
		// Only block volume expand is allowed. Update this when file volume expand is also supported.
		volumeType = prometheus.PrometheusBlockVolumeType

		volumeID := req.GetVolumeId()
		volSizeBytes := int64(req.GetCapacityRange().GetRequiredBytes())

		if !commonco.ContainerOrchestratorUtility.IsFSSEnabled(ctx, common.OnlineVolumeExtend) {
			vmList := &vmoperatortypes.VirtualMachineList{}
			err = c.vmOperatorClient.List(ctx, vmList, client.InNamespace(c.supervisorNamespace))
			if err != nil {
				msg := fmt.Sprintf("failed to list virtualmachines with error: %+v", err)
				log.Error(msg)
				return nil, status.Error(codes.Internal, msg)
			}

			for _, vmInstance := range vmList.Items {
				for _, vmVolume := range vmInstance.Status.Volumes {
					if vmVolume.Name == volumeID && vmVolume.Attached {
						msg := fmt.Sprintf("failed to expand volume: %q. Volume is attached to pod. "+
							"Only offline volume expansion is supported", volumeID)
						log.Error(msg)
						return nil, status.Error(codes.FailedPrecondition, msg)
					}
				}
			}
		}

		// Retrieve Supervisor PVC
		svPVC, err := c.supervisorClient.CoreV1().PersistentVolumeClaims(c.supervisorNamespace).Get(
			ctx, volumeID, metav1.GetOptions{})
		if err != nil {
			msg := fmt.Sprintf("failed to retrieve supervisor PVC %q in %q namespace. Error: %+v",
				volumeID, c.supervisorNamespace, err)
			log.Error(msg)
			return nil, status.Error(codes.Internal, msg)
		}

		waitForSvPvcCondition := true
		gcPvcRequestSize := resource.NewQuantity(volSizeBytes, resource.Format(resource.BinarySI))
		svPvcRequestSize := svPVC.Spec.Resources.Requests[corev1.ResourceName(corev1.ResourceStorage)]
		// Check if GC PVC request size is greater than SV PVC request size
		switch (gcPvcRequestSize).Cmp(svPvcRequestSize) {
		case 1:
			// Update requested storage in SV PVC spec
			svPvcClone := svPVC.DeepCopy()
			svPvcClone.Spec.Resources.Requests[corev1.ResourceName(corev1.ResourceStorage)] = *gcPvcRequestSize

			// Make an update call to SV API server
			log.Infof("Increasing the size of supervisor PVC %s in namespace %s to %s",
				volumeID, c.supervisorNamespace, gcPvcRequestSize.String())
			svPVC, err = c.supervisorClient.CoreV1().PersistentVolumeClaims(c.supervisorNamespace).Update(
				ctx, svPvcClone, metav1.UpdateOptions{})
			if err != nil {
				msg := fmt.Sprintf("failed to update supervisor PVC %q in %q namespace. Error: %+v",
					volumeID, c.supervisorNamespace, err)
				log.Error(msg)
				return nil, status.Error(codes.Internal, msg)
			}
		case 0:
			// GC PVC request size is equal to SV PVC request size
			log.Infof("Skipping resize call for supervisor PVC %s in namespace %s as it is already at the requested size",
				volumeID, c.supervisorNamespace)

			// SV PVC is already in FileSystemResizePending condition indicates
			// that SV PV has already been expanded to required size.
			if checkPVCCondition(ctx, svPVC, corev1.PersistentVolumeClaimFileSystemResizePending) {
				waitForSvPvcCondition = false
			} else {
				// SV PVC is not in FileSystemResizePending condition and GC PVC request size is equal to SV PVC capacity
				// indicates that SV PVC is already at required size
				if (gcPvcRequestSize).Cmp(svPVC.Status.Capacity[corev1.ResourceName(corev1.ResourceStorage)]) == 0 {
					waitForSvPvcCondition = false
				}
			}
		default:
			// GC PVC request size is lesser than SV PVC request size
			msg := fmt.Sprintf("the requested size of the Supervisor PVC %s in namespace %s is %s "+
				"which is greater than the requested size of %s",
				volumeID, c.supervisorNamespace, svPvcRequestSize.String(), gcPvcRequestSize.String())
			log.Error(msg)
			return nil, status.Error(codes.InvalidArgument, msg)
		}

		if waitForSvPvcCondition {
			// Wait for Supervisor PVC to change status to FilesystemResizePending
			err = checkForSupervisorPVCCondition(ctx, c.supervisorClient, svPVC,
				corev1.PersistentVolumeClaimFileSystemResizePending, time.Duration(getResizeTimeoutInMin(ctx))*time.Minute)
			if err != nil {
				msg := fmt.Sprintf("failed to expand volume %s in namespace %s of supervisor cluster. Error: %+v",
					volumeID, c.supervisorNamespace, err)
				log.Error(msg)
				return nil, status.Error(codes.Internal, msg)
			}
		}

		nodeExpansionRequired := true
		// Set NodeExpansionRequired to false for raw block volumes
		if _, ok := req.GetVolumeCapability().GetAccessType().(*csi.VolumeCapability_Block); ok {
			log.Infof("Node Expansion not supported for raw block volume ID %q in namespace %s of supervisor",
				volumeID, c.supervisorNamespace)
			nodeExpansionRequired = false
		}
		resp := &csi.ControllerExpandVolumeResponse{
			CapacityBytes:         volSizeBytes,
			NodeExpansionRequired: nodeExpansionRequired,
		}
		return resp, nil
	}
	resp, err := controllerExpandVolumeInternal()
	if err != nil {
		prometheus.CsiControlOpsHistVec.WithLabelValues(volumeType, prometheus.PrometheusExpandVolumeOpType,
			prometheus.PrometheusFailStatus).Observe(time.Since(start).Seconds())
	} else {
		prometheus.CsiControlOpsHistVec.WithLabelValues(volumeType, prometheus.PrometheusExpandVolumeOpType,
			prometheus.PrometheusPassStatus).Observe(time.Since(start).Seconds())
	}
	return resp, err
}

// ValidateVolumeCapabilities returns the capabilities of the volume.
func (c *controller) ValidateVolumeCapabilities(ctx context.Context, req *csi.ValidateVolumeCapabilitiesRequest) (
	*csi.ValidateVolumeCapabilitiesResponse, error) {

	log := logger.GetLogger(ctx)
	log.Infof("ValidateVolumeCapabilities: called with args %+v", *req)
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

func (c *controller) ControllerGetVolume(ctx context.Context, req *csi.ControllerGetVolumeRequest) (
	*csi.ControllerGetVolumeResponse, error) {
	return nil, status.Error(codes.Unimplemented, "")
}
