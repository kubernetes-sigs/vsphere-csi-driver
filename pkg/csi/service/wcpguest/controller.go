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
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/container-storage-interface/spec/lib/go/csi"
	"github.com/davecgh/go-spew/spew"
	"github.com/fsnotify/fsnotify"
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
	clientset "k8s.io/client-go/kubernetes"
	"k8s.io/client-go/tools/cache"
	"sigs.k8s.io/controller-runtime/pkg/client"
	cnsconfig "sigs.k8s.io/vsphere-csi-driver/pkg/common/config"
	"sigs.k8s.io/vsphere-csi-driver/pkg/csi/service/common"
	"sigs.k8s.io/vsphere-csi-driver/pkg/csi/service/logger"
	csitypes "sigs.k8s.io/vsphere-csi-driver/pkg/csi/types"
	k8s "sigs.k8s.io/vsphere-csi-driver/pkg/kubernetes"
)

var (
	// controllerCaps represents the capability of controller service
	controllerCaps = []csi.ControllerServiceCapability_RPC_Type{
		csi.ControllerServiceCapability_RPC_CREATE_DELETE_VOLUME,
		csi.ControllerServiceCapability_RPC_PUBLISH_UNPUBLISH_VOLUME,
		csi.ControllerServiceCapability_RPC_EXPAND_VOLUME,
	}
)

type controller struct {
	supervisorClient          clientset.Interface
	vmOperatorClient          client.Client
	vmWatcher                 *cache.ListWatch
	supervisorNamespace       string
	tanzukubernetesClusterUID string
	featureStates             *cnsconfig.FeatureStateSwitches
}

// New creates a CNS controller
func New() csitypes.CnsController {
	return &controller{}
}

// Init is initializing controller struct
func (c *controller) Init(config *cnsconfig.Config) error {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	ctx = logger.NewContextWithLogger(ctx)
	log := logger.GetLogger(ctx)

	log.Infof("Initializing WCPGC CSI controller")
	var err error
	// connect to the CSI controller in supervisor cluster
	c.supervisorNamespace, err = cnsconfig.GetSupervisorNamespace(ctx)
	if err != nil {
		return err
	}
	c.tanzukubernetesClusterUID = config.GC.TanzuKubernetesClusterUID
	restClientConfig := k8s.GetRestClientConfig(ctx, config.GC.Endpoint, config.GC.Port)
	c.supervisorClient, err = k8s.NewSupervisorClient(ctx, restClientConfig)
	if err != nil {
		log.Errorf("failed to create supervisorClient. Error: %+v", err)
		return err
	}
	c.vmOperatorClient, err = k8s.NewClientForGroup(ctx, restClientConfig, vmoperatortypes.GroupName)
	if err != nil {
		log.Errorf("failed to create vmOperatorClient. Error: %+v", err)
		return err
	}
	c.vmWatcher, err = k8s.NewVirtualMachineWatcher(ctx, restClientConfig, c.supervisorNamespace)
	if err != nil {
		log.Errorf("failed to create vmWatcher. Error: %+v", err)
		return err
	}

	c.featureStates = &config.FeatureStates
	pvcsiConfigPath := common.GetConfigPath(ctx)
	featureStatesCfgPath := common.GetFeatureStatesConfigPath(ctx)
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
					c.ReloadConfiguration()
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
	featureStatesCfgDirPath := filepath.Dir(featureStatesCfgPath)
	log.Infof("Adding watch on path: %q", featureStatesCfgDirPath)
	err = watcher.Add(featureStatesCfgDirPath)
	if err != nil {
		log.Errorf("Failed to watch on path: %q. err=%v", featureStatesCfgDirPath, err)
		return err
	}
	return nil
}

// ReloadConfiguration reloads configuration from the secret, and reset restClientConfig, supervisorClient
// and re-create vmOperatorClient using new config
func (c *controller) ReloadConfiguration() {
	ctx, log := logger.GetNewContextWithLogger()
	log.Info("Reloading Configuration")
	cfg, err := common.GetConfig(ctx)
	if err != nil {
		log.Errorf("failed to read config. Error: %+v", err)
		return
	}
	if cfg != nil {
		restClientConfig := k8s.GetRestClientConfig(ctx, cfg.GC.Endpoint, cfg.GC.Port)
		c.supervisorClient, err = k8s.NewSupervisorClient(ctx, restClientConfig)
		if err != nil {
			log.Errorf("failed to create supervisorClient. Error: %+v", err)
			return
		}
		log.Infof("successfully re-created supervisorClient using updated configuration")
		c.vmOperatorClient, err = k8s.NewClientForGroup(ctx, restClientConfig, vmoperatortypes.GroupName)
		if err != nil {
			log.Errorf("failed to create vmOperatorClient. Error: %+v", err)
			return
		}
		c.vmWatcher, err = k8s.NewVirtualMachineWatcher(ctx, restClientConfig, c.supervisorNamespace)
		if err != nil {
			log.Errorf("failed to create vmWatcher. Error: %+v", err)
			return
		}
		log.Infof("successfully re-created vmOperatorClient using updated configuration")
	}
}

// CreateVolume is creating CNS Volume using volume request specified
// in CreateVolumeRequest
func (c *controller) CreateVolume(ctx context.Context, req *csi.CreateVolumeRequest) (
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
	pvc, err := c.supervisorClient.CoreV1().PersistentVolumeClaims(c.supervisorNamespace).Get(supervisorPVCName, metav1.GetOptions{})
	if err != nil {
		if errors.IsNotFound(err) {
			diskSize := strconv.FormatInt(volSizeMB, 10) + "Mi"
			claim := getPersistentVolumeClaimSpecWithStorageClass(supervisorPVCName, c.supervisorNamespace, diskSize, supervisorStorageClass, getAccessMode(accessMode))
			log.Debugf("PVC claim spec is %+v", spew.Sdump(claim))
			pvc, err = c.supervisorClient.CoreV1().PersistentVolumeClaims(c.supervisorNamespace).Create(claim)
			if err != nil {
				msg := fmt.Sprintf("failed to create pvc with name: %s on namespace: %s in supervisorCluster. Error: %+v", supervisorPVCName, c.supervisorNamespace, err)
				log.Error(msg)
				return nil, status.Errorf(codes.Internal, msg)
			}
		} else {
			msg := fmt.Sprintf("failed to get pvc with name: %s on namespace: %s from supervisorCluster. Error: %+v", supervisorPVCName, c.supervisorNamespace, err)
			log.Error(msg)
			return nil, status.Errorf(codes.Internal, msg)
		}
	}
	isBound, err := isPVCInSupervisorClusterBound(ctx, c.supervisorClient, pvc, time.Duration(getProvisionTimeoutInMin(ctx))*time.Minute)
	if !isBound {
		msg := fmt.Sprintf("failed to create volume on namespace: %s  in supervisor cluster. Error: %+v", c.supervisorNamespace, err)
		log.Error(msg)
		return nil, status.Errorf(codes.Internal, msg)
	}
	attributes := make(map[string]string)
	attributes[common.AttributeDiskType] = common.DiskTypeBlockVolume
	resp := &csi.CreateVolumeResponse{
		Volume: &csi.Volume{
			VolumeId:      supervisorPVCName,
			CapacityBytes: int64(volSizeMB * common.MbInBytes),
			VolumeContext: attributes,
		},
	}
	return resp, nil
}

// DeleteVolume is deleting CNS Volume specified in DeleteVolumeRequest
func (c *controller) DeleteVolume(ctx context.Context, req *csi.DeleteVolumeRequest) (
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
	err = c.supervisorClient.CoreV1().PersistentVolumeClaims(c.supervisorNamespace).Delete(req.VolumeId, nil)
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

// ControllerPublishVolume attaches a volume to the Node VM.
// volume id and node name is retrieved from ControllerPublishVolumeRequest
func (c *controller) ControllerPublishVolume(ctx context.Context, req *csi.ControllerPublishVolumeRequest) (
	*csi.ControllerPublishVolumeResponse, error) {

	ctx = logger.NewContextWithLogger(ctx)
	log := logger.GetLogger(ctx)
	log.Infof("ControllerPublishVolume: called with args %+v", *req)
	err := validateGuestClusterControllerPublishVolumeRequest(ctx, req)
	if err != nil {
		msg := fmt.Sprintf("Validation for PublishVolume Request: %+v has failed. Error: %v", *req, err)
		log.Error(msg)
		return nil, status.Errorf(codes.Internal, msg)
	}

	virtualMachine := &vmoperatortypes.VirtualMachine{}
	vmKey := types.NamespacedName{
		Namespace: c.supervisorNamespace,
		Name:      req.NodeId,
	}
	if err := c.vmOperatorClient.Get(ctx, vmKey, virtualMachine); err != nil {
		msg := fmt.Sprintf("failed to get VirtualMachines for the node: %q. Error: %+v", req.NodeId, err)
		log.Error(msg)
		return nil, status.Errorf(codes.Internal, msg)
	}
	log.Debugf("Found virtualMachine instance for node: %q", req.NodeId)

	// Check if volume is already present in the virtualMachine.Spec.Volumes
	var isVolumePresentInSpec, isVolumeAttached bool
	var diskUUID string
	for _, volume := range virtualMachine.Spec.Volumes {
		if volume.PersistentVolumeClaim != nil && volume.PersistentVolumeClaim.ClaimName == req.VolumeId {
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
				log.Infof("Volume %v is already attached in the virtualMachine.Spec.Volumes. Disk UUID: %q", volume.Name, volume.DiskUuid)
				break
			}
		}
	} else {
		// volume is not present in the virtualMachine.Spec.Volumes, so adding volume in the spec and patching virtualMachine instance
		vmvolumes := vmoperatortypes.VirtualMachineVolume{
			Name: req.VolumeId,
			PersistentVolumeClaim: &corev1.PersistentVolumeClaimVolumeSource{
				ClaimName: req.VolumeId,
			},
		}
		timeout := time.Now().Add(time.Duration(timeoutSeconds) * time.Second)
		for {
			virtualMachine.Spec.Volumes = append(virtualMachine.Spec.Volumes, vmvolumes)
			err = c.vmOperatorClient.Update(ctx, virtualMachine)
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
			log.Debugf(fmt.Sprintf("waiting for update on virtualmachine: %q", virtualMachine.Name))
			event := <-watchVirtualMachine.ResultChan()
			vm, ok := event.Object.(*vmoperatortypes.VirtualMachine)
			if !ok {
				msg := fmt.Sprintf("Watch on virtualmachine %q timed out", virtualMachine.Name)
				log.Error(msg)
				return nil, status.Errorf(codes.Internal, msg)
			}
			log.Debugf(fmt.Sprintf("observed update on virtualmachine: %q. checking if disk UUID is set for volume: %q ", virtualMachine.Name, req.VolumeId))
			for _, volume := range vm.Status.Volumes {
				if volume.Name == req.VolumeId {
					if volume.Attached && volume.DiskUuid != "" && volume.Error == "" {
						diskUUID = volume.DiskUuid
						log.Infof("observed disk UUID %q is set for the volume %q on virtualmachine %q", volume.DiskUuid, volume.Name, vm.Name)
					} else {
						if volume.Error != "" {
							msg := fmt.Sprintf("observed Error: %q is set on the volume %q on virtualmachine %q", volume.Error, volume.Name, vm.Name)
							log.Error(msg)
							return nil, status.Errorf(codes.Internal, msg)
						}
					}
					break
				}
			}
			if diskUUID == "" {
				log.Debugf(fmt.Sprintf("disk UUID is not set for volume: %q ", req.VolumeId))
			}
		}
		log.Debugf(fmt.Sprintf("disk UUID %v is set for the volume: %q ", diskUUID, req.VolumeId))
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

// ControllerUnpublishVolume detaches a volume from the Node VM.
// volume id and node name is retrieved from ControllerUnpublishVolumeRequest
func (c *controller) ControllerUnpublishVolume(ctx context.Context, req *csi.ControllerUnpublishVolumeRequest) (
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

	// TODO: Investigate if a race condition can exist here between multiple detach calls to the same volume.
	// 	If yes, implement some locking mechanism
	virtualMachine := &vmoperatortypes.VirtualMachine{}
	vmKey := types.NamespacedName{
		Namespace: c.supervisorNamespace,
		Name:      req.NodeId,
	}
	if err := c.vmOperatorClient.Get(ctx, vmKey, virtualMachine); err != nil {
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
				virtualMachine.Spec.Volumes = append(virtualMachine.Spec.Volumes[:index], virtualMachine.Spec.Volumes[index+1:]...)
				err = c.vmOperatorClient.Update(ctx, virtualMachine)
				break
			}
		}
		if err == nil || time.Now().After(timeout) {
			break
		}
		if err := c.vmOperatorClient.Get(ctx, vmKey, virtualMachine); err != nil {
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
		log.Debugf(fmt.Sprintf("Waiting for update on VirtualMachine: %q", virtualMachine.Name))
		// Block on update events
		event := <-watchVirtualMachine.ResultChan()
		vm, ok := event.Object.(*vmoperatortypes.VirtualMachine)
		if !ok {
			msg := fmt.Sprintf("Watch on virtualmachine %q timed out", virtualMachine.Name)
			log.Error(msg)
			return nil, status.Errorf(codes.Internal, msg)
		}
		isVolumeDetached = true
		for _, volume := range vm.Status.Volumes {
			if volume.Name == req.VolumeId {
				log.Debugf(fmt.Sprintf("Volume %q still exists in VirtualMachine %q status", volume.Name, virtualMachine.Name))
				isVolumeDetached = false
				if volume.Attached && volume.Error != "" {
					msg := fmt.Sprintf("failed to detach volume %q from VirtualMachine %q with Error: %v", volume.Name, virtualMachine.Name, volume.Error)
					log.Error(msg)
					return nil, status.Errorf(codes.Internal, msg)
				}
				break
			}
		}

	}
	log.Infof("ControllerUnpublishVolume: Volume detached successfully %q", req.VolumeId)
	return &csi.ControllerUnpublishVolumeResponse{}, nil
}

// ControllerExpandVolume expands a volume.
// volume id and size is retrieved from ControllerExpandVolumeRequest
func (c *controller) ControllerExpandVolume(ctx context.Context, req *csi.ControllerExpandVolumeRequest) (
	*csi.ControllerExpandVolumeResponse, error) {
	ctx = logger.NewContextWithLogger(ctx)
	log := logger.GetLogger(ctx)
	if !c.featureStates.VolumeExtend {
		msg := "ExpandVolume feature is disabled on the cluster."
		log.Warn(msg)
		return nil, status.Errorf(codes.Unimplemented, msg)
	}
	log.Infof("ControllerExpandVolume: called with args %+v", *req)

	err := validateGuestClusterControllerExpandVolumeRequest(ctx, req)
	if err != nil {
		return nil, err
	}

	volumeID := req.GetVolumeId()
	volSizeBytes := int64(req.GetCapacityRange().GetRequiredBytes())

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
				msg := fmt.Sprintf("failed to expand volume: %q. Volume is attached to pod. Only offline volume expansion is supported", volumeID)
				log.Error(msg)
				return nil, status.Error(codes.FailedPrecondition, msg)
			}
		}
	}

	// Retrieve Supervisor PVC
	pvc, err := c.supervisorClient.CoreV1().PersistentVolumeClaims(c.supervisorNamespace).Get(volumeID, metav1.GetOptions{})
	if err != nil {
		msg := fmt.Sprintf("failed to retrieve supervisor PVC %q in %q namespace. Error: %+v", volumeID, c.supervisorNamespace, err)
		log.Error(msg)
		return nil, status.Error(codes.Internal, msg)
	}
	// Update requested storage in PVC spec
	pvcClone := pvc.DeepCopy()
	pvcClone.Spec.Resources.Requests[corev1.ResourceName(corev1.ResourceStorage)] = *resource.NewQuantity(volSizeBytes, resource.Format(resource.BinarySI))
	// Make a call to SV ControllerExpandVolume
	log.Debugf("Calling volume expansion for supervisor PVC %q in namespace %q", volumeID, c.supervisorNamespace)
	updatedPVC, err := c.supervisorClient.CoreV1().PersistentVolumeClaims(c.supervisorNamespace).Update(pvcClone)
	if err != nil {
		msg := fmt.Sprintf("failed to update supervisor PVC %q in %q namespace. Error: %+v", volumeID, c.supervisorNamespace, err)
		log.Error(msg)
		return nil, status.Error(codes.Internal, msg)
	}

	// Wait for Supervisor PVC to change status to FilesystemResizePending
	err = checkForSupervisorPVCCondition(ctx, c.supervisorClient, updatedPVC,
		corev1.PersistentVolumeClaimFileSystemResizePending, time.Duration(getResizeTimeoutInMin(ctx))*time.Minute)
	if err != nil {
		msg := fmt.Sprintf("failed to expand volume %s in namespace %s of supervisor cluster. Error: %+v", volumeID, c.supervisorNamespace, err)
		log.Error(msg)
		return nil, status.Error(codes.Internal, msg)
	}

	nodeExpansionRequired := true
	// Set NodeExpansionRequired to false for raw block volumes
	if _, ok := req.GetVolumeCapability().GetAccessType().(*csi.VolumeCapability_Block); ok {
		log.Infof("Node Expansion not supported for raw block volume ID %q in namespace %s of supervisor", volumeID, c.supervisorNamespace)
		nodeExpansionRequired = false
	}
	resp := &csi.ControllerExpandVolumeResponse{
		CapacityBytes:         volSizeBytes,
		NodeExpansionRequired: nodeExpansionRequired,
	}
	return resp, nil
}

// ValidateVolumeCapabilities returns the capabilities of the volume.
func (c *controller) ValidateVolumeCapabilities(ctx context.Context, req *csi.ValidateVolumeCapabilitiesRequest) (
	*csi.ValidateVolumeCapabilitiesResponse, error) {

	log := logger.GetLogger(ctx)
	log.Infof("ValidateVolumeCapabilities: called with args %+v", *req)
	volCaps := req.GetVolumeCapabilities()
	var confirmed *csi.ValidateVolumeCapabilitiesResponse_Confirmed
	if common.IsValidVolumeCapabilities(ctx, volCaps) {
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
