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
	"errors"
	"fmt"
	"strconv"
	"strings"

	"github.com/container-storage-interface/spec/lib/go/csi"
	vmoperatorv1alpha1 "github.com/vmware-tanzu/vm-operator-api/api/v1alpha1"
	"github.com/vmware/govmomi/object"
	"github.com/vmware/govmomi/property"
	"github.com/vmware/govmomi/vim25/mo"
	vimtypes "github.com/vmware/govmomi/vim25/types"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/client-go/dynamic"
	"sigs.k8s.io/controller-runtime/pkg/client/config"

	spv1alpha1 "sigs.k8s.io/vsphere-csi-driver/v3/pkg/apis/storagepool/cns/v1alpha1"
	"sigs.k8s.io/vsphere-csi-driver/v3/pkg/common/cns-lib/vsphere"
	cnsconfig "sigs.k8s.io/vsphere-csi-driver/v3/pkg/common/config"
	"sigs.k8s.io/vsphere-csi-driver/v3/pkg/csi/service/common"
	"sigs.k8s.io/vsphere-csi-driver/v3/pkg/csi/service/common/commonco"
	"sigs.k8s.io/vsphere-csi-driver/v3/pkg/csi/service/logger"
	k8s "sigs.k8s.io/vsphere-csi-driver/v3/pkg/kubernetes"
	"sigs.k8s.io/vsphere-csi-driver/v3/pkg/syncer/k8scloudoperator"
)

// validateCreateBlockReqParam is a helper function used to validate the parameter
// name received in the CreateVolume request for block volumes on WCP CSI driver.
// Returns true if the parameter name is valid, false otherwise.
func validateCreateBlockReqParam(paramName, value string) bool {
	return paramName == common.AttributeStoragePolicyID ||
		paramName == common.AttributeFsType ||
		paramName == common.AttributeStorageTopologyType ||
		paramName == common.AttributeStoragePool ||
		(paramName == common.AttributeHostLocal && strings.EqualFold(value, "true"))
}

const (
	spTypePrefix = "cns.vmware.com/"
	spTypeKey    = spTypePrefix + "StoragePoolType"
)

// validateCreateFileReqParam is a helper function used to validate the parameter
// name received in the CreateVolume request for file volumes on WCP CSI driver
// Returns true if the parameter name is valid, false otherwise.
func validateCreateFileReqParam(paramName, value string) bool {
	return paramName == common.AttributeStoragePolicyID ||
		paramName == common.AttributeStorageTopologyType ||
		paramName == common.AttributeFsType
}

// ValidateCreateVolumeRequest is the helper function to validate
// CreateVolumeRequest for WCP CSI driver.
// Function returns error if validation fails otherwise returns nil.
// TODO: Need to remove AttributeHostLocal after external provisioner stops
// sending this parameter.
func validateWCPCreateVolumeRequest(ctx context.Context, req *csi.CreateVolumeRequest, isBlockRequest bool) error {
	// Get create params.
	params := req.GetParameters()
	for paramName, value := range params {
		paramName = strings.ToLower(paramName)
		if isBlockRequest && !validateCreateBlockReqParam(paramName, value) {
			msg := fmt.Sprintf("Volume parameter %s is not a valid WCP CSI parameter for block volume.", paramName)
			return status.Error(codes.InvalidArgument, msg)
		} else if !isBlockRequest && !validateCreateFileReqParam(paramName, value) {
			msg := fmt.Sprintf("Volume parameter %s is not a valid WCP CSI parameter for file volumes.", paramName)
			return status.Error(codes.InvalidArgument, msg)
		}
	}
	return common.ValidateCreateVolumeRequest(ctx, req)
}

// validateWCPDeleteVolumeRequest is the helper function to validate
// DeleteVolumeRequest for WCP CSI driver.
// Function returns error if validation fails otherwise returns nil.
func validateWCPDeleteVolumeRequest(ctx context.Context, req *csi.DeleteVolumeRequest) error {
	return common.ValidateDeleteVolumeRequest(ctx, req)
}

// validateWCPControllerPublishVolumeRequest is the helper function to validate
// ControllerPublishVolumeRequest for WCP CSI driver. Function returns error if
// validation fails otherwise returns nil.
func validateWCPControllerPublishVolumeRequest(ctx context.Context, req *csi.ControllerPublishVolumeRequest) error {
	return common.ValidateControllerPublishVolumeRequest(ctx, req)
}

// validateWCPControllerUnpublishVolumeRequest is the helper function to
// validate ControllerUnpublishVolumeRequest for WCP CSI driver. Function
// returns error if validation fails otherwise returns nil.
func validateWCPControllerUnpublishVolumeRequest(ctx context.Context, req *csi.ControllerUnpublishVolumeRequest) error {
	return common.ValidateControllerUnpublishVolumeRequest(ctx, req)
}

// validateWCPControllerExpandVolumeRequest is the helper function to validate
// ExpandVolumeRequest for WCP CSI driver. Function returns error if validation
// fails otherwise returns nil.
func validateWCPControllerExpandVolumeRequest(ctx context.Context, req *csi.ControllerExpandVolumeRequest,
	manager *common.Manager, isOnlineExpansionEnabled bool) error {
	log := logger.GetLogger(ctx)
	if err := common.ValidateControllerExpandVolumeRequest(ctx, req); err != nil {
		return err
	}

	if !isOnlineExpansionEnabled {
		var nodes []*vsphere.VirtualMachine

		// TODO: Currently we only check if disk is attached to TKG nodes
		// We need to check if the disk is attached to a PodVM as well.

		// Get datacenter object from config.
		vc, err := common.GetVCenter(ctx, manager)
		if err != nil {
			return logger.LogNewErrorCodef(log, codes.Internal,
				"failed to get vcenter object with error: %+v", err)
		}
		dc := &vsphere.Datacenter{
			Datacenter: object.NewDatacenter(vc.Client.Client,
				vimtypes.ManagedObjectReference{
					Type:  "Datacenter",
					Value: vc.Config.DatacenterPaths[0],
				}),
			VirtualCenterHost: vc.Config.Host,
		}

		// Create client to list VMs from the supervisor cluster API server.
		cfg, err := config.GetConfig()
		if err != nil {
			return logger.LogNewErrorCodef(log, codes.Internal,
				"failed to get config with error: %+v", err)
		}
		vmOperatorClient, err := k8s.NewClientForGroup(ctx, cfg, vmoperatorv1alpha1.GroupName)
		if err != nil {
			return logger.LogNewErrorCodef(log, codes.Internal,
				"failed to get client for group %s with error: %+v", vmoperatorv1alpha1.GroupName, err)
		}
		vmList := &vmoperatorv1alpha1.VirtualMachineList{}
		err = vmOperatorClient.List(ctx, vmList)
		if err != nil {
			return logger.LogNewErrorCodef(log, codes.Internal,
				"failed to list virtualmachines with error: %+v", err)
		}

		// Get BIOS UUID from VMs to create VirtualMachine object.
		for _, vmInstance := range vmList.Items {
			biosUUID := vmInstance.Status.BiosUUID
			vm, err := dc.GetVirtualMachineByUUID(ctx, biosUUID, false)
			if err != nil {
				return logger.LogNewErrorCodef(log, codes.Internal,
					"failed to get vm with biosUUID: %q with error: %+v", biosUUID, err)
			}
			nodes = append(nodes, vm)
		}

		return common.IsOnlineExpansion(ctx, req.GetVolumeId(), nodes)
	}
	return nil
}

// validateWCPCreateSnapshotRequest is the helper function to
// validate CreateSnapshotRequest for CSI driver.
// Function returns error if validation fails otherwise returns nil.
func validateWCPCreateSnapshotRequest(ctx context.Context, req *csi.CreateSnapshotRequest) error {
	log := logger.GetLogger(ctx)
	volumeID := req.GetSourceVolumeId()
	if len(volumeID) == 0 {
		return logger.LogNewErrorCode(log, codes.InvalidArgument,
			"CreateSnapshot Source Volume ID must be provided")
	}

	if len(req.Name) == 0 {
		return logger.LogNewErrorCode(log, codes.InvalidArgument,
			"Snapshot name must be provided")
	}
	return nil
}

// validateWCPListSnapshotRequest validates ListSnapshotRequest on supervisor
func validateWCPListSnapshotRequest(ctx context.Context, req *csi.ListSnapshotsRequest) error {
	log := logger.GetLogger(ctx)
	maxEntries := req.MaxEntries
	if maxEntries < 0 {
		return logger.LogNewErrorCodef(log, codes.InvalidArgument,
			"ListSnapshots MaxEntries: %d cannot be negative", maxEntries)
	}
	// validate the starting token by verifying that it can be converted to a int
	if req.StartingToken != "" {
		_, err := strconv.Atoi(req.StartingToken)
		if err != nil {
			return logger.LogNewErrorCodef(log, codes.InvalidArgument,
				"ListSnapshots StartingToken: %s cannot be parsed", req.StartingToken)
		}
	}
	// validate snapshot-id conforms to vSphere CSI driver format if specified.
	// The expected format is "fcd-id+snapshot-id"
	if req.SnapshotId != "" {
		// check for the delimiter "+" in the snapshot-id.
		check := strings.Contains(req.SnapshotId, common.VSphereCSISnapshotIdDelimiter)
		if !check {
			return logger.LogNewErrorCodef(log, codes.InvalidArgument,
				"ListSnapshots SnapshotId: %s is incorrectly formatted for vSphere CSI driver",
				req.SnapshotId)
		}
	}
	return nil
}

// getK8sCloudOperatorClientConnection is a helper function that creates a
// clientConnection to k8sCloudOperator GRPC service running on syncer container.
func getK8sCloudOperatorClientConnection(ctx context.Context) (*grpc.ClientConn, error) {
	var opts []grpc.DialOption
	opts = append(opts, grpc.WithTransportCredentials(insecure.NewCredentials()))
	port := common.GetK8sCloudOperatorServicePort(ctx)
	k8sCloudOperatorServiceAddr := "127.0.0.1:" + strconv.Itoa(port)
	// Connect to k8s cloud operator gRPC service.
	conn, err := grpc.Dial(k8sCloudOperatorServiceAddr, opts...)
	if err != nil {
		return nil, err
	}
	return conn, nil
}

// GetsvMotionPlanFromK8sCloudOperatorService gets storage vMotion plan from
// K8sCloudOperator gRPC service.
func GetsvMotionPlanFromK8sCloudOperatorService(ctx context.Context,
	storagePoolName string, maintenanceMode string) (map[string]string, error) {
	log := logger.GetLogger(ctx)
	conn, err := getK8sCloudOperatorClientConnection(ctx)
	if err != nil {
		log.Errorf("Failed to establish the connection to k8s cloud operator service "+
			"when getting svMotion plan for SP: %s. Error: %+v", storagePoolName, err)
		return nil, err
	}
	defer conn.Close()
	// Create a client stub for k8s cloud operator gRPC service.
	client := k8scloudoperator.NewK8SCloudOperatorClient(conn)

	res, err := client.GetStorageVMotionPlan(ctx,
		&k8scloudoperator.StorageVMotionRequest{
			StoragePoolName: storagePoolName,
			MaintenanceMode: maintenanceMode,
		})
	if err != nil {
		msg := fmt.Sprintf("Failed to get storage vMotion plan from the k8s cloud operator service. Error: %+v", err)
		log.Error(msg)
		return nil, err
	}

	log.Infof("Got storage vMotion plan: %v from K8sCloudOperator gRPC service", res.SvMotionPlan)
	return res.SvMotionPlan, nil
}

// getVMUUIDFromK8sCloudOperatorService gets the vmuuid from K8sCloudOperator
// gRPC service.
func getVMUUIDFromK8sCloudOperatorService(ctx context.Context, volumeID string, nodeName string) (string, error) {
	log := logger.GetLogger(ctx)
	conn, err := getK8sCloudOperatorClientConnection(ctx)
	if err != nil {
		log.Errorf("Failed to establish the connection to k8s cloud operator service "+
			"when processing attach for volumeID: %s. Error: %+v", volumeID, err)
		return "", err
	}
	defer conn.Close()

	// Create a client stub for k8s cloud operator gRPC service.
	client := k8scloudoperator.NewK8SCloudOperatorClient(conn)

	// Call GetPodVMUUIDAnnotation method on the client stub.
	res, err := client.GetPodVMUUIDAnnotation(ctx,
		&k8scloudoperator.PodListenerRequest{
			VolumeID: volumeID,
			NodeName: nodeName,
		})
	if err != nil {
		msg := fmt.Sprintf("Failed to get the pod vmuuid annotation from the k8s cloud operator service. Error: %+v", err)
		log.Error(msg)
		return "", err
	}

	log.Infof("Got vmuuid: %s annotation from K8sCloudOperator gRPC service", res.VmuuidAnnotation)
	return res.VmuuidAnnotation, nil
}

// getHostMOIDFromK8sCloudOperatorService gets the host-moid from
// K8sCloudOperator gRPC service.
func getHostMOIDFromK8sCloudOperatorService(ctx context.Context, nodeName string) (string, error) {
	log := logger.GetLogger(ctx)
	conn, err := getK8sCloudOperatorClientConnection(ctx)
	if err != nil {
		log.Errorf("failed to establish the connection to k8s cloud operator service. Error: %+v", err)
		return "", err
	}
	defer conn.Close()

	// Create a client stub for gRPC service.
	client := k8scloudoperator.NewK8SCloudOperatorClient(conn)

	// Call GetHostAnnotation method on the client stub.
	res, err := client.GetHostAnnotation(ctx,
		&k8scloudoperator.HostAnnotationRequest{
			HostName:      nodeName,
			AnnotationKey: common.HostMoidAnnotationKey,
		})
	if err != nil {
		msg := fmt.Sprintf("failed to get the host moid annotation from the gRPC service. Error: %+v", err)
		log.Error(msg)
		return "", err
	}

	log.Infof("Got host-moid: %s annotation from the K8sCloudOperator gRPC service", res.AnnotationValue)
	return res.AnnotationValue, nil
}

// getDatacenterFromConfig gets the vcenter-datacenter where WCP PodVM cluster
// is deployed.
func getDatacenterFromConfig(cfg *cnsconfig.Config) (map[string]string, error) {
	vcdcMap := make(map[string]string)
	vcdcListMap, err := getVCDatacentersFromConfig(cfg)
	if err != nil {
		return vcdcMap, err
	}
	if len(vcdcListMap) != 1 {
		return vcdcMap, fmt.Errorf("found more than one vCenter instance. WCP Cluster can be deployed in only one VC")
	}

	for vcHost, dcList := range vcdcListMap {
		if len(dcList) > 1 {
			return vcdcMap, fmt.Errorf("found more than one datacenter instances: %+v for vcHost: %s. "+
				"WCP Cluster can be deployed in only one datacenter", dcList, vcHost)
		}
		vcdcMap[vcHost] = dcList[0]
	}
	return vcdcMap, nil
}

// GetVCDatacenters returns list of datacenters for each registered vCenter.
func getVCDatacentersFromConfig(cfg *cnsconfig.Config) (map[string][]string, error) {
	var err error
	vcdcMap := make(map[string][]string)
	for key, value := range cfg.VirtualCenter {
		dcList := strings.Split(value.Datacenters, ",")
		for _, dc := range dcList {
			dcMoID := strings.TrimSpace(dc)
			if dcMoID != "" {
				vcdcMap[key] = append(vcdcMap[key], dcMoID)
			}
		}
	}
	if len(vcdcMap) == 0 {
		err = errors.New("unable get vCenter datacenters from vsphere config")
	}
	return vcdcMap, err
}

// getVMByInstanceUUIDInDatacenter gets the VM with the given instance UUID
// in datacenter specified using datacenter moref value.
func getVMByInstanceUUIDInDatacenter(ctx context.Context,
	vc *vsphere.VirtualCenter,
	datacenter string,
	vmInstanceUUID string) (*vsphere.VirtualMachine, error) {
	log := logger.GetLogger(ctx)
	var dc *vsphere.Datacenter
	var vm *vsphere.VirtualMachine
	dc = &vsphere.Datacenter{
		Datacenter: object.NewDatacenter(vc.Client.Client,
			vimtypes.ManagedObjectReference{
				Type:  "Datacenter",
				Value: datacenter,
			}),
		VirtualCenterHost: vc.Config.Host,
	}
	// Get VM by UUID from datacenter.
	vm, err := dc.GetVirtualMachineByUUID(ctx, vmInstanceUUID, true)
	if err != nil {
		log.Errorf("failed to find the VM from the VM Instance UUID: %s in datacenter: %+v with err: %+v",
			vmInstanceUUID, dc, err)
		return nil, err
	}
	return vm, nil
}

// getDatastoreURLFromStoragePool returns the datastoreUrl that the given
// StoragePool represents.
func getDatastoreURLFromStoragePool(ctx context.Context, spName string) (string, error) {
	// Get a config to talk to the apiserver.
	cfg, err := config.GetConfig()
	if err != nil {
		return "", fmt.Errorf("failed to get Kubernetes config. Err: %+v", err)
	}

	// create a new StoragePool client.
	spclient, err := dynamic.NewForConfig(cfg)
	if err != nil {
		return "", fmt.Errorf("failed to create StoragePool client using config. Err: %+v", err)
	}
	spResource := spv1alpha1.SchemeGroupVersion.WithResource("storagepools")

	// Get StoragePool with spName.
	sp, err := spclient.Resource(spResource).Get(ctx, spName, metav1.GetOptions{})
	if err != nil {
		return "", fmt.Errorf("failed to get StoragePool with name %s: %+v", spName, err)
	}

	// extract the datastoreUrl field.
	datastoreURL, found, err := unstructured.NestedString(sp.Object, "spec", "parameters", "datastoreUrl")
	if !found || err != nil {
		return "", fmt.Errorf("failed to find datastoreUrl in StoragePool %s", spName)
	}
	return datastoreURL, nil
}

// getStoragePoolInfo returns the accessibleNodes and the storage-pool-type
// pertaining to the given StoragePool.
func getStoragePoolInfo(ctx context.Context, spName string) ([]string, string, error) {
	// Get a config to talk to the apiserver.
	cfg, err := config.GetConfig()
	if err != nil {
		return nil, "", fmt.Errorf("failed to get Kubernetes config. Err: %+v", err)
	}

	// Create a new StoragePool client.
	spClient, err := dynamic.NewForConfig(cfg)
	if err != nil {
		return nil, "", fmt.Errorf("failed to create StoragePool client using config. Err: %+v", err)
	}
	spResource := spv1alpha1.SchemeGroupVersion.WithResource("storagepools")

	// Get StoragePool with spName.
	sp, err := spClient.Resource(spResource).Get(ctx, spName, metav1.GetOptions{})
	if err != nil {
		return nil, "", fmt.Errorf("failed to get StoragePool with name %s: %+v", spName, err)
	}

	// Extract the accessibleNodes field.
	accessibleNodes, found, err := unstructured.NestedStringSlice(sp.Object, "status", "accessibleNodes")
	if !found || err != nil {
		return nil, "", fmt.Errorf("failed to find datastoreUrl in StoragePool %s", spName)
	}

	// Get the storage pool type.
	poolType, found, err := unstructured.NestedString(sp.Object, "metadata", "labels", spTypeKey)
	if !found || err != nil {
		return nil, "", fmt.Errorf("failed to find pool type in StoragePool %s", spName)
	}

	return accessibleNodes, poolType, nil
}

// isValidAccessibilityRequirements validates if the given accessibility
// requirement has the necessary elements in it.
func isValidAccessibilityRequirement(topologyRequirement *csi.TopologyRequirement) bool {
	if topologyRequirement == nil || topologyRequirement.GetPreferred() == nil {
		return false
	}
	return true
}

// getOverlappingNodes returns the list of nodes that is present both in the
// accessibleNodes of the storagePool and the host names present provided in
// the preferred segment of accessibility requirements.
func getOverlappingNodes(accessibleNodes []string, topologyRequirement *csi.TopologyRequirement) ([]string, error) {
	var overlappingNodes []string
	noOverLappingNodes := true
	accessibleNodeMap := make(map[string]bool)
	for _, node := range accessibleNodes {
		accessibleNodeMap[node] = true
	}
	for _, topology := range topologyRequirement.GetPreferred() {
		hostname := topology.Segments[v1.LabelHostname]
		if accessibleNodeMap[hostname] {
			// Found an overlapping node.
			noOverLappingNodes = false
			overlappingNodes = append(overlappingNodes, hostname)
		}
	}
	if noOverLappingNodes {
		return nil, fmt.Errorf("couldn't find any overlapping node as accessible nodes present in storage pool " +
			"and hostnames provided in accessibility requirements are disjoint")
	}
	return overlappingNodes, nil
}

// checkTopologyKeysFromAccessibilityReqs checks if the topology requirement contains zone or hostname labels.
func checkTopologyKeysFromAccessibilityReqs(topologyRequirement *csi.TopologyRequirement) (bool, bool) {
	var hostnameLabelPresent, zoneLabelPresent bool

	if topologyRequirement == nil || topologyRequirement.GetPreferred() == nil {
		return hostnameLabelPresent, zoneLabelPresent
	}
	for _, topology := range topologyRequirement.GetPreferred() {
		segments := topology.GetSegments()
		if _, exists := segments[v1.LabelHostname]; exists {
			hostnameLabelPresent = true
		}
		if _, exists := segments[v1.LabelTopologyZone]; exists {
			zoneLabelPresent = true
		}
		// If both zone and hostname labels are present, exit the loop and return.
		if hostnameLabelPresent && zoneLabelPresent {
			return hostnameLabelPresent, zoneLabelPresent
		}
	}
	return hostnameLabelPresent, zoneLabelPresent
}

// GetVolumeToHostMapping returns a map containing VM MoID to host MoID and VolumeID
// and VM MoID. This map is constructed by fetching all virtual machines belonging to each host.
func (c *controller) GetVolumeToHostMapping(ctx context.Context) (map[string]string, map[string]string, error) {
	log := logger.GetLogger(ctx)
	vmMoIDToHostMoID := make(map[string]string)
	volumeIDVMMap := make(map[string]string)

	// Get VirtualCenter object
	vc, err := common.GetVCenter(ctx, c.manager)
	if err != nil {
		log.Errorf("GetVcenter error %v", err)
		return nil, nil, fmt.Errorf("failed to get vCenter from Manager, err: %v", err)
	}

	// Get all the hosts belonging to the cluster
	hostSystems, err := vc.GetHostsByCluster(ctx, c.manager.CnsConfig.Global.ClusterID)
	if err != nil {
		log.Errorf("failed to get hosts for cluster %v, err:%v", c.manager.CnsConfig.Global.ClusterID, err)
		return nil, nil, fmt.Errorf("failed to get hosts for cluster %v, err:%v", c.manager.CnsConfig.Global.ClusterID, err)
	}

	// Get all the virtual machines belonging to all the hosts
	vms, err := vc.GetAllVirtualMachines(ctx, hostSystems)
	if err != nil {
		log.Errorf("failed to get VM MoID err: %v", err)
		return nil, nil, fmt.Errorf("failed to get VM MoID err: %v", err)
	}

	var vmRefs []vimtypes.ManagedObjectReference
	var vmMoList []mo.VirtualMachine

	for _, vm := range vms {
		vmRefs = append(vmRefs, vm.Reference())
	}
	properties := []string{"runtime.host", "config.hardware"}
	pc := property.DefaultCollector(vc.Client.Client)
	// Obtain host MoID and virtual disk ID
	err = pc.Retrieve(ctx, vmRefs, properties, &vmMoList)
	if err != nil {
		log.Errorf("Error while retrieving host properties, err: %v", err)
		return vmMoIDToHostMoID, volumeIDVMMap, err
	}

	// Iterate through all the VMs and build the vmMoIDToHostMoID map
	// and the volumeID to VMMoiD map
	for _, info := range vmMoList {
		vmMoID := info.Reference().Value

		if info.Runtime.Host != nil {
			vmMoIDToHostMoID[vmMoID] = info.Runtime.Host.Reference().Value
		}
		if info.Config == nil {
			continue
		}
		devices := info.Config.Hardware.Device
		vmDevices := object.VirtualDeviceList(devices)
		for _, device := range vmDevices {
			if vmDevices.TypeName(device) == "VirtualDisk" {
				if virtualDisk, ok := device.(*vimtypes.VirtualDisk); ok {
					if virtualDisk.VDiskId != nil {
						volumeIDVMMap[virtualDisk.VDiskId.Id] = vmMoID
					}
				}
			}
		}
	}
	return vmMoIDToHostMoID, volumeIDVMMap, nil
}

// getVolumeIDToVMMap returns the csi list volume response by computing the volumeID to nodeNames map for
// fake attached volumes and non-fake attached volumes.
func getVolumeIDToVMMap(ctx context.Context, c *controller, volumeIDs []string) (*csi.ListVolumesResponse, error) {
	log := logger.GetLogger(ctx)
	response := &csi.ListVolumesResponse{}

	allFakeAttachMarkedVolumes := commonco.ContainerOrchestratorUtility.GetFakeAttachedVolumes(ctx, volumeIDs)
	fakeAttachedVolumes := make([]string, 0)
	for volumeID, isfakeAttached := range allFakeAttachMarkedVolumes {
		if isfakeAttached {
			fakeAttachedVolumes = append(fakeAttachedVolumes, volumeID)
		}
	}
	// Process fake attached volumes
	log.Debugf("Fake attached volumes %v", fakeAttachedVolumes)
	volumeIDToNodesMap := commonco.ContainerOrchestratorUtility.GetNodesForVolumes(ctx, fakeAttachedVolumes)
	for volumeID, publishedNodeIDs := range volumeIDToNodesMap {
		volume := &csi.Volume{
			VolumeId: volumeID,
		}
		volumeStatus := &csi.ListVolumesResponse_VolumeStatus{
			PublishedNodeIds: publishedNodeIDs,
		}
		entry := &csi.ListVolumesResponse_Entry{
			Volume: volume,
			Status: volumeStatus,
		}
		response.Entries = append(response.Entries, entry)
	}

	// Process remaining volumes
	vmMoidToHostMoid, volumeIDToVMMap, err := c.GetVolumeToHostMapping(ctx)
	if err != nil {
		log.Errorf("failed to get VM MoID to Host MoID map, err:%v", err)
		return nil, fmt.Errorf("failed to get VM MoID to Host MoID map, err: %v", err)
	}

	hostNames := commonco.ContainerOrchestratorUtility.GetNodeIDtoNameMap(ctx)
	if len(hostNames) == 0 {
		log.Errorf("no hostnames found in the NodeIDtoName map")
		return nil, fmt.Errorf("no hostnames found in the NodeIDtoName map")
	}

	for volumeID, VMMoID := range volumeIDToVMMap {
		isFakeAttached, exists := allFakeAttachMarkedVolumes[volumeID]
		// If we do not find this entry in the input list obtained from CNS
		//, then we do not bother adding it to the result since, CNS is not aware
		// of this volume. Also, if it is fake attached volume we have handled it
		// above so we will not add it to the response here.
		if !exists || isFakeAttached {
			continue
		}

		hostMoID, ok := vmMoidToHostMoid[VMMoID]
		if !ok {
			continue
		}

		hostName, ok := hostNames[hostMoID]
		if !ok {
			continue
		}
		publishedNodeIDs := make([]string, 0)
		publishedNodeIDs = append(publishedNodeIDs, hostName)
		volume := &csi.Volume{
			VolumeId: volumeID,
		}
		volumeStatus := &csi.ListVolumesResponse_VolumeStatus{
			PublishedNodeIds: publishedNodeIDs,
		}
		entry := &csi.ListVolumesResponse_Entry{
			Volume: volume,
			Status: volumeStatus,
		}
		response.Entries = append(response.Entries, entry)
	}

	return response, nil
}
