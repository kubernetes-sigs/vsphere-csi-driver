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
	"github.com/vmware/govmomi/vim25/types"
	vimtypes "github.com/vmware/govmomi/vim25/types"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/client-go/dynamic"
	"sigs.k8s.io/controller-runtime/pkg/client/config"

	spv1alpha1 "sigs.k8s.io/vsphere-csi-driver/pkg/apis/storagepool/cns/v1alpha1"
	"sigs.k8s.io/vsphere-csi-driver/pkg/common/cns-lib/vsphere"
	cnsvsphere "sigs.k8s.io/vsphere-csi-driver/pkg/common/cns-lib/vsphere"
	cnsconfig "sigs.k8s.io/vsphere-csi-driver/pkg/common/config"
	"sigs.k8s.io/vsphere-csi-driver/pkg/csi/service/common"
	"sigs.k8s.io/vsphere-csi-driver/pkg/csi/service/logger"
	k8s "sigs.k8s.io/vsphere-csi-driver/pkg/kubernetes"
	"sigs.k8s.io/vsphere-csi-driver/pkg/syncer/k8scloudoperator"
)

// validateParam is a helper function used to validate the parameter name
// received in the CreateVolume request for WCP CSI driver
// Returns true if the parameter name is valid, false otherwise
func validateParam(paramName, value string) bool {
	return paramName == common.AttributeStoragePolicyID ||
		paramName == common.AttributeFsType ||
		paramName == common.AttributeAffineToHost ||
		paramName == common.AttributeStoragePool ||
		(paramName == common.AttributeHostLocal && strings.EqualFold(value, "true"))
}

// ValidateCreateVolumeRequest is the helper function to validate
// CreateVolumeRequest for WCP CSI driver.
// Function returns error if validation fails otherwise returns nil.
func validateWCPCreateVolumeRequest(ctx context.Context, req *csi.CreateVolumeRequest) error {
	// Get create params
	params := req.GetParameters()
	for paramName, value := range params {
		paramName = strings.ToLower(paramName)
		if !validateParam(paramName, value) {
			msg := fmt.Sprintf("Volume parameter %s is not a valid WCP CSI parameter.", paramName)
			return status.Error(codes.InvalidArgument, msg)
		}
	}
	// Fail file volume creation
	if common.IsFileVolumeRequest(ctx, req.GetVolumeCapabilities()) {
		return status.Error(codes.InvalidArgument, "File volume not supported.")
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
// ControllerPublishVolumeRequest for WCP CSI driver. Function returns error if validation fails otherwise returns nil.
func validateWCPControllerPublishVolumeRequest(ctx context.Context, req *csi.ControllerPublishVolumeRequest) error {
	return common.ValidateControllerPublishVolumeRequest(ctx, req)
}

// validateWCPControllerUnpublishVolumeRequest is the helper function to validate
// ControllerUnpublishVolumeRequest for WCP CSI driver. Function returns error if validation fails otherwise returns nil.
func validateWCPControllerUnpublishVolumeRequest(ctx context.Context, req *csi.ControllerUnpublishVolumeRequest) error {
	return common.ValidateControllerUnpublishVolumeRequest(ctx, req)
}

// validateWCPControllerExpandVolumeRequest is the helper function to validate
// ExpandVolumeRequest for WCP CSI driver.
// Function returns error if validation fails otherwise returns nil.
func validateWCPControllerExpandVolumeRequest(ctx context.Context, req *csi.ControllerExpandVolumeRequest, manager *common.Manager) error {
	log := logger.GetLogger(ctx)
	if err := common.ValidateControllerExpandVolumeRequest(ctx, req); err != nil {
		return err
	}

	var nodes []*cnsvsphere.VirtualMachine

	// TODO: Currently we only check if disk is attached to TKG nodes
	// We need to check if the disk is attached to a PodVM as well.

	// Get datacenter object from config
	vc, err := common.GetVCenter(ctx, manager)
	if err != nil {
		msg := fmt.Sprintf("failed to get vcenter object with error: %+v", err)
		log.Errorf(msg)
		return status.Errorf(codes.Internal, msg)
	}
	dc := &cnsvsphere.Datacenter{
		Datacenter: object.NewDatacenter(vc.Client.Client,
			vimtypes.ManagedObjectReference{
				Type:  "Datacenter",
				Value: vc.Config.DatacenterPaths[0],
			}),
		VirtualCenterHost: vc.Config.Host,
	}

	// Create client to list virtualmachine instances from the supervisor cluster API server
	cfg, err := config.GetConfig()
	if err != nil {
		msg := fmt.Sprintf("failed to get config with error: %+v", err)
		log.Error(msg)
		return status.Errorf(codes.Internal, msg)
	}
	vmOperatorClient, err := k8s.NewClientForGroup(ctx, cfg, vmoperatorv1alpha1.GroupName)
	if err != nil {
		msg := fmt.Sprintf("failed to get client for group %s with error: %+v", vmoperatorv1alpha1.GroupName, err)
		log.Error(msg)
		return status.Errorf(codes.Internal, msg)
	}
	vmList := &vmoperatorv1alpha1.VirtualMachineList{}
	err = vmOperatorClient.List(ctx, vmList)
	if err != nil {
		msg := fmt.Sprintf("failed to list virtualmachines with error: %+v", err)
		log.Error(msg)
		return status.Errorf(codes.Internal, msg)
	}

	// Get BIOS UUID from virtualmachine instances to create VirtualMachine object
	for _, vmInstance := range vmList.Items {
		biosUUID := vmInstance.Status.BiosUUID
		vm, err := dc.GetVirtualMachineByUUID(ctx, biosUUID, false)
		if err != nil {
			msg := fmt.Sprintf("failed to get vm with biosUUID: %q with error: %+v", biosUUID, err)
			log.Error(msg)
			return status.Errorf(codes.Internal, msg)
		}
		nodes = append(nodes, vm)
	}

	return common.IsOnlineExpansion(ctx, req.GetVolumeId(), nodes)
}

// getK8sCloudOperatorClientConnection is a helper function that creates a clientConnection to
// k8sCloudOperator GRPC service running on syncer container
func getK8sCloudOperatorClientConnection(ctx context.Context) (*grpc.ClientConn, error) {
	var opts []grpc.DialOption
	opts = append(opts, grpc.WithInsecure())
	port := common.GetK8sCloudOperatorServicePort(ctx)
	k8sCloudOperatorServiceAddr := "127.0.0.1:" + strconv.Itoa(port)
	// Connect to k8s cloud operator gRPC service
	conn, err := grpc.Dial(k8sCloudOperatorServiceAddr, opts...)
	if err != nil {
		return nil, err
	}
	return conn, nil
}

// getVMUUIDFromK8sCloudOperatorService gets the vmuuid from K8sCloudOperator gRPC service
func getVMUUIDFromK8sCloudOperatorService(ctx context.Context, volumeID string, nodeName string) (string, error) {
	log := logger.GetLogger(ctx)
	conn, err := getK8sCloudOperatorClientConnection(ctx)
	if err != nil {
		log.Errorf("Failed to establish the connection to k8s cloud operator service when processing attach for volumeID: %s. Error: %+v", volumeID, err)
		return "", err
	}
	defer conn.Close()

	// Create a client stub for k8s cloud operator gRPC service
	client := k8scloudoperator.NewK8SCloudOperatorClient(conn)

	// Call GetPodVMUUIDAnnotation method on the client stub
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

// getHostMOIDFromK8sCloudOperatorService gets the host-moid from K8sCloudOperator gRPC service
func getHostMOIDFromK8sCloudOperatorService(ctx context.Context, nodeName string) (string, error) {
	log := logger.GetLogger(ctx)
	conn, err := getK8sCloudOperatorClientConnection(ctx)
	if err != nil {
		log.Errorf("failed to establish the connection to k8s cloud operator service. Error: %+v", err)
		return "", err
	}
	defer conn.Close()

	// Create a client stub for gRPC service
	client := k8scloudoperator.NewK8SCloudOperatorClient(conn)

	// Call GetHostAnnotation method on the client stub
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

// getDatacenterFromConfig gets the vcenter-datacenter where WCP PodVM cluster is deployed
func getDatacenterFromConfig(cfg *cnsconfig.Config) (map[string]string, error) {
	vcdcMap := make(map[string]string)
	vcdcListMap, err := getVCDatacentersFromConfig(cfg)
	if err != nil {
		return vcdcMap, err
	}
	if len(vcdcListMap) != 1 {
		return vcdcMap, fmt.Errorf("Found more than one vCenter instance. WCP Cluster can be deployed in only one VC")
	}

	for vcHost, dcList := range vcdcListMap {
		if len(dcList) > 1 {
			return vcdcMap, fmt.Errorf("Found more than one datacenter instances: %+v for vcHost: %s. WCP Cluster can be deployed in only one datacenter", dcList, vcHost)
		}
		vcdcMap[vcHost] = dcList[0]
	}
	return vcdcMap, nil
}

// GetVCDatacenters returns list of datacenters for each vCenter that is registered
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
		err = errors.New("Unable get vCenter datacenters from vsphere config")
	}
	return vcdcMap, err
}

/*
 * getVMByInstanceUUIDInDatacenter gets the VM with the given instance UUID
 * in datacenter specified using datacenter moref value.
 */
func getVMByInstanceUUIDInDatacenter(ctx context.Context,
	vc *vsphere.VirtualCenter,
	datacenter string,
	vmInstanceUUID string) (*vsphere.VirtualMachine, error) {
	var dc *vsphere.Datacenter
	var vm *vsphere.VirtualMachine
	dc = &vsphere.Datacenter{
		Datacenter: object.NewDatacenter(vc.Client.Client,
			types.ManagedObjectReference{
				Type:  "Datacenter",
				Value: datacenter,
			}),
		VirtualCenterHost: vc.Config.Host,
	}
	// Get VM by UUID from datacenter
	vm, err := dc.GetVirtualMachineByUUID(ctx, vmInstanceUUID, true)
	if err != nil {
		return nil, fmt.Errorf("failed to the VM from the VM Instance UUID: %s in datacenter: %+v with err: %+v", vmInstanceUUID, dc, err)
	}
	return vm, nil
}

// getDatastoreURLFromStoragePool returns the datastoreUrl that the given StoragePool represents
func getDatastoreURLFromStoragePool(spName string) (string, error) {
	// Get a config to talk to the apiserver
	cfg, err := config.GetConfig()
	if err != nil {
		return "", fmt.Errorf("Failed to get Kubernetes config. Err: %+v", err)
	}

	// create a new StoragePool client
	spclient, err := dynamic.NewForConfig(cfg)
	if err != nil {
		return "", fmt.Errorf("Failed to create StoragePool client using config. Err: %+v", err)
	}
	spResource := spv1alpha1.SchemeGroupVersion.WithResource("storagepools")

	// Get StoragePool with spName
	sp, err := spclient.Resource(spResource).Get(spName, metav1.GetOptions{})
	if err != nil {
		return "", fmt.Errorf("Failed to get StoragePool with name %s: %+v", spName, err)
	}

	// extract the datastoreUrl field
	datastoreURL, found, err := unstructured.NestedString(sp.Object, "spec", "parameters", "datastoreUrl")
	if !found || err != nil {
		return "", fmt.Errorf("Failed to find datastoreUrl in StoragePool %s", spName)
	}
	return datastoreURL, nil
}
