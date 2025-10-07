/*
Copyright 2021-2025 The Kubernetes Authors.

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

package util

import (
	"context"
	"errors"
	"fmt"
	"os"
	"strconv"
	"strings"

	"github.com/vmware/govmomi/object"
	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	apitypes "k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/dynamic"
	"sigs.k8s.io/controller-runtime/pkg/client"
	cnsvsphere "sigs.k8s.io/vsphere-csi-driver/v3/pkg/common/cns-lib/vsphere"

	"sigs.k8s.io/vsphere-csi-driver/v3/pkg/common/utils"
	k8s "sigs.k8s.io/vsphere-csi-driver/v3/pkg/kubernetes"

	vimtypes "github.com/vmware/govmomi/vim25/types"
	"sigs.k8s.io/vsphere-csi-driver/v3/pkg/common/config"
	"sigs.k8s.io/vsphere-csi-driver/v3/pkg/csi/service/common"
	"sigs.k8s.io/vsphere-csi-driver/v3/pkg/csi/service/common/commonco"
	"sigs.k8s.io/vsphere-csi-driver/v3/pkg/csi/service/logger"
)

var virtualNetworkGVR = schema.GroupVersionResource{
	Group:    "vmware.com",
	Version:  "v1alpha1",
	Resource: "virtualnetworks",
}

var networkInfoGVR = schema.GroupVersionResource{
	Group:    "crd.nsx.vmware.com",
	Version:  "v1alpha1",
	Resource: "networkinfos",
}

var namespaceNetworkInfoGVR = schema.GroupVersionResource{
	Group:    "nsx.vmware.com",
	Version:  "v1alpha1",
	Resource: "namespacenetworkinfos",
}

const (
	snatIPAnnotation = "ncp/snat_ip"
	// Namespace for system resources.
	kubeSystemNamespace = "kube-system"
	// WCP configmap that contains info about networking configuration.
	wcpNetworkConfigMap = "wcp-network-config"
	// Key for network-provider field in 'wcp-network-config' configmap.
	networkProvider = "network_provider"
	// NSXTNetworkProvider holds the network provider name for NSX-T based setups.
	NSXTNetworkProvider = "NSXT_CONTAINER_PLUGIN"
	// VDSNetworkProvider holds the network provider name for VDS based setups.
	VDSNetworkProvider = "VSPHERE_NETWORK"
	// VPCNetworkProvider holds the network provider name for VPC based setups.
	VPCNetworkProvider = "NSX_VPC"
	vpcDefaultSnatIp   = "defaultSNATIP"
)

// GetVolumeID gets the volume ID from the PV that is bound to PVC by pvcName.
func GetVolumeID(ctx context.Context, client client.Client, pvcName string, namespace string) (string, error) {
	log := logger.GetLogger(ctx)
	// Get PVC by pvcName from namespace.
	pvc := &v1.PersistentVolumeClaim{}
	// TODO:
	// Enhancements to use informers instead of API server invocations is tracked here:
	// https://github.com/kubernetes-sigs/vsphere-csi-driver/issues/599
	err := client.Get(ctx, apitypes.NamespacedName{Name: pvcName, Namespace: namespace}, pvc)
	if err != nil {
		log.Errorf("failed to get PVC with volumename: %q on namespace: %q. Err: %+v",
			pvcName, namespace, err)
		return "", err
	}

	// Get PV by name.
	pv := &v1.PersistentVolume{}
	err = client.Get(ctx, apitypes.NamespacedName{Name: pvc.Spec.VolumeName, Namespace: ""}, pv)
	if err != nil {
		log.Errorf("failed to get PV with name: %q for PVC: %q. Err: %+v",
			pvc.Spec.VolumeName, pvcName, err)
		return "", err
	}
	return pv.Spec.CSI.VolumeHandle, nil
}

// GetTKGVMIP finds the external facing IP address of a TKG VM object from a
// given Supervisor Namespace based on the networking configuration (NSX-T or
// VDS).
func GetTKGVMIP(ctx context.Context, vmOperatorClient client.Client, dc dynamic.Interface,
	vmNamespace, vmName string, network_provider_type string) (string, error) {
	log := logger.GetLogger(ctx)
	log.Infof("Determining external IP Address of VM: %s/%s", vmNamespace, vmName)
	vmKey := apitypes.NamespacedName{
		Namespace: vmNamespace,
		Name:      vmName,
	}
	virtualMachineInstance, _, err := utils.GetVirtualMachineAllApiVersions(ctx,
		vmKey, vmOperatorClient)
	if err != nil {
		log.Errorf("failed to get virtualmachine %s/%s with error: %v", vmNamespace, vmName, err)
		return "", err
	}

	isFileVolumesWithVmServiceVmSupported := commonco.ContainerOrchestratorUtility.IsFSSEnabled(ctx,
		common.FileVolumesWithVmService)

	var networkNames []string
	for _, networkInterface := range virtualMachineInstance.Spec.Network.Interfaces {
		if !isFileVolumesWithVmServiceVmSupported {
			networkNames = append(networkNames, networkInterface.Network.Name)
		} else if networkInterface.Network.Name != "" {
			networkNames = append(networkNames, networkInterface.Network.Name)
		}
	}
	log.Debugf("VirtualMachine %s/%s is configured with networks %v", vmNamespace, vmName, networkNames)

	var ip string
	var exists bool
	if network_provider_type == NSXTNetworkProvider {
		for _, networkName := range networkNames {
			virtualNetworkInstance, err := dc.Resource(virtualNetworkGVR).Namespace(vmNamespace).Get(ctx,
				networkName, metav1.GetOptions{})
			if err != nil {
				return "", err
			}
			log.Debugf("Got VirtualNetwork instance %s/%s with annotations %v",
				vmNamespace, virtualNetworkInstance.GetName(), virtualNetworkInstance.GetAnnotations())
			ip, exists = virtualNetworkInstance.GetAnnotations()[snatIPAnnotation]
			// Pick the network interface which has the snatIPAnnotation
			if exists && ip != "" {
				break
			}
		}
		if ip == "" {
			if !isFileVolumesWithVmServiceVmSupported {
				return "", fmt.Errorf("failed to get SNAT IP annotation from VirtualMachine %s/%s",
					vmNamespace, vmName)
			}
			if len(networkNames) != 0 {
				// If networkNames for VirtualNetwork were found on the VM,
				// then some error happened in getting the SNAT IP from VirtualNetwork CR.
				return "", fmt.Errorf("failed to get SNAT IP annotation for VirtualMachine %s/%s "+
					"from VirtualNetwrok",
					vmNamespace, vmName)
			}
			// It is likely an NSX setup with VM service VMs.
			// For TKG service VMs, virtual network CR will always be present.
			ip, err = getSnatIpFromNamespaceNetworkInfo(ctx, dc, vmNamespace, vmName)
			if err != nil {
				log.Errorf("failed to get SNAT IP from NameSpaceNetworkInfo. Err %s", err)
				return "", fmt.Errorf("failed to get SNAT IP from NameSpaceNetworkInfo %s/%s",
					vmNamespace, vmName)
			}
			log.Infof("Obtained SNAT IP %s from NamespaceNetworkInfo for VirtualMachine %s/%s",
				ip, vmNamespace, vmName)
		}
	} else if network_provider_type == VDSNetworkProvider {
		ip = virtualMachineInstance.Status.Network.PrimaryIP4
		if ip == "" {
			ip = virtualMachineInstance.Status.Network.PrimaryIP6
			if ip == "" {
				return "", fmt.Errorf("vm.Status.Network.PrimaryIP6 & PrimaryIP4 is not populated for %s/%s",
					vmNamespace, vmName)
			}
		}
	} else {
		vpcName := vmNamespace
		networkInfoInstance, err := dc.Resource(networkInfoGVR).Namespace(vmNamespace).Get(ctx,
			vpcName, metav1.GetOptions{})
		if err != nil {
			return "", err
		}
		log.Debugf("Got NetworkInfo instance %s/%s", vmNamespace, networkInfoInstance.GetName())
		vpcs, found, err := unstructured.NestedSlice(networkInfoInstance.Object, "vpcs")
		if err != nil || !found || len(vpcs) == 0 {
			return "", fmt.Errorf("failed to get vpcs from networkinfo %s/%s with error: %v",
				vmNamespace, vmName, err)
		}

		vpc, ok := vpcs[0].(map[string]interface{})
		if !ok {
			return "", fmt.Errorf("failed to assert vpc to map[string]interface{} %s/%s",
				vmNamespace, vmName)
		}

		for key, value := range vpc {
			if key == vpcDefaultSnatIp {
				ip, ok = value.(string)
				if !ok {
					return "", fmt.Errorf("failed to cast key %s value to string", key)
				}
				break
			}

		}
		if ip == "" {
			return "", fmt.Errorf("spec.vpc.defaultSNATIP is not populated for "+
				"networkinfo %s/%s", vmNamespace, vmName)
		}
	}
	log.Infof("Found external IP Address %s for VirtualMachine %s/%s", ip, vmNamespace, vmName)
	return ip, nil
}

// getSnatIpFromNamespaceNetworkInfo finds VM's SNAT IP from the namespace's default NamespaceNetworkInfo CR.
func getSnatIpFromNamespaceNetworkInfo(ctx context.Context, dc dynamic.Interface,
	vmNamespace string, vmName string) (string, error) {
	log := logger.GetLogger(ctx)
	log.Infof("Determining SNAT IP for VM %s in namespace %s via NamespaceNetworkInfo CR", vmNamespace, vmName)

	namespaceNetworkInfoInstance, err := dc.Resource(namespaceNetworkInfoGVR).Namespace(vmNamespace).Get(ctx,
		vmNamespace, metav1.GetOptions{})
	if err != nil {
		return "", err
	}
	log.Debugf("Got namespaceNetworkInfo instance %s/%s", vmNamespace, namespaceNetworkInfoInstance.GetName())
	snatIP, found, err := unstructured.NestedString(namespaceNetworkInfoInstance.Object, "topology", "defaultEgressIP")
	if err != nil {
		return "", fmt.Errorf("failed to get defaultEgressIP from namespaceNetworkInfo %s/%s with error: %v",
			vmNamespace, vmName, err)

	}
	if !found {
		return "", fmt.Errorf("defaultEgressIP is not found on namespaceNetworkInfo %s/%s", vmNamespace, vmName)

	}
	if snatIP == "" {
		return "", fmt.Errorf("empty SNAT IP for VM %s on namespaceNetworkInfo instance %s/%s", vmName, vmNamespace,
			namespaceNetworkInfoInstance.GetName())
	}
	return snatIP, nil
}

// GetNetworkProvider reads the network-config configmap in Supervisor cluster.
// Returns the network provider as NSXT_CONTAINER_PLUGIN for NSX-T, or
// VSPHERE_NETWORK for VDS. Otherwise, returns an error, if network provider is
// not present in the configmap.
func GetNetworkProvider(ctx context.Context) (string, error) {
	log := logger.GetLogger(ctx)
	k8sclient, err := k8s.NewClient(ctx)
	if err != nil {
		log.Errorf("GetNetworkProvider: Creating Kubernetes client failed. Err: %v", err)
		return "", err
	}
	cm, err := k8sclient.CoreV1().ConfigMaps(kubeSystemNamespace).Get(ctx, wcpNetworkConfigMap, metav1.GetOptions{})
	if err != nil {
		return "", fmt.Errorf("failed to get config map %q in namespace %q. Err: %v",
			wcpNetworkConfigMap, kubeSystemNamespace, err)
	}

	for k, v := range cm.Data {
		if k == networkProvider {
			log.Debugf("Returning network provider value as: %q", v)
			return v, nil
		}
	}

	return "", fmt.Errorf("could not find network provider field in configmap %q in namespace %q",
		wcpNetworkConfigMap, kubeSystemNamespace)
}

// GetVCDatacenterFromConfig returns datacenter registered for each vCenter
func GetVCDatacentersFromConfig(cfg *config.Config) ([]string, string, error) {
	dcList := make([]string, 0)
	vcHost := ""
	if len(cfg.VirtualCenter) > 1 {
		return dcList, vcHost, fmt.Errorf("invalid configuration. Expected only 1 VC but found %d", len(cfg.VirtualCenter))
	}

	for host, value := range cfg.VirtualCenter {
		vcHost = host
		datacentersOnVcenter := strings.Split(value.Datacenters, ",")
		for _, dc := range datacentersOnVcenter {
			dcMoID := strings.TrimSpace(dc)
			if dcMoID != "" {
				dcList = append(dcList, dcMoID)
			}
		}
	}
	if len(dcList) == 0 {
		return dcList, vcHost, errors.New("unable get vCenter datacenters from vsphere config")
	}
	return dcList, vcHost, nil
}

// GetVMFromVcenter returns the VM from vCenter for the given nodeUUID.
func GetVMFromVcenter(ctx context.Context, nodeUUID string,
	configInfo config.ConfigurationInfo) (*cnsvsphere.VirtualMachine, error) {
	log := logger.GetLogger(ctx)

	dcList, err := GetDatacenterObjectList(ctx, configInfo)
	if err != nil {
		log.Errorf("failed to get datacenter for node: %s. Err: %+q", nodeUUID, err)
		return nil, err
	}

	for _, dc := range dcList {
		vm, err := dc.GetVirtualMachineByUUID(ctx, nodeUUID, true)
		if err != nil {
			log.Errorf("failed to find the VM with UUID: %s Err: %+v",
				nodeUUID, err)
			continue
		}
		return vm, nil
	}
	return nil, cnsvsphere.ErrVMNotFound
}

// GetDatacenterObjectList returns the list datacenters on the vCenter.
func GetDatacenterObjectList(ctx context.Context,
	configInfo config.ConfigurationInfo) ([]cnsvsphere.Datacenter, error) {
	log := logger.GetLogger(ctx)

	dcList, host, err := GetVCDatacentersFromConfig(configInfo.Cfg)
	if err != nil {
		log.Errorf("failed to find datacenter moref from config. Err: %s", err)
		return nil, err
	}

	datacenterList := make([]cnsvsphere.Datacenter, 0)
	for _, dcMoref := range dcList {
		vcenter, err := cnsvsphere.GetVirtualCenterInstance(ctx, &configInfo, false)
		if err != nil {
			log.Errorf("failed to get virtual center instance with error: %v", err)
			return nil, err
		}
		err = vcenter.Connect(ctx)
		if err != nil {
			log.Errorf("failed to connect to VC with error: %v", err)
			return nil, err
		}
		dc := &cnsvsphere.Datacenter{
			Datacenter: object.NewDatacenter(vcenter.Client.Client,
				vimtypes.ManagedObjectReference{
					Type:  "Datacenter",
					Value: dcMoref,
				}),
			VirtualCenterHost: host,
		}
		datacenterList = append(datacenterList, *dc)
	}
	return datacenterList, nil
}

// GetMaxWorkerThreads returns the maximum number of worker threads to be
// spawned by a controller to reconciler instances of a CRD. It reads the
// value from an environment variable identified by 'key'. If the environment
// variable is not set or has an invalid value, it returns the 'defaultVal'.
// The value of the environment variable should be a positive integer less
// than or equal to 'defaultVal'.
func GetMaxWorkerThreads(ctx context.Context, key string, defaultVal int) int {
	log := logger.GetLogger(ctx).With("field", key)
	workerThreads := defaultVal
	env := os.Getenv(key)
	if env == "" {
		log.Debugf("Environment variable is not set. Picking the default value %d",
			defaultVal)
		return workerThreads
	}

	val, err := strconv.Atoi(env)
	if err != nil {
		log.Warnf("Invalid value for environment variable: %q. Using default value %d",
			env, defaultVal)
		return workerThreads
	}

	switch {
	case val <= 0 || val > defaultVal:
		log.Warnf("Value %d for environment variable is invalid. Using default value %d",
			val, defaultVal)
	default:
		workerThreads = val
		log.Debugf("Maximum number of worker threads to reconcile is set to %d",
			workerThreads)
	}
	return workerThreads
}
