/*
Copyright 2021 The Kubernetes Authors.

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
	"fmt"

	vmoperatortypes "github.com/vmware-tanzu/vm-operator-api/api/v1alpha1"
	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	apitypes "k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/dynamic"
	"sigs.k8s.io/controller-runtime/pkg/client"
	k8s "sigs.k8s.io/vsphere-csi-driver/v2/pkg/kubernetes"

	"sigs.k8s.io/vsphere-csi-driver/v2/pkg/csi/service/logger"
)

var networkInterfaceGVR = schema.GroupVersionResource{
	Group:    "netoperator.vmware.com",
	Version:  "v1alpha1",
	Resource: "networkinterfaces",
}

var virtualNetworkGVR = schema.GroupVersionResource{
	Group:    "vmware.com",
	Version:  "v1alpha1",
	Resource: "virtualnetworks",
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
	vmNamespace, vmName string, nsxConfiguration bool) (string, error) {
	log := logger.GetLogger(ctx)
	log.Infof("Determining external IP Address of VM: %s/%s", vmNamespace, vmName)
	virtualMachineInstance := &vmoperatortypes.VirtualMachine{}
	vmKey := apitypes.NamespacedName{
		Namespace: vmNamespace,
		Name:      vmName,
	}
	err := vmOperatorClient.Get(ctx, vmKey, virtualMachineInstance)
	if err != nil {
		log.Errorf("failed to get virtualmachine %s/%s with error: %v", vmNamespace, vmName, err)
		return "", err
	}

	var networkName string
	for _, networkInterface := range virtualMachineInstance.Spec.NetworkInterfaces {
		// The assumption is that a TKG VM will have only a single network interface.
		// This logic needs to be revisited when multiple network interface support
		// is added.
		networkName = networkInterface.NetworkName
	}
	log.Debugf("VirtualMachine %s/%s is configured with network %s", vmNamespace, vmName, networkName)

	var ip string
	var exists bool
	if nsxConfiguration {
		virtualNetworkInstance, err := dc.Resource(virtualNetworkGVR).Namespace(vmNamespace).Get(ctx,
			networkName, metav1.GetOptions{})
		if err != nil {
			return "", err
		}
		log.Debugf("Got VirtualNetwork instance %s/%s with annotations %v",
			vmNamespace, virtualNetworkInstance.GetName(), virtualNetworkInstance.GetAnnotations())
		ip, exists = virtualNetworkInstance.GetAnnotations()[snatIPAnnotation]
		if !exists {
			return "", fmt.Errorf("failed to get SNAT IP annotation from VirtualMachine %s/%s", vmNamespace, vmName)
		}
	} else {
		networkInterfaceName := networkName + "-" + vmName
		networkInterfaceInstance, err := dc.Resource(networkInterfaceGVR).Namespace(vmNamespace).Get(ctx,
			networkInterfaceName, metav1.GetOptions{})
		if err != nil {
			return "", err
		}
		log.Debugf("Got NetworkInterface instance %+v", networkInterfaceInstance)
		ipConfigs, exists, err := unstructured.NestedSlice(networkInterfaceInstance.Object, "status", "ipConfigs")
		if err != nil {
			return "", err
		}
		if !exists {
			return "", fmt.Errorf("status.ipConfigs does not exist in NetworkInterface instance %s/%s",
				vmNamespace, networkInterfaceName)
		}
		if len(ipConfigs) == 0 {
			return "", fmt.Errorf("length of status.ipConfigs should be greater than one for NetworkInterface instance %s/%s",
				vmNamespace, networkInterfaceName)
		}
		// Assuming only a single ipConfig is supported per VM. Revisit this logic
		// when multiple ipConfigs are supported.
		ip, exists, err = unstructured.NestedString(ipConfigs[0].(map[string]interface{}), "ip")
		if err != nil {
			return "", err
		}
		if !exists {
			return "", fmt.Errorf("status.ipConfigs.ip does not exist in NetworkInterface instance %s/%s",
				vmNamespace, networkInterfaceName)
		}
	}
	log.Infof("Found external IP Address %s for VirtualMachine %s/%s", ip, vmNamespace, vmName)
	return ip, nil
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
