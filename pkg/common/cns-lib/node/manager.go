// Copyright 2018 VMware, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//    http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package node

import (
	"context"
	"errors"
	"sync"

	clientset "k8s.io/client-go/kubernetes"
	"k8s.io/klog"
	"sigs.k8s.io/vsphere-csi-driver/pkg/common/cns-lib/vsphere"
	k8s "sigs.k8s.io/vsphere-csi-driver/pkg/kubernetes"
)

var (
	// ErrNodeNotFound is returned when a node isn't found.
	ErrNodeNotFound = errors.New("node wasn't found")
	// ErrEmptyProviderId is returned when it is observed that provider id is not set on the kubernetes cluster
	ErrEmptyProviderId = errors.New("node with empty providerId present in the cluster")
)

// Manager provides functionality to manage nodes.
type Manager interface {
	// SetKubernetesClient sets kubernetes client for node manager
	SetKubernetesClient(clientset.Interface)
	// RegisterNode registers a node given its UUID, name.
	RegisterNode(nodeUUID string, nodeName string) error
	// DiscoverNode discovers a registered node given its UUID. This method
	// scans all virtual centers registered on the VirtualCenterManager for a
	// virtual machine with the given UUID.
	DiscoverNode(nodeUUID string) error
	// GetNode refreshes and returns the VirtualMachine for a registered node
	// given its UUID. If datacenter is present, GetNode will search within this
	// datacenter given its UUID. If not, it will search in all registered datacenters.
	GetNode(nodeUUID string, dc *vsphere.Datacenter) (*vsphere.VirtualMachine, error)
	// GetNodeByName refreshes and returns the VirtualMachine for a registered node
	// given its name.
	GetNodeByName(nodeName string) (*vsphere.VirtualMachine, error)
	// GetAllNodes refreshes and returns VirtualMachine for all registered
	// nodes. If nodes are added or removed concurrently, they may or may not be
	// reflected in the result of a call to this method.
	GetAllNodes() ([]*vsphere.VirtualMachine, error)
	// UnregisterNode unregisters a registered node given its name.
	UnregisterNode(nodeName string) error
}

// Metadata represents node metadata.
type Metadata interface{}

var (
	// managerInstance is a Manager singleton.
	managerInstance *defaultManager
	// onceForManager is used for initializing the Manager singleton.
	onceForManager sync.Once
)

// GetManager returns the Manager singleton.
func GetManager() Manager {
	onceForManager.Do(func() {
		klog.V(1).Info("Initializing node.defaultManager...")
		managerInstance = &defaultManager{
			nodeVMs: sync.Map{},
		}
		klog.V(1).Info("node.defaultManager initialized")
	})
	return managerInstance
}

// defaultManager holds node information and provides functionality around it.
type defaultManager struct {
	// nodeVMs maps node UUIDs to VirtualMachine objects.
	nodeVMs sync.Map
	// node name to node UUI map.
	nodeNameToUUID sync.Map
	// k8s client
	k8sClient clientset.Interface
}

// SetKubernetesClient sets specified kubernetes client to defaultManager.k8sClient
func (m *defaultManager) SetKubernetesClient(client clientset.Interface) {
	m.k8sClient = client
}

// RegisterNode registers a node with node manager using its UUID, name.
func (m *defaultManager) RegisterNode(nodeUUID string, nodeName string) error {
	m.nodeNameToUUID.Store(nodeName, nodeUUID)
	klog.V(2).Infof("Successfully registered node: %q with nodeUUID %q", nodeName, nodeUUID)
	err := m.DiscoverNode(nodeUUID)
	if err != nil {
		klog.Errorf("Failed to discover VM with uuid: %q for node: %q", nodeUUID, nodeName)
		return err
	}
	klog.V(2).Infof("Successfully discovered node: %q with nodeUUID %q", nodeName, nodeUUID)
	return nil
}

// DiscoverNode discovers a registered node given its UUID from vCenter.
// If node is not found in the vCenter for the given UUID, for ErrVMNotFound is returned to the caller
func (m *defaultManager) DiscoverNode(nodeUUID string) error {
	vm, err := vsphere.GetVirtualMachineByUUID(nodeUUID, false)
	if err != nil {
		klog.Errorf("Couldn't find VM instance with nodeUUID %s, failed to discover with err: %v", nodeUUID, err)
		return err
	}
	m.nodeVMs.Store(nodeUUID, vm)
	klog.V(2).Infof("Successfully discovered node with nodeUUID %s in vm %v", nodeUUID, vm)
	return nil
}

// GetNodeByName refreshes and returns the VirtualMachine for a registered node
// given its name.
func (m *defaultManager) GetNodeByName(nodeName string) (*vsphere.VirtualMachine, error) {
	nodeUUID, found := m.nodeNameToUUID.Load(nodeName)
	if !found {
		klog.Errorf("Node not found with nodeName %s", nodeName)
		return nil, ErrNodeNotFound
	}
	if nodeUUID != nil && nodeUUID.(string) != "" {
		return m.GetNode(nodeUUID.(string), nil)
	}
	klog.V(2).Infof("Empty nodeUUID observed in cache for the node: %q", nodeName)
	k8snodeUUID, err := k8s.GetNodeVMUUID(m.k8sClient, nodeName)
	if err != nil {
		klog.Errorf("Failed to get providerId from node: %q. Err: %v", nodeName, err)
		return nil, err
	}
	m.nodeNameToUUID.Store(nodeName, k8snodeUUID)
	return m.GetNode(k8snodeUUID, nil)

}

// GetNode refreshes and returns the VirtualMachine for a registered node
// given its UUID
func (m *defaultManager) GetNode(nodeUUID string, dc *vsphere.Datacenter) (*vsphere.VirtualMachine, error) {
	vmInf, discovered := m.nodeVMs.Load(nodeUUID)
	if !discovered {
		klog.V(2).Infof("Node hasn't been discovered yet with nodeUUID %s", nodeUUID)
		var vm *vsphere.VirtualMachine
		var err error
		if dc != nil {
			vm, err = dc.GetVirtualMachineByUUID(context.TODO(), nodeUUID, false)
			if err != nil {
				klog.Errorf("Failed to find node with nodeUUID %s on datacenter: %+v with err: %v", nodeUUID, dc, err)
				return nil, err
			}
			m.nodeVMs.Store(nodeUUID, vm)
		} else {
			if err = m.DiscoverNode(nodeUUID); err != nil {
				klog.Errorf("Failed to discover node with nodeUUID %s with err: %v", nodeUUID, err)
				return nil, err
			}

			vmInf, _ = m.nodeVMs.Load(nodeUUID)
			vm = vmInf.(*vsphere.VirtualMachine)
		}
		klog.V(2).Infof("Node was successfully discovered with nodeUUID %s in vm %v", nodeUUID, vm)
		return vm, nil
	}

	vm := vmInf.(*vsphere.VirtualMachine)
	klog.V(1).Infof("Renewing virtual machine %v with nodeUUID %q", vm, nodeUUID)

	if err := vm.Renew(true); err != nil {
		klog.Errorf("Failed to renew VM %v with nodeUUID %q with err: %v", vm, nodeUUID, err)
		return nil, err
	}

	klog.V(4).Infof("VM %v was successfully renewed with nodeUUID %q", vm, nodeUUID)
	return vm, nil
}

// GetAllNodes refreshes and returns VirtualMachine for all registered nodes.
func (m *defaultManager) GetAllNodes() ([]*vsphere.VirtualMachine, error) {
	var vms []*vsphere.VirtualMachine
	var err error
	reconnectedHosts := make(map[string]bool)

	m.nodeNameToUUID.Range(func(nodeName, nodeUUID interface{}) bool {
		if nodeName != nil && nodeUUID != nil && nodeUUID.(string) == "" {
			klog.V(2).Infof("Empty node UUID observed for the node: %q", nodeName)
			k8snodeUUID, err := k8s.GetNodeVMUUID(m.k8sClient, nodeName.(string))
			if err != nil {
				klog.Errorf("Failed to get providerId from node: %q. Err: %v", nodeName, err)
				return true
			}
			if k8snodeUUID == "" {
				klog.Errorf("Node: %q with empty providerId found in the cluster. aborting get all nodes", nodeName)
				err = ErrEmptyProviderId
				return true
			}
			m.nodeNameToUUID.Store(nodeName, k8snodeUUID)
			return false
		}
		return true
	})

	if err != nil {
		return nil, err
	}
	m.nodeVMs.Range(func(nodeUUIDInf, vmInf interface{}) bool {
		// If an entry was concurrently deleted from vm, Range could
		// possibly return a nil value for that key.
		// See https://golang.org/pkg/sync/#Map.Range for more info.
		if vmInf == nil {
			klog.Warningf("VM instance was nil, ignoring with nodeUUID %v", nodeUUIDInf)
			return true
		}

		nodeUUID := nodeUUIDInf.(string)
		vm := vmInf.(*vsphere.VirtualMachine)

		if reconnectedHosts[vm.VirtualCenterHost] {
			klog.V(3).Infof("Renewing VM %v, no new connection needed: nodeUUID %s", vm, nodeUUID)
			err = vm.Renew(false)
		} else {
			klog.V(3).Infof("Renewing VM %v with new connection: nodeUUID %s", vm, nodeUUID)
			err = vm.Renew(true)
			reconnectedHosts[vm.VirtualCenterHost] = true
		}

		if err != nil {
			klog.Errorf("Failed to renew VM %v with nodeUUID %s, aborting get all nodes", vm, nodeUUID)
			return false
		}

		klog.V(3).Infof("Updated VM %v for node with nodeUUID %s", vm, nodeUUID)
		vms = append(vms, vm)
		return true
	})

	if err != nil {
		return nil, err
	}
	return vms, nil
}

// UnregisterNode unregisters a registered node given its name.
func (m *defaultManager) UnregisterNode(nodeName string) error {
	nodeUUID, found := m.nodeNameToUUID.Load(nodeName)
	if !found {
		klog.Errorf("Node wasn't found, failed to unregister node: %q  err: %v", nodeName)
		return ErrNodeNotFound
	}
	m.nodeNameToUUID.Delete(nodeName)
	m.nodeVMs.Delete(nodeUUID)
	klog.V(2).Infof("Successfully unregistered node with nodeName %s", nodeName)
	return nil
}
