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

package node

import (
	"context"
	"errors"
	"sync"

	"sigs.k8s.io/vsphere-csi-driver/v2/pkg/csi/service/logger"

	clientset "k8s.io/client-go/kubernetes"
	"sigs.k8s.io/vsphere-csi-driver/v2/pkg/common/cns-lib/vsphere"
	k8s "sigs.k8s.io/vsphere-csi-driver/v2/pkg/kubernetes"
)

var (
	// ErrNodeNotFound is returned when a node isn't found.
	ErrNodeNotFound = errors.New("node wasn't found")
)

// Manager provides functionality to manage nodes.
type Manager interface {
	// SetKubernetesClient sets kubernetes client for node manager.
	SetKubernetesClient(client clientset.Interface)
	// RegisterNode registers a node given its UUID, name.
	RegisterNode(ctx context.Context, nodeUUID string, nodeName string) error
	// DiscoverNode discovers a registered node given its UUID. This method
	// scans all virtual centers registered on the VirtualCenterManager for a
	// virtual machine with the given UUID.
	DiscoverNode(ctx context.Context, nodeUUID string) error
	// GetNode refreshes and returns the VirtualMachine for a registered node
	// given its UUID. If datacenter is present, GetNode will search within this
	// datacenter given its UUID. If not, it will search in all registered
	// datacenters.
	GetNode(ctx context.Context, nodeUUID string, dc *vsphere.Datacenter) (*vsphere.VirtualMachine, error)
	// GetNodeByName refreshes and returns the VirtualMachine for a registered
	// node given its name.
	GetNodeByName(ctx context.Context, nodeName string) (*vsphere.VirtualMachine, error)
	// GetAllNodes refreshes and returns VirtualMachine for all registered
	// nodes. If nodes are added or removed concurrently, they may or may not be
	// reflected in the result of a call to this method.
	GetAllNodes(ctx context.Context) ([]*vsphere.VirtualMachine, error)
	// UnregisterNode unregisters a registered node given its name.
	UnregisterNode(ctx context.Context, nodeName string) error
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
func GetManager(ctx context.Context) Manager {
	onceForManager.Do(func() {
		log := logger.GetLogger(ctx)
		log.Info("Initializing node.defaultManager...")
		managerInstance = &defaultManager{
			nodeVMs: sync.Map{},
		}
		log.Info("node.defaultManager initialized")
	})
	return managerInstance
}

// defaultManager holds node information and provides functionality around it.
type defaultManager struct {
	// nodeVMs maps node UUIDs to VirtualMachine objects.
	nodeVMs sync.Map
	// node name to node UUI map.
	nodeNameToUUID sync.Map
	// k8s client.
	k8sClient clientset.Interface
}

// SetKubernetesClient sets specified kubernetes client to defaultManager.k8sClient
func (m *defaultManager) SetKubernetesClient(client clientset.Interface) {
	m.k8sClient = client
}

// RegisterNode registers a node with node manager using its UUID, name.
func (m *defaultManager) RegisterNode(ctx context.Context, nodeUUID string, nodeName string) error {
	log := logger.GetLogger(ctx)
	m.nodeNameToUUID.Store(nodeName, nodeUUID)
	log.Infof("Successfully registered node: %q with nodeUUID %q", nodeName, nodeUUID)
	err := m.DiscoverNode(ctx, nodeUUID)
	if err != nil {
		log.Errorf("failed to discover VM with uuid: %q for node: %q", nodeUUID, nodeName)
		return err
	}
	log.Infof("Successfully discovered node: %q with nodeUUID %q", nodeName, nodeUUID)
	return nil
}

// DiscoverNode discovers a registered node given its UUID from vCenter.
// If node is not found in the vCenter for the given UUID, for ErrVMNotFound
// is returned to the caller.
func (m *defaultManager) DiscoverNode(ctx context.Context, nodeUUID string) error {
	log := logger.GetLogger(ctx)
	vm, err := vsphere.GetVirtualMachineByUUID(ctx, nodeUUID, false)
	if err != nil {
		log.Errorf("Couldn't find VM instance with nodeUUID %s, failed to discover with err: %v", nodeUUID, err)
		return err
	}
	m.nodeVMs.Store(nodeUUID, vm)
	log.Infof("Successfully discovered node with nodeUUID %s in vm %v", nodeUUID, vm)
	return nil
}

// GetNodeByName refreshes and returns the VirtualMachine for a registered node
// given its name.
func (m *defaultManager) GetNodeByName(ctx context.Context, nodeName string) (*vsphere.VirtualMachine, error) {
	log := logger.GetLogger(ctx)
	nodeUUID, found := m.nodeNameToUUID.Load(nodeName)
	if !found {
		log.Errorf("Node not found with nodeName %s", nodeName)
		return nil, ErrNodeNotFound
	}
	if nodeUUID != nil && nodeUUID.(string) != "" {
		return m.GetNode(ctx, nodeUUID.(string), nil)
	}
	log.Infof("Empty nodeUUID observed in cache for the node: %q", nodeName)
	k8snodeUUID, err := k8s.GetNodeVMUUID(ctx, m.k8sClient, nodeName)
	if err != nil {
		log.Errorf("failed to get providerId from node: %q. Err: %v", nodeName, err)
		return nil, err
	}
	m.nodeNameToUUID.Store(nodeName, k8snodeUUID)
	return m.GetNode(ctx, k8snodeUUID, nil)

}

// GetNode refreshes and returns the VirtualMachine for a registered node
// given its UUID.
func (m *defaultManager) GetNode(ctx context.Context,
	nodeUUID string, dc *vsphere.Datacenter) (*vsphere.VirtualMachine, error) {
	log := logger.GetLogger(ctx)
	vmInf, discovered := m.nodeVMs.Load(nodeUUID)
	if !discovered {
		log.Infof("Node hasn't been discovered yet with nodeUUID %s", nodeUUID)
		var vm *vsphere.VirtualMachine
		var err error
		if dc != nil {
			vm, err = dc.GetVirtualMachineByUUID(context.TODO(), nodeUUID, false)
			if err != nil {
				log.Errorf("failed to find node with nodeUUID %s on datacenter: %+v with err: %v", nodeUUID, dc, err)
				return nil, err
			}
			m.nodeVMs.Store(nodeUUID, vm)
		} else {
			if err = m.DiscoverNode(ctx, nodeUUID); err != nil {
				log.Errorf("failed to discover node with nodeUUID %s with err: %v", nodeUUID, err)
				return nil, err
			}

			vmInf, _ = m.nodeVMs.Load(nodeUUID)
			vm = vmInf.(*vsphere.VirtualMachine)
		}
		log.Infof("Node was successfully discovered with nodeUUID %s in vm %v", nodeUUID, vm)
		return vm, nil
	}

	vm := vmInf.(*vsphere.VirtualMachine)
	log.Debugf("Renewing virtual machine %v with nodeUUID %q", vm, nodeUUID)

	if err := vm.Renew(ctx, true); err != nil {
		log.Errorf("failed to renew VM %v with nodeUUID %q with err: %v", vm, nodeUUID, err)
		return nil, err
	}

	log.Debugf("VM %v was successfully renewed with nodeUUID %q", vm, nodeUUID)
	return vm, nil
}

// GetAllNodes refreshes and returns VirtualMachine for all registered nodes.
func (m *defaultManager) GetAllNodes(ctx context.Context) ([]*vsphere.VirtualMachine, error) {
	log := logger.GetLogger(ctx)
	var vms []*vsphere.VirtualMachine
	var err error
	reconnectedHosts := make(map[string]bool)

	m.nodeNameToUUID.Range(func(nodeName, nodeUUID interface{}) bool {
		if nodeName != nil && nodeUUID != nil && nodeUUID.(string) == "" {
			log.Infof("Empty node UUID observed for the node: %q", nodeName)
			k8snodeUUID, err := k8s.GetNodeVMUUID(ctx, m.k8sClient, nodeName.(string))
			if err != nil {
				log.Errorf("failed to get providerId from node: %q. Err: %v", nodeName, err)
				return true
			}
			if k8snodeUUID == "" {
				log.Errorf("Node: %q with empty providerId found in the cluster. aborting get all nodes", nodeName)
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
			log.Warnf("VM instance was nil, ignoring with nodeUUID %v", nodeUUIDInf)
			return true
		}

		nodeUUID := nodeUUIDInf.(string)
		vm := vmInf.(*vsphere.VirtualMachine)

		if reconnectedHosts[vm.VirtualCenterHost] {
			log.Debugf("Renewing VM %v, no new connection needed: nodeUUID %s", vm, nodeUUID)
			err = vm.Renew(ctx, false)
		} else {
			log.Debugf("Renewing VM %v with new connection: nodeUUID %s", vm, nodeUUID)
			err = vm.Renew(ctx, true)
			reconnectedHosts[vm.VirtualCenterHost] = true
		}

		if err != nil {
			log.Errorf("failed to renew VM %v with nodeUUID %s, aborting get all nodes", vm, nodeUUID)
			return false
		}

		log.Debugf("Updated VM %v for node with nodeUUID %s", vm, nodeUUID)
		vms = append(vms, vm)
		return true
	})

	if err != nil {
		return nil, err
	}
	return vms, nil
}

// UnregisterNode unregisters a registered node given its name.
func (m *defaultManager) UnregisterNode(ctx context.Context, nodeName string) error {
	log := logger.GetLogger(ctx)
	nodeUUID, found := m.nodeNameToUUID.Load(nodeName)
	if !found {
		log.Errorf("Node wasn't found, failed to unregister node: %q  err: %v", nodeName)
		return ErrNodeNotFound
	}
	m.nodeNameToUUID.Delete(nodeName)
	m.nodeVMs.Delete(nodeUUID)
	log.Infof("Successfully unregistered node with nodeName %s", nodeName)
	return nil
}
