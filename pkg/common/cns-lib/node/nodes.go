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
	"fmt"

	storagev1 "k8s.io/api/storage/v1"

	cnsvsphere "sigs.k8s.io/vsphere-csi-driver/v3/pkg/common/cns-lib/vsphere"
	"sigs.k8s.io/vsphere-csi-driver/v3/pkg/csi/service/logger"
	k8s "sigs.k8s.io/vsphere-csi-driver/v3/pkg/kubernetes"
)

// Nodes comprises cns node manager and kubernetes informer.
type Nodes struct {
	cnsNodeManager Manager
	informMgr      *k8s.InformerManager
}

// Initialize helps initialize node manager and node informer manager.
func (nodes *Nodes) Initialize(ctx context.Context) error {
	nodes.cnsNodeManager = GetManager(ctx)
	k8sclient, err := k8s.NewClient(ctx)
	if err != nil {
		log := logger.GetLogger(ctx)
		log.Errorf("Creating Kubernetes client failed. Err: %v", err)
		return err
	}
	nodes.cnsNodeManager.SetKubernetesClient(k8sclient)
	nodes.informMgr = k8s.NewInformer(ctx, k8sclient, true)
	err = nodes.informMgr.AddCSINodeListener(ctx, nodes.csiNodeAdd,
		nodes.csiNodeUpdate, nodes.csiNodeDelete)
	if err != nil {
		return fmt.Errorf("failed to listen on CSINodes. Error: %v", err)
	}
	nodes.informMgr.Listen()
	return nil
}

func (nodes *Nodes) csiNodeAdd(obj interface{}) {
	ctx, log := logger.GetNewContextWithLogger()
	csiNode, ok := obj.(*storagev1.CSINode)
	if csiNode == nil || !ok {
		log.Warnf("csiNodeAdd: unrecognized object %+v", obj)
		return
	}
	nodeName := csiNode.Name
	nodeUUID := k8s.GetNodeIdFromCSINode(csiNode)
	if nodeUUID == "" {
		log.Warnf("csiNodeAdd: nodeId is either empty. CSINode object: %v", csiNode)
		return
	}
	err := nodes.cnsNodeManager.RegisterNode(ctx, nodeUUID, nodeName)
	if err != nil {
		// This block is required for TKGi platform where worker nodes are not upgraded along with control plane nodes.
		log.Errorf("csiNodeAdd: failed to register node vm using node ID on CSINode object: %v", csiNode)
		node, err := nodes.cnsNodeManager.GetK8sNode(ctx, nodeName)
		if node == nil || err != nil {
			log.Errorf("csiNodeAdd: failed to get k8s node object for node name: %v", nodeName)
			return
		}
		log.Infof("csiNodeAdd: registering node using providerID on the node object")
		nodeUUID = cnsvsphere.GetUUIDFromProviderID(node.Spec.ProviderID)
		err = nodes.cnsNodeManager.RegisterNode(ctx, nodeUUID, nodeName)
		if err != nil {
			log.Errorf("csiNodeAdd: failed to register node using provider ID on Node object: %v", node)
		}
	}
}

func (nodes *Nodes) csiNodeUpdate(oldObj interface{}, newObj interface{}) {
	ctx, log := logger.GetNewContextWithLogger()
	newCSINode, ok := newObj.(*storagev1.CSINode)
	if !ok {
		log.Warnf("csiNodeUpdate: unrecognized object newObj %[1]T%+[1]v", newObj)
		return
	}
	oldCSINode, ok := oldObj.(*storagev1.CSINode)
	if !ok {
		log.Warnf("csiNodeUpdate: unrecognized object oldObj %[1]T%+[1]v", oldObj)
		return
	}
	nodeName := newCSINode.Name
	newNodeId := k8s.GetNodeIdFromCSINode(newCSINode)
	oldNodeId := k8s.GetNodeIdFromCSINode(oldCSINode)
	if oldNodeId != newNodeId && newNodeId != "" {
		log.Infof("csiNodeUpdate: Observed node UUID change from "+
			"%q to %q for the node: %q", oldNodeId, newNodeId, nodeName)
		newNodeUuid := newNodeId
		err := nodes.cnsNodeManager.RegisterNode(ctx, newNodeUuid, nodeName)
		if err != nil {
			log.Warnf("csiNodeUpdate: Failed to register node:%q. err=%v",
				nodeName, err)
		}
	}
}

func (nodes *Nodes) csiNodeDelete(obj interface{}) {
	ctx, log := logger.GetNewContextWithLogger()
	csiNode, ok := obj.(*storagev1.CSINode)
	if csiNode == nil || !ok {
		log.Warnf("csiNodeDelete: unrecognized object %+v", obj)
		return
	}
	nodeName := csiNode.Name
	err := nodes.cnsNodeManager.UnregisterNode(ctx, nodeName)
	if err != nil {
		log.Warnf("csiNodeDelete: failed to unregister node:%q. err=%v", nodeName, err)
	}
}

// GetNodeVMByNameAndUpdateCache returns VirtualMachine object for given nodeName.
// This is called by ControllerPublishVolume and ControllerUnpublishVolume
// to perform attach and detach operations.
func (nodes *Nodes) GetNodeVMByNameAndUpdateCache(ctx context.Context, nodeName string) (
	*cnsvsphere.VirtualMachine, error) {
	return nodes.cnsNodeManager.GetNodeVMByNameAndUpdateCache(ctx, nodeName)
}

// GetNodeVMByNameOrUUID returns VirtualMachine object for given nodeName
// This function can be called either using nodeName or nodeUID.
func (nodes *Nodes) GetNodeVMByNameOrUUID(
	ctx context.Context, nodeNameOrUUID string) (*cnsvsphere.VirtualMachine, error) {
	return nodes.cnsNodeManager.GetNodeVMByNameOrUUID(ctx, nodeNameOrUUID)
}

// GetNodeNameByUUID fetches the name of the node given the VM UUID.
func (nodes *Nodes) GetNodeNameByUUID(ctx context.Context, nodeUUID string) (
	string, error) {
	return nodes.cnsNodeManager.GetNodeNameByUUID(ctx, nodeUUID)
}

// GetNodeVMByUuid returns VirtualMachine object for given nodeUuid.
// This is called by ControllerPublishVolume and ControllerUnpublishVolume
// to perform attach and detach operations.
func (nodes *Nodes) GetNodeVMByUuid(ctx context.Context, nodeUuid string) (*cnsvsphere.VirtualMachine, error) {
	return nodes.cnsNodeManager.GetNodeVMByUuid(ctx, nodeUuid)
}

// GetAllNodes returns VirtualMachine objects for all registered nodes in cluster.
func (nodes *Nodes) GetAllNodes(ctx context.Context) (
	[]*cnsvsphere.VirtualMachine, error) {
	return nodes.cnsNodeManager.GetAllNodes(ctx)
}

// GetAllNodesByVC returns VirtualMachine objects for all registered nodes for a particular VC.
func (nodes *Nodes) GetAllNodesByVC(ctx context.Context, vcHost string) ([]*cnsvsphere.VirtualMachine, error) {
	return nodes.cnsNodeManager.GetAllNodesByVC(ctx, vcHost)
}

// GetSharedDatastoresInK8SCluster returns list of DatastoreInfo objects for
// datastores accessible to all kubernetes nodes in the cluster.
func (nodes *Nodes) GetSharedDatastoresInK8SCluster(ctx context.Context) (
	[]*cnsvsphere.DatastoreInfo, error) {
	log := logger.GetLogger(ctx)
	nodeVMs, err := nodes.cnsNodeManager.GetAllNodes(ctx)
	if err != nil {
		log.Errorf("failed to get Nodes from nodeManager with err %+v", err)
		return nil, err
	}
	if len(nodeVMs) == 0 {
		errMsg := "empty List of Node VMs returned from nodeManager"
		log.Errorf(errMsg)
		return make([]*cnsvsphere.DatastoreInfo, 0), fmt.Errorf("%s", errMsg)
	}
	sharedDatastores, err := cnsvsphere.GetSharedDatastoresForVMs(ctx, nodeVMs)
	if err != nil {
		log.Errorf("failed to get shared datastores for node VMs. Err: %+v", err)
		return nil, err
	}
	log.Debugf("sharedDatastores : %+v", sharedDatastores)
	return sharedDatastores, nil
}
