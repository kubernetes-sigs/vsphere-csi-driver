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

	v1 "k8s.io/api/core/v1"
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
// If useNodeUuid is set, an informer on K8s CSINode is created.
// if not, an informer on K8s Node API object is created.
func (nodes *Nodes) Initialize(ctx context.Context, useNodeUuid bool) error {
	nodes.cnsNodeManager = GetManager(ctx)
	nodes.cnsNodeManager.SetUseNodeUuid(useNodeUuid)
	k8sclient, err := k8s.NewClient(ctx)
	if err != nil {
		log := logger.GetLogger(ctx)
		log.Errorf("Creating Kubernetes client failed. Err: %v", err)
		return err
	}
	nodes.cnsNodeManager.SetKubernetesClient(k8sclient)
	nodes.informMgr = k8s.NewInformer(ctx, k8sclient, true)
	if useNodeUuid {
		nodes.informMgr.AddCSINodeListener(nodes.csiNodeAdd,
			nodes.csiNodeUpdate, nodes.csiNodeDelete)
	} else {
		nodes.informMgr.AddNodeListener(nodes.nodeAdd,
			nodes.nodeUpdate, nodes.nodeDelete)
	}
	nodes.informMgr.Listen()
	return nil
}

func (nodes *Nodes) nodeAdd(obj interface{}) {
	ctx, log := logger.GetNewContextWithLogger()
	node, ok := obj.(*v1.Node)
	if node == nil || !ok {
		log.Warnf("nodeAdd: unrecognized object %+v", obj)
		return
	}
	err := nodes.cnsNodeManager.RegisterNode(ctx,
		cnsvsphere.GetUUIDFromProviderID(node.Spec.ProviderID), node.Name)
	if err != nil {
		log.Warnf("failed to register node:%q. err=%v", node.Name, err)
	}
}

func (nodes *Nodes) nodeUpdate(oldObj interface{}, newObj interface{}) {
	ctx, log := logger.GetNewContextWithLogger()
	newNode, ok := newObj.(*v1.Node)
	if !ok {
		log.Warnf("nodeUpdate: unrecognized object newObj %[1]T%+[1]v", newObj)
		return
	}
	oldNode, ok := oldObj.(*v1.Node)
	if !ok {
		log.Warnf("nodeUpdate: unrecognized object oldObj %[1]T%+[1]v", oldObj)
		return
	}
	if oldNode.Spec.ProviderID != newNode.Spec.ProviderID {
		log.Infof("nodeUpdate: Observed ProviderID change from %q to %q for the node: %q",
			oldNode.Spec.ProviderID, newNode.Spec.ProviderID, newNode.Name)

		err := nodes.cnsNodeManager.RegisterNode(ctx,
			cnsvsphere.GetUUIDFromProviderID(newNode.Spec.ProviderID), newNode.Name)
		if err != nil {
			log.Warnf("nodeUpdate: Failed to register node:%q. err=%v", newNode.Name, err)
		}
	}
}

func (nodes *Nodes) nodeDelete(obj interface{}) {
	ctx, log := logger.GetNewContextWithLogger()
	node, ok := obj.(*v1.Node)
	if node == nil || !ok {
		log.Warnf("nodeDelete: unrecognized object %+v", obj)
		return
	}
	err := nodes.cnsNodeManager.UnregisterNode(ctx, node.Name)
	if err != nil {
		log.Warnf("failed to unregister node:%q. err=%v", node.Name, err)
	}
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

// GetNodeByName returns VirtualMachine object for given nodeName.
// This is called by ControllerPublishVolume and ControllerUnpublishVolume
// to perform attach and detach operations.
func (nodes *Nodes) GetNodeByName(ctx context.Context, nodeName string) (
	*cnsvsphere.VirtualMachine, error) {
	return nodes.cnsNodeManager.GetNodeByName(ctx, nodeName)
}

// GetNodeNameByUUID fetches the name of the node given the VM UUID.
func (nodes *Nodes) GetNodeNameByUUID(ctx context.Context, nodeUUID string) (
	string, error) {
	return nodes.cnsNodeManager.GetNodeNameByUUID(ctx, nodeUUID)
}

// GetNodeByUuid returns VirtualMachine object for given nodeUuid.
// This is called by ControllerPublishVolume and ControllerUnpublishVolume
// to perform attach and detach operations.
func (nodes *Nodes) GetNodeByUuid(ctx context.Context, nodeUuid string) (*cnsvsphere.VirtualMachine, error) {
	return nodes.cnsNodeManager.GetNode(ctx, nodeUuid, nil)
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
		return make([]*cnsvsphere.DatastoreInfo, 0), fmt.Errorf(errMsg)
	}
	sharedDatastores, err := cnsvsphere.GetSharedDatastoresForVMs(ctx, nodeVMs)
	if err != nil {
		log.Errorf("failed to get shared datastores for node VMs. Err: %+v", err)
		return nil, err
	}
	log.Debugf("sharedDatastores : %+v", sharedDatastores)
	return sharedDatastores, nil
}
