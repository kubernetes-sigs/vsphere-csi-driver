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

package vanilla

import (
	"context"
	"fmt"

	cnsnode "sigs.k8s.io/vsphere-csi-driver/pkg/common/cns-lib/node"
	cnsvsphere "sigs.k8s.io/vsphere-csi-driver/pkg/common/cns-lib/vsphere"
	"sigs.k8s.io/vsphere-csi-driver/pkg/csi/service/common"
	"sigs.k8s.io/vsphere-csi-driver/pkg/csi/service/logger"
	csitypes "sigs.k8s.io/vsphere-csi-driver/pkg/csi/types"
	k8s "sigs.k8s.io/vsphere-csi-driver/pkg/kubernetes"

	"github.com/container-storage-interface/spec/lib/go/csi"
	v1 "k8s.io/api/core/v1"
)

// Nodes is the type comprising cns node manager and kubernetes informer
type Nodes struct {
	cnsNodeManager cnsnode.Manager
	informMgr      *k8s.InformerManager
}

// Initialize helps initialize node manager and node informer manager
func (nodes *Nodes) Initialize(ctx context.Context) error {
	log := logger.GetLogger(ctx)
	nodes.cnsNodeManager = cnsnode.GetManager(ctx)
	// Create the kubernetes client
	k8sclient, err := k8s.NewClient(ctx)
	if err != nil {
		log.Errorf("Creating Kubernetes client failed. Err: %v", err)
		return err
	}
	nodes.cnsNodeManager.SetKubernetesClient(k8sclient)
	nodes.informMgr = k8s.NewInformer(k8sclient)
	nodes.informMgr.AddNodeListener(nodes.nodeAdd, nodes.nodeUpdate, nodes.nodeDelete)
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
	err := nodes.cnsNodeManager.RegisterNode(ctx, common.GetUUIDFromProviderID(node.Spec.ProviderID), node.Name)
	if err != nil {
		log.Warnf("Failed to register node:%q. err=%v", node.Name, err)
	}
}

func (nodes *Nodes) nodeUpdate(oldObj interface{}, newObj interface{}) {
	ctx, log := logger.GetNewContextWithLogger()
	log = logger.GetLogger(ctx)
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
		log.Infof("nodeUpdate: Observed ProviderID change from %q to %q for the node: %q", oldNode.Spec.ProviderID, newNode.Spec.ProviderID, newNode.Name)

		err := nodes.cnsNodeManager.RegisterNode(ctx, common.GetUUIDFromProviderID(newNode.Spec.ProviderID), newNode.Name)
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
		log.Warnf("Failed to unregister node:%q. err=%v", node.Name, err)
	}
}

// GetNodeByName returns VirtualMachine object for given nodeName
// This is called by ControllerPublishVolume and ControllerUnpublishVolume to perform attach and detach operations.
func (nodes *Nodes) GetNodeByName(ctx context.Context, nodeName string) (*cnsvsphere.VirtualMachine, error) {
	return nodes.cnsNodeManager.GetNodeByName(ctx, nodeName)
}

// GetSharedDatastoresInTopology returns shared accessible datastores for specified topologyRequirement along with the map of
// datastore URL and array of accessibleTopology map for each datastore returned from this function.
// Here in this function, argument topologyRequirement can be passed in following form
// topologyRequirement [requisite:<segments:<key:"failure-domain.beta.kubernetes.io/region" value:"k8s-region-us" >
//                                 segments:<key:"failure-domain.beta.kubernetes.io/zone" value:"k8s-zone-us-east" > >
//                      requisite:<segments:<key:"failure-domain.beta.kubernetes.io/region" value:"k8s-region-us" >
//                                 segments:<key:"failure-domain.beta.kubernetes.io/zone" value:"k8s-zone-us-west" > >
//                      preferred:<segments:<key:"failure-domain.beta.kubernetes.io/region" value:"k8s-region-us" >
//                                 segments:<key:"failure-domain.beta.kubernetes.io/zone" value:"k8s-zone-us-west" > >
//                      preferred:<segments:<key:"failure-domain.beta.kubernetes.io/region" value:"k8s-region-us" >
//                                 segments:<key:"failure-domain.beta.kubernetes.io/zone" value:"k8s-zone-us-east" > > ]
//
// Return map datastoreTopologyMap looks like as below
// map[ ds:///vmfs/volumes/5d119112-7b28fe05-f51d-02000b3a3f4b/:
//         [map[failure-domain.beta.kubernetes.io/region:k8s-region-us failure-domain.beta.kubernetes.io/zone:k8s-zone-us-east]]
//      ds:///vmfs/volumes/e54abc3f-f6a5bb1f-0000-000000000000/:
//         [map[failure-domain.beta.kubernetes.io/region:k8s-region-us failure-domain.beta.kubernetes.io/zone:k8s-zone-us-east]]
//      ds:///vmfs/volumes/vsan:524fae1aaca129a5-1ee55a87f26ae626/:
//         [map[failure-domain.beta.kubernetes.io/region:k8s-region-us failure-domain.beta.kubernetes.io/zone:k8s-zone-us-west]
//         map[failure-domain.beta.kubernetes.io/region:k8s-region-us failure-domain.beta.kubernetes.io/zone:k8s-zone-us-east]]]]
func (nodes *Nodes) GetSharedDatastoresInTopology(ctx context.Context, topologyRequirement *csi.TopologyRequirement, zoneCategoryName string, regionCategoryName string) ([]*cnsvsphere.DatastoreInfo, map[string][]map[string]string, error) {
	log := logger.GetLogger(ctx)
	log.Debugf("GetSharedDatastoresInTopology: called with topologyRequirement: %+v, zoneCategoryName: %s, regionCategoryName: %s", topologyRequirement, zoneCategoryName, regionCategoryName)
	allNodes, err := nodes.cnsNodeManager.GetAllNodes(ctx)
	if err != nil {
		log.Errorf("Failed to get Nodes from nodeManager with err %+v", err)
		return nil, nil, err
	}
	if len(allNodes) == 0 {
		errMsg := fmt.Sprintf("Empty List of Node VMs returned from nodeManager")
		log.Errorf(errMsg)
		return nil, nil, fmt.Errorf(errMsg)
	}
	// getNodesInZoneRegion takes zone and region as parameter and returns list of node VMs which belongs to specified
	// zone and region.
	getNodesInZoneRegion := func(zoneValue string, regionValue string) ([]*cnsvsphere.VirtualMachine, error) {
		log.Debugf("getNodesInZoneRegion: called with zoneValue: %s, regionValue: %s", zoneValue, regionValue)
		var nodeVMsInZoneAndRegion []*cnsvsphere.VirtualMachine
		for _, nodeVM := range allNodes {
			isNodeInZoneRegion, err := nodeVM.IsInZoneRegion(ctx, zoneCategoryName, regionCategoryName, zoneValue, regionValue)
			if err != nil {
				log.Errorf("Error checking if node VM: %v belongs to zone [%s] and region [%s]. err: %+v", nodeVM, zoneValue, regionValue, err)
				return nil, err
			}
			if isNodeInZoneRegion {
				nodeVMsInZoneAndRegion = append(nodeVMsInZoneAndRegion, nodeVM)
			}
		}
		return nodeVMsInZoneAndRegion, nil
	}

	// getSharedDatastoresInTopology returns list of shared accessible datastores for requested topology along with the map of datastore URL and array of accessibleTopology
	// map for each datastore returned from this function.
	getSharedDatastoresInTopology := func(topologyArr []*csi.Topology) ([]*cnsvsphere.DatastoreInfo, map[string][]map[string]string, error) {
		log.Debugf("getSharedDatastoresInTopology: called with topologyArr: %+v", topologyArr)
		var sharedDatastores []*cnsvsphere.DatastoreInfo
		datastoreTopologyMap := make(map[string][]map[string]string)
		for _, topology := range topologyArr {
			segments := topology.GetSegments()
			zone := segments[csitypes.LabelZoneFailureDomain]
			region := segments[csitypes.LabelRegionFailureDomain]
			log.Debugf("Getting list of nodeVMs for zone [%s] and region [%s]", zone, region)
			nodeVMsInZoneRegion, err := getNodesInZoneRegion(zone, region)
			if err != nil {
				log.Errorf("Failed to find Nodes in the zone: [%s] and region: [%s]. Error: %+v", zone, region, err)
				return nil, nil, err
			}
			log.Debugf("Obtained list of nodeVMs [%+v] for zone [%s] and region [%s]", nodeVMsInZoneRegion, zone, region)
			sharedDatastoresInZoneRegion, err := nodes.GetSharedDatastoresForVMs(ctx, nodeVMsInZoneRegion)
			if err != nil {
				log.Errorf("Failed to get shared datastores for nodes: %+v in zone [%s] and region [%s]. Error: %+v", nodeVMsInZoneRegion, zone, region, err)
				return nil, nil, err
			}
			log.Debugf("Obtained shared datastores : %+v for topology: %+v", sharedDatastores, topology)
			for _, datastore := range sharedDatastoresInZoneRegion {
				accessibleTopology := make(map[string]string)
				if zone != "" {
					accessibleTopology[csitypes.LabelZoneFailureDomain] = zone
				}
				if region != "" {
					accessibleTopology[csitypes.LabelRegionFailureDomain] = region
				}
				datastoreTopologyMap[datastore.Info.Url] = append(datastoreTopologyMap[datastore.Info.Url], accessibleTopology)
			}
			sharedDatastores = append(sharedDatastores, sharedDatastoresInZoneRegion...)
		}
		return sharedDatastores, datastoreTopologyMap, nil
	}

	var sharedDatastores []*cnsvsphere.DatastoreInfo
	var datastoreTopologyMap = make(map[string][]map[string]string)
	if topologyRequirement != nil && topologyRequirement.GetPreferred() != nil {
		log.Debugf("Using preferred topology")
		sharedDatastores, datastoreTopologyMap, err = getSharedDatastoresInTopology(topologyRequirement.GetPreferred())
		if err != nil {
			log.Errorf("Error occurred  while finding shared datastores from preferred topology: %+v", topologyRequirement.GetPreferred())
			return nil, nil, err
		}
	}
	if len(sharedDatastores) == 0 && topologyRequirement != nil && topologyRequirement.GetRequisite() != nil {
		log.Debugf("Using requisite topology")
		sharedDatastores, datastoreTopologyMap, err = getSharedDatastoresInTopology(topologyRequirement.GetRequisite())
		if err != nil {
			log.Errorf("Error occurred  while finding shared datastores from requisite topology: %+v", topologyRequirement.GetRequisite())
			return nil, nil, err
		}
	}
	return sharedDatastores, datastoreTopologyMap, nil
}

// GetSharedDatastoresInK8SCluster returns list of DatastoreInfo objects for datastores accessible to all
// kubernetes nodes in the cluster.
func (nodes *Nodes) GetSharedDatastoresInK8SCluster(ctx context.Context) ([]*cnsvsphere.DatastoreInfo, error) {
	log := logger.GetLogger(ctx)
	nodeVMs, err := nodes.cnsNodeManager.GetAllNodes(ctx)
	if err != nil {
		log.Errorf("Failed to get Nodes from nodeManager with err %+v", err)
		return nil, err
	}
	if len(nodeVMs) == 0 {
		errMsg := fmt.Sprintf("Empty List of Node VMs returned from nodeManager")
		log.Errorf(errMsg)
		return make([]*cnsvsphere.DatastoreInfo, 0), fmt.Errorf(errMsg)
	}
	sharedDatastores, err := nodes.GetSharedDatastoresForVMs(ctx, nodeVMs)
	if err != nil {
		log.Errorf("Failed to get shared datastores for node VMs. Err: %+v", err)
		return nil, err
	}
	log.Debugf("sharedDatastores : %+v", sharedDatastores)
	return sharedDatastores, nil
}

// GetSharedDatastoresForVMs returns shared datastores accessible to specified nodeVMs list
func (nodes *Nodes) GetSharedDatastoresForVMs(ctx context.Context, nodeVMs []*cnsvsphere.VirtualMachine) ([]*cnsvsphere.DatastoreInfo, error) {
	var sharedDatastores []*cnsvsphere.DatastoreInfo
	log := logger.GetLogger(ctx)
	for _, nodeVM := range nodeVMs {
		log.Debugf("Getting accessible datastores for node %s", nodeVM.VirtualMachine)
		accessibleDatastores, err := nodeVM.GetAllAccessibleDatastores(ctx)
		if err != nil {
			return nil, err
		}
		if len(sharedDatastores) == 0 {
			sharedDatastores = accessibleDatastores
		} else {
			var sharedAccessibleDatastores []*cnsvsphere.DatastoreInfo
			for _, sharedDs := range sharedDatastores {
				// Check if sharedDatastores is found in accessibleDatastores
				for _, accessibleDs := range accessibleDatastores {
					// Intersection is performed based on the datastoreUrl as this uniquely identifies the datastore.
					if sharedDs.Info.Url == accessibleDs.Info.Url {
						sharedAccessibleDatastores = append(sharedAccessibleDatastores, sharedDs)
						break
					}
				}
			}
			sharedDatastores = sharedAccessibleDatastores
		}
		if len(sharedDatastores) == 0 {
			return nil, fmt.Errorf("No shared datastores found for nodeVm: %+v", nodeVM)
		}
	}
	return sharedDatastores, nil
}
