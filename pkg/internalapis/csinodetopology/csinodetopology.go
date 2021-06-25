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

package csinodetopology

import (
	"context"
	"os"
	"reflect"
	"strconv"
	"sync"
	"time"

	"github.com/container-storage-interface/spec/lib/go/csi"
	"go.uber.org/zap"
	"google.golang.org/grpc/codes"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/fields"
	"k8s.io/apimachinery/pkg/runtime"
	clientset "k8s.io/client-go/kubernetes"
	restclient "k8s.io/client-go/rest"
	"k8s.io/client-go/tools/cache"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"sigs.k8s.io/vsphere-csi-driver/pkg/common/cns-lib/node"
	cnsvsphere "sigs.k8s.io/vsphere-csi-driver/pkg/common/cns-lib/vsphere"
	"sigs.k8s.io/vsphere-csi-driver/pkg/csi/service/logger"
	csitypes "sigs.k8s.io/vsphere-csi-driver/pkg/csi/types"
	csinodetopologyv1alpha1 "sigs.k8s.io/vsphere-csi-driver/pkg/internalapis/csinodetopology/v1alpha1"
	k8s "sigs.k8s.io/vsphere-csi-driver/pkg/kubernetes"
)

const (
	// CRDSingular represents the singular name of csinodetopology CRD.
	CRDSingular = "csinodetopology"
	// CRDPlural represents the plural name of csinodetopology CRD.
	CRDPlural = "csinodetopologies"
	// defaultTimeoutInMin is the default duration for which
	// the topology service client will watch on the CSINodeTopology instance to check
	// if the Status has been updated successfully.
	defaultTimeoutInMin = 1
	// maxTimeoutInMin is the maximum duration for which
	// the topology service client will watch on the CSINodeTopology instance to check
	// if the Status has been updated successfully.
	maxTimeoutInMin = 2
)

// NodeInfo contains the information required for the TopologyService to
// identify a node and retrieve its topology information.
type NodeInfo struct {
	// NodeName uniquely identifies the node in kubernetes.
	NodeName string
	// NodeID is a unique identifier of the NodeVM in vSphere.
	NodeID string
}

// TopologyService is an interface which exposes functionality related to topology aware Kubernetes clusters.
type TopologyService interface {
	// GetNodeTopologyLabels fetches the topology labels from the ancestors of NodeVM given the NodeInfo.
	GetNodeTopologyLabels(ctx context.Context, info *NodeInfo) (map[string]string, error)
	// GetSharedDatastoresInTopology gets the list of shared datastores which adhere to the topology
	// requirement given as a parameter.
	GetSharedDatastoresInTopology(ctx context.Context, topologyRequirement *csi.TopologyRequirement) (
		[]*cnsvsphere.DatastoreInfo, map[string][]map[string]string, error)
}

// volumeTopology implements the TopologyService interface.
type volumeTopology struct {
	// csiNodeTopologyK8sClient helps operate on CSINodeTopology custom resource.
	csiNodeTopologyK8sClient client.Client
	// csiNodeTopologyWatcher is a watcher instance on the CSINodeTopology custom resource.
	csiNodeTopologyWatcher *cache.ListWatch
	// k8sClient is a kubernetes client.
	k8sClient clientset.Interface
	//k8sConfig is the in-cluster config for client to talk to the api-server.
	k8sConfig *restclient.Config
	// csiNodeTopologyInformer is an informer instance on the CSINodeTopology custom resource.
	csiNodeTopologyInformer cache.SharedIndexInformer
	// nodeMgr is an instance of the node interface which exposes functionality related to nodeVMs.
	nodeMgr node.Manager
}

var (
	// volumeTopologyInstance is a singleton instance of volumeTopology.
	volumeTopologyInstance *volumeTopology
	// volumeTopologyInstanceLock is used for handling race conditions during read, write on volumeTopologyInstance.
	// It is only used during the instantiation of volumeTopologyInstance.
	volumeTopologyInstanceLock = &sync.RWMutex{}
	// nodeTopologyInfo keeps a local cache map of every nodeID to map of topology labels it belongs to.
	nodeTopologyInfo = make(map[string]map[string]string)
	// nodeTopologyInfoLock is a RW lock used to regulate access to the nodeTopologyInfo instance.
	nodeTopologyInfoLock = &sync.RWMutex{}
)

// InitTopologyServiceInterface returns a singleton implementation of the
// TopologyService interface.
func InitTopologyServiceInterface(ctx context.Context) (TopologyService, error) {
	log := logger.GetLogger(ctx)

	volumeTopologyInstanceLock.RLock()
	if volumeTopologyInstance == nil {
		volumeTopologyInstanceLock.RUnlock()
		volumeTopologyInstanceLock.Lock()
		defer volumeTopologyInstanceLock.Unlock()
		if volumeTopologyInstance == nil {

			// Get in cluster config for client to API server.
			config, err := k8s.GetKubeConfig(ctx)
			if err != nil {
				log.Errorf("failed to get kubeconfig with error: %v", err)
				return nil, err
			}

			serviceMode := os.Getenv(csitypes.EnvVarMode)
			// Topology service is initialized in nodes and controller.
			// Few of the following initializations require rbac privileges, therefore
			// we need to segregate this section based on service mode.
			if serviceMode == "node" {
				// Create the kubernetes client.
				k8sClient, err := k8s.NewClient(ctx)
				if err != nil {
					log.Errorf("failed to create K8s client. Error: %v", err)
					return nil, err
				}

				// Create watcher for CSINodeTopology instances.
				crWatcher, err := k8s.NewCSINodeTopologyWatcher(ctx, config)
				if err != nil {
					log.Errorf("failed to create a watcher for CSINodeTopology CR. Error: %+v", err)
					return nil, err
				}

				// Create client to API server.
				crClient, err := k8s.NewClientForGroup(ctx, config, csinodetopologyv1alpha1.GroupName)
				if err != nil {
					log.Errorf("failed to create K8s client for CSINodeTopology resource with error: %v", err)
					return nil, err
				}

				volumeTopologyInstance = &volumeTopology{
					csiNodeTopologyK8sClient: crClient,
					csiNodeTopologyWatcher:   crWatcher,
					k8sClient:                k8sClient,
					k8sConfig:                config,
				}
			} else {
				// Get node manager instance.
				// Node manager should already have been initialized in controller init.
				nodeManager := node.GetManager(ctx)

				// Create and start an informer on CSINodeTopology instances.
				crInformer, err := startTopologyCRInformer(ctx, config)
				if err != nil {
					log.Errorf("failed to create an informer for CSINodeTopology instances. Error: %+v", err)
					return nil, err
				}

				volumeTopologyInstance = &volumeTopology{
					k8sConfig:               config,
					nodeMgr:                 nodeManager,
					csiNodeTopologyInformer: *crInformer,
				}
			}
		}
	} else {
		volumeTopologyInstanceLock.RUnlock()
	}
	return volumeTopologyInstance, nil
}

// startTopologyCRInformer creates and starts an informer for CSINodeTopology custom resource.
func startTopologyCRInformer(ctx context.Context, cfg *restclient.Config) (*cache.SharedIndexInformer, error) {
	log := logger.GetLogger(ctx)
	// Create an informer for CSINodeTopology instances.
	dynInformer, err := k8s.GetDynamicInformer(ctx, csinodetopologyv1alpha1.GroupName,
		csinodetopologyv1alpha1.Version, CRDPlural, metav1.NamespaceAll, cfg, true)
	if err != nil {
		log.Errorf("failed to create dynamic informer for %s CR. Error: %+v", CRDSingular,
			err)
		return nil, err
	}
	csiNodeTopologyInformer := dynInformer.Informer()

	// Add in event handler functions.
	csiNodeTopologyInformer.AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc: func(obj interface{}) {
			topologyCRAdded(obj)
		},
		UpdateFunc: func(oldObj interface{}, newObj interface{}) {
			topologyCRUpdated(oldObj, newObj)
		},
		DeleteFunc: func(obj interface{}) {
			topologyCRDeleted(obj)
		},
	})
	// Start informer.
	go func() {
		log.Infof("Informer to watch on %s CR starting..", CRDSingular)
		csiNodeTopologyInformer.Run(make(chan struct{}))
	}()
	return &csiNodeTopologyInformer, nil
}

func topologyCRAdded(obj interface{}) {
	_, log := logger.GetNewContextWithLogger()
	var nodeTopology csinodetopologyv1alpha1.CSINodeTopology

	// Validate the object received.
	err := runtime.DefaultUnstructuredConverter.FromUnstructured(obj.(*unstructured.Unstructured).Object, &nodeTopology)
	if err != nil {
		log.Warnf("topologyCRAdded: unrecognized object %+v", obj)
		return
	}
	updateNodeTopologyInfo(log, nodeTopology)
}

func topologyCRUpdated(oldObj, newObj interface{}) {
	_, log := logger.GetNewContextWithLogger()
	var newNodeTopology, oldNodeTopology csinodetopologyv1alpha1.CSINodeTopology

	// Validate the old object received.
	err := runtime.DefaultUnstructuredConverter.FromUnstructured(oldObj.(*unstructured.Unstructured).Object, &oldNodeTopology)
	if err != nil {
		log.Warnf("topologyCRUpdated: Unrecognized old object %+v", oldObj)
		return
	}
	// Validate the new object received.
	err = runtime.DefaultUnstructuredConverter.FromUnstructured(newObj.(*unstructured.Unstructured).Object, &newNodeTopology)
	if err != nil {
		log.Warnf("topologyCRUpdated: Unrecognized new object %+v", newObj)
		return
	}

	// Check if the nodeName already exists in the cache, if not, update.
	nodeTopologyInfoLock.RLock()
	if _, exists := nodeTopologyInfo[newNodeTopology.Name]; exists {
		// Check if there is any update in topology labels wrt the old object, if not return.
		if reflect.DeepEqual(oldNodeTopology.Status.TopologyLabels, newNodeTopology.Status.TopologyLabels) {
			log.Debugf("topologyCRUpdated: Ignoring the update as there is no change in topology labels.")
			return
		}
	}
	nodeTopologyInfoLock.RUnlock()

	updateNodeTopologyInfo(log, newNodeTopology)
}

// updateNodeTopologyInfo takes in a CSINodeTopology instance as an argument and updates
// the local nodeTopologyInfo map with the latest topology labels.
func updateNodeTopologyInfo(log *zap.SugaredLogger, nodeTopology csinodetopologyv1alpha1.CSINodeTopology) {
	// Check the status of the CR, if it is Success, then update local cache.
	if nodeTopology.Status.Status == csinodetopologyv1alpha1.CSINodeTopologySuccess {
		topologyLabels := make(map[string]string)
		for _, topoLabel := range nodeTopology.Status.TopologyLabels {
			topologyLabels[topoLabel.Key] = topoLabel.Value
		}
		// Acquire Lock to update cache.
		nodeTopologyInfoLock.Lock()
		nodeTopologyInfo[nodeTopology.Name] = topologyLabels
		nodeTopologyInfoLock.Unlock()
		log.Infof("Saved topology labels info for node %s", nodeTopology.Name)
	}
}

func topologyCRDeleted(obj interface{}) {
	_, log := logger.GetNewContextWithLogger()
	var nodeTopology csinodetopologyv1alpha1.CSINodeTopology

	// Validate the object received.
	err := runtime.DefaultUnstructuredConverter.FromUnstructured(obj.(*unstructured.Unstructured).Object, &nodeTopology)
	if err != nil {
		log.Warnf("topologyCRDeleted: unrecognized object %+v", obj)
		return
	}
	// Acquire Lock to remove node info from cache.
	nodeTopologyInfoLock.Lock()
	delete(nodeTopologyInfo, nodeTopology.Name)
	nodeTopologyInfoLock.Unlock()
}

// GetNodeTopologyLabels uses the CSINodeTopology CR to retrieve topology information of a node.
func (volTopology *volumeTopology) GetNodeTopologyLabels(ctx context.Context, info *NodeInfo) (
	map[string]string, error) {
	log := logger.GetLogger(ctx)

	// Fetch node object to set owner ref.
	nodeObj, err := volTopology.k8sClient.CoreV1().Nodes().Get(ctx, info.NodeName, metav1.GetOptions{})
	if err != nil {
		return nil, logger.LogNewErrorCodef(log, codes.Internal,
			"failed to fetch node object with name %q. Error: %v", info.NodeName, err)
	}
	// Create spec for CSINodeTopology.
	csiNodeTopologySpec := &csinodetopologyv1alpha1.CSINodeTopology{
		ObjectMeta: metav1.ObjectMeta{
			Name: info.NodeName,
			OwnerReferences: []metav1.OwnerReference{
				{
					APIVersion: "v1",
					Kind:       "Node",
					Name:       nodeObj.Name,
					UID:        nodeObj.UID,
				},
			},
		},
		Spec: csinodetopologyv1alpha1.CSINodeTopologySpec{
			NodeID: info.NodeID,
		},
	}
	// Create CSINodeTopology CR for the node.
	err = volTopology.csiNodeTopologyK8sClient.Create(ctx, csiNodeTopologySpec)
	if err != nil {
		if !apierrors.IsAlreadyExists(err) {
			return nil, logger.LogNewErrorCodef(log, codes.Internal,
				"failed to create CSINodeTopology CR. Error: %+v", err)
		}
	} else {
		log.Infof("Successfully created a CSINodeTopology instance for NodeName: %q", info.NodeName)
	}

	timeoutSeconds := int64((time.Duration(getCSINodeTopologyWatchTimeoutInMin(ctx)) * time.Minute).Seconds())
	watchCSINodeTopology, err := volTopology.csiNodeTopologyWatcher.Watch(metav1.ListOptions{
		FieldSelector:  fields.OneTermEqualSelector("metadata.name", info.NodeName).String(),
		TimeoutSeconds: &timeoutSeconds,
		Watch:          true,
	})
	if err != nil {
		return nil, logger.LogNewErrorCodef(log, codes.Internal,
			"failed to watch on CSINodeTopology instance with name %q. Error: %+v", info.NodeName, err)
	}
	defer watchCSINodeTopology.Stop()

	// Check if status gets updated in the instance within the given timeout seconds.
	for event := range watchCSINodeTopology.ResultChan() {
		csiNodeTopologyInstance, ok := event.Object.(*csinodetopologyv1alpha1.CSINodeTopology)
		if !ok {
			log.Warnf("Received unidentified object - %+v", event.Object)
			continue
		}
		if csiNodeTopologyInstance.Name != info.NodeName {
			continue
		}
		switch csiNodeTopologyInstance.Status.Status {
		case csinodetopologyv1alpha1.CSINodeTopologySuccess:
			accessibleTopology := make(map[string]string)
			for _, label := range csiNodeTopologyInstance.Status.TopologyLabels {
				accessibleTopology[label.Key] = label.Value
			}
			return accessibleTopology, nil
		case csinodetopologyv1alpha1.CSINodeTopologyError:
			return nil, logger.LogNewErrorCodef(log, codes.Internal,
				"failed to retrieve topology information for Node: %q. Error: %q", info.NodeName,
				csiNodeTopologyInstance.Status.ErrorMessage)
		}
	}
	return nil, logger.LogNewErrorCodef(log, codes.Internal,
		"timed out while waiting for topology labels to be updated in %q CSINodeTopology instance.",
		info.NodeName)
}

// getCSINodeTopologyWatchTimeoutInMin returns the timeout for watching
// on CSINodeTopology instances for any updates.
// If environment variable NODEGETINFO_WATCH_TIMEOUT_MINUTES is set and
// has a valid value between [1, 2], return the interval value read
// from environment variable. Otherwise, use the default timeout of 1 min.
func getCSINodeTopologyWatchTimeoutInMin(ctx context.Context) int {
	log := logger.GetLogger(ctx)
	watcherTimeoutInMin := defaultTimeoutInMin
	if v := os.Getenv("NODEGETINFO_WATCH_TIMEOUT_MINUTES"); v != "" {
		if value, err := strconv.Atoi(v); err == nil {
			switch {
			case value <= 0:
				log.Warnf("Timeout set in env variable NODEGETINFO_WATCH_TIMEOUT_MINUTES %q is equal or "+
					"less than 0, will use the default timeout of %d minute(s)", v, watcherTimeoutInMin)
			case value > maxTimeoutInMin:
				log.Warnf("Timeout set in env variable NODEGETINFO_WATCH_TIMEOUT_MINUTES %q is greater than "+
					"%d, will use the default timeout of %d minute(s)", v, maxTimeoutInMin,
					watcherTimeoutInMin)
			default:
				watcherTimeoutInMin = value
				log.Infof("Timeout is set to %d minute(s)", watcherTimeoutInMin)
			}
		} else {
			log.Warnf("Timeout set in env variable NODEGETINFO_WATCH_TIMEOUT_MINUTES %q is invalid, "+
				"using the default timeout of %d minute(s)", v, watcherTimeoutInMin)
		}
	}
	return watcherTimeoutInMin
}

// GetSharedDatastoresInTopology returns shared accessible datastores for
// specified topologyRequirement along with the map of datastore URL and
// array of accessibleTopology map for each datastore returned from this
// function.
//
// Argument topologyRequirement needs to be passed in following form:
// topologyRequirement [requisite:<segments:<key:"failure-domain.beta.kubernetes.io/region" value:"k8s-region-us" >
//                                 segments:<key:"failure-domain.beta.kubernetes.io/zone" value:"k8s-zone-us-east" > >
//                      requisite:<segments:<key:"failure-domain.beta.kubernetes.io/region" value:"k8s-region-us" >
//                                 segments:<key:"failure-domain.beta.kubernetes.io/zone" value:"k8s-zone-us-west" > >
//                      preferred:<segments:<key:"failure-domain.beta.kubernetes.io/region" value:"k8s-region-us" >
//                                 segments:<key:"failure-domain.beta.kubernetes.io/zone" value:"k8s-zone-us-west" > >
//                      preferred:<segments:<key:"failure-domain.beta.kubernetes.io/region" value:"k8s-region-us" >
//                                 segments:<key:"failure-domain.beta.kubernetes.io/zone" value:"k8s-zone-us-east" > > ]
//
// Return value of datastoreTopologyMap looks like the following:
// map[ ds:///vmfs/volumes/5d119112-7b28fe05-f51d-02000b3a3f4b/:
//         [map [ failure-domain.beta.kubernetes.io/region:k8s-region-us
//                failure-domain.beta.kubernetes.io/zone:k8s-zone-us-east ]]
//      ds:///vmfs/volumes/e54abc3f-f6a5bb1f-0000-000000000000/:
//         [map [ failure-domain.beta.kubernetes.io/region:k8s-region-us
//                failure-domain.beta.kubernetes.io/zone:k8s-zone-us-east ]]
//      ds:///vmfs/volumes/vsan:524fae1aaca129a5-1ee55a87f26ae626/:
//         [map [ failure-domain.beta.kubernetes.io/region:k8s-region-us
//                failure-domain.beta.kubernetes.io/zone:k8s-zone-us-west ]
//          map [ failure-domain.beta.kubernetes.io/region:k8s-region-us
//                failure-domain.beta.kubernetes.io/zone:k8s-zone-us-east ]] ]
func (volTopology *volumeTopology) GetSharedDatastoresInTopology(ctx context.Context,
	topologyRequirement *csi.TopologyRequirement) (
	[]*cnsvsphere.DatastoreInfo, map[string][]map[string]string, error) {

	log := logger.GetLogger(ctx)
	log.Debugf("Get shared datastores with topologyRequirement: %+v", topologyRequirement)

	var (
		err                  error
		sharedDatastores     []*cnsvsphere.DatastoreInfo
		datastoreTopologyMap = make(map[string][]map[string]string)
	)

	// Fetch shared datastores for the preferred topology requirement.
	if topologyRequirement.GetPreferred() != nil {
		log.Debugf("Using preferred topology")
		sharedDatastores, datastoreTopologyMap, err =
			volTopology.getSharedDatastoresInTopology(ctx, topologyRequirement.GetPreferred())
		if err != nil {
			log.Errorf("Error finding shared datastores using preferred topology: %+v",
				topologyRequirement.GetPreferred())
			return nil, nil, err
		}
	}
	// If there are no shared datastores for the preferred topology requirement, fetch shared
	// datastores for the requisite topology requirement instead.
	if len(sharedDatastores) == 0 && topologyRequirement.GetRequisite() != nil {
		log.Debugf("Using requisite topology")
		sharedDatastores, datastoreTopologyMap, err =
			volTopology.getSharedDatastoresInTopology(ctx, topologyRequirement.GetRequisite())
		if err != nil {
			log.Errorf("Error finding shared datastores using requisite topology: %+v",
				topologyRequirement.GetRequisite())
			return nil, nil, err
		}
	}
	return sharedDatastores, datastoreTopologyMap, nil
}

// getSharedDatastoresInTopology returns a list of shared accessible datastores
// for requested topology along with the map of datastore URL to an array of topology segments.
func (volTopology *volumeTopology) getSharedDatastoresInTopology(ctx context.Context, topologyArr []*csi.Topology) (
	[]*cnsvsphere.DatastoreInfo, map[string][]map[string]string, error) {
	log := logger.GetLogger(ctx)
	var sharedDatastores []*cnsvsphere.DatastoreInfo
	datastoreTopologyMap := make(map[string][]map[string]string)

	// A topology requirement is an array of topology segments.
	for _, topology := range topologyArr {
		segments := topology.GetSegments()
		// Fetch nodes with topology labels matching the topology segments.
		log.Debugf("Getting list of nodeVMs for topology segments %+v", segments)
		nodeVMsInZoneRegion, err := volTopology.getNodesMatchingTopologySegment(ctx, segments)
		if err != nil {
			log.Errorf("Failed to find nodes in topology segment %+v. Error: %+v", segments, err)
			return nil, nil, err
		}

		// Fetch shared datastores for the matching nodeVMs.
		log.Infof("Obtained list of nodeVMs %+v", nodeVMsInZoneRegion)
		sharedDatastoresInTopology, err := cnsvsphere.GetSharedDatastoresForVMs(ctx, nodeVMsInZoneRegion)
		if err != nil {
			log.Errorf("Failed to get shared datastores for nodes: %+v in topology segment %+v. Error: %+v",
				nodeVMsInZoneRegion, segments, err)
			return nil, nil, err
		}

		// Update datastoreTopologyMap with array of topology segments.
		for _, datastore := range sharedDatastoresInTopology {
			datastoreTopologyMap[datastore.Info.Url] =
				append(datastoreTopologyMap[datastore.Info.Url], segments)
		}
		sharedDatastores = append(sharedDatastores, sharedDatastoresInTopology...)
	}
	log.Infof("Obtained shared datastores: %+v", sharedDatastores)
	return sharedDatastores, datastoreTopologyMap, nil
}

// getNodesMatchingTopologySegment takes in topology segments as parameter and returns list
// of node VMs which belong to all the segments.
func (volTopology *volumeTopology) getNodesMatchingTopologySegment(ctx context.Context, segments map[string]string) (
	[]*cnsvsphere.VirtualMachine, error) {
	log := logger.GetLogger(ctx)

	var matchingNodeVMs []*cnsvsphere.VirtualMachine
	nodeTopologyInfoLock.RLock()
	for nodeID, topoLabels := range nodeTopologyInfo {
		isMatch := true
		for key, value := range segments {
			if topoLabels[key] != value {
				log.Debugf("Node %q did not match the topology requirement - %q: %q ", nodeID, key, value)
				isMatch = false
				break
			}
		}
		if isMatch {
			// NOTE: NodeID is set to NodeName for now. Will be changed to NodeUUID in future.
			nodeVM, err := volTopology.nodeMgr.GetNodeByName(ctx, nodeID)
			if err != nil {
				nodeTopologyInfoLock.RUnlock()
				log.Errorf("failed to retrieve NodeVM for nodeID %q. Error - %+v", nodeID, err)
				return nil, err
			}
			matchingNodeVMs = append(matchingNodeVMs, nodeVM)
		}
	}
	nodeTopologyInfoLock.RUnlock()
	return matchingNodeVMs, nil
}
