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

package k8sorchestrator

import (
	"context"
	"os"
	"strconv"
	"sync"
	"time"

	"github.com/container-storage-interface/spec/lib/go/csi"
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

	"sigs.k8s.io/vsphere-csi-driver/v2/pkg/common/cns-lib/node"
	cnsvsphere "sigs.k8s.io/vsphere-csi-driver/v2/pkg/common/cns-lib/vsphere"
	commoncotypes "sigs.k8s.io/vsphere-csi-driver/v2/pkg/csi/service/common/commonco/types"
	"sigs.k8s.io/vsphere-csi-driver/v2/pkg/csi/service/logger"
	"sigs.k8s.io/vsphere-csi-driver/v2/pkg/internalapis/csinodetopology"
	csinodetopologyv1alpha1 "sigs.k8s.io/vsphere-csi-driver/v2/pkg/internalapis/csinodetopology/v1alpha1"
	k8s "sigs.k8s.io/vsphere-csi-driver/v2/pkg/kubernetes"
)

var (
	// controllerVolumeTopologyInstance is a singleton instance of controllerVolumeTopology.
	controllerVolumeTopologyInstance *controllerVolumeTopology
	// nodeVolumeTopologyInstance is a singleton instance of nodeVolumeTopology.
	nodeVolumeTopologyInstance *nodeVolumeTopology
	// controllerVolumeTopologyInstanceLock is used for handling race conditions
	// during read, write on controllerVolumeTopologyInstance. It is only used during the
	// instantiation of controllerVolumeTopologyInstance.
	controllerVolumeTopologyInstanceLock = &sync.RWMutex{}
	// nodeVolumeTopologyInstanceLock is used for handling race conditions
	// during read, write on nodeVolumeTopologyInstance. It is only used during the
	// instantiation of nodeVolumeTopologyInstance.
	nodeVolumeTopologyInstanceLock = &sync.RWMutex{}
	// defaultTimeoutInMin is the default duration for which
	// the topology service client will watch on the CSINodeTopology instance to check
	// if the Status has been updated successfully.
	defaultTimeoutInMin = 1
	// maxTimeoutInMin is the maximum duration for which
	// the topology service client will watch on the CSINodeTopology instance to check
	// if the Status has been updated successfully.
	maxTimeoutInMin = 2
)

// nodeVolumeTopology implements the commoncotypes.NodeTopologyService interface. It stores
// the necessary kubernetes configurations and clients required to implement the methods in the interface.
type nodeVolumeTopology struct {
	// csiNodeTopologyK8sClient helps operate on CSINodeTopology custom resource.
	csiNodeTopologyK8sClient client.Client
	// csiNodeTopologyWatcher is a watcher instance on the CSINodeTopology custom resource.
	csiNodeTopologyWatcher *cache.ListWatch
	// k8sClient is a kubernetes client.
	k8sClient clientset.Interface
	//k8sConfig is the in-cluster config for client to talk to the api-server.
	k8sConfig *restclient.Config
}

// controllerVolumeTopology implements the commoncotypes.ControllerTopologyService interface. It stores
// the necessary kubernetes configurations and clients required to implement the methods in the interface.
type controllerVolumeTopology struct {
	//k8sConfig is the in-cluster config for client to talk to the api-server.
	k8sConfig *restclient.Config
	// csiNodeTopologyInformer is an informer instance on the CSINodeTopology custom resource.
	csiNodeTopologyInformer cache.SharedIndexInformer
	// nodeMgr is an instance of the node interface which exposes functionality related to nodeVMs.
	nodeMgr node.Manager
}

// InitTopologyServiceInController returns a singleton implementation of the
// commoncotypes.ControllerTopologyService interface.
func (c *K8sOrchestrator) InitTopologyServiceInController(ctx context.Context) (
	commoncotypes.ControllerTopologyService, error) {
	log := logger.GetLogger(ctx)

	controllerVolumeTopologyInstanceLock.RLock()
	if controllerVolumeTopologyInstance == nil {
		controllerVolumeTopologyInstanceLock.RUnlock()
		controllerVolumeTopologyInstanceLock.Lock()
		defer controllerVolumeTopologyInstanceLock.Unlock()
		if controllerVolumeTopologyInstance == nil {

			// Get in cluster config for client to API server.
			config, err := k8s.GetKubeConfig(ctx)
			if err != nil {
				log.Errorf("failed to get kubeconfig with error: %v", err)
				return nil, err
			}

			// Get node manager instance.
			// Node manager should already have been initialized in controller init.
			nodeManager := node.GetManager(ctx)

			// Create and start an informer on CSINodeTopology instances.
			crInformer, err := startTopologyCRInformer(ctx, config)
			if err != nil {
				log.Errorf("failed to create an informer for CSINodeTopology instances. Error: %+v", err)
				return nil, err
			}

			controllerVolumeTopologyInstance = &controllerVolumeTopology{
				k8sConfig:               config,
				nodeMgr:                 nodeManager,
				csiNodeTopologyInformer: *crInformer,
			}
			log.Info("Topology service initiated successfully")
		}
	} else {
		controllerVolumeTopologyInstanceLock.RUnlock()
	}
	return controllerVolumeTopologyInstance, nil
}

// startTopologyCRInformer creates and starts an informer for CSINodeTopology custom resource.
func startTopologyCRInformer(ctx context.Context, cfg *restclient.Config) (*cache.SharedIndexInformer, error) {
	log := logger.GetLogger(ctx)
	// Create an informer for CSINodeTopology instances.
	dynInformer, err := k8s.GetDynamicInformer(ctx, csinodetopologyv1alpha1.GroupName,
		csinodetopologyv1alpha1.Version, csinodetopology.CRDPlural, metav1.NamespaceAll, cfg, true)
	if err != nil {
		log.Errorf("failed to create dynamic informer for %s CR. Error: %+v", csinodetopology.CRDSingular,
			err)
		return nil, err
	}
	csiNodeTopologyInformer := dynInformer.Informer()

	// Start informer.
	go func() {
		log.Infof("Informer to watch on %s CR starting..", csinodetopology.CRDSingular)
		csiNodeTopologyInformer.Run(make(chan struct{}))
	}()
	return &csiNodeTopologyInformer, nil
}

// InitTopologyServiceInNode returns a singleton implementation of the commoncotypes.NodeTopologyService interface.
func (c *K8sOrchestrator) InitTopologyServiceInNode(ctx context.Context) (commoncotypes.NodeTopologyService, error) {
	log := logger.GetLogger(ctx)

	nodeVolumeTopologyInstanceLock.RLock()
	if nodeVolumeTopologyInstance == nil {
		nodeVolumeTopologyInstanceLock.RUnlock()
		nodeVolumeTopologyInstanceLock.Lock()
		defer nodeVolumeTopologyInstanceLock.Unlock()
		if nodeVolumeTopologyInstance == nil {

			// Get in cluster config for client to API server.
			config, err := k8s.GetKubeConfig(ctx)
			if err != nil {
				log.Errorf("failed to get kubeconfig with error: %v", err)
				return nil, err
			}

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

			nodeVolumeTopologyInstance = &nodeVolumeTopology{
				csiNodeTopologyK8sClient: crClient,
				csiNodeTopologyWatcher:   crWatcher,
				k8sClient:                k8sClient,
				k8sConfig:                config,
			}
			log.Infof("Topology service initiated successfully")
		}
	} else {
		nodeVolumeTopologyInstanceLock.RUnlock()
	}
	return nodeVolumeTopologyInstance, nil
}

// GetNodeTopologyLabels uses the CSINodeTopology CR to retrieve topology information of a node.
func (volTopology *nodeVolumeTopology) GetNodeTopologyLabels(ctx context.Context, nodeInfo *commoncotypes.NodeInfo) (
	map[string]string, error) {
	log := logger.GetLogger(ctx)

	// Fetch node object to set owner ref.
	nodeObj, err := volTopology.k8sClient.CoreV1().Nodes().Get(ctx, nodeInfo.NodeName, metav1.GetOptions{})
	if err != nil {
		return nil, logger.LogNewErrorCodef(log, codes.Internal,
			"failed to fetch node object with name %q. Error: %v", nodeInfo.NodeName, err)
	}
	// Create spec for CSINodeTopology.
	csiNodeTopologySpec := &csinodetopologyv1alpha1.CSINodeTopology{
		ObjectMeta: metav1.ObjectMeta{
			Name: nodeInfo.NodeName,
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
			NodeID: nodeInfo.NodeID,
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
		log.Infof("Successfully created a CSINodeTopology instance for NodeName: %q", nodeInfo.NodeName)
	}

	timeoutSeconds := int64((time.Duration(getCSINodeTopologyWatchTimeoutInMin(ctx)) * time.Minute).Seconds())
	watchCSINodeTopology, err := volTopology.csiNodeTopologyWatcher.Watch(metav1.ListOptions{
		FieldSelector:  fields.OneTermEqualSelector("metadata.name", nodeInfo.NodeName).String(),
		TimeoutSeconds: &timeoutSeconds,
		Watch:          true,
	})
	if err != nil {
		return nil, logger.LogNewErrorCodef(log, codes.Internal,
			"failed to watch on CSINodeTopology instance with name %q. Error: %+v", nodeInfo.NodeName, err)
	}
	defer watchCSINodeTopology.Stop()

	// Check if status gets updated in the instance within the given timeout seconds.
	for event := range watchCSINodeTopology.ResultChan() {
		csiNodeTopologyInstance, ok := event.Object.(*csinodetopologyv1alpha1.CSINodeTopology)
		if !ok {
			log.Warnf("Received unidentified object - %+v", event.Object)
			continue
		}
		if csiNodeTopologyInstance.Name != nodeInfo.NodeName {
			continue
		}
		switch csiNodeTopologyInstance.Status.Status {
		case csinodetopologyv1alpha1.CSINodeTopologySuccess:
			// Status set to success. Read the labels and return.
			accessibleTopology := make(map[string]string)
			for _, label := range csiNodeTopologyInstance.Status.TopologyLabels {
				accessibleTopology[label.Key] = label.Value
			}
			return accessibleTopology, nil
		case csinodetopologyv1alpha1.CSINodeTopologyError:
			// There was an error collecting topology information from nodes.
			return nil, logger.LogNewErrorCodef(log, codes.Internal,
				"failed to retrieve topology information for Node: %q. Error: %q", nodeInfo.NodeName,
				csiNodeTopologyInstance.Status.ErrorMessage)
		}
	}
	// Timed out waiting for topology labels to be updated.
	return nil, logger.LogNewErrorCodef(log, codes.Internal,
		"timed out while waiting for topology labels to be updated in %q CSINodeTopology instance.",
		nodeInfo.NodeName)
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
func (volTopology *controllerVolumeTopology) GetSharedDatastoresInTopology(ctx context.Context,
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
func (volTopology *controllerVolumeTopology) getSharedDatastoresInTopology(ctx context.Context,
	topologyArr []*csi.Topology) (
	[]*cnsvsphere.DatastoreInfo, map[string][]map[string]string, error) {
	log := logger.GetLogger(ctx)
	var sharedDatastores []*cnsvsphere.DatastoreInfo
	datastoreTopologyMap := make(map[string][]map[string]string)

	// A topology requirement is an array of topology segments.
	for _, topology := range topologyArr {
		segments := topology.GetSegments()
		// Fetch nodes with topology labels matching the topology segments.
		log.Debugf("Getting list of nodeVMs for topology segments %+v", segments)
		matchingNodeVMs, err := volTopology.getNodesMatchingTopologySegment(ctx, segments)
		if err != nil {
			log.Errorf("Failed to find nodes in topology segment %+v. Error: %+v", segments, err)
			return nil, nil, err
		}

		// Fetch shared datastores for the matching nodeVMs.
		log.Infof("Obtained list of nodeVMs %+v", matchingNodeVMs)
		sharedDatastoresInTopology, err := cnsvsphere.GetSharedDatastoresForVMs(ctx, matchingNodeVMs)
		if err != nil {
			log.Errorf("Failed to get shared datastores for nodes: %+v in topology segment %+v. Error: %+v",
				matchingNodeVMs, segments, err)
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
func (volTopology *controllerVolumeTopology) getNodesMatchingTopologySegment(ctx context.Context,
	segments map[string]string) (
	[]*cnsvsphere.VirtualMachine, error) {
	log := logger.GetLogger(ctx)

	var matchingNodeVMs []*cnsvsphere.VirtualMachine
	// Fetch node topology information from informer cache.
	nodeTopologyStore := volTopology.csiNodeTopologyInformer.GetStore()
	for _, val := range nodeTopologyStore.List() {
		var nodeTopologyInstance csinodetopologyv1alpha1.CSINodeTopology
		// Validate the object received.
		err := runtime.DefaultUnstructuredConverter.FromUnstructured(val.(*unstructured.Unstructured).Object,
			&nodeTopologyInstance)
		if err != nil {
			log.Warnf("received a non-CSINodeTopology instance: %+v", val)
			continue
		}
		// Convert array of labels to map.
		topoLabels := make(map[string]string)
		if nodeTopologyInstance.Status.Status == csinodetopologyv1alpha1.CSINodeTopologySuccess {
			for _, topoLabel := range nodeTopologyInstance.Status.TopologyLabels {
				topoLabels[topoLabel.Key] = topoLabel.Value
			}
		}
		// Check for a match of labels in every segment.
		isMatch := true
		for key, value := range segments {
			if topoLabels[key] != value {
				log.Debugf("Node %q did not match the topology requirement - %q: %q ",
					nodeTopologyInstance.Spec.NodeID, key, value)
				isMatch = false
				break
			}
		}
		if isMatch {
			// NOTE: NodeID is set to NodeName for now. Will be changed to NodeUUID in future.
			nodeVM, err := volTopology.nodeMgr.GetNodeByName(ctx, nodeTopologyInstance.Spec.NodeID)
			if err != nil {
				log.Errorf("failed to retrieve NodeVM for nodeID %q. Error - %+v",
					nodeTopologyInstance.Spec.NodeID, err)
				return nil, err
			}
			matchingNodeVMs = append(matchingNodeVMs, nodeVM)
		}
	}
	return matchingNodeVMs, nil
}
