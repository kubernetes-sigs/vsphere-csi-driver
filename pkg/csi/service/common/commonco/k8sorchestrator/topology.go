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
	"errors"
	"fmt"
	"os"
	"reflect"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/container-storage-interface/spec/lib/go/csi"
	cnstypes "github.com/vmware/govmomi/cns/types"
	"github.com/vmware/govmomi/object"
	"github.com/vmware/govmomi/vim25/mo"
	vimtypes "github.com/vmware/govmomi/vim25/types"
	"google.golang.org/grpc/codes"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	apiMeta "k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/fields"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/dynamic"
	clientset "k8s.io/client-go/kubernetes"
	restclient "k8s.io/client-go/rest"
	"k8s.io/client-go/tools/cache"
	"sigs.k8s.io/controller-runtime/pkg/client"
	cnsconfig "sigs.k8s.io/vsphere-csi-driver/v3/pkg/common/config"

	"sigs.k8s.io/vsphere-csi-driver/v3/pkg/common/cns-lib/node"
	cnsvsphere "sigs.k8s.io/vsphere-csi-driver/v3/pkg/common/cns-lib/vsphere"
	"sigs.k8s.io/vsphere-csi-driver/v3/pkg/csi/service/common"
	commoncotypes "sigs.k8s.io/vsphere-csi-driver/v3/pkg/csi/service/common/commonco/types"
	"sigs.k8s.io/vsphere-csi-driver/v3/pkg/csi/service/logger"
	"sigs.k8s.io/vsphere-csi-driver/v3/pkg/internalapis/csinodetopology"
	csinodetopologyv1alpha1 "sigs.k8s.io/vsphere-csi-driver/v3/pkg/internalapis/csinodetopology/v1alpha1"
	k8s "sigs.k8s.io/vsphere-csi-driver/v3/pkg/kubernetes"
)

var (
	// controllerVolumeTopologyInstance is a singleton instance of controllerVolumeTopology
	// created for vanilla flavor.
	controllerVolumeTopologyInstance *controllerVolumeTopology
	// wcpControllerVolumeTopologyInstance is a singleton instance of wcpControllerVolumeTopology
	// created for workload flavor.
	wcpControllerVolumeTopologyInstance *wcpControllerVolumeTopology
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
	// domainNodeMap maintains a cache of topology tags to the node names under that tag.
	// Example - {region1: {Node1: struct{}{}, Node2: struct{}{}},
	//            zone1: {Node1: struct{}{}},
	//            zone2: {Node2: struct{}{}}}
	// The nodes under each tag are maintained as a map of string with nil values to improve
	// retrieval and deletion performance.
	// CAUTION: This technique requires that tag values should not be repeated across
	// categories i.e using `us-east` as a region and as a zone is not allowed.
	domainNodeMap = make(map[string]map[string]struct{})
	// domainNodeMapInstanceLock guards the domainNodeMap instance from concurrent writes.
	domainNodeMapInstanceLock = &sync.RWMutex{}
	// azClusterMap maintains a cache of AZ instance name to the clusterMoref in that zone.
	azClusterMap = make(map[string]string)
	// azClusterMapInstanceLock guards the azClusterMap instance from concurrent writes.
	azClusterMapInstanceLock = &sync.RWMutex{}
	// preferredDatastoresMap is a map of topology domain to list of
	// datastore URLs preferred in that domain.
	// Ex: {zone1: [DSURL1, DSURL2], zone2: [DSURL3]}
	preferredDatastoresMap = make(map[string][]string)
	// preferredDatastoresMapInstanceLock guards the preferredDatastoresMap from read-write overlaps.
	preferredDatastoresMapInstanceLock = &sync.RWMutex{}
	// isMultiVCSupportEnabled is set to true only when the MultiVCenterCSITopology FSS
	// is enabled. isMultivCenterCluster is set to true only when the MultiVCenterCSITopology FSS
	// is enabled and the K8s cluster involves multiple VCs.
	isMultiVCSupportEnabled, isMultivCenterCluster bool
	// csiNodeTopologyInformer refers to a shared K8s informer listening on CSINodeTopology instances
	// in the cluster.
	csiNodeTopologyInformer *cache.SharedIndexInformer
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
	// k8sConfig is the in-cluster config for client to talk to the api-server.
	k8sConfig *restclient.Config
	// clusterFlavor is the cluster flavor.
	clusterFlavor cnstypes.CnsClusterFlavor
	// isCSINodeIdFeatureEnabled indicates whether the
	// use-csinode-id feature is enabled or not.
	isCSINodeIdFeatureEnabled bool
}

// controllerVolumeTopology implements the commoncotypes.ControllerTopologyService interface
// for vanilla flavor. It stores the necessary kubernetes configurations and clients required
// to implement the methods in the interface.
type controllerVolumeTopology struct {
	//k8sConfig is the in-cluster config for client to talk to the api-server.
	k8sConfig *restclient.Config
	// csiNodeTopologyInformer is an informer instance on the CSINodeTopology custom resource.
	csiNodeTopologyInformer cache.SharedIndexInformer
	// nodeMgr is an instance of the node interface which exposes functionality related to nodeVMs.
	nodeMgr node.Manager
	// clusterFlavor is the cluster flavor.
	clusterFlavor cnstypes.CnsClusterFlavor
	// isCSINodeIdFeatureEnabled indicates whether the
	// use-csinode-id feature is enabled or not.
	isCSINodeIdFeatureEnabled bool
	// isAcceptPreferredDatastoresFSSEnabled indicates whether the
	// accept-preferred-datastores feature is enabled or not.
	isTopologyPreferentialDatastoresFSSEnabled bool
}

// wcpControllerVolumeTopology implements the commoncotypes.ControllerTopologyService
// interface for workload flavor. It stores the necessary kubernetes configurations and informers required
// to implement the methods in the interface.
type wcpControllerVolumeTopology struct {
	//k8sConfig is the in-cluster config for client to talk to the api-server.
	k8sConfig *restclient.Config
	// azInformer is an informer instance on the AvailabilityZone custom resource.
	azInformer cache.SharedIndexInformer
}

// InitTopologyServiceInController returns a singleton implementation of the
// commoncotypes.ControllerTopologyService interface.
func (c *K8sOrchestrator) InitTopologyServiceInController(ctx context.Context) (
	commoncotypes.ControllerTopologyService, error) {
	log := logger.GetLogger(ctx)

	if c.clusterFlavor == cnstypes.CnsClusterFlavorVanilla {
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
				csiNodeTopologyInformer, err = startTopologyCRInformer(ctx, config)
				if err != nil {
					log.Errorf("failed to create an informer for CSINodeTopology instances. Error: %+v", err)
					return nil, err
				}

				clusterFlavor, err := cnsconfig.GetClusterFlavor(ctx)
				if err != nil {
					log.Errorf("failed to get cluster flavor. Error: %+v", err)
					return nil, err
				}

				// Set isMultivCenterCluster if the K8s cluster is a multi-VC cluster.
				isMultiVCSupportEnabled = c.IsFSSEnabled(ctx, common.MultiVCenterCSITopology)
				if isMultiVCSupportEnabled {
					cfg, err := cnsconfig.GetConfig(ctx)
					if err != nil {
						return nil, logger.LogNewErrorf(log, "failed to read config. Error: %+v", err)
					}
					if len(cfg.VirtualCenter) > 1 {
						isMultivCenterCluster = true
					}
				}

				controllerVolumeTopologyInstance = &controllerVolumeTopology{
					k8sConfig:                 config,
					nodeMgr:                   nodeManager,
					csiNodeTopologyInformer:   *csiNodeTopologyInformer,
					clusterFlavor:             clusterFlavor,
					isCSINodeIdFeatureEnabled: c.IsFSSEnabled(ctx, common.UseCSINodeId),
					isTopologyPreferentialDatastoresFSSEnabled: c.IsFSSEnabled(ctx,
						common.TopologyPreferentialDatastores),
				}

				if controllerVolumeTopologyInstance.isTopologyPreferentialDatastoresFSSEnabled {
					// Get CNS config.
					cnsCfg, err := cnsconfig.GetConfig(ctx)
					if err != nil {
						return nil, logger.LogNewErrorf(log, "failed to fetch CNS config. Error: %+v", err)
					}
					// Fetch preferred datastores and store in cache at regular intervals.
					go func() {
						// Read tags under PreferredDatastoresCategory in 5min interval and store in cache.
						ticker := time.NewTicker(time.Duration(cnsCfg.Global.CSIFetchPreferredDatastoresIntervalInMin) *
							time.Minute)
						for ; true; <-ticker.C {
							ctx, log := logger.GetNewContextWithLogger()
							log.Infof("Refreshing preferred datastores information...")
							if isMultiVCSupportEnabled {
								err = common.RefreshPreferentialDatastoresForMultiVCenter(ctx)
							} else {
								err = refreshPreferentialDatastores(ctx)
							}
							if err != nil {
								log.Errorf("failed to refresh preferential datastores in cluster. Error: %v", err)
								os.Exit(1)
							}
						}
					}()
				}
				log.Info("Topology service initiated successfully")
			}
		} else {
			controllerVolumeTopologyInstanceLock.RUnlock()
		}
		return controllerVolumeTopologyInstance, nil
	} else if c.clusterFlavor == cnstypes.CnsClusterFlavorWorkload {
		controllerVolumeTopologyInstanceLock.RLock()
		if wcpControllerVolumeTopologyInstance == nil {
			controllerVolumeTopologyInstanceLock.RUnlock()
			controllerVolumeTopologyInstanceLock.Lock()
			defer controllerVolumeTopologyInstanceLock.Unlock()
			if wcpControllerVolumeTopologyInstance == nil {

				// Get in cluster config for client to API server.
				config, err := k8s.GetKubeConfig(ctx)
				if err != nil {
					log.Errorf("failed to get kubeconfig with error: %v", err)
					return nil, err
				}
				// Create and start an informer on AvailabilityZone instances.
				azInformer, err := startAvailabilityZoneInformer(ctx, config)
				if err != nil {
					if err == common.ErrAvailabilityZoneCRNotRegistered {
						log.Infof("Skip initializing the topology service as the AvailabilityZone " +
							"CR is not registered.")
						return nil, nil
					}
					log.Errorf("failed to create an informer for CSINodeTopology instances. Error: %+v", err)
					return nil, err
				}
				wcpControllerVolumeTopologyInstance = &wcpControllerVolumeTopology{
					k8sConfig:  config,
					azInformer: *azInformer,
				}
			}
		} else {
			controllerVolumeTopologyInstanceLock.RUnlock()
		}
		return wcpControllerVolumeTopologyInstance, nil
	}
	return nil, logger.LogNewErrorf(log, "InitTopologyServiceInController not implemented for "+
		"cluster flavor: %q", c.clusterFlavor)
}

// refreshPreferentialDatastores refreshes the preferredDatastoresMap variable
// with latest information on the preferential datastores for each topology domain.
func refreshPreferentialDatastores(ctx context.Context) error {
	log := logger.GetLogger(ctx)
	// Get VC instance.
	cnsCfg, err := cnsconfig.GetConfig(ctx)
	if err != nil {
		return logger.LogNewErrorf(log, "failed to fetch CNS config. Error: %+v", err)
	}
	vcenterconfig, err := cnsvsphere.GetVirtualCenterConfig(ctx, cnsCfg)
	if err != nil {
		return logger.LogNewErrorf(log, "failed to get VirtualCenterConfig from CNS config. Error: %+v", err)
	}
	vc, err := cnsvsphere.GetVirtualCenterManager(ctx).GetVirtualCenter(ctx, vcenterconfig.Host)
	if err != nil {
		return logger.LogNewErrorf(log, "failed to get vCenter instance. Error: %+v", err)
	}
	// Get tag manager instance.
	tagMgr, err := cnsvsphere.GetTagManager(ctx, vc)
	if err != nil {
		return logger.LogNewErrorf(log, "failed to create tag manager. Error: %+v", err)
	}
	defer func() {
		err := tagMgr.Logout(ctx)
		if err != nil {
			log.Errorf("failed to logout tagManager. Error: %v", err)
		}
	}()
	// Get tags for category reserved for preferred datastore tagging.
	tagIds, err := tagMgr.ListTagsForCategory(ctx, common.PreferredDatastoresCategory)
	if err != nil {
		log.Infof("failed to retrieve tags for category %q. Reason: %+v", common.PreferredDatastoresCategory,
			err)
		return nil
	}
	if len(tagIds) == 0 {
		log.Info("No preferred datastores found in environment.")
		return nil
	}
	// Fetch vSphere entities on which the tags have been applied.
	attachedObjs, err := tagMgr.GetAttachedObjectsOnTags(ctx, tagIds)
	if err != nil {
		return logger.LogNewErrorf(log, "failed to retrieve objects with tags %v. Error: %+v", tagIds, err)
	}
	prefDatastoresMap := make(map[string][]string)
	for _, attachedObj := range attachedObjs {
		for _, obj := range attachedObj.ObjectIDs {
			// Preferred datastore tag should only be applied to datastores.
			if obj.Reference().Type != "Datastore" {
				log.Warnf("Preferred datastore tag applied on a non-datastore entity: %+v",
					obj.Reference())
				continue
			}
			// Fetch Datastore URL.
			var dsMo mo.Datastore
			dsObj := object.NewDatastore(vc.Client.Client, obj.Reference())
			err = dsObj.Properties(ctx, obj.Reference(), []string{"summary"}, &dsMo)
			if err != nil {
				return logger.LogNewErrorf(log, "failed to retrieve summary from datastore: %+v. Error: %v",
					obj.Reference(), err)
			}

			log.Infof("Datastore %q with URL %q is preferred in %q", dsMo.Summary.Name, dsMo.Summary.Url,
				attachedObj.Tag.Name)
			// For each topology domain, store the datastore URLs preferred in that domain.
			prefDatastoresMap[attachedObj.Tag.Name] = append(prefDatastoresMap[attachedObj.Tag.Name], dsMo.Summary.Url)
		}
	}
	// Finally, write to cache.
	if len(prefDatastoresMap) != 0 {
		preferredDatastoresMapInstanceLock.Lock()
		defer preferredDatastoresMapInstanceLock.Unlock()
		preferredDatastoresMap = prefDatastoresMap
	}
	return nil
}

// GetCSINodeTopologyInstancesList lists out all the CSINodeTopology
// instances using the K8s informer cache store.
func (c *K8sOrchestrator) GetCSINodeTopologyInstancesList() []interface{} {
	nodeTopologyStore := (*csiNodeTopologyInformer).GetStore()
	return nodeTopologyStore.List()
}

// GetCSINodeTopologyInstanceByName fetches the CSINodeTopology instance
// for a given nodeName using the K8s informer cache store.
func (c *K8sOrchestrator) GetCSINodeTopologyInstanceByName(nodeName string) (
	item interface{}, exists bool, err error) {
	nodeTopologyStore := (*csiNodeTopologyInformer).GetStore()
	return nodeTopologyStore.GetByKey(nodeName)
}

// startAvailabilityZoneInformer listens on changes to AvailabilityZone instances and updates the azClusterMap cache.
func startAvailabilityZoneInformer(ctx context.Context, cfg *restclient.Config) (*cache.SharedIndexInformer, error) {
	log := logger.GetLogger(ctx)
	// Check if AZ CR is registered in the environment.
	// Create a new AvailabilityZone client.
	azClient, err := dynamic.NewForConfig(cfg)
	if err != nil {
		return nil, fmt.Errorf("failed to create AvailabilityZone client using config. Err: %+v", err)
	}
	// Get AvailabilityZone list
	azResource := schema.GroupVersionResource{
		Group: "topology.tanzu.vmware.com", Version: "v1alpha1", Resource: "availabilityzones"}
	_, err = azClient.Resource(azResource).List(ctx, metav1.ListOptions{})
	// Handling the scenario where AvailabilityZone CR is not registered in the
	// supervisor cluster.
	if apiMeta.IsNoMatchError(err) {
		log.Info("AvailabilityZone CR is not registered on the cluster")
		return nil, common.ErrAvailabilityZoneCRNotRegistered
	}
	// At this point, we are sure the AZ CR is registered. Create an informer for AvailabilityZone instances.
	dynInformer, err := k8s.GetDynamicInformer(ctx, "topology.tanzu.vmware.com",
		"v1alpha1", "availabilityzones", metav1.NamespaceAll, cfg, true)
	if err != nil {
		log.Errorf("failed to create dynamic informer for AvailabilityZone CR. Error: %+v",
			err)
		return nil, err
	}
	availabilityZoneInformer := dynInformer.Informer()
	availabilityZoneInformer.AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc: func(obj interface{}) {
			azCRAdded(obj)
		},
		UpdateFunc: nil,
		DeleteFunc: func(obj interface{}) {
			azCRDeleted(obj)
		},
	})

	// Start informer.
	go func() {
		log.Info("Informer to watch on AvailabilityZone CR starting..")
		availabilityZoneInformer.Run(make(chan struct{}))
	}()
	return &availabilityZoneInformer, nil
}

// azCRAdded handles adding AZ name and clusterMoref to the cache.
func azCRAdded(obj interface{}) {
	ctx, log := logger.GetNewContextWithLogger()
	// Retrieve name of CR instance.
	azName, found, err := unstructured.NestedString(obj.(*unstructured.Unstructured).Object, "metadata", "name")
	if !found || err != nil {
		log.Errorf("failed to get `name` from AvailabilityZone instance: %+v, Error: %+v", obj, err)
		return
	}
	// Retrieve clusterMorefs from instance spec.
	clusterComputeResourceMoIds, found, err := unstructured.NestedStringSlice(obj.(*unstructured.Unstructured).Object,
		"spec", "clusterComputeResourceMoIDs")
	if len(clusterComputeResourceMoIds) == 0 || !found || err != nil {
		log.Errorf("failed to get `clusterComputeResourceMoIds` from AvailabilityZone instance: %+v, "+
			"Error: %+v", obj, err)
		return
	}
	// Add to cache.
	// TODO: For VC 8.0 release, zone to CCR is a 1:1 mapping.
	// Update this when one vSphere zone can span across multiple CCRs.
	addToAZClusterMap(ctx, azName, clusterComputeResourceMoIds[0])
}

// azCRUpdated handles deleting AZ name in the cache.
func azCRDeleted(obj interface{}) {
	ctx, log := logger.GetNewContextWithLogger()
	// Retrieve name of CR instance.
	azName, found, err := unstructured.NestedString(obj.(*unstructured.Unstructured).Object, "metadata", "name")
	if !found || err != nil {
		log.Errorf("failed to get `name` from AvailabilityZone instance: %+v, Error: %+v", obj, err)
		return
	}
	// Delete AZ name from cache.
	removeFromAZClusterMap(ctx, azName)
}

// Adds the CR instance name and cluster moref to the azClusterMap.
func addToAZClusterMap(ctx context.Context, azName, clusterMoref string) {
	log := logger.GetLogger(ctx)
	azClusterMapInstanceLock.Lock()
	defer azClusterMapInstanceLock.Unlock()
	azClusterMap[azName] = clusterMoref
	log.Infof("Added %q cluster to %q zone in azClusterMap", clusterMoref, azName)
}

// Removes the provided zone and clusterMoref from the azClusterMap.
func removeFromAZClusterMap(ctx context.Context, azName string) {
	log := logger.GetLogger(ctx)
	azClusterMapInstanceLock.Lock()
	defer azClusterMapInstanceLock.Unlock()
	delete(azClusterMap, azName)
	log.Infof("Removed %q zone from azClusterMap", azName)
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
	topologyInformer := dynInformer.Informer()
	topologyInformer.AddEventHandler(cache.ResourceEventHandlerFuncs{
		// Typically when the CSINodeTopology instance is created, the
		// topology labels are not populated till the reconcile loop runs.
		// However, this Add function will take care of cases where the node
		// daemonset is restarted on driver upgrades and the CSINodeTopology
		// instances already exist.
		AddFunc: func(obj interface{}) {
			topoCRAdded(obj)
		},
		// Update handler.
		UpdateFunc: func(oldObj interface{}, newObj interface{}) {
			topoCRUpdated(oldObj, newObj)
		},
		// Delete handler.
		DeleteFunc: func(obj interface{}) {
			topoCRDeleted(obj)
		},
	})

	// Start informer.
	go func() {
		log.Infof("Informer to watch on %s CR starting..", csinodetopology.CRDSingular)
		topologyInformer.Run(make(chan struct{}))
	}()
	return &topologyInformer, nil
}

// topoCRAdded checks if the CSINodeTopology instance Status is set to Success
// and populates the domainNodeMap with appropriate values.
func topoCRAdded(obj interface{}) {
	ctx, log := logger.GetNewContextWithLogger()
	// Verify object received.
	var nodeTopoObj csinodetopologyv1alpha1.CSINodeTopology
	err := runtime.DefaultUnstructuredConverter.FromUnstructured(obj.(*unstructured.Unstructured).Object, &nodeTopoObj)
	if err != nil {
		log.Errorf("topoCRAdded: failed to cast object %+v to %s. Error: %v", obj,
			csinodetopology.CRDSingular, err)
		return
	}
	// Check if Status is set to Success.
	if nodeTopoObj.Status.Status != csinodetopologyv1alpha1.CSINodeTopologySuccess {
		log.Infof("topoCRAdded: CSINodeTopology instance %q not yet ready. Status: %q",
			nodeTopoObj.Name, nodeTopoObj.Status.Status)
		return
	}
	if isMultiVCSupportEnabled {
		common.AddNodeToDomainNodeMapNew(ctx, nodeTopoObj)
		if isMultivCenterCluster {
			common.AddLabelsToTopologyVCMap(ctx, nodeTopoObj)
		}
	} else {
		addNodeToDomainNodeMap(ctx, nodeTopoObj)
	}
}

// topoCRUpdated checks if the CSINodeTopology instance Status is set to Success
// and populates the domainNodeMap with appropriate values.
func topoCRUpdated(oldObj interface{}, newObj interface{}) {
	ctx, log := logger.GetNewContextWithLogger()
	// Verify both objects received.
	var (
		oldNodeTopoObj csinodetopologyv1alpha1.CSINodeTopology
		newNodeTopoObj csinodetopologyv1alpha1.CSINodeTopology
	)
	err := runtime.DefaultUnstructuredConverter.FromUnstructured(
		newObj.(*unstructured.Unstructured).Object, &newNodeTopoObj)
	if err != nil {
		log.Errorf("topoCRUpdated: failed to cast new object %+v to %s. Error: %+v", newObj,
			csinodetopology.CRDSingular, err)
		return
	}
	err = runtime.DefaultUnstructuredConverter.FromUnstructured(
		oldObj.(*unstructured.Unstructured).Object, &oldNodeTopoObj)
	if err != nil {
		log.Errorf("topoCRUpdated: failed to cast old object %+v to %s. Error: %+v", oldObj,
			csinodetopology.CRDSingular, err)
		return
	}
	oldTopoLabelsMap := make(map[string]string)
	for _, label := range oldNodeTopoObj.Status.TopologyLabels {
		oldTopoLabelsMap[label.Key] = label.Value
	}
	newTopoLabelsMap := make(map[string]string)
	for _, label := range newNodeTopoObj.Status.TopologyLabels {
		newTopoLabelsMap[label.Key] = label.Value
	}
	// Check if there are updates to the topology labels in the Status.
	if reflect.DeepEqual(oldTopoLabelsMap, newTopoLabelsMap) {
		log.Debugf("topoCRUpdated: No change in %s CR topology labels. Ignoring the event",
			csinodetopology.CRDSingular)
		return
	}
	// Ideally a CSINodeTopology CR should never be updated after the status is set to Success but
	// in cases where this does happen, in order to maintain the correctness of domainNodeMap, we
	// will first remove the node name from previous topology labels before adding the new values.
	if oldNodeTopoObj.Status.Status == csinodetopologyv1alpha1.CSINodeTopologySuccess {
		log.Warnf("topoCRUpdated: %q instance with name %q has been updated after the Status was set to "+
			"Success. Old object - %+v. New object - %+v", csinodetopology.CRDSingular, oldNodeTopoObj.Name,
			oldNodeTopoObj, newNodeTopoObj)
		if isMultiVCSupportEnabled {
			common.RemoveNodeFromDomainNodeMapNew(ctx, oldNodeTopoObj)
			if isMultivCenterCluster {
				common.RemoveLabelsFromTopologyVCMap(ctx, oldNodeTopoObj)
			}
		} else {
			removeNodeFromDomainNodeMap(ctx, oldNodeTopoObj)
		}
	}
	// Add the node name to the domainNodeMap if the Status is set to Success.
	if newNodeTopoObj.Status.Status == csinodetopologyv1alpha1.CSINodeTopologySuccess {
		if isMultiVCSupportEnabled {
			common.AddNodeToDomainNodeMapNew(ctx, newNodeTopoObj)
			if isMultivCenterCluster {
				common.AddLabelsToTopologyVCMap(ctx, newNodeTopoObj)
			}
		} else {
			addNodeToDomainNodeMap(ctx, newNodeTopoObj)
		}
	}
}

// topoCRDeleted removes the CSINodeTopology instance name from the domainNodeMap.
func topoCRDeleted(obj interface{}) {
	ctx, log := logger.GetNewContextWithLogger()
	// Verify object received.
	var nodeTopoObj csinodetopologyv1alpha1.CSINodeTopology
	err := runtime.DefaultUnstructuredConverter.FromUnstructured(obj.(*unstructured.Unstructured).Object, &nodeTopoObj)
	if err != nil {
		log.Errorf("topoCRDeleted: failed to cast object %+v to %s type. Error: %+v",
			csinodetopology.CRDSingular, err)
		return
	}
	// Delete node name from domainNodeMap if the status of the CR was set to Success.
	if nodeTopoObj.Status.Status == csinodetopologyv1alpha1.CSINodeTopologySuccess {
		if isMultiVCSupportEnabled {
			common.RemoveNodeFromDomainNodeMapNew(ctx, nodeTopoObj)
			if isMultivCenterCluster {
				common.RemoveLabelsFromTopologyVCMap(ctx, nodeTopoObj)
			}
		} else {
			removeNodeFromDomainNodeMap(ctx, nodeTopoObj)
		}
	} else {
		log.Infof("topoCRDeleted: %q instance with name %q and status %q deleted.", csinodetopology.CRDSingular,
			nodeTopoObj.Name, nodeTopoObj.Status.Status)
	}
}

// Adds the CR instance name in the domainNodeMap wherever appropriate.
func addNodeToDomainNodeMap(ctx context.Context, nodeTopoObj csinodetopologyv1alpha1.CSINodeTopology) {
	log := logger.GetLogger(ctx)
	domainNodeMapInstanceLock.Lock()
	defer domainNodeMapInstanceLock.Unlock()
	for _, label := range nodeTopoObj.Status.TopologyLabels {
		if _, exists := domainNodeMap[label.Value]; !exists {
			domainNodeMap[label.Value] = map[string]struct{}{nodeTopoObj.Name: {}}
		} else {
			domainNodeMap[label.Value][nodeTopoObj.Name] = struct{}{}
		}
	}
	log.Infof("Added %q value to domainNodeMap", nodeTopoObj.Name)
}

// Removes the CR instance name from the domainNodeMap.
func removeNodeFromDomainNodeMap(ctx context.Context, nodeTopoObj csinodetopologyv1alpha1.CSINodeTopology) {
	log := logger.GetLogger(ctx)
	domainNodeMapInstanceLock.Lock()
	defer domainNodeMapInstanceLock.Unlock()
	for _, label := range nodeTopoObj.Status.TopologyLabels {
		delete(domainNodeMap[label.Value], nodeTopoObj.Name)
	}
	log.Infof("Removed %q value from domainNodeMap", nodeTopoObj.Name)
}

// InitTopologyServiceInNode returns a singleton implementation of the commoncotypes.NodeTopologyService interface.
func (c *K8sOrchestrator) InitTopologyServiceInNode(ctx context.Context) (
	commoncotypes.NodeTopologyService, error) {
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

			clusterFlavor, err := cnsconfig.GetClusterFlavor(ctx)
			if err != nil {
				log.Errorf("failed to get cluster flavor. Error: %+v", err)
				return nil, err
			}

			nodeVolumeTopologyInstance = &nodeVolumeTopology{
				csiNodeTopologyK8sClient:  crClient,
				csiNodeTopologyWatcher:    crWatcher,
				k8sClient:                 k8sClient,
				k8sConfig:                 config,
				clusterFlavor:             clusterFlavor,
				isCSINodeIdFeatureEnabled: c.IsFSSEnabled(ctx, common.UseCSINodeId),
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

	var err error
	if volTopology.isCSINodeIdFeatureEnabled && volTopology.clusterFlavor == cnstypes.CnsClusterFlavorVanilla {
		csiNodeTopology := &csinodetopologyv1alpha1.CSINodeTopology{}
		csiNodeTopologyKey := types.NamespacedName{
			Name: nodeInfo.NodeName,
		}

		// Get CsiNodeTopology instance
		err = volTopology.csiNodeTopologyK8sClient.Get(ctx, csiNodeTopologyKey, csiNodeTopology)
		if err != nil {
			if apierrors.IsNotFound(err) {
				err = createCSINodeTopologyInstance(ctx, volTopology, nodeInfo)
				if err != nil {
					return nil, logger.LogNewErrorCodef(log, codes.Internal, err.Error())
				}
			} else {
				msg := fmt.Sprintf("failed to get CsiNodeTopology for the node: %q. Error: %+v", nodeInfo.NodeName, err)
				return nil, logger.LogNewErrorCodef(log, codes.Internal, msg)
			}
		} else {
			// If CSINodeTopology instance already exists, check if the NodeUUID
			// parameter in Spec is populated. If not, patch the instance.
			if csiNodeTopology.Spec.NodeUUID == "" ||
				csiNodeTopology.Spec.NodeUUID != nodeInfo.NodeID {
				if csiNodeTopology.Spec.NodeUUID == "" {
					log.Infof("CSINodeTopology instance: %q with empty nodeUUID found. "+
						"Patching the instance with nodeUUID", nodeInfo.NodeName)
				} else {
					log.Infof("CSINodeTopology instance: %q with different "+
						"nodeUUID: %s found. Patching the instance with nodeUUID: %s",
						nodeInfo.NodeName, csiNodeTopology.Spec.NodeUUID, nodeInfo.NodeID)
				}
				patch := []byte(fmt.Sprintf(`{"spec":{"nodeID":"%s","nodeuuid":"%s"}}`, nodeInfo.NodeName, nodeInfo.NodeID))
				// Patch the CSINodeTopology instance with nodeUUID
				err = volTopology.csiNodeTopologyK8sClient.Patch(ctx,
					&csinodetopologyv1alpha1.CSINodeTopology{
						ObjectMeta: metav1.ObjectMeta{
							Name: nodeInfo.NodeName,
						},
					},
					client.RawPatch(types.MergePatchType, patch))
				if err != nil {
					msg := fmt.Sprintf("Fail to patch CsiNodeTopology for the node: %q "+
						"with nodeUUID: %s. Error: %+v",
						nodeInfo.NodeName, nodeInfo.NodeID, err)
					return nil, logger.LogNewErrorCodef(log, codes.Internal, msg)
				}
				log.Infof("Successfully patched CSINodeTopology instance: %q with Uuid: %q",
					nodeInfo.NodeName, nodeInfo.NodeID)
			}
		}
	} else {
		err = createCSINodeTopologyInstance(ctx, volTopology, nodeInfo)
		if err != nil {
			return nil, logger.LogNewErrorCodef(log, codes.Internal, err.Error())
		}
	}

	// Create a watcher for CSINodeTopology CRs.
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

// Create new CSINodeTopology instance if it doesn't exist
// Create CSINodeTopology instance with spec.nodeID and spec.nodeUUID
// if cluster flavor is Vanilla and UseCSINodeId feature is enabled
// else create with spec.nodeID only.
func createCSINodeTopologyInstance(ctx context.Context,
	volTopology *nodeVolumeTopology,
	nodeInfo *commoncotypes.NodeInfo) error {
	log := logger.GetLogger(ctx)
	// Fetch node object to set owner ref.
	nodeObj, err := volTopology.k8sClient.CoreV1().Nodes().Get(ctx, nodeInfo.NodeName, metav1.GetOptions{})
	if err != nil {
		msg := fmt.Sprintf("failed to fetch node object with name %q. Error: %v", nodeInfo.NodeName, err)
		return errors.New(msg)
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
	}

	// If both useCnsNodeId feature is enabled and clusterFlavor is Vanilla,
	// create the CsiNodeTopology instance with nodeID set to node name and
	// nodeUUID set to node uuid.
	if volTopology.isCSINodeIdFeatureEnabled && volTopology.clusterFlavor == cnstypes.CnsClusterFlavorVanilla {
		csiNodeTopologySpec.Spec = csinodetopologyv1alpha1.CSINodeTopologySpec{
			NodeID:   nodeInfo.NodeName,
			NodeUUID: nodeInfo.NodeID,
		}
	} else {
		// Else create CsiNodeTopology instance with nodeID set to node name.
		csiNodeTopologySpec.Spec = csinodetopologyv1alpha1.CSINodeTopologySpec{
			NodeID: nodeInfo.NodeName,
		}
	}
	// Create CSINodeTopology CR for the node.
	err = volTopology.csiNodeTopologyK8sClient.Create(ctx, csiNodeTopologySpec)
	if err != nil {
		if !apierrors.IsAlreadyExists(err) {
			msg := fmt.Sprintf("failed to create CSINodeTopology CR. Error: %+v", err)
			return errors.New(msg)
		} else {
			log.Infof("CSINodeTopology instance already exists for NodeName: %q", nodeInfo.NodeName)
		}
	} else {
		log.Infof("Successfully created a CSINodeTopology instance for NodeName: %q", nodeInfo.NodeName)
	}
	return nil
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

// GetSharedDatastoresInTopology returns shared accessible datastores for the specified topologyRequirement.
// Argument TopologyRequirement needs to be passed in following form:
// topologyRequirement [requisite:<segments:<key:"failure-domain.beta.kubernetes.io/region" value:"k8s-region-us" >
//
//	           segments:<key:"failure-domain.beta.kubernetes.io/zone" value:"k8s-zone-us-east" > >
//	requisite:<segments:<key:"failure-domain.beta.kubernetes.io/region" value:"k8s-region-us" >
//	           segments:<key:"failure-domain.beta.kubernetes.io/zone" value:"k8s-zone-us-west" > >
//	preferred:<segments:<key:"failure-domain.beta.kubernetes.io/region" value:"k8s-region-us" >
//	           segments:<key:"failure-domain.beta.kubernetes.io/zone" value:"k8s-zone-us-west" > >
//	preferred:<segments:<key:"failure-domain.beta.kubernetes.io/region" value:"k8s-region-us" >
//	           segments:<key:"failure-domain.beta.kubernetes.io/zone" value:"k8s-zone-us-east" > >
func (volTopology *controllerVolumeTopology) GetSharedDatastoresInTopology(ctx context.Context,
	reqParams interface{}) ([]*cnsvsphere.DatastoreInfo, error) {
	log := logger.GetLogger(ctx)
	params := reqParams.(commoncotypes.VanillaTopologyFetchDSParams)
	log.Debugf("Get shared datastores with topologyRequirement: %+v", params.TopologyRequirement)
	var (
		err              error
		sharedDatastores []*cnsvsphere.DatastoreInfo
	)

	// Fetch shared datastores for the preferred topology requirement.
	if params.TopologyRequirement.GetPreferred() != nil {
		log.Debugf("Using preferred topology")
		sharedDatastores, err = volTopology.getSharedDatastoresInTopology(ctx,
			params.TopologyRequirement.GetPreferred(), params)
		if err != nil {
			log.Errorf("Error finding shared datastores using preferred topology: %+v",
				params.TopologyRequirement.GetPreferred())
			return nil, err
		}
	}
	// If there are no shared datastores for the preferred topology requirement, fetch shared
	// datastores for the requisite topology requirement instead.
	if len(sharedDatastores) == 0 && params.TopologyRequirement.GetRequisite() != nil {
		log.Debugf("Using requisite topology")
		sharedDatastores, err = volTopology.getSharedDatastoresInTopology(ctx,
			params.TopologyRequirement.GetRequisite(), params)
		if err != nil {
			log.Errorf("Error finding shared datastores using requisite topology: %+v",
				params.TopologyRequirement.GetRequisite())
			return nil, err
		}
	}
	return sharedDatastores, nil
}

// getSharedDatastoresInTopology returns a list of shared accessible datastores
// for requested topology.
func (volTopology *controllerVolumeTopology) getSharedDatastoresInTopology(ctx context.Context,
	topologyArr []*csi.Topology, params commoncotypes.VanillaTopologyFetchDSParams) ([]*cnsvsphere.DatastoreInfo,
	error) {
	log := logger.GetLogger(ctx)

	var sharedDatastores []*cnsvsphere.DatastoreInfo
	// A topology requirement is an array of topology segments.
	for _, topology := range topologyArr {
		segments := topology.GetSegments()
		var (
			err                      error
			matchingNodeVMs          []*cnsvsphere.VirtualMachine
			completeTopologySegments []map[string]string
		)
		// Fetch nodes with topology labels matching the topology segments.
		log.Debugf("Getting list of nodeVMs for topology segments %+v", segments)
		if volTopology.isTopologyPreferentialDatastoresFSSEnabled {
			matchingNodeVMs, completeTopologySegments, err = volTopology.getTopologySegmentsWithMatchingNodes(ctx,
				segments)
			if err != nil {
				log.Errorf("failed to find nodes in topology segment %+v. Error: %+v", segments, err)
				return nil, err
			}
		} else {
			matchingNodeVMs, err = volTopology.getNodesMatchingTopologySegment(ctx, segments)
			if err != nil {
				log.Errorf("Failed to find nodes in topology segment %+v. Error: %+v", segments, err)
				return nil, err
			}
		}
		if len(matchingNodeVMs) == 0 {
			log.Warnf("No nodes in the cluster matched the topology requirement provided: %+v",
				segments)
			continue
		}

		// Fetch shared datastores for the matching nodeVMs.
		log.Infof("Obtained list of nodeVMs %+v", matchingNodeVMs)
		sharedDatastoresInTopology, err := cnsvsphere.GetSharedDatastoresForVMs(ctx, matchingNodeVMs)
		if err != nil {
			log.Errorf("failed to get shared datastores for nodes: %+v in topology segment %+v. Error: %+v",
				matchingNodeVMs, segments, err)
			return nil, err
		}

		// If applicable, filter the shared datastores with the preferred datastores for that segment.
		if volTopology.isTopologyPreferentialDatastoresFSSEnabled {
			// If storage policy name is mentioned in storage class, check for
			// datastore compatibility before proceeding with preferred datastores.
			if params.StoragePolicyName != "" {
				storagePolicyID, err := params.Vc.GetStoragePolicyIDByName(ctx, params.StoragePolicyName)
				if err != nil {
					return nil, logger.LogNewErrorf(log, "Error occurred while getting Profile Id "+
						"from Storage Profile Name: %s. Error: %+v", params.StoragePolicyName, err)
				}
				// Check storage policy compatibility.
				var sharedDSMoRef []vimtypes.ManagedObjectReference
				for _, ds := range sharedDatastoresInTopology {
					sharedDSMoRef = append(sharedDSMoRef, ds.Reference())
				}
				compat, err := params.Vc.PbmCheckCompatibility(ctx, sharedDSMoRef, storagePolicyID)
				if err != nil {
					return nil, logger.LogNewErrorf(log, "failed to find datastore compatibility "+
						"with storage policy ID %q. Error: %+v", storagePolicyID, err)
				}
				compatibleDsMoids := make(map[string]struct{})
				for _, ds := range compat.CompatibleDatastores() {
					compatibleDsMoids[ds.HubId] = struct{}{}
				}
				log.Infof("Datastores compatible with storage policy %q are %+v", params.StoragePolicyName,
					compatibleDsMoids)

				// Filter compatible datastores from shared datastores list.
				var compatibleDatastores []*cnsvsphere.DatastoreInfo
				for _, ds := range sharedDatastoresInTopology {
					if _, exists := compatibleDsMoids[ds.Reference().Value]; exists {
						compatibleDatastores = append(compatibleDatastores, ds)
					}
				}
				if len(compatibleDatastores) == 0 {
					return nil, logger.LogNewErrorf(log, "No compatible shared datastores found "+
						"for storage policy %q", params.StoragePolicyName)
				}
				sharedDatastoresInTopology = compatibleDatastores
			}

			// Fetch all preferred datastore URLs for the matching topology segments.
			allPreferredDSURLs := make(map[string]struct{})
			for _, topoSegs := range completeTopologySegments {
				prefDS := getPreferredDatastoresInSegments(ctx, topoSegs)
				for key, val := range prefDS {
					allPreferredDSURLs[key] = val
				}
			}
			if len(allPreferredDSURLs) != 0 {
				// If there are preferred datastores among the compatible
				// datastores, choose the preferred datastores, otherwise
				// choose the compatible datastores.
				var preferredDS []*cnsvsphere.DatastoreInfo
				for _, dsInfo := range sharedDatastoresInTopology {
					if _, ok := allPreferredDSURLs[dsInfo.Info.Url]; ok {
						preferredDS = append(preferredDS, dsInfo)
					}
				}
				if len(preferredDS) != 0 {
					sharedDatastoresInTopology = preferredDS
					log.Infof("Using preferred datastores: %+v", preferredDS)
				}
			}
		}

		// Update sharedDatastores with the list of datastores received.
		// Duplicates will not be added.
		for _, ds := range sharedDatastoresInTopology {
			var found bool
			for _, sharedDS := range sharedDatastores {
				if sharedDS.Info.Url == ds.Info.Url {
					found = true
					break
				}
			}
			if !found {
				sharedDatastores = append(sharedDatastores, ds)
			}
		}
	}
	log.Infof("Obtained shared datastores: %+v", sharedDatastores)
	return sharedDatastores, nil
}

// getPreferredDatastoresInSegments fetches preferred datastores in
// given topology segments as a map for faster retrieval.
func getPreferredDatastoresInSegments(ctx context.Context, segments map[string]string) map[string]struct{} {
	log := logger.GetLogger(ctx)
	allPreferredDSURLs := make(map[string]struct{})

	preferredDatastoresMapInstanceLock.Lock()
	defer preferredDatastoresMapInstanceLock.Unlock()
	if len(preferredDatastoresMap) == 0 {
		return allPreferredDSURLs
	}
	// Arrange applicable preferred datastores as a map.
	for _, tag := range segments {
		preferredDS, ok := preferredDatastoresMap[tag]
		if ok {
			log.Infof("Found preferred datastores %+v for topology domain %q", preferredDS, tag)
			for _, val := range preferredDS {
				allPreferredDSURLs[val] = struct{}{}
			}
		}
	}
	return allPreferredDSURLs
}

// getTopologySegmentsWithMatchingNodes takes in topology segments as parameter and returns list
// of node VMs which belong to all the segments and a list of the complete hierarchy
// of topology segments which match the topology requirement.
// For example if topology requirement given is `zone1`.
// The complete hierarchy may look like this: [{zone: zone1, city: city1}, {zone: zone1, city: city2}].
func (volTopology *controllerVolumeTopology) getTopologySegmentsWithMatchingNodes(ctx context.Context,
	segments map[string]string) ([]*cnsvsphere.VirtualMachine, []map[string]string, error) {
	log := logger.GetLogger(ctx)

	var (
		matchingNodeVMs          []*cnsvsphere.VirtualMachine
		completeTopologySegments []map[string]string
	)
	// Fetch node topology information from informer cache.
	nodeTopologyStore := volTopology.csiNodeTopologyInformer.GetStore()
	for _, val := range nodeTopologyStore.List() {
		var nodeTopologyInstance csinodetopologyv1alpha1.CSINodeTopology
		// Validate the object received.
		err := runtime.DefaultUnstructuredConverter.FromUnstructured(val.(*unstructured.Unstructured).Object,
			&nodeTopologyInstance)
		if err != nil {
			return nil, nil, logger.LogNewErrorf(log, "failed to convert unstructured object %+v to "+
				"CSINodeTopology instance. Error: %+v", val, err)
		}

		// Check CSINodeTopology instance `Status` field for success.
		if nodeTopologyInstance.Status.Status != csinodetopologyv1alpha1.CSINodeTopologySuccess {
			return nil, nil, logger.LogNewErrorf(log, "node %q not yet ready. Found CSINodeTopology instance "+
				"status: %q with error message: %q", nodeTopologyInstance.Name, nodeTopologyInstance.Status.Status,
				nodeTopologyInstance.Status.ErrorMessage)
		}
		// Convert array of labels to map.
		topoLabelsMap := make(map[string]string)
		for _, topoLabel := range nodeTopologyInstance.Status.TopologyLabels {
			topoLabelsMap[topoLabel.Key] = topoLabel.Value
		}
		// Check for a match of labels in every segment.
		isMatch := true
		for key, value := range segments {
			if topoLabelsMap[key] != value {
				log.Debugf("Node %q with topology %+v did not match the topology requirement - %q: %q ",
					nodeTopologyInstance.Name, topoLabelsMap, key, value)
				isMatch = false
				break
			}
		}
		// If there is a match, fetch the nodeVM object and add it to matchingNodeVMs.
		if isMatch {
			var nodeVM *cnsvsphere.VirtualMachine
			if volTopology.isCSINodeIdFeatureEnabled &&
				volTopology.clusterFlavor == cnstypes.CnsClusterFlavorVanilla {
				nodeVM, err = volTopology.nodeMgr.GetNode(ctx,
					nodeTopologyInstance.Spec.NodeUUID, nil)
			} else {
				nodeVM, err = volTopology.nodeMgr.GetNodeByName(ctx,
					nodeTopologyInstance.Spec.NodeID)
			}
			if err != nil {
				log.Errorf("failed to retrieve NodeVM %q. Error - %+v", nodeTopologyInstance.Spec.NodeID, err)
				return nil, nil, err
			}
			matchingNodeVMs = append(matchingNodeVMs, nodeVM)
			// Store the complete hierarchy of topology segments for future use.
			var exists bool
			for _, segs := range completeTopologySegments {
				if reflect.DeepEqual(segs, topoLabelsMap) {
					exists = true
					break
				}
			}
			if !exists {
				completeTopologySegments = append(completeTopologySegments, topoLabelsMap)
			}
		}
	}
	return matchingNodeVMs, completeTopologySegments, nil
}

// getNodesMatchingTopologySegment takes in topology segments as parameter and returns list
// of node VMs which belong to all the segments.
func (volTopology *controllerVolumeTopology) getNodesMatchingTopologySegment(ctx context.Context,
	segments map[string]string) ([]*cnsvsphere.VirtualMachine, error) {
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
			return nil, logger.LogNewErrorf(log, "failed to convert unstructured object %+v to "+
				"CSINodeTopology instance. Error: %+v", val, err)
		}

		// Check CSINodeTopology instance `Status` field for success.
		if nodeTopologyInstance.Status.Status != csinodetopologyv1alpha1.CSINodeTopologySuccess {
			return nil, logger.LogNewErrorf(log, "node %q not yet ready. Found CSINodeTopology instance "+
				"status: %q with error message: %q", nodeTopologyInstance.Name, nodeTopologyInstance.Status.Status,
				nodeTopologyInstance.Status.ErrorMessage)
		}
		// Convert array of labels to map.
		topoLabels := make(map[string]string)
		for _, topoLabel := range nodeTopologyInstance.Status.TopologyLabels {
			topoLabels[topoLabel.Key] = topoLabel.Value
		}
		// Check for a match of labels in every segment.
		isMatch := true
		for key, value := range segments {
			if topoLabels[key] != value {
				log.Debugf("Node %q with topology %+v did not match the topology requirement - %q: %q ",
					nodeTopologyInstance.Name, topoLabels, key, value)
				isMatch = false
				break
			}
		}
		if isMatch {
			var nodeVM *cnsvsphere.VirtualMachine
			if volTopology.isCSINodeIdFeatureEnabled &&
				volTopology.clusterFlavor == cnstypes.CnsClusterFlavorVanilla {
				nodeVM, err = volTopology.nodeMgr.GetNode(ctx,
					nodeTopologyInstance.Spec.NodeUUID, nil)
			} else {
				nodeVM, err = volTopology.nodeMgr.GetNodeByName(ctx,
					nodeTopologyInstance.Spec.NodeID)
			}
			if err != nil {
				log.Errorf("failed to retrieve NodeVM %q. Error - %+v", nodeTopologyInstance.Spec.NodeID, err)
				return nil, err
			}
			matchingNodeVMs = append(matchingNodeVMs, nodeVM)
		}
	}
	return matchingNodeVMs, nil
}

// GetTopologyInfoFromNodes retrieves the topology information of the given
// list of node names using the information from CSINodeTopology instances.
func (volTopology *controllerVolumeTopology) GetTopologyInfoFromNodes(ctx context.Context, reqParams interface{}) (
	[]map[string]string, error) {
	log := logger.GetLogger(ctx)
	params := reqParams.(commoncotypes.VanillaRetrieveTopologyInfoParams)
	var topologySegments []map[string]string

	// Fetch node topology information from informer cache.
	nodeTopologyStore := volTopology.csiNodeTopologyInformer.GetStore()
	for _, nodeName := range params.NodeNames {
		// Fetch CSINodeTopology instance using node name.
		item, exists, err := nodeTopologyStore.GetByKey(nodeName)
		if err != nil || !exists {
			return nil, logger.LogNewErrorf(log, "failed to find a CSINodeTopology instance with name: %q. "+
				"Error: %+v", nodeName, err)
		}

		// Validate the object received.
		var nodeTopologyInstance csinodetopologyv1alpha1.CSINodeTopology
		err = runtime.DefaultUnstructuredConverter.FromUnstructured(item.(*unstructured.Unstructured).Object,
			&nodeTopologyInstance)
		if err != nil {
			return nil, logger.LogNewErrorf(log, "failed to convert unstructured object %+v to "+
				"CSINodeTopology instance. Error: %+v", item, err)
		}
		// Check the status of CSINodeTopology instance.
		if nodeTopologyInstance.Status.Status != csinodetopologyv1alpha1.CSINodeTopologySuccess {
			return nil, logger.LogNewErrorf(log, "CSINodeTopology instance with name: %q and Status: %q not "+
				"ready yet", nodeName, nodeTopologyInstance.Status.Status)
		}

		// Convert array of labels in instance to map.
		topoLabels := make(map[string]string)
		for _, topoLabel := range nodeTopologyInstance.Status.TopologyLabels {
			topoLabels[topoLabel.Key] = topoLabel.Value
		}
		// Check if topology labels received are empty.
		if len(topoLabels) == 0 {
			log.Infof("Node %q does not belong to any topology domain. Skipping it for node "+
				"affinity calculation", nodeName)
			continue
		}

		// Check if the topology segments retrieved from node are
		// already present, else add it to topologySegments.
		var alreadyExists bool
		for _, topoMap := range topologySegments {
			if reflect.DeepEqual(topoMap, topoLabels) {
				alreadyExists = true
				break
			}
		}
		if !alreadyExists {
			topologySegments = append(topologySegments, topoLabels)
		}
	}
	log.Infof("Topology segments retrieved from nodes accessible to datastore %q are: %+v",
		params.DatastoreURL, topologySegments)

	// If the datastore is accessible from only one segment, return with it.
	if len(topologySegments) == 1 {
		return topologySegments, nil
	}

	// If the selected datastore is preferred in a zone which matches the topology requirement
	// given by customer, set this zone as the node affinity terms.
	if volTopology.isTopologyPreferentialDatastoresFSSEnabled {
		// Get the intersection between topology requirements and accessible topology domains for given datastore URL.
		var combinedAccessibleTopology []map[string]string
		for _, topology := range params.TopologyRequirement.GetPreferred() {
			reqSegments := topology.GetSegments()
			for _, segments := range topologySegments {
				isMatchingTopoReq := true
				for reqCategory, reqTag := range reqSegments {
					if tag, ok := segments[reqCategory]; ok && tag != reqTag {
						isMatchingTopoReq = false
						break
					}
				}
				if isMatchingTopoReq {
					combinedAccessibleTopology = append(combinedAccessibleTopology, segments)
				}
			}
		}
		// Finally, filter the accessible topologies with topology domains where datastore is preferred.
		var preferredAccessibleTopology []map[string]string
		for _, segments := range combinedAccessibleTopology {
			PreferredDSURLs := getPreferredDatastoresInSegments(ctx, segments)
			if len(PreferredDSURLs) != 0 {
				if _, ok := PreferredDSURLs[params.DatastoreURL]; ok {
					preferredAccessibleTopology = append(preferredAccessibleTopology, segments)
				}
			}
		}
		if len(preferredAccessibleTopology) != 0 {
			return preferredAccessibleTopology, nil
		}
	}

	// Check for each calculated topology segment to see if all nodes in that segment have access to this datastore.
	// This check will filter out topology segments in which all nodes do not have access to the chosen datastore.
	accessibleTopology, err := verifyAllNodesInTopologyAccessibleToDatastore(ctx, params.NodeNames,
		params.DatastoreURL, topologySegments)
	if err != nil {
		return nil, logger.LogNewErrorf(log, "failed to verify if all nodes in the topology segments "+
			"retrieved are accessible to datastore %q. Error: %+v", params.DatastoreURL, err)
	}
	log.Infof("Accessible topology calculated for datastore %q is %+v",
		params.DatastoreURL, accessibleTopology)
	return accessibleTopology, nil
}

func verifyAllNodesInTopologyAccessibleToDatastore(ctx context.Context, nodeNames []string,
	datastoreURL string, topologySegments []map[string]string) ([]map[string]string, error) {
	log := logger.GetLogger(ctx)

	// Create a map of nodeNames for easy retrieval later on.
	accessibleNodeNamesMap := make(map[string]struct{})
	for _, name := range nodeNames {
		accessibleNodeNamesMap[name] = struct{}{}
	}
	// Check if for each topology segment, all the nodes in that segment have access to the chosen datastore.
	var accessibleTopology []map[string]string
	for _, segments := range topologySegments {
		// Create slice of all tag values in the given segments.
		var tagValues []string
		for _, tag := range segments {
			tagValues = append(tagValues, tag)
		}
		if len(tagValues) == 0 {
			continue
		}
		// Find the intersection of node names for all the tagValues using the domainNodeMap cached values.
		var nodesInSegment []string
		for nodeName := range domainNodeMap[tagValues[0]] {
			isPresent := true
			for _, otherTag := range tagValues[1:] {
				if _, exists := domainNodeMap[otherTag][nodeName]; !exists {
					isPresent = false
					break
				}
			}
			if isPresent {
				// nodeName is present in each of the segments.
				nodesInSegment = append(nodesInSegment, nodeName)
			}
		}
		log.Debugf("Nodes %+v belong to topology segment %+v", nodesInSegment, segments)
		// Find if nodesInSegment is a subset of the accessibleNodeNamesMap. If yes, that means all
		// the nodes in the given list of segments have access to the chosen datastore.
		isSubset := true
		for _, segNode := range nodesInSegment {
			if _, exists := accessibleNodeNamesMap[segNode]; !exists {
				log.Infof("Node %q in topology segment %+v does not have access to datastore %q", segNode,
					segments, datastoreURL)
				isSubset = false
				break
			}
		}
		if isSubset {
			accessibleTopology = append(accessibleTopology, segments)
		}
	}
	return accessibleTopology, nil
}

// GetSharedDatastoresInTopology finds out shared datastores associated with the given
// clusterMorefs which match the topology requirement.
func (volTopology *wcpControllerVolumeTopology) GetSharedDatastoresInTopology(ctx context.Context,
	reqParams interface{}) ([]*cnsvsphere.DatastoreInfo, error) {
	log := logger.GetLogger(ctx)
	params := reqParams.(commoncotypes.WCPTopologyFetchDSParams)
	log.Debugf("Get shared datastores with topologyRequirement: %+v", params.TopologyRequirement)
	var sharedDatastores []*cnsvsphere.DatastoreInfo
	if params.TopologyRequirement.GetPreferred() == nil {
		return sharedDatastores, nil
	}

	// Fetch shared datastores for each segment in the preferred topology requirement.
	log.Debugf("Using preferred topology")
	for _, topology := range params.TopologyRequirement.GetPreferred() {
		segments := topology.GetSegments()

		// For each topology segments, fetch cluster morefs satisfying the condition.
		log.Debugf("Getting list of cluster morefs for topology segments %+v", segments)
		clusterMorefs, err := volTopology.getClustersMatchingTopologySegment(ctx, segments)
		if err != nil {
			return nil, logger.LogNewErrorf(log,
				"failed to fetch clusters matching topology requirement. Error: %v", err)
		}
		if len(clusterMorefs) == 0 {
			log.Warnf("No clusters matched the topology requirement provided: %+v",
				segments)
			continue
		}

		// Call GetCandidateDatastores for each cluster moref. Ignore the vsanDirectDatastores for now.
		for _, clusterMoref := range clusterMorefs {
			accessibleDs, _, err := cnsvsphere.GetCandidateDatastoresInCluster(ctx, params.Vc, clusterMoref)
			if err != nil {
				return nil, logger.LogNewErrorf(log,
					"failed to find candidate datastores to place volume in cluster %q. Error: %v",
					clusterMoref, err)
			}
			sharedDatastores = append(sharedDatastores, accessibleDs...)
		}
	}
	return sharedDatastores, nil
}

// getClustersMatchingTopologySegment fetches clusters matching the topology requirement provided by checking
// the azClusterMap cache.
func (volTopology *wcpControllerVolumeTopology) getClustersMatchingTopologySegment(ctx context.Context,
	segments map[string]string) ([]string, error) {
	log := logger.GetLogger(ctx)
	var matchingClusterMorefs []string
	for _, zone := range segments {
		clusterMoref, exists := azClusterMap[zone]
		if !exists || clusterMoref == "" {
			return nil, logger.LogNewErrorf(log, "could not find the cluster MoID for zone %q in "+
				"AvailabilityZone resources", zone)
		}
		matchingClusterMorefs = append(matchingClusterMorefs, clusterMoref)
	}
	log.Infof("Clusters matching topology requirement %+v are %+v", segments, matchingClusterMorefs)
	return matchingClusterMorefs, nil
}

// GetTopologyInfoFromNodes retrieves the topology information of the selected datastore
// using the information from azClusterMap cache.
func (volTopology *wcpControllerVolumeTopology) GetTopologyInfoFromNodes(ctx context.Context, reqParams interface{}) (
	[]map[string]string, error) {
	log := logger.GetLogger(ctx)
	params := reqParams.(commoncotypes.WCPRetrieveTopologyInfoParams)
	var topologySegments []map[string]string

	switch strings.ToLower(params.StorageTopologyType) {
	case "zonal":
		// If the topology requirement received has just one zone, use the same zone as node affinity terms on PV.
		if len(params.TopologyRequirement.GetPreferred()) == 1 {
			topologySegments = append(topologySegments, params.TopologyRequirement.GetPreferred()[0].GetSegments())
		} else {
			// If multiple zones are provided as input in the topology requirement, find the zone
			// to which the selected datastore is associated with. If this search results in multiple zones,
			// randomly choose one as node affinity.
			var selectedSegments []map[string]string
			for _, topology := range params.TopologyRequirement.GetPreferred() {
				for label, value := range topology.GetSegments() {
					clusterMoref, exists := azClusterMap[value]
					if !exists || clusterMoref == "" {
						return nil, logger.LogNewErrorf(log, "could not find the cluster MoID for zone %q in "+
							"AvailabilityZone resources", value)
					}
					datastores, err := params.Vc.GetDatastoresByCluster(ctx, clusterMoref)
					if err != nil {
						return nil, logger.LogNewErrorf(log,
							"failed to fetch datastores associated with cluster %q", clusterMoref)
					}
					for _, ds := range datastores {
						if ds.Info.Url == params.DatastoreURL {
							selectedSegments = append(selectedSegments, map[string]string{label: value})
							break
						}
					}
				}
			}

			numSelectedSegments := len(selectedSegments)
			switch {
			case numSelectedSegments == 0:
				return nil, logger.LogNewErrorf(log,
					"could not find the topology of the volume provisioned on datastore %q", params.DatastoreURL)
			case numSelectedSegments > 1:
				// This situation will arise when datastore belongs to multiple zones but the
				// storageTopologyType is `zonal`. This seems like a configuration error.
				return nil, &common.InvalidTopologyProvisioningError{ErrMsg: fmt.Sprintf(
					"zonal volume is provisioned on %q datastore which is accessible from multiple zones: %+v. "+
						"Kindly check the configuration of the storage policy used in the StorageClass.",
					params.DatastoreURL, selectedSegments)}
			default:
				topologySegments = selectedSegments
			}
		}
	default:
		// This is considered a configuration error.
		return nil, &common.InvalidTopologyProvisioningError{ErrMsg: fmt.Sprintf("unrecognised "+
			"storageTopologyType found: %q", params.StorageTopologyType)}
	}
	log.Infof("Topology of the provisioned volume detected as %+v", topologySegments)
	return topologySegments, nil
}
