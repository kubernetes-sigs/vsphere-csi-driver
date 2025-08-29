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
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"reflect"
	"slices"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/container-storage-interface/spec/lib/go/csi"
	jsonpatch "github.com/evanphx/json-patch/v5"
	cnstypes "github.com/vmware/govmomi/cns/types"
	"github.com/vmware/govmomi/object"
	"github.com/vmware/govmomi/vim25/mo"
	vimtypes "github.com/vmware/govmomi/vim25/types"
	"google.golang.org/grpc/codes"
	v1 "k8s.io/api/core/v1"
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
	clientconfig "sigs.k8s.io/controller-runtime/pkg/client/config"
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

// DatastoreRetriever interface abstracts vSphere datastore operations for testability
type DatastoreRetriever interface {
	// GetCandidateDatastoresInCluster retrieves candidate datastores for a specific cluster
	GetCandidateDatastoresInCluster(ctx context.Context, vc *cnsvsphere.VirtualCenter, clusterID string,
		includeVSANDirect bool) ([]*cnsvsphere.DatastoreInfo, []*cnsvsphere.DatastoreInfo, error)
	// GetSharedDatastoresInClusters retrieves shared datastores across multiple clusters
	GetSharedDatastoresInClusters(ctx context.Context, clusterMorefs []string,
		vc *cnsvsphere.VirtualCenter) ([]*cnsvsphere.DatastoreInfo, error)
}

// VSphereDatastoreRetriever implements DatastoreRetriever using real vSphere operations
type VSphereDatastoreRetriever struct{}

// GetCandidateDatastoresInCluster implements DatastoreRetriever interface
func (v *VSphereDatastoreRetriever) GetCandidateDatastoresInCluster(ctx context.Context,
	vc *cnsvsphere.VirtualCenter, clusterID string, includeVSANDirect bool) ([]*cnsvsphere.DatastoreInfo,
	[]*cnsvsphere.DatastoreInfo, error) {
	return cnsvsphere.GetCandidateDatastoresInCluster(ctx, vc, clusterID, includeVSANDirect)
}

// GetSharedDatastoresInClusters implements DatastoreRetriever interface
func (v *VSphereDatastoreRetriever) GetSharedDatastoresInClusters(ctx context.Context,
	clusterMorefs []string, vc *cnsvsphere.VirtualCenter) ([]*cnsvsphere.DatastoreInfo, error) {
	return getSharedDatastoresInClusters(ctx, clusterMorefs, vc)
}

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
	// azClustersMap maintains a cache of AZ instance name to the clusterMorefs in that zone.
	azClustersMap = make(map[string][]string)
	// azClusterMapInstanceLock guards the azClusterMap and azClustersMap instances from concurrent writes.
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
	isMultiVCSupportEnabled bool
	// isPodVMOnStretchedSupervisorEnabled is set to true only when the podvm-on-stretched-supervisor FSS
	// is enabled
	isPodVMOnStretchedSupervisorEnabled bool
	// csiNodeTopologyInformer refers to a shared K8s informer listening on CSINodeTopology instances
	// in the cluster.
	csiNodeTopologyInformer *cache.SharedIndexInformer
	// zoneInformer is an informer watching the namespaced zone instances in supervisor cluster.
	zoneInformer cache.SharedIndexInformer
	// startZoneInformerOnce ensures zone informer is started only once
	startZoneInformerOnce sync.Once
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
}

// wcpControllerVolumeTopology implements the commoncotypes.ControllerTopologyService
// interface for workload flavor. It stores the necessary kubernetes configurations and informers required
// to implement the methods in the interface.
type wcpControllerVolumeTopology struct {
	//k8sConfig is the in-cluster config for client to talk to the api-server.
	k8sConfig *restclient.Config
	// azInformer is an informer instance on the AvailabilityZone custom resource.
	azInformer cache.SharedIndexInformer
	// datastoreRetriever provides abstraction for vSphere datastore operations
	datastoreRetriever DatastoreRetriever
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

				// Create a cache of topology tags -> VC -> associated MoRefs in that VC to ease volume provisioning.
				err = common.DiscoverTagEntities(ctx)
				if err != nil {
					return nil, logger.LogNewErrorf(log,
						"failed to update cache with topology information. Error: %+v", err)
				}

				controllerVolumeTopologyInstance = &controllerVolumeTopology{
					k8sConfig:               config,
					nodeMgr:                 nodeManager,
					csiNodeTopologyInformer: *csiNodeTopologyInformer,
					clusterFlavor:           clusterFlavor,
				}

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
				log.Info("Topology service initiated successfully")
			}
		} else {
			controllerVolumeTopologyInstanceLock.RUnlock()
		}
		return controllerVolumeTopologyInstance, nil
	} else if c.clusterFlavor == cnstypes.CnsClusterFlavorWorkload {
		// Set isPodVMOnStretchedSupervisorEnabled if podvm-on-stretched-supervisor fss is enabled
		isPodVMOnStretchedSupervisorEnabled = c.IsFSSEnabled(ctx, common.PodVMOnStretchedSupervisor)
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
					k8sConfig:          config,
					azInformer:         *azInformer,
					datastoreRetriever: &VSphereDatastoreRetriever{},
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
	tagMgr, err := vc.GetTagManager(ctx)
	if err != nil {
		return logger.LogNewErrorf(log, "failed to create tag manager. Error: %+v", err)
	}

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
	_, err = availabilityZoneInformer.AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc: func(obj interface{}) {
			azCRAdded(obj)
		},
		UpdateFunc: nil,
		DeleteFunc: func(obj interface{}) {
			azCRDeleted(obj)
		},
	})
	if err != nil {
		return nil, logger.LogNewErrorf(log,
			"failed to add event handler on informer for availabilityzones CR. Error: %v", err)
	}

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
	addToAZClusterMap(ctx, azName, clusterComputeResourceMoIds)
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
func addToAZClusterMap(ctx context.Context, azName string, clusterMorefs []string) {
	log := logger.GetLogger(ctx)
	azClusterMapInstanceLock.Lock()
	defer azClusterMapInstanceLock.Unlock()
	if isPodVMOnStretchedSupervisorEnabled {
		azClustersMap[azName] = clusterMorefs
		log.Infof("Added clusters %v to %q zone in azClustersMap", clusterMorefs, azName)
	} else {
		azClusterMap[azName] = clusterMorefs[0]
		log.Infof("Added %q cluster to %q zone in azClusterMap", clusterMorefs[0], azName)
	}
}

// Removes the provided zone and clusterMoref from the azClusterMap.
func removeFromAZClusterMap(ctx context.Context, azName string) {
	log := logger.GetLogger(ctx)
	azClusterMapInstanceLock.Lock()
	defer azClusterMapInstanceLock.Unlock()
	if isPodVMOnStretchedSupervisorEnabled {
		delete(azClustersMap, azName)
		log.Infof("Removed %q zone from azClustersMap", azName)
	} else {
		delete(azClusterMap, azName)
		log.Infof("Removed %q zone from azClusterMap", azName)
	}
}

// GetAZClustersMap returns the zone to clusterMorefs map from the azClustersMap.
func (volTopology *wcpControllerVolumeTopology) GetAZClustersMap(ctx context.Context) map[string][]string {
	return azClustersMap
}

// GetAZClustersMap returns the zone to clusterMorefs map from the azClustersMap.
func (volTopology *controllerVolumeTopology) GetAZClustersMap(ctx context.Context) map[string][]string {
	return nil
}

// ZonesWithMultipleClustersExist returns true if zone has more than 1 cluster
func (volTopology *wcpControllerVolumeTopology) ZonesWithMultipleClustersExist(ctx context.Context) bool {
	for _, clusters := range volTopology.GetAZClustersMap(ctx) {
		if len(clusters) > 1 {
			return true
		}
	}
	return false
}

// ZonesWithMultipleClustersExist returns true if zone has more than 1 cluster
func (volTopology *controllerVolumeTopology) ZonesWithMultipleClustersExist(ctx context.Context) bool {
	return false
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
	_, err = topologyInformer.AddEventHandler(cache.ResourceEventHandlerFuncs{
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
	if err != nil {
		return nil, logger.LogNewErrorf(log, "failed to add event handler on informer for %q CR. Error: %v",
			csinodetopology.CRDPlural, err)
	}

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
		} else {
			removeNodeFromDomainNodeMap(ctx, oldNodeTopoObj)
		}
	}
	// Add the node name to the domainNodeMap if the Status is set to Success.
	if newNodeTopoObj.Status.Status == csinodetopologyv1alpha1.CSINodeTopologySuccess {
		if isMultiVCSupportEnabled {
			common.AddNodeToDomainNodeMapNew(ctx, newNodeTopoObj)
		} else {
			addNodeToDomainNodeMap(ctx, newNodeTopoObj)
		}
	}
}

// topoCRDeleted removes the CSINodeTopology instance name from the domainNodeMap.
func topoCRDeleted(obj interface{}) {
	ctx, log := logger.GetNewContextWithLogger()
	// Verify object received.
	if unknown, ok := obj.(cache.DeletedFinalStateUnknown); ok {
		if unknown.Obj == nil {
			log.Errorf("topoCRDeleted: received empty DeletedFinalStateUnknown object, ignoring")
			return
		}
		obj = unknown.Obj
	}
	unstruct, ok := obj.(*unstructured.Unstructured)
	if !ok {
		log.Errorf("topoCRDeleted: received non-unstructured object %T, ignoring", obj)
		return
	}
	var nodeTopoObj csinodetopologyv1alpha1.CSINodeTopology
	err := runtime.DefaultUnstructuredConverter.FromUnstructured(unstruct.Object, &nodeTopoObj)
	if err != nil {
		log.Errorf("topoCRDeleted: failed to cast object %+v to %s type. Error: %+v",
			obj, csinodetopology.CRDSingular, err)
		return
	}
	// Delete node name from domainNodeMap if the status of the CR was set to Success.
	if nodeTopoObj.Status.Status == csinodetopologyv1alpha1.CSINodeTopologySuccess {
		if isMultiVCSupportEnabled {
			common.RemoveNodeFromDomainNodeMapNew(ctx, nodeTopoObj)
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
				csiNodeTopologyK8sClient: crClient,
				csiNodeTopologyWatcher:   crWatcher,
				k8sClient:                k8sClient,
				k8sConfig:                config,
				clusterFlavor:            clusterFlavor,
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

	if volTopology.clusterFlavor == cnstypes.CnsClusterFlavorGuest {
		err = createCSINodeTopologyInstance(ctx, volTopology, nodeInfo)
		if err != nil {
			return nil, logger.LogNewErrorCodef(log, codes.Internal, "error %+v", err.Error())
		}
	} else {
		csiNodeTopology := &csinodetopologyv1alpha1.CSINodeTopology{}
		csiNodeTopologyKey := types.NamespacedName{
			Name: nodeInfo.NodeName,
		}
		err = volTopology.csiNodeTopologyK8sClient.Get(ctx, csiNodeTopologyKey, csiNodeTopology)
		csiNodeTopologyFound := true
		if err != nil {
			if !apierrors.IsNotFound(err) {
				msg := fmt.Sprintf("failed to get CsiNodeTopology for the node: %q. Error: %+v", nodeInfo.NodeName, err)
				return nil, logger.LogNewErrorCode(log, codes.Internal, msg)
			}
			csiNodeTopologyFound = false
			err = createCSINodeTopologyInstance(ctx, volTopology, nodeInfo)
			if err != nil {
				return nil, logger.LogNewErrorCodef(log, codes.Internal, "error %+v", err.Error())
			}
		}
		// There is an already existing topology.
		if csiNodeTopologyFound {
			newCSINodeTopology := csiNodeTopology.DeepCopy()
			newCSINodeTopology = volTopology.updateNodeIDForTopology(ctx, nodeInfo, newCSINodeTopology)
			// reset the status so as syncer can sync the object again
			newCSINodeTopology.Status.Status = ""
			_, err = volTopology.patchCSINodeTopology(ctx, csiNodeTopology, newCSINodeTopology)
			if err != nil {
				msg := fmt.Sprintf("Fail to patch CsiNodeTopology for the node: %q "+
					"with nodeUUID: %s. Error: %+v",
					nodeInfo.NodeName, nodeInfo.NodeID, err)
				return nil, logger.LogNewErrorCode(log, codes.Internal, msg)
			}
			log.Infof("Successfully patched CSINodeTopology instance: %q with Uuid: %q",
				nodeInfo.NodeName, nodeInfo.NodeID)
		}
	}

	// Create a watcher for CSINodeTopology CRs.
	timeoutSeconds := int64((time.Duration(getCSINodeTopologyWatchTimeoutInMin(ctx)) * time.Minute).Seconds())
	watchCSINodeTopology, err := volTopology.csiNodeTopologyWatcher.WatchWithContext(ctx, metav1.ListOptions{
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

func (volTopology *nodeVolumeTopology) updateNodeIDForTopology(
	ctx context.Context,
	nodeInfo *commoncotypes.NodeInfo,
	csiNodeTopology *csinodetopologyv1alpha1.CSINodeTopology) *csinodetopologyv1alpha1.CSINodeTopology {
	log := logger.GetLogger(ctx)
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
		csiNodeTopology.Spec.NodeID = nodeInfo.NodeName
		csiNodeTopology.Spec.NodeUUID = nodeInfo.NodeID
	}
	return csiNodeTopology
}

func (volTopology *nodeVolumeTopology) patchCSINodeTopology(
	ctx context.Context,
	oldTopo, newTopo *csinodetopologyv1alpha1.CSINodeTopology) (*csinodetopologyv1alpha1.CSINodeTopology, error) {
	patch, err := getCSINodePatchData(oldTopo, newTopo, true)
	if err != nil {
		return oldTopo, err
	}
	rawPatch := client.RawPatch(types.MergePatchType, patch)
	err = volTopology.csiNodeTopologyK8sClient.Patch(ctx, oldTopo, rawPatch)
	if err != nil {
		return oldTopo, err
	}
	return newTopo, nil
}

func getCSINodePatchData(
	oldNodeTopology, newNodeTopology *csinodetopologyv1alpha1.CSINodeTopology,
	addResourceVersionCheck bool) ([]byte, error) {
	patchBytes, err := getPatchData(oldNodeTopology, newNodeTopology)
	if err != nil {
		return nil, err
	}
	if addResourceVersionCheck {
		patchBytes, err = addResourceVersion(patchBytes, oldNodeTopology.ResourceVersion)
		if err != nil {
			return nil, fmt.Errorf("apply ResourceVersion to patch data failed: %v", err)
		}
	}
	return patchBytes, nil
}

func addResourceVersion(patchBytes []byte, resourceVersion string) ([]byte, error) {
	var patchMap map[string]interface{}
	err := json.Unmarshal(patchBytes, &patchMap)
	if err != nil {
		return nil, fmt.Errorf("error unmarshalling patch with %v", err)
	}
	u := unstructured.Unstructured{Object: patchMap}
	a, err := apiMeta.Accessor(&u)
	if err != nil {
		return nil, fmt.Errorf("error creating accessor with  %v", err)
	}
	a.SetResourceVersion(resourceVersion)
	versionBytes, err := json.Marshal(patchMap)
	if err != nil {
		return nil, fmt.Errorf("error marshalling json patch with %v", err)
	}
	return versionBytes, nil
}

func getPatchData(oldObj, newObj interface{}) ([]byte, error) {
	oldData, err := json.Marshal(oldObj)
	if err != nil {
		return nil, fmt.Errorf("marshal old object failed: %v", err)
	}
	newData, err := json.Marshal(newObj)
	if err != nil {
		return nil, fmt.Errorf("marshal new object failed: %v", err)
	}
	patchBytes, err := jsonpatch.CreateMergePatch(oldData, newData)
	if err != nil {
		return nil, fmt.Errorf("CreateMergePatch failed: %v", err)
	}
	return patchBytes, nil
}

// Create new CSINodeTopology instance if it doesn't exist
// Create CSINodeTopology instance with spec.nodeID and spec.nodeUUID
// if cluster flavor is Vanilla
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
	if volTopology.clusterFlavor == cnstypes.CnsClusterFlavorVanilla {
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
		matchingNodeVMs, completeTopologySegments, err = volTopology.getTopologySegmentsWithMatchingNodes(ctx,
			segments)
		if err != nil {
			log.Errorf("failed to find nodes in topology segment %+v. Error: %+v", segments, err)
			return nil, err
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
			if volTopology.clusterFlavor == cnstypes.CnsClusterFlavorVanilla {
				nodeVM, err = volTopology.nodeMgr.GetNodeVMAndUpdateCache(ctx,
					nodeTopologyInstance.Spec.NodeUUID, nil)
			} else {
				nodeVM, err = volTopology.nodeMgr.GetNodeVMByNameAndUpdateCache(ctx,
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
		zone := segments[v1.LabelTopologyZone]

		// For each topology segments, fetch cluster morefs satisfying the condition.
		log.Debugf("Getting list of cluster morefs for topology segments %+v", segments)
		clusterMorefs, err := volTopology.getClustersMatchingTopologySegment(ctx, zone)
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
		if !isPodVMOnStretchedSupervisorEnabled {
			// This code block assume we have 1 Cluster Per AZ
			accessibleDs, _, err := volTopology.datastoreRetriever.GetCandidateDatastoresInCluster(ctx,
				params.Vc, clusterMorefs[0], false)
			if err != nil {
				return nil, logger.LogNewErrorf(log,
					"failed to find candidate datastores to place volume in cluster %q. Error: %v",
					clusterMorefs[0], err)
			}
			params.TopoSegToDatastoresMap[zone] = accessibleDs
			sharedDatastores = append(sharedDatastores, accessibleDs...)
			log.Debugf("PodVMOnStretchedSupervisor Not Enabled. Topology: %+v, Zone: %s, "+
				"Topology Segments to Datastore Map: %+v, Datastores: %v",
				params.TopologyRequirement, zone, params.TopoSegToDatastoresMap, sharedDatastores)
		} else {
			// This code block adds support for multiple vSphere Clusters Per AZ
			// sharedDatastores will be calculated for all clusters within AZ
			sharedDatastoresForclusterMorefs, err := volTopology.datastoreRetriever.GetSharedDatastoresInClusters(ctx,
				clusterMorefs, params.Vc)
			if err != nil {
				return nil, logger.LogNewErrorf(log, "failed to get shared datastores "+
					"for clusters: %v, err: %v", clusterMorefs, err)
			}
			params.TopoSegToDatastoresMap[zone] = sharedDatastoresForclusterMorefs
			sharedDatastores = append(sharedDatastores, sharedDatastoresForclusterMorefs...)
			log.Debugf("PodVMOnStretchedSupervisor Enabled. Topology: %+v, Zone: %s, "+
				"Topology Segments to Datastore Map: %+v, Datastores: %v",
				params.TopologyRequirement, zone, params.TopoSegToDatastoresMap, sharedDatastores)
		}
	}
	log.Infof("Shared datastores %v for topologyRequirement: %+v", sharedDatastores,
		params.TopologyRequirement)
	return sharedDatastores, nil
}

// getClustersMatchingTopologySegment fetches clusters matching the topology requirement provided by checking
// the azClusterMap cache.
func (volTopology *wcpControllerVolumeTopology) getClustersMatchingTopologySegment(ctx context.Context, zone string) (
	[]string, error) {
	log := logger.GetLogger(ctx)
	var matchingClusterMorefs []string
	if isPodVMOnStretchedSupervisorEnabled {
		clusterMorefs, exists := azClustersMap[zone]
		if !exists || len(clusterMorefs) == 0 {
			return nil, logger.LogNewErrorf(log, "could not find the cluster MoIDs for zone %q in "+
				"AvailabilityZone resources", zone)
		}
		matchingClusterMorefs = append(matchingClusterMorefs, clusterMorefs...)
	} else {
		clusterMoref, exists := azClusterMap[zone]
		if !exists || clusterMoref == "" {
			return nil, logger.LogNewErrorf(log, "could not find the cluster MoID for zone %q in "+
				"AvailabilityZone resources", zone)
		}
		matchingClusterMorefs = append(matchingClusterMorefs, clusterMoref)
	}
	log.Infof("Clusters matching topology requirement %q are %+v", zone, matchingClusterMorefs)
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
		if params.TopologyRequirement == nil {
			// This case is for static volume provisioning using CNSRegisterVolume API
			if !isPodVMOnStretchedSupervisorEnabled {
				return nil, logger.LogNewErrorf(log, "topology requirement should not be nil. invalid "+
					"params: %v", params)
			} else {
				// If the topology requirement received is nil, then identify topology of the datastore by looking into
				// azClustersMap
				var selectedSegments []map[string]string
				for az, clusters := range azClustersMap {
					sharedDatastoresForclusters, err := getSharedDatastoresInClusters(ctx, clusters, params.Vc)
					if err != nil {
						return nil, logger.LogNewErrorf(log, "failed to get shared datastores "+
							"for clusters: %v, err: %v", clusters, err)
					}
					for _, ds := range sharedDatastoresForclusters {
						if ds.Info.Url == params.DatastoreURL {
							selectedSegments = append(selectedSegments, map[string]string{v1.LabelTopologyZone: az})
							break
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
		} else if len(params.TopologyRequirement.GetPreferred()) == 1 {
			// If the topology requirement received has just one zone, use the same zone as node affinity terms on PV.
			topologySegments = append(topologySegments, params.TopologyRequirement.GetPreferred()[0].GetSegments())
		} else {
			// If multiple zones are provided as input in the topology requirement, find the zone
			// to which the selected datastore is associated with.
			var selectedSegments []map[string]string
			for _, topology := range params.TopologyRequirement.GetPreferred() {
				for label, value := range topology.GetSegments() {
					datastores := params.TopoSegToDatastoresMap[value]
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
	// In VC 9.0, if StorageTopologyType is not set, all the zones the selected datastore
	// is accessible from will be added as node affinity terms on the PV even if the zones
	// are not associated with the namespace of the PVC.
	// This code block runs for static as well as dynamic volume provisioning case.
	case "":
		// TopoSegToDatastoresMap will be nil in case of static volume provisioning.
		if params.TopoSegToDatastoresMap == nil {
			params.TopoSegToDatastoresMap = make(map[string][]*cnsvsphere.DatastoreInfo)
		}
		var selectedSegments []map[string]string
		for zone, clusters := range azClustersMap {
			if _, exists := params.TopoSegToDatastoresMap[zone]; !exists {
				sharedDatastoresForClusters, err := getSharedDatastoresInClusters(ctx, clusters, params.Vc)
				if err != nil {
					return nil, logger.LogNewErrorf(log, "failed to get shared datastores for clusters: %v, "+
						"err: %v", clusters, err)
				}
				params.TopoSegToDatastoresMap[zone] = sharedDatastoresForClusters
			}
		}
		for zone, datastores := range params.TopoSegToDatastoresMap {
			for _, ds := range datastores {
				if ds.Info.Url == params.DatastoreURL {
					selectedSegments = append(selectedSegments, map[string]string{v1.LabelTopologyZone: zone})
					break
				}
			}
		}
		if len(selectedSegments) == 0 {
			return nil, logger.LogNewErrorf(log,
				"could not find the topology of the volume provisioned on datastore %q", params.DatastoreURL)
		}
		topologySegments = selectedSegments
	default:
		// This is considered a configuration error.
		return nil, &common.InvalidTopologyProvisioningError{ErrMsg: fmt.Sprintf("unrecognised "+
			"storageTopologyType found: %q", params.StorageTopologyType)}
	}
	log.Infof("Topology of the provisioned volume detected as %+v", topologySegments)
	return topologySegments, nil
}

// getSharedDatastoresInClusters helps find shared datastores accessible to all given clusters
func getSharedDatastoresInClusters(ctx context.Context, clusterMorefs []string,
	vc *cnsvsphere.VirtualCenter) ([]*cnsvsphere.DatastoreInfo, error) {
	log := logger.GetLogger(ctx)
	var sharedDatastoresForclusterMorefs []*cnsvsphere.DatastoreInfo
	for index, clusterMoref := range clusterMorefs {
		accessibleDs, _, err := cnsvsphere.GetCandidateDatastoresInCluster(ctx, vc, clusterMoref, false)
		if err != nil {
			return nil, logger.LogNewErrorf(log,
				"failed to find candidate datastores to place volume in cluster %q. Error: %v",
				clusterMoref, err)
		}
		if len(accessibleDs) == 0 {
			return nil, logger.LogNewErrorf(log,
				"no accessibleDs candidate datastores found to place volume for cluster %v", clusterMoref)
		}
		if index == 0 {
			sharedDatastoresForclusterMorefs = append(sharedDatastoresForclusterMorefs, accessibleDs...)
		} else {
			var sharedAccessibleDatastores []*cnsvsphere.DatastoreInfo
			for _, sharedDatastore := range sharedDatastoresForclusterMorefs {
				for _, accessibleDsInCluster := range accessibleDs {
					if sharedDatastore.Info.Url == accessibleDsInCluster.Info.Url {
						sharedAccessibleDatastores = append(sharedAccessibleDatastores, accessibleDsInCluster)
						break
					}
				}
			}
			if len(sharedAccessibleDatastores) == 0 {
				return nil, logger.LogNewErrorf(log,
					"no shared candidate datastores found to place volume for clusters %v", clusterMorefs)
			}
			sharedDatastoresForclusterMorefs = sharedAccessibleDatastores
		}
	}
	return sharedDatastoresForclusterMorefs, nil
}

// StartZonesInformer watches on changes to Zone instances.
func (c *K8sOrchestrator) StartZonesInformer(ctx context.Context, restClientConfig *restclient.Config,
	namespace string) error {

	log := logger.GetLogger(ctx)
	var initErr error
	var stopCh chan struct{}

	// Internal reset function - closes stopCh and resets globals
	resetForRetry := func() {
		if stopCh != nil {
			close(stopCh)
			stopCh = nil
		}
		zoneInformer = nil
		startZoneInformerOnce = sync.Once{}
	}

	// Internal initialization function
	startInternal := func() error {
		if restClientConfig == nil {
			cfg, err := clientconfig.GetConfig()
			if err != nil {
				return logger.LogNewErrorf(log,
					"failed to get restClientConfig. Error: %+v", err)
			}
			restClientConfig = cfg
		}

		dynInformer, err := k8s.GetDynamicInformer(ctx,
			"topology.tanzu.vmware.com", "v1alpha1", "zones",
			namespace, restClientConfig, false)
		if err != nil {
			return logger.LogNewErrorf(log,
				"failed to create dynamic informer for Zones CR. Error: %+v", err)
		}
		zoneInformer = dynInformer.Informer()

		stopCh = make(chan struct{})
		go func() {
			log.Info("Informer to watch on Zones CR starting..")
			zoneInformer.Run(stopCh)
		}()

		if !cache.WaitForCacheSync(ctx.Done(), zoneInformer.HasSynced) {
			return logger.LogNewErrorf(log, "Zone informer cache sync failed")
		}
		return nil
	}

	// Execute initialization only once
	startZoneInformerOnce.Do(func() {
		initErr = startInternal()
		if initErr != nil {
			resetForRetry()
		}
	})
	return initErr
}

// GetZonesForNamespace fetches the zones associated with a namespace.
func (c *K8sOrchestrator) GetZonesForNamespace(targetNS string) map[string]struct{} {
	var zonesMap map[string]struct{}

	// Get zones instances from the informer store.
	zones := zoneInformer.GetStore()
	for _, zoneObj := range zones.List() {
		// Only consider zones in targetNS.
		if zoneObj.(*unstructured.Unstructured).GetNamespace() != targetNS {
			continue
		}
		// Only add zones without a deletion timestamp.
		if zoneObj.(*unstructured.Unstructured).GetDeletionTimestamp() == nil {
			if zonesMap == nil {
				zonesMap = map[string]struct{}{zoneObj.(*unstructured.Unstructured).GetName(): {}}
			} else {
				zonesMap[zoneObj.(*unstructured.Unstructured).GetName()] = struct{}{}

			}
		}
	}
	return zonesMap
}

// GetActiveClustersForNamespaceInRequestedZones fetches the active clusters from the zones associated with a namespace
// for requested zone
func (c *K8sOrchestrator) GetActiveClustersForNamespaceInRequestedZones(ctx context.Context,
	targetNS string, requestedZones []string) ([]string, error) {
	log := logger.GetLogger(ctx)
	var activeClusters []string
	// Get zones instances from the informer store.
	zones := zoneInformer.GetStore()
	for _, zoneObj := range zones.List() {
		// Only consider zones in targetNS.
		zoneObjUnstructured := zoneObj.(*unstructured.Unstructured)
		// Only get active clusters from zones without a deletion timestamp.
		if zoneObjUnstructured.GetDeletionTimestamp() == nil {
			log.Debugf("skipping zone:%q as it is being deleted", zoneObjUnstructured.GetName())
			continue
		}
		// check if zone belong to namespace specified with targetNS and belong to
		// volume requirement specified with requestedZones
		if zoneObjUnstructured.GetNamespace() != targetNS && !slices.Contains(requestedZones, zoneObjUnstructured.GetName()) {
			log.Debugf("skipping zone: %q as it does not match requested targetNS: %q and requestedZones: %v requirement",
				zoneObjUnstructured.GetName(), targetNS, requestedZones)
			continue
		}
		// capture active cluster on the namespace from zone CR instance
		clusters, found, err := unstructured.NestedStringSlice(zoneObjUnstructured.Object,
			"spec", "namespace", "clusterMoIDs")
		if err != nil {
			return nil, logger.LogNewErrorf(log, "failed to get clusterMoIDs from zone instance :%q. err :%v",
				zoneObj.(*unstructured.Unstructured).GetName(), err)
		}
		if !found {
			return nil, logger.LogNewErrorf(log, "clusterMoIDs not found in zone instance :%q",
				zoneObj.(*unstructured.Unstructured).GetName())
		}
		activeClusters = append(activeClusters, clusters...)
	}
	if len(activeClusters) == 0 {
		return nil, logger.LogNewErrorf(log, "could not find active cluster for the namespace  %q "+
			"in requested zones: %v", targetNS, requestedZones)
	}
	log.Infof("active clusters: %v for namespace: %q in requested zones: %v", activeClusters, targetNS, requestedZones)
	return activeClusters, nil
}
