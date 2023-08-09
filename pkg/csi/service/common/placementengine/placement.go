package placementengine

import (
	"context"
	"reflect"

	vimtypes "github.com/vmware/govmomi/vim25/types"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"

	"sigs.k8s.io/vsphere-csi-driver/v3/pkg/common/cns-lib/node"
	cnsvsphere "sigs.k8s.io/vsphere-csi-driver/v3/pkg/common/cns-lib/vsphere"
	"sigs.k8s.io/vsphere-csi-driver/v3/pkg/csi/service/common"
	"sigs.k8s.io/vsphere-csi-driver/v3/pkg/csi/service/common/commonco"
	"sigs.k8s.io/vsphere-csi-driver/v3/pkg/csi/service/logger"
	csinodetopologyv1alpha1 "sigs.k8s.io/vsphere-csi-driver/v3/pkg/internalapis/csinodetopology/v1alpha1"
)

// GetSharedDatastores retrieves the shared accessible datastores for hosts associated
// with the topology segments requested by user.
func GetSharedDatastores(ctx context.Context, reqParams interface{}) (
	[]*cnsvsphere.DatastoreInfo, error) {
	log := logger.GetLogger(ctx)
	params := reqParams.(VanillaSharedDatastoresParams)
	nodeMgr := node.GetManager(ctx)

	log.Infof("GetSharedDatastores called with policyID: %q , Topology Segment List: %v",
		params.StoragePolicyID, params.TopologySegmentsList)
	var sharedDatastores []*cnsvsphere.DatastoreInfo
	// Iterate through each set of topology segments and find shared datastores for that segment.
	/* For example, if the topology environment is as follows:
	VC
	|-> DC (Category: region, Tag: region1)
		|-> Cluster1 (Category: zone, Tag: zone1)
			Node1
			Node2
		|-> Cluster2 (Category: zone, Tag: zone2)
			Node3
			Node4
		|-> Cluster3 (Category: zone, Tag: zone3)
			(No nodeVMs in Cluster3 yet)

	If the user chooses to provision a volume in region1, according to the code below:
	`params.TopologySegmentsList` will look like
	[
		map[string]string{region:region1}
	]
	`reqSegment` will look like map[string]string{region:region1}
	`completeTopologySegments` will look like
	[
		map[string]string{region:region1, zone:zone1},
		map[string]string{region:region1, zone:zone2}
	]
	*/
	for _, reqSegment := range params.TopologySegmentsList {
		// Fetch the complete hierarchy of topology segments.
		completeTopologySegments, err := getExpandedTopologySegments(ctx, reqSegment, nodeMgr)
		if err != nil {
			return nil, logger.LogNewErrorf(log, "failed to find nodes in topology segment %+v. Error: %+v",
				reqSegment, err)
		}
		if len(completeTopologySegments) == 0 {
			log.Warnf("No nodes in the cluster matched the topology requirement: %+v",
				reqSegment)
			continue
		}
		log.Infof("TopologySegments expanded as: %+v", completeTopologySegments)
		// For each segment in the complete topology segments hierarchy, get the matching hosts.
		for _, segment := range completeTopologySegments {
			hostMoRefs, err := common.GetHostsForSegment(ctx, segment, params.Vcenter)
			if err != nil {
				return nil, logger.LogNewErrorf(log,
					"failed to fetch hosts belonging to topology segment %+v. Error: %+v", segment, err)
			}
			// Fetch shared datastores accessible to all the hosts in this segment.
			sharedDatastoresInTopologySegment, err := cnsvsphere.GetSharedDatastoresForHosts(ctx, hostMoRefs)
			if err != nil {
				if err == cnsvsphere.ErrNoSharedDSFound {
					log.Warnf("no shared datastores found for hosts %+v belonging to topology segment: %+v",
						hostMoRefs, segment)
					continue
				}
				return nil, logger.LogNewErrorf(log, "failed to get shared datastores for hosts: %+v "+
					"in topology segment %+v. Error: %+v", hostMoRefs, segment, err)
			}
			log.Infof("Obtained list of shared datastores %+v for hosts %+v", sharedDatastoresInTopologySegment,
				hostMoRefs)

			// Filter the shared datastores with preferential datastores, if any.
			// Datastore comparison by URL.
			if common.PreferredDatastoresExist {
				// Fetch all preferred datastore URLs for the topology segment.
				prefDS := common.GetPreferredDatastoresInSegments(ctx, segment, params.Vcenter.Config.Host)
				log.Infof("Preferential datastores %v found for topology segment: %v on vCenter: %q", prefDS,
					segment, params.Vcenter.Config.Host)
				if len(prefDS) != 0 {
					// If there are preferred datastores among the shared
					// datastores, choose the preferred datastores.
					var preferredDS []*cnsvsphere.DatastoreInfo
					for _, dsInfo := range sharedDatastoresInTopologySegment {
						if _, ok := prefDS[dsInfo.Info.Url]; ok {
							preferredDS = append(preferredDS, dsInfo)
						}
					}
					if len(preferredDS) != 0 {
						sharedDatastoresInTopologySegment = preferredDS
						log.Infof("Using preferred datastores: %+v", preferredDS)
					} else {
						log.Infof("No preferential datastore selected for volume provisioning")
					}
				}
			}
			// Add the datastore list to sharedDatastores without duplicates.
			for _, ds := range sharedDatastoresInTopologySegment {
				var found bool
				for _, sharedDS := range sharedDatastores {
					if ds.Info.Url == sharedDS.Info.Url {
						found = true
						break
					}
				}
				if !found {
					sharedDatastores = append(sharedDatastores, ds)
				}
			}
		}
	}

	// Check storage policy compatibility, if given.
	// Datastore comparison by moref.
	if params.StoragePolicyID != "" {
		var sharedDSMoRef []vimtypes.ManagedObjectReference
		for _, ds := range sharedDatastores {
			sharedDSMoRef = append(sharedDSMoRef, ds.Reference())
		}
		compat, err := params.Vcenter.PbmCheckCompatibility(ctx, sharedDSMoRef, params.StoragePolicyID)
		if err != nil {
			return nil, logger.LogNewErrorf(log, "failed to find datastore compatibility "+
				"with storage policy ID %q. vCenter: %q  Error: %+v", params.StoragePolicyID,
				params.Vcenter.Config.Host, err)
		}
		compatibleDsMoids := make(map[string]struct{})
		for _, ds := range compat.CompatibleDatastores() {
			compatibleDsMoids[ds.HubId] = struct{}{}
		}
		log.Infof("Datastores compatible with storage policy %q are %+v for vCenter: %q",
			params.StoragePolicyID, compatibleDsMoids, params.Vcenter.Config.Host)

		// Filter compatible datastores from shared datastores list.
		var compatibleDatastores []*cnsvsphere.DatastoreInfo
		for _, ds := range sharedDatastores {
			if _, exists := compatibleDsMoids[ds.Reference().Value]; exists {
				compatibleDatastores = append(compatibleDatastores, ds)
			}
		}
		if len(compatibleDatastores) == 0 {
			return nil, logger.LogNewErrorf(log,
				"No compatible shared datastores found for storage policy %q on vCenter: %q",
				params.StoragePolicyID, params.Vcenter.Config.Host)
		}
		sharedDatastores = compatibleDatastores
	}

	if len(sharedDatastores) != 0 {
		log.Infof("Shared compatible datastores being considered for volume provisioning on vCenter: %q are: %+v",
			params.Vcenter.Config.Host, sharedDatastores)
	}
	return sharedDatastores, nil
}

// getExpandedTopologySegments expands the user given topology requirement to depict the complete hierarchy.
// NOTE: If there is no nodeVM in an AZ, that AZ will be skipped in complete topology hierarchy.
func getExpandedTopologySegments(ctx context.Context, requestedSegments map[string]string,
	nodeMgr node.Manager) ([]map[string]string, error) {
	log := logger.GetLogger(ctx)

	var (
		vcHost                   string
		completeTopologySegments []map[string]string
	)
	// Fetch node topology information from informer cache.
	for _, val := range commonco.ContainerOrchestratorUtility.GetCSINodeTopologyInstancesList() {
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
		topoLabelsMap := make(map[string]string)
		for _, topoLabel := range nodeTopologyInstance.Status.TopologyLabels {
			topoLabelsMap[topoLabel.Key] = topoLabel.Value
		}
		// Check for a match of labels in every segment.
		isMatch := true
		for key, value := range requestedSegments {
			if topoLabelsMap[key] != value {
				log.Debugf("Node %q with topology %+v did not match the topology requirement - %q: %q ",
					nodeTopologyInstance.Name, topoLabelsMap, key, value)
				isMatch = false
				break
			}
		}
		// If there is a match, check if each compatible NodeVM belongs to the same VC. If not,
		// error out as we do not support cross-zonal volume provisioning.
		if isMatch {
			nodeVM, err := nodeMgr.GetNode(ctx, nodeTopologyInstance.Spec.NodeUUID, nil)
			if err != nil {
				return nil, logger.LogNewErrorf(log,
					"failed to retrieve NodeVM %q. Error - %+v", nodeTopologyInstance.Spec.NodeID, err)
			}
			if vcHost == "" {
				vcHost = nodeVM.VirtualCenterHost
			} else if vcHost != nodeVM.VirtualCenterHost {
				return nil, logger.LogNewErrorf(log,
					"found NodeVM %q belonging to different vCenter: %q. Expected vCenter: %q",
					nodeVM.Name(), nodeVM.VirtualCenterHost, vcHost)
			}

			// Store the complete hierarchy of topology requestedSegments for future use.
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

	return completeTopologySegments, nil
}

// GetTopologyInfoFromNodes retrieves the topology information of the given
// list of node names using the information from CSINodeTopology instances.
func GetTopologyInfoFromNodes(ctx context.Context, reqParams interface{}) (
	[]map[string]string, error) {
	log := logger.GetLogger(ctx)
	params := reqParams.(VanillaRetrieveTopologyInfoParams)
	var topologySegments []map[string]string

	// Fetch node topology information from informer cache.
	for _, nodeName := range params.NodeNames {
		// Fetch CSINodeTopology instance using node name.
		item, exists, err := commonco.ContainerOrchestratorUtility.GetCSINodeTopologyInstanceByName(nodeName)
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

		// Check if the topology segments retrieved from node are already present, else add it to topologySegments.
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
	if common.PreferredDatastoresExist {
		// Get the intersection between topology requirements and accessible topology domains for given datastore URL.
		var combinedAccessibleTopology []map[string]string
		for _, reqSegments := range params.RequestedTopologySegments {
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
			PreferredDSURLs := common.GetPreferredDatastoresInSegments(ctx, segments, params.VCHost)
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
	accessibleTopology, err := common.VerifyAllNodesInTopologyAccessibleToDatastore(ctx, params.NodeNames,
		params.DatastoreURL, topologySegments)
	if err != nil {
		return nil, logger.LogNewErrorf(log, "failed to verify if all nodes in the topology segments "+
			"retrieved are accessible to datastore %q. Error: %+v", params.DatastoreURL, err)
	}
	log.Infof("Accessible topology calculated for datastore %q is %+v",
		params.DatastoreURL, accessibleTopology)
	return accessibleTopology, nil
}
