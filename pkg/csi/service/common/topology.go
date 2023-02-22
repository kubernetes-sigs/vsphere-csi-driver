package common

import (
	"context"
	"sync"

	"github.com/container-storage-interface/spec/lib/go/csi"
	"github.com/vmware/govmomi/object"
	"github.com/vmware/govmomi/vim25/mo"
	"sigs.k8s.io/vsphere-csi-driver/v3/pkg/common/config"

	"sigs.k8s.io/vsphere-csi-driver/v3/pkg/common/cns-lib/node"
	cnsvsphere "sigs.k8s.io/vsphere-csi-driver/v3/pkg/common/cns-lib/vsphere"
	"sigs.k8s.io/vsphere-csi-driver/v3/pkg/csi/service/logger"
	csinodetopologyv1alpha1 "sigs.k8s.io/vsphere-csi-driver/v3/pkg/internalapis/csinodetopology/v1alpha1"
)

var (
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
	// PreferredDatastoresExist signifies if preferred datastores are present in the cluster.
	PreferredDatastoresExist bool
	// preferredDatastoresMap is a map of topology domain to list of
	// datastore URLs preferred in that domain for each VC.
	// Ex: {"vc1": {"zone1": [DSURL1, DSURL2], "zone2": [DSURL3]}, "vc2": {"zone3": [DSURL5]} }
	preferredDatastoresMap = make(map[string]map[string][]string)
	// preferredDatastoresMapInstanceLock guards the preferredDatastoresMap from read-write overlaps.
	preferredDatastoresMapInstanceLock = &sync.RWMutex{}
	// topologyVCMapInstanceLock guards the topologyVCMap instance from concurrent writes.
	topologyVCMapInstanceLock = &sync.RWMutex{}
	// topologyVCMap maintains a cache of topology tags to the vCenter IP/FQDN which holds the tag.
	// Example - {region1: {VC1: struct{}{}, VC2: struct{}{}},
	//            zone1: {VC1: struct{}{}},
	//            zone2: {VC2: struct{}{}}}
	// The vCenter IP/FQDN under each tag are maintained as a map of string with nil values to improve
	// retrieval and deletion performance.
	topologyVCMap = make(map[string]map[string]struct{})
)

// GetAccessibilityRequirementsByVC clubs the accessibility requirements by the VC they belong to.
func GetAccessibilityRequirementsByVC(ctx context.Context, topoReq *csi.TopologyRequirement) (
	map[string][]map[string]string, error) {
	log := logger.GetLogger(ctx)
	// NOTE: We are only checking for preferred segments as vSphere CSI driver follows strict topology.
	if topoReq.GetPreferred() == nil {
		return nil, logger.LogNewErrorf(log, "No preferred segments specified in the "+
			"accessibility requirements.")
	}
	vcTopoSegmentsMap := make(map[string][]map[string]string)
	for _, topology := range topoReq.GetPreferred() {
		segments := topology.GetSegments()
		vcHost, err := getVCForTopologySegments(ctx, segments)
		if err != nil {
			return nil, logger.LogNewErrorf(log, "failed to fetch vCenter associated with topology segments: %+v , err: %v",
				segments, err)
		}
		vcTopoSegmentsMap[vcHost] = append(vcTopoSegmentsMap[vcHost], segments)
	}
	return vcTopoSegmentsMap, nil
}

// getVCForTopologySegments uses the topologyVCMap to retrieve the
// VC instance for the given topology segments map in a multi-VC environment.
func getVCForTopologySegments(ctx context.Context, topologySegments map[string]string) (string, error) {
	log := logger.GetLogger(ctx)
	// vcCountMap keeps a cumulative count of the occurrences of
	// VCs across all labels in the given topology segment.
	vcCountMap := make(map[string]int)

	// Find the VC which contains all the labels given in the topologySegments.
	// For example, if topologyVCMap looks like
	// {"region-1": {"vc1": struct{}{}, "vc2": struct{}{} },
	// "zone-1": {"vc1": struct{}{} },
	// "zone-2": {"vc2": struct{}{} },}
	// For a given topologySegment
	// {"topology.csi.vmware.com/k8s-region": "region-1",
	// "topology.csi.vmware.com/k8s-zone": "zone-2"}
	// we will end up with a vcCountMap as follows: {"vc1": 1, "vc2": 2}
	// We go over the vcCountMap to check which VC has a count equal to
	// the len(topologySegment), in this case 2 and return that VC.
	for topologyKey, label := range topologySegments {
		if vcList, exists := topologyVCMap[label]; exists {
			for vc := range vcList {
				vcCountMap[vc] = vcCountMap[vc] + 1
			}
		} else {
			return "", logger.LogNewErrorf(log, "Topology label %q not found in topology to vCenter mapping.",
				topologyKey+":"+label)
		}
	}
	var commonVCList []string
	numTopoLabels := len(topologySegments)
	for vc, count := range vcCountMap {
		// Add VCs to the commonVCList if they satisfied all the labels in the topology segment.
		if count == numTopoLabels {
			commonVCList = append(commonVCList, vc)
		}
	}
	switch {
	case len(commonVCList) > 1:
		return "", logger.LogNewErrorf(log, "Topology segment(s) %+v belong to more than one vCenter: %+v",
			topologySegments, commonVCList)
	case len(commonVCList) == 1:
		log.Infof("Topology segment(s) %+v belong to vCenter: %q", topologySegments, commonVCList[0])
		return commonVCList[0], nil
	}
	return "", logger.LogNewErrorf(log, "failed to find the vCenter associated with topology segments %+v",
		topologySegments)
}

// RefreshPreferentialDatastoresForMultiVCenter refreshes the preferredDatastoresMap variable
// with the latest information on the preferential datastores for each topology domain across all vCenter Servers
func RefreshPreferentialDatastoresForMultiVCenter(ctx context.Context) error {
	log := logger.GetLogger(ctx)
	cnsCfg, err := config.GetConfig(ctx)
	if err != nil {
		return logger.LogNewErrorf(log, "failed to fetch CNS config. Error: %+v", err)
	}
	vcenterconfigs, err := cnsvsphere.GetVirtualCenterConfigs(ctx, cnsCfg)
	if err != nil {
		return logger.LogNewErrorf(log, "failed to get VirtualCenterConfig from CNS config. Error: %+v", err)
	}
	prefDatastoresMap := make(map[string]map[string][]string)
	for _, vcConfig := range vcenterconfigs {
		// Get VC instance.
		vcMgr := cnsvsphere.GetVirtualCenterManager(ctx)
		if err != nil {
			return logger.LogNewErrorf(log, "failed to get vCenter manager instance. Error: %+v", err)
		}
		vc, err := vcMgr.GetVirtualCenter(ctx, vcConfig.Host)
		if err != nil {
			return logger.LogNewErrorf(log, "failed to get vCenter instance for host %q. Error: %+v",
				vcConfig.Host, err)
		}
		// Get tag manager instance.
		tagMgr, err := cnsvsphere.GetTagManager(ctx, vc)
		if err != nil {
			return logger.LogNewErrorf(log, "failed to create tag manager for vCenter %q. Error: %+v",
				vcConfig.Host, err)
		}
		defer func() {
			err := tagMgr.Logout(ctx)
			if err != nil {
				log.Errorf("failed to logout tagManager for vCenter %q. Error: %v", vcConfig.Host, err)
			}
		}()
		// Get tags for category reserved for preferred datastore tagging.
		tagIds, err := tagMgr.ListTagsForCategory(ctx, PreferredDatastoresCategory)
		if err != nil {
			log.Warnf("failed to retrieve tags for category %q in vCenter %q. Reason: %+v",
				PreferredDatastoresCategory, vcConfig.Host, err)
			continue
		}
		if len(tagIds) == 0 {
			log.Infof("No preferred datastores found in vCenter %q.", vcConfig.Host)
			continue
		}
		// Fetch vSphere entities on which the tags have been applied.
		prefDatastoresMap[vcConfig.Host] = make(map[string][]string)
		attachedObjs, err := tagMgr.GetAttachedObjectsOnTags(ctx, tagIds)
		if err != nil {
			log.Warnf("failed to retrieve objects with tags %v in vCenter %q. Error: %+v",
				tagIds, vcConfig.Host, err)
			continue
		}
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
					log.Errorf("failed to retrieve summary from datastore: %+v in vCenter %q."+
						" Error: %v", obj.Reference(), vcConfig.Host, err)
					continue
				}

				log.Infof("Datastore %q with URL %q is preferred in %q domain in vCenter %q", dsMo.Summary.Name,
					dsMo.Summary.Url, attachedObj.Tag.Name, vcConfig.Host)
				// For each topology domain, store the datastore URLs preferred in that domain.
				prefDatastoresMap[vcConfig.Host][attachedObj.Tag.Name] = append(
					prefDatastoresMap[vcConfig.Host][attachedObj.Tag.Name], dsMo.Summary.Url)
			}
		}
	}
	// Finally, write to cache.
	if len(prefDatastoresMap) != 0 {
		preferredDatastoresMapInstanceLock.Lock()
		defer preferredDatastoresMapInstanceLock.Unlock()
		preferredDatastoresMap = prefDatastoresMap
		PreferredDatastoresExist = true
		log.Debugf("preferredDatastoresMap :%v", preferredDatastoresMap)
		log.Debugf("PreferredDatastoresExist: %v", PreferredDatastoresExist)
	}
	return nil
}

// GetPreferredDatastoresInSegments fetches preferred datastores in
// given topology segments as a map for faster retrieval.
func GetPreferredDatastoresInSegments(ctx context.Context, segments map[string]string,
	vcHost string) map[string]struct{} {
	log := logger.GetLogger(ctx)
	allPreferredDSURLs := make(map[string]struct{})

	preferredDatastoresMapInstanceLock.Lock()
	defer preferredDatastoresMapInstanceLock.Unlock()
	if len(preferredDatastoresMap) == 0 || len(preferredDatastoresMap[vcHost]) == 0 {
		return allPreferredDSURLs
	}
	// Arrange applicable preferred datastores as a map.
	for _, tag := range segments {
		preferredDS, ok := preferredDatastoresMap[vcHost][tag]
		if ok {
			log.Infof("Found preferred datastores %+v for topology domain %q in vCenter %q", preferredDS, tag, vcHost)
			for _, val := range preferredDS {
				allPreferredDSURLs[val] = struct{}{}
			}
		}
	}
	return allPreferredDSURLs
}

// AddLabelsToTopologyVCMap adds topology label to VC mapping for given CSINodeTopology instance
// in the topologyVCMap variable.
func AddLabelsToTopologyVCMap(ctx context.Context, nodeTopoObj csinodetopologyv1alpha1.CSINodeTopology) {
	log := logger.GetLogger(ctx)
	// Get node manager instance.
	nodeManager := node.GetManager(ctx)
	nodeVM, err := nodeManager.GetNode(ctx, nodeTopoObj.Spec.NodeUUID, nil)
	if err != nil {
		log.Errorf("Node %q is not yet registered in the node manager. Error: %+v",
			nodeTopoObj.Spec.NodeUUID, err)
		return
	}
	log.Infof("Topology labels %+v belong to %q VC", nodeTopoObj.Status.TopologyLabels,
		nodeVM.VirtualCenterHost)
	// Update topologyVCMap with topology label and associated VC host.
	topologyVCMapInstanceLock.Lock()
	defer topologyVCMapInstanceLock.Unlock()
	for _, label := range nodeTopoObj.Status.TopologyLabels {
		if _, exists := topologyVCMap[label.Value]; !exists {
			topologyVCMap[label.Value] = map[string]struct{}{nodeVM.VirtualCenterHost: {}}
		} else {
			topologyVCMap[label.Value][nodeVM.VirtualCenterHost] = struct{}{}
		}
	}
}

// RemoveLabelsFromTopologyVCMap removes the topology label to VC mapping for given CSINodeTopology
// instance in the topologyVCMap variable.
func RemoveLabelsFromTopologyVCMap(ctx context.Context, nodeTopoObj csinodetopologyv1alpha1.CSINodeTopology) {
	log := logger.GetLogger(ctx)
	// Get node manager instance.
	nodeManager := node.GetManager(ctx)
	nodeVM, err := nodeManager.GetNode(ctx, nodeTopoObj.Spec.NodeUUID, nil)
	if err != nil {
		log.Errorf("Node %q is not yet registered in the node manager. Error: %+v",
			nodeTopoObj.Spec.NodeUUID, err)
		return
	}
	log.Infof("Removing VC %q mapping for TopologyLabels %+v.", nodeVM.VirtualCenterHost,
		nodeTopoObj.Status.TopologyLabels)
	topologyVCMapInstanceLock.Lock()
	defer topologyVCMapInstanceLock.Unlock()
	for _, label := range nodeTopoObj.Status.TopologyLabels {
		delete(topologyVCMap[label.Value], nodeVM.VirtualCenterHost)
	}
}

// AddNodeToDomainNodeMapNew adds the CR instance name in the domainNodeMap wherever appropriate.
func AddNodeToDomainNodeMapNew(ctx context.Context, nodeTopoObj csinodetopologyv1alpha1.CSINodeTopology) {
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

// RemoveNodeFromDomainNodeMapNew removes the CR instance name from the domainNodeMap.
func RemoveNodeFromDomainNodeMapNew(ctx context.Context, nodeTopoObj csinodetopologyv1alpha1.CSINodeTopology) {
	log := logger.GetLogger(ctx)
	domainNodeMapInstanceLock.Lock()
	defer domainNodeMapInstanceLock.Unlock()
	for _, label := range nodeTopoObj.Status.TopologyLabels {
		delete(domainNodeMap[label.Value], nodeTopoObj.Name)
	}
	log.Infof("Removed %q value from domainNodeMap", nodeTopoObj.Name)
}

// VerifyAllNodesInTopologyAccessibleToDatastore verifies whether all the nodes present
// in a topology segment have access to the given datastore.
func VerifyAllNodesInTopologyAccessibleToDatastore(ctx context.Context, nodeNames []string,
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
