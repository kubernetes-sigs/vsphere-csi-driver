package common

import (
	"context"
	"strings"
	"sync"

	"github.com/container-storage-interface/spec/lib/go/csi"
	"github.com/vmware/govmomi/object"
	"github.com/vmware/govmomi/vim25/mo"

	cnsvsphere "sigs.k8s.io/vsphere-csi-driver/v3/pkg/common/cns-lib/vsphere"
	"sigs.k8s.io/vsphere-csi-driver/v3/pkg/common/config"
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
	// tagVCEntityMoRefMap maintains a cache of topology tags to the vCenter IP/FQDN & the MoRef of the
	// entity which holds the tag.
	// Example - {
	//    "region-1": {"vc1": [{Type:Datacenter Value:datacenter-3}], "vc2": [{Type:Datacenter Value:datacenter-5}] },
	//	  "zone-1": {"vc1": [{Type:ClusterComputeResource Value:domain-c12}] },
	//	  "zone-2": {"vc2": [{Type:ClusterComputeResource Value:domain-c8] },}
	tagVCEntityMoRefMap = make(map[string]map[string][]mo.Reference)
)

// DiscoverTagEntities populates tagVCEntityMoRefMap with tagName -> VC -> associated MoRefs mapping.
// NOTE: Any edits to existing topology labels will require a restart of the controller.
func DiscoverTagEntities(ctx context.Context) error {
	log := logger.GetLogger(ctx)
	// Get CNS config.
	cnsCfg, err := config.GetConfig(ctx)
	if err != nil {
		return logger.LogNewErrorf(log, "failed to fetch CNS config. Error: %+v", err)
	}

	var categories []string
	zoneCat := strings.TrimSpace(cnsCfg.Labels.Zone)
	regionCat := strings.TrimSpace(cnsCfg.Labels.Region)
	if zoneCat != "" && regionCat != "" {
		categories = []string{zoneCat, regionCat}
	} else if strings.TrimSpace(cnsCfg.Labels.TopologyCategories) != "" {
		categories = strings.Split(cnsCfg.Labels.TopologyCategories, ",")
		for index := range categories {
			categories[index] = strings.TrimSpace(categories[index])
		}
	} else {
		log.Infof("DiscoverTagEntities: No topology information found in CNS config.")
		return nil
	}
	log.Infof("Topology categories being considered for tag to VC mapping are %+v", categories)

	vcenterConfigs, err := cnsvsphere.GetVirtualCenterConfigs(ctx, cnsCfg)
	if err != nil {
		return logger.LogNewErrorf(log, "failed to get VirtualCenterConfigs. Error: %v", err)
	}
	for _, vcenterCfg := range vcenterConfigs {
		// Get VC instance
		vcenter, err := cnsvsphere.GetVirtualCenterInstanceForVCenterConfig(ctx, vcenterCfg, false)
		if err != nil {
			return logger.LogNewErrorf(log, "failed to get vCenterInstance for vCenter Host: %q. Error: %v",
				vcenterCfg.Host, err)
		}
		// Get tag manager instance.
		tagManager, err := cnsvsphere.GetTagManager(ctx, vcenter)
		if err != nil {
			return logger.LogNewErrorf(log, "failed to create tagManager. Error: %v", err)
		}
		defer func() {
			err := tagManager.Logout(ctx)
			if err != nil {
				log.Errorf("failed to logout tagManager. Error: %v", err)
			}
		}()
		for _, cat := range categories {
			topoTags, err := tagManager.GetTagsForCategory(ctx, cat)
			if err != nil {
				return logger.LogNewErrorf(log, "failed to fetch tags for category %q", cat)
			}
			log.Infof("Tags associated with category %q are %+v", cat, topoTags)
			for _, tag := range topoTags {
				objMORs, err := tagManager.ListAttachedObjects(ctx, tag.ID)
				if err != nil {
					return logger.LogNewErrorf(log, "failed to fetch objects associated with tag %q", tag.Name)
				}
				log.Infof("Entities associated with tag %q are %+v", tag.Name, objMORs)
				if len(objMORs) == 0 {
					continue
				}
				if _, exists := tagVCEntityMoRefMap[tag.Name]; !exists {
					tagVCEntityMoRefMap[tag.Name] = map[string][]mo.Reference{vcenterCfg.Host: objMORs}
				} else {
					tagVCEntityMoRefMap[tag.Name][vcenterCfg.Host] = objMORs
				}
			}
		}
	}
	log.Debugf("tagVCEntityMoRefMap: %+v", tagVCEntityMoRefMap)
	return nil
}

// GetHostsForSegment retrieves the list of hosts for a topology segment by first
// finding the entities associated with the tag lower in hierarchy.
func GetHostsForSegment(ctx context.Context, topoSegment map[string]string, vCenter *cnsvsphere.VirtualCenter) (
	[]*cnsvsphere.HostSystem, error) {
	log := logger.GetLogger(ctx)
	var (
		allhostSlices [][]*cnsvsphere.HostSystem
	)

	// Get the entity MoRefs for each tag.
	for key, tag := range topoSegment {
		var hostList []*cnsvsphere.HostSystem
		entityMorefs, exists := areEntityMorefsPresentForTag(tag, vCenter.Config.Host)
		if !exists {
			// Refresh cache to see if the tag has been added recently.
			log.Infof("Refresh cache to see if the tag has been added recently")
			err := DiscoverTagEntities(ctx)
			if err != nil {
				return nil, logger.LogNewErrorf(log,
					"failed to update cache with topology information. Error: %+v", err)
			}
			entityMorefs, exists = areEntityMorefsPresentForTag(tag, vCenter.Config.Host)
			if !exists {
				return nil, logger.LogNewErrorf(log, "failed to find tag %q in VC %q.", tag, vCenter.Config.Host)
			}
		}
		log.Infof("Tag %q is applied on entities %+v", tag, entityMorefs)
		log.Debugf("Fetching hosts for entities %+v", entityMorefs)
		for _, entity := range entityMorefs {
			hosts, err := fetchHosts(ctx, entity, vCenter)
			if err != nil {
				return nil, logger.LogNewErrorf(log, "failed to fetch hosts from entity %+v. Error: %+v",
					entity.Reference(), err)
			}
			hostList = append(hostList, hosts...)
		}
		log.Infof("Hosts returned for topology category: %q and tag: %q are %v", key, tag, hostList)
		uniqueHostList := removeDuplicateHosts(hostList)
		allhostSlices = append(allhostSlices, uniqueHostList)
	}
	commonHosts := findCommonHostsforAllTopologyKeys(ctx, allhostSlices)
	log.Infof("common hosts: %v for all segments: %v", commonHosts, topoSegment)
	return commonHosts, nil
}

// removeDuplicateHosts removes duplicate entries found in the given host list.
// This could happen for example if both a cluster and one of its hosts are
// tagged at the same time. The list would then have the same host entry twice,
// which breaks assumptions in findCommonHostsforAllTopologyKeys. By removing
// duplicate entries first, this configuration can still be allowed to work.
func removeDuplicateHosts(hostList []*cnsvsphere.HostSystem) []*cnsvsphere.HostSystem {
	hostMap := make(map[string]bool)
	var uniqueHosts []*cnsvsphere.HostSystem
	for _, host := range hostList {
		if _, exists := hostMap[host.String()]; !exists {
			hostMap[host.String()] = true
			uniqueHosts = append(uniqueHosts, host)
		}
	}
	return uniqueHosts
}

// findCommonHostsforAllTopologyKeys helps find common hosts across all slices in hostLists
func findCommonHostsforAllTopologyKeys(ctx context.Context,
	hostLists [][]*cnsvsphere.HostSystem) []*cnsvsphere.HostSystem {
	log := logger.GetLogger(ctx)
	log.Infof("finding common hosts for hostlists: %v", hostLists)
	if len(hostLists) == 0 {
		return []*cnsvsphere.HostSystem{}
	}
	// Create a map to store hosts and their occurrence count
	hostCount := make(map[string]int)
	// Count occurrences of elements in the first slice
	for _, host := range hostLists[0] {
		hostCount[host.String()]++
	}
	log.Debugf("hostCount after setting count in the first slice : %v", hostCount)
	// Iterate through the remaining slices and update the hostCount map
	for i := 1; i < len(hostLists); i++ {
		for _, host := range hostLists[i] {
			// If the host exists in the map, increment its count
			if count, exists := hostCount[host.String()]; exists {
				hostCount[host.String()] = count + 1
			}
		}
	}
	log.Debugf("hostCount after iterate through the remaining slices and updated hostCount map : %v", hostCount)
	// Create a slice to store the intersection
	var commonHosts []string

	// Check if each hosts occurred in all slices
	for hostMoRefName, count := range hostCount {
		if count == len(hostLists) {
			commonHosts = append(commonHosts, hostMoRefName)
		}
	}
	log.Debugf("common hosts: %v", commonHosts)

	// Iterate through the all slices and get common hosts
	var commonHostSystem []*cnsvsphere.HostSystem
	for _, host := range commonHosts {
	out:
		for i := 0; i < len(hostLists); i++ {
			for _, hostSystem := range hostLists[i] {
				if hostSystem.String() == host {
					commonHostSystem = append(commonHostSystem, hostSystem)
					break out
				}
			}
		}
	}
	log.Debugf("commonHostSystem: %v", commonHostSystem)
	return commonHostSystem
}

// fetchHosts gives a list of hosts under the entity given as input.
func fetchHosts(ctx context.Context, entity mo.Reference, vCenter *cnsvsphere.VirtualCenter) (
	[]*cnsvsphere.HostSystem, error) {
	log := logger.GetLogger(ctx)
	var hosts []*cnsvsphere.HostSystem
	log.Infof("fetching hosts for entity: %v on vCenter: %q", entity, vCenter.Config.Host)
	switch entity.Reference().Type {
	case "rootFolder":
		folder := object.NewFolder(vCenter.Client.Client, entity.Reference())
		children, err := folder.Children(ctx)
		if err != nil {
			return nil, logger.LogNewErrorf(log,
				"failed to retrieve child entities of the rootFolder %+v. Error: %+v", entity.Reference(), err)
		}
		for _, child := range children {
			hostList, err := fetchHosts(ctx, child.Reference(), vCenter)
			if err != nil {
				return nil, logger.LogNewErrorf(log, "failed to fetch hosts from entity %+v. Error: %+v",
					child.Reference(), err)
			}
			hosts = append(hosts, hostList...)
		}
	case "Datacenter":
		dc := cnsvsphere.Datacenter{
			Datacenter:        object.NewDatacenter(vCenter.Client.Client, entity.Reference()),
			VirtualCenterHost: vCenter.Config.Host}
		var dcMo mo.Datacenter
		err := dc.Properties(ctx, dc.Reference(), []string{"hostFolder"}, &dcMo)
		if err != nil {
			return nil, logger.LogNewErrorf(log, ""+
				"failed to retrieve hostFolder property for datacenter %+v", dc.Reference())
		}
		hostList, err := fetchHosts(ctx, dcMo.HostFolder, vCenter)
		if err != nil {
			return nil, logger.LogNewErrorf(log, "failed to fetch hosts from entity %+v. Error: %+v",
				dcMo, err)
		}
		hosts = append(hosts, hostList...)
	case "Folder":
		folder := object.NewFolder(vCenter.Client.Client, entity.Reference())
		children, err := folder.Children(ctx)
		if err != nil {
			return nil, logger.LogNewErrorf(log, "failed to fetch child entities of Folder %+v. Error: %+v",
				entity.Reference(), err)
		}
		for _, child := range children {
			hostList, err := fetchHosts(ctx, child.Reference(), vCenter)
			if err != nil {
				return nil, logger.LogNewErrorf(log, "failed to fetch hosts from entity %+v. Error: %+v",
					child.Reference(), err)
			}
			hosts = append(hosts, hostList...)
		}
	case "ClusterComputeResource":
		ccr := cnsvsphere.ClusterComputeResource{
			ClusterComputeResource: object.NewClusterComputeResource(vCenter.Client.Client, entity.Reference()),
			VirtualCenterHost:      vCenter.Config.Host}
		hostList, err := ccr.GetHosts(ctx)
		if err != nil {
			return nil, logger.LogNewErrorf(log, "failed to retrieve hosts from cluster %+v. Error: %+v",
				entity.Reference(), err)
		}
		hosts = append(hosts, hostList...)
	case "ComputeResource":
		cr := object.NewComputeResource(vCenter.Client.Client, entity.Reference())
		hostList, err := cr.Hosts(ctx)
		if err != nil {
			return nil, logger.LogNewErrorf(log,
				"failed to retrieve host from stand alone host (Compute Resource) %+v. Error: %+v",
				entity.Reference(), err)
		}
		for _, host := range hostList {
			hosts = append(hosts,
				&cnsvsphere.HostSystem{HostSystem: object.NewHostSystem(vCenter.Client.Client, host.Reference())})
		}
	case "HostSystem":
		host := cnsvsphere.HostSystem{HostSystem: object.NewHostSystem(vCenter.Client.Client, entity.Reference())}
		hosts = append(hosts, &host)
	default:
		return nil, logger.LogNewErrorf(log, "unrecognised entity type found %+v.", entity.Reference())
	}

	return hosts, nil
}

// areEntityMorefsPresentForTag retrieves the entities in given VC which have the
// input tag associated with them.
func areEntityMorefsPresentForTag(tag, vcHost string) ([]mo.Reference, bool) {
	vcEntityMap, exists := tagVCEntityMoRefMap[tag]
	if !exists {
		return nil, false
	}
	entityMorefs, exists := vcEntityMap[vcHost]
	if !exists {
		return nil, false
	}
	return entityMorefs, true
}

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

// getVCForTopologySegments uses the tagVCEntityMoRefMap to retrieve the
// VC instance for the given topology segments map in a multi-VC environment.
func getVCForTopologySegments(ctx context.Context, topologySegments map[string]string) (string, error) {
	log := logger.GetLogger(ctx)
	// vcCountMap keeps a cumulative count of the occurrences of
	// VCs across all labels in the given topology segment.
	vcCountMap := make(map[string]int)

	// Find the VC which contains all the labels given in the topologySegments.
	// For example, if tagVCEntityMoRefMap looks like
	// {"region-1": {"vc1": [{Type:Datacenter Value:datacenter-3}], "vc2": [{Type:Datacenter Value:datacenter-5}] },
	// "zone-1": {"vc1": [{Type:ClusterComputeResource Value:domain-c12}] },
	// "zone-2": {"vc2": [{Type:ClusterComputeResource Value:domain-c8] },}
	// For a given topologySegment
	// {"topology.csi.vmware.com/k8s-region": "region-1",
	// "topology.csi.vmware.com/k8s-zone": "zone-2"}
	// we will end up with a vcCountMap as follows: {"vc1": 1, "vc2": 2}
	// We go over the vcCountMap to check which VC has a count equal to
	// the len(topologySegment), in this case 2 and return that VC.
	for topologyKey, label := range topologySegments {
		vcMap, exists := tagVCEntityMoRefMap[label]
		if !exists {
			// Refresh cache to see if the tag has been added recently.
			err := DiscoverTagEntities(ctx)
			if err != nil {
				return "", logger.LogNewErrorf(log,
					"failed to update cache with tag to VC to MoRef mapping. Error: %+v", err)
			}
			vcMap, exists = tagVCEntityMoRefMap[label]
		}
		if exists {
			for vc := range vcMap {
				vcCountMap[vc] = vcCountMap[vc] + 1
			}
		} else {
			return "", logger.LogNewErrorf(log, "Topology label %q not found in tag to vCenter mapping.",
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
		log.Debugf("PreferredDatastoresExist: %t", PreferredDatastoresExist)
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
