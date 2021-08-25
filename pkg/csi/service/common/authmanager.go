/*
Copyright 2020 The Kubernetes Authors.

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

package common

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/vmware/govmomi/find"
	"github.com/vmware/govmomi/object"
	"github.com/vmware/govmomi/property"
	"github.com/vmware/govmomi/vim25/mo"
	vim25types "github.com/vmware/govmomi/vim25/types"
	"google.golang.org/grpc/codes"

	cnsvsphere "sigs.k8s.io/vsphere-csi-driver/v2/pkg/common/cns-lib/vsphere"
	"sigs.k8s.io/vsphere-csi-driver/v2/pkg/csi/service/logger"
)

// AuthorizationService exposes interfaces to support authorization check on
// datastores and get datastores which will be used by create volume.
// It is provided for backward compatibility. For latest vSphere release,
// this service is redundant.
type AuthorizationService interface {
	// GetDatastoreMapForBlockVolumes returns a map of datastore URL to datastore
	// info for only those datastores the CSI VC user has Datastore.FileManagement
	// privilege for.
	GetDatastoreMapForBlockVolumes(ctx context.Context) map[string]*cnsvsphere.DatastoreInfo

	// GetFsEnabledClusterToDsMap returns a map of cluster ID to datastores info
	// objects for vSAN clusters with file services enabled. The datastores are
	// those on which the CSI VC user has Host.Config.Storage privilege.
	GetFsEnabledClusterToDsMap(ctx context.Context) map[string][]*cnsvsphere.DatastoreInfo

	// ResetvCenterInstance sets new vCenter instance for AuthorizationService
	ResetvCenterInstance(ctx context.Context, vCenter *cnsvsphere.VirtualCenter)
}

// AuthManager maintains an internal map to track the datastores that need to be
// used by create volume.
type AuthManager struct {
	// Map the datastore url to datastore info which need to be used when
	// invoking CNS CreateVolume API.
	datastoreMapForBlockVolumes map[string]*cnsvsphere.DatastoreInfo
	// Map the vSAN file services enabled cluster ID to list of datastore info
	// for that cluster. The datastore info objects are to be used when invoking
	// CNS CreateVolume API.
	fsEnabledClusterToDsMap map[string][]*cnsvsphere.DatastoreInfo
	// Make sure the update for datastoreMap is mutually exclusive.
	rwMutex sync.RWMutex
	// VCenter Instance.
	vcenter *cnsvsphere.VirtualCenter
}

// onceForAuthorizationService is used for initializing the AuthorizationService
// singleton.
var onceForAuthorizationService sync.Once

// authManagerIntance is instance of authManager and implements interface for
// AuthorizationService.
var authManagerInstance *AuthManager

// GetAuthorizationService returns the singleton AuthorizationService.
func GetAuthorizationService(ctx context.Context, vc *cnsvsphere.VirtualCenter) (AuthorizationService, error) {
	log := logger.GetLogger(ctx)
	onceForAuthorizationService.Do(func() {
		log.Info("Initializing authorization service...")

		authManagerInstance = &AuthManager{
			datastoreMapForBlockVolumes: make(map[string]*cnsvsphere.DatastoreInfo),
			fsEnabledClusterToDsMap:     make(map[string][]*cnsvsphere.DatastoreInfo),
			rwMutex:                     sync.RWMutex{},
			vcenter:                     vc,
		}
	})

	log.Info("authorization service initialized")
	return authManagerInstance, nil
}

// GetDatastoreMapForBlockVolumes returns a DatastoreMapForBlockVolumes. This
// map maps datastore url to datastore info which need to be used when creating
// block volume.
func (authManager *AuthManager) GetDatastoreMapForBlockVolumes(
	ctx context.Context) map[string]*cnsvsphere.DatastoreInfo {
	datastoreMapForBlockVolumes := make(map[string]*cnsvsphere.DatastoreInfo)
	authManager.rwMutex.RLock()
	defer authManager.rwMutex.RUnlock()
	for dsURL, dsInfo := range authManager.datastoreMapForBlockVolumes {
		datastoreMapForBlockVolumes[dsURL] = dsInfo
	}
	return datastoreMapForBlockVolumes
}

// GetFsEnabledClusterToDsMap returns a map of cluster ID with vSAN file
// services enabled to its datastore info objects.
func (authManager *AuthManager) GetFsEnabledClusterToDsMap(
	ctx context.Context) map[string][]*cnsvsphere.DatastoreInfo {
	fsEnabledClusterToDsMap := make(map[string][]*cnsvsphere.DatastoreInfo)
	authManager.rwMutex.RLock()
	defer authManager.rwMutex.RUnlock()
	for clusterID, datastores := range authManager.fsEnabledClusterToDsMap {
		fsEnabledClusterToDsMap[clusterID] = datastores
	}
	return fsEnabledClusterToDsMap
}

// ResetvCenterInstance sets new vCenter instance for AuthorizationService.
func (authManager *AuthManager) ResetvCenterInstance(ctx context.Context, vCenter *cnsvsphere.VirtualCenter) {
	log := logger.GetLogger(ctx)
	log.Info("Resetting vCenter Instance in the AuthManager")
	authManager.vcenter = vCenter
}

// refreshDatastoreMapForBlockVolumes scans all datastores in vCenter to check
// privileges, and compute the datastoreMapForBlockVolumes.
func (authManager *AuthManager) refreshDatastoreMapForBlockVolumes() {
	ctx, log := logger.GetNewContextWithLogger()
	log.Debug("auth manager: refreshDatastoreMapForBlockVolumes is triggered")
	newDatastoreMapForBlockVolumes, err := GenerateDatastoreMapForBlockVolumes(ctx, authManager.vcenter)
	if err == nil {
		authManager.rwMutex.Lock()
		defer authManager.rwMutex.Unlock()
		authManager.datastoreMapForBlockVolumes = newDatastoreMapForBlockVolumes
		log.Debugf("auth manager: datastoreMapForBlockVolumes is updated to %v", newDatastoreMapForBlockVolumes)
	} else {
		log.Warnf("auth manager: failed to get updated datastoreMapForBlockVolumes, Err: %v", err)
	}
}

// refreshFSEnabledClustersToDsMap scans all clusters with vSAN FS enabled in
// vCenter to check privileges, and compute the fsEnabledClusterToDsMap.
func (authManager *AuthManager) refreshFSEnabledClustersToDsMap() {
	ctx, log := logger.GetNewContextWithLogger()
	log.Debug("auth manager: refreshDatastoreMapsForFileVolumes is triggered")
	newFsEnabledClusterToDsMap, err := GenerateFSEnabledClustersToDsMap(ctx, authManager.vcenter)
	if err == nil {
		authManager.rwMutex.Lock()
		defer authManager.rwMutex.Unlock()

		authManager.fsEnabledClusterToDsMap = newFsEnabledClusterToDsMap
		log.Debugf("auth manager: newFsEnabledClusterToDsMap is updated to %v", newFsEnabledClusterToDsMap)
	} else {
		log.Warnf("auth manager: failed to get updated datastoreMapForFileVolumes, Err: %v", err)
	}
}

// ComputeDatastoreMapForBlockVolumes refreshes DatastoreMapForBlockVolumes
// periodically.
func ComputeDatastoreMapForBlockVolumes(authManager *AuthManager, authCheckInterval int) {
	log := logger.GetLoggerWithNoContext()
	log.Info("auth manager: ComputeDatastoreMapForBlockVolumes enter")
	ticker := time.NewTicker(time.Duration(authCheckInterval) * time.Minute)
	for ; true; <-ticker.C {
		authManager.refreshDatastoreMapForBlockVolumes()
	}
}

// ComputeFSEnabledClustersToDsMap refreshes fsEnabledClusterToDsMap
// periodically.
func ComputeFSEnabledClustersToDsMap(authManager *AuthManager, authCheckInterval int) {
	log := logger.GetLoggerWithNoContext()
	log.Info("auth manager: ComputeFSEnabledClustersToDsMap enter")
	ticker := time.NewTicker(time.Duration(authCheckInterval) * time.Minute)
	for ; true; <-ticker.C {
		authManager.refreshFSEnabledClustersToDsMap()
	}
}

// GenerateDatastoreMapForBlockVolumes scans all datastores in Vcenter and do
// privilege check on those datastoes. It will return datastores which has the
// privileges for creating block volume.
func GenerateDatastoreMapForBlockVolumes(ctx context.Context,
	vc *cnsvsphere.VirtualCenter) (map[string]*cnsvsphere.DatastoreInfo, error) {
	log := logger.GetLogger(ctx)
	// Get all datastores from VC.
	dcList, err := vc.GetDatacenters(ctx)
	if err != nil {
		return nil, logger.LogNewErrorCodef(log, codes.Internal,
			"failed to get datacenter list. err: %+v", err)
	}
	var dsURLTodsInfoMap map[string]*cnsvsphere.DatastoreInfo
	var dsURLs []string
	var dsInfos []*cnsvsphere.DatastoreInfo
	var entities []vim25types.ManagedObjectReference
	for _, dc := range dcList {
		dsURLTodsInfoMap, err = dc.GetAllDatastores(ctx)
		if err != nil {
			msg := fmt.Sprintf("failed to get dsURLTodsInfoMap. err: %+v", err)
			log.Error(msg)
		}
		for dsURL, dsInfo := range dsURLTodsInfoMap {
			dsURLs = append(dsURLs, dsURL)
			dsInfos = append(dsInfos, dsInfo)
			dsMoRef := dsInfo.Reference()
			entities = append(entities, dsMoRef)
		}
	}

	dsURLToInfoMap, err := getDatastoresWithBlockVolumePrivs(ctx, vc, dsURLs, dsInfos, entities)
	if err != nil {
		log.Errorf("failed to get datastores with required priv. Error: %+v", err)
		return nil, err
	}
	return dsURLToInfoMap, nil
}

// GenerateFSEnabledClustersToDsMap scans all clusters in VC and do privilege
// check on them. It will return a map of cluster id to the list of datastore
// objects. The key is cluster moid with vSAN FS enabled and Host.Config.Storage
// privilege. The value is a list of vSAN datastoreInfo objects for the cluster.
func GenerateFSEnabledClustersToDsMap(ctx context.Context,
	vc *cnsvsphere.VirtualCenter) (map[string][]*cnsvsphere.DatastoreInfo, error) {
	log := logger.GetLogger(ctx)
	clusterToDsInfoListMap := make(map[string][]*cnsvsphere.DatastoreInfo)

	datacenters, err := vc.ListDatacenters(ctx)
	if err != nil {
		log.Errorf("failed to find datacenters from VC: %q, Error: %+v", vc.Config.Host, err)
		return nil, err
	}
	// Get all vSAN datastores from VC.
	vsanDsURLToInfoMap, err := vc.GetVsanDatastores(ctx, datacenters)
	if err != nil {
		log.Errorf("failed to get vSAN datastores with error %+v", err)
		return nil, err
	}
	// Return empty map if no vSAN datastores are found.
	if len(vsanDsURLToInfoMap) == 0 {
		log.Debug("No vSAN datastores found")
		return clusterToDsInfoListMap, nil
	}

	// Initialize vsan client.
	err = vc.ConnectVsan(ctx)
	if err != nil {
		log.Errorf("error occurred while connecting to VSAN, err: %+v", err)
		return nil, err
	}

	fsEnabledClusterToDsURLsMap, err := getFSEnabledClusterToDsURLsMap(ctx, vc, datacenters)
	if err != nil {
		log.Errorf("failed to get file service enabled clusters map with error %+v", err)
		return nil, err
	}

	// Create a map of cluster to dsInfo objects. These objects are used while
	// calling CNS API to create file volumes.
	for clusterMoID, dsURLs := range fsEnabledClusterToDsURLsMap {
		for _, dsURL := range dsURLs {
			if dsInfo, ok := vsanDsURLToInfoMap[dsURL]; ok {
				log.Debugf("Adding vSAN datastore %q to the list for FS enabled cluster %q", dsURL, clusterMoID)
				clusterToDsInfoListMap[clusterMoID] = append(clusterToDsInfoListMap[clusterMoID], dsInfo)
			}
		}
	}

	log.Debugf("clusterToDsInfoListMap is %+v", clusterToDsInfoListMap)
	return clusterToDsInfoListMap, nil
}

// IsFileServiceEnabled checks if file service is enabled on the specified
// datastoreUrls.
func IsFileServiceEnabled(ctx context.Context, datastoreUrls []string,
	vc *cnsvsphere.VirtualCenter, datacenters []*cnsvsphere.Datacenter) (map[string]bool, error) {
	// Compute this map during controller init. Re use the map every other time.
	log := logger.GetLogger(ctx)
	err := vc.Connect(ctx)
	if err != nil {
		log.Errorf("failed to connect to VirtualCenter. err=%v", err)
		return nil, err
	}
	err = vc.ConnectVsan(ctx)
	if err != nil {
		log.Errorf("error occurred while connecting to VSAN, err: %+v", err)
		return nil, err
	}
	// Gets the datastore to file service enabled map for all vsan datastores
	// belonging to clusters with vSAN FS enabled and Host.Config.Storage
	// privileges.
	dsToFileServiceEnabledMap, err := getDsToFileServiceEnabledMap(ctx, vc, datacenters)
	if err != nil {
		log.Errorf("failed to query if file service is enabled on vsan datastores or not. error: %+v", err)
		return nil, err
	}
	log.Debugf("dsToFileServiceEnabledMap is %+v", dsToFileServiceEnabledMap)
	// Now create a map of datastores which are queried in the method.
	dsToFSEnabledMapToReturn := make(map[string]bool)
	for _, datastoreURL := range datastoreUrls {
		if val, ok := dsToFileServiceEnabledMap[datastoreURL]; ok {
			if !val {
				msg := fmt.Sprintf("File service is not enabled on the datastore: %s", datastoreURL)
				log.Debugf(msg)
				dsToFSEnabledMapToReturn[datastoreURL] = false
			} else {
				msg := fmt.Sprintf("File service is enabled on the datastore: %s", datastoreURL)
				log.Debugf(msg)
				dsToFSEnabledMapToReturn[datastoreURL] = true
			}
		} else {
			msg := fmt.Sprintf("File service is not enabled on the datastore: %s", datastoreURL)
			log.Debugf(msg)
			dsToFSEnabledMapToReturn[datastoreURL] = false
		}
	}
	return dsToFSEnabledMapToReturn, nil
}

// getDatastoresWithBlockVolumePrivs gets datastores with required priv for CSI
// user.
func getDatastoresWithBlockVolumePrivs(ctx context.Context, vc *cnsvsphere.VirtualCenter,
	dsURLs []string, dsInfos []*cnsvsphere.DatastoreInfo,
	entities []vim25types.ManagedObjectReference) (map[string]*cnsvsphere.DatastoreInfo, error) {
	log := logger.GetLogger(ctx)
	log.Debugf("auth manager: file - dsURLs %v dsInfos %v", dsURLs, dsInfos)
	// dsURLToInfoMap will store a list of vSAN datastores with vSAN FS enabled
	// for which CSI user has privilege to.
	dsURLToInfoMap := make(map[string]*cnsvsphere.DatastoreInfo)
	// Get authMgr.
	authMgr := object.NewAuthorizationManager(vc.Client.Client)
	privIds := []string{DsPriv, SysReadPriv}

	userName := vc.Config.Username
	// Invoke authMgr function HasUserPrivilegeOnEntities.
	result, err := authMgr.HasUserPrivilegeOnEntities(ctx, entities, userName, privIds)
	if err != nil {
		log.Errorf("auth manager: failed to check privilege %v on entities %v for user %s", privIds, entities, userName)
		return nil, err
	}
	log.Debugf(
		"auth manager: HasUserPrivilegeOnEntities returns %v, when checking privileges %v on entities %v for user %s",
		result, privIds, entities, userName)
	for index, entityPriv := range result {
		hasPriv := true
		privAvails := entityPriv.PrivAvailability
		for _, privAvail := range privAvails {
			if !privAvail.IsGranted {
				// Required privilege is not grant for this entity.
				hasPriv = false
				break
			}
		}
		if hasPriv {
			dsURLToInfoMap[dsURLs[index]] = dsInfos[index]
			log.Debugf("auth manager: datastore with URL %s and name %s has privileges and is added to dsURLToInfoMap",
				dsInfos[index].Info.Name, dsURLs[index])
		}
	}
	return dsURLToInfoMap, nil
}

// Creates a map of vsan datastores to file service enabled status.
// Since only datastores belonging to clusters with vSAN FS enabled and
// Host.Config.Storage privileges are returned, file service enabled status
// will be true for them.
func getDsToFileServiceEnabledMap(ctx context.Context, vc *cnsvsphere.VirtualCenter,
	datacenters []*cnsvsphere.Datacenter) (map[string]bool, error) {
	log := logger.GetLogger(ctx)
	log.Debugf("Computing the cluster to file service status (enabled/disabled) map.")

	// Get clusters with vSAN FS enabled and privileges.
	vSANFSClustersWithPriv, err := getFSEnabledClustersWithPriv(ctx, vc, datacenters)
	if err != nil {
		log.Errorf("failed to get the file service enabled clusters with privileges. error: %+v", err)
		return nil, err
	}

	dsToFileServiceEnabledMap := make(map[string]bool)
	for _, cluster := range vSANFSClustersWithPriv {
		dsMoList, err := getDatastoreMOsFromCluster(ctx, vc, cluster)
		if err != nil {
			log.Errorf("failed to get datastores for cluster %q. error: %+v", cluster.Reference().Value, err)
			return nil, err
		}
		// TODO: Also identify which vSAN datastore is management and which one
		// is a workload datastore to support file volumes on VMC.
		for _, dsMo := range dsMoList {
			if dsMo.Summary.Type == VsanDatastoreType {
				dsToFileServiceEnabledMap[dsMo.Info.GetDatastoreInfo().Url] = true
			}
		}
	}
	return dsToFileServiceEnabledMap, nil
}

// Creates a map of cluster id to datastore urls. The key is cluster moid with
// vSAN FS enabled and Host.Config.Storage privilege. The value is a list of
// vSAN datastore URLs for each cluster.
func getFSEnabledClusterToDsURLsMap(ctx context.Context, vc *cnsvsphere.VirtualCenter,
	datacenters []*cnsvsphere.Datacenter) (map[string][]string, error) {
	log := logger.GetLogger(ctx)
	log.Debugf("Computing the map for vSAN FS enabled clusters to datastore URLS.")

	// Get clusters with vSAN FS enabled and privileges.
	vSANFSClustersWithPriv, err := getFSEnabledClustersWithPriv(ctx, vc, datacenters)
	if err != nil {
		log.Errorf("failed to get the file service enabled clusters with privileges. error: %+v", err)
		return nil, err
	}

	fsEnabledClusterToDsMap := make(map[string][]string)
	for _, cluster := range vSANFSClustersWithPriv {
		clusterMoID := cluster.Reference().Value
		dsMoList, err := getDatastoreMOsFromCluster(ctx, vc, cluster)
		if err != nil {
			log.Errorf("failed to get datastores for cluster %q. error: %+v", clusterMoID, err)
			return nil, err
		}
		for _, dsMo := range dsMoList {
			if dsMo.Summary.Type == VsanDatastoreType {
				fsEnabledClusterToDsMap[clusterMoID] =
					append(fsEnabledClusterToDsMap[clusterMoID], dsMo.Info.GetDatastoreInfo().Url)
			}
		}
	}

	return fsEnabledClusterToDsMap, nil
}

// Returns a list of clusters with Host.Config.Storage privilege and vSAN file
// services enabled.
func getFSEnabledClustersWithPriv(ctx context.Context, vc *cnsvsphere.VirtualCenter,
	datacenters []*cnsvsphere.Datacenter) ([]*object.ClusterComputeResource, error) {
	log := logger.GetLogger(ctx)
	log.Debugf("Computing the clusters with vSAN file services enabled and Host.Config.Storage privileges")
	// Get clusters from datacenters.
	clusterComputeResources := []*object.ClusterComputeResource{}
	for _, datacenter := range datacenters {
		finder := find.NewFinder(datacenter.Datacenter.Client(), false)
		finder.SetDatacenter(datacenter.Datacenter)
		clusterComputeResource, err := finder.ClusterComputeResourceList(ctx, "*")
		if err != nil {
			if _, ok := err.(*find.NotFoundError); ok {
				log.Debugf("No clusterComputeResource found in dc: %+v. error: %+v", datacenter, err)
				continue
			}
			log.Errorf("Error occurred while getting clusterComputeResource. error: %+v", err)
			return nil, err
		}
		clusterComputeResources = append(clusterComputeResources, clusterComputeResource...)
	}

	// Get Clusters with HostConfigStoragePriv.
	authMgr := object.NewAuthorizationManager(vc.Client.Client)
	privIds := []string{HostConfigStoragePriv}
	userName := vc.Config.Username
	var entities []vim25types.ManagedObjectReference
	clusterComputeResourcesMap := make(map[string]*object.ClusterComputeResource)
	for _, cluster := range clusterComputeResources {
		entities = append(entities, cluster.Reference())
		clusterComputeResourcesMap[cluster.Reference().Value] = cluster
	}

	// Invoke authMgr function HasUserPrivilegeOnEntities.
	result, err := authMgr.HasUserPrivilegeOnEntities(ctx, entities, userName, privIds)
	if err != nil {
		log.Errorf("auth manager: failed to check privilege %v on entities %v for user %s", privIds, entities, userName)
		return nil, err
	}
	log.Debugf(
		"auth manager: HasUserPrivilegeOnEntities returns %v when checking privileges %v on entities %v for user %s",
		result, privIds, entities, userName)
	clusterComputeResourceWithPriv := []*object.ClusterComputeResource{}
	for _, entityPriv := range result {
		hasPriv := true
		privAvails := entityPriv.PrivAvailability
		for _, privAvail := range privAvails {
			// Required privilege is not grant for this entity.
			if !privAvail.IsGranted {
				hasPriv = false
				break
			}
		}
		if hasPriv {
			clusterComputeResourceWithPriv = append(clusterComputeResourceWithPriv,
				clusterComputeResourcesMap[entityPriv.Entity.Value])
		}
	}
	log.Debugf("Clusters with priv: %s are : %+v", HostConfigStoragePriv, clusterComputeResourceWithPriv)

	// Get clusters which are vSAN and have vSAN FS enabled.
	clusterComputeResourceWithPrivAndFS := []*object.ClusterComputeResource{}
	// Add all the vsan datastores with vsan FS (from these clusters) to map.
	for _, cluster := range clusterComputeResourceWithPriv {
		// Get the cluster config to know if file service is enabled on it or not.
		config, err := vc.VsanClient.VsanClusterGetConfig(ctx, cluster.Reference())
		if err != nil {
			log.Errorf("failed to get the vsan cluster config. error: %+v", err)
			return nil, err
		}
		if !(*config.Enabled) {
			log.Debugf("cluster: %+v is a non-vSAN cluster. Skipping this cluster", cluster)
			continue
		} else if config.FileServiceConfig == nil {
			log.Debugf("VsanClusterGetConfig.FileServiceConfig is empty. Skipping this cluster: %+v with config: %+v",
				cluster, config)
			continue
		}

		log.Debugf("cluster: %+v has vSAN file services enabled: %t", cluster, config.FileServiceConfig.Enabled)
		if config.FileServiceConfig.Enabled {
			clusterComputeResourceWithPrivAndFS = append(clusterComputeResourceWithPrivAndFS, cluster)
		}
	}

	return clusterComputeResourceWithPrivAndFS, nil
}

// Returns datastore managed objects with info & summary properties for a given
// cluster.
func getDatastoreMOsFromCluster(ctx context.Context, vc *cnsvsphere.VirtualCenter,
	cluster *object.ClusterComputeResource) ([]mo.Datastore, error) {
	log := logger.GetLogger(ctx)
	datastores, err := cluster.Datastores(ctx)
	if err != nil {
		log.Errorf("Error occurred while getting datastores from cluster %q. error: %+v", cluster.Reference().Value, err)
		return nil, err
	}

	// Get datastore properties.
	pc := property.DefaultCollector(vc.Client.Client)
	properties := []string{"info", "summary"}
	var dsList []vim25types.ManagedObjectReference
	var dsMoList []mo.Datastore
	for _, datastore := range datastores {
		dsList = append(dsList, datastore.Reference())
	}
	err = pc.Retrieve(ctx, dsList, properties, &dsMoList)
	if err != nil {
		log.Errorf("failed to retrieve datastores. dsObjList: %+v, properties: %+v, err: %v", dsList, properties, err)
		return nil, err
	}
	return dsMoList, nil
}
