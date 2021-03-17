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
	"github.com/vmware/govmomi/vim25/types"
	vim25types "github.com/vmware/govmomi/vim25/types"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"sigs.k8s.io/vsphere-csi-driver/pkg/common/cns-lib/vsphere"
	cnsvsphere "sigs.k8s.io/vsphere-csi-driver/pkg/common/cns-lib/vsphere"
	"sigs.k8s.io/vsphere-csi-driver/pkg/csi/service/logger"
)

// AuthorizationService exposes interfaces to support authorization check on datastores and get datastores
// which will be used by create volume
type AuthorizationService interface {
	// GetDatastoreMapForBlockVolumes returns a map of datastore URL to datastore info for only those
	// datastores the CSI VC user has Datastore.FileManagement privilege for.
	GetDatastoreMapForBlockVolumes(ctx context.Context) map[string]*cnsvsphere.DatastoreInfo

	// GetDatastoreMapForFileVolumes returns a map of datastore URL to datastore info for only those
	// datastores the CSI VC user has Host.Config.Storage privilege on vSAN cluster with vSAN FS enabled.
	GetDatastoreMapForFileVolumes(ctx context.Context) map[string]*cnsvsphere.DatastoreInfo
}

// AuthManager maintains an internal map to track the datastores that need to be used by create volume
type AuthManager struct {
	// map the datastore url to datastore info which need to be used when invoking CNS CreateVolume API
	datastoreMapForBlockVolumes map[string]*cnsvsphere.DatastoreInfo
	// map the datastore url to datastore info which need to be used when invoking CNS CreateVolume API
	datastoreMapForFileVolumes map[string]*cnsvsphere.DatastoreInfo
	// this mutex is to make sure the update for datastoreMap is mutually exclusive
	rwMutex sync.RWMutex
	// vCenter Instance
	vcenter *cnsvsphere.VirtualCenter
}

// onceForAuthorizationService is used for initializing the AuthorizationService singleton.
var onceForAuthorizationService sync.Once

// authManagerIntance is instance of authManager and implements interface for AuthorizationService
var authManagerInstance *AuthManager

// GetAuthorizationService returns the singleton AuthorizationService
func GetAuthorizationService(ctx context.Context, vc *cnsvsphere.VirtualCenter) (AuthorizationService, error) {
	log := logger.GetLogger(ctx)
	onceForAuthorizationService.Do(func() {
		log.Info("Initializing authorization service...")

		authManagerInstance = &AuthManager{
			datastoreMapForBlockVolumes: make(map[string]*cnsvsphere.DatastoreInfo),
			datastoreMapForFileVolumes:  make(map[string]*cnsvsphere.DatastoreInfo),
			rwMutex:                     sync.RWMutex{},
			vcenter:                     vc,
		}
	})

	log.Info("authorization service initialized")
	return authManagerInstance, nil
}

// GetDatastoreMapForBlockVolumes returns a DatastoreMapForBlockVolumes. This map maps datastore url to datastore info
// which need to be used when creating block volume.
func (authManager *AuthManager) GetDatastoreMapForBlockVolumes(ctx context.Context) map[string]*cnsvsphere.DatastoreInfo {
	datastoreMapForBlockVolumes := make(map[string]*cnsvsphere.DatastoreInfo)
	authManager.rwMutex.RLock()
	defer authManager.rwMutex.RUnlock()
	for dsURL, dsInfo := range authManager.datastoreMapForBlockVolumes {
		datastoreMapForBlockVolumes[dsURL] = dsInfo
	}
	return datastoreMapForBlockVolumes
}

// GetDatastoreMapForFileVolumes returns a DatastoreMapForFileVolumes. This map maps datastore url to datastore info
// which need to be used when creating file volume.
func (authManager *AuthManager) GetDatastoreMapForFileVolumes(ctx context.Context) map[string]*cnsvsphere.DatastoreInfo {
	datastoreMapForFileVolumes := make(map[string]*cnsvsphere.DatastoreInfo)
	authManager.rwMutex.RLock()
	defer authManager.rwMutex.RUnlock()
	for dsURL, dsInfo := range authManager.datastoreMapForFileVolumes {
		datastoreMapForFileVolumes[dsURL] = dsInfo
	}
	return datastoreMapForFileVolumes
}

// refreshDatastoreMapForBlockVolumes scans all datastores in vCenter to check privileges, and compute the
// datastoreMapForBlockVolumes
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

// refreshDatastoreMapForFileVolumes scans all vSAN datastores with vSAN FS enabled in vCenter to
// check privileges, and compute the datastoreMapForFileVolumes
func (authManager *AuthManager) refreshDatastoreMapForFileVolumes() {
	ctx, log := logger.GetNewContextWithLogger()
	log.Debug("auth manager: refreshDatastoreMapForFileVolumes is triggered")
	newDatastoreMapForFileVolumes, err := GenerateDatastoreMapForFileVolumes(ctx, authManager.vcenter)
	if err == nil {
		authManager.rwMutex.Lock()
		defer authManager.rwMutex.Unlock()
		authManager.datastoreMapForFileVolumes = newDatastoreMapForFileVolumes
		log.Debugf("auth manager: datastoreMapForFileVolumes is updated to %v", newDatastoreMapForFileVolumes)
	} else {
		log.Warnf("auth manager: failed to get updated datastoreMapForFileVolumes, Err: %v", err)
	}
}

// ComputeDatastoreMapForBlockVolumes refreshes DatastoreMapForBlockVolumes periodically
func ComputeDatastoreMapForBlockVolumes(authManager *AuthManager, authCheckInterval int) {
	log := logger.GetLoggerWithNoContext()
	log.Info("auth manager: ComputeDatastoreMapForBlockVolumes enter")
	ticker := time.NewTicker(time.Duration(authCheckInterval) * time.Minute)
	for ; true; <-ticker.C {
		authManager.refreshDatastoreMapForBlockVolumes()
	}
}

// ComputeDatastoreMapForFileVolumes refreshes DatastoreMapForFileVolumes periodically
func ComputeDatastoreMapForFileVolumes(authManager *AuthManager, authCheckInterval int) {
	log := logger.GetLoggerWithNoContext()
	log.Info("auth manager: ComputeDatastoreMapForFileVolumes enter")
	ticker := time.NewTicker(time.Duration(authCheckInterval) * time.Minute)
	for ; true; <-ticker.C {
		authManager.refreshDatastoreMapForFileVolumes()
	}
}

// GenerateDatastoreMapForBlockVolumes scans all datastores in Vcenter and do privilege check on those datastoes
// It will return datastores which has the privileges for creating block volume
func GenerateDatastoreMapForBlockVolumes(ctx context.Context, vc *vsphere.VirtualCenter) (map[string]*cnsvsphere.DatastoreInfo, error) {
	log := logger.GetLogger(ctx)
	// get all datastores from VC
	dcList, err := vc.GetDatacenters(ctx)
	if err != nil {
		msg := fmt.Sprintf("failed to get datacenter list. err: %+v", err)
		log.Error(msg)
		return nil, status.Errorf(codes.Internal, msg)
	}
	var dsURLTodsInfoMap map[string]*cnsvsphere.DatastoreInfo
	var dsURLs []string
	var dsInfos []*cnsvsphere.DatastoreInfo
	var entities []types.ManagedObjectReference
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

// GenerateDatastoreMapForFileVolumes scans all datastores in Vcenter and do privilege check on those datastoes
// It will return datastores which has the privileges for creating file volume
func GenerateDatastoreMapForFileVolumes(ctx context.Context, vc *vsphere.VirtualCenter) (map[string]*cnsvsphere.DatastoreInfo, error) {
	log := logger.GetLogger(ctx)
	dsURLToInfoMap := make(map[string]*cnsvsphere.DatastoreInfo)
	// get all vSAN datastores from VC
	vsanDsURLToInfoMap, err := vc.GetVsanDatastores(ctx)
	if err != nil {
		log.Errorf("failed to get vSAN datastores with error %+v", err)
		return nil, err
	}
	// Return empty map if no vSAN datastores are found.
	if len(vsanDsURLToInfoMap) == 0 {
		log.Debug("No vSAN datastores found")
		return dsURLToInfoMap, nil
	}
	var allvsanDatastoreUrls []string
	for dsURL := range vsanDsURLToInfoMap {
		allvsanDatastoreUrls = append(allvsanDatastoreUrls, dsURL)
	}
	fsEnabledMap, err := IsFileServiceEnabled(ctx, allvsanDatastoreUrls, vc)
	if err != nil {
		log.Errorf("failed to get if file service is enabled on vsan datastores with error %+v", err)
		return nil, err
	}

	for dsURL, dsInfo := range vsanDsURLToInfoMap {
		if val, ok := fsEnabledMap[dsURL]; ok {
			if val {
				dsURLToInfoMap[dsURL] = dsInfo
			}
		}
	}
	log.Debugf("dsURLToInfoMap is %+v", dsURLToInfoMap)
	return dsURLToInfoMap, nil
}

// IsFileServiceEnabled checks if file service is enabled on the specified datastoreUrls.
func IsFileServiceEnabled(ctx context.Context, datastoreUrls []string, vc *vsphere.VirtualCenter) (map[string]bool, error) {
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
	// This gets the datastore to file service enabled map for all the vsan datastores.
	dsToFileServiceEnabledMap, err := getDsToFileServiceEnabledMap(ctx, vc)
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

// getDatastoresWithBlockVolumePrivs gets datastores with required priv for CSI user
func getDatastoresWithBlockVolumePrivs(ctx context.Context, vc *vsphere.VirtualCenter,
	dsURLs []string, dsInfos []*cnsvsphere.DatastoreInfo, entities []types.ManagedObjectReference) (map[string]*cnsvsphere.DatastoreInfo, error) {
	log := logger.GetLogger(ctx)
	log.Debugf("auth manager: file - dsURLs %v dsInfos %v", dsURLs, dsInfos)
	// dsURLToInfoMap will store a list of vSAN datastores with vSAN FS enabled for which
	// CSI user has privilege to
	dsURLToInfoMap := make(map[string]*cnsvsphere.DatastoreInfo)
	// get authMgr
	authMgr := object.NewAuthorizationManager(vc.Client.Client)
	privIds := []string{DsPriv, SysReadPriv}

	userName := vc.Config.Username
	// invoke authMgr function HasUserPrivilegeOnEntities
	result, err := authMgr.HasUserPrivilegeOnEntities(ctx, entities, userName, privIds)
	if err != nil {
		log.Errorf("auth manager: failed to check privilege %v on entities %v for user %s", privIds, entities, userName)
		return nil, err
	}
	log.Debugf("auth manager: HasUserPrivilegeOnEntities returns %v when checking privileges %v on entities %v for user %s", result, privIds, entities, userName)
	for index, entityPriv := range result {
		hasPriv := true
		privAvails := entityPriv.PrivAvailability
		for _, privAvail := range privAvails {
			// required privilege is not grant for this entity
			if !privAvail.IsGranted {
				hasPriv = false
				break
			}
		}
		if hasPriv {
			dsURLToInfoMap[dsURLs[index]] = dsInfos[index]
			log.Debugf("auth manager: datastore with URL %s and name %s has privileges and is added to dsURLToInfoMap", dsInfos[index].Info.Name, dsURLs[index])
		}
	}
	return dsURLToInfoMap, nil
}

// Creates a map of vsan datastores to file service status (enabled/disabled).
func getDsToFileServiceEnabledMap(ctx context.Context, vc *vsphere.VirtualCenter) (map[string]bool, error) {
	log := logger.GetLogger(ctx)
	log.Debugf("Computing the cluster to file service status (enabled/disabled) map.")
	datacenters, err := vc.ListDatacenters(ctx)
	if err != nil {
		log.Errorf("failed to find datacenters from VC: %+v, Error: %+v", vc.Config.Host, err)
		return nil, err
	}

	dsToFileServiceEnabledMap := make(map[string]bool)
	// Get clusters from datacenters
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

	// Get Clusters with HostConfigStoragePriv
	authMgr := object.NewAuthorizationManager(vc.Client.Client)
	privIds := []string{HostConfigStoragePriv}
	userName := vc.Config.Username
	var entities []types.ManagedObjectReference
	clusterComputeResourcesMap := make(map[string]*object.ClusterComputeResource)
	for _, cluster := range clusterComputeResources {
		entities = append(entities, cluster.Reference())
		clusterComputeResourcesMap[cluster.Reference().Value] = cluster
	}

	// invoke authMgr function HasUserPrivilegeOnEntities
	result, err := authMgr.HasUserPrivilegeOnEntities(ctx, entities, userName, privIds)
	if err != nil {
		log.Errorf("auth manager: failed to check privilege %v on entities %v for user %s", privIds, entities, userName)
		return nil, err
	}
	log.Debugf("auth manager: HasUserPrivilegeOnEntities returns %v when checking privileges %v on entities %v for user %s", result, privIds, entities, userName)
	clusterComputeResourceWithPriv := []*object.ClusterComputeResource{}
	for _, entityPriv := range result {
		hasPriv := true
		privAvails := entityPriv.PrivAvailability
		for _, privAvail := range privAvails {
			// required privilege is not grant for this entity
			if !privAvail.IsGranted {
				hasPriv = false
				break
			}
		}
		if hasPriv {
			clusterComputeResourceWithPriv = append(clusterComputeResourceWithPriv, clusterComputeResourcesMap[entityPriv.Entity.Value])
		}
	}
	log.Debugf("Clusters with priv: %s are : %+v", HostConfigStoragePriv, clusterComputeResourceWithPriv)

	// Get clusters which are vSAN and have vSAN FS enabled
	pc := property.DefaultCollector(vc.Client.Client)
	properties := []string{"info", "summary"}
	// Get all the vsan datastores with vsan FS from these clusters and add it to map.
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
		var dsList []vim25types.ManagedObjectReference
		var dsMoList []mo.Datastore
		datastores, err := cluster.Datastores(ctx)
		if err != nil {
			log.Errorf("Error occurred while getting datastores from clusters. error: %+v", err)
			return nil, err
		}
		for _, datastore := range datastores {
			dsList = append(dsList, datastore.Reference())
		}
		err = pc.Retrieve(ctx, dsList, properties, &dsMoList)
		if err != nil {
			log.Errorf("failed to get Datastore managed objects from datastore objects."+
				" dsObjList: %+v, properties: %+v, err: %v", dsList, properties, err)
			return nil, err
		}
		// TODO: ALso identify which vSAN datastore is management
		// and which one is a workload datastore to support file volumes on VMC
		for _, dsMo := range dsMoList {
			if dsMo.Summary.Type == VsanDatastoreType {
				dsToFileServiceEnabledMap[dsMo.Info.GetDatastoreInfo().Url] = config.FileServiceConfig.Enabled
			}
		}
	}
	return dsToFileServiceEnabledMap, nil
}
