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

	"github.com/vmware/govmomi/object"
	"github.com/vmware/govmomi/vim25/types"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"sigs.k8s.io/vsphere-csi-driver/pkg/common/cns-lib/vsphere"
	cnsvsphere "sigs.k8s.io/vsphere-csi-driver/pkg/common/cns-lib/vsphere"
	"sigs.k8s.io/vsphere-csi-driver/pkg/csi/service/logger"
)

// AuthorizationService exposes interfaces to support authorization check on datastores and get datastores which need to be
// ignored by create volume
type AuthorizationService interface {

	// GetDatastoreIgnoreMapForBlockVolumes returns a map which maps datastore url to datastore info which user does not have Datastore.FileManagement privilege
	GetDatastoreIgnoreMapForBlockVolumes(ctx context.Context) map[string]*cnsvsphere.DatastoreInfo
}

// AuthManager maintains an internal map to track the datastores that need to be ignored by create volume
type AuthManager struct {
	// map the datastore url to datastore info which need to be ignored when invoking CNS CreateVolume API
	datastoreIgnoreMapForBlockVolumes map[string]*cnsvsphere.DatastoreInfo
	// this mutex is to make sure the update for datatoreIgnoreMap is mutually exclusive
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

			datastoreIgnoreMapForBlockVolumes: make(map[string]*cnsvsphere.DatastoreInfo),
			rwMutex:                           sync.RWMutex{},
			vcenter:                           vc,
		}
		authManagerInstance.refreshDatastoreIgnoreMap()
	})

	log.Info("authorization service initialized")
	return authManagerInstance, nil
}

// GetDatastoreIgnoreMapForBlockVolumes returns a DatastoreIgnoreMapForBlockVolumes. This map maps datastore url to datastore info
// which need to be ignored when creating block volume.
func (authManager *AuthManager) GetDatastoreIgnoreMapForBlockVolumes(ctx context.Context) map[string]*cnsvsphere.DatastoreInfo {
	datastoreIgnoreMapForBlockVolumes := make(map[string]*cnsvsphere.DatastoreInfo)
	authManager.rwMutex.RLock()
	defer authManager.rwMutex.RUnlock()
	for dsURL, dsInfo := range authManager.datastoreIgnoreMapForBlockVolumes {
		datastoreIgnoreMapForBlockVolumes[dsURL] = dsInfo
	}
	return datastoreIgnoreMapForBlockVolumes
}

// refreshDatastoreIgnoreMap scans all datastores in vCenter to check privileges, and compute the DatastoreIgnoreMap
func (authManager *AuthManager) refreshDatastoreIgnoreMap() {
	ctx, log := logger.GetNewContextWithLogger()
	log.Info("auth manager: refreshDatastoreIgnoreMap is triggered")
	newDatastoreIgnoreMapForBlockVolumes, err := GenerateDatastoreIgnoreMapForBlockVolumes(ctx, authManager.vcenter)
	if err == nil {
		authManager.rwMutex.Lock()
		defer authManager.rwMutex.Unlock()
		authManager.datastoreIgnoreMapForBlockVolumes = newDatastoreIgnoreMapForBlockVolumes
		log.Debugf("auth manager: datastoreIgnoreMapForBlockVolumes is updated to %v", newDatastoreIgnoreMapForBlockVolumes)
	} else {
		log.Warnf("auth manager: failed to get updated datastoreIgnoreMapForBlockVolumes, Err: %v", err)
	}
}

// ComputeDatastoreIgnoreMap refreshes DatastoreIgnoreMapForBlockVolumes periodically
func ComputeDatastoreIgnoreMap(authManager *AuthManager, authCheckInterval int) {
	log := logger.GetLoggerWithNoContext()
	log.Info("auth manager: ComputeDatastoreIgnoreMap enter")
	ticker := time.NewTicker(time.Duration(authCheckInterval) * time.Minute)
	for range ticker.C {
		authManager.refreshDatastoreIgnoreMap()
	}
}

// GenerateDatastoreIgnoreMapForBlockVolumes scans all datastores in Vcenter and do privilege check on those datastoes
// It will return datastores which does not have the privileges for creating volume
func GenerateDatastoreIgnoreMapForBlockVolumes(ctx context.Context, vc *vsphere.VirtualCenter) (map[string]*cnsvsphere.DatastoreInfo, error) {
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
	for _, dc := range dcList {
		dsURLTodsInfoMap, err = dc.GetAllDatastores(ctx)
		if err != nil {
			msg := fmt.Sprintf("failed to get dsURLTodsInfoMap. err: %+v", err)
			log.Error(msg)
		}
		for dsURL, dsInfo := range dsURLTodsInfoMap {
			dsURLs = append(dsURLs, dsURL)
			dsInfos = append(dsInfos, dsInfo)
		}
	}

	log.Debugf("auth manager: dsURLs %v dsInfos %v", dsURLs, dsInfos)
	dsIgnoreURLToInfoMap := make(map[string]*cnsvsphere.DatastoreInfo)
	// get authMgr
	authMgr := object.NewAuthorizationManager(vc.Client.Client)
	privIds := []string{DsPriv, SysReadPriv}
	var entities []types.ManagedObjectReference
	for _, dsInfo := range dsInfos {
		// go through all datastores to build entities
		dsMoRef := dsInfo.Datastore.Datastore.Reference()
		entities = append(entities, dsMoRef)
	}

	var result []types.EntityPrivilege
	userName := vc.Config.Username
	// invoke authMgr function HasUserPrivilegeOnEntities
	result, err = authMgr.HasUserPrivilegeOnEntities(ctx, entities, userName, privIds)
	if err != nil {
		log.Errorf("auth manager: failed to check privilege %v on entities %v for user %s", privIds, entities, userName)
		return nil, err
	}
	log.Debugf("auth manager: HasUserPrivilegeOnEntities returns %v when checking privileges %v on entities %v for user %s", result, privIds, entities, userName)
	for index, entityPriv := range result {
		needIgnore := false
		privAvails := entityPriv.PrivAvailability
		for _, privAvail := range privAvails {
			// required privilege is not grant for this entity
			if !privAvail.IsGranted {
				needIgnore = true
				break
			}
		}
		if needIgnore {
			dsIgnoreURLToInfoMap[dsURLs[index]] = dsInfos[index]
			log.Debugf("auth manager: datastore with URL %s  and name %s is added to dsIgnoreURLToInfoMap", dsInfos[index].Info.Name, dsURLs[index])
		}
	}
	return dsIgnoreURLToInfoMap, nil
}
