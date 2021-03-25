/*
Copyright 2019 The Kubernetes Authors.

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
	"errors"
	"fmt"

	"github.com/davecgh/go-spew/spew"
	cnstypes "github.com/vmware/govmomi/cns/types"
	"github.com/vmware/govmomi/object"
	vim25types "github.com/vmware/govmomi/vim25/types"
	vsanfstypes "github.com/vmware/govmomi/vsan/vsanfs/types"
	"golang.org/x/net/context"
	cnsvolume "sigs.k8s.io/vsphere-csi-driver/pkg/common/cns-lib/volume"
	"sigs.k8s.io/vsphere-csi-driver/pkg/common/cns-lib/vsphere"
	"sigs.k8s.io/vsphere-csi-driver/pkg/csi/service/logger"
)

// CreateBlockVolumeUtil is the helper function to create CNS block volume.
func CreateBlockVolumeUtil(ctx context.Context, clusterFlavor cnstypes.CnsClusterFlavor, manager *Manager, spec *CreateVolumeSpec, sharedDatastores []*vsphere.DatastoreInfo) (*cnsvolume.CnsVolumeInfo, error) {
	log := logger.GetLogger(ctx)
	vc, err := GetVCenter(ctx, manager)
	if err != nil {
		log.Errorf("failed to get vCenter from Manager, err: %+v", err)
		return nil, err
	}
	if spec.ScParams.StoragePolicyName != "" {
		// Get Storage Policy ID from Storage Policy Name
		spec.StoragePolicyID, err = vc.GetStoragePolicyIDByName(ctx, spec.ScParams.StoragePolicyName)
		if err != nil {
			log.Errorf("Error occurred while getting Profile Id from Profile Name: %s, err: %+v", spec.ScParams.StoragePolicyName, err)
			return nil, err
		}
	}
	var datastores []vim25types.ManagedObjectReference
	if spec.ScParams.DatastoreURL == "" {
		// Check if datastore URL is specified by the storage pool parameter
		if spec.VsanDirectDatastoreURL != "" {
			// Create Datacenter object
			var dcList []*vsphere.Datacenter
			for _, dc := range vc.Config.DatacenterPaths {
				dcList = append(dcList,
					&vsphere.Datacenter{
						Datacenter: object.NewDatacenter(
							vc.Client.Client,
							vim25types.ManagedObjectReference{
								Type:  "Datacenter",
								Value: dc,
							}),
						VirtualCenterHost: vc.Config.Host,
					})
			}
			// Search the datastore from the URL in the datacenter list
			var datastoreObj *vsphere.Datastore
			for _, datacenter := range dcList {
				datastoreObj, err = datacenter.GetDatastoreByURL(ctx, spec.VsanDirectDatastoreURL)
				if err != nil {
					log.Warnf("Failed to find datastore with URL %q in datacenter %q from VC %q, Error: %+v",
						spec.VsanDirectDatastoreURL, datacenter.InventoryPath, vc.Config.Host, err)
					continue
				}
				log.Debugf("Successfully fetched the datastore %v from the URL: %v",
					datastoreObj.Reference(), spec.VsanDirectDatastoreURL)
				datastores = append(datastores, datastoreObj.Reference())
				break
			}
			if datastores == nil {
				errMsg := fmt.Sprintf("DatastoreURL: %s specified in the create volume spec is not found.",
					spec.VsanDirectDatastoreURL)
				return nil, errors.New(errMsg)
			}
		} else {
			//  If DatastoreURL is not specified in StorageClass, get all shared datastores
			datastores = getDatastoreMoRefs(sharedDatastores)
		}
	} else {
		// Check datastore specified in the StorageClass should be shared datastore across all nodes.

		// vc.GetDatacenters returns datacenters found on the VirtualCenter.
		// If no datacenters are mentioned in the VirtualCenterConfig during registration, all
		// Datacenters for the given VirtualCenter will be returned, else only the listed
		// Datacenters are returned.
		datacenters, err := vc.GetDatacenters(ctx)
		if err != nil {
			log.Errorf("failed to find datacenters from VC: %+v, Error: %+v", vc.Config.Host, err)
			return nil, err
		}
		isSharedDatastoreURL := false
		var datastoreObj *vsphere.Datastore
		for _, datacenter := range datacenters {
			datastoreObj, err = datacenter.GetDatastoreByURL(ctx, spec.ScParams.DatastoreURL)
			if err != nil {
				log.Warnf("failed to find datastore with URL %q in datacenter %q from VC %q, Error: %+v", spec.ScParams.DatastoreURL, datacenter.InventoryPath, vc.Config.Host, err)
				continue
			}
			for _, sharedDatastore := range sharedDatastores {
				if sharedDatastore.Info.Url == spec.ScParams.DatastoreURL {
					isSharedDatastoreURL = true
					break
				}
			}
			if isSharedDatastoreURL {
				break
			}
		}
		if datastoreObj == nil {
			errMsg := fmt.Sprintf("DatastoreURL: %s specified in the storage class is not found.", spec.ScParams.DatastoreURL)
			log.Errorf(errMsg)
			return nil, errors.New(errMsg)
		}
		if isSharedDatastoreURL {
			datastores = append(datastores, datastoreObj.Reference())
		} else {
			errMsg := fmt.Sprintf("Datastore: %s specified in the storage class is not accessible to all nodes.", spec.ScParams.DatastoreURL)
			log.Errorf(errMsg)
			return nil, errors.New(errMsg)
		}
	}
	var containerClusterArray []cnstypes.CnsContainerCluster
	containerCluster := vsphere.GetContainerCluster(manager.CnsConfig.Global.ClusterID, manager.CnsConfig.VirtualCenter[vc.Config.Host].User, clusterFlavor, manager.CnsConfig.Global.ClusterDistribution)
	containerClusterArray = append(containerClusterArray, containerCluster)
	createSpec := &cnstypes.CnsVolumeCreateSpec{
		Name:       spec.Name,
		VolumeType: spec.VolumeType,
		Datastores: datastores,
		BackingObjectDetails: &cnstypes.CnsBlockBackingDetails{
			CnsBackingObjectDetails: cnstypes.CnsBackingObjectDetails{
				CapacityInMb: spec.CapacityMB,
			},
		},
		Metadata: cnstypes.CnsVolumeMetadata{
			ContainerCluster:      containerCluster,
			ContainerClusterArray: containerClusterArray,
		},
	}
	if spec.StoragePolicyID != "" {
		profileSpec := &vim25types.VirtualMachineDefinedProfileSpec{
			ProfileId: spec.StoragePolicyID,
		}
		if spec.AffineToHost != "" {
			hostVsanUUID, err := getHostVsanUUID(ctx, spec.AffineToHost, vc)
			if err != nil {
				log.Errorf("failed to get the vSAN UUID for node: %s", spec.AffineToHost)
				return nil, err
			}
			param1 := vim25types.KeyValue{Key: VsanAffinityKey, Value: hostVsanUUID}
			param2 := vim25types.KeyValue{Key: VsanAffinityMandatory, Value: "1"}
			param3 := vim25types.KeyValue{Key: VsanMigrateForDecom, Value: "1"}
			profileSpec.ProfileParams = append(profileSpec.ProfileParams, param1, param2, param3)
		}
		createSpec.Profile = append(createSpec.Profile, profileSpec)
	}

	log.Debugf("vSphere CSI driver creating volume %s with create spec %+v", spec.Name, spew.Sdump(createSpec))
	volumeInfo, err := manager.VolumeManager.CreateVolume(ctx, createSpec)
	if err != nil {
		log.Errorf("failed to create disk %s with error %+v", spec.Name, err)
		return nil, err
	}
	return volumeInfo, nil
}

// CreateFileVolumeUtil is the helper function to create CNS file volume with datastores.
func CreateFileVolumeUtil(ctx context.Context, clusterFlavor cnstypes.CnsClusterFlavor,
	manager *Manager, spec *CreateVolumeSpec, datastores []*vsphere.DatastoreInfo) (string, error) {
	log := logger.GetLogger(ctx)
	vc, err := GetVCenter(ctx, manager)
	if err != nil {
		log.Errorf("failed to get vCenter from Manager, err: %+v", err)
		return "", err
	}
	if spec.ScParams.StoragePolicyName != "" {
		// Get Storage Policy ID from Storage Policy Name
		spec.StoragePolicyID, err = vc.GetStoragePolicyIDByName(ctx, spec.ScParams.StoragePolicyName)
		if err != nil {
			log.Errorf("Error occurred while getting Profile Id from Profile Name: %q, err: %+v", spec.ScParams.StoragePolicyName, err)
			return "", err
		}
	}
	var datastoreMorefs []vim25types.ManagedObjectReference
	if spec.ScParams.DatastoreURL == "" {
		datastoreMorefs = getDatastoreMoRefs(datastores)
	} else {
		// If datastoreUrl is set in storage class, then check if part of input datastores.
		// If true, create the file volume on the datastoreUrl set in storage class.
		var isFound bool
		for _, dsInfo := range datastores {
			if spec.ScParams.DatastoreURL == dsInfo.Info.Url {
				isFound = true
				datastoreMorefs = append(datastoreMorefs, dsInfo.Reference())
				break
			}
		}
		if !isFound {
			msg := fmt.Sprintf("CSI user doesn't have permission on the datastore: %s specified in storage class",
				spec.ScParams.DatastoreURL)
			log.Error(msg)
			return "", errors.New(msg)
		}
	}

	// Retrieve net permissions from CnsConfig of manager and convert to required format
	netPerms := make([]vsanfstypes.VsanFileShareNetPermission, 0)
	for _, netPerm := range manager.CnsConfig.NetPermissions {
		netPerms = append(netPerms, vsanfstypes.VsanFileShareNetPermission{
			Ips:         netPerm.Ips,
			Permissions: netPerm.Permissions,
			AllowRoot:   !netPerm.RootSquash,
		})
	}

	var containerClusterArray []cnstypes.CnsContainerCluster
	containerCluster := vsphere.GetContainerCluster(manager.CnsConfig.Global.ClusterID, manager.CnsConfig.VirtualCenter[vc.Config.Host].User, clusterFlavor, manager.CnsConfig.Global.ClusterDistribution)
	containerClusterArray = append(containerClusterArray, containerCluster)
	createSpec := &cnstypes.CnsVolumeCreateSpec{
		Name:       spec.Name,
		VolumeType: spec.VolumeType,
		Datastores: datastoreMorefs,
		BackingObjectDetails: &cnstypes.CnsVsanFileShareBackingDetails{
			CnsFileBackingDetails: cnstypes.CnsFileBackingDetails{
				CnsBackingObjectDetails: cnstypes.CnsBackingObjectDetails{
					CapacityInMb: spec.CapacityMB,
				},
			},
		},
		Metadata: cnstypes.CnsVolumeMetadata{
			ContainerCluster:      containerCluster,
			ContainerClusterArray: containerClusterArray,
		},
		CreateSpec: &cnstypes.CnsVSANFileCreateSpec{
			SoftQuotaInMb: spec.CapacityMB,
			Permission:    netPerms,
		},
	}
	if spec.StoragePolicyID != "" {
		profileSpec := &vim25types.VirtualMachineDefinedProfileSpec{
			ProfileId: spec.StoragePolicyID,
		}
		createSpec.Profile = append(createSpec.Profile, profileSpec)
	}

	log.Debugf("vSphere CSI driver creating volume %q with create spec %+v", spec.Name, spew.Sdump(createSpec))
	volumeInfo, err := manager.VolumeManager.CreateVolume(ctx, createSpec)
	if err != nil {
		log.Errorf("failed to create file volume %q with error %+v", spec.Name, err)
		return "", err
	}
	return volumeInfo.VolumeID.Id, nil
}

// CreateFileVolumeUtilOld is the helper function to create CNS file volume with datastores from
// TargetvSANFileShareDatastoreURLs in vsphere conf.
func CreateFileVolumeUtilOld(ctx context.Context, clusterFlavor cnstypes.CnsClusterFlavor,
	manager *Manager, spec *CreateVolumeSpec) (string, error) {
	log := logger.GetLogger(ctx)
	vc, err := GetVCenter(ctx, manager)
	if err != nil {
		log.Errorf("failed to get vCenter from Manager, err: %+v", err)
		return "", err
	}
	if spec.ScParams.StoragePolicyName != "" {
		// Get Storage Policy ID from Storage Policy Name
		spec.StoragePolicyID, err = vc.GetStoragePolicyIDByName(ctx, spec.ScParams.StoragePolicyName)
		if err != nil {
			log.Errorf("Error occurred while getting Profile Id from Profile Name: %q, err: %+v", spec.ScParams.StoragePolicyName, err)
			return "", err
		}
	}
	var datastores []vim25types.ManagedObjectReference
	if spec.ScParams.DatastoreURL == "" {
		if len(manager.VcenterConfig.TargetvSANFileShareDatastoreURLs) == 0 {
			// get all vSAN datastores from VC
			vsanDsURLToInfoMap, err := vc.GetVsanDatastores(ctx)
			if err != nil {
				log.Errorf("failed to get vSAN datastores with error %+v", err)
				return "", err
			}
			var allvsanDatastoreUrls []string
			for dsURL := range vsanDsURLToInfoMap {
				allvsanDatastoreUrls = append(allvsanDatastoreUrls, dsURL)
			}
			fsEnabledMap, err := IsFileServiceEnabled(ctx, allvsanDatastoreUrls, vc)
			if err != nil {
				log.Errorf("failed to get if file service is enabled on vsan datastores with error %+v", err)
				return "", err
			}
			for dsURL, dsInfo := range vsanDsURLToInfoMap {
				if val, ok := fsEnabledMap[dsURL]; ok {
					if val {
						datastores = append(datastores, dsInfo.Reference())
					}
				}
			}
			if len(datastores) == 0 {
				msg := "No file service enabled vsan datastore is present in the environment."
				log.Error(msg)
				return "", errors.New(msg)
			}
		} else {
			// If DatastoreURL is not specified in StorageClass, get all datastores from TargetvSANFileShareDatastoreURLs
			// in vcenter configuration.
			for _, TargetvSANFileShareDatastoreURL := range manager.VcenterConfig.TargetvSANFileShareDatastoreURLs {
				datastoreMoref, err := getDatastore(ctx, vc, TargetvSANFileShareDatastoreURL)
				if err != nil {
					log.Errorf("failed to get datastore %s. Error: %+v", TargetvSANFileShareDatastoreURL, err)
					return "", err
				}
				datastores = append(datastores, datastoreMoref)
			}
		}

	} else {
		// If datastoreUrl is set in storage class, then check the allowed list is empty.
		// If true, create the file volume on the datastoreUrl set in storage class.
		if len(manager.VcenterConfig.TargetvSANFileShareDatastoreURLs) == 0 {
			datastoreMoref, err := getDatastore(ctx, vc, spec.ScParams.DatastoreURL)
			if err != nil {
				log.Errorf("failed to get datastore %q. Error: %+v", spec.ScParams.DatastoreURL, err)
				return "", err
			}
			datastores = append(datastores, datastoreMoref)
		} else {
			// If datastoreUrl is set in storage class, then check if this is in the allowed list.
			found := false
			for _, targetVSANFSDsURL := range manager.VcenterConfig.TargetvSANFileShareDatastoreURLs {
				if spec.ScParams.DatastoreURL == targetVSANFSDsURL {
					found = true
					break
				}
			}
			if !found {
				msg := fmt.Sprintf("Datastore URL %q specified in storage class is not in the allowed list %+v",
					spec.ScParams.DatastoreURL, manager.VcenterConfig.TargetvSANFileShareDatastoreURLs)
				log.Error(msg)
				return "", errors.New(msg)
			}
			datastoreMoref, err := getDatastore(ctx, vc, spec.ScParams.DatastoreURL)
			if err != nil {
				log.Errorf("failed to get datastore %q. Error: %+v", spec.ScParams.DatastoreURL, err)
				return "", err
			}
			datastores = append(datastores, datastoreMoref)
		}
	}

	// Retrieve net permissions from CnsConfig of manager and convert to required format
	netPerms := make([]vsanfstypes.VsanFileShareNetPermission, 0)
	for _, netPerm := range manager.CnsConfig.NetPermissions {
		netPerms = append(netPerms, vsanfstypes.VsanFileShareNetPermission{
			Ips:         netPerm.Ips,
			Permissions: netPerm.Permissions,
			AllowRoot:   !netPerm.RootSquash,
		})
	}

	var containerClusterArray []cnstypes.CnsContainerCluster
	containerCluster := vsphere.GetContainerCluster(manager.CnsConfig.Global.ClusterID, manager.CnsConfig.VirtualCenter[vc.Config.Host].User, clusterFlavor, manager.CnsConfig.Global.ClusterDistribution)
	containerClusterArray = append(containerClusterArray, containerCluster)
	createSpec := &cnstypes.CnsVolumeCreateSpec{
		Name:       spec.Name,
		VolumeType: spec.VolumeType,
		Datastores: datastores,
		BackingObjectDetails: &cnstypes.CnsVsanFileShareBackingDetails{
			CnsFileBackingDetails: cnstypes.CnsFileBackingDetails{
				CnsBackingObjectDetails: cnstypes.CnsBackingObjectDetails{
					CapacityInMb: spec.CapacityMB,
				},
			},
		},
		Metadata: cnstypes.CnsVolumeMetadata{
			ContainerCluster:      containerCluster,
			ContainerClusterArray: containerClusterArray,
		},
		CreateSpec: &cnstypes.CnsVSANFileCreateSpec{
			SoftQuotaInMb: spec.CapacityMB,
			Permission:    netPerms,
		},
	}
	if spec.StoragePolicyID != "" {
		profileSpec := &vim25types.VirtualMachineDefinedProfileSpec{
			ProfileId: spec.StoragePolicyID,
		}
		createSpec.Profile = append(createSpec.Profile, profileSpec)
	}

	log.Debugf("vSphere CSI driver creating volume %q with create spec %+v", spec.Name, spew.Sdump(createSpec))
	volumeInfo, err := manager.VolumeManager.CreateVolume(ctx, createSpec)
	if err != nil {
		log.Errorf("failed to create file volume %q with error %+v", spec.Name, err)
		return "", err
	}
	return volumeInfo.VolumeID.Id, nil
}

// getHostVsanUUID returns the config.clusterInfo.nodeUuid of the ESX host's HostVsanSystem
func getHostVsanUUID(ctx context.Context, hostMoID string, vc *vsphere.VirtualCenter) (string, error) {
	log := logger.GetLogger(ctx)
	log.Debugf("getHostVsanUUID for host moid: %v", hostMoID)

	// get host vsan UUID from the HostSystem
	hostMoRef := vim25types.ManagedObjectReference{Type: "HostSystem", Value: hostMoID}
	host := &vsphere.HostSystem{
		HostSystem: object.NewHostSystem(vc.Client.Client, hostMoRef),
	}
	nodeUUID, err := host.GetHostVsanNodeUUID(ctx)
	if err != nil {
		log.Errorf("Failed getting ESX host %v vsanUuid, err: %v", host, err)
		return "", err
	}
	log.Debugf("Got HostVsanUUID for host %s: %s", host.Reference(), nodeUUID)
	return nodeUUID, nil
}

// AttachVolumeUtil is the helper function to attach CNS volume to specified vm
func AttachVolumeUtil(ctx context.Context, manager *Manager,
	vm *vsphere.VirtualMachine,
	volumeID string) (string, error) {
	log := logger.GetLogger(ctx)
	log.Debugf("vSphere CSI driver is attaching volume: %q to vm: %q", volumeID, vm.String())
	diskUUID, err := manager.VolumeManager.AttachVolume(ctx, vm, volumeID)
	if err != nil {
		log.Errorf("failed to attach disk %q with VM: %q. err: %+v", volumeID, vm.String(), err)
		return "", err
	}
	log.Debugf("Successfully attached disk %s to VM %v. Disk UUID is %s", volumeID, vm, diskUUID)
	return diskUUID, nil
}

// DetachVolumeUtil is the helper function to detach CNS volume from specified vm
func DetachVolumeUtil(ctx context.Context, manager *Manager,
	vm *vsphere.VirtualMachine,
	volumeID string) error {
	log := logger.GetLogger(ctx)
	log.Debugf("vSphere CSI driver is detaching volume: %s from node vm: %s", volumeID, vm.InventoryPath)
	err := manager.VolumeManager.DetachVolume(ctx, vm, volumeID)
	if err != nil {
		log.Errorf("failed to detach disk %s with err %+v", volumeID, err)
		return err
	}
	log.Debugf("Successfully detached disk %s from VM %v.", volumeID, vm)
	return nil
}

// DeleteVolumeUtil is the helper function to delete CNS volume for given volumeId
func DeleteVolumeUtil(ctx context.Context, volManager cnsvolume.Manager, volumeID string, deleteDisk bool) error {
	log := logger.GetLogger(ctx)
	var err error
	log.Debugf("vSphere CSI driver is deleting volume: %s with deleteDisk flag: %t", volumeID, deleteDisk)
	err = volManager.DeleteVolume(ctx, volumeID, deleteDisk)
	if err != nil {
		log.Errorf("failed to delete disk %s, deleteDisk flag: %t with error %+v", volumeID, deleteDisk, err)
		return err
	}
	log.Debugf("Successfully deleted disk for volumeid: %s, deleteDisk flag: %t", volumeID, deleteDisk)
	return nil
}

// ExpandVolumeUtil is the helper function to extend CNS volume for given volumeId
func ExpandVolumeUtil(ctx context.Context, manager *Manager, volumeID string, capacityInMb int64) error {
	var err error
	log := logger.GetLogger(ctx)
	log.Debugf("vSphere CSI driver expanding volume %q to new size %d Mb.", volumeID, capacityInMb)

	expansionRequired, err := isExpansionRequired(ctx, volumeID, capacityInMb, manager)
	if err != nil {
		return err
	}
	if expansionRequired {
		log.Infof("Requested size %d Mb is greater than current size for volumeID: %q. Need volume expansion.", capacityInMb, volumeID)
		err = manager.VolumeManager.ExpandVolume(ctx, volumeID, capacityInMb)
		if err != nil {
			log.Errorf("failed to expand volume %q with error %+v", volumeID, err)
			return err
		}
		log.Infof("Successfully expanded volume for volumeid %q to new size %d Mb.", volumeID, capacityInMb)

	} else {
		log.Infof("Requested volume size is equal to current size %d Mb. Expansion not required.", capacityInMb)
	}
	return err
}

// QueryVolumeByID is the helper function to query volume by volumeID
func QueryVolumeByID(ctx context.Context, volManager cnsvolume.Manager, volumeID string) (*cnstypes.CnsVolume, error) {
	log := logger.GetLogger(ctx)
	queryFilter := cnstypes.CnsQueryFilter{
		VolumeIds: []cnstypes.CnsVolumeId{{Id: volumeID}},
	}
	queryResult, err := volManager.QueryVolume(ctx, queryFilter)
	if err != nil {
		msg := fmt.Sprintf("QueryVolume failed for volumeID: %s with error %+v", volumeID, err)
		log.Error(msg)
		return nil, err
	}
	if len(queryResult.Volumes) == 0 {
		msg := fmt.Sprintf("volumeID %q not found in QueryVolume", volumeID)
		log.Error(msg)
		return nil, ErrNotFound
	}
	return &queryResult.Volumes[0], nil
}

// Helper function to get DatastoreMoRefs
func getDatastoreMoRefs(datastores []*vsphere.DatastoreInfo) []vim25types.ManagedObjectReference {
	var datastoreMoRefs []vim25types.ManagedObjectReference
	for _, datastore := range datastores {
		datastoreMoRefs = append(datastoreMoRefs, datastore.Reference())
	}
	return datastoreMoRefs
}

// Helper function to get DatastoreMoRef for given datastoreURL in the given virtual center.
func getDatastore(ctx context.Context, vc *vsphere.VirtualCenter, datastoreURL string) (vim25types.ManagedObjectReference, error) {
	log := logger.GetLogger(ctx)
	datacenters, err := vc.GetDatacenters(ctx)
	if err != nil {
		return vim25types.ManagedObjectReference{}, err
	}
	var datastoreObj *vsphere.Datastore
	for _, datacenter := range datacenters {
		datastoreObj, err = datacenter.GetDatastoreByURL(ctx, datastoreURL)
		if err != nil {
			log.Warnf("failed to find datastore with URL %q in datacenter %q from VC %q, Error: %+v",
				datastoreURL, datacenter.InventoryPath, vc.Config.Host, err)
		} else {
			return datastoreObj.Reference(), nil
		}
	}

	msg := fmt.Sprintf("Unable to find datastore for datastore URL %s in VC %+v", datastoreURL, vc)
	return vim25types.ManagedObjectReference{}, errors.New(msg)
}

// isExpansionRequired verifies if the requested size to expand a volume is greater than the current size
func isExpansionRequired(ctx context.Context, volumeID string, requestedSize int64, manager *Manager) (bool, error) {
	log := logger.GetLogger(ctx)
	volumeIds := []cnstypes.CnsVolumeId{{Id: volumeID}}
	queryFilter := cnstypes.CnsQueryFilter{
		VolumeIds: volumeIds,
	}
	querySelection := cnstypes.CnsQuerySelection{
		Names: []string{
			string(cnstypes.QuerySelectionNameTypeBackingObjectDetails),
		},
	}
	// Query only the backing object details.
	queryResult, err := manager.VolumeManager.QueryAllVolume(ctx, queryFilter, querySelection)
	if err != nil {
		log.Errorf("failed to call QueryVolume for volumeID: %q: %v", volumeID, err)
		return false, err
	}
	var currentSize int64
	if len(queryResult.Volumes) > 0 {
		currentSize = queryResult.Volumes[0].BackingObjectDetails.(cnstypes.BaseCnsBackingObjectDetails).GetCnsBackingObjectDetails().CapacityInMb
	} else {
		msg := fmt.Sprintf("failed to find volume by querying volumeID: %q", volumeID)
		log.Error(msg)
		return false, err
	}

	return currentSize < requestedSize, nil
}
