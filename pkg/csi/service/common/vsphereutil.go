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
	"context"
	"fmt"
	"strconv"
	"strings"

	"github.com/container-storage-interface/spec/lib/go/csi"
	"github.com/davecgh/go-spew/spew"
	cnstypes "github.com/vmware/govmomi/cns/types"
	"github.com/vmware/govmomi/object"
	"github.com/vmware/govmomi/vim25/mo"
	vim25types "github.com/vmware/govmomi/vim25/types"
	vsanfstypes "github.com/vmware/govmomi/vsan/vsanfs/types"
	"google.golang.org/grpc/codes"
	"google.golang.org/protobuf/types/known/timestamppb"
	cnsvolume "sigs.k8s.io/vsphere-csi-driver/v3/pkg/common/cns-lib/volume"
	"sigs.k8s.io/vsphere-csi-driver/v3/pkg/common/cns-lib/vsphere"
	"sigs.k8s.io/vsphere-csi-driver/v3/pkg/common/config"
	csifault "sigs.k8s.io/vsphere-csi-driver/v3/pkg/common/fault"
	"sigs.k8s.io/vsphere-csi-driver/v3/pkg/common/utils"
	"sigs.k8s.io/vsphere-csi-driver/v3/pkg/csi/service/logger"
)

// VanillaCreateBlockVolParamsForMultiVC stores the parameters
// required to call CreateBlockVolumeUtilForMultiVC function.
type VanillaCreateBlockVolParamsForMultiVC struct {
	Vcenter                   *vsphere.VirtualCenter
	VolumeManager             cnsvolume.Manager
	CNSConfig                 *config.Config
	StoragePolicyID           string
	Spec                      *CreateVolumeSpec
	SharedDatastores          []*vsphere.DatastoreInfo
	SnapshotDatastoreURL      string
	ClusterFlavor             cnstypes.CnsClusterFlavor
	FilterSuspendedDatastores bool
}

// CreateBlockVolumeOptions defines the FSS required to create a block volume.
type CreateBlockVolumeOptions struct {
	FilterSuspendedDatastores,
	UseSupervisorId,
	IsVdppOnStretchedSvFssEnabled bool
	IsByokEnabled                  bool
	IsCSITransactionSupportEnabled bool
}

// CreateBlockVolumeUtil is the helper function to create CNS block volume.
func CreateBlockVolumeUtil(
	ctx context.Context,
	clusterFlavor cnstypes.CnsClusterFlavor,
	manager *Manager,
	spec *CreateVolumeSpec,
	sharedDatastores []*vsphere.DatastoreInfo,
	opts CreateBlockVolumeOptions,
	extraParams interface{}) (*cnsvolume.CnsVolumeInfo, string, error) {

	log := logger.GetLogger(ctx)
	vc, err := GetVCenter(ctx, manager)
	if err != nil {
		log.Errorf("failed to get vCenter from Manager, err: %+v", err)
		// TODO: need to extract fault from err returned by GetVCenter.
		// Currently, just return csi.fault.Internal.
		return nil, csifault.CSIInternalFault, err
	}
	if spec.ScParams.StoragePolicyName != "" {
		// Get Storage Policy ID from Storage Policy Name.
		spec.StoragePolicyID, err = vc.GetStoragePolicyIDByName(ctx, spec.ScParams.StoragePolicyName)
		if err != nil {
			log.Errorf("Error occurred while getting Profile Id from Profile Name: %s, err: %+v",
				spec.ScParams.StoragePolicyName, err)
			// TODO: need to extract fault from err returned by GetStoragePolicyIDByName.
			// Currently, just return csi.fault.Internal.
			return nil, csifault.CSIInternalFault, err
		}
	}

	if opts.FilterSuspendedDatastores {
		sharedDatastores, err = vsphere.FilterSuspendedDatastores(ctx, sharedDatastores)
		if err != nil {
			log.Errorf("Error occurred while filter suspended datastores, err: %+v", err)
			return nil, csifault.CSIInternalFault, err
		}
	}

	var datastoreObj *vsphere.Datastore
	var datastores []vim25types.ManagedObjectReference
	var datastoreInfoList []*vsphere.DatastoreInfo
	if spec.ScParams.DatastoreURL == "" {
		// Check if datastore URL is specified by the storage pool parameter.
		if spec.VsanDatastoreURL != "" {
			// Search the datastore from the URL in the datacenter list.
			var datastoreInfoObj *vsphere.DatastoreInfo
			for _, dc := range vc.Config.DatacenterPaths {
				datacenter := &vsphere.Datacenter{
					Datacenter: object.NewDatacenter(
						vc.Client.Client,
						vim25types.ManagedObjectReference{
							Type:  "Datacenter",
							Value: dc,
						}),
					VirtualCenterHost: vc.Config.Host,
				}
				datastoreInfoObj, err = datacenter.GetDatastoreInfoByURL(ctx, spec.VsanDatastoreURL)
				if err != nil {
					log.Warnf("Failed to find datastore with URL %q in datacenter %q from VC %q, Error: %+v",
						spec.VsanDatastoreURL, datacenter.InventoryPath, vc.Config.Host, err)
					continue
				}

				if opts.FilterSuspendedDatastores && vsphere.IsVolumeCreationSuspended(ctx, datastoreInfoObj) {
					continue
				}
				datastoreObj = datastoreInfoObj.Datastore
				log.Debugf("Successfully fetched the datastore %v from the URL: %v",
					datastoreObj.Reference(), spec.VsanDatastoreURL)
				datastores = append(datastores, datastoreObj.Reference())
				datastoreInfoList = append(datastoreInfoList, datastoreInfoObj)
				break
			}
			if datastores == nil {
				// TODO: Need to figure out which fault need to return when datastore is empty.
				// Currently, just return csi.fault.Internal.
				return nil, csifault.CSIInternalFault,
					logger.LogNewErrorf(log, "DatastoreURL: %s specified in the create volume spec is not found.",
						spec.VsanDatastoreURL)
			}
		} else {
			// If DatastoreURL is not specified in StorageClass, get all shared
			// datastores.
			datastores = getDatastoreMoRefs(sharedDatastores)
			datastoreInfoList = sharedDatastores
		}
	} else {
		// vc.GetDatacenters returns datacenters found on the VirtualCenter.
		// If no datacenters are mentioned in the VirtualCenterConfig during
		// registration, all Datacenters for the given VirtualCenter will be
		// returned, else only the listed Datacenters are returned.

		// TODO: Need to figure out which fault need to be extracted from err returned by GetDatacenters.
		// Currently, just return csi.fault.Internal.
		datacenters, err := vc.GetDatacenters(ctx)
		if err != nil {
			log.Errorf("failed to find datacenters from VC: %q, Error: %+v", vc.Config.Host, err)
			return nil, csifault.CSIInternalFault, err
		}
		// Check if DatastoreURL specified in the StorageClass is present in any one of the datacenters.
		var datastoreInfoObj *vsphere.DatastoreInfo
		for _, datacenter := range datacenters {
			datastoreInfoObj, err = datacenter.GetDatastoreInfoByURL(ctx, spec.ScParams.DatastoreURL)
			if err != nil {
				log.Warnf("failed to find datastore with URL %q in datacenter %q from VC %q, Error: %+v",
					spec.ScParams.DatastoreURL, datacenter.InventoryPath, vc.Config.Host, err)
				continue
			}

			if opts.FilterSuspendedDatastores && vsphere.IsVolumeCreationSuspended(ctx, datastoreInfoObj) {
				continue
			}
			datastoreObj = datastoreInfoObj.Datastore
			break
		}
		if datastoreObj == nil {
			// TODO: Need to figure out which fault need to return when datastore is empty.
			// Currently, just return csi.fault.Internal.
			return nil, csifault.CSIInternalFault, logger.LogNewErrorf(log,
				"DatastoreURL: %s specified in the storage class is not found.",
				spec.ScParams.DatastoreURL)
		}
		// Check if DatastoreURL specified in the StorageClass should be shared
		// datastore across all nodes.
		isSharedDatastoreURL := false
		for _, sharedDatastore := range sharedDatastores {
			if strings.TrimSpace(sharedDatastore.Info.Url) == strings.TrimSpace(spec.ScParams.DatastoreURL) {
				isSharedDatastoreURL = true
				break
			}
		}
		if isSharedDatastoreURL {
			datastores = append(datastores, datastoreObj.Reference())
			datastoreInfoList = append(datastoreInfoList, datastoreInfoObj)
		} else {
			// TODO: Need to figure out which fault need to return when datastore is not accessible to all nodes.
			// Currently, just return csi.fault.Internal.
			return nil, csifault.CSIInternalFault, logger.LogNewErrorf(log,
				"Datastore: %s specified in the storage class "+
					"is not accessible to all nodes.",
				spec.ScParams.DatastoreURL)
		}
	}

	if !opts.IsVdppOnStretchedSvFssEnabled || spec.VsanDatastoreURL == "" {
		fault, err := isDataStoreCompatible(ctx, vc, spec, datastores, datastoreObj)
		if err != nil {
			return nil, fault, err
		}
	}

	var containerClusterArray []cnstypes.CnsContainerCluster
	clusterID := manager.CnsConfig.Global.ClusterID
	if opts.UseSupervisorId {
		clusterID = manager.CnsConfig.Global.SupervisorID
	}
	containerCluster := vsphere.GetContainerCluster(clusterID,
		manager.CnsConfig.VirtualCenter[vc.Config.Host].User, clusterFlavor,
		manager.CnsConfig.Global.ClusterDistribution)
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

	if opts.IsCSITransactionSupportEnabled {
		volumeUID, err := ExtractVolumeIDFromPVName(ctx, spec.Name)
		if err != nil {
			return nil, csifault.CSIInternalFault, err
		}
		createSpec.VolumeId = &cnstypes.CnsVolumeId{Id: volumeUID}
	}

	if spec.StoragePolicyID != "" {
		profileSpec := &vim25types.VirtualMachineDefinedProfileSpec{
			ProfileId: spec.StoragePolicyID,
		}
		if spec.AffineToHost != "" {
			hostVsanUUID, err := getHostVsanUUID(ctx, spec.AffineToHost, vc)
			if err != nil {
				log.Errorf("failed to get the vSAN UUID for node: %s", spec.AffineToHost)
				// TODO: Need to figure out which fault need to return when it cannot gethostVsanUUID.
				// Currently, just return csi.fault.Internal.
				return nil, csifault.CSIInternalFault, err
			}
			param1 := vim25types.KeyValue{Key: VsanAffinityKey, Value: hostVsanUUID}
			param2 := vim25types.KeyValue{Key: VsanAffinityMandatory, Value: "1"}
			param3 := vim25types.KeyValue{Key: VsanMigrateForDecom, Value: "1"}
			profileSpec.ProfileParams = append(profileSpec.ProfileParams, param1, param2, param3)
		}
		createSpec.Profile = append(createSpec.Profile, profileSpec)
	}

	var snapshotVolumeCryptoKeyID *vim25types.CryptoKeyId

	// Handle the case of CreateVolumeFromSnapshot by checking if
	// the ContentSourceSnapshotID is available in CreateVolumeSpec
	if spec.ContentSourceSnapshotID != "" {
		// Parse spec.ContentSourceSnapshotID into CNS VolumeID and CNS SnapshotID using "+" as the delimiter
		cnsVolumeID, cnsSnapshotID, err := ParseCSISnapshotID(spec.ContentSourceSnapshotID)
		if err != nil {
			return nil, csifault.CSIInvalidArgumentFault, err
		}

		createSpec.VolumeSource = &cnstypes.CnsSnapshotVolumeSource{
			VolumeId: cnstypes.CnsVolumeId{
				Id: cnsVolumeID,
			},
			SnapshotId: cnstypes.CnsSnapshotId{
				Id: cnsSnapshotID,
			},
		}

		// select the compatible datastore for the case of create volume from snapshot
		// step 1: query the datastore of snapshot. By design, snapshot is always located at the same datastore
		// as the source volume
		querySelection := cnstypes.CnsQuerySelection{
			Names: []string{string(cnstypes.QuerySelectionNameTypeDataStoreUrl)},
		}
		cnsVolume, err := QueryVolumeByID(ctx, manager.VolumeManager, cnsVolumeID, &querySelection)
		if err != nil {
			return nil, csifault.CSIInternalFault, logger.LogNewErrorf(log,
				"failed to query datastore for the snapshot %s with error %+v",
				spec.ContentSourceSnapshotID, err)
		}

		// step 2: validate if the snapshot datastore is compatible with datastore candidates in create spec
		var compatibleDatastore vim25types.ManagedObjectReference
		var foundCompatibleDatastore bool = false
		for _, dsInfo := range datastoreInfoList {
			if dsInfo.Info.Url == cnsVolume.DatastoreUrl {
				log.Infof("compatible datastore found, dsURL = %q, dsRef = %v", dsInfo.Info.Url,
					dsInfo.Datastore.Reference())
				compatibleDatastore = dsInfo.Datastore.Reference()
				foundCompatibleDatastore = true
				break
			}
		}
		if !foundCompatibleDatastore {
			return nil, csifault.CSIInternalFault, logger.LogNewErrorf(log,
				"failed to get the compatible datastore for create volume from snapshot %s with error: %+v",
				spec.ContentSourceSnapshotID, err)
		}

		// overwrite the datatstores field in create spec with the compatible datastore
		log.Infof("Overwrite the datatstores field in create spec %v with the compatible datastore %v "+
			"when create volume from snapshot %s", createSpec.Datastores, compatibleDatastore,
			spec.ContentSourceSnapshotID)
		createSpec.Datastores = []vim25types.ManagedObjectReference{compatibleDatastore}

		if opts.IsByokEnabled {
			// Retrieve the encryption key ID from the source volume
			snapshotVolumeCryptoKeyID, err = QueryVolumeCryptoKeyByID(ctx, manager.VolumeManager, cnsVolumeID)
			if err != nil {
				return nil, csifault.CSIInternalFault, logger.LogNewErrorf(log,
					"failed to query volume crypto key for the snapshot %s with error %+v",
					spec.ContentSourceSnapshotID, err)
			}
		}
	}

	if opts.IsByokEnabled {
		// Build crypto spec for the new volume.
		var cryptoKeyID *vim25types.CryptoKeyId
		if spec.CryptoKeyID != nil {
			cryptoKeyID = &vim25types.CryptoKeyId{
				KeyId:      spec.CryptoKeyID.KeyID,
				ProviderId: &vim25types.KeyProviderId{Id: spec.CryptoKeyID.KeyProvider},
			}
		}

		cryptoSpec := createCryptoSpec(snapshotVolumeCryptoKeyID, cryptoKeyID)
		if cryptoSpec != nil {
			createSpec.CreateSpec = &cnstypes.CnsBlockCreateSpec{CryptoSpec: cryptoSpec}
		}
	}

	log.Debugf("vSphere CSI driver creating volume %s with create spec %+v", spec.Name, spew.Sdump(createSpec))
	volumeInfo, faultType, err := manager.VolumeManager.CreateVolume(ctx, createSpec, extraParams)
	if err != nil {
		log.Errorf("failed to create disk %s with error %+v faultType %q", spec.Name, err, faultType)
		return nil, faultType, err
	}
	return volumeInfo, "", nil
}

// CreateBlockVolumeUtilForMultiVC is the helper function to create CNS block volume when multi-VC FSS is enabled.
func CreateBlockVolumeUtilForMultiVC(ctx context.Context, reqParams interface{}, opts CreateBlockVolumeOptions) (
	*cnsvolume.CnsVolumeInfo, string, error) {
	log := logger.GetLogger(ctx)
	params, ok := reqParams.(VanillaCreateBlockVolParamsForMultiVC)
	if !ok {
		return nil, csifault.CSIInternalFault, logger.LogNewErrorf(log,
			"expected params of type VanillaCreateBlockVolParamsForMultiVC, got %+v", reqParams)
	}
	var (
		err                  error
		datastoreInfoObjList []*vsphere.DatastoreInfo
		datastores           []vim25types.ManagedObjectReference
	)
	params.SharedDatastores, err = vsphere.FilterSuspendedDatastores(ctx, params.SharedDatastores)
	if err != nil {
		return nil, csifault.CSIInternalFault, logger.LogNewErrorf(log,
			"received error while filtering suspended datastores in vCenter %q. Error: %+v",
			params.Vcenter.Config.Host, err)
	}
	if params.Spec.ScParams.DatastoreURL != "" {
		// Check if DatastoreURL specified in the StorageClass is present in shared
		// datastore across all nodes.
		isSharedDatastoreURL := false
		for _, sharedDatastore := range params.SharedDatastores {
			if strings.TrimSpace(sharedDatastore.Info.Url) == strings.TrimSpace(params.Spec.ScParams.DatastoreURL) {
				isSharedDatastoreURL = true
				break
			}
		}
		if !isSharedDatastoreURL {
			// TODO: Need to figure out which fault need to return when datastore is not accessible to all nodes.
			// Currently, just return csi.fault.Internal.
			return nil, csifault.CSIInternalFault, logger.LogNewErrorf(log,
				"Datastore: %s specified in the storage class is not accessible to all nodes in vCenter %q.",
				params.Spec.ScParams.DatastoreURL, params.Vcenter.Config.Host)
		}
		// Check if DatastoreURL specified in the StorageClass is present in any one of the datacenters.
		datastoreInfoObjList, err = getDatastoreInfoObjList(ctx, params.Vcenter, params.Spec.ScParams.DatastoreURL)
		if err != nil {
			// TODO: Need to figure out which fault need to return when datastore cannot be found in given vCenter.
			// Currently, just return csi.fault.Internal.
			return nil, csifault.CSIInternalFault, logger.LogNewErrorf(log, "failed to get datastore "+
				"object in vCenter %q for datastoreURL: %s specified in the storage class.",
				params.Vcenter.Config.Host, params.Spec.ScParams.DatastoreURL)
		}
		params.SharedDatastores = datastoreInfoObjList
	}
	// Fetch datastore morefs for Create Spec.
	for _, ds := range params.SharedDatastores {
		datastores = append(datastores, ds.Reference())
	}

	var containerClusterArray []cnstypes.CnsContainerCluster
	clusterID := params.CNSConfig.Global.ClusterID
	containerCluster := vsphere.GetContainerCluster(clusterID,
		params.CNSConfig.VirtualCenter[params.Vcenter.Config.Host].User, params.ClusterFlavor,
		params.CNSConfig.Global.ClusterDistribution)
	containerClusterArray = append(containerClusterArray, containerCluster)
	createSpec := &cnstypes.CnsVolumeCreateSpec{
		Name:       params.Spec.Name,
		VolumeType: params.Spec.VolumeType,
		Datastores: datastores,
		BackingObjectDetails: &cnstypes.CnsBlockBackingDetails{
			CnsBackingObjectDetails: cnstypes.CnsBackingObjectDetails{
				CapacityInMb: params.Spec.CapacityMB,
			},
		},
		Metadata: cnstypes.CnsVolumeMetadata{
			ContainerCluster:      containerCluster,
			ContainerClusterArray: containerClusterArray,
		},
	}

	if opts.IsCSITransactionSupportEnabled {
		log.Debugf("Creating volume with Transaction Support")
		volumeUID, err := ExtractVolumeIDFromPVName(ctx, createSpec.Name)
		if err != nil {
			return nil, csifault.CSIInternalFault, err
		}
		createSpec.VolumeId = &cnstypes.CnsVolumeId{Id: volumeUID}
	} else {
		log.Debugf("Creating volume without Transaction Support")
	}

	if params.StoragePolicyID != "" {
		profileSpec := &vim25types.VirtualMachineDefinedProfileSpec{
			ProfileId: params.StoragePolicyID,
		}
		createSpec.Profile = append(createSpec.Profile, profileSpec)
	}
	// Handle the case of CreateVolumeFromSnapshot by checking if
	// the ContentSourceSnapshotID is available in CreateVolumeSpec.
	if params.Spec.ContentSourceSnapshotID != "" {
		// Parse spec.ContentSourceSnapshotID into CNS VolumeID and CNS SnapshotID using "+" as the delimiter
		cnsVolumeID, cnsSnapshotID, err := ParseCSISnapshotID(params.Spec.ContentSourceSnapshotID)
		if err != nil {
			return nil, csifault.CSIInvalidArgumentFault, err
		}

		createSpec.VolumeSource = &cnstypes.CnsSnapshotVolumeSource{
			VolumeId: cnstypes.CnsVolumeId{
				Id: cnsVolumeID,
			},
			SnapshotId: cnstypes.CnsSnapshotId{
				Id: cnsSnapshotID,
			},
		}
		// Validate if the snapshot datastore is present in the datastore candidates in create spec.
		isSharedDatastoreURL := false
		for _, sharedDs := range params.SharedDatastores {
			if strings.TrimSpace(sharedDs.Info.Url) == strings.TrimSpace(params.SnapshotDatastoreURL) {
				isSharedDatastoreURL = true
				break
			}
		}
		if !isSharedDatastoreURL {
			return nil, csifault.CSIInternalFault, logger.LogNewErrorf(log,
				"failed to get the compatible shared datastore for create volume from snapshot %q in vCenter %q",
				params.Spec.ContentSourceSnapshotID, params.Vcenter.Config.Host)
		}
		// Check if DatastoreURL specified in the StorageClass is present in any one of the datacenters.
		datastoreInfoObjList, err = getDatastoreInfoObjList(ctx, params.Vcenter, params.SnapshotDatastoreURL)
		if err != nil {
			// TODO: Need to figure out which fault need to return when datastore cannot be found in given vCenter.
			// Currently, just return csi.fault.Internal.
			return nil, csifault.CSIInternalFault, logger.LogNewErrorf(log, "failed to get datastore "+
				"object in vCenter %q for datastore URL %q associated with snapshot %q",
				params.Vcenter.Config.Host, params.SnapshotDatastoreURL, params.Spec.ContentSourceSnapshotID)
		}

		log.Infof("Overwrite the datastores field in create spec %+v", createSpec.Datastores)
		createSpec.Datastores = nil
		for _, datastoreInfoObj := range datastoreInfoObjList {
			createSpec.Datastores = append(createSpec.Datastores, datastoreInfoObj.Reference())
			// overwrite the datastores field in create spec with the compatible datastores
			log.Infof("add snapshot datastore %v when create volume from snapshot %s", datastoreInfoObj.Reference(),
				params.Spec.ContentSourceSnapshotID)
		}
	}

	log.Debugf("vSphere CSI driver creating volume %s with create spec %+v", params.Spec.Name, spew.Sdump(createSpec))
	volumeInfo, faultType, err := params.VolumeManager.CreateVolume(ctx, createSpec, nil)
	if err != nil {
		log.Errorf("failed to create disk %s on vCenter %q with error %+v faultType %q",
			params.Spec.Name, params.Vcenter.Config.Host, err, faultType)
		if cnsvolume.IsCnsVolumeAlreadyExistsFault(ctx, faultType) {
			log.Infof("Observed volume with Id: %q is already Exists. Deleting Volume.", createSpec.VolumeId.Id)
			_, deleteError := params.VolumeManager.DeleteVolume(ctx, createSpec.VolumeId.Id, true)
			if deleteError != nil {
				log.Errorf("failed to delete volume: %q, err :%v", createSpec.VolumeId.Id, err)
				return nil, faultType, err
			}
			log.Infof("Attempt to re-create volume with Id: %q", createSpec.VolumeId.Id)
			volumeInfo, faultType, err := params.VolumeManager.CreateVolume(ctx, createSpec, nil)
			if err != nil {
				log.Errorf("failed to re-create disk %s on vCenter %q with error %+v faultType %q",
					params.Spec.Name, params.Vcenter.Config.Host, err, faultType)
				return nil, faultType, err
			} else {
				return volumeInfo, "", nil
			}
		}
		return nil, faultType, err
	}
	return volumeInfo, "", nil
}

// CreateFileVolumeUtil is the helper function to create CNS file volume with
// datastores.
func CreateFileVolumeUtil(ctx context.Context, clusterFlavor cnstypes.CnsClusterFlavor,
	vc *vsphere.VirtualCenter, volumeManager cnsvolume.Manager, cnsConfig *config.Config, spec *CreateVolumeSpec,
	datastores []*vsphere.DatastoreInfo, filterSuspendedDatastores, useSupervisorId bool, extraParams interface{}) (
	*cnsvolume.CnsVolumeInfo, string, error) {
	log := logger.GetLogger(ctx)
	var err error
	if spec.ScParams.StoragePolicyName != "" {
		// Get Storage Policy ID from Storage Policy Name.
		spec.StoragePolicyID, err = vc.GetStoragePolicyIDByName(ctx, spec.ScParams.StoragePolicyName)
		if err != nil {
			log.Errorf("Error occurred while getting Profile Id from Profile Name: %q, err: %+v",
				spec.ScParams.StoragePolicyName, err)
			// TODO: need to extract fault from err returned by GetStoragePolicyIDByName.
			// Currently, just return csi.fault.Internal.
			return nil, csifault.CSIInternalFault, err
		}
	}

	if filterSuspendedDatastores {
		datastores, err = vsphere.FilterSuspendedDatastores(ctx, datastores)
		if err != nil {
			log.Errorf("Error occurred while filter suspended datastores, err: %+v", err)
			return nil, csifault.CSIInternalFault, err
		}
	}

	var datastoreMorefs []vim25types.ManagedObjectReference
	if spec.ScParams.DatastoreURL == "" {
		datastoreMorefs = getDatastoreMoRefs(datastores)
	} else {
		// If datastoreUrl is set in storage class, then check if part of input
		// datastores. If true, create the file volume on the datastoreUrl set
		// in storage class.
		var isFound bool
		for _, dsInfo := range datastores {
			if spec.ScParams.DatastoreURL == dsInfo.Info.Url {
				isFound = true
				datastoreMorefs = append(datastoreMorefs, dsInfo.Reference())
				break
			}
		}
		if !isFound {
			// TODO: Need to figure out which fault need to be returned when datastoreURL is not specified in
			// storage class. Currently, just return csi.fault.Internal.
			return nil, csifault.CSIInternalFault, logger.LogNewErrorf(log,
				"datastore %q not found in candidate list for volume provisioning.",
				spec.ScParams.DatastoreURL)
		}
	}

	dataStoreList := []vim25types.ManagedObjectReference{}
	for _, ds := range datastores {
		dataStoreList = append(dataStoreList, ds.Reference())
	}
	fault, err := isDataStoreCompatible(ctx, vc, spec, dataStoreList, nil)
	if err != nil {
		return nil, fault, err
	}

	// Retrieve net permissions from CnsConfig of manager and convert to required
	// format.
	netPerms := make([]vsanfstypes.VsanFileShareNetPermission, 0)
	for _, netPerm := range cnsConfig.NetPermissions {
		netPerms = append(netPerms, vsanfstypes.VsanFileShareNetPermission{
			Ips:         netPerm.Ips,
			Permissions: netPerm.Permissions,
			AllowRoot:   !netPerm.RootSquash,
		})
	}

	clusterID := cnsConfig.Global.ClusterID
	if useSupervisorId {
		clusterID = cnsConfig.Global.SupervisorID
	}
	var containerClusterArray []cnstypes.CnsContainerCluster
	containerCluster := vsphere.GetContainerCluster(clusterID,
		cnsConfig.VirtualCenter[vc.Config.Host].User, clusterFlavor,
		cnsConfig.Global.ClusterDistribution)
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
	volumeInfo, faultType, err := volumeManager.CreateVolume(ctx, createSpec, extraParams)
	if err != nil {
		log.Errorf("failed to create file volume %q with error %+v faultType %q", spec.Name, err, faultType)
		return nil, faultType, err
	}
	return volumeInfo, "", nil
}

// getHostVsanUUID returns the config.clusterInfo.nodeUuid of the ESX host's
// HostVsanSystem.
func getHostVsanUUID(ctx context.Context, hostMoID string, vc *vsphere.VirtualCenter) (string, error) {
	log := logger.GetLogger(ctx)
	log.Debugf("getHostVsanUUID for host moid: %v", hostMoID)

	// Get host vsan UUID from the HostSystem.
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

// AttachVolumeUtil is the helper function to attach CNS volume to specified vm.
func AttachVolumeUtil(ctx context.Context, volumeManager cnsvolume.Manager,
	vm *vsphere.VirtualMachine,
	volumeID string, checkNVMeController bool) (string, string, error) {
	log := logger.GetLogger(ctx)
	log.Debugf("vSphere CSI driver is attaching volume: %q to vm: %q", volumeID, vm.String())
	diskUUID, faultType, err := volumeManager.AttachVolume(ctx, vm, volumeID, checkNVMeController)
	if err != nil {
		log.Errorf("failed to attach disk %q with VM: %q. err: %+v faultType %q", volumeID, vm.String(), err, faultType)
		return "", faultType, err
	}
	log.Debugf("Successfully attached disk %s to VM %v. Disk UUID is %s", volumeID, vm, diskUUID)
	return diskUUID, "", err
}

// DetachVolumeUtil is the helper function to detach CNS volume from specified
// vm.
func DetachVolumeUtil(ctx context.Context, volumeManager cnsvolume.Manager,
	vm *vsphere.VirtualMachine,
	volumeID string) (string, error) {
	log := logger.GetLogger(ctx)
	log.Debugf("vSphere CSI driver is detaching volume: %s from node vm: %s", volumeID, vm.InventoryPath)
	faultType, err := volumeManager.DetachVolume(ctx, vm, volumeID)
	if err != nil {
		log.Errorf("failed to detach disk %s with err %+v", volumeID, err)
		return faultType, err
	}
	log.Debugf("Successfully detached disk %s from VM %v.", volumeID, vm)
	return "", nil
}

// DeleteVolumeUtil is the helper function to delete CNS volume for given
// volumeId.
func DeleteVolumeUtil(ctx context.Context, volManager cnsvolume.Manager, volumeID string,
	deleteDisk bool) (string, error) {
	log := logger.GetLogger(ctx)
	var err error
	var faultType string
	log.Debugf("vSphere CSI driver is deleting volume: %s with deleteDisk flag: %t", volumeID, deleteDisk)
	faultType, err = volManager.DeleteVolume(ctx, volumeID, deleteDisk)
	if err != nil {
		log.Errorf("failed to delete disk %s, deleteDisk flag: %t with error %+v", volumeID, deleteDisk, err)
		return faultType, err
	}
	log.Debugf("Successfully deleted disk for volumeid: %s, deleteDisk flag: %t", volumeID, deleteDisk)
	return "", nil
}

// ExpandVolumeUtil is the helper function to extend CNS volume for given
// volumeId.
func ExpandVolumeUtil(ctx context.Context, vCenterManager vsphere.VirtualCenterManager,
	vCenterHost string, volumeManager cnsvolume.Manager, volumeID string, capacityInMb int64,
	useAsyncQueryVolume bool, extraParams interface{}) (string, error) {
	var err error
	log := logger.GetLogger(ctx)
	log.Debugf("vSphere CSI driver expanding volume %q to new size %d Mb.", volumeID, capacityInMb)
	var faultType string
	var isvSphere8AndAbove, expansionRequired bool

	// Checking if vsphere version is 8 and above.
	vc, err := GetVCenterFromVCHost(ctx, vCenterManager, vCenterHost)
	if err != nil {
		log.Errorf("failed to get vcenter. err=%v", err)
		return csifault.CSIInternalFault, err
	}
	isvSphere8AndAbove, err = IsvSphere8AndAbove(ctx, vc.Client.ServiceContent.About)
	if err != nil {
		return "", logger.LogNewErrorf(log,
			"Error while determining whether vSphere version is 8 and above %q. Error= %+v",
			vc.Client.ServiceContent.About.ApiVersion, err)
	}

	if !isvSphere8AndAbove {
		// Query Volume to check Volume Size for vSphere version below 8.0
		expansionRequired, err = isExpansionRequired(ctx, volumeID, capacityInMb,
			volumeManager, useAsyncQueryVolume)
		if err != nil {
			return csifault.CSIInternalFault, err
		}
	} else {
		// Skip Query Volume to check Volume Size if vSphere version is 8.0 and above
		expansionRequired = true
	}
	if expansionRequired {
		faultType, err = volumeManager.ExpandVolume(ctx, volumeID, capacityInMb, extraParams)
		if err != nil {
			log.Errorf("failed to expand volume %q with error %+v", volumeID, err)
			return faultType, err
		}
		log.Infof("Successfully expanded volume for volumeID %q to new size %d Mb.", volumeID, capacityInMb)

	} else {
		log.Infof("Requested volume size is equal to current size %d Mb. Expansion not required.", capacityInMb)
	}
	return "", nil
}

func ListSnapshotsUtil(ctx context.Context, volManager cnsvolume.Manager, volumeID string, snapshotID string,
	token string, maxEntries int64) ([]*csi.Snapshot, string, error) {
	log := logger.GetLogger(ctx)
	if snapshotID != "" {
		// Retrieve specific snapshot information.
		// The snapshotID is of format: <volume-id>+<snapshot-id>
		volID, snapID, err := ParseCSISnapshotID(snapshotID)
		if err != nil {
			log.Errorf("Unable to determine the volume-id and snapshot-id")
			return nil, "", err
		}
		snapshots, err := QueryVolumeSnapshot(ctx, volManager, volID, snapID, maxEntries)
		if err != nil {
			// Failed to retrieve information of snapshot.
			log.Errorf("failed to retrieve snapshot information for volume-id: %s snapshot-id: %s err: %+v", volID, snapID, err)
			return nil, "", err
		}
		return snapshots, "", nil
	} else if volumeID != "" {
		// Retrieve all snapshots for a volume-id
		// Check if the volume-id specified is of Block type.
		queryFilter := cnstypes.CnsQueryFilter{
			VolumeIds: []cnstypes.CnsVolumeId{{Id: volumeID}},
		}
		querySelection := cnstypes.CnsQuerySelection{
			Names: []string{string(cnstypes.QuerySelectionNameTypeVolumeType)},
		}
		// Validate that the volume-id is of block volume type.
		queryResult, err := utils.QueryVolumeUtil(ctx, volManager, queryFilter, &querySelection, true)
		if err != nil {
			return nil, "", logger.LogNewErrorCodef(log, codes.Internal,
				"queryVolumeUtil failed with err=%+v", err)
		}

		if len(queryResult.Volumes) == 0 {
			return nil, "", logger.LogNewErrorCodef(log, codes.Internal,
				"volumeID %q not found in QueryVolumeUtil", volumeID)
		}
		if queryResult.Volumes[0].VolumeType == FileVolumeType {
			return nil, "", logger.LogNewErrorCodef(log, codes.Unimplemented,
				"ListSnapshot for file volume: %q not supported", volumeID)
		}
		return QueryVolumeSnapshotsByVolumeID(ctx, volManager, volumeID, maxEntries)
	} else {
		// Retrieve all snapshots in the inventory
		return QueryAllVolumeSnapshots(ctx, volManager, token, maxEntries)
	}
}

func QueryVolumeSnapshot(ctx context.Context, volManager cnsvolume.Manager, volID string, snapID string,
	maxEntries int64) ([]*csi.Snapshot, error) {
	log := logger.GetLogger(ctx)
	var snapshots []*csi.Snapshot
	// Retrieve the individual snapshot information using CNS QuerySnapshots
	querySpec := cnstypes.CnsSnapshotQuerySpec{
		VolumeId: cnstypes.CnsVolumeId{
			Id: volID,
		},
		SnapshotId: &cnstypes.CnsSnapshotId{
			Id: snapID,
		},
	}
	querySnapFilter := cnstypes.CnsSnapshotQueryFilter{
		SnapshotQuerySpecs: []cnstypes.CnsSnapshotQuerySpec{querySpec},
		Cursor: &cnstypes.CnsCursor{
			Offset: 0,
			Limit:  maxEntries,
		},
	}
	queryResultEntries, _, err := utils.QuerySnapshotsUtil(ctx, volManager, querySnapFilter, QuerySnapshotLimit)
	if err != nil {
		return nil, logger.LogNewErrorCodef(log, codes.Internal,
			"failed retrieve snapshot info for volume-id: %s snapshot-id: %s err: %+v", volID, snapID, err)
	}
	if len(queryResultEntries) == 0 {
		return nil, logger.LogNewErrorCodef(log, codes.Internal,
			"no snapshot infos retrieved for volume-id: %s snapshot-id: %s from QuerySnapshot", volID, snapID)
	}
	snapshotResult := queryResultEntries[0]
	if snapshotResult.Error != nil {
		fault := snapshotResult.Error.Fault
		if _, ok := fault.(*cnstypes.CnsSnapshotNotFoundFault); ok {
			return nil, logger.LogNewErrorCodef(log, codes.Internal,
				"snapshot-id: %s not found for volume-id: %s during QuerySnapshots, err: %+v", snapID, volID, fault)
		}
		return nil, logger.LogNewErrorCodef(log, codes.Internal,
			"unexpected error received when retrieving snapshot info for volume-id: %s snapshot-id: %s err: %+v",
			volID, snapID, snapshotResult.Error.Fault)
	}
	//Retrieve the volume size to be returned as snapshot size.
	// TODO: Retrieve Snapshot size directly from CnsQuerySnapshot once supported.
	// Query capacity in MB, volume type and datastore url for block volume snapshot
	volumeIds := []cnstypes.CnsVolumeId{{Id: volID}}
	cnsVolumeDetailsMap, err := utils.QueryVolumeDetailsUtil(ctx, volManager, volumeIds)
	if err != nil {
		return nil, err
	}
	if _, ok := cnsVolumeDetailsMap[volID]; !ok {
		return nil, logger.LogNewErrorCodef(log, codes.Internal,
			"cns query volume did not retrieve the volume: %s", volID)
	}
	snapshotSizeInMB := cnsVolumeDetailsMap[volID].SizeInMB
	snapshotsInfo := snapshotResult.Snapshot
	snapshotCreateTimeInProto := timestamppb.New(snapshotsInfo.CreateTime)
	csiSnapshotInfo := &csi.Snapshot{
		SnapshotId:     volID + VSphereCSISnapshotIdDelimiter + snapID,
		SourceVolumeId: volID,
		CreationTime:   snapshotCreateTimeInProto,
		SizeBytes:      snapshotSizeInMB * MbInBytes,
		ReadyToUse:     true,
	}
	snapshots = append(snapshots, csiSnapshotInfo)
	return snapshots, nil
}

func QueryAllVolumeSnapshots(ctx context.Context, volManager cnsvolume.Manager, token string, maxEntries int64) (
	[]*csi.Snapshot, string, error) {
	log := logger.GetLogger(ctx)
	var csiSnapshots []*csi.Snapshot
	var offset int64
	var err error
	limit := QuerySnapshotLimit
	if maxEntries <= QuerySnapshotLimit {
		// If the max results that can be handled by callers is lower than the default limit, then
		// serve the results in a single call.
		limit = maxEntries
	}
	if token != "" {
		offset, err = strconv.ParseInt(token, 10, 64)
		if err != nil {
			log.Errorf("failed to parse the token: %s err: %v", token, err)
			return nil, "", err
		}
	}
	queryFilter := cnstypes.CnsSnapshotQueryFilter{
		Cursor: &cnstypes.CnsCursor{
			Offset: offset,
			Limit:  limit,
		},
	}
	queryResultEntries, nextToken, err := utils.QuerySnapshotsUtil(ctx, volManager, queryFilter, maxEntries)
	if err != nil {
		log.Errorf("failed to retrieve all the volume snapshots in inventory err: %+v", err)
		return nil, "", err
	}
	//populate list of volume-ids to retrieve the volume size.
	var volumeIds []cnstypes.CnsVolumeId
	for _, queryResult := range queryResultEntries {
		if queryResult.Error != nil {
			return nil, "", logger.LogNewErrorCodef(log, codes.Internal,
				"faults are not expected when invoking QuerySnapshots without volume-id and snapshot-id, fault: %+v",
				queryResult.Error.Fault)
		}
		volumeIds = append(volumeIds, queryResult.Snapshot.VolumeId)
	}
	// TODO: Retrieve Snapshot size directly from CnsQuerySnapshot once supported.
	cnsVolumeDetailsMap, err := utils.QueryVolumeDetailsUtil(ctx, volManager, volumeIds)
	if err != nil {
		log.Errorf("failed to retrieve volume details for volume-ids: %v, err: %+v", volumeIds, err)
		return nil, "", err
	}
	for _, queryResult := range queryResultEntries {
		snapshotCreateTimeInProto := timestamppb.New(queryResult.Snapshot.CreateTime)
		csiSnapshotId := queryResult.Snapshot.VolumeId.Id + VSphereCSISnapshotIdDelimiter + queryResult.Snapshot.SnapshotId.Id
		if _, ok := cnsVolumeDetailsMap[queryResult.Snapshot.VolumeId.Id]; !ok {
			return nil, "", logger.LogNewErrorCodef(log, codes.Internal,
				"cns query volume did not return the volume: %s", queryResult.Snapshot.VolumeId.Id)
		}
		csiSnapshotSize := cnsVolumeDetailsMap[queryResult.Snapshot.VolumeId.Id].SizeInMB * MbInBytes
		csiSnapshotInfo := &csi.Snapshot{
			SnapshotId:     csiSnapshotId,
			SourceVolumeId: queryResult.Snapshot.VolumeId.Id,
			CreationTime:   snapshotCreateTimeInProto,
			SizeBytes:      csiSnapshotSize,
			ReadyToUse:     true,
		}
		csiSnapshots = append(csiSnapshots, csiSnapshotInfo)
	}
	return csiSnapshots, nextToken, nil
}

func QueryVolumeSnapshotsByVolumeID(ctx context.Context, volManager cnsvolume.Manager, volumeID string,
	maxEntries int64) ([]*csi.Snapshot, string, error) {
	log := logger.GetLogger(ctx)
	var csiSnapshots []*csi.Snapshot
	limit := QuerySnapshotLimit
	if maxEntries <= QuerySnapshotLimit {
		limit = maxEntries
	}
	querySpec := cnstypes.CnsSnapshotQuerySpec{
		VolumeId: cnstypes.CnsVolumeId{
			Id: volumeID,
		},
	}
	queryFilter := cnstypes.CnsSnapshotQueryFilter{
		SnapshotQuerySpecs: []cnstypes.CnsSnapshotQuerySpec{querySpec},
		Cursor: &cnstypes.CnsCursor{
			Offset: 0,
			Limit:  limit,
		},
	}
	queryResultEntries, nextToken, err := utils.QuerySnapshotsUtil(ctx, volManager, queryFilter, maxEntries)
	if err != nil {
		log.Errorf("failed to retrieve csiSnapshots for volume-id: %s err: %+v", volumeID, err)
		return nil, "", err
	}
	// Retrieve the volume size as an approximation for snapshot size.
	// TODO: Retrieve Snapshot size directly from CnsQuerySnapshot once supported.
	// Query capacity in MB and datastore url for block volume snapshot
	volumeIds := []cnstypes.CnsVolumeId{{Id: volumeID}}
	cnsVolumeDetailsMap, err := utils.QueryVolumeDetailsUtil(ctx, volManager, volumeIds)
	if err != nil {
		log.Errorf("failed to retrieve the volume: %s details. err: %+v.", volumeID, err)
		return nil, "", err
	}
	for _, queryResult := range queryResultEntries {
		// check if the queryResult has fault, if so, the specific result can be ignored.
		if queryResult.Error != nil {
			// Currently, CnsVolumeNotFoundFault is the only possible fault when QuerySnapshots is
			// invoked with only volume-id
			fault := queryResult.Error.Fault
			if faultInfo, ok := fault.(*cnstypes.CnsVolumeNotFoundFault); ok {
				faultVolumeId := faultInfo.VolumeId.Id
				log.Warnf("volume %s was not found during QuerySnapshots, ignore volume..", faultVolumeId)
				continue
			} else {
				return csiSnapshots, nextToken, logger.LogNewErrorCodef(log, codes.Internal,
					"unexpected fault %+v received in QuerySnapshots result: %+v for volume: %s", fault, queryResult, volumeID)
			}
		}
		snapshotCreateTimeInProto := timestamppb.New(queryResult.Snapshot.CreateTime)
		csiSnapshotId := queryResult.Snapshot.VolumeId.Id + VSphereCSISnapshotIdDelimiter + queryResult.Snapshot.SnapshotId.Id
		if _, ok := cnsVolumeDetailsMap[queryResult.Snapshot.VolumeId.Id]; !ok {
			return nil, "", logger.LogNewErrorCodef(log, codes.Internal,
				"cns query volume did not return the volume: %s", queryResult.Snapshot.VolumeId.Id)
		}
		csiSnapshotSize := cnsVolumeDetailsMap[queryResult.Snapshot.VolumeId.Id].SizeInMB * MbInBytes
		csiSnapshotInfo := &csi.Snapshot{
			SnapshotId:     csiSnapshotId,
			SourceVolumeId: queryResult.Snapshot.VolumeId.Id,
			CreationTime:   snapshotCreateTimeInProto,
			SizeBytes:      csiSnapshotSize,
			ReadyToUse:     true,
		}
		csiSnapshots = append(csiSnapshots, csiSnapshotInfo)
	}
	return csiSnapshots, nextToken, nil
}

// QueryVolumeByID is the helper function to query volume by volumeID.
func QueryVolumeByID(ctx context.Context, volManager cnsvolume.Manager, volumeID string,
	querySelection *cnstypes.CnsQuerySelection) (*cnstypes.CnsVolume, error) {
	log := logger.GetLogger(ctx)
	queryFilter := cnstypes.CnsQueryFilter{
		VolumeIds: []cnstypes.CnsVolumeId{{Id: volumeID}},
	}
	queryResult, err := utils.QueryVolumeUtil(ctx, volManager, queryFilter, querySelection, true)
	if err != nil {
		log.Errorf("QueryVolumeUtil failed for volumeID: %s with error %+v", volumeID, err)
		return nil, err
	}
	if len(queryResult.Volumes) == 0 {
		log.Error("volumeID %q not found in QueryVolumeUtil", volumeID)
		return nil, ErrNotFound
	}
	return &queryResult.Volumes[0], nil
}

// Helper function to get DatastoreMoRefs.
func getDatastoreMoRefs(datastores []*vsphere.DatastoreInfo) []vim25types.ManagedObjectReference {
	var datastoreMoRefs []vim25types.ManagedObjectReference
	for _, datastore := range datastores {
		datastoreMoRefs = append(datastoreMoRefs, datastore.Reference())
	}
	return datastoreMoRefs
}

// Helper function to get DatastoreInfo object for given datastoreURL in the given
// virtual center.
func getDatastoreInfoObjList(ctx context.Context, vc *vsphere.VirtualCenter,
	datastoreURL string) ([]*vsphere.DatastoreInfo, error) {
	log := logger.GetLogger(ctx)
	var datastoreInfos []*vsphere.DatastoreInfo
	datacenters, err := vc.GetDatacenters(ctx)
	if err != nil {
		return nil, err
	}
	var datastoreInfoObj *vsphere.DatastoreInfo
	for _, datacenter := range datacenters {
		datastoreInfoObj, err = datacenter.GetDatastoreInfoByURL(ctx, datastoreURL)
		if err != nil {
			log.Warnf("failed to find datastore with URL %q in datacenter %q from VC %q. Error: %+v",
				datastoreURL, datacenter.InventoryPath, vc.Config.Host, err)
		} else {
			datastoreInfos = append(datastoreInfos, datastoreInfoObj)
		}
	}
	if len(datastoreInfos) > 0 {
		return datastoreInfos, nil
	} else {
		return nil, logger.LogNewErrorf(log,
			"Unable to find datastore for datastore URL %s in VC %+v", datastoreURL, vc)
	}
}

// isExpansionRequired verifies if the requested size to expand a volume is
// greater than the current size.
func isExpansionRequired(ctx context.Context, volumeID string, requestedSize int64,
	volumeManager cnsvolume.Manager, useAsyncQueryVolume bool) (bool, error) {
	log := logger.GetLogger(ctx)
	volumeIds := []cnstypes.CnsVolumeId{{Id: volumeID}}
	queryFilter := cnstypes.CnsQueryFilter{
		VolumeIds: volumeIds,
	}
	// Select only the backing object details.
	querySelection := cnstypes.CnsQuerySelection{
		Names: []string{
			string(cnstypes.QuerySelectionNameTypeBackingObjectDetails),
		},
	}
	// Query only the backing object details.
	queryResult, err := volumeManager.QueryAllVolume(ctx, queryFilter, querySelection)
	if err != nil {
		log.Errorf("queryVolume failed for volumeID: %q with err=%v", volumeID, err)
		return false, err
	}

	var currentSize int64
	if len(queryResult.Volumes) > 0 {
		currentSize = queryResult.Volumes[0].BackingObjectDetails.GetCnsBackingObjectDetails().CapacityInMb
	} else {
		// Error out as volume is not found during a resize operation.
		return false, logger.LogNewErrorf(log, "failed to find volume by querying volumeID: %q", volumeID)
	}

	log.Infof("isExpansionRequired: Found current size of volumeID %q to be %d Mb. "+
		"Requested size is %d Mb", volumeID, currentSize, requestedSize)
	return currentSize < requestedSize, nil
}

// CreateSnapshotUtil is the helper function to create CNS snapshot for given volumeId
//
// The desc parameter denotes the snapshot description required by CNS CreateSnapshot API. This parameter is expected
// to be filled with the CSI CreateSnapshotRequest Name, which is generated by the CSI snapshotter sidecar.
//
// The returned string is a combination of CNS VolumeID and CNS SnapshotID concatenated by the "+" sign.
// The returned cnsSnapshotInfo include details of snapshot created from the storage system, i.e., CNS.
func CreateSnapshotUtil(ctx context.Context, volumeManager cnsvolume.Manager, volumeID string,
	snapshotName string, extraParams interface{}) (string, *cnsvolume.CnsSnapshotInfo, error) {
	log := logger.GetLogger(ctx)

	log.Debugf("vSphere CSI driver is creating snapshot with description, %q, on volume: %q", snapshotName, volumeID)
	cnsSnapshotInfo, err := volumeManager.CreateSnapshot(ctx, volumeID, snapshotName, extraParams)
	if err != nil {
		log.Errorf("failed to create snapshot on volume %q with description %q with error %+v",
			volumeID, snapshotName, err)
		return "", nil, err
	}
	log.Debugf("Successfully created snapshot %q with description, %q, on volume: %q at timestamp %q",
		cnsSnapshotInfo.SnapshotID, snapshotName, volumeID, cnsSnapshotInfo.SnapshotLatestOperationCompleteTime)

	csiSnapshotID := volumeID + VSphereCSISnapshotIdDelimiter + cnsSnapshotInfo.SnapshotID

	return csiSnapshotID, cnsSnapshotInfo, nil
}

// DeleteSnapshotUtil is the helper function to delete CNS snapshot for given snapshotId
func DeleteSnapshotUtil(ctx context.Context, volumeManager cnsvolume.Manager, csiSnapshotID string,
	extraParams interface{}) (*cnsvolume.CnsSnapshotInfo, error) {
	log := logger.GetLogger(ctx)

	cnsVolumeID, cnsSnapshotID, err := ParseCSISnapshotID(csiSnapshotID)
	if err != nil {
		return nil, err
	}

	log.Debugf("vSphere CSI driver is deleting snapshot %q on volume: %q", cnsSnapshotID, cnsVolumeID)
	cnsSnapshotInfo, err := volumeManager.DeleteSnapshot(ctx, cnsVolumeID, cnsSnapshotID, extraParams)
	if err != nil {
		return nil, logger.LogNewErrorf(log, "failed to delete snapshot %q on volume %q with error %+v",
			cnsSnapshotID, cnsVolumeID, err)
	}
	log.Debugf("Successfully deleted snapshot %q on volume %q", cnsSnapshotID, cnsVolumeID)

	return cnsSnapshotInfo, nil
}

// GetCnsVolumeType is the helper function that determines the volume type based on the volume-id
func GetCnsVolumeType(ctx context.Context, volumeManager cnsvolume.Manager, volumeId string) (string, error) {
	log := logger.GetLogger(ctx)
	var volumeType string
	queryFilter := cnstypes.CnsQueryFilter{
		VolumeIds: []cnstypes.CnsVolumeId{{Id: volumeId}},
	}
	querySelection := cnstypes.CnsQuerySelection{
		Names: []string{
			string(cnstypes.QuerySelectionNameTypeVolumeType),
		},
	}
	// Select only the volume type.
	queryResult, err := volumeManager.QueryAllVolume(ctx, queryFilter, querySelection)
	if err != nil {
		return "", logger.LogNewErrorCodef(log, codes.Internal,
			"queryVolume failed for volumeID: %q with err=%+v", volumeId, err)
	}

	if len(queryResult.Volumes) == 0 {
		log.Infof("volume: %s not found during query while determining CNS volume type", volumeId)
		return "", ErrNotFound
	}
	volumeType = queryResult.Volumes[0].VolumeType
	log.Infof("volume: %s is of type: %s", volumeId, volumeType)
	return volumeType, nil
}

// GetNodeVMsWithAccessToDatastore finds out NodeVMs which have access to the given
// datastore URL by using the moref approach.
func GetNodeVMsWithAccessToDatastore(ctx context.Context, vc *vsphere.VirtualCenter, dsURL string,
	allNodeVMs []*vsphere.VirtualMachine) ([]*object.VirtualMachine, error) {

	log := logger.GetLogger(ctx)
	var accessibleNodes []*object.VirtualMachine
	totalNodes := len(allNodeVMs)

	// Create map of allNodeVM refs to nil value for easy retrieval.
	nodeMap := make(map[vim25types.ManagedObjectReference]struct{})
	for _, vmObj := range allNodeVMs {
		nodeMap[vmObj.Reference()] = struct{}{}
	}

	// Get datastore object.
	dsInfoObjList, err := getDatastoreInfoObjList(ctx, vc, dsURL)
	if err != nil {
		return nil, logger.LogNewErrorf(log, "failed to retrieve datastore object using datastore "+
			"URL %q. Error: %+v", dsURL, err)
	}
	for _, dsInfoObj := range dsInfoObjList {
		// Get datastore host mounts.
		var ds mo.Datastore
		err = dsInfoObj.Properties(ctx, dsInfoObj.Reference(), []string{"host"}, &ds)
		if err != nil {
			return nil, logger.LogNewErrorf(log, "failed to get host mounts from datastore %q. Error: %+v",
				dsURL, err)
		}
		// For each host mount, get the list of VMs.
		for _, host := range ds.Host {
			hostObj := &vsphere.HostSystem{
				HostSystem: object.NewHostSystem(vc.Client.Client, host.Key),
			}
			var hs mo.HostSystem
			err = hostObj.Properties(ctx, hostObj.Reference(), []string{"vm"}, &hs)
			if err != nil {
				return nil, logger.LogNewErrorf(log, "failed to retrieve virtual machines from host %q. Error: %+v",
					hostObj.String(), err)
			}
			// For each VM on the host, check if the VM is a NodeVM participating in the k8s cluster.
			for _, vm := range hs.Vm {
				if _, exists := nodeMap[vm]; exists {
					accessibleNodes = append(accessibleNodes, object.NewVirtualMachine(vc.Client.Client, vm))
				}
				// Check if the length of accessible nodes is equal to total count of
				// NodeVMs in this cluster. If yes, we can stop searching for more VMs.
				if len(accessibleNodes) == totalNodes {
					log.Infof("Nodes that have access to datastore %q are %+v", dsURL, accessibleNodes)
					return accessibleNodes, nil
				}
			}
		}
	}
	log.Infof("Nodes that have access to datastore %q are %+v", dsURL, accessibleNodes)
	return accessibleNodes, nil
}

// isDataStoreCompatible validates if datastore is accessible from all nodes.
func isDataStoreCompatible(ctx context.Context, vc *vsphere.VirtualCenter, spec *CreateVolumeSpec,
	datastores []vim25types.ManagedObjectReference, datastoreObj *vsphere.Datastore) (string, error) {
	log := logger.GetLogger(ctx)
	if spec.StoragePolicyID != "" {
		// Check storage policy compatibility.
		var sharedDSMoRef []vim25types.ManagedObjectReference
		for _, ds := range datastores {
			sharedDSMoRef = append(sharedDSMoRef, ds.Reference())
		}

		compat, err := vc.PbmCheckCompatibility(ctx, sharedDSMoRef, spec.StoragePolicyID)
		if err != nil {
			return csifault.CSIInternalFault, logger.LogNewErrorCodef(log, codes.Internal,
				"failed to find datastore compatibility "+
					"with storage policy ID %q. Error: %+v", spec.StoragePolicyID, err)
		}

		compatibleDsMoids := make(map[string]struct{})
		for _, ds := range compat.CompatibleDatastores() {
			compatibleDsMoids[ds.HubId] = struct{}{}
		}

		compatibleDsAccessible := false
		for _, ds := range datastores {
			if _, exists := compatibleDsMoids[ds.Reference().Value]; exists {
				compatibleDsAccessible = true
				break
			}
		}
		if !compatibleDsAccessible {
			log.Errorf("no compatible datastore for storage policy ID %q is in list of shared datastores %+v",
				spec.StoragePolicyID, datastores)
			return csifault.CSIInvalidStoragePolicyConfigurationFault, logger.LogNewErrorCodef(log, codes.Internal,
				"none of compatible datastores for given storage policy ID %q, is in the "+
					"list of shared datastore accessible to all nodes", spec.StoragePolicyID)
		}
		if datastoreObj != nil {
			if _, exists := compatibleDsMoids[datastoreObj.Reference().Value]; !exists {
				return csifault.CSIInvalidStoragePolicyConfigurationFault, logger.LogNewErrorCodef(log, codes.Internal,
					"Datastore: %s specified in the storage class is "+
						"not accessible to all nodes.", datastoreObj.Reference().Value)
			}
		}
	}
	return "", nil
}

// QueryVolumeCryptoKeyByID retrieves the encryption key ID for a volume.
func QueryVolumeCryptoKeyByID(
	ctx context.Context,
	volumeManager cnsvolume.Manager,
	volumeID string) (*vim25types.CryptoKeyId, error) {

	result, err := volumeManager.QueryVolumeInfo(ctx, []cnstypes.CnsVolumeId{{Id: volumeID}})
	if err != nil {
		return nil, err
	}

	blockVolumeInfo, ok := result.VolumeInfo.(*cnstypes.CnsBlockVolumeInfo)
	if !ok {
		return nil, fmt.Errorf("failed to retrieve CNS volume info")
	}

	storageObj := blockVolumeInfo.VStorageObject

	diskFileBackingInfo, ok := storageObj.Config.Backing.(*vim25types.BaseConfigInfoDiskFileBackingInfo)
	if !ok {
		return nil, fmt.Errorf("failed to retrieve FCD backing info")
	}

	return diskFileBackingInfo.KeyId, nil
}

// createCryptoSpec creates a crypto spec based on the requested encryption operation.
//
// - Encrypt: Source is not encrypted, target is encrypted.
// - Decrypt: Source is encrypted, target is not.
// - NoOp: Both source and target are encrypted with the same key.
// - ShallowRecrypt: Both source and target are encrypted with different keys.
func createCryptoSpec(oldKeyID, newKeyID *vim25types.CryptoKeyId) vim25types.BaseCryptoSpec {
	if oldKeyID == nil && newKeyID == nil {
		return nil
	}

	if oldKeyID == nil {
		return &vim25types.CryptoSpecEncrypt{CryptoKeyId: *newKeyID}
	}

	if newKeyID == nil {
		return &vim25types.CryptoSpecDecrypt{}
	}

	if oldKeyID.KeyId == newKeyID.KeyId &&
		oldKeyID.ProviderId.Id == newKeyID.ProviderId.Id {
		return &vim25types.CryptoSpecNoOp{}
	}

	return &vim25types.CryptoSpecShallowRecrypt{NewKeyId: *newKeyID}
}
