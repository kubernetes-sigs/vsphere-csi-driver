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

package migration

import (
	"context"
	"net/url"
	"reflect"
	"regexp"
	"strings"
	"sync"
	"time"

	"github.com/davecgh/go-spew/spew"
	"github.com/google/uuid"
	cnstypes "github.com/vmware/govmomi/cns/types"
	vim25types "github.com/vmware/govmomi/vim25/types"
	apiextensionsv1beta1 "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions/v1beta1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/tools/cache"
	"sigs.k8s.io/controller-runtime/pkg/client"

	migrationv1alpha1 "sigs.k8s.io/vsphere-csi-driver/v2/pkg/apis/migration/v1alpha1"
	cnsvolume "sigs.k8s.io/vsphere-csi-driver/v2/pkg/common/cns-lib/volume"
	"sigs.k8s.io/vsphere-csi-driver/v2/pkg/common/cns-lib/vsphere"
	cnsconfig "sigs.k8s.io/vsphere-csi-driver/v2/pkg/common/config"
	"sigs.k8s.io/vsphere-csi-driver/v2/pkg/common/utils"
	"sigs.k8s.io/vsphere-csi-driver/v2/pkg/csi/service/common"
	"sigs.k8s.io/vsphere-csi-driver/v2/pkg/csi/service/common/commonco"
	"sigs.k8s.io/vsphere-csi-driver/v2/pkg/csi/service/logger"
	k8s "sigs.k8s.io/vsphere-csi-driver/v2/pkg/kubernetes"
)

// VolumeSpec contains VolumePath and StoragePolicyID, using which Volume can
// be looked up via VolumeMigrationService.
type VolumeSpec struct {
	VolumePath        string
	StoragePolicyName string
}

// VolumeMigrationService exposes interfaces to support VCP to CSI migration.
// It will maintain internal state to map volume path to volume ID and reverse
// mapping.
type VolumeMigrationService interface {
	// GetVolumeID returns VolumeID for a given VolumeSpec.
	// Returns an error if not able to retrieve VolumeID.
	GetVolumeID(ctx context.Context, volumeSpec *VolumeSpec) (string, error)

	// GetVolumePath returns VolumePath for a given VolumeID.
	// Returns an error if not able to retrieve VolumePath.
	GetVolumePath(ctx context.Context, volumeID string) (string, error)

	// DeleteVolumeInfo helps delete mapping of volumePath to VolumeID for
	// specified volumeID.
	DeleteVolumeInfo(ctx context.Context, volumeID string) error
}

// volumeMigration holds migrated volume information and provides functionality
// around it.
type volumeMigration struct {
	// volumePath to volumeId map.
	volumePathToVolumeID sync.Map
	// k8sClient helps operate on CnsVSphereVolumeMigration custom resource.
	k8sClient client.Client
	// volumeManager helps perform Volume Operations.
	volumeManager *cnsvolume.Manager
	// cnsConfig helps retrieve vSphere CSI configuration for RegisterVolume
	// Operation.
	cnsConfig *cnsconfig.Config
}

const (
	// CRDName represent the name of cnsvspherevolumemigrations CRD.
	CRDName = "cnsvspherevolumemigrations.cns.vmware.com"
	// CRDGroupName represent the group of cnsvspherevolumemigrations CRD.
	CRDGroupName = "cns.vmware.com"
	// CRDSingular represent the singular name of cnsvspherevolumemigrations CRD.
	CRDSingular = "cnsvspherevolumemigration"
	// CRDPlural represent the plural name of cnsvspherevolumemigrations CRD.
	CRDPlural = "cnsvspherevolumemigrations"
)

var (
	// volumeMigrationInstance is instance of volumeMigration and implements
	// interface for VolumeMigrationService.
	volumeMigrationInstance *volumeMigration
	// volumeMigrationInstanceLock is used for handling race conditions during
	// read, write on volumeMigrationInstance.
	volumeMigrationInstanceLock = &sync.RWMutex{}
	// deleteVolumeInfoLock is used for handling race conditions during
	// volumeMigration CR deletion.
	deleteVolumeInfoLock = &sync.Mutex{}
)

// GetVolumeMigrationService returns the singleton VolumeMigrationService.
// Starts a cleanup routine to delete stale CRD instances if needed.
func GetVolumeMigrationService(ctx context.Context, volumeManager *cnsvolume.Manager,
	cnsConfig *cnsconfig.Config, runCleanupRoutine bool) (VolumeMigrationService, error) {
	log := logger.GetLogger(ctx)
	volumeMigrationInstanceLock.RLock()
	if volumeMigrationInstance == nil {
		volumeMigrationInstanceLock.RUnlock()
		volumeMigrationInstanceLock.Lock()
		defer volumeMigrationInstanceLock.Unlock()
		if volumeMigrationInstance == nil {
			log.Info("Initializing volume migration service...")
			// This is idempotent if CRD is pre-created then we continue with
			// initialization of volumeMigrationInstance.
			volumeMigrationServiceInitErr := k8s.CreateCustomResourceDefinitionFromSpec(ctx,
				CRDName, CRDSingular, CRDPlural, reflect.TypeOf(migrationv1alpha1.CnsVSphereVolumeMigration{}).Name(),
				migrationv1alpha1.SchemeGroupVersion.Group, migrationv1alpha1.SchemeGroupVersion.Version,
				apiextensionsv1beta1.ClusterScoped)
			if volumeMigrationServiceInitErr != nil {
				log.Errorf("failed to create volume migration CRD. Error: %v", volumeMigrationServiceInitErr)
				return nil, volumeMigrationServiceInitErr
			}
			config, volumeMigrationServiceInitErr := k8s.GetKubeConfig(ctx)
			if volumeMigrationServiceInitErr != nil {
				log.Errorf("failed to get kubeconfig. err: %v", volumeMigrationServiceInitErr)
				return nil, volumeMigrationServiceInitErr
			}
			volumeMigrationInstance = &volumeMigration{
				volumePathToVolumeID: sync.Map{},
				volumeManager:        volumeManager,
				cnsConfig:            cnsConfig,
			}
			volumeMigrationInstance.k8sClient, volumeMigrationServiceInitErr =
				k8s.NewClientForGroup(ctx, config, CRDGroupName)
			if volumeMigrationServiceInitErr != nil {
				volumeMigrationInstance = nil
				log.Errorf("failed to create k8sClient. Err: %v", volumeMigrationServiceInitErr)
				return nil, volumeMigrationServiceInitErr
			}
			go func() {
				log.Debugf("Starting Informer for cnsvspherevolumemigrations")
				informer, err := k8s.GetDynamicInformer(ctx, migrationv1alpha1.SchemeGroupVersion.Group,
					migrationv1alpha1.SchemeGroupVersion.Version, "cnsvspherevolumemigrations",
					metav1.NamespaceNone, config, true)
				if err != nil {
					log.Errorf("failed to create dynamic informer for volume migration CRD. Err: %v", err)
				}
				handlers := cache.ResourceEventHandlerFuncs{
					AddFunc: func(obj interface{}) {
						log.Debugf("received add event for VolumeMigration CR!")
						var volumeMigrationObject migrationv1alpha1.CnsVSphereVolumeMigration
						err := runtime.DefaultUnstructuredConverter.FromUnstructured(
							obj.(*unstructured.Unstructured).Object, &volumeMigrationObject)
						if err != nil {
							log.Errorf("failed to cast object to volumeMigrationObject. err: %v", err)
							return
						}
						volumeMigrationInstance.volumePathToVolumeID.Store(
							volumeMigrationObject.Spec.VolumePath, volumeMigrationObject.Spec.VolumeID)
						log.Debugf("successfully added volumePath: %q, volumeID: %q mapping in the cache",
							volumeMigrationObject.Spec.VolumePath, volumeMigrationObject.Spec.VolumeID)
					},
					DeleteFunc: func(obj interface{}) {
						log.Debugf("received delete event for VolumeMigration CR!")
						var volumeMigrationObject migrationv1alpha1.CnsVSphereVolumeMigration
						err := runtime.DefaultUnstructuredConverter.FromUnstructured(
							obj.(*unstructured.Unstructured).Object, &volumeMigrationObject)
						if err != nil {
							log.Errorf("failed to cast object to volumeMigrationObject. err: %v", err)
							return
						}
						volumeMigrationInstance.volumePathToVolumeID.Delete(volumeMigrationObject.Spec.VolumePath)
						log.Debugf("successfully deleted volumePath: %q, volumeID: %q mapping from cache",
							volumeMigrationObject.Spec.VolumePath, volumeMigrationObject.Spec.VolumeID)
					},
				}
				informer.Informer().AddEventHandler(handlers)
				stopCh := make(chan struct{})
				informer.Informer().Run(stopCh)
			}()
			if runCleanupRoutine {
				go volumeMigrationInstance.cleanupStaleCRDInstances()
			}
			log.Info("volume migration service initialized")
		}
	} else {
		volumeMigrationInstanceLock.RUnlock()
	}
	return volumeMigrationInstance, nil
}

// GetVolumeID returns VolumeID for given VolumeSpec.
// Returns an error if not able to retrieve VolumeID.
func (volumeMigration *volumeMigration) GetVolumeID(ctx context.Context, volumeSpec *VolumeSpec) (string, error) {
	log := logger.GetLogger(ctx)
	info, found := volumeMigration.volumePathToVolumeID.Load(volumeSpec.VolumePath)
	if found {
		log.Debugf("VolumeID: %q found from the cache for VolumePath: %q", info.(string), volumeSpec.VolumePath)
		return info.(string), nil
	}
	// Volume may not be registered.
	log.Infof("Could not retrieve VolumeID from cache for Volume Path: %q. Registering Volume with CNS",
		volumeSpec.VolumePath)
	volumeID, err := volumeMigration.registerVolume(ctx, volumeSpec)
	if err != nil {
		log.Errorf("failed to register volume for volumeSpec: %v, with err: %v", volumeSpec, err)
		return "", err
	}
	log.Infof("Successfully registered volumeSpec: %v with CNS. VolumeID: %v", volumeSpec, volumeID)
	cnsvSphereVolumeMigration := migrationv1alpha1.CnsVSphereVolumeMigration{
		ObjectMeta: metav1.ObjectMeta{Name: volumeID},
		Spec: migrationv1alpha1.CnsVSphereVolumeMigrationSpec{
			VolumePath: volumeSpec.VolumePath,
			VolumeID:   volumeID,
		},
	}
	log.Debugf("Saving cnsvSphereVolumeMigration CR: %v", cnsvSphereVolumeMigration)
	err = volumeMigration.saveVolumeInfo(ctx, &cnsvSphereVolumeMigration)
	if err != nil {
		log.Errorf("failed to save cnsvSphereVolumeMigration CR:%v, err: %v", err)
		return "", err
	}
	return volumeID, nil
}

// GetVolumePath returns VolumePath for given VolumeID.
// Returns an error if not able to retrieve VolumePath.
func (volumeMigration *volumeMigration) GetVolumePath(ctx context.Context, volumeID string) (string, error) {
	log := logger.GetLogger(ctx)
	var volumePath string
	volumeMigration.volumePathToVolumeID.Range(func(key, value interface{}) bool {
		if value.(string) == volumeID {
			volumePath = key.(string)
			log.Infof("Found VolumePath %v for VolumeID: %q in the cache", volumePath, volumeID)
			return false
		}
		return true
	})
	if volumePath != "" {
		return volumePath, nil
	}
	volumeMigrationResource := &migrationv1alpha1.CnsVSphereVolumeMigration{}
	err := volumeMigration.k8sClient.Get(ctx, client.ObjectKey{Name: volumeID}, volumeMigrationResource)
	if err != nil {
		if !apierrors.IsNotFound(err) {
			log.Errorf("error happened while getting CR for volumeMigration for VolumeID: %q, err: %v", volumeID, err)
			return "", err
		}
	} else {
		log.Infof("found volume path: %q for VolumeID: %q", volumeMigrationResource.Spec.VolumePath, volumeID)
		volumeMigration.volumePathToVolumeID.Store(volumeMigrationResource.Spec.VolumePath, volumeID)
		return volumeMigrationResource.Spec.VolumePath, nil
	}
	log.Infof("Could not retrieve mapping of volume path and VolumeID in the cache for VolumeID: %q. "+
		"volume may not be registered", volumeID)
	volumeIds := []cnstypes.CnsVolumeId{{Id: volumeID}}
	var host string
	if volumeMigration.cnsConfig == nil || len(volumeMigration.cnsConfig.VirtualCenter) == 0 {
		return "", logger.LogNewError(log, "could not find vcenter config")
	}
	for key := range volumeMigration.cnsConfig.VirtualCenter {
		host = key
		break
	}
	vCenter, err := vsphere.GetVirtualCenterManager(ctx).GetVirtualCenter(ctx, host)
	if err != nil {
		log.Errorf("failed to get vCenter. err: %v", err)
		return "", err
	}
	var fileBackingInfo *vim25types.BaseConfigInfoDiskFileBackingInfo
	bUseVslmAPIs, err := common.UseVslmAPIs(ctx, vCenter.Client.ServiceContent.About)
	if err != nil {
		return "", logger.LogNewErrorf(log,
			"Error while determining the correct APIs to use for vSphere version %q, Error= %+v",
			vCenter.Client.ServiceContent.About.ApiVersion, err)
	}
	if bUseVslmAPIs {
		log.Infof("Retrieving VStorageObject info using Vslm APIs")
		vStorageObject, err := (*volumeMigration.volumeManager).RetrieveVStorageObject(ctx, volumeID)
		if err != nil {
			return "", logger.LogNewErrorf(log,
				"failed to retrieve VStorageObject for volume id: %q, err: %v", volumeID, err)
		}
		log.Debugf("RetrieveVStorageObject successfully returned fileBackingInfo %v for volumeIDList %v:",
			spew.Sdump(fileBackingInfo), volumeIds)
		fileBackingInfo = vStorageObject.Config.Backing.(*vim25types.BaseConfigInfoDiskFileBackingInfo)
	} else {
		log.Infof("Calling QueryVolumeInfo using: %v", volumeIds)
		queryVolumeInfoResult, err := (*volumeMigration.volumeManager).QueryVolumeInfo(ctx, volumeIds)
		if err != nil {
			log.Errorf("QueryVolumeInfo failed for volumeID: %s, err: %v", volumeID, err)
			return "", err
		}
		log.Debugf("QueryVolumeInfo successfully returned volumeInfo %v for volumeIDList %v:",
			spew.Sdump(queryVolumeInfoResult), volumeIds)
		cnsBlockVolumeInfo := interface{}(queryVolumeInfoResult.VolumeInfo).(*cnstypes.CnsBlockVolumeInfo)
		fileBackingInfo = cnsBlockVolumeInfo.VStorageObject.Config.Backing.(*vim25types.BaseConfigInfoDiskFileBackingInfo)
	}
	log.Infof("Successfully retrieved volume path: %q for VolumeID: %q", fileBackingInfo.FilePath, volumeID)
	cnsvSphereVolumeMigration := migrationv1alpha1.CnsVSphereVolumeMigration{
		ObjectMeta: metav1.ObjectMeta{Name: volumeID},
		Spec: migrationv1alpha1.CnsVSphereVolumeMigrationSpec{
			VolumePath: fileBackingInfo.FilePath,
			VolumeID:   volumeID,
		},
	}
	log.Debugf("Saving cnsvSphereVolumeMigration CR: %v", cnsvSphereVolumeMigration)
	err = volumeMigration.saveVolumeInfo(ctx, &cnsvSphereVolumeMigration)
	if err != nil {
		log.Errorf("failed to save cnsvSphereVolumeMigration CR:%v, err: %v", err)
		return "", err
	}
	return fileBackingInfo.FilePath, nil
}

// saveVolumeInfo helps create CR for given cnsVSphereVolumeMigration. This func
// also update local cache with supplied cnsVSphereVolumeMigration, after
// successful creation of CR
func (volumeMigration *volumeMigration) saveVolumeInfo(ctx context.Context,
	cnsVSphereVolumeMigration *migrationv1alpha1.CnsVSphereVolumeMigration) error {
	log := logger.GetLogger(ctx)
	log.Infof("creating CR for cnsVSphereVolumeMigration: %+v", cnsVSphereVolumeMigration)
	err := volumeMigration.k8sClient.Create(ctx, cnsVSphereVolumeMigration)
	if err != nil {
		if !apierrors.IsAlreadyExists(err) {
			log.Errorf("failed to create CR for cnsVSphereVolumeMigration. Error: %v", err)
			return err
		}
		log.Info("CR already exists")
		return nil
	}
	log.Infof("Successfully created CR for cnsVSphereVolumeMigration: %+v", cnsVSphereVolumeMigration)
	return nil
}

// DeleteVolumeInfo helps delete mapping of volumePath to VolumeID for
// specified volumeID.
func (volumeMigration *volumeMigration) DeleteVolumeInfo(ctx context.Context, volumeID string) error {
	log := logger.GetLogger(ctx)
	deleteVolumeInfoLock.Lock()
	defer deleteVolumeInfoLock.Unlock()
	object := migrationv1alpha1.CnsVSphereVolumeMigration{
		ObjectMeta: metav1.ObjectMeta{
			Name: volumeID,
		},
	}
	err := volumeMigration.k8sClient.Delete(ctx, &object)
	if err != nil {
		if apierrors.IsNotFound(err) {
			log.Infof("volumeMigrationCR is already deleted for volumeID: %q", volumeID)
			return nil
		}
		log.Errorf("failed delete volumeMigration CR for volumeID: %q", volumeID)
		return err
	}
	return nil
}

// registerVolume takes VolumeSpec and helps register Volume with CNS.
// Returns VolumeID for successful registration, otherwise return error.
func (volumeMigration *volumeMigration) registerVolume(ctx context.Context, volumeSpec *VolumeSpec) (string, error) {
	log := logger.GetLogger(ctx)
	uuid, err := uuid.NewUUID()
	if err != nil {
		log.Errorf("failed to generate uuid")
		return "", err
	}
	re := regexp.MustCompile(`\[([^\[\]]*)\]`)
	if !re.MatchString(volumeSpec.VolumePath) {
		return "", logger.LogNewErrorf(log,
			"failed to extract datastore name from in-tree volume path: %q", volumeSpec.VolumePath)
	}
	datastoreFullPath := re.FindAllString(volumeSpec.VolumePath, -1)[0]
	vmdkPath := strings.TrimSpace(strings.Trim(volumeSpec.VolumePath, datastoreFullPath))
	datastoreFullPath = strings.Trim(strings.Trim(datastoreFullPath, "["), "]")
	datastorePathSplit := strings.Split(datastoreFullPath, "/")
	datastoreName := datastorePathSplit[len(datastorePathSplit)-1]
	var datacenters string
	var user string
	var host string
	if volumeMigration.cnsConfig == nil || len(volumeMigration.cnsConfig.VirtualCenter) == 0 {
		return "", logger.LogNewError(log, "could not find vcenter config")
	}
	for key, val := range volumeMigration.cnsConfig.VirtualCenter {
		datacenters = val.Datacenters
		user = val.User
		host = key
		break
	}
	// Get vCenter.
	vCenter, err := vsphere.GetVirtualCenterManager(ctx).GetVirtualCenter(ctx, host)
	if err != nil {
		log.Errorf("failed to get vCenter. err: %v", err)
		return "", err
	}
	datacenterPaths := make([]string, 0)
	if datacenters != "" {
		datacenterPaths = strings.Split(datacenters, ",")
	} else {
		// Get all datacenters from vCenter.
		dcs, err := vCenter.GetDatacenters(ctx)
		if err != nil {
			log.Errorf("failed to get datacenters from vCenter. err: %v", err)
			return "", err
		}
		for _, dc := range dcs {
			datacenterPaths = append(datacenterPaths, dc.InventoryPath)
		}
		log.Debugf("retrieved all datacenters %v from vCenter", datacenterPaths)
	}
	var volumeInfo *cnsvolume.CnsVolumeInfo
	var storagePolicyID string
	if volumeSpec.StoragePolicyName != "" {
		log.Debugf("Obtaining storage policy ID for storage policy name: %q", volumeSpec.StoragePolicyName)
		storagePolicyID, err = vCenter.GetStoragePolicyIDByName(ctx, volumeSpec.StoragePolicyName)
		if err != nil {
			return "", logger.LogNewErrorf(log,
				"Error occurred while getting stroage policy ID from storage policy name: %q, err: %+v",
				volumeSpec.StoragePolicyName, err)
		}
		log.Debugf("Obtained storage policy ID: %q for storage policy name: %q",
			storagePolicyID, volumeSpec.StoragePolicyName)
	}
	var containerClusterArray []cnstypes.CnsContainerCluster
	containerCluster := vsphere.GetContainerCluster(volumeMigration.cnsConfig.Global.ClusterID, user,
		cnstypes.CnsClusterFlavorVanilla, volumeMigration.cnsConfig.Global.ClusterDistribution)
	containerClusterArray = append(containerClusterArray, containerCluster)
	createSpec := &cnstypes.CnsVolumeCreateSpec{
		Name:       uuid.String(),
		VolumeType: common.BlockVolumeType,
		Metadata: cnstypes.CnsVolumeMetadata{
			ContainerCluster:      containerCluster,
			ContainerClusterArray: containerClusterArray,
		},
	}
	if storagePolicyID != "" {
		profileSpec := &vim25types.VirtualMachineDefinedProfileSpec{
			ProfileId: storagePolicyID,
		}
		createSpec.Profile = append(createSpec.Profile, profileSpec)
	}
	for _, datacenter := range datacenterPaths {
		// Check vCenter API Version
		// Format:
		// https://<vc_ip>/folder/<vm_vmdk_path>?dcPath=<datacenter-path>&dsName=<datastoreName>
		backingDiskURLPath := "https://" + host + "/folder/" +
			vmdkPath + "?dcPath=" + url.PathEscape(datacenter) + "&dsName=" + url.PathEscape(datastoreName)
		bUseVslmAPIs, err := common.UseVslmAPIs(ctx, vCenter.Client.ServiceContent.About)
		if err != nil {
			return "", logger.LogNewErrorf(log,
				"Error while determining the correct APIs to use for vSphere version %q, Error= %+v",
				vCenter.Client.ServiceContent.About.ApiVersion, err)
		}
		if bUseVslmAPIs {
			backingObjectID, err := (*volumeMigration.volumeManager).RegisterDisk(ctx,
				backingDiskURLPath, volumeSpec.VolumePath)
			if err != nil {
				return "", logger.LogNewErrorf(log,
					"registration failed for volumePath: %v", volumeSpec.VolumePath)
			}
			createSpec.BackingObjectDetails = &cnstypes.CnsBlockBackingDetails{BackingDiskId: backingObjectID}
			log.Infof("Registering volume: %q using backingDiskId :%q", volumeSpec.VolumePath, backingObjectID)
		} else {
			createSpec.BackingObjectDetails = &cnstypes.CnsBlockBackingDetails{BackingDiskUrlPath: backingDiskURLPath}
			log.Infof("Registering volume: %q using backingDiskURLPath :%q", volumeSpec.VolumePath, backingDiskURLPath)
		}
		log.Debugf("vSphere CSI driver registering volume %q with create spec %+v",
			volumeSpec.VolumePath, spew.Sdump(createSpec))
		volumeInfo, err = (*volumeMigration.volumeManager).CreateVolume(ctx, createSpec)
		if err != nil {
			log.Warnf("failed to register volume %q with createSpec: %v. error: %+v",
				volumeSpec.VolumePath, createSpec, err)
		} else {
			break
		}
	}
	if volumeInfo != nil {
		log.Infof("Successfully registered volume %q as container volume with ID: %q",
			volumeSpec.VolumePath, volumeInfo.VolumeID.Id)
	} else {
		return "", logger.LogNewErrorf(log,
			"registration failed for volumeSpec: %v", volumeSpec)
	}
	return volumeInfo.VolumeID.Id, nil
}

// cleanupStaleCRDInstances helps in cleaning up stale volume migration CRD
// instances.
func (volumeMigration *volumeMigration) cleanupStaleCRDInstances() {
	ticker := time.NewTicker(
		time.Duration(volumeMigrationInstance.cnsConfig.Global.VolumeMigrationCRCleanupIntervalInMin) * time.Minute)
	for range ticker.C {
		ctx, log := logger.GetNewContextWithLogger()
		config, err := k8s.GetKubeConfig(ctx)
		if err != nil {
			log.Warnf("failed to get kubeconfig. err: %v", err)
			continue
		}
		crdGroupClient, err := k8s.NewClientForGroup(ctx, config, CRDGroupName)
		if err != nil {
			log.Warnf("failed to create new client for group %q . Err: %+v", CRDGroupName, err)
			continue
		}

		log.Infof("Triggering CnsVSphereVolumeMigration cleanup routine")
		volumeMigrationResourceList := &migrationv1alpha1.CnsVSphereVolumeMigrationList{}
		err = crdGroupClient.List(ctx, volumeMigrationResourceList)
		if err != nil {
			log.Warnf("failed to get CnsVSphereVolumeMigration list from the cluster. Err: %+v", err)
			continue
		}
		log.Debugf("CnsVSphereVolumeMigrationList: %+v", volumeMigrationResourceList)
		queryFilter := cnstypes.CnsQueryFilter{
			ContainerClusterIds: []string{
				volumeMigrationInstance.cnsConfig.Global.ClusterID,
			},
		}
		queryAllResult, err := utils.QueryAllVolumeUtil(ctx, *volumeMigrationInstance.volumeManager, queryFilter,
			cnstypes.CnsQuerySelection{}, commonco.ContainerOrchestratorUtility.IsFSSEnabled(ctx, common.AsyncQueryVolume))
		if err != nil {
			log.Warnf("failed to queryAllVolume with err %+v", err)
			continue
		}
		if len(queryAllResult.Volumes) == 0 {
			log.Debugf("No volumes found in Query Volume Result")
			continue
		}
		log.Debugf("QueryVolumeInfo successfully returned with result:  %v:", spew.Sdump(queryAllResult))
		cnsVolumesMap := make(map[string]bool)
		for _, vol := range queryAllResult.Volumes {
			cnsVolumesMap[vol.VolumeId.Id] = true
		}
		log.Debugf("cnsVolumesMap:  %v:", cnsVolumesMap)
		for _, volumeMigrationResource := range volumeMigrationResourceList.Items {
			if _, existsInCNSVolumesMap := cnsVolumesMap[volumeMigrationResource.Name]; !existsInCNSVolumesMap {
				log.Debugf("Volume with id %s is not found in CNS", volumeMigrationResource.Name)
				err = volumeMigrationInstance.DeleteVolumeInfo(ctx, volumeMigrationResource.Name)
				if err != nil {
					log.Warnf("failed to delete volume mapping CR for %s with error %+v", volumeMigrationResource.Name, err)
					continue
				}
			}
		}
		log.Infof("Completed CnsVSphereVolumeMigration cleanup")
	}
}
