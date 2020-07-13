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
	"errors"
	"fmt"
	"net/url"
	"reflect"
	"regexp"
	"strings"
	"sync"

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

	migrationv1alpha1 "sigs.k8s.io/vsphere-csi-driver/pkg/apis/migration/v1alpha1"
	cnsvolume "sigs.k8s.io/vsphere-csi-driver/pkg/common/cns-lib/volume"
	"sigs.k8s.io/vsphere-csi-driver/pkg/common/cns-lib/vsphere"
	cnsconfig "sigs.k8s.io/vsphere-csi-driver/pkg/common/config"
	"sigs.k8s.io/vsphere-csi-driver/pkg/csi/service/common"
	"sigs.k8s.io/vsphere-csi-driver/pkg/csi/service/logger"
	k8s "sigs.k8s.io/vsphere-csi-driver/pkg/kubernetes"
)

// VolumeMigrationService exposes interfaces to support VCP to CSI migration.
// It will maintain internal state to map volume path to volume ID and reverse mapping.
type VolumeMigrationService interface {
	// GetVolumeID returns VolumeID for given VolumePath
	// Returns an error if not able to retrieve VolumeID.
	GetVolumeID(ctx context.Context, volumePath string) (string, error)

	// GetVolumePath returns VolumePath for given VolumeID
	// Returns an error if not able to retrieve VolumePath.
	GetVolumePath(ctx context.Context, volumeID string) (string, error)

	// DeleteVolumeInfo helps delete mapping of volumePath to VolumeID for specified volumeID
	DeleteVolumeInfo(ctx context.Context, volumeID string) error
}

// volumeMigration holds migrated volume information and provides functionality around it.
type volumeMigration struct {
	// volumePath to volumeId map
	volumePathToVolumeID sync.Map
	// k8sClient helps operate on CnsVSphereVolumeMigration custom resource
	k8sClient client.Client
	// volumeManager helps perform Volume Operations
	volumeManager *cnsvolume.Manager
	// cnsConfig helps retrieve vSphere CSI configuration for RegisterVolume Operation
	cnsConfig *cnsconfig.Config
}

const (
	// CRDName represent the name of cnsvspherevolumemigrations CRD
	CRDName = "cnsvspherevolumemigrations.cns.vmware.com"
	// CRDGroupName represent the group of cnsvspherevolumemigrations CRD
	CRDGroupName = "cns.vmware.com"
	// CRDSingular represent the singular name of cnsvspherevolumemigrations CRD
	CRDSingular = "cnsvspherevolumemigration"
	// CRDPlural represent the plural name of cnsvspherevolumemigrations CRD
	CRDPlural = "cnsvspherevolumemigrations"
)

// onceForVolumeMigrationService is used for initializing the VolumeMigrationService singleton.
var onceForVolumeMigrationService sync.Once

// volumeMigrationInstance is instance of volumeMigration and implements interface for VolumeMigrationService
var volumeMigrationInstance *volumeMigration

var volumeMigrationServiceInitErr error

// GetVolumeMigrationService returns the singleton VolumeMigrationService
func GetVolumeMigrationService(ctx context.Context, volumeManager *cnsvolume.Manager, cnsConfig *cnsconfig.Config) (VolumeMigrationService, error) {
	log := logger.GetLogger(ctx)
	onceForVolumeMigrationService.Do(func() {
		log.Info("Initializing volume migration service...")
		volumeMigrationInstance = &volumeMigration{
			volumePathToVolumeID: sync.Map{},
			volumeManager:        volumeManager,
			cnsConfig:            cnsConfig,
		}
		volumeMigrationServiceInitErr = k8s.CreateCustomResourceDefinition(ctx, CRDName, CRDSingular, CRDPlural,
			reflect.TypeOf(migrationv1alpha1.CnsVSphereVolumeMigration{}).Name(), migrationv1alpha1.SchemeGroupVersion.Group, migrationv1alpha1.SchemeGroupVersion.Version, apiextensionsv1beta1.ClusterScoped)
		if volumeMigrationServiceInitErr != nil {
			log.Errorf("failed to create volume migration CRD. Error: %v", volumeMigrationServiceInitErr)
			return
		}
		config, volumeMigrationServiceInitErr := k8s.GetKubeConfig(ctx)
		if volumeMigrationServiceInitErr != nil {
			log.Errorf("failed to get kubeconfig. err: %v", volumeMigrationServiceInitErr)
			return
		}
		volumeMigrationInstance.k8sClient, volumeMigrationServiceInitErr = k8s.NewClientForGroup(ctx, config, CRDGroupName)
		if volumeMigrationServiceInitErr != nil {
			log.Errorf("failed to create k8sClient. Err: %v", volumeMigrationServiceInitErr)
			return
		}
		go func() {
			log.Debugf("Starting Informer for cnsvspherevolumemigrations")
			informer, err := k8s.GetDynamicInformer(ctx, migrationv1alpha1.SchemeGroupVersion.Group, migrationv1alpha1.SchemeGroupVersion.Version, "cnsvspherevolumemigrations", "")
			if err != nil {
				log.Errorf("failed to create dynamic informer for volume migration CRD. Err: %v", err)
				return
			}
			handlers := cache.ResourceEventHandlerFuncs{
				AddFunc: func(obj interface{}) {
					log.Debugf("received add event for VolumeMigration CR!")
					var volumeMigrationObject migrationv1alpha1.CnsVSphereVolumeMigration
					runtime.DefaultUnstructuredConverter.FromUnstructured(obj.(*unstructured.Unstructured).Object, &volumeMigrationObject)
					volumeMigrationInstance.volumePathToVolumeID.Store(volumeMigrationObject.Spec.VolumePath, volumeMigrationObject.Spec.VolumeID)
					log.Debugf("successfully added volumePath: %q, volumeID: %q mapping in the cache", volumeMigrationObject.Spec.VolumePath, volumeMigrationObject.Spec.VolumeID)
				},
				DeleteFunc: func(obj interface{}) {
					log.Debugf("received delete event for VolumeMigration CR!")
					var volumeMigrationObject migrationv1alpha1.CnsVSphereVolumeMigration
					runtime.DefaultUnstructuredConverter.FromUnstructured(obj.(*unstructured.Unstructured).Object, &volumeMigrationObject)
					volumeMigrationInstance.volumePathToVolumeID.Delete(volumeMigrationObject.Spec.VolumePath)
					log.Debugf("successfully deleted volumePath: %q, volumeID: %q mapping from cache", volumeMigrationObject.Spec.VolumePath, volumeMigrationObject.Spec.VolumeID)
				},
			}
			informer.Informer().AddEventHandler(handlers)
			stopCh := make(chan struct{})
			informer.Informer().Run(stopCh)
			log.Debugf("Informer started for cnsvspherevolumemigrations")
		}()
	})
	if volumeMigrationServiceInitErr != nil {
		log.Info("volume migration service initialization failed")
		return nil, volumeMigrationServiceInitErr
	}
	log.Info("volume migration service initialized")
	return volumeMigrationInstance, nil
}

// GetVolumeID returns VolumeID for given VolumePath
// Returns an error if not able to retrieve VolumeID.
func (volumeMigration *volumeMigration) GetVolumeID(ctx context.Context, volumePath string) (string, error) {
	log := logger.GetLogger(ctx)
	info, found := volumeMigration.volumePathToVolumeID.Load(volumePath)
	if found {
		log.Infof("VolumeID: %q found from the cache for VolumePath: %q", info.(string), volumePath)
		return info.(string), nil
	}
	log.Infof("Could not retrieve VolumeID from cache for Volume Path: %q. volume may not be registered. Registering Volume with CNS", volumePath)
	volumeID, err := volumeMigration.registerVolume(ctx, volumePath)
	if err != nil {
		log.Errorf("failed to register volume for VolumePath: %q, with err: %v", volumePath, err)
		return "", err
	}
	log.Infof("successfully registered volume: %q with CNS. VolumeID: %q", volumePath, volumeID)
	cnsvSphereVolumeMigration := migrationv1alpha1.CnsVSphereVolumeMigration{
		ObjectMeta: metav1.ObjectMeta{Name: volumeID},
		Spec: migrationv1alpha1.CnsVSphereVolumeMigrationSpec{
			VolumePath: volumePath,
			VolumeID:   volumeID,
		},
	}
	log.Debugf("Saving cnsvSphereVolumeMigration CR: %v", cnsvSphereVolumeMigration)
	err = volumeMigration.saveVolumeInfo(ctx, &cnsvSphereVolumeMigration)
	if err != nil {
		log.Errorf("failed to save cnsvSphereVolumeMigration CR:%v, err: %v", err)
		return "", err
	}
	log.Infof("successfully saved cnsvSphereVolumeMigration CR: %v", cnsvSphereVolumeMigration)
	return volumeID, nil
}

// GetVolumePath returns VolumePath for given VolumeID
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
	log.Infof("Could not retrieve mapping of volume path and VolumeID in the cache for VolumeID: %q. volume may not be registered", volumeID)
	volumeIds := []cnstypes.CnsVolumeId{{Id: volumeID}}
	log.Infof("Calling QueryVolumeInfo using: %v", volumeIds)
	queryVolumeInfoResult, err := (*volumeMigration.volumeManager).QueryVolumeInfo(ctx, volumeIds)
	if err != nil {
		log.Errorf("QueryVolumeInfo failed for volumeID: %s, err: %v", volumeID, err)
		return "", err
	}
	log.Debugf("QueryVolumeInfo successfully returned volumeInfo %v for volumeIDList %v:", spew.Sdump(queryVolumeInfoResult), volumeIds)
	cnsBlockVolumeInfo := interface{}(queryVolumeInfoResult.VolumeInfo).(*cnstypes.CnsBlockVolumeInfo)
	fileBackingInfo := interface{}(cnsBlockVolumeInfo.VStorageObject.Config.Backing).(*vim25types.BaseConfigInfoDiskFileBackingInfo)
	log.Infof("successfully retrieved volume path: %q for VolumeID: %q", fileBackingInfo.FilePath, volumeID)
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
	log.Infof("successfully saved cnsvSphereVolumeMigration CR: %v", cnsvSphereVolumeMigration)
	return fileBackingInfo.FilePath, nil
}

// SaveVolumeInfo helps create CR for given cnsVSphereVolumeMigration
// this func also update local cache with supplied cnsVSphereVolumeMigration, after successful creation of CR
func (volumeMigration *volumeMigration) saveVolumeInfo(ctx context.Context, cnsVSphereVolumeMigration *migrationv1alpha1.CnsVSphereVolumeMigration) error {
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
	log.Infof("successfully created CR for cnsVSphereVolumeMigration: %+v", cnsVSphereVolumeMigration)
	return nil
}

// DeleteVolumeInfo helps delete mapping of volumePath to VolumeID for specified volumeID
func (volumeMigration *volumeMigration) DeleteVolumeInfo(ctx context.Context, volumeID string) error {
	log := logger.GetLogger(ctx)
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

// registerVolume takes VolumePath and helps register Volume with CNS
// Returns VolumeID for successful registration, otherwise return error
func (volumeMigration *volumeMigration) registerVolume(ctx context.Context, volumePath string) (string, error) {
	log := logger.GetLogger(ctx)
	uuid, err := uuid.NewUUID()
	if err != nil {
		log.Errorf("failed to generate uuid")
		return "", err
	}
	re := regexp.MustCompile(`\[([^\[\]]*)\]`)
	if !re.MatchString(volumePath) {
		msg := fmt.Sprintf("failed to extract datastore name from in-tree volume path: %q", volumePath)
		log.Errorf(msg)
		return "", errors.New(msg)
	}
	datastoreName := re.FindAllString(volumePath, -1)[0]
	vmdkPath := strings.TrimSpace(strings.Trim(volumePath, datastoreName))
	datastoreName = strings.Trim(strings.Trim(datastoreName, "["), "]")

	var datacenters string
	var user string
	var host string
	if volumeMigration.cnsConfig == nil || len(volumeMigration.cnsConfig.VirtualCenter) == 0 {
		msg := "could not find vcenter config"
		log.Errorf(msg)
		return "", errors.New(msg)
	}
	for key, val := range volumeMigration.cnsConfig.VirtualCenter {
		datacenters = val.Datacenters
		user = val.User
		host = key
		break
	}
	datacenterPaths := make([]string, 0)
	if datacenters != "" {
		datacenterPaths = strings.Split(datacenters, ",")
	} else {
		// Get vCenter
		vCenter, err := vsphere.GetVirtualCenterManager(ctx).GetVirtualCenter(ctx, host)
		if err != nil {
			log.Errorf("failed to get vCenter. err: %v", err)
			return "", err
		}
		// Connect to vCenter
		err = vCenter.Connect(ctx)
		if err != nil {
			log.Errorf("failed to connect to vCenter. err: %v", err)
			return "", err
		}
		// Get all datacenters from vCenter
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
	var volumeID *cnstypes.CnsVolumeId
	for _, datacenter := range datacenterPaths {
		// Format:
		// https://<vc_ip>/folder/<vm_vmdk_path>?dcPath=<datacenter-path>&dsName=<datastoreName>
		backingDiskURLPath := "https://" + host + "/folder/" +
			vmdkPath + "?dcPath=" + url.PathEscape(datacenter) + "&dsName=" + url.PathEscape(datastoreName)

		log.Infof("Registering volume: %q using backingDiskURLPath :%q", volumePath, backingDiskURLPath)
		var containerClusterArray []cnstypes.CnsContainerCluster
		containerCluster := vsphere.GetContainerCluster(volumeMigration.cnsConfig.Global.ClusterID, user, cnstypes.CnsClusterFlavorVanilla)
		containerClusterArray = append(containerClusterArray, containerCluster)
		createSpec := &cnstypes.CnsVolumeCreateSpec{
			Name:       uuid.String(),
			VolumeType: common.BlockVolumeType,
			Metadata: cnstypes.CnsVolumeMetadata{
				ContainerCluster:      containerCluster,
				ContainerClusterArray: containerClusterArray,
			},
			BackingObjectDetails: &cnstypes.CnsBlockBackingDetails{
				BackingDiskUrlPath: backingDiskURLPath,
			},
		}
		log.Debugf("vSphere CNS driver registering volume %q with create spec %+v", volumePath, spew.Sdump(createSpec))
		volumeID, err = (*volumeMigration.volumeManager).CreateVolume(ctx, createSpec)
		if err != nil {
			log.Warnf("failed to register volume %q with error %+v", volumePath, err)
		} else {
			break
		}
	}
	if volumeID != nil {
		log.Infof("successfully registered volume %q as container volume with ID: %q", volumePath, volumeID.Id)
	} else {
		msg := fmt.Sprintf("registration failed for volumePath: %q", volumePath)
		log.Error(msg)
		return "", errors.New(msg)
	}
	return volumeID.Id, nil
}
