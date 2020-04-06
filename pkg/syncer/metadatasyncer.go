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

package syncer

import (
	"context"
	"os"
	"path/filepath"
	"reflect"
	"strconv"
	"time"

	"github.com/pkg/errors"

	"sigs.k8s.io/vsphere-csi-driver/pkg/csi/service/logger"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/client-go/informers"
	clientset "k8s.io/client-go/kubernetes"
	"k8s.io/client-go/util/workqueue"

	"github.com/davecgh/go-spew/spew"
	"github.com/fsnotify/fsnotify"
	cnstypes "github.com/vmware/govmomi/cns/types"
	v1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"

	volumes "sigs.k8s.io/vsphere-csi-driver/pkg/common/cns-lib/volume"
	cnsvsphere "sigs.k8s.io/vsphere-csi-driver/pkg/common/cns-lib/vsphere"
	cnsconfig "sigs.k8s.io/vsphere-csi-driver/pkg/common/config"
	"sigs.k8s.io/vsphere-csi-driver/pkg/csi/service/common"
	csitypes "sigs.k8s.io/vsphere-csi-driver/pkg/csi/types"
	k8s "sigs.k8s.io/vsphere-csi-driver/pkg/kubernetes"
	cnsoperatorv1alpha1 "sigs.k8s.io/vsphere-csi-driver/pkg/syncer/cnsoperator/apis"
	"sigs.k8s.io/vsphere-csi-driver/pkg/syncer/types"
)

// newInformer returns uninitialized metadataSyncInformer
func newInformer() *metadataSyncInformer {
	return &metadataSyncInformer{}
}

// getFullSyncIntervalInMin return the FullSyncInterval
// If environment variable FULL_SYNC_INTERVAL_MINUTES is set and valid,
// return the interval value read from enviroment variable
// otherwise, use the default value 30 minutes
func getFullSyncIntervalInMin(ctx context.Context) int {
	log := logger.GetLogger(ctx)
	fullSyncIntervalInMin := defaultFullSyncIntervalInMin
	if v := os.Getenv("FULL_SYNC_INTERVAL_MINUTES"); v != "" {
		if value, err := strconv.Atoi(v); err == nil {
			if value <= 0 {
				log.Warnf("FullSync: fullSync interval set in env variable FULL_SYNC_INTERVAL_MINUTES %s is equal or less than 0, will use the default interval", v)
			} else if value > defaultFullSyncIntervalInMin {
				log.Warnf("FullSync: fullSync interval set in env variable FULL_SYNC_INTERVAL_MINUTES %s is larger than max value can be set, will use the default interval", v)
			} else {
				fullSyncIntervalInMin = value
				log.Infof("FullSync: fullSync interval is set to %d minutes", fullSyncIntervalInMin)
			}
		} else {
			log.Warnf("FullSync: fullSync interval set in env variable FULL_SYNC_INTERVAL_MINUTES %s is invalid, will use the default interval", v)
		}
	}
	return fullSyncIntervalInMin
}

// InitMetadataSyncer initializes the Metadata Sync Informer
func InitMetadataSyncer(ctx context.Context, clusterFlavor cnstypes.CnsClusterFlavor, configInfo *types.ConfigInfo) error {
	log := logger.GetLogger(ctx)
	var err error
	log.Infof("Initializing MetadataSyncer")
	metadataSyncer := newInformer()
	metadataSyncer.configInfo = configInfo

	// Create the kubernetes client from config
	k8sClient, err := k8s.NewClient(ctx)
	if err != nil {
		log.Errorf("Creating Kubernetes client failed. Err: %v", err)
		return err
	}
	metadataSyncer.clusterFlavor = clusterFlavor
	if metadataSyncer.clusterFlavor == cnstypes.CnsClusterFlavorGuest {
		// Initialize client to supervisor cluster
		// if metadata syncer is being initialized for guest clusters
		restClientConfig := k8s.GetRestClientConfig(ctx, metadataSyncer.configInfo.Cfg.GC.Endpoint, metadataSyncer.configInfo.Cfg.GC.Port)
		metadataSyncer.cnsOperatorClient, err = k8s.NewClientForGroup(ctx, restClientConfig, cnsoperatorv1alpha1.GroupName)
		if err != nil {
			log.Errorf("Creating Cns Operator client failed. Err: %v", err)
			return err
		}
		// Initialize supervisor client for Volume Health
		metadataSyncer.supervisorClient, err = k8s.NewSupervisorClient(ctx, restClientConfig)
		if err != nil {
			log.Errorf("Failed to create supervisorClient. Error: %+v", err)
			return err
		}
	} else {
		// Initialize volume manager with vcenter credentials
		// if metadata syncer is being intialized for Vanilla or Supervisor clusters
		vCenter, err := types.GetVirtualCenterInstance(ctx, configInfo)
		if err != nil {
			return err
		}
		metadataSyncer.host = vCenter.Config.Host
		metadataSyncer.volumeManager = volumes.GetManager(ctx, vCenter)
	}

	// Initialize cnsDeletionMap used by Full Sync
	cnsDeletionMap = make(map[string]bool)
	// Initialize cnsCreationMap used by Full Sync
	cnsCreationMap = make(map[string]bool)

	cfgPath := common.GetConfigPath(ctx)
	watcher, err := fsnotify.NewWatcher()
	if err != nil {
		log.Errorf("failed to create fsnotify watcher. err=%v", err)
		return err
	}
	go func() {
		for {
			log.Debugf("Waiting for event on fsnotify watcher")
			select {
			case event, ok := <-watcher.Events:
				if !ok {
					return
				}
				log.Debugf("fsnotify event: %q", event.String())
				if event.Op&fsnotify.Remove == fsnotify.Remove {
					log.Infof("Reloading Configuration")
					ReloadConfiguration(ctx, metadataSyncer)
					log.Infof("Successfully reloaded configuration from: %q", cfgPath)
					if metadataSyncer.clusterFlavor == cnstypes.CnsClusterFlavorGuest {
						log.Infof("Successfully reloaded configuration from: %q", cnsconfig.DefaultpvCSIProviderPath)
					}
				}
			case err, ok := <-watcher.Errors:
				if !ok {
					log.Errorf("fsnotify error: %+v", err)
					return
				}
			}
			log.Debugf("fsnotify event processed")
		}
	}()
	cfgDirPath := filepath.Dir(cfgPath)
	log.Infof("Adding watch on path: %q", cfgDirPath)
	err = watcher.Add(cfgDirPath)
	if err != nil {
		log.Errorf("failed to watch on path: %q. err=%v", cfgDirPath, err)
		return err
	}
	if metadataSyncer.clusterFlavor == cnstypes.CnsClusterFlavorGuest {
		log.Infof("Adding watch on path: %q", cnsconfig.DefaultpvCSIProviderPath)
		err = watcher.Add(cnsconfig.DefaultpvCSIProviderPath)
		if err != nil {
			log.Errorf("failed to watch on path: %q. err=%v", cnsconfig.DefaultpvCSIProviderPath, err)
			return err
		}
	}

	// Set up kubernetes resource listeners for metadata syncer
	metadataSyncer.k8sInformerManager = k8s.NewInformer(k8sClient)
	metadataSyncer.k8sInformerManager.AddPVCListener(
		nil, // Add
		func(oldObj interface{}, newObj interface{}) { // Update
			pvcUpdated(oldObj, newObj, metadataSyncer)
		},
		func(obj interface{}) { // Delete
			pvcDeleted(obj, metadataSyncer)
		})
	metadataSyncer.k8sInformerManager.AddPVListener(
		nil, // Add
		func(oldObj interface{}, newObj interface{}) { // Update
			pvUpdated(oldObj, newObj, metadataSyncer)
		},
		func(obj interface{}) { // Delete
			pvDeleted(obj, metadataSyncer)
		})
	metadataSyncer.k8sInformerManager.AddPodListener(
		nil, // Add
		func(oldObj interface{}, newObj interface{}) { // Update
			podUpdated(oldObj, newObj, metadataSyncer)
		},
		func(obj interface{}) { // Delete
			podDeleted(obj, metadataSyncer)
		})
	metadataSyncer.pvLister = metadataSyncer.k8sInformerManager.GetPVLister()
	metadataSyncer.pvcLister = metadataSyncer.k8sInformerManager.GetPVCLister()
	metadataSyncer.podLister = metadataSyncer.k8sInformerManager.GetPodLister()
	stopCh := metadataSyncer.k8sInformerManager.Listen()
	if stopCh == nil {
		msg := "Failed to sync informer caches"
		log.Error(msg)
		return errors.New(msg)
	}
	log.Infof("Initialized metadata syncer")

	ticker := time.NewTicker(time.Duration(getFullSyncIntervalInMin(ctx)) * time.Minute)
	// Trigger full sync
	go func() {
		for ; true; <-ticker.C {
			ctx, log = logger.GetNewContextWithLogger()
			log.Infof("fullSync is triggered")
			if metadataSyncer.clusterFlavor == cnstypes.CnsClusterFlavorGuest {
				pvcsiFullSync(ctx, metadataSyncer)
			} else {
				csiFullSync(ctx, metadataSyncer)
			}
		}
	}()

	// Trigger volume health reconciler
	go func() {
		ctx, log = logger.GetNewContextWithLogger()
		if metadataSyncer.clusterFlavor == cnstypes.CnsClusterFlavorGuest {
			initVolumeHealthReconciler(ctx, k8sClient, metadataSyncer.supervisorClient)
		}
	}()

	<-stopCh
	return nil
}

// ReloadConfiguration reloads configuration from the secret, and update controller's cached configs
func ReloadConfiguration(ctx context.Context, metadataSyncer *metadataSyncInformer) {
	log := logger.GetLogger(ctx)
	cfg, err := common.GetConfig(ctx)
	if err != nil {
		log.Errorf("failed to read config. Error: %+v", err)
		return
	}
	if metadataSyncer.clusterFlavor == cnstypes.CnsClusterFlavorGuest {
		var err error
		restClientConfig := k8s.GetRestClientConfig(ctx, cfg.GC.Endpoint, metadataSyncer.configInfo.Cfg.GC.Port)
		metadataSyncer.cnsOperatorClient, err = k8s.NewClientForGroup(ctx, restClientConfig, cnsoperatorv1alpha1.GroupName)
		if err != nil {
			log.Errorf("failed to create cns operator client. Err: %v", err)
			return
		}
		// Initialize supervisor client for Volume Health
		metadataSyncer.supervisorClient, err = k8s.NewSupervisorClient(ctx, restClientConfig)
		if err != nil {
			log.Errorf("Failed to create supervisorClient. Error: %+v", err)
			return
		}
	} else {
		newVCConfig, err := cnsvsphere.GetVirtualCenterConfig(cfg)
		if err != nil {
			log.Errorf("failed to get VirtualCenterConfig. err=%v", err)
			return
		}
		if newVCConfig != nil {
			var vcenter *cnsvsphere.VirtualCenter
			if metadataSyncer.configInfo.Cfg.Global.VCenterIP != newVCConfig.Host ||
				metadataSyncer.configInfo.Cfg.Global.User != newVCConfig.Username ||
				metadataSyncer.configInfo.Cfg.Global.Password != newVCConfig.Password {
				vcManager := cnsvsphere.GetVirtualCenterManager(ctx)
				log.Debugf("Unregistering virtual center: %q from virtualCenterManager", metadataSyncer.configInfo.Cfg.Global.VCenterIP)
				err = vcManager.UnregisterAllVirtualCenters(ctx)
				if err != nil {
					log.Errorf("failed to unregister vcenter with virtualCenterManager.")
					return
				}
				log.Debugf("Registering virtual center: %q with virtualCenterManager", newVCConfig.Host)
				vcenter, err = vcManager.RegisterVirtualCenter(ctx, newVCConfig)
				if err != nil {
					log.Errorf("failed to register VC with virtualCenterManager. err=%v", err)
					return
				}
			} else {
				vcenter, err = types.GetVirtualCenterInstance(ctx, &types.ConfigInfo{Cfg: cfg})
				if err != nil {
					log.Errorf("failed to get VirtualCenter. err=%v", err)
					return
				}
			}
			metadataSyncer.volumeManager.ResetManager(ctx, vcenter)
			metadataSyncer.volumeManager = volumes.GetManager(ctx, vcenter)
		}
		if cfg != nil {
			metadataSyncer.configInfo = &types.ConfigInfo{Cfg: cfg}
			log.Infof("updated metadataSyncer.configInfo")
		}
	}
}

// pvcUpdated updates persistent volume claim metadata on VC when pvc labels on K8S cluster have been updated
func pvcUpdated(oldObj, newObj interface{}, metadataSyncer *metadataSyncInformer) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	ctx = logger.NewContextWithLogger(ctx)
	log := logger.GetLogger(ctx)

	// Get old and new pvc objects
	oldPvc, ok := oldObj.(*v1.PersistentVolumeClaim)
	if oldPvc == nil || !ok {
		return
	}
	newPvc, ok := newObj.(*v1.PersistentVolumeClaim)
	if newPvc == nil || !ok {
		return
	}

	if newPvc.Status.Phase != v1.ClaimBound {
		log.Debugf("PVCUpdated: New PVC not in Bound phase")
		return
	}

	// Get pv object attached to pvc
	pv, err := metadataSyncer.pvLister.Get(newPvc.Spec.VolumeName)
	if pv == nil || err != nil {
		if !apierrors.IsNotFound(err) {
			log.Errorf("PVCUpdated: Error getting Persistent Volume for pvc %s in namespace %s with err: %v", newPvc.Name, newPvc.Namespace, err)
			return
		}
		log.Infof("PVCUpdated: PV with name %s not found using PV Lister. Querying API server to get PV Info", newPvc.Spec.VolumeName)
		// Create the kubernetes client from config
		k8sClient, err := k8s.NewClient(ctx)
		if err != nil {
			log.Errorf("PVCUpdated: Creating Kubernetes client failed. Err: %v", err)
			return
		}
		pv, err = k8sClient.CoreV1().PersistentVolumes().Get(newPvc.Spec.VolumeName, metav1.GetOptions{})
		if err != nil {
			log.Errorf("PVCUpdated: Error getting Persistent Volume %s from API server with err: %v", newPvc.Spec.VolumeName, err)
			return
		}
		log.Debugf("PVCUpdated: Found Persistent Volume %s from API server", newPvc.Spec.VolumeName)
	}

	// Verify if pv is vsphere csi volume
	if pv.Spec.CSI == nil || pv.Spec.CSI.Driver != csitypes.Name {
		log.Debugf("PVCUpdated: Not a Vsphere CSI Volume")
		return
	}

	// Verify is old and new labels are not equal
	if oldPvc.Status.Phase == v1.ClaimBound && reflect.DeepEqual(newPvc.Labels, oldPvc.Labels) {
		log.Debugf("PVCUpdated: Old PVC and New PVC labels equal")
		return
	}

	if metadataSyncer.clusterFlavor == cnstypes.CnsClusterFlavorGuest {
		// Invoke volume updated method for pvCSI
		pvcsiVolumeUpdated(ctx, newPvc, pv.Spec.CSI.VolumeHandle, metadataSyncer)
	} else {
		csiPVCUpdated(ctx, newPvc, pv, metadataSyncer)
	}
}

// pvDeleted deletes pvc metadata on VC when pvc has been deleted on K8s cluster
func pvcDeleted(obj interface{}, metadataSyncer *metadataSyncInformer) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	ctx = logger.NewContextWithLogger(ctx)
	log := logger.GetLogger(ctx)

	pvc, ok := obj.(*v1.PersistentVolumeClaim)
	if pvc == nil || !ok {
		log.Warnf("PVCDeleted: unrecognized object %+v", obj)
		return
	}
	log.Debugf("PVCDeleted: %+v", pvc)
	if pvc.Status.Phase != v1.ClaimBound {
		return
	}
	// Get pv object attached to pvc
	pv, err := metadataSyncer.pvLister.Get(pvc.Spec.VolumeName)
	if pv == nil || err != nil {
		log.Errorf("PVCDeleted: Error getting Persistent Volume for pvc %s in namespace %s with err: %v", pvc.Name, pvc.Namespace, err)
		return
	}

	// Verify if pv is a vsphere csi volume
	if pv.Spec.CSI == nil || pv.Spec.CSI.Driver != csitypes.Name {
		log.Debugf("PVCDeleted: Not a Vsphere CSI Volume")
		return
	}

	if metadataSyncer.clusterFlavor == cnstypes.CnsClusterFlavorGuest {
		// Invoke volume deleted method for pvCSI
		pvcsiVolumeDeleted(ctx, string(pvc.GetUID()), metadataSyncer)
	} else {
		csiPVCDeleted(ctx, pvc, pv, metadataSyncer)
	}
}

// pvUpdated updates volume metadata on VC when volume labels on K8S cluster have been updated
func pvUpdated(oldObj, newObj interface{}, metadataSyncer *metadataSyncInformer) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	ctx = logger.NewContextWithLogger(ctx)
	log := logger.GetLogger(ctx)

	// Get old and new PV objects
	oldPv, ok := oldObj.(*v1.PersistentVolume)
	if oldPv == nil || !ok {
		log.Warnf("PVUpdated: unrecognized old object %+v", oldObj)
		return
	}

	newPv, ok := newObj.(*v1.PersistentVolume)
	if newPv == nil || !ok {
		log.Warnf("PVUpdated: unrecognized new object %+v", newObj)
		return
	}
	log.Debugf("PVUpdated: PV Updated from %+v to %+v", oldPv, newPv)

	// Verify if pv is a vsphere csi volume
	if oldPv.Spec.CSI == nil || newPv.Spec.CSI == nil || newPv.Spec.CSI.Driver != csitypes.Name {
		log.Debugf("PVUpdated: PV is not a Vsphere CSI Volume: %+v", newPv)
		return
	}
	// Return if new PV status is Pending or Failed
	if newPv.Status.Phase == v1.VolumePending || newPv.Status.Phase == v1.VolumeFailed {
		log.Debugf("PVUpdated: PV %s metadata is not updated since updated PV is in phase %s", newPv.Name, newPv.Status.Phase)
		return
	}
	// Return if labels are unchanged
	if (oldPv.Status.Phase == v1.VolumeAvailable || oldPv.Status.Phase == v1.VolumeBound) && reflect.DeepEqual(newPv.GetLabels(), oldPv.GetLabels()) {
		log.Debugf("PVUpdated: PV labels have not changed")
		return
	}
	if oldPv.Status.Phase == v1.VolumeBound && newPv.Status.Phase == v1.VolumeReleased && oldPv.Spec.PersistentVolumeReclaimPolicy == v1.PersistentVolumeReclaimDelete {
		log.Debugf("PVUpdated: Volume will be deleted by controller")
		return
	}
	if newPv.DeletionTimestamp != nil {
		log.Debugf("PVUpdated: PV already deleted")
		return
	}
	if metadataSyncer.clusterFlavor == cnstypes.CnsClusterFlavorGuest {
		// Invoke volume updated method for pvCSI
		pvcsiVolumeUpdated(ctx, newPv, newPv.Spec.CSI.VolumeHandle, metadataSyncer)
	} else {
		csiPVUpdated(ctx, newPv, oldPv, metadataSyncer)
	}
}

// pvDeleted deletes volume metadata on VC when volume has been deleted on K8s cluster
func pvDeleted(obj interface{}, metadataSyncer *metadataSyncInformer) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	ctx = logger.NewContextWithLogger(ctx)
	log := logger.GetLogger(ctx)

	pv, ok := obj.(*v1.PersistentVolume)
	if pv == nil || !ok {
		log.Warnf("PVDeleted: unrecognized object %+v", obj)
		return
	}
	log.Debugf("PVDeleted: Deleting PV: %+v", pv)

	// Verify if pv is a vsphere csi volume
	if pv.Spec.CSI == nil || pv.Spec.CSI.Driver != csitypes.Name {
		log.Debugf("PVDeleted: Not a Vsphere CSI Volume: %+v", pv)
		return
	}

	if metadataSyncer.clusterFlavor == cnstypes.CnsClusterFlavorGuest {
		// Invoke volume deleted method for pvCSI
		pvcsiVolumeDeleted(ctx, string(pv.GetUID()), metadataSyncer)
	} else {
		csiPVDeleted(ctx, pv, metadataSyncer)
	}
}

// podUpdated updates pod metadata on VC when pod labels have been updated on K8s cluster
func podUpdated(oldObj, newObj interface{}, metadataSyncer *metadataSyncInformer) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	ctx = logger.NewContextWithLogger(ctx)
	log := logger.GetLogger(ctx)

	// Get old and new pod objects
	oldPod, ok := oldObj.(*v1.Pod)
	if oldPod == nil || !ok {
		log.Warnf("PodUpdated: unrecognized old object %+v", oldObj)
		return
	}
	newPod, ok := newObj.(*v1.Pod)
	if newPod == nil || !ok {
		log.Warnf("PodUpdated: unrecognized new object %+v", newObj)
		return
	}

	// If old pod is in pending state and new pod is running, update metadata
	if oldPod.Status.Phase == v1.PodPending && newPod.Status.Phase == v1.PodRunning {

		log.Debugf("PodUpdated: Pod %s calling updatePodMetadata", newPod.Name)
		// Update pod metadata
		updatePodMetadata(ctx, newPod, metadataSyncer, false)
	}
}

// podDeleted deletes pod metadata on VC when pod has been deleted on K8s cluster
func podDeleted(obj interface{}, metadataSyncer *metadataSyncInformer) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	ctx = logger.NewContextWithLogger(ctx)
	log := logger.GetLogger(ctx)

	// Get pod object
	pod, ok := obj.(*v1.Pod)
	if pod == nil || !ok {
		log.Warnf("PodDeleted: unrecognized new object %+v", obj)
		return
	}

	log.Debugf("PodDeleted: Pod %s calling updatePodMetadata", pod.Name)
	// Update pod metadata
	updatePodMetadata(ctx, pod, metadataSyncer, true)
}

// updatePodMetadata updates metadata for volumes attached to the pod
func updatePodMetadata(ctx context.Context, pod *v1.Pod, metadataSyncer *metadataSyncInformer, deleteFlag bool) {
	if metadataSyncer.clusterFlavor == cnstypes.CnsClusterFlavorGuest {
		pvcsiUpdatePod(ctx, pod, metadataSyncer, deleteFlag)
	} else {
		csiUpdatePod(ctx, pod, metadataSyncer, deleteFlag)
	}

}

// csiPVCUpdated updates volume metadata for PVC objects on the VC in Vanilla k8s and supervisor cluster
func csiPVCUpdated(ctx context.Context, pvc *v1.PersistentVolumeClaim, pv *v1.PersistentVolume, metadataSyncer *metadataSyncInformer) {
	log := logger.GetLogger(ctx)
	volumeFound := false
	// Following wait poll is required to avoid race condition between pvcUpdated and pvUpdated
	// This helps avoid race condition between pvUpdated and pvcUpdated handlers when static PV and PVC is created almost
	// at the same time using single YAML file.
	err := wait.Poll(5*time.Second, time.Minute, func() (bool, error) {
		queryFilter := cnstypes.CnsQueryFilter{
			VolumeIds: []cnstypes.CnsVolumeId{{Id: pv.Spec.CSI.VolumeHandle}},
		}
		queryResult, err := metadataSyncer.volumeManager.QueryVolume(ctx, queryFilter)
		if err != nil {
			log.Warnf("PVCUpdated: Failed to query volume metadata for volume %q with error %+v", pv.Spec.CSI.VolumeHandle, err)
			return false, err
		}
		if queryResult != nil && len(queryResult.Volumes) == 1 && queryResult.Volumes[0].VolumeId.Id == pv.Spec.CSI.VolumeHandle {
			log.Infof("PVCUpdated: volume %q found", pv.Spec.CSI.VolumeHandle)
			volumeFound = true
		}
		return volumeFound, nil
	})
	if err != nil {
		log.Errorf("PVCUpdated: Error occurred while polling to check if volume is marked as container volume. err: %+v", err)
		return
	}
	if !volumeFound {
		// volumeFound will be false when wait poll times out
		log.Errorf("PVCUpdated: volume: %q is not marked as the container volume. Skipping PVC entity metadata update.", pv.Spec.CSI.VolumeHandle)
		return
	}
	// Create updateSpec
	var metadataList []cnstypes.BaseCnsEntityMetadata
	entityReference := cnsvsphere.CreateCnsKuberenetesEntityReference(string(cnstypes.CnsKubernetesEntityTypePV), pv.Name, "", metadataSyncer.configInfo.Cfg.Global.ClusterID)
	pvcMetadata := cnsvsphere.GetCnsKubernetesEntityMetaData(pvc.Name, pvc.Labels, false, string(cnstypes.CnsKubernetesEntityTypePVC), pvc.Namespace, metadataSyncer.configInfo.Cfg.Global.ClusterID, []cnstypes.CnsKubernetesEntityReference{entityReference})

	metadataList = append(metadataList, cnstypes.BaseCnsEntityMetadata(pvcMetadata))
	containerCluster := cnsvsphere.GetContainerCluster(metadataSyncer.configInfo.Cfg.Global.ClusterID, metadataSyncer.configInfo.Cfg.VirtualCenter[metadataSyncer.host].User, metadataSyncer.clusterFlavor)

	updateSpec := &cnstypes.CnsVolumeMetadataUpdateSpec{
		VolumeId: cnstypes.CnsVolumeId{
			Id: pv.Spec.CSI.VolumeHandle,
		},
		Metadata: cnstypes.CnsVolumeMetadata{
			ContainerCluster:      containerCluster,
			ContainerClusterArray: []cnstypes.CnsContainerCluster{containerCluster},
			EntityMetadata:        metadataList,
		},
	}

	log.Debugf("PVCUpdated: Calling UpdateVolumeMetadata with updateSpec: %+v", spew.Sdump(updateSpec))
	if err := metadataSyncer.volumeManager.UpdateVolumeMetadata(ctx, updateSpec); err != nil {
		log.Errorf("PVCUpdated: UpdateVolumeMetadata failed with err %v", err)
	}
}

// csiPVCDeleted deletes volume metadata on VC when volume has been deleted on Vanilla k8s and supervisor cluster
func csiPVCDeleted(ctx context.Context, pvc *v1.PersistentVolumeClaim, pv *v1.PersistentVolume, metadataSyncer *metadataSyncInformer) {
	log := logger.GetLogger(ctx)
	// Volume will be deleted by controller when reclaim policy is delete
	if pv.Spec.PersistentVolumeReclaimPolicy == v1.PersistentVolumeReclaimDelete {
		log.Debugf("PVCDeleted: Reclaim policy is delete")
		return
	}

	// If the PV reclaim policy is retain we need to delete PVC labels
	var metadataList []cnstypes.BaseCnsEntityMetadata
	pvcMetadata := cnsvsphere.GetCnsKubernetesEntityMetaData(pvc.Name, nil, true, string(cnstypes.CnsKubernetesEntityTypePVC), pvc.Namespace, metadataSyncer.configInfo.Cfg.Global.ClusterID, nil)
	metadataList = append(metadataList, cnstypes.BaseCnsEntityMetadata(pvcMetadata))

	containerCluster := cnsvsphere.GetContainerCluster(metadataSyncer.configInfo.Cfg.Global.ClusterID, metadataSyncer.configInfo.Cfg.VirtualCenter[metadataSyncer.host].User, metadataSyncer.clusterFlavor)
	updateSpec := &cnstypes.CnsVolumeMetadataUpdateSpec{
		VolumeId: cnstypes.CnsVolumeId{
			Id: pv.Spec.CSI.VolumeHandle,
		},
		Metadata: cnstypes.CnsVolumeMetadata{
			ContainerCluster:      containerCluster,
			ContainerClusterArray: []cnstypes.CnsContainerCluster{containerCluster},
			EntityMetadata:        metadataList,
		},
	}

	log.Debugf("PVCDeleted: Calling UpdateVolumeMetadata for volume %s with updateSpec: %+v", updateSpec.VolumeId.Id, spew.Sdump(updateSpec))
	if err := metadataSyncer.volumeManager.UpdateVolumeMetadata(ctx, updateSpec); err != nil {
		log.Errorf("PVCDeleted: UpdateVolumeMetadata failed with err %v", err)
	}
}

// csiPVUpdated updates volume metadata on VC when volume labels on Vanilla k8s and supervisor cluster have been updated
func csiPVUpdated(ctx context.Context, newPv *v1.PersistentVolume, oldPv *v1.PersistentVolume, metadataSyncer *metadataSyncInformer) {
	log := logger.GetLogger(ctx)
	var metadataList []cnstypes.BaseCnsEntityMetadata
	pvMetadata := cnsvsphere.GetCnsKubernetesEntityMetaData(newPv.Name, newPv.GetLabels(), false, string(cnstypes.CnsKubernetesEntityTypePV), "", metadataSyncer.configInfo.Cfg.Global.ClusterID, nil)
	metadataList = append(metadataList, cnstypes.BaseCnsEntityMetadata(pvMetadata))

	containerCluster := cnsvsphere.GetContainerCluster(metadataSyncer.configInfo.Cfg.Global.ClusterID, metadataSyncer.configInfo.Cfg.VirtualCenter[metadataSyncer.host].User, metadataSyncer.clusterFlavor)
	if oldPv.Status.Phase == v1.VolumePending && newPv.Status.Phase == v1.VolumeAvailable && newPv.Spec.StorageClassName == "" {
		// Static PV is Created
		var volumeType string
		if oldPv.Spec.CSI.FSType == common.NfsV4FsType || oldPv.Spec.CSI.FSType == common.NfsFsType {
			volumeType = common.FileVolumeType
		} else {
			volumeType = common.BlockVolumeType
		}
		log.Debugf("PVUpdated: observed static volume provisioning for the PV: %q with volumeType: %q", newPv.Name, volumeType)
		queryFilter := cnstypes.CnsQueryFilter{
			VolumeIds: []cnstypes.CnsVolumeId{{Id: oldPv.Spec.CSI.VolumeHandle}},
		}
		volumeOperationsLock.Lock()
		defer volumeOperationsLock.Unlock()
		queryResult, err := metadataSyncer.volumeManager.QueryVolume(ctx, queryFilter)
		if err != nil {
			log.Errorf("PVUpdated: QueryVolume failed. error: %+v", err)
			return
		}
		if len(queryResult.Volumes) == 0 {
			log.Infof("PVUpdated: Verified volume: %q is not marked as container volume in CNS. Calling CreateVolume with BackingID to mark volume as Container Volume.", oldPv.Spec.CSI.VolumeHandle)
			// Call CreateVolume for Static Volume Provisioning
			createSpec := &cnstypes.CnsVolumeCreateSpec{
				Name:       oldPv.Name,
				VolumeType: volumeType,
				Metadata: cnstypes.CnsVolumeMetadata{
					ContainerCluster:      containerCluster,
					ContainerClusterArray: []cnstypes.CnsContainerCluster{containerCluster},
					EntityMetadata:        metadataList,
				},
			}

			if volumeType == common.BlockVolumeType {
				createSpec.BackingObjectDetails = &cnstypes.CnsBlockBackingDetails{
					CnsBackingObjectDetails: cnstypes.CnsBackingObjectDetails{},
					BackingDiskId:           oldPv.Spec.CSI.VolumeHandle,
				}
			} else {
				createSpec.BackingObjectDetails = &cnstypes.CnsVsanFileShareBackingDetails{
					CnsFileBackingDetails: cnstypes.CnsFileBackingDetails{
						BackingFileId: oldPv.Spec.CSI.VolumeHandle,
					},
				}
			}
			log.Debugf("PVUpdated: vSphere CSI Driver is creating volume %q with create spec %+v", oldPv.Name, spew.Sdump(createSpec))
			_, err := metadataSyncer.volumeManager.CreateVolume(ctx, createSpec)
			if err != nil {
				log.Errorf("PVUpdated: Failed to create disk %s with error %+v", oldPv.Name, err)
			} else {
				log.Infof("PVUpdated: vSphere CSI Driver has successfully marked volume: %q as the container volume.", oldPv.Spec.CSI.VolumeHandle)
			}
			// Volume is successfully created so returning from here.
			return
		} else if queryResult.Volumes[0].VolumeId.Id == oldPv.Spec.CSI.VolumeHandle {
			log.Infof("PVUpdated: Verified volume: %q is already marked as container volume in CNS.", oldPv.Spec.CSI.VolumeHandle)
			// Volume is already present in the CNS, so continue with the UpdateVolumeMetadata
		} else {
			log.Infof("PVUpdated: Queried volume: %q is other than requested volume: %q.", oldPv.Spec.CSI.VolumeHandle, queryResult.Volumes[0].VolumeId.Id)
			// unknown Volume is returned from the CNS, so returning from here.
			return
		}
	}
	// call UpdateVolumeMetadata for all other cases
	updateSpec := &cnstypes.CnsVolumeMetadataUpdateSpec{
		VolumeId: cnstypes.CnsVolumeId{
			Id: newPv.Spec.CSI.VolumeHandle,
		},
		Metadata: cnstypes.CnsVolumeMetadata{
			ContainerCluster:      containerCluster,
			ContainerClusterArray: []cnstypes.CnsContainerCluster{containerCluster},
			EntityMetadata:        metadataList,
		},
	}

	log.Debugf("PVUpdated: Calling UpdateVolumeMetadata for volume %q with updateSpec: %+v", updateSpec.VolumeId.Id, spew.Sdump(updateSpec))
	if err := metadataSyncer.volumeManager.UpdateVolumeMetadata(ctx, updateSpec); err != nil {
		log.Errorf("PVUpdated: UpdateVolumeMetadata failed with err %v", err)
		return
	}
	log.Debugf("PVUpdated: UpdateVolumeMetadata succeed for the volume %q with updateSpec: %+v", updateSpec.VolumeId.Id, spew.Sdump(updateSpec))
}

// csiPVDeleted deletes volume metadata on VC when volume has been deleted on Vanills k8s and supervisor cluster
func csiPVDeleted(ctx context.Context, pv *v1.PersistentVolume, metadataSyncer *metadataSyncInformer) {
	log := logger.GetLogger(ctx)
	var deleteDisk bool
	if pv.Spec.ClaimRef != nil && (pv.Status.Phase == v1.VolumeAvailable || pv.Status.Phase == v1.VolumeReleased) && pv.Spec.PersistentVolumeReclaimPolicy == v1.PersistentVolumeReclaimDelete {
		log.Debugf("PVDeleted: Volume deletion will be handled by Controller")
		return
	}
	volumeOperationsLock.Lock()
	defer volumeOperationsLock.Unlock()

	if pv.Spec.CSI.FSType == common.NfsV4FsType || pv.Spec.CSI.FSType == common.NfsFsType {
		log.Debugf("PVDeleted: vSphere CSI Driver is calling UpdateVolumeMetadata to delete volume metadata references for PV: %q", pv.Name)
		var metadataList []cnstypes.BaseCnsEntityMetadata
		pvMetadata := cnsvsphere.GetCnsKubernetesEntityMetaData(pv.Name, nil, true, string(cnstypes.CnsKubernetesEntityTypePV), "", metadataSyncer.configInfo.Cfg.Global.ClusterID, nil)
		metadataList = append(metadataList, cnstypes.BaseCnsEntityMetadata(pvMetadata))

		containerCluster := cnsvsphere.GetContainerCluster(metadataSyncer.configInfo.Cfg.Global.ClusterID, metadataSyncer.configInfo.Cfg.VirtualCenter[metadataSyncer.host].User, metadataSyncer.clusterFlavor)
		updateSpec := &cnstypes.CnsVolumeMetadataUpdateSpec{
			VolumeId: cnstypes.CnsVolumeId{
				Id: pv.Spec.CSI.VolumeHandle,
			},
			Metadata: cnstypes.CnsVolumeMetadata{
				ContainerCluster:      containerCluster,
				ContainerClusterArray: []cnstypes.CnsContainerCluster{containerCluster},
				EntityMetadata:        metadataList,
			},
		}

		log.Debugf("PVDeleted: Calling UpdateVolumeMetadata for volume %s with updateSpec: %+v", updateSpec.VolumeId.Id, spew.Sdump(updateSpec))
		if err := metadataSyncer.volumeManager.UpdateVolumeMetadata(ctx, updateSpec); err != nil {
			log.Errorf("PVDeleted: UpdateVolumeMetadata failed with err %v", err)
			return
		}
		queryFilter := cnstypes.CnsQueryFilter{
			VolumeIds: []cnstypes.CnsVolumeId{
				{
					Id: pv.Spec.CSI.VolumeHandle,
				},
			},
		}
		queryResult, err := metadataSyncer.volumeManager.QueryVolume(ctx, queryFilter)
		if err != nil {
			log.Errorf("PVDeleted: Failed to query volume metadata for volume %q with error %+v", pv.Spec.CSI.VolumeHandle, err)
			return
		}
		if queryResult != nil && len(queryResult.Volumes) == 1 && len(queryResult.Volumes[0].Metadata.EntityMetadata) == 0 {
			log.Infof("PVDeleted: Volume: %q is not in use by any other entity. Removing CNS tag.", pv.Spec.CSI.VolumeHandle)
			err := metadataSyncer.volumeManager.DeleteVolume(ctx, pv.Spec.CSI.VolumeHandle, false)
			if err != nil {
				log.Errorf("PVDeleted: Failed to delete volume %q with error %+v", pv.Spec.CSI.VolumeHandle, err)
				return
			}
		}

	} else {
		if pv.Spec.ClaimRef == nil || pv.Spec.PersistentVolumeReclaimPolicy != v1.PersistentVolumeReclaimDelete {
			log.Debugf("PVDeleted: Setting DeleteDisk to false")
			deleteDisk = false
		} else {
			// We set delete disk=true for the case where PV status is failed after deletion of pvc
			// In this case, metadatasyncer will remove the volume
			log.Debugf("PVDeleted: Setting DeleteDisk to true")
			deleteDisk = true
		}
		log.Debugf("PVDeleted: vSphere CSI Driver is deleting volume %v with delete disk %v", pv, deleteDisk)
		if err := metadataSyncer.volumeManager.DeleteVolume(ctx, pv.Spec.CSI.VolumeHandle, deleteDisk); err != nil {
			log.Errorf("PVDeleted: Failed to delete disk %s with error %+v", pv.Spec.CSI.VolumeHandle, err)
		}
	}
}

// csiUpdatePod update/deletes pod CnsVolumeMetadata when pod has been created/deleted on Vanilla k8s and supervisor cluster have been updated
func csiUpdatePod(ctx context.Context, pod *v1.Pod, metadataSyncer *metadataSyncInformer, deleteFlag bool) {
	log := logger.GetLogger(ctx)
	// Iterate through volumes attached to pod
	for _, volume := range pod.Spec.Volumes {
		if volume.PersistentVolumeClaim != nil {
			valid, pv, pvc := IsValidVolume(ctx, volume, pod, metadataSyncer)
			if valid {
				var metadataList []cnstypes.BaseCnsEntityMetadata
				var podMetadata *cnstypes.CnsKubernetesEntityMetadata
				if !deleteFlag {
					entityReference := cnsvsphere.CreateCnsKuberenetesEntityReference(string(cnstypes.CnsKubernetesEntityTypePVC), pvc.Name, pvc.Namespace, metadataSyncer.configInfo.Cfg.Global.ClusterID)
					podMetadata = cnsvsphere.GetCnsKubernetesEntityMetaData(pod.Name, nil, deleteFlag, string(cnstypes.CnsKubernetesEntityTypePOD), pod.Namespace, metadataSyncer.configInfo.Cfg.Global.ClusterID, []cnstypes.CnsKubernetesEntityReference{entityReference})
				} else {
					podMetadata = cnsvsphere.GetCnsKubernetesEntityMetaData(pod.Name, nil, deleteFlag, string(cnstypes.CnsKubernetesEntityTypePOD), pod.Namespace, metadataSyncer.configInfo.Cfg.Global.ClusterID, nil)
				}
				metadataList = append(metadataList, cnstypes.BaseCnsEntityMetadata(podMetadata))
				containerCluster := cnsvsphere.GetContainerCluster(metadataSyncer.configInfo.Cfg.Global.ClusterID, metadataSyncer.configInfo.Cfg.VirtualCenter[metadataSyncer.host].User, metadataSyncer.clusterFlavor)

				updateSpec := &cnstypes.CnsVolumeMetadataUpdateSpec{
					VolumeId: cnstypes.CnsVolumeId{
						Id: pv.Spec.CSI.VolumeHandle,
					},
					Metadata: cnstypes.CnsVolumeMetadata{
						ContainerCluster:      containerCluster,
						ContainerClusterArray: []cnstypes.CnsContainerCluster{containerCluster},
						EntityMetadata:        metadataList,
					},
				}

				log.Debugf("Calling UpdateVolumeMetadata for volume %s with updateSpec: %+v", updateSpec.VolumeId.Id, spew.Sdump(updateSpec))
				if err := metadataSyncer.volumeManager.UpdateVolumeMetadata(ctx, updateSpec); err != nil {
					log.Errorf("UpdateVolumeMetadata failed for volume %s with err: %v", volume.Name, err)
				}
			}
		}
	}
}

func initVolumeHealthReconciler(ctx context.Context, tkgKubeClient clientset.Interface, svcKubeClient clientset.Interface) {
	log := logger.GetLogger(ctx)

	log.Infof("initVolumeHealthReconciler is triggered")
	tkgInformerFactory := informers.NewSharedInformerFactory(tkgKubeClient, volumeHealthResyncPeriod)

	// Get the supervisor namespace in which the guest cluster is deployed
	supervisorNamespace, err := cnsconfig.GetSupervisorNamespace(ctx)
	if err != nil {
		log.Errorf("could not get supervisor namespace in which guest cluster was deployed. Err: %v", err)
		return
	}
	log.Infof("supervisorNamespace %s", supervisorNamespace)
	svcInformerFactory := informers.NewSharedInformerFactoryWithOptions(svcKubeClient, volumeHealthResyncPeriod, informers.WithNamespace(supervisorNamespace))

	rc := NewVolumeHealthReconciler(tkgKubeClient, svcKubeClient, volumeHealthResyncPeriod, tkgInformerFactory, svcInformerFactory,
		workqueue.NewItemExponentialFailureRateLimiter(volumeHealthRetryIntervalStart, volumeHealthRetryIntervalMax),
		supervisorNamespace,
	)
	tkgInformerFactory.Start(wait.NeverStop)
	svcInformerFactory.Start(wait.NeverStop)
	rc.Run(ctx, volumeHealthWorkers)
}
