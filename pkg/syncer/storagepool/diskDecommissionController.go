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

package storagepool

import (
	"context"
	"fmt"

	"github.com/vmware/govmomi/object"
	vimtypes "github.com/vmware/govmomi/vim25/types"
	"golang.org/x/sync/semaphore"
	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/watch"
	"k8s.io/client-go/dynamic"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/vsphere-csi-driver/v3/pkg/apis/storagepool/cns/v1alpha1"

	"sigs.k8s.io/vsphere-csi-driver/v3/pkg/common/cns-lib/volume"
	"sigs.k8s.io/vsphere-csi-driver/v3/pkg/common/cns-lib/vsphere"
	"sigs.k8s.io/vsphere-csi-driver/v3/pkg/common/config"
	"sigs.k8s.io/vsphere-csi-driver/v3/pkg/csi/service/logger"
	"sigs.k8s.io/vsphere-csi-driver/v3/pkg/csi/service/wcp"
	csitypes "sigs.k8s.io/vsphere-csi-driver/v3/pkg/csi/types"
	k8s "sigs.k8s.io/vsphere-csi-driver/v3/pkg/kubernetes"
	"sigs.k8s.io/vsphere-csi-driver/v3/pkg/syncer/k8scloudoperator"
)

const (
	drainModeField        = "decommMode"
	drainStatusField      = "status"
	drainFailReasonField  = "reason"
	drainSuccessStatus    = "done"
	drainFailStatus       = "fail"
	noMigrationMM         = "noAction"
	ensureAccessibilityMM = "ensureAccessibility"
	fullDataEvacuationMM  = "evacuateAll"
	targetSPAnnotationKey = spTypePrefix + "migrate-to-storagepool"
	vmUUIDAnnotationKey   = "vmware-system-vm-uuid"
)

// DiskDecommController is responsible for watching and processing disk
// decommission request.
type DiskDecommController struct {
	migrationCntlr   *migrationController
	k8sClient        client.Client
	k8sDynamicClient dynamic.Interface
	spResource       *schema.GroupVersionResource
	pvResource       *schema.GroupVersionResource
	pvcResource      *schema.GroupVersionResource
	spWatch          watch.Interface
	// Stores the current disk decommission mode ("ensureAccessibility"/
	// "evacuateAll"/none) of a SP to evaluate whether or not a new event is a
	// request for disk decommissioning of a SP. Keys are SP name and values
	// are disk decomm mode.
	diskDecommMode map[string]string
	// 1 weighted semaphore to make sure only one disk decomm request is being
	// executed.
	execSemaphore *semaphore.Weighted
}

// detachVolumes detaches all the volumes present in the specified StoragePool
// from corresponding PodVM.
// XXX: Use lister and informers if these operations become too expensive.
func (w *DiskDecommController) detachVolumes(ctx context.Context, storagePoolName string) error {
	log := logger.GetLogger(ctx)
	k8sClient, err := k8s.NewClient(ctx)
	if err != nil {
		log.Errorf("Creating Kubernetes client failed. Err: %v", err)
		return err
	}

	// Shallow copy VC to prevent nil pointer dereference exception caused due to
	// vc.Disconnect func running in parallel.
	vc := *w.migrationCntlr.vc
	err = vc.Connect(ctx)
	if err != nil {
		log.Errorf("failed to connect to vCenter. Err: %+v", err)
		return err
	}

	dc := &vsphere.Datacenter{
		Datacenter: object.NewDatacenter(vc.Client.Client,
			vimtypes.ManagedObjectReference{
				Type:  "Datacenter",
				Value: vc.Config.DatacenterPaths[0],
			}),
		VirtualCenterHost: vc.Config.Host,
	}

	clusterFlavor, err := config.GetClusterFlavor(ctx)
	if err != nil {
		return logger.LogNewErrorf(log, "failed to get cluster flavor. Error: %v", err)
	}
	volManager, err := volume.GetManager(ctx, &vc, nil, false, false, false, clusterFlavor)
	if err != nil {
		return logger.LogNewErrorf(log, "failed to create an instance of volume manager. err=%v", err)
	}

	volumes, _, err := k8scloudoperator.GetVolumesOnStoragePool(ctx, k8sClient, storagePoolName)
	if err != nil {
		log.Errorf("Failed to get the list of volumes on StoragePool %v. Err: %v", storagePoolName, err)
		return err
	}

	pods, err := k8sClient.CoreV1().Pods(v1.NamespaceAll).List(ctx, metav1.ListOptions{})
	if err != nil {
		log.Errorf("Failed to get the list of pods in all namespaces. Err: %v", err)
		return err
	}

	for _, vol := range volumes {
		pv, err := k8sClient.CoreV1().PersistentVolumes().Get(ctx, vol.PVName, metav1.GetOptions{})
		if err != nil {
			log.Warnf("Failed to get pv bounded to PVC %v", vol.PVC.Name)
			return err
		}

		volumeID := pv.Spec.CSI.VolumeHandle
		if volumeID == "" {
			log.Warnf("Failed to get volumeID corresponding to PV %v", vol.PVName)
			return fmt.Errorf("failed to get volumeID corresponding to PV %v", vol.PVName)
		}

		for _, pod := range pods.Items {
			for _, podAttachedVol := range pod.Spec.Volumes {
				if podAttachedVol.PersistentVolumeClaim != nil &&
					podAttachedVol.PersistentVolumeClaim.ClaimName == vol.PVC.Name {
					vmUUID := pod.Annotations[vmUUIDAnnotationKey]
					if vmUUID == "" {
						log.Infof("PodVM corresponding to pod %v might not be created yet. Skipping detach operation",
							pod.Name)
						continue
					}
					vm, err := dc.GetVirtualMachineByUUID(ctx, vmUUID, true)
					if err != nil {
						log.Errorf("Could not get VM object from VM UUID: %v. Err: %v", vmUUID, err)
						return err
					}
					log.Debugf("vSphere CSI driver is detaching volume: %s from vm: %s", volumeID, vm.InventoryPath)
					// It does not throw error if disk is already detached.
					_, err = volManager.DetachVolume(ctx, vm, volumeID)
					if err != nil {
						log.Errorf("failed to detach volume %s with err %+v", volumeID, err)
						return err
					}
					log.Debugf("Successfully detached volume %s from VM %v.", volumeID, vm)
					break
				}
			}
		}
	}
	return nil
}

// DecommissionDisk is responsible for making progress on disk decommission
// request. It does so by getting SvMotion plan from placement engine,
// persisting the migration plan through PVC objects and and passing this info
// to migration controller which migrates the volume to other local host
// attached disk.
func (w *DiskDecommController) DecommissionDisk(ctx context.Context, storagePoolName string, maintenanceMode string) {
	log := logger.GetLogger(ctx)
	// Make sure only 1 DecommissionDisk func is executing for a StoragePool.
	_ = w.execSemaphore.Acquire(ctx, 1)
	defer w.execSemaphore.Release(1)
	migrationFailed := false
	for {
		if migrationFailed {
			errorString := fmt.Sprintf("Fail to migrate all volumes from StoragePool %v", storagePoolName)
			err := updateDrainStatus(ctx, storagePoolName, drainFailStatus, errorString)
			if err != nil {
				log.Errorf("Failed to update drain status to '%v'. Error: %v", drainFailStatus, err)
			}
			return
		}
		// Get drain label of storagePool.
		_, found, _ := getDrainMode(ctx, storagePoolName)
		if !found {
			log.Infof("Disk decommission of StoragePool %v has been aborted/ terminated", storagePoolName)
			return
		}

		if maintenanceMode == noMigrationMM {
			err := w.detachVolumes(ctx, storagePoolName)
			if err != nil {
				log.Errorf("Failed to unmount volumes on StoragePool %v. Error: %v", storagePoolName, err)

				errorString := fmt.Sprintf("Fail to detach all volumes on StoragePool %v", storagePoolName)
				err := updateDrainStatus(ctx, storagePoolName, drainFailStatus, errorString)
				if err != nil {
					log.Errorf("Failed to update drain status to '%v'. Error: %v", drainFailStatus, err)
				}
				return
			}

			log.Infof("Successfully decommission disk %v with MM %v", storagePoolName, maintenanceMode)
			err = updateDrainStatus(ctx, storagePoolName, drainSuccessStatus, "")
			if err != nil {
				log.Errorf("Failed to update drain label of %v to %v. Error: %v", storagePoolName, drainSuccessStatus, err)
			}
			return
		}

		svMotionPlan, err := wcp.GetsvMotionPlanFromK8sCloudOperatorService(ctx, storagePoolName, maintenanceMode)
		if err != nil {
			msg := fmt.Sprintf("Failed to decommission disk. Error: %+v", err)
			err := updateDrainStatus(ctx, storagePoolName, drainFailStatus, msg)
			if err != nil {
				log.Errorf("Failed to update drain status to %v. Error: %v", drainFailStatus, err)
			}
			return
		}
		if len(svMotionPlan) == 0 {
			log.Infof("Successfully decommission disk %v", storagePoolName)
			err := updateDrainStatus(ctx, storagePoolName, drainSuccessStatus, "")
			if err != nil {
				log.Errorf("Failed to update drain label of %v to %v. Error: %v", storagePoolName, drainSuccessStatus, err)
			}
			return
		}

		pvcToMigrate := make([]v1.PersistentVolumeClaim, 0)
		for pvName, targetSPName := range svMotionPlan {
			pv := &v1.PersistentVolume{}
			err := w.k8sClient.Get(ctx, types.NamespacedName{Name: pvName}, pv)
			if err != nil {
				log.Errorf("Failed to get PV resource %v. Error: %v", pvName, err)
				migrationFailed = true
				break
			}

			if pv.Spec.ClaimRef == nil ||
				pv.Spec.ClaimRef.Name == "" ||
				pv.Spec.ClaimRef.Namespace == "" {
				log.Errorf("Failed to get PVC bounded to PV %v. Error: %v", pvName, err)
				migrationFailed = true
				break
			}

			pvcName := pv.Spec.ClaimRef.Namespace
			namespace := pv.Spec.ClaimRef.Namespace
			err = addTargetSPAnnotationOnPVC(ctx, pvcName, namespace, targetSPName)
			if err != nil {
				log.Errorf("Failed to add target SP annotation to PVC %s. Error: %s", pvcName, err)
				migrationFailed = true
				break
			}

			pvc := &v1.PersistentVolumeClaim{}
			err = w.k8sClient.Get(ctx, types.NamespacedName{
				Name:      pvcName,
				Namespace: namespace,
			}, pvc)
			if err != nil {
				log.Errorf("Unable to get the PVC %s in namespace %s. Error: %s",
					pvcName, namespace, err)
				migrationFailed = false
				break
			}

			pvcToMigrate = append(pvcToMigrate, *pvc)
		}

		_, unsuccessfulMigrations := w.migrationCntlr.MigrateVolumes(ctx, pvcToMigrate, true)
		if len(unsuccessfulMigrations) != 0 {
			migrationFailed = true
		}
	}
}

func initDiskDecommController(ctx context.Context, migrationCntlr *migrationController) (*DiskDecommController, error) {
	log := logger.GetLogger(ctx)
	log.Infof("Starting disk decommission controller")
	k8sDynamicClient, spResource, err := getSPClient(ctx)
	if err != nil {
		return nil, err
	}

	k8sClient, err := getK8sClient(ctx)
	if err != nil {
		return nil, err
	}

	w := &DiskDecommController{}
	w.k8sDynamicClient = k8sDynamicClient
	w.k8sClient = k8sClient
	w.migrationCntlr = migrationCntlr
	w.spResource = spResource
	w.pvResource = &schema.GroupVersionResource{Group: "", Version: "v1", Resource: "persistentvolumes"}
	w.pvcResource = &schema.GroupVersionResource{Group: "", Version: "v1", Resource: "persistentvolumeclaims"}
	w.diskDecommMode = make(map[string]string)
	w.execSemaphore = semaphore.NewWeighted(1)

	// Get all the pvc resource for which targetSPAnnotationKey annotations
	// exists. This will give us list of all pending migrations.

	pvcList := &v1.PersistentVolumeClaimList{}
	err = w.k8sClient.List(ctx, pvcList)
	if err != nil {
		return w, err
	}

	var pvcToMigrate []v1.PersistentVolumeClaim
	for _, pvc := range pvcList.Items {
		if pvc.ObjectMeta.Annotations != nil &&
			pvc.ObjectMeta.Annotations[targetSPAnnotationKey] != "" {
			pvcToMigrate = append(pvcToMigrate, pvc)
		}
	}

	w.migrationCntlr.MigrateVolumes(ctx, pvcToMigrate, false)

	// Start StoragePool watch to look for events putting SP under disk
	// decommission.
	err = w.renewStoragePoolWatch(ctx)
	if err != nil {
		return w, err
	}
	go w.watchStoragePool(ctx)

	// Get all sp resource.
	spList := &v1alpha1.StoragePoolList{}
	err = w.k8sClient.List(ctx, spList)
	if err != nil {
		return w, err
	}

	// Make progress on StoragePool which are under disk decommission.
	for _, sp := range spList.Items {
		spName := sp.GetName()
		if w.shouldEnterDiskDecommission(ctx, sp) && spName != "" {
			maintenanceMode := w.diskDecommMode[spName]
			go w.DecommissionDisk(ctx, spName, maintenanceMode)
		}
	}
	return w, nil
}

// As our watch can and will expire, we need a helper to renew it. Note that
// after we re-new it, we will get a bunch of already processed events.
func (w *DiskDecommController) renewStoragePoolWatch(ctx context.Context) error {
	log := logger.GetLogger(ctx)
	spClient, spResource, err := getSPClient(ctx)
	if err != nil {
		return err
	}
	// This means every 24h our watch may expire and require to be re-created.
	// When that happens, we may need to do a full remediation, hence we change
	// from 30m (default) to 24h.
	timeout := int64(60 * 60 * 24) // 24 hours.
	// The below watch returns `unstructured` events which is something we are trying to avoid.
	// TODO: check if there is a way to created a watch using StoragePool type
	w.spWatch, err = spClient.Resource(*spResource).Watch(ctx, metav1.ListOptions{
		TimeoutSeconds: &timeout,
	})
	if err != nil {
		log.Errorf("Failed to start StoragePool watch. Error: %v", err)
		return err
	}
	w.k8sDynamicClient = spClient
	k8sClient, err := getK8sClient(ctx)
	if err != nil {
		return err
	}
	w.k8sClient = k8sClient
	return nil
}

// watchStoragePool looks for event putting a SP under disk decommission. It
// does so by storing the current drain label value for each StoragePool. Once
// it gets an event which updates the drain label (established by comparing
// stored drain label value with new one) of a SP to ensureAccessibilityMM/
// fullDataEvacuationMM/noMigrationMM it invokes the func to process disk
// decommossion of that storage pool.
func (w *DiskDecommController) watchStoragePool(ctx context.Context) {
	log := logger.GetLogger(ctx)
	done := false
	for !done {
		select {
		case <-ctx.Done():
			log.Info("StoragePool watch shutdown", "ctxErr", ctx.Err())
			done = true
		case e, ok := <-w.spWatch.ResultChan():
			if !ok {
				log.Info("StoragePool watch not ok")
				err := w.renewStoragePoolWatch(ctx)
				for err != nil {
					err = w.renewStoragePoolWatch(ctx)
				}
				continue
			}
			event, ok := e.Object.(*unstructured.Unstructured)
			if !ok {
				log.Warnf("Object in StoragePool watch event is not of type *unstructured.Unstructured, but of type %T",
					e.Object)
				continue
			}

			spName := event.GetName()
			sp := &v1alpha1.StoragePool{}
			err := w.k8sClient.Get(ctx, types.NamespacedName{Name: spName}, sp)
			if err != nil {

			}
			if ok := w.shouldEnterDiskDecommission(ctx, *sp); ok {
				maintenanceMode := w.diskDecommMode[spName]
				log.Infof("Got enter disk decommission request for StoragePool %v with MM %v", spName, maintenanceMode)
				go w.DecommissionDisk(ctx, spName, maintenanceMode)
			}
		}
	}
	log.Info("watchStoragePool ends")
}

func (w *DiskDecommController) shouldEnterDiskDecommission(ctx context.Context, sp v1alpha1.StoragePool) bool {
	log := logger.GetLogger(ctx)
	if sp.Spec.Driver != csitypes.Name {
		log.Warnf("StoragePool watch event for %s does not correspond to %s driver.", sp.Name, csitypes.Name)
		return false
	}

	drainMode, found := sp.Spec.Parameters[drainModeField]
	defer func() {
		if !found {
			delete(w.diskDecommMode, sp.Name)
		} else {
			w.diskDecommMode[sp.Name] = drainMode
		}
	}()
	if (drainMode == fullDataEvacuationMM || drainMode == ensureAccessibilityMM || drainMode == noMigrationMM) &&
		drainMode != w.diskDecommMode[sp.Name] {
		// Check if status field is already populated.
		drainStatus := sp.Status.DiskDecomm[drainStatusField]
		if drainStatus != drainFailStatus && drainStatus != drainSuccessStatus {
			return true
		}
	}
	return false
}
