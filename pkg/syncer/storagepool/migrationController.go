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
	"encoding/json"
	"fmt"
	"reflect"
	"time"

	cnstypes "github.com/vmware/govmomi/cns/types"
	"github.com/vmware/govmomi/vim25/soap"
	vim25types "github.com/vmware/govmomi/vim25/types"
	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
	k8stypes "k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/vsphere-csi-driver/v3/pkg/apis/storagepool/cns/v1alpha1"

	"sigs.k8s.io/vsphere-csi-driver/v3/pkg/common/cns-lib/volume"
	cnsvsphere "sigs.k8s.io/vsphere-csi-driver/v3/pkg/common/cns-lib/vsphere"
	"sigs.k8s.io/vsphere-csi-driver/v3/pkg/common/config"
	"sigs.k8s.io/vsphere-csi-driver/v3/pkg/csi/service/common"
	"sigs.k8s.io/vsphere-csi-driver/v3/pkg/csi/service/common/commonco"
	"sigs.k8s.io/vsphere-csi-driver/v3/pkg/csi/service/logger"
	"sigs.k8s.io/vsphere-csi-driver/v3/pkg/syncer/k8scloudoperator"
)

// migrationController is responsible for processing CNS volume relocation
// to another StoragePool.
type migrationController struct {
	vc         *cnsvsphere.VirtualCenter
	clusterIDs []string
}

func initMigrationController(vc *cnsvsphere.VirtualCenter, clusterIDs []string) *migrationController {
	return &migrationController{
		vc:         vc,
		clusterIDs: clusterIDs,
	}
}

func (m *migrationController) relocateCNSVolume(ctx context.Context, volumeID string, targetSPName string) error {
	log := logger.GetLogger(ctx)
	k8sClient, err := getK8sClient(ctx)
	if err != nil {
		return err
	}

	sp := &v1alpha1.StoragePool{}
	err = k8sClient.Get(ctx, k8stypes.NamespacedName{Name: targetSPName}, sp)
	if err != nil {
		return fmt.Errorf("failed to get StoragePool object with name " + targetSPName)
	}

	datastoreURL, found := sp.Spec.Parameters["datastoreUrl"]
	if !found {
		return fmt.Errorf("failed to find datastoreUrl in StoragePool " + targetSPName)
	}

	dsInfo, err := cnsvsphere.GetDatastoreInfoByURL(ctx, m.vc, m.clusterIDs, datastoreURL)
	if err != nil {
		return fmt.Errorf("failed to get datastore corresponding to URL %v", datastoreURL)
	}

	filterSuspendedDatastores := commonco.ContainerOrchestratorUtility.IsFSSEnabled(ctx, common.CnsMgrSuspendCreateVolume)
	if filterSuspendedDatastores && cnsvsphere.IsVolumeCreationSuspended(ctx, dsInfo) {
		return fmt.Errorf("datastore corresponding to URL %v is suspended and not available for relocating volumes",
			datastoreURL)
	}

	clusterFlavor, err := config.GetClusterFlavor(ctx)
	if err != nil {
		return logger.LogNewErrorf(log, "failed to get cluster flavor. Error: %v", err)
	}
	volManager, err := volume.GetManager(ctx, m.vc, nil, false, false, false, clusterFlavor)
	if err != nil {
		return logger.LogNewErrorf(log, "failed to create an instance of volume manager. err=%v", err)
	}

	relocateSpec := cnstypes.NewCnsBlockVolumeRelocateSpec(volumeID, dsInfo.Reference())

	task, err := volManager.RelocateVolume(ctx, relocateSpec)
	log.Infof("Return from CNS Relocate API, task: %v, Error: %v", task, err)
	if err != nil {
		// Handle case when target DS is same as source DS, i.e. volume has
		// already relocated.
		if soap.IsSoapFault(err) {
			soapFault := soap.ToSoapFault(err)
			log.Debugf("type of fault: %v. SoapFault Info: %v", reflect.TypeOf(soapFault.VimFault()), soapFault)
			_, isAlreadyExistErr := soapFault.VimFault().(vim25types.AlreadyExists)
			if isAlreadyExistErr {
				// Volume already exists in the target SP, hence return success.
				return nil
			}
		}
		return err
	}
	taskInfo, err := task.WaitForResultEx(ctx)
	if err != nil {
		return err
	}
	results := taskInfo.Result.(cnstypes.CnsVolumeOperationBatchResult)
	for _, result := range results.VolumeResults {
		fault := result.GetCnsVolumeOperationResult().Fault
		if fault != nil {
			log.Errorf("Fault: %+v encountered while relocating volume %v", fault, volumeID)
			return fmt.Errorf("fault: %+v", fault.LocalizedMessage)
		}
	}
	return nil
}

func (m *migrationController) migrateVolume(ctx context.Context,
	pvc v1.PersistentVolumeClaim) (done bool, err error) {
	log := logger.GetLogger(ctx)
	pvcName := pvc.GetName()
	pvcNamespace := pvc.GetNamespace()

	defer func() {
		err := removeTargetSPAnnotationOnPVC(ctx, pvcName, pvcNamespace)
		if err != nil {
			log.Errorf("Failed to remove target SP annotation from PVC %v. Error: %v", pvcName, err)
		}
	}()

	k8sClient, err := getK8sClient(ctx)
	if err != nil {
		return false, err
	}

	pvName := pvc.Spec.VolumeName
	if pvName == "" {
		return false, fmt.Errorf(
			"could not find PV from the spec for PVC " + pvcName + " in namespace " + pvc.Namespace)
	}

	pv := &v1.PersistentVolume{}
	err = k8sClient.Get(ctx, k8stypes.NamespacedName{Name: pvName}, pv)
	if err != nil {
		log.Errorf("Failed to get PV resource with name %s. Err: %s", pvName, err)
		return false, err
	}

	if pv.Spec.CSI == nil {
		return false, fmt.Errorf("failed to get volumeID" +
			"as the CSI spec of the PV " + pvName + " is empty")
	}

	volumeID := pv.Spec.CSI.VolumeHandle
	if volumeID == "" {
		return false, fmt.Errorf("failed to get volumeID corresponding to pv " + pvName)
	}

	targetSPName, found := pvc.GetObjectMeta().GetAnnotations()[targetSPAnnotationKey]
	if !found {
		return false, fmt.Errorf(
			"failed to get target StoragePool of PVC %v. target SP name present in annotations: %v. Error: %v",
			pvcName, found, err)
	}

	targetSP := &v1alpha1.StoragePool{}
	err = k8sClient.Get(ctx, k8stypes.NamespacedName{Name: targetSPName}, targetSP)
	if err != nil {
		log.Errorf("Failed to get target StoragePool resource %s. Err: %s", targetSPName, err)
		return false, err
	}

	log.Debugf("Migrating volume %v to SP %v", volumeID, targetSP.GetName())

	// Retry the relocateCNSVolume() if we face connectivity issues with VC.
	relocateFn := func() error {
		return m.relocateCNSVolume(ctx, volumeID, targetSPName)
	}
	initBackoff := time.Duration(100) * time.Millisecond
	maxBackoff := time.Duration(60) * time.Second
	err = RetryOnError(relocateFn, initBackoff, maxBackoff, 1.5, 16)
	if err != nil {
		log.Errorf("Could not migrate PVC %v to StoragePool %v. Error: %v", pvcName, targetSPName, err)
		return false, err
	}

	log.Infof("Successfully migrated pvc %v to StoragePool %v", pvcName, targetSPName)
	// Change storagePool annotations in the PVC.
	_ = updateSourceSPAnnotationOnPVC(ctx, pvcName, pvcNamespace, targetSPName)
	return true, nil
}

// MigrateVolumes given a list of PVC migrates the corresponding volume for
// each PVC to the target SP specified by targetSPAnnotationKey
// (cns.vmware.com/migrate-to-storagepool) annotation. If the annotation is
// not present on the PVC the corresponding migration fails. The function
// returns a tuple consisting of list of PVCs for which migration succeeded
// and a list of PVCs for which migration failed. The migration is performed
// sequentially one by one by calling CNS Relocate API.
//
// If abortOnFirstFailure is true then after encountering first migration
// failure it aborts remaining PVC migrations and adds them to
// unsuccessfulMigrations list.
//
// On successful migration k8scloudoperator.StoragePoolAnnotationKey annotation
// is updated on PVC to reflect new StoragePool targetSPAnnotationKey annotation
// is removed from PVC for both successful and unsuccessful migrations.
func (m *migrationController) MigrateVolumes(ctx context.Context,
	pvcList []v1.PersistentVolumeClaim, abortOnFirstFailure bool) (
	successfulMigrations []v1.PersistentVolumeClaim, unsuccessfulMigrations []v1.PersistentVolumeClaim) {
	log := logger.GetLogger(ctx)
	shouldAbort := false
	successfulMigrations = make([]v1.PersistentVolumeClaim, 0)
	unsuccessfulMigrations = make([]v1.PersistentVolumeClaim, 0)
	for _, pvc := range pvcList {
		pvcName := pvc.GetName()
		pvcNamespace := pvc.GetNamespace()
		if shouldAbort {
			log.Infof("Migration for PVC %v has been aborted.", pvcName)
			err := removeTargetSPAnnotationOnPVC(ctx, pvcName, pvcNamespace)
			if err != nil {
				log.Errorf("Failed to remove target SP annotation from PVC %v. Error: %v", pvcName, err)
			}
			unsuccessfulMigrations = append(unsuccessfulMigrations, pvc)
			continue
		}

		// Check if disk decomm. of source StoragePool has been aborted/ terminated.
		sourceSPName, found := pvc.GetObjectMeta().GetAnnotations()[k8scloudoperator.StoragePoolAnnotationKey]
		if !found || sourceSPName == "" {
			// Log the error and assume that source SP is under disk decommission.
			log.Warn("Could not get source StoragePool name for PVC " + pvcName)
		} else {
			drainMode, found, err := getDrainMode(ctx, sourceSPName)
			if (!found || (drainMode != fullDataEvacuationMM && drainMode != ensureAccessibilityMM)) && err == nil {
				log.Infof("Disk decommission of StoragePool %v has been aborted/ terminated. Aborting migration of %v",
					sourceSPName, pvcName)
				err := removeTargetSPAnnotationOnPVC(ctx, pvcName, pvcNamespace)
				if err != nil {
					log.Errorf("Failed to remove target SP annotation from PVC %v. Error: %v", pvcName, err)
				}
				unsuccessfulMigrations = append(unsuccessfulMigrations, pvc)
				if abortOnFirstFailure {
					shouldAbort = true
				}
				continue
			}
		}

		done, err := m.migrateVolume(ctx, pvc)
		if !done || err != nil {
			log.Errorf("Error while migrating PVC %v. Error: %v", pvcName, err)
			unsuccessfulMigrations = append(unsuccessfulMigrations, pvc)
			if abortOnFirstFailure {
				shouldAbort = true
			}
			continue
		}
		successfulMigrations = append(successfulMigrations, pvc)
	}
	log.Infof("Total number of successful migrations: %v, unsuccessful migrations: %v",
		len(successfulMigrations), len(unsuccessfulMigrations))
	return successfulMigrations, unsuccessfulMigrations
}

func updateSourceSPAnnotationOnPVC(ctx context.Context, pvcName, pvcNamespace, newSourceSPName string) error {
	log := logger.GetLogger(ctx)
	patch := map[string]interface{}{
		"metadata": map[string]interface{}{
			"annotations": map[string]string{
				k8scloudoperator.StoragePoolAnnotationKey: newSourceSPName,
			},
		},
	}
	patchBytes, _ := json.Marshal(patch)

	task := func() (done bool, _ error) {
		pvcResource := schema.GroupVersionResource{Group: "", Version: "v1", Resource: "persistentvolumeclaims"}
		k8sDynamicClient, _, err := getSPClient(ctx)
		if err != nil {
			return false, err
		}

		updatedPVC, err := k8sDynamicClient.Resource(pvcResource).Namespace(pvcNamespace).Patch(ctx,
			pvcName, k8stypes.MergePatchType, patchBytes, metav1.PatchOptions{})
		if err != nil {
			log.Errorf("Failed to update current StoragePool label to %v for pvc %v", newSourceSPName, pvcName)
			return false, err
		}
		log.Debugf("Successfully updated source StoragePool label for PVC %v", updatedPVC.GetName())
		return true, nil
	}
	baseDuration := time.Duration(100) * time.Millisecond
	thresholdDuration := time.Duration(10) * time.Second
	_, err := ExponentialBackoff(task, baseDuration, thresholdDuration, 1.5, 15)
	return err
}

func removeTargetSPAnnotationOnPVC(ctx context.Context,
	pvcName, pvcNamespace string) error {
	log := logger.GetLogger(ctx)
	patch := map[string]interface{}{
		"metadata": map[string]interface{}{
			"annotations": map[string]interface{}{
				targetSPAnnotationKey: nil,
			},
		},
	}
	patchBytes, err := json.Marshal(patch)
	if err != nil {
		return err
	}

	pvcResource := schema.GroupVersionResource{Group: "", Version: "v1", Resource: "persistentvolumeclaims"}
	k8sDynamicClient, _, err := getSPClient(ctx)
	if err != nil {
		return err
	}

	_, err = k8sDynamicClient.Resource(pvcResource).Namespace(pvcNamespace).Patch(ctx,
		pvcName, k8stypes.MergePatchType, patchBytes, metav1.PatchOptions{})
	if err != nil {
		return err
	}

	log.Debugf("Successfully removed target StoragePool information from " + pvcName)
	return nil
}
