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
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	k8stypes "k8s.io/apimachinery/pkg/types"

	"sigs.k8s.io/vsphere-csi-driver/v2/pkg/common/cns-lib/volume"
	cnsvsphere "sigs.k8s.io/vsphere-csi-driver/v2/pkg/common/cns-lib/vsphere"
	"sigs.k8s.io/vsphere-csi-driver/v2/pkg/csi/service/logger"
	"sigs.k8s.io/vsphere-csi-driver/v2/pkg/syncer/k8scloudoperator"
)

// migrationController is responsible for processing CNS volume relocation
// to another StoragePool.
type migrationController struct {
	vc        *cnsvsphere.VirtualCenter
	clusterID string
}

func initMigrationController(vc *cnsvsphere.VirtualCenter, clusterID string) *migrationController {
	migrationCntlr := migrationController{
		vc:        vc,
		clusterID: clusterID,
	}
	return &migrationCntlr
}

func (m *migrationController) relocateCNSVolume(ctx context.Context, volumeID string, targetSPName string) error {
	log := logger.GetLogger(ctx)
	k8sDynamicClient, spResource, err := getSPClient(ctx)
	if err != nil {
		return err
	}
	sp, err := k8sDynamicClient.Resource(*spResource).Get(ctx, targetSPName, metav1.GetOptions{})
	if err != nil {
		return fmt.Errorf("failed to get StoragePool object with name %v", targetSPName)
	}

	datastoreURL, found, err := unstructured.NestedString(sp.Object, "spec", "parameters", "datastoreUrl")
	if !found || err != nil {
		return fmt.Errorf("failed to find datastoreUrl in StoragePool %s", targetSPName)
	}
	dsInfo, err := cnsvsphere.GetDatastoreInfoByURL(ctx, m.vc, m.clusterID, datastoreURL)
	if err != nil {
		return fmt.Errorf("failed to get datastore corressponding to URL %v", datastoreURL)
	}

	volManager := volume.GetManager(ctx, m.vc, nil, false)
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
	taskInfo, err := task.WaitForResult(ctx)
	if err != nil {
		return err
	}
	results := taskInfo.Result.(cnstypes.CnsVolumeOperationBatchResult)
	for _, result := range results.VolumeResults {
		fault := result.GetCnsVolumeOperationResult().Fault
		if fault != nil {
			log.Errorf("Fault: %+v encountered while relocating volume %v", fault, volumeID)
			return fmt.Errorf(fault.LocalizedMessage)
		}
	}
	return nil
}

func (m *migrationController) migrateVolume(ctx context.Context,
	pvc *unstructured.Unstructured) (done bool, err error) {
	log := logger.GetLogger(ctx)
	pvcName := pvc.GetName()
	pvcNamespace := pvc.GetNamespace()

	defer func() {
		_, err := removeTargetSPAnnotationOnPVC(ctx, pvcName, pvcNamespace)
		if err != nil {
			log.Errorf("Failed to remove target SP annotation from PVC %v. Error: %v", pvcName, err)
		}
	}()

	pvResource := schema.GroupVersionResource{Group: "", Version: "v1", Resource: "persistentvolumes"}
	k8sDynamicClient, spResource, err := getSPClient(ctx)
	if err != nil {
		return false, err
	}

	pvName, found, err := unstructured.NestedString(pvc.Object, "spec", "volumeName")
	if !found || err != nil {
		return false, fmt.Errorf(
			"could not get PV name bounded to PVC %v. PV info present in pvc resource: %v. Error: %v",
			pvcName, found, err)
	}
	pv, err := k8sDynamicClient.Resource(pvResource).Get(ctx, pvName, metav1.GetOptions{})
	if err != nil {
		log.Errorf("Failed to get PV resource with name %v. Err: %v", pvName, err)
		return false, err
	}

	volumeID, found, err := unstructured.NestedString(pv.Object, "spec", "csi", "volumeHandle")
	if !found || err != nil {
		return false, fmt.Errorf(
			"failed to get volumeID corresponding to pv %v. VolumeID info present in spec: %v. Error: %v",
			pvName, found, err)
	}
	targetSPName, found, err := unstructured.NestedString(pvc.Object, "metadata", "annotations", targetSPAnnotationKey)
	if !found || err != nil {
		return false, fmt.Errorf(
			"failed to get target StoragePool of PVC %v. target SP name present in annotations: %v. Error: %v",
			pvcName, found, err)
	}
	targetSP, err := k8sDynamicClient.Resource(*spResource).Get(ctx, targetSPName, metav1.GetOptions{})
	if err != nil {
		log.Errorf("Failed to get target StoragePool resource %v. Err: %v", targetSPName, err)
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
	pvcList []*unstructured.Unstructured, abortOnFirstFailure bool) (
	successfulMigrations []*unstructured.Unstructured, unsuccessfulMigrations []*unstructured.Unstructured) {
	log := logger.GetLogger(ctx)
	shouldAbort := false
	successfulMigrations = make([]*unstructured.Unstructured, 0)
	unsuccessfulMigrations = make([]*unstructured.Unstructured, 0)
	for _, pvc := range pvcList {
		pvcName := pvc.GetName()
		pvcNamespace := pvc.GetNamespace()
		if shouldAbort {
			log.Infof("Migration for PVC %v has been aborted.", pvcName)
			_, err := removeTargetSPAnnotationOnPVC(ctx, pvcName, pvcNamespace)
			if err != nil {
				log.Errorf("Failed to remove target SP annotation from PVC %v. Error: %v", pvcName, err)
			}
			unsuccessfulMigrations = append(unsuccessfulMigrations, pvc)
			continue
		}

		// Check if disk decomm. of source StoragePool has been aborted/ terminated.
		sourceSPName, found, err := unstructured.NestedString(pvc.Object, "metadata", "annotations",
			k8scloudoperator.StoragePoolAnnotationKey)
		if !found || err != nil || sourceSPName == "" {
			// Log the error and assume that source SP is under disk decommission.
			log.Warnf("Could not get source StoragePool name for PVC %v. Error: %v", pvcName, err)
		} else {
			drainMode, found, err := getDrainMode(ctx, sourceSPName)
			if (!found || (drainMode != fullDataEvacuationMM && drainMode != ensureAccessibilityMM)) && err == nil {
				log.Infof("Disk decommission of StoragePool %v has been aborted/ terminated. Aborting migration of %v",
					sourceSPName, pvcName)
				_, err := removeTargetSPAnnotationOnPVC(ctx, pvcName, pvcNamespace)
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
	pvcName, pvcNamespace string) (*unstructured.Unstructured, error) {
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
		return nil, err
	}

	pvcResource := schema.GroupVersionResource{Group: "", Version: "v1", Resource: "persistentvolumeclaims"}
	k8sDynamicClient, _, err := getSPClient(ctx)
	if err != nil {
		return nil, err
	}

	updatedPVC, err := k8sDynamicClient.Resource(pvcResource).Namespace(pvcNamespace).Patch(ctx,
		pvcName, k8stypes.MergePatchType, patchBytes, metav1.PatchOptions{})
	if err != nil {
		return nil, err
	}
	log.Debugf("Successfully removed target StoragePool information from %v. Updated PVC: %v",
		pvcName, updatedPVC.GetName())
	return updatedPVC, nil
}
