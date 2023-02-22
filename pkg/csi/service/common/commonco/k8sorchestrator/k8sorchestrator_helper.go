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

package k8sorchestrator

import (
	"context"
	"encoding/json"
	"strings"
	"time"

	k8stypes "k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/wait"

	v1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"sigs.k8s.io/vsphere-csi-driver/v3/pkg/csi/service/common"
	"sigs.k8s.io/vsphere-csi-driver/v3/pkg/csi/service/logger"
	csitypes "sigs.k8s.io/vsphere-csi-driver/v3/pkg/csi/types"
)

// getPVCAnnotations fetches annotations from PVC bound to passed volumeID and
// returns annotation key-value pairs as a map.
func (c *K8sOrchestrator) getPVCAnnotations(ctx context.Context, volumeID string) (map[string]string, error) {
	log := logger.GetLogger(ctx)
	log.Debugf("Getting annotations on pvc corresponding to volume: %s", volumeID)
	if pvc := c.volumeIDToPvcMap.get(volumeID); pvc != "" {
		parts := strings.Split(pvc, "/")
		pvcNamespace := parts[0]
		pvcName := parts[1]

		pvcObj, err := c.informerManager.GetPVCLister().PersistentVolumeClaims(pvcNamespace).Get(pvcName)
		if err != nil {
			if apierrors.IsNotFound(err) {
				// PVC may have been deleted.
				log.Debugf("PVC %s is not found in namespace %s using informer manager", pvcName, pvcNamespace)
				return nil, common.ErrNotFound
			}
			log.Errorf("failed to get pvc: %s in namespace: %s. err=%v", pvcName, pvcNamespace, err)
			return nil, err
		}

		return pvcObj.Annotations, nil
	}

	log.Debugf("could not find pvc for volumeID: %s", volumeID)
	return nil, common.ErrNotFound
}

// updatePVCAnnotations updates annotations passed as key-value pairs
// on PVC bound to passed volumeID.
func (c *K8sOrchestrator) updatePVCAnnotations(ctx context.Context,
	volumeID string, annotations map[string]string) error {
	log := logger.GetLogger(ctx)
	if pvc := c.volumeIDToPvcMap.get(volumeID); pvc != "" {
		parts := strings.Split(pvc, "/")
		pvcNamespace := parts[0]
		pvcName := parts[1]

		pvcObj, err := c.informerManager.GetPVCLister().PersistentVolumeClaims(pvcNamespace).Get(pvcName)
		if err != nil {
			if apierrors.IsNotFound(err) {
				// PVC may have been deleted. Return.
				log.Debugf("PVC %s is not found in namespace %s using informer manager", pvcName, pvcNamespace)
				return common.ErrNotFound
			}
			log.Errorf("failed to get pvc: %s in namespace: %s. err=%v", pvcName, pvcNamespace, err)
			return err
		}

		for key, val := range annotations {
			// If value is not set, remove the annotation.
			if val == "" {
				delete(pvcObj.ObjectMeta.Annotations, key)
				log.Debugf("Removing annotation %s on pvc %s/%s", key, pvcNamespace, pvcName)
			} else {
				metav1.SetMetaDataAnnotation(&pvcObj.ObjectMeta, key, val)
				log.Debugf("Updating annotation %s on pvc %s/%s to value: %s", key, pvcNamespace, pvcName, val)
			}
		}
		_, err = c.k8sClient.CoreV1().PersistentVolumeClaims(pvcNamespace).Update(ctx, pvcObj, metav1.UpdateOptions{})
		if err != nil {
			log.Errorf("failed to update pvc annotations %s/%s with err:%+v", pvcNamespace, pvcName, err)
			return err
		}
		return nil
	}

	return logger.LogNewErrorf(log, "could not find pvc for volumeID: %s", volumeID)
}

// isFileVolume checks if the Persistent Volume has ReadWriteMany or
// ReadOnlyMany support.
func isFileVolume(pv *v1.PersistentVolume) bool {
	if len(pv.Spec.AccessModes) == 0 {
		return false
	}
	for _, accessMode := range pv.Spec.AccessModes {
		if accessMode == v1.ReadWriteMany || accessMode == v1.ReadOnlyMany {
			return true
		}
	}
	return false
}

// isValidvSphereVolume returns true if the given PV metadata of a vSphere
// Volume (in-tree volume) and has migrated-to annotation on the PV
func isValidMigratedvSphereVolume(ctx context.Context, pvMetadata metav1.ObjectMeta) bool {
	log := logger.GetLogger(ctx)
	// Checking if the migrated-to annotation is found in the PV metadata.
	if annotation, annMigratedToFound := pvMetadata.Annotations[common.AnnMigratedTo]; annMigratedToFound {
		if annotation == csitypes.Name &&
			pvMetadata.Annotations[common.AnnDynamicallyProvisioned] == common.InTreePluginName {
			log.Debugf("%v annotation found with value %q for PV: %q",
				common.AnnMigratedTo, csitypes.Name, pvMetadata.Name)
			return true
		}
	}
	return false
}

// updateVolumeSnapshotAnnotations updates annotations passed as key-value pairs
// on VolumeSnapshot object
func (c *K8sOrchestrator) updateVolumeSnapshotAnnotations(ctx context.Context,
	volumeSnapshotName string, volumeSnapshotNamespace string,
	volumeSnapshotAnnotations map[string]string) (bool, error) {
	log := logger.GetLogger(ctx)
	retryCount := 0
	interval := time.Second
	limit := 5 * time.Minute
	// TODO: make this configurable
	// Attempt to update the annotation every second for 5minutes
	annotateUpdateErr := wait.PollImmediate(interval, limit, func() (bool, error) {
		retryCount++
		// Retrieve the volume snapshot and verify that it exists
		volumeSnapshot, err := c.snapshotterClient.SnapshotV1().VolumeSnapshots(volumeSnapshotNamespace).
			Get(ctx, volumeSnapshotName, metav1.GetOptions{})
		if err != nil {
			if apierrors.IsNotFound(err) {
				log.Errorf("attempt: %d, the volumesnapshot %s/%s requested to be annotated with %+v is not found",
					retryCount, volumeSnapshotNamespace, volumeSnapshotName, volumeSnapshotAnnotations)
			}
			log.Errorf("attempt: %d, failed to annotate the volumesnapshot %s/%s due to error: %+v",
				retryCount, volumeSnapshotNamespace, volumeSnapshotName, err)
			return false, nil
		}
		patchAnnotation := common.MergeMaps(volumeSnapshot.Annotations, volumeSnapshotAnnotations)

		patch := map[string]interface{}{
			"metadata": map[string]interface{}{
				"annotations": patchAnnotation,
			},
		}
		patchBytes, err := json.Marshal(patch)
		if err != nil {
			log.Errorf("attempt: %d, fail to marshal patch: %+v", retryCount, err)
			return false, nil
		}
		patchedVolumeSnapshot, err := c.snapshotterClient.SnapshotV1().VolumeSnapshots(volumeSnapshotNamespace).
			Patch(ctx, volumeSnapshotName, k8stypes.MergePatchType, patchBytes, metav1.PatchOptions{})
		if err != nil {
			log.Errorf("attempt: %d, failed to patch the volumesnapshot %s/%s with annotation %+v, error: %+v",
				retryCount, volumeSnapshotNamespace, volumeSnapshotName, volumeSnapshotAnnotations, err)
			return false, nil
		}
		log.Infof("attempt: %d, Successfully patched volumesnapshot %s/%s with latest annotations %+v",
			retryCount, patchedVolumeSnapshot.Namespace, patchedVolumeSnapshot.Name,
			patchedVolumeSnapshot.Annotations)
		return true, nil
	})
	if annotateUpdateErr != nil {
		log.Errorf("failed to patch the volumesnapshot %s/%s with annotation %+v, error: %+v",
			volumeSnapshotNamespace, volumeSnapshotName, volumeSnapshotAnnotations, annotateUpdateErr)
		return false, annotateUpdateErr
	}
	return true, nil
}
