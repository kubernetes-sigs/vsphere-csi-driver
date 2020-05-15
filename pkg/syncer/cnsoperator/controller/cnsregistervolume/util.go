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

package cnsregistervolume

import (
	"context"
	"errors"
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/google/uuid"
	cnstypes "github.com/vmware/govmomi/cns/types"
	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/fields"
	clientset "k8s.io/client-go/kubernetes"
	"sigs.k8s.io/vsphere-csi-driver/pkg/common/cns-lib/vsphere"
	"sigs.k8s.io/vsphere-csi-driver/pkg/csi/service/common"
	"sigs.k8s.io/vsphere-csi-driver/pkg/csi/service/logger"
	cnsregistervolumev1alpha1 "sigs.k8s.io/vsphere-csi-driver/pkg/syncer/cnsoperator/apis/cnsregistervolume/v1alpha1"
	cnsoperatortypes "sigs.k8s.io/vsphere-csi-driver/pkg/syncer/cnsoperator/types"
)

// isDatastoreAccessibleToCluster verifies if the datastoreUrl is accessible to cluster
// with clusterID.
func isDatastoreAccessibleToCluster(ctx context.Context, vc *vsphere.VirtualCenter,
	clusterID string, datastoreURL string) bool {
	log := logger.GetLogger(ctx)
	candidatedatastores, err := vsphere.GetCandidateDatastoresInCluster(ctx, vc, clusterID)
	if err != nil {
		log.Errorf("Failed to get candidate datastores for cluster: %s with err: %+v", clusterID, err)
		return false
	}
	for _, ds := range candidatedatastores {
		if ds.Info.Url == datastoreURL {
			log.Infof("Found datastoreUrl: %s is accessible to cluster: %s", datastoreURL, clusterID)
			return true
		}
	}
	return false
}

// constructCreateSpecForInstance creates CNS CreateVolume spec
func constructCreateSpecForInstance(r *ReconcileCnsRegisterVolume, instance *cnsregistervolumev1alpha1.CnsRegisterVolume, host string) *cnstypes.CnsVolumeCreateSpec {
	var volumeName string
	if instance.Spec.VolumeID != "" {
		volumeName = staticPvNamePrefix + instance.Spec.VolumeID
	} else {
		id, _ := uuid.NewUUID()
		volumeName = staticPvNamePrefix + id.String()
	}
	containerCluster := vsphere.GetContainerCluster(r.configInfo.Cfg.Global.ClusterID,
		r.configInfo.Cfg.VirtualCenter[host].User,
		cnstypes.CnsClusterFlavorWorkload)
	createSpec := &cnstypes.CnsVolumeCreateSpec{
		Name:       volumeName,
		VolumeType: common.BlockVolumeType,
		Metadata: cnstypes.CnsVolumeMetadata{
			ContainerCluster: containerCluster,
		},
	}
	if instance.Spec.VolumeID != "" {
		createSpec.BackingObjectDetails = &cnstypes.CnsBlockBackingDetails{
			BackingDiskId: instance.Spec.VolumeID,
		}
	} else if instance.Spec.DiskURLPath != "" {
		createSpec.BackingObjectDetails = &cnstypes.CnsBlockBackingDetails{
			BackingDiskUrlPath: instance.Spec.DiskURLPath,
		}
	}
	if instance.Spec.AccessMode == v1.ReadWriteOnce || instance.Spec.AccessMode == "" {
		createSpec.VolumeType = common.BlockVolumeType
	} else {
		createSpec.VolumeType = common.FileVolumeType
	}
	return createSpec
}

// getK8sStorageClassName gets the storage class name in K8S mapping the vsphere
// storagepolicy id.
func getK8sStorageClassName(ctx context.Context, k8sClient clientset.Interface, storagePolicyID string) (string, error) {
	log := logger.GetLogger(ctx)
	scList, err := k8sClient.StorageV1().StorageClasses().List(metav1.ListOptions{})
	if err != nil {
		msg := fmt.Sprintf("Failed to get Storageclasses from API server. Error: %+v", err)
		log.Error(msg)
		return "", errors.New(msg)
	}
	for _, sc := range scList.Items {
		scParams := sc.Parameters
		for paramName, val := range scParams {
			param := strings.ToLower(paramName)
			if param == common.AttributeStoragePolicyID && val == storagePolicyID {
				return sc.Name, nil
			}
		}
	}
	msg := fmt.Sprintf("Failed to find K8s Storageclass mapping storagepolicyId: %s", storagePolicyID)
	return "", errors.New(msg)
}

// getPersistentVolumeSpec to create PV volume spec for the given input params
func getPersistentVolumeSpec(volumeName string, volumeID string,
	capacity int64, accessMode v1.PersistentVolumeAccessMode, scName string, claimRef *v1.ObjectReference) *v1.PersistentVolume {
	capacityInMb := strconv.FormatInt(capacity, 10) + "Mi"
	pv := &v1.PersistentVolume{
		TypeMeta: metav1.TypeMeta{},
		ObjectMeta: metav1.ObjectMeta{
			Name: volumeName,
		},
		Spec: v1.PersistentVolumeSpec{
			PersistentVolumeReclaimPolicy: v1.PersistentVolumeReclaimDelete,
			Capacity: v1.ResourceList{
				v1.ResourceName(v1.ResourceStorage): resource.MustParse(capacityInMb),
			},
			PersistentVolumeSource: v1.PersistentVolumeSource{
				CSI: &v1.CSIPersistentVolumeSource{
					Driver:       cnsoperatortypes.VSphereCSIDriverName,
					VolumeHandle: volumeID,
					ReadOnly:     false,
					FSType:       "ext4",
				},
			},
			AccessModes: []v1.PersistentVolumeAccessMode{
				accessMode,
			},
			ClaimRef:         claimRef,
			StorageClassName: scName,
		},
		Status: v1.PersistentVolumeStatus{},
	}
	annotations := make(map[string]string)
	annotations["pv.kubernetes.io/provisioned-by"] = cnsoperatortypes.VSphereCSIDriverName
	pv.Annotations = annotations
	return pv
}

// getPersistentVolumeClaimSpec return the PersistentVolumeClaim spec with specified storage class
func getPersistentVolumeClaimSpec(name string, namespace string, capacity int64,
	storageClassName string, accessMode v1.PersistentVolumeAccessMode, pvName string) *v1.PersistentVolumeClaim {
	capacityInMb := strconv.FormatInt(capacity, 10) + "Mi"
	claim := &v1.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: namespace,
		},
		Spec: v1.PersistentVolumeClaimSpec{
			AccessModes: []v1.PersistentVolumeAccessMode{
				accessMode,
			},
			Resources: v1.ResourceRequirements{
				Requests: v1.ResourceList{
					v1.ResourceName(v1.ResourceStorage): resource.MustParse(capacityInMb),
				},
			},
			StorageClassName: &storageClassName,
			VolumeName:       pvName,
		},
	}
	return claim
}

// isPVCBound return true if the PVC is bound before timeout, otherwise return false
func isPVCBound(ctx context.Context, client clientset.Interface, claim *v1.PersistentVolumeClaim, timeout time.Duration) (bool, error) {
	log := logger.GetLogger(ctx)
	pvcName := claim.Name
	ns := claim.Namespace
	timeoutSeconds := int64(timeout.Seconds())

	log.Infof("Waiting up to %d seconds for PersistentVolumeClaim %v in namespace %s to have phase %s", timeoutSeconds, pvcName, ns, v1.ClaimBound)
	watchClaim, err := client.CoreV1().PersistentVolumeClaims(ns).Watch(
		metav1.ListOptions{
			FieldSelector:  fields.OneTermEqualSelector("metadata.name", pvcName).String(),
			TimeoutSeconds: &timeoutSeconds,
			Watch:          true,
		})
	if err != nil {
		errMsg := fmt.Errorf("failed to watch PersistentVolumeClaim %s with Error: %v", pvcName, err)
		log.Error(errMsg)
		return false, errMsg
	}
	defer watchClaim.Stop()

	for event := range watchClaim.ResultChan() {
		pvc, ok := event.Object.(*v1.PersistentVolumeClaim)
		if !ok {
			continue
		}
		log.Debugf("PersistentVolumeClaim %s in namespace %s is in state %s. Received event %v", pvcName, ns, pvc.Status.Phase, event)
		if pvc.Status.Phase == v1.ClaimBound && pvc.Name == pvcName {
			log.Infof("PersistentVolumeClaim %s in namespace %s is in state %s", pvcName, ns, pvc.Status.Phase)
			return true, nil
		}
	}
	return false, fmt.Errorf("persistentVolumeClaim %s in namespace %s not in phase %s within %d seconds", pvcName, ns, v1.ClaimBound, timeoutSeconds)
}

// getMaxWorkerThreadsToReconcileCnsRegisterVolume returns the maximum
// number of worker threads which can be run to reconcile CnsRegisterVolume instances.
// If environment variable WORKER_THREADS_REGISTER_VOLUME is set and valid,
// return the value read from environment variable otherwise, use the default value
func getMaxWorkerThreadsToReconcileCnsRegisterVolume(ctx context.Context) int {
	log := logger.GetLogger(ctx)
	workerThreads := defaultMaxWorkerThreadsForRegisterVolume
	if v := os.Getenv("WORKER_THREADS_REGISTER_VOLUME"); v != "" {
		if value, err := strconv.Atoi(v); err == nil {
			if value <= 0 {
				log.Warnf("Maximum number of worker threads to run set in env variable WORKER_THREADS_REGISTER_VOLUME %s is less than 1, will use the default value %d", v, defaultMaxWorkerThreadsForRegisterVolume)
			} else if value > defaultMaxWorkerThreadsForRegisterVolume {
				log.Warnf("Maximum number of worker threads to run set in env variable WORKER_THREADS_REGISTER_VOLUME %s is greater than %d, will use the default value %d",
					v, defaultMaxWorkerThreadsForRegisterVolume, defaultMaxWorkerThreadsForRegisterVolume)
			} else {
				workerThreads = value
				log.Debugf("Maximum number of worker threads to run to reconcile CnsRegisterVolume instances is set to %d", workerThreads)
			}
		} else {
			log.Warnf("Maximum number of worker threads to run set in env variable WORKER_THREADS_REGISTER_VOLUME %s is invalid, will use the default value %d", v, defaultMaxWorkerThreadsForRegisterVolume)
		}
	} else {
		log.Debugf("WORKER_THREADS_REGISTER_VOLUME is not set. Picking the default value %d", defaultMaxWorkerThreadsForRegisterVolume)
	}
	return workerThreads
}
