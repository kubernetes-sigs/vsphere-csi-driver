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
	cnsregistervolumev1alpha1 "sigs.k8s.io/vsphere-csi-driver/v2/pkg/apis/cnsoperator/cnsregistervolume/v1alpha1"
	"sigs.k8s.io/vsphere-csi-driver/v2/pkg/common/cns-lib/vsphere"
	"sigs.k8s.io/vsphere-csi-driver/v2/pkg/csi/service/common"
	"sigs.k8s.io/vsphere-csi-driver/v2/pkg/csi/service/logger"
	cnsoperatortypes "sigs.k8s.io/vsphere-csi-driver/v2/pkg/syncer/cnsoperator/types"
)

const (
	// Suffix with each storage class resource on the quota.
	// https://kubernetes.io/docs/concepts/policy/resource-quotas/#storage-resource-quota
	scResourceNameSuffix = ".storageclass.storage.k8s.io/requests.storage"
)

// isDatastoreAccessibleToCluster verifies if the datastoreUrl is accessible to
// cluster with clusterID.
func isDatastoreAccessibleToCluster(ctx context.Context, vc *vsphere.VirtualCenter,
	clusterID string, datastoreURL string) bool {
	log := logger.GetLogger(ctx)
	sharedDatastores, _, err := vsphere.GetCandidateDatastoresInCluster(ctx, vc, clusterID)
	if err != nil {
		log.Errorf("Failed to get candidate datastores for cluster: %s with err: %+v", clusterID, err)
		return false
	}
	for _, ds := range sharedDatastores {
		if ds.Info.Url == datastoreURL {
			log.Infof("Found datastoreUrl: %s is accessible to cluster: %s", datastoreURL, clusterID)
			return true
		}
	}
	return false
}

// constructCreateSpecForInstance creates CNS CreateVolume spec.
func constructCreateSpecForInstance(r *ReconcileCnsRegisterVolume,
	instance *cnsregistervolumev1alpha1.CnsRegisterVolume, host string) *cnstypes.CnsVolumeCreateSpec {
	var volumeName string
	if instance.Spec.VolumeID != "" {
		volumeName = staticPvNamePrefix + instance.Spec.VolumeID
	} else {
		id, _ := uuid.NewUUID()
		volumeName = staticPvNamePrefix + id.String()
	}
	containerCluster := vsphere.GetContainerCluster(r.configInfo.Cfg.Global.ClusterID,
		r.configInfo.Cfg.VirtualCenter[host].User,
		cnstypes.CnsClusterFlavorWorkload, r.configInfo.Cfg.Global.ClusterDistribution)
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
// storagepolicy id. The policy must also be assigned to the passed namespace.
func getK8sStorageClassName(ctx context.Context, k8sClient clientset.Interface,
	storagePolicyID string, namespace string) (string, error) {
	log := logger.GetLogger(ctx)
	scList, err := k8sClient.StorageV1().StorageClasses().List(ctx, metav1.ListOptions{})
	if err != nil {
		return "", logger.LogNewErrorf(log, "Failed to get Storageclasses from API server. Error: %+v", err)
	}
	var scName string
	for _, sc := range scList.Items {
		scParams := sc.Parameters
		for paramName, val := range scParams {
			param := strings.ToLower(paramName)
			if param == common.AttributeStoragePolicyID && val == storagePolicyID {
				scName = sc.Name
				break // Only one storage class in a cluster with a given policy ID.
			}
		}
	}

	/*
		Resource Quotas
			Name:                                                                   <namespace>-storagequota
			Resource                                                                Used  Hard
			--------                                                                ---   ---
			<storage-class-name>.storageclass.storage.k8s.io/requests.storage  		0     5Gi
	*/
	quotaList, err := k8sClient.CoreV1().ResourceQuotas(namespace).List(ctx, metav1.ListOptions{})
	if err != nil {
		return "", logger.LogNewErrorf(log, "Failed to get resource quotas on the namespace: %s", namespace)
	}

	if scName != "" && len(quotaList.Items) > 0 {
		for _, quota := range quotaList.Items {
			// Looping over each named resource in the storage quota to check if
			// it matches the storage class.
			for resource := range quota.Spec.Hard {
				if scName+scResourceNameSuffix == resource.String() {
					log.Debugf("Found k8s storage class: %s with storagePolicyId: %s and "+
						"the policy is assigned to namespace: %s", scName, storagePolicyID, namespace)
					return scName, nil
				}
			}
		}
	}

	return "", logger.LogNewErrorf(log, "Failed to find matching K8s Storageclass. "+
		"Either storagepolicyId: %s doesn't match any storage class, or the policy is not assigned to namespace: %s",
		storagePolicyID, namespace)
}

// getPersistentVolumeSpec to create PV volume spec for the given input params.
func getPersistentVolumeSpec(volumeName string, volumeID string, capacity int64,
	accessMode v1.PersistentVolumeAccessMode, scName string, claimRef *v1.ObjectReference) *v1.PersistentVolume {
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

// getPersistentVolumeClaimSpec return the PersistentVolumeClaim spec with
// specified storage class.
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

// isPVCBound return true if the PVC is bound before timeout.
// Otherwise, return false.
func isPVCBound(ctx context.Context, client clientset.Interface, claim *v1.PersistentVolumeClaim,
	timeout time.Duration) (bool, error) {
	log := logger.GetLogger(ctx)
	pvcName := claim.Name
	ns := claim.Namespace
	timeoutSeconds := int64(timeout.Seconds())

	log.Infof("Waiting up to %d seconds for PersistentVolumeClaim %v in namespace %s to have phase %s",
		timeoutSeconds, pvcName, ns, v1.ClaimBound)
	watchClaim, err := client.CoreV1().PersistentVolumeClaims(ns).Watch(
		ctx,
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
		log.Debugf("PersistentVolumeClaim %s in namespace %s is in state %s. Received event %v",
			pvcName, ns, pvc.Status.Phase, event)
		if pvc.Status.Phase == v1.ClaimBound && pvc.Name == pvcName {
			log.Infof("PersistentVolumeClaim %s in namespace %s is in state %s", pvcName, ns, pvc.Status.Phase)
			return true, nil
		}
	}
	return false, fmt.Errorf("persistentVolumeClaim %s in namespace %s not in phase %s within %d seconds",
		pvcName, ns, v1.ClaimBound, timeoutSeconds)
}

// getMaxWorkerThreadsToReconcileCnsRegisterVolume returns the maximum number
// of worker threads which can be run to reconcile CnsRegisterVolume instances.
// If environment variable WORKER_THREADS_REGISTER_VOLUME is set and valid,
// return the value read from environment variable. Otherwise, use the default
// value.
func getMaxWorkerThreadsToReconcileCnsRegisterVolume(ctx context.Context) int {
	log := logger.GetLogger(ctx)
	workerThreads := defaultMaxWorkerThreadsForRegisterVolume
	if v := os.Getenv("WORKER_THREADS_REGISTER_VOLUME"); v != "" {
		if value, err := strconv.Atoi(v); err == nil {
			if value <= 0 {
				log.Warnf("Maximum number of worker threads to run set in env variable "+
					"WORKER_THREADS_REGISTER_VOLUME %s is less than 1, will use the default value %d",
					v, defaultMaxWorkerThreadsForRegisterVolume)
			} else if value > defaultMaxWorkerThreadsForRegisterVolume {
				log.Warnf("Maximum number of worker threads to run set in env variable "+
					"WORKER_THREADS_REGISTER_VOLUME %s is greater than %d, will use the default value %d",
					v, defaultMaxWorkerThreadsForRegisterVolume, defaultMaxWorkerThreadsForRegisterVolume)
			} else {
				workerThreads = value
				log.Debugf("Maximum number of worker threads to run to reconcile CnsRegisterVolume instances is set to %d",
					workerThreads)
			}
		} else {
			log.Warnf("Maximum number of worker threads to run set in env variable "+
				"WORKER_THREADS_REGISTER_VOLUME %s is invalid, will use the default value %d",
				v, defaultMaxWorkerThreadsForRegisterVolume)
		}
	} else {
		log.Debugf("WORKER_THREADS_REGISTER_VOLUME is not set. Picking the default value %d",
			defaultMaxWorkerThreadsForRegisterVolume)
	}
	return workerThreads
}
