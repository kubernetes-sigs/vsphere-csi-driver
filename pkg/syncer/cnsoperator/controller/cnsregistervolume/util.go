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
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/google/uuid"
	cnstypes "github.com/vmware/govmomi/cns/types"
	v1 "k8s.io/api/core/v1"
	storagev1 "k8s.io/api/storage/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/fields"
	clientset "k8s.io/client-go/kubernetes"
	ctrlruntimeclient "sigs.k8s.io/controller-runtime/pkg/client"
	cnsregistervolumev1alpha1 "sigs.k8s.io/vsphere-csi-driver/v3/pkg/apis/cnsoperator/cnsregistervolume/v1alpha1"
	cnsstoragepolicyquotasv1alpha2 "sigs.k8s.io/vsphere-csi-driver/v3/pkg/apis/cnsoperator/storagepolicy/v1alpha2"
	"sigs.k8s.io/vsphere-csi-driver/v3/pkg/common/cns-lib/vsphere"
	"sigs.k8s.io/vsphere-csi-driver/v3/pkg/csi/service/common"
	"sigs.k8s.io/vsphere-csi-driver/v3/pkg/csi/service/common/commonco"
	"sigs.k8s.io/vsphere-csi-driver/v3/pkg/csi/service/logger"
	cnsoperatortypes "sigs.k8s.io/vsphere-csi-driver/v3/pkg/syncer/cnsoperator/types"
)

const (
	// Suffix with each storage class resource on the quota.
	// https://kubernetes.io/docs/concepts/policy/resource-quotas/#storage-resource-quota
	scResourceNameSuffix = ".storageclass.storage.k8s.io/requests.storage"
)

var (
	// Supported data source types for PVCs
	supportedDataSourceTypes = []struct {
		apiGroup string
		kind     string
	}{
		{"vmoperator.vmware.com", "VirtualMachine"},
	}
)

// isDatastoreAccessibleToCluster verifies if the datastoreUrl is accessible to
// cluster with clusterID.
func isDatastoreAccessibleToCluster(ctx context.Context, vc *vsphere.VirtualCenter,
	clusterID string, datastoreURL string) bool {
	log := logger.GetLogger(ctx)
	sharedDatastores, _, err := vsphere.GetCandidateDatastoresInCluster(ctx, vc, clusterID, false)
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

// isDatastoreAccessibleToAZClusters verifies if the datastoreUrl is accessible to
// any of the az cluster for the given azClustersMap.
func isDatastoreAccessibleToAZClusters(ctx context.Context, vc *vsphere.VirtualCenter,
	azClustersMap map[string][]string, datastoreURL string) bool {
	log := logger.GetLogger(ctx)
	for _, clusterIDs := range azClustersMap {
		var found bool
		for _, clusterID := range clusterIDs {
			sharedDatastores, _, err := vsphere.GetCandidateDatastoresInCluster(ctx, vc, clusterID, false)
			if err != nil {
				log.Warnf("Failed to get candidate datastores for cluster: %s with err: %+v", clusterID, err)
				continue
			}
			found = false
			for _, ds := range sharedDatastores {
				if ds.Info.Url == datastoreURL {
					log.Infof("Found datastoreUrl: %s is accessible to cluster: %s", datastoreURL, clusterID)
					found = true
				}
			}
			// If datastoreURL was found in the list of datastores accessible to the
			// cluster with clusterID, continue checking for the rest of the clusters
			// in AZ. Otherwise, break and check the next AZ in azClustersMap.
			if !found {
				break
			}
		}
		// datastoreURL was found in all the clusters with clusterIDs.
		if found {
			return true
		}
	}
	return false
}

// constructCreateSpecForInstance creates CNS CreateVolume spec.
func constructCreateSpecForInstance(ctx context.Context, r *ReconcileCnsRegisterVolume,
	instance *cnsregistervolumev1alpha1.CnsRegisterVolume,
	host string, useSupervisorId bool) *cnstypes.CnsVolumeCreateSpec {
	var volumeName string
	if instance.Spec.VolumeID != "" {
		volumeName = staticPvNamePrefix + instance.Spec.VolumeID
	} else {
		id, _ := uuid.NewUUID()
		volumeName = staticPvNamePrefix + id.String()
	}
	var clusterIDForVolumeMetadata string
	if useSupervisorId {
		clusterIDForVolumeMetadata = r.configInfo.Cfg.Global.SupervisorID
	} else {
		clusterIDForVolumeMetadata = r.configInfo.Cfg.Global.ClusterID
	}
	containerCluster := vsphere.GetContainerCluster(clusterIDForVolumeMetadata,
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

	createSpec.VolumeType = common.BlockVolumeType

	return createSpec
}

// getK8sStorageClassNameWithImmediateBindingModeForPolicy gets the storage class name in K8S mapping the vsphere
// storagepolicy id. The policy must also be assigned to the passed namespace.
func getK8sStorageClassNameWithImmediateBindingModeForPolicy(ctx context.Context, k8sClient clientset.Interface,
	client ctrlruntimeclient.Client, storagePolicyID string, namespace string,
	isPodVMOnStretchedSupervisorEnabled bool) (string, error) {
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
				if *sc.VolumeBindingMode != storagev1.VolumeBindingWaitForFirstConsumer {
					scName = sc.Name
					break
				}
			}
		}
	}

	if !isPodVMOnStretchedSupervisorEnabled {
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
	} else {
		storagePolicyQuotaList := &cnsstoragepolicyquotasv1alpha2.StoragePolicyQuotaList{}
		err := client.List(ctx, storagePolicyQuotaList, &ctrlruntimeclient.ListOptions{
			Namespace: namespace,
		})
		if err != nil {
			return "", logger.LogNewErrorf(log, "Failed to list StoragePolicyQuota CR on the namespace: %s", namespace)
		}
		log.Debugf("Found scName %s which has matching storagePolicyId %s", scName, storagePolicyID)
		log.Debugf("Fetch storagePolicyQuotaList: %+v  in namespace %s", storagePolicyQuotaList, namespace)
		foundMatchStoragePolicyQuotaCR := false
		matchedStoragePolicyQuotaCR := cnsstoragepolicyquotasv1alpha2.StoragePolicyQuota{}
		if scName != "" && len(storagePolicyQuotaList.Items) > 0 {
			for _, storagePolicyQuota := range storagePolicyQuotaList.Items {
				if storagePolicyQuota.Spec.StoragePolicyId == storagePolicyID {
					log.Debugf("Found storagePlicyQuota CR with matching storagePolicyId:%s in namespaces:%s",
						storagePolicyID, namespace)
					foundMatchStoragePolicyQuotaCR = true
					matchedStoragePolicyQuotaCR = storagePolicyQuota
					break
				}
			}
			// NOTE: Below code expects StoragePolicyQuota CR status will be populated with all fields with zero values from the
			// start (to be taken care by quota controller), otherwise this code will always return error.
			if foundMatchStoragePolicyQuotaCR && len(matchedStoragePolicyQuotaCR.Status.SCLevelQuotaStatuses) > 0 {
				for _, quota := range matchedStoragePolicyQuotaCR.Status.SCLevelQuotaStatuses {
					if quota.StorageClassName == scName {
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
}

// getPersistentVolumeSpec to create PV volume spec for the given input params.
func getPersistentVolumeSpec(volumeName string, volumeID string, capacity int64,
	accessMode v1.PersistentVolumeAccessMode, volumeMode v1.PersistentVolumeMode, scName string,
	claimRef *v1.ObjectReference) *v1.PersistentVolume {
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

	if isSharedDiskEnabled {
		if volumeMode == "" {
			// For both RWO and RWX volumes, default volumeMode is Filesystem.
			volumeMode = v1.PersistentVolumeFilesystem
		}
		pv.Spec.VolumeMode = &volumeMode
	}

	annotations := make(map[string]string)
	annotations["pv.kubernetes.io/provisioned-by"] = cnsoperatortypes.VSphereCSIDriverName
	pv.Annotations = annotations
	return pv
}

// getPersistentVolumeClaimSpec return the PersistentVolumeClaim spec with
// specified storage class.
func getPersistentVolumeClaimSpec(ctx context.Context, name string, namespace string, capacity int64,
	storageClassName string, accessMode v1.PersistentVolumeAccessMode, volumeMode v1.PersistentVolumeMode,
	pvName string,
	datastoreAccessibleTopology []map[string]string,
	instance *cnsregistervolumev1alpha1.CnsRegisterVolume) (*v1.PersistentVolumeClaim, error) {

	log := logger.GetLogger(ctx)
	capacityInMb := strconv.FormatInt(capacity, 10) + "Mi"

	var (
		segmentsArray  []string
		pvcLabels      = make(map[string]string)
		topoAnnotation = make(map[string]string)
	)
	if datastoreAccessibleTopology != nil {
		for _, topologyTerm := range datastoreAccessibleTopology {
			jsonSegment, err := json.Marshal(topologyTerm)
			if err != nil {
				return nil, logger.LogNewErrorf(log,
					"failed to marshal topology segment: %+v to json. Error: %+v", topologyTerm, err)
			}
			segmentsArray = append(segmentsArray, string(jsonSegment))
		}
		topoAnnotation[common.AnnVolumeAccessibleTopology] = "[" + strings.Join(segmentsArray, ",") + "]"
	}

	// Check if storage policy reservation related FSS is enabled
	if commonco.ContainerOrchestratorUtility.IsFSSEnabled(ctx, common.WCPMobilityNonDisruptiveImport) {
		// Check if both labelVirtualMachineName and labelStoragePolicyReservationName are on CnsRegisterVolume CR.
		// If both are present, add both to PVC
		if vmName, vmOk := instance.Labels[cnsoperatortypes.LabelVirtualMachineName]; vmOk && vmName != "" {
			if spName, spOk := instance.Labels[cnsoperatortypes.LabelStoragePolicyReservationName]; spOk && spName != "" {
				pvcLabels[cnsoperatortypes.LabelVirtualMachineName] = vmName
				pvcLabels[cnsoperatortypes.LabelStoragePolicyReservationName] = spName
			}
		}
	}

	claim := &v1.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{
			Name:        name,
			Namespace:   namespace,
			Labels:      pvcLabels,
			Annotations: topoAnnotation,
		},
		Spec: v1.PersistentVolumeClaimSpec{
			AccessModes: []v1.PersistentVolumeAccessMode{
				accessMode,
			},
			Resources: v1.VolumeResourceRequirements{
				Requests: v1.ResourceList{
					v1.ResourceName(v1.ResourceStorage): resource.MustParse(capacityInMb),
				},
			},
			StorageClassName: &storageClassName,
			VolumeName:       pvName,
		},
	}

	if isSharedDiskEnabled {
		claim.Spec.VolumeMode = &volumeMode
	}

	return claim, nil
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
