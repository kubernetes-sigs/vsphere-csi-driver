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

package k8scloudoperator

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"

	"github.com/container-storage-interface/spec/lib/go/csi"
	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	k8stypes "k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/kubernetes"
	clientconfig "sigs.k8s.io/controller-runtime/pkg/client/config"
	"sigs.k8s.io/vsphere-csi-driver/pkg/csi/service/logger"
	"sigs.k8s.io/vsphere-csi-driver/pkg/syncer/cnsoperator/apis"

	"sort"
	"strconv"
)

const (
	// bufferDiskSize to ensure successful allocation
	bufferDiskSizeMB = 4
	// PVC annotation key to specify the StoragePool on which PV should be placed.
	storagePoolAnnotationKey = "failure-domain.beta.vmware.com/storagepool"
	// AntiAffinityPreferred placement policy for storagepool
	spPolicyAntiPreferred = "placement.beta.vmware.com/storagepool_antiAffinityPreferred"
	// AntiAffinityRequired placement policy for storagepool
	spPolicyAntiRequired = "placement.beta.vmware.com/storagepool_antiAffinityRequired"
	//resource name to get storage class
	resourceName = "storagepools"
)

// StoragePoolInfo is abstraction of a storage pool list
type StoragePoolInfo struct {
	Name       string
	CapacityMB int64
}

// PersistentVolumeClaimModel is pvc list
type PersistentVolumeClaimModel struct {
	//usedsps contains all used storage pools
	usedsps []string
}

// byCombination uses several different property to rank storage pools
type byCOMBINATION []StoragePoolInfo

// Length func for ranked storage pool list
func (p byCOMBINATION) Len() int {
	return len(p)
}

// swap func for ranked storage pool list
func (p byCOMBINATION) Swap(i, j int) {
	p[i], p[j] = p[j], p[i]
}

// compare func for ranked storage pool list
func (p byCOMBINATION) Less(i, j int) bool {
	if p[i].CapacityMB > p[j].CapacityMB {
		return true
	}
	if (p[i].CapacityMB == p[j].CapacityMB) && (p[i].Name < p[j].Name) {
		return true
	}
	return false
}

// IsFilterSP is a filter function applied to a single sp.
type IsFilterSP func(StoragePoolInfo, PersistentVolumeClaimModel) bool

// IsStoragePoolUsed checks whether the given storage pool is present in the PersistentVolumeClaimModel.
func IsStoragePoolUsed(sp StoragePoolInfo, pvc PersistentVolumeClaimModel) bool {
	spNames := pvc.usedsps

	for _, c := range spNames {
		if sp.Name == c {
			return false
		}
	}
	return true
}

// IsSpNameInList checks if a name already lies in the given list
func IsSpNameInList(name string, nameList []string) bool {
	for _, c := range nameList {
		if name == c {
			return true
		}
	}
	return false
}

// ApplyFiltersSP applies a set of given filters on the given storage pool list, and returns potential candidates for placement.
// Each record will be checked against each filter.
// The filters are applied in the order they are passed in.
func ApplyFiltersSP(spList []StoragePoolInfo, pvcModel PersistentVolumeClaimModel, filters ...IsFilterSP) []StoragePoolInfo {
	// Make sure there are actually filters to be applied.
	if len(filters) == 0 {
		return spList
	}
	filteredRecords := make([]StoragePoolInfo, 0, len(spList))
	// Range over the records and apply all the filters to each record.
	// If the record passes all the filters, add it to the final slice.
	for _, r := range spList {
		keep := true
		for _, f := range filters {
			if !f(r, pvcModel) {
				keep = false
				break
			}
		}
		if keep {
			filteredRecords = append(filteredRecords, r)
		}
	}
	return filteredRecords
}

// PlacePVConStoragePool selects target storage pool to place the given PVC based on its profile and the topology information.
// If the placement is successful, the PVC will be annotated with the selected storage pool and PlacePVConStoragePool nil that means no error
// For unsuccessful placement, PlacePVConStoragePool returns error that cause the failure
func PlacePVConStoragePool(ctx context.Context, client kubernetes.Interface, tops *csi.TopologyRequirement, curPVC *v1.PersistentVolumeClaim) error {
	log := logger.GetLogger(ctx)

	// Get all StoragePool list
	sps, err := GetStoragePoolList(ctx)
	if err != nil {
		log.Errorf("Fail to get StoragePool list with %+v", err)
		return err
	}
	log.Infof("Get all storage pools %s", sps)
	if len(sps.Items) <= 0 { //there is no available storage pools
		return fmt.Errorf("fail to find any storage pool")
	}

	hostNames := GetHostCandidates(ctx, tops)
	capacity := curPVC.Spec.Resources.Requests[v1.ResourceName(v1.ResourceStorage)]
	volSizeBytes := capacity.Value()

	spList, err := PreFilterStoragePoolList(ctx, sps, *curPVC.Spec.StorageClassName, hostNames, volSizeBytes)
	if err != nil || len(spList) <= 0 {
		return err
	}
	log.Infof("PreFilterStoragePoolList get StoragePool list with %+v", spList)

	sort.Sort(byCOMBINATION(spList))
	log.Infof("Sort splist %+v", spList)
	_, preferred := curPVC.Annotations[spPolicyAntiPreferred]
	_, required := curPVC.Annotations[spPolicyAntiRequired]

	if preferred || required {
		usedSPNames, err := GetUsedStoragePools(ctx, client, curPVC)
		log.Infof("GetUsedStoragePools get StoragePool list with %+v", usedSPNames)

		if err != nil {
			return err
		}
		if preferred {
			spList = RankStoragePoolList(ctx, spList, usedSPNames)
		}
		if required {
			//go further to filter out used SPs
			//struct contains all inputs for filters
			pvcModel := PersistentVolumeClaimModel{
				usedsps: usedSPNames,
			}
			log.Infof("Get input filters: %s", pvcModel)
			spList = ApplyFiltersSP(spList, pvcModel, IsStoragePoolUsed)
			log.Infof("Find the filtered spList: %+v", spList)
		}
	}

	if len(spList) <= 0 {
		return fmt.Errorf("Fail to find a storage pool passing all criteria")
	}

	err = SetPVCAnnotation(ctx, spList[0].Name, client, curPVC.Namespace, curPVC.Name)
	if err != nil {
		return err
	}
	return nil
}

// GetStoragePoolList get all storage pool list
func GetStoragePoolList(ctx context.Context) (*unstructured.UnstructuredList, error) {
	log := logger.GetLogger(ctx)

	cfg, err := clientconfig.GetConfig()
	if err != nil {
		log.Errorf("Failed to get Kubernetes config in PSPPE. Err: %+v", err)
		return nil, err
	}

	spClient, err := dynamic.NewForConfig(cfg)
	if err != nil {
		log.Errorf("Failed to create StoragePool client using config. Err: %+v", err)
		return nil, err
	}

	spResource := schema.GroupVersion{Group: apis.GroupName, Version: apis.Version}.WithResource(resourceName)

	// TODO enable label on each storage pool and use label as filter storage pool list
	sps, err := spClient.Resource(spResource).List(metav1.ListOptions{})
	if err != nil {
		log.Errorf("Failed to get StoragePool with %+v", err)
		return nil, err
	}
	return sps, err
}

// PreFilterStoragePoolList filter out candidate storage pool list through topology and capacity
// TODO add health of storage pools together as a filter when related metrics available
func PreFilterStoragePoolList(ctx context.Context, sps *unstructured.UnstructuredList, storageClassName string, hostNames []string, volSizeBytes int64) ([]StoragePoolInfo, error) {
	log := logger.GetLogger(ctx)
	spList := []StoragePoolInfo{}

	//flag := false
	for _, sp := range sps.Items {
		spName := sp.GetName()
		if StrContainers := strings.Contains(spName, "vsandirect"); !StrContainers {
			continue
		}

		//sc compatible filter
		log.Infof("Prepare to check compatibility from PVC %+v", storageClassName)
		scs, found, err := unstructured.NestedStringSlice(sp.Object, "status", "compatibleStorageClasses")
		if !found || err != nil {
			continue
		}
		log.Infof("Nested read StoragePool compatibleStorageClasses %+v", scs)
		foundMappedSC := false
		for _, sc := range scs {
			if storageClassName == sc {
				foundMappedSC = true
				break
			}
		}
		if !foundMappedSC {
			continue
		}

		if !IsStoragePoolAccessibleByNodes(ctx, sp, hostNames) {
			continue
		}

		cap, found, err := unstructured.NestedString(sp.Object, "status", "capacity", "freeSpace")
		if !found || err != nil {
			continue
		}
		spSize, err := strconv.ParseInt(cap, 10, 64)
		if err != nil {
			log.Errorf("Fail to place for error %s when cap size of StoragePool %s", err, spName)
			return nil, err
		}
		if spSize > volSizeBytes+bufferDiskSizeMB { //filter by capacity
			spList = append(spList, StoragePoolInfo{
				Name:       spName,
				CapacityMB: spSize,
			})
		}
	}
	return spList, nil
}

// IsStoragePoolAccessibleByNodes filter out accessible storage pools from a given list of candidate nodes
func IsStoragePoolAccessibleByNodes(ctx context.Context, sp unstructured.Unstructured, hostNames []string) bool {
	log := logger.GetLogger(ctx)
	nodes, found, err := unstructured.NestedStringSlice(sp.Object, "status", "accessibleNodes")
	if !found || err != nil {
		return false
	}

	log.Infof("FilterByAccessibleNodes by hostNames %+v", hostNames)
	log.Infof("FilterByAccessibleNodes StoragePool accessibleNodes %+v", nodes)
	for _, host := range hostNames { //filter by node candidate list
		for _, node := range nodes {
			if node == host {
				return true
			}
		}
	}
	return false
}

// GetUsedStoragePools find all storage pools has been used by other PVCs on the same node
func GetUsedStoragePools(ctx context.Context, client kubernetes.Interface, curPVC *v1.PersistentVolumeClaim) ([]string, error) {
	log := logger.GetLogger(ctx)
	requiredNodeName, required := curPVC.Annotations[spPolicyAntiRequired]
	preferredNodeName, preferred := curPVC.Annotations[spPolicyAntiPreferred]

	// TODO enable PVC label and use it as filter
	pvcList, err := client.CoreV1().PersistentVolumeClaims(curPVC.Namespace).List(metav1.ListOptions{})
	if err != nil {
		log.Errorf("Failed to retrieve all PVCs in the same namespace from API server")
		return nil, err
	}

	usedSPNames := []string{}
	for _, pvcItem := range pvcList.Items {
		spName, ok := pvcItem.Annotations[storagePoolAnnotationKey]
		if !ok {
			continue
		}
		if IsSpNameInList(spName, usedSPNames) {
			continue
		}
		nodeName, setRequired := pvcItem.Annotations[spPolicyAntiRequired]
		if required && setRequired && nodeName == requiredNodeName {
			log.Infof("Find used sp %s as defined by %s", spName, spPolicyAntiRequired)
			usedSPNames = append(usedSPNames, spName)
			continue
		}
		nodeName, setPreferred := pvcItem.Annotations[spPolicyAntiPreferred]
		if preferred && setPreferred && nodeName == preferredNodeName {
			log.Infof("Find used sp %s as defined by %s", spName, spPolicyAntiPreferred)
			usedSPNames = append(usedSPNames, spName)
		}
	}
	return usedSPNames, nil
}

// RankStoragePoolList rank all candidate storage pools by a combination of different metrics
// If anti-affinity is preferred, RankStoragePoolList keeps moving used storage pools to the lower end of candidate list till the first unused storage pool
func RankStoragePoolList(ctx context.Context, spList []StoragePoolInfo, usedSpNames []string) []StoragePoolInfo {
	log := logger.GetLogger(ctx)

	if len(spList) <= 1 {
		log.Infof("No need to rank the list")
		return spList
	}

	for i := 0; i < len(spList); i++ {
		if !IsSpNameInList(spList[0].Name, usedSpNames) {
			break
		}
		log.Infof("Put used sp %s lower in splist", spList[0].Name)
		spList = append(spList[1:], spList[:1]...)
		log.Infof("Rotate the splist %s", spList)
	}
	return spList
}

// SetPVCAnnotation add annotation of selected storage pool to targeted PVC
func SetPVCAnnotation(ctx context.Context, spName string, client kubernetes.Interface, ns string, pvcName string) error {
	log := logger.GetLogger(ctx)

	if spName == "" {
		return nil
	}

	patch := map[string]interface{}{
		"metadata": map[string]interface{}{
			"annotations": map[string]string{
				storagePoolAnnotationKey: spName,
			},
		},
	}

	patchBytes, err := json.Marshal(patch)
	if err != nil {
		log.Errorf("Fail to marshal patch: %+v", err)
		return err
	}

	curPVC, err := client.CoreV1().PersistentVolumeClaims(ns).Patch(pvcName, k8stypes.MergePatchType, patchBytes)
	if err != nil {
		log.Errorf("Fail to update PVC %+v", err)
		return err
	}

	log.Infof("Find the sp %s to place PVC named as %s", curPVC.Annotations[storagePoolAnnotationKey], curPVC.Name)
	return nil
}

// GetHostCandidates get all candidate hosts from topology requirements
func GetHostCandidates(ctx context.Context, topologyRequirement *csi.TopologyRequirement) []string {
	log := logger.GetLogger(ctx)

	hostNames := []string{}
	if topologyRequirement == nil || topologyRequirement.GetPreferred() == nil {
		log.Infof("Found no Accessibility requirements")
		return hostNames
	}
	for _, topology := range topologyRequirement.GetPreferred() {
		if topology == nil {
			log.Infof("Get invalid accessibility requirement %v", topology)
		}
		value, ok := topology.Segments[v1.LabelHostname]
		if !ok {
			log.Infof("Found no hostname in the accessibility requirements %v", topology)
		}
		hostNames = append(hostNames, value)
	}
	return hostNames
}
