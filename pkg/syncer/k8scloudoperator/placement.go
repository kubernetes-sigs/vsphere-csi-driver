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
	"sort"
	"strings"
	"sync"

	"github.com/container-storage-interface/spec/lib/go/csi"
	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	k8stypes "k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/kubernetes"
	clientconfig "sigs.k8s.io/controller-runtime/pkg/client/config"

	apis "sigs.k8s.io/vsphere-csi-driver/pkg/apis/cnsoperator"
	"sigs.k8s.io/vsphere-csi-driver/pkg/csi/service/logger"
)

var mutex sync.Mutex

const (
	// bufferDiskSize to ensure successful allocation
	bufferDiskSize = 4 * 1024 * 1024
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
// XXX Change all usage of this into a map
type StoragePoolInfo struct {
	Name           string
	FreeCapInBytes int64
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
	if p[i].FreeCapInBytes > p[j].FreeCapInBytes {
		return true
	}
	if (p[i].FreeCapInBytes == p[j].FreeCapInBytes) && (p[i].Name < p[j].Name) {
		return true
	}
	return false
}

// isSPInList checks if a name already lies in the given list
func isSPInList(name string, spList []StoragePoolInfo) bool {
	for _, sp := range spList {
		if name == sp.Name {
			return true
		}
	}
	return false
}

// PlacePVConStoragePool selects target storage pool to place the given PVC based on its profile and the topology information.
// If the placement is successful, the PVC will be annotated with the selected storage pool and PlacePVConStoragePool nil that means no error
// For unsuccessful placement, PlacePVConStoragePool returns error that cause the failure
func PlacePVConStoragePool(ctx context.Context, client kubernetes.Interface, tops *csi.TopologyRequirement, curPVC *v1.PersistentVolumeClaim) error {
	log := logger.GetLogger(ctx)

	//XXX Return if this is not a vsan direct placement
	//XXX Need an identifier on the sc

	// Get all StoragePool list
	sps, err := getStoragePoolList(ctx)
	if err != nil {
		log.Errorf("Fail to get StoragePool list with %+v", err)
		return err
	}

	if len(sps.Items) <= 0 { //there is no available storage pools
		return fmt.Errorf("fail to find any storage pool")
	}

	hostNames := getHostCandidates(ctx, tops)
	capacity := curPVC.Spec.Resources.Requests[v1.ResourceName(v1.ResourceStorage)]
	volSizeBytes := capacity.Value()

	log.Infof("Starting placement for PVC %s, Topology %+v", curPVC.Name, hostNames)
	spList, err := preFilterSPList(ctx, sps, *curPVC.Spec.StorageClassName, hostNames, volSizeBytes)
	if err != nil {
		log.Infof("preFilterSPList failed with %+v", err)
		return err
	}

	if len(spList) <= 0 {
		log.Infof("Did not find any matching storage pools for %s", curPVC.Name)
		return fmt.Errorf("Fail to find a storage pool passing all criteria")
	}

	sort.Sort(byCOMBINATION(spList))

	// Sequence placement operations beyond this point to avoid race conditions
	// To protect the storage pool snapshot unpopulated for placement of PVCs with the same label
	// TODO optimization of lock scope by both persistence service and node name
	mutex.Lock()
	defer mutex.Unlock()

	spList, err = handleUsedStoragePools(ctx, client, curPVC, spList)
	if err != nil {
		log.Infof("handleUsedStoragePools failed with %+v", err)
		return err
	}

	if len(spList) <= 0 {
		log.Infof("Did not find any matching storage pools for %s", curPVC.Name)
		return fmt.Errorf("Fail to find a storage pool passing all criteria")
	}

	err = setPVCAnnotation(ctx, spList[0].Name, client, curPVC.Namespace, curPVC.Name)
	if err != nil {
		log.Infof("setPVCAnnotation failed with %+v", err)
		return err
	}

	return nil
}

// getStoragePoolList get all storage pool list
func getStoragePoolList(ctx context.Context) (*unstructured.UnstructuredList, error) {
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
	sps, err := spClient.Resource(spResource).List(metav1.ListOptions{
		LabelSelector: spTypeLabelKey,
	})
	if err != nil {
		log.Errorf("Failed to get StoragePool with %+v", err)
		return nil, err
	}
	return sps, err
}

// preFilterSPList filter out candidate storage pool list through topology and capacity
// XXX TODO add health of storage pools together as a filter when related metrics available
func preFilterSPList(ctx context.Context, sps *unstructured.UnstructuredList, storageClassName string, hostNames []string, volSizeBytes int64) ([]StoragePoolInfo, error) {
	log := logger.GetLogger(ctx)
	spList := []StoragePoolInfo{}

	totalStoragePools := len(sps.Items)
	nonVsanDirect := 0
	nonSCComp := 0
	topology := 0
	notEnoughCapacity := 0

	//flag := false
	for _, sp := range sps.Items {
		spName := sp.GetName()
		if StrContainers := strings.Contains(spName, "vsandirect"); !StrContainers {
			nonVsanDirect++
			continue
		}

		//sc compatible filter
		scs, found, err := unstructured.NestedStringSlice(sp.Object, "status", "compatibleStorageClasses")
		if !found || err != nil {
			nonVsanDirect++
			continue
		}

		foundMappedSC := false
		for _, sc := range scs {
			if storageClassName == sc {
				foundMappedSC = true
				break
			}
		}
		if !foundMappedSC {
			nonVsanDirect++
			continue
		}

		if !isStoragePoolAccessibleByNodes(ctx, sp, hostNames) {
			topology++
			continue
		}

		// the storage pool capacity is expressed in raw bytes
		spSize, found, err := unstructured.NestedInt64(sp.Object, "status", "capacity", "freeSpace")
		if !found || err != nil {
			notEnoughCapacity++
			continue
		}

		if spSize > volSizeBytes+bufferDiskSize { //filter by capacity
			spList = append(spList, StoragePoolInfo{
				Name:           spName,
				FreeCapInBytes: spSize,
			})
		} else {
			notEnoughCapacity++
		}
	}

	log.Infof("TotalPools:%d, Usable:%d. Pools removed because: not local:%d, SC mis-match:%d, topology mis-match:%d out of capacity:%d",
		totalStoragePools, len(spList), nonVsanDirect, nonSCComp, topology, notEnoughCapacity)
	return spList, nil
}

// isStoragePoolAccessibleByNodes filter out accessible storage pools from a given list of candidate nodes
func isStoragePoolAccessibleByNodes(ctx context.Context, sp unstructured.Unstructured, hostNames []string) bool {
	nodes, found, err := unstructured.NestedStringSlice(sp.Object, "status", "accessibleNodes")
	if !found || err != nil {
		return false
	}

	for _, host := range hostNames { //filter by node candidate list
		for _, node := range nodes {
			if node == host {
				return true
			}
		}
	}
	return false
}

// remove the sp from the given list
func removeSPFromList(spList []StoragePoolInfo, spName string) []StoragePoolInfo {
	for i, sp := range spList {
		if sp.Name == spName {
			copy(spList[i:], spList[i+1:])
			return spList[:len(spList)-1]
		}
	}
	return spList
}

// update used capacity of the storage pool based on the volume size of the pending PVC on it. also if this
// ends up removing the sp from the list if its free capacity falls to 0
func updateSPCapacityUsage(spList []StoragePoolInfo, spName string, pendingPVBytes int64, curPVBytes int64) (bool, bool, []StoragePoolInfo) {
	usageUpdated := false
	spRemoved := false
	for i := range spList {
		if spList[i].Name == spName {
			if spList[i].FreeCapInBytes > pendingPVBytes {
				spList[i].FreeCapInBytes -= pendingPVBytes
				if spList[i].FreeCapInBytes > curPVBytes+bufferDiskSize {
					usageUpdated = true
				} else {
					spList = removeSPFromList(spList, spName)
					spRemoved = true
				}
			} else {
				spList = removeSPFromList(spList, spName)
				spRemoved = true
			}
			break
		}
	}

	return usageUpdated, spRemoved, spList
}

// handleUsedStoragePools finds all storage pools that have been used by other PVCs on the same node and either removes them if
// if they dont satisfy the anti-affinity rules and/or updates their usage based on any pending PVs against the sp.
func handleUsedStoragePools(ctx context.Context, client kubernetes.Interface, curPVC *v1.PersistentVolumeClaim, spList []StoragePoolInfo) ([]StoragePoolInfo, error) {
	log := logger.GetLogger(ctx)

	usageUpdated := false
	requiredAntiAffinityValue, required := curPVC.Annotations[spPolicyAntiRequired]
	preferredAntiAffinityValue, preferred := curPVC.Annotations[spPolicyAntiPreferred]
	currPVCCap := curPVC.Spec.Resources.Requests[v1.ResourceName(v1.ResourceStorage)]

	pvcList, err := client.CoreV1().PersistentVolumeClaims(curPVC.Namespace).List(metav1.ListOptions{})
	if err != nil {
		log.Errorf("Failed to retrieve all PVCs in the same namespace from API server")
		return spList, err
	}

	xAffinity := 0
	xCapPending := 0

	usedSPList := []StoragePoolInfo{}
	for _, pvcItem := range pvcList.Items {
		spName, ok := pvcItem.Annotations[storagePoolAnnotationKey]
		if !ok {
			continue
		}

		// Is this even a SP we care about anyway
		if !isSPInList(spName, spList) {
			continue
		}

		// is required anti-affinity is set of the PVC then remove any SPs that are already used
		if required {
			antiAffinityValue, setRequired := pvcItem.Annotations[spPolicyAntiRequired]
			if setRequired && antiAffinityValue == requiredAntiAffinityValue {
				spList = removeSPFromList(spList, spName)
				xAffinity++
				continue
			}
		}

		// Looks like this SP is here to stay
		// update SP usage based on any unbound PVCs placed on this SP. These are still in pipeline and hence
		// the usage of SP will not be updated yet. There is always a race where the usage is already updated but
		// the PVC is not yet in bound state but we will rather be conservative and try the placement again later
		if pvcItem.Status.Phase == v1.ClaimPending {
			capacity := pvcItem.Spec.Resources.Requests[v1.ResourceName(v1.ResourceStorage)]
			spRemoved := false
			usageUpdated, spRemoved, spList = updateSPCapacityUsage(spList, spName, capacity.Value(),
				currPVCCap.Value())
			if spRemoved {
				xCapPending++
				continue
			}
		}

		// if preferred antiaffinity is set of the PVC then make a list of SPs that are already used.
		if preferred && !isSPInList(spName, usedSPList) {
			antiAffinityValue, setPreferred := pvcItem.Annotations[spPolicyAntiPreferred]
			if setPreferred && antiAffinityValue == preferredAntiAffinityValue {
				log.Infof("Find used sp %s as defined by %s", spName, spPolicyAntiPreferred)
				usedSPList = append(usedSPList, StoragePoolInfo{
					Name: spName,
				})
			}
		}

	}

	log.Infof("Removed because of: affinity rules:%d,  out of capacity:%d. Usable Pools:%d, Pools already used at least once:%d", xAffinity, xCapPending, len(spList), len(usedSPList))

	// if we have any unused SPs then we can just remove all used SPs from the
	// list. This gives us a small set of unused SPs that we can re-sort below
	if len(spList) > len(usedSPList) {
		for _, sp := range usedSPList {
			spList = removeSPFromList(spList, sp.Name)
		}
	}

	if usageUpdated {
		sort.Sort(byCOMBINATION(spList))
	}

	return spList, nil
}

// setPVCAnnotation add annotation of selected storage pool to targeted PVC
func setPVCAnnotation(ctx context.Context, spName string, client kubernetes.Interface, ns string, pvcName string) error {
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

	log.Infof("Picked sp %s for PVC %s", curPVC.Annotations[storagePoolAnnotationKey], curPVC.Name)
	return nil
}

// getHostCandidates get all candidate hosts from topology requirements
func getHostCandidates(ctx context.Context, topologyRequirement *csi.TopologyRequirement) []string {
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
