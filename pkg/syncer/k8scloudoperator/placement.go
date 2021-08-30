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
	"strconv"
	"strings"
	"sync"

	"github.com/container-storage-interface/spec/lib/go/csi"
	appsv1 "k8s.io/api/apps/v1"
	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	k8stypes "k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/kubernetes"
	clientconfig "sigs.k8s.io/controller-runtime/pkg/client/config"

	apis "sigs.k8s.io/vsphere-csi-driver/v2/pkg/apis/cnsoperator"
	cnsconfig "sigs.k8s.io/vsphere-csi-driver/v2/pkg/common/config"
	"sigs.k8s.io/vsphere-csi-driver/v2/pkg/csi/service/common"
	"sigs.k8s.io/vsphere-csi-driver/v2/pkg/csi/service/common/commonco"
	"sigs.k8s.io/vsphere-csi-driver/v2/pkg/csi/service/logger"
	"sigs.k8s.io/vsphere-csi-driver/v2/pkg/syncer/admissionhandler"
)

var (
	// Mutex used to serialize PVC placement requests.
	pvcPlacementMutex sync.Mutex
)

const (
	// StoragePoolAnnotationKey is a PVC annotation to specify the StoragePool
	// on which PV should be placed.
	StoragePoolAnnotationKey = "failure-domain.beta.vmware.com/storagepool"
	// ScNameAnnotationKey is a PVC annotation to specify storage class from
	// which PV should be provisioned.
	ScNameAnnotationKey = "volume.beta.kubernetes.io/storage-class"
	// AntiAffinityPreferred placement policy for storagepool.
	spPolicyAntiPreferred = "placement.beta.vmware.com/storagepool_antiAffinityPreferred"
	// AntiAffinityRequired placement policy for storagepool.
	spPolicyAntiRequired = "placement.beta.vmware.com/storagepool_antiAffinityRequired"
	// Resource name to get storage class.
	resourceName = "storagepools"
	// StoragePool type name for vsan-direct.
	vsanDirect                = "vsanD"
	invalidParamsErr          = "FAILED_PLACEMENT-InvalidParams"
	genericErr                = "FAILED_PLACEMENT-Generic"
	notEnoughResErr           = "FAILED_PLACEMENT-NotEnoughResources"
	invalidConfigErr          = "FAILED_PLACEMENT-InvalidConfiguration"
	siblingReplicaBoundPVCErr = "FAILED_PLACEMENT-HasSiblingReplicaBoundPVC"
	vsanSna                   = "vsan-sna"
	// Appplatform label that all vDPP PVCs must have.
	appplatformLabel = "appplatform.vmware.com/instance-id"
	// Opt out label present on pod to look for bound PVCs of all sibling replicas
	siblingReplicaCheckOptOutLabel = "psp.vmware.com/sibling-replica-check-opt-out"
)

// StoragePoolInfo is abstraction of a storage pool list.
// XXX Change all usage of this into a map.
type StoragePoolInfo struct {
	Name                  string
	AllocatableCapInBytes int64
}

// byCombination uses several different property to rank storage pools.
type byCOMBINATION []StoragePoolInfo

// Length func for ranked storage pool list.
func (p byCOMBINATION) Len() int {
	return len(p)
}

// Swap func for ranked storage pool list.
func (p byCOMBINATION) Swap(i, j int) {
	p[i], p[j] = p[j], p[i]
}

// Compare func for ranked storage pool list.
func (p byCOMBINATION) Less(i, j int) bool {
	if p[i].AllocatableCapInBytes > p[j].AllocatableCapInBytes {
		return true
	}
	if (p[i].AllocatableCapInBytes == p[j].AllocatableCapInBytes) && (p[i].Name < p[j].Name) {
		return true
	}
	return false
}

// VolumeInfo bundles up PVC info along with its bounded PV info (if any).
type VolumeInfo struct {
	PVC         v1.PersistentVolumeClaim
	PVName      string
	SizeInBytes int64
}

type migrationPlanner interface {
	getMigrationPlan(ctx context.Context, client kubernetes.Interface) (map[string]string, error)
}

// Relaxed fit decreasing also called as WFD offers appreciable approximation
// guarantees (measures closeness to optimal soln) and also outputs svMotion
// plan which results in an even free space distribution across datastores.
// In relaxed fit algorithm a volume is placed into a datastore having maximum
// free space available, contrary to tight fit algorithms, where an item is
// placed in bin with minimum appropriate free space.
type relaxedFitMigrationPlanner struct {
	volumeList      []VolumeInfo
	storagePoolList []unstructured.Unstructured
	pvcList         []v1.PersistentVolumeClaim
	sourceHostNames []string
}

func newRelaxedFitMigrationPlanner(volumeList []VolumeInfo, spList []unstructured.Unstructured,
	allPVCList []v1.PersistentVolumeClaim, accessibleNodeNames []string) migrationPlanner {
	return relaxedFitMigrationPlanner{
		volumeList:      volumeList,
		storagePoolList: spList,
		pvcList:         allPVCList,
		sourceHostNames: accessibleNodeNames,
	}
}

// Uses Relaxed Fit Decreasing (also called WFD) bin packaging algorithm to
// assign for each pvc a target storage-pool for storage vMotion.
// It tries to place a volume into a disk with maximum free space. Hence it
// tries to decrease the free space variance after migration.
func (b relaxedFitMigrationPlanner) getMigrationPlan(ctx context.Context,
	client kubernetes.Interface) (map[string]string, error) {
	log := logger.GetLogger(ctx)
	volumeToSPMap := make(map[string]string)
	// Sort volumes in decreasing order of size.
	sort.Slice(b.volumeList, func(i, j int) bool {
		return b.volumeList[i].SizeInBytes > b.volumeList[j].SizeInBytes
	})

	for _, vol := range b.volumeList {
		pvcName := vol.PVC.Name

		assignedSp, err := getSPForPVCPlacement(ctx, client, &vol.PVC, vol.SizeInBytes, b.storagePoolList,
			b.sourceHostNames, b.pvcList, vsanDirectType, false)
		if err != nil {
			log.Errorf("Failed to assign SP to PVC %v. Error: %v", pvcName, err)
			return nil, fmt.Errorf("PVC %v could not be migrated due to placement constraints "+
				"or lack of capacity in accessible datastores", pvcName)
		}

		// Map the volume with SP with highest free space. Update free space in
		// assigned SP for more accurate placement.
		assignedSPName := assignedSp.Name
		volumeToSPMap[vol.PVName] = assignedSPName
		for idx, sp := range b.storagePoolList {
			if sp.GetName() == assignedSPName {
				curAllocatableCap, found, err := unstructured.NestedInt64(sp.Object,
					"status", "capacity", "allocatableSpace")
				if !found || err != nil {
					log.Warnf("Could not get current allocatable capacity for SP %v. Found: %v. Error: %v.",
						sp.GetName(), found, err)
					break
				}
				err = unstructured.SetNestedField(b.storagePoolList[idx].Object,
					curAllocatableCap-vol.SizeInBytes, "status", "capacity", "allocatableSpace")
				if err != nil {
					log.Warnf("Could not update allocatable space for SP %v. Error: %v", sp.GetName(), err)
				}
				break
			}
		}

		// Update storagePool info in namespaceToPVCsMap so that in next loop
		// anti-affinity filter is aware of this migration.
		for index, pvc := range b.pvcList {
			if pvc.Name == pvcName {
				curAnnotations := pvc.GetAnnotations()
				curAnnotations[StoragePoolAnnotationKey] = assignedSPName
				b.pvcList[index].SetAnnotations(curAnnotations)
				break
			}
		}
	}
	log.Debugf("volumeToSPMap: %v", volumeToSPMap)
	return volumeToSPMap, nil
}

// isSPInList checks if a name already lies in the given list.
func isSPInList(name string, spList []StoragePoolInfo) bool {
	for _, sp := range spList {
		if name == sp.Name {
			return true
		}
	}
	return false
}

// GetVolumesOnStoragePool returns volume information of all PVCs present
// on the given StoragePool.
func GetVolumesOnStoragePool(ctx context.Context, client kubernetes.Interface,
	StoragePoolName string) ([]VolumeInfo, []v1.PersistentVolumeClaim, error) {
	log := logger.GetLogger(ctx)
	volumeInfoList := []VolumeInfo{}

	pvcs, err := client.CoreV1().PersistentVolumeClaims(v1.NamespaceAll).List(ctx, metav1.ListOptions{})
	if err != nil {
		log.Errorf("failed to fetch PVC from all namespaces. Error: %v", err)
		return volumeInfoList, make([]v1.PersistentVolumeClaim, 0), err
	}
	for _, pvc := range pvcs.Items {
		spName, found := pvc.Annotations[StoragePoolAnnotationKey]
		if !found || spName != StoragePoolName {
			continue
		}

		pvName := pvc.Spec.VolumeName
		pv, err := client.CoreV1().PersistentVolumes().Get(ctx, pvName, metav1.GetOptions{})
		if err != nil || pv == nil {
			if pvName == "" {
				continue
			}
			log.Errorf("failed to get PV bounded with PVC %v", pvc.Name)
			return volumeInfoList, pvcs.Items, err
		}

		// Verify that this pv is indeed bounded to PVC.
		claimRef := pv.Spec.ClaimRef
		if claimRef == nil || claimRef.Name != pvc.Name {
			log.Infof("PV %v is not bounded to PVC %v.", pvName, pvc.Name)
			continue
		}

		// pv size can be more than requested size mentioned in PVC (in general
		// they will be the same).
		volumeSize := pv.Spec.Capacity.Storage()
		volumeInfoList = append(volumeInfoList, VolumeInfo{
			PVC:         pvc,
			PVName:      pvName,
			SizeInBytes: volumeSize.Value(),
		})
	}
	return volumeInfoList, pvcs.Items, nil
}

// GetSVMotionPlan when decommissioning a SP, maps all the volumes present on
// the given SP to a suitable SP to migrate into. To reduce the friction to
// onboard new partners, the workflow for disk deconmmission does not involve
// partner participation. Eg. decommissioning a disk with "ensureAccessibility"
// MM, information of which volume can remain on the disk while ensuring
// accessibility is present only with the partner operator. Hence the svMotion
// plan generated is same for all maintenance mode provided and equivalent to
// "evacuateAll" MM. Once we move towards more deep integration between our
// partner and PSP, we will be able to communicate with partner to get only
// the necessary migrations for a given MM (for eg. "ensureAccessibility").
func GetSVMotionPlan(ctx context.Context, client kubernetes.Interface,
	storagePoolName string, maintenanceMode string) (map[string]string, error) {
	log := logger.GetLogger(ctx)
	volumesToSPMap := make(map[string]string)

	spList, err := getStoragePoolList(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to get StoragePools list. Error: %v", err)
	}
	if len(spList.Items) == 0 {
		return nil, fmt.Errorf("could not find any StoragePool to migrate volumes")
	}

	var sourceSP unstructured.Unstructured
	for _, sp := range spList.Items {
		if sp.GetName() == storagePoolName {
			sourceSP = sp
			break
		}
	}
	if sourceSP.GetName() == "" {
		return nil, fmt.Errorf("failed to find source vSAN Direct StoragePool with name %v", storagePoolName)
	}

	spType, found, err := unstructured.NestedString(sourceSP.Object, "metadata", "labels", spTypeLabelKey)
	if !found || err != nil || spType != vsanDirect {
		return nil, fmt.Errorf("given StoragePool is not a vSAN Direct Datastore")
	}

	accessibleNodes, found, err := unstructured.NestedStringSlice(sourceSP.Object, "status", "accessibleNodes")
	if !found || err != nil {
		log.Errorf("Could not get accessible host from storage pool %v. Err: %v", storagePoolName, err)
		return nil, fmt.Errorf("could not get accessible host information from StoragePool %v", storagePoolName)
	}
	if len(accessibleNodes) != 1 {
		log.Warnf("Unexpected number of accessible nodes found for storage pool %v. Expected 1 found %v",
			storagePoolName, len(accessibleNodes))
		if len(accessibleNodes) == 0 {
			return nil, fmt.Errorf("the given datastore/StoragePool is not accessible from any host. " +
				"Maybe its unmounted or host is under maintenance mode")
		}
		// If datastore is accessible from multiple host, ignore the error.
	}

	volumeInfoList, allPVCList, err := GetVolumesOnStoragePool(ctx, client, storagePoolName)
	if err != nil {
		return nil, fmt.Errorf("failed to get the list of volumes to be migrated")
	}
	if len(volumeInfoList) == 0 {
		log.Infof("No volume present in storagePool %v to migrate. ", storagePoolName)
		return volumesToSPMap, nil
	}

	// For each volume assign a target sp for storage vMotion.
	rfMigrationPlanner := newRelaxedFitMigrationPlanner(volumeInfoList, spList.Items, allPVCList, accessibleNodes)
	volumesToSPMap, err = rfMigrationPlanner.getMigrationPlan(ctx, client)

	if err != nil {
		return nil, err
	}
	return volumesToSPMap, nil
}

func getSPForPVCPlacement(ctx context.Context,
	client kubernetes.Interface,
	curPVC *v1.PersistentVolumeClaim,
	volSizeBytes int64,
	sps []unstructured.Unstructured,
	hostNames []string,
	pvcList []v1.PersistentVolumeClaim,
	spType string,
	onlinePlacement bool) (StoragePoolInfo, error) {
	log := logger.GetLogger(ctx)
	assignedSP := StoragePoolInfo{}

	log.Infof("Starting placement for PVC %s, Topology %+v", curPVC.Name, hostNames)

	scName, err := GetSCNameFromPVC(curPVC)
	if err != nil {
		log.Errorf("Fail to get Storage class name from PVC with +v", err)
		if onlinePlacement {
			stampPVCWithError(ctx, client, curPVC, invalidParamsErr)
		}
		return assignedSP, err
	}

	spList, err := preFilterSPList(ctx, sps, scName, hostNames, volSizeBytes)
	if err != nil {
		log.Infof("preFilterSPList failed with %+v", err)
		if onlinePlacement {
			stampPVCWithError(ctx, client, curPVC, genericErr)
		}
		return assignedSP, err
	}
	if len(spList) == 0 {
		log.Infof("Did not find any matching storage pools for %s", curPVC.Name)
		if onlinePlacement {
			stampPVCWithError(ctx, client, curPVC, notEnoughResErr)
		}
		return assignedSP, fmt.Errorf("fail to find a StoragePool passing all criteria")
	}

	xCapPendingSet := make(map[string]bool)
	for _, pvcItem := range pvcList {
		if pvcItem.Status.Phase == v1.ClaimPending {
			spName, ok := pvcItem.Annotations[StoragePoolAnnotationKey]
			if _, exist := xCapPendingSet[spName]; !ok || exist {
				continue
			}

			// Update SP usage based on any unbound PVCs placed on this SP. These
			// are still in pipeline and hence the usage of SP will not be updated
			// yet. There is always a race where the usage is already updated but
			// the PVC is not yet in bound state but we will rather be conservative
			// and try the placement again later.
			capacity := pvcItem.Spec.Resources.Requests[v1.ResourceName(v1.ResourceStorage)]
			spRemoved := false
			_, spRemoved, spList = updateSPCapacityUsage(spList, spName, capacity.Value(), volSizeBytes)
			if spRemoved {
				xCapPendingSet[spName] = true
				continue
			}
		}
	}
	log.Infof("%d StoragePool(s) removed due to lack of capacity after considering any unbound PVC usage: %v",
		len(xCapPendingSet), xCapPendingSet)

	sort.Sort(byCOMBINATION(spList))

	spList = handleUsedStoragePools(ctx, curPVC, volSizeBytes, spList, pvcList, spType)

	if len(spList) == 0 {
		log.Infof("Did not find any matching storage pools for %s", curPVC.Name)
		if onlinePlacement {
			stampPVCWithError(ctx, client, curPVC, notEnoughResErr)
		}
		return assignedSP, fmt.Errorf("fail to find any compatible StoragePool due to volume placement constraints")
	}

	assignedSP = spList[0]
	return assignedSP, nil
}

func stampPVCWithError(ctx context.Context, client kubernetes.Interface,
	curPVC *v1.PersistentVolumeClaim, errAnnotation string) {
	log := logger.GetLogger(ctx)
	err := setPVCAnnotation(ctx, errAnnotation, client, curPVC)
	if err != nil {
		log.Errorf("Could not set err annotation on pvc %+v", err)
	}
}

// getParentSTS finds the statefulset that currPVC belongs to.
// It goes through all the statefulsets in currPVC's namespace and
// constructs the prefix for each of its volumeclaimtemplates.
// If the prefix matches, then that STS is the parent statefulset.
func getParentSTS(ctx context.Context, client kubernetes.Interface, currPVC *v1.PersistentVolumeClaim,
	currPVCParentReplica int, currPVCPrefix string) (*appsv1.StatefulSet, error) {
	// Find all statefulsets in currPVC's namespace.
	namespace := currPVC.Namespace
	stsList, err := client.AppsV1().StatefulSets(namespace).List(ctx, metav1.ListOptions{})
	if err != nil {
		return nil, err
	}

	for _, sts := range stsList.Items {
		volumeClaimTemplates := sts.Spec.VolumeClaimTemplates
		numberOfReplicas := sts.Spec.Replicas

		// currPVC's parent replica number should be within the range of the
		// number of replicas defined in the given sts.
		if *numberOfReplicas < int32(currPVCParentReplica) {
			continue
		}

		if !strings.Contains(currPVCPrefix, sts.Name) {
			continue
		}

		for _, volume := range volumeClaimTemplates {
			// Construct the prefix that the volume name will take for each
			// replica and check weather currPVC's prefix is same as that prefix.
			// If yes, then we have found the parent statefulset.
			volumePrefix := volume.ObjectMeta.Name + "-" + sts.Name
			if currPVCPrefix == volumePrefix {
				return &sts, nil
			}
		}
	}

	// Parent statefulset not found. It means that it is a standalone pod.
	return nil, nil
}

// Finds the PVC prefix and the replica number to which the given PVC belongs to.
func getPvcPrefixAndParentReplicaID(ctx context.Context, pvcName string) (string, int, error) {
	splitCurrPVCName := strings.Split(pvcName, "-")

	if len(splitCurrPVCName) > 0 {
		// Get ReplicaID for current PVC.
		currPVCReplicaID := splitCurrPVCName[len(splitCurrPVCName)-1]
		currPVCReplicaIDStr, err := strconv.Atoi(currPVCReplicaID)

		// Get prefix for current PVC.
		splitCurrPVCName = splitCurrPVCName[:len(splitCurrPVCName)-1]
		currPVCPrefix := strings.Join(splitCurrPVCName, "-")

		return currPVCPrefix, currPVCReplicaIDStr, err
	}

	return "", -1, fmt.Errorf("failed to find parent Replica ID for PVC %+v", pvcName)
}

// getCousinPVCs finds the parent statefulset of currPVC and returns a list
// of all of its PVCs except sibling PVCs. Sibling PVCs are the ones that
// belong to the same replica as that of currPVC.
func getCousinPVCs(ctx context.Context, client kubernetes.Interface,
	currPVC *v1.PersistentVolumeClaim) ([]string, error) {
	log := logger.GetLogger(ctx)

	// Find prefix for currPVC and the replica number that currPVC belongs to.
	currPVCPrefix, currPVCParentReplica, err := getPvcPrefixAndParentReplicaID(ctx, currPVC.Name)
	if err != nil {
		log.Errorf("Failed to get prefix and replica number to which the PVC belongs. Err %+v", err)
		return []string{}, err
	}

	// Find the statefulset that the PVC belongs to.
	parentSTS, err := getParentSTS(ctx, client, currPVC, currPVCParentReplica, currPVCPrefix)
	if err != nil {
		return []string{}, err
	}
	// Parent statefulset not found, send back empty list of cousin PVCs.
	if parentSTS == nil {
		return []string{}, nil
	}

	cousinPVCs := []string{}
	// Number of replicas defined in the statefulset.
	numberOfReplicas := int(*parentSTS.Spec.Replicas)

	for i := 0; i < numberOfReplicas; i++ {
		// If replica under consideration is same as the one that currPVC
		// belongs to, ignore it as it will give sibling PVC.
		if i == currPVCParentReplica {
			continue
		}
		// Construct the cousin PVC name and add it to the list.
		cousinPVCName := currPVCPrefix + "-" + strconv.Itoa(i)
		cousinPVCs = append(cousinPVCs, cousinPVCName)
	}

	return cousinPVCs, nil
}

// Removes host at the given index.
func filterHost(s []string, index int) []string {
	return append(s[:index], s[index+1:]...)
}

// Returns a map of PVC name and object for a list of PVCs in a given
// namespace and label selector.
func getAllPvcs(ctx context.Context, client kubernetes.Interface, namespace string,
	selectLabel string) (map[string]v1.PersistentVolumeClaim, error) {
	allPVCsMap := make(map[string]v1.PersistentVolumeClaim)

	allPVCs, err := client.CoreV1().PersistentVolumeClaims(namespace).List(ctx, metav1.ListOptions{
		LabelSelector: selectLabel,
	})
	if err != nil {
		return allPVCsMap, err
	}

	for _, pvcItem := range allPVCs.Items {
		allPVCsMap[pvcItem.Name] = pvcItem
	}
	return allPVCsMap, nil
}

// Check if internal FSS to check for sibling replica bound PVCs is enabled.
func isSiblingReplicaBoundPvcFSSEnabled(ctx context.Context) bool {
	log := logger.GetLogger(ctx)

	containerOrchestratorUtility := commonco.ContainerOrchestratorUtility
	if containerOrchestratorUtility == nil {
		clusterFlavor, err := cnsconfig.GetClusterFlavor(ctx)
		if err != nil {
			log.Debugf("Failed retrieving cluster flavor. Error: %v", err)
			return false
		}
		containerOrchestratorUtility, err = commonco.GetContainerOrchestratorInterface(ctx,
			common.Kubernetes, clusterFlavor, *admissionhandler.COInitParams)
		if err != nil {
			log.Debugf("failed to get k8s interface. err: %v", err)
			return false
		}
	}

	return containerOrchestratorUtility.IsFSSEnabled(ctx, common.SiblingReplicaBoundPvcCheck)
}

// eliminateNodesWithPvcOfSiblingReplica filters out the nodes that have
// bound PVCs of sibling replicas. It finds cousin PVCs of currPVC and
// elimates all nodes which have at least one cousin PVC placed on them.
func eliminateNodesWithPvcOfSiblingReplica(ctx context.Context, client kubernetes.Interface,
	currPVC *v1.PersistentVolumeClaim, candidateHosts []string) ([]string, error) {
	log := logger.GetLogger(ctx)

	if !isSiblingReplicaBoundPvcFSSEnabled(ctx) {
		log.Infof("FSS to check for sibling replica's PVCs is not enabled.")
		return candidateHosts, nil
	}

	// Proceed only if it is a vDPP PVC.
	pvcLabels := currPVC.GetLabels()
	appplatformLabelVal, labelOk := pvcLabels[appplatformLabel]
	if !labelOk {
		log.Infof("Not a vDPP PVC %+v, skip this step.", currPVC.Name)
		return candidateHosts, nil
	}

	// Do not proceed if it is a standalone pod.
	pvcAnnotation := currPVC.ObjectMeta.Annotations
	if _, ok := pvcAnnotation[nodeAffinityAnnotationKey]; ok {
		log.Infof("Node affinity key already specified for PVC %+v, skip this step.", currPVC.Name)
		return candidateHosts, nil
	}

	if val, ok := pvcLabels[siblingReplicaCheckOptOutLabel]; ok && val == "true" {
		// siblingReplicaCheckOptOutLabel is opted in by default unless specified otherwise.
		log.Infof("Sibling Replica Bound PVC check is not opted, skip this step for PVC %s.", currPVC.Name)
		return candidateHosts, nil
	}

	cousinPVCs, err := getCousinPVCs(ctx, client, currPVC)
	if err != nil {
		return candidateHosts, err
	}

	selectLabel := fmt.Sprintf("%s=%s", appplatformLabel, appplatformLabelVal)
	allPVCs, err := getAllPvcs(ctx, client, currPVC.Namespace, selectLabel)
	if err != nil {
		return candidateHosts, err
	}

	for _, cousinPVC := range cousinPVCs {
		pvc, ok := allPVCs[cousinPVC]
		if !ok {
			// This scenario can happen if psp-operator PVC watcher has already
			// deleted the PVC.
			log.Errorf("Failed to find PVC object for PVC %s", cousinPVC)
			continue
		}

		pvName := pvc.Spec.VolumeName
		pv, err := client.CoreV1().PersistentVolumes().Get(ctx, pvName, metav1.GetOptions{})
		if err != nil || pv == nil {
			if pvName == "" {
				continue
			}
			return candidateHosts, err
		}

		// Volume Binding has to be WaitForFirstConsumer because it can only be
		// Immediate if it has the annotation failure-domain.beta.vmware.com/node
		// on the PVC. We have already verified that this annotation is not
		// present on PVC as part of the validatin above. Hence, nodeAffinity
		// value will always be defined on the PV.
		hostName := pv.Spec.NodeAffinity.Required.NodeSelectorTerms[0].MatchExpressions[0].Values[0]

		for i, host := range candidateHosts {
			if host == hostName {
				candidateHosts = filterHost(candidateHosts, i)
				break
			}
		}
	}

	return candidateHosts, nil

}

// PlacePVConStoragePool selects target storage pool to place the given PVC
// based on its profile and the topology information. If the placement is
// successful, the PVC will be annotated with the selected storage pool and
// PlacePVConStoragePool nil that means no error. For unsuccessful placement,
// PlacePVConStoragePool returns error that cause the failure.
func PlacePVConStoragePool(ctx context.Context, client kubernetes.Interface,
	tops *csi.TopologyRequirement, curPVC *v1.PersistentVolumeClaim, spType string) error {
	log := logger.GetLogger(ctx)

	// Validate accessibility requirements.
	if tops == nil {
		stampPVCWithError(ctx, client, curPVC, invalidParamsErr)
		return fmt.Errorf("invalid accessibility requirements input provided")
	}

	// Get all StoragePool list.
	sps, err := getStoragePoolList(ctx)
	if err != nil {
		log.Errorf("Fail to get StoragePool list with %+v", err)
		stampPVCWithError(ctx, client, curPVC, genericErr)
		return err
	}

	if len(sps.Items) == 0 {
		// There is no available storage pools.
		stampPVCWithError(ctx, client, curPVC, notEnoughResErr)
		return fmt.Errorf("fail to find any storage pool")
	}

	hostNames, err := getHostCandidates(ctx, curPVC, tops)
	if err != nil {
		log.Errorf("Fail to get Host candidates with %+v", err)
		stampPVCWithError(ctx, client, curPVC, invalidConfigErr)
		return err
	}

	if len(hostNames) == 0 {
		// There are no candidate hosts available.
		stampPVCWithError(ctx, client, curPVC, notEnoughResErr)
		return fmt.Errorf("failed to find any candidate nodes")
	}

	log.Infof("filtered host candidates: %v", hostNames)

	capacity := curPVC.Spec.Resources.Requests[v1.ResourceName(v1.ResourceStorage)]
	volSizeBytes := capacity.Value()

	// Sequence placement operations beyond this point to avoid race conditions.
	// To protect the storage pool snapshot unpopulated for placement of PVCs
	// with the same label.
	// TODO Optimization of lock scope by both persistence service and node name.
	pvcPlacementMutex.Lock()
	defer pvcPlacementMutex.Unlock()

	// Filter candidate hosts further to elimate nodes that may have bound PVCs
	// of sibling replicas.
	hostNames, err = eliminateNodesWithPvcOfSiblingReplica(ctx, client, curPVC, hostNames)
	if err != nil {
		// Ignore error as this should not be a blocker for placing PVCs.
		// A manual workaround is already documented.
		log.Debugf("Failed to filter hosts to eliminate nodes that may have bound PVCs of sibling replicas.  Err: %+v", err)
	} else {
		// If there are no candidate hosts left, it means there are bound PVCs
		// of a sibling replica on all nodes.
		if len(hostNames) == 0 {
			stampPVCWithError(ctx, client, curPVC, siblingReplicaBoundPVCErr)
			return fmt.Errorf("no candidate hosts left for PVC %s. All nodes have bound PVCs of sibling replicas", curPVC.Name)
		}
	}

	// We require list of PVC for placement guided by anti-affinity labels.
	pvcList, err := client.CoreV1().PersistentVolumeClaims(v1.NamespaceAll).List(ctx, metav1.ListOptions{})
	if err != nil {
		log.Errorf("Failed to retrieve PVCs from all namespace from API server")
		stampPVCWithError(ctx, client, curPVC, genericErr)
		return err
	}

	assignedSP, err := getSPForPVCPlacement(ctx, client, curPVC, volSizeBytes,
		sps.Items, hostNames, pvcList.Items, spType, true)
	if err != nil {
		log.Errorf("Failed to find any SP to place PVC %v. Error :%v", curPVC.Name, err)
		// We have already stamped PVC with corresponding error.
		return err
	}

	err = setPVCAnnotation(ctx, assignedSP.Name, client, curPVC)
	if err != nil {
		log.Errorf("setPVCAnnotation failed with %+v", err)
		return err
	}

	return nil
}

// getStoragePoolList get all storage pool list.
func getStoragePoolList(ctx context.Context) (*unstructured.UnstructuredList, error) {
	log := logger.GetLogger(ctx)

	cfg, err := clientconfig.GetConfig()
	if err != nil {
		log.Errorf("Failed to get Kubernetes config in PSP PE. Err: %+v", err)
		return nil, err
	}

	spClient, err := dynamic.NewForConfig(cfg)
	if err != nil {
		log.Errorf("Failed to create StoragePool client using config. Err: %+v", err)
		return nil, err
	}

	spResource := schema.GroupVersion{Group: apis.GroupName, Version: apis.Version}.WithResource(resourceName)

	// TODO Enable label on each storage pool and use label as filter storage
	// pool list.
	sps, err := spClient.Resource(spResource).List(ctx, metav1.ListOptions{
		LabelSelector: spTypeLabelKey,
	})
	if err != nil {
		log.Errorf("Failed to get StoragePool list. Error: %+v", err)
		return nil, err
	}
	return sps, err
}

// preFilterSPList filter out candidate storage pool list through topology and
// capacity.
// XXX TODO Add health of storage pools together as a filter when related
// metrics available.
func preFilterSPList(ctx context.Context, sps []unstructured.Unstructured,
	storageClassName string, hostNames []string, volSizeBytes int64) ([]StoragePoolInfo, error) {
	log := logger.GetLogger(ctx)
	spList := []StoragePoolInfo{}

	totalStoragePools := len(sps)
	nonVsanDirectOrSna := 0
	nonSCComp := 0
	topology := 0
	unhealthy := 0
	underDiskDecomm := 0
	notEnoughCapacity := 0

	for _, sp := range sps {
		spName := sp.GetName()
		spType, found, err := unstructured.NestedString(sp.Object, "metadata", "labels", spTypeLabelKey)
		if !found || err != nil || (spType != vsanDirect && spType != vsanSna) {
			nonVsanDirectOrSna++
			continue
		}

		// sc compatible filter.
		scs, found, err := unstructured.NestedStringSlice(sp.Object, "status", "compatibleStorageClasses")
		if !found || err != nil {
			nonSCComp++
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
			nonSCComp++
			continue
		}

		if !isStoragePoolHealthy(ctx, sp) {
			unhealthy++
			continue
		}

		if !isStoragePoolAccessibleByNodes(ctx, sp, hostNames) {
			topology++
			continue
		}

		if isStoragePoolInDiskDecommission(ctx, sp) {
			underDiskDecomm++
			continue
		}

		// The storage pool capacity is expressed in raw bytes.
		spSize, found, err := unstructured.NestedInt64(sp.Object, "status", "capacity", "allocatableSpace")
		if !found || err != nil {
			notEnoughCapacity++
			continue
		}

		if spSize > volSizeBytes { // Filter by capacity.
			spList = append(spList, StoragePoolInfo{
				Name:                  spName,
				AllocatableCapInBytes: spSize,
			})
		} else {
			notEnoughCapacity++
		}
	}

	log.Infof("TotalPools:%d, Usable:%d. Pools removed because not local:%d, SC mis-match:%d, "+
		"Unhealthy: %d, topology mis-match: %d, under disk decommission: %d, out of capacity: %d",
		totalStoragePools, len(spList), nonVsanDirectOrSna, nonSCComp, unhealthy, topology,
		underDiskDecomm, notEnoughCapacity)
	return spList, nil
}

// isStoragePoolHealthy checks if the datastores in StoragePool 'sp' is healthy.
func isStoragePoolHealthy(ctx context.Context, sp unstructured.Unstructured) bool {
	log := logger.GetLogger(ctx)
	spName := sp.GetName()
	message, found, err := unstructured.NestedString(sp.Object, "status", "error", "message")
	if err != nil {
		log.Debug("failed to fetch the storage pool health: ", err)
		return false
	}
	if !found {
		log.Debug(spName, " is healthy")
		return true
	}
	log.Debug(spName, " is unhealthy. Reason: ", message)
	return false
}

func isStoragePoolInDiskDecommission(ctx context.Context, sp unstructured.Unstructured) bool {
	log := logger.GetLogger(ctx)
	spName := sp.GetName()
	drainMode, found, err := unstructured.NestedString(sp.Object, "spec", "parameters", diskDecommissionModeField)
	if err != nil {
		log.Debugf("failed to fetch labels of storage pool %v. Err: %v", spName, err)
		return true
	}
	if !found {
		return false
	}
	log.Debugf("%v is under disk decommission with status %v", spName, drainMode)
	return true
}

// isStoragePoolAccessibleByNodes filter out accessible storage pools from
// a given list of candidate nodes.
func isStoragePoolAccessibleByNodes(ctx context.Context, sp unstructured.Unstructured, hostNames []string) bool {
	nodes, found, err := unstructured.NestedStringSlice(sp.Object, "status", "accessibleNodes")
	if !found || err != nil {
		return false
	}

	for _, host := range hostNames { // filter by node candidate list.
		for _, node := range nodes {
			if node == host {
				return true
			}
		}
	}
	return false
}

// Remove the sp from the given list.
func removeSPFromList(spList []StoragePoolInfo, spName string) []StoragePoolInfo {
	for i, sp := range spList {
		if sp.Name == spName {
			copy(spList[i:], spList[i+1:])
			return spList[:len(spList)-1]
		}
	}
	return spList
}

// Update used capacity of the storage pool based on the volume size of the
// pending PVC on it. Also, if this ends up removing the sp from the list if
// its free capacity falls to 0.
func updateSPCapacityUsage(spList []StoragePoolInfo, spName string, pendingPVBytes int64,
	curPVBytes int64) (bool, bool, []StoragePoolInfo) {
	usageUpdated := false
	spRemoved := false
	for i := range spList {
		if spList[i].Name == spName {
			if spList[i].AllocatableCapInBytes > pendingPVBytes {
				spList[i].AllocatableCapInBytes -= pendingPVBytes
				if spList[i].AllocatableCapInBytes > curPVBytes {
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

// handleUsedStoragePools finds all storage pools that have been used by other
// PVCs on the same node and either removes them if they dont satisfy the
// anti-affinity rules and/or updates their usage based on any pending PVs
// against the sp.
func handleUsedStoragePools(ctx context.Context, pvc *v1.PersistentVolumeClaim, volSizeBytes int64,
	spList []StoragePoolInfo, pvcList []v1.PersistentVolumeClaim, spType string) []StoragePoolInfo {
	log := logger.GetLogger(ctx)

	var requiredAntiAffinityValue, preferredAntiAffinityValue string
	var required, preferred bool
	if strings.Contains(spType, vsanDirectType) {
		requiredAntiAffinityValue, required = pvc.Annotations[spPolicyAntiRequired]
		preferredAntiAffinityValue, preferred = pvc.Annotations[spPolicyAntiPreferred]
	}

	curNamespace := pvc.Namespace
	xAffinity := 0

	usedSPList := []StoragePoolInfo{}
	for _, pvcItem := range pvcList {
		if pvcItem.Namespace != curNamespace {
			continue
		}

		spName, ok := pvcItem.Annotations[StoragePoolAnnotationKey]
		if !ok {
			continue
		}

		// Is this even a SP we care about anyway?
		if !isSPInList(spName, spList) {
			continue
		}

		if pvcItem.Name == pvc.GetName() {
			continue
		}

		// If required anti-affinity is set of the PVC, remove any SPs that are
		// already used.
		if required {
			antiAffinityValue, setRequired := pvcItem.Annotations[spPolicyAntiRequired]
			if setRequired && antiAffinityValue == requiredAntiAffinityValue {
				spList = removeSPFromList(spList, spName)
				xAffinity++
				continue
			}
		}

		// If preferred antiaffinity is set of the PVC, make a list of SPs that
		// are already used.
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

	log.Infof("Removed because of: affinity rules:%d. Usable Pools:%d, Pools already used at least once:%d",
		xAffinity, len(spList), len(usedSPList))

	// If we have any unused SPs then we can just remove all used SPs from the
	// list. This gives us a small set of unused SPs that we can re-sort below.
	if len(spList) > len(usedSPList) {
		for _, sp := range usedSPList {
			spList = removeSPFromList(spList, sp.Name)
		}
	}

	return spList
}

// setPVCAnnotation add annotation of selected storage pool to targeted PVC.
func setPVCAnnotation(ctx context.Context, spName string, client kubernetes.Interface,
	curPVC *v1.PersistentVolumeClaim) error {
	log := logger.GetLogger(ctx)

	if spName == "" {
		return nil
	}
	if curValue := curPVC.Annotations[StoragePoolAnnotationKey]; curValue == spName {
		return nil
	}

	patch := map[string]interface{}{
		"metadata": map[string]interface{}{
			"annotations": map[string]string{
				StoragePoolAnnotationKey: spName,
			},
		},
	}

	patchBytes, err := json.Marshal(patch)
	if err != nil {
		log.Errorf("Fail to marshal patch: %+v", err)
		return err
	}

	curPVC, err = client.CoreV1().PersistentVolumeClaims(curPVC.Namespace).Patch(ctx, curPVC.Name,
		k8stypes.MergePatchType, patchBytes, metav1.PatchOptions{})
	if err != nil {
		log.Errorf("Fail to update PVC %+v", err)
		return err
	}

	log.Infof("Picked sp %s for PVC %s", curPVC.Annotations[StoragePoolAnnotationKey], curPVC.Name)
	return nil
}

// GetSCNameFromPVC gets name of the storage class from provided PVC.
func GetSCNameFromPVC(pvc *v1.PersistentVolumeClaim) (string, error) {
	scName := pvc.Spec.StorageClassName
	if scName == nil || *scName == "" {
		scNameFromAnnotation := pvc.Annotations[ScNameAnnotationKey]
		if scNameFromAnnotation == "" {
			return "", fmt.Errorf("storage class name not specified in PVC %q", pvc.Name)
		}
		scName = &scNameFromAnnotation
	}
	return *scName, nil
}

// getHostNamesFromTopology get all candidate hosts from topology requirements.
func getHostNamesFromTopology(ctx context.Context, topologyRequirement *csi.TopologyRequirement) []string {
	log := logger.GetLogger(ctx)

	hostNames := []string{}
	if topologyRequirement == nil || topologyRequirement.GetPreferred() == nil {
		log.Infof("Found no Accessibility requirements")
		return hostNames
	}

	for _, topology := range topologyRequirement.GetPreferred() {
		if topology == nil {
			log.Infof("Get invalid accessibility requirement %v", topology)
			continue
		}
		value, ok := topology.Segments[v1.LabelHostname]
		if !ok {
			log.Infof("Found no hostname in the accessibility requirements %v", topology)
			continue
		}
		hostNames = append(hostNames, value)
	}
	return hostNames
}

// getHostCandidates returns the hostnames where the placement can be done.
// Returns error if there is a misconfiguration.
func getHostCandidates(ctx context.Context, curPVC *v1.PersistentVolumeClaim,
	tops *csi.TopologyRequirement) ([]string, error) {
	log := logger.GetLogger(ctx)
	candidateHostsFromTopology := getHostNamesFromTopology(ctx, tops)

	if affineToHostName, present := curPVC.ObjectMeta.Annotations[nodeAffinityAnnotationKey]; present {
		log.Infof("Got affinity parameter host name: %s", affineToHostName)
		// Abort placement if the affineToHostName is not present in the topology
		// requirement.
		if !isHostPresentInTopology(affineToHostName, candidateHostsFromTopology) {
			return nil, fmt.Errorf("invalid configuration - PVC nodeAffinity conflicts with Pod topology requirement")
		}
		return []string{affineToHostName}, nil
	}
	return candidateHostsFromTopology, nil
}

// isHostPresentInTopology checks if the affinityHost present in the list of
// hostnames specified in the topology requirement.
func isHostPresentInTopology(affinityHost string, topologyHosts []string) bool {
	for _, host := range topologyHosts {
		if strings.EqualFold(affinityHost, host) {
			return true
		}
	}
	return false
}
