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
	"math"
	"strings"
	"sync"

	"github.com/vmware/govmomi/object"
	"github.com/vmware/govmomi/vim25"
	"github.com/vmware/govmomi/vim25/mo"
	"github.com/vmware/govmomi/vim25/types"
	k8serrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"

	"sigs.k8s.io/vsphere-csi-driver/v2/pkg/apis/storagepool/cns/v1alpha1"
	cnsvsphere "sigs.k8s.io/vsphere-csi-driver/v2/pkg/common/cns-lib/vsphere"
	"sigs.k8s.io/vsphere-csi-driver/v2/pkg/csi/service/logger"
	csitypes "sigs.k8s.io/vsphere-csi-driver/v2/pkg/csi/types"
)

const (
	changeThresholdB = 1 * 1024 * 1024 * 1024
	// Maximum overhead space consumed during provision of a persistent volume
	// on vSAN Direct datastore. This number is empirically obtained and can
	// change with changes in underlying storage layer.
	vsanDirectOverheadSpaceB = 4 * 1024 * 1024
	// This denotes the percentage of free space with can be utilised to
	// provision persistent volumes on vSAN SNA after considering the overhead.
	// This number is empirically obtained and can change with changes in
	// underlying storage layer.
	vsanSnaUtilisationPercentage = 0.96
)

// intendedState of a StoragePool from Datastore properties fetched from VC as
// source of truth.
type intendedState struct {
	// Datastore moid in VC.
	dsMoid string
	// Datastore type in VC.
	dsType string
	// StoragePool name derived from datastore's name.
	spName string
	// From Datastore.summary.capacity in VC.
	capacity *resource.Quantity
	// From Datastore.summary.freeSpace in VC.
	freeSpace *resource.Quantity
	// Obtained after subtracting overhead from free space. for vSAN-SNA its 4%
	// less of free space, for vSAN-Direct its 4 MiB less of free space.
	// For vSAN default overhead depends on type of policy used, currently its
	// also 4% less of free space.
	allocatableSpace *resource.Quantity
	// From Datastore.summary.Url.
	url string
	// From Datastore.summary.Accessible.
	accessible bool
	// From Datastore.summary.maintenanceMode.
	datastoreInMM bool
	// True only when all hosts this Datastore is mounted on is in MM.
	allHostsInMM bool
	// Accessible list of k8s nodes on which this datastore is mounted in VC
	// cluster.
	nodes []string
	// Compatible list of StorageClass names computed from SPBM.
	compatSC []string
	// Is a remote vSAN Datastore mounted into this cluster - HCI Mesh feature.
	isRemoteVsan bool
}

// SpController holds the intended state updated by property collector listener
// and has methods to apply intended state into actual k8s state.
type SpController struct {
	vc        *cnsvsphere.VirtualCenter
	clusterID string
	// intendedStateMap stores the datastoreMoid -> IntendedState for each
	// datastore. Underlying map type map[string]*intendedState.
	intendedStateMap sync.Map
}

func newSPController(vc *cnsvsphere.VirtualCenter, clusterID string) (*SpController, error) {
	return &SpController{
		vc:        vc,
		clusterID: clusterID,
	}, nil
}

// newIntendedState creates a new IntendedState for a StoragePool.
func newIntendedState(ctx context.Context, ds *cnsvsphere.DatastoreInfo,
	scWatchCntlr *StorageClassWatch) (*intendedState, error) {
	log := logger.GetLogger(ctx)

	// Shallow copy VC to prevent nil pointer dereference exception caused due
	// to vc.Disconnect func running in parallel.
	vc := *scWatchCntlr.vc
	err := vc.Connect(ctx)
	if err != nil {
		log.Errorf("failed to connect to vCenter. Err: %+v", err)
		return nil, err
	}
	vcClient := vc.Client

	clusterID := scWatchCntlr.clusterID
	spName := makeStoragePoolName(ds.Info.Name)

	// Get datastore properties like capacity, freeSpace, dsURL, dsType,
	// accessible, inMM, containerID.
	dsProps := getDatastoreProperties(ctx, ds)
	if dsProps == nil || dsProps.capacity == nil || dsProps.freeSpace == nil {
		err := fmt.Errorf("error fetching datastore properties for %v", ds.Reference().Value)
		log.Error(err)
		return nil, err
	}

	nodesMap, err := findAccessibleNodes(ctx, ds.Datastore.Datastore, clusterID, vcClient.Client)
	if err != nil {
		log.Errorf("Error finding accessible nodes of datastore %v. Err: %+v", ds, err)
		return nil, err
	}

	// Only add nodes that are not inMM to the list of nodes.
	nodes := make([]string, 0)
	// allNodesInMM makes sense to be true only when there are nodes visible
	// for this datastore.
	allNodesInMM := len(nodesMap) != 0
	for node, inMM := range nodesMap {
		if !inMM {
			nodes = append(nodes, node)
			allNodesInMM = false
		}
	}

	dsPolicyCompatMap, err := scWatchCntlr.getDatastoreToPolicyCompatibility(ctx, []*cnsvsphere.DatastoreInfo{ds}, true)
	if err != nil {
		log.Errorf("Failed to get dsPolicyCompatMap. Err: %+v", err)
		return nil, err
	}
	compatSC := make([]string, 0)
	dsPolicies, ok := dsPolicyCompatMap[ds.Reference().Value]
	if ok {
		for _, policyID := range dsPolicies {
			scName := scWatchCntlr.policyToScMap[policyID].Name
			compatSC = append(compatSC, scName)
		}
	} else {
		log.Infof("Failed to get compatible policies for %s", ds.Reference().Value)
	}

	remoteVsan, err := isRemoteVsan(ctx, dsProps, clusterID, vcClient.Client)
	if err != nil {
		log.Errorf("Not able to determine whether Datastore is vSAN Remote. %+v", err)
		return nil, err
	}

	allocatableSpace := getAllocatableSpace(dsProps.freeSpace, dsProps.dsType)

	return &intendedState{
		dsMoid:           ds.Reference().Value,
		dsType:           dsProps.dsType,
		spName:           spName,
		capacity:         dsProps.capacity,
		freeSpace:        dsProps.freeSpace,
		allocatableSpace: allocatableSpace,
		url:              dsProps.dsURL,
		accessible:       dsProps.accessible,
		datastoreInMM:    dsProps.inMM,
		allHostsInMM:     allNodesInMM,
		nodes:            nodes,
		compatSC:         compatSC,
		isRemoteVsan:     remoteVsan,
	}, nil
}

func isRemoteVsan(ctx context.Context, dsprops *dsProps, clusterID string, vcclient *vim25.Client) (bool, error) {
	log := logger.GetLogger(ctx)
	// If datastore type is not vsan, then return false.
	if dsprops.dsType != vsanDsType {
		return false, nil
	}
	// Get vsan cluster uuid.
	clsMoRef := types.ManagedObjectReference{
		Type:  "ClusterComputeResource",
		Value: clusterID,
	}
	cls := object.NewClusterComputeResource(vcclient, clsMoRef)
	var clsMo mo.ClusterComputeResource
	err := cls.Properties(ctx, clsMoRef, []string{"configurationEx"}, &clsMo)
	if err != nil {
		log.Errorf("Error fetching cluster properties for %s. Err: %+v", clusterID, err)
		return false, err
	}
	vsanClsUUID := clsMo.ConfigurationEx.(*types.ClusterConfigInfoEx).VsanConfigInfo.DefaultConfig.Uuid
	vsanClsUUID = strings.ReplaceAll(vsanClsUUID, "-", "")
	dsContainerID := strings.ReplaceAll(dsprops.containerID, "-", "")
	log.Debugf("Verifying whether vSAN Datastore %s with containerID: %s is local to cluster uuid %s",
		dsprops.dsName, dsContainerID, vsanClsUUID)
	// The vsan datastore is a remote mounted one if its containerID is not the
	// same as vsan cluster uuid.
	if dsContainerID != vsanClsUUID {
		log.Infof("vSAN Datastore %s is remote to this cluster", dsprops.dsName)
		return true, nil
	}
	return false, nil
}

// newIntendedVsanSNAState creates a new IntendedState for a sna StoragePool.
func newIntendedVsanSNAState(ctx context.Context, scWatchCntlr *StorageClassWatch, vsan *intendedState, node string,
	vsanHost *cnsvsphere.VsanHostCapacity) (*intendedState, error) {
	log := logger.GetLogger(ctx)
	nodes := make([]string, 0)
	nodes = append(nodes, node)

	log.Infof("creating vsan sna sp %q", node)
	compatSC := make([]string, 0)
	for _, scName := range vsan.compatSC {
		if scWatchCntlr.isHostLocal(scName) {
			compatSC = append(compatSC, scName)
		}
	}
	// In vSAN-SNA overhead is upperbound by roughly 4% of volume requested
	// capacity. Thus maximum allocatable space in vSAN-SNA is 96% of
	// free-capacity.
	vsanSnaDsType := fmt.Sprintf("%s-sna", vsan.dsType)
	freeSpace := resource.NewQuantity(vsanHost.Capacity-vsanHost.CapacityUsed, resource.DecimalSI)
	allocatableSpace := getAllocatableSpace(freeSpace, vsanSnaDsType)

	return &intendedState{
		dsMoid:           fmt.Sprintf("%s-%s", vsan.dsMoid, node),
		dsType:           vsanSnaDsType,
		spName:           fmt.Sprintf("%s-%s", vsan.spName, node),
		capacity:         resource.NewQuantity(vsanHost.Capacity, resource.DecimalSI),
		freeSpace:        freeSpace,
		allocatableSpace: allocatableSpace,
		url:              vsan.url,
		// If this node is in accessible node list of the vsan-ds, then sp is
		// accessible.
		accessible: true,
		// If this node is inMM - the storagepool will not exist at all.
		datastoreInMM: false,
		// If this node is inMM - the storagepool will not exist at all.
		allHostsInMM: false,
		nodes:        nodes,
		compatSC:     compatSC,
		isRemoteVsan: false,
	}, nil
}

func getAllocatableSpace(freeSpace *resource.Quantity, dsType string) *resource.Quantity {
	if dsType == spTypePrefix+"vsanD" {
		// In vSAN Direct overhead to provision volume is upperbound by 4MiB
		// space. Thus maximum allocatable space in vSAN Direct is 4MiB less
		// than free-capacity.
		allocatableSpaceInBytes := freeSpace.Value() - vsanDirectOverheadSpaceB
		if allocatableSpaceInBytes > 0 {
			return resource.NewQuantity(allocatableSpaceInBytes, resource.DecimalSI)
		}
		return resource.NewQuantity(0, resource.DecimalSI)
	}
	// In vSAN-SNA overhead is upperbound by roughly 4% of volume requested
	// capacity. Thus maximum allocatable space in vSAN-SNA is 96% of
	// free-capacity. For vSAN datastores overhead is also affected by the
	// storage policy used, eg. ftt-0, ftt-1 etc.
	allocatableSpaceInBytes := int64(float64(freeSpace.Value()) * vsanSnaUtilisationPercentage)
	return resource.NewQuantity(allocatableSpaceInBytes, resource.DecimalSI)
}

// getVsanHostCapacities queries each ESX for vSAN disk capacity information.
// It tries to fetch as much host capacity information as possible, i.e. if
// there is an error fetching capacity for a particular host, maybe due to it
// becoming unresponsive, or packet drop etc., it ignores the error and tries
// to fetch capacity information for remaining hosts.
func (c *SpController) getVsanHostCapacities(ctx context.Context) (map[string]*cnsvsphere.VsanHostCapacity, error) {
	log := logger.GetLogger(ctx)
	out := make(map[string]*cnsvsphere.VsanHostCapacity)

	// Fetch all hosts in VC cluster.
	hosts, err := c.vc.GetHostsByCluster(ctx, c.clusterID)
	if err != nil {
		log.Errorf("Failed to find datastores from VC. Err: %+v", err)
		return out, err
	}

	// Get hostMoid to k8s node names.
	hostMoIDTok8sName, err := getHostMoIDToK8sNameMap(ctx)
	if err != nil {
		log.Errorf("Failed to get host Moids from k8s. Err: %+v", err)
		return out, err
	}

	// XXX: We should consider running these in threads to speed this up.
	for _, host := range hosts {
		capacity, err := host.GetHostVsanCapacity(ctx)
		if err != nil {
			log.Errorf("Failed to query vsan disks. Err: %+v", err)
			continue
		}
		nodeName, exists := hostMoIDTok8sName[host.Reference().Value]
		if !exists {
			err := fmt.Errorf("failed to find node name for %s in this cluster", host.Reference().Value)
			log.Error(err)
			continue
		}
		log.Infof("Host %s has capacity %+v", nodeName, capacity)
		out[nodeName] = capacity
	}
	return out, nil
}

// applyIntendedState applies the given in-memory IntendedState on to the
// actual state of a StoragePool in the WCP cluster.
func (c *SpController) applyIntendedState(ctx context.Context, state *intendedState) error {
	log := logger.GetLogger(ctx)
	spClient, spResource, err := getSPClient(ctx)
	if err != nil {
		return err
	}
	// Get StoragePool with spName and Update if already present, otherwise
	// Create resource.
	sp, err := spClient.Resource(*spResource).Get(ctx, state.spName, metav1.GetOptions{})
	if err != nil {
		statusErr, ok := err.(*k8serrors.StatusError)
		if ok && statusErr.Status().Reason == metav1.StatusReasonNotFound {
			log.Infof("Creating StoragePool instance for %s", state.spName)
			sp := state.createUnstructuredStoragePool(ctx)
			newSp, err := spClient.Resource(*spResource).Create(ctx, sp, metav1.CreateOptions{})
			if err != nil {
				log.Errorf("Error creating StoragePool %s. Err: %+v", state.spName, err)
				return err
			}
			log.Debugf("Successfully created StoragePool %v", newSp)
		}
	} else {
		// StoragePool already exists, so Update it. We don't expect
		// ConflictErrors since updates are synchronized with a lock.
		log.Infof("Updating StoragePool instance for %s", state.spName)
		sp := state.updateUnstructuredStoragePool(ctx, sp)
		newSp, err := spClient.Resource(*spResource).Update(ctx, sp, metav1.UpdateOptions{})
		if err != nil {
			log.Errorf("Error updating StoragePool %s. Err: %+v", state.spName, err)
			return err
		}
		log.Debugf("Successfully updated StoragePool %v", newSp)
	}

	// Update the underlying dsType in the all the compatible storage classes
	// for reverse mapping.
	for _, scName := range state.compatSC {
		if err := updateSPTypeInSC(ctx, scName, state.dsType); err != nil {
			log.Errorf("Failed to update compatibleSPTypes in storage class %s. Err: %+v", scName, err)
			continue
		}
	}
	c.intendedStateMap.Store(state.dsMoid, state)
	return nil
}

// updateIntendedState is called to apply only the DatastoreSummary properties
// of a StoragePool. This does not recompute the `accessibleNodes` or
// compatible StorageClass for this StoragePool.
func (c *SpController) updateIntendedState(ctx context.Context, dsMoid string, dsSummary types.DatastoreSummary,
	scWatchCntlr *StorageClassWatch) error {
	log := logger.GetLogger(ctx)
	state, ok := c.intendedStateMap.Load(dsMoid)
	if !ok {
		log.Debugf("Skipping update for %s", dsMoid)
		return nil
	}
	intendedState, ok := state.(*intendedState)
	if !ok {
		log.Debugf("Skipping update for %s, value stored in cache is of type %T, expected *intendedState", dsMoid, state)
		return nil
	}
	log.Debugf("Datastore: %s, StoragePool: %s", dsMoid, intendedState.spName)
	if intendedState.accessible != dsSummary.Accessible {
		// The accessible nodes are not available immediately after a PC
		// notification.
		log.Infof("Accessibility change for datastore %s. So scheduling a delayed reconcile.", dsMoid)
		scheduleReconcileAllStoragePools(ctx, reconcileAllFreq, reconcileAllIterations, scWatchCntlr, c)
		intendedState.accessible = dsSummary.Accessible
	}
	// For a vSAN datastore, update its own and all the vsan sna per host
	// capacity/freeSpace only if it has had a significant change.
	needToUpdateVsanCapacity := false
	if dsSummary.Type == "vsan" &&
		intendedState.isCapacityChangeSignificant(ctx, dsSummary.Capacity, dsSummary.FreeSpace) {
		needToUpdateVsanCapacity = true
	}

	if dsSummary.Type != "vsan" || needToUpdateVsanCapacity {
		intendedState.capacity = resource.NewQuantity(dsSummary.Capacity, resource.DecimalSI)
		intendedState.freeSpace = resource.NewQuantity(dsSummary.FreeSpace, resource.DecimalSI)
		intendedState.allocatableSpace = getAllocatableSpace(intendedState.freeSpace, spTypePrefix+dsSummary.Type)
	}
	intendedSpName := makeStoragePoolName(dsSummary.Name)
	oldSpName := intendedState.spName
	// Get the changes in properties for this Datastore into the intendedState.
	intendedState.spName = intendedSpName
	intendedState.url = dsSummary.Url
	intendedState.datastoreInMM = dsSummary.MaintenanceMode != string(types.DatastoreSummaryMaintenanceModeStateNormal)
	// Update StoragePool as per intendedState.
	if err := c.applyIntendedState(ctx, intendedState); err != nil {
		return err
	}
	if needToUpdateVsanCapacity {
		log.Debugf("Updating vSAN SNA capacity.")
		validStoragePoolNames := make(map[string]bool)
		err := c.updateVsanSnaIntendedState(ctx, intendedState, validStoragePoolNames, scWatchCntlr)
		if err != nil {
			return err
		}
	}
	if intendedSpName != oldSpName {
		// Need to also delete the older StoragePool.
		return deleteStoragePool(ctx, oldSpName)
	}
	return nil
}

// updateVsanSnaIntendedState for each host updates (creates if not exist) the
// vsan-sna StoragePool with vsan host's capacity, compatible storage classes
// etc. from standard vsan StoragePool state.
//
// @param  validStoragePoolNames: its an in-out param which captures the valid
// StoragePool. This is later used to delete extraneous StoragePool from
// kubernetes cluster whose corresponding datastore does not exist in vCenter
// cluster.
func (c *SpController) updateVsanSnaIntendedState(ctx context.Context, vsanState *intendedState,
	validStoragePoolNames map[string]bool, scWatchCntlr *StorageClassWatch) error {
	log := logger.GetLogger(ctx)
	vsanNodes := make(map[string]bool)
	for _, vsanNode := range vsanState.nodes {
		vsanNodes[vsanNode] = true
	}
	vsanHostCapacities, hostCapacityErr := c.getVsanHostCapacities(ctx)
	if hostCapacityErr != nil {
		// We are deferring return of hostCapacityErr, if any, after processing
		// vsan sna capacities to prevent cases where error in getting capacity
		// from one host would not only prevent us from updating capacities of
		// other vsan SNA StoragePool but also could lead to deletion of other
		// vsan SNA StoragePool as we did not mark these SP as valid in
		// validStoragePoolNames variable.
		log.Errorf("Error encountered fetching vSAN SNA Host capacities. Err: %v", hostCapacityErr)
	}

	for snaNode, vsanHostCapacity := range vsanHostCapacities {
		if _, ok := vsanNodes[snaNode]; !ok {
			log.Infof("Skipping vSAN SNA StoragePool for %s as it is not accessible", snaNode)
			continue
		}
		intendedSNAState, err := newIntendedVsanSNAState(ctx, scWatchCntlr, vsanState, snaNode, vsanHostCapacity)
		if err != nil {
			log.Errorf("Error reconciling StoragePool for vsan sna node %s. Err: %v", snaNode, err)
			continue
		}
		validStoragePoolNames[intendedSNAState.spName] = true
		err = c.applyIntendedState(ctx, intendedSNAState)
		if err != nil {
			log.Errorf("Error applying intended state of StoragePool %s. Err: %v", intendedSNAState.spName, err)
			continue
		}
	}
	return hostCapacityErr
}

func (c *SpController) deleteIntendedState(ctx context.Context, spName string) (deleted bool) {
	deleted = false
	c.intendedStateMap.Range(func(key, value interface{}) bool {
		state, ok := value.(*intendedState)
		if ok && state.spName == spName {
			c.intendedStateMap.Delete(key)
			deleted = true
			return false // Break the range loop.
		}
		return true // Iterate over next key, if any.
	})
	return deleted
}

func deleteStoragePool(ctx context.Context, spName string) error {
	log := logger.GetLogger(ctx)
	spClient, spResource, err := getSPClient(ctx)
	if err != nil {
		return err
	}
	sp, err := spClient.Resource(*spResource).Get(ctx, spName, metav1.GetOptions{})
	if err != nil {
		log.Errorf("Error getting StoragePool instance %s. Err: %v", spName, err)
		return err
	}
	driver, found, err := unstructured.NestedString(sp.Object, "spec", "driver")
	if found && err == nil && driver == csitypes.Name {
		log.Infof("Deleting StoragePool %s", spName)
		err := spClient.Resource(*spResource).Delete(ctx, spName, *metav1.NewDeleteOptions(0))
		if err != nil {
			log.Errorf("Error deleting StoragePool %s. Err: %v", spName, err)
			return err
		}
	}
	return nil
}

// getStoragePoolError returns the ErrCode and Message to fill within
// StoragePool.Status.Error.
func (state *intendedState) getStoragePoolError() *v1alpha1.StoragePoolError {
	if state.datastoreInMM {
		return v1alpha1.SpErrors[v1alpha1.ErrStateDatastoreInMM]
	}
	if !state.accessible {
		return v1alpha1.SpErrors[v1alpha1.ErrStateDatastoreNotAccessible]
	}
	if state.allHostsInMM {
		return v1alpha1.SpErrors[v1alpha1.ErrStateAllHostsInMM]
	}
	if state.nodes == nil || len(state.nodes) == 0 {
		return v1alpha1.SpErrors[v1alpha1.ErrStateNoAccessibleHosts]
	}
	return nil
}

func (state *intendedState) createUnstructuredStoragePool(ctx context.Context) *unstructured.Unstructured {
	sp := &unstructured.Unstructured{
		Object: map[string]interface{}{
			"apiVersion": "cns.vmware.com/v1alpha1",
			"kind":       "StoragePool",
			"metadata": map[string]interface{}{
				"name": state.spName,
				"labels": map[string]string{
					spTypeLabelKey: strings.ReplaceAll(state.dsType, spTypePrefix, ""),
				},
			},
			"spec": map[string]interface{}{
				"driver": csitypes.Name,
				"parameters": map[string]interface{}{
					"datastoreUrl": state.url,
				},
			},
			"status": map[string]interface{}{
				"accessibleNodes":          state.nodes,
				"compatibleStorageClasses": state.compatSC,
				"capacity": map[string]interface{}{
					"total":            state.capacity.Value(),
					"freeSpace":        state.freeSpace.Value(),
					"allocatableSpace": state.allocatableSpace.Value(),
				},
			},
		},
	}
	spErr := state.getStoragePoolError()
	if spErr != nil {
		setNestedField(ctx, sp.Object, spErr.State, "status", "error", "state")
		setNestedField(ctx, sp.Object, spErr.Message, "status", "error", "message")
	}
	return sp
}

func (state *intendedState) updateUnstructuredStoragePool(ctx context.Context,
	sp *unstructured.Unstructured) *unstructured.Unstructured {
	log := logger.GetLogger(ctx)
	setNestedField(ctx, sp.Object, state.url, "spec", "parameters", "datastoreUrl")
	setNestedField(ctx, sp.Object, strings.ReplaceAll(state.dsType, spTypePrefix, ""),
		"metadata", "labels", spTypeLabelKey)
	setNestedField(ctx, sp.Object, state.capacity.Value(), "status", "capacity", "total")
	setNestedField(ctx, sp.Object, state.freeSpace.Value(), "status", "capacity", "freeSpace")
	setNestedField(ctx, sp.Object, state.allocatableSpace.Value(), "status", "capacity", "allocatableSpace")
	err := unstructured.SetNestedStringSlice(sp.Object, state.nodes, "status", "accessibleNodes")
	if err != nil {
		log.Errorf("err: %v", err)
	}
	err = unstructured.SetNestedStringSlice(sp.Object, state.compatSC, "status", "compatibleStorageClasses")
	if err != nil {
		log.Errorf("err: %v", err)
	}
	spErr := state.getStoragePoolError()
	if spErr != nil {
		setNestedField(ctx, sp.Object, spErr.State, "status", "error", "state")
		setNestedField(ctx, sp.Object, spErr.Message, "status", "error", "message")
	} else {
		unstructured.RemoveNestedField(sp.Object, "status", "error")
	}
	return sp
}

func setNestedField(ctx context.Context, obj map[string]interface{}, value interface{}, fields ...string) {
	log := logger.GetLogger(ctx)
	if err := unstructured.SetNestedField(obj, value, fields...); err != nil {
		log.Errorf("err: %v", err)
	}
}

// isCapacityChangeSignificant returns true if the capacity change from old to
// new is deemed significant enough to warrant a remediation.
// Currently set to 1GB space change, somewhat arbitrarily.
func isCapacityChangeSignificant(old int64, new int64) bool {
	return math.Abs(float64(old-new)) > float64(changeThresholdB)
}

// Returns true if the freeCapacity is below a threshold (chosen to be 20% to
// match with disk capacity health) or if the change in capacity or freeSpace
// is significant.
func (state *intendedState) isCapacityChangeSignificant(ctx context.Context,
	vsanCapacity int64, vsanFreeCapacity int64) bool {
	log := logger.GetLogger(ctx)
	// Has the vSAN Datastore's freeSpace dropped below threshold?
	freeSpaceThreshold := 0.2
	vsanFreeSpaceLow := float64(vsanFreeCapacity)/float64(vsanCapacity) <= freeSpaceThreshold
	if vsanFreeSpaceLow {
		log.Debugf("vSAN freeSpace low: %v", vsanFreeCapacity)
		return true
	}

	// Has the vSAN Datastore's capacity of freeSpace changed significantly?
	vsanCapacityChanged := isCapacityChangeSignificant(vsanCapacity, state.capacity.Value())
	vsanFreeSpaceChanged := isCapacityChangeSignificant(vsanFreeCapacity, state.freeSpace.Value())
	log.Debugf("vSAN freeSpace changed significantly: %t, capacity changed significantly: %t",
		vsanFreeSpaceChanged, vsanCapacityChanged)
	return vsanCapacityChanged || vsanFreeSpaceChanged
}
