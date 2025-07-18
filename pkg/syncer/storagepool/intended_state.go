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
	"errors"
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
	types2 "k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"sigs.k8s.io/vsphere-csi-driver/v3/pkg/apis/storagepool/cns/v1alpha1"
	cnsvsphere "sigs.k8s.io/vsphere-csi-driver/v3/pkg/common/cns-lib/vsphere"
	"sigs.k8s.io/vsphere-csi-driver/v3/pkg/csi/service/logger"
	csitypes "sigs.k8s.io/vsphere-csi-driver/v3/pkg/csi/types"
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
	vc         *cnsvsphere.VirtualCenter
	clusterIDs []string
	// intendedStateMap stores the datastoreMoid -> IntendedState for each
	// datastore. Underlying map type map[string]*intendedState.
	intendedStateMap sync.Map
}

func newSPController(vc *cnsvsphere.VirtualCenter, clusterIDs []string) (*SpController, error) {
	return &SpController{
		vc:         vc,
		clusterIDs: clusterIDs,
	}, nil
}

// newIntendedState creates a new IntendedState for a StoragePool.
func newIntendedState(ctx context.Context, clusterID string, ds *cnsvsphere.DatastoreInfo,
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

	// Get datastore properties like capacity, freeSpace, dsURL, dsType,
	// accessible, inMM, containerID.
	dsProps := getDatastoreProperties(ctx, ds)
	if dsProps == nil || dsProps.capacity == nil || dsProps.freeSpace == nil {
		err := fmt.Errorf("error fetching datastore properties for %v", ds.Reference().Value)
		log.Error(err)
		return nil, err
	}

	remoteVsan, err := isRemoteVsan(ctx, dsProps, clusterID, vcClient.Client)
	if err != nil {
		log.Errorf("Not able to determine whether Datastore is vSAN Remote. %+v", err)
		return nil, err
	}

	nodesMap, err := findAccessibleNodes(ctx, ds.Datastore.Datastore, clusterID, vcClient.Client)
	if err != nil {
		log.Errorf("Error finding accessible nodes of datastore %v. Err: %+v", ds, err)
		return nil, err
	}

	// Only add nodes that are not in MM to the list of nodes.
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
	if dsPolicies, ok := dsPolicyCompatMap[ds.Reference().Value]; ok {
		for _, policyID := range dsPolicies {
			for scName := range scWatchCntlr.policyToScMap[policyID] {
				compatSC = append(compatSC, scName)
			}
		}
	} else {
		log.Infof("Failed to get compatible policies for %s", ds.Reference().Value)
	}
	return &intendedState{
		dsMoid:           ds.Reference().Value,
		dsType:           dsProps.dsType,
		spName:           makeStoragePoolName(ds.Info.Name),
		capacity:         dsProps.capacity,
		freeSpace:        dsProps.freeSpace,
		allocatableSpace: getAllocatableSpace(dsProps.freeSpace, dsProps.dsType),
		url:              dsProps.dsURL,
		accessible:       dsProps.accessible,
		datastoreInMM:    dsProps.inMM,
		allHostsInMM:     allNodesInMM,
		nodes:            nodes,
		compatSC:         compatSC,
		isRemoteVsan:     remoteVsan,
	}, nil
}

func isRemoteVsan(ctx context.Context, dsprops *dsProps,
	clusterID string, vcclient *vim25.Client) (bool, error) {
	log := logger.GetLogger(ctx).Named("isRemoteVsan")
	// If datastore type is not vsan, then return false.
	if dsprops.dsType != vsanDsType {
		log.Debugf("Datastore %s is not a vSAN datastore", dsprops.dsName)
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
	if dsContainerID == vsanClsUUID {
		log.Debugf("vSAN Datastore %s is not remote to this cluster", dsprops.dsName)
		return false, nil
	}

	log.Debugf("vSAN Datastore %s is remote to this cluster", dsprops.dsName)
	return true, nil
}

// newIntendedVsanSNAState creates a new IntendedState for a sna StoragePool.
func newIntendedVsanSNAState(ctx context.Context, scWatchCntlr *StorageClassWatch, vsan *intendedState, node string,
	vsanHost *cnsvsphere.VsanHostCapacity) (*intendedState, error) {
	log := logger.GetLogger(ctx)
	nodes := make([]string, 0)
	nodes = append(nodes, node)

	log.Debugf("creating vsan sna sp %q", node)
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

	var hosts []*cnsvsphere.HostSystem
	for _, clusterID := range c.clusterIDs {
		// Fetch all hosts in VC cluster.
		clusterHosts, err := c.vc.GetHostsByCluster(ctx, clusterID)
		if err != nil {
			log.Errorf("Failed to find datastores from VC. Err: %+v", err)
			continue
		}

		hosts = append(hosts, clusterHosts...)
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
		log.Debugf("Host %s has capacity %+v", nodeName, capacity)
		out[nodeName] = capacity
	}
	return out, nil
}

// applyIntendedState applies the given in-memory IntendedState on to the
// actual state of a StoragePool in the WCP cluster.
func (c *SpController) applyIntendedState(ctx context.Context, state *intendedState) error {
	log := logger.GetLogger(ctx)
	k8sClient, err := getK8sClient(ctx)
	if err != nil {
		return err
	}

	// Get StoragePool with spName and Update if already present, otherwise
	// Create resource.
	sp := &v1alpha1.StoragePool{}
	err = k8sClient.Get(ctx, types2.NamespacedName{Name: state.spName}, sp)
	if err != nil {
		var statusErr *k8serrors.StatusError
		ok := errors.As(err, &statusErr)
		if ok && statusErr.Status().Reason == metav1.StatusReasonNotFound {
			log.Infof("Creating StoragePool instance for %s", state.spName)
			sp, err = state.createStoragePool(ctx, k8sClient)
			if err != nil {
				log.Errorf("Error creating StoragePool %s. Err: %s", state.spName, err)
				return err
			}

			log.Debug("Successfully created StoragePool " + sp.Name)
		}
	} else {
		// StoragePool already exists, so Update it. We don't expect
		// ConflictErrors since updates are synchronized with a lock.
		log.Debugf("Updating StoragePool instance for %s", state.spName)
		sp, err = state.updateStoragePool(ctx, k8sClient, *sp)
		if err != nil {
			log.Errorf("Error updating StoragePool %s. Err: %+v", state.spName, err)
			return err
		}

		log.Debug("Successfully updated StoragePool " + sp.Name)
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
	k8sClient, err := getK8sClient(ctx)
	if err != nil {
		return err
	}

	sp := &v1alpha1.StoragePool{}
	err = k8sClient.Get(ctx, types2.NamespacedName{Name: spName}, sp)
	if err != nil {
		log.Errorf("Error getting StoragePool instance %s. Err: %s", spName, err)
		return err
	}

	if sp.Spec.Driver != csitypes.Name {
		log.Info("Cannot delete StoragePool " + spName + " as it is not created by " + csitypes.Name)
		return nil
	}

	log.Infof("Deleting StoragePool " + spName)
	err = k8sClient.Delete(ctx, sp, client.GracePeriodSeconds(0))
	if err != nil {
		log.Errorf("Error deleting StoragePool %s. Err: %s", spName, err)
		return err
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
	if len(state.nodes) == 0 {
		return v1alpha1.SpErrors[v1alpha1.ErrStateNoAccessibleHosts]
	}
	return nil
}

func (state *intendedState) createStoragePool(ctx context.Context, c client.Client) (*v1alpha1.StoragePool, error) {
	sp := &v1alpha1.StoragePool{
		ObjectMeta: metav1.ObjectMeta{
			Name: state.spName,
			Labels: map[string]string{
				spTypeLabelKey: strings.ReplaceAll(state.dsType, spTypePrefix, ""),
			},
		},
		Spec: v1alpha1.StoragePoolSpec{
			Driver: csitypes.Name,
			Parameters: map[string]string{
				"datastoreUrl": state.url,
			},
		},
		Status: v1alpha1.StoragePoolStatus{
			AccessibleNodes:          state.nodes,
			CompatibleStorageClasses: state.compatSC,
			Capacity: &v1alpha1.PoolCapacity{
				Total:            state.capacity,
				FreeSpace:        state.freeSpace,
				AllocatableSpace: state.allocatableSpace,
			},
			Error: state.getStoragePoolError(),
		},
	}
	err := c.Create(ctx, sp)
	return sp, err
}

func (state *intendedState) updateStoragePool(ctx context.Context, c client.Client,
	sp v1alpha1.StoragePool) (*v1alpha1.StoragePool, error) {
	sp.Spec.Parameters["datastoreUrl"] = state.url
	sp.ObjectMeta.Labels[spTypeLabelKey] = strings.ReplaceAll(state.dsType, spTypePrefix, "")
	sp.Status.Capacity.Total = state.capacity
	sp.Status.Capacity.FreeSpace = state.freeSpace
	sp.Status.Capacity.AllocatableSpace = state.allocatableSpace
	sp.Status.AccessibleNodes = state.nodes
	sp.Status.CompatibleStorageClasses = state.compatSC
	sp.Status.Error = state.getStoragePoolError()

	err := c.Update(ctx, &sp)
	return &sp, err
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
