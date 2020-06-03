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
	"strings"

	"github.com/vmware/govmomi/vim25/types"
	k8serrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"

	"sigs.k8s.io/vsphere-csi-driver/pkg/apis/storagepool/cns/v1alpha1"
	cnsvsphere "sigs.k8s.io/vsphere-csi-driver/pkg/common/cns-lib/vsphere"
	"sigs.k8s.io/vsphere-csi-driver/pkg/csi/service/logger"
	csitypes "sigs.k8s.io/vsphere-csi-driver/pkg/csi/types"
)

// intendedState of a StoragePool from Datastore properties fetched from VC as source of truth
type intendedState struct {
	// Datastore moid in VC
	dsMoid string
	// Datastore type in VC
	dsType string
	// StoragePool name derived from datastore's name
	spName string
	// From Datastore.summary.capacity in VC
	capacity *resource.Quantity
	// From Datastore.summary.freeSpace in VC
	freeSpace *resource.Quantity
	// from Datastore.summary.Url
	url string
	// from Datastore.summary.Accessible
	accessible bool
	// from Datastore.summary.maintenanceMode
	datastoreInMM bool
	// true only when all hosts this Datastore is mounted on is in MM
	allHostsInMM bool
	// accessible list of k8s nodes on which this datastore is mounted in VC cluster
	nodes []string
	// compatible list of StorageClass names computed from SPBM
	compatSC []string
}

// spController holds the intended state updated by property collector listener and has methods to apply intended state
// into actual k8s state
type spController struct {
	vc        *cnsvsphere.VirtualCenter
	clusterID string
	// intendedStateMap stores the datastoreMoid -> IntendedState for each datastore
	intendedStateMap map[string]*intendedState
}

func newSPController(vc *cnsvsphere.VirtualCenter, clusterID string) (*spController, error) {
	return &spController{
		vc:               vc,
		clusterID:        clusterID,
		intendedStateMap: make(map[string]*intendedState),
	}, nil
}

// newIntendedState creates a new IntendedState for a StoragePool
func newIntendedState(ctx context.Context, ds *cnsvsphere.DatastoreInfo,
	scWatchCntlr *StorageClassWatch) (*intendedState, error) {
	log := logger.GetLogger(ctx)
	vc := scWatchCntlr.vc
	clusterID := scWatchCntlr.clusterID
	spName := makeStoragePoolName(ds.Info.Name)

	capacity, freeSpace, dsURL, dsType, accessible, inMM := getDatastoreProperties(ctx, ds)
	if capacity == nil || freeSpace == nil {
		err := fmt.Errorf("error fetching datastore properties for %v", ds.Reference().Value)
		log.Error(err)
		return nil, err
	}

	nodesMap, err := findAccessibleNodes(ctx, ds.Datastore.Datastore, clusterID, vc.Client.Client)
	if err != nil {
		log.Errorf("Error finding accessible nodes of datastore %v. Err: %+v", ds, err)
		return nil, err
	}
	nodes := make([]string, 0)
	allNodesInMM := len(nodesMap) > 0 // initialize to true if there are accessible nodes, false otherwise
	for node, inMM := range nodesMap {
		nodes = append(nodes, node)
		allNodesInMM = allNodesInMM && inMM
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
	return &intendedState{
		dsMoid:        ds.Reference().Value,
		dsType:        dsType,
		spName:        spName,
		capacity:      capacity,
		freeSpace:     freeSpace,
		url:           dsURL,
		accessible:    accessible,
		datastoreInMM: inMM,
		allHostsInMM:  allNodesInMM,
		nodes:         nodes,
		compatSC:      compatSC,
	}, nil
}

// applyIntendedState applies the given in-memory IntendedState on to the actual state of a StoragePool in the WCP cluster.
func (c *spController) applyIntendedState(ctx context.Context, state *intendedState) error {
	log := logger.GetLogger(ctx)
	spClient, spResource, err := getSPClient(ctx)
	if err != nil {
		return err
	}
	// Get StoragePool with spName and Update if already present, otherwise Create resource
	sp, err := spClient.Resource(*spResource).Get(state.spName, metav1.GetOptions{})
	if err != nil {
		statusErr, ok := err.(*k8serrors.StatusError)
		if ok && statusErr.Status().Reason == metav1.StatusReasonNotFound {
			log.Infof("Creating StoragePool instance for %s", state.spName)
			sp := state.createUnstructuredStoragePool()
			newSp, err := spClient.Resource(*spResource).Create(sp, metav1.CreateOptions{})
			if err != nil {
				log.Errorf("Error creating StoragePool %s. Err: %+v", state.spName, err)
				return err
			}
			log.Debugf("Successfully created StoragePool %v", newSp)
		}
	} else {
		// StoragePool already exists, so Update it
		// We don't expect ConflictErrors since updates are synchronized with a lock
		log.Infof("Updating StoragePool instance for %s", state.spName)
		sp := state.updateUnstructuredStoragePool(sp)
		newSp, err := spClient.Resource(*spResource).Update(sp, metav1.UpdateOptions{})
		if err != nil {
			log.Errorf("Error updating StoragePool %s. Err: %+v", state.spName, err)
			return err
		}
		log.Debugf("Successfully updated StoragePool %v", newSp)
	}

	// update the underlying dsType in the all the compatible storage classes for reverse mapping
	for _, scName := range state.compatSC {
		if err := updateSPTypeInSC(ctx, scName, state.dsType); err != nil {
			log.Errorf("Failed to update compatibleSPTypes in storage class %s. Err: %+v", scName, err)
			continue
		}
	}
	c.intendedStateMap[state.dsMoid] = state
	return nil
}

// updateIntendedState is called to apply only the DatastoreSummary properties of a StoragePool. This does not
// recompute the `accessibleNodes` or compatible StorageClass for this StoragePool.
func (c *spController) updateIntendedState(ctx context.Context, dsMoid string, dsSummary types.DatastoreSummary,
	scWatchCntlr *StorageClassWatch) error {
	log := logger.GetLogger(ctx)
	intendedState, ok := c.intendedStateMap[dsMoid]
	if !ok {
		log.Debugf("Skipping update for %s", dsMoid)
		return nil
	}
	log.Debugf("Datastore: %s, StoragePool: %s", dsMoid, intendedState.spName)
	intendedSpName := makeStoragePoolName(dsSummary.Name)
	oldSpName := intendedState.spName
	// Get the changes in properties for this Datastore into the intendedState
	intendedState.spName = intendedSpName
	intendedState.url = dsSummary.Url
	intendedState.capacity = resource.NewQuantity(dsSummary.Capacity, resource.DecimalSI)
	intendedState.freeSpace = resource.NewQuantity(dsSummary.FreeSpace, resource.DecimalSI)
	if intendedState.accessible != dsSummary.Accessible {
		// the accessible nodes are not available immediately after a PC notification
		scheduleReconcileAllStoragePools(ctx, reconcileAllFreq, reconcileAllIterations, scWatchCntlr, c)
		intendedState.accessible = dsSummary.Accessible
	}
	intendedState.datastoreInMM = dsSummary.MaintenanceMode != string(types.DatastoreSummaryMaintenanceModeStateNormal)
	// update StoragePool as per intendedState
	if err := c.applyIntendedState(ctx, intendedState); err != nil {
		return err
	}
	if intendedSpName != oldSpName {
		// need to also delete the older StoragePool
		return deleteStoragePool(ctx, oldSpName)
	}
	return nil
}

func deleteStoragePool(ctx context.Context, spName string) error {
	log := logger.GetLogger(ctx)
	spClient, spResource, err := getSPClient(ctx)
	if err != nil {
		return err
	}
	sp, err := spClient.Resource(*spResource).Get(spName, metav1.GetOptions{})
	if err != nil {
		log.Errorf("Error getting StoragePool instance %s. Err: %v", spName, err)
		return err
	}
	driver, found, err := unstructured.NestedString(sp.Object, "spec", "driver")
	if found && err == nil && driver == csitypes.Name {
		log.Infof("Deleting StoragePool %s", spName)
		err := spClient.Resource(*spResource).Delete(spName, &metav1.DeleteOptions{})
		if err != nil {
			log.Errorf("Error deleting StoragePool %s. Err: %v", spName, err)
			return err
		}
	}
	return nil
}

// getStoragePoolError returns the ErrCode and Message to fill within StoragePool.Status.Error
func (state *intendedState) getStoragePoolError() *v1alpha1.StoragePoolError {
	if state.nodes == nil || len(state.nodes) == 0 {
		return v1alpha1.SpErrors[v1alpha1.ErrStateNoAccessibleHosts]
	}
	if state.datastoreInMM {
		return v1alpha1.SpErrors[v1alpha1.ErrStateDatastoreInMM]
	}
	if state.allHostsInMM {
		return v1alpha1.SpErrors[v1alpha1.ErrStateAllHostsInMM]
	}
	if !state.accessible {
		return v1alpha1.SpErrors[v1alpha1.ErrStateDatastoreNotAccessible]
	}
	return nil
}

func (state *intendedState) createUnstructuredStoragePool() *unstructured.Unstructured {
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
					"total":     state.capacity.Value(),
					"freeSpace": state.freeSpace.Value(),
				},
			},
		},
	}
	spErr := state.getStoragePoolError()
	if spErr != nil {
		unstructured.SetNestedField(sp.Object, spErr.State, "status", "error", "state")
		unstructured.SetNestedField(sp.Object, spErr.Message, "status", "error", "message")
	}
	return sp
}

func (state *intendedState) updateUnstructuredStoragePool(sp *unstructured.Unstructured) *unstructured.Unstructured {
	unstructured.SetNestedField(sp.Object, state.url, "spec", "parameters", "datastoreUrl")
	unstructured.SetNestedField(sp.Object, strings.ReplaceAll(state.dsType, spTypePrefix, ""), "metadata", "labels", spTypeLabelKey)
	unstructured.SetNestedField(sp.Object, state.capacity.Value(), "status", "capacity", "total")
	unstructured.SetNestedField(sp.Object, state.freeSpace.Value(), "status", "capacity", "freeSpace")
	unstructured.SetNestedStringSlice(sp.Object, state.nodes, "status", "accessibleNodes")
	unstructured.SetNestedStringSlice(sp.Object, state.compatSC, "status", "compatibleStorageClasses")
	spErr := state.getStoragePoolError()
	if spErr != nil {
		unstructured.SetNestedField(sp.Object, spErr.State, "status", "error", "state")
		unstructured.SetNestedField(sp.Object, spErr.Message, "status", "error", "message")
	} else {
		unstructured.RemoveNestedField(sp.Object, "status", "error")
	}
	return sp
}
