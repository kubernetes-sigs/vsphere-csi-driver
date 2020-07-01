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
	"os"
	"sync"

	"github.com/vmware/govmomi/property"
	"github.com/vmware/govmomi/vim25/types"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"

	cnsvsphere "sigs.k8s.io/vsphere-csi-driver/pkg/common/cns-lib/vsphere"
	"sigs.k8s.io/vsphere-csi-driver/pkg/csi/service/logger"
	csitypes "sigs.k8s.io/vsphere-csi-driver/pkg/csi/types"
)

var (
	// reconcileAllMutex should be acquired to run ReconcileAllStoragePools so that only one thread runs at a time.
	reconcileAllMutex sync.Mutex
)

// Initialize a PropertyCollector listener that updates the intended state of a StoragePool
func initListener(ctx context.Context, scWatchCntlr *StorageClassWatch, spController *SpController) error {
	log := logger.GetLogger(ctx)

	// Initialize a PropertyCollector that watches all objects in the hierarchy of
	// cluster -> hosts in the cluster -> datastores mounted on hosts. One or more StoragePool instances would be
	// created for each Datastore.
	clusterMoref := types.ManagedObjectReference{
		Type:  "ClusterComputeResource",
		Value: spController.clusterID,
	}
	ts := types.TraversalSpec{
		Type: "ClusterComputeResource",
		Path: "host",
		Skip: types.NewBool(false),
		SelectSet: []types.BaseSelectionSpec{
			&types.TraversalSpec{
				Type: "HostSystem",
				Path: "datastore",
				Skip: types.NewBool(false),
			},
		},
	}
	filter := new(property.WaitFilter)
	filter.Add(clusterMoref, clusterMoref.Type, []string{"host"}, &ts)
	prodHost := types.PropertySpec{
		Type:    "HostSystem",
		PathSet: []string{"datastore", "runtime.inMaintenanceMode"},
	}
	propDs := types.PropertySpec{
		Type:    "Datastore",
		PathSet: []string{"summary"},
	}
	filter.Spec.PropSet = append(filter.Spec.PropSet, prodHost, propDs)
	go func() {
		for {
			log.Infof("Starting property collector...")
			p := property.DefaultCollector(spController.vc.Client.Client)
			err := property.WaitForUpdates(ctx, p, filter, func(updates []types.ObjectUpdate) bool {
				ctx := logger.NewContextWithLogger(ctx)
				log = logger.GetLogger(ctx)
				log.Infof("Got %d property collector update(s)", len(updates))
				reconcileAllScheduled := false
				for _, update := range updates {
					propChange := update.ChangeSet
					log.Debugf("Got update for object %v properties %v", update.Obj, propChange)
					if update.Obj.Type == "Datastore" && len(propChange) == 1 && propChange[0].Name == "summary" && propChange[0].Op == types.PropertyChangeOpAssign {
						// Handle changes in a Datastore's summary property (that includes name, type, freeSpace, accessible) by
						// updating only the corresponding single StoragePool instance.
						ds := update.Obj
						summary, ok := propChange[0].Val.(types.DatastoreSummary)
						if !ok {
							log.Errorf("Not able to cast to DatastoreSummary: %v", propChange[0].Val)
							continue
						}
						// Datastore summary property can be updated immediately into the StoragePool
						log.Debugf("Starting update for single StoragePool %s", ds.Value)
						err := spController.updateIntendedState(ctx, ds.Value, summary, scWatchCntlr)
						if err != nil {
							log.Errorf("Error updating StoragePool for datastore %v. Err: %v", ds, err)
						}
					} else {
						// Handle changes in "hosts in cluster", "hosts inMaintenanceMode state" and "Datastores mounted on hosts" by
						// scheduling a reconcile of all StoragePool instances afresh. Schedule only once for a batch of updates
						if !reconcileAllScheduled {
							err := ReconcileAllStoragePools(ctx, scWatchCntlr, spController)
							if err != nil {
								log.Errorf("ReconcileAllStoragePools failed. err: %v", err)
							}
							reconcileAllScheduled = true
						}
					}
				}
				log.Debugf("Done processing %d property collector update(s)", len(updates))
				return false
			})
			if err != nil {
				log.Infof("Property collector session needs to be reestablished")
				err = spController.vc.Connect(ctx)
				if err != nil {
					// Terminate the container and let the pod restart it
					log.Errorf("Not able to establish connection with VC. Exiting...")
					os.Exit(-1)
				}
			}
		}
	}()

	return nil
}

// ReconcileAllStoragePools creates/updates/deletes StoragePool instances for datastores found in this k8s cluster.
// This should be invoked when there is a potential change in the list of datastores in the cluster.
func ReconcileAllStoragePools(ctx context.Context, scWatchCntlr *StorageClassWatch, spCtl *SpController) error {
	log := logger.GetLogger(ctx)

	// Only one thread at a time can execute ReconcileAllStoragePools
	reconcileAllMutex.Lock()
	defer reconcileAllMutex.Unlock()

	// Get datastores from VC
	sharedDatastores, vsanDirectDatastores, err := cnsvsphere.GetCandidateDatastoresInCluster(ctx, spCtl.vc, spCtl.clusterID)
	if err != nil {
		log.Errorf("Failed to find datastores from VC. Err: %+v", err)
		return err
	}
	datastores := append(sharedDatastores, vsanDirectDatastores...)
	validStoragePoolNames := make(map[string]bool)
	// create StoragePools that are missing and add them to intendedStateMap
	for _, dsInfo := range datastores {
		spName := makeStoragePoolName(dsInfo.Info.Name)
		validStoragePoolNames[spName] = true
		intendedState, err := newIntendedState(ctx, dsInfo, scWatchCntlr)
		if err != nil {
			log.Errorf("Error reconciling StoragePool for datastore %s. Err: %v", dsInfo.Reference().Value, err)
			continue
		}
		err = spCtl.applyIntendedState(ctx, intendedState)
		if err != nil {
			log.Errorf("Error applying intended state of StoragePool %s. Err: %v", intendedState.spName, err)
			continue
		}

		if intendedState.dsType == "cns.vmware.com/vsan" {
			// create storage pools for all accessible nodes
			for _, snaNode := range intendedState.nodes {
				intendedSNAState, err := newIntendedVsanSNAState(ctx, dsInfo, scWatchCntlr, intendedState, snaNode)
				if err != nil {
					log.Errorf("Error reconciling StoragePool for vsan sna node %s. Err: %v", snaNode, err)
					continue
				}
				validStoragePoolNames[intendedSNAState.spName] = true
				err = spCtl.applyIntendedState(ctx, intendedSNAState)
				if err != nil {
					log.Errorf("Error applying intended state of StoragePool %s. Err: %v", intendedSNAState.spName, err)
					continue
				}
			}
		}

	}

	// Delete unknown StoragePool instances owned by this driver
	return deleteStoragePools(ctx, validStoragePoolNames, spCtl)
}

func deleteStoragePools(ctx context.Context, validStoragePoolNames map[string]bool, spCtl *SpController) error {
	log := logger.GetLogger(ctx)
	spClient, spResource, err := getSPClient(ctx)
	if err != nil {
		return err
	}
	// Delete unknown StoragePool instances owned by this driver
	splist, err := spClient.Resource(*spResource).List(metav1.ListOptions{})
	if err != nil {
		log.Errorf("Error getting list of StoragePool instances. Err: %v", err)
		return err
	}
	for _, sp := range splist.Items {
		spName := sp.GetName()
		if _, valid := validStoragePoolNames[spName]; !valid {
			driver, found, err := unstructured.NestedString(sp.Object, "spec", "driver")
			if found && err == nil && driver == csitypes.Name {
				log.Infof("Deleting StoragePool %s", spName)
				err := spClient.Resource(*spResource).Delete(spName, &metav1.DeleteOptions{})
				if err != nil {
					// log error and continue
					log.Errorf("Error deleting StoragePool %s. Err: %v", spName, err)
				}
			}
			// Also delete entry from intendedStateMap
			spCtl.deleteIntendedState(ctx, spName)
		}
	}
	return nil
}
