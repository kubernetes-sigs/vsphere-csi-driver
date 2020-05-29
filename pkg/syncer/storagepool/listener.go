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
	"strconv"
	"sync"
	"time"

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
	// Run ReconcileAllStoragePools every `freq` seconds.
	reconcileAllFreq = time.Second * 60
	// Run ReconcileAllStoragePools `n` times.
	reconcileAllIterations = 5
)

// Initialize a PropertyCollector listener that updates the intended state of a StoragePool
func initListener(ctx context.Context, scWatchCntlr *StorageClassWatch, spController *spController) error {
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
		PathSet: []string{"datastore"},
	}
	propDs := types.PropertySpec{
		Type:    "Datastore",
		PathSet: []string{"summary"},
	}
	filter.Spec.PropSet = append(filter.Spec.PropSet, prodHost, propDs)
	p := property.DefaultCollector(spController.vc.Client.Client)
	go property.WaitForUpdates(ctx, p, filter, func(updates []types.ObjectUpdate) bool {
		log.Infof("Got %d update(s)", len(updates))
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
				// Handle changes in "hosts in cluster" and "Datastores mounted on hosts" by scheduling a reconcile
				// of all StoragePool instances afresh. Schedule only once for a batch of updates
				if !reconcileAllScheduled {
					scheduleReconcileAllStoragePools(ctx, reconcileAllFreq, reconcileAllIterations, scWatchCntlr, spController)
					reconcileAllScheduled = true
				}
			}
		}
		return false
	})
	log.Infof("Started property collector...")
	return nil
}

// XXX: This hack should be removed once we figure out all the properties of a StoragePool that gets updates lazily.
// Notifications for Hosts add/remove from a Cluster and Datastores un/mounted on Hosts come in too early
// before the VC is ready. For example, the vSAN Datastore mount is not completed yet for a newly added host.
// The Datastore does not have the vSANDirect tag yet for a newly added Datastore in a host. So this function
// schedules a ReconcileAllStoragePools, that computes addition and deletion of StoragePool instances, to
// run n times every f secs. Each time this function is called, the counter n is reset, so that
// ReconcileAllStoragePools can run another n times starting now. The values for n and f can be tuned so that
// ReconcileAllStoragePools can be retried enough number of times to discover additions and deletions of
// StoragePool instances in all cases.
func scheduleReconcileAllStoragePools(ctx context.Context, freq time.Duration, nTimes int, scWatchCntrl *StorageClassWatch,
	spController *spController) {
	log := logger.GetLogger(ctx)
	t := time.Now().Format("2006-01-02T15:04:05.000")
	log.Debugf("[%s] Scheduled ReconcileAllStoragePools starting", t)
	go func() {
		tick := time.NewTicker(freq)
		defer tick.Stop()
		for iteration := 0; iteration < nTimes; iteration++ {
			iterID := t + "-" + strconv.Itoa(iteration)
			select {
			case <-tick.C:
				log.Debugf("[%s] Starting reconcile for all StoragePool instances", iterID)
				err := ReconcileAllStoragePools(ctx, scWatchCntrl, spController)
				if err != nil {
					log.Errorf("[%s] Error reconciling StoragePool instances in HostMount listener. Err: %+v", iterID, err)
				} else {
					log.Debugf("[%s] Successfully reconciled all StoragePool instances", iterID)
				}
			case <-ctx.Done():
				log.Debugf("[%s] Done reconcile all loop for StoragePools", iterID)
				return
			}
		}
		log.Infof("[%s] Scheduled ReconcileAllStoragePools completed", t)
	}()
	log.Infof("[%s] Scheduled ReconcileAllStoragePools started", t)
}

// ReconcileAllStoragePools creates/updates/deletes StoragePool instances for datastores found in this k8s cluster.
// This should be invoked when there is a potential change in the list of datastores in the cluster.
func ReconcileAllStoragePools(ctx context.Context, scWatchCntlr *StorageClassWatch, spCtl *spController) error {
	log := logger.GetLogger(ctx)

	// Only one thread at a time can execute ReconcileAllStoragePools
	reconcileAllMutex.Lock()
	defer reconcileAllMutex.Unlock()

	// Get datastores from VC
	datastores, err := cnsvsphere.GetCandidateDatastoresInCluster(ctx, spCtl.vc, spCtl.clusterID)
	if err != nil {
		log.Errorf("Failed to find datastores from VC. Err: %+v", err)
		return err
	}

	validStoragePoolNames := make(map[string]bool)
	// create StoragePools that are missing and add them to intendedStateMap
	for _, dsInfo := range datastores {
		intendedState, err := newIntendedState(ctx, dsInfo, scWatchCntlr, spCtl.vc, spCtl.clusterID)
		if err != nil {
			log.Errorf("Error reconciling StoragePool for datastore %s. Err: %v", dsInfo.Reference().Value, err)
			continue
		}
		err = spCtl.applyIntendedState(ctx, intendedState)
		if err != nil {
			log.Errorf("Error applying intended state of StoragePool %s. Err: %v", intendedState.spName, err)
			continue
		}
		validStoragePoolNames[intendedState.spName] = true
	}

	// Delete unknown StoragePool instances owned by this driver
	return deleteStoragePools(ctx, validStoragePoolNames)
}

func deleteStoragePools(ctx context.Context, validStoragePoolNames map[string]bool) error {
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
		}
	}
	return nil
}
