/*
Copyright 2020 VMware, Inc.

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
	"sync"
	"time"

	"github.com/vmware/govmomi/property"
	"github.com/vmware/govmomi/vim25/types"

	k8serrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/client-go/dynamic"
	"sigs.k8s.io/controller-runtime/pkg/client/config"
	cnsvsphere "sigs.k8s.io/vsphere-csi-driver/pkg/common/cns-lib/vsphere"
	"sigs.k8s.io/vsphere-csi-driver/pkg/csi/service/logger"
	csitypes "sigs.k8s.io/vsphere-csi-driver/pkg/csi/types"
	spv1alpha1 "sigs.k8s.io/vsphere-csi-driver/pkg/syncer/storagepool/apis/cns/v1alpha1"
)

var mutex sync.Mutex

// InitHostMountListener initializes a property collector listener for any new datastores mounted to
// hosts in the k8s cluster. StoragePool instances will be created for such new datastores.
func InitHostMountListener(ctx context.Context, scWatch *StorageClassWatch, vc *cnsvsphere.VirtualCenter, clusterID string) error {
	log := logger.GetLogger(ctx)
	hosts, err := vc.GetHostsByCluster(ctx, clusterID)
	if err != nil {
		log.Errorf("Failed to get hosts from VC. Err: %+v", err)
		return err
	}

	// construct a property collector filter that includes all the hosts
	filter := new(property.WaitFilter)
	for _, host := range hosts {
		obj := host.Reference()
		filter = filter.Add(obj, obj.Type, []string{"datastore"})
	}
	// Start listening to property collector for changes to 'datastore' property
	// in any of the hosts
	p := property.DefaultCollector(vc.Client.Client)
	go property.WaitForUpdates(ctx, p, filter, func(updates []types.ObjectUpdate) bool {
		log.Infof("Got %d update(s) for host mounts", len(updates))
		for _, update := range updates {
			propChange := update.ChangeSet
			log.Infof("Got update for host %v for datastores %v", update.Obj, propChange)
		}
		log.Infof("Reconciling StoragePool instances due to mount change...")
		err := ReconcileStoragePools(ctx, scWatch, vc, clusterID, true /*dsListChange*/)
		if err != nil {
			log.Errorf("Error reconciling StoragePool instances in HostMount listener. Err: %+v", err)
		} else {
			log.Infof("Successfully reconciled StoragePool instances")
		}
		// return false to continue listening to property updates
		return false
	})
	log.Infof("Started listening to host mounts to reconcile StoragePools")
	return nil
}

// InitDatastoreCapacityListener initializes a property collector listener to handle changes to
// datastore capacity.
func InitDatastoreCapacityListener(ctx context.Context, scWatch *StorageClassWatch, vc *cnsvsphere.VirtualCenter, clusterID string) error {
	log := logger.GetLogger(ctx)
	// Get datastores from VC
	datastores, err := cnsvsphere.GetCandidateDatastoresInCluster(ctx, vc, clusterID)
	if err != nil {
		log.Errorf("Failed to find datastores from VC. Err: %+v", err)
		return err
	}

	// construct a property collector filter that includes capacity properties of shared datatores
	filter := new(property.WaitFilter)
	for _, ds := range datastores {
		obj := ds.Reference()
		filter = filter.Add(obj, obj.Type, []string{"name", "summary.capacity", "summary.freeSpace", "summary.accessible"})
	}
	// Start listening to property collector for changes to datastore's capacity and freeSpace property
	p := property.DefaultCollector(vc.Client.Client)
	go property.WaitForUpdates(ctx, p, filter, func(updates []types.ObjectUpdate) bool {
		log.Infof("Got %d update(s) for datastore capacity/freeSpace", len(updates))
		for _, update := range updates {
			propChange := update.ChangeSet
			log.Infof("Got update for datastore %v for property %v", update.Obj, propChange)
		}
		log.Infof("Reconciling StoragePool instances due to capacity change...")
		err := ReconcileStoragePools(ctx, scWatch, vc, clusterID, false /*dsListChange*/)
		if err != nil {
			log.Errorf("Error reconciling StoragePool instances in DatastoreCapacity listener. Err: %v", err)
		} else {
			log.Infof("Successfully reconciled StoragePool instances")
		}
		return false
	})
	log.Infof("Started listening to datastore capacity/freeSpace to reconcile StoragePools")
	return nil
}

// ReconcileStoragePools creates new StoragePool instances or updates existing ones for each shared
// datatsore found in this k8s cluster.
func ReconcileStoragePools(ctx context.Context, scWatch *StorageClassWatch, vc *cnsvsphere.VirtualCenter, clusterID string, dsListChange bool) error {
	log := logger.GetLogger(ctx)
	// Create a client to create/udpate StoragePool instances
	cfg, err := config.GetConfig()
	if err != nil {
		log.Errorf("Failed to get Kubernetes config. Err: %+v", err)
		return err
	}
	spclient, err := dynamic.NewForConfig(cfg)
	if err != nil {
		log.Errorf("Failed to create StoragePool client using config. Err: %+v", err)
		return err
	}
	spResource := spv1alpha1.SchemeGroupVersion.WithResource("storagepools")

	// Sequence reconcile operations beyond this point to avoid race conditions
	mutex.Lock()
	defer mutex.Unlock()

	// Get datastores from VC and create initial StoragePool instances
	datastores, err := cnsvsphere.GetCandidateDatastoresInCluster(ctx, vc, clusterID)
	if err != nil {
		log.Errorf("Failed to find datastores from VC. Err: %+v", err)
		return err
	}

	dsPolicyCompatMap, err := scWatch.getDatastoreToPolicyCompatibility(ctx, datastores, dsListChange)
	if err != nil {
		log.Errorf("Failed to get dsPolicyCompatMap. Err: %+v", err)
		return err
	}

	// Create/Update StoragePool instances for each datastore
	validStoragePoolNames := make(map[string]bool)
	for _, ds := range datastores {
		log.Debugf("%s: %v", ds.Info.Name, ds)
		spName := makeStoragePoolName(ds.Info.Name)
		validStoragePoolNames[spName] = true
		dsURL, err := ds.GetDatastoreURL(ctx)
		if err != nil {
			log.Errorf("Failed to find URL for %v. Err: %+v", ds, err)
			return err
		}
		nodes, err := findAccessibleNodes(ctx, ds, clusterID, vc.Client.Client)
		if err != nil {
			log.Errorf("Error finding accessible nodes of datastore %v. Err: %+v", ds, err)
			continue
		}

		capacity, freeSpace, err := getDatastoreProperties(ctx, ds)

		compatSC := make([]string, 0)
		dsPolicies, ok := dsPolicyCompatMap[ds.Datastore.Reference().Value]
		if ok {
			for _, policyID := range dsPolicies {
				scName := scWatch.policyToScMap[policyID].Name
				compatSC = append(compatSC, scName)
			}
		} else {
			log.Infof("Failed to get compatible policies for %s", ds.Datastore.Reference().Value)
		}

		storagepoolError := ""
		if err != nil {
			storagepoolError = err.Error()
		}

		// Get StoragePool with spName and Update if already present, otherwise Create resource
		sp, err := spclient.Resource(spResource).Get(spName, metav1.GetOptions{})
		if err != nil {
			statusErr, ok := err.(*k8serrors.StatusError)
			if ok && statusErr.Status().Reason == metav1.StatusReasonNotFound {

				sp := createUnstructuredStoragePool(spName, dsURL, capacity, freeSpace, nodes, storagepoolError, compatSC)

				newSp, err := spclient.Resource(spResource).Create(sp, metav1.CreateOptions{})
				if err != nil {
					log.Errorf("Error creating StoragePool %s. Err: %+v", spName, err)
				} else {
					log.Infof("Successfully created StoragePool %v", newSp)
				}
			}
		} else {
			// StoragePool already exists, so Update it
			// We don't expect ConflictErrors since updates are synchronized with a lock
			sp := updateUnstructuredStoragePool(sp, dsURL, capacity, freeSpace, nodes, storagepoolError, compatSC)
			newSp, err := spclient.Resource(spResource).Update(sp, metav1.UpdateOptions{})
			if err != nil {
				log.Errorf("Error updating StoragePool %s. Err: %+v", spName, err)
			} else {
				log.Infof("Successfully updated StoragePool %v", newSp)
			}
		}
	}

	// Delete unknown StoragePool instances owned by this driver
	splist, err := spclient.Resource(spResource).List(metav1.ListOptions{})
	if err != nil {
		log.Errorf("Error getting list of StoragePool instances. Err: %v", err)
		return fmt.Errorf("Error getting list of StoragePool instances")
	}
	for _, sp := range splist.Items {
		spName := sp.GetName()
		if _, valid := validStoragePoolNames[spName]; !valid {
			driver, found, err := unstructured.NestedString(sp.Object, "spec", "driver")
			if !found || err != nil || driver == csitypes.Name {
				log.Infof("Deleting StoragePool %s", spName)
				err := spclient.Resource(spResource).Delete(spName, &metav1.DeleteOptions{})
				if err != nil {
					// log error and continue
					log.Errorf("Error deleting StoragePool %s. Err: %v", spName, err)
				}
			}
		}
	}
	return nil
}

func createUnstructuredStoragePool(spName string, dsURL string, capacity *resource.Quantity,
	freeSpace *resource.Quantity, nodes []string, storagepoolError string, compatSC []string) *unstructured.Unstructured {
	sp := &unstructured.Unstructured{
		Object: map[string]interface{}{
			"apiVersion": "cns.vmware.com/v1alpha1",
			"kind":       "StoragePool",
			"metadata": map[string]interface{}{
				"name": spName,
			},
			"spec": map[string]interface{}{
				"driver": csitypes.Name,
				"parameters": map[string]interface{}{
					"datastoreUrl": dsURL,
				},
			},
			"status": map[string]interface{}{
				"accessibleNodes":          nodes,
				"compatibleStorageClasses": compatSC,
				"capacity": map[string]interface{}{
					"total":     capacity,
					"freeSpace": freeSpace,
				},
			},
		},
	}

	if storagepoolError != "" {
		unstructured.SetNestedField(sp.Object, time.Now().String(), "status", "error", "time")
		unstructured.SetNestedField(sp.Object, storagepoolError, "status", "error", "message")
	}

	return sp
}

func updateUnstructuredStoragePool(sp *unstructured.Unstructured, dsURL string,
	capacity *resource.Quantity, freeSpace *resource.Quantity,
	nodes []string, storagepoolError string, compatSC []string) *unstructured.Unstructured {
	unstructured.SetNestedField(sp.Object, dsURL, "spec", "parameters", "datastoreUrl")
	unstructured.SetNestedField(sp.Object, capacity.String(), "status", "capacity", "total")
	unstructured.SetNestedField(sp.Object, freeSpace.String(), "status", "capacity", "freeSpace")
	unstructured.SetNestedStringSlice(sp.Object, nodes, "status", "accessibleNodes")
	unstructured.SetNestedStringSlice(sp.Object, compatSC, "status", "compatibleStorageClasses")
	if storagepoolError != "" {
		unstructured.SetNestedField(sp.Object, time.Now().String(), "status", "error", "time")
		unstructured.SetNestedField(sp.Object, storagepoolError, "status", "error", "message")
	} else {
		unstructured.RemoveNestedField(sp.Object, "status", "error")
	}

	return sp
}
