/*
Copyright 2025 The Kubernetes Authors.

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
package vcutil

import (
	"context"
	"fmt"
	"log"
	"strings"

	"github.com/vmware/govmomi"
	"github.com/vmware/govmomi/view"
	"github.com/vmware/govmomi/vim25/mo"
	vim25types "github.com/vmware/govmomi/vim25/types"
	"sigs.k8s.io/vsphere-csi-driver/v3/tests/e2e/constants"
)

// This function filters all datastores
// in a vc by given datastore type.
func GetDatastoresByType(vcClient *govmomi.Client, dsType string) ([]mo.Datastore, error) {
	fmt.Printf("Fetching datastore of type : %s\n", dsType)
	// Retrieve summary property for all datastores
	datastores, err := GetAllDatastores(vcClient)
	if err != nil {
		return nil, err
	}
	var resultDatastores []mo.Datastore
	// Filter datastores by type
	for _, ds := range datastores {
		if ds.Summary.Type == dsType {
			resultDatastores = append(resultDatastores, ds)
		}
	}
	return resultDatastores, nil
}

/*
This function provides all the datastores in given vc
*/
func GetAllDatastores(vcClient *govmomi.Client) ([]mo.Datastore, error) {
	ctx := context.Background()

	// Create view manager
	m := view.NewManager(vcClient.Client)

	// Create a view of Datastore objects
	v, err := m.CreateContainerView(ctx, vcClient.ServiceContent.RootFolder, []string{"Datastore"}, true)
	if err != nil {
		return nil, err
	}
	defer v.Destroy(ctx)
	// Retrieve summary property for all datastores
	var datastores []mo.Datastore
	err = v.Retrieve(ctx, []string{constants.DATASTORE}, []string{constants.DatastoreSummary}, &datastores)
	if err != nil {
		return nil, err
	}

	for _, ds := range datastores {
		PrintDatastoreSummary(ds)
	}

	return datastores, nil
}

// Get scsi lun of vmfs datastore
func GetScsiLun(vcClient *govmomi.Client, vmfsDatastore mo.Datastore) (string, error) {
	ctx := context.Background()
	// Create view manager
	m := view.NewManager(vcClient.Client)

	// Create a view of Datastore objects
	v, err := m.CreateContainerView(ctx, vcClient.ServiceContent.RootFolder, []string{"Datastore"}, true)
	if err != nil {
		return "", err
	}
	defer v.Destroy(ctx)
	var datastores []mo.Datastore
	err = v.Retrieve(ctx, []string{constants.DATASTORE}, []string{constants.DatastoreInfo}, &datastores)
	if err != nil {
		return "", err
	}
	for _, ds := range datastores {
		if ds.Reference() == vmfsDatastore.Reference() {
			scsi := strings.Replace(ds.Info.(*vim25types.VmfsDatastoreInfo).Vmfs.Extent[0].DiskName, "mpx.", "", 1)
			return scsi, nil
		}
	}
	return "", fmt.Errorf("scsi lun for given datastore not found")
}

/*
This function prints datatore summary
*/
func PrintDatastoreSummary(datastore mo.Datastore) {
	fmt.Printf("Datastore Summary - Name: %s, Capacity: %d, Free: %d , Type: %s, Mor: %s, isShared: %t\n",
		datastore.Summary.Name,
		datastore.Summary.Capacity,
		datastore.Summary.FreeSpace,
		datastore.Summary.Type,
		datastore.Summary.Datastore.Value,
		*datastore.Summary.MultipleHostAccess)

}

// Given the host mor, this function return the ip
func GetHostName(c *govmomi.Client, hostMoRef vim25types.ManagedObjectReference) (string, error) {
	ctx := context.Background()
	var hostMo mo.HostSystem
	err := c.RetrieveOne(ctx, hostMoRef, []string{"name"}, &hostMo)
	if err != nil {
		log.Fatalf("Error retrieving host info for %s: %v", hostMoRef.Value, err)
	}
	fmt.Printf("Host connected to datastore: %s\n", hostMo.Name)
	return hostMo.Name, nil
}

// This function retrieves all ESXi hosts that have access to the specified datastore.
func GetHostsVisibleToDatastore(c *govmomi.Client, datastore vim25types.ManagedObjectReference) ([]vim25types.ManagedObjectReference, error) {
	ctx := context.Background()
	var datastoreMo mo.Datastore
	err := c.RetrieveOne(ctx, datastore.Reference(), []string{"host"}, &datastoreMo)
	if err != nil {
		return nil, err
	}
	var visibleHosts []vim25types.ManagedObjectReference
	// Iterate through the connected hosts and display their names
	for _, hostMount := range datastoreMo.Host {
		hostMoRef := hostMount.Key
		var hostMo mo.HostSystem
		err = c.RetrieveOne(ctx, hostMoRef, []string{"name"}, &hostMo)
		if err != nil {
			return nil, err
		}
		visibleHosts = append(visibleHosts, hostMoRef)
	}

	return visibleHosts, nil
}
