/*
Copyright 2019 The Kubernetes Authors.

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

package vsphere

import (
	"context"
	"fmt"

	"github.com/vmware/govmomi/object"
	"github.com/vmware/govmomi/property"
	"github.com/vmware/govmomi/vim25/mo"
	"github.com/vmware/govmomi/vim25/types"
	"sigs.k8s.io/vsphere-csi-driver/v2/pkg/csi/service/logger"
)

// Datastore holds Datastore and Datacenter information.
type Datastore struct {
	// Datastore represents the govmomi Datastore instance.
	*object.Datastore
	// Datacenter represents the datacenter on which the Datastore resides.
	Datacenter *Datacenter
}

// DatastoreInfo is a structure to store the Datastore and it's Info.
type DatastoreInfo struct {
	*Datastore
	Info *types.DatastoreInfo
}

func (di DatastoreInfo) String() string {
	return fmt.Sprintf("Datastore: %+v, datastore URL: %s", di.Datastore, di.Info.Url)
}

// GetDatastoreURLAndType returns the URL and Type of datastore
func (ds *Datastore) GetDatastoreURLAndType(ctx context.Context) (string, string, error) {
	log := logger.GetLogger(ctx)
	var dsMo mo.Datastore
	pc := property.DefaultCollector(ds.Client())
	err := pc.RetrieveOne(ctx, ds.Datastore.Reference(), []string{"summary"}, &dsMo)
	if err != nil {
		log.Errorf("Failed to retrieve datastore summary property: %v", err)
		return "", "", err
	}
	return dsMo.Summary.Url, dsMo.Summary.Type, nil
}
