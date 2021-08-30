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
	"strings"

	"sigs.k8s.io/vsphere-csi-driver/v2/pkg/csi/service/logger"

	"github.com/vmware/govmomi/find"
	"github.com/vmware/govmomi/object"
	"github.com/vmware/govmomi/property"
	"github.com/vmware/govmomi/vim25/mo"
	"github.com/vmware/govmomi/vim25/types"
)

// DatastoreInfoProperty refers to the property name info for the Datastore.
const DatastoreInfoProperty = "info"

// Datacenter holds virtual center information along with the Datacenter.
type Datacenter struct {
	// Datacenter represents the govmomi Datacenter.
	*object.Datacenter
	// VirtualCenterHost represents the virtual center host address.
	VirtualCenterHost string
}

func (dc *Datacenter) String() string {
	return fmt.Sprintf("Datacenter [Datacenter: %v, VirtualCenterHost: %v]",
		dc.Datacenter, dc.VirtualCenterHost)
}

// GetDatastoreByURL returns the *Datastore instance given its URL.
func (dc *Datacenter) GetDatastoreByURL(ctx context.Context, datastoreURL string) (*Datastore, error) {
	log := logger.GetLogger(ctx)
	finder := find.NewFinder(dc.Datacenter.Client(), false)
	finder.SetDatacenter(dc.Datacenter)
	datastores, err := finder.DatastoreList(ctx, "*")
	if err != nil {
		log.Errorf("failed to get all the datastores. err: %+v", err)
		return nil, err
	}
	var dsList []types.ManagedObjectReference
	for _, ds := range datastores {
		dsList = append(dsList, ds.Reference())
	}

	var dsMoList []mo.Datastore
	pc := property.DefaultCollector(dc.Client())
	properties := []string{DatastoreInfoProperty}
	err = pc.Retrieve(ctx, dsList, properties, &dsMoList)
	if err != nil {
		log.Errorf("failed to get Datastore managed objects from datastore objects."+
			" dsObjList: %+v, properties: %+v, err: %v", dsList, properties, err)
		return nil, err
	}
	for _, dsMo := range dsMoList {
		if dsMo.Info.GetDatastoreInfo().Url == datastoreURL {
			return &Datastore{object.NewDatastore(dc.Client(), dsMo.Reference()),
				dc}, nil
		}
	}
	err = fmt.Errorf("couldn't find Datastore given URL %q", datastoreURL)
	log.Error(err)
	return nil, err
}

// GetVirtualMachineByUUID returns the VirtualMachine instance given its UUID
// in a datacenter.
// If instanceUUID is set to true, then UUID is an instance UUID.
//  - In this case, this function searches for virtual machines whose instance
//    UUID matches the given uuid.
// If instanceUUID is set to false, then UUID is BIOS UUID.
//  - In this case, this function searches for virtual machines whose BIOS UUID
//    matches the given uuid.
func (dc *Datacenter) GetVirtualMachineByUUID(ctx context.Context,
	uuid string, instanceUUID bool) (*VirtualMachine, error) {
	log := logger.GetLogger(ctx)
	uuid = strings.ToLower(strings.TrimSpace(uuid))
	searchIndex := object.NewSearchIndex(dc.Datacenter.Client())
	svm, err := searchIndex.FindByUuid(ctx, dc.Datacenter, uuid, true, &instanceUUID)
	if err != nil {
		log.Errorf("failed to find VM given uuid %s with err: %v", uuid, err)
		return nil, err
	} else if svm == nil {
		log.Errorf("Couldn't find VM given uuid %s", uuid)
		return nil, ErrVMNotFound
	}
	vm := &VirtualMachine{
		VirtualCenterHost: dc.VirtualCenterHost,
		UUID:              uuid,
		VirtualMachine:    object.NewVirtualMachine(dc.Datacenter.Client(), svm.Reference()),
		Datacenter:        dc,
	}
	return vm, nil
}

// asyncGetAllDatacenters returns *Datacenter instances over the given
// channel. If an error occurs, it will be returned via the given error channel.
// If the given context is canceled, the processing will be stopped as soon as
// possible, and the channels will be closed before returning.
func asyncGetAllDatacenters(ctx context.Context, dcsChan chan<- *Datacenter, errChan chan<- error) {
	log := logger.GetLogger(ctx)
	defer close(dcsChan)
	defer close(errChan)

	for _, vc := range GetVirtualCenterManager(ctx).GetAllVirtualCenters() {
		// If the context was canceled, we stop looking for more Datacenters.
		select {
		case <-ctx.Done():
			err := ctx.Err()
			log.Infof("Context was done, returning with err: %v", err)
			errChan <- err
			return
		default:
		}
		dcs, err := vc.GetDatacenters(ctx)
		if err != nil {
			log.Errorf("failed to fetch datacenters for vc %v with err: %v", vc.Config.Host, err)
			errChan <- err
			return
		}

		for _, dc := range dcs {
			// If the context was canceled, we don't return more Datacenters.
			select {
			case <-ctx.Done():
				err := ctx.Err()
				log.Infof("Context was done, returning with err: %v", err)
				errChan <- err
				return
			default:
				log.Infof("Publishing datacenter %v", dc)
				dcsChan <- dc
			}
		}
	}
}

// AsyncGetAllDatacenters fetches all Datacenters asynchronously. The
// *Datacenter chan returns a *Datacenter on discovering one. The
// error chan returns a single error if one occurs. Both channels are closed
// when nothing more is to be sent.
//
// The buffer size for the *Datacenter chan can be specified via the
// buffSize parameter. For example, buffSize could be 1, in which case, the
// sender will buffer at most 1 *Datacenter instance (and possibly close
// the channel and terminate, if that was the only instance found).
//
// Note that a context.Canceled error would be returned if the context was
// canceled at some point during the execution of this function.
func AsyncGetAllDatacenters(ctx context.Context, buffSize int) (<-chan *Datacenter, <-chan error) {
	dcsChan := make(chan *Datacenter, buffSize)
	errChan := make(chan error, 1)
	go asyncGetAllDatacenters(ctx, dcsChan, errChan)
	return dcsChan, errChan
}

// GetVMMoList gets the VM Managed Objects with the given properties from the
// VM object.
func (dc *Datacenter) GetVMMoList(ctx context.Context, vmObjList []*VirtualMachine,
	properties []string) ([]mo.VirtualMachine, error) {
	log := logger.GetLogger(ctx)
	var vmMoList []mo.VirtualMachine
	var vmRefs []types.ManagedObjectReference
	if len(vmObjList) < 1 {
		msg := "VirtualMachine Object list is empty"
		log.Errorf(msg+": %v", vmObjList)
		return nil, fmt.Errorf(msg)
	}

	for _, vmObj := range vmObjList {
		vmRefs = append(vmRefs, vmObj.Reference())
	}
	pc := property.DefaultCollector(dc.Client())
	err := pc.Retrieve(ctx, vmRefs, properties, &vmMoList)
	if err != nil {
		log.Errorf("failed to get VM managed objects from VM objects. vmObjList: %+v, properties: %+v, err: %v",
			vmObjList, properties, err)
		return nil, err
	}
	return vmMoList, nil
}

// GetAllDatastores gets the datastore URL to DatastoreInfo map for all the
// datastores in the datacenter.
func (dc *Datacenter) GetAllDatastores(ctx context.Context) (map[string]*DatastoreInfo, error) {
	log := logger.GetLogger(ctx)
	finder := find.NewFinder(dc.Client(), false)
	finder.SetDatacenter(dc.Datacenter)
	datastores, err := finder.DatastoreList(ctx, "*")
	if err != nil {
		log.Errorf("failed to get all the datastores in the Datacenter %s with error: %v", dc.Datacenter.String(), err)
		return nil, err
	}
	var dsList []types.ManagedObjectReference
	for _, ds := range datastores {
		dsList = append(dsList, ds.Reference())
	}
	var dsMoList []mo.Datastore
	pc := property.DefaultCollector(dc.Client())
	properties := []string{"info"}
	err = pc.Retrieve(ctx, dsList, properties, &dsMoList)
	if err != nil {
		log.Errorf("failed to get datastore managed objects from datastore objects %v with properties %v: %v",
			dsList, properties, err)
		return nil, err
	}
	dsURLInfoMap := make(map[string]*DatastoreInfo)
	for _, dsMo := range dsMoList {
		dsURLInfoMap[dsMo.Info.GetDatastoreInfo().Url] = &DatastoreInfo{
			&Datastore{object.NewDatastore(dc.Client(), dsMo.Reference()),
				dc},
			dsMo.Info.GetDatastoreInfo()}
	}
	return dsURLInfoMap, nil
}
