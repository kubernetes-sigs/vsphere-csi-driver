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
	"encoding/json"

	"sigs.k8s.io/vsphere-csi-driver/v2/pkg/csi/service/logger"

	"github.com/vmware/govmomi/object"
	"github.com/vmware/govmomi/property"
	"github.com/vmware/govmomi/vim25/methods"
	"github.com/vmware/govmomi/vim25/mo"
	"github.com/vmware/govmomi/vim25/types"
)

// HostSystem holds details of a host instance.
type HostSystem struct {
	// HostSystem represents the host system.
	*object.HostSystem
}

// GetAllAccessibleDatastores gets the list of accessible datastores for the
// given host.
func (host *HostSystem) GetAllAccessibleDatastores(ctx context.Context) ([]*DatastoreInfo, error) {
	log := logger.GetLogger(ctx)
	var hostSystemMo mo.HostSystem
	s := object.NewSearchIndex(host.Client())
	err := s.Properties(ctx, host.Reference(), []string{"datastore"}, &hostSystemMo)
	if err != nil {
		log.Errorf("failed to retrieve datastores for host %v with err: %v", host, err)
		return nil, err
	}
	var dsRefList []types.ManagedObjectReference
	dsRefList = append(dsRefList, hostSystemMo.Datastore...)

	var dsMoList []mo.Datastore
	pc := property.DefaultCollector(host.Client())
	properties := []string{"info"}
	err = pc.Retrieve(ctx, dsRefList, properties, &dsMoList)
	if err != nil {
		log.Errorf("failed to get datastore managed objects from datastore objects %v with properties %v: %v",
			dsRefList, properties, err)
		return nil, err
	}
	var dsObjList []*DatastoreInfo
	for _, dsMo := range dsMoList {
		dsObjList = append(dsObjList,
			&DatastoreInfo{
				&Datastore{object.NewDatastore(host.Client(), dsMo.Reference()),
					nil},
				dsMo.Info.GetDatastoreInfo()})
	}
	return dsObjList, nil
}

// GetHostVsanNodeUUID gets the vSAN NodeUuid for this host.
func (host *HostSystem) GetHostVsanNodeUUID(ctx context.Context) (string, error) {
	log := logger.GetLogger(ctx)
	hostVsanSystem, err := host.ConfigManager().VsanSystem(ctx)
	if err != nil {
		log.Errorf("Failed getting the VsanSystem for host %v with err: %v", host, err)
		return "", err
	}
	var vsan mo.HostVsanSystem
	err = hostVsanSystem.Properties(ctx, hostVsanSystem.Reference(), []string{"config.clusterInfo"}, &vsan)
	if err != nil {
		log.Errorf("Failed fetching 'config.clusterInfo' of host %v with err: %v", host, err)
		return "", err
	}
	return vsan.Config.ClusterInfo.NodeUuid, nil
}

// VsanHostCapacity captures the capacity info of a host. It exists to support
// the API within this Go helper module.
type VsanHostCapacity struct {
	Capacity         int64
	CapacityReserved int64
	CapacityUsed     int64
	HostMoID         string
}

// VsanPhysicalDisk reflects the fields of JSON structure emitted by the
// VsanInternalSystem.QueryPhysicalVsanDisks API that we care about.
type VsanPhysicalDisk struct {
	IsSSD            int    `json:"isSsd,omitempty"`
	SsdUUID          string `json:"ssdUuid,omitempty"`
	Capacity         int64  `json:"capacity,omitempty"`
	CapacityReserved int64  `json:"capacityReserved,omitempty"`
	CapacityUsed     int64  `json:"capacityUsed,omitempty"`
	IsAllFlash       int    `json:"isAllFlash,omitempty"`
}

// VsanPhysicalDiskMap is what VsanInternalSystem.QueryPhysicalVsanDisks returns
type VsanPhysicalDiskMap map[string]VsanPhysicalDisk

// QueryPhysicalVsanDisks wraps the underlying vSAN API and unmarshals the JSON
// result as well.
func (host *HostSystem) QueryPhysicalVsanDisks(ctx context.Context) (VsanPhysicalDiskMap, error) {
	out := make(VsanPhysicalDiskMap)
	log := logger.GetLogger(ctx)
	// XXX: We could consider caching the VsanInternalSystem.
	hostVis, err := host.ConfigManager().VsanInternalSystem(ctx)
	if err != nil {
		log.Errorf("Failed getting the VsanSystem for host %v with err: %v", host, err)
		return out, err
	}

	// self_only means we only want the disks from this host. We could ask one
	// host for all disks, but that depends on the vSAN network being healthy.
	// For this code we rather ask the hosts for their authoritative info, which
	// is their local disks.
	req := types.QueryPhysicalVsanDisks{
		This: hostVis.Reference(),
		Props: []string{"self_only", "disk_health", "capacity", "isSsd",
			"ssdUuid", "isAllFlash", "capacityReserved", "capacityUsed"},
	}

	res, err := methods.QueryPhysicalVsanDisks(ctx, hostVis.Client(), &req)
	if err != nil {
		return out, err
	}

	err = json.Unmarshal([]byte(res.Returnval), &out)
	if err != nil {
		return out, err
	}

	return out, nil
}

// GetHostVsanCapacity wraps around QueryPhysicalVsanDisks to sum up all the
// capacity disks of a host.
func (host *HostSystem) GetHostVsanCapacity(ctx context.Context) (*VsanHostCapacity, error) {
	out := VsanHostCapacity{
		Capacity:         0,
		CapacityReserved: 0,
		CapacityUsed:     0,
		HostMoID:         host.Reference().Value,
	}
	disks, err := host.QueryPhysicalVsanDisks(ctx)
	if err != nil {
		return &out, err
	}

	for _, disk := range disks {
		if disk.IsSSD == 1 {
			// Cache disk doesn't count as capacity.
			continue
		}
		// XXX: Check for health?
		out.Capacity += disk.Capacity
		out.CapacityReserved += disk.CapacityReserved
		out.CapacityUsed += disk.CapacityUsed
	}
	return &out, nil
}
