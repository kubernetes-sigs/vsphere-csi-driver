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
	"errors"

	"sigs.k8s.io/vsphere-csi-driver/v3/pkg/csi/service/logger"

	"github.com/vmware/govmomi/object"
	"github.com/vmware/govmomi/property"
	"github.com/vmware/govmomi/vim25/methods"
	"github.com/vmware/govmomi/vim25/mo"
	"github.com/vmware/govmomi/vim25/types"
)

var (
	ErrNoSharedDSFound     = errors.New("no shared datastores found among given hosts")
	ErrNoAccessibleDSFound = errors.New("no accessible datastores found among given hosts")
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
	err := host.Properties(ctx, host.Reference(), []string{"datastore"}, &hostSystemMo)
	if err != nil {
		log.Errorf("failed to retrieve datastores for host %v with err: %v", host, err)
		return nil, err
	}
	var dsRefList []types.ManagedObjectReference
	dsRefList = append(dsRefList, hostSystemMo.Datastore...)

	var dsMoList []mo.Datastore
	pc := property.DefaultCollector(host.Client())
	properties := []string{"info", "customValue"}
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
				dsMo.Info.GetDatastoreInfo(), dsMo.CustomValue})
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

type DiskHealth struct {
	HealthFlags  int   `json:"healthFlags,omitempty"`
	HealthReason int   `json:"healthReason,omitempty"`
	TimeStamp    int64 `json:"timestamp,omitempty"`
}

// VsanPhysicalDisk reflects the fields of JSON structure emitted by the
// VsanInternalSystem.QueryPhysicalVsanDisks API that we care about.
type VsanPhysicalDisk struct {
	IsSSD            int        `json:"isSsd,omitempty"`
	SsdUUID          string     `json:"ssdUuid,omitempty"`
	Capacity         int64      `json:"capacity,omitempty"`
	CapacityReserved int64      `json:"capacityReserved,omitempty"`
	CapacityUsed     int64      `json:"capacityUsed,omitempty"`
	IsAllFlash       int        `json:"isAllFlash,omitempty"`
	Disk_Health      DiskHealth `json:"disk_health,omitempty"`
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
	log := logger.GetLogger(ctx)
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

	for key, disk := range disks {
		if disk.IsSSD == 1 {
			// Cache disk doesn't count as capacity.
			continue
		}
		// Check for disk health
		if disk.Disk_Health.HealthFlags != 0 {
			log.Infof("disk %s is erroneous. Health: %+v", key, disk.Disk_Health)
			continue
		}
		out.Capacity += disk.Capacity
		out.CapacityReserved += disk.CapacityReserved
		out.CapacityUsed += disk.CapacityUsed
	}
	return &out, nil
}

// GetSharedDatastoresForHosts returns an intersection of datastores accessible to each host in the hosts parameter.
func GetSharedDatastoresForHosts(ctx context.Context, hosts []*HostSystem) ([]*DatastoreInfo, error) {
	log := logger.GetLogger(ctx)
	var sharedDatastores []*DatastoreInfo

	for _, host := range hosts {
		accessibleDatastores, err := host.GetAllAccessibleDatastores(ctx)
		if err != nil {
			return nil, logger.LogNewErrorf(log, "failed to fetch datastores from host %+v. Error: %+v",
				host, err)
		}
		if len(accessibleDatastores) == 0 {
			return nil, logger.LogNewErrorf(log, "failed to find accessible datastores for host %+v.",
				host)
		}
		if len(sharedDatastores) == 0 {
			sharedDatastores = accessibleDatastores
		} else {
			var sharedAccessibleDatastores []*DatastoreInfo
			for _, sharedDs := range sharedDatastores {
				// Check if sharedDatastores is found in accessibleDatastores.
				for _, accessibleDs := range accessibleDatastores {
					// Intersection is performed based on the datastoreUrl as this
					// uniquely identifies the datastore.
					if accessibleDs.Info.Url == sharedDs.Info.Url {
						sharedAccessibleDatastores = append(sharedAccessibleDatastores, sharedDs)
						break
					}
				}
			}
			sharedDatastores = sharedAccessibleDatastores
		}
		if len(sharedDatastores) == 0 {
			return nil, ErrNoSharedDSFound
		}
	}
	return sharedDatastores, nil
}

// GetAllAccessibleDatastoresForHosts returns an union of all the datastores accessible
// to each host in the hosts parameter.
func GetAllAccessibleDatastoresForHosts(ctx context.Context, hosts []*HostSystem) ([]*DatastoreInfo, error) {
	log := logger.GetLogger(ctx)
	var allAccessibleDatastores []*DatastoreInfo

	for _, host := range hosts {
		accessibleDatastores, err := host.GetAllAccessibleDatastores(ctx)
		if err != nil {
			log.Warnf("failed to fetch datastores from host %+v. Error: %+v",
				host, err)
			continue
		}
		if len(accessibleDatastores) == 0 {
			return nil, logger.LogNewErrorf(log, "failed to find accessible datastores for host %+v.",
				host)
		}
		// Add the accessibleDatastores list to allAccessibleDatastores without duplicates.
		for _, candidateDS := range accessibleDatastores {
			var found bool
			for _, ds := range allAccessibleDatastores {
				if ds.Info.Url == candidateDS.Info.Url {
					found = true
					break
				}
			}
			if !found {
				allAccessibleDatastores = append(allAccessibleDatastores, candidateDS)
			}
		}
	}
	if len(allAccessibleDatastores) == 0 {
		return nil, ErrNoAccessibleDSFound
	}
	return allAccessibleDatastores, nil
}
