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
	"errors"
	"fmt"
	"strings"
	"sync"

	"github.com/vmware/govmomi/vapi/tags"
	"github.com/vmware/govmomi/vim25/mo"
	"sigs.k8s.io/vsphere-csi-driver/v2/pkg/csi/service/logger"

	"github.com/vmware/govmomi/object"
	"github.com/vmware/govmomi/vim25/types"
)

// ErrVMNotFound is returned when a virtual machine isn't found.
var ErrVMNotFound = errors.New("virtual machine wasn't found")

// VirtualMachine holds details of a virtual machine instance.
type VirtualMachine struct {
	// VirtualCenterHost represents the virtual machine's vCenter host.
	VirtualCenterHost string
	// UUID represents the virtual machine's UUID.
	UUID string
	// VirtualMachine represents the virtual machine.
	*object.VirtualMachine
	// Datacenter represents the datacenter to which the virtual machine belongs.
	Datacenter *Datacenter
}

func (vm *VirtualMachine) String() string {
	return fmt.Sprintf("%v [VirtualCenterHost: %v, UUID: %v, Datacenter: %v]",
		vm.VirtualMachine, vm.VirtualCenterHost, vm.UUID, vm.Datacenter)
}

// IsActive returns true if Virtual Machine is powered on, else returns false.
func (vm *VirtualMachine) IsActive(ctx context.Context) (bool, error) {
	log := logger.GetLogger(ctx)
	vmMoList, err := vm.Datacenter.GetVMMoList(ctx, []*VirtualMachine{vm}, []string{"summary"})
	if err != nil {
		log.Errorf("failed to get VM Managed object with property summary. err: +%v", err)
		return false, err
	}
	if vmMoList[0].Summary.Runtime.PowerState == types.VirtualMachinePowerStatePoweredOn {
		return true, nil
	}
	return false, nil
}

// renew renews the virtual machine and datacenter objects on the given vc.
func (vm *VirtualMachine) renew(vc *VirtualCenter) {
	vm.VirtualMachine = object.NewVirtualMachine(vc.Client.Client, vm.VirtualMachine.Reference())
	vm.Datacenter.Datacenter = object.NewDatacenter(vc.Client.Client, vm.Datacenter.Reference())
}

// GetAllAccessibleDatastores gets the list of accessible Datastores for the
// given Virtual Machine.
func (vm *VirtualMachine) GetAllAccessibleDatastores(ctx context.Context) ([]*DatastoreInfo, error) {
	log := logger.GetLogger(ctx)
	host, err := vm.HostSystem(ctx)
	if err != nil {
		log.Errorf("failed to get host system for VM %v with err: %v", vm.InventoryPath, err)
		return nil, err
	}
	hostObj := &HostSystem{
		HostSystem: object.NewHostSystem(vm.Client(), host.Reference()),
	}
	return hostObj.GetAllAccessibleDatastores(ctx)
}

// Renew renews the virtual machine and datacenter information. If reconnect is
// set to true, the virtual center connection is also renewed.
func (vm *VirtualMachine) Renew(ctx context.Context, reconnect bool) error {
	log := logger.GetLogger(ctx)
	vc, err := GetVirtualCenterManager(ctx).GetVirtualCenter(ctx, vm.VirtualCenterHost)
	if err != nil {
		log.Errorf("failed to get VC while renewing VM %v with err: %v", vm, err)
		return err
	}

	if reconnect {
		if err := vc.Connect(ctx); err != nil {
			log.Errorf("Failed reconnecting to VC %q while renewing VM %v with err: %v", vc.Config.Host, vm, err)
			return err
		}
	}
	vm.renew(vc)
	return nil
}

const (
	// poolSize is the number of goroutines to run while trying to find a
	// virtual machine.
	poolSize = 8
	// dcBufferSize is the buffer size for the channel that is used to
	// asynchronously receive *Datacenter instances.
	dcBufferSize = poolSize * 10
	//ProviderPrefix is the prefix used for the ProviderID set on the node
	// Example: vsphere://4201794a-f26b-8914-d95a-edeb7ecc4a8f
	providerPrefix = "vsphere://"
)

// GetVirtualMachineByUUID returns virtual machine given its UUID in entire VC.
// If instanceUuid is set to true, then UUID is an instance UUID.
// In this case, this function searches for virtual machines whose instance UUID
// matches the given uuid.
// If instanceUuid is set to false, then UUID is BIOS UUID.
// In this case, this function searches for virtual machines whose BIOS UUID
// matches the given uuid.
func GetVirtualMachineByUUID(ctx context.Context, uuid string, instanceUUID bool) (*VirtualMachine, error) {
	log := logger.GetLogger(ctx)
	log.Infof("Initiating asynchronous datacenter listing with uuid %s", uuid)
	dcsChan, errChan := AsyncGetAllDatacenters(ctx, dcBufferSize)

	var wg sync.WaitGroup
	var nodeVM *VirtualMachine
	var poolErr error

	for i := 0; i < poolSize; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				select {
				case err, ok := <-errChan:
					if !ok {
						// Async function finished.
						log.Debugf("AsyncGetAllDatacenters finished with uuid %s", uuid)
						return
					} else if err == context.Canceled {
						// Canceled by another instance of this goroutine.
						log.Debugf("AsyncGetAllDatacenters ctx was canceled with uuid %s", uuid)
						return
					} else {
						// Some error occurred.
						log.Errorf("AsyncGetAllDatacenters with uuid %s sent an error: %v", uuid, err)
						poolErr = err
						return
					}

				case dc, ok := <-dcsChan:
					if !ok {
						// Async function finished.
						log.Debugf("AsyncGetAllDatacenters finished with uuid %s", uuid)
						return
					}

					// Found some Datacenter object.
					log.Infof("AsyncGetAllDatacenters with uuid %s sent a dc %v", uuid, dc)
					if vm, err := dc.GetVirtualMachineByUUID(ctx, uuid, instanceUUID); err != nil {
						if err == ErrVMNotFound {
							// Didn't find VM on this DC, so, continue searching on other DCs.
							log.Warnf("Couldn't find VM given uuid %s on DC %v with err: %v, continuing search", uuid, dc, err)
							continue
						} else {
							// Some serious error occurred, so stop the async function.
							log.Errorf("Failed finding VM given uuid %s on DC %v with err: %v", uuid, dc, err)
							poolErr = err
							return
						}
					} else {
						// Virtual machine was found, so stop the async function.
						log.Infof("Found VM %v given uuid %s on DC %v", vm, uuid, dc)
						nodeVM = vm
						return
					}
				}
			}
		}()
	}
	wg.Wait()

	if nodeVM != nil {
		log.Infof("Returning VM %v for UUID %s", nodeVM, uuid)
		return nodeVM, nil
	} else if poolErr != nil {
		log.Errorf("Returning err: %v for UUID %s", poolErr, uuid)
		return nil, poolErr
	} else {
		log.Errorf("Returning VM not found err for UUID %s", uuid)
		return nil, ErrVMNotFound
	}
}

// GetHostSystem returns HostSystem object of the virtual machine.
func (vm *VirtualMachine) GetHostSystem(ctx context.Context) (*object.HostSystem, error) {
	log := logger.GetLogger(ctx)
	vmHost, err := vm.VirtualMachine.HostSystem(ctx)
	if err != nil {
		log.Errorf("failed to get host system for vm: %v. err: %+v", vm, err)
		return nil, err
	}
	var oHost mo.HostSystem
	err = vmHost.Properties(ctx, vmHost.Reference(), []string{"summary"}, &oHost)
	if err != nil {
		log.Errorf("failed to get host system properties. err: %+v", err)
		return nil, err
	}
	log.Debugf("Host owning node vm: %v is %s", vm, oHost.Summary.Config.Name)
	return vmHost, nil
}

// GetTagManager returns tagManager using vm client.
func (vm *VirtualMachine) GetTagManager(ctx context.Context) (*tags.Manager, error) {
	log := logger.GetLogger(ctx)
	virtualCenter, err := GetVirtualCenterManager(ctx).GetVirtualCenter(ctx, vm.VirtualCenterHost)
	if err != nil {
		log.Errorf("failed to get virtualCenter. Error: %v", err)
		return nil, err
	}
	return GetTagManager(ctx, virtualCenter)
}

// GetAncestors returns ancestors of VM.
// Example result: "Folder", "Datacenter", "Cluster".
func (vm *VirtualMachine) GetAncestors(ctx context.Context) ([]mo.ManagedEntity, error) {
	log := logger.GetLogger(ctx)
	vmHost, err := vm.GetHostSystem(ctx)
	if err != nil {
		log.Errorf("failed to get host system for vm: %v. err: %+v", vm, err)
		return nil, err
	}
	var objects []mo.ManagedEntity
	pc := vm.Datacenter.Client().ServiceContent.PropertyCollector
	// Example result: ["Folder", "Datacenter", "Cluster"]
	objects, err = mo.Ancestors(ctx, vm.Datacenter.Client(), pc, vmHost.Reference())
	if err != nil {
		log.Errorf("GetAncestors failed for %s with err %v", vmHost.Reference(), err)
		return nil, err
	}
	log.Debugf("Ancestors of node vm: %v are : [%+v]", vm, objects)
	return objects, nil
}

// GetZoneRegion returns zone and region of the node vm.
func (vm *VirtualMachine) GetZoneRegion(ctx context.Context, zoneCategoryName string,
	regionCategoryName string, tagManager *tags.Manager) (zone string, region string, err error) {
	log := logger.GetLogger(ctx)
	log.Debugf("GetZoneRegion: called with zoneCategoryName: %s, regionCategoryName: %s",
		zoneCategoryName, regionCategoryName)
	var objects []mo.ManagedEntity
	objects, err = vm.GetAncestors(ctx)
	if err != nil {
		log.Errorf("GetAncestors failed for %s with err %v", vm.Reference(), err)
		return "", "", err
	}
	// Search the hierarchy, example order:
	//    ["Host", "Cluster", "Datacenter", "Folder"]
	for i := range objects {
		obj := objects[len(objects)-1-i]
		log.Debugf("Name: %s, Type: %s", obj.Self.Value, obj.Self.Type)
		tags, err := tagManager.ListAttachedTags(ctx, obj)
		if err != nil {
			log.Errorf("Cannot list attached tags. Err: %v", err)
			return "", "", err
		}
		if len(tags) > 0 {
			log.Debugf("Object [%v] has attached Tags [%v]", obj, tags)
		}
		for _, value := range tags {
			tag, err := tagManager.GetTag(ctx, value)
			if err != nil {
				log.Errorf("failed to get tag:%s, error:%v", value, err)
				return "", "", err
			}
			log.Infof("Found tag: %s for object %v", tag.Name, obj)
			category, err := tagManager.GetCategory(ctx, tag.CategoryID)
			if err != nil {
				log.Errorf("failed to get category for tag: %s, error: %v", tag.Name, tag)
				return "", "", err
			}
			log.Debugf("Found category: %s for object %v with tag: %s", category.Name, obj, tag.Name)

			if category.Name == zoneCategoryName && zone == "" {
				zone = tag.Name
			} else if category.Name == regionCategoryName && region == "" {
				region = tag.Name
			}
			if zone != "" && region != "" {
				return zone, region, nil
			}
		}
	}
	return zone, region, err
}

// IsInZoneRegion checks if VM belongs to specified zone and region.
// This function returns true if yes, false otherwise.
func (vm *VirtualMachine) IsInZoneRegion(ctx context.Context, zoneCategoryName string, regionCategoryName string,
	zoneValue string, regionValue string, tagManager *tags.Manager) (bool, error) {
	log := logger.GetLogger(ctx)
	log.Infof("IsInZoneRegion: called with zoneCategoryName: %s, regionCategoryName: %s, zoneValue: %s, regionValue: %s",
		zoneCategoryName, regionCategoryName, zoneValue, regionValue)
	vmZone, vmRegion, err := vm.GetZoneRegion(ctx, zoneCategoryName, regionCategoryName, tagManager)
	if err != nil {
		log.Errorf("failed to get accessibleTopology for vm: %v, err: %v", vm.Reference(), err)
		return false, err
	}
	if regionValue == "" && zoneValue != "" && vmZone == zoneValue {
		// Region is not specified. If zone matches with look up zone value,
		// return true.
		log.Debugf("MoRef [%v] belongs to zone [%s]", vm.Reference(), zoneValue)
		return true, nil
	}
	if zoneValue == "" && regionValue != "" && vmRegion == regionValue {
		// Zone is not specified. If region matches with look up region value,
		// return true.
		log.Debugf("MoRef [%v] belongs to region [%s]", vm.Reference(), regionValue)
		return true, nil
	}
	if vmZone != "" && vmRegion != "" && vmRegion == regionValue && vmZone == zoneValue {
		log.Debugf("MoRef [%v] belongs to zone [%s] and region [%s]", vm.Reference(), zoneValue, regionValue)
		return true, nil
	}
	return false, nil
}

// GetUUIDFromProviderID Returns VM UUID from Node's providerID.
func GetUUIDFromProviderID(providerID string) string {
	return strings.TrimPrefix(providerID, providerPrefix)
}

// GetSharedDatastoresForVMs returns shared datastores accessible to specified
// nodeVMs list.
func GetSharedDatastoresForVMs(ctx context.Context, nodeVMs []*VirtualMachine) ([]*DatastoreInfo, error) {
	var sharedDatastores []*DatastoreInfo
	log := logger.GetLogger(ctx)
	for _, nodeVM := range nodeVMs {
		log.Debugf("Getting accessible datastores for node %s", nodeVM.VirtualMachine)
		accessibleDatastores, err := nodeVM.GetAllAccessibleDatastores(ctx)
		if err != nil {
			return nil, err
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
					if sharedDs.Info.Url == accessibleDs.Info.Url {
						sharedAccessibleDatastores = append(sharedAccessibleDatastores, sharedDs)
						break
					}
				}
			}
			sharedDatastores = sharedAccessibleDatastores
		}
		if len(sharedDatastores) == 0 {
			return nil, fmt.Errorf("no shared datastores found for nodeVm: %+v", nodeVM)
		}
	}
	return sharedDatastores, nil
}

// GetTopologyLabels populates the topology labels of the nodeVM in topologyCategories
// parameter given the category names.
func (vm *VirtualMachine) GetTopologyLabels(ctx context.Context, tagManager *tags.Manager,
	topologyCategories map[string]string) error {
	log := logger.GetLogger(ctx)

	// Get NodeVM ancestors.
	objects, err := vm.GetAncestors(ctx)
	if err != nil {
		log.Errorf("GetAncestors failed for %v with err %v", vm.Reference(), err)
		return err
	}
	// Search the hierarchy, example order: ["Host", "Cluster", "Datacenter", "Folder"].
	for i := range objects {
		obj := objects[len(objects)-1-i]
		log.Debugf("Name: %s, Type: %s", obj.Self.Value, obj.Self.Type)
		objTags, err := tagManager.ListAttachedTags(ctx, obj)
		if err != nil {
			log.Errorf("Cannot list attached tags. Err: %v", err)
			return err
		}
		// Check if object has tags.
		if len(objTags) > 0 {
			log.Debugf("Object [%v] has attached Tags [%v]", obj, objTags)
		}
		for _, value := range objTags {
			// Get tag.
			tag, err := tagManager.GetTag(ctx, value)
			if err != nil {
				log.Errorf("failed to get tag:%s, error:%v", value, err)
				return err
			}
			log.Debugf("Found tag: %s for object %v", tag.Name, obj)
			// Get category for tag.
			category, err := tagManager.GetCategory(ctx, tag.CategoryID)
			if err != nil {
				log.Errorf("failed to get category for tag: %s, error: %v", tag.Name, tag)
				return err
			}
			log.Infof("Found category: %s for object %v with tag: %s", category.Name, obj, tag.Name)
			// Check if the category belongs to a topology domain recognised by the driver.
			val, exists := topologyCategories[category.Name]
			if exists {
				// Update the value if it doesn't already exist.
				if val == "" {
					topologyCategories[category.Name] = tag.Name
				} else {
					// Error out on duplicate values for the same category.
					return logger.LogNewErrorf(log, "duplicate values detected for category %s as %q and %q",
						category.Name, val, tag.Name)
				}
			}
			// Check if values for all topology domains have been retrieved.
			// If yes, then return.
			if len(findMissingCategories(topologyCategories)) == 0 {
				log.Infof("Tags related to all topology categories found. Skipping tag check on following "+
					"entities: %v", objects[:len(objects)-1-i])
				return nil
			}
		}
	}
	// Raise error if nodeVM does not have a topology label associated with
	// each category in the vSphere config secret `Labels` section.
	missing := findMissingCategories(topologyCategories)
	if len(missing) != 0 {
		return logger.LogNewErrorf(log, "nodeVM %s does not have labels for the following categories: %+v",
			vm.Reference(), missing)
	}
	return nil
}

// findMissingCategories returns the list of keys with an empty string as value.
func findMissingCategories(topologyCategories map[string]string) []string {
	missing := make([]string, 0)
	for key, val := range topologyCategories {
		if val == "" {
			missing = append(missing, key)
		}
	}
	return missing
}
