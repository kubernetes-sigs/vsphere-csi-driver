// Copyright 2018 VMware, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//    http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package vsphere

import (
	"context"
	"errors"
	"fmt"
	"gitlab.eng.vmware.com/hatchway/govmomi/vapi/rest"
	"gitlab.eng.vmware.com/hatchway/govmomi/vapi/tags"
	"gitlab.eng.vmware.com/hatchway/govmomi/vim25/mo"
	"net/url"
	"sync"

	"gitlab.eng.vmware.com/hatchway/govmomi/object"
	"gitlab.eng.vmware.com/hatchway/govmomi/vim25/types"
	"k8s.io/klog"
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
	vmMoList, err := vm.Datacenter.GetVMMoList(ctx, []*VirtualMachine{vm}, []string{"summary"})
	if err != nil {
		klog.Errorf("Failed to get VM Managed object with property summary. err: +%v", err)
		return false, err
	}
	if vmMoList[0].Summary.Runtime.PowerState == types.VirtualMachinePowerStatePoweredOn {
		return true, nil
	}
	return false, nil
}

// renew renews the virtual machine and datacenter objects given its virtual center.
func (vm *VirtualMachine) renew(vc *VirtualCenter) {
	vm.VirtualMachine = object.NewVirtualMachine(vc.Client.Client, vm.VirtualMachine.Reference())
	vm.Datacenter.Datacenter = object.NewDatacenter(vc.Client.Client, vm.Datacenter.Reference())
}

// GetAllAccessibleDatastores gets the list of accessible Datastores for the given Virtual Machine
func (vm *VirtualMachine) GetAllAccessibleDatastores(ctx context.Context) ([]*DatastoreInfo, error) {
	host, err := vm.HostSystem(ctx)
	if err != nil {
		klog.Errorf("Failed to get host system for VM %v with err: %v", vm.InventoryPath, err)
		return nil, err
	}
	hostObj := &HostSystem{
		HostSystem: object.NewHostSystem(vm.Client(), host.Reference()),
	}
	return hostObj.GetAllAccessibleDatastores(ctx)
}

// Renew renews the virtual machine and datacenter information. If reconnect is
// set to true, the virtual center connection is also renewed.
func (vm *VirtualMachine) Renew(reconnect bool) error {
	vc, err := GetVirtualCenterManager().GetVirtualCenter(vm.VirtualCenterHost)
	if err != nil {
		klog.Errorf("Failed to get VC while renewing VM %v with err: %v", vm, err)
		return err
	}

	if reconnect {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		if err := vc.Connect(ctx); err != nil {
			klog.Errorf("Failed reconnecting to VC %q while renewing VM %v with err: %v", vc.Config.Host, vm, err)
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
)

// GetVirtualMachineByUUID returns virtual machine given its UUID in entire VC.
// If instanceUuid is set to true, then UUID is an instance UUID.
// In this case, this function searches for virtual machines whose instance UUID matches the given uuid.
// If instanceUuid is set to false, then UUID is BIOS UUID.
// In this case, this function searches for virtual machines whose BIOS UUID matches the given uuid.
func GetVirtualMachineByUUID(uuid string, instanceUUID bool) (*VirtualMachine, error) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	klog.V(2).Infof("Initiating asynchronous datacenter listing with uuid %s", uuid)
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
						klog.V(2).Infof("AsyncGetAllDatacenters finished with uuid %s", uuid)
						return
					} else if err == context.Canceled {
						// Canceled by another instance of this goroutine.
						klog.V(2).Infof("AsyncGetAllDatacenters ctx was canceled with uuid %s", uuid)
						return
					} else {
						// Some error occurred.
						klog.Errorf("AsyncGetAllDatacenters with uuid %s sent an error: %v", uuid, err)
						poolErr = err
						return
					}

				case dc, ok := <-dcsChan:
					if !ok {
						// Async function finished.
						klog.V(2).Infof("AsyncGetAllDatacenters finished with uuid %s", uuid)
						return
					}

					// Found some Datacenter object.
					klog.V(2).Infof("AsyncGetAllDatacenters with uuid %s sent a dc %v", uuid, dc)
					if vm, err := dc.GetVirtualMachineByUUID(context.Background(), uuid, instanceUUID); err != nil {
						if err == ErrVMNotFound {
							// Didn't find VM on this DC, so, continue searching on other DCs.
							klog.V(2).Infof("Couldn't find VM given uuid %s on DC %v with err: %v, continuing search", uuid, dc, err)
							continue
						} else {
							// Some serious error occurred, so stop the async function.
							klog.Errorf("Failed finding VM given uuid %s on DC %v with err: %v, canceling context", uuid, dc, err)
							cancel()
							poolErr = err
							return
						}
					} else {
						// Virtual machine was found, so stop the async function.
						klog.V(2).Infof("Found VM %v given uuid %s on DC %v, canceling context", vm, uuid, dc)
						nodeVM = vm
						cancel()
						return
					}
				}
			}
		}()
	}
	wg.Wait()

	if nodeVM != nil {
		klog.V(2).Infof("Returning VM %v for UUID %s", nodeVM, uuid)
		return nodeVM, nil
	} else if poolErr != nil {
		klog.Errorf("Returning err: %v for UUID %s", poolErr, uuid)
		return nil, poolErr
	} else {
		klog.Errorf("Returning VM not found err for UUID %s", uuid)
		return nil, ErrVMNotFound
	}
}

// GetHostSystem returns HostSystem object of the virtual machine
func (vm *VirtualMachine) GetHostSystem(ctx context.Context) (*object.HostSystem, error) {
	vmHost, err := vm.VirtualMachine.HostSystem(ctx)
	if err != nil {
		klog.Errorf("Failed to get host system for vm: %v. err: %+v", vm, err)
		return nil, err
	}
	var oHost mo.HostSystem
	err = vmHost.Properties(ctx, vmHost.Reference(), []string{"summary"}, &oHost)
	if err != nil {
		klog.Errorf("Failed to get host system properties. err: %+v", err)
		return nil, err
	}
	klog.V(4).Infof("Host owning node vm: %v is %s", vm, oHost.Summary.Config.Name)
	return vmHost, nil
}

// GetTagManager returns tagManager using vm client
func (vm *VirtualMachine) GetTagManager(ctx context.Context) (*tags.Manager, error) {
	restClient := rest.NewClient(vm.Client())
	virtualCenter, err := GetVirtualCenterManager().GetVirtualCenter(vm.VirtualCenterHost)
	if err != nil {
		klog.Errorf("Failed to get virtualCenter. Error: %v", err)
		return nil, err
	}
	signer, err := signer(ctx, vm.Client(), virtualCenter.Config.Username, virtualCenter.Config.Password)
	if err != nil {
		klog.Errorf("Failed to create the Signer. Error: %v", err)
		return nil, err
	}
	if signer == nil {
		klog.V(3).Info("Using plain text username and password")
		user := url.UserPassword(virtualCenter.Config.Username, virtualCenter.Config.Password)
		err = restClient.Login(ctx, user)
	} else {
		klog.V(3).Info("Using certificate and private key")
		err = restClient.LoginByToken(restClient.WithSigner(ctx, signer))
	}
	if err != nil {
		klog.Errorf("Failed to login for the rest client. Error: %v", err)
	}
	tagManager := tags.NewManager(restClient)
	if tagManager == nil {
		klog.Errorf("Failed to create a tagManager")
	}
	return tagManager, nil
}

// GetAncestors returns ancestors of VM
// example result: "Folder", "Datacenter", "Cluster"
func (vm *VirtualMachine) GetAncestors(ctx context.Context) ([]mo.ManagedEntity, error) {
	vmHost, err := vm.GetHostSystem(ctx)
	if err != nil {
		klog.Errorf("Failed to get host system for vm: %v. err: %+v", vm, err)
		return nil, err
	}
	var objects []mo.ManagedEntity
	pc := vm.Datacenter.Client().ServiceContent.PropertyCollector
	// example result: ["Folder", "Datacenter", "Cluster"]
	objects, err = mo.Ancestors(ctx, vm.Datacenter.Client(), pc, vmHost.Reference())
	if err != nil {
		klog.Errorf("GetAncestors failed for %s with err %v", vmHost.Reference(), err)
		return nil, err
	}
	klog.V(4).Infof("Ancestors of node vm: %v are : [%+v]", vm, objects)
	return objects, nil
}

// GetZoneRegion returns zone and region of the node vm
func (vm *VirtualMachine) GetZoneRegion(ctx context.Context, zoneCategoryName string, regionCategoryName string) (zone string, region string, err error) {
	klog.V(4).Infof("GetZoneRegion: called with zoneCategoryName: %s, regionCategoryName: %s", zoneCategoryName, regionCategoryName)
	tagManager, err := vm.GetTagManager(ctx)
	if err != nil || tagManager == nil {
		klog.Errorf("Failed to get tagManager. Error: %v", err)
		return "", "", err
	}
	defer tagManager.Logout(ctx)
	var objects []mo.ManagedEntity
	objects, err = vm.GetAncestors(ctx)
	if err != nil {
		klog.Errorf("GetAncestors failed for %s with err %v", vm.Reference(), err)
		return "", "", err
	}
	// search the hierarchy, example order: ["Host", "Cluster", "Datacenter", "Folder"]
	for i := range objects {
		obj := objects[len(objects)-1-i]
		klog.V(4).Infof("Name: %s, Type: %s", obj.Self.Value, obj.Self.Type)
		tags, err := tagManager.ListAttachedTags(ctx, obj)
		if err != nil {
			klog.Errorf("Cannot list attached tags. Err: %v", err)
			return "", "", err
		}
		if len(tags) > 0 {
			klog.V(4).Infof("Object [%v] has attached Tags [%v]", obj, tags)
		}
		for _, value := range tags {
			tag, err := tagManager.GetTag(ctx, value)
			if err != nil {
				klog.Errorf("Failed to get tag:%s, error:%v", value, err)
				return "", "", err
			}
			klog.V(4).Infof("Found tag: %s for object %v", tag.Name, obj)
			category, err := tagManager.GetCategory(ctx, tag.CategoryID)
			if err != nil {
				klog.Errorf("Failed to get category for tag: %s, error: %v", tag.Name, tag)
				return "", "", err
			}
			klog.V(4).Infof("Found category: %s for object %v with tag: %s", category.Name, obj, tag.Name)

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

// IsInZoneRegion checks if virtual machine belongs to specified zone and region
// This function returns true if virtual machine belongs to specified zone/region, else returns false.
func (vm *VirtualMachine) IsInZoneRegion(ctx context.Context, zoneCategoryName string, regionCategoryName string, zoneValue string, regionValue string) (bool, error) {
	klog.V(4).Infof("IsInZoneRegion: called with zoneCategoryName: %s, regionCategoryName: %s, zoneValue: %s, regionValue: %s", zoneCategoryName, regionCategoryName, zoneValue, regionValue)
	tagManager, err := vm.GetTagManager(ctx)
	if err != nil || tagManager == nil {
		klog.Errorf("Failed to get tagManager. Error: %v", err)
		return false, err
	}
	defer tagManager.Logout(ctx)
	vmZone, vmRegion, err := vm.GetZoneRegion(ctx, zoneCategoryName, regionCategoryName)
	if err != nil {
		klog.Errorf("failed to get accessibleTopology for vm: %v, err: %v", vm.Reference(), err)
		return false, err
	}
	if regionValue == "" && zoneValue != "" && vmZone == zoneValue {
		// region is not specified, if zone matches with look up zone value, return true
		klog.V(4).Infof("MoRef [%v] belongs to zone [%s]", vm.Reference(), zoneValue)
		return true, nil
	}
	if zoneValue == "" && regionValue != "" && vmRegion == regionValue {
		// zone is not specified, if region matches with look up region value, return true
		klog.V(4).Infof("MoRef [%v] belongs to region [%s]", vm.Reference(), regionValue)
		return true, nil
	}
	if vmZone != "" && vmRegion != "" && vmRegion == regionValue && vmZone == zoneValue {
		klog.V(4).Infof("MoRef [%v] belongs to zone [%s] and region [%s]", vm.Reference(), zoneValue, regionValue)
		return true, nil
	}
	return false, nil
}
