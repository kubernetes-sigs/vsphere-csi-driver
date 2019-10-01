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

package volume

import (
	"context"
	"errors"
	"sync"

	"github.com/davecgh/go-spew/spew"
	"gitlab.eng.vmware.com/hatchway/govmomi/cns"
	cnstypes "gitlab.eng.vmware.com/hatchway/govmomi/cns/types"
	"gitlab.eng.vmware.com/hatchway/govmomi/vim25/soap"
	vimtypes "gitlab.eng.vmware.com/hatchway/govmomi/vim25/types"
	"k8s.io/klog"
	cnsvsphere "sigs.k8s.io/vsphere-csi-driver/pkg/common/cns-lib/vsphere"
)

// Manager provides functionality to manage volumes.
type Manager interface {
	// CreateVolume creates a new volume given its spec.
	CreateVolume(spec *cnstypes.CnsVolumeCreateSpec) (*cnstypes.CnsVolumeId, error)
	// AttachVolume attaches a volume to a virtual machine given the spec.
	AttachVolume(vm *cnsvsphere.VirtualMachine, volumeID string) (string, error)
	// DetachVolume detaches a volume from the virtual machine given the spec.
	DetachVolume(vm *cnsvsphere.VirtualMachine, volumeID string) error
	// DeleteVolume deletes a volume given its spec.
	DeleteVolume(volumeID string, deleteDisk bool) error
	// UpdateVolumeMetadata updates a volume metadata given its spec.
	UpdateVolumeMetadata(spec *cnstypes.CnsVolumeMetadataUpdateSpec) error
	// QueryVolume returns volumes matching the given filter.
	QueryVolume(queryFilter cnstypes.CnsQueryFilter) (*cnstypes.CnsQueryResult, error)
	// QueryAllVolume returns all volumes matching the given filter and selection.
	QueryAllVolume(queryFilter cnstypes.CnsQueryFilter, querySelection cnstypes.CnsQuerySelection) (*cnstypes.CnsQueryResult, error)
	// ExpandVolume expands a volume to a new size.
	ExpandVolume(ctx context.Context, volumeID string, size int64) error
}

var (
	// managerInstance is a Manager singleton.
	managerInstance *defaultManager
	// onceForManager is used for initializing the Manager singleton.
	onceForManager sync.Once
)

// GetManager returns the Manager singleton.
func GetManager(vc *cnsvsphere.VirtualCenter) Manager {
	onceForManager.Do(func() {
		klog.V(1).Infof("Initializing volume.defaultManager...")
		managerInstance = &defaultManager{
			virtualCenter: vc,
		}
		klog.V(1).Infof("volume.defaultManager initialized")
	})
	return managerInstance
}

// DefaultManager provides functionality to manage volumes.
type defaultManager struct {
	virtualCenter *cnsvsphere.VirtualCenter
}

// CreateVolume creates a new volume given its spec.
func (m *defaultManager) CreateVolume(spec *cnstypes.CnsVolumeCreateSpec) (*cnstypes.CnsVolumeId, error) {
	err := validateManager(m)
	if err != nil {
		return nil, err
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	// Set up the VC connection
	err = m.virtualCenter.ConnectCns(ctx)
	if err != nil {
		klog.Errorf("ConnectCns failed with err: %+v", err)
		return nil, err
	}
	// If the VSphereUser in the CreateSpec is different from session user, update the CreateSpec
	s, err := m.virtualCenter.Client.SessionManager.UserSession(ctx)
	if err != nil {
		klog.Errorf("Failed to get usersession with err: %v", err)
		return nil, err
	}
	if s.UserName != spec.Metadata.ContainerCluster.VSphereUser {
		klog.V(4).Infof("Update VSphereUser from %s to %s", spec.Metadata.ContainerCluster.VSphereUser, s.UserName)
		spec.Metadata.ContainerCluster.VSphereUser = s.UserName
	}

	// Construct the CNS VolumeCreateSpec list
	var cnsCreateSpecList []cnstypes.CnsVolumeCreateSpec
	cnsCreateSpecList = append(cnsCreateSpecList, *spec)
	// Call the CNS CreateVolume
	task, err := m.virtualCenter.CnsClient.CreateVolume(ctx, cnsCreateSpecList)
	if err != nil {
		klog.Errorf("CNS CreateVolume failed from vCenter %q with err: %v", m.virtualCenter.Config.Host, err)
		return nil, err
	}
	// Get the taskInfo
	taskInfo, err := cns.GetTaskInfo(ctx, task)
	if err != nil {
		klog.Errorf("Failed to get taskInfo for CreateVolume task from vCenter %q with err: %v", m.virtualCenter.Config.Host, err)
		return nil, err
	}
	klog.V(2).Infof("CreateVolume: VolumeName: %q, opId: %q", spec.Name, taskInfo.ActivationId)
	// Get the taskResult
	taskResult, err := cns.GetTaskResult(ctx, taskInfo)

	if err != nil {
		klog.Errorf("unable to find the task result for CreateVolume task from vCenter %q. taskID: %q, opId: %q createResults: %+v",
			m.virtualCenter.Config.Host, taskInfo.Task.Value, taskInfo.ActivationId, taskResult)
		return nil, err
	}

	if taskResult == nil {
		klog.Errorf("taskResult is empty for CreateVolume task: %q", taskInfo.ActivationId)
		return nil, errors.New("taskResult is empty")
	}
	volumeOperationRes := taskResult.GetCnsVolumeOperationResult()
	if volumeOperationRes.Fault != nil {
		klog.Errorf("failed to create cns volume. createSpec: %q, fault: %q, opId: %q", spew.Sdump(spec), spew.Sdump(volumeOperationRes.Fault), taskInfo.ActivationId)
		return nil, errors.New(volumeOperationRes.Fault.LocalizedMessage)
	}
	klog.V(2).Infof("CreateVolume: Volume created successfully. VolumeName: %q, opId: %q, volumeID: %q", spec.Name, taskInfo.ActivationId, volumeOperationRes.VolumeId.Id)
	return &cnstypes.CnsVolumeId{
		Id: volumeOperationRes.VolumeId.Id,
	}, nil
}

// AttachVolume attaches a volume to a virtual machine given the spec.
func (m *defaultManager) AttachVolume(vm *cnsvsphere.VirtualMachine, volumeID string) (string, error) {
	err := validateManager(m)
	if err != nil {
		return "", err
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Set up the VC connection
	err = m.virtualCenter.ConnectCns(ctx)
	if err != nil {
		klog.Errorf("ConnectCns failed with err: %+v", err)
		return "", err
	}
	// Construct the CNS AttachSpec list
	var cnsAttachSpecList []cnstypes.CnsVolumeAttachDetachSpec
	cnsAttachSpec := cnstypes.CnsVolumeAttachDetachSpec{
		VolumeId: cnstypes.CnsVolumeId{
			Id: volumeID,
		},
		Vm: vm.Reference(),
	}
	cnsAttachSpecList = append(cnsAttachSpecList, cnsAttachSpec)
	// Call the CNS AttachVolume
	task, err := m.virtualCenter.CnsClient.AttachVolume(ctx, cnsAttachSpecList)
	if err != nil {
		klog.Errorf("CNS AttachVolume failed from vCenter %q with err: %v", m.virtualCenter.Config.Host, err)
		return "", err
	}
	// Get the taskInfo
	taskInfo, err := cns.GetTaskInfo(ctx, task)
	if err != nil {
		klog.Errorf("Failed to get taskInfo for AttachVolume task from vCenter %q with err: %v", m.virtualCenter.Config.Host, err)
		return "", err
	}
	klog.V(2).Infof("AttachVolume: volumeID: %q, vm: %q, opId: %q", volumeID, vm.String(), taskInfo.ActivationId)
	// Get the taskResult
	taskResult, err := cns.GetTaskResult(ctx, taskInfo)
	if err != nil {
		klog.Errorf("unable to find the task result for AttachVolume task from vCenter %q with taskID %s and attachResults %v",
			m.virtualCenter.Config.Host, taskInfo.Task.Value, taskResult)
		return "", err
	}

	if taskResult == nil {
		klog.Errorf("taskResult is empty for AttachVolume task: %q, opId: %q", taskInfo.Task.Value, taskInfo.ActivationId)
		return "", errors.New("taskResult is empty")
	}

	volumeOperationRes := taskResult.GetCnsVolumeOperationResult()
	if volumeOperationRes.Fault != nil {
		if volumeOperationRes.Fault.LocalizedMessage == CNSVolumeResourceInUseFaultMessage {
			// Volume is already attached to VM
			diskUUID, err := IsDiskAttached(ctx, vm, volumeID)
			if err != nil {
				return "", err
			}
			if diskUUID != "" {
				return diskUUID, nil
			}
		}
		klog.Errorf("failed to attach cns volume: %q to node vm: %q. fault: %q. opId: %q", volumeID, vm.String(), spew.Sdump(volumeOperationRes.Fault), taskInfo.ActivationId)
		return "", errors.New(volumeOperationRes.Fault.LocalizedMessage)
	}
	diskUUID := interface{}(taskResult).(*cnstypes.CnsVolumeAttachResult).DiskUUID
	klog.V(2).Infof("AttachVolume: Volume attached successfully. volumeID: %q, opId: %q, vm: %q, diskUUID: %q", volumeID, taskInfo.ActivationId, vm.String(), diskUUID)
	return diskUUID, nil
}

// DetachVolume detaches a volume from the virtual machine given the spec.
func (m *defaultManager) DetachVolume(vm *cnsvsphere.VirtualMachine, volumeID string) error {
	err := validateManager(m)
	if err != nil {
		return err
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	// Set up the VC connection
	err = m.virtualCenter.ConnectCns(ctx)
	if err != nil {
		klog.Errorf("ConnectCns failed with err: %+v", err)
		return err
	}
	// Construct the CNS DetachSpec list
	var cnsDetachSpecList []cnstypes.CnsVolumeAttachDetachSpec
	cnsDetachSpec := cnstypes.CnsVolumeAttachDetachSpec{
		VolumeId: cnstypes.CnsVolumeId{
			Id: volumeID,
		},
		Vm: vm.Reference(),
	}
	cnsDetachSpecList = append(cnsDetachSpecList, cnsDetachSpec)
	// Call the CNS DetachVolume
	task, err := m.virtualCenter.CnsClient.DetachVolume(ctx, cnsDetachSpecList)
	if err != nil {
		klog.Errorf("CNS DetachVolume failed from vCenter %q with err: %v", m.virtualCenter.Config.Host, err)
		return err
	}
	// Get the taskInfo
	taskInfo, err := cns.GetTaskInfo(ctx, task)
	if err != nil {
		klog.Errorf("Failed to get taskInfo for DetachVolume task from vCenter %q with err: %v", m.virtualCenter.Config.Host, err)
		return err
	}
	klog.V(2).Infof("DetachVolume: volumeID: %q, vm: %q, opId: %q", volumeID, vm.String(), taskInfo.ActivationId)
	// Get the task results for the given task
	taskResult, err := cns.GetTaskResult(ctx, taskInfo)
	if err != nil {
		klog.Errorf("unable to find the task result for DetachVolume task from vCenter %q with taskID %s and detachResults %v",
			m.virtualCenter.Config.Host, taskInfo.Task.Value, taskResult)
		return err
	}

	if taskResult == nil {
		klog.Errorf("taskResult is empty for DetachVolume task: %q, opId: %q", taskInfo.Task.Value, taskInfo.ActivationId)
		return errors.New("taskResult is empty")
	}

	volumeOperationRes := taskResult.GetCnsVolumeOperationResult()

	if volumeOperationRes.Fault != nil {
		// Volume is already attached to VM
		diskUUID, err := IsDiskAttached(ctx, vm, volumeID)
		if err != nil {
			klog.Errorf("DetachVolume: CNS Detach has failed with fault: %q. Unable to check if volume: %q is already detached from vm: %+v",
				spew.Sdump(volumeOperationRes.Fault), volumeID, vm)
			return err
		} else if diskUUID == "" {
			klog.Infof("DetachVolume: volumeID: %q not found on vm: %+v. Assuming volume is already detached",
				volumeID, vm)
			return nil
		} else {
			klog.Errorf("failed to detach cns volume:%q from node vm: %+v. fault: %q, opId: %q", volumeID, vm, spew.Sdump(volumeOperationRes.Fault), taskInfo.ActivationId)
			return errors.New(volumeOperationRes.Fault.LocalizedMessage)
		}
	}
	klog.V(2).Infof("DetachVolume: Volume detached successfully. volumeID: %q, vm: %q, opId: %q", volumeID, taskInfo.ActivationId, vm.String())
	return nil
}

// DeleteVolume deletes a volume given its spec.
func (m *defaultManager) DeleteVolume(volumeID string, deleteDisk bool) error {
	err := validateManager(m)
	if err != nil {
		return err
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	// Set up the VC connection
	err = m.virtualCenter.ConnectCns(ctx)
	if err != nil {
		klog.Errorf("ConnectCns failed with err: %+v", err)
		return err
	}
	// Construct the CNS VolumeId list
	var cnsVolumeIDList []cnstypes.CnsVolumeId
	cnsVolumeID := cnstypes.CnsVolumeId{
		Id: volumeID,
	}
	// Call the CNS DeleteVolume
	cnsVolumeIDList = append(cnsVolumeIDList, cnsVolumeID)
	task, err := m.virtualCenter.CnsClient.DeleteVolume(ctx, cnsVolumeIDList, deleteDisk)
	if err != nil {
		if soap.IsSoapFault(err) {
			soapFault := soap.ToSoapFault(err)
			if _, ok := soapFault.VimFault().(vimtypes.NotFound); ok {
				klog.V(2).Infof("VolumeID: %q, not found. Returning success for this operation since the volume is not present", volumeID)
				return nil
			}
		}
		klog.Errorf("CNS DeleteVolume failed from the  vCenter %q with err: %v", m.virtualCenter.Config.Host, err)
		return err
	}
	// Get the taskInfo
	taskInfo, err := cns.GetTaskInfo(ctx, task)
	if err != nil {
		klog.Errorf("Failed to get taskInfo for DeleteVolume task from vCenter %q with err: %v", m.virtualCenter.Config.Host, err)
		return err
	}
	klog.V(2).Infof("DeleteVolume: volumeID: %q, opId: %q", volumeID, taskInfo.ActivationId)
	// Get the task results for the given task
	taskResult, err := cns.GetTaskResult(ctx, taskInfo)
	if err != nil {
		klog.Errorf("unable to find the task result for DeleteVolume task from vCenter %q with taskID %s and deleteResults %v",
			m.virtualCenter.Config.Host, taskInfo.Task.Value, taskResult)
		return err
	}
	if taskResult == nil {
		klog.Errorf("taskResult is empty for DeleteVolume task: %q, opID: %q", taskInfo.Task.Value, taskInfo.ActivationId)
		return errors.New("taskResult is empty")
	}

	volumeOperationRes := taskResult.GetCnsVolumeOperationResult()
	if volumeOperationRes.Fault != nil {
		klog.Errorf("Failed to delete volume: %q, fault: %q, opID: %q", volumeID, spew.Sdump(volumeOperationRes.Fault), taskInfo.ActivationId)
		return errors.New(volumeOperationRes.Fault.LocalizedMessage)
	}
	klog.V(2).Infof("DeleteVolume: Volume deleted successfully. volumeID: %q, opId: %q", volumeID, taskInfo.ActivationId)
	return nil
}

// UpdateVolume updates a volume given its spec.
func (m *defaultManager) UpdateVolumeMetadata(spec *cnstypes.CnsVolumeMetadataUpdateSpec) error {
	err := validateManager(m)
	if err != nil {
		return err
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	// Set up the VC connection
	err = m.virtualCenter.ConnectCns(ctx)
	if err != nil {
		klog.Errorf("ConnectCns failed with err: %+v", err)
		return err
	}
	// If the VSphereUser in the VolumeMetadataUpdateSpec is different from session user, update the VolumeMetadataUpdateSpec
	s, err := m.virtualCenter.Client.SessionManager.UserSession(ctx)
	if err != nil {
		klog.Errorf("Failed to get usersession with err: %v", err)
		return err
	}
	if s.UserName != spec.Metadata.ContainerCluster.VSphereUser {
		klog.V(4).Infof("Update VSphereUser from %s to %s", spec.Metadata.ContainerCluster.VSphereUser, s.UserName)
		spec.Metadata.ContainerCluster.VSphereUser = s.UserName
	}

	var cnsUpdateSpecList []cnstypes.CnsVolumeMetadataUpdateSpec
	cnsUpdateSpec := cnstypes.CnsVolumeMetadataUpdateSpec{
		VolumeId: cnstypes.CnsVolumeId{
			Id: spec.VolumeId.Id,
		},
		Metadata: spec.Metadata,
	}
	cnsUpdateSpecList = append(cnsUpdateSpecList, cnsUpdateSpec)
	task, err := m.virtualCenter.CnsClient.UpdateVolumeMetadata(ctx, cnsUpdateSpecList)
	if err != nil {
		klog.Errorf("CNS UpdateVolume failed from vCenter %q with err: %v", m.virtualCenter.Config.Host, err)
		return err
	}
	// Get the taskInfo
	taskInfo, err := cns.GetTaskInfo(ctx, task)
	if err != nil {
		klog.Errorf("Failed to get taskInfo for UpdateVolume task from vCenter %q with err: %v", m.virtualCenter.Config.Host, err)
		return err
	}
	klog.V(2).Infof("UpdateVolumeMetadata: volumeID: %q, opId: %q", spec.VolumeId.Id, taskInfo.ActivationId)
	// Get the task results for the given task
	taskResult, err := cns.GetTaskResult(ctx, taskInfo)
	if err != nil {
		klog.Errorf("unable to find the task result for UpdateVolume task from vCenter %q with taskID %q, opId: %q and updateResults %+v",
			m.virtualCenter.Config.Host, taskInfo.Task.Value, taskInfo.ActivationId, taskResult)
		return err
	}

	if taskResult == nil {
		klog.Errorf("taskResult is empty for UpdateVolume task: %q, opId: %q", taskInfo.Task.Value, taskInfo.ActivationId)
		return errors.New("taskResult is empty")
	}
	volumeOperationRes := taskResult.GetCnsVolumeOperationResult()
	if volumeOperationRes.Fault != nil {
		klog.Errorf("Failed to update volume. updateSpec: %q, fault: %q, opID: %q", spew.Sdump(spec), spew.Sdump(volumeOperationRes.Fault), taskInfo.ActivationId)
		return errors.New(volumeOperationRes.Fault.LocalizedMessage)
	}
	klog.V(2).Infof("UpdateVolumeMetadata: Volume metadata updated successfully. volumeID: %q, opId: %q", spec.VolumeId.Id, taskInfo.ActivationId)
	return nil
}

// ExpandVolume expands a volume given its spec.
func (m *defaultManager) ExpandVolume(ctx context.Context, volumeID string, size int64) error {
	err := validateManager(m)
	if err != nil {
		klog.Errorf("validateManager failed with err: %+v", err)
		return err
	}
	// Set up the VC connection
	err = m.virtualCenter.ConnectCns(ctx)
	if err != nil {
		klog.Errorf("ConnectCns failed with err: %+v", err)
		return err
	}

	// Construct the CNS ExtendSpec list
	var cnsExtendSpecList []cnstypes.CnsVolumeExtendSpec
	cnsExtendSpec := cnstypes.CnsVolumeExtendSpec{
		VolumeId: cnstypes.CnsVolumeId{
			Id: volumeID,
		},
		CapacityInMb: size,
	}
	cnsExtendSpecList = append(cnsExtendSpecList, cnsExtendSpec)

	// Call the CNS ExtendVolume
	klog.V(2).Infof("Calling CnsClient.ExtendVolume: VolumeID [%q] Size [%d] cnsExtendSpecList [%#v]", volumeID, size, cnsExtendSpecList)
	task, err := m.virtualCenter.CnsClient.ExtendVolume(ctx, cnsExtendSpecList)

	if err != nil {
		if soap.IsSoapFault(err) {
			soapFault := soap.ToSoapFault(err)
			if _, ok := soapFault.VimFault().(vimtypes.NotFound); ok {
				klog.Errorf("VolumeID: %q, not found. Cannot expand volume.", volumeID)
				return errors.New("volume not found")
			}
		}
		klog.Errorf("CNS ExtendVolume failed from the vCenter %q with err: %v", m.virtualCenter.Config.Host, err)
		return err
	}
	// Get the taskInfo
	taskInfo, err := cns.GetTaskInfo(ctx, task)
	if err != nil {
		klog.Errorf("Failed to get taskInfo for ExtendVolume task from vCenter %q with err: %v", m.virtualCenter.Config.Host, err)
		return err
	}
	klog.V(2).Infof("ExpandVolume: volumeID: %q, opId: %q", volumeID, taskInfo.ActivationId)
	// Get the task results for the given task
	taskResult, err := cns.GetTaskResult(ctx, taskInfo)
	if err != nil {
		klog.Errorf("Unable to find the task result for ExtendVolume task from vCenter %q with taskID %s and extend volume Results %v",
			m.virtualCenter.Config.Host, taskInfo.Task.Value, taskResult)
		return err
	}
	if taskResult == nil {
		klog.Errorf("TaskResult is empty for ExtendVolume task: %q, opID: %q", taskInfo.Task.Value, taskInfo.ActivationId)
		return errors.New("taskResult is empty")
	}

	volumeOperationRes := taskResult.GetCnsVolumeOperationResult()
	if volumeOperationRes.Fault != nil {
		klog.Errorf("Failed to extend volume: %q, fault: %q, opID: %q", volumeID, spew.Sdump(volumeOperationRes.Fault), taskInfo.ActivationId)
		return errors.New(volumeOperationRes.Fault.LocalizedMessage)
	}
	klog.V(2).Infof("ExpandVolume: Volume expanded successfully. volumeID: %q, opId: %q", volumeID, taskInfo.ActivationId)

	return nil
}

// QueryVolume returns volumes matching the given filter.
func (m *defaultManager) QueryVolume(queryFilter cnstypes.CnsQueryFilter) (*cnstypes.CnsQueryResult, error) {
	err := validateManager(m)
	if err != nil {
		return nil, err
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	// Set up the VC connection
	err = m.virtualCenter.ConnectCns(ctx)
	if err != nil {
		klog.Errorf("ConnectCns failed with err: %+v", err)
		return nil, err
	}
	//Call the CNS QueryVolume
	res, err := m.virtualCenter.CnsClient.QueryVolume(ctx, queryFilter)
	if err != nil {
		klog.Errorf("CNS QueryVolume failed from vCenter %q with err: %v", m.virtualCenter.Config.Host, err)
		return nil, err
	}
	return res, err
}

// QueryAllVolume returns all volumes matching the given filter and selection.
func (m *defaultManager) QueryAllVolume(queryFilter cnstypes.CnsQueryFilter, querySelection cnstypes.CnsQuerySelection) (*cnstypes.CnsQueryResult, error) {
	err := validateManager(m)
	if err != nil {
		return nil, err
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	// Set up the VC connection
	err = m.virtualCenter.ConnectCns(ctx)
	if err != nil {
		klog.Errorf("ConnectCns failed with err: %+v", err)
		return nil, err
	}
	//Call the CNS QueryAllVolume
	res, err := m.virtualCenter.CnsClient.QueryAllVolume(ctx, queryFilter, querySelection)
	if err != nil {
		klog.Errorf("CNS QueryAllVolume failed from vCenter %q with err: %v", m.virtualCenter.Config.Host, err)
		return nil, err
	}
	return res, err
}
