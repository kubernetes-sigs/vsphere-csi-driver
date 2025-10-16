/*
Copyright 2021 The Kubernetes Authors.

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
package fault

const (
	// CSITaskInfoEmptyFault is the fault type when taskInfo is empty.
	CSITaskInfoEmptyFault = "csi.fault.TaskInfoEmpty"

	// CSINonStorageFaultPrefix is the prefix used for faults originating due to components other than
	// downstream vSphere storage stack.
	CSINonStorageFaultPrefix = "csi.fault.nonstorage."
	// VimFaultPrefix is the prefix used for vim faults from downstream components.
	VimFaultPrefix = "vim.fault."
	// CSIVmUuidNotFoundFault is the fault type when Pod VMs do not have the vmware-system-vm-uuid annotation.
	CSIVmUuidNotFoundFault = "csi.fault.nonstorage.VmUuidNotFound"
	// CSIVmNotFoundFault is the fault type when VM object is not found in the VC
	CSIVmNotFoundFault = "csi.fault.nonstorage.VmNotFound"
	// CSIDiskNotDetachedFault is the fault type when disk is still attached to the vm
	CSIDiskNotDetachedFault = "csi.fault.nonstorage.DiskNotDetached"
	// CSIDatacenterNotFoundFault is the fault type when Datacenter are not found in the VC
	CSIDatacenterNotFoundFault = "csi.fault.DatacenterNotFound"
	// CSIVCenterNotFoundFault is the fault type when VC instance is not found
	CSIVCenterNotFoundFault = "csi.fault.VCenterNotFound"
	// CSIFindVmByUUIDFault is the fault type when FindByUUID method fails to find the VM
	CSIFindVmByUUIDFault = "csi.fault.FindVmByUUIDFault"
	// CSIFindVmByUUIDFault is the fault type when FindByUUID method fails to find the VM
	CSIBatchAttachFault = "csi.fault.BatchAttach"
	// CSIApiServerOperationFault is the fault type when Get(), List() and others fail on the API Server
	CSIApiServerOperationFault = "csi.fault.ApiServerOperation"

	// CSIResourceUpdateConflictFault is the fault type when Update() operatiton on the API Server
	// fails with the conflict error
	CSIResourceUpdateConflictFault = "csi.fault.nonstorage.ResourceUpdateConflict"

	// CSIPvNotFoundInPvcSpecFault is the fault type when PV name is not found in PVC Spec.
	// This can happen at the time of guest cluster creation when user specifies volumes to be created
	// in the guest cluster spec. Volume creation in such cases are typically initiated by
	// vmoperator and an error is observed because the volume name is updated in the VM spec, even
	// before the volume is provisioned in supervisor cluster.
	CSIPvNotFoundInPvcSpecFault = "csi.fault.nonstorage.PvNotFoundInPvcSpec"

	// CSIVSanFileServiceDisabledFault is the fault type when trying to create a RWX volume on a cluster which vsan file
	// service is disabled.
	CSIVSanFileServiceDisabledFault = "csi.fault.invalidconfig.VSanFileServiceDisabled"

	// CSITaskResultEmptyFault is the fault type when taskResult is empty.
	CSITaskResultEmptyFault = "csi.fault.TaskResultEmpty"

	// CSIInternalFault is the fault type returned when CSI internal error occurs.
	CSIInternalFault = "csi.fault.Internal"
	// CSINotFoundFault is the fault type returned when object required is not found.
	CSINotFoundFault = "csi.fault.NotFound"
	// CNSInvalidArgumentFault is the fault type returned when invalid argument is given.
	CSIInvalidArgumentFault = "csi.fault.InvalidArgument"
	// CSIUnimplementedFault is the fault type returned when the function is unimplemented.
	CSIUnimplementedFault = "csi.fault.Unimplemented"
	// CSIInvalidStoragePolicyConfigurationFault is the fault type returned when the user provides invalid storage policy.
	CSIInvalidStoragePolicyConfigurationFault = "csi.fault.invalidconfig.InvalidStoragePolicyConfiguration"

	// Below is the list of faults coming from downstream vCenter components that we want to classify
	// as non-storage faults.

	// VimFaultInvalidHostState is the fault returned from CNS when host is not in a state to perform the volume
	// operation e.g. maintenance mode.
	VimFaultInvalidHostState = VimFaultPrefix + "InvalidHostState"
	// VimFaultHostNotConnected is the fault returned from CNS when host is not connected.
	VimFaultHostNotConnected = VimFaultPrefix + "HostNotConnected"
	// VimFaultNotFound is the fault returned from CNS when the object is not found.
	VimFaultNotFound = VimFaultPrefix + "NotFound"
	// VimFaultInvalidState is the fault returned from CNS
	// when the operation is not valid in the current state of the object.
	VimFaultInvalidState = VimFaultPrefix + "InvalidState"
	// VimFaultInvalidDatastore is the fault returned from CNS
	// when the datastore is not valid for the operation being performed.
	VimFaultInvalidDatastore = VimFaultPrefix + "InvalidDatastore"
	// VimFaultTaskInProgress is the fault returned from CNS
	// when the task is already in progress for the object.
	VimFaultTaskInProgress = VimFaultPrefix + "TaskInProgress"
	// VimFaultInvalidArgument is the fault returned from CNS
	// when the argument provided is not valid for the operation being performed.
	VimFaultInvalidArgument = VimFaultPrefix + "InvalidArgument"
	// VimFaultCNSFault is the fault returned from CNS when a generic CNS fault occurs.
	VimFaultCNSFault = VimFaultPrefix + "CnsFault"
	// VimFaultNotSupported is the fault returned from CNS when the operation is not supported on the object.
	VimFaultNotSupported = VimFaultPrefix + "NotSupported"
)
