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

	// CSIVmUuidNotFoundFault is the fault type when Pod VMs do not have the vmware-system-vm-uuid annotation.
	CSIVmUuidNotFoundFault = "csi.fault.nonstorage.VmUuidNotFound"
	// CSIVmNotFoundFault is the fault type when VM object is not found in the VC
	CSIVmNotFoundFault = "csi.fault.nonstorage.VmNotFound"

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
)
