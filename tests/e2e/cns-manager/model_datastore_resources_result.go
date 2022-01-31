/*
Copyright 2021 VMware, Inc.

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
package cnsutils

type DatastoreResourcesResult struct {
	// Datacenter on which datastore resides.
	Datacenter string `json:"datacenter,omitempty"`
	// Datastore on which container volumes are being queried.
	Datastore string `json:"datastore,omitempty"`
	// The number of volumes on the datastore.
	TotalVolumes int64 `json:"totalVolumes,omitempty"`
	// Array of CNS volumes with the FCD id and vm attachment details.
	ContainerVolumes []VolumeDetails `json:"containerVolumes,omitempty"`
	// Array of non-CNS volumes with the FCD id and vm attachment details.
	OtherVolumes []VolumeDetails `json:"otherVolumes,omitempty"`
	// Array of virtual machines on the datastore.
	VirtualMachines []VmDetails `json:"virtualMachines,omitempty"`
}
