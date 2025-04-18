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

package types

const (
	// CNSFinalizer is the finalizer on CNSNodeVmAttachment and CnsVolumeMetadata controllers
	CNSFinalizer = "cns.vmware.com"

	// CNSPvcFinalizer is the finalizer on Supervisor PVC managed by CNsNodeVMAttachment controller
	// to avoid Detach-Delete race which in-turn avoids ResourceInUse errors
	CNSPvcFinalizer = "cns.vmware.com/pvc-protection"

	// CNSVolumeFinalizer is the finalizer on Supervisor PVC created from Guest cluster
	// and associated with Guest cluster PVC,
	// This finalizer is added to avoid deletion of such PVCs directly from Supervisor.
	CNSVolumeFinalizer = "cns.vmware.com/pvc-delete-protection"

	// CNSSnapshotFinalizer is the finalizer on Supervisor VolumeSnapshot created from Guest cluster
	// and associated with Guest cluster VolumeSnapshot,
	// This finalizer is added to avoid deletion of such VolumeSnapshots directly from Supervisor.
	CNSSnapshotFinalizer = "cns.vmware.com/volumesnapshot-protection"

	// VSphereCSIDriverName is the vsphere CSI driver name
	VSphereCSIDriverName = "csi.vsphere.vmware.com"
)
