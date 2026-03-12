/*
Copyright 2018 The Kubernetes Authors.

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

package service

import (
	"context"

	"github.com/container-storage-interface/spec/lib/go/csi"
	csitypes "sigs.k8s.io/vsphere-csi-driver/v3/pkg/csi/types"
)

// Version of the driver. This should be set via ldflags.
var Version string

func (driver *vsphereCSIDriver) Probe(
	ctx context.Context,
	req *csi.ProbeRequest) (
	*csi.ProbeResponse, error) {

	return &csi.ProbeResponse{}, nil
}

func (driver *vsphereCSIDriver) GetPluginInfo(
	ctx context.Context,
	req *csi.GetPluginInfoRequest) (
	*csi.GetPluginInfoResponse, error) {

	return &csi.GetPluginInfoResponse{
		Name:          csitypes.Name,
		VendorVersion: Version,
	}, nil
}

func (driver *vsphereCSIDriver) GetPluginCapabilities(
	ctx context.Context,
	req *csi.GetPluginCapabilitiesRequest) (
	*csi.GetPluginCapabilitiesResponse, error) {

	caps := []*csi.PluginCapability{
		{
			Type: &csi.PluginCapability_Service_{
				Service: &csi.PluginCapability_Service{
					Type: csi.PluginCapability_Service_CONTROLLER_SERVICE,
				},
			},
		},
		{
			Type: &csi.PluginCapability_Service_{
				Service: &csi.PluginCapability_Service{
					Type: csi.PluginCapability_Service_VOLUME_ACCESSIBILITY_CONSTRAINTS,
				},
			},
		},
		// Advertise SnapshotMetadata service for Changed Block Tracking(CBT) support
		// The SnapshotMetadata service provides GetMetadataAllocated and GetMetadataDelta RPCs
		// for efficient backup and restore operations (CSI spec v1.10.0+)
		{
			Type: &csi.PluginCapability_Service_{
				Service: &csi.PluginCapability_Service{
					Type: csi.PluginCapability_Service_SNAPSHOT_METADATA_SERVICE,
				},
			},
		},
	}

	rep := &csi.GetPluginCapabilitiesResponse{
		Capabilities: caps,
	}
	return rep, nil
}
