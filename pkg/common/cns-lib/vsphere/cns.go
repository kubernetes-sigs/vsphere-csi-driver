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
	"gitlab.eng.vmware.com/hatchway/govmomi/cns"
	cnstypes "gitlab.eng.vmware.com/hatchway/govmomi/cns/types"
	"gitlab.eng.vmware.com/hatchway/govmomi/object"
	"gitlab.eng.vmware.com/hatchway/govmomi/vim25"
	"k8s.io/klog"
)

// NewCnsClient creates a new CNS client
func NewCnsClient(ctx context.Context, c *vim25.Client) (*cns.Client, error) {
	cnsClient, err := cns.NewClient(ctx, c)
	if err != nil {
		klog.Errorf("Failed to create a new client for CNS. err: %v", err)
		return nil, err
	}
	return cnsClient, nil
}

// ConnectCns creates a CNS client for the virtual center.
func (vc *VirtualCenter) ConnectCns(ctx context.Context) error {
	var err = vc.Connect(ctx)
	if err != nil {
		klog.Errorf("Failed to connect to Virtual Center host %q with err: %v", vc.Config.Host, err)
		return err
	}
	if vc.CnsClient == nil {
		if vc.CnsClient, err = NewCnsClient(ctx, vc.Client.Client); err != nil {
			klog.Errorf("Failed to create CNS client on vCenter host %q with err: %v", vc.Config.Host, err)
			return err
		}
	}
	return nil
}

// DisconnectCns destroys the CNS client for the virtual center.
func (vc *VirtualCenter) DisconnectCns(ctx context.Context) {
	if vc.CnsClient == nil {
		klog.V(1).Info("CnsClient wasn't connected, ignoring")
	} else {
		vc.CnsClient = nil
	}
}

// CreateVolume calls the CNS create API.
func (vc *VirtualCenter) CreateVolume(ctx context.Context, createSpecList []cnstypes.CnsVolumeCreateSpec) (*object.Task, error) {
	err := vc.ConnectCns(ctx)
	if err != nil {
		return nil, err
	}
	return vc.CnsClient.CreateVolume(ctx, createSpecList)
}

// UpdateVolumeMetadata calls the CNS CnsUpdateVolumeMetadata API with UpdateSpecs specified in the argument
func (vc *VirtualCenter) UpdateVolumeMetadata(ctx context.Context, updateSpecList []cnstypes.CnsVolumeMetadataUpdateSpec) (*object.Task, error) {
	err := vc.ConnectCns(ctx)
	if err != nil {
		return nil, err
	}
	return vc.CnsClient.UpdateVolumeMetadata(ctx, updateSpecList)
}

// DeleteVolume calls the CNS delete API.
func (vc *VirtualCenter) DeleteVolume(ctx context.Context, volumeIDList []cnstypes.CnsVolumeId, deleteDisk bool) (*object.Task, error) {
	err := vc.ConnectCns(ctx)
	if err != nil {
		return nil, err
	}
	return vc.CnsClient.DeleteVolume(ctx, volumeIDList, deleteDisk)
}

// AttachVolume calls the CNS Attach API.
func (vc *VirtualCenter) AttachVolume(ctx context.Context, attachSpecList []cnstypes.CnsVolumeAttachDetachSpec) (*object.Task, error) {
	err := vc.ConnectCns(ctx)
	if err != nil {
		return nil, err
	}
	return vc.CnsClient.AttachVolume(ctx, attachSpecList)
}

// DetachVolume calls the CNS Detach API.
func (vc *VirtualCenter) DetachVolume(ctx context.Context, detachSpecList []cnstypes.CnsVolumeAttachDetachSpec) (*object.Task, error) {
	err := vc.ConnectCns(ctx)
	if err != nil {
		return nil, err
	}
	return vc.CnsClient.DetachVolume(ctx, detachSpecList)
}

// QueryVolume calls the CNS QueryVolume API.
func (vc *VirtualCenter) QueryVolume(ctx context.Context, queryFilter cnstypes.CnsQueryFilter) (*cnstypes.CnsQueryResult, error) {
	err := vc.ConnectCns(ctx)
	if err != nil {
		return nil, err
	}
	return vc.CnsClient.QueryVolume(ctx, queryFilter)
}

// QueryVolume calls the CNS QueryAllVolume API.
func (vc *VirtualCenter) QueryAllVolume(ctx context.Context, queryFilter cnstypes.CnsQueryFilter, querySelection cnstypes.CnsQuerySelection) (*cnstypes.CnsQueryResult, error) {
	err := vc.ConnectCns(ctx)
	if err != nil {
		return nil, err
	}
	return vc.CnsClient.QueryAllVolume(ctx, queryFilter, querySelection)
}
