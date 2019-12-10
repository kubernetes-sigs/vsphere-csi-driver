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
	"gitlab.eng.vmware.com/hatchway/govmomi/vsan"
	"k8s.io/klog"
)

// ConnectVsan creates a VSAN client for the virtual center.
func (vc *VirtualCenter) ConnectVsan(ctx context.Context) error {
	var err = vc.Connect(ctx)
	if err != nil {
		klog.Errorf("Failed to connect to Virtual Center %q with err: %v", vc.Config.Host, err)
		return err
	}
	if vc.VsanClient == nil {
		if vc.VsanClient, err = vsan.NewClient(ctx, vc.Client.Client); err != nil {
			klog.Errorf("Failed to create vsan client with err: %v", err)
			return err
		}
	}
	return nil
}

// DisconnectVsan destroys the VSAN client for the virtual center.
func (vc *VirtualCenter) DisconnectVsan(ctx context.Context) error {
	if vc.VsanClient == nil {
		klog.V(4).Info("VsanClient wasn't connected, ignoring disconnect request")
	} else {
		vc.VsanClient = nil
	}
	return nil
}