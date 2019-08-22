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
	"gitlab.eng.vmware.com/hatchway/govmomi/pbm"
	"k8s.io/klog"
)

// ConnectPbm creates a PBM client for the virtual center.
func (vc *VirtualCenter) ConnectPbm(ctx context.Context) error {
	var err = vc.Connect(ctx)
	if err != nil {
		klog.Errorf("Failed to connect to Virtual Center %q with err: %v", vc.Config.Host, err)
		return err
	}
	if vc.PbmClient == nil {
		if vc.PbmClient, err = pbm.NewClient(ctx, vc.Client.Client); err != nil {
			klog.Errorf("Failed to create pbm client with err: %v", err)
			return err
		}
	}
	return nil
}

// DisconnectPbm destroys the PBM client for the virtual center.
func (vc *VirtualCenter) DisconnectPbm(ctx context.Context) error {
	if vc.PbmClient == nil {
		klog.V(1).Info("PbmClient wasn't connected, ignoring")
	} else {
		vc.PbmClient = nil
	}
	return nil
}

// GetStoragePolicyIDByName gets storage policy ID by name.
func (vc *VirtualCenter) GetStoragePolicyIDByName(ctx context.Context, storagePolicyName string) (string, error) {
	storagePolicyID, err := vc.PbmClient.ProfileIDByName(ctx, storagePolicyName)
	if err != nil {
		klog.Errorf("Failed to get StoragePolicyID from StoragePolicyName %s with err: %v", storagePolicyName, err)
		return "", err
	}
	return storagePolicyID, nil
}
