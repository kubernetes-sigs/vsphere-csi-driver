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

package vsphere

import (
	"context"

	"github.com/vmware/govmomi/vim25"
	"github.com/vmware/govmomi/vslm"
	"sigs.k8s.io/vsphere-csi-driver/v2/pkg/csi/service/logger"
)

// NewVslmClient creates a new Vslm client
func NewVslmClient(ctx context.Context, c *vim25.Client) (*vslm.Client, error) {
	log := logger.GetLogger(ctx)
	vslmClient, err := vslm.NewClient(ctx, c)
	if err != nil {
		log.Errorf("failed to create a new client for Vslm. err: %v", err)
		return nil, err
	}
	return vslmClient, nil
}

// ConnectVslm creates a Vslm client for the virtual center.
func (vc *VirtualCenter) ConnectVslm(ctx context.Context) error {
	log := logger.GetLogger(ctx)
	var err = vc.Connect(ctx)
	if err != nil {
		log.Errorf("failed to connect to Virtual Center host %q with err: %v", vc.Config.Host, err)
		return err
	}
	if vc.VslmClient == nil {
		if vc.VslmClient, err = NewVslmClient(ctx, vc.Client.Client); err != nil {
			log.Errorf("failed to create Vslm client on vCenter host %q with err: %v", vc.Config.Host, err)
			return err
		}
	}
	return nil
}

// DisconnectVslm destroys the Vslm client for the virtual center.
func (vc *VirtualCenter) DisconnectVslm(ctx context.Context) {
	log := logger.GetLogger(ctx)
	if vc.VslmClient == nil {
		log.Info("VslmClient wasn't connected, ignoring")
	} else {
		vc.VslmClient = nil
	}
}
