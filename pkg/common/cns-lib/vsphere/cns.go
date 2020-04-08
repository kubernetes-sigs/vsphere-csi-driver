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

package vsphere

import (
	"context"

	"github.com/vmware/govmomi/cns"
	"github.com/vmware/govmomi/vim25"
	"sigs.k8s.io/vsphere-csi-driver/v2/pkg/csi/service/logger"
)

// NewCnsClient creates a new CNS client
func NewCnsClient(ctx context.Context, c *vim25.Client) (*cns.Client, error) {
	log := logger.GetLogger(ctx)
	cnsClient, err := cns.NewClient(ctx, c)
	if err != nil {
		log.Errorf("failed to create a new client for CNS. err: %v", err)
		return nil, err
	}
	return cnsClient, nil
}

// ConnectCns creates a CNS client for the virtual center.
func (vc *VirtualCenter) ConnectCns(ctx context.Context) error {
	log := logger.GetLogger(ctx)
	var err = vc.Connect(ctx)
	if err != nil {
		log.Errorf("failed to connect to Virtual Center host %q with err: %v", vc.Config.Host, err)
		return err
	}
	if vc.CnsClient == nil {
		if vc.CnsClient, err = NewCnsClient(ctx, vc.Client.Client); err != nil {
			log.Errorf("failed to create CNS client on vCenter host %q with err: %v", vc.Config.Host, err)
			return err
		}
	}
	return nil
}

// DisconnectCns destroys the CNS client for the virtual center.
func (vc *VirtualCenter) DisconnectCns(ctx context.Context) {
	log := logger.GetLogger(ctx)
	if vc.CnsClient == nil {
		log.Info("CnsClient wasn't connected, ignoring")
	} else {
		vc.CnsClient = nil
	}
}
