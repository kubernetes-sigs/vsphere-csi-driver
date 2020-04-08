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

	"github.com/vmware/govmomi/vsan"
	"sigs.k8s.io/vsphere-csi-driver/v2/pkg/csi/service/logger"
)

// ConnectVsan creates a VSAN client for the virtual center.
func (vc *VirtualCenter) ConnectVsan(ctx context.Context) error {
	log := logger.GetLogger(ctx)
	var err = vc.Connect(ctx)
	if err != nil {
		log.Errorf("failed to connect to Virtual Center %q with err: %v", vc.Config.Host, err)
		return err
	}
	if vc.VsanClient == nil {
		if vc.VsanClient, err = vsan.NewClient(ctx, vc.Client.Client); err != nil {
			log.Errorf("failed to create vsan client with err: %v", err)
			return err
		}
	}
	return nil
}

// DisconnectVsan destroys the VSAN client for the virtual center.
func (vc *VirtualCenter) DisconnectVsan(ctx context.Context) error {
	log := logger.GetLogger(ctx)
	if vc.VsanClient == nil {
		log.Debug("VsanClient wasn't connected, ignoring disconnect request")
	} else {
		vc.VsanClient = nil
	}
	return nil
}
