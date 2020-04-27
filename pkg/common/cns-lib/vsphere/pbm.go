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

	"github.com/vmware/govmomi/pbm"
	"sigs.k8s.io/vsphere-csi-driver/pkg/csi/service/logger"
)

// ConnectPbm creates a PBM client for the virtual center.
func (vc *VirtualCenter) ConnectPbm(ctx context.Context) error {
	log := logger.GetLogger(ctx)
	var err = vc.Connect(ctx)
	if err != nil {
		log.Errorf("failed to connect to Virtual Center %q with err: %v", vc.Config.Host, err)
		return err
	}
	if vc.PbmClient == nil {
		if vc.PbmClient, err = pbm.NewClient(ctx, vc.Client.Client); err != nil {
			log.Errorf("failed to create pbm client with err: %v", err)
			return err
		}
	}
	return nil
}

// DisconnectPbm destroys the PBM client for the virtual center.
func (vc *VirtualCenter) DisconnectPbm(ctx context.Context) error {
	log := logger.GetLogger(ctx)
	if vc.PbmClient == nil {
		log.Info("PbmClient wasn't connected, ignoring")
	} else {
		vc.PbmClient = nil
	}
	return nil
}

// GetStoragePolicyIDByName gets storage policy ID by name.
func (vc *VirtualCenter) GetStoragePolicyIDByName(ctx context.Context, storagePolicyName string) (string, error) {
	log := logger.GetLogger(ctx)
	storagePolicyID, err := vc.PbmClient.ProfileIDByName(ctx, storagePolicyName)
	if err != nil {
		log.Errorf("failed to get StoragePolicyID from StoragePolicyName %s with err: %v", storagePolicyName, err)
		return "", err
	}
	return storagePolicyID, nil
}

// GetStoragePolicyNameByID gets storage policy name by ID.
func (vc *VirtualCenter) GetStoragePolicyNameByID(ctx context.Context, storagePolicyID string) (string, error) {
	log := logger.GetLogger(ctx)
	err := vc.ConnectPbm(ctx)
	if err != nil {
		log.Errorf("Error occurred while connecting to PBM, err: %+v", err)
		return "", err
	}
	storagePolicyName, err := vc.PbmClient.GetProfileNameByID(ctx, storagePolicyID)
	if err != nil {
		log.Errorf("Failed to get StoragePolicyName from StoragePolicyID %s with err: %+v", storagePolicyName, err)
		return "", err
	}
	return storagePolicyName, nil
}
