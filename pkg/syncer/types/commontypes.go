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

import (
	"context"
	"sync"

	"sigs.k8s.io/vsphere-csi-driver/pkg/csi/service/common"
	"sigs.k8s.io/vsphere-csi-driver/pkg/csi/service/logger"

	cnsvsphere "sigs.k8s.io/vsphere-csi-driver/pkg/common/cns-lib/vsphere"
	cnsconfig "sigs.k8s.io/vsphere-csi-driver/pkg/common/config"
)

// ConfigInfo is a struct that used to capture config param details
type ConfigInfo struct {
	Cfg *cnsconfig.Config
}

var (
	// VirtualCenter object for syncer
	vcenter *cnsvsphere.VirtualCenter
	// Ensure vcenter is a singleton
	onceForVirtualCenter sync.Once
	// vcerror message from GetVirtualCenterInstance
	vcerror error
)

// InitConfigInfo initializes the ConfigInfo struct
func InitConfigInfo(ctx context.Context) (*ConfigInfo, error) {
	log := logger.GetLogger(ctx)
	cfg, err := common.GetConfig(ctx)
	if err != nil {
		log.Errorf("failed to read config. Error: %+v", err)
		return nil, err
	}
	configInfo := &ConfigInfo{
		cfg,
	}
	return configInfo, nil
}

// GetVirtualCenterInstance returns the vcenter object singleton.
// It is thread safe.
func GetVirtualCenterInstance(ctx context.Context, configTypes *ConfigInfo) (*cnsvsphere.VirtualCenter, error) {
	onceForVirtualCenter.Do(func() {
		log := logger.GetLogger(ctx)
		var vcconfig *cnsvsphere.VirtualCenterConfig
		vcconfig, vcerror := cnsvsphere.GetVirtualCenterConfig(ctx, configTypes.Cfg)
		if vcerror != nil {
			log.Errorf("failed to get VirtualCenterConfig. Err: %+v", vcerror)
			return
		}

		// Initialize the virtual center manager
		virtualcentermanager := cnsvsphere.GetVirtualCenterManager(ctx)

		// Register virtual center manager
		vcenter, vcerror = virtualcentermanager.RegisterVirtualCenter(ctx, vcconfig)
		if vcerror != nil {
			log.Errorf("failed to register VirtualCenter . Err: %+v", vcerror)
			return
		}

		// Connect to VC
		vcerror = vcenter.Connect(ctx)
		if vcerror != nil {
			log.Errorf("failed to connect to VirtualCenter host: %q. Err: %+v", vcconfig.Host, vcerror)
			return
		}
	})
	return vcenter, vcerror
}
