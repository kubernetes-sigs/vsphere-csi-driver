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
	// VirtualCenter instance for syncer
	vCenterInstance *cnsvsphere.VirtualCenter
	// Ensure vcenter is a singleton
	vCenterInitialized bool
	// vCenterInstanceLock is used for handling race conditions while initializing a vCenter instance
	vCenterInstanceLock = &sync.RWMutex{}
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
// Takes in a boolean paramater reloadConfig.
// If reloadConfig is true, the vcenter object is instantiated again and the old object becomes eligible for garbage collection.
// If reloadConfig is false and instance was already initialized, the previous instance is returned.
func GetVirtualCenterInstance(ctx context.Context, configTypes *ConfigInfo, reloadConfig bool) (*cnsvsphere.VirtualCenter, error) {
	log := logger.GetLogger(ctx)
	vCenterInstanceLock.Lock()
	defer vCenterInstanceLock.Unlock()

	if !vCenterInitialized || reloadConfig {
		log.Infof("Initializing new vCenterInstance.")

		var vcconfig *cnsvsphere.VirtualCenterConfig
		vcconfig, err := cnsvsphere.GetVirtualCenterConfig(ctx, configTypes.Cfg)
		if err != nil {
			log.Errorf("failed to get VirtualCenterConfig. Err: %+v", err)
			return nil, err
		}

		// Initialize the virtual center manager
		virtualcentermanager := cnsvsphere.GetVirtualCenterManager(ctx)

		//Unregister all VCs from virtual center manager
		if err = virtualcentermanager.UnregisterAllVirtualCenters(ctx); err != nil {
			log.Errorf("failed to unregister vcenter with virtualCenterManager.")
			return nil, err
		}

		// Register with virtual center manager
		vCenterInstance, err = virtualcentermanager.RegisterVirtualCenter(ctx, vcconfig)
		if err != nil {
			log.Errorf("failed to register VirtualCenter . Err: %+v", err)
			return nil, err
		}

		// Connect to VC
		err = vCenterInstance.Connect(ctx)
		if err != nil {
			log.Errorf("failed to connect to VirtualCenter host: %q. Err: %+v", vcconfig.Host, err)
			return nil, err
		}

		vCenterInitialized = true
		log.Info("vCenterInstance initialized")
	}
	return vCenterInstance, nil
}
