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

	csictx "github.com/rexray/gocsi/context"
	cnstypes "gitlab.eng.vmware.com/hatchway/govmomi/cns/types"
	"k8s.io/klog"

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
	// error message from GetVirtualCenterInstance
	err error
)

// InitConfigInfo initializes the ConfigInfo struct
func InitConfigInfo(clusterFlavor cnstypes.CnsClusterFlavor) (*ConfigInfo, error) {
	var err error
	configTypes := &ConfigInfo{}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	if clusterFlavor == cnstypes.CnsClusterFlavorGuest {
		// Config path for Guest Cluster
		cfgPath := csictx.Getenv(ctx, cnsconfig.EnvGCConfig)
		if cfgPath == "" {
			cfgPath = cnsconfig.DefaultGCConfigPath
		}
		configTypes.Cfg, err = cnsconfig.GetGCconfig(cfgPath)
	} else {
		// Config path for SuperVisor and Vanilla Cluster
		cfgPath := csictx.Getenv(ctx, cnsconfig.EnvCloudConfig)
		if cfgPath == "" {
			cfgPath = cnsconfig.DefaultCloudConfigPath
		}
		configTypes.Cfg, err = cnsconfig.GetCnsconfig(cfgPath)
	}
	if err != nil {
		klog.Errorf("Failed to parse config. Err: %v", err)
		return nil, err
	}
	return configTypes, nil
}

// GetVirtualCenterInstance returns the vcenter object singleton.
// It is thread safe.
func GetVirtualCenterInstance(configTypes *ConfigInfo) (*cnsvsphere.VirtualCenter, error) {
	onceForVirtualCenter.Do(func() {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		var vcconfig *cnsvsphere.VirtualCenterConfig
		vcconfig, err = cnsvsphere.GetVirtualCenterConfig(configTypes.Cfg)
		if err != nil {
			klog.Errorf("Failed to get VirtualCenterConfig. Err: %+v", err)
			return
		}

		// Initialize the virtual center manager
		virtualcentermanager := cnsvsphere.GetVirtualCenterManager()

		// Register virtual center manager
		vcenter, err = virtualcentermanager.RegisterVirtualCenter(vcconfig)
		if err != nil {
			klog.Errorf("Failed to register VirtualCenter . Err: %+v", err)
			return
		}

		// Connect to VC
		err = vcenter.Connect(ctx)
		if err != nil {
			klog.Errorf("Failed to connect to VirtualCenter host: %q. Err: %+v", vcconfig.Host, err)
			return
		}
	})
	return vcenter, err
}
