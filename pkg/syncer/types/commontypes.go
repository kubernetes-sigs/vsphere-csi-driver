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

	csictx "github.com/rexray/gocsi/context"
	"k8s.io/klog"
	cnsvsphere "sigs.k8s.io/vsphere-csi-driver/pkg/common/cns-lib/vsphere"
	cnsconfig "sigs.k8s.io/vsphere-csi-driver/pkg/common/config"
)

// ConfigInfo is a struct that used to capture config param details
type ConfigInfo struct {
	Cfg *cnsconfig.Config
}

// VirtualCenterTypes is a common struct used to capture VC information
type VirtualCenterTypes struct {
	Vcconfig             *cnsvsphere.VirtualCenterConfig
	Virtualcentermanager cnsvsphere.VirtualCenterManager
	Vcenter              *cnsvsphere.VirtualCenter
}

const (
	// VSphereCSIDriverName is the CSI driver name
	VSphereCSIDriverName = "block.vsphere.csi.vmware.com"
)

// InitConfigInfo initializes the ConfigInfo struct
func InitConfigInfo() (*ConfigInfo, error) {
	var err error
	configTypes := &ConfigInfo{}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	cfgPath := csictx.Getenv(ctx, cnsconfig.EnvCloudConfig)
	if cfgPath == "" {
		cfgPath = cnsconfig.DefaultCloudConfigPath
	}
	configTypes.Cfg, err = cnsconfig.GetCnsconfig(cfgPath)
	if err != nil {
		klog.Errorf("Failed to parse config. Err: %v", err)
		return nil, err
	}
	return configTypes, nil
}

// InitVirtualCenterTypes initializes the VirtualCenterTypes struct
func InitVirtualCenterTypes(configTypes *ConfigInfo) (*VirtualCenterTypes, error) {
	var err error
	vcTypes := &VirtualCenterTypes{}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	vcTypes.Vcconfig, err = cnsvsphere.GetVirtualCenterConfig(configTypes.Cfg)
	if err != nil {
		klog.Errorf("Failed to get VirtualCenterConfig. Err: %+v", err)
		return nil, err
	}

	// Initialize the virtual center manager
	vcTypes.Virtualcentermanager = cnsvsphere.GetVirtualCenterManager()

	// Register virtual center manager
	vcTypes.Vcenter, err = vcTypes.Virtualcentermanager.RegisterVirtualCenter(vcTypes.Vcconfig)
	if err != nil {
		klog.Errorf("Failed to register VirtualCenter . Err: %+v", err)
		return nil, err
	}

	// Connect to VC
	err = vcTypes.Vcenter.Connect(ctx)
	if err != nil {
		klog.Errorf("Failed to connect to VirtualCenter host: %q. Err: %+v", vcTypes.Vcconfig.Host, err)
		return nil, err
	}
	return vcTypes, nil
}
