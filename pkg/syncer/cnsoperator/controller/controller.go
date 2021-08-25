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

package controller

import (
	cnstypes "github.com/vmware/govmomi/cns/types"
	"sigs.k8s.io/controller-runtime/pkg/manager"
	volumes "sigs.k8s.io/vsphere-csi-driver/v2/pkg/common/cns-lib/volume"
	"sigs.k8s.io/vsphere-csi-driver/v2/pkg/common/config"
)

// AddToManagerFuncs is a list of functions to add all Controllers to the Manager
var AddToManagerFuncs []func(manager.Manager, cnstypes.CnsClusterFlavor,
	*config.ConfigurationInfo, volumes.Manager) error

// AddToManager adds all Controllers to the Manager
func AddToManager(manager manager.Manager, clusterFlavor cnstypes.CnsClusterFlavor,
	configInfo *config.ConfigurationInfo, volumeManager volumes.Manager) error {
	for _, f := range AddToManagerFuncs {
		if err := f(manager, clusterFlavor, configInfo, volumeManager); err != nil {
			return err
		}
	}
	return nil
}
