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
	volumes "sigs.k8s.io/vsphere-csi-driver/pkg/common/cns-lib/volume"
	"sigs.k8s.io/vsphere-csi-driver/pkg/syncer/types"
)

// AddToManagerFuncsForWorkloadFlavor is a list of functions to add all Controllers to the Manager
// for workload flavor cluster
var AddToManagerFuncsForWorkloadFlavor []func(manager.Manager, *types.ConfigInfo, volumes.Manager) error

// AddToManagerFuncsForVanillaFlavor is a list of functions to add all Controllers to the Manager
// for vanilla flavor cluster
var AddToManagerFuncsForVanillaFlavor []func(manager.Manager, *types.ConfigInfo, volumes.Manager) error

// AddToManager adds all Controllers to the Manager
func AddToManager(clusterFlavor cnstypes.CnsClusterFlavor, manager manager.Manager, configInfo *types.ConfigInfo, volumeManager volumes.Manager) error {
	if clusterFlavor == cnstypes.CnsClusterFlavorVanilla {
		for _, f := range AddToManagerFuncsForVanillaFlavor {
			if err := f(manager, configInfo, volumeManager); err != nil {
				return err
			}
		}
	} else if clusterFlavor == cnstypes.CnsClusterFlavorWorkload {
		for _, f := range AddToManagerFuncsForWorkloadFlavor {
			if err := f(manager, configInfo, volumeManager); err != nil {
				return err
			}
		}
	}
	return nil
}
