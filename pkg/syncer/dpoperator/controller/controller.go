/*
Copyright 2026 The Kubernetes Authors.

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

	cnsconfig "sigs.k8s.io/vsphere-csi-driver/v3/pkg/common/config"
)

// AddToManagerFunc registers a single DP operator controller with mgr. clusterFlavor and
// configInfo are passed through as-is; each registration func is responsible for deciding
// whether it applies to clusterFlavor and for building whatever dependencies (vCenter client,
// volume manager, informers, etc.) it needs from configInfo.
type AddToManagerFunc func(mgr manager.Manager, clusterFlavor cnstypes.CnsClusterFlavor,
	configInfo *cnsconfig.ConfigurationInfo) error

// AddToManagerFuncs is the list of registration funcs invoked by AddToManager.
var AddToManagerFuncs []AddToManagerFunc

// AddToManager adds all Controllers to the Manager.
func AddToManager(mgr manager.Manager, clusterFlavor cnstypes.CnsClusterFlavor,
	configInfo *cnsconfig.ConfigurationInfo) error {
	for _, f := range AddToManagerFuncs {
		if err := f(mgr, clusterFlavor, configInfo); err != nil {
			return err
		}
	}
	return nil
}
