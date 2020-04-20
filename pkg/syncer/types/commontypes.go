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
	cnsconfig "sigs.k8s.io/vsphere-csi-driver/pkg/common/config"
)

// Commontypes is a common struct that will used by all components in this package
type Commontypes struct {
	Cfg *cnsconfig.Config
}

const (
	// VSphereCSIDriverName is the CSI driver name
	VSphereCSIDriverName = "block.vsphere.csi.vmware.com"
)
