/*
Copyright 2018 The Kubernetes Authors.

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
	"github.com/container-storage-interface/spec/lib/go/csi"

	vcfg "k8s.io/cloud-provider-vsphere/pkg/common/config"
)

// Controller is the interface for the CSI Controller Server plus extra methods
// required to support multiple API backends
type Controller interface {
	csi.ControllerServer
	Init(config *vcfg.Config) error
}
