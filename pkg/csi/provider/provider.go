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

package provider

import (
	"github.com/rexray/gocsi"

	"sigs.k8s.io/vsphere-csi-driver/v2/pkg/csi/service"
)

// New returns a new CSI Storage Plug-in Provider.
func New() gocsi.StoragePluginProvider {
	svc := service.NewDriver()
	ctrl := svc.GetController()

	return &gocsi.StoragePlugin{
		Controller:  ctrl,
		Identity:    svc,
		Node:        svc,
		BeforeServe: svc.BeforeServe,

		EnvVars: []string{
			// Enable request validation.
			gocsi.EnvVarSpecReqValidation + "=true",

			// Enable serial volume access.
			gocsi.EnvVarSerialVolAccess + "=true",
		},
	}
}
