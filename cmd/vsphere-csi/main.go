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

package main

import (
	"context"

	"github.com/rexray/gocsi"

	"k8s.io/cloud-provider-vsphere/pkg/csi/provider"
	"k8s.io/cloud-provider-vsphere/pkg/csi/service"
)

// main is ignored when this package is built as a go plug-in.
func main() {
	gocsi.Run(
		context.Background(),
		service.Name,
		"A CSI plugin for VMware vSphere storage",
		usage,
		provider.New())
}

const usage = `    X_CSI_VSPHERE_APINAME
        Specifies the name of the API to use when talking to vCenter

				The default value is "FCD" (First Class Disk)

    X_CSI_VSPHERE_CLOUD_CONFIG
        Specifies the path to the vsphere.conf file

        The default falue is "/etc/cloud/vsphere.conf"

    X_CSI_DISABLE_K8S_CLIENT
        Boolean flag that disables the Kubernetes API client to retrieve
        secrets.

        The default value is "false"
`
