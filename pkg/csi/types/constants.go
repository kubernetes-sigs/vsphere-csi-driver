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

const (
	// DefaultCloudConfigPath is /etc/cloud/vsphere.conf
	DefaultCloudConfigPath = "/etc/cloud/vsphere.conf"
)

const (
	// EnvAPI is the name of the API to use with vSphere
	EnvControllerType = "X_CSI_CONTROLLER_TYPE"

	// EnvCloudConfig contains the path to the vSphere Cloud Config
	EnvCloudConfig = "X_CSI_VSPHERE_CLOUD_CONFIG"
)
