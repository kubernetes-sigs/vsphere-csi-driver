/*
Copyright 2025 The Kubernetes Authors.

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

package constants

// Config secret testuser credentials
const (
	ConfigSecretTestUser1Password = "VMware!23"
	ConfigSecretTestUser2Password = "VMware!234"
	ConfigSecretTestUser1         = "testuser1"
	ConfigSecretTestUser2         = "testuser2"
)

// Nimbus generated passwords
const (
	AdminUser      = "Administrator@vsphere.local"
	NimbusK8sVmPwd = "NIMBUS_K8S_VM_PWD"
	NimbusEsxPwd   = "ESX_PWD"
	NimbusVcPwd    = "VC_PWD"
	RootUser       = "root"
	VcUIPwd        = "VC_ADMIN_PWD"
)
