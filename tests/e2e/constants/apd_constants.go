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
	NFS_ROUTE_COMMAND_IPV4 = "esxcfg-route"
	VSISH_SET              = "vsish -e set "
	INJECT_ERROR           = "/reliability/vmkstress/ScsiPathInjectError 1"
	CLEAR_ERROR            = "/reliability/vmkstress/ScsiPathInjectError 1"
	ERROR                  = "/injectError "
	STORAGE_PATH           = "/storage/scsifw/paths/"
	INJECT_APD_CODE        = "0x000100"
	CLEAR_APD_CODE         = "0x000000"
)
