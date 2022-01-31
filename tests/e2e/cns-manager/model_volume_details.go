/*
Copyright 2021 VMware, Inc.

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
package cnsutils

type VolumeDetails struct {
	// ID of the FCD.
	FcdId string `json:"fcdId,omitempty"`
	// Name of the FCD.
	FcdName           string                `json:"fcdName,omitempty"`
	AttachmentDetails *FcdAttachmentDetails `json:"attachmentDetails,omitempty"`
	// Host owning the node vm to which the volume is attached.
	Host string `json:"host,omitempty"`
}
