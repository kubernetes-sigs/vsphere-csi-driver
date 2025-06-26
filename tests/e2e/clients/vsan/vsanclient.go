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
package vsan

import (
	"context"

	"github.com/vmware/govmomi/vim25"
	"github.com/vmware/govmomi/vim25/soap"
	"sigs.k8s.io/vsphere-csi-driver/v3/tests/e2e/constants"
)

// VsanClient struct holds vim and soap client
type VsanClient struct {
	Vim25Client   *vim25.Client
	ServiceClient *soap.Client
}

// newVsanHealthSvcClient returns vSANhealth client.
func NewVsanHealthSvcClient(ctx context.Context, client *vim25.Client) (*VsanClient, error) {
	sc := client.Client.NewServiceClient(constants.VsanHealthPath, constants.VsanNamespace)
	return &VsanClient{client, sc}, nil
}
