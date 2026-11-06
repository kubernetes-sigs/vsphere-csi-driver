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

package vsphere

import (
	"context"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/vmware/govmomi"
	"github.com/vmware/govmomi/vim25"
	"github.com/vmware/govmomi/vim25/types"
	commontypes "sigs.k8s.io/vsphere-csi-driver/v3/pkg/common/types"
)

// TestGetVirtualCenterNormalizesMixedCaseHost verifies that GetVirtualCenter
// can retrieve a registered vCenter even when queried with a different case.
// This is critical for upgrade scenarios where old volume metadata may have
// mixed-case vCenter FQDNs but the registry stores normalized (lowercase) keys.
func TestGetVirtualCenterNormalizesMixedCaseHost(t *testing.T) {
	ctx := context.Background()

	// Setup: create a fresh manager and register a vCenter with lowercase host
	manager := &defaultVirtualCenterManager{virtualCenters: sync.Map{}}

	normalizedHost := commontypes.NewFQDN("vc.example.com")
	mixedCaseHost := commontypes.NewFQDN("VC.Example.COM")

	// Register with normalized host
	config := &VirtualCenterConfig{
		Host:     normalizedHost,
		Username: "admin@vsphere.local",
		Password: "password",
		Port:     443,
	}
	vc := &VirtualCenter{Config: config, ClientMutex: &sync.Mutex{}}
	manager.virtualCenters.Store(normalizedHost, vc)

	// Verify: querying with mixed case should find the same vCenter
	retrieved, err := manager.GetVirtualCenter(ctx, mixedCaseHost)
	assert.NoError(t, err, "GetVirtualCenter should not error for mixed-case host")
	assert.NotNil(t, retrieved, "GetVirtualCenter should return the registered vCenter")
	assert.Equal(t, normalizedHost, retrieved.Config.Host,
		"Retrieved vCenter should have the normalized host")
}

func TestIsCnsTransactionSupported(t *testing.T) {
	tests := []struct {
		name       string
		version    string
		apiVersion string
		want       bool
		wantErr    bool
	}{
		{name: "vCenter 9.1 with API 9.0", version: "9.1.0.0", apiVersion: "9.0.0.0"},
		{name: "older API", version: "9.1.0.0", apiVersion: "8.0.3.0"},
		{name: "API 9.1 with two components", version: "9.1.0.0", apiVersion: "9.1", want: true},
		{name: "API 9.1", version: "9.1.0.0", apiVersion: "9.1.0", want: true},
		{name: "API 9.1 with fourth component", version: "9.1.0.0", apiVersion: "9.1.0.0", want: true},
		{name: "newer minor API", version: "9.2.0.0", apiVersion: "9.2.0.0", want: true},
		{name: "multi-digit minor API", version: "9.10.0.0", apiVersion: "9.10.0.0", want: true},
		{name: "newer major API", version: "10.0.0.0", apiVersion: "10.0.0.0", want: true},
		{name: "product version is not used", version: "invalid", apiVersion: "9.1.0.0", want: true},
		{name: "missing API version", version: "9.1.0.0", wantErr: true},
		{name: "API version without minor", version: "9.1.0.0", apiVersion: "9", wantErr: true},
		{name: "invalid major API version", version: "9.1.0.0", apiVersion: "x.1.0.0", wantErr: true},
		{name: "invalid minor API version", version: "9.1.0.0", apiVersion: "9.x.0.0", wantErr: true},
		{name: "invalid patch API version", version: "9.1.0.0", apiVersion: "9.1.x.0", wantErr: true},
		{name: "minor version with suffix", version: "9.1.0.0", apiVersion: "9.1invalid", wantErr: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			manager := &defaultVirtualCenterManager{}
			host := commontypes.NewFQDN("vc.example.com")
			manager.virtualCenters.Store(host, &VirtualCenter{
				Client: &govmomi.Client{Client: &vim25.Client{
					ServiceContent: types.ServiceContent{About: types.AboutInfo{
						Version: tt.version, ApiVersion: tt.apiVersion,
					}},
				}},
			})

			got, err := manager.IsCnsTransactionSupported(context.Background(), host)
			if tt.wantErr {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
			}
			assert.Equal(t, tt.want, got)
		})
	}
}

func TestIsCnsTransactionSupportedUnregisteredVCenter(t *testing.T) {
	manager := &defaultVirtualCenterManager{}
	supported, err := manager.IsCnsTransactionSupported(context.Background(), commontypes.NewFQDN("missing-vc"))
	require.ErrorIs(t, err, ErrVCNotFound)
	assert.False(t, supported)
}
