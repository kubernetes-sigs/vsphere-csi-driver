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

// TestIsvSANFileShareExtendSupported verifies that expanding a vSAN file share is reported as
// supported only from vCenter 9.0, and that an unknown host or a malformed version is an error.
func TestIsvSANFileShareExtendSupported(t *testing.T) {
	ctx := context.Background()
	manager := &defaultVirtualCenterManager{virtualCenters: sync.Map{}}

	registerVCenter := func(host, version string) commontypes.FQDN {
		fqdn := commontypes.NewFQDN(host)
		manager.virtualCenters.Store(fqdn, &VirtualCenter{
			Config: &VirtualCenterConfig{Host: fqdn},
			Client: &govmomi.Client{Client: &vim25.Client{
				ServiceContent: types.ServiceContent{About: types.AboutInfo{Version: version}},
			}},
		})
		return fqdn
	}

	tests := []struct {
		name    string
		host    commontypes.FQDN
		want    bool
		wantErr bool
	}{
		{name: "vCenter 7.0.3", host: registerVCenter("vc-703.example.com", "7.0.3"), want: false},
		{name: "vCenter 8.0.3", host: registerVCenter("vc-803.example.com", "8.0.3"), want: false},
		{name: "vCenter 9.0.0.0", host: registerVCenter("vc-900.example.com", "9.0.0.0"), want: true},
		{name: "vCenter 9.1.0", host: registerVCenter("vc-910.example.com", "9.1.0"), want: true},
		{name: "malformed version", host: registerVCenter("vc-bad.example.com", "unknown"), wantErr: true},
		{name: "unregistered vCenter", host: commontypes.NewFQDN("missing.example.com"), wantErr: true},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got, err := manager.IsvSANFileShareExtendSupported(ctx, tc.host)
			if tc.wantErr {
				assert.Error(t, err)
				assert.False(t, got)
				return
			}
			assert.NoError(t, err)
			assert.Equal(t, tc.want, got)
		})
	}
}
