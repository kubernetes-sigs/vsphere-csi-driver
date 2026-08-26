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
)

// TestGetVirtualCenterNormalizesMixedCaseHost verifies that GetVirtualCenter
// can retrieve a registered vCenter even when queried with a different case.
// This is critical for upgrade scenarios where old volume metadata may have
// mixed-case vCenter FQDNs but the registry stores normalized (lowercase) keys.
func TestGetVirtualCenterNormalizesMixedCaseHost(t *testing.T) {
	ctx := context.Background()

	// Setup: create a fresh manager and register a vCenter with lowercase host
	manager := &defaultVirtualCenterManager{virtualCenters: sync.Map{}}

	normalizedHost := "vc.example.com"
	mixedCaseHost := "VC.Example.COM"

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
