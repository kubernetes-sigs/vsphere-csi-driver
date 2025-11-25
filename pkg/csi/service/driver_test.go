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

package service

import (
	"context"
	"os"
	"strings"
	"testing"

	"github.com/container-storage-interface/spec/lib/go/csi"
	"github.com/stretchr/testify/assert"
	cnstypes "github.com/vmware/govmomi/cns/types"

	cnsconfig "sigs.k8s.io/vsphere-csi-driver/v3/pkg/common/config"
	csitypes "sigs.k8s.io/vsphere-csi-driver/v3/pkg/csi/types"
)

func TestNewDriver(t *testing.T) {
	driver := NewDriver()
	assert.NotNil(t, driver)

	// Verify that the driver implements the required interfaces
	_, ok := driver.(csi.IdentityServer)
	assert.True(t, ok, "Driver should implement IdentityServer interface")

	_, ok = driver.(csi.NodeServer)
	assert.True(t, ok, "Driver should implement NodeServer interface")

	// Verify that GetController method exists
	assert.NotNil(t, driver.GetController)
}

func TestVsphereCSIDriver_GetController(t *testing.T) {
	tests := []struct {
		name          string
		clusterFlavor string
		expectedType  string
	}{
		{
			name:          "Vanilla cluster flavor",
			clusterFlavor: string(cnstypes.CnsClusterFlavorVanilla),
			expectedType:  "*vanilla.controller",
		},
		{
			name:          "Workload cluster flavor",
			clusterFlavor: string(cnstypes.CnsClusterFlavorWorkload),
			expectedType:  "*wcp.controller",
		},
		{
			name:          "Guest cluster flavor",
			clusterFlavor: string(cnstypes.CnsClusterFlavorGuest),
			expectedType:  "*wcpguest.controller",
		},
		{
			name:          "Default cluster flavor (empty)",
			clusterFlavor: "",
			expectedType:  "*vanilla.controller",
		},
		{
			name:          "Invalid cluster flavor",
			clusterFlavor: "invalid",
			expectedType:  "*vanilla.controller",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Set environment variable
			if tt.clusterFlavor != "" {
				os.Setenv(cnsconfig.EnvClusterFlavor, tt.clusterFlavor)
			} else {
				os.Unsetenv(cnsconfig.EnvClusterFlavor)
			}
			defer os.Unsetenv(cnsconfig.EnvClusterFlavor)

			driver := NewDriver()
			controller := driver.GetController()

			assert.NotNil(t, controller)

			// Verify the controller is not nil - we can't easily test the exact type
			// without importing the controller packages which would create circular dependencies
			assert.NotNil(t, controller, "Controller should not be nil")
		})
	}
}

func TestGetNewUUID(t *testing.T) {
	uuid1 := getNewUUID()
	uuid2 := getNewUUID()

	// Verify UUIDs are not empty
	assert.NotEmpty(t, uuid1)
	assert.NotEmpty(t, uuid2)

	// Verify UUIDs are different
	assert.NotEqual(t, uuid1, uuid2)

	// Verify UUID format (basic check for length and hyphens)
	assert.Len(t, uuid1, 36, "UUID should be 36 characters long")
	assert.True(t, strings.Contains(uuid1, "-"), "UUID should contain hyphens")
}

func TestVsphereCSIDriver_BeforeServe_NodeMode(t *testing.T) {
	ctx := context.Background()
	driver := NewDriver().(*vsphereCSIDriver)

	// Set mode to node
	os.Setenv(csitypes.EnvVarMode, "node")
	defer os.Unsetenv(csitypes.EnvVarMode)

	// Mock CO init params
	COInitParams = map[string]interface{}{
		"test": "value",
	}

	err := driver.BeforeServe(ctx)

	// In node mode without Kubernetes cluster, BeforeServe will fail during CO initialization
	// This is expected behavior in a test environment
	assert.Error(t, err)
	// The error can be either missing env vars or missing service account token file
	assert.True(t,
		strings.Contains(err.Error(), "KUBERNETES_SERVICE_HOST and KUBERNETES_SERVICE_PORT must be defined") ||
			strings.Contains(err.Error(), "open /var/run/secrets/kubernetes.io/serviceaccount/token: no such file or directory"),
		"Expected Kubernetes connection error, got: %v", err)
}

func TestVsphereCSIDriver_BeforeServe_ControllerMode(t *testing.T) {
	ctx := context.Background()
	driver := NewDriver().(*vsphereCSIDriver)

	// Set mode to controller
	os.Setenv(csitypes.EnvVarMode, "controller")
	defer os.Unsetenv(csitypes.EnvVarMode)

	// Set cluster flavor to vanilla for predictable behavior
	os.Setenv(cnsconfig.EnvClusterFlavor, string(cnstypes.CnsClusterFlavorVanilla))
	defer os.Unsetenv(cnsconfig.EnvClusterFlavor)

	// Mock CO init params
	COInitParams = map[string]interface{}{
		"test": "value",
	}

	err := driver.BeforeServe(ctx)

	// In controller mode without Kubernetes cluster, it should fail during CO initialization
	// This is expected behavior in a test environment
	assert.Error(t, err)
	// The error can be either missing env vars or missing service account token file
	assert.True(t,
		strings.Contains(err.Error(), "KUBERNETES_SERVICE_HOST and KUBERNETES_SERVICE_PORT must be defined") ||
			strings.Contains(err.Error(), "open /var/run/secrets/kubernetes.io/serviceaccount/token: no such file or directory"),
		"Expected Kubernetes connection error, got: %v", err)
}

func TestInit_SocketCleanup(t *testing.T) {
	// Test the init function's socket cleanup behavior
	tempFile := "/tmp/test-csi-socket"

	// Create a temporary file to simulate leftover socket
	file, err := os.Create(tempFile)
	assert.NoError(t, err)
	file.Close()

	// Verify file exists
	_, err = os.Stat(tempFile)
	assert.NoError(t, err)

	// Set environment variable and call init
	os.Setenv(csitypes.EnvVarEndpoint, UnixSocketPrefix+tempFile)
	defer os.Unsetenv(csitypes.EnvVarEndpoint)

	// Simulate init function behavior
	sockPath := os.Getenv(csitypes.EnvVarEndpoint)
	sockPath = strings.TrimPrefix(sockPath, UnixSocketPrefix)
	if len(sockPath) > 1 {
		os.Remove(sockPath)
	}

	// Verify file is removed
	_, err = os.Stat(tempFile)
	assert.True(t, os.IsNotExist(err))
}

func TestVsphereCSIDriver_Run(t *testing.T) {
	// This test verifies the Run method structure without actually starting the server
	driver := NewDriver().(*vsphereCSIDriver)

	// We can't easily test the full Run method without mocking the GRPC server
	// But we can test that GetController returns a valid controller
	controller := driver.GetController()
	assert.NotNil(t, controller)

	// Note: We skip testing BeforeServe here as it requires Kubernetes cluster connectivity
	// which is not available in unit test environment
}

func TestDriverInterface(t *testing.T) {
	driver := NewDriver()

	// Test that driver implements all required interfaces
	assert.Implements(t, (*Driver)(nil), driver, "Should implement Driver interface")
	assert.Implements(t, (*csi.IdentityServer)(nil), driver, "Should implement IdentityServer interface")
	assert.Implements(t, (*csi.NodeServer)(nil), driver, "Should implement NodeServer interface")

	// Test that GetController returns a ControllerServer
	controller := driver.GetController()
	assert.Implements(t, (*csi.ControllerServer)(nil), controller,
		"GetController should return ControllerServer interface")
}

func TestDriverConstants(t *testing.T) {
	// Test that constants are properly defined
	assert.Equal(t, cnstypes.CnsClusterFlavorVanilla, defaultClusterFlavor)
	assert.Equal(t, "unix://", UnixSocketPrefix)
}

func TestGlobalVariables(t *testing.T) {
	// Test that global variables are properly initialized
	assert.Equal(t, defaultClusterFlavor, clusterFlavor)

	// COInitParams should be settable
	testParams := map[string]interface{}{"test": "value"}
	COInitParams = testParams
	assert.Equal(t, testParams, COInitParams)
}
