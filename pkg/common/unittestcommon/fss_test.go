/*
Copyright 2026 The Kubernetes Authors.

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

package unittestcommon

import (
	"context"
	"testing"

	"sigs.k8s.io/vsphere-csi-driver/v3/pkg/csi/service/common"
)

// TestVMOwnedVolumesFSSDefaultDisabledInFake verifies the FakeK8SOrchestrator
// defaults VMOwnedVolumes to false, ensuring existing tests are unaffected.
func TestVMOwnedVolumesFSSDefaultDisabledInFake(t *testing.T) {
	ctx := context.Background()
	co, err := GetFakeContainerOrchestratorInterface(common.Kubernetes)
	if err != nil {
		t.Fatalf("failed to get fake CO interface: %v", err)
	}
	if co.IsFSSEnabled(ctx, common.VMOwnedVolumes) {
		t.Errorf("VMOwnedVolumes should default to false in FakeK8SOrchestrator")
	}
}

// TestVMOwnedVolumesFSSEnableInFake verifies EnableFSS correctly enables
// VMOwnedVolumes in the FakeK8SOrchestrator.
func TestVMOwnedVolumesFSSEnableInFake(t *testing.T) {
	ctx := context.Background()
	co, err := GetFakeContainerOrchestratorInterface(common.Kubernetes)
	if err != nil {
		t.Fatalf("failed to get fake CO interface: %v", err)
	}

	if err := co.EnableFSS(ctx, common.VMOwnedVolumes); err != nil {
		t.Fatalf("EnableFSS returned unexpected error: %v", err)
	}
	if !co.IsFSSEnabled(ctx, common.VMOwnedVolumes) {
		t.Error("IsFSSEnabled should return true after EnableFSS")
	}
}

// TestVMOwnedVolumesFSSDisableInFake verifies DisableFSS correctly disables
// VMOwnedVolumes in the FakeK8SOrchestrator after it has been enabled.
func TestVMOwnedVolumesFSSDisableInFake(t *testing.T) {
	ctx := context.Background()
	co, err := GetFakeContainerOrchestratorInterface(common.Kubernetes)
	if err != nil {
		t.Fatalf("failed to get fake CO interface: %v", err)
	}

	if err := co.EnableFSS(ctx, common.VMOwnedVolumes); err != nil {
		t.Fatalf("EnableFSS returned unexpected error: %v", err)
	}
	if err := co.DisableFSS(ctx, common.VMOwnedVolumes); err != nil {
		t.Fatalf("DisableFSS returned unexpected error: %v", err)
	}
	if co.IsFSSEnabled(ctx, common.VMOwnedVolumes) {
		t.Error("IsFSSEnabled should return false after DisableFSS")
	}
}
