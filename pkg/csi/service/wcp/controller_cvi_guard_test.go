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

package wcp

import (
	"context"
	"testing"

	"github.com/container-storage-interface/spec/lib/go/csi"
	"github.com/stretchr/testify/assert"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	csivolumeinfov1alpha1 "sigs.k8s.io/vsphere-csi-driver/v3/pkg/apis/cnsoperator/csivolumeinfo/v1alpha1"
	"sigs.k8s.io/vsphere-csi-driver/v3/pkg/common/unittestcommon"
	"sigs.k8s.io/vsphere-csi-driver/v3/pkg/csi/service/common"
	"sigs.k8s.io/vsphere-csi-driver/v3/pkg/csi/service/common/commonco"
)

// fakeCVIService is a minimal in-memory CsiVolumeInfoService for WCP package tests.
type fakeCVIService struct {
	// cviByVolID maps volumeID to the CVI that will be returned by GetCsiVolumeInfo.
	cviByVolID map[string]*csivolumeinfov1alpha1.CsiVolumeInfo
}

func (f *fakeCVIService) CreateCsiVolumeInfo(
	_ context.Context, cvi *csivolumeinfov1alpha1.CsiVolumeInfo) error {
	f.cviByVolID[cvi.Spec.VolumeID] = cvi
	return nil
}

func (f *fakeCVIService) GetCsiVolumeInfo(
	_ context.Context, _, volumeID string) (*csivolumeinfov1alpha1.CsiVolumeInfo, error) {
	return f.cviByVolID[volumeID], nil
}

func (f *fakeCVIService) GetCsiVolumeInfoByDiskUUID(
	_ context.Context, _, _ string) (*csivolumeinfov1alpha1.CsiVolumeInfo, error) {
	return nil, nil
}

func (f *fakeCVIService) UpdateCsiVolumeInfoStatus(
	_ context.Context, _ *csivolumeinfov1alpha1.CsiVolumeInfo) error {
	return nil
}

func (f *fakeCVIService) PatchCsiVolumeInfo(_ context.Context, _, _ string, _ []byte) error {
	return nil
}

func (f *fakeCVIService) DeleteCsiVolumeInfo(_ context.Context, _, _ string) error {
	return nil
}

func (f *fakeCVIService) CsiVolumeInfoExists(_ context.Context, _, _ string) (bool, error) {
	return false, nil
}

func (f *fakeCVIService) GetCsiVolumeInfoByPVCName(
	_ context.Context, _, _ string) (*csivolumeinfov1alpha1.CsiVolumeInfo, error) {
	return nil, nil
}

func (f *fakeCVIService) AddCVIProtectionFinalizer(_ context.Context, _, _ string) error {
	return nil
}

func (f *fakeCVIService) RemoveCVIProtectionFinalizer(_ context.Context, _, _ string) error {
	return nil
}

// setupCVIGuardTest installs a FakeK8SOrchestrator and enables VMOwnedVolumes FSS.
// Returns a cleanup function that restores the original values.
func setupCVIGuardTest(t *testing.T) (c *controller, cleanup func()) {
	t.Helper()
	origCO := commonco.ContainerOrchestratorUtility
	origFSS := isVMOwnedVolumesFSSEnabled

	fakeOrch, err := unittestcommon.GetFakeContainerOrchestratorInterface(common.Kubernetes)
	if err != nil {
		t.Fatalf("failed to create fake orchestrator: %v", err)
	}
	if fssErr := fakeOrch.(*unittestcommon.FakeK8SOrchestrator).EnableFSS(
		context.Background(), "supports_vm_owned_volumes"); fssErr != nil {
		t.Fatalf("failed to enable VMOwnedVolumes FSS: %v", fssErr)
	}
	commonco.ContainerOrchestratorUtility = fakeOrch
	isVMOwnedVolumesFSSEnabled = true

	ctrl := &controller{
		cviService: &fakeCVIService{
			cviByVolID: make(map[string]*csivolumeinfov1alpha1.CsiVolumeInfo),
		},
	}

	return ctrl, func() {
		commonco.ContainerOrchestratorUtility = origCO
		isVMOwnedVolumesFSSEnabled = origFSS
	}
}

// makeCVIWithState returns a minimal CsiVolumeInfo with the given ownership state.
func makeCVIWithState(volumeID string,
	state csivolumeinfov1alpha1.OwnershipState,
) *csivolumeinfov1alpha1.CsiVolumeInfo {
	return &csivolumeinfov1alpha1.CsiVolumeInfo{
		Spec:   csivolumeinfov1alpha1.CsiVolumeInfoSpec{VolumeID: volumeID},
		Status: csivolumeinfov1alpha1.CsiVolumeInfoStatus{OwnershipState: state},
	}
}

// seedCVI places a CVI into the fakeCVIService for the given volumeID.
// The FakeK8SOrchestrator maps "mock-pvc-volume-<volumeID>" to namespace "test-ns".
func seedCVI(ctrl *controller, volumeID string, cvi *csivolumeinfov1alpha1.CsiVolumeInfo) {
	ctrl.cviService.(*fakeCVIService).cviByVolID[volumeID] = cvi
}

// -- ControllerExpandVolume CVI guard tests -----------------------------------

func TestExpandVolume_CVIGuard(t *testing.T) {
	tests := []struct {
		name        string
		volumeID    string
		cviState    *csivolumeinfov1alpha1.OwnershipState
		fssEnabled  bool
		wantBlocked bool
	}{
		{
			name:        "CSI_MANAGED proceeds",
			volumeID:    "vol-csimgd",
			cviState:    ptr(csivolumeinfov1alpha1.OwnershipStateCSIManaged),
			fssEnabled:  true,
			wantBlocked: false,
		},
		{
			name:        "VM_MANAGED is rejected",
			volumeID:    "vol-vmmgd",
			cviState:    ptr(csivolumeinfov1alpha1.OwnershipStateVMManaged),
			fssEnabled:  true,
			wantBlocked: true,
		},
		{
			name:        "TRANSFERRING_TO_VM is rejected",
			volumeID:    "vol-txtovm",
			cviState:    ptr(csivolumeinfov1alpha1.OwnershipStateTransferringToVM),
			fssEnabled:  true,
			wantBlocked: true,
		},
		{
			name:        "TRANSFERRING_TO_CSI is rejected",
			volumeID:    "vol-txtocsi",
			cviState:    ptr(csivolumeinfov1alpha1.OwnershipStateTransferringToCSI),
			fssEnabled:  true,
			wantBlocked: true,
		},
		{
			name:        "no CVI (brownfield) proceeds",
			volumeID:    "vol-brownfield",
			cviState:    nil,
			fssEnabled:  true,
			wantBlocked: false,
		},
		{
			name:        "FSS disabled proceeds regardless of CVI state",
			volumeID:    "vol-fssdisabled",
			cviState:    ptr(csivolumeinfov1alpha1.OwnershipStateVMManaged),
			fssEnabled:  false,
			wantBlocked: false,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			ctrl, cleanup := setupCVIGuardTest(t)
			defer cleanup()

			if !tc.fssEnabled {
				isVMOwnedVolumesFSSEnabled = false
			}
			if tc.cviState != nil {
				seedCVI(ctrl, tc.volumeID, makeCVIWithState(tc.volumeID, *tc.cviState))
			}

			// Call ControllerExpandVolume and recover from any panic. The test
			// infrastructure has no real VC, so code paths beyond the CVI guard
			// may panic or return unrelated errors. What we verify is whether the
			// guard itself produces a FailedPrecondition response.
			var (
				err        error
				didPanic   bool
				panicValue interface{}
			)
			func() {
				defer func() {
					if r := recover(); r != nil {
						didPanic = true
						panicValue = r
					}
				}()
				_, err = ctrl.ControllerExpandVolume(context.Background(),
					&csi.ControllerExpandVolumeRequest{
						VolumeId: tc.volumeID,
						CapacityRange: &csi.CapacityRange{
							RequiredBytes: 1024 * 1024 * 1024,
						},
					})
			}()

			if tc.wantBlocked {
				// Guard must fire before any panic-prone code.
				assert.False(t, didPanic, "expected guard to return before panic, panicValue=%v", panicValue)
				assert.Error(t, err)
				s, ok := status.FromError(err)
				assert.True(t, ok)
				assert.Equal(t, codes.FailedPrecondition, s.Code(),
					"expected FailedPrecondition, got %v", s.Code())
			} else {
				// Guard must NOT fire. If code panics afterwards, that is expected (no VC).
				// If it returns a gRPC error, that error must not be FailedPrecondition.
				if !didPanic && err != nil {
					s, ok := status.FromError(err)
					if ok {
						assert.NotEqual(t, codes.FailedPrecondition, s.Code(),
							"guard must not fire for CSI_MANAGED/brownfield/FSS-disabled")
					}
				}
			}
		})
	}
}

// -- CreateSnapshot CVI guard tests -------------------------------------------

func TestCreateSnapshot_CVIGuard(t *testing.T) {
	tests := []struct {
		name        string
		volumeID    string
		cviState    *csivolumeinfov1alpha1.OwnershipState
		fssEnabled  bool
		wantBlocked bool
	}{
		{
			name:        "CSI_MANAGED proceeds",
			volumeID:    "snap-vol-csimgd",
			cviState:    ptr(csivolumeinfov1alpha1.OwnershipStateCSIManaged),
			fssEnabled:  true,
			wantBlocked: false,
		},
		{
			name:        "VM_MANAGED is rejected with VirtualMachineSnapshot hint",
			volumeID:    "snap-vol-vmmgd",
			cviState:    ptr(csivolumeinfov1alpha1.OwnershipStateVMManaged),
			fssEnabled:  true,
			wantBlocked: true,
		},
		{
			name:        "TRANSFERRING_TO_VM is rejected",
			volumeID:    "snap-vol-txtovm",
			cviState:    ptr(csivolumeinfov1alpha1.OwnershipStateTransferringToVM),
			fssEnabled:  true,
			wantBlocked: true,
		},
		{
			name:        "TRANSFERRING_TO_CSI is rejected",
			volumeID:    "snap-vol-txtocsi",
			cviState:    ptr(csivolumeinfov1alpha1.OwnershipStateTransferringToCSI),
			fssEnabled:  true,
			wantBlocked: true,
		},
		{
			name:        "no CVI proceeds",
			volumeID:    "snap-vol-brownfield",
			cviState:    nil,
			fssEnabled:  true,
			wantBlocked: false,
		},
		{
			name:        "FSS disabled proceeds",
			volumeID:    "snap-vol-fssdisabled",
			cviState:    ptr(csivolumeinfov1alpha1.OwnershipStateVMManaged),
			fssEnabled:  false,
			wantBlocked: false,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			ctrl, cleanup := setupCVIGuardTest(t)
			defer cleanup()

			if !tc.fssEnabled {
				isVMOwnedVolumesFSSEnabled = false
			}
			if tc.cviState != nil {
				seedCVI(ctrl, tc.volumeID, makeCVIWithState(tc.volumeID, *tc.cviState))
			}

			// block-volume-snapshot FSS is already enabled by GetFakeContainerOrchestratorInterface.

			var (
				err        error
				didPanic   bool
				panicValue interface{}
			)
			func() {
				defer func() {
					if r := recover(); r != nil {
						didPanic = true
						panicValue = r
					}
				}()
				_, err = ctrl.CreateSnapshot(context.Background(),
					&csi.CreateSnapshotRequest{
						SourceVolumeId: tc.volumeID,
						Name:           "test-snapshot",
						Parameters: map[string]string{
							"csi.vsphere.volume-snapshot-namespace": "test-ns",
						},
					})
			}()

			if tc.wantBlocked {
				assert.False(t, didPanic, "expected guard to return before panic, panicValue=%v", panicValue)
				assert.Error(t, err)
				s, ok := status.FromError(err)
				assert.True(t, ok)
				assert.Equal(t, codes.FailedPrecondition, s.Code(),
					"expected FailedPrecondition, got %v", s.Code())
				assert.Contains(t, s.Message(), "VirtualMachineSnapshot")
			} else {
				if !didPanic && err != nil {
					s, ok := status.FromError(err)
					if ok {
						assert.NotEqual(t, codes.FailedPrecondition, s.Code(),
							"guard must not fire")
					}
				}
			}
		})
	}
}

func ptr[T any](v T) *T { return &v }
