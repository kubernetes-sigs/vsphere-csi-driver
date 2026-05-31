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

package admissionhandler

import (
	"context"
	"errors"
	"runtime"
	"strings"
	"testing"

	"github.com/agiledragon/gomonkey/v2"

	csivolumeinfo "sigs.k8s.io/vsphere-csi-driver/v3/pkg/apis/cnsoperator/csivolumeinfo"
	csivolumeinfov1alpha1 "sigs.k8s.io/vsphere-csi-driver/v3/pkg/apis/cnsoperator/csivolumeinfo/v1alpha1"
)

// skipOnARMForMonkey skips gomonkey tests on ARM because gomonkey relies on
// instruction patching which is not reliable on Apple Silicon / ARM.
func skipOnARMForMonkey(t *testing.T) {
	t.Helper()
	if runtime.GOARCH == "arm64" {
		t.Skip("gomonkey instruction patching is not reliable on arm64; skipping")
	}
}

// fakeCVISvcForWebhook is a minimal CsiVolumeInfoService that returns a
// configured CVI (or error) for GetCsiVolumeInfoByPVCName.
type fakeCVISvcForWebhook struct {
	cvi *csivolumeinfov1alpha1.CsiVolumeInfo
	err error
}

func (f *fakeCVISvcForWebhook) GetCsiVolumeInfo(
	_ context.Context, _, _ string) (*csivolumeinfov1alpha1.CsiVolumeInfo, error) {
	return nil, nil
}
func (f *fakeCVISvcForWebhook) GetCsiVolumeInfoByDiskUUID(
	_ context.Context, _, _ string) (*csivolumeinfov1alpha1.CsiVolumeInfo, error) {
	return nil, nil
}
func (f *fakeCVISvcForWebhook) GetCsiVolumeInfoByPVCName(
	_ context.Context, _, _ string) (*csivolumeinfov1alpha1.CsiVolumeInfo, error) {
	return f.cvi, f.err
}
func (f *fakeCVISvcForWebhook) CreateCsiVolumeInfo(
	_ context.Context, _ *csivolumeinfov1alpha1.CsiVolumeInfo) error {
	return nil
}
func (f *fakeCVISvcForWebhook) UpdateCsiVolumeInfoStatus(
	_ context.Context, _ *csivolumeinfov1alpha1.CsiVolumeInfo) error {
	return nil
}
func (f *fakeCVISvcForWebhook) PatchCsiVolumeInfo(_ context.Context, _, _ string, _ []byte) error {
	return nil
}
func (f *fakeCVISvcForWebhook) DeleteCsiVolumeInfo(_ context.Context, _, _ string) error {
	return nil
}
func (f *fakeCVISvcForWebhook) CsiVolumeInfoExists(_ context.Context, _, _ string) (bool, error) {
	return false, nil
}
func (f *fakeCVISvcForWebhook) AddCVIProtectionFinalizer(_ context.Context, _, _ string) error {
	return nil
}
func (f *fakeCVISvcForWebhook) RemoveCVIProtectionFinalizer(_ context.Context, _, _ string) error {
	return nil
}

// TestCheckCVISnapshotRetained_SnapshotRetained verifies that a CVI in the
// snapshot-retained state (VM_MANAGED + vmName="") causes rejection.
func TestCheckCVISnapshotRetained_SnapshotRetained(t *testing.T) {
	skipOnARMForMonkey(t)

	cvi := &csivolumeinfov1alpha1.CsiVolumeInfo{}
	cvi.Status.OwnershipState = csivolumeinfov1alpha1.OwnershipStateVMManaged
	cvi.Status.VMName = ""

	fakeSvc := &fakeCVISvcForWebhook{cvi: cvi}
	patches := gomonkey.ApplyFunc(csivolumeinfo.InitCsiVolumeInfoService,
		func(_ context.Context) (csivolumeinfo.CsiVolumeInfoService, error) {
			return fakeSvc, nil
		})
	defer patches.Reset()

	denied, msg := checkCVISnapshotRetained(context.Background(), "ns", "pvc-retained")
	if !denied {
		t.Error("expected denial for snapshot-retained CVI")
	}
	if !strings.Contains(msg, "pvc-retained") {
		t.Errorf("expected message to contain PVC name, got: %q", msg)
	}
}

// TestCheckCVISnapshotRetained_CSIManaged verifies that a CSI_MANAGED CVI
// does not block deletion.
func TestCheckCVISnapshotRetained_CSIManaged(t *testing.T) {
	skipOnARMForMonkey(t)

	cvi := &csivolumeinfov1alpha1.CsiVolumeInfo{}
	cvi.Status.OwnershipState = csivolumeinfov1alpha1.OwnershipStateCSIManaged

	fakeSvc := &fakeCVISvcForWebhook{cvi: cvi}
	patches := gomonkey.ApplyFunc(csivolumeinfo.InitCsiVolumeInfoService,
		func(_ context.Context) (csivolumeinfo.CsiVolumeInfoService, error) {
			return fakeSvc, nil
		})
	defer patches.Reset()

	denied, _ := checkCVISnapshotRetained(context.Background(), "ns", "pvc-csi")
	if denied {
		t.Error("expected no denial for CSI_MANAGED CVI")
	}
}

// TestCheckCVISnapshotRetained_VMManaged_WithVMName verifies that VM_MANAGED
// with a non-empty vmName (disk is on a VM, not snapshot-retained) does not
// block deletion.
func TestCheckCVISnapshotRetained_VMManaged_WithVMName(t *testing.T) {
	skipOnARMForMonkey(t)

	cvi := &csivolumeinfov1alpha1.CsiVolumeInfo{}
	cvi.Status.OwnershipState = csivolumeinfov1alpha1.OwnershipStateVMManaged
	cvi.Status.VMName = "some-vm"

	fakeSvc := &fakeCVISvcForWebhook{cvi: cvi}
	patches := gomonkey.ApplyFunc(csivolumeinfo.InitCsiVolumeInfoService,
		func(_ context.Context) (csivolumeinfo.CsiVolumeInfoService, error) {
			return fakeSvc, nil
		})
	defer patches.Reset()

	denied, _ := checkCVISnapshotRetained(context.Background(), "ns", "pvc-vm-attached")
	if denied {
		t.Error("expected no denial when VM_MANAGED with vmName set (disk is on VM, not snapshot-retained)")
	}
}

// TestCheckCVISnapshotRetained_NoCVI verifies that a brownfield PVC (no CVI)
// is allowed.
func TestCheckCVISnapshotRetained_NoCVI(t *testing.T) {
	skipOnARMForMonkey(t)

	fakeSvc := &fakeCVISvcForWebhook{cvi: nil}
	patches := gomonkey.ApplyFunc(csivolumeinfo.InitCsiVolumeInfoService,
		func(_ context.Context) (csivolumeinfo.CsiVolumeInfoService, error) {
			return fakeSvc, nil
		})
	defer patches.Reset()

	denied, _ := checkCVISnapshotRetained(context.Background(), "ns", "pvc-brownfield")
	if denied {
		t.Error("expected no denial when no CVI exists (brownfield volume)")
	}
}

// TestCheckCVISnapshotRetained_ServiceInitFails verifies that a service
// initialization failure results in allowing the deletion (fail-open).
func TestCheckCVISnapshotRetained_ServiceInitFails(t *testing.T) {
	skipOnARMForMonkey(t)

	patches := gomonkey.ApplyFunc(csivolumeinfo.InitCsiVolumeInfoService,
		func(_ context.Context) (csivolumeinfo.CsiVolumeInfoService, error) {
			return nil, errors.New("api server unreachable")
		})
	defer patches.Reset()

	denied, _ := checkCVISnapshotRetained(context.Background(), "ns", "pvc-fail")
	if denied {
		t.Error("expected no denial when service init fails (fail-open)")
	}
}

// TestCheckCVISnapshotRetained_CVILookupFails verifies that a CVI lookup
// error allows deletion (fail-open).
func TestCheckCVISnapshotRetained_CVILookupFails(t *testing.T) {
	skipOnARMForMonkey(t)

	fakeSvc := &fakeCVISvcForWebhook{err: errors.New("lookup error")}
	patches := gomonkey.ApplyFunc(csivolumeinfo.InitCsiVolumeInfoService,
		func(_ context.Context) (csivolumeinfo.CsiVolumeInfoService, error) {
			return fakeSvc, nil
		})
	defer patches.Reset()

	denied, _ := checkCVISnapshotRetained(context.Background(), "ns", "pvc-lookup-fail")
	if denied {
		t.Error("expected no denial on CVI lookup error (fail-open)")
	}
}
