package cnsfileaccessconfig

import (
	"context"
	"fmt"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	vmoperatortypes "github.com/vmware-tanzu/vm-operator/api/v1alpha2"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	v1a1 "sigs.k8s.io/vsphere-csi-driver/v3/pkg/apis/cnsoperator/cnsfileaccessconfig/v1alpha1"
	"sigs.k8s.io/vsphere-csi-driver/v3/pkg/internalapis/cnsoperator/cnsfilevolumeclient"
)

// fakeFileVolumeClient is a test double for cnsfilevolumeclient.FileVolumeClient.
type fakeFileVolumeClient struct {
	vmIP              string
	vmsAssociatedWith int
	vmIPErr           error
}

func (f *fakeFileVolumeClient) GetClientVMsFromIPList(_ context.Context, _, _ string) ([]string, error) {
	return nil, nil
}
func (f *fakeFileVolumeClient) AddClientVMToIPList(_ context.Context, _, _, _ string) error {
	return nil
}
func (f *fakeFileVolumeClient) RemoveClientVMFromIPList(_ context.Context, _, _, _ string) error {
	return nil
}
func (f *fakeFileVolumeClient) GetVMIPFromVMName(_ context.Context, _, _ string) (string, int, error) {
	return f.vmIP, f.vmsAssociatedWith, f.vmIPErr
}
func (f *fakeFileVolumeClient) CnsFileVolumeClientExistsForPvc(_ context.Context, _ string) (bool, error) {
	return false, nil
}

// initTestPackageState initializes package-level state that is normally set up by Add().
// Must be called at the start of tests that exercise controller methods directly.
func initTestPackageState() {
	volumePermissionLockMap = &sync.Map{}
}

// TestRemovePermissionsForFileVolume_EmptyVMIP verifies that when GetVMIPFromVMName returns an
// empty IP (CnsFileVolumeClient absent or VM not registered), removePermissionsForFileVolume
// returns nil so the caller can proceed to remove the finalizer.
func TestRemovePermissionsForFileVolume_EmptyVMIP(t *testing.T) {
	ctx := context.Background()
	initTestPackageState()

	fake := &fakeFileVolumeClient{vmIP: "", vmsAssociatedWith: 0, vmIPErr: nil}
	orig := getFileVolumeClientInstanceFn
	getFileVolumeClientInstanceFn = func(_ context.Context) (cnsfilevolumeclient.FileVolumeClient, error) {
		return fake, nil
	}
	t.Cleanup(func() { getFileVolumeClientInstanceFn = orig })

	r := &ReconcileCnsFileAccessConfig{}
	instance := &v1a1.CnsFileAccessConfig{
		ObjectMeta: metav1.ObjectMeta{Name: "test-cfg", Namespace: "ns1"},
		Spec:       v1a1.CnsFileAccessConfigSpec{PvcName: "pvc1", VMName: "vm1"},
	}

	err := r.removePermissionsForFileVolume(ctx, "vol-1", instance, false)

	assert.NoError(t, err)
}

// TestRemovePermissionsForFileVolume_GetInstanceError verifies that when GetFileVolumeClientInstance
// itself fails, removePermissionsForFileVolume propagates the error.
func TestRemovePermissionsForFileVolume_GetInstanceError(t *testing.T) {
	ctx := context.Background()
	initTestPackageState()

	orig := getFileVolumeClientInstanceFn
	getFileVolumeClientInstanceFn = func(_ context.Context) (cnsfilevolumeclient.FileVolumeClient, error) {
		return nil, fmt.Errorf("kubeconfig unavailable")
	}
	t.Cleanup(func() { getFileVolumeClientInstanceFn = orig })

	r := &ReconcileCnsFileAccessConfig{}
	instance := &v1a1.CnsFileAccessConfig{
		ObjectMeta: metav1.ObjectMeta{Name: "test-cfg", Namespace: "ns1"},
		Spec:       v1a1.CnsFileAccessConfigSpec{PvcName: "pvc1", VMName: "vm1"},
	}

	err := r.removePermissionsForFileVolume(ctx, "vol-1", instance, false)

	assert.Error(t, err)
	assert.Contains(t, err.Error(), "Failed to get CNSFileVolumeClient instance")
}

func TestValidateVmAndPvc_NoLabels(t *testing.T) {
	ctx := context.Background()

	err := validateVmAndPvc(ctx, nil, "vm-1", "pvc-1", "ns-1", nil, &vmoperatortypes.VirtualMachine{})
	assert.NoError(t, err)
}

func TestValidateVmAndPvc_NotDevOpsUser(t *testing.T) {
	ctx := context.Background()

	labels := map[string]string{
		"other.label": "value",
	}

	err := validateVmAndPvc(ctx, labels, "vm-1", "pvc-1", "ns-1", nil, &vmoperatortypes.VirtualMachine{})
	assert.NoError(t, err)
}

func TestValidateVmAndPvc_DevOpsUser_ValidVM(t *testing.T) {
	ctx := context.Background()

	labels := map[string]string{
		devopsUserLabelKey: "true",
	}

	vm := &vmoperatortypes.VirtualMachine{
		ObjectMeta: metav1.ObjectMeta{
			Name:   "my-vm",
			Labels: map[string]string{"custom.label": "value"},
		},
	}

	err := validateVmAndPvc(ctx, labels, "vm-1", "pvc-1", "ns-1", nil, vm)
	assert.NoError(t, err)
}

func TestValidateVmAndPvc_DevOpsUser_InvalidVM(t *testing.T) {
	ctx := context.Background()

	labels := map[string]string{
		devopsUserLabelKey: "true",
	}

	vm := &vmoperatortypes.VirtualMachine{
		ObjectMeta: metav1.ObjectMeta{
			Name: "my-vm",
			Labels: map[string]string{
				capvVmLabelKey + "/machine": "value", // Contains capvVmLabelKey
			},
		},
	}

	err := validateVmAndPvc(ctx, labels, "vm-1", "pvc-1", "ns-1", nil, vm)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "Invalid combination")
	assert.Contains(t, err.Error(), "my-vm")
}
