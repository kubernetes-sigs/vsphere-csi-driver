package cnsfileaccessconfig

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/agiledragon/gomonkey/v2"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	vmoperatortypes "github.com/vmware-tanzu/vm-operator/api/v1alpha2"
	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes/scheme"
	"k8s.io/client-go/tools/record"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
	cnsoperatorapis "sigs.k8s.io/vsphere-csi-driver/v3/pkg/apis/cnsoperator"
	v1a1 "sigs.k8s.io/vsphere-csi-driver/v3/pkg/apis/cnsoperator/cnsfileaccessconfig/v1alpha1"
	commonconfig "sigs.k8s.io/vsphere-csi-driver/v3/pkg/common/config"
	"sigs.k8s.io/vsphere-csi-driver/v3/pkg/common/unittestcommon"
	"sigs.k8s.io/vsphere-csi-driver/v3/pkg/csi/service/common"
	"sigs.k8s.io/vsphere-csi-driver/v3/pkg/csi/service/common/commonco"
	"sigs.k8s.io/vsphere-csi-driver/v3/pkg/internalapis/cnsoperator/cnsfilevolumeclient"
	cnsoperatortypes "sigs.k8s.io/vsphere-csi-driver/v3/pkg/syncer/cnsoperator/types"
)

// fakeFileVolumeClient is a test double for cnsfilevolumeclient.FileVolumeClient.
// Its zero value ("", 0, nil everywhere) behaves like a fully no-op implementation.
// The *Calls counters let tests assert that no-op downstream calls are skipped
// once GetVMIPFromVMName reports there is nothing to clean up (empty vmIP).
type fakeFileVolumeClient struct {
	vmIP              string
	vmsAssociatedWith int
	vmIPErr           error

	getClientVMsFromIPListCalls   int
	removeClientVMFromIPListCalls int
}

func (f *fakeFileVolumeClient) GetClientVMsFromIPList(_ context.Context, _, _ string) ([]string, error) {
	f.getClientVMsFromIPListCalls++
	return nil, nil
}

func (f *fakeFileVolumeClient) AddClientVMToIPList(_ context.Context, _, _, _ string) error {
	return nil
}

func (f *fakeFileVolumeClient) RemoveClientVMFromIPList(_ context.Context, _, _, _ string) error {
	f.removeClientVMFromIPListCalls++
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
// returns nil immediately without making any further no-op calls (e.g. RemoveClientVMFromIPList),
// so the caller can proceed to remove the finalizer.
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
	assert.Zero(t, fake.removeClientVMFromIPListCalls,
		"RemoveClientVMFromIPList should be skipped once vmIP is reported empty")
}

// TestRemovePermissionsForFileVolume_EmptyVMIP_SkipsACLRemoval verifies that the empty-vmIP
// guard short-circuits before the ACL-removal branch is even evaluated. r.volumeManager is left
// nil here, so if the guard were removed and configureVolumeACLs were reached, this test would
// panic on a nil volumeManager instead of passing.
func TestRemovePermissionsForFileVolume_EmptyVMIP_SkipsACLRemoval(t *testing.T) {
	ctx := context.Background()
	initTestPackageState()

	// vmsAssociatedWith is set to 1 to match the condition that would otherwise trigger
	// ACL removal (vmsAssociatedWithIP == 1); the empty vmIP must still take precedence.
	fake := &fakeFileVolumeClient{vmIP: "", vmsAssociatedWith: 1, vmIPErr: nil}
	orig := getFileVolumeClientInstanceFn
	getFileVolumeClientInstanceFn = func(_ context.Context) (cnsfilevolumeclient.FileVolumeClient, error) {
		return fake, nil
	}
	t.Cleanup(func() { getFileVolumeClientInstanceFn = orig })

	r := &ReconcileCnsFileAccessConfig{} // volumeManager intentionally left nil
	instance := &v1a1.CnsFileAccessConfig{
		ObjectMeta: metav1.ObjectMeta{Name: "test-cfg", Namespace: "ns1"},
		Spec:       v1a1.CnsFileAccessConfigSpec{PvcName: "pvc1", VMName: "vm1"},
	}

	err := r.removePermissionsForFileVolume(ctx, "vol-1", instance, false /* skipConfigureVolumeACL */)

	assert.NoError(t, err)
	assert.Zero(t, fake.removeClientVMFromIPListCalls)
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

// TestReconcile_DeleteWithUnboundPVC_RemovesPvcProtectionFinalizer is the regression test for a
// vsan-file PVC getting stuck forever in Terminating: when a CnsFileAccessConfig CR referencing an
// unbound PVC (Spec.VolumeName == "") is deleted while its VM is still alive, the reconciler must
// still remove the cns.vmware.com/pvc-protection finalizer from the PVC, even though
// util.GetVolumeID fails with NotFound for the never-provisioned volume.
func TestReconcile_DeleteWithUnboundPVC_RemovesPvcProtectionFinalizer(t *testing.T) {
	ctx := context.Background()

	const (
		testNamespace = "test-ns"
		testPvcName   = "test-pvc"
		testVMName    = "test-vm"
		testInstance  = "test-cnsfileaccessconfig"
	)

	// Enable FileVolumesWithVmService so that a NotFound from util.GetVolumeID is treated as
	// "volume never provisioned" rather than a hard failure.
	fakeCOIf, err := unittestcommon.GetFakeContainerOrchestratorInterface(common.Kubernetes)
	require.NoError(t, err)
	fakeCO, ok := fakeCOIf.(*unittestcommon.FakeK8SOrchestrator)
	require.True(t, ok)
	origCO := commonco.ContainerOrchestratorUtility
	commonco.ContainerOrchestratorUtility = fakeCO
	defer func() { commonco.ContainerOrchestratorUtility = origCO }()
	require.NoError(t, fakeCO.EnableFSS(ctx, common.FileVolumesWithVmService))

	// PVC never bound to a PV, but it already carries the CNS PVC protection finalizer
	// stamped by addPvcFinalizer when the CnsFileAccessConfig was created.
	pvc := &v1.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{
			Name:       testPvcName,
			Namespace:  testNamespace,
			Finalizers: []string{cnsoperatortypes.CNSPvcFinalizer},
		},
	}

	vm := &vmoperatortypes.VirtualMachine{
		ObjectMeta: metav1.ObjectMeta{
			Name:      testVMName,
			Namespace: testNamespace,
		},
	}

	instance := &v1a1.CnsFileAccessConfig{
		ObjectMeta: metav1.ObjectMeta{
			Name:              testInstance,
			Namespace:         testNamespace,
			Finalizers:        []string{cnsoperatortypes.CNSFinalizer},
			DeletionTimestamp: &metav1.Time{Time: time.Now()},
		},
		Spec: v1a1.CnsFileAccessConfigSpec{
			PvcName: testPvcName,
			VMName:  testVMName,
		},
	}

	s := scheme.Scheme
	require.NoError(t, cnsoperatorapis.AddToScheme(s))
	require.NoError(t, vmoperatortypes.AddToScheme(s))

	fakeClient := fake.NewClientBuilder().
		WithScheme(s).
		WithStatusSubresource(instance).
		WithRuntimeObjects(instance, pvc).
		Build()

	vmClient := fake.NewClientBuilder().WithScheme(s).WithObjects(vm).Build()

	// isPvcInUse() resolves the CnsFileVolumeClient singleton, which otherwise tries to build a
	// real in-cluster kube client. Patch it to report "not in use" so removeFinalizerFromPVC
	// proceeds to actually strip the finalizer instead of erroring out.
	patches := gomonkey.ApplyFunc(cnsfilevolumeclient.GetFileVolumeClientInstance,
		func(ctx context.Context) (cnsfilevolumeclient.FileVolumeClient, error) {
			return &fakeFileVolumeClient{}, nil
		})
	defer patches.Reset()

	backOffDuration = make(map[types.NamespacedName]time.Duration)
	volumePermissionLockMap = &sync.Map{}

	r := &ReconcileCnsFileAccessConfig{
		client:           fakeClient,
		scheme:           s,
		configInfo:       &commonconfig.ConfigurationInfo{},
		vmOperatorClient: vmClient,
		recorder:         record.NewFakeRecorder(1024),
	}

	_, err = r.Reconcile(ctx, reconcile.Request{
		NamespacedName: types.NamespacedName{Name: testInstance, Namespace: testNamespace},
	})
	require.NoError(t, err)

	updatedPVC := &v1.PersistentVolumeClaim{}
	require.NoError(t, fakeClient.Get(ctx,
		types.NamespacedName{Name: testPvcName, Namespace: testNamespace}, updatedPVC))
	assert.False(t, controllerutil.ContainsFinalizer(updatedPVC, cnsoperatortypes.CNSPvcFinalizer),
		"expected cns.vmware.com/pvc-protection finalizer to be removed from the never-bound PVC")
}
