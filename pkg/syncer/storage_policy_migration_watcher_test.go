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

package syncer

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"

	v1 "k8s.io/api/core/v1"
	storagev1 "k8s.io/api/storage/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	k8stypes "k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/dynamic"
	dynamicfake "k8s.io/client-go/dynamic/fake"
	clientset "k8s.io/client-go/kubernetes"
	k8sfake "k8s.io/client-go/kubernetes/fake"
	"k8s.io/client-go/tools/cache"
	client "sigs.k8s.io/controller-runtime/pkg/client"
	ctrlclientfake "sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"

	volumes "sigs.k8s.io/vsphere-csi-driver/v3/pkg/common/cns-lib/volume"
	"sigs.k8s.io/vsphere-csi-driver/v3/pkg/common/unittestcommon"
	"sigs.k8s.io/vsphere-csi-driver/v3/pkg/csi/service/common"
	"sigs.k8s.io/vsphere-csi-driver/v3/pkg/csi/service/common/commonco"
	"sigs.k8s.io/vsphere-csi-driver/v3/pkg/internalapis/cnsvolumeinfo"
	cnsvolumeinfov1alpha1 "sigs.k8s.io/vsphere-csi-driver/v3/pkg/internalapis/cnsvolumeinfo/v1alpha1"
	k8stesting "sigs.k8s.io/vsphere-csi-driver/v3/pkg/kubernetes/testing"
)

// TestClassifyMigrationCR exercises the migration-CR -> outcome mapping that
// is the contract between the Mobility Operator (v1alpha4) and the CSI Syncer.
// Outcome is keyed off the Ready condition, and only when Ready is current:
// Ready.observedGeneration == metadata.generation and the CR spec targets the
// policy CSI is modifying to.
func TestClassifyMigrationCR(t *testing.T) {
	const (
		pvcName = "pvc-1"
		target  = "policy-uuid-gold"
		gen     = int64(3)
	)
	// ready builds a Ready condition observed at observedGeneration.
	ready := func(status, reason string, observedGeneration int64) map[string]interface{} {
		return map[string]interface{}{"type": "Ready", "status": status, "reason": reason,
			"observedGeneration": observedGeneration}
	}
	// volumeMigration builds a VolumeMigration CR at generation gen whose spec
	// targets specPolicy, with the given status conditions (nil -> no status).
	volumeMigration := func(specPolicy string, conditions ...interface{}) *unstructured.Unstructured {
		cr := &unstructured.Unstructured{Object: map[string]interface{}{
			"kind":     common.MigrationCRKindVolume,
			"metadata": map[string]interface{}{"name": "cr-1", "generation": gen},
			"spec":     map[string]interface{}{"storagePolicyID": specPolicy},
		}}
		if conditions != nil {
			cr.Object["status"] = map[string]interface{}{"conditions": conditions}
		}
		return cr
	}
	// vmInfraMigration builds a VirtualMachineInfraMigration CR at generation gen
	// whose spec targets specPolicy for pvc.
	vmInfraMigration := func(pvc, specPolicy string, conditions ...interface{}) *unstructured.Unstructured {
		return &unstructured.Unstructured{Object: map[string]interface{}{
			"kind":     common.MigrationCRKindVMInfra,
			"metadata": map[string]interface{}{"name": "cr-1", "generation": gen},
			"spec": map[string]interface{}{
				"targetStorage": map[string]interface{}{
					"volumes": []interface{}{
						map[string]interface{}{"pvcName": pvc, "storagePolicyID": specPolicy},
					},
				},
			},
			"status": map[string]interface{}{"conditions": conditions},
		}}
	}

	tests := []struct {
		name     string
		cr       *unstructured.Unstructured
		noTarget bool // pass "" as the expected policy
		want     migrationOutcome
	}{
		{name: "nil CR -> InProgress", cr: nil, want: migrationOutcomeInProgress},
		{name: "no status -> InProgress", cr: volumeMigration(target), want: migrationOutcomeInProgress},
		{
			name: "no Ready condition -> InProgress",
			cr: volumeMigration(target,
				map[string]interface{}{"type": "Validated", "status": "True", "reason": "Accepted"},
				map[string]interface{}{"type": "StorageInSync", "status": "True", "reason": "InSync"}),
			want: migrationOutcomeInProgress,
		},
		{
			name: "Ready=Unknown -> InProgress",
			cr:   volumeMigration(target, ready("Unknown", "Reconciling", gen)),
			want: migrationOutcomeInProgress,
		},

		// Table row: Ready=True, any reason -> Complete.
		{
			name: "Ready=True -> Complete",
			cr: volumeMigration(target,
				map[string]interface{}{"type": "Validated", "status": "True", "reason": "Accepted"},
				ready("True", "Succeeded", gen)),
			want: migrationOutcomeComplete,
		},
		{
			name: "Ready=True on VirtualMachineInfraMigration -> Complete",
			cr:   vmInfraMigration(pvcName, target, ready("True", "Succeeded", gen)),
			want: migrationOutcomeComplete,
		},

		// Table row: Ready=False with a terminal reason -> Infeasible.
		{
			name: "Ready=False/Infeasible -> Infeasible",
			cr:   volumeMigration(target, ready("False", "Infeasible", gen)),
			want: migrationOutcomeInfeasible,
		},
		{
			name: "Ready=False/NotSupported -> Infeasible",
			cr:   volumeMigration(target, ready("False", "NotSupported", gen)),
			want: migrationOutcomeInfeasible,
		},
		{
			name: "Ready=False/UnsupportedZoneChange -> Infeasible",
			cr:   volumeMigration(target, ready("False", "UnsupportedZoneChange", gen)),
			want: migrationOutcomeInfeasible,
		},

		// Table row: Ready=False with any other reason -> keep polling.
		{
			name: "Ready=False/InProgress -> InProgress",
			cr:   volumeMigration(target, ready("False", "InProgress", gen)),
			want: migrationOutcomeInProgress,
		},
		{
			name: "Ready=False/Internal -> InProgress",
			cr:   volumeMigration(target, ready("False", "Internal", gen)),
			want: migrationOutcomeInProgress,
		},
		{
			name: "Ready=False with no reason -> InProgress",
			cr:   volumeMigration(target, ready("False", "", gen)),
			want: migrationOutcomeInProgress,
		},

		// Stale Ready: observedGeneration behind metadata.generation.
		{
			name: "stale Ready=True (old observedGeneration) -> InProgress",
			cr:   volumeMigration(target, ready("True", "Succeeded", gen-1)),
			want: migrationOutcomeInProgress,
		},
		{
			name: "stale Ready=False/NotSupported (old observedGeneration) -> InProgress",
			cr:   volumeMigration(target, ready("False", "NotSupported", gen-1)),
			want: migrationOutcomeInProgress,
		},
		{
			name: "Ready without observedGeneration -> InProgress",
			cr: volumeMigration(target,
				map[string]interface{}{"type": "Ready", "status": "True", "reason": "Succeeded"}),
			want: migrationOutcomeInProgress,
		},

		// Stale Ready: CR spec targets a different policy (retarget/rollback not
		// yet applied to the CR, or applied but not yet reconciled).
		{
			name: "Ready=True for another policy on VolumeMigration -> InProgress",
			cr:   volumeMigration("policy-uuid-silver", ready("True", "Succeeded", gen)),
			want: migrationOutcomeInProgress,
		},
		{
			name: "Ready=False/Infeasible for another policy on VirtualMachineInfraMigration -> InProgress",
			cr:   vmInfraMigration(pvcName, "policy-uuid-silver", ready("False", "Infeasible", gen)),
			want: migrationOutcomeInProgress,
		},
		{
			name: "VirtualMachineInfraMigration without an entry for this PVC -> InProgress",
			cr:   vmInfraMigration("other-pvc", target, ready("True", "Succeeded", gen)),
			want: migrationOutcomeInProgress,
		},
		{
			name:     "unresolved expected policy -> InProgress",
			cr:       volumeMigration(target, ready("True", "Succeeded", gen)),
			noTarget: true,
			want:     migrationOutcomeInProgress,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			expected := target
			if tc.noTarget {
				expected = ""
			}
			got := classifyMigrationCR(tc.cr, pvcName, expected)
			if got != tc.want {
				t.Fatalf("classifyMigrationCR(%q) = %v, want %v", tc.name, got, tc.want)
			}
		})
	}
}

// TestLookupNewStoragePolicyID verifies the per-kind storage policy ID lookup
// from a migration CR (Mobility Operator v1alpha4).
// VirtualMachineInfraMigration uses spec.targetStorage.volumes[];
// VolumeMigration uses spec.storagePolicyID.
func TestLookupNewStoragePolicyID(t *testing.T) {
	// VirtualMachineInfraMigration CR: per-volume policy IDs under
	// spec.targetStorage.volumes[].
	vmInfraCR := &unstructured.Unstructured{Object: map[string]interface{}{
		"kind": "VirtualMachineInfraMigration",
		"spec": map[string]interface{}{
			"targetStorage": map[string]interface{}{
				"homeStoragePolicyID": "policy-uuid-vm-home",
				"volumes": []interface{}{
					map[string]interface{}{
						"pvcName":         "pvc-data-1",
						"storagePolicyID": "policy-uuid-silver",
					},
					map[string]interface{}{
						"pvcName":         "pvc-logs-2",
						"storagePolicyID": "policy-uuid-gold",
					},
				},
			},
		},
	}}

	t.Run("VirtualMachineInfraMigration", func(t *testing.T) {
		tests := []struct {
			name    string
			pvcName string
			want    string
		}{
			{name: "first match", pvcName: "pvc-data-1", want: "policy-uuid-silver"},
			{name: "second match", pvcName: "pvc-logs-2", want: "policy-uuid-gold"},
			{name: "no match", pvcName: "pvc-nope", want: ""},
			{name: "empty pvc name", pvcName: "", want: ""},
		}
		for _, tc := range tests {
			t.Run(tc.name, func(t *testing.T) {
				if got := lookupNewStoragePolicyID(vmInfraCR, tc.pvcName); got != tc.want {
					t.Fatalf("lookupNewStoragePolicyID(%q) = %q, want %q", tc.pvcName, got, tc.want)
				}
			})
		}

		t.Run("missing targetStorage", func(t *testing.T) {
			empty := &unstructured.Unstructured{Object: map[string]interface{}{
				"kind": "VirtualMachineInfraMigration",
				"spec": map[string]interface{}{},
			}}
			if got := lookupNewStoragePolicyID(empty, "pvc-data-1"); got != "" {
				t.Fatalf("lookupNewStoragePolicyID(no targetStorage) = %q, want empty", got)
			}
		})

		t.Run("targetStorage without volumes", func(t *testing.T) {
			homeOnly := &unstructured.Unstructured{Object: map[string]interface{}{
				"kind": "VirtualMachineInfraMigration",
				"spec": map[string]interface{}{
					"targetStorage": map[string]interface{}{
						"homeStoragePolicyID": "policy-uuid-vm-home",
					},
				},
			}}
			if got := lookupNewStoragePolicyID(homeOnly, "pvc-data-1"); got != "" {
				t.Fatalf("lookupNewStoragePolicyID(no volumes) = %q, want empty", got)
			}
		})

		t.Run("ignores legacy spec.volumes[]", func(t *testing.T) {
			// A pre-v1alpha4 shape must not match against v1alpha4 parsing.
			legacy := &unstructured.Unstructured{Object: map[string]interface{}{
				"kind": "VirtualMachineInfraMigration",
				"spec": map[string]interface{}{
					"volumes": []interface{}{
						map[string]interface{}{
							"pvcName":         "pvc-data-1",
							"storagePolicyID": "policy-uuid-silver",
						},
					},
				},
			}}
			if got := lookupNewStoragePolicyID(legacy, "pvc-data-1"); got != "" {
				t.Fatalf("lookupNewStoragePolicyID(legacy spec.volumes) = %q, want empty", got)
			}
		})
	})

	t.Run("VolumeMigration", func(t *testing.T) {
		// VolumeMigration CR: single top-level storagePolicyID.
		volMigrationCR := &unstructured.Unstructured{Object: map[string]interface{}{
			"kind": "VolumeMigration",
			"spec": map[string]interface{}{
				"storagePolicyID": "policy-uuid-gold",
			},
		}}
		// pvcName is ignored for VolumeMigration since the policy ID is at the
		// top of spec.
		if got := lookupNewStoragePolicyID(volMigrationCR, "any-pvc"); got != "policy-uuid-gold" {
			t.Fatalf("lookupNewStoragePolicyID(VolumeMigration) = %q, want %q", got, "policy-uuid-gold")
		}

		t.Run("missing storagePolicyID", func(t *testing.T) {
			empty := &unstructured.Unstructured{Object: map[string]interface{}{
				"kind": "VolumeMigration",
				"spec": map[string]interface{}{},
			}}
			if got := lookupNewStoragePolicyID(empty, "any-pvc"); got != "" {
				t.Fatalf("lookupNewStoragePolicyID(no storagePolicyID) = %q, want empty", got)
			}
		})
	})

	t.Run("nil CR", func(t *testing.T) {
		if got := lookupNewStoragePolicyID(nil, "pvc-data-1"); got != "" {
			t.Fatalf("lookupNewStoragePolicyID(nil) = %q, want empty", got)
		}
	})

	t.Run("unknown kind", func(t *testing.T) {
		cr := &unstructured.Unstructured{Object: map[string]interface{}{
			"kind": "MysteryMigration",
			"spec": map[string]interface{}{
				"storagePolicyID": "policy-uuid-gold",
			},
		}}
		if got := lookupNewStoragePolicyID(cr, "pvc-data-1"); got != "" {
			t.Fatalf("lookupNewStoragePolicyID(unknown kind) = %q, want empty", got)
		}
	})
}

// TestMigrationCRGVR ensures we map the supported migration CR kinds to the
// expected GVRs in the Mobility Operator API group.
func TestMigrationCRGVR(t *testing.T) {
	t.Run("VirtualMachineInfraMigration", func(t *testing.T) {
		gvr, err := migrationCRGVR("VirtualMachineInfraMigration")
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if gvr.Group != "mobility-operator.vmware.com" || gvr.Version != "v1alpha4" ||
			gvr.Resource != "virtualmachineinframigrations" {
			t.Fatalf("unexpected GVR: %+v", gvr)
		}
	})
	t.Run("VolumeMigration", func(t *testing.T) {
		gvr, err := migrationCRGVR("VolumeMigration")
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if gvr.Group != "mobility-operator.vmware.com" || gvr.Version != "v1alpha4" ||
			gvr.Resource != "volumemigrations" {
			t.Fatalf("unexpected GVR: %+v", gvr)
		}
	})
	t.Run("unsupported kind", func(t *testing.T) {
		if _, err := migrationCRGVR("MysteryMigration"); err == nil {
			t.Fatalf("expected error for unsupported kind, got nil")
		}
	})
}

// -----------------------------------------------------------------------------
// Test scaffolding (fakes and helpers) shared by the watcher / patch tests.
// -----------------------------------------------------------------------------

// resetMigrationStateForTest clears global maps and seam overrides so tests
// don't leak state to one another. Always call via t.Cleanup so even failing
// tests restore production defaults.
func resetMigrationStateForTest(t *testing.T) {
	t.Helper()
	activeMigrationsMu.Lock()
	for k, m := range activeMigrations {
		if m != nil && m.cancel != nil {
			m.cancel()
		}
		delete(activeMigrations, k)
	}
	activeMigrationsMu.Unlock()

	origCO := commonco.ContainerOrchestratorUtility
	origStart := migrationStartMigrationWatcher
	origK8s := migrationK8sNewClient
	origInit := migrationInitVolumeInfoService
	origGetter := migrationDynamicClientGetter
	origHandle := migrationHandleStoragePolicyChange
	origResolve := migrationResolveTargetPolicyID
	origInterval := migrationPollInterval
	origSvc := volumeInfoService
	origFSS := IsVMPVCStoragePolicyMutabilityEnabled

	t.Cleanup(func() {
		activeMigrationsMu.Lock()
		for k, m := range activeMigrations {
			if m != nil && m.cancel != nil {
				m.cancel()
			}
			delete(activeMigrations, k)
		}
		activeMigrationsMu.Unlock()

		commonco.ContainerOrchestratorUtility = origCO
		migrationStartMigrationWatcher = origStart
		migrationK8sNewClient = origK8s
		migrationInitVolumeInfoService = origInit
		migrationDynamicClientGetter = origGetter
		migrationHandleStoragePolicyChange = origHandle
		migrationResolveTargetPolicyID = origResolve
		migrationPollInterval = origInterval
		volumeInfoService = origSvc
		IsVMPVCStoragePolicyMutabilityEnabled = origFSS
	})
}

// enableMigrationFSS installs a FakeK8SOrchestrator with the
// VM_PVC_STORAGE_POLICY_MUTABILITY FSS turned on, and marks the feature as resolved-enabled.
// The migration watcher gates on IsVMPVCStoragePolicyMutabilityEnabled, which InitMetadataSyncer
// populates per cluster flavor at startup and which no unit test exercises, so it has to be set
// here too. resetMigrationStateForTest restores both.
func enableMigrationFSS(t *testing.T) {
	t.Helper()
	fakeCO, err := unittestcommon.GetFakeContainerOrchestratorInterface(common.Kubernetes)
	if err != nil {
		t.Fatalf("GetFakeContainerOrchestratorInterface failed: %v", err)
	}
	if err := fakeCO.EnableFSS(context.Background(), common.VMPVCStoragePolicyMutability); err != nil {
		t.Fatalf("EnableFSS failed: %v", err)
	}
	commonco.ContainerOrchestratorUtility = fakeCO
	IsVMPVCStoragePolicyMutabilityEnabled = true
}

// fakeVolumeInfoServiceImpl is a minimal cnsvolumeinfo.VolumeInfoService used
// by patch and propagation tests. Only the methods exercised by the migration
// watcher are functional; the rest return zero values / nil.
type fakeVolumeInfoServiceImpl struct {
	mu sync.Mutex

	// Test-provided fixtures.
	infoByID map[string]*cnsvolumeinfov1alpha1.CNSVolumeInfo

	// Errors to return on next call (consumed in order).
	getErr         error
	patchErr       error
	patchStatusErr error

	// Recorded calls.
	patchSpecCalls   []recordedPatch
	patchStatusCalls []recordedPatch
}

type recordedPatch struct {
	volumeID string
	body     []byte
	retries  int
}

func newFakeVolumeInfoService() *fakeVolumeInfoServiceImpl {
	return &fakeVolumeInfoServiceImpl{infoByID: map[string]*cnsvolumeinfov1alpha1.CNSVolumeInfo{}}
}

func (f *fakeVolumeInfoServiceImpl) GetvCenterForVolumeID(
	_ context.Context, _ string) (string, error) {
	return "", nil
}

func (f *fakeVolumeInfoServiceImpl) CreateVolumeInfo(
	_ context.Context, _ string, _ string) error {
	return nil
}

func (f *fakeVolumeInfoServiceImpl) CreateVolumeInfoWithPolicyInfo(
	_ context.Context, _, _, _, _, _ string, _ *resource.Quantity, _ bool) error {
	return nil
}

func (f *fakeVolumeInfoServiceImpl) DeleteVolumeInfo(_ context.Context, _ string) error {
	return nil
}

func (f *fakeVolumeInfoServiceImpl) ListAllVolumeInfos() []interface{} { return nil }

func (f *fakeVolumeInfoServiceImpl) Informer() cache.SharedIndexInformer { return nil }

func (f *fakeVolumeInfoServiceImpl) VolumeInfoCrExistsForVolume(
	_ context.Context, _ string) (bool, error) {
	return false, nil
}

func (f *fakeVolumeInfoServiceImpl) GetVolumeInfoForVolumeID(
	_ context.Context, volumeID string) (*cnsvolumeinfov1alpha1.CNSVolumeInfo, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.getErr != nil {
		return nil, f.getErr
	}
	if cvi, ok := f.infoByID[volumeID]; ok {
		out := cvi.DeepCopy()
		return out, nil
	}
	return nil, apierrors.NewNotFound(schema.GroupResource{Group: "cns.vmware.com", Resource: "cnsvolumeinfoes"},
		volumeID)
}

func (f *fakeVolumeInfoServiceImpl) PatchVolumeInfo(
	_ context.Context, volumeID string, patch []byte, retries int) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.patchSpecCalls = append(f.patchSpecCalls, recordedPatch{volumeID, append([]byte(nil), patch...), retries})
	return f.patchErr
}

func (f *fakeVolumeInfoServiceImpl) PatchVolumeInfoStatus(
	_ context.Context, volumeID string, patch []byte, retries int) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.patchStatusCalls = append(f.patchStatusCalls, recordedPatch{volumeID, append([]byte(nil), patch...), retries})
	return f.patchStatusErr
}

func (f *fakeVolumeInfoServiceImpl) UpdateVolumeInfoSnapshotDetails(_ context.Context, _, _ string,
	_ *volumes.CnsSnapshotInfo, _ volumes.Manager) error {
	return nil
}

// makePVC returns a bound PVC pointing at PV `pvName`, with optional VAC.
func makePVC(ns, name, pvName, storageClass, vacName string) *v1.PersistentVolumeClaim {
	pvc := &v1.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: ns},
		Spec: v1.PersistentVolumeClaimSpec{
			VolumeName:       pvName,
			StorageClassName: stringPtr(storageClass),
		},
	}
	if vacName != "" {
		pvc.Spec.VolumeAttributesClassName = stringPtr(vacName)
	}
	return pvc
}

func makePV(name, handle string) *v1.PersistentVolume {
	return &v1.PersistentVolume{
		ObjectMeta: metav1.ObjectMeta{Name: name},
		Spec: v1.PersistentVolumeSpec{
			PersistentVolumeSource: v1.PersistentVolumeSource{
				CSI: &v1.CSIPersistentVolumeSource{
					Driver:       "csi.vsphere.vmware.com",
					VolumeHandle: handle,
				},
			},
		},
	}
}

func stringPtr(s string) *string { return &s }

// newSyncerWithListers returns a metadataSyncInformer wired with informer
// listers seeded by the given PVCs and PVs. Caches sync before return so the
// listers are immediately queryable.
func newSyncerWithListers(
	t *testing.T, ctx context.Context, pvcs []*v1.PersistentVolumeClaim, pvs []*v1.PersistentVolume,
) (*metadataSyncInformer, clientset.Interface) {
	t.Helper()
	objs := []runtime.Object{}
	for _, p := range pvcs {
		objs = append(objs, p)
	}
	for _, p := range pvs {
		objs = append(objs, p)
	}
	client := k8sfake.NewClientset(objs...)
	im := k8stesting.NewInformerForTest(ctx, client)
	syncer := &metadataSyncInformer{
		pvLister:           im.GetPVLister(),
		pvcLister:          im.GetPVCLister(),
		k8sInformerManager: im,
	}
	stop := im.Listen()
	if stop == nil {
		t.Fatal("Listen() returned nil")
	}
	// Wait briefly for caches to populate.
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		ready := true
		for _, p := range pvcs {
			if _, err := im.GetPVCLister().PersistentVolumeClaims(p.Namespace).Get(p.Name); err != nil {
				ready = false
				break
			}
		}
		if ready {
			for _, p := range pvs {
				if _, err := im.GetPVLister().Get(p.Name); err != nil {
					ready = false
					break
				}
			}
		}
		if ready {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	return syncer, client
}

// cnsScheme returns a runtime.Scheme that includes CNSVolumeInfo types.
func cnsScheme() *runtime.Scheme {
	s := runtime.NewScheme()
	if err := cnsvolumeinfov1alpha1.AddToScheme(s); err != nil {
		panic("failed to add CNSVolumeInfo to scheme: " + err.Error())
	}
	return s
}

// makeFakeCNSClient builds a controller-runtime fake client pre-seeded with
// the supplied CNSVolumeInfo objects and configured to handle the status
// subresource separately (matching the real CRD annotation).
func makeFakeCNSClient(objs ...*cnsvolumeinfov1alpha1.CNSVolumeInfo) client.Client {
	s := cnsScheme()
	b := ctrlclientfake.NewClientBuilder().
		WithScheme(s).
		WithStatusSubresource(&cnsvolumeinfov1alpha1.CNSVolumeInfo{})
	for _, o := range objs {
		b = b.WithObjects(o)
	}
	return b.Build()
}

// makeFakeCNSClientWithInterceptor wraps the fake client with interceptor
// functions, useful for injecting errors into specific operations.
func makeFakeCNSClientWithInterceptor(
	funcs interceptor.Funcs,
	objs ...*cnsvolumeinfov1alpha1.CNSVolumeInfo,
) client.Client {
	s := cnsScheme()
	b := ctrlclientfake.NewClientBuilder().
		WithScheme(s).
		WithStatusSubresource(&cnsvolumeinfov1alpha1.CNSVolumeInfo{}).
		WithInterceptorFuncs(funcs)
	for _, o := range objs {
		b = b.WithObjects(o)
	}
	return b.Build()
}

// getCNSVolumeInfoFromClient fetches a CNSVolumeInfo by volumeID from a fake
// client.
func getCNSVolumeInfoFromClient(t *testing.T, c client.Client, volumeID string) *cnsvolumeinfov1alpha1.CNSVolumeInfo {
	t.Helper()
	obj := &cnsvolumeinfov1alpha1.CNSVolumeInfo{}
	key := k8stypes.NamespacedName{
		Name:      cnsvolumeinfo.GetCnsVolumeInfoCrName(volumeID),
		Namespace: common.GetCSINamespace(),
	}
	if err := c.Get(context.Background(), key, obj); err != nil {
		t.Fatalf("getCNSVolumeInfoFromClient: Get(%s) failed: %v", volumeID, err)
	}
	return obj
}

// -----------------------------------------------------------------------------
// Direct-function unit tests.
// -----------------------------------------------------------------------------

func TestRemoveMigrationWatcher(t *testing.T) {
	resetMigrationStateForTest(t)
	ctx := context.Background()

	// no-op when key absent (does not panic, doesn't change map).
	RemoveMigrationWatcher(ctx, "ns/none", "test")

	// real entry gets cancelled and deleted.
	_, cancel := context.WithCancel(ctx)
	cancelled := false
	wrappedCancel := func() {
		cancelled = true
		cancel()
	}
	activeMigrationsMu.Lock()
	activeMigrations["ns/pvc"] = &activeMigration{
		cancel: wrappedCancel,
		crKind: "VolumeMigration",
		crName: "pvc",
	}
	activeMigrationsMu.Unlock()

	RemoveMigrationWatcher(ctx, "ns/pvc", "test")

	if !cancelled {
		t.Fatal("expected cancel func to be invoked")
	}
	activeMigrationsMu.Lock()
	_, present := activeMigrations["ns/pvc"]
	activeMigrationsMu.Unlock()
	if present {
		t.Fatal("expected entry to be removed from activeMigrations")
	}
}

func TestHandlePvcDeletedForMigration(t *testing.T) {
	resetMigrationStateForTest(t)
	ctx := context.Background()

	// nil PVC is a no-op.
	handlePvcDeletedForMigration(ctx, nil)

	cancelled := false
	activeMigrationsMu.Lock()
	activeMigrations["ns/pvc"] = &activeMigration{
		cancel: func() { cancelled = true },
		crKind: "VolumeMigration",
		crName: "pvc",
	}
	activeMigrationsMu.Unlock()

	handlePvcDeletedForMigration(ctx, &v1.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{Namespace: "ns", Name: "pvc"},
	})

	if !cancelled {
		t.Fatal("expected cancel for matching active migration")
	}
}

func TestHandlePvcMigrationAnnotations(t *testing.T) {
	pvc := func(ns, name string, ann map[string]string) *v1.PersistentVolumeClaim {
		return &v1.PersistentVolumeClaim{
			ObjectMeta: metav1.ObjectMeta{Namespace: ns, Name: name, Annotations: ann},
		}
	}

	// handlePvcMigrationAnnotations must not depend on the orchestrator utility: it is invoked from
	// the PVC informer on both cluster flavors and gates purely on the resolved feature flag. A nil
	// orchestrator therefore has to be survivable, so that re-introducing a lookup here without a
	// nil guard fails loudly instead of panicking inside the informer callback.
	t.Run("does not dereference the orchestrator", func(t *testing.T) {
		resetMigrationStateForTest(t)
		enableMigrationFSS(t)
		commonco.ContainerOrchestratorUtility = nil
		called := false
		migrationStartMigrationWatcher = func(
			_ context.Context, _, _, _, _ string, _ *metadataSyncInformer) {
			called = true
		}
		handlePvcMigrationAnnotations(context.Background(), nil,
			pvc("ns", "pvc", map[string]string{
				common.AnnMigrationCRKind: common.MigrationCRKindVolume,
				common.AnnMigrationCRName: "cr",
			}), nil)
		if !called {
			t.Fatal("expected watcher to start: the feature is enabled and the orchestrator is unused")
		}
	})

	t.Run("FSS disabled short-circuits", func(t *testing.T) {
		resetMigrationStateForTest(t)
		fakeCO, _ := unittestcommon.GetFakeContainerOrchestratorInterface(common.Kubernetes)
		// FSS not enabled by default.
		commonco.ContainerOrchestratorUtility = fakeCO
		called := false
		migrationStartMigrationWatcher = func(
			_ context.Context, _, _, _, _ string, _ *metadataSyncInformer) {
			called = true
		}
		handlePvcMigrationAnnotations(context.Background(), nil,
			pvc("ns", "pvc", map[string]string{
				common.AnnMigrationCRKind: common.MigrationCRKindVolume,
				common.AnnMigrationCRName: "cr",
			}), nil)
		if called {
			t.Fatal("startMigrationWatcher should not be called when FSS is disabled")
		}
	})

	t.Run("no annotations and no old annotations: no-op", func(t *testing.T) {
		resetMigrationStateForTest(t)
		enableMigrationFSS(t)
		called := false
		migrationStartMigrationWatcher = func(
			_ context.Context, _, _, _, _ string, _ *metadataSyncInformer) {
			called = true
		}
		handlePvcMigrationAnnotations(context.Background(),
			pvc("ns", "pvc", nil),
			pvc("ns", "pvc", nil),
			nil)
		if called {
			t.Fatal("startMigrationWatcher should not be called with no annotations")
		}
	})

	t.Run("annotations removed: cancels existing watcher", func(t *testing.T) {
		resetMigrationStateForTest(t)
		enableMigrationFSS(t)
		cancelled := false
		activeMigrationsMu.Lock()
		activeMigrations["ns/pvc"] = &activeMigration{
			cancel: func() { cancelled = true }, crKind: "VolumeMigration", crName: "old",
		}
		activeMigrationsMu.Unlock()

		handlePvcMigrationAnnotations(context.Background(),
			pvc("ns", "pvc", map[string]string{
				common.AnnMigrationCRKind: common.MigrationCRKindVolume,
				common.AnnMigrationCRName: "old",
			}),
			pvc("ns", "pvc", nil),
			nil)
		if !cancelled {
			t.Fatal("expected cancel of removed-annotation watcher")
		}
	})

	t.Run("unsupported kind: warning, no start", func(t *testing.T) {
		resetMigrationStateForTest(t)
		enableMigrationFSS(t)
		called := false
		migrationStartMigrationWatcher = func(
			_ context.Context, _, _, _, _ string, _ *metadataSyncInformer) {
			called = true
		}
		handlePvcMigrationAnnotations(context.Background(), nil,
			pvc("ns", "pvc", map[string]string{
				common.AnnMigrationCRKind: "MysteryMigration",
				common.AnnMigrationCRName: "cr",
			}), nil)
		if called {
			t.Fatal("startMigrationWatcher should not be called for unsupported kind")
		}
	})

	t.Run("same kind/name as existing watcher: no-op", func(t *testing.T) {
		resetMigrationStateForTest(t)
		enableMigrationFSS(t)
		called := false
		migrationStartMigrationWatcher = func(
			_ context.Context, _, _, _, _ string, _ *metadataSyncInformer) {
			called = true
		}
		activeMigrationsMu.Lock()
		activeMigrations["ns/pvc"] = &activeMigration{
			cancel: func() {}, crKind: common.MigrationCRKindVolume, crName: "cr",
		}
		activeMigrationsMu.Unlock()

		handlePvcMigrationAnnotations(context.Background(), nil,
			pvc("ns", "pvc", map[string]string{
				common.AnnMigrationCRKind: common.MigrationCRKindVolume,
				common.AnnMigrationCRName: "cr",
			}), nil)
		if called {
			t.Fatal("startMigrationWatcher should not be called when watcher is already active for same CR")
		}
	})

	t.Run("annotation kind/name change: cancels old, starts new", func(t *testing.T) {
		resetMigrationStateForTest(t)
		enableMigrationFSS(t)
		cancelled := false
		started := false
		activeMigrationsMu.Lock()
		activeMigrations["ns/pvc"] = &activeMigration{
			cancel: func() { cancelled = true }, crKind: common.MigrationCRKindVolume, crName: "old",
		}
		activeMigrationsMu.Unlock()
		migrationStartMigrationWatcher = func(
			_ context.Context, ns, name, kind, crName string, _ *metadataSyncInformer) {
			started = true
			if ns != "ns" || name != "pvc" || kind != common.MigrationCRKindVolume || crName != "new" {
				t.Errorf("unexpected start args: %s/%s on %s/%s", ns, name, kind, crName)
			}
		}
		handlePvcMigrationAnnotations(context.Background(), nil,
			pvc("ns", "pvc", map[string]string{
				common.AnnMigrationCRKind: common.MigrationCRKindVolume,
				common.AnnMigrationCRName: "new",
			}), nil)
		if !cancelled {
			t.Fatal("expected old watcher to be cancelled")
		}
		if !started {
			t.Fatal("expected new watcher to be started")
		}
	})

	t.Run("fresh annotations: starts watcher", func(t *testing.T) {
		resetMigrationStateForTest(t)
		enableMigrationFSS(t)
		started := false
		migrationStartMigrationWatcher = func(
			_ context.Context, _, _, _, _ string, _ *metadataSyncInformer) {
			started = true
		}
		handlePvcMigrationAnnotations(context.Background(), nil,
			pvc("ns", "pvc", map[string]string{
				common.AnnMigrationCRKind: common.MigrationCRKindVMInfra,
				common.AnnMigrationCRName: "vminfra-cr",
			}), nil)
		if !started {
			t.Fatal("expected new watcher to be started")
		}
	})
}

func TestVolumeIDForPVC(t *testing.T) {
	resetMigrationStateForTest(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	pvcGood := makePVC("ns", "pvc-1", "pv-1", "sc", "")
	pvGood := makePV("pv-1", "csi-handle-1")
	pvcEmptyBind := makePVC("ns", "pvc-empty", "", "sc", "")
	pvNoCSI := &v1.PersistentVolume{
		ObjectMeta: metav1.ObjectMeta{Name: "pv-nocsi"},
		Spec:       v1.PersistentVolumeSpec{},
	}
	pvcRefNoCSI := makePVC("ns", "pvc-nocsi", "pv-nocsi", "sc", "")

	syncer, _ := newSyncerWithListers(t, ctx,
		[]*v1.PersistentVolumeClaim{pvcGood, pvcEmptyBind, pvcRefNoCSI},
		[]*v1.PersistentVolume{pvGood, pvNoCSI},
	)

	t.Run("lister hit returns handle", func(t *testing.T) {
		got, err := volumeIDForPVC(ctx, "ns", "pvc-1", syncer)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if got != "csi-handle-1" {
			t.Fatalf("got %q, want csi-handle-1", got)
		}
	})

	t.Run("empty VolumeName errors", func(t *testing.T) {
		_, err := volumeIDForPVC(ctx, "ns", "pvc-empty", syncer)
		if err == nil || !strings.Contains(err.Error(), "no bound volume") {
			t.Fatalf("unexpected error: %v", err)
		}
	})

	t.Run("PV missing CSI section errors", func(t *testing.T) {
		_, err := volumeIDForPVC(ctx, "ns", "pvc-nocsi", syncer)
		if err == nil || !strings.Contains(err.Error(), "no CSI volume handle") {
			t.Fatalf("unexpected error: %v", err)
		}
	})

	t.Run("lister miss for PVC falls back to k8s client", func(t *testing.T) {
		client := k8sfake.NewClientset(pvcGood, pvGood)
		migrationK8sNewClient = func(_ context.Context) (clientset.Interface, error) {
			return client, nil
		}
		// metadataSyncer == nil forces the fallback path.
		got, err := volumeIDForPVC(ctx, "ns", "pvc-1", nil)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if got != "csi-handle-1" {
			t.Fatalf("got %q, want csi-handle-1", got)
		}
	})

	t.Run("PVC client factory error propagates", func(t *testing.T) {
		migrationK8sNewClient = func(_ context.Context) (clientset.Interface, error) {
			return nil, errors.New("no k8s")
		}
		_, err := volumeIDForPVC(ctx, "ns", "pvc-1", nil)
		if err == nil || !strings.Contains(err.Error(), "failed to create k8s client") {
			t.Fatalf("unexpected error: %v", err)
		}
	})

	t.Run("PVC client get error propagates", func(t *testing.T) {
		client := k8sfake.NewClientset() // no PVC
		migrationK8sNewClient = func(_ context.Context) (clientset.Interface, error) {
			return client, nil
		}
		_, err := volumeIDForPVC(ctx, "ns", "pvc-missing", nil)
		if err == nil || !strings.Contains(err.Error(), "failed to get PVC") {
			t.Fatalf("unexpected error: %v", err)
		}
	})
}

func TestGetMigrationCR(t *testing.T) {
	resetMigrationStateForTest(t)
	ctx := context.Background()

	t.Run("unknown kind errors before client call", func(t *testing.T) {
		called := false
		migrationDynamicClientGetter = func() (dynamic.Interface, error) {
			called = true
			return nil, nil
		}
		_, err := getMigrationCR(ctx, "ns", "Mystery", "name")
		if err == nil {
			t.Fatal("expected error for unknown kind")
		}
		if called {
			t.Fatal("dynamic client should not be requested for unknown kind")
		}
	})

	t.Run("getter error propagates", func(t *testing.T) {
		migrationDynamicClientGetter = func() (dynamic.Interface, error) {
			return nil, errors.New("boom")
		}
		_, err := getMigrationCR(ctx, "ns", common.MigrationCRKindVolume, "name")
		if err == nil || !strings.Contains(err.Error(), "boom") {
			t.Fatalf("unexpected error: %v", err)
		}
	})

	t.Run("fetches an existing VolumeMigration CR", func(t *testing.T) {
		scheme := runtime.NewScheme()
		gvr := schema.GroupVersionResource{
			Group:    common.MobilityOperatorGroup,
			Version:  common.MobilityOperatorVersion,
			Resource: common.MobilityOperatorVolumeResource,
		}
		obj := &unstructured.Unstructured{}
		obj.SetGroupVersionKind(schema.GroupVersionKind{
			Group:   common.MobilityOperatorGroup,
			Version: common.MobilityOperatorVersion,
			Kind:    common.MigrationCRKindVolume,
		})
		obj.SetNamespace("ns")
		obj.SetName("pvc-data-1")
		_ = unstructured.SetNestedField(obj.Object, "policy-uuid-gold", "spec", "storagePolicyID")
		fakeDyn := dynamicfake.NewSimpleDynamicClientWithCustomListKinds(scheme,
			map[schema.GroupVersionResource]string{gvr: "VolumeMigrationList"}, obj)
		migrationDynamicClientGetter = func() (dynamic.Interface, error) { return fakeDyn, nil }

		got, err := getMigrationCR(ctx, "ns", common.MigrationCRKindVolume, "pvc-data-1")
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if got.GetName() != "pvc-data-1" {
			t.Fatalf("got name %q, want pvc-data-1", got.GetName())
		}
		if pid, _, _ := unstructured.NestedString(got.Object, "spec", "storagePolicyID"); pid != "policy-uuid-gold" {
			t.Fatalf("unexpected storagePolicyID: %q", pid)
		}
	})
}

func TestGetVolumeInfoService(t *testing.T) {
	resetMigrationStateForTest(t)
	ctx := context.Background()

	t.Run("returns package-level singleton when set", func(t *testing.T) {
		resetMigrationStateForTest(t)
		want := newFakeVolumeInfoService()
		volumeInfoService = want
		got, err := getVolumeInfoService(ctx)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if got != want {
			t.Fatal("expected package singleton to be returned as-is")
		}
	})

	t.Run("falls back to InitVolumeInfoService when singleton nil", func(t *testing.T) {
		resetMigrationStateForTest(t)
		volumeInfoService = nil
		fakeSvc := newFakeVolumeInfoService()
		migrationInitVolumeInfoService = func(_ context.Context) (cnsvolumeinfo.VolumeInfoService, error) {
			return fakeSvc, nil
		}
		got, err := getVolumeInfoService(ctx)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if got != fakeSvc {
			t.Fatal("expected fallback to return the fake service")
		}
	})

	t.Run("propagates init error", func(t *testing.T) {
		resetMigrationStateForTest(t)
		volumeInfoService = nil
		migrationInitVolumeInfoService = func(_ context.Context) (cnsvolumeinfo.VolumeInfoService, error) {
			return nil, errors.New("init failed")
		}
		_, err := getVolumeInfoService(ctx)
		if err == nil || !strings.Contains(err.Error(), "init failed") {
			t.Fatalf("expected wrapped init error, got %v", err)
		}
	})
}

func TestPatchMigrationConditionsType(t *testing.T) {
	resetMigrationStateForTest(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	pvc := makePVC("ns", "pvc-1", "pv-1", "sc", "")
	pv := makePV("pv-1", "csi-handle-1")
	syncer, _ := newSyncerWithListers(t, ctx,
		[]*v1.PersistentVolumeClaim{pvc}, []*v1.PersistentVolume{pv})

	// Seed the CNSVolumeInfo the client needs to find for the status patch target.
	seedCVI := &cnsvolumeinfov1alpha1.CNSVolumeInfo{
		ObjectMeta: metav1.ObjectMeta{
			Name:      cnsvolumeinfo.GetCnsVolumeInfoCrName("csi-handle-1"),
			Namespace: common.GetCSINamespace(),
		},
	}

	t.Run("patches InProgress condition via cnsOperatorClient", func(t *testing.T) {
		syncer.cnsOperatorClient = makeFakeCNSClient(seedCVI)

		err := patchMigrationConditionsInProgress(ctx, "ns", "pvc-1", "policy-uuid-gold", syncer)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		got := getCNSVolumeInfoFromClient(t, syncer.cnsOperatorClient, "csi-handle-1")
		if len(got.Status.MigrationConditions) != 1 {
			t.Fatalf("expected 1 condition, got %d", len(got.Status.MigrationConditions))
		}
		if got.Status.MigrationConditions[0].Type != cnsvolumeinfov1alpha1.MigrationConditionInProgress {
			t.Fatalf("got condition type %q, want %q",
				got.Status.MigrationConditions[0].Type, cnsvolumeinfov1alpha1.MigrationConditionInProgress)
		}
		if got.Status.MigrationTargetStoragePolicyID != "policy-uuid-gold" {
			t.Fatalf("MigrationTargetStoragePolicyID=%q, want policy-uuid-gold",
				got.Status.MigrationTargetStoragePolicyID)
		}
	})

	t.Run("patches Terminal Infeasible condition", func(t *testing.T) {
		syncer.cnsOperatorClient = makeFakeCNSClient(seedCVI)

		err := patchMigrationConditionsTerminal(ctx, "ns", "pvc-1",
			cnsvolumeinfov1alpha1.MigrationConditionInfeasible, "policy-uuid-gold", syncer)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		got := getCNSVolumeInfoFromClient(t, syncer.cnsOperatorClient, "csi-handle-1")
		if len(got.Status.MigrationConditions) == 0 {
			t.Fatal("expected at least one condition")
		}
		if got.Status.MigrationConditions[0].Type != cnsvolumeinfov1alpha1.MigrationConditionInfeasible {
			t.Fatalf("got condition type %q, want %q",
				got.Status.MigrationConditions[0].Type, cnsvolumeinfov1alpha1.MigrationConditionInfeasible)
		}
		if got.Status.MigrationTargetStoragePolicyID != "policy-uuid-gold" {
			t.Fatalf("MigrationTargetStoragePolicyID=%q, want policy-uuid-gold",
				got.Status.MigrationTargetStoragePolicyID)
		}
	})

	t.Run("volumeID resolution failure propagates", func(t *testing.T) {
		syncer.cnsOperatorClient = makeFakeCNSClient(seedCVI)

		err := patchMigrationConditionsType(ctx, "ns", "does-not-exist",
			cnsvolumeinfov1alpha1.MigrationConditionInfeasible, "policy-uuid-gold", syncer)
		if err == nil {
			t.Fatal("expected error when PVC missing")
		}
	})

	t.Run("client status patch error propagates", func(t *testing.T) {
		syncer.cnsOperatorClient = makeFakeCNSClientWithInterceptor(interceptor.Funcs{
			SubResourcePatch: func(_ context.Context, _ client.Client, _ string,
				_ client.Object, _ client.Patch, _ ...client.SubResourcePatchOption) error {
				return errors.New("patch failed")
			},
		}, seedCVI)

		err := patchMigrationConditionsType(ctx, "ns", "pvc-1",
			cnsvolumeinfov1alpha1.MigrationConditionInProgress, "policy-uuid-gold", syncer)
		if err == nil || !strings.Contains(err.Error(), "patch failed") {
			t.Fatalf("expected patch failure, got %v", err)
		}
	})
}

func TestBuildMigrationSuccessPatches(t *testing.T) {
	now := time.Date(2026, 5, 21, 12, 0, 0, 0, time.UTC)
	// mkCVI builds an OLD CNSVolumeInfo. sc is the (immutable) StorageClassName;
	// vac is the previous VolumeAttributeClassName (may be empty); pid is the
	// previous StoragePolicyID.
	mkCVI := func(sc, vac, pid string) *cnsvolumeinfov1alpha1.CNSVolumeInfo {
		return &cnsvolumeinfov1alpha1.CNSVolumeInfo{
			Spec: cnsvolumeinfov1alpha1.CNSVolumeInfoSpec{
				StorageClassName:         sc,
				VolumeAttributeClassName: vac,
				K8sCompliantName:         vac,
				StoragePolicyID:          pid,
			},
		}
	}

	volumeMigrationCR := &unstructured.Unstructured{Object: map[string]interface{}{
		"kind": common.MigrationCRKindVolume,
		"spec": map[string]interface{}{
			"storagePolicyID": "policy-uuid-gold",
		},
	}}

	t.Run("first migration (no prior VAC) -> spec patches VAC+K8sCN+PolicyID; SC unchanged",
		func(t *testing.T) {
			old := mkCVI("sc-old", "", "policy-uuid-silver")
			pvc := makePVC("ns", "pvc-1", "pv-1", "sc-old", "vac-gold")

			spec, status, newCVI, vac, pid, err := buildMigrationSuccessPatches(
				old, pvc, volumeMigrationCR, "pvc-1", now)
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if vac != "vac-gold" || pid != "policy-uuid-gold" {
				t.Fatalf("derived vac=%q pid=%q", vac, pid)
			}
			// StorageClassName must be untouched on the projected newCVI.
			if newCVI.Spec.StorageClassName != "sc-old" {
				t.Fatalf("StorageClassName was mutated to %q (must remain sc-old)",
					newCVI.Spec.StorageClassName)
			}
			if newCVI.Spec.VolumeAttributeClassName != "vac-gold" {
				t.Fatalf("VolumeAttributeClassName=%q want vac-gold",
					newCVI.Spec.VolumeAttributeClassName)
			}
			if newCVI.Spec.K8sCompliantName != "vac-gold" {
				t.Fatalf("K8sCompliantName=%q want vac-gold (mirrors VAC)",
					newCVI.Spec.K8sCompliantName)
			}
			if newCVI.Spec.StoragePolicyID != "policy-uuid-gold" {
				t.Fatalf("StoragePolicyID=%q want policy-uuid-gold",
					newCVI.Spec.StoragePolicyID)
			}
			var specMap map[string]interface{}
			if err := json.Unmarshal(spec, &specMap); err != nil {
				t.Fatalf("spec not JSON: %v", err)
			}
			inner := specMap["spec"].(map[string]interface{})
			if inner["volumeAttributeClassName"] != "vac-gold" {
				t.Fatalf("spec patch missing volumeAttributeClassName: %v", inner)
			}
			if inner["k8sCompliantName"] != "vac-gold" {
				t.Fatalf("spec patch missing k8sCompliantName: %v", inner)
			}
			if inner["storagePolicyID"] != "policy-uuid-gold" {
				t.Fatalf("spec patch missing storagePolicyID: %v", inner)
			}
			if _, hasSC := inner["storageClassName"]; hasSC {
				t.Fatalf("spec patch MUST NOT include storageClassName (immutable): %v", inner)
			}
			if !strings.Contains(string(status), cnsvolumeinfov1alpha1.MigrationConditionComplete) {
				t.Fatalf("status patch missing Complete: %s", status)
			}
			if !strings.Contains(string(status), now.Format(time.RFC3339)) {
				t.Fatalf("status patch missing timestamp %q: %s",
					now.Format(time.RFC3339), status)
			}
			if !strings.Contains(string(status), `StorageClass \"sc-old\" unchanged`) {
				t.Fatalf("status message should call out unchanged StorageClass: %s", status)
			}
			var statusMap map[string]interface{}
			if err := json.Unmarshal(status, &statusMap); err != nil {
				t.Fatalf("status not JSON: %v", err)
			}
			if got := statusMap["status"].(map[string]interface{})["migrationTargetStoragePolicyID"]; got !=
				"policy-uuid-gold" {
				t.Fatalf("status patch migrationTargetStoragePolicyID=%v, want policy-uuid-gold", got)
			}
		})

	t.Run("VAC->VAC migration -> spec patches new VAC+K8sCN+PolicyID", func(t *testing.T) {
		old := mkCVI("sc-old", "vac-silver", "policy-uuid-silver")
		pvc := makePVC("ns", "pvc-1", "pv-1", "sc-old", "vac-gold")

		spec, _, newCVI, vac, pid, err := buildMigrationSuccessPatches(
			old, pvc, volumeMigrationCR, "pvc-1", now)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if vac != "vac-gold" || pid != "policy-uuid-gold" {
			t.Fatalf("derived vac=%q pid=%q", vac, pid)
		}
		if newCVI.Spec.StorageClassName != "sc-old" {
			t.Fatalf("StorageClassName was mutated to %q (must remain sc-old)",
				newCVI.Spec.StorageClassName)
		}
		if newCVI.Spec.VolumeAttributeClassName != "vac-gold" ||
			newCVI.Spec.K8sCompliantName != "vac-gold" {
			t.Fatalf("VAC/K8sCN not updated: %+v", newCVI.Spec)
		}
		var specMap map[string]interface{}
		if err := json.Unmarshal(spec, &specMap); err != nil {
			t.Fatalf("spec not JSON: %v", err)
		}
		inner := specMap["spec"].(map[string]interface{})
		if inner["volumeAttributeClassName"] != "vac-gold" ||
			inner["k8sCompliantName"] != "vac-gold" ||
			inner["storagePolicyID"] != "policy-uuid-gold" {
			t.Fatalf("unexpected spec patch: %v", inner)
		}
		if _, hasSC := inner["storageClassName"]; hasSC {
			t.Fatalf("spec patch MUST NOT include storageClassName: %v", inner)
		}
	})

	t.Run("no spec change when CVI already matches the migration result -> nil spec patch",
		func(t *testing.T) {
			old := mkCVI("sc-old", "vac-gold", "policy-uuid-gold")
			pvc := makePVC("ns", "pvc-1", "pv-1", "sc-old", "vac-gold")

			spec, status, _, _, _, err := buildMigrationSuccessPatches(
				old, pvc, volumeMigrationCR, "pvc-1", now)
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if spec != nil {
				t.Fatalf("expected nil spec patch (no change), got: %s", spec)
			}
			if len(status) == 0 {
				t.Fatal("expected non-empty status patch")
			}
		})

	t.Run("PVC has no VAC -> no VAC/K8sCN patch (only StoragePolicyID may change)",
		func(t *testing.T) {
			old := mkCVI("sc-old", "", "policy-uuid-silver")
			pvc := makePVC("ns", "pvc-1", "pv-1", "sc-old", "")
			spec, _, newCVI, vac, pid, err := buildMigrationSuccessPatches(
				old, pvc, volumeMigrationCR, "pvc-1", now)
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if vac != "" {
				t.Fatalf("got vac=%q, want empty (PVC has no VAC)", vac)
			}
			if pid != "policy-uuid-gold" {
				t.Fatalf("got pid=%q, want policy-uuid-gold", pid)
			}
			if newCVI.Spec.VolumeAttributeClassName != "" ||
				newCVI.Spec.K8sCompliantName != "" {
				t.Fatalf("VAC/K8sCN should remain empty: %+v", newCVI.Spec)
			}
			if newCVI.Spec.StorageClassName != "sc-old" {
				t.Fatalf("StorageClassName was mutated: %+v", newCVI.Spec)
			}
			if spec == nil {
				t.Fatal("expected spec patch (StoragePolicyID changed)")
			}
			var specMap map[string]interface{}
			if err := json.Unmarshal(spec, &specMap); err != nil {
				t.Fatalf("spec not JSON: %v", err)
			}
			inner := specMap["spec"].(map[string]interface{})
			if _, hasVAC := inner["volumeAttributeClassName"]; hasVAC {
				t.Fatalf("spec patch must not set volumeAttributeClassName: %v", inner)
			}
			if _, hasK8sCN := inner["k8sCompliantName"]; hasK8sCN {
				t.Fatalf("spec patch must not set k8sCompliantName: %v", inner)
			}
			if _, hasSC := inner["storageClassName"]; hasSC {
				t.Fatalf("spec patch must not set storageClassName: %v", inner)
			}
		})

	t.Run("nil PVC is tolerated (no VAC change, no SC mutation)", func(t *testing.T) {
		old := mkCVI("sc-old", "vac-silver", "policy-uuid-silver")
		_, status, newCVI, vac, pid, err := buildMigrationSuccessPatches(
			old, nil, volumeMigrationCR, "pvc-1", now)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if vac != "" {
			t.Fatalf("got vac=%q, want empty", vac)
		}
		if pid != "policy-uuid-gold" {
			t.Fatalf("got pid=%q, want policy-uuid-gold", pid)
		}
		if newCVI.Spec.StorageClassName != "sc-old" {
			t.Fatalf("StorageClassName was mutated: %+v", newCVI.Spec)
		}
		if !strings.Contains(string(status), cnsvolumeinfov1alpha1.MigrationConditionComplete) {
			t.Fatalf("status patch missing Complete: %s", status)
		}
	})
}

func TestPropagateMigrationSuccess(t *testing.T) {
	resetMigrationStateForTest(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	pvc := makePVC("ns", "pvc-1", "pv-1", "sc-old", "vac-gold")
	pv := makePV("pv-1", "csi-handle-1")
	syncer, _ := newSyncerWithListers(t, ctx,
		[]*v1.PersistentVolumeClaim{pvc}, []*v1.PersistentVolume{pv})

	fakeSvc := newFakeVolumeInfoService()
	fakeSvc.infoByID["csi-handle-1"] = &cnsvolumeinfov1alpha1.CNSVolumeInfo{
		ObjectMeta: metav1.ObjectMeta{Name: "csi-handle-1"},
		Spec: cnsvolumeinfov1alpha1.CNSVolumeInfoSpec{
			VolumeID:         "csi-handle-1",
			StorageClassName: "sc-old",
			StoragePolicyID:  "policy-uuid-silver",
		},
	}
	volumeInfoService = fakeSvc

	// seedCVI is pre-seeded in the fake client so the patch target exists.
	seedCVI := &cnsvolumeinfov1alpha1.CNSVolumeInfo{
		ObjectMeta: metav1.ObjectMeta{
			Name:      cnsvolumeinfo.GetCnsVolumeInfoCrName("csi-handle-1"),
			Namespace: common.GetCSINamespace(),
		},
		Spec: cnsvolumeinfov1alpha1.CNSVolumeInfoSpec{
			VolumeID:         "csi-handle-1",
			StorageClassName: "sc-old",
			StoragePolicyID:  "policy-uuid-silver",
		},
	}

	cr := &unstructured.Unstructured{Object: map[string]interface{}{
		"kind": common.MigrationCRKindVolume,
		"spec": map[string]interface{}{"storagePolicyID": "policy-uuid-gold"},
	}}

	t.Run("happy path: spec + status patched, quota cascade fires", func(t *testing.T) {
		quotaCalled := false
		migrationHandleStoragePolicyChange = func(
			_ context.Context, _ client.Client,
			oldCVI, newCVI cnsvolumeinfov1alpha1.CNSVolumeInfo) {
			quotaCalled = true
			if oldCVI.Spec.StoragePolicyID != "policy-uuid-silver" ||
				newCVI.Spec.StoragePolicyID != "policy-uuid-gold" {
				t.Errorf("unexpected quota args old=%s new=%s",
					oldCVI.Spec.StoragePolicyID, newCVI.Spec.StoragePolicyID)
			}
		}
		syncer.cnsOperatorClient = makeFakeCNSClient(seedCVI)

		if err := propagateMigrationSuccess(ctx, syncer, listerPVC(t, syncer, "ns", "pvc-1"), cr); err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if !quotaCalled {
			t.Fatal("expected quota cascade")
		}
		got := getCNSVolumeInfoFromClient(t, syncer.cnsOperatorClient, "csi-handle-1")
		if got.Spec.StoragePolicyID != "policy-uuid-gold" {
			t.Fatalf("spec not patched: StoragePolicyID=%q, want policy-uuid-gold", got.Spec.StoragePolicyID)
		}
		if len(got.Status.MigrationConditions) == 0 {
			t.Fatal("expected MigrationConditions in status after patch")
		}
		if got.Status.MigrationConditions[0].Type != cnsvolumeinfov1alpha1.MigrationConditionComplete {
			t.Fatalf("got condition %q, want Complete", got.Status.MigrationConditions[0].Type)
		}
		if got.Status.MigrationTargetStoragePolicyID != "policy-uuid-gold" {
			t.Fatalf("MigrationTargetStoragePolicyID=%q, want policy-uuid-gold",
				got.Status.MigrationTargetStoragePolicyID)
		}
	})

	t.Run("status patch failure propagates", func(t *testing.T) {
		migrationHandleStoragePolicyChange = func(
			_ context.Context, _ client.Client,
			_, _ cnsvolumeinfov1alpha1.CNSVolumeInfo) {
		}
		syncer.cnsOperatorClient = makeFakeCNSClientWithInterceptor(interceptor.Funcs{
			SubResourcePatch: func(_ context.Context, _ client.Client, _ string,
				_ client.Object, _ client.Patch, _ ...client.SubResourcePatchOption) error {
				return errors.New("patch status fail")
			},
		}, seedCVI)

		if err := propagateMigrationSuccess(ctx, syncer, listerPVC(t, syncer, "ns", "pvc-1"), cr); err == nil ||
			!strings.Contains(err.Error(), "patch status fail") {
			t.Fatalf("expected status patch failure, got: %v", err)
		}
	})

	t.Run("nil PVC errors", func(t *testing.T) {
		syncer.cnsOperatorClient = makeFakeCNSClient(seedCVI)
		if err := propagateMigrationSuccess(ctx, syncer, nil, cr); err == nil {
			t.Fatal("expected error for nil PVC")
		}
	})
}

func TestStartMigrationWatcher(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	pvc := makePVC("ns", "pvc-1", "pv-1", "sc-old", "vac-gold")
	pv := makePV("pv-1", "csi-handle-1")
	syncer, _ := newSyncerWithListers(t, ctx,
		[]*v1.PersistentVolumeClaim{pvc}, []*v1.PersistentVolume{pv})

	fakeSvc := newFakeVolumeInfoService()
	fakeSvc.infoByID["csi-handle-1"] = &cnsvolumeinfov1alpha1.CNSVolumeInfo{
		ObjectMeta: metav1.ObjectMeta{Name: "csi-handle-1"},
		Spec: cnsvolumeinfov1alpha1.CNSVolumeInfoSpec{
			VolumeID:         "csi-handle-1",
			StorageClassName: "sc-old",
			StoragePolicyID:  "policy-uuid-silver",
		},
	}

	// The fake cnsOperatorClient is seeded with the CNSVolumeInfo object so
	// that all patch calls (InProgress, Complete, Infeasible) have a target.
	watcherSeedCVI := &cnsvolumeinfov1alpha1.CNSVolumeInfo{
		ObjectMeta: metav1.ObjectMeta{
			Name:      cnsvolumeinfo.GetCnsVolumeInfoCrName("csi-handle-1"),
			Namespace: common.GetCSINamespace(),
		},
		Spec: cnsvolumeinfov1alpha1.CNSVolumeInfoSpec{
			VolumeID:         "csi-handle-1",
			StorageClassName: "sc-old",
			StoragePolicyID:  "policy-uuid-silver",
		},
	}

	// Build a fake dynamic client serving a VolumeMigration CR that we will
	// mutate mid-test to drive the watcher's state transitions.
	scheme := runtime.NewScheme()
	gvr := schema.GroupVersionResource{
		Group:    common.MobilityOperatorGroup,
		Version:  common.MobilityOperatorVersion,
		Resource: common.MobilityOperatorVolumeResource,
	}

	// makeCRAt builds the VolumeMigration CR at the given generation, targeting
	// specPolicy, with Ready observed at observedGeneration.
	makeCRAt := func(ready, reason string, generation, observedGeneration int64,
		specPolicy string) *unstructured.Unstructured {
		cr := &unstructured.Unstructured{Object: map[string]interface{}{
			"apiVersion": common.MobilityOperatorGroup + "/" + common.MobilityOperatorVersion,
			"kind":       common.MigrationCRKindVolume,
			"metadata": map[string]interface{}{"namespace": "ns", "name": "cr-1",
				"generation": generation},
			"spec": map[string]interface{}{"storagePolicyID": specPolicy},
		}}
		if ready != "" {
			_ = unstructured.SetNestedSlice(cr.Object, []interface{}{
				map[string]interface{}{"type": "Ready", "status": ready, "reason": reason,
					"observedGeneration": observedGeneration},
			}, "status", "conditions")
		}
		return cr
	}
	// makeCR builds a current CR (Ready observed at the latest generation) that
	// targets the policy the watcher resolves for the PVC.
	makeCR := func(ready, reason string) *unstructured.Unstructured {
		return makeCRAt(ready, reason, 1, 1, "policy-uuid-gold")
	}
	updateCR := func(t *testing.T, dyn dynamic.Interface, cr *unstructured.Unstructured) {
		t.Helper()
		if _, err := dyn.Resource(gvr).Namespace("ns").Update(ctx, cr, metav1.UpdateOptions{}); err != nil {
			t.Fatalf("update CR failed: %v", err)
		}
	}
	watcherActive := func() bool {
		activeMigrationsMu.Lock()
		defer activeMigrationsMu.Unlock()
		_, present := activeMigrations["ns/pvc-1"]
		return present
	}

	// setup resets global state for a subtest and returns a counter of quota
	// transfers and a setter for the target policy the watcher resolves.
	setup := func(t *testing.T) (quotaCalls func() int, setTarget func(string)) {
		resetMigrationStateForTest(t)
		migrationPollInterval = 10 * time.Millisecond
		volumeInfoService = fakeSvc
		syncer.cnsOperatorClient = makeFakeCNSClient(watcherSeedCVI.DeepCopy())

		var mu sync.Mutex
		calls := 0
		migrationHandleStoragePolicyChange = func(
			_ context.Context, _ client.Client,
			_, _ cnsvolumeinfov1alpha1.CNSVolumeInfo) {
			mu.Lock()
			defer mu.Unlock()
			calls++
		}
		target := "policy-uuid-gold"
		migrationResolveTargetPolicyID = func(_ context.Context, _, _ string,
			_ *metadataSyncInformer) (*v1.PersistentVolumeClaim, string, error) {
			mu.Lock()
			defer mu.Unlock()
			return pvc, target, nil
		}
		return func() int {
				mu.Lock()
				defer mu.Unlock()
				return calls
			}, func(p string) {
				mu.Lock()
				defer mu.Unlock()
				target = p
			}
	}

	// startWatcher serves initialCR from a fake dynamic client and starts the
	// watcher against it.
	startWatcher := func(t *testing.T, initialCR *unstructured.Unstructured) dynamic.Interface {
		t.Helper()
		fakeDyn := dynamicfake.NewSimpleDynamicClientWithCustomListKinds(scheme,
			map[schema.GroupVersionResource]string{gvr: "VolumeMigrationList"}, initialCR)
		migrationDynamicClientGetter = func() (dynamic.Interface, error) { return fakeDyn, nil }
		startMigrationWatcher(ctx, "ns", "pvc-1", common.MigrationCRKindVolume, "cr-1", syncer)
		return fakeDyn
	}

	// keepsPolling lets the watcher tick several times and asserts that it did
	// not treat the current CR as terminal.
	keepsPolling := func(t *testing.T) {
		t.Helper()
		time.Sleep(60 * time.Millisecond)
		if !watcherActive() {
			t.Fatal("watcher exited, want it to keep polling")
		}
	}

	waitForExit := func(t *testing.T) {
		t.Helper()
		deadline := time.Now().Add(2 * time.Second)
		for time.Now().Before(deadline) {
			if !watcherActive() {
				return
			}
			time.Sleep(20 * time.Millisecond)
		}
		t.Fatalf("watcher did not exit in time")
	}

	cviStatus := func(t *testing.T) cnsvolumeinfov1alpha1.CNSVolumeInfoStatus {
		t.Helper()
		return getCNSVolumeInfoFromClient(t, syncer.cnsOperatorClient, "csi-handle-1").Status
	}
	assertCondition := func(t *testing.T, wantType, wantTarget string) {
		t.Helper()
		st := cviStatus(t)
		if len(st.MigrationConditions) != 1 || st.MigrationConditions[0].Type != wantType {
			t.Fatalf("MigrationConditions=%+v, want a single %s", st.MigrationConditions, wantType)
		}
		if st.MigrationTargetStoragePolicyID != wantTarget {
			t.Fatalf("MigrationTargetStoragePolicyID=%q, want %q", st.MigrationTargetStoragePolicyID, wantTarget)
		}
	}

	t.Run("terminal Complete propagates and exits", func(t *testing.T) {
		quotaCalls, _ := setup(t)
		dyn := startWatcher(t, makeCR("Unknown", "Reconciling"))
		keepsPolling(t)
		assertCondition(t, cnsvolumeinfov1alpha1.MigrationConditionInProgress, "policy-uuid-gold")

		updateCR(t, dyn, makeCR("True", "Succeeded"))
		waitForExit(t)
		assertCondition(t, cnsvolumeinfov1alpha1.MigrationConditionComplete, "policy-uuid-gold")
		if quotaCalls() != 1 {
			t.Fatalf("quota transfer called %d times, want 1", quotaCalls())
		}
	})

	t.Run("Ready=False/InProgress keeps polling, then Complete applies spec patch and quota", func(t *testing.T) {
		quotaCalls, _ := setup(t)
		dyn := startWatcher(t, makeCR("False", "InProgress"))
		keepsPolling(t)
		updateCR(t, dyn, makeCR("False", "Internal"))
		keepsPolling(t)
		assertCondition(t, cnsvolumeinfov1alpha1.MigrationConditionInProgress, "policy-uuid-gold")

		updateCR(t, dyn, makeCR("True", "Succeeded"))
		waitForExit(t)
		assertCondition(t, cnsvolumeinfov1alpha1.MigrationConditionComplete, "policy-uuid-gold")
		got := getCNSVolumeInfoFromClient(t, syncer.cnsOperatorClient, "csi-handle-1")
		if got.Spec.StoragePolicyID != "policy-uuid-gold" || got.Spec.VolumeAttributeClassName != "vac-gold" {
			t.Fatalf("CNSVolumeInfo spec not patched: %+v", got.Spec)
		}
		if quotaCalls() != 1 {
			t.Fatalf("quota transfer called %d times, want 1", quotaCalls())
		}
	})

	t.Run("terminal Infeasible propagates and exits", func(t *testing.T) {
		quotaCalls, _ := setup(t)
		dyn := startWatcher(t, makeCR("Unknown", "Reconciling"))
		keepsPolling(t)

		updateCR(t, dyn, makeCR("False", "NotSupported"))
		waitForExit(t)
		assertCondition(t, cnsvolumeinfov1alpha1.MigrationConditionInfeasible, "policy-uuid-gold")
		if quotaCalls() != 0 {
			t.Fatalf("quota transfer called %d times, want 0", quotaCalls())
		}
	})

	t.Run("stale Ready=True from an older generation is ignored", func(t *testing.T) {
		quotaCalls, _ := setup(t)
		// Retarget/rollback bumped the generation; Ready is still the previous
		// run's True.
		dyn := startWatcher(t, makeCRAt("True", "Succeeded", 2, 1, "policy-uuid-gold"))
		keepsPolling(t)
		if quotaCalls() != 0 {
			t.Fatalf("quota transfer called %d times on stale Ready, want 0", quotaCalls())
		}

		updateCR(t, dyn, makeCRAt("True", "Succeeded", 2, 2, "policy-uuid-gold"))
		waitForExit(t)
		assertCondition(t, cnsvolumeinfov1alpha1.MigrationConditionComplete, "policy-uuid-gold")
	})

	t.Run("stale terminal Ready for another policy is ignored", func(t *testing.T) {
		setup(t)
		// The CR still targets the previous policy and carries its terminal
		// Ready; the PVC now asks for policy-uuid-gold.
		dyn := startWatcher(t, makeCRAt("False", "NotSupported", 1, 1, "policy-uuid-silver"))
		keepsPolling(t)
		assertCondition(t, cnsvolumeinfov1alpha1.MigrationConditionInProgress, "policy-uuid-gold")

		updateCR(t, dyn, makeCRAt("False", "NotSupported", 2, 2, "policy-uuid-gold"))
		waitForExit(t)
		assertCondition(t, cnsvolumeinfov1alpha1.MigrationConditionInfeasible, "policy-uuid-gold")
	})

	t.Run("target change mid-watch re-patches InProgress with the new target", func(t *testing.T) {
		_, setTarget := setup(t)
		dyn := startWatcher(t, makeCR("False", "InProgress"))
		keepsPolling(t)
		assertCondition(t, cnsvolumeinfov1alpha1.MigrationConditionInProgress, "policy-uuid-gold")

		setTarget("policy-uuid-platinum")
		keepsPolling(t)
		assertCondition(t, cnsvolumeinfov1alpha1.MigrationConditionInProgress, "policy-uuid-platinum")

		// A Ready=True for the old target must not complete the new modify.
		updateCR(t, dyn, makeCRAt("True", "Succeeded", 1, 1, "policy-uuid-gold"))
		keepsPolling(t)

		updateCR(t, dyn, makeCRAt("True", "Succeeded", 2, 2, "policy-uuid-platinum"))
		waitForExit(t)
		assertCondition(t, cnsvolumeinfov1alpha1.MigrationConditionComplete, "policy-uuid-platinum")
	})

	t.Run("target resolution failure keeps polling", func(t *testing.T) {
		setup(t)
		var mu sync.Mutex
		lookupFails := true
		migrationResolveTargetPolicyID = func(_ context.Context, _, _ string,
			_ *metadataSyncInformer) (*v1.PersistentVolumeClaim, string, error) {
			mu.Lock()
			defer mu.Unlock()
			if lookupFails {
				return nil, "", errors.New("VAC lookup failed")
			}
			return pvc, "policy-uuid-gold", nil
		}
		startWatcher(t, makeCR("True", "Succeeded"))
		keepsPolling(t)

		// Once the lookup succeeds, the current Ready=True completes the modify.
		mu.Lock()
		lookupFails = false
		mu.Unlock()
		waitForExit(t)
		assertCondition(t, cnsvolumeinfov1alpha1.MigrationConditionComplete, "policy-uuid-gold")
	})

	// seedRecorded replaces the CNSVolumeInfo with one that already records a
	// migration result, as a finished watcher leaves it.
	const recordedMessage = "recorded by an earlier watcher"
	seedRecorded := func(conditionType, target, specPolicy string) {
		cvi := watcherSeedCVI.DeepCopy()
		cvi.Spec.StoragePolicyID = specPolicy
		cvi.Status.MigrationTargetStoragePolicyID = target
		cvi.Status.MigrationConditions = []metav1.Condition{{
			Type: conditionType, Status: metav1.ConditionTrue, Reason: conditionType, Message: recordedMessage,
		}}
		syncer.cnsOperatorClient = makeFakeCNSClient(cvi)
	}
	assertUnchanged := func(t *testing.T, wantType string) {
		t.Helper()
		st := cviStatus(t)
		if len(st.MigrationConditions) != 1 || st.MigrationConditions[0].Type != wantType ||
			st.MigrationConditions[0].Message != recordedMessage {
			t.Fatalf("MigrationConditions=%+v, want the recorded %s left untouched", st.MigrationConditions, wantType)
		}
	}

	t.Run("relaunch after Complete for the same target exits without writing", func(t *testing.T) {
		quotaCalls, _ := setup(t)
		seedRecorded(cnsvolumeinfov1alpha1.MigrationConditionComplete, "policy-uuid-gold", "policy-uuid-gold")
		startWatcher(t, makeCR("True", "Succeeded"))
		waitForExit(t)
		assertUnchanged(t, cnsvolumeinfov1alpha1.MigrationConditionComplete)
		if quotaCalls() != 0 {
			t.Fatalf("quota transfer called %d times, want 0", quotaCalls())
		}
	})

	t.Run("relaunch after Infeasible for the same target exits without writing", func(t *testing.T) {
		setup(t)
		seedRecorded(cnsvolumeinfov1alpha1.MigrationConditionInfeasible, "policy-uuid-gold", "policy-uuid-silver")
		startWatcher(t, makeCR("False", "NotSupported"))
		waitForExit(t)
		assertUnchanged(t, cnsvolumeinfov1alpha1.MigrationConditionInfeasible)
	})

	t.Run("recorded Complete but CR is a new generation -> writes InProgress and polls", func(t *testing.T) {
		setup(t)
		seedRecorded(cnsvolumeinfov1alpha1.MigrationConditionComplete, "policy-uuid-gold", "policy-uuid-gold")
		dyn := startWatcher(t, makeCRAt("True", "Succeeded", 2, 1, "policy-uuid-gold"))
		keepsPolling(t)
		assertCondition(t, cnsvolumeinfov1alpha1.MigrationConditionInProgress, "policy-uuid-gold")

		updateCR(t, dyn, makeCRAt("True", "Succeeded", 2, 2, "policy-uuid-gold"))
		waitForExit(t)
		assertCondition(t, cnsvolumeinfov1alpha1.MigrationConditionComplete, "policy-uuid-gold")
	})

	t.Run("recorded result for another target -> propagates the current result", func(t *testing.T) {
		quotaCalls, _ := setup(t)
		seedRecorded(cnsvolumeinfov1alpha1.MigrationConditionInfeasible, "policy-uuid-silver", "policy-uuid-silver")
		startWatcher(t, makeCR("True", "Succeeded"))
		waitForExit(t)
		assertCondition(t, cnsvolumeinfov1alpha1.MigrationConditionComplete, "policy-uuid-gold")
		if quotaCalls() != 1 {
			t.Fatalf("quota transfer called %d times, want 1", quotaCalls())
		}
	})

	t.Run("success records the VAC from the validated PVC snapshot", func(t *testing.T) {
		setup(t)
		// The lister PVC names vac-gold; the snapshot the target was resolved
		// from names vac-snapshot. Success must record the snapshot's VAC.
		snapshot := pvc.DeepCopy()
		snapshot.Spec.VolumeAttributesClassName = stringPtr("vac-snapshot")
		migrationResolveTargetPolicyID = func(_ context.Context, _, _ string,
			_ *metadataSyncInformer) (*v1.PersistentVolumeClaim, string, error) {
			return snapshot, "policy-uuid-gold", nil
		}
		startWatcher(t, makeCR("True", "Succeeded"))
		waitForExit(t)
		got := getCNSVolumeInfoFromClient(t, syncer.cnsOperatorClient, "csi-handle-1")
		if got.Spec.VolumeAttributeClassName != "vac-snapshot" || got.Spec.StoragePolicyID != "policy-uuid-gold" {
			t.Fatalf("CNSVolumeInfo spec=%+v, want VAC vac-snapshot with policy-uuid-gold", got.Spec)
		}
	})

	t.Run("VAC change while recording a result keeps the watcher polling for the new target", func(t *testing.T) {
		_, setTarget := setup(t)
		IsVMPVCStoragePolicyMutabilityEnabled = true
		// Slow ticks leave room to retarget between two polls.
		migrationPollInterval = 200 * time.Millisecond
		annotated := func(vac string) *v1.PersistentVolumeClaim {
			p := makePVC("ns", "pvc-1", "pv-1", "sc-old", vac)
			p.Annotations = map[string]string{
				common.AnnMigrationCRKind: common.MigrationCRKindVolume,
				common.AnnMigrationCRName: "cr-1",
			}
			return p
		}
		waitForCondition := func(t *testing.T, wantType, wantTarget string) {
			t.Helper()
			deadline := time.Now().Add(3 * time.Second)
			for time.Now().Before(deadline) {
				st := cviStatus(t)
				if len(st.MigrationConditions) == 1 && st.MigrationConditions[0].Type == wantType &&
					st.MigrationTargetStoragePolicyID == wantTarget {
					return
				}
				time.Sleep(5 * time.Millisecond)
			}
			t.Fatalf("CNSVolumeInfo never reached %s for %s: %+v", wantType, wantTarget, cviStatus(t))
		}

		dyn := startWatcher(t, makeCR("True", "Succeeded"))
		// A PVC update that keeps the VAC must not flag the watcher; the VAC
		// change must. Both arrive before the first tick records the result for
		// the old target, as when a retarget races the watcher.
		handlePvcMigrationAnnotations(ctx, annotated("vac-gold"), annotated("vac-gold"), syncer)
		activeMigrationsMu.Lock()
		flagged := activeMigrations["ns/pvc-1"].recheck
		activeMigrationsMu.Unlock()
		if flagged {
			t.Fatal("PVC update without a VAC change flagged the watcher")
		}
		handlePvcMigrationAnnotations(ctx, annotated("vac-gold"), annotated("vac-platinum"), syncer)

		waitForCondition(t, cnsvolumeinfov1alpha1.MigrationConditionComplete, "policy-uuid-gold")
		setTarget("policy-uuid-platinum")
		waitForCondition(t, cnsvolumeinfov1alpha1.MigrationConditionInProgress, "policy-uuid-platinum")
		if !watcherActive() {
			t.Fatal("watcher exited, want it to keep polling for the new target")
		}

		updateCR(t, dyn, makeCRAt("True", "Succeeded", 2, 2, "policy-uuid-platinum"))
		waitForExit(t)
		assertCondition(t, cnsvolumeinfov1alpha1.MigrationConditionComplete, "policy-uuid-platinum")
	})

	t.Run("CR deleted -> watcher exits", func(t *testing.T) {
		setup(t)
		dyn := startWatcher(t, makeCR("Unknown", "Reconciling"))
		keepsPolling(t)
		if err := dyn.Resource(gvr).Namespace("ns").Delete(ctx, "cr-1", metav1.DeleteOptions{}); err != nil {
			t.Fatalf("delete CR failed: %v", err)
		}
		waitForExit(t)
	})
}

// TestResolveMigrationTargetPolicyID verifies the PVC -> VAC -> storagePolicyID
// resolution used by the watcher, including a VAC recreated with another policy.
func TestResolveMigrationTargetPolicyID(t *testing.T) {
	resetMigrationStateForTest(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	pvcGold := makePVC("ns", "pvc-gold", "pv-1", "sc", "vac-gold")
	pvcNoVAC := makePVC("ns", "pvc-novac", "pv-2", "sc", "")
	pvcNoParam := makePVC("ns", "pvc-noparam", "pv-3", "sc", "vac-noparam")
	pvcMissingVAC := makePVC("ns", "pvc-missingvac", "pv-4", "sc", "vac-missing")
	syncer, _ := newSyncerWithListers(t, ctx,
		[]*v1.PersistentVolumeClaim{pvcGold, pvcNoVAC, pvcNoParam, pvcMissingVAC}, nil)

	vacClient := k8sfake.NewClientset(
		&storagev1.VolumeAttributesClass{
			ObjectMeta: metav1.ObjectMeta{Name: "vac-gold"},
			DriverName: "csi.vsphere.vmware.com",
			Parameters: map[string]string{"storagePolicyID": "policy-uuid-gold"},
		},
		&storagev1.VolumeAttributesClass{
			ObjectMeta: metav1.ObjectMeta{Name: "vac-noparam"},
			DriverName: "csi.vsphere.vmware.com",
			Parameters: map[string]string{"other": "x"},
		},
	)
	migrationK8sNewClient = func(_ context.Context) (clientset.Interface, error) {
		return vacClient, nil
	}

	t.Run("resolves the VAC storage policy and returns the PVC snapshot", func(t *testing.T) {
		pvc, got, err := resolveMigrationTargetPolicyID(ctx, "ns", "pvc-gold", syncer)
		if err != nil || got != "policy-uuid-gold" {
			t.Fatalf("got (%q, %v), want policy-uuid-gold", got, err)
		}
		if pvc == nil || pvc.Name != "pvc-gold" {
			t.Fatalf("got PVC %+v, want pvc-gold", pvc)
		}
	})

	t.Run("a VAC recreated with another policy is picked up", func(t *testing.T) {
		vacs := vacClient.StorageV1().VolumeAttributesClasses()
		if err := vacs.Delete(ctx, "vac-gold", metav1.DeleteOptions{}); err != nil {
			t.Fatalf("delete VAC: %v", err)
		}
		if _, err := vacs.Create(ctx, &storagev1.VolumeAttributesClass{
			ObjectMeta: metav1.ObjectMeta{Name: "vac-gold"},
			DriverName: "csi.vsphere.vmware.com",
			Parameters: map[string]string{"storagePolicyID": "policy-uuid-gold-2"},
		}, metav1.CreateOptions{}); err != nil {
			t.Fatalf("recreate VAC: %v", err)
		}
		_, got, err := resolveMigrationTargetPolicyID(ctx, "ns", "pvc-gold", syncer)
		if err != nil || got != "policy-uuid-gold-2" {
			t.Fatalf("got (%q, %v), want policy-uuid-gold-2", got, err)
		}
	})

	for _, tc := range []struct{ name, pvc, wantErr string }{
		{"PVC without VAC errors", "pvc-novac", "no VolumeAttributesClassName"},
		{"VAC without storagePolicyID errors", "pvc-noparam", "no storagePolicyID parameter"},
		{"missing VAC errors", "pvc-missingvac", "failed to get VolumeAttributesClass"},
		{"missing PVC errors", "pvc-does-not-exist", "failed to get PVC"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, _, err := resolveMigrationTargetPolicyID(ctx, "ns", tc.pvc, syncer)
			if err == nil || !strings.Contains(err.Error(), tc.wantErr) {
				t.Fatalf("got %v, want error containing %q", err, tc.wantErr)
			}
		})
	}

	t.Run("nil syncer errors", func(t *testing.T) {
		if _, _, err := resolveMigrationTargetPolicyID(ctx, "ns", "pvc-gold", nil); err == nil {
			t.Fatal("expected error for nil syncer")
		}
	})
}

// TestGetMigrationDynamicClientDirect exercises the real sync.Once-guarded
// constructor. Outside a cluster, config.GetConfig() fails; we just assert
// the function returns (a nil client, an error) and does not panic. This
// covers the code path that the migrationDynamicClientGetter seam normally
// short-circuits in other tests.
func TestGetMigrationDynamicClientDirect(t *testing.T) {
	// Save and restore the sync.Once + result so we don't poison other tests.
	origOnce := migrationDynamicClientOnce
	origClient := migrationDynamicClient
	origErr := migrationDynamicClientErr
	t.Cleanup(func() {
		migrationDynamicClientOnce = origOnce
		migrationDynamicClient = origClient
		migrationDynamicClientErr = origErr
	})
	// Force a fresh attempt by replacing the sync.Once pointer.
	migrationDynamicClientOnce = &sync.Once{}
	migrationDynamicClient = nil
	migrationDynamicClientErr = nil

	client, err := getMigrationDynamicClient()
	// In a unit-test process there's no kubeconfig and no in-cluster service
	// account; we expect an error. The exact error message is environment-
	// dependent, so just assert the contract: err != nil OR client != nil.
	if err == nil && client == nil {
		t.Fatal("expected either a client or an error; got both nil")
	}
	// Second call returns the same cached result (sync.Once semantics).
	client2, err2 := getMigrationDynamicClient()
	if client != client2 || (err == nil) != (err2 == nil) {
		t.Fatalf("sync.Once not honored: got (%v, %v) then (%v, %v)", client, err, client2, err2)
	}
}

// TestPropagateMigrationSuccessFailures exercises the early-return branches
// of propagateMigrationSuccess that aren't reached by the happy-path tests.
func TestPropagateMigrationSuccessFailures(t *testing.T) {
	resetMigrationStateForTest(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	pvcEmpty := makePVC("ns", "pvc-empty", "", "sc", "")
	pvNoCSI := &v1.PersistentVolume{
		ObjectMeta: metav1.ObjectMeta{Name: "pv-nocsi"},
		Spec:       v1.PersistentVolumeSpec{},
	}
	pvcRefNoCSI := makePVC("ns", "pvc-nocsi", "pv-nocsi", "sc", "")
	pvcRefMissingPV := makePVC("ns", "pvc-no-pv", "pv-absent", "sc", "")
	pvcGood := makePVC("ns", "pvc-1", "pv-1", "sc-old", "vac-gold")
	pvGood := makePV("pv-1", "csi-handle-1")

	syncer, _ := newSyncerWithListers(t, ctx,
		[]*v1.PersistentVolumeClaim{pvcEmpty, pvcRefNoCSI, pvcRefMissingPV, pvcGood},
		[]*v1.PersistentVolume{pvNoCSI, pvGood},
	)

	seedCVI := &cnsvolumeinfov1alpha1.CNSVolumeInfo{
		ObjectMeta: metav1.ObjectMeta{
			Name:      cnsvolumeinfo.GetCnsVolumeInfoCrName("csi-handle-1"),
			Namespace: common.GetCSINamespace(),
		},
		Spec: cnsvolumeinfov1alpha1.CNSVolumeInfoSpec{
			VolumeID: "csi-handle-1", StorageClassName: "sc-old", StoragePolicyID: "policy-uuid-silver",
		},
	}
	syncer.cnsOperatorClient = makeFakeCNSClient(seedCVI)

	cr := &unstructured.Unstructured{Object: map[string]interface{}{
		"kind": common.MigrationCRKindVolume,
		"spec": map[string]interface{}{"storagePolicyID": "policy-uuid-gold"},
	}}

	t.Run("PVC with empty VolumeName -> 'no bound volume yet'", func(t *testing.T) {
		err := propagateMigrationSuccess(ctx, syncer, listerPVC(t, syncer, "ns", "pvc-empty"), cr)
		if err == nil || !strings.Contains(err.Error(), "no bound volume") {
			t.Fatalf("unexpected error: %v", err)
		}
	})

	t.Run("PV missing from lister -> 'failed to get PV'", func(t *testing.T) {
		err := propagateMigrationSuccess(ctx, syncer, listerPVC(t, syncer, "ns", "pvc-no-pv"), cr)
		if err == nil || !strings.Contains(err.Error(), "failed to get PV") {
			t.Fatalf("unexpected error: %v", err)
		}
	})

	t.Run("PV missing CSI section -> 'no CSI volume handle'", func(t *testing.T) {
		err := propagateMigrationSuccess(ctx, syncer, listerPVC(t, syncer, "ns", "pvc-nocsi"), cr)
		if err == nil || !strings.Contains(err.Error(), "no CSI volume handle") {
			t.Fatalf("unexpected error: %v", err)
		}
	})

	t.Run("getVolumeInfoService error propagates", func(t *testing.T) {
		volumeInfoService = nil
		migrationInitVolumeInfoService = func(_ context.Context) (cnsvolumeinfo.VolumeInfoService, error) {
			return nil, errors.New("init failed")
		}
		err := propagateMigrationSuccess(ctx, syncer, listerPVC(t, syncer, "ns", "pvc-1"), cr)
		if err == nil || !strings.Contains(err.Error(), "init failed") {
			t.Fatalf("unexpected error: %v", err)
		}
	})

	t.Run("GetVolumeInfoForVolumeID error propagates", func(t *testing.T) {
		fakeSvc := newFakeVolumeInfoService()
		fakeSvc.getErr = errors.New("get failed")
		volumeInfoService = fakeSvc
		err := propagateMigrationSuccess(ctx, syncer, listerPVC(t, syncer, "ns", "pvc-1"), cr)
		if err == nil || !strings.Contains(err.Error(), "get failed") {
			t.Fatalf("unexpected error: %v", err)
		}
	})

	t.Run("spec patch error propagates", func(t *testing.T) {
		fakeSvc := newFakeVolumeInfoService()
		fakeSvc.infoByID["csi-handle-1"] = &cnsvolumeinfov1alpha1.CNSVolumeInfo{
			Spec: cnsvolumeinfov1alpha1.CNSVolumeInfoSpec{
				VolumeID: "csi-handle-1", StorageClassName: "sc-old", StoragePolicyID: "policy-uuid-silver",
			},
		}
		volumeInfoService = fakeSvc
		migrationHandleStoragePolicyChange = func(
			_ context.Context, _ client.Client,
			_, _ cnsvolumeinfov1alpha1.CNSVolumeInfo) {
		}
		syncer.cnsOperatorClient = makeFakeCNSClientWithInterceptor(interceptor.Funcs{
			Patch: func(_ context.Context, _ client.WithWatch, _ client.Object,
				_ client.Patch, _ ...client.PatchOption) error {
				return errors.New("spec patch fail")
			},
		}, seedCVI)
		err := propagateMigrationSuccess(ctx, syncer, listerPVC(t, syncer, "ns", "pvc-1"), cr)
		if err == nil || !strings.Contains(err.Error(), "spec patch fail") {
			t.Fatalf("unexpected error: %v", err)
		}
	})
}

// TestPatchMigrationConditionsTypeClientError covers the path where the
// cnsOperatorClient Status patch returns an error.
// (Previously this tested getVolumeInfoService failure; the patch path now
// uses cnsOperatorClient directly.)
func TestPatchMigrationConditionsTypeClientError(t *testing.T) {
	resetMigrationStateForTest(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	pvc := makePVC("ns", "pvc-1", "pv-1", "sc", "")
	pv := makePV("pv-1", "csi-handle-1")
	syncer, _ := newSyncerWithListers(t, ctx,
		[]*v1.PersistentVolumeClaim{pvc}, []*v1.PersistentVolume{pv})

	seedCVI := &cnsvolumeinfov1alpha1.CNSVolumeInfo{
		ObjectMeta: metav1.ObjectMeta{
			Name:      cnsvolumeinfo.GetCnsVolumeInfoCrName("csi-handle-1"),
			Namespace: common.GetCSINamespace(),
		},
	}
	syncer.cnsOperatorClient = makeFakeCNSClientWithInterceptor(interceptor.Funcs{
		SubResourcePatch: func(_ context.Context, _ client.Client, _ string,
			_ client.Object, _ client.Patch, _ ...client.SubResourcePatchOption) error {
			return errors.New("client patch err")
		},
	}, seedCVI)

	err := patchMigrationConditionsType(ctx, "ns", "pvc-1",
		cnsvolumeinfov1alpha1.MigrationConditionInProgress, "policy-uuid-gold", syncer)
	if err == nil || !strings.Contains(err.Error(), "client patch err") {
		t.Fatalf("expected client patch error, got: %v", err)
	}
}

// TestClassifyMigrationCRSkipsNonMapEntry exercises the defensive `continue`
// when a status.conditions[] entry isn't a map (would be a CRD violation but
// we don't panic on it).
func TestClassifyMigrationCRSkipsNonMapEntry(t *testing.T) {
	cr := &unstructured.Unstructured{Object: map[string]interface{}{
		"kind":     common.MigrationCRKindVolume,
		"metadata": map[string]interface{}{"name": "cr-1", "generation": int64(1)},
		"spec":     map[string]interface{}{"storagePolicyID": "policy-uuid-gold"},
		"status": map[string]interface{}{
			"conditions": []interface{}{
				"not-a-map", // skipped
				map[string]interface{}{"type": "Ready", "status": "True", "reason": "Succeeded",
					"observedGeneration": int64(1)},
			},
		},
	}}
	if got := classifyMigrationCR(cr, "pvc-1", "policy-uuid-gold"); got != migrationOutcomeComplete {
		t.Fatalf("got %v, want Complete", got)
	}
}

// TestLookupNewStoragePolicyIDSkipsNonMapVolumeEntry covers the defensive
// continue path inside the VMInfra volumes slice walk.
func TestLookupNewStoragePolicyIDSkipsNonMapVolumeEntry(t *testing.T) {
	cr := &unstructured.Unstructured{Object: map[string]interface{}{
		"kind": common.MigrationCRKindVMInfra,
		"spec": map[string]interface{}{
			"targetStorage": map[string]interface{}{
				"volumes": []interface{}{
					"not-a-map", // skipped
					map[string]interface{}{"pvcName": "pvc-1", "storagePolicyID": "policy-2"},
				},
			},
		},
	}}
	if got := lookupNewStoragePolicyID(cr, "pvc-1"); got != "policy-2" {
		t.Fatalf("got %q, want policy-2", got)
	}
}

// TestVolumeIDForPVCMissingPVFallback ensures the path where PVC is in cache
// but PV is not (and the fallback k8s client is exercised) is covered.
func TestVolumeIDForPVCMissingPVFallback(t *testing.T) {
	resetMigrationStateForTest(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	pvc := makePVC("ns", "pvc-1", "pv-1", "sc", "")
	pv := makePV("pv-1", "csi-handle-1")

	// Lister contains PVC but NOT the PV - simulates a race where the
	// PVC was synced but the PV informer hasn't caught up.
	syncer, _ := newSyncerWithListers(t, ctx,
		[]*v1.PersistentVolumeClaim{pvc}, nil)

	// Stub k8s fallback to return the PV.
	client := k8sfake.NewClientset(pv)
	migrationK8sNewClient = func(_ context.Context) (clientset.Interface, error) {
		return client, nil
	}

	got, err := volumeIDForPVC(ctx, "ns", "pvc-1", syncer)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if got != "csi-handle-1" {
		t.Fatalf("got %q, want csi-handle-1", got)
	}
}

// TestVolumeIDForPVCPVFallbackErrors covers the two PV-fallback failure paths.
func TestVolumeIDForPVCPVFallbackErrors(t *testing.T) {
	resetMigrationStateForTest(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	pvc := makePVC("ns", "pvc-1", "pv-1", "sc", "")
	syncer, _ := newSyncerWithListers(t, ctx,
		[]*v1.PersistentVolumeClaim{pvc}, nil)

	t.Run("PV client factory error", func(t *testing.T) {
		migrationK8sNewClient = func(_ context.Context) (clientset.Interface, error) {
			return nil, errors.New("no client")
		}
		_, err := volumeIDForPVC(ctx, "ns", "pvc-1", syncer)
		if err == nil || !strings.Contains(err.Error(), "failed to create k8s client") {
			t.Fatalf("unexpected error: %v", err)
		}
	})

	t.Run("PV not found via fallback", func(t *testing.T) {
		client := k8sfake.NewClientset() // no PV
		migrationK8sNewClient = func(_ context.Context) (clientset.Interface, error) {
			return client, nil
		}
		_, err := volumeIDForPVC(ctx, "ns", "pvc-1", syncer)
		if err == nil || !strings.Contains(err.Error(), "failed to get PV") {
			t.Fatalf("unexpected error: %v", err)
		}
	})
}

// listerPVC returns the PVC snapshot from the syncer's lister, as the watcher
// passes it to propagateMigrationSuccess.
func listerPVC(t *testing.T, syncer *metadataSyncInformer, ns, name string) *v1.PersistentVolumeClaim {
	t.Helper()
	pvc, err := syncer.pvcLister.PersistentVolumeClaims(ns).Get(name)
	if err != nil {
		t.Fatalf("listerPVC: Get(%s/%s) failed: %v", ns, name, err)
	}
	return pvc
}
