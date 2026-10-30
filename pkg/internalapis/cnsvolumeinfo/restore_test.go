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

package cnsvolumeinfo

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	apiextensionsv1 "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	apimeta "k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"
	"sigs.k8s.io/yaml"

	cnsvolumeinfoconfig "sigs.k8s.io/vsphere-csi-driver/v3/pkg/internalapis/cnsvolumeinfo/config"
	cnsvolumeinfov1alpha1 "sigs.k8s.io/vsphere-csi-driver/v3/pkg/internalapis/cnsvolumeinfo/v1alpha1"
)

const (
	testRestoreVolumeID  = "3c6b9a1e-5d2f-4f4e-9b8a-2a71c0e5d9f4"
	testRestoreNamespace = "ns1"
	testRestoreUID       = "9b1f0c3d-4e5a-4b6c-8d7e-0f1a2b3c7a2e"
	testRestoreSizeBytes = int64(5368709120)
)

// restoreHarness runs the restore methods against a fake API server and records the calls made.
type restoreHarness struct {
	t       *testing.T
	service *volumeInfo
	backend client.Client

	// Hooks run before the call reaches the fake API server; a non-nil error fails the call.
	beforeDVIGet      func() error
	beforePatch       func(attempt int) error
	beforeStatusPatch func(attempt int) error

	mu                sync.Mutex
	dviGets           int
	dviWrites         int
	creates           int
	patches           int
	statusPatches     int
	statusPatchBodies []string
}

func newRestoreHarness(t *testing.T, objs ...client.Object) *restoreHarness {
	t.Helper()
	scheme := runtime.NewScheme()
	require.NoError(t, cnsvolumeinfov1alpha1.AddToScheme(scheme))
	h := &restoreHarness{t: t}
	isDVI := func(obj client.Object) bool {
		_, ok := obj.(*cnsvolumeinfov1alpha1.CnsDeletedVolumeInfo)
		return ok
	}
	c := fake.NewClientBuilder().WithScheme(scheme).WithObjects(objs...).
		WithStatusSubresource(&cnsvolumeinfov1alpha1.CNSVolumeInfo{}).
		WithInterceptorFuncs(interceptor.Funcs{
			Get: func(ctx context.Context, c client.WithWatch, key client.ObjectKey, obj client.Object,
				opts ...client.GetOption) error {
				if isDVI(obj) {
					h.mu.Lock()
					h.dviGets++
					h.mu.Unlock()
					if h.beforeDVIGet != nil {
						if err := h.beforeDVIGet(); err != nil {
							return err
						}
					}
				}
				return c.Get(ctx, key, obj, opts...)
			},
			Create: func(ctx context.Context, c client.WithWatch, obj client.Object,
				opts ...client.CreateOption) error {
				h.mu.Lock()
				if isDVI(obj) {
					h.dviWrites++
				}
				h.creates++
				h.mu.Unlock()
				return c.Create(ctx, obj, opts...)
			},
			Update: func(ctx context.Context, c client.WithWatch, obj client.Object,
				opts ...client.UpdateOption) error {
				if isDVI(obj) {
					h.mu.Lock()
					h.dviWrites++
					h.mu.Unlock()
				}
				return c.Update(ctx, obj, opts...)
			},
			Delete: func(ctx context.Context, c client.WithWatch, obj client.Object,
				opts ...client.DeleteOption) error {
				if isDVI(obj) {
					h.mu.Lock()
					h.dviWrites++
					h.mu.Unlock()
				}
				return c.Delete(ctx, obj, opts...)
			},
			Patch: func(ctx context.Context, c client.WithWatch, obj client.Object, patch client.Patch,
				opts ...client.PatchOption) error {
				h.mu.Lock()
				if isDVI(obj) {
					h.dviWrites++
				}
				h.patches++
				attempt := h.patches
				h.mu.Unlock()
				if h.beforePatch != nil {
					if err := h.beforePatch(attempt); err != nil {
						return err
					}
				}
				return c.Patch(ctx, obj, patch, opts...)
			},
			SubResourcePatch: func(ctx context.Context, c client.Client, subResourceName string,
				obj client.Object, patch client.Patch, opts ...client.SubResourcePatchOption) error {
				body, err := patch.Data(obj)
				require.NoError(t, err)
				h.mu.Lock()
				h.statusPatches++
				attempt := h.statusPatches
				h.statusPatchBodies = append(h.statusPatchBodies, string(body))
				h.mu.Unlock()
				if h.beforeStatusPatch != nil {
					if err := h.beforeStatusPatch(attempt); err != nil {
						return err
					}
				}
				return c.SubResource(subResourceName).Patch(ctx, obj, patch, opts...)
			},
		}).Build()
	h.backend = c
	h.service = &volumeInfo{k8sClient: c}
	return h
}

func testRestoreDVI(namespace string, sizeBytes int64) *cnsvolumeinfov1alpha1.CnsDeletedVolumeInfo {
	return &cnsvolumeinfov1alpha1.CnsDeletedVolumeInfo{
		ObjectMeta: metav1.ObjectMeta{Name: testRestoreVolumeID, Namespace: csiNamespace},
		Spec: cnsvolumeinfov1alpha1.CnsDeletedVolumeInfoSpec{
			VolumeID:         testRestoreVolumeID,
			Namespace:        namespace,
			StorageClassName: "sc-gold",
			StoragePolicyID:  "policy-gold",
			SizeBytes:        sizeBytes,
		},
	}
}

func testRestoreCVI(mutate func(*cnsvolumeinfov1alpha1.CNSVolumeInfo)) *cnsvolumeinfov1alpha1.CNSVolumeInfo {
	cvi := &cnsvolumeinfov1alpha1.CNSVolumeInfo{
		ObjectMeta: metav1.ObjectMeta{Name: testRestoreVolumeID, Namespace: csiNamespace},
		Spec: cnsvolumeinfov1alpha1.CNSVolumeInfoSpec{
			VolumeID:         testRestoreVolumeID,
			Namespace:        testRestoreNamespace,
			VCenterServer:    "vc1.example.com",
			StoragePolicyID:  "policy-gold",
			StorageClassName: "sc-gold",
		},
	}
	if mutate != nil {
		mutate(cvi)
	}
	return cvi
}

func stamped(uid string) func(*cnsvolumeinfov1alpha1.CNSVolumeInfo) {
	return func(cvi *cnsvolumeinfov1alpha1.CNSVolumeInfo) {
		cvi.Annotations = map[string]string{cnsvolumeinfov1alpha1.AnnRestoredByCnsRegisterVolume: uid}
	}
}

func testRestoredVolumeInfo() RestoredVolumeInfo {
	capacity := resource.MustParse("20Gi")
	return RestoredVolumeInfo{
		VolumeID:         testRestoreVolumeID,
		Namespace:        testRestoreNamespace,
		StoragePolicyID:  "policy-gold",
		StorageClassName: "sc-gold",
		VCenter:          "vc1.example.com",
		Capacity:         &capacity,
		RestoredBy:       testRestoreUID,
	}
}

func (h *restoreHarness) cvi() *cnsvolumeinfov1alpha1.CNSVolumeInfo {
	h.t.Helper()
	cvi := &cnsvolumeinfov1alpha1.CNSVolumeInfo{}
	require.NoError(h.t, h.backend.Get(context.Background(),
		types.NamespacedName{Namespace: csiNamespace, Name: testRestoreVolumeID}, cvi))
	return cvi
}

func (h *restoreHarness) cviExists() bool {
	h.t.Helper()
	err := h.backend.Get(context.Background(),
		types.NamespacedName{Namespace: csiNamespace, Name: testRestoreVolumeID},
		&cnsvolumeinfov1alpha1.CNSVolumeInfo{})
	if apierrors.IsNotFound(err) {
		return false
	}
	require.NoError(h.t, err)
	return true
}

func (h *restoreHarness) deleteDVI() {
	h.t.Helper()
	require.NoError(h.t, h.backend.Delete(context.Background(), testRestoreDVI(testRestoreNamespace, 0)))
	h.mu.Lock()
	h.dviWrites--
	h.mu.Unlock()
}

func restoreConditionOf(t *testing.T, cvi *cnsvolumeinfov1alpha1.CNSVolumeInfo) *metav1.Condition {
	t.Helper()
	return apimeta.FindStatusCondition(cvi.Status.Conditions, cnsvolumeinfov1alpha1.ConditionSnapshotSizeRestored)
}

func TestPrecheckRestore(t *testing.T) {
	ctx := context.Background()
	readErr := apierrors.NewServiceUnavailable("CRD not served")
	for _, tc := range []struct {
		name        string
		volumeID    string
		objs        []client.Object
		dviGetErr   error
		wantErr     string
		wantDVIGets int
	}{
		{name: "no volume ID", volumeID: "", wantErr: "restore requires spec.volumeID"},
		{name: "deleted volume with DVI", volumeID: testRestoreVolumeID,
			objs: []client.Object{testRestoreDVI(testRestoreNamespace, testRestoreSizeBytes)}, wantDVIGets: 1},
		{name: "DVI with zero size passes", volumeID: testRestoreVolumeID,
			objs: []client.Object{testRestoreDVI(testRestoreNamespace, 0)}, wantDVIGets: 1},
		{name: "DVI not found", volumeID: testRestoreVolumeID,
			wantErr: "restore prerequisite not met: CnsDeletedVolumeInfo " + csiNamespace + "/" +
				testRestoreVolumeID + " not found", wantDVIGets: 1},
		{name: "namespace mismatch", volumeID: testRestoreVolumeID,
			objs: []client.Object{testRestoreDVI("ns2", testRestoreSizeBytes)},
			wantErr: "restore prerequisite not met: CnsDeletedVolumeInfo " + csiNamespace + "/" +
				testRestoreVolumeID + " belongs to namespace ns2, not ns1", wantDVIGets: 1},
		{name: "DVI read error", volumeID: testRestoreVolumeID, dviGetErr: readErr,
			wantErr:     "failed to read CnsDeletedVolumeInfo " + csiNamespace + "/" + testRestoreVolumeID,
			wantDVIGets: 1},
		{name: "live CVI stamped by this registration passes without a DVI read", volumeID: testRestoreVolumeID,
			objs: []client.Object{testRestoreCVI(stamped(testRestoreUID))}},
		{name: "live CVI without stamp fails", volumeID: testRestoreVolumeID,
			objs: []client.Object{testRestoreCVI(nil), testRestoreDVI(testRestoreNamespace, testRestoreSizeBytes)},
			wantErr: "restore requires a deleted volume: CnsVolumeInfo " + csiNamespace + "/" +
				testRestoreVolumeID + " is live"},
		{name: "live CVI stamped by another registration fails", volumeID: testRestoreVolumeID,
			objs: []client.Object{testRestoreCVI(stamped("other-uid")),
				testRestoreDVI(testRestoreNamespace, testRestoreSizeBytes)},
			wantErr: "is live"},
		{name: "terminating CVI with DVI passes", volumeID: testRestoreVolumeID,
			objs: []client.Object{
				testRestoreCVI(func(cvi *cnsvolumeinfov1alpha1.CNSVolumeInfo) {
					now := metav1.Now()
					cvi.DeletionTimestamp = &now
					cvi.Finalizers = []string{cnsvolumeinfov1alpha1.SnapServiceFinalizer}
				}),
				testRestoreDVI(testRestoreNamespace, testRestoreSizeBytes)},
			wantDVIGets: 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			h := newRestoreHarness(t, tc.objs...)
			if tc.dviGetErr != nil {
				h.beforeDVIGet = func() error { return tc.dviGetErr }
			}
			err := h.service.PrecheckRestore(ctx, tc.volumeID, testRestoreNamespace, testRestoreUID)
			if tc.wantErr == "" {
				require.NoError(t, err)
			} else {
				require.ErrorContains(t, err, tc.wantErr)
			}
			assert.Equal(t, tc.wantDVIGets, h.dviGets)
			assert.Zero(t, h.creates+h.patches+h.statusPatches, "the precheck must not write")
		})
	}
}

func TestPrecheckRestoreRequiresStamp(t *testing.T) {
	h := newRestoreHarness(t, testRestoreCVI(nil))
	err := h.service.PrecheckRestore(context.Background(), testRestoreVolumeID, testRestoreNamespace, "")
	require.Error(t, err, "an empty stamp must not match a CVI without a stamp")
}

func TestPrecheckRestorePassesOnceDVIAppears(t *testing.T) {
	ctx := context.Background()
	h := newRestoreHarness(t)
	require.Error(t, h.service.PrecheckRestore(ctx, testRestoreVolumeID, testRestoreNamespace, testRestoreUID))
	require.NoError(t, h.backend.Create(ctx, testRestoreDVI(testRestoreNamespace, testRestoreSizeBytes)))
	require.NoError(t, h.service.PrecheckRestore(ctx, testRestoreVolumeID, testRestoreNamespace, testRestoreUID))
}

func TestCreateOrUpdateRestoredVolumeInfoSeedsOnCreate(t *testing.T) {
	ctx := context.Background()
	h := newRestoreHarness(t, testRestoreDVI(testRestoreNamespace, testRestoreSizeBytes))
	before := time.Now().Add(-time.Second)

	result, err := h.service.CreateOrUpdateRestoredVolumeInfo(ctx, testRestoredVolumeInfo())
	require.NoError(t, err)
	assert.True(t, result.Seeded)
	assert.True(t, result.SnapshotSizeRestored())

	cvi := h.cvi()
	assert.Equal(t, 1, h.creates, "the seed must be written by the Create")
	assert.Zero(t, h.patches)
	require.NotNil(t, cvi.Spec.AggregatedSnapshotSize)
	assert.Equal(t, testRestoreSizeBytes, cvi.Spec.AggregatedSnapshotSize.Value())
	assert.True(t, cvi.Spec.ValidAggregatedSnapshotSize)
	assert.True(t, cvi.Spec.SnapshotLatestOperationCompleteTime.After(before))
	assert.Contains(t, cvi.Finalizers, cnsvolumeinfov1alpha1.SnapServiceFinalizer)
	assert.Equal(t, testRestoreUID, cvi.Annotations[cnsvolumeinfov1alpha1.AnnRestoredByCnsRegisterVolume])
	assert.Equal(t, testRestoreNamespace, cvi.Spec.Namespace)
	assert.Equal(t, "sc-gold", cvi.Spec.StorageClassName)
	assert.Equal(t, "policy-gold", cvi.Spec.StoragePolicyID)
	assert.Equal(t, "vc1.example.com", cvi.Spec.VCenterServer)
	require.NotNil(t, cvi.Spec.Capacity)
	assert.Equal(t, "20Gi", cvi.Spec.Capacity.String())

	condition := restoreConditionOf(t, cvi)
	require.NotNil(t, condition)
	assert.Equal(t, metav1.ConditionTrue, condition.Status)
	assert.Equal(t, cnsvolumeinfov1alpha1.ReasonSeededFromDeletedVolumeInfo, condition.Reason)
	assert.Contains(t, condition.Message, "seeded 5368709120 bytes")
	assert.Zero(t, h.dviWrites, "CSI must never write or delete a DVI")
}

func TestCreateOrUpdateRestoredVolumeInfoWithoutMatchingDVI(t *testing.T) {
	for _, tc := range []struct {
		name       string
		objs       []client.Object
		wantReason string
	}{
		{name: "no DVI", wantReason: cnsvolumeinfov1alpha1.ReasonDeletedVolumeInfoNotFound},
		{name: "DVI in another namespace", objs: []client.Object{testRestoreDVI("ns2", testRestoreSizeBytes)},
			wantReason: cnsvolumeinfov1alpha1.ReasonDeletedVolumeInfoNamespaceMismatch},
		{name: "DVI of another volume", objs: []client.Object{
			func() client.Object {
				dvi := testRestoreDVI(testRestoreNamespace, testRestoreSizeBytes)
				dvi.Spec.VolumeID = "other-volume"
				return dvi
			}()},
			wantReason: cnsvolumeinfov1alpha1.ReasonDeletedVolumeInfoNotFound},
	} {
		t.Run(tc.name, func(t *testing.T) {
			h := newRestoreHarness(t, tc.objs...)
			result, err := h.service.CreateOrUpdateRestoredVolumeInfo(context.Background(), testRestoredVolumeInfo())
			require.NoError(t, err)
			assert.False(t, result.Seeded)
			assert.False(t, result.SnapshotSizeRestored())
			assert.Equal(t, tc.wantReason, result.Condition.Reason)

			cvi := h.cvi()
			assert.Nil(t, cvi.Spec.AggregatedSnapshotSize)
			assert.False(t, cvi.Spec.ValidAggregatedSnapshotSize)
			assert.Contains(t, cvi.Finalizers, cnsvolumeinfov1alpha1.SnapServiceFinalizer)
			assert.Equal(t, testRestoreUID, cvi.Annotations[cnsvolumeinfov1alpha1.AnnRestoredByCnsRegisterVolume])
			condition := restoreConditionOf(t, cvi)
			require.NotNil(t, condition)
			assert.Equal(t, metav1.ConditionFalse, condition.Status)
			assert.Equal(t, tc.wantReason, condition.Reason)
		})
	}
}

func TestCreateOrUpdateRestoredVolumeInfoDVIReadErrorWritesNothing(t *testing.T) {
	h := newRestoreHarness(t)
	h.beforeDVIGet = func() error { return apierrors.NewServiceUnavailable("CRD not served") }
	_, err := h.service.CreateOrUpdateRestoredVolumeInfo(context.Background(), testRestoredVolumeInfo())
	require.Error(t, err)
	assert.False(t, h.cviExists())
	assert.Zero(t, h.creates+h.patches+h.statusPatches)
}

func TestCreateOrUpdateRestoredVolumeInfoRequiresStamp(t *testing.T) {
	h := newRestoreHarness(t, testRestoreDVI(testRestoreNamespace, testRestoreSizeBytes))
	info := testRestoredVolumeInfo()
	info.RestoredBy = ""
	_, err := h.service.CreateOrUpdateRestoredVolumeInfo(context.Background(), info)
	require.Error(t, err)
	assert.False(t, h.cviExists())
}

func TestCreateOrUpdateRestoredVolumeInfoTerminatingCVIIsAnError(t *testing.T) {
	terminating := testRestoreCVI(func(cvi *cnsvolumeinfov1alpha1.CNSVolumeInfo) {
		now := metav1.Now()
		cvi.DeletionTimestamp = &now
		cvi.Finalizers = []string{cnsvolumeinfov1alpha1.SnapServiceFinalizer}
	})
	h := newRestoreHarness(t, terminating, testRestoreDVI(testRestoreNamespace, testRestoreSizeBytes))
	_, err := h.service.CreateOrUpdateRestoredVolumeInfo(context.Background(), testRestoredVolumeInfo())
	require.ErrorContains(t, err, "still terminating")
	assert.Zero(t, h.creates+h.patches+h.statusPatches)
}

func TestCreateOrUpdateRestoredVolumeInfoUpdatesLiveCVI(t *testing.T) {
	existingSize := resource.MustParse("3Gi")
	for _, tc := range []struct {
		name         string
		existing     *cnsvolumeinfov1alpha1.CNSVolumeInfo
		wantSeeded   bool
		wantSize     int64
		wantPatches  int
		wantStatus   metav1.ConditionStatus
		wantReason   string
		otherFinal   bool
		wantStampSet bool
	}{
		{
			name:        "live CVI without a snapshot size gets the seed",
			existing:    testRestoreCVI(nil),
			wantSeeded:  true,
			wantSize:    testRestoreSizeBytes,
			wantPatches: 1,
			wantStatus:  metav1.ConditionTrue,
			wantReason:  cnsvolumeinfov1alpha1.ReasonSeededFromDeletedVolumeInfo,
		},
		{
			name: "live CVI with a snapshot size keeps it",
			existing: testRestoreCVI(func(cvi *cnsvolumeinfov1alpha1.CNSVolumeInfo) {
				cvi.Finalizers = []string{"other.example.com/finalizer"}
				cvi.Spec.AggregatedSnapshotSize = &existingSize
				cvi.Spec.ValidAggregatedSnapshotSize = true
			}),
			wantSize:    existingSize.Value(),
			wantPatches: 1,
			wantStatus:  metav1.ConditionTrue,
			wantReason:  cnsvolumeinfov1alpha1.ReasonSeededFromDeletedVolumeInfo,
			otherFinal:  true,
		},
		{
			name: "complete CVI is not patched",
			existing: testRestoreCVI(func(cvi *cnsvolumeinfov1alpha1.CNSVolumeInfo) {
				stamped(testRestoreUID)(cvi)
				cvi.Finalizers = []string{cnsvolumeinfov1alpha1.SnapServiceFinalizer}
				cvi.Spec.AggregatedSnapshotSize = &existingSize
				cvi.Spec.ValidAggregatedSnapshotSize = true
			}),
			wantSize:   existingSize.Value(),
			wantStatus: metav1.ConditionTrue,
			wantReason: cnsvolumeinfov1alpha1.ReasonSeededFromDeletedVolumeInfo,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			h := newRestoreHarness(t, tc.existing, testRestoreDVI(testRestoreNamespace, testRestoreSizeBytes))
			result, err := h.service.CreateOrUpdateRestoredVolumeInfo(context.Background(), testRestoredVolumeInfo())
			require.NoError(t, err)
			assert.Equal(t, tc.wantSeeded, result.Seeded)
			assert.Zero(t, h.creates)
			assert.Equal(t, tc.wantPatches, h.patches)

			cvi := h.cvi()
			require.NotNil(t, cvi.Spec.AggregatedSnapshotSize)
			assert.Equal(t, tc.wantSize, cvi.Spec.AggregatedSnapshotSize.Value())
			assert.Contains(t, cvi.Finalizers, cnsvolumeinfov1alpha1.SnapServiceFinalizer)
			if tc.otherFinal {
				assert.Contains(t, cvi.Finalizers, "other.example.com/finalizer")
			}
			assert.Equal(t, testRestoreUID, cvi.Annotations[cnsvolumeinfov1alpha1.AnnRestoredByCnsRegisterVolume])
			condition := restoreConditionOf(t, cvi)
			require.NotNil(t, condition)
			assert.Equal(t, tc.wantStatus, condition.Status)
			assert.Equal(t, tc.wantReason, condition.Reason)
		})
	}
}

func TestCreateOrUpdateRestoredVolumeInfoNeverReplacesAnotherStamp(t *testing.T) {
	h := newRestoreHarness(t, testRestoreCVI(stamped("other-uid")),
		testRestoreDVI(testRestoreNamespace, testRestoreSizeBytes))
	_, err := h.service.CreateOrUpdateRestoredVolumeInfo(context.Background(), testRestoredVolumeInfo())
	require.ErrorContains(t, err, `was restored by CnsRegisterVolume "other-uid"`)
	assert.Zero(t, h.patches)
	assert.Zero(t, h.statusPatches)
	cvi := h.cvi()
	assert.Equal(t, "other-uid", cvi.Annotations[cnsvolumeinfov1alpha1.AnnRestoredByCnsRegisterVolume])
	assert.Nil(t, cvi.Spec.AggregatedSnapshotSize, "the CVI of another restore must not be seeded")
}

func TestGetRestoredBy(t *testing.T) {
	now := metav1.Now()
	for _, tc := range []struct {
		name     string
		existing *cnsvolumeinfov1alpha1.CNSVolumeInfo
		want     string
	}{
		{name: "no CVI"},
		{name: "CVI without a stamp", existing: testRestoreCVI(nil)},
		{name: "stamped CVI", existing: testRestoreCVI(stamped(testRestoreUID)), want: testRestoreUID},
		{name: "terminating CVI", existing: testRestoreCVI(func(cvi *cnsvolumeinfov1alpha1.CNSVolumeInfo) {
			stamped(testRestoreUID)(cvi)
			cvi.DeletionTimestamp = &now
			cvi.Finalizers = []string{cnsvolumeinfov1alpha1.SnapServiceFinalizer}
		})},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var objs []client.Object
			if tc.existing != nil {
				objs = append(objs, tc.existing)
			}
			h := newRestoreHarness(t, objs...)
			got, err := h.service.GetRestoredBy(context.Background(), testRestoreVolumeID)
			require.NoError(t, err)
			assert.Equal(t, tc.want, got)
		})
	}
}

func TestCreateOrUpdateRestoredVolumeInfoRetriesPatchConflict(t *testing.T) {
	h := newRestoreHarness(t, testRestoreCVI(nil), testRestoreDVI(testRestoreNamespace, testRestoreSizeBytes))
	h.beforePatch = func(attempt int) error {
		if attempt == 1 {
			return apierrors.NewConflict(cnsvolumeinfov1alpha1.Resource("cnsvolumeinfoes"),
				testRestoreVolumeID, errors.New("changed"))
		}
		return nil
	}
	result, err := h.service.CreateOrUpdateRestoredVolumeInfo(context.Background(), testRestoredVolumeInfo())
	require.NoError(t, err)
	assert.True(t, result.Seeded)
	assert.Equal(t, 2, h.patches)
	assert.Equal(t, testRestoreSizeBytes, h.cvi().Spec.AggregatedSnapshotSize.Value())
}

func TestCreateOrUpdateRestoredVolumeInfoPatchCarriesResourceVersion(t *testing.T) {
	h := newRestoreHarness(t, testRestoreCVI(nil), testRestoreDVI(testRestoreNamespace, testRestoreSizeBytes))
	h.beforePatch = func(attempt int) error {
		if attempt == 1 {
			// A concurrent writer changes the CVI after CSI read it; the stale patch must be rejected.
			cvi := h.cvi()
			size := resource.MustParse("7Gi")
			cvi.Spec.AggregatedSnapshotSize = &size
			cvi.Spec.ValidAggregatedSnapshotSize = true
			require.NoError(t, h.backend.Update(context.Background(), cvi))
		}
		return nil
	}
	result, err := h.service.CreateOrUpdateRestoredVolumeInfo(context.Background(), testRestoredVolumeInfo())
	require.NoError(t, err)
	assert.False(t, result.Seeded, "the concurrent value must not be replaced by the seed")
	assert.Equal(t, int64(7*1024*1024*1024), h.cvi().Spec.AggregatedSnapshotSize.Value())
	assert.Equal(t, 2, h.patches)
}

func TestCreateOrUpdateRestoredVolumeInfoRetryAfterDVIDeletedKeepsTrue(t *testing.T) {
	ctx := context.Background()
	h := newRestoreHarness(t, testRestoreDVI(testRestoreNamespace, testRestoreSizeBytes))
	_, err := h.service.CreateOrUpdateRestoredVolumeInfo(ctx, testRestoredVolumeInfo())
	require.NoError(t, err)
	first := restoreConditionOf(t, h.cvi())
	require.NotNil(t, first)

	h.deleteDVI() // DPO deletes the DVI once the CVI is live.
	creates, patches, statusPatches := h.creates, h.patches, h.statusPatches
	result, err := h.service.CreateOrUpdateRestoredVolumeInfo(ctx, testRestoredVolumeInfo())
	require.NoError(t, err)
	assert.True(t, result.SnapshotSizeRestored())

	cvi := h.cvi()
	condition := restoreConditionOf(t, cvi)
	require.NotNil(t, condition)
	assert.Equal(t, metav1.ConditionTrue, condition.Status)
	assert.Equal(t, cnsvolumeinfov1alpha1.ReasonSeededFromDeletedVolumeInfo, condition.Reason)
	assert.Equal(t, first.LastTransitionTime, condition.LastTransitionTime)
	assert.Equal(t, testRestoreSizeBytes, cvi.Spec.AggregatedSnapshotSize.Value())
	assert.Equal(t, []int{creates, patches, statusPatches}, []int{h.creates, h.patches, h.statusPatches},
		"a retry must not write again")
}

func TestCreateOrUpdateRestoredVolumeInfoCrashBeforeConditionSetsTrue(t *testing.T) {
	// An earlier attempt created the seeded CVI and crashed before setting the condition; DPO then
	// deleted the DVI.
	size := resource.NewQuantity(testRestoreSizeBytes, resource.BinarySI)
	seeded := testRestoreCVI(func(cvi *cnsvolumeinfov1alpha1.CNSVolumeInfo) {
		stamped(testRestoreUID)(cvi)
		cvi.Finalizers = []string{cnsvolumeinfov1alpha1.SnapServiceFinalizer}
		cvi.Spec.AggregatedSnapshotSize = size
		cvi.Spec.ValidAggregatedSnapshotSize = true
	})
	h := newRestoreHarness(t, seeded)
	result, err := h.service.CreateOrUpdateRestoredVolumeInfo(context.Background(), testRestoredVolumeInfo())
	require.NoError(t, err)
	assert.False(t, result.Seeded)
	assert.Zero(t, h.creates+h.patches, "the CVI spec must not change")
	condition := restoreConditionOf(t, h.cvi())
	require.NotNil(t, condition)
	assert.Equal(t, metav1.ConditionTrue, condition.Status)
	assert.Equal(t, cnsvolumeinfov1alpha1.ReasonSeededFromDeletedVolumeInfo, condition.Reason)
}

func TestCreateOrUpdateRestoredVolumeInfoKeepsFalseAfterCNSValue(t *testing.T) {
	// The restore found no DVI; CNS later wrote a value. A later retry must keep the existing condition.
	ctx := context.Background()
	h := newRestoreHarness(t)
	_, err := h.service.CreateOrUpdateRestoredVolumeInfo(ctx, testRestoredVolumeInfo())
	require.NoError(t, err)
	cvi := h.cvi()
	size := resource.MustParse("2Gi")
	cvi.Spec.AggregatedSnapshotSize = &size
	cvi.Spec.ValidAggregatedSnapshotSize = true
	require.NoError(t, h.backend.Update(ctx, cvi))

	result, err := h.service.CreateOrUpdateRestoredVolumeInfo(ctx, testRestoredVolumeInfo())
	require.NoError(t, err)
	assert.Equal(t, metav1.ConditionFalse, result.Condition.Status)
	assert.Equal(t, cnsvolumeinfov1alpha1.ReasonDeletedVolumeInfoNotFound,
		restoreConditionOf(t, h.cvi()).Reason)
}

func TestCreateOrUpdateRestoredVolumeInfoRetriesStatusConflict(t *testing.T) {
	h := newRestoreHarness(t, testRestoreDVI(testRestoreNamespace, testRestoreSizeBytes))
	h.beforeStatusPatch = func(attempt int) error {
		if attempt == 1 {
			return apierrors.NewConflict(cnsvolumeinfov1alpha1.Resource("cnsvolumeinfoes"),
				testRestoreVolumeID, errors.New("changed"))
		}
		return nil
	}
	_, err := h.service.CreateOrUpdateRestoredVolumeInfo(context.Background(), testRestoredVolumeInfo())
	require.NoError(t, err)
	assert.Equal(t, 2, h.statusPatches)
	for _, body := range h.statusPatchBodies {
		assert.Contains(t, body, `"resourceVersion"`, "every status patch must carry a precondition")
		assert.NotContains(t, body, `"spec"`, "the status patch must carry only the condition")
	}
	assert.NotNil(t, restoreConditionOf(t, h.cvi()))
}

func TestCreateOrUpdateRestoredVolumeInfoStatusErrorIsReturned(t *testing.T) {
	ctx := context.Background()
	h := newRestoreHarness(t, testRestoreDVI(testRestoreNamespace, testRestoreSizeBytes))
	h.beforeStatusPatch = func(int) error { return apierrors.NewServiceUnavailable("down") }
	_, err := h.service.CreateOrUpdateRestoredVolumeInfo(ctx, testRestoredVolumeInfo())
	require.Error(t, err)
	assert.True(t, h.cviExists())

	// The retry after the failure adds the condition without changing the seeded spec.
	h.beforeStatusPatch = nil
	creates, patches := h.creates, h.patches
	_, err = h.service.CreateOrUpdateRestoredVolumeInfo(ctx, testRestoredVolumeInfo())
	require.NoError(t, err)
	assert.Equal(t, []int{creates, patches}, []int{h.creates, h.patches})
	condition := restoreConditionOf(t, h.cvi())
	require.NotNil(t, condition)
	assert.Equal(t, metav1.ConditionTrue, condition.Status)
}

func TestRestoreLeavesMigrationConditionsAlone(t *testing.T) {
	migration := metav1.Condition{Type: cnsvolumeinfov1alpha1.MigrationConditionComplete,
		Status: metav1.ConditionTrue, Reason: "Done", LastTransitionTime: metav1.Now()}
	cvi := testRestoreCVI(func(cvi *cnsvolumeinfov1alpha1.CNSVolumeInfo) {
		cvi.Status.MigrationConditions = []metav1.Condition{migration}
	})
	h := newRestoreHarness(t, cvi, testRestoreDVI(testRestoreNamespace, testRestoreSizeBytes))
	_, err := h.service.CreateOrUpdateRestoredVolumeInfo(context.Background(), testRestoredVolumeInfo())
	require.NoError(t, err)
	got := h.cvi()
	assert.Len(t, got.GetConditions(), 1, "GetConditions stays bound to migrationConditions")
	assert.Equal(t, cnsvolumeinfov1alpha1.MigrationConditionComplete, got.GetConditions()[0].Type)
	assert.NotNil(t, restoreConditionOf(t, got))
}

func TestMarkRestoredSnapshotSizeMeasured(t *testing.T) {
	validSize := resource.MustParse("5Gi")
	withCondition := func(status metav1.ConditionStatus, reason string, valid bool,
		size *resource.Quantity) *cnsvolumeinfov1alpha1.CNSVolumeInfo {
		return testRestoreCVI(func(cvi *cnsvolumeinfov1alpha1.CNSVolumeInfo) {
			cvi.Spec.AggregatedSnapshotSize = size
			cvi.Spec.ValidAggregatedSnapshotSize = valid
			if reason != "" {
				cvi.Status.Conditions = []metav1.Condition{{
					Type: cnsvolumeinfov1alpha1.ConditionSnapshotSizeRestored, Status: status, Reason: reason,
					Message: "CnsDeletedVolumeInfo not found", LastTransitionTime: metav1.Now(),
				}}
			}
		})
	}
	for _, tc := range []struct {
		name          string
		cvi           *cnsvolumeinfov1alpha1.CNSVolumeInfo
		wantStatus    metav1.ConditionStatus
		wantReason    string
		wantCondition bool
		wantPatches   int
	}{
		{name: "False with a valid size turns True",
			cvi: withCondition(metav1.ConditionFalse, cnsvolumeinfov1alpha1.ReasonDeletedVolumeInfoNotFound,
				true, &validSize),
			wantCondition: true, wantStatus: metav1.ConditionTrue,
			wantReason: cnsvolumeinfov1alpha1.ReasonMeasuredByCNS, wantPatches: 1},
		{name: "False with an invalid size stays False",
			cvi: withCondition(metav1.ConditionFalse, cnsvolumeinfov1alpha1.ReasonDeletedVolumeInfoNotFound,
				false, &validSize),
			wantCondition: true, wantStatus: metav1.ConditionFalse,
			wantReason: cnsvolumeinfov1alpha1.ReasonDeletedVolumeInfoNotFound},
		{name: "False without a size stays False",
			cvi: withCondition(metav1.ConditionFalse, cnsvolumeinfov1alpha1.ReasonDeletedVolumeInfoNotFound,
				false, nil),
			wantCondition: true, wantStatus: metav1.ConditionFalse,
			wantReason: cnsvolumeinfov1alpha1.ReasonDeletedVolumeInfoNotFound},
		{name: "True is left alone",
			cvi: withCondition(metav1.ConditionTrue, cnsvolumeinfov1alpha1.ReasonSeededFromDeletedVolumeInfo,
				true, &validSize),
			wantCondition: true, wantStatus: metav1.ConditionTrue,
			wantReason: cnsvolumeinfov1alpha1.ReasonSeededFromDeletedVolumeInfo},
		{name: "missing condition is never created", cvi: withCondition("", "", true, &validSize)},
	} {
		t.Run(tc.name, func(t *testing.T) {
			h := newRestoreHarness(t, tc.cvi)
			require.NoError(t, h.service.MarkRestoredSnapshotSizeMeasured(context.Background(), testRestoreVolumeID))
			assert.Equal(t, tc.wantPatches, h.statusPatches)
			condition := restoreConditionOf(t, h.cvi())
			if !tc.wantCondition {
				assert.Nil(t, condition)
				return
			}
			require.NotNil(t, condition)
			assert.Equal(t, tc.wantStatus, condition.Status)
			assert.Equal(t, tc.wantReason, condition.Reason)
		})
	}
}

func TestMarkRestoredSnapshotSizeMeasuredMissingCVI(t *testing.T) {
	h := newRestoreHarness(t)
	require.NoError(t, h.service.MarkRestoredSnapshotSizeMeasured(context.Background(), testRestoreVolumeID))
	assert.Zero(t, h.statusPatches)
}

func TestCRDManifestsDecode(t *testing.T) {
	for _, tc := range []struct {
		fs   interface{ ReadFile(string) ([]byte, error) }
		file string
		kind string
	}{
		{cnsvolumeinfoconfig.EmbedCnsVolumeInfoFile, cnsvolumeinfoconfig.EmbedCnsVolumeInfoFileName, "CNSVolumeInfo"},
		{cnsvolumeinfoconfig.EmbedCnsDeletedVolumeInfoFile, cnsvolumeinfoconfig.EmbedCnsDeletedVolumeInfoFileName,
			cnsvolumeinfov1alpha1.CnsDeletedVolumeInfoKind},
	} {
		t.Run(tc.file, func(t *testing.T) {
			data, err := tc.fs.ReadFile(tc.file)
			require.NoError(t, err)
			crd := &apiextensionsv1.CustomResourceDefinition{}
			require.NoError(t, yaml.UnmarshalStrict(data, crd))
			assert.Equal(t, cnsvolumeinfov1alpha1.GroupName, crd.Spec.Group)
			assert.Equal(t, tc.kind, crd.Spec.Names.Kind)
			require.Len(t, crd.Spec.Versions, 1)
			schema := crd.Spec.Versions[0].Schema.OpenAPIV3Schema
			if tc.kind == cnsvolumeinfov1alpha1.CnsDeletedVolumeInfoKind {
				spec := schema.Properties["spec"]
				assert.NotContains(t, spec.Properties, "validSize")
				require.NotNil(t, spec.Properties["sizeBytes"].Minimum)
				assert.Zero(t, *spec.Properties["sizeBytes"].Minimum)
				for _, field := range []string{"volumeID", "namespace", "storageClassName", "storagePolicyID"} {
					require.NotNil(t, spec.Properties[field].MinLength, field)
					assert.Equal(t, int64(1), *spec.Properties[field].MinLength, field)
				}
				assert.ElementsMatch(t, []string{"volumeID", "namespace", "storageClassName", "storagePolicyID",
					"sizeBytes"}, spec.Required)
				assert.Nil(t, spec.Properties["volumeAttributeClassName"].MinLength,
					"volumeAttributeClassName is optional and may be empty")
				assert.Nil(t, crd.Spec.Versions[0].Subresources)
				return
			}
			status := schema.Properties["status"]
			conditions, ok := status.Properties["conditions"]
			require.True(t, ok, "the CVI CRD must define status.conditions")
			assert.Equal(t, "map", *conditions.XListType)
			assert.Contains(t, status.Properties, "migrationConditions")
		})
	}
}
