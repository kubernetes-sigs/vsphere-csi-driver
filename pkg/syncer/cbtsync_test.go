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
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	cnstypes "github.com/vmware/govmomi/cns/types"
	v1 "k8s.io/api/core/v1"
	storagev1 "k8s.io/api/storage/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	corelisters "k8s.io/client-go/listers/core/v1"
	storagelistersv1 "k8s.io/client-go/listers/storage/v1"
	"k8s.io/client-go/tools/cache"
	"sigs.k8s.io/controller-runtime/pkg/client"

	cnsvolume "sigs.k8s.io/vsphere-csi-driver/v3/pkg/common/cns-lib/volume"
	"sigs.k8s.io/vsphere-csi-driver/v3/pkg/csi/service/common"
	"sigs.k8s.io/vsphere-csi-driver/v3/pkg/csi/service/logger"
)

// fakeVolMgr records SetVolumeControlFlags / ClearVolumeControlFlags and QueryAllVolume calls
// and returns configurable errors. It embeds cnsvolume.MockManager to satisfy the rest of
// the cnsvolume.Manager interface.
type fakeVolMgr struct {
	cnsvolume.MockManager

	mu              sync.Mutex
	setCalls        []string
	clearCalls      []string
	setErr          error
	clearErr        error
	queryResult     *cnstypes.CnsQueryResult
	queryErr        error
	queryBatchSizes []int
	// queryFn, if set, takes precedence over queryResult/queryErr; lets a test produce
	// a per-batch response to assert batching behavior.
	queryFn func(filter cnstypes.CnsQueryFilter) (*cnstypes.CnsQueryResult, error)
}

func (f *fakeVolMgr) SetVolumeControlFlags(_ context.Context, volumeID string, _ []string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.setCalls = append(f.setCalls, volumeID)
	return f.setErr
}

func (f *fakeVolMgr) ClearVolumeControlFlags(_ context.Context, volumeID string, _ []string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.clearCalls = append(f.clearCalls, volumeID)
	return f.clearErr
}

func (f *fakeVolMgr) QueryAllVolume(_ context.Context, filter cnstypes.CnsQueryFilter,
	_ cnstypes.CnsQuerySelection) (*cnstypes.CnsQueryResult, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.queryBatchSizes = append(f.queryBatchSizes, len(filter.VolumeIds))
	if f.queryFn != nil {
		return f.queryFn(filter)
	}
	return f.queryResult, f.queryErr
}

func (f *fakeVolMgr) setCallsCopy() []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	out := make([]string, len(f.setCalls))
	copy(out, f.setCalls)
	return out
}

func (f *fakeVolMgr) clearCallsCopy() []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	out := make([]string, len(f.clearCalls))
	copy(out, f.clearCalls)
	return out
}

func (f *fakeVolMgr) queryBatchSizesCopy() []int {
	f.mu.Lock()
	defer f.mu.Unlock()
	out := make([]int, len(f.queryBatchSizes))
	copy(out, f.queryBatchSizes)
	return out
}

var _ cnsvolume.Manager = (*fakeVolMgr)(nil)

// newCBTBlockPV returns a vSphere CSI block-volume PersistentVolume.
func newCBTBlockPV(name, volumeHandle string) *v1.PersistentVolume {
	return &v1.PersistentVolume{
		ObjectMeta: metav1.ObjectMeta{Name: name},
		Spec: v1.PersistentVolumeSpec{
			PersistentVolumeSource: v1.PersistentVolumeSource{
				CSI: &v1.CSIPersistentVolumeSource{
					Driver:       common.VSphereCSIDriverName,
					VolumeHandle: volumeHandle,
					VolumeAttributes: map[string]string{
						common.AttributeDiskType: common.DiskTypeBlockVolume,
					},
				},
			},
		},
	}
}

// newPVC returns a Bound PVC referencing the given PV.
func newPVC(namespace, name, pvName string, labels map[string]string,
	annotations map[string]string) *v1.PersistentVolumeClaim {
	return &v1.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{
			Name:        name,
			Namespace:   namespace,
			Labels:      labels,
			Annotations: annotations,
		},
		Spec:   v1.PersistentVolumeClaimSpec{VolumeName: pvName},
		Status: v1.PersistentVolumeClaimStatus{Phase: v1.ClaimBound},
	}
}

// newPVCWithPhase returns a PVC in the requested phase.
func newPVCWithPhase(namespace, name, pvName string,
	phase v1.PersistentVolumeClaimPhase) *v1.PersistentVolumeClaim {
	pvc := newPVC(namespace, name, pvName, nil, nil)
	pvc.Status.Phase = phase
	return pvc
}

// newVolumeAttachmentForPV returns a VolumeAttachment for the given PV name.
func newVolumeAttachmentForPV(name, pvName string) *storagev1.VolumeAttachment {
	pvNameRef := pvName
	return &storagev1.VolumeAttachment{
		ObjectMeta: metav1.ObjectMeta{Name: name},
		Spec: storagev1.VolumeAttachmentSpec{
			Source: storagev1.VolumeAttachmentSource{PersistentVolumeName: &pvNameRef},
		},
	}
}

// cnsVolumeWithCBT builds a minimal CnsVolume with the given ID and CBT state.
func cnsVolumeWithCBT(id string, status cnstypes.CnsVolumeCBTStatus) cnstypes.CnsVolume {
	return cnstypes.CnsVolume{
		VolumeId:             cnstypes.CnsVolumeId{Id: id},
		ChangedBlockTracking: status,
	}
}

// newTestListers builds in-memory PV / PVC / VolumeAttachment listers seeded with the given
// objects so tests can stand in for the singleton InformerManager listers.
func newTestListers(t *testing.T, objs ...client.Object) (corelisters.PersistentVolumeLister,
	corelisters.PersistentVolumeClaimLister, storagelistersv1.VolumeAttachmentLister) {
	t.Helper()
	pvIdx := cache.NewIndexer(cache.MetaNamespaceKeyFunc,
		cache.Indexers{cache.NamespaceIndex: cache.MetaNamespaceIndexFunc})
	pvcIdx := cache.NewIndexer(cache.MetaNamespaceKeyFunc,
		cache.Indexers{cache.NamespaceIndex: cache.MetaNamespaceIndexFunc})
	vaIdx := cache.NewIndexer(cache.MetaNamespaceKeyFunc,
		cache.Indexers{cache.NamespaceIndex: cache.MetaNamespaceIndexFunc})
	for _, o := range objs {
		switch obj := o.(type) {
		case *v1.PersistentVolume:
			require.NoError(t, pvIdx.Add(obj))
		case *v1.PersistentVolumeClaim:
			require.NoError(t, pvcIdx.Add(obj))
		case *storagev1.VolumeAttachment:
			require.NoError(t, vaIdx.Add(obj))
		}
	}
	return corelisters.NewPersistentVolumeLister(pvIdx),
		corelisters.NewPersistentVolumeClaimLister(pvcIdx),
		storagelistersv1.NewVolumeAttachmentLister(vaIdx)
}

func TestLoadAttachedPVNames(t *testing.T) {
	ctx, _ := logger.GetNewContextWithLogger()

	t.Run("EmptyClusterReturnsEmptySet", func(t *testing.T) {
		_, _, vaLister := newTestListers(t)
		got, err := loadAttachedPVNames(vaLister)
		require.NoError(t, err)
		assert.Empty(t, got)
	})

	t.Run("CollectsAllAttachedPVNames", func(t *testing.T) {
		va1 := newVolumeAttachmentForPV("va-1", "pv-attached-1")
		va2 := newVolumeAttachmentForPV("va-2", "pv-attached-2")
		emptyName := ""
		vaEmpty := &storagev1.VolumeAttachment{
			ObjectMeta: metav1.ObjectMeta{Name: "va-empty"},
			Spec: storagev1.VolumeAttachmentSpec{
				Source: storagev1.VolumeAttachmentSource{PersistentVolumeName: &emptyName},
			},
		}
		vaNil := &storagev1.VolumeAttachment{ObjectMeta: metav1.ObjectMeta{Name: "va-nil"}}

		_, _, vaLister := newTestListers(t, va1, va2, vaEmpty, vaNil)
		got, err := loadAttachedPVNames(vaLister)
		require.NoError(t, err)
		assert.Equal(t, map[string]struct{}{
			"pv-attached-1": {},
			"pv-attached-2": {},
		}, got)
	})

	_ = ctx
}

func TestPvcShouldBeConsideredForCBT(t *testing.T) {
	ctx, _ := logger.GetNewContextWithLogger()

	attached := func(pvNames ...string) map[string]struct{} {
		m := make(map[string]struct{}, len(pvNames))
		for _, n := range pvNames {
			m[n] = struct{}{}
		}
		return m
	}

	tests := []struct {
		name        string
		pvc         *v1.PersistentVolumeClaim
		attachedPVs map[string]struct{}
		want        bool
	}{
		{
			name: "NotBound",
			pvc:  newPVCWithPhase("ns", "p", "pv-x", v1.ClaimPending),
			want: false,
		},
		{
			name: "NoVolumeName",
			pvc:  newPVC("ns", "p", "", nil, nil),
			want: false,
		},
		{
			name:        "AttachedPV",
			pvc:         newPVC("ns", "p", "pv-x", nil, nil),
			attachedPVs: attached("pv-x"),
			want:        false,
		},
		{
			name: "VMServiceAttachedAnnotation",
			pvc: newPVC("ns", "p", "pv-x", nil,
				map[string]string{"cns.vmware.com/usedby-vm-abc": "true"}),
			attachedPVs: attached(),
			want:        false,
		},
		{
			name:        "Eligible",
			pvc:         newPVC("ns", "p", "pv-x", nil, nil),
			attachedPVs: attached(),
			want:        true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.want, pvcEligibleForCBTChange(ctx, tt.pvc, tt.attachedPVs))
		})
	}
}

func TestResolvePVCToVolumeID(t *testing.T) {
	ctx, _ := logger.GetNewContextWithLogger()

	t.Run("MissingPVReturnsFalse", func(t *testing.T) {
		pvLister, _, _ := newTestListers(t)
		pvc := newPVC("ns", "pvc-1", "pv-missing", nil, nil)
		id, ok := resolvePVCToVolumeID(ctx, pvLister, pvc)
		assert.False(t, ok)
		assert.Empty(t, id)
	})

	t.Run("NonCSIPVReturnsFalse", func(t *testing.T) {
		pv := &v1.PersistentVolume{ObjectMeta: metav1.ObjectMeta{Name: "pv-1"}}
		pvLister, _, _ := newTestListers(t, pv)
		pvc := newPVC("ns", "pvc-1", "pv-1", nil, nil)
		_, ok := resolvePVCToVolumeID(ctx, pvLister, pvc)
		assert.False(t, ok)
	})

	t.Run("ForeignCSIDriverReturnsFalse", func(t *testing.T) {
		pv := &v1.PersistentVolume{
			ObjectMeta: metav1.ObjectMeta{Name: "pv-1"},
			Spec: v1.PersistentVolumeSpec{
				PersistentVolumeSource: v1.PersistentVolumeSource{
					CSI: &v1.CSIPersistentVolumeSource{Driver: "other.csi.driver"},
				},
			},
		}
		pvLister, _, _ := newTestListers(t, pv)
		pvc := newPVC("ns", "pvc-1", "pv-1", nil, nil)
		_, ok := resolvePVCToVolumeID(ctx, pvLister, pvc)
		assert.False(t, ok)
	})

	t.Run("FileVolumeReturnsFalse", func(t *testing.T) {
		pv := newCBTBlockPV("pv-1", "vol-1")
		pv.Spec.CSI.VolumeAttributes[common.AttributeDiskType] = "vSphere CNS File Volume"
		pvLister, _, _ := newTestListers(t, pv)
		pvc := newPVC("ns", "pvc-1", "pv-1", nil, nil)
		_, ok := resolvePVCToVolumeID(ctx, pvLister, pvc)
		assert.False(t, ok)
	})

	t.Run("EmptyVolumeHandleReturnsFalse", func(t *testing.T) {
		pv := newCBTBlockPV("pv-1", "")
		pvLister, _, _ := newTestListers(t, pv)
		pvc := newPVC("ns", "pvc-1", "pv-1", nil, nil)
		_, ok := resolvePVCToVolumeID(ctx, pvLister, pvc)
		assert.False(t, ok)
	})

	t.Run("ValidBlockVolumeReturnsHandle", func(t *testing.T) {
		pv := newCBTBlockPV("pv-1", "vol-handle-1")
		pvLister, _, _ := newTestListers(t, pv)
		pvc := newPVC("ns", "pvc-1", "pv-1", nil, nil)
		id, ok := resolvePVCToVolumeID(ctx, pvLister, pvc)
		assert.True(t, ok)
		assert.Equal(t, "vol-handle-1", id)
	})
}

func TestBuildPVCCandidates(t *testing.T) {
	ctx, _ := logger.GetNewContextWithLogger()

	t.Run("EmptyNamespaceReturnsEmpty", func(t *testing.T) {
		pvLister, pvcLister, vaLister := newTestListers(t)
		got, err := buildPVCCandidates(ctx, pvLister, pvcLister, vaLister, "ns")
		require.NoError(t, err)
		assert.Empty(t, got)
	})

	t.Run("AttachedPVCSkipped", func(t *testing.T) {
		pv := newCBTBlockPV("pv-1", "vol-1")
		pvc := newPVC("ns", "pvc-1", "pv-1", nil, nil)
		va := newVolumeAttachmentForPV("va-1", "pv-1")
		pvLister, pvcLister, vaLister := newTestListers(t, pv, pvc, va)
		got, err := buildPVCCandidates(ctx, pvLister, pvcLister, vaLister, "ns")
		require.NoError(t, err)
		assert.Empty(t, got)
	})

	t.Run("EligiblePVCIncluded", func(t *testing.T) {
		pv := newCBTBlockPV("pv-1", "vol-1")
		pvc := newPVC("ns", "pvc-1", "pv-1", nil, nil)
		pvLister, pvcLister, vaLister := newTestListers(t, pv, pvc)
		got, err := buildPVCCandidates(ctx, pvLister, pvcLister, vaLister, "ns")
		require.NoError(t, err)
		require.Len(t, got, 1)
		assert.Equal(t, "vol-1", got[0].volumeID)
	})

	t.Run("NonBoundPVCsSkipped", func(t *testing.T) {
		pv := newCBTBlockPV("pv-bound", "vol-bound")
		pvcBound := newPVC("ns", "pvc-bound", "pv-bound", nil, nil)
		pvcPending := newPVCWithPhase("ns", "pvc-pending", "", v1.ClaimPending)
		pvLister, pvcLister, vaLister := newTestListers(t, pv, pvcBound, pvcPending)
		got, err := buildPVCCandidates(ctx, pvLister, pvcLister, vaLister, "ns")
		require.NoError(t, err)
		require.Len(t, got, 1)
		assert.Equal(t, "vol-bound", got[0].volumeID)
	})

	t.Run("MixedEligibilityFiltersCorrectly", func(t *testing.T) {
		pvGood := newCBTBlockPV("pv-good", "vol-good")
		pvFile := newCBTBlockPV("pv-file", "vol-file")
		pvFile.Spec.CSI.VolumeAttributes[common.AttributeDiskType] = "vSphere CNS File Volume"
		pvAttached := newCBTBlockPV("pv-attached", "vol-attached")

		pvcGood := newPVC("ns", "pvc-good", "pv-good", nil, nil)
		pvcFile := newPVC("ns", "pvc-file", "pv-file", nil, nil)
		pvcAttached := newPVC("ns", "pvc-attached", "pv-attached", nil, nil)
		va := newVolumeAttachmentForPV("va-1", "pv-attached")

		pvLister, pvcLister, vaLister := newTestListers(t, pvGood, pvFile, pvAttached,
			pvcGood, pvcFile, pvcAttached, va)
		got, err := buildPVCCandidates(ctx, pvLister, pvcLister, vaLister, "ns")
		require.NoError(t, err)
		require.Len(t, got, 1, "only pvcGood should survive all filters")
		assert.Equal(t, "vol-good", got[0].volumeID)
	})
}

func TestFilterCandidatesByCBTState(t *testing.T) {
	ctx, _ := logger.GetNewContextWithLogger()

	t.Run("EmptyInputReturnsEmpty", func(t *testing.T) {
		got, err := filterCandidatesByCBTState(ctx, &fakeVolMgr{}, "ns", nil, true)
		require.NoError(t, err)
		assert.Empty(t, got)
	})

	t.Run("QueryErrorReturnsError", func(t *testing.T) {
		pvc := newPVC("ns", "pvc-1", "pv-1", nil, nil)
		vm := &fakeVolMgr{queryErr: errors.New("cns unavailable")}
		in := []pvcWithVolume{{pvc: pvc, volumeID: "vol-1"}}
		got, err := filterCandidatesByCBTState(ctx, vm, "ns", in, true)
		require.Error(t, err, "query failure must be surfaced so the caller can requeue")
		assert.Nil(t, got)
	})

	t.Run("AlreadyInTargetStateFiltered", func(t *testing.T) {
		vm := &fakeVolMgr{queryResult: &cnstypes.CnsQueryResult{}}
		pvc := newPVC("ns", "pvc-1", "pv-1", nil, nil)
		in := []pvcWithVolume{{pvc: pvc, volumeID: "vol-1"}}
		got, err := filterCandidatesByCBTState(ctx, vm, "ns", in, true)
		require.NoError(t, err)
		assert.Empty(t, got, "volume already in target state must be filtered out")
	})

	t.Run("NeedsFlipIsKept", func(t *testing.T) {
		vm := &fakeVolMgr{
			queryResult: &cnstypes.CnsQueryResult{
				Volumes: []cnstypes.CnsVolume{cnsVolumeWithCBT("vol-1", cnstypes.CnsVolumeCBTStatusDisabled)},
			},
		}
		pvc := newPVC("ns", "pvc-1", "pv-1", nil, nil)
		in := []pvcWithVolume{{pvc: pvc, volumeID: "vol-1"}}
		got, err := filterCandidatesByCBTState(ctx, vm, "ns", in, true)
		require.NoError(t, err)
		require.Len(t, got, 1)
		assert.Equal(t, "vol-1", got[0].volumeID)
	})
}

// TestQueryVolumesNeedingFlip exercises the QueryAllVolume input batching.
func TestQueryVolumesNeedingFlip(t *testing.T) {
	ctx, _ := logger.GetNewContextWithLogger()

	t.Run("EmptyInputIssuesNoQueries", func(t *testing.T) {
		vm := &fakeVolMgr{}
		got, err := queryVolumesNeedingFlip(ctx, vm, nil, true)
		require.NoError(t, err)
		assert.Empty(t, got)
		assert.Empty(t, vm.queryBatchSizesCopy())
	})

	t.Run("SingleBatchUnderThreshold", func(t *testing.T) {
		ids := []string{"vol-1", "vol-2", "vol-3"}
		vm := &fakeVolMgr{
			queryFn: func(filter cnstypes.CnsQueryFilter) (*cnstypes.CnsQueryResult, error) {
				vols := make([]cnstypes.CnsVolume, 0, len(filter.VolumeIds))
				for _, v := range filter.VolumeIds {
					vols = append(vols, cnsVolumeWithCBT(v.Id, cnstypes.CnsVolumeCBTStatusDisabled))
				}
				return &cnstypes.CnsQueryResult{Volumes: vols}, nil
			},
		}
		got, err := queryVolumesNeedingFlip(ctx, vm, ids, true)
		require.NoError(t, err)
		assert.Len(t, got, 3)
		assert.Equal(t, []int{3}, vm.queryBatchSizesCopy())
	})

	t.Run("MultipleBatchesAtBoundary", func(t *testing.T) {
		const total = 2*volumdIDLimitPerQuery + volumdIDLimitPerQuery/2
		ids := make([]string, total)
		for i := range ids {
			ids[i] = fmt.Sprintf("vol-%d", i)
		}
		vm := &fakeVolMgr{
			queryFn: func(filter cnstypes.CnsQueryFilter) (*cnstypes.CnsQueryResult, error) {
				vols := make([]cnstypes.CnsVolume, 0, len(filter.VolumeIds))
				for _, v := range filter.VolumeIds {
					vols = append(vols, cnsVolumeWithCBT(v.Id, cnstypes.CnsVolumeCBTStatusDisabled))
				}
				return &cnstypes.CnsQueryResult{Volumes: vols}, nil
			},
		}
		got, err := queryVolumesNeedingFlip(ctx, vm, ids, true)
		require.NoError(t, err)
		assert.Len(t, got, total)
		assert.Equal(t,
			[]int{volumdIDLimitPerQuery, volumdIDLimitPerQuery, volumdIDLimitPerQuery / 2},
			vm.queryBatchSizesCopy())
	})

	t.Run("FirstBatchErrorAborts", func(t *testing.T) {
		const total = 2 * volumdIDLimitPerQuery
		ids := make([]string, total)
		for i := range ids {
			ids[i] = fmt.Sprintf("vol-%d", i)
		}
		vm := &fakeVolMgr{queryErr: errors.New("cns boom")}
		_, err := queryVolumesNeedingFlip(ctx, vm, ids, true)
		require.Error(t, err)
		assert.Equal(t, []int{volumdIDLimitPerQuery}, vm.queryBatchSizesCopy(),
			"should stop at the first failing batch instead of plowing through")
	})

	t.Run("DisableUsesEnabledFilter", func(t *testing.T) {
		vm := &fakeVolMgr{
			queryFn: func(filter cnstypes.CnsQueryFilter) (*cnstypes.CnsQueryResult, error) {
				assert.Equal(t, cnstypes.CnsVolumeCBTStatusEnabled, filter.ChangedBlockTracking,
					"disable reconcile must query for currently-enabled volumes")
				return &cnstypes.CnsQueryResult{}, nil
			},
		}
		_, err := queryVolumesNeedingFlip(ctx, vm, []string{"vol-1"}, false)
		require.NoError(t, err)
	})
}

// TestPeriodicSkipsActiveReconcile verifies that syncCBTForNamespace skips work when
// a background controller reconcile is in flight for the same namespace.
func TestPeriodicSkipsActiveReconcile(t *testing.T) {
	ctx, _ := logger.GetNewContextWithLogger()
	ns := "skip-test-ns"

	_, fakeCancel := context.WithCancel(ctx)
	cbtWorkMu.Lock()
	cbtWorkMap[ns] = &cbtWork{cancel: fakeCancel}
	cbtWorkMu.Unlock()
	defer func() {
		cbtWorkMu.Lock()
		delete(cbtWorkMap, ns)
		cbtWorkMu.Unlock()
		fakeCancel()
	}()

	vm := &fakeVolMgr{}
	pvLister, pvcLister, vaLister := newTestListers(t)

	err := syncCBTForNamespace(ctx, vm, pvLister, pvcLister, vaLister, ns, true)
	require.NoError(t, err)
	assert.Empty(t, vm.setCallsCopy(), "periodic sync must not call Set when reconcile is active")
	assert.Empty(t, vm.clearCallsCopy(), "periodic sync must not call Clear when reconcile is active")
}

// TestReconcileCancelsInFlightWork verifies that ReconcileCBTForNamespace cancels the
// context of any in-flight CBT work (periodic sync or a previous background reconcile)
// for the same namespace before scheduling its own work.
func TestReconcileCancelsInFlightWork(t *testing.T) {
	ctx, _ := logger.GetNewContextWithLogger()
	ns := "cancel-test-ns"

	fakeCtx, fakeCancel := context.WithCancel(ctx)
	cbtWorkMu.Lock()
	cbtWorkMap[ns] = &cbtWork{cancel: fakeCancel}
	cbtWorkMu.Unlock()
	defer func() {
		cbtWorkMu.Lock()
		delete(cbtWorkMap, ns)
		cbtWorkMu.Unlock()
	}()

	pv := newCBTBlockPV("pv-1", "vol-1")
	pvc := newPVC(ns, "pvc-1", "pv-1", nil, nil)
	vm := &fakeVolMgr{queryResult: &cnstypes.CnsQueryResult{}}
	pvLister, pvcLister, vaLister := newTestListers(t, pv, pvc)

	require.NoError(t, ReconcileCBTForNamespace(ctx, vm, pvLister, pvcLister, vaLister, ns, true))

	assert.ErrorIs(t, fakeCtx.Err(), context.Canceled,
		"ReconcileCBTForNamespace must cancel the in-flight CBT work context")

	// Wait for the background goroutine to finish so it doesn't leak into the next test.
	assert.Eventually(t, func() bool {
		cbtWorkMu.Lock()
		defer cbtWorkMu.Unlock()
		return cbtWorkMap[ns] == nil
	}, time.Second*5, time.Millisecond*10,
		"background reconcile goroutine must clear cbtWorkMap entry on exit")
}

// TestReconcileClearsStateAfterCompletion verifies that the cbtWorkMap entry is removed
// once the background reconcile goroutine launched by ReconcileCBTForNamespace finishes,
// so future periodic syncs are no longer blocked.
func TestReconcileClearsStateAfterCompletion(t *testing.T) {
	ctx, _ := logger.GetNewContextWithLogger()
	ns := "mark-test-ns"

	cbtWorkMu.Lock()
	delete(cbtWorkMap, ns)
	cbtWorkMu.Unlock()

	pv := newCBTBlockPV("pv-1", "vol-1")
	pvc := newPVC(ns, "pvc-1", "pv-1", nil, nil)
	vm := &fakeVolMgr{queryResult: &cnstypes.CnsQueryResult{}}
	pvLister, pvcLister, vaLister := newTestListers(t, pv, pvc)

	require.NoError(t, ReconcileCBTForNamespace(ctx, vm, pvLister, pvcLister, vaLister, ns, true))

	assert.Eventually(t, func() bool {
		cbtWorkMu.Lock()
		defer cbtWorkMu.Unlock()
		return cbtWorkMap[ns] == nil
	}, time.Second*5, time.Millisecond*10,
		"namespace entry must be deleted after the background reconcile goroutine returns")
}

// TestReconcileQueueFullReturnsError verifies that ReconcileCBTForNamespace returns an
// error (so the controller can requeue) when the bounded background pool is fully busy.
func TestReconcileQueueFullReturnsError(t *testing.T) {
	ctx, _ := logger.GetNewContextWithLogger()

	queue := getCBTWorkQueue(ctx)
	capacity := cap(queue)
	require.Greater(t, capacity, 0, "queue must have non-zero capacity")

	// Fill every slot so the next non-blocking enqueue cannot proceed.
	for i := 0; i < capacity; i++ {
		queue <- struct{}{}
	}
	defer func() {
		for i := 0; i < capacity; i++ {
			<-queue
		}
	}()

	vm := &fakeVolMgr{}
	pvLister, pvcLister, vaLister := newTestListers(t)
	err := ReconcileCBTForNamespace(ctx, vm, pvLister, pvcLister, vaLister, "queue-full-ns", true)
	require.Error(t, err, "queue-full reconcile must return error so the controller requeues")
	assert.Contains(t, err.Error(), "queue is full")
	assert.Empty(t, vm.setCallsCopy(),
		"no CBT work should run when reconcile was rejected due to a full queue")
}

func TestApplyCnsCbtFlags(t *testing.T) {
	ctx, _ := logger.GetNewContextWithLogger()

	t.Run("EmptyCandidatesIsNoop", func(t *testing.T) {
		vm := &fakeVolMgr{}
		err := applyCnsCbtFlags(ctx, vm, "ns", nil, true)
		require.NoError(t, err)
		assert.Empty(t, vm.setCallsCopy())
		assert.Empty(t, vm.clearCallsCopy())
	})

	t.Run("EnableCallsSet", func(t *testing.T) {
		pvc := newPVC("ns", "pvc-1", "pv-1", nil, nil)
		vm := &fakeVolMgr{}
		candidates := []pvcWithVolume{{pvc: pvc, volumeID: "vol-1"}}
		err := applyCnsCbtFlags(ctx, vm, "ns", candidates, true)
		require.NoError(t, err)
		assert.Equal(t, []string{"vol-1"}, vm.setCallsCopy())
		assert.Empty(t, vm.clearCallsCopy())
	})

	t.Run("DisableCallsClear", func(t *testing.T) {
		pvc := newPVC("ns", "pvc-1", "pv-1", nil, nil)
		vm := &fakeVolMgr{}
		candidates := []pvcWithVolume{{pvc: pvc, volumeID: "vol-1"}}
		err := applyCnsCbtFlags(ctx, vm, "ns", candidates, false)
		require.NoError(t, err)
		assert.Empty(t, vm.setCallsCopy())
		assert.Equal(t, []string{"vol-1"}, vm.clearCallsCopy())
	})

	t.Run("SetErrorIsLogged", func(t *testing.T) {
		pvc := newPVC("ns", "pvc-1", "pv-1", nil, nil)
		vm := &fakeVolMgr{setErr: errors.New("cns set failed")}
		candidates := []pvcWithVolume{{pvc: pvc, volumeID: "vol-1"}}
		err := applyCnsCbtFlags(ctx, vm, "ns", candidates, true)
		require.NoError(t, err, "per-volume errors are best-effort and must not be returned")
		assert.Equal(t, []string{"vol-1"}, vm.setCallsCopy())
	})
}
