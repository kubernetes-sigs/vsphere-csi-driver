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
	"encoding/json"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	cnstypes "github.com/vmware/govmomi/cns/types"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"

	cnsvolume "sigs.k8s.io/vsphere-csi-driver/v3/pkg/common/cns-lib/volume"
	"sigs.k8s.io/vsphere-csi-driver/v3/pkg/csi/service/common"
	"sigs.k8s.io/vsphere-csi-driver/v3/pkg/csi/service/logger"
	cnsvolumeinfov1alpha1 "sigs.k8s.io/vsphere-csi-driver/v3/pkg/internalapis/cnsvolumeinfo/v1alpha1"
)

const testSnapshotVolumeID = "11111111-2222-3333-4444-555555555555"

// snapshotDetailsHarness runs UpdateVolumeInfoSnapshotDetails against a fake API server. beforePatch,
// when set, runs before each CSI patch reaches the API server, with the patch's attempt number; it
// can write the CnsVolumeInfo as a concurrent writer would, or fail the patch.
type snapshotDetailsHarness struct {
	t           *testing.T
	service     *volumeInfo
	backend     client.Client
	beforePatch func(ctx context.Context, c client.WithWatch, attempt int) (forward bool, err error)

	mu             sync.Mutex
	patchAttempts  int
	gets           int
	remeasureCalls int
	// appliedMb records the aggregated snapshot size, in MB, after every CSI patch the API server accepted.
	appliedMb []int64
}

func newSnapshotDetailsHarness(t *testing.T, objs ...client.Object) *snapshotDetailsHarness {
	t.Helper()
	withFastRetries(t)
	scheme := runtime.NewScheme()
	require.NoError(t, cnsvolumeinfov1alpha1.AddToScheme(scheme))
	h := &snapshotDetailsHarness{t: t}
	c := fake.NewClientBuilder().WithScheme(scheme).WithObjects(objs...).
		WithInterceptorFuncs(interceptor.Funcs{
			Get: func(ctx context.Context, c client.WithWatch, key client.ObjectKey, obj client.Object,
				opts ...client.GetOption) error {
				h.mu.Lock()
				h.gets++
				h.mu.Unlock()
				return c.Get(ctx, key, obj, opts...)
			},
			Patch: func(ctx context.Context, c client.WithWatch, obj client.Object, patch client.Patch,
				opts ...client.PatchOption) error {
				h.mu.Lock()
				h.patchAttempts++
				attempt := h.patchAttempts
				h.mu.Unlock()
				if h.beforePatch != nil {
					forward, err := h.beforePatch(ctx, c, attempt)
					if !forward {
						return err
					}
				}
				if err := c.Patch(ctx, obj, patch, opts...); err != nil {
					return err
				}
				h.mu.Lock()
				h.appliedMb = append(h.appliedMb, sizeMb(h.read(ctx, c)))
				h.mu.Unlock()
				return nil
			},
		}).Build()
	h.backend = c
	h.service = &volumeInfo{k8sClient: c}
	return h
}

// cnsReporting returns a CNS volume manager that counts its queries and reports capacityInMb.
func (h *snapshotDetailsHarness) cnsReporting(capacityInMb int64, err error) cnsvolume.Manager {
	return &capacityReportingManager{h: h, capacityInMb: capacityInMb, err: err}
}

// capacityReportingManager answers single-volume CNS queries with a fixed aggregated snapshot capacity.
// Its first transientFailures queries fail.
type capacityReportingManager struct {
	cnsvolume.Manager
	h                 *snapshotDetailsHarness
	capacityInMb      int64
	err               error
	transientFailures int
}

func (m *capacityReportingManager) QueryVolume(_ context.Context,
	queryFilter cnstypes.CnsQueryFilter) (*cnstypes.CnsQueryResult, error) {
	m.h.mu.Lock()
	m.h.remeasureCalls++
	m.h.mu.Unlock()
	if m.err != nil {
		return nil, m.err
	}
	if m.transientFailures > 0 {
		m.transientFailures--
		return nil, errors.New("CNS query timed out")
	}
	return &cnstypes.CnsQueryResult{Volumes: []cnstypes.CnsVolume{{
		VolumeId:             queryFilter.VolumeIds[0],
		BackingObjectDetails: &cnstypes.CnsBlockBackingDetails{AggregatedSnapshotCapacityInMb: m.capacityInMb},
	}}}, nil
}

func (h *snapshotDetailsHarness) read(ctx context.Context, c client.Client) *cnsvolumeinfov1alpha1.CNSVolumeInfo {
	h.t.Helper()
	cvi := &cnsvolumeinfov1alpha1.CNSVolumeInfo{}
	require.NoError(h.t, c.Get(ctx, types.NamespacedName{Namespace: csiNamespace, Name: testSnapshotVolumeID}, cvi))
	return cvi
}

// current returns the CnsVolumeInfo without counting the read as a CSI Get.
func (h *snapshotDetailsHarness) current(ctx context.Context) *cnsvolumeinfov1alpha1.CNSVolumeInfo {
	h.t.Helper()
	h.mu.Lock()
	gets := h.gets
	h.mu.Unlock()
	cvi := h.read(ctx, h.backend)
	h.mu.Lock()
	h.gets = gets
	h.mu.Unlock()
	return cvi
}

func newSnapshotCVI(capacityInMb int64) *cnsvolumeinfov1alpha1.CNSVolumeInfo {
	return &cnsvolumeinfov1alpha1.CNSVolumeInfo{
		ObjectMeta: metav1.ObjectMeta{Name: testSnapshotVolumeID, Namespace: csiNamespace},
		Spec: cnsvolumeinfov1alpha1.CNSVolumeInfoSpec{
			VolumeID:                    testSnapshotVolumeID,
			ValidAggregatedSnapshotSize: true,
			AggregatedSnapshotSize:      resource.NewQuantity(capacityInMb*common.MbInBytes, resource.BinarySI),
		},
	}
}

func measurement(capacityInMb int64) *cnsvolume.CnsSnapshotInfo {
	return &cnsvolume.CnsSnapshotInfo{
		SourceVolumeID:                      testSnapshotVolumeID,
		AggregatedSnapshotCapacityInMb:      capacityInMb,
		SnapshotLatestOperationCompleteTime: time.Now(),
	}
}

func sizeMb(cvi *cnsvolumeinfov1alpha1.CNSVolumeInfo) int64 {
	if cvi.Spec.AggregatedSnapshotSize == nil {
		return 0
	}
	return cvi.Spec.AggregatedSnapshotSize.Value() / common.MbInBytes
}

// concurrentSnapshotWrite writes a new aggregated snapshot size the way DPO would, with an Update.
func concurrentSnapshotWrite(ctx context.Context, c client.Client, capacityInMb int64) error {
	cvi := &cnsvolumeinfov1alpha1.CNSVolumeInfo{}
	key := types.NamespacedName{Namespace: csiNamespace, Name: testSnapshotVolumeID}
	if err := c.Get(ctx, key, cvi); err != nil {
		return err
	}
	cvi.Spec.AggregatedSnapshotSize = resource.NewQuantity(capacityInMb*common.MbInBytes, resource.BinarySI)
	cvi.Spec.ValidAggregatedSnapshotSize = true
	return c.Update(ctx, cvi)
}

func withFastRetries(t *testing.T) {
	t.Helper()
	saved := snapshotDetailsRetryInterval
	snapshotDetailsRetryInterval = time.Millisecond
	t.Cleanup(func() { snapshotDetailsRetryInterval = saved })
}

func TestUpdateVolumeInfoSnapshotDetailsCommonPathWritesMeasurement(t *testing.T) {
	ctx := logger.NewContextWithLogger(context.Background())
	h := newSnapshotDetailsHarness(t, newSnapshotCVI(5))
	preRead := h.current(ctx)

	err := h.service.UpdateVolumeInfoSnapshotDetails(ctx, testSnapshotVolumeID, preRead.ResourceVersion,
		measurement(10), h.cnsReporting(0, errors.New("must not be called")))

	require.NoError(t, err)
	cvi := h.current(ctx)
	assert.Equal(t, int64(10), sizeMb(cvi))
	assert.True(t, cvi.Spec.ValidAggregatedSnapshotSize)
	assert.Equal(t, 1, h.patchAttempts, "one conditional patch")
	assert.Equal(t, 0, h.gets, "no extra read on the common path")
	assert.Equal(t, 0, h.remeasureCalls, "no extra CNS query on the common path")
}

// A concurrent writer (e.g. DPO) writes between the pre-read and the CSI write. The older CSI
// measurement must never be applied; the re-measured value must be.
func TestUpdateVolumeInfoSnapshotDetailsConcurrentWriteIsNotOverwritten(t *testing.T) {
	ctx := logger.NewContextWithLogger(context.Background())
	h := newSnapshotDetailsHarness(t, newSnapshotCVI(5))
	preRead := h.current(ctx)
	h.beforePatch = func(ctx context.Context, c client.WithWatch, attempt int) (bool, error) {
		if attempt == 1 {
			require.NoError(t, concurrentSnapshotWrite(ctx, c, 12))
		}
		return true, nil
	}

	err := h.service.UpdateVolumeInfoSnapshotDetails(ctx, testSnapshotVolumeID, preRead.ResourceVersion,
		measurement(10), h.cnsReporting(12, nil))

	require.NoError(t, err)
	assert.Equal(t, int64(12), sizeMb(h.current(ctx)))
	assert.Equal(t, []int64{12}, h.appliedMb, "the stale 10 MB measurement was never written")
	assert.Equal(t, 1, h.remeasureCalls)
	assert.Equal(t, 1, h.gets, "one live read after the conflict")
}

// Two CSI operations on one volume measured in one order and written in the other (spec E3).
func TestUpdateVolumeInfoSnapshotDetailsOutOfOrderResults(t *testing.T) {
	ctx := logger.NewContextWithLogger(context.Background())
	h := newSnapshotDetailsHarness(t, newSnapshotCVI(10))
	preRead := h.current(ctx)

	// Operation B measured 10 MB after operation A's 6 MB, and writes first.
	require.NoError(t, h.service.UpdateVolumeInfoSnapshotDetails(ctx, testSnapshotVolumeID,
		preRead.ResourceVersion, measurement(10), h.cnsReporting(0, errors.New("unexpected"))))
	// Operation A writes its older 6 MB with the same pre-read version: it must re-measure.
	require.NoError(t, h.service.UpdateVolumeInfoSnapshotDetails(ctx, testSnapshotVolumeID,
		preRead.ResourceVersion, measurement(6), h.cnsReporting(10, nil)))

	assert.Equal(t, int64(10), sizeMb(h.current(ctx)))
	assert.Equal(t, []int64{10, 10}, h.appliedMb, "the late 6 MB measurement was never written")
}

// Any change to the CnsVolumeInfo, even unrelated, conflicts; it costs a re-measure but no correctness.
func TestUpdateVolumeInfoSnapshotDetailsUnrelatedChangeRemeasures(t *testing.T) {
	ctx := logger.NewContextWithLogger(context.Background())
	h := newSnapshotDetailsHarness(t, newSnapshotCVI(5))
	preRead := h.current(ctx)
	h.beforePatch = func(ctx context.Context, c client.WithWatch, attempt int) (bool, error) {
		if attempt == 1 {
			cvi := h.read(ctx, c)
			cvi.Finalizers = append(cvi.Finalizers, "example.com/protect")
			require.NoError(t, c.Update(ctx, cvi))
		}
		return true, nil
	}

	err := h.service.UpdateVolumeInfoSnapshotDetails(ctx, testSnapshotVolumeID, preRead.ResourceVersion,
		measurement(10), h.cnsReporting(10, nil))

	require.NoError(t, err)
	cvi := h.current(ctx)
	assert.Equal(t, int64(10), sizeMb(cvi))
	assert.Equal(t, []string{"example.com/protect"}, cvi.Finalizers, "the merge patch keeps other fields")
	assert.Equal(t, 1, h.remeasureCalls)
}

// A stale cached pre-read only causes a false conflict, never a stale write.
func TestUpdateVolumeInfoSnapshotDetailsStalePreReadVersion(t *testing.T) {
	ctx := logger.NewContextWithLogger(context.Background())
	h := newSnapshotDetailsHarness(t, newSnapshotCVI(5))
	stale := h.current(ctx).ResourceVersion
	require.NoError(t, concurrentSnapshotWrite(ctx, h.backend, 7))
	h.gets = 0

	err := h.service.UpdateVolumeInfoSnapshotDetails(ctx, testSnapshotVolumeID, stale, measurement(10),
		h.cnsReporting(8, nil))

	require.NoError(t, err)
	assert.Equal(t, int64(8), sizeMb(h.current(ctx)))
	assert.Equal(t, []int64{8}, h.appliedMb)
	assert.Equal(t, 2, h.patchAttempts)
}

func TestUpdateVolumeInfoSnapshotDetailsWithoutPreReadRemeasures(t *testing.T) {
	for name, tc := range map[string]struct {
		preReadVersion bool
		measured       *cnsvolume.CnsSnapshotInfo
	}{
		"no pre-read version": {preReadVersion: false, measured: measurement(10)},
		"no measurement":      {preReadVersion: true, measured: nil},
	} {
		t.Run(name, func(t *testing.T) {
			ctx := logger.NewContextWithLogger(context.Background())
			h := newSnapshotDetailsHarness(t, newSnapshotCVI(5))
			rv := ""
			if tc.preReadVersion {
				rv = h.current(ctx).ResourceVersion
			}

			err := h.service.UpdateVolumeInfoSnapshotDetails(ctx, testSnapshotVolumeID, rv, tc.measured,
				h.cnsReporting(9, nil))

			require.NoError(t, err)
			assert.Equal(t, int64(9), sizeMb(h.current(ctx)))
			assert.Equal(t, []int64{9}, h.appliedMb)
			assert.Equal(t, 1, h.remeasureCalls)
			assert.Equal(t, 1, h.gets)
			assert.Equal(t, 1, h.patchAttempts)
		})
	}
}

func TestUpdateVolumeInfoSnapshotDetailsRemeasureFailureLeavesCVIUnchanged(t *testing.T) {
	ctx := logger.NewContextWithLogger(context.Background())
	h := newSnapshotDetailsHarness(t, newSnapshotCVI(5))
	preRead := h.current(ctx)
	h.beforePatch = func(ctx context.Context, c client.WithWatch, attempt int) (bool, error) {
		require.NoError(t, concurrentSnapshotWrite(ctx, c, 12))
		return true, nil
	}

	err := h.service.UpdateVolumeInfoSnapshotDetails(ctx, testSnapshotVolumeID, preRead.ResourceVersion,
		measurement(10), h.cnsReporting(0, errors.New("CNS unavailable")))

	require.Error(t, err)
	assert.Equal(t, int64(12), sizeMb(h.current(ctx)), "the concurrent writer's value stands")
	assert.Empty(t, h.appliedMb)
	assert.Equal(t, 1, h.patchAttempts, "nothing is written without a measurement")
	assert.Equal(t, maxSnapshotDetailsWriteAttempts-1, h.remeasureCalls, "every remaining attempt re-measures")
}

// A transient failure while re-measuring uses up an attempt; the next attempt re-measures and writes.
func TestUpdateVolumeInfoSnapshotDetailsTransientRemeasureFailureRetries(t *testing.T) {
	ctx := logger.NewContextWithLogger(context.Background())
	h := newSnapshotDetailsHarness(t, newSnapshotCVI(5))
	preRead := h.current(ctx)
	h.beforePatch = func(ctx context.Context, c client.WithWatch, attempt int) (bool, error) {
		if attempt == 1 {
			require.NoError(t, concurrentSnapshotWrite(ctx, c, 12))
		}
		return true, nil
	}

	err := h.service.UpdateVolumeInfoSnapshotDetails(ctx, testSnapshotVolumeID, preRead.ResourceVersion,
		measurement(10), &capacityReportingManager{h: h, capacityInMb: 12, transientFailures: 1})

	require.NoError(t, err)
	assert.Equal(t, int64(12), sizeMb(h.current(ctx)))
	assert.Equal(t, []int64{12}, h.appliedMb, "the stale 10 MB measurement was never written")
	assert.Equal(t, 2, h.remeasureCalls, "one failed and one successful re-measure")
	assert.Equal(t, 2, h.patchAttempts)
}

func TestUpdateVolumeInfoSnapshotDetailsConflictsExhaustAttempts(t *testing.T) {
	ctx := logger.NewContextWithLogger(context.Background())
	h := newSnapshotDetailsHarness(t, newSnapshotCVI(5))
	preRead := h.current(ctx)
	h.beforePatch = func(ctx context.Context, c client.WithWatch, attempt int) (bool, error) {
		require.NoError(t, concurrentSnapshotWrite(ctx, c, int64(100+attempt)))
		return true, nil
	}

	err := h.service.UpdateVolumeInfoSnapshotDetails(ctx, testSnapshotVolumeID, preRead.ResourceVersion,
		measurement(10), h.cnsReporting(20, nil))

	require.Error(t, err)
	assert.Equal(t, maxSnapshotDetailsWriteAttempts, h.patchAttempts)
	assert.Equal(t, maxSnapshotDetailsWriteAttempts-1, h.remeasureCalls)
	assert.Empty(t, h.appliedMb, "no CSI write was applied")
	assert.Equal(t, int64(100+maxSnapshotDetailsWriteAttempts), sizeMb(h.current(ctx)))
}

func TestUpdateVolumeInfoSnapshotDetailsTransientErrorRetriesSameVersion(t *testing.T) {
	ctx := logger.NewContextWithLogger(context.Background())
	h := newSnapshotDetailsHarness(t, newSnapshotCVI(5))
	preRead := h.current(ctx)
	h.beforePatch = func(ctx context.Context, c client.WithWatch, attempt int) (bool, error) {
		if attempt == 1 {
			return false, errors.New("connection reset")
		}
		return true, nil
	}

	err := h.service.UpdateVolumeInfoSnapshotDetails(ctx, testSnapshotVolumeID, preRead.ResourceVersion,
		measurement(10), h.cnsReporting(0, errors.New("must not be called")))

	require.NoError(t, err)
	assert.Equal(t, int64(10), sizeMb(h.current(ctx)))
	assert.Equal(t, 2, h.patchAttempts)
	assert.Equal(t, 0, h.remeasureCalls, "a transient failure does not discard the measurement")
}

// The API server applied the write but the response was lost. The retry conflicts with CSI's own
// write and re-measures, so the result is still correct.
func TestUpdateVolumeInfoSnapshotDetailsLostResponseAfterApply(t *testing.T) {
	ctx := logger.NewContextWithLogger(context.Background())
	h := newSnapshotDetailsHarness(t, newSnapshotCVI(5))
	preRead := h.current(ctx)
	h.beforePatch = func(ctx context.Context, c client.WithWatch, attempt int) (bool, error) {
		if attempt == 1 {
			patch := []byte(`{"spec":{"aggregatedsnapshotsize":"10Mi","validaggregatedsnapshotsize":true}}`)
			target := &cnsvolumeinfov1alpha1.CNSVolumeInfo{ObjectMeta: metav1.ObjectMeta{
				Name: testSnapshotVolumeID, Namespace: csiNamespace}}
			require.NoError(t, c.Patch(ctx, target, client.RawPatch(types.MergePatchType, patch)))
			return false, errors.New("timeout awaiting response")
		}
		return true, nil
	}

	err := h.service.UpdateVolumeInfoSnapshotDetails(ctx, testSnapshotVolumeID, preRead.ResourceVersion,
		measurement(10), h.cnsReporting(10, nil))

	require.NoError(t, err)
	assert.Equal(t, int64(10), sizeMb(h.current(ctx)))
	assert.Equal(t, 1, h.remeasureCalls)
}

func TestUpdateVolumeInfoSnapshotDetailsNotFound(t *testing.T) {
	ctx := logger.NewContextWithLogger(context.Background())
	h := newSnapshotDetailsHarness(t)

	err := h.service.UpdateVolumeInfoSnapshotDetails(ctx, testSnapshotVolumeID, "7", measurement(10),
		h.cnsReporting(0, errors.New("must not be called")))
	require.Error(t, err)
	assert.Equal(t, 1, h.patchAttempts)

	err = h.service.UpdateVolumeInfoSnapshotDetails(ctx, testSnapshotVolumeID, "", nil,
		h.cnsReporting(0, errors.New("must not be called")))
	require.Error(t, err)
	assert.Equal(t, 0, h.remeasureCalls, "a missing CnsVolumeInfo is not re-measured")
}

func TestUpdateVolumeInfoSnapshotDetailsInvalidCapacity(t *testing.T) {
	ctx := logger.NewContextWithLogger(context.Background())
	h := newSnapshotDetailsHarness(t, newSnapshotCVI(5))
	preRead := h.current(ctx)

	err := h.service.UpdateVolumeInfoSnapshotDetails(ctx, testSnapshotVolumeID, preRead.ResourceVersion,
		measurement(-1), h.cnsReporting(0, errors.New("must not be called")))

	require.NoError(t, err)
	cvi := h.current(ctx)
	assert.False(t, cvi.Spec.ValidAggregatedSnapshotSize)
	assert.Equal(t, int64(5), sizeMb(cvi), "the last known size is kept")
}

// A replayed result may carry no completion time; the write must not clear the stored one.
func TestUpdateVolumeInfoSnapshotDetailsZeroCompletionTime(t *testing.T) {
	ctx := logger.NewContextWithLogger(context.Background())
	cvi := newSnapshotCVI(5)
	cvi.Spec.SnapshotLatestOperationCompleteTime = metav1.NewTime(time.Now().Add(-time.Hour))
	h := newSnapshotDetailsHarness(t, cvi)
	preRead := h.current(ctx)
	start := time.Now().Truncate(time.Second)
	measured := measurement(10)
	measured.SnapshotLatestOperationCompleteTime = time.Time{}

	err := h.service.UpdateVolumeInfoSnapshotDetails(ctx, testSnapshotVolumeID, preRead.ResourceVersion,
		measured, h.cnsReporting(0, errors.New("must not be called")))

	require.NoError(t, err)
	written := h.current(ctx).Spec.SnapshotLatestOperationCompleteTime.Time
	assert.False(t, written.Before(start), "the current time is written instead of clearing the field")
}

func TestSnapshotDetailsPatchCarriesResourceVersion(t *testing.T) {
	ctx := logger.NewContextWithLogger(context.Background())
	patchBytes, err := snapshotDetailsPatch(ctx, testSnapshotVolumeID, "42", 3, time.Now())
	require.NoError(t, err)

	var patch map[string]map[string]interface{}
	require.NoError(t, json.Unmarshal(patchBytes, &patch))
	assert.Equal(t, "42", patch["metadata"]["resourceVersion"])
	assert.Equal(t, "3Mi", patch["spec"]["aggregatedsnapshotsize"])
	assert.Equal(t, true, patch["spec"]["validaggregatedsnapshotsize"])
}
