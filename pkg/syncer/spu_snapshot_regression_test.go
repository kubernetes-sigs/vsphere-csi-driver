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
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"k8s.io/apimachinery/pkg/api/resource"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	apitypes "k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"

	storagepolicyv1alpha3 "sigs.k8s.io/vsphere-csi-driver/v3/pkg/apis/cnsoperator/storagepolicy/v1alpha3"
)

// The cache is updated before informer callbacks run. A full sync can therefore
// reconcile the new total before the callback for that same change is delivered.
// Replaying that callback must not add or subtract the change a second time.
func TestSPUSnapshotDelayedCallbackAfterFullSync(t *testing.T) {
	for _, tc := range []struct {
		name, before, after string
	}{
		{name: "increase", before: "100Mi", after: "150Mi"},
		{name: "decrease", before: "150Mi", after: "100Mi"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			patches := 0
			funcs := interceptor.Funcs{SubResourcePatch: func(ctx context.Context, cl client.Client,
				sub string, obj client.Object, p client.Patch, opts ...client.SubResourcePatchOption) error {
				patches++
				return cl.SubResource(sub).Patch(ctx, obj, p, opts...)
			}}
			e := newSPUTestEnv(t, funcs, testSPU(tc.before))
			old := testCVI("a", "gold", "", testPolicyID, tc.before, true, "")
			updated := testCVI("a", "gold", "", testPolicyID, tc.after, true, "")
			e.put(t, old)
			require.NoError(t, e.informer.GetIndexer().Update(updated))

			// Full sync enqueues the SPU after the cache has already changed.
			e.queue.Add(testSPUKey)
			require.True(t, e.r.processNext(context.Background()))
			expected := resource.MustParse(tc.after)
			require.Zero(t, e.spu(t).Status.ResourceTypeLevelQuotaUsage.Used.Cmp(expected))

			// Deliver the delayed update callback, including both affected keys.
			require.True(t, snapshotFieldsChanged(old, updated))
			e.r.enqueue(old)
			e.r.enqueue(updated)
			require.Equal(t, 1, e.queue.Len())
			require.True(t, e.r.processNext(context.Background()))
			assert.Zero(t, e.spu(t).Status.ResourceTypeLevelQuotaUsage.Used.Cmp(expected))
			assert.Equal(t, 1, patches, "the delayed callback should observe the already-correct total")
			assert.Zero(t, e.queue.Len())
		})
	}
}

func TestSPUSnapshotChangeDuringReconcileQueuesAnotherPass(t *testing.T) {
	var e *spuTestEnv
	patches := 0
	funcs := interceptor.Funcs{SubResourcePatch: func(ctx context.Context, cl client.Client,
		sub string, obj client.Object, p client.Patch, opts ...client.SubResourcePatchOption) error {
		patches++
		if patches == 1 {
			// The worker has summed 100Mi, but the cache changes before that sum
			// is published. The update marks the in-flight key dirty.
			updated := testCVI("a", "gold", "", testPolicyID, "150Mi", true, "")
			require.NoError(t, e.informer.GetIndexer().Update(updated))
			e.r.enqueue(updated)
			assert.Zero(t, e.queue.Len(), "an in-flight key must not be handed to a second worker")
		}
		return cl.SubResource(sub).Patch(ctx, obj, p, opts...)
	}}
	e = newSPUTestEnv(t, funcs, testSPU("0"))
	e.put(t, testCVI("a", "gold", "", testPolicyID, "100Mi", true, ""))
	e.queue.Add(testSPUKey)

	require.True(t, e.r.processNext(context.Background()))
	assert.Equal(t, int64(100*1024*1024), e.spu(t).Status.ResourceTypeLevelQuotaUsage.Used.Value())
	require.Equal(t, 1, e.queue.Len(), "Done must schedule the event received during the first pass")
	require.True(t, e.r.processNext(context.Background()))
	assert.Equal(t, int64(150*1024*1024), e.spu(t).Status.ResourceTypeLevelQuotaUsage.Used.Value())
	assert.Equal(t, 2, patches)
	assert.Zero(t, e.queue.Len())
}

func TestSPUSnapshotPatchRetryResumsChangedInventory(t *testing.T) {
	var e *spuTestEnv
	patches := 0
	funcs := interceptor.Funcs{SubResourcePatch: func(ctx context.Context, cl client.Client,
		sub string, obj client.Object, p client.Patch, opts ...client.SubResourcePatchOption) error {
		patches++
		if patches == 1 {
			// A retry of the previously computed 100Mi patch would be stale.
			require.NoError(t, e.informer.GetIndexer().Update(
				testCVI("a", "gold", "", testPolicyID, "150Mi", true, "")))
			return errors.New("transient status patch failure")
		}
		return cl.SubResource(sub).Patch(ctx, obj, p, opts...)
	}}
	e = newSPUTestEnv(t, funcs, testSPU("0"))
	e.put(t, testCVI("a", "gold", "", testPolicyID, "100Mi", true, ""))

	require.NoError(t, e.r.reconcile(context.Background(), testSPUKey))
	assert.Equal(t, 2, patches)
	assert.Equal(t, int64(150*1024*1024), e.spu(t).Status.ResourceTypeLevelQuotaUsage.Used.Value())
}

func TestSPUSnapshotUsedInitializationOmitsReserved(t *testing.T) {
	for _, tc := range []struct {
		name  string
		quota *storagepolicyv1alpha3.QuotaUsageDetails
	}{
		{name: "missing quotaUsage"},
		{name: "missing Used", quota: &storagepolicyv1alpha3.QuotaUsageDetails{}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			patches := 0
			funcs := interceptor.Funcs{SubResourcePatch: func(ctx context.Context, cl client.Client,
				sub string, obj client.Object, p client.Patch, opts ...client.SubResourcePatchOption) error {
				patches++
				data, err := p.Data(obj)
				require.NoError(t, err)
				var patch map[string]interface{}
				require.NoError(t, json.Unmarshal(data, &patch))
				assert.Equal(t, map[string]interface{}{
					"status": map[string]interface{}{
						"quotaUsage": map[string]interface{}{"used": "1Gi"},
					},
				}, patch, "initialization must not include Reserved, even as null")

				// Simulate CVOR initialization after the worker's GET. Apply a
				// Reserved-only patch directly through the underlying client.
				reservedPatch := client.RawPatch(apitypes.MergePatchType,
					[]byte(`{"status":{"quotaUsage":{"reserved":"10Gi"}}}`))
				require.NoError(t, cl.SubResource(sub).Patch(ctx, obj.DeepCopyObject().(client.Object), reservedPatch))
				return cl.SubResource(sub).Patch(ctx, obj, p, opts...)
			}}
			spu := testSPU("0")
			spu.Status.ResourceTypeLevelQuotaUsage = tc.quota
			e := newSPUTestEnv(t, funcs, spu)
			e.put(t, testCVI("a", "gold", "", testPolicyID, "1Gi", true, ""))

			require.NoError(t, e.r.reconcile(context.Background(), testSPUKey))
			got := e.spu(t).Status.ResourceTypeLevelQuotaUsage
			require.NotNil(t, got)
			require.NotNil(t, got.Used)
			require.NotNil(t, got.Reserved)
			assert.Equal(t, int64(1024*1024*1024), got.Used.Value())
			assert.Equal(t, int64(10*1024*1024*1024), got.Reserved.Value())
			assert.Equal(t, 1, patches)
		})
	}
}

func TestSPUSnapshotFieldTypeChangesTriggerReconcile(t *testing.T) {
	for _, tc := range []struct {
		field         string
		before, after interface{}
	}{
		{field: "aggregatedsnapshotsize", before: int64(100), after: "100"},
		{field: "validaggregatedsnapshotsize", before: "true", after: true},
		{field: "snapshotlatestoperationcompletetime", before: int64(123), after: "123"},
		{field: "storagePolicyID", before: int64(7), after: "7"},
	} {
		t.Run(tc.field, func(t *testing.T) {
			old := testCVI("a", "gold", "", testPolicyID, "100Mi", true, "")
			require.NoError(t, unstructured.SetNestedField(old.Object, tc.before, "spec", tc.field))
			updated := old.DeepCopy()
			require.NoError(t, unstructured.SetNestedField(updated.Object, tc.after, "spec", tc.field))
			assert.True(t, snapshotFieldsChanged(old, updated), "invalid-to-valid type changes must be reconciled")
			assert.True(t, snapshotFieldsChanged(updated, old), "valid-to-invalid type changes must be reconciled")
			assert.False(t, snapshotFieldsChanged(updated, updated.DeepCopy()))
		})
	}
}
