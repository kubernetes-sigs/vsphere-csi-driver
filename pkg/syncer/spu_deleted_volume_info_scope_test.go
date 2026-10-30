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
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/client-go/tools/cache"
	"sigs.k8s.io/controller-runtime/pkg/client"

	storagepolicyv1alpha3 "sigs.k8s.io/vsphere-csi-driver/v3/pkg/apis/cnsoperator/storagepolicy/v1alpha3"
	"sigs.k8s.io/vsphere-csi-driver/v3/pkg/csi/service/common"
)

func TestDeletedVolumeInfoUnknownNamespaceQuarantinedUntilCorrected(t *testing.T) {
	for _, tc := range []struct {
		name  string
		value interface{}
	}{
		{"missing", nil},
		{"null", nil},
		{"empty", ""},
		{"wrong type", int64(1)},
		{"path", "other-tenant/gold-snapshot-usage"},
		{"uppercase", "Tenant"},
		{"domain", "tenant.example"},
		{"too long", strings.Repeat("n", 64)},
	} {
		t.Run(tc.name, func(t *testing.T) {
			e := newDVITestEnv(t, testSPU("100Mi"))
			e.put(t, testCVI("live", "gold", "", testPolicyID, "10Mi", true, ""))
			bad := testDVI("deleted", "gold", "", testPolicyID, 25)
			unstructured.RemoveNestedField(bad.Object, "spec", "namespace")
			if tc.name != "missing" {
				require.NoError(t, unstructured.SetNestedField(bad.Object, tc.value, "spec", "namespace"))
			}
			putDVI(t, e, bad)
			e.r.enqueueDeletedVolumeInfo(bad)
			require.Zero(t, e.queue.Len(), "unknown tenant must not trigger a supervisor-wide hold or sweep")
			require.NoError(t, e.r.reconcile(context.Background(), testSPUKey))
			assert.Equal(t, int64(10*1024*1024), e.spu(t).Status.ResourceTypeLevelQuotaUsage.Used.Value())
			assert.NotContains(t, e.spu(t).Annotations, common.MissingSnapshotAggregatedCapacity)

			fixed := testDVI("deleted", "gold", "", testPolicyID, 25)
			putDVI(t, e, fixed)
			e.r.enqueueDeletedVolumeInfo(bad)
			e.r.enqueueDeletedVolumeInfo(fixed)
			require.Equal(t, 1, e.queue.Len())
			require.True(t, e.r.processNext(context.Background()))
			assert.Equal(t, int64(10*1024*1024+25), e.spu(t).Status.ResourceTypeLevelQuotaUsage.Used.Value())
		})
	}
}

func TestDeletedVolumeInfoMalformedStorageAssociationHoldsOnlyTenant(t *testing.T) {
	for _, tc := range []struct {
		name, field string
		value       interface{}
	}{
		{"missing storage class", "storageClassName", nil},
		{"empty storage class", "storageClassName", ""},
		{"wrong storage class type", "storageClassName", true},
		{"invalid storage class", "storageClassName", "gold/silver"},
		{"wrong VAC type", "volumeAttributeClassName", true},
		{"invalid VAC", "volumeAttributeClassName", "fast/slow"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			silver := testSPU("50Mi")
			silver.Name = "silver-snapshot-usage"
			other := testSPU("75Mi")
			other.Namespace = "other-tenant"
			e := newDVITestEnv(t, testSPU("100Mi"), silver, other)
			bad := testDVI("deleted", "gold", "", testPolicyID, 25)
			require.NoError(t, unstructured.SetNestedField(bad.Object, tc.value, "spec", tc.field))
			putDVI(t, e, bad)
			e.r.enqueueDeletedVolumeInfo(bad)
			require.True(t, e.r.processNext(context.Background()))
			assert.ElementsMatch(t, []string{testSPUKey, testSPUNamespace + "/" + silver.Name}, drainDVIQueue(e))
			for _, spu := range []*storagepolicyv1alpha3.StoragePolicyUsage{testSPU("100Mi"), silver} {
				require.Error(t, e.r.recomputeOnce(context.Background(), spu.Namespace, spu.Name,
					spu.Namespace+"/"+spu.Name))
				got := &storagepolicyv1alpha3.StoragePolicyUsage{}
				require.NoError(t, e.client.Get(context.Background(), client.ObjectKeyFromObject(spu), got))
				assert.Zero(t, got.Status.ResourceTypeLevelQuotaUsage.Used.Cmp(*spu.Status.ResourceTypeLevelQuotaUsage.Used))
				assert.Equal(t, "true", got.Annotations[common.MissingSnapshotAggregatedCapacity])
			}
			require.NoError(t, e.r.reconcile(context.Background(), other.Namespace+"/"+other.Name))
			got := &storagepolicyv1alpha3.StoragePolicyUsage{}
			require.NoError(t, e.client.Get(context.Background(), client.ObjectKeyFromObject(other), got))
			assert.True(t, got.Status.ResourceTypeLevelQuotaUsage.Used.IsZero())
			assert.NotContains(t, got.Annotations, common.MissingSnapshotAggregatedCapacity)

			fixed := testDVI("deleted", "gold", "", testPolicyID, 25)
			putDVI(t, e, fixed)
			e.r.enqueueDeletedVolumeInfo(bad)
			e.r.enqueueDeletedVolumeInfo(fixed)
			for e.queue.Len() != 0 {
				require.True(t, e.r.processNext(context.Background()))
			}
			assert.Equal(t, int64(25), e.spu(t).Status.ResourceTypeLevelQuotaUsage.Used.Value())
			assert.NotContains(t, e.spu(t).Annotations, common.MissingSnapshotAggregatedCapacity)
			require.NoError(t, e.client.Get(context.Background(), client.ObjectKeyFromObject(silver), got))
			assert.True(t, got.Status.ResourceTypeLevelQuotaUsage.Used.IsZero())
			assert.NotContains(t, got.Annotations, common.MissingSnapshotAggregatedCapacity)
		})
	}
}

func TestDeletedVolumeInfoMalformedTenantMoveAndDeletionRecovery(t *testing.T) {
	other := testSPU("75Mi")
	other.Namespace = "other-tenant"
	e := newDVITestEnv(t, testSPU("100Mi"), other)
	bad := testDVI("deleted", "", "", testPolicyID, 25)
	putDVI(t, e, bad)
	require.Error(t, e.r.recomputeOnce(context.Background(), testSPUNamespace, testSPUName, testSPUKey))
	moved := bad.DeepCopy()
	require.NoError(t, unstructured.SetNestedField(moved.Object, other.Namespace, "spec", "namespace"))
	putDVI(t, e, moved)
	e.r.enqueueDeletedVolumeInfo(bad)
	e.r.enqueueDeletedVolumeInfo(moved)
	require.True(t, e.r.processNext(context.Background()))
	require.True(t, e.r.processNext(context.Background()))
	assert.ElementsMatch(t, []string{testSPUKey, other.Namespace + "/" + other.Name}, drainDVIQueue(e))
	require.NoError(t, e.r.reconcile(context.Background(), testSPUKey))
	assert.True(t, e.spu(t).Status.ResourceTypeLevelQuotaUsage.Used.IsZero())
	assert.NotContains(t, e.spu(t).Annotations, common.MissingSnapshotAggregatedCapacity)
	require.Error(t, e.r.recomputeOnce(context.Background(), other.Namespace, other.Name,
		other.Namespace+"/"+other.Name))

	require.NoError(t, e.r.dviInformer.GetIndexer().Delete(moved))
	e.r.enqueueDeletedVolumeInfo(cache.DeletedFinalStateUnknown{Obj: moved})
	require.True(t, e.r.processNext(context.Background()))
	require.Equal(t, 1, e.queue.Len())
	require.True(t, e.r.processNext(context.Background()))
	got := &storagepolicyv1alpha3.StoragePolicyUsage{}
	require.NoError(t, e.client.Get(context.Background(), client.ObjectKeyFromObject(other), got))
	assert.True(t, got.Status.ResourceTypeLevelQuotaUsage.Used.IsZero())
	assert.NotContains(t, got.Annotations, common.MissingSnapshotAggregatedCapacity)
}

func TestDeletedVolumeInfoValidVACHoldsOnlyKnownSPUWhenStorageClassMalformed(t *testing.T) {
	for _, tc := range []struct {
		name  string
		value interface{}
	}{
		{"missing", nil},
		{"null", nil},
		{"empty", ""},
		{"wrong type", true},
		{"invalid name", "gold/silver"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			fast := testSPU("75Mi")
			fast.Name = "vac-fast-snapshot-usage"
			slow := testSPU("50Mi")
			slow.Name = "vac-slow-snapshot-usage"
			e := newDVITestEnv(t, testSPU("100Mi"), fast, slow)
			e.put(t, testCVI("live", "gold", "", testPolicyID, "10Mi", true, ""))
			bad := testDVI("deleted", "gold", "fast", testPolicyID, 25)
			unstructured.RemoveNestedField(bad.Object, "spec", "storageClassName")
			if tc.name != "missing" {
				require.NoError(t, unstructured.SetNestedField(bad.Object, tc.value, "spec", "storageClassName"))
			}
			putDVI(t, e, bad)
			e.r.enqueueDeletedVolumeInfo(bad)
			fastKey := testSPUNamespace + "/" + fast.Name
			assert.Equal(t, []string{fastKey}, drainDVIQueue(e),
				"the valid VAC must scope malformed SC handling to its exact SPU")
			require.Error(t, e.r.recomputeOnce(context.Background(), fast.Namespace, fast.Name, fastKey))
			got := &storagepolicyv1alpha3.StoragePolicyUsage{}
			require.NoError(t, e.client.Get(context.Background(), client.ObjectKeyFromObject(fast), got))
			assert.Equal(t, int64(75*1024*1024), got.Status.ResourceTypeLevelQuotaUsage.Used.Value())
			assert.Equal(t, "true", got.Annotations[common.MissingSnapshotAggregatedCapacity])

			require.NoError(t, e.r.reconcile(context.Background(), testSPUKey))
			assert.Equal(t, int64(10*1024*1024), e.spu(t).Status.ResourceTypeLevelQuotaUsage.Used.Value())
			assert.NotContains(t, e.spu(t).Annotations, common.MissingSnapshotAggregatedCapacity)
			require.NoError(t, e.r.reconcile(context.Background(), testSPUNamespace+"/"+slow.Name))
			require.NoError(t, e.client.Get(context.Background(), client.ObjectKeyFromObject(slow), got))
			assert.True(t, got.Status.ResourceTypeLevelQuotaUsage.Used.IsZero())
			assert.NotContains(t, got.Annotations, common.MissingSnapshotAggregatedCapacity)

			fixed := testDVI("deleted", "gold", "fast", testPolicyID, 25)
			putDVI(t, e, fixed)
			e.r.enqueueDeletedVolumeInfo(fixed)
			require.True(t, e.r.processNext(context.Background()))
			require.NoError(t, e.client.Get(context.Background(), client.ObjectKeyFromObject(fast), got))
			assert.Equal(t, int64(25), got.Status.ResourceTypeLevelQuotaUsage.Used.Value())
			assert.NotContains(t, got.Annotations, common.MissingSnapshotAggregatedCapacity)
		})
	}
}
