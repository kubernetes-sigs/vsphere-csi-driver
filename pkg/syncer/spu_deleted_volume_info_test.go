package syncer

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/watch"
	dynamicfake "k8s.io/client-go/dynamic/fake"
	clienttesting "k8s.io/client-go/testing"
	"k8s.io/client-go/tools/cache"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"

	storagepolicyv1alpha3 "sigs.k8s.io/vsphere-csi-driver/v3/pkg/apis/cnsoperator/storagepolicy/v1alpha3"
	"sigs.k8s.io/vsphere-csi-driver/v3/pkg/csi/service/common"
	"sigs.k8s.io/vsphere-csi-driver/v3/pkg/internalapis/cnsvolumeinfo"
)

const testDVISystemNamespace = "tenant-system-ns"

func testDVI(name, sc, vac, policy string, size int64, valid bool) *unstructured.Unstructured {
	spec := map[string]interface{}{
		"volumeID": name, "namespace": testSPUNamespace, "storageClassName": sc,
		"storagePolicyID": policy, "sizeBytes": size, "validSize": valid,
	}
	if vac != "" {
		spec["volumeAttributeClassName"] = vac
	}
	return &unstructured.Unstructured{Object: map[string]interface{}{
		"apiVersion": "protectionandrecovery.vmware.com/v1alpha1", "kind": "DeletedVolumeInfo",
		"metadata": map[string]interface{}{"name": name, "namespace": testDVISystemNamespace},
		"spec":     spec,
	}}
}

func testDVIDynamicClient(objs ...runtime.Object) *dynamicfake.FakeDynamicClient {
	return dynamicfake.NewSimpleDynamicClientWithCustomListKinds(runtime.NewScheme(),
		map[schema.GroupVersionResource]string{deletedVolumeInfoGVR: "DeletedVolumeInfoList"}, objs...)
}

func newDVITestEnv(t *testing.T, spus ...*storagepolicyv1alpha3.StoragePolicyUsage) *spuTestEnv {
	t.Helper()
	e := newSPUTestEnv(t, interceptor.Funcs{}, spus...)
	var err error
	e.r, err = newSPUSnapshotReconciler(e.client, e.informer, e.queue,
		deletedVolumeInfoInformer(testDVIDynamicClient()))
	require.NoError(t, err)
	return e
}

func putDVI(t *testing.T, e *spuTestEnv, objs ...*unstructured.Unstructured) {
	t.Helper()
	for _, obj := range objs {
		require.NoError(t, e.r.dviInformer.GetIndexer().Add(obj))
	}
}

func drainDVIQueue(e *spuTestEnv) []string {
	var keys []string
	for e.queue.Len() > 0 {
		key, _ := e.queue.Get()
		keys = append(keys, key)
		e.queue.Done(key)
	}
	return keys
}

func TestDeletedVolumeInfoDisabledDoesNotRequireKubernetes(t *testing.T) {
	dviClient, err := newDeletedVolumeInfoClient(context.Background(), false)
	require.NoError(t, err)
	assert.Nil(t, dviClient, "capability-off must not construct a client or start DVI observation")
	e := newSPUTestEnv(t, interceptor.Funcs{}, testSPU("1Gi"))
	e.put(t, testCVI("live", "gold", "", testPolicyID, "25Mi", true, ""))
	require.NoError(t, e.r.reconcile(context.Background(), testSPUKey))
	assert.Equal(t, int64(25*1024*1024), e.spu(t).Status.ResourceTypeLevelQuotaUsage.Used.Value())
}

func TestDeletedVolumeInfoDecode(t *testing.T) {
	for _, tc := range []struct {
		name  string
		size  int64
		valid bool
		vac   string
	}{
		{name: "size greater than four GiB", size: 5368709120, valid: true},
		{name: "zero is a valid charge", size: 0, valid: true},
		{name: "last known size", size: 5368709120, valid: false},
		{name: "volume attribute class", size: 1, valid: true, vac: "fast"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, err := decodeDeletedVolumeInfo(testDVI("volume", "gold", tc.vac, testPolicyID, tc.size, tc.valid))
			require.NoError(t, err)
		})
	}
	requiredFields := []string{
		"volumeID", "namespace", "storageClassName", "storagePolicyID", "sizeBytes", "validSize",
	}
	for _, field := range requiredFields {
		t.Run("missing "+field, func(t *testing.T) {
			u := testDVI("volume", "gold", "", testPolicyID, 1, true)
			unstructured.RemoveNestedField(u.Object, "spec", field)
			_, err := decodeDeletedVolumeInfo(u)
			require.Error(t, err)
		})
	}
	for _, tc := range []struct {
		name, field string
		value       interface{}
	}{
		{"negative size", "sizeBytes", int64(-1)},
		{"size quantity string", "sizeBytes", "5Gi"},
		{"size numeric string", "sizeBytes", "5368709120"},
		{"size fractional number", "sizeBytes", float64(1.5)},
		{"size floating point integer", "sizeBytes", float64(1)},
		{"null size", "sizeBytes", nil},
		{"null valid flag", "validSize", nil},
		{"string valid flag", "validSize", "true"},
		{"empty volume ID", "volumeID", ""},
		{"empty namespace", "namespace", ""},
		{"empty storage class", "storageClassName", ""},
		{"empty policy", "storagePolicyID", ""},
		{"wrong policy type", "storagePolicyID", int64(1)},
		{"wrong VAC type", "volumeAttributeClassName", true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			u := testDVI("volume", "gold", "", testPolicyID, 1, true)
			require.NoError(t, unstructured.SetNestedField(u.Object, tc.value, "spec", tc.field))
			_, err := decodeDeletedVolumeInfo(u)
			require.Error(t, err)
		})
	}
}

func TestDeletedVolumeInfoAddsOnlyMatchingTenantStorageAndPolicy(t *testing.T) {
	e := newDVITestEnv(t, testSPU("0"))
	e.put(t, testCVI("live", "gold", "", testPolicyID, "100Mi", true, ""))
	otherTenant := testDVI("other-tenant", "gold", "", testPolicyID, 10*1024*1024*1024, true)
	require.NoError(t, unstructured.SetNestedField(otherTenant.Object, "other-user-ns", "spec", "namespace"))
	putDVI(t, e,
		testDVI("deleted", "gold", "", testPolicyID, 5368709120, true),
		testDVI("other-policy", "gold", "", "other-policy", 10*1024*1024*1024, true),
		testDVI("other-sc", "silver", "", testPolicyID, 10*1024*1024*1024, true),
		testDVI("other-vac", "gold", "fast", testPolicyID, 10*1024*1024*1024, true),
		otherTenant)
	require.NoError(t, e.r.reconcile(context.Background(), testSPUKey))
	assert.Equal(t, int64(5368709120+100*1024*1024), e.spu(t).Status.ResourceTypeLevelQuotaUsage.Used.Value(),
		"DVI lives in the system namespace but charges spec.namespace")
}

func TestDeletedVolumeInfoVolumeAttributeClassTakesPrecedence(t *testing.T) {
	vacSPU := testSPU("0")
	vacSPU.Name = "vac-fast-snapshot-usage"
	e := newDVITestEnv(t, testSPU("0"), vacSPU)
	putDVI(t, e, testDVI("deleted", "gold", "fast", testPolicyID, 25, true))
	require.NoError(t, e.r.reconcile(context.Background(), testSPUKey))
	require.NoError(t, e.r.reconcile(context.Background(), testSPUNamespace+"/"+vacSPU.Name))
	assert.Zero(t, e.spu(t).Status.ResourceTypeLevelQuotaUsage.Used.Value())
	got := &storagepolicyv1alpha3.StoragePolicyUsage{}
	require.NoError(t, e.client.Get(context.Background(), client.ObjectKeyFromObject(vacSPU), got))
	assert.Equal(t, int64(25), got.Status.ResourceTypeLevelQuotaUsage.Used.Value())
}

func TestDeletedVolumeInfoUpdatesDeletesAndPreservesReservations(t *testing.T) {
	spu := testSPU("1Gi")
	spu.Status.ResourceTypeLevelQuotaUsage.Reserved.Set(123)
	e := newDVITestEnv(t, spu)
	e.put(t, testCVI("live", "gold", "", testPolicyID, "100Mi", true, ""))
	deleted := testDVI("deleted", "gold", "", testPolicyID, 20*1024*1024, false)
	putDVI(t, e, deleted)
	require.NoError(t, e.r.reconcile(context.Background(), testSPUKey))
	got := e.spu(t)
	assert.Equal(t, int64(120*1024*1024), got.Status.ResourceTypeLevelQuotaUsage.Used.Value())
	assert.Equal(t, "true", got.Annotations[common.MissingSnapshotAggregatedCapacity])
	assert.Equal(t, int64(123), got.Status.ResourceTypeLevelQuotaUsage.Reserved.Value())
	updated := testDVI("deleted", "gold", "", testPolicyID, 30*1024*1024, true)
	putDVI(t, e, updated)
	require.NoError(t, e.r.reconcile(context.Background(), testSPUKey))
	got = e.spu(t)
	assert.Equal(t, int64(130*1024*1024), got.Status.ResourceTypeLevelQuotaUsage.Used.Value())
	assert.NotContains(t, got.Annotations, common.MissingSnapshotAggregatedCapacity)
	require.NoError(t, e.r.dviInformer.GetIndexer().Delete(updated))
	require.NoError(t, e.r.reconcile(context.Background(), testSPUKey))
	assert.Equal(t, int64(100*1024*1024), e.spu(t).Status.ResourceTypeLevelQuotaUsage.Used.Value())
	assert.Equal(t, int64(123), e.spu(t).Status.ResourceTypeLevelQuotaUsage.Reserved.Value())
}

func TestDeletedVolumeInfoCVIWinsUntilRemoved(t *testing.T) {
	for _, tc := range []struct {
		name, sc, policy, size string
		terminating            bool
		want                   int64
	}{
		{name: "same SPU", sc: "gold", policy: testPolicyID, size: "10Mi", want: 10 * 1024 * 1024},
		{name: "different storage class", sc: "silver", policy: testPolicyID, size: "10Mi"},
		{name: "different policy", sc: "gold", policy: "other-policy", size: "10Mi"},
		{name: "no snapshot size", sc: "gold", policy: testPolicyID},
		{name: "terminating CVI", sc: "gold", policy: testPolicyID, size: "10Mi", terminating: true,
			want: 10 * 1024 * 1024},
	} {
		t.Run(tc.name, func(t *testing.T) {
			e := newDVITestEnv(t, testSPU("0"))
			cvi := testCVI("cvi-name", tc.sc, "", tc.policy, tc.size, true, "")
			require.NoError(t, unstructured.SetNestedField(cvi.Object, "volume-id", "spec", "volumeID"))
			if tc.terminating {
				now := metav1.Now()
				cvi.SetDeletionTimestamp(&now)
			}
			e.put(t, cvi)
			putDVI(t, e, testDVI("volume-id", "gold", "", testPolicyID, 20*1024*1024, false))
			require.NoError(t, e.r.reconcile(context.Background(), testSPUKey))
			assert.Equal(t, tc.want, e.spu(t).Status.ResourceTypeLevelQuotaUsage.Used.Value())
			assert.NotContains(t, e.spu(t).Annotations, common.MissingSnapshotAggregatedCapacity,
				"suppressed DVI staleness must not affect the live-volume contribution")
			require.NoError(t, e.informer.GetIndexer().Delete(cvi))
			require.NoError(t, e.r.reconcile(context.Background(), testSPUKey))
			assert.Equal(t, int64(20*1024*1024), e.spu(t).Status.ResourceTypeLevelQuotaUsage.Used.Value())
			assert.Equal(t, "true", e.spu(t).Annotations[common.MissingSnapshotAggregatedCapacity])
		})
	}
}

func TestDeletedVolumeInfoCapturedCVIIsNotCountedTwiceAfterDeletion(t *testing.T) {
	e := newDVITestEnv(t, testSPU("100Mi"))
	cvi := testCVI("volume-id", "gold", "", testPolicyID, "10Mi", true, "")
	e.put(t, cvi)
	putDVI(t, e, testDVI("volume-id", "gold", "", testPolicyID, 20*1024*1024, false))

	// Reconciliation captures and sums CVI inventory before reading DVI inventory.
	// Delete CVI between those reads to reproduce the handoff without timing races.
	liveObjs, err := e.informer.GetIndexer().ByIndex(cnsvolumeinfo.SPUSnapshotIndex, testSPUKey)
	require.NoError(t, err)
	require.Len(t, liveObjs, 1)
	liveTotal, liveMissing, err := sumSnapshotCapacity(liveObjs, testPolicyID)
	require.NoError(t, err)
	require.NoError(t, e.informer.GetIndexer().Delete(cvi))
	dvis, err := e.r.deletedVolumeInfosForSPU(testSPUKey)
	require.NoError(t, err)
	deletedTotal, deletedMissing, err := e.r.sumDeletedVolumeInfoCapacity(dvis, liveObjs, testPolicyID)
	require.NoError(t, err)
	assert.True(t, deletedTotal.IsZero(), "the captured CVI contribution must suppress its DVI in this pass")
	liveTotal.Add(*deletedTotal)
	assert.Equal(t, int64(10*1024*1024), liveTotal.Value())
	assert.False(t, liveMissing || deletedMissing, "suppressed DVI staleness must not mark the CVI contribution stale")

	// The next pass sees no CVI and takes over both the DVI charge and its stale flag.
	require.NoError(t, e.r.reconcile(context.Background(), testSPUKey))
	assert.Equal(t, int64(20*1024*1024), e.spu(t).Status.ResourceTypeLevelQuotaUsage.Used.Value())
	assert.Equal(t, "true", e.spu(t).Annotations[common.MissingSnapshotAggregatedCapacity])
}

func TestDeletedVolumeInfoDuplicateVolumeIDPreservesUsedAndRecovers(t *testing.T) {
	e := newDVITestEnv(t, testSPU("100Mi"))
	e.put(t, testCVI("live", "gold", "", testPolicyID, "10Mi", true, ""))
	dvi := testDVI("volume-id", "gold", "", testPolicyID, 25, true)
	duplicate := dvi.DeepCopy()
	duplicate.SetNamespace("other-system-ns")
	require.NoError(t, unstructured.SetNestedField(duplicate.Object, int64(50), "spec", "sizeBytes"))
	putDVI(t, e, dvi, duplicate)

	err := e.r.recomputeOnce(context.Background(), testSPUNamespace, testSPUName, testSPUKey)
	require.ErrorContains(t, err, "multiple DeletedVolumeInfo records for volume")
	assert.Equal(t, int64(100*1024*1024), e.spu(t).Status.ResourceTypeLevelQuotaUsage.Used.Value(),
		"ambiguous records must neither double-charge nor choose one copy arbitrarily")
	assert.Equal(t, "true", e.spu(t).Annotations[common.MissingSnapshotAggregatedCapacity])

	require.NoError(t, e.r.dviInformer.GetIndexer().Delete(duplicate))
	e.r.enqueueDeletedVolumeInfo(cache.DeletedFinalStateUnknown{Obj: duplicate})
	require.Equal(t, 1, e.queue.Len())
	require.True(t, e.r.processNext(context.Background()))
	assert.Equal(t, int64(10*1024*1024+25), e.spu(t).Status.ResourceTypeLevelQuotaUsage.Used.Value())
	assert.NotContains(t, e.spu(t).Annotations, common.MissingSnapshotAggregatedCapacity)
}

func TestDeletedVolumeInfoMalformedInventoryPreservesUsed(t *testing.T) {
	for _, field := range []string{"sizeBytes", "validSize", "volumeID", "storageClassName",
		"volumeAttributeClassName", "storagePolicyID"} {
		t.Run(field, func(t *testing.T) {
			e := newDVITestEnv(t, testSPU("100Mi"))
			e.put(t, testCVI("live", "gold", "", testPolicyID, "10Mi", true, ""))
			bad := testDVI("bad", "gold", "", testPolicyID, 20, true)
			require.NoError(t, unstructured.SetNestedField(bad.Object, nil, "spec", field))
			putDVI(t, e, bad)
			err := e.r.recomputeOnce(context.Background(), testSPUNamespace, testSPUName, testSPUKey)
			require.Error(t, err)
			assert.Equal(t, int64(100*1024*1024), e.spu(t).Status.ResourceTypeLevelQuotaUsage.Used.Value())
			assert.Equal(t, "true", e.spu(t).Annotations[common.MissingSnapshotAggregatedCapacity])
			putDVI(t, e, testDVI("bad", "gold", "", testPolicyID, 20, true))
			require.NoError(t, e.r.reconcile(context.Background(), testSPUKey))
			assert.Equal(t, int64(10*1024*1024+20), e.spu(t).Status.ResourceTypeLevelQuotaUsage.Used.Value())
			assert.NotContains(t, e.spu(t).Annotations, common.MissingSnapshotAggregatedCapacity)
		})
	}
}

func TestDeletedVolumeInfoWaitsForMissingSPU(t *testing.T) {
	e := newDVITestEnv(t)
	putDVI(t, e, testDVI("deleted", "gold", "", testPolicyID, 25, true))
	require.Error(t, e.r.recomputeOnce(context.Background(), testSPUNamespace, testSPUName, testSPUKey))
	require.NoError(t, e.client.Create(context.Background(), testSPU("0")))
	require.NoError(t, e.r.reconcile(context.Background(), testSPUKey))
	assert.Equal(t, int64(25), e.spu(t).Status.ResourceTypeLevelQuotaUsage.Used.Value())
}

func TestDeletedVolumeInfoEnqueueAndCVICrossPolicyEvents(t *testing.T) {
	e := newDVITestEnv(t)
	dvi := testDVI("volume-id", "gold", "", testPolicyID, 25, true)
	putDVI(t, e, dvi)
	e.r.enqueueDeletedVolumeInfo(dvi)
	assert.Equal(t, []string{testSPUKey}, drainDVIQueue(e))
	e.r.enqueueDeletedVolumeInfo(cache.DeletedFinalStateUnknown{Key: "deleted", Obj: dvi})
	assert.Equal(t, []string{testSPUKey}, drainDVIQueue(e), "DVI tombstones retain the old association")
	cvi := testCVI("cvi-name", "silver", "", "other-policy", "", false, "")
	require.NoError(t, unstructured.SetNestedField(cvi.Object, "volume-id", "spec", "volumeID"))
	e.r.enqueue(cvi)
	assert.ElementsMatch(t, []string{testSPUKey, testSPUNamespace + "/silver-snapshot-usage"}, drainDVIQueue(e),
		"CVI changes must also repair the DVI SPU after a policy move or restore")
	e.r.enqueue(cache.DeletedFinalStateUnknown{Key: "cvi-name", Obj: cvi})
	assert.ElementsMatch(t, []string{testSPUKey, testSPUNamespace + "/silver-snapshot-usage"}, drainDVIQueue(e))
}

func TestDeletedVolumeInfoFieldsChanged(t *testing.T) {
	original := testDVI("deleted", "gold", "", testPolicyID, 25, true)
	metadataOnly := original.DeepCopy()
	metadataOnly.SetLabels(map[string]string{"updated": "true"})
	assert.False(t, deletedVolumeInfoFieldsChanged(original, metadataOnly))
	for _, tc := range []struct {
		field string
		value interface{}
	}{
		{"sizeBytes", int64(26)}, {"sizeBytes", "25"}, {"validSize", false},
		{"volumeID", "another-volume"}, {"namespace", "other-namespace"},
		{"storageClassName", "silver"}, {"volumeAttributeClassName", "fast"},
		{"storagePolicyID", "other-policy"},
	} {
		t.Run(tc.field, func(t *testing.T) {
			updated := original.DeepCopy()
			require.NoError(t, unstructured.SetNestedField(updated.Object, tc.value, "spec", tc.field))
			assert.True(t, deletedVolumeInfoFieldsChanged(original, updated))
			assert.True(t, deletedVolumeInfoFieldsChanged(updated, original))
		})
	}
}

func TestDeletedVolumeInfoInformerRoutesAddMoveAndDelete(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	dvi := testDVI("deleted", "gold", "", testPolicyID, 25, true)
	dynamicClient := testDVIDynamicClient(dvi)
	e := newSPUTestEnv(t, interceptor.Funcs{})
	informer := deletedVolumeInfoInformer(dynamicClient)
	var err error
	e.r, err = newSPUSnapshotReconciler(e.client, e.informer, e.queue, informer)
	require.NoError(t, err)
	go informer.Run(ctx.Done())
	require.Eventually(t, func() bool { return informer.HasSynced() && e.queue.Len() == 1 },
		5*time.Second, 10*time.Millisecond)
	assert.Equal(t, []string{testSPUKey}, drainDVIQueue(e))
	updated := dvi.DeepCopy()
	require.NoError(t, unstructured.SetNestedField(updated.Object, "fast", "spec", "volumeAttributeClassName"))
	_, err = dynamicClient.Resource(deletedVolumeInfoGVR).Namespace(testDVISystemNamespace).
		Update(ctx, updated, metav1.UpdateOptions{})
	require.NoError(t, err)
	require.Eventually(t, func() bool { return e.queue.Len() == 2 }, 5*time.Second, 10*time.Millisecond)
	assert.ElementsMatch(t, []string{testSPUKey, testSPUNamespace + "/vac-fast-snapshot-usage"}, drainDVIQueue(e))
	require.NoError(t, dynamicClient.Resource(deletedVolumeInfoGVR).Namespace(testDVISystemNamespace).
		Delete(ctx, dvi.GetName(), metav1.DeleteOptions{}))
	require.Eventually(t, func() bool { return e.queue.Len() == 1 }, 5*time.Second, 10*time.Millisecond)
	assert.Equal(t, []string{testSPUNamespace + "/vac-fast-snapshot-usage"}, drainDVIQueue(e))
}

func testEmptyCVIInformer() cache.SharedIndexInformer {
	return cache.NewSharedIndexInformer(&cache.ListWatch{
		ListWithContextFunc: func(context.Context, metav1.ListOptions) (runtime.Object, error) {
			return &unstructured.UnstructuredList{}, nil
		},
		WatchFuncWithContext: func(_ context.Context, opts metav1.ListOptions) (watch.Interface, error) {
			w := watch.NewRaceFreeFake()
			if opts.SendInitialEvents != nil && *opts.SendInitialEvents {
				bookmark := &unstructured.Unstructured{}
				bookmark.SetResourceVersion("1")
				bookmark.SetAnnotations(map[string]string{metav1.InitialEventsAnnotationKey: "true"})
				w.Action(watch.Bookmark, bookmark)
			}
			return w, nil
		},
	}, &unstructured.Unstructured{}, 0, cnsvolumeinfo.SPUSnapshotIndexers())
}

func TestDeletedVolumeInfoUnavailableAPIHoldsUsageAndRecovers(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	var available atomic.Bool
	var failedLists atomic.Int32
	var patches atomic.Int32
	dynamicClient := testDVIDynamicClient(testDVI("deleted", "gold", "", testPolicyID, 5368709120, true))
	dynamicClient.PrependReactor("list", "deletedvolumeinfos", func(clienttesting.Action) (bool, runtime.Object, error) {
		if available.Load() {
			return false, nil, nil
		}
		failedLists.Add(1)
		return true, nil, apierrors.NewNotFound(deletedVolumeInfoGVR.GroupResource(), "")
	})
	e := newSPUTestEnv(t, interceptor.Funcs{SubResourcePatch: func(ctx context.Context, c client.Client,
		sub string, obj client.Object, p client.Patch, opts ...client.SubResourcePatchOption) error {
		patches.Add(1)
		return c.SubResource(sub).Patch(ctx, obj, p, opts...)
	}}, testSPU("100Mi"))
	cviInformer := testEmptyCVIInformer()
	dviInformer := deletedVolumeInfoInformer(dynamicClient)
	r, err := newSPUSnapshotReconciler(e.client, cviInformer, e.queue, dviInformer)
	require.NoError(t, err)
	go cviInformer.Run(ctx.Done())
	done := make(chan struct{})
	go func() { r.run(ctx, 2); close(done) }()
	e.queue.Add(testSPUKey)
	require.Eventually(t, func() bool { return cviInformer.HasSynced() && failedLists.Load() > 0 },
		5*time.Second, 10*time.Millisecond)
	assert.Never(t, func() bool { return patches.Load() > 0 }, 250*time.Millisecond, 10*time.Millisecond,
		"an unavailable DVI API must not be interpreted as an empty inventory")
	assert.Equal(t, int64(100*1024*1024), e.spu(t).Status.ResourceTypeLevelQuotaUsage.Used.Value())
	available.Store(true)
	require.Eventually(t, func() bool {
		return e.spu(t).Status.ResourceTypeLevelQuotaUsage.Used.Value() == 5368709120
	}, 10*time.Second, 10*time.Millisecond, "restoring API availability should allow cache sync and repair Used")
	assert.True(t, dviInformer.HasSynced())
	cancel()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("DVI workers did not stop on cancellation")
	}
}

func TestDeletedVolumeInfoRestartEnumerationRetainsUsage(t *testing.T) {
	e := newDVITestEnv(t, testSPU("999Mi"))
	putDVI(t, e, testDVI("deleted", "gold", "", testPolicyID, 20*1024*1024, true))
	e.put(t, testCVI("live", "gold", "", testPolicyID, "10Mi", true, ""))
	require.NoError(t, e.r.enqueueAllSnapshotSPUs(context.Background()))
	require.Equal(t, 1, e.queue.Len())
	require.True(t, e.r.processNext(context.Background()))
	assert.Equal(t, int64(30*1024*1024), e.spu(t).Status.ResourceTypeLevelQuotaUsage.Used.Value())
	// A later full-sync pass must include DVI bytes again rather than restoring the CVI-only total.
	require.NoError(t, e.r.enqueueAllSnapshotSPUs(context.Background()))
	require.True(t, e.r.processNext(context.Background()))
	assert.Equal(t, int64(30*1024*1024), e.spu(t).Status.ResourceTypeLevelQuotaUsage.Used.Value())
}

func TestDeletedVolumeInfoMalformedAssociationSweepsAndRecovers(t *testing.T) {
	otherSPU := testSPU("50Mi")
	otherSPU.Namespace = "other-tenant"
	e := newDVITestEnv(t, testSPU("100Mi"), otherSPU)
	bad := testDVI("deleted", "gold", "", testPolicyID, 25, true)
	unstructured.RemoveNestedField(bad.Object, "spec", "storageClassName")
	putDVI(t, e, bad)
	e.r.enqueueDeletedVolumeInfo(bad)
	require.Equal(t, 1, e.queue.Len())
	require.True(t, e.r.processNext(context.Background()))
	assert.Equal(t, []string{testSPUKey}, drainDVIQueue(e),
		"an invalid storage association must trigger a sweep only in its tenant namespace")
	require.Error(t, e.r.recomputeOnce(context.Background(), testSPUNamespace, testSPUName, testSPUKey))
	assert.Equal(t, int64(100*1024*1024), e.spu(t).Status.ResourceTypeLevelQuotaUsage.Used.Value())
	assert.Equal(t, "true", e.spu(t).Annotations[common.MissingSnapshotAggregatedCapacity])
	require.NoError(t, e.r.reconcile(context.Background(), "other-tenant/"+testSPUName))
	got := &storagepolicyv1alpha3.StoragePolicyUsage{}
	require.NoError(t, e.client.Get(context.Background(), client.ObjectKeyFromObject(otherSPU), got))
	assert.True(t, got.Status.ResourceTypeLevelQuotaUsage.Used.IsZero())
	assert.NotContains(t, got.Annotations, common.MissingSnapshotAggregatedCapacity)
	// Removal must release held SPUs in this namespace without sweeping other tenants.
	require.NoError(t, e.r.dviInformer.GetIndexer().Delete(bad))
	e.r.enqueueDeletedVolumeInfo(cache.DeletedFinalStateUnknown{Obj: bad})
	require.True(t, e.r.processNext(context.Background()))
	require.Equal(t, 1, e.queue.Len())
	require.True(t, e.r.processNext(context.Background()))
	for _, spu := range []*storagepolicyv1alpha3.StoragePolicyUsage{testSPU("0"), otherSPU} {
		got := &storagepolicyv1alpha3.StoragePolicyUsage{}
		require.NoError(t, e.client.Get(context.Background(), client.ObjectKeyFromObject(spu), got))
		assert.True(t, got.Status.ResourceTypeLevelQuotaUsage.Used.IsZero())
		assert.NotContains(t, got.Annotations, common.MissingSnapshotAggregatedCapacity)
	}
}

func TestDeletedVolumeInfoStartupIndexesExistingCVI(t *testing.T) {
	e := newSPUTestEnv(t, interceptor.Funcs{}, testSPU("100Mi"))
	// The volume-info service can populate its cache before DVI startup discovery finishes.
	e.put(t, testCVI("existing-volume", "gold", "", testPolicyID, "10Mi", true, ""))
	dviInformer := deletedVolumeInfoInformer(testDVIDynamicClient())
	require.NoError(t, dviInformer.GetIndexer().Add(
		testDVI("existing-volume", "gold", "", testPolicyID, 20*1024*1024, true)))
	var err error
	e.r, err = newSPUSnapshotReconciler(e.client, e.informer, e.queue, dviInformer)
	require.NoError(t, err)
	require.NoError(t, e.r.reconcile(context.Background(), testSPUKey))
	assert.Equal(t, int64(10*1024*1024), e.spu(t).Status.ResourceTypeLevelQuotaUsage.Used.Value(),
		"the newly added volume ID index must include CVIs already present in the cache")
}

func TestDeletedVolumeInfoRestoreWatchWithoutSnapshotSize(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	e := newDVITestEnv(t, testSPU("0"))
	putDVI(t, e, testDVI("restored-volume", "gold", "", testPolicyID, 25, true))
	w := watch.NewRaceFreeFake()
	t.Cleanup(w.Stop)
	informer := cache.NewSharedIndexInformer(&cache.ListWatch{
		ListWithContextFunc: func(context.Context, metav1.ListOptions) (runtime.Object, error) {
			return &unstructured.UnstructuredList{}, nil
		},
		WatchFuncWithContext: func(_ context.Context, opts metav1.ListOptions) (watch.Interface, error) {
			if opts.SendInitialEvents != nil && *opts.SendInitialEvents {
				bookmark := &unstructured.Unstructured{}
				bookmark.SetResourceVersion("1")
				bookmark.SetAnnotations(map[string]string{metav1.InitialEventsAnnotationKey: "true"})
				w.Action(watch.Bookmark, bookmark)
			}
			return w, nil
		},
	}, &unstructured.Unstructured{}, 0, cnsvolumeinfo.SPUSnapshotIndexers())
	var err error
	e.r, err = newSPUSnapshotReconciler(e.client, informer, e.queue, e.r.dviInformer)
	require.NoError(t, err)
	go informer.Run(ctx.Done())
	require.Eventually(t, informer.HasSynced, 5*time.Second, 10*time.Millisecond)
	require.NoError(t, e.r.reconcile(ctx, testSPUKey))
	assert.Equal(t, int64(25), e.spu(t).Status.ResourceTypeLevelQuotaUsage.Used.Value())
	// Restoring to another policy can recreate a CVI without snapshot measurements.
	// Its Add event must still suppress and enqueue the old DVI association.
	cvi := testCVI("restored-volume", "silver", "", "restored-policy", "", false, "")
	w.Add(cvi)
	require.Eventually(t, func() bool { return e.queue.Len() == 2 }, 5*time.Second, 10*time.Millisecond)
	assert.ElementsMatch(t, []string{testSPUKey, testSPUNamespace + "/silver-snapshot-usage"}, drainDVIQueue(e))
	require.NoError(t, e.r.reconcile(ctx, testSPUKey))
	assert.True(t, e.spu(t).Status.ResourceTypeLevelQuotaUsage.Used.IsZero())
	w.Delete(cvi)
	require.Eventually(t, func() bool { return e.queue.Len() == 2 }, 5*time.Second, 10*time.Millisecond)
	assert.ElementsMatch(t, []string{testSPUKey, testSPUNamespace + "/silver-snapshot-usage"}, drainDVIQueue(e))
	require.NoError(t, e.r.reconcile(ctx, testSPUKey))
	assert.Equal(t, int64(25), e.spu(t).Status.ResourceTypeLevelQuotaUsage.Used.Value())
}
