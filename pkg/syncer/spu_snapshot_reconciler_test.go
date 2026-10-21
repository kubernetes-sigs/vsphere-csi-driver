package syncer

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/watch"
	"k8s.io/client-go/tools/cache"
	"k8s.io/client-go/util/workqueue"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"

	cnsoperatorv1alpha1 "sigs.k8s.io/vsphere-csi-driver/v3/pkg/apis/cnsoperator"
	storagepolicyv1alpha3 "sigs.k8s.io/vsphere-csi-driver/v3/pkg/apis/cnsoperator/storagepolicy/v1alpha3"
	"sigs.k8s.io/vsphere-csi-driver/v3/pkg/csi/service/common"
	"sigs.k8s.io/vsphere-csi-driver/v3/pkg/internalapis/cnsvolumeinfo"
)

const (
	testSPUNamespace = "ns1"
	testSPUName      = "gold-snapshot-usage"
	testSPUKey       = "ns1/gold-snapshot-usage"
	testPolicyID     = "policy-1"
)

type spuTestEnv struct {
	r        *spuSnapshotReconciler
	informer cache.SharedIndexInformer
	queue    workqueue.TypedRateLimitingInterface[string]
	client   client.Client
}

func newSPUTestEnv(t *testing.T, funcs interceptor.Funcs,
	spus ...*storagepolicyv1alpha3.StoragePolicyUsage) *spuTestEnv {
	scheme := runtime.NewScheme()
	assert.NoError(t, cnsoperatorv1alpha1.AddToScheme(scheme))
	builder := fake.NewClientBuilder().WithScheme(scheme).
		WithStatusSubresource(&storagepolicyv1alpha3.StoragePolicyUsage{}).WithInterceptorFuncs(funcs)
	for _, s := range spus {
		builder = builder.WithObjects(s)
	}
	c := builder.Build()
	informer := cache.NewSharedIndexInformer(&cache.ListWatch{}, &unstructured.Unstructured{}, 0,
		cnsvolumeinfo.SPUSnapshotIndexers())
	queue := workqueue.NewTypedRateLimitingQueue(workqueue.DefaultTypedControllerRateLimiter[string]())
	t.Cleanup(queue.ShutDown)
	r, err := newSPUSnapshotReconciler(c, informer, queue)
	assert.NoError(t, err)
	return &spuTestEnv{r: r, informer: informer, queue: queue, client: c}
}

func testSPU(used string) *storagepolicyv1alpha3.StoragePolicyUsage {
	u := resource.MustParse(used)
	reserved := resource.MustParse("0")
	apiGroup := ResourceAPIgroupSnapshot
	return &storagepolicyv1alpha3.StoragePolicyUsage{
		ObjectMeta: metav1.ObjectMeta{Namespace: testSPUNamespace, Name: testSPUName},
		Spec: storagepolicyv1alpha3.StoragePolicyUsageSpec{
			StorageClassName: "gold", StoragePolicyId: testPolicyID, ResourceKind: ResourceKindSnapshot,
			ResourceExtensionName: SnapQuotaExtensionServiceName, ResourceAPIgroup: &apiGroup,
		},
		Status: storagepolicyv1alpha3.StoragePolicyUsageStatus{
			ResourceTypeLevelQuotaUsage: &storagepolicyv1alpha3.QuotaUsageDetails{Used: &u, Reserved: &reserved},
		},
	}
}

// testCVI builds a CnsVolumeInfo with the given aggregated snapshot size ("" for none).
func testCVI(name, sc, vac, policy, size string, valid bool, snapTime string) *unstructured.Unstructured {
	spec := map[string]interface{}{
		"namespace": testSPUNamespace, "storageClassName": sc, "storagePolicyID": policy,
		"validaggregatedsnapshotsize": valid,
	}
	if vac != "" {
		spec["volumeAttributeClassName"] = vac
	}
	if size != "" {
		spec["aggregatedsnapshotsize"] = size
	}
	if snapTime != "" {
		spec["snapshotlatestoperationcompletetime"] = snapTime
	}
	return &unstructured.Unstructured{Object: map[string]interface{}{
		"apiVersion": "cns.vmware.com/v1alpha1", "kind": "CNSVolumeInfo",
		"metadata": map[string]interface{}{"name": name, "namespace": "vmware-system-csi"},
		"spec":     spec,
	}}
}

func (e *spuTestEnv) put(t *testing.T, objs ...*unstructured.Unstructured) {
	for _, o := range objs {
		assert.NoError(t, e.informer.GetIndexer().Add(o))
	}
}

func (e *spuTestEnv) spu(t *testing.T) *storagepolicyv1alpha3.StoragePolicyUsage {
	got := &storagepolicyv1alpha3.StoragePolicyUsage{}
	assert.NoError(t, e.client.Get(context.Background(),
		client.ObjectKey{Namespace: testSPUNamespace, Name: testSPUName}, got))
	return got
}

func TestSPUSnapshotRecomputeFixesDoubleCount(t *testing.T) {
	// The old handler added the 50Mi delta on top of a total the sync had already updated: 200Mi.
	e := newSPUTestEnv(t, interceptor.Funcs{}, testSPU("200Mi"))
	e.put(t, testCVI("a", "gold", "", testPolicyID, "150Mi", true, ""))
	assert.NoError(t, e.r.reconcile(context.Background(), testSPUKey))
	assert.Equal(t, int64(150*1024*1024), e.spu(t).Status.ResourceTypeLevelQuotaUsage.Used.Value())
}

func TestSPUSnapshotRecomputeSumsOnlyMatchingPolicy(t *testing.T) {
	e := newSPUTestEnv(t, interceptor.Funcs{}, testSPU("0"))
	e.put(t,
		testCVI("a", "gold", "", testPolicyID, "100Mi", true, ""),
		testCVI("b", "gold", "", testPolicyID, "50Mi", true, ""),
		testCVI("c", "gold", "", "other-policy", "1Gi", true, ""),
		testCVI("d", "silver", "", testPolicyID, "1Gi", true, ""))
	assert.NoError(t, e.r.reconcile(context.Background(), testSPUKey))
	assert.Equal(t, int64(150*1024*1024), e.spu(t).Status.ResourceTypeLevelQuotaUsage.Used.Value())
}

func TestSPUSnapshotRecomputeResetsToZeroAndInitializesStatus(t *testing.T) {
	e := newSPUTestEnv(t, interceptor.Funcs{}, testSPU("1Gi"))
	assert.NoError(t, e.r.reconcile(context.Background(), testSPUKey))
	assert.Equal(t, int64(0), e.spu(t).Status.ResourceTypeLevelQuotaUsage.Used.Value())

	noStatus := testSPU("0")
	noStatus.Status = storagepolicyv1alpha3.StoragePolicyUsageStatus{}
	e2 := newSPUTestEnv(t, interceptor.Funcs{}, noStatus)
	e2.put(t, testCVI("a", "gold", "", testPolicyID, "10Mi", true, ""))
	assert.NoError(t, e2.r.reconcile(context.Background(), testSPUKey))
	assert.Equal(t, int64(10*1024*1024), e2.spu(t).Status.ResourceTypeLevelQuotaUsage.Used.Value())
}

func TestSPUSnapshotRecomputeNotFound(t *testing.T) {
	e := newSPUTestEnv(t, interceptor.Funcs{})
	assert.NoError(t, e.r.reconcile(context.Background(), testSPUKey), "removed association needs no retry")
	e.put(t, testCVI("a", "gold", "", testPolicyID, "10Mi", true, ""))
	assert.Error(t, e.r.recomputeOnce(context.Background(), testSPUNamespace, testSPUName, testSPUKey),
		"a contributor must retry until its SPU exists")
	require.NoError(t, e.client.Create(context.Background(), testSPU("0")))
	require.NoError(t, e.r.reconcile(context.Background(), testSPUKey))
	assert.Equal(t, int64(10*1024*1024), e.spu(t).Status.ResourceTypeLevelQuotaUsage.Used.Value())
}

func TestSPUSnapshotRecomputeKeepsReservedWrittenConcurrently(t *testing.T) {
	patches := 0
	funcs := interceptor.Funcs{SubResourcePatch: func(ctx context.Context, cl client.Client, sub string,
		obj client.Object, p client.Patch, opts ...client.SubResourcePatchOption) error {
		patches++
		if patches == 1 {
			// Another writer patches Reserved between the worker's GET and PATCH.
			cur := &storagepolicyv1alpha3.StoragePolicyUsage{}
			assert.NoError(t, cl.Get(ctx, client.ObjectKeyFromObject(obj), cur))
			r := resource.MustParse("10")
			cur.Status.ResourceTypeLevelQuotaUsage.Reserved = &r
			assert.NoError(t, cl.Status().Update(ctx, cur))
		}
		return cl.SubResource(sub).Patch(ctx, obj, p, opts...)
	}}
	e := newSPUTestEnv(t, funcs, testSPU("100Mi"))
	e.put(t, testCVI("a", "gold", "", testPolicyID, "190Mi", true, ""))
	assert.NoError(t, e.r.reconcile(context.Background(), testSPUKey))
	got := e.spu(t).Status.ResourceTypeLevelQuotaUsage
	assert.Equal(t, int64(190*1024*1024), got.Used.Value())
	assert.Equal(t, int64(10), got.Reserved.Value())
	assert.Equal(t, 1, patches, "no retry is expected since the patch has no resourceVersion")
}

func TestSPUSnapshotRecomputeRetriesWithReread(t *testing.T) {
	gets, patches := 0, 0
	funcs := interceptor.Funcs{
		Get: func(ctx context.Context, cl client.WithWatch, key client.ObjectKey, obj client.Object,
			opts ...client.GetOption) error {
			gets++
			return cl.Get(ctx, key, obj, opts...)
		},
		SubResourcePatch: func(ctx context.Context, cl client.Client, sub string, obj client.Object,
			p client.Patch, opts ...client.SubResourcePatchOption) error {
			patches++
			if patches <= 2 {
				return errors.New("transient")
			}
			return cl.SubResource(sub).Patch(ctx, obj, p, opts...)
		}}
	e := newSPUTestEnv(t, funcs, testSPU("0"))
	e.put(t, testCVI("a", "gold", "", testPolicyID, "100Mi", true, ""))
	assert.NoError(t, e.r.reconcile(context.Background(), testSPUKey))
	assert.Equal(t, 3, gets, "every attempt re-reads the StoragePolicyUsage")
	assert.Equal(t, int64(100*1024*1024), e.spu(t).Status.ResourceTypeLevelQuotaUsage.Used.Value())
}

func TestSPUSnapshotProcessNextRequeuesWithBackoffOnFailure(t *testing.T) {
	funcs := interceptor.Funcs{SubResourcePatch: func(context.Context, client.Client, string, client.Object,
		client.Patch, ...client.SubResourcePatchOption) error {
		return errors.New("down")
	}}
	e := newSPUTestEnv(t, funcs, testSPU("0"))
	e.put(t, testCVI("a", "gold", "", testPolicyID, "100Mi", true, ""))
	e.queue.Add(testSPUKey)
	assert.True(t, e.r.processNext(context.Background()))
	assert.Equal(t, 1, e.queue.NumRequeues(testSPUKey))

	// Success clears the backoff.
	e2 := newSPUTestEnv(t, interceptor.Funcs{}, testSPU("0"))
	e2.queue.AddRateLimited(testSPUKey)
	assert.Equal(t, 1, e2.queue.NumRequeues(testSPUKey))
	e2.queue.Add(testSPUKey)
	assert.True(t, e2.r.processNext(context.Background()))
	assert.Equal(t, 0, e2.queue.NumRequeues(testSPUKey))
}

func TestSPUSnapshotMissingCapacityAnnotation(t *testing.T) {
	e := newSPUTestEnv(t, interceptor.Funcs{}, testSPU("0"))
	e.put(t, testCVI("a", "gold", "", testPolicyID, "", false, "2026-10-07T22:25:46Z"))
	assert.NoError(t, e.r.reconcile(context.Background(), testSPUKey))
	assert.Equal(t, "true", e.spu(t).Annotations[common.MissingSnapshotAggregatedCapacity])
	assert.Equal(t, int64(0), e.spu(t).Status.ResourceTypeLevelQuotaUsage.Used.Value())

	// The size becomes valid, the annotation is removed.
	assert.NoError(t, e.informer.GetIndexer().Update(
		testCVI("a", "gold", "", testPolicyID, "10Mi", true, "2026-10-07T22:25:46Z")))
	assert.NoError(t, e.r.reconcile(context.Background(), testSPUKey))
	got := e.spu(t)
	assert.NotContains(t, got.Annotations, common.MissingSnapshotAggregatedCapacity)
	assert.Equal(t, int64(10*1024*1024), got.Status.ResourceTypeLevelQuotaUsage.Used.Value())
}

func TestSPUSnapshotNoWriteWhenUpToDate(t *testing.T) {
	patches := 0
	funcs := interceptor.Funcs{
		SubResourcePatch: func(ctx context.Context, cl client.Client, sub string, obj client.Object,
			p client.Patch, opts ...client.SubResourcePatchOption) error {
			patches++
			return cl.SubResource(sub).Patch(ctx, obj, p, opts...)
		},
		Patch: func(ctx context.Context, cl client.WithWatch, obj client.Object, p client.Patch,
			opts ...client.PatchOption) error {
			patches++
			return cl.Patch(ctx, obj, p, opts...)
		}}
	e := newSPUTestEnv(t, funcs, testSPU("100Mi"))
	e.put(t, testCVI("a", "gold", "", testPolicyID, "100Mi", true, ""))
	assert.NoError(t, e.r.reconcile(context.Background(), testSPUKey))
	assert.Equal(t, 0, patches)
}

func TestSPUSnapshotQueueMergesBurstAndSeparatesNamespaces(t *testing.T) {
	e := newSPUTestEnv(t, interceptor.Funcs{})
	for i := 0; i < 1000; i++ {
		e.queue.Add("ns1/gold-snapshot-usage")
	}
	e.queue.Add("ns2/gold-snapshot-usage")
	assert.Equal(t, 2, e.queue.Len(), "1000 events of one SPU are one entry, another SPU is separate")

	// While ns1 is being processed, new events for it are merged into one more entry and
	// ns2 can be picked up by another worker.
	k1, _ := e.queue.Get()
	e.queue.Add(k1)
	e.queue.Add(k1)
	k2, _ := e.queue.Get()
	assert.NotEqual(t, k1, k2)
	assert.Equal(t, 0, e.queue.Len(), "a key being processed is not handed out twice")
	e.queue.Done(k1)
	assert.Equal(t, 1, e.queue.Len())
}

func TestSPUSnapshotEnqueue(t *testing.T) {
	e := newSPUTestEnv(t, interceptor.Funcs{})
	drain := func() []string {
		var keys []string
		for e.queue.Len() > 0 {
			k, _ := e.queue.Get()
			keys = append(keys, k)
			e.queue.Done(k)
		}
		return keys
	}
	a := testCVI("a", "gold", "", testPolicyID, "100Mi", true, "")

	e.r.enqueue(a)
	assert.Equal(t, []string{testSPUKey}, drain())

	e.r.enqueue(cache.DeletedFinalStateUnknown{Key: "a", Obj: a})
	assert.Equal(t, []string{testSPUKey}, drain(), "tombstone is unwrapped")

	e.r.enqueue("not an unstructured")
	e.r.enqueue(testCVI("b", "", "", testPolicyID, "1Mi", true, ""))
	assert.Empty(t, drain())

	// VAC migration: the old and the new SPU are both enqueued.
	b := testCVI("a", "gold", "fast", testPolicyID, "100Mi", true, "")
	assert.True(t, snapshotFieldsChanged(a, b))
	e.r.enqueue(a)
	e.r.enqueue(b)
	assert.ElementsMatch(t, []string{testSPUKey, "ns1/vac-fast-snapshot-usage"}, drain())
}

func TestSPUSnapshotFieldsChanged(t *testing.T) {
	a := testCVI("a", "gold", "", testPolicyID, "100Mi", true, "")
	assert.False(t, snapshotFieldsChanged(a, a.DeepCopy()))
	assert.True(t, snapshotFieldsChanged(a, testCVI("a", "gold", "", testPolicyID, "150Mi", true, "")))
	assert.True(t, snapshotFieldsChanged(a, testCVI("a", "gold", "", testPolicyID, "100Mi", false, "")))
	assert.True(t, snapshotFieldsChanged(a, testCVI("a", "gold", "", testPolicyID, "100Mi", true, "2026-10-07T22:25:46Z")))
	assert.True(t, snapshotFieldsChanged(a, testCVI("a", "gold", "", "other", "100Mi", true, "")))
	assert.False(t, snapshotFieldsChanged("x", a))
}

func TestSPUSnapshotHasSnapshotInfo(t *testing.T) {
	assert.False(t, hasSnapshotInfo(testCVI("a", "gold", "", testPolicyID, "", true, "")))
	assert.True(t, hasSnapshotInfo(testCVI("a", "gold", "", testPolicyID, "1Mi", true, "")))
	assert.True(t, hasSnapshotInfo(testCVI("a", "gold", "", testPolicyID, "", false, "2026-10-07T22:25:46Z")))
}

func TestSPUSnapshotRunDoesNotWriteBeforeCacheSync(t *testing.T) {
	e := newSPUTestEnv(t, interceptor.Funcs{}, testSPU("1Gi"))
	e.queue.Add(testSPUKey)
	ctx, cancel := context.WithTimeout(context.Background(), 300*time.Millisecond)
	defer cancel()
	e.r.run(ctx, 2) // the informer never starts, so it never syncs
	time.Sleep(100 * time.Millisecond)
	assert.Equal(t, int64(1024*1024*1024), e.spu(t).Status.ResourceTypeLevelQuotaUsage.Used.Value())
}

func TestEnqueueSnapshotSPUWithoutQueue(t *testing.T) {
	orig := spuSnapshotQueue
	defer func() { spuSnapshotQueue = orig }()
	spuSnapshotQueue = nil
	enqueueSnapshotSPU(testSPUKey) // must not panic

	q := workqueue.NewTypedRateLimitingQueue(workqueue.DefaultTypedControllerRateLimiter[string]())
	defer q.ShutDown()
	spuSnapshotQueue = q
	enqueueSnapshotSPU(testSPUKey)
	assert.Equal(t, 1, q.Len())
}

func TestSPUSnapshotInvalidInventoryHoldsUsed(t *testing.T) {
	tests := []struct {
		name   string
		mutate func(map[string]interface{})
	}{
		{"malformed quantity", func(s map[string]interface{}) { s["aggregatedsnapshotsize"] = "broken" }},
		{"wrong quantity type", func(s map[string]interface{}) { s["aggregatedsnapshotsize"] = true }},
		{"null quantity", func(s map[string]interface{}) { s["aggregatedsnapshotsize"] = nil }},
		{"negative quantity", func(s map[string]interface{}) { s["aggregatedsnapshotsize"] = "-20Mi" }},
		{"missing capacity flag", func(s map[string]interface{}) { delete(s, "validaggregatedsnapshotsize") }},
		{"wrong capacity flag type", func(s map[string]interface{}) { s["validaggregatedsnapshotsize"] = "true" }},
		{"missing policy", func(s map[string]interface{}) { delete(s, "storagePolicyID") }},
		{"wrong policy type", func(s map[string]interface{}) { s["storagePolicyID"] = int64(123) }},
		{"activity without quantity", func(s map[string]interface{}) {
			delete(s, "aggregatedsnapshotsize")
			s["snapshotlatestoperationcompletetime"] = "2026-10-07T22:25:46Z"
		}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			e := newSPUTestEnv(t, interceptor.Funcs{}, testSPU("100Mi"))
			bad := testCVI("bad", "gold", "", testPolicyID, "20Mi", true, "")
			tt.mutate(bad.Object["spec"].(map[string]interface{}))
			e.put(t, bad, testCVI("valid", "gold", "", testPolicyID, "10Mi", true, ""))
			err := e.r.recomputeOnce(context.Background(), testSPUNamespace, testSPUName, testSPUKey)
			require.Error(t, err)
			assert.Equal(t, int64(100*1024*1024), e.spu(t).Status.ResourceTypeLevelQuotaUsage.Used.Value())
			assert.Equal(t, "true", e.spu(t).Annotations[common.MissingSnapshotAggregatedCapacity])
		})
	}
}

func TestSPUSnapshotInvalidCapacityUsesLastKnown(t *testing.T) {
	tests := []struct {
		name     string
		size     string
		wantUsed int64
	}{
		{"last known size counted", "20Mi", 30 * 1024 * 1024},
		{"no last known size counts zero", "", 10 * 1024 * 1024},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			e := newSPUTestEnv(t, interceptor.Funcs{}, testSPU("100Mi"))
			e.put(t,
				testCVI("unknown", "gold", "", testPolicyID, tt.size, false, "2026-10-07T22:25:46Z"),
				testCVI("valid", "gold", "", testPolicyID, "10Mi", true, ""))
			require.NoError(t, e.r.recomputeOnce(context.Background(), testSPUNamespace, testSPUName, testSPUKey))
			assert.Equal(t, tt.wantUsed, e.spu(t).Status.ResourceTypeLevelQuotaUsage.Used.Value())
			assert.Equal(t, "true", e.spu(t).Annotations[common.MissingSnapshotAggregatedCapacity])
		})
	}
}

func TestSPUSnapshotInvalidCapacityRecovers(t *testing.T) {
	e := newSPUTestEnv(t, interceptor.Funcs{}, testSPU("100Mi"))
	e.put(t,
		testCVI("unknown", "gold", "", testPolicyID, "20Mi", false, "2026-10-07T22:25:46Z"),
		testCVI("valid", "gold", "", testPolicyID, "10Mi", true, ""))
	require.NoError(t, e.r.recomputeOnce(context.Background(), testSPUNamespace, testSPUName, testSPUKey))
	assert.Equal(t, "true", e.spu(t).Annotations[common.MissingSnapshotAggregatedCapacity])

	e.put(t, testCVI("unknown", "gold", "", testPolicyID, "25Mi", true, "2026-10-08T22:25:46Z"))
	require.NoError(t, e.r.recomputeOnce(context.Background(), testSPUNamespace, testSPUName, testSPUKey))
	assert.Equal(t, int64(35*1024*1024), e.spu(t).Status.ResourceTypeLevelQuotaUsage.Used.Value())
	assert.NotContains(t, e.spu(t).Annotations, common.MissingSnapshotAggregatedCapacity)
}

func TestSPUSnapshotIntegerQuantity(t *testing.T) {
	e := newSPUTestEnv(t, interceptor.Funcs{}, testSPU("100Mi"))
	intCVI := testCVI("int", "gold", "", testPolicyID, "", true, "2026-10-07T22:25:46Z")
	intCVI.Object["spec"].(map[string]interface{})["aggregatedsnapshotsize"] = int64(20 * 1024 * 1024)
	e.put(t, intCVI, testCVI("str", "gold", "", testPolicyID, "10Mi", true, ""))
	require.NoError(t, e.r.recomputeOnce(context.Background(), testSPUNamespace, testSPUName, testSPUKey))
	assert.Equal(t, int64(30*1024*1024), e.spu(t).Status.ResourceTypeLevelQuotaUsage.Used.Value())
}

func TestSPUSnapshotValidZeroAndNeverSnapshot(t *testing.T) {
	e := newSPUTestEnv(t, interceptor.Funcs{}, testSPU("100Mi"))
	e.put(t,
		testCVI("zero", "gold", "", testPolicyID, "0", true, "2026-10-07T22:25:46Z"),
		testCVI("unused", "gold", "", testPolicyID, "", false, "0001-01-01T00:00:00Z"))
	require.NoError(t, e.r.reconcile(context.Background(), testSPUKey))
	assert.True(t, e.spu(t).Status.ResourceTypeLevelQuotaUsage.Used.IsZero())
	assert.NotContains(t, e.spu(t).Annotations, common.MissingSnapshotAggregatedCapacity)
}

func TestSPUSnapshotIgnoresOtherResourceOwners(t *testing.T) {
	for _, field := range []string{"kind", "extension", "api group"} {
		t.Run(field, func(t *testing.T) {
			spu := testSPU("100Mi")
			switch field {
			case "kind":
				spu.Spec.ResourceKind = ResourceKindPVC
			case "extension":
				spu.Spec.ResourceExtensionName = "another-extension"
			case "api group":
				spu.Spec.ResourceAPIgroup = nil
			}
			e := newSPUTestEnv(t, interceptor.Funcs{}, spu)
			e.put(t, testCVI("a", "gold", "", testPolicyID, "10Mi", true, ""))
			require.NoError(t, e.r.enqueueAllSnapshotSPUs(context.Background()))
			assert.Zero(t, e.queue.Len())
			require.NoError(t, e.r.reconcile(context.Background(), testSPUKey))
			assert.Equal(t, int64(100*1024*1024), e.spu(t).Status.ResourceTypeLevelQuotaUsage.Used.Value())
		})
	}
}

func TestSPUSnapshotStartupRetriesAndClearsEmptyUsage(t *testing.T) {
	var lists atomic.Int32
	funcs := interceptor.Funcs{List: func(ctx context.Context, cl client.WithWatch, list client.ObjectList,
		opts ...client.ListOption) error {
		if lists.Add(1) == 1 {
			return errors.New("temporary list failure")
		}
		return cl.List(ctx, list, opts...)
	}}
	e := newSPUTestEnv(t, funcs, testSPU("20Mi"))
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	informer := cache.NewSharedIndexInformer(&cache.ListWatch{
		ListWithContextFunc: func(context.Context, metav1.ListOptions) (runtime.Object, error) {
			return &unstructured.UnstructuredList{}, nil
		},
		WatchFuncWithContext: func(_ context.Context, opts metav1.ListOptions) (watch.Interface, error) {
			w := watch.NewRaceFreeFake()
			if opts.SendInitialEvents != nil && *opts.SendInitialEvents {
				// Watch-list initialization completes when the initial-events bookmark arrives.
				bookmark := &unstructured.Unstructured{}
				bookmark.SetResourceVersion("1")
				bookmark.SetAnnotations(map[string]string{metav1.InitialEventsAnnotationKey: "true"})
				w.Action(watch.Bookmark, bookmark)
			}
			return w, nil
		},
	}, &unstructured.Unstructured{}, 0, cnsvolumeinfo.SPUSnapshotIndexers())
	r, err := newSPUSnapshotReconciler(e.client, informer, e.queue)
	require.NoError(t, err)
	go informer.Run(ctx.Done())
	done := make(chan struct{})
	go func() { r.run(ctx, 2); close(done) }()
	require.Eventually(t, func() bool {
		return e.spu(t).Status.ResourceTypeLevelQuotaUsage.Used.IsZero()
	}, 10*time.Second, 10*time.Millisecond, "empty SPU must be repaired despite initial List failure")
	assert.GreaterOrEqual(t, lists.Load(), int32(2))
	cancel()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("workers did not stop on cancellation")
	}
}

func TestSPUSnapshotRetryStopsOnCancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	var attempts int
	e := newSPUTestEnv(t, interceptor.Funcs{SubResourcePatch: func(context.Context, client.Client, string,
		client.Object, client.Patch, ...client.SubResourcePatchOption) error {
		attempts++
		cancel()
		return errors.New("patch failed")
	}}, testSPU("0"))
	e.put(t, testCVI("a", "gold", "", testPolicyID, "10Mi", true, ""))
	require.ErrorIs(t, e.r.reconcile(ctx, testSPUKey), context.Canceled)
	assert.Equal(t, 1, attempts)
}
