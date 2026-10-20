package syncer

import (
	"context"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	k8stypes "k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	cnsoperatorv1alpha1 "sigs.k8s.io/vsphere-csi-driver/v3/pkg/apis/cnsoperator"
	storagepolicyv1alpha3 "sigs.k8s.io/vsphere-csi-driver/v3/pkg/apis/cnsoperator/storagepolicy/v1alpha3"
)

const (
	testSPUName      = "sc-pvc-usage"
	testSPUNamespace = "test-ns"
)

func newTestSPUClient(t *testing.T, reserved string) client.Client {
	scheme := runtime.NewScheme()
	assert.NoError(t, cnsoperatorv1alpha1.AddToScheme(scheme))
	reservedQty := resource.MustParse(reserved)
	usedQty := resource.MustParse("0")
	spu := &storagepolicyv1alpha3.StoragePolicyUsage{
		ObjectMeta: metav1.ObjectMeta{Name: testSPUName, Namespace: testSPUNamespace},
		Status: storagepolicyv1alpha3.StoragePolicyUsageStatus{
			ResourceTypeLevelQuotaUsage: &storagepolicyv1alpha3.QuotaUsageDetails{
				Reserved: &reservedQty,
				Used:     &usedQty,
			},
		},
	}
	return fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(spu).
		WithStatusSubresource(&storagepolicyv1alpha3.StoragePolicyUsage{}).
		Build()
}

func getTestSPU(t *testing.T, ctx context.Context, c client.Client) *storagepolicyv1alpha3.StoragePolicyUsage {
	spu := &storagepolicyv1alpha3.StoragePolicyUsage{}
	assert.NoError(t, c.Get(ctx, k8stypes.NamespacedName{Namespace: testSPUNamespace, Name: testSPUName}, spu))
	return spu
}

func addReserved(qty string) func(*storagepolicyv1alpha3.StoragePolicyUsage) bool {
	return func(spu *storagepolicyv1alpha3.StoragePolicyUsage) bool {
		spu.Status.ResourceTypeLevelQuotaUsage.Reserved.Add(resource.MustParse(qty))
		return true
	}
}

// TestPatchStoragePolicyUsageRejectsStaleObject verifies the patch carries resourceVersion, so a
// patch computed from a stale copy is rejected instead of silently overwriting a newer write.
func TestPatchStoragePolicyUsageRejectsStaleObject(t *testing.T) {
	ctx := context.Background()
	c := newTestSPUClient(t, "10Gi")

	stale := getTestSPU(t, ctx, c)
	// A concurrent writer bumps Reserved after "stale" was read.
	assert.NoError(t, UpdateStoragePolicyUsage(ctx, c, getTestSPU(t, ctx, c), addReserved("5Gi")))

	patched := stale.DeepCopy()
	patched.Status.ResourceTypeLevelQuotaUsage.Reserved.Add(resource.MustParse("1Gi"))
	err := PatchStoragePolicyUsage(ctx, c, stale, patched)
	assert.True(t, apierrors.IsConflict(err), "expected conflict, got %v", err)

	got := getTestSPU(t, ctx, c).Status.ResourceTypeLevelQuotaUsage.Reserved
	assert.Equal(t, int64(15*1024*1024*1024), got.Value())
}

// TestUpdateStoragePolicyUsageReappliesOnConflict verifies a relative change computed from a
// stale copy is re-applied to the latest CR instead of overwriting the concurrent update.
func TestUpdateStoragePolicyUsageReappliesOnConflict(t *testing.T) {
	ctx := context.Background()
	c := newTestSPUClient(t, "10Gi")

	stale := getTestSPU(t, ctx, c)
	assert.NoError(t, UpdateStoragePolicyUsage(ctx, c, getTestSPU(t, ctx, c), addReserved("5Gi")))

	calls := 0
	err := UpdateStoragePolicyUsage(ctx, c, stale, func(spu *storagepolicyv1alpha3.StoragePolicyUsage) bool {
		calls++
		return addReserved("1Gi")(spu)
	})
	assert.NoError(t, err)
	assert.Equal(t, 2, calls, "mutate should be re-run against the re-fetched CR")

	got := getTestSPU(t, ctx, c).Status.ResourceTypeLevelQuotaUsage.Reserved
	assert.Equal(t, int64(16*1024*1024*1024), got.Value())
}

// TestUpdateStoragePolicyUsageSkip verifies no patch is sent when mutate returns false.
func TestUpdateStoragePolicyUsageSkip(t *testing.T) {
	ctx := context.Background()
	c := newTestSPUClient(t, "10Gi")

	before := getTestSPU(t, ctx, c)
	err := UpdateStoragePolicyUsage(ctx, c, before.DeepCopy(), func(spu *storagepolicyv1alpha3.StoragePolicyUsage) bool {
		spu.Status.ResourceTypeLevelQuotaUsage.Reserved.Add(resource.MustParse("1Gi"))
		return false
	})
	assert.NoError(t, err)
	after := getTestSPU(t, ctx, c)
	assert.Equal(t, before.ResourceVersion, after.ResourceVersion)
	assert.Equal(t, int64(10*1024*1024*1024), after.Status.ResourceTypeLevelQuotaUsage.Reserved.Value())
}

// TestUpdateStoragePolicyUsageConcurrent runs many concurrent increments, all starting from the
// same stale copy, and verifies none of them is lost.
func TestUpdateStoragePolicyUsageConcurrent(t *testing.T) {
	ctx := context.Background()
	c := newTestSPUClient(t, "0")

	const writers = 5
	stale := getTestSPU(t, ctx, c)
	var wg sync.WaitGroup
	errs := make([]error, writers)
	for i := 0; i < writers; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			errs[i] = UpdateStoragePolicyUsage(ctx, c, stale.DeepCopy(), addReserved("1Gi"))
		}(i)
	}
	wg.Wait()
	for _, err := range errs {
		assert.NoError(t, err)
	}

	got := getTestSPU(t, ctx, c).Status.ResourceTypeLevelQuotaUsage.Reserved
	assert.Equal(t, int64(writers*1024*1024*1024), got.Value())
}
