package syncer

import (
	"fmt"
	"testing"

	"k8s.io/client-go/tools/cache"

	"sigs.k8s.io/vsphere-csi-driver/v3/pkg/internalapis/cnsvolumeinfo"
)

// BenchmarkSPUSnapshotUsed measures the local indexed sum for a single busy SPU.
// It deliberately excludes API requests, watch delivery, and inventory setup.
func BenchmarkSPUSnapshotUsed(b *testing.B) {
	for _, count := range []int{1000, 120000} {
		b.Run(fmt.Sprintf("volumes-%d", count), func(b *testing.B) {
			index := cache.NewIndexer(cache.MetaNamespaceKeyFunc, cnsvolumeinfo.SPUSnapshotIndexers())
			for i := 0; i < count; i++ {
				if err := index.Add(testCVI(fmt.Sprintf("volume-%d", i), "gold", "", testPolicyID,
					"1Mi", true, "")); err != nil {
					b.Fatal(err)
				}
			}
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				objs, err := index.ByIndex(cnsvolumeinfo.SPUSnapshotIndex, testSPUKey)
				if err != nil {
					b.Fatal(err)
				}
				total, _, err := sumSnapshotCapacity(objs, testPolicyID)
				if err != nil {
					b.Fatal(err)
				}
				if total.Value() != int64(count)*1024*1024 {
					b.Fatalf("unexpected sum: %s", total.String())
				}
			}
		})
	}
}
