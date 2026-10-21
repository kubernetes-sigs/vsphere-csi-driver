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
	"fmt"
	"testing"

	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/client-go/tools/cache"
)

func newCVI(name string, spec map[string]interface{}) *unstructured.Unstructured {
	return &unstructured.Unstructured{Object: map[string]interface{}{
		"apiVersion": "cns.vmware.com/v1alpha1",
		"kind":       "CNSVolumeInfo",
		"metadata":   map[string]interface{}{"name": name, "namespace": "vmware-system-csi"},
		"spec":       spec,
	}}
}

func TestSPUSnapshotKey(t *testing.T) {
	tests := []struct {
		name   string
		spec   map[string]interface{}
		want   string
		wantOK bool
	}{
		{"storage class", map[string]interface{}{"namespace": "ns1", "storageClassName": "gold"},
			"ns1/gold-snapshot-usage", true},
		{"vac takes precedence with prefix",
			map[string]interface{}{"namespace": "ns1", "storageClassName": "gold", "volumeAttributeClassName": "fast"},
			"ns1/vac-fast-snapshot-usage", true},
		{"vac and sc with same name do not collide",
			map[string]interface{}{"namespace": "ns1", "storageClassName": "x", "volumeAttributeClassName": "gold"},
			"ns1/vac-gold-snapshot-usage", true},
		{"missing namespace", map[string]interface{}{"storageClassName": "gold"}, "", false},
		{"missing class", map[string]interface{}{"namespace": "ns1"}, "", false},
		{"malformed namespace", map[string]interface{}{"namespace": int64(1), "storageClassName": "gold"}, "", false},
		{"malformed storage class", map[string]interface{}{"namespace": "ns1", "storageClassName": int64(1)}, "", false},
		{"malformed vac does not fall back to storage class",
			map[string]interface{}{"namespace": "ns1", "storageClassName": "gold", "volumeAttributeClassName": int64(1)},
			"", false},
		{"vac does not require a storage class",
			map[string]interface{}{"namespace": "ns1", "volumeAttributeClassName": "fast"},
			"ns1/vac-fast-snapshot-usage", true},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got, ok := SPUSnapshotKey(newCVI("v", tc.spec))
			if got != tc.want || ok != tc.wantOK {
				t.Fatalf("got (%q, %v), want (%q, %v)", got, ok, tc.want, tc.wantOK)
			}
		})
	}
	if key, ok := SPUSnapshotKey(nil); ok || key != "" {
		t.Fatalf("nil CVI has key (%q, %v)", key, ok)
	}
}

func TestHasSnapshotInfo(t *testing.T) {
	tests := []struct {
		name string
		spec map[string]interface{}
		want bool
	}{
		{"never recorded snapshots", map[string]interface{}{}, false},
		{"default invalid flag alone", map[string]interface{}{"validaggregatedsnapshotsize": false}, false},
		{"null operation time", map[string]interface{}{"snapshotlatestoperationcompletetime": nil}, false},
		{"empty operation time", map[string]interface{}{"snapshotlatestoperationcompletetime": ""}, false},
		{"zero operation time", map[string]interface{}{"snapshotlatestoperationcompletetime": "0001-01-01T00:00:00Z"}, false},
		{"recorded operation", map[string]interface{}{"snapshotlatestoperationcompletetime": "2026-10-07T22:25:46Z"}, true},
		{"malformed operation", map[string]interface{}{"snapshotlatestoperationcompletetime": int64(1)}, true},
		{"positive capacity", map[string]interface{}{"aggregatedsnapshotsize": "1Gi"}, true},
		{"zero capacity", map[string]interface{}{"aggregatedsnapshotsize": "0"}, true},
		{"negative capacity", map[string]interface{}{"aggregatedsnapshotsize": "-1Gi"}, true},
		{"malformed quantity", map[string]interface{}{"aggregatedsnapshotsize": "invalid"}, true},
		{"wrong capacity type", map[string]interface{}{"aggregatedsnapshotsize": int64(1)}, true},
		{"null capacity", map[string]interface{}{"aggregatedsnapshotsize": nil}, true},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if got := HasSnapshotInfo(newCVI("v", tc.spec)); got != tc.want {
				t.Fatalf("HasSnapshotInfo = %v, want %v", got, tc.want)
			}
		})
	}
	if HasSnapshotInfo(nil) {
		t.Fatal("nil CVI must not have snapshot information")
	}
}

func TestSPUSnapshotIndex(t *testing.T) {
	idx := cache.NewIndexer(cache.MetaNamespaceKeyFunc, SPUSnapshotIndexers())
	base := func() map[string]interface{} {
		return map[string]interface{}{"namespace": "ns1", "storageClassName": "gold"}
	}
	withSize := base()
	withSize["aggregatedsnapshotsize"] = "150Mi"
	withTime := base()
	withTime["snapshotlatestoperationcompletetime"] = "2026-10-07T22:25:46Z"
	zeroTime := base()
	zeroTime["snapshotlatestoperationcompletetime"] = "0001-01-01T00:00:00Z"
	nullTime := base()
	nullTime["snapshotlatestoperationcompletetime"] = nil

	for name, spec := range map[string]map[string]interface{}{
		"a": withSize, "b": withTime, "c": base(), "d": zeroTime, "e": nullTime,
	} {
		if err := idx.Add(newCVI(name, spec)); err != nil {
			t.Fatal(err)
		}
	}
	objs, err := idx.ByIndex(SPUSnapshotIndex, "ns1/gold-snapshot-usage")
	if err != nil {
		t.Fatal(err)
	}
	if len(objs) != 2 {
		t.Fatalf("expected only CRs with a snapshot size or snapshot time to be indexed, got %d", len(objs))
	}

	// A CR which loses its snapshot size drops out of the index on update.
	if err := idx.Update(newCVI("a", base())); err != nil {
		t.Fatal(err)
	}
	objs, _ = idx.ByIndex(SPUSnapshotIndex, "ns1/gold-snapshot-usage")
	if len(objs) != 1 {
		t.Fatalf("expected 1 indexed CR after update, got %d", len(objs))
	}
}

func TestSPUSnapshotIndexRetainsMalformedContributors(t *testing.T) {
	idx := cache.NewIndexer(cache.MetaNamespaceKeyFunc, SPUSnapshotIndexers())
	u := newCVI("v", map[string]interface{}{
		"namespace": "ns1", "storageClassName": "gold", "aggregatedsnapshotsize": "10Gi",
	})
	if err := idx.Add(u); err != nil {
		t.Fatal(err)
	}
	for _, capacity := range []interface{}{int64(10), nil, "malformed", "-10Gi"} {
		updated := u.DeepCopy()
		updated.Object["spec"].(map[string]interface{})["aggregatedsnapshotsize"] = capacity
		if err := idx.Update(updated); err != nil {
			t.Fatal(err)
		}
		objs, err := idx.ByIndex(SPUSnapshotIndex, "ns1/gold-snapshot-usage")
		if err != nil || len(objs) != 1 {
			t.Fatalf("invalid capacity %v disappeared from index: objects=%d, error=%v", capacity, len(objs), err)
		}
	}
}

func TestSPUSnapshotIndexMovesAndDeletesContribution(t *testing.T) {
	idx := cache.NewIndexer(cache.MetaNamespaceKeyFunc, SPUSnapshotIndexers())
	u := newCVI("v", map[string]interface{}{
		"namespace": "ns1", "storageClassName": "gold", "aggregatedsnapshotsize": "10Gi",
	})
	if err := idx.Add(u); err != nil {
		t.Fatal(err)
	}
	updated := u.DeepCopy()
	updated.Object["spec"].(map[string]interface{})["namespace"] = "ns2"
	updated.Object["spec"].(map[string]interface{})["volumeAttributeClassName"] = "fast"
	if err := idx.Update(updated); err != nil {
		t.Fatal(err)
	}
	for key, want := range map[string]int{"ns1/gold-snapshot-usage": 0, "ns2/vac-fast-snapshot-usage": 1} {
		objs, err := idx.ByIndex(SPUSnapshotIndex, key)
		if err != nil || len(objs) != want {
			t.Fatalf("key %s: got %d objects and error %v, want %d", key, len(objs), err, want)
		}
	}
	if err := idx.Delete(updated); err != nil {
		t.Fatal(err)
	}
	objs, err := idx.ByIndex(SPUSnapshotIndex, "ns2/vac-fast-snapshot-usage")
	if err != nil || len(objs) != 0 {
		t.Fatalf("deleted CVI remains indexed: objects=%d, error=%v", len(objs), err)
	}
}

func TestSPUSnapshotIndexUnassignableObjectDoesNotStopCache(t *testing.T) {
	idx := cache.NewIndexer(cache.MetaNamespaceKeyFunc, SPUSnapshotIndexers())
	u := newCVI("v", map[string]interface{}{
		"namespace": "ns1", "storageClassName": "gold", "aggregatedsnapshotsize": "10Gi",
		"volumeAttributeClassName": int64(1),
	})
	if err := idx.Add(u); err != nil {
		t.Fatalf("unassignable CVI must not fail the cache update: %v", err)
	}
	if _, exists, err := idx.GetByKey("vmware-system-csi/v"); err != nil || !exists {
		t.Fatalf("unassignable CVI must remain in the informer store: exists=%v, error=%v", exists, err)
	}
	objs, err := idx.ByIndex(SPUSnapshotIndex, "ns1/gold-snapshot-usage")
	if err != nil || len(objs) != 0 {
		t.Fatalf("malformed VAC must not be charged to the StorageClass: objects=%d, error=%v", len(objs), err)
	}
}

// BenchmarkSPUSnapshotIndex120k measures local index lookup only. It excludes API delivery,
// inventory construction, quantity summing, and quota propagation, so it is not a settlement SLA.
func BenchmarkSPUSnapshotIndex120k(b *testing.B) {
	for _, groups := range []int{1, 600} {
		b.Run(fmt.Sprintf("%d_SPUs", groups), func(b *testing.B) {
			idx := cache.NewIndexer(cache.MetaNamespaceKeyFunc, SPUSnapshotIndexers())
			for i := 0; i < 120000; i++ {
				u := newCVI(fmt.Sprintf("v-%d", i), map[string]interface{}{
					"namespace": "ns1", "storageClassName": fmt.Sprintf("sc-%d", i%groups),
					"aggregatedsnapshotsize": "10Gi", "validaggregatedsnapshotsize": true,
				})
				if err := idx.Add(u); err != nil {
					b.Fatal(err)
				}
			}
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				objs, err := idx.ByIndex(SPUSnapshotIndex, "ns1/sc-0-snapshot-usage")
				if err != nil || len(objs) != 120000/groups {
					b.Fatalf("objects=%d, error=%v", len(objs), err)
				}
			}
		})
	}
}
