package cnsvolumeinfo

import (
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
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got, ok := SPUSnapshotKey(newCVI("v", tc.spec))
			if got != tc.want || ok != tc.wantOK {
				t.Fatalf("got (%q, %v), want (%q, %v)", got, ok, tc.want, tc.wantOK)
			}
		})
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
