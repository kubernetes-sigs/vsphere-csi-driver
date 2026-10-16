package cnsvolumeinfo

import (
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/client-go/tools/cache"

	storagepolicyv1alpha3 "sigs.k8s.io/vsphere-csi-driver/v3/pkg/apis/cnsoperator/storagepolicy/v1alpha3"
)

const (
	// SPUSnapshotIndex is the name of the informer index which maps the snapshot StoragePolicyUsage
	// "<namespace>/<name>" key to the CnsVolumeInfo CRs that contribute to its Used value.
	SPUSnapshotIndex = "spuSnapshot"

	// vacSPUPrefix is prepended to a VolumeAttributesClass name when it is used to name a
	// StoragePolicyUsage CR. It avoids collisions with StorageClasses which have the same name.
	vacSPUPrefix = "vac-"
)

// SPUSnapshotKey returns the "<namespace>/<name>" key of the snapshot StoragePolicyUsage CR that the
// given CnsVolumeInfo CR accounts to. The name is "<sc>-snapshot-usage", or "vac-<vac>-snapshot-usage"
// for VAC based volumes, so the key can be used directly for a Get of the StoragePolicyUsage CR.
// Only the needed fields are read, the object is not converted to a typed CnsVolumeInfo.
// It returns false when the CR has no namespace or class name.
func SPUSnapshotKey(u *unstructured.Unstructured) (string, bool) {
	ns, _, _ := unstructured.NestedString(u.Object, "spec", "namespace")
	sc, _, _ := unstructured.NestedString(u.Object, "spec", "storageClassName")
	vac, _, _ := unstructured.NestedString(u.Object, "spec", "volumeAttributeClassName")
	name := sc
	if vac != "" {
		name = vacSPUPrefix + vac
	}
	if ns == "" || name == "" {
		return "", false
	}
	return ns + "/" + name + "-" + storagepolicyv1alpha3.NameSuffixForSnapshot, true
}

// affectsSnapshotUsage returns true if the CR can change the snapshot Used value or the missing
// snapshot capacity annotation of its StoragePolicyUsage CR: it has an aggregated snapshot size,
// or a snapshot operation was completed on the volume.
func affectsSnapshotUsage(u *unstructured.Unstructured) bool {
	if _, found, _ := unstructured.NestedString(u.Object, "spec", "aggregatedsnapshotsize"); found {
		return true
	}
	t, _, _ := unstructured.NestedString(u.Object, "spec", "snapshotlatestoperationcompletetime")
	return t != "" && t != "0001-01-01T00:00:00Z"
}

// SPUSnapshotIndexers returns the indexers to be registered on the CnsVolumeInfo informer, before it starts.
func SPUSnapshotIndexers() cache.Indexers {
	return cache.Indexers{
		SPUSnapshotIndex: func(obj interface{}) ([]string, error) {
			u, ok := obj.(*unstructured.Unstructured)
			if !ok || !affectsSnapshotUsage(u) {
				return nil, nil
			}
			if key, ok := SPUSnapshotKey(u); ok {
				return []string{key}, nil
			}
			return nil, nil
		},
	}
}
