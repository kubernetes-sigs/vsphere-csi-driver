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

	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/client-go/tools/cache"

	storagepolicyv1alpha3 "sigs.k8s.io/vsphere-csi-driver/v3/pkg/apis/cnsoperator/storagepolicy/v1alpha3"
	"sigs.k8s.io/vsphere-csi-driver/v3/pkg/csi/service/logger"
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
// It returns false when a required association field is missing or malformed. In particular, an
// invalid VAC field must not silently fall back to the StorageClass and charge the wrong SPU.
func SPUSnapshotKey(u *unstructured.Unstructured) (string, bool) {
	if u == nil {
		return "", false
	}
	ns, _, err := unstructured.NestedString(u.Object, "spec", "namespace")
	if err != nil || ns == "" {
		return "", false
	}
	vac, _, err := unstructured.NestedString(u.Object, "spec", "volumeAttributeClassName")
	if err != nil {
		return "", false
	}
	var name string
	if vac != "" {
		name = vacSPUPrefix + vac
	} else {
		name, _, err = unstructured.NestedString(u.Object, "spec", "storageClassName")
		if err != nil || name == "" {
			return "", false
		}
	}
	return ns + "/" + name + "-" + storagepolicyv1alpha3.NameSuffixForSnapshot, true
}

// HasSnapshotInfo reports whether a CVI may contribute snapshot usage or a missing-capacity
// diagnostic. A malformed capacity must remain indexed so the reconciler can reject an incomplete
// sum instead of silently omitting the volume. A null/empty/zero operation time by itself is the
// existing representation of a volume which has not recorded snapshot activity.
func HasSnapshotInfo(u *unstructured.Unstructured) bool {
	if u == nil {
		return false
	}
	if _, found, err := unstructured.NestedFieldNoCopy(u.Object, "spec", "aggregatedsnapshotsize"); found || err != nil {
		return true
	}
	v, found, err := unstructured.NestedFieldNoCopy(u.Object, "spec", "snapshotlatestoperationcompletetime")
	if err != nil {
		return true
	}
	if !found || v == nil {
		return false
	}
	t, ok := v.(string)
	if !ok {
		return true
	}
	return t != "" && t != "0001-01-01T00:00:00Z"
}

// SPUSnapshotIndexers returns the indexers to be registered on the CnsVolumeInfo informer, before it starts.
func SPUSnapshotIndexers() cache.Indexers {
	return cache.Indexers{
		SPUSnapshotIndex: func(obj interface{}) ([]string, error) {
			u, ok := obj.(*unstructured.Unstructured)
			if !ok || !HasSnapshotInfo(u) {
				return nil, nil
			}
			if key, ok := SPUSnapshotKey(u); ok {
				return []string{key}, nil
			}
			// An index error can stop informer cache processing. Keep the object in the cache and
			// diagnose the unusable association instead; inventory repair must supply its key.
			logger.GetLogger(context.Background()).Errorf("Cannot index snapshot-bearing CnsVolumeInfo %q: "+
				"workload namespace or class association is missing or malformed", u.GetName())
			return nil, nil
		},
	}
}
