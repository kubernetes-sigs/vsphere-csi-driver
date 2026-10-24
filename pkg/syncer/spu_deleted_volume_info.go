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
	"fmt"
	"reflect"

	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/dynamic/dynamicinformer"
	"k8s.io/client-go/tools/cache"
	"sigs.k8s.io/vsphere-csi-driver/v3/pkg/csi/service/logger"
	"sigs.k8s.io/vsphere-csi-driver/v3/pkg/internalapis/cnsvolumeinfo"
	k8s "sigs.k8s.io/vsphere-csi-driver/v3/pkg/kubernetes"
)

// Keep the provisional external API contract here. CSI only reads DVI; the protection
// service owns its CRD, creation, sizing and deletion. No generated API dependency is needed.
var deletedVolumeInfoGVR = schema.GroupVersionResource{
	Group: "protectionandrecovery.vmware.com", Version: "v1alpha1", Resource: "deletedvolumeinfos",
}

const (
	snapshotVolumeIDIndex = "snapshot-volume-id"
	// An unassignable record makes the inventory incomplete. Index it rather than dropping
	// it, and enqueue a sweep so existing SPUs cannot silently lose its charge.
	unassignedDeletedVolumeInfoKey = "unassigned-deleted-volume-info"
)

// deletedVolumeInfo is CSI's private read model, not a second definition of the CRD.
type deletedVolumeInfo struct {
	volumeID                 string
	namespace                string
	storageClassName         string
	volumeAttributeClassName string
	storagePolicyID          string
	sizeBytes                int64
	validSize                bool
}

func decodeDeletedVolumeInfo(u *unstructured.Unstructured) (deletedVolumeInfo, error) {
	var d deletedVolumeInfo
	for field, dest := range map[string]*string{
		"volumeID": &d.volumeID, "namespace": &d.namespace,
		"storageClassName": &d.storageClassName, "storagePolicyID": &d.storagePolicyID,
	} {
		value, _, err := unstructured.NestedString(u.Object, "spec", field)
		if err != nil || value == "" {
			return d, fmt.Errorf("DeletedVolumeInfo %s/%s has no valid %s", u.GetNamespace(), u.GetName(), field)
		}
		*dest = value
	}
	var err error
	d.volumeAttributeClassName, _, err = unstructured.NestedString(u.Object, "spec", "volumeAttributeClassName")
	if err != nil {
		return d, fmt.Errorf("DeletedVolumeInfo %s/%s has malformed volumeAttributeClassName: %w",
			u.GetNamespace(), u.GetName(), err)
	}
	var found bool
	d.sizeBytes, found, err = unstructured.NestedInt64(u.Object, "spec", "sizeBytes")
	if err != nil || !found || d.sizeBytes < 0 {
		return d, fmt.Errorf("DeletedVolumeInfo %s/%s requires nonnegative integer sizeBytes",
			u.GetNamespace(), u.GetName())
	}
	d.validSize, found, err = unstructured.NestedBool(u.Object, "spec", "validSize")
	if err != nil || !found {
		return d, fmt.Errorf("DeletedVolumeInfo %s/%s has missing or malformed validSize",
			u.GetNamespace(), u.GetName())
	}
	return d, nil
}

func deletedVolumeInfoSPUKey(u *unstructured.Unstructured) (string, bool) {
	// Validate association fields before reusing the CVI naming rules. Invalid sizing or
	// policy fields must still reach the correct SPU, where reconciliation reports them.
	for _, field := range []string{"namespace", "storageClassName"} {
		value, _, err := unstructured.NestedString(u.Object, "spec", field)
		if err != nil || value == "" {
			return "", false
		}
	}
	if _, _, err := unstructured.NestedString(u.Object, "spec", "volumeAttributeClassName"); err != nil {
		return "", false
	}
	return cnsvolumeinfo.SPUSnapshotKey(u)
}

func snapshotVolumeID(obj interface{}) string {
	u, ok := obj.(*unstructured.Unstructured)
	if !ok {
		return ""
	}
	id, _, _ := unstructured.NestedString(u.Object, "spec", "volumeID")
	return id
}

func snapshotVolumeIDIndexFunc(obj interface{}) ([]string, error) {
	if id := snapshotVolumeID(obj); id != "" {
		return []string{id}, nil
	}
	return nil, nil
}

func newDeletedVolumeInfoInformer(ctx context.Context, enabled bool) (cache.SharedIndexInformer, error) {
	if !enabled {
		return nil, nil
	}
	cfg, err := k8s.GetKubeConfig(ctx)
	if err != nil {
		return nil, err
	}
	c, err := dynamic.NewForConfig(cfg)
	if err != nil {
		return nil, err
	}
	return deletedVolumeInfoInformer(c), nil
}

func deletedVolumeInfoInformer(c dynamic.Interface) cache.SharedIndexInformer {
	// DVI lives in tenant system namespaces, whereas its quota is charged to spec.namespace.
	// The Supervisor syncer needs inventory from all tenant system namespaces.
	return dynamicinformer.NewFilteredDynamicInformer(c, deletedVolumeInfoGVR, metav1.NamespaceAll, 0,
		cache.Indexers{
			cnsvolumeinfo.SPUSnapshotIndex: func(obj interface{}) ([]string, error) {
				if u, ok := obj.(*unstructured.Unstructured); ok {
					if key, valid := deletedVolumeInfoSPUKey(u); valid {
						return []string{key}, nil
					}
				}
				return []string{unassignedDeletedVolumeInfoKey}, nil
			},
			snapshotVolumeIDIndex: snapshotVolumeIDIndexFunc,
		}, nil).Informer()
}

var deletedVolumeInfoFields = []string{"volumeID", "namespace", "storageClassName",
	"volumeAttributeClassName", "storagePolicyID", "sizeBytes", "validSize"}

func deletedVolumeInfoFieldsChanged(oldObj, newObj interface{}) bool {
	oldU, oldOK := oldObj.(*unstructured.Unstructured)
	newU, newOK := newObj.(*unstructured.Unstructured)
	if !oldOK || !newOK {
		return true
	}
	for _, field := range deletedVolumeInfoFields {
		oldValue, oldFound, _ := unstructured.NestedFieldNoCopy(oldU.Object, "spec", field)
		newValue, newFound, _ := unstructured.NestedFieldNoCopy(newU.Object, "spec", field)
		if oldFound != newFound || !reflect.DeepEqual(oldValue, newValue) {
			return true
		}
	}
	return false
}

func (r *spuSnapshotReconciler) enqueueDeletedVolumeInfo(obj interface{}) {
	if tombstone, ok := obj.(cache.DeletedFinalStateUnknown); ok {
		obj = tombstone.Obj
	}
	if u, ok := obj.(*unstructured.Unstructured); ok {
		if key, valid := deletedVolumeInfoSPUKey(u); valid {
			r.queue.Add(key)
			return
		}
		logger.GetLogger(context.Background()).Errorf(
			"Cannot identify snapshot StoragePolicyUsage for DeletedVolumeInfo %s/%s",
			u.GetNamespace(), u.GetName())
	}
	r.queue.Add(unassignedDeletedVolumeInfoKey)
}

// enqueueDeletedVolumeInfoForCVI handles restores and deletion handoffs even when the
// CVI and DVI belong to different SPUs, or the restored CVI has no snapshot size yet.
func (r *spuSnapshotReconciler) enqueueDeletedVolumeInfoForCVI(obj interface{}) {
	if r.dviInformer == nil {
		return
	}
	id := snapshotVolumeID(obj)
	if id == "" {
		return
	}
	objs, err := r.dviInformer.GetIndexer().ByIndex(snapshotVolumeIDIndex, id)
	if err != nil {
		logger.GetLogger(context.Background()).Errorf("Cannot find DeletedVolumeInfo for volume %q: %v", id, err)
		r.queue.Add(unassignedDeletedVolumeInfoKey)
		return
	}
	for _, dvi := range objs {
		r.enqueueDeletedVolumeInfo(dvi)
	}
}

func (r *spuSnapshotReconciler) deletedVolumeInfosForSPU(key string) ([]interface{}, error) {
	if r.dviInformer == nil {
		return nil, nil
	}
	unassigned, err := r.dviInformer.GetIndexer().ByIndex(cnsvolumeinfo.SPUSnapshotIndex, unassignedDeletedVolumeInfoKey)
	if err != nil {
		return nil, err
	}
	if len(unassigned) != 0 {
		return nil, fmt.Errorf("%d DeletedVolumeInfo records have invalid quota associations", len(unassigned))
	}
	return r.dviInformer.GetIndexer().ByIndex(cnsvolumeinfo.SPUSnapshotIndex, key)
}

func (r *spuSnapshotReconciler) sumDeletedVolumeInfoCapacity(objs, liveObjs []interface{},
	policyID string) (*resource.Quantity, bool, error) {
	total := resource.NewQuantity(0, resource.BinarySI)
	missing := false
	// A CVI can disappear after the caller reads its SPU index. Do not count its DVI
	// in the same pass as the already captured CVI contribution.
	liveIDs := make(map[string]bool, len(liveObjs))
	for _, obj := range liveObjs {
		if id := snapshotVolumeID(obj); id != "" {
			liveIDs[id] = true
		}
	}
	seen := make(map[string]bool, len(objs))
	for _, obj := range objs {
		u, ok := obj.(*unstructured.Unstructured)
		if !ok {
			return nil, false, fmt.Errorf("unexpected DeletedVolumeInfo cache object %T", obj)
		}
		// A known different policy is irrelevant to this SPU, including its size validity.
		policy, _, err := unstructured.NestedString(u.Object, "spec", "storagePolicyID")
		if err == nil && policy != "" && policy != policyID {
			continue
		}
		d, err := decodeDeletedVolumeInfo(u)
		if err != nil {
			return nil, false, err
		}
		live, err := r.informer.GetIndexer().ByIndex(snapshotVolumeIDIndex, d.volumeID)
		if err != nil {
			return nil, false, err
		}
		// CVI remains authoritative even while Terminating. DPO must create DVI before
		// releasing its CVI finalizer, and remove DVI when a restored CVI takes over.
		if liveIDs[d.volumeID] || len(live) != 0 {
			continue
		}
		if seen[d.volumeID] {
			return nil, false, fmt.Errorf("multiple DeletedVolumeInfo records for volume %q", d.volumeID)
		}
		seen[d.volumeID] = true
		total.Add(*resource.NewQuantity(d.sizeBytes, resource.BinarySI))
		missing = missing || !d.validSize
	}
	return total, missing, nil
}
