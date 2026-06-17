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
	"testing"

	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/client-go/util/workqueue"
)

// makeUnstructuredSPQ returns a minimal *unstructured.Unstructured representing
// a StoragePolicyQuota with the given name, namespace, and storagePolicyId.
// The structure mirrors what the dynamic SPQ informer stores in its cache.
func makeUnstructuredSPQ(name, namespace, policyID string) *unstructured.Unstructured {
	return &unstructured.Unstructured{
		Object: map[string]interface{}{
			"apiVersion": "cns.vmware.com/v1alpha3",
			"kind":       "StoragePolicyQuota",
			"metadata": map[string]interface{}{
				"name":      name,
				"namespace": namespace,
			},
			"spec": map[string]interface{}{
				"storagePolicyId": policyID,
				"limit":           "9223372036854775807",
			},
		},
	}
}

// newTestReconciler creates a storagePolicyQuotaReconciler with a real (but
// unstarted) rate-limiting queue and no cluster connectivity, suitable for
// unit-testing the handler methods in isolation.
func newTestReconciler() *storagePolicyQuotaReconciler {
	q := workqueue.NewTypedRateLimitingQueueWithConfig(
		workqueue.DefaultTypedControllerRateLimiter[any](),
		workqueue.TypedRateLimitingQueueConfig[any]{Name: "test-spq"},
	)
	return &storagePolicyQuotaReconciler{
		svcQuotaOpsQueue: q,
	}
}

// drainQueue returns all items currently in the queue without blocking.
func drainQueue(q workqueue.TypedRateLimitingInterface[any]) []*ObjectToProcess {
	var items []*ObjectToProcess
	for q.Len() > 0 {
		obj, _ := q.Get()
		items = append(items, obj.(*ObjectToProcess))
		q.Done(obj)
	}
	return items
}

// TestAddStoragePolicyQuota_EnqueuesWithIsDeletedFalse verifies that a valid
// Unstructured SPQ is parsed and enqueued with isDeleted=false, and that the
// SPQ's name, namespace, and storagePolicyId are faithfully preserved.
func TestAddStoragePolicyQuota_EnqueuesWithIsDeletedFalse(t *testing.T) {
	rc := newTestReconciler()
	spq := makeUnstructuredSPQ("my-spq", "my-ns", "policy-123")

	rc.addStoragePolicyQuota(spq)

	items := drainQueue(rc.svcQuotaOpsQueue)
	if len(items) != 1 {
		t.Fatalf("expected 1 queued item, got %d", len(items))
	}
	if items[0].isDeleted {
		t.Errorf("isDeleted = true, want false")
	}
	if items[0].policyQuota.Name != "my-spq" {
		t.Errorf("Name = %q, want %q", items[0].policyQuota.Name, "my-spq")
	}
	if items[0].policyQuota.Namespace != "my-ns" {
		t.Errorf("Namespace = %q, want %q", items[0].policyQuota.Namespace, "my-ns")
	}
	if items[0].policyQuota.Spec.StoragePolicyId != "policy-123" {
		t.Errorf("StoragePolicyId = %q, want %q", items[0].policyQuota.Spec.StoragePolicyId, "policy-123")
	}
}

// TestDelStoragePolicyQuota_EnqueuesWithIsDeletedTrue verifies that a valid
// Unstructured SPQ is parsed and enqueued with isDeleted=true when the
// DeleteFunc fires, so the worker calls syncStoragePolicyQuotaDelCase.
func TestDelStoragePolicyQuota_EnqueuesWithIsDeletedTrue(t *testing.T) {
	rc := newTestReconciler()
	spq := makeUnstructuredSPQ("del-spq", "del-ns", "policy-del")

	rc.delStoragePolicyQuota(spq)

	items := drainQueue(rc.svcQuotaOpsQueue)
	if len(items) != 1 {
		t.Fatalf("expected 1 queued item, got %d", len(items))
	}
	if !items[0].isDeleted {
		t.Errorf("isDeleted = false, want true")
	}
	if items[0].policyQuota.Name != "del-spq" {
		t.Errorf("Name = %q, want %q", items[0].policyQuota.Name, "del-spq")
	}
	if items[0].policyQuota.Spec.StoragePolicyId != "policy-del" {
		t.Errorf("StoragePolicyId = %q, want %q", items[0].policyQuota.Spec.StoragePolicyId, "policy-del")
	}
}
