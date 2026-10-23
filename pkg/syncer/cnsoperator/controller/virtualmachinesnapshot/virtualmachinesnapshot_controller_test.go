/*
Copyright 2025 The Kubernetes Authors.

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

package virtualmachinesnapshot

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	vmoperatortypes "github.com/vmware-tanzu/vm-operator/api/v1alpha5"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	apitypes "k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/record"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
)

const testNamespace = "test-ns"

// newTestReconciler returns a reconciler backed by a fake client holding the
// given VirtualMachineSnapshots. If patchErr is non-nil, every Patch issued by
// the reconciler fails with it, which drives Reconcile down its error path.
func newTestReconciler(t *testing.T, patchErr error,
	snapshots ...*vmoperatortypes.VirtualMachineSnapshot) *ReconcileVirtualMachineSnapshot {
	t.Helper()
	scheme := runtime.NewScheme()
	if err := vmoperatortypes.AddToScheme(scheme); err != nil {
		t.Fatalf("failed to register vmoperator scheme: %v", err)
	}
	builder := fake.NewClientBuilder().WithScheme(scheme)
	for _, s := range snapshots {
		builder = builder.WithObjects(s)
	}
	if patchErr != nil {
		builder = builder.WithInterceptorFuncs(interceptor.Funcs{
			Patch: func(ctx context.Context, cli client.WithWatch, obj client.Object,
				patch client.Patch, opts ...client.PatchOption) error {
				return patchErr
			},
		})
	}
	c := builder.Build()
	return &ReconcileVirtualMachineSnapshot{
		client:                 c,
		scheme:                 scheme,
		vmSnapVMOperatorClient: c,
		// A FakeRecorder with a nil Events channel discards events without blocking.
		recorder: &record.FakeRecorder{},
	}
}

// resetBackOffDuration installs an empty backOffDuration map for the test and
// clears it afterwards.
func resetBackOffDuration(t *testing.T) {
	t.Helper()
	backOffDurationMapMutex.Lock()
	backOffDuration = make(map[apitypes.NamespacedName]time.Duration)
	backOffDurationMapMutex.Unlock()
	t.Cleanup(func() {
		backOffDurationMapMutex.Lock()
		backOffDuration = nil
		backOffDurationMapMutex.Unlock()
	})
}

func newVMSnapshot(name string) *vmoperatortypes.VirtualMachineSnapshot {
	return &vmoperatortypes.VirtualMachineSnapshot{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: testNamespace},
	}
}

func getBackOffDuration(key apitypes.NamespacedName) (time.Duration, bool) {
	backOffDurationMapMutex.Lock()
	defer backOffDurationMapMutex.Unlock()
	d, ok := backOffDuration[key]
	return d, ok
}

func TestReconcile_FailureDoublesBackOff(t *testing.T) {
	resetBackOffDuration(t)
	snap := newVMSnapshot("snap")
	r := newTestReconciler(t, errors.New("patch failed"), snap)
	req := reconcile.Request{NamespacedName: apitypes.NamespacedName{Namespace: testNamespace, Name: snap.Name}}

	// RequeueAfter is the backoff captured before recordEvent doubles it.
	for _, want := range []time.Duration{time.Second, 2 * time.Second, 4 * time.Second} {
		result, err := r.Reconcile(context.Background(), req)
		if err != nil {
			t.Fatalf("Reconcile returned unexpected error: %v", err)
		}
		if result.RequeueAfter != want {
			t.Fatalf("expected RequeueAfter %v, got %v", want, result.RequeueAfter)
		}
	}
	if got, _ := getBackOffDuration(req.NamespacedName); got != 8*time.Second {
		t.Fatalf("expected stored backoff 8s after three failures, got %v", got)
	}
}

func TestReconcile_SuccessClearsBackOff(t *testing.T) {
	resetBackOffDuration(t)
	snap := newVMSnapshot("snap")
	r := newTestReconciler(t, nil, snap)
	req := reconcile.Request{NamespacedName: apitypes.NamespacedName{Namespace: testNamespace, Name: snap.Name}}

	backOffDurationMapMutex.Lock()
	backOffDuration[req.NamespacedName] = 16 * time.Second
	backOffDurationMapMutex.Unlock()

	result, err := r.Reconcile(context.Background(), req)
	if err != nil {
		t.Fatalf("Reconcile returned unexpected error: %v", err)
	}
	if result != (reconcile.Result{}) {
		t.Fatalf("expected empty Result on success, got %+v", result)
	}
	if _, ok := getBackOffDuration(req.NamespacedName); ok {
		t.Fatal("expected backoff entry to be removed after successful reconcile")
	}
}

// TestReconcile_ConcurrentFailures runs failing reconciles for many snapshots in
// parallel, as the controller does with MaxConcurrentReconciles > 1. Run with
// -race: every access to backOffDuration on the error path must hold
// backOffDurationMapMutex, otherwise the race detector reports a concurrent
// map read and write.
func TestReconcile_ConcurrentFailures(t *testing.T) {
	resetBackOffDuration(t)
	const numSnapshots = 10
	const rounds = 5
	snapshots := make([]*vmoperatortypes.VirtualMachineSnapshot, numSnapshots)
	for i := range snapshots {
		snapshots[i] = newVMSnapshot(fmt.Sprintf("snap-%d", i))
	}
	r := newTestReconciler(t, errors.New("patch failed"), snapshots...)

	var wg sync.WaitGroup
	for _, snap := range snapshots {
		wg.Add(1)
		go func(name string) {
			defer wg.Done()
			req := reconcile.Request{NamespacedName: apitypes.NamespacedName{Namespace: testNamespace, Name: name}}
			for i := 0; i < rounds; i++ {
				if _, err := r.Reconcile(context.Background(), req); err != nil {
					t.Errorf("Reconcile %s returned unexpected error: %v", name, err)
					return
				}
			}
		}(snap.Name)
	}
	wg.Wait()

	want := time.Second << rounds
	for _, snap := range snapshots {
		key := apitypes.NamespacedName{Namespace: testNamespace, Name: snap.Name}
		if got, _ := getBackOffDuration(key); got != want {
			t.Errorf("expected backoff %v for %s, got %v", want, snap.Name, got)
		}
	}
}
