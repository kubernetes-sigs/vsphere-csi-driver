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

package storagepolicyinfo

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	apitypes "k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"
	"sigs.k8s.io/controller-runtime/pkg/event"

	storagepolicyv1alpha2 "sigs.k8s.io/vsphere-csi-driver/v3/pkg/apis/cnsoperator/storagepolicy/v1alpha2"
	spiv1alpha1 "sigs.k8s.io/vsphere-csi-driver/v3/pkg/apis/cnsoperator/storagepolicyinfo/v1alpha1"
	"sigs.k8s.io/vsphere-csi-driver/v3/pkg/csi/service/logger"
)

func TestSPIEnqueuePeriodicResyncIncludesQuotas(t *testing.T) {
	for _, tc := range []struct {
		name       string
		failQuotas bool
		want       []apitypes.NamespacedName
	}{
		{name: "only assigned policies preserving namespaces", want: []apitypes.NamespacedName{
			{Namespace: "ns-a", Name: "gold"}, {Namespace: "ns-b", Name: "gold"},
		}},
		{name: "quota list failure enqueues nothing", failQuotas: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx := logger.NewContextWithLogger(context.Background())
			cli := fake.NewClientBuilder().WithScheme(testScheme(t)).WithObjects(
				&spiv1alpha1.StoragePolicyInfo{ObjectMeta: metav1.ObjectMeta{Namespace: "ns-a", Name: "gold"}},
				&spiv1alpha1.StoragePolicyInfo{ObjectMeta: metav1.ObjectMeta{Namespace: "ns-a", Name: "orphan"}},
				testSPQ("ns-a", "gold"), testSPQ("ns-b", "gold"), testSPQ("ns-a", "backed-off"),
				&storagepolicyv1alpha2.StoragePolicyQuota{ObjectMeta: metav1.ObjectMeta{
					Namespace: "ns-a", Name: "invalid-name",
				}},
			).WithInterceptorFuncs(interceptor.Funcs{
				List: func(ctx context.Context, c client.WithWatch, list client.ObjectList,
					opts ...client.ListOption) error {
					switch list.(type) {
					case *spiv1alpha1.StoragePolicyInfoList:
						t.Error("slow sync must not list SPIs")
						return assert.AnError
					case *storagepolicyv1alpha2.StoragePolicyQuotaList:
						if tc.failQuotas {
							return assert.AnError
						}
					}
					return c.List(ctx, list, opts...)
				},
			}).Build()
			r := &ReconcileStoragePolicyInfo{backOffDuration: map[apitypes.NamespacedName]time.Duration{
				{Namespace: "ns-a", Name: "backed-off"}: 2 * time.Second,
			}}
			ch := make(chan event.GenericEvent, 10)
			enqueuePeriodicResync(ctx, cli, ch, r)
			close(ch)
			var got []apitypes.NamespacedName
			for e := range ch {
				got = append(got, apitypes.NamespacedName{Namespace: e.Object.GetNamespace(), Name: e.Object.GetName()})
			}
			assert.ElementsMatch(t, tc.want, got)
			// Queueing an absent SPI must not create it directly.
			err := cli.Get(ctx, apitypes.NamespacedName{Namespace: "ns-b", Name: "gold"},
				&spiv1alpha1.StoragePolicyInfo{})
			require.True(t, apierrors.IsNotFound(err))
		})
	}
}

func TestSPIGetSlowSyncInterval(t *testing.T) {
	ctx, _ := logger.GetNewContextWithLogger()
	tests := []struct {
		name string
		env  string
		want time.Duration
	}{
		{"unset uses default", "", defaultSlowSyncIntervalMin * time.Minute},
		{"valid override", "60", 60 * time.Minute},
		{"valid override with surrounding whitespace", "  60  ", 60 * time.Minute},
		{"zero uses default", "0", defaultSlowSyncIntervalMin * time.Minute},
		{"negative uses default", "-1", defaultSlowSyncIntervalMin * time.Minute},
		{"invalid uses default", "abc", defaultSlowSyncIntervalMin * time.Minute},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Setenv(slowSyncIntervalEnvVar, tt.env)
			assert.Equal(t, tt.want, getSlowSyncInterval(ctx))
		})
	}
}

func TestSPIStartPeriodicResyncEnqueues(t *testing.T) {
	scheme := testScheme(t)
	const n = 3
	objs := make([]client.Object, n)
	for i := 0; i < n; i++ {
		objs[i] = testSPQ("ns-a", fmt.Sprintf("policy-%d", i))
	}
	cli := fake.NewClientBuilder().WithScheme(scheme).WithObjects(objs...).Build()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	r := &ReconcileStoragePolicyInfo{
		backOffDuration: make(map[apitypes.NamespacedName]time.Duration),
	}
	ch := make(chan event.GenericEvent, 256)
	StartPeriodicResync(ctx, cli, ch, 50*time.Millisecond, r)

	got := make(map[string]bool)
	deadline := time.After(5 * time.Second)
	for len(got) < n {
		select {
		case e := <-ch:
			got[e.Object.GetName()] = true
		case <-deadline:
			t.Fatalf("timed out waiting for events: got %d/%d", len(got), n)
		}
	}
	assert.Len(t, got, n)
}

func TestSPIStartPeriodicResyncSkipsBackedOffInstances(t *testing.T) {
	scheme := testScheme(t)
	objs := []client.Object{
		testSPQ("ns-a", "backed-off"),
		testSPQ("ns-a", "eligible"),
	}
	cli := fake.NewClientBuilder().WithScheme(scheme).WithObjects(objs...).Build()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	r := &ReconcileStoragePolicyInfo{
		backOffDuration: map[apitypes.NamespacedName]time.Duration{
			// A backoff greater than one second marks an instance as backed off
			// after a failed reconcile; slow-sync must skip it.
			{Namespace: "ns-a", Name: "backed-off"}: 2 * time.Second,
		},
	}
	ch := make(chan event.GenericEvent, 256)
	StartPeriodicResync(ctx, cli, ch, 50*time.Millisecond, r)

	select {
	case e := <-ch:
		assert.Equal(t, "eligible", e.Object.GetName())
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for eligible instance to be enqueued")
	}

	select {
	case e := <-ch:
		assert.Equal(t, "eligible", e.Object.GetName(),
			"only the eligible instance should be repeatedly enqueued; backed-off instance should be skipped")
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for second tick to enqueue the eligible instance again")
	}
}

func TestSPIStartPeriodicResyncEnqueuesBaselineBackoffInstances(t *testing.T) {
	scheme := testScheme(t)
	objs := []client.Object{
		testSPQ("ns-a", "baseline"),
	}
	cli := fake.NewClientBuilder().WithScheme(scheme).WithObjects(objs...).Build()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	r := &ReconcileStoragePolicyInfo{
		backOffDuration: map[apitypes.NamespacedName]time.Duration{
			// A one-second backoff is the healthy baseline (new instance or last
			// reconcile succeeded); it must NOT be treated as backed off.
			{Namespace: "ns-a", Name: "baseline"}: time.Second,
		},
	}
	ch := make(chan event.GenericEvent, 256)
	StartPeriodicResync(ctx, cli, ch, 50*time.Millisecond, r)

	select {
	case e := <-ch:
		assert.Equal(t, "baseline", e.Object.GetName(),
			"an instance at the one-second baseline backoff should still be enqueued")
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for baseline instance to be enqueued")
	}
}

func TestSPIStartPeriodicResyncSkipsWhenChannelFull(t *testing.T) {
	scheme := testScheme(t)
	objs := []client.Object{
		testSPQ("ns-a", "policy-0"),
		testSPQ("ns-a", "policy-1"),
	}
	cli := fake.NewClientBuilder().WithScheme(scheme).WithObjects(objs...).Build()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	r := &ReconcileStoragePolicyInfo{
		backOffDuration: make(map[apitypes.NamespacedName]time.Duration),
	}
	// Unbuffered and never drained: every send on this tick must hit the
	// default case instead of blocking the goroutine.
	ch := make(chan event.GenericEvent)
	StartPeriodicResync(ctx, cli, ch, 50*time.Millisecond, r)

	// The goroutine must still observe ctx cancellation promptly, proving it
	// never got stuck blocking on the full/unread channel.
	time.Sleep(200 * time.Millisecond)
	cancel()

	select {
	case <-ch:
		t.Fatal("no event should have been received; channel was never drained")
	default:
	}
}

func TestSPIStartPeriodicResyncStopsOnCancel(t *testing.T) {
	scheme := testScheme(t)
	cli := fake.NewClientBuilder().WithScheme(scheme).Build()

	ctx, cancel := context.WithCancel(context.Background())
	r := &ReconcileStoragePolicyInfo{
		backOffDuration: make(map[apitypes.NamespacedName]time.Duration),
	}
	ch := make(chan event.GenericEvent, 256)
	StartPeriodicResync(ctx, cli, ch, time.Hour, r)

	cancel()
	time.Sleep(50 * time.Millisecond)
	select {
	case <-ch:
		t.Fatal("unexpected event received after context cancel")
	default:
	}
}
