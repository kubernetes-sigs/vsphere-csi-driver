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

package clusterstoragepolicyinfo

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	apitypes "k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/event"

	clusterspiv1alpha1 "sigs.k8s.io/vsphere-csi-driver/v3/pkg/apis/cnsoperator/clusterstoragepolicyinfo/v1alpha1"
	"sigs.k8s.io/vsphere-csi-driver/v3/pkg/csi/service/logger"
)

func TestGetSlowSyncInterval(t *testing.T) {
	ctx, _ := logger.GetNewContextWithLogger()
	tests := []struct {
		name string
		env  string
		want time.Duration
	}{
		{"unset uses default", "", defaultSlowSyncIntervalMin * time.Minute},
		{"valid override", "60", 60 * time.Minute},
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

func TestStartPeriodicResyncEnqueues(t *testing.T) {
	scheme := testScheme(t)
	const n = 3
	objs := make([]client.Object, n)
	for i := 0; i < n; i++ {
		objs[i] = &clusterspiv1alpha1.ClusterStoragePolicyInfo{
			ObjectMeta: metav1.ObjectMeta{Name: fmt.Sprintf("policy-%d", i)},
		}
	}
	cli := fake.NewClientBuilder().WithScheme(scheme).WithObjects(objs...).Build()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	ch := make(chan event.GenericEvent, 256)
	StartPeriodicResync(ctx, cli, ch, 50*time.Millisecond)

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

func TestStartPeriodicResyncSkipsBackedOffInstances(t *testing.T) {
	scheme := testScheme(t)
	objs := []client.Object{
		&clusterspiv1alpha1.ClusterStoragePolicyInfo{ObjectMeta: metav1.ObjectMeta{Name: "backed-off"}},
		&clusterspiv1alpha1.ClusterStoragePolicyInfo{ObjectMeta: metav1.ObjectMeta{Name: "eligible"}},
	}
	cli := fake.NewClientBuilder().WithScheme(scheme).WithObjects(objs...).Build()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	backOffDurationMapMutex.Lock()
	backOffDuration = make(map[apitypes.NamespacedName]time.Duration)
	nextEligibleReconcile = map[apitypes.NamespacedName]time.Time{
		{Name: "backed-off"}: time.Now().Add(time.Hour),
	}
	backOffDurationMapMutex.Unlock()
	t.Cleanup(func() {
		backOffDurationMapMutex.Lock()
		backOffDuration = nil
		nextEligibleReconcile = nil
		backOffDurationMapMutex.Unlock()
	})

	ch := make(chan event.GenericEvent, 256)
	StartPeriodicResync(ctx, cli, ch, 50*time.Millisecond)

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

func TestStartPeriodicResyncStopsOnCancel(t *testing.T) {
	scheme := testScheme(t)
	cli := fake.NewClientBuilder().WithScheme(scheme).Build()

	ctx, cancel := context.WithCancel(context.Background())
	ch := make(chan event.GenericEvent, 256)
	StartPeriodicResync(ctx, cli, ch, time.Hour)

	cancel()
	time.Sleep(50 * time.Millisecond)
	select {
	case <-ch:
		t.Fatal("unexpected event received after context cancel")
	default:
	}
}
