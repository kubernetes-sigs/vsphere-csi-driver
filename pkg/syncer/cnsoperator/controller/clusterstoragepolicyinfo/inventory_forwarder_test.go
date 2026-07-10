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
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	apitypes "k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/event"

	"sigs.k8s.io/vsphere-csi-driver/v3/pkg/csi/service/logger"
)

func TestForwardPolicyChanges_RelaysAsGenericEvent(t *testing.T) {
	ctx, cancel := context.WithCancel(logger.NewContextWithLogger(context.Background()))
	defer cancel()

	src := make(chan string, 1)
	dst := make(chan event.GenericEvent, 1)
	go forwardPolicyChanges(ctx, src, dst)

	src <- "policy-1"

	select {
	case e := <-dst:
		assert.Equal(t, "policy-1", e.Object.GetName())
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for forwarded event")
	}
}

func TestForwardPolicyChanges_DropsWhenDstFull(t *testing.T) {
	ctx, cancel := context.WithCancel(logger.NewContextWithLogger(context.Background()))
	defer cancel()

	src := make(chan string, 2)
	dst := make(chan event.GenericEvent) // unbuffered and never read: always full
	go forwardPolicyChanges(ctx, src, dst)

	src <- "policy-1"
	src <- "policy-2"

	// Must not block or panic despite dst never draining.
	time.Sleep(50 * time.Millisecond)

	select {
	case <-dst:
		t.Fatal("expected forwarded events to be dropped, not delivered")
	default:
	}
}

func TestForwardPolicyChanges_SkipsBackedOffPolicy(t *testing.T) {
	ctx, cancel := context.WithCancel(logger.NewContextWithLogger(context.Background()))
	defer cancel()

	backOffDurationMapMutex.Lock()
	backOffDuration = map[apitypes.NamespacedName]time.Duration{
		// A backoff greater than one second marks this policy as already
		// scheduled to reconcile via RequeueAfter after a prior failure; the
		// forwarder must skip it rather than push a redundant immediate reconcile.
		{Name: "backed-off"}: 2 * time.Second,
	}
	backOffDurationMapMutex.Unlock()
	t.Cleanup(func() {
		backOffDurationMapMutex.Lock()
		backOffDuration = nil
		backOffDurationMapMutex.Unlock()
	})

	src := make(chan string, 2)
	dst := make(chan event.GenericEvent, 2)
	go forwardPolicyChanges(ctx, src, dst)

	src <- "backed-off"
	src <- "eligible"

	select {
	case e := <-dst:
		assert.Equal(t, "eligible", e.Object.GetName(), "only the eligible policy should be forwarded")
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for eligible policy to be forwarded")
	}

	select {
	case e := <-dst:
		t.Fatalf("unexpected second forwarded event for %q; backed-off policy should have been skipped", e.Object.GetName())
	case <-time.After(50 * time.Millisecond):
	}
}

func TestForwardPolicyChanges_BaselineBackoffIsNotSkipped(t *testing.T) {
	ctx, cancel := context.WithCancel(logger.NewContextWithLogger(context.Background()))
	defer cancel()

	backOffDurationMapMutex.Lock()
	backOffDuration = map[apitypes.NamespacedName]time.Duration{
		// The one-second baseline (set for new/just-succeeded instances) must
		// not be treated as "backed off" — only values greater than it are.
		{Name: "policy-1"}: time.Second,
	}
	backOffDurationMapMutex.Unlock()
	t.Cleanup(func() {
		backOffDurationMapMutex.Lock()
		backOffDuration = nil
		backOffDurationMapMutex.Unlock()
	})

	src := make(chan string, 1)
	dst := make(chan event.GenericEvent, 1)
	go forwardPolicyChanges(ctx, src, dst)

	src <- "policy-1"

	select {
	case e := <-dst:
		assert.Equal(t, "policy-1", e.Object.GetName())
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for forwarded event")
	}
}

func TestForwardPolicyChanges_SkipsEmptyPolicyName(t *testing.T) {
	ctx, cancel := context.WithCancel(logger.NewContextWithLogger(context.Background()))
	defer cancel()

	src := make(chan string, 2)
	dst := make(chan event.GenericEvent, 2)
	go forwardPolicyChanges(ctx, src, dst)

	src <- ""
	src <- "policy-1"

	select {
	case e := <-dst:
		assert.Equal(t, "policy-1", e.Object.GetName(), "the empty name must be skipped, not forwarded")
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for policy-1 to be forwarded")
	}

	select {
	case e := <-dst:
		t.Fatalf("unexpected second forwarded event for %q", e.Object.GetName())
	case <-time.After(50 * time.Millisecond):
	}
}

func TestForwardPolicyChanges_StopsWhenSrcClosed(t *testing.T) {
	ctx, cancel := context.WithCancel(logger.NewContextWithLogger(context.Background()))
	defer cancel()

	src := make(chan string)
	dst := make(chan event.GenericEvent)
	done := make(chan struct{})
	go func() {
		forwardPolicyChanges(ctx, src, dst)
		close(done)
	}()

	close(src)

	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("forwardPolicyChanges did not stop after src was closed; " +
			"a single-value receive from a closed channel never blocks, so this would spin forever")
	}
}

func TestForwardPolicyChanges_StopsOnCancel(t *testing.T) {
	ctx, cancel := context.WithCancel(logger.NewContextWithLogger(context.Background()))

	src := make(chan string)
	dst := make(chan event.GenericEvent)
	done := make(chan struct{})
	go func() {
		forwardPolicyChanges(ctx, src, dst)
		close(done)
	}()

	cancel()

	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("forwardPolicyChanges did not stop after context cancellation")
	}
}
