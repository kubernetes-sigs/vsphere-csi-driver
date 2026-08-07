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

package util

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"sigs.k8s.io/vsphere-csi-driver/v3/pkg/csi/service/logger"
)

func TestGetSlowSyncInterval(t *testing.T) {
	ctx, _ := logger.GetNewContextWithLogger()
	const (
		envVar     = "TEST_SLOW_SYNC_INTERVAL_MINUTES"
		defaultMin = 60
	)
	tests := []struct {
		name string
		env  string
		want time.Duration
	}{
		{"unset uses default", "", defaultMin * time.Minute},
		{"valid override", "30", 30 * time.Minute},
		{"valid override with surrounding whitespace", "  30  ", 30 * time.Minute},
		{"zero uses default", "0", defaultMin * time.Minute},
		{"negative uses default", "-1", defaultMin * time.Minute},
		{"invalid uses default", "abc", defaultMin * time.Minute},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Setenv(envVar, tt.env)
			assert.Equal(t, tt.want,
				GetSlowSyncInterval(ctx, "Test", envVar, defaultMin))
		})
	}
}

// TestRunPeriodicResyncDoesNotFireImmediately verifies the first sweep waits out an
// interval instead of running as soon as RunPeriodicResync is called, and that
// cancelling during that initial wait still returns.
func TestRunPeriodicResyncDoesNotFireImmediately(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())

	fired := make(chan struct{}, 1)
	done := make(chan struct{})
	go func() {
		RunPeriodicResync(ctx, "Test", time.Hour, 60, func(context.Context) {
			select {
			case fired <- struct{}{}:
			default:
			}
		})
		close(done)
	}()

	select {
	case <-fired:
		t.Fatal("sweep fired immediately; the first sweep should wait out the interval")
	case <-time.After(200 * time.Millisecond):
	}

	// Cancelling mid-wait must return rather than block for the full hour.
	cancel()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("RunPeriodicResync did not return after context cancel during the initial wait")
	}
}

// TestRunPeriodicResyncFiresAndStops verifies the sweep runs once the interval has
// elapsed, repeats, and that the call returns promptly after the context is
// cancelled.
func TestRunPeriodicResyncFiresAndStops(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())

	fired := make(chan struct{}, 2)
	done := make(chan struct{})
	go func() {
		RunPeriodicResync(ctx, "Test", 50*time.Millisecond, 60, func(context.Context) {
			select {
			case fired <- struct{}{}:
			default:
			}
		})
		close(done)
	}()

	// Two sweeps: the one the initial wait timed, plus a subsequent periodic one.
	for i := range 2 {
		select {
		case <-fired:
		case <-time.After(5 * time.Second):
			t.Fatalf("timed out waiting for sweep %d", i+1)
		}
	}

	cancel()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("RunPeriodicResync did not return after context cancel")
	}
}

// TestRunPeriodicResyncNonPositiveInterval verifies a non-positive interval falls
// back to defaultMin instead of spinning, checked by asserting no sweep happens in a
// short window since the fallback is minute-scale.
func TestRunPeriodicResyncNonPositiveInterval(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())

	fired := make(chan struct{}, 1)
	done := make(chan struct{})
	go func() {
		RunPeriodicResync(ctx, "Test", 0, 1, func(context.Context) {
			select {
			case fired <- struct{}{}:
			default:
			}
		})
		close(done)
	}()

	select {
	case <-fired:
		t.Fatal("sweep fired inside the short window; a non-positive interval should " +
			"fall back to the minute-scale default, not spin")
	case <-time.After(200 * time.Millisecond):
	}

	cancel()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("RunPeriodicResync did not return after context cancel")
	}
}
