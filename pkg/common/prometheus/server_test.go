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

package prometheus

import (
	"context"
	"errors"
	_ "expvar" // registers /debug/vars on http.DefaultServeMux, as the binaries do
	"net"
	"net/http"
	"net/http/httptest"
	_ "net/http/pprof" // registers /debug/pprof/* on http.DefaultServeMux, as the binaries do
	"slices"
	"testing"
	"time"
)

// debugPaths are served on http.DefaultServeMux in the binaries as a side effect
// of imports: net/http/pprof, expvar, and golang.org/x/net/trace (linked in via
// gRPC). None of them may be reachable on the metrics port.
var debugPaths = []string{
	"/debug/pprof/",
	"/debug/pprof/heap",
	"/debug/pprof/profile",
	"/debug/vars",
	"/debug/requests",
	"/debug/events",
}

func init() {
	// golang.org/x/net/trace is only an indirect dependency, so register
	// stand-ins for its handlers rather than importing it here.
	stub := http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusOK) })
	http.Handle("/debug/requests", stub)
	http.Handle("/debug/events", stub)
}

func TestNewMetricsServer(t *testing.T) {
	srv := NewMetricsServer(":2112")

	if srv.Addr != ":2112" {
		t.Errorf("Addr = %q, want %q", srv.Addr, ":2112")
	}
	if srv.Handler == nil || srv.Handler == http.DefaultServeMux {
		t.Fatalf("metrics server must use a private ServeMux")
	}
	if srv.ReadHeaderTimeout == 0 || srv.ReadTimeout == 0 || srv.WriteTimeout == 0 || srv.IdleTimeout == 0 {
		t.Errorf("all timeouts must be set, got ReadHeader=%v Read=%v Write=%v Idle=%v",
			srv.ReadHeaderTimeout, srv.ReadTimeout, srv.WriteTimeout, srv.IdleTimeout)
	}

	if metricsGatherTimeout >= srv.WriteTimeout {
		t.Errorf("gather timeout %v must be shorter than WriteTimeout %v", metricsGatherTimeout, srv.WriteTimeout)
	}

	rec := httptest.NewRecorder()
	srv.Handler.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, MetricsPath, nil))
	if rec.Code != http.StatusOK {
		t.Errorf("GET %s: status = %d, want %d", MetricsPath, rec.Code, http.StatusOK)
	}

	for _, path := range debugPaths {
		// Make sure the path really is registered on the default mux, so a 404
		// from the metrics server is meaningful.
		if _, pattern := http.DefaultServeMux.Handler(httptest.NewRequest(http.MethodGet, path, nil)); pattern == "" {
			t.Fatalf("%s is not registered on http.DefaultServeMux; test setup is wrong", path)
		}
		rec := httptest.NewRecorder()
		srv.Handler.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, path, nil))
		if rec.Code != http.StatusNotFound {
			t.Errorf("GET %s: status = %d, want %d", path, rec.Code, http.StatusNotFound)
		}
	}
}

func TestNewMetricsServerCanBeCalledRepeatedly(t *testing.T) {
	// The metrics goroutines rebuild the server on each restart; this must not
	// panic with a duplicate-registration error.
	NewMetricsServer(":2112")
	NewMetricsServer(":2112")
}

func TestRunWithBackoffGrowsAndCaps(t *testing.T) {
	ctx := context.Background()
	calls := 0
	serve := func(context.Context) error {
		calls++
		return errors.New("bind: address already in use")
	}
	var waits []time.Duration
	wait := func(_ context.Context, d time.Duration) bool {
		waits = append(waits, d)
		return len(waits) < 6
	}

	runWithBackoff(ctx, serve, time.Second, 8*time.Second, wait)

	want := []time.Duration{1 * time.Second, 2 * time.Second, 4 * time.Second,
		8 * time.Second, 8 * time.Second, 8 * time.Second}
	if !slices.Equal(waits, want) {
		t.Errorf("waits = %v, want %v", waits, want)
	}
	if calls != len(want) {
		t.Errorf("serve called %d times, want %d", calls, len(want))
	}
}

func TestRunWithBackoffResetsAfterHealthyRun(t *testing.T) {
	ctx := context.Background()
	const maxBackoff = 20 * time.Millisecond
	calls := 0
	serve := func(context.Context) error {
		calls++
		if calls == 3 {
			// Simulate a server that stayed up longer than maxBackoff before exiting.
			time.Sleep(maxBackoff)
		}
		return errors.New("server exited")
	}
	var waits []time.Duration
	wait := func(_ context.Context, d time.Duration) bool {
		waits = append(waits, d)
		return len(waits) < 4
	}

	runWithBackoff(ctx, serve, time.Millisecond, maxBackoff, wait)

	want := []time.Duration{1 * time.Millisecond, 2 * time.Millisecond, 1 * time.Millisecond, 2 * time.Millisecond}
	if !slices.Equal(waits, want) {
		t.Errorf("waits = %v, want %v", waits, want)
	}
}

func TestRunMetricsServerReturnsWhenContextDone(t *testing.T) {
	// Occupy a port so ListenAndServe fails immediately, as when the metrics
	// port is already in use.
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("failed to listen: %v", err)
	}
	defer ln.Close()

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		RunMetricsServer(ctx, ln.Addr().String())
		close(done)
	}()
	cancel()

	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("RunMetricsServer did not return after context was cancelled")
	}
}

func TestRunMetricsServerShutsDownOnCancel(t *testing.T) {
	// Reserve a free port, then release it for the metrics server to use.
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("failed to listen: %v", err)
	}
	addr := ln.Addr().String()
	ln.Close()

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		RunMetricsServer(ctx, addr)
		close(done)
	}()

	url := "http://" + addr + MetricsPath
	deadline := time.Now().Add(5 * time.Second)
	for {
		resp, err := http.Get(url)
		if err == nil {
			resp.Body.Close()
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("metrics server did not come up on %s: %v", addr, err)
		}
		time.Sleep(10 * time.Millisecond)
	}

	cancel()
	select {
	case <-done:
	case <-time.After(10 * time.Second):
		t.Fatal("RunMetricsServer did not return after context was cancelled")
	}
	if conn, err := net.DialTimeout("tcp", addr, time.Second); err == nil {
		conn.Close()
		t.Errorf("metrics server is still listening on %s after cancel", addr)
	}
}
