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

package memory_watcher

import (
	"bytes"
	"compress/gzip"
	"context"
	"encoding/base64"
	"errors"
	"io"
	"math"
	"runtime"
	"runtime/debug"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func waitFor(t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for %s", what)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func waitIdle(t *testing.T, p *ProfileLogger) {
	t.Helper()
	waitFor(t, "in-flight dump to finish", func() bool {
		p.mu.Lock()
		defer p.mu.Unlock()
		return !p.dumping
	})
}

func setGoMemLimit(t *testing.T, limit int64) {
	t.Helper()
	prev := debug.SetMemoryLimit(limit)
	t.Cleanup(func() { debug.SetMemoryLimit(prev) })
}

type fakeDumps struct {
	count atomic.Int64
	lives []uint64
	mu    sync.Mutex
	err   error
}

func (f *fakeDumps) dump(_ context.Context, live uint64) error {
	f.count.Add(1)
	f.mu.Lock()
	f.lives = append(f.lives, live)
	f.mu.Unlock()
	return f.err
}

func newTestWatcher(t *testing.T, cooldown time.Duration, live *atomic.Uint64) (*ProfileLogger, *fakeDumps) {
	t.Helper()
	setGoMemLimit(t, 1<<30)
	p, err := newProfileLogger(Config{Cooldown: cooldown})
	if err != nil {
		t.Fatal(err)
	}
	f := &fakeDumps{}
	p.threshold = 100
	p.elevatedBytes = 80
	p.readLive = func() uint64 { return live.Load() }
	p.dumpFn = f.dump
	t.Cleanup(func() { waitIdle(t, p) })
	return p, f
}

func TestNewProfileLoggerValidation(t *testing.T) {
	setGoMemLimit(t, 1<<30)
	tests := []struct {
		name    string
		cfg     Config
		wantErr bool
	}{
		{"defaults ok", Config{}, false},
		{"percent ok", Config{ThresholdPercent: 50}, false},
		{"percent 100 ok", Config{ThresholdPercent: 100}, false},
		{"percent too high", Config{ThresholdPercent: 101}, true},
		{"percent negative", Config{ThresholdPercent: -1}, true},
		{"unknown profile", Config{Profiles: []string{"nope"}}, true},
		{"known profiles", Config{Profiles: []string{"heap", "goroutine", "mutex"}}, false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := newProfileLogger(tt.cfg)
			if (err != nil) != tt.wantErr {
				t.Fatalf("err = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestDefaults(t *testing.T) {
	setGoMemLimit(t, 1<<30)
	p, err := newProfileLogger(Config{})
	if err != nil {
		t.Fatal(err)
	}
	if p.cfg.ThresholdPercent != 95 || p.cfg.Cooldown != 5*time.Minute ||
		strings.Join(p.cfg.Profiles, ",") != "heap,goroutine,allocs" {
		t.Fatalf("unexpected defaults: %+v", p.cfg)
	}
}

func TestThresholdFromGoMemLimit(t *testing.T) {
	tests := []struct {
		name    string
		goLimit int64
		percent int
		want    uint64
		wantErr bool
	}{
		{"default 95 percent", 1000, 0, 950, false},
		{"custom percent", 4000, 50, 2000, false},
		{"100 percent", 1000, 100, 1000, false},
		{"GOMEMLIMIT unset", math.MaxInt64, 0, 0, true},
		{"GOMEMLIMIT too small", 1, 50, 0, true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			setGoMemLimit(t, tt.goLimit)
			p, err := newProfileLogger(Config{ThresholdPercent: tt.percent})
			if (err != nil) != tt.wantErr {
				t.Fatalf("err = %v, wantErr %v", err, tt.wantErr)
			}
			if err == nil && p.getThreshold() != tt.want {
				t.Fatalf("threshold = %d, want %d", p.getThreshold(), tt.want)
			}
		})
	}
}

func TestOnGCThresholdAndCooldown(t *testing.T) {
	var live atomic.Uint64
	p, f := newTestWatcher(t, time.Hour, &live)
	ctx := context.Background()

	live.Store(99)
	p.onGC(ctx)
	time.Sleep(100 * time.Millisecond)
	if n := f.count.Load(); n != 0 {
		t.Fatalf("dumped below threshold: %d", n)
	}

	live.Store(150)
	p.onGC(ctx)
	waitFor(t, "first dump", func() bool { return f.count.Load() == 1 })
	waitIdle(t, p)

	p.onGC(ctx)
	time.Sleep(100 * time.Millisecond)
	if n := f.count.Load(); n != 1 {
		t.Fatalf("cooldown not honored: %d dumps", n)
	}

	p.mu.Lock()
	p.lastDump = time.Now().Add(-2 * time.Hour)
	p.mu.Unlock()
	p.onGC(ctx)
	waitFor(t, "dump after cooldown", func() bool { return f.count.Load() == 2 })

	f.mu.Lock()
	defer f.mu.Unlock()
	if len(f.lives) != 2 || f.lives[0] != 150 || f.lives[1] != 150 {
		t.Fatalf("dump received wrong live heap readings: %v", f.lives)
	}
}

func TestOnGCExactThresholdBoundary(t *testing.T) {
	var live atomic.Uint64
	p, f := newTestWatcher(t, time.Nanosecond, &live)

	live.Store(p.threshold - 1)
	p.onGC(context.Background())
	time.Sleep(100 * time.Millisecond)
	if n := f.count.Load(); n != 0 {
		t.Fatalf("dumped one byte below threshold: %d", n)
	}

	live.Store(p.threshold)
	p.onGC(context.Background())
	waitFor(t, "dump at exactly the threshold", func() bool { return f.count.Load() == 1 })
}

func TestOnGCSkipsWhileDumping(t *testing.T) {
	var live atomic.Uint64
	live.Store(150)
	p, f := newTestWatcher(t, time.Nanosecond, &live)
	p.mu.Lock()
	p.dumping = true
	p.mu.Unlock()

	p.onGC(context.Background())
	time.Sleep(100 * time.Millisecond)
	if n := f.count.Load(); n != 0 {
		t.Fatalf("dumped while another dump was in progress: %d", n)
	}

	p.mu.Lock()
	p.dumping = false
	p.mu.Unlock()
}

func TestOnGCConcurrentCallsDumpOnce(t *testing.T) {
	var live atomic.Uint64
	live.Store(1000)
	p, f := newTestWatcher(t, time.Hour, &live)

	var wg sync.WaitGroup
	for i := 0; i < 64; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			p.onGC(context.Background())
		}()
	}
	wg.Wait()
	waitFor(t, "dump", func() bool { return f.count.Load() >= 1 })
	time.Sleep(300 * time.Millisecond)
	if n := f.count.Load(); n != 1 {
		t.Fatalf("64 concurrent GC callbacks produced %d dumps, want 1", n)
	}
}

func TestMetricUnavailableNeverDumps(t *testing.T) {
	var live atomic.Uint64
	p, f := newTestWatcher(t, time.Nanosecond, &live)
	for i := 0; i < 5; i++ {
		p.onGC(context.Background())
	}
	time.Sleep(100 * time.Millisecond)
	if n := f.count.Load(); n != 0 {
		t.Fatalf("dumped with a zero live-heap reading: %d", n)
	}
}

func TestFailedDumpReleasesFlagAndRetriesAfterCooldown(t *testing.T) {
	var live atomic.Uint64
	live.Store(1000)
	p, f := newTestWatcher(t, time.Nanosecond, &live)
	f.err = errors.New("boom")

	p.onGC(context.Background())
	waitFor(t, "failed dump", func() bool { return f.count.Load() == 1 })
	waitIdle(t, p)

	f.mu.Lock()
	f.err = nil
	f.mu.Unlock()
	time.Sleep(10 * time.Millisecond)
	p.onGC(context.Background())
	waitFor(t, "dump after the failure", func() bool { return f.count.Load() == 2 })
}

func TestSustainedLeakDumpsOncePerCooldown(t *testing.T) {
	var live atomic.Uint64
	live.Store(1000)
	p, f := newTestWatcher(t, 300*time.Millisecond, &live)

	deadline := time.Now().Add(1100 * time.Millisecond)
	for time.Now().Before(deadline) {
		p.onGC(context.Background())
		time.Sleep(5 * time.Millisecond)
	}
	waitIdle(t, p)
	if n := f.count.Load(); n < 3 || n > 4 {
		t.Fatalf("got %d dumps over ~1.1s with a 300ms cooldown, want 3 or 4", n)
	}
}

func TestElevatedHeapTracedOncePerEpisode(t *testing.T) {
	var live atomic.Uint64
	p, _ := newTestWatcher(t, time.Hour, &live)
	ctx := context.Background()
	state := func() (traced bool, since time.Time) {
		p.mu.Lock()
		defer p.mu.Unlock()
		return p.elevatedTraced, p.elevatedSince
	}
	backdate := func() {
		p.mu.Lock()
		p.elevatedSince = time.Now().Add(-2 * elevatedHeapWindow)
		p.mu.Unlock()
	}

	p.traceElevated(ctx, 80)
	if traced, since := state(); traced || since.IsZero() {
		t.Fatalf("episode not started: traced=%v since=%v", traced, since)
	}

	p.traceElevated(ctx, 80)
	if traced, _ := state(); traced {
		t.Fatal("elevated heap traced before the window elapsed")
	}

	backdate()
	p.traceElevated(ctx, 80)
	if traced, _ := state(); !traced {
		t.Fatal("elevated heap not traced after the window elapsed")
	}

	backdate()
	p.traceElevated(ctx, 80)
	if traced, _ := state(); !traced {
		t.Fatal("trace state lost within the same episode")
	}

	p.traceElevated(ctx, 10)
	if traced, since := state(); traced || !since.IsZero() {
		t.Fatalf("episode not ended by a GC below the level: traced=%v since=%v", traced, since)
	}
}

func TestGetLiveHeapSize(t *testing.T) {
	sink := make([][]byte, 0, 64)
	for i := 0; i < 64; i++ {
		sink = append(sink, make([]byte, 1<<20))
	}
	runtime.GC()
	if live := getLiveHeapSize(); live < 32<<20 {
		t.Fatalf("live heap = %d, expected at least 32MiB while %d MiB is referenced", live, len(sink))
	}
	runtime.KeepAlive(sink)
}

func TestStartTriggersOnRealGCAndStopsOnCancel(t *testing.T) {
	limit := int64(64 << 20)
	setGoMemLimit(t, limit)
	p, err := newProfileLogger(Config{Cooldown: time.Nanosecond})
	if err != nil {
		t.Fatal(err)
	}
	f := &fakeDumps{}
	p.dumpFn = f.dump
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(func() { waitIdle(t, p) })
	p.start(ctx)
	if want := uint64(limit) / 100 * 95; p.getThreshold() != want {
		t.Fatalf("threshold = %d, want 95%% of 64MiB", p.getThreshold())
	}

	runtime.GC()
	runtime.GC()
	time.Sleep(100 * time.Millisecond)
	if n := f.count.Load(); n != 0 {
		t.Fatalf("dumped before heap reached threshold: %d", n)
	}

	sink := make([]byte, 80<<20)
	for i := 0; i < len(sink); i += 4096 {
		sink[i] = 1
	}
	waitFor(t, "dump after heap crossed threshold", func() bool {
		runtime.GC()
		return f.count.Load() > 0
	})
	runtime.KeepAlive(sink)

	cancel()
	time.Sleep(200 * time.Millisecond)
	runtime.GC()
	time.Sleep(100 * time.Millisecond)
	runtime.GC()
	time.Sleep(300 * time.Millisecond)
	before := f.count.Load()
	for i := 0; i < 3; i++ {
		runtime.GC()
		time.Sleep(100 * time.Millisecond)
	}
	if after := f.count.Load(); after != before {
		t.Fatalf("watcher kept dumping after cancel: %d -> %d", before, after)
	}
	runtime.KeepAlive(sink)
}

func TestInitMemoryWatcherFailsWithoutGOMEMLIMIT(t *testing.T) {
	setGoMemLimit(t, math.MaxInt64)
	if err := InitMemoryWatcher(context.Background(), Config{}); err == nil {
		t.Fatal("expected InitMemoryWatcher to fail when GOMEMLIMIT is not set")
	}
}

func TestStartWithAlreadyCancelledContext(t *testing.T) {
	setGoMemLimit(t, 64<<20)
	p, err := newProfileLogger(Config{Cooldown: time.Nanosecond})
	if err != nil {
		t.Fatal(err)
	}
	f := &fakeDumps{}
	p.readLive = func() uint64 { return 1 << 40 }
	p.dumpFn = f.dump
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	t.Cleanup(func() { waitIdle(t, p) })
	p.start(ctx)
	time.Sleep(100 * time.Millisecond)
	for i := 0; i < 5; i++ {
		runtime.GC()
		time.Sleep(50 * time.Millisecond)
	}
	if n := f.count.Load(); n > 1 {
		t.Fatalf("a cancelled watcher kept dumping: %d dumps", n)
	}
}

func TestFinalizerChainSurvivesManyGCCycles(t *testing.T) {
	setGoMemLimit(t, 1<<30)
	p, err := newProfileLogger(Config{})
	if err != nil {
		t.Fatal(err)
	}
	var calls atomic.Int64
	p.readLive = func() uint64 {
		calls.Add(1)
		return 0
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	p.start(ctx)
	for i := 0; i < 30; i++ {
		runtime.GC()
		time.Sleep(5 * time.Millisecond)
	}
	if got := calls.Load(); got < 20 {
		t.Fatalf("watcher saw %d of 30 GC cycles; the finalizer chain broke", got)
	}
}

func TestTwoWatchersAreIndependent(t *testing.T) {
	setGoMemLimit(t, 1<<30)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	var seen [2]atomic.Int64
	for i := range seen {
		p, err := newProfileLogger(Config{})
		if err != nil {
			t.Fatal(err)
		}
		i := i
		p.readLive = func() uint64 {
			seen[i].Add(1)
			return 0
		}
		p.start(ctx)
	}
	for i := 0; i < 10; i++ {
		runtime.GC()
		time.Sleep(10 * time.Millisecond)
	}
	for i := range seen {
		if seen[i].Load() < 5 {
			t.Fatalf("watcher %d saw only %d GC cycles", i, seen[i].Load())
		}
	}
}

func TestDumpWritesEveryConfiguredProfileToTheLog(t *testing.T) {
	setGoMemLimit(t, 1<<30)
	p, err := newProfileLogger(Config{Profiles: []string{"goroutine", "heap", "goroutine"}})
	if err != nil {
		t.Fatal(err)
	}
	if err := p.dump(context.Background(), 123); err != nil {
		t.Fatalf("dump failed: %v", err)
	}
}

func TestStreamProfileRoundTrip(t *testing.T) {
	for _, name := range []string{"goroutine", "heap", "allocs"} {
		for _, chunkSize := range []int{1, 7, 64, logChunkSize, 1 << 20} {
			var got strings.Builder
			next := 0
			chunks, total, err := streamProfile(name, chunkSize, func(seq int, data string) {
				if seq != next {
					t.Fatalf("chunk out of order: got %d want %d", seq, next)
				}
				next++
				if len(data) == 0 || len(data) > chunkSize {
					t.Fatalf("chunk %d has size %d, want 1..%d", seq, len(data), chunkSize)
				}
				got.WriteString(data)
			})
			if err != nil {
				t.Fatal(err)
			}
			if chunks != next || total != got.Len() || chunks == 0 {
				t.Fatalf("%s/%d: chunks=%d next=%d total=%d len=%d", name, chunkSize, chunks, next, total, got.Len())
			}
			raw, err := base64.StdEncoding.DecodeString(got.String())
			if err != nil {
				t.Fatalf("%s/%d: invalid base64: %v", name, chunkSize, err)
			}
			zr, err := gzip.NewReader(bytes.NewReader(raw))
			if err != nil {
				t.Fatalf("%s/%d: not gzip: %v", name, chunkSize, err)
			}
			plain, err := io.ReadAll(zr)
			if err != nil || len(plain) == 0 {
				t.Fatalf("%s/%d: bad profile payload: len=%d err=%v", name, chunkSize, len(plain), err)
			}
		}
	}
}

func TestChunkerBoundaries(t *testing.T) {
	for _, size := range []int{1, 3, 8} {
		for _, n := range []int{0, 1, size - 1, size, size + 1, size * 4, size*4 + 1} {
			payload := bytes.Repeat([]byte("x"), n)
			var parts []string
			c := &chunker{size: size, buf: make([]byte, 0, size), emit: func(_ int, d string) { parts = append(parts, d) }}
			if _, err := c.Write(payload); err != nil {
				t.Fatal(err)
			}
			c.flush()
			if strings.Join(parts, "") != string(payload) {
				t.Fatalf("size=%d n=%d: reassembly mismatch", size, n)
			}
			for _, p := range parts {
				if len(p) > size {
					t.Fatalf("size=%d n=%d: oversize chunk %d", size, n, len(p))
				}
			}
			if wantChunks := (n + size - 1) / size; len(parts) != wantChunks {
				t.Fatalf("size=%d n=%d: %d chunks, want %d", size, n, len(parts), wantChunks)
			}
		}
	}
}
