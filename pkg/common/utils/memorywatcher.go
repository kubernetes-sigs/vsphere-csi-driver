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

package utils

import (
	"context"
	"encoding/base64"
	"errors"
	"fmt"
	"math"
	"runtime"
	"runtime/debug"
	"runtime/metrics"
	"runtime/pprof"
	"sync"
	"sync/atomic"
	"time"

	"sigs.k8s.io/vsphere-csi-driver/v3/pkg/csi/service/logger"
)

const (
	defaultMemoryThresholdPercent = 95
	defaultMemoryCooldown         = 5 * time.Minute
	liveHeapMetricName            = "/gc/heap/live:bytes"
	logChunkSize                  = 8192
)

var defaultMemoryProfiles = []string{"heap", "goroutine", "allocs"}

// MemoryWatcherConfig configures a MemoryWatcher.
type MemoryWatcherConfig struct {
	// ThresholdPercent is the live Go heap size, as a percentage in (0, 100] of
	// GOMEMLIMIT, at which profiles are written to the log. Defaults to 95.
	// GOMEMLIMIT must be set; Start fails otherwise.
	ThresholdPercent float64
	// Profiles lists the runtime/pprof profile names to write, for example
	// "heap", "goroutine", "allocs". Defaults to heap, goroutine and allocs.
	Profiles []string
	// Cooldown is the minimum time between two dumps. Defaults to 5 minutes.
	Cooldown time.Duration
}

// MemoryWatcher writes pprof profiles to the log when the live Go heap reaches
// a percentage of GOMEMLIMIT. It never polls: it is driven by the garbage
// collector, using a finalizer on a sentinel object that the runtime runs once
// per GC cycle. The live heap size after each cycle is read from runtime/metrics
// and compared with the threshold.
//
// Only Go heap is considered, not total process or cgroup memory. Because the
// check runs right after a GC cycle, the heap profile written at that moment
// reflects the just-completed cycle and no extra GC is forced.
//
// Each profile is logged as a "memory watcher profile begin" line, numbered
// "memory watcher profile chunk" lines carrying base64 text in the "data" field,
// and a "memory watcher profile end" line with the chunk and byte counts. All
// lines carry the "profile" and "dumpId" fields. To recover a profile from JSON
// logs, for example with kubectl logs --previous:
//
//	jq -r 'select(.msg=="memory watcher profile chunk" and .profile=="heap" and
//	  .dumpId=="<id>") | .data' | tr -d '\n' | base64 -d > heap.pb.gz
//	go tool pprof heap.pb.gz
type MemoryWatcher struct {
	cfg       MemoryWatcherConfig
	threshold uint64
	readLive  func() uint64
	dumpFn    func(ctx context.Context, live uint64) error
	stopped   atomic.Bool
	mu        sync.Mutex
	lastDump  time.Time
	dumping   bool
}

// gcSentinel carries a pointer so it is never tiny-allocated, which could leave
// its finalizer unrun.
type gcSentinel struct {
	_ *int
}

// InitMemoryWatcher validates cfg and applies defaults. It does not start
// watching; call Start.
func InitMemoryWatcher(cfg MemoryWatcherConfig) (*MemoryWatcher, error) {
	if cfg.ThresholdPercent == 0 {
		cfg.ThresholdPercent = defaultMemoryThresholdPercent
	}
	if cfg.ThresholdPercent < 0 || cfg.ThresholdPercent > 100 {
		return nil, errors.New("memory watcher: ThresholdPercent must be in (0, 100]")
	}
	if len(cfg.Profiles) == 0 {
		cfg.Profiles = defaultMemoryProfiles
	}
	for _, name := range cfg.Profiles {
		if pprof.Lookup(name) == nil {
			return nil, fmt.Errorf("memory watcher: unknown pprof profile %q", name)
		}
	}
	if cfg.Cooldown <= 0 {
		cfg.Cooldown = defaultMemoryCooldown
	}
	w := &MemoryWatcher{cfg: cfg, readLive: readLiveHeap}
	w.dumpFn = w.dump
	return w, nil
}

// Threshold returns the effective threshold in bytes. It is 0 until Start has
// resolved it.
func (w *MemoryWatcher) Threshold() uint64 {
	return w.threshold
}

// Start resolves the threshold from GOMEMLIMIT and begins watching; it returns
// immediately. Watching stops after ctx is cancelled, at the next GC cycle.
func (w *MemoryWatcher) Start(ctx context.Context) error {
	log := logger.GetLogger(ctx)
	if err := w.resolveThreshold(ctx); err != nil {
		return fmt.Errorf("memory watcher: %w", err)
	}
	w.arm(ctx)
	go func() {
		<-ctx.Done()
		w.stopped.Store(true)
		log.Info("memory watcher stopped")
	}()
	log.With("thresholdBytes", w.threshold, "profiles", w.cfg.Profiles).Info("memory watcher started")
	return nil
}

func (w *MemoryWatcher) resolveThreshold(ctx context.Context) error {
	limit := debug.SetMemoryLimit(-1)
	if limit <= 0 || limit == math.MaxInt64 {
		return errors.New("GOMEMLIMIT is not set, cannot derive threshold")
	}
	w.threshold = uint64(float64(limit) * w.cfg.ThresholdPercent / 100)
	if w.threshold == 0 {
		return errors.New("computed threshold is zero")
	}
	logger.GetLogger(ctx).With("goMemLimitBytes", limit, "thresholdPercent", w.cfg.ThresholdPercent,
		"thresholdBytes", w.threshold).Info("memory watcher threshold resolved from GOMEMLIMIT")
	return nil
}

func (w *MemoryWatcher) arm(ctx context.Context) {
	s := &gcSentinel{}
	runtime.SetFinalizer(s, func(*gcSentinel) {
		if w.stopped.Load() {
			return
		}
		w.onGC(ctx)
		w.arm(ctx)
	})
}

func (w *MemoryWatcher) onGC(ctx context.Context) {
	live := w.readLive()
	if live < w.threshold {
		return
	}
	w.mu.Lock()
	if w.dumping || (!w.lastDump.IsZero() && time.Since(w.lastDump) < w.cfg.Cooldown) {
		w.mu.Unlock()
		return
	}
	w.dumping = true
	w.lastDump = time.Now()
	w.mu.Unlock()
	go w.dumpAndLog(ctx, live)
}

func (w *MemoryWatcher) dumpAndLog(ctx context.Context, live uint64) {
	log := logger.GetLogger(ctx)
	defer func() {
		w.mu.Lock()
		w.dumping = false
		w.mu.Unlock()
	}()
	log.With("liveHeapBytes", live, "thresholdBytes", w.threshold).
		Warn("memory watcher: live heap reached threshold, writing profiles to the log")
	start := time.Now()
	if err := w.dumpFn(ctx, live); err != nil {
		log.With("err", err).Error("memory watcher: writing profiles failed")
		return
	}
	log.With("profiles", w.cfg.Profiles, "duration", time.Since(start)).Info("memory watcher: wrote profiles")
}

func (w *MemoryWatcher) dump(ctx context.Context, live uint64) error {
	stamp := time.Now().UTC().Format("20060102T150405Z")
	var errs []error
	for _, name := range w.cfg.Profiles {
		if err := logProfile(ctx, name, stamp, live); err != nil {
			errs = append(errs, fmt.Errorf("%s: %w", name, err))
		}
	}
	return errors.Join(errs...)
}

// logProfile writes one profile to the log as a begin line, numbered base64
// chunk lines, and an end line carrying the chunk and byte counts.
func logProfile(ctx context.Context, name, stamp string, live uint64) error {
	log := logger.GetLogger(ctx).With("profile", name, "dumpId", stamp, "liveHeapBytes", live)
	log.Info("memory watcher profile begin")
	chunks, bytes, err := streamProfile(name, logChunkSize, func(seq int, data string) {
		log.With("seq", seq, "data", data).Info("memory watcher profile chunk")
	})
	log.With("chunks", chunks, "base64Bytes", bytes).Info("memory watcher profile end")
	return err
}

// streamProfile writes the named profile through a base64 encoder and hands the
// encoded text to emit in pieces of at most chunkSize bytes, in order. Memory
// use is bounded by chunkSize, not by the profile size. Concatenating the
// pieces and base64-decoding them yields the gzip-compressed profile.
func streamProfile(name string, chunkSize int, emit func(seq int, data string)) (chunks, bytes int, err error) {
	c := &chunker{size: chunkSize, buf: make([]byte, 0, chunkSize), emit: emit}
	enc := base64.NewEncoder(base64.StdEncoding, c)
	err = pprof.Lookup(name).WriteTo(enc, 0)
	if cerr := enc.Close(); err == nil {
		err = cerr
	}
	c.flush()
	return c.chunks, c.total, err
}

type chunker struct {
	size   int
	buf    []byte
	emit   func(seq int, data string)
	chunks int
	total  int
}

func (c *chunker) Write(p []byte) (int, error) {
	written := len(p)
	for len(p) > 0 {
		n := c.size - len(c.buf)
		if n > len(p) {
			n = len(p)
		}
		c.buf = append(c.buf, p[:n]...)
		p = p[n:]
		if len(c.buf) == c.size {
			c.flush()
		}
	}
	return written, nil
}

func (c *chunker) flush() {
	if len(c.buf) == 0 {
		return
	}
	c.emit(c.chunks, string(c.buf))
	c.chunks++
	c.total += len(c.buf)
	c.buf = c.buf[:0]
}

func readLiveHeap() uint64 {
	sample := []metrics.Sample{{Name: liveHeapMetricName}}
	metrics.Read(sample)
	if sample[0].Value.Kind() != metrics.KindUint64 {
		return 0
	}
	return sample[0].Value.Uint64()
}

// StartMemoryWatcher starts a MemoryWatcher with the default configuration: it
// writes profiles to the log when the live Go heap reaches 95% of GOMEMLIMIT. It
// never returns an error and never stops the process: problems, such as
// GOMEMLIMIT not being set, are logged and the watcher is not started.
func StartMemoryWatcher(ctx context.Context) {
	log := logger.GetLogger(ctx)
	w, err := InitMemoryWatcher(MemoryWatcherConfig{})
	if err != nil {
		log.With("err", err).Error("memory watcher is not started")
		return
	}
	if err := w.Start(ctx); err != nil {
		log.With("err", err).Error("memory watcher is not started")
	}
}
