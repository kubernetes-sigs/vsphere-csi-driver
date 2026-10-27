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
	defaultThresholdPercent = 95
	defaultCooldown         = 5 * time.Minute
	liveHeapMetricName      = "/gc/heap/live:bytes"
	logChunkSize            = 8192
	// elevatedHeapPercent is the percent of GOMEMLIMIT that marks an elevated heap.
	elevatedHeapPercent = 80
	// elevatedHeapWindow is how long the heap must stay elevated before tracing.
	elevatedHeapWindow = time.Minute
)

// Config configures a ProfileLogger.
type Config struct {
	// ThresholdPercent is the whole percentage in (0, 100] of GOMEMLIMIT that the
	// live Go heap must reach before profiles are written to the log. Defaults to 95.
	ThresholdPercent int
	// Profiles lists the runtime/pprof profile names to write, for example
	// "heap", "goroutine", "allocs". Defaults to heap, goroutine and allocs.
	Profiles []string
	// Cooldown is the minimum time between two dumps. Defaults to 5 minutes.
	Cooldown time.Duration
}

// ProfileLogger writes pprof profiles to the log when the live Go heap reaches
// a percentage of GOMEMLIMIT. It is driven by the garbage collector, using a
// finalizer on a sentinel object that the runtime runs once per GC cycle. The
// live heap size after each cycle is read from runtime/metrics and compared
// with the threshold.
type ProfileLogger struct {
	cfg            Config
	threshold      uint64
	elevatedBytes  uint64
	readLive       func() uint64
	dumpFn         func(ctx context.Context, live uint64) error
	stopped        atomic.Bool
	mu             sync.Mutex
	lastDump       time.Time
	dumping        bool
	elevatedSince  time.Time
	elevatedTraced bool
}

// newProfileLogger validates cfg, applies defaults and derives the dump
// threshold from GOMEMLIMIT.
func newProfileLogger(cfg Config) (*ProfileLogger, error) {
	// Only heap, goroutine and allocs are logged by default. They are cheap to
	// take at any time and show what retains memory, which goroutines are
	// blocked and what is allocating. Other profiles, such as block and mutex,
	// need runtime sampling enabled or add noticeable cost, so they are opt-in
	// through Config.Profiles.
	defaultProfiles := []string{"heap", "goroutine", "allocs"}

	if cfg.ThresholdPercent == 0 {
		cfg.ThresholdPercent = defaultThresholdPercent
	}
	if cfg.ThresholdPercent < 0 || cfg.ThresholdPercent > 100 {
		return nil, errors.New("memory watcher: ThresholdPercent must be in (0, 100]")
	}
	if len(cfg.Profiles) == 0 {
		cfg.Profiles = defaultProfiles
	}
	for _, name := range cfg.Profiles {
		if pprof.Lookup(name) == nil {
			return nil, fmt.Errorf("memory watcher: unknown pprof profile %q", name)
		}
	}
	if cfg.Cooldown <= 0 {
		cfg.Cooldown = defaultCooldown
	}
	p := &ProfileLogger{cfg: cfg, readLive: getLiveHeapSize}
	if !p.setThreshold() {
		return nil, errors.New("memory watcher: GOMEMLIMIT is not set or too small to derive a threshold")
	}
	p.dumpFn = p.dump
	return p, nil
}

// GetThreshold returns the dump threshold in bytes.
func (p *ProfileLogger) getThreshold() uint64 {
	return p.threshold
}

// InitMemoryWatcher validates cfg, resolves the dump threshold from GOMEMLIMIT and begins
// watching; it returns immediately. It returns an error, and starts nothing, if
// cfg is invalid or GOMEMLIMIT is not set. Watching stops after ctx is cancelled,
// at the next GC cycle.
func InitMemoryWatcher(ctx context.Context, cfg Config) error {
	p, err := newProfileLogger(cfg)
	if err != nil {
		return err
	}
	p.start(ctx)
	return nil
}

func (p *ProfileLogger) start(ctx context.Context) {
	log := logger.GetLogger(ctx)
	p.arm(ctx)
	go func() {
		<-ctx.Done()
		p.stopped.Store(true)
		log.Info("memory watcher stopped")
	}()
	log.With("thresholdBytes", p.threshold, "profiles", p.cfg.Profiles).Info("memory watcher started")
}

// setThreshold derives the dump threshold and the elevated-heap level from
// GOMEMLIMIT. It reports false when GOMEMLIMIT is unset or too small to yield a
// non-zero threshold.
func (p *ProfileLogger) setThreshold() bool {
	limit := debug.SetMemoryLimit(-1)
	if limit <= 0 || limit == math.MaxInt64 {
		return false
	}
	perPercent := uint64(limit) / 100
	p.threshold = perPercent * uint64(p.cfg.ThresholdPercent)
	p.elevatedBytes = perPercent * elevatedHeapPercent
	return p.threshold != 0
}

// gcSentinel carries a pointer so it is never tiny-allocated, which could leave
// its finalizer unrun.
type gcSentinel struct {
	_ *int
}

// arm registers a finalizer on a new gcSentinel. The runtime runs it after the
// GC cycle that finds the sentinel unreachable, and the finalizer then calls
// onGC and arms the next sentinel. The chain ends once the watcher is stopped.
func (p *ProfileLogger) arm(ctx context.Context) {
	s := &gcSentinel{}
	runtime.SetFinalizer(s, func(*gcSentinel) {
		if p.stopped.Load() {
			return
		}
		p.onGC(ctx)
		p.arm(ctx)
	})
}

// onGC reads the live heap after a GC cycle, traces an elevated heap and starts
// a dump when the live heap reaches the threshold and no dump is running or
// within the cooldown.
func (p *ProfileLogger) onGC(ctx context.Context) {
	live := p.readLive()
	p.traceElevated(ctx, live)
	if live < p.threshold {
		return
	}
	p.mu.Lock()
	if p.dumping || (!p.lastDump.IsZero() && time.Since(p.lastDump) < p.cfg.Cooldown) {
		p.mu.Unlock()
		return
	}
	p.dumping = true
	p.lastDump = time.Now()
	p.mu.Unlock()
	go p.dumpAndLog(ctx, live)
}

// traceElevated writes one warning per episode when the live heap has stayed at
// or above elevatedHeapPercent of GOMEMLIMIT for elevatedHeapWindow. An episode
// ends at the first GC cycle that finds the heap below that level.
func (p *ProfileLogger) traceElevated(ctx context.Context, live uint64) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if live < p.elevatedBytes {
		p.elevatedSince = time.Time{}
		p.elevatedTraced = false
		return
	}
	if p.elevatedSince.IsZero() {
		p.elevatedSince = time.Now()
		return
	}
	if p.elevatedTraced || time.Since(p.elevatedSince) < elevatedHeapWindow {
		return
	}
	p.elevatedTraced = true
	logger.GetLogger(ctx).With("liveHeapBytes", live, "elevatedBytes", p.elevatedBytes,
		"elevatedFor", time.Since(p.elevatedSince)).
		Warn("memory watcher: live heap did not drop below the elevated level after GC")
}

func (p *ProfileLogger) dumpAndLog(ctx context.Context, live uint64) {
	log := logger.GetLogger(ctx)
	defer func() {
		p.mu.Lock()
		p.dumping = false
		p.mu.Unlock()
	}()
	log.With("liveHeapBytes", live, "thresholdBytes", p.threshold).
		Warn("memory watcher: live heap reached threshold, writing profiles to the log")
	start := time.Now()
	if err := p.dumpFn(ctx, live); err != nil {
		log.With("err", err).Error("memory watcher: writing profiles failed")
		return
	}
	log.With("profiles", p.cfg.Profiles, "duration", time.Since(start)).Info("memory watcher: wrote profiles")
}

func (p *ProfileLogger) dump(ctx context.Context, live uint64) error {
	stamp := time.Now().UTC().Format("20060102T150405Z")
	var errs []error
	for _, name := range p.cfg.Profiles {
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

func getLiveHeapSize() uint64 {
	sample := []metrics.Sample{{Name: liveHeapMetricName}}
	metrics.Read(sample)
	if sample[0].Value.Kind() != metrics.KindUint64 {
		return 0
	}
	return sample[0].Value.Uint64()
}
