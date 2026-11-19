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
	"net/http"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"

	"sigs.k8s.io/vsphere-csi-driver/v3/pkg/csi/service/logger"
)

const (
	// MetricsPath is the HTTP path on which Prometheus metrics are served.
	MetricsPath = "/metrics"

	metricsReadHeaderTimeout = 10 * time.Second
	metricsReadTimeout       = 30 * time.Second
	metricsWriteTimeout      = 30 * time.Second
	metricsIdleTimeout       = 120 * time.Second
	// metricsGatherTimeout bounds how long a scrape may spend gathering metrics.
	// It is shorter than metricsWriteTimeout so that a slow scrape gets a clean
	// 503 instead of a connection reset when the write deadline passes.
	metricsGatherTimeout = 20 * time.Second
	// metricsShutdownTimeout bounds graceful shutdown when ctx is cancelled.
	metricsShutdownTimeout = 5 * time.Second

	// metricsRestartInitialBackoff and metricsRestartMaxBackoff bound the delay
	// between restarts of the metrics server after it exits.
	metricsRestartInitialBackoff = 1 * time.Second
	metricsRestartMaxBackoff     = 60 * time.Second
)

// NewMetricsServer returns an http.Server that serves only Prometheus metrics
// on addr. It uses a private ServeMux rather than http.DefaultServeMux so that
// handlers registered on the default mux as a side effect (e.g. the
// /debug/pprof/* handlers from the net/http/pprof blank import) are not exposed
// on the metrics port. Explicit timeouts guard against slow clients holding
// connections open.
func NewMetricsServer(addr string) *http.Server {
	mux := http.NewServeMux()
	// Equivalent to promhttp.Handler(), plus a gather timeout.
	mux.Handle(MetricsPath, promhttp.InstrumentMetricHandler(prometheus.DefaultRegisterer,
		promhttp.HandlerFor(prometheus.DefaultGatherer, promhttp.HandlerOpts{Timeout: metricsGatherTimeout})))
	return &http.Server{
		Addr:              addr,
		Handler:           mux,
		ReadHeaderTimeout: metricsReadHeaderTimeout,
		ReadTimeout:       metricsReadTimeout,
		WriteTimeout:      metricsWriteTimeout,
		IdleTimeout:       metricsIdleTimeout,
	}
}

// RunMetricsServer serves Prometheus metrics on addr, restarting the server if
// it exits. Restarts are delayed with a capped exponential backoff so that a
// persistent failure (e.g. the port is already in use) does not turn into a
// busy loop. The backoff resets once the server has stayed up for at least
// the maximum backoff. When ctx is done, the running server is shut down and
// RunMetricsServer returns.
func RunMetricsServer(ctx context.Context, addr string) {
	runWithBackoff(ctx, func(ctx context.Context) error {
		return serveUntilDone(ctx, NewMetricsServer(addr))
	}, metricsRestartInitialBackoff, metricsRestartMaxBackoff, waitOrDone)
}

// serveUntilDone runs srv until it fails or ctx is done, in which case srv is
// shut down gracefully.
func serveUntilDone(ctx context.Context, srv *http.Server) error {
	stop := context.AfterFunc(ctx, func() {
		shutdownCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), metricsShutdownTimeout)
		defer cancel()
		_ = srv.Shutdown(shutdownCtx)
	})
	defer stop()
	return srv.ListenAndServe()
}

// runWithBackoff calls serve in a loop, using wait to pause between calls.
// It returns when ctx is done or wait returns false.
func runWithBackoff(ctx context.Context, serve func(ctx context.Context) error, initial, maxBackoff time.Duration,
	wait func(ctx context.Context, d time.Duration) bool) {
	log := logger.GetLogger(ctx)
	backoff := initial
	for {
		log.Info("Starting the http server to expose Prometheus metrics..")
		start := time.Now()
		err := serve(ctx)
		if ctx.Err() != nil {
			log.Info("Stopped the http server that exposes Prometheus metrics")
			return
		}
		if err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Errorf("Http server that exposes the Prometheus metrics exited with err: %+v", err)
		}
		if time.Since(start) >= maxBackoff {
			backoff = initial
		}
		log.Infof("Restarting http server to expose Prometheus metrics in %v..", backoff)
		if !wait(ctx, backoff) {
			return
		}
		backoff = min(backoff*2, maxBackoff)
	}
}

// waitOrDone sleeps for d and returns true, or returns false early if ctx is done.
func waitOrDone(ctx context.Context, d time.Duration) bool {
	timer := time.NewTimer(d)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-timer.C:
		return true
	}
}
