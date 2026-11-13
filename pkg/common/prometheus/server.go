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
	"net/http"
	"time"

	"github.com/prometheus/client_golang/prometheus/promhttp"
)

const (
	// MetricsPath is the HTTP path on which Prometheus metrics are served.
	MetricsPath = "/metrics"

	metricsReadHeaderTimeout = 10 * time.Second
	metricsReadTimeout       = 30 * time.Second
	metricsWriteTimeout      = 30 * time.Second
	metricsIdleTimeout       = 120 * time.Second
)

// NewMetricsServer returns an http.Server that serves only Prometheus metrics
// on addr. It uses a private ServeMux rather than http.DefaultServeMux so that
// handlers registered on the default mux as a side effect (e.g. the
// /debug/pprof/* handlers from the net/http/pprof blank import) are not exposed
// on the metrics port. Explicit timeouts guard against slow clients holding
// connections open.
func NewMetricsServer(addr string) *http.Server {
	mux := http.NewServeMux()
	mux.Handle(MetricsPath, promhttp.Handler())
	return &http.Server{
		Addr:              addr,
		Handler:           mux,
		ReadHeaderTimeout: metricsReadHeaderTimeout,
		ReadTimeout:       metricsReadTimeout,
		WriteTimeout:      metricsWriteTimeout,
		IdleTimeout:       metricsIdleTimeout,
	}
}
