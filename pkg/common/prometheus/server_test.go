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
	"net/http/httptest"
	_ "net/http/pprof" // registers /debug/pprof/* on http.DefaultServeMux, as the binaries do
	"testing"
)

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

	tests := []struct {
		path       string
		wantStatus int
	}{
		{MetricsPath, http.StatusOK},
		{"/debug/pprof/", http.StatusNotFound},
		{"/debug/pprof/heap", http.StatusNotFound},
		{"/debug/pprof/profile", http.StatusNotFound},
	}
	for _, tt := range tests {
		rec := httptest.NewRecorder()
		srv.Handler.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, tt.path, nil))
		if rec.Code != tt.wantStatus {
			t.Errorf("GET %s: status = %d, want %d", tt.path, rec.Code, tt.wantStatus)
		}
	}
}

func TestNewMetricsServerCanBeCalledRepeatedly(t *testing.T) {
	// The metrics goroutines rebuild the server on each restart; this must not
	// panic with a duplicate-registration error.
	NewMetricsServer(":2112")
	NewMetricsServer(":2112")
}
