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
	"os"
	"strconv"
	"strings"
	"time"

	"k8s.io/apimachinery/pkg/util/wait"

	"sigs.k8s.io/vsphere-csi-driver/v3/pkg/csi/service/logger"
)

// slowSyncJitterFactor spreads out the cnsoperator controllers' resync schedules so
// they don't fire in lockstep against the API server.
const slowSyncJitterFactor = 0.01

// GetSlowSyncInterval reads envVar (whole minutes) and returns the resync interval,
// falling back to defaultMin if unset/invalid/non-positive. logPrefix namespaces
// the log lines to the calling controller.
func GetSlowSyncInterval(ctx context.Context, logPrefix, envVar string,
	defaultMin int) time.Duration {
	log := logger.GetLogger(ctx)
	v := strings.TrimSpace(os.Getenv(envVar))
	if v == "" {
		return time.Duration(defaultMin) * time.Minute
	}
	value, err := strconv.Atoi(v)
	if err != nil {
		log.Warnf("%s slow sync: %s=%q is invalid, using default %d minutes",
			logPrefix, envVar, v, defaultMin)
		return time.Duration(defaultMin) * time.Minute
	}
	if value <= 0 {
		log.Warnf("%s slow sync: %s=%q is non-positive, using default %d minutes",
			logPrefix, envVar, v, defaultMin)
		return time.Duration(defaultMin) * time.Minute
	}
	log.Infof("%s slow sync: interval set to %d minutes", logPrefix, value)
	return time.Duration(value) * time.Minute
}

// RunPeriodicResync waits one interval (jittered), invokes listAndEnqueue, and
// repeats until ctx is cancelled, including the first run. Blocks, so
// non-blocking callers (e.g. StartPeriodicResync) invoke it via `go`. Falls back
// to defaultMin if interval is non-positive, matching GetSlowSyncInterval's own
// fallback.
//
// The per-CR listing/backoff/send logic lives in the caller's closure since backoff
// bookkeeping differs per controller (package-level map vs. per-reconciler field).
func RunPeriodicResync(ctx context.Context, logPrefix string, interval time.Duration, defaultMin int,
	listAndEnqueue func(ctx context.Context)) {
	log := logger.GetLogger(ctx)
	if interval <= 0 {
		// Defensive: GetSlowSyncInterval never returns a non-positive value, but
		// guard against a caller passing one so we never spin nor silently stop
		// resyncing altogether.
		log.Warnf("%s slow sync: interval %s is non-positive, using default %d minutes",
			logPrefix, interval, defaultMin)
		interval = time.Duration(defaultMin) * time.Minute
	}
	// JitterUntil's first call is immediate and unjittered; skip its work so the
	// first real sweep waits out a jittered interval too.
	first := true
	wait.JitterUntil(func() {
		if first {
			first = false
			return
		}
		listAndEnqueue(ctx)
	}, interval, slowSyncJitterFactor, false, ctx.Done())
	log.Infof("%s periodic resync stopping", logPrefix)
}
