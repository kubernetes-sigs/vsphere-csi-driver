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

package storagepolicyinfo

import (
	"context"
	"os"
	"strconv"
	"strings"
	"time"

	apitypes "k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/event"

	spiv1alpha1 "sigs.k8s.io/vsphere-csi-driver/v3/pkg/apis/cnsoperator/storagepolicyinfo/v1alpha1"
	"sigs.k8s.io/vsphere-csi-driver/v3/pkg/csi/service/logger"
)

const (
	// slowSyncIntervalEnvVar is the environment variable that controls how often
	// all StoragePolicyInfo CRs are re-enqueued for reconciliation. Expressed in
	// minutes; defaults to 60 (1 hour).
	slowSyncIntervalEnvVar     = "NS_STORAGE_POLICY_INFO_RESYNC_INTERVAL_MINUTES"
	defaultSlowSyncIntervalMin = 60
)

// getSlowSyncInterval returns the periodic resync interval for StoragePolicyInfo
// CRs.
func getSlowSyncInterval(ctx context.Context) time.Duration {
	log := logger.GetLogger(ctx)
	v := strings.TrimSpace(os.Getenv(slowSyncIntervalEnvVar))
	if v == "" {
		return defaultSlowSyncIntervalMin * time.Minute
	}
	value, err := strconv.Atoi(v)
	if err != nil {
		log.Warnf("StoragePolicyInfo slow sync: %s=%q is invalid, "+
			"using default %d minutes", slowSyncIntervalEnvVar, v, defaultSlowSyncIntervalMin)
		return defaultSlowSyncIntervalMin * time.Minute
	}
	if value <= 0 {
		log.Warnf("StoragePolicyInfo slow sync: %s=%q is non-positive, "+
			"using default %d minutes", slowSyncIntervalEnvVar, v, defaultSlowSyncIntervalMin)
		return defaultSlowSyncIntervalMin * time.Minute
	}
	log.Infof("StoragePolicyInfo slow sync: interval set to %d minutes", value)
	return time.Duration(value) * time.Minute
}

// StartPeriodicResync runs a goroutine that, every interval, lists all
// StoragePolicyInfo CRs across all namespaces and sends them to ch so the
// controller re-reconciles each one (slow sync).
func StartPeriodicResync(ctx context.Context, c client.Client,
	ch chan<- event.GenericEvent, interval time.Duration, r *ReconcileStoragePolicyInfo) {
	log := logger.GetLogger(ctx)
	go func() {
		if interval <= 0 {
			log.Warnf("StoragePolicyInfo slow sync: interval %s is non-positive, "+
				"using default %d minutes", interval, defaultSlowSyncIntervalMin)
			interval = defaultSlowSyncIntervalMin * time.Minute
		}
		ticker := time.NewTicker(interval)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				log.Infof("StoragePolicyInfo periodic resync goroutine stopping")
				return
			case <-ticker.C:
				var list spiv1alpha1.StoragePolicyInfoList
				if err := c.List(ctx, &list); err != nil {
					log.Errorf("StoragePolicyInfo periodic resync: list failed: %v", err)
					continue
				}
				enqueued := 0
				for i := range list.Items {
					obj := &list.Items[i]
					namespacedName := apitypes.NamespacedName{
						Namespace: obj.Namespace,
						Name:      obj.Name,
					}

					r.backOffDurationMapMutex.Lock()
					backoff := r.backOffDuration[namespacedName]
					r.backOffDurationMapMutex.Unlock()
					if backoff > time.Second {
						// Backoff is only incremented above one second after a
						// failed reconcile (it is reset to one second / deleted on
						// success). A value greater than one second therefore means
						// the instance is already scheduled to reconcile via
						// RequeueAfter, so skip it here to avoid defeating the
						// backoff.
						continue
					}

					select {
					case ch <- event.GenericEvent{Object: obj}:
						enqueued++
					case <-ctx.Done():
						return
					default:
						// ch is full; do not block this goroutine waiting for a slot,
						// since that would stall the rest of this sweep. Skip obj for
						// this tick, it will be picked up on the next resync interval.
						log.Warnf("StoragePolicyInfo periodic resync: resync channel full, "+
							"skipping %q for this tick", namespacedName)
					}
				}
				log.Infof("StoragePolicyInfo periodic resync: enqueued %d/%d CRs",
					enqueued, len(list.Items))
			}
		}
	}()
}
