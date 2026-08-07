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
	"time"

	apitypes "k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/event"

	spiv1alpha1 "sigs.k8s.io/vsphere-csi-driver/v3/pkg/apis/cnsoperator/storagepolicyinfo/v1alpha1"
	"sigs.k8s.io/vsphere-csi-driver/v3/pkg/csi/service/logger"
	"sigs.k8s.io/vsphere-csi-driver/v3/pkg/syncer/cnsoperator/util"
)

const (
	// slowSyncIntervalEnvVar is the environment variable that controls how often
	// all StoragePolicyInfo CRs are re-enqueued for reconciliation. Expressed in
	// minutes; defaults to 60 (1 hour).
	slowSyncIntervalEnvVar     = "NS_STORAGE_POLICY_INFO_RESYNC_INTERVAL_MINUTES"
	defaultSlowSyncIntervalMin = 60
	// slowSyncLogPrefix namespaces slow-sync log lines to this controller.
	slowSyncLogPrefix = "StoragePolicyInfo"
)

// getSlowSyncInterval returns the periodic resync interval for StoragePolicyInfo
// CRs.
func getSlowSyncInterval(ctx context.Context) time.Duration {
	return util.GetSlowSyncInterval(ctx, slowSyncLogPrefix, slowSyncIntervalEnvVar,
		defaultSlowSyncIntervalMin)
}

// StartPeriodicResync lists all StoragePolicyInfo CRs across all namespaces every
// interval (jittered) and sends each to ch for re-reconciliation (slow sync).
// Returns immediately; the goroutine runs until ctx is cancelled.
func StartPeriodicResync(ctx context.Context, c client.Client,
	ch chan<- event.GenericEvent, interval time.Duration, r *ReconcileStoragePolicyInfo) {
	go util.RunPeriodicResync(ctx, slowSyncLogPrefix, interval, defaultSlowSyncIntervalMin, func(ctx context.Context) {
		log := logger.GetLogger(ctx)
		var list spiv1alpha1.StoragePolicyInfoList
		if err := c.List(ctx, &list); err != nil {
			log.Errorf("StoragePolicyInfo periodic resync: list failed: %v", err)
			return
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
				// A backoff above one second means the instance failed a recent
				// reconcile and is already scheduled to retry via RequeueAfter, so
				// skip it here to avoid defeating the backoff.
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
	})
}
