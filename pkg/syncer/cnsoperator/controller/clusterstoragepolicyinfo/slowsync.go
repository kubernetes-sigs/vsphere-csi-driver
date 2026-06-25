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

package clusterstoragepolicyinfo

import (
	"context"
	"os"
	"strconv"
	"time"

	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/event"

	clusterspiv1alpha1 "sigs.k8s.io/vsphere-csi-driver/v3/pkg/apis/cnsoperator/clusterstoragepolicyinfo/v1alpha1"
	"sigs.k8s.io/vsphere-csi-driver/v3/pkg/csi/service/logger"
)

const (
	// slowSyncIntervalEnvVar is the environment variable that controls how often
	// all ClusterStoragePolicyInfo CRs are re-enqueued for reconciliation against
	// the vCenter. Expressed in minutes; defaults to 720 (12 hours).
	slowSyncIntervalEnvVar     = "STORAGE_POLICY_INFO_RESYNC_INTERVAL_MINUTES"
	defaultSlowSyncIntervalMin = 720
)

// getSlowSyncInterval returns the periodic resync interval for
// ClusterStoragePolicyInfo CRs.
func getSlowSyncInterval(ctx context.Context) time.Duration {
	log := logger.GetLogger(ctx)
	v := os.Getenv(slowSyncIntervalEnvVar)
	if v == "" {
		return defaultSlowSyncIntervalMin * time.Minute
	}
	value, err := strconv.Atoi(v)
	if err != nil {
		log.Warnf("ClusterStoragePolicyInfo slow sync: %s=%q is invalid, "+
			"using default %d minutes", slowSyncIntervalEnvVar, v, defaultSlowSyncIntervalMin)
		return defaultSlowSyncIntervalMin * time.Minute
	}
	if value <= 0 {
		log.Warnf("ClusterStoragePolicyInfo slow sync: %s=%q is non-positive, "+
			"using default %d minutes", slowSyncIntervalEnvVar, v, defaultSlowSyncIntervalMin)
		return defaultSlowSyncIntervalMin * time.Minute
	}
	log.Infof("ClusterStoragePolicyInfo slow sync: interval set to %d minutes", value)
	return time.Duration(value) * time.Minute
}

// StartPeriodicResync runs a goroutine that, every interval, lists all
// ClusterStoragePolicyInfo CRs and sends them to ch so the controller
// re-reconciles each one against the vCenter (slow sync).
func StartPeriodicResync(ctx context.Context, c client.Client,
	ch chan<- event.GenericEvent, interval time.Duration) {
	log := logger.GetLogger(ctx)
	go func() {
		ticker := time.NewTicker(interval)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				log.Infof("ClusterStoragePolicyInfo periodic resync goroutine stopping")
				return
			case <-ticker.C:
				var list clusterspiv1alpha1.ClusterStoragePolicyInfoList
				if err := c.List(ctx, &list); err != nil {
					log.Errorf("ClusterStoragePolicyInfo periodic resync: list failed: %v", err)
					continue
				}
				for i := range list.Items {
					obj := &list.Items[i]
					select {
					case ch <- event.GenericEvent{Object: obj}:
					case <-ctx.Done():
						return
					}
				}
				log.Infof("ClusterStoragePolicyInfo periodic resync: enqueued %d CRs", len(list.Items))
			}
		}
	}()
}
