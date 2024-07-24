/*
Copyright 2024 The Kubernetes Authors.

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

package cnsunregistervolume

import (
	"context"
	"os"
	"strconv"

	"sigs.k8s.io/vsphere-csi-driver/v3/pkg/csi/service/logger"
)

// getMaxWorkerThreadsToReconcileCnsUnregisterVolume returns the maximum number
// of worker threads which can be run to reconcile CnsUnregisterVolume instances.
// If environment variable WORKER_THREADS_UNREGISTER_VOLUME is set and valid,
// return the value read from environment variable. Otherwise, use the default
// value.
func getMaxWorkerThreadsToReconcileCnsUnregisterVolume(ctx context.Context) int {
	log := logger.GetLogger(ctx)
	workerThreads := defaultMaxWorkerThreadsForUnregisterVolume
	if v := os.Getenv("WORKER_THREADS_UNREGISTER_VOLUME"); v != "" {
		if value, err := strconv.Atoi(v); err == nil {
			if value <= 0 {
				log.Warnf("Maximum number of worker threads to run set in env variable "+
					"WORKER_THREADS_UNREGISTER_VOLUME %s is less than 1, will use the default value %d",
					v, defaultMaxWorkerThreadsForUnregisterVolume)
			} else if value > defaultMaxWorkerThreadsForUnregisterVolume {
				log.Warnf("Maximum number of worker threads to run set in env variable "+
					"WORKER_THREADS_UNREGISTER_VOLUME %s is greater than %d, will use the default value %d",
					v, defaultMaxWorkerThreadsForUnregisterVolume, defaultMaxWorkerThreadsForUnregisterVolume)
			} else {
				workerThreads = value
				log.Debugf("Maximum number of worker threads to run to reconcile CnsUnregisterVolume instances is set to %d",
					workerThreads)
			}
		} else {
			log.Warnf("Maximum number of worker threads to run set in env variable "+
				"WORKER_THREADS_UNREGISTER_VOLUME %s is invalid, will use the default value %d",
				v, defaultMaxWorkerThreadsForUnregisterVolume)
		}
	} else {
		log.Debugf("WORKER_THREADS_UNREGISTER_VOLUME is not set. Picking the default value %d",
			defaultMaxWorkerThreadsForUnregisterVolume)
	}
	return workerThreads
}
