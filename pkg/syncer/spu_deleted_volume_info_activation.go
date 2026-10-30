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

package syncer

import (
	"context"
	"time"

	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/tools/cache"

	"sigs.k8s.io/vsphere-csi-driver/v3/pkg/csi/service/logger"
	k8s "sigs.k8s.io/vsphere-csi-driver/v3/pkg/kubernetes"
)

// deletedVolumeInfoInstallRetryInterval is the wait between attempts to install the
// CnsDeletedVolumeInfo CRD at startup.
const deletedVolumeInfoInstallRetryInterval = 5 * time.Second

// newDeletedVolumeInfoClient returns the client of the CnsDeletedVolumeInfo informer, or nil when
// SnapService local protection is disabled and no CnsDeletedVolumeInfo is read.
func newDeletedVolumeInfoClient(ctx context.Context, enabled bool) (dynamic.Interface, error) {
	if !enabled {
		return nil, nil
	}
	cfg, err := k8s.GetKubeConfig(ctx)
	if err != nil {
		return nil, err
	}
	return dynamic.NewForConfig(cfg)
}

// initializeDeletedVolumeInfo installs the CnsDeletedVolumeInfo CRD and returns its informer. It
// retries the installation until it succeeds or ctx is canceled, so that the snapshot
// StoragePolicyUsage reconciler never publishes Used from CnsVolumeInfo CRs alone. A nil client
// means the capability is disabled: nothing is installed and no informer is returned.
func initializeDeletedVolumeInfo(ctx context.Context, c dynamic.Interface, retryInterval time.Duration,
	installCRD func(context.Context) error) (cache.SharedIndexInformer, error) {
	if c == nil {
		return nil, nil
	}
	log := logger.GetLogger(ctx)
	err := wait.PollUntilContextCancel(ctx, retryInterval, true, func(ctx context.Context) (bool, error) {
		if err := installCRD(ctx); err != nil {
			log.Warnf("Failed to install the CnsDeletedVolumeInfo CRD, preserving snapshot Used and "+
				"retrying in %s: %v", retryInterval, err)
			return false, nil
		}
		return true, nil
	})
	if err != nil {
		return nil, err
	}
	log.Info("CnsDeletedVolumeInfo CRD installed; enabling deleted-volume snapshot accounting")
	return deletedVolumeInfoInformer(c), nil
}
