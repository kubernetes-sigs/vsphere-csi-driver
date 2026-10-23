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
	"fmt"
	"time"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/tools/cache"

	"sigs.k8s.io/vsphere-csi-driver/v3/pkg/csi/service/logger"
	k8s "sigs.k8s.io/vsphere-csi-driver/v3/pkg/kubernetes"
)

const (
	deletedVolumeInfoStartupRetryInterval = 5 * time.Second
	deletedVolumeInfoPollInterval         = 5 * time.Minute
)

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

func deletedVolumeInfoAPIAvailable(ctx context.Context, c dynamic.Interface) (bool, error) {
	// Probe the resource endpoint, not an individual DVI or the CRD object. This
	// uses existing list RBAC; an empty successful list still means installed.
	probeCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	_, err := c.Resource(deletedVolumeInfoGVR).Namespace(metav1.NamespaceAll).
		List(probeCtx, metav1.ListOptions{Limit: 1})
	if apierrors.IsNotFound(err) {
		return false, nil
	}
	if err != nil {
		return false, fmt.Errorf("discover DeletedVolumeInfo API: %w", err)
	}
	return true, nil
}

// initializeDeletedVolumeInfo fixes accounting mode before the reconciler starts.
// Only confirmed absence permits CVI-only accounting. Startup API errors retry
// without publishing partial usage. Later installation is activated by a restart.
func initializeDeletedVolumeInfo(ctx context.Context, c dynamic.Interface,
	startupRetryInterval, pollInterval time.Duration, restart func()) (cache.SharedIndexInformer, error) {
	if c == nil {
		return nil, nil
	}
	log := logger.GetLogger(ctx)
	var available bool
	err := wait.PollUntilContextCancel(ctx, startupRetryInterval, true, func(ctx context.Context) (bool, error) {
		if err := ctx.Err(); err != nil {
			return false, err
		}
		var err error
		available, err = deletedVolumeInfoAPIAvailable(ctx, c)
		if err != nil {
			log.Warnf("Cannot determine DeletedVolumeInfo API availability, preserving snapshot Used and retrying: %v", err)
			return false, nil
		}
		return true, nil
	})
	if err != nil {
		return nil, err
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if available {
		log.Info("DeletedVolumeInfo API available at startup; enabling snapshot accounting inventory")
		return deletedVolumeInfoInformer(c), nil
	}
	log.Infof("DeletedVolumeInfo API absent; using CVI-only snapshot accounting and polling every %s", pollInterval)
	go pollDeletedVolumeInfoAPI(ctx, c, pollInterval, restart)
	return nil, nil
}

// pollDeletedVolumeInfoAPI runs only when the API was absent at startup. A
// successful probe requests one syncer restart, even if the callback returns.
func pollDeletedVolumeInfoAPI(ctx context.Context, c dynamic.Interface, interval time.Duration, restart func()) {
	log := logger.GetLogger(ctx)
	_ = wait.PollUntilContextCancel(ctx, interval, false, func(ctx context.Context) (bool, error) {
		if err := ctx.Err(); err != nil {
			return false, err
		}
		available, err := deletedVolumeInfoAPIAvailable(ctx, c)
		if err != nil {
			log.Warnf("Cannot check for DeletedVolumeInfo API installation, will retry: %v", err)
			return false, nil
		}
		if !available {
			return false, nil
		}
		if err := ctx.Err(); err != nil {
			return false, err
		}
		log.Info("DeletedVolumeInfo API installed; restarting syncer to enable snapshot accounting inventory")
		restart()
		return true, nil
	})
}
