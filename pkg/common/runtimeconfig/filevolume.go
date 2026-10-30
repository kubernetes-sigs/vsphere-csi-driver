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

package runtimeconfig

import (
	"context"
	"sync"

	"k8s.io/client-go/kubernetes"

	"sigs.k8s.io/vsphere-csi-driver/v3/pkg/common/config"
	"sigs.k8s.io/vsphere-csi-driver/v3/pkg/common/types"
)

// runtimeFileVolumes shares an in-memory cache and retry queue between the config loader and watch
// worker. Keeping it across config loads also preserves failures queued before watches start.
var runtimeFileVolumes = newFileVolumeSource()

type fileVolumeKey struct {
	namespace string
	host      types.FQDN
	sddc      string
}

type fileVolumeSettings struct {
	activated bool
	clusters  string
}

// fileVolumeSource preserves successful reads across fresh config Secret loads and feeds failures
// to the file volume worker. Credential reloads can succeed while this optional source is unavailable.
type fileVolumeSource struct {
	mu           sync.Mutex
	lastGood     map[fileVolumeKey]fileVolumeSettings
	failures     uint64
	lastError    error
	retryPending bool
	trigger      chan struct{}
}

func newFileVolumeSource() *fileVolumeSource {
	return &fileVolumeSource{trigger: make(chan struct{}, 1)}
}

// apply refreshes the cache on success. On a read or parse failure, it restores settings only for
// the same namespace, vCenter host and SDDC, and queues a retry. Without a matching cache entry,
// the values loaded from the config Secret stay in cfg.
func (s *fileVolumeSource) apply(ctx context.Context, client kubernetes.Interface, namespace string,
	cfg *config.Config) error {
	if !hasSDDCName(cfg) {
		return nil
	}
	s.mu.Lock()
	defer s.mu.Unlock()

	err := applyFileVolumeConfig(ctx, client, namespace, cfg)
	if err == nil {
		// Successful reads replace the fallback, including an explicit disable or missing keys.
		// A missing ConfigMap is also a successful result: use the config Secret's values.
		// This prevents a later read failure from reviving settings that were removed.
		s.lastGood = make(map[fileVolumeKey]fileVolumeSettings)
	}
	for host, vc := range cfg.VirtualCenter {
		if vc.SDDCName == "" {
			continue
		}
		key := fileVolumeKey{namespace: namespace, host: host, sddc: vc.SDDCName}
		if err == nil {
			s.lastGood[key] = fileVolumeSettings{vc.FileVolumeActivated, vc.TargetvSANFileShareClusters}
		} else if previous, ok := s.lastGood[key]; ok {
			vc.FileVolumeActivated = previous.activated
			vc.TargetvSANFileShareClusters = previous.clusters
		}
	}
	if err != nil {
		s.failures++
		s.lastError = err
		// Only the first failure queues a reload. Repeated failures must respect the worker's
		// retry interval instead of continually interrupting its backoff.
		if !s.retryPending {
			s.retryPending = true
			select {
			case s.trigger <- struct{}{}:
			default:
			}
		}
	}
	return err
}

// reload exposes an optional-source failure to the retry worker even though the config loader
// accepts the fallback. Count failures during the callback: a concurrent successful read alone
// does not prove that the controller has applied the new settings.
func (s *fileVolumeSource) reload(reload func() error) func() error {
	return func() error {
		s.mu.Lock()
		before := s.failures
		s.mu.Unlock()
		if err := reload(); err != nil {
			return err
		}
		s.mu.Lock()
		defer s.mu.Unlock()
		if s.failures != before {
			return s.lastError
		}
		// Clear pending recovery only after the reload succeeds without another source failure.
		// A successful config read in apply alone may never reach the controller's active config.
		s.retryPending = false
		return nil
	}
}
