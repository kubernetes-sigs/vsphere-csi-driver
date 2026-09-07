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

package certwatcher

import (
	"bytes"
	"context"
	"crypto/x509"
	"errors"
	"fmt"
	"os"
	"sync"
	"time"

	"github.com/fsnotify/fsnotify"

	"sigs.k8s.io/vsphere-csi-driver/v3/pkg/csi/service/logger"
)

// pollInterval is a backstop against missed or coalesced fsnotify events -
// for example a CA-only rotation that lands in the same Secret as, but does
// not itself touch, files this process otherwise watches.
const pollInterval = 30 * time.Second

// LoadCACertPool reads and parses a CA bundle from the given path.
func LoadCACertPool(caPath string) (*x509.CertPool, error) {
	caBytes, err := os.ReadFile(caPath)
	if err != nil {
		return nil, fmt.Errorf("failed to read CA certificate file: %w", err)
	}
	caCertPool := x509.NewCertPool()
	if ok := caCertPool.AppendCertsFromPEM(caBytes); !ok {
		return nil, errors.New("failed to parse CA certificate: invalid PEM format")
	}
	return caCertPool, nil
}

// CAWatcher watches a CA bundle file on disk for changes and keeps an
// in-memory CertPool up to date. It is independent of, and does not rely on,
// any other watcher for the leaf certificate this CA may have issued -
// callers should call GetCACertPool to fetch a fresh pool on every use
// (e.g. per TLS handshake) rather than caching its result.
type CAWatcher struct {
	sync.RWMutex

	current *x509.CertPool
	rawPEM  []byte

	watcher *fsnotify.Watcher

	caPath string
}

// New returns a new CAWatcher for the given CA bundle path. It performs an
// initial read of the file before returning, so a returned error means the
// CA bundle is not yet available on disk.
func New(caPath string) (*CAWatcher, error) {
	cw := &CAWatcher{
		caPath: caPath,
	}

	if err := cw.reload(); err != nil {
		return nil, err
	}

	watcher, err := fsnotify.NewWatcher()
	if err != nil {
		return nil, fmt.Errorf("failed to create fsnotify watcher: %w", err)
	}
	cw.watcher = watcher

	if err := cw.watcher.Add(caPath); err != nil {
		cw.watcher.Close()
		return nil, fmt.Errorf("failed to watch %q: %w", caPath, err)
	}

	return cw, nil
}

// GetCACertPool returns the currently loaded CA certificate pool, used to
// verify peer certificates in mTLS.
func (cw *CAWatcher) GetCACertPool() (*x509.CertPool, error) {
	cw.RLock()
	defer cw.RUnlock()
	if cw.current == nil {
		return nil, errors.New("no CA certificate pool available")
	}
	return cw.current, nil
}

// Start begins reacting to filesystem events and polling for changes to the
// CA bundle file, and blocks until ctx is cancelled.
func (cw *CAWatcher) Start(ctx context.Context) error {
	log := logger.GetLogger(ctx)

	go cw.watch(ctx)

	ticker := time.NewTicker(pollInterval)
	defer ticker.Stop()

	log.Infof("Started CA certificate watcher for: %s", cw.caPath)
	for {
		select {
		case <-ctx.Done():
			log.Infof("Stopping CA certificate watcher for: %s", cw.caPath)
			return cw.watcher.Close()
		case <-ticker.C:
			if err := cw.reload(); err != nil {
				log.Errorf("failed to poll CA certificate: %v", err)
			}
		}
	}
}

func (cw *CAWatcher) watch(ctx context.Context) {
	log := logger.GetLogger(ctx)
	for {
		select {
		case event, ok := <-cw.watcher.Events:
			if !ok {
				return
			}
			switch {
			case event.Op.Has(fsnotify.Write), event.Op.Has(fsnotify.Create):
				// fall through to reload below
			case event.Op.Has(fsnotify.Chmod), event.Op.Has(fsnotify.Remove):
				// Kubernetes Secret volume updates replace the file via a
				// symlink swap, which can surface as remove; re-add the
				// watch on the file at the same path.
				if err := cw.watcher.Add(event.Name); err != nil {
					log.Errorf("failed to re-watch %q after change: %v", event.Name, err)
				}
			default:
				continue
			}

			if err := cw.reload(); err != nil {
				log.Errorf("failed to reload CA certificate after change: %v", err)
			}
		case err, ok := <-cw.watcher.Errors:
			if !ok {
				return
			}
			log.Errorf("CA certificate watcher error: %v", err)
		case <-ctx.Done():
			return
		}
	}
}

func (cw *CAWatcher) reload() error {
	caBytes, err := os.ReadFile(cw.caPath)
	if err != nil {
		return fmt.Errorf("failed to read CA certificate file: %w", err)
	}

	cw.RLock()
	unchanged := bytes.Equal(cw.rawPEM, caBytes)
	cw.RUnlock()
	if unchanged {
		return nil
	}

	caCertPool := x509.NewCertPool()
	if ok := caCertPool.AppendCertsFromPEM(caBytes); !ok {
		return errors.New("failed to parse CA certificate: invalid PEM format")
	}

	cw.Lock()
	cw.current = caCertPool
	cw.rawPEM = caBytes
	cw.Unlock()
	return nil
}
