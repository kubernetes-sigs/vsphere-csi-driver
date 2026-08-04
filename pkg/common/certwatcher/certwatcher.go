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

// Package certwatcher provides a helper to load an mTLS identity (certificate,
// private key, and CA bundle) from disk, with optional hot-reload on file
// change so long-lived servers can pick up cert-manager-rotated certificates
// without a restart.
package certwatcher

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"os"
	"sync"
	"time"

	"github.com/fsnotify/fsnotify"

	"sigs.k8s.io/vsphere-csi-driver/v3/pkg/csi/service/logger"
)

// LoadCertificateAndCAPool synchronously reads and parses the certificate,
// private key, and CA bundle from the given paths. It performs a single,
// one-shot read with no filesystem watching, so it is suitable for
// short-lived clients that re-dial (and therefore naturally re-read the
// files) on every call.
func LoadCertificateAndCAPool(certPath, keyPath, caPath string) (tls.Certificate, *x509.CertPool, error) {
	cert, err := tls.LoadX509KeyPair(certPath, keyPath)
	if err != nil {
		return tls.Certificate{}, nil, fmt.Errorf("failed to load X509 key pair: %w", err)
	}

	caBytes, err := os.ReadFile(caPath)
	if err != nil {
		return tls.Certificate{}, nil, fmt.Errorf("failed to read CA certificate file: %w", err)
	}
	caCertPool := x509.NewCertPool()
	if ok := caCertPool.AppendCertsFromPEM(caBytes); !ok {
		return tls.Certificate{}, nil, errors.New("failed to parse CA certificate: invalid PEM format")
	}

	return cert, caCertPool, nil
}

// CertWatcher watches a certificate, private key, and CA bundle on disk for
// changes and keeps an in-memory copy up to date, enabling zero-downtime
// certificate rotation for long-lived gRPC servers: new connections
// automatically use the latest certificate/CA without a process restart.
type CertWatcher struct {
	sync.RWMutex

	currentCert       *tls.Certificate
	currentCACertPool *x509.CertPool

	watcher *fsnotify.Watcher

	certPath string
	keyPath  string
	caPath   string
}

// New returns a new CertWatcher for the given certificate, key, and CA bundle
// paths. It performs an initial synchronous read of all three files before
// returning, so a returned error means the identity is not yet available on
// disk.
func New(certPath, keyPath, caPath string) (*CertWatcher, error) {
	cw := &CertWatcher{
		certPath: certPath,
		keyPath:  keyPath,
		caPath:   caPath,
	}

	if err := cw.reload(); err != nil {
		return nil, err
	}

	watcher, err := fsnotify.NewWatcher()
	if err != nil {
		return nil, fmt.Errorf("failed to create fsnotify watcher: %w", err)
	}
	cw.watcher = watcher

	// Register watches synchronously here, rather than in Start, so that by
	// the time New returns, a concurrent write to certPath/keyPath is
	// guaranteed to be observed even if Start hasn't been scheduled yet.
	//
	// The CA path is not watched directly: cert-manager only rewrites ca.crt
	// in the same Secret update as the leaf cert/key, so re-reading it
	// whenever cert/key change is sufficient to observe CA rotation too.
	for _, f := range []string{certPath, keyPath} {
		if err := cw.watcher.Add(f); err != nil {
			return nil, fmt.Errorf("failed to watch %q: %w", f, err)
		}
	}

	return cw, nil
}

// GetCertificate returns the currently loaded certificate. It is designed to
// be used as a tls.Config.GetCertificate callback.
func (cw *CertWatcher) GetCertificate(_ *tls.ClientHelloInfo) (*tls.Certificate, error) {
	cw.RLock()
	defer cw.RUnlock()
	if cw.currentCert == nil {
		return nil, errors.New("no certificate available")
	}
	return cw.currentCert, nil
}

// GetCACertPool returns the currently loaded CA certificate pool, used to
// verify peer certificates in mTLS.
func (cw *CertWatcher) GetCACertPool() (*x509.CertPool, error) {
	cw.RLock()
	defer cw.RUnlock()
	if cw.currentCACertPool == nil {
		return nil, errors.New("no CA certificate pool available")
	}
	return cw.currentCACertPool, nil
}

// Start begins reacting to filesystem events for the certificate and key
// files (watched since New was called) and blocks until ctx is cancelled.
// Callers should invoke this in its own goroutine.
func (cw *CertWatcher) Start(ctx context.Context) error {
	log := logger.GetLogger(ctx)

	go cw.watch(ctx)

	log.Infof("Started certificate watcher for cert: %s, key: %s, ca: %s", cw.certPath, cw.keyPath, cw.caPath)
	<-ctx.Done()

	log.Infof("Stopping certificate watcher for cert: %s", cw.certPath)
	return cw.watcher.Close()
}

func (cw *CertWatcher) watch(ctx context.Context) {
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
				// watch on the (new) file at the same path.
				if err := cw.watcher.Add(event.Name); err != nil {
					log.Errorf("failed to re-watch %q after change: %v", event.Name, err)
				}
			default:
				continue
			}

			// Kubernetes Secret volume updates are not atomic across files
			// in the same directory; give the kubelet a brief moment to
			// finish updating all of them before reloading.
			time.Sleep(100 * time.Millisecond)
			if err := cw.reload(); err != nil {
				log.Errorf("failed to reload certificate after change: %v", err)
			} else {
				log.Infof("Reloaded certificate from %s", cw.certPath)
			}
		case err, ok := <-cw.watcher.Errors:
			if !ok {
				return
			}
			log.Errorf("certificate watcher error: %v", err)
		case <-ctx.Done():
			return
		}
	}
}

func (cw *CertWatcher) reload() error {
	cert, caCertPool, err := LoadCertificateAndCAPool(cw.certPath, cw.keyPath, cw.caPath)
	if err != nil {
		return err
	}

	cw.Lock()
	cw.currentCert = &cert
	cw.currentCACertPool = caCertPool
	cw.Unlock()
	return nil
}
