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
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"math/big"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// generateCA returns a PEM-encoded self-signed CA certificate and the key
// used to sign it, for building test leaf certificates.
func generateCA(t *testing.T, commonName string) ([]byte, *ecdsa.PrivateKey, *x509.Certificate) {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	require.NoError(t, err)

	tmpl := &x509.Certificate{
		SerialNumber:          big.NewInt(1),
		Subject:               pkix.Name{CommonName: commonName},
		NotBefore:             time.Now().Add(-time.Hour),
		NotAfter:              time.Now().Add(time.Hour),
		IsCA:                  true,
		KeyUsage:              x509.KeyUsageCertSign | x509.KeyUsageDigitalSignature,
		BasicConstraintsValid: true,
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, &key.PublicKey, key)
	require.NoError(t, err)

	cert, err := x509.ParseCertificate(der)
	require.NoError(t, err)

	return pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der}), key, cert
}

// generateLeaf returns PEM-encoded leaf certificate and key, signed by the
// given CA, with commonName as its Subject CommonName.
func generateLeaf(t *testing.T, commonName string, caCert *x509.Certificate, caKey *ecdsa.PrivateKey) ([]byte, []byte) {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	require.NoError(t, err)

	tmpl := &x509.Certificate{
		SerialNumber: big.NewInt(2),
		Subject:      pkix.Name{CommonName: commonName},
		NotBefore:    time.Now().Add(-time.Hour),
		NotAfter:     time.Now().Add(time.Hour),
		KeyUsage:     x509.KeyUsageDigitalSignature | x509.KeyUsageKeyEncipherment,
		ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth, x509.ExtKeyUsageClientAuth},
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, caCert, &key.PublicKey, caKey)
	require.NoError(t, err)

	keyDER, err := x509.MarshalECPrivateKey(key)
	require.NoError(t, err)

	certPEM := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der})
	keyPEM := pem.EncodeToMemory(&pem.Block{Type: "EC PRIVATE KEY", Bytes: keyDER})
	return certPEM, keyPEM
}

// writeIdentity writes tls.crt/tls.key/ca.crt into dir with the given
// contents, mirroring the shape of a cert-manager-issued Secret volume mount.
func writeIdentity(t *testing.T, dir string, certPEM, keyPEM, caPEM []byte) (certPath, keyPath, caPath string) {
	t.Helper()
	certPath = filepath.Join(dir, "tls.crt")
	keyPath = filepath.Join(dir, "tls.key")
	caPath = filepath.Join(dir, "ca.crt")
	require.NoError(t, os.WriteFile(certPath, certPEM, 0o600))
	require.NoError(t, os.WriteFile(keyPath, keyPEM, 0o600))
	require.NoError(t, os.WriteFile(caPath, caPEM, 0o600))
	return certPath, keyPath, caPath
}

func TestLoadCertificateAndCAPool(t *testing.T) {
	caPEM, caKey, caCert := generateCA(t, "test-ca")
	leafCertPEM, leafKeyPEM := generateLeaf(t, "test-leaf", caCert, caKey)

	t.Run("valid identity loads successfully", func(t *testing.T) {
		dir := t.TempDir()
		certPath, keyPath, caPath := writeIdentity(t, dir, leafCertPEM, leafKeyPEM, caPEM)

		cert, pool, err := LoadCertificateAndCAPool(certPath, keyPath, caPath)
		require.NoError(t, err)
		require.NotNil(t, cert.Certificate)
		require.NotNil(t, pool)

		parsed, err := x509.ParseCertificate(cert.Certificate[0])
		require.NoError(t, err)
		require.Equal(t, "test-leaf", parsed.Subject.CommonName)

		// Confirm that the CA can validate a certificate
		// genuinely signed by this CA.
		_, err = parsed.Verify(x509.VerifyOptions{Roots: pool, KeyUsages: []x509.ExtKeyUsage{x509.ExtKeyUsageAny}})
		require.NoError(t, err)
	})

	t.Run("missing cert file errors", func(t *testing.T) {
		dir := t.TempDir()
		_, keyPath, caPath := writeIdentity(t, dir, leafCertPEM, leafKeyPEM, caPEM)
		_, _, err := LoadCertificateAndCAPool(filepath.Join(dir, "missing.crt"), keyPath, caPath)
		require.Error(t, err)
	})

	t.Run("missing CA file errors", func(t *testing.T) {
		dir := t.TempDir()
		certPath, keyPath, _ := writeIdentity(t, dir, leafCertPEM, leafKeyPEM, caPEM)
		_, _, err := LoadCertificateAndCAPool(certPath, keyPath, filepath.Join(dir, "missing.crt"))
		require.Error(t, err)
	})

	t.Run("invalid PEM in CA file errors", func(t *testing.T) {
		dir := t.TempDir()
		certPath, keyPath, caPath := writeIdentity(t, dir, leafCertPEM, leafKeyPEM, caPEM)
		require.NoError(t, os.WriteFile(caPath, []byte("not a pem"), 0o600))
		_, _, err := LoadCertificateAndCAPool(certPath, keyPath, caPath)
		require.Error(t, err)
	})
}

func TestCertWatcher_InitialLoad(t *testing.T) {
	caPEM, caKey, caCert := generateCA(t, "test-ca")
	leafCertPEM, leafKeyPEM := generateLeaf(t, "test-leaf-1", caCert, caKey)

	dir := t.TempDir()
	certPath, keyPath, caPath := writeIdentity(t, dir, leafCertPEM, leafKeyPEM, caPEM)

	cw, err := New(certPath, keyPath, caPath)
	require.NoError(t, err)

	cert, err := cw.GetCertificate(nil)
	require.NoError(t, err)
	parsed, err := x509.ParseCertificate(cert.Certificate[0])
	require.NoError(t, err)
	require.Equal(t, "test-leaf-1", parsed.Subject.CommonName)

	pool, err := cw.GetCACertPool()
	require.NoError(t, err)
	require.NotNil(t, pool)
}

func TestCertWatcher_MissingFiles(t *testing.T) {
	dir := t.TempDir()
	_, err := New(filepath.Join(dir, "tls.crt"), filepath.Join(dir, "tls.key"), filepath.Join(dir, "ca.crt"))
	require.Error(t, err)
}

func TestCertWatcher_RotationIsPickedUp(t *testing.T) {
	caPEM, caKey, caCert := generateCA(t, "test-ca")
	leafCertPEM1, leafKeyPEM1 := generateLeaf(t, "test-leaf-1", caCert, caKey)
	leafCertPEM2, leafKeyPEM2 := generateLeaf(t, "test-leaf-2", caCert, caKey)

	dir := t.TempDir()
	certPath, keyPath, caPath := writeIdentity(t, dir, leafCertPEM1, leafKeyPEM1, caPEM)

	cw, err := New(certPath, keyPath, caPath)
	require.NoError(t, err)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() {
		_ = cw.Start(ctx)
	}()

	// Simulate a cert-manager Secret volume rotation: rewrite cert and key.
	require.NoError(t, os.WriteFile(certPath, leafCertPEM2, 0o600))
	require.NoError(t, os.WriteFile(keyPath, leafKeyPEM2, 0o600))

	require.Eventually(t, func() bool {
		cert, err := cw.GetCertificate(nil)
		if err != nil {
			return false
		}
		parsed, err := x509.ParseCertificate(cert.Certificate[0])
		if err != nil {
			return false
		}
		return parsed.Subject.CommonName == "test-leaf-2"
	}, 5*time.Second, 50*time.Millisecond, "expected certificate watcher to pick up rotated certificate")
}
