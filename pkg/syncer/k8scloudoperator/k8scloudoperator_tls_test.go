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

package k8scloudoperator

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"math/big"
	"net"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/credentials"

	"sigs.k8s.io/vsphere-csi-driver/v3/pkg/common/tlsprofile"
)

// testCA is a throwaway CA able to issue server and client certificates.
type testCA struct {
	cert *x509.Certificate
	key  *ecdsa.PrivateKey
	pem  []byte
}

func newTestCA(t *testing.T) *testCA {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	require.NoError(t, err)
	tmpl := &x509.Certificate{
		SerialNumber:          big.NewInt(time.Now().UnixNano()),
		Subject:               pkix.Name{CommonName: "test-ca"},
		NotBefore:             time.Now().Add(-time.Hour),
		NotAfter:              time.Now().Add(time.Hour),
		IsCA:                  true,
		BasicConstraintsValid: true,
		KeyUsage:              x509.KeyUsageCertSign | x509.KeyUsageDigitalSignature,
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, &key.PublicKey, key)
	require.NoError(t, err)
	cert, err := x509.ParseCertificate(der)
	require.NoError(t, err)
	return &testCA{cert: cert, key: key, pem: pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der})}
}

// issue returns a PEM certificate and PEM private key signed by the CA.
func (ca *testCA) issue(t *testing.T, cn string, usage x509.ExtKeyUsage) (certPEM, keyPEM []byte) {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	require.NoError(t, err)
	tmpl := &x509.Certificate{
		SerialNumber: big.NewInt(time.Now().UnixNano()),
		Subject:      pkix.Name{CommonName: cn},
		NotBefore:    time.Now().Add(-time.Hour),
		NotAfter:     time.Now().Add(time.Hour),
		KeyUsage:     x509.KeyUsageDigitalSignature,
		ExtKeyUsage:  []x509.ExtKeyUsage{usage},
		DNSNames:     []string{"localhost"},
		IPAddresses:  []net.IP{net.ParseIP("127.0.0.1")},
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, ca.cert, &key.PublicKey, ca.key)
	require.NoError(t, err)
	keyDER, err := x509.MarshalECPrivateKey(key)
	require.NoError(t, err)
	return pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der}),
		pem.EncodeToMemory(&pem.Block{Type: "EC PRIVATE KEY", Bytes: keyDER})
}

// writeServerDir writes tls.crt, tls.key and ca.crt for a server whose identity is issued by ca.
func writeServerDir(t *testing.T, dir string, ca *testCA) {
	t.Helper()
	crt, key := ca.issue(t, "localhost", x509.ExtKeyUsageServerAuth)
	require.NoError(t, os.WriteFile(filepath.Join(dir, "ca.crt"), ca.pem, 0o600))
	require.NoError(t, os.WriteFile(filepath.Join(dir, "tls.key"), key, 0o600))
	require.NoError(t, os.WriteFile(filepath.Join(dir, "tls.crt"), crt, 0o600))
}

// clientConfig builds a client TLS config trusting ca. A nil clientCN means no client certificate.
func clientConfig(t *testing.T, ca *testCA, clientCN *string) *tls.Config {
	t.Helper()
	pool := x509.NewCertPool()
	pool.AddCert(ca.cert)
	cfg := &tls.Config{
		RootCAs:    pool,
		ServerName: "localhost",
		NextProtos: []string{"h2"}, // gRPC refuses connections that do not negotiate ALPN h2
	}
	if clientCN != nil {
		crt, key := ca.issue(t, *clientCN, x509.ExtKeyUsageClientAuth)
		pair, err := tls.X509KeyPair(crt, key)
		require.NoError(t, err)
		cfg.Certificates = []tls.Certificate{pair}
	}
	return cfg
}

// serve accepts a single connection and runs the gRPC server-side TLS handshake on it.
func serve(t *testing.T, creds credentials.TransportCredentials) (addr string, serverErr <-chan error) {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	t.Cleanup(func() { _ = ln.Close() })
	errCh := make(chan error, 1)
	go func() {
		conn, err := ln.Accept()
		if err != nil {
			errCh <- err
			return
		}
		defer conn.Close()
		_ = conn.SetDeadline(time.Now().Add(10 * time.Second))
		secured, _, err := creds.ServerHandshake(conn)
		if err == nil {
			// Let the client observe the end of the handshake before closing.
			_ = secured.Close()
		}
		errCh <- err
	}()
	return ln.Addr().String(), errCh
}

// connect dials the server and returns the group the client negotiated. The server-side outcome is
// authoritative for client-certificate rejection, since in TLS 1.3 the client finishes its
// handshake before the server verifies the client certificate.
func connect(t *testing.T, creds credentials.TransportCredentials, cfg *tls.Config) (tls.CurveID, error) {
	t.Helper()
	addr, serverErr := serve(t, creds)
	conn, err := net.DialTimeout("tcp", addr, 10*time.Second)
	require.NoError(t, err)
	defer conn.Close()
	_ = conn.SetDeadline(time.Now().Add(10 * time.Second))
	c := tls.Client(conn, cfg)
	clientErr := c.Handshake()
	srvErr := <-serverErr
	if srvErr != nil {
		return 0, srvErr
	}
	if clientErr != nil {
		return 0, clientErr
	}
	return c.ConnectionState().CurveID, nil
}

func startCreds(t *testing.T, dir string) credentials.TransportCredentials {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	creds, err := newServerTransportCredentialsFromDir(ctx, dir)
	require.NoError(t, err)
	return creds
}

func strPtr(s string) *string { return &s }

// Each profile restricts the negotiated group even over mTLS, and the client certificate pin still holds.
func TestServerTransportCredentialsGroupPerProfile(t *testing.T) {
	tests := []struct {
		profile string
		want    tls.CurveID
	}{
		{"", tls.X25519MLKEM768}, // unset selects COMPATIBLE
		{"COMPATIBLE", tls.X25519MLKEM768},
		{"NIST_2024", tls.SecP256r1MLKEM768},
		{"NIST_2024_TLS_13_ONLY", tls.SecP256r1MLKEM768},
		{"CNSA_10", tls.SecP384r1MLKEM1024},
		{"not-a-profile", tls.X25519MLKEM768}, // invalid falls back to COMPATIBLE
	}
	for _, tc := range tests {
		t.Run("profile="+tc.profile, func(t *testing.T) {
			t.Setenv(tlsprofile.ProfileEnvVar, tc.profile)
			ca := newTestCA(t)
			dir := t.TempDir()
			writeServerDir(t, dir, ca)
			creds := startCreds(t, dir)

			cfg := clientConfig(t, ca, strPtr(k8sCloudOperatorClientCertCN))
			cfg.CurvePreferences = tlsprofile.ClientCurvePreferences()
			got, err := connect(t, creds, cfg)
			require.NoError(t, err)
			assert.Equal(t, tc.want, got)
		})
	}
}

// A classical-only client is accepted unless the profile forbids classical groups outright.
func TestServerTransportCredentialsClassicalClient(t *testing.T) {
	ca := newTestCA(t)
	dir := t.TempDir()
	writeServerDir(t, dir, ca)
	classical := func() *tls.Config {
		cfg := clientConfig(t, ca, strPtr(k8sCloudOperatorClientCertCN))
		cfg.CurvePreferences = []tls.CurveID{tls.CurveP256}
		return cfg
	}

	t.Setenv(tlsprofile.ProfileEnvVar, "COMPATIBLE")
	got, err := connect(t, startCreds(t, dir), classical())
	require.NoError(t, err)
	assert.Equal(t, tls.CurveP256, got)

	t.Setenv(tlsprofile.ProfileEnvVar, "CNSA_10")
	_, err = connect(t, startCreds(t, dir), classical())
	assert.Error(t, err, "CNSA_10 allows only P-384 among the classical groups")
}

// The post-quantum change must not weaken the existing mTLS requirements.
func TestServerTransportCredentialsClientIdentity(t *testing.T) {
	t.Setenv(tlsprofile.ProfileEnvVar, "COMPATIBLE")
	ca := newTestCA(t)
	dir := t.TempDir()
	writeServerDir(t, dir, ca)
	creds := startCreds(t, dir)

	t.Run("no client certificate is refused", func(t *testing.T) {
		_, err := connect(t, creds, clientConfig(t, ca, nil))
		assert.Error(t, err)
	})
	t.Run("wrong CommonName is refused", func(t *testing.T) {
		_, err := connect(t, creds, clientConfig(t, ca, strPtr("someone-else")))
		assert.Error(t, err)
	})
	t.Run("client signed by an untrusted CA is refused", func(t *testing.T) {
		other := newTestCA(t)
		cfg := clientConfig(t, ca, nil)
		crt, key := other.issue(t, k8sCloudOperatorClientCertCN, x509.ExtKeyUsageClientAuth)
		pair, err := tls.X509KeyPair(crt, key)
		require.NoError(t, err)
		cfg.Certificates = []tls.Certificate{pair}
		_, err = connect(t, creds, cfg)
		assert.Error(t, err)
	})
}

// TLS 1.2 clients keep working: the groups only matter for TLS 1.3 negotiation, and MinVersion is unchanged.
func TestServerTransportCredentialsTLS12Client(t *testing.T) {
	t.Setenv(tlsprofile.ProfileEnvVar, "COMPATIBLE")
	ca := newTestCA(t)
	dir := t.TempDir()
	writeServerDir(t, dir, ca)

	cfg := clientConfig(t, ca, strPtr(k8sCloudOperatorClientCertCN))
	cfg.MinVersion = tls.VersionTLS12
	cfg.MaxVersion = tls.VersionTLS12
	cfg.CipherSuites = []uint16{tls.TLS_ECDHE_ECDSA_WITH_AES_256_GCM_SHA384}
	_, err := connect(t, startCreds(t, dir), cfg)
	assert.NoError(t, err)
}

// After a certificate and CA rotation, new handshakes use the new identity and still obey the profile,
// and clients holding certificates from the old CA are refused.
func TestServerTransportCredentialsRotationKeepsProfile(t *testing.T) {
	t.Setenv(tlsprofile.ProfileEnvVar, "CNSA_10")
	oldCA := newTestCA(t)
	dir := t.TempDir()
	writeServerDir(t, dir, oldCA)
	creds := startCreds(t, dir)

	full := func(ca *testCA) *tls.Config {
		cfg := clientConfig(t, ca, strPtr(k8sCloudOperatorClientCertCN))
		cfg.CurvePreferences = tlsprofile.ClientCurvePreferences()
		return cfg
	}
	got, err := connect(t, creds, full(oldCA))
	require.NoError(t, err)
	require.Equal(t, tls.SecP384r1MLKEM1024, got)

	// Rotate: new CA, new server identity.
	newCA := newTestCA(t)
	writeServerDir(t, dir, newCA)

	require.Eventually(t, func() bool {
		got, err := connect(t, creds, full(newCA))
		return err == nil && got == tls.SecP384r1MLKEM1024
	}, 15*time.Second, 200*time.Millisecond, "rotated identity must be served with the profile's groups")

	_, err = connect(t, creds, full(oldCA))
	assert.Error(t, err, "a client from the retired CA must be refused after rotation")

	// The profile still applies: a classical P-256-only client is refused under CNSA_10.
	cfg := full(newCA)
	cfg.CurvePreferences = []tls.CurveID{tls.CurveP256}
	_, err = connect(t, creds, cfg)
	assert.Error(t, err)
}
