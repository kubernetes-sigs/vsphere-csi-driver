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

package admissionhandler

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"math/big"
	"net"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"sigs.k8s.io/vsphere-csi-driver/v3/pkg/common/tlsprofile"
)

func TestApplyServerTLSProfile(t *testing.T) {
	ctx := context.Background()

	t.Run("defaults to COMPATIBLE when TLS_PROFILE is unset", func(t *testing.T) {
		t.Setenv(tlsprofile.ProfileEnvVar, "")
		cfg := &tls.Config{}
		applyServerTLSProfile(ctx, cfg)
		assert.Equal(t, tlsprofile.ServerCurvePreferences(tlsprofile.ProfileCompatible), cfg.CurvePreferences)
	})

	t.Run("honours the configured profile", func(t *testing.T) {
		t.Setenv(tlsprofile.ProfileEnvVar, "CNSA_10")
		cfg := &tls.Config{}
		applyServerTLSProfile(ctx, cfg)
		assert.Equal(t, tlsprofile.ServerCurvePreferences(tlsprofile.ProfileCNSA10), cfg.CurvePreferences)
	})

	t.Run("falls back to the default profile on an invalid value", func(t *testing.T) {
		t.Setenv(tlsprofile.ProfileEnvVar, "not-a-profile")
		cfg := &tls.Config{}
		applyServerTLSProfile(ctx, cfg)
		assert.Equal(t, tlsprofile.ServerCurvePreferences(tlsprofile.DefaultProfile), cfg.CurvePreferences)
	})
}

func testServerCert(t *testing.T) tls.Certificate {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	require.NoError(t, err)
	tmpl := &x509.Certificate{
		SerialNumber: big.NewInt(1),
		Subject:      pkix.Name{CommonName: "localhost"},
		NotBefore:    time.Now().Add(-time.Hour),
		NotAfter:     time.Now().Add(time.Hour),
		DNSNames:     []string{"localhost"},
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, &key.PublicKey, key)
	require.NoError(t, err)
	return tls.Certificate{Certificate: [][]byte{der}, PrivateKey: key}
}

// webhookHandshake runs a handshake against cfg over loopback TCP and returns the negotiated TLS version and group.
func webhookHandshake(t *testing.T, cfg, client *tls.Config) (uint16, tls.CurveID, error) {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	defer ln.Close()
	srvErr := make(chan error, 1)
	go func() {
		conn, err := ln.Accept()
		if err != nil {
			srvErr <- err
			return
		}
		defer conn.Close()
		_ = conn.SetDeadline(time.Now().Add(10 * time.Second))
		srvErr <- tls.Server(conn, cfg).Handshake()
	}()
	conn, err := net.DialTimeout("tcp", ln.Addr().String(), 10*time.Second)
	require.NoError(t, err)
	defer conn.Close()
	_ = conn.SetDeadline(time.Now().Add(10 * time.Second))
	c := tls.Client(conn, client)
	if err := c.Handshake(); err != nil {
		return 0, 0, err
	}
	if err := <-srvErr; err != nil {
		return 0, 0, err
	}
	st := c.ConnectionState()
	return st.Version, st.CurveID, nil
}

// The webhook keeps its existing cipher suites and TLS 1.2 minimum next to the new groups.
func TestNewWebhookTLSConfigKeepsExistingSettings(t *testing.T) {
	t.Setenv(tlsprofile.ProfileEnvVar, "NIST_2024")
	cfg := newWebhookTLSConfig(context.Background(), testServerCert(t))

	assert.Equal(t, uint16(tls.VersionTLS12), cfg.MinVersion)
	assert.Equal(t, []uint16{
		tls.TLS_ECDHE_ECDSA_WITH_AES_256_GCM_SHA384,
		tls.TLS_ECDHE_RSA_WITH_AES_256_GCM_SHA384,
		tls.TLS_ECDHE_RSA_WITH_AES_128_GCM_SHA256,
		tls.TLS_ECDHE_ECDSA_WITH_AES_128_GCM_SHA256,
	}, cfg.CipherSuites)
	assert.Equal(t, tlsprofile.ServerCurvePreferences(tlsprofile.ProfileNIST2024), cfg.CurvePreferences)
	assert.Len(t, cfg.Certificates, 1)
}

func TestWebhookHandshakePerProfile(t *testing.T) {
	cert := testServerCert(t)
	full := func() *tls.Config {
		return &tls.Config{InsecureSkipVerify: true, CurvePreferences: tlsprofile.ClientCurvePreferences()} //nolint:gosec
	}
	tests := []struct {
		profile string
		want    tls.CurveID
	}{
		{"COMPATIBLE", tls.X25519MLKEM768},
		{"NIST_2024", tls.SecP256r1MLKEM768},
		{"NIST_2024_TLS_13_ONLY", tls.SecP256r1MLKEM768},
		{"CNSA_10", tls.SecP384r1MLKEM1024},
	}
	for _, tc := range tests {
		t.Run(tc.profile, func(t *testing.T) {
			t.Setenv(tlsprofile.ProfileEnvVar, tc.profile)
			ver, curve, err := webhookHandshake(t, newWebhookTLSConfig(context.Background(), cert), full())
			require.NoError(t, err)
			assert.Equal(t, uint16(tls.VersionTLS13), ver)
			assert.Equal(t, tc.want, curve)
		})
	}
}

// A TLS 1.2 client (for example an older kube-apiserver) must still be able to reach the webhook, and
// TLS 1.3 clients that only offer a classical group must work unless the profile excludes it.
func TestWebhookHandshakeCompatibility(t *testing.T) {
	cert := testServerCert(t)

	t.Run("TLS 1.2 client", func(t *testing.T) {
		t.Setenv(tlsprofile.ProfileEnvVar, "COMPATIBLE")
		client := &tls.Config{
			InsecureSkipVerify: true, //nolint:gosec
			MaxVersion:         tls.VersionTLS12,
			CipherSuites:       []uint16{tls.TLS_ECDHE_ECDSA_WITH_AES_256_GCM_SHA384},
		}
		ver, _, err := webhookHandshake(t, newWebhookTLSConfig(context.Background(), cert), client)
		require.NoError(t, err)
		assert.Equal(t, uint16(tls.VersionTLS12), ver)
	})

	t.Run("P-256 only client against COMPATIBLE", func(t *testing.T) {
		t.Setenv(tlsprofile.ProfileEnvVar, "COMPATIBLE")
		client := &tls.Config{InsecureSkipVerify: true, CurvePreferences: []tls.CurveID{tls.CurveP256}} //nolint:gosec
		_, curve, err := webhookHandshake(t, newWebhookTLSConfig(context.Background(), cert), client)
		require.NoError(t, err)
		assert.Equal(t, tls.CurveP256, curve)
	})

	t.Run("P-256 only client against CNSA_10 is refused", func(t *testing.T) {
		t.Setenv(tlsprofile.ProfileEnvVar, "CNSA_10")
		client := &tls.Config{InsecureSkipVerify: true, CurvePreferences: []tls.CurveID{tls.CurveP256}} //nolint:gosec
		_, _, err := webhookHandshake(t, newWebhookTLSConfig(context.Background(), cert), client)
		assert.Error(t, err)
	})

	t.Run("HelloRetryRequest: client key_share is a group the profile rejects", func(t *testing.T) {
		// Go sends a key_share for its first preference only; the NIST_2024 server does not allow
		// X25519MLKEM768, so it must ask for a retry with an allowed group.
		t.Setenv(tlsprofile.ProfileEnvVar, "NIST_2024")
		client := &tls.Config{
			InsecureSkipVerify: true, //nolint:gosec
			CurvePreferences:   []tls.CurveID{tls.X25519MLKEM768, tls.SecP256r1MLKEM768, tls.CurveP256},
		}
		_, curve, err := webhookHandshake(t, newWebhookTLSConfig(context.Background(), cert), client)
		require.NoError(t, err)
		assert.Equal(t, tls.SecP256r1MLKEM768, curve)
	})
}
