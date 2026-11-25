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

package tlsprofile

import (
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
)

func TestParseProfile(t *testing.T) {
	tests := []struct {
		in      string
		want    Profile
		wantErr bool
	}{
		{"", ProfileCompatible, false},
		{"COMPATIBLE", ProfileCompatible, false},
		{" nist_2024 ", ProfileNIST2024, false},
		{"NIST_2024_TLS_13_ONLY", ProfileNIST2024TLS13Only, false},
		{"cnsa_10", ProfileCNSA10, false},
		{"bogus", ProfileCompatible, true},
	}
	for _, tc := range tests {
		got, err := ParseProfile(tc.in)
		assert.Equal(t, tc.want, got, tc.in)
		assert.Equal(t, tc.wantErr, err != nil, tc.in)
	}
}

func TestProfileFromEnv(t *testing.T) {
	t.Setenv(ProfileEnvVar, "CNSA_10")
	p, err := ProfileFromEnv()
	require.NoError(t, err)
	assert.Equal(t, ProfileCNSA10, p)

	t.Setenv(ProfileEnvVar, "nope")
	p, err = ProfileFromEnv()
	assert.Error(t, err)
	assert.Equal(t, DefaultProfile, p)
}

func TestServerCurvePreferences(t *testing.T) {
	assert.Equal(t, []tls.CurveID{
		tls.X25519MLKEM768, tls.SecP256r1MLKEM768, tls.SecP384r1MLKEM1024, tls.MLKEM1024,
		tls.CurveP256, tls.CurveP384, tls.CurveP521,
	}, ServerCurvePreferences(ProfileCompatible))
	assert.Equal(t, []tls.CurveID{
		tls.SecP256r1MLKEM768, tls.SecP384r1MLKEM1024,
		tls.CurveP256, tls.CurveP384, tls.CurveP521,
	}, ServerCurvePreferences(ProfileNIST2024))
	assert.Equal(t, ServerCurvePreferences(ProfileNIST2024), ServerCurvePreferences(ProfileNIST2024TLS13Only))
	assert.Equal(t, []tls.CurveID{tls.SecP384r1MLKEM1024, tls.MLKEM1024, tls.CurveP384},
		ServerCurvePreferences(ProfileCNSA10))
	assert.Equal(t, ServerCurvePreferences(DefaultProfile), ServerCurvePreferences("unknown"))
}

func TestReturnedSlicesAreCopies(t *testing.T) {
	s := ServerCurvePreferences(ProfileCompatible)
	s[0] = tls.CurveP521
	assert.Equal(t, tls.X25519MLKEM768, ServerCurvePreferences(ProfileCompatible)[0])
	c := ClientCurvePreferences()
	c[0] = tls.CurveP521
	assert.Equal(t, tls.X25519MLKEM768, ClientCurvePreferences()[0])
}

func TestEveryServerGroupIsAdvertisedByClient(t *testing.T) {
	client := map[tls.CurveID]bool{}
	for _, g := range ClientCurvePreferences() {
		client[g] = true
	}
	for p := range serverGroups {
		for _, g := range ServerCurvePreferences(p) {
			assert.True(t, client[g], "profile %s group %v not advertised by client", p, g)
		}
	}
}

func selfSignedCert(t *testing.T) tls.Certificate {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	require.NoError(t, err)
	tmpl := &x509.Certificate{
		SerialNumber: big.NewInt(1),
		Subject:      pkix.Name{CommonName: "localhost"},
		NotBefore:    time.Now().Add(-time.Hour),
		NotAfter:     time.Now().Add(time.Hour),
		DNSNames:     []string{"localhost"},
		IPAddresses:  []net.IP{net.ParseIP("127.0.0.1")},
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, &key.PublicKey, key)
	require.NoError(t, err)
	return tls.Certificate{Certificate: [][]byte{der}, PrivateKey: key}
}

// handshake runs a TLS handshake over a loopback TCP connection and returns the
// group negotiated with the server. A real socket is used because net.Pipe is
// unbuffered and deadlocks once the server writes post-handshake messages.
func handshake(t *testing.T, serverCfg, clientCfg *tls.Config) (tls.CurveID, error) {
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
		srvErr <- tls.Server(conn, serverCfg).Handshake()
	}()

	conn, err := net.DialTimeout("tcp", ln.Addr().String(), 10*time.Second)
	require.NoError(t, err)
	defer conn.Close()
	_ = conn.SetDeadline(time.Now().Add(10 * time.Second))
	cli := tls.Client(conn, clientCfg)
	if err := cli.Handshake(); err != nil {
		return 0, err
	}
	if err := <-srvErr; err != nil {
		return 0, err
	}
	return cli.ConnectionState().CurveID, nil
}

func TestHandshakeNegotiatedGroup(t *testing.T) {
	cert := selfSignedCert(t)
	tests := []struct {
		profile Profile
		want    tls.CurveID
	}{
		{ProfileCompatible, tls.X25519MLKEM768},
		{ProfileNIST2024, tls.SecP256r1MLKEM768},
		{ProfileNIST2024TLS13Only, tls.SecP256r1MLKEM768},
		{ProfileCNSA10, tls.SecP384r1MLKEM1024},
	}
	for _, tc := range tests {
		t.Run(string(tc.profile), func(t *testing.T) {
			serverCfg := &tls.Config{Certificates: []tls.Certificate{cert}, MinVersion: tls.VersionTLS13}
			ApplyServer(serverCfg, tc.profile)
			clientCfg := &tls.Config{InsecureSkipVerify: true, MinVersion: tls.VersionTLS13} //nolint:gosec
			ApplyClient(clientCfg)
			got, err := handshake(t, serverCfg, clientCfg)
			require.NoError(t, err)
			assert.Equal(t, tc.want, got, "negotiated %v", got)
		})
	}
}

// A client that only offers classical groups must still be able to connect to a
// server on a profile that allows them, and be refused by CNSA_10.
func TestHandshakeClassicalClient(t *testing.T) {
	cert := selfSignedCert(t)
	classical := func() *tls.Config {
		return &tls.Config{
			InsecureSkipVerify: true, //nolint:gosec
			MinVersion:         tls.VersionTLS13,
			CurvePreferences:   []tls.CurveID{tls.CurveP256},
		}
	}

	serverCfg := &tls.Config{Certificates: []tls.Certificate{cert}, MinVersion: tls.VersionTLS13}
	ApplyServer(serverCfg, ProfileCompatible)
	got, err := handshake(t, serverCfg, classical())
	require.NoError(t, err)
	assert.Equal(t, tls.CurveP256, got)

	serverCfg = &tls.Config{Certificates: []tls.Certificate{cert}, MinVersion: tls.VersionTLS13}
	ApplyServer(serverCfg, ProfileCNSA10)
	_, err = handshake(t, serverCfg, classical())
	assert.Error(t, err)
}

// Pure MLKEM1024 is part of the COMPATIBLE and CNSA_10 server lists and of the client list, so a
// client that offers only it must negotiate it with those profiles and be refused by NIST_2024.
func TestHandshakePureMLKEM1024(t *testing.T) {
	cert := selfSignedCert(t)
	only := func() *tls.Config {
		return &tls.Config{
			InsecureSkipVerify: true, //nolint:gosec
			MinVersion:         tls.VersionTLS13,
			CurvePreferences:   []tls.CurveID{tls.MLKEM1024},
		}
	}
	for _, p := range []Profile{ProfileCompatible, ProfileCNSA10} {
		serverCfg := &tls.Config{Certificates: []tls.Certificate{cert}, MinVersion: tls.VersionTLS13}
		ApplyServer(serverCfg, p)
		got, err := handshake(t, serverCfg, only())
		require.NoError(t, err, string(p))
		assert.Equal(t, tls.MLKEM1024, got, string(p))
	}
	serverCfg := &tls.Config{Certificates: []tls.Certificate{cert}, MinVersion: tls.VersionTLS13}
	ApplyServer(serverCfg, ProfileNIST2024)
	_, err := handshake(t, serverCfg, only())
	assert.Error(t, err)
}
