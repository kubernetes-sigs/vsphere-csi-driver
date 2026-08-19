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
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"math/big"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

// makeCertWithCN returns a self-signed certificate with the given Subject
// CommonName.
func makeCertWithCN(t *testing.T, cn string) *x509.Certificate {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	assert.NoError(t, err)

	tmpl := &x509.Certificate{
		SerialNumber: big.NewInt(1),
		Subject:      pkix.Name{CommonName: cn},
		NotBefore:    time.Now().Add(-time.Hour),
		NotAfter:     time.Now().Add(time.Hour),
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, &key.PublicKey, key)
	assert.NoError(t, err)

	cert, err := x509.ParseCertificate(der)
	assert.NoError(t, err)
	return cert
}

// connStateWithChain builds a tls.ConnectionState carrying the given
// certificate as the sole verified chain.
func connStateWithChain(certs ...*x509.Certificate) tls.ConnectionState {
	if len(certs) == 0 {
		return tls.ConnectionState{}
	}
	return tls.ConnectionState{VerifiedChains: [][]*x509.Certificate{certs}}
}

// TestVerifyK8sCloudOperatorClientConnection verifies that only a client
// certificate whose CommonName matches k8sCloudOperatorClientCertCN is
// authorized, even though a certificate chain reaching this callback has
// already passed standard CA verification - the CommonName pin is the second,
// independent check that stops a different CA-signed identity (e.g. this
// service's own server certificate) from also being usable as a caller.
func TestVerifyK8sCloudOperatorClientConnection(t *testing.T) {
	t.Run("no verified chains", func(t *testing.T) {
		err := verifyK8sCloudOperatorClientConnection(tls.ConnectionState{})
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "no verified client certificate chain")
	})

	t.Run("empty first chain", func(t *testing.T) {
		err := verifyK8sCloudOperatorClientConnection(
			tls.ConnectionState{VerifiedChains: [][]*x509.Certificate{{}}})
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "no verified client certificate chain")
	})

	t.Run("matching CommonName is authorized", func(t *testing.T) {
		cert := makeCertWithCN(t, k8sCloudOperatorClientCertCN)
		err := verifyK8sCloudOperatorClientConnection(connStateWithChain(cert))
		assert.NoError(t, err)
	})

	t.Run("wrong CommonName is rejected even though CA-valid", func(t *testing.T) {
		cert := makeCertWithCN(t, "some-other-identity")
		err := verifyK8sCloudOperatorClientConnection(connStateWithChain(cert))
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "unauthorized client certificate CommonName")
		assert.Contains(t, err.Error(), "some-other-identity")
	})

	t.Run("server's own CommonName is rejected", func(t *testing.T) {
		cert := makeCertWithCN(t, "vmware-system-csi-k8scloudoperator-server")
		err := verifyK8sCloudOperatorClientConnection(connStateWithChain(cert))
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "unauthorized client certificate CommonName")
	})
}
