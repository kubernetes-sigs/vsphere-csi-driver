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

package vsphere

import (
	"context"
	"crypto/tls"
	"strconv"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/vmware/govmomi/simulator"

	"sigs.k8s.io/vsphere-csi-driver/v3/pkg/common/tlsprofile"
	commontypes "sigs.k8s.io/vsphere-csi-driver/v3/pkg/common/types"
)

// TestNewClientAdvertisesFullGroupList connects NewClient to a TLS vcsim and records the key exchange
// groups in the ClientHello it actually sends. The vCenter connection must offer the complete list,
// not only the group the client precomputes a key_share for.
func TestNewClientAdvertisesFullGroupList(t *testing.T) {
	ctx := context.Background()

	model := simulator.VPX()
	require.NoError(t, model.Create())
	defer model.Remove()

	var (
		mu     sync.Mutex
		offers [][]tls.CurveID
	)
	model.Service.TLS = &tls.Config{
		GetConfigForClient: func(chi *tls.ClientHelloInfo) (*tls.Config, error) {
			mu.Lock()
			defer mu.Unlock()
			offers = append(offers, append([]tls.CurveID(nil), chi.SupportedCurves...))
			return nil, nil
		},
	}
	s := model.Service.NewServer()
	defer s.Close()

	port, err := strconv.Atoi(s.URL.Port())
	require.NoError(t, err)
	vc := &VirtualCenter{
		Config: &VirtualCenterConfig{
			Scheme:   "https",
			Host:     commontypes.NewFQDN(s.URL.Hostname()),
			Port:     port,
			Username: "user",
			Password: "pass",
			Insecure: true,
		},
		ClientMutex: &sync.Mutex{},
	}

	client, _, err := vc.NewClient(ctx, "pqc-tls-test")
	require.NoError(t, err)
	require.NotNil(t, client)

	mu.Lock()
	defer mu.Unlock()
	require.NotEmpty(t, offers, "vcsim saw no TLS ClientHello")
	for _, got := range offers {
		assert.Equal(t, tlsprofile.ClientCurvePreferences(), got)
	}
}

// The group list is applied to the shared transport, so every connection derived from the client
// (service clients such as vsan) advertises it too, and it does not replace the verification settings.
func TestNewClientKeepsTLSVerificationSettings(t *testing.T) {
	ctx := context.Background()

	model := simulator.VPX()
	require.NoError(t, model.Create())
	defer model.Remove()
	model.Service.TLS = new(tls.Config)
	s := model.Service.NewServer()
	defer s.Close()

	port, err := strconv.Atoi(s.URL.Port())
	require.NoError(t, err)

	t.Run("insecure", func(t *testing.T) {
		vc := &VirtualCenter{
			Config: &VirtualCenterConfig{
				Scheme: "https", Host: commontypes.NewFQDN(s.URL.Hostname()), Port: port,
				Username: "user", Password: "pass", Insecure: true,
			},
			ClientMutex: &sync.Mutex{},
		}
		client, _, err := vc.NewClient(ctx, "pqc-tls-test")
		require.NoError(t, err)
		cfg := client.Client.DefaultTransport().TLSClientConfig
		assert.True(t, cfg.InsecureSkipVerify)
		assert.Equal(t, tlsprofile.ClientCurvePreferences(), cfg.CurvePreferences)
	})

	t.Run("verification stays on and the server certificate is rejected", func(t *testing.T) {
		vc := &VirtualCenter{
			Config: &VirtualCenterConfig{
				Scheme: "https", Host: commontypes.NewFQDN(s.URL.Hostname()), Port: port,
				Username: "user", Password: "pass", Insecure: false,
			},
			ClientMutex: &sync.Mutex{},
		}
		_, _, err := vc.NewClient(ctx, "pqc-tls-test")
		assert.Error(t, err, "the simulator certificate is not trusted, so verification must still fail")
	})

	t.Run("thumbprint mode is preserved", func(t *testing.T) {
		vc := &VirtualCenter{
			Config: &VirtualCenterConfig{
				Scheme: "https", Host: commontypes.NewFQDN(s.URL.Hostname()), Port: port,
				Username: "user", Password: "pass", Insecure: false,
				Thumbprint: "00:00:00:00:00:00:00:00:00:00:00:00:00:00:00:00:00:00:00:00",
			},
			ClientMutex: &sync.Mutex{},
		}
		_, _, err := vc.NewClient(ctx, "pqc-tls-test")
		assert.Error(t, err, "a wrong thumbprint must still be rejected")
	})
}
