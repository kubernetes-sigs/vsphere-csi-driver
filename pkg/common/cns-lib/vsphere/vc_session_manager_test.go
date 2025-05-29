package vsphere_test

import (
	"context"
	"crypto/x509"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	vclib "sigs.k8s.io/vsphere-csi-driver/v3/pkg/common/cns-lib/vsphere"
)

const (
	validToken    = "validtoken"
	validResponse = "a-valid-response"
)

var (
	handler = http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		authZHdr := r.Header.Get("Authorization")
		if authZHdr != fmt.Sprintf("Bearer %s", validToken) {
			w.WriteHeader(http.StatusForbidden)
			return
		}
		if r.URL.Path == "/timeout" {
			time.Sleep(15 * time.Millisecond)
			w.WriteHeader(http.StatusOK)
			return
		}
		if r.URL.Path == "/invalid-token" {
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte("not a json"))
			return
		}
		if r.URL.Path == "/session" {
			token := vclib.SharedSessionResponse{
				Token: validResponse,
			}
			response, err := json.Marshal(&token)
			if err != nil {
				w.WriteHeader(http.StatusInternalServerError)
				return
			}
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write(response)
			return
		}
		if r.URL.Path == "/empty" {
			token := vclib.SharedSessionResponse{
				Token: "",
			}
			response, err := json.Marshal(&token)
			if err != nil {
				w.WriteHeader(http.StatusInternalServerError)
				return
			}
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write(response)
			return
		}
		w.WriteHeader(http.StatusNotFound)
	})
)

func TestGetSharedToken(t *testing.T) {
	ctx := context.Background()
	t.Run("when options are invalid", func(t *testing.T) {
		t.Run("should fail when no URL is sent", func(t *testing.T) {
			_, err := vclib.GetSharedToken(ctx, vclib.SharedTokenOptions{})
			assert.ErrorContains(t, err, "URL of session manager cannot be empty")
		})

		t.Run("should fail when no token is passed and SA token cannot be read", func(t *testing.T) {
			_, err := vclib.GetSharedToken(ctx, vclib.SharedTokenOptions{
				URL: "http://something.tld/lala",
			})
			assert.ErrorContains(t, err, "failed reading token from service account: "+
				"open /var/run/secrets/kubernetes.io/serviceaccount/token: no such file or directory")
		})

		t.Run("should fail when passed URL is invalid", func(t *testing.T) {
			_, err := vclib.GetSharedToken(ctx, vclib.SharedTokenOptions{
				URL:   "https://some-session-manager.tld:xxxxx/session",
				Token: "anything",
			})
			assert.ErrorContains(t, err, "invalid port")
		})
	})

	t.Run("when using a valid session manager", func(t *testing.T) {
		server := httptest.NewTLSServer(handler)

		certpool := x509.NewCertPool()
		certpool.AddCert(server.Certificate())
		t.Cleanup(server.Close)

		t.Run("should respect the timeout", func(t *testing.T) {
			reqURL := fmt.Sprintf("%s/timeout", server.URL)
			_, err := vclib.GetSharedToken(ctx, vclib.SharedTokenOptions{
				URL:                 reqURL,
				TrustedCertificates: certpool,
				Token:               validToken,
				Timeout:             5 * time.Millisecond,
			})
			assert.ErrorContains(t, err, "context deadline exceeded")
		})
		t.Run("should fail when calling an invalid path", func(t *testing.T) {
			_, err := vclib.GetSharedToken(ctx, vclib.SharedTokenOptions{
				URL:                 server.URL,
				TrustedCertificates: certpool,
				Token:               validToken,
			})
			assert.ErrorContains(t, err, "404 Not Found")
		})
		t.Run("should fail when an empty token is returned", func(t *testing.T) {
			reqURL := fmt.Sprintf("%s/empty", server.URL)
			_, err := vclib.GetSharedToken(ctx, vclib.SharedTokenOptions{
				URL:                 reqURL,
				TrustedCertificates: certpool,
				Token:               validToken,
			})
			assert.ErrorContains(t, err, "returned vc session token is empty")
		})

		t.Run("should fail when an invalid json is returned", func(t *testing.T) {
			reqURL := fmt.Sprintf("%s/invalid-token", server.URL)
			_, err := vclib.GetSharedToken(ctx, vclib.SharedTokenOptions{
				URL:                 reqURL,
				TrustedCertificates: certpool,
				Token:               validToken,
			})
			assert.ErrorContains(t, err, "failed decoding vc session manager response")
		})

		t.Run("should fail when no cert is passed and insecureskipverify is false", func(t *testing.T) {
			reqURL := fmt.Sprintf("%s/session", server.URL)
			_, err := vclib.GetSharedToken(ctx, vclib.SharedTokenOptions{
				URL:   reqURL,
				Token: validToken,
			})
			assert.ErrorContains(t, err, "tls: failed to verify certificate: x509")
		})

		t.Run("should return a valid token for the right request and insecureskip=true", func(t *testing.T) {
			reqURL := fmt.Sprintf("%s/session", server.URL)
			token, err := vclib.GetSharedToken(ctx, vclib.SharedTokenOptions{
				URL:                reqURL,
				InsecureSkipVerify: true,
				Token:              validToken,
			})
			assert.NoError(t, err)
			assert.Equal(t, validResponse, token)
		})

		t.Run("should return a valid token for the right request and cert", func(t *testing.T) {
			reqURL := fmt.Sprintf("%s/session", server.URL)
			token, err := vclib.GetSharedToken(ctx, vclib.SharedTokenOptions{
				URL:                 reqURL,
				TrustedCertificates: certpool,
				Token:               validToken,
			})
			assert.NoError(t, err)
			assert.Equal(t, validResponse, token)
		})

		t.Run("should return a valid token when using a file as a token", func(t *testing.T) {
			tokenFile, err := os.CreateTemp("", "")
			require.NoError(t, err)
			require.NoError(t, tokenFile.Close())
			require.NoError(t, os.WriteFile(tokenFile.Name(), []byte(validToken), 0755))

			reqURL := fmt.Sprintf("%s/session", server.URL)
			token, err := vclib.GetSharedToken(ctx, vclib.SharedTokenOptions{
				URL:                 reqURL,
				TrustedCertificates: certpool,
				TokenFile:           tokenFile.Name(),
			})
			assert.NoError(t, err)
			assert.Equal(t, validResponse, token)
		})
	})

}
