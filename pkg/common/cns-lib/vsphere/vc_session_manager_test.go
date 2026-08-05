package vsphere_test

import (
	"context"
	"crypto/x509"
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"sync/atomic"
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

		// Note this covers options.Timeout (the http.Client timeout), not the
		// caller's context. Cancellation is covered by TestGetSharedTokenHonoursContext.
		t.Run("should respect the options timeout", func(t *testing.T) {
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

// TestGetSharedTokenHonoursContext checks that the caller's context actually
// reaches the request. options.Timeout caps the total call but cannot abort one
// early, so without the context bound to the request a cancelled caller — a pod
// shutting down, or an operation whose deadline has passed — would still block
// for the full timeout.
func TestGetSharedTokenHonoursContext(t *testing.T) {
	// Blocks until the client goes away, so the only way out is cancellation.
	released := make(chan struct{})
	server := httptest.NewServer(http.HandlerFunc(
		func(w http.ResponseWriter, r *http.Request) {
			select {
			case <-r.Context().Done():
			case <-released:
			}
		}))
	t.Cleanup(func() {
		close(released)
		server.Close()
	})

	// Runs the call off the test goroutine so an ignored context surfaces as a
	// fast, explicit failure instead of hanging until the go test deadline.
	callWithContext := func(t *testing.T, ctx context.Context) error {
		t.Helper()
		done := make(chan error, 1)
		go func() {
			_, err := vclib.GetSharedToken(ctx, vclib.SharedTokenOptions{
				URL:     server.URL,
				Token:   validToken,
				Timeout: time.Hour, // must not be what unblocks us
			})
			done <- err
		}()

		select {
		case err := <-done:
			return err
		case <-time.After(10 * time.Second):
			t.Fatal("GetSharedToken did not return well after its context was done; " +
				"the caller's context is not reaching the request")
			return nil
		}
	}

	t.Run("should abort when the context deadline passes", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
		defer cancel()

		err := callWithContext(t, ctx)
		require.Error(t, err)
		assert.ErrorIs(t, err, context.DeadlineExceeded)
	})

	t.Run("should abort when the context is cancelled", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		time.AfterFunc(50*time.Millisecond, cancel)

		err := callWithContext(t, ctx)
		require.Error(t, err)
		assert.ErrorIs(t, err, context.Canceled)
	})
}

// TestGetSharedTokenReleasesConnections guards against connections outliving the
// call that opened them. Two things are needed for that and both have been wrong
// here: the response body must be closed on every exit path (not just the success
// one), and the per-call Transport must have its idle connections closed, since a
// zero-value Transport has no IdleConnTimeout and would otherwise hold the socket
// and its goroutines open indefinitely. GetSharedToken runs on every login and
// reconnect, so anything left behind accumulates.
func TestGetSharedTokenReleasesConnections(t *testing.T) {
	const requests = 10

	for _, tc := range []struct {
		name   string
		status int
		body   string
	}{
		{name: "non-200 response", status: http.StatusForbidden, body: "denied"},
		{name: "undecodable response", status: http.StatusOK, body: "not a json"},
		{name: "empty token response", status: http.StatusOK, body: `{"token":""}`},
		{name: "valid response", status: http.StatusOK, body: `{"token":"` + validResponse + `"}`},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var opened, closed int64
			server := httptest.NewUnstartedServer(http.HandlerFunc(
				func(w http.ResponseWriter, r *http.Request) {
					w.WriteHeader(tc.status)
					_, _ = w.Write([]byte(tc.body))
				}))
			server.Config.ConnState = func(_ net.Conn, state http.ConnState) {
				switch state {
				case http.StateNew:
					atomic.AddInt64(&opened, 1)
				case http.StateClosed:
					atomic.AddInt64(&closed, 1)
				}
			}
			server.Start()
			t.Cleanup(server.Close)

			for i := 0; i < requests; i++ {
				_, _ = vclib.GetSharedToken(context.Background(), vclib.SharedTokenOptions{
					URL:   server.URL,
					Token: validToken,
				})
			}

			// Assert the invariant (everything opened is released) rather than a
			// connection count, so this still holds if the client is later reworked
			// to reuse connections instead of building a Transport per call.
			totalOpened := atomic.LoadInt64(&opened)
			require.GreaterOrEqual(t, totalOpened, int64(1),
				"expected the calls to open at least one connection")

			// Teardown is observed asynchronously by the server, so poll.
			assert.Eventually(t, func() bool {
				return atomic.LoadInt64(&closed) == totalOpened
			}, 10*time.Second, 20*time.Millisecond,
				"expected all %d connections to be closed, saw %d; a connection left "+
					"established means a response body or an idle Transport was leaked",
				totalOpened, atomic.LoadInt64(&closed))
		})
	}
}
