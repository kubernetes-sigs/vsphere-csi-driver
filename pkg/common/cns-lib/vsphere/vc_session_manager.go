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
	"crypto/x509"
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"net/url"
	"os"
	"strings"
	"time"
)

const (
	saFile = "/var/run/secrets/kubernetes.io/serviceaccount/token"
)

// SharedSessionResponse is the expected structure for a session manager valid
// token response
type SharedSessionResponse struct {
	Token string `json:"token"`
}

// SharedTokenOptions represents the options that can be used when calling vc session manager
type SharedTokenOptions struct {
	// URL is the session manager URL. Eg.: https://my-session-manager/session)
	URL string
	// Token is the authorization token that should be passed to session manager
	Token string
	// TrustedCertificates contains the certpool of certificates trusted by the client
	TrustedCertificates *x509.CertPool
	// InsecureSkipVerify defines if bad certificates requests should be ignored
	InsecureSkipVerify bool
	// Timeout defines the client timeout. Defaults to 5 seconds
	Timeout time.Duration
	// TokenFile defines a file with token content. Defaults to Kubernetes Service Account file
	TokenFile string
}

// newSessionManagerTransport builds the http.Transport used to reach the
// session manager. Split out from GetSharedToken so the Proxy wiring below
// can be asserted on directly: a real request only invokes it, once,
// permanently caching a "no proxy" result if none was configured yet, which
// would make a live end-to-end proxy test order-dependent and unreliable
// given every other test in this file also calls GetSharedToken for real.
func newSessionManagerTransport(options SharedTokenOptions) *http.Transport {
	return &http.Transport{
		// http.DefaultTransport sets this; a bare &http.Transport{} literal
		// does not, and leaves Proxy nil, silently ignoring HTTP_PROXY/
		// HTTPS_PROXY/NO_PROXY. Needed for deployments that reach the session
		// manager through an egress proxy.
		Proxy: http.ProxyFromEnvironment,
		TLSClientConfig: &tls.Config{
			RootCAs:            options.TrustedCertificates,
			InsecureSkipVerify: options.InsecureSkipVerify,
		},
	}
}

// validateSessionManagerURL ensures the bearer token is never sent in
// cleartext over a network. https is always accepted; plain http is accepted
// only when the host is a loopback address (e.g. a sidecar on the same pod).
func validateSessionManagerURL(rawURL string) error {
	u, err := url.Parse(rawURL)
	if err != nil {
		return fmt.Errorf("invalid URL of session manager: %w", err)
	}
	switch strings.ToLower(u.Scheme) {
	case "https":
		return nil
	case "http":
		host := u.Hostname()
		if strings.EqualFold(host, "localhost") {
			return nil
		}
		if ip := net.ParseIP(host); ip != nil && ip.IsLoopback() {
			return nil
		}
		return fmt.Errorf("URL of session manager must use https; plain http is only "+
			"allowed for loopback hosts, got host %q", host)
	default:
		return fmt.Errorf("URL of session manager must use https, got scheme %q", u.Scheme)
	}
}

// GetSharedToken executes an http request on session manager and gets the session manager
// token that can be reused on govmomi sessions
func GetSharedToken(ctx context.Context, options SharedTokenOptions) (string, error) {
	if options.URL == "" {
		return "", fmt.Errorf("URL of session manager cannot be empty")
	}
	// Validate before reading the token so a bad URL never touches the secret.
	if err := validateSessionManagerURL(options.URL); err != nil {
		return "", err
	}

	if options.TokenFile == "" {
		options.TokenFile = saFile
	}

	// If the token is empty, we should use service account from the Pod instead
	if options.Token == "" {
		saValue, err := os.ReadFile(options.TokenFile)
		if err != nil {
			return "", fmt.Errorf("failed reading token from service account: %w", err)
		}
		// A manually created token file commonly picks up a trailing newline
		// (e.g. `echo token > file` rather than `echo -n`). An Authorization
		// header value containing a raw newline isn't sent to the session
		// manager at all: net/http rejects it client-side with "invalid header
		// field value", which gives no hint the actual problem is the file's
		// whitespace.
		options.Token = strings.TrimSpace(string(saValue))
	}

	timeout := 5 * time.Second
	if options.Timeout != 0 {
		timeout = options.Timeout
	}

	transport := newSessionManagerTransport(options)

	client := &http.Client{
		Timeout:   timeout,
		Transport: transport,
	}
	// This transport is per-call, so its idle connections have no other user.
	// Left alone they would stay established indefinitely (a zero-value
	// Transport has no IdleConnTimeout), holding a socket and its reader and
	// writer goroutines long after this function returns.
	defer transport.CloseIdleConnections()

	// Bind the caller's context so cancellation and deadlines are honoured.
	// options.Timeout only caps the total call; it cannot abort one early.
	request, err := http.NewRequestWithContext(ctx, http.MethodGet, options.URL, nil)
	if err != nil {
		return "", fmt.Errorf("failed creating new http client: %w", err)
	}
	authToken := fmt.Sprintf("Bearer %s", options.Token)
	request.Header.Add("Authorization", authToken)

	resp, err := client.Do(request)
	if err != nil {
		return "", fmt.Errorf("failed calling vc session manager: %w", err)
	}
	// Close on every exit path, not just the success one. net/http guarantees a
	// non-nil Body even when the response has none, and the caller always owns it.
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return "", fmt.Errorf("invalid vc session manager response: %s", resp.Status)
	}

	token := &SharedSessionResponse{}
	decoder := json.NewDecoder(resp.Body)
	if err := decoder.Decode(token); err != nil {
		return "", fmt.Errorf("failed decoding vc session manager response: %w", err)
	}

	if token.Token == "" {
		return "", fmt.Errorf("returned vc session token is empty")
	}
	return token.Token, nil
}
