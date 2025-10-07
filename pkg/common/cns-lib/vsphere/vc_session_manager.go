package vsphere

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
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

// GetSharedToken executes an http request on session manager and gets the session manager
// token that can be reused on govmomi sessions
func GetSharedToken(ctx context.Context, options SharedTokenOptions) (string, error) {
	if options.URL == "" {
		return "", fmt.Errorf("URL of session manager cannot be empty")
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
		options.Token = string(saValue)
	}

	timeout := 5 * time.Second
	if options.Timeout != 0 {
		timeout = options.Timeout
	}

	transport := &http.Transport{
		TLSClientConfig: &tls.Config{
			RootCAs:            options.TrustedCertificates,
			InsecureSkipVerify: options.InsecureSkipVerify,
		},
	}

	client := &http.Client{
		Timeout:   timeout,
		Transport: transport,
	}

	request, err := http.NewRequest(http.MethodGet, options.URL, nil)
	if err != nil {
		return "", fmt.Errorf("failed creating new http client: %w", err)
	}
	authToken := fmt.Sprintf("Bearer %s", options.Token)
	request.Header.Add("Authorization", authToken)

	resp, err := client.Do(request)
	if err != nil {
		return "", fmt.Errorf("failed calling vc session manager: %w", err)
	}

	if resp.StatusCode != http.StatusOK {
		return "", fmt.Errorf("invalid vc session manager response: %s", resp.Status)
	}

	token := &SharedSessionResponse{}
	defer resp.Body.Close()
	decoder := json.NewDecoder(resp.Body)
	if err := decoder.Decode(token); err != nil {
		return "", fmt.Errorf("failed decoding vc session manager response: %w", err)
	}

	if token.Token == "" {
		return "", fmt.Errorf("returned vc session token is empty")
	}
	return token.Token, nil
}
