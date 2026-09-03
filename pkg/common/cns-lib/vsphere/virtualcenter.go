/*
Copyright 2019 The Kubernetes Authors.

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
	"encoding/pem"
	"errors"
	"fmt"
	"net"
	"net/http"
	neturl "net/url"
	"reflect"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/vmware/govmomi"
	"github.com/vmware/govmomi/cns"
	"github.com/vmware/govmomi/find"
	"github.com/vmware/govmomi/object"
	"github.com/vmware/govmomi/pbm"
	"github.com/vmware/govmomi/property"
	"github.com/vmware/govmomi/session"
	"github.com/vmware/govmomi/sts"
	"github.com/vmware/govmomi/vapi/rest"
	"github.com/vmware/govmomi/vim25"
	"github.com/vmware/govmomi/vim25/mo"
	"github.com/vmware/govmomi/vim25/soap"
	"github.com/vmware/govmomi/vim25/types"
	"github.com/vmware/govmomi/vsan"
	"github.com/vmware/govmomi/vslm"

	"sigs.k8s.io/vsphere-csi-driver/v3/pkg/common/config"
	commontypes "sigs.k8s.io/vsphere-csi-driver/v3/pkg/common/types"
	"sigs.k8s.io/vsphere-csi-driver/v3/pkg/csi/service/logger"

	"sigs.k8s.io/vsphere-csi-driver/v3/pkg/common/prometheus"
)

const (
	// DefaultScheme is the default connection scheme.
	DefaultScheme = "https"
	// DefaultRoundTripperCount is the default SOAP round tripper count.
	DefaultRoundTripperCount = 3

	// success request
	statusSuccess = "success"
	// failed request
	statusFailUnknown = "fail-unknown"

	// Retry configuration for InvalidLogin errors during connection establishment.
	// These constants define the retry behavior when authentication fails,
	// which can happen during password rotation by WCP service.
	// Uses a flat 3-minute (180s) delay matching vCenter's SSO lockout window
	// (5 attempts in 180s). After one retry, the function returns an error,
	// allowing Kubernetes to restart the container for fresh attempts.
	// This approach is lockout-proof and leverages Kubernetes' restart mechanism.
	maxLoginRetries = 2 // Initial attempt + 1 retry after 180s
	retryDelay      = 3 * time.Minute

	// restLoginCooldown is how long connect() waits before attempting another
	// rest login after one was rejected as an authentication failure. It matches
	// retryDelay, and for the same reason: the rest re-login path runs on every
	// connect() and has no attempt limit of its own, so without a cooldown a
	// wrong password would spend vCenter's SSO lockout budget (5 attempts in
	// 180s) within seconds on an active cluster and lock the account out.
	restLoginCooldown = retryDelay
)

// restLoginTimeout bounds a rest re-login attempt on connect()'s repair path.
// NewClient sets soap.Client.Timeout to 0 and the rest client shares that
// transport, so an endpoint that accepts the connection and then never answers
// would otherwise hang that attempt indefinitely -- while holding ClientMutex,
// which every other caller of Connect is queued behind. A variable rather than
// a constant so tests can shorten it.
var restLoginTimeout = 60 * time.Second

// VirtualCenter holds details of a virtual center instance.
type VirtualCenter struct {
	// Config represents the virtual center configuration.
	Config *VirtualCenterConfig
	// Client represents the govmomi client instance for the connection.
	Client *govmomi.Client
	// RestClient represents the govmomi rest client
	RestClient *rest.Client
	// PbmClient represents the govmomi PBM Client instance.
	PbmClient *pbm.Client
	// CnsClient represents the CNS client instance.
	CnsClient *cns.Client
	// VsanClient represents the VSAN client instance.
	VsanClient *vsan.Client
	// VslmClient represents the Vslm client instance.
	VslmClient *vslm.Client
	// ClientMutex is used for exclusive connection creation.
	ClientMutex *sync.Mutex
	// restLoginCooldownUntil is the time before which connect() will not
	// attempt another rest re-login, set when one was rejected as an
	// authentication failure. Zero means no cooldown is in effect. Read and
	// written only from connect(), which callers reach through Connect while it
	// holds ClientMutex, the same protection vc.Client and vc.RestClient have.
	restLoginCooldownUntil time.Time
}

type MetricRoundTripper struct {
	roundTripper soap.RoundTripper
	clientName   string
}

var (
	// VCenter instance. It is a singleton.
	vCenterInstance *VirtualCenter
	// Map of VCenter Hostname and vCenter instances
	vCenterInstances = make(map[commontypes.FQDN]*VirtualCenter)

	// Has the vCenter instance been initialized?
	vCenterInitialized bool
	// vCenterInstanceLock makes sure only one vCenter instance be initialized.
	vCenterInstanceLock = &sync.RWMutex{}
	// vCenterInstancesLock makes sure only one vCenter being initialized for specific host
	vCenterInstancesLock = &sync.RWMutex{}
)

func (vc *VirtualCenter) String() string {
	return fmt.Sprintf("VirtualCenter [Config: %v, Client: %v, PbmClient: %v]",
		vc.Config, vc.Client, vc.PbmClient)
}

// VirtualCenterConfig represents virtual center configuration.
type VirtualCenterConfig struct {
	// Scheme represents the connection scheme. (Ex: https)
	Scheme string
	// Host represents the virtual center host address.
	Host commontypes.FQDN
	// Username represents the virtual center username.
	Username string `sensitive:"true"`
	// Password represents the virtual center password in clear text.
	Password string `sensitive:"true"`
	// Specifies the path to a CA certificate in PEM format. This has no effect
	// if Insecure is enabled. Optional; if not configured, the system's CA
	// certificates will be used.
	CAFile string
	// Thumbprint specifies the certificate thumbprint to use. This has no effect
	// if InsecureFlag is enabled.
	Thumbprint string
	// MigrationDataStore specifies datastore which is set as default datastore in legacy cloud-config
	// and hence should be used as default datastore.
	MigrationDataStoreURL string
	// DatacenterPaths represents paths of datacenters on the virtual center.
	DatacenterPaths []string
	// TargetvSANFileShareClusters represents file service enabled vSAN clusters
	// on which file volumes can be created.
	TargetvSANFileShareClusters []string
	// Port represents the virtual center host port.
	Port int
	// RoundTripperCount is the SOAP round tripper count.
	// retries = RoundTripperCount - 1
	RoundTripperCount int
	// QueryLimit specifies the number of volumes that can be fetched by CNS
	// QueryAll API at a time
	QueryLimit int
	// ListVolumeThreshold specifies the maximum number of differences in volume that
	// can exist between CNS and kubernetes
	ListVolumeThreshold int
	// Specifies whether to verify the server's certificate chain. Set to true to
	// skip verification.
	Insecure bool
	// when ReloadVCConfigForNewClient is set to true it forces re-read config secret when
	// new vc client needs to be created
	ReloadVCConfigForNewClient bool
	// FileVolumeActivated indicates whether file service has been enabled on any vSAN cluster or not
	FileVolumeActivated bool
	// VCSessionManagerURL is the path of a rest api capable of generating vCenter Cloned tokens
	// to be reused by clients. When this is used, Username and Password configuration are ignored
	VCSessionManagerURL string
	// VCSessionManagerToken is the token that should be passed to authenticate against the session manager
	// If empty, the Pod service account will be used
	VCSessionManagerToken string `sensitive:"true"`
}

// String returns a string representation of VirtualCenterConfig with sensitive fields redacted.
func (vcc *VirtualCenterConfig) String() string {
	if vcc == nil {
		return "<nil>"
	}
	val := reflect.ValueOf(*vcc)
	typ := val.Type()

	var fields []string
	for i := 0; i < val.NumField(); i++ {
		field := typ.Field(i)
		value := val.Field(i)

		if field.Tag.Get("sensitive") == "true" {
			fields = append(fields, fmt.Sprintf("%s:%s", field.Name, strings.Repeat("*", value.Len())))
		} else {
			fields = append(fields, fmt.Sprintf("%s:%v", field.Name, value.Interface()))
		}
	}

	return fmt.Sprintf("{%s}", strings.Join(fields, " "))
}

// NewClient creates a new govmomi Client instance.
func (vc *VirtualCenter) NewClient(ctx context.Context, useragent string) (*govmomi.Client, *rest.Client, error) {
	log := logger.GetLogger(ctx)
	if vc.Config.Scheme == "" {
		vc.Config.Scheme = DefaultScheme
	}

	url, err := soap.ParseURL(net.JoinHostPort(vc.Config.Host.String(), strconv.Itoa(vc.Config.Port)))
	if err != nil {
		log.Errorf("failed to parse URL %s with err: %v", url, err)
		return nil, nil, err
	}

	soapClient := soap.NewClient(url, vc.Config.Insecure)
	if len(vc.Config.CAFile) > 0 && !vc.Config.Insecure {
		if err := soapClient.SetRootCAs(vc.Config.CAFile); err != nil {
			log.Errorf("failed to load CA file: %v", err)
			return nil, nil, err
		}
	} else if len(vc.Config.Thumbprint) > 0 && !vc.Config.Insecure {
		soapClient.SetThumbprint(url.Host, vc.Config.Thumbprint)
		log.Debugf("using thumbprint %s for url %s ", vc.Config.Thumbprint, url.Host)
	}

	soapClient.Timeout = 0 * time.Minute
	log.Debugf("Setting vCenter soap client timeout to %v", soapClient.Timeout)
	vimClient, err := vim25.NewClient(ctx, soapClient)
	if err != nil {
		log.Errorf("failed to create new client with err: %v", err)
		return nil, nil, err
	}

	// Invoke KeepAlive on the vimClient RoundTripper to keep the connection alive
	// every 10 minutes.
	vimClient.RoundTripper = session.KeepAlive(vimClient.RoundTripper, 10*time.Minute)
	err = vimClient.UseServiceVersion("vsan")
	if err != nil && !vc.Config.Host.Equal("127.0.0.1") {
		// Skipping error for simulator connection for unit tests.
		log.Errorf("Failed to set vimClient service version to vsan. err: %v", err)
		return nil, nil, err
	}
	vimClient.UserAgent = useragent
	client := &govmomi.Client{
		Client:         vimClient,
		SessionManager: session.NewManager(vimClient),
	}

	restClient := rest.NewClient(client.Client)

	err = vc.login(ctx, client, restClient)
	if err != nil {
		log.Errorf("failed to login to vc. err: %v", err)
		return nil, nil, err
	}

	s, err := client.SessionManager.UserSession(ctx)
	if err != nil {
		log.Errorf("failed to get UserSession. err: %v", err)
		return nil, nil, err
	}
	// Refer to this issue - https://github.com/vmware/govmomi/issues/2922
	// When Session Manager -> UserSession can return nil user session with nil error
	// so handling the case for nil session.
	if s == nil {
		return nil, nil, errors.New("nil session obtained from session manager")
	}
	log.Infof("New session ID for '%s' = %s", s.UserName, s.Key)

	if vc.Config.RoundTripperCount == 0 {
		vc.Config.RoundTripperCount = DefaultRoundTripperCount
	}
	rt := vim25.Retry(client.RoundTripper, vim25.TemporaryNetworkError(vc.Config.RoundTripperCount))
	client.RoundTripper = &MetricRoundTripper{clientName: "soap", roundTripper: rt}
	return client, restClient, nil
}

// soapSessionID returns the value of the SOAP session cookie, which vCenter also
// accepts as a REST session id and is how the rest client rides along on a cloned
// session.
//
// soap.Client.SessionCookie returns nil when the jar holds no session cookie, so
// it is checked rather than dereferenced. In practice a successful CloneSession
// leaves the cookie in place, but the caller cannot prove that, and a nil
// dereference on a login path that gets retried would crash the pod rather than
// fail the call.
func soapSessionID(client *govmomi.Client) (string, error) {
	cookie := client.SessionCookie()
	if cookie == nil {
		return "", errors.New("no vCenter session cookie found after cloning the session")
	}
	if cookie.Value == "" {
		return "", errors.New("vCenter session cookie after cloning the session is empty")
	}
	return cookie.Value, nil
}

// login calls SessionManager.LoginByToken if certificate and private key are
// configured. Otherwise, calls SessionManager.Login with user and password.
func (vc *VirtualCenter) login(ctx context.Context, client *govmomi.Client, restClient *rest.Client) error {
	log := logger.GetLogger(ctx)
	var err error

	// If session manager is used, username and password can be discarded/ignored.
	// After CloneSession succeeds, the only remaining step is restClient.SessionID,
	// a local field set with no vCenter call, so there is no step left that can
	// fail here.
	if vc.Config.VCSessionManagerURL != "" {
		token, err := GetSharedToken(ctx, SharedTokenOptions{
			URL:   vc.Config.VCSessionManagerURL,
			Token: vc.Config.VCSessionManagerToken,
		})
		if err != nil {
			log.Errorf("error getting shared session token: %s", err)
			return err
		}
		if err := client.SessionManager.CloneSession(ctx, token); err != nil {
			log.Errorf("error getting cloned session token: %s", err)
			return err
		}
		if err := vc.restLogin(ctx, client, restClient); err != nil {
			log.Errorf("error deriving rest session from cloned session: %s", err)
			return err
		}
		return nil
	}

	b, _ := pem.Decode([]byte(vc.Config.Username))
	if b == nil {
		if err := client.SessionManager.Login(ctx, neturl.UserPassword(vc.Config.Username, vc.Config.Password)); err != nil {
			log.Errorf("error logging soap client: %v", err)
			return err
		}
		if err := vc.restLogin(ctx, client, restClient); err != nil {
			// Keep the SOAP session and succeed anyway: REST is only needed
			// for tag/topology operations, which fail on their own -- and recover on
			// their own -- if they need it and it is still down. Failing the whole
			// login here would mean a vAPI outage blocks everything that needs only
			// SOAP, including driver startup, since Connect() runs from
			// controller.Init(). The next connect() call re-logins the rest client
			// on its own, without touching this SOAP session, once vAPI is
			// reachable again.
			log.Warnf("error logging rest client, continuing with SOAP-only session: %v", err)
		}
		return nil
	}

	signer, err := vc.issueSTSSigner(ctx, client)
	if err != nil {
		return err
	}

	header := soap.Header{Security: signer}
	if err := client.SessionManager.LoginByToken(client.Client.WithHeader(ctx, header)); err != nil {
		return err
	}

	if err := restLoginByToken(ctx, restClient, signer); err != nil {
		// See the equivalent comment in the username/password branch above.
		log.Warnf("error logging rest client by token, continuing with SOAP-only session: %v", err)
	}
	return nil
}

// issueSTSSigner loads the configured certificate/private key pair and issues a
// SAML token from vCenter's STS for it.
func (vc *VirtualCenter) issueSTSSigner(ctx context.Context, client *govmomi.Client) (*sts.Signer, error) {
	log := logger.GetLogger(ctx)

	cert, err := tls.X509KeyPair([]byte(vc.Config.Username), []byte(vc.Config.Password))
	if err != nil {
		log.Errorf("failed to load X509 key pair with err: %v", err)
		return nil, err
	}

	tokens, err := sts.NewClient(ctx, client.Client)
	if err != nil {
		log.Errorf("failed to create STS client with err: %v", err)
		return nil, err
	}

	// Certificate (rather than Userinfo) makes this a Holder-of-Key token,
	// bound to this request's private key and normally usable only by the
	// connection that negotiated it. The signer built from it is reused on a
	// second, separate connection -- the rest client's LoginByToken -- which is
	// a delegation: the rest client presents a token it did not itself
	// negotiate. Delegatable is what permits a HoK token to be reused that way;
	// govc's own login command does the identical one-token, two-logins pattern
	// (cli/session/login.go: issueToken, loginByToken, loginRestByToken) and
	// also sets it for the same reason.
	req := sts.TokenRequest{
		Certificate: &cert,
		Delegatable: true,
	}

	signer, err := tokens.Issue(ctx, req)
	if err != nil {
		log.Errorf("failed to issue SAML token with err: %v", err)
		return nil, err
	}
	return signer, nil
}

func restLoginByToken(ctx context.Context, restClient *rest.Client, signer *sts.Signer) error {
	return restClient.LoginByToken(restClient.WithSigner(ctx, signer))
}

// restLogin authenticates only the rest client, against a SOAP session that is
// already authenticated. It is what login() uses for the rest half of a fresh
// login, and what connect() uses to repair a rest session on a client whose
// SOAP session is still healthy -- the rest client shares the SOAP client's
// transport, so it can be re-authenticated on its own without disturbing the
// SOAP session or any of the dependent clients (pbm, cns, vslm, vsan) built on
// top of it.
func (vc *VirtualCenter) restLogin(ctx context.Context, client *govmomi.Client, restClient *rest.Client) error {
	// With the shared session manager the rest client does not hold a session of
	// its own: its session id is the SOAP session's cookie, so "re-login" is just
	// re-deriving that id from the current SOAP session.
	if vc.Config.VCSessionManagerURL != "" {
		sessionID, err := soapSessionID(client)
		if err != nil {
			return err
		}
		restClient.SessionID(sessionID)
		return nil
	}

	if b, _ := pem.Decode([]byte(vc.Config.Username)); b == nil {
		return restClient.Login(ctx, neturl.UserPassword(vc.Config.Username, vc.Config.Password))
	}

	// Certificate auth: the token issued for the original login is not kept
	// around, so a rest-only re-login issues a fresh one. That is an STS call
	// against vCenter, not vAPI, so it works while vAPI is the part that is down.
	signer, err := vc.issueSTSSigner(ctx, client)
	if err != nil {
		return err
	}
	return restLoginByToken(ctx, restClient, signer)
}

// cleanupVCClient logs out and clears the VC client to ensure a clean state.
// This helper is used during error handling and retry logic.
func (vc *VirtualCenter) cleanupVCClient(ctx context.Context) {
	log := logger.GetLogger(ctx)
	if vc.Client != nil {
		if err := vc.Client.Logout(ctx); err != nil {
			log.With("err", err).Warn("Could not logout of VC session")
		}
	}
	if vc.RestClient != nil {
		// TODO: On vSphere U3, with a shared session this may return an error as logging
		// out from Soap also logs out from rest.
		if err := vc.RestClient.Logout(ctx); err != nil {
			log.With("err", err).Warn("Could not logout of VC rest session")
		}
	}
}

// Connect establishes a new connection with vSphere with updated credentials.
// If credentials are invalid due to password rotation, it retries with a flat 3-minute delay
// to avoid account lockout. The delay matches vCenter's SSO lockout window (5 attempts in 180s),
// ensuring each retry happens after the previous lockout window expires.
// For any other failure, it fails immediately without retry.
// After each login attempt, the client is cleaned up to avoid resource leaks.
func (vc *VirtualCenter) Connect(ctx context.Context) error {
	log := logger.GetLogger(ctx)

	vc.ClientMutex.Lock()
	defer vc.ClientMutex.Unlock()

	var err error
	for attempt := 1; attempt <= maxLoginRetries; attempt++ {
		err = vc.connect(ctx)
		if err == nil {
			// Connection successful
			log.With("attempt", attempt).Debug("Successfully connected to vCenter")
			return nil
		}

		// Check if this is an InvalidLogin error that we should retry
		if !IsInvalidLoginError(ctx, err) {
			// Not an authentication error - fail immediately without retry
			log.With("err", err).Error("Cannot connect to vCenter")
			break
		}

		// If this is the last attempt, don't wait - just exit
		if attempt >= maxLoginRetries {
			log.With("attempts", maxLoginRetries, "err", err).
				Warn("Cannot connect to vCenter after all retry attempts with InvalidLogin error")
			break
		}

		// Log the retry attempt
		log.With("attempt", attempt, "retryDelay", retryDelay, "maxAttempts", maxLoginRetries).
			Warn("Unable to login to VC with invalid login error, retrying (possibly due to credential rotation)")

		// Cleanup before retrying to ensure clean state
		vc.cleanupVCClient(ctx)

		// Wait before retrying.
		// Since defer in not a good practice in loops, we stop the timer explicitly.
		retryTimer := time.NewTimer(retryDelay)
		select {
		case <-ctx.Done():
			retryTimer.Stop()
			log.With("err", ctx.Err()).Error("Context cancelled during retry backoff")
			return ctx.Err()
		case <-retryTimer.C:
			retryTimer.Stop()
			// Continue to next attempt
		}
	}

	// We reach here in one of the following cases:
	// 1. All retry attempts failed with InvalidLogin errors.
	// 2. The context was cancelled.
	// 3. Some other error occurred while creating a new client.
	// In all these cases, we need to clean up the client before returning the error.
	vc.cleanupVCClient(ctx)
	return fmt.Errorf("failed to connect to vCenter: %w", err)
}

// connect creates a connection to the virtual center host.
func (vc *VirtualCenter) connect(ctx context.Context) error {
	log := logger.GetLogger(ctx)

	// If client was never initialized, initialize one.
	var err error
	useragent, err := config.GetSessionUserAgent(ctx)
	if err != nil {
		log.Errorf("failed to get useragent for vCenter session. error: %+v", err)
		return err
	}

	// Once initialised, don't nullify vc.Client or dependent clients.
	// This function will detect the invalid session (after logout)
	// and recreate dependent clients.
	// Nullifying vc.Client would cause connect() to return early
	// before dependent clients are recreated, leaving them with stale sessions.
	if vc.Client == nil || vc.RestClient == nil {
		if vc.Config.ReloadVCConfigForNewClient {
			err = ReadVCConfigs(ctx, vc)
			if err != nil {
				return err
			}
		}
		log.Infof("VirtualCenter.connect() creating new client")
		if vc.Client, vc.RestClient, err = vc.NewClient(ctx, useragent); err != nil {
			log.Errorf("failed to create govmomi client with err: %v", err)
			if !vc.Config.Insecure {
				log.Errorf("failed to connect to vCenter using CA file: %q", vc.Config.CAFile)
			}
			return err
		}

		vc.restLoginCooldownUntil = time.Time{}
		log.Infof("VirtualCenter.connect() successfully created new client")
		return nil
	}

	// If session hasn't expired, nothing to do.
	sessionMgr := session.NewManager(vc.Client.Client)
	// SessionMgr.UserSession(ctx) retrieves and returns the SessionManager's
	// CurrentSession field. Nil is returned if the session is not
	// authenticated or timed out.

	userSession, err := sessionMgr.UserSession(ctx)
	if err != nil {
		log.Errorf("failed to obtain user session with err: %v", err)
		// An error here can mean the vcenter itself is down so
		// we should return early with error
		return err
	}

	// When authenticated through the shared session manager, the rest client's
	// session ID is the SOAP session's own cookie (see login()), so they are the
	// same vCenter session object rather than two independent ones. The logout
	// TODOs above already rely on this: on vSphere U3+ logging out of one logs
	// out of the other. So userSession above already speaks for the rest
	// session too, and checking it again would just be a second network call
	// for the same answer.
	restSessionValid := userSession != nil
	if vc.Config.VCSessionManagerURL == "" {
		restSession, err := vc.RestClient.Session(ctx)
		if err != nil {
			// Deliberately not returned. govmomi maps only HTTP 401 to
			// (nil, nil); any other failure -- a vAPI endpoint restarting, a
			// 503 mid-upgrade, a network blip -- comes back as an error. If
			// that were returned, Connect would treat the whole connection as
			// failed and call cleanupVCClient, which logs out the SOAP session
			// that userSession above just proved healthy, and leaves both
			// client pointers non-nil but dead for anything not going through
			// Connect. A REST session that cannot be read is instead treated
			// as one that needs re-establishing, which the code below does.
			log.Errorf("failed to obtain rest user session, will re-login. err: %v", err)
			restSessionValid = false
		} else {
			restSessionValid = restSession != nil
		}
	}

	// No need to re-login
	if userSession != nil && restSessionValid {
		return nil
	}

	if userSession != nil {
		// Only the rest session is unusable; the SOAP session this client holds
		// is live. Re-authenticate the rest client in place rather than tearing
		// the whole client down: restClient rides on the same soap.Client, so it
		// can be re-logged-in on its own, and the SOAP session plus every
		// dependent client (pbm, cns, vslm, vsan) built on it stay valid.
		//
		// Tearing down here is what made a vAPI outage a churn loop: login()
		// deliberately succeeds with a SOAP-only session when the rest login
		// fails, so each Connect() would log out a working SOAP session, build a
		// new one, fail the rest login again, and leave the next Connect() to do
		// the same -- a full re-login per call for as long as vAPI stayed down.
		vc.repairRestSession(ctx)
		return nil
	}

	log.Infof("logging out current session and clearing idle sessions")

	if vc.Client != nil && vc.Client.Client != nil {
		err = vc.Client.Logout(ctx)
		if err != nil {
			log.Errorf("failed to logout current session. still clearing idle sessions. err: %v", err)
		}
	}

	if vc.RestClient != nil {
		// TODO: On U3 shared session this may return an error, if the Soap logout
		// happened correctly, but can be safely ignored
		if err := vc.RestClient.Logout(ctx); err != nil {
			log.Infof("failed to logout current rest session. still clearing idle sessions. err: %v", err)
		}
	}

	// If session has expired, create a new instance.
	log.Infof("Creating a new client session as the existing one isn't valid or not authenticated")
	if vc.Config.ReloadVCConfigForNewClient {
		err = ReadVCConfigs(ctx, vc)
		if err != nil {
			return err
		}
	}
	if vc.Client, vc.RestClient, err = vc.NewClient(ctx, useragent); err != nil {
		log.Errorf("failed to create govmomi client with err: %v", err)
		if !vc.Config.Insecure {
			log.Errorf("failed to connect to vCenter using CA file: %q", vc.Config.CAFile)
		}
		return err
	}
	vc.restLoginCooldownUntil = time.Time{}
	// Recreate PbmClient if created using timed out VC Client.
	if vc.PbmClient != nil {
		if vc.PbmClient, err = pbm.NewClient(ctx, vc.Client.Client); err != nil {
			log.Errorf("failed to create pbm client with err: %v", err)
			return err
		}
		vc.PbmClient.RoundTripper = &MetricRoundTripper{clientName: "pbm", roundTripper: vc.PbmClient.RoundTripper}
	}
	// Recreate CNSClient if created using timed out VC Client.
	if vc.CnsClient != nil {
		if vc.CnsClient, err = NewCnsClient(ctx, vc.Client.Client); err != nil {
			log.Errorf("failed to create CNS client on vCenter host %v with err: %v",
				vc.Config.Host, err)
			return err
		}
	}
	// Recreate VslmClient if created using timed out VC Client.
	if vc.VslmClient != nil {
		if vc.VslmClient, err = NewVslmClient(ctx, vc.Client.Client); err != nil {
			log.Errorf("failed to create Vslm client on vCenter host %v with err: %v",
				vc.Config.Host, err)
			return err
		}
	}
	// Recreate VSAN client if created using timed out VC Client.
	if vc.VsanClient != nil {
		if vc.VsanClient, err = vsan.NewClient(ctx, vc.Client.Client); err != nil {
			log.Errorf("failed to create vsan client with err: %v", err)
			return err
		}
		vc.VsanClient.RoundTripper = &MetricRoundTripper{clientName: "vsan", roundTripper: vc.VsanClient.RoundTripper}
	}

	return nil
}

// repairRestSession re-authenticates the rest client of a VirtualCenter whose
// SOAP session is still valid. A failure is never fatal, matching login(): a
// vAPI outage leaves a usable SOAP-only connection, and callers that need vAPI
// fail on their own and recover when a later connect() retries this.
//
// Retries are immediate for transport-shaped failures, which is what makes
// recovery from a vAPI outage take a single round trip once it is back. An
// authentication failure is different: it will not recover on its own, and this
// path runs on every connect() with no attempt limit, so retrying one at full
// speed would burn through vCenter's SSO lockout budget. Those get a cooldown.
func (vc *VirtualCenter) repairRestSession(ctx context.Context) {
	log := logger.GetLogger(ctx)

	if remaining := time.Until(vc.restLoginCooldownUntil); remaining > 0 {
		log.Warnf("not attempting a rest re-login for another %v: the last attempt was rejected as an "+
			"authentication failure, and retrying it every connect() risks locking the account out",
			remaining.Truncate(time.Second))
		return
	}

	loginCtx, cancel := context.WithTimeout(ctx, restLoginTimeout)
	defer cancel()

	if err := vc.restLogin(loginCtx, vc.Client, vc.RestClient); err != nil {
		if isRestAuthFailure(ctx, err) {
			vc.restLoginCooldownUntil = time.Now().Add(restLoginCooldown)
			log.Errorf("rest re-login was rejected as an authentication failure, "+
				"continuing with SOAP-only session and not retrying for %v: %v", restLoginCooldown, err)
		} else {
			log.Warnf("failed to re-login rest client, continuing with SOAP-only session: %v", err)
		}
		return
	}
	vc.restLoginCooldownUntil = time.Time{}
	log.Infof("re-logged in rest client without recreating the SOAP session")
}

// isRestAuthFailure reports whether err is vCenter rejecting the credentials
// rather than a transport-level failure.
//
// A vAPI login rejection arrives as an HTTP 401, which govmomi returns as its
// own status error rather than a SOAP fault, so IsInvalidLoginError alone does
// not recognise it. IsInvalidLoginError still matters for the certificate path,
// where the rest login is preceded by an STS token issue -- a SOAP call, which
// faults with InvalidLogin in the usual way when the certificate is rejected.
func isRestAuthFailure(ctx context.Context, err error) bool {
	return rest.IsStatusError(err, http.StatusUnauthorized) || IsInvalidLoginError(ctx, err)
}

// ReadVCConfigs will ensure we are always reading the latest config
// before attempting to create a new govmomi client.
// It works in case of both vanilla (including multi-vc) and wcp
func ReadVCConfigs(ctx context.Context, vc *VirtualCenter) error {
	log := logger.GetLogger(ctx)
	log.Infof("Reloading latest VC config from vSphere Config Secret for vcenter: %q", vc.Config.Host)
	cfg, err := config.GetConfig(ctx)
	if err != nil {
		return logger.LogNewErrorf(log, "failed to read config. Error: %+v", err)
	}
	var foundVCConfig bool
	newVcenterConfigs, err := GetVirtualCenterConfigs(ctx, cfg)
	if err != nil {
		return logger.LogNewErrorf(log, "failed to get VirtualCenterConfigs. err=%v", err)
	}
	for _, newvcconfig := range newVcenterConfigs {
		if newvcconfig.Host == vc.Config.Host {
			newvcconfig.ReloadVCConfigForNewClient = true
			vc.Config = newvcconfig
			log.Infof("Successfully set latest VC config for vcenter: %q", vc.Config.Host)
			foundVCConfig = true
			break
		}
	}
	if !foundVCConfig {
		return logger.LogNewErrorf(log, "failed to get vCenter config for Host: %q", vc.Config.Host)
	}

	return nil
}

// ListDatacenters returns all Datacenters.
func (vc *VirtualCenter) ListDatacenters(ctx context.Context) (
	[]*Datacenter, error) {
	log := logger.GetLogger(ctx)
	if err := vc.Connect(ctx); err != nil {
		log.Errorf("failed to connect to vCenter. err: %v", err)
		return nil, err
	}
	finder := find.NewFinder(vc.Client.Client, false)
	dcList, err := finder.DatacenterList(ctx, "*")
	if err != nil {
		log.Errorf("failed to list datacenters with err: %v", err)
		return nil, err
	}

	var dcs []*Datacenter
	for _, dcObj := range dcList {
		dc := &Datacenter{Datacenter: dcObj, VirtualCenterHost: vc.Config.Host}
		dcs = append(dcs, dc)
	}
	return dcs, nil
}

// getDatacenters returns Datacenter instances given their paths.
func (vc *VirtualCenter) getDatacenters(ctx context.Context, dcPaths []string) (
	[]*Datacenter, error) {
	log := logger.GetLogger(ctx)
	finder := find.NewFinder(vc.Client.Client, false)
	var dcs []*Datacenter
	for _, dcPath := range dcPaths {
		dcObj, err := finder.Datacenter(ctx, dcPath)
		if err != nil {
			log.Errorf("failed to fetch datacenter given dcPath %s with err: %v", dcPath, err)
			return nil, err
		}
		dc := &Datacenter{Datacenter: dcObj, VirtualCenterHost: vc.Config.Host}
		dcs = append(dcs, dc)
	}
	return dcs, nil
}

// GetActiveUser returns the current logged in user. It is fetched from govmomi.Session
// to reflect the real current user being used.
//
// This makes a single attempt and does not establish or repair a session.
// Every caller reaches it just after something else (GetVCenter, GetVCenters,
// GetDatacenters, ...) has already called Connect, so there is a session by
// then and a Connect here would only duplicate the UserSession round trip
// below. In the narrow case where the session dies in between, the error is
// returned and the caller's own retry -- the CSI sidecar for volume
// operations, the periodic refresh for the auth manager -- reconnects.
func (vc *VirtualCenter) GetActiveUser(ctx context.Context) (string, error) {
	if vc.Client == nil || vc.Client.SessionManager == nil {
		return "", fmt.Errorf("client or sessionmanager are nil")
	}

	userSession, err := vc.Client.SessionManager.UserSession(ctx)
	if err != nil {
		return "", fmt.Errorf("error getting current user: %w", err)
	}

	// Refer to this issue - https://github.com/vmware/govmomi/issues/2922
	// Session Manager -> UserSession can return nil user session with nil error
	// so handling the case for nil session.
	if userSession == nil {
		return "", errors.New("nil session obtained from session manager")
	}
	return userSession.UserName, nil
}

// ConfiguredOrActiveUser returns vc.Config.Username when that value is actually
// the username the session is authenticated as. That is a local read with no
// vCenter round trip, and covers the common username/password case. Otherwise it
// falls back to GetActiveUser, which asks vCenter for the live session's user.
//
// Two cases must not use the configured value, because in both of them it is
// either not a username or not this session's username:
//
//   - Shared session manager (VCSessionManagerURL). The session is a clone of
//     whichever user the session manager authenticated as, which has nothing to
//     do with Username. Username is expected to be empty here, but that is not
//     enforced: validateConfig only waives the username/password requirement
//     when VCSessionManagerURL is set, it never clears an already-configured
//     User. So the mode, not the emptiness of Username, is what decides.
//   - Certificate authentication. Username holds a PEM-encoded certificate
//     rather than a user, and Password holds its private key -- see login(),
//     which selects that path with this same pem.Decode check. Returning the
//     PEM block as a username would put it in CNS volume metadata.
func (vc *VirtualCenter) ConfiguredOrActiveUser(ctx context.Context) (string, error) {
	if vc.Config.Username == "" || vc.Config.VCSessionManagerURL != "" {
		return vc.GetActiveUser(ctx)
	}
	if block, _ := pem.Decode([]byte(vc.Config.Username)); block != nil {
		return vc.GetActiveUser(ctx)
	}
	return vc.Config.Username, nil
}

// GetDatacenters returns Datacenters found on the VirtualCenter. If no
// datacenters are mentioned in the VirtualCenterConfig during registration, all
// Datacenters for the given VirtualCenter will be returned. If DatacenterPaths
// is configured in VirtualCenterConfig during registration, only the listed
// Datacenters are returned.
func (vc *VirtualCenter) GetDatacenters(ctx context.Context) ([]*Datacenter, error) {
	log := logger.GetLogger(ctx)
	if err := vc.Connect(ctx); err != nil {
		log.Errorf("failed to connect to vCenter. err: %v", err)
		return nil, err
	}
	if len(vc.Config.DatacenterPaths) != 0 {
		return vc.getDatacenters(ctx, vc.Config.DatacenterPaths)
	}
	return vc.ListDatacenters(ctx)
}

// Disconnect disconnects the virtual center host connection if connected.
func (vc *VirtualCenter) Disconnect(ctx context.Context) error {
	log := logger.GetLogger(ctx)
	if vc.Client == nil {
		log.Info("Client wasn't connected, ignoring")
		return nil
	}
	if err := vc.Client.Logout(ctx); err != nil {
		log.Errorf("failed to logout with err: %v", err)
		return err
	}

	// We don't return an error here because logging out from Rest failing
	// can happen on VC shared sessions
	if vc.RestClient != nil {
		if err := vc.RestClient.Logout(ctx); err != nil {
			log.Infof("failed to logout rest with err: %v", err)
		}
	}
	vc.Client = nil
	vc.RestClient = nil
	return nil
}

// GetHostsByCluster return hosts inside the cluster using cluster moref.
func (vc *VirtualCenter) GetHostsByCluster(ctx context.Context,
	clusterMorefValue string) ([]*HostSystem, error) {
	log := logger.GetLogger(ctx)
	if err := vc.Connect(ctx); err != nil {
		log.Errorf("failed to connect to vCenter. err: %v", err)
		return nil, err
	}
	clusterMoref := types.ManagedObjectReference{
		Type:  "ClusterComputeResource",
		Value: clusterMorefValue,
	}
	clusterComputeResourceMo := mo.ClusterComputeResource{}
	err := vc.Client.RetrieveOne(ctx, clusterMoref, []string{"host"}, &clusterComputeResourceMo)
	if err != nil {
		log.Errorf("failed to fetch hosts from cluster given clusterMorefValue %s with err: %v",
			clusterMorefValue, err)
		return nil, err
	}
	var hostObjList []*HostSystem
	for _, hostMoref := range clusterComputeResourceMo.Host {
		hostObjList = append(hostObjList,
			&HostSystem{
				HostSystem: object.NewHostSystem(vc.Client.Client, hostMoref),
			})
	}
	return hostObjList, nil
}

// GetVsanDatastores returns all the datastore URL to DatastoreInfo map for all
// the vSAN datastores in the VC.
func (vc *VirtualCenter) GetVsanDatastores(ctx context.Context,
	datacenters []*Datacenter) (map[string]*DatastoreInfo, error) {
	log := logger.GetLogger(ctx)
	if err := vc.Connect(ctx); err != nil {
		log.Errorf("failed to connect to vCenter. err: %v", err)
		return nil, err
	}
	vsanDsURLInfoMap := make(map[string]*DatastoreInfo)
	for _, dc := range datacenters {
		finder := find.NewFinder(dc.Datacenter.Client(), false)
		finder.SetDatacenter(dc.Datacenter)
		datastoresList, err := finder.DatastoreList(ctx, "*")
		if err != nil {
			if _, ok := err.(*find.NotFoundError); ok {
				log.Debugf("No datastores found on %q datacenter", dc.Name())
				continue
			}
			log.Errorf("failed to get all the datastores. err: %+v", err)
			return nil, err
		}
		var dsMorList []types.ManagedObjectReference
		for _, ds := range datastoresList {
			dsMorList = append(dsMorList, ds.Reference())
		}
		var dsMoList []mo.Datastore
		pc := property.DefaultCollector(dc.Client())
		properties := []string{"summary", "info", "customValue"}
		err = pc.Retrieve(ctx, dsMorList, properties, &dsMoList)
		if err != nil {
			log.Errorf("failed to get Datastore managed objects from datastore objects."+
				" dsObjList: %+v, properties: %+v, err: %v", dsMorList, properties, err)
			return nil, err
		}

		for _, dsMo := range dsMoList {
			if dsMo.Summary.Type == "vsan" {
				vsanDsURLInfoMap[dsMo.Info.GetDatastoreInfo().Url] = &DatastoreInfo{
					&Datastore{object.NewDatastore(dc.Client(), dsMo.Reference()),
						dc},
					dsMo.Info.GetDatastoreInfo(), dsMo.CustomValue}
			}
		}
	}
	return vsanDsURLInfoMap, nil
}

// GetDatastoresByCluster return datastores inside the cluster using its moref.
// NOTE: The return value can contain duplicates.
func (vc *VirtualCenter) GetDatastoresByCluster(ctx context.Context,
	clusterMorefValue string) ([]*DatastoreInfo, error) {
	log := logger.GetLogger(ctx)
	if err := vc.Connect(ctx); err != nil {
		log.Errorf("failed to connect to vCenter. err: %v", err)
		return nil, err
	}
	clusterMoref := types.ManagedObjectReference{
		Type:  "ClusterComputeResource",
		Value: clusterMorefValue,
	}
	clusterComputeResourceMo := mo.ClusterComputeResource{}
	err := vc.Client.RetrieveOne(ctx, clusterMoref, []string{"host"}, &clusterComputeResourceMo)
	if err != nil {
		log.Errorf("Failed to fetch hosts from cluster given clusterMorefValue %s with err: %v",
			clusterMorefValue, err)
		return nil, err
	}

	var dsList []*DatastoreInfo
	for _, hostMoref := range clusterComputeResourceMo.Host {
		host := &HostSystem{
			HostSystem: object.NewHostSystem(vc.Client.Client, hostMoref),
		}
		dsInfos, err := host.GetAllAccessibleDatastores(ctx)
		if err != nil {
			log.Errorf("Failed to fetch datastores from host %s. Err: %v", hostMoref, err)
			return nil, err
		}
		dsList = append(dsList, dsInfos...)
	}
	return dsList, nil
}

// GetVirtualCenterInstance returns the vcenter object singleton.
// It is thread safe. Takes in a boolean paramater reloadConfig.
// If reinitialize is true, the vcenter object is instantiated again and the
// old object becomes eligible for garbage collection.
// If reinitialize is false and instance was already initialized, the previous
// instance is returned.
func GetVirtualCenterInstance(ctx context.Context,
	config *config.ConfigurationInfo, reinitialize bool) (*VirtualCenter, error) {
	log := logger.GetLogger(ctx)
	vCenterInstanceLock.Lock()
	defer vCenterInstanceLock.Unlock()

	if !vCenterInitialized || reinitialize {
		log.Infof("Initializing new vCenterInstance.")

		var vcconfig *VirtualCenterConfig
		vcconfig, err := GetVirtualCenterConfig(ctx, config.Cfg)
		if err != nil {
			log.Errorf("failed to get VirtualCenterConfig. Err: %+v", err)
			return nil, err
		}

		// Initialize the virtual center manager.
		virtualcentermanager := GetVirtualCenterManager(ctx)

		// Unregister all VCs from virtual center manager.
		if err = virtualcentermanager.UnregisterAllVirtualCenters(ctx); err != nil {
			log.Errorf("failed to unregister vcenter with virtualCenterManager.")
			return nil, err
		}

		// Register with virtual center manager.
		vCenterInstance, err = virtualcentermanager.RegisterVirtualCenter(ctx, vcconfig)
		if err != nil {
			log.Errorf("failed to register VirtualCenter . Err: %+v", err)
			return nil, err
		}

		// Connect to VC.
		err = vCenterInstance.Connect(ctx)
		if err != nil {
			log.Errorf("failed to connect to VirtualCenter host: %q. Err: %+v",
				vcconfig.Host, err)
			return nil, err
		}

		vCenterInitialized = true
		log.Info("vCenterInstance initialized")
	}
	return vCenterInstance, nil
}

// GetVirtualCenterInstanceForVCenterConfig returns the vcenter object for given vCenter Config
// Takes in a boolean paramater reloadConfig.
// If reinitialize is true, the vcenter object is instantiated again and the
// old object becomes eligible for garbage collection.
// If reinitialize is false and instance was already initialized, the previous
// instance is returned.
func GetVirtualCenterInstanceForVCenterConfig(ctx context.Context,
	vcconfig *VirtualCenterConfig, reinitialize bool) (*VirtualCenter, error) {
	log := logger.GetLogger(ctx)
	vCenterInstancesLock.Lock()
	defer vCenterInstancesLock.Unlock()

	hostKey := vcconfig.Host
	_, found := vCenterInstances[hostKey]
	if !found || reinitialize {
		log.Infof("Initializing new vCenterInstance for vCenter %q", vcconfig.Host)
		// Initialize the virtual center manager.
		virtualcentermanager := GetVirtualCenterManager(ctx)
		if found {
			// Unregister the VC from virtual center manager.
			if err := virtualcentermanager.UnregisterVirtualCenter(ctx, vcconfig.Host); err != nil {
				return nil, logger.LogNewErrorf(log, "failed to unregister VirtualCenter %q with "+
					"virtualCenterManager. Err: %+v", vcconfig.Host, err)
			}
		}
		// Register with virtual center manager.
		vcInstance, err := virtualcentermanager.RegisterVirtualCenter(ctx, vcconfig)
		if err != nil {
			if err == ErrVCAlreadyRegistered {
				return nil, ErrVCAlreadyRegistered
			}
			return nil, logger.LogNewErrorf(log, "failed to register VirtualCenter %q Err: %+v",
				vcconfig.Host, err)
		}
		// Connect to VC.
		err = vcInstance.Connect(ctx)
		if err != nil {
			log.Errorf("failed to connect to VirtualCenter host: %q. Err: %+v",
				vcconfig.Host, err)
			return nil, err
		}
		vCenterInstances[hostKey] = vcInstance
		log.Infof("vCenterInstance for vCenter: %q initialized", vcconfig.Host)
	}
	return vCenterInstances[hostKey], nil
}

// UnregisterAllVirtualCenters helps unregister and logout all registered vCenter instances
// This function is called before exiting container to logout current sessions
func UnregisterAllVirtualCenters(ctx context.Context) error {
	log := logger.GetLogger(ctx)
	vCenterInstancesLock.Lock()
	defer vCenterInstancesLock.Unlock()

	// Initialize the virtual center manager.
	virtualcentermanager := GetVirtualCenterManager(ctx)
	// Unregister all vCenters from virtual center manager.
	if err := virtualcentermanager.UnregisterAllVirtualCenters(ctx); err != nil {
		return logger.LogNewErrorf(log, "failed to unregister all VirtualCenter servers. Err: %+v", err)
	}
	return nil
}

// GetVirtualCenterInstanceForVCenterHost returns the vcenter object for given vCenter host.
func GetVirtualCenterInstanceForVCenterHost(ctx context.Context, vcHost commontypes.FQDN,
	reconnect bool) (*VirtualCenter, error) {
	log := logger.GetLogger(ctx)
	vCenterInstancesLock.RLock()
	defer vCenterInstancesLock.RUnlock()

	vc, found := vCenterInstances[vcHost]
	if !found || vc == nil {
		return nil, logger.LogNewErrorf(log, "failed to get VirtualCenter instance for host %q.", vcHost)
	}
	if reconnect {
		err := vc.Connect(ctx)
		if err != nil {
			return nil, logger.LogNewErrorf(log, "failed to connect to VirtualCenter host: %q. Error: %v",
				vcHost, err)
		}
	}
	return vc, nil
}

// GetAllVirtualMachines gets the VM Managed Objects with the given properties from the
// VM object.
func (vc *VirtualCenter) GetAllVirtualMachines(ctx context.Context,
	hostObjList []*HostSystem) ([]*object.VirtualMachine, error) {
	log := logger.GetLogger(ctx)
	var hostMoList []mo.HostSystem
	var hostRefs []types.ManagedObjectReference
	if len(hostObjList) < 1 {
		msg := "host object list is empty"
		log.Errorf(msg+": %v", hostObjList)
		return nil, fmt.Errorf("%s", msg)
	}

	properties := []string{"vm"}
	for _, hostObj := range hostObjList {
		hostRefs = append(hostRefs, hostObj.Reference())
	}

	pc := property.DefaultCollector(vc.Client.Client)
	err := pc.Retrieve(ctx, hostRefs, properties, &hostMoList)
	if err != nil {
		log.Errorf("failed to get host managed objects from host objects. hostObjList: %+v, properties: %+v, err: %v",
			hostObjList, properties, err)
		return nil, err
	}

	var vmRefList []types.ManagedObjectReference
	for _, hostMo := range hostMoList {
		vmRefList = append(vmRefList, hostMo.Vm...)
	}

	var virtualMachines []*object.VirtualMachine
	for _, vmRef := range vmRefList {
		vm := object.NewVirtualMachine(vc.Client.Client, vmRef)
		virtualMachines = append(virtualMachines, vm)
	}
	return virtualMachines, nil
}

func (mrt *MetricRoundTripper) RoundTrip(ctx context.Context, req, resp soap.HasFault) error {
	vreq := reflect.ValueOf(req).Elem().FieldByName("Req").Elem()
	requestName := vreq.Type().Name()
	requestTime := time.Now()
	err := mrt.roundTripper.RoundTrip(ctx, req, resp)
	if err != nil {
		timeTaken := time.Since(requestTime).Seconds()
		prometheus.RequestOpsMetric.WithLabelValues(requestName, mrt.clientName, statusFailUnknown).Observe(timeTaken)
		return err
	}

	timeTaken := time.Since(requestTime).Seconds()
	prometheus.RequestOpsMetric.WithLabelValues(requestName, mrt.clientName, statusSuccess).Observe(timeTaken)
	return nil
}
