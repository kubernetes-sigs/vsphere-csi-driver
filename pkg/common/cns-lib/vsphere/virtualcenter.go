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
	neturl "net/url"
	"reflect"
	"strconv"
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
	"github.com/vmware/govmomi/vapi/tags"
	"github.com/vmware/govmomi/vim25"
	"github.com/vmware/govmomi/vim25/mo"
	"github.com/vmware/govmomi/vim25/soap"
	"github.com/vmware/govmomi/vim25/types"
	"github.com/vmware/govmomi/vsan"
	"github.com/vmware/govmomi/vslm"

	"sigs.k8s.io/vsphere-csi-driver/v3/pkg/common/config"
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
)

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
	// tagManager represents the tagmanager client instance.
	tagManager *tags.Manager

	// ClientMutex is used for exclusive connection creation.
	ClientMutex *sync.Mutex
}

type MetricRoundTripper struct {
	clientName   string
	roundTripper soap.RoundTripper
}

var (
	// VCenter instance. It is a singleton.
	vCenterInstance *VirtualCenter
	// Map of VCenter Hostname and vCenter instances
	vCenterInstances = make(map[string]*VirtualCenter)

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
	Host string
	// Port represents the virtual center host port.
	Port int
	// Username represents the virtual center username.
	Username string
	// Password represents the virtual center password in clear text.
	Password string
	// Specifies whether to verify the server's certificate chain. Set to true to
	// skip verification.
	Insecure bool
	// Specifies the path to a CA certificate in PEM format. This has no effect
	// if Insecure is enabled. Optional; if not configured, the system's CA
	// certificates will be used.
	CAFile string
	// Thumbprint specifies the certificate thumbprint to use. This has no effect
	// if InsecureFlag is enabled.
	Thumbprint string
	// RoundTripperCount is the SOAP round tripper count.
	// retries = RoundTripperCount - 1
	RoundTripperCount int
	// DatacenterPaths represents paths of datacenters on the virtual center.
	DatacenterPaths []string
	// TargetvSANFileShareClusters represents file service enabled vSAN clusters
	// on which file volumes can be created.
	TargetvSANFileShareClusters []string
	// QueryLimit specifies the number of volumes that can be fetched by CNS
	// QueryAll API at a time
	QueryLimit int
	// ListVolumeThreshold specifies the maximum number of differences in volume that
	// can exist between CNS and kubernetes
	ListVolumeThreshold int
	// MigrationDataStore specifies datastore which is set as default datastore in legacy cloud-config
	// and hence should be used as default datastore.
	MigrationDataStoreURL string
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
	VCSessionManagerToken string
}

// NewClient creates a new govmomi Client instance.
func (vc *VirtualCenter) NewClient(ctx context.Context, useragent string) (*govmomi.Client, *rest.Client, error) {
	log := logger.GetLogger(ctx)
	if vc.Config.Scheme == "" {
		vc.Config.Scheme = DefaultScheme
	}

	url, err := soap.ParseURL(net.JoinHostPort(vc.Config.Host, strconv.Itoa(vc.Config.Port)))
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
	err = vimClient.UseServiceVersion("vsan")
	if err != nil && vc.Config.Host != "127.0.0.1" {
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
	client.RoundTripper = &MetricRoundTripper{"soap", rt}
	return client, restClient, nil
}

// login calls SessionManager.LoginByToken if certificate and private key are
// configured. Otherwise, calls SessionManager.Login with user and password.
func (vc *VirtualCenter) login(ctx context.Context, client *govmomi.Client, restClient *rest.Client) error {
	log := logger.GetLogger(ctx)
	var err error

	// If session manager is used, username and password can be discarded/ignored
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
		restClient.SessionID(client.SessionCookie().Value)
		return nil
	}

	b, _ := pem.Decode([]byte(vc.Config.Username))
	if b == nil {
		if err := client.SessionManager.Login(ctx, neturl.UserPassword(vc.Config.Username, vc.Config.Password)); err != nil {
			log.Errorf("error logging soap client: %v", err)
			return err
		}
		return restClient.Login(ctx, neturl.UserPassword(vc.Config.Username, vc.Config.Password))
	}

	cert, err := tls.X509KeyPair([]byte(vc.Config.Username), []byte(vc.Config.Password))
	if err != nil {
		log.Errorf("failed to load X509 key pair with err: %v", err)
		return err
	}

	tokens, err := sts.NewClient(ctx, client.Client)
	if err != nil {
		log.Errorf("failed to create STS client with err: %v", err)
		return err
	}

	req := sts.TokenRequest{
		Certificate: &cert,
		Delegatable: true,
	}

	signer, err := tokens.Issue(ctx, req)
	if err != nil {
		log.Errorf("failed to issue SAML token with err: %v", err)
		return err
	}

	header := soap.Header{Security: signer}
	if err := client.SessionManager.LoginByToken(client.Client.WithHeader(ctx, header)); err != nil {
		return err
	}

	return restClient.LoginByToken(restClient.WithSigner(ctx, signer))
}

// Connect establishes a new connection with vSphere with updated credentials.
// If credentials are invalid then it fails the connection.
func (vc *VirtualCenter) Connect(ctx context.Context) error {
	log := logger.GetLogger(ctx)

	vc.ClientMutex.Lock()
	defer vc.ClientMutex.Unlock()

	// Set up the vc connection.
	err := vc.connect(ctx)
	if err != nil {
		log.Errorf("Cannot connect to vCenter with err: %v", err)
		// Logging out of the current session to make sure the retry create
		// a new client in the next attempt.
		defer func() {
			if vc.Client != nil {
				logoutErr := vc.Client.Logout(ctx)
				if logoutErr != nil {
					log.Errorf("Could not logout of VC session. Error: %v", logoutErr)
				}
			}
			if vc.RestClient != nil {
				logoutErr := vc.RestClient.Logout(ctx)
				if logoutErr != nil {
					// TODO: On vSphere U3, with a shared session this may return an error as logging
					// out from Soap also logs out from rest.
					log.Errorf("Could not logout of VC rest session. Error: %v", logoutErr)
				}
			}
		}()
	}
	return err
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

		log.Infof("VirtualCenter.connect() successfully created new client")
		return nil
	}

	// If session hasn't expired, nothing to do.
	sessionMgr := session.NewManager(vc.Client.Client)
	// SessionMgr.UserSession(ctx) retrieves and returns the SessionManager's
	// CurrentSession field. Nil is returned if the session is not
	// authenticated or timed out.
	shouldLogin := false

	userSession, err := sessionMgr.UserSession(ctx)
	if err != nil {
		log.Errorf("failed to obtain user session with err: %v", err)
		// Login again if session is invalid
		shouldLogin = true
		//return err <- Check if we really want to return error here or follow with re-login
	}

	restSession, err := vc.RestClient.Session(ctx)
	if err != nil {
		log.Errorf("failed to obtain rest user session with err: %v", err)
		// Login again if session is invalid
		shouldLogin = true
	}

	// No need to re-login
	if userSession != nil && restSession != nil && !shouldLogin {
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
			log.Errorf("failed to logout current rest session. still clearing idle sessions. err: %v", err)
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
	// Recreate PbmClient if created using timed out VC Client.
	if vc.PbmClient != nil {
		if vc.PbmClient, err = pbm.NewClient(ctx, vc.Client.Client); err != nil {
			log.Errorf("failed to create pbm client with err: %v", err)
			return err
		}
		vc.PbmClient.RoundTripper = &MetricRoundTripper{"pbm", vc.PbmClient.RoundTripper}
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
		vc.VsanClient.RoundTripper = &MetricRoundTripper{"vsan", vc.VsanClient.RoundTripper}
	}

	// Recreate the Tag Manager client if created using timed out Rest client
	if vc.tagManager != nil {
		vc.tagManager = tags.NewManager(vc.RestClient)
	}

	return nil
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
// to reflect the real current user being used
func (vc *VirtualCenter) GetActiveUser(ctx context.Context) (string, error) {
	if vc.Client == nil || vc.Client.SessionManager == nil {
		return "", fmt.Errorf("client or sessionmanager are null")
	}

	userSession, err := vc.Client.SessionManager.UserSession(ctx)
	if err != nil {
		return "", fmt.Errorf("error getting current user: %w", err)
	}
	return userSession.UserName, nil
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
	if err := vc.RestClient.Logout(ctx); err != nil {
		log.Errorf("failed to logout rest with err: %v", err)
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

	_, found := vCenterInstances[vcconfig.Host]
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
		vCenterInstances[vcconfig.Host] = vcInstance
		log.Infof("vCenterInstance for vCenter: %q initialized", vcconfig.Host)
	}
	return vCenterInstances[vcconfig.Host], nil
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
func GetVirtualCenterInstanceForVCenterHost(ctx context.Context, vcHost string,
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
