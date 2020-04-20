// Copyright 2018 VMware, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//    http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package vsphere

import (
	"context"
	"crypto/tls"
	"encoding/pem"
	"fmt"
	"net"
	neturl "net/url"
	"strconv"
	"sync"

	"gitlab.eng.vmware.com/hatchway/govmomi/cns"
	"gitlab.eng.vmware.com/hatchway/govmomi/property"
	"gitlab.eng.vmware.com/hatchway/govmomi/vsan"

	"sigs.k8s.io/vsphere-csi-driver/pkg/csi/service/logger"

	"gitlab.eng.vmware.com/hatchway/govmomi"
	"gitlab.eng.vmware.com/hatchway/govmomi/find"
	"gitlab.eng.vmware.com/hatchway/govmomi/object"
	"gitlab.eng.vmware.com/hatchway/govmomi/pbm"
	"gitlab.eng.vmware.com/hatchway/govmomi/session"
	"gitlab.eng.vmware.com/hatchway/govmomi/sts"
	"gitlab.eng.vmware.com/hatchway/govmomi/vim25"
	"gitlab.eng.vmware.com/hatchway/govmomi/vim25/mo"
	"gitlab.eng.vmware.com/hatchway/govmomi/vim25/soap"
	"gitlab.eng.vmware.com/hatchway/govmomi/vim25/types"
)

const (
	// DefaultScheme is the default connection scheme.
	DefaultScheme = "https"
	// DefaultRoundTripperCount is the default SOAP round tripper count.
	DefaultRoundTripperCount = 3
)

// VirtualCenter holds details of a virtual center instance.
type VirtualCenter struct {
	// Config represents the virtual center configuration.
	Config *VirtualCenterConfig
	// Client represents the govmomi client instance for the connection.
	Client *govmomi.Client
	// PbmClient represents the govmomi PBM Client instance.
	PbmClient *pbm.Client
	// CnsClient represents the CNS client instance.
	CnsClient *cns.Client
	// VsanClient represents the VSAN client instance.
	VsanClient *vsan.Client
}

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
	// Insecure tells if an insecure connection is allowed.
	Insecure bool
	// RoundTripperCount is the SOAP round tripper count. (retries = RoundTripperCount - 1)
	RoundTripperCount int
	// DatacenterPaths represents paths of datacenters on the virtual center.
	DatacenterPaths []string
	// TargetDatastoreUrlsForFile represents URLs of file service enabled vSAN datastores in the virtual center.
	TargetvSANFileShareDatastoreURLs []string
}

// clientMutex is used for exclusive connection creation.
var clientMutex sync.Mutex

// newClient creates a new govmomi Client instance.
func (vc *VirtualCenter) newClient(ctx context.Context) (*govmomi.Client, error) {
	log := logger.GetLogger(ctx)
	if vc.Config.Scheme == "" {
		vc.Config.Scheme = DefaultScheme
	}

	url, err := soap.ParseURL(net.JoinHostPort(vc.Config.Host, strconv.Itoa(vc.Config.Port)))
	if err != nil {
		log.Errorf("Failed to parse URL %s with err: %v", url, err)
		return nil, err
	}

	soapClient := soap.NewClient(url, vc.Config.Insecure)
	vimClient, err := vim25.NewClient(ctx, soapClient)
	if err != nil {
		log.Errorf("Failed to create new client with err: %v", err)
		return nil, err
	}
	vimClient.UseServiceVersion("vsan")
	vimClient.UserAgent = "k8s-csi-useragent"

	client := &govmomi.Client{
		Client:         vimClient,
		SessionManager: session.NewManager(vimClient),
	}

	err = vc.login(ctx, client)
	if err != nil {
		return nil, err
	}

	s, err := client.SessionManager.UserSession(ctx)
	if err == nil {
		log.Infof("New session ID for '%s' = %s", s.UserName, s.Key)
	}

	if vc.Config.RoundTripperCount == 0 {
		vc.Config.RoundTripperCount = DefaultRoundTripperCount
	}
	client.RoundTripper = vim25.Retry(client.RoundTripper, vim25.TemporaryNetworkError(vc.Config.RoundTripperCount))
	return client, nil
}

// login calls SessionManager.LoginByToken if certificate and private key are configured,
// otherwise calls SessionManager.Login with user and password.
func (vc *VirtualCenter) login(ctx context.Context, client *govmomi.Client) error {
	log := logger.GetLogger(ctx)
	var err error

	b, _ := pem.Decode([]byte(vc.Config.Username))
	if b == nil {
		return client.SessionManager.Login(ctx, neturl.UserPassword(vc.Config.Username, vc.Config.Password))
	}

	cert, err := tls.X509KeyPair([]byte(vc.Config.Username), []byte(vc.Config.Password))
	if err != nil {
		log.Errorf("Failed to load X509 key pair with err: %v", err)
		return err
	}

	tokens, err := sts.NewClient(ctx, client.Client)
	if err != nil {
		log.Errorf("Failed to create STS client with err: %v", err)
		return err
	}

	req := sts.TokenRequest{
		Certificate: &cert,
	}

	signer, err := tokens.Issue(ctx, req)
	if err != nil {
		log.Errorf("Failed to issue SAML token with err: %v", err)
		return err
	}

	header := soap.Header{Security: signer}
	return client.SessionManager.LoginByToken(client.Client.WithHeader(ctx, header))
}

// Connect establishes a new connection with vSphere with updated credentials
// If credentials are invalid then it fails the connection.
func (vc *VirtualCenter) Connect(ctx context.Context) error {
	log := logger.GetLogger(ctx)
	// Set up the vc connection
	err := vc.connect(ctx, false)
	if err != nil {
		log.Errorf("Cannot connect to vCenter with err: %v", err)
	}
	return err
}

// connect creates a connection to the virtual center host.
func (vc *VirtualCenter) connect(ctx context.Context, requestNewSession bool) error {
	log := logger.GetLogger(ctx)
	clientMutex.Lock()
	defer clientMutex.Unlock()

	// If client was never initialized, initialize one.
	var err error
	if vc.Client == nil {
		if vc.Client, err = vc.newClient(ctx); err != nil {
			log.Errorf("Failed to create govmomi client with err: %v", err)
			return err
		}
		return nil
	}
	if !requestNewSession {
		// If session hasn't expired, nothing to do.
		sessionMgr := session.NewManager(vc.Client.Client)
		// SessionMgr.UserSession(ctx) retrieves and returns the SessionManager's CurrentSession field
		// Nil is returned if the session is not authenticated or timed out.
		if userSession, err := sessionMgr.UserSession(ctx); err != nil {
			log.Errorf("Failed to obtain user session with err: %v", err)
			return err
		} else if userSession != nil {
			return nil
		}
	}
	// If session has expired, create a new instance.
	log.Warnf("Creating a new client session as the existing session isn't valid or not authenticated")
	if vc.Client, err = vc.newClient(ctx); err != nil {
		log.Errorf("Failed to create govmomi client with err: %v", err)
		return err
	}
	// Recreate PbmClient If created using timed out VC Client
	if vc.PbmClient != nil {
		if vc.PbmClient, err = pbm.NewClient(ctx, vc.Client.Client); err != nil {
			log.Errorf("Failed to create pbm client with err: %v", err)
			return err
		}
	}
	// Recreate CNSClient If created using timed out VC Client
	if vc.CnsClient != nil {
		if vc.CnsClient, err = NewCnsClient(ctx, vc.Client.Client); err != nil {
			log.Errorf("Failed to create CNS client on vCenter host %v with err: %v", vc.Config.Host, err)
			return err
		}
	}
	// Recreate VSAN client if created using timed out VC Client
	if vc.VsanClient != nil {
		if vc.VsanClient, err = vsan.NewClient(ctx, vc.Client.Client); err != nil {
			log.Errorf("Failed to create vsan client with err: %v", err)
			return err
		}
	}
	return nil
}

// listDatacenters returns all Datacenters.
func (vc *VirtualCenter) listDatacenters(ctx context.Context) ([]*Datacenter, error) {
	log := logger.GetLogger(ctx)
	finder := find.NewFinder(vc.Client.Client, false)
	dcList, err := finder.DatacenterList(ctx, "*")
	if err != nil {
		log.Errorf("Failed to list datacenters with err: %v", err)
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
func (vc *VirtualCenter) getDatacenters(ctx context.Context, dcPaths []string) ([]*Datacenter, error) {
	log := logger.GetLogger(ctx)
	finder := find.NewFinder(vc.Client.Client, false)
	var dcs []*Datacenter
	for _, dcPath := range dcPaths {
		dcObj, err := finder.Datacenter(ctx, dcPath)
		if err != nil {
			log.Errorf("Failed to fetch datacenter given dcPath %s with err: %v", dcPath, err)
			return nil, err
		}
		dc := &Datacenter{Datacenter: dcObj, VirtualCenterHost: vc.Config.Host}
		dcs = append(dcs, dc)
	}
	return dcs, nil
}

// GetDatacenters returns Datacenters found on the VirtualCenter. If no
// datacenters are mentioned in the VirtualCenterConfig during registration, all
// Datacenters for the given VirtualCenter will be returned. If DatacenterPaths
// is configured in VirtualCenterConfig during registration, only the listed
// Datacenters are returned.
func (vc *VirtualCenter) GetDatacenters(ctx context.Context) ([]*Datacenter, error) {
	if len(vc.Config.DatacenterPaths) != 0 {
		return vc.getDatacenters(ctx, vc.Config.DatacenterPaths)
	}
	return vc.listDatacenters(ctx)
}

// Disconnect disconnects the virtual center host connection if connected.
func (vc *VirtualCenter) Disconnect(ctx context.Context) error {
	log := logger.GetLogger(ctx)
	if vc.Client == nil {
		log.Info("Client wasn't connected, ignoring")
		return nil
	}
	if err := vc.Client.Logout(ctx); err != nil {
		log.Errorf("Failed to logout with err: %v", err)
		return err
	}
	vc.Client = nil
	return nil
}

// GetHostsByCluster return hosts inside the cluster using cluster moref.
func (vc *VirtualCenter) GetHostsByCluster(ctx context.Context, clusterMorefValue string) ([]*HostSystem, error) {
	log := logger.GetLogger(ctx)
	clusterMoref := types.ManagedObjectReference{
		Type:  "ClusterComputeResource",
		Value: clusterMorefValue,
	}
	clusterComputeResourceMo := mo.ClusterComputeResource{}
	err := vc.Client.RetrieveOne(ctx, clusterMoref, []string{"host"}, &clusterComputeResourceMo)
	if err != nil {
		log.Errorf("Failed to fetch hosts from cluster given clusterMorefValue %s with err: %v", clusterMorefValue, err)
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

// GetVsanDatastores returns all the vsan datastore exists in the vc inventory
func (vc *VirtualCenter) GetVsanDatastores(ctx context.Context) ([]mo.Datastore, error) {
	log := logger.GetLogger(ctx)
	datacenters, err := vc.GetDatacenters(ctx)
	if err != nil {
		log.Errorf("Failed to find datacenters from VC: %+v, Error: %+v", vc.Config.Host, err)
		return nil, err
	}

	var vsanDatastores []mo.Datastore
	for _, dc := range datacenters {
		finder := find.NewFinder(dc.Datacenter.Client(), false)
		finder.SetDatacenter(dc.Datacenter)
		datastoresList, err := finder.DatastoreList(ctx, "*")
		if err != nil {
			log.Errorf("Failed to get all the datastores. err: %+v", err)
			return nil, err
		}
		var dsMorList []types.ManagedObjectReference
		for _, ds := range datastoresList {
			dsMorList = append(dsMorList, ds.Reference())
		}
		var dsMoList []mo.Datastore
		pc := property.DefaultCollector(dc.Client())
		properties := []string{"summary"}
		err = pc.Retrieve(ctx, dsMorList, properties, &dsMoList)
		if err != nil {
			log.Errorf("Failed to get Datastore managed objects from datastore objects."+
				" dsObjList: %+v, properties: %+v, err: %v", dsMorList, properties, err)
			return nil, err
		}

		for _, dsMo := range dsMoList {
			if dsMo.Summary.Type == "vsan" {
				vsanDatastores = append(vsanDatastores, dsMo)
			}
		}
	}

	return vsanDatastores, nil
}
