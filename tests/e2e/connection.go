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

package e2e

import (
	"context"
	"fmt"
	neturl "net/url"
	"sync"

	gomega "github.com/onsi/gomega"
	"github.com/vmware/govmomi"
	"github.com/vmware/govmomi/session"
	"github.com/vmware/govmomi/vim25"
	"github.com/vmware/govmomi/vim25/soap"
	vimtypes "github.com/vmware/govmomi/vim25/types"
	"k8s.io/kubernetes/test/e2e/framework"
)

type cnsClient struct {
	*soap.Client
}

const (
	vsanNamespace            = "vsan"
	vsanHealthPath           = "/vsanHealth"
	roundTripperDefaultCount = 3
)

var (
	clientMutex              sync.Mutex
	cnsVolumeManagerInstance = vimtypes.ManagedObjectReference{
		Type:  "CnsVolumeManager",
		Value: "cns-volume-manager",
	}
	clientLock sync.Mutex
)

// connect helps make a connection to vCenter Server
// No actions are taken if a connection exists and alive. Otherwise, a new client will be created.
func connect(ctx context.Context, vs *vSphere) {
	clientLock.Lock()
	var err error
	defer clientLock.Unlock()
	if vs.Client == nil {
		vs.Client = newClient(ctx, vs)
	}
	manager := session.NewManager(vs.Client.Client)
	userSession, err := manager.UserSession(ctx)
	gomega.Expect(err).NotTo(gomega.HaveOccurred())
	if userSession != nil {
		return
	}
	framework.Logf("Creating new client session since the existing session is not valid or not authenticated")
	err = vs.Client.Logout(ctx)
	gomega.Expect(err).NotTo(gomega.HaveOccurred())
	vs.Client = newClient(ctx, vs)
}

// newClient creates a new client for vSphere connection
func newClient(ctx context.Context, vs *vSphere) *govmomi.Client {
	url, err := neturl.Parse(fmt.Sprintf("https://%s:%s/sdk", vs.Config.Global.VCenterHostname, vs.Config.Global.VCenterPort))
	gomega.Expect(err).NotTo(gomega.HaveOccurred())
	url.User = neturl.UserPassword(vs.Config.Global.User, vs.Config.Global.Password)
	client, err := govmomi.NewClient(ctx, url, true)
	gomega.Expect(err).NotTo(gomega.HaveOccurred())
	client.RoundTripper = vim25.Retry(client.RoundTripper, vim25.TemporaryNetworkError(roundTripperDefaultCount))
	return client
}

// newCnsClient creates a new CNS client
func newCnsClient(ctx context.Context, c *vim25.Client) (*cnsClient, error) {
	sc := c.Client.NewServiceClient(vsanHealthPath, vsanNamespace)
	return &cnsClient{sc}, nil
}

// connectCns creates a CNS client for the virtual center.
func connectCns(ctx context.Context, vs *vSphere) error {
	var err error
	clientMutex.Lock()
	defer clientMutex.Unlock()
	if vs.CnsClient == nil {
		vs.CnsClient, err = newCnsClient(ctx, vs.Client.Client)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
	}
	return nil
}
