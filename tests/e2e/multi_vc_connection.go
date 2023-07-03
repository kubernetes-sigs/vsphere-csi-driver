/*
Copyright 2023 The Kubernetes Authors.

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
	"strings"

	gomega "github.com/onsi/gomega"
	"github.com/vmware/govmomi"
	"github.com/vmware/govmomi/session"
	"github.com/vmware/govmomi/vim25"
	"github.com/vmware/govmomi/vim25/soap"
	"k8s.io/kubernetes/test/e2e/framework"
)

/*
connectMultiVC helps make a connection to a multiple vCenter Server. No actions are taken if a connection
exists and alive. Otherwise, a new client will be created.
*/
func connectMultiVC(ctx context.Context, vs *multiVCvSphere) {
	clientLock.Lock()
	defer clientLock.Unlock()
	if vs.multiVcClient == nil {
		framework.Logf("Creating new VC session")
		vs.multiVcClient = newClientForMultiVC(ctx, vs)
	}
	for i := 0; i < len(vs.multiVcClient); i++ {
		manager := session.NewManager(vs.multiVcClient[i].Client)
		userSession, err := manager.UserSession(ctx)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		if userSession != nil {
			continue
		} else {
			framework.Logf("Current session is not valid or not authenticated, trying to logout from it")
			err = vs.multiVcClient[i].Logout(ctx)
			if err != nil {
				framework.Logf("Ignoring the log out error: %v", err)
			}
			framework.Logf("Creating new client session after attempting to logout from existing session")
			vs.multiVcClient = newClientForMultiVC(ctx, vs)
		}
	}
}

/*
newClientForMultiVC creates a new client for vSphere connection on a multivc environment
*/
func newClientForMultiVC(ctx context.Context, vs *multiVCvSphere) []*govmomi.Client {
	var clients []*govmomi.Client
	configUser := strings.Split(vs.multivcConfig.Global.User, ",")
	configPwd := strings.Split(vs.multivcConfig.Global.Password, ",")
	configvCenterHostname := strings.Split(vs.multivcConfig.Global.VCenterHostname, ",")
	configvCenterPort := strings.Split(vs.multivcConfig.Global.VCenterPort, ",")
	for i := 0; i < len(configvCenterHostname); i++ {
		framework.Logf("https://%s:%s/sdk", configvCenterHostname[i], configvCenterPort[i])
		url, err := neturl.Parse(fmt.Sprintf("https://%s:%s/sdk",
			configvCenterHostname[i], configvCenterPort[i]))
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		url.User = neturl.UserPassword(configUser[i], configPwd[i])
		client, err := govmomi.NewClient(ctx, url, true)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		err = client.UseServiceVersion(vsanNamespace)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		client.RoundTripper = vim25.Retry(client.RoundTripper, vim25.TemporaryNetworkError(roundTripperDefaultCount))
		clients = append(clients, client)
	}
	return clients
}

/*
connectMultiVcCns creates a CNS client for the virtual center for a multivc environment
*/
func connectMultiVcCns(ctx context.Context, vs *multiVCvSphere) error {
	var err error
	clientMutex.Lock()
	defer clientMutex.Unlock()
	if vs.multiVcCnsClient == nil {
		vs.multiVcCnsClient = make([]*cnsClient, len(vs.multiVcClient))
		for i := 0; i < len(vs.multiVcClient); i++ {
			vs.multiVcCnsClient[i], err = newCnsClient(ctx, vs.multiVcClient[i].Client)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}
	}
	return nil
}

func newVsanHealthSvcClientForMultiVC(ctx context.Context, c *govmomi.Client) (*multiVcVsanClient, error) {
	sc := c.Client.NewServiceClient(vsanHealthPath, vsanNamespace)
	vimClient := c.Client
	return &multiVcVsanClient{
		multiVCvim25Client:   []*vim25.Client{vimClient},
		multiVCserviceClient: []*soap.Client{sc},
	}, nil
}
