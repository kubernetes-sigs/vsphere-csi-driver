/*
Copyright 2025 The Kubernetes Authors.

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
package vc

import (
	"context"
	"fmt"
	neturl "net/url"

	"github.com/onsi/gomega"
	"github.com/vmware/govmomi"
	"github.com/vmware/govmomi/vim25"
	"sigs.k8s.io/vsphere-csi-driver/v3/tests/e2e/constants"
	"sigs.k8s.io/vsphere-csi-driver/v3/tests/e2e/env"
)

const (
	RoundTripperDefaultCount = 3
)

// This function creates a new client for vpxd connection.
func NewClient(ctx context.Context, vCenterIp string,
	vCenterIpPort string, user string, password string) *govmomi.Client {
	isPrivateNetwork := env.GetBoolEnvVarOrDefault("IS_PRIVATE_NETWORK", false)
	if isPrivateNetwork {
		vCenterIp = env.GetStringEnvVarOrDefault("LOCAL_HOST_IP", constants.DefaultlocalhostIP)
	}
	url, err := neturl.Parse(fmt.Sprintf("https://%s:%s/sdk",
		vCenterIp, vCenterIpPort))
	gomega.Expect(err).NotTo(gomega.HaveOccurred())
	url.User = neturl.UserPassword(user, password)
	client, err := govmomi.NewClient(ctx, url, true)
	gomega.Expect(err).NotTo(gomega.HaveOccurred())
	err = client.UseServiceVersion(constants.VsanNamespace)
	gomega.Expect(err).NotTo(gomega.HaveOccurred())
	client.RoundTripper = vim25.Retry(client.RoundTripper, vim25.TemporaryNetworkError(RoundTripperDefaultCount))
	return client
}
