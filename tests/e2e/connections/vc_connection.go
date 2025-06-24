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
package connections

import (
	"context"
	"sync"

	"github.com/onsi/gomega"
	"github.com/vmware/govmomi/session"
	"k8s.io/kubernetes/test/e2e/framework"
	"sigs.k8s.io/vsphere-csi-driver/v3/tests/e2e/clients/vc"
	"sigs.k8s.io/vsphere-csi-driver/v3/tests/e2e/config"
)

var (
	clientMutex sync.Mutex
	clientLock  sync.Mutex
)

// This function take care of connecting to a vc server with credentials provided in e2e test config.
// No actions are taken if a connection exists and alive.
// Otherwise, a new client will be created.
func ConnectToVC(ctx context.Context, e2eTestConfig *config.E2eTestConfig, forceRefresh ...bool) {
	clientLock.Lock()
	var err error
	refresh := false

	if len(forceRefresh) > 0 {
		if forceRefresh[0] {
			refresh = true
		}
	}

	defer clientLock.Unlock()
	if e2eTestConfig.VcClient == nil {
		framework.Logf("Creating new VC session")
		e2eTestConfig.VcClient = vc.NewClient(ctx, e2eTestConfig.TestInput.Global.VCenterHostname,
			e2eTestConfig.TestInput.Global.VCenterPort, e2eTestConfig.TestInput.Global.User,
			e2eTestConfig.TestInput.Global.Password)
	}
	manager := session.NewManager(e2eTestConfig.VcClient.Client)
	userSession, err := manager.UserSession(ctx)
	gomega.Expect(err).NotTo(gomega.HaveOccurred())

	if userSession != nil && !refresh {
		return
	}

	framework.Logf("Current session is not valid or not authenticated, trying to logout from it")
	err = e2eTestConfig.VcClient.Logout(ctx)
	if err != nil {
		framework.Logf("Ignoring the log out error: %v", err)
	}
	framework.Logf("Creating new client session after attempting to logout from existing session")
	e2eTestConfig.VcClient = vc.NewClient(ctx, e2eTestConfig.TestInput.Global.VCenterHostname,
		e2eTestConfig.TestInput.Global.VCenterPort, e2eTestConfig.TestInput.Global.User,
		e2eTestConfig.TestInput.Global.Password)
}
