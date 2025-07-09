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

	"github.com/onsi/gomega"
	"sigs.k8s.io/vsphere-csi-driver/v3/tests/e2e/clients/cns"
	"sigs.k8s.io/vsphere-csi-driver/v3/tests/e2e/config"
)

// This function take care of creating and providing a cns client to e2e test config.
func ConnectCns(ctx context.Context, e2eTestConfig *config.E2eTestConfig) error {
	var err error
	clientMutex.Lock()
	defer clientMutex.Unlock()
	if e2eTestConfig.CnsClient == nil {
		e2eTestConfig.CnsClient, err = cns.NewCnsClient(ctx, e2eTestConfig.VcClient.Client)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
	}
	return nil
}
