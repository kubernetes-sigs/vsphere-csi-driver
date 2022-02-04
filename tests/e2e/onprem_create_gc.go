/*
Copyright 2021 The Kubernetes Authors.

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
	"github.com/onsi/ginkgo"
	"github.com/onsi/gomega"
	"k8s.io/kubernetes/test/e2e/framework"
)

var _ = ginkgo.Describe("Create GC", func() {

	ginkgo.BeforeEach(func() {
		bootstrap()
	})

	/*
		Test to Create TKC using root user
		Steps
			1.	Get WCP session id with devops user
			2.	Create TKC with the session id from step 1
			3.	Verify newly created TKC is up and running
	*/
	ginkgo.It("[csi-guest] Create GC onprem with Root user", func() {
		ginkgo.By("Get WCP session id")

		sessionID := getVCentreSessionId(e2eVSphere.Config.Global.VCenterHostname, e2eVSphere.Config.Global.User,
			e2eVSphere.Config.Global.Password)
		wcpCluster := getWCPCluster(sessionID, e2eVSphere.Config.Global.VCenterHostname)
		wcpHost := getWCPHost(wcpCluster, e2eVSphere.Config.Global.VCenterHostname, sessionID)
		framework.Logf("wcphost %s", wcpHost)
		wcpToken := getWCPSessionId(wcpHost, e2eVSphere.Config.Global.User, e2eVSphere.Config.Global.Password)
		updatedtkg := setupTKGyaml(wcpHost, wcpToken, devopsTKG, onpremrootTKG)
		framework.Logf("Secound Guest cluster %s", updatedtkg)

		ginkgo.By("Creating Guest Cluster with root User")
		createGC(wcpHost, wcpToken)
		ginkgo.By("Validate the Guest Cluster is up and running")
		err := getGC(wcpHost, wcpToken, onpremrootTKG)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

	})

})
