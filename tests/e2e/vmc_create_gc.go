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
	ginkgo "github.com/onsi/ginkgo/v2"
	"github.com/onsi/gomega"
	"k8s.io/kubernetes/test/e2e/framework"
)

const (
	gcManifestPath = "testing-manifests/tkg/"
)

var _ = ginkgo.Describe("Create GC", func() {

	ginkgo.BeforeEach(func() {
		bootstrap()
	})

	/*
		Test to Create TKC using devops user
		Steps
			1.	Get WCP session id with devops user
			2.	Create TKC with the session id from step 1
			3.	Verify newly created TKC is up and running
	*/

	ginkgo.It("[vmc] Create GC using devops user", func() {

		ginkgo.By("Get WCP session id")
		gomega.Expect((e2eVSphere.Config.Global.VmcDevopsUser)).NotTo(gomega.BeEmpty(), "Devops user is not set")
		wcpToken := getWCPSessionId(vmcWcpHost, e2eVSphere.Config.Global.VmcDevopsUser,
			e2eVSphere.Config.Global.VmcDevopsPassword)
		framework.Logf("vmcWcpHost %s", vmcWcpHost)

		ginkgo.By("Creating Guest Cluster with Devops User")
		createGC(vmcWcpHost, wcpToken)
		ginkgo.By("Validate the Guest Cluster is up and running")
		err := getGC(vmcWcpHost, wcpToken, devopsTKG)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

	})

	/*
		Test to Create TKC using cloudadmin user
		Steps
			1.	Get WCP session id with cloudadmin user
			2.	Create TKC with the session id from step 1
			3.	Verify newly created TKC is up and running
	*/

	ginkgo.It("[vmc] Create GC using cloudadmin user", func() {

		ginkgo.By("Get WCP session id")
		gomega.Expect((e2eVSphere.Config.Global.VmcCloudUser)).NotTo(gomega.BeEmpty(), "VmcCloudUser is not set")
		wcpToken := getWCPSessionId(vmcWcpHost, e2eVSphere.Config.Global.VmcCloudUser,
			e2eVSphere.Config.Global.VmcCloudPassword)
		framework.Logf("vmcWcpHost %s", vmcWcpHost)

		ginkgo.By("Creating Guest Cluster with cloudadmin User")
		createGC(vmcWcpHost, wcpToken)
		ginkgo.By("Validate the Guest Cluster is up and running")
		err := getGC(vmcWcpHost, wcpToken, cloudadminTKG)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

	})

})
