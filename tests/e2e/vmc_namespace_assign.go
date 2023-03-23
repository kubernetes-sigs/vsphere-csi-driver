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
	ginkgo "github.com/onsi/ginkgo/v2"
	"github.com/onsi/gomega"
	// "[k8s.io/kubernetes/test/e2e/framework](http://k8s.io/kubernetes/test/e2e/framework)"
)

const (
	nsManifestPath = "testing-manifests/tkg/"
)

var _ = ginkgo.Describe("Create NS", func() {

	ginkgo.BeforeEach(func() {
		bootstrap()
	})

	/*
	   Test to Create TKC using devops user
	   Steps
	           1.      Get WCP session id with devops user
	           2.      Create TKC with the session id from step 1
	           3.      Verify newly created TKC is up and running
	*/

	/*
	   Test to Create TKC using cloudadmin user
	   Steps
	           1.      Get WCP session id with cloudadmin user
	           2.      Create TKC with the session id from step 1
	           3.      Verify newly created TKC is up and running
	*/

	ginkgo.It("[vmc] Create NAMESPACE using cloudadmin user and assign Devops User", func() {

		ginkgo.By("Get WCP session id")
		gomega.Expect((e2eVSphere.Config.Global.VmcCloudUser)).NotTo(gomega.BeEmpty(), "VmcCloudUser is not set")
		sessionid := getVCentreSessionId(e2eVSphere.Config.Global.VCenterHostname, e2eVSphere.Config.Global.User,
			e2eVSphere.Config.Global.Password)

		ginkgo.By("Creating namespace  with cloudadmin User")
		createnamespace(sessionid, e2eVSphere.Config.Global.VCenterHostname)

	})

})
