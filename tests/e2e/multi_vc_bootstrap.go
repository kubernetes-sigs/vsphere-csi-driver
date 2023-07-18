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

	"github.com/onsi/gomega"
	"k8s.io/kubernetes/test/e2e/framework"
	"k8s.io/kubernetes/test/e2e/framework/testfiles"
)

var multiVCe2eVSphere multiVCvSphere
var multiVCtestConfig *e2eTestConfig

/*
multiVCbootstrap function takes care of initializing necessary tests context for e2e tests
*/
func multiVCbootstrap(withoutDc ...bool) {
	var err error
	multiVCtestConfig, err = getConfig()
	gomega.Expect(err).NotTo(gomega.HaveOccurred())
	if len(withoutDc) > 0 {
		if withoutDc[0] {
			(*multiVCtestConfig).Global.Datacenters = ""
		}
	}
	multiVCe2eVSphere = multiVCvSphere{
		multivcConfig: multiVCtestConfig,
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	// connects and verifies multiple VC connections
	connectMultiVC(ctx, &multiVCe2eVSphere)

	if framework.TestContext.RepoRoot != "" {
		testfiles.AddFileSource(testfiles.RootFileSource{Root: framework.TestContext.RepoRoot})
	}
	framework.TestContext.Provider = "vsphere"
}
