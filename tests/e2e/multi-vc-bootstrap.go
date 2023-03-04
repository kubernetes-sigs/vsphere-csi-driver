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

	"github.com/onsi/gomega"
	"k8s.io/kubernetes/test/e2e/framework"
	"k8s.io/kubernetes/test/e2e/framework/testfiles"
)

var e2eVSphere1 vSphere
var testConfig1 *e2eTestConfig

// bootstrap function takes care of initializing necessary tests context for e2e tests
func bootstrap1(withoutDc ...bool) {
	var err error
	testConfig, err = getConfig()
	gomega.Expect(err).NotTo(gomega.HaveOccurred())
	if len(withoutDc) > 0 {
		if withoutDc[0] {
			(*testConfig).Global.Datacenters = ""
		}
	}
	e2eVSphere = vSphere{
		Config: testConfig,
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	connect(ctx, &e2eVSphere)
	if framework.TestContext.RepoRoot != "" {
		testfiles.AddFileSource(testfiles.RootFileSource{Root: framework.TestContext.RepoRoot})
	}
	framework.TestContext.Provider = "vsphere"
}
