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

package localtests_test

import (
	"testing"

	ginkgo "github.com/onsi/ginkgo/v2"
	gomega "github.com/onsi/gomega"
	_ "k8s.io/kubernetes/test/e2e/framework/debug/init"
)

// func init() {
// 	framework.AfterReadingAllFlags(&framework.TestContext)
// }

func TestE2E(t *testing.T) {
	gomega.RegisterFailHandler(ginkgo.Fail)
	_, reporterConfig := ginkgo.GinkgoConfiguration()
	reporterConfig.JUnitReport = "junit.xml"
	ginkgo.RunSpecs(t, "Basic-Tests", reporterConfig)
}
