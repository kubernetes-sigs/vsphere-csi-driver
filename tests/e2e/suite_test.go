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
	"flag"
	"os"
	"path/filepath"
	"strings"
	"testing"

	cnstypes "github.com/vmware/govmomi/cns/types"

	ginkgo "github.com/onsi/ginkgo/v2"
	gomega "github.com/onsi/gomega"
	"k8s.io/kubernetes/test/e2e/framework"
	"k8s.io/kubernetes/test/e2e/framework/config"
)

const kubeconfigEnvVar = "KUBECONFIG"
const busyBoxImageEnvVar = "BUSYBOX_IMAGE"
const windowsImageEnvVar = "WINDOWS_IMAGE"

func init() {
	// k8s.io/kubernetes/tests/e2e/framework requires env KUBECONFIG to be set
	// it does not fall back to defaults
	if os.Getenv(kubeconfigEnvVar) == "" {
		kubeconfig := filepath.Join(os.Getenv("HOME"), ".kube", "config")
		os.Setenv(kubeconfigEnvVar, kubeconfig)
	}

	framework.AfterReadingAllFlags(&framework.TestContext)
	clusterFlavor := cnstypes.CnsClusterFlavor(os.Getenv(envClusterFlavor))
	setClusterFlavor(clusterFlavor)

	if os.Getenv(busyBoxImageEnvVar) != "" {
		busyBoxImageOnGcr = os.Getenv(busyBoxImageEnvVar)
	}

	if os.Getenv(windowsImageEnvVar) != "" {
		windowsImageOnMcr = os.Getenv(windowsImageEnvVar)
	}
}

func TestE2E(t *testing.T) {
	handleFlags()
	gomega.RegisterFailHandler(ginkgo.Fail)
	_, reporterConfig := ginkgo.GinkgoConfiguration()
	reporterConfig.JUnitReport = "junit.xml"
	ginkgo.RunSpecs(t, "CNS CSI Driver End-to-End Tests", reporterConfig)
}

func handleFlags() {
	config.CopyFlags(config.Flags, flag.CommandLine)
	framework.RegisterCommonFlags(flag.CommandLine)
	framework.TestContext.KubeConfig = os.Getenv(kubeconfigEnvVar)
	mydir, err := os.Getwd()
	framework.ExpectNoError(err)
	framework.TestContext.RepoRoot = strings.ReplaceAll(mydir, "/tests/e2e", "")
	flag.Parse()
}
