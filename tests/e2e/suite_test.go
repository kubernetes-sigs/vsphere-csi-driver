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
	"flag"
	"os"
	"path/filepath"
	"testing"

	cnstypes "github.com/vmware/govmomi/cns/types"

	ginkgo "github.com/onsi/ginkgo"
	"github.com/onsi/ginkgo/reporters"
	gomega "github.com/onsi/gomega"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/kubernetes/test/e2e/framework"
	"k8s.io/kubernetes/test/e2e/framework/config"
)

const kubeconfigEnvVar = "KUBECONFIG"

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
}

func TestE2E(t *testing.T) {
	handleFlags()
	gomega.RegisterFailHandler(ginkgo.Fail)
	junitReporter := reporters.NewJUnitReporter("junit.xml")
	ginkgo.RunSpecsWithDefaultAndCustomReporters(t, "CNS CSI Driver End-to-End Tests",
		[]ginkgo.Reporter{junitReporter})
}

func handleFlags() {
	config.CopyFlags(config.Flags, flag.CommandLine)
	framework.RegisterCommonFlags(flag.CommandLine)
	framework.RegisterClusterFlags(flag.CommandLine)
	flag.Parse()
}

var _ = ginkgo.BeforeSuite(func() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	kubeconfig := os.Getenv(kubeconfigEnvVar)
	if kubeconfig == "" {
		kubeconfig = filepath.Join(os.Getenv("HOME"), ".kube", "config")
	}
	client, err := createKubernetesClientFromConfig(kubeconfig)
	gomega.Expect(err).NotTo(gomega.HaveOccurred())

	if k8senv := GetAndExpectStringEnvVar("SUPERVISOR_CLUSTER_KUBE_CONFIG"); k8senv != "" {
		svcClient, err := createKubernetesClientFromConfig(k8senv)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		// Get SVC CSI Replica Count
		svcDeployment, err := svcClient.AppsV1().Deployments(csiSystemNamespace).Get(ctx,
			vSphereCSIControllerPodNamePrefix, metav1.GetOptions{})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		svcCsiReplicaCount = *svcDeployment.Spec.Replicas
	}
	deployment, err := client.AppsV1().Deployments(csiSystemNamespace).Get(ctx,
		vSphereCSIControllerPodNamePrefix, metav1.GetOptions{})
	gomega.Expect(err).NotTo(gomega.HaveOccurred())
	csiReplicaCount = *deployment.Spec.Replicas
})
