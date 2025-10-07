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

package bootstrap

import (
	"context"
	"os"
	"path/filepath"
	"strings"

	"github.com/onsi/gomega"
	cnstypes "github.com/vmware/govmomi/cns/types"
	"k8s.io/kubernetes/test/e2e/framework"
	"k8s.io/kubernetes/test/e2e/framework/testfiles"
	"sigs.k8s.io/vsphere-csi-driver/v3/tests/e2e/clients/cns"
	"sigs.k8s.io/vsphere-csi-driver/v3/tests/e2e/clients/vc"
	"sigs.k8s.io/vsphere-csi-driver/v3/tests/e2e/config"
	"sigs.k8s.io/vsphere-csi-driver/v3/tests/e2e/constants"
	"sigs.k8s.io/vsphere-csi-driver/v3/tests/e2e/env"
)

// bootstrap function takes care of initializing necessary tests context for e2e tests
func Bootstrap(optionArgs ...bool) *config.E2eTestConfig {
	var err error
	var testInputData *config.TestInputData
	var e2eTestConfig *config.E2eTestConfig
	testInputData, err = config.GetConfig()
	gomega.Expect(err).NotTo(gomega.HaveOccurred())
	if len(optionArgs) > 0 {
		if optionArgs[0] {
			(*testInputData).Global.Datacenters = ""
		}
	}
	e2eTestConfig = &config.E2eTestConfig{
		TestInput: testInputData,
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	e2eTestConfig.VcClient = vc.NewClient(ctx, e2eTestConfig.TestInput.Global.VCenterHostname,
		e2eTestConfig.TestInput.Global.VCenterPort, e2eTestConfig.TestInput.Global.User,
		e2eTestConfig.TestInput.Global.Password)

	e2eTestConfig.CnsClient, _ = cns.NewCnsClient(ctx, e2eTestConfig.VcClient.Client)

	if framework.TestContext.RepoRoot != "" {
		testfiles.AddFileSource(testfiles.RootFileSource{Root: framework.TestContext.RepoRoot})
	}
	framework.TestContext.Provider = "vsphere"
	initTestInputData(e2eTestConfig.TestInput)
	return e2eTestConfig

}

// This function initialize test env with required input data
func initTestInputData(testConfig *config.TestInputData) {
	// k8s.io/kubernetes/tests/e2e/framework requires env KUBECONFIG to be set
	// it does not fall back to defaults
	if os.Getenv(constants.KubeconfigEnvVar) == "" {
		kubeconfig := filepath.Join(os.Getenv("HOME"), ".kube", "config")
		os.Setenv(constants.KubeconfigEnvVar, kubeconfig)
	}
	clusterFlavor := cnstypes.CnsClusterFlavor(os.Getenv(constants.EnvClusterFlavor))
	setClusterFlavor(testConfig, clusterFlavor)
	setSShdPort(testConfig)
	env.SetRequiredImages(testConfig)
	env.SetKubeEnv()
}

// this function sets the boolean variables w.r.t the Cluster type.
func setClusterFlavor(testConfig *config.TestInputData, clusterFlavor cnstypes.CnsClusterFlavor) {
	switch clusterFlavor {
	case cnstypes.CnsClusterFlavorWorkload:
		testConfig.ClusterFlavor.SupervisorCluster = true
	case cnstypes.CnsClusterFlavorGuest:
		testConfig.ClusterFlavor.GuestCluster = true
	default:
		testConfig.ClusterFlavor.VanillaCluster = true
	}

	// Check if the access mode is set for File volume setups
	kind := os.Getenv(constants.EnvAccessMode)
	if strings.TrimSpace(string(kind)) == "RWX" {
		testConfig.TestBedInfo.RwxAccessMode = true
	}

	// Check if its the vcptocsi tesbed
	mode := os.Getenv(constants.EnvVpToCsi)
	if strings.TrimSpace(string(mode)) == "1" {
		testConfig.TestBedInfo.Vcptocsi = true
	}
	//Check if its windows env
	workerNode := os.Getenv(constants.EnvWorkerType)
	if strings.TrimSpace(string(workerNode)) == "WINDOWS" {
		testConfig.TestBedInfo.WindowsEnv = true
	}

	// Check if it's multiple supervisor cluster setup
	svcType := os.Getenv(constants.EnvSupervisorType)
	if strings.TrimSpace(string(svcType)) == "MULTI_SVC" {
		testConfig.TestBedInfo.MultipleSvc = true
	}

	//Check if it is multivc env
	topologyType := os.Getenv(constants.EnvTopologyType)
	if strings.TrimSpace(string(topologyType)) == "MULTI_VC" {
		testConfig.TestBedInfo.Multivc = true
	}

	//Check if its stretched SVC testbed
	testbedType := os.Getenv(constants.EnvStretchedSvc)
	if strings.TrimSpace(string(testbedType)) == "1" {
		testConfig.TestBedInfo.StretchedSVC = true
	}
}

/*
The setSShdPort function dynamically configures SSH port mappings for a vSphere test environment by reading
environment variables and adapting to the network type and topology.
It sets up SSH access to vCenter servers, ESXi hosts, and Kubernetes masters based on the environment configuration.
*/
func setSShdPort(testConfig *config.TestInputData) {
	testConfig.TestBedInfo.VcAddress = env.GetAndExpectEnvVar(constants.EnvVcIP1)
	testConfig.TestBedInfo.IsPrivateNetwork = env.GetBoolEnvVarOrDefault("IS_PRIVATE_NETWORK", false)

	if testConfig.TestBedInfo.Multivc {
		testConfig.TestBedInfo.VcAddress2 = env.GetAndExpectEnvVar(constants.EnvVcIP2)
		testConfig.TestBedInfo.VcAddress3 = env.GetAndExpectEnvVar(constants.EnvVcIP3)
	}

	if testConfig.TestBedInfo.IsPrivateNetwork {
		if testConfig.TestBedInfo.Multivc {
			testConfig.TestBedInfo.VcIp2SshPortNum = env.GetorIgnoreStringEnvVar(constants.EnvVc2SshdPortNum)
			testConfig.TestBedInfo.VcIp3SshPortNum = env.GetorIgnoreStringEnvVar(constants.EnvVc3SshdPortNum)

			safeInsertToMap(testConfig, testConfig.TestBedInfo.VcAddress2, testConfig.TestBedInfo.VcIp2SshPortNum)
			safeInsertToMap(testConfig, testConfig.TestBedInfo.VcAddress3, testConfig.TestBedInfo.VcIp3SshPortNum)
		}

		// reading masterIP and its port number
		testConfig.TestBedInfo.MasterIP1 = env.GetorIgnoreStringEnvVar(constants.EnvMasterIP1)
		testConfig.TestBedInfo.MasterIP2 = env.GetorIgnoreStringEnvVar(constants.EnvMasterIP2)
		testConfig.TestBedInfo.MasterIP3 = env.GetorIgnoreStringEnvVar(constants.EnvMasterIP3)
		testConfig.TestBedInfo.K8sMasterIp1PortNum = env.GetorIgnoreStringEnvVar(constants.EnvMasterIP1SshdPortNum)
		testConfig.TestBedInfo.K8sMasterIp2PortNum = env.GetorIgnoreStringEnvVar(constants.EnvMasterIP2SshdPortNum)
		testConfig.TestBedInfo.K8sMasterIp3PortNum = env.GetorIgnoreStringEnvVar(constants.EnvMasterIP3SshdPortNum)

		testConfig.TestBedInfo.VcIp1SshPortNum = env.GetorIgnoreStringEnvVar(constants.EnvVc1SshdPortNum)

		// reading esxi ip and its port
		testConfig.TestBedInfo.EsxIp1 = env.GetorIgnoreStringEnvVar(constants.EnvEsxIp1)
		testConfig.TestBedInfo.EsxIp1PortNum = env.GetorIgnoreStringEnvVar(constants.EnvEsx1PortNum)
		testConfig.TestBedInfo.EsxIp2PortNum = env.GetorIgnoreStringEnvVar(constants.EnvEsx2PortNum)
		testConfig.TestBedInfo.EsxIp3PortNum = env.GetorIgnoreStringEnvVar(constants.EnvEsx3PortNum)
		testConfig.TestBedInfo.EsxIp4PortNum = env.GetorIgnoreStringEnvVar(constants.EnvEsx4PortNum)
		testConfig.TestBedInfo.EsxIp5PortNum = env.GetorIgnoreStringEnvVar(constants.EnvEsx5PortNum)
		testConfig.TestBedInfo.EsxIp6PortNum = env.GetorIgnoreStringEnvVar(constants.EnvEsx6PortNum)
		testConfig.TestBedInfo.EsxIp7PortNum = env.GetorIgnoreStringEnvVar(constants.EnvEsx7PortNum)
		testConfig.TestBedInfo.EsxIp8PortNum = env.GetorIgnoreStringEnvVar(constants.EnvEsx8PortNum)
		testConfig.TestBedInfo.EsxIp9PortNum = env.GetorIgnoreStringEnvVar(constants.EnvEsx9PortNum)
		testConfig.TestBedInfo.EsxIp10PortNum = env.GetorIgnoreStringEnvVar(constants.EnvEsx10PortNum)

		testConfig.TestBedInfo.EsxIp2 = env.GetorIgnoreStringEnvVar(constants.EnvEsxIp2)
		testConfig.TestBedInfo.EsxIp3 = env.GetorIgnoreStringEnvVar(constants.EnvEsxIp3)
		testConfig.TestBedInfo.EsxIp4 = env.GetorIgnoreStringEnvVar(constants.EnvEsxIp4)
		testConfig.TestBedInfo.EsxIp5 = env.GetorIgnoreStringEnvVar(constants.EnvEsxIp5)
		testConfig.TestBedInfo.EsxIp6 = env.GetorIgnoreStringEnvVar(constants.EnvEsxIp6)
		testConfig.TestBedInfo.EsxIp7 = env.GetorIgnoreStringEnvVar(constants.EnvEsxIp7)
		testConfig.TestBedInfo.EsxIp8 = env.GetorIgnoreStringEnvVar(constants.EnvEsxIp8)
		testConfig.TestBedInfo.EsxIp9 = env.GetorIgnoreStringEnvVar(constants.EnvEsxIp9)
		testConfig.TestBedInfo.EsxIp10 = env.GetorIgnoreStringEnvVar(constants.EnvEsxIp10)

		safeInsertToMap(testConfig, testConfig.TestBedInfo.VcAddress, constants.VcIp1SshPortNum)
		safeInsertToMap(testConfig, testConfig.TestBedInfo.MasterIP1, constants.K8sMasterIp1PortNum)
		safeInsertToMap(testConfig, testConfig.TestBedInfo.MasterIP2, constants.K8sMasterIp2PortNum)
		safeInsertToMap(testConfig, testConfig.TestBedInfo.MasterIP3, constants.K8sMasterIp3PortNum)
		safeInsertToMap(testConfig, testConfig.TestBedInfo.EsxIp1, constants.EsxIp1PortNum)
		safeInsertToMap(testConfig, testConfig.TestBedInfo.EsxIp2, constants.EsxIp2PortNum)
		safeInsertToMap(testConfig, testConfig.TestBedInfo.EsxIp3, constants.EsxIp3PortNum)
		safeInsertToMap(testConfig, testConfig.TestBedInfo.EsxIp4, constants.EsxIp4PortNum)
		safeInsertToMap(testConfig, testConfig.TestBedInfo.EsxIp5, constants.EsxIp5PortNum)
		safeInsertToMap(testConfig, testConfig.TestBedInfo.EsxIp6, constants.EsxIp6PortNum)
		safeInsertToMap(testConfig, testConfig.TestBedInfo.EsxIp7, constants.EsxIp7PortNum)
		safeInsertToMap(testConfig, testConfig.TestBedInfo.EsxIp8, constants.EsxIp8PortNum)
		safeInsertToMap(testConfig, testConfig.TestBedInfo.EsxIp9, constants.EsxIp9PortNum)
		safeInsertToMap(testConfig, testConfig.TestBedInfo.EsxIp10, constants.EsxIp10PortNum)
	}
}

/*
This function add ip and port map to testinput data
*/
func safeInsertToMap(testConfig *config.TestInputData, key, value string) {
	if key != "" && value != "" {
		testConfig.TestBedInfo.IpPortMap[key] = value
	}
}
