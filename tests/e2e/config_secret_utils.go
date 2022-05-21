/*
Copyright 2022 The Kubernetes Authors.

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
	"fmt"
	"time"

	"github.com/onsi/ginkgo"
	"github.com/onsi/gomega"
	"golang.org/x/crypto/ssh"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	clientset "k8s.io/client-go/kubernetes"
	"k8s.io/kubernetes/test/e2e/framework"
	fssh "k8s.io/kubernetes/test/e2e/framework/ssh"
)

var insecureFlag bool
var clusterId string
var clusterDistribution string
var dataCenters string
var vCenterPort string
var user string
var password string
var vCenterIp string

func govcLoginCmd(vcAddress string) string {
	loginCmd := "export GOVC_INSECURE=1;"
	loginCmd += fmt.Sprintf("export GOVC_URL='https://administrator@vsphere.local:Admin!23@%s';", vcAddress)
	framework.Logf(loginCmd)
	return loginCmd
}

func createTestUser(loginCmd string, sshClientConfig *ssh.ClientConfig, masterIp string,
	testUser string, testUserPassword string) error {
	// create 2 test users
	createUser := loginCmd + "govc sso.user.create -p " + testUserPassword + " " + testUser
	framework.Logf(createUser)
	result, err := sshExec(sshClientConfig, masterIp, createUser)
	if err != nil && result.Code != 0 {
		fssh.LogResult(result)
		return fmt.Errorf("couldn't execute command: %s on host: %v , error: %s",
			createUser, masterIp, err)
	}
	return nil
}

func deleteUsersRolesAndPermissions(loginCmd string, sshClientConfig *ssh.ClientConfig,
	masterIp string, testUser string,
	dataCenter string, cluster string) error {
	// delete Permissions
	deletePermissions := loginCmd + "govc permissions.remove -principal " + testUser +
		"@vsphere.local " + dataCenter + ";" +
		"while read i; do govc permissions.remove -principal " + testUser + "@vsphere.local $i;done </root/hosts.txt;" +
		"while read i; do govc permissions.remove -principal " + testUser + "@vsphere.local $i;done </root/k8s_vm.txt;" +
		"govc permissions.remove -principal " + testUser + "@vsphere.local " + cluster + ";" +
		"govc permissions.remove -principal " + testUser + "@vsphere.local /" + dataCenter + "/datastore/vsanDatastore;" +
		"govc permissions.remove -principal " + testUser + "@vsphere.local /" + dataCenter + "/datastore/local-0;" +
		"govc permissions.remove -principal " + testUser + "@vsphere.local /" + dataCenter + "'/datastore/local-0 (1)';" +
		"govc permissions.remove -principal " + testUser + "@vsphere.local "
	framework.Logf(deletePermissions)
	result, err := sshExec(sshClientConfig, masterIp, deletePermissions)
	if err != nil && result.Code != 0 {
		fssh.LogResult(result)
		return fmt.Errorf("couldn't execute command: %s on host: %v , error: %s",
			deletePermissions, masterIp, err)
	}

	// delete roles of testusers
	deleteRoles := loginCmd + "govc role.remove CNS-DATASTORE-" + testUser + ";" +
		"govc role.remove CNS-HOST-CONFIG-STORAGE-" + testUser + ";" +
		"govc role.remove CNS-VM-" + testUser + ";" +
		"govc role.remove CNS-SEARCH-AND-SPBM-" + testUser
	framework.Logf(deleteRoles)
	result, err = sshExec(sshClientConfig, masterIp, deleteRoles)
	if err != nil && result.Code != 0 {
		fssh.LogResult(result)
		return fmt.Errorf("couldn't execute command: %s on host: %v , error: %s",
			deleteRoles, masterIp, err)
	}

	// delete test user
	deleteUser := loginCmd + "govc sso.user.rm " + testUser
	framework.Logf(deleteUser)
	result, err = sshExec(sshClientConfig, masterIp, deleteUser)
	if err != nil && result.Code != 0 {
		fssh.LogResult(result)
		return fmt.Errorf("couldn't execute command: %s on host: %v , error: %s",
			deleteUser, masterIp, err)
	}
	return nil
}

func createRolesForTestUser(loginCmd string, sshClientConfig *ssh.ClientConfig,
	masterIp string, testUser string) error {
	createRoleCmdFortestUser := loginCmd + "govc role.create CNS-DATASTORE-" + testUser +
		" Datastore.FileManagement;" +
		"govc role.create CNS-HOST-CONFIG-STORAGE-" + testUser +
		" Host.Config.Storage;" +
		"govc role.create CNS-VM-" + testUser +
		" VirtualMachine.Config.AddExistingDisk VirtualMachine.Config.AddRemoveDevice;" +
		"govc role.create CNS-SEARCH-AND-SPBM-" + testUser +
		" Cns.Searchable StorageProfile.View"
	framework.Logf(createRoleCmdFortestUser)
	result, err := sshExec(sshClientConfig, masterIp, createRoleCmdFortestUser)
	if err != nil && result.Code != 0 {
		fssh.LogResult(result)
		return fmt.Errorf("couldn't execute command: %s on host: %v , error: %s",
			createRoleCmdFortestUser, masterIp, err)
	}
	return nil
}

func getDataCenterClusterHostAndVmDetails(loginCmd string, sshClientConfig *ssh.ClientConfig,
	masterIp string) (string, string, error) {
	// fetch datacenter details
	ginkgo.By("Executing command for fetching data center details")
	dataCenter := loginCmd + "govc datacenter.info | grep -i Name | awk '{print $2}' | tr -d '\n'"
	framework.Logf(dataCenter)
	dcResult, err := sshExec(sshClientConfig, masterIp, dataCenter)
	if err != nil && dcResult.Code != 0 {
		fssh.LogResult(dcResult)
		return "", "", fmt.Errorf("couldn't execute command: %s on host: %v , error: %s",
			dataCenter, masterIp, err)
	}

	// fetch cluster details
	ginkgo.By("Executing command for fetching cluster details")
	cluster := loginCmd + "govc ls /" + dcResult.Stdout + "/host | tr -d '\n'"
	framework.Logf(cluster)
	clusterResult, err := sshExec(sshClientConfig, masterIp, cluster)
	if err != nil && clusterResult.Code != 0 {
		fssh.LogResult(clusterResult)
		return "", "", fmt.Errorf("couldn't execute command: %s on host: %v , error: %s",
			cluster, masterIp, err)
	}

	// Get all esxi hosts details
	ginkgo.By("Executing command for fetching esxi host details")
	hosts := loginCmd + "govc ls " + clusterResult.Stdout + " " + " | grep 10. > /root/hosts.txt | tr -d '\n'"
	framework.Logf(hosts)
	hostsResult, err := sshExec(sshClientConfig, masterIp, hosts)
	if err != nil && hostsResult.Code != 0 {
		fssh.LogResult(hostsResult)
		return "", "", fmt.Errorf("couldn't execute command: %s on host: %v , error: %s",
			hosts, masterIp, err)
	}

	// Get all k8s vm details
	ginkgo.By("Executing command for fetching k8s vm details")
	vms := loginCmd + "govc ls /" + dcResult.Stdout + "/vm" + " " + "| grep k8s > /root/k8s_vm.txt | tr -d '\n'"
	framework.Logf(vms)
	vMsResult, err := sshExec(sshClientConfig, masterIp, hosts)
	if err != nil && vMsResult.Code != 0 {
		fssh.LogResult(vMsResult)
		return "", "", fmt.Errorf("couldn't execute command: %s on host: %v , error: %s",
			vms, masterIp, err)
	}
	return dcResult.Stdout, clusterResult.Stdout, nil
}

func setDataCenterLevelPermission(loginCmd string, sshClientConfig *ssh.ClientConfig,
	masterIp string, dataCenter string, testUser string) error {
	setPermissionForDataCenter := loginCmd + "govc permissions.set -principal " + testUser +
		"@vsphere.local -propagate=false -role ReadOnly " + dataCenter + " | tr -d '\n'"
	framework.Logf(setPermissionForDataCenter)
	result, err := sshExec(sshClientConfig, masterIp, setPermissionForDataCenter)
	if err != nil && result.Code != 0 {
		fssh.LogResult(result)
		return fmt.Errorf("couldn't execute command: %s on host: %v , error: %s",
			setPermissionForDataCenter, masterIp, err)
	}
	return nil
}

func setHostLevelPermission(loginCmd string, sshClientConfig *ssh.ClientConfig,
	masterIp string, testUser string) error {
	setPermissionForHosts := loginCmd + "while read i; do govc permissions.set -principal " +
		testUser + "@vsphere.local -propagate=false -role ReadOnly $i;done </root/hosts.txt | tr -d '\n'"
	framework.Logf(setPermissionForHosts)
	result, err := sshExec(sshClientConfig, masterIp, setPermissionForHosts)
	if err != nil && result.Code != 0 {
		fssh.LogResult(result)
		return fmt.Errorf("couldn't execute command: %s on host: %v , error: %s",
			setPermissionForHosts, masterIp, err)
	}
	return nil
}

func setK8sVMLevelPermission(loginCmd string, sshClientConfig *ssh.ClientConfig,
	masterIp string, testUser string) error {
	setPermissionForK8sVms := loginCmd + "while read i; do govc permissions.set -principal " +
		testUser + "@vsphere.local -propagate=false -role CNS-VM-" + testUser +
		" $i;done </root/k8s_vm.txt | tr -d '\n'"
	framework.Logf(setPermissionForK8sVms)
	result, err := sshExec(sshClientConfig, masterIp, setPermissionForK8sVms)
	if err != nil && result.Code != 0 {
		fssh.LogResult(result)
		return fmt.Errorf("couldn't execute command: %s on host: %v , error: %s",
			setPermissionForK8sVms, masterIp, err)
	}
	return nil
}

func setClusterLevelPermission(loginCmd string, sshClientConfig *ssh.ClientConfig,
	masterIp string, testUser string, cluster string) error {
	setPermissionForCluster := loginCmd + "govc permissions.set -principal " +
		testUser + "@vsphere.local " +
		"-propagate=false -role CNS-HOST-CONFIG-STORAGE-" + testUser + " " + cluster + " | tr -d '\n'"
	framework.Logf(setPermissionForCluster)
	result, err := sshExec(sshClientConfig, masterIp, setPermissionForCluster)
	if err != nil && result.Code != 0 {
		fssh.LogResult(result)
		return fmt.Errorf("couldn't execute command: %s on host: %v , error: %s",
			setPermissionForCluster, masterIp, err)
	}
	return nil
}

func setDataStoreLevelPermission(loginCmd string, sshClientConfig *ssh.ClientConfig,
	masterIp string, testUser string, dataCenter string) error {
	setPermissionForDataStore := loginCmd + "govc permissions.set -principal " + testUser + "@vsphere.local " +
		"-propagate=false -role  CNS-DATASTORE-" + testUser + " " + "/" + dataCenter + "/datastore/vsanDatastore;" +
		"govc permissions.set -principal " + testUser + "@vsphere.local -propagate=false -role " +
		"CNS-DATASTORE-" + testUser + " " + "/" + dataCenter + "/datastore/local-0;" +
		"govc permissions.set -principal " + testUser + "@vsphere.local -propagate=false -role " +
		"CNS-DATASTORE-" + testUser + " " + "/" + dataCenter + "'/datastore/local-0 (1)';" +
		"govc permissions.set -principal " + testUser + "@vsphere.local " +
		"-propagate=false -role  CNS-SEARCH-AND-SPBM-" + testUser + " /"
	framework.Logf(setPermissionForDataStore)
	result, err := sshExec(sshClientConfig, masterIp, setPermissionForDataStore)
	if err != nil && result.Code != 0 {
		fssh.LogResult(result)
		return fmt.Errorf("couldn't execute command: %s on host: %v , error: %s",
			setPermissionForDataStore, masterIp, err)
	}
	return nil
}

func createCsiVsphereConfFile(clusterName string, clusterDistribution string, vCenterIP string,
	inSecureFlag bool, testUser string, password string, vCenterPort string,
	dataCenter string, loginCmd string, sshClientConfig *ssh.ClientConfig,
	masterIp string) error {
	conf := (fmt.Sprintf("tee csi-vsphere.conf >/dev/null <<EOF\n[Global]\ncluster-id = \"%s\"\n"+
		"cluster-distribution = \"%s\"\n\n[VirtualCenter \"%s\"]\n"+
		"insecure-flag = \"%t\"\nuser = \"%s\"\npassword = \"%s\"\nport = \"%s\"\n"+
		"datacenters = \"%s\"\nEOF",
		clusterName, clusterDistribution, vCenterIP, inSecureFlag, testUser, password,
		vCenterPort, dataCenter))
	framework.Logf(conf)
	result, err := sshExec(sshClientConfig, masterIp, conf)
	if err != nil && result.Code != 0 {
		fssh.LogResult(result)
		return fmt.Errorf("couldn't execute command: %s on host: %v , error: %s",
			conf, masterIp, err)
	}
	applyConf := "kubectl create secret generic vsphere-config-secret --from-file=csi-vsphere.conf " +
		"-n vmware-system-csi"
	framework.Logf(applyConf)
	result, err = sshExec(sshClientConfig, masterIp, applyConf)
	if err != nil && result.Code != 0 {
		fssh.LogResult(result)
		return fmt.Errorf("couldn't execute command: %s on host: %v , error: %s",
			applyConf, masterIp, err)
	}
	return nil

}

func deleteCsiVsphereConfFile(loginCmd string, sshClientConfig *ssh.ClientConfig,
	masterIp string) error {
	deleteConf := "kubectl delete secret vsphere-config-secret --namespace=vmware-system-csi"
	framework.Logf(deleteConf)
	result, err := sshExec(sshClientConfig, masterIp, deleteConf)
	if err != nil && result.Code != 0 {
		fssh.LogResult(result)
		return fmt.Errorf("couldn't execute command: %s on host: %v , error: %s",
			deleteConf, masterIp, err)
	}
	return nil
}

func installCsiDriver(loginCmd string, sshClientConfig *ssh.ClientConfig,
	masterIp string) error {
	deleteCsiPods := "kubectl delete pods -n vmware-system-csi --all"
	framework.Logf(deleteCsiPods)
	result, err := sshExec(sshClientConfig, masterIp, deleteCsiPods)
	if err != nil && result.Code != 0 {
		fssh.LogResult(result)
		return fmt.Errorf("couldn't execute command: %s on host: %v , error: %s",
			deleteCsiPods, masterIp, err)
	}
	time.Sleep(60 * time.Second)
	return nil
}

func getVcenterHostName(sshClientConfig *ssh.ClientConfig, masterIp string, vcenterIp string) (string, error) {
	getVcenterHostName := "nslookup " + vcenterIp + "| grep name |  awk '{print $4}' | tr -d '\n'"
	framework.Logf(getVcenterHostName)
	vCenterRes, err := sshExec(sshClientConfig, masterIp, getVcenterHostName)
	if err != nil && vCenterRes.Code != 0 {
		fssh.LogResult(vCenterRes)
		return "", fmt.Errorf("couldn't execute command: %s on host: %v , error: %s",
			getVcenterHostName, masterIp, err)
	}
	return vCenterRes.Stdout, nil

}

func getConfigSecretFileValues(client clientset.Interface, ctx context.Context) (bool, string, string, string, string) {
	currentSecret, err := client.CoreV1().Secrets(csiSystemNamespace).Get(ctx, configSecret, metav1.GetOptions{})
	gomega.Expect(err).NotTo(gomega.HaveOccurred())
	originalConf := string(currentSecret.Data[vSphereCSIConf])
	vsphereCfg, err := readConfigFromSecretString(originalConf)
	gomega.Expect(err).NotTo(gomega.HaveOccurred())
	insecureFlag = vsphereCfg.Global.InsecureFlag
	clusterId = vsphereCfg.Global.ClusterID
	clusterDistribution = vsphereCfg.Global.ClusterDistribution
	dataCenters = vsphereCfg.Global.Datacenters
	vCenterPort = vsphereCfg.Global.VCenterPort
	user = vsphereCfg.Global.User
	password = vsphereCfg.Global.Password
	vCenterIp = vsphereCfg.Global.VCenterHostname
	framework.Logf("cluster-id: %s"+"  "+"cluster-distribution: %s"+"  "+
		"VirtualCenter: %s"+"  "+"insecure-flag: %t"+"  "+
		"user: %s"+"  "+"password: %s"+"  "+"vCenterPort: %s"+"  "+"dataCenters: %s", clusterId,
		clusterDistribution, vCenterIp, insecureFlag, user, password, vCenterPort, dataCenters)
	return insecureFlag, clusterId, clusterDistribution, dataCenters, vCenterPort
}
