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
	"strings"

	ginkgo "github.com/onsi/ginkgo/v2"
	"github.com/onsi/gomega"
	"golang.org/x/crypto/ssh"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	clientset "k8s.io/client-go/kubernetes"
	"k8s.io/kubernetes/test/e2e/framework"
	fssh "k8s.io/kubernetes/test/e2e/framework/ssh"
)

// govc login command
func govcLoginCmd1(vcAddress string, portNo string) string {
	loginCmd := "export GOVC_INSECURE=1;"
	loginCmd += fmt.Sprintf("export GOVC_URL='https://%s:%s@%s:%s';", vCenterUIUser, vCenterUIPassword, vcAddress, portNo)
	return loginCmd
}

// createTestUser util method is used for creating test users
func createTestUser(vcAddress string, vCenterPort string, sshClientConfig *ssh.ClientConfig, masterIp string,
	testUser string, testUserPassword string) error {
	// create test user
	createUser := govcLoginCmd1(vcAddress, vCenterPort) + "govc sso.user.create -p " + testUserPassword + " " + testUser
	framework.Logf("Create testuser: %s ", createUser)
	result, err := sshExec(sshClientConfig, masterIp, createUser)
	if err != nil && result.Code != 0 {
		fssh.LogResult(result)
		return fmt.Errorf("couldn't execute command: %s on host: %v , error: %s",
			createUser, masterIp, err)
	}
	return nil
}

// changeTestUserPassword util method is used for changing test user password
func changeTestUserPassword(vcAddress string, vCenterPort string, sshClientConfig *ssh.ClientConfig, masterIp string,
	testUser string, testUserPassword string) error {
	// change test user password
	changeUserPassword := govcLoginCmd1(vcAddress, vCenterPort) + "govc sso.user.update -p " + testUserPassword + " " + testUser
	framework.Logf("Password change for testuser: %s ", changeUserPassword)
	result, err := sshExec(sshClientConfig, masterIp, changeUserPassword)
	if err != nil && result.Code != 0 {
		fssh.LogResult(result)
		return fmt.Errorf("couldn't execute command: %s on host: %v , error: %s",
			changeUserPassword, masterIp, err)
	}
	return nil
}

// deleteUsersRolesAndPermissions method is used to delete roles and permissions of a test users
func deleteUsersRolesAndPermissions(vcAddress string, vCenterPort string, sshClientConfig *ssh.ClientConfig,
	masterIp string, testUser string, testUserAlias string, dataCenter []string, cluster []string, hosts []string,
	vms []string, datastores []string) error {
	ginkgo.By("Delete user permissions")
	err := deleteUserPermissions(vcAddress, vCenterPort, sshClientConfig, masterIp, testUserAlias, dataCenter,
		cluster, hosts, vms, datastores)
	if err != nil {
		//return err
		framework.Logf("User Permissions not found")
	}

	ginkgo.By("Delete user roles")
	err = deleteUserRoles(vcAddress, vCenterPort, sshClientConfig, masterIp, testUser)
	if err != nil {
		//return err
		framework.Logf("User Roles not found")
	}
	return nil
}

// deleteUserPermissions method is used to delete permissions of a test user
func deleteUserPermissions(vcAddress string, vCenterPort string, sshClientConfig *ssh.ClientConfig,
	masterIp string, testUser string, dataCenter []string, clusters []string, hosts []string,
	vms []string, datastores []string) error {
	err := deleteDataCenterPermissions(vcAddress, vCenterPort, sshClientConfig, masterIp, testUser, dataCenter)
	if err != nil {
		return err
	}
	err = deleteHostsLevelPermission(vcAddress, vCenterPort, sshClientConfig, masterIp, testUser, hosts)
	if err != nil {
		return err
	}
	err = deleteVMsLevelPermission(vcAddress, vCenterPort, sshClientConfig, masterIp, testUser, vms)
	if err != nil {
		return err
	}
	err = deleteClusterLevelPermission(vcAddress, vCenterPort, sshClientConfig, masterIp, testUser, clusters)
	if err != nil {
		return err
	}
	for i := 0; i < len(dataCenter); i++ {
		err = deleteDataStoreLevelPermission(vcAddress, vCenterPort, sshClientConfig, masterIp, testUser, dataCenter[i],
			datastores)
		if err != nil {
			return err
		}
	}
	err = deleteOtherPermissionsFromTestUser(vcAddress, vCenterPort, sshClientConfig, masterIp, testUser)
	if err != nil {
		return err
	}
	return nil
}

// deleteDataCenterPermissions method is used to delete DataCenter Permissions from a test user
func deleteDataCenterPermissions(vcAddress string, vCenterPort string, sshClientConfig *ssh.ClientConfig,
	masterIp string, testUser string, dataCenter []string) error {
	for i := 0; i < len(dataCenter); i++ {
		deleteDataCenterPermissions := govcLoginCmd1(vcAddress, vCenterPort) + "govc permissions.remove -principal " + testUser +
			" " + dataCenter[i]
		framework.Logf("Delete datacenter level permissions: %s ", deleteDataCenterPermissions)
		result, err := sshExec(sshClientConfig, masterIp, deleteDataCenterPermissions)
		if err != nil && result.Code != 0 {
			fssh.LogResult(result)
			return fmt.Errorf("couldn't execute command: %s on host: %v , error: %s",
				deleteDataCenterPermissions, masterIp, err)
		}
	}
	return nil
}

// deleteHostsLevelPermission method is used to delete hosts level permissions from a test user
func deleteHostsLevelPermission(vcAddress string, vCenterPort string, sshClientConfig *ssh.ClientConfig,
	masterIp string, testUser string, hosts []string) error {
	for i := 0; i < len(hosts); i++ {
		deleteHostsLevelPermissions := govcLoginCmd1(vcAddress, vCenterPort) + "govc permissions.remove -principal " + testUser +
			" " + hosts[i]
		framework.Logf("Delete host level permissions from test user: %s ", deleteHostsLevelPermissions)
		result, err := sshExec(sshClientConfig, masterIp, deleteHostsLevelPermissions)
		if err != nil && result.Code != 0 {
			fssh.LogResult(result)
			return fmt.Errorf("couldn't execute command: %s on host: %v , error: %s",
				deleteHostsLevelPermissions, masterIp, err)
		}
	}
	return nil
}

// deleteVMsLevelPermission method is used to delete vm level permissions from a test user
func deleteVMsLevelPermission(vcAddress string, vCenterPort string, sshClientConfig *ssh.ClientConfig,
	masterIp string, testUser string, vms []string) error {
	for i := 0; i < len(vms); i++ {
		deleteVmsLevelPermissions := govcLoginCmd1(vcAddress, vCenterPort) + "govc permissions.remove -principal " + testUser +
			" " + vms[i]
		framework.Logf("Delete vm level permissions from test user: %s ", deleteVmsLevelPermissions)
		result, err := sshExec(sshClientConfig, masterIp, deleteVmsLevelPermissions)
		if err != nil && result.Code != 0 {
			fssh.LogResult(result)
			return fmt.Errorf("couldn't execute command: %s on host: %v , error: %s",
				deleteVmsLevelPermissions, masterIp, err)
		}
	}
	return nil
}

// deleteClusterLevelPermission method is used to delete cluster level permissions from a test user
func deleteClusterLevelPermission(vcAddress string, vCenterPort string, sshClientConfig *ssh.ClientConfig,
	masterIp string, testUser string, clusters []string) error {
	for i := 0; i < len(clusters); i++ {
		deleteClusterLevelPermissions := govcLoginCmd1(vcAddress, vCenterPort) + "govc permissions.remove -principal " + testUser +
			" " + clusters[i]
		framework.Logf("Delete cluster level permissions from test user: %s ", deleteClusterLevelPermissions)
		result, err := sshExec(sshClientConfig, masterIp, deleteClusterLevelPermissions)
		if err != nil && result.Code != 0 {
			fssh.LogResult(result)
			return fmt.Errorf("couldn't execute command: %s on host: %v , error: %s",
				deleteClusterLevelPermissions, masterIp, err)
		}
	}
	return nil
}

// deleteDataStoreLevelPermission method is used to delete datastore level permissions from a test user
func deleteDataStoreLevelPermission(vcAddress string, vCenterPort string, sshClientConfig *ssh.ClientConfig,
	masterIp string, testUser string, dataCenter string, datastores []string) error {
	for i := 0; i < len(datastores); i++ {
		deleteDataStoreLevelPermissions := govcLoginCmd1(vcAddress, vCenterPort) + "govc permissions.remove -principal " +
			testUser + " '" + datastores[i] + "'"
		framework.Logf("Delete datastore level permissions from test user: %s ", deleteDataStoreLevelPermissions)
		result, err := sshExec(sshClientConfig, masterIp, deleteDataStoreLevelPermissions)
		if err != nil && result.Code != 0 {
			fssh.LogResult(result)
			return fmt.Errorf("couldn't execute command: %s on host: %v , error: %s",
				deleteDataStoreLevelPermissions, masterIp, err)
		}
	}
	return nil
}

// deleteOtherPermissionsFromTestUser method is used to remove permissions from a test user
func deleteOtherPermissionsFromTestUser(vcAddress string, vCenterPort string, sshClientConfig *ssh.ClientConfig,
	masterIp string, testUser string) error {
	deleteOtherPermissions := govcLoginCmd1(vcAddress, vCenterPort) + "govc permissions.remove -principal " +
		testUser
	framework.Logf("Delete user permissions: %s ", deleteOtherPermissions)
	result, err := sshExec(sshClientConfig, masterIp, deleteOtherPermissions)
	if err != nil && result.Code != 0 {
		fssh.LogResult(result)
		return fmt.Errorf("couldn't execute command: %s on host: %v , error: %s",
			deleteOtherPermissions, masterIp, err)
	}
	return nil
}

// deleteUserRoles method is used to delete roles of a test user
func deleteUserRoles(vcAddress string, vCenterPort string, sshClientConfig *ssh.ClientConfig,
	masterIp string, testUser string) error {
	deleteRoles := govcLoginCmd1(vcAddress, vCenterPort) + "govc role.remove CNS-DATASTORE-" + testUser + ";" +
		"govc role.remove CNS-HOST-CONFIG-STORAGE-" + testUser + ";" +
		"govc role.remove CNS-VM-" + testUser + ";" +
		"govc role.remove CNS-SEARCH-AND-SPBM-" + testUser
	framework.Logf("Delete user roles: %s ", deleteRoles)
	result, err := sshExec(sshClientConfig, masterIp, deleteRoles)
	if err != nil && result.Code != 0 {
		fssh.LogResult(result)
		return fmt.Errorf("couldn't execute command: %s on host: %v , error: %s",
			deleteRoles, masterIp, err)
	}
	return nil

}

// deleteTestUser method is used to delete config secret test users
func deleteTestUser(vcAddress string, vCenterPort string, sshClientConfig *ssh.ClientConfig,
	masterIp string, testUser string) error {
	deleteUser := govcLoginCmd1(vcAddress, vCenterPort) + "govc sso.user.rm " + testUser
	framework.Logf("Delete User: %s ", deleteUser)
	result, err := sshExec(sshClientConfig, masterIp, deleteUser)
	if err != nil && result.Code != 0 {
		fssh.LogResult(result)
		return fmt.Errorf("couldn't execute command: %s on host: %v , error: %s",
			deleteUser, masterIp, err)
	}
	return nil

}

// createRolesForTestUser method is used to create roles for a test user
func createRolesForTestUser(vcAddress string, vCenterPort string, sshClientConfig *ssh.ClientConfig,
	masterIp string, testUser string) error {
	createRoleCmdFortestUser := govcLoginCmd1(vcAddress, vCenterPort) + "govc role.create CNS-DATASTORE-" + testUser +
		" Datastore.FileManagement;" +
		"govc role.create CNS-HOST-CONFIG-STORAGE-" + testUser + " Host.Config.Storage;" +
		"govc role.create CNS-VM-" + testUser +
		" VirtualMachine.Config.AddExistingDisk VirtualMachine.Config.AddRemoveDevice;" +
		"govc role.create CNS-SEARCH-AND-SPBM-" + testUser + " Cns.Searchable StorageProfile.View"
	framework.Logf("Create roles for user: %s ", createRoleCmdFortestUser)
	result, err := sshExec(sshClientConfig, masterIp, createRoleCmdFortestUser)
	if err != nil && result.Code != 0 {
		fssh.LogResult(result)
		return fmt.Errorf("couldn't execute command: %s on host: %v , error: %s",
			createRoleCmdFortestUser, masterIp, err)
	}
	return nil
}

/*
	getDataCenterClusterHostAndVmDetails method is used to fetch data center details, cluster

details, host details and vm details
*/
func getDataCenterClusterHostAndVmDetails(vcAddress string, vCenterPort string, sshClientConfig *ssh.ClientConfig,
	masterIp string) ([]string, []string, []string, []string, []string, error) {
	// fetch datacenter details
	dataCenters, err := getDataCenterDetails(vcAddress, vCenterPort, sshClientConfig, masterIp)
	gomega.Expect(err).NotTo(gomega.HaveOccurred())

	// fetch cluster details
	clusters, err := getClusterDetails(vcAddress, vCenterPort, sshClientConfig, masterIp, dataCenters)
	gomega.Expect(err).NotTo(gomega.HaveOccurred())

	// fetch esxi hosts details
	hosts, err := getEsxiHostDetails(vcAddress, vCenterPort, sshClientConfig, masterIp, clusters)
	gomega.Expect(err).NotTo(gomega.HaveOccurred())

	// fetch vm details
	vms, err := getVmDetails(vcAddress, vCenterPort, sshClientConfig, masterIp, dataCenters)
	gomega.Expect(err).NotTo(gomega.HaveOccurred())

	// fetch datastore details
	datastores, err := getDatastoreDetails(vcAddress, vCenterPort, sshClientConfig, masterIp, dataCenters)
	gomega.Expect(err).NotTo(gomega.HaveOccurred())

	return dataCenters, clusters, hosts, vms, datastores, nil

}

// getDataCenterDetails method is used to fetch data center details
func getDataCenterDetails(vcAddress string, vCenterPort string, sshClientConfig *ssh.ClientConfig,
	masterIp string) ([]string, error) {
	var dataCenterList []string
	// fetch datacenter details
	ginkgo.By("Executing command for fetching data center details")
	dataCenter := govcLoginCmd1(vcAddress, vCenterPort) + "govc datacenter.info | grep -i Name | awk '{print $2}'"
	framework.Logf("Fetch dataCenter: %s ", dataCenter)
	dcResult, err := sshExec(sshClientConfig, masterIp, dataCenter)
	if err != nil && dcResult.Code != 0 {
		fssh.LogResult(dcResult)
		return nil, fmt.Errorf("couldn't execute command: %s on host: %v , error: %s",
			dataCenter, masterIp, err)
	}
	dcList := strings.Split(dcResult.Stdout, "\n")
	for i := 0; i < len(dcList); i++ {
		if dcList[i] != "" {
			dataCenterList = append(dataCenterList, dcList[i])
		}
	}
	return dataCenterList, nil
}

// getDataCenterDetails method is used to fetch cluster details
func getClusterDetails(vcAddress string, vCenterPort string, sshClientConfig *ssh.ClientConfig,
	masterIp string, dataCenter []string) ([]string, error) {
	var clusterList []string
	var clusList []string
	// fetch cluster details
	ginkgo.By("Executing command for fetching cluster details")
	for i := 0; i < len(dataCenter); i++ {
		cluster := govcLoginCmd1(vcAddress, vCenterPort) + "govc ls /" + dataCenter[i] + "/host"
		framework.Logf("Fetch Cluster: %s ", cluster)
		clusterResult, err := sshExec(sshClientConfig, masterIp, cluster)
		if err != nil && clusterResult.Code != 0 {
			fssh.LogResult(clusterResult)
			return nil, fmt.Errorf("couldn't execute command: %s on host: %v , error: %s",
				cluster, masterIp, err)
		}
		if clusterResult.Stdout != "" {
			clusListTemp := strings.Split(clusterResult.Stdout, "\n")
			clusList = append(clusList, clusListTemp...)
		}
		for i := 0; i < len(clusList); i++ {
			if clusList[i] != "" {
				clusterList = append(clusterList, clusList[i])
			}
		}
		clusList = nil
	}
	return clusterList, nil

}

// getEsxiHostDetails method is used to fetch esxi hosts details
func getEsxiHostDetails(vcAddress string, vCenterPort string, sshClientConfig *ssh.ClientConfig,
	masterIp string, cluster []string) ([]string, error) {
	var hostsList []string
	var hostList []string
	// Get all esxi hosts details
	ginkgo.By("Executing command for fetching esxi host details")
	for i := 0; i < len(cluster); i++ {
		hosts := govcLoginCmd1(vcAddress, vCenterPort) + "govc ls " + cluster[i] + " " + " | grep 10."
		framework.Logf("Fetch host details: %s ", hosts)
		hostsResult, err := sshExec(sshClientConfig, masterIp, hosts)
		if err != nil && hostsResult.Code != 0 {
			fssh.LogResult(hostsResult)
			return nil, fmt.Errorf("couldn't execute command: %s on host: %v , error: %s",
				hosts, masterIp, err)
		}
		if hostsResult.Stdout != "" {
			hostListTemp := strings.Split(hostsResult.Stdout, "\n")
			hostList = append(hostList, hostListTemp...)
		}
		for i := 0; i < len(hostList); i++ {
			if hostList[i] != "" {
				hostsList = append(hostsList, hostList[i])
			}
		}
		hostList = nil
	}
	return hostsList, nil
}

// getVmDetails method is used to fetch vm details
func getVmDetails(vcAddress string, vCenterPort string, sshClientConfig *ssh.ClientConfig,
	masterIp string, dataCenter []string) ([]string, error) {
	var vmsList []string
	var vmList []string
	// get vm details
	ginkgo.By("Executing command for fetching k8s vm details")
	for i := 0; i < len(dataCenter); i++ {
		vms := govcLoginCmd1(vcAddress, vCenterPort) + "govc ls /" + dataCenter[i] + "/vm" + " " + "| grep 'k8s\\|haproxy'"
		framework.Logf("Fetch vm details: %s ", vms)
		vMsResult, err := sshExec(sshClientConfig, masterIp, vms)
		if err != nil && vMsResult.Code != 0 {
			fssh.LogResult(vMsResult)
			return nil, fmt.Errorf("couldn't execute command: %s on host: %v , error: %s",
				vms, masterIp, err)
		}
		if vMsResult.Stdout != "" {
			vmListTemp := strings.Split(vMsResult.Stdout, "\n")
			vmList = append(vmList, vmListTemp...)
		}
		for i := 0; i < len(vmList); i++ {
			if vmList[i] != "" {
				vmsList = append(vmsList, vmList[i])
			}
		}
		vmList = nil
	}
	return vmsList, nil
}

// getDatastoreDetails method is used to fetch datastore details
func getDatastoreDetails(vcAddress string, vCenterPort string, sshClientConfig *ssh.ClientConfig,
	masterIp string, dataCenter []string) ([]string, error) {
	var dsList []string
	var datastores []string
	// get vm details
	ginkgo.By("Executing command for fetching datastore details")
	for i := 0; i < len(dataCenter); i++ {
		ds := govcLoginCmd1(vcAddress, vCenterPort) + "govc ls /" + dataCenter[i] + "/datastore"
		framework.Logf("Fetch datastore details: %s ", ds)
		dsResult, err := sshExec(sshClientConfig, masterIp, ds)
		if err != nil && dsResult.Code != 0 {
			fssh.LogResult(dsResult)
			return nil, fmt.Errorf("couldn't execute command: %s on host: %v , error: %s",
				ds, masterIp, err)
		}
		if dsResult.Stdout != "" {
			dsListTemp := strings.Split(dsResult.Stdout, "\n")
			dsList = append(dsList, dsListTemp...)
		}
		for i := 0; i < len(dsList); i++ {
			if dsList[i] != "" {
				datastores = append(datastores, dsList[i])
			}
		}
		dsList = nil
	}
	return datastores, nil
}

// setDataCenterLevelPermission is used to set data center level permissions for test user
func setDataCenterLevelPermission(vcAddress string, vCenterPort string, sshClientConfig *ssh.ClientConfig,
	masterIp string, dataCenter string, testUser string) error {
	setPermissionForDataCenter := govcLoginCmd1(vcAddress, vCenterPort) + "govc permissions.set -principal " + testUser +
		" -propagate=false -role ReadOnly " + dataCenter + " | tr -d '\n'"
	framework.Logf("Assign datacenter level Permissions to user: %s ", setPermissionForDataCenter)
	result, err := sshExec(sshClientConfig, masterIp, setPermissionForDataCenter)
	if err != nil && result.Code != 0 {
		fssh.LogResult(result)
		return fmt.Errorf("couldn't execute command: %s on host: %v , error: %s",
			setPermissionForDataCenter, masterIp, err)
	}
	return nil
}

// setHostLevelPermission is used to set host level permissions for test user
func setHostLevelPermission(vcAddress string, vCenterPort string, sshClientConfig *ssh.ClientConfig,
	masterIp string, testUser string, hosts []string) error {
	for i := 0; i < len(hosts); i++ {
		setPermissionForHosts := govcLoginCmd1(vcAddress, vCenterPort) + "govc permissions.set -principal " +
			testUser + " -propagate=false -role ReadOnly " + hosts[i] + "| tr -d '\n'"
		framework.Logf("Assign host level Permissions to user: %s ", setPermissionForHosts)
		result, err := sshExec(sshClientConfig, masterIp, setPermissionForHosts)
		if err != nil && result.Code != 0 {
			fssh.LogResult(result)
			return fmt.Errorf("couldn't execute command: %s on host: %v , error: %s",
				setPermissionForHosts, masterIp, err)
		}
	}
	return nil
}

// setVMLevelPermission is used to set vm level permissions for test user
func setVMLevelPermission(vcAddress string, vCenterPort string, sshClientConfig *ssh.ClientConfig,
	masterIp string, testUserAlias string, testUser string, vms []string) error {
	for i := 0; i < len(vms); i++ {
		setPermissionForK8sVms := govcLoginCmd1(vcAddress, vCenterPort) + "govc permissions.set -principal " +
			testUserAlias + " -propagate=false -role CNS-VM-" + testUser +
			" " + vms[i] + " | tr -d '\n'"
		framework.Logf("Assign vm level Permissions to user: %s ", setPermissionForK8sVms)
		result, err := sshExec(sshClientConfig, masterIp, setPermissionForK8sVms)
		if err != nil && result.Code != 0 {
			fssh.LogResult(result)
			return fmt.Errorf("couldn't execute command: %s on host: %v , error: %s",
				setPermissionForK8sVms, masterIp, err)
		}
	}
	return nil
}

// setClusterLevelPermission is used to set cluster level permissions for test user
func setClusterLevelPermission(vcAddress string, vCenterPort string, sshClientConfig *ssh.ClientConfig,
	masterIp string, testUserAlias string, testUser string, cluster string) error {
	setPermissionForCluster := govcLoginCmd1(vcAddress, vCenterPort) + "govc permissions.set -principal " +
		testUserAlias + " -propagate=false -role CNS-HOST-CONFIG-STORAGE-" + testUser + " " + cluster + " | tr -d '\n'"
	framework.Logf("Assign cluster level Permissions to user: %s ", setPermissionForCluster)
	result, err := sshExec(sshClientConfig, masterIp, setPermissionForCluster)
	if err != nil && result.Code != 0 {
		fssh.LogResult(result)
		return fmt.Errorf("couldn't execute command: %s on host: %v , error: %s",
			setPermissionForCluster, masterIp, err)
	}
	return nil
}

// setDataStoreLevelPermission is used to set datastore level permissions for test user
func setDataStoreLevelPermission(vcAddress string, vCenterPort string, sshClientConfig *ssh.ClientConfig,
	masterIp string, testUserAlias string, testUser string, dataCenter string, datastores []string) error {
	for i := 0; i < len(datastores); i++ {
		setPermissionForDataStore := govcLoginCmd1(vcAddress, vCenterPort) + "govc permissions.set -principal " +
			testUserAlias + " " +
			"-propagate=false -role CNS-DATASTORE-" + testUser + " '" + datastores[i] + "'"
		framework.Logf("Assign datastore level Permissions to user: %s ", setPermissionForDataStore)
		result, err := sshExec(sshClientConfig, masterIp, setPermissionForDataStore)
		if err != nil && result.Code != 0 {
			fssh.LogResult(result)
			return fmt.Errorf("couldn't execute command: %s on host: %v , error: %s",
				setPermissionForDataStore, masterIp, err)
		}
	}
	return nil
}

// setSearchlevelPermission method is used to set search level permissions
func setSearchlevelPermission(vcAddress string, vCenterPort string, sshClientConfig *ssh.ClientConfig,
	masterIp string, testUserAlias string, testUser string) error {
	setSearchLevelPermission := govcLoginCmd1(vcAddress, vCenterPort) + "govc permissions.set -principal " + testUserAlias +
		" -propagate=false -role CNS-SEARCH-AND-SPBM-" + testUser + " /"
	framework.Logf("Assign search level Permissions to user: %s ", setSearchLevelPermission)
	result, err := sshExec(sshClientConfig, masterIp, setSearchLevelPermission)
	if err != nil && result.Code != 0 {
		fssh.LogResult(result)
		return fmt.Errorf("couldn't execute command: %s on host: %v , error: %s",
			setSearchLevelPermission, masterIp, err)
	}
	return nil
}

// createCsiVsphereSecret method is used to create csi vsphere secret file
func createCsiVsphereSecret(clusterName string, clusterDistribution string, vCenterIP string,
	inSecureFlag bool, testUser string, password string, vCenterPort string,
	dataCenter string, sshClientConfig *ssh.ClientConfig,
	masterIp string) error {
	conf := `tee csi-vsphere.conf >/dev/null <<EOF
[Global]
cluster-id = "%s"
cluster-distribution = "%s"

[VirtualCenter "%s"]
insecure-flag = "%t"
user = "%s"
password = "%s"
port = "%s"
datacenters = "%s"
EOF

`
	// currentSecret, err := client.CoreV1().Secrets(csiSystemNamespace).Get(ctx, conf, metav1.GetOptions{})
	// if err != nil {
	// 	return err
	// }
	// secret, err := client.CoreV1().Secrets(csiSystemNamespace).Create(ctx, currentSecret, metav1.CreateOptions{})
	// if err != nil {
	// 	return err
	// }
	// fmt.Println(secret)
	createConf := fmt.Sprintf(conf, clusterName, clusterDistribution, vCenterIP, inSecureFlag, testUser, password,
		vCenterPort, dataCenter)
	framework.Logf("vsphere config secret file: %s ", createConf)
	result, err := sshExec(sshClientConfig, masterIp, createConf)
	if err != nil && result.Code != 0 {
		fssh.LogResult(result)
		return fmt.Errorf("couldn't execute command: %s on host: %v , error: %s",
			conf, masterIp, err)
	}
	applyConf := "kubectl create secret generic vsphere-config-secret --from-file=csi-vsphere.conf " +
		"-n vmware-system-csi"
	framework.Logf("Create vsphere config secret: %s ", applyConf)
	result, err = sshExec(sshClientConfig, masterIp, applyConf)
	if err != nil && result.Code != 0 {
		fssh.LogResult(result)
		return fmt.Errorf("couldn't execute command: %s on host: %v , error: %s",
			applyConf, masterIp, err)
	}
	return nil
}

// createCsiVsphereSecret method is used to create csi vsphere secret file
func createCsiVsphereSecretForFileVanilla(clusterName string, clusterDistribution string, vCenterIP string,
	inSecureFlag bool, testUser string, password string, vCenterPort string,
	dataCenter string, sshClientConfig *ssh.ClientConfig,
	masterIp string, targetUrl string) error {
	conf := `tee csi-vsphere.conf >/dev/null <<EOF
[Global]
cluster-id = "%s"
cluster-distribution = "%s"

[VirtualCenter "%s"]
insecure-flag = "%t"
user = "%s"
password = "%s"
port = "%s"
datacenters = "%s"
targetvSANFileShareDatastoreURLs = "%s"

EOF

`
	// currentSecret, err := client.CoreV1().Secrets(csiSystemNamespace).Get(ctx, conf, metav1.GetOptions{})
	// if err != nil {
	// 	return err
	// }
	// secret, err := client.CoreV1().Secrets(csiSystemNamespace).Create(ctx, currentSecret, metav1.CreateOptions{})
	// if err != nil {
	// 	return err
	// }
	// fmt.Println(secret)
	createConf := fmt.Sprintf(conf, clusterName, clusterDistribution, vCenterIP, inSecureFlag, testUser, password,
		vCenterPort, dataCenter, targetUrl)
	framework.Logf("vsphere config secret file: %s ", createConf)
	result, err := sshExec(sshClientConfig, masterIp, createConf)
	if err != nil && result.Code != 0 {
		fssh.LogResult(result)
		return fmt.Errorf("couldn't execute command: %s on host: %v , error: %s",
			conf, masterIp, err)
	}
	applyConf := "kubectl create secret generic vsphere-config-secret --from-file=csi-vsphere.conf " +
		"-n vmware-system-csi"
	framework.Logf("Create vsphere config secret: %s ", applyConf)
	result, err = sshExec(sshClientConfig, masterIp, applyConf)
	if err != nil && result.Code != 0 {
		fssh.LogResult(result)
		return fmt.Errorf("couldn't execute command: %s on host: %v , error: %s",
			applyConf, masterIp, err)
	}
	return nil
}

// deleteCsiVsphereSecret method is used to delete csi vsphere config secret file
func deleteCsiVsphereSecret(sshClientConfig *ssh.ClientConfig, masterIp string) error {
	// currentSecret, err := client.CoreV1().Secrets(csiSystemNamespace).Get(ctx, configSecret,
	// 	metav1.GetOptions{})
	// if err != nil {
	// 	return err
	// }
	// err = client.CoreV1().Secrets(csiSystemNamespace).Delete(ctx, currentSecret.Name, *metav1.NewDeleteOptions(0))
	// if err != nil {
	// 	return err
	// }

	deleteConf := "kubectl delete secret vsphere-config-secret --namespace=vmware-system-csi"
	framework.Logf("Delete vsphere config secret: %s ", deleteConf)
	result, err := sshExec(sshClientConfig, masterIp, deleteConf)
	if err != nil && result.Code != 0 {
		fssh.LogResult(result)
		return fmt.Errorf("couldn't execute command: %s on host: %v , error: %s",
			deleteConf, masterIp, err)
	}
	return nil
}

// getConfigSecretFileValues method is used to fetch config secret file values
func getConfigSecretFileValues(client clientset.Interface, ctx context.Context) e2eTestConfig {
	var testConfig e2eTestConfig
	currentSecret, err := client.CoreV1().Secrets(csiSystemNamespace).Get(ctx, configSecret, metav1.GetOptions{})
	gomega.Expect(err).NotTo(gomega.HaveOccurred())
	originalConf := string(currentSecret.Data[vSphereCSIConf])
	vsphereCfg, err := readConfigFromSecretString(originalConf)
	gomega.Expect(err).NotTo(gomega.HaveOccurred())
	testConfig.Global.InsecureFlag = vsphereCfg.Global.InsecureFlag
	testConfig.Global.ClusterID = vsphereCfg.Global.ClusterID
	testConfig.Global.ClusterDistribution = vsphereCfg.Global.ClusterDistribution
	testConfig.Global.Datacenters = vsphereCfg.Global.Datacenters
	testConfig.Global.VCenterPort = vsphereCfg.Global.VCenterPort
	testConfig.Global.User = vsphereCfg.Global.User
	testConfig.Global.Password = vsphereCfg.Global.Password
	testConfig.Global.VCenterHostname = vsphereCfg.Global.VCenterHostname
	config := `
InsecureFlag: "%t"
ClusterID: "%s"
ClusterDistribution: "%s"
Datacenters: "%s"
VCenterPort: "%s"
User: "%s"
Password: "%s"
VCenterHostname: "%s"`

	secretConfig := fmt.Sprintf(config, vsphereCfg.Global.InsecureFlag,
		vsphereCfg.Global.ClusterID, vsphereCfg.Global.ClusterDistribution,
		vsphereCfg.Global.Datacenters, vsphereCfg.Global.VCenterPort, vsphereCfg.Global.User,
		vsphereCfg.Global.Password, vsphereCfg.Global.VCenterHostname)

	framework.Logf("vsphere config secret file: %s ", secretConfig)
	return testConfig
}

func getConfigSecretFileValuesForFileVanilla(client clientset.Interface, ctx context.Context) e2eTestConfig {
	var testConfig e2eTestConfig
	var secretConfig string
	currentSecret, err := client.CoreV1().Secrets(csiSystemNamespace).Get(ctx, configSecret, metav1.GetOptions{})
	gomega.Expect(err).NotTo(gomega.HaveOccurred())
	originalConf := string(currentSecret.Data[vSphereCSIConf])
	vsphereCfg, err := readConfigFromSecretString(originalConf)
	gomega.Expect(err).NotTo(gomega.HaveOccurred())
	testConfig.Global.InsecureFlag = vsphereCfg.Global.InsecureFlag
	testConfig.Global.ClusterID = vsphereCfg.Global.ClusterID
	testConfig.Global.ClusterDistribution = vsphereCfg.Global.ClusterDistribution
	testConfig.Global.Datacenters = vsphereCfg.Global.Datacenters
	testConfig.Global.VCenterPort = vsphereCfg.Global.VCenterPort
	testConfig.Global.User = vsphereCfg.Global.User
	testConfig.Global.Password = vsphereCfg.Global.Password
	testConfig.Global.VCenterHostname = vsphereCfg.Global.VCenterHostname
	testConfig.Global.TargetvSANFileShareDatastoreURLs = vsphereCfg.Global.TargetvSANFileShareDatastoreURLs
	config := `
InsecureFlag: "%t"
ClusterID: "%s"
ClusterDistribution: "%s"
Datacenters: "%s"	
VCenterPort: "%s"
User: "%s"
Password: "%s"
VCenterHostname: "%s"
TargetvSANFileShareDatastoreURLs: "%s"
	
`
	secretConfig = fmt.Sprintf(config, vsphereCfg.Global.InsecureFlag,
		vsphereCfg.Global.ClusterID, vsphereCfg.Global.ClusterDistribution,
		vsphereCfg.Global.Datacenters, vsphereCfg.Global.VCenterPort, vsphereCfg.Global.User,
		vsphereCfg.Global.Password, vsphereCfg.Global.VCenterHostname,
		vsphereCfg.Global.TargetvSANFileShareDatastoreURLs)

	framework.Logf("vsphere config secret file: %s ", secretConfig)
	return testConfig
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
