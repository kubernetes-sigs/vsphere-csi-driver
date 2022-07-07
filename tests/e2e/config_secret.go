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

	ginkgo "github.com/onsi/ginkgo/v2"
	"github.com/onsi/gomega"
	"golang.org/x/crypto/ssh"
	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	clientset "k8s.io/client-go/kubernetes"
	"k8s.io/kubernetes/test/e2e/framework"
	fnodes "k8s.io/kubernetes/test/e2e/framework/node"
	fpod "k8s.io/kubernetes/test/e2e/framework/pod"
	fpv "k8s.io/kubernetes/test/e2e/framework/pv"
	admissionapi "k8s.io/pod-security-admission/api"
)

var _ = ginkgo.Describe("Config-Secret", func() {
	f := framework.NewDefaultFramework("config-secret-volume-provisioning")
	f.NamespacePodSecurityEnforceLevel = admissionapi.LevelPrivileged
	var (
		client                 clientset.Interface
		namespace              string
		sshClientConfig        *ssh.ClientConfig
		vcAddress              string
		allMasterIps           []string
		masterIp               string
		dataCenters            []string
		clusters               []string
		hosts                  []string
		vms                    []string
		datastores             []string
		err                    error
		secretConf             e2eTestConfig
		configSecretUser1Alias string
		configSecretUser2Alias string
		csiNamespace           string
		csiReplicas            int32
		vCenterPort            string
	)

	ginkgo.BeforeEach(func() {
		var cancel context.CancelFunc
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		client = f.ClientSet
		namespace = f.Namespace.Name
		bootstrap()
		nodeList, err := fnodes.GetReadySchedulableNodes(f.ClientSet)
		framework.ExpectNoError(err, "Unable to find ready and schedulable Node")
		if !(len(nodeList.Items) > 0) {
			framework.Failf("Unable to find ready and schedulable Node")
		}

		allMasterIps = getK8sMasterIPs(ctx, client)
		masterIp = allMasterIps[0]
		sshClientConfig = &ssh.ClientConfig{
			User: "root",
			Auth: []ssh.AuthMethod{
				ssh.Password(k8sVmPasswd),
			},
			HostKeyCallback: ssh.InsecureIgnoreHostKey(),
		}
		configSecretUser1Alias = configSecretTestUser1 + "@vsphere.local"
		configSecretUser2Alias = configSecretTestUser2 + "@vsphere.local"
		vcAddress = e2eVSphere.Config.Global.VCenterHostname
		vCenterPort = e2eVSphere.Config.Global.VCenterPort

		ginkgo.By("Fetch DataCenter, Cluster and hosts details")
		dataCenters, clusters, hosts, vms, datastores, err = getDataCenterClusterHostAndVmDetails(vcAddress,
			vCenterPort, sshClientConfig, masterIp)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Delete testuser1")
		err = deleteTestUser(vcAddress, vCenterPort, sshClientConfig, masterIp, configSecretTestUser1)
		if err != nil {
			framework.Logf("couldn't execute command on host: %v , error: %s", masterIp, err)
		}
		ginkgo.By("Delete testuser2")
		err = deleteTestUser(vcAddress, vCenterPort, sshClientConfig, masterIp, configSecretTestUser2)
		if err != nil {
			framework.Logf("couldn't execute command on host: %v , error: %s", masterIp, err)
		}
		ginkgo.By("Delete Roles for testuser1")
		err = deleteUserRoles(vcAddress, vCenterPort, sshClientConfig, masterIp, configSecretTestUser1)
		if err != nil {
			framework.Logf("couldn't execute command on host: %v , error: %s", masterIp, err)
		}
		ginkgo.By("Delete Roles for testuser2")
		err = deleteUserRoles(vcAddress, vCenterPort, sshClientConfig, masterIp, configSecretTestUser2)
		if err != nil {
			framework.Logf("couldn't execute command on host: %v , error: %s", masterIp, err)
		}
		csiNamespace = GetAndExpectStringEnvVar(envCSINamespace)
		csiDeployment, err := client.AppsV1().Deployments(csiNamespace).Get(
			ctx, vSphereCSIControllerPodNamePrefix, metav1.GetOptions{})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		csiReplicas = *csiDeployment.Spec.Replicas
	})

	/*
		TESTCASE-1
		Change VC user
		Steps:
		1. Create two users (user1 and user2) with required roles and privileges and with same password
		2. Create csi-vsphere.conf with user1's credentials and create vsphere-config-secret in turn using that file
		3. Install CSI driver
		4. Verify we can create a PVC and attach it to pod
		5. Change csi-vsphere.conf with user2's credentials and re-create vsphere-config-secret in turn using that file
		6. Verify we can create a PVC and attach it to pod
		7. Cleanup all objects created during the test
	*/

	ginkgo.It("[csi-config-secret-block][csi-config-secret-file] Change vcenter users in vsphere config "+
		"secret file having same password", func() {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		ginkgo.By("Create testuser1")
		err = createTestUser(vcAddress, vCenterPort, sshClientConfig, masterIp, configSecretTestUser1,
			configSecretTestUser1Password)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		defer func() {
			ginkgo.By("Delete testuser1")
			err = deleteTestUser(vcAddress, vCenterPort, sshClientConfig, masterIp, configSecretTestUser1)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}()
		ginkgo.By("Create testuser2")
		err = createTestUser(vcAddress, vCenterPort, sshClientConfig, masterIp, configSecretTestUser2,
			configSecretTestUser1Password)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		defer func() {
			ginkgo.By("Delete testuser2")
			err = deleteTestUser(vcAddress, vCenterPort, sshClientConfig, masterIp, configSecretTestUser2)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}()

		ginkgo.By("Create roles for testuser1")
		err = createRolesForTestUser(vcAddress, vCenterPort, sshClientConfig, masterIp, configSecretTestUser1)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Create roles for testuser2")
		err = createRolesForTestUser(vcAddress, vCenterPort, sshClientConfig, masterIp, configSecretTestUser2)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Assign required roles and privileges for testuser1 and testuser2")

		ginkgo.By("Set DataCenter level permission for testuser1 and testuser2")
		for i := 0; i < len(dataCenters); i++ {
			err = setDataCenterLevelPermission(vcAddress, vCenterPort, sshClientConfig, masterIp, dataCenters[i],
				configSecretUser1Alias)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			err = setDataCenterLevelPermission(vcAddress, vCenterPort, sshClientConfig, masterIp, dataCenters[i],
				configSecretUser2Alias)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}

		ginkgo.By("Set hosts level permission for testuser1 and testuser2")
		err = setHostLevelPermission(vcAddress, vCenterPort, sshClientConfig, masterIp, configSecretUser1Alias, hosts)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		err = setHostLevelPermission(vcAddress, vCenterPort, sshClientConfig, masterIp, configSecretUser2Alias, hosts)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Set k8s VM level permission for testuser1 and testuser2")
		err = setVMLevelPermission(vcAddress, vCenterPort, sshClientConfig, masterIp, configSecretUser1Alias,
			configSecretTestUser1, vms)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		err = setVMLevelPermission(vcAddress, vCenterPort, sshClientConfig, masterIp, configSecretUser2Alias,
			configSecretTestUser2, vms)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Set cluster level permission for testuser1 and testuser2")
		for i := 0; i < len(clusters); i++ {
			err = setClusterLevelPermission(vcAddress, vCenterPort, sshClientConfig, masterIp,
				configSecretUser1Alias, configSecretTestUser1, clusters[i])
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			err = setClusterLevelPermission(vcAddress, vCenterPort, sshClientConfig, masterIp,
				configSecretUser2Alias, configSecretTestUser2, clusters[i])
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}

		ginkgo.By("Set Datastore level permission for testuser1 and testuser2")
		for i := 0; i < len(dataCenters); i++ {
			err = setDataStoreLevelPermission(vcAddress, vCenterPort, sshClientConfig, masterIp,
				configSecretUser1Alias, configSecretTestUser1, dataCenters[i], datastores)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			err = setDataStoreLevelPermission(vcAddress, vCenterPort, sshClientConfig, masterIp,
				configSecretUser2Alias, configSecretTestUser2, dataCenters[i], datastores)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}

		ginkgo.By("Set search level permission for testuser1 and testuser2")
		err = setSearchlevelPermission(vcAddress, vCenterPort, sshClientConfig, masterIp, configSecretUser1Alias,
			configSecretTestUser1)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		err = setSearchlevelPermission(vcAddress, vCenterPort, sshClientConfig, masterIp, configSecretUser2Alias,
			configSecretTestUser2)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		defer func() {
			ginkgo.By("Delete testUser1, its roles and permissions")
			err = deleteUsersRolesAndPermissions(vcAddress, vCenterPort, sshClientConfig, masterIp,
				configSecretTestUser1, configSecretUser1Alias, dataCenters, clusters, hosts, vms, datastores)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			ginkgo.By("Delete testUser2, its roles and permissions")
			err = deleteUsersRolesAndPermissions(vcAddress, vCenterPort, sshClientConfig, masterIp,
				configSecretTestUser2, configSecretUser2Alias, dataCenters, clusters, hosts, vms,
				datastores)
		}()

		ginkgo.By("Fetch vsphere-config-secret file details")
		secretConf = getConfigSecretFileValues(client, ctx)

		ginkgo.By("Delete previously created vsphere-config-secret file")
		err = deleteCsiVsphereSecret(sshClientConfig, masterIp)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Create vsphere-config-secret file with testuser1 credentials")
		err = createCsiVsphereSecret(secretConf.Global.ClusterID,
			secretConf.Global.ClusterDistribution, secretConf.Global.VCenterHostname,
			secretConf.Global.InsecureFlag,
			configSecretUser1Alias, configSecretTestUser1Password, secretConf.Global.VCenterPort,
			secretConf.Global.Datacenters, sshClientConfig, masterIp)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Restart CSI driver")
		_, err = restartCSIDriver(ctx, client, namespace, csiReplicas)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Verify we can create a PVC and attach it to pod")
		ginkgo.By("Creating Storage Class")
		storageclass, err := createStorageClass(client, nil, nil, "", "", false, "")
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		defer func() {
			ginkgo.By("Delete Storage Class")
			err = client.StorageV1().StorageClasses().Delete(ctx, storageclass.Name, *metav1.NewDeleteOptions(0))
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}()

		ginkgo.By("Creating Pvc")
		pvclaim1, err := createPVC(client, namespace, nil, "", storageclass, "")
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		// Waiting for PVC to be bound.
		var pvclaims1 []*v1.PersistentVolumeClaim
		pvclaims1 = append(pvclaims1, pvclaim1)
		ginkgo.By("Waiting for all claims to be in bound state")
		_, err = fpv.WaitForPVClaimBoundPhase(client, pvclaims1, framework.ClaimProvisionTimeout)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		defer func() {
			err = fpv.DeletePersistentVolumeClaim(client, pvclaim1.Name, namespace)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}()

		ginkgo.By("Creating pod")
		pod1, err := createPod(client, namespace, nil, []*v1.PersistentVolumeClaim{pvclaim1}, false, "")
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		defer func() {
			ginkgo.By(fmt.Sprintf("Deleting the pod %s in namespace %s", pod1.Name, namespace))
			err = fpod.DeletePodWithWait(client, pod1)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}()

		ginkgo.By("Fetch vsphere-config-secret file details")
		secretConf = getConfigSecretFileValues(client, ctx)

		ginkgo.By("Delete previously created vsphere-config-secret file")
		err = deleteCsiVsphereSecret(sshClientConfig, masterIp)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Create vsphere-config-secret file with testuser2 credentials")
		err = createCsiVsphereSecret(secretConf.Global.ClusterID,
			secretConf.Global.ClusterDistribution, secretConf.Global.VCenterHostname,
			secretConf.Global.InsecureFlag,
			configSecretUser2Alias, configSecretTestUser1Password, secretConf.Global.VCenterPort,
			secretConf.Global.Datacenters, sshClientConfig, masterIp)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		defer func() {
			ginkgo.By("Reverting back csi-vsphere.conf with its original vcenter user " +
				"and its credentials")
			secretConf = getConfigSecretFileValues(client, ctx)
			err = deleteCsiVsphereSecret(sshClientConfig, masterIp)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			err = createCsiVsphereSecret(secretConf.Global.ClusterID,
				secretConf.Global.ClusterDistribution, secretConf.Global.VCenterHostname,
				secretConf.Global.InsecureFlag,
				vCenterUIUser, vCenterUIPassword, secretConf.Global.VCenterPort,
				secretConf.Global.Datacenters, sshClientConfig, masterIp)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			_, err = restartCSIDriver(ctx, client, namespace, csiReplicas)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}()

		ginkgo.By("Restart CSI driver")
		_, err = restartCSIDriver(ctx, client, namespace, csiReplicas)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Verify we can create a PVC and attach it to pod")
		ginkgo.By("Creating Pvc")
		pvclaim2, err := createPVC(client, namespace, nil, "", storageclass, "")
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		// Waiting for PVC to be bound.
		var pvclaims2 []*v1.PersistentVolumeClaim
		pvclaims2 = append(pvclaims2, pvclaim2)
		ginkgo.By("Waiting for all claims to be in bound state")
		_, err = fpv.WaitForPVClaimBoundPhase(client, pvclaims2, framework.ClaimProvisionTimeout)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		defer func() {
			err = fpv.DeletePersistentVolumeClaim(client, pvclaim2.Name, namespace)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}()

		ginkgo.By("Creating pod")
		pod2, err := createPod(client, namespace, nil, []*v1.PersistentVolumeClaim{pvclaim2}, false, "")
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		defer func() {
			ginkgo.By(fmt.Sprintf("Deleting the pod %s in namespace %s", pod2.Name, namespace))
			err = fpod.DeletePodWithWait(client, pod2)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}()
	})

	/*
		TESTCASE-2
		Change VC user's password
		Steps:
		1. Create a user, user1, with required roles and privileges
		2. Create csi-vsphere.conf with user1's credentials and create vsphere-config-secret
		in turn using that file
		3. Install CSI driver
		4. Verify we can create a PVC and attach it to pod
		5. Change password for user1
		6. try to create a PVC and verify it gets bound successfully
		7. Restart CSI controller pod
		8. Try to create a PVC verify that it is stuck in pending state
		9. Change csi-vsphere.conf with updated user1's credentials and re-create
		vsphere-config-secret in turn using that file
		10. Verify we can create a PVC and attach it to pod
		11. Verify PVC created in step 6 gets bound eventually
		12. Cleanup all objects created during the test
	*/

	ginkgo.It("[csi-config-secret-block][csi-config-secret-file] Change vcenter user and its password "+
		"in vsphere config secret file", func() {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		ginkgo.By("Create testuser1")
		err = createTestUser(vcAddress, vCenterPort, sshClientConfig, masterIp, configSecretTestUser1,
			configSecretTestUser1Password)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		defer func() {
			ginkgo.By("Delete testuser1")
			err = deleteTestUser(vcAddress, vCenterPort, sshClientConfig, masterIp, configSecretTestUser1)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}()
		ginkgo.By("Create testuser2")
		err = createTestUser(vcAddress, vCenterPort, sshClientConfig, masterIp, configSecretTestUser2,
			configSecretTestUser1Password)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		defer func() {
			ginkgo.By("Delete testuser1")
			err = deleteTestUser(vcAddress, vCenterPort, sshClientConfig, masterIp, configSecretTestUser2)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}()

		ginkgo.By("Create roles for testuser1")
		err = createRolesForTestUser(vcAddress, vCenterPort, sshClientConfig, masterIp, configSecretTestUser1)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Create roles for testuser2")
		err = createRolesForTestUser(vcAddress, vCenterPort, sshClientConfig, masterIp, configSecretTestUser2)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Assign required roles and privileges for testuser1 and testuser2")

		ginkgo.By("Set DataCenter level permission for testuser1 and testuser2")
		for i := 0; i < len(dataCenters); i++ {
			err = setDataCenterLevelPermission(vcAddress, vCenterPort, sshClientConfig, masterIp, dataCenters[i],
				configSecretUser1Alias)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			err = setDataCenterLevelPermission(vcAddress, vCenterPort, sshClientConfig, masterIp, dataCenters[i],
				configSecretUser2Alias)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}

		ginkgo.By("Set hosts level permission for testuser1 and testuser2")
		err = setHostLevelPermission(vcAddress, vCenterPort, sshClientConfig, masterIp, configSecretUser1Alias, hosts)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		err = setHostLevelPermission(vcAddress, vCenterPort, sshClientConfig, masterIp, configSecretUser2Alias, hosts)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Set k8s VM level permission for testuser1 and testuser2")
		err = setVMLevelPermission(vcAddress, vCenterPort, sshClientConfig, masterIp, configSecretUser1Alias,
			configSecretTestUser1, vms)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		err = setVMLevelPermission(vcAddress, vCenterPort, sshClientConfig, masterIp, configSecretUser2Alias,
			configSecretTestUser2, vms)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Set cluster level permission for testuser1 and testuser2")
		for i := 0; i < len(clusters); i++ {
			err = setClusterLevelPermission(vcAddress, vCenterPort, sshClientConfig, masterIp,
				configSecretUser1Alias, configSecretTestUser1, clusters[i])
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			err = setClusterLevelPermission(vcAddress, vCenterPort, sshClientConfig, masterIp,
				configSecretUser2Alias, configSecretTestUser2, clusters[i])
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}

		ginkgo.By("Set Datastore level permission for testuser1 and testuser2")
		for i := 0; i < len(dataCenters); i++ {
			err = setDataStoreLevelPermission(vcAddress, vCenterPort, sshClientConfig, masterIp,
				configSecretUser1Alias, configSecretTestUser1, dataCenters[i], datastores)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			err = setDataStoreLevelPermission(vcAddress, vCenterPort, sshClientConfig, masterIp,
				configSecretUser2Alias, configSecretTestUser2, dataCenters[i], datastores)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}

		ginkgo.By("Set search level permission for testuser1 and testuser2")
		err = setSearchlevelPermission(vcAddress, vCenterPort, sshClientConfig, masterIp, configSecretUser1Alias,
			configSecretTestUser1)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		err = setSearchlevelPermission(vcAddress, vCenterPort, sshClientConfig, masterIp, configSecretUser2Alias,
			configSecretTestUser2)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		defer func() {
			ginkgo.By("Delete testUser1, its roles and permissions")
			err = deleteUsersRolesAndPermissions(vcAddress, vCenterPort, sshClientConfig, masterIp,
				configSecretTestUser1, configSecretUser1Alias, dataCenters, clusters, hosts, vms, datastores)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			ginkgo.By("Delete testUser2, its roles and permissions")
			err = deleteUsersRolesAndPermissions(vcAddress, vCenterPort, sshClientConfig, masterIp,
				configSecretTestUser2, configSecretUser2Alias, dataCenters, clusters, hosts, vms,
				datastores)
		}()

		ginkgo.By("Fetch vsphere-config-secret file details")
		secretConf = getConfigSecretFileValues(client, ctx)

		ginkgo.By("Delete previously created vsphere-config-secret file")
		err = deleteCsiVsphereSecret(sshClientConfig, masterIp)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Create vsphere-config-secret file with testuser1 credentials")
		err = createCsiVsphereSecret(secretConf.Global.ClusterID,
			secretConf.Global.ClusterDistribution, secretConf.Global.VCenterHostname,
			secretConf.Global.InsecureFlag,
			configSecretUser1Alias, configSecretTestUser1Password, secretConf.Global.VCenterPort,
			secretConf.Global.Datacenters, sshClientConfig, masterIp)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Restart CSI driver")
		_, err = restartCSIDriver(ctx, client, namespace, csiReplicas)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Verify we can create a PVC and attach it to pod")
		ginkgo.By("Creating Storage Class")
		storageclass, err := createStorageClass(client, nil, nil, "", "", false, "")
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		defer func() {
			ginkgo.By("Delete Storage Class")
			err = client.StorageV1().StorageClasses().Delete(ctx, storageclass.Name, *metav1.NewDeleteOptions(0))
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}()

		ginkgo.By("Creating Pvc")
		pvclaim1, err := createPVC(client, namespace, nil, "", storageclass, "")
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		// Waiting for PVC to be bound.
		var pvclaims1 []*v1.PersistentVolumeClaim
		pvclaims1 = append(pvclaims1, pvclaim1)
		ginkgo.By("Waiting for all claims to be in bound state")
		_, err = fpv.WaitForPVClaimBoundPhase(client, pvclaims1, framework.ClaimProvisionTimeout)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		defer func() {
			err = fpv.DeletePersistentVolumeClaim(client, pvclaim1.Name, namespace)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}()

		ginkgo.By("Creating pod")
		pod1, err := createPod(client, namespace, nil, []*v1.PersistentVolumeClaim{pvclaim1}, false, "")
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		defer func() {
			ginkgo.By(fmt.Sprintf("Deleting the pod %s in namespace %s", pod1.Name, namespace))
			err = fpod.DeletePodWithWait(client, pod1)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}()

		ginkgo.By("Change password for testuser1")
		testUser1NewPassword := "Admin!123"
		err = changeTestUserPassword(vcAddress, vCenterPort, sshClientConfig, masterIp, configSecretTestUser1,
			testUser1NewPassword)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Try to create a PVC and verify it gets bound successfully")
		pvclaim2, err := createPVC(client, namespace, nil, "", storageclass, "")
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		// Waiting for PVC to be bound.
		var pvclaims2 []*v1.PersistentVolumeClaim
		pvclaims2 = append(pvclaims2, pvclaim2)
		ginkgo.By("Waiting for all claims to be in bound state")
		_, err = fpv.WaitForPVClaimBoundPhase(client, pvclaims2, framework.ClaimProvisionTimeout)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		defer func() {
			ginkgo.By("Deleting the PVC")
			err = fpv.DeletePersistentVolumeClaim(client, pvclaim2.Name, namespace)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}()

		ginkgo.By("Restart CSI controller pod")
		csiDeployment, err := client.AppsV1().Deployments(csiNamespace).Get(
			ctx, vSphereCSIControllerPodNamePrefix, metav1.GetOptions{})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		csiPodReplicas := csiDeployment.Spec.Replicas
		framework.Logf("Stopping CSI driver")
		_ = updateDeploymentReplica(client, 0, vSphereCSIControllerPodNamePrefix, csiNamespace)
		framework.Logf("Starting CSI driver")
		deployment, err := client.AppsV1().Deployments(csiNamespace).Get(ctx, vSphereCSIControllerPodNamePrefix,
			metav1.GetOptions{})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		*deployment.Spec.Replicas = *csiPodReplicas
		_, err = client.AppsV1().Deployments(csiNamespace).Update(ctx, deployment, metav1.UpdateOptions{})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Try to create a PVC verify that it is stuck in pending state")
		pvclaim3, err := createPVC(client, namespace, nil, "", storageclass, "")
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		ginkgo.By("Expect claim status to be in Pending state")
		err = fpv.WaitForPersistentVolumeClaimPhase(v1.ClaimPending, client,
			pvclaim3.Namespace, pvclaim3.Name, framework.Poll, time.Minute)
		gomega.Expect(err).NotTo(gomega.HaveOccurred(),
			fmt.Sprintf("Failed to find the volume in pending state with err: %v", err))
		defer func() {
			ginkgo.By("Deleting the PVC")
			err = fpv.DeletePersistentVolumeClaim(client, pvclaim3.Name, namespace)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}()

		ginkgo.By("Fetch vsphere-config-secret file details")
		secretConf = getConfigSecretFileValues(client, ctx)

		ginkgo.By("Delete previously created vsphere-config-secret file")
		err = deleteCsiVsphereSecret(sshClientConfig, masterIp)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Create vsphere-config-secret file with testuser1 credentials")
		err = createCsiVsphereSecret(secretConf.Global.ClusterID,
			secretConf.Global.ClusterDistribution, secretConf.Global.VCenterHostname,
			secretConf.Global.InsecureFlag,
			configSecretUser1Alias, testUser1NewPassword, secretConf.Global.VCenterPort,
			secretConf.Global.Datacenters, sshClientConfig, masterIp)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		defer func() {
			ginkgo.By("Reverting back csi-vsphere.conf with its original vcenter user " +
				"and its credentials")
			secretConf = getConfigSecretFileValues(client, ctx)
			err = deleteCsiVsphereSecret(sshClientConfig, masterIp)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			err = createCsiVsphereSecret(secretConf.Global.ClusterID,
				secretConf.Global.ClusterDistribution, secretConf.Global.VCenterHostname,
				secretConf.Global.InsecureFlag,
				vCenterUIUser, vCenterUIPassword, secretConf.Global.VCenterPort,
				secretConf.Global.Datacenters, sshClientConfig, masterIp)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			_, err = restartCSIDriver(ctx, client, namespace, csiReplicas)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}()

		ginkgo.By("Verify we can create a PVC and attach it to pod")
		ginkgo.By("Creating Pvc")
		pvclaim4, err := createPVC(client, namespace, nil, "", storageclass, "")
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		// Waiting for PVC to be bound.
		var pvclaims4 []*v1.PersistentVolumeClaim
		pvclaims4 = append(pvclaims4, pvclaim4)
		ginkgo.By("Waiting for all claims to be in bound state")
		time.Sleep(5 * time.Minute)
		_, err = fpv.WaitForPVClaimBoundPhase(client, pvclaims4, framework.ClaimProvisionTimeout)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		defer func() {
			// delete pvc
			err = fpv.DeletePersistentVolumeClaim(client, pvclaim4.Name, namespace)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}()

		ginkgo.By("Creating pod")
		pod2, err := createPod(client, namespace, nil, []*v1.PersistentVolumeClaim{pvclaim4}, false, "")
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		defer func() {
			ginkgo.By(fmt.Sprintf("Deleting the pod %s in namespace %s", pod2.Name, namespace))
			err = fpod.DeletePodWithWait(client, pod2)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}()

		ginkgo.By("Verify the PVC which was stuck in Pending state should gets bound eventually")
		pvclaim3, err = client.CoreV1().PersistentVolumeClaims(namespace).Get(ctx, pvclaim3.Name, metav1.GetOptions{})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		gomega.Expect(pvclaim3.Status.Phase == v1.ClaimBound).To(gomega.BeTrue())
	})

	/*
		TESTCASE-3
		Change VC user and password
		Steps:
		1. Create two users (user1 and user2) with required roles and privileges and with different passwords
		2. Create csi-vsphere.conf with user1's credentials and create vsphere-config-secret
		 in turn using that file
		3. Install CSI driver
		4. Verify we can create a PVC and attach it to pod
		5. Change csi-vsphere.conf with user2's credentials and re-create vsphere-config-secret
		in turn using that file
		6. Verify we can create a PVC and attach it to pod
		7. Cleanup all objects created during the test
	*/

	ginkgo.It("[csi-config-secret-block][csi-config-secret-file] Change vcenter users in vsphere "+
		"config secret file having different password", func() {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		ginkgo.By("Create testuser1")
		err = createTestUser(vcAddress, vCenterPort, sshClientConfig, masterIp, configSecretTestUser1,
			configSecretTestUser1Password)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		defer func() {
			ginkgo.By("Delete testuser1")
			err = deleteTestUser(vcAddress, vCenterPort, sshClientConfig, masterIp, configSecretTestUser1)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}()
		ginkgo.By("Create testuser2")
		err = createTestUser(vcAddress, vCenterPort, sshClientConfig, masterIp, configSecretTestUser2,
			configSecretTestUser2Password)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		defer func() {
			ginkgo.By("Delete testuser1")
			err = deleteTestUser(vcAddress, vCenterPort, sshClientConfig, masterIp, configSecretTestUser2)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}()

		ginkgo.By("Create roles for testuser1")
		err = createRolesForTestUser(vcAddress, vCenterPort, sshClientConfig, masterIp, configSecretTestUser1)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Create roles for testuser2")
		err = createRolesForTestUser(vcAddress, vCenterPort, sshClientConfig, masterIp, configSecretTestUser2)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Assign required roles and privileges for testuser1 and testuser2")

		ginkgo.By("Set DataCenter level permission for testuser1 and testuser2")
		for i := 0; i < len(dataCenters); i++ {
			err = setDataCenterLevelPermission(vcAddress, vCenterPort, sshClientConfig, masterIp, dataCenters[i],
				configSecretUser1Alias)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			err = setDataCenterLevelPermission(vcAddress, vCenterPort, sshClientConfig, masterIp, dataCenters[i],
				configSecretUser2Alias)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}

		ginkgo.By("Set hosts level permission for testuser1 and testuser2")
		err = setHostLevelPermission(vcAddress, vCenterPort, sshClientConfig, masterIp, configSecretUser1Alias, hosts)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		err = setHostLevelPermission(vcAddress, vCenterPort, sshClientConfig, masterIp, configSecretUser2Alias, hosts)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Set k8s VM level permission for testuser1 and testuser2")
		err = setVMLevelPermission(vcAddress, vCenterPort, sshClientConfig, masterIp, configSecretUser1Alias,
			configSecretTestUser1, vms)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		err = setVMLevelPermission(vcAddress, vCenterPort, sshClientConfig, masterIp, configSecretUser2Alias,
			configSecretTestUser2, vms)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Set cluster level permission for testuser1 and testuser2")
		for i := 0; i < len(clusters); i++ {
			err = setClusterLevelPermission(vcAddress, vCenterPort, sshClientConfig, masterIp,
				configSecretUser1Alias, configSecretTestUser1, clusters[i])
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			err = setClusterLevelPermission(vcAddress, vCenterPort, sshClientConfig, masterIp,
				configSecretUser2Alias, configSecretTestUser2, clusters[i])
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}

		ginkgo.By("Set Datastore level permission for testuser1 and testuser2")
		for i := 0; i < len(dataCenters); i++ {
			err = setDataStoreLevelPermission(vcAddress, vCenterPort, sshClientConfig, masterIp,
				configSecretUser1Alias, configSecretTestUser1, dataCenters[i], datastores)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			err = setDataStoreLevelPermission(vcAddress, vCenterPort, sshClientConfig, masterIp,
				configSecretUser2Alias, configSecretTestUser2, dataCenters[i], datastores)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}

		ginkgo.By("Set search level permission for testuser1 and testuser2")
		err = setSearchlevelPermission(vcAddress, vCenterPort, sshClientConfig, masterIp, configSecretUser1Alias,
			configSecretTestUser1)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		err = setSearchlevelPermission(vcAddress, vCenterPort, sshClientConfig, masterIp, configSecretUser2Alias,
			configSecretTestUser2)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		defer func() {
			ginkgo.By("Delete testUser1, its roles and permissions")
			err = deleteUsersRolesAndPermissions(vcAddress, vCenterPort, sshClientConfig, masterIp,
				configSecretTestUser1, configSecretUser1Alias, dataCenters, clusters, hosts, vms, datastores)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			ginkgo.By("Delete testUser2, its roles and permissions")
			err = deleteUsersRolesAndPermissions(vcAddress, vCenterPort, sshClientConfig, masterIp,
				configSecretTestUser2, configSecretUser2Alias, dataCenters, clusters, hosts, vms,
				datastores)
		}()

		ginkgo.By("Fetch vsphere-config-secret file details")
		secretConf = getConfigSecretFileValues(client, ctx)

		ginkgo.By("Delete previously created vsphere-config-secret file")
		err = deleteCsiVsphereSecret(sshClientConfig, masterIp)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Create vsphere-config-secret file with testuser1 credentials")
		err = createCsiVsphereSecret(secretConf.Global.ClusterID,
			secretConf.Global.ClusterDistribution, secretConf.Global.VCenterHostname,
			secretConf.Global.InsecureFlag,
			configSecretUser1Alias, configSecretTestUser1Password, secretConf.Global.VCenterPort,
			secretConf.Global.Datacenters, sshClientConfig, masterIp)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Restart CSI driver")
		_, err = restartCSIDriver(ctx, client, csiNamespace, csiReplicas)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Verify we can create a PVC and attach it to pod")
		ginkgo.By("Creating Storage Class")
		storageclass, err := createStorageClass(client, nil, nil, "", "", false, "")
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		defer func() {
			ginkgo.By("Delete Storage Class")
			err = client.StorageV1().StorageClasses().Delete(ctx, storageclass.Name, *metav1.NewDeleteOptions(0))
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}()

		ginkgo.By("Creating Pvc")
		pvclaim1, err := createPVC(client, namespace, nil, "", storageclass, "")
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		// Waiting for PVC to be bound.
		var pvclaims1 []*v1.PersistentVolumeClaim
		pvclaims1 = append(pvclaims1, pvclaim1)
		ginkgo.By("Waiting for all claims to be in bound state")
		_, err = fpv.WaitForPVClaimBoundPhase(client, pvclaims1, framework.ClaimProvisionTimeout)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		defer func() {
			err = fpv.DeletePersistentVolumeClaim(client, pvclaim1.Name, namespace)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}()

		ginkgo.By("Creating pod")
		pod1, err := createPod(client, namespace, nil, []*v1.PersistentVolumeClaim{pvclaim1}, false, "")
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		defer func() {
			ginkgo.By(fmt.Sprintf("Deleting the pod %s in namespace %s", pod1.Name, namespace))
			err = fpod.DeletePodWithWait(client, pod1)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}()

		ginkgo.By("Fetch vsphere-config-secret file details")
		secretConf = getConfigSecretFileValues(client, ctx)

		ginkgo.By("Delete previously created vsphere-config-secret file")
		err = deleteCsiVsphereSecret(sshClientConfig, masterIp)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Create vsphere-config-secret file with testuser2 credentials")
		err = createCsiVsphereSecret(secretConf.Global.ClusterID,
			secretConf.Global.ClusterDistribution, secretConf.Global.VCenterHostname,
			secretConf.Global.InsecureFlag,
			configSecretUser2Alias, configSecretTestUser2Password, secretConf.Global.VCenterPort,
			secretConf.Global.Datacenters, sshClientConfig, masterIp)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		defer func() {
			ginkgo.By("Reverting back csi-vsphere.conf with its original vcenter user " +
				"and its credentials")
			secretConf = getConfigSecretFileValues(client, ctx)
			err = deleteCsiVsphereSecret(sshClientConfig, masterIp)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			err = createCsiVsphereSecret(secretConf.Global.ClusterID,
				secretConf.Global.ClusterDistribution, secretConf.Global.VCenterHostname,
				secretConf.Global.InsecureFlag,
				vCenterUIUser, vCenterUIPassword, secretConf.Global.VCenterPort,
				secretConf.Global.Datacenters, sshClientConfig, masterIp)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			ginkgo.By("Restart CSI driver")
			_, err = restartCSIDriver(ctx, client, csiNamespace, csiReplicas)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}()

		ginkgo.By("Restart CSI driver")
		_, err = restartCSIDriver(ctx, client, csiNamespace, csiReplicas)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Verify we can create a PVC and attach it to pod")
		ginkgo.By("Creating Pvc")
		pvclaim2, err := createPVC(client, namespace, nil, "", storageclass, "")
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		// Waiting for PVC to be bound.
		var pvclaims2 []*v1.PersistentVolumeClaim
		pvclaims2 = append(pvclaims2, pvclaim2)
		ginkgo.By("Waiting for all claims to be in bound state")
		_, err = fpv.WaitForPVClaimBoundPhase(client, pvclaims2, framework.ClaimProvisionTimeout)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		defer func() {
			err = fpv.DeletePersistentVolumeClaim(client, pvclaim2.Name, namespace)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}()

		ginkgo.By("Creating pod")
		pod2, err := createPod(client, namespace, nil, []*v1.PersistentVolumeClaim{pvclaim2}, false, "")
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		defer func() {
			ginkgo.By(fmt.Sprintf("Deleting the pod %s in namespace %s", pod2.Name, namespace))
			err = fpod.DeletePodWithWait(client, pod2)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}()
	})

	/*
		TESTCASE-4
		Change IP to host name and vice versa
		Steps:
		1. Create two users (user1 and user2) with required roles and privileges and with different passwords
		2. Create csi-vsphere.conf with user1's credentials and VC IP and then
		create vsphere-config-secret in turn using that file
		3. Install CSI driver
		4. Verify we can create a PVC and attach it to pod
		5. Change csi-vsphere.conf to use VC hostname instead of VC IP and re-create
		vsphere-config-secret in turn using that file
		6. Verify we can create a PVC and attach it to pod
		7. Change csi-vsphere.conf to use VC IP instead of VC hostname and re-create
		vsphere-config-secret in turn using that file
		8. Verify we can create a PVC and attach it to pod
		9. Cleanup all objects created during the test
	*/

	ginkgo.It("[csi-config-secret-block][csi-config-secret-file] Change vcenter ip to hostname and "+
		"viceversa in vsphere config secret file", func() {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		ginkgo.By("Create testuser1")
		err = createTestUser(vcAddress, vCenterPort, sshClientConfig, masterIp, configSecretTestUser1,
			configSecretTestUser1Password)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		defer func() {
			ginkgo.By("Delete testuser1")
			err = deleteTestUser(vcAddress, vCenterPort, sshClientConfig, masterIp, configSecretTestUser1)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}()
		ginkgo.By("Create testuser2")
		err = createTestUser(vcAddress, vCenterPort, sshClientConfig, masterIp, configSecretTestUser2,
			configSecretTestUser2Password)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		defer func() {
			ginkgo.By("Delete testuser1")
			err = deleteTestUser(vcAddress, vCenterPort, sshClientConfig, masterIp, configSecretTestUser2)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}()

		ginkgo.By("Create roles for testuser1")
		err = createRolesForTestUser(vcAddress, vCenterPort, sshClientConfig, masterIp, configSecretTestUser1)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Create roles for testuser2")
		err = createRolesForTestUser(vcAddress, vCenterPort, sshClientConfig, masterIp, configSecretTestUser2)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Assign required roles and privileges for testuser1 and testuser2")

		ginkgo.By("Set DataCenter level permission for testuser1 and testuser2")
		for i := 0; i < len(dataCenters); i++ {
			err = setDataCenterLevelPermission(vcAddress, vCenterPort, sshClientConfig, masterIp, dataCenters[i],
				configSecretUser1Alias)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			err = setDataCenterLevelPermission(vcAddress, vCenterPort, sshClientConfig, masterIp, dataCenters[i],
				configSecretUser2Alias)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}

		ginkgo.By("Set hosts level permission for testuser1 and testuser2")
		err = setHostLevelPermission(vcAddress, vCenterPort, sshClientConfig, masterIp, configSecretUser1Alias, hosts)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		err = setHostLevelPermission(vcAddress, vCenterPort, sshClientConfig, masterIp, configSecretUser2Alias, hosts)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Set k8s VM level permission for testuser1 and testuser2")
		err = setVMLevelPermission(vcAddress, vCenterPort, sshClientConfig, masterIp, configSecretUser1Alias,
			configSecretTestUser1, vms)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		err = setVMLevelPermission(vcAddress, vCenterPort, sshClientConfig, masterIp, configSecretUser2Alias,
			configSecretTestUser2, vms)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Set cluster level permission for testuser1 and testuser2")
		for i := 0; i < len(clusters); i++ {
			err = setClusterLevelPermission(vcAddress, vCenterPort, sshClientConfig, masterIp,
				configSecretUser1Alias, configSecretTestUser1, clusters[i])
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			err = setClusterLevelPermission(vcAddress, vCenterPort, sshClientConfig, masterIp,
				configSecretUser2Alias, configSecretTestUser2, clusters[i])
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}

		ginkgo.By("Set Datastore level permission for testuser1 and testuser2")
		for i := 0; i < len(dataCenters); i++ {
			err = setDataStoreLevelPermission(vcAddress, vCenterPort, sshClientConfig, masterIp,
				configSecretUser1Alias, configSecretTestUser1, dataCenters[i], datastores)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			err = setDataStoreLevelPermission(vcAddress, vCenterPort, sshClientConfig, masterIp,
				configSecretUser2Alias, configSecretTestUser2, dataCenters[i], datastores)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}

		ginkgo.By("Set search level permission for testuser1 and testuser2")
		err = setSearchlevelPermission(vcAddress, vCenterPort, sshClientConfig, masterIp, configSecretUser1Alias,
			configSecretTestUser1)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		err = setSearchlevelPermission(vcAddress, vCenterPort, sshClientConfig, masterIp, configSecretUser2Alias,
			configSecretTestUser2)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		defer func() {
			ginkgo.By("Delete testUser1, its roles and permissions")
			err = deleteUsersRolesAndPermissions(vcAddress, vCenterPort, sshClientConfig, masterIp,
				configSecretTestUser1, configSecretUser1Alias, dataCenters, clusters, hosts, vms, datastores)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			ginkgo.By("Delete testUser2, its roles and permissions")
			err = deleteUsersRolesAndPermissions(vcAddress, vCenterPort, sshClientConfig, masterIp,
				configSecretTestUser2, configSecretUser2Alias, dataCenters, clusters, hosts, vms,
				datastores)
		}()

		ginkgo.By("Fetch vsphere-config-secret file details")
		secretConf = getConfigSecretFileValues(client, ctx)
		vCenterIP := secretConf.Global.VCenterHostname

		ginkgo.By("Delete previously created vsphere-config-secret file")
		err = deleteCsiVsphereSecret(sshClientConfig, masterIp)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Create vsphere-config-secret file with testuser1 credentials" +
			"and provide vcenter IP")
		err = createCsiVsphereSecret(secretConf.Global.ClusterID,
			secretConf.Global.ClusterDistribution, secretConf.Global.VCenterHostname,
			secretConf.Global.InsecureFlag,
			configSecretUser1Alias, configSecretTestUser1Password, secretConf.Global.VCenterPort,
			secretConf.Global.Datacenters, sshClientConfig, masterIp)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Restart CSI driver")
		_, err = restartCSIDriver(ctx, client, csiNamespace, csiReplicas)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Verify we can create a PVC and attach it to pod")
		ginkgo.By("Creating Storage Class")
		storageclass, err := createStorageClass(client, nil, nil, "", "", false, "")
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		defer func() {
			ginkgo.By("Delete Storage Class")
			err = client.StorageV1().StorageClasses().Delete(ctx, storageclass.Name, *metav1.NewDeleteOptions(0))
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}()

		ginkgo.By("Creating Pvc")
		pvclaim1, err := createPVC(client, namespace, nil, "", storageclass, "")
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		// Waiting for PVC to be bound.
		var pvclaims1 []*v1.PersistentVolumeClaim
		pvclaims1 = append(pvclaims1, pvclaim1)
		ginkgo.By("Waiting for all claims to be in bound state")
		_, err = fpv.WaitForPVClaimBoundPhase(client, pvclaims1, framework.ClaimProvisionTimeout)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		defer func() {
			err = fpv.DeletePersistentVolumeClaim(client, pvclaim1.Name, namespace)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}()

		ginkgo.By("Creating pod")
		pod1, err := createPod(client, namespace, nil, []*v1.PersistentVolumeClaim{pvclaim1}, false, "")
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		defer func() {
			ginkgo.By(fmt.Sprintf("Deleting the pod %s in namespace %s", pod1.Name, namespace))
			err = fpod.DeletePodWithWait(client, pod1)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}()

		ginkgo.By("Fetch vsphere-config-secret file details")
		secretConf = getConfigSecretFileValues(client, ctx)

		ginkgo.By("Get vcenter hotsname")
		vCenterHostName, err := getVcenterHostName(sshClientConfig, masterIp, secretConf.Global.VCenterHostname)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Delete previously created vsphere-config-secret file")
		err = deleteCsiVsphereSecret(sshClientConfig, masterIp)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Create vsphere-config-secret file with testuser1 credentials" +
			"and by providing vcenter host name")
		err = createCsiVsphereSecret(secretConf.Global.ClusterID,
			secretConf.Global.ClusterDistribution, vCenterHostName,
			secretConf.Global.InsecureFlag,
			configSecretUser1Alias, configSecretTestUser1Password, secretConf.Global.VCenterPort,
			secretConf.Global.Datacenters, sshClientConfig, masterIp)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Restart CSI driver")
		_, err = restartCSIDriver(ctx, client, csiNamespace, csiReplicas)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Verify we can create a PVC and attach it to pod")
		ginkgo.By("Creating Pvc")
		pvclaim2, err := createPVC(client, namespace, nil, "", storageclass, "")
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		// Waiting for PVC to be bound.
		var pvclaims2 []*v1.PersistentVolumeClaim
		pvclaims2 = append(pvclaims2, pvclaim2)
		ginkgo.By("Waiting for all claims to be in bound state")
		_, err = fpv.WaitForPVClaimBoundPhase(client, pvclaims2, framework.ClaimProvisionTimeout)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		defer func() {
			// delete pvc
			err = fpv.DeletePersistentVolumeClaim(client, pvclaim2.Name, namespace)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}()

		ginkgo.By("Creating pod")
		pod2, err := createPod(client, namespace, nil, []*v1.PersistentVolumeClaim{pvclaim2}, false, "")
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		defer func() {
			// delete pod
			ginkgo.By(fmt.Sprintf("Deleting the pod %s in namespace %s", pod2.Name, namespace))
			err = fpod.DeletePodWithWait(client, pod2)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}()

		ginkgo.By("Fetch vsphere-config-secret file details")
		secretConf = getConfigSecretFileValues(client, ctx)

		ginkgo.By("Delete previously created vsphere-config-secret file")
		err = deleteCsiVsphereSecret(sshClientConfig, masterIp)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Create vsphere-config-secret file with testuser1 credentials" +
			"and provide vcenter IP")
		err = createCsiVsphereSecret(secretConf.Global.ClusterID,
			secretConf.Global.ClusterDistribution, vCenterIP,
			secretConf.Global.InsecureFlag,
			configSecretUser1Alias, configSecretTestUser1Password, secretConf.Global.VCenterPort,
			secretConf.Global.Datacenters, sshClientConfig, masterIp)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		defer func() {
			ginkgo.By("Reverting back csi-vsphere.conf with its original vcenter user " +
				"and its credentials")
			secretConf = getConfigSecretFileValues(client, ctx)
			err = deleteCsiVsphereSecret(sshClientConfig, masterIp)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			err = createCsiVsphereSecret(secretConf.Global.ClusterID,
				secretConf.Global.ClusterDistribution, secretConf.Global.VCenterHostname,
				secretConf.Global.InsecureFlag,
				vCenterUIUser, vCenterUIPassword, secretConf.Global.VCenterPort,
				secretConf.Global.Datacenters, sshClientConfig, masterIp)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			ginkgo.By("Restart CSI driver")
			_, err = restartCSIDriver(ctx, client, csiNamespace, csiReplicas)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}()

		ginkgo.By("Restart CSI driver")
		_, err = restartCSIDriver(ctx, client, csiNamespace, csiReplicas)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Verify we can create a PVC and attach it to pod")
		ginkgo.By("Creating Pvc")
		pvclaim3, err := createPVC(client, namespace, nil, "", storageclass, "")
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		// Waiting for PVC to be bound.
		var pvclaims3 []*v1.PersistentVolumeClaim
		pvclaims3 = append(pvclaims3, pvclaim3)
		ginkgo.By("Waiting for all claims to be in bound state")
		_, err = fpv.WaitForPVClaimBoundPhase(client, pvclaims3, framework.ClaimProvisionTimeout)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		defer func() {
			// delete pvc
			err = fpv.DeletePersistentVolumeClaim(client, pvclaim3.Name, namespace)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}()

		ginkgo.By("Creating pod")
		pod3, err := createPod(client, namespace, nil, []*v1.PersistentVolumeClaim{pvclaim3}, false, "")
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		defer func() {
			// delete pod
			ginkgo.By(fmt.Sprintf("Deleting the pod %s in namespace %s", pod3.Name, namespace))
			err = fpod.DeletePodWithWait(client, pod3)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}()
	})

	/*
		TESTCASE-6
		Add wrong user and switch back to correct user
		Steps:
		1. Create a user, user1, with required roles and privileges
		2. Create csi-vsphere.conf with user1's credentials and create vsphere-config-secret
		in turn using that file
		3. Install CSI driver
		4. Verify we can create a PVC and attach it to pod
		5. Change csi-vsphere.conf and add a dummy user and re-create vsphere-config-secret in turn using that file
		6. Try to create a PVC verify that it is stuck in pending state
		(Note: PVC will go to bound state with the dummy user who does not have any privilege
			to login to VC, This is because CSI will continue with the previous session ID
		7. Change csi-vsphere.conf with updated user1's credentials and re-create vsphere-config-secret
		in turn using that file
		8. Verify we can create a PVC and attach it to pod
		9. Cleanup all objects created during the test
	*/

	ginkgo.It("[csi-config-secret-block][csi-config-secret-file] Change vcenter user to wrong "+
		"user and switch back to correct user in vsphere config secret file", func() {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		ginkgo.By("Create testuser1")
		err = createTestUser(vcAddress, vCenterPort, sshClientConfig, masterIp, configSecretTestUser1,
			configSecretTestUser1Password)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		defer func() {
			ginkgo.By("Delete testuser1")
			err = deleteTestUser(vcAddress, vCenterPort, sshClientConfig, masterIp, configSecretTestUser1)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}()
		ginkgo.By("Create testuser2")
		err = createTestUser(vcAddress, vCenterPort, sshClientConfig, masterIp, configSecretTestUser2,
			configSecretTestUser2Password)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		defer func() {
			ginkgo.By("Delete testuser2")
			err = deleteTestUser(vcAddress, vCenterPort, sshClientConfig, masterIp, configSecretTestUser2)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}()

		ginkgo.By("Create roles for testuser1")
		err = createRolesForTestUser(vcAddress, vCenterPort, sshClientConfig, masterIp, configSecretTestUser1)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Create roles for testuser2")
		err = createRolesForTestUser(vcAddress, vCenterPort, sshClientConfig, masterIp, configSecretTestUser2)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Assign required roles and privileges for testuser1 and testuser2")

		ginkgo.By("Set DataCenter level permission for testuser1 and testuser2")
		for i := 0; i < len(dataCenters); i++ {
			err = setDataCenterLevelPermission(vcAddress, vCenterPort, sshClientConfig, masterIp, dataCenters[i],
				configSecretUser1Alias)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			err = setDataCenterLevelPermission(vcAddress, vCenterPort, sshClientConfig, masterIp, dataCenters[i],
				configSecretUser2Alias)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}

		ginkgo.By("Set hosts level permission for testuser1 and testuser2")
		err = setHostLevelPermission(vcAddress, vCenterPort, sshClientConfig, masterIp, configSecretUser1Alias, hosts)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		err = setHostLevelPermission(vcAddress, vCenterPort, sshClientConfig, masterIp, configSecretUser2Alias, hosts)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Set k8s VM level permission for testuser1 and testuser2")
		err = setVMLevelPermission(vcAddress, vCenterPort, sshClientConfig, masterIp, configSecretUser1Alias,
			configSecretTestUser1, vms)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		err = setVMLevelPermission(vcAddress, vCenterPort, sshClientConfig, masterIp, configSecretUser2Alias,
			configSecretTestUser2, vms)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Set cluster level permission for testuser1 and testuser2")
		for i := 0; i < len(clusters); i++ {
			err = setClusterLevelPermission(vcAddress, vCenterPort, sshClientConfig, masterIp,
				configSecretUser1Alias, configSecretTestUser1, clusters[i])
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			err = setClusterLevelPermission(vcAddress, vCenterPort, sshClientConfig, masterIp,
				configSecretUser2Alias, configSecretTestUser2, clusters[i])
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}

		ginkgo.By("Set Datastore level permission for testuser1 and testuser2")
		for i := 0; i < len(dataCenters); i++ {
			err = setDataStoreLevelPermission(vcAddress, vCenterPort, sshClientConfig, masterIp,
				configSecretUser1Alias, configSecretTestUser1, dataCenters[i], datastores)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			err = setDataStoreLevelPermission(vcAddress, vCenterPort, sshClientConfig, masterIp,
				configSecretUser2Alias, configSecretTestUser2, dataCenters[i], datastores)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}

		ginkgo.By("Set search level permission for testuser1 and testuser2")
		err = setSearchlevelPermission(vcAddress, vCenterPort, sshClientConfig, masterIp, configSecretUser1Alias,
			configSecretTestUser1)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		err = setSearchlevelPermission(vcAddress, vCenterPort, sshClientConfig, masterIp, configSecretUser2Alias,
			configSecretTestUser2)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		defer func() {
			ginkgo.By("Delete testUser1, its roles and permissions")
			err = deleteUsersRolesAndPermissions(vcAddress, vCenterPort, sshClientConfig, masterIp,
				configSecretTestUser1, configSecretUser1Alias, dataCenters, clusters, hosts, vms, datastores)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			ginkgo.By("Delete testUser2, its roles and permissions")
			err = deleteUsersRolesAndPermissions(vcAddress, vCenterPort, sshClientConfig, masterIp,
				configSecretTestUser2, configSecretUser2Alias, dataCenters, clusters, hosts, vms,
				datastores)
		}()

		ginkgo.By("Fetch vsphere-config-secret file details")
		secretConf = getConfigSecretFileValues(client, ctx)

		ginkgo.By("Delete previously created vsphere-config-secret file")
		err = deleteCsiVsphereSecret(sshClientConfig, masterIp)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Create vsphere-config-secret file with testuser1 credentials")
		err = createCsiVsphereSecret(secretConf.Global.ClusterID,
			secretConf.Global.ClusterDistribution, secretConf.Global.VCenterHostname,
			secretConf.Global.InsecureFlag,
			configSecretUser1Alias, configSecretTestUser1Password, secretConf.Global.VCenterPort,
			secretConf.Global.Datacenters, sshClientConfig, masterIp)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Restart CSI driver")
		_, err = restartCSIDriver(ctx, client, csiNamespace, csiReplicas)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Verify we can create a PVC and attach it to pod")
		ginkgo.By("Creating Storage Class")
		storageclass, err := createStorageClass(client, nil, nil, "", "", false, "")
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		defer func() {
			ginkgo.By("Delete Storage Class")
			err = client.StorageV1().StorageClasses().Delete(ctx, storageclass.Name, *metav1.NewDeleteOptions(0))
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}()

		ginkgo.By("Creating Pvc")
		pvclaim1, err := createPVC(client, namespace, nil, "", storageclass, "")
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		// Waiting for PVC to be bound.
		var pvclaims1 []*v1.PersistentVolumeClaim
		pvclaims1 = append(pvclaims1, pvclaim1)
		ginkgo.By("Waiting for all claims to be in bound state")
		_, err = fpv.WaitForPVClaimBoundPhase(client, pvclaims1, framework.ClaimProvisionTimeout)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		defer func() {
			err = fpv.DeletePersistentVolumeClaim(client, pvclaim1.Name, namespace)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}()

		ginkgo.By("Creating pod")
		pod1, err := createPod(client, namespace, nil, []*v1.PersistentVolumeClaim{pvclaim1}, false, "")
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		defer func() {
			ginkgo.By(fmt.Sprintf("Deleting the pod %s in namespace %s", pod1.Name, namespace))
			err = fpod.DeletePodWithWait(client, pod1)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}()

		ginkgo.By("Fetch vsphere-config-secret file details")
		secretConf = getConfigSecretFileValues(client, ctx)

		ginkgo.By("Delete previously created vsphere-config-secret file")
		err = deleteCsiVsphereSecret(sshClientConfig, masterIp)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Create vsphere-config-secret file with dummy user credentials")
		testUser := "dummyUser@vsphere.local"
		err = createCsiVsphereSecret(secretConf.Global.ClusterID,
			secretConf.Global.ClusterDistribution, secretConf.Global.VCenterHostname,
			secretConf.Global.InsecureFlag,
			testUser, configSecretTestUser1Password, secretConf.Global.VCenterPort,
			secretConf.Global.Datacenters, sshClientConfig, masterIp)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Verify we can create a PVC and attach it to pod")
		ginkgo.By("Creating Pvc")
		pvclaim2, err := createPVC(client, namespace, nil, "", storageclass, "")
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		// Waiting for PVC to be bound.
		var pvclaims2 []*v1.PersistentVolumeClaim
		pvclaims2 = append(pvclaims2, pvclaim2)
		ginkgo.By("Waiting for all claims to be in bound state")
		_, err = fpv.WaitForPVClaimBoundPhase(client, pvclaims2, framework.ClaimProvisionTimeout)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		defer func() {
			// delete pvc
			err = fpv.DeletePersistentVolumeClaim(client, pvclaim2.Name, namespace)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}()

		ginkgo.By("Creating pod")
		pod2, err := createPod(client, namespace, nil, []*v1.PersistentVolumeClaim{pvclaim2}, false, "")
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		defer func() {
			// delete pod
			ginkgo.By(fmt.Sprintf("Deleting the pod %s in namespace %s", pod2.Name, namespace))
			err = fpod.DeletePodWithWait(client, pod2)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}()

		ginkgo.By("Fetch vsphere-config-secret file details")
		secretConf = getConfigSecretFileValues(client, ctx)

		ginkgo.By("Delete previously created vsphere-config-secret file")
		err = deleteCsiVsphereSecret(sshClientConfig, masterIp)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Create vsphere-config-secret file with testuser1 credentials")
		err = createCsiVsphereSecret(secretConf.Global.ClusterID,
			secretConf.Global.ClusterDistribution, secretConf.Global.VCenterHostname,
			secretConf.Global.InsecureFlag,
			configSecretUser1Alias, configSecretTestUser1Password, secretConf.Global.VCenterPort,
			secretConf.Global.Datacenters, sshClientConfig, masterIp)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		defer func() {
			ginkgo.By("Reverting back csi-vsphere.conf with its original vcenter user " +
				"and its credentials")
			secretConf = getConfigSecretFileValues(client, ctx)
			err = deleteCsiVsphereSecret(sshClientConfig, masterIp)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			err = createCsiVsphereSecret(secretConf.Global.ClusterID,
				secretConf.Global.ClusterDistribution, secretConf.Global.VCenterHostname,
				secretConf.Global.InsecureFlag,
				vCenterUIUser, vCenterUIPassword, secretConf.Global.VCenterPort,
				secretConf.Global.Datacenters, sshClientConfig, masterIp)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			ginkgo.By("Restart CSI driver")
			_, err = restartCSIDriver(ctx, client, csiNamespace, csiReplicas)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}()

		ginkgo.By("Restart CSI driver")
		_, err = restartCSIDriver(ctx, client, csiNamespace, csiReplicas)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Verify we can create a PVC and attach it to pod")
		ginkgo.By("Creating Pvc")
		pvclaim3, err := createPVC(client, namespace, nil, "", storageclass, "")
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		// Waiting for PVC to be bound.
		var pvclaims3 []*v1.PersistentVolumeClaim
		pvclaims3 = append(pvclaims3, pvclaim3)
		ginkgo.By("Waiting for all claims to be in bound state")
		_, err = fpv.WaitForPVClaimBoundPhase(client, pvclaims3, framework.ClaimProvisionTimeout)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		defer func() {
			err = fpv.DeletePersistentVolumeClaim(client, pvclaim3.Name, namespace)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}()

		ginkgo.By("Creating pod")
		pod3, err := createPod(client, namespace, nil, []*v1.PersistentVolumeClaim{pvclaim3}, false, "")
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		defer func() {
			ginkgo.By(fmt.Sprintf("Deleting the pod %s in namespace %s", pod3.Name, namespace))
			err = fpod.DeletePodWithWait(client, pod3)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}()
	})

	/*
		TESTCASE-10
		Add the wrong datacenter and switch back to the correct datacenter
		Steps:
		1. Create a user, user1, with required roles and privileges
		2. Create csi-vsphere.conf with user1's credentials and create vsphere-config-secret
		in turn using that file
		3. Install CSI driver
		4. Verify we can create a PVC and attach it to pod
		5.  Change csi-vsphere.conf and add a dummy datacentre and re-create vsphere-config-secret
		in turn using that file
		6. Try to create a PVC verify that it is stuck in pending state
		7. Change csi-vsphere.conf with updated datacentre and re-create vsphere-config-secret
		in turn using that file
		8. Verify we can create a PVC and attach it to pod
		9. Verify PVC created in step 6 gets bound eventually
		10. Cleanup all objects created during the test
	*/

	ginkgo.It("[csi-config-secret-block][csi-config-secret-file] Add wrong datacenter and switch back "+
		"to the correct datacenter in vsphere config secret file", func() {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		ginkgo.By("Create testuser1")
		err = createTestUser(vcAddress, vCenterPort, sshClientConfig, masterIp, configSecretTestUser1,
			configSecretTestUser1Password)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		defer func() {
			ginkgo.By("Delete testuser1")
			err = deleteTestUser(vcAddress, vCenterPort, sshClientConfig, masterIp, configSecretTestUser1)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}()
		ginkgo.By("Create testuser2")
		err = createTestUser(vcAddress, vCenterPort, sshClientConfig, masterIp, configSecretTestUser2,
			configSecretTestUser2Password)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		defer func() {
			ginkgo.By("Delete testuser2")
			err = deleteTestUser(vcAddress, vCenterPort, sshClientConfig, masterIp, configSecretTestUser2)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}()

		ginkgo.By("Create roles for testuser1")
		err = createRolesForTestUser(vcAddress, vCenterPort, sshClientConfig, masterIp, configSecretTestUser1)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Create roles for testuser2")
		err = createRolesForTestUser(vcAddress, vCenterPort, sshClientConfig, masterIp, configSecretTestUser2)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Assign required roles and privileges for testuser1 and testuser2")

		ginkgo.By("Set DataCenter level permission for testuser1 and testuser2")
		for i := 0; i < len(dataCenters); i++ {
			err = setDataCenterLevelPermission(vcAddress, vCenterPort, sshClientConfig, masterIp, dataCenters[i],
				configSecretUser1Alias)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			err = setDataCenterLevelPermission(vcAddress, vCenterPort, sshClientConfig, masterIp, dataCenters[i],
				configSecretUser2Alias)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}

		ginkgo.By("Set hosts level permission for testuser1 and testuser2")
		err = setHostLevelPermission(vcAddress, vCenterPort, sshClientConfig, masterIp, configSecretUser1Alias, hosts)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		err = setHostLevelPermission(vcAddress, vCenterPort, sshClientConfig, masterIp, configSecretUser2Alias, hosts)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Set k8s VM level permission for testuser1 and testuser2")
		err = setVMLevelPermission(vcAddress, vCenterPort, sshClientConfig, masterIp, configSecretUser1Alias,
			configSecretTestUser1, vms)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		err = setVMLevelPermission(vcAddress, vCenterPort, sshClientConfig, masterIp, configSecretUser2Alias,
			configSecretTestUser2, vms)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Set cluster level permission for testuser1 and testuser2")
		for i := 0; i < len(clusters); i++ {
			err = setClusterLevelPermission(vcAddress, vCenterPort, sshClientConfig, masterIp,
				configSecretUser1Alias, configSecretTestUser1, clusters[i])
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			err = setClusterLevelPermission(vcAddress, vCenterPort, sshClientConfig, masterIp,
				configSecretUser2Alias, configSecretTestUser2, clusters[i])
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}

		ginkgo.By("Set Datastore level permission for testuser1 and testuser2")
		for i := 0; i < len(dataCenters); i++ {
			err = setDataStoreLevelPermission(vcAddress, vCenterPort, sshClientConfig, masterIp,
				configSecretUser1Alias, configSecretTestUser1, dataCenters[i], datastores)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			err = setDataStoreLevelPermission(vcAddress, vCenterPort, sshClientConfig, masterIp,
				configSecretUser2Alias, configSecretTestUser2, dataCenters[i], datastores)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}

		ginkgo.By("Set search level permission for testuser1 and testuser2")
		err = setSearchlevelPermission(vcAddress, vCenterPort, sshClientConfig, masterIp, configSecretUser1Alias,
			configSecretTestUser1)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		err = setSearchlevelPermission(vcAddress, vCenterPort, sshClientConfig, masterIp, configSecretUser2Alias,
			configSecretTestUser2)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		defer func() {
			ginkgo.By("Delete testUser1, its roles and permissions")
			err = deleteUsersRolesAndPermissions(vcAddress, vCenterPort, sshClientConfig, masterIp,
				configSecretTestUser1, configSecretUser1Alias, dataCenters, clusters, hosts, vms, datastores)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			ginkgo.By("Delete testUser2, its roles and permissions")
			err = deleteUsersRolesAndPermissions(vcAddress, vCenterPort, sshClientConfig, masterIp,
				configSecretTestUser2, configSecretUser2Alias, dataCenters, clusters, hosts, vms,
				datastores)
		}()

		ginkgo.By("Fetch vsphere-config-secret file details")
		secretConf = getConfigSecretFileValues(client, ctx)
		OriginalDataCenter := secretConf.Global.Datacenters

		ginkgo.By("Delete previously created vsphere-config-secret file")
		err = deleteCsiVsphereSecret(sshClientConfig, masterIp)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Create vsphere-config-secret file with testuser1 credentials")
		err = createCsiVsphereSecret(secretConf.Global.ClusterID,
			secretConf.Global.ClusterDistribution, secretConf.Global.VCenterHostname,
			secretConf.Global.InsecureFlag,
			configSecretUser1Alias, configSecretTestUser1Password, secretConf.Global.VCenterPort,
			secretConf.Global.Datacenters, sshClientConfig, masterIp)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		ginkgo.By("Restart CSI driver")
		_, err = restartCSIDriver(ctx, client, csiNamespace, csiReplicas)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Verify we can create a PVC and attach it to pod")
		ginkgo.By("Creating Storage Class")
		storageclass, err := createStorageClass(client, nil, nil, "", "", false, "")
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		defer func() {
			ginkgo.By("Delete Storage Class")
			err = client.StorageV1().StorageClasses().Delete(ctx, storageclass.Name, *metav1.NewDeleteOptions(0))
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}()

		ginkgo.By("Creating Pvc")
		pvclaim1, err := createPVC(client, namespace, nil, "", storageclass, "")
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		// Waiting for PVC to be bound.
		var pvclaims1 []*v1.PersistentVolumeClaim
		pvclaims1 = append(pvclaims1, pvclaim1)
		ginkgo.By("Waiting for all claims to be in bound state")
		_, err = fpv.WaitForPVClaimBoundPhase(client, pvclaims1, framework.ClaimProvisionTimeout)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		defer func() {
			err = fpv.DeletePersistentVolumeClaim(client, pvclaim1.Name, namespace)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}()

		ginkgo.By("Creating pod")
		pod1, err := createPod(client, namespace, nil, []*v1.PersistentVolumeClaim{pvclaim1}, false, "")
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		defer func() {
			ginkgo.By(fmt.Sprintf("Deleting the pod %s in namespace %s", pod1.Name, namespace))
			err = fpod.DeletePodWithWait(client, pod1)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}()

		ginkgo.By("Fetch vsphere-config-secret file details")
		secretConf = getConfigSecretFileValues(client, ctx)

		ginkgo.By("Delete previously created vsphere-config-secret file")
		err = deleteCsiVsphereSecret(sshClientConfig, masterIp)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Create vsphere-config-secret file with dummy datacenter")
		dummyDataCenter := "dummy-data-center"
		err = createCsiVsphereSecret(secretConf.Global.ClusterID,
			secretConf.Global.ClusterDistribution, secretConf.Global.VCenterHostname,
			secretConf.Global.InsecureFlag,
			configSecretUser1Alias, configSecretTestUser1Password, secretConf.Global.VCenterPort,
			dummyDataCenter, sshClientConfig, masterIp)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Try to create a PVC verify that it is stuck in pending state")
		pvclaim2, err := createPVC(client, namespace, nil, "", storageclass, "")
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		ginkgo.By("Expect claim status to be in Pending state")
		err = fpv.WaitForPersistentVolumeClaimPhase(v1.ClaimPending, client,
			pvclaim2.Namespace, pvclaim2.Name, framework.Poll, time.Minute)
		gomega.Expect(err).NotTo(gomega.HaveOccurred(),
			fmt.Sprintf("Failed to find the volume in pending state with err: %v", err))
		defer func() {
			// delete pvc
			err = fpv.DeletePersistentVolumeClaim(client, pvclaim2.Name, namespace)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}()

		ginkgo.By("Fetch vsphere-config-secret file details")
		secretConf = getConfigSecretFileValues(client, ctx)

		ginkgo.By("Delete previously created vsphere-config-secret file")
		err = deleteCsiVsphereSecret(sshClientConfig, masterIp)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Create vsphere-config-secret file with testuser1 credentials")
		err = createCsiVsphereSecret(secretConf.Global.ClusterID,
			secretConf.Global.ClusterDistribution, secretConf.Global.VCenterHostname,
			secretConf.Global.InsecureFlag,
			configSecretUser1Alias, configSecretTestUser1Password, secretConf.Global.VCenterPort,
			OriginalDataCenter, sshClientConfig, masterIp)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		defer func() {
			ginkgo.By("Reverting back csi-vsphere.conf with its original vcenter user " +
				"and its credentials")
			secretConf = getConfigSecretFileValues(client, ctx)
			err = deleteCsiVsphereSecret(sshClientConfig, masterIp)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			err = createCsiVsphereSecret(secretConf.Global.ClusterID,
				secretConf.Global.ClusterDistribution, secretConf.Global.VCenterHostname,
				secretConf.Global.InsecureFlag,
				vCenterUIUser, vCenterUIPassword, secretConf.Global.VCenterPort,
				secretConf.Global.Datacenters, sshClientConfig, masterIp)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			ginkgo.By("Restart CSI driver")
			_, err = restartCSIDriver(ctx, client, csiNamespace, csiReplicas)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}()

		ginkgo.By("Restart CSI driver")
		_, err = restartCSIDriver(ctx, client, csiNamespace, csiReplicas)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Verify we can create a PVC and attach it to pod")
		ginkgo.By("Creating Pvc")
		pvclaim3, err := createPVC(client, namespace, nil, "", storageclass, "")
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		// Waiting for PVC to be bound.
		var pvclaims3 []*v1.PersistentVolumeClaim
		pvclaims3 = append(pvclaims3, pvclaim3)
		ginkgo.By("Waiting for all claims to be in bound state")
		_, err = fpv.WaitForPVClaimBoundPhase(client, pvclaims3, framework.ClaimProvisionTimeout)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		defer func() {
			// delete pvc
			err = fpv.DeletePersistentVolumeClaim(client, pvclaim3.Name, namespace)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}()

		ginkgo.By("Creating pod")
		pod2, err := createPod(client, namespace, nil, []*v1.PersistentVolumeClaim{pvclaim3}, false, "")
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		defer func() {
			// delete pod
			ginkgo.By(fmt.Sprintf("Deleting the pod %s in namespace %s", pod2.Name, namespace))
			err = fpod.DeletePodWithWait(client, pod2)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}()

		ginkgo.By("Verify the PVC which was stuck in Pending state should gets bound eventually")
		pvclaim2, err = client.CoreV1().PersistentVolumeClaims(namespace).Get(ctx, pvclaim2.Name, metav1.GetOptions{})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		gomega.Expect(pvclaim2.Status.Phase == v1.ClaimBound).To(gomega.BeTrue())
	})

	/*
		TESTCASE-7
		Add a user without adding required roles/privileges to provision volume and switch back to the correct one
		Steps:
		1. Create two users user1 and user2(with same password) where user1 has the required roles
		and privileges and user2 has required roles and privileges to login to VC
		2. Create csi-vsphere.conf with user1's credentials and create vsphere-config-secret in turn
		using that file
		3. Install CSI driver
		4. Verify we can create a PVC and attach it to pod
		5. Change csi-vsphere.conf and add user2's credentials and re-create vsphere-config-secret in turn using that file
		6. Try to create a PVC verify that it is stuck in pending state
		7. Change csi-vsphere.conf with updated user1's credentials and re-create
		vsphere-config-secret in turn using that file
		8. Verify we can create a PVC and attach it to pod
		9. Verify PVC created in step 6 gets bound eventually
		10. Cleanup all objects created during the test
	*/

	ginkgo.It("[csi-config-secret-block][csi-config-secret-file] Add a user without adding required "+
		"roles and privileges to provision volume and switch back to the correct one", func() {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		ginkgo.By("Create testuser1")
		err = createTestUser(vcAddress, vCenterPort, sshClientConfig, masterIp, configSecretTestUser1,
			configSecretTestUser1Password)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Create testuser2")
		err = createTestUser(vcAddress, vCenterPort, sshClientConfig, masterIp, configSecretTestUser2,
			configSecretTestUser1Password)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Create roles for testuser1")
		err = createRolesForTestUser(vcAddress, vCenterPort, sshClientConfig, masterIp, configSecretTestUser1)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Create roles for testuser2")
		err = createRolesForTestUser(vcAddress, vCenterPort, sshClientConfig, masterIp, configSecretTestUser2)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Assign required roles and privileges for testuser1 and testuser2")

		ginkgo.By("Set DataCenter level permission for testuser1")
		for i := 0; i < len(dataCenters); i++ {
			err = setDataCenterLevelPermission(vcAddress, vCenterPort, sshClientConfig, masterIp, dataCenters[i],
				configSecretUser1Alias)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}

		ginkgo.By("Set hosts level permission for testuser1 and testuser2")
		err = setHostLevelPermission(vcAddress, vCenterPort, sshClientConfig, masterIp, configSecretUser1Alias, hosts)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		err = setHostLevelPermission(vcAddress, vCenterPort, sshClientConfig, masterIp, configSecretUser2Alias, hosts)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Set k8s VM level permission for testuser1")
		err = setVMLevelPermission(vcAddress, vCenterPort, sshClientConfig, masterIp, configSecretUser1Alias,
			configSecretTestUser1, vms)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Set cluster level permission for testuser1 and testuser2")
		for i := 0; i < len(clusters); i++ {
			err = setClusterLevelPermission(vcAddress, vCenterPort, sshClientConfig, masterIp,
				configSecretUser1Alias, configSecretTestUser1, clusters[i])
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			err = setClusterLevelPermission(vcAddress, vCenterPort, sshClientConfig, masterIp,
				configSecretUser2Alias, configSecretTestUser2, clusters[i])
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}

		ginkgo.By("Set Datastore level permission for testuser1")
		for i := 0; i < len(dataCenters); i++ {
			err = setDataStoreLevelPermission(vcAddress, vCenterPort, sshClientConfig, masterIp,
				configSecretUser1Alias, configSecretTestUser1, dataCenters[i], datastores)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}

		ginkgo.By("Set search level permission for testuser1")
		err = setSearchlevelPermission(vcAddress, vCenterPort, sshClientConfig, masterIp, configSecretUser1Alias,
			configSecretTestUser1)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		defer func() {
			ginkgo.By("Delete testUser1, its roles and permissions")
			err = deleteUsersRolesAndPermissions(vcAddress, vCenterPort, sshClientConfig, masterIp,
				configSecretTestUser1, configSecretUser1Alias, dataCenters, clusters, hosts, vms, datastores)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			ginkgo.By("Delete testUser2, its roles and permissions")
			err = deleteUsersRolesAndPermissions(vcAddress, vCenterPort, sshClientConfig, masterIp,
				configSecretTestUser2, configSecretUser2Alias, dataCenters, clusters, hosts, vms,
				datastores)
		}()

		ginkgo.By("Fetch vsphere-config-secret file details")
		secretConf = getConfigSecretFileValues(client, ctx)

		ginkgo.By("Delete previously created vsphere-config-secret file")
		err = deleteCsiVsphereSecret(sshClientConfig, masterIp)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Create vsphere-config-secret file with testuser1 credentials")
		err = createCsiVsphereSecret(secretConf.Global.ClusterID,
			secretConf.Global.ClusterDistribution, secretConf.Global.VCenterHostname,
			secretConf.Global.InsecureFlag,
			configSecretUser1Alias, configSecretTestUser1Password, secretConf.Global.VCenterPort,
			secretConf.Global.Datacenters, sshClientConfig, masterIp)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Restart CSI driver")
		_, err = restartCSIDriver(ctx, client, csiNamespace, csiReplicas)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Verify we can create a PVC and attach it to pod")
		ginkgo.By("Creating Storage Class")
		storageclass, err := createStorageClass(client, nil, nil, "", "", false, "")
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		defer func() {
			ginkgo.By("Delete Storage Class")
			err = client.StorageV1().StorageClasses().Delete(ctx, storageclass.Name, *metav1.NewDeleteOptions(0))
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}()

		ginkgo.By("Creating Pvc")
		pvclaim1, err := createPVC(client, namespace, nil, "", storageclass, "")
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		// Waiting for PVC to be bound.
		var pvclaims1 []*v1.PersistentVolumeClaim
		pvclaims1 = append(pvclaims1, pvclaim1)
		ginkgo.By("Waiting for all claims to be in bound state")
		_, err = fpv.WaitForPVClaimBoundPhase(client, pvclaims1, framework.ClaimProvisionTimeout)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		defer func() {
			err = fpv.DeletePersistentVolumeClaim(client, pvclaim1.Name, namespace)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}()

		ginkgo.By("Creating pod")
		pod1, err := createPod(client, namespace, nil, []*v1.PersistentVolumeClaim{pvclaim1}, false, "")
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		defer func() {
			ginkgo.By(fmt.Sprintf("Deleting the pod %s in namespace %s", pod1.Name, namespace))
			err = fpod.DeletePodWithWait(client, pod1)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}()

		ginkgo.By("Fetch vsphere-config-secret file details")
		secretConf = getConfigSecretFileValues(client, ctx)

		ginkgo.By("Delete previously created vsphere-config-secret file")
		err = deleteCsiVsphereSecret(sshClientConfig, masterIp)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Create vsphere-config-secret file with testuser2 credentials")
		err = createCsiVsphereSecret(secretConf.Global.ClusterID,
			secretConf.Global.ClusterDistribution, secretConf.Global.VCenterHostname,
			secretConf.Global.InsecureFlag,
			configSecretUser2Alias, configSecretTestUser1Password, secretConf.Global.VCenterPort,
			secretConf.Global.Datacenters, sshClientConfig, masterIp)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Restart CSI driver")
		_, err = restartCSIDriver(ctx, client, csiNamespace, csiReplicas)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Try to create a PVC verify that it is stuck in pending state")
		pvclaim2, err := createPVC(client, namespace, nil, "", storageclass, "")
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		ginkgo.By("Expect claim status to be in Pending state")
		err = fpv.WaitForPersistentVolumeClaimPhase(v1.ClaimPending, client,
			pvclaim2.Namespace, pvclaim2.Name, framework.Poll, time.Minute)
		gomega.Expect(err).NotTo(gomega.HaveOccurred(),
			fmt.Sprintf("Failed to find the volume in pending state with err: %v", err))
		defer func() {
			ginkgo.By("Deleting the PVC")
			err = fpv.DeletePersistentVolumeClaim(client, pvclaim2.Name, namespace)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}()

		ginkgo.By("Fetch vsphere-config-secret file details")
		secretConf = getConfigSecretFileValues(client, ctx)

		ginkgo.By("Delete previously created vsphere-config-secret file")
		err = deleteCsiVsphereSecret(sshClientConfig, masterIp)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Create vsphere-config-secret file with testuser1 credentials")
		err = createCsiVsphereSecret(secretConf.Global.ClusterID,
			secretConf.Global.ClusterDistribution, secretConf.Global.VCenterHostname,
			secretConf.Global.InsecureFlag,
			configSecretUser1Alias, configSecretTestUser1Password, secretConf.Global.VCenterPort,
			secretConf.Global.Datacenters, sshClientConfig, masterIp)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		defer func() {
			ginkgo.By("Reverting back csi-vsphere.conf with its original vcenter user " +
				"and its credentials")
			secretConf = getConfigSecretFileValues(client, ctx)
			err = deleteCsiVsphereSecret(sshClientConfig, masterIp)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			err = createCsiVsphereSecret(secretConf.Global.ClusterID,
				secretConf.Global.ClusterDistribution, secretConf.Global.VCenterHostname,
				secretConf.Global.InsecureFlag,
				vCenterUIUser, vCenterUIPassword, secretConf.Global.VCenterPort,
				secretConf.Global.Datacenters, sshClientConfig, masterIp)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			ginkgo.By("Restart CSI driver")
			_, err = restartCSIDriver(ctx, client, csiNamespace, csiReplicas)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}()

		ginkgo.By("Restart CSI driver")
		_, err = restartCSIDriver(ctx, client, csiNamespace, csiReplicas)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Verify we can create a PVC and attach it to pod")
		ginkgo.By("Creating Pvc")
		pvclaim3, err := createPVC(client, namespace, nil, "", storageclass, "")
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		// Waiting for PVC to be bound.
		var pvclaims3 []*v1.PersistentVolumeClaim
		pvclaims3 = append(pvclaims3, pvclaim3)
		ginkgo.By("Waiting for all claims to be in bound state")
		_, err = fpv.WaitForPVClaimBoundPhase(client, pvclaims3, framework.ClaimProvisionTimeout)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		defer func() {
			// delete pvc
			err = fpv.DeletePersistentVolumeClaim(client, pvclaim3.Name, namespace)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}()

		ginkgo.By("Creating pod")
		pod2, err := createPod(client, namespace, nil, []*v1.PersistentVolumeClaim{pvclaim3}, false, "")
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		defer func() {
			ginkgo.By(fmt.Sprintf("Deleting the pod %s in namespace %s", pod2.Name, namespace))
			err = fpod.DeletePodWithWait(client, pod2)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}()

		time.Sleep(5 * time.Minute)
		ginkgo.By("Verify the PVC which was stuck in Pending state should gets bound eventually")
		pvclaim2, err = client.CoreV1().PersistentVolumeClaims(namespace).Get(ctx, pvclaim2.Name, metav1.GetOptions{})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		gomega.Expect(pvclaim2.Status.Phase == v1.ClaimBound).To(gomega.BeTrue())
	})

	/*
		TESTCASE-8
		Add necessary roles/privileges to the user post CSI driver creation
		Steps:
		1. Create a user, user1, without the required roles and privileges
		2. Create csi-vsphere.conf with user1's credentials and create vsphere-config-secret in turn using that file
		3. Install CSI driver
		4. Try to create a PVC verify that it is stuck in pending state
		5. Add necessary roles/privileges the user1
		6. Try to create another PVC
		7. Both PVCs created in prior steps should get binding eventually
		8. Cleanup all objects created during the test
	*/

	ginkgo.It("[csi-config-secret-block][csi-config-secret-file] Add necessary roles and privileges "+
		"to the user post CSI driver creation", func() {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		ginkgo.By("Create testuser1")
		err = createTestUser(vcAddress, vCenterPort, sshClientConfig, masterIp, configSecretTestUser1,
			configSecretTestUser1Password)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		defer func() {
			ginkgo.By("Delete testuser1")
			err = deleteTestUser(vcAddress, vCenterPort, sshClientConfig, masterIp, configSecretTestUser1)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}()
		ginkgo.By("Create testuser2")
		err = createTestUser(vcAddress, vCenterPort, sshClientConfig, masterIp, configSecretTestUser2,
			configSecretTestUser2Password)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		defer func() {
			ginkgo.By("Delete testuser2")
			err = deleteTestUser(vcAddress, vCenterPort, sshClientConfig, masterIp, configSecretTestUser2)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}()

		ginkgo.By("Assign minimal roles and privileges to testuser1")

		ginkgo.By("Create minimal roles for testuser1")
		err = createRolesForTestUser(vcAddress, vCenterPort, sshClientConfig, masterIp, configSecretTestUser1)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Set DataCenter level permission for testuser1")
		for i := 0; i < len(dataCenters); i++ {
			err = setDataCenterLevelPermission(vcAddress, vCenterPort, sshClientConfig, masterIp, dataCenters[i],
				configSecretUser1Alias)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}

		ginkgo.By("Set hosts level permission for testuser1")
		err = setHostLevelPermission(vcAddress, vCenterPort, sshClientConfig, masterIp, configSecretUser1Alias, hosts)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Set search level permission for testuser1")
		err = setSearchlevelPermission(vcAddress, vCenterPort, sshClientConfig, masterIp, configSecretUser1Alias,
			configSecretTestUser1)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Fetch vsphere-config-secret file details")
		secretConf = getConfigSecretFileValues(client, ctx)

		ginkgo.By("Delete previously created vsphere-config-secret file")
		err = deleteCsiVsphereSecret(sshClientConfig, masterIp)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Create vsphere-config-secret file with testuser1 credentials")
		err = createCsiVsphereSecret(secretConf.Global.ClusterID,
			secretConf.Global.ClusterDistribution, secretConf.Global.VCenterHostname,
			secretConf.Global.InsecureFlag,
			configSecretUser1Alias, configSecretTestUser1Password, secretConf.Global.VCenterPort,
			secretConf.Global.Datacenters, sshClientConfig, masterIp)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		defer func() {
			ginkgo.By("Reverting back csi-vsphere.conf with its original vcenter user " +
				"and its credentials")
			secretConf = getConfigSecretFileValues(client, ctx)
			err = deleteCsiVsphereSecret(sshClientConfig, masterIp)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			err = createCsiVsphereSecret(secretConf.Global.ClusterID,
				secretConf.Global.ClusterDistribution, secretConf.Global.VCenterHostname,
				secretConf.Global.InsecureFlag,
				vCenterUIUser, vCenterUIPassword, secretConf.Global.VCenterPort,
				secretConf.Global.Datacenters, sshClientConfig, masterIp)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			ginkgo.By("Restart CSI driver")
			_, err = restartCSIDriver(ctx, client, csiNamespace, csiReplicas)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}()

		ginkgo.By("Restart CSI driver")
		_, err = restartCSIDriver(ctx, client, csiNamespace, csiReplicas)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Creating Storage Class")
		storageclass, err := createStorageClass(client, nil, nil, "", "", false, "")
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		defer func() {
			ginkgo.By("Delete Storage Class")
			err = client.StorageV1().StorageClasses().Delete(ctx, storageclass.Name, *metav1.NewDeleteOptions(0))
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}()

		ginkgo.By("Try to create a PVC verify that it is stuck in pending state")
		pvclaim1, err := createPVC(client, namespace, nil, "", storageclass, "")
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		ginkgo.By("Expect claim status to be in Pending state")
		err = fpv.WaitForPersistentVolumeClaimPhase(v1.ClaimPending, client,
			pvclaim1.Namespace, pvclaim1.Name, framework.Poll, time.Minute)
		gomega.Expect(err).NotTo(gomega.HaveOccurred(),
			fmt.Sprintf("Failed to find the volume in pending state with err: %v", err))
		defer func() {
			ginkgo.By("Deleting the PVC")
			err = fpv.DeletePersistentVolumeClaim(client, pvclaim1.Name, namespace)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}()

		ginkgo.By("Assign required roles and privileges to testuser1")

		ginkgo.By("Set k8s VM level permission for testuser1")
		err = setVMLevelPermission(vcAddress, vCenterPort, sshClientConfig, masterIp, configSecretUser1Alias,
			configSecretTestUser1, vms)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Set cluster level permission for testuser1")
		for i := 0; i < len(clusters); i++ {
			err = setClusterLevelPermission(vcAddress, vCenterPort, sshClientConfig, masterIp,
				configSecretUser1Alias, configSecretTestUser1, clusters[i])
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}

		ginkgo.By("Set Datastore level permission for testuser1")
		for i := 0; i < len(dataCenters); i++ {
			err = setDataStoreLevelPermission(vcAddress, vCenterPort, sshClientConfig, masterIp,
				configSecretUser1Alias, configSecretTestUser1, dataCenters[i], datastores)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}
		defer func() {
			ginkgo.By("Delete testUser1, its roles and permissions")
			err = deleteUsersRolesAndPermissions(vcAddress, vCenterPort, sshClientConfig, masterIp,
				configSecretTestUser1, configSecretUser1Alias, dataCenters, clusters, hosts, vms, datastores)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}()

		ginkgo.By("Restart CSI driver")
		_, err = restartCSIDriver(ctx, client, csiNamespace, csiReplicas)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Verify we can create a PVC and attach it to pod")
		ginkgo.By("Creating Pvc")
		pvclaim2, err := createPVC(client, namespace, nil, "", storageclass, "")
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		// Waiting for PVC to be bound.
		var pvclaims2 []*v1.PersistentVolumeClaim
		pvclaims2 = append(pvclaims2, pvclaim2)
		ginkgo.By("Waiting for all claims to be in bound state")
		_, err = fpv.WaitForPVClaimBoundPhase(client, pvclaims2, framework.ClaimProvisionTimeout)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		defer func() {
			err = fpv.DeletePersistentVolumeClaim(client, pvclaim2.Name, namespace)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}()

		ginkgo.By("Creating pod")
		pod1, err := createPod(client, namespace, nil, []*v1.PersistentVolumeClaim{pvclaim2}, false, "")
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		defer func() {
			ginkgo.By(fmt.Sprintf("Deleting the pod %s in namespace %s", pod1.Name, namespace))
			err = fpod.DeletePodWithWait(client, pod1)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}()

		time.Sleep(5 * time.Minute)
		ginkgo.By("Verify the PVC which was stuck in Pending state should gets bound eventually")
		pvclaim1, err = client.CoreV1().PersistentVolumeClaims(namespace).Get(ctx, pvclaim1.Name, metav1.GetOptions{})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		gomega.Expect(pvclaim1.Status.Phase == v1.ClaimBound).To(gomega.BeTrue())
	})

	/*
		TESTCASE-11
		Add the wrong targetvSANFileShareDatastoreURLs and switch back to the correct
		targetvSANFileShareDatastoreURLs
		Steps:
		1. Create a user, user1, with required roles and privileges
		2. Create csi-vsphere.conf with user1's credentials and create vsphere-config-secret,
		in turn, using that file
		3. Install CSI driver
		4. Verify we can create a PVC and attach it to the pod
		5. Change csi-vsphere.conf and add an empty value to targetvSANFileShareDatastoreURLs and
		re-create vsphere-config-secret in turn using that file
		6. Verify CSI driver comes up successfully
		7. Try to create a PVC and verify PVCs are provisioned successfully
		8. Change csi-vsphere.conf with updated targetvSANFileShareDatastoreURLs and
		re-create vsphere-config-secret, in turn, using that file
		9. Verify we can create a PVC and attach it to the pod
		10. Cleanup all objects created during the test
	*/

	ginkgo.It("[csi-config-secret-file] Add wrong targetvSANFileShareDatastoreURLs and switch back to the correct "+
		"targetvSANFileShareDatastoreURLs", func() {
		ctx, cancel := context.WithCancel(context.Background())
		targetDsURL := GetAndExpectStringEnvVar(envSharedDatastoreURL)
		defer cancel()
		ginkgo.By("Create testuser1")
		err = createTestUser(vcAddress, vCenterPort, sshClientConfig, masterIp, configSecretTestUser1,
			configSecretTestUser1Password)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		defer func() {
			ginkgo.By("Delete testuser1")
			err = deleteTestUser(vcAddress, vCenterPort, sshClientConfig, masterIp, configSecretTestUser1)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}()
		ginkgo.By("Create testuser2")
		err = createTestUser(vcAddress, vCenterPort, sshClientConfig, masterIp, configSecretTestUser2,
			configSecretTestUser1Password)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		defer func() {
			ginkgo.By("Delete testuser2")
			err = deleteTestUser(vcAddress, vCenterPort, sshClientConfig, masterIp, configSecretTestUser2)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}()

		ginkgo.By("Create roles for testuser1")
		err = createRolesForTestUser(vcAddress, vCenterPort, sshClientConfig, masterIp, configSecretTestUser1)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Create roles for testuser2")
		err = createRolesForTestUser(vcAddress, vCenterPort, sshClientConfig, masterIp, configSecretTestUser2)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Assign required roles and privileges for testuser1 and testuser2")

		ginkgo.By("Set DataCenter level permission for testuser1 and testuser2")
		for i := 0; i < len(dataCenters); i++ {
			err = setDataCenterLevelPermission(vcAddress, vCenterPort, sshClientConfig, masterIp, dataCenters[i],
				configSecretUser1Alias)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			err = setDataCenterLevelPermission(vcAddress, vCenterPort, sshClientConfig, masterIp, dataCenters[i],
				configSecretUser2Alias)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}

		ginkgo.By("Set hosts level permission for testuser1 and testuser2")
		err = setHostLevelPermission(vcAddress, vCenterPort, sshClientConfig, masterIp, configSecretUser1Alias, hosts)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		err = setHostLevelPermission(vcAddress, vCenterPort, sshClientConfig, masterIp, configSecretUser2Alias, hosts)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Set k8s VM level permission for testuser1 and testuser2")
		err = setVMLevelPermission(vcAddress, vCenterPort, sshClientConfig, masterIp, configSecretUser1Alias,
			configSecretTestUser1, vms)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		err = setVMLevelPermission(vcAddress, vCenterPort, sshClientConfig, masterIp, configSecretUser2Alias,
			configSecretTestUser2, vms)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Set cluster level permission for testuser1 and testuser2")
		for i := 0; i < len(clusters); i++ {
			err = setClusterLevelPermission(vcAddress, vCenterPort, sshClientConfig, masterIp,
				configSecretUser1Alias, configSecretTestUser1, clusters[i])
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			err = setClusterLevelPermission(vcAddress, vCenterPort, sshClientConfig, masterIp,
				configSecretUser2Alias, configSecretTestUser2, clusters[i])
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}

		ginkgo.By("Set Datastore level permission for testuser1 and testuser2")
		for i := 0; i < len(dataCenters); i++ {
			err = setDataStoreLevelPermission(vcAddress, vCenterPort, sshClientConfig, masterIp,
				configSecretUser1Alias, configSecretTestUser1, dataCenters[i], datastores)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			err = setDataStoreLevelPermission(vcAddress, vCenterPort, sshClientConfig, masterIp,
				configSecretUser2Alias, configSecretTestUser2, dataCenters[i], datastores)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}

		ginkgo.By("Set search level permission for testuser1 and testuser2")
		err = setSearchlevelPermission(vcAddress, vCenterPort, sshClientConfig, masterIp, configSecretUser1Alias,
			configSecretTestUser1)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		err = setSearchlevelPermission(vcAddress, vCenterPort, sshClientConfig, masterIp, configSecretUser2Alias,
			configSecretTestUser2)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		defer func() {
			ginkgo.By("Delete testUser1, its roles and permissions")
			err = deleteUsersRolesAndPermissions(vcAddress, vCenterPort, sshClientConfig, masterIp,
				configSecretTestUser1, configSecretUser1Alias, dataCenters, clusters, hosts, vms, datastores)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			ginkgo.By("Delete testUser2, its roles and permissions")
			err = deleteUsersRolesAndPermissions(vcAddress, vCenterPort, sshClientConfig, masterIp,
				configSecretTestUser2, configSecretUser2Alias, dataCenters, clusters, hosts, vms,
				datastores)
		}()

		ginkgo.By("Fetch vsphere-config-secret file details")
		secretConf = getConfigSecretFileValues(client, ctx)

		ginkgo.By("Delete previously created vsphere-config-secret file")
		err = deleteCsiVsphereSecret(sshClientConfig, masterIp)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Create vsphere-config-secret file with testuser1 credentials")
		err = createCsiVsphereSecret(secretConf.Global.ClusterID,
			secretConf.Global.ClusterDistribution, secretConf.Global.VCenterHostname,
			secretConf.Global.InsecureFlag,
			configSecretUser1Alias, configSecretTestUser1Password, secretConf.Global.VCenterPort,
			secretConf.Global.Datacenters, sshClientConfig, masterIp)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Restart CSI driver")
		_, err = restartCSIDriver(ctx, client, namespace, csiReplicas)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Verify we can create a PVC and attach it to pod")
		ginkgo.By("Creating Storage Class")
		storageclass, err := createStorageClass(client, nil, nil, "", "", false, "")
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		defer func() {
			ginkgo.By("Delete Storage Class")
			err = client.StorageV1().StorageClasses().Delete(ctx, storageclass.Name, *metav1.NewDeleteOptions(0))
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}()

		ginkgo.By("Creating Pvc")
		pvclaim1, err := createPVC(client, namespace, nil, "", storageclass, "")
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		// Waiting for PVC to be bound.
		var pvclaims1 []*v1.PersistentVolumeClaim
		pvclaims1 = append(pvclaims1, pvclaim1)
		ginkgo.By("Waiting for all claims to be in bound state")
		_, err = fpv.WaitForPVClaimBoundPhase(client, pvclaims1, framework.ClaimProvisionTimeout)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		defer func() {
			err = fpv.DeletePersistentVolumeClaim(client, pvclaim1.Name, namespace)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}()

		ginkgo.By("Creating pod")
		pod1, err := createPod(client, namespace, nil, []*v1.PersistentVolumeClaim{pvclaim1}, false, "")
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		defer func() {
			ginkgo.By(fmt.Sprintf("Deleting the pod %s in namespace %s", pod1.Name, namespace))
			err = fpod.DeletePodWithWait(client, pod1)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}()

		ginkgo.By("Fetch vsphere-config-secret file details")
		secretConf = getConfigSecretFileValues(client, ctx)

		ginkgo.By("Delete previously created vsphere-config-secret file")
		err = deleteCsiVsphereSecret(sshClientConfig, masterIp)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Create vsphere-config-secret file with testuser1 credentials and " +
			"pass an empty value to target datastore url")
		err = createCsiVsphereSecretForFileVanilla(secretConf.Global.ClusterID,
			secretConf.Global.ClusterDistribution, secretConf.Global.VCenterHostname,
			secretConf.Global.InsecureFlag,
			configSecretUser1Alias, configSecretTestUser1Password, secretConf.Global.VCenterPort,
			secretConf.Global.Datacenters, sshClientConfig, masterIp, "")
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Restart CSI driver")
		_, err = restartCSIDriver(ctx, client, namespace, csiReplicas)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Verify we can create a PVC and attach it to pod")
		ginkgo.By("Creating Pvc")
		pvclaim2, err := createPVC(client, namespace, nil, "", storageclass, "")
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		// Waiting for PVC to be bound.
		var pvclaims2 []*v1.PersistentVolumeClaim
		pvclaims2 = append(pvclaims2, pvclaim2)
		ginkgo.By("Waiting for all claims to be in bound state")
		_, err = fpv.WaitForPVClaimBoundPhase(client, pvclaims2, framework.ClaimProvisionTimeout)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		defer func() {
			err = fpv.DeletePersistentVolumeClaim(client, pvclaim2.Name, namespace)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}()

		ginkgo.By("Creating pod")
		pod2, err := createPod(client, namespace, nil, []*v1.PersistentVolumeClaim{pvclaim2}, false, "")
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		defer func() {
			ginkgo.By(fmt.Sprintf("Deleting the pod %s in namespace %s", pod2.Name, namespace))
			err = fpod.DeletePodWithWait(client, pod2)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}()

		ginkgo.By("Fetch vsphere-config-secret file details")
		secretConf = getConfigSecretFileValuesForFileVanilla(client, ctx)

		ginkgo.By("Delete previously created vsphere-config-secret file")
		err = deleteCsiVsphereSecret(sshClientConfig, masterIp)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Create vsphere-config-secret file with testuser1 credentials and " +
			"pass correct target datatsore url")
		err = createCsiVsphereSecretForFileVanilla(secretConf.Global.ClusterID,
			secretConf.Global.ClusterDistribution, secretConf.Global.VCenterHostname,
			secretConf.Global.InsecureFlag,
			configSecretUser1Alias, configSecretTestUser1Password, secretConf.Global.VCenterPort,
			secretConf.Global.Datacenters, sshClientConfig, masterIp, targetDsURL)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		defer func() {
			ginkgo.By("Reverting back csi-vsphere.conf with its original vcenter user " +
				"and its credentials")
			secretConf = getConfigSecretFileValues(client, ctx)
			err = deleteCsiVsphereSecret(sshClientConfig, masterIp)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			err = createCsiVsphereSecret(secretConf.Global.ClusterID,
				secretConf.Global.ClusterDistribution, secretConf.Global.VCenterHostname,
				secretConf.Global.InsecureFlag,
				vCenterUIUser, vCenterUIPassword, secretConf.Global.VCenterPort,
				secretConf.Global.Datacenters, sshClientConfig, masterIp)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			_, err = restartCSIDriver(ctx, client, namespace, csiReplicas)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}()

		ginkgo.By("Restart CSI driver")
		_, err = restartCSIDriver(ctx, client, namespace, csiReplicas)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Verify we can create a PVC and attach it to pod")
		ginkgo.By("Creating Pvc")
		pvclaim3, err := createPVC(client, namespace, nil, "", storageclass, "")
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		// Waiting for PVC to be bound.
		var pvclaims3 []*v1.PersistentVolumeClaim
		pvclaims3 = append(pvclaims3, pvclaim3)
		ginkgo.By("Waiting for all claims to be in bound state")
		_, err = fpv.WaitForPVClaimBoundPhase(client, pvclaims3, framework.ClaimProvisionTimeout)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		defer func() {
			err = fpv.DeletePersistentVolumeClaim(client, pvclaim3.Name, namespace)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}()

		ginkgo.By("Creating pod")
		pod3, err := createPod(client, namespace, nil, []*v1.PersistentVolumeClaim{pvclaim3}, false, "")
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		defer func() {
			ginkgo.By(fmt.Sprintf("Deleting the pod %s in namespace %s", pod3.Name, namespace))
			err = fpod.DeletePodWithWait(client, pod3)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}()

	})

	/* TESTCASE-12
		VC with Custom Port
		Steps:
	    1. Create a VC with non default port
	    2. Create a user, user1, with required roles and privileges
	    3. Create csi-vsphere.conf with user1's credentials and default VC port, and
		create vsphere-config-secret in turn using that file
	    4. Install CSI driver
	    5. Create PVC and verify is stuck in pending state
	    6. Change VC HTTPS port to non default port in the vsphere-config-secret
	    7. Try to create a PVC verify that it is successful and PVC created in step5 is also successful
	    8. Verify we can create pods using the PVC created in step 5 and 7
	    9. Cleanup all objects created during the test
	*/

	ginkgo.It("[vc-custom-port] VC with Custom Port", func() {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		ginkgo.By("Create testuser1")
		err = createTestUser(vcAddress, vCenterPort, sshClientConfig, masterIp, configSecretTestUser1,
			configSecretTestUser1Password)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		defer func() {
			ginkgo.By("Delete testuser1")
			err = deleteTestUser(vcAddress, vCenterPort, sshClientConfig, masterIp, configSecretTestUser1)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}()
		ginkgo.By("Create testuser2")
		err = createTestUser(vcAddress, vCenterPort, sshClientConfig, masterIp, configSecretTestUser2,
			configSecretTestUser1Password)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		defer func() {
			ginkgo.By("Delete testuser2")
			err = deleteTestUser(vcAddress, vCenterPort, sshClientConfig, masterIp, configSecretTestUser2)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}()

		ginkgo.By("Create roles for testuser1")
		err = createRolesForTestUser(vcAddress, vCenterPort, sshClientConfig, masterIp, configSecretTestUser1)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Create roles for testuser2")
		err = createRolesForTestUser(vcAddress, vCenterPort, sshClientConfig, masterIp, configSecretTestUser2)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Assign required roles and privileges for testuser1 and testuser2")

		ginkgo.By("Set DataCenter level permission for testuser1 and testuser2")
		for i := 0; i < len(dataCenters); i++ {
			err = setDataCenterLevelPermission(vcAddress, vCenterPort, sshClientConfig, masterIp, dataCenters[i],
				configSecretUser1Alias)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			err = setDataCenterLevelPermission(vcAddress, vCenterPort, sshClientConfig, masterIp, dataCenters[i],
				configSecretUser2Alias)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}

		ginkgo.By("Set hosts level permission for testuser1 and testuser2")
		err = setHostLevelPermission(vcAddress, vCenterPort, sshClientConfig, masterIp, configSecretUser1Alias, hosts)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		err = setHostLevelPermission(vcAddress, vCenterPort, sshClientConfig, masterIp, configSecretUser2Alias, hosts)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Set k8s VM level permission for testuser1 and testuser2")
		err = setVMLevelPermission(vcAddress, vCenterPort, sshClientConfig, masterIp, configSecretUser1Alias,
			configSecretTestUser1, vms)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		err = setVMLevelPermission(vcAddress, vCenterPort, sshClientConfig, masterIp, configSecretUser2Alias,
			configSecretTestUser2, vms)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Set cluster level permission for testuser1 and testuser2")
		for i := 0; i < len(clusters); i++ {
			err = setClusterLevelPermission(vcAddress, vCenterPort, sshClientConfig, masterIp,
				configSecretUser1Alias, configSecretTestUser1, clusters[i])
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			err = setClusterLevelPermission(vcAddress, vCenterPort, sshClientConfig, masterIp,
				configSecretUser2Alias, configSecretTestUser2, clusters[i])
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}

		ginkgo.By("Set Datastore level permission for testuser1 and testuser2")
		for i := 0; i < len(dataCenters); i++ {
			err = setDataStoreLevelPermission(vcAddress, vCenterPort, sshClientConfig, masterIp,
				configSecretUser1Alias, configSecretTestUser1, dataCenters[i], datastores)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			err = setDataStoreLevelPermission(vcAddress, vCenterPort, sshClientConfig, masterIp,
				configSecretUser2Alias, configSecretTestUser2, dataCenters[i], datastores)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}

		ginkgo.By("Set search level permission for testuser1 and testuser2")
		err = setSearchlevelPermission(vcAddress, vCenterPort, sshClientConfig, masterIp, configSecretUser1Alias,
			configSecretTestUser1)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		err = setSearchlevelPermission(vcAddress, vCenterPort, sshClientConfig, masterIp, configSecretUser2Alias,
			configSecretTestUser2)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		defer func() {
			ginkgo.By("Delete testUser1, its roles and permissions")
			err = deleteUsersRolesAndPermissions(vcAddress, vCenterPort, sshClientConfig, masterIp,
				configSecretTestUser1, configSecretUser1Alias, dataCenters, clusters, hosts, vms, datastores)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			ginkgo.By("Delete testUser2, its roles and permissions")
			err = deleteUsersRolesAndPermissions(vcAddress, vCenterPort, sshClientConfig, masterIp,
				configSecretTestUser2, configSecretUser2Alias, dataCenters, clusters, hosts, vms,
				datastores)
		}()

		ginkgo.By("Fetch vsphere-config-secret file details")
		secretConf = getConfigSecretFileValues(client, ctx)

		ginkgo.By("Delete previously created vsphere-config-secret file")
		err = deleteCsiVsphereSecret(sshClientConfig, masterIp)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Create vsphere-config-secret file with testuser1 credentials and " +
			"using defaut vcenter port")
		vCenterDefaultPort := "443"
		err = createCsiVsphereSecret(secretConf.Global.ClusterID,
			secretConf.Global.ClusterDistribution, secretConf.Global.VCenterHostname,
			secretConf.Global.InsecureFlag,
			configSecretUser1Alias, configSecretTestUser1Password, vCenterDefaultPort,
			secretConf.Global.Datacenters, sshClientConfig, masterIp)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Restart CSI driver")
		_, err = restartCSIDriver(ctx, client, namespace, csiReplicas)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Creating Storage Class")
		storageclass, err := createStorageClass(client, nil, nil, "", "", false, "")
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		defer func() {
			ginkgo.By("Delete Storage Class")
			err = client.StorageV1().StorageClasses().Delete(ctx, storageclass.Name, *metav1.NewDeleteOptions(0))
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}()

		ginkgo.By("Try to create a PVC verify that it is stuck in pending state")
		pvclaim1, err := createPVC(client, namespace, nil, "", storageclass, "")
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		ginkgo.By("Expect claim status to be in Pending state")
		err = fpv.WaitForPersistentVolumeClaimPhase(v1.ClaimPending, client,
			pvclaim1.Namespace, pvclaim1.Name, framework.Poll, time.Minute)
		gomega.Expect(err).NotTo(gomega.HaveOccurred(),
			fmt.Sprintf("Failed to find the volume in pending state with err: %v", err))
		defer func() {
			ginkgo.By("Deleting the PVC")
			err = fpv.DeletePersistentVolumeClaim(client, pvclaim1.Name, namespace)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}()

		ginkgo.By("Fetch vsphere-config-secret file details")
		secretConf = getConfigSecretFileValues(client, ctx)

		ginkgo.By("Delete previously created vsphere-config-secret file")
		err = deleteCsiVsphereSecret(sshClientConfig, masterIp)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Create vsphere-config-secret file with testuser1 credentials using " +
			"non default vcenter port")
		err = createCsiVsphereSecret(secretConf.Global.ClusterID,
			secretConf.Global.ClusterDistribution, secretConf.Global.VCenterHostname,
			secretConf.Global.InsecureFlag,
			configSecretUser1Alias, configSecretTestUser1Password, vCenterPort,
			secretConf.Global.Datacenters, sshClientConfig, masterIp)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		defer func() {
			ginkgo.By("Reverting back csi-vsphere.conf with its original vcenter user " +
				"and its credentials")
			secretConf = getConfigSecretFileValues(client, ctx)
			err = deleteCsiVsphereSecret(sshClientConfig, masterIp)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			err = createCsiVsphereSecret(secretConf.Global.ClusterID,
				secretConf.Global.ClusterDistribution, secretConf.Global.VCenterHostname,
				secretConf.Global.InsecureFlag,
				vCenterUIUser, vCenterUIPassword, vCenterPort,
				secretConf.Global.Datacenters, sshClientConfig, masterIp)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			_, err = restartCSIDriver(ctx, client, namespace, csiReplicas)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}()

		ginkgo.By("Restart CSI driver")
		_, err = restartCSIDriver(ctx, client, namespace, csiReplicas)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Verify we can create a PVC and attach it to pod")
		ginkgo.By("Creating Pvc")
		pvclaim2, err := createPVC(client, namespace, nil, "", storageclass, "")
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		// Waiting for PVC to be bound.
		var pvclaims2 []*v1.PersistentVolumeClaim
		pvclaims2 = append(pvclaims2, pvclaim2)
		ginkgo.By("Waiting for all claims to be in bound state")
		_, err = fpv.WaitForPVClaimBoundPhase(client, pvclaims2, framework.ClaimProvisionTimeout)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		defer func() {
			err = fpv.DeletePersistentVolumeClaim(client, pvclaim2.Name, namespace)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}()

		ginkgo.By("Creating pod")
		pod2, err := createPod(client, namespace, nil, []*v1.PersistentVolumeClaim{pvclaim2}, false, "")
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		defer func() {
			ginkgo.By(fmt.Sprintf("Deleting the pod %s in namespace %s", pod2.Name, namespace))
			err = fpod.DeletePodWithWait(client, pod2)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}()

		time.Sleep(5 * time.Minute)
		ginkgo.By("Verify the PVC which was stuck in Pending state should gets bound eventually")
		// Waiting for PVC to be bound.
		pvclaim1, err = client.CoreV1().PersistentVolumeClaims(namespace).Get(ctx, pvclaim1.Name, metav1.GetOptions{})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		var pvclaims1 []*v1.PersistentVolumeClaim
		pvclaims1 = append(pvclaims1, pvclaim1)
		ginkgo.By("Waiting for all claims to be in bound state")
		_, err = fpv.WaitForPVClaimBoundPhase(client, pvclaims1, framework.ClaimProvisionTimeout)
		gomega.Expect(pvclaim1.Status.Phase == v1.ClaimBound).To(gomega.BeTrue())

		ginkgo.By("Creating pod")
		pod3, err := createPod(client, namespace, nil, []*v1.PersistentVolumeClaim{pvclaim1}, false, "")
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		defer func() {
			ginkgo.By(fmt.Sprintf("Deleting the pod %s in namespace %s", pod3.Name, namespace))
			err = fpod.DeletePodWithWait(client, pod3)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}()
	})

	/* TESTCASE-13
	Modify VC Port and validate the workloads
	Steps:
		1.Create a user, user1, with required roles and privileges
		2. Create csi-vsphere.conf with user1's credentials and create vsphere-config-secret in turn
		using that file
		3. Install CSI driver
		4. Verify we can create a PVC and attach it to pod
		5. Change csi-vsphere.conf and add a dummy vc port and re-create vsphere-config-secret in turn
		using that file
		6. Try to create a PVC and verify that it is stuck in a pending state
		7. Change csi-vsphere.conf with correct VC port and re-create vsphere-config-secret, in turn,
		using that file
		8. Verify we can create a PVC and attach it to pod
		9. Verify PVC created in step 6 gets bound eventually
		10. Cleanup all objects created during the test
	*/

	ginkgo.It("[vc-custom-port] Modify VC Port and validate the workloads", func() {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		ginkgo.By("Create testuser1")
		err = createTestUser(vcAddress, vCenterPort, sshClientConfig, masterIp, configSecretTestUser1,
			configSecretTestUser1Password)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		defer func() {
			ginkgo.By("Delete testuser1")
			err = deleteTestUser(vcAddress, vCenterPort, sshClientConfig, masterIp, configSecretTestUser1)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}()
		ginkgo.By("Create testuser2")
		err = createTestUser(vcAddress, vCenterPort, sshClientConfig, masterIp, configSecretTestUser2,
			configSecretTestUser1Password)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		defer func() {
			ginkgo.By("Delete testuser2")
			err = deleteTestUser(vcAddress, vCenterPort, sshClientConfig, masterIp, configSecretTestUser2)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}()

		ginkgo.By("Create roles for testuser1")
		err = createRolesForTestUser(vcAddress, vCenterPort, sshClientConfig, masterIp, configSecretTestUser1)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Create roles for testuser2")
		err = createRolesForTestUser(vcAddress, vCenterPort, sshClientConfig, masterIp, configSecretTestUser2)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Assign required roles and privileges for testuser1 and testuser2")

		ginkgo.By("Set DataCenter level permission for testuser1 and testuser2")
		for i := 0; i < len(dataCenters); i++ {
			err = setDataCenterLevelPermission(vcAddress, vCenterPort, sshClientConfig, masterIp, dataCenters[i],
				configSecretUser1Alias)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			err = setDataCenterLevelPermission(vcAddress, vCenterPort, sshClientConfig, masterIp, dataCenters[i],
				configSecretUser2Alias)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}

		ginkgo.By("Set hosts level permission for testuser1 and testuser2")
		err = setHostLevelPermission(vcAddress, vCenterPort, sshClientConfig, masterIp, configSecretUser1Alias, hosts)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		err = setHostLevelPermission(vcAddress, vCenterPort, sshClientConfig, masterIp, configSecretUser2Alias, hosts)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Set k8s VM level permission for testuser1 and testuser2")
		err = setVMLevelPermission(vcAddress, vCenterPort, sshClientConfig, masterIp, configSecretUser1Alias,
			configSecretTestUser1, vms)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		err = setVMLevelPermission(vcAddress, vCenterPort, sshClientConfig, masterIp, configSecretUser2Alias,
			configSecretTestUser2, vms)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Set cluster level permission for testuser1 and testuser2")
		for i := 0; i < len(clusters); i++ {
			err = setClusterLevelPermission(vcAddress, vCenterPort, sshClientConfig, masterIp,
				configSecretUser1Alias, configSecretTestUser1, clusters[i])
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			err = setClusterLevelPermission(vcAddress, vCenterPort, sshClientConfig, masterIp,
				configSecretUser2Alias, configSecretTestUser2, clusters[i])
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}

		ginkgo.By("Set Datastore level permission for testuser1 and testuser2")
		for i := 0; i < len(dataCenters); i++ {
			err = setDataStoreLevelPermission(vcAddress, vCenterPort, sshClientConfig, masterIp,
				configSecretUser1Alias, configSecretTestUser1, dataCenters[i], datastores)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			err = setDataStoreLevelPermission(vcAddress, vCenterPort, sshClientConfig, masterIp,
				configSecretUser2Alias, configSecretTestUser2, dataCenters[i], datastores)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}

		ginkgo.By("Set search level permission for testuser1 and testuser2")
		err = setSearchlevelPermission(vcAddress, vCenterPort, sshClientConfig, masterIp, configSecretUser1Alias,
			configSecretTestUser1)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		err = setSearchlevelPermission(vcAddress, vCenterPort, sshClientConfig, masterIp, configSecretUser2Alias,
			configSecretTestUser2)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		defer func() {
			ginkgo.By("Delete testUser1, its roles and permissions")
			err = deleteUsersRolesAndPermissions(vcAddress, vCenterPort, sshClientConfig, masterIp,
				configSecretTestUser1, configSecretUser1Alias, dataCenters, clusters, hosts, vms, datastores)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			ginkgo.By("Delete testUser2, its roles and permissions")
			err = deleteUsersRolesAndPermissions(vcAddress, vCenterPort, sshClientConfig, masterIp,
				configSecretTestUser2, configSecretUser2Alias, dataCenters, clusters, hosts, vms, datastores)
		}()

		ginkgo.By("Fetch vsphere-config-secret file details")
		secretConf = getConfigSecretFileValues(client, ctx)

		ginkgo.By("Delete previously created vsphere-config-secret file")
		err = deleteCsiVsphereSecret(sshClientConfig, masterIp)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Create vsphere-config-secret file with testuser1 credentials")
		err = createCsiVsphereSecret(secretConf.Global.ClusterID,
			secretConf.Global.ClusterDistribution, secretConf.Global.VCenterHostname,
			secretConf.Global.InsecureFlag,
			configSecretUser1Alias, configSecretTestUser1Password, vCenterPort,
			secretConf.Global.Datacenters, sshClientConfig, masterIp)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Restart CSI driver")
		_, err = restartCSIDriver(ctx, client, namespace, csiReplicas)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Verify we can create a PVC and attach it to pod")
		ginkgo.By("Creating Storage Class")
		storageclass, err := createStorageClass(client, nil, nil, "", "", false, "")
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		defer func() {
			ginkgo.By("Delete Storage Class")
			err = client.StorageV1().StorageClasses().Delete(ctx, storageclass.Name, *metav1.NewDeleteOptions(0))
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}()

		ginkgo.By("Creating Pvc")
		pvclaim1, err := createPVC(client, namespace, nil, "", storageclass, "")
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		// Waiting for PVC to be bound.
		var pvclaims1 []*v1.PersistentVolumeClaim
		pvclaims1 = append(pvclaims1, pvclaim1)
		ginkgo.By("Waiting for all claims to be in bound state")
		_, err = fpv.WaitForPVClaimBoundPhase(client, pvclaims1, framework.ClaimProvisionTimeout)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		defer func() {
			err = fpv.DeletePersistentVolumeClaim(client, pvclaim1.Name, namespace)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}()

		ginkgo.By("Creating pod")
		pod1, err := createPod(client, namespace, nil, []*v1.PersistentVolumeClaim{pvclaim1}, false, "")
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		defer func() {
			ginkgo.By(fmt.Sprintf("Deleting the pod %s in namespace %s", pod1.Name, namespace))
			err = fpod.DeletePodWithWait(client, pod1)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}()

		ginkgo.By("Fetch vsphere-config-secret file details")
		secretConf = getConfigSecretFileValues(client, ctx)

		ginkgo.By("Delete previously created vsphere-config-secret file")
		err = deleteCsiVsphereSecret(sshClientConfig, masterIp)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Create vsphere-config-secret file with testuser1 credentials and using dummy vcenter port")
		dummyvCenterPort := "4444"
		err = createCsiVsphereSecret(secretConf.Global.ClusterID,
			secretConf.Global.ClusterDistribution, secretConf.Global.VCenterHostname,
			secretConf.Global.InsecureFlag,
			configSecretUser1Alias, configSecretTestUser1Password, dummyvCenterPort,
			secretConf.Global.Datacenters, sshClientConfig, masterIp)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Restart CSI driver")
		_, err = restartCSIDriver(ctx, client, namespace, csiReplicas)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Try to create a PVC verify that it is stuck in pending state")
		pvclaim2, err := createPVC(client, namespace, nil, "", storageclass, "")
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		ginkgo.By("Expect claim status to be in Pending state")
		err = fpv.WaitForPersistentVolumeClaimPhase(v1.ClaimPending, client,
			pvclaim2.Namespace, pvclaim2.Name, framework.Poll, time.Minute)
		gomega.Expect(err).NotTo(gomega.HaveOccurred(),
			fmt.Sprintf("Failed to find the volume in pending state with err: %v", err))
		defer func() {
			ginkgo.By("Deleting the PVC")
			err = fpv.DeletePersistentVolumeClaim(client, pvclaim2.Name, namespace)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}()

		ginkgo.By("Fetch vsphere-config-secret file details")
		secretConf = getConfigSecretFileValues(client, ctx)

		ginkgo.By("Delete previously created vsphere-config-secret file")
		err = deleteCsiVsphereSecret(sshClientConfig, masterIp)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Create vsphere-config-secret file with testuser1 credentials and giving correct vcenter port")
		err = createCsiVsphereSecret(secretConf.Global.ClusterID,
			secretConf.Global.ClusterDistribution, secretConf.Global.VCenterHostname,
			secretConf.Global.InsecureFlag,
			configSecretUser1Alias, configSecretTestUser1Password, vCenterPort,
			secretConf.Global.Datacenters, sshClientConfig, masterIp)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		defer func() {
			ginkgo.By("Reverting back csi-vsphere.conf with its original vcenter user " +
				"and its credentials")
			secretConf = getConfigSecretFileValues(client, ctx)
			err = deleteCsiVsphereSecret(sshClientConfig, masterIp)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			err = createCsiVsphereSecret(secretConf.Global.ClusterID,
				secretConf.Global.ClusterDistribution, secretConf.Global.VCenterHostname,
				secretConf.Global.InsecureFlag,
				vCenterUIUser, vCenterUIPassword, vCenterPort,
				secretConf.Global.Datacenters, sshClientConfig, masterIp)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			_, err = restartCSIDriver(ctx, client, namespace, csiReplicas)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}()

		ginkgo.By("Restart CSI driver")
		_, err = restartCSIDriver(ctx, client, namespace, csiReplicas)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Verify we can create a PVC and attach it to pod")
		ginkgo.By("Creating Pvc")
		pvclaim3, err := createPVC(client, namespace, nil, "", storageclass, "")
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		// Waiting for PVC to be bound.
		var pvclaims3 []*v1.PersistentVolumeClaim
		pvclaims3 = append(pvclaims3, pvclaim3)
		ginkgo.By("Waiting for all claims to be in bound state")
		_, err = fpv.WaitForPVClaimBoundPhase(client, pvclaims3, framework.ClaimProvisionTimeout)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		defer func() {
			err = fpv.DeletePersistentVolumeClaim(client, pvclaim3.Name, namespace)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}()

		ginkgo.By("Creating pod")
		pod2, err := createPod(client, namespace, nil, []*v1.PersistentVolumeClaim{pvclaim3}, false, "")
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		defer func() {
			ginkgo.By(fmt.Sprintf("Deleting the pod %s in namespace %s", pod2.Name, namespace))
			err = fpod.DeletePodWithWait(client, pod2)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}()

		time.Sleep(5 * time.Minute)
		ginkgo.By("Verify the PVC which was stuck in Pending state should gets bound eventually")
		pvclaim2, err = client.CoreV1().PersistentVolumeClaims(namespace).Get(ctx, pvclaim2.Name, metav1.GetOptions{})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		gomega.Expect(pvclaim2.Status.Phase == v1.ClaimBound).To(gomega.BeTrue())
	})
})
