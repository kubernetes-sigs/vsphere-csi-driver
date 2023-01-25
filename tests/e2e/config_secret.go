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
	"time"

	ginkgo "github.com/onsi/ginkgo/v2"
	"github.com/onsi/gomega"
	"github.com/vmware/govmomi/object"
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
	f := framework.NewDefaultFramework("config-secret-changes")
	f.NamespacePodSecurityEnforceLevel = admissionapi.LevelPrivileged
	var (
		client                    clientset.Interface
		namespace                 string
		allMasterIps              []string
		masterIp                  string
		dataCenters               []*object.Datacenter
		clusters                  []string
		hosts                     []string
		vms                       []string
		datastores                []string
		configSecretUser1Alias    string
		configSecretUser2Alias    string
		csiNamespace              string
		csiReplicas               int32
		vCenterUIUser             string
		vCenterUIPassword         string
		propagateVal              string
		revertOriginalvCenterUser bool
		vCenterIP                 string
		vCenterPort               string
		dataCenter                string
		sshClientConfig           *ssh.ClientConfig
		nimbusGeneratedK8sVmPwd   string
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

		// fetching required parameters
		allMasterIps = getK8sMasterIPs(ctx, client)
		masterIp = allMasterIps[0]
		vCenterUIUser = e2eVSphere.Config.Global.User
		vCenterUIPassword = e2eVSphere.Config.Global.Password
		vCenterIP = e2eVSphere.Config.Global.VCenterHostname
		vCenterPort = e2eVSphere.Config.Global.VCenterPort
		dataCenter = e2eVSphere.Config.Global.Datacenters
		propagateVal = "false"
		revertOriginalvCenterUser = false
		configSecretUser1Alias = configSecretTestUser1 + "@vsphere.local"
		configSecretUser2Alias = configSecretTestUser2 + "@vsphere.local"
		nimbusGeneratedK8sVmPwd = GetAndExpectStringEnvVar(nimbusK8sVmPwd)

		sshClientConfig = &ssh.ClientConfig{
			User: "root",
			Auth: []ssh.AuthMethod{
				ssh.Password(nimbusGeneratedK8sVmPwd),
			},
			HostKeyCallback: ssh.InsecureIgnoreHostKey(),
		}

		// fetching datacenter, cluster, host details
		dataCenters, clusters, hosts, vms, datastores = getDataCenterClusterHostAndVmDetails(ctx, masterIp, sshClientConfig)

		ginkgo.By("Delete roles, permissions for testuser1 if already exist")
		deleteTestUserAndRemoveRolesPrivileges(masterIp, sshClientConfig, configSecretTestUser1,
			configSecretTestUser1Password, configSecretUser1Alias, propagateVal,
			dataCenters, clusters, hosts, vms, datastores)

		ginkgo.By("Delete roles, permissions for testuser2 if already exist")
		deleteTestUserAndRemoveRolesPrivileges(masterIp, sshClientConfig, configSecretTestUser2,
			configSecretTestUser1Password, configSecretUser2Alias, propagateVal,
			dataCenters, clusters, hosts, vms, datastores)

		csiNamespace = GetAndExpectStringEnvVar(envCSINamespace)
		csiDeployment, err := client.AppsV1().Deployments(csiNamespace).Get(
			ctx, vSphereCSIControllerPodNamePrefix, metav1.GetOptions{})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		csiReplicas = *csiDeployment.Spec.Replicas
	})

	ginkgo.AfterEach(func() {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		if !revertOriginalvCenterUser {
			ginkgo.By("Reverting back csi-vsphere.conf with its original vcenter user " +
				"and its credentials")
			createCsiVsphereSecret(client, ctx, vCenterUIUser, vCenterUIPassword, csiNamespace, vCenterIP,
				vCenterPort, dataCenter, "")

			ginkgo.By("Restart CSI driver")
			restartSuccess, err := restartCSIDriver(ctx, client, namespace, csiReplicas)
			gomega.Expect(restartSuccess).To(gomega.BeTrue(), "csi driver restart not successful")
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}
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

	ginkgo.It("[csi-config-secret-block][csi-config-secret-file] Update user credentials in vsphere config "+
		"secret keeping password same for both test users", func() {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		ginkgo.By("Create testuser1 and assign required roles and privileges to testuser1")
		createTestUserAndAssignRolesPrivileges(masterIp, sshClientConfig, configSecretTestUser1,
			configSecretTestUser1Password, configSecretUser1Alias, propagateVal,
			dataCenters, clusters, hosts, vms, datastores, "createUser", "createRoles")
		defer func() {
			ginkgo.By("Delete testuser1 and remove roles and privileges assigned to testuser1")
			deleteTestUserAndRemoveRolesPrivileges(masterIp, sshClientConfig, configSecretTestUser1,
				configSecretTestUser1Password, configSecretUser1Alias, propagateVal,
				dataCenters, clusters, hosts, vms, datastores)
		}()

		ginkgo.By("Create testuser2 and assign required roles and privileges to testuser2")
		createTestUserAndAssignRolesPrivileges(masterIp, sshClientConfig, configSecretTestUser2,
			configSecretTestUser1Password, configSecretUser2Alias, propagateVal,
			dataCenters, clusters, hosts, vms, datastores, "createUser", "createRoles")
		defer func() {
			ginkgo.By("Delete testuser2 and remove roles and privileges assigned to testuser2")
			deleteTestUserAndRemoveRolesPrivileges(masterIp, sshClientConfig, configSecretTestUser2,
				configSecretTestUser1Password, configSecretUser2Alias, propagateVal,
				dataCenters, clusters, hosts, vms, datastores)
		}()

		ginkgo.By("Create vsphere-config-secret file with testuser1 credentials")
		createCsiVsphereSecret(client, ctx, configSecretUser1Alias, configSecretTestUser1Password,
			csiNamespace, vCenterIP, vCenterPort, dataCenter, "")

		ginkgo.By("Restart CSI driver")
		restartSuccess, err := restartCSIDriver(ctx, client, namespace, csiReplicas)
		gomega.Expect(restartSuccess).To(gomega.BeTrue(), "csi driver restart not successful")
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Creating Storage Class")
		storageclass, err := createStorageClass(client, nil, nil, "", "", false, "")
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		defer func() {
			ginkgo.By("Delete Storage Class")
			err = client.StorageV1().StorageClasses().Delete(ctx, storageclass.Name, *metav1.NewDeleteOptions(0))
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}()

		ginkgo.By("Verify we can create a PVC and attach it to pod")
		pod1, pvclaim1, pv1 := verifyPvcPodCreationAfterConfigSecretChange(client, namespace, storageclass)
		defer func() {
			performCleanUpOfPvcPod(client, namespace, pod1, pvclaim1, pv1)
		}()

		ginkgo.By("Create vsphere-config-secret file with testuser2 credentials")
		createCsiVsphereSecret(client, ctx, configSecretUser2Alias, configSecretTestUser1Password, csiNamespace,
			vCenterIP, vCenterPort, dataCenter, "")
		defer func() {
			ginkgo.By("Reverting back csi-vsphere.conf with its original vcenter user " +
				"and its credentials")
			createCsiVsphereSecret(client, ctx, vCenterUIUser, vCenterUIPassword, csiNamespace, vCenterIP,
				vCenterPort, dataCenter, "")
			revertOriginalvCenterUser = true

			ginkgo.By("Restart CSI driver")
			restartSuccess, err = restartCSIDriver(ctx, client, namespace, csiReplicas)
			gomega.Expect(restartSuccess).To(gomega.BeTrue(), "csi driver restart not successful")
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}()

		ginkgo.By("Restart CSI driver")
		restartSuccess, err = restartCSIDriver(ctx, client, namespace, csiReplicas)
		gomega.Expect(restartSuccess).To(gomega.BeTrue(), "csi driver restart not successful")
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Verify we can create a PVC and attach it to pod")
		pod2, pvclaim2, pv2 := verifyPvcPodCreationAfterConfigSecretChange(client, namespace, storageclass)
		defer func() {
			performCleanUpOfPvcPod(client, namespace, pod2, pvclaim2, pv2)
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

	ginkgo.It("[csi-config-secret-block][csi-config-secret-file] Change vcenter user password "+
		"and restart csi controller pod", func() {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		testUser1NewPassword := "Admin!123"
		ignoreLabels := make(map[string]string)

		ginkgo.By("Create testuser1 and assign required roles and privileges to testuser1")
		createTestUserAndAssignRolesPrivileges(masterIp, sshClientConfig, configSecretTestUser1,
			configSecretTestUser1Password, configSecretUser1Alias, propagateVal,
			dataCenters, clusters, hosts, vms, datastores, "createUser", "createRoles")
		defer func() {
			ginkgo.By("Delete testuser1 and remove roles and privileges assigned to testuser1")
			deleteTestUserAndRemoveRolesPrivileges(masterIp, sshClientConfig, configSecretTestUser1,
				testUser1NewPassword, configSecretUser1Alias, propagateVal,
				dataCenters, clusters, hosts, vms, datastores)
		}()

		ginkgo.By("Create testuser2 and assign required roles and privileges to testuser2")
		createTestUserAndAssignRolesPrivileges(masterIp, sshClientConfig, configSecretTestUser2,
			configSecretTestUser2Password, configSecretUser2Alias, propagateVal,
			dataCenters, clusters, hosts, vms, datastores, "createUser", "createRoles")
		defer func() {
			ginkgo.By("Delete testuser2 and remove roles and privileges assigned to testuser2")
			deleteTestUserAndRemoveRolesPrivileges(masterIp, sshClientConfig, configSecretTestUser2,
				configSecretTestUser2Password, configSecretUser2Alias, propagateVal,
				dataCenters, clusters, hosts, vms, datastores)
		}()

		ginkgo.By("Update vsphere-config-secret with testuser1 credentials")
		createCsiVsphereSecret(client, ctx, configSecretUser1Alias, configSecretTestUser1Password, csiNamespace,
			vCenterIP, vCenterPort, dataCenter, "")

		ginkgo.By("Restart CSI driver")
		restartSuccess, err := restartCSIDriver(ctx, client, namespace, csiReplicas)
		gomega.Expect(restartSuccess).To(gomega.BeTrue(), "csi driver restart not successful")
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Creating Storage Class")
		storageclass, err := createStorageClass(client, nil, nil, "", "", false, "")
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		defer func() {
			ginkgo.By("Delete Storage Class")
			err = client.StorageV1().StorageClasses().Delete(ctx, storageclass.Name, *metav1.NewDeleteOptions(0))
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}()

		ginkgo.By("Verify we can create a PVC and attach it to pod")
		pod1, pvclaim1, pv1 := verifyPvcPodCreationAfterConfigSecretChange(client, namespace, storageclass)
		defer func() {
			performCleanUpOfPvcPod(client, namespace, pod1, pvclaim1, pv1)
		}()

		ginkgo.By("Change password for testuser1")
		err = changeTestUserPassword(masterIp, sshClientConfig, configSecretTestUser1, testUser1NewPassword)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Try to create a PVC and verify it gets bound successfully")
		pvclaim2, err := createPVC(client, namespace, nil, "", storageclass, "")
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
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
		err = updateDeploymentReplicawithWait(client, 0, vSphereCSIControllerPodNamePrefix, csiNamespace)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		err = updateDeploymentReplicawithWait(client, csiReplicas, vSphereCSIControllerPodNamePrefix, csiNamespace)
		if err != nil {
			if strings.Contains(err.Error(), "error waiting for deployment") {
				framework.Logf("csi pods are not in ready state")
			} else {
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
			}
		}

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

		ginkgo.By("Update vsphere-config-secret file with testuser1 updated credentials")
		createCsiVsphereSecret(client, ctx, configSecretUser1Alias, testUser1NewPassword, csiNamespace,
			vCenterIP, vCenterPort, dataCenter, "")
		defer func() {
			ginkgo.By("Reverting back csi-vsphere.conf with its original vcenter user " +
				"and its credentials")
			createCsiVsphereSecret(client, ctx, vCenterUIUser, vCenterUIPassword, csiNamespace,
				vCenterIP, vCenterPort, dataCenter, "")
			revertOriginalvCenterUser = true

			ginkgo.By("Restart CSI driver")
			restartSuccess, err = restartCSIDriver(ctx, client, namespace, csiReplicas)
			gomega.Expect(restartSuccess).To(gomega.BeTrue(), "csi driver restart not successful")
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}()

		ginkgo.By("Restart CSI driver")
		restartSuccess, err = restartCSIDriver(ctx, client, namespace, csiReplicas)
		gomega.Expect(restartSuccess).To(gomega.BeTrue(), "csi driver restart not successful")
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Wait for csi controller pods to be in running state")
		list_of_pods, err := fpod.GetPodsInNamespace(client, csiNamespace, ignoreLabels)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		for i := 0; i < len(list_of_pods); i++ {
			err = fpod.WaitTimeoutForPodRunningInNamespace(client, list_of_pods[i].Name, csiNamespace, pollTimeout)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}

		ginkgo.By("Verify we can create a PVC and attach it to pod")
		pod4, pvclaim4, pv4 := verifyPvcPodCreationAfterConfigSecretChange(client, namespace, storageclass)
		defer func() {
			performCleanUpOfPvcPod(client, namespace, pod4, pvclaim4, pv4)
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

	ginkgo.It("[csi-config-secret-block][csi-config-secret-file] Update user credentials in vsphere config "+
		"secret keeping password different for both test users", func() {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		ginkgo.By("Create testuser1 and assign required roles and privileges to testuser1")
		createTestUserAndAssignRolesPrivileges(masterIp, sshClientConfig, configSecretTestUser1,
			configSecretTestUser1Password, configSecretUser1Alias, propagateVal,
			dataCenters, clusters, hosts, vms, datastores, "createUser", "createRoles")
		defer func() {
			ginkgo.By("Delete testuser1 and remove roles and privileges assigned to testuser1")
			deleteTestUserAndRemoveRolesPrivileges(masterIp, sshClientConfig, configSecretTestUser1,
				configSecretTestUser1Password, configSecretUser1Alias, propagateVal,
				dataCenters, clusters, hosts, vms, datastores)
		}()

		ginkgo.By("Create testuser2 and assign required roles and privileges to testuser2")
		createTestUserAndAssignRolesPrivileges(masterIp, sshClientConfig, configSecretTestUser2,
			configSecretTestUser2Password, configSecretUser2Alias, propagateVal,
			dataCenters, clusters, hosts, vms, datastores, "createUser", "createRoles")
		defer func() {
			ginkgo.By("Delete testuser2 and remove roles and privileges assigned to testuser2")
			deleteTestUserAndRemoveRolesPrivileges(masterIp, sshClientConfig, configSecretTestUser2,
				configSecretTestUser2Password, configSecretUser2Alias, propagateVal,
				dataCenters, clusters, hosts, vms, datastores)
		}()

		ginkgo.By("Update vsphere-config-secret with testuser1 credentials")
		createCsiVsphereSecret(client, ctx, configSecretUser1Alias, configSecretTestUser1Password, csiNamespace,
			vCenterIP, vCenterPort, dataCenter, "")

		ginkgo.By("Restart CSI driver")
		restartSuccess, err := restartCSIDriver(ctx, client, namespace, csiReplicas)
		gomega.Expect(restartSuccess).To(gomega.BeTrue(), "csi driver restart not successful")
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Creating Storage Class")
		storageclass, err := createStorageClass(client, nil, nil, "", "", false, "")
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		defer func() {
			ginkgo.By("Delete Storage Class")
			err = client.StorageV1().StorageClasses().Delete(ctx, storageclass.Name, *metav1.NewDeleteOptions(0))
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}()

		ginkgo.By("Verify we can create a PVC and attach it to pod")
		pod1, pvclaim1, pv1 := verifyPvcPodCreationAfterConfigSecretChange(client, namespace, storageclass)
		defer func() {
			performCleanUpOfPvcPod(client, namespace, pod1, pvclaim1, pv1)
		}()

		ginkgo.By("Update vsphere-config-secret with testuser2 credentials")
		createCsiVsphereSecret(client, ctx, configSecretUser2Alias, configSecretTestUser2Password, csiNamespace,
			vCenterIP, vCenterPort, dataCenter, "")
		defer func() {
			ginkgo.By("Reverting back csi-vsphere.conf with its original vcenter user " +
				"and its credentials")
			createCsiVsphereSecret(client, ctx, vCenterUIUser, vCenterUIPassword, csiNamespace,
				vCenterIP, vCenterPort, dataCenter, "")
			revertOriginalvCenterUser = true

			ginkgo.By("Restart CSI driver")
			restartSuccess, err = restartCSIDriver(ctx, client, namespace, csiReplicas)
			gomega.Expect(restartSuccess).To(gomega.BeTrue(), "csi driver restart not successful")
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}()

		ginkgo.By("Restart CSI driver")
		restartSuccess, err = restartCSIDriver(ctx, client, namespace, csiReplicas)
		gomega.Expect(restartSuccess).To(gomega.BeTrue(), "csi driver restart not successful")
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Verify we can create a PVC and attach it to pod")
		pod2, pvclaim2, pv2 := verifyPvcPodCreationAfterConfigSecretChange(client, namespace, storageclass)
		defer func() {
			performCleanUpOfPvcPod(client, namespace, pod2, pvclaim2, pv2)
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
		"viceversa in vsphere config secret", func() {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		ginkgo.By("Create testuser1 and assign required roles and privileges to testuser1")
		createTestUserAndAssignRolesPrivileges(masterIp, sshClientConfig, configSecretTestUser1,
			configSecretTestUser1Password, configSecretUser1Alias, propagateVal,
			dataCenters, clusters, hosts, vms, datastores, "createUser", "createRoles")
		defer func() {
			ginkgo.By("Delete testuser1 and remove roles and privileges assigned to testuser1")
			deleteTestUserAndRemoveRolesPrivileges(masterIp, sshClientConfig, configSecretTestUser1,
				configSecretTestUser1Password, configSecretUser1Alias, propagateVal,
				dataCenters, clusters, hosts, vms, datastores)
		}()

		ginkgo.By("Create testuser2 and assign required roles and privileges to testuser2")
		createTestUserAndAssignRolesPrivileges(masterIp, sshClientConfig, configSecretTestUser2,
			configSecretTestUser2Password, configSecretUser2Alias, propagateVal,
			dataCenters, clusters, hosts, vms, datastores, "createUser", "createRoles")
		defer func() {
			ginkgo.By("Delete testuser2 and remove roles and privileges assigned to testuser2")
			deleteTestUserAndRemoveRolesPrivileges(masterIp, sshClientConfig, configSecretTestUser2,
				configSecretTestUser2Password, configSecretUser2Alias, propagateVal,
				dataCenters, clusters, hosts, vms, datastores)
		}()

		ginkgo.By("Update vsphere-config-secret with testuser1 credentials using vcenter IP")
		createCsiVsphereSecret(client, ctx, configSecretUser1Alias, configSecretTestUser1Password, csiNamespace,
			vCenterIP, vCenterPort, dataCenter, "")

		ginkgo.By("Restart CSI driver")
		restartSuccess, err := restartCSIDriver(ctx, client, namespace, csiReplicas)
		gomega.Expect(restartSuccess).To(gomega.BeTrue(), "csi driver restart not successful")
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

		ginkgo.By("Verify we can create a PVC and attach it to pod")
		pod1, pvclaim1, pv1 := verifyPvcPodCreationAfterConfigSecretChange(client, namespace, storageclass)
		defer func() {
			performCleanUpOfPvcPod(client, namespace, pod1, pvclaim1, pv1)
		}()

		ginkgo.By("Fetch vcenter hotsname")
		vCenterHostName := getVcenterHostName(vCenterIP)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Update vsphere-config-secret to use vcenter hostname")
		createCsiVsphereSecret(client, ctx, configSecretUser1Alias, configSecretTestUser1Password, csiNamespace,
			vCenterHostName, vCenterPort, dataCenter, "")

		ginkgo.By("Restart CSI driver")
		restartSuccess, err = restartCSIDriver(ctx, client, namespace, csiReplicas)
		gomega.Expect(restartSuccess).To(gomega.BeTrue(), "csi driver restart not successful")
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Verify we can create a PVC and attach it to pod")
		pod2, pvclaim2, pv2 := verifyPvcPodCreationAfterConfigSecretChange(client, namespace, storageclass)
		defer func() {
			performCleanUpOfPvcPod(client, namespace, pod2, pvclaim2, pv2)
		}()

		ginkgo.By("Update vsphere-config-secret to use vcenter IP")
		createCsiVsphereSecret(client, ctx, configSecretUser1Alias, configSecretTestUser1Password, csiNamespace,
			vCenterIP, vCenterPort, dataCenter, "")
		defer func() {
			ginkgo.By("Reverting back csi-vsphere.conf with its original vcenter user " +
				"and its credentials")
			createCsiVsphereSecret(client, ctx, vCenterUIUser, vCenterUIPassword, csiNamespace, vCenterIP, vCenterPort,
				dataCenter, "")
			revertOriginalvCenterUser = true

			ginkgo.By("Restart CSI driver")
			restartSuccess, err = restartCSIDriver(ctx, client, namespace, csiReplicas)
			gomega.Expect(restartSuccess).To(gomega.BeTrue(), "csi driver restart not successful")
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}()

		ginkgo.By("Restart CSI driver")
		restartSuccess, err = restartCSIDriver(ctx, client, namespace, csiReplicas)
		gomega.Expect(restartSuccess).To(gomega.BeTrue(), "csi driver restart not successful")
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Verify we can create a PVC and attach it to pod")
		pod3, pvclaim3, pv3 := verifyPvcPodCreationAfterConfigSecretChange(client, namespace, storageclass)
		defer func() {
			performCleanUpOfPvcPod(client, namespace, pod3, pvclaim3, pv3)
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
		7. Change csi-vsphere.conf with updated user1's credentials and re-create vsphere-config-secret
		in turn using that file
		8. Verify we can create a PVC and attach it to pod
		9. Cleanup all objects created during the test
	*/

	ginkgo.It("[csi-config-secret-block][csi-config-secret-file] Change vcenter user to wrong dummy "+
		"user and later switch back to correct one", func() {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		dummyTestUser := "dummyUser@vsphere.local"

		ginkgo.By("Create testuser1 and assign required roles and privileges to testuser1")
		createTestUserAndAssignRolesPrivileges(masterIp, sshClientConfig, configSecretTestUser1,
			configSecretTestUser1Password, configSecretUser1Alias, propagateVal,
			dataCenters, clusters, hosts, vms, datastores, "createUser", "createRoles")
		defer func() {
			ginkgo.By("Delete testuser1 and remove roles and privileges assigned to testuser1")
			deleteTestUserAndRemoveRolesPrivileges(masterIp, sshClientConfig, configSecretTestUser1,
				configSecretTestUser1Password, configSecretUser1Alias, propagateVal,
				dataCenters, clusters, hosts, vms, datastores)
		}()

		ginkgo.By("Create testuser2 and assign required roles and privileges to testuser2")
		createTestUserAndAssignRolesPrivileges(masterIp, sshClientConfig, configSecretTestUser2,
			configSecretTestUser1Password, configSecretUser2Alias, propagateVal,
			dataCenters, clusters, hosts, vms, datastores, "createUser", "createRoles")
		defer func() {
			ginkgo.By("Delete testuser2 and remove roles and privileges assigned to testuser2")
			deleteTestUserAndRemoveRolesPrivileges(masterIp, sshClientConfig, configSecretTestUser2,
				configSecretTestUser1Password, configSecretUser2Alias, propagateVal,
				dataCenters, clusters, hosts, vms, datastores)
		}()

		ginkgo.By("Update vsphere-config-secret with testuser1 credentials")
		createCsiVsphereSecret(client, ctx, configSecretUser1Alias, configSecretTestUser1Password, csiNamespace,
			vCenterIP, vCenterPort, dataCenter, "")

		ginkgo.By("Restart CSI driver")
		restartSuccess, err := restartCSIDriver(ctx, client, namespace, csiReplicas)
		gomega.Expect(restartSuccess).To(gomega.BeTrue(), "csi driver restart not successful")
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Creating Storage Class")
		storageclass, err := createStorageClass(client, nil, nil, "", "", false, "")
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		defer func() {
			ginkgo.By("Delete Storage Class")
			err = client.StorageV1().StorageClasses().Delete(ctx, storageclass.Name, *metav1.NewDeleteOptions(0))
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}()

		ginkgo.By("Verify we can create a PVC and attach it to pod")
		pod1, pvclaim1, pv1 := verifyPvcPodCreationAfterConfigSecretChange(client, namespace, storageclass)
		defer func() {
			performCleanUpOfPvcPod(client, namespace, pod1, pvclaim1, pv1)
		}()

		ginkgo.By("Update vsphere-config-secret with dummy user credentials")
		createCsiVsphereSecret(client, ctx, dummyTestUser, configSecretTestUser1Password, csiNamespace,
			vCenterIP, vCenterPort, dataCenter, "")

		ginkgo.By("Restart CSI driver")
		err = updateDeploymentReplicawithWait(client, 0, vSphereCSIControllerPodNamePrefix, csiNamespace)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		err = updateDeploymentReplicawithWait(client, csiReplicas, vSphereCSIControllerPodNamePrefix, csiNamespace)
		if err != nil {
			if strings.Contains(err.Error(), "error waiting for deployment") {
				framework.Logf("csi pods are not in ready state")
			} else {
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
			}
		}

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

		ginkgo.By("Update vsphere-config-secret with testuser1 credentials")
		createCsiVsphereSecret(client, ctx, configSecretUser1Alias, configSecretTestUser1Password, csiNamespace,
			vCenterIP, vCenterPort, dataCenter, "")
		defer func() {
			ginkgo.By("Reverting back csi-vsphere.conf with its original vcenter user " +
				"and its credentials")
			createCsiVsphereSecret(client, ctx, vCenterUIUser, vCenterUIPassword, csiNamespace,
				vCenterIP, vCenterPort, dataCenter, "")
			revertOriginalvCenterUser = true

			ginkgo.By("Restart CSI driver")
			restartSuccess, err = restartCSIDriver(ctx, client, namespace, csiReplicas)
			gomega.Expect(restartSuccess).To(gomega.BeTrue(), "csi driver restart not successful")
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}()

		ginkgo.By("Restart CSI driver")
		restartSuccess, err = restartCSIDriver(ctx, client, namespace, csiReplicas)
		gomega.Expect(restartSuccess).To(gomega.BeTrue(), "csi driver restart not successful")
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Verify we can create a PVC and attach it to pod")
		pod3, pvclaim3, pv3 := verifyPvcPodCreationAfterConfigSecretChange(client, namespace, storageclass)
		defer func() {
			performCleanUpOfPvcPod(client, namespace, pod3, pvclaim3, pv3)
		}()
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
		"roles and privileges and switch back to the correct one", func() {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		ginkgo.By("Create testuser1 and assign required roles and privileges to testuser1")
		createTestUserAndAssignRolesPrivileges(masterIp, sshClientConfig, configSecretTestUser1,
			configSecretTestUser1Password, configSecretUser1Alias, propagateVal,
			dataCenters, clusters, hosts, vms, datastores, "createUser", "createRoles")
		defer func() {
			ginkgo.By("Delete testuser1 and remove roles and privileges assigned to testuser1")
			deleteTestUserAndRemoveRolesPrivileges(masterIp, sshClientConfig, configSecretTestUser1,
				configSecretTestUser1Password, configSecretUser1Alias, propagateVal,
				dataCenters, clusters, hosts, vms, datastores)
		}()

		ginkgo.By("Create testuser2 and assign limited roles and privileges to testuser2")
		createTestUserAndAssignLimitedRolesAndPrivileges(masterIp, sshClientConfig, configSecretTestUser2,
			configSecretTestUser1Password, configSecretUser2Alias, propagateVal,
			dataCenters, clusters, hosts, vms, datastores)
		defer func() {
			ginkgo.By("Delete testuser2 and remove roles and privileges assigned to testuser2")
			deleteTestUserAndRemoveRolesPrivileges(masterIp, sshClientConfig, configSecretTestUser2,
				configSecretTestUser1Password, configSecretUser2Alias, propagateVal,
				dataCenters, clusters, hosts, vms, datastores)
		}()

		ginkgo.By("Update vsphere-config-secret with testuser1 credentials")
		createCsiVsphereSecret(client, ctx, configSecretUser1Alias, configSecretTestUser1Password, csiNamespace,
			vCenterIP, vCenterPort, dataCenter, "")

		ginkgo.By("Restart CSI driver")
		restartSuccess, err := restartCSIDriver(ctx, client, namespace, csiReplicas)
		gomega.Expect(restartSuccess).To(gomega.BeTrue(), "csi driver restart not successful")
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Creating Storage Class")
		storageclass, err := createStorageClass(client, nil, nil, "", "", false, "")
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		defer func() {
			ginkgo.By("Delete Storage Class")
			err = client.StorageV1().StorageClasses().Delete(ctx, storageclass.Name, *metav1.NewDeleteOptions(0))
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}()

		ginkgo.By("Verify we can create a PVC and attach it to pod")
		pod1, pvclaim1, pv1 := verifyPvcPodCreationAfterConfigSecretChange(client, namespace, storageclass)
		defer func() {
			performCleanUpOfPvcPod(client, namespace, pod1, pvclaim1, pv1)
		}()

		ginkgo.By("Update vsphere-config-secret with testuser2 credentials")
		createCsiVsphereSecret(client, ctx, configSecretUser2Alias, configSecretTestUser1Password, csiNamespace,
			vCenterIP, vCenterPort, dataCenter, "")

		ginkgo.By("Restart CSI driver")
		restartSuccess, err = restartCSIDriver(ctx, client, namespace, csiReplicas)
		gomega.Expect(restartSuccess).To(gomega.BeTrue(), "csi driver restart not successful")
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

		ginkgo.By("Update vsphere-config-secret with testuser1 credentials which has required roles and privileges")
		createCsiVsphereSecret(client, ctx, configSecretUser1Alias, configSecretTestUser1Password, csiNamespace,
			vCenterIP, vCenterPort, dataCenter, "")
		defer func() {
			ginkgo.By("Reverting back csi-vsphere.conf with its original vcenter user " +
				"and its credentials")
			createCsiVsphereSecret(client, ctx, vCenterUIUser, vCenterUIPassword, csiNamespace,
				vCenterIP, vCenterPort, dataCenter, "")
			revertOriginalvCenterUser = true

			ginkgo.By("Restart CSI driver")
			restartSuccess, err = restartCSIDriver(ctx, client, namespace, csiReplicas)
			gomega.Expect(restartSuccess).To(gomega.BeTrue(), "csi driver restart not successful")
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}()

		ginkgo.By("Restart CSI driver")
		restartSuccess, err = restartCSIDriver(ctx, client, namespace, csiReplicas)
		gomega.Expect(restartSuccess).To(gomega.BeTrue(), "csi driver restart not successful")
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Verify we can create a PVC and attach it to pod")
		pod3, pvclaim3, pvs3 := verifyPvcPodCreationAfterConfigSecretChange(client, namespace, storageclass)
		defer func() {
			performCleanUpOfPvcPod(client, namespace, pod3, pvclaim3, pvs3)
		}()

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

		ginkgo.By("Create testuser1 and assign limited roles and privileges to testuser1")
		createTestUserAndAssignLimitedRolesAndPrivileges(masterIp, sshClientConfig, configSecretTestUser1,
			configSecretTestUser1Password, configSecretUser1Alias, propagateVal,
			dataCenters, clusters, hosts, vms, datastores)
		defer func() {
			ginkgo.By("Delete testuser1 and remove roles and privileges assigned to testuser1")
			deleteTestUserAndRemoveRolesPrivileges(masterIp, sshClientConfig, configSecretTestUser1,
				configSecretTestUser1Password, configSecretUser1Alias, propagateVal,
				dataCenters, clusters, hosts, vms, datastores)
		}()

		ginkgo.By("Create testuser2 and assign limited roles and privileges to testuser2")
		createTestUserAndAssignLimitedRolesAndPrivileges(masterIp, sshClientConfig, configSecretTestUser2,
			configSecretTestUser2Password, configSecretUser2Alias, propagateVal,
			dataCenters, clusters, hosts, vms, datastores)
		defer func() {
			ginkgo.By("Delete testuser2 and remove roles and privileges assigned to testuser2")
			deleteTestUserAndRemoveRolesPrivileges(masterIp, sshClientConfig, configSecretTestUser2,
				configSecretTestUser2Password, configSecretUser2Alias, propagateVal,
				dataCenters, clusters, hosts, vms, datastores)
		}()

		ginkgo.By("Update vsphere-config-secret with testuser1 credentials")
		createCsiVsphereSecret(client, ctx, configSecretUser1Alias, configSecretTestUser1Password, csiNamespace,
			vCenterIP, vCenterPort, dataCenter, "")
		defer func() {
			ginkgo.By("Reverting back csi-vsphere.conf with its original vcenter user " +
				"and its credentials")
			createCsiVsphereSecret(client, ctx, vCenterUIUser, vCenterUIPassword, csiNamespace,
				vCenterIP, vCenterPort, dataCenter, "")
			revertOriginalvCenterUser = true

			ginkgo.By("Restart CSI driver")
			restartSuccess, err := restartCSIDriver(ctx, client, namespace, csiReplicas)
			gomega.Expect(restartSuccess).To(gomega.BeTrue(), "csi driver restart not successful")
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}()

		ginkgo.By("Restart CSI driver")
		restartSuccess, err := restartCSIDriver(ctx, client, namespace, csiReplicas)
		gomega.Expect(restartSuccess).To(gomega.BeTrue(), "csi driver restart not successful")
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
		createTestUserAndAssignRolesPrivileges(masterIp, sshClientConfig, configSecretTestUser1,
			configSecretTestUser1Password, configSecretUser1Alias, propagateVal,
			dataCenters, clusters, hosts, vms, datastores, "reuseUser", "reuseRoles")

		ginkgo.By("Restart CSI driver")
		restartSuccess, err = restartCSIDriver(ctx, client, namespace, csiReplicas)
		gomega.Expect(restartSuccess).To(gomega.BeTrue(), "csi driver restart not successful")
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Verify we can create a PVC and attach it to pod")
		pod2, pvclaim2, pvs2 := verifyPvcPodCreationAfterConfigSecretChange(client, namespace, storageclass)
		defer func() {
			performCleanUpOfPvcPod(client, namespace, pod2, pvclaim2, pvs2)
		}()

		ginkgo.By("Verify the PVC which was stuck in Pending state should gets bound eventually")
		pvclaim1, err = client.CoreV1().PersistentVolumeClaims(namespace).Get(ctx, pvclaim1.Name, metav1.GetOptions{})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		gomega.Expect(pvclaim1.Status.Phase == v1.ClaimBound).To(gomega.BeTrue())
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
		5.  Change csi-vsphere.conf and add a dummy datacenter and re-create vsphere-config-secret
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
		dummyDataCenter := "Dummy-Data-Center"

		ginkgo.By("Create testuser1 and assign required roles and privileges to testuser1")
		createTestUserAndAssignRolesPrivileges(masterIp, sshClientConfig, configSecretTestUser1,
			configSecretTestUser1Password, configSecretUser1Alias, propagateVal,
			dataCenters, clusters, hosts, vms, datastores, "createUser", "createRoles")
		defer func() {
			ginkgo.By("Delete testuser1 and remove roles and privileges assigned to testuser1")
			deleteTestUserAndRemoveRolesPrivileges(masterIp, sshClientConfig, configSecretTestUser1,
				configSecretTestUser1Password, configSecretUser1Alias, propagateVal,
				dataCenters, clusters, hosts, vms, datastores)
		}()

		ginkgo.By("Create testuser2 and assign required roles and privileges to testuser2")
		createTestUserAndAssignRolesPrivileges(masterIp, sshClientConfig, configSecretTestUser2,
			configSecretTestUser2Password, configSecretUser2Alias, propagateVal,
			dataCenters, clusters, hosts, vms, datastores, "createUser", "createRoles")
		defer func() {
			ginkgo.By("Delete testuser2 and remove roles and privileges assigned to testuser2")
			deleteTestUserAndRemoveRolesPrivileges(masterIp, sshClientConfig, configSecretTestUser2,
				configSecretTestUser2Password, configSecretUser2Alias, propagateVal,
				dataCenters, clusters, hosts, vms, datastores)
		}()

		ginkgo.By("Update vsphere-config-secret with testuser1 credentials")
		createCsiVsphereSecret(client, ctx, configSecretUser1Alias, configSecretTestUser1Password, csiNamespace,
			vCenterIP, vCenterPort, dataCenter, "")

		ginkgo.By("Restart CSI driver")
		restartSuccess, err := restartCSIDriver(ctx, client, namespace, csiReplicas)
		gomega.Expect(restartSuccess).To(gomega.BeTrue(), "csi driver restart not successful")
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Creating Storage Class")
		storageclass, err := createStorageClass(client, nil, nil, "", "", false, "")
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		defer func() {
			ginkgo.By("Delete Storage Class")
			err = client.StorageV1().StorageClasses().Delete(ctx, storageclass.Name, *metav1.NewDeleteOptions(0))
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}()

		ginkgo.By("Verify we can create a PVC and attach it to pod")
		pod1, pvclaim1, pv1 := verifyPvcPodCreationAfterConfigSecretChange(client, namespace, storageclass)
		defer func() {
			performCleanUpOfPvcPod(client, namespace, pod1, pvclaim1, pv1)
		}()

		ginkgo.By("Update vsphere-config-secret with dummy datacenter details")
		createCsiVsphereSecret(client, ctx, configSecretUser1Alias, configSecretTestUser1Password, csiNamespace,
			vCenterIP, vCenterPort, dummyDataCenter, "")

		ginkgo.By("Restart CSI driver")
		err = updateDeploymentReplicawithWait(client, 0, vSphereCSIControllerPodNamePrefix, csiNamespace)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		err = updateDeploymentReplicawithWait(client, csiReplicas, vSphereCSIControllerPodNamePrefix, csiNamespace)
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

		ginkgo.By("Update vsphere-config-secret with testuser1 credentials")
		createCsiVsphereSecret(client, ctx, configSecretUser1Alias, configSecretTestUser1Password, csiNamespace,
			vCenterIP, vCenterPort, dataCenter, "")
		defer func() {
			ginkgo.By("Reverting back csi-vsphere.conf with its original vcenter user " +
				"and its credentials")
			createCsiVsphereSecret(client, ctx, vCenterUIUser, vCenterUIPassword, csiNamespace,
				vCenterIP, vCenterPort, dataCenter, "")
			revertOriginalvCenterUser = true

			ginkgo.By("Restart CSI driver")
			restartSuccess, err = restartCSIDriver(ctx, client, namespace, csiReplicas)
			gomega.Expect(restartSuccess).To(gomega.BeTrue(), "csi driver restart not successful")
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}()

		ginkgo.By("Restart CSI driver")
		restartSuccess, err = restartCSIDriver(ctx, client, namespace, csiReplicas)
		gomega.Expect(restartSuccess).To(gomega.BeTrue(), "csi driver restart not successful")
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Verify we can create a PVC and attach it to pod")
		pod3, pvclaim3, pv3 := verifyPvcPodCreationAfterConfigSecretChange(client, namespace, storageclass)
		defer func() {
			performCleanUpOfPvcPod(client, namespace, pod3, pvclaim3, pv3)
		}()

		ginkgo.By("Verify the PVC which was stuck in Pending state should gets bound eventually")
		pvclaim2, err = client.CoreV1().PersistentVolumeClaims(namespace).Get(ctx, pvclaim2.Name, metav1.GetOptions{})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		gomega.Expect(pvclaim2.Status.Phase == v1.ClaimBound).To(gomega.BeTrue())

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
		defer cancel()
		targetDsURL := GetAndExpectStringEnvVar(envSharedDatastoreURL)

		ginkgo.By("Create testuser1 and assign required roles and privileges to testuser1")
		createTestUserAndAssignRolesPrivileges(masterIp, sshClientConfig, configSecretTestUser1,
			configSecretTestUser1Password, configSecretUser1Alias, propagateVal,
			dataCenters, clusters, hosts, vms, datastores, "createUser", "createRoles")
		defer func() {
			ginkgo.By("Delete testuser1 and remove roles and privileges assigned to testuser1")
			deleteTestUserAndRemoveRolesPrivileges(masterIp, sshClientConfig, configSecretTestUser1,
				configSecretTestUser1Password, configSecretUser1Alias, propagateVal,
				dataCenters, clusters, hosts, vms, datastores)
		}()

		ginkgo.By("Create testuser2 and assign required roles and privileges to testuser2")
		createTestUserAndAssignRolesPrivileges(masterIp, sshClientConfig, configSecretTestUser2,
			configSecretTestUser2Password, configSecretUser2Alias, propagateVal,
			dataCenters, clusters, hosts, vms, datastores, "createUser", "createRoles")
		defer func() {
			ginkgo.By("Delete testuser2 and remove roles and privileges assigned to testuser2")
			deleteTestUserAndRemoveRolesPrivileges(masterIp, sshClientConfig, configSecretTestUser2,
				configSecretTestUser2Password, configSecretUser2Alias, propagateVal,
				dataCenters, clusters, hosts, vms, datastores)
		}()

		ginkgo.By("Update vsphere-config-secret with testuser1 credentials")
		createCsiVsphereSecret(client, ctx, configSecretUser1Alias, configSecretTestUser1Password, csiNamespace,
			vCenterIP, vCenterPort, dataCenter, "")

		ginkgo.By("Restart CSI driver")
		restartSuccess, err := restartCSIDriver(ctx, client, namespace, csiReplicas)
		gomega.Expect(restartSuccess).To(gomega.BeTrue(), "csi driver restart not successful")
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Creating Storage Class")
		storageclass, err := createStorageClass(client, nil, nil, "", "", false, "")
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		defer func() {
			ginkgo.By("Delete Storage Class")
			err = client.StorageV1().StorageClasses().Delete(ctx, storageclass.Name, *metav1.NewDeleteOptions(0))
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}()

		ginkgo.By("Verify we can create a PVC and attach it to pod")
		pod1, pvclaim1, pvs1 := verifyPvcPodCreationAfterConfigSecretChange(client, namespace, storageclass)
		defer func() {
			performCleanUpOfPvcPod(client, namespace, pod1, pvclaim1, pvs1)
		}()

		ginkgo.By("Update vsphere-config-secret with testuser1 credentials and pass target datastore url")
		createCsiVsphereSecret(client, ctx, configSecretUser1Alias, configSecretTestUser1Password, csiNamespace,
			vCenterIP, vCenterPort, dataCenter, targetDsURL)
		defer func() {
			ginkgo.By("Reverting back csi-vsphere.conf with its original vcenter user " +
				"and its credentials")
			createCsiVsphereSecret(client, ctx, vCenterUIUser, vCenterUIPassword, csiNamespace,
				vCenterIP, vCenterPort, dataCenter, "")
			revertOriginalvCenterUser = true

			ginkgo.By("Restart CSI driver")
			restartSuccess, err = restartCSIDriver(ctx, client, namespace, csiReplicas)
			gomega.Expect(restartSuccess).To(gomega.BeTrue(), "csi driver restart not successful")
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}()

		ginkgo.By("Restart CSI driver")
		restartSuccess, err = restartCSIDriver(ctx, client, namespace, csiReplicas)
		gomega.Expect(restartSuccess).To(gomega.BeTrue(), "csi driver restart not successful")
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Verify we can create a PVC and attach it to pod")
		pod2, pvclaim2, pvs2 := verifyPvcPodCreationAfterConfigSecretChange(client, namespace, storageclass)
		defer func() {
			performCleanUpOfPvcPod(client, namespace, pod2, pvclaim2, pvs2)
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
		defaultvCenterPort := "443"

		ginkgo.By("Create testuser1 and assign required roles and privileges to testuser1")
		createTestUserAndAssignRolesPrivileges(masterIp, sshClientConfig, configSecretTestUser1,
			configSecretTestUser1Password, configSecretUser1Alias, propagateVal,
			dataCenters, clusters, hosts, vms, datastores, "createUser", "createRoles")
		defer func() {
			ginkgo.By("Delete testuser1 and remove roles and privileges assigned to testuser1")
			deleteTestUserAndRemoveRolesPrivileges(masterIp, sshClientConfig, configSecretTestUser1,
				configSecretTestUser1Password, configSecretUser1Alias, propagateVal,
				dataCenters, clusters, hosts, vms, datastores)
		}()

		ginkgo.By("Create testuser2 and assign required roles and privileges to testuser2")
		createTestUserAndAssignRolesPrivileges(masterIp, sshClientConfig, configSecretTestUser2,
			configSecretTestUser2Password, configSecretUser2Alias, propagateVal,
			dataCenters, clusters, hosts, vms, datastores, "createUser", "createRoles")
		defer func() {
			ginkgo.By("Delete testuser2 and remove roles and privileges assigned to testuser2")
			deleteTestUserAndRemoveRolesPrivileges(masterIp, sshClientConfig, configSecretTestUser2,
				configSecretTestUser2Password, configSecretUser2Alias, propagateVal,
				dataCenters, clusters, hosts, vms, datastores)
		}()

		ginkgo.By("Create vsphere-config-secret file with testuser1 credentials using default vc port")
		createCsiVsphereSecret(client, ctx, configSecretUser1Alias, configSecretTestUser1Password, csiNamespace,
			vCenterIP, defaultvCenterPort, dataCenter, "")

		ginkgo.By("Restart CSI driver")
		restartSuccess, err := restartCSIDriver(ctx, client, namespace, csiReplicas)
		gomega.Expect(restartSuccess).To(gomega.BeTrue(), "csi driver restart not successful")
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

		ginkgo.By("Try to create a PVC verify that it is stuck in pending state")
		pvclaim1, err := createPVC(client, namespace, nil, "", storageclass, "")
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		ginkgo.By("Expect claim status to be in Pending state")
		err = fpv.WaitForPersistentVolumeClaimPhase(v1.ClaimPending, client,
			pvclaim1.Namespace, pvclaim1.Name, framework.Poll, time.Minute)
		gomega.Expect(err).NotTo(gomega.HaveOccurred(),
			fmt.Sprintf("Failed to find the volume in pending state with err: %v", err))
		var pvclaims1 []*v1.PersistentVolumeClaim
		pvclaims1 = append(pvclaims1, pvclaim1)
		defer func() {
			ginkgo.By("Deleting the PVC")
			err = fpv.DeletePersistentVolumeClaim(client, pvclaim1.Name, namespace)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}()

		ginkgo.By("Create vsphere-config-secret file with testuser1 credentials using non-default port")
		createCsiVsphereSecret(client, ctx, configSecretUser1Alias, configSecretTestUser1Password, csiNamespace,
			vCenterIP, vCenterPort, dataCenter, "")
		defer func() {
			ginkgo.By("Reverting back csi-vsphere.conf with its original vcenter user " +
				"and its credentials")
			createCsiVsphereSecret(client, ctx, vCenterUIUser, vCenterUIPassword, csiNamespace,
				vCenterIP, vCenterPort, dataCenter, "")
			revertOriginalvCenterUser = true

			ginkgo.By("Restart CSI driver")
			restartSuccess, err = restartCSIDriver(ctx, client, namespace, csiReplicas)
			gomega.Expect(restartSuccess).To(gomega.BeTrue(), "csi driver restart not successful")
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}()

		ginkgo.By("Restart CSI driver")
		restartSuccess, err = restartCSIDriver(ctx, client, namespace, csiReplicas)
		gomega.Expect(restartSuccess).To(gomega.BeTrue(), "csi driver restart not successful")
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Verify we can create a PVC and attach it to pod")
		pod2, pvclaim2, pvs2 := verifyPvcPodCreationAfterConfigSecretChange(client, namespace, storageclass)
		defer func() {
			performCleanUpOfPvcPod(client, namespace, pod2, pvclaim2, pvs2)
		}()

		ginkgo.By("Verify the PVC which was stuck in Pending state should gets bound eventually")
		pvs1, err := fpv.WaitForPVClaimBoundPhase(client, pvclaims1, framework.ClaimProvisionTimeout)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		gomega.Expect(pvs1).NotTo(gomega.BeEmpty())
		pv1 := pvs1[0]

		ginkgo.By("Creating pod")
		pod1, err := createPod(client, namespace, nil, []*v1.PersistentVolumeClaim{pvclaim1}, false, "")
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		defer func() {
			ginkgo.By(fmt.Sprintf("Deleting the pod %s in namespace %s", pod1.Name, namespace))
			err = fpod.DeletePodWithWait(client, pod1)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}()

		ginkgo.By("Verify volume metadata for POD, PVC and PV")
		err = waitAndVerifyCnsVolumeMetadata(pv1.Spec.CSI.VolumeHandle, pvclaim1, pv1, pod1)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
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
		dummyvCenterPort := "4444"

		ginkgo.By("Create testuser1 and assign required roles and privileges to testuser1")
		createTestUserAndAssignRolesPrivileges(masterIp, sshClientConfig, configSecretTestUser1,
			configSecretTestUser1Password, configSecretUser1Alias, propagateVal,
			dataCenters, clusters, hosts, vms, datastores, "createUser", "createRoles")
		defer func() {
			ginkgo.By("Delete testuser1 and remove roles and privileges assigned to testuser1")
			deleteTestUserAndRemoveRolesPrivileges(masterIp, sshClientConfig, configSecretTestUser1,
				configSecretTestUser1Password, configSecretUser1Alias, propagateVal,
				dataCenters, clusters, hosts, vms, datastores)
		}()

		ginkgo.By("Create testuser2 and assign required roles and privileges to testuser2")
		createTestUserAndAssignRolesPrivileges(masterIp, sshClientConfig, configSecretTestUser2,
			configSecretTestUser2Password, configSecretUser2Alias, propagateVal,
			dataCenters, clusters, hosts, vms, datastores, "createUser", "createRoles")
		defer func() {
			ginkgo.By("Delete testuser2 and remove roles and privileges assigned to testuser2")
			deleteTestUserAndRemoveRolesPrivileges(masterIp, sshClientConfig, configSecretTestUser2,
				configSecretTestUser2Password, configSecretUser2Alias, propagateVal,
				dataCenters, clusters, hosts, vms, datastores)
		}()

		ginkgo.By("Create vsphere-config-secret file using testuser1 credentials")
		createCsiVsphereSecret(client, ctx, configSecretUser1Alias, configSecretTestUser1Password, csiNamespace,
			vCenterIP, vCenterPort, dataCenter, "")

		ginkgo.By("Restart CSI driver")
		restartSuccess, err := restartCSIDriver(ctx, client, namespace, csiReplicas)
		gomega.Expect(restartSuccess).To(gomega.BeTrue(), "csi driver restart not successful")
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

		ginkgo.By("Verify we can create a PVC and attach it to pod")
		pod1, pvclaim1, pvs1 := verifyPvcPodCreationAfterConfigSecretChange(client, namespace, storageclass)
		defer func() {
			performCleanUpOfPvcPod(client, namespace, pod1, pvclaim1, pvs1)
		}()

		ginkgo.By("Update vsphere-config-secret file with dummy vcenter port")
		createCsiVsphereSecret(client, ctx, configSecretUser1Alias, configSecretTestUser1Password, csiNamespace,
			vCenterIP, dummyvCenterPort, dataCenter, "")

		ginkgo.By("Restart CSI driver")
		restartSuccess, err = restartCSIDriver(ctx, client, namespace, csiReplicas)
		gomega.Expect(restartSuccess).To(gomega.BeTrue(), "csi driver restart not successful")
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

		ginkgo.By("Update vsphere-config-secret with correct vCenter credentials")
		createCsiVsphereSecret(client, ctx, configSecretUser1Alias, configSecretTestUser1Password, csiNamespace,
			vCenterIP, vCenterPort, dataCenter, "")
		defer func() {
			ginkgo.By("Reverting back csi-vsphere.conf with its original vcenter user " +
				"and its credentials")
			createCsiVsphereSecret(client, ctx, vCenterUIUser, vCenterUIPassword, csiNamespace,
				vCenterIP, vCenterPort, dataCenter, "")
			revertOriginalvCenterUser = true

			ginkgo.By("Restart CSI driver")
			restartSuccess, err = restartCSIDriver(ctx, client, namespace, csiReplicas)
			gomega.Expect(restartSuccess).To(gomega.BeTrue(), "csi driver restart not successful")
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}()

		ginkgo.By("Restart CSI driver")
		restartSuccess, err = restartCSIDriver(ctx, client, namespace, csiReplicas)
		gomega.Expect(restartSuccess).To(gomega.BeTrue(), "csi driver restart not successful")
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Verify we can create a PVC and attach it to pod")
		pod3, pvclaim3, pvs3 := verifyPvcPodCreationAfterConfigSecretChange(client, namespace, storageclass)
		defer func() {
			performCleanUpOfPvcPod(client, namespace, pod3, pvclaim3, pvs3)
		}()

		ginkgo.By("Verify the PVC which was stuck in Pending state should gets bound eventually")
		pvclaim2, err = client.CoreV1().PersistentVolumeClaims(namespace).Get(ctx, pvclaim2.Name, metav1.GetOptions{})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		gomega.Expect(pvclaim2.Status.Phase == v1.ClaimBound).To(gomega.BeTrue())
	})
})
