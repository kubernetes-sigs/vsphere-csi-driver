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
	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	clientset "k8s.io/client-go/kubernetes"
	"k8s.io/kubernetes/test/e2e/framework"
	fnodes "k8s.io/kubernetes/test/e2e/framework/node"
	fpod "k8s.io/kubernetes/test/e2e/framework/pod"
	fpv "k8s.io/kubernetes/test/e2e/framework/pv"
)

var _ = ginkgo.Describe("[csi-config-secret] Config-Secret", func() {
	f := framework.NewDefaultFramework("config-secret-volume-provisioning")
	var (
		client              clientset.Interface
		namespace           string
		sshClientConfig     *ssh.ClientConfig
		vcAddress           string
		allMasterIps        []string
		masterIp            string
		loginCmd            string
		csiNamespace        string
		testUser            string
		dataCenter          string
		cluster             string
		err                 error
		insecureFlag        bool
		clusterId           string
		clusterDistribution string
		dataCenters         string
		vCenterPort         string
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
		vcAddress = e2eVSphere.Config.Global.VCenterHostname
		csiNamespace = GetAndExpectStringEnvVar(envCSINamespace)
		// govc login command
		loginCmd = govcLoginCmd(vcAddress)

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

	ginkgo.It("Change vcenter users in vsphere config secret file having same password", func() {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		// Step1: Create two users (user1 and user2) with required roles and privileges and with same password
		ginkgo.By("Create 2 test users")
		err = createTestUser(loginCmd, sshClientConfig, masterIp, configSecretTestUser1,
			configSecertTestUser1Password)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		err = createTestUser(loginCmd, sshClientConfig, masterIp, configSecretTestUser2,
			configSecertTestUser1Password)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		// create roles for testuser1
		ginkgo.By("Create roles for testuser1")
		err = createRolesForTestUser(loginCmd, sshClientConfig, masterIp, configSecretTestUser1)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		// create roles for testuser2
		ginkgo.By("Create roles for testuser2")
		err = createRolesForTestUser(loginCmd, sshClientConfig, masterIp, configSecretTestUser2)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Fetch DataCenter, Cluster and hosts details")
		dataCenter, cluster, err = getDataCenterClusterHostAndVmDetails(loginCmd, sshClientConfig,
			masterIp)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Assign required roles and privileges for testuser1 and testuser2")

		// set DataCenter level permission for testuser1 and testuser2
		ginkgo.By("Set DataCenter level permission for testuser1 and testuser2")
		err = setDataCenterLevelPermission(loginCmd, sshClientConfig, masterIp, dataCenter,
			configSecretTestUser1)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		err = setDataCenterLevelPermission(loginCmd, sshClientConfig, masterIp, dataCenter,
			configSecretTestUser2)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		// set hosts level permission for testuser1 and testuser2
		ginkgo.By("Set hosts level permission for testuser1 and testuser2")
		err = setHostLevelPermission(loginCmd, sshClientConfig, masterIp, configSecretTestUser1)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		err = setHostLevelPermission(loginCmd, sshClientConfig, masterIp, configSecretTestUser2)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		// set K8s VM level permission for testuser1 and testuser2
		ginkgo.By("Set k8s VM level permission for testuser1 and testuser2")
		err = setK8sVMLevelPermission(loginCmd, sshClientConfig, masterIp, configSecretTestUser1)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		err = setK8sVMLevelPermission(loginCmd, sshClientConfig, masterIp, configSecretTestUser2)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		// set cluster level permission for testuser1 and testuser2
		ginkgo.By("Set cluster level permission for testuser1 and testuser2")
		err = setClusterLevelPermission(loginCmd, sshClientConfig, masterIp,
			configSecretTestUser1, cluster)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		err = setClusterLevelPermission(loginCmd, sshClientConfig, masterIp,
			configSecretTestUser2, cluster)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		// set datastore level permission for testuser1 and testuser2
		ginkgo.By("Set datastore level permission for testuser1 and testuser2")
		err = setDataStoreLevelPermission(loginCmd, sshClientConfig, masterIp,
			configSecretTestUser1, dataCenter)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		err = setDataStoreLevelPermission(loginCmd, sshClientConfig, masterIp,
			configSecretTestUser2, dataCenter)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		defer func() {
			// delete users, its roles and permissions
			ginkgo.By("Delete testUser1, its roles and permissions")
			err = deleteUsersRolesAndPermissions(loginCmd, sshClientConfig, masterIp,
				configSecretTestUser1, dataCenter, cluster)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			ginkgo.By("Delete testUser2, its roles and permissions")
			err = deleteUsersRolesAndPermissions(loginCmd, sshClientConfig, masterIp,
				configSecretTestUser2, dataCenter, cluster)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())

			// Create csi-vsphere.conf with original vcenter user and its credentails
			ginkgo.By("Create csi-vsphere.conf with original vcenter user and its credentails")
			insecureFlag, clusterId, clusterDistribution,
				dataCenters, vCenterPort = getConfigSecretFileValues(client, ctx)
			err = deleteCsiVsphereConfFile(loginCmd, sshClientConfig, masterIp)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			err = createCsiVsphereConfFile(clusterId, clusterDistribution, vcAddress, insecureFlag,
				vCenterUIUser, vCenterUIPassword, vCenterPort, dataCenters,
				loginCmd, sshClientConfig, masterIp)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}()

		/* Step2 : Create csi-vsphere.conf with user1's credentials and create
		vsphere-config-secret in turn using that file */
		ginkgo.By("Fetch vsphere-config-secret file details")
		insecureFlag, clusterId, clusterDistribution,
			dataCenters, vCenterPort = getConfigSecretFileValues(client, ctx)

		// delete csi vsphere conf file
		ginkgo.By("Delete previously created vsphere-config-secret file")
		err = deleteCsiVsphereConfFile(loginCmd, sshClientConfig, masterIp)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		// create csi vsphere conf file with testUser1
		ginkgo.By("Create vsphere-config-secret file with testuser1 credentials")
		testUser = configSecretTestUser1 + "@vsphere.local"
		err = createCsiVsphereConfFile(clusterId, clusterDistribution, vcAddress, insecureFlag,
			testUser, configSecertTestUser1Password, vCenterPort, dataCenters,
			loginCmd, sshClientConfig, masterIp)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		// Step3: Install CSI driver
		ginkgo.By("Install CSI driver")
		err = installCsiDriver(loginCmd, sshClientConfig, masterIp)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		// Step4: Verify we can create a PVC and attach it to pod
		ginkgo.By("Verify we can create a PVC and attach it to pod")
		ginkgo.By("Creating Storage Class")
		storageclass, err := createStorageClass(client, nil, nil, "", "", false, "")
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		defer func() {
			// delete storage class
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
			// delete pvc
			err = fpv.DeletePersistentVolumeClaim(client, pvclaim1.Name, namespace)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}()

		ginkgo.By("Creating pod")
		pod1, err := createPod(client, namespace, nil, []*v1.PersistentVolumeClaim{pvclaim1}, false, "")
		defer func() {
			// delete pod
			ginkgo.By(fmt.Sprintf("Deleting the pod %s in namespace %s", pod1.Name, namespace))
			err = fpod.DeletePodWithWait(client, pod1)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}()

		/* Step5: Change csi-vsphere.conf with user2's credentials and re-create
		vsphere-config-secret in turn using that file */
		ginkgo.By("Fetch vsphere-config-secret file details")
		insecureFlag, clusterId, clusterDistribution,
			dataCenters, vCenterPort = getConfigSecretFileValues(client, ctx)

		// delete csi vsphere conf file
		ginkgo.By("Delete previously created vsphere-config-secret file")
		err = deleteCsiVsphereConfFile(loginCmd, sshClientConfig, masterIp)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		// create csi vsphere conf file with testUser2
		ginkgo.By("Create vsphere-config-secret file with testuser2 credentials")
		testUser = configSecretTestUser2 + "@vsphere.local"
		err = createCsiVsphereConfFile(clusterId, clusterDistribution, vcAddress, insecureFlag,
			testUser, configSecertTestUser1Password, vCenterPort, dataCenters,
			loginCmd, sshClientConfig, masterIp)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		// Step6: Verify we can create a PVC and attach it to pod
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
		defer func() {
			// delete pod
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

	ginkgo.It("Change vcenter user and its password in vsphere config secret file", func() {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		// Step1: Create a user, user1, with required roles and privileges
		ginkgo.By("Create 2 test users")
		err = createTestUser(loginCmd, sshClientConfig, masterIp, configSecretTestUser1,
			configSecertTestUser1Password)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		err = createTestUser(loginCmd, sshClientConfig, masterIp, configSecretTestUser2,
			configSecertTestUser1Password)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		// create roles for testuser1
		ginkgo.By("Create roles for testuser1")
		err = createRolesForTestUser(loginCmd, sshClientConfig, masterIp, configSecretTestUser1)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		// create roles for testuser2
		ginkgo.By("Create roles for testuser2")
		err = createRolesForTestUser(loginCmd, sshClientConfig, masterIp, configSecretTestUser2)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Fetch DataCenter, Cluster and hosts details")
		dataCenter, cluster, err = getDataCenterClusterHostAndVmDetails(loginCmd, sshClientConfig,
			masterIp)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Assign required roles and privileges for testuser1 and testuser2")

		// set DataCenter level permission for testuser1 and testuser2
		ginkgo.By("Set DataCenter level permission for testuser1 and testuser2")
		err = setDataCenterLevelPermission(loginCmd, sshClientConfig, masterIp, dataCenter,
			configSecretTestUser1)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		err = setDataCenterLevelPermission(loginCmd, sshClientConfig, masterIp, dataCenter,
			configSecretTestUser2)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		// set hosts level permission for testuser1 and testuser2
		ginkgo.By("Set hosts level permission for testuser1 and testuser2")
		err = setHostLevelPermission(loginCmd, sshClientConfig, masterIp,
			configSecretTestUser1)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		err = setHostLevelPermission(loginCmd, sshClientConfig, masterIp,
			configSecretTestUser2)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		// set K8s VM level permission for testuser1 and testuser2
		ginkgo.By("Set k8s VM level permission for testuser1 and testuser2")
		err = setK8sVMLevelPermission(loginCmd, sshClientConfig, masterIp,
			configSecretTestUser1)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		err = setK8sVMLevelPermission(loginCmd, sshClientConfig, masterIp,
			configSecretTestUser2)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		// set cluster level permission for testuser1 and testuser2
		ginkgo.By("Set cluster level permission for testuser1 and testuser2")
		err = setClusterLevelPermission(loginCmd, sshClientConfig, masterIp,
			configSecretTestUser1, cluster)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		err = setClusterLevelPermission(loginCmd, sshClientConfig, masterIp,
			configSecretTestUser2, cluster)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		// set datastore level permission for testuser1 and testuser2
		ginkgo.By("Set datastore level permission for testuser1 and testuser2")
		err = setDataStoreLevelPermission(loginCmd, sshClientConfig, masterIp,
			configSecretTestUser1, dataCenter)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		err = setDataStoreLevelPermission(loginCmd, sshClientConfig, masterIp,
			configSecretTestUser2, dataCenter)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		defer func() {
			// delete users, its roles and permissions
			ginkgo.By("Delete testUser1, its roles and permissions")
			err = deleteUsersRolesAndPermissions(loginCmd, sshClientConfig, masterIp,
				configSecretTestUser1, dataCenter, cluster)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			ginkgo.By("Delete testUser2, its roles and permissions")
			err = deleteUsersRolesAndPermissions(loginCmd, sshClientConfig, masterIp,
				configSecretTestUser2, dataCenter, cluster)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())

			// Create csi-vsphere.conf with original vcenter user and its credentails
			ginkgo.By("Create csi-vsphere.conf with original vcenter user and its credentails")
			insecureFlag, clusterId, clusterDistribution,
				dataCenters, vCenterPort = getConfigSecretFileValues(client, ctx)
			err = deleteCsiVsphereConfFile(loginCmd, sshClientConfig, masterIp)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			err = createCsiVsphereConfFile(clusterId, clusterDistribution, vcAddress, insecureFlag,
				vCenterUIUser, vCenterUIPassword, vCenterPort, dataCenters,
				loginCmd, sshClientConfig, masterIp)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}()

		/* Step2: Create csi-vsphere.conf with user1's credentials and create
		vsphere-config-secret in turn using that file */
		ginkgo.By("Fetch vsphere-config-secret file details")
		insecureFlag, clusterId, clusterDistribution,
			dataCenters, vCenterPort = getConfigSecretFileValues(client, ctx)

		// delete csi vsphere conf file
		ginkgo.By("Delete previously created vsphere-config-secret file")
		err = deleteCsiVsphereConfFile(loginCmd, sshClientConfig, masterIp)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		// create csi vsphere conf file with testUser1
		ginkgo.By("Create vsphere-config-secret file with testuser1 credentials")
		testUser = configSecretTestUser1 + "@vsphere.local"
		err = createCsiVsphereConfFile(clusterId, clusterDistribution, vcAddress, insecureFlag,
			testUser, configSecertTestUser1Password, vCenterPort, dataCenters,
			loginCmd, sshClientConfig, masterIp)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		// Step3: Install CSI driver
		ginkgo.By("Install CSI driver")
		err = installCsiDriver(loginCmd, sshClientConfig, masterIp)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		// Step4: Verify we can create a PVC and attach it to pod
		ginkgo.By("Verify we can create a PVC and attach it to pod")
		ginkgo.By("Creating Storage Class")
		storageclass, err := createStorageClass(client, nil, nil, "", "", false, "")
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		defer func() {
			// delete storage class
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
			// delete pvc
			err = fpv.DeletePersistentVolumeClaim(client, pvclaim1.Name, namespace)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}()

		ginkgo.By("Creating pod")
		pod1, err := createPod(client, namespace, nil, []*v1.PersistentVolumeClaim{pvclaim1}, false, "")
		defer func() {
			// delete pod
			ginkgo.By(fmt.Sprintf("Deleting the pod %s in namespace %s", pod1.Name, namespace))
			err = fpod.DeletePodWithWait(client, pod1)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}()

		// Step5: Change password for user1
		ginkgo.By("Change password for testuser1")
		testUser1NewPassword := e2eTestPassword

		// Step6: Try to create a PVC and verify it gets bound successfully
		ginkgo.By("Try to create a PVC and verify it gets bound successfully")
		pvc1, err := createPVC(client, namespace, nil, "", storageclass, "")
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		// Waiting for PVC to be bound.
		var pvclaims2 []*v1.PersistentVolumeClaim
		pvclaims2 = append(pvclaims2, pvc1)
		ginkgo.By("Waiting for all claims to be in bound state")
		_, err = fpv.WaitForPVClaimBoundPhase(client, pvclaims2, framework.ClaimProvisionTimeout)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		defer func() {
			ginkgo.By("Deleting the PVC")
			err = fpv.DeletePersistentVolumeClaim(client, pvc1.Name, namespace)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}()

		// Step7: Restart CSI controller pod
		ginkgo.By("Restart CSI controller pod")
		csiDeployment, err := client.AppsV1().Deployments(csiNamespace).Get(
			ctx, vSphereCSIControllerPodNamePrefix, metav1.GetOptions{})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		csiReplicas := csiDeployment.Spec.Replicas
		framework.Logf("Stopping CSI driver")
		_ = updateDeploymentReplica(client, 0, vSphereCSIControllerPodNamePrefix, csiNamespace)
		framework.Logf("Starting CSI driver")
		_ = updateDeploymentReplica(client, *csiReplicas, vSphereCSIControllerPodNamePrefix, csiNamespace)

		// Step8: Try to create a PVC verify that it is stuck in pending state
		ginkgo.By("Try to create a PVC verify that it is stuck in pending state")
		pvc2, err := createPVC(client, namespace, nil, "", storageclass, "")
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		ginkgo.By("Expect claim status to be in Pending state")
		err = fpv.WaitForPersistentVolumeClaimPhase(v1.ClaimPending, client,
			pvc2.Namespace, pvc2.Name, framework.Poll, time.Minute)
		gomega.Expect(err).NotTo(gomega.HaveOccurred(),
			fmt.Sprintf("Failed to find the volume in pending state with err: %v", err))
		defer func() {
			ginkgo.By("Deleting the PVC")
			err = fpv.DeletePersistentVolumeClaim(client, pvc2.Name, namespace)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}()

		/* Step9: Change csi-vsphere.conf with updated user1's credentials and re-create
		vsphere-config-secret in turn using that file */
		ginkgo.By("Fetch vsphere-config-secret file details")
		insecureFlag, clusterId, clusterDistribution,
			dataCenters, vCenterPort = getConfigSecretFileValues(client, ctx)

		// delete csi vsphere conf file
		ginkgo.By("Delete previously created vsphere-config-secret file")
		err = deleteCsiVsphereConfFile(loginCmd, sshClientConfig, masterIp)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		// create csi vsphere conf file with testUser1
		ginkgo.By("Create vsphere-config-secret file with testuser1 credentials")
		err = createCsiVsphereConfFile(clusterId, clusterDistribution, vcAddress, insecureFlag,
			testUser, testUser1NewPassword, vCenterPort, dataCenters,
			loginCmd, sshClientConfig, masterIp)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		// Step10: Verify we can create a PVC and attach it to pod
		ginkgo.By("Verify we can create a PVC and attach it to pod")
		ginkgo.By("Creating Pvc")
		pvclaim2, err := createPVC(client, namespace, nil, "", storageclass, "")
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		// Waiting for PVC to be bound.
		var pvclaims3 []*v1.PersistentVolumeClaim
		pvclaims3 = append(pvclaims3, pvclaim2)
		ginkgo.By("Waiting for all claims to be in bound state")
		_, err = fpv.WaitForPVClaimBoundPhase(client, pvclaims3, framework.ClaimProvisionTimeout)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		defer func() {
			// delete pvc
			err = fpv.DeletePersistentVolumeClaim(client, pvclaim2.Name, namespace)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}()

		ginkgo.By("Creating pod")
		pod2, err := createPod(client, namespace, nil, []*v1.PersistentVolumeClaim{pvclaim2}, false, "")
		defer func() {
			// delete pod
			ginkgo.By(fmt.Sprintf("Deleting the pod %s in namespace %s", pod2.Name, namespace))
			err = fpod.DeletePodWithWait(client, pod2)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}()

		// Step11: Verify the PVC which was stuck in Pening state should gets bound eventually
		ginkgo.By("Verify the PVC which was stuck in Pening state should gets bound eventually")
		pvc2, err = client.CoreV1().PersistentVolumeClaims(namespace).Get(ctx, pvc2.Name, metav1.GetOptions{})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		gomega.Expect(pvc2.Status.Phase == v1.ClaimBound).To(gomega.BeTrue())

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

	ginkgo.It("Change vcenter users in vsphere config secret file having different password", func() {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		/* Step1: Create two users (user1 and user2) with required roles and
		privileges and with different passwords */
		ginkgo.By("Create 2 test users")
		err = createTestUser(loginCmd, sshClientConfig, masterIp, configSecretTestUser1,
			configSecertTestUser1Password)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		err = createTestUser(loginCmd, sshClientConfig, masterIp, configSecretTestUser2,
			configSecertTestUser2Password)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		// create roles for testuser1
		ginkgo.By("Create roles for testuser1")
		err = createRolesForTestUser(loginCmd, sshClientConfig, masterIp, configSecretTestUser1)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		// create roles for testuser2
		ginkgo.By("Create roles for testuser2")
		err = createRolesForTestUser(loginCmd, sshClientConfig, masterIp, configSecretTestUser2)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Fetch DataCenter, Cluster and hosts details")
		dataCenter, cluster, err = getDataCenterClusterHostAndVmDetails(loginCmd, sshClientConfig, masterIp)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Assign required roles and privileges for testuser1 and testuser2")

		// set DataCenter level permission for testuser1 and testuser2
		ginkgo.By("Set DataCenter level permission for testuser1 and testuser2")
		err = setDataCenterLevelPermission(loginCmd, sshClientConfig, masterIp, dataCenter,
			configSecretTestUser1)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		err = setDataCenterLevelPermission(loginCmd, sshClientConfig, masterIp, dataCenter,
			configSecretTestUser2)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		// set hosts level permission for testuser1 and testuser2
		ginkgo.By("Set hosts level permission for testuser1 and testuser2")
		err = setHostLevelPermission(loginCmd, sshClientConfig, masterIp, configSecretTestUser1)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		err = setHostLevelPermission(loginCmd, sshClientConfig, masterIp, configSecretTestUser2)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		// set K8s VM level permission for testuser1 and testuser2
		ginkgo.By("Set k8s VM level permission for testuser1 and testuser2")
		err = setK8sVMLevelPermission(loginCmd, sshClientConfig, masterIp, configSecretTestUser1)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		err = setK8sVMLevelPermission(loginCmd, sshClientConfig, masterIp, configSecretTestUser2)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		// set cluster level permission for testuser1 and testuser2
		ginkgo.By("Set cluster level permission for testuser1 and testuser2")
		err = setClusterLevelPermission(loginCmd, sshClientConfig, masterIp,
			configSecretTestUser1, cluster)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		err = setClusterLevelPermission(loginCmd, sshClientConfig, masterIp,
			configSecretTestUser2, cluster)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		// set datastore level permission for testuser1 and testuser2
		ginkgo.By("Set datastore level permission for testuser1 and testuser2")
		err = setDataStoreLevelPermission(loginCmd, sshClientConfig, masterIp,
			configSecretTestUser1, dataCenter)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		err = setDataStoreLevelPermission(loginCmd, sshClientConfig, masterIp,
			configSecretTestUser2, dataCenter)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		defer func() {
			// delete users, its roles and permissions
			ginkgo.By("Delete testUser1, its roles and permissions")
			err = deleteUsersRolesAndPermissions(loginCmd, sshClientConfig, masterIp,
				configSecretTestUser1, dataCenter, cluster)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			ginkgo.By("Delete testUser2, its roles and permissions")
			err = deleteUsersRolesAndPermissions(loginCmd, sshClientConfig, masterIp,
				configSecretTestUser2, dataCenter, cluster)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())

			// Create csi-vsphere.conf with original vcenter user and its credentails
			ginkgo.By("Create csi-vsphere.conf with original vcenter user and its credentails")
			insecureFlag, clusterId, clusterDistribution,
				dataCenters, vCenterPort = getConfigSecretFileValues(client, ctx)
			err = deleteCsiVsphereConfFile(loginCmd, sshClientConfig, masterIp)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			err = createCsiVsphereConfFile(clusterId, clusterDistribution, vcAddress, insecureFlag,
				vCenterUIUser, vCenterUIPassword, vCenterPort, dataCenters,
				loginCmd, sshClientConfig, masterIp)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}()

		/* Step2: Create csi-vsphere.conf with user1's credentials and create
		vsphere-config-secret in turn using that file */
		ginkgo.By("Fetch vsphere-config-secret file details")
		insecureFlag, clusterId, clusterDistribution,
			dataCenters, vCenterPort = getConfigSecretFileValues(client, ctx)

		// delete csi vsphere conf file
		ginkgo.By("Delete previously created vsphere-config-secret file")
		err = deleteCsiVsphereConfFile(loginCmd, sshClientConfig, masterIp)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		// create csi vsphere conf file with testUser1
		ginkgo.By("Create vsphere-config-secret file with testuser1 credentials")
		testUser = configSecretTestUser1 + "@vsphere.local"
		err = createCsiVsphereConfFile(clusterId, clusterDistribution, vcAddress, insecureFlag,
			testUser, configSecertTestUser1Password, vCenterPort, dataCenters,
			loginCmd, sshClientConfig, masterIp)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		// Step3: Install CSI driver
		ginkgo.By("Install CSI driver")
		err = installCsiDriver(loginCmd, sshClientConfig, masterIp)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		// Step4: Verify we can create a PVC and attach it to pod
		ginkgo.By("Verify we can create a PVC and attach it to pod")
		ginkgo.By("Creating Storage Class")
		storageclass, err := createStorageClass(client, nil, nil, "", "", false, "")
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		defer func() {
			// delete storage class
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
			// delete pvc
			err = fpv.DeletePersistentVolumeClaim(client, pvclaim1.Name, namespace)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}()

		ginkgo.By("Creating pod")
		pod1, err := createPod(client, namespace, nil, []*v1.PersistentVolumeClaim{pvclaim1}, false, "")
		defer func() {
			// delete pod
			ginkgo.By(fmt.Sprintf("Deleting the pod %s in namespace %s", pod1.Name, namespace))
			err = fpod.DeletePodWithWait(client, pod1)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}()

		/* Step5: Change csi-vsphere.conf with user2's credentials and re-create
		vsphere-config-secret in turn using that file */
		// delete csi vsphere conf file
		ginkgo.By("Fetch vsphere-config-secret file details")
		insecureFlag, clusterId, clusterDistribution,
			dataCenters, vCenterPort = getConfigSecretFileValues(client, ctx)

		// delete csi vsphere conf file
		ginkgo.By("Delete previously created vsphere-config-secret file")
		err = deleteCsiVsphereConfFile(loginCmd, sshClientConfig, masterIp)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		// create csi vsphere conf file with testUser1
		ginkgo.By("Create vsphere-config-secret file with testuser2 credentials")
		testUser = configSecretTestUser2 + "@vsphere.local"
		err = createCsiVsphereConfFile(clusterId, clusterDistribution, vcAddress, insecureFlag,
			testUser, configSecertTestUser2Password, vCenterPort, dataCenters,
			loginCmd, sshClientConfig, masterIp)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		// Step6: Verify we can create a PVC and attach it to pod
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
		defer func() {
			// delete pod
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

	ginkgo.It("Change vcenter user, ip and hostname in vsphere config secret file", func() {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		/* Step1: Create two users (user1 and user2) with required roles and privileges
		and with different passwords */
		ginkgo.By("Create 2 test users")
		err = createTestUser(loginCmd, sshClientConfig, masterIp, configSecretTestUser1,
			configSecertTestUser1Password)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		err = createTestUser(loginCmd, sshClientConfig, masterIp, configSecretTestUser2,
			configSecertTestUser2Password)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		// create roles for testuser1
		ginkgo.By("Create roles for testuser1")
		err = createRolesForTestUser(loginCmd, sshClientConfig, masterIp, configSecretTestUser1)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		// create roles for testuser2
		ginkgo.By("Create roles for testuser2")
		err = createRolesForTestUser(loginCmd, sshClientConfig, masterIp, configSecretTestUser2)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Fetch DataCenter, Cluster and hosts details")
		dataCenter, cluster, err = getDataCenterClusterHostAndVmDetails(loginCmd, sshClientConfig,
			masterIp)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Assign required roles and privileges for testuser1 and testuser2")

		// set DataCenter level permission for testuser1 and testuser2
		ginkgo.By("Set DataCenter level permission for testuser1 and testuser2")
		err = setDataCenterLevelPermission(loginCmd, sshClientConfig, masterIp, dataCenter,
			configSecretTestUser1)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		err = setDataCenterLevelPermission(loginCmd, sshClientConfig, masterIp, dataCenter,
			configSecretTestUser2)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		// set hosts level permission for testuser1 and testuser2
		ginkgo.By("Set hosts level permission for testuser1 and testuser2")
		err = setHostLevelPermission(loginCmd, sshClientConfig, masterIp, configSecretTestUser1)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		err = setHostLevelPermission(loginCmd, sshClientConfig, masterIp, configSecretTestUser2)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		// set K8s VM level permission for testuser1 and testuser2
		ginkgo.By("Set k8s VM level permission for testuser1 and testuser2")
		err = setK8sVMLevelPermission(loginCmd, sshClientConfig, masterIp, configSecretTestUser1)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		err = setK8sVMLevelPermission(loginCmd, sshClientConfig, masterIp, configSecretTestUser2)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		// set cluster level permission for testuser1 and testuser2
		ginkgo.By("Set cluster level permission for testuser1 and testuser2")
		err = setClusterLevelPermission(loginCmd, sshClientConfig, masterIp, configSecretTestUser1,
			cluster)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		err = setClusterLevelPermission(loginCmd, sshClientConfig, masterIp, configSecretTestUser2,
			cluster)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		// set datastore level permission for testuser1 and testuser2
		ginkgo.By("Set datastore level permission for testuser1 and testuser2")
		err = setDataStoreLevelPermission(loginCmd, sshClientConfig, masterIp, configSecretTestUser1,
			dataCenter)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		err = setDataStoreLevelPermission(loginCmd, sshClientConfig, masterIp, configSecretTestUser2,
			dataCenter)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		defer func() {
			// delete users, its roles and permissions
			ginkgo.By("Delete testUser1, its roles and permissions")
			err = deleteUsersRolesAndPermissions(loginCmd, sshClientConfig, masterIp,
				configSecretTestUser1, dataCenter, cluster)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			ginkgo.By("Delete testUser2, its roles and permissions")
			err = deleteUsersRolesAndPermissions(loginCmd, sshClientConfig, masterIp,
				configSecretTestUser2, dataCenter, cluster)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())

			// Create csi-vsphere.conf with original vcenter user and its credentails
			ginkgo.By("Create csi-vsphere.conf with original vcenter user and its credentails")
			insecureFlag, clusterId, clusterDistribution,
				dataCenters, vCenterPort = getConfigSecretFileValues(client, ctx)
			err = deleteCsiVsphereConfFile(loginCmd, sshClientConfig, masterIp)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			err = createCsiVsphereConfFile(clusterId, clusterDistribution, vcAddress, insecureFlag,
				vCenterUIUser, vCenterUIPassword, vCenterPort, dataCenters,
				loginCmd, sshClientConfig, masterIp)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}()

		/* Step2: Create csi-vsphere.conf with user1's credentials and VC IP and then
		create vsphere-config-secret in turn using that file */
		ginkgo.By("Fetch vsphere-config-secret file details")
		insecureFlag, clusterId, clusterDistribution,
			dataCenters, vCenterPort = getConfigSecretFileValues(client, ctx)

		// delete csi vsphere conf file
		ginkgo.By("Delete previously created vsphere-config-secret file")
		err = deleteCsiVsphereConfFile(loginCmd, sshClientConfig, masterIp)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		// create csi vsphere conf file with testUser1
		ginkgo.By("Create vsphere-config-secret file with testuser1 credentials " +
			"and by providing vcenter IP")
		testUser = configSecretTestUser1 + "@vsphere.local"
		err = createCsiVsphereConfFile(clusterId, clusterDistribution, vcAddress, insecureFlag,
			testUser, configSecertTestUser1Password, vCenterPort, dataCenters,
			loginCmd, sshClientConfig, masterIp)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		// Step3 : Install CSI driver
		ginkgo.By("Install CSI driver")
		err = installCsiDriver(loginCmd, sshClientConfig, masterIp)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		// Step4 : Verify we can create a PVC and attach it to pod
		ginkgo.By("Verify we can create a PVC and attach it to pod")
		ginkgo.By("Creating Storage Class")
		storageclass, err := createStorageClass(client, nil, nil, "", "", false, "")
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		defer func() {
			// delete storage class
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
			// delete pvc
			err = fpv.DeletePersistentVolumeClaim(client, pvclaim1.Name, namespace)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}()

		ginkgo.By("Creating pod")
		pod1, err := createPod(client, namespace, nil, []*v1.PersistentVolumeClaim{pvclaim1}, false, "")
		defer func() {
			// delete pod
			ginkgo.By(fmt.Sprintf("Deleting the pod %s in namespace %s", pod1.Name, namespace))
			err = fpod.DeletePodWithWait(client, pod1)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}()

		/* Step5: Change csi-vsphere.conf to use VC hostname instead of VC IP and
		re-create vsphere-config-secret in turn using that file */
		ginkgo.By("Fetch vsphere-config-secret file details")
		insecureFlag, clusterId, clusterDistribution,
			dataCenters, vCenterPort = getConfigSecretFileValues(client, ctx)

		// Get vcenter hotsname
		ginkgo.By("Get vcenter hotsname")
		vCenterHostName, err := getVcenterHostName(sshClientConfig, masterIp, vCenterIp)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		// delete csi vsphere conf file
		ginkgo.By("Delete previously created vsphere-config-secret file")
		err = deleteCsiVsphereConfFile(loginCmd, sshClientConfig, masterIp)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		// create csi vsphere conf file with testUser1
		ginkgo.By("Create vsphere-config-secret file with testuser1 credentials " +
			"and by providing vcenter hostname")
		err = createCsiVsphereConfFile(clusterId, clusterDistribution, vCenterHostName, insecureFlag,
			testUser, configSecertTestUser1Password, vCenterPort, dataCenters,
			loginCmd, sshClientConfig, masterIp)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		// Step6 : Verify we can create a PVC and attach it to pod
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
		defer func() {
			// delete pod
			ginkgo.By(fmt.Sprintf("Deleting the pod %s in namespace %s", pod2.Name, namespace))
			err = fpod.DeletePodWithWait(client, pod2)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}()

		/* Step7: Change csi-vsphere.conf to use VC IP instead of VC hostname and
		re-create vsphere-config-secret in turn using that file */
		ginkgo.By("Fetch vsphere-config-secret file details")
		insecureFlag, clusterId, clusterDistribution,
			dataCenters, vCenterPort = getConfigSecretFileValues(client, ctx)

		// delete csi vsphere conf file
		ginkgo.By("Delete previously created vsphere-config-secret file")
		err = deleteCsiVsphereConfFile(loginCmd, sshClientConfig, masterIp)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		// create csi vsphere conf file with testUser1
		ginkgo.By("Create vsphere-config-secret file with testuser1 credentials " +
			"and reverting vcenter hostname to vcenter IP")
		err = createCsiVsphereConfFile(clusterId, clusterDistribution, vcAddress, insecureFlag,
			testUser, configSecertTestUser1Password, vCenterPort, dataCenters,
			loginCmd, sshClientConfig, masterIp)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		// Step8 : Verify we can create a PVC and attach it to pod
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

	ginkgo.It("Change vcenter user to wrong user and switch back to correct user "+
		"in vsphere config secret file", func() {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		// Step1: Create a user, user1, with required roles and privileges
		ginkgo.By("Create 2 test users")
		err = createTestUser(loginCmd, sshClientConfig, masterIp, configSecretTestUser1,
			configSecertTestUser1Password)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		err = createTestUser(loginCmd, sshClientConfig, masterIp, configSecretTestUser2,
			configSecertTestUser2Password)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		// create roles for testuser1
		ginkgo.By("Create roles for testuser1")
		err = createRolesForTestUser(loginCmd, sshClientConfig, masterIp, configSecretTestUser1)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		// create roles for testuser2
		ginkgo.By("Create roles for testuser2")
		err = createRolesForTestUser(loginCmd, sshClientConfig, masterIp, configSecretTestUser2)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Fetch DataCenter, Cluster and hosts details")
		dataCenter, cluster, err = getDataCenterClusterHostAndVmDetails(loginCmd, sshClientConfig,
			masterIp)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Assign required roles and privileges for testuser1 and testuser2")

		// set DataCenter level permission for testuser1 and testuser2
		ginkgo.By("Set DataCenter level permission for testuser1 and testuser2")
		err = setDataCenterLevelPermission(loginCmd, sshClientConfig, masterIp, dataCenter,
			configSecretTestUser1)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		err = setDataCenterLevelPermission(loginCmd, sshClientConfig, masterIp, dataCenter,
			configSecretTestUser2)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		// set hosts level permission for testuser1 and testuser2
		ginkgo.By("Set hosts level permission for testuser1 and testuser2")
		err = setHostLevelPermission(loginCmd, sshClientConfig, masterIp, configSecretTestUser1)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		err = setHostLevelPermission(loginCmd, sshClientConfig, masterIp, configSecretTestUser2)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		// set K8s VM level permission for testuser1 and testuser2
		ginkgo.By("Set k8s VM level permission for testuser1 and testuser2")
		err = setK8sVMLevelPermission(loginCmd, sshClientConfig, masterIp, configSecretTestUser1)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		err = setK8sVMLevelPermission(loginCmd, sshClientConfig, masterIp, configSecretTestUser2)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		// set cluster level permission for testuser1 and testuser2
		ginkgo.By("Set cluster level permission for testuser1 and testuser2")
		err = setClusterLevelPermission(loginCmd, sshClientConfig, masterIp,
			configSecretTestUser1, cluster)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		err = setClusterLevelPermission(loginCmd, sshClientConfig, masterIp,
			configSecretTestUser2, cluster)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		// set datastore level permission for testuser1 and testuser2
		ginkgo.By("Set datastore level permission for testuser1 and testuser2")
		err = setDataStoreLevelPermission(loginCmd, sshClientConfig, masterIp,
			configSecretTestUser1, dataCenter)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		err = setDataStoreLevelPermission(loginCmd, sshClientConfig, masterIp,
			configSecretTestUser2, dataCenter)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		defer func() {
			// delete users, its roles and permissions
			ginkgo.By("Delete testUser1, its roles and permissions")
			err = deleteUsersRolesAndPermissions(loginCmd, sshClientConfig, masterIp,
				configSecretTestUser1, dataCenter, cluster)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			ginkgo.By("Delete testUser2, its roles and permissions")
			err = deleteUsersRolesAndPermissions(loginCmd, sshClientConfig, masterIp,
				configSecretTestUser2, dataCenter, cluster)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())

			// Create csi-vsphere.conf with original vcenter user and its credentails
			ginkgo.By("Create csi-vsphere.conf with original vcenter user and its credentails")
			insecureFlag, clusterId, clusterDistribution,
				dataCenters, vCenterPort = getConfigSecretFileValues(client, ctx)
			err = deleteCsiVsphereConfFile(loginCmd, sshClientConfig, masterIp)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			err = createCsiVsphereConfFile(clusterId, clusterDistribution, vcAddress, insecureFlag,
				vCenterUIUser, vCenterUIPassword, vCenterPort, dataCenters,
				loginCmd, sshClientConfig, masterIp)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}()

		/* Step2 : Create csi-vsphere.conf with user1's credentials and
		create vsphere-config-secret in turn using that file */
		ginkgo.By("Fetch vsphere-config-secret file details")
		insecureFlag, clusterId, clusterDistribution,
			dataCenters, vCenterPort = getConfigSecretFileValues(client, ctx)

		// delete csi vsphere conf file
		ginkgo.By("Delete previously created vsphere-config-secret file")
		err = deleteCsiVsphereConfFile(loginCmd, sshClientConfig, masterIp)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		// create csi vsphere conf file with testUser1
		ginkgo.By("Create vsphere-config-secret file with testuser1 credentials")
		testUser = configSecretTestUser1 + "@vsphere.local"
		err = createCsiVsphereConfFile(clusterId, clusterDistribution, vcAddress, insecureFlag,
			testUser, configSecertTestUser1Password, vCenterPort, dataCenters,
			loginCmd, sshClientConfig, masterIp)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		// Step3: Install CSI driver
		ginkgo.By("Install CSI driver")
		err = installCsiDriver(loginCmd, sshClientConfig, masterIp)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		// Step4: Verify we can create a PVC and attach it to pod
		ginkgo.By("Verify we can create a PVC and attach it to pod")
		ginkgo.By("Creating Storage Class")
		storageclass, err := createStorageClass(client, nil, nil, "", "", false, "")
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		defer func() {
			// delete storage class
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
			// delete pvc
			err = fpv.DeletePersistentVolumeClaim(client, pvclaim1.Name, namespace)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}()

		ginkgo.By("Creating pod")
		pod1, err := createPod(client, namespace, nil, []*v1.PersistentVolumeClaim{pvclaim1}, false, "")
		defer func() {
			// delete pod
			ginkgo.By(fmt.Sprintf("Deleting the pod %s in namespace %s", pod1.Name, namespace))
			err = fpod.DeletePodWithWait(client, pod1)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}()

		/* Step5: Change csi-vsphere.conf and add a dummy user and re-create
		vsphere-config-secret in turn using that file */
		ginkgo.By("Fetch vsphere-config-secret file details")
		insecureFlag, clusterId, clusterDistribution,
			dataCenters, vCenterPort = getConfigSecretFileValues(client, ctx)

		// delete csi vsphere conf file
		ginkgo.By("Delete previously created vsphere-config-secret file")
		err = deleteCsiVsphereConfFile(loginCmd, sshClientConfig, masterIp)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		// create csi vsphere conf file with dummy user
		ginkgo.By("Create vsphere-config-secret file with dummy user credentials")
		testUser = "dummyUser@vsphere.local"
		err = createCsiVsphereConfFile(clusterId, clusterDistribution, vcAddress, insecureFlag,
			testUser, configSecertTestUser1Password, vCenterPort, dataCenters,
			loginCmd, sshClientConfig, masterIp)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		// Step6: Verify we can create a PVC and attach it to pod
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
		defer func() {
			// delete pod
			ginkgo.By(fmt.Sprintf("Deleting the pod %s in namespace %s", pod2.Name, namespace))
			err = fpod.DeletePodWithWait(client, pod2)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}()

		/* Step7: Change csi-vsphere.conf with updated user1's credentials and
		re-create vsphere-config-secret in turn using that file */
		ginkgo.By("Fetch vsphere-config-secret file details")
		insecureFlag, clusterId, clusterDistribution,
			dataCenters, vCenterPort = getConfigSecretFileValues(client, ctx)

		// delete csi vsphere conf file
		ginkgo.By("Delete previously created vsphere-config-secret file")
		err = deleteCsiVsphereConfFile(loginCmd, sshClientConfig, masterIp)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		// create csi vsphere conf file with testuser1
		ginkgo.By("Create vsphere-config-secret file with testuser1 credentials")
		testUser = configSecretTestUser1 + "@vsphere.local"
		err = createCsiVsphereConfFile(clusterId, clusterDistribution, vcAddress, insecureFlag,
			testUser, configSecertTestUser1Password, vCenterPort, dataCenters,
			loginCmd, sshClientConfig, masterIp)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		// Step8: Verify we can create a PVC and attach it to pod
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
		defer func() {
			// delete pod
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

	ginkgo.It("Change vcenter datacenter to dummy value and switch back to correct datacenter "+
		"in vsphere config secret file", func() {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		// Step1: Create a user, user1, with required roles and privileges
		ginkgo.By("Create 2 test users")
		err = createTestUser(loginCmd, sshClientConfig, masterIp, configSecretTestUser1,
			configSecertTestUser1Password)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		err = createTestUser(loginCmd, sshClientConfig, masterIp, configSecretTestUser2,
			configSecertTestUser2Password)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		// create roles for testuser1
		ginkgo.By("Create roles for testuser1")
		err = createRolesForTestUser(loginCmd, sshClientConfig, masterIp, configSecretTestUser1)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		// create roles for testuser2
		ginkgo.By("Create roles for testuser2")
		err = createRolesForTestUser(loginCmd, sshClientConfig, masterIp, configSecretTestUser2)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Fetch DataCenter, Cluster and hosts details")
		dataCenter, cluster, err = getDataCenterClusterHostAndVmDetails(loginCmd, sshClientConfig,
			masterIp)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Assign required roles and privileges for testuser1 and testuser2")

		// set DataCenter level permission for testuser1 and testuser2
		ginkgo.By("Set DataCenter level permission for testuser1 and testuser2")
		err = setDataCenterLevelPermission(loginCmd, sshClientConfig, masterIp, dataCenter,
			configSecretTestUser1)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		err = setDataCenterLevelPermission(loginCmd, sshClientConfig, masterIp, dataCenter,
			configSecretTestUser2)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		// set hosts level permission for testuser1 and testuser2
		ginkgo.By("Set hosts level permission for testuser1 and testuser2")
		err = setHostLevelPermission(loginCmd, sshClientConfig, masterIp, configSecretTestUser1)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		err = setHostLevelPermission(loginCmd, sshClientConfig, masterIp, configSecretTestUser2)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		// set K8s VM level permission for testuser1 and testuser2
		ginkgo.By("Set k8s VM level permission for testuser1 and testuser2")
		err = setK8sVMLevelPermission(loginCmd, sshClientConfig, masterIp, configSecretTestUser1)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		err = setK8sVMLevelPermission(loginCmd, sshClientConfig, masterIp, configSecretTestUser2)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		// set cluster level permission for testuser1 and testuser2
		ginkgo.By("Set cluster level permission for testuser1 and testuser2")
		err = setClusterLevelPermission(loginCmd, sshClientConfig, masterIp,
			configSecretTestUser1, cluster)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		err = setClusterLevelPermission(loginCmd, sshClientConfig, masterIp,
			configSecretTestUser2, cluster)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		// set datastore level permission for testuser1 and testuser2
		ginkgo.By("Set datastore level permission for testuser1 and testuser2")
		err = setDataStoreLevelPermission(loginCmd, sshClientConfig, masterIp,
			configSecretTestUser1, dataCenter)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		err = setDataStoreLevelPermission(loginCmd, sshClientConfig, masterIp,
			configSecretTestUser2, dataCenter)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		defer func() {
			// delete users, its roles and permissions
			ginkgo.By("Delete testUser1, its roles and permissions")
			err = deleteUsersRolesAndPermissions(loginCmd, sshClientConfig, masterIp,
				configSecretTestUser1, dataCenter, cluster)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			ginkgo.By("Delete testUser2, its roles and permissions")
			err = deleteUsersRolesAndPermissions(loginCmd, sshClientConfig, masterIp,
				configSecretTestUser2, dataCenter, cluster)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())

			// Create csi-vsphere.conf with original vcenter user and its credentails
			ginkgo.By("Create csi-vsphere.conf with original vcenter user and its credentails")
			insecureFlag, clusterId, clusterDistribution,
				dataCenters, vCenterPort = getConfigSecretFileValues(client, ctx)
			err = deleteCsiVsphereConfFile(loginCmd, sshClientConfig, masterIp)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			err = createCsiVsphereConfFile(clusterId, clusterDistribution, vcAddress, insecureFlag,
				vCenterUIUser, vCenterUIPassword, vCenterPort, dataCenters,
				loginCmd, sshClientConfig, masterIp)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}()

		/* Step2 : Create csi-vsphere.conf with user1's credentials and
		create vsphere-config-secret in turn using that file */
		ginkgo.By("Fetch vsphere-config-secret file details")
		insecureFlag, clusterId, clusterDistribution,
			dataCenters, vCenterPort = getConfigSecretFileValues(client, ctx)
		correctDataCenter := dataCenter

		// delete csi vsphere conf file
		ginkgo.By("Delete previously created vsphere-config-secret file")
		err = deleteCsiVsphereConfFile(loginCmd, sshClientConfig, masterIp)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		// create csi vsphere conf file with testUser1
		ginkgo.By("Create vsphere-config-secret file with testuser1 credentials " +
			"and provide correct datacenter details")
		testUser = configSecretTestUser1 + "@vsphere.local"
		err = createCsiVsphereConfFile(clusterId, clusterDistribution, vcAddress, insecureFlag,
			testUser, configSecertTestUser1Password, vCenterPort, dataCenters,
			loginCmd, sshClientConfig, masterIp)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		// Step3: Install CSI driver
		ginkgo.By("Install CSI driver")
		err = installCsiDriver(loginCmd, sshClientConfig, masterIp)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		// Step4: Verify we can create a PVC and attach it to pod
		ginkgo.By("Verify we can create a PVC and attach it to pod")
		ginkgo.By("Creating Storage Class")
		storageclass, err := createStorageClass(client, nil, nil, "", "", false, "")
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		defer func() {
			// delete storage class
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
			// delete pvc
			err = fpv.DeletePersistentVolumeClaim(client, pvclaim1.Name, namespace)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}()

		ginkgo.By("Creating pod")
		pod1, err := createPod(client, namespace, nil, []*v1.PersistentVolumeClaim{pvclaim1}, false, "")
		defer func() {
			// delete pod
			ginkgo.By(fmt.Sprintf("Deleting the pod %s in namespace %s", pod1.Name, namespace))
			err = fpod.DeletePodWithWait(client, pod1)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}()

		/* Step5: Change csi-vsphere.conf and add a dummy datacenter and
		re-create vsphere-config-secret in turn using that file */
		ginkgo.By("Fetch vsphere-config-secret file details")
		insecureFlag, clusterId, clusterDistribution,
			dataCenters, vCenterPort = getConfigSecretFileValues(client, ctx)

		// delete csi vsphere conf file
		ginkgo.By("Delete previously created vsphere-config-secret file")
		err = deleteCsiVsphereConfFile(loginCmd, sshClientConfig, masterIp)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		// create csi vsphere conf file with dummy dataCenter
		ginkgo.By("Create vsphere-config-secret file with dummy datacenter")
		dummyDataCenter := "dummy-data-center"
		err = createCsiVsphereConfFile(clusterId, clusterDistribution, vcAddress, insecureFlag,
			testUser, configSecertTestUser1Password, vCenterPort, dummyDataCenter,
			loginCmd, sshClientConfig, masterIp)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		// Step6: Try to create a PVC verify that it is stuck in pending state
		ginkgo.By("Try to create a PVC verify that it is stuck in pending state")
		pvc1, err := createPVC(client, namespace, nil, "", storageclass, "")
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		ginkgo.By("Expect claim status to be in Pending state")
		err = fpv.WaitForPersistentVolumeClaimPhase(v1.ClaimPending, client,
			pvc1.Namespace, pvc1.Name, framework.Poll, time.Minute)
		gomega.Expect(err).NotTo(gomega.HaveOccurred(),
			fmt.Sprintf("Failed to find the volume in pending state with err: %v", err))
		defer func() {
			// delete pvc
			err = fpv.DeletePersistentVolumeClaim(client, pvc1.Name, namespace)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}()

		/* Step7: Change csi-vsphere.conf with updated datacenter and
		re-create vsphere-config-secret in turn using that file */
		ginkgo.By("Fetch vsphere-config-secret file details")
		insecureFlag, clusterId, clusterDistribution,
			dataCenters, vCenterPort = getConfigSecretFileValues(client, ctx)

		// delete csi vsphere conf file
		ginkgo.By("Delete previously created vsphere-config-secret file")
		err = deleteCsiVsphereConfFile(loginCmd, sshClientConfig, masterIp)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		// create csi vsphere conf file with testuser1 and with correct data center
		ginkgo.By("Create vsphere-config-secret file with correct data center details")
		err = createCsiVsphereConfFile(clusterId, clusterDistribution, vcAddress, insecureFlag,
			testUser, configSecertTestUser1Password, vCenterPort, correctDataCenter,
			loginCmd, sshClientConfig, masterIp)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		// Step8: Verify we can create a PVC and attach it to pod
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
		defer func() {
			// delete pod
			ginkgo.By(fmt.Sprintf("Deleting the pod %s in namespace %s", pod2.Name, namespace))
			err = fpod.DeletePodWithWait(client, pod2)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}()

		// Step9: Verify the PVC which was stuck in Pening state should gets bound eventually
		ginkgo.By("Verify the PVC which was stuck in Pening state should gets bound eventually")
		pvc1, err = client.CoreV1().PersistentVolumeClaims(namespace).Get(ctx, pvc1.Name, metav1.GetOptions{})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		gomega.Expect(pvc1.Status.Phase == v1.ClaimBound).To(gomega.BeTrue())
	})
})
