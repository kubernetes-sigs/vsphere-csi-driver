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

	ginkgo "github.com/onsi/ginkgo/v2"
	"github.com/onsi/gomega"
	"github.com/vmware/govmomi/object"
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
		propagateVal = "false"
		revertOriginalvCenterUser = false
		configSecretUser1Alias = configSecretTestUser1 + "@vsphere.local"
		configSecretUser2Alias = configSecretTestUser2 + "@vsphere.local"

		// fetching datacenter, cluster, host details
		dataCenters, clusters, hosts, vms, datastores = getDataCenterClusterHostAndVmDetails(ctx, masterIp)

		ginkgo.By("Delete roles, permissions for testuser1 if already exist")
		deleteTestUserAndRemoveRolesPrivileges(masterIp, configSecretTestUser1,
			configSecretTestUser1Password, configSecretUser1Alias, propagateVal,
			dataCenters, clusters, hosts, vms, datastores)

		ginkgo.By("Delete roles, permissions for testuser2 if already exist")
		deleteTestUserAndRemoveRolesPrivileges(masterIp, configSecretTestUser2,
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
			createCsiVsphereSecret(client, ctx, vCenterUIUser, vCenterUIPassword, csiNamespace)

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
		createTestUserAndAssignRolesPrivileges(masterIp, configSecretTestUser1,
			configSecretTestUser1Password, configSecretUser1Alias, propagateVal,
			dataCenters, clusters, hosts, vms, datastores)
		defer func() {
			ginkgo.By("Delete testuser1 and remove assigned roles and privileges to testuser1")
			deleteTestUserAndRemoveRolesPrivileges(masterIp, configSecretTestUser1,
				configSecretTestUser1Password, configSecretUser1Alias, propagateVal,
				dataCenters, clusters, hosts, vms, datastores)
		}()

		ginkgo.By("Create testuser2 and assign required roles and privileges to testuser2")
		createTestUserAndAssignRolesPrivileges(masterIp, configSecretTestUser2,
			configSecretTestUser1Password, configSecretUser2Alias, propagateVal,
			dataCenters, clusters, hosts, vms, datastores)
		defer func() {
			ginkgo.By("Delete testuser2 and remove assigned roles and privileges to testuser2")
			deleteTestUserAndRemoveRolesPrivileges(masterIp, configSecretTestUser2,
				configSecretTestUser1Password, configSecretUser2Alias, propagateVal,
				dataCenters, clusters, hosts, vms, datastores)
		}()

		ginkgo.By("Create vsphere-config-secret file with testuser1 credentials")
		createCsiVsphereSecret(client, ctx, configSecretUser1Alias, configSecretTestUser1Password, csiNamespace)

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

		ginkgo.By("Creating PVC")
		pvclaim1, err := createPVC(client, namespace, nil, "", storageclass, "")
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		var pvclaims1 []*v1.PersistentVolumeClaim
		pvclaims1 = append(pvclaims1, pvclaim1)
		ginkgo.By("Waiting for all claims to be in bound state")
		pvs1, err := fpv.WaitForPVClaimBoundPhase(client, pvclaims1, framework.ClaimProvisionTimeout)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		gomega.Expect(pvs1).NotTo(gomega.BeEmpty())
		pv1 := pvs1[0]
		defer func() {
			err = fpv.DeletePersistentVolumeClaim(client, pvclaim1.Name, namespace)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			ginkgo.By("Verify PVs, volumes are deleted from CNS")
			err = e2eVSphere.waitForCNSVolumeToBeDeleted(pv1.Spec.CSI.VolumeHandle)
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

		ginkgo.By("Verify volume metadata for POD, PVC and PV")
		err = waitAndVerifyCnsVolumeMetadata(pv1.Spec.CSI.VolumeHandle, pvclaim1, pv1, pod1)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Create vsphere-config-secret file with testuser2 credentials")
		createCsiVsphereSecret(client, ctx, configSecretUser2Alias, configSecretTestUser1Password, csiNamespace)
		defer func() {
			ginkgo.By("Reverting back csi-vsphere.conf with its original vcenter user " +
				"and its credentials")
			createCsiVsphereSecret(client, ctx, vCenterUIUser, vCenterUIPassword, csiNamespace)
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
		ginkgo.By("Creating Pvc")
		pvclaim2, err := createPVC(client, namespace, nil, "", storageclass, "")
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		var pvclaims2 []*v1.PersistentVolumeClaim
		pvclaims2 = append(pvclaims2, pvclaim2)
		ginkgo.By("Waiting for all claims to be in bound state")
		pvs2, err := fpv.WaitForPVClaimBoundPhase(client, pvclaims2, framework.ClaimProvisionTimeout)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		gomega.Expect(pvs2).NotTo(gomega.BeEmpty())
		pv2 := pvs2[0]
		defer func() {
			err = fpv.DeletePersistentVolumeClaim(client, pvclaim2.Name, namespace)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())

			ginkgo.By("Verify PVs, volumes are deleted from CNS")
			err = fpv.WaitForPersistentVolumeDeleted(client, pv2.Name, poll, pollTimeout)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			err = e2eVSphere.waitForCNSVolumeToBeDeleted(pv2.Spec.CSI.VolumeHandle)
			gomega.Expect(err).NotTo(gomega.HaveOccurred(),
				"Volume: %s should not be present in the CNS after it is deleted from "+
					"kubernetes", pv2.Spec.CSI.VolumeHandle)
		}()

		ginkgo.By("Creating pod")
		pod2, err := createPod(client, namespace, nil, []*v1.PersistentVolumeClaim{pvclaim2}, false, "")
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		defer func() {
			ginkgo.By(fmt.Sprintf("Deleting the pod %s in namespace %s", pod2.Name, namespace))
			err = fpod.DeletePodWithWait(client, pod2)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}()

		ginkgo.By("Verify volume metadata for POD, PVC and PV")
		err = waitAndVerifyCnsVolumeMetadata(pv2.Spec.CSI.VolumeHandle, pvclaim2, pv2, pod2)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
	})
})
