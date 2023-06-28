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
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	clientset "k8s.io/client-go/kubernetes"
	"k8s.io/kubernetes/test/e2e/framework"
	fnodes "k8s.io/kubernetes/test/e2e/framework/node"
	admissionapi "k8s.io/pod-security-admission/api"
)

var _ = ginkgo.Describe("Config-Secret", func() {
	f := framework.NewDefaultFramework("config-secret-changes")
	f.NamespacePodSecurityEnforceLevel = admissionapi.LevelPrivileged
	var (
		client    clientset.Interface
		namespace string

		configSecretUser1Alias string
		configSecretUser2Alias string
		csiNamespace           string
		csiReplicas            int32
		vCenterUIUser          string
		vCenterUIPassword      string

		revertOriginalvCenterUser bool
		vCenterIP                 string
		vCenterPort               string
		dataCenter                string
	)

	ginkgo.BeforeEach(func() {
		var cancel context.CancelFunc
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		client = f.ClientSet
		namespace = f.Namespace.Name
		multiVCbootstrap()
		nodeList, err := fnodes.GetReadySchedulableNodes(f.ClientSet)
		framework.ExpectNoError(err, "Unable to find ready and schedulable Node")
		if !(len(nodeList.Items) > 0) {
			framework.Failf("Unable to find ready and schedulable Node")
		}

		secret, err := client.CoreV1().Secrets(csiSystemNamespace).Get(ctx, configSecret, metav1.GetOptions{})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		originalConf := string(secret.Data[vSphereCSIConf])
		config, err := readConfigFromSecretString(originalConf)
		fmt.Println(config)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

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

	/* TESTCASE-1
		Change VC password on one of the VC, also update the vsphere-csi-secret

		// Steps
	    1. Create SC with all topologies present in allowed Topology  list
	    2. Change the VC UI password for any one VC, Update the same in "CSI config secret" file and re-create the
		secret
		3. Restart CSI driver
	    4. Wait for some time, CSI will auto identify the change in vsphere-secret file and get updated
	    5. Create  statefulset.
		6. PVCs ad Pods should be in bound and running state.
		7. Make sure all the common verification points are met
	        a) Verify node affinity on all the PV's
	        b) Verify that POD should be up and running on the appropriate nodes
	    8. Scale up/down the statefullset
	    9. Clean up the data
	*/

	ginkgo.It("TestConfigMulti", func() {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		ginkgo.By("Create StorageClass with no allowed topolgies")
		scSpec := getVSphereStorageClassSpec(defaultNginxStorageClassName, nil, nil, "",
			"", false)
		sc, err := client.StorageV1().StorageClasses().Create(ctx, scSpec, metav1.CreateOptions{})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		defer func() {
			err = client.StorageV1().StorageClasses().Delete(ctx, sc.Name, *metav1.NewDeleteOptions(0))
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}()

		ginkgo.By("Fetching the username and password of the current vcenter session from secret")
		secret, err := client.CoreV1().Secrets(csiSystemNamespace).Get(ctx, configSecret, metav1.GetOptions{})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		originalConf := string(secret.Data[vSphereCSIConf])
		vsphereCfg, err := readConfigFromSecretString(originalConf)
		fmt.Println(vsphereCfg)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By(fmt.Sprintln("Changing password on the vCenter host"))
		vCenterHostname := strings.Split(multiVCe2eVSphere.multivcConfig.Global.VCenterHostname, ",")
		vcAddress := vCenterHostname[0] + ":" + sshdPort
		vCenterUserName := strings.Split(multiVCe2eVSphere.multivcConfig.Global.User, ",")
		username := vCenterUserName[0]
		vCenterPwd := strings.Split(multiVCe2eVSphere.multivcConfig.Global.Password, ",")
		originalPassword := vCenterPwd[0]
		newPassword := e2eTestPassword
		err = invokeVCenterChangePassword(username, originalPassword, newPassword, vcAddress)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		originalVCPasswordChanged := true
		defer func() {
			if originalVCPasswordChanged {
				ginkgo.By("Reverting the password change")
				err = invokeVCenterChangePassword(username, newPassword, originalPassword, vcAddress)
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
			}
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

})
