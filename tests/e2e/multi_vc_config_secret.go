/*
	Copyright 2023 The Kubernetes Authors.

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
	"sync"
	"time"

	"github.com/onsi/ginkgo/v2"
	"github.com/onsi/gomega"
	"golang.org/x/crypto/ssh"
	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	clientset "k8s.io/client-go/kubernetes"
	"k8s.io/kubernetes/test/e2e/framework"
	e2ekubectl "k8s.io/kubernetes/test/e2e/framework/kubectl"
	fnodes "k8s.io/kubernetes/test/e2e/framework/node"
	fpod "k8s.io/kubernetes/test/e2e/framework/pod"
	fpv "k8s.io/kubernetes/test/e2e/framework/pv"
	fss "k8s.io/kubernetes/test/e2e/framework/statefulset"
	admissionapi "k8s.io/pod-security-admission/api"
)

var _ = ginkgo.Describe("[multivc-configsecret] MultiVc-ConfigSecret", func() {
	f := framework.NewDefaultFramework("multivc-configsecret")
	f.NamespacePodSecurityEnforceLevel = admissionapi.LevelPrivileged
	var (
		client                      clientset.Interface
		namespace                   string
		csiNamespace                string
		csiReplicas                 int32
		podAntiAffinityToSet        bool
		stsScaleUp                  bool
		stsScaleDown                bool
		verifyTopologyAffinity      bool
		allowedTopologies           []v1.TopologySelectorLabelRequirement
		scaleUpReplicaCount         int32
		scaleDownReplicaCount       int32
		nodeAffinityToSet           bool
		parallelStatefulSetCreation bool
		stsReplicas                 int32
		parallelPodPolicy           bool
		originalVC1PasswordChanged  bool
		originalVC3PasswordChanged  bool
		vCenterIP                   string
		vCenterUser                 string
		vCenterPassword             string
		vCenterPort                 string
		dataCenter                  string
		err                         error
		revertToOriginalVsphereConf bool
		multiVCSetupType            string
		allMasterIps                []string
		sshClientConfig             *ssh.ClientConfig
		nimbusGeneratedK8sVmPwd     string
		revertToOriginalCsiYaml     bool
		newNamespace                *v1.Namespace
	)

	ginkgo.BeforeEach(func() {
		var cancel context.CancelFunc
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		client = f.ClientSet
		namespace = f.Namespace.Name
		multiVCbootstrap()

		sc, err := client.StorageV1().StorageClasses().Get(ctx, defaultNginxStorageClassName, metav1.GetOptions{})
		if err == nil && sc != nil {
			gomega.Expect(client.StorageV1().StorageClasses().Delete(ctx, sc.Name,
				*metav1.NewDeleteOptions(0))).NotTo(gomega.HaveOccurred())
		}

		nodeList, err := fnodes.GetReadySchedulableNodes(ctx, f.ClientSet)
		framework.ExpectNoError(err, "Unable to find ready and schedulable Node")
		if !(len(nodeList.Items) > 0) {
			framework.Failf("Unable to find ready and schedulable Node")
		}

		verifyTopologyAffinity = true
		stsScaleUp = true
		stsScaleDown = true

		// read namespace
		csiNamespace = GetAndExpectStringEnvVar(envCSINamespace)
		csiDeployment, err := client.AppsV1().Deployments(csiNamespace).Get(
			ctx, vSphereCSIControllerPodNamePrefix, metav1.GetOptions{})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		csiReplicas = *csiDeployment.Spec.Replicas

		// read testbed topology map
		topologyMap := GetAndExpectStringEnvVar(envTopologyMap)
		allowedTopologies = createAllowedTopolgies(topologyMap)

		// save original vsphere conf credentials in temp variable
		vCenterIP = multiVCe2eVSphere.multivcConfig.Global.VCenterHostname
		vCenterUser = multiVCe2eVSphere.multivcConfig.Global.User
		vCenterPassword = multiVCe2eVSphere.multivcConfig.Global.Password
		vCenterPort = multiVCe2eVSphere.multivcConfig.Global.VCenterPort
		dataCenter = multiVCe2eVSphere.multivcConfig.Global.Datacenters

		// read type of multi-vc setup
		multiVCSetupType = GetAndExpectStringEnvVar(envMultiVCSetupType)
		nimbusGeneratedK8sVmPwd = GetAndExpectStringEnvVar(nimbusK8sVmPwd)

		sshClientConfig = &ssh.ClientConfig{
			User: "root",
			Auth: []ssh.AuthMethod{
				ssh.Password(nimbusGeneratedK8sVmPwd),
			},
			HostKeyCallback: ssh.InsecureIgnoreHostKey(),
		}

		// fetching k8s master ip
		allMasterIps = getK8sMasterIPs(ctx, client)
	})

	ginkgo.AfterEach(func() {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		if revertToOriginalVsphereConf {
			ginkgo.By("Reverting back csi-vsphere.conf with its original vcenter user " +
				"and its credentials")
			vsphereCfg, err := readVsphereConfSecret(client, ctx, csiNamespace)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())

			vsphereCfg.Global.VCenterHostname = vCenterIP
			vsphereCfg.Global.User = vCenterUser
			vsphereCfg.Global.Password = vCenterPassword
			vsphereCfg.Global.VCenterPort = vCenterPort
			vsphereCfg.Global.Datacenters = dataCenter
			err = writeNewDataAndUpdateVsphereConfSecret(client, ctx, csiNamespace, vsphereCfg)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())

			ginkgo.By("Restart CSI driver")
			restartSuccess, err := restartCSIDriver(ctx, client, csiNamespace, csiReplicas)
			gomega.Expect(restartSuccess).To(gomega.BeTrue(), "csi driver restart not successful")
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}

		if originalVC1PasswordChanged {
			clientIndex := 0
			username := strings.Split(vCenterUser, ",")[0]
			originalPassword := strings.Split(vCenterPassword, ",")[0]
			newPassword := e2eTestPassword
			ginkgo.By("Reverting the password change")
			err = invokeVCenterChangePassword(ctx, username, newPassword, originalPassword, vcAddress,
				clientIndex)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}

		if originalVC3PasswordChanged {
			clientIndex2 := 2
			username3 := strings.Split(vCenterUser, ",")[2]
			originalPassword3 := strings.Split(vCenterPassword, ",")[2]
			newPassword3 := "Admin!23"
			ginkgo.By("Reverting the password change")
			err = invokeVCenterChangePassword(ctx, username3, newPassword3, originalPassword3, vcAddress3,
				clientIndex2)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			originalVC3PasswordChanged = false
		}

		if revertToOriginalCsiYaml {
			vsphereCfg, err := readVsphereConfSecret(client, ctx, csiNamespace)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())

			vsphereCfg.Global.VCenterHostname = vCenterIP
			vsphereCfg.Global.User = vCenterUser
			vsphereCfg.Global.Password = vCenterPassword
			vsphereCfg.Global.VCenterPort = vCenterPort
			vsphereCfg.Global.Datacenters = dataCenter

			ginkgo.By("Recreate config secret on a default csi system namespace")
			err = deleteVsphereConfigSecret(client, ctx, newNamespace.Name)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			err = createVsphereConfigSecret(csiNamespace, vsphereCfg, sshClientConfig, allMasterIps)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			ginkgo.By("Revert vsphere CSI driver on a default csi system namespace")
			err = setNewNameSpaceInCsiYaml(ctx, client, sshClientConfig, newNamespace.Name, csiNamespace, allMasterIps)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}
	})

	/* TESTCASE-1
		Change VC password on one of the VC, also update the vsphere-csi-secret

		// Steps
	    1. Create SC with all topologies present in allowed Topology list
	    2. Change the VC UI password for any one VC, Update the same in "CSI config secret" file and re-create the
		secret
		3. Re-start the CSI driver
	    4. Wait for some time, CSI will auto identify the change in vsphere-secret file and get updated
	    5. Create  Statefulset. PVCs and Pods should be in bound and running state.
		6. Make sure all the common verification points are met
	        a) Verify node affinity on all the PV's
	        b) Verify that POD should be up and running on the appropriate nodes
	    7. Scale-up/Scale-down the statefulset
	    8. Clean up the data
	*/

	ginkgo.It("[pq-multivc] Change vCenter password on one of the multi-vc setup and update the same "+
		"in csi vsphere conf", ginkgo.Label(p1, vsphereConfigSecret, block, vanilla,
		multiVc, vc70, flaky), func() {

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		clientIndex := 0
		stsReplicas = 3
		scaleUpReplicaCount = 5
		scaleDownReplicaCount = 2

		ginkgo.By("Create StorageClass with all allowed topolgies set")
		scSpec := getVSphereStorageClassSpec(defaultNginxStorageClassName, nil, allowedTopologies, "",
			"", false)
		sc, err := client.StorageV1().StorageClasses().Create(ctx, scSpec, metav1.CreateOptions{})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		defer func() {
			err = client.StorageV1().StorageClasses().Delete(ctx, sc.Name, *metav1.NewDeleteOptions(0))
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}()

		// read original vsphere config secret
		vsphereCfg, err := readVsphereConfSecret(client, ctx, csiNamespace)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		framework.Logf("vcAddress - %s ", vcAddress)
		username := strings.Split(vsphereCfg.Global.User, ",")[0]
		originalPassword := strings.Split(vsphereCfg.Global.Password, ",")[0]
		newPassword := e2eTestPassword
		ginkgo.By(fmt.Sprintf("Original password %s, new password %s", originalPassword, newPassword))

		ginkgo.By("Changing password on the vCenter VC1 host")
		err = invokeVCenterChangePassword(ctx, username, originalPassword, newPassword, vcAddress, clientIndex)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		originalVC1PasswordChanged = true

		// here 0 indicates that we are updating only VC1 password
		ginkgo.By("Create vsphere-config-secret file with new VC1 password")
		passwordList := strings.Split(vsphereCfg.Global.Password, ",")
		passwordList[0] = newPassword
		vsphereCfg.Global.Password = strings.Join(passwordList, ",")

		// here we are writing new password of VC1 and later updating vsphere config secret
		err = writeNewDataAndUpdateVsphereConfSecret(client, ctx, csiNamespace, vsphereCfg)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		revertToOriginalVsphereConf = true
		defer func() {
			if originalVC1PasswordChanged {
				ginkgo.By("Reverting the password change")
				err = invokeVCenterChangePassword(ctx, username, newPassword, originalPassword, vcAddress,
					clientIndex)
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
				originalVC1PasswordChanged = false
			}

			if revertToOriginalVsphereConf {
				ginkgo.By("Reverting back csi-vsphere.conf with its original vcenter user " +
					"and its credentials")
				vsphereCfg.Global.VCenterHostname = vCenterIP
				vsphereCfg.Global.User = vCenterUser
				vsphereCfg.Global.Password = vCenterPassword
				vsphereCfg.Global.VCenterPort = vCenterPort
				vsphereCfg.Global.Datacenters = dataCenter
				err = writeNewDataAndUpdateVsphereConfSecret(client, ctx, csiNamespace, vsphereCfg)
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
				revertToOriginalVsphereConf = false

				ginkgo.By("Restart CSI driver")
				restartSuccess, err := restartCSIDriver(ctx, client, csiNamespace, csiReplicas)
				gomega.Expect(restartSuccess).To(gomega.BeTrue(), "csi driver restart not successful")
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
			}

		}()

		ginkgo.By("Restart CSI driver")
		restartSuccess, err := restartCSIDriver(ctx, client, csiNamespace, csiReplicas)
		gomega.Expect(restartSuccess).To(gomega.BeTrue(), "csi driver restart not successful")
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Create StatefulSet and verify pv affinity and pod affinity details")
		service, statefulset, err := createStafeulSetAndVerifyPVAndPodNodeAffinty(ctx, client, namespace,
			parallelPodPolicy, stsReplicas, nodeAffinityToSet, allowedTopologies,
			podAntiAffinityToSet, parallelStatefulSetCreation, false, "", nil, verifyTopologyAffinity, "")
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		defer func() {
			fss.DeleteAllStatefulSets(ctx, client, namespace)
			deleteService(namespace, client, service)
		}()

		ginkgo.By("Perform scaleup/scaledown operation on statefulsets and " +
			"verify pv affinity and pod affinity")
		err = performScalingOnStatefulSetAndVerifyPvNodeAffinity(ctx, client, scaleUpReplicaCount,
			scaleDownReplicaCount, statefulset, parallelStatefulSetCreation, namespace,
			allowedTopologies, stsScaleUp, stsScaleDown, verifyTopologyAffinity)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
	})

	/* Testcase-2
	copy same VC details twice in csi-vsphere conf - NEGETIVE

	In csi-vsphere conf, copy VC1's details twice , mention VC2 details once and create secret
	Observe the system behaviour , Expectation is CSI pod's should show CLBO or should show error
	*/

	ginkgo.It("[pq-multivc] Copy same vCenter details twice in csi vsphere conf in a multi-vc setup", ginkgo.Label(p2,
		vsphereConfigSecret, block, vanilla, multiVc, vc70, flaky), func() {

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		// read original vsphere config secret
		vsphereCfg, err := readVsphereConfSecret(client, ctx, csiNamespace)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		/* here we are updating only vCenter IP and vCenter Password, rest all other vCenter credentials will
		remain same
		VC1 and VC3 credentials will be same in conf secret, VC2 will be having its exact credentials

		Note: For this scenario, we will not be doing any restart of service
		*/

		ginkgo.By("Create vsphere-config-secret file with same VC credential details 2 times")
		if multiVCSetupType == "multi-2vc-setup" {
			// copying VC1 IP to VC2 IP
			vCenterIPList := strings.Split(vsphereCfg.Global.VCenterHostname, ",")
			vCenterIPList[1] = vCenterIPList[0]
			vsphereCfg.Global.VCenterHostname = strings.Join(vCenterIPList, ",")

			// assigning new Password to VC2
			passwordList := strings.Split(vsphereCfg.Global.Password, ",")
			passwordList[1] = e2eTestPassword
			vsphereCfg.Global.Password = strings.Join(passwordList, ",")
		} else if multiVCSetupType == "multi-3vc-setup" {
			// copying VC1 IP to VC3 IP
			vCenterIPList := strings.Split(vsphereCfg.Global.VCenterHostname, ",")
			vCenterIPList[2] = vCenterIPList[0]
			vsphereCfg.Global.VCenterHostname = strings.Join(vCenterIPList, ",")

			// assigning new Password to VC3
			passwordList := strings.Split(vsphereCfg.Global.Password, ",")
			passwordList[2] = e2eTestPassword
			vsphereCfg.Global.Password = strings.Join(passwordList, ",")
		}

		/* here we are copying VC1 credentials to VC3 credentials in case of 3-VC setup and
		VC1 credentials to VC2 credentials in case of 2-VC setup and later updating vsphere config secret */

		err = writeNewDataAndUpdateVsphereConfSecret(client, ctx, csiNamespace, vsphereCfg)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		revertToOriginalVsphereConf = true
		defer func() {
			if revertToOriginalVsphereConf {
				ginkgo.By("Reverting back csi-vsphere.conf with its original vcenter user " +
					"and its credentials")
				vsphereCfg.Global.VCenterHostname = vCenterIP
				vsphereCfg.Global.User = vCenterUser
				vsphereCfg.Global.Password = vCenterPassword
				vsphereCfg.Global.VCenterPort = vCenterPort
				vsphereCfg.Global.Datacenters = dataCenter
				err = writeNewDataAndUpdateVsphereConfSecret(client, ctx, csiNamespace, vsphereCfg)
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
				revertToOriginalVsphereConf = false

				ginkgo.By("Restart CSI driver")
				restartSuccess, err := restartCSIDriver(ctx, client, csiNamespace, csiReplicas)
				gomega.Expect(restartSuccess).To(gomega.BeTrue(), "csi driver restart not successful")
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
			}

		}()

		framework.Logf("Wait for %v for csi to auto-detect the config secret changes", pollTimeout)
		time.Sleep(pollTimeout)

		framework.Logf("Verify CSI Pods are in CLBO state")
		deploymentPods, err := client.AppsV1().Deployments(csiNamespace).Get(ctx, vSphereCSIControllerPodNamePrefix,
			metav1.GetOptions{})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		if deploymentPods.Status.UnavailableReplicas != 0 {
			framework.Logf("CSI Pods are in CLBO state")
		}

		ginkgo.By("Try to create a PVC verify that it is stuck in pending state")
		storageclass, pvclaim, err := createPVCAndStorageClass(ctx, client, namespace, nil, nil, "",
			allowedTopologies, "", false, "")
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		defer func() {
			err = client.StorageV1().StorageClasses().Delete(ctx, storageclass.Name, *metav1.NewDeleteOptions(0))
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			err = fpv.DeletePersistentVolumeClaim(ctx, client, pvclaim.Name, namespace)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}()

		ginkgo.By("Expect claim to fail as invalid storage policy is specified in Storage Class")
		err = fpv.WaitForPersistentVolumeClaimPhase(ctx, v1.ClaimBound,
			client, pvclaim.Namespace, pvclaim.Name, framework.Poll, framework.ClaimProvisionTimeout)
		gomega.Expect(err).To(gomega.HaveOccurred())

		expectedErrMsg := "Waiting for a volume to be created"
		err = waitForEvent(ctx, client, namespace, expectedErrMsg, pvclaim.Name)
		gomega.Expect(err).NotTo(gomega.HaveOccurred(), fmt.Sprintf("Expected error : %q", expectedErrMsg))
	})

	/* Testcase-3
	In  csi-vsphere conf, use VC-hostname instead of VC-IP for one VC and try to switch the same during
	a workload vcreation

	1. In csi-vsphere conf, for VC1 use VC-IP, for VC2 use VC-hostname
	2. Create SC
	3. Create Statefulset of replica 10
	4. Try to switch the VC-ip and VC-hostname in config secret and re-create
	5. Reboot CSI-driver to consider the new change
	6. Make sure Statefulset creation should be successful
	7. Verify the node affinity rules
	8. Verify the list-volume response in CSI logs
	[This will be covered in list volume testcase]
	9. Clean up the data
	*/

	ginkgo.It("[pq-multivc] Use VC-hostname instead of VC-IP for one VC and try to switch the same during"+
		"a workload vcreation in a multivc setup", ginkgo.Label(p1,
		vsphereConfigSecret, block, vanilla, multiVc, vc70, flaky), func() {

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		stsReplicas = 10
		scaleUpReplicaCount = 5
		scaleDownReplicaCount = 2

		ginkgo.By("Create StorageClass with all allowed topolgies set")
		scSpec := getVSphereStorageClassSpec(defaultNginxStorageClassName, nil, allowedTopologies, "",
			"", false)
		sc, err := client.StorageV1().StorageClasses().Create(ctx, scSpec, metav1.CreateOptions{})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		defer func() {
			err = client.StorageV1().StorageClasses().Delete(ctx, sc.Name, *metav1.NewDeleteOptions(0))
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}()

		// read original vsphere config secret
		vsphereCfg, err := readVsphereConfSecret(client, ctx, csiNamespace)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Use VC-IP for VC1 and VC-hostname for VC2 in a multi VC setup")
		vCenterList := strings.Split(vsphereCfg.Global.VCenterHostname, ",")
		vCenterIPVC2 := vCenterList[1]

		ginkgo.By("Fetch vcenter hotsname for VC2")
		vCenterHostName := getHostName(vCenterList[1])
		vCenterList[1] = vCenterHostName
		vsphereCfg.Global.VCenterHostname = strings.Join(vCenterList, ",")

		// here we are writing hostname of VC2 and later updating vsphere config secret
		err = writeNewDataAndUpdateVsphereConfSecret(client, ctx, csiNamespace, vsphereCfg)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		revertToOriginalVsphereConf = true
		defer func() {
			if revertToOriginalVsphereConf {
				ginkgo.By("Reverting back csi-vsphere.conf with its original vcenter user " +
					"and its credentials")
				vsphereCfg.Global.VCenterHostname = vCenterIP
				vsphereCfg.Global.User = vCenterUser
				vsphereCfg.Global.Password = vCenterPassword
				vsphereCfg.Global.VCenterPort = vCenterPort
				vsphereCfg.Global.Datacenters = dataCenter
				err = writeNewDataAndUpdateVsphereConfSecret(client, ctx, csiNamespace, vsphereCfg)
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
				revertToOriginalVsphereConf = false

				ginkgo.By("Restart CSI driver")
				restartSuccess, err := restartCSIDriver(ctx, client, csiNamespace, csiReplicas)
				gomega.Expect(restartSuccess).To(gomega.BeTrue(), "csi driver restart not successful")
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
			}

		}()

		ginkgo.By("Restart CSI driver")
		restartSuccess, err := restartCSIDriver(ctx, client, csiNamespace, csiReplicas)
		gomega.Expect(restartSuccess).To(gomega.BeTrue(), "csi driver restart not successful")
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Create StatefulSet and verify pv affinity and pod affinity details")
		service, statefulset, err := createStafeulSetAndVerifyPVAndPodNodeAffinty(ctx, client, namespace,
			parallelPodPolicy, stsReplicas, nodeAffinityToSet, allowedTopologies,
			podAntiAffinityToSet, parallelStatefulSetCreation, false, "", nil, verifyTopologyAffinity, "")
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		defer func() {
			deleteService(namespace, client, service)
			fss.DeleteAllStatefulSets(ctx, client, namespace)
		}()

		ginkgo.By("Use VC-IP for VC2 and VC-hostname for VC1 in a multi VC setup")
		ginkgo.By("Fetch vcenter hotsname of VC1")
		vCenterHostNameVC1 := getHostName(vCenterList[0])
		vCenterList[0] = vCenterHostNameVC1
		vCenterList[1] = vCenterIPVC2
		vsphereCfg.Global.VCenterHostname = strings.Join(vCenterList, ",")

		// here we are writing hostname of VC1 and later updating vsphere config secret
		err = writeNewDataAndUpdateVsphereConfSecret(client, ctx, csiNamespace, vsphereCfg)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		revertToOriginalVsphereConf = true

		ginkgo.By("Restart CSI driver")
		restartSuccess, err = restartCSIDriver(ctx, client, csiNamespace, csiReplicas)
		gomega.Expect(restartSuccess).To(gomega.BeTrue(), "csi driver restart not successful")
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Perform scaleup/scaledown operation on statefulsets and " +
			"verify pv affinity and pod affinity")
		err = performScalingOnStatefulSetAndVerifyPvNodeAffinity(ctx, client, scaleUpReplicaCount,
			scaleDownReplicaCount, statefulset, parallelStatefulSetCreation, namespace,
			allowedTopologies, stsScaleUp, stsScaleDown, verifyTopologyAffinity)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
	})

	/* Testcase-4
		Install CSI driver on different namespace and restart CSI-controller and node daemon sets in
		between the statefulset creation

		1. Create SC with allowedTopology details contains all availability zones
	    2. Create 3 Statefulset with replica-5
	    3. Re-start the CSI controller Pod and node-daemon sets
	    4. Wait for PVC to reach bound state and POD to reach Running state
	    5. Volumes should get distributed among all the Availability zones
	    6. Verify the PV node affinity details should have appropriate node details
	    7. The Pods should be running on the appropriate nodes
	    8. Scale-up/scale-down the statefulset
	    9. Verify the node affinity details also verify the POD details
	    10. Clean up the data
	*/

	ginkgo.It("[pq-multivc] Install CSI driver on different namespace and restart CSI-controller and node daemon sets"+
		"in between the statefulset creation", ginkgo.Label(p2,
		vsphereConfigSecret, block, vanilla, multiVc, vc70, flaky), func() {

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		stsReplicas = 5
		sts_count := 3
		ignoreLabels := make(map[string]string)
		parallelStatefulSetCreation = true

		// read original vsphere config secret
		vsphereCfg, err := readVsphereConfSecret(client, ctx, csiNamespace)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Create a new namespace")
		newNamespace, err = createNamespace(client, ctx, "test-ns")
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		defer func() {
			ginkgo.By("Delete newly created namespace")
			err = deleteNamespace(client, ctx, newNamespace.Name)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}()

		ginkgo.By("Delete config secret created on csi namespace")
		err = deleteVsphereConfigSecret(client, ctx, csiNamespace)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Create config secret on a new namespace")
		err = createVsphereConfigSecret(newNamespace.Name, vsphereCfg, sshClientConfig, allMasterIps)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Install vsphere CSI driver on different test namespace")
		err = setNewNameSpaceInCsiYaml(ctx, client, sshClientConfig, csiNamespace, newNamespace.Name, allMasterIps)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		revertToOriginalCsiYaml = true
		defer func() {
			if revertToOriginalCsiYaml {
				ginkgo.By("Recreate config secret on a default csi system namespace")
				err = deleteVsphereConfigSecret(client, ctx, newNamespace.Name)
				gomega.Expect(err).NotTo(gomega.HaveOccurred())

				err = createVsphereConfigSecret(csiNamespace, vsphereCfg, sshClientConfig, allMasterIps)
				gomega.Expect(err).NotTo(gomega.HaveOccurred())

				ginkgo.By("Revert vsphere CSI driver on a system csi namespace")
				err = setNewNameSpaceInCsiYaml(ctx, client, sshClientConfig, newNamespace.Name, csiNamespace, allMasterIps)
				gomega.Expect(err).NotTo(gomega.HaveOccurred())

				revertToOriginalCsiYaml = false
			}
		}()

		ginkgo.By("Create StorageClass with all allowed topolgies set")
		scSpec := getVSphereStorageClassSpec(defaultNginxStorageClassName, nil, allowedTopologies, "",
			"", false)
		sc, err := client.StorageV1().StorageClasses().Create(ctx, scSpec, metav1.CreateOptions{})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		defer func() {
			err = client.StorageV1().StorageClasses().Delete(ctx, sc.Name, *metav1.NewDeleteOptions(0))
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}()

		ginkgo.By("Creating service")
		service := CreateService(namespace, client)
		defer func() {
			deleteService(namespace, client, service)
		}()

		ginkgo.By("Creating multiple StatefulSets specs in parallel")
		statefulSets := createParallelStatefulSetSpec(namespace, sts_count, stsReplicas)

		ginkgo.By("Trigger multiple StatefulSets creation in parallel")
		var wg sync.WaitGroup
		wg.Add(sts_count)
		for i := 0; i < len(statefulSets); i++ {
			go createParallelStatefulSets(client, namespace, statefulSets[i], stsReplicas, &wg)
			if i == 1 {
				ginkgo.By("Restart CSI driver")
				restartSuccess, err := restartCSIDriver(ctx, client, newNamespace.Name, csiReplicas)
				gomega.Expect(restartSuccess).To(gomega.BeTrue(), "csi driver restart not successful")
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
			}
		}
		wg.Wait()

		ginkgo.By("Waiting for StatefulSets Pods to be in Ready State")
		err = waitForStsPodsToBeInReadyRunningState(ctx, client, namespace, statefulSets)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		defer func() {
			deleteAllStsAndPodsPVCsInNamespace(ctx, client, namespace)
		}()

		ginkgo.By("Verify PV node affinity and that the PODS are running on appropriate node")
		for i := 0; i < len(statefulSets); i++ {
			err = verifyPVnodeAffinityAndPODnodedetailsForStatefulsetsLevel5(ctx, client,
				statefulSets[i], namespace, allowedTopologies, parallelStatefulSetCreation)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}

		ginkgo.By("Perform scaleup/scaledown operation on statefulset and verify pv and pod affinity details")
		for i := 0; i < len(statefulSets); i++ {
			if i == 0 {
				stsScaleUp = false
				scaleDownReplicaCount = 3
				framework.Logf("Scale down StatefulSet1 replica count to 3")
				err = performScalingOnStatefulSetAndVerifyPvNodeAffinity(ctx, client, scaleUpReplicaCount,
					scaleDownReplicaCount, statefulSets[i], parallelStatefulSetCreation, namespace,
					allowedTopologies, stsScaleUp, stsScaleDown, verifyTopologyAffinity)
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
			}
			if i == 1 {
				scaleUpReplicaCount = 9
				stsScaleDown = false
				framework.Logf("Scale up StatefulSet2 replica count to 9")
				err = performScalingOnStatefulSetAndVerifyPvNodeAffinity(ctx, client, scaleUpReplicaCount,
					scaleDownReplicaCount, statefulSets[i], parallelStatefulSetCreation, namespace,
					allowedTopologies, stsScaleUp, stsScaleDown, verifyTopologyAffinity)
				gomega.Expect(err).NotTo(gomega.HaveOccurred())

			}
			if i == 2 {
				framework.Logf("Scale up StatefulSet3 replica count to 13 and later scale it down " +
					"to replica count 2 and in between restart node daemon set")
				scaleUpReplicaCount = 13
				scaleDownReplicaCount = 2

				// Fetch the number of CSI pods running before restart
				list_of_pods, err := fpod.GetPodsInNamespace(ctx, client, newNamespace.Name, ignoreLabels)
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
				num_csi_pods := len(list_of_pods)

				// Collecting and dumping csi pod logs before restrating CSI daemonset
				collectPodLogs(ctx, client, newNamespace.Name)

				// Restart CSI daemonset
				ginkgo.By("Restart Daemonset")
				cmd := []string{"rollout", "restart", "daemonset/vsphere-csi-node", "--namespace=" + newNamespace.Name}
				e2ekubectl.RunKubectlOrDie(newNamespace.Name, cmd...)

				ginkgo.By("Waiting for daemon set rollout status to finish")
				statusCheck := []string{"rollout", "status", "daemonset/vsphere-csi-node", "--namespace=" + newNamespace.Name}
				e2ekubectl.RunKubectlOrDie(newNamespace.Name, statusCheck...)

				// wait for csi Pods to be in running ready state
				err = fpod.WaitForPodsRunningReady(ctx, client, newNamespace.Name, int(num_csi_pods),
					time.Duration(pollTimeout))
				gomega.Expect(err).NotTo(gomega.HaveOccurred())

				err = performScalingOnStatefulSetAndVerifyPvNodeAffinity(ctx, client, scaleUpReplicaCount,
					scaleDownReplicaCount, statefulSets[i], parallelStatefulSetCreation, namespace,
					allowedTopologies, stsScaleUp, stsScaleDown, verifyTopologyAffinity)
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
			}
		}

	})

	/* Testcase-5
		In vsphere-config, keep different passwords on each VC and check Statefulset creation and reboot VC

		1. Set different passwords to VC's in config secret .
	    2. Re-start csi driver pods
	    3. Wait for some time for csi to pick the changes on vsphere-secret file
	    4. Create Statefulsets
	    5. Reboot any one VC
		6. Revert the original VC password and vsphere conf, but this time do not restart csi driver
	    6. Scale up/Scale-down the statefulset
	    7. Verify the node affinity
	    8. Clean up the data
	*/

	ginkgo.It("[pq-multivc] Keep different passwords on each VC and check Statefulset creation and reboot"+
		" VC", ginkgo.Label(p2,
		vsphereConfigSecret, block, vanilla, multiVc, vc70, flaky, negative), func() {

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		stsReplicas = 3
		scaleUpReplicaCount = 5
		scaleDownReplicaCount = 2
		var clientIndex2 int
		var username3, newPassword3, originalPassword3 string

		// read original vsphere config secret
		vsphereCfg, err := readVsphereConfSecret(client, ctx, csiNamespace)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Create StorageClass with all allowed topolgies set")
		scSpec := getVSphereStorageClassSpec(defaultNginxStorageClassName, nil, allowedTopologies, "",
			"", false)
		sc, err := client.StorageV1().StorageClasses().Create(ctx, scSpec, metav1.CreateOptions{})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		defer func() {
			err = client.StorageV1().StorageClasses().Delete(ctx, sc.Name, *metav1.NewDeleteOptions(0))
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}()

		// read VC1 credentials
		username1 := strings.Split(vsphereCfg.Global.User, ",")[0]
		originalPassword1 := strings.Split(vsphereCfg.Global.Password, ",")[0]
		newPassword1 := "E2E-test-password!23"

		ginkgo.By("Changing password on the vCenter VC1 host")
		clientIndex0 := 0
		err = invokeVCenterChangePassword(ctx, username1, originalPassword1, newPassword1, vcAddress, clientIndex0)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		originalVC1PasswordChanged = true

		if multiVCSetupType == "multi-3vc-setup" {
			// read VC3 credentials
			username3 = strings.Split(vsphereCfg.Global.User, ",")[2]
			originalPassword3 = strings.Split(vsphereCfg.Global.Password, ",")[2]
			newPassword3 = "Admin!23"

			ginkgo.By("Changing password on the vCenter VC3 host")
			clientIndex2 = 2
			err = invokeVCenterChangePassword(ctx, username3, originalPassword3, newPassword3, vcAddress3, clientIndex2)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			originalVC3PasswordChanged = true

			ginkgo.By("Create vsphere-config-secret file with new VC1 and new VC2 password in 3-VC setup")
			passwordList := strings.Split(vsphereCfg.Global.Password, ",")
			passwordList[0] = newPassword1
			passwordList[2] = newPassword3
			vsphereCfg.Global.Password = strings.Join(passwordList, ",")
		} else if multiVCSetupType == "multi-2vc-setup" {
			ginkgo.By("Create vsphere-config-secret file with new VC1 password in 2-VC setup")
			passwordList := strings.Split(vsphereCfg.Global.Password, ",")
			passwordList[0] = newPassword1
			vsphereCfg.Global.Password = strings.Join(passwordList, ",")
		}

		// here we are writing new password of VC1 and VC2 and later updating vsphere config secret
		err = writeNewDataAndUpdateVsphereConfSecret(client, ctx, csiNamespace, vsphereCfg)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		revertToOriginalVsphereConf = true
		defer func() {
			if originalVC1PasswordChanged {
				ginkgo.By("Reverting the password change")
				err = invokeVCenterChangePassword(ctx, username1, newPassword1, originalPassword1, vcAddress,
					clientIndex0)
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
				originalVC1PasswordChanged = false
			}

			if multiVCSetupType == "multi-3vc-setup" {
				if originalVC3PasswordChanged {
					ginkgo.By("Reverting the password change")
					err = invokeVCenterChangePassword(ctx, username3, newPassword3, originalPassword3, vcAddress3,
						clientIndex2)
					gomega.Expect(err).NotTo(gomega.HaveOccurred())
					originalVC3PasswordChanged = false
				}
			}

			if revertToOriginalVsphereConf {
				ginkgo.By("Reverting back csi-vsphere.conf with its original vcenter user " +
					"and its credentials")
				vsphereCfg.Global.VCenterHostname = vCenterIP
				vsphereCfg.Global.User = vCenterUser
				vsphereCfg.Global.Password = vCenterPassword
				vsphereCfg.Global.VCenterPort = vCenterPort
				vsphereCfg.Global.Datacenters = dataCenter
				err = writeNewDataAndUpdateVsphereConfSecret(client, ctx, csiNamespace, vsphereCfg)
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
				revertToOriginalVsphereConf = false

				ginkgo.By("Restart CSI driver")
				restartSuccess, err := restartCSIDriver(ctx, client, csiNamespace, csiReplicas)
				gomega.Expect(restartSuccess).To(gomega.BeTrue(), "csi driver restart not successful")
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
			}

		}()

		ginkgo.By("Restart CSI driver")
		restartSuccess, err := restartCSIDriver(ctx, client, csiNamespace, csiReplicas)
		gomega.Expect(restartSuccess).To(gomega.BeTrue(), "csi driver restart not successful")
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Create StatefulSet and verify pv affinity and pod affinity details")
		service, statefulset, err := createStafeulSetAndVerifyPVAndPodNodeAffinty(ctx, client, namespace,
			parallelPodPolicy, stsReplicas, nodeAffinityToSet, allowedTopologies,
			podAntiAffinityToSet, parallelStatefulSetCreation, false, "", nil, verifyTopologyAffinity, "")
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		defer func() {
			fss.DeleteAllStatefulSets(ctx, client, namespace)
			deleteService(namespace, client, service)
		}()

		ginkgo.By("Rebooting VC2")
		framework.Logf("vcAddress - %s ", vcAddress2)
		err = invokeVCenterReboot(ctx, vcAddress2)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		err = waitForHostToBeUp(vcAddress2)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		ginkgo.By("Done with reboot")

		essentialServices := []string{spsServiceName, vsanhealthServiceName, vpxdServiceName}
		checkVcenterServicesRunning(ctx, vcAddress2, essentialServices)

		ginkgo.By("Reverting the password change on VC1")
		err = invokeVCenterChangePassword(ctx, username1, newPassword1, originalPassword1, vcAddress,
			clientIndex0)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		originalVC1PasswordChanged = false

		if multiVCSetupType == "multi-3vc-setup" {
			ginkgo.By("Reverting the password change on VC3")
			err = invokeVCenterChangePassword(ctx, username3, newPassword3, originalPassword3, vcAddress3,
				clientIndex2)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			originalVC3PasswordChanged = false
		}

		ginkgo.By("Reverting back csi-vsphere.conf with its original vcenter user " +
			"and its credentials")
		vsphereCfg.Global.VCenterHostname = vCenterIP
		vsphereCfg.Global.User = vCenterUser
		vsphereCfg.Global.Password = vCenterPassword
		vsphereCfg.Global.VCenterPort = vCenterPort
		vsphereCfg.Global.Datacenters = dataCenter
		err = writeNewDataAndUpdateVsphereConfSecret(client, ctx, csiNamespace, vsphereCfg)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		revertToOriginalVsphereConf = false

		framework.Logf("Wait for %v for csi driver to auto-detect the changes", pollTimeout)
		time.Sleep(pollTimeout)

		ginkgo.By("Perform scaleup/scaledown operation on statefulsets and " +
			"verify pv affinity and pod affinity")
		err = performScalingOnStatefulSetAndVerifyPvNodeAffinity(ctx, client, scaleUpReplicaCount,
			scaleDownReplicaCount, statefulset, parallelStatefulSetCreation, namespace,
			allowedTopologies, stsScaleUp, stsScaleDown, verifyTopologyAffinity)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
	})

	/*Testcase-6
	Change VC in the UI but not on the vsphere secret
	1. Create SC
	2. Without updating the vsphere secret on the VC password
	3. Create a statefulset
	4. Until driver restarts all the workflows will go fine
	5. Statefulset creation should be successful
	6. Restart the driver
	7. There should be error while provisioning volume on the VC on which password has updated
	8. Clean up the data
	*/

	ginkgo.It("[pq-multivc] Change VC in the UI but not on the vsphere secret and verify "+
		"volume creation workflow on a multivc setup", ginkgo.Label(p2,
		vsphereConfigSecret, block, vanilla, multiVc, vc70, flaky), func() {

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		clientIndex := 0

		ginkgo.By("Changing password on the vCenter VC1 host")
		// read original vsphere config secret
		vsphereCfg, err := readVsphereConfSecret(client, ctx, csiNamespace)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		framework.Logf("vcAddress - %s ", vcAddress)
		username := strings.Split(vsphereCfg.Global.User, ",")[0]
		originalPassword := strings.Split(vsphereCfg.Global.Password, ",")[0]
		newPassword := e2eTestPassword
		ginkgo.By(fmt.Sprintf("Original password %s, new password %s", originalPassword, newPassword))
		err = invokeVCenterChangePassword(ctx, username, originalPassword, newPassword, vcAddress, clientIndex)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		originalVC1PasswordChanged = true
		defer func() {
			if originalVC1PasswordChanged {
				ginkgo.By("Reverting the password change")
				err = invokeVCenterChangePassword(ctx, username, newPassword, originalPassword, vcAddress,
					clientIndex)
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
				originalVC1PasswordChanged = false
			}
		}()

		framework.Logf("Wait for %v for csi driver to auto-detect the changes", pollTimeout)
		time.Sleep(pollTimeout)

		framework.Logf("Verify CSI Pods are in CLBO state")
		deploymentPods, err := client.AppsV1().Deployments(csiNamespace).Get(ctx, vSphereCSIControllerPodNamePrefix,
			metav1.GetOptions{})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		if deploymentPods.Status.UnavailableReplicas != 0 {
			framework.Logf("CSI Pods are in CLBO state")
		}

		ginkgo.By("Try to create a PVC verify that it is stuck in pending state")
		storageclass, pvclaim, err := createPVCAndStorageClass(ctx, client, namespace, nil, nil, "",
			allowedTopologies, "", false, "")
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		defer func() {
			err = client.StorageV1().StorageClasses().Delete(ctx, storageclass.Name, *metav1.NewDeleteOptions(0))
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			err = fpv.DeletePersistentVolumeClaim(ctx, client, pvclaim.Name, namespace)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}()

		ginkgo.By("Expect claim to fail as invalid storage policy is specified in Storage Class")
		err = fpv.WaitForPersistentVolumeClaimPhase(ctx, v1.ClaimBound,
			client, pvclaim.Namespace, pvclaim.Name, framework.Poll, framework.ClaimProvisionTimeout)
		gomega.Expect(err).To(gomega.HaveOccurred())
		expectedErrMsg := "Waiting for a volume to be created"
		err = waitForEvent(ctx, client, namespace, expectedErrMsg, pvclaim.Name)
		gomega.Expect(err).NotTo(gomega.HaveOccurred(), fmt.Sprintf("Expected error : %q", expectedErrMsg))
	})

	/* Testcase-7
	Make a wrong entry in vsphere conf for one of the VC  and create a secret
	Logs will have error messages, But driver still be up and running .
	until driver restarts , volume creation will be fine
	After re-starting CSI driver it should throw error, should not come to running state until the error in
	vsphere-secret is fixed
	*/

	ginkgo.It("[pq-multivc] Add any wrong entry in vsphere conf and verify csi pods behaviour", ginkgo.Label(p2,
		vsphereConfigSecret, block, vanilla, multiVc, vc70, flaky), func() {

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		wrongPortNoVC1 := "337"

		// read original vsphere config secret
		vsphereCfg, err := readVsphereConfSecret(client, ctx, csiNamespace)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Create vsphere-config-secret file with wrong port number in VC1")
		portList := strings.Split(vsphereCfg.Global.VCenterPort, ",")
		portList[0] = wrongPortNoVC1
		vsphereCfg.Global.VCenterPort = strings.Join(portList, ",")

		err = writeNewDataAndUpdateVsphereConfSecret(client, ctx, csiNamespace, vsphereCfg)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		revertToOriginalVsphereConf = true
		defer func() {
			if revertToOriginalVsphereConf {
				ginkgo.By("Reverting back csi-vsphere.conf with its original vcenter user " +
					"and its credentials")
				vsphereCfg.Global.VCenterHostname = vCenterIP
				vsphereCfg.Global.User = vCenterUser
				vsphereCfg.Global.Password = vCenterPassword
				vsphereCfg.Global.VCenterPort = vCenterPort
				vsphereCfg.Global.Datacenters = dataCenter
				err = writeNewDataAndUpdateVsphereConfSecret(client, ctx, csiNamespace, vsphereCfg)
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
				revertToOriginalVsphereConf = false

				ginkgo.By("Restart CSI driver")
				restartSuccess, err := restartCSIDriver(ctx, client, csiNamespace, csiReplicas)
				gomega.Expect(restartSuccess).To(gomega.BeTrue(), "csi driver restart not successful")
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
			}

		}()

		framework.Logf("Wait for %v to see the CSI Pods ready running status after wrong entry in vsphere conf", pollTimeout)
		time.Sleep(pollTimeout)

		framework.Logf("Verify CSI Pods status")
		deploymentPods, err := client.AppsV1().Deployments(csiNamespace).Get(ctx,
			vSphereCSIControllerPodNamePrefix, metav1.GetOptions{})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		if deploymentPods.Status.AvailableReplicas == csiReplicas {
			framework.Logf("CSi Pods are in ready running state")
		}

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

		framework.Logf("Verify CSI Pods status now")
		deploymentPods, err = client.AppsV1().Deployments(csiNamespace).Get(ctx,
			vSphereCSIControllerPodNamePrefix, metav1.GetOptions{})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		if deploymentPods.Status.UnavailableReplicas != csiReplicas {
			framework.Logf("CSi Pods are not ready or in CLBO state with %d unavilable csi pod replica",
				deploymentPods.Status.UnavailableReplicas)
		}

		ginkgo.By("Try to create a PVC verify that it is stuck in pending state")
		storageclass, pvclaim, err := createPVCAndStorageClass(ctx, client, namespace, nil, nil, "",
			allowedTopologies, "", false, "")
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		defer func() {
			err = client.StorageV1().StorageClasses().Delete(ctx, storageclass.Name, *metav1.NewDeleteOptions(0))
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			err = fpv.DeletePersistentVolumeClaim(ctx, client, pvclaim.Name, namespace)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}()

		ginkgo.By("Expect claim to fail as invalid storage policy is specified in Storage Class")
		err = fpv.WaitForPersistentVolumeClaimPhase(ctx, v1.ClaimBound,
			client, pvclaim.Namespace, pvclaim.Name, framework.Poll, framework.ClaimProvisionTimeout)
		gomega.Expect(err).To(gomega.HaveOccurred())
		expectedErrMsg := "Waiting for a volume to be created"
		err = waitForEvent(ctx, client, namespace, expectedErrMsg, pvclaim.Name)
		gomega.Expect(err).NotTo(gomega.HaveOccurred(), fmt.Sprintf("Expected error : %q", expectedErrMsg))
	})
})
