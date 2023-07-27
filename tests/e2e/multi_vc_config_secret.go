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
	"sync"
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
	fss "k8s.io/kubernetes/test/e2e/framework/statefulset"
	admissionapi "k8s.io/pod-security-admission/api"
)

var _ = ginkgo.Describe("Config-Secret", func() {
	f := framework.NewDefaultFramework("config-secret-changes")
	f.NamespacePodSecurityEnforceLevel = admissionapi.LevelPrivileged
	var (
		client    clientset.Interface
		namespace string

		csiNamespace                string
		csiReplicas                 int32
		podAntiAffinityToSet        bool
		stsScaleUp                  bool
		stsScaleDown                bool
		verifyTopologyAffinity      bool
		allowedTopologies           []v1.TopologySelectorLabelRequirement
		scaleUpReplicaCount         int32
		scaleDownReplicaCount       int32
		allowedTopologyLen          int
		nodeAffinityToSet           bool
		parallelStatefulSetCreation bool
		stsReplicas                 int32
		parallelPodPolicy           bool
		originalVCPasswordChanged   bool
		vCenterIP                   string
		vCenterUser                 string
		vCenterPassword             string
		vCenterPort                 string
		dataCenter                  string
		err                         error
		revertToOriginalVsphereConf bool
		multiVCSetupType            string
		allMasterIps                []string
		masterIp                    string
		sshClientConfig             *ssh.ClientConfig
		nimbusGeneratedK8sVmPwd     string
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

		nodeList, err := fnodes.GetReadySchedulableNodes(f.ClientSet)
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
		topologyMap := GetAndExpectStringEnvVar(topologyMap)
		allowedTopologies = createAllowedTopolgies(topologyMap, topologyLength)

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
		masterIp = allMasterIps[0]
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
			writeNewDataAndUpdateVsphereConfSecret(client, ctx, csiNamespace, vsphereCfg)

			ginkgo.By("Restart CSI driver")
			restartSuccess, err := restartCSIDriver(ctx, client, namespace, csiReplicas)
			gomega.Expect(restartSuccess).To(gomega.BeTrue(), "csi driver restart not successful")
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}

		if originalVCPasswordChanged {
			clientIndex := 0
			vcAddress := strings.Split(multiVCe2eVSphere.multivcConfig.Global.VCenterHostname, ",")[0] + ":" + sshdPort
			username := strings.Split(multiVCe2eVSphere.multivcConfig.Global.User, ",")[0]
			originalPassword := strings.Split(multiVCe2eVSphere.multivcConfig.Global.Password, ",")[0]
			newPassword := e2eTestPassword
			ginkgo.By("Reverting the password change")
			err = invokeVCenterChangePassword(username, newPassword, originalPassword, vcAddress, true,
				clientIndex)
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

	ginkgo.It("TestConfigMulti", func() {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		clientIndex := 0
		stsReplicas = 3
		scaleUpReplicaCount = 7
		scaleDownReplicaCount = 1

		ginkgo.By("Create StorageClass with all allowed topolgies set")
		scSpec := getVSphereStorageClassSpec(defaultNginxStorageClassName, nil, allowedTopologies, "",
			"", false)
		sc, err := client.StorageV1().StorageClasses().Create(ctx, scSpec, metav1.CreateOptions{})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		defer func() {
			err = client.StorageV1().StorageClasses().Delete(ctx, sc.Name, *metav1.NewDeleteOptions(0))
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}()

		ginkgo.By("Changing password on the vCenter VC1 host")

		// read original vsphere config secret
		vsphereCfg, err := readVsphereConfSecret(client, ctx, csiNamespace)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		vcAddress := strings.Split(vsphereCfg.Global.VCenterHostname, ",")[0] + ":" + sshdPort
		framework.Logf("vcAddress - %s ", vcAddress)
		username := strings.Split(vsphereCfg.Global.User, ",")[0]
		originalPassword := strings.Split(vsphereCfg.Global.Password, ",")[0]
		newPassword := e2eTestPassword
		ginkgo.By(fmt.Sprintf("Original password %s, new password %s", originalPassword, newPassword))
		err = invokeVCenterChangePassword(username, originalPassword, newPassword, vcAddress, true, clientIndex)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		originalVCPasswordChanged = true
		defer func() {
			if originalVCPasswordChanged {
				ginkgo.By("Reverting the password change")
				err = invokeVCenterChangePassword(username, newPassword, originalPassword, vcAddress, true,
					clientIndex)
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
				originalVCPasswordChanged = false
			}
		}()

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
			if revertToOriginalVsphereConf {
				ginkgo.By("Reverting back csi-vsphere.conf with its original vcenter user " +
					"and its credentials")
				vsphereCfg.Global.VCenterHostname = vCenterIP
				vsphereCfg.Global.User = vCenterUser
				vsphereCfg.Global.Password = vCenterPassword
				vsphereCfg.Global.VCenterPort = vCenterPort
				vsphereCfg.Global.Datacenters = dataCenter
				writeNewDataAndUpdateVsphereConfSecret(client, ctx, csiNamespace, vsphereCfg)
				revertToOriginalVsphereConf = false

				ginkgo.By("Restart CSI driver")
				restartSuccess, err := restartCSIDriver(ctx, client, namespace, csiReplicas)
				gomega.Expect(restartSuccess).To(gomega.BeTrue(), "csi driver restart not successful")
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
			}

		}()

		ginkgo.By("Restart CSI driver")
		restartSuccess, err := restartCSIDriver(ctx, client, namespace, csiReplicas)
		gomega.Expect(restartSuccess).To(gomega.BeTrue(), "csi driver restart not successful")
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Create StatefulSet and verify pv affinity and pod affinity details")
		service, statefulset := createStafeulSetAndVerifyPVAndPodNodeAffinty(ctx, client, namespace,
			parallelPodPolicy, stsReplicas, nodeAffinityToSet, allowedTopologies, allowedTopologyLen,
			podAntiAffinityToSet, parallelStatefulSetCreation, false, "", "", nil, verifyTopologyAffinity)
		defer func() {
			fss.DeleteAllStatefulSets(client, namespace)
			deleteService(namespace, client, service)
		}()

		ginkgo.By("Perform scaleup/scaledown operation on statefulsets and " +
			"verify pv affinity and pod affinity")
		performScalingOnStatefulSetAndVerifyPvNodeAffinity(ctx, client, scaleUpReplicaCount,
			scaleDownReplicaCount, statefulset, parallelStatefulSetCreation, namespace,
			allowedTopologies, stsScaleUp, stsScaleDown, verifyTopologyAffinity)
	})

	/* Testcase-2
	copy same VC details twice in csi-vsphere conf - NEGETIVE

	In csi-vsphere conf, copy VC1's details twice , mention VC2 details once and create secret
	Observe the system behaviour , Expectation is CSI pod's should show CLBO or should show error
	*/

	ginkgo.It("Test2ConfigMulti", func() {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

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

		/* here we are updating only vCenter IP and vCenter Password, rest all other vCenter credentials will
		remain same
		VC1 and VC3 credentials will be same in conf secret, VC2 will be having its exact credentials

		Note: For this scenario, we will not be doing any restart of service
		*/

		ginkgo.By("Create vsphere-config-secret file with same VC credential details 2 times")
		if multiVCSetupType == "multi-2vc-setup" {
			vCenterIPList := strings.Split(vsphereCfg.Global.VCenterHostname, ",")
			vCenterIPList[1] = vCenterIPList[0]
			vsphereCfg.Global.VCenterHostname = strings.Join(vCenterIPList, ",")
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

		/* here we are copying VC1 credentails to VC3 credentials in case of 3-VC setup and
		VC1 credentails to VC2 credentials and later updating vsphere config secret */

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
				writeNewDataAndUpdateVsphereConfSecret(client, ctx, csiNamespace, vsphereCfg)
				revertToOriginalVsphereConf = false

				ginkgo.By("Restart CSI driver")
				restartSuccess, err := restartCSIDriver(ctx, client, namespace, csiReplicas)
				gomega.Expect(restartSuccess).To(gomega.BeTrue(), "csi driver restart not successful")
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
			}

		}()

		framework.Logf("Wait for %v for csi to auto-detect the config secret changes", pollTimeout)
		time.Sleep(pollTimeout)

		framework.Logf("Verify CSI Pods are in CLBO state")
		deploymentPods, err := client.AppsV1().Deployments(csiNamespace).Get(ctx, vSphereCSIControllerPodNamePrefix,
			metav1.GetOptions{})
		if deploymentPods.Status.UnavailableReplicas != 0 {
			framework.Logf("CSI Pods are in CLBO state")
		}

		ginkgo.By("Try to create a PVC verify that it is stuck in pending state")
		pvclaim, err := createPVC(client, namespace, nil, "", sc, "")
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		ginkgo.By("Expect claim status to be in Pending state")
		err = fpv.WaitForPersistentVolumeClaimPhase(v1.ClaimPending, client,
			pvclaim.Namespace, pvclaim.Name, framework.Poll, time.Minute)
		gomega.Expect(err).NotTo(gomega.HaveOccurred(),
			fmt.Sprintf("Failed to find the volume in pending state with err: %v", err))
		defer func() {
			ginkgo.By("Deleting the PVC")
			err = fpv.DeletePersistentVolumeClaim(client, pvclaim.Name, namespace)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}()

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
	9. Clean up the data
	*/

	ginkgo.It("Test3ConfigMulti", func() {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		stsReplicas = 3

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
		vCenterHostName := getVcenterHostName(vCenterList[1])
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
				writeNewDataAndUpdateVsphereConfSecret(client, ctx, csiNamespace, vsphereCfg)
				revertToOriginalVsphereConf = false

				ginkgo.By("Restart CSI driver")
				restartSuccess, err := restartCSIDriver(ctx, client, namespace, csiReplicas)
				gomega.Expect(restartSuccess).To(gomega.BeTrue(), "csi driver restart not successful")
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
			}

		}()

		ginkgo.By("Restart CSI driver")
		restartSuccess, err := restartCSIDriver(ctx, client, namespace, csiReplicas)
		gomega.Expect(restartSuccess).To(gomega.BeTrue(), "csi driver restart not successful")
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Create StatefulSet and verify pv affinity and pod affinity details")
		service, _ := createStafeulSetAndVerifyPVAndPodNodeAffinty(ctx, client, namespace,
			parallelPodPolicy, stsReplicas, nodeAffinityToSet, allowedTopologies, allowedTopologyLen,
			podAntiAffinityToSet, parallelStatefulSetCreation, false, "", "", nil, verifyTopologyAffinity)
		defer func() {
			deleteService(namespace, client, service)
		}()

		ginkgo.By("Use VC-IP for VC2 and VC-Hostname for VC1 in a multi VC setup")
		ginkgo.By("Fetch vcenter hotsname of VC1")
		vCenterHostNameVC1 := getVcenterHostName(vCenterList[0])
		vCenterList[0] = vCenterHostNameVC1
		vCenterList[1] = vCenterIPVC2
		vsphereCfg.Global.VCenterHostname = strings.Join(vCenterList, ",")

		ginkgo.By("Creating PVC")
		pvclaim, err := createPVC(client, namespace, nil, "", sc, "")
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		var pvclaims []*v1.PersistentVolumeClaim
		pvclaims = append(pvclaims, pvclaim)
		ginkgo.By("Waiting for all claims to be in bound state")
		pvs, err := fpv.WaitForPVClaimBoundPhase(client, pvclaims, framework.ClaimProvisionTimeout)
		pv := getPvFromClaim(client, namespace, pvclaim.Name)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		gomega.Expect(pvs).NotTo(gomega.BeEmpty())

		defer func() {
			ginkgo.By("Deleting PVC's and PV's")
			err = fpv.DeletePersistentVolumeClaim(client, pvclaim.Name, namespace)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			framework.ExpectNoError(fpv.WaitForPersistentVolumeDeleted(client, pv.Name, poll, pollTimeoutShort))
			err = multiVCe2eVSphere.waitForCNSVolumeToBeDeletedInMultiVC(pv.Spec.CSI.VolumeHandle)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())

			deleteAllStsAndPodsPVCsInNamespace(ctx, client, namespace)
		}()

		ginkgo.By("Creating pod")
		pod, err := createPod(client, namespace, nil, []*v1.PersistentVolumeClaim{pvclaim}, false, "")
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By(fmt.Sprintf("Verify volume: %s is attached to the node: %s",
			pv.Spec.CSI.VolumeHandle, pod.Spec.NodeName))
		vmUUID := getNodeUUID(ctx, client, pod.Spec.NodeName)
		isDiskAttached, err := multiVCe2eVSphere.verifyVolumeIsAttachedToVMInMultiVC(client,
			pv.Spec.CSI.VolumeHandle, vmUUID)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		gomega.Expect(isDiskAttached).To(gomega.BeTrue(), "Volume is not attached")
		defer func() {
			ginkgo.By(fmt.Sprintf("Deleting the pod %s in namespace %s", pod.Name, namespace))
			err = fpod.DeletePodWithWait(client, pod)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())

			ginkgo.By("Verify volume is detached from the node")
			isDiskDetached, err := multiVCe2eVSphere.waitForVolumeDetachedFromNodeInMultiVC(client,
				pv.Spec.CSI.VolumeHandle, pod.Spec.NodeName)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			gomega.Expect(isDiskDetached).To(gomega.BeTrue(),
				fmt.Sprintf("Volume %q is not detached from the node", pv.Spec.CSI.VolumeHandle))

		}()

		ginkgo.By("Verify PV node affinity and that the PODS are running on appropriate node")
		verifyPVnodeAffinityAndPODnodedetailsFoStandalonePodLevel5(ctx, client, pod, namespace,
			allowedTopologies, true)
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

	ginkgo.It("Test4ConfigMulti", func() {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		newNS := "test-ns"
		statefulSetReplicaCount := int32(5)
		sts_count := 3
		ignoreLabels := make(map[string]string)

		ginkgo.By("Install vsphere CSI driver on different test namespace")
		setNewNameSpaceInCsiYaml(ctx, client, sshClientConfig, masterIp, csiSystemNamespace, newNS)
		defer func() {
			setNewNameSpaceInCsiYaml(ctx, client, sshClientConfig, masterIp, newNS, csiSystemNamespace)
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
		statefulSets := createParallelStatefulSetSpec(namespace, sts_count, statefulSetReplicaCount)

		ginkgo.By("Trigger multiple StatefulSets creation in parallel. During StatefulSets " +
			"creation, kill CSI-Provisioner, CSI-Attacher container in between")
		var wg sync.WaitGroup
		wg.Add(sts_count)
		for i := 0; i < len(statefulSets); i++ {
			go createParallelStatefulSets(client, namespace, statefulSets[i], statefulSetReplicaCount, &wg)
			if i == 1 {
				ginkgo.By("Restart CSI driver")
				restartSuccess, err := restartCSIDriver(ctx, client, newNS, csiReplicas)
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
			verifyPVnodeAffinityAndPODnodedetailsForStatefulsetsLevel5(ctx, client,
				statefulSets[i], namespace, allowedTopologies, parallelStatefulSetCreation, true)
		}

		ginkgo.By("Perform scaleup/scaledown operation on statefulset and verify pv and pod affinity details")
		for i := 0; i < len(statefulSets); i++ {
			if i == 0 {
				stsScaleUp = false
				scaleDownReplicaCount = 3
				framework.Logf("Scale down StatefulSet1 replica count to 3")
				performScalingOnStatefulSetAndVerifyPvNodeAffinity(ctx, client, scaleUpReplicaCount,
					scaleDownReplicaCount, statefulSets[i], parallelStatefulSetCreation, namespace,
					allowedTopologies, stsScaleUp, stsScaleDown, verifyTopologyAffinity)
			}
			if i == 1 {
				scaleUpReplicaCount = 9
				stsScaleDown = false
				framework.Logf("Scale up StatefulSet2 replica count to 9 and in between " +
					"kill vsphere syncer container")
				performScalingOnStatefulSetAndVerifyPvNodeAffinity(ctx, client, scaleUpReplicaCount,
					scaleDownReplicaCount, statefulSets[i], parallelStatefulSetCreation, namespace,
					allowedTopologies, stsScaleUp, stsScaleDown, verifyTopologyAffinity)

			}
			if i == 2 {
				framework.Logf("Scale up StatefulSet2 replica count to 9 and in between " +
					"kill vsphere syncer container")
				scaleUpReplicaCount = 13
				scaleDownReplicaCount = 2

				// Fetch the number of CSI pods running before restart
				list_of_pods, err := fpod.GetPodsInNamespace(client, csiSystemNamespace, ignoreLabels)
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
				num_csi_pods := len(list_of_pods)

				// Collecting and dumping csi pod logs before restrating CSI daemonset
				collectPodLogs(ctx, client, csiSystemNamespace)

				// Restart CSI daemonset
				ginkgo.By("Restart Daemonset")
				cmd := []string{"rollout", "restart", "daemonset/vsphere-csi-node", "--namespace=" + csiSystemNamespace}
				framework.RunKubectlOrDie(csiSystemNamespace, cmd...)

				scaleUpReplicaCount = 9
				stsScaleDown = false
				framework.Logf("Scale up StatefulSet2 replica count to 9 and in between " +
					"kill vsphere syncer container")
				performScalingOnStatefulSetAndVerifyPvNodeAffinity(ctx, client, scaleUpReplicaCount,
					scaleDownReplicaCount, statefulSets[i], parallelStatefulSetCreation, namespace,
					allowedTopologies, stsScaleUp, stsScaleDown, verifyTopologyAffinity)

				ginkgo.By("Waiting for daemon set rollout status to finish")
				statusCheck := []string{"rollout", "status", "daemonset/vsphere-csi-node", "--namespace=" + csiSystemNamespace}
				framework.RunKubectlOrDie(csiSystemNamespace, statusCheck...)

				// wait for csi Pods to be in running ready state
				err = fpod.WaitForPodsRunningReady(client, csiSystemNamespace, int32(num_csi_pods), 0, pollTimeout, ignoreLabels)
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
			}
		}

	})

	/* Testcase-5
		In vsphere-config  keep different passwords on each VC and check statefull set creation and reboot VC

		     Set different passwords to VC's  in config secret .
	    Re-start csi driver pods
	    Wait for some time for csi to pick the changes on vsphere-secret  file
	     Create statefull sets
	    Reboot any one VC
	    scale up/down the statefull set
	    Verify the node affinity
	    clean up the data

	*/

	/*Testcase-6
	Change VC in the UI but not on the  vsphere secret
	    Create Sc
	    Without updating the vsphere secret  on the  VC password
	    Create a statefullset
	    Untill driver restarts All the workflows will go fine.
	    stateful set creation should be successfull
	    restart the driver
	    There should be error while provisioning volume on the VC on which password has updated
	    Clean up the data

	*/

	/* Testcase-7
	Make a wrong entry in vsphere conf for one of the VC  and create a secret

	    Logs will have error messages  , But driver still be up and running .

	    until driver restarts , volume creation will be fine
	    After re-starting CSI driver  it should throw error , should not come to running state untill the error in vsphere-secret   is fixed
	*/
})
