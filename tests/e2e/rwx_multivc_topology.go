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
	"math/rand"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	ginkgo "github.com/onsi/ginkgo/v2"
	"github.com/onsi/gomega"
	cnstypes "github.com/vmware/govmomi/cns/types"
	"golang.org/x/crypto/ssh"
	v1 "k8s.io/api/core/v1"
	storagev1 "k8s.io/api/storage/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	clientset "k8s.io/client-go/kubernetes"
	"k8s.io/kubernetes/test/e2e/framework"
	fdep "k8s.io/kubernetes/test/e2e/framework/deployment"
	fnodes "k8s.io/kubernetes/test/e2e/framework/node"
	fpod "k8s.io/kubernetes/test/e2e/framework/pod"
	e2eoutput "k8s.io/kubernetes/test/e2e/framework/pod/output"
	fpv "k8s.io/kubernetes/test/e2e/framework/pv"
	fss "k8s.io/kubernetes/test/e2e/framework/statefulset"
	admissionapi "k8s.io/pod-security-admission/api"
)

var _ = ginkgo.Describe("[csi-multi-vc-topology] Multi-VC", func() {
	f := framework.NewDefaultFramework("multi-vc")
	f.NamespacePodSecurityEnforceLevel = admissionapi.LevelPrivileged
	var (
		client            clientset.Interface
		namespace         string
		bindingMode       storagev1.VolumeBindingMode
		allowedTopologies []v1.TopologySelectorLabelRequirement
		topValStartIndex  int
		topValEndIndex    int
		topkeyStartIndex  int
		//readOnly                   bool
		scParameters               map[string]string
		storagePolicyInVc1         string
		stsReplicas                int32
		accessmode                 v1.PersistentVolumeAccessMode
		scaleUpReplicaCount        int32
		scaleDownReplicaCount      int32
		stsScaleUp                 bool
		stsScaleDown               bool
		storagePolicyInVc1Vc2      string
		datastoreURLVC2            string
		storagePolicyToDelete      string
		nimbusGeneratedK8sVmPwd    string
		allMasterIps               []string
		masterIp                   string
		sshClientConfig            *ssh.ClientConfig
		isVsanHealthServiceStopped bool
		isSPSServiceStopped        bool
		isStorageProfileDeleted    bool
		pandoraSyncWaitTime        int
	)

	ginkgo.BeforeEach(func() {
		var cancel context.CancelFunc
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		client = f.ClientSet
		namespace = f.Namespace.Name

		multiVCbootstrap()

		// perform cleanup if any stale entry left for Storage Class
		sc, err := client.StorageV1().StorageClasses().Get(ctx, defaultNginxStorageClassName, metav1.GetOptions{})
		if err == nil && sc != nil {
			gomega.Expect(client.StorageV1().StorageClasses().Delete(ctx, sc.Name,
				*metav1.NewDeleteOptions(0))).NotTo(gomega.HaveOccurred())
		}

		// verify k8s nodes ready staus
		nodeList, err := fnodes.GetReadySchedulableNodes(f.ClientSet)
		framework.ExpectNoError(err, "Unable to find ready and schedulable Node")
		if !(len(nodeList.Items) > 0) {
			framework.Failf("Unable to find ready and schedulable Node")
		}

		// Env. variables read
		topologyMap := GetAndExpectStringEnvVar(topologyMap)
		storagePolicyInVc1 = GetAndExpectStringEnvVar(envStoragePolicyNameVC1)
		datastoreURLVC2 = GetAndExpectStringEnvVar(envSharedDatastoreURLVC2)
		storagePolicyToDelete = GetAndExpectStringEnvVar(envStoragePolicyNameToDeleteLater)
		nimbusGeneratedK8sVmPwd = GetAndExpectStringEnvVar(nimbusK8sVmPwd)

		// helper method call
		allowedTopologies = createAllowedTopolgies(topologyMap, topologyLength)
		allMasterIps = getK8sMasterIPs(ctx, client)

		// Global variables
		scParameters = make(map[string]string)
		accessmode = v1.ReadWriteMany
		bindingMode = storagev1.VolumeBindingWaitForFirstConsumer
		masterIp = allMasterIps[0]
		sshClientConfig = &ssh.ClientConfig{
			User: "root",
			Auth: []ssh.AuthMethod{
				ssh.Password(nimbusGeneratedK8sVmPwd),
			},
			HostKeyCallback: ssh.InsecureIgnoreHostKey(),
		}

		if isVsanHealthServiceStopped {
			vCenterHostname := strings.Split(multiVCe2eVSphere.multivcConfig.Global.VCenterHostname, ",")
			vcAddress := vCenterHostname[0] + ":" + sshdPort
			framework.Logf("Bringing vsanhealth up before terminating the test")
			startVCServiceWait4VPs(ctx, vcAddress, vsanhealthServiceName, &isVsanHealthServiceStopped)
		}

		if isSPSServiceStopped {
			vCenterHostname := strings.Split(multiVCe2eVSphere.multivcConfig.Global.VCenterHostname, ",")
			vcAddress := vCenterHostname[0] + ":" + sshdPort
			framework.Logf("Bringing sps up before terminating the test")
			startVCServiceWait4VPs(ctx, vcAddress, spsServiceName, &isSPSServiceStopped)
			isSPSServiceStopped = false
		}

		if isStorageProfileDeleted {
			clientIndex := 0
			err = createStorageProfile(masterIp, sshClientConfig, storagePolicyToDelete, clientIndex)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}

		if os.Getenv(envPandoraSyncWaitTime) != "" {
			pandoraSyncWaitTime, err = strconv.Atoi(os.Getenv(envPandoraSyncWaitTime))
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		} else {
			pandoraSyncWaitTime = defaultPandoraSyncWaitTime
			fmt.Println(pandoraSyncWaitTime)
		}

	})

	ginkgo.AfterEach(func() {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		framework.Logf("Perform cleanup of any statefulset entry left")
		ginkgo.By(fmt.Sprintf("Deleting all statefulsets in namespace: %v", namespace))
		fss.DeleteAllStatefulSets(client, namespace)
		ginkgo.By(fmt.Sprintf("Deleting service nginx in namespace: %v", namespace))
		err := client.CoreV1().Services(namespace).Delete(ctx, servicename, *metav1.NewDeleteOptions(0))
		if !apierrors.IsNotFound(err) {
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}

		framework.Logf("Perform cleanup of any left over stale PVs")
		allPvs, err := client.CoreV1().PersistentVolumes().List(ctx, metav1.ListOptions{})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		for _, pv := range allPvs.Items {
			err := client.CoreV1().PersistentVolumes().Delete(ctx, pv.Name, metav1.DeleteOptions{})
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}
	})

	/* TESTCASE-1
	Deployment Pods and Standalone Pods creation with different SC. Perform Scaleup of Deployment Pods
	SC1 → WFC Binding Mode with default values
	SC2 → Immediate Binding Mode with all allowed topologies i.e. zone-1 > zone-2 > zone-3

		Steps:
	    1. Create SC1 with default values so that all AZ's should be considered for volume provisioning.
	    2. Create PVC with "RWX" access mode using SC created in step #1.
	    3. Wait for PVC to reach Bound state
	    4. Create deployment Pod with replica count 3 and specify Node selector terms.
	    5. Volumes should get distributed across all AZs.
	    6. Pods should be running on the appropriate nodes as mentioned in Deployment node selector terms.
	    7. Verify CNS metadata for Pod, PVC and PV.
	    8. Create SC2 with Immediate Binding mode and with all levels of allowed topology set considering all 3 VC's.
	    9. Create PVC with "RWX" access mpde using SC created in step #8.
	    10. Verify PVC should reach to Bound state.
	    11. Create 5 Pods with different access mode and attach it to PVC created in step #9.
	    12. Verify Pods should reach to Running state.
	    13. Try reading/writing onto the volume from different Pods.
	    14. Perform scale up of deployment Pods to replica count 5. Verify Scaling operation should go smooth.
	    15. Perform cleanup by deleting deployment Pods, PVCs and the StorageClass (SC)
	*/

	ginkgo.It("TC1", ginkgo.Label(p0, block, vanilla, multiVc, newTest), func() {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		// here in 3-VC setup, node affinity is taken as  k8s-zone -> zone-1 i.e only VC1 allowed topology
		topValStartIndex = 0
		topValEndIndex = 1

		ginkgo.By("Set specific allowed topology")
		allowedTopologies = setSpecificAllowedTopology(allowedTopologies, topkeyStartIndex, topValStartIndex,
			topValEndIndex)

		labelsMap := make(map[string]string)
		labelsMap["app"] = "test"
		scParameters := make(map[string]string)
		scParameters[scParamFsType] = nfs4FSType
		replica := 1

		nodeSelectorTerms := getNodeSelectorMapForDeploymentPods(allowedTopologies)

		ginkgo.By(fmt.Sprintf("Creating Storage Class with access mode %q and fstype %q", v1.ReadWriteMany, nfs4FSType))
		storageclass1, pvclaim1, _, err := createAndVerifyPvcWithStorageClass(client, namespace, labelsMap, scParameters, diskSize, allowedTopologies, bindingMode, false, accessmode, "", false, false, 1, false, true)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		defer func() {
			err := client.StorageV1().StorageClasses().Delete(ctx, storageclass1.Name, *metav1.NewDeleteOptions(0))
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}()

		ginkgo.By("Create Deployments")
		deployment, err := createDeployment(ctx, client, int32(replica), labelsMap, nodeSelectorTerms, namespace, []*v1.PersistentVolumeClaim{pvclaim1[0]}, execRWXCommandPod1, false, nginxImage)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		defer func() {
			framework.Logf("Delete deployment set")
			err := client.AppsV1().Deployments(namespace).Delete(ctx, deployment.Name, *metav1.NewDeleteOptions(0))
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}()

		ginkgo.By("Wait for deployment pods to be up and running")
		pods, err := fdep.GetPodsForDeployment(client, deployment)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		pod := pods.Items[0]
		err = fpod.WaitForPodNameRunningInNamespace(client, pod.Name, namespace)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		pv := getPvFromClaim(client, pvclaim1[0].Namespace, pvclaim1[0].Name)
		volHandle := pv.Spec.CSI.VolumeHandle

		ginkgo.By("Verify volume metadata for POD, PVC and PV")
		err = waitAndVerifyCnsVolumeMetadata(volHandle, pvclaim1[0], pv, &pod)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Verify deployment pods are scheduled on a selected node selector terms")
		err = verifyDeploymentPodNodeAffinity(ctx, client, namespace, allowedTopologies, pods, pv)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		// here in 3-VC setup, node affinity is taken as  k8s-zone -> zone-1,zone-2 and zone-3 i.e all 3 VCs allowed topology
		topValStartIndex = 0
		topValEndIndex = 1

		ginkgo.By("Set specific allowed topology")
		allowedTopologies = setSpecificAllowedTopology(allowedTopologies, topkeyStartIndex, topValStartIndex,
			topValEndIndex)

		labelsMap = make(map[string]string)
		labelsMap["app"] = "e2e"
		scParameters = make(map[string]string)
		scParameters[scParamFsType] = nfs4FSType

		ginkgo.By(fmt.Sprintf("Creating Storage Class with access mode %q and fstype %q", v1.ReadWriteMany, nfs4FSType))
		storageclass2, _, _, err := createAndVerifyPvcWithStorageClass(client, namespace, labelsMap, scParameters, diskSize,
			allowedTopologies, "", false, accessmode, "", false, false, 1, true, false)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		defer func() {
			err := client.StorageV1().StorageClasses().Delete(ctx, storageclass2.Name, *metav1.NewDeleteOptions(0))
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}()

		// ginkgo.By("Create 3 standalone Pods using the same PVC")
		// var standalonePod *v1.Pod
		// for i := 0; i < 3; i++ {
		// 	if i == 2 {
		// 		readOnly = true
		// 	}
		// 	standalonePod, err = createStandalonePodsForRWXVolume(client, ctx, namespace, nil, pvclaim2, false, execRWXCommandPod, readOnly)
		// 	gomega.Expect(err).NotTo(gomega.HaveOccurred())

		// 	ginkgo.By("Verify volume metadata for POD, PVC and PV")
		// 	err = waitAndVerifyCnsVolumeMetadata(volHandle, pvclaim2, pv2, standalonePod)
		// 	gomega.Expect(err).NotTo(gomega.HaveOccurred())
		// }

		// defer func(pod *v1.Pod) {
		// 	ginkgo.By(fmt.Sprintf("Deleting the pod %s in namespace %s", pod.Name, namespace))
		// 	err := fpod.DeletePodWithWait(client, pod)
		// 	gomega.Expect(err).NotTo(gomega.HaveOccurred())
		// }(standalonePod)

	})

	/* TESTCASE-2
		StatefulSet Workload with default Pod Management and Scale-Up/Scale-Down Operations
		SC → Storage Policy (VC-1) with specific allowed topology i.e. k8s-zone:zone-1 and with WFC Binding mode

		Steps:
	    1. Create SC with storage policy name (tagged with vSAN ds) available in single VC (VC-1)
	    2. Create StatefulSet with 5 replicas using default Pod management policy
		3. Allow time for PVCs and Pods to reach Bound and Running states, respectively.
		4. Volume provisioning should happen on the AZ as specified in the SC.
		5. Pod placement can happen in any available availability zones (AZs)
		6. Verify CNS metadata for PVC and Pod.
		7. Scale-up/Scale-down the statefulset. Verify scaling operation went successful.
		8. Volume provisioning should happen on the AZ as specified in the SC but Pod placement can occur in any availability zones (AZs) during the scale-up operation.
		9. Perform cleanup by deleting StatefulSets, PVCs, and the StorageClass (SC)
	*/

	ginkgo.It("TC2", ginkgo.Label(p0, block, vanilla, multiVc, newTest), func() {

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		/* here we are considering storage policy of VC1 and the allowed topology is k8s-zone -> zone-1 i.e. VC1
		 */

		stsReplicas = 5
		scParameters[scParamStoragePolicyName] = storagePolicyInVc1
		topValStartIndex = 0
		topValEndIndex = 1
		scaleUpReplicaCount = 9
		scaleDownReplicaCount = 2

		ginkgo.By("Set specific allowed topology")
		allowedTopologies = setSpecificAllowedTopology(allowedTopologies, topkeyStartIndex, topValStartIndex,
			topValEndIndex)

		ginkgo.By("Create StorageClass with storage policy specified")
		scSpec := getVSphereStorageClassSpec(defaultNginxStorageClassName, scParameters, allowedTopologies, "",
			bindingMode, false)
		sc, err := client.StorageV1().StorageClasses().Create(ctx, scSpec, metav1.CreateOptions{})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		defer func() {
			err = client.StorageV1().StorageClasses().Delete(ctx, sc.Name, *metav1.NewDeleteOptions(0))
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}()

		ginkgo.By("Create StatefulSet and verify pv affinity and pod affinity details")
		service, statefulset, err := createStafeulSetAndVerifyPVAndPodNodeAffinty(ctx, client, namespace, false, stsReplicas, false, allowedTopologies, 0, false,
			false, false, "", accessmode, sc, false)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		defer func() {
			fss.DeleteAllStatefulSets(client, namespace)
			deleteService(namespace, client, service)
		}()

		// ginkgo.By("Verify volume placement")
		// isCorrectPlacement := e2eVSphere.verifyPreferredDatastoreMatch(volHandle, datastoreUrlsRack3)
		// gomega.Expect(isCorrectPlacement).To(gomega.BeTrue(), fmt.Sprintf("Volume provisioning has happened on the wrong datastore. Expected 'true', got '%v'", isCorrectPlacement))

		ginkgo.By("Perform scaleup/scaledown operation on statefulsets and " +
			"verify pv affinity and pod affinity")
		err = performScalingOnStatefulSetAndVerifyPvNodeAffinity(ctx, client, scaleUpReplicaCount,
			scaleDownReplicaCount, statefulset, false, namespace,
			allowedTopologies, stsScaleUp, stsScaleDown, false)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
	})

	/* TESTCASE-3
	PVC and multiple Pods creation → Same Policy is available in two VCs
	SC → Same Storage Policy Name (VC-1 and VC-2) with specific allowed topology i.e. k8s-zone:zone-1,zone-2 and with Immediate Binding mode

	Steps:
	1. Create SC with Storage policy name available in VC1 and VC2 and allowed topology set to k8s-zone:zone-1,zone-2 and with Immediate Binding mode
	2. Create PVC with "RWX" access mode using SC created in step #1.
	3. Create Deployment Pods with replica count 2 using PVC created in step #2.
	4. Allow time for PVCs and Pods to reach Bound and Running states, respectively.
	5. PVC should get created on the AZ as given in the SC.
	6. Pod placement can happen on any AZ.
	7. Verify CNS metadata for PVC and Pod.
	8. Perform Scale up of deployment Pod replica to count 5
	9. Verify scaling operation should go successful.
	10. Perform scale down of deployment Pod to count 1.
	11. Verify scaling down operation went smooth.
	12. Perform cleanup by deleting Deployment, PVCs, and the SC
	*/

	ginkgo.It("Workload creation when storage policy available in multivc setup"+
		"is given in SC", ginkgo.Label(p0, block, vanilla, multiVc, newTest), func() {

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		/*  In case of 3-VC setup, we are considering storage policy of VC1 and VC2 and the allowed
		topology is k8s-zone -> zone-1, zone-2 i.e VC1 and VC2 allowed topologies.
		*/

		topValStartIndex = 0
		topValEndIndex = 2
		labelsMap := make(map[string]string)
		labelsMap["app"] = "test"
		scParameters := make(map[string]string)
		scParameters[scParamFsType] = nfs4FSType
		scParameters[scParamStoragePolicyName] = storagePolicyInVc1Vc2
		replica := int32(3)

		ginkgo.By("Set specific allowed topology")
		allowedTopologies = setSpecificAllowedTopology(allowedTopologies, topkeyStartIndex, topValStartIndex,
			topValEndIndex)

		ginkgo.By(fmt.Sprintf("Creating Storage Class with access mode %q and fstype %q", v1.ReadWriteMany, nfs4FSType))
		storageclass, pvclaim, pv, err := createAndVerifyPvcWithStorageClass(client, namespace, labelsMap, scParameters, diskSize, allowedTopologies, bindingMode, false, accessmode, "", false, false, 1, false, false)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		volHandle := pv[0].Spec.CSI.VolumeHandle
		defer func() {
			err := client.StorageV1().StorageClasses().Delete(ctx, storageclass.Name, *metav1.NewDeleteOptions(0))
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}()

		defer func() {
			err := fpv.DeletePersistentVolumeClaim(client, pvclaim[0].Name, namespace)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			err = e2eVSphere.waitForCNSVolumeToBeDeleted(volHandle)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}()

		ginkgo.By("Create Deployments")
		deployment, err := createDeployment(ctx, client, int32(replica), labelsMap, nil, namespace, []*v1.PersistentVolumeClaim{pvclaim[0]}, execRWXCommandPod1, false, nginxImage)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		defer func() {
			framework.Logf("Delete deployment set")
			err := client.AppsV1().Deployments(namespace).Delete(ctx, deployment.Name, *metav1.NewDeleteOptions(0))
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}()

		ginkgo.By("Wait for deployment pods to be up and running")
		pods, err := fdep.GetPodsForDeployment(client, deployment)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		pod1 := pods.Items[0]
		pod2 := pods.Items[1]
		err = fpod.WaitForPodNameRunningInNamespace(client, pod1.Name, namespace)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		// Check filesystem used to mount volume inside pod is xfs as expeted
		ginkgo.By("Verify if filesystem used to mount volume is xfs as expected")
		_, err = e2eoutput.LookForStringInPodExec(namespace, pod1.Name, []string{"/bin/cat", "/mnt/volume1/fstype"},
			nfs4FSType, time.Minute)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		// Create file1.txt at mountpath inside pod.
		ginkgo.By(fmt.Sprintf("Creating file file1.txt at mountpath inside pod: %v", pod1.Name))
		data1 := "This file file1.txt is written before migration"
		filePath1 := "/mnt/volume1/file1.txt"
		writeDataOnFileFromPod(namespace, pod1.Name, filePath1, data1)

		ginkgo.By("Verify that data can be successfully read from file1.txt which was written before migration")
		output := readFileFromPod(namespace, pod2.Name, filePath1)
		gomega.Expect(output == data1+"\n").To(gomega.BeTrue(), "Pod is not able to read file1.txt post migration")

		// Create new file file2.txt at mountpath inside pod.
		ginkgo.By(fmt.Sprintf("Creating file file2.txt at mountpath inside pod: %v", pod2.Name))
		data2 := "This file file2.txt is written post migration"
		filePath2 := "/mnt/volume1/file2.txt"
		writeDataOnFileFromPod(namespace, pod2.Name, filePath2, data2)

		ginkgo.By("Verify that data written post migration can be successfully read from file2.txt")
		output = readFileFromPod(namespace, pod2.Name, filePath2)
		gomega.Expect(output == data2+"\n").To(gomega.BeTrue(), "Pod is not able to read file2.txt")

		ginkgo.By("Verify volume metadata for POD, PVC and PV")
		err = waitAndVerifyCnsVolumeMetadata(volHandle, pvclaim[0], pv[0], &pod1)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Scale up deployment to 5 replicas")
		replica = 5
		_, err = scaleDeploymentPods(ctx, client, deployment, namespace, replica)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Scale down deployment to replica 1")
		replica = 1
		_, err = scaleDeploymentPods(ctx, client, deployment, namespace, replica)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
	})

	/* TESTCASE-4
	Deployment Pod creation with allowed topology details in SC specific to VC-2
	SC → specific allowed topology i.e. k8s-zone:zone-2 and datastore url of VC-2 with WFC Binding mode

	Steps:
	1. Create SC with allowed topology of VC-2 and use datastore url from same VC with WFC Binding mode.
	2. Create PVC with "RWX" access mode using SC created in step #1.
	3. Verify PVC should reach to Bound state and it should be created on the AZ as given in the SC.
	4. Create deployment Pods with replica count 3 using PVC created in step #2.
	5. Wait for Pods to reach running ready state.
	6. Pods should get created on any VC or any AZ.
	7. Try reading/writing data into the volume.
	8. Perform cleanup by deleting deployment Pods, PVC and SC
	*/

	ginkgo.It("T***C4", ginkgo.Label(p2, block, vanilla, multiVc, newTest, negative), func() {

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		topValStartIndex = 0
		topValEndIndex = 1
		scParameters[scParamDatastoreURL] = datastoreURLVC2

		ginkgo.By("Set specific allowed topology")
		allowedTopologies = setSpecificAllowedTopology(allowedTopologies, topkeyStartIndex, topValStartIndex,
			topValEndIndex)

		storageclass, pvclaim, err := createPVCAndStorageClass(client,
			namespace, nil, scParameters, "", allowedTopologies, "", false, "")
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		defer func() {
			err = client.StorageV1().StorageClasses().Delete(ctx, storageclass.Name, *metav1.NewDeleteOptions(0))
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			err = fpv.DeletePersistentVolumeClaim(client, pvclaim.Name, namespace)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}()

		ginkgo.By("Expect claim to fail as non-compatible datastore url is specified in Storage Class")
		framework.ExpectError(fpv.WaitForPersistentVolumeClaimPhase(v1.ClaimBound,
			client, pvclaim.Namespace, pvclaim.Name, framework.Poll, framework.ClaimProvisionTimeout))
		expectedErrMsg := "failed to create volume"
		err = waitForEvent(ctx, client, namespace, expectedErrMsg, pvclaim.Name)
		gomega.Expect(err).NotTo(gomega.HaveOccurred(), fmt.Sprintf("Expected error : %q", expectedErrMsg))
	})

	/* TESTCASE-5
	Deploy Statefulset workload with allowed topology details in SC specific to VC-1 and VC-3
	SC → specific allowed topology like K8s-zone: zone-1 and zone-3 and with WFC Binding mode

		Steps//
	    1. Create SC with allowedTopology details set to VC-1 and VC-3 availability Zone i.e. zone-1 and zone-3
	    2. Create Statefulset with replica  count 3
	    3. Wait for PVC to reach bound state and POD to reach Running state
	    4. Volume provision should happen on the AZ as specified in the SC.
	    5. Pods should get created on any AZ i.e. across all 3 VCs
	    6. Scale-up the statefuset replica count to 7.
	    7. Verify scaling operation went smooth and new volumes should get created on the VC-1 and VC-3 AZ.
		8. Newly created Pods should get created across any of the AZs.
		9. Perform Scale down operation. Reduce the replica count from 7 to 2.
		10. Verify scale down operation went smooth.
	    11. Perform cleanup by deleting StatefulSets, PVCs, and the StorageClass (SC)
	*/

	/* TESTCASE-6
		Deploy workload with allowed topology details in SC specific to VC-2 and VC-3 but storage policy is passed from VC-3 (vSAN DS)
		Storage Policy (VC-1) and datastore url (VC-2) both passed

		Steps//
	    1. Create SC1 with allowed Topology details set to VC-2 and  VC-3 availability Zone and specify storage policy tagged to vSAN ds of VC-3 whch is not vSAN FS enabled.
	    2. Create PVC using the SC created in step #1
	    3. PVC creation should fail with an appropriate error message as vSAN FS is disabled in VC-3
	    4. Create SC2 with allowedTopology details set to all AZs i.e. Zone-1, Zone-2 and Zone-3. Also pass storage policy tagged to vSAN DS from VC-1 and Datastore url tagged to vSAN DS from VC-2 with Immediate Binding mode.
	    5. Create PVC using the SC created in step #4
	    6. PVC creation should fail with an appropriate error message i.e. no compatible datastore found
	    7. Perform cleanup by deleting SC's, PVC's and SC
	*/

	/* TESTCASE-7
		Create storage policy in VC-1 and VC-2 and create Storage class with the same and  delete Storage policy in VC1 . expected to go to VC2
		Storage Policy (VC-1) and datastore url (VC-2) both passed

		Steps//
	    1. Create Storage policy with same name which is present in both VC1 and VC2
	    2. Create Storage Class with allowed topology specify to VC-1 and VC-2 and specify storage policy created in step #1.
		3. Delete storage policy from VC1
	    4. Create few PVCs (2 to 3 PVCs) with "RWX" access mode using SC created in step #1
	    5. Create Deployment Pods with replica count 2 and Standalone Pods using PVC created in step #3.
	    6. Verify PVC should reach Bound state and Pods should reach Running state.
	    7. Volume provisioning can happen on the AZ as specified in the SC i.e. it should provision volume on VC2 AZ as VC-1 storage policy is deleted
	    8. Pod placement can happen on any AZ.
	    9. Perform Scaleup operation on deployment Pods. Increase the replica count from 2 to 4.
	    10. Verify CNS metadata for few Pods and PVC.
	    11. Perform Scaleup by deleting Pods, PVC and SC.
	*/

	ginkgo.It("Create storage policy in multivc and later delete storage policy from "+
		"one of the VC", ginkgo.Label(p2, block, vanilla, multiVc, newTest, negative), func() {

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		topValStartIndex = 1
		topValEndIndex = 2
		stsReplicas = 3
		clientIndex := 0

		scParameters[scParamStoragePolicyName] = storagePolicyToDelete

		/* Considering partial allowed topology of VC2 in case of 2-VC setup i.e. k8s-zone -> zone-2
		And, allowed topology of VC2 in case of 3-VC setup i.e. k8s-zone -> zone-2
		*/

		ginkgo.By("Set specific allowed topology")
		allowedTopologies = setSpecificAllowedTopology(allowedTopologies, topkeyStartIndex,
			topValStartIndex, topValEndIndex)

		ginkgo.By("Create StorageClass")
		scSpec := getVSphereStorageClassSpec(defaultNginxStorageClassName, scParameters, nil, "",
			"", false)
		sc, err := client.StorageV1().StorageClasses().Create(ctx, scSpec, metav1.CreateOptions{})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		defer func() {
			err = client.StorageV1().StorageClasses().Delete(ctx, sc.Name, *metav1.NewDeleteOptions(0))
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}()

		ginkgo.By("Delete Storage Policy created in VC1")
		err = deleteStorageProfile(masterIp, sshClientConfig, storagePolicyToDelete, clientIndex)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		isStorageProfileDeleted = true
		defer func() {
			if isStorageProfileDeleted {
				err = createStorageProfile(masterIp, sshClientConfig, storagePolicyToDelete, clientIndex)
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
				isStorageProfileDeleted = false
			}
		}()

		ginkgo.By("Create StatefulSet and verify pv affinity and pod affinity details")
		service, _, err := createStafeulSetAndVerifyPVAndPodNodeAffinty(ctx, client, namespace, false,
			stsReplicas, false, allowedTopologies, 0, false,
			false, false, "", "", nil, false)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		defer func() {
			fss.DeleteAllStatefulSets(client, namespace)
			deleteService(namespace, client, service)
		}()
	})

	/* TESTCASE-8
	Static PVC creation
	SC → specific allowed topology ie. VC1 K8s:zone-zone-1

	Steps//
	1. Create a file share on any 1 VC
	2. Create PV using the above created File Share
	3. Create PVC using above created  PV
	4. Wait for PV and PVC to be in Bound state
	5. Create Pod using PVC created in step #3
	6. Verify Pod should reach to Running state.
	7. Verify volume provisioning should happen on the AZ where File Share is created, Pod can be created on any AZ.
	8. Verify CNS volume metadata for Pod and PVC.
	9. Perform cleanup by deleting PVC, PV and SC.
	*/

	/* TESTCASE-9
		Storage policy is present in VC1 and VC1 is under reboot
		SC → specific allowed topology i.e. k8s-zone:zone-1 with WFC Binding mode and with Storage Policy (vSAN DS of VC-1)

		Steps//
	    1. Create a StorageClass (SC) tagged to vSAN datastore of VC-1.
	    2. Create a StatefulSet with a replica count of 5 and a size of 5Gi, using the previously created StorageClass.
	    3. Initiate a reboot of VC-1 while statefulSet creation is in progress.
	    4. Confirm that volume creation is in a "Pending" state while VC-1 is rebooting.
	    5. Wait for VC-1 to be fully operational.
	    6. Ensure that, upon VC-1 recovery, all volumes come up on the worker nodes within VC-1.
	    7. Confirm that volume provisioning occurs exclusively on VC-1.
	    8. Pod placement can happen on any AZ.
	    9. Perform cleanup by deleting Pods, PVCs and SC
	*/

	ginkgo.It("TCtest", ginkgo.Label(p1, block,
		vanilla, multiVc, newTest, negative), func() {

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		stsReplicas = 3
		scParameters[scParamStoragePolicyName] = storagePolicyInVc1
		scaleDownReplicaCount = 2
		scaleUpReplicaCount = 7
		topValStartIndex = 0
		topValEndIndex = 1
		sts_count := 2

		// here, we are considering the allowed topology of VC1 i.e. k8s-zone -> zone-1

		ginkgo.By("Set specific allowed topology")
		allowedTopologies = setSpecificAllowedTopology(allowedTopologies, topkeyStartIndex, topValStartIndex,
			topValEndIndex)

		ginkgo.By("Create StorageClass with storage policy specified")
		scSpec := getVSphereStorageClassSpec(defaultNginxStorageClassName, scParameters, nil, "",
			"", false)
		sc, err := client.StorageV1().StorageClasses().Create(ctx, scSpec, metav1.CreateOptions{})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		defer func() {
			err = client.StorageV1().StorageClasses().Delete(ctx, sc.Name, *metav1.NewDeleteOptions(0))
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}()

		ginkgo.By("Create service")
		service := CreateService(namespace, client)
		defer func() {
			deleteService(namespace, client, service)
		}()

		ginkgo.By("Rebooting VC1")
		vCenterHostname := strings.Split(multiVCe2eVSphere.multivcConfig.Global.VCenterHostname, ",")
		vcAddress := vCenterHostname[0] + ":" + sshdPort
		framework.Logf("vcAddress - %s ", vcAddress)
		err = invokeVCenterReboot(vcAddress)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		err = waitForHostToBeUp(vCenterHostname[0])
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		ginkgo.By("Done with reboot")

		ginkgo.By("Create 3 StatefulSet with replica count 3")
		statefulSets := createParallelStatefulSetSpec(namespace, sts_count, stsReplicas)
		var wg sync.WaitGroup
		wg.Add(sts_count)
		for i := 0; i < len(statefulSets); i++ {
			go createParallelStatefulSets(client, namespace, statefulSets[i],
				stsReplicas, &wg)

		}
		wg.Wait()

		essentialServices := []string{spsServiceName, vsanhealthServiceName, vpxdServiceName}
		checkVcenterServicesRunning(ctx, vcAddress, essentialServices)

		//After reboot
		multiVCbootstrap()

		ginkgo.By("Waiting for StatefulSets Pods to be in Ready State")
		err = waitForStsPodsToBeInReadyRunningState(ctx, client, namespace, statefulSets)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		defer func() {
			deleteAllStsAndPodsPVCsInNamespace(ctx, client, namespace)
		}()

		ginkgo.By("Verify PV node affinity and that the PODS are running on appropriate node")
		for i := 0; i < len(statefulSets); i++ {
			err = verifyPVnodeAffinityAndPODnodedetailsForStatefulsetsLevel5(ctx, client, statefulSets[i],
				namespace, allowedTopologies, false)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}

		ginkgo.By("Perform scaleup/scaledown operation on statefulsets and " +
			"verify pv affinity and pod affinity")
		for i := 0; i < len(statefulSets); i++ {
			err = performScalingOnStatefulSetAndVerifyPvNodeAffinity(ctx, client, scaleUpReplicaCount,
				scaleDownReplicaCount, statefulSets[i], false, namespace,
				allowedTopologies, stsScaleUp, stsScaleDown, false)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}
	})

	/* TESTCASE-10
	VSAN-health down on VC1
	SC → default values with Immediate Binding mode

	Steps//
	1. Create SC with default values  so all the AZ's should be considered for volume provisioning.
	2. Create few dynamic PVCs and add labels to PVCs and PVs
	3. Bring down the VSAN-health on VC-1
	4. Since vSAN-health on VC1 is down, all the volumes should get created either on VC-2 or VC-3 AZ
	5. Verify PVC should reach to Bound state.
	6. Create deployment Pods for each PVC with replica count 2.
	7. Update the label on PV and PVC created in step #2
	8. Bring up the vSAN-health on VC-1.
	9. Scale up all deployment Pods to replica count 5.
	10. Wait for 2 full sync cycle
	11. Verify CNS-metadata for the volumes that are created
	12. Verify that the volume provisioning should resume on VC-1 as well
	13. Scale down the deployment pod replica count to 1.
	14. Verify scale down operation went successful.
	15. Perform cleanup by deleting Pods, PVCs and SC
	*/

	ginkgo.It("Create workloads when VSAN-health is down on VC1", ginkgo.Label(p1, block,
		vanilla, multiVc, newTest, negative), func() {

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		stsReplicas = 5
		pvcCount := 3
		scaleDownReplicaCount = 1
		scaleUpReplicaCount = 10
		labelKey := "volId"
		labelValue := "pvcVolume"
		labels := make(map[string]string)
		labels[labelKey] = labelValue
		sts_count := 2
		var fullSyncWaitTime int
		var err error

		// Read full-sync value.
		if os.Getenv(envFullSyncWaitTime) != "" {
			fullSyncWaitTime, err = strconv.Atoi(os.Getenv(envFullSyncWaitTime))
			framework.Logf("Full-Sync interval time value is = %v", fullSyncWaitTime)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}

		/* here, For 2-VC setup, we are considering all the allowed topologies of VC1 and VC2
		i.e. k8s-zone -> zone-1,zone-2,zone-3,zone-4,zone-5

		For 3-VC setup, we are considering all the allowed topologies of VC1, VC2 and VC3
		i.e. k8s-zone -> zone-1,zone-2,zone-3
		*/

		ginkgo.By("Create StorageClass with default parameters")
		scSpec := getVSphereStorageClassSpec(defaultNginxStorageClassName, nil, nil, "",
			"", false)
		sc, err := client.StorageV1().StorageClasses().Create(ctx, scSpec, metav1.CreateOptions{})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		defer func() {
			err = client.StorageV1().StorageClasses().Delete(ctx, sc.Name, *metav1.NewDeleteOptions(0))
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}()

		ginkgo.By("Trigger multiple PVCs")
		pvclaimsList := createMultiplePVCsInParallel(ctx, client, namespace, sc, pvcCount, labels)

		ginkgo.By("Verify PVC claim to be in bound phase and create POD for each PVC")
		for i := 0; i < len(pvclaimsList); i++ {
			pvc, err := fpv.WaitForPVClaimBoundPhase(client,
				[]*v1.PersistentVolumeClaim{pvclaimsList[i]}, framework.ClaimProvisionTimeout)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			gomega.Expect(pvc).NotTo(gomega.BeEmpty())

		}
		defer func() {
			ginkgo.By("Deleting PVC's and PV's")
			for i := 0; i < len(pvclaimsList); i++ {
				pv := getPvFromClaim(client, pvclaimsList[i].Namespace, pvclaimsList[i].Name)
				err = fpv.DeletePersistentVolumeClaim(client, pvclaimsList[i].Name, namespace)
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
				framework.ExpectNoError(fpv.WaitForPersistentVolumeDeleted(client, pv.Name, poll, pollTimeoutShort))
				err = multiVCe2eVSphere.waitForCNSVolumeToBeDeletedInMultiVC(pv.Spec.CSI.VolumeHandle)
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
			}

			deleteAllStsAndPodsPVCsInNamespace(ctx, client, namespace)
		}()

		ginkgo.By("Bring down Vsan-health service on VC1")
		vCenterHostname := strings.Split(multiVCe2eVSphere.multivcConfig.Global.VCenterHostname, ",")
		vcAddress := vCenterHostname[0] + ":" + sshdPort
		framework.Logf("vcAddress - %s ", vcAddress)
		err = invokeVCenterServiceControl(stopOperation, vsanhealthServiceName, vcAddress)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		isVsanHealthServiceStopped = true
		defer func() {
			if isVsanHealthServiceStopped {
				framework.Logf("Bringing vsanhealth up before terminating the test")
				startVCServiceWait4VPs(ctx, vcAddress, vsanhealthServiceName, &isVsanHealthServiceStopped)
			}
		}()

		ginkgo.By("Create 2 StatefulSet with replica count 5 when vsan-health is down")
		statefulSets := createParallelStatefulSetSpec(namespace, sts_count, stsReplicas)
		var wg sync.WaitGroup
		wg.Add(sts_count)
		for i := 0; i < len(statefulSets); i++ {
			go createParallelStatefulSets(client, namespace, statefulSets[i],
				stsReplicas, &wg)
		}
		wg.Wait()

		labelKey = "volIdNew"
		labelValue = "pvcVolumeNew"
		labels = make(map[string]string)
		labels[labelKey] = labelValue

		ginkgo.By("Updating labels for PVC and PV")
		for i := 0; i < len(pvclaimsList); i++ {
			pv := getPvFromClaim(client, pvclaimsList[i].Namespace, pvclaimsList[i].Name)
			framework.Logf("Updating labels %+v for pvc %s in namespace %s", labels, pvclaimsList[i].Name,
				pvclaimsList[i].Namespace)
			pvc, err := client.CoreV1().PersistentVolumeClaims(namespace).Get(ctx, pvclaimsList[i].Name, metav1.GetOptions{})
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			pvc.Labels = labels
			_, err = client.CoreV1().PersistentVolumeClaims(namespace).Update(ctx, pvc, metav1.UpdateOptions{})
			gomega.Expect(err).NotTo(gomega.HaveOccurred())

			framework.Logf("Updating labels %+v for pv %s", labels, pv.Name)
			pv.Labels = labels
			_, err = client.CoreV1().PersistentVolumes().Update(ctx, pv, metav1.UpdateOptions{})
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}

		ginkgo.By(fmt.Sprintln("Starting vsan-health on the vCenter host"))
		startVCServiceWait4VPs(ctx, vcAddress, vsanhealthServiceName, &isVsanHealthServiceStopped)

		ginkgo.By(fmt.Sprintf("Sleeping for %v seconds to allow full sync finish", fullSyncWaitTime))
		time.Sleep(time.Duration(fullSyncWaitTime) * time.Second)

		ginkgo.By("Waiting for labels to be updated for PVC and PV")
		for i := 0; i < len(pvclaimsList); i++ {
			pv := getPvFromClaim(client, pvclaimsList[i].Namespace, pvclaimsList[i].Name)

			framework.Logf("Waiting for labels %+v to be updated for pvc %s in namespace %s",
				labels, pvclaimsList[i].Name, pvclaimsList[i].Namespace)
			err = multiVCe2eVSphere.waitForLabelsToBeUpdatedInMultiVC(pv.Spec.CSI.VolumeHandle, labels,
				string(cnstypes.CnsKubernetesEntityTypePVC), pvclaimsList[i].Name, pvclaimsList[i].Namespace)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())

			framework.Logf("Waiting for labels %+v to be updated for pv %s", labels, pv.Name)
			err = multiVCe2eVSphere.waitForLabelsToBeUpdatedInMultiVC(pv.Spec.CSI.VolumeHandle, labels,
				string(cnstypes.CnsKubernetesEntityTypePV), pv.Name, pv.Namespace)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}

		ginkgo.By("Waiting for StatefulSets Pods to be in Ready State")
		err = waitForStsPodsToBeInReadyRunningState(ctx, client, namespace, statefulSets)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Verify PV node affinity and that the PODS are running on appropriate node")
		for i := 0; i < len(statefulSets); i++ {
			err = verifyPVnodeAffinityAndPODnodedetailsForStatefulsetsLevel5(ctx, client, statefulSets[i],
				namespace, allowedTopologies, false)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}

		ginkgo.By("Perform scaleup/scaledown operation on statefulsets and " +
			"verify pv affinity and pod affinity")
		err = performScalingOnStatefulSetAndVerifyPvNodeAffinity(ctx, client, scaleUpReplicaCount,
			scaleDownReplicaCount, statefulSets[0], false, namespace,
			allowedTopologies, stsScaleUp, stsScaleDown, false)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
	})

	/* TESTCASE-11
	SPS down on VC2 + use storage policy while creating StatefulSet
	SC → default values with Immediate Binding mode

	Steps//
	1. Create SC with default values  so all the AZ's should be considered for volume provisioning.
	2. Create few dynamic PVCs and add labels to PVCs and PVs
	3. Bring down the VSAN-health on VC-1
	4. Since vSAN-health on VC1 is down, all the volumes should get created either on VC-2 or VC-3 AZ
	5. Verify PVC should reach to Bound state.
	6. Create deployment Pods for each PVC with replica count 2.
	7. Update the label on PV and PVC created in step #2
	8. Bring up the vSAN-health on VC-1.
	9. Scale up all deployment Pods to replica count 5.
	10. Wait for 2 full sync cycle
	11. Verify CNS-metadata for the volumes that are created
	12. Verify that the volume provisioning should resume on VC-1 as well
	13. Scale down the deployment pod replica count to 1.
	14. Verify scale down operation went successful.
	15. Perform cleanup by deleting Pods, PVCs and SC
	*/

	ginkgo.It("Create workloads with storage policy given in SC and when sps service is down", ginkgo.Label(p1,
		block, vanilla, multiVc, newTest, negative), func() {

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		/* here we are considering storage policy of VC1 and VC2 so
		the allowed topology to be considered as for 2-VC and 3-VC setup is k8s-zone -> zone-1,zone-2

		*/

		stsReplicas = 5
		scParameters[scParamStoragePolicyName] = storagePolicyInVc1Vc2
		topValStartIndex = 0
		topValEndIndex = 2
		scaleUpReplicaCount = 10
		scaleDownReplicaCount = 1
		sts_count := 2

		ginkgo.By("Set specific allowed topology")
		allowedTopologies = setSpecificAllowedTopology(allowedTopologies, topkeyStartIndex, topValStartIndex,
			topValEndIndex)

		ginkgo.By("Create StorageClass with storage policy specified")
		scSpec := getVSphereStorageClassSpec(defaultNginxStorageClassName, scParameters, nil, "",
			"", false)
		sc, err := client.StorageV1().StorageClasses().Create(ctx, scSpec, metav1.CreateOptions{})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		defer func() {
			err = client.StorageV1().StorageClasses().Delete(ctx, sc.Name, *metav1.NewDeleteOptions(0))
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}()

		ginkgo.By("Create StatefulSet and verify pv affinity and pod affinity details")
		service, statefulset, err := createStafeulSetAndVerifyPVAndPodNodeAffinty(ctx, client, namespace, false,
			stsReplicas, false, allowedTopologies, 0, false,
			false, false, "", "", nil, false)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		defer func() {
			deleteAllStsAndPodsPVCsInNamespace(ctx, client, namespace)
			deleteService(namespace, client, service)
		}()

		ginkgo.By("Verify PV node affinity and that the PODS are running on appropriate node")
		err = verifyPVnodeAffinityAndPODnodedetailsForStatefulsetsLevel5(ctx, client, statefulset,
			namespace, allowedTopologies, false)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Bring down SPS service")
		vCenterHostname := strings.Split(multiVCe2eVSphere.multivcConfig.Global.VCenterHostname, ",")
		vcAddress := vCenterHostname[0] + ":" + sshdPort
		framework.Logf("vcAddress - %s ", vcAddress)
		err = invokeVCenterServiceControl(stopOperation, spsServiceName, vcAddress)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		isSPSServiceStopped = true
		err = waitVCenterServiceToBeInState(spsServiceName, vcAddress, svcStoppedMessage)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		defer func() {
			if isSPSServiceStopped {
				framework.Logf("Bringing sps up before terminating the test")
				startVCServiceWait4VPs(ctx, vcAddress, spsServiceName, &isSPSServiceStopped)
				isSPSServiceStopped = false
			}
		}()

		ginkgo.By("Create 2 StatefulSet with replica count 5 when sps-service is down")
		statefulSets := createParallelStatefulSetSpec(namespace, sts_count, stsReplicas)
		var wg sync.WaitGroup
		wg.Add(sts_count)
		for i := 0; i < len(statefulSets); i++ {
			go createParallelStatefulSets(client, namespace, statefulSets[i],
				stsReplicas, &wg)
		}
		wg.Wait()

		ginkgo.By("Bringup SPS service")
		startVCServiceWait4VPs(ctx, vcAddress, spsServiceName, &isSPSServiceStopped)

		framework.Logf("Waiting for %v seconds for testbed to be in the normal state", pollTimeoutShort)
		time.Sleep(pollTimeoutShort)

		ginkgo.By("Waiting for StatefulSets Pods to be in Ready State")
		err = waitForStsPodsToBeInReadyRunningState(ctx, client, namespace, statefulSets)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Verify PV node affinity and that the PODS are running on appropriate node")
		for i := 0; i < len(statefulSets); i++ {
			err = verifyPVnodeAffinityAndPODnodedetailsForStatefulsetsLevel5(ctx, client, statefulSets[i],
				namespace, allowedTopologies, false)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}

		ginkgo.By("Perform scaleup/scaledown operation on statefulsets and " +
			"verify pv affinity and pod affinity")
		err = performScalingOnStatefulSetAndVerifyPvNodeAffinity(ctx, client, scaleUpReplicaCount,
			scaleDownReplicaCount, statefulSets[0], false, namespace,
			allowedTopologies, stsScaleUp, stsScaleDown, false)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
	})

	/* TESTCASE-12
		Validate Listvolume response
		query-limit = 3
	    list-volume-threshold = 1
	    Enable debug logs

		SC → default values with WFC Binding mode
		Steps//
	    1. Create Storage Class with allowed topology set to defaul values.
	    2. Create multiple PVC with "RWX"access mode.
	    3. Create deployment Pods with replica count 3 using the PVC created in step #2.
	    4. Once the Pods are up and running validate the logs. Verify the ListVolume Response in the logs , now it should populate volume ID's and the node_id's.
	    5. Check the token number on "nextToken".
	    6. If there are 4 volumes in the list(index 0,1,2), and query limit = 3, then next_token = 2, after the first ListVolume response, and empty after the second ListVolume response.
	    7. Scale up/down the deployment and validate the ListVolume Response
	    8. Cleanup the data
	*/
	ginkgo.It("Validate Listvolume response when volume is deleted from CNS "+
		"in a multivc", ginkgo.Label(p1, block, vanilla, multiVc, newTest), func() {

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		curtime := time.Now().Unix()
		randomValue := rand.Int()
		val := strconv.FormatInt(int64(randomValue), 10)
		val = string(val[1:3])
		curtimestring := strconv.FormatInt(curtime, 10)
		scName := "nginx-sc-default-" + curtimestring + val

		var volumesBeforeScaleUp []string
		containerName := "vsphere-csi-controller"

		ginkgo.By("scale down CSI driver POD to 1 , so that it will" +
			"be easy to validate all Listvolume response on one driver POD")
		collectPodLogs(ctx, client, csiSystemNamespace)
		scaledownCSIDriver, err := scaleCSIDriver(ctx, client, namespace, 1)
		gomega.Expect(scaledownCSIDriver).To(gomega.BeTrue(), "csi driver scaledown is not successful")
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		defer func() {
			ginkgo.By("Scale up the csi-driver replica to 3")
			success, err := scaleCSIDriver(ctx, client, namespace, 3)
			gomega.Expect(success).To(gomega.BeTrue(), "csi driver scale up to 3 replica not successful")
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}()

		ginkgo.By("Creating StorageClass for Statefulset")
		scSpec := getVSphereStorageClassSpec(scName, nil, allowedTopologies, "", "", false)
		sc, err := client.StorageV1().StorageClasses().Create(ctx, scSpec, metav1.CreateOptions{})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		defer func() {
			err := client.StorageV1().StorageClasses().Delete(ctx, sc.Name, *metav1.NewDeleteOptions(0))
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}()

		ginkgo.By("Creating service")
		service := CreateService(namespace, client)
		defer func() {
			deleteService(namespace, client, service)
		}()
		ginkgo.By("Creating statefulset with replica 3 and a deployment")
		statefulset, deployment, volumesBeforeScaleUp := createStsDeployment(ctx, client, namespace, sc, true,
			false, 0, "", "")

		ginkgo.By("Verify PV node affinity and that the PODS are running on appropriate node")
		err = verifyPVnodeAffinityAndPODnodedetailsForStatefulsetsLevel5(ctx, client, statefulset,
			namespace, allowedTopologies, false)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		err = verifyPVnodeAffinityAndPODnodedetailsForDeploymentSetsLevel5(ctx, client, deployment, namespace,
			allowedTopologies, true)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		//List volume responses will show up in the interval of every 1 minute.
		time.Sleep(pollTimeoutShort)

		ginkgo.By("Validate ListVolume Response for all the volumes")
		logMessage := "List volume response: entries:"
		_, _, err = getCSIPodWhereListVolumeResponseIsPresent(ctx, client, sshClientConfig,
			containerName, logMessage, volumesBeforeScaleUp)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		scaleUpReplicaCount = 5
		stsScaleDown = false
		ginkgo.By("Perform scaleup operation on statefulset and verify pv affinity and pod affinity")
		err = performScalingOnStatefulSetAndVerifyPvNodeAffinity(ctx, client, scaleUpReplicaCount,
			scaleDownReplicaCount, statefulset, false, namespace,
			allowedTopologies, stsScaleUp, stsScaleDown, false)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		//List volume responses will show up in the interval of every 1 minute.
		time.Sleep(pollTimeoutShort)

		ginkgo.By("Validate pagination")
		logMessage = "token for next set: 3"
		_, _, err = getCSIPodWhereListVolumeResponseIsPresent(ctx, client, sshClientConfig, containerName, logMessage, nil)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		replicas := 0
		ginkgo.By(fmt.Sprintf("Scaling down statefulsets to number of Replica: %v", replicas))
		_, scaledownErr := fss.Scale(client, statefulset, int32(replicas))
		gomega.Expect(scaledownErr).NotTo(gomega.HaveOccurred())
		fss.WaitForStatusReplicas(client, statefulset, int32(replicas))
		ssPodsAfterScaleDown := fss.GetPodList(client, statefulset)
		gomega.Expect(len(ssPodsAfterScaleDown.Items) == int(replicas)).To(gomega.BeTrue(),
			"Number of Pods in the statefulset should match with number of replicas")

		pvcList := getAllPVCFromNamespace(client, namespace)
		for _, pvc := range pvcList.Items {
			framework.ExpectNoError(fpv.DeletePersistentVolumeClaim(client, pvc.Name, namespace),
				"Failed to delete PVC", pvc.Name)
		}
		//List volume responses will show up in the interval of every 1 minute.
		//To see the empty response, It is required to wait for 1 min after deleteting all the PVC's
		time.Sleep(pollTimeoutShort)

		ginkgo.By("Validate ListVolume Response when no volumes are present")
		logMessage = "ListVolumes served 0 results"

		_, _, err = getCSIPodWhereListVolumeResponseIsPresent(ctx, client, sshClientConfig, containerName, logMessage, nil)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
	})

	/* TESTCASE-13
			When vSAN FS is enabled in all 3 VCs
			SC → default values and WFC Binding mode

	    1. Create Storage Class with default values and with WFC Binding mode.
	    2. Create StatefulSet with 5 replicas using Parallel Pod management policy and with RWX access mode.
	    3. Volume provisioning and pod placement can happen on any AZ.
	    4. Perform Scale-up operation. Increase the replica count to 9.
	    5. Verify scaling operation should go fine.
	    6. Volume provisioning and pod placement can happen on any AZ.
	    7. Perform Scale-down operation. Decrease the replica count to 7
	    8. Verify scale down operation went smooth.
	    9. Perform cleanup by deleting StatefulSet, Pods, PVCs and SC.
	*/

	/* TESTCASE-14
			When vSAN FS is enabled only on 1 single VC
			SC → all allowed topology specify i.e. k8s-zone:zone-1 and with Immediate Binding mode

	    1. Create Storage Class with default values and with WFC Binding mode.
	    2. Create few PVCs with RWX access mode.
	    3. Wait for all PVCs to reach Bound state.
		4. Volume provisioning should happen on the AZ as given in the SC.
		5. Create standalone and deployment pods (with replica count 2) and attach it to each PVC.
		6. Wait for Pods to reach running ready state.
		7. Pods can be created on any AZ.
	    8. Perform Scale-up operation on deployment Pods. Increase the replica count to 4.
	    9. Verify scaling operation went fine.
	    10. Perform Scale-down operation. Decrease the replica count to 1
	    11. Verify scale down operation went smooth.
	    12. Perform cleanup by deleting StatefulSet, Pods, PVCs and SC.
	*/

})
