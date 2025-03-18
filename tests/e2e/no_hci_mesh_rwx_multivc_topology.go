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
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	ginkgo "github.com/onsi/ginkgo/v2"
	"github.com/onsi/gomega"
	cnstypes "github.com/vmware/govmomi/cns/types"
	"golang.org/x/crypto/ssh"
	appsv1 "k8s.io/api/apps/v1"
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

var _ = ginkgo.Describe("[rwx-nohci-multivc-positive] RWX-Topology-NoHciMesh-MultiVc-Positive", func() {
	f := framework.NewDefaultFramework("rwx-nohci-multivc-positive")
	f.NamespacePodSecurityEnforceLevel = admissionapi.LevelPrivileged
	var (
		client                     clientset.Interface
		namespace                  string
		allowedTopologies          []v1.TopologySelectorLabelRequirement
		topValStartIndex           int
		topValEndIndex             int
		topkeyStartIndex           int
		scParameters               map[string]string
		bindingModeWffc            storagev1.VolumeBindingMode
		bindingModeImm             storagev1.VolumeBindingMode
		accessmode                 v1.PersistentVolumeAccessMode
		labelsMap                  map[string]string
		sshClientConfig            *ssh.ClientConfig
		nimbusGeneratedK8sVmPwd    string
		allMasterIps               []string
		masterIp                   string
		isStorageProfileDeleted    bool
		storagePolicyToDelete      string
		isVsanHealthServiceStopped bool
		isSPSServiceStopped        bool
		err                        error
		isVcRebooted               bool
	)

	ginkgo.BeforeEach(func() {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		client = f.ClientSet
		namespace = f.Namespace.Name

		// connecting to multiple VCs
		multiVCbootstrap()

		// fetch list f k8s nodes
		nodeList, err := fnodes.GetReadySchedulableNodes(ctx, f.ClientSet)
		framework.ExpectNoError(err, "Unable to find ready and schedulable Node")
		if !(len(nodeList.Items) > 0) {
			framework.Failf("Unable to find ready and schedulable Node")
		}

		//global variables
		scParameters = make(map[string]string)
		labelsMap = make(map[string]string)
		accessmode = v1.ReadWriteMany
		bindingModeWffc = storagev1.VolumeBindingWaitForFirstConsumer
		bindingModeImm = storagev1.VolumeBindingImmediate

		// read topology map
		topologyMap := GetAndExpectStringEnvVar(envTopologyMap)
		allowedTopologies = createAllowedTopolgies(topologyMap)

		//setting map values
		labelsMap["app"] = "test"
		scParameters[scParamFsType] = nfs4FSType

		// nimbus export variable
		nimbusGeneratedK8sVmPwd = GetAndExpectStringEnvVar(nimbusK8sVmPwd)
		storagePolicyToDelete = GetAndExpectStringEnvVar(envStoragePolicyNameToDeleteLater)

		// ssh connection to master vm
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

		ginkgo.By("Perform cleanup if any resource left in the setup before starting a new test")

		// perfrom cleanup of old stale entries of storage class if left in the setup
		storageClassList, err := client.StorageV1().StorageClasses().List(ctx, metav1.ListOptions{})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		if len(storageClassList.Items) != 0 {
			for _, sc := range storageClassList.Items {
				gomega.Expect(client.StorageV1().StorageClasses().Delete(ctx, sc.Name,
					*metav1.NewDeleteOptions(0))).NotTo(gomega.HaveOccurred())
			}
		}

		// perfrom cleanup of old stale entries of statefulset if left in the setup
		statefulsets, err := client.AppsV1().StatefulSets(namespace).List(ctx, metav1.ListOptions{})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		if len(statefulsets.Items) != 0 {
			for _, sts := range statefulsets.Items {
				gomega.Expect(client.AppsV1().StatefulSets(namespace).Delete(ctx, sts.Name,
					*metav1.NewDeleteOptions(0))).NotTo(gomega.HaveOccurred())
			}
		}

		// perfrom cleanup of old stale entries of nginx service if left in the setup
		err = client.CoreV1().Services(namespace).Delete(ctx, servicename, *metav1.NewDeleteOptions(0))
		if !apierrors.IsNotFound(err) {
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}

		// perfrom cleanup of old stale entries of pvc if left in the setup
		pvcs, err := client.CoreV1().PersistentVolumeClaims(namespace).List(ctx, metav1.ListOptions{})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		if len(pvcs.Items) != 0 {
			for _, pvc := range pvcs.Items {
				gomega.Expect(client.CoreV1().PersistentVolumeClaims(namespace).Delete(ctx, pvc.Name,
					*metav1.NewDeleteOptions(0))).NotTo(gomega.HaveOccurred())
			}
		}

		// perfrom cleanup of old stale entries of any deployment entry if left in the setup
		depList, err := client.AppsV1().Deployments(namespace).List(ctx, metav1.ListOptions{})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		if len(depList.Items) != 0 {
			for _, dep := range depList.Items {
				err := client.AppsV1().Deployments(namespace).Delete(ctx, dep.Name, metav1.DeleteOptions{})
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
			}
		}

		// perfrom cleanup of old stale entries of pv if left in the setup
		pvs, err := client.CoreV1().PersistentVolumes().List(ctx, metav1.ListOptions{})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		if len(pvs.Items) != 0 {
			for _, pv := range pvs.Items {
				gomega.Expect(client.CoreV1().PersistentVolumes().Delete(ctx, pv.Name,
					*metav1.NewDeleteOptions(0))).NotTo(gomega.HaveOccurred())
			}
		}

		if isVsanHealthServiceStopped {
			vcAddress, _, err := readMultiVcAddress(0)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			framework.Logf("Bringing vsanhealth up before terminating the test")
			startVCServiceWait4VPs(ctx, vcAddress, vsanhealthServiceName, &isVsanHealthServiceStopped)
		}

		if isSPSServiceStopped {
			vcAddress, _, err := readMultiVcAddress(0)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			framework.Logf("Bringing sps up before terminating the test")
			startVCServiceWait4VPs(ctx, vcAddress, spsServiceName, &isSPSServiceStopped)
			isSPSServiceStopped = false
		}

		if isStorageProfileDeleted {
			clientIndex := 0
			err = createStorageProfile(masterIp, sshClientConfig, storagePolicyToDelete, clientIndex)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}

		// restarting pending and stopped services after vc reboot if any
		if isVcRebooted {
			vcAddress, _, err := readMultiVcAddress(0)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			err = checkVcServicesHealthPostReboot(ctx, vcAddress, pollTimeout)
			gomega.Expect(err).NotTo(gomega.HaveOccurred(),
				"Setup is not in healthy state, Got timed-out waiting for required VC services to be up and running")
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
	   7. Create SC2 with Immediate Binding mode and with all levels of allowed topology set considering all 3 VC's.
	   8. Create PVC with "RWX" access mode using SC created in step #8.
	   9. Verify PVC should reach to Bound state.
	   10. Create 3 Pods with different access mode and attach it to PVC created in step #9.
	   11. Verify Pods should reach to Running state.
	   12. Try reading/writing onto the volume from different Pods.
	   13. Perform scale up of deployment Pods to replica count 6. Verify Scaling operation should go smooth.
	   14. Perform scale-down operation on deployment pods. Decrease the replica count to 1.
	   15. Verify scale down operation went smooth.
	   16. Perform cleanup by deleting deployment Pods, PVCs and the StorageClass (SC)
	*/

	ginkgo.It("Deployment pods and standalone pods creation with rwx pvcs "+
		"with different allowed topology given in SCs", ginkgo.Label(p0, file, vanilla, multiVc, newTest), func() {

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		// variable declaration
		var pvs1, pvs2 []*v1.PersistentVolume
		var pvclaim1, pvclaim2 *v1.PersistentVolumeClaim
		var pv1, pv2 *v1.PersistentVolume

		// deployment pod and pvc count
		replica := 3
		createPvcItr := 1
		createDepItr := 1

		ginkgo.By(fmt.Sprintf("Creating Storage Class with access mode %q and fstype %q with "+
			"no allowed topology specified", accessmode, nfs4FSType))
		storageclass1, pvclaims1, err := createStorageClassWithMultiplePVCs(client, namespace, labelsMap,
			scParameters, diskSize, nil, bindingModeWffc, false, accessmode, "", nil, createPvcItr, false, false)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		defer func() {
			err := client.StorageV1().StorageClasses().Delete(ctx, storageclass1.Name, *metav1.NewDeleteOptions(0))
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}()
		defer func() {
			err := fpv.DeletePersistentVolumeClaim(ctx, client, pvclaim1.Name, namespace)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			err = multiVCe2eVSphere.waitForCNSVolumeToBeDeletedInMultiVC(pv1.Spec.CSI.VolumeHandle)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}()

		ginkgo.By("Set node selector term for deployment pods so that all pods " +
			"should get created on one single AZ i.e. VC-1(zone-1)")
		/* here in 3-VC setup, deployment pod node affinity is taken as  k8s-zone -> zone-1
		i.e only VC1 allowed topology
		*/
		topValStartIndex = 0
		topValEndIndex = 1
		allowedTopologies = setSpecificAllowedTopology(allowedTopologies, topkeyStartIndex, topValStartIndex,
			topValEndIndex)
		nodeSelectorTerms, err := getNodeSelectorMapForDeploymentPods(allowedTopologies)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		// taking pvclaims1 0th index because we are creating only single RWX PVC in this case
		pvclaim1 = pvclaims1[0]

		ginkgo.By("Create Deployment with replica count 3")
		deploymentList, pods, err := createVerifyAndScaleDeploymentPods(ctx, client, namespace, int32(replica),
			false, labelsMap, pvclaim1, nodeSelectorTerms, execRWXCommandPod, nginxImage, false, nil, createDepItr)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		defer func() {
			framework.Logf("Delete deployment set")
			err := client.AppsV1().Deployments(namespace).Delete(ctx, deploymentList[0].Name, *metav1.NewDeleteOptions(0))
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}()

		ginkgo.By("Verify deployment pods are scheduled on a specified node selector terms")
		err = verifyDeploymentPodNodeAffinity(ctx, client, namespace, allowedTopologies, pods)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Verify PVC Bound state and CNS side verification")
		pvs1, err = checkVolumeStateAndPerformCnsVerification(ctx, client, pvclaims1, "", "")
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		// taking pv1 0th index because we are creating only single RWX PVC in this case
		pv1 = pvs1[0]

		// standalone pod and pvc count
		noPodsToDeploy := 3
		createPvcItr = 1

		// here considering all 3 VCs so start index is set to 0 and endIndex will be set to 3rd VC value
		topValStartIndex = 0
		topValEndIndex = 3

		ginkgo.By("Set allowed topology to all 3 VCs")
		allowedTopologyForSC := setSpecificAllowedTopology(allowedTopologies, topkeyStartIndex, topValStartIndex,
			topValEndIndex)

		ginkgo.By(fmt.Sprintf("Creating Storage Class with access mode %q and fstype %q with allowed "+
			"topology set to all topology levels", accessmode, nfs4FSType))
		storageclass2, pvclaims2, err := createStorageClassWithMultiplePVCs(client, namespace, labelsMap,
			scParameters, diskSize, allowedTopologyForSC, bindingModeImm, false, accessmode, "", nil, createPvcItr, false, false)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		defer func() {
			err := client.StorageV1().StorageClasses().Delete(ctx, storageclass2.Name, *metav1.NewDeleteOptions(0))
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}()

		defer func() {
			err := fpv.DeletePersistentVolumeClaim(ctx, client, pvclaim2.Name, namespace)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			err = multiVCe2eVSphere.waitForCNSVolumeToBeDeletedInMultiVC(pv2.Spec.CSI.VolumeHandle)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}()

		ginkgo.By("Verify PVC Bound state and CNS side verification")
		pvs2, err = checkVolumeStateAndPerformCnsVerification(ctx, client, pvclaims2, "", "")
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		// taking pvclaims2 0th index because we are creating only single RWX PVC in this case
		pvclaim2 = pvclaims2[0]
		pv2 = pvs2[0]

		ginkgo.By("Create 3 standalone Pods using the same PVC with different read/write permissions")
		podList, err := createStandalonePodsForRWXVolume(client, ctx, namespace, nil, pvclaim2, false,
			execRWXCommandPod, noPodsToDeploy)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		defer func() {
			for i := 0; i < len(podList); i++ {
				ginkgo.By(fmt.Sprintf("Deleting the pod %s in namespace %s", podList[i].Name, namespace))
				err := fpod.DeletePodWithWait(ctx, client, podList[i])
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
			}
		}()
		ginkgo.By("Scale up deployment to 6 replica")
		replica = 6
		/* here createDepItr value is set to 0, because we are performing scaleup operation
		and not creating any new deployment */
		_, pods, err = createVerifyAndScaleDeploymentPods(ctx, client, namespace, int32(replica), true,
			labelsMap, pvclaim1, nodeSelectorTerms, execRWXCommandPod, nginxImage, true, deploymentList[0], 0)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Verify deployment pods are scheduled on a specified node selector terms")
		err = verifyDeploymentPodNodeAffinity(ctx, client, namespace, allowedTopologies, pods)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Scale down deployment to 1 replica")
		replica = 1
		_, _, err = createVerifyAndScaleDeploymentPods(ctx, client, namespace, int32(replica), true, labelsMap,
			pvclaim1, nodeSelectorTerms, execRWXCommandPod, nginxImage, true, deploymentList[0], 0)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
	})

	/* TESTCASE-2
	   StatefulSet Workload with default Pod Management and Scale-Up/Scale-Down Operations
	   SC → Storage Policy (VC-1) with specific allowed topology i.e. k8s-zone:zone-1 and with WFC Binding mode

	   Steps:
	   1. Create SC with storage policy name (tagged with vSAN ds) available in single VC (VC-1)
	   2. Create StatefulSet with 3 replicas using default Pod management policy
	   3. Allow time for PVCs and Pods to reach Bound and Running states, respectively.
	   4. Volume provisioning should happen on the AZ as specified in the SC.
	   5. Pod placement can happen in any available availability zones (AZs)
	   6. Verify CNS metadata for PVC and Pod.
	   7. Scale-up/Scale-down the statefulset. Verify scaling operation went successful.
	   8. Volume provisioning should happen on the AZ as specified in the SC but Pod placement can
	   occur in any availability zones
	   (AZs) during the scale-up operation.
	   9. Perform cleanup by deleting StatefulSets, PVCs, and the StorageClass (SC)
	*/

	ginkgo.It("Statefulset creation with rwx pvcs having storage "+
		"policy defined only in AZ1", ginkgo.Label(p0, file, vanilla, multiVc, newTest), func() {

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		//storage policy read of cluster-1(rack-1)
		storagePolicyName := GetAndExpectStringEnvVar(envStoragePolicyNameVC1)
		scParameters["storagepolicyname"] = storagePolicyName

		// sts pod replica count
		stsReplicas := 3
		scaleUpReplicaCount := 5
		scaleDownReplicaCount := 1

		// here considering VC-1 so start index is set to 0 and endIndex will be set to 1
		topValStartIndex = 0
		topValEndIndex = 1

		ginkgo.By("Set allowed topology to VC-1")
		allowedTopologyForSC := setSpecificAllowedTopology(allowedTopologies, topkeyStartIndex, topValStartIndex,
			topValEndIndex)

		ginkgo.By(fmt.Sprintf("Creating Storage Class with access mode %q and fstype %q with"+
			"allowed topology specified to VC-1 (zone-1) and with WFFC binding mode", accessmode, nfs4FSType))
		scSpec := getVSphereStorageClassSpec(defaultNginxStorageClassName, scParameters, allowedTopologyForSC, "",
			bindingModeWffc, false)
		sc, err := client.StorageV1().StorageClasses().Create(ctx, scSpec, metav1.CreateOptions{})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		defer func() {
			err = client.StorageV1().StorageClasses().Delete(ctx, sc.Name, *metav1.NewDeleteOptions(0))
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}()

		ginkgo.By("Create StatefulSet with replica count 3 with RWX PVC access mode")
		service, statefulset, err := createStafeulSetAndVerifyPVAndPodNodeAffinty(ctx, client, namespace, false,
			int32(stsReplicas), false, allowedTopologyForSC, false, false, false, accessmode, sc, false, "")
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		defer func() {
			fss.DeleteAllStatefulSets(ctx, client, namespace)
			deleteService(namespace, client, service)
		}()

		/*Storage policy of vc-1 is passed in storage class creation, so fetching the list of datastores
		which is available in VC-1, volume provision should happen on VC-1 vSAN FS compatible datastore */
		datastoreUrlsRack1, _, _, _, err := fetchDatastoreListMap(ctx, client)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		ginkgo.By("Verify volume placement, should match with the allowed topology specified")
		err = volumePlacementVerificationForSts(ctx, client, statefulset, namespace, datastoreUrlsRack1)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Perform scaleup/scaledown operation on statefulsets")
		err = performScalingOnStatefulSetAndVerifyPvNodeAffinity(ctx, client, int32(scaleUpReplicaCount),
			int32(scaleDownReplicaCount), statefulset, false, namespace, allowedTopologyForSC, true, true, false)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Verify new volume placement, should match with the allowed topology specified")
		err = volumePlacementVerificationForSts(ctx, client, statefulset, namespace, datastoreUrlsRack1)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
	})

	/* TESTCASE-3
	   PVC and multiple Pods creation → Same Policy is available in two VCs
	   SC → Same Storage Policy Name (VC-1 and VC-2) with specific allowed topology i.e.
	   k8s-zone:zone-1,zone-2 and with Immediate Binding mode

	   Steps:
	   1. Create SC with Storage policy name available in VC1 and VC2 and allowed topology set to
	   k8s-zone:zone-1,zone-2 and with Immediate Binding mode
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

	ginkgo.It("Multiple pods creation with single rwx pvcs created "+
		"with storage policy available ", ginkgo.Label(p0, file, vanilla, multiVc, newTest), func() {

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		// deployment pod and pvc count
		replica := 2
		createPvcItr := 1
		createDepItr := 1

		storagePolicyInVc1Vc2 := GetAndExpectStringEnvVar(envStoragePolicyNameInVC1VC2)
		scParameters[scParamStoragePolicyName] = storagePolicyInVc1Vc2

		/*
			We are considering storage policy of VC1 and VC2 and the allowed
			topology is k8s-zone -> zone-1, zone-2 i.e VC1 and VC2 allowed topologies.
		*/
		topValStartIndex = 0
		topValEndIndex = 2

		ginkgo.By("Set allowed topology to VC-1 and VC-2")
		allowedTopologyForSC := setSpecificAllowedTopology(allowedTopologies, topkeyStartIndex, topValStartIndex,
			topValEndIndex)

		ginkgo.By(fmt.Sprintf("Creating Storage Class with access mode %q and fstype %q with "+
			"no allowed topology specified", accessmode, nfs4FSType))
		storageclass, pvclaims, err := createStorageClassWithMultiplePVCs(client, namespace, labelsMap,
			scParameters, diskSize, allowedTopologyForSC, bindingModeImm, false, accessmode, "", nil, createPvcItr, false, false)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		defer func() {
			err := client.StorageV1().StorageClasses().Delete(ctx, storageclass.Name, *metav1.NewDeleteOptions(0))
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}()

		ginkgo.By("Verify PVC Bound state and CNS side verification")
		pvs, err := checkVolumeStateAndPerformCnsVerification(ctx, client, pvclaims, storagePolicyInVc1Vc2, "")
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		// taking pvclaims and pv 0th index because we are creating only single RWX PVC in this case
		pvclaim := pvclaims[0]
		pv := pvs[0]
		defer func() {
			err := fpv.DeletePersistentVolumeClaim(ctx, client, pvclaim.Name, namespace)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			err = multiVCe2eVSphere.waitForCNSVolumeToBeDeletedInMultiVC(pv.Spec.CSI.VolumeHandle)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}()

		// volume placement should happen either on VC-1 or VC-2
		ginkgo.By("Verify volume placement, should match with the allowed topology specified")
		datastoreUrlsRack1, datastoreUrlsRack2, _, _, err := fetchDatastoreListMap(ctx, client)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		var datastoreUrlsVc1Vc2 []string
		datastoreUrlsVc1Vc2 = append(datastoreUrlsVc1Vc2, datastoreUrlsRack1...)
		datastoreUrlsVc1Vc2 = append(datastoreUrlsVc1Vc2, datastoreUrlsRack2...)
		isCorrectPlacement := multiVCe2eVSphere.verifyPreferredDatastoreMatchInMultiVC(pv.Spec.CSI.VolumeHandle,
			datastoreUrlsVc1Vc2)
		gomega.Expect(isCorrectPlacement).To(gomega.BeTrue(), fmt.Sprintf("Volume provisioning has happened on the wrong "+
			"datastore. Expected 'true', got '%v'", isCorrectPlacement))

		ginkgo.By("Create Deployment with replica count 2")
		deploymentList, _, err := createVerifyAndScaleDeploymentPods(ctx, client, namespace, int32(replica), false, labelsMap,
			pvclaim, nil, execRWXCommandPod, nginxImage, false, nil, createDepItr)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		defer func() {
			framework.Logf("Delete deployment set")
			err := client.AppsV1().Deployments(namespace).Delete(ctx, deploymentList[0].Name, *metav1.NewDeleteOptions(0))
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}()

		ginkgo.By("Scale up deployment to 5 replica")
		replica = 5
		_, _, err = createVerifyAndScaleDeploymentPods(ctx, client, namespace, int32(replica), true, labelsMap,
			pvclaim, nil, execRWXCommandPod, nginxImage, true, deploymentList[0], 0)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Scale down deployment to 1 replica")
		replica = 1
		_, pods, err := createVerifyAndScaleDeploymentPods(ctx, client, namespace, int32(replica), true,
			labelsMap, pvclaim, nil, execRWXCommandPod, nginxImage, true, deploymentList[0], 0)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Verify volume metadata for deployment pod, pvc and pv")
		err = waitAndVerifyCnsVolumeMetadata(ctx, pv.Spec.CSI.VolumeHandle, pvclaim, pv, &pods.Items[0])
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
	})

	/* TESTCASE-4
	   Standalone Pods creation with allowed topology details in SC specific to VC-2
	   SC → specific allowed topology i.e. k8s-zone:zone-2 and datastore url of VC-2 with WFC Binding mode

	   Steps:
	   1. Create SC with allowed topology of VC-2 and use datastore url from same VC with WFC Binding mode.
	   2. Create PVC with "RWX" access mode using SC created in step #1.
	   3. Verify PVC should reach to Bound state and it should be created on the AZ as given in the SC.
	   4. PVC should get created on the given AZ of SC.
	   5. Create 3 standalone pods using PVC created in step #2.
	   6. Wait for Pods to reach running ready state.
	   7. Pods should get created on any VC or any AZ.
	   8. Try reading/writing data into the volume.
	   9. Perform cleanup by deleting deployment Pods, PVC and SC
	*/

	ginkgo.It("Multiple standalone pods attached to single rwx pvc with datastore url "+
		"tagged to Az2 in SC", ginkgo.Label(p0, file, vanilla, multiVc, newTest), func() {

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		var pv *v1.PersistentVolume

		// pods and pvc count
		noPodsToDeploy := 3
		createPvcItr := 1

		// vSAN FS compatible datastore of VC-2
		dsUrl := GetAndExpectStringEnvVar(envSharedDatastoreURLVC2)
		scParameters["datastoreurl"] = dsUrl

		/*
			We are considering allowed topology of VC-2 i.e.
			k8s-zone -> zone-2 and datastore url of VC-2
		*/
		topValStartIndex = 1
		topValEndIndex = 2

		ginkgo.By("Set allowed topology to VC-2")
		allowedTopologyForSC := setSpecificAllowedTopology(allowedTopologies, topkeyStartIndex, topValStartIndex,
			topValEndIndex)

		ginkgo.By(fmt.Sprintf("Creating Storage Class with access mode %q and fstype %q with "+
			"allowed topology set to VC-2 AZ ", accessmode, nfs4FSType))
		storageclass, pvclaims, err := createStorageClassWithMultiplePVCs(client, namespace, labelsMap,
			scParameters, diskSize, allowedTopologyForSC, bindingModeWffc, false, accessmode, "", nil,
			createPvcItr, false, false)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		defer func() {
			err := client.StorageV1().StorageClasses().Delete(ctx, storageclass.Name, *metav1.NewDeleteOptions(0))
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}()

		// taking pvclaims 0th index because we are creating only single RWX PVC in this case
		pvclaim := pvclaims[0]
		defer func() {
			err := fpv.DeletePersistentVolumeClaim(ctx, client, pvclaim.Name, namespace)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			err = multiVCe2eVSphere.waitForCNSVolumeToBeDeletedInMultiVC(pv.Spec.CSI.VolumeHandle)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}()

		ginkgo.By("Create 3 standalone Pods using the same PVC with different read/write permissions")
		podList, err := createStandalonePodsForRWXVolume(client, ctx, namespace, nil, pvclaim, false, execRWXCommandPod,
			noPodsToDeploy)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		defer func() {
			for i := 0; i < len(podList); i++ {
				ginkgo.By(fmt.Sprintf("Deleting the pod %s in namespace %s", podList[i].Name, namespace))
				err := fpod.DeletePodWithWait(ctx, client, podList[i])
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
			}
		}()

		ginkgo.By("Verify PVC Bound state and CNS side verification")
		pvs, err := checkVolumeStateAndPerformCnsVerification(ctx, client, pvclaims, "", dsUrl)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		// taking pvs 0th index because we are creating only single RWX PVC in this case
		pv = pvs[0]

		// volume placement should happen either on VC-2
		ginkgo.By("Verify volume placement, should match with the allowed topology specified")
		_, datastoreUrlsRack2, _, _, err := fetchDatastoreListMap(ctx, client)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		isCorrectPlacement := multiVCe2eVSphere.verifyPreferredDatastoreMatchInMultiVC(pv.Spec.CSI.VolumeHandle,
			datastoreUrlsRack2)
		gomega.Expect(isCorrectPlacement).To(gomega.BeTrue(), fmt.Sprintf("Volume provisioning has happened on the wrong "+
			"datastore. Expected 'true', got '%v'", isCorrectPlacement))
	})

	/* TESTCASE-5
	   Deploy Statefulset workload with allowed topology details in SC specific to VC-1 and VC-3
	   SC → specific allowed topology like K8s-zone: zone-1 and zone-3 and with Immediate Binding mode

	   Steps//
	   1. Create SC with allowedTopology details set to VC-1 and VC-3 availability Zone i.e. zone-1 and zone-3
	   2. Create Statefulset with replica  count 3
	   3. Wait for PVC to reach bound state and POD to reach Running state
	   4. Volume provision should happen on the AZ as specified in the SC.
	   5. Pods should get created on any AZ i.e. across all 3 VCs
	   6. Scale-up the statefuset replica count to 7.
	   7. Verify scaling operation went smooth and new volumes should get created on the VC-1 and VC-2 AZ.
	   8. Newly created Pods should get created across any of the AZs.
	   9. Perform Scale down operation. Reduce the replica count from 7 to 2.
	   10. Verify scale down operation went smooth.
	   11. Perform cleanup by deleting StatefulSets, PVCs, and the StorageClass (SC)
	*/

	ginkgo.It("Scaling operations involving statefulSet pods with multiple replicas, "+
		"each connected to rwx pvcs with allowed topology specified is "+
		"of AZ-1 and AZ-3", ginkgo.Label(p0, file, vanilla, multiVc, newTest), func() {

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		// pod replica count
		stsReplicas := 3
		scaleUpReplicaCount := 7
		scaleDownReplicaCount := 2

		/*
			We are considering allowed topology of VC-1 and VC-3 i.e.
			k8s-zone -> zone-1 and zone-3
		*/
		topValStartIndex = 0
		topValEndIndex = 3

		ginkgo.By("Setting allowed topology to vc-1 and vc-3")
		allowedTopologyForSC := setSpecificAllowedTopology(allowedTopologies, topkeyStartIndex, topValStartIndex,
			topValEndIndex)
		// fetching vc-1 and vc-3 AZ
		value1 := allowedTopologyForSC[0].Values[0]
		value3 := allowedTopologyForSC[0].Values[2]
		// Set the values back into allowedTopologyForSC
		allowedTopologyForSC[0].Values = []string{value1, value3}

		ginkgo.By(fmt.Sprintf("Creating Storage Class with access mode %q and fstype %q with no "+
			"allowed topology specified and with WFFC binding mode", accessmode, nfs4FSType))
		scSpec := getVSphereStorageClassSpec(defaultNginxStorageClassName, scParameters, allowedTopologyForSC,
			"", bindingModeImm, false)
		sc, err := client.StorageV1().StorageClasses().Create(ctx, scSpec, metav1.CreateOptions{})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		defer func() {
			err = client.StorageV1().StorageClasses().Delete(ctx, sc.Name, *metav1.NewDeleteOptions(0))
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}()

		ginkgo.By("Create StatefulSet with replica count 3 with RWX PVC access mode")
		service, statefulset, err := createStafeulSetAndVerifyPVAndPodNodeAffinty(ctx, client, namespace, true,
			int32(stsReplicas), false, allowedTopologyForSC, false, false, false, accessmode, sc, false, "")
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		defer func() {
			fss.DeleteAllStatefulSets(ctx, client, namespace)
			deleteService(namespace, client, service)
		}()

		ginkgo.By("Verify volume placement, should match with the allowed topology specified")
		datastoreUrlsRack1, _, datastoreUrlsRack3, _, err := fetchDatastoreListMap(ctx, client)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		var datastoreUrls []string
		datastoreUrls = append(datastoreUrls, datastoreUrlsRack1...)
		datastoreUrls = append(datastoreUrls, datastoreUrlsRack3...)
		err = volumePlacementVerificationForSts(ctx, client, statefulset, namespace, datastoreUrls)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Perform scaleup/scaledown operation on statefulsets")
		err = performScalingOnStatefulSetAndVerifyPvNodeAffinity(ctx, client, int32(scaleUpReplicaCount),
			int32(scaleDownReplicaCount), statefulset, false, namespace, allowedTopologyForSC, true, true, false)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
	})

	/* TESTCASE-6
	   Deploy workload with allowed topology details in SC specific to vc1 and vc2 but
	   storage policy is passed from vc1 (vSAN DS) and datastore url vc2 both passed

	   Steps:
	   1. Create SC with allowedTopology details set to AZs i.e. zone1, zone2
	   Also pass storage policy tagged to vSAN DS from vc1 and datastore url tagged to vSAN DS from vc2 with
	   Immediate Binding mode.
	   2. Create PVC using the SC created above.
	   3. PVC creation should fail with an appropriate error message i.e. no compatible datastore found
	   4. Perform cleanup by deleting SC and PVC
	*/

	ginkgo.It("Deploy workload with allowed topology of vc1 and "+
		"vc2, tag storage policy of vc1 and datastore url of vc2", ginkgo.Label(p2, file, vanilla, multiVc,
		newTest, negative), func() {

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		/* set allowed topology of vc1 and vc2*/
		topValStartIndex = 0
		topValEndIndex = 2
		datastoreUrl := GetAndExpectStringEnvVar(envSharedDatastoreURLVC2)
		storagePolicyName := GetAndExpectStringEnvVar(envStoragePolicyNameVC1)
		scParameters[scParamDatastoreURL] = datastoreUrl
		scParameters[scParamStoragePolicyName] = storagePolicyName
		expectedErrMsg := "not found in candidate list for volume provisioning"

		ginkgo.By("Set specific allowed topology")
		allowedTopologies = setSpecificAllowedTopology(allowedTopologies, topkeyStartIndex, topValStartIndex,
			topValEndIndex)

		storageclass, pvclaim, err := createPVCAndStorageClass(ctx, client,
			namespace, nil, scParameters, diskSize, allowedTopologies, bindingModeImm, false, accessmode)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		defer func() {
			err = client.StorageV1().StorageClasses().Delete(ctx, storageclass.Name, *metav1.NewDeleteOptions(0))
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			err = fpv.DeletePersistentVolumeClaim(ctx, client, pvclaim.Name, namespace)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}()

		ginkgo.By("Expect claim to fail provisioning volume since no valid datastore is specified in the storage class")
		err = fpv.WaitForPersistentVolumeClaimPhase(ctx, v1.ClaimBound, client,
			pvclaim.Namespace, pvclaim.Name, framework.Poll, time.Minute/2)
		gomega.Expect(err).To(gomega.HaveOccurred())
		ginkgo.By(fmt.Sprintf("Expected failure message: %+q", expectedErrMsg))
		isFailureFound := checkEventsforError(client, namespace,
			metav1.ListOptions{FieldSelector: fmt.Sprintf("involvedObject.name=%s", pvclaim.Name)}, expectedErrMsg)
		gomega.Expect(isFailureFound).To(gomega.BeTrue(), "Unable to verify pvc create failure")
	})

	/* TESTCASE-7
	   Create storage policy in vc1 and vc2 and create storage class with the same policy
	   and  delete Storage policy in vc1 .Expected pvc to go to vc2

	   Steps:
	   1. Create Storage policy with same name which is present in both vc1 and vc2
	   2. Create Storage Class with allowed topology specify to vc1 and vc2 and specify storage policy created in step #1.
	   3. Delete storage policy from VC1
	   4. Create few PVCs (2 to 3 PVCs) with "RWX" access mode using SC created in step #1
	   5. Create Deployment Pods with replica count 2 and Standalone Pods using PVC created in step #3.
	   6. Verify PVC should reach Bound state and Pods should reach Running state.
	   7. Volume provisioning can happen on the AZ as specified in the SC i.e. it
	   should provision volume on VC2 AZ as VC-1 storage policy is deleted
	   8. Pod placement can happen on any AZ.
	   9. Perform Scaleup operation on deployment Pods. Increase the replica count from 2 to 4.
	   10. Perform cleanup by deleting Pods, PVC and SC.
	*/

	ginkgo.It("Same storage policy is available in vc1 and vc2 and later delete storage policy from "+
		"one of the VC", ginkgo.Label(p1, file, vanilla, multiVc, newTest), func() {

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		var depl []*appsv1.Deployment
		var podList []*v1.Pod

		/* set allowed topology of vc1 and vc2*/
		topValStartIndex = 0
		topValEndIndex = 2
		clientIndex := 0

		// pods and pvc count
		noPodsToDeploy := 3
		createPvcItr := 3
		createDepItr := 1
		replica := 2

		// storage policy which is common in both vc1 and vc2
		scParameters[scParamStoragePolicyName] = storagePolicyToDelete

		ginkgo.By("Set allowed topology specific to vc1 and vc2")
		allowedTopologies = setSpecificAllowedTopology(allowedTopologies, topkeyStartIndex,
			topValStartIndex, topValEndIndex)

		ginkgo.By(fmt.Sprintf("Creating Storage Class with access mode %q and fstype %q with "+
			"no allowed topology specified", accessmode, nfs4FSType))
		storageclass, _, err := createStorageClassWithMultiplePVCs(client, namespace, labelsMap,
			scParameters, diskSize, allowedTopologies, bindingModeImm, false, accessmode, "", nil, createPvcItr, false, true)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		defer func() {
			err := client.StorageV1().StorageClasses().Delete(ctx, storageclass.Name, *metav1.NewDeleteOptions(0))
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}()

		ginkgo.By("Delete Storage Policy created in VC1")
		err = deleteStorageProfile(masterIp, sshClientConfig, storagePolicyToDelete, clientIndex)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		isStorageProfileDeleted = true
		defer func() {
			err = createStorageProfile(masterIp, sshClientConfig, storagePolicyToDelete, clientIndex)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			isStorageProfileDeleted = false
		}()

		ginkgo.By("Creating multiple pvcs with rwx access mode")
		_, pvclaims, err = createStorageClassWithMultiplePVCs(client, namespace, labelsMap,
			scParameters, diskSize, allowedTopologies, bindingModeImm, false, accessmode,
			"", storageclass, createPvcItr, true, false)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Verify PVC Bound state and CNS side verification")
		_, err = checkVolumeStateAndPerformCnsVerification(ctx, client, pvclaims, "", "")
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		defer func() {
			for i := 0; i < len(pvclaims); i++ {
				pv := getPvFromClaim(client, pvclaims[i].Namespace, pvclaims[i].Name)
				err := fpv.DeletePersistentVolumeClaim(ctx, client, pvclaims[i].Name, namespace)
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
				err = multiVCe2eVSphere.waitForCNSVolumeToBeDeletedInMultiVC(pv.Spec.CSI.VolumeHandle)
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
			}
		}()

		ginkgo.By("Create Deployments and standalone pods")
		for i := 0; i < len(pvclaims); i++ {
			if i != 2 {
				deploymentList, _, err := createVerifyAndScaleDeploymentPods(ctx, client, namespace, int32(replica),
					false, labelsMap, pvclaims[i], nil, execRWXCommandPod, nginxImage, false, nil, createDepItr)
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
				depl = append(depl, deploymentList...)
			} else {
				// Create 3 standalone pods and attach it to 3rd rwx pvc
				ginkgo.By("Create 3 standalone Pods using the same PVC with different read/write permissions")
				podList, err = createStandalonePodsForRWXVolume(client, ctx, namespace, nil, pvclaims[i], false, execRWXCommandPod,
					noPodsToDeploy)
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
			}
		}

		defer func() {
			for i := 0; i < len(podList); i++ {
				ginkgo.By(fmt.Sprintf("Deleting the pod %s in namespace %s", podList[i].Name, namespace))
				err := fpod.DeletePodWithWait(ctx, client, podList[i])
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
			}
		}()

		defer func() {
			framework.Logf("Delete deployment set")
			for i := 0; i < len(depl); i++ {
				err := client.AppsV1().Deployments(namespace).Delete(ctx, depl[i].Name, *metav1.NewDeleteOptions(0))
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
			}
		}()

		// volume placement should happen on VC-2
		ginkgo.By("Verify volume placement, should match with the allowed topology specified")
		_, datastoreUrlsRack2, _, _, err := fetchDatastoreListMap(ctx, client)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		for i := 0; i < len(pvclaims); i++ {
			pv := getPvFromClaim(client, pvclaims[i].Namespace, pvclaims[i].Name)
			isCorrectPlacement := multiVCe2eVSphere.verifyPreferredDatastoreMatchInMultiVC(pv.Spec.CSI.VolumeHandle,
				datastoreUrlsRack2)
			gomega.Expect(isCorrectPlacement).To(gomega.BeTrue(), fmt.Sprintf("Volume provisioning has happened on the wrong "+
				"datastore. Expected 'true', got '%v'", isCorrectPlacement))
		}

		ginkgo.By("Scale up deployment to 4 replica")
		replica = 4
		_, _, err = createVerifyAndScaleDeploymentPods(ctx, client, namespace, int32(replica),
			true, labelsMap, pvclaims[0], nil, execRWXCommandPod, nginxImage, true, depl[0], 0)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

	})

	/* TESTCASE-9
	   Storage policy is present in VC1 and VC1 is under reboot
	   SC → specific allowed topology i.e. k8s-zone:zone-1 with
	   immediate Binding mode and with Storage Policy (vSAN DS of VC-1)

	   Steps//
	   1. Create a StorageClass (SC) tagged to vSAN datastore of VC-1.
	   2. Create a deployment with a replica count of 3 and a size of 2Gi, using the previously created StorageClass.
	   3. Initiate a reboot of VC-1 while deployment pod creation is in progress.
	   4. Confirm that volume creation is in a "Pending" state while VC-1 is rebooting.
	   5. Wait for VC-1 to be fully operational.
	   6. Ensure that, upon VC-1 recovery, all volumes come up on the worker nodes within VC-1.
	   7. Confirm that volume provisioning occurs exclusively on VC-1.
	   8. Pod placement can happen on any AZ.
	   9. Perform scaleup/scaledown operation on deployment pods.
	   10. Perform cleanup by deleting Pods, PVCs and SC
	*/

	ginkgo.It("Creating statefulset with rwx pvcs with storage policy "+
		"tagged to vc1 datastore and vc1 is under reboot", ginkgo.Label(p1, file, vanilla,
		multiVc, disruptive), func() {

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		// storage policy of vc1
		storagePolicyInVc1 := GetAndExpectStringEnvVar(envStoragePolicyNameVC1)
		scParameters[scParamStoragePolicyName] = storagePolicyInVc1

		// count of pvc and deployment pod
		replica := 3
		createPvcItr := 1

		// here, we are considering the allowed topology of VC1 i.e. k8s-zone -> zone-1
		topValStartIndex = 0
		topValEndIndex = 1

		ginkgo.By("Set specific allowed topology of vc1")
		allowedTopologies = setSpecificAllowedTopology(allowedTopologies, topkeyStartIndex, topValStartIndex,
			topValEndIndex)

		ginkgo.By(fmt.Sprintf("Creating Storage Class with access mode %q and fstype %q with "+
			"no allowed topology specified", accessmode, nfs4FSType))
		storageclass, pvclaims, err := createStorageClassWithMultiplePVCs(client, namespace, labelsMap,
			scParameters, diskSize, allowedTopologies, bindingModeImm, false, accessmode, "", nil, createPvcItr, false, false)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		defer func() {
			err := client.StorageV1().StorageClasses().Delete(ctx, storageclass.Name, *metav1.NewDeleteOptions(0))
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}()

		ginkgo.By("Verify PVC Bound state and CNS side verification")
		pvs, err := checkVolumeStateAndPerformCnsVerification(ctx, client, pvclaims, "", "")
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		// taking pvclaims and pvs 0th index because we are creating only single RWX PVC in this case
		pvclaim := pvclaims[0]
		pv := pvs[0]
		defer func() {
			err := fpv.DeletePersistentVolumeClaim(ctx, client, pvclaim.Name, namespace)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			err = multiVCe2eVSphere.waitForCNSVolumeToBeDeletedInMultiVC(pv.Spec.CSI.VolumeHandle)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}()

		ginkgo.By("Create deployment")
		deployment, err := createDeployment(ctx, client, int32(replica), labelsMap, nil, namespace,
			[]*v1.PersistentVolumeClaim{pvclaim}, execRWXCommandPod, false, nginxImage)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Rebooting VC1")
		vCenterHostname := strings.Split(multiVCe2eVSphere.multivcConfig.Global.VCenterHostname, ",")
		vcAddress := vCenterHostname[0] + ":" + sshdPort
		framework.Logf("vcAddress - %s ", vcAddress)
		err = invokeVCenterReboot(ctx, vcAddress)
		isVcRebooted = true
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		err = waitForHostToBeUp(vCenterHostname[0])
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		ginkgo.By("Done with reboot")

		essentialServices := []string{spsServiceName, vsanhealthServiceName, vpxdServiceName}
		checkVcenterServicesRunning(ctx, vcAddress, essentialServices)

		//After reboot
		multiVCbootstrap()

		ginkgo.By("Verify deployment pods should be in running ready state post vc reboot")
		pods, err := fdep.GetPodsForDeployment(ctx, client, deployment)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		pod := pods.Items[0]
		err = fpod.WaitForPodNameRunningInNamespace(ctx, client, pod.Name, namespace)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		defer func() {
			framework.Logf("Delete deployment set")
			err := client.AppsV1().Deployments(namespace).Delete(ctx, deployment.Name, *metav1.NewDeleteOptions(0))
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}()

		ginkgo.By("Verify volume placement, should match with the allowed topology specified")
		datastoreUrlsVc1, _, _, _, err := fetchDatastoreListMap(ctx, client)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		isCorrectPlacement := multiVCe2eVSphere.verifyPreferredDatastoreMatchInMultiVC(pv.Spec.CSI.VolumeHandle,
			datastoreUrlsVc1)
		gomega.Expect(isCorrectPlacement).To(gomega.BeTrue(), fmt.Sprintf("Volume provisioning has happened on the "+
			"wrong datastore. Expected 'true', got '%v'", isCorrectPlacement))

		ginkgo.By("Scale up deployment pods to 5 replicas")
		replica = 5
		_, _, err = createVerifyAndScaleDeploymentPods(ctx, client, namespace, int32(replica),
			true, labelsMap, pvclaim, nil, execRWXCommandPod, nginxImage, true, deployment, 0)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Scale down deployment pods to 1 replica")
		replica = 1
		_, _, err = createVerifyAndScaleDeploymentPods(ctx, client, namespace, int32(replica),
			true, labelsMap, pvclaim, nil, execRWXCommandPod, nginxImage, true, deployment, 0)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
	})

	/* TESTCASE-10
	   VSAN-health down on VC1
	   SC → default values with Immediate Binding mode

	   Steps:
	   1. Create SC with default values so all the AZ's should be considered for volume provisioning
	   2. Create few dynamic PVCs and add labels to PVCs and PVs.
	   3. Verify pvc should reach to bound state
	   4. Bring down the VSAN-health on VC-1
	   5. Create new PVCs, add labels to the new pvcs when vsan health is down on vc1
	   6. New pvc should be stuck in a pending state
	   7. Create deployment Pods for the PVC created in step #2 with replica count 1.
	   8. Deployment pods should be stuck in ContainerCreating state due to vsan health cns query will fail
	   9. Update the label on PV and PVC created in step #2
	   10. Bring up the vSAN-health on vc1
	   11. Wait for 2 full sync cycle
	   12. Verify deployment Pods should come back to running state.
	   13. Verify that the pvcs which were created when service is down, evetually should reach to Bound state
	   14. Verify that the labels gets updated in CNS.
	   15. Perform scaleup operation on deployment pods by increasing the replica count to 5.
	   16. Perform scaledown operation on dep3 by decreasing the replica count to 2.
	   17. Perform cleanup by deleting pods,pvc and sc
	*/

	ginkgo.It("Pod creation and lables update when "+
		"vsan health is down", ginkgo.Label(p1, file, vanilla, multiVc, newTest, disruptive), func() {

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		var fullSyncWaitTime int
		var depl []*appsv1.Deployment

		// pvc and deployment pod count
		replica := 1
		createPvcItr := 4

		// Read full-sync value.
		if os.Getenv(envFullSyncWaitTime) != "" {
			fullSyncWaitTime, err = strconv.Atoi(os.Getenv(envFullSyncWaitTime))
			framework.Logf("Full-Sync interval time value is = %v", fullSyncWaitTime)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}

		ginkgo.By(fmt.Sprintf("Creating Storage Class with access mode %q and fstype %q", accessmode, nfs4FSType))
		storageclass, pvclaims, err := createStorageClassWithMultiplePVCs(client, namespace, labelsMap,
			scParameters, diskSize, nil, bindingModeImm, false, accessmode,
			"", nil, createPvcItr, false, false)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		defer func() {
			err := client.StorageV1().StorageClasses().Delete(ctx, storageclass.Name, *metav1.NewDeleteOptions(0))
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}()

		defer func() {
			for i := 0; i < len(pvclaims); i++ {
				pv := getPvFromClaim(client, pvclaims[i].Namespace, pvclaims[i].Name)
				err := fpv.DeletePersistentVolumeClaim(ctx, client, pvclaims[i].Name, namespace)
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
				err = multiVCe2eVSphere.waitForCNSVolumeToBeDeletedInMultiVC(pv.Spec.CSI.VolumeHandle)
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
			}
		}()

		ginkgo.By("Verify PVC Bound state and CNS side verification")
		_, err = checkVolumeStateAndPerformCnsVerification(ctx, client, pvclaims, "", "")
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Bring down Vsan-health service on VC1")
		vCenterHostname := strings.Split(multiVCe2eVSphere.multivcConfig.Global.VCenterHostname, ",")
		vcAddress := vCenterHostname[0] + ":" + sshdPort
		framework.Logf("vcAddress - %s ", vcAddress)
		err = invokeVCenterServiceControl(ctx, stopOperation, vsanhealthServiceName, vcAddress)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		isVsanHealthServiceStopped = true
		defer func() {
			if isVsanHealthServiceStopped {
				framework.Logf("Bringing vsanhealth up before terminating the test")
				startVCServiceWait4VPs(ctx, vcAddress, vsanhealthServiceName, &isVsanHealthServiceStopped)
			}
		}()

		ginkgo.By("Creating new pvcs when vsan health is down, it should be stuck in a pending state")
		createPvcItr = 3
		_, pvclaimsNew, err := createStorageClassWithMultiplePVCs(client, namespace, labelsMap,
			scParameters, diskSize, nil, bindingModeImm, false, accessmode,
			"", storageclass, createPvcItr, true, false)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		defer func() {
			for i := 0; i < len(pvclaimsNew); i++ {
				pv := getPvFromClaim(client, pvclaimsNew[i].Namespace, pvclaimsNew[i].Name)
				err := fpv.DeletePersistentVolumeClaim(ctx, client, pvclaimsNew[i].Name, namespace)
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
				err = multiVCe2eVSphere.waitForCNSVolumeToBeDeletedInMultiVC(pv.Spec.CSI.VolumeHandle)
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
			}
		}()

		ginkgo.By("Create Deployment pod with replica count 1 for each rwx pvc")
		for i := 0; i < len(pvclaims); i++ {
			deploymentSpec := getDeploymentSpec(ctx, client, int32(replica), labelsMap, nil, namespace,
				pvclaims, execRWXCommandPod, false, nginxImage)
			deployment, err := client.AppsV1().Deployments(namespace).Create(ctx, deploymentSpec, metav1.CreateOptions{})
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			depl = append(depl, deployment)
		}

		labelKey := "volIdNew"
		labelValue := "pvcVolumeNew"
		labels := make(map[string]string)
		labels[labelKey] = labelValue

		ginkgo.By("Updating labels for PVC and PV")
		for i := 0; i < len(pvclaims); i++ {
			pv := getPvFromClaim(client, pvclaims[i].Namespace, pvclaims[i].Name)
			framework.Logf("Updating labels %+v for pvc %s in namespace %s", labels, pvclaims[i].Name,
				pvclaims[i].Namespace)
			pvc, err := client.CoreV1().PersistentVolumeClaims(namespace).Get(ctx, pvclaims[i].Name, metav1.GetOptions{})
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

		ginkgo.By("Verify deployment pods should be in running ready state post vsan health service up")
		for i := 0; i < len(depl); i++ {
			pods, err := fdep.GetPodsForDeployment(ctx, client, depl[i])
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			pod := pods.Items[0]
			err = fpod.WaitForPodNameRunningInNamespace(ctx, client, pod.Name, namespace)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}
		defer func() {
			framework.Logf("Delete deployment set")
			for i := 0; i < len(depl); i++ {
				err := client.AppsV1().Deployments(namespace).Delete(ctx, depl[i].Name, *metav1.NewDeleteOptions(0))
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
			}
		}()

		ginkgo.By("Verify PVC Bound state and CNS side verification for newly created pvc")
		_, err = checkVolumeStateAndPerformCnsVerification(ctx, client, pvclaimsNew, "", "")
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Waiting for labels to be updated for PVC and PV")
		for i := 0; i < len(pvclaims); i++ {
			pv := getPvFromClaim(client, pvclaims[i].Namespace, pvclaims[i].Name)

			framework.Logf("Waiting for labels %+v to be updated for pvc %s in namespace %s",
				labels, pvclaims[i].Name, pvclaims[i].Namespace)
			err = multiVCe2eVSphere.waitForLabelsToBeUpdatedInMultiVC(pv.Spec.CSI.VolumeHandle, labels,
				string(cnstypes.CnsKubernetesEntityTypePVC), pvclaims[i].Name, pvclaims[i].Namespace)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())

			framework.Logf("Waiting for labels %+v to be updated for pv %s", labels, pv.Name)
			err = multiVCe2eVSphere.waitForLabelsToBeUpdatedInMultiVC(pv.Spec.CSI.VolumeHandle, labels,
				string(cnstypes.CnsKubernetesEntityTypePV), pv.Name, pv.Namespace)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}

		ginkgo.By("Scale up deployments to replica count 5")
		for i := 0; i < len(depl); i++ {
			replica = 5
			_, _, err = createVerifyAndScaleDeploymentPods(ctx, client, namespace, int32(replica),
				true, labelsMap, pvclaims[i], nil, execRWXCommandPod, nginxImage, true, depl[i], 0)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}

		ginkgo.By("Scale down deployment3 pods to 2 replica")
		replica = 2
		_, _, err = createVerifyAndScaleDeploymentPods(ctx, client, namespace, int32(replica),
			true, labelsMap, pvclaims[2], nil, execRWXCommandPod, nginxImage, true, depl[2], 0)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
	})

	/* TESTCASE-11
	   sps service down
	   SC → all allowed topologies specified with Immediate Binding mode

	   Steps:
	   1. Create SC with all allowed topology values specified so all the AZ's should be considered for volume provisioning
	   2. Create few dynamic PVCs and add labels to PVCs and PVs.
	   3. Verify pvc should reach to bound state
	   4. Bring down the sps-service on vc1
	   5. Create deployment Pods for the PVC created in step #2 with replica count 1.
	   8. Deployment pods should be stuck in ContainerCreating state due to sps service down
	   10. Bring up the sps service on vc1
	   12. Verify deployment Pods should come back to running state.
	   15. Perform scaleup operation on deployment pods by increasing the replica count to 3.
	   16. Perform scaledown operation on dep3 by decreasing the replica count to 2.
	   17. Perform cleanup by deleting pods,pvc and sc
	*/

	ginkgo.It("RWX pvcs creation and in between sps "+
		"service is down on any AZ", ginkgo.Label(p1, file, vanilla, multiVc, newTest, disruptive), func() {

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		var depl []*appsv1.Deployment

		// storage policy of vc1
		storagePolicyInVc1 := GetAndExpectStringEnvVar(envStoragePolicyNameVC1)
		scParameters[scParamStoragePolicyName] = storagePolicyInVc1

		// pvc and deployment pod count
		replica := 1
		createPvcItr := 4

		// here, we are considering the allowed topology of all AZs
		topValStartIndex = 0
		topValEndIndex = 3

		ginkgo.By("Set specific allowed topology of vc1")
		allowedTopologies = setSpecificAllowedTopology(allowedTopologies, topkeyStartIndex, topValStartIndex,
			topValEndIndex)

		ginkgo.By(fmt.Sprintf("Creating Storage Class with access mode %q and fstype %q", accessmode, nfs4FSType))
		storageclass, pvclaims, err := createStorageClassWithMultiplePVCs(client, namespace, labelsMap,
			scParameters, diskSize, allowedTopologies, bindingModeImm, false, accessmode,
			"", nil, createPvcItr, false, false)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		defer func() {
			err := client.StorageV1().StorageClasses().Delete(ctx, storageclass.Name, *metav1.NewDeleteOptions(0))
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}()

		defer func() {
			for i := 0; i < len(pvclaims); i++ {
				pv := getPvFromClaim(client, pvclaims[i].Namespace, pvclaims[i].Name)
				err := fpv.DeletePersistentVolumeClaim(ctx, client, pvclaims[i].Name, namespace)
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
				err = multiVCe2eVSphere.waitForCNSVolumeToBeDeletedInMultiVC(pv.Spec.CSI.VolumeHandle)
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
			}
		}()

		ginkgo.By("Verify PVC Bound state and CNS side verification")
		_, err = checkVolumeStateAndPerformCnsVerification(ctx, client, pvclaims, "", "")
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Bring down SPS service")
		vCenterHostname := strings.Split(multiVCe2eVSphere.multivcConfig.Global.VCenterHostname, ",")
		vcAddress := vCenterHostname[0] + ":" + sshdPort
		framework.Logf("vcAddress - %s ", vcAddress)
		err = invokeVCenterServiceControl(ctx, stopOperation, spsServiceName, vcAddress)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		isSPSServiceStopped = true
		err = waitVCenterServiceToBeInState(ctx, spsServiceName, vcAddress, svcStoppedMessage)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		defer func() {
			if isSPSServiceStopped {
				framework.Logf("Bringing sps up before terminating the test")
				startVCServiceWait4VPs(ctx, vcAddress, spsServiceName, &isSPSServiceStopped)
				isSPSServiceStopped = false
			}
		}()

		ginkgo.By("Create Deployment pod with replica count 1 for each rwx pvc")
		for i := 0; i < len(pvclaims); i++ {
			deploymentSpec := getDeploymentSpec(ctx, client, int32(replica), labelsMap, nil, namespace,
				pvclaims, execRWXCommandPod, false, nginxImage)
			deployment, err := client.AppsV1().Deployments(namespace).Create(ctx, deploymentSpec, metav1.CreateOptions{})
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			depl = append(depl, deployment)
		}

		ginkgo.By("Bringup SPS service")
		startVCServiceWait4VPs(ctx, vcAddress, spsServiceName, &isSPSServiceStopped)

		ginkgo.By("Verify deployment pods should be in running ready state post sps service up")
		for i := 0; i < len(depl); i++ {
			pods, err := fdep.GetPodsForDeployment(ctx, client, depl[i])
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			pod := pods.Items[0]
			err = fpod.WaitForPodNameRunningInNamespace(ctx, client, pod.Name, namespace)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}
		defer func() {
			framework.Logf("Delete deployment set")
			for i := 0; i < len(depl); i++ {
				err := client.AppsV1().Deployments(namespace).Delete(ctx, depl[i].Name, *metav1.NewDeleteOptions(0))
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
			}
		}()

		ginkgo.By("Scale up deployments to replica count 3")
		for i := 0; i < len(depl); i++ {
			replica = 3
			_, _, err = createVerifyAndScaleDeploymentPods(ctx, client, namespace, int32(replica),
				true, labelsMap, pvclaims[i], nil, execRWXCommandPod, nginxImage, true, depl[i], 0)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}

		ginkgo.By("Scale down deployment3 pods to 2 replica")
		replica = 2
		_, _, err = createVerifyAndScaleDeploymentPods(ctx, client, namespace, int32(replica),
			true, labelsMap, pvclaims[2], nil, execRWXCommandPod, nginxImage, true, depl[2], 0)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
	})

	/*
	   TESTCASE-13
	   When vSAN FS is enabled in all 3 VCs
	   SC → all allowed topology specified and Immediate Binding mode

	   1. Create Storage Class with all allowed topology specified and with Immediate Binding mode.
	   2. Create StatefulSet with 3 replicas using Parallel Pod management policy and with RWX access
	   mode and specify node selector term only to vc1 AZ
	   3. Wait for Pod and PVC to reach running ready state.
	   4. Volume provisioning can happen on any AZ but statefulset pod placement should happen only on vc1 AZ.
	   5. Create few RWX pvcs.
	   6. Wait for PVCs to reach Bound state.
	   7. Volume provisoning can happen on any AZ since all allowed topologies are specified.
	   8. Create 3 deployment pods each connected to rwx pvc with replica count 1.
	   9. Specify node selector term to vc2 AZ so that all deployment pods should get created only on vc2 AZ.
	   10. Create standalone pod and attached it to the 4th rwx pvc.
	   11. Specify node selector term for standalone pod to  vc3 AZ.
	   12. Verify pod should reach to ready running sttae.
	   13. Verify pod should get created on the specified node selector term AZ.
	   14. Perform scaleup operation on statefulset pods. Increase the replica count to 5.
	   15. Verify newly created statefulset pods to get created on specified node selector term AZ.
	   16. Perform scaleup operation on any one deployment pods. Increase the replica count to 3.
	   17. Verify scaling operation went smooth and newly created pods should get created on the specified node selector.
	   18. Perform cleanup by deleting StatefulSet, Pods, PVCs and SC.
	*/

	ginkgo.It("Different types of pods creation attached to rwx pvcs "+
		"involving scaling operation", ginkgo.Label(p0, file, vanilla, multiVc, newTest), func() {

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		var depl []*appsv1.Deployment
		var podList []*v1.Pod
		var pods *v1.PodList

		// pvc and deployment pod count
		replica := 1
		stsReplicas := 3
		createPvcItr := 4
		createDepItr := 1
		noPodsToDeploy := 3

		// here, we are considering the allowed topology of all AZs
		topValStartIndex = 0
		topValEndIndex = 3

		ginkgo.By("Set all allowed topology of all VCs for storage class creation")
		allowedTopologiesForSC := setSpecificAllowedTopology(allowedTopologies, topkeyStartIndex, topValStartIndex,
			topValEndIndex)

		ginkgo.By(fmt.Sprintf("Creating Storage Class with access mode %q and fstype %q", accessmode, nfs4FSType))
		storageclass, pvclaims, err := createStorageClassWithMultiplePVCs(client, namespace, labelsMap,
			scParameters, diskSize, allowedTopologiesForSC, bindingModeImm, false, accessmode,
			"nginx-sc", nil, createPvcItr, false, false)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		defer func() {
			err := client.StorageV1().StorageClasses().Delete(ctx, storageclass.Name, *metav1.NewDeleteOptions(0))
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}()

		ginkgo.By("Verify PVC Bound state and CNS side verification")
		_, err = checkVolumeStateAndPerformCnsVerification(ctx, client, pvclaims, "", "")
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		// here, we are considering allowed topology of vc1(zone-1) for statefulset pod node selector terms
		topValStartIndex = 0
		topValEndIndex = 1

		ginkgo.By("Setting node affinity for statefulset pods to be created in vc1 AZ")
		allowedTopologiesForSts := setSpecificAllowedTopology(allowedTopologies, topkeyStartIndex, topValStartIndex,
			topValEndIndex)

		ginkgo.By("Create StatefulSet with replica count 3 with RWX PVC access mode")
		service, statefulset, err := createStafeulSetAndVerifyPVAndPodNodeAffinty(ctx, client, namespace, true,
			int32(stsReplicas), true, allowedTopologiesForSts, false, false, false, accessmode, storageclass, false, "")
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		defer func() {
			fss.DeleteAllStatefulSets(ctx, client, namespace)
			deleteService(namespace, client, service)
		}()

		defer func() {
			for i := 0; i < len(pvclaims); i++ {
				pv := getPvFromClaim(client, pvclaims[i].Namespace, pvclaims[i].Name)
				err := fpv.DeletePersistentVolumeClaim(ctx, client, pvclaims[i].Name, namespace)
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
				err = multiVCe2eVSphere.waitForCNSVolumeToBeDeletedInMultiVC(pv.Spec.CSI.VolumeHandle)
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
			}
		}()

		ginkgo.By("Set node selector term for deployment pods so that all pods " +
			"should get created on one single AZ i.e. vc2(zone-2)")
		/* here in 3-VC setup, deployment pod node affinity is taken as  k8s-zone -> zone-2
		i.e only VC2 allowed topology
		*/
		topValStartIndex = 1
		topValEndIndex = 2
		allowedTopologiesForDep := setSpecificAllowedTopology(allowedTopologies, topkeyStartIndex, topValStartIndex,
			topValEndIndex)
		nodeSelectorTermsForDep, err := getNodeSelectorMapForDeploymentPods(allowedTopologiesForDep)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Set node selector term for standlone pods so that all pods should get " +
			"created on one single AZ i.e. cluster-3(rack-3)")
		/*
			We are considering allowed topology of VC-1 and VC-3 i.e.
			k8s-zone ->  zone-3
		*/
		topValStartIndex = 2
		topValEndIndex = 3

		ginkgo.By("Setting allowed topology to vc-3")
		allowedTopologiesForStandalonePod := setSpecificAllowedTopology(allowedTopologies, topkeyStartIndex, topValStartIndex,
			topValEndIndex)
		nodeSelectorTermsForPods, err := getNodeSelectorMapForDeploymentPods(allowedTopologiesForStandalonePod)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Create Deployments")
		for i := 0; i < len(pvclaims); i++ {
			if i != 3 {
				var deployment []*appsv1.Deployment
				deployment, pods, err = createVerifyAndScaleDeploymentPods(ctx, client, namespace, int32(replica), false, labelsMap,
					pvclaims[i], nodeSelectorTermsForDep, execRWXCommandPod, nginxImage, false, nil, createDepItr)
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
				depl = append(depl, deployment...)
			} else {
				ginkgo.By("Create 3 standalone Pods using the same PVC with different read/write permissions")
				podList, err = createStandalonePodsForRWXVolume(client, ctx, namespace, nodeSelectorTermsForPods,
					pvclaims[i], false, execRWXCommandPod, noPodsToDeploy)
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
			}
		}
		defer func() {
			framework.Logf("Delete deployment set")
			for i := 0; i < len(depl); i++ {
				err := client.AppsV1().Deployments(namespace).Delete(ctx, depl[i].Name, *metav1.NewDeleteOptions(0))
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
			}
		}()
		defer func() {
			for i := 0; i < len(podList); i++ {
				ginkgo.By(fmt.Sprintf("Deleting the pod %s in namespace %s", podList[i].Name, namespace))
				err := fpod.DeletePodWithWait(ctx, client, podList[i])
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
			}
		}()

		ginkgo.By("Verify deployment pods are scheduled on a specified node selector terms")
		err = verifyDeploymentPodNodeAffinity(ctx, client, namespace, allowedTopologiesForDep, pods)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Verify standalone pods are scheduled on a selected node selector terms")
		for i := 0; i < len(podList); i++ {
			err = verifyStandalonePodAffinity(ctx, client, podList[i], allowedTopologies)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}

		ginkgo.By("Scale up deployment1 pods to 5 replicas")
		replica = 3
		_, _, err = createVerifyAndScaleDeploymentPods(ctx, client, namespace, int32(replica),
			true, labelsMap, pvclaims[0], nil, execRWXCommandPod, nginxImage, true, depl[0], 0)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Scale down deployment2 pods to 2 replica")
		replica = 2
		_, _, err = createVerifyAndScaleDeploymentPods(ctx, client, namespace, int32(replica),
			true, labelsMap, pvclaims[1], nil, execRWXCommandPod, nginxImage, true, depl[1], 0)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Perform scaleup/scaledown operation on statefulsets")
		scaleUpReplicaCount := 6
		scaleDownReplicaCount := 1
		err = performScalingOnStatefulSetAndVerifyPvNodeAffinity(ctx, client, int32(scaleUpReplicaCount),
			int32(scaleDownReplicaCount), statefulset, false, namespace, nil, true, true, false)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
	})

	/*
		Testcase-8
		Static PVC creation
		SC → specific allowed topology (VC1) → K8s:zone-zone-1

		Steps:
		1. Create a file share on any 1 VC
		2. Create PV using the above created File Share
		3. Create PVC using above created  PV
		4. Wait for PV and PVC to be in Bound state
		5. Create Pod using PVC created in step #3
		6. Verify Pod should reach to Running state.
		7. Verify volume provisioning and pod placement should happen on the AZ where File Share is created.
		8. Verify CNS metedata.
		9. Perform cleanup by deleting PVC, PV and SC
	*/

	ginkgo.It("Static volume creation in rwx multivc setup", ginkgo.Label(p0, file, vanilla,
		level5, level2, newTest), func() {

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		// creating file share on VC1 vsan FS enabled datastore
		datastoreUrlVc1 := GetAndExpectStringEnvVar(envSharedDatastoreURLVC1)

		// fetch file share volume id
		fileShareVolumeId, err := multiVCe2eVSphere.createFileShareForMultiVc(datastoreUrlVc1, 0)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Creating the PV with policy set to delete with ReadWritePermissions")
		pvSpec := getPersistentVolumeSpecForFileShare(fileShareVolumeId,
			v1.PersistentVolumeReclaimDelete, labelsMap, accessmode)
		pv, err := client.CoreV1().PersistentVolumes().Create(ctx, pvSpec, metav1.CreateOptions{})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		err = multiVCe2eVSphere.waitForCNSVolumeToBeCreatedInMultiVC(pv.Spec.CSI.VolumeHandle)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Creating the PVC where static PV is created with ReadWrite permissions")
		pvc := getPersistentVolumeClaimSpecForFileShare(namespace, labelsMap, pv.Name, accessmode)
		pvc, err = client.CoreV1().PersistentVolumeClaims(namespace).Create(ctx, pvc, metav1.CreateOptions{})
		var pvclaims []*v1.PersistentVolumeClaim
		pvclaims = append(pvclaims, pvc)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		// Wait for PV and PVC to Bind.
		framework.ExpectNoError(fpv.WaitOnPVandPVC(ctx, client, f.Timeouts, namespace, pv, pvc))
		defer func() {
			ginkgo.By("Deleting the PV Claim")
			err = fpv.DeletePersistentVolumeClaim(ctx, client, pvc.Name, namespace)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}()

		ginkgo.By("Verify PVC Bound state and CNS side verification")
		_, err = checkVolumeStateAndPerformCnsVerification(ctx, client, pvclaims, "", "")
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Set node selector term for deployment pods so that all pods " +
			"should get created on one single AZ i.e. VC-2(zone-2)")
		/* here in 3-VC setup, deployment pod node affinity is taken as  k8s-zone -> zone-2
		i.e only VC2 allowed topology
		*/
		topValStartIndex = 1
		topValEndIndex = 2
		allowedTopologies = setSpecificAllowedTopology(allowedTopologies, topkeyStartIndex, topValStartIndex,
			topValEndIndex)
		nodeSelectorTerms, err := getNodeSelectorMapForDeploymentPods(allowedTopologies)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Creating the Pod attached to ReadWrite permission PVC")
		pod1, err := createPod(ctx, client, namespace, nodeSelectorTerms, pvclaims, false, execRWXCommandPod)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		defer func() {
			ginkgo.By("Deleting the Pod")
			framework.ExpectNoError(fpod.DeletePodWithWait(ctx, client, pod1), "Failed to delete pod", pod1.Name)
		}()

		ginkgo.By("Verify the volume is accessible and available to the pod by creating an empty file")
		filepath := filepath.Join("/mnt/volume1", "/emptyFile.txt")
		_, err = e2eoutput.LookForStringInPodExec(namespace, pod1.Name, []string{"/bin/touch", filepath}, "", time.Minute)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Verify container volume metadata is matching the one in CNS cache")
		err = verifyVolumeMetadataInCNSForMultiVC(&multiVCe2eVSphere, pv.Spec.CSI.VolumeHandle, pvc.Name, pv.Name, pod1.Name)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Verify volume placement, should match with the allowed topology specified")
		datastoreUrlsRack1, _, _, _, err := fetchDatastoreListMap(ctx, client)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		isCorrectPlacement := multiVCe2eVSphere.verifyPreferredDatastoreMatchInMultiVC(pv.Spec.CSI.VolumeHandle,
			datastoreUrlsRack1)
		gomega.Expect(isCorrectPlacement).To(gomega.BeTrue(), fmt.Sprintf("Volume provisioning has happened on the wrong "+
			"datastore. Expected 'true', got '%v'", isCorrectPlacement))
	})
})
