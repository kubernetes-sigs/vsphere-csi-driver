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
	"strconv"
	"strings"
	"sync"
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
	fpv "k8s.io/kubernetes/test/e2e/framework/pv"
	fss "k8s.io/kubernetes/test/e2e/framework/statefulset"
	admissionapi "k8s.io/pod-security-admission/api"
)

var _ = ginkgo.Describe("[no-hci-mesh-topology-multivc] No-Hci-Mesh-Topology-MultiVc", func() {
	f := framework.NewDefaultFramework("rwx-topology")
	f.NamespacePodSecurityEnforceLevel = admissionapi.LevelPrivileged
	var (
		client    clientset.Interface
		namespace string

		allowedTopologies           []v1.TopologySelectorLabelRequirement
		topValStartIndex            int
		topValEndIndex              int
		topkeyStartIndex            int
		scParameters                map[string]string
		bindingModeWffc             storagev1.VolumeBindingMode
		bindingModeImm              storagev1.VolumeBindingMode
		accessmode                  v1.PersistentVolumeAccessMode
		labelsMap                   map[string]string
		replica                     int32
		createPvcItr                int
		createDepItr                int
		pvs                         []*v1.PersistentVolume
		pvclaim                     *v1.PersistentVolumeClaim
		pv                          *v1.PersistentVolume
		sshClientConfig             *ssh.ClientConfig
		nimbusGeneratedK8sVmPwd     string
		allMasterIps                []string
		masterIp                    string
		isStorageProfileDeleted     bool
		storagePolicyToDelete       string
		isVsanHealthServiceStopped  bool
		isSPSServiceStopped         bool
		err                         error
		csiNamespace                string
		revertToOriginalVsphereConf bool
		vCenterIP                   string
		vCenterUser                 string
		vCenterPassword             string
		vCenterPort                 string
		dataCenter                  string
		csiReplicas                 int32
		noPodsToDeploy              int
		originalVc3PasswordChanged  bool
		k8sVersion                  string
		ClusterdatastoreListVC      []map[string]string
		ClusterdatastoreListVC1     map[string]string
		ClusterdatastoreListVC2     map[string]string
		ClusterdatastoreListVC3     map[string]string
		nodeList                    *v1.NodeList
		fullSyncWaitTime            int
	)

	ginkgo.BeforeEach(func() {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		client = f.ClientSet
		namespace = f.Namespace.Name

		// connecting to multiple VCs
		multiVCbootstrap()

		// fetch list f k8s nodes
		nodeList, err = fnodes.GetReadySchedulableNodes(ctx, f.ClientSet)
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
		topologyMap := GetAndExpectStringEnvVar(topologyMap)
		allowedTopologies = createAllowedTopolgies(topologyMap, topologyLength)

		//setting map values
		labelsMap["app"] = "test"
		scParameters[scParamFsType] = nfs4FSType

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

		// nimbus export variable
		nimbusGeneratedK8sVmPwd = GetAndExpectStringEnvVar(nimbusK8sVmPwd)
		storagePolicyToDelete = GetAndExpectStringEnvVar(envStoragePolicyNameToDeleteLater)

		// read csi pods namespace
		csiNamespace = GetAndExpectStringEnvVar(envCSINamespace)

		// read csi deployment pods replica count
		csiDeployment, err := client.AppsV1().Deployments(csiNamespace).Get(
			ctx, vSphereCSIControllerPodNamePrefix, metav1.GetOptions{})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		csiReplicas = *csiDeployment.Spec.Replicas

		// save original vsphere conf credentials in temp variable
		vCenterIP = multiVCe2eVSphere.multivcConfig.Global.VCenterHostname
		vCenterUser = multiVCe2eVSphere.multivcConfig.Global.User
		vCenterPassword = multiVCe2eVSphere.multivcConfig.Global.Password
		vCenterPort = multiVCe2eVSphere.multivcConfig.Global.VCenterPort
		dataCenter = multiVCe2eVSphere.multivcConfig.Global.Datacenters

		// fetching k8s version
		v, err := client.Discovery().ServerVersion()
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		k8sVersion = v.Major + "." + v.Minor

		// fetching cluster details
		clientIndex := 0
		clusterComputeResource, _, err = getClusterNameForMultiVC(ctx, &multiVCe2eVSphere, clientIndex)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		// fetching list of datastores available in different VCs
		ClusterdatastoreListVC1, ClusterdatastoreListVC2,
			ClusterdatastoreListVC3, err = getDatastoresListFromMultiVCs(masterIp, sshClientConfig,
			clusterComputeResource[0])
		ClusterdatastoreListVC = append(ClusterdatastoreListVC, ClusterdatastoreListVC1,
			ClusterdatastoreListVC2, ClusterdatastoreListVC3)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		// Read full-sync value.
		if os.Getenv(envFullSyncWaitTime) != "" {
			fullSyncWaitTime, err = strconv.Atoi(os.Getenv(envFullSyncWaitTime))
			framework.Logf("Full-Sync interval time value is = %v", fullSyncWaitTime)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}
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

		if originalVc3PasswordChanged {
			clientIndex2 := 2
			vcAddress3 := strings.Split(vCenterIP, ",")[2] + ":" + sshdPort
			username3 := strings.Split(vCenterUser, ",")[2]
			originalPassword3 := strings.Split(vCenterPassword, ",")[2]
			newPassword3 := "Admin!23"
			ginkgo.By("Reverting the password change")
			err = invokeVCenterChangePassword(ctx, username3, newPassword3, originalPassword3, vcAddress3,
				clientIndex2)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			originalVc3PasswordChanged = false
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
	    13. Perform scale up of deployment Pods to replica count 5. Verify Scaling operation should go smooth.
		14. Perform scale-down operation on deployment pods. Decrease the replica count to 1.
		15. Verify scale down operation went smooth.
	    16. Perform cleanup by deleting deployment Pods, PVCs and the StorageClass (SC)
	*/

	ginkgo.It("[rwx-topology-multivc-positive] Deployment pods and standalone pods creation with rwx pvcs "+
		"with different allowed topology given in SCs", ginkgo.Label(p0, file, vanilla, multiVc, newTest), func() {

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		// variable declaration
		var pvs1, pvs2 []*v1.PersistentVolume
		var pvclaim1, pvclaim2 *v1.PersistentVolumeClaim
		var pv1, pv2 *v1.PersistentVolume

		// deployment pod and pvc count
		replica = 3
		createPvcItr = 1
		createDepItr = 1

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
		deploymentList, _, err := createVerifyAndScaleDeploymentPods(ctx, client, namespace, replica, false, labelsMap,
			pvclaim1, nodeSelectorTerms, execRWXCommandPod, nginxImage, false, nil, createDepItr)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		defer func() {
			framework.Logf("Delete deployment set")
			err := client.AppsV1().Deployments(namespace).Delete(ctx, deploymentList[0].Name, *metav1.NewDeleteOptions(0))
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}()

		ginkgo.By("Verify PVC Bound state and CNS side verification")
		pvs1, err = checkVolumeStateAndPerformCnsVerification(ctx, client, pvclaims1, "", "")
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		// taking pv1 0th index because we are creating only single RWX PVC in this case
		pv1 = pvs1[0]

		// standalone pod and pvc count
		noPodsToDeploy = 3
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
			scParameters, diskSize, allowedTopologyForSC, bindingModeImm, false, accessmode, "", nil, 1, false, false)
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
		_, pods, err := createVerifyAndScaleDeploymentPods(ctx, client, namespace, replica, true,
			labelsMap, pvclaim1, nodeSelectorTerms, execRWXCommandPod, nginxImage, true, deploymentList[0], 0)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Verify deployment pods are scheduled on a specified node selector terms")
		err = verifyDeploymentPodNodeAffinity(ctx, client, namespace, allowedTopologies, pods)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Scale down deployment to 1 replica")
		replica = 1
		_, _, err = createVerifyAndScaleDeploymentPods(ctx, client, namespace, replica, true, labelsMap,
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

	ginkgo.It("[rwx-topology-multivc-positive] Statefulset creation with rwx pvcs having storage "+
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
			int32(stsReplicas), false, allowedTopologyForSC, false, false, false, accessmode, sc, false)
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

	ginkgo.It("[rwx-topology-multivc-positive] Multiple pods creation with single rwx pvcs created "+
		"with storage policy available ", ginkgo.Label(p0, file, vanilla, multiVc, newTest), func() {

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		// deployment pod and pvc count
		replica = 2
		createPvcItr = 1
		createDepItr = 1

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
		defer func() {
			err := fpv.DeletePersistentVolumeClaim(ctx, client, pvclaim.Name, namespace)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			err = multiVCe2eVSphere.waitForCNSVolumeToBeDeletedInMultiVC(pv.Spec.CSI.VolumeHandle)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}()

		ginkgo.By("Verify PVC Bound state and CNS side verification")
		pvs, err = checkVolumeStateAndPerformCnsVerification(ctx, client, pvclaims, storagePolicyInVc1Vc2, "")
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		// taking pvclaims and pv 0th index because we are creating only single RWX PVC in this case
		pvclaim = pvclaims[0]
		pv = pvs[0]

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
		deploymentList, _, err := createVerifyAndScaleDeploymentPods(ctx, client, namespace, replica, false, labelsMap,
			pvclaim, nil, execRWXCommandPod, nginxImage, false, nil, createDepItr)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		defer func() {
			framework.Logf("Delete deployment set")
			err := client.AppsV1().Deployments(namespace).Delete(ctx, deploymentList[0].Name, *metav1.NewDeleteOptions(0))
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}()

		ginkgo.By("Scale up deployment to 5 replica")
		replica = 5
		_, _, err = createVerifyAndScaleDeploymentPods(ctx, client, namespace, replica, true, labelsMap,
			pvclaim, nil, execRWXCommandPod, nginxImage, true, deploymentList[0], 0)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Scale down deployment to 1 replica")
		replica = 1
		_, pods, err := createVerifyAndScaleDeploymentPods(ctx, client, namespace, replica, true,
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

	ginkgo.It("[rwx-topology-multivc-positive] TC4Multiple standalone pods attached to single rwx pvc with datastore url"+
		"tagged to Az2 in SC", ginkgo.Label(p0, file, vanilla, multiVc, newTest), func() {

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		// pods and pvc count
		noPodsToDeploy = 3
		createPvcItr = 1

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
		defer func() {
			err := fpv.DeletePersistentVolumeClaim(ctx, client, pvclaim.Name, namespace)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			err = multiVCe2eVSphere.waitForCNSVolumeToBeDeletedInMultiVC(pv.Spec.CSI.VolumeHandle)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}()

		// taking pvclaims 0th index because we are creating only single RWX PVC in this case
		pvclaim = pvclaims[0]

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
		pvs, err = checkVolumeStateAndPerformCnsVerification(ctx, client, pvclaims, "", dsUrl)
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

	ginkgo.It("[rwx-topology-multivc-positive] Scaling operations involving statefulSet pods with multiple replicas, "+
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
			int32(stsReplicas), false, allowedTopologyForSC, false, false, false, accessmode, sc, false)
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

	ginkgo.It("[rwx-topology-multivc-positive] Deploy workload with allowed topology of vc1 and "+
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

		Steps//
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

	ginkgo.It("[rwx-topology-multivc-positive] TC7Same storage policy is available in vc1 and vc2 and later delete storage policy from "+
		"one of the VC", ginkgo.Label(p1, file, vanilla, multiVc, newTest), func() {

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		/* set allowed topology of vc1 and vc2*/
		topValStartIndex = 0
		topValEndIndex = 2
		clientIndex := 0

		// pods and pvc count
		noPodsToDeploy = 3
		createPvcItr = 3
		createDepItr = 1
		replica = 2

		// variable declaration
		var depl []*appsv1.Deployment
		var podList []*v1.Pod
		var err error

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
			if isStorageProfileDeleted {
				err = createStorageProfile(masterIp, sshClientConfig, storagePolicyToDelete, clientIndex)
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
				isStorageProfileDeleted = false
			}
		}()

		ginkgo.By("Creating multiple pvcs with rwx access mode")
		_, pvclaims, err := createStorageClassWithMultiplePVCs(client, namespace, labelsMap,
			scParameters, diskSize, allowedTopologies, bindingModeImm, false, accessmode,
			"", storageclass, createPvcItr, true, false)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Verify PVC Bound state and CNS side verification")
		pvs, err = checkVolumeStateAndPerformCnsVerification(ctx, client, pvclaims, "", "")
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		defer func() {
			for i := 0; i < len(pvclaims); i++ {
				pv = getPvFromClaim(client, pvclaims[i].Namespace, pvclaims[i].Name)
				err := fpv.DeletePersistentVolumeClaim(ctx, client, pvclaims[i].Name, namespace)
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
				err = multiVCe2eVSphere.waitForCNSVolumeToBeDeletedInMultiVC(pv.Spec.CSI.VolumeHandle)
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
			}
		}()

		ginkgo.By("Create Deployments and standalone pods")
		for i := 0; i < len(pvclaims); i++ {
			deploymentList, _, err := createVerifyAndScaleDeploymentPods(ctx, client, namespace, replica, false, labelsMap,
				pvclaims[i], nil, execRWXCommandPod, nginxImage, false, nil, createDepItr)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			depl = append(depl, deploymentList...)

			// Create 3 standalone pods and attach it to 3rd rwx pvc
			if i == 2 {
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
			pv = getPvFromClaim(client, pvclaims[i].Namespace, pvclaims[i].Name)
			isCorrectPlacement := multiVCe2eVSphere.verifyPreferredDatastoreMatchInMultiVC(pv.Spec.CSI.VolumeHandle,
				datastoreUrlsRack2)
			gomega.Expect(isCorrectPlacement).To(gomega.BeTrue(), fmt.Sprintf("Volume provisioning has happened on the wrong "+
				"datastore. Expected 'true', got '%v'", isCorrectPlacement))
		}

		ginkgo.By("Scale up deployment to 4 replica")
		replica = 4
		_, _, err = createVerifyAndScaleDeploymentPods(ctx, client, namespace, replica,
			true, labelsMap, pvclaim, nil, execRWXCommandPod, nginxImage, true, depl[0], 0)
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
		9. Perform cleanup by deleting Pods, PVCs and SC
	*/

	ginkgo.It("[rwx-topology-multivc-positive] TC9Creating statefulset with rwx pvcs with storage policy "+
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

		defer func() {
			err := fpv.DeletePersistentVolumeClaim(ctx, client, pvclaim.Name, namespace)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			err = multiVCe2eVSphere.waitForCNSVolumeToBeDeletedInMultiVC(pv.Spec.CSI.VolumeHandle)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}()

		ginkgo.By("Verify PVC Bound state and CNS side verification")
		pvs, err = checkVolumeStateAndPerformCnsVerification(ctx, client, pvclaims, "", "")
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		// taking pvclaims and pvs 0th index because we are creating only single RWX PVC in this case
		pvclaim = pvclaims[0]
		pv = pvs[0]

		ginkgo.By("Create deployment")
		deployment, err := createDeployment(ctx, client, int32(replica), labelsMap, nil, namespace,
			[]*v1.PersistentVolumeClaim{pvclaim}, execRWXCommandPod, false, nginxImage)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Rebooting VC1")
		vCenterHostname := strings.Split(multiVCe2eVSphere.multivcConfig.Global.VCenterHostname, ",")
		vcAddress := vCenterHostname[0] + ":" + sshdPort
		framework.Logf("vcAddress - %s ", vcAddress)
		err = invokeVCenterReboot(ctx, vcAddress)
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

	ginkgo.It("[rwx-topology-multivc-positive] Podcreation and lables update when "+
		"vsan health is down", ginkgo.Label(p1, file, vanilla, multiVc, newTest, disruptive), func() {

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		// variable declaration
		var depl []*appsv1.Deployment
		var fullSyncWaitTime int

		// pvc and deployment pod count
		replica = 1
		createPvcItr = 4
		createDepItr = 1

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
				pv = getPvFromClaim(client, pvclaims[i].Namespace, pvclaims[i].Name)
				err := fpv.DeletePersistentVolumeClaim(ctx, client, pvclaims[i].Name, namespace)
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
				err = multiVCe2eVSphere.waitForCNSVolumeToBeDeletedInMultiVC(pv.Spec.CSI.VolumeHandle)
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
			}
		}()

		ginkgo.By("Verify PVC Bound state and CNS side verification")
		pvs, err = checkVolumeStateAndPerformCnsVerification(ctx, client, pvclaims, "", "")
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
				pv = getPvFromClaim(client, pvclaimsNew[i].Namespace, pvclaimsNew[i].Name)
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
				true, labelsMap, pvclaim, nil, execRWXCommandPod, nginxImage, true, depl[i], 0)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}

		ginkgo.By("Scale down deployment3 pods to 2 replica")
		replica = 2
		_, _, err = createVerifyAndScaleDeploymentPods(ctx, client, namespace, replica,
			true, labelsMap, pvclaim, nil, execRWXCommandPod, nginxImage, true, depl[2], 0)
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

	ginkgo.It("[rwx-topology-multivc-positive] RWX pvcs creation and in between sps "+
		"service is down on any AZ", ginkgo.Label(p1, file, vanilla, multiVc, newTest, disruptive), func() {

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		// variable declaration
		var depl []*appsv1.Deployment

		// pvc and deployment pod count
		replica = 1
		createPvcItr = 4
		createDepItr = 4

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
				pv = getPvFromClaim(client, pvclaims[i].Namespace, pvclaims[i].Name)
				err := fpv.DeletePersistentVolumeClaim(ctx, client, pvclaims[i].Name, namespace)
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
				err = multiVCe2eVSphere.waitForCNSVolumeToBeDeletedInMultiVC(pv.Spec.CSI.VolumeHandle)
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
			}
		}()

		ginkgo.By("Verify PVC Bound state and CNS side verification")
		pvs, err = checkVolumeStateAndPerformCnsVerification(ctx, client, pvclaims, "", "")
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
				true, labelsMap, pvclaim, nil, execRWXCommandPod, nginxImage, true, depl[i], 0)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}

		ginkgo.By("Scale down deployment3 pods to 2 replica")
		replica = 2
		_, _, err = createVerifyAndScaleDeploymentPods(ctx, client, namespace, replica,
			true, labelsMap, pvclaim, nil, execRWXCommandPod, nginxImage, true, depl[2], 0)
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
		11. Specify node selector term for standalone pod to be vc2 and vc3 AZ.
		12. Verify pod should reach to ready running sttae.
		13. Verify pod should get created on the specified node selector term AZ.
		14. Perform scaleup operation on statefulset pods. Increase the replica count to 5.
		15. Verify newly created statefulset pods to get created on specified node selector term AZ.
		16. Perform scaleup operation on any one deployment pods. Increase the replica count to 3.
		17. Verify scaling operation went smooth and newly created pods should get created on the specified node selector.
		18. Perform cleanup by deleting StatefulSet, Pods, PVCs and SC.
	*/

	ginkgo.It("[rwx-topology-multivc-positive] TC13Different types of pods creation attached to rwx pvcs "+
		"involving scaling operation", ginkgo.Label(p0, file, vanilla, multiVc, newTest), func() {

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		// variable declaration
		var depl []*appsv1.Deployment
		var pods *v1.PodList
		var podList []*v1.Pod

		// pvc and deployment pod count
		replica = 1
		stsReplicas := 3
		createPvcItr = 4
		createDepItr = 1
		noPodsToDeploy = 3

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
		pvs, err = checkVolumeStateAndPerformCnsVerification(ctx, client, pvclaims, "", "")
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		// here, we are considering allowed topology of vc1(zone-1) for statefulset pod node selector terms
		topValStartIndex = 0
		topValEndIndex = 1

		ginkgo.By("Setting node affinity for statefulset pods to be created in vc1 AZ")
		allowedTopologiesForSts := setSpecificAllowedTopology(allowedTopologies, topkeyStartIndex, topValStartIndex,
			topValEndIndex)

		ginkgo.By("Create StatefulSet with replica count 3 with RWX PVC access mode")
		service, _, err := createStafeulSetAndVerifyPVAndPodNodeAffinty(ctx, client, namespace, true,
			int32(stsReplicas), true, allowedTopologiesForSts, false, false, false, accessmode, storageclass, false)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		defer func() {
			fss.DeleteAllStatefulSets(ctx, client, namespace)
			deleteService(namespace, client, service)
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
			We are considering allowed topology of VC-3 i.e.
			k8s-zone -> zone-3
		*/
		topValStartIndex = 2
		topValEndIndex = 3

		ginkgo.By("Setting allowed topology to vc-1 and vc-3")
		allowedTopologiesForStandalonePod := setSpecificAllowedTopology(allowedTopologies, topkeyStartIndex, topValStartIndex,
			topValEndIndex)
		nodeSelectorTermsForPods, err := getNodeSelectorMapForDeploymentPods(allowedTopologiesForStandalonePod)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Create Deployments")
		for i := 0; i < len(pvclaims); i++ {
			if i != 3 {
				var deployment []*appsv1.Deployment
				deployment, pods, err = createVerifyAndScaleDeploymentPods(ctx, client, namespace, replica, false, labelsMap,
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
			for i := 0; i < len(pvclaims); i++ {
				pv = getPvFromClaim(client, pvclaims[i].Namespace, pvclaims[i].Name)
				err := fpv.DeletePersistentVolumeClaim(ctx, client, pvclaims[i].Name, namespace)
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
				err = multiVCe2eVSphere.waitForCNSVolumeToBeDeletedInMultiVC(pv.Spec.CSI.VolumeHandle)
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
		_, _, err = createVerifyAndScaleDeploymentPods(ctx, client, namespace, replica,
			true, labelsMap, pvclaim, nil, execRWXCommandPod, nginxImage, true, depl[0], 0)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Scale down deployment2 pods to 2 replica")
		replica = 2
		_, _, err = createVerifyAndScaleDeploymentPods(ctx, client, namespace, replica,
			true, labelsMap, pvclaim, nil, execRWXCommandPod, nginxImage, true, depl[1], 0)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
	})

	/*
		TESTCASE-8
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
		8. Verify that volume Info CR got created with the right VC details
		9. Verify CNS metedata.
		10. Perform cleanup by deleting PVC, PV and SC.
	*/
	ginkgo.It("[rwx-topology-multivc-positive] test8", ginkgo.Label(p0, file, vanilla, multiVc, newTest), func() {
	})

	/*
		TESTCASE-12
		Validate Listvolume response
		query-limit = 3, list-volume-threshold = 1, Enable debug logs
		SC → default values with WFC Binding mode

		Steps:
		1. Create Storage Class with allowed topology set to defaul values.
		2. Create multiple PVC with "RWX"access mode.
		3. Create deployment Pods with replica count 3 using the PVC created in step #2.
		4. Once the Pods are up and running validate the logs. Verify the ListVolume Response in the logs ,
		now it should populate volume ID's and the node_id's.
		5. Check the token number on "nextToken".
		6. If there are 4 volumes in the list(index 0,1,2), and query limit = 3, then next_token = 2,
		after the first ListVolume response, and empty after the second ListVolume response.
		7. Scale up/down the deployment and validate the ListVolume Response
		8. Cleanup the data
	*/
	ginkgo.It("[rwx-topology-multivc-positive] test12", ginkgo.Label(p0, file, vanilla, multiVc, newTest), func() {

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		// variable declaration
		var depl []*appsv1.Deployment

		// pvc and deployment pod count
		replica = 3
		createPvcItr = 4
		createDepItr = 1
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

		ginkgo.By(fmt.Sprintf("Creating Storage Class with access mode %q and fstype %q", accessmode, nfs4FSType))
		storageclass, pvclaims, err := createStorageClassWithMultiplePVCs(client, namespace, labelsMap,
			scParameters, diskSize, nil, bindingModeWffc, false, accessmode,
			"nginx-sc", nil, createPvcItr, false, false)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		defer func() {
			err := client.StorageV1().StorageClasses().Delete(ctx, storageclass.Name, *metav1.NewDeleteOptions(0))
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}()

		defer func() {
			for i := 0; i < len(pvclaims); i++ {
				pv = getPvFromClaim(client, pvclaims[i].Namespace, pvclaims[i].Name)
				err := fpv.DeletePersistentVolumeClaim(ctx, client, pvclaims[i].Name, namespace)
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
				err = multiVCe2eVSphere.waitForCNSVolumeToBeDeletedInMultiVC(pv.Spec.CSI.VolumeHandle)
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
			}
		}()

		ginkgo.By("Create Deployments")
		for i := 0; i < len(pvclaims); i++ {
			var deployment []*appsv1.Deployment
			deployment, _, err = createVerifyAndScaleDeploymentPods(ctx, client, namespace, replica, false, labelsMap,
				pvclaims[i], nil, execRWXCommandPod, nginxImage, false, nil, createDepItr)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			depl = append(depl, deployment...)
		}
		defer func() {
			framework.Logf("Delete deployment set")
			for i := 0; i < len(depl); i++ {
				err := client.AppsV1().Deployments(namespace).Delete(ctx, depl[i].Name, *metav1.NewDeleteOptions(0))
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
			}
		}()

		ginkgo.By("Verify PVC Bound state and CNS side verification")
		pvs, err = checkVolumeStateAndPerformCnsVerification(ctx, client, pvclaims, "", "")
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		//List volume responses will show up in the interval of every 1 minute.
		time.Sleep(pollTimeoutShort)

		ginkgo.By("Validate ListVolume Response for all the volumes")
		logMessage := "List volume response: entries:"
		for i := 0; i < len(pvs); i++ {
			_, _, err = getCSIPodWhereListVolumeResponseIsPresent(ctx, client, sshClientConfig,
				containerName, logMessage, []string{pvs[i].Spec.CSI.VolumeHandle})
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}

		//List volume responses will show up in the interval of every 1 minute.
		time.Sleep(pollTimeoutShort)

		ginkgo.By("Validate pagination")
		logMessage = "token for next set: 3"
		_, _, err = getCSIPodWhereListVolumeResponseIsPresent(ctx, client, sshClientConfig, containerName, logMessage, nil)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		//List volume responses will show up in the interval of every 1 minute.
		//To see the empty response, It is required to wait for 1 min after deleteting all the PVC's
		time.Sleep(pollTimeoutShort)

		ginkgo.By("Validate ListVolume Response when no volumes are present")
		logMessage = "ListVolumes served 0 results"

		_, _, err = getCSIPodWhereListVolumeResponseIsPresent(ctx, client, sshClientConfig, containerName, logMessage, nil)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Scale up deployment1 pods to 5 replicas")
		replica = 3
		_, _, err = createVerifyAndScaleDeploymentPods(ctx, client, namespace, replica,
			true, labelsMap, pvclaim, nil, execRWXCommandPod, nginxImage, true, depl[0], 0)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Scale down deployment2 pods to 2 replica")
		replica = 2
		_, _, err = createVerifyAndScaleDeploymentPods(ctx, client, namespace, replica,
			true, labelsMap, pvclaim, nil, execRWXCommandPod, nginxImage, true, depl[1], 0)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		//List volume responses will show up in the interval of every 1 minute.
		time.Sleep(pollTimeoutShort)

		ginkgo.By("Validate pagination")
		logMessage = "token for next set: 3"
		_, _, err = getCSIPodWhereListVolumeResponseIsPresent(ctx, client, sshClientConfig, containerName, logMessage, nil)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		//List volume responses will show up in the interval of every 1 minute.
		//To see the empty response, It is required to wait for 1 min after deleteting all the PVC's
		time.Sleep(pollTimeoutShort)

		ginkgo.By("Validate ListVolume Response when no volumes are present")
		logMessage = "ListVolumes served 0 results"

		_, _, err = getCSIPodWhereListVolumeResponseIsPresent(ctx, client, sshClientConfig, containerName, logMessage, nil)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

	})

	// ********* RWX Topology MultiVC Config Secret usecases ***************

	/*
		Testcase-14
		Wrong entry in vsphere config secret, no csi driver restart
		VC IP wrong for VC1, wrong username for VC3
		SC → all allowed topology specify i.e. k8s:zone → zone-1/zone-2/zone-3 with Immediate Binding mode

		Steps:
		1. Create a SC with allowed topology shared across all 3 VC's with immediate binding mode.
		2. Create a dynamic PVC. Wait for PVC to reach Bound state.
		3. Volume provisioning can happen on any AZ.
		4. Create a deployment Pod with replica count 3 using the PVC created in step #2. Wait for Pod to reach running state.
		5. Pod placement can happen on any AZ.
		6. Update vsphere config secret by passing wrong VC IP for VC1 and wrong username for VC3 with no csi driver restart.
		7. Wait for the default time for CSI driver to pick up the new config secret changes.
		8. Create new dynamic rwx pvc after updating vsphere config secret.
		9. PVC should get stuck in a Pending state with an appropriate error message.
		10. Perform cleanup by deleting Pods, PVC and SC.
	*/

	ginkgo.It("[rwx-topology-multivc-configsecret] testconfig1", ginkgo.Label(p0, file, vanilla, multiVc, newTest), func() {

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		// deployment pod and pvc count
		replica = 3
		createPvcItr = 1
		createDepItr = 1

		ginkgo.By(fmt.Sprintf("Creating Storage Class with access mode %q and fstype %q with "+
			"no allowed topology specified", accessmode, nfs4FSType))
		storageclass1, pvclaims1, err := createStorageClassWithMultiplePVCs(client, namespace, labelsMap,
			scParameters, diskSize, nil, bindingModeImm, false, accessmode, "", nil, createPvcItr, false, false)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		defer func() {
			err := client.StorageV1().StorageClasses().Delete(ctx, storageclass1.Name, *metav1.NewDeleteOptions(0))
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}()

		ginkgo.By("Set node selector term for deployment pods so that all pods " +
			"should get created on one single AZ i.e. VC-3(zone-3)")
		/* here in 3-VC setup, deployment pod node affinity is taken as  k8s-zone -> zone-3
		i.e only VC1 allowed topology
		*/
		topValStartIndex = 2
		topValEndIndex = 3
		allowedTopologies = setSpecificAllowedTopology(allowedTopologies, topkeyStartIndex, topValStartIndex,
			topValEndIndex)
		nodeSelectorTerms, err := getNodeSelectorMapForDeploymentPods(allowedTopologies)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Verify PVC Bound state and CNS side verification")
		pvs1, err := checkVolumeStateAndPerformCnsVerification(ctx, client, pvclaims1, "", "")
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		// taking pvclaims1 0th index because we are creating only single RWX PVC in this case
		pvclaim1 := pvclaims1[0]
		pv1 := pvs1[0]

		defer func() {
			err := fpv.DeletePersistentVolumeClaim(ctx, client, pvclaim1.Name, namespace)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			err = multiVCe2eVSphere.waitForCNSVolumeToBeDeletedInMultiVC(pv1.Spec.CSI.VolumeHandle)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}()

		ginkgo.By("Create Deployment with replica count 3")
		deploymentList, _, err := createVerifyAndScaleDeploymentPods(ctx, client, namespace, replica, false, labelsMap,
			pvclaim1, nodeSelectorTerms, execRWXCommandPod, nginxImage, false, nil, createDepItr)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		defer func() {
			framework.Logf("Delete deployment set")
			err := client.AppsV1().Deployments(namespace).Delete(ctx, deploymentList[0].Name, *metav1.NewDeleteOptions(0))
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}()

		// read original vsphere config secret
		vsphereCfg, err := readVsphereConfSecret(client, ctx, csiNamespace)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Create vsphere-config-secret file with wrong VC IP in VC1 and wrong username in VC3")
		vCenterIPList := strings.Split(vsphereCfg.Global.VCenterHostname, ",")
		// set wrong vc ip in vc1
		vCenterIPList[0] = "10.10.10.10"
		vsphereCfg.Global.VCenterHostname = strings.Join(vCenterIPList, ",")

		// set wrong username in vc3
		userList := strings.Split(vsphereCfg.Global.User, ",")
		userList[2] = "testconfig@vsphere.local"
		vsphereCfg.Global.Password = strings.Join(userList, ",")

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

		_, pvclaims2, err := createStorageClassWithMultiplePVCs(client, namespace, labelsMap,
			scParameters, diskSize, nil, bindingModeImm, false, accessmode, "", nil, createPvcItr, true, false)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Expect claim to fail provisioning volume due to wrong entries in config secret")
		err = fpv.WaitForPersistentVolumeClaimPhase(ctx, v1.ClaimBound, client,
			pvclaims2[0].Namespace, pvclaims2[0].Name, framework.Poll, time.Minute/2)
		gomega.Expect(err).To(gomega.HaveOccurred())

		expectedErrMsg := "failed to create volume"
		ginkgo.By(fmt.Sprintf("Expected failure message: %+q", expectedErrMsg))
		isFailureFound := checkEventsforError(client, namespace,
			metav1.ListOptions{FieldSelector: fmt.Sprintf("involvedObject.name=%s", pvclaims2[0].Name)}, expectedErrMsg)
		gomega.Expect(isFailureFound).To(gomega.BeTrue(), "Unable to verify pvc create failure")
	})

	/*
		Testcase-15
		Net Permissions change in config secret
		ips = "*", permissions = "READ_WRITE" -> change to ->  ips = "*", permissions = "No_ACCESS"
		SC → specific allowed topology -> k8s-rack:rack-1 with Immediate Binding mode

		Steps:
		Please note - Initially the config secret is set with "*" and with permission "READ_WRITE"

		1. Create Storage Class. Set allowed topology to rack-1 with  Immediate Binding mode.
		2. Create PVC using Storage Class. Set access mode to "RWX"
		3. Verify PVC State. Ensure the PVC reaches the Bound state.
		4. Volume provisioning should happen on the specify allowed topology of rack-1
		5. Create 3 Pods using the PVC created in step #2. Make sure to set Pod3 to "read-only" mode.
		6. Verify Pod States. Confirm all 3 Pods are in an up and running state.
		7. Exec to Pod1: Create a text file and write data into the mounted directory.
		8. Exec to Pod2: Try to access the text file created by Pod1. Cat the file and later write
		some data into it. Verify that Pod2 can read and write into the text file.
		9. Exec to Pod3: Try to access the text file created by Pod1. Cat the file and attempt to write
		some data into it. Verify that Pod3 can read but cannot write due to "READ_ONLY" permission.
		10. Pod placement should happen on the worker node of the topology of SC.
		11. Update vsphere config secert cnd change net permisson of above to this "*" with "NO_ACCESS"
		12. Do not restart CSI driver and wait for CSI driver to pickup the new changes in config secret.
		13. Try creating new PVC. Expectation is that it should get stuck in Pending state with an
		appropriate error due to "NO_ACCESS" constraint.
		14. Now try restarting CSI driver.
		15. Create new PVC after csi driver restart.
		16. PVC should get stuck in Pending state with an appropriate error message.
		17. Perform Cleanup. Revert config secret to its original values, Delete Pods, PVC,
		and Storage Class created in the previous steps.
	*/

	ginkgo.It("[rwx-topology-multivc-configsecret] testconfig2", ginkgo.Label(p0, file, vanilla, multiVc, newTest), func() {

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		// pvc and pod count
		createPvcItr = 1
		createDepItr = 1
		replica = 1
		noPodsToDeploy = 3

		// here, we are considering allowed topology to be from AZ1
		topValStartIndex = 0
		topValEndIndex = 1

		ginkgo.By("Set allowed topology to AZ1 for storage class creation")
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
		pvs, err = checkVolumeStateAndPerformCnsVerification(ctx, client, pvclaims, "", "")
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		// taking pvclaims2 0th index because we are creating only single RWX PVC in this case
		pvclaim = pvclaims[0]
		pv = pvs[0]

		ginkgo.By("Verify volume placement, should match with the allowed topology specified")
		datastoreUrlsVc1, _, _, _, err := fetchDatastoreListMap(ctx, client)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		isCorrectPlacement := multiVCe2eVSphere.verifyPreferredDatastoreMatchInMultiVC(pv.Spec.CSI.VolumeHandle,
			datastoreUrlsVc1)
		gomega.Expect(isCorrectPlacement).To(gomega.BeTrue(), fmt.Sprintf("Volume provisioning has happened on the "+
			"wrong datastore. Expected 'true', got '%v'", isCorrectPlacement))

		ginkgo.By("Create 3 standalone Pods using the same PVC with different read/write permissions")
		podList, err := createStandalonePodsForRWXVolume(client, ctx, namespace, nil, pvclaim, false,
			execRWXCommandPod, noPodsToDeploy)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		defer func() {
			for i := 0; i < len(podList); i++ {
				ginkgo.By(fmt.Sprintf("Deleting the pod %s in namespace %s", podList[i].Name, namespace))
				err := fpod.DeletePodWithWait(ctx, client, podList[i])
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
			}
		}()
	})

	/*
		Testcase-16
		Net Permissions change in config secret
		ips = "10.20.20.0/24", permissions = "READ_WRITE" -> change to -> ips = "10.20.20.0/24", permissions = "READ_ONLY"
		SC → specific allowed topology i.e. k8s-rack:rack-2 with Immediate Binding mode

		Steps:
		Please note - Initially the config secret is set with "*" and with permission "READ_WRITE"

		1. Create Storage Class. Set allowed topology to rack-2 with  Immediate Binding mode.
		2. Create PVC using Storage Class. Set access mode to "RWX"
		3. Verify PVC State. Ensure the PVC reaches the Bound state.
		4. Volume provisioning should happen on the specify allowed topology of rack-2
		5. Create 3 Pods using the PVC created in step #2. Make sure to set Pod3 to "read-only" mode.
		6. Verify Pod States. Confirm all 3 Pods are in an up and running state.
		7. Exec to Pod1: Create a text file and write data into the mounted directory.
		8. Exec to Pod2: Try to access the text file created by Pod1. Cat the file and later
			write some data into it. Verify that Pod2 can read and write into the text file.
		9. Exec to Pod3: Try to access the text file created by Pod1. Cat the file and attempt to write some
			data into it. Verify that Pod3 can read but cannot write due to "READ_ONLY" permission.
		10. Pod placement should happen on the worker node of the topology of SC.
		11. Update vsphere config secert cnd change net permisson of above to this "*" with "READ_ONLY"
		12. Do not restart CSI driver and wait for CSI driver to pickup the new changes in config secret.
		13. Try creating new PVC. Expectation is that it should get stuck in Pending state with
			an appropriate error due to "READ_ONLY" constraint.
		14. Try accessing the files written on volume created in step #2. Verify the behaviour.
		15. Now try restarting CSI driver.
		16. Create new PVC after csi driver restart.
		17. PVC should get stuck in Pending state with an appropriate error message.
		18. Perform Cleanup. Revert config secret to its original values,
		19. Delete Pods, PVC, and Storage Class created in the previous steps.
	*/

	ginkgo.It("[rwx-topology-multivc-configsecret] testconfig3", ginkgo.Label(p0, file, vanilla, multiVc, newTest), func() {

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		// pvc and pod count
		createPvcItr = 1
		createDepItr = 1
		replica = 1
		noPodsToDeploy = 3

		// here, we are considering allowed topology to be from AZ2
		topValStartIndex = 1
		topValEndIndex = 2

		ginkgo.By("Set allowed topology to Az2 for storage class creation")
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
		pvs, err = checkVolumeStateAndPerformCnsVerification(ctx, client, pvclaims, "", "")
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		// taking pvclaims 0th index because we are creating only single RWX PVC in this case
		pvclaim = pvclaims[0]
		pv = pvs[0]

		ginkgo.By("Verify volume placement, should match with the allowed topology specified")
		_, datastoreUrlsVc2, _, _, err := fetchDatastoreListMap(ctx, client)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		isCorrectPlacement := multiVCe2eVSphere.verifyPreferredDatastoreMatchInMultiVC(pv.Spec.CSI.VolumeHandle,
			datastoreUrlsVc2)
		gomega.Expect(isCorrectPlacement).To(gomega.BeTrue(), fmt.Sprintf("Volume provisioning has happened on the "+
			"wrong datastore. Expected 'true', got '%v'", isCorrectPlacement))

		ginkgo.By("Create 3 standalone Pods using the same PVC with different read/write permissions")
		podList, err := createStandalonePodsForRWXVolume(client, ctx, namespace, nil, pvclaim, false,
			execRWXCommandPod, noPodsToDeploy)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		defer func() {
			for i := 0; i < len(podList); i++ {
				ginkgo.By(fmt.Sprintf("Deleting the pod %s in namespace %s", podList[i].Name, namespace))
				err := fpod.DeletePodWithWait(ctx, client, podList[i])
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
			}
		}()
	})

	/*
		Testcase-17
		VC Password change in VC-3 in VC UI but not updated in config secret
		SC → all allowed topology specified with Immediate Binding mode

		Steps:
		1. Create a SC with allowed topology shared across all 3 VC's.
		2. Create PVC with rwx access mode using SC created above.
		3. Verify that PVC reach to Bound state. PVC should get created on any AZ.
		4. Create standlone pods using pvc created above.
		4. Pods can be created on any AZ.
		5. Change VC-3 password in VC UI but do not update it in config secret.
		6. Perform scale-up operation. Increase the replica count to 10. Expectation is
			that scaling operation should go fine.
		7. Verify newly created PVCs and Pods status. PVC should reach to Bound state and Pod should
			reach to running state.
		8. Wait for sometime and later verify CSI Pods status. Check if CSI pods are in CLBO state
			or in running state.
		9. Revert the VC-3 password to its original value.
		10. Perform cleanup by deleting Pods, PVCs etc.
	*/

	ginkgo.It("[rwx-topology-multivc-configsecret] testconfig4", ginkgo.Label(p0, file, vanilla, multiVc, newTest), func() {

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		// pvc and pod count
		createPvcItr = 1
		createDepItr = 1
		replica = 1
		noPodsToDeploy = 3

		// here, we are considering all allowed topology from all AZs
		topValStartIndex = 0
		topValEndIndex = 2
		clientIndex := 2

		ginkgo.By("Set allowed topology to all AZs for storage class creation")
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
		pvs, err = checkVolumeStateAndPerformCnsVerification(ctx, client, pvclaims, "", "")
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		// taking pvclaims 0th index because we are creating only single RWX PVC in this case
		pvclaim = pvclaims[0]
		pv = pvs[0]

		ginkgo.By("Create 3 standalone Pods using the same PVC with different read/write permissions")
		podList, err := createStandalonePodsForRWXVolume(client, ctx, namespace, nil, pvclaim, false,
			execRWXCommandPod, noPodsToDeploy)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		defer func() {
			for i := 0; i < len(podList); i++ {
				ginkgo.By(fmt.Sprintf("Deleting the pod %s in namespace %s", podList[i].Name, namespace))
				err := fpod.DeletePodWithWait(ctx, client, podList[i])
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
			}
		}()

		// read original vsphere config secret
		vsphereCfg, err := readVsphereConfSecret(client, ctx, csiNamespace)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		vcAddress := strings.Split(vsphereCfg.Global.VCenterHostname, ",")[0] + ":" + sshdPort
		framework.Logf("vcAddress - %s ", vcAddress)
		username := strings.Split(vsphereCfg.Global.User, ",")[0]
		originalPassword := strings.Split(vsphereCfg.Global.Password, ",")[0]
		newPassword := e2eTestPassword
		ginkgo.By(fmt.Sprintf("Original password %s, new password %s", originalPassword, newPassword))

		ginkgo.By("Changing password on the vCenter Vc3 host")
		err = invokeVCenterChangePassword(ctx, username, originalPassword, newPassword, vcAddress, clientIndex)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		originalVc3PasswordChanged = true

		framework.Logf("Wait for %v for csi to auto-detect the config secret changes", pollTimeout)
		time.Sleep(pollTimeout)

		framework.Logf("Verify CSI Pods are in CLBO state")
		deploymentPods, err := client.AppsV1().Deployments(csiNamespace).Get(ctx, vSphereCSIControllerPodNamePrefix,
			metav1.GetOptions{})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		if deploymentPods.Status.UnavailableReplicas != 0 {
			framework.Logf("CSI Pods are in CLBO state")
		}

		_, pvclaims2, err := createStorageClassWithMultiplePVCs(client, namespace, labelsMap,
			scParameters, diskSize, nil, bindingModeImm, false, accessmode, "", nil, createPvcItr, true, false)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Expect claim to fail provisioning volume due to wrong entries in config secret")
		err = fpv.WaitForPersistentVolumeClaimPhase(ctx, v1.ClaimBound, client,
			pvclaims2[0].Namespace, pvclaims2[0].Name, framework.Poll, time.Minute/2)
		gomega.Expect(err).To(gomega.HaveOccurred())

		expectedErrMsg := "failed to create volume"
		ginkgo.By(fmt.Sprintf("Expected failure message: %+q", expectedErrMsg))
		isFailureFound := checkEventsforError(client, namespace,
			metav1.ListOptions{FieldSelector: fmt.Sprintf("involvedObject.name=%s", pvclaims2[0].Name)}, expectedErrMsg)
		gomega.Expect(isFailureFound).To(gomega.BeTrue(), "Unable to verify pvc create failure")
	})

	/* ********* RWX Topology MultiVC SiteDown usecases *************** */

	/*
		Testcase-18
		VC-3 has the workload up and running Now bring down all ESXis in vc-3 and
		try to scale up the StatefulSet
		SC → specific allowed topology i.e. k8s-zone:zone-1 and zone-2, Same Storage Policy name
		tagged to vSAN DS set in VC-1 and VC-2, Immediate Binding mode

		1. Create Storage Class with allowed topology set to zone-1 and zone-2 and pass Storage Policy,
		same storage policy name is available in VC-1 and VC-2.
		2. Create 4 StatefulSet with 5 replicas using Parallel Pod management policy.
		3. Wait for all the PVCs to reach Bound and Pods to reach Running state.
		4. Power off all the ESXi's in vc3.
		5. Scale up the StatefulSet. Increase the replica to 20.
		6. Confirm that the Pods created on VC-1 are in the Terminating state.
		7. Bring up 2 ESXi's in VC-1.
		8. Confirm that Kubernetes nodes in a disconnected state are migrating to the newly brought up ESXi hosts.
		9. Verify StatefulSet Pods status. Pods should come back to Running state which went to Terminating state.
		10. StatefulSet creation should take place on VC-1 and VC-2 now.
		11. Power on all the ESX's in VC-1.
		12. Confirm that the StatefulSet is scaled, and the replica count reaches 20.
		13. Check the status of Kubernetes nodes and workload Pods to ensure that all Pods are in an up and running state.
		14.  Perform cleanup by deleting StatefulSet Pods, PVC, and SC.
	*/

	ginkgo.It("[rwx-topology-multivc-sitedown] testsite1", ginkgo.Label(p0, file, vanilla, multiVc, newTest), func() {

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		// pvc and deployment pod count
		replica = 1
		stsReplicas := 5
		createPvcItr = 4
		createDepItr = 1
		noPodsToDeploy = 3
		stsCount := 4

		// storage policy
		storagePolicyName := GetAndExpectStringEnvVar(envStoragePolicyNameInVC1VC2)
		scParameters["storagepolicyname"] = storagePolicyName

		// here, we are considering allowed topology of Az1 and Az2
		topValStartIndex = 0
		topValEndIndex = 2

		ginkgo.By("Set all allowed topology of all VCs for storage class creation")
		allowedTopologiesForSC := setSpecificAllowedTopology(allowedTopologies, topkeyStartIndex, topValStartIndex,
			topValEndIndex)

		ginkgo.By(fmt.Sprintf("Creating Storage Class with access mode %q and fstype %q with"+
			"allowed topology specified to VC-1 (zone-1) and with WFFC binding mode", accessmode, nfs4FSType))
		scSpec := getVSphereStorageClassSpec(defaultNginxStorageClassName, scParameters, allowedTopologiesForSC, "",
			bindingModeImm, false)
		sc, err := client.StorageV1().StorageClasses().Create(ctx, scSpec, metav1.CreateOptions{})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		defer func() {
			err = client.StorageV1().StorageClasses().Delete(ctx, sc.Name, *metav1.NewDeleteOptions(0))
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}()

		ginkgo.By("Create 3 StatefulSet with replica count 5")
		statefulSets := createParallelStatefulSetSpec(namespace, stsCount, int32(stsReplicas))
		var wg sync.WaitGroup
		wg.Add(stsCount)
		for i := 0; i < len(statefulSets); i++ {
			go createParallelStatefulSets(client, namespace, statefulSets[i],
				int32(stsReplicas), &wg)

		}
		wg.Wait()

		ginkgo.By("Bring down ESXI hosts i.e. partial site down on vc3 multi-setup")
		// read testbed info json for VC3
		readVcEsxIpsViaTestbedInfoJson(GetAndExpectStringEnvVar(envTestbedInfoJsonPathVC2))

		// read cluster details in VC2
		clientIndex := 2
		clusterComputeResource, _, err = getClusterNameForMultiVC(ctx, &multiVCe2eVSphere, clientIndex)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		elements := strings.Split(clusterComputeResource[0].InventoryPath, "/")
		clusterName := elements[len(elements)-1]

		// read hosts in a cluster
		hostsInCluster := getHostsByClusterName(ctx, clusterComputeResource, clusterName)

		// power off esxi hosts
		noOfHostToBringDown := 3
		var powerOffHostsLists []string
		powerOffHostsList := powerOffEsxiHostsInMultiVcCluster(ctx, &multiVCe2eVSphere,
			noOfHostToBringDown, hostsInCluster)
		powerOffHostsLists = append(powerOffHostsLists, powerOffHostsList...)
		defer func() {
			ginkgo.By("Bring up  ESXi host which were powered off in VC2 multi-setup")
			for i := 0; i < len(powerOffHostsLists); i++ {
				powerOnEsxiHostByCluster(powerOffHostsLists[i])
			}

			ginkgo.By("Wait for k8s cluster to be healthy")
			wait4AllK8sNodesToBeUp(ctx, client, nodeList)
			err = waitForAllNodes2BeReady(ctx, client, pollTimeout*4)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}()

		ginkgo.By("K8s cluster will not be in a healthy state due to vc3 site down and worker nodes not in a healthy state")
		wait4AllK8sNodesToBeUp(ctx, client, nodeList)
		err = waitForAllNodes2BeReady(ctx, client, pollTimeout*4)
		gomega.Expect(err).To(gomega.HaveOccurred())

		ginkgo.By("Waiting for StatefulSets Pods to be in Ready State")
		err = waitForStsPodsToBeInReadyRunningState(ctx, client, namespace, statefulSets)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		defer func() {
			deleteAllStsAndPodsPVCsInNamespace(ctx, client, namespace)
		}()

		ginkgo.By("Bring up ESXi host which were powered off")
		for i := 0; i < len(powerOffHostsLists); i++ {
			powerOnEsxiHostByCluster(powerOffHostsLists[i])
		}

		ginkgo.By("Perform scaleup/scaledown operation on statefulsets")
		for i := 0; i < len(statefulSets); i++ {
			if i == 0 {
				stsScaleDown := false
				scaleUpReplicaCount := 7
				framework.Logf("Scale up StatefulSet1 replica count to 7")
				err = performScalingOnStatefulSetAndVerifyPvNodeAffinity(ctx, client, int32(scaleUpReplicaCount),
					0, statefulSets[i], true, namespace,
					allowedTopologies, false, stsScaleDown, false)
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
			}
			if i == 1 {
				stsScaleUp := false
				scaleDownReplicaCount := 5
				framework.Logf("Scale down StatefulSet2 replica count to 5")
				err = performScalingOnStatefulSetAndVerifyPvNodeAffinity(ctx, client, 0,
					int32(scaleDownReplicaCount), statefulSets[i], true, namespace,
					allowedTopologies, stsScaleUp, false, false)
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
			}
			if i == 2 {
				scaleUpReplicaCount := 12
				scaleDownReplicaCount := 9
				framework.Logf("Scale up StatefulSet3 replica count to 12 and later scale down replica " +
					"count  to 9")
				err = performScalingOnStatefulSetAndVerifyPvNodeAffinity(ctx, client, int32(scaleUpReplicaCount),
					int32(scaleDownReplicaCount), statefulSets[i], true, namespace,
					allowedTopologies, true, true, false)
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
			}
		}

	})

	/*
		Testcase-19
		VC-2 fully down and VC-3 partially down where vSAN FS is enabled in all 3 VC's
		SC → all allowed topology set i.e. zone-1,zone-2 and zone-3 with Immediate Binding mode

		1. Create a Storage Class with allowed topology set to all 3 VC's.
		2. Create 4 StatefulSet with 5 replicas using Parallel Pod management policy.
		3. While StatefulSet Pods are getting created, in between bring down all ESXI's of vc2 in parallel.
		4. Volume provisioning and Pods should get created on other 2 healthy VC's.
		5. Perform scaleup operation. Increase the replca count to 10.
		6. When scaleup operation is going on, bring down few esxi hosts of vc3 in parallel.
		7. Now vc2 is fully down and vc3 is partially down
		8. K8s nodes will transitioned to a not-responding state.
		9. Verify Scaling operation. Expectation is that the Scaleup operation should continue and new Pods,
		PVCs should get created on the healty available VC.
		10. Create few dynamic PVCs.
		11. Verify dynamic PVCs should reach to Bound state.
		12. Create few standalone Pods and Deployment Pods using PVC created in step #10.
		13. Verify newly created pvc and pod creation
		14. Bring up VC-2 and VC-3 all ESXi hosts.
		15. Check the status of Kubernetes nodes and workload Pods to ensure that all Pods are in an up and running state.
		16. Perform scaleup operation. Increase the replica count to 20.
		17. Verify scaling operation went smooth.
		18. PVCs and Pods should get distributed across all AZs.
		19. Scale down StatefulSet replica count to 3.
		20. Verify scaling down operation went smooth.
		21. Perform cleanup by deleting StatefulSet, Pods, PVC and SC.
	*/

	ginkgo.It("[rwx-topology-multivc-sitedown] Multiple site failures on a different multi vc setup", ginkgo.Label(p0, file, vanilla, multiVc, newTest), func() {

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		// variable declaration
		var depl []*appsv1.Deployment
		var powerOffHostsLists []string
		var podList []*v1.Pod

		// pvc and deployment pod count
		createPvcItr = 5
		createDepItr = 1
		noPodsToDeploy = 3
		stsReplicas := 5
		stsCount := 4

		// here, we are considering the allowed topology of all AZs
		topValStartIndex = 0
		topValEndIndex = 3

		ginkgo.By("Set all allowed topology of all VCs for storage class creation")
		allowedTopologiesForSC := setSpecificAllowedTopology(allowedTopologies, topkeyStartIndex, topValStartIndex,
			topValEndIndex)

		ginkgo.By(fmt.Sprintf("Creating Storage Class with access mode %q and fstype %q with "+
			"no allowed topology specified", accessmode, nfs4FSType))
		storageclass, pvclaims, err := createStorageClassWithMultiplePVCs(client, namespace, labelsMap,
			scParameters, diskSize, allowedTopologiesForSC, bindingModeImm, false, accessmode, defaultNginxStorageClassName,
			nil, createPvcItr, false, false)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		defer func() {
			err := client.StorageV1().StorageClasses().Delete(ctx, storageclass.Name, *metav1.NewDeleteOptions(0))
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}()
		defer func() {
			for i := 0; i < len(pvclaims); i++ {
				pv = getPvFromClaim(client, pvclaims[i].Namespace, pvclaims[i].Name)
				err := fpv.DeletePersistentVolumeClaim(ctx, client, pvclaims[i].Name, namespace)
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
				err = multiVCe2eVSphere.waitForCNSVolumeToBeDeletedInMultiVC(pv.Spec.CSI.VolumeHandle)
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
			}
		}()

		ginkgo.By("Create 4 StatefulSet with replica count 5")
		statefulSets := createParallelStatefulSetSpec(namespace, stsCount, int32(stsReplicas))
		var wg sync.WaitGroup
		wg.Add(stsCount)
		for i := 0; i < len(statefulSets); i++ {
			go createParallelStatefulSets(client, namespace, statefulSets[i],
				int32(stsReplicas), &wg)

		}
		wg.Wait()

		ginkgo.By("Bring down all ESXI hosts of vc2 multi-setup")
		// read testbed info json for VC2
		clientIndex := 1
		readVcEsxIpsViaTestbedInfoJson(GetAndExpectStringEnvVar(envTestbedInfoJsonPathVC2))

		// read cluster details in VC2
		clusterComputeResource, _, err = getClusterNameForMultiVC(ctx, &multiVCe2eVSphere, clientIndex)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		elements := strings.Split(clusterComputeResource[0].InventoryPath, "/")
		clusterName := elements[len(elements)-1]

		// read hosts in a cluster
		hostsInCluster := getHostsByClusterName(ctx, clusterComputeResource, clusterName)

		// power off all esxi hosts of vc2
		noOfHostToBringDown := 3
		powerOffHostsListVc2 := powerOffEsxiHostsInMultiVcCluster(ctx, &multiVCe2eVSphere,
			noOfHostToBringDown, hostsInCluster)
		powerOffHostsLists = append(powerOffHostsLists, powerOffHostsListVc2...)
		defer func() {
			ginkgo.By("Bring up  ESXi host which were powered off in VC2 multi-setup")
			for i := 0; i < len(powerOffHostsLists); i++ {
				powerOnEsxiHostByCluster(powerOffHostsLists[i])
			}

			ginkgo.By("Wait for k8s cluster to be healthy")
			wait4AllK8sNodesToBeUp(ctx, client, nodeList)
			err = waitForAllNodes2BeReady(ctx, client, pollTimeout*4)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())

		}()

		ginkgo.By("k8s cluster will not be in a healthy state because all the k8s nodes on vc2 will be in a not ready state")
		wait4AllK8sNodesToBeUp(ctx, client, nodeList)
		err = waitForAllNodes2BeReady(ctx, client, pollTimeout*4)
		gomega.Expect(err).To(gomega.HaveOccurred())

		ginkgo.By("Waiting for StatefulSets Pods to be in Ready State")
		err = waitForStsPodsToBeInReadyRunningState(ctx, client, namespace, statefulSets)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		defer func() {
			deleteAllStsAndPodsPVCsInNamespace(ctx, client, namespace)
		}()

		ginkgo.By("Perform scaleup operation on statefulsets")
		scaleUpReplicaCount := 10
		for i := 0; i > len(statefulSets); i++ {
			err = performScalingOnStatefulSetAndVerifyPvNodeAffinity(ctx, client, int32(scaleUpReplicaCount),
				0, statefulSets[i], true, namespace, allowedTopologiesForSC, true, false, false)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())

			if i == 3 {
				ginkgo.By("Partially bring down esxi hosts in vc3 multi-setup")
				// read testbed info json for Vc3
				clientIndex = 2
				readVcEsxIpsViaTestbedInfoJson(GetAndExpectStringEnvVar(envTestbedInfoJsonPathVC2))

				// read cluster details in vc3
				clusterComputeResource, _, err = getClusterNameForMultiVC(ctx, &multiVCe2eVSphere, clientIndex)
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
				elements = strings.Split(clusterComputeResource[0].InventoryPath, "/")
				clusterName = elements[len(elements)-1]

				// read hosts in a cluster
				hostsInCluster = getHostsByClusterName(ctx, clusterComputeResource, clusterName)

				// power off one esxi hosts of vc3
				noOfHostToBringDown = 1
				powerOffHostsListVc3 := powerOffEsxiHostsInMultiVcCluster(ctx, &multiVCe2eVSphere,
					noOfHostToBringDown, hostsInCluster)
				powerOffHostsLists = append(powerOffHostsLists, powerOffHostsListVc3...)
			}
		}
		defer func() {
			ginkgo.By("Bring up  ESXi host which were powered off in Vc3 multi-setup")
			for i := 0; i < len(powerOffHostsLists); i++ {
				powerOnEsxiHostByCluster(powerOffHostsLists[i])
			}

			// k8s cluster should come back to healthy state once all esxi hosts are restored
			ginkgo.By("Wait for k8s cluster to be healthy")
			wait4AllK8sNodesToBeUp(ctx, client, nodeList)
			err = waitForAllNodes2BeReady(ctx, client, pollTimeout*4)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}()

		ginkgo.By("k8s cluster will not be in a healthy state because k8s nodes in vc2 nd vc3 will be in a not responding state")
		wait4AllK8sNodesToBeUp(ctx, client, nodeList)
		err = waitForAllNodes2BeReady(ctx, client, pollTimeout*4)
		gomega.Expect(err).To(gomega.HaveOccurred())

		ginkgo.By("Creating few pvcs keeping rwx access mode")
		_, pvclaims, err = createStorageClassWithMultiplePVCs(client, namespace, labelsMap,
			scParameters, diskSize, nil, bindingModeImm, false, accessmode, "", nil, createPvcItr, false, false)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		defer func() {
			for i := 0; i < len(pvclaims); i++ {
				pv = getPvFromClaim(client, pvclaims[i].Namespace, pvclaims[i].Name)
				err := fpv.DeletePersistentVolumeClaim(ctx, client, pvclaims[i].Name, namespace)
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
				err = multiVCe2eVSphere.waitForCNSVolumeToBeDeletedInMultiVC(pv.Spec.CSI.VolumeHandle)
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
			}
		}()

		ginkgo.By("Create Deployments and standalone pods")
		for i := 0; i < len(pvclaims); i++ {
			deploymentList, _, err := createVerifyAndScaleDeploymentPods(ctx, client, namespace, replica, false, labelsMap,
				pvclaims[i], nil, execRWXCommandPod, nginxImage, false, nil, createDepItr)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			depl = append(depl, deploymentList...)

			// Create 3 standalone pods and attach it to 3rd rwx pvc
			if i == 2 {
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

		ginkgo.By("Bring up ESXi host which were powered off")
		for i := 0; i < len(powerOffHostsLists); i++ {
			powerOnEsxiHostByCluster(powerOffHostsLists[i])
		}

		ginkgo.By("Wait for k8s cluster to be healthy")
		wait4AllK8sNodesToBeUp(ctx, client, nodeList)
		err = waitForAllNodes2BeReady(ctx, client, pollTimeout*4)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Perform scaleup operation on statefulsets")
		scaleUpReplicaCount = 20
		for i := 0; i < len(statefulSets); i++ {
			err = performScalingOnStatefulSetAndVerifyPvNodeAffinity(ctx, client, int32(scaleUpReplicaCount),
				0, statefulSets[i], false, namespace, allowedTopologiesForSC, true, false, false)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}

		ginkgo.By("Perform scaledown operation on statefulsets")
		scaleDownReplicaCount := 3
		for i := 0; i < len(statefulSets); i++ {
			err = performScalingOnStatefulSetAndVerifyPvNodeAffinity(ctx, client, 0,
				int32(scaleDownReplicaCount), statefulSets[i], false, namespace, allowedTopologiesForSC, true, false, false)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}
	})

	/* ********* RWX Topology MultiVC multi replica and operation storm usecase *************** */

	/*
		Testcase-20
		VC1 → 1 host down , VC2 → fully down, VC3 → up and running where vSAN FS is enabled in all 3 VCs
		Workload Pod creation in scale + Multiple site down (partial/full) + kill CSI provisioner and CSi-attacher +
		dynamic Pod/PVC creation + vSAN-Health service down

		SC → specific allowed topology set i.e. zone-1,zone-2 and zone-3 with Immediate Binding mode

		Steps:
		1. Identify the CSI-Controller-Pod where CSI Provisioner, CSI Attacher and vsphere-Syncer are the leader
		2. Create Storage class with allowed topology set to all levels of
		AZ i.e. on vc-1, vc-2 and vc-3 with Immediate binding mode.
		3. Create 5 dynamic PVCs with "RWX" acces mode. While volume creation is in progress,
		kill CSI-Provisioner identified in step #1
		4. Create 5 deployment Pods with 3 replicas each
		5. While pod creation is in progress, kill CSI-attacher identified in step #1
		6. Wait for PVCs to reach Bound state and Pods to reach running state.
		7. Volume provisioning and pod placement should happen on any of the AZ.
		8. Bring down one esxi host in vc-3
		9. Verify k8s node status and CSI Pods status.
		11. Try to perform scaleup operation on deployment (dep1 and dep2). Incrase the replica count from 3 to 7
		12. Verify Scaling operation. It should succeed and volumes and Pods should get created.
		13. Try reading/writing into the volume from each deployment Pod
		14. Now bring down all esxi hosts of VC-2.
		15. Verify k8s node status and CSI Pods status.
		16. Try to perform scaleup operation on dep1 . Incrase the replica count from 7 to 10.
		17. Verify Scaling operation. It should succeed and volumes and Pods should get created.
		18. Create few dynamic PVC's with labels added for PV and PVC with "RWX" access mode
		19. Verify newly created PVC creation status. It should reach to Bound state.
		20. Stop vSAN-health service.
		21. Update labels for PV and PVC.
		22. Start vSAN-health service.
		23. Create standalone Pods using the PVC created in step #13.
		24. Verify Pods status. PVC should reach to Bound state and Pods should reach to running state.
		25. Create StatefulSet with 3 replica
		26. Verify PVC and Pod should be in Bound and Running state.
		27. Bring back all the ESXI hosts in VC-1 and VC-2.
		28. Wait for system to be back to normal.
		29. Verify CSI Pods status and K8s nodes status.
		30. Perform scale-up/scale-down operation on statefulset created in step #24
		31. Verify scaling operation went successful
		32. Verify CNS metadata for the PVCs for which labels were updated.
		33. Perform cleanup by deleting StatefulSets, Pods, PVCs and SC.
	*/

	ginkgo.It("[rwx-topology-multivc-operationstorm] Scale workload creation and in parallel sitedown on multi Vcs", ginkgo.Label(p0, file, vanilla, multiVc, newTest), func() {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		// variable declaration
		var depl []*appsv1.Deployment
		var powerOffHostsLists []string
		var podList []*v1.Pod

		// pvc and deployment pod count
		createPvcItr = 5
		createDepItr = 1
		noPodsToDeploy = 3
		stsReplicas := 3

		ginkgo.By("Get current leader where CSI-Provisioner, CSI-Attacher and " +
			"Vsphere-Syncer is running and find the master node IP where these containers are running")
		csiProvisionerLeader, csiProvisionerControlIp, err := getK8sMasterNodeIPWhereContainerLeaderIsRunning(ctx,
			client, sshClientConfig, provisionerContainerName)
		framework.Logf("CSI-Provisioner is running on Leader Pod %s "+
			"which is running on master node %s", csiProvisionerLeader, csiProvisionerControlIp)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		csiAttacherLeaderleader, csiAttacherControlIp, err := getK8sMasterNodeIPWhereContainerLeaderIsRunning(ctx,
			client, sshClientConfig, attacherContainerName)
		framework.Logf("CSI-Attacher is running on Leader Pod %s "+
			"which is running on master node %s", csiAttacherLeaderleader, csiAttacherControlIp)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		vsphereSyncerLeader, vsphereSyncerControlIp, err := getK8sMasterNodeIPWhereContainerLeaderIsRunning(ctx,
			client, sshClientConfig, syncerContainerName)
		framework.Logf("Vsphere-Syncer is running on Leader Pod %s "+
			"which is running on master node %s", vsphereSyncerLeader, vsphereSyncerControlIp)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		// here, we are considering the allowed topology of all AZs
		topValStartIndex = 0
		topValEndIndex = 3

		ginkgo.By("Set all allowed topology of all VCs for storage class creation")
		allowedTopologiesForSC := setSpecificAllowedTopology(allowedTopologies, topkeyStartIndex, topValStartIndex,
			topValEndIndex)

		ginkgo.By(fmt.Sprintf("Creating Storage Class with access mode %q and fstype %q with "+
			"no allowed topology specified", accessmode, nfs4FSType))
		storageclass, pvclaims, err := createStorageClassWithMultiplePVCs(client, namespace, labelsMap,
			scParameters, diskSize, allowedTopologiesForSC, bindingModeImm, false, accessmode, defaultNginxStorageClassName,
			nil, createPvcItr, false, false)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		defer func() {
			err := client.StorageV1().StorageClasses().Delete(ctx, storageclass.Name, *metav1.NewDeleteOptions(0))
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}()
		defer func() {
			for i := 0; i < len(pvclaims); i++ {
				pv = getPvFromClaim(client, pvclaims[i].Namespace, pvclaims[i].Name)
				err := fpv.DeletePersistentVolumeClaim(ctx, client, pvclaims[i].Name, namespace)
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
				err = multiVCe2eVSphere.waitForCNSVolumeToBeDeletedInMultiVC(pv.Spec.CSI.VolumeHandle)
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
			}
		}()

		ginkgo.By("Kill CSI-Provisioner container while pvc creation is in progress")
		err = execDockerPauseNKillOnContainer(sshClientConfig, csiProvisionerControlIp, provisionerContainerName,
			k8sVersion)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Verify PVC Bound state and CNS side verification")
		pvs, err = checkVolumeStateAndPerformCnsVerification(ctx, client, pvclaims, "", "")
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Create deployment with replica count 5 and attached it to each RWX PVC created")
		for i := 0; i < len(pvclaims); i++ {
			deployment, err := createDeployment(ctx, client, int32(replica), labelsMap, nil, namespace,
				[]*v1.PersistentVolumeClaim{pvclaims[i]}, execRWXCommandPod, false, nginxImage)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			depl = append(depl, deployment)

			if i == 3 {
				ginkgo.By("Kill CSI-Attacher container while deployment pod creation is in progress")
				err = execDockerPauseNKillOnContainer(sshClientConfig, csiAttacherControlIp, attacherContainerName,
					k8sVersion)
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
			}
		}

		ginkgo.By("Verify deployment pods status")
		for i := 0; i < len(depl); i++ {
			pods, err := fdep.GetPodsForDeployment(ctx, client, depl[i])
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			pod := pods.Items[0]
			err = fpod.WaitForPodNameRunningInNamespace(ctx, client, pod.Name, namespace)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}
		defer func() {
			for i := 0; i < len(depl); i++ {
				framework.Logf("Delete deployment set")
				err := client.AppsV1().Deployments(namespace).Delete(ctx, depl[i].Name, *metav1.NewDeleteOptions(0))
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
			}
		}()

		ginkgo.By("Bring down ESXI hosts i.e. partial site down on VC3 multi-setup")
		// read testbed info json for vc3
		readVcEsxIpsViaTestbedInfoJson(GetAndExpectStringEnvVar(envTestbedInfoJsonPathVC3))

		// read cluster details in VC2
		clientIndex := 1
		clusterComputeResource, _, err = getClusterNameForMultiVC(ctx, &multiVCe2eVSphere, clientIndex)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		elements := strings.Split(clusterComputeResource[0].InventoryPath, "/")
		clusterName := elements[len(elements)-1]

		// read hosts in a cluster
		hostsInCluster := getHostsByClusterName(ctx, clusterComputeResource, clusterName)

		// power off esxi hosts
		noOfHostToBringDown := 1
		powerOffHostsListVc2 := powerOffEsxiHostsInMultiVcCluster(ctx, &multiVCe2eVSphere,
			noOfHostToBringDown, hostsInCluster)
		powerOffHostsLists = append(powerOffHostsLists, powerOffHostsListVc2...)
		defer func() {
			ginkgo.By("Bring up  ESXi host which were powered off in VC2 multi-setup")
			for i := 0; i < len(powerOffHostsLists); i++ {
				powerOnEsxiHostByCluster(powerOffHostsLists[i])
			}

			ginkgo.By("Wait for k8s cluster to be healthy")
			wait4AllK8sNodesToBeUp(ctx, client, nodeList)
			err = waitForAllNodes2BeReady(ctx, client, pollTimeout*4)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())

		}()

		ginkgo.By("k8s cluster will not be in a healthy state due to vc2 k8s worker nodes being in not responsing state")
		wait4AllK8sNodesToBeUp(ctx, client, nodeList)
		err = waitForAllNodes2BeReady(ctx, client, pollTimeout*4)
		gomega.Expect(err).To(gomega.HaveOccurred())

		ginkgo.By("Scale up deployment1 and deployment2 to 7 replica pods")
		replica = 7
		/* here createDepItr value is set to 0, because we are performing scaleup operation
		and not creating any new deployment */
		for i := 0; i < len(depl)-3; i++ {
			_, _, err := createVerifyAndScaleDeploymentPods(ctx, client, namespace, replica, true,
				labelsMap, pvclaims[i], nil, execRWXCommandPod, nginxImage, true, depl[i], 0)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}

		ginkgo.By("Bring down ESXI hosts i.e. partial site down on VC3 multi-setup")
		// read testbed info json for vc3
		readVcEsxIpsViaTestbedInfoJson(GetAndExpectStringEnvVar(envTestbedInfoJsonPathVC3))

		// read cluster details in VC3
		clientIndex = 2
		clusterComputeResource, _, err = getClusterNameForMultiVC(ctx, &multiVCe2eVSphere, clientIndex)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		elements = strings.Split(clusterComputeResource[0].InventoryPath, "/")
		clusterName = elements[len(elements)-1]

		// read hosts in a cluster
		hostsInCluster = getHostsByClusterName(ctx, clusterComputeResource, clusterName)

		// power off esxi hosts
		noOfHostToBringDown = 3
		powerOffHostsListVc3 := powerOffEsxiHostsInMultiVcCluster(ctx, &multiVCe2eVSphere,
			noOfHostToBringDown, hostsInCluster)
		powerOffHostsLists = append(powerOffHostsLists, powerOffHostsListVc3...)
		defer func() {
			ginkgo.By("Bring up  ESXi host which were powered off in VC3 multi-setup")
			for i := 0; i < len(powerOffHostsLists); i++ {
				powerOnEsxiHostByCluster(powerOffHostsLists[i])
			}

			ginkgo.By("Wait for k8s cluster to be healthy")
			wait4AllK8sNodesToBeUp(ctx, client, nodeList)
			err = waitForAllNodes2BeReady(ctx, client, pollTimeout*4)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())

		}()

		// all the nodes residing on vc-3 will be down, so k8s cluster will not be healthy
		ginkgo.By("Wait for k8s cluster to be healthy")
		wait4AllK8sNodesToBeUp(ctx, client, nodeList)
		err = waitForAllNodes2BeReady(ctx, client, pollTimeout*4)
		gomega.Expect(err).To(gomega.HaveOccurred())

		ginkgo.By("Scale up deployment1 to 10 replica pods")
		replica = 10
		_, _, err = createVerifyAndScaleDeploymentPods(ctx, client, namespace, replica, true,
			labelsMap, pvclaims[0], nil, execRWXCommandPod, nginxImage, true, depl[0], 0)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Creating new rwx dynamic pvcs when vc-2 all hosts are down")
		createPvcItr = 5
		_, pvclaimsNew, err := createStorageClassWithMultiplePVCs(client, namespace, labelsMap,
			scParameters, diskSize, allowedTopologiesForSC, bindingModeImm, false, accessmode, "",
			storageclass, createPvcItr, true, false)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Verify PVC Bound state and CNS side verification for newly created RWX PVCs")
		_, err = checkVolumeStateAndPerformCnsVerification(ctx, client, pvclaimsNew, "", "")
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		defer func() {
			for i := 0; i < len(pvclaimsNew); i++ {
				pv = getPvFromClaim(client, pvclaimsNew[i].Namespace, pvclaimsNew[i].Name)
				err := fpv.DeletePersistentVolumeClaim(ctx, client, pvclaimsNew[i].Name, namespace)
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
				err = multiVCe2eVSphere.waitForCNSVolumeToBeDeletedInMultiVC(pv.Spec.CSI.VolumeHandle)
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
			}
		}()

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

		ginkgo.By("Create 3 standalone Pods using the same PVC with different read/write permissions")
		for i := 0; i < len(pvclaimsNew); i++ {
			pods, err := createStandalonePodsForRWXVolume(client, ctx, namespace, nil, pvclaimsNew[i], false,
				execRWXCommandPod, noPodsToDeploy)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			podList = append(podList, pods...)
		}
		defer func() {
			for i := 0; i < len(podList); i++ {
				ginkgo.By(fmt.Sprintf("Deleting the pod %s in namespace %s", podList[i].Name, namespace))
				err := fpod.DeletePodWithWait(ctx, client, podList[i])
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
			}
		}()

		ginkgo.By("Create StatefulSet with 20 replica pods. Each statefulset pod is attached to RWX PVC")
		service, statefulset, err := createStafeulSetAndVerifyPVAndPodNodeAffinty(ctx, client, namespace, true,
			int32(stsReplicas), false, allowedTopologiesForSC, false, false, false, accessmode, storageclass, false)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		defer func() {
			fss.DeleteAllStatefulSets(ctx, client, namespace)
			deleteService(namespace, client, service)
		}()

		ginkgo.By("Bring up ESXi host which were powered off")
		for i := 0; i < len(powerOffHostsLists); i++ {
			powerOnEsxiHostByCluster(powerOffHostsLists[i])
		}

		ginkgo.By("Wait for k8s cluster to be healthy")
		wait4AllK8sNodesToBeUp(ctx, client, nodeList)
		err = waitForAllNodes2BeReady(ctx, client, pollTimeout*4)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Perform scaleup/scaledown operation on statefulsets")
		scaleUpReplicaCount := 5
		scaleDownReplicaCount := 1
		err = performScalingOnStatefulSetAndVerifyPvNodeAffinity(ctx, client, int32(scaleUpReplicaCount),
			int32(scaleDownReplicaCount), statefulset, false, namespace, allowedTopologiesForSC, true, true, false)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
	})
})
