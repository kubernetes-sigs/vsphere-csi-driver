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

	ginkgo "github.com/onsi/ginkgo/v2"
	"github.com/onsi/gomega"
	v1 "k8s.io/api/core/v1"
	storagev1 "k8s.io/api/storage/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	clientset "k8s.io/client-go/kubernetes"
	"k8s.io/kubernetes/test/e2e/framework"
	fnodes "k8s.io/kubernetes/test/e2e/framework/node"
	fpod "k8s.io/kubernetes/test/e2e/framework/pod"
	fpv "k8s.io/kubernetes/test/e2e/framework/pv"
	fss "k8s.io/kubernetes/test/e2e/framework/statefulset"
	admissionapi "k8s.io/pod-security-admission/api"
)

var _ = ginkgo.Describe("[csi-multi-vc-topology] Multi-VC", func() {
	f := framework.NewDefaultFramework("multi-vc")
	f.NamespacePodSecurityEnforceLevel = admissionapi.LevelPrivileged
	var (
		client    clientset.Interface
		namespace string

		allowedTopologies []v1.TopologySelectorLabelRequirement
		topValStartIndex  int
		topValEndIndex    int
		topkeyStartIndex  int
		scParameters      map[string]string
		bindingModeWffc   storagev1.VolumeBindingMode
		bindingModeImm    storagev1.VolumeBindingMode
		accessmode        v1.PersistentVolumeAccessMode
		labelsMap         map[string]string
		replica           int32
		pvcItr            int
		pvs               []*v1.PersistentVolume
		pvclaim           *v1.PersistentVolumeClaim
		pv                *v1.PersistentVolume
	)

	ginkgo.BeforeEach(func() {

		client = f.ClientSet
		namespace = f.Namespace.Name

		// connecting to multiple VCs
		multiVCbootstrap()

		// fetch list f k8s nodes
		nodeList, err := fnodes.GetReadySchedulableNodes(f.ClientSet)
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

	ginkgo.It("TC1", ginkgo.Label(p0, file, vanilla, level5, level2, newTest), func() {

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		// variable declaration
		var pvs1, pvs2 []*v1.PersistentVolume
		var pvclaim1, pvclaim2 *v1.PersistentVolumeClaim
		var pv1, pv2 *v1.PersistentVolume

		// deployment pod and pvc count
		replica = 3
		pvcItr = 1

		ginkgo.By(fmt.Sprintf("Creating Storage Class with access mode %q and fstype %q with "+
			"no allowed topology specified", accessmode, nfs4FSType))
		storageclass1, pvclaims1, err := createStorageClassWithMultiplePVCs(client, namespace, labelsMap,
			scParameters, diskSize, nil, bindingModeWffc, false, accessmode, "", pvcItr, false, false)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		defer func() {
			err := client.StorageV1().StorageClasses().Delete(ctx, storageclass1.Name, *metav1.NewDeleteOptions(0))
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}()
		defer func() {
			err := fpv.DeletePersistentVolumeClaim(client, pvclaim1.Name, namespace)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			err = multiVCe2eVSphere.waitForCNSVolumeToBeDeletedInMultiVC(pv1.Spec.CSI.VolumeHandle)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}()

		ginkgo.By("Set node selector term for deployment pods so that all pods should get created on one single AZ i.e. VC-1(zone-1)")
		/* here in 3-VC setup, deployment pod node affinity is taken as  k8s-zone -> zone-1
		i.e only VC1 allowed topology
		*/
		topValStartIndex = 0
		topValEndIndex = 1
		allowedTopologies = setSpecificAllowedTopology(allowedTopologies, topkeyStartIndex, topValStartIndex,
			topValEndIndex)
		nodeSelectorTerms := getNodeSelectorMapForDeploymentPods(allowedTopologies)

		// taking pvclaims1 0th index because we are creating only single RWX PVC in this case
		pvclaim1 = pvclaims1[0]

		ginkgo.By("Create Deployment with replica count 3")
		deployment, _, err := createVerifyAndScaleDeploymentPods(ctx, client, namespace, replica, false, labelsMap,
			pvclaim1, nodeSelectorTerms, false, nil)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		defer func() {
			framework.Logf("Delete deployment set")
			err := client.AppsV1().Deployments(namespace).Delete(ctx, deployment.Name, *metav1.NewDeleteOptions(0))
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}()

		ginkgo.By("Verify PVC Bound state and CNS side verification")
		pvs1, err = checkVolumeStateAndPerformCnsVerification(client, pvclaims1, "", "")
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		// taking pv1 0th index because we are creating only single RWX PVC in this case
		pv1 = pvs1[0]

		// standalone pod and pvc count
		noPodsToDeploy := 3
		pvcItr = 1

		// here considering all 3 VCs so start index is set to 0 and endIndex will be set to 3rd VC value
		topValStartIndex = 0
		topValEndIndex = 3

		ginkgo.By("Set allowed topology to all 3 VCs")
		allowedTopologyForSC := setSpecificAllowedTopology(allowedTopologies, topkeyStartIndex, topValStartIndex,
			topValEndIndex)

		ginkgo.By(fmt.Sprintf("Creating Storage Class with access mode %q and fstype %q with allowed "+
			"topology set to all topology levels", accessmode, nfs4FSType))
		storageclass2, pvclaims2, err := createStorageClassWithMultiplePVCs(client, namespace, labelsMap, scParameters, diskSize,
			allowedTopologyForSC, bindingModeImm, false, accessmode, "", 1, false, false)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		defer func() {
			err := client.StorageV1().StorageClasses().Delete(ctx, storageclass2.Name, *metav1.NewDeleteOptions(0))
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}()

		defer func() {
			err := fpv.DeletePersistentVolumeClaim(client, pvclaim2.Name, namespace)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			err = multiVCe2eVSphere.waitForCNSVolumeToBeDeletedInMultiVC(pv2.Spec.CSI.VolumeHandle)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}()

		ginkgo.By("Verify PVC Bound state and CNS side verification")
		pvs2, err = checkVolumeStateAndPerformCnsVerification(client, pvclaims2, "", "")
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		// taking pvclaims2 0th index because we are creating only single RWX PVC in this case
		pvclaim2 = pvclaims2[0]
		pv2 = pvs2[0]

		ginkgo.By("Create 3 standalone Pods using the same PVC with different read/write permissions")
		podList, err := createStandalonePodsForRWXVolume(client, ctx, namespace, nil, pvclaim2, false, execRWXCommandPod, noPodsToDeploy)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		defer func() {
			for i := 0; i < len(podList); i++ {
				ginkgo.By(fmt.Sprintf("Deleting the pod %s in namespace %s", podList[i].Name, namespace))
				err := fpod.DeletePodWithWait(client, podList[i])
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
			}
		}()
		ginkgo.By("Scale up deployment to 6 replica")
		replica = 6
		deployment, pods, err := createVerifyAndScaleDeploymentPods(ctx, client, namespace, replica, true,
			labelsMap, pvclaim1, nodeSelectorTerms, true, deployment)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Verify deployment pods are scheduled on a specified node selector terms")
		err = verifyDeploymentPodNodeAffinity(ctx, client, namespace, allowedTopologies, pods, pv1)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Scale down deployment to 1 replica")
		replica = 1
		deployment, _, err = createVerifyAndScaleDeploymentPods(ctx, client, namespace, replica, true, labelsMap,
			pvclaim1, nodeSelectorTerms, true, deployment)
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
		8. Volume provisioning should happen on the AZ as specified in the SC but Pod placement can occur in any availability zones
		(AZs) during the scale-up operation.
		9. Perform cleanup by deleting StatefulSets, PVCs, and the StorageClass (SC)
	*/

	ginkgo.It("TC2", ginkgo.Label(p0, file, vanilla, level5, level2, newTest), func() {

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
		scSpec := getVSphereStorageClassSpec(defaultNginxStorageClassName, scParameters, allowedTopologyForSC, "", bindingModeWffc, false)
		sc, err := client.StorageV1().StorageClasses().Create(ctx, scSpec, metav1.CreateOptions{})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		defer func() {
			err = client.StorageV1().StorageClasses().Delete(ctx, sc.Name, *metav1.NewDeleteOptions(0))
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}()

		ginkgo.By("Create StatefulSet with replica count 3 with RWX PVC access mode")
		service, statefulset, err := createStafeulSetAndVerifyPVAndPodNodeAffinty(ctx, client, namespace, false,
			int32(stsReplicas), false, nil, 0, false, false, false, "", accessmode, sc, false)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		defer func() {
			fss.DeleteAllStatefulSets(client, namespace)
			deleteService(namespace, client, service)
		}()

		/*Storage poloicy of vc-1 is passed in storage class creation, so fetching the list of datastores
		which is avilable in VC-1, volume provision should happen on VC-1 vSAN FS compatible datastore */
		datastoreUrlsRack1, _, _, _, err := fetchDatastoreListMap(ctx, client)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Verify volume placement, should match with the allowed topology specified")
		err = volumePlacementVerificationForSts(ctx, client, statefulset, namespace, datastoreUrlsRack1)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Perform scaleup/scaledown operation on statefulsets")
		err = performScalingOnStatefulSetAndVerifyPvNodeAffinity(ctx, client, int32(scaleUpReplicaCount), int32(scaleDownReplicaCount),
			statefulset, false, namespace, nil, true, true, false)
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

	ginkgo.It("TC3", ginkgo.Label(p0, file, vanilla, level5, level2, newTest), func() {

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		// deployment pod and pvc count
		replica = 2
		pvcItr = 1
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
			scParameters, diskSize, allowedTopologyForSC, bindingModeImm, false, accessmode, "", pvcItr, false, false)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		defer func() {
			err := client.StorageV1().StorageClasses().Delete(ctx, storageclass.Name, *metav1.NewDeleteOptions(0))
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}()
		defer func() {
			err := fpv.DeletePersistentVolumeClaim(client, pvclaim.Name, namespace)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			err = multiVCe2eVSphere.waitForCNSVolumeToBeDeletedInMultiVC(pv.Spec.CSI.VolumeHandle)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}()

		ginkgo.By("Verify PVC Bound state and CNS side verification")
		pvs, err = checkVolumeStateAndPerformCnsVerification(client, pvclaims, storagePolicyInVc1Vc2, "")
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
		isCorrectPlacement := multiVCe2eVSphere.verifyPreferredDatastoreMatchInMultiVC(pv.Spec.CSI.VolumeHandle, datastoreUrlsVc1Vc2)
		gomega.Expect(isCorrectPlacement).To(gomega.BeTrue(), fmt.Sprintf("Volume provisioning has happened on the wrong "+
			"datastore. Expected 'true', got '%v'", isCorrectPlacement))

		ginkgo.By("Create Deployment with replica count 2")
		deployment, _, err := createVerifyAndScaleDeploymentPods(ctx, client, namespace, replica, false, labelsMap,
			pvclaim, nil, false, nil)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		defer func() {
			framework.Logf("Delete deployment set")
			err := client.AppsV1().Deployments(namespace).Delete(ctx, deployment.Name, *metav1.NewDeleteOptions(0))
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}()

		ginkgo.By("Scale up deployment to 5 replica")
		replica = 5
		deployment, _, err = createVerifyAndScaleDeploymentPods(ctx, client, namespace, replica, true, labelsMap,
			pvclaim, nil, true, deployment)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Scale down deployment to 1 replica")
		replica = 1
		deployment, pods, err := createVerifyAndScaleDeploymentPods(ctx, client, namespace, replica, true, labelsMap, pvclaim, nil, true, deployment)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Verify volume metadata for deployment pod, pvc and pv")
		err = waitAndVerifyCnsVolumeMetadata(pv.Spec.CSI.VolumeHandle, pvclaim, pv, &pods.Items[0])
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

	ginkgo.It("TC4", ginkgo.Label(p0, file, vanilla, level5, level2, newTest), func() {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		// deployment pod and pvc count
		replica = 2
		pvcItr = 1
		storagePolicyInVc1Vc2 := GetAndExpectStringEnvVar(envStoragePolicyNameInVC1VC2)
		scParameters[scParamStoragePolicyName] = storagePolicyInVc1Vc2

		/*
			We are considering allowed topology of VC-2 i.e.
			k8s-zone -> zone-2 and datastore url of VC-2
		*/
		topValStartIndex = 1
		topValEndIndex = 2

		// vSAN FS compatible datastore of VC-2
		dsUrl := GetAndExpectStringEnvVar(envSharedDatastoreURLVC2)
		scParameters["datastoreurl"] = dsUrl

		ginkgo.By("Set allowed topology to VC-2")
		allowedTopologyForSC := setSpecificAllowedTopology(allowedTopologies, topkeyStartIndex, topValStartIndex,
			topValEndIndex)

		ginkgo.By(fmt.Sprintf("Creating Storage Class with access mode %q and fstype %q with "+
			"allowed topology set to VC-2 AZ ", accessmode, nfs4FSType))
		storageclass, pvclaims, err := createStorageClassWithMultiplePVCs(client, namespace, labelsMap,
			scParameters, diskSize, allowedTopologyForSC, bindingModeWffc, false, accessmode, "", pvcItr, false, false)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		defer func() {
			err := client.StorageV1().StorageClasses().Delete(ctx, storageclass.Name, *metav1.NewDeleteOptions(0))
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}()
		defer func() {
			err := fpv.DeletePersistentVolumeClaim(client, pvclaim.Name, namespace)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			err = multiVCe2eVSphere.waitForCNSVolumeToBeDeletedInMultiVC(pv.Spec.CSI.VolumeHandle)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}()

		// taking pvclaims and pv 0th index because we are creating only single RWX PVC in this case
		pvclaim = pvclaims[0]

		ginkgo.By("Create Deployment with replica count 3")
		deployment, _, err := createVerifyAndScaleDeploymentPods(ctx, client, namespace, replica, false, labelsMap,
			pvclaim, nil, false, nil)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		defer func() {
			framework.Logf("Delete deployment set")
			err := client.AppsV1().Deployments(namespace).Delete(ctx, deployment.Name, *metav1.NewDeleteOptions(0))
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}()

		ginkgo.By("Verify PVC Bound state and CNS side verification")
		pvs, err = checkVolumeStateAndPerformCnsVerification(client, pvclaims, storagePolicyInVc1Vc2, "")
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		// taking pvclaims and pv 0th index because we are creating only single RWX PVC in this case
		pv = pvs[0]

		// volume placement should happen either should be on VC-2
		ginkgo.By("Verify volume placement, should match with the allowed topology specified")
		_, datastoreUrlsRack2, _, _, err := fetchDatastoreListMap(ctx, client)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		isCorrectPlacement := multiVCe2eVSphere.verifyPreferredDatastoreMatchInMultiVC(pv.Spec.CSI.VolumeHandle, datastoreUrlsRack2)
		gomega.Expect(isCorrectPlacement).To(gomega.BeTrue(), fmt.Sprintf("Volume provisioning has happened on the wrong "+
			"datastore. Expected 'true', got '%v'", isCorrectPlacement))

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
	    1. Create SC1 with allowed Topology details set to VC-2 and VC-3 availability Zone and specify storage policy tagged
		to vSAN ds of VC-3 whch is not vSAN FS enabled.
	    2. Create PVC using the SC created in step #1
	    3. PVC creation should fail with an appropriate error message as vSAN FS is disabled in VC-3
	    4. Create SC2 with allowedTopology details set to all AZs i.e. Zone-1, Zone-2 and Zone-3.
		Also pass storage policy tagged to vSAN DS from VC-1 and Datastore url tagged to vSAN DS from VC-2 with
		Immediate Binding mode.
	    5. Create PVC using the SC created in step #4
	    6. PVC creation should fail with an appropriate error message i.e. no compatible datastore found
	    7. Perform cleanup by deleting SC's, PVC's and SC
	*/

	// ginkgo.It("TC6", ginkgo.Label(p2, file, vanilla, level5, level2, stable, negative), func() {

	// 	ctx, cancel := context.WithCancel(context.Background())
	// 	defer cancel()

	// 	expectedErrMsg := "failed to fetch vCenter associated with topology segments"

	// 	// Get allowed topologies for Storage Class
	// 	allowedTopologyForSC := getTopologySelector(topologyAffinityDetails, topologyCategories,
	// 		topologyLength)
	// 	allowedTopologyForSC[4].Values = []string{"rack15"}

	// 	ginkgo.By(fmt.Sprintf("Creating Storage Class with access mode %q and fstype %q with invalid allowed "+
	// 		"topolgies specified", accessmode, nfs4FSType))
	// 	storageclass, pvclaim, err := createPVCAndStorageClass(client, namespace, labelsMap, scParameters, "", allowedTopologyForSC, "", false, accessmode)
	// 	gomega.Expect(err).NotTo(gomega.HaveOccurred())
	// 	defer func() {
	// 		err := client.StorageV1().StorageClasses().Delete(ctx, storageclass.Name, *metav1.NewDeleteOptions(0))
	// 		gomega.Expect(err).NotTo(gomega.HaveOccurred())
	// 	}()
	// 	defer func() {
	// 		err := fpv.DeletePersistentVolumeClaim(client, pvclaim.Name, namespace)
	// 		gomega.Expect(err).NotTo(gomega.HaveOccurred())
	// 	}()

	// 	ginkgo.By("Expect claim to fail provisioning volume since invalid allowed topology is specified in the storage class")
	// 	err = fpv.WaitForPersistentVolumeClaimPhase(v1.ClaimBound, client,
	// 		pvclaim.Namespace, pvclaim.Name, framework.Poll, time.Minute/2)
	// 	gomega.Expect(err).To(gomega.HaveOccurred())

	// 	ginkgo.By(fmt.Sprintf("Expected failure message: %+q", expectedErrMsg))
	// 	isFailureFound := checkEventsforError(client, namespace,
	// 		metav1.ListOptions{FieldSelector: fmt.Sprintf("involvedObject.name=%s", pvclaim.Name)}, expectedErrMsg)
	// 	gomega.Expect(isFailureFound).To(gomega.BeTrue(), "Unable to verify pvc create failure")
	// })

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
