/*
	Copyright 2024 The Kubernetes Authors.

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
	"time"

	snapclient "github.com/kubernetes-csi/external-snapshotter/client/v6/clientset/versioned"
	ginkgo "github.com/onsi/ginkgo/v2"
	"github.com/onsi/gomega"
	v1 "k8s.io/api/core/v1"
	storagev1 "k8s.io/api/storage/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	clientset "k8s.io/client-go/kubernetes"
	restclient "k8s.io/client-go/rest"
	"k8s.io/kubernetes/test/e2e/framework"
	fdep "k8s.io/kubernetes/test/e2e/framework/deployment"
	fnodes "k8s.io/kubernetes/test/e2e/framework/node"
	fpod "k8s.io/kubernetes/test/e2e/framework/pod"
	fpv "k8s.io/kubernetes/test/e2e/framework/pv"
	fss "k8s.io/kubernetes/test/e2e/framework/statefulset"
	admissionapi "k8s.io/pod-security-admission/api"
)

var _ = ginkgo.Describe("[rwx-topology] RWX-Topology", func() {
	f := framework.NewDefaultFramework("rwx-topology")
	f.NamespacePodSecurityEnforceLevel = admissionapi.LevelPrivileged
	var (
		client                  clientset.Interface
		namespace               string
		bindingModeWffc         storagev1.VolumeBindingMode
		bindingModeImm          storagev1.VolumeBindingMode
		topologyAffinityDetails map[string][]string
		topologyCategories      []string
		leafNode                int
		leafNodeTag0            int
		leafNodeTag1            int
		leafNodeTag2            int
		topologyLength          int
		scParameters            map[string]string
		restConfig              *restclient.Config
		snapc                   *snapclient.Clientset
		pandoraSyncWaitTime     int
		accessmode              v1.PersistentVolumeAccessMode
		labelsMap               map[string]string
		replica                 int32
		pvcItr                  int
		pvs                     []*v1.PersistentVolume
		pvclaim                 *v1.PersistentVolumeClaim
		pv                      *v1.PersistentVolume
	)

	ginkgo.BeforeEach(func() {
		client = f.ClientSet
		namespace = f.Namespace.Name

		// connecting to vc
		bootstrap()

		// fetch list of k8s nodes
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

		// pandora sync wait time
		if os.Getenv(envPandoraSyncWaitTime) != "" {
			pandoraSyncWaitTime, err = strconv.Atoi(os.Getenv(envPandoraSyncWaitTime))
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		} else {
			pandoraSyncWaitTime = defaultPandoraSyncWaitTime
		}

		// read topology map
		topologyLength, leafNode, leafNodeTag0, leafNodeTag1, leafNodeTag2 = 5, 4, 0, 1, 2
		topologyMap := GetAndExpectStringEnvVar(topologyMap)
		topologyAffinityDetails, topologyCategories = createTopologyMapLevel5(topologyMap,
			topologyLength)

		// snapshot
		restConfig = getRestConfigClient()
		snapc, err = snapclient.NewForConfig(restConfig)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		//setting map values
		labelsMap["app"] = "test"
		scParameters[scParamFsType] = nfs4FSType
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

		// perfrom cleanup of old stale entries of pv if left in the setup
		pvs, err := client.CoreV1().PersistentVolumes().List(ctx, metav1.ListOptions{})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		if len(pvs.Items) != 0 {
			for _, pv := range pvs.Items {
				gomega.Expect(client.CoreV1().PersistentVolumes().Delete(ctx, pv.Name,
					*metav1.NewDeleteOptions(0))).NotTo(gomega.HaveOccurred())
			}
		}
	})

	/*
		TESTCASE-1
		Deployment Pods with multiple replicas attached to single RWX PVC

		Steps:
		1. Create a StorageClass with BindingMode set to Immediate with no allowed topologies specified and with fsType as "nfs4"
		2. Create one PVC with "RWX" access mode.
		3. Wait for PVC to reach Bound state.
		4. Create deployment Pods with replica count 3 and attach it to above PVC.
		5. Wait for deployment Pods to reach Running state.
		6. As the SC lacks a specific topology, volume provisioning and Pod placement can occur in any AZ.
		7. Perform cleanup by deleting Pods, PVCs, and the SC.
	*/

	ginkgo.It("TC1Deployment Pods with multiple replicas attached to a "+
		"single RWX PVC", ginkgo.Label(p0, file, vanilla, level5, level2, newTest), func() {

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		// deployment pod and pvc count
		replica = 3
		pvcItr = 1

		ginkgo.By(fmt.Sprintf("Creating Storage Class with access mode %q and fstype %q with "+
			"no allowed topology specified", accessmode, nfs4FSType))
		storageclass, pvclaims, err := createStorageClassWithMultiplePVCs(client, namespace, labelsMap,
			scParameters, diskSize, nil, bindingModeImm, false, accessmode, "", pvcItr, false, false)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		defer func() {
			err := client.StorageV1().StorageClasses().Delete(ctx, storageclass.Name, *metav1.NewDeleteOptions(0))
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}()
		defer func() {
			err := fpv.DeletePersistentVolumeClaim(client, pvclaim.Name, namespace)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			err = e2eVSphere.waitForCNSVolumeToBeDeleted(pv.Spec.CSI.VolumeHandle)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}()

		ginkgo.By("Verify PVC Bound state and CNS side verification")
		pvs, err = checkVolumeStateAndPerformCnsVerification(client, pvclaims, "", "")
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		// taking pvclaims and pvs 0th index because we are creating only single RWX PVC in this case
		pvclaim = pvclaims[0]
		pv = pvs[0]

		ginkgo.By("Create Deployment")
		deployment, _, err := createVerifyAndScaleDeploymentPods(ctx, client, namespace, replica, false, labelsMap, pvclaim, nil, false, nil)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		defer func() {
			framework.Logf("Delete deployment set")
			err := client.AppsV1().Deployments(namespace).Delete(ctx, deployment.Name, *metav1.NewDeleteOptions(0))
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}()
	})

	/*
		TESTCASE-2
		StatefulSet Pods with multiple replicas attached to multiple RWX PVCs with scaling operation

		Steps:
		1. Create a StorageClass with BindingMode set to WFFC with no allowed topologies specified and with fsType as "nfs4"
		2. Use the SC to deploy a StatefulSet with 3 replicas, ReadWriteMany access mode, and Parallel pod management policy.
		Allow time for PVCs and Pods to reach Bound and Running states, respectively.
		3. Increase StatefulSet replica count to 5. Verify scaling operation.
		4. Decrease replica count to 1. Verify smooth scaling operation.
		5. As the SC lacks a specific topology, volume provisioning and Pod placement will occur in any available
		availability zones (AZs) for new Pod and PVC.
		6. Perform cleanup by deleting Pods, PVCs, and the SC.
	*/

	ginkgo.It("TC2Scaling operations involving StatefulSet Pods with multiple replicas, "+
		"each connected to RWX PVCs", ginkgo.Label(p0, file, vanilla, level5, level2, newTest), func() {

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		// pod replica count
		stsReplicas := 3
		scaleUpReplicaCount := 5
		scaleDownReplicaCount := 1

		ginkgo.By(fmt.Sprintf("Creating Storage Class with access mode %q and fstype %q with no "+
			"allowed topology specified and with WFFC binding mode", accessmode, nfs4FSType))
		scSpec := getVSphereStorageClassSpec(defaultNginxStorageClassName, scParameters, nil, "", bindingModeWffc, false)
		sc, err := client.StorageV1().StorageClasses().Create(ctx, scSpec, metav1.CreateOptions{})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		defer func() {
			err = client.StorageV1().StorageClasses().Delete(ctx, sc.Name, *metav1.NewDeleteOptions(0))
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}()

		ginkgo.By("Create StatefulSet with replica count 3 with RWX PVC access mode")
		service, statefulset, err := createStafeulSetAndVerifyPVAndPodNodeAffinty(ctx, client, namespace, true,
			int32(stsReplicas), false, nil, 0, false, false, false, "", accessmode, sc, false)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		defer func() {
			fss.DeleteAllStatefulSets(client, namespace)
			deleteService(namespace, client, service)
		}()

		ginkgo.By("Perform scaleup/scaledown operation on statefulsets")
		err = performScalingOnStatefulSetAndVerifyPvNodeAffinity(ctx, client, int32(scaleUpReplicaCount), int32(scaleDownReplicaCount),
			statefulset, false, namespace, nil, true, true, false)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
	})

	/*
		TESTCASE-3
		Multiple standalone pods are connected to a single RWX PVC, with each pod configured to
		have distinct read/write access permissions
		SC → WFC Binding Mode with allowed topology specified i.e. region-1 > zone-1 > building-1 > level-1 > rack-3

		Steps:
		1. Create a Storage Class with BindingMode set to WFC and allowed topologies set to
		   region-1 > zone-1 > building-1 > level-1 > rack-3
		2. Create RWX PVC using the SC created above.
		3. Wait for PVC to reach Bound state.
		4. Volume provisioning should happen on the datastore of the allowed topology as given in the SC.
		5. Create 3 standalone Pods using the PVC created in step #2.
		6. For Pod1 and Pod2 set read/write access and try reading/writing data into the volume.
		For the 3rd Pod set readOnly mode to true and verify that any write operation by Pod3 onto the volume
		should fail with an appropriate error message.
		7. Wait for Pods to reach Running Ready state.
		8. Pods can be created on any AZ.
		9. Perform cleanup by deleting Pod,PVC and SC.

	*/

	ginkgo.It("TC3Multiple Standalone Pods attached to a single RWX "+
		"PVC", ginkgo.Label(p0, file, vanilla, level5, level2, newTest), func() {

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		// standalone pod and pvc count
		noPodsToDeploy := 3
		pvcItr = 1

		// Create allowed topologies for Storage Class: region1 > zone1 > building1 > level1 > rack > rack3
		allowedTopologyForSC := getTopologySelector(topologyAffinityDetails, topologyCategories, topologyLength, leafNode, leafNodeTag2)

		ginkgo.By(fmt.Sprintf("Creating Storage Class with access mode %q and fstype %q with allowed "+
			"topology set to cluster-3", accessmode, nfs4FSType))
		storageclass, pvclaims, err := createStorageClassWithMultiplePVCs(client, namespace, labelsMap, scParameters, diskSize,
			allowedTopologyForSC, bindingModeWffc, false, accessmode, "", 1, false, false)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		defer func() {
			err := client.StorageV1().StorageClasses().Delete(ctx, storageclass.Name, *metav1.NewDeleteOptions(0))
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}()

		defer func() {
			err := fpv.DeletePersistentVolumeClaim(client, pvclaim.Name, namespace)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			err = e2eVSphere.waitForCNSVolumeToBeDeleted(pv.Spec.CSI.VolumeHandle)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}()

		// taking pvclaims 0th index because we are creating only single RWX PVC in this case
		pvclaim = pvclaims[0]

		ginkgo.By("Create 3 standalone Pods using the same PVC with different read/write permissions")
		podList, err := createStandalonePodsForRWXVolume(client, ctx, namespace, nil, pvclaim, false, execRWXCommandPod, noPodsToDeploy)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		defer func() {
			for i := 0; i < len(podList); i++ {
				ginkgo.By(fmt.Sprintf("Deleting the pod %s in namespace %s", podList[i].Name, namespace))
				err := fpod.DeletePodWithWait(client, podList[i])
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
			}
		}()

		ginkgo.By("Verify PVC Bound state and CNS side verification")
		pvs, err = checkVolumeStateAndPerformCnsVerification(client, pvclaims, "", "")
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		// taking pvs 0th index because we are creating only single RWX PVC in this case
		pv = pvs[0]

		ginkgo.By("Verify volume placement, should match with the allowed topology specified")
		_, _, datastoreUrlsRack3, _, err := fetchDatastoreListMap(ctx, client)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		isCorrectPlacement := e2eVSphere.verifyPreferredDatastoreMatch(pv.Spec.CSI.VolumeHandle, datastoreUrlsRack3)
		gomega.Expect(isCorrectPlacement).To(gomega.BeTrue(), fmt.Sprintf("Volume provisioning has happened on the wrong "+
			"datastore. Expected 'true', got '%v'", isCorrectPlacement))
	})

	/*
		TESTCASE-4
		Deployment Pods with multiple replicas attached to single RWX PVC with vSAN storage policy of rack-1
		defined in SC aloing with top level allowed topology
		SC → Immediate Binding Mode with allowed topology specified as region-1 > zone-1 > building-1
		and Storage policy specified which is tagged to datastore in rack-1 (vSAN DS)

		Steps:
		1. Create a Storage Class with immediate Binding mode and allowed topology set to region-1 > zone-1 > building-1.
		2. Set a Storage policy specific to datastore in rack-1 (vSAN tagged Storage Policy) in SC.
		3. Create a RWX PVC using the Storage Class created in step #1.
		4. Wait for PVC to reach Bound state.
		5. Volume provisioning should happen on the datastore of the allowed topology as given in the SC.
		6. Create deployment Pod with relica count 3 using the PVC created in step #3.
		7. Set node selector teams so that all Pods should get created on rack-1
		8. Wait for deployment Pod to reach running ready state.
		9. Deployment Pods should get created on any worker node of AZ1.
		10. Verify CNS metadata for PVC and Pod.
		11. Perform scaleup operation on deployment Pods. Increase the replica count from 3 to 6.
		12. Verify scaling operation should go fine.
		13. Perform scale down operation on deployment Pods. Decrease the replica count from 6 to 2.
		14. Verify scale down operation went smooth.
		15. Perform cleanup by deleting Pods, PVCs and SC.
	*/

	ginkgo.It("TC4", ginkgo.Label(p0, file, vanilla, level5, level2, newTest), func() {

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		// deployment pod and pvc count
		replica = 3
		pvcItr = 1

		//storage policy read of cluster-1(rack-1)
		storagePolicyName := GetAndExpectStringEnvVar(envVsanDsStoragePolicyCluster1)
		scParameters["storagepolicyname"] = storagePolicyName

		ginkgo.By("Set node selector term for deployment pods so that all pods should get created on one single AZ i.e. cluster-1(rack-1)")
		allowedTopologies := getTopologySelector(topologyAffinityDetails, topologyCategories,
			topologyLength, leafNode, leafNodeTag0)[4:]
		nodeSelectorTerms := getNodeSelectorMapForDeploymentPods(allowedTopologies)

		// Get allowed topologies for Storage Class (region1 > zone1 > building1)
		allowedTopologyForSC := getTopologySelector(topologyAffinityDetails, topologyCategories, 3)

		ginkgo.By(fmt.Sprintf("Creating Storage Class with access mode %q and fstype %q with higher "+
			"level allowed topology", accessmode, nfs4FSType))
		storageclass, pvclaims, err := createStorageClassWithMultiplePVCs(client, namespace, labelsMap, scParameters, diskSize,
			allowedTopologyForSC, bindingModeImm, false, accessmode, "", pvcItr, false, false)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		defer func() {
			err := client.StorageV1().StorageClasses().Delete(ctx, storageclass.Name, *metav1.NewDeleteOptions(0))
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}()

		defer func() {
			err := fpv.DeletePersistentVolumeClaim(client, pvclaim.Name, namespace)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			err = e2eVSphere.waitForCNSVolumeToBeDeleted(pv.Spec.CSI.VolumeHandle)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}()

		ginkgo.By("Verify PVC Bound state and CNS side verification")
		pvs, err = checkVolumeStateAndPerformCnsVerification(client, pvclaims, storagePolicyName, "")
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		// taking pvclaims and pvs 0th index because we are creating only single RWX PVC in this case
		pvclaim = pvclaims[0]
		pv = pvs[0]

		ginkgo.By("Verify volume placement, it should get created on the datastore as taken in the SC")
		datastoreUrlsRack1, _, _, _, err := fetchDatastoreListMap(ctx, client)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		isCorrectPlacement := e2eVSphere.verifyPreferredDatastoreMatch(pv.Spec.CSI.VolumeHandle, datastoreUrlsRack1)
		gomega.Expect(isCorrectPlacement).To(gomega.BeTrue(), fmt.Sprintf("Volume provisioning has happened on "+
			"the wrong datastore. Expected 'true', got '%v'", isCorrectPlacement))

		ginkgo.By("Create Deployments and specify node selector terms")
		deployment, pods, err := createVerifyAndScaleDeploymentPods(ctx, client, namespace, replica, false, labelsMap, pvclaim, nodeSelectorTerms, false, nil)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		defer func() {
			framework.Logf("Delete deployment set")
			err := client.AppsV1().Deployments(namespace).Delete(ctx, deployment.Name, *metav1.NewDeleteOptions(0))
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}()

		ginkgo.By("Verify deployment pods are scheduled on a specified node selector terms")
		err = verifyDeploymentPodNodeAffinity(ctx, client, namespace, allowedTopologies, pods, pv)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Scale up deployment to 6 replica")
		replica = 6
		deployment, pods, err = createVerifyAndScaleDeploymentPods(ctx, client, namespace, replica, true, labelsMap, pvclaim, nodeSelectorTerms, true, deployment)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Verify deployment pods are scheduled on a specified node selector terms")
		err = verifyDeploymentPodNodeAffinity(ctx, client, namespace, allowedTopologies, pods, pv)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Scale down deployment to 1 replica")
		replica = 1
		deployment, pods, err = createVerifyAndScaleDeploymentPods(ctx, client, namespace, replica, true, labelsMap, pvclaim, nodeSelectorTerms, true, deployment)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Verify volume metadata for deployment pod, pvc and pv")
		err = waitAndVerifyCnsVolumeMetadata(pv.Spec.CSI.VolumeHandle, pvclaim, pv, &pods.Items[0])
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
	})

	/*
		TESTCASE-5
		Deployment Pods with multiple replicas attached to single RWX PVC
		SC → Immediate Binding Mode with allowed topology specified  as region-1 > zone-1 > building-1 > levl-1 > rack-2/rack-3 and
		datastore url specified which is shared across rack-2 and rack-3 (Non-vSAN FS datastore)

		Steps:
		1. Create a Storage Class with binding mode set to Immediate and with allowed topology set to
		region-1 > zone-1 > building-1 > levl-1 > rack-2/rack-3 for level-5
		2. Set datastore url shared across rack-2 and rack-3 (take a datastore which is not file Share compatible)
		3. Create RWX PVC using the SC created above.
		4. PVC creation will be stuck in a Pending state with an appropriate error due to non-VSAN FS datastore used.
		5. Perform cleanup by deleting PVC, SC.
	*/

	ginkgo.It("TC5", ginkgo.Label(p1, file, vanilla, level5, level2, newTest), func() {

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		// non vSAN FS compatible datastore
		dsUrl := GetAndExpectStringEnvVar(envNonVsanDsUrl)
		scParameters["datastoreurl"] = dsUrl
		expectedErrMsg := "not found in candidate list for volume provisioning"

		/* Get allowed topologies for Storage Class
		region1 > zone1 > building1 > level1 > rack > (rack2 and rack3)
		Taking Shared datstore which is not vSAN compatible between Rack2 and Rack3 */
		allowedTopologyForSC := getTopologySelector(topologyAffinityDetails, topologyCategories,
			topologyLength, leafNode, leafNodeTag1, leafNodeTag2)

		ginkgo.By(fmt.Sprintf("Creating Storage Class with access mode %q and fstype %q with allowed topolgies specified as cluster-2/cluster-3 and non-compatible shared datastore", accessmode, nfs4FSType))
		storageclass, pvclaim, err := createPVCAndStorageClass(client, namespace, labelsMap, scParameters, diskSize, allowedTopologyForSC, bindingModeImm, false, accessmode)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		defer func() {
			err := client.StorageV1().StorageClasses().Delete(ctx, storageclass.Name, *metav1.NewDeleteOptions(0))
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}()
		defer func() {
			err := fpv.DeletePersistentVolumeClaim(client, pvclaim.Name, namespace)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}()

		ginkgo.By("Expect claim to fail provisioning volume since no valid VSAN datastore is specified in storage class")
		err = fpv.WaitForPersistentVolumeClaimPhase(v1.ClaimBound, client,
			pvclaim.Namespace, pvclaim.Name, framework.Poll, time.Minute/2)
		gomega.Expect(err).To(gomega.HaveOccurred())

		ginkgo.By(fmt.Sprintf("Expected failure message: %+q", expectedErrMsg))
		isFailureFound := checkEventsforError(client, namespace,
			metav1.ListOptions{FieldSelector: fmt.Sprintf("involvedObject.name=%s", pvclaim.Name)}, expectedErrMsg)
		gomega.Expect(isFailureFound).To(gomega.BeTrue(), "Unable to verify pvc create failure")
	})

	/*
		TESTCASE-6
		Deployment Pod creation
		SC → Immediate Binding Mode with allowed topology specified as region-1 > zone-1 > building-1 > level-1 > rack-3 and
		storage policy specified which is tagged to datastore in rack-3 (vSAN DS)

		Steps:
		1. Create a Storage Class with volumeBindingMode set to Immediate and specify allowed topologies for the SC, such as region-1 > zone-1 > building-1 > level-1 > rack-3 and a storage policy specific to cluster3 (rack-3).
		2. Create a PVC with accessmode set to "RWX".
		3. Verify volume provisioning should happen on the SC allowed topology.
		3. Create a deployment Pod with node selector term set to rack-2 using the PVC created in step 2 with a replica count of 1 and configured with ReadWriteMany (RWX) access mode.
		4. Deployment Pod should get created on the given AZ as specified in the node selector term of deployment pod.
		5. Verify CNS metadata for PVC and Pod.
		7. Scaleup deployment pod replica count to 3.
		8. Verify deployment pod scaling operation went smooth.
		9. New deployment Pod should get created on the given AZ as specified in the node selector term of deployment pod.
		6. Perform cleanup by deleting deployment Pod, PVC and SC.
	*/

	ginkgo.It("TC6", ginkgo.Label(p0, file, vanilla, level5, level2, newTest), func() {

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		// deployment pod and pvc count
		replica = 1
		pvcItr = 1

		// storage policy of cluster-3(rack-3)
		storagePolicyName := GetAndExpectStringEnvVar(envVsanDsStoragePolicyCluster3)
		scParameters["storagepolicyname"] = storagePolicyName

		// Node selector term for deployment Pod region1 > zone1 > building1 > level1 > rack2
		ginkgo.By("Set specific allowed topology for node selector terms")
		allowedTopologies := getTopologySelector(topologyAffinityDetails, topologyCategories,
			topologyLength, leafNode, leafNodeTag1)
		nodeSelectorTerms := getNodeSelectorMapForDeploymentPods(allowedTopologies)

		// Get allowed topologies for Storage Class region1 > zone1 > building1 > level1 > rack3
		allowedTopologyForSC := getTopologySelector(topologyAffinityDetails, topologyCategories,
			topologyLength, leafNode, leafNodeTag2)

		ginkgo.By(fmt.Sprintf("Creating Storage Class with access mode %q and fstype %q with allowed "+
			"topology specified to cluster3", accessmode, nfs4FSType))
		storageclass, pvclaims, err := createStorageClassWithMultiplePVCs(client, namespace, labelsMap, scParameters, diskSize,
			allowedTopologyForSC, bindingModeImm, false, accessmode, "", pvcItr, false, false)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		defer func() {
			err := client.StorageV1().StorageClasses().Delete(ctx, storageclass.Name, *metav1.NewDeleteOptions(0))
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}()

		defer func() {
			err := fpv.DeletePersistentVolumeClaim(client, pvclaims[0].Name, namespace)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			err = e2eVSphere.waitForCNSVolumeToBeDeleted(pvs[0].Spec.CSI.VolumeHandle)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())

		}()

		ginkgo.By("Verify PVC Bound state and CNS side verification")
		pvs, err = checkVolumeStateAndPerformCnsVerification(client, pvclaims, storagePolicyName, "")
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Verify volume placement")
		_, _, datastoreUrlsRack3, _, err := fetchDatastoreListMap(ctx, client)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		isCorrectPlacement := e2eVSphere.verifyPreferredDatastoreMatch(pvs[0].Spec.CSI.VolumeHandle, datastoreUrlsRack3)
		gomega.Expect(isCorrectPlacement).To(gomega.BeTrue(), fmt.Sprintf("Volume provisioning has "+
			"happened on the wrong datastore. Expected 'true', got '%v'", isCorrectPlacement))

		//taking pvclaims and pvs 0th index because we are creating only single RWX PVC in this case
		pvclaim = pvclaims[0]
		pv = pvs[0]

		ginkgo.By("Create Deployment pod with node selector terms specific to rack-2 but sc allowed topology set to rack-3")
		deployment, pods, err := createVerifyAndScaleDeploymentPods(ctx, client, namespace, replica, false, labelsMap, pvclaim, nodeSelectorTerms, false, nil)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		defer func() {
			framework.Logf("Delete deployment set")
			err := client.AppsV1().Deployments(namespace).Delete(ctx, deployment.Name, *metav1.NewDeleteOptions(0))
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}()

		ginkgo.By("Verify deployment pods are scheduled on a specified node selector terms")
		err = verifyDeploymentPodNodeAffinity(ctx, client, namespace, allowedTopologies, pods, pv)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Scale up deployment to 3 replica")
		replica = 3
		deployment, pods, err = createVerifyAndScaleDeploymentPods(ctx, client, namespace, replica, true, labelsMap, pvclaim, nodeSelectorTerms, true, deployment)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Verify deployment pods are scheduled on a specified node selector terms")
		err = verifyDeploymentPodNodeAffinity(ctx, client, namespace, allowedTopologies, pods, pv)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Verify volume metadata for deployment pod, pvc and pv")
		err = waitAndVerifyCnsVolumeMetadata(pv.Spec.CSI.VolumeHandle, pvclaim, pv, &pods.Items[0])
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

	})

	/*
		TESTCASE-7
		StatefulSet Workload with Parallel Pod Management and Scale-Up/Scale-Down Operations
		SC → WFC Binding Mode with allowed topology specified as region-1 > zone-1

		Steps:
		1. Create a Storage Class with volumeBindingMode set to WFFC and specify allowed topology (e.g., region-1 > zone-1).
		2. Create statefulSet with 3 replicas, each configured with ReadWriteMany (RWX) access mode and with default Pod management policy and specify pod affinity
		3. Ensure that all the PVCs and Pods are successfully created.
		-  The PVCs should be created in any available Availability Zone (AZ).
		-  The Pods should be created in any AZ
		4. Increase the replica count of statefulset to 5.
		5. Decrease the replica count of statefulset to 1
		6. Verify that the scaling up and scaling down of the StatefulSet is successful.
		7. Volume provisioning and Pod placement will occur in the available availability zones (AZs) specified in the SC during the scale-up operation.
		8. Verify CNS metadata for any one PVC and Pod.
		9. Perform cleanup by deleting StatefulSet, PVCs, and SC
	*/

	ginkgo.It("TC7", ginkgo.Label(p1, file, vanilla, level5, level2, newTest), func() {

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		stsReplicas := 3
		scaleUpReplicaCount := 5
		scaleDownReplicaCount := 1

		// Get allowed topologies for Storage Class (region1 > zone1)
		allowedTopologyForSC := getTopologySelector(topologyAffinityDetails, topologyCategories, 2)

		ginkgo.By(fmt.Sprintf("Creating Storage Class with access mode %q and fstype %q with allowed topology specified at top level and with WFFC binding mode", accessmode, nfs4FSType))
		scSpec := getVSphereStorageClassSpec(defaultNginxStorageClassName, scParameters, allowedTopologyForSC, "", bindingModeWffc, false)
		sc, err := client.StorageV1().StorageClasses().Create(ctx, scSpec, metav1.CreateOptions{})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		defer func() {
			err = client.StorageV1().StorageClasses().Delete(ctx, sc.Name, *metav1.NewDeleteOptions(0))
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}()

		ginkgo.By("Create StatefulSet with replica count 3 with RWX PVC access mode with pod affinity set to true")
		service, statefulset, err := createStafeulSetAndVerifyPVAndPodNodeAffinty(ctx, client, namespace, false, int32(stsReplicas), false,
			allowedTopologyForSC, 0, true, false, false, "", accessmode, sc, false)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		defer func() {
			fss.DeleteAllStatefulSets(client, namespace)
			deleteService(namespace, client, service)
		}()

		ginkgo.By("Verify volume placement, should match with the allowed topology specified")
		err = volumePlacementVerificationForSts(ctx, client, statefulset, namespace)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Perform scaleup/scaledown operation on statefulsets")
		err = performScalingOnStatefulSetAndVerifyPvNodeAffinity(ctx, client, int32(scaleUpReplicaCount), int32(scaleDownReplicaCount),
			statefulset, false, namespace, nil, true, true, false)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
	})

	/*
		TESTCASE-8
		Storage Class Creation with Invalid Label Details in One Level
		SC → Immediate Binding Mode with invalid allowed topology specified  i.e. region-1 > zone-1 > building-1 > level-1 > rack-15

		Steps:
		1. Create a Storage Class with invalid label details, specifically using a label path such as "region-1 > zone-1 > building-1 > level-1 > rack-15"
		2. Create a PVC) using the SC created in step #1 with RWX access mode.
		3. As the label path is invalid, an appropriate error message should be displayed.
		4. Perform cleanup by deleting PVC and SC.
	*/

	ginkgo.It("TC8", ginkgo.Label(p2, file, vanilla, level5, level2, stable, negative), func() {

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		expectedErrMsg := "failed to fetch vCenter associated with topology segments"

		// Get allowed topologies for Storage Class
		allowedTopologyForSC := getTopologySelector(topologyAffinityDetails, topologyCategories,
			topologyLength)
		allowedTopologyForSC[4].Values = []string{"rack15"}

		ginkgo.By(fmt.Sprintf("Creating Storage Class with access mode %q and fstype %q with invalid allowed topolgies specified", accessmode, nfs4FSType))
		storageclass, pvclaim, err := createPVCAndStorageClass(client, namespace, labelsMap, scParameters, "", allowedTopologyForSC, "", false, accessmode)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		defer func() {
			err := client.StorageV1().StorageClasses().Delete(ctx, storageclass.Name, *metav1.NewDeleteOptions(0))
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}()
		defer func() {
			err := fpv.DeletePersistentVolumeClaim(client, pvclaim.Name, namespace)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}()

		ginkgo.By("Expect claim to fail provisioning volume since invalid allowed topology is specified in the storage class")
		err = fpv.WaitForPersistentVolumeClaimPhase(v1.ClaimBound, client,
			pvclaim.Namespace, pvclaim.Name, framework.Poll, time.Minute/2)
		gomega.Expect(err).To(gomega.HaveOccurred())

		ginkgo.By(fmt.Sprintf("Expected failure message: %+q", expectedErrMsg))
		isFailureFound := checkEventsforError(client, namespace,
			metav1.ListOptions{FieldSelector: fmt.Sprintf("involvedObject.name=%s", pvclaim.Name)}, expectedErrMsg)
		gomega.Expect(isFailureFound).To(gomega.BeTrue(), "Unable to verify pvc create failure")
	})

	/*
		TESTCASE-9
		Create Storage Class with a single label in allowed topology
		SC → Immediate Binding Mode with specific allowed topology specified  i.e. k8s-rack > rack-1

		Steps:
		1. Create a Storage Class with only one level of topology details specified i.e. k8s-rack > rack-1.
		2. Create a PVC using the SC created in step #1, with ReadWriteMany (RWX) access mode.
		3. Wait for PVC to reach Bound state.
		4. Create deployment Pod with replica count 1 and specify node selector as same as allowed topology of SC.
		5. Wait for deployment Pod to be in running state.
		6. PVC should get created on the AZ as specified in the SC allowed topology and the Pod placement should happen on the AZ
		as specified in the node selector terms.
		7. Increase the replica count to 3.
		8. Wait for new pods to be in running state.
		9. New Pods should get created on the same AZ as specified in the node selector term of deployment.
		7. Perform cleanup by deleting Pods, PVC and SC.
	*/

	ginkgo.It("TC9", ginkgo.Label(p1, file, vanilla, level5, level2, newTest), func() {

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		// deployment pod and pvc count
		replica = 1
		pvcItr = 1

		// Get allowed topologies for Storage Class (rack > rack1)
		ginkgo.By("Set node selector term for deployment pods so that all pods should get created on one single AZ i.e. cluster-1(rack-1)")
		allowedTopologies := getTopologySelector(topologyAffinityDetails, topologyCategories,
			topologyLength, leafNode, leafNodeTag0)[4:]
		nodeSelectorTerms := getNodeSelectorMapForDeploymentPods(allowedTopologies)

		ginkgo.By(fmt.Sprintf("Creating Storage Class with access mode %q and fstype %q with allowed topology "+
			"specified to single level cluster-1", accessmode, nfs4FSType))
		storageclass, pvclaims, err := createStorageClassWithMultiplePVCs(client, namespace, labelsMap, scParameters, diskSize,
			allowedTopologies, bindingModeImm, false, accessmode, "", pvcItr, false, false)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		defer func() {
			err := client.StorageV1().StorageClasses().Delete(ctx, storageclass.Name, *metav1.NewDeleteOptions(0))
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}()

		defer func() {
			err := fpv.DeletePersistentVolumeClaim(client, pvclaims[0].Name, namespace)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			err = e2eVSphere.waitForCNSVolumeToBeDeleted(pvs[0].Spec.CSI.VolumeHandle)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}()

		ginkgo.By("Verify PVC Bound state and CNS side verification")
		pvs, err = checkVolumeStateAndPerformCnsVerification(client, pvclaims, "", "")
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Verify volume placement, should match with the allowed topology specified")
		datastoreUrlsRack1, _, _, _, err := fetchDatastoreListMap(ctx, client)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		isCorrectPlacement := e2eVSphere.verifyPreferredDatastoreMatch(pvs[0].Spec.CSI.VolumeHandle, datastoreUrlsRack1)
		gomega.Expect(isCorrectPlacement).To(gomega.BeTrue(), fmt.Sprintf("Volume provisioning has happened on "+
			"the wrong datastore. Expected 'true', got '%v'", isCorrectPlacement))

		//taking pvclaims and pvs 0th index because we are creating only single RWX PVC in this case
		pvclaim = pvclaims[0]
		pv = pvs[0]

		ginkgo.By("Create Deployment")
		deployment, pods, err := createVerifyAndScaleDeploymentPods(ctx, client, namespace, replica, false, labelsMap, pvclaim, nodeSelectorTerms, false, nil)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		defer func() {
			framework.Logf("Delete deployment set")
			err := client.AppsV1().Deployments(namespace).Delete(ctx, deployment.Name, *metav1.NewDeleteOptions(0))
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}()

		ginkgo.By("Verify deployment pods are scheduled on a specified node selector terms")
		err = verifyDeploymentPodNodeAffinity(ctx, client, namespace, allowedTopologies, pods, pv)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Scale up deployment to 3 replica")
		replica = 3
		deployment, pods, err = createVerifyAndScaleDeploymentPods(ctx, client, namespace, replica, true, labelsMap, pvclaim,
			nodeSelectorTerms, true, deployment)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Verify deployment pods are scheduled on a specified node selector terms")
		err = verifyDeploymentPodNodeAffinity(ctx, client, namespace, allowedTopologies, pods, pv)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

	})

	/*
		TESTCASE-10
		Create Storage Class with a single label in allowed topology and with datastore url
		SC → top level allowed topology specified i.e. k8s-region:region-1 with WFC Binding mode and
		datastore url passed (vSAN ds of rack-3)

		Steps:
		1. Create Storage Class with only top level of topology details specified i.e. region-1
		2. Create PVC using the Storage Class created in step #1, with ReadWriteMany (RWX) access mode.
		3. Wait for PVC to reach Bound state.
		4. Create multiple Pods using the PVC created in the step #2 with node selector term specific to rack-3
		5. Wait for Pod to be in running state.
		6. PVC should get created on the AZ as specified in the SC allowed topology and the Pod placement should happen on any AZ.
		7. Perform cleanup by deleting Pods,PVC and SC.
	*/

	ginkgo.It("TC10", ginkgo.Label(p0, file, vanilla, level5, level2, newTest), func() {

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		// pvc and pod count
		pvcItr = 1
		no_pods_to_deploy := 3

		// vSAN datastore url of cluster-3(rack-3)
		datastoreurl := GetAndExpectStringEnvVar(envVsanDsUrlCluster3)
		scParameters[scParamDatastoreURL] = datastoreurl

		// Create allowed topologies for Storage Class: k8s-region > region-1
		allowedTopologyForSC := getTopologySelector(topologyAffinityDetails, topologyCategories, 1)

		ginkgo.By("Set node selector term for standlone pods so that all pods should get created on one single AZ i.e. cluster-3(rack-3)")
		allowedTopologies := getTopologySelector(topologyAffinityDetails, topologyCategories, topologyLength, leafNode, leafNodeTag2)
		nodeSelectorTerms := getNodeSelectorMapForDeploymentPods(allowedTopologies)

		ginkgo.By(fmt.Sprintf("Creating Storage Class with access mode %q and fstype %q with allowed "+
			"topology set at the top level", accessmode, nfs4FSType))
		storageclass, pvclaims, err := createStorageClassWithMultiplePVCs(client, namespace, nil, scParameters, diskSize,
			allowedTopologyForSC, bindingModeWffc, false, accessmode, "", pvcItr, false, false)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		defer func() {
			err := client.StorageV1().StorageClasses().Delete(ctx, storageclass.Name, *metav1.NewDeleteOptions(0))
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}()

		defer func() {
			err := fpv.DeletePersistentVolumeClaim(client, pvclaims[0].Name, namespace)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			err = e2eVSphere.waitForCNSVolumeToBeDeleted(pvs[0].Spec.CSI.VolumeHandle)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}()

		//taking pvclaims 0th index because we are creating only single RWX PVC in this case
		pvclaim = pvclaims[0]

		ginkgo.By("Create 3 standalone Pods using the same PVC")
		podList, err := createStandalonePodsForRWXVolume(client, ctx, namespace, nodeSelectorTerms, pvclaims[0], false,
			execRWXCommandPod, no_pods_to_deploy)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		defer func() {
			for i := 0; i < len(podList); i++ {
				ginkgo.By(fmt.Sprintf("Deleting the pod %s in namespace %s", podList[i].Name, namespace))
				err := fpod.DeletePodWithWait(client, podList[i])
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
			}
		}()

		ginkgo.By("Verify PVC Bound state and CNS side verification")
		pvs, err = checkVolumeStateAndPerformCnsVerification(client, pvclaims, "", datastoreurl)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		//taking pvs 0th index because we are creating only single RWX PVC in this case
		pv = pvs[0]

		ginkgo.By("Verify volume placement, should match with the allowed topology specified")
		_, _, datastoreUrlsRack3, _, err := fetchDatastoreListMap(ctx, client)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		isCorrectPlacement := e2eVSphere.verifyPreferredDatastoreMatch(pvs[0].Spec.CSI.VolumeHandle, datastoreUrlsRack3)
		gomega.Expect(isCorrectPlacement).To(gomega.BeTrue(), fmt.Sprintf("Volume provisioning has happened on the "+
			"wrong datastore. Expected 'true', got '%v'", isCorrectPlacement))

		ginkgo.By("Verify standalone pods are scheduled on a selected node selector terms")
		for i := 0; i < len(podList); i++ {
			err = verifyStandalonePodAffinity(ctx, client, podList[i], namespace, allowedTopologies)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}
	})

	/*
		TESTCASE-11
		Volume expansion on file volumes
		SC → all levels of topology details with Immediate Binding mode

		Steps:
		1. Create Storage Class with all levels of topology details specified in the Storage Class and with Immediate Binding mode.
		2. Create PVC with RWX access mode using SC created above.
		3. Verify that the PVC transitions to the Bound state and PVC is created in any of the Availability Zone.
		4. Trigger Offline Volume Expansion on the PVC created in step #2
		5. Verify that the PVC resize operation fails with an appropriate error message, as volume expansion is not supported for File Volumes.
		6. Perform Cleanup by deleting PVC and SC.
	*/

	ginkgo.It("TC11", ginkgo.Label(p1, file, vanilla, level5, level2, stable), func() {

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		expectedErrMsg := "volume expansion is only supported for block volume type"

		// Get allowed topologies for Storage Class i.e. region1 > zone1 > building1 > level1 > rack > rack1/rack2/rack3
		allowedTopologyForSC := getTopologySelector(topologyAffinityDetails, topologyCategories, topologyLength)

		ginkgo.By(fmt.Sprintf("Creating Storage Class with access mode %q and fstype %q and set allowVolumeExpansion "+
			"to true with all allowed topolgies specified", accessmode, nfs4FSType))
		storageclass, pvclaim, err := createPVCAndStorageClass(client, namespace, labelsMap, scParameters, diskSize, allowedTopologyForSC,
			bindingModeImm, true, accessmode)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		defer func() {
			err := client.StorageV1().StorageClasses().Delete(ctx, storageclass.Name, *metav1.NewDeleteOptions(0))
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}()
		defer func() {
			err := fpv.DeletePersistentVolumeClaim(client, pvclaim.Name, namespace)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}()

		ginkgo.By("Wait for PVC and PV to reach Bound state")
		_, err = fpv.WaitForPVClaimBoundPhase(client, []*v1.PersistentVolumeClaim{pvclaim}, framework.ClaimProvisionTimeout)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Modify PVC spec to trigger volume expansion, Expect to fail as file volume expansion is not supported")
		currentPvcSize := pvclaim.Spec.Resources.Requests[v1.ResourceStorage]
		newSize := currentPvcSize.DeepCopy()
		newSize.Add(resource.MustParse("3Gi"))
		framework.Logf("currentPvcSize %v, newSize %v", currentPvcSize, newSize)

		newPVC, err := expandPVCSize(pvclaim, newSize, client)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		pvclaim = newPVC
		gomega.Expect(pvclaim).NotTo(gomega.BeNil())

		pvcSize := pvclaim.Spec.Resources.Requests[v1.ResourceStorage]
		if pvcSize.Cmp(newSize) != 0 {
			framework.Failf("error updating pvc size %q", pvclaim.Name)
		}

		ginkgo.By("Verify if controller resize failed")
		err = waitForPvResizeForGivenPvc(pvclaim, client, pollTimeoutShort)
		gomega.Expect(err).To(gomega.HaveOccurred())

		ginkgo.By("Volume expansion should fail for file volume")
		ginkgo.By(fmt.Sprintf("Expected failure message: %+q", expectedErrMsg))
		isFailureFound := checkEventsforError(client, namespace,
			metav1.ListOptions{FieldSelector: fmt.Sprintf("involvedObject.name=%s", pvclaim.Name)}, expectedErrMsg)
		gomega.Expect(isFailureFound).To(gomega.BeTrue(),
			fmt.Sprintf("Expected pvc creation failure with error message: %s", expectedErrMsg))
	})

	/*
		TESTCASE-12
		PVC creation using shared datastore lacking capability of vSAN FS
		SC → default values, shared datastore url passed shared across all AZs lacking vSAN FS capability, Immediate Binding mode

		Steps:
		1. Create a Storage Class with default values and pass datastore Url shared across all AZs but lacking the vSAN FS capability with Immediate binding mode.
		2. Create a PVC using the SC created in step #1, with ReadWriteMany (RWX) access mode.
		3. PVC should get stuck in Pending state with appropriate error message because the datastore url passed in SC is not vSAN FS enabled datastore
		4. Perform cleanup by deleting PVC and SC
	*/

	ginkgo.It("TC12", ginkgo.Label(p1, file, vanilla, level5, level2, stable), func() {

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		datastoreurl := GetAndExpectStringEnvVar(envNonVsanDsUrl)
		scParameters[scParamDatastoreURL] = datastoreurl

		ginkgo.By(fmt.Sprintf("Creating Storage Class with access mode %q and fstype %q with no allowed "+
			"topolgies specified", accessmode, nfs4FSType))
		storageclass, pvclaim, err := createPVCAndStorageClass(client, namespace, labelsMap, scParameters,
			diskSize, nil, bindingModeImm, false, accessmode)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		defer func() {
			err := client.StorageV1().StorageClasses().Delete(ctx, storageclass.Name, *metav1.NewDeleteOptions(0))
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}()
		defer func() {
			err := fpv.DeletePersistentVolumeClaim(client, pvclaim.Name, namespace)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}()

		ginkgo.By("Expect claim to fail provisioning volume since no valid VSAN datastore is specified in storage class")
		err = fpv.WaitForPersistentVolumeClaimPhase(v1.ClaimBound, client,
			pvclaim.Namespace, pvclaim.Name, framework.Poll, time.Minute/2)
		gomega.Expect(err).To(gomega.HaveOccurred())
		expectedErrMsg := "failed to provision volume with StorageClass \"" + storageclass.Name + "\""
		ginkgo.By(fmt.Sprintf("Expected failure message: %+q", expectedErrMsg))
		isFailureFound := checkEventsforError(client, namespace,
			metav1.ListOptions{FieldSelector: fmt.Sprintf("involvedObject.name=%s", pvclaim.Name)}, expectedErrMsg)
		gomega.Expect(isFailureFound).To(gomega.BeTrue(), "Unable to verify pvc create failure")
	})

	/*
		TESTCASE-13
		Allowed topology specifies rack-2, but the storage policy is provided from rack-3
		SC → specific allowed topology i.e. k8s-rack:rack-2 and storage Policy specified tagged with
		rack-3 datastore

		Steps:
		1. Create a Storage Class  with specific allowed topology (rack-2) and storage policy
		tagged with rack-3 vSAN datastore
		2. Create a PVC using the SC created in step #1, with ReadWriteMany (RWX) access mode.
		3. PVC should get stuck in Pending state with an appropriate error message
		4. Perform cleanup by deleting PVC and SC
	*/

	ginkgo.It("TC13", ginkgo.Label(p2, file, vanilla, level5, level2, stable), func() {

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		expectedErrMsg := "No compatible datastores found for storage policy"

		// storage policy of rack3
		storagePolicyName := GetAndExpectStringEnvVar(envVsanDsStoragePolicyCluster3)
		scParameters["storagepolicyname"] = storagePolicyName

		/* Get allowed topologies for Storage Class
		(region1 > zone1 > building1 > level1 > rack > rack2) */
		allowedTopologyForSC := getTopologySelector(topologyAffinityDetails, topologyCategories,
			topologyLength, leafNode, leafNodeTag1)

		ginkgo.By(fmt.Sprintf("Creating Storage Class with access mode %q and fstype %q with no "+
			"allowed topolgies specified", accessmode, nfs4FSType))
		storageclass, pvclaim, err := createPVCAndStorageClass(client, namespace, labelsMap, scParameters,
			diskSize, allowedTopologyForSC, bindingModeImm, false, accessmode)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		defer func() {
			err := client.StorageV1().StorageClasses().Delete(ctx, storageclass.Name, *metav1.NewDeleteOptions(0))
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}()
		defer func() {
			err := fpv.DeletePersistentVolumeClaim(client, pvclaim.Name, namespace)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}()

		ginkgo.By("Expect claim to fail provisioning volume since no valid datastore is specified in the storage class")
		err = fpv.WaitForPersistentVolumeClaimPhase(v1.ClaimBound, client,
			pvclaim.Namespace, pvclaim.Name, framework.Poll, time.Minute/2)
		gomega.Expect(err).To(gomega.HaveOccurred())
		ginkgo.By(fmt.Sprintf("Expected failure message: %+q", expectedErrMsg))
		isFailureFound := checkEventsforError(client, namespace,
			metav1.ListOptions{FieldSelector: fmt.Sprintf("involvedObject.name=%s", pvclaim.Name)}, expectedErrMsg)
		gomega.Expect(isFailureFound).To(gomega.BeTrue(), "Unable to verify pvc create failure")
	})

	/*
		TESTCASE-14
		RWM with dynamic Snapshot
		SC → specific allowed topology i.e. region-1 > zone-1 > building-1 > level-1 > rack-1 with Immediate Binding mode

		Steps:
		1. Create a Storage Class with specific allowed topology of rack-1
		2. Create a PVC using the SC created in step #1, with ReadWriteMany (RWX) access mode.
		3. Verify PVC should reach Bound state.
		4. Verify PVC placement. It should provision on the same AZ as given in the SC allowed topology.
		5. Create deployment Pod with replica count 3.
		6. Pod placement can happen on any AZ.
		7. Create a dynamic volume snapshot for the PVC created in step #2.
		8. Verify dynamic volume snapshot should fail with an appropriate error message.
		9.  Perform cleanup by deleting VS, PVC and SC
	*/

	ginkgo.It("TC14", ginkgo.Label(p1, file, vanilla, level5, level2, stable), func() {

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		pvcItr = 1
		replica = 3

		/* Get allowed topologies for Storage Class
		(region1 > zone1 > building1 > level1 > rack > rack1) */
		allowedTopologyForSC := getTopologySelector(topologyAffinityDetails, topologyCategories,
			topologyLength, leafNode, leafNodeTag0)

		ginkgo.By(fmt.Sprintf("Creating Storage Class with access mode %q and fstype %q", accessmode, nfs4FSType))
		storageclass, pvclaims, err := createStorageClassWithMultiplePVCs(client, namespace, labelsMap, scParameters, diskSize,
			allowedTopologyForSC, bindingModeImm, false, accessmode, "", pvcItr, false, false)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		defer func() {
			err := client.StorageV1().StorageClasses().Delete(ctx, storageclass.Name, *metav1.NewDeleteOptions(0))
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}()

		defer func() {
			err := fpv.DeletePersistentVolumeClaim(client, pvclaims[0].Name, namespace)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			err = e2eVSphere.waitForCNSVolumeToBeDeleted(pvs[0].Spec.CSI.VolumeHandle)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}()

		ginkgo.By("Verify PVC Bound state and CNS side verification")
		pvs, err = checkVolumeStateAndPerformCnsVerification(client, pvclaims, "", "")
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		// taking pvclaims and pvs 0th index because we are creating only single RWX PVC in this case
		pvclaim = pvclaims[0]
		pv = pvs[0]

		ginkgo.By("Create Deployment")
		deployment, _, err := createVerifyAndScaleDeploymentPods(ctx, client, namespace, replica, false, labelsMap, pvclaim, nil, false, nil)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		defer func() {
			framework.Logf("Delete deployment set")
			err := client.AppsV1().Deployments(namespace).Delete(ctx, deployment.Name, *metav1.NewDeleteOptions(0))
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}()
		ginkgo.By("Create volume snapshot class")
		volumeSnapshotClass, err := createVolumeSnapshotClass(ctx, snapc, deletionPolicy)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		defer func() {
			err = snapc.SnapshotV1().VolumeSnapshotClasses().Delete(ctx, volumeSnapshotClass.Name,
				metav1.DeleteOptions{})
			gomega.Expect(err).NotTo(gomega.HaveOccurred())

		}()

		ginkgo.By("Create a dynamic volume snapshot and verify volume snapshot creation failed")
		volumeSnapshot, snapshotContent, _,
			_, _, err := createDynamicVolumeSnapshot(ctx, namespace, snapc, volumeSnapshotClass,
			pvclaim, pv.Spec.CSI.VolumeHandle, diskSize, true)
		gomega.Expect(err).To(gomega.HaveOccurred())
		defer func() {
			err = deleteVolumeSnapshotContent(ctx, snapshotContent, snapc, namespace, pandoraSyncWaitTime)
			gomega.Expect(err).To(gomega.HaveOccurred())

			framework.Logf("Deleting volume snapshot")
			deleteVolumeSnapshotWithPandoraWait(ctx, snapc, namespace, volumeSnapshot.Name, pandoraSyncWaitTime)

			framework.Logf("Wait till the volume snapshot is deleted")
			err = waitForVolumeSnapshotContentToBeDeletedWithPandoraWait(ctx, snapc,
				*volumeSnapshot.Status.BoundVolumeSnapshotContentName, pandoraSyncWaitTime)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())

		}()
	})

	/*
		TESTCASE-15
		Deployment Pod with Scale-Up/Scale-Down Operations
		SC → WFC Binding Mode with allowed topology specified i.e. region-1 > zone-1 > building-1 > level-1 > rack-1 and rack-3

		Steps:
		1. Create Storage Class with WFFC binding mode and allowed topology set to
		region-1 > zone-1 > building-1 > level-1 > rack-1 and rack-3
		2. Create 3 PVCs with "RWX" access mode using SC created in step #1.
		3. Wait for PVCs to reach to Bound state.
		4. Create deployment Pod for each PVC with replica count 1.
		5. Verify volume placement. Provisioning should happen as per the allowed topology specified in the SC.
		6. Pods should get created on any AZ.
		5. Perform scale operation on deployment pod. Increase the replica count to 3
		6. Verify CNS metadata for any one PVC and Pod.
		7. Scaleup deployment pod replica count to 5.
		8. Verify scaling operation went smooth.
		9. New Pods should get created on any AZ.
		10. Perform cleanup by deleting Pods, PVC and SC


	*/

	ginkgo.It("TC15", ginkgo.Label(p1, file, vanilla, level5, level2, stable), func() {

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		replica = 3

		/* Get allowed topologies for Storage Class
		(region1 > zone1 > building1 > level1 > rack > rack1/rack3) */
		allowedTopologyForSC := getTopologySelector(topologyAffinityDetails, topologyCategories,
			topologyLength, leafNode, leafNodeTag0, leafNodeTag2)

		pvcItr = 1
		ginkgo.By(fmt.Sprintf("Creating Storage Class with access mode %q and fstype %q", accessmode, nfs4FSType))
		storageclass, pvclaims, err := createStorageClassWithMultiplePVCs(client, namespace, labelsMap, scParameters, diskSize,
			allowedTopologyForSC, bindingModeWffc, false, accessmode, "", pvcItr, false, false)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		defer func() {
			err := client.StorageV1().StorageClasses().Delete(ctx, storageclass.Name, *metav1.NewDeleteOptions(0))
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}()

		defer func() {
			err := fpv.DeletePersistentVolumeClaim(client, pvclaims[0].Name, namespace)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			err = e2eVSphere.waitForCNSVolumeToBeDeleted(pvs[0].Spec.CSI.VolumeHandle)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}()

		pvcItr = 3
		_, pvclaims, err = createStorageClassWithMultiplePVCs(client, namespace, labelsMap, scParameters, diskSize,
			allowedTopologyForSC, bindingModeWffc, false, accessmode, "", pvcItr, true, false)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Create Deployments")
		for i := 0; i < len(pvclaims); i++ {
			deployment, _, err := createVerifyAndScaleDeploymentPods(ctx, client, namespace, replica, false, labelsMap, pvclaim, nil, false, nil)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}
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

		defer func() {
			err = fpv.DeletePersistentVolumeClaim(client, pvclaim2.Name, namespace)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			ginkgo.By("Verify PVs, volumes are deleted from CNS")
			err = e2eVSphere.waitForCNSVolumeToBeDeleted(pv1.Spec.CSI.VolumeHandle)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())

		}()

	})
})
