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
	"path/filepath"
	"time"

	ginkgo "github.com/onsi/ginkgo/v2"
	"github.com/onsi/gomega"
	appsv1 "k8s.io/api/apps/v1"
	v1 "k8s.io/api/core/v1"
	storagev1 "k8s.io/api/storage/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	clientset "k8s.io/client-go/kubernetes"
	"k8s.io/kubernetes/test/e2e/framework"
	fnodes "k8s.io/kubernetes/test/e2e/framework/node"
	fpod "k8s.io/kubernetes/test/e2e/framework/pod"
	e2eoutput "k8s.io/kubernetes/test/e2e/framework/pod/output"
	fpv "k8s.io/kubernetes/test/e2e/framework/pv"
	admissionapi "k8s.io/pod-security-admission/api"
)

var _ = ginkgo.Describe("[rwx-hci-singlevc-positive] RWX-Topology-HciMesh-SingleVc-Positive", func() {
	f := framework.NewDefaultFramework("rwx-hci-singlevc-positive")
	f.NamespacePodSecurityEnforceLevel = admissionapi.LevelPrivileged
	var (
		client                  clientset.Interface
		namespace               string
		bindingModeWffc         storagev1.VolumeBindingMode
		bindingModeImm          storagev1.VolumeBindingMode
		topologyAffinityDetails map[string][]string
		topologyCategories      []string
		leafNode                int
		leafNodeTag1            int
		leafNodeTag2            int
		topologyLength          int
		scParameters            map[string]string
		accessmode              v1.PersistentVolumeAccessMode
		labelsMap               map[string]string
		replica                 int32
		createPvcItr            int
		createDepItr            int
		pvclaim                 *v1.PersistentVolumeClaim
		depl                    []*appsv1.Deployment
		podList                 []*v1.Pod
		pvs                     []*v1.PersistentVolume
		noPodsToDeploy          int
	)

	ginkgo.BeforeEach(func() {
		client = f.ClientSet
		namespace = f.Namespace.Name

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		// connecting to vc
		bootstrap()

		// fetch list of k8s nodes
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

		//read topology map
		/*
			here 5 is the actual 5-level topology length (ex - region, zone, building, level, rack)
			4 represents the 4th level topology length (ex - region, zone, building, level)
			here rack further has 3 rack levels -> rack1, rack2 and rack3
			0 represents rack1
			1 represents rack2
			2 represents rack3
		*/
		topologyLength, leafNode, _, leafNodeTag1, leafNodeTag2 = 5, 4, 0, 1, 2
		topologyMap := GetAndExpectStringEnvVar(envTopologyMap)
		topologyAffinityDetails, topologyCategories = createTopologyMapLevel5(topologyMap)

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

		// perfrom cleanup of old stale entries of standalone pods if left in the setup
		pods, err := client.CoreV1().Pods(namespace).List(ctx, metav1.ListOptions{})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		if len(pods.Items) != 0 {
			for _, pod := range pods.Items {
				gomega.Expect(client.CoreV1().Pods(namespace).Delete(ctx, pod.Name,
					*metav1.NewDeleteOptions(0))).NotTo(gomega.HaveOccurred())
			}
			podList = nil
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
			depl = nil
		}

		// perfrom cleanup of old stale entries of pv if left in the setup
		pvs, err := client.CoreV1().PersistentVolumes().List(ctx, metav1.ListOptions{})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		if len(pvs.Items) != 0 {
			for _, pv := range pvs.Items {
				gomega.Expect(client.CoreV1().PersistentVolumes().Delete(ctx, pv.Name,
					*metav1.NewDeleteOptions(0))).NotTo(gomega.HaveOccurred())
			}
			pvs = nil
		}
	})

	/*
		TESTCASE-1
		Deployment scale up/down
		SC with WFFC with no Topology

		Steps:
		1. Create storage class with bindingMode set to WFFC. Do not define any allowed topologies.
		2. Create 4 PVCs with RWX access mode.
		3. Create 4 deployments with 3 replicas using the above SC.
		4. Wait for PVC to be created and reach the Bound state, and for the Pods to be created and reach a running state.
		5. Since the SC doesn't specify a specific topology, volume provisioning and Pod placement will occur
		in any available availability zones (AZs).
		6. Perform scaleup/scaledown operation on deployment
		7. Perform cleanup by deleting deployment, PVCs and SC
	*/

	ginkgo.It("Multiple deployment pods with scaling operation "+
		"attached to different rwx pvcs", ginkgo.Label(p0, file, vanilla, level5, level2, newTest), func() {

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		// pvc and deployment pod count
		replica = 3
		createPvcItr = 4
		createDepItr = 1

		ginkgo.By(fmt.Sprintf("Creating Storage Class with access mode %q and fstype %q", accessmode, nfs4FSType))
		storageclass, pvclaims, err := createStorageClassWithMultiplePVCs(client, namespace, labelsMap,
			scParameters, diskSize, nil, bindingModeWffc, false, accessmode,
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
				err = e2eVSphere.waitForCNSVolumeToBeDeleted(pv.Spec.CSI.VolumeHandle)
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
			}
		}()

		ginkgo.By("Create Deployments")
		for i := 0; i < len(pvclaims); i++ {
			deploymentList, _, err := createVerifyAndScaleDeploymentPods(ctx, client, namespace, replica, false, labelsMap,
				pvclaims[i], nil, execRWXCommandPod, nginxImage, false, nil, createDepItr)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			depl = append(depl, deploymentList...)
		}
		defer func() {
			framework.Logf("Delete deployment set")
			for i := 0; i < len(depl); i++ {
				err := client.AppsV1().Deployments(namespace).Delete(ctx, depl[i].Name, *metav1.NewDeleteOptions(0))
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
			}
		}()

		ginkgo.By("Verify PVC Bound state and CNS side verification")
		_, err = checkVolumeStateAndPerformCnsVerification(ctx, client, pvclaims, "", "")
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Scale up deployment1 pods to 5 replicas")
		replica = 5
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
		TESTCASE-2
		PVCs with RWX accessmode and SC with Immediate mode with topology set to
		region-1 > zone-1 > building-1 > level-1 > rack-3

		Steps:
		1. Create a StorageClass with bindingMode set to Immediate. and specify allowed
		topologies for the SC, such as region-1 > zone-1 > building-1 > level-1 > rack-3
		2. Create few PVCs with RWX accessmode.
		3. Wait for the PVCs to be created and reach the Bound state.
		4. Attach 3 pods to each PVC and wait for them to come to running state.
		5. Volume provisioning should happen on the specified availability zone,
		while pods can come up on any accessibility zone.
		6. Perform cleanup by deleting Pods, PVCs and SC
	*/

	ginkgo.It("Multiple standalone pods attached to a rwx "+
		"pvcs", ginkgo.Label(p0, file, vanilla, level5, level2, newTest), func() {

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		// standalone pod and pvc count
		noPodsToDeploy = 3
		createPvcItr = 3

		// Create allowed topologies for Storage Class: region1 > zone1 > building1 > level1 > rack > rack3
		allowedTopologyForSC := getTopologySelector(topologyAffinityDetails, topologyCategories,
			topologyLength, leafNode, leafNodeTag2)

		ginkgo.By(fmt.Sprintf("Creating Storage Class with access mode %q and fstype %q with allowed "+
			"topology set to Az3", accessmode, nfs4FSType))
		storageclass, pvclaims, err := createStorageClassWithMultiplePVCs(client, namespace, labelsMap, scParameters,
			diskSize, allowedTopologyForSC, bindingModeImm, false, accessmode, "", nil, createPvcItr, false, false)
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
				err = e2eVSphere.waitForCNSVolumeToBeDeleted(pv.Spec.CSI.VolumeHandle)
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
			}
		}()

		ginkgo.By("Verify PVC Bound state and CNS side verification")
		pvs, err = checkVolumeStateAndPerformCnsVerification(ctx, client, pvclaims, "", "")
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Create 3 standalone Pods and attached it to each rwx pvcs having " +
			"different read/write permissions set")
		for i := 0; i < len(pvclaims); i++ {
			pods, err := createStandalonePodsForRWXVolume(client, ctx, namespace, nil, pvclaims[i], false, execRWXCommandPod,
				noPodsToDeploy)
			podList = append(podList, pods...)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}
		defer func() {
			for i := 0; i < len(podList); i++ {
				ginkgo.By(fmt.Sprintf("Deleting the pod %s in namespace %s", podList[i].Name, namespace))
				err := fpod.DeletePodWithWait(ctx, client, podList[i])
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
			}
		}()

		ginkgo.By("Verify volume placement, should match with the allowed topology specified")
		_, _, datastoreUrlsRack3, _, err := fetchDatastoreListMap(ctx, client)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		for i := 0; i < len(pvs); i++ {
			isCorrectPlacement := e2eVSphere.verifyPreferredDatastoreMatch(pvs[i].Spec.CSI.VolumeHandle, datastoreUrlsRack3)
			gomega.Expect(isCorrectPlacement).To(gomega.BeTrue(), fmt.Sprintf("Volume provisioning has happened on the wrong "+
				"datastore. Expected 'true', got '%v'", isCorrectPlacement))
		}
	})

	/*
		TESTCASE-3
		Workload creation with higher level topology
		Immediate, region-1 > zone-1 > building-1, storage policy specific to local datastore in rack-1

		Steps:
		1. Create Storage class with bindingmode set to Immediate and allowed topologies set
		to region-1 > zone-1 > building-1. Implement a Storage policy specific to local datastore in rack-1
		2. Create 5 PVCs with RWX access mode.
		3. Create 5 deployments with 1 replicas using PVC created in step #2.
		4. Wait for the PVCs to be created and reach the Bound state, and for the Pods to be
		created and reach a running state.
		5. Verify CSI honors storage policy mentioned in storageclass and provisions volume in rack1.
		6. Volume provisioning happens only on local datastore i.e. vSAN Datastore of
		Cluster-1 (rack-1) where vSAN FS is enabled on cluster1
		7. Validate CNS metadata for each PVC and Pod.
		8. Scale up all deployment pods to different replica set.
		9. Perform cleanup by deleting pod,pvc and sc
	*/

	ginkgo.It("Deployment Pods attached to single RWX PVC with vSAN storage policy of rack-1 "+
		"defined in SC along with top level allowed topology "+
		"specified", ginkgo.Label(p0, file, vanilla, level5, level2, newTest), func() {

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		// deployment pod and pvc count
		replica = 1
		createPvcItr = 5
		createDepItr = 1

		//storage policy read of cluster-1(rack-1)
		storagePolicyName := GetAndExpectStringEnvVar(envVsanDsStoragePolicyCluster1)
		scParameters["storagepolicyname"] = storagePolicyName

		// Get allowed topologies for Storage Class (region1 > zone1 > building1)
		allowedTopologyForSC := getTopologySelector(topologyAffinityDetails, topologyCategories, 3)

		ginkgo.By(fmt.Sprintf("Creating Storage Class with access mode %q and fstype %q", accessmode, nfs4FSType))
		storageclass, pvclaims, err := createStorageClassWithMultiplePVCs(client, namespace, labelsMap,
			scParameters, diskSize, allowedTopologyForSC, bindingModeImm, false, accessmode,
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
				err = e2eVSphere.waitForCNSVolumeToBeDeleted(pv.Spec.CSI.VolumeHandle)
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
			}
		}()

		ginkgo.By("Verify PVC Bound state and CNS side verification")
		pvs, err = checkVolumeStateAndPerformCnsVerification(ctx, client, pvclaims, storagePolicyName, "")
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Create Deployments with replica count 1 and attach it to each rwx pvc")
		for i := 0; i < len(pvclaims); i++ {
			deploymentList, pods, err := createVerifyAndScaleDeploymentPods(ctx, client, namespace, replica, false, labelsMap,
				pvclaims[i], nil, execRWXCommandPod, nginxImage, false, nil, createDepItr)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			depl = append(depl, deploymentList...)

			ginkgo.By("Verify volume metadata for any one deployment pod, pvc and pv")
			pv := getPvFromClaim(client, pvclaims[i].Namespace, pvclaims[i].Name)
			err = waitAndVerifyCnsVolumeMetadata(ctx, pv.Spec.CSI.VolumeHandle, pvclaim, pv, &pods.Items[0])
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}
		defer func() {
			framework.Logf("Delete deployment set")
			for i := 0; i < len(depl); i++ {
				err := client.AppsV1().Deployments(namespace).Delete(ctx, depl[i].Name, *metav1.NewDeleteOptions(0))
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
			}
		}()

		ginkgo.By("Verify volume placement, should match with the allowed topology specified")
		datastoreUrlsRack1, _, _, _, err := fetchDatastoreListMap(ctx, client)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		for i := 0; i < len(pvs); i++ {
			isCorrectPlacement := e2eVSphere.verifyPreferredDatastoreMatch(pvs[i].Spec.CSI.VolumeHandle, datastoreUrlsRack1)
			gomega.Expect(isCorrectPlacement).To(gomega.BeTrue(), fmt.Sprintf("Volume provisioning has happened on the "+
				"wrong datastore. Expected 'true', got '%v'", isCorrectPlacement))
		}

		ginkgo.By("Scale up deployment pods to different replica sets")
		for i := 0; i < len(depl); i++ {
			if i == 0 {
				replica = 5
			} else if i == 1 {
				replica = 3
			} else if i == 2 {
				replica = 7
			} else {
				replica = 4
			}
			_, _, err = createVerifyAndScaleDeploymentPods(ctx, client, namespace, replica,
				true, labelsMap, pvclaim, nil, execRWXCommandPod, nginxImage, true, depl[i], 0)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}
	})

	/*
		TESTCASE-4
		Deployment creation with specific datastore URL in storageclass
		Immediate, region-1 > zone-1 > building-1 > level-1 > rack-2 and rack-3,
		Set specific datastore URL pointing to remote datastore url

		Steps:
		1. Create storage class with bindingmode set to Immediate and allowed topologies
		set to region-1 > zone-1 > building-1 > level-1 > rack-2 and rack-3. Set specific datastore URL pointing
		to remote datastore url
		2. Create 5 PVCs with RWX access mode.
		3. Create 4 deployment pods with replica count 1 and attach it to 4 rwx pvcs.
		4. Wait for the PVCs to be created and reach the Bound state, and for the Pods
		to be created and reach a running state.
		5, Create 2 standalone pods for 5th rwx pvc and verify all standalone pods should reach to running state.
		5. Volume provisioning should happen on the specified availability zone,
		while pods can come up on any accessibility zone.
		6. Verify CSI honors datastoreUrl mentioned in storageclass and provisions volume in that particular datastore.
		7. Perform scaleup/scaledown operation on deployment pods.
		8. Perform cleanup by deleting pods, pvc and sc.
	*/

	ginkgo.It("Deployment pods attached to rwx pvcs using remote "+
		"datastore url", ginkgo.Label(p0, file, vanilla, level5, level2, newTest), func() {

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		// remote datastore url
		remoteDsUrl := GetAndExpectStringEnvVar(envRemoteDatastoreUrl)
		scParameters["datastoreurl"] = remoteDsUrl

		// deployment pod and pvc count
		replica = 1
		createPvcItr = 5
		createDepItr = 1
		noPodsToDeploy = 2

		//Get allowed topologies for Storage Class (region1 > zone1 > building1)
		allowedTopologyForSC := getTopologySelector(topologyAffinityDetails, topologyCategories,
			topologyLength, leafNode, leafNodeTag1, leafNodeTag2)

		ginkgo.By(fmt.Sprintf("Creating Storage Class with access mode %q and fstype %q", accessmode, nfs4FSType))
		storageclass, pvclaims, err := createStorageClassWithMultiplePVCs(client, namespace, labelsMap,
			scParameters, diskSize, allowedTopologyForSC, bindingModeImm, false, accessmode,
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
				err = e2eVSphere.waitForCNSVolumeToBeDeleted(pv.Spec.CSI.VolumeHandle)
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
			}
		}()

		ginkgo.By("Verify PVC Bound state and CNS side verification")
		pvs, err = checkVolumeStateAndPerformCnsVerification(ctx, client, pvclaims, "", remoteDsUrl)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Create Deployments with ")
		for i := 0; i < len(pvclaims); i++ {
			if i != 4 {
				deploymentList, depPod, err := createVerifyAndScaleDeploymentPods(ctx, client, namespace, replica, false, labelsMap,
					pvclaims[i], nil, execRWXCommandPod, nginxImage, false, nil, createDepItr)
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
				depl = append(depl, deploymentList...)

				ginkgo.By("Verify volume metadata for any one deployment pod, pvc and pv")
				pv := getPvFromClaim(client, pvclaims[i].Namespace, pvclaims[i].Name)
				err = waitAndVerifyCnsVolumeMetadata(ctx, pv.Spec.CSI.VolumeHandle, pvclaim, pv, &depPod.Items[0])
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
			} else {
				// create standalone pod for 5th rwx pvc
				pods, err := createStandalonePodsForRWXVolume(client, ctx, namespace, nil, pvclaims[i], false, execRWXCommandPod,
					noPodsToDeploy)
				podList = append(podList, pods...)
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

		ginkgo.By("Verify volume placement, should match with the allowed topology specified")
		datastoreUrlsRack1, _, _, _, err := fetchDatastoreListMap(ctx, client)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		for i := 0; i < len(pvs); i++ {
			isCorrectPlacement := e2eVSphere.verifyPreferredDatastoreMatch(pvs[i].Spec.CSI.VolumeHandle, datastoreUrlsRack1)
			gomega.Expect(isCorrectPlacement).To(gomega.BeTrue(), fmt.Sprintf("Volume provisioning has happened on the "+
				"wrong datastore. Expected 'true', got '%v'", isCorrectPlacement))
		}

		ginkgo.By("Scale up all deployment pods to replica count 5")
		for i := 0; i < len(depl); i++ {
			_, _, err = createVerifyAndScaleDeploymentPods(ctx, client, namespace, replica,
				true, labelsMap, pvclaims[i], nil, execRWXCommandPod, nginxImage, true, depl[i], 0)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}

		ginkgo.By("Scale down deployment2 pods to replica ")
		replica = 2
		_, _, err = createVerifyAndScaleDeploymentPods(ctx, client, namespace, replica,
			true, labelsMap, pvclaims[1], nil, execRWXCommandPod, nginxImage, true, depl[1], 0)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
	})

	/*
		TESTCASE-5
		Static PVC creation

		Steps:
		1. Create 2 file shares using the CNS Create Volume API with the following
		configuration - Read-write mode and ReadOnly Netpermissions.
		2. Create PVs using the volume IDs of the previously created file shares created in step 1.
		3. Create PVCs using the PVs created in step 3.
		4. Wait for the PVs and PVCs to be in the Bound state.
		5. Create a POD using the PVC created in step 4.
		6. Verify volume accessibility and multiple pods can perform operations on volume according to Netpermissions.
		7. Perform cleanup by deleting Pods, PVCs and SC.
	*/

	ginkgo.It("Create static volumes with different read/write permissions "+
		"with policy ste to delete", ginkgo.Label(p0, file, vanilla, level5, level2, newTest), func() {

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		// remote datastore url
		remoteDsUrl := GetAndExpectStringEnvVar(envRemoteDatastoreUrl)

		// fetch file share volume id
		fileShareVolumeId1, err := creatFileShareForSingleVc(remoteDsUrl)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		fileShareVolumeId2, err := creatFileShareForSingleVc(remoteDsUrl)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Creating the PV with policy set to delete with ReadOnlyPermissions")
		pvSpec1 := getPersistentVolumeSpecForFileShare(fileShareVolumeId1,
			v1.PersistentVolumeReclaimDelete, labelsMap, v1.ReadOnlyMany)
		pv1, err := client.CoreV1().PersistentVolumes().Create(ctx, pvSpec1, metav1.CreateOptions{})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		err = e2eVSphere.waitForCNSVolumeToBeCreated(pv1.Spec.CSI.VolumeHandle)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Creating the PV with policy set to delete with ReadWritePermissions")
		pvSpec2 := getPersistentVolumeSpecForFileShare(fileShareVolumeId2,
			v1.PersistentVolumeReclaimDelete, labelsMap, accessmode)
		pv2, err := client.CoreV1().PersistentVolumes().Create(ctx, pvSpec2, metav1.CreateOptions{})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		err = e2eVSphere.waitForCNSVolumeToBeCreated(pv2.Spec.CSI.VolumeHandle)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Creating the PVC where static PV is created with ReadOnly permissions")
		pvc1 := getPersistentVolumeClaimSpecForFileShare(namespace, labelsMap, pv1.Name, v1.ReadOnlyMany)
		pvc1, err = client.CoreV1().PersistentVolumeClaims(namespace).Create(ctx, pvc1, metav1.CreateOptions{})
		var pvclaims1 []*v1.PersistentVolumeClaim
		pvclaims1 = append(pvclaims1, pvc1)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		// Wait for PV and PVC to Bind.
		framework.ExpectNoError(fpv.WaitOnPVandPVC(ctx, client, f.Timeouts, namespace, pv1, pvc1))
		defer func() {
			ginkgo.By("Deleting the PV Claim")
			err := fpv.DeletePersistentVolumeClaim(ctx, client, pvc1.Name, namespace)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}()

		ginkgo.By("Verify PVC Bound state and CNS side verification")
		pvs, err = checkVolumeStateAndPerformCnsVerification(ctx, client, pvclaims1, "", "")
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Creating the PVC where static PV is created with ReadWrite permissions")
		pvc2 := getPersistentVolumeClaimSpecForFileShare(namespace, labelsMap, pv2.Name, accessmode)
		pvc2, err = client.CoreV1().PersistentVolumeClaims(namespace).Create(ctx, pvc2, metav1.CreateOptions{})
		var pvclaims2 []*v1.PersistentVolumeClaim
		pvclaims2 = append(pvclaims2, pvc2)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		// Wait for PV and PVC to Bind.
		framework.ExpectNoError(fpv.WaitOnPVandPVC(ctx, client, f.Timeouts, namespace, pv2, pvc2))
		defer func() {
			ginkgo.By("Deleting the PV Claim")
			err := fpv.DeletePersistentVolumeClaim(ctx, client, pvc2.Name, namespace)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}()

		ginkgo.By("Verify PVC Bound state and CNS side verification")
		pvs, err = checkVolumeStateAndPerformCnsVerification(ctx, client, pvclaims2, "", "")
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Set node selector term for standlone pods so that all pods should get " +
			"created on one single AZ i.e. cluster-3(rack-3)")
		allowedTopologies := getTopologySelector(topologyAffinityDetails, topologyCategories,
			topologyLength, leafNode, leafNodeTag2)
		nodeSelectorTerms, err := getNodeSelectorMapForDeploymentPods(allowedTopologies)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Creating the Pod attached to ReadOnly permission PVC")
		pod1, err := createPod(ctx, client, namespace, nodeSelectorTerms, pvclaims1, false, execRWXCommandPod)
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
		err = verifyVolumeMetadataInCNS(&e2eVSphere, pv1.Spec.CSI.VolumeHandle, pvc1.Name, pv1.Name, pod1.Name)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Creating the Pod attached to ReadWrite permissions pvc")
		pod2, err := createPod(ctx, client, namespace, nil, pvclaims2, false, execRWXCommandPod)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		defer func() {
			ginkgo.By("Deleting the Pod")
			framework.ExpectNoError(fpod.DeletePodWithWait(ctx, client, pod2), "Failed to delete pod", pod2.Name)
		}()

		ginkgo.By("Verify container volume metadata is matching the one in CNS cache")
		err = verifyVolumeMetadataInCNS(&e2eVSphere, pv2.Spec.CSI.VolumeHandle, pvc2.Name, pv2.Name, pod2.Name)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Verify volume placement, should match with the allowed topology specified")
		datastoreUrlsRack1, _, _, _, err := fetchDatastoreListMap(ctx, client)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		isCorrectPlacement := e2eVSphere.verifyPreferredDatastoreMatch(pv1.Spec.CSI.VolumeHandle, datastoreUrlsRack1)
		gomega.Expect(isCorrectPlacement).To(gomega.BeTrue(), fmt.Sprintf("Volume provisioning has happened on the wrong "+
			"datastore. Expected 'true', got '%v'", isCorrectPlacement))

		isCorrectPlacement = e2eVSphere.verifyPreferredDatastoreMatch(pv2.Spec.CSI.VolumeHandle, datastoreUrlsRack1)
		gomega.Expect(isCorrectPlacement).To(gomega.BeTrue(), fmt.Sprintf("Volume provisioning has happened on the wrong "+
			"datastore. Expected 'true', got '%v'", isCorrectPlacement))
	})
})
