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
	"path/filepath"
	"strconv"
	"time"

	ginkgo "github.com/onsi/ginkgo/v2"
	"github.com/onsi/gomega"
	"github.com/vmware/govmomi/object"
	vim25types "github.com/vmware/govmomi/vim25/types"
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

	snapclient "github.com/kubernetes-csi/external-snapshotter/client/v8/clientset/versioned"
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
		allowedTopologies       []v1.TopologySelectorLabelRequirement
		topologyCategories      []string
		leafNode                int
		leafNodeTag0            int
		leafNodeTag1            int
		leafNodeTag2            int
		topologyLength          int
		scParameters            map[string]string
		accessmode              v1.PersistentVolumeAccessMode
		labelsMap               map[string]string
		migratedVms             []vim25types.ManagedObjectReference
		topologySetupType       string
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
		topologyLength, leafNode, leafNodeTag0, leafNodeTag1, leafNodeTag2 = 5, 4, 0, 1, 2
		topologyMap := GetAndExpectStringEnvVar(envTopologyMap)
		topologyAffinityDetails, topologyCategories = createTopologyMapLevel5(topologyMap)
		allowedTopologies = createAllowedTopolgies(topologyMap)

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
				err = client.AppsV1().Deployments(namespace).Delete(ctx, dep.Name, metav1.DeleteOptions{})
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

		var depl []*appsv1.Deployment

		// pvc and deployment pod count
		replica := 3
		createPvcItr := 4
		createDepItr := 1

		ginkgo.By(fmt.Sprintf("Creating Storage Class with access mode %q and fstype %q", accessmode, nfs4FSType))
		storageclass, pvclaims, err := createStorageClassWithMultiplePVCs(client, namespace, labelsMap,
			scParameters, diskSize, nil, bindingModeWffc, false, accessmode,
			"", nil, createPvcItr, false, false)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		defer func() {
			err = client.StorageV1().StorageClasses().Delete(ctx, storageclass.Name, *metav1.NewDeleteOptions(0))
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}()

		defer func() {
			for i := 0; i < len(pvclaims); i++ {
				pv := getPvFromClaim(client, pvclaims[i].Namespace, pvclaims[i].Name)
				err = fpv.DeletePersistentVolumeClaim(ctx, client, pvclaims[i].Name, namespace)
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
				err = e2eVSphere.waitForCNSVolumeToBeDeleted(pv.Spec.CSI.VolumeHandle)
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
			}
		}()

		ginkgo.By("Create Deployments")
		for i := 0; i < len(pvclaims); i++ {
			deploymentList, _, err := createVerifyAndScaleDeploymentPods(ctx, client, namespace, int32(replica), false,
				labelsMap, pvclaims[i], nil, execRWXCommandPod, nginxImage, false, nil, createDepItr)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			depl = append(depl, deploymentList...)
		}
		defer func() {
			framework.Logf("Delete deployment set")
			for i := 0; i < len(depl); i++ {
				err = client.AppsV1().Deployments(namespace).Delete(ctx, depl[i].Name, *metav1.NewDeleteOptions(0))
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
			}
		}()

		ginkgo.By("Verify PVC Bound state and CNS side verification")
		_, err = checkVolumeStateAndPerformCnsVerification(ctx, client, pvclaims, "", "")
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Scale up deployment1 pods to 5 replicas")
		replica = 5
		_, _, err = createVerifyAndScaleDeploymentPods(ctx, client, namespace, int32(replica),
			true, labelsMap, pvclaims[0], nil, execRWXCommandPod, nginxImage, true, depl[0], 0)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Scale down deployment2 pods to 2 replica")
		replica = 2
		_, _, err = createVerifyAndScaleDeploymentPods(ctx, client, namespace, int32(replica),
			true, labelsMap, pvclaims[1], nil, execRWXCommandPod, nginxImage, true, depl[1], 0)
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

		var podList []*v1.Pod

		// standalone pod and pvc count
		noPodsToDeploy := 3
		createPvcItr := 3

		// Create allowed topologies for Storage Class: region1 > zone1 > building1 > level1 > rack > rack3
		allowedTopologyForSC := getTopologySelector(topologyAffinityDetails, topologyCategories,
			topologyLength, leafNode, leafNodeTag2)

		ginkgo.By(fmt.Sprintf("Creating Storage Class with access mode %q and fstype %q with allowed "+
			"topology set to Az3", accessmode, nfs4FSType))
		storageclass, pvclaims, err := createStorageClassWithMultiplePVCs(client, namespace, labelsMap, scParameters,
			diskSize, allowedTopologyForSC, bindingModeImm, false, accessmode, "", nil, createPvcItr, false, false)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		defer func() {
			err = client.StorageV1().StorageClasses().Delete(ctx, storageclass.Name, *metav1.NewDeleteOptions(0))
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}()

		defer func() {
			for i := 0; i < len(pvclaims); i++ {
				pv := getPvFromClaim(client, pvclaims[i].Namespace, pvclaims[i].Name)
				err = fpv.DeletePersistentVolumeClaim(ctx, client, pvclaims[i].Name, namespace)
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
				err = e2eVSphere.waitForCNSVolumeToBeDeleted(pv.Spec.CSI.VolumeHandle)
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
			}
		}()

		ginkgo.By("Verify PVC Bound state and CNS side verification")
		pvs, err := checkVolumeStateAndPerformCnsVerification(ctx, client, pvclaims, "", "")
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
				err = fpod.DeletePodWithWait(ctx, client, podList[i])
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

		var depl []*appsv1.Deployment

		// deployment pod and pvc count
		replica := 1
		createPvcItr := 5
		createDepItr := 1

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
			err = client.StorageV1().StorageClasses().Delete(ctx, storageclass.Name, *metav1.NewDeleteOptions(0))
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}()

		defer func() {
			for i := 0; i < len(pvclaims); i++ {
				pv := getPvFromClaim(client, pvclaims[i].Namespace, pvclaims[i].Name)
				err = fpv.DeletePersistentVolumeClaim(ctx, client, pvclaims[i].Name, namespace)
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
				err = e2eVSphere.waitForCNSVolumeToBeDeleted(pv.Spec.CSI.VolumeHandle)
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
			}
		}()

		ginkgo.By("Verify PVC Bound state and CNS side verification")
		pvs, err := checkVolumeStateAndPerformCnsVerification(ctx, client, pvclaims, storagePolicyName, "")
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Create Deployments with replica count 1 and attach it to each rwx pvc")
		for i := 0; i < len(pvclaims); i++ {
			deploymentList, pods, err := createVerifyAndScaleDeploymentPods(ctx, client, namespace, int32(replica),
				false, labelsMap, pvclaims[i], nil, execRWXCommandPod, nginxImage, false, nil, createDepItr)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			depl = append(depl, deploymentList...)

			ginkgo.By("Verify volume metadata for any one deployment pod, pvc and pv")
			pv := getPvFromClaim(client, pvclaims[i].Namespace, pvclaims[i].Name)
			err = waitAndVerifyCnsVolumeMetadata(ctx, pv.Spec.CSI.VolumeHandle, pvclaims[i], pv, &pods.Items[0])
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}
		defer func() {
			framework.Logf("Delete deployment set")
			for i := 0; i < len(depl); i++ {
				err = client.AppsV1().Deployments(namespace).Delete(ctx, depl[i].Name, *metav1.NewDeleteOptions(0))
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
			_, _, err = createVerifyAndScaleDeploymentPods(ctx, client, namespace, int32(replica),
				true, labelsMap, pvclaims[i], nil, execRWXCommandPod, nginxImage, true, depl[i], 0)
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

		var depl []*appsv1.Deployment
		var podList []*v1.Pod

		// remote datastore url
		remoteDsUrl := GetAndExpectStringEnvVar(envRemoteDatastoreUrl)
		scParameters["datastoreurl"] = remoteDsUrl

		// deployment pod and pvc count
		replica := 1
		createPvcItr := 5
		createDepItr := 1
		noPodsToDeploy := 2

		//Get allowed topologies for Storage Class (region1 > zone1 > building1)
		allowedTopologyForSC := getTopologySelector(topologyAffinityDetails, topologyCategories,
			topologyLength, leafNode, leafNodeTag1, leafNodeTag2)

		ginkgo.By(fmt.Sprintf("Creating Storage Class with access mode %q and fstype %q", accessmode, nfs4FSType))
		storageclass, pvclaims, err := createStorageClassWithMultiplePVCs(client, namespace, labelsMap,
			scParameters, diskSize, allowedTopologyForSC, bindingModeImm, false, accessmode,
			"", nil, createPvcItr, false, false)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		defer func() {
			err = client.StorageV1().StorageClasses().Delete(ctx, storageclass.Name, *metav1.NewDeleteOptions(0))
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}()

		defer func() {
			for i := 0; i < len(pvclaims); i++ {
				pv := getPvFromClaim(client, pvclaims[i].Namespace, pvclaims[i].Name)
				err = fpv.DeletePersistentVolumeClaim(ctx, client, pvclaims[i].Name, namespace)
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
				err = e2eVSphere.waitForCNSVolumeToBeDeleted(pv.Spec.CSI.VolumeHandle)
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
			}
		}()

		ginkgo.By("Verify PVC Bound state and CNS side verification")
		pvs, err := checkVolumeStateAndPerformCnsVerification(ctx, client, pvclaims, "", remoteDsUrl)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Create Deployments with ")
		for i := 0; i < len(pvclaims); i++ {
			if i != 4 {
				deploymentList, depPod, err := createVerifyAndScaleDeploymentPods(ctx, client, namespace, int32(replica),
					false, labelsMap, pvclaims[i], nil, execRWXCommandPod, nginxImage, false, nil, createDepItr)
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
				depl = append(depl, deploymentList...)

				ginkgo.By("Verify volume metadata for any one deployment pod, pvc and pv")
				pv := getPvFromClaim(client, pvclaims[i].Namespace, pvclaims[i].Name)
				err = waitAndVerifyCnsVolumeMetadata(ctx, pv.Spec.CSI.VolumeHandle, pvclaims[i], pv, &depPod.Items[0])
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
				err = client.AppsV1().Deployments(namespace).Delete(ctx, depl[i].Name, *metav1.NewDeleteOptions(0))
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
			}
		}()
		defer func() {
			for i := 0; i < len(podList); i++ {
				ginkgo.By(fmt.Sprintf("Deleting the pod %s in namespace %s", podList[i].Name, namespace))
				err = fpod.DeletePodWithWait(ctx, client, podList[i])
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
			_, _, err = createVerifyAndScaleDeploymentPods(ctx, client, namespace, int32(replica),
				true, labelsMap, pvclaims[i], nil, execRWXCommandPod, nginxImage, true, depl[i], 0)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}

		ginkgo.By("Scale down deployment2 pods to replica ")
		replica = 2
		_, _, err = createVerifyAndScaleDeploymentPods(ctx, client, namespace, int32(replica),
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
			err = fpv.DeletePersistentVolumeClaim(ctx, client, pvc1.Name, namespace)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}()

		ginkgo.By("Verify PVC Bound state and CNS side verification")
		_, err = checkVolumeStateAndPerformCnsVerification(ctx, client, pvclaims1, "", "")
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
			err = fpv.DeletePersistentVolumeClaim(ctx, client, pvc2.Name, namespace)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}()

		ginkgo.By("Verify PVC Bound state and CNS side verification")
		_, err = checkVolumeStateAndPerformCnsVerification(ctx, client, pvclaims2, "", "")
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

	/*
		TESTCASE-6
		Expose pvc created from snapshot to different AZ

		Steps:
		1. Create a SC say sc1 with AZ1 affinity and one more say sc2
			with AZ3 affinity and both SCs should point to remote ds.
		2. Create a PVC with sc1 say pvc1 on remote ds and wait for it to be bound
		3. Create a pod in AZ1 with pvc1 say pod1
		   and wait for it be ready and verify pvc1 is accessible in pod1
		4. Write some data to pvc1
		5. Take snapshot of pvc1 say snap1
		6. Create a PVC say pvc2 (on remote ds) from snap1 and sc2
		7. Create pod in AZ 3 say pod 2 with pvc2 and wait for it be ready and verify pvc2 is accessible in pod2
		8. Verify data from step 4 in pvc2
		9. Verify pvc1 is accessible in pod1
		10.Cleanup pod, pvc, snaps and sc created in the test.
	*/

	ginkgo.It("Expose pvc created from snapshot to different"+
		" AZ", ginkgo.Label(p0, block, vanilla, level5, newTest), func() {

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		var err error
		var snapc *snapclient.Clientset
		var pandoraSyncWaitTime int
		remoteDsUrl := GetAndExpectStringEnvVar(envRemoteHCIDsUrl)

		scParameters = map[string]string{}
		scParameters[scParamStoragePolicyName] = GetAndExpectStringEnvVar(envStoragePolicyNameForHCIRemoteDatastores)
		if os.Getenv(envPandoraSyncWaitTime) != "" {
			pandoraSyncWaitTime, err = strconv.Atoi(os.Getenv(envPandoraSyncWaitTime))
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		} else {
			pandoraSyncWaitTime = defaultPandoraSyncWaitTime
		}

		nodes, err := client.CoreV1().Nodes().List(ctx, metav1.ListOptions{})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		//Get allowed topologies for Storage Class (region1 > zone1 > building1 > level1 > rack3)
		allowedTopologyForSC1 := getTopologySelector(topologyAffinityDetails, topologyCategories,
			topologyLength, leafNode, leafNodeTag0)
		allowedTopologyForSC2 := getTopologySelector(topologyAffinityDetails, topologyCategories,
			topologyLength, leafNode, leafNodeTag2)

		ginkgo.By("Create storageclass with rack-1 topology" +
			" and dsurl pointing to remote datastore")
		sc1Spec := getVSphereStorageClassSpec("sc-az1", scParameters, allowedTopologyForSC1, "", "", false)
		sc1, err := client.StorageV1().StorageClasses().Create(ctx, sc1Spec, metav1.CreateOptions{})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Create storageclass with rack-3 topology" +
			" and dsurl pointing to remote datastore")
		sc2Spec := getVSphereStorageClassSpec("sc-az3", scParameters, allowedTopologyForSC2, "", "", false)
		sc2, err := client.StorageV1().StorageClasses().Create(ctx, sc2Spec, metav1.CreateOptions{})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		defer func() {
			err := client.StorageV1().StorageClasses().Delete(ctx, sc1.Name, *metav1.NewDeleteOptions(0))
			gomega.Expect(err).NotTo(gomega.HaveOccurred())

			err = client.StorageV1().StorageClasses().Delete(ctx, sc2.Name, *metav1.NewDeleteOptions(0))
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}()

		pvclaims2d := [][]*v1.PersistentVolumeClaim{}
		ginkgo.By("Create PVC say pvc1 with above created storageclass")
		pvc1, pv1, err := createPVCAndQueryVolumeInCNS(ctx, client, namespace, nil, "", diskSize, sc1, true)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		pvclaims2d = append(pvclaims2d, []*v1.PersistentVolumeClaim{pvc1})
		volHandle1 := pv1[0].Spec.CSI.VolumeHandle

		ginkgo.By("Verify if VolumeID is created on the remote datastore")
		dsUrlWhereVolumeIsPresent := fetchDsUrl4CnsVol(e2eVSphere, volHandle1)
		framework.Logf("Volume: %s is present on %s", volHandle1, dsUrlWhereVolumeIsPresent)
		e2eVSphere.verifyDatastoreMatch(volHandle1, []string{remoteDsUrl})

		ginkgo.By("Verify volume topology for pvc created")
		_, err = verifyVolumeTopologyForLevel5(pv1[0], topologyAffinityDetails)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		defer func() {
			err := fpv.DeletePersistentVolumeClaim(ctx, client, pvc1.Name, namespace)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			err = e2eVSphere.waitForCNSVolumeToBeDeleted(volHandle1)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}()

		ginkgo.By("Create pod from pvc1")
		pod1, err := createPod(ctx, client, namespace, nil, []*v1.PersistentVolumeClaim{pvc1}, false,
			"")
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Verify that volumes are accessible for all the pods")
		verifyVolMountsInPods(ctx, client, []*v1.Pod{pod1}, pvclaims2d)

		defer func() {
			deletePodsAndWaitForVolsToDetach(ctx, client, []*v1.Pod{pod1}, true)
		}()

		ginkgo.By("Verify pod got created in the specified topology")
		_, err = verifyPodLocationLevel5(pod1, nodes, topologyAffinityDetails)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Create volume snapshot class")
		volumeSnapshotClass, err := createVolumeSnapshotClass(ctx, snapc, deletionPolicy)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		defer func() {
			if vanillaCluster {
				err = snapc.SnapshotV1().VolumeSnapshotClasses().Delete(ctx,
					volumeSnapshotClass.Name, metav1.DeleteOptions{})
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
			}
		}()

		ginkgo.By("Create a volume snapshot say snap1")
		volumeSnapshot, snapshotContent, snapshotCreated,
			snapshotContentCreated, _, _, err := createDynamicVolumeSnapshot(ctx, namespace,
			snapc, volumeSnapshotClass,
			pvc1, volHandle1, diskSize, true)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		defer func() {
			if snapshotContentCreated {
				err = deleteVolumeSnapshotContent(ctx, snapshotContent, snapc,
					pandoraSyncWaitTime)
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
			}

			if snapshotCreated {
				framework.Logf("Deleting volume snapshot")
				deleteVolumeSnapshotWithPandoraWait(ctx, snapc, namespace,
					volumeSnapshot.Name, pandoraSyncWaitTime)

				framework.Logf("Wait till the volume snapshot is deleted")
				err = waitForVolumeSnapshotContentToBeDeletedWithPandoraWait(ctx, snapc,
					*volumeSnapshot.Status.BoundVolumeSnapshotContentName, pandoraSyncWaitTime)
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
			}
		}()

		ginkgo.By("Restore volume from snapshot say pvc2 snap1 with sc2 storageclass")
		pvc2, pv2, _ := verifyVolumeRestoreOperation(ctx, client,
			namespace, sc2, volumeSnapshot, diskSize, false)
		volHandle2 := pv2[0].Spec.CSI.VolumeHandle
		pvclaims2d = append(pvclaims2d, []*v1.PersistentVolumeClaim{pvc2})

		ginkgo.By("Verify if VolumeID is created on the remote datastore")
		dsUrlWhereVolumeIsPresent = fetchDsUrl4CnsVol(e2eVSphere, volHandle2)
		framework.Logf("Volume: %s is present on %s", volHandle2, dsUrlWhereVolumeIsPresent)
		e2eVSphere.verifyDatastoreMatch(volHandle1, []string{remoteDsUrl})

		ginkgo.By("Verify volume topology for pvc created")
		_, err = verifyVolumeTopologyForLevel5(pv2[0], topologyAffinityDetails)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		defer func() {
			err := fpv.DeletePersistentVolumeClaim(ctx, client, pvc2.Name, namespace)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			err = e2eVSphere.waitForCNSVolumeToBeDeleted(volHandle2)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}()

		ginkgo.By("Verify pod got created in the specified topology")
		pod2, err := createPod(ctx, client, namespace, nil, []*v1.PersistentVolumeClaim{pvc2}, false,
			"")
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		defer func() {
			deletePodsAndWaitForVolsToDetach(ctx, client, []*v1.Pod{pod2}, true)
		}()
		ginkgo.By("Verify that volumes are accessible for all the pods")
		verifyVolMountsInPods(ctx, client, []*v1.Pod{pod2}, pvclaims2d)
		_, err = verifyPodLocationLevel5(pod2, nodes, topologyAffinityDetails)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

	})

	/*
	   Topology - Relocate vm between local and remote ds
	   Steps:

	   1. Create a SC with AZ1 affinity and one more with AZ3 affinity
	      respectively and both SCs should point to remote ds.
	   2. Create 5 pvcs each with remote ds in AZ1 and AZ3 and
	      wait for them to be bound using SCs from step 1.
	   3. Create pods with each of the PVCs from step 2 in AZ1 and
	      AZ3 respectively and wait for them to be ready.
	   4. Verify volumes are accessible in each of the pods from step 3
	   5. Storage vmotion remote workers in AZ1 and AZ3 to local ds
	   6. Verify volumes are accessible in each of the PVCs from step2
	   7. Storage vmotion workers from step 4 back to remote ds
	   8. Verify volumes are accessible in each of the PVCs from step2
	   9. Cleanup all the pods, pvcs and SCs created for the test
	*/
	ginkgo.It("Topology - Relocate vm between local and remote"+
		" ds", ginkgo.Label(p0, block, vanilla, hci), func() {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		ginkgo.By("Create a SC which points to remote vsan ds")
		scParameters = map[string]string{}
		var allowedTopologyForSC1, allowedTopologyForSC2 []v1.TopologySelectorLabelRequirement
		var sc1, sc2 *storagev1.StorageClass
		var err error
		scParameters[scParamStoragePolicyName] = GetAndExpectStringEnvVar(envStoragePolicyNameForHCIRemoteDatastores)

		workervms := getWorkerVmMoRefs(ctx, client)
		ginkgo.By("Storage vmotion workers back to remote datastore")
		for _, vm := range workervms {
			e2eVSphere.svmotionVM2DiffDs(ctx, object.NewVirtualMachine(e2eVSphere.Client.Client,
				vm.Reference()),
				GetAndExpectStringEnvVar(envRemoteHCIDsUrl))
			migratedVms = append(migratedVms[:0], migratedVms[1:]...)
		}
		if topologySetupType == "Level2" {

			/* Setting zone1 for pod node selector terms */
			keyValues := []KeyValue{
				{KeyIndex: 0, ValueIndex: 0}, // Key: k8s-region, Value: region1
				{KeyIndex: 1, ValueIndex: 0}, // Key: k8s-zone, Value: zone1
			}
			framework.Logf("keyValues: %v", keyValues)
			allowedTopologyForSC1, err = getAllowedTopologyForLevel2(allowedTopologies, keyValues)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			sc1Spec := getVSphereStorageClassSpec("remote-az1", scParameters, allowedTopologyForSC1, "", "", false)
			sc1, err = client.StorageV1().StorageClasses().Create(ctx, sc1Spec, metav1.CreateOptions{})
			gomega.Expect(err).NotTo(gomega.HaveOccurred())

			/* Setting zone3 for pod node selector terms */
			keyValues = []KeyValue{
				{KeyIndex: 0, ValueIndex: 2}, // Key: k8s-region, Value: region3
				{KeyIndex: 1, ValueIndex: 2}, // Key: k8s-zone, Value: zone3
			}

			allowedTopologyForSC2, err = getAllowedTopologyForLevel2(allowedTopologies, keyValues)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			sc2Spec := getVSphereStorageClassSpec("remote-az3", scParameters,
				allowedTopologyForSC2, "", "", false)
			sc2, err = client.StorageV1().StorageClasses().Create(ctx, sc2Spec,
				metav1.CreateOptions{})
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		} else if topologySetupType == "Level5" {
			allowedTopologyForSC1 = getTopologySelector(topologyAffinityDetails, topologyCategories,
				topologyLength, leafNode, leafNodeTag0)
			sc1Spec := getVSphereStorageClassSpec("remote-az1", scParameters, allowedTopologyForSC1, "", "", false)
			sc1, err = client.StorageV1().StorageClasses().Create(ctx, sc1Spec, metav1.CreateOptions{})
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			allowedTopologyForSC2 = getTopologySelector(topologyAffinityDetails, topologyCategories,
				topologyLength, leafNode, leafNodeTag2)
			sc2Spec := getVSphereStorageClassSpec("remote-az3", scParameters, allowedTopologyForSC2, "", "", false)
			sc2, err = client.StorageV1().StorageClasses().Create(ctx, sc2Spec, metav1.CreateOptions{})
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}

		defer func() {
			err := client.StorageV1().StorageClasses().Delete(ctx, sc1.Name, *metav1.NewDeleteOptions(0))
			gomega.Expect(err).NotTo(gomega.HaveOccurred())

			err = client.StorageV1().StorageClasses().Delete(ctx, sc2.Name, *metav1.NewDeleteOptions(0))
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}()

		nodes, err := client.CoreV1().Nodes().List(ctx, metav1.ListOptions{})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		scs := []*storagev1.StorageClass{}
		scs = append(scs, sc1, sc2)
		k := 0
		pvcs := []*v1.PersistentVolumeClaim{}
		pvclaims2d := [][]*v1.PersistentVolumeClaim{}
		ginkgo.By("create 5 pvcs with each storageclass on remote vsan ds")
		for i := 0; i < 10 && k < 2; i++ {
			pvc, err := createPVC(ctx, client, namespace, nil, "", scs[k], "")
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			pvcs = append(pvcs, pvc)
			pvclaims2d = append(pvclaims2d, []*v1.PersistentVolumeClaim{pvc})
			if i == 5 || i == 10 {
				k += 1
			}
		}

		ginkgo.By("Wait for pvcs to be bound")
		pvs, err := fpv.WaitForPVClaimBoundPhase(ctx, client, pvcs, framework.ClaimProvisionTimeout)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		defer func() {
			for i, pvc := range pvcs {
				err = fpv.DeletePersistentVolumeClaim(ctx, client, pvc.Name, namespace)
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
				err = e2eVSphere.waitForCNSVolumeToBeDeleted(pvs[i].Spec.CSI.VolumeHandle)
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
			}
		}()

		ginkgo.By("Attach a pod to each of the PVCs created earlier")
		ginkgo.By("Wait for all pods to be running and verify that the respective pvcs are accessible")
		pods := createMultiplePods(ctx, client, pvclaims2d, true)

		defer func() {
			deletePodsAndWaitForVolsToDetach(ctx, client, pods, true)
		}()

		for i := 0; i < 10; i++ {
			_, err = verifyVolumeTopologyForLevel5(pvs[i], topologyAffinityDetails)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			_, err = verifyPodLocationLevel5(pods[i], nodes, topologyAffinityDetails)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}

		ginkgo.By("Storage vmotion workers to local vsan datastore")
		for _, vm := range workervms {
			e2eVSphere.svmotionVM2DiffDs(ctx, object.NewVirtualMachine(e2eVSphere.Client.Client,
				vm.Reference()),
				GetAndExpectStringEnvVar(envSharedDatastoreURL))
			migratedVms = append(migratedVms, vm)
		}

		ginkgo.By("Verify that volumes are accessible for all the pods")
		verifyVolMountsInPods(ctx, client, pods, pvclaims2d)

		ginkgo.By("Storage vmotion workers back to remote datastore")
		for _, vm := range workervms {
			e2eVSphere.svmotionVM2DiffDs(ctx, object.NewVirtualMachine(e2eVSphere.Client.Client,
				vm.Reference()),
				GetAndExpectStringEnvVar(envRemoteHCIDsUrl))
			migratedVms = append(migratedVms[:0], migratedVms[1:]...)
		}

		ginkgo.By("Verify that volumes are accessible for all the pods that belong to remote workers")
		verifyVolMountsInPods(ctx, client, pods, pvclaims2d)

		for i := 0; i < 10; i++ {
			_, err = verifyVolumeTopologyForLevel5(pvs[i], topologyAffinityDetails)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			_, err = verifyPodLocationLevel5(pods[i], nodes, topologyAffinityDetails)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}

	})
})
