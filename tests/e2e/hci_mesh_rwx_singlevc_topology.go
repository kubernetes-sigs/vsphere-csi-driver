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
	fpv "k8s.io/kubernetes/test/e2e/framework/pv"
	admissionapi "k8s.io/pod-security-admission/api"
)

var _ = ginkgo.Describe("[hci-mesh-rwx-topology] Hci-Mesh-Topology-SingleVc", func() {
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
		leafNodeTag2            int
		topologyLength          int
		scParameters            map[string]string
		accessmode              v1.PersistentVolumeAccessMode
		labelsMap               map[string]string
		replica                 int32
		createPvcItr            int
		createDepItr            int
		pvclaim                 *v1.PersistentVolumeClaim
		pv                      *v1.PersistentVolume
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
		//bindingModeImm = storagev1.VolumeBindingImmediate

		//read topology map
		topologyLength, leafNode, _, _, leafNodeTag2 = 5, 4, 0, 1, 2
		topologyMap := GetAndExpectStringEnvVar(topologyMap)
		topologyAffinityDetails, topologyCategories = createTopologyMapLevel5(topologyMap,
			topologyLength)

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
		"attached to rwx pvcs", ginkgo.Label(p0, file, vanilla, level5, level2, newTest), func() {

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		// variable declaration
		var depl []*appsv1.Deployment

		// pvc and deployment pod count
		replica = 3
		createPvcItr = 4
		createDepItr = 4

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
				pv = getPvFromClaim(client, pvclaims[i].Namespace, pvclaims[i].Name)
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

	ginkgo.It("Multiple standalone pods attached to a multiple rwx "+
		"pvcs", ginkgo.Label(p0, file, vanilla, level5, level2, newTest), func() {

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		// standalone pod and pvc count
		noPodsToDeploy := 3
		createPvcItr = 3

		//variable declaration
		var podList []*v1.Pod
		var err error

		// Create allowed topologies for Storage Class: region1 > zone1 > building1 > level1 > rack > rack3
		allowedTopologyForSC := getTopologySelector(topologyAffinityDetails, topologyCategories,
			topologyLength, leafNode, leafNodeTag2)

		ginkgo.By(fmt.Sprintf("Creating Storage Class with access mode %q and fstype %q with allowed "+
			"topology set to AZ3", accessmode, nfs4FSType))
		storageclass, pvclaims, err := createStorageClassWithMultiplePVCs(client, namespace, labelsMap, scParameters,
			diskSize, allowedTopologyForSC, bindingModeImm, false, accessmode, "", nil, createPvcItr, false, false)
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
		_, err = checkVolumeStateAndPerformCnsVerification(ctx, client, pvclaims, "", "")
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Create 3 standalone Pods using the same PVC with different read/write permissions")
		for i := 0; i < len(pvclaims); i++ {
			podList, err = createStandalonePodsForRWXVolume(client, ctx, namespace, nil, pvclaims[i], false, execRWXCommandPod,
				noPodsToDeploy)
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
		isCorrectPlacement := e2eVSphere.verifyPreferredDatastoreMatch(pv.Spec.CSI.VolumeHandle, datastoreUrlsRack3)
		gomega.Expect(isCorrectPlacement).To(gomega.BeTrue(), fmt.Sprintf("Volume provisioning has happened on the wrong "+
			"datastore. Expected 'true', got '%v'", isCorrectPlacement))
	})
})
