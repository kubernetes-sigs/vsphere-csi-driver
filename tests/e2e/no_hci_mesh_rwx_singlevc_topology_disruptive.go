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
	"github.com/vmware/govmomi/object"
	"golang.org/x/crypto/ssh"
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
	fss "k8s.io/kubernetes/test/e2e/framework/statefulset"
	admissionapi "k8s.io/pod-security-admission/api"
)

var _ = ginkgo.Describe("[rwx-nohci-singlevc-disruptive] RWX-Topology-NoHciMesh-SingleVc-Disruptive", func() {
	f := framework.NewDefaultFramework("rwx-nohci-singlevc-disruptive")
	f.NamespacePodSecurityEnforceLevel = admissionapi.LevelPrivileged
	var (
		client                  clientset.Interface
		namespace               string
		bindingModeImm          storagev1.VolumeBindingMode
		topologyAffinityDetails map[string][]string
		topologyCategories      []string
		leafNode                int
		leafNodeTag0            int
		topologyLength          int
		scParameters            map[string]string
		accessmode              v1.PersistentVolumeAccessMode
		labelsMap               map[string]string
		replica                 int32
		createPvcItr            int
		createDepItr            int
		pvclaim                 *v1.PersistentVolumeClaim
		allowedTopologies       []v1.TopologySelectorLabelRequirement
		err                     error
		topologySetupType       string
		nodeSelectorTerms       map[string]string
		allowedTopologyForPod   []v1.TopologySelectorLabelRequirement
		depl                    []*appsv1.Deployment
		topologyClusterList     []string
		powerOffHostsList       []string
		nimbusGeneratedK8sVmPwd string
		sshClientConfig         *ssh.ClientConfig
		stsReplicas             int
		nodeList                *v1.NodeList
		noPodsToDeploy          int
		clusterComputeResource  []*object.ClusterComputeResource
		noOfHostToBringDown     int
		podList                 []*v1.Pod
		service                 *v1.Service
		statefulset             *appsv1.StatefulSet
	)

	ginkgo.BeforeEach(func() {
		client = f.ClientSet
		namespace = f.Namespace.Name

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		// connecting to vc
		bootstrap()

		// fetch list of k8s nodes
		nodeList, err = fnodes.GetReadySchedulableNodes(ctx, f.ClientSet)
		framework.ExpectNoError(err, "Unable to find ready and schedulable Node")
		if !(len(nodeList.Items) > 0) {
			framework.Failf("Unable to find ready and schedulable Node")
		}

		sc, err := client.StorageV1().StorageClasses().Get(ctx, defaultNginxStorageClassName, metav1.GetOptions{})
		if err == nil && sc != nil {
			gomega.Expect(client.StorageV1().StorageClasses().Delete(ctx, sc.Name,
				*metav1.NewDeleteOptions(0))).NotTo(gomega.HaveOccurred())
		}

		//global variables
		scParameters = make(map[string]string)
		labelsMap = make(map[string]string)
		accessmode = v1.ReadWriteMany
		bindingModeImm = storagev1.VolumeBindingImmediate
		topologyClusterNames := GetAndExpectStringEnvVar(topologyCluster)
		topologyClusterList = ListTopologyClusterNames(topologyClusterNames)
		nimbusGeneratedK8sVmPwd = GetAndExpectStringEnvVar(nimbusK8sVmPwd)

		// reading testbedingo json
		readVcEsxIpsViaTestbedInfoJson(GetAndExpectStringEnvVar(envTestbedInfoJsonPath))

		//read topology map
		/*
			here 5 is the actual 5-level topology length (ex - region, zone, building, level, rack)
			4 represents the 4th level topology length (ex - region, zone, building, level)
			here rack further has 3 rack levels -> rack1, rack2 and rack3
			0 represents rack1
			1 represents rack2
			2 represents rack3
		*/

		topologyLength, leafNode, leafNodeTag0, _, _ = 5, 4, 0, 1, 2
		topologyMap := GetAndExpectStringEnvVar(envTopologyMap)
		topologyAffinityDetails, topologyCategories = createTopologyMapLevel5(topologyMap)
		allowedTopologies = createAllowedTopolgies(topologyMap)

		//setting map values
		labelsMap["app"] = "test"
		scParameters[scParamFsType] = nfs4FSType

		/* Reading the topology setup type (Level 2 or Level 5), and based on the selected setup type,
		executing the test case on the corresponding setup */
		topologySetupType = GetAndExpectStringEnvVar(envTopologySetupType)

		sshClientConfig = &ssh.ClientConfig{
			User: "root",
			Auth: []ssh.AuthMethod{
				ssh.Password(nimbusGeneratedK8sVmPwd),
			},
			HostKeyCallback: ssh.InsecureIgnoreHostKey(),
		}

		// Get Cluster details
		clusterComputeResource, _, err = getClusterName(ctx, &e2eVSphere)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
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
	   Bring down full site cluster-2
	   SC → Immediate Binding Mode, default values

	   Steps:
	   1. Create SC with Imemdiate Binding mode and set SC with default values
	   2. Create around 10 PVCs with RWX access mode.
	   3. Wait for all PVCs to reach Bound state.
	   4. Create deployment Pods for each PVC with replica count 7 - specify nodeSelectorTerms for any one deployment set.
	   5. Wait for Pods to reach running state
	   6. Verify that all the Pods are spread across all the AZ as specified in the node selector terms.
	   7. Bring Az2 site completely down.
	   10. While one site is completely down, In between, Scale up first 3 deployment sets to 10 replica count.
	   11. Wait for some time and make sure all 10 replicas Pods for the deployment sets are
	   in running state and new PVCs to be in Bound state.
	   12. Scale down next 3 deployment sets to 4 replicas
	   13. Wait for some time and verify that scale down operation went successful.
	   14. Note - verify if we are able to read or write on to the volumes when site is down
	   15. Create statefulset with replica count 3 with pvc created with rwx access mode
	   16. Verify sts pods to be in running/ready and pvc to be in Bound state.
	   17. Create new pvc and attach it to a standalone pod post site recovery.
	   18. Bring up the site which was down.
	   19. Wait for testbed to be back to normal state.
	   20. Verify the k8s nodes status and CSI Pods status.
	   21. Verify that all the workload Pods are in up and running state after site recovery.
	   22. Perform scaling operation again on any deployment set.
	   23. Verify scaling operation went smooth.
	   24. Make sure K8s cluster  is healthy
	   25. Perform cleanup by deleting Pods, PVCs and SC
	*/

	ginkgo.It("When full site Az2 is down", ginkgo.Label(p1, file, vanilla, level5, level2,
		newTest, disruptive), func() {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		var podsNodeSelectorterm *v1.PodList
		var podList []*v1.Pod
		var deploymentList []*appsv1.Deployment
		var service *v1.Service

		// deployment pod and pvc count
		stsReplicas = 3
		replica = 7
		createPvcItr = 10
		createDepItr = 1
		noPodsToDeploy = 2

		ginkgo.By(fmt.Sprintf("Creating Storage Class with access mode %q and fstype %q with "+
			"no allowed topology specified", accessmode, nfs4FSType))
		storageclass, pvclaims, err := createStorageClassWithMultiplePVCs(client, namespace, labelsMap,
			scParameters, diskSize, nil, bindingModeImm, false, accessmode, defaultNginxStorageClassName,
			nil, createPvcItr, false, false)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		defer func() {
			err = client.StorageV1().StorageClasses().Delete(ctx, storageclass.Name, *metav1.NewDeleteOptions(0))
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}()

		defer func() {
			fss.DeleteAllStatefulSets(ctx, client, namespace)
			deleteService(namespace, client, service)
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
		_, err = checkVolumeStateAndPerformCnsVerification(ctx, client, pvclaims, "", "")
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Set allowed topology for pod node selector terms")
		if topologySetupType == "Level2" {
			/* Setting zone1 for pod node selector terms */
			keyValues := []KeyValue{
				{KeyIndex: 0, ValueIndex: 0}, // Key: k8s-region, Value: region1
				{KeyIndex: 1, ValueIndex: 0}, // Key: k8s-zone, Value: zone1
			}
			allowedTopologyForPod, err = getAllowedTopologyForLevel2(allowedTopologies, keyValues)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			nodeSelectorTerms, err = getNodeSelectorMapForDeploymentPods(allowedTopologyForPod)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())

		} else if topologySetupType == "Level5" {
			/* setting rack1(cluster1) as node selector terms for deployment pod creation */
			allowedTopologyForPod = getTopologySelector(topologyAffinityDetails, topologyCategories,
				topologyLength, leafNode, leafNodeTag0)[4:]
			nodeSelectorTerms, err = getNodeSelectorMapForDeploymentPods(allowedTopologyForPod)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}

		ginkgo.By("Create Deployments and specify node selector terms only for deployment1, " +
			"rest all other deployment pod should get spread across all AZ")
		for i := 0; i < len(pvclaims); i++ {
			if i == 0 {
				deploymentList, podsNodeSelectorterm, err = createVerifyAndScaleDeploymentPods(ctx, client, namespace,
					replica, false, labelsMap, pvclaims[i], nodeSelectorTerms, execRWXCommandPod,
					nginxImage, false, nil, createDepItr)
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
				depl = append(depl, deploymentList...)
			} else {
				deploymentList, _, err := createVerifyAndScaleDeploymentPods(ctx, client, namespace, replica, false, labelsMap,
					pvclaims[i], nil, execRWXCommandPod, nginxImage, false, nil, createDepItr)
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
				depl = append(depl, deploymentList...)
			}
		}
		defer func() {
			for i := 0; i < len(depl); i++ {
				framework.Logf("Delete deployment set")
				err = client.AppsV1().Deployments(namespace).Delete(ctx, depl[i].Name, *metav1.NewDeleteOptions(0))
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
			}
		}()

		ginkgo.By("Verify deployment pods are scheduled on a specified node selector terms")
		err = verifyDeploymentPodNodeAffinity(ctx, client, namespace, allowedTopologyForPod, podsNodeSelectorterm)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Bring down all esxi hosts in Az2")
		hostsInCluster := getHostsByClusterName(ctx, clusterComputeResource, topologyClusterList[1])
		powerOffHostsList = powerOffEsxiHostByCluster(ctx, &e2eVSphere, topologyClusterList[1],
			len(hostsInCluster))
		defer func() {
			ginkgo.By("Verifying k8s node status after site recovery")
			err = verifyK8sNodeStatusAfterSiteRecovery(client, ctx, sshClientConfig, nodeList)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}()

		ginkgo.By("Scale up deployment set replica count of dep1, dep2 and dep3 to 10 when Az2 site is down")
		replica = 10
		for i := 0; i < 3; i++ {
			_, _, _ = createVerifyAndScaleDeploymentPods(ctx, client, namespace, replica,
				true, labelsMap, pvclaim, nodeSelectorTerms, execRWXCommandPod, nginxImage, true, depl[i], 0)
		}

		ginkgo.By("Scale down deployment set replica count of dep4, dep5 and dep6 to 4 when Az2 site is down")
		replica = 4
		for i := 3; i < 6; i++ {
			_, _, _ = createVerifyAndScaleDeploymentPods(ctx, client, namespace, replica,
				true, labelsMap, pvclaim, nodeSelectorTerms, execRWXCommandPod, nginxImage, true, depl[i], 0)
		}

		ginkgo.By("Create StatefulSet with replica count 3 with RWX PVC access mode")
		service, _, _ = createStafeulSetAndVerifyPVAndPodNodeAffinty(ctx, client, namespace, true,
			int32(stsReplicas), false, nil, false, false, false, accessmode, storageclass, false, "")

		ginkgo.By("Create new rwx pvc when Az2 site is down")
		createPvcItr = 5
		_, pvclaimsNew, err := createStorageClassWithMultiplePVCs(client, namespace, labelsMap, scParameters,
			diskSize, nil, bindingModeImm, false, accessmode, "", storageclass, createPvcItr, true, false)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		defer func() {
			for i := 0; i < len(pvclaimsNew); i++ {
				pv := getPvFromClaim(client, pvclaimsNew[i].Namespace, pvclaimsNew[i].Name)
				err = fpv.DeletePersistentVolumeClaim(ctx, client, pvclaimsNew[i].Name, namespace)
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
				err = e2eVSphere.waitForCNSVolumeToBeDeleted(pv.Spec.CSI.VolumeHandle)
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
			}
		}()

		ginkgo.By("Create standalone Pods with different read/write permissions and attach it to a newly created rwx pvcs")
		noPodsToDeploy = 2
		for i := 0; i < len(pvclaimsNew); i++ {
			pods, err := createStandalonePodsForRWXVolume(client, ctx, namespace, nil, pvclaimsNew[i], false, execRWXCommandPod,
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

		// Bring up
		ginkgo.By("Bring up all ESXi host which were powered off")
		for i := 0; i < len(powerOffHostsList); i++ {
			powerOnEsxiHostByCluster(powerOffHostsList[i])
		}

		ginkgo.By("Verifying k8s node status after site recovery")
		err = verifyK8sNodeStatusAfterSiteRecovery(client, ctx, sshClientConfig, nodeList)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		// Verify all the workload Pods are in up and running state
		pods, err := client.CoreV1().Pods(namespace).List(ctx, metav1.ListOptions{})
		for _, pod := range pods.Items {
			err = fpod.WaitForPodRunningInNamespace(ctx, client, &pod)
			err = fpod.WaitForPodNotPending(ctx, client, namespace, pod.Name)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}

		ginkgo.By("Scale up all deployment sets to replica count of 12")
		replica = 12
		for i := 0; i < len(depl); i++ {
			_, _, err = createVerifyAndScaleDeploymentPods(ctx, client, namespace, replica,
				true, labelsMap, pvclaim, nodeSelectorTerms, execRWXCommandPod, nginxImage, true, depl[i], 0)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}

		ginkgo.By("Scale down all deployment sets to replica count of 1")
		replica = 1
		for i := 0; i < len(depl); i++ {
			_, _, err = createVerifyAndScaleDeploymentPods(ctx, client, namespace, replica,
				true, labelsMap, pvclaim, nodeSelectorTerms, execRWXCommandPod, nginxImage, true, depl[i], 0)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}
	})

	/*
		TESTCASE-2
		2 sites are partially down (cluster-2 and cluster-3) and 1 site fully healthy (cluster-1)
		SC → Immediate Binding Mode, → all allowed topology specified ,
		→ region-1 > zone-1 > building-1 > level-1 > rack-1/rack-2/rack-3

		Steps:
		1. Create SC with Immediate Binding mode and set SC with all levels of allowed topology.
		2. Create multiple rwx pvcs around 10
		3. Wait for all PVCs to reach Bound state
		4. Create deployment Pods for each rwx created pvc with replica count 1.
		5. Verify cns volume metadata for pod,pvc, and pv
		6. Wait for pods to reach running ready state
		7. Verify that all the Pods are spread across all AZs
		8. Verify cns metadata for each pod,pvc,pv
		9. Bring down partial sites zone2 and zone3
		10. Create new set of rwx pvcs when sites are down.
		11. Attach newly created pvcs to single standalone pods with different read/write permissions.
		12. Create statefulset with rwx access mode with replica count 5
		13. While the sites are down, perform scaleup operation on deployment sets. Increase the replica count to 5 each.
		14. Bring back both the sites which were down i.e. cluster2 and cluster3
		15. Wait for testbed to be back to normal state.
		16. Verify that all the workload Pods are in up and running state.
		17. Verify scaling operation went smooth for deployment set.
		18. Perform scaledown operation on statefulset. Decrease the replica count to 2.
		19. Verify statefulset scale down operation went smooth.
		20. Create new 3 rwx pvc and attach it to 3 standalone pods each.
		21. Verify new pvc creation and pods creation went smooth post site recovery.
		22. Perform cleanup by deleting Pods, PVCs and SC.
	*/

	ginkgo.It("When Az2 and Az3 sites are partially down", ginkgo.Label(p1, file, vanilla,
		level5, level2, newTest, disruptive), func() {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		noOfHostToBringDown = 1
		stsReplicas = 3
		replica = 1
		createPvcItr = 10
		createDepItr = 1

		ginkgo.By(fmt.Sprintf("Creating Storage Class with access mode %q and fstype %q with "+
			"all allowed topologies specified", accessmode, nfs4FSType))
		storageclass, pvclaims, err := createStorageClassWithMultiplePVCs(client, namespace, labelsMap,
			scParameters, diskSize, allowedTopologies, bindingModeImm, false, accessmode,
			defaultNginxStorageClassName, nil, createPvcItr, false, false)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		defer func() {
			err = client.StorageV1().StorageClasses().Delete(ctx, storageclass.Name, *metav1.NewDeleteOptions(0))
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}()

		defer func() {
			fss.DeleteAllStatefulSets(ctx, client, namespace)
			deleteService(namespace, client, service)
		}()

		ginkgo.By("Verify PVC Bound state and CNS side verification")
		_, err = checkVolumeStateAndPerformCnsVerification(ctx, client, pvclaims, "", "")
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
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
			deploymentList, pods, err := createVerifyAndScaleDeploymentPods(ctx, client, namespace, replica, false, labelsMap,
				pvclaims[i], nil, execRWXCommandPod, nginxImage, false, nil, createDepItr)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			depl = append(depl, deploymentList...)

			pv := getPvFromClaim(client, pvclaims[i].Namespace, pvclaims[i].Name)

			ginkgo.By("Verify volume metadata for deployment pod, pvc and pv")
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

		ginkgo.By("Perform partial sitedown on Az2 and Az3")
		powerOffHostsList = powerOffEsxiHostByCluster(ctx, &e2eVSphere, topologyClusterList[1],
			noOfHostToBringDown)
		defer func() {
			ginkgo.By("Bring up ESXi host which were powered off in zone1")
			powerOnEsxiHostByCluster(powerOffHostsList[0])
		}()
		powerOffHostsList1 := powerOffEsxiHostByCluster(ctx, &e2eVSphere, topologyClusterList[2],
			noOfHostToBringDown)
		defer func() {
			ginkgo.By("Bring up  ESXi host which were powered off in zone2")
			powerOnEsxiHostByCluster(powerOffHostsList1[0])
		}()
		powerOffHostsList = append(powerOffHostsList, powerOffHostsList1...)

		ginkgo.By("Create new 5 rwx pvcs when Az2 and Az3 partial sites are down")
		createPvcItr = 5
		_, pvclaimsNew, err := createStorageClassWithMultiplePVCs(client, namespace, labelsMap, scParameters,
			diskSize, nil, bindingModeImm, false, accessmode, "", storageclass, createPvcItr, true, false)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		defer func() {
			for i := 0; i < len(pvclaimsNew); i++ {
				pv := getPvFromClaim(client, pvclaimsNew[i].Namespace, pvclaimsNew[i].Name)
				err = fpv.DeletePersistentVolumeClaim(ctx, client, pvclaimsNew[i].Name, namespace)
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
				err = e2eVSphere.waitForCNSVolumeToBeDeleted(pv.Spec.CSI.VolumeHandle)
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
			}
		}()

		ginkgo.By("Create standalone Pods with different read/write permissions and attach it to a newly created rwx pvcs")
		noPodsToDeploy = 1
		for i := 0; i < len(pvclaimsNew); i++ {
			pods, _ := createStandalonePodsForRWXVolume(client, ctx, namespace, nil, pvclaimsNew[i], false, execRWXCommandPod,
				noPodsToDeploy)
			podList = append(podList, pods...)
		}
		defer func() {
			for i := 0; i < len(podList); i++ {
				ginkgo.By(fmt.Sprintf("Deleting the pod %s in namespace %s", podList[i].Name, namespace))
				err = fpod.DeletePodWithWait(ctx, client, podList[i])
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
			}
		}()

		ginkgo.By("Create StatefulSet with replica count 3 using RWX PVC access mode when Az2 and Az3 sites are down")
		service, statefulset, _ = createStafeulSetAndVerifyPVAndPodNodeAffinty(ctx, client, namespace, true,
			int32(stsReplicas), false, nil, false, false, false, accessmode, storageclass, false, "")

		ginkgo.By("Scale up all deployment sets to replica count of 5")
		replica = 5
		for i := 0; i < len(depl); i++ {
			_, _, _ = createVerifyAndScaleDeploymentPods(ctx, client, namespace, replica,
				true, labelsMap, pvclaim, nodeSelectorTerms, execRWXCommandPod, nginxImage, true, depl[i], 0)
		}

		// Bring up
		ginkgo.By("Bring up all ESXi host which were powered off")
		for i := 0; i < len(powerOffHostsList); i++ {
			powerOnEsxiHostByCluster(powerOffHostsList[i])
		}

		ginkgo.By("Verifying k8s node status after site recovery")
		err = verifyK8sNodeStatusAfterSiteRecovery(client, ctx, sshClientConfig, nodeList)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Verify PVC Bound state and CNS side verification")
		_, err = checkVolumeStateAndPerformCnsVerification(ctx, client, pvclaimsNew, "", "")
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		// Verify all the workload Pods are in up and running state
		pods, err := client.CoreV1().Pods(namespace).List(ctx, metav1.ListOptions{})
		for _, pod := range pods.Items {
			err = fpod.WaitForPodRunningInNamespace(ctx, client, &pod)
			err = fpod.WaitForPodNotPending(ctx, client, namespace, pod.Name)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}

		ginkgo.By("Perform scaledown operation on statefulset. Decrease the replica count to 2")
		scaleDownReplicaCount := 2
		err = performScalingOnStatefulSetAndVerifyPvNodeAffinity(ctx, client, 0,
			int32(scaleDownReplicaCount), statefulset, false, namespace, nil, false, true, false)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Create 3 new rwx pvcs once site is recovered")
		createPvcItr = 3
		_, pvclaimsNewSet, err := createStorageClassWithMultiplePVCs(client, namespace, labelsMap,
			scParameters, diskSize, allowedTopologies, bindingModeImm, false, accessmode, defaultNginxStorageClassName,
			storageclass, createPvcItr, true, false)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Verify PVC Bound state and CNS side verification")
		_, err = checkVolumeStateAndPerformCnsVerification(ctx, client, pvclaimsNewSet, "", "")
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		defer func() {
			for i := 0; i < len(pvclaimsNewSet); i++ {
				pv := getPvFromClaim(client, pvclaimsNewSet[i].Namespace, pvclaimsNewSet[i].Name)
				err = fpv.DeletePersistentVolumeClaim(ctx, client, pvclaimsNewSet[i].Name, namespace)
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
				err = e2eVSphere.waitForCNSVolumeToBeDeleted(pv.Spec.CSI.VolumeHandle)
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
			}
		}()

		ginkgo.By("Create 3 standalone Pods with different read/write permissions and attach it the rwx pvc")
		var podListNew []*v1.Pod
		noPodsToDeploy = 3
		for i := 0; i < len(pvclaimsNewSet); i++ {
			pods, err := createStandalonePodsForRWXVolume(client, ctx, namespace, nil,
				pvclaimsNewSet[i], false, execRWXCommandPod,
				noPodsToDeploy)
			podListNew = append(podListNew, pods...)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())

		}
		defer func() {
			for i := 0; i < len(podListNew); i++ {
				ginkgo.By(fmt.Sprintf("Deleting the pod %s in namespace %s", podListNew[i].Name, namespace))
				err = fpod.DeletePodWithWait(ctx, client, podListNew[i])
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
			}
		}()
	})

})
