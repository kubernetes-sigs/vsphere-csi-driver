/*
	Copyright 2019 The Kubernetes Authors.

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
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/onsi/ginkgo"
	"github.com/onsi/gomega"
	"github.com/vmware/govmomi/object"
	appsv1 "k8s.io/api/apps/v1"
	v1 "k8s.io/api/core/v1"
	storagev1 "k8s.io/api/storage/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	clientset "k8s.io/client-go/kubernetes"
	"k8s.io/kubernetes/test/e2e/framework"
	fnodes "k8s.io/kubernetes/test/e2e/framework/node"
	fpod "k8s.io/kubernetes/test/e2e/framework/pod"
	fss "k8s.io/kubernetes/test/e2e/framework/statefulset"
)

var _ = ginkgo.Describe("[csi-topology-vanilla-level5] Topology-Aware-Provisioning-With-SiteDown-Cases", func() {
	f := framework.NewDefaultFramework("e2e-vsphere-topology-aware-provisioning")
	var (
		client                  clientset.Interface
		namespace               string
		bindingMode             storagev1.VolumeBindingMode
		allowedTopologies       []v1.TopologySelectorLabelRequirement
		topologyAffinityDetails map[string][]string
		topologyCategories      []string
		topologyLength          int
		sts_count               int
		statefulSetReplicaCount int32
		nodeList                *v1.NodeList
		err                     error
		topologyClusterList     []string
		csiNs                   string
		powerOffHostsList       []string
	)
	ginkgo.BeforeEach(func() {
		client = f.ClientSet
		namespace = f.Namespace.Name
		bootstrap()
		nodeList, err = fnodes.GetReadySchedulableNodes(f.ClientSet)
		framework.ExpectNoError(err, "Unable to find ready and schedulable Node")
		if !(len(nodeList.Items) > 0) {
			framework.Failf("Unable to find ready and schedulable Node")
		}
		bindingMode = storagev1.VolumeBindingWaitForFirstConsumer
		topologyLength = 5
		topologyMap := GetAndExpectStringEnvVar(topologyMap)
		topologyAffinityDetails, topologyCategories = createTopologyMapLevel5(topologyMap, topologyLength)
		allowedTopologies = createAllowedTopolgies(topologyMap, topologyLength)
		topologyCluster := GetAndExpectStringEnvVar(topologyClusterNames)
		topologyClusterList = ListTopologyClusterNames(topologyCluster)
		readVcEsxIpsViaTestbedInfoJson(GetAndExpectStringEnvVar(envTestbedInfoJsonPath))
		//csiNs = GetAndExpectStringEnvVar(envCSINamespace)
	})

	/*
		TESTCASE-1
		Distribute Multiple PV's across all the zones (50 Pv's)
		As per the deployment , Below assumptions are made
		cluster 1 - > 2 ESX in site A , 2 ESX in site B
		Cluster 2 - > Site A
		Cluster 3 → Site B

		Steps//
		1. Create SC with volumeBindingMode as WaitForFirstConsumer and has parallel pod
		management policy(SC example 1)
		2. Use the above created storageclass and create 3 stateful with parallel pod-management
		policy and each with replica 20and NodeSelector Terms set to all three countries
		3. Create Stateful set and wait for some time for Pod's to create and to be in running state
		4. Once the POD is in running state verify that POD's are created on all countries as specified in
		nodeAffinity entry
		5. Describe on the PV's and Verify node affinity details
		6. Bring down site B
		7. Wait for some time and verify that all the POD's that were on cluster3 are in
		Terminating state, and nodes that are on cluster 1 comes up on different ESX's in the same
		cluster1 and respective POD's are back to running state .
		8. Scale up First stateful set to 55 replica's and scaledown second stateful set to 5 Replicas
		9. Since 2nd stateful set is scaled down , there will be 5 PVC's without POD linking to it.
		Delete those PVC's
		10. Bring Up Site B
		11. wait for  site B VMs to come up and k8s to be healthy
		12. Verify First stateful set is up with 55 replica's , and 2nd  stateful sets is
		scaled down to 5 succesffully
		13. Bring all ESX's in cluster 2 down
		14. Wait for some time and verify that all the POD's that were on cluster2 are in Terminating state
		15. Scale down First stateful set to 10 replica's
		16. Scale up second stateful set to 55 replica's
		17. Wait for some time for new pod's to get created and few POD's which were in cluster 2
		goes to terminating state.
		18. Verify the node affinity details of PV should be proper
		19. Bring up ESX's in cluster 2
		20. Randomly pick few PVC and check CNS metadata data details
		21. Delete all stateful sets
		22. Delete PVC's and SC
	*/

	ginkgo.It("Volume provisioning when partial siteA cluster1 and cluster2 hosts are down", func() {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		sts_count = 3
		statefulSetReplicaCount = 5

		// Get allowed topologies for Storage Class
		allowedTopologyForSC := getTopologySelector(topologyAffinityDetails, topologyCategories,
			topologyLength)

		// Create SC with WFC BindingMode
		ginkgo.By("Creating Storage Class with WFC Binding Mode and allowed topolgies of 5 levels")
		storageclass, err := createStorageClass(client, nil, allowedTopologyForSC, "",
			bindingMode, false, "nginx-sc")
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		defer func() {
			err := client.StorageV1().StorageClasses().Delete(ctx, storageclass.Name, *metav1.NewDeleteOptions(0))
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}()

		// Creating StatefulSet service
		ginkgo.By("Creating service")
		service := CreateService(namespace, client)
		defer func() {
			deleteService(namespace, client, service)
		}()

		// Create multiple StatefulSets Specs in parallel
		ginkgo.By("Creating multiple StatefulSets specs in parallel")
		statefulSets := createParallelStatefulSetSpec(namespace, sts_count)

		// Trigger multiple StatefulSets creation in parallel.
		ginkgo.By("Trigger multiple StatefulSets creation in parallel")
		var wg sync.WaitGroup
		wg.Add(3)
		for i := 0; i < len(statefulSets); i++ {
			go createParallelStatefulSets(client, namespace, statefulSets[i],
				statefulSetReplicaCount, &wg)
		}
		wg.Wait()

		// Waiting for StatefulSets Pods to be in Ready State
		ginkgo.By("Waiting for StatefulSets Pods to be in Ready State")
		time.Sleep(60 * time.Second)

		// Verify that all parallel triggered StatefulSets Pods creation should be in up and running state
		ginkgo.By("Verify that all parallel triggered StatefulSets Pods creation should be in up and running state")
		for i := 0; i < len(statefulSets); i++ {
			// verify that the StatefulSets pods are in ready state
			fss.WaitForStatusReadyReplicas(client, statefulSets[i], statefulSetReplicaCount)
			gomega.Expect(CheckMountForStsPods(client, statefulSets[i], mountPath)).NotTo(gomega.HaveOccurred())

			// Get list of Pods in each StatefulSet and verify the replica count
			ssPods := GetListOfPodsInSts(client, statefulSets[i])
			gomega.Expect(ssPods.Items).NotTo(gomega.BeEmpty(),
				fmt.Sprintf("Unable to get list of Pods from the Statefulset: %v", statefulSets[i].Name))
			gomega.Expect(len(ssPods.Items) == int(statefulSetReplicaCount)).To(gomega.BeTrue(),
				"Number of Pods in the statefulset should match with number of replicas")
		}

		/* Verify PV nde affinity and that the pods are running on appropriate nodes
		for each StatefulSet pod */
		ginkgo.By("Verify PV node affinity and that the PODS are running on appropriate node")
		for i := 0; i < len(statefulSets); i++ {
			verifyPVnodeAffinityAndPODnodedetailsForStatefulsetsLevel5(ctx, client,
				statefulSets[i], namespace, allowedTopologies, true)
		}

		// Bring down all ESxi Hosts of cluster2
		ginkgo.By("Bring down all ESxi Hosts of cluster2")
		powerOffHostsList = powerOffEsxiHostByCluster(ctx, &e2eVSphere, topologyClusterList[1],
			0, 0)

		csiNs = "vmware-system-csi"

		ginkgo.By("Wait for k8s cluster to be healthy")
		wait4AllK8sNodesToBeUp(ctx, client, nodeList)
		err = waitForAllNodes2BeReady(ctx, client, pollTimeout*4)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		csipods, err := client.CoreV1().Pods(csiNs).List(ctx, metav1.ListOptions{})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		err = fpod.WaitForPodsRunningReady(client, csiNs, int32(csipods.Size()), 0, pollTimeout, nil)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		workloadPods, err := client.CoreV1().Pods(csiNs).List(ctx, metav1.ListOptions{})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		err = fpod.WaitForPodsRunningReady(client, namespace, int32(workloadPods.Size()), 0, pollTimeout, nil)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		/* Verify PV nde affinity and that the pods are running on appropriate nodes
		for each StatefulSet pod */
		ginkgo.By("Verify PV node affinity and that the PODS are running on appropriate node")
		for i := 0; i < len(statefulSets); i++ {
			verifyPVnodeAffinityAndPODnodedetailsForStatefulsetsLevel5(ctx, client,
				statefulSets[i], namespace, allowedTopologies, true)
		}

		// Scale up statefulSets replicas count
		ginkgo.By("Scaleup any one StatefulSets replica")
		statefulSetReplicaCount = 2
		ginkgo.By("Scale down statefulset replica and verify the replica count")
		scaleUpStatefulSetPod(ctx, client, statefulSets[1], namespace, statefulSetReplicaCount, true)
		ssPodsAfterScaleDown := GetListOfPodsInSts(client, statefulSets[1])
		gomega.Expect(len(ssPodsAfterScaleDown.Items) == int(statefulSetReplicaCount)).To(gomega.BeTrue(),
			"Number of Pods in the statefulset should match with number of replicas")

		// Scale down statefulSets replica count
		statefulSetReplicaCount = 2
		ginkgo.By("Scale down statefulset replica count")
		for i := 0; i < len(statefulSets); i++ {
			scaleDownStatefulSetPod(ctx, client, statefulSets[i], namespace, statefulSetReplicaCount, true)
			ssPodsAfterScaleDown := GetListOfPodsInSts(client, statefulSets[i])
			gomega.Expect(len(ssPodsAfterScaleDown.Items) == int(statefulSetReplicaCount)).To(gomega.BeTrue(),
				"Number of Pods in the statefulset should match with number of replicas")

		}

		// Bring up
		ginkgo.By("Bring up all ESXi host which were powered off")
		for i := 0; i < len(powerOffHostsList); i++ {
			powerOnEsxiHostByCluster(powerOffHostsList[i])
		}

		ginkgo.By("Wait for k8s cluster to be healthy")
		wait4AllK8sNodesToBeUp(ctx, client, nodeList)
		err = waitForAllNodes2BeReady(ctx, client, pollTimeout*4)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		csipods, err = client.CoreV1().Pods(csiNs).List(ctx, metav1.ListOptions{})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		err = fpod.WaitForPodsRunningReady(client, csiNs, int32(csipods.Size()), 0, pollTimeout, nil)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		workloadPods, err = client.CoreV1().Pods(csiNs).List(ctx, metav1.ListOptions{})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		err = fpod.WaitForPodsRunningReady(client, namespace, int32(workloadPods.Size()), 0, pollTimeout, nil)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		for i := 0; i < len(statefulSets); i++ {
			ssPodsBeforeScaleDown := fss.GetPodList(client, statefulSets[i])
			//var volumesBeforeScaleDown []string
			for _, sspod := range ssPodsBeforeScaleDown.Items {
				_, err := client.CoreV1().Pods(namespace).Get(ctx, sspod.Name, metav1.GetOptions{})
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
				for _, volumespec := range sspod.Spec.Volumes {
					if volumespec.PersistentVolumeClaim != nil {
						pv := getPvFromClaim(client, statefulSets[i].Namespace, volumespec.PersistentVolumeClaim.ClaimName)
						//volumesBeforeScaleDown = append(volumesBeforeScaleDown, pv.Spec.CSI.VolumeHandle)
						// Verify the attached volume match the one in CNS cache
						err := verifyVolumeMetadataInCNS(&e2eVSphere, pv.Spec.CSI.VolumeHandle,
							volumespec.PersistentVolumeClaim.ClaimName, pv.ObjectMeta.Name, sspod.Name)
						gomega.Expect(err).NotTo(gomega.HaveOccurred())
						// err = waitAndVerifyCnsVolumeMetadata(pv.Spec.CSI.VolumeHandle,
						// 	volumespec.PersistentVolumeClaim, pv, sspod)
						// gomega.Expect(err).NotTo(gomega.HaveOccurred())
					}
				}
			}
		}
	})

	/*
		TESTCASE-2
		Delete one of the containernode + bring down few esx's

		Steps//
		1. Identify the Pod where CSI Provisioner is the leader.
		2. Create Storage class with Wait for first consumer
		3. Bring down all esx in cluster2
		4. Using the above created SC , Create 5 statefulset with parallel POD management policy
		each with 10 replica's
		5. While the Statefulsets is creating PVCs and Pods, kill CSI Provisioner container
		identified in the step 1, where CSI provisioner is the leader.
		6. csi-provisioner in other replica should take the leadership to help provisioning of the volume.
		7. Wait until all PVCs and Pods are created for Statefulsets . Since all esx's down in cluster 2 POD's should come up on cluster 1 and cluster 3
		8. Expect all PVCs for Statefulsets to be in the bound state.
		9. Expect all Pods for Statefulsets to be in the running state
		10. Describe PV and Verify the node affinity rule . Make sure node affinity should contain all 5 levels of topology details
		11. POD should be running in the appropriate nodes
		12. Bring up ESX's in cluster 2
		13. Identify the Pod where CSI Attacher is the leader
		14. Scale down the Statefulsets replica count to 5,
		15. While the Statefulsets is scaling down Pods, kill CSI Attacher container
		identified in the step 11, where CSI Attacher is the leader.
		16. Wait untill the POD count goes down to 5
		17. Delete the PVC's which are not used by POD's
		18. Scale up replica count 20
		19. PVC's, POD's should come up on all three clusters (labeled as countries)
		20. verify node affinity details on PV's should be proper with all 5 level details
		21. Delete statefulset
		22. Delete PVC's and SC
	*/

	ginkgo.It("Volume provisioning when partial siteA cluster1 and cluster2 hosts are down", func() {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		sts_count = 3
		statefulSetReplicaCount = 3
		var noOfHostToBringDownInCluster1 = 1
		var noOfHostToBringDownInCluster2 = 1

		// Get allowed topologies for Storage Class
		allowedTopologyForSC := getTopologySelector(topologyAffinityDetails, topologyCategories,
			topologyLength)

		// Create SC with WFC BindingMode
		ginkgo.By("Creating Storage Class with WFC Binding Mode and allowed topolgies of 5 levels")
		storageclass, err := createStorageClass(client, nil, allowedTopologyForSC, "",
			bindingMode, false, "nginx-sc")
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		defer func() {
			err := client.StorageV1().StorageClasses().Delete(ctx, storageclass.Name, *metav1.NewDeleteOptions(0))
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}()

		// Creating StatefulSet service
		ginkgo.By("Creating service")
		service := CreateService(namespace, client)
		defer func() {
			deleteService(namespace, client, service)
		}()

		// Create multiple StatefulSets Specs in parallel
		ginkgo.By("Creating multiple StatefulSets specs in parallel")
		statefulSets := createParallelStatefulSetSpec(namespace, sts_count)

		// Trigger multiple StatefulSets creation in parallel.
		ginkgo.By("Trigger multiple StatefulSets creation in parallel")
		var wg sync.WaitGroup
		wg.Add(3)
		for i := 0; i < len(statefulSets); i++ {
			go createParallelStatefulSets(client, namespace, statefulSets[i],
				statefulSetReplicaCount, &wg)
		}
		wg.Wait()

		// Waiting for StatefulSets Pods to be in Ready State
		ginkgo.By("Waiting for StatefulSets Pods to be in Ready State")
		time.Sleep(60 * time.Second)

		// Verify that all parallel triggered StatefulSets Pods creation should be in up and running state
		ginkgo.By("Verify that all parallel triggered StatefulSets Pods creation should be in up and running state")
		for i := 0; i < len(statefulSets); i++ {
			// verify that the StatefulSets pods are in ready state
			fss.WaitForStatusReadyReplicas(client, statefulSets[i], statefulSetReplicaCount)
			gomega.Expect(CheckMountForStsPods(client, statefulSets[i], mountPath)).NotTo(gomega.HaveOccurred())

			// Get list of Pods in each StatefulSet and verify the replica count
			ssPods := GetListOfPodsInSts(client, statefulSets[i])
			gomega.Expect(ssPods.Items).NotTo(gomega.BeEmpty(),
				fmt.Sprintf("Unable to get list of Pods from the Statefulset: %v", statefulSets[i].Name))
			gomega.Expect(len(ssPods.Items) == int(statefulSetReplicaCount)).To(gomega.BeTrue(),
				"Number of Pods in the statefulset should match with number of replicas")
		}

		/* Verify PV nde affinity and that the pods are running on appropriate nodes
		for each StatefulSet pod */
		ginkgo.By("Verify PV node affinity and that the PODS are running on appropriate node")
		for i := 0; i < len(statefulSets); i++ {
			verifyPVnodeAffinityAndPODnodedetailsForStatefulsetsLevel5(ctx, client,
				statefulSets[i], namespace, allowedTopologies, true)
		}

		// Bring down 3 ESXi's that belongs to Cluster1 and Bring down 2 ESXi's that belongs to Cluster2
		ginkgo.By("Bring down 3 ESXi's that belongs to Cluster1 and Bring down 2 ESXi's that belongs to Cluster2")
		for i := 0; i < len(topologyClusterList); i++ {
			powerOffHostsList = powerOffEsxiHostByCluster(ctx, &e2eVSphere, topologyClusterList[i],
				noOfHostToBringDownInCluster1, noOfHostToBringDownInCluster2)
			if i == 2 {
				break
			}
		}
		csiNs = "vmware-system-csi"

		ginkgo.By("Wait for k8s cluster to be healthy")
		wait4AllK8sNodesToBeUp(ctx, client, nodeList)
		err = waitForAllNodes2BeReady(ctx, client, pollTimeout*4)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		csipods, err := client.CoreV1().Pods(csiNs).List(ctx, metav1.ListOptions{})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		err = fpod.WaitForPodsRunningReady(client, csiNs, int32(csipods.Size()), 0, pollTimeout, nil)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		workloadPods, err := client.CoreV1().Pods(csiNs).List(ctx, metav1.ListOptions{})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		err = fpod.WaitForPodsRunningReady(client, namespace, int32(workloadPods.Size()), 0, pollTimeout, nil)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		/* Verify PV nde affinity and that the pods are running on appropriate nodes
		for each StatefulSet pod */
		ginkgo.By("Verify PV node affinity and that the PODS are running on appropriate node")
		for i := 0; i < len(statefulSets); i++ {
			verifyPVnodeAffinityAndPODnodedetailsForStatefulsetsLevel5(ctx, client,
				statefulSets[i], namespace, allowedTopologies, true)
		}

		// Scale up statefulSets replicas count
		ginkgo.By("Scaleup any one StatefulSets replica")
		statefulSetReplicaCount = 2
		ginkgo.By("Scale down statefulset replica and verify the replica count")
		scaleUpStatefulSetPod(ctx, client, statefulSets[1], namespace, statefulSetReplicaCount, true)
		ssPodsAfterScaleDown := GetListOfPodsInSts(client, statefulSets[1])
		gomega.Expect(len(ssPodsAfterScaleDown.Items) == int(statefulSetReplicaCount)).To(gomega.BeTrue(),
			"Number of Pods in the statefulset should match with number of replicas")

		// Scale down statefulSets replica count
		statefulSetReplicaCount = 2
		ginkgo.By("Scale down statefulset replica count")
		for i := 0; i < len(statefulSets); i++ {
			scaleDownStatefulSetPod(ctx, client, statefulSets[i], namespace, statefulSetReplicaCount, true)
			ssPodsAfterScaleDown := GetListOfPodsInSts(client, statefulSets[i])
			gomega.Expect(len(ssPodsAfterScaleDown.Items) == int(statefulSetReplicaCount)).To(gomega.BeTrue(),
				"Number of Pods in the statefulset should match with number of replicas")

		}

		// Bring up
		ginkgo.By("Bring up all ESXi host which were powered off")
		for i := 0; i < len(powerOffHostsList); i++ {
			powerOnEsxiHostByCluster(powerOffHostsList[i])
		}

		ginkgo.By("Wait for k8s cluster to be healthy")
		wait4AllK8sNodesToBeUp(ctx, client, nodeList)
		err = waitForAllNodes2BeReady(ctx, client, pollTimeout*4)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		csipods, err = client.CoreV1().Pods(csiNs).List(ctx, metav1.ListOptions{})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		err = fpod.WaitForPodsRunningReady(client, csiNs, int32(csipods.Size()), 0, pollTimeout, nil)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		workloadPods, err = client.CoreV1().Pods(csiNs).List(ctx, metav1.ListOptions{})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		err = fpod.WaitForPodsRunningReady(client, namespace, int32(workloadPods.Size()), 0, pollTimeout, nil)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		for i := 0; i < len(statefulSets); i++ {
			ssPodsBeforeScaleDown := fss.GetPodList(client, statefulSets[i])
			//var volumesBeforeScaleDown []string
			for _, sspod := range ssPodsBeforeScaleDown.Items {
				_, err := client.CoreV1().Pods(namespace).Get(ctx, sspod.Name, metav1.GetOptions{})
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
				for _, volumespec := range sspod.Spec.Volumes {
					if volumespec.PersistentVolumeClaim != nil {
						pv := getPvFromClaim(client, statefulSets[i].Namespace, volumespec.PersistentVolumeClaim.ClaimName)
						//volumesBeforeScaleDown = append(volumesBeforeScaleDown, pv.Spec.CSI.VolumeHandle)
						// Verify the attached volume match the one in CNS cache
						err := verifyVolumeMetadataInCNS(&e2eVSphere, pv.Spec.CSI.VolumeHandle,
							volumespec.PersistentVolumeClaim.ClaimName, pv.ObjectMeta.Name, sspod.Name)
						gomega.Expect(err).NotTo(gomega.HaveOccurred())
						// err = waitAndVerifyCnsVolumeMetadata(pv.Spec.CSI.VolumeHandle,
						// 	volumespec.PersistentVolumeClaim, pv, sspod)
						// gomega.Expect(err).NotTo(gomega.HaveOccurred())
					}
				}
			}
		}
	})
})

func createParallelStatefulSetSpec(namespace string, no_of_sts int) []*appsv1.StatefulSet {
	stss := []*appsv1.StatefulSet{}
	var statefulset *appsv1.StatefulSet
	for i := 0; i < no_of_sts; i++ {
		statefulset = GetStatefulSetFromManifest(namespace)
		statefulset.Name = "thread-" + strconv.Itoa(i) + "-" + statefulset.Name
		statefulset.Spec.PodManagementPolicy = appsv1.ParallelPodManagement
		statefulset.Spec.VolumeClaimTemplates[len(statefulset.Spec.VolumeClaimTemplates)-1].
			Annotations["volume.beta.kubernetes.io/storage-class"] = "nginx-sc"
		stss = append(stss, statefulset)
	}
	return stss
}

func createParallelStatefulSets(client clientset.Interface, namespace string,
	statefulset *appsv1.StatefulSet, replicas int32, wg *sync.WaitGroup) {
	defer wg.Done()
	ginkgo.By("Creating statefulset")
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	framework.Logf(fmt.Sprintf("Creating statefulset %v/%v with %d replicas and selector %+v",
		statefulset.Namespace, statefulset.Name, *(statefulset.Spec.Replicas), statefulset.Spec.Selector))
	_, err := client.AppsV1().StatefulSets(namespace).Create(ctx, statefulset, metav1.CreateOptions{})
	gomega.Expect(err).NotTo(gomega.HaveOccurred())
}

func powerOffEsxiHostByCluster(ctx context.Context, vs *vSphere, clusterName string,
	cluster1EsxCount int, cluster2EsxCount int) []string {
	var powerOffHostsList []string
	var clusterHostlist []*object.ClusterComputeResource
	var hostsInCluster []*object.HostSystem
	clusterLists, _, err := getClusterName(ctx, &e2eVSphere)
	gomega.Expect(err).NotTo(gomega.HaveOccurred())
	for i := 0; i < len(clusterLists); i++ {
		if strings.Contains(clusterLists[i].ComputeResource.Common.InventoryPath, clusterName) {
			clusterHostlist = append(clusterHostlist, clusterLists[i])
			hostsInCluster = getHosts(ctx, clusterHostlist)
		}
	}
	if cluster1EsxCount == 0 && cluster2EsxCount == 0 {
		hostsInCluster = getHosts(ctx, clusterHostlist)
		for i := 0; i < len(hostsInCluster); i++ {
			for _, esxInfo := range tbinfo.esxHosts {
				host := hostsInCluster[i].Common.InventoryPath
				hostIp := strings.Split(host, "/")
				if hostIp[6] == esxInfo["ip"] {
					esxHostName := esxInfo["vmName"]
					powerOffHostsList = append(powerOffHostsList, esxHostName)
					// err = vMPowerMgmt(tbinfo.user, tbinfo.location, esxHostName, false)
					// gomega.Expect(err).NotTo(gomega.HaveOccurred())
					err = waitForHostToBeDown(esxInfo["ip"])
					gomega.Expect(err).NotTo(gomega.HaveOccurred())
				}
			}
		}
	}
	for i := 0; i < cluster1EsxCount; i++ {
		for _, esxInfo := range tbinfo.esxHosts {
			host := hostsInCluster[i].Common.InventoryPath
			hostIp := strings.Split(host, "/")
			if hostIp[6] == esxInfo["ip"] {
				esxHostName := esxInfo["vmName"]
				powerOffHostsList = append(powerOffHostsList, esxHostName)
				// err = vMPowerMgmt(tbinfo.user, tbinfo.location, esxHostName, false)
				// gomega.Expect(err).NotTo(gomega.HaveOccurred())
				err = waitForHostToBeDown(esxInfo["ip"])
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
			}
		}
	}
	return powerOffHostsList
}

func powerOnEsxiHostByCluster(hostToPowerOn string) {
	for _, esxInfo := range tbinfo.esxHosts {
		if hostToPowerOn == esxInfo["ip"] {
			esxHostName := esxInfo["vmName"]
			err := vMPowerMgmt(tbinfo.user, tbinfo.location, esxHostName, true)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())

		}
		err := waitForHostToBeUp(hostToPowerOn)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
	}
}
