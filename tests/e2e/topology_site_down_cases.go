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
		noOfHostToBringDown     int
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
		//topologyCluster := GetAndExpectStringEnvVar(topologyClusterNames)
		topologyCluster := "cluster-1,cluster-2,cluster-3"
		topologyClusterList = ListTopologyClusterNames(topologyCluster)
		readVcEsxIpsViaTestbedInfoJson(GetAndExpectStringEnvVar(envTestbedInfoJsonPath))
		//csiNs = GetAndExpectStringEnvVar(envCSINamespace)
	})

	/*
		TESTCASE-1
		Bring down Partial site A (2 esx in cluster1 + 3 esx in cluster2)
		Steps//
		1. Create SC with waitForFirstConsumer
		2. Create 6 statefulsets each with replica count 3 - nodeSelectorTerms details
		3. Wait for all the workload pods to reach running state
		4. Verify that all the POD's spread across zones
		5. Verify that the PV's have node affinity details
		6. In cluster1 bring down 2 ESX's  that belongs to siteA and in cluster 2 bring down 3 ESX's hosts
		7. wait for some time and verify that all the node VM's are bought up on the ESX which was
		in ON state in the respective clusters
		8. Verify that All the workload POD's are up and running
		9. Verify that the Node affinity details on the PV holds the zone and region  details
		10. scale up any one stateful set to 10 replica's
		11. Wait for some time and make sure all 10 replica's are in running state
		12. Scale down the stateful set to 5 replicas
		13. wait for some time and verify that stateful set used in step 12 has 5 replicas  running
		14. Bring up all the ESX hosts
		15. Wait for testbed to be back to normal
		16. Verify that all the POD's and sts are up and running
		17. Verify All the PV's has proper node affinity details and also make sure POD's are
		running on the same node where respective PVC is created
		18. Verify there were no issue with replica scale up/down and verify pod entry
		in CNS volume-metadata for the volumes associated with the PVC used by stateful sets are updated
		19. Make sure K8s cluster  is healthy
		20. Delete above created STS, PVC's and SC
	*/

	ginkgo.It("Volume provisioning when partial siteA cluster1 and cluster2 hosts are down", func() {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		sts_count = 3
		statefulSetReplicaCount = 3
		noOfHostToBringDown = 1

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
		ginkgo.By("Bring down ESXi's that belongs to Cluster1 and Bring down ESXi's that belongs to Cluster2")
		for i := 0; i < len(topologyClusterList); i++ {
			powerOffHostsList = powerOffEsxiHostByCluster(ctx, &e2eVSphere, topologyClusterList[i],
				noOfHostToBringDown)
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

		/* Verify PV nde affinity and that the pods are running on appropriate nodes
		for each StatefulSet pod */
		ginkgo.By("Verify PV node affinity and that the PODS are running on appropriate node")
		for i := 0; i < len(statefulSets); i++ {
			verifyPVnodeAffinityAndPODnodedetailsForStatefulsetsLevel5(ctx, client,
				statefulSets[i], namespace, allowedTopologies, true)
		}

		for i := 0; i < len(statefulSets); i++ {
			ssPodsBeforeScaleDown := fss.GetPodList(client, statefulSets[i])
			for _, pod := range ssPodsBeforeScaleDown.Items {
				_, pvc := getPvcPvFromPod(ctx, client, namespace, &pod)
				_, err := client.CoreV1().Pods(namespace).Get(ctx, pod.Name, metav1.GetOptions{})
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
				for _, volumespec := range pod.Spec.Volumes {
					if volumespec.PersistentVolumeClaim != nil {
						pv := getPvFromClaim(client, statefulSets[i].Namespace, volumespec.PersistentVolumeClaim.ClaimName)
						// Verify the attached volume match the one in CNS cache
						err := verifyVolumeMetadataInCNS(&e2eVSphere, pv.Spec.CSI.VolumeHandle,
							volumespec.PersistentVolumeClaim.ClaimName, pv.ObjectMeta.Name, pod.Name)
						gomega.Expect(err).NotTo(gomega.HaveOccurred())

						err = waitAndVerifyCnsVolumeMetadata(pv.Spec.CSI.VolumeHandle,
							pvc[1], pv, &pod)
						gomega.Expect(err).NotTo(gomega.HaveOccurred())
					}
				}
			}
		}
		ginkgo.By("Verify k8s cluster is healthy")
		wait4AllK8sNodesToBeUp(ctx, client, nodeList)
		err = waitForAllNodes2BeReady(ctx, client, pollTimeout*4)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
	})

	/*
		TESTCASE-2
		Bring down Partial site B (2 esx in cluster1 + 3 esx in cluster3)
		Steps//
		1. Create SC with waitForFirstConsumer
		2. Create 6 statefulsets each with replica count 3 - nodeSelectorTerms details
		3. Wait for all the workload pods to reach running state
		4. Verify that all the POD's spread across zones
		5. Verify that the PV's have proper node affinity details
		6. In cluster1 bring down 2 ESX's that belongs to site B and in cluster 3 bring down 3 ESX's hosts
		7. wait for some time and verify that all the node VM's are bought up on the
		ESX which was in ON state in the respective clusters
		8. Verify that All the workload POD's are up and running
		9. Verify the Node affinity details on the PV
		10. scale up any one stateful set to 10 replica's
		11. Wait for some time and make sure all 10 replica's are in running state
		12. Scale down the stateful set to 5 replicas
		13. wait for some time and verify that stateful set used in step 12 has 5 replicas  running
		14. Bring up all the ESX hosts
		15. Wait for testbed to be back to normal
		16. Verify that all the POD's and sts are up and running
		17. Verify All the PV's has proper node affinity details and also make
		sure POD's are running on the same node where respective PVC is created
		18. Verify there were no issue with replica scale up/down and verify pod
		entry in CNS volume-metadata for the volumes associated with the PVC used by stateful sets are updated.
		19. Make sure K8s cluster  is healthy
		20. Delete above created STS, PVC's and SC
	*/

	ginkgo.It("Volume provisioning when partial siteB cluster2 and cluster3 hosts are down", func() {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		sts_count = 3
		statefulSetReplicaCount = 3
		noOfHostToBringDown = 1

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
				noOfHostToBringDown)
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

		/* Verify PV nde affinity and that the pods are running on appropriate nodes
		for each StatefulSet pod */
		ginkgo.By("Verify PV node affinity and that the PODS are running on appropriate node")
		for i := 0; i < len(statefulSets); i++ {
			verifyPVnodeAffinityAndPODnodedetailsForStatefulsetsLevel5(ctx, client,
				statefulSets[i], namespace, allowedTopologies, true)
		}

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

		ginkgo.By("Wait for k8s cluster to be healthy")
		wait4AllK8sNodesToBeUp(ctx, client, nodeList)
		err = waitForAllNodes2BeReady(ctx, client, pollTimeout*4)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
	})

	/*
		TESTCASE-3
		Bring down full site A (3 esx in cluster1 + 4 esx in cluster2)
		Steps//
		1. Create SC with waitForFirstConsumer
		2. Create 6 stateful sets each with replica count 3 - nodeSelectorTerms details
		3. Wait for all the workload pods to reach running state
		4. Verify that all the POD's spread across zones
		5. Verify that the PV's have zone and Region details
		6. In cluster1 bring down 3 ESX's  that belongs to siteA and in cluster 2 bring down all 4 ESX's hosts
		7. wait for some time and verify that all the node VM's that are in site A
		bought up on the ESX which was  on same  respective clusters.
		8. The POD's that were created on nodes of zone 2 may reach terminating state
		9. Verify that the Node affinity details on the PV holds the zone and region  details
		10. scale up any one stateful set to 10 replica's
		11. Wait for some time and make sure all 10 replica's are in running state
		12. Scale down the stateful set to 5 replicas
		13. wait for some time and verify that stateful set used in step 12 has 5 replicas  running
		14. Bring up all the ESX hosts
		15. Wait for testbed to be back to normal
		16. Verify that all the POD's and sts are up and running
		17. Verify All the PV's has proper zone/region details and also make sure POD's are
		running on the same node where respective PVC is created
		18. Verify there were no issue with replica scale up/down and verify pod entry
		in CNS volume-metadata for the volumes associated with the PVC used by stateful-sets are updated
		19. Make sure K8s cluster  is healthy
		20. Delete above created STS, PVC's and SC
	*/

	ginkgo.It("Volume provisioning when partial siteC cluster1 and cluster3 hosts are down", func() {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		sts_count = 3
		statefulSetReplicaCount = 3
		noOfHostToBringDown = 1

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
				noOfHostToBringDown)
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

		/* Verify PV nde affinity and that the pods are running on appropriate nodes
		for each StatefulSet pod */
		ginkgo.By("Verify PV node affinity and that the PODS are running on appropriate node")
		for i := 0; i < len(statefulSets); i++ {
			verifyPVnodeAffinityAndPODnodedetailsForStatefulsetsLevel5(ctx, client,
				statefulSets[i], namespace, allowedTopologies, true)
		}

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

		ginkgo.By("Wait for k8s cluster to be healthy")
		wait4AllK8sNodesToBeUp(ctx, client, nodeList)
		err = waitForAllNodes2BeReady(ctx, client, pollTimeout*4)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
	})

	/*
		TESTCASE-4
		Bring down full site B (3 esx in cluster1 + 4 esx in cluster2)
		Steps//
		1. Create SC with waitForFirstConsumer
		2. Create 6 stateful sets each with replica count 3 - nodeSelectorTerms details
		3. Wait for all the workload pods to reach running state
		4. Verify that all the POD's spread across zones
		5. Verify that the PV's has node affinity details
		6. In cluster1 bring down 3 ESX's that belongs to site B and in cluster 3 bring down all 4  ESX's hosts
		7. wait for some time and verify that all the node VM's that are in site B  in
		cluster1 bought up on the ESX which was  on same  respective clusters.
		8. The POD's that were created on nodes of zone 3 may reach terminating state
		9. Verify that the Node affinity details on the PV holds the zone and region  details
		10. Verify that All the workload POD's are up and running
		11. Verify that the Node affinity details on the PV holds proper node affinity details
		12. scale up any one stateful set to 10 replica's
		13. Wait for some time and make sure all 10 replica's are in running state
		14. Scale down the stateful set to 5 replicas
		15. wait for some time and verify that stateful set used in step 12 has 5 replicas  running
		16. Bring up all the ESX hosts
		17. Wait for testbed to be back to normal
		18. Verify that all the POD's and sts are up and running
		19. Verify All the PV's has proper node affinity details and also make sure
		POD's are running on the same node where respective PVC is created
		20. Verify there were no issue with replica scale up/down and verify pod entry in
		CNS volume-metadata for the volumes associated with the PVC used by stateful sets are updated
		21. Make sure K8s cluster  is healthy
		22. Delete above created STS, PVC's and SC
	*/

	ginkgo.It("Volume provisioning when partial siteC cluster1 and cluster3 hosts are down", func() {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		sts_count = 3
		statefulSetReplicaCount = 3
		noOfHostToBringDown = 1

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
				noOfHostToBringDown)
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

		/* Verify PV nde affinity and that the pods are running on appropriate nodes
		for each StatefulSet pod */
		ginkgo.By("Verify PV node affinity and that the PODS are running on appropriate node")
		for i := 0; i < len(statefulSets); i++ {
			verifyPVnodeAffinityAndPODnodedetailsForStatefulsetsLevel5(ctx, client,
				statefulSets[i], namespace, allowedTopologies, true)
		}

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

		ginkgo.By("Wait for k8s cluster to be healthy")
		wait4AllK8sNodesToBeUp(ctx, client, nodeList)
		err = waitForAllNodes2BeReady(ctx, client, pollTimeout*4)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
	})

	/*
		TESTCASE-5
		Bring  zone2 completely down
		Steps//
		1. Create SC with waitForFirstConsumer
		2. Create 6 stateful sets each with replica count 3 - nodeSelectorTerms details
		3. Wait for all the workload pods to reach running state
		4. Verify that all the POD's spread across zones
		5. Verify that the PV's has proper node affinity details
		6. In cluster2 bring down all  ESX's
		7. The POD's that were created on nodes of zone 2 may reach terminating state
		8. Verify that the Node affinity details on the PV should have proper node affinity details
		9. scale up any one stateful set to 10 replica's
		10. Wait for some time and make sure all 10 replica's are in running state
		11. Scale down the stateful set to 5 replicas
		12. wait for some time and verify that stateful set used in step 12 has 5 replicas  running
		13. Bring up all the ESX hosts
		14. Wait for testbed to be back to normal
		15. Verify that all the POD's and sts are up and running
		16. Verify All the PV's has proper node affinity details and also make sure
		POD's are running on the same node where respective PVC is created
		17. Verify there were no issue with replica scale up/down and verify pod entry in
		CNS volume-metadata for the volumes associated with the PVC used by stateful sets are updated
		18. Make sure K8s cluster  is healthy
		19. Delete above created STS, PVC's and SC
	*/

	ginkgo.It("Volume provisioning when partial siteC cluster1 and cluster3 hosts are down", func() {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		sts_count = 3
		statefulSetReplicaCount = 3
		var noOfHostToBringDown = 1

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
				noOfHostToBringDown)
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

		/* Verify PV nde affinity and that the pods are running on appropriate nodes
		for each StatefulSet pod */
		ginkgo.By("Verify PV node affinity and that the PODS are running on appropriate node")
		for i := 0; i < len(statefulSets); i++ {
			verifyPVnodeAffinityAndPODnodedetailsForStatefulsetsLevel5(ctx, client,
				statefulSets[i], namespace, allowedTopologies, true)
		}

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

		ginkgo.By("Wait for k8s cluster to be healthy")
		wait4AllK8sNodesToBeUp(ctx, client, nodeList)
		err = waitForAllNodes2BeReady(ctx, client, pollTimeout*4)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
	})

	/*
		TESTCASE-6
		Bring Zone3 completely down
		Steps//
		1. Create SC with waitForFirstConsumer
		2. Create 6 stateful sets each with replica count 3 - nodeSelectorTerms details
		3. Wait for all the workload pods to reach running state
		4. Verify that all the POD's spread across zones
		5. Verify that the PV's have proper node affinity details
		6. In cluster3 bring down all  ESX's
		7. The POD's that were created on nodes of zone 3 may reach terminating state
		8. Verify that the Node affinity details on the PV holds proper node affinity details of all 5 levels
		9. scale up any one stateful set to 10 replica's
		10. Wait for some time and make sure all 10 replica's are in running state
		11. Scale down the stateful set to 5 replicas
		12. wait for some time and verify that stateful used in step 9 has 5 replicas  running
		13. Bring up all the ESX hosts
		14. Wait for testbed to be back to normal
		15. Verify that all the POD's and sts are up and running
		16. Verify All the PV's has proper node affinity details and also make
		sure POD's are running on the same node where respective PVC is created
		17. Verify there were no issue with replica scale up/down and verify pod entry
		in CNS volume-metadata for the volumes associated with the PVC used by stateful sets are updated
		18. Make sure K8s cluster  is healthy
		19. Delete above created STS, PVC's and SC
	*/

	ginkgo.It("Volume provisioning when partial siteC cluster1 and cluster3 hosts are down", func() {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		sts_count = 3
		statefulSetReplicaCount = 3
		var noOfHostToBringDown = 1
		var powerOffHost []string

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
				noOfHostToBringDown)
			powerOffHost = append(powerOffHost, powerOffHostsList...)
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
		for i := 0; i < len(powerOffHost); i++ {
			powerOnEsxiHostByCluster(powerOffHost[i])
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

		/* Verify PV nde affinity and that the pods are running on appropriate nodes
		for each StatefulSet pod */
		ginkgo.By("Verify PV node affinity and that the PODS are running on appropriate node")
		for i := 0; i < len(statefulSets); i++ {
			verifyPVnodeAffinityAndPODnodedetailsForStatefulsetsLevel5(ctx, client,
				statefulSets[i], namespace, allowedTopologies, true)
		}

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

		ginkgo.By("Wait for k8s cluster to be healthy")
		wait4AllK8sNodesToBeUp(ctx, client, nodeList)
		err = waitForAllNodes2BeReady(ctx, client, pollTimeout*4)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
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
	esxCount int) []string {
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
	for i := 0; i < esxCount; i++ {
		for _, esxInfo := range tbinfo.esxHosts {
			host := hostsInCluster[i].Common.InventoryPath
			hostIp := strings.Split(host, "/")
			if hostIp[6] == esxInfo["ip"] {
				esxHostName := esxInfo["vmName"]
				powerOffHostsList = append(powerOffHostsList, esxHostName)
				err = vMPowerMgmt(tbinfo.user, tbinfo.location, esxHostName, false)
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
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
