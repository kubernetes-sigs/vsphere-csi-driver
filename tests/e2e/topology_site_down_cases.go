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
	"strings"
	"sync"
	"time"

	"github.com/onsi/ginkgo"
	"github.com/onsi/gomega"
	"github.com/vmware/govmomi/object"
	vimtypes "github.com/vmware/govmomi/vim25/types"
	"golang.org/x/crypto/ssh"
	v1 "k8s.io/api/core/v1"
	storagev1 "k8s.io/api/storage/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	clientset "k8s.io/client-go/kubernetes"
	"k8s.io/kubernetes/test/e2e/framework"
	fnodes "k8s.io/kubernetes/test/e2e/framework/node"
	fpod "k8s.io/kubernetes/test/e2e/framework/pod"
	fssh "k8s.io/kubernetes/test/e2e/framework/ssh"
	fss "k8s.io/kubernetes/test/e2e/framework/statefulset"
)

var _ = ginkgo.Describe("[csi-topology-sitedown-level5] Topology-Aware-Provisioning-With-SiteDown-Cases", func() {
	f := framework.NewDefaultFramework("e2e-vsphere-topology-aware-provisioning")
	var (
		client                  clientset.Interface
		namespace               string
		bindingMode             storagev1.VolumeBindingMode
		allowedTopologies       []v1.TopologySelectorLabelRequirement
		topologyAffinityDetails map[string][]string
		topologyCategories      []string
		sts_count               int
		statefulSetReplicaCount int32
		nodeList                *v1.NodeList
		topologyClusterList     []string
		powerOffHostsList       []string
		noOfHostToBringDown     int
		topologyLength          int
		sshClientConfig         *ssh.ClientConfig
	)
	ginkgo.BeforeEach(func() {
		client = f.ClientSet
		namespace = f.Namespace.Name
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		bootstrap()
		sc, err := client.StorageV1().StorageClasses().Get(ctx, defaultNginxStorageClassName, metav1.GetOptions{})
		if err == nil && sc != nil {
			gomega.Expect(client.StorageV1().StorageClasses().Delete(ctx, sc.Name, *metav1.NewDeleteOptions(0))).
				NotTo(gomega.HaveOccurred())
		}
		nodeList, err = fnodes.GetReadySchedulableNodes(f.ClientSet)
		framework.ExpectNoError(err, "Unable to find ready and schedulable Node")
		if !(len(nodeList.Items) > 0) {
			framework.Failf("Unable to find ready and schedulable Node")
		}
		topologyLength = 5
		bindingMode = storagev1.VolumeBindingWaitForFirstConsumer
		topologyMap := GetAndExpectStringEnvVar(topologyMap)
		topologyAffinityDetails, topologyCategories = createTopologyMapLevel5(topologyMap, topologyLength)
		allowedTopologies = createAllowedTopolgies(topologyMap, topologyLength)
		topologyClusterNames := GetAndExpectStringEnvVar(topologyCluster)
		topologyClusterList = ListTopologyClusterNames(topologyClusterNames)
		readVcEsxIpsViaTestbedInfoJson(GetAndExpectStringEnvVar(envTestbedInfoJsonPath))
		sshClientConfig = &ssh.ClientConfig{
			User: "root",
			Auth: []ssh.AuthMethod{
				ssh.Password(k8sVmPasswd),
			},
			HostKeyCallback: ssh.InsecureIgnoreHostKey(),
		}
	})

	ginkgo.AfterEach(func() {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		ginkgo.By(fmt.Sprintf("Deleting all statefulsets in namespace: %v", namespace))
		fss.DeleteAllStatefulSets(client, namespace)
		ginkgo.By(fmt.Sprintf("Deleting service nginx in namespace: %v", namespace))
		err := client.CoreV1().Services(namespace).Delete(ctx, servicename, *metav1.NewDeleteOptions(0))
		if !apierrors.IsNotFound(err) {
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}
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

	ginkgo.It("Volume provisioning when partial sites zone1 and zone2 hosts are down", func() {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		sts_count = 3
		statefulSetReplicaCount = 5
		noOfHostToBringDown = 1
		var ssPods *v1.PodList

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
		statefulSets := createParallelStatefulSetSpec(namespace, sts_count, statefulSetReplicaCount)

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
		time.Sleep(pollTimeoutSixMin)

		// Verify that all parallel triggered StatefulSets Pods creation should be in up and running state
		ginkgo.By("Verify that all parallel triggered StatefulSets Pods creation should be in up and running state")
		for i := 0; i < len(statefulSets); i++ {
			// verify that the StatefulSets pods are in ready state
			fss.WaitForStatusReadyReplicas(client, statefulSets[i], statefulSetReplicaCount)
			gomega.Expect(CheckMountForStsPods(client, statefulSets[i], mountPath)).NotTo(gomega.HaveOccurred())

			// Get list of Pods in each StatefulSet and verify the replica count
			ssPods = GetListOfPodsInSts(client, statefulSets[i])
			gomega.Expect(ssPods.Items).NotTo(gomega.BeEmpty(),
				fmt.Sprintf("Unable to get list of Pods from the Statefulset: %v", statefulSets[i].Name))
			gomega.Expect(len(ssPods.Items) == int(statefulSetReplicaCount)).To(gomega.BeTrue(),
				"Number of Pods in the statefulset should match with number of replicas")
		}
		defer func() {
			ginkgo.By(fmt.Sprintf("Deleting all statefulsets in namespace: %v", namespace))
			fss.DeleteAllStatefulSets(client, namespace)
		}()

		/* Verify PV nde affinity and that the pods are running on appropriate nodes
		for each StatefulSet pod */
		ginkgo.By("Verify PV node affinity and that the PODS are running on appropriate node")
		for i := 0; i < len(statefulSets); i++ {
			verifyPVnodeAffinityAndPODnodedetailsForStatefulsetsLevel5(ctx, client,
				statefulSets[i], namespace, allowedTopologies, true)
		}

		// Bring down 1 ESXi's that belongs to Cluster1 and Bring down 1 ESXi's that belongs to Cluster2
		ginkgo.By("Bring down 1 ESXi host that belongs to Cluster1 and Bring down 1 ESXi " +
			"host that belongs to Cluster2")
		powerOffHostsList = powerOffEsxiHostByCluster(ctx, &e2eVSphere, topologyClusterList[0],
			noOfHostToBringDown)
		powerOffHostsList1 := powerOffEsxiHostByCluster(ctx, &e2eVSphere, topologyClusterList[1],
			noOfHostToBringDown)
		powerOffHostsList = append(powerOffHostsList, powerOffHostsList1...)
		defer func() {
			ginkgo.By("Bring up all ESXi host which were powered off")
			for i := 0; i < len(powerOffHostsList); i++ {
				powerOnEsxiHostByCluster(powerOffHostsList[i])
			}
		}()

		// Wait for k8s cluster to be healthy
		ginkgo.By("Wait for k8s cluster to be healthy")
		wait4AllK8sNodesToBeUp(ctx, client, nodeList)
		err = waitForAllNodes2BeReady(ctx, client, pollTimeout*4)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		// Verify all the workload Pods are in up and running state
		ginkgo.By("Verify all the workload Pods are in up and running state")
		ssPods = fss.GetPodList(client, statefulSets[0])
		for _, pod := range ssPods.Items {
			err := fpod.WaitForPodRunningInNamespace(client, &pod)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}

		/* Verify PV nde affinity and that the pods are running on appropriate nodes
		for each StatefulSet pod */
		ginkgo.By("Verify PV node affinity and that the PODS are running on appropriate node")
		for i := 0; i < len(statefulSets); i++ {
			verifyPVnodeAffinityAndPODnodedetailsForStatefulsetsLevel5(ctx, client,
				statefulSets[i], namespace, allowedTopologies, true)
		}

		// Scale up statefulSets replicas count
		ginkgo.By("Scaleup any one StatefulSets replica")
		statefulSetReplicaCount = 3
		ginkgo.By("Scale down statefulset replica and verify the replica count")
		scaleUpStatefulSetPod(ctx, client, statefulSets[0], namespace, statefulSetReplicaCount, true)
		ssPodsAfterScaleDown := GetListOfPodsInSts(client, statefulSets[0])
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

		// Verify all the workload Pods are in up and running state
		ginkgo.By("Verify all the workload Pods are in up and running state")
		ssPods = fss.GetPodList(client, statefulSets[0])
		for _, pod := range ssPods.Items {
			err := fpod.WaitForPodRunningInNamespace(client, &pod)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}

		/* Verify PV nde affinity and that the pods are running on appropriate nodes
		for each StatefulSet pod */
		ginkgo.By("Verify PV node affinity and that the PODS are running on appropriate node")
		for i := 0; i < len(statefulSets); i++ {
			verifyPVnodeAffinityAndPODnodedetailsForStatefulsetsLevel5(ctx, client,
				statefulSets[i], namespace, allowedTopologies, true)
		}

		// verifyVolumeMetadataInCNS
		ginkgo.By("Verify pod entry in CNS volume-metadata for the volumes associated with the PVC")
		ssPodsBeforeScaleDown := fss.GetPodList(client, statefulSets[1])
		for _, pod := range ssPodsBeforeScaleDown.Items {
			_, err := client.CoreV1().Pods(namespace).Get(ctx, pod.Name, metav1.GetOptions{})
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			for _, volumespec := range pod.Spec.Volumes {
				if volumespec.PersistentVolumeClaim != nil {
					pv := getPvFromClaim(client, pod.Namespace, volumespec.PersistentVolumeClaim.ClaimName)
					// Verify the attached volume match the one in CNS cache
					err := verifyVolumeMetadataInCNS(&e2eVSphere, pv.Spec.CSI.VolumeHandle,
						volumespec.PersistentVolumeClaim.ClaimName, pv.ObjectMeta.Name, pod.Name)
					gomega.Expect(err).NotTo(gomega.HaveOccurred())
				}
			}
		}
		ginkgo.By("Verify k8s cluster is healthy")
		wait4AllK8sNodesToBeUp(ctx, client, nodeList)
		err = waitForAllNodes2BeReady(ctx, client, pollTimeout*4)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		// Scale down statefulSets replica count
		statefulSetReplicaCount = 0
		ginkgo.By("Scale down statefulset replica count")
		for i := 0; i < len(statefulSets); i++ {
			scaleDownStatefulSetPod(ctx, client, statefulSets[i], namespace, statefulSetReplicaCount, true)
			ssPodsAfterScaleDown := GetListOfPodsInSts(client, statefulSets[i])
			gomega.Expect(len(ssPodsAfterScaleDown.Items) == int(statefulSetReplicaCount)).To(gomega.BeTrue(),
				"Number of Pods in the statefulset should match with number of replicas")
		}
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

	ginkgo.It("Volume provisioning when partial sites zone2 and zone3 hosts are down", func() {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		sts_count = 3
		statefulSetReplicaCount = 5
		noOfHostToBringDown = 1
		var ssPods *v1.PodList

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
		statefulSets := createParallelStatefulSetSpec(namespace, sts_count, statefulSetReplicaCount)

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
		time.Sleep(pollTimeoutSixMin)

		// Verify that all parallel triggered StatefulSets Pods creation should be in up and running state
		ginkgo.By("Verify that all parallel triggered StatefulSets Pods creation should be in up and running state")
		for i := 0; i < len(statefulSets); i++ {
			// verify that the StatefulSets pods are in ready state
			fss.WaitForStatusReadyReplicas(client, statefulSets[i], statefulSetReplicaCount)
			gomega.Expect(CheckMountForStsPods(client, statefulSets[i], mountPath)).NotTo(gomega.HaveOccurred())

			// Get list of Pods in each StatefulSet and verify the replica count
			ssPods = GetListOfPodsInSts(client, statefulSets[i])
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

		// Bring down 1 ESXi's that belongs to Cluster2 and Bring down 1 ESXi's that belongs to Cluster3
		ginkgo.By("Bring down 1 ESXi host that belongs to Cluster2 and Bring down 1 ESXi " +
			"host that belongs to Cluster3")
		powerOffHostsList = powerOffEsxiHostByCluster(ctx, &e2eVSphere, topologyClusterList[1],
			noOfHostToBringDown)
		powerOffHostsList1 := powerOffEsxiHostByCluster(ctx, &e2eVSphere, topologyClusterList[2],
			noOfHostToBringDown)
		powerOffHostsList = append(powerOffHostsList, powerOffHostsList1...)
		defer func() {
			ginkgo.By("Bring up all ESXi host which were powered off")
			for i := 0; i < len(powerOffHostsList); i++ {
				powerOnEsxiHostByCluster(powerOffHostsList[i])
			}
		}()

		ginkgo.By("Wait for k8s cluster to be healthy")
		wait4AllK8sNodesToBeUp(ctx, client, nodeList)
		err = waitForAllNodes2BeReady(ctx, client, pollTimeout*4)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		// Verify all the workload Pods are in up and running state
		ginkgo.By("Verify all the workload Pods are in up and running state")
		ssPods = fss.GetPodList(client, statefulSets[1])
		for _, pod := range ssPods.Items {
			err := fpod.WaitForPodRunningInNamespace(client, &pod)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())

		}

		/* Verify PV nde affinity and that the pods are running on appropriate nodes
		for each StatefulSet pod */
		ginkgo.By("Verify PV node affinity and that the PODS are running on appropriate node")
		for i := 0; i < len(statefulSets); i++ {
			verifyPVnodeAffinityAndPODnodedetailsForStatefulsetsLevel5(ctx, client,
				statefulSets[i], namespace, allowedTopologies, true)
		}

		// Scale up statefulSets replicas count
		ginkgo.By("Scaleup any one StatefulSets replica")
		statefulSetReplicaCount = 3
		ginkgo.By("Scale down statefulset replica and verify the replica count")
		scaleUpStatefulSetPod(ctx, client, statefulSets[0], namespace, statefulSetReplicaCount, true)
		ssPodsAfterScaleDown := GetListOfPodsInSts(client, statefulSets[0])
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

		/* Verify PV nde affinity and that the pods are running on appropriate nodes
		for each StatefulSet pod */
		ginkgo.By("Verify PV node affinity and that the PODS are running on appropriate node")
		for i := 0; i < len(statefulSets); i++ {
			verifyPVnodeAffinityAndPODnodedetailsForStatefulsetsLevel5(ctx, client,
				statefulSets[i], namespace, allowedTopologies, true)
		}

		// verifyVolumeMetadataInCNS
		ginkgo.By("Verify pod entry in CNS volume-metadata for the volumes associated with the PVC")
		ssPodsBeforeScaleDown := fss.GetPodList(client, statefulSets[1])
		for _, pod := range ssPodsBeforeScaleDown.Items {
			_, err := client.CoreV1().Pods(namespace).Get(ctx, pod.Name, metav1.GetOptions{})
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			for _, volumespec := range pod.Spec.Volumes {
				if volumespec.PersistentVolumeClaim != nil {
					pv := getPvFromClaim(client, pod.Namespace, volumespec.PersistentVolumeClaim.ClaimName)
					// Verify the attached volume match the one in CNS cache
					err := verifyVolumeMetadataInCNS(&e2eVSphere, pv.Spec.CSI.VolumeHandle,
						volumespec.PersistentVolumeClaim.ClaimName, pv.ObjectMeta.Name, pod.Name)
					gomega.Expect(err).NotTo(gomega.HaveOccurred())
				}
			}
		}

		ginkgo.By("Verify k8s cluster is healthy")
		wait4AllK8sNodesToBeUp(ctx, client, nodeList)
		err = waitForAllNodes2BeReady(ctx, client, pollTimeout*4)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		// Scale down statefulSets replica count
		statefulSetReplicaCount = 0
		ginkgo.By("Scale down statefulset replica count")
		for i := 0; i < len(statefulSets); i++ {
			scaleDownStatefulSetPod(ctx, client, statefulSets[i], namespace, statefulSetReplicaCount, true)
			ssPodsAfterScaleDown := GetListOfPodsInSts(client, statefulSets[i])
			gomega.Expect(len(ssPodsAfterScaleDown.Items) == int(statefulSetReplicaCount)).To(gomega.BeTrue(),
				"Number of Pods in the statefulset should match with number of replicas")
		}
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

	ginkgo.It("Volume provisioning when partial sites zone1 zone2 and zone3 hosts are down", func() {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		sts_count = 3
		statefulSetReplicaCount = 3
		var ssPods *v1.PodList

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
		statefulSets := createParallelStatefulSetSpec(namespace, sts_count, statefulSetReplicaCount)

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
		time.Sleep(pollTimeoutSixMin)

		// Verify that all parallel triggered StatefulSets Pods creation should be in up and running state
		ginkgo.By("Verify that all parallel triggered StatefulSets Pods creation should be in up and running state")
		for i := 0; i < len(statefulSets); i++ {
			// verify that the StatefulSets pods are in ready state
			fss.WaitForStatusReadyReplicas(client, statefulSets[i], statefulSetReplicaCount)
			gomega.Expect(CheckMountForStsPods(client, statefulSets[i], mountPath)).NotTo(gomega.HaveOccurred())

			// Get list of Pods in each StatefulSet and verify the replica count
			ssPods = GetListOfPodsInSts(client, statefulSets[i])
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

		// Bring down ESXi host that belongs to zone1, zone2 and zone3
		ginkgo.By("Bring down ESXi host that belongs to zone1, zone2 and zone3")
		powerOffHostsList = powerOffEsxiHostByCluster(ctx, &e2eVSphere, topologyClusterList[0],
			noOfHostToBringDown)
		powerOffHostsList2 := powerOffEsxiHostByCluster(ctx, &e2eVSphere, topologyClusterList[1],
			noOfHostToBringDown)
		powerOffHostsList3 := powerOffEsxiHostByCluster(ctx, &e2eVSphere, topologyClusterList[2],
			noOfHostToBringDown)
		powerOffHostsList = append(powerOffHostsList, powerOffHostsList2...)
		powerOffHostsList = append(powerOffHostsList, powerOffHostsList3...)
		defer func() {
			ginkgo.By("Bring up all ESXi host which were powered off")
			for i := 0; i < len(powerOffHostsList); i++ {
				powerOnEsxiHostByCluster(powerOffHostsList[i])
			}
		}()

		/* Verify PV nde affinity and that the pods are running on appropriate nodes
		for each StatefulSet pod */
		ginkgo.By("Verify PV node affinity and that the PODS are running on appropriate node")
		for i := 0; i < len(statefulSets); i++ {
			verifyPVnodeAffinityAndPODnodedetailsForStatefulsetsLevel5(ctx, client,
				statefulSets[i], namespace, allowedTopologies, true)
		}

		// Bring up
		ginkgo.By("Bring up all ESXi host which were powered off")
		for i := 0; i < len(powerOffHostsList); i++ {
			powerOnEsxiHostByCluster(powerOffHostsList[i])
		}

		// Scale up statefulSets replicas count
		ginkgo.By("Scaleup any one StatefulSets replica")
		statefulSetReplicaCount = 3
		ginkgo.By("Scale down statefulset replica and verify the replica count")
		scaleUpStatefulSetPod(ctx, client, statefulSets[0], namespace, statefulSetReplicaCount, true)
		ssPodsAfterScaleDown := GetListOfPodsInSts(client, statefulSets[0])
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

		ginkgo.By("Wait for k8s cluster to be healthy")
		wait4AllK8sNodesToBeUp(ctx, client, nodeList)
		err = waitForAllNodes2BeReady(ctx, client, pollTimeout*4)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		/* Verify PV nde affinity and that the pods are running on appropriate nodes
		for each StatefulSet pod */
		ginkgo.By("Verify PV node affinity and that the PODS are running on appropriate node")
		for i := 0; i < len(statefulSets); i++ {
			verifyPVnodeAffinityAndPODnodedetailsForStatefulsetsLevel5(ctx, client,
				statefulSets[i], namespace, allowedTopologies, true)
		}

		// verifyVolumeMetadataInCNS
		ginkgo.By("Verify pod entry in CNS volume-metadata for the volumes associated with the PVC")
		ssPodsBeforeScaleDown := fss.GetPodList(client, statefulSets[1])
		for _, pod := range ssPodsBeforeScaleDown.Items {
			_, err := client.CoreV1().Pods(namespace).Get(ctx, pod.Name, metav1.GetOptions{})
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			for _, volumespec := range pod.Spec.Volumes {
				if volumespec.PersistentVolumeClaim != nil {
					pv := getPvFromClaim(client, pod.Namespace, volumespec.PersistentVolumeClaim.ClaimName)
					// Verify the attached volume match the one in CNS cache
					err := verifyVolumeMetadataInCNS(&e2eVSphere, pv.Spec.CSI.VolumeHandle,
						volumespec.PersistentVolumeClaim.ClaimName, pv.ObjectMeta.Name, pod.Name)
					gomega.Expect(err).NotTo(gomega.HaveOccurred())
				}
			}
		}

		ginkgo.By("Verify k8s cluster is healthy")
		wait4AllK8sNodesToBeUp(ctx, client, nodeList)
		err = waitForAllNodes2BeReady(ctx, client, pollTimeout*4)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		// Scale down statefulSets replica count
		statefulSetReplicaCount = 0
		ginkgo.By("Scale down statefulset replica count")
		for i := 0; i < len(statefulSets); i++ {
			scaleDownStatefulSetPod(ctx, client, statefulSets[i], namespace, statefulSetReplicaCount, true)
			ssPodsAfterScaleDown := GetListOfPodsInSts(client, statefulSets[i])
			gomega.Expect(len(ssPodsAfterScaleDown.Items) == int(statefulSetReplicaCount)).To(gomega.BeTrue(),
				"Number of Pods in the statefulset should match with number of replicas")
		}
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

	ginkgo.It("Volume provisioning when zone2 hosts are down", func() {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		sts_count = 3
		statefulSetReplicaCount = 5
		var ssPods *v1.PodList

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
		statefulSets := createParallelStatefulSetSpec(namespace, sts_count, statefulSetReplicaCount)

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
		time.Sleep(pollTimeoutSixMin)

		// Verify that all parallel triggered StatefulSets Pods creation should be in up and running state
		ginkgo.By("Verify that all parallel triggered StatefulSets Pods creation should be in up and running state")
		for i := 0; i < len(statefulSets); i++ {
			// verify that the StatefulSets pods are in ready state
			fss.WaitForStatusReadyReplicas(client, statefulSets[i], statefulSetReplicaCount)
			gomega.Expect(CheckMountForStsPods(client, statefulSets[i], mountPath)).NotTo(gomega.HaveOccurred())

			// Get list of Pods in each StatefulSet and verify the replica count
			ssPods = GetListOfPodsInSts(client, statefulSets[i])
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

		// Bring down ESXi hosts that belongs to zone2
		ginkgo.By("Bring down ESXi hosts that belongs to zone2")
		clusterComputeResource, _, err := getClusterName(ctx, &e2eVSphere)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		hostsInCluster := getHostsByClusterName(ctx, clusterComputeResource, topologyClusterList[1])
		powerOffHostsList = powerOffEsxiHostByCluster(ctx, &e2eVSphere, topologyClusterList[1],
			(len(hostsInCluster) - 2))
		defer func() {
			ginkgo.By("Bring up all ESXi host which were powered off")
			for i := 0; i < len(powerOffHostsList); i++ {
				powerOnEsxiHostByCluster(powerOffHostsList[i])
			}
		}()

		// Verify all the workload Pods are in up and running state
		ginkgo.By("Verify all the workload Pods are in up and running state")
		ssPods = fss.GetPodList(client, statefulSets[1])
		for _, pod := range ssPods.Items {
			err := fpod.WaitForPodRunningInNamespace(client, &pod)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())

		}

		/* Verify PV nde affinity and that the pods are running on appropriate nodes
		for each StatefulSet pod */
		ginkgo.By("Verify PV node affinity and that the PODS are running on appropriate node")
		for i := 0; i < len(statefulSets); i++ {
			verifyPVnodeAffinityAndPODnodedetailsForStatefulsetsLevel5(ctx, client,
				statefulSets[i], namespace, allowedTopologies, true)
		}

		// Scale up statefulSets replicas count
		ginkgo.By("Scaleup any one StatefulSets replica")
		statefulSetReplicaCount = 3
		ginkgo.By("Scale down statefulset replica and verify the replica count")
		scaleUpStatefulSetPod(ctx, client, statefulSets[0], namespace, statefulSetReplicaCount, true)
		ssPodsAfterScaleDown := GetListOfPodsInSts(client, statefulSets[0])
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

		/* Verify PV nde affinity and that the pods are running on appropriate nodes
		for each StatefulSet pod */
		ginkgo.By("Verify PV node affinity and that the PODS are running on appropriate node")
		for i := 0; i < len(statefulSets); i++ {
			verifyPVnodeAffinityAndPODnodedetailsForStatefulsetsLevel5(ctx, client,
				statefulSets[i], namespace, allowedTopologies, true)
		}

		// verifyVolumeMetadataInCNS
		ginkgo.By("Verify pod entry in CNS volume-metadata for the volumes associated with the PVC")
		ssPodsBeforeScaleDown := fss.GetPodList(client, statefulSets[1])
		for _, pod := range ssPodsBeforeScaleDown.Items {
			_, err := client.CoreV1().Pods(namespace).Get(ctx, pod.Name, metav1.GetOptions{})
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			for _, volumespec := range pod.Spec.Volumes {
				if volumespec.PersistentVolumeClaim != nil {
					pv := getPvFromClaim(client, pod.Namespace, volumespec.PersistentVolumeClaim.ClaimName)
					// Verify the attached volume match the one in CNS cache
					err := verifyVolumeMetadataInCNS(&e2eVSphere, pv.Spec.CSI.VolumeHandle,
						volumespec.PersistentVolumeClaim.ClaimName, pv.ObjectMeta.Name, pod.Name)
					gomega.Expect(err).NotTo(gomega.HaveOccurred())
				}
			}
		}

		ginkgo.By("Verify k8s cluster is healthy")
		wait4AllK8sNodesToBeUp(ctx, client, nodeList)
		err = waitForAllNodes2BeReady(ctx, client, pollTimeout*4)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		// Scale down statefulSets replica count
		statefulSetReplicaCount = 0
		ginkgo.By("Scale down statefulset replica count")
		for i := 0; i < len(statefulSets); i++ {
			scaleDownStatefulSetPod(ctx, client, statefulSets[i], namespace, statefulSetReplicaCount, true)
			ssPodsAfterScaleDown := GetListOfPodsInSts(client, statefulSets[i])
			gomega.Expect(len(ssPodsAfterScaleDown.Items) == int(statefulSetReplicaCount)).To(gomega.BeTrue(),
				"Number of Pods in the statefulset should match with number of replicas")
		}
	})

	/*
		Testcase-5
			Bring  zone3 completely down
			Steps//
			1. Create SC with  waitForFirstConsumer
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

	ginkgo.It("Volume provisioning when zone3 hosts are down", func() {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		sts_count = 3
		statefulSetReplicaCount = 5
		var ssPods *v1.PodList

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
		statefulSets := createParallelStatefulSetSpec(namespace, sts_count, statefulSetReplicaCount)

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
		time.Sleep(pollTimeoutSixMin)

		// Verify that all parallel triggered StatefulSets Pods creation should be in up and running state
		ginkgo.By("Verify that all parallel triggered StatefulSets Pods creation should be in up and running state")
		for i := 0; i < len(statefulSets); i++ {
			// verify that the StatefulSets pods are in ready state
			fss.WaitForStatusReadyReplicas(client, statefulSets[i], statefulSetReplicaCount)
			gomega.Expect(CheckMountForStsPods(client, statefulSets[i], mountPath)).NotTo(gomega.HaveOccurred())

			// Get list of Pods in each StatefulSet and verify the replica count
			ssPods = GetListOfPodsInSts(client, statefulSets[i])
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

		// Bring down ESXi hosts that belongs to zone3
		ginkgo.By("Bring down ESXi hosts that belongs to zone3")
		clusterComputeResource, _, err := getClusterName(ctx, &e2eVSphere)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		hostsInCluster := getHostsByClusterName(ctx, clusterComputeResource, topologyClusterList[2])
		powerOffHostsList = powerOffEsxiHostByCluster(ctx, &e2eVSphere, topologyClusterList[2],
			(len(hostsInCluster) - 2))
		defer func() {
			ginkgo.By("Bring up all ESXi host which were powered off")
			for i := 0; i < len(powerOffHostsList); i++ {
				powerOnEsxiHostByCluster(powerOffHostsList[i])
			}
		}()

		// Verify all the workload Pods are in up and running state
		ginkgo.By("Verify all the workload Pods are in up and running state")
		ssPods = fss.GetPodList(client, statefulSets[1])
		for _, pod := range ssPods.Items {
			err := fpod.WaitForPodRunningInNamespace(client, &pod)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())

		}

		/* Verify PV nde affinity and that the pods are running on appropriate nodes
		for each StatefulSet pod */
		ginkgo.By("Verify PV node affinity and that the PODS are running on appropriate node")
		for i := 0; i < len(statefulSets); i++ {
			verifyPVnodeAffinityAndPODnodedetailsForStatefulsetsLevel5(ctx, client,
				statefulSets[i], namespace, allowedTopologies, true)
		}

		// Scale up statefulSets replicas count
		ginkgo.By("Scaleup any one StatefulSets replica")
		statefulSetReplicaCount = 3
		ginkgo.By("Scale down statefulset replica and verify the replica count")
		scaleUpStatefulSetPod(ctx, client, statefulSets[0], namespace, statefulSetReplicaCount, true)
		ssPodsAfterScaleDown := GetListOfPodsInSts(client, statefulSets[0])
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

		/* Verify PV nde affinity and that the pods are running on appropriate nodes
		for each StatefulSet pod */
		ginkgo.By("Verify PV node affinity and that the PODS are running on appropriate node")
		for i := 0; i < len(statefulSets); i++ {
			verifyPVnodeAffinityAndPODnodedetailsForStatefulsetsLevel5(ctx, client,
				statefulSets[i], namespace, allowedTopologies, true)
		}

		// verifyVolumeMetadataInCNS
		ginkgo.By("Verify pod entry in CNS volume-metadata for the volumes associated with the PVC")
		ssPodsBeforeScaleDown := fss.GetPodList(client, statefulSets[1])
		for _, pod := range ssPodsBeforeScaleDown.Items {
			_, err := client.CoreV1().Pods(namespace).Get(ctx, pod.Name, metav1.GetOptions{})
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			for _, volumespec := range pod.Spec.Volumes {
				if volumespec.PersistentVolumeClaim != nil {
					pv := getPvFromClaim(client, pod.Namespace, volumespec.PersistentVolumeClaim.ClaimName)
					// Verify the attached volume match the one in CNS cache
					err := verifyVolumeMetadataInCNS(&e2eVSphere, pv.Spec.CSI.VolumeHandle,
						volumespec.PersistentVolumeClaim.ClaimName, pv.ObjectMeta.Name, pod.Name)
					gomega.Expect(err).NotTo(gomega.HaveOccurred())
				}
			}
		}

		ginkgo.By("Verify k8s cluster is healthy")
		wait4AllK8sNodesToBeUp(ctx, client, nodeList)
		err = waitForAllNodes2BeReady(ctx, client, pollTimeout*4)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		// Scale down statefulSets replica count
		statefulSetReplicaCount = 0
		ginkgo.By("Scale down statefulset replica count")
		for i := 0; i < len(statefulSets); i++ {
			scaleDownStatefulSetPod(ctx, client, statefulSets[i], namespace, statefulSetReplicaCount, true)
			ssPodsAfterScaleDown := GetListOfPodsInSts(client, statefulSets[i])
			gomega.Expect(len(ssPodsAfterScaleDown.Items) == int(statefulSetReplicaCount)).To(gomega.BeTrue(),
				"Number of Pods in the statefulset should match with number of replicas")
		}
	})

	/*
		TESTCASE-6
		Bring Zone2 completely down
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

	ginkgo.It("Volume provisioning when zone2 is completely down", func() {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		sts_count = 3
		statefulSetReplicaCount = 3
		var ssPods *v1.PodList

		// get cluster resource details
		clusterComputeResource, _, err := getClusterName(ctx, &e2eVSphere)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		// Get allowed topologies for Storage Class
		allowedTopologyForSC := getTopologySelector(topologyAffinityDetails, topologyCategories,
			topologyLength)

		// Create SC with WFC BindingMode
		ginkgo.By("Creating Storage Class with WFC Binding Mode and allowed topolgies of 5 levels")
		storageclass, err := createStorageClass(client, nil, allowedTopologyForSC, "",
			bindingMode, false, "nginx-sc")
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		defer func() {
			err := client.StorageV1().StorageClasses().Delete(ctx, storageclass.Name,
				*metav1.NewDeleteOptions(0))
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
		statefulSets := createParallelStatefulSetSpec(namespace, sts_count, statefulSetReplicaCount)

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
		time.Sleep(pollTimeoutSixMin)

		// Verify that all parallel triggered StatefulSets Pods creation should be in up and running state
		ginkgo.By("Verify that all parallel triggered StatefulSets Pods creation should be in up and running state")
		for i := 0; i < len(statefulSets); i++ {
			// verify that the StatefulSets pods are in ready state
			fss.WaitForStatusReadyReplicas(client, statefulSets[i], statefulSetReplicaCount)
			gomega.Expect(CheckMountForStsPods(client, statefulSets[i], mountPath)).NotTo(gomega.HaveOccurred())

			// Get list of Pods in each StatefulSet and verify the replica count
			ssPods = GetListOfPodsInSts(client, statefulSets[i])
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

		// Bring down ESXi hosts that belongs to zone2
		ginkgo.By("Bring down all ESXi hosts that belongs to zone2")
		hostsInCluster := getHostsByClusterName(ctx, clusterComputeResource, topologyClusterList[1])
		powerOffHostsList = powerOffEsxiHostByCluster(ctx, &e2eVSphere, topologyClusterList[1],
			len(hostsInCluster))
		defer func() {
			ginkgo.By("Bring up all ESXi host which were powered off")
			for i := 0; i < len(powerOffHostsList); i++ {
				powerOnEsxiHostByCluster(powerOffHostsList[i])
			}
			ginkgo.By("Bring up all K8s nodes which got disconnected due to powered off esxi hosts")
			k8sMasterIPs := getK8sMasterIPs(ctx, client)
			restartKubeletCmd := "kubectl get nodes | grep NotReady |  awk '{print $1}'"
			framework.Logf("Invoking command '%v' on host %v", restartKubeletCmd, k8sMasterIPs[0])
			result, err := sshExec(sshClientConfig, k8sMasterIPs[0], restartKubeletCmd)
			nodeNames := strings.Split(result.Stdout, "\n")
			if err != nil && result.Code != 0 {
				fssh.LogResult(result)
				gomega.Expect(err).NotTo(gomega.HaveOccurred(),
					fmt.Sprintf("command failed/couldn't execute command: %s on host: %v", restartKubeletCmd,
						k8sMasterIPs[0]))
			}
			for _, nodeName := range nodeNames {
				if nodeName != "" {
					vmUUID := getNodeUUID(ctx, client, nodeName)
					gomega.Expect(vmUUID).NotTo(gomega.BeEmpty())
					framework.Logf("VM uuid is: %s for node: %s", vmUUID, nodeName)
					vmRef, err := e2eVSphere.getVMByUUID(ctx, vmUUID)
					gomega.Expect(err).NotTo(gomega.HaveOccurred())
					framework.Logf("vmRef: %v for the VM uuid: %s", vmRef, vmUUID)
					gomega.Expect(vmRef).NotTo(gomega.BeNil(), "vmRef should not be nil")
					vm := object.NewVirtualMachine(e2eVSphere.Client.Client, vmRef.Reference())
					_, err = vm.PowerOn(ctx)
					gomega.Expect(err).NotTo(gomega.HaveOccurred())

					err = vm.WaitForPowerState(ctx, vimtypes.VirtualMachinePowerStatePoweredOn)
					gomega.Expect(err).NotTo(gomega.HaveOccurred())
				}
			}
		}()

		// Scale up statefulSets replicas count
		ginkgo.By("Scaleup any one StatefulSets replica")
		statefulSetReplicaCount = 7
		ginkgo.By("Scale down statefulset replica and verify the replica count")
		_, scaleupErr := scaleStatefulSetPods(client, statefulSets[1], statefulSetReplicaCount)
		gomega.Expect(scaleupErr).NotTo(gomega.HaveOccurred())

		// Bring up
		ginkgo.By("Bring up all ESXi host which were powered off")
		for i := 0; i < len(powerOffHostsList); i++ {
			powerOnEsxiHostByCluster(powerOffHostsList[i])
		}

		ginkgo.By("Bring up all K8s nodes which got disconnected due to powered off esxi hosts")
		k8sMasterIPs := getK8sMasterIPs(ctx, client)
		restartKubeletCmd := "kubectl get nodes | grep NotReady |  awk '{print $1}'"
		framework.Logf("Invoking command '%v' on host %v", restartKubeletCmd, k8sMasterIPs[0])
		result, err := sshExec(sshClientConfig, k8sMasterIPs[0], restartKubeletCmd)
		nodeNames := strings.Split(result.Stdout, "\n")
		if err != nil && result.Code != 0 {
			fssh.LogResult(result)
			gomega.Expect(err).NotTo(gomega.HaveOccurred(),
				fmt.Sprintf("command failed/couldn't execute command: %s on host: %v", restartKubeletCmd,
					k8sMasterIPs[0]))
		}
		for _, nodeName := range nodeNames {
			if nodeName != "" {
				vmUUID := getNodeUUID(ctx, client, nodeName)
				gomega.Expect(vmUUID).NotTo(gomega.BeEmpty())
				framework.Logf("VM uuid is: %s for node: %s", vmUUID, nodeName)
				vmRef, err := e2eVSphere.getVMByUUID(ctx, vmUUID)
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
				framework.Logf("vmRef: %v for the VM uuid: %s", vmRef, vmUUID)
				gomega.Expect(vmRef).NotTo(gomega.BeNil(), "vmRef should not be nil")
				vm := object.NewVirtualMachine(e2eVSphere.Client.Client, vmRef.Reference())
				_, err = vm.PowerOn(ctx)
				gomega.Expect(err).NotTo(gomega.HaveOccurred())

				err = vm.WaitForPowerState(ctx, vimtypes.VirtualMachinePowerStatePoweredOn)
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
			}
		}

		// verify scale up went successful
		ginkgo.By("Verify Scaling of StatefulSet is successful")
		ssPodsAfterScaleUp := fss.GetPodList(client, statefulSets[1])
		gomega.Expect(ssPodsAfterScaleUp.Items).NotTo(gomega.BeEmpty(),
			fmt.Sprintf("Unable to get list of Pods from the Statefulset: %v", statefulSets[1].Name))
		gomega.Expect(len(ssPodsAfterScaleUp.Items) == int(statefulSetReplicaCount)).To(gomega.BeTrue(),
			"Number of Pods in the statefulset should match with number of replicas")
		// After scale up, verify all vSphere volumes are attached to node VMs.
		ginkgo.By("Verify all volumes are attached to Nodes after Statefulsets is scaled up")
		for _, sspod := range ssPodsAfterScaleUp.Items {
			err := fpod.WaitForPodsReady(client, statefulSets[1].Namespace, sspod.Name, 0)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			pod, err := client.CoreV1().Pods(namespace).Get(ctx, sspod.Name, metav1.GetOptions{})
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			for _, volumespec := range pod.Spec.Volumes {
				if volumespec.PersistentVolumeClaim != nil {
					pv := getPvFromClaim(client, statefulSets[1].Namespace, volumespec.PersistentVolumeClaim.ClaimName)
					ginkgo.By(fmt.Sprintf("Verify volume: %s is attached to the node: %s",
						pv.Spec.CSI.VolumeHandle, sspod.Spec.NodeName))
					var vmUUID string
					var exists bool
					ctx, cancel := context.WithCancel(context.Background())
					defer cancel()
					if vanillaCluster {
						vmUUID = getNodeUUID(ctx, client, sspod.Spec.NodeName)
					} else {
						annotations := pod.Annotations
						vmUUID, exists = annotations[vmUUIDLabel]
						gomega.Expect(exists).To(gomega.BeTrue(), fmt.Sprintf("Pod doesn't have %s annotation", vmUUIDLabel))
						_, err := e2eVSphere.getVMByUUID(ctx, vmUUID)
						gomega.Expect(err).NotTo(gomega.HaveOccurred())
					}
					isDiskAttached, err := e2eVSphere.isVolumeAttachedToVM(client, pv.Spec.CSI.VolumeHandle, vmUUID)
					gomega.Expect(err).NotTo(gomega.HaveOccurred(), "Disk is not attached to the node")
					gomega.Expect(isDiskAttached).To(gomega.BeTrue(), "Disk is not attached")
					ginkgo.By("After scale up, verify the attached volumes match those in CNS Cache")
					err = verifyVolumeMetadataInCNS(&e2eVSphere, pv.Spec.CSI.VolumeHandle,
						volumespec.PersistentVolumeClaim.ClaimName, pv.ObjectMeta.Name, sspod.Name)
					gomega.Expect(err).NotTo(gomega.HaveOccurred())
				}
			}
		}

		// Scale down statefulSets replica count
		ginkgo.By("Scaleup any one StatefulSets replica")
		statefulSetReplicaCount = 3
		ginkgo.By("Scale down statefulset replica and verify the replica count")
		scaleUpStatefulSetPod(ctx, client, statefulSets[0], namespace, statefulSetReplicaCount, true)

		/* Verify PV nde affinity and that the pods are running on appropriate nodes
		for each StatefulSet pod */
		ginkgo.By("Verify PV node affinity and that the PODS are running on appropriate node")
		for i := 0; i < len(statefulSets); i++ {
			verifyPVnodeAffinityAndPODnodedetailsForStatefulsetsLevel5(ctx, client,
				statefulSets[i], namespace, allowedTopologies, true)
		}

		// verifyVolumeMetadataInCNS
		ginkgo.By("Verify pod entry in CNS volume-metadata for the volumes associated with the PVC")
		ssPodsBeforeScaleDown := fss.GetPodList(client, statefulSets[1])
		for _, pod := range ssPodsBeforeScaleDown.Items {
			_, err := client.CoreV1().Pods(namespace).Get(ctx, pod.Name, metav1.GetOptions{})
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			for _, volumespec := range pod.Spec.Volumes {
				if volumespec.PersistentVolumeClaim != nil {
					pv := getPvFromClaim(client, pod.Namespace, volumespec.PersistentVolumeClaim.ClaimName)
					// Verify the attached volume match the one in CNS cache
					err := verifyVolumeMetadataInCNS(&e2eVSphere, pv.Spec.CSI.VolumeHandle,
						volumespec.PersistentVolumeClaim.ClaimName, pv.ObjectMeta.Name, pod.Name)
					gomega.Expect(err).NotTo(gomega.HaveOccurred())
				}
			}
		}
		ginkgo.By("Verify k8s cluster is healthy")
		wait4AllK8sNodesToBeUp(ctx, client, nodeList)
		err = waitForAllNodes2BeReady(ctx, client, pollTimeout*4)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		// Scale down statefulSets replica count
		statefulSetReplicaCount = 0
		ginkgo.By("Scale down statefulset replica count")
		for i := 0; i < len(statefulSets); i++ {
			scaleDownStatefulSetPod(ctx, client, statefulSets[i], namespace, statefulSetReplicaCount, true)
			ssPodsAfterScaleDown := GetListOfPodsInSts(client, statefulSets[i])
			gomega.Expect(len(ssPodsAfterScaleDown.Items) == int(statefulSetReplicaCount)).To(gomega.BeTrue(),
				"Number of Pods in the statefulset should match with number of replicas")
		}
	})
})
