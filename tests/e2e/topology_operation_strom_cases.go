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
		powerOffHostsList       []string
		sshClientConfig         *ssh.ClientConfig
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

	ginkgo.It("Volume provisioning when multiple statefulsets creation is in "+
		"progress and in between hosts are down", func() {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		sts_count = 3
		statefulSetReplicaCount = 20
		var ssPods *v1.PodList

		// get cluster details
		clusterComputeResource, _, err = getClusterName(ctx, &e2eVSphere)
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
		hostsInCluster := getHostsByClusterName(ctx, clusterComputeResource, topologyClusterList[2])
		powerOffHostsList = powerOffEsxiHostByCluster(ctx, &e2eVSphere, topologyClusterList[2],
			(len(hostsInCluster) - 2))
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

		// Scale up statefulSets replicas count
		ginkgo.By("Scaleup any one StatefulSets replica")
		statefulSetReplicaCount = 10
		ginkgo.By("Scale down statefulset replica and verify the replica count")
		scaleUpStatefulSetPod(ctx, client, statefulSets[0], namespace, statefulSetReplicaCount, true)
		ssPodsAfterScaleDown := GetListOfPodsInSts(client, statefulSets[0])
		gomega.Expect(len(ssPodsAfterScaleDown.Items) == int(statefulSetReplicaCount)).To(gomega.BeTrue(),
			"Number of Pods in the statefulset should match with number of replicas")

		// Scale down statefulSets replica count
		statefulSetReplicaCount = 5
		ginkgo.By("Scale down statefulset replica count")
		scaleDownStatefulSetPod(ctx, client, statefulSets[1], namespace, statefulSetReplicaCount, true)
		ssPodsAfterScaleDown = GetListOfPodsInSts(client, statefulSets[1])
		gomega.Expect(len(ssPodsAfterScaleDown.Items) == int(statefulSetReplicaCount)).To(gomega.BeTrue(),
			"Number of Pods in the statefulset should match with number of replicas")

		// Bring up
		ginkgo.By("Bring up all ESXi host which were powered off")
		for i := 0; i < len(powerOffHostsList); i++ {
			powerOnEsxiHostByCluster(powerOffHostsList[i])
		}

		ginkgo.By("Wait for k8s cluster to be healthy")
		wait4AllK8sNodesToBeUp(ctx, client, nodeList)
		err = waitForAllNodes2BeReady(ctx, client, pollTimeout*4)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		// Bring down ESXi hosts that belongs to zone2
		ginkgo.By("Bring down ESXi hosts that belongs to zone2")
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		hostsInCluster = getHostsByClusterName(ctx, clusterComputeResource, topologyClusterList[1])
		powerOffHostsList = powerOffEsxiHostByCluster(ctx, &e2eVSphere, topologyClusterList[1],
			(len(hostsInCluster) - 2))
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

		// Scale up statefulSets replicas count
		ginkgo.By("Scaleup any one StatefulSets replica")
		statefulSetReplicaCount = 10
		ginkgo.By("Scale down statefulset replica and verify the replica count")
		scaleUpStatefulSetPod(ctx, client, statefulSets[0], namespace, statefulSetReplicaCount, true)
		ssPodsAfterScaleDown = GetListOfPodsInSts(client, statefulSets[0])
		gomega.Expect(len(ssPodsAfterScaleDown.Items) == int(statefulSetReplicaCount)).To(gomega.BeTrue(),
			"Number of Pods in the statefulset should match with number of replicas")

		// Scale down statefulSets replica count
		statefulSetReplicaCount = 5
		ginkgo.By("Scale down statefulset replica count")
		scaleDownStatefulSetPod(ctx, client, statefulSets[1], namespace, statefulSetReplicaCount, true)
		ssPodsAfterScaleDown = GetListOfPodsInSts(client, statefulSets[1])
		gomega.Expect(len(ssPodsAfterScaleDown.Items) == int(statefulSetReplicaCount)).To(gomega.BeTrue(),
			"Number of Pods in the statefulset should match with number of replicas")

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
		7. Wait until all PVCs and Pods are created for Statefulsets . Since all esx's down in
		cluster 2 POD's should come up on cluster 1 and cluster 3
		8. Expect all PVCs for Statefulsets to be in the bound state.
		9. Expect all Pods for Statefulsets to be in the running state
		10. Describe PV and Verify the node affinity rule . Make sure node affinity should
		contain all 5 levels of topology details
		11. POD should be running in the appropriate nodes
		12. Bring up ESX's in cluster 2
		13. Identify the Pod where CSI Attacher is the leader
		14. Scale down the Statefulsets replica count to 5,
		15. While the Statefulsets is scaling down Pods, kill CSI Attacher container
		identified in the step 11, where CSI Attacher is the leader.
		16. Wait until the POD count goes down to 5
		17. Delete the PVC's which are not used by POD's
		18. Scale up replica count 20
		19. PVC's, POD's should come up on all three clusters (labeled as countries)
		20. verify node affinity details on PV's should be proper with all 5 level details
		21. Delete statefulset
		22. Delete PVC's and SC
	*/

	ginkgo.It("Volume provisioning when multiple statefulsets creation in progress and in "+
		"between hosts and container nodes are down", func() {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		sts_count = 3
		statefulSetReplicaCount = 10
		var ssPods *v1.PodList
		containerName := "csi-provisioner"

		/* Get current leader Csi-Controller-Pod where CSI Provisioner is running and " +
		find the master node IP where this Csi-Controller-Pod is running */
		ginkgo.By("Get current leader Csi-Controller-Pod name where CSI Provisioner is running and " +
			"find the master node IP where this Csi-Controller-Pod is running")
		csi_controller_pod, k8sMasterIP, err := getK8sMasterNodeIPWhereContainerLeaderIsRunning(ctx,
			client, sshClientConfig, containerName)
		framework.Logf("CSI-Provisioner is running on Leader Pod %s "+
			"which is running on master node %s", csi_controller_pod, k8sMasterIP)
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
			err := client.StorageV1().StorageClasses().Delete(ctx, storageclass.Name, *metav1.NewDeleteOptions(0))
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}()

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
		// Creating StatefulSet service
		ginkgo.By("Creating service")
		service := CreateService(namespace, client)
		defer func() {
			deleteService(namespace, client, service)
		}()

		// Create multiple StatefulSets Specs in parallel
		ginkgo.By("Creating multiple StatefulSets specs in parallel")
		statefulSets := createParallelStatefulSetSpec(namespace, sts_count, statefulSetReplicaCount)

		/* Trigger multiple StatefulSets creation in parallel.
		During StatefulSets creation, kill CSI Provisioner container in between */
		ginkgo.By("Trigger multiple StatefulSets creation in parallel. During StatefulSets " +
			"creation, kill CSI Provisioner container in between ")
		var wg sync.WaitGroup
		wg.Add(3)
		for i := 0; i < len(statefulSets); i++ {
			go createParallelStatefulSets(client, namespace, statefulSets[i],
				statefulSetReplicaCount, &wg)
			if i == 2 {
				/* Kill container CSI-Provisioner on the master node where elected leader CSi-Controller-Pod
				is running */
				ginkgo.By("Kill container CSI-Provisioner on the master node where elected leader " +
					"CSi-Controller-Pod is running")
				err = execDockerPauseNKillOnContainer(sshClientConfig, k8sMasterIP, containerName)
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
			}
		}
		wg.Wait()

		/* Get newly elected leader Csi-Controller-Pod where CSI Provisioner is running" +
		find new master node IP where this Csi-Controller-Pod is running */
		ginkgo.By("Get newly Leader Csi-Controller-Pod where CSI Provisioner is running and " +
			"find the master node IP where this Csi-Controller-Pod is running")
		csi_controller_pod, k8sMasterIP, err = getK8sMasterNodeIPWhereContainerLeaderIsRunning(ctx,
			client, sshClientConfig, containerName)
		framework.Logf("CSI-Provisioner is running on newly elected Leader Pod %s "+
			"which is running on master node %s", csi_controller_pod, k8sMasterIP)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

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

		// Bring up
		ginkgo.By("Bring up all ESXi host which were powered off")
		for i := 0; i < len(powerOffHostsList); i++ {
			powerOnEsxiHostByCluster(powerOffHostsList[i])
		}

		/* Get current leader Csi-Controller-Pod where CSI Attacher is running and " +
		find the master node IP where this Csi-Controller-Pod is running */
		containerName = "CSI-Attacher"
		ginkgo.By("Get current leader Csi-Controller-Pod name where CSI Attacher is running and " +
			"find the master node IP where this Csi-Controller-Pod is running")
		csi_controller_pod, k8sMasterIP, err = getK8sMasterNodeIPWhereContainerLeaderIsRunning(ctx,
			client, sshClientConfig, containerName)
		framework.Logf("CSI-Attacher is running on Leader Pod %s "+
			"which is running on master node %s", csi_controller_pod, k8sMasterIP)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

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

		// Scale down statefulSets replicas count
		ginkgo.By("Scaledown StatefulSets replica in parallel. During StatefulSets replica scaledown " +
			"kill CSI Attacher container in between")
		statefulSetReplicaCount = 5
		ginkgo.By("Scale down statefulset replica and verify the replica count")
		for i := 0; i < len(statefulSets); i++ {
			scaleDownStatefulSetPod(ctx, client, statefulSets[i], namespace, statefulSetReplicaCount, true)
			ssPodsAfterScaleDown := GetListOfPodsInSts(client, statefulSets[i])
			gomega.Expect(len(ssPodsAfterScaleDown.Items) == int(statefulSetReplicaCount)).To(gomega.BeTrue(),
				"Number of Pods in the statefulset should match with number of replicas")
			if i == 2 {
				/* Kill container CSI-Attacher on the master node where elected leader CSi-Controller-Pod
				is running */
				ginkgo.By("Kill container CSI-Attacher on the master node where elected leader CSi-Controller-Pod " +
					"is running")
				err = execDockerPauseNKillOnContainer(sshClientConfig, k8sMasterIP, containerName)
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
			}
		}

		/* Get newly elected leader Csi-Controller-Pod where CSI Provisioner is running" +
		   find new master node IP where this Csi-Controller-Pod is running */
		ginkgo.By("Get newly elected leader Csi-Controller-Pod where CSI Attacher is running and " +
			"find the master node IP where this Csi-Controller-Pod is running")
		containerName = "csi-provisioner"
		csi_controller_pod, k8sMasterIP, err = getK8sMasterNodeIPWhereContainerLeaderIsRunning(ctx,
			client, sshClientConfig, containerName)
		framework.Logf("CSI-Attacher is running on elected Leader Pod %s "+
			"which is running on master node %s", csi_controller_pod, k8sMasterIP)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		for i := 0; i < len(statefulSets); i++ {
			// Scale up statefulSets replicas count
			ginkgo.By("Scaleup any one StatefulSets replica")
			statefulSetReplicaCount = 15
			ginkgo.By("Scale down statefulset replica and verify the replica count")
			scaleUpStatefulSetPod(ctx, client, statefulSets[i], namespace, statefulSetReplicaCount, true)
			ssPodsAfterScaleDown := GetListOfPodsInSts(client, statefulSets[i])
			gomega.Expect(len(ssPodsAfterScaleDown.Items) == int(statefulSetReplicaCount)).To(gomega.BeTrue(),
				"Number of Pods in the statefulset should match with number of replicas")
		}

		/* Verify PV nde affinity and that the pods are running on appropriate nodes
		for each StatefulSet pod */
		ginkgo.By("Verify PV node affinity and that the PODS are running on appropriate node")
		for i := 0; i < len(statefulSets); i++ {
			verifyPVnodeAffinityAndPODnodedetailsForStatefulsetsLevel5(ctx, client,
				statefulSets[i], namespace, allowedTopologies, true)
		}

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

func createParallelStatefulSetSpec(namespace string, no_of_sts int, replicas int32) []*appsv1.StatefulSet {
	stss := []*appsv1.StatefulSet{}
	var statefulset *appsv1.StatefulSet
	for i := 0; i < no_of_sts; i++ {
		statefulset = GetStatefulSetFromManifest(namespace)
		statefulset.Name = "thread-" + strconv.Itoa(i) + "-" + statefulset.Name
		statefulset.Spec.PodManagementPolicy = appsv1.ParallelPodManagement
		statefulset.Spec.VolumeClaimTemplates[len(statefulset.Spec.VolumeClaimTemplates)-1].
			Annotations["volume.beta.kubernetes.io/storage-class"] = "nginx-sc"
		statefulset.Spec.Replicas = &replicas
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
		statefulset.Namespace, statefulset.Name, replicas, statefulset.Spec.Selector))
	_, err := client.AppsV1().StatefulSets(namespace).Create(ctx, statefulset, metav1.CreateOptions{})
	gomega.Expect(err).NotTo(gomega.HaveOccurred())
}

func powerOffEsxiHostByCluster(ctx context.Context, vs *vSphere, clusterName string,
	esxCount int) []string {
	var powerOffHostsList []string
	var hostsInCluster []*object.HostSystem
	clusterComputeResource, _, err := getClusterName(ctx, &e2eVSphere)
	gomega.Expect(err).NotTo(gomega.HaveOccurred())
	fmt.Println("============= clusterName ============", clusterName)
	hostsInCluster = getHostsByClusterName(ctx, clusterComputeResource, clusterName)
	fmt.Println("============ hostsInCluster ============ ", hostsInCluster)
	for i := 0; i < esxCount; i++ {
		for _, esxInfo := range tbinfo.esxHosts {
			host := hostsInCluster[i].Common.InventoryPath
			fmt.Println("========== Host============", host)
			hostIp := strings.Split(host, "/")
			fmt.Println("========== Host ip ============", hostIp)
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
	fmt.Println("=============hostToPowerOn ==============", hostToPowerOn)
	var esxHostIp string = ""
	for _, esxInfo := range tbinfo.esxHosts {
		if hostToPowerOn == esxInfo["vmName"] {
			esxHostIp = esxInfo["ip"]
			fmt.Println("================ esxHostIp ===============", esxHostIp)
			err := vMPowerMgmt(tbinfo.user, tbinfo.location, hostToPowerOn, true)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())

		}
	}
	err := waitForHostToBeUp(esxHostIp)
	gomega.Expect(err).NotTo(gomega.HaveOccurred())
}
