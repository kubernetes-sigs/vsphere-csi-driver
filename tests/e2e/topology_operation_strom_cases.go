/*
	Copyright 2022 The Kubernetes Authors.

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
	"sync"
	"time"

	ginkgo "github.com/onsi/ginkgo/v2"
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
	fpv "k8s.io/kubernetes/test/e2e/framework/pv"
	fss "k8s.io/kubernetes/test/e2e/framework/statefulset"
	admissionapi "k8s.io/pod-security-admission/api"
)

var _ = ginkgo.Describe("[csi-topology-operation-strom-level5] "+
	"Topology-Provisioning-With-OperationStrom-Cases", func() {
	f := framework.NewDefaultFramework("e2e-vsphere-topology-aware-provisioning")
	f.NamespacePodSecurityEnforceLevel = admissionapi.LevelPrivileged
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
		err                     error
		topologyClusterList     []string
		powerOffHostsList       []string
		sshClientConfig         *ssh.ClientConfig
		k8sVersion              string
		nimbusGeneratedK8sVmPwd string
	)
	ginkgo.BeforeEach(func() {
		client = f.ClientSet
		namespace = f.Namespace.Name
		var cancel context.CancelFunc
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		bootstrap()
		nodeList, err = fnodes.GetReadySchedulableNodes(f.ClientSet)
		framework.ExpectNoError(err, "Unable to find ready and schedulable Node")
		if !(len(nodeList.Items) > 0) {
			framework.Failf("Unable to find ready and schedulable Node")
		}
		sc, err := client.StorageV1().StorageClasses().Get(ctx, defaultNginxStorageClassName, metav1.GetOptions{})
		if err == nil && sc != nil {
			gomega.Expect(client.StorageV1().StorageClasses().Delete(ctx, sc.Name,
				*metav1.NewDeleteOptions(0))).NotTo(gomega.HaveOccurred())
		}
		// fetching k8s version
		v, err := client.Discovery().ServerVersion()
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		k8sVersion = v.Major + "." + v.Minor

		bindingMode = storagev1.VolumeBindingWaitForFirstConsumer
		topologyMap := GetAndExpectStringEnvVar(topologyMap)
		topologyAffinityDetails, topologyCategories = createTopologyMapLevel5(topologyMap, topologyLength)
		allowedTopologies = createAllowedTopolgies(topologyMap, topologyLength)
		topologyClusterNames := GetAndExpectStringEnvVar(topologyCluster)
		topologyClusterList = ListTopologyClusterNames(topologyClusterNames)
		readVcEsxIpsViaTestbedInfoJson(GetAndExpectStringEnvVar(envTestbedInfoJsonPath))
		nimbusGeneratedK8sVmPwd = GetAndExpectStringEnvVar(nimbusK8sVmPwd)

		sshClientConfig = &ssh.ClientConfig{
			User: "root",
			Auth: []ssh.AuthMethod{
				ssh.Password(nimbusGeneratedK8sVmPwd),
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
		Steps//
		1. Create SC with volumeBindingMode as WaitForFirstConsumer and has parallel pod
		management policy
		2. Use the above created storageclass and create 3 stateful with parallel pod-management
		policy and each with replica 20 and NodeSelector Terms set to all topology levels
		3. Create Stateful set and wait for some time for Pod's to create and to be in running state
		4. Once the POD is in running state verify that POD's are created on all topology levels as specified in
		nodeAffinity entry
		5. Describe on the PV's and Verify node affinity details
		6. Bring down few esxi hosts in zone3
		7. Wait for some time and verify that the nodes that are on zone3 comes up on different ESX's in
		 the same zone3 and respective POD's are back to running state.
		8. Scale up First stateful set to 35 replica's and scaledown second stateful set to 5 Replicas
		9. Bring Up zone3
		10. Wait for k8s cluster to be healthy
		11. Verify First stateful set is up with 35 replica's , and 2nd  stateful sets is
		scaled down to 5 succesffully
		12. Bring down few esxi hosts in zone2
		13. Wait for some time and verify that the nodes that are on zone2 comes up on different ESX's in
		 the same zone2 and respective POD's are back to running state.
		14. Scale down First stateful set to 10 replica's
		15. Scale up second stateful set to 35 replica's
		16. Wait for some time for new pod's to get created.
		17. Verify the node affinity details of PV should be proper
		18. Bring up esx's in zone2
		19. Check PVC CNS metadata data details
		20. Delete all stateful sets
		21. Delete PVC's and SC
	*/

	ginkgo.It("Volume provisioning when multiple statefulsets creation is in "+
		"progress and in between zones hosts are down", func() {
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
		wg.Add(sts_count)
		for i := 0; i < len(statefulSets); i++ {
			go createParallelStatefulSets(client, namespace, statefulSets[i],
				statefulSetReplicaCount, &wg)
		}
		wg.Wait()
		defer func() {
			ginkgo.By(fmt.Sprintf("Deleting all statefulsets in namespace: %v", namespace))
			fss.DeleteAllStatefulSets(client, namespace)
		}()

		ginkgo.By("Waiting for StatefulSets Pods to be in Ready State")
		err = waitForStsPodsToBeInReadyRunningState(ctx, client, namespace, statefulSets)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		/* Verify PV nde affinity and that the pods are running on appropriate nodes
		for each StatefulSet pod */
		ginkgo.By("Verify PV node affinity and that the PODS are running on appropriate node")
		for i := 0; i < len(statefulSets); i++ {
			verifyPVnodeAffinityAndPODnodedetailsForStatefulsetsLevel5(ctx, client,
				statefulSets[i], namespace, allowedTopologies, true)
		}

		// Bring down few esxi hosts that belongs to zone3
		ginkgo.By("Bring down few esxi hosts that belongs to zone3")
		hostsInCluster := getHostsByClusterName(ctx, clusterComputeResource, topologyClusterList[2])
		powerOffHostsList = powerOffEsxiHostByCluster(ctx, &e2eVSphere, topologyClusterList[2],
			(len(hostsInCluster) - 2))
		defer func() {
			ginkgo.By("Bring up all ESXi host which were powered off in zone3")
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
		ginkgo.By("Scale up statefulset replica and verify the replica count")
		statefulSetReplicaCount = 35
		scaleUpStatefulSetPod(ctx, client, statefulSets[0], namespace, statefulSetReplicaCount, true)
		ssPodsAfterScaleDown := GetListOfPodsInSts(client, statefulSets[0])
		gomega.Expect(len(ssPodsAfterScaleDown.Items) == int(statefulSetReplicaCount)).To(gomega.BeTrue(),
			"Number of Pods in the statefulset should match with number of replicas")

		// Scale down statefulSets replica count
		statefulSetReplicaCount = 5
		ginkgo.By("Scale down statefulset replica and verify the replica count")
		scaleDownStatefulSetPod(ctx, client, statefulSets[1], namespace, statefulSetReplicaCount, true)
		ssPodsAfterScaleDown = GetListOfPodsInSts(client, statefulSets[1])
		gomega.Expect(len(ssPodsAfterScaleDown.Items) == int(statefulSetReplicaCount)).To(gomega.BeTrue(),
			"Number of Pods in the statefulset should match with number of replicas")

		// Bring up all esxi hosts which were powered off in zone3
		ginkgo.By("Bring up all ESXi host which were powered off in zone3")
		for i := 0; i < len(powerOffHostsList); i++ {
			powerOnEsxiHostByCluster(powerOffHostsList[i])
		}

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

		// Bring down few esxi hosts that belongs to zone2
		ginkgo.By("Bring down few esxi hosts that belongs to zone2")
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		hostsInCluster = getHostsByClusterName(ctx, clusterComputeResource, topologyClusterList[1])
		powerOffHostsList = powerOffEsxiHostByCluster(ctx, &e2eVSphere, topologyClusterList[1],
			(len(hostsInCluster) - 2))
		defer func() {
			ginkgo.By("Bring up all ESXi host which were powered off in zone2")
			for i := 0; i < len(powerOffHostsList); i++ {
				powerOnEsxiHostByCluster(powerOffHostsList[i])
			}
		}()

		// Scale down statefulSets replicas count
		statefulSetReplicaCount = 10
		ginkgo.By("Scale down statefulset replica and verify the replica count")
		scaleDownStatefulSetPod(ctx, client, statefulSets[0], namespace, statefulSetReplicaCount, true)
		ssPodsAfterScaleDown = GetListOfPodsInSts(client, statefulSets[0])
		gomega.Expect(len(ssPodsAfterScaleDown.Items) == int(statefulSetReplicaCount)).To(gomega.BeTrue(),
			"Number of Pods in the statefulset should match with number of replicas")

		// Scale up statefulSets replica count
		statefulSetReplicaCount = 35
		ginkgo.By("Scale up statefulset replica and verify the replica count")
		scaleUpStatefulSetPod(ctx, client, statefulSets[1], namespace, statefulSetReplicaCount, true)
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

		// Bring up all ESXi host which were powered off in zone2
		ginkgo.By("Bring up all ESXi host which were powered off in zone2")
		for i := 0; i < len(powerOffHostsList); i++ {
			powerOnEsxiHostByCluster(powerOffHostsList[i])
		}

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
		defer func() {
			pvcs, err := client.CoreV1().PersistentVolumeClaims(namespace).List(ctx, metav1.ListOptions{})
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			for _, claim := range pvcs.Items {
				pv := getPvFromClaim(client, namespace, claim.Name)
				err := fpv.DeletePersistentVolumeClaim(client, claim.Name, namespace)
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
				ginkgo.By("Verify it's PV and corresponding volumes are deleted from CNS")
				err = fpv.WaitForPersistentVolumeDeleted(client, pv.Name, poll,
					pollTimeout)
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
				volumeHandle := pv.Spec.CSI.VolumeHandle
				err = e2eVSphere.waitForCNSVolumeToBeDeleted(volumeHandle)
				gomega.Expect(err).NotTo(gomega.HaveOccurred(),
					fmt.Sprintf("Volume: %s should not be present in the CNS after it is deleted from "+
						"kubernetes", volumeHandle))
			}
		}()
	})

	/*
		TESTCASE-2
		Delete one of the container node + bring down esxi hosts of zone2

		Steps//
		1. Identify the Pod where CSI Provisioner is the leader.
		2. Create Storage class with Wait for first consumer
		3. Bring down all esxi hosts in zone2
		4. Using the above created SC , Create 3 statefulset with parallel POD management policy
		each with 10 replica's
		5. While the Statefulsets is creating PVCs and Pods, kill CSI Provisioner container
		identified in the step 1, where CSI provisioner is the leader.
		6. csi-provisioner in other replica should take the leadership to help provisioning of the volume.
		7. Bring up ESX's in zone2
		8. Expect all PVCs for Statefulsets to be in the bound state.
		9. Expect all Pods for Statefulsets to be in the running state
		10. Describe PV and Verify the node affinity rule . Make sure node affinity should
		contain all 5 levels of topology details
		11. POD should be running in the appropriate nodes
		12. Identify the Pod where CSI Attacher is the leader
		13. Scale down the Statefulsets replica count to 5,
		14. While the Statefulsets is scaling down Pods, kill CSI Attacher container
		identified in the step 13, where CSI Attacher is the leader.
		15. Wait until the POD count goes down to 5
		16. Scale up replica count 20
		17. PVC's, POD's should come up on all the topology levels specified.
		18. Verify node affinity details on PV's should be proper with all 5 level details.
		19. Delete statefulset
		20. Delete PVC's and SC
	*/

	ginkgo.It("Volume provisioning when multiple statefulsets creation in progress and in "+
		"between zones hosts and container nodes are down", func() {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		sts_count = 3
		statefulSetReplicaCount = 10
		var ssPods *v1.PodList
		containerName := provisionerContainerName
		var notReadyNodes []v1.Node
		notReadyNodes = nil

		// get cluster details
		clusterComputeResource, _, err = getClusterName(ctx, &e2eVSphere)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		/* Get current leader Csi-Controller-Pod where CSI Provisioner is running and " +
		find the master node IP where this Csi-Controller-Pod is running */
		ginkgo.By("Get current leader Csi-Controller-Pod name where CSI Provisioner is running and " +
			"find the master node IP where this container leader is running")
		controller_name, k8sMasterIP, err := getK8sMasterNodeIPWhereContainerLeaderIsRunning(ctx,
			client, sshClientConfig, containerName)
		framework.Logf("CSI-Provisioner is running on Leader Pod %s "+
			"which is running on master node %s", controller_name, k8sMasterIP)
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

		// Bring down all esxi hosts that belongs to zone2
		ginkgo.By("Bring down all ESXi hosts that belongs to zone2")
		hostsInCluster := getHostsByClusterName(ctx, clusterComputeResource, topologyClusterList[1])
		powerOffHostsList = powerOffEsxiHostByCluster(ctx, &e2eVSphere, topologyClusterList[1],
			len(hostsInCluster))
		defer func() {
			ginkgo.By("Bring up all ESXi host which were powered off in zone2")
			for i := 0; i < len(powerOffHostsList); i++ {
				powerOnEsxiHostByCluster(powerOffHostsList[i])
			}
			nodes, err := client.CoreV1().Nodes().List(ctx, metav1.ListOptions{})
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			for _, node := range nodes.Items {
				if !fnodes.IsConditionSetAsExpected(&node, v1.NodeReady, true) {
					notReadyNodes = append(notReadyNodes, node)
				}
			}
			if len(notReadyNodes) != 0 {
				ginkgo.By("Bring up all K8s nodes which got disconnected due to powered off esxi hosts")
				for _, nodeName := range notReadyNodes {
					vmUUID := getNodeUUID(ctx, client, nodeName.Name)
					gomega.Expect(vmUUID).NotTo(gomega.BeEmpty())
					framework.Logf("VM uuid is: %s for node: %s", vmUUID, nodeName.Name)
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
		wg.Add(sts_count)
		for i := 0; i < len(statefulSets); i++ {
			go createParallelStatefulSets(client, namespace, statefulSets[i],
				statefulSetReplicaCount, &wg)
			if i == 2 {
				/* Kill container CSI-Provisioner on the master node where elected leader
				is running */
				ginkgo.By("Kill container CSI-Provisioner on the master node where elected leader " +
					"is running")
				err = execDockerPauseNKillOnContainer(sshClientConfig, k8sMasterIP, containerName, k8sVersion)
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
			}
		}
		wg.Wait()

		// Bring up all ESXi host which were powered off in zone2
		ginkgo.By("Bring up all ESXi host which were powered off in zone2")
		for i := 0; i < len(powerOffHostsList); i++ {
			powerOnEsxiHostByCluster(powerOffHostsList[i])
		}
		nodes, err := client.CoreV1().Nodes().List(ctx, metav1.ListOptions{})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		for _, node := range nodes.Items {
			if !fnodes.IsConditionSetAsExpected(&node, v1.NodeReady, true) {
				notReadyNodes = append(notReadyNodes, node)
			}
		}
		if len(notReadyNodes) != 0 {
			ginkgo.By("Bring up all K8s nodes which got disconnected due to powered off esxi hosts")
			for _, nodeName := range notReadyNodes {
				vmUUID := getNodeUUID(ctx, client, nodeName.Name)
				gomega.Expect(vmUUID).NotTo(gomega.BeEmpty())
				framework.Logf("VM uuid is: %s for node: %s", vmUUID, nodeName.Name)
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

		/* Get newly elected leader Csi-Controller-Pod where CSI Provisioner is running" +
		find new master node IP where this Csi-Controller-Pod is running */
		ginkgo.By("Get newly Leader Csi-Controller-Pod where CSI Provisioner is running and " +
			"find the master node IP where this Csi-Controller-Pod is running")
		controller_name, k8sMasterIP, err = getK8sMasterNodeIPWhereContainerLeaderIsRunning(ctx,
			client, sshClientConfig, containerName)
		framework.Logf("CSI-Provisioner is running on newly elected Leader Pod %s "+
			"which is running on master node %s", controller_name, k8sMasterIP)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		// Wait for testbed to be back to normal
		time.Sleep(pollTimeoutShort)

		ginkgo.By("Waiting for StatefulSets Pods to be in Ready State")
		err = waitForStsPodsToBeInReadyRunningState(ctx, client, namespace, statefulSets)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		/* Verify PV nde affinity and that the pods are running on appropriate nodes
		for each StatefulSet pod */
		ginkgo.By("Verify PV node affinity and that the PODS are running on appropriate node")
		for i := 0; i < len(statefulSets); i++ {
			verifyPVnodeAffinityAndPODnodedetailsForStatefulsetsLevel5(ctx, client,
				statefulSets[i], namespace, allowedTopologies, true)
		}

		/* Get current leader Csi-Controller-Pod where CSI Attacher is running and " +
		find the master node IP where this Csi-Controller-Pod is running */
		containerName = "csi-attacher"
		ginkgo.By("Get current leader Csi-Controller-Pod name where CSI Attacher is running and " +
			"find the master node IP where this Csi-Controller-Pod is running")
		controller_name, k8sMasterIP, err = getK8sMasterNodeIPWhereContainerLeaderIsRunning(ctx,
			client, sshClientConfig, containerName)
		framework.Logf("csi-attacher is running on Leader Pod %s "+
			"which is running on master node %s", controller_name, k8sMasterIP)
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
				/* Kill csi-attacher container */
				ginkgo.By("Kill csi-attacher container")
				err = execDockerPauseNKillOnContainer(sshClientConfig, k8sMasterIP, containerName, k8sVersion)
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
			}
		}

		/* Get newly elected leader Csi-Controller-Pod where CSI Provisioner is running" +
		   find new master node IP where this Csi-Controller-Pod is running */
		ginkgo.By("Get newly elected leader Csi-Controller-Pod where CSI Attacher is running and " +
			"find the master node IP where this Csi-Controller-Pod is running")
		controller_name, k8sMasterIP, err = getK8sMasterNodeIPWhereContainerLeaderIsRunning(ctx,
			client, sshClientConfig, containerName)
		framework.Logf("csi-attacher is running on elected Leader Pod %s "+
			"which is running on master node %s", controller_name, k8sMasterIP)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		for i := 0; i < len(statefulSets); i++ {
			// Scale up statefulSets replicas count
			statefulSetReplicaCount = 20
			ginkgo.By("Scale up statefulset replica and verify the replica count")
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
		defer func() {
			pvcs, err := client.CoreV1().PersistentVolumeClaims(namespace).List(ctx, metav1.ListOptions{})
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			for _, claim := range pvcs.Items {
				pv := getPvFromClaim(client, namespace, claim.Name)
				err := fpv.DeletePersistentVolumeClaim(client, claim.Name, namespace)
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
				ginkgo.By("Verify it's PV and corresponding volumes are deleted from CNS")
				err = fpv.WaitForPersistentVolumeDeleted(client, pv.Name, poll,
					pollTimeout)
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
				volumeHandle := pv.Spec.CSI.VolumeHandle
				err = e2eVSphere.waitForCNSVolumeToBeDeleted(volumeHandle)
				gomega.Expect(err).NotTo(gomega.HaveOccurred(),
					fmt.Sprintf("Volume: %s should not be present in the CNS after it is deleted from "+
						"kubernetes", volumeHandle))
			}
		}()
	})
})
