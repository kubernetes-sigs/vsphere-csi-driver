/*
	Copyright 2023 The Kubernetes Authors.

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

	"github.com/onsi/ginkgo/v2"
	"github.com/onsi/gomega"
	"github.com/vmware/govmomi/object"
	"golang.org/x/crypto/ssh"
	v1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	clientset "k8s.io/client-go/kubernetes"
	"k8s.io/kubernetes/test/e2e/framework"
	fnodes "k8s.io/kubernetes/test/e2e/framework/node"
	admissionapi "k8s.io/pod-security-admission/api"
)

var _ = ginkgo.Describe("[multivc-sitedown] MultiVc-SiteDown", func() {
	f := framework.NewDefaultFramework("multivc-sitedown")
	f.NamespacePodSecurityEnforceLevel = admissionapi.LevelPrivileged
	var (
		client                      clientset.Interface
		namespace                   string
		allowedTopologies           []v1.TopologySelectorLabelRequirement
		statefulSetReplicaCount     int32
		stsScaleUp                  bool
		stsScaleDown                bool
		parallelStatefulSetCreation bool
		scaleUpReplicaCount         int32
		scaleDownReplicaCount       int32
		hostsInCluster              []*object.HostSystem
		clusterComputeResource      []*object.ClusterComputeResource
		topValStartIndex            int
		topValEndIndex              int
		topkeyStartIndex            int
		nodeList                    *v1.NodeList
		storagePolicyInVc1Vc2       string
		scParameters                map[string]string
		verifyTopologyAffinity      bool
		parallelPodPolicy           bool
		allMasterIps                []string
		masterIp                    string
		sshClientConfig             *ssh.ClientConfig
		nimbusGeneratedK8sVmPwd     string
		dataCenters                 []*object.Datacenter
		workerInitialAlias          []string
		ClusterdatastoreListVC      []map[string]string
		ClusterdatastoreListVC1     map[string]string
		ClusterdatastoreListVC2     map[string]string
		ClusterdatastoreListVC3     map[string]string
	)
	ginkgo.BeforeEach(func() {
		var cancel context.CancelFunc
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		client = f.ClientSet
		namespace = f.Namespace.Name

		multiVCbootstrap()

		stsScaleUp = true
		stsScaleDown = true
		verifyTopologyAffinity = true
		parallelStatefulSetCreation = true

		sc, err := client.StorageV1().StorageClasses().Get(ctx, defaultNginxStorageClassName, metav1.GetOptions{})
		if err == nil && sc != nil {
			gomega.Expect(client.StorageV1().StorageClasses().Delete(ctx, sc.Name,
				*metav1.NewDeleteOptions(0))).NotTo(gomega.HaveOccurred())
		}

		nodeList, err = fnodes.GetReadySchedulableNodes(ctx, f.ClientSet)
		framework.ExpectNoError(err, "Unable to find ready and schedulable Node")
		if !(len(nodeList.Items) > 0) {
			framework.Failf("Unable to find ready and schedulable Node")
		}

		topologyMap := GetAndExpectStringEnvVar(envTopologyMap)
		allowedTopologies = createAllowedTopolgies(topologyMap)
		scParameters = make(map[string]string)
		storagePolicyInVc1Vc2 = GetAndExpectStringEnvVar(envStoragePolicyNameInVC1VC2)
		nimbusGeneratedK8sVmPwd = GetAndExpectStringEnvVar(nimbusK8sVmPwd)

		sshClientConfig = &ssh.ClientConfig{
			User: "root",
			Auth: []ssh.AuthMethod{
				ssh.Password(nimbusGeneratedK8sVmPwd),
			},
			HostKeyCallback: ssh.InsecureIgnoreHostKey(),
		}

		// fetching k8s master ip
		allMasterIps = getK8sMasterIPs(ctx, client)
		masterIp = allMasterIps[0]

		// fetching datacenter details
		dataCenters, err = multiVCe2eVSphere.getAllDatacentersForMultiVC(ctx)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		clusterWorkerMap := GetAndExpectStringEnvVar(workerClusterMap)
		_, workerInitialAlias = createTopologyMapLevel5(clusterWorkerMap)

		// fetching cluster details
		clientIndex := 0
		clusterComputeResource, _, err = getClusterNameForMultiVC(ctx, &multiVCe2eVSphere, clientIndex)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		// fetching list of datastores available in different VCs
		ClusterdatastoreListVC1, ClusterdatastoreListVC2,
			ClusterdatastoreListVC3, err = getDatastoresListFromMultiVCs(masterIp, sshClientConfig,
			clusterComputeResource[0])
		ClusterdatastoreListVC = append(ClusterdatastoreListVC, ClusterdatastoreListVC1,
			ClusterdatastoreListVC2, ClusterdatastoreListVC3)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
	})

	ginkgo.AfterEach(func() {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		ginkgo.By(fmt.Sprintf("Deleting all statefulsets in namespace: %v", namespace))
		deleteAllStsAndPodsPVCsInNamespace(ctx, client, namespace)
		ginkgo.By(fmt.Sprintf("Deleting service nginx in namespace: %v", namespace))
		err := client.CoreV1().Services(namespace).Delete(ctx, servicename, *metav1.NewDeleteOptions(0))
		if !apierrors.IsNotFound(err) {
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}

		framework.Logf("Perform cleanup of any left over stale PVs")
		allPvs, err := client.CoreV1().PersistentVolumes().List(ctx, metav1.ListOptions{})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		for _, pv := range allPvs.Items {
			err := client.CoreV1().PersistentVolumes().Delete(ctx, pv.Name, metav1.DeleteOptions{})
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}
	})

	/* Testcase-1
	Bring down few esx on VC2's availability zones  (partial site down)

	1. Create SC which allowed to provisionn volume on VC2 AZs
	2. Create 3 statefulset each with 5 replica
	3. Bring down few ESX in VC2
	4. Verify k8s node status
	5. Wait for all the statefull sets to come up
	6. Bring up the ESX hosts which were powered off in step2
	7. Scale-up/Scale-down the statefulset
	8. Make sure common validation points are met on PV,PVC and POD
	9. Clean up the data
	*/

	ginkgo.It("[pq-multivc] Bring down few esx on VC2 availability zones in a multi-vc setup", ginkgo.Label(p1,
		block, vanilla, multiVc, vc70, flaky, disruptive), func() {

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		sts_count := 3
		statefulSetReplicaCount = 5
		noOfHostToBringDown := 1
		var powerOffHostsLists []string
		topValStartIndex = 1
		topValEndIndex = 2
		clientIndex := 1

		ginkgo.By("Set specific allowed topology")
		allowedTopologies = setSpecificAllowedTopology(allowedTopologies, topkeyStartIndex, topValStartIndex,
			topValEndIndex)

		ginkgo.By("Create SC on VC2 avaialibility zone")
		storageclass, err := createStorageClass(client, nil, allowedTopologies, "", "", false, "nginx-sc")
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		defer func() {
			err := client.StorageV1().StorageClasses().Delete(ctx, storageclass.Name, *metav1.NewDeleteOptions(0))
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}()

		ginkgo.By("Create service")
		service := CreateService(namespace, client)
		defer func() {
			deleteService(namespace, client, service)
		}()

		ginkgo.By("Create 3 StatefulSet with replica count 5")
		statefulSets := createParallelStatefulSetSpec(namespace, sts_count, statefulSetReplicaCount)
		var wg sync.WaitGroup
		wg.Add(sts_count)
		for i := 0; i < len(statefulSets); i++ {
			go createParallelStatefulSets(client, namespace, statefulSets[i],
				statefulSetReplicaCount, &wg)

		}
		wg.Wait()

		ginkgo.By("Bring down ESXI hosts i.e. partial site down on VC2 multi-setup")
		// read testbed info json for VC2
		readVcEsxIpsViaTestbedInfoJson(GetAndExpectStringEnvVar(envTestbedInfoJsonPathVC2))

		// read cluster details in VC2
		clusterComputeResource, _, err = getClusterNameForMultiVC(ctx, &multiVCe2eVSphere, clientIndex)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		elements := strings.Split(clusterComputeResource[0].InventoryPath, "/")
		clusterName := elements[len(elements)-1]

		// read hosts in a cluster
		hostsInCluster = getHostsByClusterName(ctx, clusterComputeResource, clusterName)

		// power off esxi hosts
		powerOffHostsList := powerOffEsxiHostsInMultiVcCluster(ctx, &multiVCe2eVSphere,
			noOfHostToBringDown, hostsInCluster)
		powerOffHostsLists = append(powerOffHostsLists, powerOffHostsList...)
		defer func() {
			ginkgo.By("Bring up  ESXi host which were powered off in VC2 multi-setup")
			for i := 0; i < len(powerOffHostsLists); i++ {
				powerOnEsxiHostByCluster(powerOffHostsLists[i])
			}

			ginkgo.By("Wait for k8s cluster to be healthy")
			wait4AllK8sNodesToBeUp(nodeList)
			err = waitForAllNodes2BeReady(ctx, client, pollTimeout*4)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())

		}()

		ginkgo.By("Wait for k8s cluster to be healthy")
		wait4AllK8sNodesToBeUp(nodeList)
		err = waitForAllNodes2BeReady(ctx, client, pollTimeout*4)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Waiting for StatefulSets Pods to be in Ready State")
		err = waitForStsPodsToBeInReadyRunningState(ctx, client, namespace, statefulSets)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		defer func() {
			deleteAllStsAndPodsPVCsInNamespace(ctx, client, namespace)
		}()

		ginkgo.By("Verify PV node affinity and that the PODS are running on appropriate node")
		for i := 0; i < len(statefulSets); i++ {
			err = verifyPVnodeAffinityAndPODnodedetailsForStatefulsetsLevel5(ctx, client,
				statefulSets[i], namespace, allowedTopologies, parallelStatefulSetCreation)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}

		ginkgo.By("Bring up ESXi host which were powered off")
		for i := 0; i < len(powerOffHostsLists); i++ {
			powerOnEsxiHostByCluster(powerOffHostsLists[i])
		}

		ginkgo.By("Perform scaleup/scaledown operation on statefulset and " +
			"verify pv and pod affinity details")
		for i := 0; i < len(statefulSets); i++ {
			if i == 0 {
				stsScaleDown = false
				scaleUpReplicaCount = 7
				framework.Logf("Scale up StatefulSet1 replica count to 7")
				err = performScalingOnStatefulSetAndVerifyPvNodeAffinity(ctx, client, scaleUpReplicaCount,
					scaleDownReplicaCount, statefulSets[i], parallelStatefulSetCreation, namespace,
					allowedTopologies, stsScaleUp, stsScaleDown, verifyTopologyAffinity)
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
			}
			if i == 1 {
				stsScaleUp = false
				scaleDownReplicaCount = 5
				framework.Logf("Scale down StatefulSet2 replica count to 5")
				err = performScalingOnStatefulSetAndVerifyPvNodeAffinity(ctx, client, scaleUpReplicaCount,
					scaleDownReplicaCount, statefulSets[i], parallelStatefulSetCreation, namespace,
					allowedTopologies, stsScaleUp, stsScaleDown, verifyTopologyAffinity)
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
			}
			if i == 2 {
				scaleUpReplicaCount = 12
				scaleDownReplicaCount = 9
				framework.Logf("Scale up StatefulSet3 replica count to 12 and later scale down replica " +
					"count  to 9")
				err = performScalingOnStatefulSetAndVerifyPvNodeAffinity(ctx, client, scaleUpReplicaCount,
					scaleDownReplicaCount, statefulSets[i], parallelStatefulSetCreation, namespace,
					allowedTopologies, stsScaleUp, stsScaleDown, verifyTopologyAffinity)
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
			}
		}
	})

	/* Testcase-2
		VC1 has the workload up and running, Now bring down the ESX in VC1 and try to scale up the statefulset

	    1. Create storage policy under VC1 and VC2
	    2. Use the same storage policy and create statefulset of 10 replica
	    3. Wait for all the PVCs to bound and Pods to reach running state
	    4. Bring partial site down, Power off few esxi in VC1
	    5. Scale up the statefulset
	    6. Since the ESXs and nodes are down in VC1, running pods will reach error state,
		new Statefulset creation will not happen
	    7. Bring up one ESX in VC1
	    8. Migrate the nodes in VC1 and make it move to Powered on ESX
	    9. Statefulset should slowly coma back to the running state
	    10. New statefulset creation should take place on VC1 and VC2
	    11. Power on all the ESXs in VC1
	    12. Make sure common validation points are met on PV,PVC and POD
	    13. Clean up the data
	*/

	ginkgo.It("[pq-multivc] Bring down few esx on VC1 availability zones in a multi-vc setup", ginkgo.Label(p1,
		block, vanilla, multiVc, vc70, flaky, disruptive), func() {

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		statefulSetReplicaCount = 10
		scParameters[scParamStoragePolicyName] = storagePolicyInVc1Vc2
		topValStartIndex = 0
		topValEndIndex = 2
		scaleUpReplicaCount = 15
		parallelPodPolicy = true
		clientIndex := 0
		noOfHostToBringDown := 1
		var powerOffHostsLists []string

		ginkgo.By("Set specific allowed topology")
		allowedTopologies = setSpecificAllowedTopology(allowedTopologies, topkeyStartIndex, topValStartIndex,
			topValEndIndex)

		ginkgo.By("Create SC with storage policy which is available in VC1 and VC2")
		storageclass, err := createStorageClass(client, scParameters, nil, "", "", false, "nginx-sc")
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		defer func() {
			err := client.StorageV1().StorageClasses().Delete(ctx, storageclass.Name, *metav1.NewDeleteOptions(0))
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}()

		ginkgo.By("Create StatefulSet and verify pv affinity and pod affinity details")
		service, statefulset, err := createStafeulSetAndVerifyPVAndPodNodeAffinty(ctx, client, namespace,
			parallelPodPolicy, statefulSetReplicaCount, false, allowedTopologies,
			false, parallelStatefulSetCreation, false, "", nil, verifyTopologyAffinity, "")
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		defer func() {
			deleteAllStsAndPodsPVCsInNamespace(ctx, client, namespace)
			deleteService(namespace, client, service)
		}()

		ginkgo.By("Verify PV node affinity and that the PODS are running on appropriate node")
		err = verifyPVnodeAffinityAndPODnodedetailsForStatefulsetsLevel5(ctx, client,
			statefulset, namespace, allowedTopologies, parallelStatefulSetCreation)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Bring down few ESXI hosts i.e. partial site down on VC1 in a multi-setup")
		// read testbedinfo json for VC1
		readVcEsxIpsViaTestbedInfoJson(GetAndExpectStringEnvVar(envTestbedInfoJsonPathVC1))

		// fetch cluster name
		clusterComputeResource, _, err = getClusterNameForMultiVC(ctx, &multiVCe2eVSphere, clientIndex)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		elements := strings.Split(clusterComputeResource[0].InventoryPath, "/")
		clusterName := elements[len(elements)-1]

		// get list of hosts in a cluster
		hostsInCluster = getHostsByClusterName(ctx, clusterComputeResource, clusterName)

		// Bring down esxi hosts and append the hosts list which needs to be powered on later
		powerOffHostsList := powerOffEsxiHostsInMultiVcCluster(ctx, &multiVCe2eVSphere,
			noOfHostToBringDown, hostsInCluster)
		powerOffHostsLists = append(powerOffHostsLists, powerOffHostsList...)
		defer func() {
			ginkgo.By("Bring up  ESXi host which were powered off in VC1 in a multi setup")
			for i := 0; i < len(powerOffHostsLists); i++ {
				powerOnEsxiHostByCluster(powerOffHostsLists[i])
			}

			ginkgo.By("Wait for k8s cluster to be healthy")
			wait4AllK8sNodesToBeUp(nodeList)
			err = waitForAllNodes2BeReady(ctx, client, pollTimeout*4)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())

		}()

		ginkgo.By("Wait for k8s cluster to be healthy")
		wait4AllK8sNodesToBeUp(nodeList)
		err = waitForAllNodes2BeReady(ctx, client, pollTimeout*4)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Perform scaleup/scaledown operation on statefulset and verify pv and pod affinity details")
		stsScaleDown = false
		err = performScalingOnStatefulSetAndVerifyPvNodeAffinity(ctx, client, scaleUpReplicaCount,
			scaleDownReplicaCount, statefulset, parallelStatefulSetCreation, namespace,
			allowedTopologies, stsScaleUp, stsScaleDown, verifyTopologyAffinity)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Bring up  ESXi host which were powered off in VC1 in a multi setup")
		for i := 0; i < len(powerOffHostsLists); i++ {
			powerOnEsxiHostByCluster(powerOffHostsLists[i])
		}
	})

	/* Testcase-3
		Full site down on VC2  ESXs

	    1. Create SC which allowed to provision volume on both VC1 and VC2 AZs
	    2. Create 3 statefulset each with 5 replica
	    3. Bring down all ESX in VC2
	    4. Even though the ESXs in VC2 were down volume provisioning should succeed on VC1
	    5. Wait for all the Statefulsets to come up
	    6. Bring up the ESX hosts which were powered off in step2
	    7. Scale-up/Scale-down the statefulsets
	    8. Make sure common validation points are met on PV,PVC and POD
	    9. Clean up the data
	*/

	ginkgo.It("[pq-multivc] Bring down full site VC2 in a multi setup", ginkgo.Label(p1,
		block, vanilla, multiVc, vc70, flaky, disruptive), func() {

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		statefulSetReplicaCount = 5
		topValStartIndex = 0
		topValEndIndex = 2
		parallelPodPolicy = true
		clientIndex := 1
		noOfHostToBringDown := 3
		var powerOffHostsLists []string
		scParameters[scParamStoragePolicyName] = storagePolicyInVc1Vc2
		sts_count := 3

		ginkgo.By("Set specific allowed topology")
		allowedTopologies = setSpecificAllowedTopology(allowedTopologies, topkeyStartIndex, topValStartIndex,
			topValEndIndex)

		ginkgo.By("Create SC with availability zone in VC1 and VC2")
		storageclass, err := createStorageClass(client, scParameters, allowedTopologies, "", "", false,
			"nginx-sc")
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		defer func() {
			err := client.StorageV1().StorageClasses().Delete(ctx, storageclass.Name, *metav1.NewDeleteOptions(0))
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}()

		ginkgo.By("Create service")
		service := CreateService(namespace, client)
		defer func() {
			deleteService(namespace, client, service)
		}()

		ginkgo.By("Create 3 StatefulSet with replica count 5")
		statefulSets := createParallelStatefulSetSpec(namespace, sts_count, statefulSetReplicaCount)
		var wg sync.WaitGroup
		wg.Add(sts_count)
		for i := 0; i < len(statefulSets); i++ {
			go createParallelStatefulSets(client, namespace, statefulSets[i],
				statefulSetReplicaCount, &wg)

		}
		wg.Wait()
		defer func() {
			deleteAllStsAndPodsPVCsInNamespace(ctx, client, namespace)
		}()

		ginkgo.By("Bring down all ESXI hosts in VC2 in a multi-setup")
		// read testbedinfo json for VC2
		readVcEsxIpsViaTestbedInfoJson(GetAndExpectStringEnvVar(envTestbedInfoJsonPathVC2))

		// fetch cluster name
		clusterComputeResource, _, err = getClusterNameForMultiVC(ctx, &multiVCe2eVSphere, clientIndex)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		elements := strings.Split(clusterComputeResource[0].InventoryPath, "/")
		clusterName := elements[len(elements)-1]

		// get list of hosts in a cluster
		hostsInCluster = getHostsByClusterName(ctx, clusterComputeResource, clusterName)

		// Bring down esxi hosts and append the hosts list which needs to be powered on later
		powerOffHostsList := powerOffEsxiHostsInMultiVcCluster(ctx, &multiVCe2eVSphere,
			noOfHostToBringDown, hostsInCluster)
		powerOffHostsLists = append(powerOffHostsLists, powerOffHostsList...)
		defer func() {
			ginkgo.By("Bring up  ESXi host which were powered off in VC2 in a multi setup")
			for i := 0; i < len(powerOffHostsLists); i++ {
				powerOnEsxiHostByCluster(powerOffHostsLists[i])
			}

			ginkgo.By("Wait for k8s cluster to be healthy")
			wait4AllK8sNodesToBeUp(nodeList)
			err = waitForAllNodes2BeReady(ctx, client, pollTimeout*4)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())

		}()

		ginkgo.By("Bring up  ESXi host which were powered off in VC2 in a multi setup")
		for i := 0; i < len(powerOffHostsLists); i++ {
			powerOnEsxiHostByCluster(powerOffHostsLists[i])
		}

		ginkgo.By("Wait for k8s cluster to be healthy")
		wait4AllK8sNodesToBeUp(nodeList)
		err = waitForAllNodes2BeReady(ctx, client, pollTimeout*4)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Waiting for StatefulSets Pods to be in Ready State")
		err = waitForStsPodsToBeInReadyRunningState(ctx, client, namespace, statefulSets)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Perform scaleup/scaledown operation on statefulset and " +
			"verify pv and pod affinity details")
		for i := 0; i < len(statefulSets); i++ {
			if i == 0 {
				stsScaleDown = false
				scaleUpReplicaCount = 12
				framework.Logf("Scale up StatefulSet1 replica count to 12")
				err = performScalingOnStatefulSetAndVerifyPvNodeAffinity(ctx, client, scaleUpReplicaCount,
					scaleDownReplicaCount, statefulSets[i], parallelStatefulSetCreation, namespace,
					allowedTopologies, stsScaleUp, stsScaleDown, verifyTopologyAffinity)
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
			}
			if i == 1 {
				stsScaleUp = false
				scaleDownReplicaCount = 3
				framework.Logf("Scale down StatefulSet1 replica count to 12")
				err = performScalingOnStatefulSetAndVerifyPvNodeAffinity(ctx, client, scaleUpReplicaCount,
					scaleDownReplicaCount, statefulSets[i], parallelStatefulSetCreation, namespace,
					allowedTopologies, stsScaleUp, stsScaleDown, verifyTopologyAffinity)
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
			}
			if i == 2 {
				scaleUpReplicaCount = 15
				scaleDownReplicaCount = 1
				framework.Logf("Scale up replica count to 15 and later scale down replica count to 1")
				err = performScalingOnStatefulSetAndVerifyPvNodeAffinity(ctx, client, scaleUpReplicaCount,
					scaleDownReplicaCount, statefulSets[i], parallelStatefulSetCreation, namespace,
					allowedTopologies, stsScaleUp, stsScaleDown, verifyTopologyAffinity)
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
			}
		}
	})

	/* Testcase-4
	Full site down on VC1 ESXs

	    1. Create SC which allowed to provisionn volume on both vc1 and VC2 AZs
	    2. Create 3 statefulset each with 5 replica
	    3. Bring down all ESXs in VC1
	    4. Even though the ESX's in VC1 were down volume provisioning should succeed on VC2
	    5. Wait for all the statefulsets to come up
	    6. Bring up the ESX hosts which were powered off in step2
	    7. Scale-up/Scale-down the Statefulsets
	    8. Make sure common validation points are met on PV,PVC and POD
	    9. Clean up the data
	*/

	ginkgo.It("[pq-multivc] Bring down full site VC1 in a multi setup", ginkgo.Label(p1,
		block, vanilla, multiVc, vc70, flaky, disruptive), func() {

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		statefulSetReplicaCount = 5
		topValStartIndex = 0
		topValEndIndex = 2
		parallelPodPolicy = true
		clientIndex := 0
		noOfHostToBringDown := 4
		var powerOffHostsLists []string
		sts_count := 3
		scParameters[scParamStoragePolicyName] = storagePolicyInVc1Vc2

		ginkgo.By("Set specific allowed topology")
		allowedTopologies = setSpecificAllowedTopology(allowedTopologies, topkeyStartIndex, topValStartIndex,
			topValEndIndex)

		ginkgo.By("Create SC with availability zone in VC1 and VC2")
		storageclass, err := createStorageClass(client, scParameters, allowedTopologies, "", "", false, "nginx-sc")
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		defer func() {
			err := client.StorageV1().StorageClasses().Delete(ctx, storageclass.Name, *metav1.NewDeleteOptions(0))
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}()

		ginkgo.By("Create service")
		service := CreateService(namespace, client)
		defer func() {
			deleteService(namespace, client, service)
		}()

		ginkgo.By("Create 3 StatefulSet with replica count 5")
		statefulSets := createParallelStatefulSetSpec(namespace, sts_count, statefulSetReplicaCount)
		var wg sync.WaitGroup
		wg.Add(sts_count)
		for i := 0; i < len(statefulSets); i++ {
			go createParallelStatefulSets(client, namespace, statefulSets[i],
				statefulSetReplicaCount, &wg)

		}
		wg.Wait()
		defer func() {
			deleteAllStsAndPodsPVCsInNamespace(ctx, client, namespace)
		}()

		ginkgo.By("Bring down all ESXI hosts in VC1 in a multi-setup")
		// read testbedinfo json for VC2
		readVcEsxIpsViaTestbedInfoJson(GetAndExpectStringEnvVar(envTestbedInfoJsonPathVC1))

		// fetch cluster name
		clusterComputeResource, _, err = getClusterNameForMultiVC(ctx, &multiVCe2eVSphere, clientIndex)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		elements := strings.Split(clusterComputeResource[0].InventoryPath, "/")
		clusterName := elements[len(elements)-1]

		// get list of hosts in a cluster
		hostsInCluster = getHostsByClusterName(ctx, clusterComputeResource, clusterName)

		// Bring down esxi hosts and append the hosts list which needs to be powered on later
		powerOffHostsList := powerOffEsxiHostsInMultiVcCluster(ctx, &multiVCe2eVSphere,
			noOfHostToBringDown, hostsInCluster)
		powerOffHostsLists = append(powerOffHostsLists, powerOffHostsList...)

		defer func() {
			ginkgo.By("Bring up  ESXi host which were powered off in VC1 in a multi setup")
			for i := 0; i < len(powerOffHostsLists); i++ {
				powerOnEsxiHostByCluster(powerOffHostsLists[i])
			}

			ginkgo.By("Wait for k8s cluster to be healthy")
			wait4AllK8sNodesToBeUp(nodeList)
			err = waitForAllNodes2BeReady(ctx, client, pollTimeout*4)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())

		}()

		ginkgo.By("Bring up  ESXi host which were powered off in VC1 in a multi setup")
		for i := 0; i < len(powerOffHostsLists); i++ {
			powerOnEsxiHostByCluster(powerOffHostsLists[i])
		}

		ginkgo.By("Wait for k8s cluster to be healthy")
		wait4AllK8sNodesToBeUp(nodeList)
		err = waitForAllNodes2BeReady(ctx, client, pollTimeout*4)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Waiting for StatefulSets Pods to be in Ready State")
		err = waitForStsPodsToBeInReadyRunningState(ctx, client, namespace, statefulSets)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Perform scaleup/scaledown operation on statefulset and " +
			"verify pv and pod affinity details")
		for i := 0; i < len(statefulSets); i++ {
			if i == 0 {
				stsScaleDown = false
				scaleUpReplicaCount = 12
				framework.Logf("Scale up StatefulSet1 replica count to 12")
				err = performScalingOnStatefulSetAndVerifyPvNodeAffinity(ctx, client, scaleUpReplicaCount,
					scaleDownReplicaCount, statefulSets[i], parallelStatefulSetCreation, namespace,
					allowedTopologies, stsScaleUp, stsScaleDown, verifyTopologyAffinity)
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
			}
			if i == 1 {
				stsScaleUp = false
				scaleDownReplicaCount = 3
				framework.Logf("Scale down StatefulSet1 replica count to 12")
				err = performScalingOnStatefulSetAndVerifyPvNodeAffinity(ctx, client, scaleUpReplicaCount,
					scaleDownReplicaCount, statefulSets[i], parallelStatefulSetCreation, namespace,
					allowedTopologies, stsScaleUp, stsScaleDown, verifyTopologyAffinity)
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
			}
			if i == 2 {
				scaleUpReplicaCount = 15
				scaleDownReplicaCount = 1
				framework.Logf("Scale up replica count to 15 and later scale down replica count to 1")
				err = performScalingOnStatefulSetAndVerifyPvNodeAffinity(ctx, client, scaleUpReplicaCount,
					scaleDownReplicaCount, statefulSets[i], parallelStatefulSetCreation, namespace,
					allowedTopologies, stsScaleUp, stsScaleDown, verifyTopologyAffinity)
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
			}
		}
	})

	/* Testcase-5
		Disconnect, suspend and put datastore in maintenance mode on each cluster

	    1. In every cluster, either disconnect(power of the datastore-ip),
		suspend and put datastore in manitenance mode
	    2. Create SC
	    3. Create 3 Statefulset each with 10 replica's
	    4. Volume provisioning should take place on the active datastores
	    5. Connect back all the datastores
	    6. Scale-up/Scale-down the statefulsets
	    7. Make sure common validation points are met on PV,PVC and POD
	    8. Clean up the data
	*/

	ginkgo.It("[pq-multivc] Bring down datastores in a multi vc setup", ginkgo.Label(p2,
		block, vanilla, multiVc, vc70, flaky, disruptive), func() {

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		statefulSetReplicaCount = 10
		parallelPodPolicy = true
		clientIndex := 1
		sts_count := 3
		isDatastoreInMaintenanceMode := false

		ginkgo.By("Create SC with allowed topology set to all the VCs")
		storageclass, err := createStorageClass(client, nil, allowedTopologies, "", "", false, "nginx-sc")
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		defer func() {
			err := client.StorageV1().StorageClasses().Delete(ctx, storageclass.Name, *metav1.NewDeleteOptions(0))
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}()

		ginkgo.By("Create service")
		service := CreateService(namespace, client)
		defer func() {
			deleteService(namespace, client, service)
		}()

		ginkgo.By("Migrate all the worker vms residing on the NFS datatsore before " +
			"making datastore into maintenance mode in VC-2")
		var destDsName string
		for datastore := range ClusterdatastoreListVC2 {
			if !strings.Contains(datastore, "nfs") {
				destDsName = datastore
				break
			}
		}

		framework.Logf("Fetch worker vms sitting on VC-2")
		clientIndex = 1
		vMsToMigrate, err := fetchWorkerNodeVms(masterIp, sshClientConfig, dataCenters, workerInitialAlias[0],
			clientIndex)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		framework.Logf("Move worker vms to destination datastore in VC-2")
		isMigrateSuccess, err := migrateVmsFromDatastore(masterIp, sshClientConfig, destDsName,
			vMsToMigrate, clientIndex)
		gomega.Expect(isMigrateSuccess).To(gomega.BeTrue(), "Migration of vms failed")
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		framework.Logf("Perform suspend operation on the source NFS datastore")
		soureDsName := "nfs"
		suspendDatastoreOp := "off"
		datastoreName := suspendDatastore(ctx, suspendDatastoreOp, soureDsName, envTestbedInfoJsonPathVC2)
		suspendDatastoreOp = "on"
		defer func() {
			if suspendDatastoreOp == "on" {
				ginkgo.By("Power on the suspended datastore")
				resumeDatastore(datastoreName, suspendDatastoreOp, envTestbedInfoJsonPathVC2)
			}
		}()

		// here fetching the source datastore and destination datastore

		i := 0
		for datastore := range ClusterdatastoreListVC3 {
			if i == 0 {
				if !strings.Contains("vsan", datastore) {
					soureDsName = datastore
				}
			}
			if i == 1 {
				destDsName = datastore
			}
			i++
		}

		framework.Logf("Fetch worker vms sitting on VC-3")
		clientIndex = 2
		vMsToMigrate, err = fetchWorkerNodeVms(masterIp, sshClientConfig, dataCenters, workerInitialAlias[0],
			clientIndex)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		framework.Logf("Move all the vms to destination datastore")
		isMigrateSuccess, err = migrateVmsFromDatastore(masterIp, sshClientConfig, destDsName, vMsToMigrate,
			clientIndex)
		gomega.Expect(isMigrateSuccess).To(gomega.BeTrue(), "Migration of vms failed")
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		framework.Logf("Put source datastore in maintenance mode on VC-3 multi setup")
		err = preferredDatastoreInMaintenanceMode(masterIp, sshClientConfig, dataCenters, soureDsName,
			clientIndex)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		isDatastoreInMaintenanceMode = true
		defer func() {
			if isDatastoreInMaintenanceMode {
				err = exitDatastoreFromMaintenanceMode(masterIp, sshClientConfig, soureDsName,
					clientIndex)
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
				isDatastoreInMaintenanceMode = false
			}
		}()

		ginkgo.By("Create 3 StatefulSet with replica count 5")
		statefulSets := createParallelStatefulSetSpec(namespace, sts_count, statefulSetReplicaCount)
		var wg sync.WaitGroup
		wg.Add(sts_count)
		for i := 0; i < len(statefulSets); i++ {
			go createParallelStatefulSets(client, namespace, statefulSets[i],
				statefulSetReplicaCount, &wg)

		}
		wg.Wait()

		ginkgo.By("Waiting for StatefulSets Pods to be in Ready State")
		err = waitForStsPodsToBeInReadyRunningState(ctx, client, namespace, statefulSets)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		defer func() {
			deleteAllStsAndPodsPVCsInNamespace(ctx, client, namespace)
		}()

		ginkgo.By("Power on the suspended datastore residing on VC-2")
		suspendDatastoreOp = "on"
		powerOnPreferredDatastore(datastoreName, suspendDatastoreOp)
		suspendDatastoreOp = "off"

		framework.Logf("Exit datastore from maintenance mode residing on VC-3")
		err = exitDatastoreFromMaintenanceMode(masterIp, sshClientConfig, soureDsName, 2)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		isDatastoreInMaintenanceMode = false

		ginkgo.By("Perform scaleup/scaledown operation on statefulset and " +
			"verify pv and pod affinity details")
		for i := 0; i < len(statefulSets); i++ {
			if i == 0 {
				stsScaleDown = false
				scaleUpReplicaCount = 12
				framework.Logf("Scale up StatefulSet1 replica count to 12")
				err = performScalingOnStatefulSetAndVerifyPvNodeAffinity(ctx, client, scaleUpReplicaCount,
					scaleDownReplicaCount, statefulSets[i], parallelStatefulSetCreation, namespace,
					allowedTopologies, stsScaleUp, stsScaleDown, verifyTopologyAffinity)
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
			}
			if i == 1 {
				stsScaleUp = false
				scaleDownReplicaCount = 3
				framework.Logf("Scale down StatefulSet1 replica count to 12")
				err = performScalingOnStatefulSetAndVerifyPvNodeAffinity(ctx, client, scaleUpReplicaCount,
					scaleDownReplicaCount, statefulSets[i], parallelStatefulSetCreation, namespace,
					allowedTopologies, stsScaleUp, stsScaleDown, verifyTopologyAffinity)
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
			}
			if i == 2 {
				scaleUpReplicaCount = 15
				scaleDownReplicaCount = 1
				framework.Logf("Scale up replica count to 15 and later scale down replica count to 1")
				err = performScalingOnStatefulSetAndVerifyPvNodeAffinity(ctx, client, scaleUpReplicaCount,
					scaleDownReplicaCount, statefulSets[i], parallelStatefulSetCreation, namespace,
					allowedTopologies, stsScaleUp, stsScaleDown, verifyTopologyAffinity)
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
			}
		}
	})
})
