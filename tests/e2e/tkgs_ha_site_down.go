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
	"strconv"
	"strings"
	"time"

	"github.com/onsi/ginkgo/v2"
	"github.com/onsi/gomega"
	"golang.org/x/crypto/ssh"
	appsv1 "k8s.io/api/apps/v1"
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

var _ = ginkgo.Describe("[csi-tkgs-ha] Tkgs-HA-SiteDownTests", func() {
	f := framework.NewDefaultFramework("e2e-tkgs-ha")
	f.NamespacePodSecurityEnforceLevel = admissionapi.LevelPrivileged
	var (
		client               clientset.Interface
		namespace            string
		scParameters         map[string]string
		allowedTopologyHAMap map[string][]string
		categories           []string
		zonalPolicy          string
		zonalWffcPolicy      string
		sshWcpConfig         *ssh.ClientConfig
	)
	ginkgo.BeforeEach(func() {
		client = f.ClientSet
		namespace = getNamespaceToRunTests(f)
		bootstrap()
		scParameters = make(map[string]string)
		topologyHaMap := GetAndExpectStringEnvVar(topologyHaMap)
		_, categories = createTopologyMapLevel5(topologyHaMap, tkgshaTopologyLevels)
		allowedTopologies := createAllowedTopolgies(topologyHaMap, tkgshaTopologyLevels)
		allowedTopologyHAMap = createAllowedTopologiesMap(allowedTopologies)
		framework.Logf("Topology map: %v, categories: %v", allowedTopologyHAMap, categories)
		zonalPolicy = GetAndExpectStringEnvVar(envZonalStoragePolicyName)
		if zonalPolicy == "" {
			ginkgo.Fail(envZonalStoragePolicyName + " env variable not set")
		}
		zonalWffcPolicy = GetAndExpectStringEnvVar(envZonalWffcStoragePolicyName)
		if zonalWffcPolicy == "" {
			ginkgo.Fail(envZonalWffcStoragePolicyName + " env variable not set")
		}
		framework.Logf("zonal policy: %s and zonal wffc policy: %s", zonalPolicy, zonalWffcPolicy)

		nodeList, err := fnodes.GetReadySchedulableNodes(client)
		framework.ExpectNoError(err, "Unable to find ready and schedulable Node")
		if !(len(nodeList.Items) > 0) {
			framework.Failf("Unable to find ready and schedulable Node")
		}

		svcMasterPwd := GetAndExpectStringEnvVar(svcMasterPassword)
		sshWcpConfig = &ssh.ClientConfig{
			User: rootUser,
			Auth: []ssh.AuthMethod{
				ssh.Password(svcMasterPwd),
			},
			HostKeyCallback: ssh.InsecureIgnoreHostKey(),
		}

		if guestCluster {
			svcClient, svNamespace := getSvcClientAndNamespace()
			setResourceQuota(svcClient, svNamespace, rqLimit)
		}
		readVcEsxIpsViaTestbedInfoJson(GetAndExpectStringEnvVar(envTestbedInfoJsonPath))

	})

	/*
		Bring down ESX in AZ1
		1. Use Zonal storage class of AZ1 with immediate binding
		   and create 5 statefulset with 3 replica's
		2. Bring down AZ1
		3. Stateful set pods which is created using zonal storage class
		   should remain in terminating state
		4. Bring up all the ESX's of AZ1 up
		5. Wait for stateful set in step 3 to come up
		6. Increase the statefulset replica count to 5
		7. make sure stateful set's get  distributed among all the AZ's .
		   And make sure PVC's and POD's are up and running in AZ1 as well
		8. validate the annotation on SVC-PVC , and node affinity
		   on PV on both SVC and GC namespace.
		9. Make sure newly created PVC's And POD's are up and POD's
		   scheduled on appropriate nodes.
		   Svc-pv and gc-pv's should have appropriate node affinity.
		10.Wait and verify that the k8s cluster is healthy and statefulset replicas are running fine
		11.Clean up the data
	*/
	ginkgo.It("Bring down ESX in AZ1", func() {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		ginkgo.By("CNS_TEST: Running for GC setup")
		nodeList, err := fnodes.GetReadySchedulableNodes(client)
		framework.ExpectNoError(err, "Unable to find ready and schedulable Node")
		if !(len(nodeList.Items) > 0) {
			framework.Failf("Unable to find ready and schedulable Node")
		}
		var replicas int32 = 3
		stsCount := 5
		var stsList []*appsv1.StatefulSet

		ginkgo.By("Create statefulset with parallel pod management policy with replica 3")
		createResourceQuota(client, namespace, rqLimit, zonalPolicy)
		scParameters[svStorageClassName] = zonalPolicy
		storageclass, err := client.StorageV1().StorageClasses().Get(ctx, zonalPolicy, metav1.GetOptions{})
		if !apierrors.IsNotFound(err) {
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}

		// Creating StatefulSet service
		ginkgo.By("Creating service")
		service := CreateService(namespace, client)
		defer func() {
			deleteService(namespace, client, service)
		}()

		// Get Cluster details
		clusterComputeResource, _, err := getClusterName(ctx, &e2eVSphere)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		svcMasterIp := getApiServerIpOfZone(ctx, "zone-2")

		clusterName := getClusterNameFromZone(ctx, "zone-1")
		for i := 0; i < stsCount; i++ {
			statefulset := GetStatefulSetFromManifest(namespace)
			ginkgo.By("Creating statefulset")
			statefulset.Spec.PodManagementPolicy = appsv1.ParallelPodManagement
			statefulset.Name = "sts-" + strconv.Itoa(i) + "-" + statefulset.Name
			statefulset.Spec.Template.Labels["app"] = statefulset.Name
			statefulset.Spec.Selector.MatchLabels["app"] = statefulset.Name
			statefulset.Spec.VolumeClaimTemplates[len(statefulset.Spec.VolumeClaimTemplates)-1].
				Annotations["volume.beta.kubernetes.io/storage-class"] = storageclass.Name
			*statefulset.Spec.Replicas = replicas
			CreateStatefulSet(namespace, statefulset, client)
			stsList = append(stsList, statefulset)

		}

		defer func() {
			scaleDownNDeleteStsDeploymentsInNamespace(ctx, client, namespace)
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

		ginkgo.By("Verify if all sts replicas are in Running state")
		for _, statefulset := range stsList {
			fss.WaitForStatusReadyReplicas(client, statefulset, replicas)
			gomega.Expect(fss.CheckMount(client, statefulset, mountPath)).NotTo(gomega.HaveOccurred())
			ssPodsBeforeScaleDown := fss.GetPodList(client, statefulset)
			gomega.Expect(ssPodsBeforeScaleDown.Items).NotTo(gomega.BeEmpty(),
				fmt.Sprintf("Unable to get list of Pods from the Statefulset: %v", statefulset.Name))
			gomega.Expect(len(ssPodsBeforeScaleDown.Items) == int(replicas)).To(gomega.BeTrue(),
				"Number of Pods in the statefulset should match with number of replicas")
		}
		ignoreLabels := make(map[string]string)
		podList, err := fpod.GetPodsInNamespace(client, namespace, ignoreLabels)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Bring down ESX hosts of AZ1")
		hostsInCluster := getHostsByClusterName(ctx, clusterComputeResource, clusterName)
		powerOffHostsList := powerOffEsxiHostByCluster(ctx, &e2eVSphere, clusterName,
			len(hostsInCluster))
		defer func() {
			ginkgo.By("Bring up ESXi host which were powered off in zone1")
			for i := 0; i < len(powerOffHostsList); i++ {
				powerOnEsxiHostByCluster(powerOffHostsList[i])
			}
		}()
		framework.Logf("Sleeping for 5 mins for gc nodes to go fully down")
		time.Sleep(5 * time.Minute)
		nodeNames := getNodesOfZone(nodeList, "zone-1")
		framework.Logf("nodeNames: %v", nodeNames)
		podNames := getPodsFromNodeNames(podList, nodeNames)
		framework.Logf("podInfo: %v", podNames)

		ginkgo.By("Verify if sts pods of zone-1 are in Terminating state")
		for _, podName := range podNames {
			err = waitForPodsToBeInTerminatingPhase(sshWcpConfig, svcMasterIp,
				podName, namespace, pollTimeout*2)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}

		ginkgo.By("Bring up ESXi host which were powered off in zone1")
		for i := 0; i < len(powerOffHostsList); i++ {
			powerOnEsxiHostByCluster(powerOffHostsList[i])
		}
		framework.Logf("Sleeping for 5 mins for gc to create new nodes and be fully up")
		time.Sleep(5 * time.Minute)

		ginkgo.By("Verify SVC PVC annotations and node affinities on GC and SVC PVs")
		for _, statefulset := range stsList {
			verifyVolumeMetadataOnStatefulsets(client, ctx, namespace, statefulset, replicas,
				allowedTopologyHAMap, categories, zonalPolicy, nodeList, f)
		}

	})

	/*
		Bring down ESX of AZ1 and AZ2
		1. Use Zonal storage class of AZ1 with immediate binding
		   and create 5 statefulset with 3 replica's
		2. Bring down AZ1 and AZ2
		3. Stateful set pods which is created using zonal storage class
		   created in AZ1 and AZ2 should remain in terminating state
		4. Bring up all the ESX's of AZ1 up
		5. Wait for stateful set in step 3 to come up
		6. Increase the statefulset replica count to 5
		7. make sure stateful set's get  distributed among all the AZ's .
		   And make sure PVC's and POD's are up and running in AZ1 as well
		8. validate the annotation on SVC-PVC , and node affinity
		   on PV on both SVC and GC namespace.
		9. Make sure newly created PVC's And POD's are up and POD's
		   scheduled on appropriate nodes.
		   Svc-pv and gc-pv's should have appropriate node affinity.
		10.Wait and verify that the k8s cluster is healthy and statefulset replicas are running fine
		11.Clean up the data
	*/
	ginkgo.It("Bring down ESX of AZ1 and AZ2", func() {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		ginkgo.By("CNS_TEST: Running for GC setup")
		nodeList, err := fnodes.GetReadySchedulableNodes(client)
		framework.ExpectNoError(err, "Unable to find ready and schedulable Node")
		if !(len(nodeList.Items) > 0) {
			framework.Failf("Unable to find ready and schedulable Node")
		}
		var replicas int32 = 3
		stsCount := 5
		var stsList []*appsv1.StatefulSet

		ginkgo.By("Create statefulset with parallel pod management policy with replica 3")
		createResourceQuota(client, namespace, rqLimit, zonalPolicy)
		scParameters[svStorageClassName] = zonalPolicy
		storageclass, err := client.StorageV1().StorageClasses().Get(ctx, zonalPolicy, metav1.GetOptions{})
		if !apierrors.IsNotFound(err) {
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}

		// Creating StatefulSet service
		ginkgo.By("Creating service")
		service := CreateService(namespace, client)
		defer func() {
			deleteService(namespace, client, service)
		}()

		// Get Cluster details
		clusterComputeResource, _, err := getClusterName(ctx, &e2eVSphere)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		clusterName1 := getClusterNameFromZone(ctx, "zone-1")
		clusterName2 := getClusterNameFromZone(ctx, "zone-2")
		svcMasterIp := getApiServerIpOfZone(ctx, "zone-3")

		for i := 0; i < stsCount; i++ {
			statefulset := GetStatefulSetFromManifest(namespace)
			ginkgo.By("Creating statefulset")
			statefulset.Spec.PodManagementPolicy = appsv1.ParallelPodManagement
			statefulset.Name = "sts-" + strconv.Itoa(i) + "-" + statefulset.Name
			statefulset.Spec.Template.Labels["app"] = statefulset.Name
			statefulset.Spec.Selector.MatchLabels["app"] = statefulset.Name
			statefulset.Spec.VolumeClaimTemplates[len(statefulset.Spec.VolumeClaimTemplates)-1].
				Annotations["volume.beta.kubernetes.io/storage-class"] = storageclass.Name
			*statefulset.Spec.Replicas = replicas
			CreateStatefulSet(namespace, statefulset, client)
			stsList = append(stsList, statefulset)

		}

		defer func() {
			scaleDownNDeleteStsDeploymentsInNamespace(ctx, client, namespace)
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

		ginkgo.By("Verify if all sts replicas are in Running state")
		for _, statefulset := range stsList {
			fss.WaitForStatusReadyReplicas(client, statefulset, replicas)
			gomega.Expect(fss.CheckMount(client, statefulset, mountPath)).NotTo(gomega.HaveOccurred())
			ssPodsBeforeScaleDown := fss.GetPodList(client, statefulset)
			gomega.Expect(ssPodsBeforeScaleDown.Items).NotTo(gomega.BeEmpty(),
				fmt.Sprintf("Unable to get list of Pods from the Statefulset: %v", statefulset.Name))
			gomega.Expect(len(ssPodsBeforeScaleDown.Items) == int(replicas)).To(gomega.BeTrue(),
				"Number of Pods in the statefulset should match with number of replicas")
		}

		ignoreLabels := make(map[string]string)
		podList, err := fpod.GetPodsInNamespace(client, namespace, ignoreLabels)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		var powerOffHostsList []string
		ginkgo.By("Bring down ESX hosts of AZ1 and AZ2")
		hostsInCluster1 := getHostsByClusterName(ctx, clusterComputeResource, clusterName1)
		powerOffHostsList1 := powerOffEsxiHostByCluster(ctx, &e2eVSphere, clusterName1,
			len(hostsInCluster1))

		hostsInCluster2 := getHostsByClusterName(ctx, clusterComputeResource, clusterName2)
		powerOffHostsList2 := powerOffEsxiHostByCluster(ctx, &e2eVSphere, clusterName2,
			len(hostsInCluster2))
		powerOffHostsList = append(powerOffHostsList, powerOffHostsList1...)
		powerOffHostsList = append(powerOffHostsList, powerOffHostsList2...)
		defer func() {
			ginkgo.By("Bring up ESXi host which were powered off in zone1 and zone2")
			for i := 0; i < len(powerOffHostsList); i++ {
				powerOnEsxiHostByCluster(powerOffHostsList[i])
			}
		}()
		framework.Logf("Sleeping for 5 mins for gc nodes to go fully down")
		time.Sleep(5 * time.Minute)

		framework.Logf("Verify wcp apiserrver is unreachable as other apiservers are down")
		err = waitForPodsToBeInTerminatingPhase(sshWcpConfig, svcMasterIp,
			podList[0].Name, namespace, pollTimeout)
		if strings.Contains(err.Error(), "was refused") ||
			strings.Contains(err.Error(), "Unable to connect to the server") {
			framework.Logf("wcp apiserver ip is unaccessible")
		}

		ginkgo.By("Bring up ESXi host which were powered off in zone1")
		for i := 0; i < len(powerOffHostsList1); i++ {
			powerOnEsxiHostByCluster(powerOffHostsList1[i])
		}
		ginkgo.By("Waiting for apiserver of zone-3 to be reachable and fully up")
		err = waitForApiServerToBeUp(svcMasterIp, sshWcpConfig, pollTimeout*3)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		time.Sleep(5 * time.Minute)

		nodeNames := getNodesOfZone(nodeList, "zone-2")
		framework.Logf("nodeNames: %v", nodeNames)
		podNames := getPodsFromNodeNames(podList, nodeNames)
		framework.Logf("podInfo: %v", podNames)

		ginkgo.By("Verify if sts pods of zone-2 are in Terminating state")
		for _, podName := range podNames {
			err = waitForPodsToBeInTerminatingPhase(sshWcpConfig, svcMasterIp,
				podName, namespace, pollTimeout*2)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}

		ginkgo.By("Bring up ESXi host which were powered off in zone2")
		for i := 0; i < len(powerOffHostsList2); i++ {
			powerOnEsxiHostByCluster(powerOffHostsList2[i])
		}
		framework.Logf("Sleeping for 5 mins for gc to create new nodes and be fully up")
		time.Sleep(5 * time.Minute)

		ginkgo.By("Verify SVC PVC annotations and node affinities on GC and SVC PVs")
		for _, statefulset := range stsList {
			verifyVolumeMetadataOnStatefulsets(client, ctx, namespace, statefulset, replicas,
				allowedTopologyHAMap, categories, zonalPolicy, nodeList, f)
		}

	})
})
