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
	"os"
	"strconv"
	"strings"
	"time"

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

var _ = ginkgo.Describe("[rwx-multivc-operationstorm] RWX-MultiVc-OperationStorm", func() {
	f := framework.NewDefaultFramework("rwx-multivc-operationstorm")
	f.NamespacePodSecurityEnforceLevel = admissionapi.LevelPrivileged
	var (
		client                     clientset.Interface
		namespace                  string
		allowedTopologies          []v1.TopologySelectorLabelRequirement
		sshClientConfig            *ssh.ClientConfig
		nimbusGeneratedK8sVmPwd    string
		k8sVersion                 string
		ClusterdatastoreListVC     []map[string]string
		ClusterdatastoreListVC1    map[string]string
		ClusterdatastoreListVC2    map[string]string
		ClusterdatastoreListVC3    map[string]string
		allMasterIps               []string
		masterIp                   string
		clusterComputeResource     []*object.ClusterComputeResource
		nodeList                   *v1.NodeList
		topValStartIndex           int
		topValEndIndex             int
		topkeyStartIndex           int
		bindingModeImm             storagev1.VolumeBindingMode
		accessmode                 v1.PersistentVolumeAccessMode
		scParameters               map[string]string
		labelsMap                  map[string]string
		isVsanHealthServiceStopped bool
	)

	ginkgo.BeforeEach(func() {
		var cancel context.CancelFunc
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		client = f.ClientSet
		namespace = f.Namespace.Name

		multiVCbootstrap()

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
		nimbusGeneratedK8sVmPwd = GetAndExpectStringEnvVar(nimbusK8sVmPwd)

		sshClientConfig = &ssh.ClientConfig{
			User: "root",
			Auth: []ssh.AuthMethod{
				ssh.Password(nimbusGeneratedK8sVmPwd),
			},
			HostKeyCallback: ssh.InsecureIgnoreHostKey(),
		}

		// fetching k8s version
		v, err := client.Discovery().ServerVersion()
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		k8sVersion = v.Major + "." + v.Minor

		// fetching k8s master ip
		allMasterIps = getK8sMasterIPs(ctx, client)
		masterIp = allMasterIps[0]

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

		//global variables
		scParameters = make(map[string]string)
		labelsMap = make(map[string]string)
		accessmode = v1.ReadWriteMany
		bindingModeImm = storagev1.VolumeBindingImmediate

		//setting map values
		labelsMap["app"] = "test"
		scParameters[scParamFsType] = nfs4FSType
	})

	ginkgo.AfterEach(func() {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		ginkgo.By(fmt.Sprintf("Deleting all statefulsets in namespace: %v", namespace))
		fss.DeleteAllStatefulSets(ctx, client, namespace)
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

		if isVsanHealthServiceStopped {
			vCenterHostname := strings.Split(e2eVSphere.Config.Global.VCenterHostname, ",")
			vcAddress := vCenterHostname[0] + ":" + sshdPort
			framework.Logf("Bringing vsanhealth up before terminating the test")
			startVCServiceWait4VPs(ctx, vcAddress, vsanhealthServiceName, &isVsanHealthServiceStopped)
		}
	})

	/*
		VC-2 has the workload up and running Now bring down all ESXis in VC-2 and try to scale up the StatefulSet
		SC → specific allowed topology, → k8s-zone:zone-1 and zone-2, → Same Storage Policy name
		tagged to vSAN DS set in VC-1 and VC-2, → Immediate Binding mode

		Steps:
		1. Create a Storage Class with allowed topology set to Vc-1 and Vc-2 AZ and pass storage policy of vc1 and vc2
		2. Create few dynamic pvcs (around 5) using rwx access mode.
		3. Wait for PVcs to reach Bound state.
		4. Create deployment pods with replica count 2 using pvc created above.
		5. Wait for Pods to reach running ready state.
		6. Create statefulset with replica count 3.
		7. Bring down full site vc-1
		8. Create new rwx pvcs when sites are down.
		9. Perform scaleup operation on 1-3 deployment sets. Increase the replica count to 5.
		10. Perform scaledown operation on 4-5 deployment set. Decrease the replica count to 1.
		11. Perform scaleup operation on statefulset. Increase the replica count to 5.
		12. Bring up the site
		13. Wait for testbed to be back to healthy state and k8s node to be in running state.
		14. Create standalone pods for rwx pvcs created after site failure.
		15. Verify all workload pods to reach running ready state.
		16. Perform scaleup operation on all deployment set. Increase the replica count to 7.
		17. Verify scaling operation went smooth for deploymentset.
		18. Perform scaleup operation on statefulset. Increase the replica count to 7.
		19. Verify scaling operation went smooth for statefulset.
		20. Perform cleanup by deleting pods, pvcs and sc.
	*/

	ginkgo.It("Bring down full site Vc2", ginkgo.Label(p1, block, vanilla, multiVc, newTest,
		disruptive), func() {

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		var service *v1.Service
		var depl []*appsv1.Deployment
		var podList []*v1.Pod
		var statefulset *appsv1.StatefulSet
		var powerOffHostsList []string

		createPvcItr := 5
		replica := 2
		createDepItr := 1

		// storage policy of vc1
		storagePolicyInVc1Vc2 := GetAndExpectStringEnvVar(envStoragePolicyNameInVC1VC2)
		scParameters[scParamStoragePolicyName] = storagePolicyInVc1Vc2

		// here considering VC-1, VC-2 so start index is set to 0 and endIndex will be set to 2
		topValStartIndex = 1
		topValEndIndex = 3

		ginkgo.By("Set allowed topology to vc1 and vc2")
		allowedTopologyForSC := setSpecificAllowedTopology(allowedTopologies, topkeyStartIndex, topValStartIndex,
			topValEndIndex)

		ginkgo.By(fmt.Sprintf("Creating Storage Class with access mode %q and fstype %q ", accessmode, nfs4FSType))
		storageclass, pvclaims, err := createStorageClassWithMultiplePVCs(client, namespace, labelsMap,
			scParameters, diskSize, allowedTopologyForSC, bindingModeImm, false, accessmode, "", nil, createPvcItr, false, false)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		defer func() {
			err := client.StorageV1().StorageClasses().Delete(ctx, storageclass.Name, *metav1.NewDeleteOptions(0))
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
				err = multiVCe2eVSphere.waitForCNSVolumeToBeDeletedInMultiVC(pv.Spec.CSI.VolumeHandle)
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
			}
		}()

		ginkgo.By("Verify PVC Bound state and CNS side verification")
		_, err = checkVolumeStateAndPerformCnsVerification(ctx, client, pvclaims, "", "")
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Create Deployments")
		for i := 0; i < len(pvclaims); i++ {
			deploymentList, _, err := createVerifyAndScaleDeploymentPods(ctx, client, namespace, int32(replica),
				false, labelsMap, pvclaims[i], nil, execRWXCommandPod, nginxImage, false, nil, createDepItr)
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

		ginkgo.By("Create StatefulSet with replica count 3 with RWX PVC access mode")
		stsReplicas := 3
		service, statefulset, err = createStafeulSetAndVerifyPVAndPodNodeAffinty(ctx, client, namespace, true,
			int32(stsReplicas), false, allowedTopologies, false, false, false, accessmode, storageclass, false, "")
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Bring down full site vc-2")
		// read testbedinfo json for VC2
		clientIndex := 1
		readVcEsxIpsViaTestbedInfoJson(GetAndExpectStringEnvVar(envTestbedInfoJsonPathVC2))

		// fetch cluster name
		clusterComputeResource, _, err = getClusterNameForMultiVC(ctx, &multiVCe2eVSphere, clientIndex)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		elements := strings.Split(clusterComputeResource[0].InventoryPath, "/")
		clusterName := elements[len(elements)-1]

		// get list of hosts in a cluster
		hostsInCluster := getHostsByClusterName(ctx, clusterComputeResource, clusterName)

		// Bring down esxi hosts and append the hosts list which needs to be powered on later
		powerOffHostsList = powerOffEsxiHostsInMultiVcCluster(ctx, &multiVCe2eVSphere,
			len(hostsInCluster), hostsInCluster)
		defer func() {
			ginkgo.By("Bring up  ESXi host which were powered off in VC2 in a multi setup")
			for i := 0; i < len(powerOffHostsList); i++ {
				powerOnEsxiHostByCluster(powerOffHostsList[i])
			}

			ginkgo.By("Wait for k8s cluster to be healthy")
			wait4AllK8sNodesToBeUp(ctx, client, nodeList)
			err = waitForAllNodes2BeReady(ctx, client, pollTimeout*4)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())

		}()

		ginkgo.By("Create new 5 rwx pvcs when vc-2 and vc-3 sites are down")
		createPvcItr = 5
		_, pvclaimsNew, err := createStorageClassWithMultiplePVCs(client, namespace, labelsMap, scParameters,
			diskSize, nil, bindingModeImm, false, accessmode, "", storageclass, createPvcItr, true, false)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		defer func() {
			for i := 0; i < len(pvclaimsNew); i++ {
				pv := getPvFromClaim(client, pvclaimsNew[i].Namespace, pvclaimsNew[i].Name)
				err = fpv.DeletePersistentVolumeClaim(ctx, client, pvclaimsNew[i].Name, namespace)
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
				err = multiVCe2eVSphere.waitForCNSVolumeToBeDeletedInMultiVC(pv.Spec.CSI.VolumeHandle)
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
			}
		}()

		ginkgo.By("Scale up dep1,dep2 and dep3 to replica count 5")
		replica = 5
		for i := 0; i < 2; i++ {
			_, _, _ = createVerifyAndScaleDeploymentPods(ctx, client, namespace, int32(replica),
				true, labelsMap, pvclaims[i], nil, execRWXCommandPod, nginxImage, true, depl[i], 0)
		}

		ginkgo.By("Scale down dep4,dep5 to replica count 1")
		replica = 1
		for i := 3; i < 5; i++ {
			_, _, _ = createVerifyAndScaleDeploymentPods(ctx, client, namespace, int32(replica),
				true, labelsMap, pvclaims[i], nil, execRWXCommandPod, nginxImage, true, depl[i], 0)
		}

		ginkgo.By("Perform scaleup operation on statefulset. Increase the replica count to 5")
		scaleupReplicaCount := 5
		_ = performScalingOnStatefulSetAndVerifyPvNodeAffinity(ctx, client, int32(scaleupReplicaCount),
			0, statefulset, false, namespace, allowedTopologies, true, false, false)

		// Bring up
		ginkgo.By("Bring up  ESXi host which were powered off in VC1 in a multi setup")
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
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			err = fpod.WaitForPodNotPending(ctx, client, namespace, pod.Name)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}

		ginkgo.By("Create standalone Pods with different read/write permissions and attach it to a newly created rwx pvcs")
		noPodsToDeploy := 3
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

		ginkgo.By("Scale up all deployment set to replica count 7")
		replica = 7
		for i := 0; i < len(depl); i++ {
			_, _, err = createVerifyAndScaleDeploymentPods(ctx, client, namespace, int32(replica),
				true, labelsMap, pvclaims[i], nil, execRWXCommandPod, nginxImage, true, depl[i], 0)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}

		ginkgo.By("Perform scaleup operation on statefulset. Increase the replica count to 7")
		scaleupReplicaCount = 7
		err = performScalingOnStatefulSetAndVerifyPvNodeAffinity(ctx, client, int32(scaleupReplicaCount),
			0, statefulset, false, namespace, allowedTopologies, true, false, false)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
	})

	/*
	   VC-2 and VC-3 sites are partially down
	   SC → all allowed topology set, → zone-1,zone-2 and zone-3, → Immediate Binding mode

	   Steps:
	   1. Create a Storage Class with allowed topology set to all 3 VC's and pass storage policy of vc1
	   2. Create few dynamic pvcs (around 5) using rwx access mode.
	   3. Wait for PVcs to reach Bound state.
	   4. Create deployment pods with replica count 2 using pvc created above.
	   5. Wait for Pods to reach running ready state.
	   6. Create statefulset with replica count 3.
	   7. Bring down partial sites of vc-2 and vc-3.
	   8. Create new rwx pvcs when sites are down.
	   9. Perform scaleup operation on 1-3 deployment sets. Increase the replica count to 5.
	   10. Perform scaledown operation on 4-5 deployment set. Decrease the replica count to 1.
	   11. Perform scaleup operation on statefulset. Increase the replica count to 5.
	   12. Bring up both the sites.
	   13. Wait for testbed to be back to healthy state and k8s node to be in running state.
	   14. Create standalone pods for rwx pvcs created after site failure.
	   15. Verify all workload pods to reach running ready state.
	   16. Perform scaleup operation on all deployment set. Increase the replica count to 7.
	   17. Verify scaling operation went smooth for deploymentset.
	   18. Perform scaleup operation on statefulset. Increase the replica count to 7.
	   19. Verify scaling operation went smooth for statefulset.
	   20. Perform cleanup by deleting pods, pvcs and sc.
	*/

	ginkgo.It("Partial vc-2 and vc-3 sites are down", ginkgo.Label(p1, block, vanilla, multiVc, newTest,
		disruptive), func() {

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		var service *v1.Service
		var depl []*appsv1.Deployment
		var podList []*v1.Pod
		var statefulset *appsv1.StatefulSet
		var powerOffHostsList []string

		createPvcItr := 5
		replica := 2
		createDepItr := 1

		// storage policy of vc1
		storagePolicyInVc1Vc2 := GetAndExpectStringEnvVar(envStoragePolicyNameInVC1VC2)
		scParameters[scParamStoragePolicyName] = storagePolicyInVc1Vc2

		// here considering VC-1, VC-2 so start index is set to 0 and endIndex will be set to 2
		topValStartIndex = 0
		topValEndIndex = 2

		ginkgo.By("Set allowed topology to all VCs")
		allowedTopologyForSC := setSpecificAllowedTopology(allowedTopologies, topkeyStartIndex, topValStartIndex,
			topValEndIndex)

		ginkgo.By(fmt.Sprintf("Creating Storage Class with access mode %q and fstype %q with "+
			"no allowed topology specified", accessmode, nfs4FSType))
		storageclass, pvclaims, err := createStorageClassWithMultiplePVCs(client, namespace, labelsMap,
			scParameters, diskSize, allowedTopologyForSC, bindingModeImm, false, accessmode, "", nil, createPvcItr, false, false)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		defer func() {
			err := client.StorageV1().StorageClasses().Delete(ctx, storageclass.Name, *metav1.NewDeleteOptions(0))
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
				err = multiVCe2eVSphere.waitForCNSVolumeToBeDeletedInMultiVC(pv.Spec.CSI.VolumeHandle)
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
			}
		}()

		ginkgo.By("Verify PVC Bound state and CNS side verification")
		_, err = checkVolumeStateAndPerformCnsVerification(ctx, client, pvclaims, "", "")
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Create Deployments")
		for i := 0; i < len(pvclaims); i++ {
			deploymentList, _, err := createVerifyAndScaleDeploymentPods(ctx, client, namespace, int32(replica),
				false, labelsMap, pvclaims[i], nil, execRWXCommandPod, nginxImage, false, nil, createDepItr)
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

		ginkgo.By("Create StatefulSet with replica count 3 with RWX PVC access mode")
		stsReplicas := 3
		service, statefulset, err = createStafeulSetAndVerifyPVAndPodNodeAffinty(ctx, client, namespace, true,
			int32(stsReplicas), false, allowedTopologies, false, false, false, accessmode, storageclass, false, "")
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Bring down full site vc-1")
		// read testbedinfo json for VC1
		clientIndex := 0
		readVcEsxIpsViaTestbedInfoJson(GetAndExpectStringEnvVar(envTestbedInfoJsonPathVC1))

		// fetch cluster name
		clusterComputeResource, _, err = getClusterNameForMultiVC(ctx, &multiVCe2eVSphere, clientIndex)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		elements := strings.Split(clusterComputeResource[0].InventoryPath, "/")
		clusterName := elements[len(elements)-1]

		// get list of hosts in a cluster
		hostsInCluster := getHostsByClusterName(ctx, clusterComputeResource, clusterName)

		// Bring down esxi hosts and append the hosts list which needs to be powered on later
		powerOffHostsList = powerOffEsxiHostsInMultiVcCluster(ctx, &multiVCe2eVSphere,
			len(hostsInCluster), hostsInCluster)
		defer func() {
			ginkgo.By("Bring up  ESXi host which were powered off in VC1 in a multi setup")
			for i := 0; i < len(powerOffHostsList); i++ {
				powerOnEsxiHostByCluster(powerOffHostsList[i])
			}

			ginkgo.By("Wait for k8s cluster to be healthy")
			wait4AllK8sNodesToBeUp(ctx, client, nodeList)
			err = waitForAllNodes2BeReady(ctx, client, pollTimeout*4)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())

		}()

		ginkgo.By("Create new 5 rwx pvcs when vc-2 and vc-3 sites are down")
		createPvcItr = 5
		_, pvclaimsNew, err := createStorageClassWithMultiplePVCs(client, namespace, labelsMap, scParameters,
			diskSize, nil, bindingModeImm, false, accessmode, "", storageclass, createPvcItr, true, false)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		defer func() {
			for i := 0; i < len(pvclaimsNew); i++ {
				pv := getPvFromClaim(client, pvclaimsNew[i].Namespace, pvclaimsNew[i].Name)
				err = fpv.DeletePersistentVolumeClaim(ctx, client, pvclaimsNew[i].Name, namespace)
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
				err = multiVCe2eVSphere.waitForCNSVolumeToBeDeletedInMultiVC(pv.Spec.CSI.VolumeHandle)
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
			}
		}()

		ginkgo.By("Scale up dep1,dep2 and dep3 to replica count 5")
		replica = 5
		for i := 0; i < 2; i++ {
			_, _, _ = createVerifyAndScaleDeploymentPods(ctx, client, namespace, int32(replica),
				true, labelsMap, pvclaims[i], nil, execRWXCommandPod, nginxImage, true, depl[i], 0)
		}

		ginkgo.By("Scale down dep4,dep5 to replica count 1")
		replica = 1
		for i := 3; i < 5; i++ {
			_, _, _ = createVerifyAndScaleDeploymentPods(ctx, client, namespace, int32(replica),
				true, labelsMap, pvclaims[i], nil, execRWXCommandPod, nginxImage, true, depl[i], 0)
		}

		ginkgo.By("Perform scaleup operation on statefulset. Increase the replica count to 5")
		scaleupReplicaCount := 5
		_ = performScalingOnStatefulSetAndVerifyPvNodeAffinity(ctx, client, int32(scaleupReplicaCount),
			0, statefulset, false, namespace, allowedTopologies, true, false, false)

		// Bring up
		ginkgo.By("Bring up  ESXi host which were powered off in VC1 in a multi setup")
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
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			err = fpod.WaitForPodNotPending(ctx, client, namespace, pod.Name)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}

		ginkgo.By("Create standalone Pods with different read/write permissions and attach it to a newly created rwx pvcs")
		noPodsToDeploy := 3
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

		ginkgo.By("Scale up all deployment set to replica count 7")
		replica = 7
		for i := 0; i < len(depl); i++ {
			_, _, err = createVerifyAndScaleDeploymentPods(ctx, client, namespace, int32(replica),
				true, labelsMap, pvclaims[i], nil, execRWXCommandPod, nginxImage, true, depl[i], 0)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}

		ginkgo.By("Perform scaleup operation on statefulset. Increase the replica count to 7")
		scaleupReplicaCount = 7
		err = performScalingOnStatefulSetAndVerifyPvNodeAffinity(ctx, client, int32(scaleupReplicaCount),
			0, statefulset, false, namespace, allowedTopologies, true, false, false)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
	})

	/*
		VC1 → partially down , VC2 → fully down, VC3 → up and running
		SC → all allowed topology set, → zone-1,zone-2 and zone-3, → Immediate Binding mode

		Workload Pod creation in scale + Multiple site down (partial/full) + kill CSI provisioner and
		CSi-attacher + dynamic Pod/PVC creation + vSAN-Health service down

		Steps:
		1. Identify the CSI-Controller-Pod where CSI Provisioner, CSI Attacher and vsphere-Syncer are the leader
		2. Create Storage class with allowed topology set to all levels of AZ i.e. on VC-1,VC-2 and VC-3.
		3. Create 5 dynamic PVCs with "RWX" acces mode. While volume creation is in progress,
		kill CSI-Provisioner identified in step #1
		4. Create 5 deployment Pods with 3 replicas each
		5. While pod creation is in progress, kill CSI-attacher, identified in step #1
		6. Wait for PVCs to reach Bound state and Pods to reach running state.
		7. Volume provisioning and pod placement should happen on any of the AZ.
		8. Now bring down few hosts in VC-1.
		9. Verify k8s cluster health state.
		10. Try to perform scaleup operation on deployment (dep1 and dep2). Incrase the replica count from 3 to 7
		11. Now bring down all esxi hosts of VC-2.
		12. Try to perform scaleup operation on dep1 . Incrase the replica count from 7 to 10.
		13. Create few dynamic PVC's with labels added for PV and PVC with "RWX" access mode
		14. Stop vSAN-health service.
		15. Update labels for PV and PVC.
		16. Start vSAN-health service.
		17. Create standalone Pods using the PVC created in step #13.
		18. Create StatefulSet with 3 replica
		19. Bring back all the ESXI hosts in vc-1 and vc-2
		20. Wait for system to be back to normal.
		21. Verify CSI Pods status and K8s nodes status.
		22. Perform scale-up/scale-down operation on statefulset
		23. Verify scaling operation on statefulset went successful
		24. Perform scaleup/scaledown operation on different deployment sets.
		25. Verify scaling operation went successful.
		26. Perform cleanup by deleting StatefulSets, Pods, PVCs and SC.
	*/

	ginkgo.It("Operation storm with multiple workload creation "+
		"in multivc env", ginkgo.Label(p1, block, vanilla, multiVc, newTest, disruptive), func() {

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		var fullSyncWaitTime int
		var service *v1.Service
		var depl []*appsv1.Deployment
		var powerOffHostsLists []string
		var podList []*v1.Pod
		var statefulset *appsv1.StatefulSet

		if os.Getenv(envFullSyncWaitTime) != "" {
			fullSyncWaitTime, err := strconv.Atoi(os.Getenv(envFullSyncWaitTime))
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			if fullSyncWaitTime <= 0 || fullSyncWaitTime > defaultFullSyncWaitTime {
				framework.Failf("The FullSync Wait time %v is not set correctly", fullSyncWaitTime)
			}
		} else {
			fullSyncWaitTime = defaultFullSyncWaitTime
		}

		createPvcItr := 5
		replica := 3
		createDepItr := 1

		ginkgo.By("Get current leader where CSI-Provisioner, CSI-Attacher and " +
			"Vsphere-Syncer is running and find the master node IP where these containers are running")
		csiProvisionerLeader, csiProvisionerControlIp, err := getK8sMasterNodeIPWhereContainerLeaderIsRunning(ctx,
			client, sshClientConfig, provisionerContainerName)
		framework.Logf("CSI-Provisioner is running on Leader Pod %s "+
			"which is running on master node %s", csiProvisionerLeader, csiProvisionerControlIp)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		csiAttacherLeaderleader, csiAttacherControlIp, err := getK8sMasterNodeIPWhereContainerLeaderIsRunning(ctx,
			client, sshClientConfig, attacherContainerName)
		framework.Logf("CSI-Attacher is running on Leader Pod %s "+
			"which is running on master node %s", csiAttacherLeaderleader, csiAttacherControlIp)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		vsphereSyncerLeader, vsphereSyncerControlIp, err := getK8sMasterNodeIPWhereContainerLeaderIsRunning(ctx,
			client, sshClientConfig, syncerContainerName)
		framework.Logf("Vsphere-Syncer is running on Leader Pod %s "+
			"which is running on master node %s", vsphereSyncerLeader, vsphereSyncerControlIp)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		// here considering VC-1, VC-2 and VC-3 so start index is set to 0 and endIndex will be set to 3
		topValStartIndex = 0
		topValEndIndex = 3

		ginkgo.By("Set allowed topology to all VCs")
		allowedTopologyForSC := setSpecificAllowedTopology(allowedTopologies, topkeyStartIndex, topValStartIndex,
			topValEndIndex)

		ginkgo.By(fmt.Sprintf("Creating Storage Class with access mode %q and fstype %q with "+
			"no allowed topology specified", accessmode, nfs4FSType))
		storageclass, pvclaims, err := createStorageClassWithMultiplePVCs(client, namespace, labelsMap,
			scParameters, diskSize, allowedTopologyForSC, bindingModeImm, false, accessmode, "", nil, createPvcItr, false, false)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		defer func() {
			err := client.StorageV1().StorageClasses().Delete(ctx, storageclass.Name, *metav1.NewDeleteOptions(0))
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
				err = multiVCe2eVSphere.waitForCNSVolumeToBeDeletedInMultiVC(pv.Spec.CSI.VolumeHandle)
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
			}
		}()

		ginkgo.By("Kill CSI-Provisioner container while pvc creation is in progress")
		err = execDockerPauseNKillOnContainer(sshClientConfig, csiProvisionerControlIp, provisionerContainerName,
			k8sVersion)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Verify PVC Bound state and CNS side verification")
		_, err = checkVolumeStateAndPerformCnsVerification(ctx, client, pvclaims, "", "")
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Create Deployments")
		for i := 0; i < len(pvclaims); i++ {
			deploymentList, _, err := createVerifyAndScaleDeploymentPods(ctx, client, namespace, int32(replica),
				false, labelsMap, pvclaims[i], nil, execRWXCommandPod, nginxImage, false, nil, createDepItr)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			depl = append(depl, deploymentList...)

			if i == 2 {
				ginkgo.By("Kill CSI-Attacher container while pod creation is in progress")
				err = execDockerPauseNKillOnContainer(sshClientConfig, csiAttacherControlIp, attacherContainerName,
					k8sVersion)
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

		ginkgo.By("Partial sitedown on vc-1")
		// read testbed info json for VC1
		readVcEsxIpsViaTestbedInfoJson(GetAndExpectStringEnvVar(envTestbedInfoJsonPathVC2))

		// here clientIndex 0 refers VC-1
		clientIndex := 0
		noOfHostToBringDown := 1
		clusterComputeResource, _, err = getClusterNameForMultiVC(ctx, &multiVCe2eVSphere, clientIndex)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		elements := strings.Split(clusterComputeResource[0].InventoryPath, "/")
		clusterName := elements[len(elements)-1]

		// read hosts in a cluster in Vc1
		hostsInCluster := getHostsByClusterName(ctx, clusterComputeResource, clusterName)

		// power off esxi hosts
		powerOffHostsListInVc1 := powerOffEsxiHostsInMultiVcCluster(ctx, &multiVCe2eVSphere,
			noOfHostToBringDown, hostsInCluster)
		powerOffHostsLists = append(powerOffHostsLists, powerOffHostsListInVc1...)
		defer func() {
			ginkgo.By("Bring up  ESXi host which were powered off in vc-1")
			for i := 0; i < len(powerOffHostsListInVc1); i++ {
				powerOnEsxiHostByCluster(powerOffHostsListInVc1[i])
			}

			ginkgo.By("Wait for k8s cluster to be healthy")
			wait4AllK8sNodesToBeUp(ctx, client, nodeList)
			err = waitForAllNodes2BeReady(ctx, client, pollTimeout*4)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}()

		ginkgo.By("Scale up depl1 and depl2 to 7 replica")
		replica = 7
		for i := 0; i < 2; i++ {
			_, _, _ = createVerifyAndScaleDeploymentPods(ctx, client, namespace, int32(replica),
				true, labelsMap, pvclaims[i], nil, execRWXCommandPod, nginxImage, true, depl[i], 0)
		}

		ginkgo.By("Bring down all ESXI hosts in VC2 in a multi-setup")
		// read testbedinfo json for VC2
		readVcEsxIpsViaTestbedInfoJson(GetAndExpectStringEnvVar(envTestbedInfoJsonPathVC2))

		// here clientIndex 1 refers to vc-2 setup
		clientIndex = 1
		clusterComputeResource, _, err = getClusterNameForMultiVC(ctx, &multiVCe2eVSphere, clientIndex)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		elements = strings.Split(clusterComputeResource[0].InventoryPath, "/")
		clusterName = elements[len(elements)-1]

		// get list of hosts in a cluster
		hostsInCluster = getHostsByClusterName(ctx, clusterComputeResource, clusterName)

		// Bring down esxi hosts and append the hosts list which needs to be powered on later
		powerOffHostsListInVc2 := powerOffEsxiHostsInMultiVcCluster(ctx, &multiVCe2eVSphere,
			noOfHostToBringDown, hostsInCluster)
		powerOffHostsLists = append(powerOffHostsLists, powerOffHostsListInVc2...)
		defer func() {
			ginkgo.By("Bring up  ESXi host which were powered off in VC2 in a multi setup")
			for i := 0; i < len(powerOffHostsListInVc2); i++ {
				powerOnEsxiHostByCluster(powerOffHostsListInVc2[i])
			}

			ginkgo.By("Wait for k8s cluster to be healthy")
			wait4AllK8sNodesToBeUp(ctx, client, nodeList)
			err = waitForAllNodes2BeReady(ctx, client, pollTimeout*4)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())

		}()

		ginkgo.By("Perform scaleup operation on dep1. Increase the replica count from 7 to 10")
		replica = 10
		_, _, _ = createVerifyAndScaleDeploymentPods(ctx, client, namespace, int32(replica),
			true, labelsMap, pvclaims[0], nil, execRWXCommandPod, nginxImage, true, depl[0], 0)

		ginkgo.By("Create new 5 rwx pvcs when vc-1 partially down and vc-2 fully down")
		createPvcItr = 5
		_, pvclaimsNew, err := createStorageClassWithMultiplePVCs(client, namespace, labelsMap, scParameters,
			diskSize, nil, bindingModeImm, false, accessmode, "", storageclass, createPvcItr, true, false)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		defer func() {
			for i := 0; i < len(pvclaimsNew); i++ {
				pv := getPvFromClaim(client, pvclaimsNew[i].Namespace, pvclaimsNew[i].Name)
				err = fpv.DeletePersistentVolumeClaim(ctx, client, pvclaimsNew[i].Name, namespace)
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
				err = multiVCe2eVSphere.waitForCNSVolumeToBeDeletedInMultiVC(pv.Spec.CSI.VolumeHandle)
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
			}
		}()

		ginkgo.By(fmt.Sprintln("Stopping vsan-health on the vCenter host"))
		vcAddress := e2eVSphere.Config.Global.VCenterHostname + ":" + sshdPort
		isVsanHealthServiceStopped = true
		err = invokeVCenterServiceControl(ctx, stopOperation, vsanhealthServiceName, vcAddress)
		defer func() {
			if isVsanHealthServiceStopped {
				startVCServiceWait4VPs(ctx, vcAddress, vsanhealthServiceName, &isVsanHealthServiceStopped)
			}
		}()
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		ginkgo.By(fmt.Sprintf("Sleeping for %v seconds to allow vsan-health to completely shutdown",
			vsanHealthServiceWaitTime))
		time.Sleep(time.Duration(vsanHealthServiceWaitTime) * time.Second)

		labelKey := "volIdNew"
		labelValue := "pvcVolumeNew"
		labels := make(map[string]string)
		labels[labelKey] = labelValue

		ginkgo.By("Updating labels for PVC and PV")
		for i := 0; i < len(pvclaims); i++ {
			pv := getPvFromClaim(client, pvclaims[i].Namespace, pvclaims[i].Name)
			framework.Logf("Updating labels %+v for pvc %s in namespace %s", labels, pvclaims[i].Name,
				pvclaims[i].Namespace)
			pvc, err := client.CoreV1().PersistentVolumeClaims(namespace).Get(ctx, pvclaims[i].Name, metav1.GetOptions{})
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			pvc.Labels = labels
			_, err = client.CoreV1().PersistentVolumeClaims(namespace).Update(ctx, pvc, metav1.UpdateOptions{})
			gomega.Expect(err).NotTo(gomega.HaveOccurred())

			framework.Logf("Updating labels %+v for pv %s", labels, pv.Name)
			pv.Labels = labels
			_, err = client.CoreV1().PersistentVolumes().Update(ctx, pv, metav1.UpdateOptions{})
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}

		ginkgo.By(fmt.Sprintln("Starting vsan-health on the vCenter host"))
		startVCServiceWait4VPs(ctx, vcAddress, vsanhealthServiceName, &isVsanHealthServiceStopped)

		ginkgo.By(fmt.Sprintf("Sleeping for %v seconds to allow full sync finish", fullSyncWaitTime))
		time.Sleep(time.Duration(fullSyncWaitTime) * time.Second)

		ginkgo.By("Create standalone Pods with different read/write permissions and attach it to a newly created rwx pvcs")
		noPodsToDeploy := 3
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

		ginkgo.By("Create StatefulSet with replica count 3 with RWX PVC access mode")
		stsReplicas := 3
		service, statefulset, _ = createStafeulSetAndVerifyPVAndPodNodeAffinty(ctx, client, namespace, true,
			int32(stsReplicas), false, allowedTopologies, false, false, false, accessmode, storageclass, false, "")

		ginkgo.By("Bring up  ESXi host which were powered off in VC2 in a multi setup")
		for i := 0; i < len(powerOffHostsLists); i++ {
			powerOnEsxiHostByCluster(powerOffHostsLists[i])
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
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			err = fpod.WaitForPodNotPending(ctx, client, namespace, pod.Name)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}

		ginkgo.By("Perform scaleup operation on statefulset. Increase the replica count to 7")
		scaleupReplicaCount := 7
		err = performScalingOnStatefulSetAndVerifyPvNodeAffinity(ctx, client, int32(scaleupReplicaCount),
			0, statefulset, false, namespace, allowedTopologies, true, false, false)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Perform scaledown operation on statefulset. Decrease the replica count to 1")
		scaleDownReplicaCount := 1
		err = performScalingOnStatefulSetAndVerifyPvNodeAffinity(ctx, client, 0,
			int32(scaleDownReplicaCount), statefulset, false, namespace, allowedTopologies, false, true, false)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Scale down dep1,dep2,dep3 to replica count of 1 and verify cns volume metadata")
		replica = 1
		for i := 0; i < 3; i++ {
			_, pods, err = createVerifyAndScaleDeploymentPods(ctx, client, namespace, int32(replica),
				true, labelsMap, pvclaims[i], nil, execRWXCommandPod, nginxImage, true, depl[i], 0)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())

			pv := getPvFromClaim(client, pvclaims[i].Namespace, pvclaims[i].Name)

			ginkgo.By("Verify volume metadata for deployment pod, pvc and pv")
			err = waitAndVerifyCnsVolumeMetadata(ctx, pv.Spec.CSI.VolumeHandle, pvclaims[i], pv, &pods.Items[i])
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}

		ginkgo.By("Scale down dep4, dep5 to replica count 5")
		replica = 5
		for i := 3; i < 5; i++ {
			_, pods, err = createVerifyAndScaleDeploymentPods(ctx, client, namespace, int32(replica),
				true, labelsMap, pvclaims[i], nil, execRWXCommandPod, nginxImage, true, depl[i], 0)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())

			pv := getPvFromClaim(client, pvclaims[i].Namespace, pvclaims[i].Name)

			ginkgo.By("Verify volume metadata for deployment pod, pvc and pv")
			err = waitAndVerifyCnsVolumeMetadata(ctx, pv.Spec.CSI.VolumeHandle, pvclaims[i], pv, &pods.Items[i])
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}
	})
})
