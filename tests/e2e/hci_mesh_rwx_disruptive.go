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
	"os"
	"strconv"
	"strings"
	"time"

	ginkgo "github.com/onsi/ginkgo/v2"
	"github.com/onsi/gomega"
	"github.com/vmware/govmomi/object"
	vimtypes "github.com/vmware/govmomi/vim25/types"
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

var _ = ginkgo.Describe("[rwx-hci-singlevc-disruptive] RWX-Topology-HciMesh-SingleVc-Disruptive", func() {
	f := framework.NewDefaultFramework("rwx-hci-singlevc-disruptive")
	f.NamespacePodSecurityEnforceLevel = admissionapi.LevelPrivileged
	var (
		client                     clientset.Interface
		namespace                  string
		bindingModeImm             storagev1.VolumeBindingMode
		bindingModeWffc            storagev1.VolumeBindingMode
		scParameters               map[string]string
		accessmode                 v1.PersistentVolumeAccessMode
		labelsMap                  map[string]string
		allowedTopologies          []v1.TopologySelectorLabelRequirement
		err                        error
		topologyClusterList        []string
		nimbusGeneratedK8sVmPwd    string
		sshClientConfig            *ssh.ClientConfig
		nodeList                   *v1.NodeList
		clusterComputeResource     []*object.ClusterComputeResource
		k8sVersion                 string
		isVsanHealthServiceStopped bool
		vmknic4VsanDown            bool
		nicMgr                     *object.HostVirtualNicManager
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
		bindingModeWffc = storagev1.VolumeBindingWaitForFirstConsumer
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

		topologyMap := GetAndExpectStringEnvVar(envTopologyMap)
		allowedTopologies = createAllowedTopolgies(topologyMap)

		//setting map values
		labelsMap["app"] = "test"
		scParameters[scParamFsType] = nfs4FSType

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

		// fetching k8s version
		v, err := client.Discovery().ServerVersion()
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		k8sVersion = v.Major + "." + v.Minor

		if isVsanHealthServiceStopped {
			vCenterHostname := strings.Split(e2eVSphere.Config.Global.VCenterHostname, ",")
			vcAddress := vCenterHostname[0] + ":" + sshdPort
			framework.Logf("Bringing vsanhealth up before terminating the test")
			startVCServiceWait4VPs(ctx, vcAddress, vsanhealthServiceName, &isVsanHealthServiceStopped)
		}
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

		// perfrom cleanup of old stale entries of standalone pods if left in the setup
		pods, err := client.CoreV1().Pods(namespace).List(ctx, metav1.ListOptions{})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		if len(pods.Items) != 0 {
			for _, pod := range pods.Items {
				gomega.Expect(client.CoreV1().Pods(namespace).Delete(ctx, pod.Name,
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

		// Initialize an empty slice to hold all hosts
		var hostsList []*object.HostSystem

		// Fetch hosts from each cluster
		hostsInCluster1 := getHostsByClusterName(ctx, clusterComputeResource, topologyClusterList[0])
		hostsInCluster2 := getHostsByClusterName(ctx, clusterComputeResource, topologyClusterList[1])
		hostsInCluster3 := getHostsByClusterName(ctx, clusterComputeResource, topologyClusterList[2])
		hostsInCluster4 := getHostsByClusterName(ctx, clusterComputeResource, topologyClusterList[3])

		// Append hosts from each cluster to the hostsList
		hostsList = append(hostsList, hostsInCluster1...)
		hostsList = append(hostsList, hostsInCluster2...)
		hostsList = append(hostsList, hostsInCluster3...)
		hostsList = append(hostsList, hostsInCluster4...)

		// Bring up ESXi hosts which were powered off
		ginkgo.By("Bring up ESXi host which were powered off in Az3")
		for _, host := range hostsList {
			hostName := host.String()
			powerOnEsxiHostByCluster(hostName)
		}

		if vmknic4VsanDown {
			ginkgo.By("enable vsan network on the host's vmknic in cluster4")
			err := nicMgr.SelectVnic(ctx, "vsan", GetAndExpectStringEnvVar(envVmknic4Vsan))
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			vmknic4VsanDown = false
		}
	})

	/*
		Put some ESX hosts from remote cluster into maintenance mode with EvacuateAlldata

		Immediate mode, all allowed topologies, remote datastore url

		Steps:
		1. Create SC with Immedaite mode with all allowed topology details.
		2. Create few PVCs with RWX access mode.
		3. Create deployment sets with different replica counts.
		4. Volume provisioning should happen on the remote datastore url.
		5. Create statefulset with replica count 5.
		6. Pods can get created on any Az.
		7. Isolate a host that has a k8s worker with some pods attached to it.
		8. Wait for 5-10 mins, verify that the k8s worker is restarted and brought up on another host.
		9. Verify that the replicas on the k8s worker come up successfully
		10. Scale up deployment to 3 replica and scale down statefulset1 to 3 replicas
		11. Re-integrate the host
		12. Verify k8s cluster health state. All k8s nodes should be in a healthy state.
		13. Verify workload pods status. ALl pods should come up to healthy running state.
		14. Create new PVCs and attach it to a standalone pods once hosts are recovered from MM mode.
		15. New PVC should get created on remote datastore and pods can be created on any Az.
		16. Perform cleanup by deleting Pods, PVCs and SC.
	*/

	ginkgo.It("Put some ESX hosts from remote cluster into maintenance "+
		"mode with EvacuateAlldata", ginkgo.Label(p1, file, vanilla,
		level5, level2, newTest, disruptive), func() {

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		var service *v1.Service
		var replica int32
		var depl []*appsv1.Deployment
		var statefulset *appsv1.StatefulSet

		createPvcItr := 5
		createDepItr := 1
		stsReplicas := 7

		// remote datastore url
		remoteDsUrl := GetAndExpectStringEnvVar(envRemoteDatastoreUrl)
		scParameters["datastoreurl"] = remoteDsUrl

		ginkgo.By(fmt.Sprintf("Creating Storage Class with access mode %q and fstype %q ", accessmode, nfs4FSType))
		storageclass, pvclaims, err := createStorageClassWithMultiplePVCs(client, namespace, labelsMap,
			scParameters, diskSize, nil, bindingModeImm, false, accessmode,
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
		_, err = checkVolumeStateAndPerformCnsVerification(ctx, client, pvclaims, "", remoteDsUrl)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Create Deployment")
		for i := 0; i < len(pvclaims); i++ {
			if i == 0 {
				replica = 3
			} else if i == 1 {
				replica = 4
			} else if i == 2 {
				replica = 5
			} else {
				replica = 1
			}
			deploymentList, _, err := createVerifyAndScaleDeploymentPods(ctx, client, namespace, replica, false, labelsMap,
				pvclaims[i], nil, execRWXCommandPod, nginxImage, false, nil, createDepItr)
			depl = append(depl, deploymentList...)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}
		defer func() {
			framework.Logf("Delete deployment set")
			for i := 0; i < len(depl); i++ {
				err = client.AppsV1().Deployments(namespace).Delete(ctx, depl[i].Name, *metav1.NewDeleteOptions(0))
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
			}
		}()

		ginkgo.By("Create StatefulSet with replica count 3 with RWX PVC access mode")
		service, statefulset, err = createStafeulSetAndVerifyPVAndPodNodeAffinty(ctx, client, namespace, true,
			int32(stsReplicas), false, allowedTopologies, false, false, false, accessmode, storageclass, false, "")
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Enter host into maintenance mode with evacuateAllData")
		var timeout int32 = 300
		hostsInCluster4 := getHostsByClusterName(ctx, clusterComputeResource, topologyClusterList[3])
		for i := 0; i < len(hostsInCluster4)-2; i++ {
			enterHostIntoMM(ctx, hostsInCluster4[i], evacMModeType, timeout, true)
		}
		defer func() {
			for i := 0; i < len(hostsInCluster4)-2; i++ {
				framework.Logf("Exit the hosts from MM before terminating the test")
				exitHostMM(ctx, hostsInCluster4[i], timeout)
			}
		}()

		ginkgo.By("Scale up all deployment sets to replica count 5")
		replica = 5
		for i := 0; i < len(depl); i++ {
			_, _, err = createVerifyAndScaleDeploymentPods(ctx, client, namespace, replica,
				true, labelsMap, pvclaims[i], nil, execRWXCommandPod, nginxImage, true, depl[i], 0)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}

		ginkgo.By("Perform scaledown operation on statefulset sets. Decrease the replica count to 3")
		scaleDownReplicaCount := 3
		err = performScalingOnStatefulSetAndVerifyPvNodeAffinity(ctx, client, 0,
			int32(scaleDownReplicaCount), statefulset, false, namespace, allowedTopologies, false, true, false)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Exit the hosts from MM")
		for i := 0; i < len(hostsInCluster4)-2; i++ {
			exitHostMM(ctx, hostsInCluster4[i], timeout)
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

		ginkgo.By("Creating new rwx pvcs when multiple sites are down")
		createPvcItr = 4
		_, pvclaimsNew, _ := createStorageClassWithMultiplePVCs(client, namespace, labelsMap, scParameters,
			diskSize, nil, bindingModeImm, false, accessmode, "", storageclass, createPvcItr, true, false)
		defer func() {
			for i := 0; i < len(pvclaimsNew); i++ {
				pv := getPvFromClaim(client, pvclaimsNew[i].Namespace, pvclaimsNew[i].Name)
				err = fpv.DeletePersistentVolumeClaim(ctx, client, pvclaimsNew[i].Name, namespace)
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
				err = e2eVSphere.waitForCNSVolumeToBeDeleted(pv.Spec.CSI.VolumeHandle)
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
			}
		}()

		ginkgo.By("Create standalone Pods and attach it to pvc created above")
		var podListNew []*v1.Pod
		noPodsToDeploy := 3
		for i := 0; i < len(pvclaimsNew); i++ {
			podListNew, _ = createStandalonePodsForRWXVolume(client, ctx, namespace, nil,
				pvclaimsNew[i], false, execRWXCommandPod, noPodsToDeploy)
		}
		defer func() {
			for i := 0; i < len(podListNew); i++ {
				ginkgo.By(fmt.Sprintf("Deleting the pod %s in namespace %s", podListNew[i].Name, namespace))
				err = fpod.DeletePodWithWait(ctx, client, podListNew[i])
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
			}
		}()
	})

	/*
	   Partial bring down remote cluster
	   SC → Immediate Binding Mode, → default values, datastore url of HCI Mesh datastore

	   Steps:
	   1. Create SC with Immediate Binding mode and set SC with default values,
	   taking datastore url of HCI Mesh datastore (remote datastore in this case)
	   2. Create multiple PVCs with RWX access mode.
	   3. Wait for all PVCs to reach Bound state.
	   4. Create deployment Pods for each PVC with replica count 3
	   5. Wait for Pods to reach running state
	   6. Pods should get created on any Az.
	   7. PVC should get created on the remote datastore.
	   8. Bring down all ESXi hosts in cluster2 i.e. bringing down full site i.e. cluster2 (rack-2)
	   9. Scale up deployment1 Pod replica to count 5
	   10. Scale down deployment2 Pod to 1 replica
	   11. Create new multiple PVCs with "RWX"access mode.
	   12. Attach newly created PVcs to standalone Pods
	   13. Bring up the site which was down.
	   14. Wait for testbed to be back to normal state.
	   15. Verify K8s Nodes status and Workload Pod status. All should come back to Running Ready state.
	   16. Perform scaleup operation on deployment sets. Increase the replica count to 7.
	   17. Perform cleanup by deleting Pods, PVCs and SC.
	*/

	ginkgo.It("Partial bring down remote cluster and set remote datastore url in sc", ginkgo.Label(p1, file, vanilla,
		level5, level2, newTest, disruptive), func() {

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		var depl []*appsv1.Deployment

		replica := 3
		createPvcItr := 5
		createDepItr := 1

		// remote datastore url
		remoteDsUrl := GetAndExpectStringEnvVar(envRemoteDatastoreUrl)
		scParameters["datastoreurl"] = remoteDsUrl

		ginkgo.By(fmt.Sprintf("Creating Storage Class with access mode %q and fstype %q ", accessmode, nfs4FSType))
		storageclass, pvclaims, err := createStorageClassWithMultiplePVCs(client, namespace, labelsMap,
			scParameters, diskSize, nil, bindingModeImm, false, accessmode,
			defaultNginxStorageClassName, nil, createPvcItr, false, false)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		defer func() {
			err = client.StorageV1().StorageClasses().Delete(ctx, storageclass.Name, *metav1.NewDeleteOptions(0))
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
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
		_, err = checkVolumeStateAndPerformCnsVerification(ctx, client, pvclaims, "", remoteDsUrl)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Create Deployment")
		for i := 0; i < len(pvclaims); i++ {
			deploymentList, _, err := createVerifyAndScaleDeploymentPods(ctx, client, namespace, int32(replica), false,
				labelsMap, pvclaims[i], nil, execRWXCommandPod, nginxImage, false, nil, createDepItr)
			depl = append(depl, deploymentList...)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}
		defer func() {
			framework.Logf("Delete deployment set")
			for i := 0; i < len(depl); i++ {
				err = client.AppsV1().Deployments(namespace).Delete(ctx, depl[i].Name, *metav1.NewDeleteOptions(0))
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
			}
		}()

		ginkgo.By("Partially bring down remote cluster")
		hostsInCluster := getHostsByClusterName(ctx, clusterComputeResource, topologyClusterList[3])
		powerOffHostsList4 := powerOffEsxiHostByCluster(ctx, &e2eVSphere, topologyClusterList[3],
			len(hostsInCluster)-1)
		defer func() {
			ginkgo.By("Bring up ESXi host which were powered off inremote cluster")
			for i := 0; i < len(powerOffHostsList4); i++ {
				powerOnEsxiHostByCluster(powerOffHostsList4[i])
			}
		}()

		ginkgo.By("Scale up deployment1 to replica count 5")
		replica = 5
		_, _, _ = createVerifyAndScaleDeploymentPods(ctx, client, namespace, int32(replica),
			true, labelsMap, pvclaims[0], nil, execRWXCommandPod, nginxImage, true, depl[0], 0)

		ginkgo.By("Scale down deployment2 to replica count 1")
		replica = 1
		_, _, _ = createVerifyAndScaleDeploymentPods(ctx, client, namespace, int32(replica),
			true, labelsMap, pvclaims[1], nil, execRWXCommandPod, nginxImage, true, depl[1], 0)

		ginkgo.By("Creating new rwx pvcs when multiple sites are down")
		createPvcItr = 4
		_, pvclaimsNew, _ := createStorageClassWithMultiplePVCs(client, namespace, labelsMap, scParameters,
			diskSize, nil, bindingModeImm, false, accessmode, "", storageclass, createPvcItr, true, false)
		defer func() {
			for i := 0; i < len(pvclaimsNew); i++ {
				pv := getPvFromClaim(client, pvclaimsNew[i].Namespace, pvclaimsNew[i].Name)
				err = fpv.DeletePersistentVolumeClaim(ctx, client, pvclaimsNew[i].Name, namespace)
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
				err = e2eVSphere.waitForCNSVolumeToBeDeleted(pv.Spec.CSI.VolumeHandle)
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
			}
		}()

		ginkgo.By("Create standalone Pods and attach it to pvc created above")
		var podListNew []*v1.Pod
		noPodsToDeploy := 3
		for i := 0; i < len(pvclaimsNew); i++ {
			podListNew, _ = createStandalonePodsForRWXVolume(client, ctx, namespace, nil,
				pvclaimsNew[i], false, execRWXCommandPod, noPodsToDeploy)
		}
		defer func() {
			for i := 0; i < len(podListNew); i++ {
				ginkgo.By(fmt.Sprintf("Deleting the pod %s in namespace %s", podListNew[i].Name, namespace))
				err = fpod.DeletePodWithWait(ctx, client, podListNew[i])
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
			}
		}()

		// Bring up
		ginkgo.By("Bring up all ESXi host which were powered off")
		for i := 0; i < len(powerOffHostsList4); i++ {
			powerOnEsxiHostByCluster(powerOffHostsList4[i])
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

		ginkgo.By("Scale up all deployment sets to replica count 10")
		replica = 10
		for i := 0; i < len(depl); i++ {
			_, _, err = createVerifyAndScaleDeploymentPods(ctx, client, namespace, int32(replica),
				true, labelsMap, pvclaims[i], nil, execRWXCommandPod, nginxImage, true, depl[i], 0)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}
	})

	/*
		PSOD all ESXI hosts on remote cluster
		SC → Immediate Binding mode, all allowed topology set

		Steps:
		1. Create SC with Immediate binding mode and allowed topology set to all racks
		2. Create multiple PVCs with "RWX" access mode.
		3. Create deployment set with replica count 3.
		4. Wait for all the Pods and PVCs to reach Bound and running state.
		5. Make sure Pods are running across all AZs
		6. Create statefulset with replica count 3
		7. PSOD all esxi hosts which is on cluster-4
		8. Create new set of rwx pvc when psod is triggered on cluster-2
		9. Create 3 standalone pods and attach it to newly created rwx pvcs
		10. PSOD all esxi hosts which is in cluster-4
		11. Perform scaleup operation on deployment and statefulset. Increase the replica count to 5.
		12. Create new PVCs and attach it standalone Pods.
		13. Verify the ESXI hosts for which we performed PSOD should come back to responding state.
		14. Verify that the k8s cluster is healthy
		15. Once the testbed is normal verify that the all workload Pods are in up and running state.
		16. Perform scale-up/scale-down on the dep-1 and dep-2
		17. Perform cleanup by deleting Pods, PVCs and SC.
	*/

	ginkgo.It("Psod all esxi hosts on hci remote cluster where "+
		"remote datastore is pass in sc", ginkgo.Label(p1, file, vanilla,
		level5, level2, newTest, disruptive), func() {

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		var pvclaimsNew []*v1.PersistentVolumeClaim
		var service *v1.Service
		var depl []*appsv1.Deployment
		var statefulset *appsv1.StatefulSet
		var podList []*v1.Pod

		stsReplicas := 3
		replica := 3
		createPvcItr := 5
		createDepItr := 1

		// remote datastore url
		remoteDsUrl := GetAndExpectStringEnvVar(envRemoteDatastoreUrl)
		scParameters["datastoreurl"] = remoteDsUrl

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

		ginkgo.By("Create Deployments")
		for i := 0; i < len(pvclaims); i++ {
			deploymentList, _, err := createVerifyAndScaleDeploymentPods(ctx, client, namespace, int32(replica), false,
				labelsMap, pvclaims[i], nil, execRWXCommandPod, nginxImage, false, nil, createDepItr)
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

		ginkgo.By("Create StatefulSet with replica count 3 using RWX PVC access mode")
		service, statefulset, err = createStafeulSetAndVerifyPVAndPodNodeAffinty(ctx, client, namespace, true,
			int32(stsReplicas), false, allowedTopologies, false, false, false, accessmode, storageclass, false, "")
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Fetch list of hosts in remote cluster4")
		hostListCluster4 := fetchListofHostsInCluster(ctx, topologyClusterList[3])

		ginkgo.By("PSOD all host in remote cluster4 and when psod is triggered, create new set of rwx pvc")
		for i := 0; i < len(hostListCluster4); i++ {
			err = psodHost(hostListCluster4[i])
			gomega.Expect(err).NotTo(gomega.HaveOccurred())

			if i == 0 {
				ginkgo.By("Create new 3 rwx pvcs")
				createPvcItr = 3
				_, pvclaimsNew, err = createStorageClassWithMultiplePVCs(client, namespace, labelsMap, scParameters,
					diskSize, nil, bindingModeImm, false, accessmode, "", storageclass, createPvcItr, true, false)
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
			}

			if i == 2 {
				ginkgo.By("Verify PVC Bound state and CNS side verification")
				_, err = checkVolumeStateAndPerformCnsVerification(ctx, client, pvclaimsNew, "", "")
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
			}
		}

		ginkgo.By("Create standalone Pods with different read/write permissions and attach it to a newly created rwx pvcs")
		noPodsToDeploy := 3
		for i := 0; i < len(pvclaimsNew); i++ {
			pods, _ := createStandalonePodsForRWXVolume(client, ctx, namespace, nil, pvclaimsNew[i], false, execRWXCommandPod,
				noPodsToDeploy)
			podList = append(podList, pods...)
		}
		defer func() {
			for i := 0; i < len(hostListCluster4); i++ {
				ginkgo.By("checking host status")
				err := waitForHostToBeUp(hostListCluster4[i])
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
			}
		}()
		defer func() {
			for i := 0; i < len(pvclaimsNew); i++ {
				pv := getPvFromClaim(client, pvclaimsNew[i].Namespace, pvclaimsNew[i].Name)
				err = fpv.DeletePersistentVolumeClaim(ctx, client, pvclaimsNew[i].Name, namespace)
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
				err = e2eVSphere.waitForCNSVolumeToBeDeleted(pv.Spec.CSI.VolumeHandle)
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
			}
		}()
		defer func() {
			for i := 0; i < len(podList); i++ {
				ginkgo.By(fmt.Sprintf("Deleting the pod %s in namespace %s", podList[i].Name, namespace))
				err = fpod.DeletePodWithWait(ctx, client, podList[i])
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
			}
		}()

		ginkgo.By("PSOD again all host in remote cluster4 and perform scaleup " +
			"operation on deployment and statefulset")
		for i := 0; i < len(hostListCluster4); i++ {
			err = psodHost(hostListCluster4[i])
			gomega.Expect(err).NotTo(gomega.HaveOccurred())

			if i == 0 {
				ginkgo.By("Scale up all deployment set replica count to 5 when psod is performed on Az3.")
				replica = 5
				for i := 0; i < len(depl); i++ {
					_, _, _ = createVerifyAndScaleDeploymentPods(ctx, client, namespace, int32(replica),
						true, labelsMap, pvclaims[i], nil, execRWXCommandPod, nginxImage, true, depl[i], 0)
				}
			}

			if i == 2 {
				ginkgo.By("Perform scaleup operation on statefulset. Increase the replica count to 5")
				scaleUpReplicaCount := 5
				err = performScalingOnStatefulSetAndVerifyPvNodeAffinity(ctx, client, int32(scaleUpReplicaCount),
					0, statefulset, false, namespace, allowedTopologies, true, false, false)
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
			}
		}

		ginkgo.By("Creating new rwx pvc when psod is trigegerd on Az3 site")
		createPvcItr = 1
		_, pvclaimsNewSet, err := createStorageClassWithMultiplePVCs(client, namespace, labelsMap, scParameters,
			diskSize, allowedTopologies, bindingModeImm, false, accessmode, "", storageclass, createPvcItr, true, false)
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

		ginkgo.By("Create single standalone Pods using pvc created above")
		noPodsToDeploy = 1
		podListNew, err := createStandalonePodsForRWXVolume(client, ctx, namespace, nil, pvclaimsNewSet[0], false,
			execRWXCommandPod, noPodsToDeploy)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		defer func() {
			for i := 0; i < len(podListNew); i++ {
				ginkgo.By(fmt.Sprintf("Deleting the pod %s in namespace %s", podListNew[i].Name, namespace))
				err = fpod.DeletePodWithWait(ctx, client, podListNew[i])
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
			}
		}()

		ginkgo.By("Verify all esxi hosts have recovered from PSOD")
		for i := 0; i < len(hostListCluster4); i++ {
			ginkgo.By("checking host status")
			err := waitForHostToBeUp(hostListCluster4[i])
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
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

		ginkgo.By("Scale up dep1 to replica count 10")
		replica = 10
		_, _, err = createVerifyAndScaleDeploymentPods(ctx, client, namespace, int32(replica),
			true, labelsMap, pvclaims[0], nil, execRWXCommandPod, nginxImage, true, depl[0], 0)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Scale down dep2 to replica count 1")
		replica = 1
		_, _, err = createVerifyAndScaleDeploymentPods(ctx, client, namespace, int32(replica),
			true, labelsMap, pvclaims[1], nil, execRWXCommandPod, nginxImage, true, depl[1], 0)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
	})

	/*
		vsan partition in remote cluster
		Steps:
		1. Create SC with WFFC with all allowed topology details using remote datastore url
		2. Create few PVCs with RWX access mode.
		3. Create deployments each with replica count of 3 using pvcs created above.
		4. Disable vsan network on one of the hosts in cluster 4.
		5. There should not be any visible impact on the replicas and PVs should be accessible
		(but they will become non-compliant, since hftt=1).
		6. Volume provisioning should happen on the specified remote datastore url,
		while pods can come up on any accessibility zone.
		7. Perform deployment scale up.
		8. Write some data to one of the PVs from deployment and read I/O back and verify its integrity.
		9. Enable vsan network on all of the hosts in cluster 4.
		10. Wait for k8s cluster to be in a healthy state.
		11. Wait for workload pods to be in a running state.
		12. Verify the PVs are accessible and compliant now.
		13. Perform deployment scale up and verify scaleup should go fine.
		14. Perform cleanup by deleting Pods, PVCs and SC.
	*/
	ginkgo.It("vsan partition in remote hci cluster", ginkgo.Label(p0, block, vanilla, hci), func() {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		createPvcItr := 5
		createDepItr := 1
		replica := 3

		var depl []*appsv1.Deployment

		// remote datastore url
		remoteDsUrl := GetAndExpectStringEnvVar(envRemoteDatastoreUrl)
		scParameters["datastoreurl"] = remoteDsUrl

		ginkgo.By(fmt.Sprintf("Creating Storage Class with access mode %q and fstype %q ", accessmode, nfs4FSType))
		storageclass, pvclaims, err := createStorageClassWithMultiplePVCs(client, namespace, labelsMap,
			scParameters, diskSize, nil, bindingModeWffc, false, accessmode,
			defaultNginxStorageClassName, nil, createPvcItr, false, false)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		defer func() {
			err = client.StorageV1().StorageClasses().Delete(ctx, storageclass.Name, *metav1.NewDeleteOptions(0))
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
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

		ginkgo.By("Create Deployment")
		for i := 0; i < len(pvclaims); i++ {
			deploymentList, _, err := createVerifyAndScaleDeploymentPods(ctx, client, namespace, int32(replica),
				false, labelsMap, pvclaims[i], nil, execRWXCommandPod, nginxImage, false, nil, createDepItr)
			depl = append(depl, deploymentList...)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}
		defer func() {
			framework.Logf("Delete deployment set")
			for i := 0; i < len(depl); i++ {
				err = client.AppsV1().Deployments(namespace).Delete(ctx, depl[i].Name, *metav1.NewDeleteOptions(0))
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
			}
		}()

		ginkgo.By("Verify PVC Bound state and CNS side verification")
		_, err = checkVolumeStateAndPerformCnsVerification(ctx, client, pvclaims, "", remoteDsUrl)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("disable vsan network on one the host's vmknic in cluster4")
		workervms := getWorkerVmMoRefs(ctx, client)
		targetHost := e2eVSphere.getHostFromVMReference(ctx, workervms[0].Reference())
		targetHostSystem := object.NewHostSystem(e2eVSphere.Client.Client, targetHost.Reference())
		nicMgr, err = targetHostSystem.ConfigManager().VirtualNicManager(ctx)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		vmknic4VsanDown = true
		err = nicMgr.DeselectVnic(ctx, "vsan", GetAndExpectStringEnvVar(envVmknic4Vsan))
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		defer func() {
			ginkgo.By("enable vsan network on the host's vmknic in cluster4")
			err = nicMgr.SelectVnic(ctx, "vsan", GetAndExpectStringEnvVar(envVmknic4Vsan))
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			vmknic4VsanDown = false
		}()

		ginkgo.By("Scale up all deployment sets to replica count 5")
		replica = 5
		for i := 0; i < len(depl); i++ {
			_, _, err = createVerifyAndScaleDeploymentPods(ctx, client, namespace, int32(replica),
				true, labelsMap, pvclaims[i], nil, execRWXCommandPod, nginxImage, true, depl[i], 0)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}

		ginkgo.By("enable vsan network on the host's vmknic in cluster4")
		err = nicMgr.SelectVnic(ctx, "vsan", GetAndExpectStringEnvVar(envVmknic4Vsan))
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		vmknic4VsanDown = false

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

		ginkgo.By("Scale up all deployment sets to replica count 7")
		replica = 7
		for i := 0; i < len(depl); i++ {
			_, _, err = createVerifyAndScaleDeploymentPods(ctx, client, namespace, int32(replica),
				true, labelsMap, pvclaims[i], nil, execRWXCommandPod, nginxImage, true, depl[i], 0)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}
	})

	/*
		One host isolation on any k8s cluster
		Immediate mode, all allowed topologies, remote datastore url

		Steps:
		1. Create SC with Immedaite mode with all allowed topology details.
		2. Create few PVCs with RWX access mode.
		3. Create deployment sets with different replica counts.
		4. Volume provisioning should happen on the remote datastore url.
		5. Create statefulset with replica count 5.
		6. Pods can get created on any Az.
		7. Isolate a host that has a k8s worker with some pods attached to it.
		8. Wait for 5-10 mins, verify that the k8s worker is restarted and brought up on another host.
		9. Verify that the replicas on the k8s worker come up successfully
		10. Scale up deployment to 3 replica and scale down statefulset1 to 3 replicas
		11. Re-integrate the host
		12. Verify k8s cluster health state. All k8s nodes should be in a healthy state.
		13. Verify workload pods status. ALl pods should come up to healthy running state.
		14. Perform cleanup by deleting Pods, PVCs and SC.
	*/

	ginkgo.It("Any single host isolation from the multicluster hci setup ", ginkgo.Label(p1, file, vanilla,
		level5, level2, newTest, disruptive), func() {

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		var service *v1.Service
		var replica int32
		var depl []*appsv1.Deployment
		var statefulset *appsv1.StatefulSet

		createPvcItr := 5
		createDepItr := 1
		stsReplicas := 7

		// remote datastore url
		remoteDsUrl := GetAndExpectStringEnvVar(envRemoteDatastoreUrl)
		scParameters["datastoreurl"] = remoteDsUrl

		ginkgo.By(fmt.Sprintf("Creating Storage Class with access mode %q and fstype %q ", accessmode, nfs4FSType))
		storageclass, pvclaims, err := createStorageClassWithMultiplePVCs(client, namespace, labelsMap,
			scParameters, diskSize, nil, bindingModeImm, false, accessmode,
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
		_, err = checkVolumeStateAndPerformCnsVerification(ctx, client, pvclaims, "", remoteDsUrl)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Create Deployment")
		for i := 0; i < len(pvclaims); i++ {
			if i == 0 {
				replica = 3
			} else if i == 1 {
				replica = 4
			} else if i == 2 {
				replica = 5
			} else {
				replica = 1
			}
			deploymentList, _, err := createVerifyAndScaleDeploymentPods(ctx, client, namespace, replica, false, labelsMap,
				pvclaims[i], nil, execRWXCommandPod, nginxImage, false, nil, createDepItr)
			depl = append(depl, deploymentList...)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}
		defer func() {
			framework.Logf("Delete deployment set")
			for i := 0; i < len(depl); i++ {
				err = client.AppsV1().Deployments(namespace).Delete(ctx, depl[i].Name, *metav1.NewDeleteOptions(0))
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
			}
		}()

		ginkgo.By("Create StatefulSet with replica count 3 with RWX PVC access mode")
		service, statefulset, err = createStafeulSetAndVerifyPVAndPodNodeAffinty(ctx, client, namespace, true,
			int32(stsReplicas), false, allowedTopologies, false, false, false, accessmode, storageclass, false, "")
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Enter host into maintenance mode with evacuateAllData, i.e. isolate one host from Az2")
		var timeout int32 = 300
		hostsInCluster2 := getHostsByClusterName(ctx, clusterComputeResource, topologyClusterList[1])
		enterHostIntoMM(ctx, hostsInCluster2[0], evacMModeType, timeout, true)
		defer func() {
			framework.Logf("Exit the hosts from MM before terminating the test")
			exitHostMM(ctx, hostsInCluster2[0], timeout)
		}()

		ginkgo.By("Scale up all deployment sets to replica count 5")
		replica = 5
		for i := 0; i < len(depl); i++ {
			_, _, err = createVerifyAndScaleDeploymentPods(ctx, client, namespace, int32(replica),
				true, labelsMap, pvclaims[i], nil, execRWXCommandPod, nginxImage, true, depl[i], 0)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}

		ginkgo.By("Perform scaledown operation on statefulset sets. Decrease the replica count to 3")
		scaleDownReplicaCount := 3
		err = performScalingOnStatefulSetAndVerifyPvNodeAffinity(ctx, client, 0,
			int32(scaleDownReplicaCount), statefulset, false, namespace, allowedTopologies, false, true, false)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Exit the hosts from MM")
		exitHostMM(ctx, hostsInCluster2[0], timeout)

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
	})

	/*
	   Power off 2 worker nodes
	   Immediate, all allowed topology, remote datastore url
	   Steps:

	   1. Create SC with Immediate with all topology details.
	   2. Create few PVCs with RWX access mode.
	   3. Volume provision should happen on the remote datastore.
	   4. Create deployment pods with different replica counts.
	   5. Pods can be created on any Az.
	   6. Power off 2 k8s worker nodes.
	   7. Create new rwc pvcs.
	   8. Attach it to standalone Pods.
	   9. Perform scaleup operation on any one dpeloyment Pod.
	   10. Power on the worker nodes.
	   11. Wait for k8s cluster to be healthy.
	   12. Verify k8s nodes status. Nodes should come up to healthy running state.
	   13. Wait for all workload pods to come to healthy state.
	   14. Perform scaleup operaion on all deployment Pods. Increase the replica count to 7.
	   15. Perform cleanup by deleting Pods, PVC and SC,
	*/

	ginkgo.It("Power off worker nodes in a k8s cluster having hci mesh "+
		"datastore tagged in sc", ginkgo.Label(p1, file, vanilla,
		level5, level2, newTest, disruptive), func() {

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		createPvcItr := 5
		createDepItr := 1

		var replica int32
		var depl []*appsv1.Deployment

		// remote datastore url
		remoteDsUrl := GetAndExpectStringEnvVar(envRemoteDatastoreUrl)
		scParameters["datastoreurl"] = remoteDsUrl

		ginkgo.By(fmt.Sprintf("Creating Storage Class with access mode %q and fstype %q ", accessmode, nfs4FSType))
		storageclass, pvclaims, err := createStorageClassWithMultiplePVCs(client, namespace, labelsMap,
			scParameters, diskSize, nil, bindingModeImm, false, accessmode,
			defaultNginxStorageClassName, nil, createPvcItr, false, false)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		defer func() {
			err = client.StorageV1().StorageClasses().Delete(ctx, storageclass.Name, *metav1.NewDeleteOptions(0))
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
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
		_, err = checkVolumeStateAndPerformCnsVerification(ctx, client, pvclaims, "", remoteDsUrl)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Create Deployment")
		for i := 0; i < len(pvclaims); i++ {
			if i == 0 {
				replica = 3
			} else if i == 1 {
				replica = 4
			} else if i == 2 {
				replica = 5
			} else {
				replica = 1
			}
			deploymentList, _, err := createVerifyAndScaleDeploymentPods(ctx, client, namespace, replica, false, labelsMap,
				pvclaims[i], nil, execRWXCommandPod, nginxImage, false, nil, createDepItr)
			depl = append(depl, deploymentList...)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}
		defer func() {
			framework.Logf("Delete deployment set")
			for i := 0; i < len(depl); i++ {
				err = client.AppsV1().Deployments(namespace).Delete(ctx, depl[i].Name, *metav1.NewDeleteOptions(0))
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
			}
		}()

		// power off 2 worker nodes
		ginkgo.By("Power off 2 worker nodes in a k8s cluster")
		powerOffNodeList, _ := getControllerRuntimeDetails(client, csiSystemNamespace)

		var vms []*object.VirtualMachine
		for i := 0; i < 2; i++ {
			nodeName := powerOffNodeList[i]
			ginkgo.By(fmt.Sprintf("Power off the node: %v", nodeName))
			vmUUID := getNodeUUID(ctx, client, nodeName)
			gomega.Expect(vmUUID).NotTo(gomega.BeEmpty())
			framework.Logf("VM uuid is: %s for node: %s", vmUUID, nodeName)
			vmRef, err := e2eVSphere.getVMByUUID(ctx, vmUUID)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			framework.Logf("vmRef: %+v for the VM uuid: %s", vmRef, vmUUID)
			gomega.Expect(vmRef).NotTo(gomega.BeNil(), "vmRef should not be nil")
			vm := object.NewVirtualMachine(e2eVSphere.Client.Client, vmRef.Reference())
			vms = append(vms, vm)
			_, err = vm.PowerOff(ctx)
			framework.ExpectNoError(err)

			err = vm.WaitForPowerState(ctx, vimtypes.VirtualMachinePowerStatePoweredOff)
			framework.ExpectNoError(err, "Unable to power off the node")

			ginkgo.By("Wait for k8s to schedule the pod on other node")
			time.Sleep(k8sPodTerminationTimeOutLong)
		}

		ginkgo.By("Creating new rwx pvcs when multiple sites are down")
		createPvcItr = 4
		_, pvclaimsNew, _ := createStorageClassWithMultiplePVCs(client, namespace, labelsMap, scParameters,
			diskSize, nil, bindingModeImm, false, accessmode, "", storageclass, createPvcItr, true, false)
		defer func() {
			for i := 0; i < len(pvclaimsNew); i++ {
				pv := getPvFromClaim(client, pvclaimsNew[i].Namespace, pvclaimsNew[i].Name)
				err = fpv.DeletePersistentVolumeClaim(ctx, client, pvclaimsNew[i].Name, namespace)
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
				err = e2eVSphere.waitForCNSVolumeToBeDeleted(pv.Spec.CSI.VolumeHandle)
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
			}
		}()

		ginkgo.By("Create standalone Pods and attach it to pvc created above")
		var podListNew []*v1.Pod
		noPodsToDeploy := 1
		for i := 0; i < len(pvclaimsNew); i++ {
			podListNew, _ = createStandalonePodsForRWXVolume(client, ctx, namespace, nil,
				pvclaimsNew[i], false, execRWXCommandPod, noPodsToDeploy)
		}
		defer func() {
			for i := 0; i < len(podListNew); i++ {
				ginkgo.By(fmt.Sprintf("Deleting the pod %s in namespace %s", podListNew[i].Name, namespace))
				err = fpod.DeletePodWithWait(ctx, client, podListNew[i])
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
			}
		}()

		ginkgo.By("Scale up deployment1 to replica count 5")
		replica = 5
		_, _, _ = createVerifyAndScaleDeploymentPods(ctx, client, namespace, int32(replica),
			true, labelsMap, pvclaims[0], nil, execRWXCommandPod, nginxImage, true, depl[0], 0)

		// power on vm
		ginkgo.By("Power on the k8s worker node which was powererd off")
		for i := 0; i < len(vms); i++ {
			_, err = vms[i].PowerOn(ctx)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
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

		ginkgo.By("Scale up all deployment sets to replica count 10")
		replica = 7
		for i := 0; i < len(depl); i++ {
			_, _, err = createVerifyAndScaleDeploymentPods(ctx, client, namespace, int32(replica),
				true, labelsMap, pvclaims[i], nil, execRWXCommandPod, nginxImage, true, depl[i], 0)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}
	})

	/*
	   Bring down full site cluster-2/Az2
	   SC → Immediate Binding Mode, → default values, datastore url of HCI Mesh datastore

	   Steps:
	   1. Create SC with Immediate Binding mode and set SC with default values,
	   taking datastore url of HCI Mesh datastore (remote datastore in this case)
	   2. Create multiple PVCs with RWX access mode.
	   3. Wait for all PVCs to reach Bound state.
	   4. Create deployment Pods for each PVC with replica count 3
	   5. Wait for Pods to reach running state
	   6. Pods should get created on any Az.
	   7. PVC should get created on the remote datastore.
	   8. Bring down all ESXi hosts in cluster2 i.e. bringing down full site i.e. cluster2 (rack-2)
	   9. Scale up deployment1 Pod replica to count 5
	   10. Scale down deployment2 Pod to 1 replica
	   11. Create new multiple PVCs with "RWX"access mode.
	   12. Attach newly created PVcs to standalone Pods
	   13. Bring up the site which was down.
	   14. Wait for testbed to be back to normal state.
	   15. Verify K8s Nodes status and Workload Pod status. All should come back to Running Ready state.
	   16. Perform scaleup operation on deployment sets. Increase the replica count to 7.
	   17. Perform cleanup by deleting Pods, PVCs and SC.
	*/

	ginkgo.It("Full site down of Az2 when remote datastore url is passed in sc", ginkgo.Label(p1, file, vanilla,
		level5, level2, newTest, disruptive), func() {

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		var depl []*appsv1.Deployment

		replica := 3
		createPvcItr := 5
		createDepItr := 1

		// remote datastore url
		remoteDsUrl := GetAndExpectStringEnvVar(envRemoteDatastoreUrl)
		scParameters["datastoreurl"] = remoteDsUrl

		ginkgo.By(fmt.Sprintf("Creating Storage Class with access mode %q and fstype %q ", accessmode, nfs4FSType))
		storageclass, pvclaims, err := createStorageClassWithMultiplePVCs(client, namespace, labelsMap,
			scParameters, diskSize, nil, bindingModeImm, false, accessmode,
			defaultNginxStorageClassName, nil, createPvcItr, false, false)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		defer func() {
			err = client.StorageV1().StorageClasses().Delete(ctx, storageclass.Name, *metav1.NewDeleteOptions(0))
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
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
		_, err = checkVolumeStateAndPerformCnsVerification(ctx, client, pvclaims, "", remoteDsUrl)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Create Deployment")
		for i := 0; i < len(pvclaims); i++ {
			deploymentList, _, err := createVerifyAndScaleDeploymentPods(ctx, client, namespace, int32(replica), false,
				labelsMap, pvclaims[i], nil, execRWXCommandPod, nginxImage, false, nil, createDepItr)
			depl = append(depl, deploymentList...)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}
		defer func() {
			framework.Logf("Delete deployment set")
			for i := 0; i < len(depl); i++ {
				err = client.AppsV1().Deployments(namespace).Delete(ctx, depl[i].Name, *metav1.NewDeleteOptions(0))
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
			}
		}()

		ginkgo.By("Bring Az2 site fully down")
		hostsInCluster := getHostsByClusterName(ctx, clusterComputeResource, topologyClusterList[1])
		powerOffHostsList2 := powerOffEsxiHostByCluster(ctx, &e2eVSphere, topologyClusterList[1],
			len(hostsInCluster))
		defer func() {
			ginkgo.By("Bring up ESXi host which were powered off in Az2")
			for i := 0; i < len(powerOffHostsList2); i++ {
				powerOnEsxiHostByCluster(powerOffHostsList2[i])
			}
		}()

		ginkgo.By("Scale up deployment1 to replica count 5")
		replica = 5
		_, _, _ = createVerifyAndScaleDeploymentPods(ctx, client, namespace, int32(replica),
			true, labelsMap, pvclaims[0], nil, execRWXCommandPod, nginxImage, true, depl[0], 0)

		ginkgo.By("Scale down deployment2 to replica count 1")
		replica = 1
		_, _, _ = createVerifyAndScaleDeploymentPods(ctx, client, namespace, int32(replica),
			true, labelsMap, pvclaims[1], nil, execRWXCommandPod, nginxImage, true, depl[1], 0)

		ginkgo.By("Creating new rwx pvcs when multiple sites are down")
		createPvcItr = 4
		_, pvclaimsNew, _ := createStorageClassWithMultiplePVCs(client, namespace, labelsMap, scParameters,
			diskSize, nil, bindingModeImm, false, accessmode, "", storageclass, createPvcItr, true, false)
		defer func() {
			for i := 0; i < len(pvclaimsNew); i++ {
				pv := getPvFromClaim(client, pvclaimsNew[i].Namespace, pvclaimsNew[i].Name)
				err = fpv.DeletePersistentVolumeClaim(ctx, client, pvclaimsNew[i].Name, namespace)
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
				err = e2eVSphere.waitForCNSVolumeToBeDeleted(pv.Spec.CSI.VolumeHandle)
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
			}
		}()

		ginkgo.By("Create standalone Pods and attach it to pvc created above")
		var podListNew []*v1.Pod
		noPodsToDeploy := 3
		for i := 0; i < len(pvclaimsNew); i++ {
			podListNew, _ = createStandalonePodsForRWXVolume(client, ctx, namespace, nil,
				pvclaimsNew[i], false, execRWXCommandPod, noPodsToDeploy)
		}
		defer func() {
			for i := 0; i < len(podListNew); i++ {
				ginkgo.By(fmt.Sprintf("Deleting the pod %s in namespace %s", podListNew[i].Name, namespace))
				err = fpod.DeletePodWithWait(ctx, client, podListNew[i])
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
			}
		}()

		// Bring up
		ginkgo.By("Bring up all ESXi host which were powered off")
		for i := 0; i < len(powerOffHostsList2); i++ {
			powerOnEsxiHostByCluster(powerOffHostsList2[i])
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

		ginkgo.By("Scale up all deployment sets to replica count 10")
		replica = 10
		for i := 0; i < len(depl); i++ {
			_, _, err = createVerifyAndScaleDeploymentPods(ctx, client, namespace, int32(replica),
				true, labelsMap, pvclaims[i], nil, execRWXCommandPod, nginxImage, true, depl[i], 0)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}
	})

	/*
		Bring down full site cluster-3 and bring down 2 ESXi-hosts in Remote cluster
		SC → Immediate Binding Mode, → default values, → datastore url of HCI Mesh datastore

		Steps:
		1. Create SC with Immediate binding mode and with default values, pass datastore url of remote cluster
		2. Create multiple PVCs with "RWX" access mode.
		3. Create deployment set with replica count 3 using rwx pvc created above
		4. Wait for all the Pods and PVCs to reach Bound and running state.
		5. Make sure Pods are running across all AZs
		6. Create statefulset with replica count 3.
		7. Wait for statefulset pvc to reach Bound and statefulset pods to reach running.
		8. Bring down full site Az3 down and partial site down in remote cluster.
		9. Now perform scaleup operation on statefulset and deployment set.
		10. Create new rwx pvcs when sites are down.
		11. Attach standalone pods to newly created pvcs.
		12. Bring back the sites.
		13. Wait for testbed to come back to healthy state.
		14. Verify all the workload pod status. It should come to running state.
		15. Perform scaledown operation on statefulset.
		16. Perform scaleup operation on deployment set.
		17. Verify scaling operation went smooth.
		18. Perform cleanup by deleting Pods, PVCs and Sc.

	*/

	ginkgo.It("Full site down of Az2 and partial site down of "+
		"remote Az where remote ds url is passed in sc", ginkgo.Label(p1, file, vanilla,
		level5, level2, newTest, disruptive), func() {

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		var service *v1.Service
		var depl []*appsv1.Deployment
		var statefulset *appsv1.StatefulSet
		var powerOffHostsList []string

		stsReplicas := 3
		replica := 3
		createPvcItr := 5
		createDepItr := 1

		// remote datastore url
		remoteDsUrl := GetAndExpectStringEnvVar(envRemoteDatastoreUrl)
		scParameters["datastoreurl"] = remoteDsUrl

		ginkgo.By(fmt.Sprintf("Creating Storage Class with access mode %q and fstype %q ", accessmode, nfs4FSType))
		storageclass, pvclaims, err := createStorageClassWithMultiplePVCs(client, namespace, labelsMap,
			scParameters, diskSize, nil, bindingModeImm, false, accessmode,
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
		_, err = checkVolumeStateAndPerformCnsVerification(ctx, client, pvclaims, "", remoteDsUrl)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Create Deployment")
		for i := 0; i < len(pvclaims); i++ {
			deploymentList, _, err := createVerifyAndScaleDeploymentPods(ctx, client, namespace, int32(replica), false,
				labelsMap, pvclaims[i], nil, execRWXCommandPod, nginxImage, false, nil, createDepItr)
			depl = append(depl, deploymentList...)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}
		defer func() {
			framework.Logf("Delete deployment set")
			for i := 0; i < len(depl); i++ {
				err = client.AppsV1().Deployments(namespace).Delete(ctx, depl[i].Name, *metav1.NewDeleteOptions(0))
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
			}
		}()

		ginkgo.By("Create StatefulSet with replica count 3 using RWX PVC access mode")
		service, statefulset, err = createStafeulSetAndVerifyPVAndPodNodeAffinty(ctx, client, namespace, true,
			int32(stsReplicas), false, nil, false, false, false, accessmode, storageclass, false, "")
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Bring down full site Az3 and bring down partial site remote cluster")
		hostsInCluster := getHostsByClusterName(ctx, clusterComputeResource, topologyClusterList[2])
		powerOffHostsList3 := powerOffEsxiHostByCluster(ctx, &e2eVSphere, topologyClusterList[2],
			len(hostsInCluster))
		powerOffHostsList = append(powerOffHostsList, powerOffHostsList3...)
		defer func() {
			ginkgo.By("Bring up ESXi host which were powered off in Az3")
			for i := 0; i < len(powerOffHostsList3); i++ {
				powerOnEsxiHostByCluster(powerOffHostsList3[i])
			}
		}()

		noOfHostToBringDown := 1
		powerOffHostsList4 := powerOffEsxiHostByCluster(ctx, &e2eVSphere, topologyClusterList[3],
			noOfHostToBringDown)
		powerOffHostsList = append(powerOffHostsList, powerOffHostsList4...)
		defer func() {
			ginkgo.By("Bring up  ESXi host which were powered off in Az4")
			powerOnEsxiHostByCluster(powerOffHostsList4[0])
		}()

		ginkgo.By("Scale up deployment to replica count 5")
		replica = 5
		for i := 0; i < len(depl); i++ {
			_, _, err = createVerifyAndScaleDeploymentPods(ctx, client, namespace, int32(replica),
				true, labelsMap, pvclaims[i], nil, execRWXCommandPod, nginxImage, true, depl[i], 0)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}

		ginkgo.By("Perform scaleup operation on statefulset. Increase the replica count to 5")
		scaleUpReplicaCount := 5
		err = performScalingOnStatefulSetAndVerifyPvNodeAffinity(ctx, client, int32(scaleUpReplicaCount),
			0, statefulset, false, namespace, nil, true, false, false)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Creating new rwx pvcs when multiple sites are down")
		createPvcItr = 4
		_, pvclaimsNew, _ := createStorageClassWithMultiplePVCs(client, namespace, labelsMap, scParameters,
			diskSize, nil, bindingModeImm, false, accessmode, "", storageclass, createPvcItr, true, false)
		defer func() {
			for i := 0; i < len(pvclaimsNew); i++ {
				pv := getPvFromClaim(client, pvclaimsNew[i].Namespace, pvclaimsNew[i].Name)
				err = fpv.DeletePersistentVolumeClaim(ctx, client, pvclaimsNew[i].Name, namespace)
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
				err = e2eVSphere.waitForCNSVolumeToBeDeleted(pv.Spec.CSI.VolumeHandle)
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
			}
		}()

		ginkgo.By("Create standalone Pods and attach it to pvc created above")
		var podListNew []*v1.Pod
		noPodsToDeploy := 3
		for i := 0; i < len(pvclaimsNew); i++ {
			podListNew, _ = createStandalonePodsForRWXVolume(client, ctx, namespace,
				nil, pvclaimsNew[i], false, execRWXCommandPod, noPodsToDeploy)
		}
		defer func() {
			for i := 0; i < len(podListNew); i++ {
				ginkgo.By(fmt.Sprintf("Deleting the pod %s in namespace %s", podListNew[i].Name, namespace))
				err = fpod.DeletePodWithWait(ctx, client, podListNew[i])
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

		ginkgo.By("Scale down statefulset replica count to 1")
		scaleDownReplicaCount := 1
		err = performScalingOnStatefulSetAndVerifyPvNodeAffinity(ctx, client, 0,
			int32(scaleDownReplicaCount), statefulset, false, namespace, nil, false, true, false)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Perform scaleup operation on deployment. Increase the replica count to 7")
		replica = 7
		for i := 0; i < len(depl); i++ {
			_, _, err = createVerifyAndScaleDeploymentPods(ctx, client, namespace, int32(replica),
				true, labelsMap, pvclaims[i], nil, execRWXCommandPod, nginxImage, true, depl[i], 0)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}
	})

	/*
		Partial sites down (cluster-2 and cluster-3) and remote site fully down
		SC → Immediate Binding Mode, → all allowed topology specified

		Steps:
		1. Create SC with Immediate Binding mode and set SC with all allowed topology details
		2. Create multiple PVCs with RWX access mode.
		3. Wait for all PVCs to reach Bound state.
		4. Create deployment Pods for each PVC with replica count 2
		5. Wait for Pods to reach running state
		6. PVCs and Pods should get created on any AZ as all allowed topology is specified in SC.
		7. When deployment Pod creation is in progress, Bring down 1 esxi hosts each
		in cluster-2 and cluster-3 and bring down full remote cluster down
		8. Create statefulset with replica count 3 when sites are down.
		9. Create new set of rwx pvcs when sites are down.
		10. Attach standalone pods to pvcs created in step #9.
		11. Perform scaleup operation on deployment. Increase the replica count of dep1 to 5.
		12. Bring up all sites which were down i.e. cluster2, cluster3 and remote cluster
		13. Wait for testbed to be back to normal state.
		14. Verify K8s Nodes status and Workload Pod status. All should come back to Running Ready state.
		15. Perform Scaleup/ScaleDown operation on deployment set and statefulset.
		16. Verify scaling operation should go smooth once sites are restored.
		17. Perform cleanup by deleting Pods, PVCs and SC.
	*/

	ginkgo.It("Partial multiple sites down and remote site fully down", ginkgo.Label(p1, file, vanilla,
		level5, level2, newTest, disruptive), func() {

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		var service *v1.Service
		var depl []*appsv1.Deployment
		var powerOffHostsList []string
		var statefulset *appsv1.StatefulSet

		stsReplicas := 3
		replica := 2
		createPvcItr := 5
		createDepItr := 1

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

		ginkgo.By("Create Deployments and perform sites down when pod creations are in progress")
		for i := 0; i < len(pvclaims); i++ {
			deploymentList, _, _ := createVerifyAndScaleDeploymentPods(ctx, client, namespace, int32(replica), false, labelsMap,
				pvclaims[i], nil, execRWXCommandPod, nginxImage, false, nil, createDepItr)
			depl = append(depl, deploymentList...)

			if i == 2 {
				ginkgo.By("Bring down 1 ESXi host in zone2 and Bring down 1 ESXi " +
					"host in zone3 and full remote site down")
				noOfHostToBringDown := 1
				powerOffHostsList2 := powerOffEsxiHostByCluster(ctx, &e2eVSphere, topologyClusterList[1],
					noOfHostToBringDown)
				powerOffHostsList = append(powerOffHostsList, powerOffHostsList2...)
				defer func() {
					ginkgo.By("Bring up ESXi host which were powered off in Az2")
					powerOnEsxiHostByCluster(powerOffHostsList2[0])
				}()

				powerOffHostsList3 := powerOffEsxiHostByCluster(ctx, &e2eVSphere, topologyClusterList[2],
					noOfHostToBringDown)
				powerOffHostsList = append(powerOffHostsList, powerOffHostsList3...)
				defer func() {
					ginkgo.By("Bring up  ESXi host which were powered off in Az3")
					powerOnEsxiHostByCluster(powerOffHostsList3[0])
				}()

				hostsInCluster := getHostsByClusterName(ctx, clusterComputeResource, topologyClusterList[3])
				powerOffHostsList4 := powerOffEsxiHostByCluster(ctx, &e2eVSphere, topologyClusterList[3],
					len(hostsInCluster))
				powerOffHostsList = append(powerOffHostsList, powerOffHostsList4...)
				defer func() {
					ginkgo.By("Bring up ESXi host which were powered off in remote cluster")
					for i := 0; i < len(powerOffHostsList4); i++ {
						powerOnEsxiHostByCluster(powerOffHostsList4[i])
					}
				}()
			}
		}
		defer func() {
			framework.Logf("Delete deployment set")
			for i := 0; i < len(depl); i++ {
				err = client.AppsV1().Deployments(namespace).Delete(ctx, depl[i].Name, *metav1.NewDeleteOptions(0))
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
			}
		}()

		ginkgo.By("Create StatefulSet with replica count 3 using RWX PVC access mode when sites are down")
		service, statefulset, _ = createStafeulSetAndVerifyPVAndPodNodeAffinty(ctx, client, namespace, true,
			int32(stsReplicas), false, allowedTopologies, false, false, false, accessmode, storageclass, false, "")

		ginkgo.By("Creating new rwx pvcs when multiple sites are down")
		createPvcItr = 4
		_, pvclaimsNew, _ := createStorageClassWithMultiplePVCs(client, namespace, labelsMap, scParameters,
			diskSize, allowedTopologies, bindingModeImm, false, accessmode, "", storageclass, createPvcItr, true, false)
		defer func() {
			for i := 0; i < len(pvclaimsNew); i++ {
				pv := getPvFromClaim(client, pvclaimsNew[i].Namespace, pvclaimsNew[i].Name)
				err = fpv.DeletePersistentVolumeClaim(ctx, client, pvclaimsNew[i].Name, namespace)
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
				err = e2eVSphere.waitForCNSVolumeToBeDeleted(pv.Spec.CSI.VolumeHandle)
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
			}
		}()

		ginkgo.By("Create single standalone Pods using pvc created above")
		var podListNew []*v1.Pod
		noPodsToDeploy := 3
		for i := 0; i < len(pvclaimsNew); i++ {
			podListNew, _ = createStandalonePodsForRWXVolume(client, ctx, namespace, nil,
				pvclaimsNew[i], false, execRWXCommandPod, noPodsToDeploy)
		}
		defer func() {
			for i := 0; i < len(podListNew); i++ {
				ginkgo.By(fmt.Sprintf("Deleting the pod %s in namespace %s", podListNew[i].Name, namespace))
				err = fpod.DeletePodWithWait(ctx, client, podListNew[i])
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
			}
		}()

		ginkgo.By("Scale up dep1 to replica count 5")
		replica = 5
		_, _, err = createVerifyAndScaleDeploymentPods(ctx, client, namespace, int32(replica),
			true, labelsMap, pvclaims[0], nil, execRWXCommandPod, nginxImage, true, depl[0], 0)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

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

		ginkgo.By("Scale down dep3 to replica count 1")
		replica = 1
		_, _, err = createVerifyAndScaleDeploymentPods(ctx, client, namespace, int32(replica),
			true, labelsMap, pvclaims[2], nil, execRWXCommandPod, nginxImage, true, depl[2], 0)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Perform scaleup operation on statefulset. Increase the replica count to 5")
		scaleUpReplicaCount := 1
		err = performScalingOnStatefulSetAndVerifyPvNodeAffinity(ctx, client, int32(scaleUpReplicaCount),
			0, statefulset, false, namespace, allowedTopologies, true, false, false)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
	})

	/*
		PSOD all ESXI hosts which is on cluster-2 and cluster-3
		SC → Immediate Binding mode, all allowed topology set

		Steps:
		1. Create SC with Immediate binding mode and allowed topology set to all racks
		2. Create multiple PVCs with "RWX" access mode.
		3. Create deployment set with replica count 3.
		4. Wait for all the Pods and PVCs to reach Bound and running state.
		5. Make sure Pods are running across all AZs
		6. Create statefulset with replica count 3
		7. PSOD all esxi hosts which is on cluster-2
		8. Create new set of rwx pvc when psod is triggered on cluster-2
		9. Create 3 standalone pods and attach it to newly created rwx pvcs
		10. PSOD all esxi hosts which is in cluster-3
		11. Perform scaleup operation on deployment and statefulset. Increase the replica count to 5.
		12. Create new PVCs and attach it standalone Pods.
		13. Verify the ESXI hosts for which we performed PSOD should come back to responding state.
		14. Verify that the k8s cluster is healthy
		15. Once the testbed is normal verify that the all workload Pods are in up and running state.
		16. Perform scale-up/scale-down on the dep-1 and dep-2
		17. Perform cleanup by deleting Pods, PVCs and SC.
	*/

	ginkgo.It("Psod all esxi hosts on Az2 and Az3 and in between rwx volume "+
		"creation and workload creation", ginkgo.Label(p1, file, vanilla,
		level5, level2, newTest, disruptive), func() {

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		var pvclaimsNew []*v1.PersistentVolumeClaim
		var hostList []string
		var service *v1.Service
		var depl []*appsv1.Deployment
		var statefulset *appsv1.StatefulSet
		var podList []*v1.Pod

		stsReplicas := 3
		replica := 3
		createPvcItr := 5
		createDepItr := 1

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

		ginkgo.By("Create Deployments")
		for i := 0; i < len(pvclaims); i++ {
			deploymentList, _, err := createVerifyAndScaleDeploymentPods(ctx, client, namespace, int32(replica), false,
				labelsMap, pvclaims[i], nil, execRWXCommandPod, nginxImage, false, nil, createDepItr)
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

		ginkgo.By("Create StatefulSet with replica count 3 using RWX PVC access mode")
		service, statefulset, err = createStafeulSetAndVerifyPVAndPodNodeAffinty(ctx, client, namespace, true,
			int32(stsReplicas), false, allowedTopologies, false, false, false, accessmode, storageclass, false, "")
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Fetch list of hosts in local cluster2")
		hostListCluster2 := fetchListofHostsInCluster(ctx, topologyClusterList[1])
		hostList = append(hostList, hostListCluster2...)

		ginkgo.By("PSOD all host in local cluster2 and when psod is triggered, create new set of rwx pvc")
		for i := 0; i < len(hostListCluster2); i++ {
			err = psodHost(hostListCluster2[i])
			gomega.Expect(err).NotTo(gomega.HaveOccurred())

			if i == 0 {
				ginkgo.By("Create new 3 rwx pvcs")
				createPvcItr = 3
				_, pvclaimsNew, err = createStorageClassWithMultiplePVCs(client, namespace, labelsMap, scParameters,
					diskSize, nil, bindingModeImm, false, accessmode, "", storageclass, createPvcItr, true, false)
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
			}

			if i == 2 {
				ginkgo.By("Verify PVC Bound state and CNS side verification")
				_, err = checkVolumeStateAndPerformCnsVerification(ctx, client, pvclaimsNew, "", "")
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
			}
		}

		ginkgo.By("Create standalone Pods with different read/write permissions and attach it to a newly created rwx pvcs")
		noPodsToDeploy := 3
		for i := 0; i < len(pvclaimsNew); i++ {
			pods, _ := createStandalonePodsForRWXVolume(client, ctx, namespace, nil, pvclaimsNew[i], false, execRWXCommandPod,
				noPodsToDeploy)
			podList = append(podList, pods...)
		}
		defer func() {
			for i := 0; i < len(hostListCluster2); i++ {
				ginkgo.By("checking host status")
				err := waitForHostToBeUp(hostListCluster2[i])
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
			}
		}()
		defer func() {
			for i := 0; i < len(pvclaimsNew); i++ {
				pv := getPvFromClaim(client, pvclaimsNew[i].Namespace, pvclaimsNew[i].Name)
				err = fpv.DeletePersistentVolumeClaim(ctx, client, pvclaimsNew[i].Name, namespace)
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
				err = e2eVSphere.waitForCNSVolumeToBeDeleted(pv.Spec.CSI.VolumeHandle)
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
			}
		}()
		defer func() {
			for i := 0; i < len(podList); i++ {
				ginkgo.By(fmt.Sprintf("Deleting the pod %s in namespace %s", podList[i].Name, namespace))
				err = fpod.DeletePodWithWait(ctx, client, podList[i])
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
			}
		}()

		ginkgo.By("Fetch list of hosts in local cluster3")
		hostListCluster3 := fetchListofHostsInCluster(ctx, topologyClusterList[2])
		hostList = append(hostList, hostListCluster3...)

		ginkgo.By("PSOD all host in local cluster3 and perform scaleup " +
			"operation on deployment and statefulset")
		for i := 0; i < len(hostListCluster3); i++ {
			err = psodHost(hostListCluster3[i])
			gomega.Expect(err).NotTo(gomega.HaveOccurred())

			if i == 0 {
				ginkgo.By("Scale up all deployment set replica count to 5 when psod is performed on Az3.")
				replica = 5
				for i := 0; i < len(depl); i++ {
					_, _, _ = createVerifyAndScaleDeploymentPods(ctx, client, namespace, int32(replica),
						true, labelsMap, pvclaims[i], nil, execRWXCommandPod, nginxImage, true, depl[i], 0)
				}
			}

			if i == 2 {
				ginkgo.By("Perform scaleup operation on statefulset. Increase the replica count to 5")
				scaleUpReplicaCount := 5
				err = performScalingOnStatefulSetAndVerifyPvNodeAffinity(ctx, client, int32(scaleUpReplicaCount),
					0, statefulset, false, namespace, allowedTopologies, true, false, false)
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
			}
		}

		ginkgo.By("Creating new rwx pvc when psod is trigegerd on Az3 site")
		createPvcItr = 1
		_, pvclaimsNewSet, err := createStorageClassWithMultiplePVCs(client, namespace, labelsMap, scParameters,
			diskSize, allowedTopologies, bindingModeImm, false, accessmode, "", storageclass, createPvcItr, true, false)
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

		ginkgo.By("Create single standalone Pods using pvc created above")
		noPodsToDeploy = 1
		podListNew, err := createStandalonePodsForRWXVolume(client, ctx, namespace, nil, pvclaimsNewSet[0],
			false, execRWXCommandPod, noPodsToDeploy)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		defer func() {
			for i := 0; i < len(podListNew); i++ {
				ginkgo.By(fmt.Sprintf("Deleting the pod %s in namespace %s", podListNew[i].Name, namespace))
				err = fpod.DeletePodWithWait(ctx, client, podListNew[i])
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
			}
		}()

		ginkgo.By("Verify all esxi hosts have recovered from PSOD")
		for i := 0; i < len(hostList); i++ {
			ginkgo.By("checking host status")
			err := waitForHostToBeUp(hostList[i])
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
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

		ginkgo.By("Scale up dep1 to replica count 10")
		replica = 10
		_, _, err = createVerifyAndScaleDeploymentPods(ctx, client, namespace, int32(replica),
			true, labelsMap, pvclaims[0], nil, execRWXCommandPod, nginxImage, true, depl[0], 0)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Scale down dep2 to replica count 1")
		replica = 1
		_, _, err = createVerifyAndScaleDeploymentPods(ctx, client, namespace, int32(replica),
			true, labelsMap, pvclaims[1], nil, execRWXCommandPod, nginxImage, true, depl[1], 0)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
	})

	/*
	   Operation Storm and Multi Replica Usecase
	   Workload Pod creation in scale + Multiple site down (partial/full) + kill CSI provisioner and CSi-attacher +
	   dynamic Pod/PVC creation + vSAN-Health service down

	   SC → all allowed topology set, → Immediate Binding mode

	   Steps:
	   1. Identify the CSI-Controller-Pod where CSI Provisioner, CSI Attacher are the leader
	   2. Create Storage class with allowed topology set to all levels of AZs
	   3. Create 10 dynamic PVCs with "RWX" acces mode.
	   4. While volume creation is in progress, kill CSI-Provisioner identified in step #1
	   5. Wait for rwx pvcs to reach Bound state.
	   6. Create deployment Pods with 3 replicas each for each rwx created pvc
	   7. While pod creation is in progress, kill CSI-attacher, identified in step #1
	   8. Wait for PVCs to reach Bound state and Pods to reach running state.
	   9. Volume provisioning and pod placement should happen on any of the AZ.
	   10. Bring down all ESXI hosts of cluster-2/Az2
	   12. Try to perform scaleup operation on deployment (dep1, dep2 and dep3)
	   Increase the replica count from 3 to 7. When scaleup is going on, kill the CSI Attacher identified in step #1
	   13. Try reading/writing into the volume from each deployment Pod
	   14. Now bring down partially ESXi hosts of cluster3
	   15. Try to perform scaleup operation on all deployments . Incrase the replica count from 7 to 10.
	   16. Create few dynamic PVC's with labels added for PV and PVC with "RWX" access mode
	   17. Verify newly created PVC creation status. It should reach to Bound state.
	   18. Stop vSAN-health service.
	   19. Update labels for PV and PVC.
	   20. Start vSAN-health service.
	   21. Create standalone Pods using the newly PVC created
	   22. Verify Pods status. PVC should reach to Bound state and Pods should reach to running state.
	   23. Create StatefulSet with 3 replica
	   24. Bring back all the ESXI hosts of cluster-2 and cluster-3
	   25. Wait for system to be back to normal.
	   26. Verify all workload pods should be in a running ready state after site recovery.
	   26. Perform scale-up/scale-down operation on statefulset.
	   27. Veirfy scaling operation went smooth for statefulset.
	   28. Perform scaledown operation on deployment set. Decrease the replica count to 1.
	   29. Verify CNS metadata for the PVCs for which labels were updated.
	   30. Perform cleanup by deleting StatefulSets, Pods, PVCs and SC.
	*/

	ginkgo.It("Operation storm with workload creation on scale", ginkgo.Label(p1, file, vanilla,
		level5, level2, newTest, disruptive), func() {

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		var pvclaimsNew []*v1.PersistentVolumeClaim
		var fullSyncWaitTime int
		var service *v1.Service
		var depl []*appsv1.Deployment
		var powerOffHostsList []string
		var podList []*v1.Pod
		var statefulset *appsv1.StatefulSet

		stsReplicas := 3
		replica := 3
		createPvcItr := 10
		createDepItr := 1

		if os.Getenv(envFullSyncWaitTime) != "" {
			fullSyncWaitTime, err := strconv.Atoi(os.Getenv(envFullSyncWaitTime))
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			if fullSyncWaitTime <= 0 || fullSyncWaitTime > defaultFullSyncWaitTime {
				framework.Failf("The FullSync Wait time %v is not set correctly", fullSyncWaitTime)
			}
		} else {
			fullSyncWaitTime = defaultFullSyncWaitTime
		}

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
		defer func() {
			for i := 0; i < len(pvclaims); i++ {
				pv := getPvFromClaim(client, pvclaims[i].Namespace, pvclaims[i].Name)
				err = fpv.DeletePersistentVolumeClaim(ctx, client, pvclaims[i].Name, namespace)
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
				err = e2eVSphere.waitForCNSVolumeToBeDeleted(pv.Spec.CSI.VolumeHandle)
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
			}
		}()

		ginkgo.By("Kill CSI-Provisioner container while pvc creation is in progress")
		err = execDockerPauseNKillOnContainer(sshClientConfig, csiProvisionerControlIp, provisionerContainerName,
			k8sVersion)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Verify PVC Bound state and CNS side verification")
		pvs, err := checkVolumeStateAndPerformCnsVerification(ctx, client, pvclaims, "", "")
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Create Deployments with 3 replicas each for each rwx created pvc")
		for i := 0; i < len(pvclaims); i++ {
			deploymentList, _, err := createVerifyAndScaleDeploymentPods(ctx, client, namespace, int32(replica),
				false, labelsMap, pvclaims[i], nil, execRWXCommandPod, nginxImage, false, nil, createDepItr)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			depl = append(depl, deploymentList...)

			if i == 3 {
				csiAttacherLeaderleader, csiAttacherControlIp, err = getK8sMasterNodeIPWhereContainerLeaderIsRunning(ctx,
					client, sshClientConfig, attacherContainerName)
				framework.Logf("CSI-Attacher is running on Leader Pod %s "+
					"which is running on master node %s", csiAttacherLeaderleader, csiAttacherControlIp)
				gomega.Expect(err).NotTo(gomega.HaveOccurred())

				ginkgo.By("Kill CSI-Attacher container")
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

		ginkgo.By("Bring down all esxi hosts in Az2")
		hostsInCluster := getHostsByClusterName(ctx, clusterComputeResource, topologyClusterList[1])
		powerOffHostsList = powerOffEsxiHostByCluster(ctx, &e2eVSphere, topologyClusterList[1],
			len(hostsInCluster))
		defer func() {
			ginkgo.By("Verifying k8s node status after site recovery")
			err = verifyK8sNodeStatusAfterSiteRecovery(client, ctx, sshClientConfig, nodeList)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}()

		ginkgo.By("Scale up deployment set replica count of dep1, dep2 and " +
			"dep3 to 7 when Az2 site is down and in between kill csi attacher")
		replica = 7
		for i := 0; i < 3; i++ {
			_, _, _ = createVerifyAndScaleDeploymentPods(ctx, client, namespace, int32(replica),
				true, labelsMap, pvclaims[i], nil, execRWXCommandPod, nginxImage, true, depl[i], 0)

			if i == 1 {
				ginkgo.By("Kill CSI-Attacher container")
				err = execDockerPauseNKillOnContainer(sshClientConfig, csiAttacherControlIp, attacherContainerName,
					k8sVersion)
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
			}
		}

		ginkgo.By("Bring down few esxi hosts in Az3, partial site down")
		noOfHostToBringDown := 1
		powerOffHostsList1 := powerOffEsxiHostByCluster(ctx, &e2eVSphere, topologyClusterList[2],
			noOfHostToBringDown)
		defer func() {
			ginkgo.By("Bring up  ESXi host which were powered off in Az3")
			powerOnEsxiHostByCluster(powerOffHostsList1[0])
		}()
		powerOffHostsList = append(powerOffHostsList, powerOffHostsList1...)

		ginkgo.By("Scale up all deployment set replica count to 10 when Az2 full site is down and Az3 partial site is down")
		replica = 10
		for i := 0; i < len(depl); i++ {
			_, _, _ = createVerifyAndScaleDeploymentPods(ctx, client, namespace, int32(replica),
				true, labelsMap, pvclaims[i], nil, execRWXCommandPod, nginxImage, true, depl[i], 0)
		}

		ginkgo.By("Create new set of rwx pvcs")
		createPvcItr = 5
		_, pvclaimsNew, err = createStorageClassWithMultiplePVCs(client, namespace, labelsMap,
			scParameters, diskSize, allowedTopologies, bindingModeImm, false, accessmode,
			"", storageclass, createPvcItr, true, false)
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

		ginkgo.By("Verify PVC Bound state and CNS side verification")
		_, err = checkVolumeStateAndPerformCnsVerification(ctx, client, pvclaimsNew, "", "")
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

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
		service, statefulset, _ = createStafeulSetAndVerifyPVAndPodNodeAffinty(ctx, client, namespace, true,
			int32(stsReplicas), false, allowedTopologies, false, false, false, accessmode, storageclass, false, "")

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

		ginkgo.By("Scale down all deployment sets to replica count of 1 and verify cns volume metadata")
		replica = 1
		for i := 0; i < len(depl); i++ {
			_, pods, err = createVerifyAndScaleDeploymentPods(ctx, client, namespace, int32(replica),
				true, labelsMap, pvclaims[i], nil, execRWXCommandPod, nginxImage, true, depl[i], 0)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())

			ginkgo.By("Verify volume metadata for deployment pod, pvc and pv")
			err = waitAndVerifyCnsVolumeMetadata(ctx, pvs[i].Spec.CSI.VolumeHandle, pvclaims[i], pvs[i], &pods.Items[i])
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}
	})
})
