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
	"sync"
	"time"

	"github.com/onsi/ginkgo"
	"github.com/onsi/gomega"
	"golang.org/x/crypto/ssh"
	apps "k8s.io/api/apps/v1"
	appsv1 "k8s.io/api/apps/v1"
	v1 "k8s.io/api/core/v1"
	storagev1 "k8s.io/api/storage/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	clientset "k8s.io/client-go/kubernetes"
	"k8s.io/kubernetes/test/e2e/framework"
	fnodes "k8s.io/kubernetes/test/e2e/framework/node"
	fpod "k8s.io/kubernetes/test/e2e/framework/pod"
	fpv "k8s.io/kubernetes/test/e2e/framework/pv"
	fss "k8s.io/kubernetes/test/e2e/framework/statefulset"
)

var _ = ginkgo.Describe("[csi-topology-vanilla-level5] Topology-Aware-Provisioning-With-Statefulset-Level5", func() {
	f := framework.NewDefaultFramework("e2e-vsphere-topology-aware-provisioning")
	var (
		client                  clientset.Interface
		namespace               string
		bindingMode             storagev1.VolumeBindingMode
		allowedTopologies       []v1.TopologySelectorLabelRequirement
		topologyAffinityDetails map[string][]string
		topologyCategories      []string
		topologyLength          int
	)
	ginkgo.BeforeEach(func() {
		client = f.ClientSet
		namespace = f.Namespace.Name
		bootstrap()
		nodeList, err := fnodes.GetReadySchedulableNodes(f.ClientSet)
		framework.ExpectNoError(err, "Unable to find ready and schedulable Node")
		if !(len(nodeList.Items) > 0) {
			framework.Failf("Unable to find ready and schedulable Node")
		}
		bindingMode = storagev1.VolumeBindingWaitForFirstConsumer
		//topologyLength, leafNode, leafNodeTag0, leafNodeTag1, leafNodeTag2 = 5, 4, 0, 1, 2
		topologyLength = 5

		topologyMap := GetAndExpectStringEnvVar(topologyMap)
		topologyAffinityDetails, topologyCategories = createTopologyMapLevel5(topologyMap, topologyLength)
		allowedTopologies = createAllowedTopolgies(topologyMap, topologyLength)
		fmt.Println(allowedTopologies)
	})

	/*
		TESTCASE-1
		Verify behaviour when CSI Provisioner is deleted during statefulset
		(parallel POD management Policy) creation
		WFC +allowedTopologies (all 5 level - region > zone > building > level > rack)

		Steps//
		1. Identify the Pod where CSI Provisioner is the leader.
		2. Create Storage class with Wait for first consumer.
		3. Create 5 statefulset with parallel POD management policy each with 10 replica's
		using above SC.
		4. While the Statefulsets is creating PVCs and Pods, kill CSI Provisioner container
		identified in the step 1, where CSI provisioner is the leader.
		5. csi-provisioner in other replica should take the leadership to help provisioning
		of the volume.
		6. Wait until all PVCs and Pods are created for Statefulsets.
		7. Expect all PVCs for Statefulsets to be in the bound state.
		8. Verify node affinity details on PV's.
		9. Expect all Pods for Statefulsets to be in the running state.
		10. Describe PV and Verify the node affinity rule. Make sure node affinity
		should contain all 5 levels of topology details.
		11. POD should be running in the appropriate nodes.
		12. Identify the Pod where CSI Attacher is the leader.
		13. Scale down the Statefulsets replica count to 5.
		14. Wait untill the POD count goes down to 5.
		15. Identify the new CSI controller Pod  where CSI Provisioner is the leader.
		16. Scale Down replica count 0.
		17. Delete Stateful set PVC's , During deletion, kill the POD identified in step 13.
		18. All the PVC's and PV's should get deleted. No orphan volumes should be left on the system.
		19. Delete Statefulsets.

	*/

	ginkgo.It("Volume provisioning when CSI Provisioner is deleted during statefulset creation", func() {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		controller_name := "csi-provisioner"
		sshClientConfig := &ssh.ClientConfig{
			User: "root",
			Auth: []ssh.AuthMethod{
				ssh.Password(k8sVmPasswd),
			},
			HostKeyCallback: ssh.InsecureIgnoreHostKey(),
		}
		/* Get allowed topologies for Storage Class
		region1 > zone1 > building1 > level1 > rack > rack1/rack2/rack3 */
		allowedTopologyForSC := getTopologySelector(topologyAffinityDetails, topologyCategories,
			topologyLength)

		// Create SC with WFC BindingMode
		storageclass, err := createStorageClass(client, nil, allowedTopologyForSC, "",
			bindingMode, false, "nginx-sc")
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		defer func() {
			err := client.StorageV1().StorageClasses().Delete(ctx, storageclass.Name, *metav1.NewDeleteOptions(0))
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}()

		ginkgo.By("Creating service")
		service := CreateService(namespace, client)
		defer func() {
			deleteService(namespace, client, service)
		}()

		// Get CSI_Controller_Pod name where CSI_Attacher is the Leader
		// Get K8sMasterNodeIP where CSI_Controller_Pod is running
		k8sMasterIP, csi_controller_pod, err := getK8sMasterNodeIPWhereControllerLeaderIsRunning(ctx,
			client, sshClientConfig, controller_name)
		fmt.Println(csi_controller_pod)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		// Execute Docker Pause and Kill cmd on K8sMasterNodeIP identified in above step
		err = executeDockerPauseKillCmd(sshClientConfig, k8sMasterIP, controller_name)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		// Get Parallel StatefulSets Specs for creation of StatefulSets
		statefulSets := createParallelStatefulSetSpec(namespace, 3)

		// Trigger 3 StatefulSets in parallel
		var wg sync.WaitGroup
		wg.Add(3)
		var statefulSetReplicaCount int32 = 3
		for i := 0; i < len(statefulSets); i++ {
			go createParallelStatefulSets(client, namespace, statefulSets[i],
				statefulSetReplicaCount, &wg)
		}
		wg.Wait()

		/* Verify PV nde affinity and that the pods are running on appropriate nodes
		for each StatefulSet pod */
		ginkgo.By("Verify PV node affinity and that the PODS are running on appropriate node")
		for i := 0; i < len(statefulSets); i++ {
			verifyPVnodeAffinityAndPODnodedetailsForStatefulsetsLevel5(ctx, client,
				statefulSets[i], namespace, allowedTopologies)
		}

		// Scale down statefulSets replicas count
		statefulSetReplicaCount = 5
		ginkgo.By("Scale down statefulset replica count")
		for i := 0; i < len(statefulSets); i++ {
			scaleDownStatefulSetPod(ctx, client, statefulSets[i], namespace, statefulSetReplicaCount)
		}

		k8sMasterIP, csi_controller_pod, err = getK8sMasterNodeIPWhereControllerLeaderIsRunning(ctx, client,
			sshClientConfig, controller_name)
		fmt.Println(csi_controller_pod)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		err = executeDockerPauseKillCmd(sshClientConfig, k8sMasterIP, controller_name)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		// Scale down statefulSets replica count to 0
		statefulSetReplicaCount = 0
		ginkgo.By("Scale down statefulset replica count to 0")
		for i := 0; i < len(statefulSets); i++ {
			scaleDownStatefulSetPod(ctx, client, statefulSets[i], namespace, statefulSetReplicaCount)
		}
	})

	/*
		TESTCASE-2
		Verify behaviour when CSI Attacher is deleted during statefulset
		(parallel POD management Policy) creation
		WFC + allowedTopologies (all 5 level)

		Steps//
		1. Identify the Pod where CSI attacher is the leader.
		2. Create Storage class with Wait for first consumer.
		3. Create 5 statefulsets with parallel POD management policy each with 10 replica's.
		4. While the Statefulsets is creating PVCs and Pods, delete the CSI controller Pod
		identified in the step 1, where CSI Attacher is the leader.
		5. csi-provisioner in other replica should take the leadership to help provisioning of the volume.
		6. Wait until all PVCs and Pods are created for Statefulsets.
		7. Expect all PVCs for Statefulsets to be in the bound state.
		8. Verify node affinity details on PV's
		9. Expect all Pods for Statefulsets to be in the running state
		10. Describe PV and Verify the node affinity rule. Make sure node affinity
		should contain all 5 levels of topology details
		11. POD should be running in the appropriate nodes
		12. Scale down the Statefulsets replica count to 5 ,
		During scale down delete CSI controller POD identified in step 10
		13. Wait untill the POD count goes down to 5
		14. csi-Attacher in other replica should take the leadership to detach Volume
		15. Delete Statefulsets and Delete PVCs.
	*/

	ginkgo.It("Volume provisioning when CSI Attacher is deleted during statefulset creation", func() {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		controller_name := "csi-attacher"
		ignoreLabels := make(map[string]string)
		sshClientConfig := &ssh.ClientConfig{
			User: "root",
			Auth: []ssh.AuthMethod{
				ssh.Password(k8sVmPasswd),
			},
			HostKeyCallback: ssh.InsecureIgnoreHostKey(),
		}
		/* Get allowed topologies for Storage Class
		region1 > zone1 > building1 > level1 > rack > rack1/rack2/rack3 */
		allowedTopologyForSC := getTopologySelector(topologyAffinityDetails, topologyCategories,
			topologyLength)

		// Create SC with WFC Binding mode
		storageclass, err := createStorageClass(client, nil, allowedTopologyForSC,
			"", bindingMode, false, "nginx-sc")
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		defer func() {
			err := client.StorageV1().StorageClasses().Delete(ctx, storageclass.Name, *metav1.NewDeleteOptions(0))
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}()

		ginkgo.By("Creating service")
		service := CreateService(namespace, client)
		defer func() {
			deleteService(namespace, client, service)
		}()

		// Get CSI_Controller_Pod name where CSI_Attacher is the Leader
		// Get K8sMasterNodeIP where CSI_Controller_Pod is running
		k8sMasterIP, csi_controller_pod, err := getK8sMasterNodeIPWhereControllerLeaderIsRunning(ctx, client,
			sshClientConfig, controller_name)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		fmt.Println(k8sMasterIP)

		// Get Parallel StatefulSets Specs for creation of StatefulSets
		statefulSets := createParallelStatefulSetSpec(namespace, 3)

		// Trigger 3 StatefulSets in parallel
		var wg sync.WaitGroup
		wg.Add(3)
		var statefulSetReplicaCount int32 = 3
		for i := 0; i < len(statefulSets); i++ {
			if i == 2 {
				_ = updateDeploymentReplica(client, 1, csi_controller_pod, csiSystemNamespace)
			}
			go createParallelStatefulSets(client, namespace, statefulSets[i],
				statefulSetReplicaCount, &wg)
		}
		wg.Wait()

		time.Sleep(5 * time.Second)

		// Verify and wait for the Pods to be in up and running state
		for i := 0; i < len(statefulSets); i++ {
			fss.WaitForStatusReadyReplicas(client, statefulSets[i], statefulSetReplicaCount)
			gomega.Expect(fss.CheckMount(client, statefulSets[i], mountPath)).NotTo(gomega.HaveOccurred())
			err = fpod.WaitForPodsRunningReady(client, namespace, int32(5), 0, pollTimeout, ignoreLabels)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			ssPodsBeforeScaleDown := fss.GetPodList(client, statefulSets[i])
			gomega.Expect(ssPodsBeforeScaleDown.Items).NotTo(gomega.BeEmpty(),
				fmt.Sprintf("Unable to get list of Pods from the Statefulset: %v", statefulSets[i].Name))
			gomega.Expect(len(ssPodsBeforeScaleDown.Items) == int(statefulSetReplicaCount)).To(gomega.BeTrue(),
				"Number of Pods in the statefulset should match with number of replicas")
		}

		/* Verify PV nde affinity and that the pods are running on appropriate nodes
		for each StatefulSet pod */
		ginkgo.By("Verify PV node affinity and that the PODS are running on appropriate node")
		for i := 0; i < len(statefulSets); i++ {
			verifyPVnodeAffinityAndPODnodedetailsForStatefulsetsLevel5(ctx, client,
				statefulSets[i], namespace, allowedTopologies)
		}

		// Scale down StatefulSets replicas count to to 5
		statefulSetReplicaCount = 5
		ginkgo.By("Scale down statefulset replica count to 0")
		for i := 0; i < len(statefulSets); i++ {
			scaleDownStatefulSetPod(ctx, client, statefulSets[i], namespace, statefulSetReplicaCount)
			if i == 1 {
				_ = updateDeploymentReplica(client, 1, csi_controller_pod, csiSystemNamespace)
			}
		}

		// Get CSI_Controller_Pod name where CSI_Attacher is the Leader
		// Get K8sMasterNodeIP where CSI_Controller_Pod is running
		k8sMasterIP, csi_controller_pod, err = getK8sMasterNodeIPWhereControllerLeaderIsRunning(ctx, client,
			sshClientConfig, controller_name)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		fmt.Println(k8sMasterIP)
		fmt.Println(csi_controller_pod)

		// Scale down StatefulSets replicas count to 0
		statefulSetReplicaCount = 0
		ginkgo.By("Scale down statefulset replica count to 0")
		for i := 0; i < len(statefulSets); i++ {
			scaleDownStatefulSetPod(ctx, client, statefulSets[i], namespace, statefulSetReplicaCount)
		}
	})

	/*
		TESTCASE -3
		Verify the behaviour when node daemon set restarts
		WFC + allowedTopologies (all 5 level)

		Steps//
		1. Identify the Pod where CSI Attacher is the leader.
		2. Create Storage class with Wait for first consumer.
		3. Create 5 statefulset with parallel POD management policy each with 5 replica's
		using above SC.
		4. While the Statefulsets is creating PVCs and Pods, delete the CSI controller Pod
		identified in the step 1, where CSI Attacher is the leader.
		5. Csi-Attacher in other replica should take the leadership to help provisioning
		of the volume.
		6. Wait until all PVCs and Pods are created for Statefulsets
		7. Expect all PVCs for Statefulsets to be in the bound state.
		8. Verify node affinity details on PV's
		9. Expect all Pods for Statefulsets to be in the running state.
		10. Describe PV and Verify the node affinity rule.
		Make sure node affinity should contain all 5 levels of topology details.
		11. POD should be running in the appropriate nodes.
		12. Restart node daemon set.
		13. Scale up the statefulset to 10 replicas and wait for all the POD's to reach running state.
		14. Describe PV and Verify the node affinity rule. Make sure node affinity
		should contain all 5 levels of topology details
		15. POD should be running in the appropriate nodes
		16. Scale down the Statefulsets replica count to 0. Wait for some time for POD to get Terminated.
		17. Delete the statefulset
		18. Delete Statefulsets and Delete PVCs.
	*/

	ginkgo.It("Volume provisioning when node daemonset restarts during statefulset creation", func() {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		controller_name := "csi-attacher"
		ignoreLabels := make(map[string]string)
		sshClientConfig := &ssh.ClientConfig{
			User: "root",
			Auth: []ssh.AuthMethod{
				ssh.Password(k8sVmPasswd),
			},
			HostKeyCallback: ssh.InsecureIgnoreHostKey(),
		}
		/* Get allowed topologies for Storage Class
		region1 > zone1 > building1 > level1 > rack > rack1/rack2/rack3 */
		allowedTopologyForSC := getTopologySelector(topologyAffinityDetails, topologyCategories,
			topologyLength)

		// Create SC with WFC Binding Mode
		storageclass, err := createStorageClass(client, nil, allowedTopologyForSC,
			"", bindingMode, false, "nginx-sc")
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		defer func() {
			err := client.StorageV1().StorageClasses().Delete(ctx, storageclass.Name, *metav1.NewDeleteOptions(0))
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}()

		ginkgo.By("Creating service")
		service := CreateService(namespace, client)
		defer func() {
			deleteService(namespace, client, service)
		}()

		// Get CSI_Controller_Pod name where CSI_Attacher is the Leader
		// Get K8sMasterNodeIP where CSI_Controller_Pod is running
		k8sMasterIP, csi_controller_pod, err := getK8sMasterNodeIPWhereControllerLeaderIsRunning(ctx, client,
			sshClientConfig, controller_name)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		fmt.Println(k8sMasterIP)

		// Get Parallel StatefulSets Specs for creation of StatefulSets
		statefulSets := createParallelStatefulSetSpec(namespace, 3)

		/* Trigger 3 StatefulSets in parallel and in between kill CSI Controller Pod
		where CSI-Attacher is a leader */
		var wg sync.WaitGroup
		wg.Add(3)
		var statefulSetReplicaCount int32 = 3
		for i := 0; i < len(statefulSets); i++ {
			if i == 2 {
				deployment := updateDeploymentReplica(client, 1, csi_controller_pod, csiSystemNamespace)
				fmt.Println(deployment)
			}
			go createParallelStatefulSets(client, namespace, statefulSets[i],
				statefulSetReplicaCount, &wg)
		}
		wg.Wait()

		// Get CSI_Controller_Pod name where CSI_Attacher is the Leader
		// Get K8sMasterNodeIP where CSI_Controller_Pod is running
		k8sMasterIP, csi_controller_pod, err = getK8sMasterNodeIPWhereControllerLeaderIsRunning(ctx, client,
			sshClientConfig, controller_name)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		fmt.Println(k8sMasterIP)
		fmt.Println(csi_controller_pod)

		time.Sleep(5 * time.Second)

		// Wait for StatefulSets Pods to be in up and running state
		for i := 0; i < len(statefulSets); i++ {
			fss.WaitForStatusReadyReplicas(client, statefulSets[i], statefulSetReplicaCount)
			gomega.Expect(fss.CheckMount(client, statefulSets[i], mountPath)).NotTo(gomega.HaveOccurred())
			err = fpod.WaitForPodsRunningReady(client, namespace, int32(5), 0, pollTimeout, ignoreLabels)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			ssPodsBeforeScaleDown := fss.GetPodList(client, statefulSets[i])
			gomega.Expect(ssPodsBeforeScaleDown.Items).NotTo(gomega.BeEmpty(),
				fmt.Sprintf("Unable to get list of Pods from the Statefulset: %v", statefulSets[i].Name))
			gomega.Expect(len(ssPodsBeforeScaleDown.Items) == int(statefulSetReplicaCount)).To(gomega.BeTrue(),
				"Number of Pods in the statefulset should match with number of replicas")
		}

		/* Verify PV nde affinity and that the pods are running on appropriate nodes
		for each StatefulSet pod */
		ginkgo.By("Verify PV node affinity and that the PODS are running on appropriate node")
		for i := 0; i < len(statefulSets); i++ {
			verifyPVnodeAffinityAndPODnodedetailsForStatefulsetsLevel5(ctx, client,
				statefulSets[i], namespace, allowedTopologies)
		}

		// Fetch the number of CSI pods running before restart
		list_of_pods, err := fpod.GetPodsInNamespace(client, csiSystemNamespace, ignoreLabels)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		// Restart CSI daemonset
		cmd := []string{"rollout", "restart", "daemonset/vsphere-csi-node", "--namespace=" + csiSystemNamespace}
		framework.RunKubectlOrDie(csiSystemNamespace, cmd...)

		// Wait for the CSI Pods to be up and Running
		num_csi_pods := len(list_of_pods)
		err = fpod.WaitForPodsRunningReady(client, csiSystemNamespace, int32(num_csi_pods), 0, pollTimeout, ignoreLabels)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		// Scale up statefulSets replicas count
		statefulSetReplicaCount += 5
		for i := 0; i < len(statefulSets); i++ {
			scaleUpStatefulSetPod(ctx, client, statefulSets[i], namespace, statefulSetReplicaCount)
		}

		/* Verify PV nde affinity and that the pods are running on appropriate nodes
		for each StatefulSet pod */
		ginkgo.By("Verify PV node affinity and that the PODS are running on appropriate node")
		for i := 0; i < len(statefulSets); i++ {
			verifyPVnodeAffinityAndPODnodedetailsForStatefulsetsLevel5(ctx, client,
				statefulSets[i], namespace, allowedTopologies)
		}

		// Scale down statefulset to 0 replicas
		statefulSetReplicaCount = 0
		ginkgo.By("Scale down statefulset replica count to 0")
		for i := 0; i < len(statefulSets); i++ {
			scaleDownStatefulSetPod(ctx, client, statefulSets[i], namespace, statefulSetReplicaCount)
		}
	})

	/*
		TESTCASE-7
		Verify behaviour when CSI Attacher is deleted during deployments POD creation

		Steps//
		1. Identify the Pod where CSI Attacher is the leader.
		2. Create SC with Immediate Binding modee.
		3. Create multiple PVC's using above SC.
		4. Verify node affinity details on PV's.
		5. Create multiple Deployments using above created PVC's with replica 1.
		6. While the deployments are creating Pods, delete the CSI controller
		Pod identified in the step 1, where CSI Attacher is the leader.
		7. Csi-Attacher in other replica should take the leadership to help creating the PODs.
		8. Wait until all Pods are created for Deployments.
		9. Expect all Pods for deployments to be in the running state.
		10. Delete deployments and Delete PVCs.
	*/

	ginkgo.It("Verify behaviour when CSI Attacher is deleted during deployments pod creation", func() {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		controller_name := "csi-attacher"
		var lables = make(map[string]string)
		lables["app"] = "nginx"
		//replica := 1
		sshClientConfig := &ssh.ClientConfig{
			User: "root",
			Auth: []ssh.AuthMethod{
				ssh.Password(k8sVmPasswd),
			},
			HostKeyCallback: ssh.InsecureIgnoreHostKey(),
		}

		// Identify the CSI_Controller_Pod name where CSI_Attacher is the Leader
		// Identify K8sMasterNodeIP where CSI_Controller_Pod is running
		csi_controller_pod, k8sMasterIP, err := getK8sMasterNodeIPWhereControllerLeaderIsRunning(ctx,
			client, sshClientConfig, controller_name)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		fmt.Println(k8sMasterIP)

		/* Get allowed topologies for Storage Class
		region1 > zone1 > building1 > level1 > rack > rack1/rack2/rack3 */
		allowedTopologyForSC := getTopologySelector(topologyAffinityDetails, topologyCategories,
			topologyLength)

		// Create SC with Immediate Binding Mode
		storageclass, err := createStorageClass(client, nil, allowedTopologyForSC,
			"", "", false, "")
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		defer func() {
			err := client.StorageV1().StorageClasses().Delete(ctx, storageclass.Name,
				*metav1.NewDeleteOptions(0))
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}()

		var pvcCount int = 3
		pvclaimsList := createMultiplePVCsInParallel(client, namespace, storageclass, pvcCount)

		for i := 0; i < len(pvclaimsList); i++ {
			var pvclaims []*v1.PersistentVolumeClaim
			pvclaims = append(pvclaims, pvclaimsList[i])
			_, err := fpv.WaitForPVClaimBoundPhase(client, pvclaims,
				framework.ClaimProvisionTimeout)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())

			ginkgo.By("Create Deployments")
			deployment, err := createDeployment(ctx, client, 1, lables,
				nil, namespace, pvclaims, "", false, nginxImage)
			fmt.Println(deployment)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			if i == 2 {
				deployment := updateDeploymentReplica(client, 2, vSphereCSIControllerPodNamePrefix,
					csiSystemNamespace)
				fmt.Println(deployment)
			}
		}

		k8sMasterIP, csi_controller_pod, err = getK8sMasterNodeIPWhereControllerLeaderIsRunning(ctx, client,
			sshClientConfig, controller_name)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		fmt.Println(k8sMasterIP)
		fmt.Println(csi_controller_pod)
	})
})

func createParallelStatefulSets(client clientset.Interface, namespace string,
	statefulset *appsv1.StatefulSet, replicas int32, wg *sync.WaitGroup) {
	defer wg.Done()
	ginkgo.By("Creating statefulset")
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	framework.Logf(fmt.Sprintf("Creating statefulset %v/%v with %d replicas and selector %+v",
		statefulset.Namespace, statefulset.Name, *(statefulset.Spec.Replicas), statefulset.Spec.Selector))
	_, err := client.AppsV1().StatefulSets(namespace).Create(ctx, statefulset, metav1.CreateOptions{})
	framework.ExpectNoError(err)
}

func createParallelStatefulSetSpec(namespace string, no_of_sts int) []*apps.StatefulSet {
	stss := []*apps.StatefulSet{}
	var statefulset *apps.StatefulSet
	for i := 0; i < no_of_sts; i++ {
		statefulset = GetStatefulSetFromManifest(namespace)
		statefulset.Name = "thread-" + strconv.Itoa(i) + "-" + statefulset.Name
		statefulset.Spec.PodManagementPolicy = apps.ParallelPodManagement
		statefulset.Spec.VolumeClaimTemplates[len(statefulset.Spec.VolumeClaimTemplates)-1].
			Annotations["volume.beta.kubernetes.io/storage-class"] = "nginx-sc"
		stss = append(stss, statefulset)
	}
	return stss
}

func createMultiplePVCsInParallel(client clientset.Interface, namespace string,
	storageclass *storagev1.StorageClass, count int) []*v1.PersistentVolumeClaim {
	var pvclaims []*v1.PersistentVolumeClaim
	for i := 0; i < count; i++ {
		pvclaim, err := createPVC(client, namespace, nil, "", storageclass, "")
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		pvclaims = append(pvclaims, pvclaim)
	}
	return pvclaims
}

func deleteCsiControllerPodWhereLeaderIsRunning(ctx context.Context,
	client clientset.Interface, sshClientConfig *ssh.ClientConfig,
	controller_name string) (string, string, error) {
	ignoreLabels := make(map[string]string)
	var k8sMasterNodeIP string
	var csi_controller_pod string
	list_of_pods, err := fpod.GetPodsInNamespace(client, csiSystemNamespace, ignoreLabels)
	gomega.Expect(err).NotTo(gomega.HaveOccurred())
	for i := 0; i < len(list_of_pods); i++ {
		if strings.Contains(list_of_pods[i].Name, vSphereCSIControllerPodNamePrefix) {
			k8sMasterIPs := getK8sMasterIPs(ctx, client)
			for _, k8sMasterIP := range k8sMasterIPs {
				grepCmdForFindingCurrentLeader := "kubectl logs " + list_of_pods[i].Name + " -n " +
					"vmware-system-csi " + controller_name + " | grep 'new leader detected, current leader:' " +
					" | tail -1 | awk '{print $10}' | tr -d '\n'"
				framework.Logf("Invoking command '%v' on host %v", grepCmdForFindingCurrentLeader,
					k8sMasterIP)
				RunningLeaderInfo, err := sshExec(sshClientConfig, k8sMasterIP,
					grepCmdForFindingCurrentLeader)
				csi_controller_pod = RunningLeaderInfo.Stdout
				if err != nil || RunningLeaderInfo.Code != 0 {
					fssh.LogResult(RunningLeaderInfo)
					return "", "", fmt.Errorf("couldn't execute command: %s on host: %v , error: %s",
						grepCmdForFindingCurrentLeader, k8sMasterIP, err)
				}
	
}
