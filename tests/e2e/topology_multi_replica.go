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

var _ = ginkgo.Describe("[csi-topology-vanilla-level5] Topology-Aware-Provisioning-With-MultiReplica-Statefulset-Level5",
	func() {
		f := framework.NewDefaultFramework("e2e-vsphere-topology-aware-provisioning")
		var (
			client                  clientset.Interface
			namespace               string
			bindingMode             storagev1.VolumeBindingMode
			allowedTopologies       []v1.TopologySelectorLabelRequirement
			topologyAffinityDetails map[string][]string
			topologyCategories      []string
			topologyLength          int
			isSPSServiceStopped     bool
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
			topologyLength = 5
			isSPSServiceStopped = false

			topologyMap := GetAndExpectStringEnvVar(topologyMap)
			topologyAffinityDetails, topologyCategories = createTopologyMapLevel5(topologyMap, topologyLength)
			allowedTopologies = createAllowedTopolgies(topologyMap, topologyLength)
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
			12. Identify the Pod where CSI Attacher is the leader
			13. Scale down the Statefulsets replica count to 5,
			14. While the Statefulsets is scaling down Pods, kill CSI Attacher container
			identified in the step 11, where CSI Attacher is the leader.
			15. Wait untill the POD count goes down to 5
			16. Identify the new CSI controller Pod  where CSI Provisioner is the leader.
			17. Scale Down replica count 0
			18. Delete PVCs, Statefulsets, SC

		*/

		ginkgo.It("Volume provisioning when CSI Provisioner is deleted during statefulset creation", func() {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			var controller_name string = "csi-provisioner"
			var sts_count int = 3
			var statefulSetReplicaCount int32 = 3
			ignoreLabels := make(map[string]string)
			sshClientConfig := &ssh.ClientConfig{
				User: "root",
				Auth: []ssh.AuthMethod{
					ssh.Password(k8sVmPasswd),
				},
				HostKeyCallback: ssh.InsecureIgnoreHostKey(),
			}

			/* Get current leader Csi-Controller-Pod where CSI Provisioner is running and " +
			find the master node IP where this Csi-Controller-Pod is running */
			ginkgo.By("Get current eader Csi-Controller-Pod where CSI Provisioner is running and " +
				"find the master node IP where this Csi-Controller-Pod is running")
			csi_controller_pod, k8sMasterIP, err := getK8sMasterNodeIPWhereControllerLeaderIsRunning(ctx,
				client, sshClientConfig, controller_name)
			framework.Logf("CSI-Provisioner is running on Leader Pod %s "+
				"which is running on master node %s", csi_controller_pod, k8sMasterIP)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())

			/* Get allowed topologies for Storage Class
			region1 > zone1 > building1 > level1 > rack > rack1/rack2/rack3 */
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

			// Create Multiple StatefulSets Specs for creation of StatefulSets
			ginkgo.By("Creating Multiple StatefulSets Specs")
			statefulSets := createParallelStatefulSetSpec(namespace, sts_count)

			/* Trigger multiple StatefulSets creation in parallel. During StatefulSets
			creation, in between kill CSI Provisioner container */
			ginkgo.By("Trigger multiple StatefulSets creation in parallel. During StatefulSets " +
				"creation, in between kill CSI Provisioner container")
			var wg sync.WaitGroup
			wg.Add(3)
			for i := 0; i < len(statefulSets); i++ {
				go createParallelStatefulSets(client, namespace, statefulSets[i],
					statefulSetReplicaCount, &wg)
				if i == 1 {
					/* Execute Docker container Pause and Docker container Kill cmd on the master node
					where Leader CSi-Controller-Pod is running */
					ginkgo.By("Execute Docker container Pause and Docker container Kill cmd on the master " +
						"node where Leader CSi-Controller-Pod is running")
					err = executeDockerPauseKillCmd(sshClientConfig, k8sMasterIP, controller_name)
					gomega.Expect(err).NotTo(gomega.HaveOccurred())
				}
			}
			wg.Wait()

			/* Get new current leader Csi-Controller-Pod where CSI Provisioner is running" +
			find new master node IP where this Csi-Controller-Pod is running */
			ginkgo.By("Get new current Leader Csi-Controller-Pod where CSI Provisioner is running and " +
				"find the master node IP where this Csi-Controller-Pod is running")
			csi_controller_pod, k8sMasterIP, err = getK8sMasterNodeIPWhereControllerLeaderIsRunning(ctx,
				client, sshClientConfig, controller_name)
			framework.Logf("CSI-Provisioner is running on newly elected Leader Pod %s "+
				"which is running on master node %s", csi_controller_pod, k8sMasterIP)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())

			// Waiting for StatefulSets Pods to be in Ready State
			ginkgo.By("Waiting for StatefulSets Pods to be in Ready State")
			time.Sleep(5 * time.Second)

			// Verify that all multiple StatefulSets Pods creation should be in up and running state
			ginkgo.By("Verify that all multiple StatefulSets Pods creation should be in up and running state")
			for i := 0; i < len(statefulSets); i++ {
				// verify that the StatefulSets pods are in ready state
				fss.WaitForStatusReadyReplicas(client, statefulSets[i], statefulSetReplicaCount)
				gomega.Expect(fss.CheckMount(client, statefulSets[i], mountPath)).NotTo(gomega.HaveOccurred())

				// verify that the StatefulSets pods are in running state
				err = fpod.WaitForPodsRunningReady(client, namespace, int32(5), 0, pollTimeout, ignoreLabels)
				gomega.Expect(err).NotTo(gomega.HaveOccurred())

				// verify StatefulSets replica count
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

			/* Get current leader Csi-Controller-Pod where CSI Attacher is running" +
			find new master node IP where this Csi-Controller-Pod is running */
			ginkgo.By("Get current Leader Csi-Controller-Pod where CSI Attacher is running and " +
				"find the master node IP where this Csi-Controller-Pod is running")
			controller_name = "csi-attacher"
			csi_controller_pod, k8sMasterIP, err = getK8sMasterNodeIPWhereControllerLeaderIsRunning(ctx,
				client, sshClientConfig, controller_name)
			framework.Logf("CSI-Attacher is running on elected Leader Pod %s "+
				"which is running on master node %s", csi_controller_pod, k8sMasterIP)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())

			// Scale down statefulSets replicas count
			ginkgo.By("Scaledown StatefulSets replica count in parallel. During StatefulSets replica scaledown " +
				"in between kill CSI Attacher container")
			statefulSetReplicaCount = 2
			ginkgo.By("Scale down statefulset replica count")
			for i := 0; i < len(statefulSets); i++ {
				scaleDownStatefulSetPod(ctx, client, statefulSets[i], namespace, statefulSetReplicaCount)
				if i == 1 {
					/* Execute Docker container Pause and Docker container Kill cmd on the master node
					where Leader CSi-Controller-Pod is running */
					ginkgo.By("Execute Docker container Pause and Docker container Kill cmd on the master " +
						"node where Leader CSi-Controller-Pod is running")
					err = executeDockerPauseKillCmd(sshClientConfig, k8sMasterIP, controller_name)
					gomega.Expect(err).NotTo(gomega.HaveOccurred())
				}
			}

			/* Get new current leader Csi-Controller-Pod where CSI Provisioner is running" +
			find new master node IP where this Csi-Controller-Pod is running */
			ginkgo.By("Get current Leader Csi-Controller-Pod where CSI Provisioner is running and " +
				"find the master node IP where this Csi-Controller-Pod is running")
			controller_name = "csi-provisioner"
			csi_controller_pod, k8sMasterIP, err = getK8sMasterNodeIPWhereControllerLeaderIsRunning(ctx,
				client, sshClientConfig, controller_name)
			framework.Logf("CSI-Provisioner is running on elected Leader Pod %s "+
				"which is running on master node %s", csi_controller_pod, k8sMasterIP)
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
			var sts_count int = 3
			var statefulSetReplicaCount int32 = 3
			sshClientConfig := &ssh.ClientConfig{
				User: "root",
				Auth: []ssh.AuthMethod{
					ssh.Password(k8sVmPasswd),
				},
				HostKeyCallback: ssh.InsecureIgnoreHostKey(),
			}

			/* Get current leader Csi-Controller-Pod where CSI Attacher is running" +
			find master node IP where this Csi-Controller-Pod is running */
			ginkgo.By("Get current Leader Csi-Controller-Pod where CSI Attacher is running and " +
				"find the master node IP where this Csi-Controller-Pod is running")
			csi_controller_pod, k8sMasterIP, err := getK8sMasterNodeIPWhereControllerLeaderIsRunning(ctx,
				client, sshClientConfig, controller_name)
			framework.Logf("CSI-Attacher is running on elected Leader Pod %s "+
				"which is running on master node %s", csi_controller_pod, k8sMasterIP)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())

			/* Get allowed topologies for Storage Class
			region1 > zone1 > building1 > level1 > rack > rack1/rack2/rack3 */
			allowedTopologyForSC := getTopologySelector(topologyAffinityDetails, topologyCategories,
				topologyLength)

			// Create SC with WFC Binding mode
			ginkgo.By("Creating Storage Class with WFC Binding mode")
			storageclass, err := createStorageClass(client, nil, allowedTopologyForSC,
				"", bindingMode, false, "nginx-sc")
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			defer func() {
				err := client.StorageV1().StorageClasses().Delete(ctx, storageclass.Name,
					*metav1.NewDeleteOptions(0))
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
			}()

			// Creating Service for StatefulSet
			ginkgo.By("Creating service")
			service := CreateService(namespace, client)
			defer func() {
				deleteService(namespace, client, service)
			}()

			// Create Multiple StatefulSets Specs for creation of StatefulSets
			ginkgo.By("Creating Multiple StatefulSets Specs")
			statefulSets := createParallelStatefulSetSpec(namespace, sts_count)

			/* Trigger multiple StatefulSets creation in parallel. During StatefulSets
			creation, in between delete elected leader Csi-Controller-Pod where CSI-Attacher is running */
			ginkgo.By("Trigger multiple StatefulSets creation in parallel. During StatefulSets " +
				"creation, in between delete elected leader Csi-Controller-Pod where CSI-Attacher " +
				"is running")
			var wg sync.WaitGroup
			wg.Add(3)
			for i := 0; i < len(statefulSets); i++ {
				go createParallelStatefulSets(client, namespace, statefulSets[i],
					statefulSetReplicaCount, &wg)
				if i == 1 {
					/* Delete elected leader CSi-Controller-Pod where CSI-Attacher is running */
					ginkgo.By("Delete elected leader CSi-Controller-Pod where CSI-Attacher is running")
					err = deleteCsiControllerPodWhereLeaderIsRunning(ctx, client, sshClientConfig,
						csi_controller_pod)
					gomega.Expect(err).NotTo(gomega.HaveOccurred())
				}
			}
			wg.Wait()

			/* Get newly current leader Csi-Controller-Pod where CSI Attacher is running" +
			find master node IP where this Csi-Controller-Pod is running */
			ginkgo.By("Get newly elected current Leader Csi-Controller-Pod where CSI Attacher is running and " +
				"find the master node IP where this Csi-Controller-Pod is running")
			csi_controller_pod, k8sMasterIP, err = getK8sMasterNodeIPWhereControllerLeaderIsRunning(ctx,
				client, sshClientConfig, controller_name)
			framework.Logf("CSI-Attacher is running on newly elected Leader Pod %s "+
				"which is running on master node %s", csi_controller_pod, k8sMasterIP)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())

			// Waiting for StatefulSets Pods to be in Ready State
			ginkgo.By("Waiting for StatefulSets Pods to be in Ready State")
			time.Sleep(5 * time.Second)

			// Verify that all multiple StatefulSets Pods creation should be in up and running state
			ginkgo.By("Verify that all multiple StatefulSets Pods creation should be in up and running state")
			for i := 0; i < len(statefulSets); i++ {
				// verify that the StatefulSets pods are in ready state
				fss.WaitForStatusReadyReplicas(client, statefulSets[i], statefulSetReplicaCount)
				gomega.Expect(fss.CheckMount(client, statefulSets[i], mountPath)).NotTo(gomega.HaveOccurred())

				// verify that the StatefulSets pods are in running state
				err = fpod.WaitForPodsRunningReady(client, namespace, int32(5), 0, pollTimeout, ignoreLabels)
				gomega.Expect(err).NotTo(gomega.HaveOccurred())

				// verify StatefulSets replica count
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

			// Scale down StatefulSets replicas count
			statefulSetReplicaCount = 2
			ginkgo.By("Scale down statefulset replica count")
			for i := 0; i < len(statefulSets); i++ {
				scaleDownStatefulSetPod(ctx, client, statefulSets[i], namespace, statefulSetReplicaCount)
				if i == 1 {
					/* Delete newly elected leader CSi-Controller-Pod where CSI-Attacher is running */
					ginkgo.By("Delete elected leader CSi-Controller-Pod where CSI-Attacher is running")
					err = deleteCsiControllerPodWhereLeaderIsRunning(ctx, client, sshClientConfig,
						csi_controller_pod)
					gomega.Expect(err).NotTo(gomega.HaveOccurred())
				}

				/* Get newly elected current leader Csi-Controller-Pod where CSI Attacher is running" +
				find new master node IP where this Csi-Controller-Pod is running */
				ginkgo.By("Get newly elected current Leader Csi-Controller-Pod where CSI Provisioner is " +
					"running and find the master node IP where this Csi-Controller-Pod is running")
				csi_controller_pod, k8sMasterIP, err = getK8sMasterNodeIPWhereControllerLeaderIsRunning(ctx,
					client, sshClientConfig, controller_name)
				framework.Logf("CSI-Attacher is running on elected Leader Pod %s "+
					"which is running on master node %s", csi_controller_pod, k8sMasterIP)
				gomega.Expect(err).NotTo(gomega.HaveOccurred())

				// Scale down statefulSets replica count to 0
				statefulSetReplicaCount = 0
				ginkgo.By("Scale down statefulset replica count to 0")
				for i := 0; i < len(statefulSets); i++ {
					scaleDownStatefulSetPod(ctx, client, statefulSets[i], namespace, statefulSetReplicaCount)
				}
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
			var sts_count int = 3
			var statefulSetReplicaCount int32 = 3
			sshClientConfig := &ssh.ClientConfig{
				User: "root",
				Auth: []ssh.AuthMethod{
					ssh.Password(k8sVmPasswd),
				},
				HostKeyCallback: ssh.InsecureIgnoreHostKey(),
			}

			/* Get current leader Csi-Controller-Pod where CSI Attacher is running" +
			find master node IP where this Csi-Controller-Pod is running */
			ginkgo.By("Get current Leader Csi-Controller-Pod where CSI Attacher is running and " +
				"find the master node IP where this Csi-Controller-Pod is running")
			csi_controller_pod, k8sMasterIP, err := getK8sMasterNodeIPWhereControllerLeaderIsRunning(ctx,
				client, sshClientConfig, controller_name)
			framework.Logf("CSI-Attacher is running on elected Leader Pod %s "+
				"which is running on master node %s", csi_controller_pod, k8sMasterIP)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())

			/* Get allowed topologies for Storage Class
			region1 > zone1 > building1 > level1 > rack > rack1/rack2/rack3 */
			allowedTopologyForSC := getTopologySelector(topologyAffinityDetails, topologyCategories,
				topologyLength)

			// Create SC with WFC Binding mode
			ginkgo.By("Creating Storage Class with WFC Binding mode")
			storageclass, err := createStorageClass(client, nil, allowedTopologyForSC,
				"", bindingMode, false, "nginx-sc")
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			defer func() {
				err := client.StorageV1().StorageClasses().Delete(ctx, storageclass.Name, *metav1.NewDeleteOptions(0))
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
			}()

			// Creating Service for StatefulSet
			ginkgo.By("Creating service")
			service := CreateService(namespace, client)
			defer func() {
				deleteService(namespace, client, service)
			}()

			// Create Multiple StatefulSets Specs for creation of StatefulSets
			ginkgo.By("Creating Multiple StatefulSets Specs")
			statefulSets := createParallelStatefulSetSpec(namespace, sts_count)

			/* Trigger multiple StatefulSets creation in parallel. During StatefulSets
			creation, in between delete elected leader Csi-Controller-Pod where CSI-Attacher is running */
			ginkgo.By("Trigger multiple StatefulSets creation in parallel. During StatefulSets " +
				"creation, in between delete elected leader Csi-Controller-Pod where CSI-Attacher " +
				"is running")
			var wg sync.WaitGroup
			wg.Add(3)
			for i := 0; i < len(statefulSets); i++ {
				go createParallelStatefulSets(client, namespace, statefulSets[i],
					statefulSetReplicaCount, &wg)
				if i == 1 {
					/* Delete elected leader CSi-Controller-Pod where CSI-Attacher is running */
					ginkgo.By("Delete elected leader CSi-Controller-Pod where CSI-Attacher is running")
					err = deleteCsiControllerPodWhereLeaderIsRunning(ctx, client, sshClientConfig,
						csi_controller_pod)
					gomega.Expect(err).NotTo(gomega.HaveOccurred())
				}
			}
			wg.Wait()

			/* Get newly current leader Csi-Controller-Pod where CSI Attacher is running" +
			find master node IP where this Csi-Controller-Pod is running */
			ginkgo.By("Get newly elected current Leader Csi-Controller-Pod where CSI Attacher is running and " +
				"find the master node IP where this Csi-Controller-Pod is running")
			csi_controller_pod, k8sMasterIP, err = getK8sMasterNodeIPWhereControllerLeaderIsRunning(ctx,
				client, sshClientConfig, controller_name)
			framework.Logf("CSI-Attacher is running on newly elected Leader Pod %s "+
				"which is running on master node %s", csi_controller_pod, k8sMasterIP)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())

			// Waiting for StatefulSets Pods to be in Ready State
			ginkgo.By("Waiting for StatefulSets Pods to be in Ready State")
			time.Sleep(5 * time.Second)

			// Verify that all multiple StatefulSets Pods creation should be in up and running state
			ginkgo.By("Verify that all multiple StatefulSets Pods creation should be in up and running state")
			for i := 0; i < len(statefulSets); i++ {
				// verify that the StatefulSets pods are in ready state
				fss.WaitForStatusReadyReplicas(client, statefulSets[i], statefulSetReplicaCount)
				gomega.Expect(fss.CheckMount(client, statefulSets[i], mountPath)).NotTo(gomega.HaveOccurred())

				// verify that the StatefulSets pods are in running state
				err = fpod.WaitForPodsRunningReady(client, namespace, int32(5), 0, pollTimeout, ignoreLabels)
				gomega.Expect(err).NotTo(gomega.HaveOccurred())

				// verify StatefulSets replica count
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
			ginkgo.By("Scale up SttaefulSets replica count in parallel")
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
			var pvcCount int = 3
			var deploymentReplicaCount int32 = 1
			var deploymentList []*appsv1.Deployment
			ignoreLabels := make(map[string]string)
			sshClientConfig := &ssh.ClientConfig{
				User: "root",
				Auth: []ssh.AuthMethod{
					ssh.Password(k8sVmPasswd),
				},
				HostKeyCallback: ssh.InsecureIgnoreHostKey(),
			}

			/* Get current leader Csi-Controller-Pod where CSI Attacher is running" +
			find master node IP where this Csi-Controller-Pod is running */
			ginkgo.By("Get current Leader Csi-Controller-Pod where CSI Attacher is running and " +
				"find the master node IP where this Csi-Controller-Pod is running")
			csi_controller_pod, k8sMasterIP, err := getK8sMasterNodeIPWhereControllerLeaderIsRunning(ctx,
				client, sshClientConfig, controller_name)
			framework.Logf("CSI-Attacher is running on elected Leader Pod %s "+
				"which is running on master node %s", csi_controller_pod, k8sMasterIP)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())

			/* Get allowed topologies for Storage Class
			region1 > zone1 > building1 > level1 > rack > rack1/rack2/rack3 */
			allowedTopologyForSC := getTopologySelector(topologyAffinityDetails, topologyCategories,
				topologyLength)

			// Create SC with Immediate Binding Mode
			ginkgo.By("Creating Storage Class")
			storageclass, err := createStorageClass(client, nil, allowedTopologyForSC,
				"", "", false, "")
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			defer func() {
				err := client.StorageV1().StorageClasses().Delete(ctx, storageclass.Name,
					*metav1.NewDeleteOptions(0))
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
			}()

			// Creating multiple PVCs
			ginkgo.By("Trigger multiple PVCs")
			pvclaimsList := createMultiplePVCsInParallel(client, namespace, storageclass, pvcCount)

			/* Verifying if all PVCs are in Bound phase and trigger Deployment Pods
			for each created PVC.
			During Deployment Pod creation, delete leader Csi-Controller-Pod where
			CSI-Attacher is running
			*/
			ginkgo.By("Verifying if all PVCs are in Bound phase and trigger Deployment Pods with replica count 1" +
				"for each created PVC. During Deployment Pod creation, delete leader Csi-Controller-Pod where " +
				"CSI-Attacher is running")
			for i := 0; i < len(pvclaimsList); i++ {
				// checking if each PVC is in Bound phase
				var pvclaims []*v1.PersistentVolumeClaim
				pvclaims = append(pvclaims, pvclaimsList[i])
				_, err := fpv.WaitForPVClaimBoundPhase(client, pvclaims,
					framework.ClaimProvisionTimeout)
				gomega.Expect(err).NotTo(gomega.HaveOccurred())

				// Triggering Deployment Pod for each created PVC
				deployment, err := createDeployment(ctx, client, deploymentReplicaCount, lables,
					nil, namespace, pvclaims, "", false, nginxImage)
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
				deploymentList = append(deploymentList, deployment)
				gomega.Expect(err).NotTo(gomega.HaveOccurred())

				// Delete elected leader Csi-Controller-Pod where CSi-Attacher is running
				if i == 1 {
					ginkgo.By("Delete elected leader Csi-Controller-Pod where CSi-Attacher is running")
					err = deleteCsiControllerPodWhereLeaderIsRunning(ctx, client,
						sshClientConfig, csi_controller_pod)
					gomega.Expect(err).NotTo(gomega.HaveOccurred())
				}
			}
			//
			defer func() {
				framework.Logf("Delete PVC's")
				for i := 0; i < len(pvclaimsList); i++ {
					err := fpv.DeletePersistentVolumeClaim(client, pvclaimsList[i].Name, namespace)
					gomega.Expect(err).NotTo(gomega.HaveOccurred())
				}
			}()

			defer func() {
				framework.Logf("Delete deployment set")
				for i := 0; i < len(deploymentList); i++ {
					err := client.AppsV1().Deployments(namespace).Delete(ctx, deploymentList[i].Name,
						*metav1.NewDeleteOptions(0))
					gomega.Expect(err).NotTo(gomega.HaveOccurred())
				}
			}()

			/* Get current leader Csi-Controller-Pod where CSI Attacher is running" +
			find master node IP where this Csi-Controller-Pod is running */
			ginkgo.By("Get newly eleted current Leader Csi-Controller-Pod where CSI Attacher is running and " +
				"find the master node IP where this Csi-Controller-Pod is running")
			csi_controller_pod, k8sMasterIP, err = getK8sMasterNodeIPWhereControllerLeaderIsRunning(ctx,
				client, sshClientConfig, controller_name)
			framework.Logf("CSI-Attacher is running on newly elected Leader Pod %s "+
				"which is running on master node %s", csi_controller_pod, k8sMasterIP)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())

			// Verify deployment Pods to be in up and running state
			ginkgo.By("Verify deployment Pods to be in up and running state")
			list_of_pods, err := fpod.GetPodsInNamespace(client, namespace, ignoreLabels)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			num_csi_pods := len(list_of_pods)

			time.Sleep(1 * time.Minute)
			err = fpod.WaitForPodsRunningReady(client, namespace, int32(num_csi_pods), 0,
				pollTimeout, ignoreLabels)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		})

		/*
			Verify the behaviour when SPS service is down along with CSI Provisioner.

			Steps//
			1. Identify the pod where CSI Provisioner is the leader.
			2. Create Storage class with Immediate Binding mode.
			3. Bring down SPS service (service-control --stop sps).
			4. Create around 5 statefulsets with parallel POD management policy each with
			20 replica's using above created SC.
			5. While the Statefulsets is creating PVCs and Pods, delete the CSI controller Pod
			identified in the step 1, where CSI provisioner is the leader.
			6. csi-provisioner in other replica should take the leadership to help provisioning
			of the volume.
			7. Bring up SPS service (service-control --start sps).
			8. Wait until all PVCs and Pods are created for Statefulsets.
			9. Expect all PVCs for Statefulsets to be in the bound state.
			10. Verify node affinity details on PV's.
			11. Expect all Pods for Statefulsets to be in the running state.
			12. Identify the Pod where CSI Attacher is the leader.
			13. Scale down the Statefulsets replica count to 5, During scale down delete
			CSI controller POD.
			14. Wait untill the POD count goes down to 5.
			15. csi-Attacher in other replica should take the leadership to detach Volume.
			16. Delete Statefulsets and Delete PVCs.
		*/

		ginkgo.It("Volume provisioning when sps service is down during statefulset creation", func() {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			controller_name := "csi-provisioner"
			ignoreLabels := make(map[string]string)
			var sts_count int = 3
			var statefulSetReplicaCount int32 = 3
			vcAddress := e2eVSphere.Config.Global.VCenterHostname + ":" + sshdPort
			sshClientConfig := &ssh.ClientConfig{
				User: "root",
				Auth: []ssh.AuthMethod{
					ssh.Password(k8sVmPasswd),
				},
				HostKeyCallback: ssh.InsecureIgnoreHostKey(),
			}

			/* Get current leader Csi-Controller-Pod where CSI Provisioner is running" +
			find master node IP where this Csi-Controller-Pod is running */
			ginkgo.By("Get current Leader Csi-Controller-Pod where CSI Provisioner is running and " +
				"find the master node IP where this Csi-Controller-Pod is running")
			csi_controller_pod, k8sMasterIP, err := getK8sMasterNodeIPWhereControllerLeaderIsRunning(ctx,
				client, sshClientConfig, controller_name)
			framework.Logf("CSI-Provisioner is running on elected Leader Pod %s "+
				"which is running on master node %s", csi_controller_pod, k8sMasterIP)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())

			/* Get allowed topologies for Storage Class
			region1 > zone1 > building1 > level1 > rack > rack1/rack2/rack3 */
			allowedTopologyForSC := getTopologySelector(topologyAffinityDetails, topologyCategories,
				topologyLength)

			// Create SC with Immediate Binding mode
			ginkgo.By("Creating Storage Class with Immediate Binding mode")
			storageclass, err := createStorageClass(client, nil, allowedTopologyForSC,
				"", "", false, "nginx-sc")
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			defer func() {
				err := client.StorageV1().StorageClasses().Delete(ctx, storageclass.Name,
					*metav1.NewDeleteOptions(0))
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
			}()

			// Bring down SPS service
			ginkgo.By("Bring down SPS service")
			isSPSServiceStopped = true
			err = invokeVCenterServiceControl(stopOperation, spsServiceName, vcAddress)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			defer func() {
				if isSPSServiceStopped {
					framework.Logf("Bringing sps up before terminating the test")
					err = invokeVCenterServiceControl(startOperation, spsServiceName, vcAddress)
					gomega.Expect(err).NotTo(gomega.HaveOccurred())
					isSPSServiceStopped = false
				}
			}()

			// Creating Service for StatefulSet
			ginkgo.By("Creating service")
			service := CreateService(namespace, client)
			defer func() {
				deleteService(namespace, client, service)
			}()

			// Create Multiple StatefulSets Specs for creation of StatefulSets
			ginkgo.By("Creating Multiple StatefulSets Specs")
			statefulSets := createParallelStatefulSetSpec(namespace, sts_count)

			/* Trigger multiple StatefulSets creation in parallel. During StatefulSets
			creation, in between delete elected leader Csi-Controller-Pod where CSI-Provisioner is running */
			ginkgo.By("Trigger multiple StatefulSets creation in parallel. During StatefulSets " +
				"creation, in between delete elected leader Csi-Controller-Pod where CSI-Provisioner " +
				"is running")
			var wg sync.WaitGroup
			wg.Add(3)
			for i := 0; i < len(statefulSets); i++ {
				go createParallelStatefulSets(client, namespace, statefulSets[i],
					statefulSetReplicaCount, &wg)
				if i == 1 {
					/* Delete elected leader CSi-Controller-Pod where CSI-Attacher is running */
					ginkgo.By("Delete elected leader CSi-Controller-Pod where CSI-Provisioner is running")
					err = deleteCsiControllerPodWhereLeaderIsRunning(ctx, client, sshClientConfig,
						csi_controller_pod)
					gomega.Expect(err).NotTo(gomega.HaveOccurred())
				}
			}
			wg.Wait()

			/* Get newly current leader Csi-Controller-Pod where CSI Provisioner is running" +
			find master node IP where this Csi-Controller-Pod is running */
			ginkgo.By("Get newly elected current Leader Csi-Controller-Pod where CSI " +
				"Provisioner is running and find the master node IP where " +
				"this Csi-Controller-Pod is running")
			csi_controller_pod, k8sMasterIP, err = getK8sMasterNodeIPWhereControllerLeaderIsRunning(ctx,
				client, sshClientConfig, controller_name)
			framework.Logf("CSI-Provisioner is running on newly elected Leader Pod %s "+
				"which is running on master node %s", csi_controller_pod, k8sMasterIP)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())

			// Bring up SPS service
			if isSPSServiceStopped {
				framework.Logf("Bringing sps up before terminating the test")
				err = invokeVCenterServiceControl(startOperation, spsServiceName, vcAddress)
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
			}

			// Waiting for StatefulSets Pods to be in Ready State
			ginkgo.By("Waiting for StatefulSets Pods to be in Ready State")
			time.Sleep(5 * time.Second)

			// Verify that all multiple StatefulSets Pods creation should be in up and running state
			ginkgo.By("Verify that all multiple StatefulSets Pods creation should be in up and running state")
			for i := 0; i < len(statefulSets); i++ {
				// verify that the StatefulSets pods are in ready state
				fss.WaitForStatusReadyReplicas(client, statefulSets[i], statefulSetReplicaCount)
				gomega.Expect(fss.CheckMount(client, statefulSets[i], mountPath)).NotTo(gomega.HaveOccurred())

				// verify that the StatefulSets pods are in running state
				err = fpod.WaitForPodsRunningReady(client, namespace, int32(5), 0, pollTimeout, ignoreLabels)
				gomega.Expect(err).NotTo(gomega.HaveOccurred())

				// verify StatefulSets replica count
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

			/* Get elected current leader Csi-Controller-Pod where CSI Attacher is running" +
			find new master node IP where this Csi-Controller-Pod is running */
			ginkgo.By("Get elected current Leader Csi-Controller-Pod where CSI Attacher is " +
				"running and find the master node IP where this Csi-Controller-Pod is running")
			controller_name = "csi-attacher"
			csi_controller_pod, k8sMasterIP, err = getK8sMasterNodeIPWhereControllerLeaderIsRunning(ctx,
				client, sshClientConfig, controller_name)
			framework.Logf("CSI-Attacher is running on elected Leader Pod %s "+
				"which is running on master node %s", csi_controller_pod, k8sMasterIP)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())

			// Scale down StatefulSets replicas count
			statefulSetReplicaCount = 2
			ginkgo.By("Scale down statefulset replica count")
			for i := 0; i < len(statefulSets); i++ {
				scaleDownStatefulSetPod(ctx, client, statefulSets[i], namespace, statefulSetReplicaCount)
				if i == 1 {
					/* Delete newly elected leader CSi-Controller-Pod where CSI-Attacher is running */
					ginkgo.By("Delete elected leader CSi-Controller-Pod where CSI-Attacher is running")
					err = deleteCsiControllerPodWhereLeaderIsRunning(ctx, client, sshClientConfig,
						csi_controller_pod)
					gomega.Expect(err).NotTo(gomega.HaveOccurred())
				}

				/* Get newly elected current leader Csi-Controller-Pod where CSI Attacher is running" +
				find new master node IP where this Csi-Controller-Pod is running */
				ginkgo.By("Get newly elected current Leader Csi-Controller-Pod where CSI Provisioner is " +
					"running and find the master node IP where this Csi-Controller-Pod is running")
				csi_controller_pod, k8sMasterIP, err = getK8sMasterNodeIPWhereControllerLeaderIsRunning(ctx,
					client, sshClientConfig, controller_name)
				framework.Logf("CSI-Attacher is running on elected Leader Pod %s "+
					"which is running on master node %s", csi_controller_pod, k8sMasterIP)
				gomega.Expect(err).NotTo(gomega.HaveOccurred())

				// Scale down statefulSets replica count to 0
				statefulSetReplicaCount = 0
				ginkgo.By("Scale down statefulset replica count to 0")
				for i := 0; i < len(statefulSets); i++ {
					scaleDownStatefulSetPod(ctx, client, statefulSets[i], namespace, statefulSetReplicaCount)
				}
			}
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
