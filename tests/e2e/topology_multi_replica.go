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
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	ginkgo "github.com/onsi/ginkgo/v2"
	"github.com/onsi/gomega"
	cnstypes "github.com/vmware/govmomi/cns/types"
	"github.com/vmware/govmomi/find"
	"github.com/vmware/govmomi/object"
	"github.com/vmware/govmomi/vim25/types"
	"golang.org/x/crypto/ssh"
	appsv1 "k8s.io/api/apps/v1"
	v1 "k8s.io/api/core/v1"
	storagev1 "k8s.io/api/storage/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	clientset "k8s.io/client-go/kubernetes"
	"k8s.io/kubernetes/test/e2e/framework"
	fnodes "k8s.io/kubernetes/test/e2e/framework/node"
	fpod "k8s.io/kubernetes/test/e2e/framework/pod"
	fpv "k8s.io/kubernetes/test/e2e/framework/pv"
	fss "k8s.io/kubernetes/test/e2e/framework/statefulset"
	admissionapi "k8s.io/pod-security-admission/api"
)

var _ = ginkgo.Describe("[csi-topology-multireplica-level5] Topology-Aware-Provisioning-With-MultiReplica-Level5",
	func() {
		f := framework.NewDefaultFramework("e2e-vsphere-topology-aware-provisioning")
		f.NamespacePodSecurityEnforceLevel = admissionapi.LevelPrivileged
		var (
			client                     clientset.Interface
			c                          clientset.Interface
			namespace                  string
			bindingMode                storagev1.VolumeBindingMode
			allowedTopologies          []v1.TopologySelectorLabelRequirement
			topologyAffinityDetails    map[string][]string
			topologyCategories         []string
			topologyLength             int
			isSPSServiceStopped        bool
			isVsanHealthServiceStopped bool
			sshClientConfig            *ssh.ClientConfig
			vcAddress                  string
			container_name             string
			sts_count                  int
			statefulSetReplicaCount    int32
			pvcCount                   int
			originalSizeInMb           int64
			fsSize                     int64
			expectedErrMsg             string
			deploymentReplicaCount     int32
			deploymentList             []*appsv1.Deployment
			datastoreURL               string
			pandoraSyncWaitTime        int
			defaultDatacenter          *object.Datacenter
			defaultDatastore           *object.Datastore
			fullSyncWaitTime           int
			k8sVersion                 string
			nimbusGeneratedVcPwd       string
			nimbusGeneratedK8sVmPwd    string
		)
		ginkgo.BeforeEach(func() {
			var cancel context.CancelFunc
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			client = f.ClientSet
			namespace = f.Namespace.Name
			bootstrap()
			sc, err := client.StorageV1().StorageClasses().Get(ctx, defaultNginxStorageClassName, metav1.GetOptions{})
			if err == nil && sc != nil {
				gomega.Expect(client.StorageV1().StorageClasses().Delete(ctx, sc.Name,
					*metav1.NewDeleteOptions(0))).NotTo(gomega.HaveOccurred())
			}
			nodeList, err := fnodes.GetReadySchedulableNodes(f.ClientSet)
			framework.ExpectNoError(err, "Unable to find ready and schedulable Node")
			if !(len(nodeList.Items) > 0) {
				framework.Failf("Unable to find ready and schedulable Node")
			}
			// fetching k8s version
			v, err := client.Discovery().ServerVersion()
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			k8sVersion = v.Major + "." + v.Minor

			bindingMode = storagev1.VolumeBindingWaitForFirstConsumer
			topologyLength = 5
			isSPSServiceStopped = false
			isVsanHealthServiceStopped = false

			topologyMap := GetAndExpectStringEnvVar(topologyMap)
			topologyAffinityDetails, topologyCategories = createTopologyMapLevel5(topologyMap, topologyLength)
			allowedTopologies = createAllowedTopolgies(topologyMap, topologyLength)

			nimbusGeneratedK8sVmPwd = GetAndExpectStringEnvVar(nimbusK8sVmPwd)
			nimbusGeneratedVcPwd = GetAndExpectStringEnvVar(nimbusVcPwd)

			sshClientConfig = &ssh.ClientConfig{
				User: "root",
				Auth: []ssh.AuthMethod{
					ssh.Password(nimbusGeneratedK8sVmPwd),
				},
				HostKeyCallback: ssh.InsecureIgnoreHostKey(),
			}
			vcAddress = e2eVSphere.Config.Global.VCenterHostname + ":" + sshdPort
			if os.Getenv(envPandoraSyncWaitTime) != "" {
				pandoraSyncWaitTime, err = strconv.Atoi(os.Getenv(envPandoraSyncWaitTime))
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
			} else {
				pandoraSyncWaitTime = defaultPandoraSyncWaitTime
			}
			var datacenters []string
			datastoreURL = GetAndExpectStringEnvVar(envSharedDatastoreURL)
			finder := find.NewFinder(e2eVSphere.Client.Client, false)
			cfg, err := getConfig()
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			dcList := strings.Split(cfg.Global.Datacenters, ",")
			for _, dc := range dcList {
				dcName := strings.TrimSpace(dc)
				if dcName != "" {
					datacenters = append(datacenters, dcName)
				}
			}
			for _, dc := range datacenters {
				defaultDatacenter, err = finder.Datacenter(ctx, dc)
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
				finder.SetDatacenter(defaultDatacenter)
				defaultDatastore, err = getDatastoreByURL(ctx, datastoreURL, defaultDatacenter)
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
			}
			// Read full-sync value.
			if os.Getenv(envFullSyncWaitTime) != "" {
				fullSyncWaitTime, err = strconv.Atoi(os.Getenv(envFullSyncWaitTime))
				framework.Logf("Full-Sync interval time value is = %v", fullSyncWaitTime)
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
			}

			c = client
			controllerClusterConfig := os.Getenv(contollerClusterKubeConfig)
			if controllerClusterConfig != "" {
				framework.Logf("Creating client for remote kubeconfig")
				remoteC, err := createKubernetesClientFromConfig(controllerClusterConfig)
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
				c = remoteC
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
			Verify behaviour when CSI Provisioner is deleted during statefulset
			(parallel POD management Policy) creation
			WFC + allowedTopologies (all 5 level - region > zone > building > level > rack)

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
			15. Wait until the POD count goes down to 5
			16. Identify the new CSI controller Pod  where CSI Provisioner is the leader.
			17. Scale Down replica count 0
			18. Delete StatefulSet Pod's, PVC's.
			19. Verify StatefulSet Pod's, PVC's are deleted successfully.
		*/

		ginkgo.It("Volume provisioning when CSI Provisioner is deleted during statefulset creation", func() {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			container_name = "csi-provisioner"
			sts_count = 3
			statefulSetReplicaCount = 3

			/* Get current leader Csi-Controller-Pod where CSI Provisioner is running and " +
			find the master node IP where this Csi-Controller-Pod is running */
			ginkgo.By("Get current leader Csi-Controller-Pod name where CSI Provisioner is running and " +
				"find the master node IP where this Csi-Controller-Pod is running")
			csi_controller_pod, k8sMasterIP, err := getK8sMasterNodeIPWhereContainerLeaderIsRunning(ctx,
				c, sshClientConfig, container_name)
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
					err = execDockerPauseNKillOnContainer(sshClientConfig, k8sMasterIP, container_name, k8sVersion)
					gomega.Expect(err).NotTo(gomega.HaveOccurred())
				}
			}
			wg.Wait()

			/* Get newly elected leader Csi-Controller-Pod where CSI Provisioner is running" +
			find new master node IP where this Csi-Controller-Pod is running */
			ginkgo.By("Get newly Leader Csi-Controller-Pod where CSI Provisioner is running and " +
				"find the master node IP where this Csi-Controller-Pod is running")
			csi_controller_pod, k8sMasterIP, err = getK8sMasterNodeIPWhereContainerLeaderIsRunning(ctx,
				c, sshClientConfig, container_name)
			framework.Logf("CSI-Provisioner is running on newly elected Leader Pod %s "+
				"which is running on master node %s", csi_controller_pod, k8sMasterIP)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())

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

			/* Get current leader Csi-Controller-Pod where CSI Attacher is running" +
			find new master node IP where this Csi-Controller-Pod is running */
			ginkgo.By("Get current Leader Csi-Controller-Pod where CSI Attacher is running and " +
				"find the master node IP where this Csi-Controller-Pod is running")
			container_name = "csi-attacher"
			csi_controller_pod, k8sMasterIP, err = getK8sMasterNodeIPWhereContainerLeaderIsRunning(ctx,
				c, sshClientConfig, container_name)
			framework.Logf("CSI-Attacher is running on elected Leader Pod %s "+
				"which is running on master node %s", csi_controller_pod, k8sMasterIP)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())

			// Scale down statefulSets replicas count
			ginkgo.By("Scaledown StatefulSets replica in parallel. During StatefulSets replica scaledown " +
				"kill CSI Attacher container in between")
			statefulSetReplicaCount = 2
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
					err = execDockerPauseNKillOnContainer(sshClientConfig, k8sMasterIP, container_name, k8sVersion)
					gomega.Expect(err).NotTo(gomega.HaveOccurred())
				}
			}

			/* Get newly elected leader Csi-Controller-Pod where CSI Provisioner is running" +
			find new master node IP where this Csi-Controller-Pod is running */
			ginkgo.By("Get newly elected leader Csi-Controller-Pod where CSI Provisioner is running and " +
				"find the master node IP where this Csi-Controller-Pod is running")
			container_name = "csi-provisioner"
			csi_controller_pod, k8sMasterIP, err = getK8sMasterNodeIPWhereContainerLeaderIsRunning(ctx,
				c, sshClientConfig, container_name)
			framework.Logf("CSI-Provisioner is running on elected Leader Pod %s "+
				"which is running on master node %s", csi_controller_pod, k8sMasterIP)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())

			// Scale down statefulSets replica count to 0
			statefulSetReplicaCount = 0
			ginkgo.By("Scale down statefulset replica count to 0")
			for i := 0; i < len(statefulSets); i++ {
				scaleDownStatefulSetPod(ctx, client, statefulSets[i], namespace, statefulSetReplicaCount, true)
				ssPodsAfterScaleDown := GetListOfPodsInSts(client, statefulSets[i])
				gomega.Expect(len(ssPodsAfterScaleDown.Items) == int(statefulSetReplicaCount)).To(gomega.BeTrue(),
					"Number of Pods in the statefulset should match with number of replicas")
				if i == 2 {
					/* Kill container CSI-Provisioner on the master node where elected leader CSi-Controller-Pod
					is running */
					ginkgo.By("Kill container CSI-Provisioner on the master node where elected leader CSi-Controller-Pod " +
						"is running")
					err = execDockerPauseNKillOnContainer(sshClientConfig, k8sMasterIP, container_name, k8sVersion)
					gomega.Expect(err).NotTo(gomega.HaveOccurred())
				}
			}
			// Verify that the StatefulSet Pods, PVC's are deleted successfully
			ginkgo.By("Verify that the StatefulSet Pods, PVC's are successfully deleted")
			for i := 0; i < len(statefulSets); i++ {
				ssPodsAfterScaleDown := GetListOfPodsInSts(client, statefulSets[i])
				if len(ssPodsAfterScaleDown.Items) == 0 {
					framework.Logf("StatefulSet %s and it's replicas are deleted successfully", statefulSets[i].Name)
				}
				gomega.Expect(len(ssPodsAfterScaleDown.Items) == int(statefulSetReplicaCount)).To(gomega.BeTrue(),
					"Number of Pods in the statefulset should match with number of replicas")
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
			13. Wait until the POD count goes down to 5
			14. csi-Attacher in other replica should take the leadership to detach Volume
			15. Delete Statefulsets and Delete PVCs.
		*/

		ginkgo.It("Volume provisioning when CSI Attacher is deleted during statefulset creation", func() {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			container_name = "csi-attacher"
			sts_count = 3
			statefulSetReplicaCount = 3

			/* Get current leader Csi-Controller-Pod where CSI Attacher is running" +
			find master node IP where this Csi-Controller-Pod is running */
			ginkgo.By("Get current Leader Csi-Controller-Pod where CSI Attacher is running and " +
				"find the master node IP where this Csi-Controller-Pod is running")
			csi_controller_pod, k8sMasterIP, err := getK8sMasterNodeIPWhereContainerLeaderIsRunning(ctx,
				c, sshClientConfig, container_name)
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
			ginkgo.By("Creating multiple StatefulSets Specs")
			statefulSets := createParallelStatefulSetSpec(namespace, sts_count, statefulSetReplicaCount)

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
				if i == 2 {
					/* Delete elected leader CSi-Controller-Pod where CSI-Attacher is running */
					ginkgo.By("Delete elected leader CSi-Controller-Pod where CSI-Attacher is running")
					err = deleteCsiControllerPodWhereLeaderIsRunning(ctx, client, csi_controller_pod)
					gomega.Expect(err).NotTo(gomega.HaveOccurred())
				}
			}
			wg.Wait()

			/* Get newly current leader Csi-Controller-Pod where CSI Attacher is running" +
			find master node IP where this Csi-Controller-Pod is running */
			ginkgo.By("Get newly elected current Leader Csi-Controller-Pod where CSI Attacher is running and " +
				"find the master node IP where this Csi-Controller-Pod is running")
			csi_controller_pod, k8sMasterIP, err = getK8sMasterNodeIPWhereContainerLeaderIsRunning(ctx,
				c, sshClientConfig, container_name)
			framework.Logf("CSI-Attacher is running on newly elected Leader Pod %s "+
				"which is running on master node %s", csi_controller_pod, k8sMasterIP)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())

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

			// Scale down StatefulSets replicas count
			statefulSetReplicaCount = 2
			ginkgo.By("Scale down statefulset replica count")
			for i := 0; i < len(statefulSets); i++ {
				scaleDownStatefulSetPod(ctx, client, statefulSets[i], namespace, statefulSetReplicaCount, true)
				ssPodsAfterScaleDown := GetListOfPodsInSts(client, statefulSets[i])
				gomega.Expect(len(ssPodsAfterScaleDown.Items) == int(statefulSetReplicaCount)).To(gomega.BeTrue(),
					"Number of Pods in the statefulset should match with number of replicas")
				if i == 2 {
					/* Delete newly elected leader CSi-Controller-Pod where CSI-Attacher is running */
					ginkgo.By("Delete elected leader CSi-Controller-Pod where CSI-Attacher is running")
					err = deleteCsiControllerPodWhereLeaderIsRunning(ctx, client, csi_controller_pod)
					gomega.Expect(err).NotTo(gomega.HaveOccurred())
				}
			}

			/* Get newly elected current leader Csi-Controller-Pod where CSI Attacher is running" +
			find new master node IP where this Csi-Controller-Pod is running */
			ginkgo.By("Get newly elected current Leader Csi-Controller-Pod where CSI Attacer is " +
				"running and find the master node IP where this Csi-Controller-Pod is running")
			csi_controller_pod, k8sMasterIP, err = getK8sMasterNodeIPWhereContainerLeaderIsRunning(ctx,
				c, sshClientConfig, container_name)
			framework.Logf("CSI-Attacher is running on elected Leader Pod %s "+
				"which is running on master node %s", csi_controller_pod, k8sMasterIP)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())

			// Scale down statefulSets replica count to 0
			statefulSetReplicaCount = 0
			ginkgo.By("Scale down statefulset replica count to 0")
			for i := 0; i < len(statefulSets); i++ {
				scaleDownStatefulSetPod(ctx, client, statefulSets[i], namespace, statefulSetReplicaCount, true)
				ssPodsAfterScaleDown := GetListOfPodsInSts(client, statefulSets[i])
				gomega.Expect(len(ssPodsAfterScaleDown.Items) == int(statefulSetReplicaCount)).To(gomega.BeTrue(),
					"Number of Pods in the statefulset should match with number of replicas")
			}

			// Verify that the StatefulSet Pods, PVC's are deleted successfully
			ginkgo.By("Verify that the StatefulSet Pods, PVC's are successfully deleted")
			for i := 0; i < len(statefulSets); i++ {
				ssPodsAfterScaleDown := GetListOfPodsInSts(client, statefulSets[i])
				if len(ssPodsAfterScaleDown.Items) == 0 {
					framework.Logf("StatefulSet %s and it's replicas are deleted successfully", statefulSets[i].Name)
				}
				gomega.Expect(len(ssPodsAfterScaleDown.Items) == int(statefulSetReplicaCount)).To(gomega.BeTrue(),
					"Number of Pods in the statefulset should match with number of replicas")
			}
		})

		/*
			TESTCASE-3
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
			container_name = "csi-attacher"
			sts_count = 3
			statefulSetReplicaCount = 3
			ignoreLabels := make(map[string]string)

			/* Get current leader Csi-Controller-Pod where CSI Attacher is running" +
			find master node IP where this Csi-Controller-Pod is running */
			ginkgo.By("Get current Leader Csi-Controller-Pod where CSI Attacher is running and " +
				"find the master node IP where this Csi-Controller-Pod is running")
			csi_controller_pod, k8sMasterIP, err := getK8sMasterNodeIPWhereContainerLeaderIsRunning(ctx,
				c, sshClientConfig, container_name)
			framework.Logf("CSI-Attacher is running on elected Leader Pod %s "+
				"which is running on master node %s", csi_controller_pod, k8sMasterIP)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())

			/* Get allowed topologies for Storage Class */
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

			// Create multiple StatefulSets Specs for creation of StatefulSets
			ginkgo.By("Creating multiple StatefulSets Specs")
			statefulSets := createParallelStatefulSetSpec(namespace, sts_count, statefulSetReplicaCount)

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
				if i == 2 {
					/* Delete elected leader CSi-Controller-Pod where CSI-Attacher is running */
					ginkgo.By("Delete elected leader CSi-Controller-Pod where CSI-Attacher is running")
					err = deleteCsiControllerPodWhereLeaderIsRunning(ctx, client, csi_controller_pod)
					gomega.Expect(err).NotTo(gomega.HaveOccurred())
				}
			}
			wg.Wait()

			/* Get newly current leader Csi-Controller-Pod where CSI Attacher is running" +
			find master node IP where this Csi-Controller-Pod is running */
			ginkgo.By("Get newly elected current Leader Csi-Controller-Pod where CSI Attacher is running and " +
				"find the master node IP where this Csi-Controller-Pod is running")
			csi_controller_pod, k8sMasterIP, err = getK8sMasterNodeIPWhereContainerLeaderIsRunning(ctx,
				c, sshClientConfig, container_name)
			framework.Logf("CSI-Attacher is running on newly elected Leader Pod %s "+
				"which is running on master node %s", csi_controller_pod, k8sMasterIP)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())

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

			// Fetch the number of CSI pods running before restart
			list_of_pods, err := fpod.GetPodsInNamespace(client, csiSystemNamespace, ignoreLabels)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			num_csi_pods := len(list_of_pods)

			// Collecting and dumping csi pod logs before restrating CSI daemonset
			collectPodLogs(ctx, client, csiSystemNamespace)

			// Restart CSI daemonset
			ginkgo.By("Restart Daemonset")
			cmd := []string{"rollout", "restart", "daemonset/vsphere-csi-node", "--namespace=" + csiSystemNamespace}
			framework.RunKubectlOrDie(csiSystemNamespace, cmd...)

			ginkgo.By("Waiting for daemon set rollout status to finish")
			statusCheck := []string{"rollout", "status", "daemonset/vsphere-csi-node", "--namespace=" + csiSystemNamespace}
			framework.RunKubectlOrDie(csiSystemNamespace, statusCheck...)

			// wait for csi Pods to be in running ready state
			err = fpod.WaitForPodsRunningReady(client, csiSystemNamespace, int32(num_csi_pods), 0, pollTimeout, ignoreLabels)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())

			// Scale up statefulSets replicas count
			ginkgo.By("Scale up StaefulSets replicas in parallel")
			statefulSetReplicaCount = 5
			for i := 0; i < len(statefulSets); i++ {
				scaleUpStatefulSetPod(ctx, client, statefulSets[i], namespace, statefulSetReplicaCount, true)
			}

			/* Verify PV nde affinity and that the pods are running on appropriate nodes
			for each StatefulSet pod */
			ginkgo.By("Verify PV node affinity and that the PODS are running on appropriate node")
			for i := 0; i < len(statefulSets); i++ {
				verifyPVnodeAffinityAndPODnodedetailsForStatefulsetsLevel5(ctx, client,
					statefulSets[i], namespace, allowedTopologies, true)
			}

			// Scale down statefulset to 0 replicas
			statefulSetReplicaCount = 0
			ginkgo.By("Scale down statefulset replica count to 0")
			for i := 0; i < len(statefulSets); i++ {
				scaleDownStatefulSetPod(ctx, client, statefulSets[i], namespace, statefulSetReplicaCount, true)
			}
		})

		/*
			TESTCASE-4
			Verify the behaviour when CSI-resizer deleted and VSAN-Health is down during online Volume expansion
			Immediate +allowedTopologies (all 5 level)

			Steps//
			1. Identify the Pod where CSI-resizer is the leader.
			2. Create SC with allowVolumeExpansion set to true.
			3. Create multiple PVC's (around 10) using SC above created SC.
			4. Verify node affinity details on PV's.
			5. Create multiple Pod's using above created PVCs.
			6. Bring down vsan-health service (Login to VC and execute : service-control --stop vsan-health)
			7. Trigger online volume expansion on all the PVC's. At the same time delete the Pod identified in the Step 1.
			8. Expand volume should fail with error service unavailable.
			9. Bring up the VSAN-health (Login to VC and execute : service-control --start vsan-health)
			10. Expect Volume should be expanded by the newly elected csi-resizer leader, and filesystem for the
			volume on the pod should also be expanded.
			11. Describe PV and Verify the node affinity rule.
			Make sure node affinity should contain all 5 levels of topology details
			12. POD should be running in the appropriate nodes.
			13. Delete Pod, PVC and SC.
		*/

		ginkgo.It("Verify behaviour when CSI-resizer deleted and VSAN-Health "+
			"is down during online Volume expansion", func() {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			container_name = "csi-resizer"
			pvcCount = 5
			scParameters := make(map[string]string)
			var podList []*v1.Pod

			/* Get current leader Csi-Controller-Pod where CSI Resizer is running" +
			find master node IP where this Csi-Controller-Pod is running */
			ginkgo.By("Get current Leader Csi-Controller-Pod where CSI Resizer is running and " +
				"find the master node IP where this Csi-Controller-Pod is running")
			csi_controller_pod, k8sMasterIP, err := getK8sMasterNodeIPWhereContainerLeaderIsRunning(ctx,
				c, sshClientConfig, container_name)
			framework.Logf("CSI-Resizer is running on elected Leader Pod %s "+
				"which is running on master node %s", csi_controller_pod, k8sMasterIP)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())

			/* Get allowed topologies for Storage Class */
			allowedTopologyForSC := getTopologySelector(topologyAffinityDetails, topologyCategories,
				topologyLength)

			// Create SC with Immedaite Binding mode and allowVolumeExpansion set to true
			ginkgo.By("Create SC with Immedaite Binding mode and allowVolumeExpansion set to true")
			scParameters[scParamFsType] = ext4FSType
			storageclass, err := createStorageClass(client, scParameters, allowedTopologyForSC,
				"", "", true, "")
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			defer func() {
				err := client.StorageV1().StorageClasses().Delete(ctx, storageclass.Name, *metav1.NewDeleteOptions(0))
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
			}()

			// Creating multiple PVCs
			ginkgo.By("Trigger multiple PVCs")
			pvclaimsList := createMultiplePVCsInParallel(ctx, client, namespace, storageclass, pvcCount)

			// Verify PVC claim to be in bound phase and create POD for each PVC
			ginkgo.By("Verify PVC claim to be in bound phase and create POD for each PVC")
			for i := 0; i < len(pvclaimsList); i++ {
				var pvclaims []*v1.PersistentVolumeClaim
				// Waiting for PVC claim to be in bound phase
				pvc, err := fpv.WaitForPVClaimBoundPhase(client,
					[]*v1.PersistentVolumeClaim{pvclaimsList[i]}, framework.ClaimProvisionTimeout)
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
				gomega.Expect(pvc).NotTo(gomega.BeEmpty())

				// Get PV details
				pv := getPvFromClaim(client, pvclaimsList[i].Namespace, pvclaimsList[i].Name)

				// create Pod for each PVC
				ginkgo.By("Creating Pod")
				pvclaims = append(pvclaims, pvclaimsList[i])
				pod, err := createPod(client, namespace, nil, pvclaims, false, "")
				podList = append(podList, pod)
				gomega.Expect(err).NotTo(gomega.HaveOccurred())

				// verify volume is attached to the node
				ginkgo.By(fmt.Sprintf("Verify volume: %s is attached to the node: %s",
					pv.Spec.CSI.VolumeHandle, pod.Spec.NodeName))
				vmUUID := getNodeUUID(ctx, client, pod.Spec.NodeName)
				isDiskAttached, err := e2eVSphere.isVolumeAttachedToVM(client, pv.Spec.CSI.VolumeHandle, vmUUID)
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
				gomega.Expect(isDiskAttached).To(gomega.BeTrue(), "Volume is not attached")
			}
			defer func() {
				// cleanup code for deleting PVC
				ginkgo.By("Deleting PVC's and PV's")
				for i := 0; i < len(pvclaimsList); i++ {
					pv := getPvFromClaim(client, pvclaimsList[i].Namespace, pvclaimsList[i].Name)
					err = fpv.DeletePersistentVolumeClaim(client, pvclaimsList[i].Name, namespace)
					gomega.Expect(err).NotTo(gomega.HaveOccurred())
					framework.ExpectNoError(fpv.WaitForPersistentVolumeDeleted(client, pv.Name, poll, pollTimeoutShort))
					err = e2eVSphere.waitForCNSVolumeToBeDeleted(pv.Spec.CSI.VolumeHandle)
					gomega.Expect(err).NotTo(gomega.HaveOccurred())
				}
			}()
			defer func() {
				// cleanup code for deleting POD
				for i := 0; i < len(podList); i++ {
					ginkgo.By(fmt.Sprintf("Deleting the pod %s in namespace %s", podList[i].Name, namespace))
					err = fpod.DeletePodWithWait(client, podList[i])
					gomega.Expect(err).NotTo(gomega.HaveOccurred())
				}
				// Verify volume is detached from the node
				ginkgo.By("Verify volume is detached from the node")
				for i := 0; i < len(pvclaimsList); i++ {
					pv := getPvFromClaim(client, pvclaimsList[i].Namespace, pvclaimsList[i].Name)
					isDiskDetached, err := e2eVSphere.waitForVolumeDetachedFromNode(client,
						pv.Spec.CSI.VolumeHandle, podList[i].Spec.NodeName)
					gomega.Expect(err).NotTo(gomega.HaveOccurred())
					gomega.Expect(isDiskDetached).To(gomega.BeTrue(),
						fmt.Sprintf("Volume %q is not detached from the node", pv.Spec.CSI.VolumeHandle))
				}
			}()

			// Stopping vsan-health service on vcenter host
			ginkgo.By(fmt.Sprintf("Stopping %v on the vCenter host", vsanhealthServiceName))
			isVsanHealthServiceStopped = true
			err = invokeVCenterServiceControl(stopOperation, vsanhealthServiceName, vcAddress)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			err = waitVCenterServiceToBeInState(vsanhealthServiceName, vcAddress, svcStoppedMessage)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			defer func() {
				if isVsanHealthServiceStopped {
					ginkgo.By(fmt.Sprintf("Starting %v on the vCenter host", vsanhealthServiceName))
					startVCServiceWait4VPs(ctx, vcAddress, vsanhealthServiceName, &isVsanHealthServiceStopped)
				}
			}()

			// Expanding pvc when vsan-health service on vcenter host is down
			ginkgo.By("Expanding pvc when vsan-health service on vcenter host is down")
			for i := 0; i < len(pvclaimsList); i++ {
				currentPvcSize := pvclaimsList[i].Spec.Resources.Requests[v1.ResourceStorage]
				newSize := currentPvcSize.DeepCopy()
				newSize.Add(resource.MustParse("1Gi"))
				framework.Logf("currentPvcSize %v, newSize %v", currentPvcSize, newSize)
				pvclaim, err := expandPVCSize(pvclaimsList[i], newSize, client)
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
				gomega.Expect(pvclaim).NotTo(gomega.BeNil())

				// File system resize should not succeed Since Vsan-health is down. Expect an error
				ginkgo.By("File system resize should not succeed Since Vsan-health is down. Expect an error")
				expectedErrMsg = "503 Service Unavailable"
				framework.Logf("Expected failure message: %+q", expectedErrMsg)
				err = waitForEvent(ctx, client, namespace, expectedErrMsg, pvclaim.Name)
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
				if i == 1 {
					ginkgo.By("Delete elected leader Csi-Controller-Pod where CSi-Attacher is running")
					err = deleteCsiControllerPodWhereLeaderIsRunning(ctx, client, csi_controller_pod)
					gomega.Expect(err).NotTo(gomega.HaveOccurred())
				}
			}
			// Starting vsan-health service on vcenter host
			ginkgo.By("Bringup vsanhealth service")
			startVCServiceWait4VPs(ctx, vcAddress, vsanhealthServiceName, &isVsanHealthServiceStopped)

			/* Get current leader Csi-Controller-Pod where CSI Resizer is running" +
			find master node IP where this Csi-Controller-Pod is running */
			ginkgo.By("Get current Leader Csi-Controller-Pod where CSI Resizer is running and " +
				"find the master node IP where this Csi-Controller-Pod is running")
			csi_controller_pod, k8sMasterIP, err = getK8sMasterNodeIPWhereContainerLeaderIsRunning(ctx,
				c, sshClientConfig, container_name)
			framework.Logf("CSI-Resizer is running on elected Leader Pod %s "+
				"which is running on master node %s", csi_controller_pod, k8sMasterIP)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())

			// Expanding pvc when vsan-health service on vcenter host is started
			ginkgo.By("Expanding pvc when vsan-health service on vcenter host is started")
			for i := 0; i < len(pvclaimsList); i++ {
				currentPvcSize := pvclaimsList[i].Spec.Resources.Requests[v1.ResourceStorage]
				newSize := currentPvcSize.DeepCopy()
				newSize.Add(resource.MustParse("1Gi"))
				framework.Logf("currentPvcSize %v, newSize %v", currentPvcSize, newSize)
				pvclaim, err := expandPVCSize(pvclaimsList[i], newSize, client)
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
				gomega.Expect(pvclaim).NotTo(gomega.BeNil())

				ginkgo.By("Waiting for file system resize to finish")
				pvclaim, err = waitForFSResize(pvclaimsList[i], client)
				gomega.Expect(err).NotTo(gomega.HaveOccurred())

				pvcConditions := pvclaim.Status.Conditions
				expectEqual(len(pvcConditions), 0, "pvc should not have conditions")
			}
			for i := 0; i < len(podList); i++ {
				ginkgo.By("Verify filesystem size for mount point /mnt/volume1")
				fsSize, err = getFSSizeMb(f, podList[i])
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
				gomega.Expect(fsSize).Should(gomega.BeNumerically(">", originalSizeInMb),
					fmt.Sprintf("error updating filesystem size."+
						"Resulting filesystem size is %d", fsSize))
				ginkgo.By("File system resize finished successfully")
			}

			/* Verify PV nde affinity and that the pods are running on appropriate nodes
			for each StatefulSet pod */
			ginkgo.By("Verify PV node affinity and that the PODS are running on appropriate node")
			for i := 0; i < len(podList); i++ {
				verifyPVnodeAffinityAndPODnodedetailsFoStandalonePodLevel5(ctx, client, podList[i],
					namespace, allowedTopologies)
			}
		})

		/*
			TESTCASE-5
			Verify the behaviour when CSI-resizer deleted during offline volume expansion
			Immediate +allowedTopologies (all 5 level)

			Steps//
			1. Identify the Pod where CSI-resizer is the leader.
			2. Create SC with allowVolumeExpansion set to true.
			3. Create multiple PVC's using SC created above.
			4. Describe PV and verify the node affinity details of all 5 levels.
			5. Expand PVC size, and delete pod which is identified in step1.
			6. Create POD's using the above created PVC's.
			7. Expect Volume should be expanded by the newly elected csi-resizer leader,
			and filesystem for the volume on the pod should also be expanded.
			8. Delete Pod, PVC and SC
		*/
		ginkgo.It("Verify the behaviour when CSI-resizer deleted during offline volume expansion", func() {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			container_name = "csi-resizer"
			pvcCount = 5
			scParameters := make(map[string]string)
			var podList []*v1.Pod

			/* Get current leader Csi-Controller-Pod where CSI Resizer is running" +
			find master node IP where this Csi-Controller-Pod is running */
			ginkgo.By("Get current Leader Csi-Controller-Pod where CSI Resizer is running and " +
				"find the master node IP where this Csi-Controller-Pod is running")
			csi_controller_pod, k8sMasterIP, err := getK8sMasterNodeIPWhereContainerLeaderIsRunning(ctx,
				c, sshClientConfig, container_name)
			framework.Logf("CSI-Resizer is running on elected Leader Pod %s "+
				"which is running on master node %s", csi_controller_pod, k8sMasterIP)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())

			// Get allowed topologies for Storage Class
			allowedTopologyForSC := getTopologySelector(topologyAffinityDetails, topologyCategories,
				topologyLength)

			// Create SC with Immedaite Binding mode and allowVolumeExpansion set to true
			ginkgo.By("Create SC with Immedaite Binding mode and allowVolumeExpansion set to true")
			scParameters[scParamFsType] = ext4FSType
			storageclass, err := createStorageClass(client, scParameters, allowedTopologyForSC,
				"", "", true, "")
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			defer func() {
				err := client.StorageV1().StorageClasses().Delete(ctx, storageclass.Name, *metav1.NewDeleteOptions(0))
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
			}()

			// Creating multiple PVCs
			ginkgo.By("Trigger multiple PVCs")
			pvclaimsList := createMultiplePVCsInParallel(ctx, client, namespace, storageclass, pvcCount)

			// Verify PVC claim to be in bound phase and create POD for each PVC
			ginkgo.By("Verify PVC claim to be in bound phase and create POD for each PVC")
			for i := 0; i < len(pvclaimsList); i++ {
				// Waiting for PVC claim to be in bound phase
				pvc, err := fpv.WaitForPVClaimBoundPhase(client,
					[]*v1.PersistentVolumeClaim{pvclaimsList[i]}, framework.ClaimProvisionTimeout)
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
				gomega.Expect(pvc).NotTo(gomega.BeEmpty())
			}

			// Expanding PVC
			ginkgo.By("Expand pvc and in between delete elected leader Csi-Controller-Pod " +
				"where CSi-Resizer is running ")
			for i := 0; i < len(pvclaimsList); i++ {
				// Get PV details
				pv := getPvFromClaim(client, pvclaimsList[i].Namespace, pvclaimsList[i].Name)

				currentPvcSize := pvclaimsList[i].Spec.Resources.Requests[v1.ResourceStorage]
				newSize := currentPvcSize.DeepCopy()
				newSize.Add(resource.MustParse("1Gi"))
				framework.Logf("currentPvcSize %v, newSize %v", currentPvcSize, newSize)
				pvclaim, err := expandPVCSize(pvclaimsList[i], newSize, client)
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
				gomega.Expect(pvclaim).NotTo(gomega.BeNil())

				pvcSize := pvclaim.Spec.Resources.Requests[v1.ResourceStorage]
				if pvcSize.Cmp(newSize) != 0 {
					framework.Failf("error updating pvc size %q", pvclaim.Name)
				}

				ginkgo.By("Waiting for controller volume resize to finish")
				err = waitForPvResizeForGivenPvc(pvclaim, client, totalResizeWaitPeriod)
				gomega.Expect(err).NotTo(gomega.HaveOccurred())

				ginkgo.By("Checking for conditions on pvc")
				_, err = waitForPVCToReachFileSystemResizePendingCondition(client, namespace,
					pvclaim.Name, pollTimeout)
				gomega.Expect(err).NotTo(gomega.HaveOccurred())

				volHandle := pv.Spec.CSI.VolumeHandle
				ginkgo.By(fmt.Sprintf("Invoking QueryCNSVolumeWithResult with VolumeID: %s", volHandle))
				queryResult, err := e2eVSphere.queryCNSVolumeWithResult(volHandle)
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
				if len(queryResult.Volumes) == 0 {
					err = fmt.Errorf("queryCNSVolumeWithResult returned no volume")
				}
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
				ginkgo.By("Verifying disk size requested in volume expansion is honored")
				newSizeInMb := int64(3072)
				if queryResult.Volumes[0].BackingObjectDetails.(*cnstypes.CnsBlockBackingDetails).CapacityInMb != newSizeInMb {
					err = fmt.Errorf("got wrong disk size after volume expansion")
				}
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
				if i == 4 {
					ginkgo.By("Delete elected leader Csi-Controller-Pod where CSi-Resizer is running")
					err = deleteCsiControllerPodWhereLeaderIsRunning(ctx, client, csi_controller_pod)
					gomega.Expect(err).NotTo(gomega.HaveOccurred())
				}
			}

			// Verify PVC claim to be in bound phase and create POD for each PVC
			ginkgo.By("Verify PVC claim to be in bound phase and create POD for each PVC")
			for i := 0; i < len(pvclaimsList); i++ {
				var pvclaims []*v1.PersistentVolumeClaim
				// Get PV details
				pv := getPvFromClaim(client, pvclaimsList[i].Namespace, pvclaimsList[i].Name)

				// create Pod for each PVC
				ginkgo.By("Creating Pod")
				pvclaims = append(pvclaims, pvclaimsList[i])
				pod, err := createPod(client, namespace, nil, pvclaims, false, "")
				podList = append(podList, pod)
				gomega.Expect(err).NotTo(gomega.HaveOccurred())

				// verify volume is attached to the node
				ginkgo.By(fmt.Sprintf("Verify volume: %s is attached to the node: %s",
					pv.Spec.CSI.VolumeHandle, pod.Spec.NodeName))
				vmUUID := getNodeUUID(ctx, client, pod.Spec.NodeName)
				isDiskAttached, err := e2eVSphere.isVolumeAttachedToVM(client, pv.Spec.CSI.VolumeHandle, vmUUID)
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
				gomega.Expect(isDiskAttached).To(gomega.BeTrue(), "Volume is not attached")
			}
			defer func() {
				// cleanup code for deleting PVC
				ginkgo.By("Deleting PVC's and PV's")
				for i := 0; i < len(pvclaimsList); i++ {
					pv := getPvFromClaim(client, pvclaimsList[i].Namespace, pvclaimsList[i].Name)
					err = fpv.DeletePersistentVolumeClaim(client, pvclaimsList[i].Name, namespace)
					gomega.Expect(err).NotTo(gomega.HaveOccurred())
					framework.ExpectNoError(fpv.WaitForPersistentVolumeDeleted(client, pv.Name, poll, pollTimeoutShort))
					err = e2eVSphere.waitForCNSVolumeToBeDeleted(pv.Spec.CSI.VolumeHandle)
					gomega.Expect(err).NotTo(gomega.HaveOccurred())
				}
			}()
			defer func() {
				// cleanup code for deleting POD
				for i := 0; i < len(podList); i++ {
					ginkgo.By(fmt.Sprintf("Deleting the pod %s in namespace %s", podList[i].Name, namespace))
					err = fpod.DeletePodWithWait(client, podList[i])
					gomega.Expect(err).NotTo(gomega.HaveOccurred())
				}
				// Verify volume is detached from the node
				ginkgo.By("Verify volume is detached from the node")
				for i := 0; i < len(pvclaimsList); i++ {
					pv := getPvFromClaim(client, pvclaimsList[i].Namespace, pvclaimsList[i].Name)
					isDiskDetached, err := e2eVSphere.waitForVolumeDetachedFromNode(client,
						pv.Spec.CSI.VolumeHandle, podList[i].Spec.NodeName)
					gomega.Expect(err).NotTo(gomega.HaveOccurred())
					gomega.Expect(isDiskDetached).To(gomega.BeTrue(),
						fmt.Sprintf("Volume %q is not detached from the node", pv.Spec.CSI.VolumeHandle))
				}
			}()

			/* Get newly elected leader Csi-Controller-Pod where CSI Resizer is running" +
			find master node IP where this Csi-Controller-Pod is running */
			ginkgo.By("Get newly elected leader Csi-Controller-Pod where CSI Resizer is running and " +
				"find the master node IP where this Csi-Controller-Pod is running")
			csi_controller_pod, k8sMasterIP, err = getK8sMasterNodeIPWhereContainerLeaderIsRunning(ctx,
				c, sshClientConfig, container_name)
			framework.Logf("CSI-Resizer is running on elected Leader Pod %s "+
				"which is running on master node %s", csi_controller_pod, k8sMasterIP)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())

			// Verify file system resize
			ginkgo.By("Verify file system resize completed successfully")
			for i := 0; i < len(pvclaimsList); i++ {
				ginkgo.By("Waiting for file system resize to finish")
				pvclaim, err := waitForFSResize(pvclaimsList[i], client)
				gomega.Expect(err).NotTo(gomega.HaveOccurred())

				pvcConditions := pvclaim.Status.Conditions
				expectEqual(len(pvcConditions), 0, "pvc should not have conditions")
			}
			for i := 0; i < len(podList); i++ {
				ginkgo.By("Verify filesystem size for mount point /mnt/volume1")
				fsSize, err = getFSSizeMb(f, podList[i])
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
				gomega.Expect(fsSize).Should(gomega.BeNumerically(">", originalSizeInMb),
					fmt.Sprintf("error updating filesystem size for %q. "+
						"Resulting filesystem size is %d", pvclaimsList[i].Name, fsSize))
				ginkgo.By("File system resize finished successfully")
			}

			/* Verify PV nde affinity and that the pods are running on appropriate nodes
			for each StatefulSet pod */
			ginkgo.By("Verify PV node affinity and that the PODS are running on appropriate node")
			for i := 0; i < len(podList); i++ {
				verifyPVnodeAffinityAndPODnodedetailsFoStandalonePodLevel5(ctx, client, podList[i],
					namespace, allowedTopologies)
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
			container_name = "csi-attacher"
			var lables = make(map[string]string)
			lables["app"] = "nginx"
			pvcCount = 3
			deploymentReplicaCount = 1
			ignoreLabels := make(map[string]string)

			/* Get current leader Csi-Controller-Pod where CSI Attacher is running" +
			find master node IP where this Csi-Controller-Pod is running */
			ginkgo.By("Get current Leader Csi-Controller-Pod where CSI Attacher is running and " +
				"find the master node IP where this Csi-Controller-Pod is running")
			csi_controller_pod, k8sMasterIP, err := getK8sMasterNodeIPWhereContainerLeaderIsRunning(ctx,
				c, sshClientConfig, container_name)
			framework.Logf("CSI-Attacher is running on elected Leader Pod %s "+
				"which is running on master node %s", csi_controller_pod, k8sMasterIP)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())

			// Get allowed topologies for Storage Class
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
			pvclaimsList := createMultiplePVCsInParallel(ctx, client, namespace, storageclass, pvcCount)

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
				/* Verify PV nde affinity and that the pods are running on appropriate nodes
				for each StatefulSet pod */
				verifyPVnodeAffinityAndPODnodedetailsForDeploymentSetsLevel5(ctx, client, deployment,
					namespace, allowedTopologies, true)
				deploymentList = append(deploymentList, deployment)
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
				// Delete elected leader Csi-Controller-Pod where CSi-Attacher is running
				if i == 2 {
					ginkgo.By("Delete elected leader Csi-Controller-Pod where CSi-Attacher is running")
					err = deleteCsiControllerPodWhereLeaderIsRunning(ctx, client, csi_controller_pod)
					gomega.Expect(err).NotTo(gomega.HaveOccurred())
				}
			}
			defer func() {
				// cleanup code for deleting PVC
				ginkgo.By("Deleting PVC")
				for i := 0; i < len(pvclaimsList); i++ {
					pv := getPvFromClaim(client, pvclaimsList[i].Namespace, pvclaimsList[i].Name)
					err = fpv.DeletePersistentVolumeClaim(client, pvclaimsList[i].Name, namespace)
					gomega.Expect(err).NotTo(gomega.HaveOccurred())
					framework.ExpectNoError(fpv.WaitForPersistentVolumeDeleted(client, pv.Name, poll, pollTimeoutShort))
					err = e2eVSphere.waitForCNSVolumeToBeDeleted(pv.Spec.CSI.VolumeHandle)
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
			csi_controller_pod, k8sMasterIP, err = getK8sMasterNodeIPWhereContainerLeaderIsRunning(ctx,
				c, sshClientConfig, container_name)
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
			TESTCASE-8
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
			14. Wait until the POD count goes down to 5.
			15. csi-Attacher in other replica should take the leadership to detach Volume.
			16. Delete Statefulsets and Delete PVCs.
		*/

		ginkgo.It("Volume provisioning when sps service is down during statefulset creation", func() {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			container_name = "csi-provisioner"
			sts_count = 3
			statefulSetReplicaCount = 3

			/* Get current leader Csi-Controller-Pod where CSI Provisioner is running" +
			find master node IP where this Csi-Controller-Pod is running */
			ginkgo.By("Get current Leader Csi-Controller-Pod where CSI Provisioner is running and " +
				"find the master node IP where this Csi-Controller-Pod is running")
			csi_controller_pod, k8sMasterIP, err := getK8sMasterNodeIPWhereContainerLeaderIsRunning(ctx,
				c, sshClientConfig, container_name)
			framework.Logf("CSI-Provisioner is running on elected Leader Pod %s "+
				"which is running on master node %s", csi_controller_pod, k8sMasterIP)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())

			// Get allowed topologies for Storage Class
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
				startVCServiceWait4VPs(ctx, vcAddress, spsServiceName, &isSPSServiceStopped)
			}()

			// Creating Service for StatefulSet
			ginkgo.By("Creating service")
			service := CreateService(namespace, client)
			defer func() {
				deleteService(namespace, client, service)
			}()

			// Create Multiple StatefulSets Specs for creation of StatefulSets
			ginkgo.By("Creating Multiple StatefulSets Specs")
			statefulSets := createParallelStatefulSetSpec(namespace, sts_count, statefulSetReplicaCount)

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
				if i == 2 {
					/* Delete elected leader CSi-Controller-Pod where CSI-Attacher is running */
					ginkgo.By("Delete elected leader CSi-Controller-Pod where CSI-Provisioner is running")
					err = deleteCsiControllerPodWhereLeaderIsRunning(ctx, client, csi_controller_pod)
					gomega.Expect(err).NotTo(gomega.HaveOccurred())
				}
			}
			wg.Wait()

			/* Get newly current leader Csi-Controller-Pod where CSI Provisioner is running" +
			find master node IP where this Csi-Controller-Pod is running */
			ginkgo.By("Get newly elected current Leader Csi-Controller-Pod where CSI " +
				"Provisioner is running and find the master node IP where " +
				"this Csi-Controller-Pod is running")
			csi_controller_pod, k8sMasterIP, err = getK8sMasterNodeIPWhereContainerLeaderIsRunning(ctx,
				c, sshClientConfig, container_name)
			framework.Logf("CSI-Provisioner is running on newly elected Leader Pod %s "+
				"which is running on master node %s", csi_controller_pod, k8sMasterIP)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())

			// Bring up SPS service
			if isSPSServiceStopped {
				startVCServiceWait4VPs(ctx, vcAddress, spsServiceName, &isSPSServiceStopped)
			}

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

			/* Get elected current leader Csi-Controller-Pod where CSI Attacher is running" +
			find new master node IP where this Csi-Controller-Pod is running */
			ginkgo.By("Get elected current Leader Csi-Controller-Pod where CSI Attacher is " +
				"running and find the master node IP where this Csi-Controller-Pod is running")
			container_name = "csi-attacher"
			csi_controller_pod, k8sMasterIP, err = getK8sMasterNodeIPWhereContainerLeaderIsRunning(ctx,
				c, sshClientConfig, container_name)
			framework.Logf("CSI-Attacher is running on elected Leader Pod %s "+
				"which is running on master node %s", csi_controller_pod, k8sMasterIP)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())

			// Scale down StatefulSets replicas count
			statefulSetReplicaCount = 2
			ginkgo.By("Scale down statefulset replica count")
			for i := 0; i < len(statefulSets); i++ {
				scaleDownStatefulSetPod(ctx, client, statefulSets[i], namespace, statefulSetReplicaCount, true)
				if i == 1 {
					/* Delete newly elected leader CSi-Controller-Pod where CSI-Attacher is running */
					ginkgo.By("Delete elected leader CSi-Controller-Pod where CSI-Attacher is running")
					err = deleteCsiControllerPodWhereLeaderIsRunning(ctx, client, csi_controller_pod)
					gomega.Expect(err).NotTo(gomega.HaveOccurred())
				}
			}

			/* Get newly elected current leader Csi-Controller-Pod where CSI Attacher is running" +
			find new master node IP where this Csi-Controller-Pod is running */
			ginkgo.By("Get newly elected current Leader Csi-Controller-Pod where CSI Provisioner is " +
				"running and find the master node IP where this Csi-Controller-Pod is running")
			csi_controller_pod, k8sMasterIP, err = getK8sMasterNodeIPWhereContainerLeaderIsRunning(ctx,
				c, sshClientConfig, container_name)
			framework.Logf("CSI-Attacher is running on elected Leader Pod %s "+
				"which is running on master node %s", csi_controller_pod, k8sMasterIP)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())

			// Scale down statefulSets replica count to 0
			statefulSetReplicaCount = 0
			ginkgo.By("Scale down statefulset replica count to 0")
			for i := 0; i < len(statefulSets); i++ {
				scaleDownStatefulSetPod(ctx, client, statefulSets[i], namespace, statefulSetReplicaCount, true)
			}

			// Verify that the StatefulSet Pods, PVC's are deleted successfully
			ginkgo.By("Verify that the StatefulSet Pods, PVC's are successfully deleted")
			for i := 0; i < len(statefulSets); i++ {
				ssPodsAfterScaleDown := GetListOfPodsInSts(client, statefulSets[i])
				if len(ssPodsAfterScaleDown.Items) == 0 {
					framework.Logf("StatefulSet %s and it's replicas are deleted successfully", statefulSets[i].Name)
				}
				gomega.Expect(len(ssPodsAfterScaleDown.Items) == int(statefulSetReplicaCount)).To(gomega.BeTrue(),
					"Number of Pods in the statefulset should match with number of replicas")
			}
		})

		/*
			TESTCASE-10
			verify Dynamic provisioning with Thick policy when CSI-provisioner goes down
			Immediate +allowedTopologies (all 5 level)

			Steps//
			1. Identify the process where CSI Provisioner is the leader.
			2. Create a Storage class with "thick" provisioning policy
			3. Create multiple dynamically provisioned pvc's. While PVC's are creating
			delete the CSI controller Pod identified in the step 1, where CSI provisioner is the leader.
			4. Csi-provisioner in other replica should take the leadership to help
			provisioning of the volume.
			5. Wait until all PVCs reach bound state.
			6. Identify the process where CSI Attacher is the leader.
			7. Create multiple POD's using above created PVC's .
			While Pod's are creating delete the CSI controller process identified in the step 6,
			where CSI Attacher is the leader.
			8. Csi-Attacher in other replica should take the leadership to help creating POD's.
			9. Delete POD's, PVC's and SC.
		*/

		ginkgo.It("Verify dynamic provisioning with Thick policy when CSI-provisioner goes down", func() {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			container_name = "csi-provisioner"
			pvcCount = 5
			scParameters := make(map[string]string)
			var pvclaimsList []*v1.PersistentVolumeClaim
			var podList []*v1.Pod

			/* Get current leader Csi-Controller-Pod where CSI Provisioner is running" +
			find master node IP where this Csi-Controller-Pod is running */
			ginkgo.By("Get current Leader Csi-Controller-Pod where CSI Provisioner is running and " +
				"find the master node IP where this Csi-Controller-Pod is running")
			csi_controller_pod, k8sMasterIP, err := getK8sMasterNodeIPWhereContainerLeaderIsRunning(ctx,
				c, sshClientConfig, container_name)
			framework.Logf("CSI-Provisioner is running on elected Leader Pod %s "+
				"which is running on master node %s", csi_controller_pod, k8sMasterIP)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())

			// Get allowed topologies for Storage Class
			allowedTopologyForSC := getTopologySelector(topologyAffinityDetails, topologyCategories,
				topologyLength)

			// Create SC with Immedaite Binding mode
			ginkgo.By("Create SC with Immedaite Binding mode and thick provisioning policy")
			scParameters[scParamStoragePolicyName] = "Management Storage Policy - Regular"
			storageclass, err := createStorageClass(client, scParameters, allowedTopologyForSC,
				"", "", false, "")
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			defer func() {
				err := client.StorageV1().StorageClasses().Delete(ctx, storageclass.Name, *metav1.NewDeleteOptions(0))
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
			}()

			// Creating multiple PVCs
			ginkgo.By("Trigger multiple PVCs")
			for i := 0; i < pvcCount; i++ {
				pvclaim, err := createPVC(client, namespace, nil, "", storageclass, "")
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
				pvclaimsList = append(pvclaimsList, pvclaim)
				if i == 4 {
					ginkgo.By("Delete elected leader Csi-Controller-Pod where CSi-Provisioner is running")
					err = deleteCsiControllerPodWhereLeaderIsRunning(ctx, client, csi_controller_pod)
					gomega.Expect(err).NotTo(gomega.HaveOccurred())
				}
			}

			/* Get newly elected leader Csi-Controller-Pod where CSI Provisioner is running" +
			find master node IP where this Csi-Controller-Pod is running */
			ginkgo.By("Get newly elected leader Csi-Controller-Pod where CSI Provisioner is running and " +
				"find the master node IP where this Csi-Controller-Pod is running")
			csi_controller_pod, k8sMasterIP, err = getK8sMasterNodeIPWhereContainerLeaderIsRunning(ctx,
				c, sshClientConfig, container_name)
			framework.Logf("CSI-Provisioner is running on elected Leader Pod %s "+
				"which is running on master node %s", csi_controller_pod, k8sMasterIP)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())

			/* Get elected leader Csi-Controller-Pod where CSI Attacher is running" +
			find master node IP where this Csi-Controller-Pod is running */
			ginkgo.By("Get elected leader Csi-Controller-Pod where CSI Attacher is running and " +
				"find the master node IP where this Csi-Controller-Pod is running")
			container_name = "csi-attacher"
			csi_controller_pod, k8sMasterIP, err = getK8sMasterNodeIPWhereContainerLeaderIsRunning(ctx,
				c, sshClientConfig, container_name)
			framework.Logf("CSI-Attacher is running on elected Leader Pod %s "+
				"which is running on master node %s", csi_controller_pod, k8sMasterIP)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())

			// Verify PVC claim to be in bound phase and create POD for each PVC
			ginkgo.By("Verify PVC claim to be in bound phase and create POD for each PVC")
			for i := 0; i < len(pvclaimsList); i++ {
				var pvclaims []*v1.PersistentVolumeClaim
				// Waiting for PVC claim to be in bound phase
				pvc, err := fpv.WaitForPVClaimBoundPhase(client,
					[]*v1.PersistentVolumeClaim{pvclaimsList[i]}, framework.ClaimProvisionTimeout)
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
				gomega.Expect(pvc).NotTo(gomega.BeEmpty())

				// Get PV details
				pv := getPvFromClaim(client, pvclaimsList[i].Namespace, pvclaimsList[i].Name)

				// create Pod for each PVC
				ginkgo.By("Creating Pod")
				pvclaims = append(pvclaims, pvclaimsList[i])
				pod, err := createPod(client, namespace, nil, pvclaims, false, "")
				podList = append(podList, pod)
				gomega.Expect(err).NotTo(gomega.HaveOccurred())

				// verify volume is attached to the node
				ginkgo.By(fmt.Sprintf("Verify volume: %s is attached to the node: %s",
					pv.Spec.CSI.VolumeHandle, pod.Spec.NodeName))
				vmUUID := getNodeUUID(ctx, client, pod.Spec.NodeName)
				isDiskAttached, err := e2eVSphere.isVolumeAttachedToVM(client, pv.Spec.CSI.VolumeHandle, vmUUID)
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
				gomega.Expect(isDiskAttached).To(gomega.BeTrue(), "Volume is not attached")
				if i == 4 {
					ginkgo.By("Delete elected leader Csi-Controller-Pod where CSi-Attacher is running")
					err = deleteCsiControllerPodWhereLeaderIsRunning(ctx, client, csi_controller_pod)
					gomega.Expect(err).NotTo(gomega.HaveOccurred())
				}
			}
			defer func() {
				// cleanup code for deleting PVC
				ginkgo.By("Deleting PVC's and PV's")
				for i := 0; i < len(pvclaimsList); i++ {
					pv := getPvFromClaim(client, pvclaimsList[i].Namespace, pvclaimsList[i].Name)
					err = fpv.DeletePersistentVolumeClaim(client, pvclaimsList[i].Name, namespace)
					gomega.Expect(err).NotTo(gomega.HaveOccurred())
					framework.ExpectNoError(fpv.WaitForPersistentVolumeDeleted(client, pv.Name, poll, pollTimeoutShort))
					err = e2eVSphere.waitForCNSVolumeToBeDeleted(pv.Spec.CSI.VolumeHandle)
					gomega.Expect(err).NotTo(gomega.HaveOccurred())
				}
			}()
			defer func() {
				// cleanup code for deleting POD
				for i := 0; i < len(podList); i++ {
					ginkgo.By(fmt.Sprintf("Deleting the pod %s in namespace %s", podList[i].Name, namespace))
					err = fpod.DeletePodWithWait(client, podList[i])
					gomega.Expect(err).NotTo(gomega.HaveOccurred())
				}
				// Verify volume is detached from the node
				ginkgo.By("Verify volume is detached from the node")
				for i := 0; i < len(pvclaimsList); i++ {
					pv := getPvFromClaim(client, pvclaimsList[i].Namespace, pvclaimsList[i].Name)
					isDiskDetached, err := e2eVSphere.waitForVolumeDetachedFromNode(client,
						pv.Spec.CSI.VolumeHandle, podList[i].Spec.NodeName)
					gomega.Expect(err).NotTo(gomega.HaveOccurred())
					gomega.Expect(isDiskDetached).To(gomega.BeTrue(),
						fmt.Sprintf("Volume %q is not detached from the node", pv.Spec.CSI.VolumeHandle))
				}
			}()

			/* Get newly elected leader Csi-Controller-Pod where CSI Attacher is running" +
			find master node IP where this Csi-Controller-Pod is running */
			ginkgo.By("Get newly elected leader Csi-Controller-Pod where CSI Attacher is running and " +
				"find the master node IP where this Csi-Controller-Pod is running")
			csi_controller_pod, k8sMasterIP, err = getK8sMasterNodeIPWhereContainerLeaderIsRunning(ctx,
				c, sshClientConfig, container_name)
			framework.Logf("CSI-Attacher is running on elected Leader Pod %s "+
				"which is running on master node %s", csi_controller_pod, k8sMasterIP)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())

			/* Verify PV nde affinity and that the pods are running on appropriate nodes
			for each StatefulSet pod */
			ginkgo.By("Verify PV node affinity and that the PODS are running on appropriate node")
			for i := 0; i < len(podList); i++ {
				verifyPVnodeAffinityAndPODnodedetailsFoStandalonePodLevel5(ctx, client, podList[i],
					namespace, allowedTopologies)
			}
		})

		/*
			TESTCASE-11
			Password rotation
			Immediate +allowedTopologies (all 5 level)

			Steps//
			1. Create StorageClass and PVC
			2. Wait for PVC to be Bound
			3. Change VC password
			4. Modify the password in csi-vsphpere.conf and csi secret to reflect the new password
			5. Delete PVC
			6. Revert the secret file change
			7. Revert the password change
			8. Create New storage class
			9. Identify the process where CSI Provisioner is the leader.
			10. Create multiple PVC's and during PVC creation delete the process identified in step 9 where
			CSI Provisioner is the leader.
			11. csi-provisioner in other replica should take the leadership to help provisioning of the volume.
			12. Wait until all PVCs reach bound state
			13. Verify node affinity details on PV's
			14. Create POD using the above created PVC, make sure pod reaches running state
			15. Delete POD, PVC and SC
		*/

		ginkgo.It("Password rotation during multiple pvc creations", func() {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			container_name = "csi-provisioner"
			pvcCount = 5
			scParameters := make(map[string]string)
			var pvclaimsList []*v1.PersistentVolumeClaim
			var podList []*v1.Pod
			var pvclaims []*v1.PersistentVolumeClaim

			// Get allowed topologies for Storage Class
			allowedTopologyForSC := getTopologySelector(topologyAffinityDetails, topologyCategories,
				topologyLength)

			// create SC and PVC
			ginkgo.By("Create StorageClass and PVC")
			sc, pvc, err := createPVCAndStorageClass(client, namespace, nil,
				scParameters, diskSize, allowedTopologyForSC, "", false, "")
			gomega.Expect(err).NotTo(gomega.HaveOccurred())

			// Wait for PVC to be in Bound phase
			pvclaims = append(pvclaims, pvc)
			persistentvolumes, err := fpv.WaitForPVClaimBoundPhase(client, pvclaims,
				framework.ClaimProvisionTimeout)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			volHandle := persistentvolumes[0].Spec.CSI.VolumeHandle
			gomega.Expect(volHandle).NotTo(gomega.BeEmpty())
			defer func() {
				err := client.StorageV1().StorageClasses().Delete(ctx, sc.Name,
					*metav1.NewDeleteOptions(0))
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
			}()
			defer func() {
				err := fpv.DeletePersistentVolumeClaim(client, pvc.Name, namespace)
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
				err = e2eVSphere.waitForCNSVolumeToBeDeleted(volHandle)
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
				pvc = nil
			}()

			ginkgo.By("fetching the username and password of the current vcenter session from secret")
			secret, err := c.CoreV1().Secrets(csiSystemNamespace).Get(ctx, configSecret, metav1.GetOptions{})
			gomega.Expect(err).NotTo(gomega.HaveOccurred())

			originalConf := string(secret.Data[vSphereCSIConf])
			vsphereCfg, err := readConfigFromSecretString(originalConf)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())

			ginkgo.By(fmt.Sprintln("Changing password on the vCenter host"))
			vcAddress := e2eVSphere.Config.Global.VCenterHostname + ":" + sshdPort
			username := vsphereCfg.Global.User
			newPassword := e2eTestPassword
			err = invokeVCenterChangePassword(username, nimbusGeneratedVcPwd, newPassword, vcAddress)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())

			ginkgo.By("Modifying the password in the secret")
			vsphereCfg.Global.Password = newPassword
			modifiedConf, err := writeConfigToSecretString(vsphereCfg)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())

			ginkgo.By("Updating the secret to reflect the new password")
			secret.Data[vSphereCSIConf] = []byte(modifiedConf)
			_, err = c.CoreV1().Secrets(csiSystemNamespace).Update(ctx, secret, metav1.UpdateOptions{})
			gomega.Expect(err).NotTo(gomega.HaveOccurred())

			// Collecting csi pod logs before restarting csi driver
			collectPodLogs(ctx, client, csiSystemNamespace)

			deployment, err := c.AppsV1().Deployments(csiSystemNamespace).Get(ctx,
				vSphereCSIControllerPodNamePrefix, metav1.GetOptions{})
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			csiReplicaCount := *deployment.Spec.Replicas

			ginkgo.By("Stopping CSI driver")
			isServiceStopped, err := stopCSIPods(ctx, c)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			defer func() {
				if isServiceStopped {
					framework.Logf("Starting CSI driver")
					isServiceStopped, err = startCSIPods(ctx, c, csiReplicaCount)
					gomega.Expect(err).NotTo(gomega.HaveOccurred())
				}
			}()
			framework.Logf("Starting CSI driver")
			_, err = startCSIPods(ctx, c, csiReplicaCount)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())

			// As we are in the same vCenter session, deletion of PVC should go through
			ginkgo.By("Deleting PVC")
			err = fpv.DeletePersistentVolumeClaim(client, pvc.Name, namespace)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())

			ginkgo.By("Reverting the password change")
			err = invokeVCenterChangePassword(username, newPassword, nimbusGeneratedVcPwd, vcAddress)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())

			ginkgo.By("Reverting the secret change back to reflect the original password")
			currentSecret, err := c.CoreV1().Secrets(csiSystemNamespace).Get(ctx, configSecret, metav1.GetOptions{})
			gomega.Expect(err).NotTo(gomega.HaveOccurred())

			currentSecret.Data[vSphereCSIConf] = []byte(originalConf)
			_, err = c.CoreV1().Secrets(csiSystemNamespace).Update(ctx, currentSecret, metav1.UpdateOptions{})
			gomega.Expect(err).NotTo(gomega.HaveOccurred())

			storageclass, err := createStorageClass(client, scParameters, allowedTopologyForSC,
				"", "", true, "")
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			defer func() {
				err := client.StorageV1().StorageClasses().Delete(ctx, storageclass.Name, *metav1.NewDeleteOptions(0))
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
			}()

			/* Get current leader Csi-Controller-Pod where CSI Resizer is running" +
			find master node IP where this Csi-Controller-Pod is running */
			ginkgo.By("Get current leader Csi-Controller-Pod where CSI Provisioner is running and " +
				"find the master node IP where this Csi-Controller-Pod is running")
			csi_controller_pod, k8sMasterIP, err := getK8sMasterNodeIPWhereContainerLeaderIsRunning(ctx,
				c, sshClientConfig, container_name)
			framework.Logf("CSI-Provisioner is running on elected Leader Pod %s "+
				"which is running on master node %s", csi_controller_pod, k8sMasterIP)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())

			// Creating multiple PVCs
			ginkgo.By("Trigger multiple PVCs")
			for i := 0; i < pvcCount; i++ {
				pvclaim, err := createPVC(client, namespace, nil, "", storageclass, "")
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
				pvclaimsList = append(pvclaimsList, pvclaim)
				if i == 4 {
					ginkgo.By("Delete elected leader Csi-Controller-Pod where CSi-Provisioner is running")
					err = deleteCsiControllerPodWhereLeaderIsRunning(ctx, client, csi_controller_pod)
					gomega.Expect(err).NotTo(gomega.HaveOccurred())
				}
			}

			/* Get newly elected leader Csi-Controller-Pod where CSI Resizer is running" +
			find master node IP where this Csi-Controller-Pod is running */
			ginkgo.By("Get newly elected leader Csi-Controller-Pod where CSI Resizer is running and " +
				"find the master node IP where this Csi-Controller-Pod is running")
			csi_controller_pod, k8sMasterIP, err = getK8sMasterNodeIPWhereContainerLeaderIsRunning(ctx,
				c, sshClientConfig, container_name)
			framework.Logf("CSI-Resizer is running on elected Leader Pod %s "+
				"which is running on master node %s", csi_controller_pod, k8sMasterIP)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())

			// Verify PVC claim to be in bound phase and create POD for each PVC
			ginkgo.By("Verify PVC claim to be in bound phase and create POD for each PVC")
			for i := 0; i < len(pvclaimsList); i++ {
				var pvclaims []*v1.PersistentVolumeClaim
				// Waiting for PVC claim to be in bound phase
				pvc, err := fpv.WaitForPVClaimBoundPhase(client,
					[]*v1.PersistentVolumeClaim{pvclaimsList[i]}, framework.ClaimProvisionTimeout)
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
				gomega.Expect(pvc).NotTo(gomega.BeEmpty())

				// Get PV details
				pv := getPvFromClaim(client, pvclaimsList[i].Namespace, pvclaimsList[i].Name)

				// create Pod for each PVC
				ginkgo.By("Creating Pod")
				pvclaims = append(pvclaims, pvclaimsList[i])
				pod, err := createPod(client, namespace, nil, pvclaims, false, "")
				podList = append(podList, pod)
				gomega.Expect(err).NotTo(gomega.HaveOccurred())

				// verify volume is attached to the node
				ginkgo.By(fmt.Sprintf("Verify volume: %s is attached to the node: %s",
					pv.Spec.CSI.VolumeHandle, pod.Spec.NodeName))
				vmUUID := getNodeUUID(ctx, client, pod.Spec.NodeName)
				isDiskAttached, err := e2eVSphere.isVolumeAttachedToVM(client, pv.Spec.CSI.VolumeHandle, vmUUID)
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
				gomega.Expect(isDiskAttached).To(gomega.BeTrue(), "Volume is not attached")
			}
			defer func() {
				// cleanup code for deleting PVC
				ginkgo.By("Deleting PVC's and PV's")
				for i := 0; i < len(pvclaimsList); i++ {
					pv := getPvFromClaim(client, pvclaimsList[i].Namespace, pvclaimsList[i].Name)
					err = fpv.DeletePersistentVolumeClaim(client, pvclaimsList[i].Name, namespace)
					gomega.Expect(err).NotTo(gomega.HaveOccurred())
					framework.ExpectNoError(fpv.WaitForPersistentVolumeDeleted(client, pv.Name, poll, pollTimeoutShort))
					err = e2eVSphere.waitForCNSVolumeToBeDeleted(pv.Spec.CSI.VolumeHandle)
					gomega.Expect(err).NotTo(gomega.HaveOccurred())
				}
			}()
			defer func() {
				// cleanup code for deleting POD
				for i := 0; i < len(podList); i++ {
					ginkgo.By(fmt.Sprintf("Deleting the pod %s in namespace %s", podList[i].Name, namespace))
					err = fpod.DeletePodWithWait(client, podList[i])
					gomega.Expect(err).NotTo(gomega.HaveOccurred())
				}
				// Verify volume is detached from the node
				ginkgo.By("Verify volume is detached from the node")
				for i := 0; i < len(pvclaimsList); i++ {
					pv := getPvFromClaim(client, pvclaimsList[i].Namespace, pvclaimsList[i].Name)
					isDiskDetached, err := e2eVSphere.waitForVolumeDetachedFromNode(client,
						pv.Spec.CSI.VolumeHandle, podList[i].Spec.NodeName)
					gomega.Expect(err).NotTo(gomega.HaveOccurred())
					gomega.Expect(isDiskDetached).To(gomega.BeTrue(),
						fmt.Sprintf("Volume %q is not detached from the node", pv.Spec.CSI.VolumeHandle))
				}
			}()

			/* Verify PV nde affinity and that the pods are running on appropriate nodes
			for each StatefulSet pod */
			ginkgo.By("Verify PV node affinity and that the PODS are running on appropriate node")
			for i := 0; i < len(podList); i++ {
				verifyPVnodeAffinityAndPODnodedetailsFoStandalonePodLevel5(ctx, client, podList[i],
					namespace, allowedTopologies)
			}
		})
		// TESTCASE-6
		/*
			Verify the behaviour when CSI syncer is deleted and check fullsync
			Steps//
			1. Identify the Pod where CSI syncer is the leader.
			2. Create FCD on the shared datastore accessible to all nodes.
			3. Create PV/PVC Statically using the above FCD and using reclaim policy retain.
			At the same time kill CSI syncer container identified in the Step 1.
			4. Syncer container in other replica should take leadership and take over tasks for pushing metadata of the volumes.
			5. Create dynamic PVC's where reclaim policy is delete
			6. Verify node affinity details on PV's
			7. Create two POD's, one using static PVC's and another one using dynamic PVC's.
			8. Wait for POD's to be in running state.
			9. Delete POD's
			10. Delete PVC where reclaim policy is retain.
			11. Delete claim ref in PV's which are in released state and wait till it reaches available state.
			12. Re-create PVC using reclaim PV which is in Available state.
			13. Create two POD's, one using static PVC and another using dynamic PVC.
			14. Wait for two full sync cycle
			15. Expect all volume metadata, PVC metadata, Pod metadata should be present on the CNS.
			16. Delete the POD's , PVC's and PV's
		*/
		ginkgo.It("Verify behaviour when CSI syncer is deleted and check fullsync", func() {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			container_name = "vsphere-syncer"
			scParameters := make(map[string]string)
			var pvclaimsList []*v1.PersistentVolumeClaim
			var podList []*v1.Pod

			/* Get current leader Csi-Controller-Pod where vsphere-syncer is running" +
			find master node IP where this Csi-Controller-Pod is running */
			ginkgo.By("Get current Leader Csi-Controller-Pod where vsphere-syncer is running and " +
				"find the master node IP where this Csi-Controller-Pod is running")
			csi_controller_pod, k8sMasterIP, err := getK8sMasterNodeIPWhereContainerLeaderIsRunning(ctx,
				c, sshClientConfig, container_name)
			framework.Logf("vsphere-syncer is running on elected Leader Pod %s "+
				"which is running on master node %s", csi_controller_pod, k8sMasterIP)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())

			// Get allowed topologies for Storage Class
			allowedTopologyForSC := getTopologySelector(topologyAffinityDetails, topologyCategories,
				topologyLength)
			// Create SC with Immediate BindingMode and allowed topology set to 5 levels
			ginkgo.By("Creating Storage Class")
			scParameters["datastoreurl"] = datastoreURL
			storageclass, err := createStorageClass(client, scParameters, allowedTopologyForSC, "", "",
				false, "")
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			defer func() {
				err := client.StorageV1().StorageClasses().Delete(ctx, storageclass.Name,
					*metav1.NewDeleteOptions(0))
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
			}()

			// Creating FCD disk
			ginkgo.By("Creating FCD Disk")
			fcdID, err := e2eVSphere.createFCD(ctx, "BasicStaticFCD", diskSizeInMb,
				defaultDatastore.Reference())
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			ginkgo.By(fmt.Sprintf("Sleeping for %v seconds to allow newly created FCD:%s to sync "+
				"with pandora",
				pandoraSyncWaitTime, fcdID))
			time.Sleep(time.Duration(pandoraSyncWaitTime) * time.Second)
			// Creating label for PV. PVC will use this label as Selector to find PV.
			staticPVLabels := make(map[string]string)
			staticPVLabels["fcd-id"] = fcdID

			// Creating PV using above created SC and FCD
			ginkgo.By("Creating the PV")
			staticPv := getPersistentVolumeSpecWithStorageClassFCDNodeSelector(fcdID,
				v1.PersistentVolumeReclaimRetain, storageclass.Name, staticPVLabels,
				diskSize, allowedTopologyForSC)
			staticPv, err = client.CoreV1().PersistentVolumes().Create(ctx, staticPv, metav1.CreateOptions{})
			if err != nil {
				return
			}
			err = e2eVSphere.waitForCNSVolumeToBeCreated(staticPv.Spec.CSI.VolumeHandle)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())

			// Delete elected leader Csi-Controller-Pod where vsphere-syncer is running
			ginkgo.By("Delete elected leader Csi-Controller-Pod where vsphere-syncer is running")
			err = deleteCsiControllerPodWhereLeaderIsRunning(ctx, client, csi_controller_pod)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			ginkgo.By("Get newly elected leader Csi-Controller-Pod where CSI Syncer is running and " +
				"find the master node IP where this Csi-Controller-Pod is running")
			csi_controller_pod, k8sMasterIP, err = getK8sMasterNodeIPWhereContainerLeaderIsRunning(ctx,
				c, sshClientConfig, container_name)
			framework.Logf("vsphere-syncer is running on elected Leader Pod %s "+
				"which is running on master node %s", csi_controller_pod, k8sMasterIP)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())

			// Creating PVC using above created PV
			ginkgo.By("Creating static PVC")
			staticPvc := getPersistentVolumeClaimSpec(namespace, staticPVLabels, staticPv.Name)
			staticPvc.Spec.StorageClassName = &storageclass.Name
			staticPvc, err = client.CoreV1().PersistentVolumeClaims(namespace).Create(ctx, staticPvc,
				metav1.CreateOptions{})
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			pvclaimsList = append(pvclaimsList, staticPvc)
			// Wait for PV and PVC to Bind.
			ginkgo.By("Wait for PV and PVC to Bind")
			framework.ExpectNoError(fpv.WaitOnPVandPVC(client, framework.NewTimeoutContextWithDefaults(),
				namespace, staticPv, staticPvc))
			ginkgo.By("Verifying CNS entry is present in cache")
			_, err = e2eVSphere.queryCNSVolumeWithResult(staticPv.Spec.CSI.VolumeHandle)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())

			// Creating Pod using above created PVC
			ginkgo.By("Creating Pod using static PVC")
			var staticPvcClaims []*v1.PersistentVolumeClaim
			staticPvcClaims = append(staticPvcClaims, staticPvc)
			StaticPod, err := createPod(client, namespace, nil, staticPvcClaims, false, "")
			podList = append(podList, StaticPod)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			ginkgo.By(fmt.Sprintf("Verify volume: %s is attached to the node: %s", staticPv.Spec.CSI.VolumeHandle,
				StaticPod.Spec.NodeName))
			vmUUID := getNodeUUID(ctx, client, StaticPod.Spec.NodeName)
			isDiskAttached, err := e2eVSphere.isVolumeAttachedToVM(client, staticPv.Spec.CSI.VolumeHandle, vmUUID)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			gomega.Expect(isDiskAttached).To(gomega.BeTrue(), "Volume is not attached")

			ginkgo.By("Verify container volume metadata is present in CNS cache")
			ginkgo.By(fmt.Sprintf("Invoking QueryCNSVolume with VolumeID: %s", staticPv.Spec.CSI.VolumeHandle))
			_, err = e2eVSphere.queryCNSVolumeWithResult(staticPv.Spec.CSI.VolumeHandle)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())

			labels := []types.KeyValue{{Key: "fcd-id", Value: fcdID}}
			ginkgo.By("Verify container volume metadata is matching the one in CNS cache")
			err = verifyVolumeMetadataInCNS(&e2eVSphere, staticPv.Spec.CSI.VolumeHandle,
				staticPvc.Name, staticPv.ObjectMeta.Name, StaticPod.Name, labels...)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())

			// create dynamic PVC
			ginkgo.By("Creating dynamic PVC")
			dynamicPvc, err := createPVC(client, namespace, nil, "", storageclass, "")
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			var pvclaims []*v1.PersistentVolumeClaim
			pvclaims = append(pvclaims, dynamicPvc)
			ginkgo.By("Waiting for all claims to be in bound state")
			_, err = fpv.WaitForPVClaimBoundPhase(client, pvclaims, framework.ClaimProvisionTimeout)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			dynamicPv := getPvFromClaim(client, dynamicPvc.Namespace, dynamicPvc.Name)
			pvclaimsList = append(pvclaimsList, dynamicPvc)

			ginkgo.By("Creating Pod from dynamic PVC")
			dynamicPod, err := createPod(client, namespace, nil, pvclaims, false, "")
			podList = append(podList, dynamicPod)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			// verify volume is attached to the node
			ginkgo.By(fmt.Sprintf("Verify volume: %s is attached to the node: %s",
				dynamicPv.Spec.CSI.VolumeHandle, dynamicPod.Spec.NodeName))
			vmUUID = getNodeUUID(ctx, client, dynamicPod.Spec.NodeName)
			isDiskAttached, err = e2eVSphere.isVolumeAttachedToVM(client, dynamicPv.Spec.CSI.VolumeHandle, vmUUID)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			gomega.Expect(isDiskAttached).To(gomega.BeTrue(), "Volume is not attached")

			/* Verify PV node affinity and that the PODS are running on appropriate node as
			specified in the allowed topologies of SC */
			ginkgo.By("Verify PV node affinity and that the PODS are running on " +
				"appropriate node as specified in the allowed topologies of SC")
			for i := 0; i < len(podList); i++ {
				verifyPVnodeAffinityAndPODnodedetailsFoStandalonePodLevel5(ctx, client, podList[i], namespace,
					allowedTopologies)
			}

			// Deleting Pod's
			for i := 0; i < len(podList); i++ {
				ginkgo.By(fmt.Sprintf("Deleting the pod %s in namespace %s", podList[i].Name, namespace))
				err = fpod.DeletePodWithWait(client, podList[i])
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
			}
			// Verify volume is detached from the node
			ginkgo.By("Verify volume is detached from the node")
			for i := 0; i < len(pvclaimsList); i++ {
				pv := getPvFromClaim(client, pvclaimsList[i].Namespace, pvclaimsList[i].Name)
				isDiskDetached, err := e2eVSphere.waitForVolumeDetachedFromNode(client,
					pv.Spec.CSI.VolumeHandle, podList[i].Spec.NodeName)
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
				gomega.Expect(isDiskDetached).To(gomega.BeTrue(),
					fmt.Sprintf("Volume %q is not detached from the node", pv.Spec.CSI.VolumeHandle))
			}

			// Deleting PVC
			ginkgo.By("Delete static PVC")
			err = client.CoreV1().PersistentVolumeClaims(namespace).Delete(ctx, staticPvc.Name,
				*metav1.NewDeleteOptions(0))
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			framework.Logf("PVC %s is deleted successfully", staticPvc.Name)
			// Verify PV exist and is in released status
			ginkgo.By("Check PV exists and is released")
			staticPv, err = waitForPvToBeReleased(ctx, client, staticPv.Name)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			framework.Logf("PV status after deleting PVC: %s", staticPv.Status.Phase)
			// Remove claim from PV and check its status.
			ginkgo.By("Remove claimRef from PV")
			staticPv.Spec.ClaimRef = nil
			staticPv, err = client.CoreV1().PersistentVolumes().Update(ctx, staticPv, metav1.UpdateOptions{})
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			framework.Logf("PV status after removing claim : %s", staticPv.Status.Phase)

			// Recreate PVC with same name as created above
			ginkgo.By("ReCreating the PVC")
			newStaticPvclaim := getPersistentVolumeClaimSpec(namespace, nil, staticPv.Name)
			newStaticPvclaim.Spec.StorageClassName = &storageclass.Name
			newStaticPvclaim.Name = staticPvc.Name
			newStaticPvclaim, err = client.CoreV1().PersistentVolumeClaims(namespace).Create(ctx, newStaticPvclaim,
				metav1.CreateOptions{})
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			// Wait for newly created PVC to bind to the existing PV
			ginkgo.By("Wait for the PVC to bind the lingering pv")
			err = fpv.WaitOnPVandPVC(client, framework.NewTimeoutContextWithDefaults(), namespace, staticPv,
				newStaticPvclaim)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			defer func() {
				ginkgo.By("Deleting the PV Claim")
				framework.ExpectNoError(fpv.DeletePersistentVolumeClaim(client, newStaticPvclaim.Name, namespace),
					"Failed to delete PVC", newStaticPvclaim.Name)
				newStaticPvclaim = nil

				ginkgo.By("Deleting the PV")
				err = client.CoreV1().PersistentVolumes().Delete(ctx, staticPv.Name, *metav1.NewDeleteOptions(0))
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
			}()

			// Creating new Pod using static pvc
			ginkgo.By("Creating new Pod using static pvc")
			var newStaticPvcClaims []*v1.PersistentVolumeClaim
			newStaticPvcClaims = append(newStaticPvcClaims, newStaticPvclaim)
			newstaticPod, err := createPod(client, namespace, nil, newStaticPvcClaims, false, "")
			// verify volume is attached to the node
			ginkgo.By(fmt.Sprintf("Verify volume: %s is attached to the node: %s",
				staticPv.Spec.CSI.VolumeHandle, newstaticPod.Spec.NodeName))
			vmUUID = getNodeUUID(ctx, client, newstaticPod.Spec.NodeName)
			isDiskAttached, err = e2eVSphere.isVolumeAttachedToVM(client, staticPv.Spec.CSI.VolumeHandle, vmUUID)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			gomega.Expect(isDiskAttached).To(gomega.BeTrue(), "Volume is not attached")
			defer func() {
				ginkgo.By("Deleting the Pod")
				framework.ExpectNoError(fpod.DeletePodWithWait(client, newstaticPod), "Failed to delete pod",
					newstaticPod.Name)
				ginkgo.By(fmt.Sprintf("Verify volume is detached from the node: %s", newstaticPod.Spec.NodeName))
				isDiskDetached, err := e2eVSphere.waitForVolumeDetachedFromNode(client,
					staticPv.Spec.CSI.VolumeHandle, newstaticPod.Spec.NodeName)
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
				gomega.Expect(isDiskDetached).To(gomega.BeTrue(), "Volume is not detached from the node")
			}()

			ginkgo.By("Creating new Pod using dynamic pvc")
			newDynamicPod, err := createPod(client, namespace, nil, pvclaims, false, "")
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			// verify volume is attached to the node
			ginkgo.By(fmt.Sprintf("Verify volume: %s is attached to the node: %s",
				dynamicPv.Spec.CSI.VolumeHandle, newDynamicPod.Spec.NodeName))
			vmUUID = getNodeUUID(ctx, client, newDynamicPod.Spec.NodeName)
			isDiskAttached, err = e2eVSphere.isVolumeAttachedToVM(client, dynamicPv.Spec.CSI.VolumeHandle, vmUUID)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			gomega.Expect(isDiskAttached).To(gomega.BeTrue(), "Volume is not attached")
			defer func() {
				ginkgo.By("Deleting the Pod")
				framework.ExpectNoError(fpod.DeletePodWithWait(client, newDynamicPod), "Failed to delete pod",
					newDynamicPod.Name)
				ginkgo.By(fmt.Sprintf("Verify volume is detached from the node: %s", newDynamicPod.Spec.NodeName))
				isDiskDetached, err := e2eVSphere.waitForVolumeDetachedFromNode(client,
					staticPv.Spec.CSI.VolumeHandle, newDynamicPod.Spec.NodeName)
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
				gomega.Expect(isDiskDetached).To(gomega.BeTrue(), "Volume is not detached from the node")
			}()

			ginkgo.By(fmt.Sprintf("Sleeping for %v seconds to allow full sync finish", fullSyncWaitTime))
			time.Sleep(time.Duration(fullSyncWaitTime) * time.Second)

			// Verify volume metadata for static POD, PVC and PV
			ginkgo.By("Verify volume metadata for static POD, PVC and PV")
			err = waitAndVerifyCnsVolumeMetadata(staticPv.Spec.CSI.VolumeHandle, newStaticPvclaim, staticPv, newstaticPod)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())

			// Verify volume metadata for dynamic POD, PVC and PV
			ginkgo.By("Verify volume metadata for dynamic POD, PVC and PV")
			err = waitAndVerifyCnsVolumeMetadata(dynamicPv.Spec.CSI.VolumeHandle, dynamicPvc, dynamicPv, newDynamicPod)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		})

		/*
		   TESTCASE-9
		   Verify Label update when syncer container goes down
		   Immediate +allowedTopologies (all 5 level)

		   Steps//
		   1. Identify the process where CSI syncer is the leader.
		   2. Create a Storage class.
		   3. Create Multiple dynamic PVC's.
		   4. Add labels to PVC's and PV's.
		   5. Delete the CSI process identified in the step 1, where CSI syncer is the leader.
		   6. vsphere-syncer in another replica should take the leadership to help label update.
		   7. Verify CNS metadata for PVC's to check newly added labels.
		   8. Identify the process where CSI syncer is the leader
		   9. Delete labels from PVC's and PV's
		   10. Delete the CSI process identified in the step 8, where CSI syncer is the leader.
		   11. Verify CNS metadata for PVC's and PV's , Make sure label entries should got removed
		*/

		ginkgo.It("Verify Label update when syncer container goes down", func() {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			container_name = "vsphere-syncer"
			pvcCount = 5
			labelKey := "volId"
			labelValue := "pvcVolume"
			labels := make(map[string]string)
			labels[labelKey] = labelValue

			/* Get current leader Csi-Controller-Pod where CSI Syncer is running" +
			   find master node IP where this Csi-Controller-Pod is running */
			ginkgo.By("Get current Leader Csi-Controller-Pod where CSI Syncer is running and " +
				"find the master node IP where this Csi-Controller-Pod is running")
			csi_controller_pod, k8sMasterIP, err := getK8sMasterNodeIPWhereContainerLeaderIsRunning(ctx,
				c, sshClientConfig, container_name)
			framework.Logf("vsphere-syncer is running on elected Leader Pod %s "+
				"which is running on master node %s", csi_controller_pod, k8sMasterIP)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())

			// Get allowed topologies for Storage Class
			allowedTopologyForSC := getTopologySelector(topologyAffinityDetails, topologyCategories,
				topologyLength)

			// Create SC with Immedaite Binding mode
			ginkgo.By("Create SC with Immedaite Binding mode and allowVolumeExpansion set to true")
			storageclass, err := createStorageClass(client, nil, allowedTopologyForSC,
				"", "", false, "")
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			defer func() {
				err := client.StorageV1().StorageClasses().Delete(ctx, storageclass.Name, *metav1.NewDeleteOptions(0))
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
			}()

			// Creating multiple PVCs
			ginkgo.By("Trigger multiple PVCs")
			pvclaimsList := createMultiplePVCsInParallel(ctx, client, namespace, storageclass, pvcCount)
			defer func() {
				// cleanup code for deleting PVC
				ginkgo.By("Deleting PVC's and PV's")
				for i := 0; i < len(pvclaimsList); i++ {
					pv := getPvFromClaim(client, pvclaimsList[i].Namespace, pvclaimsList[i].Name)
					err = fpv.DeletePersistentVolumeClaim(client, pvclaimsList[i].Name, namespace)
					gomega.Expect(err).NotTo(gomega.HaveOccurred())
					framework.ExpectNoError(fpv.WaitForPersistentVolumeDeleted(client, pv.Name, poll, pollTimeoutShort))
					err = e2eVSphere.waitForCNSVolumeToBeDeleted(pv.Spec.CSI.VolumeHandle)
					gomega.Expect(err).NotTo(gomega.HaveOccurred())
				}
			}()

			// Verify PVC claim to be in bound phase
			ginkgo.By("Verify PVC claim to be in bound phase")
			for i := 0; i < len(pvclaimsList); i++ {
				var pvc *v1.PersistentVolumeClaim
				pvc = pvclaimsList[i]
				// Waiting for PVC claim to be in bound phase
				pvs, err := fpv.WaitForPVClaimBoundPhase(client,
					[]*v1.PersistentVolumeClaim{pvc}, framework.ClaimProvisionTimeout)
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
				gomega.Expect(pvs).NotTo(gomega.BeEmpty())
				pv := pvs[0]

				labels := make(map[string]string)
				labels[labelKey] = labelValue

				ginkgo.By(fmt.Sprintf("Updating labels %+v for pvc %s in namespace %s", labels, pvc.Name, pvc.Namespace))
				pvc, err = client.CoreV1().PersistentVolumeClaims(namespace).Get(ctx, pvc.Name, metav1.GetOptions{})
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
				pvc.Labels = labels
				_, err = client.CoreV1().PersistentVolumeClaims(namespace).Update(ctx, pvc, metav1.UpdateOptions{})
				gomega.Expect(err).NotTo(gomega.HaveOccurred())

				ginkgo.By(fmt.Sprintf("Updating labels %+v for pv %s", labels, pv.Name))
				pv.Labels = labels

				_, err = client.CoreV1().PersistentVolumes().Update(ctx, pv, metav1.UpdateOptions{})
				gomega.Expect(err).NotTo(gomega.HaveOccurred())

				ginkgo.By(fmt.Sprintf("Waiting for labels %+v to be updated for pvc %s in namespace %s",
					labels, pvc.Name, pvc.Namespace))
				err = e2eVSphere.waitForLabelsToBeUpdated(pv.Spec.CSI.VolumeHandle,
					labels, string(cnstypes.CnsKubernetesEntityTypePVC), pvc.Name, pvc.Namespace)
				gomega.Expect(err).NotTo(gomega.HaveOccurred())

				ginkgo.By(fmt.Sprintf("Waiting for labels %+v to be updated for pv %s", labels, pv.Name))
				err = e2eVSphere.waitForLabelsToBeUpdated(pv.Spec.CSI.VolumeHandle,
					labels, string(cnstypes.CnsKubernetesEntityTypePV), pv.Name, pv.Namespace)
				gomega.Expect(err).NotTo(gomega.HaveOccurred())

				if i == 4 {
					ginkgo.By("Delete elected leader CSi-Controller-Pod where vsphere-syncer is running")
					err = deleteCsiControllerPodWhereLeaderIsRunning(ctx, client, csi_controller_pod)
					gomega.Expect(err).NotTo(gomega.HaveOccurred())

					/* Get newly elected current leader Csi-Controller-Pod where CSI Syncer is running" +
					find new master node IP where this Csi-Controller-Pod is running */
					ginkgo.By("Get newly elected current Leader Csi-Controller-Pod where CSI Syncer is " +
						"running and find the master node IP where this Csi-Controller-Pod is running")
					csi_controller_pod, k8sMasterIP, err = getK8sMasterNodeIPWhereContainerLeaderIsRunning(ctx,
						c, sshClientConfig, container_name)
					framework.Logf("vsphere-syncer is running on elected Leader Pod %s "+
						"which is running on master node %s", csi_controller_pod, k8sMasterIP)
					gomega.Expect(err).NotTo(gomega.HaveOccurred())
				}
			}
			for i := 0; i < len(pvclaimsList); i++ {
				var pvc *v1.PersistentVolumeClaim
				pvc = pvclaimsList[i]
				pvs, err := fpv.WaitForPVClaimBoundPhase(client,
					[]*v1.PersistentVolumeClaim{pvc}, framework.ClaimProvisionTimeout)
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
				gomega.Expect(pvs).NotTo(gomega.BeEmpty())
				pv := pvs[0]

				ginkgo.By(fmt.Sprintf("Fetching updated pvc %s in namespace %s", pvc.Name, pvc.Namespace))
				pvc, err = client.CoreV1().PersistentVolumeClaims(namespace).Get(ctx, pvc.Name, metav1.GetOptions{})
				gomega.Expect(err).NotTo(gomega.HaveOccurred())

				ginkgo.By(fmt.Sprintf("Deleting labels %+v for pvc %s in namespace %s", labels, pvc.Name, pvc.Namespace))
				pvc.Labels = make(map[string]string)
				_, err = client.CoreV1().PersistentVolumeClaims(namespace).Update(ctx, pvc, metav1.UpdateOptions{})
				gomega.Expect(err).NotTo(gomega.HaveOccurred())

				ginkgo.By(fmt.Sprintf("Waiting for labels %+v to be deleted for pvc %s in namespace %s",
					labels, pvc.Name, pvc.Namespace))
				err = e2eVSphere.waitForLabelsToBeUpdated(pv.Spec.CSI.VolumeHandle,
					pvc.Labels, string(cnstypes.CnsKubernetesEntityTypePVC), pvc.Name, pvc.Namespace)
				gomega.Expect(err).NotTo(gomega.HaveOccurred())

				ginkgo.By(fmt.Sprintf("Fetching updated pv %s", pv.Name))
				pv, err = client.CoreV1().PersistentVolumes().Get(ctx, pv.Name, metav1.GetOptions{})
				gomega.Expect(err).NotTo(gomega.HaveOccurred())

				ginkgo.By(fmt.Sprintf("Deleting labels %+v for pv %s", labels, pv.Name))
				pv.Labels = make(map[string]string)
				_, err = client.CoreV1().PersistentVolumes().Update(ctx, pv, metav1.UpdateOptions{})
				gomega.Expect(err).NotTo(gomega.HaveOccurred())

				ginkgo.By(fmt.Sprintf("Waiting for labels %+v to be deleted for pv %s", labels, pv.Name))
				err = e2eVSphere.waitForLabelsToBeUpdated(pv.Spec.CSI.VolumeHandle,
					pv.Labels, string(cnstypes.CnsKubernetesEntityTypePV), pv.Name, pv.Namespace)
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
			}
		})
	})
