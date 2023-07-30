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

	ginkgo "github.com/onsi/ginkgo/v2"
	"github.com/onsi/gomega"
	"github.com/vmware/govmomi/object"
	"golang.org/x/crypto/ssh"
	v1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	clientset "k8s.io/client-go/kubernetes"
	"k8s.io/kubernetes/test/e2e/framework"
	fnodes "k8s.io/kubernetes/test/e2e/framework/node"
	fss "k8s.io/kubernetes/test/e2e/framework/statefulset"
	admissionapi "k8s.io/pod-security-admission/api"
)

var _ = ginkgo.Describe("[csi-multi-vc-topology] Multi-VC-Operation-Storm", func() {
	f := framework.NewDefaultFramework("csi-multi-vc")
	f.NamespacePodSecurityEnforceLevel = admissionapi.LevelPrivileged
	var (
		client                      clientset.Interface
		namespace                   string
		allowedTopologies           []v1.TopologySelectorLabelRequirement
		sshClientConfig             *ssh.ClientConfig
		nimbusGeneratedK8sVmPwd     string
		statefulSetReplicaCount     int32
		k8sVersion                  string
		stsScaleUp                  bool
		stsScaleDown                bool
		verifyTopologyAffinity      bool
		parallelStatefulSetCreation bool
		scaleUpReplicaCount         int32
		scaleDownReplicaCount       int32
		multiVCSetupType            string
		hostsInCluster              []*object.HostSystem
		topologyClusterList         []string
		csiNamespace                string
		csiReplicas                 int32
		ClusterdatastoreListVC      []map[string]string
		ClusterdatastoreListVC1     map[string]string
		ClusterdatastoreListVC2     map[string]string
		ClusterdatastoreListVC3     map[string]string
		allMasterIps                []string
		masterIp                    string
		dataCenters                 []*object.Datacenter
		clusterComputeResource      []*object.ClusterComputeResource
		nodeList                    *v1.NodeList
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

		nodeList, err := fnodes.GetReadySchedulableNodes(f.ClientSet)
		framework.ExpectNoError(err, "Unable to find ready and schedulable Node")
		if !(len(nodeList.Items) > 0) {
			framework.Failf("Unable to find ready and schedulable Node")
		}

		topologyMap := GetAndExpectStringEnvVar(topologyMap)
		allowedTopologies = createAllowedTopolgies(topologyMap, topologyLength)
		nimbusGeneratedK8sVmPwd = GetAndExpectStringEnvVar(nimbusK8sVmPwd)
		multiVCSetupType = GetAndExpectStringEnvVar(envMultiVCSetupType)

		//clusterComputeResource, _, err := getClusterName(ctx, &e2eVSphere)

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

		// fetching datacenter details
		dataCenters, err = multiVCe2eVSphere.getAllDatacentersForMultiVC(ctx)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		csiNamespace = GetAndExpectStringEnvVar(envCSINamespace)
		csiDeployment, err := client.AppsV1().Deployments(csiNamespace).Get(
			ctx, vSphereCSIControllerPodNamePrefix, metav1.GetOptions{})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		csiReplicas = *csiDeployment.Spec.Replicas

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
			clusterComputeResource[0], true)
		ClusterdatastoreListVC = append(ClusterdatastoreListVC, ClusterdatastoreListVC1,
			ClusterdatastoreListVC2, ClusterdatastoreListVC3)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

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

		framework.Logf("Perform cleanup of any left over stale PVs")
		allPvs, err := client.CoreV1().PersistentVolumes().List(ctx, metav1.ListOptions{})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		for _, pv := range allPvs.Items {
			err := client.CoreV1().PersistentVolumes().Delete(ctx, pv.Name, metav1.DeleteOptions{})
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}
	})

	/*
	   Bring down ESX + create 50 PVCs + kill CSI provisioner and CSi-attacher  + Put one ESX in maintenance mode

	   1. Create SC with default values so that it is allowed to provision volume on any AZ's
	   2. Identify the CSI-Controller-Pod where CSI Provisioner, CSI-Attacher are running
	   3. Create 5 Statefulset each with 10 replicas
	   4. Bring down few esx hosts in VC2 as well as VC3
	   5. While the Statefulsets is creating PVCs and Pods, kill CSI Provisioner container, CSI-attacher
	   identified in the step 1
	   6. Wait for all the statefulsets to come up
	   7. Scale up one statefulset to 15 replica
	   8. Scale down any one statefulset to replica 1
	   9. Restart the CSI-controller PODS
	   10. Verify the node affinity on all the PV's
	   12. Bring up all the ESX hosts which were powered off in step 4
	   13. Bring down one datastore
	   14. Scale up/down the statefulset
	   15. Make sure common validation points are met on PV,PVC and POD
	   16. Verify the CNS entry of few CSi volumes
	   17. Clean up  the data
	*/

	ginkgo.It("OSCreate statefulset pods in scale and in between bring down datatsore, esxi hosts "+
		"and kill containers", func() {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		sts_count := 5
		statefulSetReplicaCount = 10
		noOfHostToBringDown := 1
		isDatastoreInMaintenanceMode := false
		var powerOffHostsLists []string

		ginkgo.By("Create SC with allowed topology spread across multiple VCs")
		storageclass, err := createStorageClass(client, nil, nil, "", "", false, "nginx-sc")
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		defer func() {
			err := client.StorageV1().StorageClasses().Delete(ctx, storageclass.Name, *metav1.NewDeleteOptions(0))
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}()

		ginkgo.By("Get current leader where CSI-Provisioner, CSI-Attacher and " +
			"Vsphere-Syncer is running and find the master node IP where these containers are running")
		leader1, k8sMasterIP1, err := getK8sMasterNodeIPWhereContainerLeaderIsRunning(ctx,
			client, sshClientConfig, provisionerContainerName)
		framework.Logf("CSI-Provisioner is running on Leader Pod %s "+
			"which is running on master node %s", leader1, k8sMasterIP1)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		leader2, k8sMasterIP2, err := getK8sMasterNodeIPWhereContainerLeaderIsRunning(ctx,
			client, sshClientConfig, attacherContainerName)
		framework.Logf("CSI-Attacher is running on Leader Pod %s "+
			"which is running on master node %s", leader2, k8sMasterIP2)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Creating service")
		service := CreateService(namespace, client)
		defer func() {
			deleteService(namespace, client, service)
		}()

		ginkgo.By("Creating multiple StatefulSets specs in parallel")
		statefulSets := createParallelStatefulSetSpec(namespace, sts_count, statefulSetReplicaCount)
		defer func() {
			deleteAllStsAndPodsPVCsInNamespace(ctx, client, namespace)
		}()

		ginkgo.By("Trigger multiple StatefulSets creation in parallel. During StatefulSets " +
			"creation, kill CSI-Provisioner, CSI-Attacher container in between")
		var wg sync.WaitGroup
		wg.Add(sts_count)
		for i := 0; i < len(statefulSets); i++ {
			go createParallelStatefulSets(client, namespace, statefulSets[i], statefulSetReplicaCount, &wg)
			if i == 1 {
				ginkgo.By("Kill CSI-Provisioner container")
				err = execDockerPauseNKillOnContainer(sshClientConfig, k8sMasterIP1, provisionerContainerName,
					k8sVersion)
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
			}

			if i == 2 {
				ginkgo.By("Kill CSI-Attacher container")
				err = execDockerPauseNKillOnContainer(sshClientConfig, k8sMasterIP2, attacherContainerName,
					k8sVersion)
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
			}
		}
		wg.Wait()

		if multiVCSetupType == "multi-2vc-setup" {
			ginkgo.By("Bring down 1 ESXi host each in VC1 and  VC2")
			for i := 0; i <= 1; i++ {
				if i == 0 {
					readVcEsxIpsViaTestbedInfoJson(GetAndExpectStringEnvVar(envTestbedInfoJsonPathVC1))
				}
				if i == 1 {
					readVcEsxIpsViaTestbedInfoJson(GetAndExpectStringEnvVar(envTestbedInfoJsonPathVC2))
				}
				clusterComputeResource, _, err = getClusterNameForMultiVC(ctx, &multiVCe2eVSphere, i)
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
				elements := strings.Split(clusterComputeResource[0].InventoryPath, "/")
				clusterName := elements[len(elements)-1]
				hostsInCluster = getHostsByClusterName(ctx, clusterComputeResource, clusterName)
				powerOffHostsList := powerOffEsxiHostsInMultiVcCluster(ctx, &multiVCe2eVSphere,
					noOfHostToBringDown, hostsInCluster)
				powerOffHostsLists = append(powerOffHostsLists, powerOffHostsList...)
			}
		} else if multiVCSetupType == "multi-3vc-setup" {
			ginkgo.By("Bring down 1 ESXi host each in VC2 and  VC3")
			for i := 1; i <= 2; i++ {
				if i == 1 {
					readVcEsxIpsViaTestbedInfoJson(GetAndExpectStringEnvVar(envTestbedInfoJsonPathVC2))
				}
				if i == 2 {
					readVcEsxIpsViaTestbedInfoJson(GetAndExpectStringEnvVar(envTestbedInfoJsonPathVC3))
				}
				for i := 1; i < len(topologyClusterList); i++ {
					clusterComputeResource, _, err = getClusterNameForMultiVC(ctx, &multiVCe2eVSphere, i)
					gomega.Expect(err).NotTo(gomega.HaveOccurred())
					elements := strings.Split(clusterComputeResource[0].InventoryPath, "/")
					clusterName := elements[len(elements)-1]
					fmt.Println(clusterName)
					hostsInCluster = getHostsByClusterName(ctx, clusterComputeResource, clusterName)
					powerOffHostsList := powerOffEsxiHostsInMultiVcCluster(ctx, &multiVCe2eVSphere,
						noOfHostToBringDown, hostsInCluster)
					powerOffHostsLists = append(powerOffHostsLists, powerOffHostsList...)
				}
			}
		}
		defer func() {
			ginkgo.By("Bring up  ESXi host which were powered off")
			if multiVCSetupType == "multi-2vc-setup" {
				for i := 0; i < len(powerOffHostsLists); i++ {
					if i == 0 {
						readVcEsxIpsViaTestbedInfoJson(GetAndExpectStringEnvVar(envTestbedInfoJsonPathVC1))
					}
					if i == 1 {
						readVcEsxIpsViaTestbedInfoJson(GetAndExpectStringEnvVar(envTestbedInfoJsonPathVC2))
					}
					powerOnEsxiHostByCluster(powerOffHostsLists[i])
				}
			} else if multiVCSetupType == "multi-3vc-setup" {
				for i := 0; i < len(powerOffHostsLists); i++ {
					if i == 0 {
						readVcEsxIpsViaTestbedInfoJson(GetAndExpectStringEnvVar(envTestbedInfoJsonPathVC2))
					}
					if i == 1 {
						readVcEsxIpsViaTestbedInfoJson(GetAndExpectStringEnvVar(envTestbedInfoJsonPathVC3))
					}
					powerOnEsxiHostByCluster(powerOffHostsLists[i])
				}
			}
		}()

		/* Note: In 2-VC multi-setup, no node vms migration will take place when the partial or full site goes down
		and hence the StatefulSet Pods will remain stuck in Pending or Terminating state because the node vms
		are placed in local datatsore with limited storage capacity therefore, no vms migration will take place

		Here, to verify if Sts Pods are stuck in Pending/Terminating state with valid reason or not, we will be
		verifying the Pod Events and will look for event reason "NodeNotReady"
		*/

		if multiVCSetupType == "multi-2vc-setup" {
			for i := 0; i < len(statefulSets); i++ {
				verifyStsPodsRelicaStatusWhenSiteIsDown(client, statefulSets[i], statefulSetReplicaCount,
					"multi-2vc-setup", namespace)
			}
		} else {
			ginkgo.By("Waiting for StatefulSets Pods to be in Ready State")
			err = waitForStsPodsToBeInReadyRunningState(ctx, client, namespace, statefulSets)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}

		ginkgo.By("Verify PV node affinity and that the PODS are running on appropriate node")
		for i := 0; i < len(statefulSets); i++ {
			verifyPVnodeAffinityAndPODnodedetailsForStatefulsetsLevel5(ctx, client,
				statefulSets[i], namespace, allowedTopologies, parallelStatefulSetCreation, true)
		}

		ginkgo.By("Perform scaleup/scaledown operation on statefulset and " +
			"verify pv and pod affinity details")
		for i := 0; i < len(statefulSets)-1; i++ {
			if i == 0 {
				stsScaleDown = false
				scaleUpReplicaCount = 15
				framework.Logf("Scale up StatefulSet1 replica count to 15")
				performScalingOnStatefulSetAndVerifyPvNodeAffinity(ctx, client, scaleUpReplicaCount,
					scaleDownReplicaCount, statefulSets[i], parallelStatefulSetCreation, namespace,
					allowedTopologies, stsScaleUp, stsScaleDown, verifyTopologyAffinity)
			}
			if i == 1 {
				scaleDownReplicaCount = 1
				stsScaleUp = false
				framework.Logf("Scale up StatefulSet2 replica count to 9 and in between " +
					"kill vsphere syncer container")
				performScalingOnStatefulSetAndVerifyPvNodeAffinity(ctx, client, scaleUpReplicaCount,
					scaleDownReplicaCount, statefulSets[i], parallelStatefulSetCreation, namespace,
					allowedTopologies, stsScaleUp, stsScaleDown, verifyTopologyAffinity)
			}
		}

		ginkgo.By("Restart CSI driver")
		restartSuccess, err := restartCSIDriver(ctx, client, csiNamespace, csiReplicas)
		gomega.Expect(restartSuccess).To(gomega.BeTrue(), "csi driver restart not successful")
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		// Bring up
		ginkgo.By("Bring up all ESXi host which were powered off")
		for i := 0; i < len(powerOffHostsLists); i++ {
			powerOnEsxiHostByCluster(powerOffHostsLists[i])
		}

		// Wait for k8s cluster to be healthy
		ginkgo.By("Wait for k8s cluster to be healthy")
		wait4AllK8sNodesToBeUp(ctx, client, nodeList)
		err = waitForAllNodes2BeReady(ctx, client, pollTimeout*4)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		framework.Logf("Put preferred datastore in maintenance mode")
		err = putDatastoreInMaintenanceMode(masterIp, sshClientConfig, dataCenters, "", true, 2)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		isDatastoreInMaintenanceMode = true
		defer func() {
			if isDatastoreInMaintenanceMode {
				err = exitDatastoreFromMaintenanceMode(masterIp, sshClientConfig, dataCenters, "", true, 2)
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
			}
		}()

		for i := 0; i < len(statefulSets); i++ {
			ginkgo.By("Perform scaleup/scaledown operation on statefulset and " +
				"verify pv and pod affinity details")
			if i == 0 {
				stsScaleUp = false
				scaleDownReplicaCount = 3
				framework.Logf("Scale down StatefulSet1 replica count to 3")
				performScalingOnStatefulSetAndVerifyPvNodeAffinity(ctx, client, scaleUpReplicaCount,
					scaleDownReplicaCount, statefulSets[i], parallelStatefulSetCreation, namespace,
					allowedTopologies, stsScaleUp, stsScaleDown, verifyTopologyAffinity)
			}
			if i == 1 {
				scaleUpReplicaCount = 9
				stsScaleDown = false
				framework.Logf("Scale up StatefulSet2 replica count to 9")
				performScalingOnStatefulSetAndVerifyPvNodeAffinity(ctx, client, scaleUpReplicaCount,
					scaleDownReplicaCount, statefulSets[i], parallelStatefulSetCreation, namespace,
					allowedTopologies, stsScaleUp, stsScaleDown, verifyTopologyAffinity)
			}
			if i == 2 {
				scaleUpReplicaCount = 4
				scaleDownReplicaCount = 5
				framework.Logf("Scale up StatefulSet3 replica count to 4 and " +
					"Scale down StatefulSet1 replica count to 5")
				performScalingOnStatefulSetAndVerifyPvNodeAffinity(ctx, client, scaleUpReplicaCount,
					scaleDownReplicaCount, statefulSets[i], parallelStatefulSetCreation, namespace,
					allowedTopologies, stsScaleUp, stsScaleDown, verifyTopologyAffinity)
			}
		}
	})
})
