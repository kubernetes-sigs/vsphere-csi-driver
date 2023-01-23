/*
	Copyright 2020 The Kubernetes Authors.

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
	"time"

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
	fpod "k8s.io/kubernetes/test/e2e/framework/pod"
	fpv "k8s.io/kubernetes/test/e2e/framework/pv"
	fss "k8s.io/kubernetes/test/e2e/framework/statefulset"
	admissionapi "k8s.io/pod-security-admission/api"
)

var _ = ginkgo.Describe("[Disruptive-Preferential-Topology] Preferential-Topology-Disruptive-Provisioning", func() {
	f := framework.NewDefaultFramework("preferential-topology-aware-provisioning")
	f.NamespacePodSecurityEnforceLevel = admissionapi.LevelPrivileged
	var (
		client                         clientset.Interface
		namespace                      string
		allowedTopologies              []v1.TopologySelectorLabelRequirement
		topologyAffinityDetails        map[string][]string
		topologyCategories             []string
		topologyLength                 int
		preferredDatastoreChosen       int
		shareddatastoreListMap         map[string]string
		nonShareddatastoreListMapRack1 map[string]string
		nonShareddatastoreListMapRack2 map[string]string
		allMasterIps                   []string
		masterIp                       string
		rack1DatastoreListMap          map[string]string
		rack2DatastoreListMap          map[string]string
		dataCenters                    []*object.Datacenter
		clusters                       []string
		preferredDatastorePaths        []string
		allowedTopologyRacks           []string
		err                            error
		nodeList                       *v1.NodeList
		allowedTopologyForRack1        []v1.TopologySelectorLabelRequirement
		allowedTopologyForRack2        []v1.TopologySelectorLabelRequirement
		leafNode                       int
		leafNodeTag0                   int
		leafNodeTag1                   int
		workerInitialAlias             []string
		dsNameToPerformNimbusOps       []string
		csiReplicas                    int32
		csiNamespace                   string
		sshClientConfig                *ssh.ClientConfig
		nimbusGeneratedK8sVmPwd        string
	)
	ginkgo.BeforeEach(func() {
		var cancel context.CancelFunc
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		client = f.ClientSet
		namespace = f.Namespace.Name
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

		// read testbedinfo json file
		readVcEsxIpsViaTestbedInfoJson(GetAndExpectStringEnvVar(envTestbedInfoJsonPath))
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
		dataCenters, err = e2eVSphere.getAllDatacenters(ctx)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		// fetching cluster details
		clusters, err = getTopologyLevel5ClusterGroupNames(masterIp, sshClientConfig, dataCenters)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		// creating level-5 allowed topology map
		topologyLength, leafNode, leafNodeTag0, leafNodeTag1, _ = 5, 4, 0, 1, 2
		topologyMap := GetAndExpectStringEnvVar(topologyMap)
		topologyAffinityDetails, topologyCategories = createTopologyMapLevel5(topologyMap,
			topologyLength)
		allowedTopologies = createAllowedTopolgies(topologyMap, topologyLength)

		csiNamespace = GetAndExpectStringEnvVar(envCSINamespace)
		csiDeployment, err := client.AppsV1().Deployments(csiNamespace).Get(
			ctx, vSphereCSIControllerPodNamePrefix, metav1.GetOptions{})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		csiReplicas = *csiDeployment.Spec.Replicas

		clusterWorkerMap := GetAndExpectStringEnvVar(workerClusterMap)
		_, workerInitialAlias = createTopologyMapLevel5(clusterWorkerMap, topologyLength)
		datastoreClusterMap := GetAndExpectStringEnvVar(datastoreClusterMap)
		_, dsNameToPerformNimbusOps = createTopologyMapLevel5(datastoreClusterMap, topologyLength)

		// fetching list of datatstores shared between vm's
		shareddatastoreListMap, err = getListOfSharedDatastoresBetweenVMs(masterIp, sshClientConfig, dataCenters)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		// Fetching list of datastores available in different racks
		rack1DatastoreListMap, err = getListOfDatastoresByClusterName(masterIp, sshClientConfig, clusters[0])
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		rack2DatastoreListMap, err = getListOfDatastoresByClusterName(masterIp, sshClientConfig, clusters[1])
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		// Fetching list of datastores which is specific to each rack
		nonShareddatastoreListMapRack1 = getNonSharedDatastoresInCluster(rack1DatastoreListMap,
			shareddatastoreListMap)
		nonShareddatastoreListMapRack2 = getNonSharedDatastoresInCluster(rack2DatastoreListMap,
			shareddatastoreListMap)

		// fetching cluster topology racks level
		allowedTopologyRacks = nil
		allowedTopologyForSC := getTopologySelector(topologyAffinityDetails, topologyCategories,
			topologyLength)
		for i := 4; i < len(allowedTopologyForSC); i++ {
			for j := 0; j < len(allowedTopologyForSC[i].Values); j++ {
				allowedTopologyRacks = append(allowedTopologyRacks, allowedTopologyForSC[i].Values[j])
			}
		}

		// Get different allowed topologies required for creating Storage Class
		allowedTopologyForRack1 = getTopologySelector(topologyAffinityDetails, topologyCategories,
			topologyLength, leafNode, leafNodeTag0)
		allowedTopologyForRack2 = getTopologySelector(topologyAffinityDetails, topologyCategories,
			topologyLength, leafNode, leafNodeTag1)

		//set preferred datatsore time interval
		setPreferredDatastoreTimeInterval(client, ctx, csiNamespace, namespace, csiReplicas)

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
		framework.Logf("Perform preferred datastore tags cleanup after test completion")
		err = deleteTagCreatedForPreferredDatastore(masterIp, sshClientConfig, allowedTopologyRacks)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		framework.Logf("Recreate preferred datastore tags post cleanup")
		err = createTagForPreferredDatastore(masterIp, sshClientConfig, allowedTopologyRacks)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

	})

	/*
		Testcase-17:
			Bring down Partial site rack-2(cluster-2)

			Steps
			1. Assign Tag rack-1, Category "cns.vmware.topology-preferred-datastores" to datastore specific
			to rack-1. (ex - NFS-1)
			2. Assign Tag rack-2, Category "cns.vmware.topology-preferred-datastores" to datastore specific
			to rack-2 (ex - NFS-2)
			3. Assign Tag rack-3, Category "cns.vmware.topology-preferred-datastores" to datastore shared
			across all racks (ex - sharedVMFS-12)
			4. Create SC with Immediate binding mode and allowed topologies set to all racks
			5. Create 3 statefulsets each with replica count 3
			6. Wait for all the workload pods to reach running state
			7. POD should come up on the node which is present in the same details as mentioned in storage class
			8. Verify that the PV's have specified node affinity details.
			9. Verify volume should be provisioned on the tagged preferred datastore
			10. Change datastore preference for all the 3 racks.
			11. In rack-2 bring down 2 ESX's hosts
			12. Wait for some time and verify that all the node VM's are bought up on the ESX which was in ON
			state in the respective site.
			13. Verify that All the workload POD's are up and running
			14. Scale up StatefulSet to 10 replica's
			15. Verify volume provisioning should be on the newly tagged preferred datatstore.
			16. Wait for some time and make sure all 10 replica's are in running state
			17. Remove tags from previosly preferred datatsore and assign new tag to single datastore
			specific to rack-2
			18. Scale down the StatefulSet to 5 replicas
			19. Verify scaledown operation went successful.
			20. Bring up all the ESX hosts
			21. Perform scaleup operation again and verify that the volume should be provisioned on the newly
			preferred datastore.
			22. Wait for testbed to be back to normal
			23. Verify that all the POD's and sts are up and running
			24. Verify All the PV's has proper node affinity details and also make sure POD's are running on the
			same node where respective PVC is created
			25. Make sure K8s cluster  is healthy
			26. Perform cleanup. Delete StatefulSet, PVC and PV, SC.
	*/
	ginkgo.It("Bring down partial site when multiple preferred datatsores are tagged", func() {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		preferredDatastoreChosen = 1
		preferredDatastorePaths = nil
		var preferredDatastorePathsToDel []string
		allDatastoresListMap := make(map[string]string)
		var datastorestMap []map[string]string

		// get cluster details
		clusterComputeResource, _, err := getClusterName(ctx, &e2eVSphere)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		datastorestMap = append(datastorestMap, nonShareddatastoreListMapRack1, nonShareddatastoreListMapRack2,
			shareddatastoreListMap)

		// choose preferred datastore
		ginkgo.By("Tag preferred datastore for volume provisioning")
		for i := 0; i < len(allowedTopologyRacks); i++ {
			preferredDatastorePath, err := tagPreferredDatastore(masterIp, sshClientConfig, allowedTopologyRacks[i],
				preferredDatastoreChosen, datastorestMap[i], nil)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			preferredDatastorePaths = append(preferredDatastorePaths, preferredDatastorePath...)
		}

		// fetch datastore map which is tagged as preferred datastore in different racks
		for i := 0; i < len(datastorestMap); i++ {
			for key, val := range datastorestMap[i] {
				if preferredDatastorePaths[i] == key {
					allDatastoresListMap[key] = val
				}
			}
			if i == 3 {
				break
			}
		}

		framework.Logf("Waiting for %v for preferred datastore to get refreshed in the environment",
			preferredDatastoreTimeOutInterval)
		time.Sleep(preferredDatastoreTimeOutInterval)

		ginkgo.By("Creating StorageClass for Statefulset")
		scSpec := getVSphereStorageClassSpec(defaultNginxStorageClassName, nil, allowedTopologies,
			"", "", false)
		sc, err := client.StorageV1().StorageClasses().Create(ctx, scSpec, metav1.CreateOptions{})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		defer func() {
			err = client.StorageV1().StorageClasses().Delete(ctx, sc.Name, *metav1.NewDeleteOptions(0))
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}()

		ginkgo.By("Creating statefulset with 3 replica")
		statefulset := GetStatefulSetFromManifest(namespace)
		CreateStatefulSet(namespace, statefulset, client)
		replicas := *(statefulset.Spec.Replicas)
		defer func() {
			framework.Logf("Deleting all statefulset in namespace: %v", namespace)
			fss.DeleteAllStatefulSets(client, namespace)
		}()

		// Waiting for pods status to be Ready.
		fss.WaitForStatusReadyReplicas(client, statefulset, replicas)
		gomega.Expect(fss.CheckMount(client, statefulset, mountPath)).NotTo(gomega.HaveOccurred())
		ssPodsBeforeScaleDown := fss.GetPodList(client, statefulset)
		gomega.Expect(ssPodsBeforeScaleDown.Items).NotTo(gomega.BeEmpty(),
			fmt.Sprintf("Unable to get list of Pods from the Statefulset: %v", statefulset.Name))
		gomega.Expect(len(ssPodsBeforeScaleDown.Items) == int(replicas)).To(gomega.BeTrue(),
			"Number of Pods in the statefulset should match with number of replicas")

		//verifying volume provisioning
		ginkgo.By("Verify volume is provisioned on the preferred datatsore")
		err = verifyVolumeProvisioningForStatefulSet(ctx, client, statefulset, namespace, preferredDatastorePaths,
			allDatastoresListMap, true, false)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Verify PV node affinity and that the PODS are running on " +
			"appropriate node as specified in the allowed topologies of SC")
		verifyPVnodeAffinityAndPODnodedetailsForStatefulsetsLevel5(ctx, client, statefulset,
			namespace, allowedTopologies, false)

		ginkgo.By("Remove preferred datastore tag chosen for volume provisioning")
		for i := 0; i < len(allowedTopologyRacks); i++ {
			err = detachTagCreatedOnPreferredDatastore(masterIp, sshClientConfig, preferredDatastorePaths[i],
				allowedTopologyRacks[i])
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}

		ginkgo.By("Tag new preferred datatsore for volume provisioning")
		for i := 0; i < len(allowedTopologyRacks); i++ {
			preferredDatastorePath, err := tagPreferredDatastore(masterIp, sshClientConfig, allowedTopologyRacks[i],
				preferredDatastoreChosen, datastorestMap[i], preferredDatastorePaths)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			preferredDatastorePaths = append(preferredDatastorePaths, preferredDatastorePath...)
			preferredDatastorePathsToDel = append(preferredDatastorePathsToDel, preferredDatastorePath...)
		}
		defer func() {
			ginkgo.By("Preferred datastore tags cleanup")
			for i := 0; i < len(allowedTopologyRacks); i++ {
				err = detachTagCreatedOnPreferredDatastore(masterIp, sshClientConfig, preferredDatastorePathsToDel[i],
					allowedTopologyRacks[i])
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
			}
		}()

		// fetch datastore map which is tagged as preferred datastore in different racks
		for i := 0; i < len(datastorestMap); i++ {
			for j := 0; j < len(preferredDatastorePaths); j++ {
				for key, val := range datastorestMap[i] {
					if preferredDatastorePaths[j] == key {
						allDatastoresListMap[key] = val
					}
				}
			}
		}

		framework.Logf("Waiting for %v for preferred datastore to get refreshed in the environment",
			preferredDatastoreTimeOutInterval)
		time.Sleep(preferredDatastoreTimeOutInterval)

		ginkgo.By("Bring down few esxi hosts in rack-2(cluster-2)")
		hostsInCluster := getHostsByClusterName(ctx, clusterComputeResource, clusters[1])
		powerOffHostsList := powerOffEsxiHostByCluster(ctx, &e2eVSphere, clusters[1], (len(hostsInCluster) - 2))
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

		ginkgo.By("Verify all the workload Pods are in up and running state")
		ssPods := fss.GetPodList(client, statefulset)
		for _, pod := range ssPods.Items {
			err := fpod.WaitForPodRunningInNamespace(client, &pod)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}

		ginkgo.By("Scale up statefulset replica and verify the replica count")
		replicas = 10
		scaleUpStatefulSetPod(ctx, client, statefulset, namespace, replicas, true)
		ssPodsAfterScaleDown := GetListOfPodsInSts(client, statefulset)
		gomega.Expect(len(ssPodsAfterScaleDown.Items) == int(replicas)).To(gomega.BeTrue(),
			"Number of Pods in the statefulset should match with number of replicas")

		//verifying volume provisioning
		ginkgo.By("Verify volume is provisioned on the preferred datatsore")
		err = verifyVolumeProvisioningForStatefulSet(ctx, client, statefulset, namespace, preferredDatastorePaths,
			allDatastoresListMap, true, false)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Verify PV node affinity and that the PODS are running on " +
			"appropriate node as specified in the allowed topologies of SC")
		verifyPVnodeAffinityAndPODnodedetailsForStatefulsetsLevel5(ctx, client, statefulset,
			namespace, allowedTopologies, false)

		ginkgo.By("Remove preferred datastore tag in rack-2(cluster-2)")
		err = detachTagCreatedOnPreferredDatastore(masterIp, sshClientConfig, preferredDatastorePaths[4],
			allowedTopologyRacks[1])
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Tag new preferred datatsore for volume provisioning")
		preferredDatastoreRack2New, err := tagPreferredDatastore(masterIp, sshClientConfig, allowedTopologyRacks[1],
			preferredDatastoreChosen, shareddatastoreListMap, nil)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		preferredDatastorePaths = append(preferredDatastorePaths, preferredDatastoreRack2New...)
		defer func() {
			ginkgo.By("Remove preferred datastore tag")
			err = detachTagCreatedOnPreferredDatastore(masterIp, sshClientConfig, preferredDatastoreRack2New[0],
				allowedTopologyRacks[1])
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}()

		// Scale down statefulSets replica count
		replicas = 5
		ginkgo.By("Scale down statefulset replica and verify the replica count")
		scaleDownStatefulSetPod(ctx, client, statefulset, namespace, replicas, true)
		ssPodsAfterScaleDown = GetListOfPodsInSts(client, statefulset)
		gomega.Expect(len(ssPodsAfterScaleDown.Items) == int(replicas)).To(gomega.BeTrue(),
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
		ssPods = fss.GetPodList(client, statefulset)
		for _, pod := range ssPods.Items {
			err := fpod.WaitForPodRunningInNamespace(client, &pod)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}

		// Scale up statefulSets replicas count
		ginkgo.By("Scale up statefulset replica and verify the replica count")
		replicas = 13
		scaleUpStatefulSetPod(ctx, client, statefulset, namespace, replicas, true)
		ssPodsAfterScaleDown = GetListOfPodsInSts(client, statefulset)
		gomega.Expect(len(ssPodsAfterScaleDown.Items) == int(replicas)).To(gomega.BeTrue(),
			"Number of Pods in the statefulset should match with number of replicas")

		//verifying volume provisioning
		ginkgo.By("Verify volume is provisioned on the preferred datatsore")
		err = verifyVolumeProvisioningForStatefulSet(ctx, client, statefulset, namespace, preferredDatastorePaths,
			allDatastoresListMap, true, false)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Verify PV node affinity and that the PODS are running on " +
			"appropriate node as specified in the allowed topologies of SC")
		verifyPVnodeAffinityAndPODnodedetailsForStatefulsetsLevel5(ctx, client, statefulset,
			namespace, allowedTopologies, false)
	})

	/*
		Testcase-18:
			Preferred datastore inaccessible which is on rack-2

			Steps
			1. Assign Tag rack-2, Category "cns.vmware.topology-preferred-datastores" to datastore
			specific to rack-2 (NFS-2 in this case)
			2. Assign Tag rack-2, Category "cns.vmware.topology-preferred-datastores" to datastore
			shared across all racks (sharedVMFS-12 in this case))
			3. Create SC with Immediate with allowed topology set to rack-2
			4. Create StatefulSet with replica count 10
			5. Wait for PVC to reach Bound and Pods to be in running state.
			6. Verify volume provisioning. It should provision volume on any of the preferred datatsore.
			7. Power off the datastore or make datatsore inaccessible where volume has been provisioned (ex - NFS-2)
			8. Create new PVC
			9. Wait for PVC to reach Bound state.
			10. Describe PV and verify node affinity details.
			11. Verify volume provisioning. It should provision volume on the other datatsore.
			12. Create Pod using PVC created above.
			13. Wait for Pod to reach running state.
			14. POD should come up on the node which is present in the same details as mentioned in storage class
			15. Perform ScaleUp operation. Increase the replica count from 10 to 15.
			16. Verify scaleup operation should succeed.
			17. Add/ Attach the datastore back to the site.
			18. Change datatsores preference for one preferred datastore
			19. Perform scaleup operation again. Increase the replica count to 20.
			20. Wait for new PVC's to reach Bound state and Pod to reach running state.
			21. Verify volume provisioning should be on the newly preferred datastore.
			22. Perform cleanup. Delete StatefulSet, PVC and PV, SC.
	*/

	ginkgo.It("Multiple preferred datatstores are tagged in rack-2 where one preferred datatsore "+
		"moved to inaccessible or in power off state", func() {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		preferredDatastoreChosen = 1
		datastoreOp := "off"
		preferredDatastorePaths = nil

		ginkgo.By("Tag preferred datastore for volume provisioning in rack-2(cluster-2)")
		preferredDatastore1, err := tagPreferredDatastore(masterIp, sshClientConfig, allowedTopologyRacks[1],
			preferredDatastoreChosen, nonShareddatastoreListMapRack2, nil)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		if !strings.Contains(preferredDatastore1[0], "nfs") {
			err = detachTagCreatedOnPreferredDatastore(masterIp, sshClientConfig, preferredDatastore1[0],
				allowedTopologyRacks[1])
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			preferredDatastore1, err = tagPreferredDatastore(masterIp, sshClientConfig, allowedTopologyRacks[1],
				preferredDatastoreChosen, nonShareddatastoreListMapRack2, preferredDatastore1)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			preferredDatastorePaths = append(preferredDatastorePaths, preferredDatastore1...)
		} else {
			preferredDatastorePaths = append(preferredDatastorePaths, preferredDatastore1...)
		}

		preferredDatastore2, err := tagPreferredDatastore(masterIp, sshClientConfig, allowedTopologyRacks[1],
			preferredDatastoreChosen, shareddatastoreListMap, nil)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		preferredDatastorePaths = append(preferredDatastorePaths, preferredDatastore2...)
		defer func() {
			ginkgo.By("Remove preferred datastore tag")
			for i := 0; i < len(preferredDatastorePaths); i++ {
				err = detachTagCreatedOnPreferredDatastore(masterIp, sshClientConfig, preferredDatastorePaths[i],
					allowedTopologyRacks[1])
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
			}
		}()

		framework.Logf("Waiting for %v for preferred datastore to get refreshed in the environment",
			preferredDatastoreTimeOutInterval)
		time.Sleep(preferredDatastoreTimeOutInterval)

		ginkgo.By("Creating StorageClass for Statefulset")
		scSpec := getVSphereStorageClassSpec(defaultNginxStorageClassName, nil, allowedTopologyForRack2,
			"", "", false)
		sc, err := client.StorageV1().StorageClasses().Create(ctx, scSpec, metav1.CreateOptions{})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		defer func() {
			err = client.StorageV1().StorageClasses().Delete(ctx, sc.Name, *metav1.NewDeleteOptions(0))
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}()

		ginkgo.By("Creating statefulset with 10 replicas")
		statefulset := GetStatefulSetFromManifest(namespace)
		*(statefulset.Spec.Replicas) = 10
		CreateStatefulSet(namespace, statefulset, client)
		replicas := *(statefulset.Spec.Replicas)
		defer func() {
			framework.Logf("Deleting all statefulset in namespace: %v", namespace)
			fss.DeleteAllStatefulSets(client, namespace)
		}()

		// Waiting for pods status to be Ready.
		fss.WaitForStatusReadyReplicas(client, statefulset, replicas)
		gomega.Expect(fss.CheckMount(client, statefulset, mountPath)).NotTo(gomega.HaveOccurred())
		ssPodsBeforeScaleDown := fss.GetPodList(client, statefulset)
		gomega.Expect(ssPodsBeforeScaleDown.Items).NotTo(gomega.BeEmpty(),
			fmt.Sprintf("Unable to get list of Pods from the Statefulset: %v", statefulset.Name))
		gomega.Expect(len(ssPodsBeforeScaleDown.Items) == int(replicas)).To(gomega.BeTrue(),
			"Number of Pods in the statefulset should match with number of replicas")

		//verifying volume provisioning
		ginkgo.By("Verify volume is provisioned on the preferred datatsore")
		err = verifyVolumeProvisioningForStatefulSet(ctx, client, statefulset, namespace, preferredDatastorePaths,
			rack2DatastoreListMap, false, false)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Verify PV node affinity and that the PODS are running on " +
			"appropriate node as specified in the allowed topologies of SC")
		verifyPVnodeAffinityAndPODnodedetailsForStatefulsetsLevel5(ctx, client, statefulset,
			namespace, allowedTopologyForRack2, false)

		ginkgo.By("Migrate the worker vms residing on the nfs datatsore before " +
			"making datastore inaccessible")

		framework.Logf("Fetch worker vms residing on rack-2")
		vMsToMigrate, err := fetchWorkerNodeVms(masterIp, sshClientConfig, dataCenters, workerInitialAlias[1])
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		framework.Logf("Move worker vms if residing on nfs to another preferred datastore")
		isMigrateSuccess, err := migrateVmsFromDatastore(masterIp, sshClientConfig, preferredDatastore2[0], vMsToMigrate)
		gomega.Expect(isMigrateSuccess).To(gomega.BeTrue(), "Migration of vms failed")
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		framework.Logf("Perform power off operation on nfs preferred datastore")
		datastoreName := powerOffPreferredDatastore(ctx, &e2eVSphere, datastoreOp, dsNameToPerformNimbusOps[1])
		defer func() {
			datastoreOp = "on"
			powerOnPreferredDatastore(datastoreName, datastoreOp)
		}()

		ginkgo.By("Creating PVC")
		pvclaim1, err := createPVC(client, namespace, nil, "", sc, "")
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		var pvclaims1 []*v1.PersistentVolumeClaim
		pvclaims1 = append(pvclaims1, pvclaim1)
		ginkgo.By("Waiting for all claims to be in bound state")
		pvs1, err := fpv.WaitForPVClaimBoundPhase(client, pvclaims1, framework.ClaimProvisionTimeout)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		gomega.Expect(pvs1).NotTo(gomega.BeEmpty())
		pv1 := pvs1[0]
		defer func() {
			err = fpv.DeletePersistentVolumeClaim(client, pvclaim1.Name, namespace)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			ginkgo.By("Verify PVs, volumes are deleted from CNS")
			err = e2eVSphere.waitForCNSVolumeToBeDeleted(pv1.Spec.CSI.VolumeHandle)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())

		}()

		ginkgo.By("Creating pod")
		pod, err := createPod(client, namespace, nil, []*v1.PersistentVolumeClaim{pvclaim1}, false, "")
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		defer func() {
			ginkgo.By(fmt.Sprintf("Deleting the pod %s in namespace %s", pod.Name, namespace))
			err = fpod.DeletePodWithWait(client, pod)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}()

		// verifying volume provisioning
		ginkgo.By("Verify volume is provisioned on the preferred datatsore")
		verifyVolumeProvisioningForStandalonePods(ctx, client, pod, namespace, preferredDatastorePaths,
			rack2DatastoreListMap)

		ginkgo.By("Verify PV node affinity and that the PODS are running on appropriate " +
			"node as specified in the allowed topologies of SC")
		verifyPVnodeAffinityAndPODnodedetailsFoStandalonePodLevel5(ctx, client, pod, namespace,
			allowedTopologyForRack2)

		// perform statefulset scaleup
		replicas = 15
		ginkgo.By("Scale up statefulset replica count from 10 to 15")
		scaleUpStatefulSetPod(ctx, client, statefulset, namespace, replicas, false)

		//verifying volume provisioning
		ginkgo.By("Verify volume is provisioned on the preferred datatsore")
		err = verifyVolumeProvisioningForStatefulSet(ctx, client, statefulset, namespace, preferredDatastorePaths,
			rack2DatastoreListMap, false, false)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Verify PV node affinity and that the PODS are running on " +
			"appropriate node as specified in the allowed topologies of SC")
		verifyPVnodeAffinityAndPODnodedetailsForStatefulsetsLevel5(ctx, client, statefulset,
			namespace, allowedTopologyForRack2, false)

		ginkgo.By("Power on the inaccessible datastore")
		datastoreOp = "on"
		powerOnPreferredDatastore(datastoreName, datastoreOp)

		ginkgo.By("Remove preferred datastore tag chosen for volume provisioning")
		err = detachTagCreatedOnPreferredDatastore(masterIp, sshClientConfig, preferredDatastore1[0], allowedTopologyRacks[1])
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		newPreferredDatastore, err := tagPreferredDatastore(masterIp, sshClientConfig, allowedTopologyRacks[1],
			preferredDatastoreChosen, nonShareddatastoreListMapRack2, preferredDatastorePaths)
		preferredDatastorePaths = append(preferredDatastorePaths, newPreferredDatastore...)

		// perform statefulset scaleup
		replicas = 20
		ginkgo.By("Scale up statefulset replica count from 15 to 20")
		scaleUpStatefulSetPod(ctx, client, statefulset, namespace, replicas, false)

		//verifying volume provisioning
		ginkgo.By("Verify volume is provisioned on the preferred datatsore")
		err = verifyVolumeProvisioningForStatefulSet(ctx, client, statefulset, namespace, preferredDatastorePaths,
			rack2DatastoreListMap, false, false)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Verify PV node affinity and that the PODS are running on " +
			"appropriate node as specified in the allowed topologies of SC")
		verifyPVnodeAffinityAndPODnodedetailsForStatefulsetsLevel5(ctx, client, statefulset,
			namespace, allowedTopologyForRack2, false)
	})

	/*
		Testcase-19:
			One of the preferred datastore is in maintennace mode

			Steps
			1. Assign Tag rack-1, Category "cns.vmware.topology-preferred-datastores" to datastore
			specific to rack-1
			2. Assign Tag rack-1, Category "cns.vmware.topology-preferred-datastores" to datastore
			shared across all racks
			3. Create SC with Immediate binding mode with allowed topology set to rack-1
			4. Create StatefulSet with replica count 10
			5. Wait for PVC to reach Bound and Pods to be in running state.
			6. Verify volume provisioning. It should provision volume on any of the preferred datatsore.
			7. Put one of the preferred datastore in maintenance mode.
			8. Create new PVC
			9. Wait for PVC to reach Bound state.
			10. Describe PV and verify node affinity details.
			11. Verify volume provisioning. It should provision volume on the other datatsore.
			12. Create Pod using PVC created above.
			13. Wait for Pod to reach running state.
			14. POD should come up on the node which is present in the same details as mentioned in storage class
			15. Perform ScaleUp operation. Increase the replica count from 10 to 15.
			16. Verify scaleup operation should succeed.
			17. Exit the datastore from maintenance mode.
			18. Change datatsores preference for one of the preferred datastore
			19. Perform scaleup operation again. Increase the replica count to 20.
			20. Wait for new PVC's to reach Bound state and Pod to reach running state.
			21. Verify volume provisioning should be on the newly preferred datastore.
			22. Perform cleanup. Delete StatefulSet, PVC and PV, SC.
	*/

	ginkgo.It("Multiple preferred datastores are tagged in rack-1 where one of the preferred datastore "+
		"is moved to maintenance mode", func() {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		preferredDatastoreChosen = 1
		preferredDatastorePaths = nil
		isDatastoreInMaintenanceMode := false

		ginkgo.By("Tag preferred datastore for volume provisioning in rack-1(cluster-1)")
		preferredDatastore1, err := tagPreferredDatastore(masterIp, sshClientConfig, allowedTopologyRacks[0],
			preferredDatastoreChosen, nonShareddatastoreListMapRack1, nil)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		if !strings.Contains(preferredDatastore1[0], "nfs") {
			err = detachTagCreatedOnPreferredDatastore(masterIp, sshClientConfig, preferredDatastore1[0],
				allowedTopologyRacks[0])
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			preferredDatastore1, err = tagPreferredDatastore(masterIp, sshClientConfig, allowedTopologyRacks[0],
				preferredDatastoreChosen, nonShareddatastoreListMapRack1, preferredDatastore1)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			preferredDatastorePaths = append(preferredDatastorePaths, preferredDatastore1...)
		} else {
			preferredDatastorePaths = append(preferredDatastorePaths, preferredDatastore1...)
		}

		preferredDatastore2, err := tagPreferredDatastore(masterIp, sshClientConfig, allowedTopologyRacks[0],
			preferredDatastoreChosen, shareddatastoreListMap, nil)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		preferredDatastorePaths = append(preferredDatastorePaths, preferredDatastore2...)
		defer func() {
			ginkgo.By("Remove preferred datastore tag")
			for i := 0; i < len(preferredDatastorePaths); i++ {
				err = detachTagCreatedOnPreferredDatastore(masterIp, sshClientConfig, preferredDatastorePaths[i],
					allowedTopologyRacks[0])
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
			}
		}()

		framework.Logf("Waiting for %v for preferred datastore to get refreshed in the environment",
			preferredDatastoreTimeOutInterval)
		time.Sleep(preferredDatastoreTimeOutInterval)

		ginkgo.By("Creating StorageClass for Statefulset")
		scSpec := getVSphereStorageClassSpec(defaultNginxStorageClassName, nil, allowedTopologyForRack1,
			"", "", false)
		sc, err := client.StorageV1().StorageClasses().Create(ctx, scSpec, metav1.CreateOptions{})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		defer func() {
			err = client.StorageV1().StorageClasses().Delete(ctx, sc.Name, *metav1.NewDeleteOptions(0))
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}()

		// Creating statefulset with 3 replicas
		ginkgo.By("Creating statefulset with 3 replica")
		statefulset := GetStatefulSetFromManifest(namespace)
		*(statefulset.Spec.Replicas) = 10
		CreateStatefulSet(namespace, statefulset, client)
		replicas := *(statefulset.Spec.Replicas)
		defer func() {
			framework.Logf("Deleting all statefulset in namespace: %v", namespace)
			fss.DeleteAllStatefulSets(client, namespace)
		}()

		// Waiting for pods status to be Ready.
		fss.WaitForStatusReadyReplicas(client, statefulset, replicas)
		gomega.Expect(fss.CheckMount(client, statefulset, mountPath)).NotTo(gomega.HaveOccurred())
		ssPodsBeforeScaleDown := fss.GetPodList(client, statefulset)
		gomega.Expect(ssPodsBeforeScaleDown.Items).NotTo(gomega.BeEmpty(),
			fmt.Sprintf("Unable to get list of Pods from the Statefulset: %v", statefulset.Name))
		gomega.Expect(len(ssPodsBeforeScaleDown.Items) == int(replicas)).To(gomega.BeTrue(),
			"Number of Pods in the statefulset should match with number of replicas")

		//verifying volume provisioning
		ginkgo.By("Verify volume is provisioned on the preferred datatsore")
		err = verifyVolumeProvisioningForStatefulSet(ctx, client, statefulset, namespace, preferredDatastorePaths,
			rack1DatastoreListMap, false, false)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Verify PV node affinity and that the PODS are running on " +
			"appropriate node as specified in the allowed topologies of SC")
		verifyPVnodeAffinityAndPODnodedetailsForStatefulsetsLevel5(ctx, client, statefulset,
			namespace, allowedTopologyForRack1, false)

		ginkgo.By("Migrate all the worker vms residing on the preferred datatsore before " +
			"putting it into maintenance mode")

		framework.Logf("Fetch worker vms sitting on rack-1")
		vMsToMigrate, err := fetchWorkerNodeVms(masterIp, sshClientConfig, dataCenters, workerInitialAlias[0])
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		framework.Logf("Move worker vms if residing on preferred datastore to another " +
			"preferred  datastore")
		isMigrateSuccess, err := migrateVmsFromDatastore(masterIp, sshClientConfig, preferredDatastore2[0], vMsToMigrate)
		gomega.Expect(isMigrateSuccess).To(gomega.BeTrue(), "Migration of vms failed")
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		framework.Logf("Put preferred datastore in maintenance mode")
		err = preferredDatastoreInMaintenanceMode(masterIp, sshClientConfig, dataCenters, preferredDatastore1[0])
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		isDatastoreInMaintenanceMode = true
		defer func() {
			if isDatastoreInMaintenanceMode {
				err = exitDatastoreFromMaintenanceMode(masterIp, sshClientConfig, dataCenters, preferredDatastore1[0])
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
			}
		}()

		ginkgo.By("Creating PVC")
		pvclaim1, err := createPVC(client, namespace, nil, "", sc, "")
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		var pvclaims1 []*v1.PersistentVolumeClaim
		pvclaims1 = append(pvclaims1, pvclaim1)
		ginkgo.By("Waiting for all claims to be in bound state")
		pvs1, err := fpv.WaitForPVClaimBoundPhase(client, pvclaims1, framework.ClaimProvisionTimeout)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		gomega.Expect(pvs1).NotTo(gomega.BeEmpty())
		pv1 := pvs1[0]
		defer func() {
			err = fpv.DeletePersistentVolumeClaim(client, pvclaim1.Name, namespace)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			ginkgo.By("Verify PVs, volumes are deleted from CNS")
			err = e2eVSphere.waitForCNSVolumeToBeDeleted(pv1.Spec.CSI.VolumeHandle)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())

		}()

		ginkgo.By("Creating pod")
		pod, err := createPod(client, namespace, nil, []*v1.PersistentVolumeClaim{pvclaim1}, false, "")
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		defer func() {
			ginkgo.By(fmt.Sprintf("Deleting the pod %s in namespace %s", pod.Name, namespace))
			err = fpod.DeletePodWithWait(client, pod)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}()

		// verifying volume provisioning
		ginkgo.By("Verify volume is provisioned on the preferred datatsore")
		verifyVolumeProvisioningForStandalonePods(ctx, client, pod, namespace, preferredDatastorePaths,
			rack1DatastoreListMap)

		ginkgo.By("Verify PV node affinity and that the PODS are running on appropriate " +
			"node as specified in the allowed topologies of SC")
		verifyPVnodeAffinityAndPODnodedetailsFoStandalonePodLevel5(ctx, client, pod, namespace,
			allowedTopologyForRack1)

		// perform statefulset scaleup
		replicas = 15
		ginkgo.By("Scale up statefulset replica count from 10 to 15")
		scaleUpStatefulSetPod(ctx, client, statefulset, namespace, replicas, false)

		//verifying volume provisioning
		ginkgo.By("Verify volume is provisioned on the preferred datatsore")
		err = verifyVolumeProvisioningForStatefulSet(ctx, client, statefulset, namespace, preferredDatastorePaths,
			rack1DatastoreListMap, false, false)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Verify PV node affinity and that the PODS are running on " +
			"appropriate node as specified in the allowed topologies of SC")
		verifyPVnodeAffinityAndPODnodedetailsForStatefulsetsLevel5(ctx, client, statefulset,
			namespace, allowedTopologyForRack1, false)

		ginkgo.By("Exit datastore from Maintenance mode")
		err = exitDatastoreFromMaintenanceMode(masterIp, sshClientConfig, dataCenters, preferredDatastore1[0])
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		isDatastoreInMaintenanceMode = false

		ginkgo.By("Remove preferred datastore tag chosen for volume provisioning")
		err = detachTagCreatedOnPreferredDatastore(masterIp, sshClientConfig, preferredDatastore1[0], allowedTopologyRacks[0])
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		newPreferredDatastore, err := tagPreferredDatastore(masterIp, sshClientConfig, allowedTopologyRacks[0],
			preferredDatastoreChosen, nonShareddatastoreListMapRack1, preferredDatastorePaths)
		preferredDatastorePaths = append(preferredDatastorePaths, newPreferredDatastore...)

		// perform statefulset scaleup
		replicas = 20
		ginkgo.By("Scale up statefulset replica count from 15 to 20")
		scaleUpStatefulSetPod(ctx, client, statefulset, namespace, replicas, false)

		//verifying volume provisioning
		ginkgo.By("Verify volume is provisioned on the preferred datatsore")
		err = verifyVolumeProvisioningForStatefulSet(ctx, client, statefulset, namespace, preferredDatastorePaths,
			rack1DatastoreListMap, false, false)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Verify PV node affinity and that the PODS are running on " +
			"appropriate node as specified in the allowed topologies of SC")
		verifyPVnodeAffinityAndPODnodedetailsForStatefulsetsLevel5(ctx, client, statefulset,
			namespace, allowedTopologyForRack1, false)
	})

	/*
		Testcase-20:
			Preferred datastore is in suspended mode which is on rack-3

			Steps
			1. Assign Tag rack-2, Category "cns.vmware.topology-preferred-datastores" to datastore
			specific to rack-2
			2. Assign Tag rack-2, Category "cns.vmware.topology-preferred-datastores" to datastore
			shared across all racks
			3. Create SC with Immediate with allowed topology set to rack-2
			4. Create StatefulSet with replica count 10
			5. Wait for PVC to reach Bound and Pods to be in running state.
			6. Verify volume provisioning. It should provision volume on any of the preferred datatsore.
			7. Suspend the datastore where volume has been provisioned (ex - NFS-2)
			8. Create new PVC
			9. Wait for PVC to reach Bound state.
			10. Describe PV and verify node affinity details.
			11. Verify volume provisioning. It should provision volume on the other datatsore.
			12. Create Pod using PVC created above.
			13. Wait for Pod to reach running state.
			14. POD should come up on the node which is present in the same details as mentioned in storage class
			15. Perform ScaleUp operation. Increase the replica count from 10 to 15.
			16. Verify scaleup operation should succeed.
			17. Add/ Attach the datastore back to the site.
			18. Change datatsores preference for one preferred datastore
			19. Perform scaleup operation again. Increase the replica count to 20.
			20. Wait for new PVC's to reach Bound state and Pod to reach running state.
			21. Verify volume provisioning should be on the newly preferred datastore.
			22. Perform cleanup. Delete StatefulSet, PVC and PV, SC.
	*/

	ginkgo.It("Multiple preferred datatstores are tagged in rack-2 where one preferred datatsore "+
		"moved to suspended state", func() {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		preferredDatastoreChosen = 1
		datastoreOp := "suspend"
		preferredDatastorePaths = nil

		ginkgo.By("Tag preferred datastore for volume provisioning in rack-2(cluster-2)")
		preferredDatastore1, err := tagPreferredDatastore(masterIp, sshClientConfig, allowedTopologyRacks[1],
			preferredDatastoreChosen, nonShareddatastoreListMapRack2, nil)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		if !strings.Contains(preferredDatastore1[0], "nfs") {
			err = detachTagCreatedOnPreferredDatastore(masterIp, sshClientConfig, preferredDatastore1[0],
				allowedTopologyRacks[1])
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			preferredDatastore1, err = tagPreferredDatastore(masterIp, sshClientConfig, allowedTopologyRacks[1],
				preferredDatastoreChosen, nonShareddatastoreListMapRack2, preferredDatastore1)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			preferredDatastorePaths = append(preferredDatastorePaths, preferredDatastore1...)
		} else {
			preferredDatastorePaths = append(preferredDatastorePaths, preferredDatastore1...)
		}

		preferredDatastore2, err := tagPreferredDatastore(masterIp, sshClientConfig, allowedTopologyRacks[1],
			preferredDatastoreChosen, shareddatastoreListMap, nil)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		preferredDatastorePaths = append(preferredDatastorePaths, preferredDatastore2...)

		defer func() {
			ginkgo.By("Remove preferred datastore tag")
			for i := 0; i < len(preferredDatastorePaths); i++ {
				err = detachTagCreatedOnPreferredDatastore(masterIp, sshClientConfig, preferredDatastorePaths[i],
					allowedTopologyRacks[1])
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
			}

		}()

		framework.Logf("Waiting for %v for preferred datastore to get refreshed in the environment",
			preferredDatastoreTimeOutInterval)
		time.Sleep(preferredDatastoreTimeOutInterval)

		ginkgo.By("Creating StorageClass for Statefulset")
		scSpec := getVSphereStorageClassSpec(defaultNginxStorageClassName, nil, allowedTopologyForRack2,
			"", "", false)
		sc, err := client.StorageV1().StorageClasses().Create(ctx, scSpec, metav1.CreateOptions{})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		defer func() {
			err = client.StorageV1().StorageClasses().Delete(ctx, sc.Name, *metav1.NewDeleteOptions(0))
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}()

		// Creating statefulset with 3 replicas
		ginkgo.By("Creating statefulset with 3 replica")
		statefulset := GetStatefulSetFromManifest(namespace)
		*(statefulset.Spec.Replicas) = 10
		CreateStatefulSet(namespace, statefulset, client)
		replicas := *(statefulset.Spec.Replicas)
		defer func() {
			framework.Logf("Deleting all statefulset in namespace: %v", namespace)
			fss.DeleteAllStatefulSets(client, namespace)
		}()

		// Waiting for pods status to be Ready.
		fss.WaitForStatusReadyReplicas(client, statefulset, replicas)
		gomega.Expect(fss.CheckMount(client, statefulset, mountPath)).NotTo(gomega.HaveOccurred())
		ssPodsBeforeScaleDown := fss.GetPodList(client, statefulset)
		gomega.Expect(ssPodsBeforeScaleDown.Items).NotTo(gomega.BeEmpty(),
			fmt.Sprintf("Unable to get list of Pods from the Statefulset: %v", statefulset.Name))
		gomega.Expect(len(ssPodsBeforeScaleDown.Items) == int(replicas)).To(gomega.BeTrue(),
			"Number of Pods in the statefulset should match with number of replicas")

		//verifying volume provisioning
		ginkgo.By("Verify volume is provisioned on the preferred datatsore")
		err = verifyVolumeProvisioningForStatefulSet(ctx, client, statefulset, namespace, preferredDatastorePaths,
			rack2DatastoreListMap, false, false)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Verify PV node affinity and that the PODS are running on " +
			"appropriate node as specified in the allowed topologies of SC")
		verifyPVnodeAffinityAndPODnodedetailsForStatefulsetsLevel5(ctx, client, statefulset,
			namespace, allowedTopologyForRack2, false)

		ginkgo.By("Migrate all the worker vms residing on the nfs datatsore before " +
			"making datastore inaccessible")

		framework.Logf("Fetch worker vms sitting on rack-2")
		vMsToMigrate, err := fetchWorkerNodeVms(masterIp, sshClientConfig, dataCenters, workerInitialAlias[1])
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		framework.Logf("Move worker vms if residing on nfs to another preferred sharedvmfs datastore")
		isMigrateSuccess, err := migrateVmsFromDatastore(masterIp, sshClientConfig, preferredDatastore2[0], vMsToMigrate)
		gomega.Expect(isMigrateSuccess).To(gomega.BeTrue(), "Migration of vms failed")
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		framework.Logf("Perform suspend operation on the preferred datastore")
		datastoreName := powerOffPreferredDatastore(ctx, &e2eVSphere, datastoreOp, dsNameToPerformNimbusOps[1])
		defer func() {
			ginkgo.By("Power on the suspended datastore")
			datastoreOp = "on"
			powerOnPreferredDatastore(datastoreName, datastoreOp)
		}()

		ginkgo.By("Creating PVC")
		pvclaim1, err := createPVC(client, namespace, nil, "", sc, "")
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		var pvclaims1 []*v1.PersistentVolumeClaim
		pvclaims1 = append(pvclaims1, pvclaim1)
		ginkgo.By("Waiting for all claims to be in bound state")
		pvs1, err := fpv.WaitForPVClaimBoundPhase(client, pvclaims1, framework.ClaimProvisionTimeout)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		gomega.Expect(pvs1).NotTo(gomega.BeEmpty())
		pv1 := pvs1[0]
		defer func() {
			err = fpv.DeletePersistentVolumeClaim(client, pvclaim1.Name, namespace)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			ginkgo.By("Verify PVs, volumes are deleted from CNS")
			err = e2eVSphere.waitForCNSVolumeToBeDeleted(pv1.Spec.CSI.VolumeHandle)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())

		}()

		ginkgo.By("Creating pod")
		pod, err := createPod(client, namespace, nil, []*v1.PersistentVolumeClaim{pvclaim1}, false, "")
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		defer func() {
			ginkgo.By(fmt.Sprintf("Deleting the pod %s in namespace %s", pod.Name, namespace))
			err = fpod.DeletePodWithWait(client, pod)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}()

		// verifying volume provisioning
		ginkgo.By("Verify volume is provisioned on the preferred datatsore")
		verifyVolumeProvisioningForStandalonePods(ctx, client, pod, namespace, preferredDatastorePaths,
			rack2DatastoreListMap)

		ginkgo.By("Verify PV node affinity and that the PODS are running on appropriate " +
			"node as specified in the allowed topologies of SC")
		verifyPVnodeAffinityAndPODnodedetailsFoStandalonePodLevel5(ctx, client, pod, namespace,
			allowedTopologyForRack2)

		// perform statefulset scaleup
		replicas = 15
		ginkgo.By("Scale up statefulset replica count from 10 to 15")
		scaleUpStatefulSetPod(ctx, client, statefulset, namespace, replicas, false)

		//verifying volume provisioning
		ginkgo.By("Verify volume is provisioned on the preferred datatsore")
		err = verifyVolumeProvisioningForStatefulSet(ctx, client, statefulset, namespace, preferredDatastorePaths,
			rack2DatastoreListMap, false, false)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Verify PV node affinity and that the PODS are running on " +
			"appropriate node as specified in the allowed topologies of SC")
		verifyPVnodeAffinityAndPODnodedetailsForStatefulsetsLevel5(ctx, client, statefulset,
			namespace, allowedTopologyForRack2, false)

		ginkgo.By("Power on the suspended datastore")
		datastoreOp = "on"
		powerOnPreferredDatastore(datastoreName, datastoreOp)

		ginkgo.By("Remove preferred datastore tag chosen for volume provisioning")
		err = detachTagCreatedOnPreferredDatastore(masterIp, sshClientConfig, preferredDatastore1[0], allowedTopologyRacks[1])
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		newPreferredDatastore, err := tagPreferredDatastore(masterIp, sshClientConfig, allowedTopologyRacks[1],
			preferredDatastoreChosen, nonShareddatastoreListMapRack2, preferredDatastorePaths)
		preferredDatastorePaths = append(preferredDatastorePaths, newPreferredDatastore...)

		// perform statefulset scaleup
		replicas = 20
		ginkgo.By("Scale up statefulset replica count from 15 to 20")
		scaleUpStatefulSetPod(ctx, client, statefulset, namespace, replicas, false)

		//verifying volume provisioning
		ginkgo.By("Verify volume is provisioned on the preferred datatsore")
		err = verifyVolumeProvisioningForStatefulSet(ctx, client, statefulset, namespace, preferredDatastorePaths,
			rack2DatastoreListMap, false, false)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Verify PV node affinity and that the PODS are running on " +
			"appropriate node as specified in the allowed topologies of SC")
		verifyPVnodeAffinityAndPODnodedetailsForStatefulsetsLevel5(ctx, client, statefulset,
			namespace, allowedTopologyForRack2, false)
	})
})
