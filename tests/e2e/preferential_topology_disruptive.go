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
	"time"

	ginkgo "github.com/onsi/ginkgo/v2"
	"github.com/onsi/gomega"
	"github.com/vmware/govmomi/object"
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

var _ = ginkgo.Describe("[Disruptive-Preferential-Topology] Preferential-Topology-Disruptive-Provisioning", func() {
	f := framework.NewDefaultFramework("preferential-topology-aware-provisioning")
	f.NamespacePodSecurityEnforceLevel = admissionapi.LevelPrivileged
	var (
		client                         clientset.Interface
		namespace                      string
		bindingMode                    storagev1.VolumeBindingMode
		allowedTopologies              []v1.TopologySelectorLabelRequirement
		topologyAffinityDetails        map[string][]string
		topologyCategories             []string
		topologyLength                 int
		leafNode                       int
		leafNodeTag0                   int
		leafNodeTag1                   int
		leafNodeTag2                   int
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
		csiReplicas                    int32
		csiNamespace                   string
		preferredDatastorePaths        []string
		allowedTopologyRacks           []string
		allowedTopologyForRack1        []v1.TopologySelectorLabelRequirement
		allowedTopologyForRack2        []v1.TopologySelectorLabelRequirement
		allDatastoresListMap           map[string]string
		err                            error
		datastoreOp                    string
		nodeList                       *v1.NodeList
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
		bindingMode = storagev1.VolumeBindingWaitForFirstConsumer

		readVcEsxIpsViaTestbedInfoJson(GetAndExpectStringEnvVar(envTestbedInfoJsonPath))

		// fetching k8s master ip
		allMasterIps = getK8sMasterIPs(ctx, client)
		masterIp = allMasterIps[0]

		// fetching datacenter details
		dataCenters, err = e2eVSphere.getAllDatacenters(ctx)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		// fetching cluster details
		clusters, err = getTopologyLevel5ClusterGroupNames(masterIp, dataCenters)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		// creating level-5 allowed topology map
		topologyLength, leafNode, leafNodeTag0, leafNodeTag1, leafNodeTag2 = 5, 4, 0, 1, 2
		fmt.Println(leafNodeTag2)

		topologyMap := GetAndExpectStringEnvVar(topologyMap)
		topologyAffinityDetails, topologyCategories = createTopologyMapLevel5(topologyMap,
			topologyLength)
		allowedTopologies = createAllowedTopolgies(topologyMap, topologyLength)

		// fetching csi pods replicas
		csiNamespace = GetAndExpectStringEnvVar(envCSINamespace)
		csiDeployment, err := client.AppsV1().Deployments(csiNamespace).Get(
			ctx, vSphereCSIControllerPodNamePrefix, metav1.GetOptions{})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		csiReplicas = *csiDeployment.Spec.Replicas
		fmt.Println(csiReplicas)

		// set preferred datatsore time interval
		//setPreferredDatastoreTimeInterval(client, ctx, csiNamespace, namespace, csiReplicas)

		// fetching list of datatstores shared between vm's
		shareddatastoreListMap, err = getListOfSharedDatastoresBetweenVMs(masterIp, dataCenters)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		framework.Logf("Fetching list of datastores available in different racks")
		rack1DatastoreListMap, err = getListOfDatastoresByClusterName(masterIp, clusters[0])
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		rack2DatastoreListMap, err = getListOfDatastoresByClusterName(masterIp, clusters[1])
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		// rack3DatastoreListMap, err = getListOfDatastoresByClusterName(masterIp, clusters[2])
		// gomega.Expect(err).NotTo(gomega.HaveOccurred())

		framework.Logf("Fetching list of datastores which is specific to each rack")
		nonShareddatastoreListMapRack1 = getNonSharedDatastoresInCluster(rack1DatastoreListMap,
			shareddatastoreListMap)
		nonShareddatastoreListMapRack2 = getNonSharedDatastoresInCluster(rack2DatastoreListMap,
			shareddatastoreListMap)
		// nonShareddatastoreListMapRack3 = getNonSharedDatastoresInCluster(rack3DatastoreListMap,
		// 	shareddatastoreListMap)

		// Get different allowed topologies required for creating Storage Class
		allowedTopologyForRack1 = getTopologySelector(topologyAffinityDetails, topologyCategories,
			topologyLength, leafNode, leafNodeTag0)
		allowedTopologyForRack2 = getTopologySelector(topologyAffinityDetails, topologyCategories,
			topologyLength, leafNode, leafNodeTag1)
		// allowedTopologyForRack3 = getTopologySelector(topologyAffinityDetails, topologyCategories,
		// 	topologyLength, leafNode, leafNodeTag2)

		// fetching cluster topology racks level
		allowedTopologyForSC := getTopologySelector(topologyAffinityDetails, topologyCategories,
			topologyLength)
		for i := 4; i < len(allowedTopologyForSC); i++ {
			for j := 0; j < len(allowedTopologyForSC[i].Values); j++ {
				allowedTopologyRacks = append(allowedTopologyRacks, allowedTopologyForSC[i].Values[j])
			}
		}
		// fetching datacenter details
		dataCenters, err = e2eVSphere.getAllDatacenters(ctx)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		// fetching list of datatstores available in a testbed
		allDatastoresListMap, err = getListOfAvailableDatastores(masterIp)
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
		framework.Logf("Perform preferred datastore tags cleanup after test completion")
		err = deleteTagCreatedForPreferredDatastore(masterIp, allowedTopologyRacks)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		framework.Logf("Recreate preferred datastore tags post cleanup")
		err = createTagForPreferredDatastore(masterIp, allowedTopologyRacks)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		framework.Logf("Waiting for %v for preferred datastore to get refreshed in the environment",
			preferredDatastoreTimeOutInterval)
		time.Sleep(preferredDatastoreTimeOutInterval)
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
			10. Change datastore preference.
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

		// get cluster details
		clusterComputeResource, _, err = getClusterName(ctx, &e2eVSphere)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		// choose preferred datastore
		ginkgo.By("Tag preferred datastore for volume provisioning")
		preferredDatastoreRack1, err := tagPreferredDatastore(masterIp, allowedTopologyRacks[0],
			preferredDatastoreChosen, nonShareddatastoreListMapRack1, nil)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		preferredDatastorePaths = append(preferredDatastorePaths, preferredDatastoreRack1...)
		preferredDatastoreRack2, err := tagPreferredDatastore(masterIp, allowedTopologyRacks[1],
			preferredDatastoreChosen, nonShareddatastoreListMapRack2, nil)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		preferredDatastorePaths = append(preferredDatastorePaths, preferredDatastoreRack2...)
		preferredDatastoreRack3, err := tagPreferredDatastore(masterIp, allowedTopologyRacks[2],
			preferredDatastoreChosen, shareddatastoreListMap, nil)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		preferredDatastorePaths = append(preferredDatastorePaths, preferredDatastoreRack3...)

		framework.Logf("Waiting for %v for preferred datastore to get refreshed in the environment",
			preferredDatastoreTimeOutInterval)
		time.Sleep(preferredDatastoreTimeOutInterval)

		ginkgo.By("Creating StorageClass for Statefulset")
		scSpec := getVSphereStorageClassSpec(defaultNginxStorageClassName, nil, allowedTopologies,
			"", bindingMode, false)
		sc, err := client.StorageV1().StorageClasses().Create(ctx, scSpec, metav1.CreateOptions{})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		defer func() {
			err = client.StorageV1().StorageClasses().Delete(ctx, sc.Name, *metav1.NewDeleteOptions(0))
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}()

		// Creating statefulset with 3 replicas
		ginkgo.By("Creating statefulset with 3 replica")
		statefulset := GetStatefulSetFromManifest(namespace)
		ginkgo.By("Creating statefulset")
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
		err = detachTagCreatedOnPreferredDatastore(masterIp, preferredDatastoreRack1[0], allowedTopologyRacks[0])
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		err = detachTagCreatedOnPreferredDatastore(masterIp, preferredDatastoreRack2[0], allowedTopologyRacks[1])
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		err = detachTagCreatedOnPreferredDatastore(masterIp, preferredDatastoreRack3[0], allowedTopologyRacks[2])
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Tag new preferred datatsore for volume provisioning")
		preferredDatastore1, err := tagPreferredDatastore(masterIp, allowedTopologyRacks[0],
			preferredDatastoreChosen, nonShareddatastoreListMapRack1, preferredDatastoreRack1)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		preferredDatastorePaths = append(preferredDatastorePaths, preferredDatastore1...)
		defer func() {
			ginkgo.By("Remove preferred datastore tag")
			err = detachTagCreatedOnPreferredDatastore(masterIp, preferredDatastore1[0],
				allowedTopologyRacks[0])
			gomega.Expect(err).NotTo(gomega.HaveOccurred())

		}()
		preferredDatastore2, err := tagPreferredDatastore(masterIp, allowedTopologyRacks[1],
			preferredDatastoreChosen, nonShareddatastoreListMapRack2, preferredDatastoreRack2)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		preferredDatastorePaths = append(preferredDatastorePaths, preferredDatastore2...)
		defer func() {
			ginkgo.By("Remove preferred datastore tag")
			err = detachTagCreatedOnPreferredDatastore(masterIp, preferredDatastore2[0],
				allowedTopologyRacks[1])
			gomega.Expect(err).NotTo(gomega.HaveOccurred())

		}()
		preferredDatastore3, err := tagPreferredDatastore(masterIp, allowedTopologyRacks[2],
			preferredDatastoreChosen, shareddatastoreListMap, preferredDatastoreRack3)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		preferredDatastorePaths = append(preferredDatastorePaths, preferredDatastore3...)
		defer func() {
			ginkgo.By("Remove preferred datastore tag")
			err = detachTagCreatedOnPreferredDatastore(masterIp, preferredDatastore3[0],
				allowedTopologyRacks[2])
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}()

		framework.Logf("Waiting for %v for preferred datastore to get refreshed in the environment",
			preferredDatastoreTimeOutInterval)
		time.Sleep(preferredDatastoreTimeOutInterval)

		// Bring down few esxi hosts that belongs to zone3
		ginkgo.By("Bring down few esxi hosts")
		hostsInCluster := getHostsByClusterName(ctx, clusterComputeResource, clusters[1])
		powerOffHostsList := powerOffEsxiHostByCluster(ctx, &e2eVSphere, clusters[0],
			(len(hostsInCluster) - 2))
		defer func() {
			ginkgo.By("Bring up all ESXi host which were powered off")
			for i := 0; i < len(powerOffHostsList); i++ {
				powerOnEsxiHostByCluster(powerOffHostsList[i])
			}
		}()

		// ginkgo.By("Wait for k8s cluster to be healthy")
		// wait4AllK8sNodesToBeUp(ctx, client, nodeList)
		// err = waitForAllNodes2BeReady(ctx, client, pollTimeout*4)
		// gomega.Expect(err).NotTo(gomega.HaveOccurred())

		// Verify all the workload Pods are in up and running state
		ginkgo.By("Verify all the workload Pods are in up and running state")
		ssPods := fss.GetPodList(client, statefulset)
		for _, pod := range ssPods.Items {
			err := fpod.WaitForPodRunningInNamespace(client, &pod)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}

		// Scale up statefulSets replicas count
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

		ginkgo.By("Remove preferred datastore tag chosen for volume provisioning")
		err = detachTagCreatedOnPreferredDatastore(masterIp, preferredDatastore2[0], allowedTopologyRacks[1])
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Tag new preferred datatsore for volume provisioning")
		preferredDatastore2, err = tagPreferredDatastore(masterIp, allowedTopologyRacks[1],
			preferredDatastoreChosen, shareddatastoreListMap, preferredDatastorePaths)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		preferredDatastorePaths = append(preferredDatastorePaths, preferredDatastore2...)
		defer func() {
			ginkgo.By("Remove preferred datastore tag")
			err = detachTagCreatedOnPreferredDatastore(masterIp, preferredDatastore2[0],
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
			specific to rack-2
			2. Assign Tag rack-2, Category "cns.vmware.topology-preferred-datastores" to datastore
			shared across all racks (sharedVMFS-12)
			3. Create SC with Immediate with allowed topology set to rack-2
			4. Create StatefulSet with replica count 10
			5. Wait for PVC to reach Bound and Pods to be in running state.
			6. Verify volume provisioning. It should provision volume on any of the preferred datatsore.
			7. Power off the datastore or make datatsore inaccessible where volume has been provisioned (ex - sharedVMFS-12)
			8. Since datastore has been inacessible, the PVC's which were created on the inaccessible datatsore should go to
			inaccessible state (on vCenter UI)
			9. Make sure Node affinity details on the PV shows proper details.
			10. Create new PVC
			11. Wait for PVC to reach Bound state.
			12. Describe PV and verify node affinity details.
			13. Verify volume provisioning. It should provision volume on the other datatsore.
			14. Create Pod using PVC created above.
			15. Wait for Pod to reach running state.
			16. POD should come up on the node which is present in the same details as mentioned in storage class
			17. Perform ScaleUp operation. Increase the replica count from 10 to 13.
			18. ScaleUp operation should fail sue to datastore disconnectivity.
			19. Add/ Attach the datastore back to the site-1
			20. Verify scaling should succeed now. It should start provisioning volume on both the datatsores.
			21. Verify PVC and Pod status.
			22. Verify volume provisioning.
			23. Change datatsores preference
			24. Perform scaleup operation again.
			25. Wait for new PVC's to reach Bound state and Pod to reach running state.
			26. Verify volume provisioning should be on the newly preferred datastore.
			27. Perform cleanup. Delete StatefulSet, PVC and PV, SC.
	*/
	ginkgo.It("Tag multiple preferred datatstores and one preferred datatsore went to inaccessible state", func() {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		preferredDatastoreChosen = 1

		// choose preferred datastore
		ginkgo.By("Tag preferred datastore for volume provisioning in rack-2(cluster-2)")
		preferredDatastorePaths, err = tagPreferredDatastore(masterIp, allowedTopologyRacks[1],
			preferredDatastoreChosen, nonShareddatastoreListMapRack2, nil)
		preferredDatastore, err := tagPreferredDatastore(masterIp, allowedTopologyRacks[1],
			preferredDatastoreChosen, shareddatastoreListMap, nil)
		preferredDatastorePaths = append(preferredDatastorePaths, preferredDatastore...)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		defer func() {
			ginkgo.By("Remove preferred datastore tag")
			for i := 0; i < len(preferredDatastorePaths); i++ {
				err = detachTagCreatedOnPreferredDatastore(masterIp, preferredDatastorePaths[i],
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
		ginkgo.By("Creating statefulset")
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
			namespace, allowedTopologies, false)

		ginkgo.By("Bring down the datastore")
		datastoreOp = "off"

		datastoreName := powerOffPreferredDatastore(ctx, &e2eVSphere, datastoreOp)

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
			allowedTopologies)

		// perform statefulset scaleup
		replicas = 13
		ginkgo.By("Scale up statefulset replica count from 3 to 10")
		scaleUpStatefulSetPod(ctx, client, statefulset, namespace, replicas, false)

		//verifying volume provisioning
		ginkgo.By("Verify volume is provisioned on the preferred datatsore")
		err = verifyVolumeProvisioningForStatefulSet(ctx, client, statefulset, namespace, preferredDatastorePaths,
			rack2DatastoreListMap, false, false)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Verify PV node affinity and that the PODS are running on " +
			"appropriate node as specified in the allowed topologies of SC")
		verifyPVnodeAffinityAndPODnodedetailsForStatefulsetsLevel5(ctx, client, statefulset,
			namespace, allowedTopologies, false)

		ginkgo.By("Bring up the datastore which was down")
		datastoreOp = "on"
		powerOnPreferredDatastore(datastoreName, datastoreOp)

		ginkgo.By("Remove preferred datastore tag chosen for volume provisioning")
		err = detachTagCreatedOnPreferredDatastore(masterIp, preferredDatastorePaths[0], allowedTopologyRacks[1])
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Tag new preferred datatsore for volume provisioning in rack-2(cluster-2)")
		preferredDatastore, err = tagPreferredDatastore(masterIp, allowedTopologyRacks[1],
			preferredDatastoreChosen, nonShareddatastoreListMapRack2, preferredDatastorePaths)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		preferredDatastorePaths = append(preferredDatastorePaths, preferredDatastore...)

		// perform statefulset scaleup
		replicas = 20
		ginkgo.By("Scale up statefulset replica count from 13 to 20")
		scaleUpStatefulSetPod(ctx, client, statefulset, namespace, replicas, false)

		//verifying volume provisioning
		ginkgo.By("Verify volume is provisioned on the preferred datatsore")
		err = verifyVolumeProvisioningForStatefulSet(ctx, client, statefulset, namespace, preferredDatastorePaths,
			rack2DatastoreListMap, false, false)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Verify PV node affinity and that the PODS are running on " +
			"appropriate node as specified in the allowed topologies of SC")
		verifyPVnodeAffinityAndPODnodedetailsForStatefulsetsLevel5(ctx, client, statefulset,
			namespace, allowedTopologies, false)
	})

	/*
		Testcase-19:
			Preferred datastore in maintenance mode which is on site-1

			Steps
			1. Assign Tag "site-1', Category "cns.vmware.topology-preferred-datastores" to datastore specific to site-1. (NFS-1)
			2. Assign Tag "site-1', Category "cns.vmware.topology-preferred-datastores" to datastore
			shared across site-1 and site-2 (sharedVMFS-12)
			3. Create SC with waitForFirstConsumer with allowed topology set to region-1/site-1
			4. Create StatefulSet with replica count 7
			5. Wait for PVC to reach Bound and Pods to be in running state.
			6. Verify volume provisioning. It should provision volume on any of the preferred datatsore.
			7. Put datastore into MM mode where volume has been provisioned (ex - sharedVMFS-12)
			8. All the vm's sitting on MM datatsore should get migrated to another preferred datastore.
			9. All Pods, PVC's created on MM mode datatsore should be in active and running state
			since all the vm's are migrated to another datatsore.
			10. Make sure Node affinity details on the PV shows proper details.
			11. Create new PVC
			12. Wait for PVC to reach Bound state.
			13. Describe PV and verify node affinity details.
			14. Verify volume provisioning. It should provision volume on the other datatsore.
			15. Create Pod using PVC created above.
			16. Wait for Pod to reach running state.
			17. POD should come up on the node which is present in the same details as mentioned in storage class
			18. Perform ScaleUp operation. Increase the replica count from 10 to 13.
			19. ScaleUp operation should go fine.
			20. Exit MM mode from the datatsore.
			21. Verify scaling should succeed now. It should start provisioning volume on any of the active datatsores.
			22. Verify PVC and Pod status.
			23. Verify volume provisioning.
			24. Perform cleanup. Delete StatefulSet, PVC and PV, SC.
	*/

	ginkgo.It("Preferred datatsore is in maintenance mode", func() {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		preferredDatastoreChosen = 1

		// choose preferred datastore
		ginkgo.By("Tag preferred datastore for volume provisioning in rack-2(cluster-2)")
		preferredDatastorePaths, err = tagPreferredDatastore(masterIp, allowedTopologyRacks[1],
			preferredDatastoreChosen, nonShareddatastoreListMapRack2, nil)
		preferredDatastore, err := tagPreferredDatastore(masterIp, allowedTopologyRacks[1],
			preferredDatastoreChosen, shareddatastoreListMap, nil)
		preferredDatastorePaths = append(preferredDatastorePaths, preferredDatastore...)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		defer func() {
			ginkgo.By("Remove preferred datastore tag")
			for i := 0; i < len(preferredDatastorePaths); i++ {
				err = detachTagCreatedOnPreferredDatastore(masterIp, preferredDatastorePaths[i],
					allowedTopologyRacks[1])
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
			}

		}()

		framework.Logf("Waiting for %v for preferred datastore to get refreshed in the environment",
			preferredDatastoreTimeOutInterval)
		time.Sleep(preferredDatastoreTimeOutInterval)

		ginkgo.By("Creating StorageClass for Statefulset")
		scSpec := getVSphereStorageClassSpec(defaultNginxStorageClassName, nil, allowedTopologyForRack2,
			"", bindingMode, false)
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
		ginkgo.By("Creating statefulset")
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
			namespace, allowedTopologies, false)

		ginkgo.By("Bring down the datastore")
		datastoreOp = "off"
		datastoreName := powerOffPreferredDatastore(ctx, &e2eVSphere, datastoreOp)

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
			allowedTopologies)

		// perform statefulset scaleup
		replicas = 13
		ginkgo.By("Scale up statefulset replica count from 3 to 10")
		scaleUpStatefulSetPod(ctx, client, statefulset, namespace, replicas, false)

		//verifying volume provisioning
		ginkgo.By("Verify volume is provisioned on the preferred datatsore")
		err = verifyVolumeProvisioningForStatefulSet(ctx, client, statefulset, namespace, preferredDatastorePaths,
			rack2DatastoreListMap, false, false)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Verify PV node affinity and that the PODS are running on " +
			"appropriate node as specified in the allowed topologies of SC")
		verifyPVnodeAffinityAndPODnodedetailsForStatefulsetsLevel5(ctx, client, statefulset,
			namespace, allowedTopologies, false)

		ginkgo.By("Bring up the datastore which was down")
		datastoreOp = "on"
		powerOnPreferredDatastore(datastoreName, datastoreOp)

		ginkgo.By("Remove preferred datastore tag chosen for volume provisioning")
		err = detachTagCreatedOnPreferredDatastore(masterIp, preferredDatastorePaths[0], allowedTopologyRacks[1])
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Tag new preferred datatsore for volume provisioning in rack-2(cluster-2)")
		preferredDatastore, err = tagPreferredDatastore(masterIp, allowedTopologyRacks[1],
			preferredDatastoreChosen, nonShareddatastoreListMapRack2, preferredDatastorePaths)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		preferredDatastorePaths = append(preferredDatastorePaths, preferredDatastore...)

		// perform statefulset scaleup
		replicas = 20
		ginkgo.By("Scale up statefulset replica count from 13 to 20")
		scaleUpStatefulSetPod(ctx, client, statefulset, namespace, replicas, false)

		//verifying volume provisioning
		ginkgo.By("Verify volume is provisioned on the preferred datatsore")
		err = verifyVolumeProvisioningForStatefulSet(ctx, client, statefulset, namespace, preferredDatastorePaths,
			rack2DatastoreListMap, false, false)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Verify PV node affinity and that the PODS are running on " +
			"appropriate node as specified in the allowed topologies of SC")
		verifyPVnodeAffinityAndPODnodedetailsForStatefulsetsLevel5(ctx, client, statefulset,
			namespace, allowedTopologies, false)
	})

	/*
		Testcase-20:
			Preferred datastore suspended which is on rack-1

			Steps
			1. Assign Tag rack-1, Category "cns.vmware.topology-preferred-datastores" to datastore
			specific to rack-1 (NFS-1)
			2. Assign Tag rack-1, Category "cns.vmware.topology-preferred-datastores" to datastore
			shared across site-1 and site-2 (sharedVMFS-12)
			3. Create SC with waitForFirstConsumer with allowed topology set to rack-1
			4. Create StatefulSet with replica count 10
			5. Wait for PVC to reach Bound and Pods to be in running state.
			6. Verify volume provisioning. It should provision volume on any of the preferred datatsore.
			7. Suspend the datastore or make datatsore inaccessible where volume has been provisioned
			(ex - sharedVMFS-12)
			8. Since datastore is suspended, the PVC's which were created on the inaccessible datatsore
			should go to inaccessible state. (On vCenter UI)
			9. Make sure Node affinity details on the PV shows proper details.
			10. Create new PVC
			11. Wait for PVC to reach Bound state.
			12. Describe PV and verify node affinity details.
			13. Verify volume provisioning. It should provision volume on the other datatsore.
			14. Create Pod using PVC created above.
			15. Wait for Pod to reach running state.
			16. POD should come up on the node which is present in the same details as mentioned in storage class
			17. Perform ScaleUp operation. Increase the replica count from 10 to 13.
			18. ScaleUp operation should fail sue to datastore disconnectivity.
			19. Add/ Attach the datastore back to the site-1
			20. Verify scaling should succeed now. It should start provisioning volume on both the datatsores.
			21. Verify PVC and Pod status.
			22. Verify volume provisioning.
			23. Change datatsores preference
			24. Perform scaleup operation again.
			25. Wait for new PVC's to reach Bound state and Pod to reach running state.
			26. Verify volume provisioning should be on the newly preferred datastore.
			27. Perform cleanup. Delete StatefulSet, PVC and PV, SC.
	*/

	ginkgo.It("Preferred datatsore is in suspended mode", func() {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		preferredDatastoreChosen = 1

		// choose preferred datastore
		ginkgo.By("Tag preferred datastore for volume provisioning in rack-1(cluster-1)")
		preferredDatastorePaths, err = tagPreferredDatastore(masterIp, allowedTopologyRacks[0],
			preferredDatastoreChosen, nonShareddatastoreListMapRack1, nil)
		preferredDatastore, err := tagPreferredDatastore(masterIp, allowedTopologyRacks[0],
			preferredDatastoreChosen, shareddatastoreListMap, nil)
		preferredDatastorePaths = append(preferredDatastorePaths, preferredDatastore...)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		defer func() {
			ginkgo.By("Remove preferred datastore tag")
			for i := 0; i < len(preferredDatastorePaths); i++ {
				err = detachTagCreatedOnPreferredDatastore(masterIp, preferredDatastorePaths[i],
					allowedTopologyRacks[0])
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
			}

		}()

		framework.Logf("Waiting for %v for preferred datastore to get refreshed in the environment",
			preferredDatastoreTimeOutInterval)
		time.Sleep(preferredDatastoreTimeOutInterval)

		ginkgo.By("Creating StorageClass for Statefulset")
		scSpec := getVSphereStorageClassSpec(defaultNginxStorageClassName, nil, allowedTopologyForRack1,
			"", bindingMode, false)
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
		ginkgo.By("Creating statefulset")
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
			namespace, allowedTopologies, false)

		ginkgo.By("Bring down the datastore")
		datastoreOp = "suspend"
		datastoreName := powerOffPreferredDatastore(ctx, &e2eVSphere, datastoreOp)

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
			allowedTopologies)

		// perform statefulset scaleup
		replicas = 13
		ginkgo.By("Scale up statefulset replica count from 3 to 10")
		scaleUpStatefulSetPod(ctx, client, statefulset, namespace, replicas, false)

		//verifying volume provisioning
		ginkgo.By("Verify volume is provisioned on the preferred datatsore")
		err = verifyVolumeProvisioningForStatefulSet(ctx, client, statefulset, namespace, preferredDatastorePaths,
			rack1DatastoreListMap, false, false)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Verify PV node affinity and that the PODS are running on " +
			"appropriate node as specified in the allowed topologies of SC")
		verifyPVnodeAffinityAndPODnodedetailsForStatefulsetsLevel5(ctx, client, statefulset,
			namespace, allowedTopologies, false)

		ginkgo.By("Bring up the datastore which was down")
		datastoreOp = "on"
		powerOnPreferredDatastore(datastoreName, datastoreOp)

		ginkgo.By("Remove preferred datastore tag chosen for volume provisioning")
		err = detachTagCreatedOnPreferredDatastore(masterIp, preferredDatastorePaths[0], allowedTopologyRacks[0])
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Tag new preferred datatsore for volume provisioning in rack-1(cluster-1)")
		preferredDatastore, err = tagPreferredDatastore(masterIp, allowedTopologyRacks[0],
			preferredDatastoreChosen, nonShareddatastoreListMapRack1, preferredDatastorePaths)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		preferredDatastorePaths = append(preferredDatastorePaths, preferredDatastore...)

		// perform statefulset scaleup
		replicas = 20
		ginkgo.By("Scale up statefulset replica count from 13 to 20")
		scaleUpStatefulSetPod(ctx, client, statefulset, namespace, replicas, false)

		//verifying volume provisioning
		ginkgo.By("Verify volume is provisioned on the preferred datatsore")
		err = verifyVolumeProvisioningForStatefulSet(ctx, client, statefulset, namespace, preferredDatastorePaths,
			rack1DatastoreListMap, false, false)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Verify PV node affinity and that the PODS are running on " +
			"appropriate node as specified in the allowed topologies of SC")
		verifyPVnodeAffinityAndPODnodedetailsForStatefulsetsLevel5(ctx, client, statefulset,
			namespace, allowedTopologies, false)
	})

})
