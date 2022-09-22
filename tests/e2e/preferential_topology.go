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
	"time"

	"github.com/onsi/ginkgo/v2"
	"github.com/onsi/gomega"
	"github.com/vmware/govmomi/object"
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

var _ = ginkgo.Describe("[Preferential-Topology] Preferential-Topology-Provisioning", func() {
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
		nonShareddatastoreListMapRack3 map[string]string
		allMasterIps                   []string
		masterIp                       string
		rack1DatastoreListMap          map[string]string
		rack2DatastoreListMap          map[string]string
		rack3DatastoreListMap          map[string]string
		dataCenters                    []*object.Datacenter
		clusters                       []string
		csiReplicas                    int32
		csiNamespace                   string
		preferredDatastorePaths        []string
		allowedTopologyRacks           []string
		allowedTopologyForRack1        []v1.TopologySelectorLabelRequirement
		allowedTopologyForRack2        []v1.TopologySelectorLabelRequirement
		allowedTopologyForRack3        []v1.TopologySelectorLabelRequirement
		err                            error
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
		bindingMode = storagev1.VolumeBindingWaitForFirstConsumer
		csiNamespace = GetAndExpectStringEnvVar(envCSINamespace)

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
		topologyMap := GetAndExpectStringEnvVar(topologyMap)
		topologyAffinityDetails, topologyCategories = createTopologyMapLevel5(topologyMap,
			topologyLength)
		allowedTopologies = createAllowedTopolgies(topologyMap, topologyLength)

		csiDeployment, err := client.AppsV1().Deployments(csiNamespace).Get(
			ctx, vSphereCSIControllerPodNamePrefix, metav1.GetOptions{})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		csiReplicas = *csiDeployment.Spec.Replicas

		// fetching list of datatstores shared between vm's
		shareddatastoreListMap, err = getListOfSharedDatastoresBetweenVMs(masterIp, dataCenters)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		// fetching list of datastores available in different racks
		rack1DatastoreListMap, err = getListOfDatastoresByClusterName(masterIp, clusters[0])
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		rack2DatastoreListMap, err = getListOfDatastoresByClusterName(masterIp, clusters[1])
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		rack3DatastoreListMap, err = getListOfDatastoresByClusterName(masterIp, clusters[2])
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		// fetching list of datastores which is specific to each rack
		nonShareddatastoreListMapRack1 = getNonSharedDatastoresInCluster(rack1DatastoreListMap,
			shareddatastoreListMap)
		nonShareddatastoreListMapRack2 = getNonSharedDatastoresInCluster(rack2DatastoreListMap,
			shareddatastoreListMap)
		nonShareddatastoreListMapRack3 = getNonSharedDatastoresInCluster(rack3DatastoreListMap,
			shareddatastoreListMap)

		// Get different allowed topologies required for creating Storage Class
		allowedTopologyForRack1 = getTopologySelector(topologyAffinityDetails, topologyCategories,
			topologyLength, leafNode, leafNodeTag0)
		allowedTopologyForRack2 = getTopologySelector(topologyAffinityDetails, topologyCategories,
			topologyLength, leafNode, leafNodeTag1)
		allowedTopologyForRack3 = getTopologySelector(topologyAffinityDetails, topologyCategories,
			topologyLength, leafNode, leafNodeTag2)
		allowedTopologyRacks = nil
		allowedTopologyForSC := getTopologySelector(topologyAffinityDetails, topologyCategories,
			topologyLength)
		for i := 4; i < len(allowedTopologyForSC); i++ {
			for j := 0; j < len(allowedTopologyForSC[i].Values); j++ {
				allowedTopologyRacks = append(allowedTopologyRacks, allowedTopologyForSC[i].Values[j])
			}
		}

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
		Testcase-1:
			Tag single datastore and verify it is honored

			Steps
			1. Assign Tag rack-2, Category "cns.vmware.topology-preferred-datastores" to datastore specific to
			rack-2. (ex- NFS-2)
			2. Create Storage Class SC1 with WFC binding mode and allowed topologies set to rack-2.
			i.e (region-1 > zone-1 > building-1 > level-1 > rack-2)
			3. Create StatefulSet using default pod management policy with replica 3 using SC1
			4. Wait for StatefulSet pods to come up.
			5. Wait for PV, PVC to reach bound and POD to reach running state
			6. Describe PV and verify node affinity details should contain allowed topology details.
			7. Verify volume should be provisioned on the selected preferred datastore of rack-2.
			8. Make sure POD is running on the same node as mentioned in the node affinity details
			9. Assign Tag rack-1, Category "cns.vmware.topology-preferred-datastores" to datastore specific to
			rack-1. (ex- NFS-1)
			10. Create Storage Class SC2 with Immediate binding mode and allowed topologies set to rack-1
			i.e region-1 > zone-1 > building-1 > level-1 > rack-1.
			11. Create PVC
			12. Wait for PVC to reach Bound state.
			13. Describe PV and verify node affinity details should contain allowed topologies.
			14. Verify volume should be provisioned on the selected preferred datastore of rack-1.
			15. Create standlone Pod from PVC created above.
			16. Wait for Pod to reach running state.
			17. Make sure Pod is running on the same node as mentioned in the node affinity details.
			18. Perform cleanup. Delete StatefulSet, PVC, PV and SC.
			19. Remove datastore preference tags as part of cleanup.
	*/
	ginkgo.It("Tag single preferred datastore each in rack-1 and rack-2 and verify it is honored", func() {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		preferredDatastoreChosen = 1
		preferredDatastorePaths = nil

		// choose preferred datastore
		ginkgo.By("Tag preferred datastore for volume provisioning in rack-2(cluster-2))")
		preferredDatastorePaths, err = tagPreferredDatastore(masterIp, allowedTopologyRacks[1],
			preferredDatastoreChosen, nonShareddatastoreListMapRack2, nil)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		preferredDatastoreRack2 := preferredDatastorePaths[0]
		defer func() {
			ginkgo.By("Remove preferred datastore tag")
			err = detachTagCreatedOnPreferredDatastore(masterIp, preferredDatastoreRack2,
				allowedTopologyRacks[1])
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
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

		ginkgo.By("Creating service")
		service := CreateService(namespace, client)
		defer func() {
			deleteService(namespace, client, service)
		}()

		// Creating Statefulset
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

		// verifying volume provisioning
		ginkgo.By("Verify volume is provisioned on the preferred datatsore")
		err = verifyVolumeProvisioningForStatefulSet(ctx, client, statefulset, namespace, preferredDatastorePaths,
			nonShareddatastoreListMapRack2, false, false)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Verify PV node affinity and that the PODS are running on " +
			"appropriate node as specified in the allowed topologies of SC")
		verifyPVnodeAffinityAndPODnodedetailsForStatefulsetsLevel5(ctx, client, statefulset,
			namespace, allowedTopologyForRack2, false)

		// choose preferred datastore in rack-1
		ginkgo.By("Tag preferred datatstore for volume provisioning in rack-1(cluster-1))")
		preferredDatastorePaths, err = tagPreferredDatastore(masterIp, allowedTopologyRacks[0],
			preferredDatastoreChosen, nonShareddatastoreListMapRack1, nil)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		defer func() {
			ginkgo.By("Remove preferred datastore tag")
			err = detachTagCreatedOnPreferredDatastore(masterIp, preferredDatastorePaths[0],
				allowedTopologyRacks[0])
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}()

		framework.Logf("Waiting for %v for preferred datastore to get refreshed in the environment",
			preferredDatastoreTimeOutInterval)
		time.Sleep(preferredDatastoreTimeOutInterval)

		ginkgo.By("Creating Storage class and standalone PVC")
		storageclass1, pvclaim, err := createPVCAndStorageClass(client, namespace, nil, nil, "",
			allowedTopologyForRack1, "", false, "")
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		defer func() {
			err = client.StorageV1().StorageClasses().Delete(ctx, storageclass1.Name, *metav1.NewDeleteOptions(0))
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}()

		//Wait for PVC to reach Bound state
		ginkgo.By("Expect claim to provision volume successfully")
		pvs1, err := fpv.WaitForPVClaimBoundPhase(client,
			[]*v1.PersistentVolumeClaim{pvclaim}, framework.ClaimProvisionTimeout)
		gomega.Expect(err).NotTo(gomega.HaveOccurred(), "Failed to provision volume")
		pv1 := pvs1[0]
		defer func() {
			err = fpv.DeletePersistentVolumeClaim(client, pvclaim.Name, pvclaim.Namespace)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			err = e2eVSphere.waitForCNSVolumeToBeDeleted(pv1.Spec.CSI.VolumeHandle)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}()

		// Creating Pod and verifying volume is attached to the node
		ginkgo.By("Creating a pod")
		pod, err := createPod(client, namespace, nil, []*v1.PersistentVolumeClaim{pvclaim}, false, "")
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By(fmt.Sprintf("Verify volume:%s is attached to the node: %s",
			pv1.Spec.CSI.VolumeHandle, pod.Spec.NodeName))
		vmUUID := getNodeUUID(ctx, client, pod.Spec.NodeName)
		isDiskAttached, err := e2eVSphere.isVolumeAttachedToVM(client, pv1.Spec.CSI.VolumeHandle, vmUUID)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		gomega.Expect(isDiskAttached).To(gomega.BeTrue(), "Volume is not attached to the node")
		defer func() {
			ginkgo.By("Deleting the pod and wait for disk to detach")
			err := fpod.DeletePodWithWait(client, pod)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())

			ginkgo.By("Verify volume is detached from the node")
			isDiskDetached, err := e2eVSphere.waitForVolumeDetachedFromNode(client,
				pv1.Spec.CSI.VolumeHandle, pod.Spec.NodeName)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			gomega.Expect(isDiskDetached).To(gomega.BeTrue(),
				fmt.Sprintf("Volume %q is not detached from the node %q", pv1.Spec.CSI.VolumeHandle, pod.Spec.NodeName))
		}()

		// verifying volume provisioning
		ginkgo.By("Verify volume is provisioned on the preferred datatsore")
		verifyVolumeProvisioningForStandalonePods(ctx, client, pod, namespace, preferredDatastorePaths,
			nonShareddatastoreListMapRack1)

		ginkgo.By("Verify PV node affinity and that the PODS are running on appropriate " +
			"node as specified in the allowed topologies of SC")
		verifyPVnodeAffinityAndPODnodedetailsFoStandalonePodLevel5(ctx, client, pod, namespace,
			allowedTopologyForRack1)
	})

	/*
		Testcase-2:
			Tag multiple datastore specific to rack-3 and verify it is honored

			Steps
			1. Assign Tag rack-3, Category "cns.vmware.topology-preferred-datastores" to
			datastore specific to rack-3 (ex- NFS-3)
			2. Assign Tag rack-3, Category "cns.vmware.topology-preferred-datastores" to datastore
			shared across all racks (ex- sharedVMFS-12)
			3. Create SC with WFC binding mode and allowed topologies set to rack-3 in the SC
			4. Create StatefulSet with replica 3 with the above SC
			5. Wait for all the StatefulSet to come up
			6. Wait for PV , PVC to reach Bound and POD to reach running state
			7. Describe PV and verify node affinity details should contain allowed topology details.
			8. Verify volume should be provisioned on any of the preferred datastores of rack-3.
			9. Make sure POD is running on the same node as mentioned in the node affinity details
			10. Perform Scaleup of StatefulSet. Increase the replica count from 3 to 10.
			11. Wait for all the StatefulSet to come up
			12. Wait for PV , PVC to reach bound and POD to reach running state
			13. Describe PV and verify node affinity details should contain allowed topology details.
			14. Verify volume should be provisioned on any of the preferred datastores of rack-3.
			15. Make sure POD is running on the same node as mentioned in the node affinity details
			16. Perform cleanup. Delete StatefulSet, PVC, PV and SC.
			17. Remove datastore preference tags as part of cleanup.
	*/
	ginkgo.It("Tag multiple preferred datastores in rack-3 and verify it is honored", func() {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		preferredDatastoreChosen = 1
		preferredDatastorePaths = nil

		// choose preferred datastore
		ginkgo.By("Tag preferred datastore for volume provisioning in rack-3(cluster-3))")
		preferredDatastorePaths, err := tagPreferredDatastore(masterIp, allowedTopologyRacks[2],
			preferredDatastoreChosen, nonShareddatastoreListMapRack3, nil)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		preferredDatastore, err := tagPreferredDatastore(masterIp, allowedTopologyRacks[2],
			preferredDatastoreChosen, shareddatastoreListMap, nil)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		preferredDatastorePaths = append(preferredDatastorePaths, preferredDatastore...)
		defer func() {
			ginkgo.By("Remove preferred datatsore tag")
			for i := 0; i < len(preferredDatastorePaths); i++ {
				err = detachTagCreatedOnPreferredDatastore(masterIp, preferredDatastorePaths[i],
					allowedTopologyRacks[2])
			}
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}()

		framework.Logf("Waiting for %v for preferred datastore to get refreshed in the environment",
			preferredDatastoreTimeOutInterval)
		time.Sleep(preferredDatastoreTimeOutInterval)

		ginkgo.By("Creating StorageClass for Statefulset")
		scSpec := getVSphereStorageClassSpec(defaultNginxStorageClassName, nil, allowedTopologyForRack3,
			"", bindingMode, false)
		sc, err := client.StorageV1().StorageClasses().Create(ctx, scSpec, metav1.CreateOptions{})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		defer func() {
			err = client.StorageV1().StorageClasses().Delete(ctx, sc.Name, *metav1.NewDeleteOptions(0))
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}()

		ginkgo.By("Creating service")
		service := CreateService(namespace, client)
		defer func() {
			deleteService(namespace, client, service)
		}()

		// Creating statefulset
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
			rack3DatastoreListMap, false, false)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Verify PV node affinity and that the PODS are running on " +
			"appropriate node as specified in the allowed topologies of SC")
		verifyPVnodeAffinityAndPODnodedetailsForStatefulsetsLevel5(ctx, client, statefulset,
			namespace, allowedTopologyForRack3, false)

		// perform statefulset scaleup
		replicas = 10
		ginkgo.By("Scale up statefulset replica count from 3 to 10")
		scaleUpStatefulSetPod(ctx, client, statefulset, namespace, replicas, false)

		//verifying volume provisioning
		ginkgo.By("Verify volume is provisioned on the preferred datatsore")
		err = verifyVolumeProvisioningForStatefulSet(ctx, client, statefulset, namespace, preferredDatastorePaths,
			rack3DatastoreListMap, false, false)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Verify PV node affinity and that the PODS are running on " +
			"appropriate node as specified in the allowed topologies of SC")
		verifyPVnodeAffinityAndPODnodedetailsForStatefulsetsLevel5(ctx, client, statefulset,
			namespace, allowedTopologyForRack3, false)
	})

	/*
		Testcase-3:
			Tag multiple datastore specific to rack-2 and verify it is honored

			Steps
			1. Assign Tag rack-2, Category "cns.vmware.topology-preferred-datastores" to datastore specific
			to rack-2 (ex- NFS-2)
			2. Assign Tag rack-2, Category "cns.vmware.topology-preferred-datastores" to datastore
			specific to rack-2 (ex- vSAN-2)
			3. Assign Tag rack-2, Category "cns.vmware.topology-preferred-datastores" to datastore
			shared across all racks (ex- NFS-12)
			4. Create SC with Immediate binding mode and allowed topologies set to rack-2 in the SC
			5. Create StatefulSet with parallel pod management policy and replica 3 with the above SC
			6. Wait for all the StatefulSet to come up
			7. Wait for PV , PVC to reach bound and POD to reach running state
			8. Describe PV and verify node affinity details should contain allowed topology details.
			9. Verify volume should be provisioned on any of the preferred datastores of rack-2
			10. Perform Scaleup of StatefulSet. Increase the replica count from 3 to 10.
			11. Wait for all the StatefulSet to come up
			12. Wait for PV , PVC to reach bound and POD to reach running state
			13. Describe PV and verify node affinity details should contain specified allowed topology details.
			14. Verify volume should be provisioned on any of the preferred datastores of rack-2
			15. Make sure POD is running on the same node as mentioned in the node affinity details
			16. Perform ScaleDown of StatefulSet. Decrease the replica count from 10 to 5.
			17. Verify scaledown operation went successful.
			18. Perform cleanup. Delete StatefulSet, PVC, PV and SC.
			19. Remove datastore preference tags as part of cleanup.
	*/
	ginkgo.It("Tag multiple preferred datastores in rack-2 and verify it is honored", func() {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		preferredDatastoreChosen = 2
		preferredDatastorePaths = nil

		// choose preferred datastore
		ginkgo.By("Tag preferred datatsore for volume provisioning in rack-2(cluster-2))")
		preferredDatastorePaths, err := tagPreferredDatastore(masterIp, allowedTopologyRacks[1],
			preferredDatastoreChosen, nonShareddatastoreListMapRack2, nil)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		preferredDatastoreChosen = 1
		preferredDatastore, err := tagPreferredDatastore(masterIp, allowedTopologyRacks[1],
			preferredDatastoreChosen, shareddatastoreListMap, nil)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		preferredDatastorePaths = append(preferredDatastorePaths, preferredDatastore...)
		defer func() {
			ginkgo.By("Remove preferred datastore tag")
			for i := 0; i < len(preferredDatastorePaths); i++ {
				err = detachTagCreatedOnPreferredDatastore(masterIp, preferredDatastorePaths[i],
					allowedTopologyRacks[1])
			}
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
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

		ginkgo.By("Creating service")
		service := CreateService(namespace, client)
		defer func() {
			deleteService(namespace, client, service)
		}()

		// Creating statefulset with 3 replicas
		ginkgo.By("Creating statefulset with 3 replica")
		statefulset := GetStatefulSetFromManifest(namespace)
		statefulset.Spec.PodManagementPolicy = appsv1.ParallelPodManagement
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
			namespace, allowedTopologyForRack2, false)

		// perform statefulset scaleup
		replicas = 10
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
			namespace, allowedTopologyForRack2, false)

		// perform statefulset scaledown
		replicas = 5
		ginkgo.By("Scale down statefulset replica count from 10 to 5")
		scaleDownStatefulSetPod(ctx, client, statefulset, namespace, replicas, false)
	})

	/*
		Testcase-4:
			Tag preferred datastore to rack-1 which is accessible across all racks and
			verify it is honored

			Steps
			1. Assign Tag rack-1, Category "cns.vmware.topology-preferred-datastores" to datastore
			accessible across all racks. (ex - sharedVMFS-12)
			2. Create SC with WFC binding mode and allowed topologies set to all rack levels in the SC
			3. Create StatefulSet with replica 3 with the above SC
			4. Wait for all the StatefulSet to come up
			5. Wait for PV, PVC to reach Bound and POD to reach running state
			6. Describe PV and verify node affinity details should contain allowed topology details.
			7. Verify atleast one of the sts volume provisioning should be on the preferred datastore.
			8. Make sure POD is running on the same node as mentioned in node affinity details
			9. Perform cleanup. Delete StatefulSet, PVC, PV and SC.
			10. Remove datastore preference tags as part of cleanup.
	*/
	ginkgo.It("Assign preferred tag to shared datastore which is accessible across all racks "+
		"and verify it is honored", func() {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		preferredDatastoreChosen = 1
		preferredDatastorePaths = nil

		// choose preferred datastore
		ginkgo.By("Tag preferred datatsore for volume provisioning in rack-1(cluster-1))")
		preferredDatastorePaths, err := tagPreferredDatastore(masterIp, allowedTopologyRacks[0],
			preferredDatastoreChosen, shareddatastoreListMap, nil)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		defer func() {
			ginkgo.By("Remove preferred datatsore tag")
			err = detachTagCreatedOnPreferredDatastore(masterIp, preferredDatastorePaths[0],
				allowedTopologyRacks[0])
			gomega.Expect(err).NotTo(gomega.HaveOccurred())

		}()

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

		ginkgo.By("Creating service")
		service := CreateService(namespace, client)
		defer func() {
			deleteService(namespace, client, service)
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
			shareddatastoreListMap, true, false)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Verify PV node affinity and that the PODS are running on " +
			"appropriate node as specified in the allowed topologies of SC")
		verifyPVnodeAffinityAndPODnodedetailsForStatefulsetsLevel5(ctx, client, statefulset,
			namespace, allowedTopologies, false)
	})

	/*
		Testcase-5:
			Multiple tags are assigned to Shared datastore with single allowed topology

			Steps
			1. Assign Tag rack-1, Category "cns.vmware.topology-preferred-datastores" to datastore
			accessible across all racks (ex - sharedVMFS-12)
			2. Assign Tag rack-2, Category "cns.vmware.topology-preferred-datastores" to datastore
			accessible across all racks (ex - sharedVMFS-12)
			3. Create SC with WFC binding mode and allowed topologies set to rack-2 in the SC
			4. Create StatefulSet with replica 3 with the above SC
			5. Wait for all the StatefulSet to come up
			6. Wait for PV , PVC to reach Bound and POD to reach running state
			7. Describe PV and verify node affinity details should contain allowed topology details.
			8. Verify volume provisioning should be on the preferred datastore.
			9. Make sure POD is running on the same node as mentioned in node affinity details
			10. Perform cleanup. Delete StatefulSet, PVC, PV and SC.
			11. Remove datastore preference tags as part of cleanup.
	*/
	ginkgo.It("Assign multiple preferred tags to shared datastore with single allowed topology set", func() {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		preferredDatastoreChosen = 1
		preferredDatastorePaths = nil

		// choose preferred datastore
		ginkgo.By("Tag preferred datatsore for volume provisioning in rack-2(cluster-2))")
		preferredDatastorePaths, err := tagPreferredDatastore(masterIp, allowedTopologyRacks[0],
			preferredDatastoreChosen, shareddatastoreListMap, nil)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		err = tagSameDatastoreAsPreferenceToDifferentRacks(masterIp, allowedTopologyRacks[1],
			preferredDatastoreChosen, preferredDatastorePaths)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		defer func() {
			ginkgo.By("Remove preferred datatsore tag")
			for j := 0; j < len(allowedTopologyRacks)-1; j++ {
				err = detachTagCreatedOnPreferredDatastore(masterIp, preferredDatastorePaths[0],
					allowedTopologyRacks[j])
			}
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
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

		ginkgo.By("Creating service")
		service := CreateService(namespace, client)
		defer func() {
			deleteService(namespace, client, service)
		}()

		// Creating Statefulset
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
			shareddatastoreListMap, false, false)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Verify PV node affinity and that the PODS are running on " +
			"appropriate node as specified in the allowed topologies of SC")
		verifyPVnodeAffinityAndPODnodedetailsForStatefulsetsLevel5(ctx, client, statefulset,
			namespace, allowedTopologyForRack2, false)
	})
})
