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
		allDatastoresListMap           map[string]string
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
		isSPSServiceStopped            bool
		vcAddress                      string
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
		isSPSServiceStopped = false
		vcAddress = e2eVSphere.Config.Global.VCenterHostname + ":" + sshdPort

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

		// fetching csi pods replicas
		csiNamespace = GetAndExpectStringEnvVar(envCSINamespace)
		csiDeployment, err := client.AppsV1().Deployments(csiNamespace).Get(
			ctx, vSphereCSIControllerPodNamePrefix, metav1.GetOptions{})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		csiReplicas = *csiDeployment.Spec.Replicas

		// fetching list of datatstores shared between vm's
		shareddatastoreListMap, err = getListOfSharedDatastoresBetweenVMs(masterIp, dataCenters)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		// fetching list of datatstores available in a testbed
		allDatastoresListMap, err = getListOfAvailableDatastores(masterIp)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		framework.Logf("Fetching list of datastores available in different racks")
		rack1DatastoreListMap, err = getListOfDatastoresByClusterName(masterIp, clusters[0])
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		rack2DatastoreListMap, err = getListOfDatastoresByClusterName(masterIp, clusters[1])
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		rack3DatastoreListMap, err = getListOfDatastoresByClusterName(masterIp, clusters[2])
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		framework.Logf("Fetching list of datastores which is specific to each rack")
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
		// fetching cluster topology racks level
		allowedTopologyForSC := getTopologySelector(topologyAffinityDetails, topologyCategories,
			topologyLength)
		for i := 4; i < len(allowedTopologyForSC); i++ {
			for j := 0; j < len(allowedTopologyForSC[i].Values); j++ {
				allowedTopologyRacks = append(allowedTopologyRacks, allowedTopologyForSC[i].Values[j])
			}
		}
		csiNamespace = GetAndExpectStringEnvVar(envCSINamespace)

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

		if isSPSServiceStopped {
			framework.Logf("Bringing sps up before terminating the test")
			startVCServiceWait4VPs(ctx, vcAddress, spsServiceName, &isSPSServiceStopped)
		}
	})

	/*
		Testcase-1:
			Tag single datastore and verify it is honored

			Steps
			1. Assign Tag rack-2, Category "cns.vmware.topology-preferred-datastores" to datastore specific to
			rack-2. (ex- NFS-2)
			2. Create Storage Class SC1 with WFC binding mode and allowed topologies set to rack-2.
			3. Create StatefulSet sts1 using default pod management policy with replica 3 using SC1
			4. Wait for StatefulSet sts1 to come up.
			5. Wait for PV, PVC to reach bound and POD to reach running state
			6. Describe PV and verify node affinity details should contain allowed topology details.
			7. Verify volume should be provisioned on the selected preferred datastore of rack-2.
			8. Make sure POD is running on the same node as mentioned in the node affinity details
			9. Assign Tag rack-1, Category "cns.vmware.topology-preferred-datastores" to datastore specific to
			rack-1. (ex- NFS-1)
			10. Create Storage Class SC2 with Immediate binding mode and allowed topologies set to
			region-1 > zone-1 > building-1 > level-1 > rack-1.
			11. Create PVC-1
			12. Wait for PVC-1 to reach Bound state.
			13. Describe PV and verify node affinity details should contain allowed topologies.
			14. Verify volume should be provisioned on the selected preferred datastore of rack-1.
			15. Create Pod from PVC-1 created above.
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

		ginkgo.By("Creating service")
		service := CreateService(namespace, client)
		defer func() {
			deleteService(namespace, client, service)
		}()

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
			nonShareddatastoreListMapRack2, false, false)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Verify PV node affinity and that the PODS are running on " +
			"appropriate node as specified in the allowed topologies of SC")
		verifyPVnodeAffinityAndPODnodedetailsForStatefulsetsLevel5(ctx, client, statefulset,
			namespace, allowedTopologies, false)

		// choose preferred datastore
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

		// Creating Storage Class and PVC
		ginkgo.By("Creating Storage class and standalone PVC")
		storageclass, pvclaim, err := createPVCAndStorageClass(client, namespace, nil, nil, "",
			allowedTopologyForRack1, "", false, "")
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		defer func() {
			err = client.StorageV1().StorageClasses().Delete(ctx, storageclass.Name, *metav1.NewDeleteOptions(0))
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
			allowedTopologies)
	})

	/*
		Testcase-2:
			Tag multiple datastore specific to rack-3 and verify it is honored

			Steps
			1. Assign Tag rack-3, Category "cns.vmware.topology-preferred-datastores" to
			datastore specific to rack-3 (ex- NFS-1)
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
			rack3DatastoreListMap, false, false)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Verify PV node affinity and that the PODS are running on " +
			"appropriate node as specified in the allowed topologies of SC")
		verifyPVnodeAffinityAndPODnodedetailsForStatefulsetsLevel5(ctx, client, statefulset,
			namespace, allowedTopologies, false)

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
			namespace, allowedTopologies, false)
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

		ginkgo.By("Creating service")
		service := CreateService(namespace, client)
		defer func() {
			deleteService(namespace, client, service)
		}()

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
			namespace, allowedTopologies, false)

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
			namespace, allowedTopologies, false)

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
	ginkgo.It("Assign preferred tag to shared datastore which is shared across all racks "+
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

		ginkgo.By("Creating service")
		service := CreateService(namespace, client)
		defer func() {
			deleteService(namespace, client, service)
		}()

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

		ginkgo.By("Creating service")
		service := CreateService(namespace, client)
		defer func() {
			deleteService(namespace, client, service)
		}()

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
			namespace, allowedTopologies, false)
	})

	/*
		Testcase-6:
			Multiple tags are assigned to Shared datastore with multiple allowed topology

			Steps
			1. Assign Tag rack-1, Category "cns.vmware.topology-preferred-datastores" to datastore
			accessible across all racks (ex - sharedVMFS-12)
			2. Assign Tag rack-2, Category "cns.vmware.topology-preferred-datastores" to datastore
			accessible across all racks (ex - sharedVMFS-12)
			3. Assign Tag rack-3, Category "cns.vmware.topology-preferred-datastores" to datastore
			accessible across all racks (ex - sharedVMFS-12)
			4. Create SC with WFC binding mode and allowed topologies set to rack-1/rack-2/rack-3 in the SC
			5. Create StatefulSet with replica 3 with the above SC
			6. Wait for all the StatefulSet to come up
			7. Wait for PV , PVC to reach bound and POD to reach running state
			8. Describe PV and verify node affinity details should contain allowed topology details.
			9. Verify volume provisioning should be on the preferred datastore.
			10. Make sure POD is running on the same node as mentioned in node affinity details
			11. Perform cleanup. Delete StatefulSet, PVC, PV and SC.
			12. Remove datastore preference tags as part of cleanup.
	*/
	ginkgo.It("Assign multiple tags to preferred shared datastore which is shared across all "+
		"racks with multiple allowed topologies", func() {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		preferredDatastoreChosen = 1
		preferredDatastorePaths = nil

		// choose preferred datastore
		ginkgo.By("Tag preferred datatsore for volume provisioning")
		preferredDatastorePaths, err := tagPreferredDatastore(masterIp, allowedTopologyRacks[0],
			preferredDatastoreChosen, shareddatastoreListMap, nil)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		err = tagSameDatastoreAsPreferenceToDifferentRacks(masterIp, allowedTopologyRacks[1],
			preferredDatastoreChosen, preferredDatastorePaths)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		err = tagSameDatastoreAsPreferenceToDifferentRacks(masterIp, allowedTopologyRacks[2],
			preferredDatastoreChosen, preferredDatastorePaths)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		defer func() {
			ginkgo.By("Remove preferred datatsore tag")
			for j := 0; j < len(allowedTopologyRacks); j++ {
				err = detachTagCreatedOnPreferredDatastore(masterIp, preferredDatastorePaths[0],
					allowedTopologyRacks[j])
			}
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}()

		framework.Logf("Waiting for %v for preferred datastore to get refreshed in the environment",
			preferredDatastoreTimeOutInterval)
		time.Sleep(preferredDatastoreTimeOutInterval)

		ginkgo.By("Creating service")
		service := CreateService(namespace, client)
		defer func() {
			deleteService(namespace, client, service)
		}()

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
			shareddatastoreListMap, true, false)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Verify PV node affinity and that the PODS are running on " +
			"appropriate node as specified in the allowed topologies of SC")
		verifyPVnodeAffinityAndPODnodedetailsForStatefulsetsLevel5(ctx, client, statefulset,
			namespace, allowedTopologies, false)
	})

	/*
		Testcase-7:
			Single tag is assigned to Shared datastore with multiple allowed topology

			Steps
			1. Assign Tag rack-3, Category "cns.vmware.topology-preferred-datastores" to
			datastore accessible across all racks (ex - sharedVMFS-12)
			2. Create SC with WFC binding mode and allowed topologies set to rack-1/rack-2/rack-3 in the SC
			3. Create StatefulSet with replica 3 with the above SC
			4. Wait for all the StatefulSet to come up
			5. Wait for PV , PVC to reach Bound and POD to reach running state
			6. Describe PV and verify node affinity details.
			7. Verify atleast one of sts volume provisioning should be on the preferred datastore.
			8. Make sure POD is running on the same node as mentioned in node affinity details
			9. Perform cleanup. Delete StatefulSet, PVC, PV and SC.
			10. Remove datastore preference tags as part of cleanup.
	*/
	ginkgo.It("Assign single tag to preferred shared datastore which is shared across all "+
		"racks with multiple allowed topologies", func() {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		preferredDatastoreChosen = 1
		preferredDatastorePaths = nil

		// choose preferred datastore
		ginkgo.By("Tag preferred datastore for volume provisioning")
		preferredDatastorePaths, err := tagPreferredDatastore(masterIp, allowedTopologyRacks[2],
			preferredDatastoreChosen, shareddatastoreListMap, nil)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		defer func() {
			ginkgo.By("Remove the preferred datastore tag")
			err = detachTagCreatedOnPreferredDatastore(masterIp, preferredDatastorePaths[0],
				allowedTopologyRacks[2])
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}()

		framework.Logf("Waiting for %v for preferred datastore to get refreshed in the environment",
			preferredDatastoreTimeOutInterval)
		time.Sleep(preferredDatastoreTimeOutInterval)

		ginkgo.By("Creating service")
		service := CreateService(namespace, client)
		defer func() {
			deleteService(namespace, client, service)
		}()

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
			shareddatastoreListMap, true, false)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Verify PV node affinity and that the PODS are running on " +
			"appropriate node as specified in the allowed topologies of SC")
		verifyPVnodeAffinityAndPODnodedetailsForStatefulsetsLevel5(ctx, client, statefulset,
			namespace, allowedTopologies, false)
	})

	/*
		Testcase-8:
			Change datastore preference in rack-1 and verify it is honored

			Steps
			1. Assign Tag rack-1, Category "cns.vmware.topology-preferred-datastores" to datastore
			specific to rack-1 (ex - vSAN-1)
			2. Create SC with  Immediate Binding mode and allowed topologies set to rack-1 in the SC
			3. Create StatefulSet sts1 with parallel pod management policy and replica 3 with the above SC
			4. Wait for all the StatefulSet to come up
			5. Wait for PV , PVC to reach Bound and POD to reach running state
			6. Describe PV and verify node affinity details should contain allowed topology details.
			7. Verify volume should be provisioned on the preferred datastore of rack-1
			8. Make sure POD is running on the same node as mentioned in the node affinity details
			9. Remove tag from previously preferred datastore and assign tag rack-1 and category
			"cns.vmware.topology-preferred-datastores" to new datastore shared across all racks (ex - sharedVMFS-12)
			10. Wait for the refresh interval time which is set at which preferred datatsore
			is picked up for volume provisioning.
			11. Create StatefulSet sts2 with replica 3 with the above SC
			12. Wait for all the StatefulSet to come up
			13. Wait for PV , PVC to reach Bound and POD to reach running state
			14. Describe PV and verify node affinity details should allowed topology details.
			15. Verify volume should be provisioned on the newly selected preferred datastore of rack-2
			16. Make sure POD is running on the same node as mentioned in the node affinity details
			17. Perform cleanup. Delete StatefulSet, PVC, PV and SC.
			18. Remove datastore preference tags as part of cleanup.
	*/
	ginkgo.It("Change datatsore preferences in rack-1 and verify it is honored", func() {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		preferredDatastoreChosen = 1
		preferredDatastorePaths = nil

		// choose preferred datastore
		ginkgo.By("Tag preferred datastore for volume provisioning in rack-1(cluster-1))")
		preferredDatastorePaths, err := tagPreferredDatastore(masterIp, allowedTopologyRacks[0],
			preferredDatastoreChosen, nonShareddatastoreListMapRack1, nil)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		framework.Logf("Waiting for %v for preferred datastore to get refreshed in the environment",
			preferredDatastoreTimeOutInterval)
		time.Sleep(preferredDatastoreTimeOutInterval)

		ginkgo.By("Creating service")
		service := CreateService(namespace, client)
		defer func() {
			deleteService(namespace, client, service)
		}()

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
		ginkgo.By("Creating Statefulset with 3 replica")
		sts1 := GetStatefulSetFromManifest(namespace)
		sts1.Spec.PodManagementPolicy = appsv1.ParallelPodManagement
		ginkgo.By("Creating statefulset")
		CreateStatefulSet(namespace, sts1, client)
		sts1Replicas := *(sts1.Spec.Replicas)
		defer func() {
			framework.Logf("Deleting all statefulset in namespace: %v", namespace)
			DeleteAllStsInGivenNamespace(client, namespace)
		}()

		// Waiting for pods status to be Ready.
		fss.WaitForStatusReadyReplicas(client, sts1, sts1Replicas)
		gomega.Expect(fss.CheckMount(client, sts1, mountPath)).NotTo(gomega.HaveOccurred())
		ssPodsBeforeScaleDown := GetListOfPodsInSts(client, sts1)
		gomega.Expect(ssPodsBeforeScaleDown.Items).NotTo(gomega.BeEmpty(),
			fmt.Sprintf("Unable to get list of Pods from the Statefulset: %v", sts1.Name))
		gomega.Expect(len(ssPodsBeforeScaleDown.Items) == int(sts1Replicas)).To(gomega.BeTrue(),
			"Number of Pods in the statefulset should match with number of replicas")

		//verifying volume provisioning
		ginkgo.By("Verify volume is provisioned on the preferred datatsore")
		err = verifyVolumeProvisioningForStatefulSet(ctx, client, sts1, namespace, preferredDatastorePaths,
			nonShareddatastoreListMapRack1, false, true)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Verify PV node affinity and that the PODS are running on " +
			"appropriate node as specified in the allowed topologies of SC")
		verifyPVnodeAffinityAndPODnodedetailsForStatefulsetsLevel5(ctx, client, sts1,
			namespace, allowedTopologies, true)

		ginkgo.By("Remove preferred datatsore tag which is chosen for volume provisioning")
		err = detachTagCreatedOnPreferredDatastore(masterIp, preferredDatastorePaths[0], allowedTopologyRacks[0])
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Tag new preferred datastore from rack-1 for volume provisioning")
		preferredDatastorePaths, err = tagPreferredDatastore(masterIp, allowedTopologyRacks[0],
			preferredDatastoreChosen, shareddatastoreListMap, nil)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		defer func() {
			ginkgo.By("Remove the datastore preference chosen for volume provisioning")
			err = detachTagCreatedOnPreferredDatastore(masterIp, preferredDatastorePaths[0], allowedTopologyRacks[0])
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}()

		framework.Logf("Waiting for %v for preferred datastore to get refreshed in the environment",
			preferredDatastoreTimeOutInterval)
		time.Sleep(preferredDatastoreTimeOutInterval)

		// Creating statefulset2 with 3 replicas
		ginkgo.By("Creating statefulset2 with 3 replica")
		sts2 := GetStatefulSetFromManifest(namespace)
		sts2.Name = "sts"
		sts2Replicas := *(sts2.Spec.Replicas)
		stsReplicas := sts1Replicas + sts2Replicas
		ginkgo.By("Creating statefulset1")
		createMultipleStatefulSetInSameNS(namespace, sts2, client, stsReplicas)

		// Waiting for pods status to be Ready.
		fss.WaitForStatusReadyReplicas(client, sts2, sts2Replicas)
		gomega.Expect(fss.CheckMount(client, sts2, mountPath)).NotTo(gomega.HaveOccurred())
		ssPodsBeforeScaleDown = GetListOfPodsInSts(client, sts2)
		gomega.Expect(ssPodsBeforeScaleDown.Items).NotTo(gomega.BeEmpty(),
			fmt.Sprintf("Unable to get list of Pods from the Statefulset: %v", sts2.Name))
		gomega.Expect(len(ssPodsBeforeScaleDown.Items) == int(sts2Replicas)).To(gomega.BeTrue(),
			"Number of Pods in the statefulset should match with number of replicas")

		//verifying volume provisioning
		ginkgo.By("Verify volume is provisioned on the preferred datatsore")
		err = verifyVolumeProvisioningForStatefulSet(ctx, client, sts2, namespace, preferredDatastorePaths,
			shareddatastoreListMap, false, true)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Verify PV node affinity and that the PODS are running on " +
			"appropriate node as specified in the allowed topologies of SC")
		verifyPVnodeAffinityAndPODnodedetailsForStatefulsetsLevel5(ctx, client, sts2,
			namespace, allowedTopologies, true)
	})

	/*
		Testcase-9:
			Change datastore preference in rack-3 and verify it is honored

			Steps
			1. Assign Tag rack-3, Category "cns.vmware.topology-preferred-datastores" to datastore
			specific to rack-3 (ex - vSAN-2)
			2. Create SC with WFC and allowed topologies set to rack-3 in the SC
			3. Create StatefulSet sts1 with replica 3 with the above SC
			4. Wait for all the StatefulSet to come up
			5. Wait for PV , PVC to reach Bound and POD to reach Running state
			6. Describe PV and verify node affinity details should contain allowed topology details.
			7. Verify volume should be provisioned on the selected preferred datastore of rack-3.
			8. Make sure POD is running on the same node as mentioned in node affinity details
			9. Remove tags from previously preferred datastore and assign tag rack-3 and category
			"cns.vmware.topology-preferred-datastores" to new datastore specific to rack-3 (ex - NFS-2)
			10. Wait for the refresh interval time which is set at which preferred datatsore
			is picked up for volume provisioning.
			11. Create StatefulSet sts2 with replica 3 with the above SC
			12. Wait for all the StatefulSet to come up
			13. Describe PV and verify node affinity details should contain the specified allowed topology details.
			14. Verify volume should be provisioned on the newly selected preferred datastore of rack-3
			15. Scaleup StatefulSet sts1 created in step3. Increase the replica count from 3 to 10.
			16. Wait for all the StatefulSet to come up
			17. Wait for PV , PVC to reach Bound and POD to reach running state
			18. Describe PV and verify node affinity details should contain both specified allowed topologies
			 details.
			19. Verify volume should be provisioned on the newly selected preferred datastore of rack-3.
			20. Make sure POD is running on the same node as mentioned in the node affinity details
			21. Perform cleanup. Delete StatefulSet, PVC, PV and SC.
			22. Remove datastore preference tags as part of cleanup.
	*/
	ginkgo.It("Change datatsore preferences in rack-3 and verify it is honored", func() {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		preferredDatastoreChosen = 1
		preferredDatastorePaths = nil

		// choose preferred datastore
		ginkgo.By("Tag preferred datastore for volume provisioning in rack-3(cluster-3))")
		preferredDatastorePaths, err := tagPreferredDatastore(masterIp, allowedTopologyRacks[2],
			preferredDatastoreChosen, nonShareddatastoreListMapRack3, nil)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		preferredDatastore := preferredDatastorePaths

		framework.Logf("Waiting for %v for preferred datastore to get refreshed in the environment",
			preferredDatastoreTimeOutInterval)
		time.Sleep(preferredDatastoreTimeOutInterval)

		ginkgo.By("Creating service")
		service := CreateService(namespace, client)
		defer func() {
			deleteService(namespace, client, service)
		}()

		ginkgo.By("Creating StorageClass for Statefulset")
		scSpec := getVSphereStorageClassSpec(defaultNginxStorageClassName, nil, allowedTopologyForRack3,
			"", bindingMode, false)
		sc, err := client.StorageV1().StorageClasses().Create(ctx, scSpec, metav1.CreateOptions{})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		defer func() {
			err = client.StorageV1().StorageClasses().Delete(ctx, sc.Name, *metav1.NewDeleteOptions(0))
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}()

		// Creating statefulset with 3 replicas
		ginkgo.By("Creating Statefulset with 3 replica")
		sts1 := GetStatefulSetFromManifest(namespace)
		sts1.Spec.PodManagementPolicy = appsv1.ParallelPodManagement
		ginkgo.By("Creating statefulset")
		CreateStatefulSet(namespace, sts1, client)
		sts1Replicas := *(sts1.Spec.Replicas)
		defer func() {
			framework.Logf("Deleting all statefulset in namespace: %v", namespace)
			DeleteAllStsInGivenNamespace(client, namespace)
		}()

		// Waiting for pods status to be Ready.
		fss.WaitForStatusReadyReplicas(client, sts1, sts1Replicas)
		gomega.Expect(fss.CheckMount(client, sts1, mountPath)).NotTo(gomega.HaveOccurred())
		ssPodsBeforeScaleDown := GetListOfPodsInSts(client, sts1)
		gomega.Expect(ssPodsBeforeScaleDown.Items).NotTo(gomega.BeEmpty(),
			fmt.Sprintf("Unable to get list of Pods from the Statefulset: %v", sts1.Name))
		gomega.Expect(len(ssPodsBeforeScaleDown.Items) == int(sts1Replicas)).To(gomega.BeTrue(),
			"Number of Pods in the statefulset should match with number of replicas")

		//verifying volume provisioning
		ginkgo.By("Verify volume is provisioned on the preferred datatsore")
		err = verifyVolumeProvisioningForStatefulSet(ctx, client, sts1, namespace, preferredDatastorePaths,
			nonShareddatastoreListMapRack3, false, true)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Verify PV node affinity and that the PODS are running on " +
			"appropriate node as specified in the allowed topologies of SC")
		verifyPVnodeAffinityAndPODnodedetailsForStatefulsetsLevel5(ctx, client, sts1,
			namespace, allowedTopologies, false)

		ginkgo.By("Remove preferred datatsore tag chosen for volume provisioning")
		err = detachTagCreatedOnPreferredDatastore(masterIp, preferredDatastorePaths[0], allowedTopologyRacks[2])
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Tag new preferred datatsore for volume provisioning in rack-3")
		preferredDatastorePaths, err = tagPreferredDatastore(masterIp, allowedTopologyRacks[2],
			preferredDatastoreChosen, nonShareddatastoreListMapRack3, preferredDatastorePaths)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
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

		// Creating statefulset2 with 3 replicas
		ginkgo.By("Creating statefulset2 with 3 replica")
		sts2 := GetStatefulSetFromManifest(namespace)
		sts2.Name = "sts"
		sts2Replicas := *(sts2.Spec.Replicas)
		stsReplicas := sts1Replicas + sts2Replicas
		ginkgo.By("Creating statefulset1")
		createMultipleStatefulSetInSameNS(namespace, sts2, client, stsReplicas)

		// Waiting for pods status to be Ready.
		fss.WaitForStatusReadyReplicas(client, sts2, sts2Replicas)
		gomega.Expect(fss.CheckMount(client, sts2, mountPath)).NotTo(gomega.HaveOccurred())
		ssPodsBeforeScaleDown = GetListOfPodsInSts(client, sts2)
		gomega.Expect(ssPodsBeforeScaleDown.Items).NotTo(gomega.BeEmpty(),
			fmt.Sprintf("Unable to get list of Pods from the Statefulset: %v", sts2.Name))
		gomega.Expect(len(ssPodsBeforeScaleDown.Items) == int(sts2Replicas)).To(gomega.BeTrue(),
			"Number of Pods in the statefulset should match with number of replicas")

		//verifying volume provisioning
		ginkgo.By("Verify volume is provisioned on the specified datatsore")
		err = verifyVolumeProvisioningForStatefulSet(ctx, client, sts2, namespace, preferredDatastorePaths,
			nonShareddatastoreListMapRack3, false, true)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		// Verify node and pv topology affinity should contains specified zone and region details of SC
		ginkgo.By("Verify node and pv topology affinity should contains specified zone and region details of SC")
		verifyPVnodeAffinityAndPODnodedetailsForStatefulsetsLevel5(ctx, client, sts2,
			namespace, allowedTopologies, true)

		// perform statefulset scaleup
		sts1Replicas = 10
		ginkgo.By("Scale up statefulset replica count from 3 to 10")
		preferredDatastorePaths = append(preferredDatastorePaths, preferredDatastore...)
		scaleUpStatefulSetPod(ctx, client, sts1, namespace, sts1Replicas, true)

		//verifying volume is provisioned on the preferred datastore
		ginkgo.By("Verify volume is provisioned on the specified datatsore")
		err = verifyVolumeProvisioningForStatefulSet(ctx, client, sts1, namespace, preferredDatastorePaths,
			nonShareddatastoreListMapRack3, false, true)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Verify PV node affinity and that the PODS are running on " +
			"appropriate node as specified in the allowed topologies of SC")
		verifyPVnodeAffinityAndPODnodedetailsForStatefulsetsLevel5(ctx, client, sts2,
			namespace, allowedTopologies, true)
	})

	/*
		Testcase-10:
		Change  datastore preference  multiple times and  perform Scaleup/ScaleDown operation on StatefulSet

		Steps
		1. Assign Tag rack-1, Category "cns.vmware.topology-preferred-datastores" to datastore shared
		across all racks (ex- NFS-12)
		2. Create SC with WFC Binding mode and allowed topologies set to rack-1 in the SC
		3. Create StatefulSet with parallel pod management policy and replica 3 with the above SC
		4. Wait for all the StatefulSet to come up
		5. Wait for PV , PVC to reach bound and POD to reach running state
		6. Describe PV and verify node affinity details should contain specified allowed topologies details.
		7. Verify volume should be provisioned on the selected preferred datastore of rack-1
		8. Make sure POD is running on the same node as mentioned in the node affinity details
		9. Remove tag from preferred datastore and assign tag rack-1  and category
		"cns.vmware.topology-preferred-datastores" to new datastore specific to rack-1  (ex - vSAN-1).
		10. Wait for the refresh interval time which is set at which preferred datatsore
			is picked up for volume provisioning.
		11. Perform Scaleup operation, Increase the replica count of StatefulSet from 3 to 13.
		12. Wait for all the StatefulSet to come up
		13. Wait for PV , PVC to reach Bound and POD to reach running state
		14. Describe PV and verify node affinity details should contain specified allowed topolgies details.
		15. Verify volume should be provisioned on the newly selected preferred datastore.
		16. Perform Scaledown operation. Decrease the replica count from 13 to 6.
		17. Remove tag from preferred datastore and assign tag rack-1 and  category
		"cns.vmware.topology-preferred-datastores" to new datastore which is shared specific to rack-1. (ex- NFS-1)
		18. Wait for the refresh interval time which is set at which preferred datatsore
			is picked up for volume provisioning.
		19. Perform Scaleup operation again and increase the replica count from 6 to 20.
		20. Wait for all the StatefulSet to come up
		21. Wait for PV , PVC to reach bound and POD to reach running state
		22. Describe PV and verify node affinity details should contain specified allowed topology details.
		23. Verify volume should be provisioned on the newly selected preferred datastore of rack-1
		24. Make sure POD is running on the same node as mentioned in the node affinity details
		25. Perform cleanup. Delete StatefulSet, PVC, PV and SC.
		26. Remove datastore preference tags as part of cleanup.
	*/
	ginkgo.It("Change datastore preference multiple times and  perform scaleup and scaleDown "+
		"operation on StatefulSet", func() {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		preferredDatastoreChosen = 1
		preferredDatastorePaths = nil

		// choose preferred datastore
		ginkgo.By("Tag preferred datastore for volume provisioning in rack-1(cluster-1))")
		preferredDatastorePaths, err := tagPreferredDatastore(masterIp, allowedTopologyRacks[0],
			preferredDatastoreChosen, shareddatastoreListMap, nil)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		framework.Logf("Waiting for %v for preferred datastore to get refreshed in the environment",
			preferredDatastoreTimeOutInterval)
		time.Sleep(preferredDatastoreTimeOutInterval)

		ginkgo.By("Creating service")
		service := CreateService(namespace, client)
		defer func() {
			deleteService(namespace, client, service)
		}()

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
		ginkgo.By("Creating Statefulset with 3 replica")
		sts1 := GetStatefulSetFromManifest(namespace)
		sts1.Spec.PodManagementPolicy = appsv1.ParallelPodManagement
		ginkgo.By("Creating statefulset")
		CreateStatefulSet(namespace, sts1, client)
		sts1Replicas := *(sts1.Spec.Replicas)
		defer func() {
			framework.Logf("Deleting all statefulset in namespace: %v", namespace)
			fss.DeleteAllStatefulSets(client, namespace)
		}()

		// Waiting for pods status to be Ready.
		fss.WaitForStatusReadyReplicas(client, sts1, sts1Replicas)
		gomega.Expect(fss.CheckMount(client, sts1, mountPath)).NotTo(gomega.HaveOccurred())
		ssPodsBeforeScaleDown := GetListOfPodsInSts(client, sts1)
		gomega.Expect(ssPodsBeforeScaleDown.Items).NotTo(gomega.BeEmpty(),
			fmt.Sprintf("Unable to get list of Pods from the Statefulset: %v", sts1.Name))
		gomega.Expect(len(ssPodsBeforeScaleDown.Items) == int(sts1Replicas)).To(gomega.BeTrue(),
			"Number of Pods in the statefulset should match with number of replicas")

		//verify volume provisioning
		ginkgo.By("Verify volume is provisioned on the preferred datatsore")
		err = verifyVolumeProvisioningForStatefulSet(ctx, client, sts1, namespace, preferredDatastorePaths,
			shareddatastoreListMap, false, false)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Verify PV node affinity and that the PODS are running on " +
			"appropriate node as specified in the allowed topologies of SC")
		verifyPVnodeAffinityAndPODnodedetailsForStatefulsetsLevel5(ctx, client, sts1,
			namespace, allowedTopologies, false)

		ginkgo.By("Remove preferred datastore tag chosen for volume provisioning")
		err = detachTagCreatedOnPreferredDatastore(masterIp, preferredDatastorePaths[0], allowedTopologyRacks[0])
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Tag new preferred datatsore for volume provisioning in rack-1(cluster-1)")
		preferredDatastore, err := tagPreferredDatastore(masterIp, allowedTopologyRacks[0],
			preferredDatastoreChosen, nonShareddatastoreListMapRack1, nil)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		preferredDatastorePaths = append(preferredDatastorePaths, preferredDatastore...)

		framework.Logf("Waiting for %v for preferred datastore to get refreshed in the environment",
			preferredDatastoreTimeOutInterval)
		time.Sleep(preferredDatastoreTimeOutInterval)

		// perform statefulset scaleup
		sts1Replicas = 13
		ginkgo.By("Scale up statefulset replica count from 3 to 13")
		scaleUpStatefulSetPod(ctx, client, sts1, namespace, sts1Replicas, false)

		//verifying volume is provisioned on the preferred datastore
		ginkgo.By("Verify volume is provisioned on the specified datatsore")
		err = verifyVolumeProvisioningForStatefulSet(ctx, client, sts1, namespace, preferredDatastorePaths,
			rack1DatastoreListMap, false, true)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Verify PV node affinity and that the PODS are running on " +
			"appropriate node as specified in the allowed topologies of SC")
		verifyPVnodeAffinityAndPODnodedetailsForStatefulsetsLevel5(ctx, client, sts1,
			namespace, allowedTopologies, true)

		// perform statefulset scaledown
		sts1Replicas = 6
		ginkgo.By("Scale down statefulset replica count from 13 to 6")
		scaleDownStatefulSetPod(ctx, client, sts1, namespace, sts1Replicas, false)

		ginkgo.By("Remove preferred datastore tag chosen for volume provisioning")
		err = detachTagCreatedOnPreferredDatastore(masterIp, preferredDatastorePaths[1], allowedTopologyRacks[0])
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Tag new datastore chosen for volume provisioning")
		preferredDatastore, err = tagPreferredDatastore(masterIp, allowedTopologyRacks[0],
			preferredDatastoreChosen, nonShareddatastoreListMapRack1, preferredDatastorePaths)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		preferredDatastorePaths = append(preferredDatastorePaths, preferredDatastore...)
		defer func() {
			ginkgo.By("Remove preferred datastore tags chosen for volume provisioning")
			for i := 0; i < len(preferredDatastorePaths); i++ {
				err = detachTagCreatedOnPreferredDatastore(masterIp, preferredDatastorePaths[i], allowedTopologyRacks[0])
			}
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}()

		framework.Logf("Waiting for %v for preferred datastore to get refreshed in the environment",
			preferredDatastoreTimeOutInterval)
		time.Sleep(preferredDatastoreTimeOutInterval)

		// perform statefulset scaleup
		sts1Replicas = 20
		ginkgo.By("Scale up statefulset replica count from 6 to 20")
		scaleUpStatefulSetPod(ctx, client, sts1, namespace, sts1Replicas, false)

		//verifying volume is provisioned on the preferred datastore
		ginkgo.By("Verify volume is provisioned on the specified datatsore")
		err = verifyVolumeProvisioningForStatefulSet(ctx, client, sts1, namespace, preferredDatastorePaths,
			rack1DatastoreListMap, false, false)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Verify PV node affinity and that the PODS are running on " +
			"appropriate node as specified in the allowed topologies of SC")
		verifyPVnodeAffinityAndPODnodedetailsForStatefulsetsLevel5(ctx, client, sts1,
			namespace, allowedTopologies, false)
	})

	/*
		Testcase-11:
		Tag single datastore but no allowed Topologies are set in the SC

		Steps
		1. Assign Tag rack-1, Category "cns.vmware.topology-preferred-datastores" to datastore
		specific to rack-1 (ex - NFS-1)
		2. Create SC with WFC binding mode with no allowed topologies set in the SC
		3. Create StatefulSet with replica 3 with the above SC
		4. Wait for all the StatefulSet to come up.
		5. Wait for PV , PVC to reach Bound and POD to reach running state
		6. Pods should get created on any of the racks rack-1/rack-2/rack-3 since no allowed topology
		is specified.
		7. Describe PV and verify node affinity details.
		8. If node affinity has rack-1 allowed topology, then volume should be provisioned on the preferred
		datastore of rack-1.
		9. If node affinity has either rack-2/rack-3 allowed topology, then volume should be provisioned
		on any of the datastores since no preference is given.
		10. Make sure POD is running on the same node as mentioned in the node affinity details.
		11. Remove tag from preferred datastore and assign tag rack-2 and
		category "cns.vmware.topology-preferred-datastores" to new datastore which is shared
		specific to rack-2 (ex - NFS-2)
		12. Wait for the refresh interval time which is set at which preferred datatsore
		is picked up for volume provisioning.
		13. Perform Scaleup operation, Increase the replica count of StatefulSet from 3 to 7.
		14. Wait for all the StatefulSet to come up
		15. Describe PV and verify node affinity details.
		16. If node affinity has rack-1/rack-3 allowed topology, then volume should be provisioned
		on any of the datastores since no preference is given.
		17. If node affinity has rack-2 allowed topology, then volume should be provisioned on the
		tagged preferred datastore of rack-2.
		18. Make sure POD is running on the same node as mentioned in the node affinity details
		18. Remove tag from preferred datastore and assign tag rack-3 and category
		"cns.vmware.topology-preferred-datastores" to a new datastore which is shared specific
		to rack-3 (ex - vSAN-3)
		19. Wait for the refresh interval time which is set at which preferred datatsore
		is pciked up for volume provisioning.
		20. Perform Scaleup operation, Increase the replica count of StatefulSet from 7 to 13.
		21. Wait for all the StatefulSet to come up
		22. Wait for PV, PVC to reach bound and POD to reach running state
		23. Describe PV and verify node affinity details.
		24. If node affinity has rack-1/rack-2 allowed topology, then volume should be provisioned
		on any of the datastores since no preference is given.
		25. If node affinity has rack-3 allowed topology, then volume should be provisioned on the
		tagged preferred datastore of rack-3
		26. Perform cleanup. Delete StatefulSet, PVC, PV and SC.
		27. Remove datastore preference tags as part of cleanup.
	*/
	ginkgo.It("Tag single datastore from each rack when no allowed topologies are set in the SC", func() {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		preferredDatastoreChosen = 1
		preferredDatastorePaths = nil

		// choose preferred datastore
		ginkgo.By("Tag preferred datatsore for volume provisioning in rack-1(cluster-1)")
		preferredDatastorePaths, err := tagPreferredDatastore(masterIp, allowedTopologyRacks[0],
			preferredDatastoreChosen, nonShareddatastoreListMapRack1, nil)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		framework.Logf("Waiting for %v for preferred datastore to get refreshed in the environment",
			preferredDatastoreTimeOutInterval)
		time.Sleep(preferredDatastoreTimeOutInterval)

		ginkgo.By("Creating service")
		service := CreateService(namespace, client)
		defer func() {
			deleteService(namespace, client, service)
		}()

		ginkgo.By("Creating StorageClass for Statefulset")
		scSpec := getVSphereStorageClassSpec(defaultNginxStorageClassName, nil, nil,
			"", bindingMode, false)
		sc, err := client.StorageV1().StorageClasses().Create(ctx, scSpec, metav1.CreateOptions{})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		defer func() {
			err = client.StorageV1().StorageClasses().Delete(ctx, sc.Name, *metav1.NewDeleteOptions(0))
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}()

		// Creating statefulset with 3 replicas
		ginkgo.By("Creating Statefulset with 3 replica")
		sts1 := GetStatefulSetFromManifest(namespace)
		sts1.Spec.PodManagementPolicy = appsv1.ParallelPodManagement
		ginkgo.By("Creating statefulset")
		CreateStatefulSet(namespace, sts1, client)
		sts1Replicas := *(sts1.Spec.Replicas)
		defer func() {
			framework.Logf("Deleting all statefulset in namespace: %v", namespace)
			fss.DeleteAllStatefulSets(client, namespace)
		}()

		// Waiting for pods status to be Ready.
		fss.WaitForStatusReadyReplicas(client, sts1, sts1Replicas)
		gomega.Expect(fss.CheckMount(client, sts1, mountPath)).NotTo(gomega.HaveOccurred())
		ssPodsBeforeScaleDown := GetListOfPodsInSts(client, sts1)
		gomega.Expect(ssPodsBeforeScaleDown.Items).NotTo(gomega.BeEmpty(),
			fmt.Sprintf("Unable to get list of Pods from the Statefulset: %v", sts1.Name))
		gomega.Expect(len(ssPodsBeforeScaleDown.Items) == int(sts1Replicas)).To(gomega.BeTrue(),
			"Number of Pods in the statefulset should match with number of replicas")

		//verifying volume provisioning
		ginkgo.By("Verify volume is provisioned on the preferred datatsore")
		err = verifyVolumeProvisioningForStatefulSet(ctx, client, sts1, namespace, preferredDatastorePaths,
			nonShareddatastoreListMapRack1, true, false)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		// Verify affinity details
		ginkgo.By("Verify node and pv topology affinity details")
		verifyPVnodeAffinityAndPODnodedetailsForStatefulsetsLevel5(ctx, client, sts1,
			namespace, allowedTopologies, false)

		ginkgo.By("Remove preferred datatsore tag which was chosen for volume provisioning in rack-1")
		err = detachTagCreatedOnPreferredDatastore(masterIp, preferredDatastorePaths[0], allowedTopologyRacks[0])
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Tag new preferred datatsore for volume provisioning in rack-2(cluster-2)")
		preferredDatastorePaths, err = tagPreferredDatastore(masterIp, allowedTopologyRacks[1],
			preferredDatastoreChosen, nonShareddatastoreListMapRack2, nil)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		framework.Logf("Waiting for %v for preferred datastore to get refreshed in the environment",
			preferredDatastoreTimeOutInterval)
		time.Sleep(preferredDatastoreTimeOutInterval)

		// perform statefulset scaleup
		sts1Replicas = 7
		ginkgo.By("Scale up statefulset replica count from 3 to 7")
		scaleUpStatefulSetPod(ctx, client, sts1, namespace, sts1Replicas, true)

		//verify volume provisioning
		ginkgo.By("Verify volume is provisioned on the preferred datatsore")
		err = verifyVolumeProvisioningForStatefulSet(ctx, client, sts1, namespace, preferredDatastorePaths,
			nonShareddatastoreListMapRack2, true, false)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		// Verify node and pv topology affinity should contains specified zone and region details of SC
		ginkgo.By("Verify node and pv topology affinity should contains specified zone and region details of SC")
		verifyPVnodeAffinityAndPODnodedetailsForStatefulsetsLevel5(ctx, client, sts1,
			namespace, allowedTopologies, false)

		ginkgo.By("Remove the datastore preference chosen for volume provisioning")
		err = detachTagCreatedOnPreferredDatastore(masterIp, preferredDatastorePaths[0], allowedTopologyRacks[1])
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Choosing new preferred datatsore for volume provisioning in rack-3(cluster-3)")
		preferredDatastorePaths, err = tagPreferredDatastore(masterIp, allowedTopologyRacks[2],
			preferredDatastoreChosen, nonShareddatastoreListMapRack3, nil)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		defer func() {
			ginkgo.By("Remove the datastore preference chosen for volume provisioning")
			err = detachTagCreatedOnPreferredDatastore(masterIp, preferredDatastorePaths[0], allowedTopologyRacks[2])
			gomega.Expect(err).NotTo(gomega.HaveOccurred())

		}()

		framework.Logf("Waiting for %v for preferred datastore to get refreshed in the environment",
			preferredDatastoreTimeOutInterval)
		time.Sleep(preferredDatastoreTimeOutInterval)

		// perform statefulset scaleup
		sts1Replicas = 13
		ginkgo.By("Scale up statefulset replica count from 7 to 13")
		scaleUpStatefulSetPod(ctx, client, sts1, namespace, sts1Replicas, false)

		//verifying volume is provisioned on the preferred datastore
		ginkgo.By("Verify volume is provisioned on the preferred datatsore")
		err = verifyVolumeProvisioningForStatefulSet(ctx, client, sts1, namespace, preferredDatastorePaths,
			nonShareddatastoreListMapRack3, true, false)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Verify PV node affinity and that the PODS are running on " +
			"appropriate node as specified in the allowed topologies of SC")
		verifyPVnodeAffinityAndPODnodedetailsForStatefulsetsLevel5(ctx, client, sts1,
			namespace, allowedTopologies, false)
	})

	/*
		Testcase-12:
		Tag multiple datastore and provide specific datastore Url in SC

		Steps
		1. Assign Tag rack-2, Category "cns.vmware.topology-preferred-datastores" to datastore
		specific to rack-2(ex- NFS-2)
		2. Assign Tag rack-2, Category "cns.vmware.topology-preferred-datastores" to datastore
		specific to rack-2 (ex- vSAN-2)
		3. Assign Tag rack-2, Category "cns.vmware.topology-preferred-datastores" to datastore shared
		across all racks (ex- NFS-12)
		4. Create SC with WFC binding mode with allowed topologies set to rack-2 and
		provide any specific preferred datastore url in the SC (ex - vSAN datastore url)
		5. Create StatefulSet with replica 3 with the above SC
		6. Wait for all the StatefulSet to come up
		7. Wait for PV , PVC to reach bound and POD to reach running state
		8. Describe PV and verify node affinity details should contain specified allowed details.
		9. Verify volume should be provisioned on the preferred datastore mentioned in the storage class.
		10. Make sure POD is running on the same node as mentioned in node affinity details
		11. Perform cleanup. Delete StatefulSet, PVC, PV and SC.
		12. Remove datastore preference tags as part of cleanup.
	*/
	ginkgo.It("Tag multiple datastore and provide specific datastore url in SC", func() {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		preferredDatastoreChosen = 2
		preferredDatastorePaths = nil
		scParameters := make(map[string]string)
		DataStoreUrlSpecificToCluster := GetAndExpectStringEnvVar(datastoreUrlSpecificToCluster)
		scParameters["datastoreurl"] = DataStoreUrlSpecificToCluster

		// choose preferred datastore
		ginkgo.By("Tag preferred datatsore for volume provisioning in rack-2(cluster-2)")
		preferredDatastorePaths, err = tagPreferredDatastore(masterIp, allowedTopologyRacks[1],
			preferredDatastoreChosen, nonShareddatastoreListMapRack2, nil)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		preferredDatastoreChosen = 1
		preferredDatastore, err := tagPreferredDatastore(masterIp, allowedTopologyRacks[1],
			preferredDatastoreChosen, shareddatastoreListMap, nil)
		preferredDatastorePaths = append(preferredDatastorePaths, preferredDatastore...)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
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

		ginkgo.By("Creating service")
		service := CreateService(namespace, client)
		defer func() {
			deleteService(namespace, client, service)
		}()

		ginkgo.By("Creating StorageClass for Statefulset")
		scSpec := getVSphereStorageClassSpec(defaultNginxStorageClassName, scParameters, allowedTopologyForRack2,
			"", bindingMode, false)
		sc, err := client.StorageV1().StorageClasses().Create(ctx, scSpec, metav1.CreateOptions{})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		defer func() {
			err = client.StorageV1().StorageClasses().Delete(ctx, sc.Name, *metav1.NewDeleteOptions(0))
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}()

		// Creating statefulset with 3 replicas
		ginkgo.By("Creating Statefulset with 3 replica")
		sts1 := GetStatefulSetFromManifest(namespace)
		sts1.Spec.PodManagementPolicy = appsv1.ParallelPodManagement
		ginkgo.By("Creating statefulset")
		CreateStatefulSet(namespace, sts1, client)
		sts1Replicas := *(sts1.Spec.Replicas)
		defer func() {
			framework.Logf("Deleting all statefulset in namespace: %v", namespace)
			fss.DeleteAllStatefulSets(client, namespace)
		}()

		// Waiting for pods status to be Ready.
		fss.WaitForStatusReadyReplicas(client, sts1, sts1Replicas)
		gomega.Expect(fss.CheckMount(client, sts1, mountPath)).NotTo(gomega.HaveOccurred())
		ssPodsBeforeScaleDown := GetListOfPodsInSts(client, sts1)
		gomega.Expect(ssPodsBeforeScaleDown.Items).NotTo(gomega.BeEmpty(),
			fmt.Sprintf("Unable to get list of Pods from the Statefulset: %v", sts1.Name))
		gomega.Expect(len(ssPodsBeforeScaleDown.Items) == int(sts1Replicas)).To(gomega.BeTrue(),
			"Number of Pods in the statefulset should match with number of replicas")

		//verifying volume provisioning
		ginkgo.By("Verify volume is provisioned on the specified datatsore")
		for _, sspod := range ssPodsBeforeScaleDown.Items {
			_, err := client.CoreV1().Pods(namespace).Get(ctx, sspod.Name, metav1.GetOptions{})
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			for _, volumespec := range sspod.Spec.Volumes {
				if volumespec.PersistentVolumeClaim != nil {
					pv := getPvFromClaim(client, sts1.Namespace, volumespec.PersistentVolumeClaim.ClaimName)
					ginkgo.By(fmt.Sprintf("Invoking QueryCNSVolumeWithResult with VolumeID: %s",
						pv.Spec.CSI.VolumeHandle))
					isDatastoreMatchFound := e2eVSphere.verifyPreferredDatastoreMatch(pv.Spec.CSI.VolumeHandle,
						DataStoreUrlSpecificToCluster)
					if isDatastoreMatchFound {
						framework.Logf("volume %s is created on the chosen preferred datastore %v",
							pv.Spec.CSI.VolumeHandle, DataStoreUrlSpecificToCluster)
					}
					gomega.Expect(isDatastoreMatchFound).To(gomega.BeTrue(), "Volume is provisioned on wrong datastore")
				}
			}
		}
		// Verify affinity details
		ginkgo.By("Verify node and pv topology affinity details")
		verifyPVnodeAffinityAndPODnodedetailsForStatefulsetsLevel5(ctx, client, sts1,
			namespace, allowedTopologies, false)
	})

	/*
		Testcase-13:
		Tag multiple preferred datastore and create PVC which is greater than the size of any one
		preferred datastore capacity

		Steps
		1. Assign Tag rack-2, Category "cns.vmware.topology-preferred-datastores" to datastore specific
		to rack-2
		2. Assign Tag rack-2, Category "cns.vmware.topology-preferred-datastores" to datastore shared
		across all racks
		3. Create SC with Immediate binding mode with allowed topologies set to rack-2 and
		provide storage policy details in the SC (NFS-2 datastore is given in the storage policy)
		4. Create PVC which is greater than the available capacity of any preferred datastore (ex- NFS-2)
		5. PVC should get stuck in Pending state and should throw an appropriate error message.
		6. Perform cleanup. Delete PVC and SC.
		7. Remove datastore preference tags as part of cleanup.
	*/
	// ginkgo.It("Tag multiple preferred datastore and create PVC which is greater than the "+
	// 	"size of preferred datastore capacity", func() {
	// 	ctx, cancel := context.WithCancel(context.Background())
	// 	defer cancel()
	// 	preferredDatastoreChosen = 1
	// 	preferredDatastorePaths = nil
	// 	scParameters := make(map[string]string)
	// 	nFSDefaultStoragePolicyName := GetAndExpectStringEnvVar(nFSDefaultStoragePolicyName)
	// 	scParameters[scParamStoragePolicyName] = nFSDefaultStoragePolicyName
	// 	dsSize := "50Gi"

	// 	// choose preferred datastore
	// 	ginkgo.By("Tag preferred datatsore for volume provisioning in rack-2(cluster-2)")
	// 	preferredDatastorePaths, err := tagPreferredDatastore(masterIp, allowedTopologyRacks[1],
	// 		preferredDatastoreChosen, nonShareddatastoreListMapRack2, nil)
	// 	gomega.Expect(err).NotTo(gomega.HaveOccurred())
	// 	preferredDatastore, err := tagPreferredDatastore(masterIp, allowedTopologyRacks[1],
	// 		preferredDatastoreChosen, shareddatastoreListMap, nil)
	// 	gomega.Expect(err).NotTo(gomega.HaveOccurred())
	// 	preferredDatastorePaths = append(preferredDatastorePaths, preferredDatastore...)
	// 	defer func() {
	// 		ginkgo.By("Remove preferred datastore tag")
	// 		for i := 0; i < len(preferredDatastorePaths); i++ {
	// 			err = detachTagCreatedOnPreferredDatastore(masterIp, preferredDatastorePaths[i],
	// 				allowedTopologyRacks[1])
	// 		}
	// 		gomega.Expect(err).NotTo(gomega.HaveOccurred())
	// 	}()

	// 	framework.Logf("Waiting for %v for preferred datastore to get refreshed in the environment",
	// 		preferredDatastoreTimeOutInterval)
	// 	time.Sleep(preferredDatastoreTimeOutInterval)

	// 	ginkgo.By("Create storage class and PVC")
	// 	storageclass, pvclaim, err := createPVCAndStorageClass(client,
	// 		namespace, nil, scParameters, dsSize, allowedTopologyForRack2, "", false, "")
	// 	gomega.Expect(err).NotTo(gomega.HaveOccurred())
	// 	defer func() {
	// 		err = client.StorageV1().StorageClasses().Delete(ctx, storageclass.Name, *metav1.NewDeleteOptions(0))
	// 		gomega.Expect(err).NotTo(gomega.HaveOccurred())
	// 		err = fpv.DeletePersistentVolumeClaim(client, pvclaim.Name, namespace)
	// 		gomega.Expect(err).NotTo(gomega.HaveOccurred())
	// 	}()

	// 	ginkgo.By("Expect claim to fail provisioning volume within the topology")
	// 	framework.ExpectError(fpv.WaitForPersistentVolumeClaimPhase(v1.ClaimBound,
	// 		client, pvclaim.Namespace, pvclaim.Name, pollTimeoutShort, framework.PollShortTimeout))
	// 	// Get the event list and verify if it contains expected error message
	// 	eventList, _ := client.CoreV1().Events(pvclaim.Namespace).List(ctx, metav1.ListOptions{})
	// 	gomega.Expect(eventList.Items).NotTo(gomega.BeEmpty())
	// 	expectedErrMsg := "failed to get shared datastores for topology requirement"
	// 	err = waitForEvent(ctx, client, namespace, expectedErrMsg, pvclaim.Name)
	// 	gomega.Expect(err).NotTo(gomega.HaveOccurred(), fmt.Sprintf("Expected error : %q", expectedErrMsg))
	// })

	/*
		Testcase-14:
		Tag datastore and provide storage policy  in SC

		Steps
		1. Assign Tag rack-2, Category "cns.vmware.topology-preferred-datastores" to datastore
		shared across all racks (ex - sharedvmfs-0)
		2. Create SC with allowed topologies set to rack-2 and provide Storage Policy which is
		set to datastore which is shared only specific to rack-2 (ex - NFS-2, the storage policy
			mentioned in the datastore is not set as tagged preferred datastore)
		3. Create PVC.
		4. Verify PVC should go to Bound state.
		5. Describe pv node affinity rules.
		6. Verify volume should be provisioned on the selected preferred datatsore.
		7. Create Pod from PVC created above.
		8. Verify Pod should go to running state.
		9. POD should come up on the node which is present in the same details as mentioned in storage class
		10. Perform cleanup. Delete Pod, PVC and PV, SC.
		11. Remove datastore preference tags as part of cleanup.
	*/
	ginkgo.It("Tag preferred datastore and provide storage policy in SC", func() {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		preferredDatastoreChosen = 1
		preferredDatastorePaths = nil
		scParameters := make(map[string]string)
		nFSDefaultStoragePolicyName := GetAndExpectStringEnvVar(nFSDefaultStoragePolicyName)
		scParameters[scParamStoragePolicyName] = nFSDefaultStoragePolicyName

		// choose preferred datastore
		ginkgo.By("Tag preferred datatsore for volume provisioning in rack-2(cluster-2)")
		preferredDatastorePaths, err := tagPreferredDatastore(masterIp, allowedTopologyRacks[1],
			preferredDatastoreChosen, shareddatastoreListMap, nil)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		defer func() {
			ginkgo.By("Remove preferred datastore tag")
			err = detachTagCreatedOnPreferredDatastore(masterIp, preferredDatastorePaths[0],
				allowedTopologyRacks[1])
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}()

		framework.Logf("Waiting for %v for preferred datastore to get refreshed in the environment",
			preferredDatastoreTimeOutInterval)
		time.Sleep(preferredDatastoreTimeOutInterval)

		// Creating Storage Class and PVC
		ginkgo.By("Creating Storage class and standalone PVC")
		storageclass, pvclaim, err := createPVCAndStorageClass(client, namespace, nil, scParameters, "",
			allowedTopologyForRack2, "", false, "")
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		defer func() {
			err = client.StorageV1().StorageClasses().Delete(ctx, storageclass.Name, *metav1.NewDeleteOptions(0))
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

		nFSDataStoreUrl := GetAndExpectStringEnvVar(nFSStoragePolicyDatastoreUrl)
		// verifying volume provisioning
		ginkgo.By("Verify volume is provisioned on the preferred datatsore")
		for _, volumespec := range pod.Spec.Volumes {
			if volumespec.PersistentVolumeClaim != nil {
				pv := getPvFromClaim(client, pod.Namespace, volumespec.PersistentVolumeClaim.ClaimName)
				ginkgo.By(fmt.Sprintf("Invoking QueryCNSVolumeWithResult with VolumeID: %s",
					pv.Spec.CSI.VolumeHandle))
				isDatastoreMatchFound := e2eVSphere.verifyPreferredDatastoreMatch(pv.Spec.CSI.VolumeHandle, nFSDataStoreUrl)
				if isDatastoreMatchFound {
					framework.Logf("volume %s is created on the chosen preferred datastore %v",
						pv.Spec.CSI.VolumeHandle, nFSDataStoreUrl)
				}
				gomega.Expect(isDatastoreMatchFound).To(gomega.BeTrue(), "Volume is provisioned on wrong datastore")
			}
		}

		ginkgo.By("Verify PV node affinity and that the PODS are running on appropriate " +
			"node as specified in the allowed topologies of SC")
		verifyPVnodeAffinityAndPODnodedetailsFoStandalonePodLevel5(ctx, client, pod, namespace,
			allowedTopologies)
	})

	/*
		Testcase-15:
		When all racks are specfied

		Steps
		1. Assign Tag rack-1, Category "cns.vmware.topology-preferred-datastores" to  datastore
		specific to rack-1. (ex - NFS-1)
		2. Assign Tag rack-2, Category "cns.vmware.topology-preferred-datastores" to  datastore
		specific to rack-2. (ex - NFS-2)
		3. Assign Tag rack-3, Category "cns.vmware.topology-preferred-datastores" to  datastore
		shared across all racks (ex - sharedVMFS-12)
		4. Create SC with WFC binding mode with allowed topologies set to all racks rack-1/rack-2/rack-3
		in the SC
		5. Create StatefulSet with replica 3 with the above SC
		6. Wait for all the StatefulSet to come up
		7. Wait for PV , PVC to reach Bound and POD to reach running state
		8. Verify volume provisioning. It should provision volume on the preferred datastores.
		9. Make sure POD is running on the same node as mentioned in the node affinity details.
		10. Scaleup StatefulSet from replica 3 to replica 10.
		11. Wait for all the StatefulSet to come up
		12. Wait for PV , PVC to reach Bound and POD to reach running state
		13. Verify volume provisioning. It should provision volume on the preferred datastores.
		14. Perform cleanup. Delete StatefulSet, PVC and PV, SC.
		15. Remove datastore preference tags as part of cleanup.
	*/
	ginkgo.It("When all racks are specfied in allowed topology with different datastore tag assign to each rack", func() {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		preferredDatastoreChosen = 1
		preferredDatastorePaths = nil

		// choose preferred datastore
		ginkgo.By("Tag preferred datatsores in different racks")
		preferredDatastorePaths, err = tagPreferredDatastore(masterIp, allowedTopologyRacks[0],
			preferredDatastoreChosen, nonShareddatastoreListMapRack1, nil)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		defer func() {
			err = detachTagCreatedOnPreferredDatastore(masterIp, preferredDatastorePaths[0],
				allowedTopologyRacks[0])
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}()
		preferredDatastore1, err := tagPreferredDatastore(masterIp, allowedTopologyRacks[1],
			preferredDatastoreChosen, nonShareddatastoreListMapRack2, nil)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		defer func() {
			err = detachTagCreatedOnPreferredDatastore(masterIp, preferredDatastore1[0],
				allowedTopologyRacks[1])
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}()
		preferredDatastorePaths = append(preferredDatastorePaths, preferredDatastore1...)
		preferredDatastore2, err := tagPreferredDatastore(masterIp, allowedTopologyRacks[2],
			preferredDatastoreChosen, shareddatastoreListMap, nil)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		defer func() {
			err = detachTagCreatedOnPreferredDatastore(masterIp, preferredDatastore2[0],
				allowedTopologyRacks[2])
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}()
		preferredDatastorePaths = append(preferredDatastorePaths, preferredDatastore2...)

		framework.Logf("Waiting for %v for preferred datastore to get refreshed in the environment",
			preferredDatastoreTimeOutInterval)
		time.Sleep(preferredDatastoreTimeOutInterval)

		ginkgo.By("Creating service")
		service := CreateService(namespace, client)
		defer func() {
			deleteService(namespace, client, service)
		}()

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
		ginkgo.By("Creating Statefulset with 3 replica")
		sts1 := GetStatefulSetFromManifest(namespace)
		ginkgo.By("Creating statefulset")
		CreateStatefulSet(namespace, sts1, client)
		sts1Replicas := *(sts1.Spec.Replicas)
		defer func() {
			framework.Logf("Deleting all statefulset in namespace: %v", namespace)
			fss.DeleteAllStatefulSets(client, namespace)
		}()

		// Waiting for pods status to be Ready.
		fss.WaitForStatusReadyReplicas(client, sts1, sts1Replicas)
		gomega.Expect(fss.CheckMount(client, sts1, mountPath)).NotTo(gomega.HaveOccurred())
		ssPodsBeforeScaleDown := GetListOfPodsInSts(client, sts1)
		gomega.Expect(ssPodsBeforeScaleDown.Items).NotTo(gomega.BeEmpty(),
			fmt.Sprintf("Unable to get list of Pods from the Statefulset: %v", sts1.Name))
		gomega.Expect(len(ssPodsBeforeScaleDown.Items) == int(sts1Replicas)).To(gomega.BeTrue(),
			"Number of Pods in the statefulset should match with number of replicas")

		//verifying volume provisioning
		ginkgo.By("Verify volume is provisioned on the preferred datatsore")
		err = verifyVolumeProvisioningForStatefulSet(ctx, client, sts1, namespace, preferredDatastorePaths,
			allDatastoresListMap, true, false)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		// Verify affinity details
		ginkgo.By("Verify node and pv topology affinity details")
		verifyPVnodeAffinityAndPODnodedetailsForStatefulsetsLevel5(ctx, client, sts1,
			namespace, allowedTopologies, false)

		// perform statefulset scaleup
		sts1Replicas = 13
		ginkgo.By("Scale up statefulset replica count from 7 to 13")
		scaleUpStatefulSetPod(ctx, client, sts1, namespace, sts1Replicas, false)

		//verifying volume provisioning
		ginkgo.By("Verify volume is provisioned on the preferred datatsore")
		err = verifyVolumeProvisioningForStatefulSet(ctx, client, sts1, namespace, preferredDatastorePaths,
			allDatastoresListMap, true, false)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		// Verify affinity details
		ginkgo.By("Verify node and pv topology affinity details")
		verifyPVnodeAffinityAndPODnodedetailsForStatefulsetsLevel5(ctx, client, sts1,
			namespace, allowedTopologies, false)
	})

	/*
		Testcase-16:
		Invalid Tag assign
		Assign tag rack-1 to datastores which is accessible only on rack-2

		Steps
		1. Assign Tag rack-1, Category "cns.vmware.topology-preferred-datastores" to all
		datastore which is accessible only on rack-2. (ex - NFS-2, vSAN-2)
		2. Create SC with Immediate binding mode with allowed topologies set to rack-2
		3. Create PVC using above created SC
		4. Check PVC status. It should reach Bound status.
		5. Create Pod using PVC. Verify Pod running status.
		6. Verify volume provisioning on the preferred datastore.
		7. Verify Pod node affinity details.
		5. Perform cleanup. Delete PVC and SC.
		6. Remove datastore preference tags as part of cleanup.
	*/
	ginkgo.It("Assign tag rack-1 to datastores which is accessible only on rack-2", func() {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		preferredDatastoreChosen = 2
		preferredDatastorePaths = nil

		// choose preferred datastore
		ginkgo.By("Tag rack-1 to preferred datatsore which is accessible only on rack-2")
		preferredDatastorePaths, err := tagPreferredDatastore(masterIp, allowedTopologyRacks[0],
			preferredDatastoreChosen, nonShareddatastoreListMapRack2, nil)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		preferredDatastoreChosen = 1
		preferredDatastore, err := tagPreferredDatastore(masterIp, allowedTopologyRacks[0],
			preferredDatastoreChosen, shareddatastoreListMap, nil)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		preferredDatastorePaths = append(preferredDatastorePaths, preferredDatastore...)
		defer func() {
			for i := 0; i < len(preferredDatastorePaths); i++ {
				err = detachTagCreatedOnPreferredDatastore(masterIp, preferredDatastorePaths[i],
					allowedTopologyRacks[0])
			}
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}()

		framework.Logf("Waiting for %v for preferred datastore to get refreshed in the environment",
			preferredDatastoreTimeOutInterval)
		time.Sleep(preferredDatastoreTimeOutInterval)

		ginkgo.By("Creating service")
		service := CreateService(namespace, client)
		defer func() {
			deleteService(namespace, client, service)
		}()

		// Creating Storage Class and PVC
		ginkgo.By("Creating Storage class and standalone PVC")
		storageclass, pvclaim, err := createPVCAndStorageClass(client, namespace, nil, nil, "",
			allowedTopologyForRack2, "", false, "")
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		defer func() {
			err = client.StorageV1().StorageClasses().Delete(ctx, storageclass.Name, *metav1.NewDeleteOptions(0))
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
			rack2DatastoreListMap)

		ginkgo.By("Verify PV node affinity and that the PODS are running on appropriate " +
			"node as specified in the allowed topologies of SC")
		verifyPVnodeAffinityAndPODnodedetailsFoStandalonePodLevel5(ctx, client, pod, namespace,
			allowedTopologies)
	})

	/*
		Testcase-21:
		Use vSan default storage policy and give preference to other datatsores

		Steps
		1. Assign Tag rack-1, Category "cns.vmware.topology-preferred-datastores" to datastore
		shared across all racks (ex - sharedVMFS-12)
		2. Assign Tag rack-2, Category "cns.vmware.topology-preferred-datastores" to datastore
		shared across all racks (ex - sharedVMFS-12)
		2. Assign Tag rack-3, Category "cns.vmware.topology-preferred-datastores" to datastore
		shared across all racks (ex - sharedVMFS-12)
		3. Create SC with allowed topologies set to only rack-2 and provide vSAN default Storage Policy
		(having vSAN-1, vSAN-2, vSAN-3) Note: vSAN-1, vSAN-2, vSAN is not marked as preferred datastore
		4. Create StatefulSet Pod with replica count 3.
		5. Verify PVC should go to Bound state.
		6. Describe pv node affinity rules. It should show proper node affinity details.
		7. Verify volume should be provisioned on the datastore mentioned in the storage policy of storage class.
		8. Verify Pod are in running status.
		9. POD should come up on the node which is present in the same details as mentioned in storage class.
		10. Perform cleanup. Delete Pod, PVC and PV, SC.
		11. Remove datastore preference tags as part of cleanup.
	*/
	ginkgo.It("Assign vSan default storage policy and give preference to other datatsores", func() {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		preferredDatastoreChosen = 1
		preferredDatastorePaths = nil
		scParameters := make(map[string]string)
		scParameters[scParamStoragePolicyName] = vsanDefaultStoragePolicyName
		datastoreUrlSpecificToCluster := GetAndExpectStringEnvVar(datastoreUrlSpecificToCluster)

		// choose preferred datastore
		ginkgo.By("Assign Tags to preferred datatsore for volume provisioning")
		preferredDatastorePaths, err := tagPreferredDatastore(masterIp, allowedTopologyRacks[0],
			preferredDatastoreChosen, shareddatastoreListMap, nil)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		err = tagSameDatastoreAsPreferenceToDifferentRacks(masterIp, allowedTopologyRacks[1],
			preferredDatastoreChosen, preferredDatastorePaths)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		err = tagSameDatastoreAsPreferenceToDifferentRacks(masterIp, allowedTopologyRacks[2],
			preferredDatastoreChosen, preferredDatastorePaths)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		defer func() {
			for i := 0; i < len(allowedTopologyRacks); i++ {
				err = detachTagCreatedOnPreferredDatastore(masterIp, preferredDatastorePaths[0],
					allowedTopologyRacks[i])
			}
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}()

		framework.Logf("Waiting for %v for preferred datastore to get refreshed in the environment",
			preferredDatastoreTimeOutInterval)
		time.Sleep(preferredDatastoreTimeOutInterval)

		// Creating Storage Class and PVC
		ginkgo.By("Creating Storage class and standalone PVC")
		storageclass, pvclaim, err := createPVCAndStorageClass(client, namespace, nil, scParameters, "",
			allowedTopologyForRack2, "", false, "")
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		defer func() {
			err = client.StorageV1().StorageClasses().Delete(ctx, storageclass.Name, *metav1.NewDeleteOptions(0))
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

		ginkgo.By("Verify volume is provisioned on the preferred datatsore")
		for _, volumespec := range pod.Spec.Volumes {
			if volumespec.PersistentVolumeClaim != nil {
				pv := getPvFromClaim(client, pod.Namespace, volumespec.PersistentVolumeClaim.ClaimName)
				ginkgo.By(fmt.Sprintf("Invoking QueryCNSVolumeWithResult with VolumeID: %s",
					pv.Spec.CSI.VolumeHandle))
				isDatastoreMatchFound := e2eVSphere.verifyPreferredDatastoreMatch(pv.Spec.CSI.VolumeHandle,
					datastoreUrlSpecificToCluster)
				if isDatastoreMatchFound {
					framework.Logf("volume %s is created on the chosen preferred datastore %v",
						pv.Spec.CSI.VolumeHandle, datastoreUrlSpecificToCluster)
				}
				gomega.Expect(isDatastoreMatchFound).To(gomega.BeTrue(), "Volume is provisioned on wrong datastore")
			}
		}

		ginkgo.By("Verify PV node affinity and that the PODS are running on appropriate " +
			"node as specified in the allowed topologies of SC")
		verifyPVnodeAffinityAndPODnodedetailsFoStandalonePodLevel5(ctx, client, pod, namespace,
			allowedTopologies)
	})

	/*
		Testcase-22:
		Assign invalid storage policy and give preference to the datatsore specific to rack-1

		Steps
		1. Assign Tag rack-1, Category "cns.vmware.topology-preferred-datastores" to datastore specific
		to rack-1. (ex - NFS-1)
		2. Create SC with allowed topologies set to only rack-1  and provide Storage Policy with
		no datastore tagged to it.
		3. Create PVC using SC created above.
		4. Verify PVC should be stuck in Pending state and appropriate error message should be displayed.
		5. Perform cleanup. Delete PVC and SC.
		6. Remove datastore preference tags as part of cleanup.
	*/
	ginkgo.It("Assign invalid storage policy and give preference to the datatsore specific to rack-1", func() {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		preferredDatastoreChosen = 1
		preferredDatastorePaths = nil
		scParameters := make(map[string]string)
		nFSDefaultStoragePolicyName := GetAndExpectStringEnvVar(nFSDefaultStoragePolicyName)
		scParameters[scParamStoragePolicyName] = nFSDefaultStoragePolicyName

		// choose preferred datastore
		ginkgo.By("Assign Tags to preferred datatsore for volume provisioning")
		preferredDatastorePaths, err := tagPreferredDatastore(masterIp, allowedTopologyRacks[0],
			preferredDatastoreChosen, nonShareddatastoreListMapRack1, nil)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		defer func() {
			err = detachTagCreatedOnPreferredDatastore(masterIp, preferredDatastorePaths[0],
				allowedTopologyRacks[0])
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}()

		framework.Logf("Waiting for %v for preferred datastore to get refreshed in the environment",
			preferredDatastoreTimeOutInterval)
		time.Sleep(preferredDatastoreTimeOutInterval)

		ginkgo.By("Create storage class and PVC")
		storageclass, pvclaim, err := createPVCAndStorageClass(client,
			namespace, nil, scParameters, "", allowedTopologyForRack1, "", false, "")
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		defer func() {
			err = client.StorageV1().StorageClasses().Delete(ctx, storageclass.Name, *metav1.NewDeleteOptions(0))
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			err = fpv.DeletePersistentVolumeClaim(client, pvclaim.Name, namespace)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}()

		ginkgo.By("Expect claim to fail provisioning volume within the topology")
		framework.ExpectError(fpv.WaitForPersistentVolumeClaimPhase(v1.ClaimBound,
			client, pvclaim.Namespace, pvclaim.Name, pollTimeoutShort, framework.PollShortTimeout))
		// Get the event list and verify if it contains expected error message
		eventList, _ := client.CoreV1().Events(pvclaim.Namespace).List(ctx, metav1.ListOptions{})
		gomega.Expect(eventList.Items).NotTo(gomega.BeEmpty())
		expectedErrMsg := "No compatible shared datastores found for storage policy"
		err = waitForEvent(ctx, client, namespace, expectedErrMsg, pvclaim.Name)
		gomega.Expect(err).NotTo(gomega.HaveOccurred(), fmt.Sprintf("Expected error : %q", expectedErrMsg))
	})

	/*
		Testcase-23:
		Datastore preference when sps service is down

		Steps
		1. Assign Tag rack-2, Category "cns.vmware.topology-preferred-datastores" to datastore specific
		to rack-2 ex - NFS-2
		2. Assign Tag rack-2, Category "cns.vmware.topology-preferred-datastores" to datastorew specific
		to site-2 ex - vSAN-2
		3. Create StorageClass with allowed topology set to rack-2
		4. Create standalone PVC-1.
		5. Wait for PVC-1 to got to Bound state.
		6. Describe PV-1 node affinity and verify volume should be provisoned on the preferred datastore.
		7. Bring down sps-service.
		8. Try to create a new PVC-2.
		9. Verify PVC-2 creation status.
		10. Bring up sps service.
		11. Verify PVC-2 should reach Bound state.
		12. Describe PV-2 node affinity and verify volume provisioning.
		13. Create Pod-1 and Pod-2 using PVC-1 and PVC-2.
		14. Verify Pod running status and check its node affinity details.
		15. Perform cleanup. Delete Pod,PVC and SC.
		16. Remove datastore preference tags as part of cleanup.
	*/
	ginkgo.It("Datastore preference when sps service is down", func() {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		preferredDatastoreChosen = 2
		preferredDatastorePaths = nil

		// choose preferred datastore
		ginkgo.By("Assign Tag to preferred datatsore for volume provisioning in rack-2(cluster-2)")
		preferredDatastorePaths, err := tagPreferredDatastore(masterIp, allowedTopologyRacks[1],
			preferredDatastoreChosen, nonShareddatastoreListMapRack2, nil)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		defer func() {
			for i := 0; i < len(preferredDatastorePaths); i++ {
				err = detachTagCreatedOnPreferredDatastore(masterIp, preferredDatastorePaths[i],
					allowedTopologyRacks[1])
			}
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}()

		framework.Logf("Waiting for %v for preferred datastore to get refreshed in the environment",
			preferredDatastoreTimeOutInterval)
		time.Sleep(preferredDatastoreTimeOutInterval)

		// Creating Storage Class and PVC
		ginkgo.By("Creating Storage class and standalone PVC")
		storageclass, pvclaim, err := createPVCAndStorageClass(client, namespace, nil, nil, "",
			allowedTopologyForRack2, "", false, "")
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		defer func() {
			err = client.StorageV1().StorageClasses().Delete(ctx, storageclass.Name, *metav1.NewDeleteOptions(0))
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

		ginkgo.By("Bring down SPS service")
		isSPSServiceStopped = true
		err = invokeVCenterServiceControl(stopOperation, spsServiceName, vcAddress)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		err = waitVCenterServiceToBeInState(spsServiceName, vcAddress, svcStoppedMessage)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		defer func() {
			if isSPSServiceStopped {
				framework.Logf("Bringing sps up before terminating the test")
				startVCServiceWait4VPs(ctx, vcAddress, spsServiceName, &isSPSServiceStopped)
			}
		}()

		// Create PVC using above SC
		pvc, err := createPVC(client, namespace, nil, "", storageclass, "")
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		defer func() {
			if pvc != nil {
				ginkgo.By("Delete the PVC")
				err = fpv.DeletePersistentVolumeClaim(client, pvc.Name, namespace)
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
			}
		}()

		ginkgo.By("Expect claim status to be in Pending state since sps service is down")
		err = fpv.WaitForPersistentVolumeClaimPhase(v1.ClaimPending, client,
			pvc.Namespace, pvc.Name, framework.Poll, time.Minute)
		gomega.Expect(err).NotTo(gomega.HaveOccurred(),
			fmt.Sprintf("Failed to find the volume in pending state with err: %v", err))

		ginkgo.By("Bringup SPS service")
		startVCServiceWait4VPs(ctx, vcAddress, spsServiceName, &isSPSServiceStopped)

		ginkgo.By("Expect claim to be in Bound state")
		pvcs1, err := fpv.WaitForPVClaimBoundPhase(client,
			[]*v1.PersistentVolumeClaim{pvc}, framework.ClaimProvisionTimeout)
		gomega.Expect(err).NotTo(gomega.HaveOccurred(), "Failed to provision volume")
		pvc1 := pvcs1[0]
		defer func() {
			err = fpv.DeletePersistentVolumeClaim(client, pvc.Name, pvc.Namespace)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			err = e2eVSphere.waitForCNSVolumeToBeDeleted(pvc1.Spec.CSI.VolumeHandle)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}()

		// Creating Pod and verifying volume is attached to the node
		ginkgo.By("Creating a pod")
		pod1, err := createPod(client, namespace, nil, []*v1.PersistentVolumeClaim{pvclaim}, false, "")
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		ginkgo.By(fmt.Sprintf("Verify volume:%s is attached to the node: %s",
			pv1.Spec.CSI.VolumeHandle, pod1.Spec.NodeName))
		vmUUID := getNodeUUID(ctx, client, pod1.Spec.NodeName)
		isDiskAttached, err := e2eVSphere.isVolumeAttachedToVM(client, pv1.Spec.CSI.VolumeHandle, vmUUID)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		gomega.Expect(isDiskAttached).To(gomega.BeTrue(), "Volume is not attached to the node")
		defer func() {
			ginkgo.By("Deleting the pod and wait for disk to detach")
			err := fpod.DeletePodWithWait(client, pod1)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())

			ginkgo.By("Verify volume is detached from the node")
			isDiskDetached, err := e2eVSphere.waitForVolumeDetachedFromNode(client,
				pv1.Spec.CSI.VolumeHandle, pod1.Spec.NodeName)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			gomega.Expect(isDiskDetached).To(gomega.BeTrue(),
				fmt.Sprintf("Volume %q is not detached from the node %q", pv1.Spec.CSI.VolumeHandle, pod1.Spec.NodeName))
		}()

		// Creating Pod and verifying volume is attached to the node
		ginkgo.By("Creating a pod")
		pod2, err := createPod(client, namespace, nil, []*v1.PersistentVolumeClaim{pvc}, false, "")
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		ginkgo.By(fmt.Sprintf("Verify volume:%s is attached to the node: %s",
			pvc1.Spec.CSI.VolumeHandle, pod2.Spec.NodeName))
		vmUUID2 := getNodeUUID(ctx, client, pod2.Spec.NodeName)
		isDiskAttached, err = e2eVSphere.isVolumeAttachedToVM(client, pvc1.Spec.CSI.VolumeHandle, vmUUID2)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		gomega.Expect(isDiskAttached).To(gomega.BeTrue(), "Volume is not attached to the node")
		defer func() {
			ginkgo.By("Deleting the pod and wait for disk to detach")
			err := fpod.DeletePodWithWait(client, pod2)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())

			ginkgo.By("Verify volume is detached from the node")
			isDiskDetached, err := e2eVSphere.waitForVolumeDetachedFromNode(client,
				pvc1.Spec.CSI.VolumeHandle, pod2.Spec.NodeName)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			gomega.Expect(isDiskDetached).To(gomega.BeTrue(),
				fmt.Sprintf("Volume %q is not detached from the node %q", pvc1.Spec.CSI.VolumeHandle, pod2.Spec.NodeName))
		}()

		// verifying volume provisioning
		ginkgo.By("Verify volume is provisioned on the preferred datatsore")
		verifyVolumeProvisioningForStandalonePods(ctx, client, pod1, namespace, preferredDatastorePaths,
			nonShareddatastoreListMapRack2)

		// verifying volume provisioning
		ginkgo.By("Verify volume is provisioned on the preferred datatsore")
		verifyVolumeProvisioningForStandalonePods(ctx, client, pod2, namespace, preferredDatastorePaths,
			nonShareddatastoreListMapRack2)

		ginkgo.By("Verify PV node affinity and that the PODS are running on appropriate " +
			"node as specified in the allowed topologies of SC")
		verifyPVnodeAffinityAndPODnodedetailsFoStandalonePodLevel5(ctx, client, pod1, namespace,
			allowedTopologies)

		ginkgo.By("Verify PV node affinity and that the PODS are running on appropriate " +
			"node as specified in the allowed topologies of SC")
		verifyPVnodeAffinityAndPODnodedetailsFoStandalonePodLevel5(ctx, client, pod2, namespace,
			allowedTopologies)
	})

})
