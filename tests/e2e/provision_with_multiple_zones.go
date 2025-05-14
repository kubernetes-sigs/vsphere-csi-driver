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
	"strings"

	"github.com/onsi/ginkgo/v2"
	"github.com/onsi/gomega"
	v1 "k8s.io/api/core/v1"
	storagev1 "k8s.io/api/storage/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	clientset "k8s.io/client-go/kubernetes"
	"k8s.io/kubernetes/test/e2e/framework"
	fnodes "k8s.io/kubernetes/test/e2e/framework/node"
	fpod "k8s.io/kubernetes/test/e2e/framework/pod"
	fpv "k8s.io/kubernetes/test/e2e/framework/pv"
	fss "k8s.io/kubernetes/test/e2e/framework/statefulset"
	admissionapi "k8s.io/pod-security-admission/api"
)

var _ = ginkgo.Describe("[csi-topology-vanilla] Topology-Aware-Provisioning-With-Multiple-Zones", func() {
	f := framework.NewDefaultFramework("e2e-vsphere-topology-aware-provisioning")
	f.NamespacePodSecurityEnforceLevel = admissionapi.LevelPrivileged
	var (
		client               clientset.Interface
		namespace            string
		zoneValues           []string
		regionValues         []string
		allowedTopologies    []v1.TopologySelectorLabelRequirement
		pvclaim              *v1.PersistentVolumeClaim
		pv                   *v1.PersistentVolume
		storageclass         *storagev1.StorageClass
		topologyWithSharedDS string
		err                  error
	)
	ginkgo.BeforeEach(func() {
		client = f.ClientSet
		namespace = f.Namespace.Name
		bootstrap()
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		nodeList, err := fnodes.GetReadySchedulableNodes(ctx, f.ClientSet)
		framework.ExpectNoError(err, "Unable to find ready and schedulable Node")
		if !(len(nodeList.Items) > 0) {
			framework.Failf("Unable to find ready and schedulable Node")
		}
		// Preparing allowedTopologies using topologies with shared and non shared datastores
		topologyWithSharedDS = GetAndExpectStringEnvVar(envRegionZoneWithSharedDS)
		topologyWithNoSharedDS := GetAndExpectStringEnvVar(envRegionZoneWithNoSharedDS)
		topologyValues := topologyWithSharedDS + "," + topologyWithNoSharedDS
		regionValues, zoneValues, allowedTopologies = topologyParameterForStorageClass(topologyValues)
	})

	ginkgo.AfterEach(func() {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		ginkgo.By("Performing test cleanup")
		if pvclaim != nil {
			framework.ExpectNoError(fpv.DeletePersistentVolumeClaim(ctx, client, pvclaim.Name, namespace),
				"Failed to delete PVC ", pvclaim.Name)
		}

		if pv != nil {
			framework.ExpectNoError(fpv.WaitForPersistentVolumeDeleted(ctx, client, pv.Name, poll, pollTimeoutShort))
			framework.ExpectNoError(e2eVSphere.waitForCNSVolumeToBeDeleted(pv.Spec.CSI.VolumeHandle))
			err = client.StorageV1().StorageClasses().Delete(ctx, storageclass.Name, *metav1.NewDeleteOptions(0))
		}
	})

	/*
		Provisioning with multiple zones and regions specified in the Storage Class.
		Volume will be provisioned on first zone/region with shared datastore across nodes.

		Steps
		1. Create a Storage Class with multiple valid regions and zones specified in “AllowedTopologies”
		2. Create a PVC using above SC
		3. Wait for PVC to be in bound phase
		4. Verify PV is created in zone and region that has shared accessible datastores across all nodes in this zone/region
		5. Create a Pod attached to the above PV
		6. Verify Pod is scheduled on node located within the specified zone and region
		7. Delete Pod and wait for disk to be detached
		8. Delete PVC
		9. Delete Storage Class
	*/
	ginkgo.It("Verify provisioning with multiple zones and with only one zone associated with "+
		"shared datastore", ginkgo.Label(p0, block, vanilla, level2, stable), func() {

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		storageclass, pvclaim, err = createPVCAndStorageClass(ctx, client,
			namespace, nil, nil, "", allowedTopologies, "", false, "")
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		defer func() {
			err = client.StorageV1().StorageClasses().Delete(ctx, storageclass.Name, *metav1.NewDeleteOptions(0))
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}()
		ginkgo.By("Expect claim to pass provisioning volume")
		err = fpv.WaitForPersistentVolumeClaimPhase(ctx, v1.ClaimBound, client,
			pvclaim.Namespace, pvclaim.Name, framework.Poll, framework.ClaimProvisionTimeout)
		gomega.Expect(err).NotTo(gomega.HaveOccurred(), fmt.Sprintf("Failed to provision volume with err: %v", err))

		ginkgo.By("Verify if volume is provisioned in specified zone and region")
		pv = getPvFromClaim(client, pvclaim.Namespace, pvclaim.Name)
		pvRegion, pvZone, err := verifyVolumeTopology(pv, zoneValues, regionValues)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Verify if volume is provisioned in zone and region containing shared datastore")
		gomega.Expect(strings.Contains(topologyWithSharedDS, pvRegion)).To(gomega.BeTrue(),
			fmt.Sprintf("Topology with shared datatore %q does not contain region in which volume is provisioned: %q",
				topologyWithSharedDS, pvRegion))
		gomega.Expect(strings.Contains(topologyWithSharedDS, pvZone)).To(gomega.BeTrue(),
			fmt.Sprintf("Topology with shared datatore %q does not contain zone in which volume is provisioned: %q",
				topologyWithSharedDS, pvZone))

		ginkgo.By("Creating a pod")
		pod, err := createPod(ctx, client, namespace, nil, []*v1.PersistentVolumeClaim{pvclaim}, false, "")
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By(fmt.Sprintf("Verify volume: %s is attached to the node: %s",
			pv.Spec.CSI.VolumeHandle, pod.Spec.NodeName))
		vmUUID := getNodeUUID(ctx, client, pod.Spec.NodeName)
		isDiskAttached, err := e2eVSphere.isVolumeAttachedToVM(client, pv.Spec.CSI.VolumeHandle, vmUUID)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		gomega.Expect(isDiskAttached).To(gomega.BeTrue(), "Volume is not attached to the node")

		ginkgo.By("Verify Pod is scheduled on a node belonging to same topology as the shared datastore")
		nodeList, err := fnodes.GetReadySchedulableNodes(ctx, f.ClientSet)
		framework.ExpectNoError(err, "Unable to find ready and schedulable Node")
		if !(len(nodeList.Items) > 0) {
			framework.Failf("Unable to find ready and schedulable Node")
		}
		err = verifyPodLocation(pod, nodeList, pvZone, pvRegion)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Performing cleanup")
		ginkgo.By("Deleting the pod and wait for disk to detach")
		err = fpod.DeletePodWithWait(ctx, client, pod)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Deleting the PVC")
		err = fpv.DeletePersistentVolumeClaim(ctx, client, pvclaim.Name, namespace)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		pvclaim = nil

		ginkgo.By("Verify if PV is deleted")
		framework.ExpectNoError(fpv.WaitForPersistentVolumeDeleted(ctx, client, pv.Name, poll, pollTimeoutShort))
		pv = nil
	})

	/*
		Provisioning volume using storage policy with multiple zone and region details
			in the allowed topology
		//Steps
		1. Create SC with multiple Zone and region details specified in the SC
		2. Create statefulset with replica 3 using the above SC
		3. Wait for PV, PVC to bound
		4. Statefulset should get distributed across zones.
		5. Describe PV and verify node affinity details should contain both
			zone and region details
		5a. If a volume provisioned on datastore that is shared only within
			zone1 then node affinity will have details of only zone1
		5b. If a volume provisioned on datastore that is shared only on zone2 then
			node affinity will have details of only zone2
		6. Verify statefulset pod is running on the same node as mentioned in
			node affinity details
		7. Delete POD,PVC,PV
	*/
	ginkgo.It("Provisioning volume using storage policy with multiple zone and region "+
		"details in the allowed topology", ginkgo.Label(p0, block, vanilla, level2, stable), func() {

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		// Took Region1, Zone1 of Cluster1 and Region2, Zone2 of Cluster2
		topologyLabelsCluster1 := GetAndExpectStringEnvVar(envRegionZoneWithSharedDS)
		topologyLabelsCluster2 := GetAndExpectStringEnvVar(envRegionZoneWithNoSharedDS)
		topologyValues := topologyLabelsCluster1 + "," + topologyLabelsCluster2
		regionValues, zoneValues, allowedTopologies = topologyParameterForStorageClass(topologyValues)

		// Creating StorageClass with multiple zone and region topology details
		ginkgo.By("Creating StorageClass for Statefulset")
		scSpec := getVSphereStorageClassSpec(defaultNginxStorageClassName, nil, allowedTopologies, "", "", false)
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
		var replica int32 = 3
		statefulset.Spec.Replicas = &replica
		replicas := *(statefulset.Spec.Replicas)
		CreateStatefulSet(namespace, statefulset, client)
		defer func() {
			framework.Logf("Deleting all statefulset in namespace: %v", namespace)
			fss.DeleteAllStatefulSets(ctx, client, namespace)
		}()

		// Waiting for pods status to be Ready.
		fss.WaitForStatusReadyReplicas(ctx, client, statefulset, replicas)
		gomega.Expect(fss.CheckMount(ctx, client, statefulset, mountPath)).NotTo(gomega.HaveOccurred())
		ssPodsBeforeScaleDown, err := fss.GetPodList(ctx, client, statefulset)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		gomega.Expect(ssPodsBeforeScaleDown.Items).NotTo(gomega.BeEmpty(),
			fmt.Sprintf("Unable to get list of Pods from the Statefulset: %v", statefulset.Name))
		gomega.Expect(len(ssPodsBeforeScaleDown.Items) == int(replicas)).To(gomega.BeTrue(),
			"Number of Pods in the statefulset should match with number of replicas")

		/* Verify node and pv topology affinity should contains specified zone and region
		details of SC and statefulset is distributed across zones and regions */
		ginkgo.By("Verify node and pv topology affinity should contains specified zone and region details of SC")
		verifyPVnodeAffinityAndPODnodedetailsForStatefulsets(ctx, client, statefulset, namespace, zoneValues, regionValues)
	})

})
