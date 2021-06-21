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
	"fmt"
	"strings"

	"github.com/onsi/ginkgo"
	"github.com/onsi/gomega"
	v1 "k8s.io/api/core/v1"
	storagev1 "k8s.io/api/storage/v1"
	clientset "k8s.io/client-go/kubernetes"
	"k8s.io/kubernetes/test/e2e/framework"
)

var _ = ginkgo.Describe("[csi-topology-vanilla] Topology-Aware-Provisioning-With-Only-Zone-Or-Region", func() {
	f := framework.NewDefaultFramework("e2e-vsphere-topology-aware-provisioning")
	var (
		client            clientset.Interface
		namespace         string
		regionZoneValue   string
		zoneValues        []string
		regionValues      []string
		pvZone            string
		pvRegion          string
		allowedTopologies []v1.TopologySelectorLabelRequirement
		nodeList          *v1.NodeList
		pvclaim           *v1.PersistentVolumeClaim
		storageclass      *storagev1.StorageClass
		err               error
	)
	ginkgo.BeforeEach(func() {
		client = f.ClientSet
		namespace = f.Namespace.Name
		bootstrap()
		nodeList = framework.GetReadySchedulableNodesOrDie(f.ClientSet)
		if !(len(nodeList.Items) > 0) {
			framework.Failf("Unable to find ready and schedulable Node")
		}
		// Preparing allowedTopologies using topologies with shared and non shared datastores
		regionZoneValue = GetAndExpectStringEnvVar(envRegionZoneWithSharedDS)
	})

	/*
		Test to verify provisioning volume with only region specified in Storage Class succeeds.
		Volume will be provisioned on shared datastore accessible in the region, regardless of zone.

		Steps
		1.Create a Storage Class with only region specified in “AllowedTopologies”
		2.Create a PVC which uses above SC
		3.Wait for PVC to be in Bound phase
		4.Verify PV is successfully created in specified region
		5.Verify PV contains only Node Affinity rules for region
		6.Create Pod attached to the above PV
		7.Verify Pod is scheduled on node within the region
		8.Delete Pod and wait for disk to be detached
		9.Delete PVC
		10.Delete Storage Class
	*/
	ginkgo.It("Verify provisioning succeeds with only region specified in Storage Class", func() {
		// Extract the region section of the topology values
		// <region-with-shared-datastore>:<zone-with-shared-datastore>
		// Value = <region-with-shared-datastore>:
		regionZone := strings.Split(regionZoneValue, ":")
		inputRegion := regionZone[0]
		topologyRegionOnly := inputRegion + ":"
		regionValues, zoneValues, allowedTopologies = topologyParameterForStorageClass(topologyRegionOnly)
		sharedDatastoreURL := GetAndExpectStringEnvVar(envSharedDatastoreURL)
		scParameters := make(map[string]string)
		scParameters[scParamDatastoreURL] = sharedDatastoreURL
		storageclass, pvclaim, err = createPVCAndStorageClass(client, namespace, nil, scParameters, "", allowedTopologies, "", false, "")
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Expect claim to pass provisioning volume")
		err = framework.WaitForPersistentVolumeClaimPhase(v1.ClaimBound, client, pvclaim.Namespace, pvclaim.Name, framework.Poll, pollTimeoutShort)
		gomega.Expect(err).NotTo(gomega.HaveOccurred(), fmt.Sprintf("failed to provision volume with err: %v", err))

		ginkgo.By("Verify if volume is provisioned in specified region")
		pv := getPvFromClaim(client, pvclaim.Namespace, pvclaim.Name)
		pvRegion, _, err = verifyVolumeTopology(pv, nil, regionValues)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Creating a pod")
		pod, err := CreatePod(client, namespace, nil, []*v1.PersistentVolumeClaim{pvclaim}, false, "")
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By(fmt.Sprintf("Verify volume: %s is attached to the node: %s", pv.Spec.CSI.VolumeHandle, pod.Spec.NodeName))
		vmUUID := getNodeUUID(client, pod.Spec.NodeName)
		isDiskAttached, err := e2eVSphere.isVolumeAttachedToVM(client, pv.Spec.CSI.VolumeHandle, vmUUID)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		gomega.Expect(isDiskAttached).To(gomega.BeTrue(), "Volume is not attached to the node")

		ginkgo.By("Verify Pod is scheduled on a node belonging to same topology as the PV it is attached to")
		err = verifyPodLocation(pod, nodeList, "", pvRegion)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Performing cleanup")
		ginkgo.By("Deleting the pod and wait for disk to detach")
		err = framework.DeletePodWithWait(f, client, pod)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Deleting the PVC")
		err = framework.DeletePersistentVolumeClaim(client, pvclaim.Name, namespace)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Deleting the Storage Class")
		err = client.StorageV1().StorageClasses().Delete(storageclass.Name, nil)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
	})

	/*
		Test to verify provisioning volume with only zone specified in Storage Class succeeds.
		Volume should be provisioned in the zone.

		Steps
		1.Create a Storage Class with only zone specified in “AllowedTopologies”
		2.Create a PVC which uses above SC
		3.Wait for PVC to be in Bound phase
		4.Verify PV is successfully created in specified zone
		5.Verify PV contains only Node Affinity rules for zone
		6.Create Pod attached to the above PV
		7.Verify Pod is scheduled on node within the zone
		8.Delete Pod and wait for disk to be detached
		9.Delete PVC
		10.Delete Storage Class
	*/
	ginkgo.It("Verify provisioning succeeds with only zone specified in Storage Class", func() {
		// Extract the zone section of the topology values
		// <region-with-shared-datastore>:<zone-with-shared-datastore>
		// Value = :<zone-with-shared-datastore>
		regionZone := strings.Split(regionZoneValue, ":")
		inputZone := regionZone[1]
		topologyZoneOnly := ":" + inputZone
		regionValues, zoneValues, allowedTopologies = topologyParameterForStorageClass(topologyZoneOnly)
		sharedDatastoreURL := GetAndExpectStringEnvVar(envSharedDatastoreURL)
		scParameters := make(map[string]string)
		scParameters[scParamDatastoreURL] = sharedDatastoreURL
		storageclass, pvclaim, err = createPVCAndStorageClass(client, namespace, nil, scParameters, "", allowedTopologies, "", false, "")
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Expect claim to pass provisioning volume")
		err = framework.WaitForPersistentVolumeClaimPhase(v1.ClaimBound, client, pvclaim.Namespace, pvclaim.Name, framework.Poll, pollTimeoutShort)
		gomega.Expect(err).NotTo(gomega.HaveOccurred(), fmt.Sprintf("failed to provision volume with err: %v", err))

		ginkgo.By("Verify if volume is provisioned in specified zone")
		pv := getPvFromClaim(client, pvclaim.Namespace, pvclaim.Name)
		_, pvZone, err = verifyVolumeTopology(pv, zoneValues, nil)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Creating a pod")
		pod, err := CreatePod(client, namespace, nil, []*v1.PersistentVolumeClaim{pvclaim}, false, "")
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By(fmt.Sprintf("Verify volume: %s is attached to the node: %s", pv.Spec.CSI.VolumeHandle, pod.Spec.NodeName))
		vmUUID := getNodeUUID(client, pod.Spec.NodeName)
		isDiskAttached, err := e2eVSphere.isVolumeAttachedToVM(client, pv.Spec.CSI.VolumeHandle, vmUUID)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		gomega.Expect(isDiskAttached).To(gomega.BeTrue(), "Volume is not attached to the node")

		ginkgo.By("Verify Pod is scheduled on a node belonging to same topology as the PV it is attached to")
		err = verifyPodLocation(pod, nodeList, pvZone, "")
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Performing cleanup")
		ginkgo.By("Deleting the pod and wait for disk to detach")
		err = framework.DeletePodWithWait(f, client, pod)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Deleting the PVC")
		err = framework.DeletePersistentVolumeClaim(client, pvclaim.Name, namespace)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Deleting the Storage Class")
		err = client.StorageV1().StorageClasses().Delete(storageclass.Name, nil)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
	})
})
