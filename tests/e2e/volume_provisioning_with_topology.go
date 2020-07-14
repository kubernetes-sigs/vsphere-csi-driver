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
	"time"

	"github.com/onsi/ginkgo"
	"github.com/onsi/gomega"
	v1 "k8s.io/api/core/v1"
	storagev1 "k8s.io/api/storage/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	clientset "k8s.io/client-go/kubernetes"
	"k8s.io/kubernetes/test/e2e/framework"
)

var _ = ginkgo.Describe("[csi-topology-vanilla] Basic-Topology-Aware-Provisioning", func() {
	f := framework.NewDefaultFramework("e2e-vsphere-topology-aware-provisioning")
	var (
		client            clientset.Interface
		namespace         string
		zoneValues        []string
		regionValues      []string
		allZones          []string
		allRegions        []string
		pvZone            string
		pvRegion          string
		allowedTopologies []v1.TopologySelectorLabelRequirement
		nodeList          *v1.NodeList
		pod               *v1.Pod
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
		regionZoneValue := GetAndExpectStringEnvVar(envRegionZoneWithSharedDS)
		regionValues, zoneValues, allowedTopologies = topologyParameterForStorageClass(regionZoneValue)

		// Preparing all zones and regions with shared and non shared datastores
		topologyWithSharedDS := GetAndExpectStringEnvVar(envRegionZoneWithSharedDS)
		topologyWithNoSharedDS := GetAndExpectStringEnvVar(envRegionZoneWithNoSharedDS)
		topologyWithOnlyOneNode := GetAndExpectStringEnvVar(envTopologyWithOnlyOneNode)
		topologyValues := topologyWithSharedDS + "," + topologyWithNoSharedDS + "," + topologyWithOnlyOneNode
		allRegions, allZones, _ = topologyParameterForStorageClass(topologyValues)
	})

	testCleanUpUtil := func() {
		ginkgo.By("Performing cleanup")
		ginkgo.By("Deleting the pod and wait for disk to detach")
		err := framework.DeletePodWithWait(f, client, pod)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Deleting the PVC")
		err = framework.DeletePersistentVolumeClaim(client, pvclaim.Name, namespace)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Deleting the Storage Class")
		err = client.StorageV1().StorageClasses().Delete(storageclass.Name, nil)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
	}

	verifyBasicTopologyBasedVolumeProvisioning := func(f *framework.Framework, client clientset.Interface, namespace string, scParameters map[string]string, allowedTopologies []v1.TopologySelectorLabelRequirement) {

		storageclass, pvclaim, err = createPVCAndStorageClass(client, namespace, nil, scParameters, "", allowedTopologies, "", false, "")
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Expect claim to pass provisioning volume")
		err = framework.WaitForPersistentVolumeClaimPhase(v1.ClaimBound, client, pvclaim.Namespace, pvclaim.Name, framework.Poll, time.Minute)
		gomega.Expect(err).NotTo(gomega.HaveOccurred(), fmt.Sprintf("failed to provision volume with err: %v", err))

		ginkgo.By("Verify if volume is provisioned in specified zone and region")
		pv := getPvFromClaim(client, pvclaim.Namespace, pvclaim.Name)
		if allowedTopologies == nil {
			zoneValues = allZones
			regionValues = allRegions
		}
		pvRegion, pvZone, err = verifyVolumeTopology(pv, zoneValues, regionValues)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Creating a pod")
		pod, err := framework.CreatePod(client, namespace, nil, []*v1.PersistentVolumeClaim{pvclaim}, false, "")
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By(fmt.Sprintf("Verify volume:%s is attached to the node: %s", pv.Spec.CSI.VolumeHandle, pod.Spec.NodeName))
		vmUUID := getNodeUUID(client, pod.Spec.NodeName)
		isDiskAttached, err := e2eVSphere.isVolumeAttachedToVM(client, pv.Spec.CSI.VolumeHandle, vmUUID)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		gomega.Expect(isDiskAttached).To(gomega.BeTrue(), "Volume is not attached to the node")

		ginkgo.By("Verify Pod is scheduled on a node belonging to same topology as the PV it is attached to")
		err = verifyPodLocation(pod, nodeList, pvZone, pvRegion)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
	}

	invokeTopologyBasedVolumeProvisioningWithInaccessibleParameters := func(f *framework.Framework, client clientset.Interface, namespace string, scParameters map[string]string, allowedTopologies []v1.TopologySelectorLabelRequirement, expectedErrMsg string) {

		storageclass, pvclaim, err = createPVCAndStorageClass(client, namespace, nil, scParameters, "", allowedTopologies, "", false, "")
		defer func() {
			err = client.StorageV1().StorageClasses().Delete(storageclass.Name, nil)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}()

		ginkgo.By("Expect claim to fail provisioning volume on inaccessible non shared datastore")
		err = framework.WaitForPersistentVolumeClaimPhase(v1.ClaimBound, client, pvclaim.Namespace, pvclaim.Name, framework.Poll, time.Minute/2)
		gomega.Expect(err).To(gomega.HaveOccurred())
		// Get the event list and verify if it contains expected error message
		eventList, _ := client.CoreV1().Events(pvclaim.Namespace).List(metav1.ListOptions{})
		gomega.Expect(eventList.Items).NotTo(gomega.BeEmpty())
		actualErrMsg := eventList.Items[len(eventList.Items)-1].Message
		framework.Logf(fmt.Sprintf("Actual failure message: %+q", actualErrMsg))
		framework.Logf(fmt.Sprintf("Expected failure message: %+q", expectedErrMsg))
		gomega.Expect(strings.Contains(actualErrMsg, expectedErrMsg)).To(gomega.BeTrue(), fmt.Sprintf("actualErrMsg: %q does not contain expectedErrMsg: %q", actualErrMsg, expectedErrMsg))
	}

	/*
		Test to verify provisioning volume with valid zone and region specified in Storage Class succeeds.
		Volume should be provisioned with Node Affinity rules for that zone/region.
		Pod should be scheduled on Node located within that zone/region.

		Steps
		1. Create a Storage Class with valid region and zone specified in “AllowedTopologies”
		2. Create a PVC using above SC
		3. Wait for PVC to be in bound phase
		4. Verify PV is created in specified zone and region
		5. Create a Pod attached to the above PV
		6. Verify Pod is scheduled on node located within the specified zone and region
		7. Delete Pod and wait for disk to be detached
		8. Delete PVC
		9. Delete Storage Class
	*/
	ginkgo.It("Verify provisioning with valid topology specified in Storage Class passes", func() {
		verifyBasicTopologyBasedVolumeProvisioning(f, client, namespace, nil, allowedTopologies)
		testCleanUpUtil()
	})

	/*
		Test to verify if provisioning with valid topology and a shared data store url specified in the Storage Class succeeds.
		Volume should be provisioned with Node Affinity rules for that zone/region and on datastore matching datastoreURL.
		Pod should be scheduled on Node located within that zone/region.

		Steps
		1. Create a Storage Class with spec containing valid region and zone in “AllowedTopologies” and datastoreURL accessible to this zone.
		2. Create a PVC using above SC
		3. Wait for PVC to be in bound phase
		4. Verify volume is created in specified region and zone on datastore matching datastoreURL.
		5. Create a Pod attached to the above PV
		6. Verify Pod is scheduled on node located within the specified zone and region
		7. Delete Pod and wait for disk to be detached
		8. Delete PVC
		9. Delete Storage Class
	*/
	ginkgo.It("Verify provisioning with valid topology and accessible shared datastore specified in Storage Class passes", func() {
		sharedDatastoreURL := GetAndExpectStringEnvVar(envSharedDatastoreURL)
		scParameters := make(map[string]string)
		scParameters[scParamDatastoreURL] = sharedDatastoreURL
		verifyBasicTopologyBasedVolumeProvisioning(f, client, namespace, scParameters, allowedTopologies)
		testCleanUpUtil()
	})

	/*
		Test to verify if provisioning with valid topology and storage policy specified in the Storage Class succeeds.
		Volume should be provisioned with Node Affinity rules for that zone/region and on datastore compatible with Storage Policy specified.
		Pod should be scheduled on Node located within that zone/region.

		Steps
		1. Create a Storage Class with with valid region and zone specified in “AllowedTopologies” and Storage Policy accessible to this zone.
		2. Create a PVC using above SC
		3. Wait for PVC to be in bound phase
		4. Verify volume is created in specified region and zone on datastore compatible with Storage Policy specified.
		5. Create a Pod attached to the above PV
		6. Verify Pod is scheduled on node located within the specified zone and region
		7. Delete Pod and wait for disk to be detached
		8. Delete PVC
		9. Delete Storage Class
	*/
	ginkgo.It("Verify dynamic volume provisioning works when allowed topology and storage policy is specified in the storageclass", func() {
		storagePolicyNameForSharedDatastores := GetAndExpectStringEnvVar(envStoragePolicyNameForSharedDatastores)
		scParameters := make(map[string]string)
		scParameters[scParamStoragePolicyName] = storagePolicyNameForSharedDatastores
		verifyBasicTopologyBasedVolumeProvisioning(f, client, namespace, scParameters, allowedTopologies)
		testCleanUpUtil()
	})

	/*
		Test to verify provisioning volume with valid zone and region fails, when an inaccessible non-shared datastore url is specified in Storage Class.

		Steps
		1. Create a Storage Class with spec containing valid region and zone in “AllowedTopologies” and datastoreURL inaccessible to this zone.
		2. Create a PVC using above SC
		3. Verify PVC creation fails with “Not Accessible” error
		4. Delete PVC
	*/
	ginkgo.It("Verify provisioning volume with valid zone and region fails when an inaccessible non-shared datastore url is specified in Storage Class", func() {
		nonSharedDatastoreURLInZone := GetAndExpectStringEnvVar(envInaccessibleZoneDatastoreURL)
		scParameters := make(map[string]string)
		scParameters[scParamDatastoreURL] = nonSharedDatastoreURLInZone
		errStringToVerify := "DatastoreURL: " + scParameters[scParamDatastoreURL] + " specified in the storage class is not accessible in the topology"
		invokeTopologyBasedVolumeProvisioningWithInaccessibleParameters(f, client, namespace, scParameters, allowedTopologies, errStringToVerify)
	})

	/*
		Test to verify provisioning volume with valid zone and region fails, when storage policy from different zone is specified in Storage Class.

		Steps
		1. Create a Storage Class with spec containing valid region and zone in “AllowedTopologies” and storage policy from different zone.
		2. Create a PVC using above SC
		3. Verify PVC creation fails with “Not Accessible” error
		4. Delete PVC
	*/
	ginkgo.It("Verify provisioning volume with valid zone and region fails when storage policy from different zone is specified in Storage Class", func() {
		storagePolicyNameFromOtherZone := GetAndExpectStringEnvVar(envStoragePolicyNameFromInaccessibleZone)
		scParameters := make(map[string]string)
		scParameters[scParamStoragePolicyName] = storagePolicyNameFromOtherZone
		errStringToVerify := "No compatible datastore found for storagePolicy"
		invokeTopologyBasedVolumeProvisioningWithInaccessibleParameters(f, client, namespace, scParameters, allowedTopologies, errStringToVerify)
	})

	/*
		Test to verify provisioning volume with no zone and region specified in Storage Class succeeds.
		Volume should be provisioned with no Node Affinity rules for that zone/region.

		Steps
		1. Create a Storage Class with no region or zone specified in “AllowedTopologies”
		2. Create a PVC using above SC
		3. Wait for PVC to be in bound phase
		4. Verify volume creation is successful
		5. Verify PV does not contain any node affinity rules
		6. Delete PVC
		7. Delete Storage Class
	*/
	ginkgo.It("Verify provisioning with no topology specified in Storage Class passes", func() {
		verifyBasicTopologyBasedVolumeProvisioning(f, client, namespace, nil, nil)
		testCleanUpUtil()
	})
})
