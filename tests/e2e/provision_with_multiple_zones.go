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
	clientset "k8s.io/client-go/kubernetes"
	"k8s.io/kubernetes/test/e2e/framework"
)

var _ = ginkgo.Describe("[csi-topology-vanilla] Topology-Aware-Provisioning-With-Multiple-Zones", func() {
	f := framework.NewDefaultFramework("e2e-vsphere-topology-aware-provisioning")
	var (
		client               clientset.Interface
		namespace            string
		zoneValues           []string
		regionValues         []string
		allowedTopologies    []v1.TopologySelectorLabelRequirement
		nodeList             *v1.NodeList
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
		nodeList = framework.GetReadySchedulableNodesOrDie(f.ClientSet)
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
		ginkgo.By("Performing test cleanup")
		if pvclaim != nil {
			framework.ExpectNoError(framework.DeletePersistentVolumeClaim(client, pvclaim.Name, namespace), "Failed to delete PVC ", pvclaim.Name)
		}

		if pv != nil {
			framework.ExpectNoError(framework.WaitForPersistentVolumeDeleted(client, pv.Name, poll, pollTimeoutShort))
			framework.ExpectNoError(e2eVSphere.waitForCNSVolumeToBeDeleted(pv.Spec.CSI.VolumeHandle))
			err = client.StorageV1().StorageClasses().Delete(storageclass.Name, nil)
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
	ginkgo.It("Verify provisioning with multiple zones and with only one zone associated with shared datastore", func() {
		storageclass, pvclaim, err = createPVCAndStorageClass(client, namespace, nil, nil, "", allowedTopologies, "", false, "")
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		defer func() {
			err = client.StorageV1().StorageClasses().Delete(storageclass.Name, nil)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			err = client.CoreV1().PersistentVolumeClaims(namespace).Delete(pvclaim.Name, nil)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}()
		ginkgo.By("Expect claim to pass provisioning volume")
		err = framework.WaitForPersistentVolumeClaimPhase(v1.ClaimBound, client, pvclaim.Namespace, pvclaim.Name, framework.Poll, time.Minute)
		gomega.Expect(err).NotTo(gomega.HaveOccurred(), fmt.Sprintf("failed to provision volume with err: %v", err))

		ginkgo.By("Verify if volume is provisioned in specified zone and region")
		pv = getPvFromClaim(client, pvclaim.Namespace, pvclaim.Name)
		pvRegion, pvZone, err := verifyVolumeTopology(pv, zoneValues, regionValues)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Verify if volume is provisioned in zone and region containing shared datastore")
		gomega.Expect(strings.Contains(topologyWithSharedDS, pvRegion)).To(gomega.BeTrue(), fmt.Sprintf("Topology with shared datatore %q does not contain region in which volume is provisioned: %q", topologyWithSharedDS, pvRegion))
		gomega.Expect(strings.Contains(topologyWithSharedDS, pvZone)).To(gomega.BeTrue(), fmt.Sprintf("Topology with shared datatore %q does not contain zone in which volume is provisioned: %q", topologyWithSharedDS, pvZone))

		ginkgo.By("Creating a pod")
		pod, err := framework.CreatePod(client, namespace, nil, []*v1.PersistentVolumeClaim{pvclaim}, false, "")
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By(fmt.Sprintf("Verify volume: %s is attached to the node: %s", pv.Spec.CSI.VolumeHandle, pod.Spec.NodeName))
		vmUUID := getNodeUUID(client, pod.Spec.NodeName)
		isDiskAttached, err := e2eVSphere.isVolumeAttachedToVM(client, pv.Spec.CSI.VolumeHandle, vmUUID)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		gomega.Expect(isDiskAttached).To(gomega.BeTrue(), "Volume is not attached to the node")

		ginkgo.By("Verify Pod is scheduled on a node belonging to same topology as the shared datastore")
		err = verifyPodLocation(pod, nodeList, pvZone, pvRegion)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Performing cleanup")
		ginkgo.By("Deleting the pod and wait for disk to detach")
		err = framework.DeletePodWithWait(f, client, pod)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Deleting the PVC")
		err = framework.DeletePersistentVolumeClaim(client, pvclaim.Name, namespace)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		pvclaim = nil

		ginkgo.By("Verify if PV is deleted")
		framework.ExpectNoError(framework.WaitForPersistentVolumeDeleted(client, pv.Name, poll, pollTimeoutShort))
		pv = nil

		ginkgo.By("Deleting the Storage Class")
		err = client.StorageV1().StorageClasses().Delete(storageclass.Name, nil)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		storageclass = nil
	})
})
