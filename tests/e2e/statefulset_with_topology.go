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
	"time"

	"github.com/onsi/ginkgo"
	"github.com/onsi/gomega"
	v1 "k8s.io/api/core/v1"
	clientset "k8s.io/client-go/kubernetes"
	"k8s.io/kubernetes/test/e2e/framework"
)

var _ = ginkgo.Describe("[csi-topology-vanilla] Topology-Aware-Provisioning-With-Statefulset", func() {
	f := framework.NewDefaultFramework("e2e-vsphere-topology-aware-provisioning")
	var (
		client            clientset.Interface
		namespace         string
		zoneValues        []string
		regionValues      []string
		pvZone            string
		pvRegion          string
		allowedTopologies []v1.TopologySelectorLabelRequirement
		nodeList          *v1.NodeList
		pod               *v1.Pod
	)
	ginkgo.BeforeEach(func() {
		client = f.ClientSet
		namespace = f.Namespace.Name
		bootstrap()
		nodeList = framework.GetReadySchedulableNodesOrDie(f.ClientSet)
		if !(len(nodeList.Items) > 0) {
			framework.Failf("Unable to find ready and schedulable Node")
		}
		// Preparing allowedTopologies using topologies with shared datastores
		regionZoneValue := GetAndExpectStringEnvVar(envRegionZoneWithSharedDS)
		regionValues, zoneValues, allowedTopologies = topologyParameterForStorageClass(regionZoneValue)
	})

	/*
		Test to verify stateful set with one replica is scheduled on a node within the topology after killing it.
		Stateful set should be scheduled on Node located within the selected zone/region.

		Steps
		1. Create a Storage Class with spec containing valid region and zone in “AllowedTopologies”.
		2. Create Statefulset using the above SC
		3. Verify PV is present in specified Topology
		4. Verify Pod is scheduled on node within the specified zone and region
		5. Kill Pod
		6. Verify Pod is rescheduled to a node within the same zone and region
		7. Delete Statefulset
		8. Delete PVC
		9. Delete SC
	*/
	ginkgo.It("Verify if stateful set is scheduled on a node within the topology after deleting the pod", func() {
		ginkgo.By("Creating StorageClass for Statefulset")
		scSpec := getVSphereStorageClassSpec(storageclassname, nil, allowedTopologies, "", "", false)
		sc, err := client.StorageV1().StorageClasses().Create(scSpec)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		defer func() {
			err = client.StorageV1().StorageClasses().Delete(sc.Name, nil)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}()
		ginkgo.By("Creating statefulset with single replica")
		statefulsetTester := framework.NewStatefulSetTester(client)
		statefulset := createStatefulSetWithOneReplica(client, manifestPath, namespace)
		statefulsetTester.WaitForStatusReadyReplicas(statefulset, 1)
		gomega.Expect(statefulsetTester.CheckMount(statefulset, mountPath)).NotTo(gomega.HaveOccurred())

		ssPodsBeforeDelete := statefulsetTester.GetPodList(statefulset)
		gomega.Expect(ssPodsBeforeDelete.Items).NotTo(gomega.BeEmpty(), fmt.Sprintf("Unable to get list of Pods from the Statefulset: %v", statefulset.Name))
		gomega.Expect(len(ssPodsBeforeDelete.Items) == 1).To(gomega.BeTrue(), "Number of Pods in the statefulset should be 1")

		ginkgo.By("Deleting the pod")
		pod = &ssPodsBeforeDelete.Items[0]
		statefulsetTester.DeleteStatefulPodAtIndex(0, statefulset)

		// Wait for 30 seconds, after deleting the pod. By the end of this wait, the pod would be created again on any of the nodes
		time.Sleep(time.Duration(sleepTimeOut) * time.Second)

		for _, volumespec := range pod.Spec.Volumes {
			if volumespec.PersistentVolumeClaim != nil {
				pv := getPvFromClaim(client, statefulset.Namespace, volumespec.PersistentVolumeClaim.ClaimName)
				pvRegion, pvZone, err = verifyVolumeTopology(pv, zoneValues, regionValues)
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
				ssPodsAfterDelete := statefulsetTester.GetPodList(statefulset)
				pod = &ssPodsAfterDelete.Items[0]
				ginkgo.By("Verify Pod is scheduled in on a node belonging to same topology as the PV it is attached to")
				err = verifyPodLocation(pod, nodeList, pvZone, pvRegion)
			}
		}

		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		framework.Logf("Deleting all statefulset in namespace: %v", namespace)
		framework.DeleteAllStatefulSets(client, namespace)
	})

})
