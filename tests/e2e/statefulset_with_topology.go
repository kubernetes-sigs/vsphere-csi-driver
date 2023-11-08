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
	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	clientset "k8s.io/client-go/kubernetes"
	"k8s.io/kubernetes/test/e2e/framework"
	fnodes "k8s.io/kubernetes/test/e2e/framework/node"
	fss "k8s.io/kubernetes/test/e2e/framework/statefulset"
	admissionapi "k8s.io/pod-security-admission/api"
)

var _ = ginkgo.Describe("[csi-topology-vanilla] Topology-Aware-Provisioning-With-Statefulset", func() {
	f := framework.NewDefaultFramework("e2e-vsphere-topology-aware-provisioning")
	f.NamespacePodSecurityEnforceLevel = admissionapi.LevelPrivileged
	var (
		client            clientset.Interface
		namespace         string
		zoneValues        []string
		regionValues      []string
		pvZone            string
		pvRegion          string
		allowedTopologies []v1.TopologySelectorLabelRequirement
		pod               *v1.Pod
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
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		ginkgo.By("Creating StorageClass for Statefulset")
		scSpec := getVSphereStorageClassSpec(defaultNginxStorageClassName, nil, allowedTopologies, "", "", false)
		sc, err := client.StorageV1().StorageClasses().Create(ctx, scSpec, metav1.CreateOptions{})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		defer func() {
			err = client.StorageV1().StorageClasses().Delete(ctx, sc.Name, *metav1.NewDeleteOptions(0))
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}()

		ginkgo.By("Creating statefulset with single replica")
		statefulset, service := createStatefulSetWithOneReplica(client, manifestPath, namespace)
		defer func() {
			deleteService(namespace, client, service)
		}()
		fss.WaitForStatusReadyReplicas(ctx, client, statefulset, 1)
		gomega.Expect(fss.CheckMount(ctx, client, statefulset, mountPath)).NotTo(gomega.HaveOccurred())

		ssPodsBeforeDelete := fss.GetPodList(ctx, client, statefulset)
		gomega.Expect(ssPodsBeforeDelete.Items).NotTo(gomega.BeEmpty(),
			fmt.Sprintf("Unable to get list of Pods from the Statefulset: %v", statefulset.Name))
		gomega.Expect(len(ssPodsBeforeDelete.Items) == 1).To(gomega.BeTrue(),
			"Number of Pods in the statefulset should be 1")

		ginkgo.By("Deleting the pod")
		pod = &ssPodsBeforeDelete.Items[0]
		DeleteStatefulPodAtIndex(client, 0, statefulset)

		// Wait for 30 seconds, after deleting the pod. By the end of this wait,
		// the pod would be created again on any of the nodes.
		time.Sleep(time.Duration(sleepTimeOut) * time.Second)

		for _, volumespec := range pod.Spec.Volumes {
			if volumespec.PersistentVolumeClaim != nil {
				pv := getPvFromClaim(client, statefulset.Namespace, volumespec.PersistentVolumeClaim.ClaimName)
				pvRegion, pvZone, err = verifyVolumeTopology(pv, zoneValues, regionValues)
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
				ssPodsAfterDelete := fss.GetPodList(ctx, client, statefulset)
				pod = &ssPodsAfterDelete.Items[0]
				ginkgo.By("Verify Pod is scheduled in on a node belonging to same topology as the PV it is attached to")
				ctx, cancel := context.WithCancel(context.Background())
				defer cancel()
				nodeList, err := fnodes.GetReadySchedulableNodes(ctx, f.ClientSet)
				framework.ExpectNoError(err, "Unable to find ready and schedulable Node")
				if !(len(nodeList.Items) > 0) {
					framework.Failf("Unable to find ready and schedulable Node")
				}
				err = verifyPodLocation(pod, nodeList, pvZone, pvRegion)
				gomega.Expect(err).NotTo(gomega.HaveOccurred())

			}
		}
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		framework.Logf("Deleting all statefulset in namespace: %v", namespace)
		fss.DeleteAllStatefulSets(ctx, client, namespace)
	})

	/*
		Storage policy with single zone and region details in the allowed topology
		Verify POD is running on the same node as mentioned in node affinity details.
		Steps
		1. Create SC with only zone and region details specified in the SC
		2. Create statefulset with replica 3 with the above SC
		3. Wait for all the stateful set to be up and running
		4. Wait for pvc to be in bound state and statefulset pod in running state
		5. Describe pv and verify node affinity details should contain both zone and region details
		6. Verify POD is running on the same node as mentioned in node affinity details
		7. Delete statefulset
		8. Delete PVC and SC
	*/
	ginkgo.It("Storage policy with single zone and region details in the allowed topology", func() {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		// Creating StorageClass with topology details
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
		CreateStatefulSet(namespace, statefulset, client)
		replicas := *(statefulset.Spec.Replicas)
		defer func() {
			framework.Logf("Deleting all statefulset in namespace: %v", namespace)
			fss.DeleteAllStatefulSets(ctx, client, namespace)
		}()

		// Waiting for pods status to be Ready.
		fss.WaitForStatusReadyReplicas(ctx, client, statefulset, replicas)
		gomega.Expect(fss.CheckMount(ctx, client, statefulset, mountPath)).NotTo(gomega.HaveOccurred())
		ssPodsBeforeScaleDown := fss.GetPodList(ctx, client, statefulset)
		gomega.Expect(ssPodsBeforeScaleDown.Items).NotTo(gomega.BeEmpty(),
			fmt.Sprintf("Unable to get list of Pods from the Statefulset: %v", statefulset.Name))
		gomega.Expect(len(ssPodsBeforeScaleDown.Items) == int(replicas)).To(gomega.BeTrue(),
			"Number of Pods in the statefulset should match with number of replicas")

		// Verify node and pv topology affinity should contains specified zone and region details of SC
		ginkgo.By("Verify node and pv topology affinity should contains specified zone and region details of SC")
		verifyPVnodeAffinityAndPODnodedetailsForStatefulsets(ctx, client, statefulset, namespace, zoneValues, regionValues)
	})
})
