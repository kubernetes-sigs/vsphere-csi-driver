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

	"github.com/onsi/ginkgo"
	"github.com/onsi/gomega"
	v1 "k8s.io/api/core/v1"
	storagev1 "k8s.io/api/storage/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	clientset "k8s.io/client-go/kubernetes"
	"k8s.io/kubernetes/test/e2e/framework"
	fnodes "k8s.io/kubernetes/test/e2e/framework/node"
	fpod "k8s.io/kubernetes/test/e2e/framework/pod"
	fpv "k8s.io/kubernetes/test/e2e/framework/pv"
	admissionapi "k8s.io/pod-security-admission/api"
)

var _ = ginkgo.Describe("[csi-topology-for-new-node] Topology-Provisioning-For-New-Node-With-Diff-Labels", func() {
	f := framework.NewDefaultFramework("e2e-vsphere-topology-aware-provisioning")
	f.NamespacePodSecurityEnforceLevel = admissionapi.LevelPrivileged
	var (
		client            clientset.Interface
		namespace         string
		allowedTopologies []v1.TopologySelectorLabelRequirement
		nodeList          *v1.NodeList
		pvclaim           *v1.PersistentVolumeClaim
		storageclass      *storagev1.StorageClass
		err               error
		regionZoneValue   string
	)
	ginkgo.BeforeEach(func() {
		client = f.ClientSet
		namespace = f.Namespace.Name
		bootstrap()
		nodeList, err = fnodes.GetReadySchedulableNodes(f.ClientSet)
		framework.ExpectNoError(err, "Unable to find ready and schedulable Node")
		if !(len(nodeList.Items) > 0) {
			framework.Failf("Unable to find ready and schedulable Node")
		}
	})

	/*
		In the existing setup add a node with different tag and category and
		create a SC by mentioning that tag

		Steps
		1. Create SC with the tag that is newly added on the existing node
		(Note: adding new node will be handled in jenkins job)
		2. Create PVC using the above created SC
		3. Volume provisioning should fail since NodeVM does not belong to a label
		from all the categories mentioned under `topology-categories`
	*/

	ginkgo.It("Verify volume provisioning when storage class created with different tag and category", func() {
		var cancel context.CancelFunc
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		// Read invalid tag and invalid category label from env. variable
		regionZoneValue = GetAndExpectStringEnvVar(envTopologyWithInvalidTagInvalidCat)
		_, _, allowedTopologies = topologyParameterForStorageClass(regionZoneValue)
		regionZone := strings.Split(regionZoneValue, ":")
		topologyNonExistingRegionZone := regionZone[0] + ":" + regionZone[1]

		// Create allowed topology required for creating Storage Class
		_, _, allowedTopologies = topologyParameterForStorageClass(topologyNonExistingRegionZone)

		// Create Storage Class and PVC
		storageclass, pvclaim, err = createPVCAndStorageClass(client,
			namespace, nil, nil, "", allowedTopologies, "", false, "")
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		defer func() {
			err = client.StorageV1().StorageClasses().Delete(ctx, storageclass.Name, *metav1.NewDeleteOptions(0))
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			err = fpv.DeletePersistentVolumeClaim(client, pvclaim.Name, namespace)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}()

		// Expect claim to fail provisioning volume within the topology
		ginkgo.By("Expect claim to fail provisioning volume within the topology")
		framework.ExpectError(fpv.WaitForPersistentVolumeClaimPhase(v1.ClaimBound,
			client, pvclaim.Namespace, pvclaim.Name, pollTimeoutShort, framework.PollShortTimeout))

		// Get the event list and verify if it contains expected error message
		eventList, _ := client.CoreV1().Events(pvclaim.Namespace).List(ctx, metav1.ListOptions{})
		gomega.Expect(eventList.Items).NotTo(gomega.BeEmpty())
		actualErrMsg := eventList.Items[len(eventList.Items)-1].Message
		framework.Logf(fmt.Sprintf("Actual failure message: %+q", actualErrMsg))
		expectedErrMsg := "failed to get shared datastores for topology requirement"
		framework.Logf(fmt.Sprintf("Expected failure message: %+q", expectedErrMsg))
		gomega.Expect(strings.Contains(actualErrMsg, expectedErrMsg)).To(gomega.BeTrue(),
			fmt.Sprintf("actualErrMsg: %q does not contain expectedErrMsg: %q", actualErrMsg, expectedErrMsg))
	})

	/*
		Add a new node with a different tag under a known category

		Steps
		1. To an existing set create a new node and add a new tag to a known category
		(Note: adding new node will be handled in jenkins job)
		2. Create SC which allows provisioning of volume on the specific node
		3. Create PVC using the above SC
		4. Wait for PVC and PV to bound
		5. Describe on the PV and validate node affinity details
		6. Create POD using above created PVC
		7. Write some data on to POD
		8. Delete POD, PVC,SC
	*/

	ginkgo.It("Verify volume provisioning when storage class created with different tag under a known category", func() {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		StoragePolicyName := GetAndExpectStringEnvVar(envStoragePolicyNameForNonSharedDatastores)
		scParameters := make(map[string]string)
		scParameters[scParamStoragePolicyName] = StoragePolicyName

		// Read invalid tag under a known valid category from env. variable
		regionZoneValue = GetAndExpectStringEnvVar(envTopologyWithInvalidTagValidCat)
		regionZone := strings.Split(regionZoneValue, ":")
		topologyInvalidTagValidCat := regionZone[0] + ":" + regionZone[1]

		// Get allowed topologies required for creating storage class
		regionValues, zoneValues, allowedTopologies := topologyParameterForStorageClass(topologyInvalidTagValidCat)

		// Create Storage Class and PVC
		storageclass, pvclaim, err = createPVCAndStorageClass(client,
			namespace, nil, scParameters, "", allowedTopologies, "", false, "")
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		defer func() {
			err = client.StorageV1().StorageClasses().Delete(ctx, storageclass.Name, *metav1.NewDeleteOptions(0))
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			err = fpv.DeletePersistentVolumeClaim(client, pvclaim.Name, namespace)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}()

		ginkgo.By("Expect claim to pass provisioning volume")
		err = fpv.WaitForPersistentVolumeClaimPhase(v1.ClaimBound, client,
			pvclaim.Namespace, pvclaim.Name, framework.Poll, pollTimeoutShort)
		gomega.Expect(err).NotTo(gomega.HaveOccurred(), fmt.Sprintf("Failed to provision volume with err: %v", err))

		ginkgo.By("Verify if volume is provisioned in specified region")
		pv := getPvFromClaim(client, pvclaim.Namespace, pvclaim.Name)
		pvRegion, pvZone, err = verifyVolumeTopology(pv, zoneValues, regionValues)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Creating a pod")
		pod, err := createPod(client, namespace, nil, []*v1.PersistentVolumeClaim{pvclaim}, false, execCommand)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		defer func() {
			ginkgo.By("Deleting the pod and wait for disk to detach")
			err = fpod.DeletePodWithWait(client, pod)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}()

		ginkgo.By(fmt.Sprintf("Verify volume: %s is attached to the node: %s",
			pv.Spec.CSI.VolumeHandle, pod.Spec.NodeName))
		vmUUID := getNodeUUID(ctx, client, pod.Spec.NodeName)
		isDiskAttached, err := e2eVSphere.isVolumeAttachedToVM(client, pv.Spec.CSI.VolumeHandle, vmUUID)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		gomega.Expect(isDiskAttached).To(gomega.BeTrue(), "Volume is not attached to the node")

		ginkgo.By("Verify Pod is scheduled on a node belonging to same topology as the PV it is attached to")
		nodeList, err := fnodes.GetReadySchedulableNodes(f.ClientSet)
		framework.ExpectNoError(err, "Unable to find ready and schedulable Node")
		if !(len(nodeList.Items) > 0) {
			framework.Failf("Unable to find ready and schedulable Node")
		}
		err = verifyPodLocation(pod, nodeList, pvZone, pvRegion)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
	})

})
