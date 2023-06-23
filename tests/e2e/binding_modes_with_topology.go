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
	storagev1 "k8s.io/api/storage/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	clientset "k8s.io/client-go/kubernetes"
	"k8s.io/kubernetes/test/e2e/framework"
	fnodes "k8s.io/kubernetes/test/e2e/framework/node"
	fpod "k8s.io/kubernetes/test/e2e/framework/pod"
	fpv "k8s.io/kubernetes/test/e2e/framework/pv"
	admissionapi "k8s.io/pod-security-admission/api"
)

var _ = ginkgo.Describe("[csi-topology-vanilla] Topology-Aware-Provisioning-With-Volume-Binding-Modes", func() {
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
		nodeList          *v1.NodeList
		pod               *v1.Pod
		pvclaim           *v1.PersistentVolumeClaim
		pv                *v1.PersistentVolume
		storageclass      *storagev1.StorageClass
		bindingMode       storagev1.VolumeBindingMode
		err               error
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
		bindingMode = storagev1.VolumeBindingWaitForFirstConsumer
	})

	ginkgo.AfterEach(func() {
		ginkgo.By("Performing test cleanup")
		if pvclaim != nil {
			framework.ExpectNoError(fpv.DeletePersistentVolumeClaim(client, pvclaim.Name, namespace),
				"Failed to delete PVC ", pvclaim.Name)
		}

		if pv != nil {
			framework.ExpectNoError(fpv.WaitForPersistentVolumeDeleted(client, pv.Name, poll, pollTimeoutShort))
			framework.ExpectNoError(e2eVSphere.waitForCNSVolumeToBeDeleted(pv.Spec.CSI.VolumeHandle))
		}
	})
	verifyTopologyAwareProvisioning := func(f *framework.Framework, client clientset.Interface, namespace string,
		scParameters map[string]string, allowedTopologies []v1.TopologySelectorLabelRequirement) {
		var cancel context.CancelFunc
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		storageclass, pvclaim, err = createPVCAndStorageClass(client, namespace,
			nil, nil, "", allowedTopologies, bindingMode, false, "")
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		defer func() {
			ginkgo.By("Deleting the Storage Class")
			err = client.StorageV1().StorageClasses().Delete(ctx, storageclass.Name, *metav1.NewDeleteOptions(0))
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}()
		// Wait for additional 30 seconds to make sure that provision volume claim
		// remains in pending state waiting for first consumer.
		ginkgo.By("Waiting for 30 seconds and verifying whether the PVC is still in pending state")
		time.Sleep(time.Duration(sleepTimeOut) * time.Second)

		ginkgo.By("Expect claim status to be in Pending state")
		err = fpv.WaitForPersistentVolumeClaimPhase(v1.ClaimPending, client,
			pvclaim.Namespace, pvclaim.Name, framework.Poll, framework.ClaimProvisionTimeout)
		gomega.Expect(err).NotTo(gomega.HaveOccurred(), "Failed to find the volume in pending "+
			"state with err: "+err.Error())

		ginkgo.By("Creating a pod")
		pod, err = createPod(client, namespace, nil, []*v1.PersistentVolumeClaim{pvclaim}, false, "")
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Expect claim to be in Bound state and provisioning volume passes")
		err = fpv.WaitForPersistentVolumeClaimPhase(v1.ClaimBound, client,
			pvclaim.Namespace, pvclaim.Name, framework.Poll, framework.ClaimProvisionTimeout)
		gomega.Expect(err).NotTo(gomega.HaveOccurred(), "Failed to provision volume with err: "+err.Error())

		pv = getPvFromClaim(client, pvclaim.Namespace, pvclaim.Name)
		ginkgo.By(fmt.Sprintf("Verify volume: %s is attached to the node: %s",
			pv.Spec.CSI.VolumeHandle, pod.Spec.NodeName))
		vmUUID := getNodeUUID(ctx, client, pod.Spec.NodeName)
		isDiskAttached, err := e2eVSphere.isVolumeAttachedToVM(client, pv.Spec.CSI.VolumeHandle, vmUUID)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		gomega.Expect(isDiskAttached).To(gomega.BeTrue(), "Volume is not attached to the node")

		if allowedTopologies == nil {
			// Get the topology value from pod's location to verify if it matches
			// with volume's node affinity rules.
			ginkgo.By("Verify volume is provisioned in same zone and region as that of the Pod")
			podRegion, podZone, err := getTopologyFromPod(pod, nodeList)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())

			zones := []string{podZone}
			regions := []string{podRegion}
			pvRegion, pvZone, err = verifyVolumeTopology(pv, zones, regions)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		} else {
			ginkgo.By("Verify if volume is provisioned in the selected zone and region")
			pvRegion, pvZone, err = verifyVolumeTopology(pv, zoneValues, regionValues)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())

			ginkgo.By("Verify Pod is scheduled on a node belonging to same topology as the PV it is attached to")
			err = verifyPodLocation(pod, nodeList, pvZone, pvRegion)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}

		ginkgo.By("Performing cleanup")
		ginkgo.By("Deleting the pod and wait for disk to detach")
		err = fpod.DeletePodWithWait(client, pod)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Deleting the PVC")
		err = fpv.DeletePersistentVolumeClaim(client, pvclaim.Name, namespace)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		pvclaim = nil

		ginkgo.By("Verify if PV is deleted")
		framework.ExpectNoError(fpv.WaitForPersistentVolumeDeleted(client, pv.Name, poll, pollTimeoutShort))
		pv = nil
	}

	// Test to verify provisioning with "VolumeBindingMode = WaitForFirstConsumer"
	// in Storage Class and "AllowedTopologies" not specified.
	// When AllowedTopologies is not specified Volume Create Request will have
	// all zones and regions in "AccessibilityRequirement".
	//
	// Steps
	// 1. Create a Storage Class with "VolumeBindingMode = WaitForFirstConsumer".
	// 2. Create a PVC using the above SC.
	// 3. Verify that the PVC is not in Bound phase.
	// 4. Create a Pod using the above PVC.
	// 5. Verify volume is created and contains NodeAffinity rules.
	// 6. Verify Pod is scheduled in zone and region where volume was created.
	// 7. Delete Pod and wait for disk to be detached.
	// 8. Delete PVC.
	// 9. Delete SC.
	ginkgo.It("Verify provisioning succeeds with VolumeBindingMode set to "+
		"WaitForFirstConsumer and without AllowedTopologies in the storage class ", func() {
		verifyTopologyAwareProvisioning(f, client, namespace, nil, nil)
	})

	// Test to verify provisioning with "VolumeBindingMode = WaitForFirstConsumer"
	// in Storage Class and "AllowedTopologies" also specified.
	//
	// Steps
	// 1. Create a Storage Class with "VolumeBindingMode = WaitForFirstConsumer".
	// 2. Create a PVC using the above SC.
	// 3. Verify that the PVC is not in Bound phase.
	// 4. Create a Pod using the above PVC.
	// 5. Verify volume is created and contains NodeAffinity rules.
	// 6. Verify Pod is scheduled in zone and region where volume was created.
	// 7. Delete Pod and wait for disk to be detached.
	// 8. Delete PVC.
	// 9. Delete SC.
	ginkgo.It("Verify topology aware provisioning succeeds with VolumeBindingMode set to WaitForFirstConsumer", func() {
		// Preparing allowedTopologies using topologies with shared datastores.
		regionZoneValue := GetAndExpectStringEnvVar(envRegionZoneWithSharedDS)
		regionValues, zoneValues, allowedTopologies = topologyParameterForStorageClass(regionZoneValue)
		verifyTopologyAwareProvisioning(f, client, namespace, nil, allowedTopologies)
	})

})
