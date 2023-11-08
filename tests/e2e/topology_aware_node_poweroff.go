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
	vimtypes "github.com/vmware/govmomi/vim25/types"
	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	clientset "k8s.io/client-go/kubernetes"
	"k8s.io/kubernetes/test/e2e/framework"
	fnodes "k8s.io/kubernetes/test/e2e/framework/node"
	fpv "k8s.io/kubernetes/test/e2e/framework/pv"
	fss "k8s.io/kubernetes/test/e2e/framework/statefulset"
	admissionapi "k8s.io/pod-security-admission/api"
)

var _ = ginkgo.Describe("[csi-topology-vanilla] Topology-Aware-Provisioning-With-Power-Cycles", func() {
	f := framework.NewDefaultFramework("e2e-vsphere-topology-aware-provisioning")
	f.NamespacePodSecurityEnforceLevel = admissionapi.LevelPrivileged
	var (
		client            clientset.Interface
		namespace         string
		zoneValues        []string
		regionValues      []string
		pvZone            string
		pvRegion          string
		nodeList          *v1.NodeList
		allowedTopologies []v1.TopologySelectorLabelRequirement
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
	})

	// 1. Create a Storage Class with spec containing valid region and zone in
	//    “AllowedTopologies”. Ensure that there are multiple nodes in this zone
	//    and region.
	// 2. Create Stateful set with replica=1 attached to above PV.
	// 3. Verify PV is created in the specified zone and region.
	// 4. Verify Pod is scheduled on node within the specified zone and region.
	// 5. Power off node on which Pod is running.
	// 6. Wait for 7 minutes for k8s to detach the volume and schedule the pod
	//    on other node.
	// 7. Force delete the pod.
	// 8. Wait for 7 minutes for k8s to attach the volume on other node.
	// 9. Verify Stateful set is in running state on a node within this zone and
	//    region.
	// 10. Delete Stateful set and wait for disk to be detached.
	// 11. Delete PVC.
	// 12. Delete SC.
	ginkgo.It("Verify if stateful set is scheduled on a node within the topology after node power off", func() {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		// Preparing allowedTopologies using topologies with shared and non shared datastores
		regionValues, zoneValues, allowedTopologies = topologyParameterForStorageClass(
			GetAndExpectStringEnvVar(envRegionZoneWithSharedDS))

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

		podList := fss.GetPodList(ctx, client, statefulset)
		gomega.Expect(podList.Items).NotTo(gomega.BeEmpty(),
			fmt.Sprintf("Unable to get list of Pods from the Statefulset: %v", statefulset.Name))
		gomega.Expect(len(podList.Items) == 1).To(gomega.BeTrue(), "Number of Pods in the statefulset should be 1")

		pod := podList.Items[0]
		nodeNameToPowerOff := pod.Spec.NodeName

		var pv *v1.PersistentVolume
		for _, volumespec := range pod.Spec.Volumes {
			if volumespec.PersistentVolumeClaim != nil {
				pv = getPvFromClaim(client, statefulset.Namespace, volumespec.PersistentVolumeClaim.ClaimName)
				pvRegion, pvZone, err = verifyVolumeTopology(pv, zoneValues, regionValues)
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
			}
		}

		ginkgo.By(fmt.Sprintf("Power off the node: %v", nodeNameToPowerOff))

		vmUUID := getNodeUUID(ctx, client, nodeNameToPowerOff)
		gomega.Expect(vmUUID).NotTo(gomega.BeEmpty())
		framework.Logf("VM uuid is: %s for node: %s", vmUUID, nodeNameToPowerOff)
		vmRef, err := e2eVSphere.getVMByUUID(ctx, vmUUID)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		framework.Logf("vmRef: %v for the VM uuid: %s", vmRef, vmUUID)
		gomega.Expect(vmRef).NotTo(gomega.BeNil(), "vmRef should not be nil")
		vm := object.NewVirtualMachine(e2eVSphere.Client.Client, vmRef.Reference())
		_, err = vm.PowerOff(ctx)
		framework.ExpectNoError(err)
		defer func() {
			_, err := vm.PowerOn(ctx)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}()

		err = vm.WaitForPowerState(ctx, vimtypes.VirtualMachinePowerStatePoweredOff)
		framework.ExpectNoError(err, "Unable to power off the node")
		defer func() {
			_, err := vm.PowerOn(ctx)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}()

		ginkgo.By("Wait for 7 minutes for k8s to schedule the pod on other node")
		time.Sleep(k8sPodTerminationTimeOut)

		ginkgo.By("Forcefully deleting the pod")
		DeleteStatefulPodAtIndex(client, 0, statefulset)

		ginkgo.By("Wait for 7 minutes for k8s to detach the volume from powered off node and " +
			"start the pod successfully on other node")
		time.Sleep(k8sPodTerminationTimeOut)
		fss.WaitForRunning(ctx, client, 1, 1, statefulset)

		ginkgo.By(fmt.Sprintf("Wait until the Volume is detached the node: %v", nodeNameToPowerOff))
		isDiskDetached, err := e2eVSphere.waitForVolumeDetachedFromNode(client,
			pv.Spec.CSI.VolumeHandle, nodeNameToPowerOff)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		gomega.Expect(isDiskDetached).To(gomega.BeTrue(), "Volume is not detached from the node")

		podList = fss.GetPodList(ctx, client, statefulset)
		pod = podList.Items[0]
		failoverNode := pod.Spec.NodeName

		ginkgo.By(fmt.Sprintf("Verify volume: %s is attached to the node: %s", pv.Spec.CSI.VolumeHandle, failoverNode))
		vmUUID = getNodeUUID(ctx, client, failoverNode)
		isDiskAttached, err := e2eVSphere.isVolumeAttachedToVM(client, pv.Spec.CSI.VolumeHandle, vmUUID)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		gomega.Expect(isDiskAttached).To(gomega.BeTrue(), "Volume is not attached to the node")

		ginkgo.By("Verify Pod is scheduled on another node belonging to same topology as the PV it is attached to")
		nodeList, err := fnodes.GetReadySchedulableNodes(ctx, f.ClientSet)
		framework.ExpectNoError(err, "Unable to find ready and schedulable Node")
		if !(len(nodeList.Items) > 0) {
			framework.Failf("Unable to find ready and schedulable Node")
		}
		err = verifyPodLocation(&pod, nodeList, pvZone, pvRegion)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By(fmt.Sprintf("Power on the previous node: %v", nodeNameToPowerOff))
		_, err = vm.PowerOn(ctx)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		err = vm.WaitForPowerState(ctx, vimtypes.VirtualMachinePowerStatePoweredOn)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		framework.Logf("Deleting all statefulset in namespace: %v", namespace)
		fss.DeleteAllStatefulSets(ctx, client, namespace)
		for _, volumespec := range pod.Spec.Volumes {
			if volumespec.PersistentVolumeClaim != nil {
				ginkgo.By("Deleting the PVC")
				err = fpv.DeletePersistentVolumeClaim(ctx, client, volumespec.PersistentVolumeClaim.ClaimName, namespace)
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
			}
		}
	})

	// 1. Create a Storage Class with spec containing valid region and zone in
	//    “AllowedTopologies”. Ensure that there is only one  node in this zone
	//    and region.
	// 2. Create Stateful set with replica=1 attached to above PV.
	// 3. Verify PV is created in the specified zone and region.
	// 4. Power off node on which Pod is running.
	// 5. Wait for 7 minutes for k8s to detach the volume and schedule the pod
	//    on same node.
	// 6. Force delete the pod.
	// 7. Wait for 7 minutes for k8s to attach the volume on same node.
	// 8. Verify Stateful set is in running state on the same node.
	// 9. Delete Stateful set and wait for disk to be detached.
	// 10. Delete PVC.
	// 11. Delete SC.
	ginkgo.It("Verify if stateful set do not get scheduled on other zone "+
		"after powering off the only node in current zone", func() {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		topologyValue := GetAndExpectStringEnvVar(envTopologyWithOnlyOneNode)
		regionValues, zoneValues, allowedTopologies = topologyParameterForStorageClass(topologyValue)

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

		ginkgo.By("Creating statefulset with single replica")
		statefulset, service := createStatefulSetWithOneReplica(client, manifestPath, namespace)
		defer func() {
			deleteService(namespace, client, service)
		}()
		fss.WaitForStatusReadyReplicas(ctx, client, statefulset, 1)

		podList := fss.GetPodList(ctx, client, statefulset)
		gomega.Expect(podList.Items).NotTo(gomega.BeEmpty(),
			fmt.Sprintf("Unable to get list of Pods from the Statefulset: %v", statefulset.Name))
		gomega.Expect(len(podList.Items) == 1).To(gomega.BeTrue(), "Number of Pods in the statefulset should be 1")

		pod := podList.Items[0]
		nodeNameBeforePowerOff := pod.Spec.NodeName

		var pv *v1.PersistentVolume
		for _, volumespec := range pod.Spec.Volumes {
			if volumespec.PersistentVolumeClaim != nil {
				pv = getPvFromClaim(client, statefulset.Namespace, volumespec.PersistentVolumeClaim.ClaimName)
				pvRegion, pvZone, err = verifyVolumeTopology(pv, zoneValues, regionValues)
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
			}
		}

		ginkgo.By(fmt.Sprintf("Power off the node: %v", nodeNameBeforePowerOff))

		vmUUID := getNodeUUID(ctx, client, nodeNameBeforePowerOff)
		gomega.Expect(vmUUID).NotTo(gomega.BeEmpty())
		framework.Logf("VM uuid is: %s for node: %s", vmUUID, nodeNameBeforePowerOff)
		vmRef, err := e2eVSphere.getVMByUUID(ctx, vmUUID)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		framework.Logf("vmRef: %v for the VM uuid: %s", vmRef, vmUUID)
		gomega.Expect(vmRef).NotTo(gomega.BeNil(), "vmRef should not be nil")
		vm := object.NewVirtualMachine(e2eVSphere.Client.Client, vmRef.Reference())
		_, err = vm.PowerOff(ctx)
		framework.ExpectNoError(err)
		defer func() {
			_, err = vm.PowerOn(ctx)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}()

		err = vm.WaitForPowerState(ctx, vimtypes.VirtualMachinePowerStatePoweredOff)
		framework.ExpectNoError(err, "Unable to power off the node")

		ginkgo.By("Wait for 7 minutes for k8s to detach volume and terminate the pod on current node")
		time.Sleep(k8sPodTerminationTimeOut)

		ginkgo.By("Forcefully deleting the pod")
		DeleteStatefulPodAtIndex(client, 0, statefulset)

		ginkgo.By("Wait for 7 minutes for k8s to attempt volume attachment and start the pod")
		time.Sleep(k8sPodTerminationTimeOut)

		ginkgo.By(fmt.Sprintf("Wait until the Volume is detached the node: %v", nodeNameBeforePowerOff))
		isDiskDetached, err := e2eVSphere.waitForVolumeDetachedFromNode(client,
			pv.Spec.CSI.VolumeHandle, nodeNameBeforePowerOff)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		gomega.Expect(isDiskDetached).To(gomega.BeTrue(), "Volume is not detached from the node")

		podList = fss.GetPodList(ctx, client, statefulset)
		pod = podList.Items[0]
		nodeNameAfterPodReschedule := pod.Spec.NodeName
		ginkgo.By("Verify if the pod was not scheduled on other node")
		if nodeNameAfterPodReschedule != "" {
			gomega.Expect(nodeNameAfterPodReschedule).To(gomega.BeEmpty(),
				fmt.Sprintf("Pod was scheduled on node: %v", nodeNameAfterPodReschedule))
		}

		ginkgo.By(fmt.Sprintf("Power on the previous node: %v", nodeNameBeforePowerOff))
		_, err = vm.PowerOn(ctx)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		err = vm.WaitForPowerState(ctx, vimtypes.VirtualMachinePowerStatePoweredOn)
		framework.ExpectNoError(err, "Unable to power on the node")

		ginkgo.By("Wait for 7 minutes for k8s to attach the volume")
		time.Sleep(k8sPodTerminationTimeOut)

		fss.WaitForStatusReadyReplicas(ctx, client, statefulset, 1)
		err = verifyPodLocation(&pod, nodeList, pvZone, pvRegion)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		framework.Logf("Pod was not scheduled on any other zone")

		framework.Logf("Deleting all statefulset in namespace: %v", namespace)
		fss.DeleteAllStatefulSets(ctx, client, namespace)
		for _, volumespec := range pod.Spec.Volumes {
			if volumespec.PersistentVolumeClaim != nil {
				ginkgo.By("Deleting the PVC")
				err = fpv.DeletePersistentVolumeClaim(ctx, client, volumespec.PersistentVolumeClaim.ClaimName, namespace)
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
			}
		}
	})

})
