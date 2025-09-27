/*
Copyright 2021 The Kubernetes Authors.

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
	"os"
	"strconv"
	"time"

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
	admissionapi "k8s.io/pod-security-admission/api"
)

var _ bool = ginkgo.Describe("[csi-block-vanilla] [csi-file-vanilla] "+
	"CNS-CSI Cluster Distribution Operations during VC reboot", func() {
	f := framework.NewDefaultFramework("csi-cns-telemetry")
	f.NamespacePodSecurityEnforceLevel = admissionapi.LevelPrivileged
	var (
		client       clientset.Interface
		namespace    string
		isVcRebooted bool
	)
	ginkgo.BeforeEach(func() {
		client = f.ClientSet
		namespace = getNamespaceToRunTests(f)
		bootstrap()
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		nodeList, err := fnodes.GetReadySchedulableNodes(ctx, f.ClientSet)
		framework.ExpectNoError(err, "Unable to find ready and schedulable Node")
		if !(len(nodeList.Items) > 0) {
			framework.Failf("Unable to find ready and schedulable Node")
		}

		if vanillaCluster {
			// Reset the cluster distribution value to default value "CSI-Vanilla".
			setClusterDistribution(ctx, client, vanillaClusterDistribution)
		}
	})

	ginkgo.AfterEach(func() {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		if vanillaCluster {
			// Reset the cluster distribution value to default value "CSI-Vanilla".
			setClusterDistribution(ctx, client, vanillaClusterDistribution)
		}

		// restarting pending and stopped services after vc reboot if any
		if isVcRebooted {
			err := checkVcServicesHealthPostReboot(ctx, vcAddress, pollTimeout)
			gomega.Expect(err).NotTo(gomega.HaveOccurred(),
				"Setup is not in healthy state, Got timed-out waiting for required VC services to be up and running")
		}
	})

	// Steps
	// 1. Create StorageClass.
	// 2. Create PVC which uses the StorageClass created in step 1.
	// 3. Wait for PV to be provisioned.
	// 4. Wait for PVC's status to become bound.
	// 5. Create pod using PVC.
	// 6. Wait for Disk to be attached to the node.
	// 7. Reboot VC.
	// 8. Update cluster-distribution value and create PVC while VC reboots.
	// 9. Wait for the VC to reboot completely and PVC to be bound.
	// 10. Create pod using the PVC.
	// 12. Wait for Disk to be attached to the node.
	// 13. Delete pod and Wait for Volume Disk to be detached from the Node.
	// 14. Delete PVC, PV and Storage Class.

	ginkgo.It("[csi-block-vanilla][csi-file-vanilla][csi-block-vanilla-serialized][pq-vanilla-file][pq-vanilla-block]"+
		"verify volume operations while vc reboot", ginkgo.Label(p1, block, file, vanilla, disruptive,
		negative, vc70), func() {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		var storageclass *storagev1.StorageClass
		var pvclaim *v1.PersistentVolumeClaim
		var storageclass2 *storagev1.StorageClass
		var pvclaim2 *v1.PersistentVolumeClaim
		var fullSyncWaitTime int
		var err error

		// Read full-sync value.
		if os.Getenv(envFullSyncWaitTime) != "" {
			fullSyncWaitTime, err = strconv.Atoi(os.Getenv(envFullSyncWaitTime))
			framework.Logf("Full-Sync interval time value is = %v", fullSyncWaitTime)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}

		ginkgo.By("Creating Storage Class and PVC")
		// Decide which test setup is available to run.
		if vanillaCluster {
			ginkgo.By("CNS_TEST: Running for Vanilla setup")
			storageclass, pvclaim, err = createPVCAndStorageClass(ctx, client, namespace, nil, nil, "", nil, "", false, "")
		}
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		defer func() {
			err := client.StorageV1().StorageClasses().Delete(ctx, storageclass.Name, *metav1.NewDeleteOptions(0))
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}()

		defer func() {
			err = fpv.DeletePersistentVolumeClaim(ctx, client, pvclaim.Name, namespace)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}()

		ginkgo.By("Waiting for claim to be in bound phase")
		pvc, err := fpv.WaitForPVClaimBoundPhase(ctx, client,
			[]*v1.PersistentVolumeClaim{pvclaim}, 2*framework.ClaimProvisionTimeout)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		gomega.Expect(pvc).NotTo(gomega.BeEmpty())
		pv := getPvFromClaim(client, pvclaim.Namespace, pvclaim.Name)
		volumeID := pv.Spec.CSI.VolumeHandle

		ginkgo.By("Creating pod to attach PV to the node")
		pod, err := createPod(ctx, client, namespace, nil, []*v1.PersistentVolumeClaim{pvclaim}, false, "")
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		var vmUUID string
		var vmUUID2 string
		nodeName := pod.Spec.NodeName

		if vanillaCluster {
			vmUUID = getNodeUUID(ctx, client, pod.Spec.NodeName)
		}

		ginkgo.By(fmt.Sprintf("Verify volume: %s is attached to the node: %s", volumeID, nodeName))
		isDiskAttached, err := e2eVSphere.isVolumeAttachedToVM(client, volumeID, vmUUID)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		gomega.Expect(isDiskAttached).To(gomega.BeTrue(), "Volume is not attached to the node")

		ginkgo.By("Rebooting VC")
		err = invokeVCenterReboot(ctx, vcAddress)
		isVcRebooted = true
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		err = waitForHostToBeUp(vcAddress)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		ginkgo.By("Done with reboot")

		// Sleep for a short while for the VC to shutdown, before invoking PVC
		// creation and cluster-distribution value.
		time.Sleep(pollTimeoutShort)

		ginkgo.By("Set cluster-distribution value while the VC reboot in progress")

		if vanillaCluster {
			// Set Cluster-distribution while the VC reboot in progress.
			setClusterDistribution(ctx, client, vanillaClusterDistributionWithSpecialChar)
		}

		defer func() {
			if vanillaCluster {
				// Reset the cluster distribution value to default value "CSI-Vanilla".
				setClusterDistribution(ctx, client, vanillaClusterDistribution)
			}
		}()

		// Decide which test setup is available to run.
		if vanillaCluster {
			ginkgo.By("Creating another PVC for Vanilla setup")
			storageclass2, pvclaim2, err = createPVCAndStorageClass(ctx, client, namespace, nil, nil, "", nil, "", false, "")
		}
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		defer func() {
			err := client.StorageV1().StorageClasses().Delete(ctx, storageclass2.Name, *metav1.NewDeleteOptions(0))
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}()

		defer func() {
			err = fpv.DeletePersistentVolumeClaim(ctx, client, pvclaim2.Name, namespace)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}()

		err = waitForHostToBeUp(vcAddress)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		ginkgo.By("Done with reboot")
		essentialServices := []string{spsServiceName, vsanhealthServiceName, vpxdServiceName}
		checkVcenterServicesRunning(ctx, vcAddress, essentialServices)

		// After reboot.
		bootstrap()

		ginkgo.By("Waiting for PVC2 claim to be in bound phase")
		pvc2, err := fpv.WaitForPVClaimBoundPhase(ctx, client,
			[]*v1.PersistentVolumeClaim{pvclaim2}, 2*framework.ClaimProvisionTimeout)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		gomega.Expect(pvc2).NotTo(gomega.BeEmpty())
		pv2 := getPvFromClaim(client, pvclaim2.Namespace, pvclaim2.Name)
		volumeID2 := pv2.Spec.CSI.VolumeHandle

		ginkgo.By("Creating pod to attach PV2 to the node")
		pod2, err := createPod(ctx, client, namespace, nil, []*v1.PersistentVolumeClaim{pvclaim2}, false, "")
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		nodeName2 := pod2.Spec.NodeName

		if vanillaCluster {
			vmUUID2 = getNodeUUID(ctx, client, pod2.Spec.NodeName)
		}

		ginkgo.By(fmt.Sprintf("Verify volume: %s is attached to the node: %s", volumeID2, nodeName2))
		isDiskAttached2, err := e2eVSphere.isVolumeAttachedToVM(client, volumeID2, vmUUID2)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		gomega.Expect(isDiskAttached2).To(gomega.BeTrue(), "Volume is not attached to the node")

		// Verify the attached volume has cluster-distribution value set.
		ginkgo.By(fmt.Sprintf("Invoking QueryCNSVolumeWithResult for PVC with VolumeID: %s", pv2.Spec.CSI.VolumeHandle))
		queryResult, err := e2eVSphere.queryCNSVolumeWithResult(pv2.Spec.CSI.VolumeHandle)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		gomega.Expect(len(queryResult.Volumes) > 0).To(gomega.BeTrue())

		framework.Logf("Cluster-distribution value on CNS is %s",
			queryResult.Volumes[0].Metadata.ContainerClusterArray[0].ClusterDistribution)
		gomega.Expect(queryResult.Volumes[0].Metadata.ContainerClusterArray[0].ClusterDistribution).Should(
			gomega.Equal(vanillaClusterDistributionWithSpecialChar), "Wrong/empty cluster-distribution name present on CNS")

		ginkgo.By(fmt.Sprintf("Deleting the pod %s in namespace %s", pod.Name, namespace))
		err = fpod.DeletePodWithWait(ctx, client, pod)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By(fmt.Sprintf("Deleting the pod %s in namespace %s", pod2.Name, namespace))
		err = fpod.DeletePodWithWait(ctx, client, pod2)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		if vanillaCluster {
			ginkgo.By("Verify volume is detached from the node")
			isDiskDetached, err := e2eVSphere.waitForVolumeDetachedFromNode(client,
				pv.Spec.CSI.VolumeHandle, pod.Spec.NodeName)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			gomega.Expect(isDiskDetached).To(gomega.BeTrue(),
				fmt.Sprintf("Volume %q is not detached from the node %q", pv.Spec.CSI.VolumeHandle, pod.Spec.NodeName))

			ginkgo.By("Verify volume is detached from the node")
			isDiskDetached2, err := e2eVSphere.waitForVolumeDetachedFromNode(client,
				pv2.Spec.CSI.VolumeHandle, pod2.Spec.NodeName)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			gomega.Expect(isDiskDetached2).To(gomega.BeTrue(),
				fmt.Sprintf("Volume %q is not detached from the node %q", pv2.Spec.CSI.VolumeHandle, pod2.Spec.NodeName))

		}
	})

	// Steps
	// 1. Create StorageClass.
	// 2. Create PVC which uses the StorageClass created in step 1.
	// 3. Wait for PV to be provisioned.
	// 4. Wait for PVC's status to become bound.
	// 5. Create pod using PVC.
	// 6. Wait for Disk to be attached to the node.
	// 7. Reboot VC.
	// 8. Update cluster-distribution value and create PVC after VC reboots.
	// 9. Wait for PVC to be bound.
	// 10. Create pod using the PVC.
	// 12. Wait for Disk to be attached to the node.
	// 13. Delete pod and Wait for Volume Disk to be detached from the Node.
	// 14. Delete PVC, PV and Storage Class.
	ginkgo.It("[csi-block-vanilla][csi-file-vanilla][csi-block-vanilla-serialized][pq-vanilla-file][pq-vanilla-block]"+
		"verify volume operations after vc reboots", ginkgo.Label(p1, block, file, vanilla, disruptive,
		negative, vc70), func() {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		var storageclass *storagev1.StorageClass
		var pvclaim *v1.PersistentVolumeClaim
		var fullSyncWaitTime int
		var err error

		// Read full-sync value.
		if os.Getenv(envFullSyncWaitTime) != "" {
			fullSyncWaitTime, err = strconv.Atoi(os.Getenv(envFullSyncWaitTime))
			framework.Logf("Full-Sync interval time value is = %v", fullSyncWaitTime)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}

		ginkgo.By("Rebooting VC")
		err = invokeVCenterReboot(ctx, vcAddress)
		isVcRebooted = true
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		err = waitForHostToBeUp(vcAddress)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		ginkgo.By("Done with reboot")
		essentialServices := []string{spsServiceName, vsanhealthServiceName, vpxdServiceName}
		checkVcenterServicesRunning(ctx, vcAddress, essentialServices)

		// After reboot.
		bootstrap()

		// Sleep for a short while for the VC to shutdown, before invoking PVC
		// creation and cluster-distribution value.
		time.Sleep(pollTimeoutShort)

		ginkgo.By("Set cluster-distribution value after the VC reboot in progress")
		// Set Cluster-distribution while the VC reboot in progress.
		setClusterDistribution(ctx, client, vanillaClusterDistributionWithSpecialChar)

		defer func() {
			// Reset the cluster distribution value to default value "CSI-Vanilla".
			setClusterDistribution(ctx, client, vanillaClusterDistribution)
		}()

		ginkgo.By("Creating Storage Class and PVC")
		// Decide which test setup is available to run.
		if vanillaCluster {
			ginkgo.By("CNS_TEST: Running for Vanilla setup")
			storageclass, pvclaim, err = createPVCAndStorageClass(ctx, client, namespace, nil, nil, "", nil, "", false, "")
		}
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		defer func() {
			err := client.StorageV1().StorageClasses().Delete(ctx, storageclass.Name, *metav1.NewDeleteOptions(0))
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}()

		ginkgo.By("Waiting for claim to be in bound phase")
		pvc, err := fpv.WaitForPVClaimBoundPhase(ctx, client,
			[]*v1.PersistentVolumeClaim{pvclaim}, 2*framework.ClaimProvisionTimeout)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		gomega.Expect(pvc).NotTo(gomega.BeEmpty())
		pv := getPvFromClaim(client, pvclaim.Namespace, pvclaim.Name)
		volumeID := pv.Spec.CSI.VolumeHandle

		defer func() {
			err = fpv.DeletePersistentVolumeClaim(ctx, client, pvclaim.Name, namespace)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}()

		ginkgo.By("Creating pod to attach PV to the node")
		pod, err := createPod(ctx, client, namespace, nil, []*v1.PersistentVolumeClaim{pvclaim}, false, "")
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		var vmUUID string

		nodeName := pod.Spec.NodeName

		if vanillaCluster {
			vmUUID = getNodeUUID(ctx, client, pod.Spec.NodeName)
		}

		ginkgo.By(fmt.Sprintf("Verify volume: %s is attached to the node: %s", volumeID, nodeName))
		isDiskAttached, err := e2eVSphere.isVolumeAttachedToVM(client, volumeID, vmUUID)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		gomega.Expect(isDiskAttached).To(gomega.BeTrue(), "Volume is not attached to the node")

		// Verify the attached volume has cluster-distribution value set.
		ginkgo.By(fmt.Sprintf("Invoking QueryCNSVolumeWithResult for PVC with VolumeID: %s", pv.Spec.CSI.VolumeHandle))
		queryResult, err := e2eVSphere.queryCNSVolumeWithResult(pv.Spec.CSI.VolumeHandle)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		gomega.Expect(len(queryResult.Volumes) > 0).To(gomega.BeTrue())

		framework.Logf("Cluster-distribution value on CNS is %s",
			queryResult.Volumes[0].Metadata.ContainerClusterArray[0].ClusterDistribution)
		gomega.Expect(queryResult.Volumes[0].Metadata.ContainerClusterArray[0].ClusterDistribution).Should(
			gomega.Equal(vanillaClusterDistributionWithSpecialChar), "Wrong/empty cluster-distribution name present on CNS")

		ginkgo.By(fmt.Sprintf("Deleting the pod %s in namespace %s", pod.Name, namespace))
		err = fpod.DeletePodWithWait(ctx, client, pod)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		if vanillaCluster {
			ginkgo.By("Verify volume is detached from the node")
			isDiskDetached, err := e2eVSphere.waitForVolumeDetachedFromNode(client,
				pv.Spec.CSI.VolumeHandle, pod.Spec.NodeName)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			gomega.Expect(isDiskDetached).To(gomega.BeTrue(),
				fmt.Sprintf("Volume %q is not detached from the node %q", pv.Spec.CSI.VolumeHandle, pod.Spec.NodeName))
		}
	})
})
