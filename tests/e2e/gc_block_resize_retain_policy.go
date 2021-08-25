/*
Copyright 2020 The Kubernetes Authors.

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
	"strings"

	"github.com/onsi/ginkgo"
	"github.com/onsi/gomega"
	cnstypes "github.com/vmware/govmomi/cns/types"
	v1 "k8s.io/api/core/v1"
	storagev1 "k8s.io/api/storage/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/wait"
	clientset "k8s.io/client-go/kubernetes"
	"k8s.io/kubernetes/test/e2e/framework"
	fnodes "k8s.io/kubernetes/test/e2e/framework/node"
	fpod "k8s.io/kubernetes/test/e2e/framework/pod"
	fpv "k8s.io/kubernetes/test/e2e/framework/pv"
	k8s "sigs.k8s.io/vsphere-csi-driver/v2/pkg/kubernetes"
)

var _ = ginkgo.Describe("[csi-guest] Volume Expansion Tests with reclaimation policy retain", func() {
	f := framework.NewDefaultFramework("gc-resize-reclaim-policy-retain")
	var (
		client            clientset.Interface
		clientNewGc       clientset.Interface
		namespace         string
		namespaceNewGC    string
		storagePolicyName string
		storageclass      *storagev1.StorageClass
		pvclaim           *v1.PersistentVolumeClaim
		err               error
		volHandle         string
		svcPVCName        string
		pv                *v1.PersistentVolume
		pvcDeleted        bool
		pvcDeletedInSvc   bool
		pvDeleted         bool
		cmd               []string
		cmd2              []string
	)

	ginkgo.BeforeEach(func() {
		client = f.ClientSet
		namespace = f.Namespace.Name

		bootstrap()
		ginkgo.By("Getting ready nodes on GC 1")
		nodeList, err := fnodes.GetReadySchedulableNodes(f.ClientSet)
		framework.ExpectNoError(err, "Unable to find ready and schedulable Node")
		if !(len(nodeList.Items) > 0) {
			framework.Failf("Unable to find ready and schedulable Node")
		}
		storagePolicyName = GetAndExpectStringEnvVar(envStoragePolicyNameForSharedDatastores)

		scParameters := make(map[string]string)
		scParameters[scParamFsType] = ext4FSType
		// Set resource quota.
		ginkgo.By("Set Resource quota for GC")
		svcClient, svNamespace := getSvcClientAndNamespace()
		setResourceQuota(svcClient, svNamespace, rqLimit)
		// Create Storage class and PVC.
		ginkgo.By("Creating Storage Class and PVC with allowVolumeExpansion = true")

		scParameters[svStorageClassName] = storagePolicyName
		storageclass, err = createStorageClass(client, scParameters, nil, v1.PersistentVolumeReclaimRetain, "", true, "")
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		pvclaim, err = createPVC(client, namespace, nil, "", storageclass, "")
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		// Waiting for PVC to be bound.
		var pvclaims []*v1.PersistentVolumeClaim
		pvclaims = append(pvclaims, pvclaim)
		ginkgo.By("Waiting for all claims to be in bound state")
		persistentvolumes, err := fpv.WaitForPVClaimBoundPhase(client, pvclaims, framework.ClaimProvisionTimeout)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		pv = persistentvolumes[0]
		volHandle = getVolumeIDFromSupervisorCluster(pv.Spec.CSI.VolumeHandle)
		gomega.Expect(volHandle).NotTo(gomega.BeEmpty())
		svcPVCName = pv.Spec.CSI.VolumeHandle

		pvcDeleted = false
		pvcDeletedInSvc = false
		pvDeleted = false

		// Replace second element with pod.Name.
		cmd = []string{"exec", "", fmt.Sprintf("--namespace=%v", namespace),
			"--", "/bin/sh", "-c", "df -Tkm | grep /mnt/volume1"}
	})

	ginkgo.AfterEach(func() {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		if !pvcDeleted {
			err = client.CoreV1().PersistentVolumeClaims(namespace).Delete(ctx, pvclaim.Name, *metav1.NewDeleteOptions(0))
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}
		if !pvDeleted {
			err = client.CoreV1().PersistentVolumes().Delete(ctx, pv.Name, *metav1.NewDeleteOptions(0))
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}
		if !pvcDeletedInSvc {
			svcClient, svcNamespace := getSvcClientAndNamespace()
			err := svcClient.CoreV1().PersistentVolumeClaims(svcNamespace).Delete(ctx,
				svcPVCName, *metav1.NewDeleteOptions(0))
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}
		err = e2eVSphere.waitForCNSVolumeToBeDeleted(pv.Spec.CSI.VolumeHandle)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		ginkgo.By("Verify volume is deleted in Supervisor Cluster")
		volumeExists := verifyVolumeExistInSupervisorCluster(svcPVCName)
		gomega.Expect(volumeExists).To(gomega.BeFalse())
		err = client.StorageV1().StorageClasses().Delete(ctx, storageclass.Name, *metav1.NewDeleteOptions(0))
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		svcClient, svNamespace := getSvcClientAndNamespace()
		setResourceQuota(svcClient, svNamespace, defaultrqLimit)
	})

	// Combined:
	// PV with reclaim policy retain can be resized using new GC PVC.
	// PV with reclaim policy can be resized using new GC PVC with pod.
	// Steps:
	// 1. Create a SC with allowVolumeExpansion set to 'true' and with reclaim
	//    policy set to 'Retain'.
	// 2. create a GC PVC using the SC created in step 1 and wait for binding
	//    with PV.
	// 3. Create a pod  in GC to use PVC created in step 2 and file system init.
	// 4. Delete GC pod created in step 3.
	// 5. Delete GC PVC created in step 2.
	// 6. Verify GC PVC is removed but SVC PVC, PV and GC PV still exists.
	// 7. Remove claimRef from the PV lingering in GC  to get it to Available
	//    state.
	// 8. Create new PVC in GC using the PV lingering in GC using the same SC
	//    from step 1.
	// 9. Verify same SVC PVC is reused.
	// 10. Resize PVC in GC.
	// 11. Wait for PVC in GC to reach "FilesystemResizePending" state.
	// 12. Check using CNS query that size has got updated to what was used in
	//     step 8.
	// 13. Verify size of PV in SVC and GC to same as the one used in the step 8.
	// 14. Create a pod in GC to use PVC create in step 6.
	// 15. Wait for FS resize.
	// 16. Verify size of PVC SVC and GC are equal and bigger than what it was
	//     after step 3.
	// 17. Delete pod created in step 12.
	// 18. Delete PVC created in step 6.
	// 19. Delete PV leftover in GC.
	// 20. Delete SC created in step 1.
	ginkgo.It("PV with reclaim policy can be reused and resized with pod", func() {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		// Create a Pod to use this PVC, and verify volume has been attached.
		ginkgo.By("Creating pod to attach PV to the node")
		pod, err := createPod(client, namespace, nil, []*v1.PersistentVolumeClaim{pvclaim}, false, execCommand)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By(fmt.Sprintf("Verify volume: %s is attached to the node: %s",
			pv.Spec.CSI.VolumeHandle, pod.Spec.NodeName))
		vmUUID, err := getVMUUIDFromNodeName(pod.Spec.NodeName)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		isDiskAttached, err := e2eVSphere.isVolumeAttachedToVM(client, volHandle, vmUUID)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		gomega.Expect(isDiskAttached).To(gomega.BeTrue(), "Volume is not attached to the node")

		ginkgo.By("Verify the volume is accessible and filesystem type is as expected")
		cmd[1] = pod.Name
		lastOutput := framework.RunKubectlOrDie(namespace, cmd...)
		gomega.Expect(strings.Contains(lastOutput, ext4FSType)).NotTo(gomega.BeFalse())

		ginkgo.By("Check filesystem size for mount point /mnt/volume1 before expansion")
		originalFsSize, err := getFSSizeMb(f, pod)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		// Delete POD.
		ginkgo.By(fmt.Sprintf("Deleting the pod %s in namespace %s", pod.Name, namespace))
		err = fpod.DeletePodWithWait(client, pod)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Verify volume is detached from the node")
		isDiskDetached, err := e2eVSphere.waitForVolumeDetachedFromNode(client,
			pv.Spec.CSI.VolumeHandle, pod.Spec.NodeName)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		gomega.Expect(isDiskDetached).To(gomega.BeTrue(),
			fmt.Sprintf("Volume %q is not detached from the node %q", pv.Spec.CSI.VolumeHandle, pod.Spec.NodeName))

		ginkgo.By("Delete PVC in GC")
		err = client.CoreV1().PersistentVolumeClaims(namespace).Delete(ctx, pvclaim.Name, *metav1.NewDeleteOptions(0))
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		pvcDeleted = true

		ginkgo.By("Check GC PV exists and is released")
		pv, err = waitForPvToBeReleased(ctx, client, pv.Name)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		oldPvUID := string(pv.UID)
		fmt.Println("PV uuid", oldPvUID)

		ginkgo.By("Check SVC PVC exists")
		_ = getPVCFromSupervisorCluster(svcPVCName)

		ginkgo.By("Remove claimRef from GC PVC")
		pv.Spec.ClaimRef = nil
		pv, err = client.CoreV1().PersistentVolumes().Update(ctx, pv, metav1.UpdateOptions{})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Creating the PVC in guest cluster")
		pvclaim = getPersistentVolumeClaimSpec(namespace, nil, pv.Name)
		pvclaim.Spec.StorageClassName = &storageclass.Name
		pvclaim, err = client.CoreV1().PersistentVolumeClaims(namespace).Create(ctx, pvclaim, metav1.CreateOptions{})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		defer func() {
			err = client.CoreV1().PersistentVolumeClaims(namespace).Delete(ctx, pvclaim.Name, *metav1.NewDeleteOptions(0))
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}()

		ginkgo.By("Wait for the PVC in guest cluster to bind the lingering pv")
		framework.ExpectNoError(fpv.WaitOnPVandPVC(client, framework.NewTimeoutContextWithDefaults(),
			namespace, pv, pvclaim))

		// Modify PVC spec to trigger volume expansion.
		// We expand the PVC while no pod is using it to ensure offline expansion.
		ginkgo.By("Expanding current pvc")
		currentPvcSize := pvclaim.Spec.Resources.Requests[v1.ResourceStorage]
		newSize := currentPvcSize.DeepCopy()
		newSize.Add(resource.MustParse("1Gi"))
		framework.Logf("currentPvcSize %v, newSize %v", currentPvcSize, newSize)
		pvclaim, err = expandPVCSize(pvclaim, newSize, client)
		framework.ExpectNoError(err, "While updating pvc for more size")
		gomega.Expect(pvclaim).NotTo(gomega.BeNil())

		pvcSize := pvclaim.Spec.Resources.Requests[v1.ResourceStorage]
		if pvcSize.Cmp(newSize) != 0 {
			framework.Failf("error updating pvc size %q", pvclaim.Name)
		}
		ginkgo.By("Checking for PVC request size change on SVC PVC")
		b, err := verifyPvcRequestedSizeUpdateInSupervisorWithWait(svcPVCName, newSize)
		gomega.Expect(b).To(gomega.BeTrue())
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Waiting for controller volume resize to finish")
		err = waitForPvResizeForGivenPvc(pvclaim, client, totalResizeWaitPeriod)
		framework.ExpectNoError(err, "While waiting for pvc resize to finish")

		ginkgo.By("Checking for resize on SVC PV")
		verifyPVSizeinSupervisor(svcPVCName, newSize)

		ginkgo.By("Checking for conditions on pvc")
		pvclaim, err = waitForPVCToReachFileSystemResizePendingCondition(client, namespace, pvclaim.Name, pollTimeout)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Checking for 'FileSystemResizePending' status condition on SVC PVC")
		_, err = checkSvcPvcHasGivenStatusCondition(svcPVCName, true, v1.PersistentVolumeClaimFileSystemResizePending)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By(fmt.Sprintf("Invoking QueryCNSVolumeWithResult with VolumeID: %s", volHandle))
		queryResult, err := e2eVSphere.queryCNSVolumeWithResult(volHandle)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		if len(queryResult.Volumes) == 0 {
			err = fmt.Errorf("QueryCNSVolumeWithResult returned no volume")
		}
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		ginkgo.By("Verifying disk size requested in volume expansion is honored")
		newSizeInMb := convertGiStrToMibInt64(newSize)
		if queryResult.Volumes[0].BackingObjectDetails.(*cnstypes.CnsBlockBackingDetails).CapacityInMb != newSizeInMb {
			err = fmt.Errorf("got wrong disk size after volume expansion")
		}
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		// Create a new Pod to use this PVC, and verify volume has been attached.
		ginkgo.By("Creating a new pod to attach PV again to the node")
		pod, err = createPod(client, namespace, nil, []*v1.PersistentVolumeClaim{pvclaim}, false, execCommand)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By(fmt.Sprintf("Verify volume after expansion: %s is attached to the node: %s",
			pv.Spec.CSI.VolumeHandle, pod.Spec.NodeName))
		vmUUID, err = getVMUUIDFromNodeName(pod.Spec.NodeName)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		isDiskAttached, err = e2eVSphere.isVolumeAttachedToVM(client, volHandle, vmUUID)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		gomega.Expect(isDiskAttached).To(gomega.BeTrue(), "Volume is not attached to the node")

		ginkgo.By("Verify after expansion the filesystem type is as expected")
		cmd[1] = pod.Name
		lastOutput = framework.RunKubectlOrDie(namespace, cmd...)
		gomega.Expect(strings.Contains(lastOutput, ext4FSType)).NotTo(gomega.BeFalse())

		ginkgo.By("Waiting for file system resize to finish")
		pvclaim, err = waitForFSResize(pvclaim, client)
		framework.ExpectNoError(err, "while waiting for fs resize to finish")

		pvcConditions := pvclaim.Status.Conditions
		expectEqual(len(pvcConditions), 0, "pvc should not have conditions")

		ginkgo.By("Verify filesystem size for mount point /mnt/volume1 after expansion")
		fsSize, err := getFSSizeMb(f, pod)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		// Filesystem size may be smaller than the size of the block volume.
		// Here since filesystem was already formatted on the original volume,
		// we can compare the new filesystem size with the original filesystem size.
		if fsSize <= originalFsSize {
			framework.Failf("error updating filesystem size for %q. Resulting filesystem size is %d", pvclaim.Name, fsSize)
		}
		ginkgo.By("File system resize finished successfully in GC")
		ginkgo.By("Checking for PVC resize completion on SVC PVC")
		gomega.Expect(verifyResizeCompletedInSupervisor(svcPVCName)).To(gomega.BeTrue())

		// Delete POD.
		ginkgo.By(fmt.Sprintf("Deleting the new pod %s in namespace %s after expansion", pod.Name, namespace))
		err = fpod.DeletePodWithWait(client, pod)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Verify volume is detached from the node after expansion")
		isDiskDetached, err = e2eVSphere.waitForVolumeDetachedFromNode(client,
			pv.Spec.CSI.VolumeHandle, pod.Spec.NodeName)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		gomega.Expect(isDiskDetached).To(gomega.BeTrue(),
			fmt.Sprintf("Volume %q is not detached from the node %q", pv.Spec.CSI.VolumeHandle, pod.Spec.NodeName))

	})

	// PV with reclaim policy retain can be resized when used in a fresh GC.
	// Steps:
	// 1. Create a SC with allowVolumeExpansion set to 'true' in GC1.
	// 2. create a GC1 PVC using the SC created in step 1 and wait for binding
	//    with PV with reclaim policy set to 'Retain'.
	// 3. Delete GC1 PVC.
	// 4. verify GC1 PVC is removed but SVC PV, PVC and GC1 PV still exist.
	// 5. delete GC1 PV.  SVC PV, PVC still exist.
	// 6. Create a new GC GC2.
	// 7. create SC in GC1 similar to the SC created in step 1 but with reclaim
	//    policy set to delete.
	// 8. Create new PV in GC2 using the SVC PVC from step 5 and SC created in
	//    step 7.
	// 9. create new  PVC in GC2 using PV created in step 8.
	// 10. verify a new PVC API object is created.
	// 11. Resize PVC from step 9 in GC2.
	// 12. Wait for PVC in GC2 and SVC to reach "FilesystemResizePending" state.
	// 13. Check using CNS query that size has got updated to what was used in
	//     step 11.
	// 14. Verify size of PVs in SVC and GC to same as the one used in the step 11.
	// 15. delete PVC created in step 9.
	// 16. delete SC created in step 1 and step 7.
	// 17. delete GC2.
	// Steps 6 and 17 need to run manually before and after this suite.
	ginkgo.It("PV with reclaim policy retain can be resized when used in a fresh GC", func() {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		newGcKubconfigPath := os.Getenv("NEW_GUEST_CLUSTER_KUBE_CONFIG")
		if newGcKubconfigPath == "" {
			ginkgo.Skip("Env NEW_GUEST_CLUSTER_KUBE_CONFIG is missing")
		}
		clientNewGc, err = k8s.CreateKubernetesClientFromConfig(newGcKubconfigPath)
		gomega.Expect(err).NotTo(gomega.HaveOccurred(),
			fmt.Sprintf("Error creating k8s client with %v: %v", newGcKubconfigPath, err))
		ginkgo.By("Creating namespace on second GC")
		ns, err := framework.CreateTestingNS(f.BaseName, clientNewGc, map[string]string{
			"e2e-framework": f.BaseName,
		})
		gomega.Expect(err).NotTo(gomega.HaveOccurred(), "Error creating namespace on second GC")

		namespaceNewGC = ns.Name
		framework.Logf("Created namespace on second GC %v", namespaceNewGC)
		defer func() {
			err := clientNewGc.CoreV1().Namespaces().Delete(ctx, namespaceNewGC, *metav1.NewDeleteOptions(0))
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}()

		ginkgo.By("Getting ready nodes on GC 2")
		nodeList, err := fnodes.GetReadySchedulableNodes(clientNewGc)
		framework.ExpectNoError(err, "Unable to find ready and schedulable Node")
		gomega.Expect(len(nodeList.Items)).NotTo(gomega.BeZero(), "Unable to find ready and schedulable Node")

		ginkgo.By("Delete PVC and PV form orignal GC")
		err = client.CoreV1().PersistentVolumeClaims(namespace).Delete(ctx, pvclaim.Name, *metav1.NewDeleteOptions(0))
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		pvcDeleted = true
		err = client.CoreV1().PersistentVolumes().Delete(ctx, pv.Name, *metav1.NewDeleteOptions(0))
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		pvDeleted = true

		ginkgo.By("Check SVC PVC still exists")
		_ = getPVCFromSupervisorCluster(svcPVCName)

		scParameters := make(map[string]string)
		scParameters[scParamFsType] = ext4FSType
		scParameters[svStorageClassName] = storagePolicyName
		storageclassNewGC, err := createStorageClass(clientNewGc,
			scParameters, nil, v1.PersistentVolumeReclaimDelete, "", true, "")
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		pvc, err := createPVC(clientNewGc, namespaceNewGC, nil, "", storageclassNewGC, "")
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		var pvcs []*v1.PersistentVolumeClaim
		pvcs = append(pvcs, pvc)
		ginkgo.By("Waiting for all claims to be in bound state")
		pvs, err := fpv.WaitForPVClaimBoundPhase(clientNewGc, pvcs, framework.ClaimProvisionTimeout)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		pvtemp := pvs[0]

		defer func() {
			err = clientNewGc.CoreV1().PersistentVolumeClaims(namespaceNewGC).Delete(ctx,
				pvc.Name, *metav1.NewDeleteOptions(0))
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			err = clientNewGc.StorageV1().StorageClasses().Delete(ctx,
				storageclassNewGC.Name, *metav1.NewDeleteOptions(0))
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			err = e2eVSphere.waitForCNSVolumeToBeDeleted(pvtemp.Spec.CSI.VolumeHandle)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			ginkgo.By("Verify volume is deleted in Supervisor Cluster")
			volumeExists := verifyVolumeExistInSupervisorCluster(pvtemp.Spec.CSI.VolumeHandle)
			gomega.Expect(volumeExists).To(gomega.BeFalse())
		}()

		volumeID := getVolumeIDFromSupervisorCluster(svcPVCName)
		gomega.Expect(volumeID).NotTo(gomega.BeEmpty())

		ginkgo.By("Creating the PV")
		pvNew := getPersistentVolumeSpec(svcPVCName, v1.PersistentVolumeReclaimDelete, nil)
		pvNew.Annotations = pvtemp.Annotations
		pvNew.Spec.StorageClassName = pvtemp.Spec.StorageClassName
		pvNew.Spec.CSI = pvtemp.Spec.CSI
		pvNew.Spec.CSI.VolumeHandle = svcPVCName
		pvNew, err = clientNewGc.CoreV1().PersistentVolumes().Create(ctx, pvNew, metav1.CreateOptions{})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Creating the PVC")
		pvcNew := getPersistentVolumeClaimSpec(namespaceNewGC, nil, pvNew.Name)
		pvcNew.Spec.StorageClassName = &pvtemp.Spec.StorageClassName
		pvcNew, err = clientNewGc.CoreV1().PersistentVolumeClaims(namespaceNewGC).Create(ctx,
			pvcNew, metav1.CreateOptions{})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		// Wait for PV and PVC to Bind.
		framework.ExpectNoError(fpv.WaitOnPVandPVC(clientNewGc,
			framework.NewTimeoutContextWithDefaults(), namespaceNewGC, pvNew, pvcNew))

		ginkgo.By("Expanding current pvc")
		currentPvcSize := pvcNew.Spec.Resources.Requests[v1.ResourceStorage]
		newSize := currentPvcSize.DeepCopy()
		newSize.Add(resource.MustParse("1Gi"))
		framework.Logf("currentPvcSize %v, newSize %v", currentPvcSize, newSize)
		pvcNew, err = expandPVCSize(pvcNew, newSize, clientNewGc)
		framework.ExpectNoError(err, "While updating pvc for more size")
		gomega.Expect(pvcNew).NotTo(gomega.BeNil())

		pvcSize := pvcNew.Spec.Resources.Requests[v1.ResourceStorage]
		if pvcSize.Cmp(newSize) != 0 {
			framework.Failf("error updating pvc size %q", pvcNew.Name)
		}
		ginkgo.By("Checking for PVC request size change on SVC PVC")
		b, err := verifyPvcRequestedSizeUpdateInSupervisorWithWait(svcPVCName, newSize)
		gomega.Expect(b).To(gomega.BeTrue())
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Waiting for controller volume resize to finish")
		err = waitForPvResizeForGivenPvc(pvcNew, clientNewGc, totalResizeWaitPeriod)
		framework.ExpectNoError(err, "While waiting for pvc resize to finish")

		ginkgo.By("Checking for resize on SVC PV")
		verifyPVSizeinSupervisor(svcPVCName, newSize)

		ginkgo.By("Checking for conditions on pvc")
		pvcNew, err = waitForPVCToReachFileSystemResizePendingCondition(clientNewGc,
			namespaceNewGC, pvcNew.Name, pollTimeout)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Checking for 'FileSystemResizePending' status condition on SVC PVC")
		_, err = checkSvcPvcHasGivenStatusCondition(svcPVCName, true, v1.PersistentVolumeClaimFileSystemResizePending)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By(fmt.Sprintf("Invoking QueryCNSVolumeWithResult with VolumeID: %s", volHandle))
		queryResult, err := e2eVSphere.queryCNSVolumeWithResult(volHandle)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		if len(queryResult.Volumes) == 0 {
			err = fmt.Errorf("QueryCNSVolumeWithResult returned no volume")
		}
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		ginkgo.By("Verifying disk size requested in volume expansion is honored")
		newSizeInMb := convertGiStrToMibInt64(newSize)
		if queryResult.Volumes[0].BackingObjectDetails.(*cnstypes.CnsBlockBackingDetails).CapacityInMb != newSizeInMb {
			err = fmt.Errorf("got wrong disk size after volume expansion")
		}
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		// Create a new Pod to use this PVC, and verify volume has been attached.
		ginkgo.By("Creating a pod to attach PV again to the node")
		pod, err := createPod(clientNewGc, namespaceNewGC, nil, []*v1.PersistentVolumeClaim{pvcNew}, false, execCommand)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By(fmt.Sprintf("Verify volume after expansion: %s is attached to the node: %s",
			pvNew.Spec.CSI.VolumeHandle, pod.Spec.NodeName))
		vmUUID, err := getVMUUIDFromNodeName(pod.Spec.NodeName)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		isDiskAttached, err := e2eVSphere.isVolumeAttachedToVM(clientNewGc, volHandle, vmUUID)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		gomega.Expect(isDiskAttached).To(gomega.BeTrue(), "Volume is not attached to the node")

		ginkgo.By("Verify after expansion the filesystem type is as expected")
		oldKubeConfig := framework.TestContext.KubeConfig
		framework.TestContext.KubeConfig = newGcKubconfigPath
		defer func() {
			framework.TestContext.KubeConfig = oldKubeConfig
		}()

		cmd2 = []string{"exec", pod.Name, fmt.Sprintf("--namespace=%v", namespaceNewGC),
			"--", "/bin/sh", "-c", "df -Tkm | grep /mnt/volume1"}
		lastOutput := framework.RunKubectlOrDie(namespaceNewGC, cmd2...)
		gomega.Expect(strings.Contains(lastOutput, ext4FSType)).NotTo(gomega.BeFalse())

		ginkgo.By("Waiting for file system resize to finish")
		pvcNew, err = waitForFSResize(pvcNew, clientNewGc)
		framework.ExpectNoError(err, "while waiting for fs resize to finish")

		pvcConditions := pvclaim.Status.Conditions
		expectEqual(len(pvcConditions), 0, "pvc should not have conditions")

		ginkgo.By("File system resize finished successfully in GC")
		ginkgo.By("Checking for PVC resize completion on SVC PVC")
		gomega.Expect(verifyResizeCompletedInSupervisor(svcPVCName)).To(gomega.BeTrue())

		// Delete POD.
		ginkgo.By(fmt.Sprintf("Deleting the new pod %s in namespace %s after expansion", pod.Name, namespaceNewGC))
		err = fpod.DeletePodWithWait(clientNewGc, pod)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Verify volume is detached from the node after expansion")
		isDiskDetached, err := e2eVSphere.waitForVolumeDetachedFromNode(clientNewGc,
			pv.Spec.CSI.VolumeHandle, pod.Spec.NodeName)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		gomega.Expect(isDiskDetached).To(gomega.BeTrue(),
			fmt.Sprintf("Volume %q is not detached from the node %q", pv.Spec.CSI.VolumeHandle, pod.Spec.NodeName))

		ginkgo.By("Deleting the PV Claim")
		framework.ExpectNoError(fpv.DeletePersistentVolumeClaim(clientNewGc, pvcNew.Name, namespaceNewGC),
			"Failed to delete PVC ", pvcNew.Name)
		pvcNew = nil

		ginkgo.By("Verify PV should be deleted automatically")
		framework.ExpectNoError(fpv.WaitForPersistentVolumeDeleted(clientNewGc, pvNew.Name, poll, pollTimeoutShort))
		pvNew = nil

		ginkgo.By("Verify volume is deleted in Supervisor Cluster")
		volumeExists := verifyVolumeExistInSupervisorCluster(svcPVCName)
		gomega.Expect(volumeExists).To(gomega.BeFalse())
		pvcDeletedInSvc = true

	})

	/* Verify deleting GC PVC during online volume expansion when reclaim policy in SC set to retain.
	   Reuse the PV and create PVC and perform volume expansion

		1. Create a SC with allowVolumeExpansion set to 'true' and with reclaim policy set to 'Retain'
		2. create a GC PVC using the SC created in step 1 and wait for binding with PV
		3. Delete GC PVC
		4. verify GC PVC is removed but SVC PV, PVC and GC PV still exist
		5. remove claimRef from the PV lingering in GC  to get it to Available state
		6. Create new PVC in GC using the PV lingering in GC using SC created in step 1
		7. verify same SVC PVC is reused
		8. Create a POD using the PVC created in step 6
		9. Resize PVC in GC
		10. Check using CNS query that size has SV PVC and GC PVC are same
		11. Wait for resize to complete and verify that "FilesystemResizePending" is removed from SV PVC and GC PVC
		12. check the size of GC PVC and SVC PVC using CNS query.
		13. verify data is intact on the PV mounted on the pod
		14. Delete POD
		15. Delete PVC in GC
		16. Delete left over PV in GC
		15. Delete SC

	*/
	ginkgo.It("Verify online volume expansion when PV with reclaim policy is reused to create PVC", func() {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		ginkgo.By("Delete PVC in GC")
		err = client.CoreV1().PersistentVolumeClaims(namespace).Delete(ctx, pvclaim.Name, *metav1.NewDeleteOptions(0))
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		pvcDeleted = true

		ginkgo.By("Check GC PV exists and is released")
		pv, err = waitForPvToBeReleased(ctx, client, pv.Name)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		oldPvUID := string(pv.UID)
		fmt.Println("PV uuid", oldPvUID)

		ginkgo.By("Check SVC PVC exists")
		_ = getPVCFromSupervisorCluster(svcPVCName)

		ginkgo.By("Remove claimRef from GC PVC")
		pv.Spec.ClaimRef = nil
		pv, err = client.CoreV1().PersistentVolumes().Update(ctx, pv, metav1.UpdateOptions{})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Creating the PVC in guest cluster")
		pvclaim = getPersistentVolumeClaimSpec(namespace, nil, pv.Name)
		pvclaim.Spec.StorageClassName = &storageclass.Name
		pvclaim, err = client.CoreV1().PersistentVolumeClaims(namespace).Create(ctx, pvclaim, metav1.CreateOptions{})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		defer func() {
			err = client.CoreV1().PersistentVolumeClaims(namespace).Delete(ctx, pvclaim.Name, *metav1.NewDeleteOptions(0))
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}()

		ginkgo.By("Wait for the PVC in guest cluster to bind the lingering pv")
		err = fpv.WaitOnPVandPVC(client, framework.NewTimeoutContextWithDefaults(), namespace, pv, pvclaim)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		// Create a POD to use this PVC, and verify volume has been attached.
		ginkgo.By("Creating pod to attach PV to the node")
		pod, err := createPod(client, namespace, nil, []*v1.PersistentVolumeClaim{pvclaim}, false, execCommand)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By(fmt.Sprintf("Verify volume: %s is attached to the node: %s",
			pv.Spec.CSI.VolumeHandle, pod.Spec.NodeName))
		vmUUID, err := getVMUUIDFromNodeName(pod.Spec.NodeName)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		isDiskAttached, err := e2eVSphere.isVolumeAttachedToVM(client, volHandle, vmUUID)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		gomega.Expect(isDiskAttached).To(gomega.BeTrue(), "Volume is not attached to the node")

		ginkgo.By("Verify the volume is accessible and filesystem type is as expected")
		cmd[1] = pod.Name
		lastOutput := framework.RunKubectlOrDie(namespace, cmd...)
		gomega.Expect(strings.Contains(lastOutput, ext4FSType)).NotTo(gomega.BeFalse())

		ginkgo.By("Check filesystem size for mount point /mnt/volume1 before expansion")
		originalFsSize, err := getFSSizeMb(f, pod)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		defer func() {
			// Delete POD.
			ginkgo.By(fmt.Sprintf("Deleting the pod %s in namespace %s", pod.Name, namespace))
			err = fpod.DeletePodWithWait(client, pod)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())

			ginkgo.By("Verify volume is detached from the node")
			isDiskDetached, err := e2eVSphere.waitForVolumeDetachedFromNode(client,
				pv.Spec.CSI.VolumeHandle, pod.Spec.NodeName)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			gomega.Expect(isDiskDetached).To(gomega.BeTrue(),
				fmt.Sprintf("Volume %q is not detached from the node %q", pv.Spec.CSI.VolumeHandle, pod.Spec.NodeName))

		}()

		// Modify PVC spec to trigger volume expansion.
		// We expand the PVC while no pod is using it to ensure offline expansion.
		ginkgo.By("Expanding current pvc")
		currentPvcSize := pvclaim.Spec.Resources.Requests[v1.ResourceStorage]
		newSize := currentPvcSize.DeepCopy()
		newSize.Add(resource.MustParse("1Gi"))
		framework.Logf("currentPvcSize %v, newSize %v", currentPvcSize, newSize)
		pvclaim, err = expandPVCSize(pvclaim, newSize, client)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		gomega.Expect(pvclaim).NotTo(gomega.BeNil())

		ginkgo.By("Checking for PVC request size change on SVC PVC")
		b, err := verifyPvcRequestedSizeUpdateInSupervisorWithWait(svcPVCName, newSize)
		gomega.Expect(b).To(gomega.BeTrue())
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Waiting for controller volume resize to finish")
		err = waitForPvResizeForGivenPvc(pvclaim, client, totalResizeWaitPeriod)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Checking for resize on SVC PV")
		verifyPVSizeinSupervisor(svcPVCName, newSize)

		ginkgo.By(fmt.Sprintf("Invoking QueryCNSVolumeWithResult with VolumeID: %s", volHandle))
		queryResult, err := e2eVSphere.queryCNSVolumeWithResult(volHandle)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		if len(queryResult.Volumes) == 0 {
			err = fmt.Errorf("QueryCNSVolumeWithResult returned no volume")
		}
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		ginkgo.By("Verifying disk size requested in volume expansion is honored")
		newSizeInMb := convertGiStrToMibInt64(newSize)
		if queryResult.Volumes[0].BackingObjectDetails.(*cnstypes.CnsBlockBackingDetails).CapacityInMb != newSizeInMb {
			err = fmt.Errorf("got wrong disk size after volume expansion")
		}
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Waiting for file system resize to finish")
		pvclaim, err = waitForFSResize(pvclaim, client)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		pvcConditions := pvclaim.Status.Conditions
		expectEqual(len(pvcConditions), 0, "pvc should not have conditions")

		ginkgo.By("Verify filesystem size for mount point /mnt/volume1 after expansion")
		fsSize, err := getFSSizeMb(f, pod)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		// Filesystem size may be smaller than the size of the block volume.
		// Here since filesystem was already formatted on the original volume,
		// we can compare the new filesystem size with the original filesystem size.
		gomega.Expect(fsSize).Should(gomega.BeNumerically(">", originalFsSize),
			fmt.Sprintf("error updating filesystem size for %q. Resulting filesystem size is %d", pvclaim.Name, fsSize))
		ginkgo.By("File system resize finished successfully")

		ginkgo.By("File system resize finished successfully in GC")
		ginkgo.By("Checking for PVC resize completion on SVC PVC")
		gomega.Expect(verifyResizeCompletedInSupervisor(svcPVCName)).To(gomega.BeTrue())

	})

})

func waitForPvToBeReleased(ctx context.Context, client clientset.Interface,
	pvName string) (*v1.PersistentVolume, error) {
	var pv *v1.PersistentVolume
	var err error
	waitErr := wait.PollImmediate(resizePollInterval, pollTimeoutShort, func() (bool, error) {
		pv, err = client.CoreV1().PersistentVolumes().Get(ctx, pvName, metav1.GetOptions{})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		if pv.Status.Phase == v1.VolumeReleased {
			return true, nil
		}
		return false, nil
	})
	return pv, waitErr
}
