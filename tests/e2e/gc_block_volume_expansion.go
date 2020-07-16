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
	"math/rand"
	"os/exec"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"time"

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
)

var _ = ginkgo.Describe("[csi-guest] Volume Expansion Test", func() {
	f := framework.NewDefaultFramework("gc-volume-expansion")
	var (
		client            clientset.Interface
		namespace         string
		storagePolicyName string
		storageclass      *storagev1.StorageClass
		pvclaim           *v1.PersistentVolumeClaim
		err               error
		volHandle         string
		svcPVCName        string
		pv                *v1.PersistentVolume
		pvcDeleted        bool
		cmd               []string
		svcClient         clientset.Interface
	)
	ginkgo.BeforeEach(func() {
		client = f.ClientSet
		namespace = f.Namespace.Name
		bootstrap()
		nodeList, err := fnodes.GetReadySchedulableNodes(f.ClientSet)
		framework.ExpectNoError(err, "Unable to find ready and schedulable Node")
		if !(len(nodeList.Items) > 0) {
			framework.Failf("Unable to find ready and schedulable Node")
		}

		framework.ExpectNoError(err, "Unable to find ready and schedulable Node")
		storagePolicyName = GetAndExpectStringEnvVar(envStoragePolicyNameForSharedDatastores)

		scParameters := make(map[string]string)
		scParameters[scParamFsType] = ext4FSType
		// Create Storage class and PVC
		ginkgo.By("Creating Storage Class and PVC with allowVolumeExpansion = true")

		scParameters[svStorageClassName] = storagePolicyName
		storageclass, pvclaim, err = createPVCAndStorageClass(client, namespace, nil, scParameters, "", nil, "", true, "")
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		// Waiting for PVC to be bound
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

		// replace second element with pod.Name
		cmd = []string{"exec", "--namespace=" + namespace, "--", "/bin/sh", "-c", "df -Tkm | grep /mnt/volume1"}
		svcClient, _ = getSvcClientAndNamespace()

	})

	ginkgo.AfterEach(func() {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		if !pvcDeleted {
			err := fpv.DeletePersistentVolumeClaim(client, pvclaim.Name, namespace)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			err = e2eVSphere.waitForCNSVolumeToBeDeleted(pv.Spec.CSI.VolumeHandle)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			ginkgo.By("Verify volume is deleted in Supervisor Cluster")
			volumeExists := verifyVolumeExistInSupervisorCluster(svcPVCName)
			gomega.Expect(volumeExists).To(gomega.BeFalse())
		}
		err := client.StorageV1().StorageClasses().Delete(ctx, storageclass.Name, *metav1.NewDeleteOptions(0))
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
	})

	/*
		Verify offline expansion triggers FS resize
		Steps:
		1. Create a SC with allowVolumeExpansion set to 'true' in GC
		2. create a PVC using the SC created in step 1 in GC and wait for binding with PV
		3. create a pod using the pvc created in step 2 in GC and wait FS init
		4. write some data to the PV mounted on the pod
		5. delete pod created in step 3 in GC
		6. Resize PVC with new size in GC
		7. wait for PVC Status Condition changed to "FilesystemResizePending" in GC
		8. compare GC and SVC PV sizes are same and is equal to what we used in step 6
		9. Check Size from CNS query is same as what was used in step 6
		10. create new pod in GC using PVC created in step 2 to trigger FS expansion
		11. wait for new size of PVC in GC and compare with SVC PVC size for equality.
		12. verify data is intact on the PV mounted on the pod
		13. delete the pod created in step 10
		14. delete PVC created in step 2
		15. delete SC created in step 1
	*/
	ginkgo.It("Verify offline expansion triggers FS resize", func() {
		// Create a POD to use this PVC, and verify volume has been attached
		ginkgo.By("Creating pod to attach PV to the node")
		pod, err := fpod.CreatePod(client, namespace, nil, []*v1.PersistentVolumeClaim{pvclaim}, false, execCommand)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By(fmt.Sprintf("Verify volume: %s is attached to the node: %s", pv.Spec.CSI.VolumeHandle, pod.Spec.NodeName))
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

		rand.Seed(time.Now().Unix())
		testdataFile := fmt.Sprintf("/tmp/testdata_%v_%v", time.Now().Unix(), rand.Intn(1000))
		ginkgo.By(fmt.Sprintf("Creating a 512mb test data file %v", testdataFile))
		op, err := exec.Command("dd", "if=/dev/urandom", fmt.Sprintf("of=%v", testdataFile), "bs=64k", "count=8000").Output()
		fmt.Println(op)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		defer func() {
			op, err = exec.Command("rm", "-f", testdataFile).Output()
			fmt.Println(op)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}()

		_ = framework.RunKubectlOrDie("cp", testdataFile, fmt.Sprintf("%v/%v:/mnt/volume1/testdata", namespace, pod.Name))

		// Delete POD
		ginkgo.By(fmt.Sprintf("Deleting the pod %s in namespace %s", pod.Name, namespace))
		err = fpod.DeletePodWithWait(client, pod)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Verify volume is detached from the node")
		isDiskDetached, err := e2eVSphere.waitForVolumeDetachedFromNode(client, pv.Spec.CSI.VolumeHandle, pod.Spec.NodeName)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		gomega.Expect(isDiskDetached).To(gomega.BeTrue(), fmt.Sprintf("Volume %q is not detached from the node %q", pv.Spec.CSI.VolumeHandle, pod.Spec.NodeName))

		// Modify PVC spec to trigger volume expansion
		// We expand the PVC while no pod is using it to ensure offline expansion
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
		pvclaim, err = waitForPVCToReachFileSystemResizePendingCondition(f, namespace, pvclaim.Name, pollTimeout)
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
			err = fmt.Errorf("Got wrong disk size after volume expansion")
		}
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		// Create a new POD to use this PVC, and verify volume has been attached
		ginkgo.By("Creating a new pod to attach PV again to the node")
		pod, err = fpod.CreatePod(client, namespace, nil, []*v1.PersistentVolumeClaim{pvclaim}, false, execCommand)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By(fmt.Sprintf("Verify volume after expansion: %s is attached to the node: %s", pv.Spec.CSI.VolumeHandle, pod.Spec.NodeName))
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
		if fsSize < originalFsSize {
			framework.Failf("error updating filesystem size for %q. Resulting filesystem size is %d", pvclaim.Name, fsSize)
		}

		ginkgo.By("Checking data consistency after PVC resize")
		_ = framework.RunKubectlOrDie("cp", fmt.Sprintf("%v/%v:/mnt/volume1/testdata", namespace, pod.Name), testdataFile+"_pod")
		defer func() {
			op, err = exec.Command("rm", "-f", testdataFile+"_pod").Output()
			fmt.Println("rm: ", op)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}()
		ginkgo.By("Running diff...")
		op, err = exec.Command("diff", testdataFile, testdataFile+"_pod").Output()
		fmt.Println("diff: ", op)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		gomega.Expect(len(op)).To(gomega.BeZero())

		ginkgo.By("File system resize finished successfully in GC")
		ginkgo.By("Checking for PVC resize completion on SVC PVC")
		gomega.Expect(verifyResizeCompletedInSupervisor(svcPVCName)).To(gomega.BeTrue())

		// Delete POD
		ginkgo.By(fmt.Sprintf("Deleting the new pod %s in namespace %s after expansion", pod.Name, namespace))
		err = fpod.DeletePodWithWait(client, pod)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Verify volume is detached from the node after expansion")
		isDiskDetached, err = e2eVSphere.waitForVolumeDetachedFromNode(client, pv.Spec.CSI.VolumeHandle, pod.Spec.NodeName)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		gomega.Expect(isDiskDetached).To(gomega.BeTrue(), fmt.Sprintf("Volume %q is not detached from the node %q", pv.Spec.CSI.VolumeHandle, pod.Spec.NodeName))
	})

	/*
		verify offline block volume expansion triggered when SVC CSI pod is down succeeds once SVC CSI pod comes up
		Steps:
		1. Create a SC with allowVolumeExpansion set to 'true' in GC
		2. create a PVC using the SC created in step 1 in GC and wait for binding with PV
		3. create a pod using the pvc created in step 2 in GC and wait FS init
		4. delete pod created in step 3 in GC
		5. bring CSI-controller pod down in SVC
		6. Resize PVC with new size in GC
		7. check for retries in GC
		8. PVC in GC is in "Resizing" state and PVC in SVC has no state related to expansion
		9. bring the CSI-controller pod up in SVC
		10. wait for PVC Status Condition changed to "FilesystemResizePending" in GC
		11. Check Size from CNS query is same as what was used in step 7
		12. create new pod in GC to trigger FS expansion
		13. wait for new size of PVC in GC and compare with SVC PVC size for equality.
		14. delete the pod created in step 11
		15. delete PVC created in step 2
		16. delete SC created in step 1
	*/
	ginkgo.It("verify offline block volume expansion triggered when SVC CSI pod is down succeeds once SVC CSI pod comes up", func() {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		// Create a POD to use this PVC, and verify volume has been attached
		ginkgo.By("Creating pod to attach PV to the node")
		pod, err := fpod.CreatePod(client, namespace, nil, []*v1.PersistentVolumeClaim{pvclaim}, false, execCommand)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By(fmt.Sprintf("Verify volume: %s is attached to the node: %s", pv.Spec.CSI.VolumeHandle, pod.Spec.NodeName))
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

		// Delete POD
		ginkgo.By(fmt.Sprintf("Deleting the pod %s in namespace %s after expansion", pod.Name, namespace))
		err = fpod.DeletePodWithWait(client, pod)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Verify volume is detached from the node after expansion")
		isDiskDetached, err := e2eVSphere.waitForVolumeDetachedFromNode(client, pv.Spec.CSI.VolumeHandle, pod.Spec.NodeName)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		gomega.Expect(isDiskDetached).To(gomega.BeTrue(), fmt.Sprintf("Volume %q is not detached from the node %q", pv.Spec.CSI.VolumeHandle, pod.Spec.NodeName))

		ginkgo.By("Sleeping for 20s...")
		time.Sleep(20 * time.Second)

		ginkgo.By("Bringing SVC CSI controller down...")
		svcCsiDeployment := updateDeploymentReplica(svcClient, 0, vSphereCSIControllerPodNamePrefix, csiSystemNamespace)
		defer func() {
			if *svcCsiDeployment.Spec.Replicas == 0 {
				ginkgo.By("Bringing SVC CSI controller up (cleanup)...")
				updateDeploymentReplica(svcClient, 1, vSphereCSIControllerPodNamePrefix, csiSystemNamespace)
			}
		}()

		ginkgo.By("Expanding current pvc")
		currentPvcSize := pvclaim.Spec.Resources.Requests[v1.ResourceStorage]
		newSize := currentPvcSize.DeepCopy()
		newSize.Add(resource.MustParse("1Gi"))
		framework.Logf("currentPvcSize %v, newSize %v", currentPvcSize, newSize)
		pvclaim, err = expandPVCSize(pvclaim, newSize, client)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		gomega.Expect(pvclaim).NotTo(gomega.BeNil())
		pvcSize := pvclaim.Spec.Resources.Requests[v1.ResourceStorage]
		gomega.Expect(pvcSize.Cmp(newSize)).To(gomega.BeZero())
		ginkgo.By("Checking for PVC request size change on SVC PVC")
		b, err := verifyPvcRequestedSizeUpdateInSupervisorWithWait(svcPVCName, newSize)
		gomega.Expect(b).To(gomega.BeTrue())
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By(fmt.Sprintf("Sleeping for %v seconds for retries...", sleepTimeOut))
		time.Sleep(sleepTimeOut * time.Second)
		pvclaim, err = client.CoreV1().PersistentVolumeClaims(namespace).Get(ctx, pvclaim.Name, metav1.GetOptions{})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		gomega.Expect(pvclaim).NotTo(gomega.BeNil())
		pvclaim, err = checkPvcHasGivenStatusCondition(client, namespace, pvclaim.Name, true, v1.PersistentVolumeClaimResizing)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		gomega.Expect(pvclaim).NotTo(gomega.BeNil())
		_, err = checkSvcPvcHasGivenStatusCondition(svcPVCName, false, "")
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Bringing SVC CSI controller up...")
		svcCsiDeployment = updateDeploymentReplica(svcClient, 1, vSphereCSIControllerPodNamePrefix, csiSystemNamespace)

		ginkgo.By("Waiting for controller volume resize to finish")
		err = waitForPvResizeForGivenPvc(pvclaim, client, totalResizeWaitPeriod)
		framework.ExpectNoError(err, "While waiting for pvc resize to finish")

		ginkgo.By("Checking for PVC request size change on SVC PVC")
		b, err = verifyPvcRequestedSizeUpdateInSupervisorWithWait(svcPVCName, newSize)
		gomega.Expect(b).To(gomega.BeTrue())
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Checking for resize on SVC PV")
		verifyPVSizeinSupervisor(svcPVCName, newSize)

		ginkgo.By("Checking for conditions on pvc")
		pvclaim, err = waitForPVCToReachFileSystemResizePendingCondition(f, namespace, pvclaim.Name, pollTimeout)
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
			err = fmt.Errorf("Got wrong disk size after volume expansion")
		}
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		// Create a new POD to use this PVC, and verify volume has been attached
		ginkgo.By("Creating a new pod to attach PV again to the node")
		pod, err = fpod.CreatePod(client, namespace, nil, []*v1.PersistentVolumeClaim{pvclaim}, false, execCommand)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By(fmt.Sprintf("Verify volume after expansion: %s is attached to the node: %s", pv.Spec.CSI.VolumeHandle, pod.Spec.NodeName))
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
		if fsSize < originalFsSize {
			framework.Failf("error updating filesystem size for %q. Resulting filesystem size is %d", pvclaim.Name, fsSize)
		}

		ginkgo.By("File system resize finished successfully in GC")
		ginkgo.By("Checking for PVC resize completion on SVC PVC")
		gomega.Expect(verifyResizeCompletedInSupervisor(svcPVCName)).To(gomega.BeTrue())

		// Delete POD
		ginkgo.By(fmt.Sprintf("Deleting the new pod %s in namespace %s after expansion", pod.Name, namespace))
		err = fpod.DeletePodWithWait(client, pod)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Verify volume is detached from the node after expansion")
		isDiskDetached, err = e2eVSphere.waitForVolumeDetachedFromNode(client, pv.Spec.CSI.VolumeHandle, pod.Spec.NodeName)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		gomega.Expect(isDiskDetached).To(gomega.BeTrue(), fmt.Sprintf("Volume %q is not detached from the node %q", pv.Spec.CSI.VolumeHandle, pod.Spec.NodeName))

	})

	/*
		resize PVC concurrently with different size
		Steps:
		1. Create a SC with allowVolumeExpansion set to 'true'
		2. create a PVC of 2Gi using the SC created in step 1 and wait for binding with PV
		3. resize GC PVC to 4Gi and 5Gi in two seperate threads
		4. Verify GC PVC reaches 5Gi "FilesystemResizePending" state
		5. Check using CNS query that size of the volume is 5Gi
		6. Verify size of PVs in SVC and GC are 5Gi
		7. delete the PVC created in step 2
		8. delete SC created in step 1
	*/
	ginkgo.It("Verify pvc expanded concurrently with different sizes expands to largest size", func() {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		ginkgo.By("Expanding current pvc")
		currentPvcSize := pvclaim.Spec.Resources.Requests[v1.ResourceStorage]
		newSize1 := currentPvcSize.DeepCopy()
		newSize1.Add(resource.MustParse("1Gi"))
		newSize2 := currentPvcSize.DeepCopy()
		newSize2.Add(resource.MustParse("2Gi"))
		newSize3 := currentPvcSize.DeepCopy()
		newSize3.Add(resource.MustParse("3Gi"))

		var wg sync.WaitGroup
		wg.Add(3)
		go resize(client, pvclaim, currentPvcSize, newSize2, &wg)
		go resize(client, pvclaim, currentPvcSize, newSize1, &wg)
		go resize(client, pvclaim, currentPvcSize, newSize3, &wg)
		wg.Wait()

		pvclaim, err = client.CoreV1().PersistentVolumeClaims(namespace).Get(ctx, pvclaim.Name, metav1.GetOptions{})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		gomega.Expect(pvclaim).NotTo(gomega.BeNil())

		pvcSize := pvclaim.Spec.Resources.Requests[v1.ResourceStorage]
		if pvcSize.Cmp(newSize3) != 0 {
			framework.Failf("error updating pvc size %q", pvclaim.Name)
		}

		ginkgo.By("Checking for PVC request size change on SVC PVC")
		b, err := verifyPvcRequestedSizeUpdateInSupervisorWithWait(svcPVCName, newSize3)
		gomega.Expect(b).To(gomega.BeTrue())
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Waiting for controller volume resize to finish")
		err = waitForPvResizeForGivenPvc(pvclaim, client, totalResizeWaitPeriod)
		framework.ExpectNoError(err, "While waiting for pvc resize to finish")

		ginkgo.By("Checking for resize on SVC PV")
		verifyPVSizeinSupervisor(svcPVCName, newSize3)

		ginkgo.By("Checking for conditions on pvc")
		pvclaim, err = waitForPVCToReachFileSystemResizePendingCondition(f, namespace, pvclaim.Name, pollTimeout)
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
		newSizeInMb := convertGiStrToMibInt64(newSize3)
		if queryResult.Volumes[0].BackingObjectDetails.(*cnstypes.CnsBlockBackingDetails).CapacityInMb != newSizeInMb {
			err = fmt.Errorf("Got wrong disk size after volume expansion")
		}
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
	})

	/*
		CNS down during resize
		Steps:
		1. Create a SC with allowVolumeExpansion set to 'true'
		2. create a PVC using the SC created in step 1 and wait for binding with PV
		3. Bring CNS down
		4. resize the PVC created in step 2
		5. check that PVC in GC AND SVC are still in "Resizing" state even after 3 mins and retires are being made to resize
		6. Bring CNS up
		7. Verify GC PVC eventually reaches "FilesystemResizePending" state
		8. Check using CNS query that size has got updated to what was used in step 4
		9. Verify size of PVs in SVC and GC to same as the one used in the step 4
		10. delete PVC created in step 2
		11. delete SC created in step 1
	*/
	ginkgo.It("Verify volume expansion eventually succeeds when CNS is unavailable during initial expansion", func() {
		ginkgo.By(fmt.Sprintln("Stopping vsan-health on the vCenter host"))
		vcAddress := e2eVSphere.Config.Global.VCenterHostname + ":" + sshdPort
		err = invokeVCenterServiceControl(stopOperation, vsanhealthServiceName, vcAddress)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		ginkgo.By(fmt.Sprintf("Sleeping for %v seconds to allow vsan-health to completely shutdown", vsanHealthServiceWaitTime))
		time.Sleep(time.Duration(vsanHealthServiceWaitTime) * time.Second)
		vsanDown := true
		defer func() {
			if vsanDown {
				ginkgo.By(fmt.Sprintln("Starting vsan-health on the vCenter host (cleanup)"))
				vcAddress := e2eVSphere.Config.Global.VCenterHostname + ":" + sshdPort
				err = invokeVCenterServiceControl(startOperation, vsanhealthServiceName, vcAddress)
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
				ginkgo.By(fmt.Sprintf("Sleeping for %v seconds to allow vsan-health to come up again", vsanHealthServiceWaitTime))
				time.Sleep(time.Duration(vsanHealthServiceWaitTime) * time.Second)
			}
		}()

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

		ginkgo.By(fmt.Sprintf("Sleeping for %v for SVC operation timeout...", svOperationTimeout))
		time.Sleep(svOperationTimeout)
		pvclaim, err = checkPvcHasGivenStatusCondition(client, namespace, pvclaim.Name, true, v1.PersistentVolumeClaimResizing)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		_, err = checkSvcPvcHasGivenStatusCondition(svcPVCName, true, v1.PersistentVolumeClaimResizing)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By(fmt.Sprintln("Starting vsan-health on the vCenter host"))
		vcAddress = e2eVSphere.Config.Global.VCenterHostname + ":" + sshdPort
		err = invokeVCenterServiceControl(startOperation, vsanhealthServiceName, vcAddress)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		ginkgo.By(fmt.Sprintf("Sleeping for %v seconds to allow vsan-health to come up again", vsanHealthServiceWaitTime))
		time.Sleep(time.Duration(vsanHealthServiceWaitTime) * time.Second)
		vsanDown = false

		ginkgo.By("Waiting for controller volume resize to finish")
		err = waitForPvResizeForGivenPvc(pvclaim, client, totalResizeWaitPeriod)
		framework.ExpectNoError(err, "While waiting for pvc resize to finish")

		ginkgo.By("Checking for PVC request size change on SVC PVC")
		b, err = verifyPvcRequestedSizeUpdateInSupervisorWithWait(svcPVCName, newSize)
		gomega.Expect(b).To(gomega.BeTrue())
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Checking for resize on SVC PV")
		verifyPVSizeinSupervisor(svcPVCName, newSize)

		ginkgo.By("Checking for conditions on pvc")
		pvclaim, err = waitForPVCToReachFileSystemResizePendingCondition(f, namespace, pvclaim.Name, pollTimeout)
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
			err = fmt.Errorf("Got wrong disk size after volume expansion")
		}
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

	})

})

func resize(client clientset.Interface, pvc *v1.PersistentVolumeClaim, currentPvcSize resource.Quantity, newSize resource.Quantity, wg *sync.WaitGroup) {
	defer wg.Done()
	framework.Logf("currentPvcSize %v, newSize %v", currentPvcSize, newSize)
	_, err := expandPVCSize(pvc, newSize, client)
	framework.Logf("Error from expansion attempt on pvc %v, to %v: %v", pvc.Name, newSize, err)
}

// verifyPVCRequestedSizeInSupervisor compares Pvc.Spec.Resources.Requests[v1.ResourceStorage] with given size in SVC
func verifyPVCRequestedSizeInSupervisor(pvcName string, size resource.Quantity) bool {
	SvcPvc := getPVCFromSupervisorCluster(pvcName)
	pvcSize := SvcPvc.Spec.Resources.Requests[v1.ResourceStorage]
	framework.Logf("SVC PVC requested size: %v", pvcSize)
	return pvcSize.Cmp(size) == 0
}

// verifyPVCRequestedSizeInSupervisor waits till Pvc.Spec.Resources.Requests[v1.ResourceStorage] matches with given size in SVC
func verifyPvcRequestedSizeUpdateInSupervisorWithWait(pvcName string, size resource.Quantity) (bool, error) {
	var b bool

	waitErr := wait.PollImmediate(resizePollInterval, totalResizeWaitPeriod, func() (bool, error) {
		b = verifyPVCRequestedSizeInSupervisor(pvcName, size)
		return b, nil
	})
	return b, waitErr
}

// verifyResizeCompletedInSupervisor checks if resize relates status conditions are removed from PVC and
// Pvc.Spec.Resources.Requests[v1.ResourceStorage] is same as pvc.Status.Capacity[v1.ResourceStorage] in SVC
func verifyResizeCompletedInSupervisor(pvcName string) bool {
	pvc := getPVCFromSupervisorCluster(pvcName)
	pvcRequestedSize := pvc.Spec.Resources.Requests[v1.ResourceStorage]
	pvcCapacitySize := pvc.Status.Capacity[v1.ResourceStorage]

	//If pvc's capacity size is greater than or equal to pvc's request size then done
	if pvcCapacitySize.Cmp(pvcRequestedSize) < 0 {
		return false
	}

	if len(pvc.Status.Conditions) == 0 {
		return true
	}
	return false
}

// checkSvcPvcHasGivenStatusCondition checks if the status condition in SVC PVC matches with the one we want
func checkSvcPvcHasGivenStatusCondition(pvcName string, conditionsPresent bool, condition v1.PersistentVolumeClaimConditionType) (*v1.PersistentVolumeClaim, error) {
	svcClient, svcNamespace := getSvcClientAndNamespace()
	return checkPvcHasGivenStatusCondition(svcClient, svcNamespace, pvcName, conditionsPresent, condition)
}

// verifyPVSizeinSupervisor compares PV.Spec.Capacity[v1.ResourceStorage] in SVC with the size passed here
func verifyPVSizeinSupervisor(svcPVCName string, newSize resource.Quantity) {
	svcPV := getPvFromSupervisorCluster(svcPVCName)
	svcPVSize := svcPV.Spec.Capacity[v1.ResourceStorage]
	// If pv size is greater or equal to requested size that means controller resize is finished.
	gomega.Expect(svcPVSize.Cmp(newSize) >= 0).To(gomega.BeTrue())
}

// waitForPVCToReachFileSystemResizePendingCondition waits for PVC to reach FileSystemResizePendingCondition status condition
func waitForPVCToReachFileSystemResizePendingCondition(f *framework.Framework, namespace string, pvcName string, timeout time.Duration) (*v1.PersistentVolumeClaim, error) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	var pvclaim *v1.PersistentVolumeClaim
	var err error

	waitErr := wait.PollImmediate(resizePollInterval, timeout, func() (bool, error) {
		pvclaim, err = f.ClientSet.CoreV1().PersistentVolumeClaims(namespace).Get(ctx, pvcName, metav1.GetOptions{})
		if err != nil {
			return false, fmt.Errorf("error while fetching pvc '%v' after controller resize: %v", pvcName, err)
		}
		gomega.Expect(pvclaim).NotTo(gomega.BeNil())

		inProgressConditions := pvclaim.Status.Conditions
		// if there are conditions on the PVC, it must be of FileSystemResizePending type
		if len(inProgressConditions) > 0 {
			expectEqual(len(inProgressConditions), 1, fmt.Sprintf("PVC '%v' has more than one status conditions", pvcName))
			if inProgressConditions[0].Type == v1.PersistentVolumeClaimFileSystemResizePending {
				return true, nil
			}
			gomega.Expect(inProgressConditions[0].Type == v1.PersistentVolumeClaimResizing).To(gomega.BeTrue(), fmt.Sprintf("PVC '%v' is not in 'Resizing' or 'FileSystemResizePending' status condition", pvcName))
		} else {
			return false, fmt.Errorf("Resize was not triggered on PVC '%v' or no status conditions related to resizing found on it", pvcName)
		}
		return false, nil
	})
	return pvclaim, waitErr
}

// checkPvcHasGivenStatusCondition checks if the status condition in PVC matches with the one we want
func checkPvcHasGivenStatusCondition(client clientset.Interface, namespace string, pvcName string, conditionsPresent bool, condition v1.PersistentVolumeClaimConditionType) (*v1.PersistentVolumeClaim, error) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	pvclaim, err := client.CoreV1().PersistentVolumeClaims(namespace).Get(ctx, pvcName, metav1.GetOptions{})
	gomega.Expect(err).NotTo(gomega.HaveOccurred())
	gomega.Expect(pvclaim).NotTo(gomega.BeNil())
	inProgressConditions := pvclaim.Status.Conditions

	if len(inProgressConditions) == 0 {
		if conditionsPresent {
			return pvclaim, fmt.Errorf("No status conditions found on PVC: %v", pvcName)
		}
		return pvclaim, nil
	}
	expectEqual(len(inProgressConditions), 1, fmt.Sprintf("PVC '%v' has more than one status condition", pvcName))
	if inProgressConditions[0].Type != condition {
		return pvclaim, fmt.Errorf("Status condition found on PVC '%v' is '%v', and is not matching with expected status condition '%v'", pvcName, inProgressConditions[0].Type, condition)
	}
	return pvclaim, nil
}

// convertGiStrToMibInt64 returns integer numbers of Mb equivalent to string of the form \d+Gi
func convertGiStrToMibInt64(size resource.Quantity) int64 {
	r, err := regexp.Compile("[0-9]+")
	gomega.Expect(err).NotTo(gomega.HaveOccurred())
	sizeInt, err := strconv.Atoi(r.FindString(size.String()))
	gomega.Expect(err).NotTo(gomega.HaveOccurred())
	return int64(sizeInt * 1024)
}
