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
	"os"
	"os/exec"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"time"

	ginkgo "github.com/onsi/ginkgo/v2"
	"github.com/onsi/gomega"
	cnstypes "github.com/vmware/govmomi/cns/types"
	"github.com/vmware/govmomi/object"
	v1 "k8s.io/api/core/v1"
	storagev1 "k8s.io/api/storage/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/wait"
	clientset "k8s.io/client-go/kubernetes"
	restclient "k8s.io/client-go/rest"
	"k8s.io/kubernetes/test/e2e/framework"
	fnodes "k8s.io/kubernetes/test/e2e/framework/node"
	fpod "k8s.io/kubernetes/test/e2e/framework/pod"
	fpv "k8s.io/kubernetes/test/e2e/framework/pv"
	admissionapi "k8s.io/pod-security-admission/api"
)

var _ = ginkgo.Describe("[csi-guest] Volume Expansion Test", func() {
	f := framework.NewDefaultFramework("gc-volume-expansion")
	f.NamespacePodSecurityEnforceLevel = admissionapi.LevelPrivileged
	var (
		client                     clientset.Interface
		pandoraSyncWaitTime        int
		namespace                  string
		storagePolicyName          string
		storageclass               *storagev1.StorageClass
		pvclaim                    *v1.PersistentVolumeClaim
		err                        error
		volHandle                  string
		svcPVCName                 string
		pv                         *v1.PersistentVolume
		pvcDeleted                 bool
		cmd                        []string
		svcClient                  clientset.Interface
		svNamespace                string
		defaultDatastore           *object.Datastore
		restConfig                 *restclient.Config
		isVsanHealthServiceStopped bool
		isGCCSIDeploymentPODdown   bool
	)
	ginkgo.BeforeEach(func() {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
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

		// Set resource quota.
		ginkgo.By("Set Resource quota for GC")
		svcClient, svNamespace = getSvcClientAndNamespace()
		setResourceQuota(svcClient, svNamespace, rqLimit)

		// Create Storage class and PVC.
		ginkgo.By("Creating Storage Class and PVC with allowVolumeExpansion = true")

		scParameters[svStorageClassName] = storagePolicyName
		storageclass, pvclaim, err = createPVCAndStorageClass(client, namespace, nil, scParameters, "", nil, "", true, "")
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

		// Replace second element with pod.Name.
		cmd = []string{"exec", "", "--namespace=" + namespace, "--", "/bin/sh", "-c", "df -Tkm | grep /mnt/volume1"}

		// Set up default pandora sync wait time.
		pandoraSyncWaitTime = defaultPandoraSyncWaitTime

		defaultDatastore = getDefaultDatastore(ctx)
		// Get restConfig.
		restConfig = getRestConfigClient()
		isGCCSIDeploymentPODdown = false

	})

	ginkgo.AfterEach(func() {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		vcAddress := e2eVSphere.Config.Global.VCenterHostname + ":" + sshdPort
		if isVsanHealthServiceStopped {
			startVCServiceWait4VPs(ctx, vcAddress, vsanhealthServiceName, &isVsanHealthServiceStopped)
		}
		if !pvcDeleted {
			err := fpv.DeletePersistentVolumeClaim(client, pvclaim.Name, namespace)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			err = e2eVSphere.waitForCNSVolumeToBeDeleted(volHandle)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			ginkgo.By("Verify volume is deleted in Supervisor Cluster")
			volumeExists := verifyVolumeExistInSupervisorCluster(svcPVCName)
			gomega.Expect(volumeExists).To(gomega.BeFalse())
		}
		err := client.StorageV1().StorageClasses().Delete(ctx, storageclass.Name, *metav1.NewDeleteOptions(0))
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		svcClient, svNamespace = getSvcClientAndNamespace()
		setResourceQuota(svcClient, svNamespace, defaultrqLimit)

		if isGCCSIDeploymentPODdown {
			_ = updateDeploymentReplica(client, 1, vSphereCSIControllerPodNamePrefix, csiSystemNamespace)
		}
	})

	// Verify offline expansion triggers FS resize.
	// Steps:
	// 1. Create a SC with allowVolumeExpansion set to 'true' in GC.
	// 2. create a PVC using the SC created in step 1 in GC and wait for binding
	//    with PV.
	// 3. create a pod using the pvc created in step 2 in GC and wait FS init.
	// 4. write some data to the PV mounted on the pod.
	// 5. delete pod created in step 3 in GC.
	// 6. Resize PVC with new size in GC.
	// 7. wait for PVC Status Condition changed to "FilesystemResizePending" in GC.
	// 8. compare GC and SVC PV sizes are same and is equal to what we used in
	//    step 6.
	// 9. Check Size from CNS query is same as what was used in step 6.
	// 10. create new pod in GC using PVC created in step 2 to trigger FS expansion.
	// 11. wait for new size of PVC in GC and compare with SVC PVC size for equality.
	// 12. verify data is intact on the PV mounted on the pod.
	// 13. delete the pod created in step 10.
	// 14. delete PVC created in step 2.
	// 15. delete SC created in step 1.
	ginkgo.It("Verify offline expansion triggers FS resize", func() {
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

		rand.New(rand.NewSource(time.Now().Unix()))
		testdataFile := fmt.Sprintf("/tmp/testdata_%v_%v", time.Now().Unix(), rand.Intn(1000))
		ginkgo.By(fmt.Sprintf("Creating a 512mb test data file %v", testdataFile))
		op, err := exec.Command("dd", "if=/dev/urandom", fmt.Sprintf("of=%v", testdataFile),
			"bs=64k", "count=8000").Output()
		fmt.Println(op)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		defer func() {
			op, err = exec.Command("rm", "-f", testdataFile).Output()
			fmt.Println(op)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}()

		_ = framework.RunKubectlOrDie(namespace, "cp", testdataFile,
			fmt.Sprintf("%v/%v:/mnt/volume1/testdata", namespace, pod.Name))

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

		// Modify PVC spec to trigger volume expansion. We expand the PVC while
		// no pod is using it to ensure offline expansion.
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

		ginkgo.By("Checking for 'FileSystemResizePending' status condition on SVC PVC")
		err = waitForSvcPvcToReachFileSystemResizePendingCondition(svcPVCName, pollTimeout)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Checking for conditions on pvc")
		pvclaim, err = waitForPVCToReachFileSystemResizePendingCondition(client, namespace, pvclaim.Name, pollTimeout)
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
		if fsSize < originalFsSize {
			framework.Failf("error updating filesystem size for %q. Resulting filesystem size is %d", pvclaim.Name, fsSize)
		}

		ginkgo.By("Checking data consistency after PVC resize")
		_ = framework.RunKubectlOrDie(namespace, "cp",
			fmt.Sprintf("%v/%v:/mnt/volume1/testdata", namespace, pod.Name), testdataFile+"_pod")
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
		_, err = waitForFSResizeInSvc(svcPVCName)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		// Delete POD.
		ginkgo.By(fmt.Sprintf("Deleting the new pod %s in namespace %s after expansion", pod.Name, namespace))
		err = fpod.DeletePodWithWait(client, pod)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Verify volume is detached from the node after expansion")
		isDiskDetached, err = e2eVSphere.waitForVolumeDetachedFromNode(client, pv.Spec.CSI.VolumeHandle, pod.Spec.NodeName)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		gomega.Expect(isDiskDetached).To(gomega.BeTrue(), fmt.Sprintf("Volume %q is not detached from the node %q",
			pv.Spec.CSI.VolumeHandle, pod.Spec.NodeName))
	})

	// Verify offline block volume expansion triggered when SVC CSI pod is down
	// succeeds once SVC CSI pod comes up.
	// Steps:
	// 1. Create a SC with allowVolumeExpansion set to 'true' in GC.
	// 2. create a PVC using the SC created in step 1 in GC and wait for binding
	//    with PV.
	// 3. create a pod using the pvc created in step 2 in GC and wait FS init.
	// 4. delete pod created in step 3 in GC.
	// 5. bring CSI-controller pod down in SVC.
	// 6. Resize PVC with new size in GC.
	// 7. check for retries in GC.
	// 8. PVC in GC is in "Resizing" state and PVC in SVC has no state related
	//    to expansion.
	// 9. bring the CSI-controller pod up in SVC.
	// 10. wait for PVC Status Condition changed to "FilesystemResizePending"
	//     in GC.
	// 11. Check Size from CNS query is same as what was used in step 7.
	// 12. create new pod in GC to trigger FS expansion.
	// 13. wait for new size of PVC in GC and compare with SVC PVC size for
	//     equality.
	// 14. delete the pod created in step 11.
	// 15. delete PVC created in step 2.
	// 16. delete SC created in step 1.

	//TODO: need to add this test under destuctive test [csi-guest-destructive]
	ginkgo.It("verify offline block volume expansion triggered when SVC CSI pod is down "+
		"succeeds once SVC CSI pod comes up", func() {
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
		ginkgo.By(fmt.Sprintf("Deleting the pod %s in namespace %s after expansion", pod.Name, namespace))
		err = fpod.DeletePodWithWait(client, pod)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Verify volume is detached from the node after expansion")
		isDiskDetached, err := e2eVSphere.waitForVolumeDetachedFromNode(client,
			pv.Spec.CSI.VolumeHandle, pod.Spec.NodeName)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		gomega.Expect(isDiskDetached).To(gomega.BeTrue(), fmt.Sprintf("Volume %q is not detached from the node %q",
			pv.Spec.CSI.VolumeHandle, pod.Spec.NodeName))

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
		pvclaim, err = checkPvcHasGivenStatusCondition(client,
			namespace, pvclaim.Name, true, v1.PersistentVolumeClaimResizing)
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

		ginkgo.By("Checking for 'FileSystemResizePending' status condition on SVC PVC")
		err = waitForSvcPvcToReachFileSystemResizePendingCondition(svcPVCName, pollTimeout)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Checking for conditions on pvc")
		pvclaim, err = waitForPVCToReachFileSystemResizePendingCondition(client, namespace, pvclaim.Name, pollTimeout)
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
		if fsSize < originalFsSize {
			framework.Failf("error updating filesystem size for %q. Resulting filesystem size is %d", pvclaim.Name, fsSize)
		}

		ginkgo.By("File system resize finished successfully in GC")
		ginkgo.By("Checking for PVC resize completion on SVC PVC")
		_, err = waitForFSResizeInSvc(svcPVCName)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		// Delete POD
		ginkgo.By(fmt.Sprintf("Deleting the new pod %s in namespace %s after expansion", pod.Name, namespace))
		err = fpod.DeletePodWithWait(client, pod)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Verify volume is detached from the node after expansion")
		isDiskDetached, err = e2eVSphere.waitForVolumeDetachedFromNode(client,
			pv.Spec.CSI.VolumeHandle, pod.Spec.NodeName)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		gomega.Expect(isDiskDetached).To(gomega.BeTrue(), fmt.Sprintf("Volume %q is not detached from the node %q",
			pv.Spec.CSI.VolumeHandle, pod.Spec.NodeName))

	})

	// Resize PVC concurrently with different size.
	// Steps:
	// 1. Create a SC with allowVolumeExpansion set to 'true'.
	// 2. create a PVC of 2Gi using the SC created in step 1 and wait for
	//    binding with PV.
	// 3. resize GC PVC to 4Gi and 5Gi in two separate threads.
	// 4. Verify GC PVC reaches 5Gi "FilesystemResizePending" state.
	// 5. Check using CNS query that size of the volume is 5Gi.
	// 6. Verify size of PVs in SVC and GC are 5Gi.
	// 7. delete the PVC created in step 2.
	// 8. delete SC created in step 1.
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

		ginkgo.By("Checking for 'FileSystemResizePending' status condition on SVC PVC")
		err = waitForSvcPvcToReachFileSystemResizePendingCondition(svcPVCName, pollTimeout)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Checking for conditions on pvc")
		pvclaim, err = waitForPVCToReachFileSystemResizePendingCondition(client, namespace, pvclaim.Name, pollTimeout)
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
			err = fmt.Errorf("got wrong disk size after volume expansion")
		}
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
	})

	// CNS down during resize.
	// Steps:
	// 1. Create a SC with allowVolumeExpansion set to 'true'.
	// 2. create a PVC using the SC created in step 1 and wait for binding
	//    with PV.
	// 3. Bring CNS down.
	// 4. resize the PVC created in step 2.
	// 5. check that PVC in GC AND SVC are still in "Resizing" state even after
	//    3 mins and retires are being made to resize.
	// 6. Bring CNS up.
	// 7. Verify GC PVC eventually reaches "FilesystemResizePending" state.
	// 8. Check using CNS query that size has got updated to what was used in
	//    step 4.
	// 9. Verify size of PVs in SVC and GC to same as the one used in the step 4.
	// 10. delete PVC created in step 2.
	// 11. delete SC created in step 1.
	//TODO: need to add this test under destuctive test [csi-guest-destructive]
	ginkgo.It("Verify volume expansion eventually succeeds when CNS is unavailable during initial expansion", func() {
		ginkgo.By(fmt.Sprintln("Stopping vsan-health on the vCenter host"))
		vcAddress := e2eVSphere.Config.Global.VCenterHostname + ":" + sshdPort
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		err = invokeVCenterServiceControl(stopOperation, vsanhealthServiceName, vcAddress)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		ginkgo.By(fmt.Sprintf("Sleeping for %v seconds to allow vsan-health to completely shutdown",
			vsanHealthServiceWaitTime))
		time.Sleep(time.Duration(vsanHealthServiceWaitTime) * time.Second)
		isVsanHealthServiceStopped := true
		defer func() {
			if isVsanHealthServiceStopped {
				startVCServiceWait4VPs(ctx, vcAddress, vsanhealthServiceName, &isVsanHealthServiceStopped)
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
		pvclaim, err = checkPvcHasGivenStatusCondition(client,
			namespace, pvclaim.Name, true, v1.PersistentVolumeClaimResizing)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		_, err = checkSvcPvcHasGivenStatusCondition(svcPVCName, true, v1.PersistentVolumeClaimResizing)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By(fmt.Sprintln("Starting vsan-health on the vCenter host"))
		vcAddress = e2eVSphere.Config.Global.VCenterHostname + ":" + sshdPort
		startVCServiceWait4VPs(ctx, vcAddress, vsanhealthServiceName, &isVsanHealthServiceStopped)

		ginkgo.By("Waiting for controller volume resize to finish")
		err = waitForPvResizeForGivenPvc(pvclaim, client, totalResizeWaitPeriod)
		framework.ExpectNoError(err, "While waiting for pvc resize to finish")

		ginkgo.By("Checking for PVC request size change on SVC PVC")
		b, err = verifyPvcRequestedSizeUpdateInSupervisorWithWait(svcPVCName, newSize)
		gomega.Expect(b).To(gomega.BeTrue())
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Checking for resize on SVC PV")
		verifyPVSizeinSupervisor(svcPVCName, newSize)

		ginkgo.By("Checking for 'FileSystemResizePending' status condition on SVC PVC")
		err = waitForSvcPvcToReachFileSystemResizePendingCondition(svcPVCName, pollTimeout)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Checking for conditions on pvc")
		pvclaim, err = waitForPVCToReachFileSystemResizePendingCondition(client, namespace, pvclaim.Name, pollTimeout)
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

	})

	// CNS down during resize and delete GC PVC while CNS is still down.
	// Steps:
	// 1. Create a SC with allowVolumeExpansion set to 'true'.
	// 2. Create a PVC using the SC created in step 1 and wait for binding with PV.
	// 3. Bring CNS down.
	// 4. Resize the PVC created in step 2.
	// 5. Check that PVC in GC AND SVC are still in "Resizing" state even after
	//    3 mins and retries are being made to resize.
	// 6. Delete GC PVC created in step 2.
	// 7. Verify resize retries stop.
	// 8. Bring CNS up.
	// 9. Delete SC created in step 1.
	//
	// TODO: There is an upstream work going on to prevent PVC deletion when
	//       resize is in progress. This test needs to be re-evaluated in the
	//       future when the upstream happens.
	//TODO: need to add this test under destuctive test [csi-guest-destructive]
	ginkgo.It("Verify while CNS is down the volume expansion can be triggered and "+
		"the volume can deleted with pending resize operation", func() {
		ginkgo.By(fmt.Sprintln("Stopping vsan-health on the vCenter host"))
		vcAddress := e2eVSphere.Config.Global.VCenterHostname + ":" + sshdPort
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		err = invokeVCenterServiceControl(stopOperation, vsanhealthServiceName, vcAddress)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		ginkgo.By(fmt.Sprintf("Sleeping for %v seconds to allow vsan-health to completely shutdown",
			vsanHealthServiceWaitTime))
		time.Sleep(time.Duration(vsanHealthServiceWaitTime) * time.Second)
		isVsanHealthServiceStopped := true
		defer func() {
			if isVsanHealthServiceStopped {
				ginkgo.By(fmt.Sprintln("Starting vsan-health on the vCenter host (cleanup)"))
				vcAddress = e2eVSphere.Config.Global.VCenterHostname + ":" + sshdPort
				startVCServiceWait4VPs(ctx, vcAddress, vsanhealthServiceName, &isVsanHealthServiceStopped)
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
		pvclaim, err = checkPvcHasGivenStatusCondition(client,
			namespace, pvclaim.Name, true, v1.PersistentVolumeClaimResizing)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		_, err = checkSvcPvcHasGivenStatusCondition(svcPVCName, true, v1.PersistentVolumeClaimResizing)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		err = fpv.DeletePersistentVolumeClaim(client, pvclaim.Name, namespace)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By(fmt.Sprintln("Starting vsan-health on the vCenter host"))
		vcAddress = e2eVSphere.Config.Global.VCenterHostname + ":" + sshdPort
		startVCServiceWait4VPs(ctx, vcAddress, vsanhealthServiceName, &isVsanHealthServiceStopped)

		ginkgo.By("Verify volume is deleted in Supervisor Cluster")
		volumeExists := verifyVolumeExistInSupervisorCluster(svcPVCName)
		gomega.Expect(volumeExists).To(gomega.BeFalse())
		pvcDeleted = true
		err = e2eVSphere.waitForCNSVolumeToBeDeleted(pv.Spec.CSI.VolumeHandle)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
	})

	// Resize beyond storage policy quota fails.
	// Steps:
	// 1. Create a SC with allowVolumeExpansion set to 'true'.
	// 2. Set a quota limit on the storage policy in VC.
	// 3. Create a PVC using the SC created in step 1 and wait for binding with PV.
	// 4. Resize GC PVC to size bigger than what storage policy quota allows.
	// 5. Verify resize fails.
	// 6. Increase quota on the storage policy to allow the exapnsion.
	// 7. Verify SVC and PVC reach "FilesystemResizePending" state and PVs have
	//    the new size used in step 3.
	// 8. Check using CNS query that size has got updated to what was used in step 4.
	// 9. Verify size of PVs in SVC and GC to same as the one used in the step 4.
	// 10. Delete PVC created in step 2.
	// 11. Delete SC created in step 1.
	ginkgo.It("Resize beyond storage policy quota fails", func() {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		ginkgo.By("Set quota in SVC for 5Gi on policy(SC) - " + storagePolicyName)
		svcClient, svcNamespace := getSvcClientAndNamespace()
		createResourceQuota(svcClient, svcNamespace, "5Gi", storagePolicyName)
		defer deleteResourceQuota(svcClient, svcNamespace)

		// Modify PVC spec to trigger volume expansion.
		// We expand the PVC while no pod is using it to ensure offline expansion.
		ginkgo.By("Expanding current pvc")
		currentPvcSize := pvclaim.Spec.Resources.Requests[v1.ResourceStorage]
		newSize := currentPvcSize.DeepCopy()
		newSize.Add(resource.MustParse("5Gi"))
		framework.Logf("currentPvcSize %v, newSize %v", currentPvcSize, newSize)
		pvclaim.Spec.Resources.Requests[v1.ResourceStorage] = newSize

		pvclaim, err = expandPVCSize(pvclaim, newSize, client)
		framework.ExpectNoError(err, "While updating pvc for more size")
		gomega.Expect(pvclaim).NotTo(gomega.BeNil())
		pvcSize := pvclaim.Spec.Resources.Requests[v1.ResourceStorage]
		if pvcSize.Cmp(newSize) != 0 {
			framework.Failf("error updating pvc size %q", pvclaim.Name)
		}
		err = waitForEvent(ctx, client, namespace, "exceeded quota", pvclaim.Name)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Remove quota in SVC for policy(SC) - " + storagePolicyName)
		deleteResourceQuota(svcClient, svcNamespace)

		ginkgo.By("Checking for PVC request size change on SVC PVC")
		b, err := verifyPvcRequestedSizeUpdateInSupervisorWithWait(svcPVCName, newSize)
		gomega.Expect(b).To(gomega.BeTrue())
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Waiting for controller volume resize to finish")
		err = waitForPvResizeForGivenPvc(pvclaim, client, totalResizeWaitPeriod)
		framework.ExpectNoError(err, "While waiting for pvc resize to finish")

		ginkgo.By("Checking for resize on SVC PV")
		verifyPVSizeinSupervisor(svcPVCName, newSize)

		ginkgo.By("Checking for 'FileSystemResizePending' status condition on SVC PVC")
		err = waitForSvcPvcToReachFileSystemResizePendingCondition(svcPVCName, pollTimeout)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Checking for conditions on pvc")
		pvclaim, err = waitForPVCToReachFileSystemResizePendingCondition(client, namespace, pvclaim.Name, pollTimeout)
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

	})

	// Verify resize triggered when volume was online resumes when volumes
	// becomes offline.
	// Steps:
	// 1. Create StorageClass with allowVolumeExpansion set to true.
	// 2. Create a GC PVC with 1 Gi and wait for it bound in the GC.
	// 3. Extend the GC PVC to 2Gi, verify GC PVC and SVC PVC remains 1Gi and
	//    have FileSystemResizePending condition , GC PV  and SVC PV change to
	//    2Gi.
	// 4. Create a pod in GC with PVC created in step 2, wait for the pod to
	//    reach running state
	// 5. Verify GC PVC and SVC PVC size change to 2 Gi and FileSystemResizePending
	//    condition is removed.
	// 6. Extend GC PVC to 3Gi.
	// 7. Verify error message indicating that we don't support online expansion.
	// 8. Delete the pod created in step 4.
	// 9. Resize triggered in step 6 finishes and GC and SVC PVCs remain at 2Gi
	//    and have FileSystemResizePending condition, GC and SVC PVs change to 3Gi.
	// 10. Create a pod with PVC created in step 2, wait for the pod to reach
	//     running state
	// 11. Verify GC PVC and SVC PVC size change to 3 Gi and FileSystemResizePending
	//     condition is removed.
	// 12. Delete pod created in step 10.
	// 13. Delete PVC created in step 2.
	// 14. Delete SC created in step 1.
	ginkgo.It("verify resize triggered when volume was online resumes when volumes becomes offline", func() {
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
			framework.Failf("error updating pvc %q to size %v", pvclaim.Name, newSize)
		}

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

		ginkgo.By("Waiting for controller volume resize to finish")
		err = waitForPvResizeForGivenPvc(pvclaim, client, totalResizeWaitPeriod)
		framework.ExpectNoError(err, "While waiting for pvc resize to finish")

		ginkgo.By("Checking for PVC request size change on SVC PVC")
		b, err := verifyPvcRequestedSizeUpdateInSupervisorWithWait(svcPVCName, newSize)
		gomega.Expect(b).To(gomega.BeTrue())
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Checking for resize on SVC PV")
		verifyPVSizeinSupervisor(svcPVCName, newSize)

		ginkgo.By("Checking for 'FileSystemResizePending' status condition on SVC PVC")
		err = waitForSvcPvcToReachFileSystemResizePendingCondition(svcPVCName, pollTimeout)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Checking for conditions on pvc")
		pvclaim, err = waitForPVCToReachFileSystemResizePendingCondition(client, namespace, pvclaim.Name, pollTimeout)
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
		if fsSize < originalFsSize {
			framework.Failf("error updating filesystem size for %q. Resulting filesystem size is %d", pvclaim.Name, fsSize)
		}
		ginkgo.By("File system resize finished successfully in GC")
		ginkgo.By("Checking for PVC resize completion on SVC PVC")
		_, err = waitForFSResizeInSvc(svcPVCName)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

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

	/*

		// Looping resize beyond storage policy quota fails
		// Steps:
		// 1. Create StorageClass with allowVolumeExpansion set to true.
		// 2. Set a quota limit on the storage policy corresponding to SC created in
		//    step 1 in VC to 10Gi.
		// 3. Create PVC of 2Gi in GC with SC created in step 1 and wait for binding
		//    with PV.
		// 4. Create a pod in GC using the PVC created in step 3, wait for FS creation.
		// 5. Delete the pod create in step 4 in GC.
		// 6. In a loop of 10, modify PVC's size by adding 1 Gi at a time to trigger
		//    offline volume expansion.
		// 7. Check the resize operation fails since it exceeds quota.
		// 8. Check using CNS query that size has got updated to 10Gi.
		// 9. Verify size of PVs in SVC and GC are 10Gi.
		// 10. Delete PVC created in step 3.
		// 11. Delete SC created in step 1.

		// TODO: revisit in a future version

		ginkgo.It("Looping resize beyond storage policy quota fails", func() {
			quota := "10Gi"
			ginkgo.By(fmt.Sprintf("Set quota in SVC for %v on policy(SC) - %v", quota, storagePolicyName))
			svcClient, svcNamespace := getSvcClientAndNamespace()
			createResourceQuota(svcClient, svcNamespace, quota, storagePolicyName)
			defer deleteResourceQuota(svcClient, svcNamespace)

			ginkgo.By("Expanding pvc 10 times")
			currentPvcSize := pvclaim.Spec.Resources.Requests[v1.ResourceStorage]
			newSize := currentPvcSize.DeepCopy()
			quotaSize := resource.MustParse(quota)
			for i := 0; i < 10; i++ {
				newSize.Add(resource.MustParse("1Gi"))
				ginkgo.By(fmt.Sprintf("Expanding pvc to new size: %v", newSize))
				pvclaim, err := expandPVCSize(pvclaim, newSize, client)
				gomega.Expect(pvclaim).NotTo(gomega.BeNil())
				gomega.Expect(err).NotTo(gomega.HaveOccurred())

				pvcSize := pvclaim.Spec.Resources.Requests[v1.ResourceStorage]
				if pvcSize.Cmp(newSize) != 0 {
					framework.Failf("error updating pvc %q to size %v", pvclaim.Name, newSize)
				}
				ginkgo.By("Sleeping for 2 seconds...")
				time.Sleep(2 * time.Second)
			}

			ginkgo.By(fmt.Sprintf("Waiting for PV resize to %v", quotaSize))
			err = waitForPvResize(pv, client, quotaSize, totalResizeWaitPeriod)
			framework.ExpectNoError(err, "While waiting for pvc resize to finish")

			ginkgo.By("Checking for PVC request size change on SVC PVC")
			b, err := verifyPvcRequestedSizeUpdateInSupervisorWithWait(svcPVCName, quotaSize)
			gomega.Expect(b).To(gomega.BeTrue())
			gomega.Expect(err).NotTo(gomega.HaveOccurred())

			ginkgo.By("Checking for resize on SVC PV")
			verifyPVSizeinSupervisor(svcPVCName, quotaSize)

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
			newSizeInMb := convertGiStrToMibInt64(quotaSize)
			if queryResult.Volumes[0].BackingObjectDetails.(*cnstypes.CnsBlockBackingDetails).CapacityInMb != newSizeInMb {
				err = fmt.Errorf("Got wrong disk size after volume expansion")
			}
			gomega.Expect(err).NotTo(gomega.HaveOccurred())

		})
	*/

	// Verify offline block volume expansion succeeds when GC CSI pod is down
	// when SVC PVC reaches FilesystemResizePending state and GC CSI comes up.
	// Steps:
	// 1. Create a SC with allowVolumeExpansion set to 'true'.
	// 2. Create a GC PVC using the SC created in step 1 and wait for binding
	//    with PV.
	// 3. Create a pod using the pvc created in step 2 in GC and wait for FS init.
	// 4. Delete pod created in step 3 in GC.
	// 5. Resize PVC with new size.
	// 6. Check PVC in SVC is in "Resizing" state.
	// 7. Bring CSI-controller pod down in GC.
	// 8. Wait for SVC PVC to reach "FilesystemResizePending" state.
	// 9. Check Size from CNS query is same as what was used in step 5.
	// 10. Bring the CSI-controller pod up in GC.
	// 11. Wait for PVC Status Condition in GC to reach "FilesystemResizePending"
	//     state.
	// 12. Create new pod in GC to trigger FS expansion.
	// 13. Wait for new size of PVC in GC and compare with SVC PVC size for
	//     equality.
	// 14. Delete the pod created in step 11.
	// 15. Delete PVC created in step 2.
	// 16. Delete SC created in step 1.
	ginkgo.It("verify offline block volume expansion succeeds when GC CSI pod is down "+
		"when SVC PVC reaches FilesystemResizePending state and GC CSI comes up", func() {
		thickProvPolicy := os.Getenv(envStoragePolicyNameWithThickProvision)
		if thickProvPolicy == "" {
			ginkgo.Skip(envStoragePolicyNameWithThickProvision + " env variable not set")
		}

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		ginkgo.By("Creating Storage Class and PVC with allowVolumeExpansion = true")
		scParameters := make(map[string]string)
		scParameters[scParamFsType] = ext4FSType
		scParameters[svStorageClassName] = thickProvPolicy
		sc, pvc, err := createPVCAndStorageClass(client, namespace, nil, scParameters, "", nil, "", true, "")
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		var pvcs []*v1.PersistentVolumeClaim
		pvcs = append(pvcs, pvc)
		ginkgo.By("Waiting for all claims to be in bound state")
		persistentvolumes, err := fpv.WaitForPVClaimBoundPhase(client, pvcs, framework.ClaimProvisionTimeout)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		pvol := persistentvolumes[0]
		volHandleSvc := getVolumeIDFromSupervisorCluster(pvol.Spec.CSI.VolumeHandle)
		gomega.Expect(volHandleSvc).NotTo(gomega.BeEmpty())
		svcPvcName := pvol.Spec.CSI.VolumeHandle
		defer func() {
			err = fpv.DeletePersistentVolumeClaim(client, pvc.Name, namespace)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			err = e2eVSphere.waitForCNSVolumeToBeDeleted(volHandleSvc)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			ginkgo.By("Verify volume is deleted in Supervisor Cluster")
			err = waitTillVolumeIsDeletedInSvc(svcPvcName, poll, pollTimeoutShort)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			err = client.StorageV1().StorageClasses().Delete(ctx, sc.Name, *metav1.NewDeleteOptions(0))
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}()
		// Create a Pod to use this PVC, and verify volume has been attached.
		ginkgo.By("Creating pod to attach PV to the node")
		pod, err := createPod(client, namespace, nil, []*v1.PersistentVolumeClaim{pvc}, false, execCommand)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By(fmt.Sprintf("Verify volume: %s is attached to the node: %s", volHandleSvc, pod.Spec.NodeName))
		vmUUID, err := getVMUUIDFromNodeName(pod.Spec.NodeName)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		isDiskAttached, err := e2eVSphere.isVolumeAttachedToVM(client, volHandleSvc, vmUUID)
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
		ginkgo.By(fmt.Sprintf("Deleting the pod %s in namespace %s before expansion", pod.Name, namespace))
		err = fpod.DeletePodWithWait(client, pod)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Verify volume is detached from the node before expansion")
		isDiskDetached, err := e2eVSphere.waitForVolumeDetachedFromNode(client,
			pvol.Spec.CSI.VolumeHandle, pod.Spec.NodeName)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		gomega.Expect(isDiskDetached).To(gomega.BeTrue(),
			fmt.Sprintf("Volume %q is not detached from the node %q", volHandleSvc, pod.Spec.NodeName))

		// Modify PVC spec to trigger volume expansion.
		// We expand the PVC while no pod is using it to ensure offline expansion.
		ginkgo.By("Expanding current pvc")
		currentPvcSize := pvc.Spec.Resources.Requests[v1.ResourceStorage]
		newSize := currentPvcSize.DeepCopy()
		newSize.Add(resource.MustParse("3Gi"))
		framework.Logf("currentPvcSize %v, newSize %v", currentPvcSize, newSize)
		pvc, err = expandPVCSize(pvc, newSize, client)
		framework.ExpectNoError(err, "While updating pvc for more size")
		gomega.Expect(pvc).NotTo(gomega.BeNil())

		pvcSize := pvc.Spec.Resources.Requests[v1.ResourceStorage]
		if pvcSize.Cmp(newSize) != 0 {
			framework.Failf("error updating pvc size %q", pvc.Name)
		}

		ginkgo.By("Checking for PVC request size change on SVC PVC")
		boolSvcPvcRequestSizeUpdated, err := verifyPvcRequestedSizeUpdateInSupervisorWithWait(svcPvcName, newSize)
		gomega.Expect(boolSvcPvcRequestSizeUpdated).To(gomega.BeTrue())
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		// Note: In our test environment PVC resize of 3Gi with thick provision
		//       enabled VSAN policy took some time, hence PVC would still be in
		//       'Resizing' state, allowing us to perfrom subsequent steps.
		//       This may fail if the environment on which this test is run is a
		//       lot faster than our minimal test infra.
		ginkgo.By("Checking GC pvc is having 'Resizing' status condition")
		pvc, err = checkPvcHasGivenStatusCondition(client, namespace, pvc.Name, true, v1.PersistentVolumeClaimResizing)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Checking for 'Resizing' status condition on SVC PVC")
		_, err = checkSvcPvcHasGivenStatusCondition(svcPvcName, true, v1.PersistentVolumeClaimResizing)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Bringing GC CSI controller down...")
		isGCCSIDeploymentPODdown = true
		_ = updateDeploymentReplica(client, 0, vSphereCSIControllerPodNamePrefix, csiSystemNamespace)

		ginkgo.By("Waiting for SVC PV resize to complete")
		err = verifyPVSizeinSupervisorWithWait(svcPvcName, newSize)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		time.Sleep(2 * time.Second)

		ginkgo.By("Checking for 'FileSystemResizePending' status condition on SVC PVC")
		err = waitForSvcPvcToReachFileSystemResizePendingCondition(svcPvcName, pollTimeout)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Bringing GC CSI controller up...")
		_ = updateDeploymentReplica(client, 1, vSphereCSIControllerPodNamePrefix, csiSystemNamespace)
		isGCCSIDeploymentPODdown = false

		ginkgo.By("Waiting for GC PV resize to finish")
		err = waitForPvResizeForGivenPvc(pvc, client, totalResizeWaitPeriod)
		framework.ExpectNoError(err, "While waiting for pvc resize to finish")
		time.Sleep(2 * time.Second)

		ginkgo.By("Checking for conditions on pvc")
		pvc, err = checkPvcHasGivenStatusCondition(client,
			namespace, pvc.Name, true, v1.PersistentVolumeClaimFileSystemResizePending)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By(fmt.Sprintf("Invoking QueryCNSVolumeWithResult with VolumeID: %s", volHandleSvc))
		queryResult, err := e2eVSphere.queryCNSVolumeWithResult(volHandleSvc)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		gomega.Expect(len(queryResult.Volumes)).NotTo(gomega.BeZero(), "QueryCNSVolumeWithResult returned no volume")
		ginkgo.By("Verifying disk size requested in volume expansion is honored")
		newSizeInMb := convertGiStrToMibInt64(newSize)
		framework.Logf("Size in CNS: %d",
			queryResult.Volumes[0].BackingObjectDetails.(*cnstypes.CnsBlockBackingDetails).CapacityInMb)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		gomega.Expect(queryResult.Volumes[0].BackingObjectDetails.(*cnstypes.CnsBlockBackingDetails).CapacityInMb).To(
			gomega.Equal(newSizeInMb), "Got wrong disk size after volume expansion")

		// Create a new Pod to use this PVC, and verify volume has been attached.
		ginkgo.By("Creating a new pod to attach PV again to the node")
		pod, err = createPod(client, namespace, nil, []*v1.PersistentVolumeClaim{pvc}, false, execCommand)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		vmUUID, err = getVMUUIDFromNodeName(pod.Spec.NodeName)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		ginkgo.By(fmt.Sprintf("Verify volume after expansion: %s is attached to the node: %s", volHandleSvc, vmUUID))
		isDiskAttached, err = e2eVSphere.isVolumeAttachedToVM(client, volHandleSvc, vmUUID)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		gomega.Expect(isDiskAttached).To(gomega.BeTrue(), "Volume is not attached to the node")

		ginkgo.By("Verify after expansion the filesystem type is as expected")
		cmd[1] = pod.Name
		lastOutput = framework.RunKubectlOrDie(namespace, cmd...)
		gomega.Expect(strings.Contains(lastOutput, ext4FSType)).NotTo(gomega.BeFalse())

		ginkgo.By("Waiting for file system resize to finish")
		pvc, err = waitForFSResize(pvc, client)
		framework.ExpectNoError(err, "while waiting for fs resize to finish")

		pvcConditions := pvc.Status.Conditions
		expectEqual(len(pvcConditions), 0, "pvc should not have conditions")

		ginkgo.By("Verify filesystem size for mount point /mnt/volume1 after expansion")
		fsSize, err := getFSSizeMb(f, pod)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		// Filesystem size may be smaller than the size of the block volume.
		// Here since filesystem was already formatted on the original volume,
		// we can compare the new filesystem size with the original filesystem size.
		if fsSize < originalFsSize {
			framework.Failf("error updating filesystem size for %q. Resulting filesystem size is %d", pvc.Name, fsSize)
		}

		// Delete POD
		ginkgo.By(fmt.Sprintf("Deleting the pod %s in namespace %s before expansion", pod.Name, namespace))
		err = fpod.DeletePodWithWait(client, pod)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Verify volume is detached from the node before expansion")
		isDiskDetached, err = e2eVSphere.waitForVolumeDetachedFromNode(client, volHandleSvc, pod.Spec.NodeName)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		gomega.Expect(isDiskDetached).To(gomega.BeTrue(),
			fmt.Sprintf("Volume %q is not detached from the node %q", volHandleSvc, pod.Spec.NodeName))
	})

	// Verify deletion of GC PVC is successful when FCD expansion is in progress.
	// Steps:
	// 1. Create a SC with allowVolumeExpansion set to 'true'.
	// 2. Create a GC PVC using the SC created in step 1 and wait for binding
	//    with PV.
	// 3. Resize PVC in GC with new size.
	// 4. Wait for PVC Status Condition in SVC to reach "Resizing" state.
	// 5. Check FCD resize was triggered.
	// 6. Delete PVC from GC.
	// 7. Check no residues are left - PVC, PV in GC and SVC, and FCD.
	// 8. Delete SC created in step 1.
	ginkgo.It("Verify deletion of GC PVC is successful when FCD expansion is in progress", func() {
		thickProvPolicy := os.Getenv(envStoragePolicyNameWithThickProvision)
		if thickProvPolicy == "" {
			ginkgo.Skip(envStoragePolicyNameWithThickProvision + " env variable not set")
		}

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		ginkgo.By("Creating Storage Class and PVC with allowVolumeExpansion = true")
		setResourceQuota(client, namespace, rqLimit)
		scParameters := make(map[string]string)
		scParameters[scParamFsType] = ext4FSType
		scParameters[svStorageClassName] = thickProvPolicy
		sc, pvc, err := createPVCAndStorageClass(client, namespace, nil, scParameters, "", nil, "", true, "")
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		var pvcs []*v1.PersistentVolumeClaim
		pvcs = append(pvcs, pvc)
		ginkgo.By("Waiting for all claims to be in bound state")
		persistentvolumes, err := fpv.WaitForPVClaimBoundPhase(client, pvcs, framework.ClaimProvisionTimeout)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		pvol := persistentvolumes[0]
		svcPvcName := pvol.Spec.CSI.VolumeHandle
		volHandleSvc := getVolumeIDFromSupervisorCluster(svcPvcName)
		defer func() {
			err = fpv.DeletePersistentVolumeClaim(client, pvc.Name, namespace)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			err = e2eVSphere.waitForCNSVolumeToBeDeleted(volHandleSvc)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			ginkgo.By("Verify volume is deleted in Supervisor Cluster")
			err = waitTillVolumeIsDeletedInSvc(svcPvcName, poll, pollTimeoutShort)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			err = client.StorageV1().StorageClasses().Delete(ctx, sc.Name, *metav1.NewDeleteOptions(0))
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}()

		// Modify PVC spec to trigger volume expansion.
		// We expand the PVC while no pod is using it to ensure offline expansion.
		ginkgo.By("Expanding current pvc")
		currentPvcSize := pvc.Spec.Resources.Requests[v1.ResourceStorage]
		newSize := currentPvcSize.DeepCopy()
		newSize.Add(resource.MustParse("2Gi"))
		framework.Logf("currentPvcSize %v, newSize %v", currentPvcSize, newSize)
		pvc, err = expandPVCSize(pvc, newSize, client)
		framework.ExpectNoError(err, "While updating pvc for more size")
		gomega.Expect(pvc).NotTo(gomega.BeNil())

		pvcSize := pvc.Spec.Resources.Requests[v1.ResourceStorage]
		if pvcSize.Cmp(newSize) != 0 {
			framework.Failf("error updating pvc size %q", pvc.Name)
		}
		ginkgo.By("Checking for PVC request size change on SVC PVC")
		b, err := verifyPvcRequestedSizeUpdateInSupervisorWithWait(svcPvcName, newSize)
		gomega.Expect(b).To(gomega.BeTrue())
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Checking GC pvc is have 'Resizing' status condition")
		pvc, err = checkPvcHasGivenStatusCondition(client, namespace, pvc.Name, true, v1.PersistentVolumeClaimResizing)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Checking for 'Resizing' status condition on SVC PVC")
		_, err = checkSvcPvcHasGivenStatusCondition(svcPvcName, true, v1.PersistentVolumeClaimResizing)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		// PVC deletion happens in the defer block.
	})

	// Verify Online expansion triggers FS resize.
	// Steps:
	// 1. Create a SC with allowVolumeExpansion set to 'true' in GC.
	// 2. create a PVC using the SC created in step 1 in GC and wait for binding
	//    with PV.
	// 3. create a pod using the pvc created in step 2 in GC and wait FS init.
	// 5. write some data to the PV mounted on the pod.
	// 6. Resize PVC with new size in GC.
	// 7. Wait for PVC resize on corresponding SVC PVC. compare GC and SVC PV
	//    sizes are same and is equal to what we used in step 6.
	// 9. Check Size from CNS query is same as what was used in step 6.
	// 10. verify data is intact on the PV mounted on the pod and verify
	//     filesystem size.
	// 11. delete the pod created in step 10.
	// 12. delete PVC created in step 2.
	// 13. delete SC created in step 1.
	ginkgo.It("Verify Online volume expansion on dynamic PVC and check FS resize", func() {
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

		rand.New(rand.NewSource(time.Now().Unix()))
		testdataFile := fmt.Sprintf("/tmp/testdata_%v_%v", time.Now().Unix(), rand.Intn(1000))
		ginkgo.By(fmt.Sprintf("Creating a 512mb test data file %v", testdataFile))
		op, err := exec.Command("dd", "if=/dev/urandom", fmt.Sprintf("of=%v", testdataFile),
			"bs=64k", "count=8000").Output()
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		defer func() {
			op, err = exec.Command("rm", "-f", testdataFile).Output()
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}()

		_ = framework.RunKubectlOrDie(namespace, "cp", testdataFile,
			fmt.Sprintf("%v/%v:/mnt/volume1/testdata", namespace, pod.Name))

		onlineVolumeResizeCheck(f, client, namespace, svcPVCName, volHandle, pvclaim, pod)

		ginkgo.By("Checking data consistency after PVC resize")
		_ = framework.RunKubectlOrDie(namespace, "cp",
			fmt.Sprintf("%v/%v:/mnt/volume1/testdata", namespace, pod.Name), testdataFile+"_pod")
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
		_, err = waitForFSResizeInSvc(svcPVCName)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		// Delete POD.
		ginkgo.By(fmt.Sprintf("Deleting the new pod %s in namespace %s after expansion", pod.Name, namespace))
		err = fpod.DeletePodWithWait(client, pod)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Verify volume is detached from the node after expansion")
		isDiskDetached, err := e2eVSphere.waitForVolumeDetachedFromNode(client,
			pv.Spec.CSI.VolumeHandle, pod.Spec.NodeName)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		gomega.Expect(isDiskDetached).To(gomega.BeTrue(),
			fmt.Sprintf("Volume %q is not detached from the node %q", pv.Spec.CSI.VolumeHandle, pod.Spec.NodeName))
	})

	// Verify online resize of PVC concurrently with different size.
	// Steps:
	// 1. Create a SC with allowVolumeExpansion set to 'true'.
	// 2. create a PVC of 2Gi using the SC created in step 1 and wait for
	//    binding with PV.
	// 3. resize GC PVC to 1Gi, 2Gi and 8Gi in separate threads.
	// 4. Check using CNS query that size of the volume is 10Gi.
	// 5. Verify size of PVs in SVC and GC are 10Gi.
	// 6. delete the PVC, pod and SC.
	ginkgo.It("Verify online volume resize on pvc expanded concurrently with different sizes", func() {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		ginkgo.By("Verify online volume expansion when PVC resized concurrently " +
			"with different sizes expands to largest size")
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

		ginkgo.By("Expanding current pvc")
		currentPvcSize := pvclaim.Spec.Resources.Requests[v1.ResourceStorage]
		newSize1 := currentPvcSize.DeepCopy()
		newSize1.Add(resource.MustParse("1Gi"))
		newSize2 := currentPvcSize.DeepCopy()
		newSize2.Add(resource.MustParse("2Gi"))
		newSize3 := currentPvcSize.DeepCopy()
		newSize3.Add(resource.MustParse("8Gi"))

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

		ginkgo.By("Waiting for file system resize to finish")
		pvclaim, err = waitForFSResize(pvclaim, client)
		framework.ExpectNoError(err, "while waiting for fs resize to finish")

		pvcConditions := pvclaim.Status.Conditions
		expectEqual(len(pvcConditions), 0, "pvc should not have conditions")

		ginkgo.By("Verify filesystem size for mount point /mnt/volume1")
		fsSize, err := getFSSizeMb(f, pod)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		framework.Logf("FileSystemSize after PVC resize %d mb , FileSystemSize Before PVC resize %d mb ",
			fsSize, originalFsSize)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		// Filesystem size may be smaller than the size of the block volume
		// so here we are checking if the new filesystem size is greater than
		// the original volume size as the filesystem is formatted for the
		// first time.
		gomega.Expect(fsSize).Should(gomega.BeNumerically(">", 7000),
			fmt.Sprintf("error updating filesystem size for %q. Filesystem size is not more than 7GB", pvclaim.Name))

		ginkgo.By("online volume expansion is successful")
	})

	// Online volume Resize beyond storage policy quota fails.
	// Steps:
	// 1. Create a SC with allowVolumeExpansion set to 'true'.
	// 2. set a quota limit on the storage policy in VC.
	// 3. create a PVC using the SC created in step 1 and wait for binding with PV.
	// 4. Create Pod using above PVC.
	// 5. resize GC PVC to size bigger than what storage policy quota allows.
	// 6. Verify resize fails.
	// 7. Increase quota on the storage policy to allow the exapnsion.
	// 8. Verify SVC and PVC reach "FilesystemResizePending" state and PVs have
	//    the new size used in step 3.
	// 9. Check using CNS query that size has got updated to what was used in step 4.
	// 10. Verify size of PVs in SVC and GC to same as the one used in the step 4.
	// 11. delete PVC created in step 2.
	// 12. delete SC created in step 1.
	ginkgo.It("Online resize beyond storage policy quota fails", func() {
		var originalSizeInMb, fsSize int64

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		ginkgo.By("Verify online expansion when resize beyond storage policy quota fails")
		// Create a Pod to use this PVC, and verify volume has been attached.
		ginkgo.By("Creating pod to attach PV to the node")
		pod, err := createPod(client, namespace, nil, []*v1.PersistentVolumeClaim{pvclaim}, false, execCommand)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		defer func() {
			// Delete POD.
			ginkgo.By(fmt.Sprintf("Deleting the pod %s in namespace %s", pod.Name, namespace))
			err := fpod.DeletePodWithWait(client, pod)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			isDiskDetached, err := e2eVSphere.waitForVolumeDetachedFromNode(client,
				pv.Spec.CSI.VolumeHandle, pod.Spec.NodeName)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			gomega.Expect(isDiskDetached).To(gomega.BeTrue(),
				fmt.Sprintf("Volume %q is not detached from the node %q", pv.Spec.CSI.VolumeHandle, pod.Spec.NodeName))

		}()

		ginkgo.By(fmt.Sprintf("Verify volume: %s is attached to the node: %s",
			pv.Spec.CSI.VolumeHandle, pod.Spec.NodeName))
		vmUUID, err := getVMUUIDFromNodeName(pod.Spec.NodeName)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		isDiskAttached, err := e2eVSphere.isVolumeAttachedToVM(client, volHandle, vmUUID)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		gomega.Expect(isDiskAttached).To(gomega.BeTrue(), "Volume is not attached to the node")

		// Fetch original FileSystemSize.
		ginkgo.By("Verify filesystem size for mount point /mnt/volume1 before expansion")
		originalSizeInMb, err = getFSSizeMb(f, pod)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Set quota in SVC for 5Gi on policy(SC) - " + storagePolicyName)
		svcClient, svcNamespace := getSvcClientAndNamespace()
		createResourceQuota(svcClient, svcNamespace, "5Gi", storagePolicyName)
		defer deleteResourceQuota(svcClient, svcNamespace)

		// Modify PVC spec to trigger volume expansion.
		// We expand the PVC while no pod is using it to ensure offline expansion.
		ginkgo.By("Expanding current pvc")
		currentPvcSize := pvclaim.Spec.Resources.Requests[v1.ResourceStorage]
		newSize := currentPvcSize.DeepCopy()
		newSize.Add(resource.MustParse("5Gi"))
		framework.Logf("currentPvcSize %v, newSize %v", currentPvcSize, newSize)
		pvclaim.Spec.Resources.Requests[v1.ResourceStorage] = newSize

		pvclaim, err = expandPVCSize(pvclaim, newSize, client)
		framework.ExpectNoError(err, "While updating pvc for more size")
		gomega.Expect(pvclaim).NotTo(gomega.BeNil())
		pvcSize := pvclaim.Spec.Resources.Requests[v1.ResourceStorage]
		if pvcSize.Cmp(newSize) != 0 {
			framework.Failf("error updating pvc size %q", pvclaim.Name)
		}
		err = waitForEvent(ctx, client, namespace, "exceeded quota", pvclaim.Name)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Remove quota in SVC for policy(SC) - " + storagePolicyName)
		deleteResourceQuota(svcClient, svcNamespace)

		ginkgo.By("Checking for PVC request size change on SVC PVC")
		b, err := verifyPvcRequestedSizeUpdateInSupervisorWithWait(svcPVCName, newSize)
		gomega.Expect(b).To(gomega.BeTrue())
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Waiting for controller volume resize to finish")
		err = waitForPvResizeForGivenPvc(pvclaim, client, totalResizeWaitPeriod)
		framework.ExpectNoError(err, "While waiting for pvc resize to finish")

		ginkgo.By("Checking for resize on SVC PV")
		verifyPVSizeinSupervisor(svcPVCName, newSize)

		pvcConditions := pvclaim.Status.Conditions
		expectEqual(len(pvcConditions), 0, "pvc should not have conditions")

		ginkgo.By(fmt.Sprintf("Invoking QueryCNSVolumeWithResult with VolumeID: %s", volHandle))
		queryResult, err := e2eVSphere.queryCNSVolumeWithResult(volHandle)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Waiting for file system resize to finish")
		pvclaim, err = waitForFSResize(pvclaim, client)
		framework.ExpectNoError(err, "while waiting for fs resize to finish")

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

		ginkgo.By("Verify filesystem size for mount point /mnt/volume1")
		fsSize, err = getFSSizeMb(f, pod)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		// Filesystem size may be smaller than the size of the block volume
		// so here we are checking if the new filesystem size is greater than
		// the original volume size as the filesystem is formatted for the
		// first time.
		gomega.Expect(fsSize).Should(gomega.BeNumerically(">", originalSizeInMb),
			fmt.Sprintf("error updating filesystem size for %q. Resulting filesystem size is %d", pvclaim.Name, fsSize))
		ginkgo.By("File system resize finished successfully")

	})

	// This test verifies the static provisioning workflow in guest cluster when
	// svcPVC=gcPVC.
	//
	// Test Steps:
	// 1. Create FCD with valid storage policy on gc-svc.
	// 2. Create Resource quota.
	// 3. Create CNS register volume with above created FCD on SVC.
	// 4. Verify svc-PV, svc-PVC got created , check the bidirectional reference
	//    on svc.
	// 5. On GC create a gc-PV by pointing volume handle got created by static
	//    provisioning on svc-PVC (in step 4).
	// 6. On GC create a gc-PVC pointing to above created PV (step 5).
	// 7. Wait for gc-PV , gc-PVC to get bound.
	// 8. Create POD, verify the status.
	// 9. Trigger online volume expansion on gc-pvc and make sure volume
	//    expansion is successful.
	// 10. Delete all the above created PV, PVC and resource quota.
	ginkgo.It("Online volume resize on statically created PVC on guest cluster svcPVC=gcPVC", func() {
		var err error
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		curtime := time.Now().Unix()
		randomValue := rand.Int()
		val := strconv.FormatInt(int64(randomValue), 10)
		val = string(val[1:3])
		curtimestring := strconv.FormatInt(curtime, 10)
		svpvcName := "cns-pvc-" + curtimestring + val
		framework.Logf("pvc name :%s", svpvcName)
		namespace = getNamespaceToRunTests(f)

		_, storageclass, profileID := staticProvisioningPreSetUpUtil(ctx, f, client, storagePolicyName)

		// Get supvervisor cluster client.
		svcClient, svNamespace := getSvcClientAndNamespace()

		ginkgo.By("Creating FCD (CNS Volume)")
		fcdID, err := e2eVSphere.createFCDwithValidProfileID(ctx,
			"staticfcd"+curtimestring, profileID, int64(5048), defaultDatastore.Reference())
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		ginkgo.By(fmt.Sprintf("Sleeping for %v seconds to allow newly created FCD:%s to sync with pandora",
			pandoraSyncWaitTime, fcdID))
		time.Sleep(time.Duration(pandoraSyncWaitTime) * time.Second)

		ginkgo.By("Create CNS register volume with above created FCD")
		cnsRegisterVolume := getCNSRegisterVolumeSpec(ctx, svNamespace, fcdID, "", svpvcName, v1.ReadWriteOnce)
		err = createCNSRegisterVolume(ctx, restConfig, cnsRegisterVolume)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		framework.ExpectNoError(waitForCNSRegisterVolumeToGetCreated(ctx,
			restConfig, svNamespace, cnsRegisterVolume, poll, supervisorClusterOperationsTimeout))
		cnsRegisterVolumeName := cnsRegisterVolume.GetName()
		framework.Logf("CNS register volume name : %s", cnsRegisterVolumeName)

		ginkgo.By("verify created PV, PVC and check the bidirectional reference")
		svcPVC, err := svcClient.CoreV1().PersistentVolumeClaims(svNamespace).Get(ctx, svpvcName, metav1.GetOptions{})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		svcPV := getPvFromClaim(svcClient, svNamespace, svpvcName)
		verifyBidirectionalReferenceOfPVandPVC(ctx, svcClient, svcPVC, svcPV, fcdID)

		gcPVC, gcPV, pod, _ := createStaticPVandPVCandPODinGuestCluster(client, ctx, namespace, svpvcName, "5Gi",
			storageclass, v1.PersistentVolumeReclaimDelete)
		defer func() {
			ginkgo.By("Deleting the gc PVC")
			framework.ExpectNoError(fpv.DeletePersistentVolumeClaim(client, gcPVC.Name, namespace),
				"Failed to delete PVC ", gcPVC.Name)

			ginkgo.By("Deleting the gc PV")
			framework.ExpectNoError(fpv.DeletePersistentVolume(client, gcPV.Name))

			testCleanUpUtil(ctx, restConfig, client, nil, svNamespace, svcPVC.Name, svcPV.Name)

		}()

		defer func() {
			ginkgo.By("Deleting the pod")
			err = fpod.DeletePodWithWait(client, pod)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())

			ginkgo.By("Verify volume is detached from the node")
			isDiskDetached, err := e2eVSphere.waitForVolumeDetachedFromNode(client,
				gcPV.Spec.CSI.VolumeHandle, pod.Spec.NodeName)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			gomega.Expect(isDiskDetached).To(gomega.BeTrue(),
				fmt.Sprintf("Volume %q is not detached from the node %q", gcPV.Spec.CSI.VolumeHandle, pod.Spec.NodeName))

		}()

		volHandle = getVolumeIDFromSupervisorCluster(gcPV.Spec.CSI.VolumeHandle)
		framework.Logf("Volume Handle :%s", volHandle)

		onlineVolumeResizeCheck(f, client, namespace, svcPVCName, volHandle, gcPVC, pod)

	})

	// This test verifies the static provisioning workflow in guest cluster when
	// gcPVC < svcPVC.
	//
	// Test Steps:
	// 1. Create FCD with valid storage policy on gc-svc.
	// 2. Create Resource quota.
	// 3. Create CNS register volume with above created FCD on SVC.
	// 4. Verify svc-PV, svc-PVC got created , check the bidirectional reference
	//    on svc.
	// 5. On GC create a gc-PV by pointing volume handle got created by static
	//    provisioning on svc-PVC but size of gcPVC < svcPVC.
	// 6. On GC create a gc-PVC pointing to above created PV gcPVC < svcPVC(step 5).
	// 7. Wait for gc-PV , gc-PVC to get bound.
	// 8. Create POD, verify the status.
	// 9. Trigger online volume expansion on gc-pvc, Since  svc-PVC size is
	//    already greater than the gcPVC , Online expansion on gcPVC should fail.
	// 10. Delete all the above created PV, PVC and resource quota.
	ginkgo.It("Online volume resize on statically created PVC on guest cluster when gcPVC<svcPVC", func() {
		var err error
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		curtime := time.Now().Unix()
		randomValue := rand.Int()
		val := strconv.FormatInt(int64(randomValue), 10)
		val = string(val[1:3])
		curtimestring := strconv.FormatInt(curtime, 10)
		svpvcName := "cns-pvc-" + curtimestring + val
		framework.Logf("pvc name :%s", svpvcName)
		namespace = getNamespaceToRunTests(f)

		_, storageclass, profileID := staticProvisioningPreSetUpUtil(ctx, f, client, storagePolicyName)

		// Get supvervisor cluster client.
		svcClient, svNamespace := getSvcClientAndNamespace()

		ginkgo.By("Creating FCD (CNS Volume)")
		fcdID, err := e2eVSphere.createFCDwithValidProfileID(ctx,
			"staticfcd"+curtimestring, profileID, int64(5048), defaultDatastore.Reference())
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		ginkgo.By(fmt.Sprintf("Sleeping for %v seconds to allow newly created FCD:%s to sync with pandora",
			pandoraSyncWaitTime, fcdID))
		time.Sleep(time.Duration(pandoraSyncWaitTime) * time.Second)

		ginkgo.By("Create CNS register volume with above created FCD")
		cnsRegisterVolume := getCNSRegisterVolumeSpec(ctx, svNamespace, fcdID, "", svpvcName, v1.ReadWriteOnce)
		err = createCNSRegisterVolume(ctx, restConfig, cnsRegisterVolume)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		framework.ExpectNoError(waitForCNSRegisterVolumeToGetCreated(ctx,
			restConfig, svNamespace, cnsRegisterVolume, poll, supervisorClusterOperationsTimeout))
		cnsRegisterVolumeName := cnsRegisterVolume.GetName()
		framework.Logf("CNS register volume name : %s", cnsRegisterVolumeName)

		ginkgo.By("verify created PV, PVC and check the bidirectional reference")
		svcPVC, err := svcClient.CoreV1().PersistentVolumeClaims(svNamespace).Get(ctx, svpvcName, metav1.GetOptions{})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		svcPV := getPvFromClaim(svcClient, svNamespace, svpvcName)
		verifyBidirectionalReferenceOfPVandPVC(ctx, svcClient, svcPVC, svcPV, fcdID)

		gcPVC, gcPV, pod, _ := createStaticPVandPVCandPODinGuestCluster(client, ctx, namespace, svpvcName, "1Gi",
			storageclass, v1.PersistentVolumeReclaimDelete)

		defer func() {
			ginkgo.By("Deleting the gc PVC")
			framework.ExpectNoError(fpv.DeletePersistentVolumeClaim(client, gcPVC.Name, namespace),
				"Failed to delete PVC ", gcPVC.Name)

			ginkgo.By("deleting gvPV")
			framework.ExpectNoError(fpv.DeletePersistentVolume(client, gcPV.Name))

			testCleanUpUtil(ctx, restConfig, client, nil, svNamespace, svcPVC.Name, svcPV.Name)
		}()

		defer func() {
			ginkgo.By("Deleting the pod")
			err = fpod.DeletePodWithWait(client, pod)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())

			ginkgo.By("Verify volume is detached from the node")
			isDiskDetached, err := e2eVSphere.waitForVolumeDetachedFromNode(client,
				gcPV.Spec.CSI.VolumeHandle, pod.Spec.NodeName)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			gomega.Expect(isDiskDetached).To(gomega.BeTrue(),
				fmt.Sprintf("Volume %q is not detached from the node %q", gcPV.Spec.CSI.VolumeHandle, pod.Spec.NodeName))

		}()

		ginkgo.By("Verify operation will fail because svc PVC is already greater than gc PVC")
		currentPvcSize := gcPVC.Spec.Resources.Requests[v1.ResourceStorage]
		newSize := currentPvcSize.DeepCopy()
		newSize.Add(resource.MustParse("2Gi"))
		framework.Logf("currentPvcSize %v, newSize %v", currentPvcSize, newSize)
		_, err = expandPVCSize(gcPVC, newSize, client)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Filesystem resize should fail since svcPVC size is greater than gcPVC size. " +
			"Volume expansion on gcPVC will fail")
		expectedErrMsg := "greater than the requested size"
		framework.Logf("Expected failure message: %+q", expectedErrMsg)
		err = waitForEvent(ctx, client, namespace, expectedErrMsg, gcPVC.Name)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

	})

	// This test verifies the static provisioning workflow in guest cluster when
	// svcPVC<gcPVC.
	//
	// Test Steps:
	// 1. Create FCD with valid storage policy on gc-svc.
	// 2. Create Resource quota.
	// 3. Create CNS register volume with above created FCD on SVC.
	// 4. verify svc-PV, svc-PVC got created , check the bidirectional reference
	//    on svc.
	// 5. On GC create a gc-PV by pointing volume handle got created by static
	//    provisioning on svc-PVC but size of gcPVC > svcPVC.
	// 6. On GC create a gc-PVC pointing to above created PV gcPVC > svcPVC(step 5).
	// 7. Wait for gc-PV , gc-PVC to get bound.
	// 8. Create POD, verify the status.
	// 9. Trigger online volume expansion on gc-pvc, Online expansion on gcPVC
	//    should be successful.
	// 10. Delete all the above created PV, PVC and resource quota.
	ginkgo.It("Online volume resize on statically created PVC on guest cluster when svcPVC<gcPVC", func() {
		var err error
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		curtime := time.Now().Unix()
		randomValue := rand.Int()
		val := strconv.FormatInt(int64(randomValue), 10)
		val = string(val[1:3])
		curtimestring := strconv.FormatInt(curtime, 10)
		svpvcName := "cns-pvc-" + curtimestring + val
		framework.Logf("pvc name :%s", svpvcName)
		namespace = getNamespaceToRunTests(f)

		_, storageclass, profileID := staticProvisioningPreSetUpUtil(ctx, f, client, storagePolicyName)

		// Get supvervisor cluster client.
		svcClient, svNamespace := getSvcClientAndNamespace()

		ginkgo.By("Creating FCD (CNS Volume)")
		fcdID, err := e2eVSphere.createFCDwithValidProfileID(ctx,
			"staticfcd"+curtimestring, profileID, int64(5048), defaultDatastore.Reference())
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		ginkgo.By(fmt.Sprintf("Sleeping for %v seconds to allow newly created FCD:%s to sync with pandora",
			pandoraSyncWaitTime, fcdID))
		time.Sleep(time.Duration(pandoraSyncWaitTime) * time.Second)

		ginkgo.By("Create CNS register volume with above created FCD")
		cnsRegisterVolume := getCNSRegisterVolumeSpec(ctx, svNamespace, fcdID, "", svpvcName, v1.ReadWriteOnce)
		err = createCNSRegisterVolume(ctx, restConfig, cnsRegisterVolume)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		framework.ExpectNoError(waitForCNSRegisterVolumeToGetCreated(ctx,
			restConfig, svNamespace, cnsRegisterVolume, poll, supervisorClusterOperationsTimeout))
		cnsRegisterVolumeName := cnsRegisterVolume.GetName()
		framework.Logf("CNS register volume name : %s", cnsRegisterVolumeName)

		ginkgo.By("verify created PV, PVC and check the bidirectional reference")
		svcPVC, err := svcClient.CoreV1().PersistentVolumeClaims(svNamespace).Get(ctx, svpvcName, metav1.GetOptions{})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		svcPV := getPvFromClaim(svcClient, svNamespace, svpvcName)
		verifyBidirectionalReferenceOfPVandPVC(ctx, svcClient, svcPVC, svcPV, fcdID)

		gcPVC, gcPV, pod, _ := createStaticPVandPVCandPODinGuestCluster(client, ctx, namespace, svpvcName, "7Gi",
			storageclass, v1.PersistentVolumeReclaimDelete)

		defer func() {
			ginkgo.By("Deleting the gc PVC")
			framework.ExpectNoError(fpv.DeletePersistentVolumeClaim(client, gcPVC.Name, namespace),
				"Failed to delete PVC ", gcPVC.Name)

			ginkgo.By("deleting gvPV")
			framework.ExpectNoError(fpv.DeletePersistentVolume(client, gcPV.Name))

			testCleanUpUtil(ctx, restConfig, client, nil, svNamespace, svcPVC.Name, svcPV.Name)
		}()

		defer func() {
			ginkgo.By("Deleting the pod")
			err = fpod.DeletePodWithWait(client, pod)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())

			ginkgo.By("Verify volume is detached from the node")
			isDiskDetached, err := e2eVSphere.waitForVolumeDetachedFromNode(client,
				gcPV.Spec.CSI.VolumeHandle, pod.Spec.NodeName)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			gomega.Expect(isDiskDetached).To(gomega.BeTrue(),
				fmt.Sprintf("Volume %q is not detached from the node %q", gcPV.Spec.CSI.VolumeHandle, pod.Spec.NodeName))

		}()

		volHandle = getVolumeIDFromSupervisorCluster(gcPV.Spec.CSI.VolumeHandle)
		framework.Logf("Volume Handle :%s", volHandle)

		onlineVolumeResizeCheck(f, client, namespace, svcPVCName, volHandle, gcPVC, pod)

	})

	//  Verify Online block volume expansion triggered when SVC CSI pod is down succeeds once SVC CSI pod comes up.
	//    Steps:
	// 	   1. Create a SC with allowVolumeExpansion set to 'true' in GC.
	// 	   2. create a PVC using the SC created in step 1 in GC and wait for binding
	// 	      with PV.
	// 	   3. create a pod using the pvc created in step 2 in GC and wait FS init.
	//     4. bring CSI-controller pod down in SVC.
	//     5. Resize PVC with new size in GC.
	//     6. check for retries in GC.
	//     7. PVC in GC is in "Resizing" state and PVC in SVC has no state related
	// 	      to expansion.
	// 	   8. bring the CSI-controller pod up in SVC.
	//     9. wait for PVC Status Condition changed to "FilesystemResizePending"
	//        in GC.
	//     10. Check Size from CNS query is same as what was used in step 7.
	//     11. wait for new size of PVC in GC and compare with SVC PVC size for
	//         equality.
	//     12. delete the pod, pvc,sc

	ginkgo.It("verify online block volume expansion triggered when SVC CSI pod is down"+
		"succeeds once SVC CSI pod comes up", func() {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
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
			ginkgo.By(fmt.Sprintf("Deleting the pod %s in namespace %s", pod.Name, namespace))
			err = fpod.DeletePodWithWait(client, pod)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())

			ginkgo.By("Verify volume is detached from the node after expansion")
			isDiskDetached, err := e2eVSphere.waitForVolumeDetachedFromNode(client,
				pv.Spec.CSI.VolumeHandle, pod.Spec.NodeName)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			gomega.Expect(isDiskDetached).To(gomega.BeTrue(), fmt.Sprintf("Volume %q is not detached from the node %q",
				pv.Spec.CSI.VolumeHandle, pod.Spec.NodeName))
		}()

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

		pvclaim, err = client.CoreV1().PersistentVolumeClaims(namespace).Get(ctx, pvclaim.Name, metav1.GetOptions{})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		gomega.Expect(pvclaim).NotTo(gomega.BeNil())
		pvclaim, err = checkPvcHasGivenStatusCondition(client,
			namespace, pvclaim.Name, true, v1.PersistentVolumeClaimResizing)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		gomega.Expect(pvclaim).NotTo(gomega.BeNil())
		_, err = checkSvcPvcHasGivenStatusCondition(svcPVCName, false, "")
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Bringing SVC CSI controller up...")
		svcCsiDeployment = updateDeploymentReplica(svcClient, 1, vSphereCSIControllerPodNamePrefix, csiSystemNamespace)

		ginkgo.By("Waiting for controller volume resize to finish")
		err = waitForPvResizeForGivenPvc(pvclaim, client, totalResizeWaitPeriod)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Checking for PVC request size change on SVC PVC")
		b, err := verifyPvcRequestedSizeUpdateInSupervisorWithWait(svcPVCName, newSize)
		gomega.Expect(b).To(gomega.BeTrue())
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Checking for resize on SVC PV")
		verifyPVSizeinSupervisor(svcPVCName, newSize)

		ginkgo.By("Checking for 'FileSystemResizePending' status condition on SVC PVC")
		err = waitForSvcPvcToReachFileSystemResizePendingCondition(svcPVCName, pollTimeout)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Checking for conditions on pvc")
		pvclaim, err = waitForPVCToReachFileSystemResizePendingCondition(client, namespace, pvclaim.Name, pollTimeout)
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

		ginkgo.By("Verify after expansion the filesystem type is as expected")
		cmd[1] = pod.Name
		lastOutput = framework.RunKubectlOrDie(namespace, cmd...)
		gomega.Expect(strings.Contains(lastOutput, ext4FSType)).NotTo(gomega.BeFalse())

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
		if fsSize < originalFsSize {
			framework.Failf("error updating filesystem size for %q. Resulting filesystem size is %d", pvclaim.Name, fsSize)
		}

		ginkgo.By("File system resize finished successfully in GC")
		ginkgo.By("Checking for PVC resize completion on SVC PVC")
		_, err = waitForFSResizeInSvc(svcPVCName)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

	})

	// Verify online block volume expansion succeeds when GC CSI pod is down

	//  Steps:
	//  1. Create a SC with allowVolumeExpansion set to 'true'.
	//  2. Create a GC PVC using the SC created in step 1 and wait for binding
	//     with PV.
	//  3. Create a pod using the pvc created in step 2 in GC and wait for FS init.
	//  4. Resize PVC with new size.
	//  5. Bring CSI-controller pod down in GC.
	//  6. Check PVC in SVC is in "FilesystemResizePending" state.
	//  7. Check Size from CNS query is same as what was used in step 5.
	//  8. Bring the CSI-controller pod up in GC.
	//  9. Wait for new size of PVC in GC and compare with SVC PVC size
	//     for equality.
	//  10. Delete POD, PVC, SC

	ginkgo.It("verify Online block volume expansion succeeds when GC CSI pod is "+
		"down when SVC PVC reaches FilesystemResizePending state and GC CSI comes up", func() {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		// Create a POD to use this PVC, and verify volume has been attached.
		pvclaim, err = client.CoreV1().PersistentVolumeClaims(namespace).Get(ctx, pvclaim.Name, metav1.GetOptions{})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

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
			ginkgo.By(fmt.Sprintf("Deleting the pod %s in namespace %s", pod.Name, namespace))
			err = fpod.DeletePodWithWait(client, pod)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())

			ginkgo.By("Verify volume is detached from the node after expansion")
			isDiskDetached, err := e2eVSphere.waitForVolumeDetachedFromNode(client,
				pv.Spec.CSI.VolumeHandle, pod.Spec.NodeName)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			gomega.Expect(isDiskDetached).To(gomega.BeTrue(), fmt.Sprintf("Volume %q is not detached from the node %q",
				pv.Spec.CSI.VolumeHandle, pod.Spec.NodeName))
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

		pvclaim, err = client.CoreV1().PersistentVolumeClaims(namespace).Get(ctx, pvclaim.Name, metav1.GetOptions{})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		gomega.Expect(pvclaim).NotTo(gomega.BeNil())

		//  Note: This may fail if the environment on which this test is run is a
		//  lot faster than our minimal test infra.
		ginkgo.By("Checking GC pvc is having 'Resizing' status condition")
		pvclaim, err = checkPvcHasGivenStatusCondition(client, namespace,
			pvclaim.Name, true, v1.PersistentVolumeClaimResizing)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		gomega.Expect(pvclaim).NotTo(gomega.BeNil())

		ginkgo.By("Bringing GC CSI controller down...")
		isGCCSIDeploymentPODdown = true
		_ = updateDeploymentReplica(client, 0, vSphereCSIControllerPodNamePrefix, csiSystemNamespace)

		defer func() {
			_ = updateDeploymentReplica(client, 1, vSphereCSIControllerPodNamePrefix, csiSystemNamespace)
			isGCCSIDeploymentPODdown = false
		}()

		ginkgo.By("Checking for 'FileSystemResizePending' status condition on SVC PVC")
		err = waitForSvcPvcToReachFileSystemResizePendingCondition(svcPVCName, pollTimeout)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Bringing GC CSI controller up...")
		_ = updateDeploymentReplica(client, 1, vSphereCSIControllerPodNamePrefix, csiSystemNamespace)
		isGCCSIDeploymentPODdown = false

		ginkgo.By("Waiting for GC PV resize to finish")
		err = waitForPvResizeForGivenPvc(pvclaim, client, totalResizeWaitPeriod)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By(fmt.Sprintf("Invoking QueryCNSVolumeWithResult with VolumeID: %s", volHandle))
		queryResult, err := e2eVSphere.queryCNSVolumeWithResult(volHandle)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		gomega.Expect(len(queryResult.Volumes)).NotTo(gomega.BeZero(), "QueryCNSVolumeWithResult returned no volume")
		ginkgo.By("Verifying disk size requested in volume expansion is honored")
		newSizeInMb := convertGiStrToMibInt64(newSize)
		framework.Logf("Size in CNS: %d",
			queryResult.Volumes[0].BackingObjectDetails.(*cnstypes.CnsBlockBackingDetails).CapacityInMb)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		gomega.Expect(queryResult.Volumes[0].BackingObjectDetails.(*cnstypes.CnsBlockBackingDetails).CapacityInMb).To(
			gomega.Equal(newSizeInMb), "Got wrong disk size after volume expansion")

		ginkgo.By("Verify after expansion the filesystem type is as expected")
		cmd[1] = pod.Name
		lastOutput = framework.RunKubectlOrDie(namespace, cmd...)
		gomega.Expect(strings.Contains(lastOutput, ext4FSType)).NotTo(gomega.BeFalse())

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
			fmt.Sprintf("error updating filesystem size for %q. Resulting filesystem size is %d",
				pvclaim.Name, fsSize))
		ginkgo.By("File system resize finished successfully")

	})

})

func resize(client clientset.Interface, pvc *v1.PersistentVolumeClaim,
	currentPvcSize resource.Quantity, newSize resource.Quantity, wg *sync.WaitGroup) {
	defer wg.Done()
	framework.Logf("currentPvcSize %v, newSize %v", currentPvcSize, newSize)
	_, err := expandPVCSize(pvc, newSize, client)
	framework.Logf("Error from expansion attempt on pvc %v, to %v: %v", pvc.Name, newSize, err)
}

// verifyPVCRequestedSizeInSupervisor compares
// Pvc.Spec.Resources.Requests[v1.ResourceStorage] with given size in SVC.
func verifyPVCRequestedSizeInSupervisor(pvcName string, size resource.Quantity) bool {
	SvcPvc := getPVCFromSupervisorCluster(pvcName)
	pvcSize := SvcPvc.Spec.Resources.Requests[v1.ResourceStorage]
	framework.Logf("SVC PVC requested size: %v", pvcSize)
	return pvcSize.Cmp(size) == 0
}

// verifyPvcRequestedSizeUpdateInSupervisorWithWait waits till
// Pvc.Spec.Resources.Requests[v1.ResourceStorage] matches with given size
// in SVC.
func verifyPvcRequestedSizeUpdateInSupervisorWithWait(pvcName string, size resource.Quantity) (bool, error) {
	var b bool

	waitErr := wait.PollImmediate(resizePollInterval, totalResizeWaitPeriod, func() (bool, error) {
		b = verifyPVCRequestedSizeInSupervisor(pvcName, size)
		return b, nil
	})
	return b, waitErr
}

// verifyResizeCompletedInSupervisor checks if resize relates status conditions
// are removed from PVC and Pvc.Spec.Resources.Requests[v1.ResourceStorage] is
// same as pvc.Status.Capacity[v1.ResourceStorage] in SVC.
func verifyResizeCompletedInSupervisor(pvcName string) bool {
	pvc := getPVCFromSupervisorCluster(pvcName)
	pvcRequestedSize := pvc.Spec.Resources.Requests[v1.ResourceStorage]
	pvcCapacitySize := pvc.Status.Capacity[v1.ResourceStorage]
	// If pvc's capacity size is greater than or equal to pvc's request size
	// then done.
	if pvcCapacitySize.Cmp(pvcRequestedSize) < 0 {
		return false
	}
	if len(pvc.Status.Conditions) == 0 {
		return true
	}
	return false
}

// checkSvcPvcHasGivenStatusCondition checks if the status condition in SVC PVC
// matches with the one we want.
func checkSvcPvcHasGivenStatusCondition(pvcName string, conditionsPresent bool,
	condition v1.PersistentVolumeClaimConditionType) (*v1.PersistentVolumeClaim, error) {
	svcClient, svcNamespace := getSvcClientAndNamespace()
	return checkPvcHasGivenStatusCondition(svcClient, svcNamespace, pvcName, conditionsPresent, condition)
}

// waitForSvcPvcToReachFileSystemResizePendingCondition waits for SVC PVC to
// reach FileSystemResizePendingCondition status condition.
func waitForSvcPvcToReachFileSystemResizePendingCondition(svcPvcName string, timeout time.Duration) error {
	waitErr := wait.PollImmediate(resizePollInterval, timeout, func() (bool, error) {
		_, err := checkSvcPvcHasGivenStatusCondition(svcPvcName, true, v1.PersistentVolumeClaimFileSystemResizePending)
		if err == nil {
			return true, nil
		}
		if strings.Contains(err.Error(), "not matching") {
			return false, nil
		}
		return false, err
	})
	return waitErr
}

// verifyPVSizeinSupervisor compares PV.Spec.Capacity[v1.ResourceStorage] in
// SVC with the size passed here.
func verifyPVSizeinSupervisor(svcPVCName string, newSize resource.Quantity) {
	svcPV := getPvFromSupervisorCluster(svcPVCName)
	svcPVSize := svcPV.Spec.Capacity[v1.ResourceStorage]
	// If pv size is greater or equal to requested size that means controller
	// resize is finished.
	gomega.Expect(svcPVSize.Cmp(newSize) >= 0).To(gomega.BeTrue())
}

func verifyPVSizeinSupervisorWithWait(svcPVCName string, newSize resource.Quantity) error {
	waitErr := wait.PollImmediate(resizePollInterval, totalResizeWaitPeriod, func() (bool, error) {
		svcPV := getPvFromSupervisorCluster(svcPVCName)
		svcPVSize := svcPV.Spec.Capacity[v1.ResourceStorage]
		return svcPVSize.Cmp(newSize) >= 0, nil
	})
	return waitErr
}

// waitForPVCToReachFileSystemResizePendingCondition waits for PVC to reach
// FileSystemResizePendingCondition status condition.
func waitForPVCToReachFileSystemResizePendingCondition(client clientset.Interface,
	namespace string, pvcName string, timeout time.Duration) (*v1.PersistentVolumeClaim, error) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	var pvclaim *v1.PersistentVolumeClaim
	var err error

	waitErr := wait.PollImmediate(resizePollInterval, timeout, func() (bool, error) {
		pvclaim, err = client.CoreV1().PersistentVolumeClaims(namespace).Get(ctx, pvcName, metav1.GetOptions{})
		if err != nil {
			return false, fmt.Errorf("error while fetching pvc '%v' after controller resize: %v", pvcName, err)
		}
		gomega.Expect(pvclaim).NotTo(gomega.BeNil())

		inProgressConditions := pvclaim.Status.Conditions
		// If there are conditions on the PVC, it must be of
		// FileSystemResizePending type.
		if len(inProgressConditions) > 0 {
			expectEqual(len(inProgressConditions), 1, fmt.Sprintf("PVC '%v' has more than one status conditions", pvcName))
			if inProgressConditions[0].Type == v1.PersistentVolumeClaimFileSystemResizePending {
				return true, nil
			}
			gomega.Expect(inProgressConditions[0].Type == v1.PersistentVolumeClaimResizing).To(gomega.BeTrue(),
				fmt.Sprintf("PVC '%v' is not in 'Resizing' or 'FileSystemResizePending' status condition", pvcName))
		} else {
			return false, fmt.Errorf(
				"resize was not triggered on PVC '%v' or no status conditions related to resizing found on it", pvcName)
		}
		return false, nil
	})
	return pvclaim, waitErr
}

// checkPvcHasGivenStatusCondition checks if the status condition in PVC
// matches with the one we want.
func checkPvcHasGivenStatusCondition(client clientset.Interface, namespace string, pvcName string,
	conditionsPresent bool, condition v1.PersistentVolumeClaimConditionType) (*v1.PersistentVolumeClaim, error) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	pvclaim, err := client.CoreV1().PersistentVolumeClaims(namespace).Get(ctx, pvcName, metav1.GetOptions{})
	gomega.Expect(err).NotTo(gomega.HaveOccurred())
	gomega.Expect(pvclaim).NotTo(gomega.BeNil())
	inProgressConditions := pvclaim.Status.Conditions

	if len(inProgressConditions) == 0 {
		if conditionsPresent {
			return pvclaim, fmt.Errorf("no status conditions found on PVC: %v", pvcName)
		}
		return pvclaim, nil
	}
	expectEqual(len(inProgressConditions), 1, fmt.Sprintf("PVC '%v' has more than one status condition", pvcName))
	if inProgressConditions[0].Type != condition {
		return pvclaim, fmt.Errorf(
			"status condition found on PVC '%v' is '%v', and is not matching with expected status condition '%v'",
			pvcName, inProgressConditions[0].Type, condition)
	}
	return pvclaim, nil
}

// convertGiStrToMibInt64 returns integer numbers of Mb equivalent to string
// of the form \d+Gi.
func convertGiStrToMibInt64(size resource.Quantity) int64 {
	r, err := regexp.Compile("[0-9]+")
	gomega.Expect(err).NotTo(gomega.HaveOccurred())
	sizeInt, err := strconv.Atoi(r.FindString(size.String()))
	gomega.Expect(err).NotTo(gomega.HaveOccurred())
	return int64(sizeInt * 1024)
}

// waitForFSResize waits for the filesystem in the pv to be resized.
func waitForFSResizeInSvc(svcPVCName string) (*v1.PersistentVolumeClaim, error) {
	svcClient, _ := getSvcClientAndNamespace()
	svcPvc := getPVCFromSupervisorCluster(svcPVCName)
	return waitForFSResize(svcPvc, svcClient)
}

// onlineVolumeResizeCheck this method increases the PVC size, which is attached
// to POD.
func onlineVolumeResizeCheck(f *framework.Framework, client clientset.Interface,
	namespace string, svcPVCName string, volHandle string, pvclaim *v1.PersistentVolumeClaim, pod *v1.Pod) {
	var originalSizeInMb int64
	var err error
	// Fetch original FileSystemSize.
	ginkgo.By("Verify filesystem size for mount point /mnt/volume1 before expansion")
	originalSizeInMb, err = getFSSizeMb(f, pod)
	gomega.Expect(err).NotTo(gomega.HaveOccurred())

	// Resize PVC.
	// Modify PVC spec to trigger volume expansion.
	ginkgo.By("Expanding current pvc")
	currentPvcSize := pvclaim.Spec.Resources.Requests[v1.ResourceStorage]
	newSize := currentPvcSize.DeepCopy()
	newSize.Add(resource.MustParse("1Gi"))
	framework.Logf("currentPvcSize %v, newSize %v", currentPvcSize, newSize)
	pvclaim, err = expandPVCSize(pvclaim, newSize, client)
	gomega.Expect(pvclaim).NotTo(gomega.BeNil())
	gomega.Expect(err).NotTo(gomega.HaveOccurred())

	_, err = waitForFSResizeInSvc(svcPVCName)
	gomega.Expect(err).NotTo(gomega.HaveOccurred())

	ginkgo.By("Waiting for file system resize to finish")
	pvclaim, err = waitForFSResize(pvclaim, client)
	framework.ExpectNoError(err, "while waiting for fs resize to finish")

	pvcConditions := pvclaim.Status.Conditions
	expectEqual(len(pvcConditions), 0, "pvc should not have conditions")

	ginkgo.By(fmt.Sprintf("Invoking QueryCNSVolumeWithResult with VolumeID: %s", volHandle))
	queryResult, err := e2eVSphere.queryCNSVolumeWithResult(volHandle)
	gomega.Expect(err).NotTo(gomega.HaveOccurred())

	if len(queryResult.Volumes) == 0 {
		err = fmt.Errorf("QueryCNSVolumeWithResult returned no volume")
	}
	gomega.Expect(err).NotTo(gomega.HaveOccurred())

	var fsSize int64
	ginkgo.By("Verify filesystem size for mount point /mnt/volume1")
	fsSize, err = getFSSizeMb(f, pod)
	gomega.Expect(err).NotTo(gomega.HaveOccurred())
	framework.Logf("File system size after expansion : %s", fsSize)
	// Filesystem size may be smaller than the size of the block volume
	// so here we are checking if the new filesystem size is greater than
	// the original volume size as the filesystem is formatted for the
	// first time.
	gomega.Expect(fsSize).Should(gomega.BeNumerically(">", originalSizeInMb),
		fmt.Sprintf("error updating filesystem size for %q. Resulting filesystem size is %d", pvclaim.Name, fsSize))
	ginkgo.By("File system resize finished successfully")

	framework.Logf("Online volume expansion in GC PVC is successful")

}

// createStaticPVandPVCandPODinGuestCluster creates static PV and PVC in guest cluster.
func createStaticPVandPVCandPODinGuestCluster(client clientset.Interface,
	ctx context.Context, namespace string, svpvcName string, size string, storageclass *storagev1.StorageClass,
	pvReclaimPolicy v1.PersistentVolumeReclaimPolicy) (*v1.PersistentVolumeClaim, *v1.PersistentVolume, *v1.Pod, string) {

	gcPVC, gcPV := createStaticPVandPVCinGuestCluster(client, ctx, namespace,
		svpvcName, size, storageclass, pvReclaimPolicy)
	// Create a Pod to use this PVC, and verify volume has been attached.
	ginkgo.By("Creating pod to attach PV to the node")
	pod, err := createPod(client, namespace, nil, []*v1.PersistentVolumeClaim{gcPVC}, false, execCommand)
	gomega.Expect(err).NotTo(gomega.HaveOccurred())

	ginkgo.By(fmt.Sprintf("Verify volume: %s is attached to the node: %s",
		gcPV.Spec.CSI.VolumeHandle, pod.Spec.NodeName))
	vmUUID, err := getVMUUIDFromNodeName(pod.Spec.NodeName)
	gomega.Expect(err).NotTo(gomega.HaveOccurred())
	volHandle := getVolumeIDFromSupervisorCluster(gcPV.Spec.CSI.VolumeHandle)
	isDiskAttached, err := e2eVSphere.isVolumeAttachedToVM(client, volHandle, vmUUID)
	gomega.Expect(err).NotTo(gomega.HaveOccurred())
	gomega.Expect(isDiskAttached).To(gomega.BeTrue(), "Volume is not attached to the node")

	return gcPVC, gcPV, pod, vmUUID

}

// createStaticPVandPVCinGuestCluster creates static PV and PVC in guest cluster.
func createStaticPVandPVCinGuestCluster(client clientset.Interface, ctx context.Context, namespace string,
	svpvcName string, size string, storageclass *storagev1.StorageClass,
	pvReclaimPolicy v1.PersistentVolumeReclaimPolicy) (*v1.PersistentVolumeClaim, *v1.PersistentVolume) {
	ginkgo.By("Creating PV in guest cluster")
	gcPV := getPersistentVolumeSpecWithStorageclass(svpvcName, pvReclaimPolicy,
		storageclass.Name, nil, size)
	gcPV, err := client.CoreV1().PersistentVolumes().Create(ctx, gcPV, metav1.CreateOptions{})
	gomega.Expect(err).NotTo(gomega.HaveOccurred())
	gcPVName := gcPV.GetName()
	framework.Logf("PV name in GC : %s", gcPVName)
	err = fpv.WaitForPersistentVolumePhase("Available", client, gcPVName, poll, pollTimeout)
	gomega.Expect(err).NotTo(gomega.HaveOccurred())

	ginkgo.By("Creating PVC in guest cluster")
	gcPVC := getPVCSpecWithPVandStorageClass(svpvcName, namespace, nil, gcPVName, storageclass.Name, size)
	gcPVC, err = client.CoreV1().PersistentVolumeClaims(namespace).Create(ctx, gcPVC, metav1.CreateOptions{})
	gomega.Expect(err).NotTo(gomega.HaveOccurred())

	ginkgo.By("Waiting for claim to be in bound phase")
	err = fpv.WaitForPersistentVolumeClaimPhase(v1.ClaimBound, client,
		namespace, gcPVC.Name, framework.Poll, framework.ClaimProvisionTimeout)
	gomega.Expect(err).NotTo(gomega.HaveOccurred())

	framework.Logf("PVC name in GC : %s", gcPVC.GetName())

	return gcPVC, gcPV

}
