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
	"time"

	"github.com/onsi/ginkgo"
	"github.com/onsi/gomega"
	v1 "k8s.io/api/core/v1"
	storagev1 "k8s.io/api/storage/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	clientset "k8s.io/client-go/kubernetes"
	"k8s.io/kubernetes/test/e2e/framework"
)

/*

Test to verify fstype specified in storage-class is being honored after volume creation.

Steps
1. Create StorageClass with fstype set to valid type (default case included).
2. Create PVC which uses the StorageClass created in step 1.
3. Wait for PV to be provisioned.
4. Wait for PVC's status to become Bound.
5. Create pod using PVC on specific node.
6. Wait for Disk to be attached to the node.
7. Execute command in the pod to get fstype.
8. Delete pod and Wait for Volume Disk to be detached from the Node.
9. Delete PVC, PV and Storage Class.

Test to verify if an invalid fstype specified in storage class fails pod creation.

 Steps
 1. Create StorageClass with inavlid.
 2. Create PVC which uses the StorageClass created in step 1.
 3. Wait for PV to be provisioned.
 4. Wait for PVC's status to become Bound.
 5. Create pod using PVC.
 6. Verify if the pod creation fails.
 7. Verify if the MountVolume.MountDevice fails because it is unable to find the file system executable file on the node.
*/

var _ = ginkgo.Describe("[csi-block-e2e] [csi-common-e2e] Volume Filesystem Type Test", func() {
	f := framework.NewDefaultFramework("volume-fstype")
	var (
		client                clientset.Interface
		namespace             string
		isK8SVanillaTestSetup bool
		storagePolicyName     string
		profileID             string
	)
	ginkgo.BeforeEach(func() {
		client = f.ClientSet
		isK8SVanillaTestSetup = GetAndExpectBoolEnvVar(envK8SVanillaTestSetup)
		if isK8SVanillaTestSetup {
			namespace = f.Namespace.Name
		} else {
			namespace = GetAndExpectStringEnvVar(envSupervisorClusterNamespace)
		}
		bootstrap()
		nodeList := framework.GetReadySchedulableNodesOrDie(f.ClientSet)
		if !(len(nodeList.Items) > 0) {
			framework.Failf("Unable to find ready and schedulable Node")
		}
		isK8SVanillaTestSetup = GetAndExpectBoolEnvVar(envK8SVanillaTestSetup)
		if !isK8SVanillaTestSetup {
			ginkgo.By("CNS_TEST: Running for WCP setup")
			storagePolicyName = GetAndExpectStringEnvVar(envStoragePolicyNameForSharedDatastores)
			profileID = e2eVSphere.GetSpbmPolicyID(storagePolicyName)
			// create resource quota
			createResourceQuota(client, namespace, rqLimit, storagePolicyName)
		}
	})
	ginkgo.AfterEach(func() {
		if !isK8SVanillaTestSetup {
			deleteResourceQuota(client, namespace)
		}
	})

	ginkgo.It("CSI - verify fstype - ext3 formatted volume", func() {
		invokeTestForFstype(f, client, namespace, ext3FSType, ext3FSType, isK8SVanillaTestSetup, storagePolicyName, profileID)
	})

	ginkgo.It("CSI - verify fstype - default value should be ext4", func() {
		invokeTestForFstype(f, client, namespace, "", ext4FSType, isK8SVanillaTestSetup, storagePolicyName, profileID)
	})

	ginkgo.It("CSI - verify invalid fstype", func() {
		invokeTestForInvalidFstype(f, client, namespace, invalidFSType, isK8SVanillaTestSetup, storagePolicyName, profileID)
	})
})

func invokeTestForFstype(f *framework.Framework, client clientset.Interface, namespace string, fstype string, expectedContent string, isK8SVanillaTestSetup bool, storagePolicyName string, profileID string) {
	ginkgo.By(fmt.Sprintf("Invoking Test for fstype: %s", fstype))
	scParameters := make(map[string]string)
	scParameters["fstype"] = fstype
	// Create Storage class and PVC
	ginkgo.By("Creating Storage Class With Fstype")
	var storageclass *storagev1.StorageClass
	var pvclaim *v1.PersistentVolumeClaim
	var err error
	// decide which test setup is available to run
	if isK8SVanillaTestSetup {
		ginkgo.By("CNS_TEST: Running for vanilla k8s setup")
		storageclass, pvclaim, err = createPVCAndStorageClass(client, namespace, nil, scParameters, "", nil, "")
	} else {
		ginkgo.By("CNS_TEST: Running for WCP setup")
		scParameters[scParamStoragePolicyID] = profileID
		storageclass, pvclaim, err = createPVCAndStorageClass(client, namespace, nil, scParameters, "", nil, "", storagePolicyName)
	}

	gomega.Expect(err).NotTo(gomega.HaveOccurred())
	defer client.StorageV1().StorageClasses().Delete(storageclass.Name, nil)

	// Waiting for PVC to be bound
	var pvclaims []*v1.PersistentVolumeClaim
	pvclaims = append(pvclaims, pvclaim)
	ginkgo.By("Waiting for all claims to be in bound state")
	persistentvolumes, err := framework.WaitForPVClaimBoundPhase(client, pvclaims, framework.ClaimProvisionTimeout)
	gomega.Expect(err).NotTo(gomega.HaveOccurred())

	// Create a POD to use this PVC, and verify volume has been attached
	ginkgo.By("Creating pod to attach PV to the node")
	pod, err := framework.CreatePod(client, namespace, nil, []*v1.PersistentVolumeClaim{pvclaim}, false, execCommand)
	gomega.Expect(err).NotTo(gomega.HaveOccurred())

	pv := persistentvolumes[0]
	var vmUUID string
	var exists bool
	ginkgo.By(fmt.Sprintf("Verify volume: %s is attached to the node: %s", pv.Spec.CSI.VolumeHandle, pod.Spec.NodeName))
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	if isK8SVanillaTestSetup {
		vmUUID = getNodeUUID(client, pod.Spec.NodeName)
	} else {
		annotations := pod.Annotations
		vmUUID, exists = annotations[vmUUIDLabel]
		gomega.Expect(exists).To(gomega.BeTrue(), fmt.Sprintf("Pod doesn't have %s annotation", vmUUIDLabel))
		_, err := e2eVSphere.getVMByUUID(ctx, vmUUID)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
	}
	isDiskAttached, err := e2eVSphere.isVolumeAttachedToVM(client, pv.Spec.CSI.VolumeHandle, vmUUID)
	gomega.Expect(err).NotTo(gomega.HaveOccurred())
	gomega.Expect(isDiskAttached).To(gomega.BeTrue(), fmt.Sprintf("Volume is not attached to the node"))

	ginkgo.By("Verify the volume is accessible and filesystem type is as expected")
	_, err = framework.LookForStringInPodExec(namespace, pod.Name, []string{"/bin/cat", "/mnt/volume1/fstype"}, expectedContent, time.Minute)
	gomega.Expect(err).NotTo(gomega.HaveOccurred())

	// Delete POD and PVC
	ginkgo.By("Deleting the pod")
	framework.DeletePodWithWait(f, client, pod)

	if isK8SVanillaTestSetup {
		ginkgo.By("Verify volume is detached from the node")
		isDiskDetached, err := e2eVSphere.waitForVolumeDetachedFromNode(client, pv.Spec.CSI.VolumeHandle, pod.Spec.NodeName)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		gomega.Expect(isDiskDetached).To(gomega.BeTrue(), fmt.Sprintf("Volume %q is not detached from the node %q", pv.Spec.CSI.VolumeHandle, pod.Spec.NodeName))
	} else {
		ginkgo.By("Wait for 2 minutes for the pod to get terminated successfully")
		time.Sleep(time.Duration(120) * time.Second)
		ginkgo.By(fmt.Sprintf("Verify volume: %s is detached from PodVM with vmUUID: %s", pv.Spec.CSI.VolumeHandle, pod.Spec.NodeName))
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		_, err := e2eVSphere.getVMByUUID(ctx, vmUUID)
		gomega.Expect(err).To(gomega.HaveOccurred(), fmt.Sprintf("PodVM with vmUUID: %s still exists. So volume: %s is not detached from the PodVM", vmUUID, pod.Spec.NodeName))
	}

	err = framework.DeletePersistentVolumeClaim(client, pvclaim.Name, namespace)
	gomega.Expect(err).NotTo(gomega.HaveOccurred())
}

func invokeTestForInvalidFstype(f *framework.Framework, client clientset.Interface, namespace string, fstype string, isK8SVanillaTestSetup bool, storagePolicyName string, profileID string) {
	scParameters := make(map[string]string)
	scParameters["fstype"] = fstype

	// Create Storage class and PVC
	ginkgo.By("Creating Storage Class With Invalid Fstype")
	var storageclass *storagev1.StorageClass
	var pvclaim *v1.PersistentVolumeClaim
	var err error
	// decide which test setup is available to run
	if isK8SVanillaTestSetup {
		ginkgo.By("CNS_TEST: Running for vanilla k8s setup")
		storageclass, pvclaim, err = createPVCAndStorageClass(client, namespace, nil, scParameters, "", nil, "")
	} else {
		ginkgo.By("CNS_TEST: Running for WCP setup")
		scParameters[scParamStoragePolicyID] = profileID
		storageclass, pvclaim, err = createPVCAndStorageClass(client, namespace, nil, scParameters, "", nil, "", storagePolicyName)
	}

	gomega.Expect(err).NotTo(gomega.HaveOccurred())
	defer client.StorageV1().StorageClasses().Delete(storageclass.Name, nil)

	// Waiting for PVC to be bound
	var pvclaims []*v1.PersistentVolumeClaim
	pvclaims = append(pvclaims, pvclaim)
	ginkgo.By("Waiting for all claims to be in bound state")
	persistentvolumes, err := framework.WaitForPVClaimBoundPhase(client, pvclaims, framework.ClaimProvisionTimeout)
	gomega.Expect(err).NotTo(gomega.HaveOccurred())

	// Create a POD to use this PVC, and verify volume has been attached
	ginkgo.By("Creating pod to attach PV to the node")
	pod, err := framework.CreatePod(client, namespace, nil, []*v1.PersistentVolumeClaim{pvclaim}, false, execCommand)
	gomega.Expect(err).To(gomega.HaveOccurred())

	eventList, err := client.CoreV1().Events(namespace).List(metav1.ListOptions{})
	gomega.Expect(eventList.Items).NotTo(gomega.BeEmpty())
	pv := persistentvolumes[0]
	errorMsg := `MountVolume.MountDevice failed for volume "` + pv.Name
	isFailureFound := false
	for _, item := range eventList.Items {
		ginkgo.By(fmt.Sprintf("Print errorMessage %q \n", item.Message))
		if strings.Contains(item.Message, errorMsg) {
			isFailureFound = true
		}
	}
	gomega.Expect(isFailureFound).To(gomega.BeTrue(), "Unable to verify MountVolume.MountDevice failure")

	// pod.Spec.NodeName may not be set yet when pod just created
	// refetch pod to get pod.Spec.NodeName
	podNodeName := pod.Spec.NodeName
	ginkgo.By(fmt.Sprintf("podNodeName: %v podName: %v", podNodeName, pod.Name))
	pod, err = client.CoreV1().Pods(pod.Namespace).Get(pod.Name, metav1.GetOptions{})
	gomega.Expect(err).NotTo(gomega.HaveOccurred())
	podNodeName = pod.Spec.NodeName
	ginkgo.By(fmt.Sprintf("Refetch the POD: podNodeName: %v podName: %v", podNodeName, pod.Name))

	// Delete POD and PVC
	ginkgo.By("Deleting the pod")
	framework.DeletePodWithWait(f, client, pod)

	ginkgo.By("Verify volume is detached from the node")
	isDiskDetached, err := e2eVSphere.waitForVolumeDetachedFromNode(client, pv.Spec.CSI.VolumeHandle, podNodeName)
	gomega.Expect(isDiskDetached).To(gomega.BeTrue(), fmt.Sprintf("Volume %q is not detached from the node %q", pv.Spec.CSI.VolumeHandle, podNodeName))

	err = framework.DeletePersistentVolumeClaim(client, pvclaim.Name, namespace)
	gomega.Expect(err).NotTo(gomega.HaveOccurred())

}
