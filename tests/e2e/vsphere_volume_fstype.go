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
	"strings"
	"time"

	"github.com/onsi/ginkgo"
	"github.com/onsi/gomega"
	v1 "k8s.io/api/core/v1"
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

var _ = ginkgo.Describe("[csi-block-e2e] Volume Filesystem Type Test", func() {
	f := framework.NewDefaultFramework("volume-fstype")
	var (
		client    clientset.Interface
		namespace string
	)
	ginkgo.BeforeEach(func() {
		client = f.ClientSet
		namespace = f.Namespace.Name
		bootstrap()
		nodeList := framework.GetReadySchedulableNodesOrDie(f.ClientSet)
		if !(len(nodeList.Items) > 0) {
			framework.Failf("Unable to find ready and schedulable Node")
		}
	})

	ginkgo.It("CSI - verify fstype - ext3 formatted volume", func() {
		invokeTestForFstype(f, client, namespace, ext3FSType, ext3FSType)
	})

	ginkgo.It("CSI - verify fstype - default value should be ext4", func() {
		invokeTestForFstype(f, client, namespace, "", ext4FSType)
	})

	ginkgo.It("CSI - verify invalid fstype", func() {
		invokeTestForInvalidFstype(f, client, namespace, invalidFSType)
	})
})

func invokeTestForFstype(f *framework.Framework, client clientset.Interface, namespace string, fstype string, expectedContent string) {
	ginkgo.By(fmt.Sprintf("Invoking Test for fstype: %s", fstype))
	scParameters := make(map[string]string)
	scParameters["fstype"] = fstype

	// Create Storage class and PVC
	ginkgo.By("Creating Storage Class With Fstype")
	storageclass, pvclaim, err := createPVCAndStorageClass(client, namespace, nil, scParameters, "", nil, "")
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

	ginkgo.By("Verify volume is attached to the node")
	pv := persistentvolumes[0]
	isDiskAttached, err := e2eVSphere.isVolumeAttachedToNode(client, pv.Spec.CSI.VolumeHandle, pod.Spec.NodeName)
	gomega.Expect(err).NotTo(gomega.HaveOccurred())
	gomega.Expect(isDiskAttached).To(gomega.BeTrue(), fmt.Sprintf("Volume is not attached to the node"))

	ginkgo.By("Verify the volume is accessible and filesystem type is as expected")
	_, err = framework.LookForStringInPodExec(namespace, pod.Name, []string{"/bin/cat", "/mnt/volume1/fstype"}, expectedContent, time.Minute)
	gomega.Expect(err).NotTo(gomega.HaveOccurred())

	// Delete POD and PVC
	ginkgo.By("Deleting the pod")
	framework.DeletePodWithWait(f, client, pod)

	ginkgo.By("Verify volume is detached from the node")
	isDiskDetached, err := e2eVSphere.waitForVolumeDetachedFromNode(client, pv.Spec.CSI.VolumeHandle, pod.Spec.NodeName)
	gomega.Expect(err).NotTo(gomega.HaveOccurred())
	gomega.Expect(isDiskDetached).To(gomega.BeTrue(), fmt.Sprintf("Volume %q is not detached from the node %q", pv.Spec.CSI.VolumeHandle, pod.Spec.NodeName))

	err = framework.DeletePersistentVolumeClaim(client, pvclaim.Name, namespace)
	gomega.Expect(err).NotTo(gomega.HaveOccurred())
}

func invokeTestForInvalidFstype(f *framework.Framework, client clientset.Interface, namespace string, fstype string) {
	scParameters := make(map[string]string)
	scParameters["fstype"] = fstype

	// Create Storage class and PVC
	ginkgo.By("Creating Storage Class With Invalid Fstype")
	storageclass, pvclaim, err := createPVCAndStorageClass(client, namespace, nil, scParameters, "", nil, "")
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
	gomega.Expect(err).NotTo(gomega.HaveOccurred())
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
	gomega.Expect(err).NotTo(gomega.HaveOccurred())
	gomega.Expect(isDiskDetached).To(gomega.BeTrue(), fmt.Sprintf("Volume %q is not detached from the node %q", pv.Spec.CSI.VolumeHandle, podNodeName))

	err = framework.DeletePersistentVolumeClaim(client, pvclaim.Name, namespace)
	gomega.Expect(err).NotTo(gomega.HaveOccurred())

}
