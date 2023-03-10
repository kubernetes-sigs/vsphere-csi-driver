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

// Test to verify fstype specified in storage-class is being honored after
// volume creation.
//
// Steps
// 1. Create StorageClass with fstype set to valid type (default case included).
// 2. Create PVC which uses the StorageClass created in step 1.
// 3. Wait for PV to be provisioned.
// 4. Wait for PVC's status to become Bound.
// 5. Create pod using PVC on specific node.
// 6. Wait for Disk to be attached to the node.
// 7. Execute command in the pod to get fstype.
// 8. Delete pod and Wait for Volume Disk to be detached from the Node.
// 9. Delete PVC, PV and Storage Class.
//
// Test to verify if an invalid fstype specified in storage class fails pod
// creation.
//
// Steps
// 1. Create StorageClass with inavlid fstype.
// 2. Create PVC which uses the StorageClass created in step 1.
// 3. Make sure that PVC remains in pending state with proper error message.

var _ = ginkgo.Describe("[csi-block-vanilla] Volume Filesystem Type Test", func() {

	f := framework.NewDefaultFramework("volume-fstype")
	f.NamespacePodSecurityEnforceLevel = admissionapi.LevelPrivileged
	var (
		client            clientset.Interface
		namespace         string
		storagePolicyName string
		profileID         string
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
	})

	ginkgo.It("[csi-block-vanilla-serialized] CSI - verify fstype - ext3 formatted volume", func() {
		invokeTestForFstype(f, client, namespace, ext3FSType, ext3FSType, storagePolicyName, profileID)
	})

	ginkgo.It("[csi-block-vanilla-parallelized] CSI - verify fstype - default value should be ext4", func() {
		invokeTestForFstype(f, client, namespace, "", ext4FSType, storagePolicyName, profileID)
	})

	ginkgo.It("[csi-block-vanilla-parallelized] CSI - verify fstype - xfs formatted volume", func() {
		invokeTestForFstype(f, client, namespace, xfsFSType, xfsFSType, storagePolicyName, profileID)
	})

	ginkgo.It("[csi-block-vanilla-parallelized] CSI - verify invalid fstype", func() {
		invokeTestForInvalidFstype(f, client, namespace, invalidFSType, storagePolicyName, profileID)
	})
})

func invokeTestForFstype(f *framework.Framework, client clientset.Interface,
	namespace string, fstype string, expectedContent string, storagePolicyName string, profileID string) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	ginkgo.By(fmt.Sprintf("Invoking Test for fstype: %s", fstype))
	scParameters := make(map[string]string)
	scParameters[scParamFsType] = fstype
	// Create Storage class and PVC
	ginkgo.By("Creating Storage Class With Fstype")
	var storageclass *storagev1.StorageClass
	var pvclaim *v1.PersistentVolumeClaim
	var err error

	ginkgo.By("CNS_TEST: Running for vanilla k8s setup")
	storageclass, pvclaim, err = createPVCAndStorageClass(client, namespace, nil, scParameters, "", nil, "", false, "")
	gomega.Expect(err).NotTo(gomega.HaveOccurred())
	defer func() {
		err := client.StorageV1().StorageClasses().Delete(ctx, storageclass.Name, *metav1.NewDeleteOptions(0))
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
	}()
	defer func() {
		err := fpv.DeletePersistentVolumeClaim(client, pvclaim.Name, namespace)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
	}()

	// Waiting for PVC to be bound
	var pvclaims []*v1.PersistentVolumeClaim
	pvclaims = append(pvclaims, pvclaim)
	ginkgo.By("Waiting for all claims to be in bound state")
	persistentvolumes, err := fpv.WaitForPVClaimBoundPhase(client, pvclaims, framework.ClaimProvisionTimeout)
	gomega.Expect(err).NotTo(gomega.HaveOccurred())

	// Create a Pod to use this PVC, and verify volume has been attached
	ginkgo.By("Creating pod to attach PV to the node")
	pod, _ := createPod(client, namespace, nil, []*v1.PersistentVolumeClaim{pvclaim}, false, execCommand)
	err = fpod.WaitForPodNameRunningInNamespace(client, pod.Name, namespace)
	gomega.Expect(err).NotTo(gomega.HaveOccurred())

	pv := persistentvolumes[0]
	var vmUUID string
	ginkgo.By(fmt.Sprintf("Verify volume: %s is attached to the node: %s", pv.Spec.CSI.VolumeHandle, pod.Spec.NodeName))
	vmUUID = getNodeUUID(ctx, client, pod.Spec.NodeName)
	isDiskAttached, err := e2eVSphere.isVolumeAttachedToVM(client, pv.Spec.CSI.VolumeHandle, vmUUID)
	gomega.Expect(err).NotTo(gomega.HaveOccurred())
	gomega.Expect(isDiskAttached).To(gomega.BeTrue(), "Volume is not attached to the node")

	ginkgo.By("Verify the volume is accessible and filesystem type is as expected")
	_, err = framework.LookForStringInPodExec(namespace, pod.Name, []string{"/bin/cat", "/mnt/volume1/fstype"},
		expectedContent, time.Minute)
	gomega.Expect(err).NotTo(gomega.HaveOccurred())

	// Delete POD
	ginkgo.By(fmt.Sprintf("Deleting the pod %s in namespace %s", pod.Name, namespace))
	err = fpod.DeletePodWithWait(client, pod)
	gomega.Expect(err).NotTo(gomega.HaveOccurred())

	ginkgo.By("Verify volume is detached from the node")
	isDiskDetached, err := e2eVSphere.waitForVolumeDetachedFromNode(client, pv.Spec.CSI.VolumeHandle, pod.Spec.NodeName)
	gomega.Expect(err).NotTo(gomega.HaveOccurred())
	gomega.Expect(isDiskDetached).To(gomega.BeTrue(),
		fmt.Sprintf("Volume %q is not detached from the node %q", pv.Spec.CSI.VolumeHandle, pod.Spec.NodeName))
}

func invokeTestForInvalidFstype(f *framework.Framework, client clientset.Interface,
	namespace string, fstype string, storagePolicyName string, profileID string) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	scParameters := make(map[string]string)
	scParameters[scParamFsType] = fstype

	// Create Storage class and PVC
	ginkgo.By("Creating Storage Class With Invalid Fstype")
	var storageclass *storagev1.StorageClass
	var pvclaim *v1.PersistentVolumeClaim
	var err error

	ginkgo.By("CNS_TEST: Running for vanilla k8s setup")
	storageclass, pvclaim, err = createPVCAndStorageClass(client, namespace, nil, scParameters, "", nil, "", false, "")
	gomega.Expect(err).NotTo(gomega.HaveOccurred())
	defer func() {
		err := client.StorageV1().StorageClasses().Delete(ctx, storageclass.Name, *metav1.NewDeleteOptions(0))
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
	}()
	defer func() {
		err := fpv.DeletePersistentVolumeClaim(client, pvclaim.Name, namespace)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
	}()

	ginkgo.By(fmt.Sprintf("Expect claim to fail as filesystem %q is invalid.", fstype))
	expectedErrMsg := "fstype " + fstype + " not supported for ReadWriteOnce volume creation"
	err = waitForEvent(ctx, client, namespace, expectedErrMsg, pvclaim.Name)
	gomega.Expect(err).NotTo(gomega.HaveOccurred(), "Expected error : %q", expectedErrMsg)
}
