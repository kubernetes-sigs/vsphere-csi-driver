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

	"github.com/onsi/ginkgo"
	"github.com/onsi/gomega"
	v1 "k8s.io/api/core/v1"
	storagev1 "k8s.io/api/storage/v1"
	clientset "k8s.io/client-go/kubernetes"
	"k8s.io/kubernetes/test/e2e/framework"
)

/*
	Steps

	1. Create a PVC.
	2. Create pod and wait for pod to become ready.
	3. Verify volume is attached.
	4. Create empty file on the volume to verify volume is writable.
	5. Verify newly created file and previously created files exist on the volume.
	6. Delete pod.
	7. Create a new pod using the previously created volume and wait for pod to become ready.
	8. Create another empty file on the volume .
	9. Verify newly created file and previously created files exist on the volume.
	10. Delete pod.
	11. Wait for volume to be detached.

*/

var _ = ginkgo.Describe("[csi-block-vanilla] [csi-supervisor] [csi-guest] Data Persistence", func() {
	f := framework.NewDefaultFramework("e2e-data-persistence")
	var (
		client            clientset.Interface
		namespace         string
		scParameters      map[string]string
		storagePolicyName string
		svcPVCName        string // PVC Name in the Supervisor Cluster
	)
	ginkgo.BeforeEach(func() {
		client = f.ClientSet
		namespace = getNamespaceToRunTests(f)
		nodeList := framework.GetReadySchedulableNodesOrDie(f.ClientSet)
		if !(len(nodeList.Items) > 0) {
			framework.Failf("Unable to find ready and schedulable Node")
		}
		bootstrap()
		scParameters = make(map[string]string)
		storagePolicyName = GetAndExpectStringEnvVar(envStoragePolicyNameForSharedDatastores)
	})

	ginkgo.It("Should create and delete pod with the same volume source", func() {
		var sc *storagev1.StorageClass
		var pvc *v1.PersistentVolumeClaim
		var err error
		ginkgo.By("Creating Storage Class and PVC")
		// decide which test setup is available to run
		if vanillaCluster {
			ginkgo.By("CNS_TEST: Running for vanilla k8s setup")
			sc, pvc, err = createPVCAndStorageClass(client, namespace, nil, nil, "", nil, "", false, "")
		} else if supervisorCluster {
			ginkgo.By("CNS_TEST: Running for WCP setup")
			ginkgo.By(fmt.Sprintf("storagePolicyName: %s", storagePolicyName))
			profileID := e2eVSphere.GetSpbmPolicyID(storagePolicyName)
			scParameters[scParamStoragePolicyID] = profileID
			// create resource quota
			createResourceQuota(client, namespace, rqLimit, storagePolicyName)
			sc, pvc, err = createPVCAndStorageClass(client, namespace, nil, scParameters, "", nil, "", false, "", storagePolicyName)
		} else {
			ginkgo.By("CNS_TEST: Running for GC setup")
			scParameters[svStorageClassName] = storagePolicyName
			sc, pvc, err = createPVCAndStorageClass(client, namespace, nil, scParameters, "", nil, "", false, "")
		}
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		defer func() {
			err := client.StorageV1().StorageClasses().Delete(sc.Name, nil)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}()

		ginkgo.By(fmt.Sprintf("Waiting for claim %s to be in bound phase", pvc.Name))
		pvs, err := framework.WaitForPVClaimBoundPhase(client, []*v1.PersistentVolumeClaim{pvc}, framework.ClaimProvisionTimeout)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		gomega.Expect(pvs).NotTo(gomega.BeEmpty())
		pv := pvs[0]
		volumeID := pv.Spec.CSI.VolumeHandle
		if guestCluster {
			// svcPVCName refers to PVC Name in the supervisor cluster
			svcPVCName = volumeID
			volumeID = getVolumeIDFromSupervisorCluster(svcPVCName)
			gomega.Expect(volumeID).NotTo(gomega.BeEmpty())
		}
		defer func() {
			err := framework.DeletePersistentVolumeClaim(client, pvc.Name, namespace)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			err = e2eVSphere.waitForCNSVolumeToBeDeleted(pv.Spec.CSI.VolumeHandle)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}()

		ginkgo.By("Creating pod")
		pod, err := framework.CreatePod(client, namespace, nil, []*v1.PersistentVolumeClaim{pvc}, false, "")
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By(fmt.Sprintf("Verify volume: %s is attached to the node: %s", pv.Spec.CSI.VolumeHandle, pod.Spec.NodeName))
		var vmUUID string
		var exists bool
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		if vanillaCluster {
			vmUUID = getNodeUUID(client, pod.Spec.NodeName)
		} else if supervisorCluster {
			annotations := pod.Annotations
			vmUUID, exists = annotations[vmUUIDLabel]
			gomega.Expect(exists).To(gomega.BeTrue(), fmt.Sprintf("Pod doesn't have %s annotation", vmUUIDLabel))
			_, err := e2eVSphere.getVMByUUID(ctx, vmUUID)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		} else {
			vmUUID, err = getVMUUIDFromNodeName(pod.Spec.NodeName)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			verifyCRDInSupervisor(ctx, f, pod.Spec.NodeName+"-"+svcPVCName, crdCNSNodeVMAttachment, crdVersion, crdGroup, true)
		}
		isDiskAttached, err := e2eVSphere.isVolumeAttachedToVM(client, volumeID, vmUUID)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		gomega.Expect(isDiskAttached).To(gomega.BeTrue(), fmt.Sprintf("Volume is not attached to the node, %s", vmUUID))

		var volumeFiles []string
		// Create an empty file on the mounted volumes on the pod
		ginkgo.By(fmt.Sprintf("Creating an empty file on the volume mounted on: %v", pod.Name))
		newEmptyFileName := fmt.Sprintf("/mnt/volume1/%v_file_A.txt", namespace)
		volumeFiles = append(volumeFiles, newEmptyFileName)
		createAndVerifyFilesOnVolume(namespace, pod.Name, []string{newEmptyFileName}, volumeFiles)
		ginkgo.By("Deleting the pod")
		err = framework.DeletePodWithWait(f, client, pod)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		if supervisorCluster {
			ginkgo.By("Wait for 3 minutes for the pod to get terminated successfully")
			time.Sleep(supervisorClusterOperationsTimeout)
			ginkgo.By(fmt.Sprintf("Verify volume: %s is detached from PodVM with vmUUID: %s", pv.Spec.CSI.VolumeHandle, vmUUID))
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			_, err := e2eVSphere.getVMByUUID(ctx, vmUUID)
			gomega.Expect(err).To(gomega.HaveOccurred(), fmt.Sprintf("PodVM with vmUUID: %s still exists. So volume: %s is not detached from the PodVM", vmUUID, pv.Spec.CSI.VolumeHandle))
		} else {
			ginkgo.By("Verify volume is detached from the node")
			isDiskDetached, err := e2eVSphere.waitForVolumeDetachedFromNode(client, pv.Spec.CSI.VolumeHandle, pod.Spec.NodeName)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			gomega.Expect(isDiskDetached).To(gomega.BeTrue(), fmt.Sprintf("Volume %q is not detached from the node %q", pv.Spec.CSI.VolumeHandle, pod.Spec.NodeName))
			if guestCluster {
				ginkgo.By("Waiting for 30 seconds to allow CnsNodeVMAttachment controller to reconcile resource")
				time.Sleep(waitTimeForCNSNodeVMAttachmentReconciler)
				verifyCRDInSupervisor(ctx, f, pod.Spec.NodeName+"-"+svcPVCName, crdCNSNodeVMAttachment, crdVersion, crdGroup, false)
			}
		}

		ginkgo.By("Creating a new pod using the same volume")
		pod, err = framework.CreatePod(client, namespace, nil, []*v1.PersistentVolumeClaim{pvc}, false, "")
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By(fmt.Sprintf("Verify volume: %s is attached to the node: %s", pv.Spec.CSI.VolumeHandle, pod.Spec.NodeName))

		ctx, cancel = context.WithCancel(context.Background())
		defer cancel()
		if vanillaCluster {
			vmUUID = getNodeUUID(client, pod.Spec.NodeName)
		} else if supervisorCluster {
			annotations := pod.Annotations
			vmUUID, exists = annotations[vmUUIDLabel]
			gomega.Expect(exists).To(gomega.BeTrue(), fmt.Sprintf("Pod doesn't have %s annotation", vmUUIDLabel))
			_, err := e2eVSphere.getVMByUUID(ctx, vmUUID)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		} else {
			vmUUID, err = getVMUUIDFromNodeName(pod.Spec.NodeName)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			verifyCRDInSupervisor(ctx, f, pod.Spec.NodeName+"-"+svcPVCName, crdCNSNodeVMAttachment, crdVersion, crdGroup, true)
		}
		isDiskAttached, err = e2eVSphere.isVolumeAttachedToVM(client, volumeID, vmUUID)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		gomega.Expect(isDiskAttached).To(gomega.BeTrue(), "Volume is not attached to the node")

		// Create another empty file on the mounted volume on the pod and
		// verify newly and previously created files present on the volume mounted on the pod
		ginkgo.By(fmt.Sprintf("Creating a second empty file on the same volume mounted on: %v", pod.Name))
		newEmptyFileName = fmt.Sprintf("/mnt/volume1/%v_file_B.txt", namespace)
		volumeFiles = append(volumeFiles, newEmptyFileName)
		createAndVerifyFilesOnVolume(namespace, pod.Name, []string{newEmptyFileName}, volumeFiles)
		ginkgo.By("Deleting the pod")
		err = framework.DeletePodWithWait(f, client, pod)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		if supervisorCluster {
			ginkgo.By("Wait for 3 minutes for the pod to get terminated successfully")
			time.Sleep(supervisorClusterOperationsTimeout)
			ginkgo.By(fmt.Sprintf("Verify volume: %s is detached from PodVM with vmUUID: %s", pv.Spec.CSI.VolumeHandle, vmUUID))
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			_, err := e2eVSphere.getVMByUUID(ctx, vmUUID)
			gomega.Expect(err).To(gomega.HaveOccurred(), fmt.Sprintf("PodVM with vmUUID: %s still exists. So volume: %s is not detached from the PodVM", vmUUID, pv.Spec.CSI.VolumeHandle))
		} else {
			ginkgo.By("Verify volume is detached from the node")
			isDiskDetached, err := e2eVSphere.waitForVolumeDetachedFromNode(client, pv.Spec.CSI.VolumeHandle, pod.Spec.NodeName)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			gomega.Expect(isDiskDetached).To(gomega.BeTrue(), fmt.Sprintf("Volume %q is not detached from the node %q", pv.Spec.CSI.VolumeHandle, pod.Spec.NodeName))
			if guestCluster {
				ginkgo.By("Waiting for 30 seconds to allow CnsNodeVMAttachment controller to reconcile resource")
				time.Sleep(waitTimeForCNSNodeVMAttachmentReconciler)
				verifyCRDInSupervisor(ctx, f, pod.Spec.NodeName+"-"+svcPVCName, crdCNSNodeVMAttachment, crdVersion, crdGroup, false)
			}
		}
	})
})

func createAndVerifyFilesOnVolume(namespace string, podname string, newEmptyfilesToCreate []string, filesToCheck []string) {
	createEmptyFilesOnVSphereVolume(namespace, podname, newEmptyfilesToCreate)
	ginkgo.By(fmt.Sprintf("Verify files exist on volume mounted on: %v", podname))
	verifyFilesExistOnVSphereVolume(namespace, podname, filesToCheck...)
}
