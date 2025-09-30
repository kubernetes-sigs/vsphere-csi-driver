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
	"os"
	"path/filepath"
	"strconv"

	"github.com/onsi/ginkgo/v2"
	"github.com/onsi/gomega"
	v1 "k8s.io/api/core/v1"
	storage "k8s.io/api/storage/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	clientset "k8s.io/client-go/kubernetes"
	"k8s.io/kubernetes/test/e2e/framework"
	fnodes "k8s.io/kubernetes/test/e2e/framework/node"
	fpod "k8s.io/kubernetes/test/e2e/framework/pod"
	fpv "k8s.io/kubernetes/test/e2e/framework/pv"
	admissionapi "k8s.io/pod-security-admission/api"
)

/*
	Test to perform volume Ops storm.

	Steps
    	1. Create storage class for dynamic volume provisioning using CSI driver.
    	2. Create PVCs using above storage class in annotation, requesting 2 GB volume.
    	3. Wait until all disks are ready and all PVs and PVCs get bind. (CreateVolume storm)
    	4. Create pod to mount volumes using PVCs created in step 2. (AttachDisk storm)
    	5. Wait for pod status to be running.
    	6. Verify all volumes accessible and available in the pod.
    	7. Delete pod.
    	8. wait until volumes gets detached. (DetachDisk storm)
    	9. Delete all PVCs. This should delete all Disks. (DeleteVolume storm)
		10. Delete storage class.
*/

var _ = ginkgo.Describe("[csi-block-vanilla] [csi-block-vanilla-parallelized] Volume Operations Storm", func() {

	// TODO: Enable this test for WCP after it provides consistent results
	f := framework.NewDefaultFramework("volume-ops-storm")
	f.NamespacePodSecurityEnforceLevel = admissionapi.LevelPrivileged
	const defaultVolumeOpsScale = 30
	const defaultVolumeOpsScaleWCP = 29
	var (
		client            clientset.Interface
		namespace         string
		scParameters      map[string]string
		storageclass      *storage.StorageClass
		pvclaim           *v1.PersistentVolumeClaim
		pvclaims          []*v1.PersistentVolumeClaim
		podArray          []*v1.Pod
		persistentvolumes []*v1.PersistentVolume
		err               error
		volumeOpsScale    int
		storagePolicyName string
	)
	ginkgo.BeforeEach(func() {
		client = f.ClientSet
		namespace = getNamespaceToRunTests(f)
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		nodeList, err := fnodes.GetReadySchedulableNodes(ctx, f.ClientSet)
		framework.ExpectNoError(err, "Unable to find ready and schedulable Node")
		if !(len(nodeList.Items) > 0) {
			framework.Failf("Unable to find ready and schedulable Node")
		}
		bootstrap()
		if os.Getenv("VOLUME_OPS_SCALE") != "" {
			volumeOpsScale, err = strconv.Atoi(os.Getenv(envVolumeOperationsScale))
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		} else {
			if vanillaCluster {
				volumeOpsScale = defaultVolumeOpsScale
			} else {
				volumeOpsScale = defaultVolumeOpsScaleWCP
			}
		}
		pvclaims = make([]*v1.PersistentVolumeClaim, volumeOpsScale)
		podArray = make([]*v1.Pod, volumeOpsScale)
		scParameters = make(map[string]string)
		storagePolicyName = GetAndExpectStringEnvVar(envStoragePolicyNameForSharedDatastores)
	})

	ginkgo.AfterEach(func() {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		if supervisorCluster {
			deleteResourceQuota(client, namespace)
		}
		ginkgo.By("Deleting all PVCs")
		for _, claim := range pvclaims {
			err := fpv.DeletePersistentVolumeClaim(ctx, client, claim.Name, namespace)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}
		ginkgo.By("Verify PVs, volumes are deleted from CNS")
		for _, pv := range persistentvolumes {
			err := fpv.WaitForPersistentVolumeDeleted(ctx, client, pv.Name, framework.Poll, framework.PodDeleteTimeout)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			err = e2eVSphere.waitForCNSVolumeToBeDeleted(pv.Spec.CSI.VolumeHandle)
			gomega.Expect(err).NotTo(gomega.HaveOccurred(),
				fmt.Sprintf("Volume: %s should not be present in the CNS after it is deleted from kubernetes",
					pv.Spec.CSI.VolumeHandle))
		}
	})

	ginkgo.It("[pq-vanilla-block]create/delete pod with many volumes and verify no attach/detach call should "+
		"fail", ginkgo.Label(p0, vanilla, block, windows, vc70), func() {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		ginkgo.By(fmt.Sprintf("Running test with VOLUME_OPS_SCALE: %v", volumeOpsScale))
		ginkgo.By("Creating Storage Class")

		// decide which test setup is available to run
		if vanillaCluster {
			ginkgo.By("CNS_TEST: Running for vanilla k8s setup")
			scParameters = nil
			storagePolicyName = ""
		} else {
			ginkgo.By("CNS_TEST: Running for WCP setup")
			profileID := e2eVSphere.GetSpbmPolicyID(storagePolicyName)
			scParameters[scParamStoragePolicyID] = profileID
			// create resource quota
			createResourceQuota(client, namespace, "100Gi", storagePolicyName)
		}

		storageclass, err = client.StorageV1().StorageClasses().Create(ctx,
			getVSphereStorageClassSpec(storagePolicyName, scParameters, nil, "", "", false),
			metav1.CreateOptions{})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		defer func() {
			err := client.StorageV1().StorageClasses().Delete(ctx, storageclass.Name, *metav1.NewDeleteOptions(0))
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}()

		ginkgo.By("Creating PVCs using the Storage Class")
		count := 0
		for count < volumeOpsScale {
			pvclaims[count], err = fpv.CreatePVC(ctx, client, namespace,
				getPersistentVolumeClaimSpecWithStorageClass(namespace, diskSize, storageclass, nil, ""))
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			count++
		}

		ginkgo.By("Waiting for all claims to be in bound state")
		persistentvolumes, err = fpv.WaitForPVClaimBoundPhase(ctx, client, pvclaims, framework.ClaimProvisionTimeout)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Creating pod to attach PVs to the node")
		pod, err := createPod(ctx, client, namespace, nil, pvclaims, false, "")
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		defer func() {
			err = client.CoreV1().Pods(namespace).Delete(ctx, pod.Name, *metav1.NewDeleteOptions(0))
			if !apierrors.IsNotFound(err) {
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
			}
		}()

		var vmUUID string
		ginkgo.By("Verify the volumes are attached to the node vm")
		for _, pv := range persistentvolumes {
			var exists bool
			ginkgo.By(fmt.Sprintf("Verify volume: %s is attached to the node: %s",
				pv.Spec.CSI.VolumeHandle, pod.Spec.NodeName))
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			if vanillaCluster {
				vmUUID = getNodeUUID(ctx, client, pod.Spec.NodeName)
			} else {
				annotations := pod.Annotations
				vmUUID, exists = annotations[vmUUIDLabel]
				gomega.Expect(exists).To(gomega.BeTrue(), fmt.Sprintf("Pod doesn't have %s annotation", vmUUIDLabel))
				_, err := e2eVSphere.getVMByUUID(ctx, vmUUID)
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
			}
			isDiskAttached, err := e2eVSphere.isVolumeAttachedToVM(client, pv.Spec.CSI.VolumeHandle, vmUUID)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			gomega.Expect(isDiskAttached).To(gomega.BeTrue(),
				fmt.Sprintf("Volume: %s is not attached to the node: %s", pv.Spec.CSI.VolumeHandle, pod.Spec.NodeName))
		}

		ginkgo.By("Verify all volumes are accessible in the pod")
		for index := range persistentvolumes {
			// Verify Volumes are accessible by creating an empty file on the volume
			filepath := filepath.Join("/mnt/", fmt.Sprintf("volume%v", index+1), "/emptyFile.txt")
			createEmptyFilesOnVSphereVolume(namespace, pod.Name, []string{filepath})
		}

		ginkgo.By("Deleting pod")
		framework.ExpectNoError(fpod.DeletePodWithWait(ctx, client, pod))

		ginkgo.By("Verify volumes are detached from the node")
		for _, pv := range persistentvolumes {
			if vanillaCluster {
				ginkgo.By(fmt.Sprintf("Verify volume: %s is detached to the node: %s",
					pv.Spec.CSI.VolumeHandle, pod.Spec.NodeName))
				isDiskDetached, err := e2eVSphere.waitForVolumeDetachedFromNode(client,
					pv.Spec.CSI.VolumeHandle, pod.Spec.NodeName)
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
				gomega.Expect(isDiskDetached).To(gomega.BeTrue(),
					fmt.Sprintf("Volume %q is not detached from the node %q", pv.Spec.CSI.VolumeHandle, pod.Spec.NodeName))
			} else {
				ginkgo.By(fmt.Sprintf("Verify volume: %s is detached from PodVM with vmUUID: %s",
					pv.Spec.CSI.VolumeHandle, pod.Spec.NodeName))
				ctx, cancel := context.WithCancel(context.Background())
				defer cancel()
				_, err := e2eVSphere.getVMByUUIDWithWait(ctx, vmUUID, supervisorClusterOperationsTimeout)
				gomega.Expect(err).To(gomega.HaveOccurred(),
					fmt.Sprintf("PodVM with vmUUID: %s still exists. So volume: %s is not detached from the PodVM",
						vmUUID, pod.Spec.NodeName))
			}
		}
	})

	/*
		Test to verify detach race on Namespace deletion

		Steps
		1. Create StorageClass
		2. Create PVCs which uses the StorageClass created in step 1.
		3. Wait for PV to be provisioned.
		4. Wait for PVC's status to become Bound.
		5. Create pods using PVCs on specific node
		6. Wait for Disk to be attached to the node.
		7. Delete Namespace  and Wait for volumes to be deleted and Volume Disk to be detached from the Node.
	*/

	ginkgo.It("[csi-file-vanilla][csi-guest][pq-vanilla-file][pq-vanilla-block][pq-vks][pq-vks-n1][pq-vks-n2] Delete "+
		"namespace to confirm all volumes and pods are deleted", ginkgo.Label(p0, vanilla, block, file, tkg, vc70), func() {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		ginkgo.By(fmt.Sprintf("Running test with VOLUME_OPS_SCALE: %v", volumeOpsScale))
		ginkgo.By("Creating Storage Class")

		// decide which test setup is available to run
		if vanillaCluster {
			ginkgo.By("CNS_TEST: Running for vanilla k8s setup")
			scParameters = nil
			storagePolicyName = ""
		} else if guestCluster {
			ginkgo.By("CNS_TEST: Running for GC setup")
			scParameters[svStorageClassName] = storagePolicyName
			svcClient, svNamespace := getSvcClientAndNamespace()
			setResourceQuota(svcClient, svNamespace, rqLimit)
		}
		if guestCluster {
			storageclass, err = createStorageClass(client, scParameters, nil, "", "", true, "")
		} else {
			scSpec := getVSphereStorageClassSpec(storagePolicyName, scParameters, nil, "", "", false)
			storageclass, err = client.StorageV1().StorageClasses().Create(ctx, scSpec, metav1.CreateOptions{})
		}
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		defer func() {
			err := client.StorageV1().StorageClasses().Delete(ctx, storageclass.Name, *metav1.NewDeleteOptions(0))
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}()

		count := 0
		for count < volumeOpsScale {
			ginkgo.By("Creating PVCs using the Storage Class")
			pvclaims[count], err = fpv.CreatePVC(ctx, client, namespace,
				getPersistentVolumeClaimSpecWithStorageClass(namespace, diskSize, storageclass, nil, ""))
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			count++
		}

		ginkgo.By("Waiting for all claims to be in bound state")
		persistentvolumes, err = fpv.WaitForPVClaimBoundPhase(ctx, client, pvclaims, framework.ClaimProvisionTimeout)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		podCount := 0
		for podCount < volumeOpsScale {
			ginkgo.By("Creating pod to attach PVs to the node")
			pvclaim = pvclaims[podCount]
			podArray[podCount], err = createPod(ctx, client, namespace, nil, []*v1.PersistentVolumeClaim{pvclaim}, false, "")
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			podCount++
		}

		defer func() {
			err = client.CoreV1().Namespaces().Delete(ctx, namespace, *metav1.NewDeleteOptions(0))
			if !apierrors.IsNotFound(err) {
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
			}
		}()

		var vmUUID string
		ginkgo.By("Verify the volumes are attached to the node vm")
		podCount = 0
		for podCount < volumeOpsScale {
			pvclaim = pvclaims[podCount]
			pv := getPvFromClaim(client, namespace, pvclaim.Name)

			volumeID := pv.Spec.CSI.VolumeHandle
			if vanillaCluster {
				vmUUID = getNodeUUID(ctx, client, podArray[podCount].Spec.NodeName)
			}
			if guestCluster {
				vmUUID, err = getVMUUIDFromNodeName(podArray[podCount].Spec.NodeName)
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
				// Get volume ID from Supervisor cluster
				volumeID = getVolumeIDFromSupervisorCluster(volumeID)
			}

			ginkgo.By(fmt.Sprintf("Verify volume: %s is attached to the node: %s",
				pv.Spec.CSI.VolumeHandle, podArray[podCount].Spec.NodeName))
			isDiskAttached, err := e2eVSphere.isVolumeAttachedToVM(client, volumeID, vmUUID)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			gomega.Expect(isDiskAttached).To(gomega.BeTrue(),
				fmt.Sprintf("Volume: %s is not attached to the node: %s",
					pv.Spec.CSI.VolumeHandle, podArray[podCount].Spec.NodeName))
			podCount++
		}

		ginkgo.By("Verify whether Namespace deleted or not")
		err = client.CoreV1().Namespaces().Delete(ctx, namespace, *metav1.NewDeleteOptions(0))
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		err = waitForNamespaceToGetDeleted(ctx, client, namespace, healthStatusPollInterval, k8sPodTerminationTimeOutLong)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Verify the volumes are deleted from CNS")
		pvCount := 0
		for pvCount < volumeOpsScale {
			err = e2eVSphere.waitForCNSVolumeToBeDeleted(persistentvolumes[pvCount].Spec.CSI.VolumeHandle)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			pvCount++
		}
	})
})
