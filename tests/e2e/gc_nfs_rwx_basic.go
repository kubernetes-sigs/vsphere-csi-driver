/*
Copyright 2026 The Kubernetes Authors.

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

	snapV1 "github.com/kubernetes-csi/external-snapshotter/client/v8/apis/volumesnapshot/v1"
	snapclient "github.com/kubernetes-csi/external-snapshotter/client/v8/clientset/versioned"
	"github.com/onsi/ginkgo/v2"
	"github.com/onsi/gomega"
	v1 "k8s.io/api/core/v1"
	storagev1 "k8s.io/api/storage/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	clientset "k8s.io/client-go/kubernetes"
	"k8s.io/kubernetes/test/e2e/framework"
	fnodes "k8s.io/kubernetes/test/e2e/framework/node"
	fpod "k8s.io/kubernetes/test/e2e/framework/pod"
	fpv "k8s.io/kubernetes/test/e2e/framework/pv"
	admissionapi "k8s.io/pod-security-admission/api"
)

var _ = ginkgo.Describe("[guest-nfs-rwx] Guest NFS RWX Volume Test", ginkgo.Label(p0, tkg, file, newTest), func() {
	f := framework.NewDefaultFramework("gc-nfs-rwx-basic")
	f.NamespacePodSecurityEnforceLevel = admissionapi.LevelPrivileged
	var (
		client         clientset.Interface
		snapc          *snapclient.Clientset
		namespace      string
		nfsServerIP    string
		nfsServerShare string
	)

	ginkgo.BeforeEach(func() {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		client = f.ClientSet
		namespace = getNamespaceToRunTests(f)
		nfsServerIP = GetAndExpectStringEnvVar(envNFSServerIP)
		nfsServerShare = GetAndExpectStringEnvVar(envNFSServerShare)

		// bootstrap() is deliberately not called: it logs in to the vCenter named in E2E_TEST_CONF_FILE,
		// and a guest NFS volume has nothing in vCenter, CNS or the Supervisor to check. The cluster
		// flavor and the busybox image this test needs are set by the suite's init().
		nodeList, err := fnodes.GetReadySchedulableNodes(ctx, client)
		framework.ExpectNoError(err, "Unable to find ready and schedulable Node")
		if !(len(nodeList.Items) > 0) {
			framework.Failf("Unable to find ready and schedulable Node")
		}

		// With the feature state off, CreateVolume is refused and the PVC would stay Pending until the
		// provision timeout, so fail fast with the reason instead.
		csiNamespace := GetStringEnvVarOrDefault(envCSINamespace, csiSystemNamespace)
		gomega.Expect(isCsiFssEnabled(ctx, client, csiNamespace, guestNFSVolumeFss)).To(gomega.BeTrue(),
			"feature state %q must be enabled in the %s ConfigMap in namespace %s",
			guestNFSVolumeFss, csiFssCM, csiNamespace)

		snapc, err = snapclient.NewForConfig(getRestConfigClientForGuestCluster(nil))
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
	})

	/*
		Test to verify a guest NFS RWX volume end to end. The volume is served by the NFS backend of
		pvCSI (a StorageClass with a "server" parameter), so nothing is checked in CNS, vCenter or the
		Supervisor.

		Steps
		1. Create a StorageClass with the NFS server and share parameters, allowVolumeExpansion true
		2. Create an RWX PVC with it, wait for it to be Bound and verify the volume handle has the
		   guestnfs: prefix
		3. Create a pod using the PVC, write a file and read it back
		4. Delete the pod
		5. Expand the PVC and wait for the PV size and the PVC status capacity to show the new size
		6. Create a new pod using the PVC and verify the file written in step 3 is still there
		7. Create a VolumeSnapshotClass and a VolumeSnapshot of the PVC and wait for it to be ReadyToUse
		8. Delete the pod
		9. Delete the VolumeSnapshot and wait for its VolumeSnapshotContent to be deleted, delete the
		   VolumeSnapshotClass, then delete the PVC and wait for the PV to be deleted
		10. Delete the StorageClass
	*/
	ginkgo.It("Verify guest NFS RWX volume provision, expansion and snapshot", func() {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		nfsTestData := "Hello message from test into Pod1"

		ginkgo.By("Creating an NFS StorageClass")
		scParameters := map[string]string{
			scParamNFSServer: nfsServerIP,
			scParamNFSShare:  nfsServerShare,
		}
		storageclass, err := createStorageClass(client, scParameters, nil, "",
			storagev1.VolumeBindingImmediate, true, "")
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		defer func() {
			err := client.StorageV1().StorageClasses().Delete(ctx, storageclass.Name, *metav1.NewDeleteOptions(0))
			if !apierrors.IsNotFound(err) {
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
			}
		}()

		ginkgo.By("Creating an RWX PVC with the NFS StorageClass")
		pvclaim, err := createPVC(ctx, client, namespace, nil, diskSize, storageclass, v1.ReadWriteMany)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		pvcName := pvclaim.Name
		var pv *v1.PersistentVolume
		defer func() {
			err := fpv.DeletePersistentVolumeClaim(ctx, client, pvcName, namespace)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			if pv != nil {
				err = fpv.WaitForPersistentVolumeDeleted(ctx, client, pv.Name, poll, pollTimeout)
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
			}
		}()

		ginkgo.By("Expect claim to provision volume successfully")
		persistentvolumes, err := fpv.WaitForPVClaimBoundPhase(ctx, client,
			[]*v1.PersistentVolumeClaim{pvclaim}, framework.ClaimProvisionTimeout)
		gomega.Expect(err).NotTo(gomega.HaveOccurred(), "Failed to provision volume")
		pv = persistentvolumes[0]
		gomega.Expect(pv.Spec.CSI).NotTo(gomega.BeNil())
		volHandle := pv.Spec.CSI.VolumeHandle
		ginkgo.By(fmt.Sprintf("Verifying that volume handle %q is a guest NFS volume", volHandle))
		gomega.Expect(volHandle).To(gomega.HavePrefix(guestNFSVolumeIDPrefix))

		ginkgo.By("Creating pod1 using the PVC")
		pod, err := createPod(ctx, client, namespace, nil, []*v1.PersistentVolumeClaim{pvclaim}, false,
			execRWXCommandPod1)
		// Registered before the error check: createPod returns the pod even when it does not reach Running.
		defer func() {
			err := fpod.DeletePodWithWait(ctx, client, pod)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}()
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Verify the volume is accessible and Read/write is possible from pod1")
		output := readFileFromPod(namespace, pod.Name, filePathPod1)
		gomega.Expect(output).To(gomega.ContainSubstring("Hello message from Pod1"))
		writeDataOnFileFromPod(namespace, pod.Name, filePathPod1, nfsTestData)
		output = readFileFromPod(namespace, pod.Name, filePathPod1)
		gomega.Expect(output).To(gomega.ContainSubstring(nfsTestData))

		ginkgo.By(fmt.Sprintf("Deleting pod1 %s in namespace %s", pod.Name, namespace))
		err = fpod.DeletePodWithWait(ctx, client, pod)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Expanding the PVC")
		currentPvcSize := pvclaim.Spec.Resources.Requests[v1.ResourceStorage]
		newSize := currentPvcSize.DeepCopy()
		newSize.Add(resource.MustParse("1Gi"))
		framework.Logf("currentPvcSize %v, newSize %v", currentPvcSize, newSize)
		pvclaim, err = expandPVCSize(pvclaim, newSize, client)
		framework.ExpectNoError(err, "While updating pvc for more size")
		gomega.Expect(pvclaim).NotTo(gomega.BeNil())
		pvcSize := pvclaim.Spec.Resources.Requests[v1.ResourceStorage]
		if pvcSize.Cmp(newSize) != 0 {
			framework.Failf("error updating pvc size %q", pvcName)
		}

		ginkgo.By("Waiting for controller volume resize to finish")
		err = waitForPvResizeForGivenPvc(pvclaim, client, totalResizeWaitPeriod)
		framework.ExpectNoError(err, "While waiting for pvc resize to finish")

		// The NFS backend reports NodeExpansionRequired false, so the resizer finishes the expansion
		// itself: status.capacity is updated without a pod and no FileSystemResizePending condition
		// is set.
		ginkgo.By("Waiting for the PVC status capacity to show the new size")
		pvclaim, err = waitForFSResize(pvclaim, client)
		framework.ExpectNoError(err, "While waiting for the PVC status capacity to be updated")
		pvcStatusSize := pvclaim.Status.Capacity[v1.ResourceStorage]
		framework.Logf("PVC %s status capacity is %v", pvcName, pvcStatusSize.String())

		ginkgo.By("Creating pod2 using the expanded PVC")
		pod2, err := createPod(ctx, client, namespace, nil, []*v1.PersistentVolumeClaim{pvclaim}, false,
			execRWXCommandPod2)
		// Registered before the error check: createPod returns the pod even when it does not reach Running.
		defer func() {
			err := fpod.DeletePodWithWait(ctx, client, pod2)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}()
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Verify the data written from pod1 is still present on the volume")
		output = readFileFromPod(namespace, pod2.Name, filePathPod1)
		gomega.Expect(output).To(gomega.ContainSubstring(nfsTestData))

		ginkgo.By("Create volume snapshot class")
		volumeSnapshotClass, err := snapc.SnapshotV1().VolumeSnapshotClasses().Create(ctx,
			getVolumeSnapshotClassSpec(snapV1.DeletionPolicy(deletionPolicy), nil), metav1.CreateOptions{})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		defer func() {
			err := snapc.SnapshotV1().VolumeSnapshotClasses().Delete(ctx, volumeSnapshotClass.Name,
				metav1.DeleteOptions{})
			if !apierrors.IsNotFound(err) {
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
			}
		}()

		ginkgo.By("Create a volume snapshot")
		volumeSnapshot, err := snapc.SnapshotV1().VolumeSnapshots(namespace).Create(ctx,
			getVolumeSnapshotSpec(namespace, volumeSnapshotClass.Name, pvcName), metav1.CreateOptions{})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		snapshotName := volumeSnapshot.Name
		framework.Logf("Volume snapshot name is : %s", snapshotName)
		var snapshotContentName string
		defer func() {
			deleteVolumeSnapshotWithPollWait(ctx, snapc, namespace, snapshotName)
			if snapshotContentName != "" {
				err := waitForVolumeSnapshotContentToBeDeleted(*snapc, ctx, snapshotContentName)
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
			}
		}()

		ginkgo.By("Verify volume snapshot is ready to use")
		volumeSnapshot, err = waitForVolumeSnapshotReadyToUse(*snapc, ctx, namespace, snapshotName)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		gomega.Expect(volumeSnapshot.Status.BoundVolumeSnapshotContentName).NotTo(gomega.BeNil())
		snapshotContentName = *volumeSnapshot.Status.BoundVolumeSnapshotContentName
		snapshotContent, err := snapc.SnapshotV1().VolumeSnapshotContents().Get(ctx, snapshotContentName,
			metav1.GetOptions{})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		gomega.Expect(snapshotContent.Status).NotTo(gomega.BeNil())
		gomega.Expect(snapshotContent.Status.SnapshotHandle).NotTo(gomega.BeNil())
		ginkgo.By(fmt.Sprintf("Verifying that snapshot handle %q is a guest NFS snapshot",
			*snapshotContent.Status.SnapshotHandle))
		gomega.Expect(*snapshotContent.Status.SnapshotHandle).To(gomega.HavePrefix(guestNFSVolumeIDPrefix))

		ginkgo.By(fmt.Sprintf("Deleting pod2 %s in namespace %s", pod2.Name, namespace))
		err = fpod.DeletePodWithWait(ctx, client, pod2)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By(fmt.Sprintf("Deleting volume snapshot %s", snapshotName))
		deleteVolumeSnapshotWithPollWait(ctx, snapc, namespace, snapshotName)
		ginkgo.By(fmt.Sprintf("Wait till the volume snapshot content %s is deleted", snapshotContentName))
		err = waitForVolumeSnapshotContentToBeDeleted(*snapc, ctx, snapshotContentName)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By(fmt.Sprintf("Deleting volume snapshot class %s", volumeSnapshotClass.Name))
		err = snapc.SnapshotV1().VolumeSnapshotClasses().Delete(ctx, volumeSnapshotClass.Name, metav1.DeleteOptions{})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By(fmt.Sprintf("Deleting PVC %s and waiting for PV %s to be deleted", pvcName, pv.Name))
		err = fpv.DeletePersistentVolumeClaim(ctx, client, pvcName, namespace)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		err = fpv.WaitForPersistentVolumeDeleted(ctx, client, pv.Name, poll, pollTimeout)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By(fmt.Sprintf("Deleting storage class %s", storageclass.Name))
		err = client.StorageV1().StorageClasses().Delete(ctx, storageclass.Name, *metav1.NewDeleteOptions(0))
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
	})
})
