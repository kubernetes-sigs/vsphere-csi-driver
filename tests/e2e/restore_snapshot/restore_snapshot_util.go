/*
Copyright 2025 The Kubernetes Authors.

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
package restore_snapshot

import (
	"context"
	"fmt"
	"strings"
	"time"

	snapV1 "github.com/kubernetes-csi/external-snapshotter/client/v8/apis/volumesnapshot/v1"
	"github.com/onsi/ginkgo/v2"
	"github.com/onsi/gomega"
	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	clientset "k8s.io/client-go/kubernetes"
	"k8s.io/kubernetes/test/e2e/framework"
	"sigs.k8s.io/vsphere-csi-driver/v3/tests/e2e/config"
	"sigs.k8s.io/vsphere-csi-driver/v3/tests/e2e/constants"
	"sigs.k8s.io/vsphere-csi-driver/v3/tests/e2e/csisnapshot"
	"sigs.k8s.io/vsphere-csi-driver/v3/tests/e2e/k8testutil"
	"sigs.k8s.io/vsphere-csi-driver/v3/tests/e2e/vcutil"

	fpod "k8s.io/kubernetes/test/e2e/framework/pod"
	fpv "k8s.io/kubernetes/test/e2e/framework/pv"
)

// verifyVolumeRestoreOperation verifies if volume(PVC) restore from given snapshot
// and creates pod and checks attach volume operation if verifyPodCreation is set to true
func VerifyVolumeRestoreOperationOnDifferentDatastore(ctx context.Context, vs *config.E2eTestConfig,
	client clientset.Interface, namespace string, storagePolicyNames []string,
	volumeSnapshot *snapV1.VolumeSnapshot, diskSize string, verifyPodCreation bool) {

	for _, storagePolicyName := range storagePolicyNames {
		ginkgo.By("Create storage class")
		scParameters := make(map[string]string)
		storagePolicyName := strings.ToLower(strings.ReplaceAll(storagePolicyName, " ", "-"))
		profileID := vcutil.GetSpbmPolicyID(storagePolicyName, vs)
		scParameters[constants.ScParamStoragePolicyID] = profileID
		storageclass, err := k8testutil.CreateStorageClass(client, vs, scParameters, nil, "", "", false, storagePolicyName)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		defer func() {
			ginkgo.By("Delete Storage Class")
			err = client.StorageV1().StorageClasses().Delete(ctx, storageclass.Name, *metav1.NewDeleteOptions(0))
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}()

		ginkgo.By("Create PVC from snapshot on datastore")
		pvcSpec := csisnapshot.GetPersistentVolumeClaimSpecWithDatasource(namespace, diskSize, storageclass, nil,
			v1.ReadWriteOnce, volumeSnapshot.Name, constants.Snapshotapigroup)

		pvclaim2, err := fpv.CreatePVC(ctx, client, namespace, pvcSpec)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		persistentvolumes2, err := fpv.WaitForPVClaimBoundPhase(ctx, client,
			[]*v1.PersistentVolumeClaim{pvclaim2}, framework.ClaimProvisionTimeout)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		volHandle2 := persistentvolumes2[0].Spec.CSI.VolumeHandle
		if vs.TestInput.ClusterFlavor.GuestCluster {
			volHandle2 = k8testutil.GetVolumeIDFromSupervisorCluster(volHandle2)
		}
		gomega.Expect(volHandle2).NotTo(gomega.BeEmpty())
		ginkgo.DeferCleanup(func() {
			cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), time.Minute)
			defer cleanupCancel()
			err := fpv.DeletePersistentVolumeClaim(cleanupCtx, client, pvclaim2.Name, namespace)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			err = vcutil.WaitForCNSVolumeToBeDeleted(vs, volHandle2)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		})

		var pod *v1.Pod
		if verifyPodCreation {
			// Create a Pod to use this PVC, and verify volume has been attached
			ginkgo.By("Creating pod to attach PV to the node")
			pod, err = k8testutil.CreatePod(ctx, vs, client, namespace, nil,
				[]*v1.PersistentVolumeClaim{pvclaim2}, false, constants.ExecRWXCommandPod1)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			defer func() {
				// Delete POD
				ginkgo.By(fmt.Sprintf("Deleting the pod %s in namespace %s", pod.Name, namespace))
				err = fpod.DeletePodWithWait(ctx, client, pod)
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
			}()
			var vmUUID string
			var exists bool
			nodeName := pod.Spec.NodeName

			if vs.TestInput.ClusterFlavor.GuestCluster {
				vmUUID, err = vcutil.GetVMUUIDFromNodeName(vs, pod.Spec.NodeName)
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
			} else if vs.TestInput.ClusterFlavor.SupervisorCluster {
				annotations := pod.Annotations
				vmUUID, exists = annotations[constants.VmUUIDLabel]
				gomega.Expect(exists).To(gomega.BeTrue(), fmt.Sprintf("Pod doesn't have %s annotation", constants.VmUUIDLabel))
				_, err := vcutil.GetVMByUUID(ctx, vs, vmUUID)
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
			}

			ginkgo.By(fmt.Sprintf("Verify volume: %s is attached to the node: %s", volHandle2, nodeName))
			isDiskAttached, err := vcutil.IsVolumeAttachedToVM(client, vs, volHandle2, vmUUID)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			gomega.Expect(isDiskAttached).To(gomega.BeTrue(), "Volume is not attached to the node")
			/* TODO- test Bug
			ginkgo.By("Verify the volume is accessible and Read/write is possible")
			var cmd []string
			var wrtiecmd []string

			wrtiecmd = []string{"exec", pod.Name, "--namespace=" + namespace, "--", "/bin/sh", "-c",
				"echo 'Hello message from test into Pod1' >> /mnt/volume1/Pod1.html"}

			cmd = []string{"exec", pod.Name, "--namespace=" + namespace, "--", "/bin/sh", "-c",
				"cat ", constants.FilePathPod1}

			e2ekubectl.RunKubectlOrDie(namespace, wrtiecmd...)
			output := e2ekubectl.RunKubectlOrDie(namespace, cmd...)
			gomega.Expect(strings.Contains(output, "Hello message from test into Pod1")).NotTo(gomega.BeFalse())
			*/

		}
	}
}

// func testCleanUp(ctx context.Context, vs *config.E2eTestConfig, client clientset.Interface, namespace string) {
// 	adminClient, c := initializeClusterClientsByUserRoles(c)
// 	pvNames := sets.NewString()
// 	pvcPollErr := wait.PollUntilContextTimeout(ctx, constants.PollTimeoutShort, StatefulSetTimeout, true,
// 		func(ctx context.Context) (bool, error) {
// 			pvcList, err := c.CoreV1().PersistentVolumeClaims(namespace).List(context.TODO(),
// 				metav1.ListOptions{LabelSelector: labels.Everything().String()})
// 			if err != nil {
// 				framework.Logf("WARNING: Failed to list pvcs, retrying %v", err)
// 				return false, nil
// 			}
// 			for _, pvc := range pvcList.Items {
// 				pvNames.Insert(pvc.Spec.VolumeName)
// 				framework.Logf("Deleting pvc: %v with volume %v", pvc.Name, pvc.Spec.VolumeName)
// 				if err := c.CoreV1().PersistentVolumeClaims(namespace).Delete(ctx, pvc.Name,
// 					metav1.DeleteOptions{}); err != nil {
// 					return false, nil
// 				}
// 			}
// 			return true, nil
// 		})
// 	if pvcPollErr != nil {
// 		errList = append(errList, "Timeout waiting for pvc deletion.")
// 	}

// 	pollErr := wait.PollUntilContextTimeout(ctx, StatefulSetPoll, StatefulSetTimeout, true,
// 		func(ctx context.Context) (bool, error) {
// 			pvList, err := client.CoreV1().PersistentVolumes().List(context.TODO(),
// 				metav1.ListOptions{LabelSelector: labels.Everything().String()})
// 			if err != nil {
// 				framework.Logf("WARNING: Failed to list pvs, retrying %v", err)
// 				return false, nil
// 			}
// 			waitingFor := []string{}
// 			for _, pv := range pvList.Items {
// 				if pvNames.Has(pv.Name) {
// 					waitingFor = append(waitingFor, fmt.Sprintf("%v: %+v", pv.Name, pv.Status))
// 				}
// 			}
// 			if len(waitingFor) == 0 {
// 				return true, nil
// 			}
// 			framework.Logf("Still waiting for pvs of statefulset to disappear:\n%v", strings.Join(waitingFor, "\n"))
// 			return false, nil
// 		})
// 	if pollErr != nil {
// 		errList = append(errList, "Timeout waiting for pv provisioner to delete pvs, this might mean the test leaked pvs.")

// 	}
// }
