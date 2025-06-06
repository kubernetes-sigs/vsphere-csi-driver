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

package e2e

import (
	"context"
	"fmt"

	snapV1 "github.com/kubernetes-csi/external-snapshotter/client/v8/apis/volumesnapshot/v1"
	snapclient "github.com/kubernetes-csi/external-snapshotter/client/v8/clientset/versioned"
	"github.com/onsi/ginkgo/v2"
	"github.com/onsi/gomega"
	"golang.org/x/crypto/ssh"
	corev1 "k8s.io/api/core/v1"
	storagev1 "k8s.io/api/storage/v1"
	v1 "k8s.io/api/storage/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	clientset "k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/kubernetes/test/e2e/framework"
	fpod "k8s.io/kubernetes/test/e2e/framework/pod"
	fpv "k8s.io/kubernetes/test/e2e/framework/pv"
)

var pvcToDelete []*corev1.PersistentVolumeClaim
var snapClassToDelete []*snapV1.VolumeSnapshotClass
var snapContentToDelete []*snapV1.VolumeSnapshotContent
var snapToDelete []*snapV1.VolumeSnapshot
var podToDelete []*corev1.Pod
var lcToDelete []*corev1.PersistentVolumeClaim

/*
This method will create PVC, attach pod to it and creates snapshot
*/
func createPvcPodAndSnapshot(ctx context.Context, client clientset.Interface, namespace string, storageclass *v1.StorageClass, doCreatePod bool) {

	// Create PVC and verify PVC is bound
	pvclaim, pv := createAndValidatePvc(ctx, client, namespace, storageclass)

	// Create Pod and attach to PVC
	if doCreatePod {
		_ = createPodForPvc(ctx, client, namespace, pvclaim)
	}

	// create volume snapshot
	createVolumeSnapshot(ctx, namespace, pvclaim, pv)
}

func createPodForPvc(ctx context.Context, client clientset.Interface, namespace string, pvclaim *corev1.PersistentVolumeClaim) *corev1.Pod {
	ginkgo.By("Create Pod to attach to Pvc")
	pod, err := createPod(ctx, client, namespace, nil, []*corev1.PersistentVolumeClaim{pvclaim}, false,
		execRWXCommandPod1)
	gomega.Expect(err).NotTo(gomega.HaveOccurred())

	podToDelete = append(podToDelete, pod)
	return pod
}

/*
Create volume snapshot
*/
func createVolumeSnapshot(ctx context.Context, namespace string, pvclaim *corev1.PersistentVolumeClaim, pv []*corev1.PersistentVolume) {
	// Create or get volume snapshot class
	ginkgo.By("Get or create volume snapshot class")
	snapc := getSnashotClientSet()
	volumeSnapshotClass, err := createVolumeSnapshotClass(ctx, snapc, deletionPolicy)
	gomega.Expect(err).NotTo(gomega.HaveOccurred())

	// Add volumesnapshotclass to the list to be deleted
	snapClassToDelete = append(snapClassToDelete, volumeSnapshotClass)

	// Create volume snapshot
	ginkgo.By("Create a volume snapshot")
	volumeSnapshot, snapshotContent, _,
		_, _, _, err := createDynamicVolumeSnapshot(ctx, namespace, snapc, volumeSnapshotClass,
		pvclaim, pv[0].Spec.CSI.VolumeHandle, diskSize, true)
	gomega.Expect(err).NotTo(gomega.HaveOccurred())

	snapContentToDelete = append(snapContentToDelete, snapshotContent)
	snapToDelete = append(snapToDelete, volumeSnapshot)
}

/*
Create and validate PVC status
*/
func createAndValidatePvc(ctx context.Context, client clientset.Interface, namespace string, storageclass *v1.StorageClass) (*corev1.PersistentVolumeClaim, []*corev1.PersistentVolume) {
	ginkgo.By("Create PVC")
	pvclaim, err := createPVC(ctx, client, namespace, nil, "", storageclass, "")
	gomega.Expect(err).NotTo(gomega.HaveOccurred())

	// Validate PVC is bound
	gomega.Expect(err).NotTo(gomega.HaveOccurred())
	pv, err := fpv.WaitForPVClaimBoundPhase(ctx,
		client, []*corev1.PersistentVolumeClaim{pvclaim}, framework.ClaimProvisionTimeout)
	gomega.Expect(err).NotTo(gomega.HaveOccurred())

	pvcToDelete = append(pvcToDelete, pvclaim)

	return pvclaim, pv
}

/*
Get snashot client set
*/
func getSnashotClientSet() *snapclient.Clientset {
	var restConfig *rest.Config
	if guestCluster {
		restConfig = getRestConfigClientForGuestCluster(nil)
	} else {
		restConfig = getRestConfigClient()
	}
	snapc, err := snapclient.NewForConfig(restConfig)
	gomega.Expect(err).NotTo(gomega.HaveOccurred())
	return snapc
}

/*
Create PVC using linked clone annotation
*/
func createLinkedClonePvc(ctx context.Context, client clientset.Interface, namespace string, storageclass *storagev1.StorageClass) (*corev1.PersistentVolumeClaim, error) {
	pvcspec := PvcSpecWithLinkedCloneAnnotation(namespace, storageclass)
	ginkgo.By(fmt.Sprintf("Creating PVC in namespace: %s using Storage Class: %s",
		namespace, storageclass.Name))
	pvclaim, err := fpv.CreatePVC(ctx, client, namespace, pvcspec)
	gomega.Expect(err).NotTo(gomega.HaveOccurred(), fmt.Sprintf("Failed to create PVC: %v", err))
	framework.Logf("PVC %v created successfully in namespace: %v", pvclaim.Name, namespace)

	// add to list to run cleanup
	lcToDelete = append(lcToDelete, pvclaim)

	return pvclaim, err
}

/*
Create linked clone PVC and verify its Bound
*/
func createAndValidateLinkedClone(ctx context.Context, client clientset.Interface, namespace string, storageclass *storagev1.StorageClass) *corev1.PersistentVolumeClaim {

	// create linked clone PVC
	pvclaim, err := createLinkedClonePvc(ctx, client, namespace, storageclass)
	gomega.Expect(err).NotTo(gomega.HaveOccurred(), fmt.Sprintf("Failed to create PVC: %v", err))

	// Validate PVC is bound
	_, err = fpv.WaitForPVClaimBoundPhase(ctx,
		client, []*corev1.PersistentVolumeClaim{pvclaim}, framework.ClaimProvisionTimeout)
	gomega.Expect(err).NotTo(gomega.HaveOccurred())

	return pvclaim
}

/*
This function generates a PVC specification with linked clone annotation.
*/
func PvcSpecWithLinkedCloneAnnotation(namespace string, storageclass *storagev1.StorageClass) *corev1.PersistentVolumeClaim {
	disksize := diskSize
	claim := &corev1.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{
			GenerateName: "pvc-",
			Namespace:    namespace,
			Annotations: map[string]string{
				"csi.vsphere.volume/linked-clone": "true",
			},
		},
		Spec: corev1.PersistentVolumeClaimSpec{
			AccessModes: []corev1.PersistentVolumeAccessMode{
				accessMode,
			},
			Resources: corev1.VolumeResourceRequirements{
				Requests: corev1.ResourceList{
					corev1.ResourceName(corev1.ResourceStorage): resource.MustParse(disksize),
				},
			},
			StorageClassName: &(storageclass.Name),
		},
	}
	return claim
}

func validateLcInListVolume(ctx context.Context, client clientset.Interface, pvc *corev1.PersistentVolumeClaim, namespace string) {
	ginkgo.By("Validate ListVolume Response for all the volumes")
	var logMessage string
	var sshClientConfig *ssh.ClientConfig
	var svcMasterPswd string
	containerName := "vsphere-csi-controller"
	var volumeHandle []string
	if vanillaCluster {
		logMessage = "List volume response: entries:"
		nimbusGeneratedK8sVmPwd := GetAndExpectStringEnvVar(nimbusK8sVmPwd)
		sshClientConfig = &ssh.ClientConfig{
			User: "root",
			Auth: []ssh.AuthMethod{
				ssh.Password(nimbusGeneratedK8sVmPwd),
			},
			HostKeyCallback: ssh.InsecureIgnoreHostKey(),
		}
	}
	if supervisorCluster {
		logMessage = "ListVolumes:"
		svcMasterPswd = GetAndExpectStringEnvVar(svcMasterPassword)
		sshClientConfig = &ssh.ClientConfig{
			User: "root",
			Auth: []ssh.AuthMethod{
				ssh.Password(svcMasterPswd),
			},
			HostKeyCallback: ssh.InsecureIgnoreHostKey(),
		}
	}

	pv := getPvFromClaim(client, namespace, pvc.Name)
	volumeHandle = append(volumeHandle, pv.Spec.CSI.VolumeHandle)
	_, _, err := getCSIPodWhereListVolumeResponseIsPresent(ctx, client, sshClientConfig,
		containerName, logMessage, volumeHandle)
	gomega.Expect(err).NotTo(gomega.HaveOccurred())
}

/*
Run cleanup, delete all the resources created in the test
*/
func cleanup(ctx context.Context, client clientset.Interface, namespace string) {
	snapc := getSnashotClientSet()

	// Delete linked clone PVC
	for i := 0; i < len(lcToDelete); i++ {
		err := fpv.DeletePersistentVolumeClaim(ctx, client, lcToDelete[0].Name, namespace)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
	}

	// Delete volume snapshot
	for i := 0; i < len(snapToDelete); i++ {
		framework.Logf("Deleting volume snapshot")
		deleteVolumeSnapshotWithPandoraWait(ctx, snapc, namespace, snapToDelete[0].Name, defaultPandoraSyncWaitTime)

		framework.Logf("Wait till the volume snapshot is deleted")
		err := waitForVolumeSnapshotContentToBeDeletedWithPandoraWait(ctx, snapc,
			*snapToDelete[0].Status.BoundVolumeSnapshotContentName, defaultPandoraSyncWaitTime)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
	}

	// Delete snapshot content if created
	for i := 0; i < len(snapContentToDelete); i++ {
		err := deleteVolumeSnapshotContent(ctx, snapContentToDelete[0], snapc, defaultPandoraSyncWaitTime)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
	}

	// Delete snapshot class if created
	for i := 0; i < len(snapClassToDelete); i++ {
		if vanillaCluster {
			err := snapc.SnapshotV1().VolumeSnapshotClasses().Delete(ctx, snapClassToDelete[0].Name,
				metav1.DeleteOptions{})
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}
	}

	// Delete Pod
	for i := 0; i < len(podToDelete); i++ {
		ginkgo.By(fmt.Sprintf("Deleting the pod %s in namespace %s", podToDelete[0].Name, namespace))
		err := fpod.DeletePodWithWait(ctx, client, podToDelete[0])
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
	}

	// Delete PVC
	for i := 0; i < len(pvcToDelete); i++ {
		err := fpv.DeletePersistentVolumeClaim(ctx, client, pvcToDelete[0].Name, namespace)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
	}
}
