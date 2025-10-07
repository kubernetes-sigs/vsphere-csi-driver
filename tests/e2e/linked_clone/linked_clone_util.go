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

package linked_clone

import (
	"context"
	"fmt"
	"math/rand"
	"time"

	snapV1 "github.com/kubernetes-csi/external-snapshotter/client/v8/apis/volumesnapshot/v1"
	"github.com/onsi/ginkgo/v2"
	"github.com/onsi/gomega"
	"golang.org/x/crypto/ssh"
	corev1 "k8s.io/api/core/v1"
	storagev1 "k8s.io/api/storage/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	clientset "k8s.io/client-go/kubernetes"
	"k8s.io/kubernetes/test/e2e/framework"
	fpod "k8s.io/kubernetes/test/e2e/framework/pod"
	fpv "k8s.io/kubernetes/test/e2e/framework/pv"
	fss "k8s.io/kubernetes/test/e2e/framework/statefulset"
	"sigs.k8s.io/vsphere-csi-driver/v3/tests/e2e/config"
	"sigs.k8s.io/vsphere-csi-driver/v3/tests/e2e/constants"
	"sigs.k8s.io/vsphere-csi-driver/v3/tests/e2e/csisnapshot"
	"sigs.k8s.io/vsphere-csi-driver/v3/tests/e2e/env"
	"sigs.k8s.io/vsphere-csi-driver/v3/tests/e2e/k8testutil"
)

var pvcToDelete []*corev1.PersistentVolumeClaim
var snapClassToDelete []*snapV1.VolumeSnapshotClass
var snapContentToDelete []*snapV1.VolumeSnapshotContent
var snapToDelete []*snapV1.VolumeSnapshot
var podToDelete []*corev1.Pod
var lcToDelete []*corev1.PersistentVolumeClaim

/*
This method will create PVC, attach pod to it and creates snapshot
Keeping this method here as it populates the array used for cleanup
*/
func CreatePvcPodAndSnapshotMap(
	ctx context.Context,
	e2eTestConfig *config.E2eTestConfig,
	client clientset.Interface,
	namespace string,
	storageclass *storagev1.StorageClass,
	doCreatePod bool,
	doCreateDep bool,
) (
	*corev1.PersistentVolumeClaim,
	[]*corev1.PersistentVolume,
	*snapV1.VolumeSnapshot,
) {

	fmt.Println("Create PVC and verify PVC is bound")
	pvclaim, pv := k8testutil.CreateAndValidatePvc(ctx, client, namespace, storageclass)
	pvcToDelete = append(pvcToDelete, pvclaim)

	fmt.Println("Create Pod and attach to PVC")
	if doCreatePod || doCreateDep {
		pod, _ := k8testutil.CreatePodForPvc(
			ctx,
			e2eTestConfig,
			client,
			namespace,
			[]*corev1.PersistentVolumeClaim{pvclaim},
			doCreatePod,
			doCreateDep,
		)
		podToDelete = append(podToDelete, pod)
	}

	// TODO : Write data to volume

	fmt.Println("create volume snapshot")
	volumeSnapshot, _ := CreateVolumeSnapshotMap(ctx, e2eTestConfig, namespace, pvclaim, pv, constants.DiskSize)

	return pvclaim, pv, volumeSnapshot
}

/*
Create volume snapshotMap for volumeSnapshotClass, snapshotContent, volumeSnapshot
This method calls CreateVolumeSnapshot from util.
*/
func CreateVolumeSnapshotMap(
	ctx context.Context,
	e2eTestConfig *config.E2eTestConfig,
	namespace string,
	pvclaim *corev1.PersistentVolumeClaim,
	pv []*corev1.PersistentVolume,
	diskSize string,
) (
	*snapV1.VolumeSnapshot,
	*snapV1.VolumeSnapshotContent,
) {
	// Create snapshot class and dynamic snapshot
	volumeSnapshotClass, volumeSnapshot, snapshotContent := csisnapshot.CreateVolumeSnapshot(
		ctx,
		e2eTestConfig,
		namespace,
		pvclaim,
		pv,
		diskSize,
	)
	snapClassToDelete = append(snapClassToDelete, volumeSnapshotClass)
	snapContentToDelete = append(snapContentToDelete, snapshotContent)
	snapToDelete = append(snapToDelete, volumeSnapshot)

	return volumeSnapshot, snapshotContent
}

/*
Create PVC using linked clone annotation
*/
func CreateLinkedClonePvc(
	ctx context.Context,
	client clientset.Interface,
	namespace string,
	storageclass *storagev1.StorageClass,
	volumeSnapshotName string,
) (
	*corev1.PersistentVolumeClaim,
	error,
) {
	pvcspec := PvcSpecWithLinkedCloneAnnotation(
		namespace,
		storageclass,
		corev1.ReadWriteOnce,
		constants.Snapshotapigroup,
		volumeSnapshotName,
	)

	ginkgo.By(fmt.Sprintf("Creating linked-clone PVC in namespace: %s using Storage Class: %s",
		namespace, storageclass.Name))
	pvclaim, err := fpv.CreatePVC(ctx, client, namespace, pvcspec)
	gomega.Expect(err).NotTo(gomega.HaveOccurred(), fmt.Sprintf("Failed to create PVC: %v", err))
	framework.Logf("linked-clone PVC %v created successfully in namespace: %v", pvclaim.Name, namespace)

	// add to list to run cleanup
	lcToDelete = append(lcToDelete, pvclaim)

	return pvclaim, err
}

/*
Create linked clone PVC and verify its Bound
*/
func CreateAndValidateLinkedClone(
	ctx context.Context,
	client clientset.Interface,
	namespace string,
	storageclass *storagev1.StorageClass,
	volumeSnapshotName string,
) (
	*corev1.PersistentVolumeClaim,
	[]*corev1.PersistentVolume,
) {

	// create linked clone PVC
	pvclaim, err := CreateLinkedClonePvc(ctx, client, namespace, storageclass, volumeSnapshotName)
	gomega.Expect(err).NotTo(gomega.HaveOccurred(), fmt.Sprintf("Failed to create PVC: %v", err))

	// Validate PVC is bound
	pv, err := fpv.WaitForPVClaimBoundPhase(ctx,
		client, []*corev1.PersistentVolumeClaim{pvclaim}, framework.ClaimProvisionTimeout)
	gomega.Expect(err).NotTo(gomega.HaveOccurred())

	// Validate label and annotation
	framework.Logf("Verify linked-clone lable on the LC-PVC")
	pvcLable := pvclaim.Labels
	framework.Logf("Found linked-clone label: %s", pvcLable)
	gomega.Expect(pvcLable).To(gomega.HaveKeyWithValue("linked-clone", "true"))
	framework.Logf("Verify linked-clone annotation on the LC-PVC")
	annotationsMap := pvclaim.Annotations
	gomega.Expect(annotationsMap).To(gomega.HaveKeyWithValue(constants.LinkedCloneAnnotationKey, "true"))

	return pvclaim, pv
}

/*
This function generates a PVC specification with linked clone annotation.
*/
func PvcSpecWithLinkedCloneAnnotation(
	namespace string,
	storageclass *storagev1.StorageClass,
	accessMode corev1.PersistentVolumeAccessMode,
	snapshotapigroup string,
	datasourceName string,
) *corev1.PersistentVolumeClaim {

	disksize := constants.DiskSize
	claim := &corev1.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{
			GenerateName: "lc-pvc-",
			Namespace:    namespace,
			Annotations: map[string]string{
				constants.LinkedCloneAnnotationKey: "true",
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
			DataSource: &corev1.TypedLocalObjectReference{
				APIGroup: &snapshotapigroup,
				Kind:     "VolumeSnapshot",
				Name:     datasourceName,
			},
			StorageClassName: &(storageclass.Name),
		},
	}
	return claim
}

/*
Verify linked volume lists in list volume
*/
func ValidateLinkedCloneInListVolume(
	ctx context.Context,
	e2eTestConfig *config.E2eTestConfig,
	client clientset.Interface,
	pvc *corev1.PersistentVolumeClaim,
	namespace string,
) {
	ginkgo.By("Validate ListVolume Response for all the volumes")
	var logMessage string
	var sshClientConfig *ssh.ClientConfig
	var svcMasterPswd string
	containerName := constants.VSphereCSIControllerPodNamePrefix
	var volumeHandle []string

	if e2eTestConfig.TestInput.ClusterFlavor.VanillaCluster {
		logMessage = "List volume response: entries:"
		nimbusGeneratedK8sVmPwd := env.GetAndExpectStringEnvVar(constants.NimbusK8sVmPwd)
		sshClientConfig = getSshClient(nimbusGeneratedK8sVmPwd)
	}
	if e2eTestConfig.TestInput.ClusterFlavor.SupervisorCluster {
		logMessage = "ListVolumes:"
		svcMasterPswd = env.GetAndExpectStringEnvVar(constants.SvcMasterPassword)
		sshClientConfig = getSshClient(svcMasterPswd)
	}

	pv := k8testutil.GetPvFromClaim(client, namespace, pvc.Name)
	volumeHandle = append(volumeHandle, pv.Spec.CSI.VolumeHandle)

	//List volume responses will show up in the interval of every 1 minute.
	time.Sleep(constants.PollTimeoutShort)

	_, _, err := k8testutil.GetCSIPodWhereListVolumeResponseIsPresent(ctx, e2eTestConfig, client, sshClientConfig,
		containerName, logMessage, volumeHandle)
	gomega.Expect(err).NotTo(gomega.HaveOccurred())
}

/*
Function to create the ssh client
*/
func getSshClient(password string) *ssh.ClientConfig {
	return &ssh.ClientConfig{
		User: "root",
		Auth: []ssh.AuthMethod{
			ssh.Password(password),
		},
		HostKeyCallback: ssh.InsecureIgnoreHostKey(),
	}
}

/*
Run cleanup, delete all the resources created in the test
*/
func Cleanup(ctx context.Context, client clientset.Interface, e2eTestConfig *config.E2eTestConfig, namespace string) {
	snapc := csisnapshot.GetSnashotClientSet(e2eTestConfig)

	// Delete Pod
	for i := 0; i < len(podToDelete); i++ {
		ginkgo.By(fmt.Sprintf("Deleting the pod %s in namespace %s", podToDelete[0].Name, namespace))
		err := fpod.DeletePodWithWait(ctx, client, podToDelete[0])
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
	}

	// Delete linked clone PVC
	for i := 0; i < len(lcToDelete); i++ {
		err := fpv.DeletePersistentVolumeClaim(ctx, client, lcToDelete[0].Name, namespace)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
	}

	// Delete volume snapshot
	for i := 0; i < len(snapToDelete); i++ {
		framework.Logf("Deleting volume snapshot")
		csisnapshot.DeleteVolumeSnapshotWithPandoraWait(
			ctx,
			snapc,
			namespace,
			snapToDelete[0].Name,
			constants.DefaultPandoraSyncWaitTime,
		)

		framework.Logf("Wait till the volume snapshot is deleted")
		err := csisnapshot.WaitForVolumeSnapshotContentToBeDeletedWithPandoraWait(ctx, snapc,
			*snapToDelete[0].Status.BoundVolumeSnapshotContentName, constants.DefaultPandoraSyncWaitTime)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
	}

	// Delete snapshot content if created
	for i := 0; i < len(snapContentToDelete); i++ {
		err := csisnapshot.DeleteVolumeSnapshotContent(
			ctx,
			snapContentToDelete[0],
			snapc,
			constants.DefaultPandoraSyncWaitTime,
		)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
	}

	// Delete snapshot class if created
	for i := 0; i < len(snapClassToDelete); i++ {
		if e2eTestConfig.TestInput.ClusterFlavor.VanillaCluster {
			err := snapc.SnapshotV1().VolumeSnapshotClasses().Delete(ctx, snapClassToDelete[0].Name,
				metav1.DeleteOptions{})
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}
	}

	// Delete PVC
	for i := 0; i < len(pvcToDelete); i++ {
		err := fpv.DeletePersistentVolumeClaim(ctx, client, pvcToDelete[0].Name, namespace)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
	}
}

// This method can be used to create statefulset with linked clone annotation
func CreateAndValidateLinkedCloneWithSts(
	ctx context.Context,
	e2eTestConfig *config.E2eTestConfig,
	client clientset.Interface,
	namespace string,
	sc *storagev1.StorageClass,
	snapName string,
	snapshotapigroup string,
) {
	statefulset := k8testutil.GetStatefulSetFromManifest(e2eTestConfig.TestInput, namespace)
	ginkgo.By("Creating statefulset")
	annotations := map[string]string{
		"csi.vsphere.volume/fast-provisioning": "true",
	}
	statefulset.Spec.VolumeClaimTemplates[len(statefulset.Spec.VolumeClaimTemplates)-1].
		Spec.StorageClassName = &sc.Name

	// Add LC annotaion
	statefulset.Spec.VolumeClaimTemplates[len(statefulset.Spec.VolumeClaimTemplates)-1].
		ObjectMeta.SetAnnotations(annotations)

	// Add datastource
	statefulset.Spec.VolumeClaimTemplates[len(statefulset.Spec.VolumeClaimTemplates)-1].
		Spec.DataSource = new(corev1.TypedLocalObjectReference)
	statefulset.Spec.VolumeClaimTemplates[len(statefulset.Spec.VolumeClaimTemplates)-1].
		Spec.DataSource.Name = snapName
	statefulset.Spec.VolumeClaimTemplates[len(statefulset.Spec.VolumeClaimTemplates)-1].
		Spec.DataSource.Kind = "VolumeSnapshot"
	statefulset.Spec.VolumeClaimTemplates[len(statefulset.Spec.VolumeClaimTemplates)-1].
		Spec.DataSource.APIGroup = &snapshotapigroup

	// Change the name
	r := rand.New(rand.NewSource(time.Now().Unix()))
	name := fmt.Sprintf("web-%v", r.Intn(10000))
	statefulset.ObjectMeta.Name = name

	k8testutil.CreateStatefulSet(namespace, statefulset, client)
	replicas := *(statefulset.Spec.Replicas)
	// Waiting for pods status to be Ready.
	fss.WaitForStatusReadyReplicas(ctx, client, statefulset, replicas)

}
