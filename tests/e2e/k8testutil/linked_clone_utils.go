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

package k8testutil

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"time"

	snapV1 "github.com/kubernetes-csi/external-snapshotter/client/v8/apis/volumesnapshot/v1"
	snapclient "github.com/kubernetes-csi/external-snapshotter/client/v8/clientset/versioned"
	"github.com/onsi/ginkgo/v2"
	"github.com/onsi/gomega"
	"golang.org/x/crypto/ssh"
	appsv1 "k8s.io/api/apps/v1"
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
	"sigs.k8s.io/vsphere-csi-driver/v3/tests/e2e/config"
	"sigs.k8s.io/vsphere-csi-driver/v3/tests/e2e/constants"
	"sigs.k8s.io/vsphere-csi-driver/v3/tests/e2e/env"
	"sigs.k8s.io/vsphere-csi-driver/v3/tests/e2e/vcutil"
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
func CreatePvcPodAndSnapshot(ctx context.Context, e2eTestConfig *config.E2eTestConfig, client clientset.Interface, namespace string, storageclass *v1.StorageClass, doCreatePod bool, doCreateDep bool) *snapV1.VolumeSnapshot {

	// Create PVC and verify PVC is bound
	pvclaim, pv := createAndValidatePvc(ctx, client, namespace, storageclass)

	// Create Pod and attach to PVC
	if doCreatePod || doCreateDep {
		CreatePodForPvc(ctx, e2eTestConfig, client, namespace, pvclaim, doCreatePod, doCreateDep)
	}

	// TODO : Write data to volume

	// create volume snapshot
	volumeSnapshot := CreateVolumeSnapshot(ctx, e2eTestConfig, namespace, pvclaim, pv)

	return volumeSnapshot
}

func CreatePodForPvc(ctx context.Context, e2eTestConfig *config.E2eTestConfig, client clientset.Interface, namespace string, pvclaim *corev1.PersistentVolumeClaim, deCreatePod bool, doCreateDep bool) (*corev1.Pod, *appsv1.Deployment) {
	ginkgo.By("Create Pod to attach to Pvc")
	var pod *corev1.Pod
	var dep *appsv1.Deployment
	var err error
	if deCreatePod {
		pod, err = CreatePod(ctx, e2eTestConfig, client, namespace, nil, []*corev1.PersistentVolumeClaim{pvclaim}, false,
			constants.ExecRWXCommandPod1)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		podToDelete = append(podToDelete, pod)
	} else if doCreateDep {
		labelsMap := make(map[string]string)
		labelsMap["app"] = "test"
		dep, err = CreateDeployment(ctx, e2eTestConfig, client, 1, labelsMap, nil, namespace,
			[]*corev1.PersistentVolumeClaim{pvclaim}, constants.ExecRWXCommandPod1, false, constants.NginxImage)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
	}
	return pod, dep
}

/*
Create volume snapshot
*/
func CreateVolumeSnapshot(ctx context.Context, e2eTestConfig *config.E2eTestConfig, namespace string, pvclaim *corev1.PersistentVolumeClaim, pv []*corev1.PersistentVolume) *snapV1.VolumeSnapshot {
	// Create or get volume snapshot class
	ginkgo.By("Get or create volume snapshot class")
	snapc := getSnashotClientSet(e2eTestConfig)
	volumeSnapshotClass, err := CreateVolumeSnapshotClass(ctx, e2eTestConfig, snapc, constants.DeletionPolicy)
	gomega.Expect(err).NotTo(gomega.HaveOccurred())

	// Add volumesnapshotclass to the list to be deleted
	snapClassToDelete = append(snapClassToDelete, volumeSnapshotClass)

	// Create volume snapshot
	ginkgo.By("Create a volume snapshot")
	volumeSnapshot, snapshotContent, _,
		_, _, _, err := CreateDynamicVolumeSnapshot(ctx, e2eTestConfig, namespace, snapc, volumeSnapshotClass,
		pvclaim, pv[0].Spec.CSI.VolumeHandle, constants.DiskSize, true)
	gomega.Expect(err).NotTo(gomega.HaveOccurred())

	snapContentToDelete = append(snapContentToDelete, snapshotContent)
	snapToDelete = append(snapToDelete, volumeSnapshot)

	return volumeSnapshot
}

/*
Create and validate PVC status
*/
func createAndValidatePvc(ctx context.Context, client clientset.Interface, namespace string, storageclass *v1.StorageClass) (*corev1.PersistentVolumeClaim, []*corev1.PersistentVolume) {
	ginkgo.By("Create PVC")
	pvclaim, err := CreatePVC(ctx, client, namespace, nil, "", storageclass, "")
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
func getSnashotClientSet(e2eTestConfig *config.E2eTestConfig) *snapclient.Clientset {
	var restConfig *rest.Config
	if e2eTestConfig.TestInput.ClusterFlavor.GuestCluster {
		restConfig = GetRestConfigClientForGuestCluster(nil)
	} else {
		restConfig = vcutil.GetRestConfigClient(e2eTestConfig)
	}
	snapc, err := snapclient.NewForConfig(restConfig)
	gomega.Expect(err).NotTo(gomega.HaveOccurred())
	return snapc
}

/*
Create PVC using linked clone annotation
*/
func createLinkedClonePvc(ctx context.Context, client clientset.Interface, namespace string, storageclass *storagev1.StorageClass, volumeSnapshotName string) (*corev1.PersistentVolumeClaim, error) {
	pvcspec := PvcSpecWithLinkedCloneAnnotation(namespace, storageclass, corev1.ReadWriteOnce, constants.Snapshotapigroup, volumeSnapshotName)
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
func CreateAndValidateLinkedClone(ctx context.Context, client clientset.Interface, namespace string, storageclass *storagev1.StorageClass, volumeSnapshotName string) (*corev1.PersistentVolumeClaim, []*corev1.PersistentVolume) {

	// create linked clone PVC
	pvclaim, err := createLinkedClonePvc(ctx, client, namespace, storageclass, volumeSnapshotName)
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
func PvcSpecWithLinkedCloneAnnotation(namespace string, storageclass *storagev1.StorageClass, accessMode corev1.PersistentVolumeAccessMode, snapshotapigroup string, datasourceName string) *corev1.PersistentVolumeClaim {
	disksize := constants.DiskSize
	claim := &corev1.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{
			GenerateName: "lc-pvc-",
			Namespace:    namespace,
			Annotations: map[string]string{
				"csi.vsphere.volume/fast-provisioning": "true",
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
func ValidateLcInListVolume(ctx context.Context, e2eTestConfig *config.E2eTestConfig, client clientset.Interface, pvc *corev1.PersistentVolumeClaim, namespace string) {
	ginkgo.By("Validate ListVolume Response for all the volumes")
	var logMessage string
	var sshClientConfig *ssh.ClientConfig
	var svcMasterPswd string
	containerName := "vsphere-csi-controller"
	var volumeHandle []string

	if e2eTestConfig.TestInput.ClusterFlavor.VanillaCluster {
		logMessage = "List volume response: entries:"
		nimbusGeneratedK8sVmPwd := env.GetAndExpectStringEnvVar(constants.NimbusK8sVmPwd)
		sshClientConfig = &ssh.ClientConfig{
			User: "root",
			Auth: []ssh.AuthMethod{
				ssh.Password(nimbusGeneratedK8sVmPwd),
			},
			HostKeyCallback: ssh.InsecureIgnoreHostKey(),
		}
	}
	if e2eTestConfig.TestInput.ClusterFlavor.SupervisorCluster {
		logMessage = "ListVolumes:"
		svcMasterPswd = env.GetAndExpectStringEnvVar(constants.SvcMasterPassword)
		sshClientConfig = &ssh.ClientConfig{
			User: "root",
			Auth: []ssh.AuthMethod{
				ssh.Password(svcMasterPswd),
			},
			HostKeyCallback: ssh.InsecureIgnoreHostKey(),
		}
	}

	pv := GetPvFromClaim(client, namespace, pvc.Name)
	volumeHandle = append(volumeHandle, pv.Spec.CSI.VolumeHandle)

	//List volume responses will show up in the interval of every 1 minute.
	time.Sleep(constants.PollTimeoutShort)

	_, _, err := GetCSIPodWhereListVolumeResponseIsPresent(ctx, e2eTestConfig, client, sshClientConfig,
		containerName, logMessage, volumeHandle)
	gomega.Expect(err).NotTo(gomega.HaveOccurred())
}

/*
Run cleanup, delete all the resources created in the test
*/
func Cleanup(ctx context.Context, client clientset.Interface, e2eTestConfig *config.E2eTestConfig, namespace string) {
	snapc := getSnashotClientSet(e2eTestConfig)

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
		DeleteVolumeSnapshotWithPandoraWait(ctx, snapc, namespace, snapToDelete[0].Name, constants.DefaultPandoraSyncWaitTime)

		framework.Logf("Wait till the volume snapshot is deleted")
		err := WaitForVolumeSnapshotContentToBeDeletedWithPandoraWait(ctx, snapc,
			*snapToDelete[0].Status.BoundVolumeSnapshotContentName, constants.DefaultPandoraSyncWaitTime)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
	}

	// Delete snapshot content if created
	for i := 0; i < len(snapContentToDelete); i++ {
		err := DeleteVolumeSnapshotContent(ctx, snapContentToDelete[0], snapc, constants.DefaultPandoraSyncWaitTime)
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

func ParseMi(value string) (int, error) {
	// Remove the "Mi" suffix and convert to int
	numeric := strings.TrimSuffix(value, "Mi")
	return strconv.Atoi(numeric)
}
