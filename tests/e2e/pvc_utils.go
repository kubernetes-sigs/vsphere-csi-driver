/*
Copyright 2024 The Kubernetes Authors.

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

	"github.com/onsi/ginkgo/v2"
	"github.com/onsi/gomega"
	corev1 "k8s.io/api/core/v1"
	storagev1 "k8s.io/api/storage/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	clientset "k8s.io/client-go/kubernetes"
	"k8s.io/kubernetes/test/e2e/framework"
	fpv "k8s.io/kubernetes/test/e2e/framework/pv"
	"sigs.k8s.io/vsphere-csi-driver/v3/pkg/common/cns-lib/crypto"
	"sigs.k8s.io/vsphere-csi-driver/v3/pkg/csi/service/common"
)

type PersistentVolumeClaimOptions struct {
	Namespace           string
	StorageClassName    string
	EncryptionClassName string
	SnapshotName        string
	Size                string
}

func buildPersistentVolumeClaimSpec(opts PersistentVolumeClaimOptions) *corev1.PersistentVolumeClaim {
	if opts.Size == "" {
		opts.Size = diskSizeSmall
	}

	pvc := &corev1.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{
			GenerateName: "pvc-",
			Namespace:    opts.Namespace,
		},
		Spec: corev1.PersistentVolumeClaimSpec{
			AccessModes: []corev1.PersistentVolumeAccessMode{
				corev1.ReadWriteOnce,
			},
			Resources: corev1.VolumeResourceRequirements{
				Requests: corev1.ResourceList{
					corev1.ResourceName(corev1.ResourceStorage): resource.MustParse(opts.Size),
				},
			},
		},
	}

	if opts.StorageClassName != "" {
		pvc.Spec.StorageClassName = &opts.StorageClassName
	}

	if opts.EncryptionClassName != "" {
		pvc.Annotations = map[string]string{
			crypto.PVCEncryptionClassAnnotationName: opts.EncryptionClassName,
		}
	}

	if opts.SnapshotName != "" {
		snapshotApiGroup := common.VolumeSnapshotApiGroup
		volumeSnapshotKind := common.VolumeSnapshotKind
		pvc.Spec.DataSource = &corev1.TypedLocalObjectReference{
			APIGroup: &snapshotApiGroup,
			Kind:     volumeSnapshotKind,
			Name:     opts.SnapshotName,
		}
	}

	return pvc
}

func createPersistentVolumeClaim(
	ctx context.Context,
	client clientset.Interface,
	opts PersistentVolumeClaimOptions) *corev1.PersistentVolumeClaim {

	pvc := buildPersistentVolumeClaimSpec(opts)

	var err error
	pvc, err = client.
		CoreV1().
		PersistentVolumeClaims(opts.Namespace).
		Create(ctx, pvc, metav1.CreateOptions{})
	gomega.Expect(err).NotTo(gomega.HaveOccurred())

	_, err = fpv.WaitForPVClaimBoundPhase(
		ctx,
		client,
		[]*corev1.PersistentVolumeClaim{pvc},
		framework.ClaimProvisionTimeout*2)
	gomega.Expect(err).NotTo(gomega.HaveOccurred())

	pvc, err = client.
		CoreV1().
		PersistentVolumeClaims(opts.Namespace).
		Get(ctx, pvc.Name, metav1.GetOptions{})
	gomega.Expect(err).NotTo(gomega.HaveOccurred())
	gomega.Expect(pvc.Spec.VolumeName).NotTo(gomega.BeEmpty())

	return pvc
}

func deletePersistentVolumeClaim(
	ctx context.Context,
	client clientset.Interface,
	pvc *corev1.PersistentVolumeClaim) {

	if pvc != nil {
		err := client.
			CoreV1().
			PersistentVolumeClaims(pvc.Namespace).
			Delete(ctx, pvc.Name, metav1.DeleteOptions{})

		gomega.Expect(err).NotTo(gomega.HaveOccurred())
	}
}

// createStaticPVandPVCinGuestCluster creates static PV and PVC in guest cluster.
func createStaticPVandPVCinGuestCluster(client clientset.Interface, ctx context.Context, namespace string,
	svpvcName string, size string, storageclass *storagev1.StorageClass,
	pvReclaimPolicy corev1.PersistentVolumeReclaimPolicy) (*corev1.PersistentVolumeClaim, *corev1.PersistentVolume) {
	ginkgo.By("Creating PV in guest cluster")
	gcPV := getPersistentVolumeSpecWithStorageclass(svpvcName, pvReclaimPolicy,
		storageclass.Name, nil, size)
	gcPV, err := client.CoreV1().PersistentVolumes().Create(ctx, gcPV, metav1.CreateOptions{})
	gomega.Expect(err).NotTo(gomega.HaveOccurred())
	gcPVName := gcPV.GetName()
	framework.Logf("PV name in GC : %s", gcPVName)
	err = fpv.WaitForPersistentVolumePhase(ctx, "Available", client, gcPVName, poll, pollTimeout)
	gomega.Expect(err).NotTo(gomega.HaveOccurred())

	ginkgo.By("Creating PVC in guest cluster")
	gcPVC := getPVCSpecWithPVandStorageClass(svpvcName, namespace, nil, gcPVName, storageclass.Name, size)
	gcPVC, err = client.CoreV1().PersistentVolumeClaims(namespace).Create(ctx, gcPVC, metav1.CreateOptions{})
	gomega.Expect(err).NotTo(gomega.HaveOccurred())

	ginkgo.By("Waiting for claim to be in bound phase")
	err = fpv.WaitForPersistentVolumeClaimPhase(ctx, corev1.ClaimBound, client,
		namespace, gcPVC.Name, framework.Poll, framework.ClaimProvisionTimeout)
	gomega.Expect(err).NotTo(gomega.HaveOccurred())

	framework.Logf("PVC name in GC : %s", gcPVC.GetName())

	return gcPVC, gcPV
}

// createStaticPVandPVCandPODinGuestCluster creates static PV and PVC in guest cluster.
func createStaticPVandPVCandPODinGuestCluster(client clientset.Interface,
	ctx context.Context, namespace string, svpvcName string, size string, storageclass *storagev1.StorageClass,
	pvReclaimPolicy corev1.PersistentVolumeReclaimPolicy) (*corev1.PersistentVolumeClaim, *corev1.PersistentVolume,
	*corev1.Pod, string) {

	gcPVC, gcPV := createStaticPVandPVCinGuestCluster(client, ctx, namespace,
		svpvcName, size, storageclass, pvReclaimPolicy)
	// Create a Pod to use this PVC, and verify volume has been attached.
	ginkgo.By("Creating pod to attach PV to the node")
	pod, err := createPod(ctx, client, namespace, nil, []*corev1.PersistentVolumeClaim{gcPVC}, false, execCommand)
	gomega.Expect(err).NotTo(gomega.HaveOccurred())

	ginkgo.By(fmt.Sprintf("Verify volume: %s is attached to the node: %s",
		gcPV.Spec.CSI.VolumeHandle, pod.Spec.NodeName))
	vmUUID, err := getVMUUIDFromNodeName(pod.Spec.NodeName)
	gomega.Expect(err).NotTo(gomega.HaveOccurred())
	volHandle := getVolumeIDFromSupervisorCluster(gcPV.Spec.CSI.VolumeHandle)
	isDiskAttached, err := e2eVSphere.isVolumeAttachedToVM(client, volHandle, vmUUID)
	gomega.Expect(err).NotTo(gomega.HaveOccurred())
	gomega.Expect(isDiskAttached).To(gomega.BeTrue(), "Volume is not attached to the node")

	return gcPVC, gcPV, pod, vmUUID

}

// checkSvcPvcHasGivenStatusCondition checks if the status condition in SVC PVC
// matches with the one we want.
func checkSvcPvcHasGivenStatusCondition(pvcName string, conditionsPresent bool,
	condition corev1.PersistentVolumeClaimConditionType) (*corev1.PersistentVolumeClaim, error) {
	svcClient, svcNamespace := getSvcClientAndNamespace()
	return checkPvcHasGivenStatusCondition(svcClient, svcNamespace, pvcName, conditionsPresent, condition)
}

// verifyResizeCompletedInSupervisor checks if resize relates status conditions
// are removed from PVC and Pvc.Spec.Resources.Requests[v1.ResourceStorage] is
// same as pvc.Status.Capacity[v1.ResourceStorage] in SVC.
func verifyResizeCompletedInSupervisor(pvcName string) bool {
	pvc := getPVCFromSupervisorCluster(pvcName)
	pvcRequestedSize := pvc.Spec.Resources.Requests[corev1.ResourceStorage]
	pvcCapacitySize := pvc.Status.Capacity[corev1.ResourceStorage]
	// If pvc's capacity size is greater than or equal to pvc's request size
	// then done.
	if pvcCapacitySize.Cmp(pvcRequestedSize) < 0 {
		return false
	}
	if len(pvc.Status.Conditions) == 0 {
		return true
	}
	return false
}
