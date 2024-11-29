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

	"github.com/onsi/gomega"
	corev1 "k8s.io/api/core/v1"
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
