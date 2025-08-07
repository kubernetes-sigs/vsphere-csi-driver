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

	"github.com/onsi/ginkgo/v2"
	v1 "k8s.io/api/core/v1"
	storagev1 "k8s.io/api/storage/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	clientset "k8s.io/client-go/kubernetes"
	"k8s.io/kubernetes/test/e2e/framework"

	fpv "k8s.io/kubernetes/test/e2e/framework/pv"
)

func createPVCSpecWithDifferentConfigurations(namespace string, ds string, storageclass *storagev1.StorageClass,
	pvclaimlabels map[string]string, accessMode v1.PersistentVolumeAccessMode,
	volumeMode v1.PersistentVolumeMode) *v1.PersistentVolumeClaim {

	// Set default values if not provided
	if namespace == "" {
		namespace = "default"
	}
	if ds == "" {
		ds = "1Gi"
	}
	if accessMode == "" {
		accessMode = v1.ReadWriteOnce
	}

	// Construct the PVC spec without volumeMode
	pvcSpec := v1.PersistentVolumeClaimSpec{
		AccessModes: []v1.PersistentVolumeAccessMode{accessMode},
		Resources: v1.VolumeResourceRequirements{
			Requests: v1.ResourceList{
				v1.ResourceStorage: resource.MustParse(ds),
			},
		},
	}

	// Set storageClassName if provided
	if storageclass != nil && storageclass.Name != "" {
		pvcSpec.StorageClassName = &storageclass.Name
	}

	// Only set volumeMode if it was passed explicitly
	if volumeMode != "" {
		pvcSpec.VolumeMode = &volumeMode
	}

	// Create the final PVC object
	claim := &v1.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{
			GenerateName: "pvc-",
			Namespace:    namespace,
			Labels:       pvclaimlabels,
		},
		Spec: pvcSpec,
	}

	return claim
}

func createPVCsOnScaleWithDifferentConfigurations(
	ctx context.Context,
	client clientset.Interface,
	namespace string,
	pvclaimlabels map[string]string,
	diskSize string,
	storageclass *storagev1.StorageClass,
	accessMode v1.PersistentVolumeAccessMode,
	volumeMode v1.PersistentVolumeMode,
	createPvcCountLimit int,
) ([]*v1.PersistentVolumeClaim, error) {

	ginkgo.By(fmt.Sprintf(" Namespace: %q, DiskSize: %q, AccessMode: %q, VolumeMode: %q, Labels: %+v, StorageClass: %+v, PVC Count: %d",
		namespace, diskSize, accessMode, volumeMode, pvclaimlabels, storageclass, createPvcCountLimit,
	))

	var createdPVCs []*v1.PersistentVolumeClaim

	for i := 0; i < createPvcCountLimit; i++ {
		pvcspec := createPVCSpecWithDifferentConfigurations(
			namespace, diskSize, storageclass, pvclaimlabels, accessMode, volumeMode,
		)

		ginkgo.By(fmt.Sprintf("Creating PVC [%d/%d] with StorageClass: %+v, DiskSize: %q, AccessMode: %q, VolumeMode: %q, Labels: %+v",
			i+1, createPvcCountLimit, storageclass, diskSize, accessMode, volumeMode, pvclaimlabels,
		))

		pvc, err := fpv.CreatePVC(ctx, client, namespace, pvcspec)
		if err != nil {
			return nil, fmt.Errorf("failed to create PVC [%d/%d]: %v", i+1, createPvcCountLimit, err)
		}

		framework.Logf("PVC created: %s in namespace: %s", pvc.Name, namespace)
		createdPVCs = append(createdPVCs, pvc)
	}

	return createdPVCs, nil
}
