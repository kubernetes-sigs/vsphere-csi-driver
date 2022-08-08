/*
Copyright 2020 The Kubernetes Authors.

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
	"strings"

	ginkgo "github.com/onsi/ginkgo/v2"
	"github.com/onsi/gomega"

	v1 "k8s.io/api/core/v1"
	storagev1 "k8s.io/api/storage/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	clientset "k8s.io/client-go/kubernetes"
	fpv "k8s.io/kubernetes/test/e2e/framework/pv"
)

// getVcpVSphereStorageClassSpec to get VCP storage class spec
func getVcpVSphereStorageClassSpec(name string, scParameters map[string]string, zones []string,
	ReclaimPolicy v1.PersistentVolumeReclaimPolicy, bindingMode storagev1.VolumeBindingMode,
	allowVolumeExpansion bool) *storagev1.StorageClass {
	if bindingMode == "" {
		bindingMode = storagev1.VolumeBindingImmediate
	}
	sc := &storagev1.StorageClass{
		TypeMeta: metav1.TypeMeta{
			Kind: "StorageClass",
		},
		ObjectMeta: metav1.ObjectMeta{
			GenerateName: "vcp-sc-",
		},
		Provisioner:       vcpProvisionerName,
		VolumeBindingMode: &bindingMode,
	}
	if name != "" {
		sc.ObjectMeta.Name = name
	}
	if scParameters != nil {
		sc.Parameters = scParameters
	}
	if zones != nil {
		term := v1.TopologySelectorTerm{
			MatchLabelExpressions: []v1.TopologySelectorLabelRequirement{
				{
					Key:    v1.LabelZoneFailureDomain,
					Values: zones,
				},
			},
		}
		sc.AllowedTopologies = append(sc.AllowedTopologies, term)
	}
	if ReclaimPolicy != "" {
		sc.ReclaimPolicy = &ReclaimPolicy
	}
	if allowVolumeExpansion {
		sc.AllowVolumeExpansion = &allowVolumeExpansion
	}
	return sc
}

// createVcpStorageClass helps creates a VCP storage class with specified name, storageclass parameters
func createVcpStorageClass(client clientset.Interface, scParameters map[string]string, zones []string,
	scReclaimPolicy v1.PersistentVolumeReclaimPolicy, bindingMode storagev1.VolumeBindingMode,
	allowVolumeExpansion bool, scName string) (*storagev1.StorageClass, error) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	ginkgo.By(fmt.Sprintf("Creating StorageClass %s with scParameters: %+v and zones: %+v and "+
		"ReclaimPolicy: %+v and allowVolumeExpansion: %t",
		scName, scParameters, zones, scReclaimPolicy, allowVolumeExpansion))
	storageclass, err := client.StorageV1().StorageClasses().Create(ctx,
		getVcpVSphereStorageClassSpec(scName, scParameters, zones, scReclaimPolicy, bindingMode, allowVolumeExpansion),
		metav1.CreateOptions{})
	gomega.Expect(err).NotTo(gomega.HaveOccurred(), fmt.Sprintf("Failed to create storage class with err: %v", err))
	return storageclass, err
}

// getvSphereVolumePathFromClaim fetches vSphere Volume Path from PVC which used VCP PV
func getvSphereVolumePathFromClaim(ctx context.Context, client clientset.Interface,
	namespace string, claimName string) string {
	pvclaim, err := client.CoreV1().PersistentVolumeClaims(namespace).Get(ctx, claimName, metav1.GetOptions{})
	gomega.Expect(err).NotTo(gomega.HaveOccurred())
	pv, err := client.CoreV1().PersistentVolumes().Get(ctx, pvclaim.Spec.VolumeName, metav1.GetOptions{})
	gomega.Expect(err).NotTo(gomega.HaveOccurred())
	return pv.Spec.VsphereVolume.VolumePath
}

// getUUIDFromProviderID strips ProviderPrefix - "vsphere://" from the providerID
// this gives the VM UUID which can be used to find Node VM from vCenter
func getUUIDFromProviderID(providerID string) string {
	return strings.TrimPrefix(providerID, providerPrefix)
}

// getVcpPersistentVolumeSpec function to create vsphere volume spec with
// given VMDK volume path, Reclaim Policy and labels.
func getVcpPersistentVolumeSpec(volumePath string, persistentVolumeReclaimPolicy v1.PersistentVolumeReclaimPolicy,
	labels map[string]string) *v1.PersistentVolume {
	annotations := make(map[string]string)
	annotations[pvAnnotationProvisionedBy] = vcpProvisionerName
	pv := fpv.MakePersistentVolume(fpv.PersistentVolumeConfig{
		NamePrefix: "vspherepv-",
		PVSource: v1.PersistentVolumeSource{
			VsphereVolume: &v1.VsphereVirtualDiskVolumeSource{
				VolumePath: volumePath,
				FSType:     "ext4",
			},
		},
		ReclaimPolicy: persistentVolumeReclaimPolicy,
		Capacity:      "2Gi",
		AccessModes: []v1.PersistentVolumeAccessMode{
			v1.ReadWriteOnce,
		},
		Labels: labels,
	})
	pv.Annotations = annotations
	return pv
}

// getVcpPersistentVolumeClaimSpec function to get vsphere persistent volume spec with given selector labels.
func getVcpPersistentVolumeClaimSpec(migrationEnabledByDefault bool, namespace string, size string,
	storageclass *storagev1.StorageClass, pvclaimlabels map[string]string, accessMode v1.PersistentVolumeAccessMode,
) *v1.PersistentVolumeClaim {
	pvc := getPersistentVolumeClaimSpecWithStorageClass(namespace, size, storageclass, pvclaimlabels, accessMode)
	annotations := make(map[string]string)
	if migrationEnabledByDefault {
		annotations[pvcAnnotationStorageProvisioner] = e2evSphereCSIDriverName
	} else {
		annotations[pvcAnnotationStorageProvisioner] = vcpProvisionerName
	}
	pvc.Annotations = annotations
	return pvc
}
