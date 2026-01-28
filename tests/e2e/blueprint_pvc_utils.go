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
	"strings"

	"github.com/onsi/ginkgo/v2"
	"github.com/onsi/gomega"
	"github.com/vmware/govmomi/find"
	"github.com/vmware/govmomi/object"
	v1 "k8s.io/api/core/v1"
	storagev1 "k8s.io/api/storage/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	clientset "k8s.io/client-go/kubernetes"
	"k8s.io/kubernetes/test/e2e/framework"
	fpv "k8s.io/kubernetes/test/e2e/framework/pv"
	"k8s.io/utils/pointer"
	cnsregistervolumev1alpha1 "sigs.k8s.io/vsphere-csi-driver/v3/pkg/apis/cnsoperator/cnsregistervolume/v1alpha1"
)

func createBluePrintPvcSpec(namespace string, scName string,
	vmName string, diskSize string, accessMode v1.PersistentVolumeAccessMode) *v1.PersistentVolumeClaim {
	if accessMode == "" {
		accessMode = v1.ReadWriteOnce
	}
	if diskSize == "" {
		diskSize = "1Gi"
	}

	claim := &v1.PersistentVolumeClaim{
		TypeMeta: metav1.TypeMeta{
			APIVersion: "v1",
			Kind:       "PersistentVolumeClaim",
		},
		ObjectMeta: metav1.ObjectMeta{
			GenerateName: "blueprint-pvc-",
			Namespace:    namespace,
		},
		Spec: v1.PersistentVolumeClaimSpec{
			StorageClassName: &scName,
			DataSourceRef: &v1.TypedObjectReference{
				APIGroup: pointer.String("vmoperator.vmware.com"),
				Kind:     "VirtualMachine",
				Name:     vmName,
			},
			AccessModes: []v1.PersistentVolumeAccessMode{
				accessMode,
			},
			Resources: v1.VolumeResourceRequirements{
				Requests: v1.ResourceList{
					v1.ResourceStorage: resource.MustParse(diskSize),
				},
			},
		},
	}

	return claim
}

func createBluePrintPVC(ctx context.Context, client clientset.Interface, namespace string,
	storageclass *storagev1.StorageClass, accessMode v1.PersistentVolumeAccessMode, vmName string,
	diskSize string) (*v1.PersistentVolumeClaim, error) {

	storageClassName := storageclass.Name

	// Generate the PVC spec
	pvcSpec := createBluePrintPvcSpec(namespace, storageClassName, vmName, diskSize, accessMode)

	storageQty := pvcSpec.Spec.Resources.Requests[v1.ResourceStorage]
	storageSize := (&storageQty).String()

	// Log all values going into the PVC spec
	ginkgo.By(fmt.Sprintf(
		"Creating Blueprint PVC with details:\n"+
			"  Namespace        : %s\n"+
			"  StorageClassName : %s\n"+
			"  DataSourceRef    : (apiGroup=%s, kind=%s, name=%s)\n"+
			"  DiskSize         : %s\n"+
			"  AccessMode       : %s\n",
		namespace,
		storageClassName,
		*pvcSpec.Spec.DataSourceRef.APIGroup,
		pvcSpec.Spec.DataSourceRef.Kind,
		pvcSpec.Spec.DataSourceRef.Name,
		storageSize,
		pvcSpec.Spec.AccessModes[0],
	))

	// Create the PVC
	pvc, err := fpv.CreatePVC(ctx, client, namespace, pvcSpec)
	gomega.Expect(err).NotTo(gomega.HaveOccurred(),
		fmt.Sprintf("Failed to create Blueprint PVC in namespace %s: %v", namespace, err))

	framework.Logf("Blueprint PVC created successfully: %q in namespace: %q", pvc.Name, namespace)
	return pvc, nil
}

func getCNSRegisterVolumeSpecForBluePrintPvc(namespace string, volumeID string, pvcName string,
	accessMode v1.PersistentVolumeAccessMode,
) *cnsregistervolumev1alpha1.CnsRegisterVolume {

	framework.Logf("Creating CNSRegisterVolume spec for blueprint pvc: %s, VolumeID: %s", pvcName, volumeID)
	if accessMode == "" {
		accessMode = v1.ReadWriteOnce
	}

	cnsRegisterVolume := &cnsregistervolumev1alpha1.CnsRegisterVolume{
		TypeMeta: metav1.TypeMeta{
			APIVersion: "cns.vmware.com/v1alpha1",
			Kind:       "CnsRegisterVolume",
		},
		ObjectMeta: metav1.ObjectMeta{
			// Use GenerateName to let Kubernetes append a random suffix
			GenerateName: "cnsregvol-",
			Namespace:    namespace,
		},
		Spec: cnsregistervolumev1alpha1.CnsRegisterVolumeSpec{
			VolumeID:   volumeID,
			AccessMode: accessMode,
			PvcName:    pvcName,
		},
	}

	return cnsRegisterVolume
}

func readDatastoreUrlForFcdCreation(ctx context.Context, datastoreURL string) *object.Datastore {
	var datacenters []string
	finder := find.NewFinder(e2eVSphere.Client.Client, false)
	cfg, err := getConfig()
	gomega.Expect(err).NotTo(gomega.HaveOccurred())
	dcList := strings.Split(cfg.Global.Datacenters, ",")
	for _, dc := range dcList {
		dcName := strings.TrimSpace(dc)
		if dcName != "" {
			datacenters = append(datacenters, dcName)
		}
	}
	for _, dc := range datacenters {
		defaultDatacenter, err := finder.Datacenter(ctx, dc)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		finder.SetDatacenter(defaultDatacenter)
		defaultDatastore, err = getDatastoreByURL(ctx, datastoreURL, defaultDatacenter)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
	}
	return defaultDatastore
}
