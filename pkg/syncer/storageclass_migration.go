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

package syncer

import (
	"context"
	"fmt"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes"

	"sigs.k8s.io/vsphere-csi-driver/v3/pkg/csi/service/logger"
	k8s "sigs.k8s.io/vsphere-csi-driver/v3/pkg/kubernetes"
)

// vSphereCSIProvisioner is the provisioner name of the vSphere CSI driver's StorageClasses.
const vSphereCSIProvisioner = "csi.vsphere.vmware.com"

// RunStorageClassExpansionMigration runs MigrateStorageClassExpansion against the cluster the
// syncer is running in.
func RunStorageClassExpansionMigration(ctx context.Context) error {
	client, err := k8s.NewClient(ctx)
	if err != nil {
		return fmt.Errorf("failed to create a Kubernetes client: %w", err)
	}
	return MigrateStorageClassExpansion(ctx, client)
}

// MigrateStorageClassExpansion sets allowVolumeExpansion=true on every StorageClass of the
// vSphere CSI driver that does not have it. StorageClasses that already allow expansion, and
// those of other provisioners, are left alone, so running it again is harmless.
func MigrateStorageClassExpansion(ctx context.Context, client kubernetes.Interface) error {
	log := logger.GetLogger(ctx)
	scs, err := client.StorageV1().StorageClasses().List(ctx, metav1.ListOptions{})
	if err != nil {
		return fmt.Errorf("failed to list StorageClasses: %w", err)
	}
	patch := []byte(`{"allowVolumeExpansion":true}`)
	for _, sc := range scs.Items {
		if sc.Provisioner != vSphereCSIProvisioner ||
			(sc.AllowVolumeExpansion != nil && *sc.AllowVolumeExpansion) {
			continue
		}
		if _, err := client.StorageV1().StorageClasses().Patch(ctx, sc.Name, types.MergePatchType, patch,
			metav1.PatchOptions{}); err != nil {
			return fmt.Errorf("failed to patch StorageClass %s: %w", sc.Name, err)
		}
		log.Infof("Set allowVolumeExpansion=true on StorageClass %s", sc.Name)
	}
	return nil
}
