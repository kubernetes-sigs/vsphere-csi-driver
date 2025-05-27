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
	cnstypes "github.com/vmware/govmomi/cns/types"
	"github.com/vmware/govmomi/vim25/types"
	vsanfstypes "github.com/vmware/govmomi/vsan/vsanfs/types"
)

// Create file share spec which is used to create fileshare
func getFileShareCreateSpec(datastore types.ManagedObjectReference) *cnstypes.CnsVolumeCreateSpec {
	netPermissions := vsanfstypes.VsanFileShareNetPermission{
		Ips:         "*",
		Permissions: vsanfstypes.VsanFileShareAccessTypeREAD_WRITE,
		AllowRoot:   true,
	}
	containerCluster := &cnstypes.CnsContainerCluster{
		ClusterType:   string(cnstypes.CnsClusterTypeKubernetes),
		ClusterId:     e2eVSphere.Config.Global.ClusterID,
		VSphereUser:   e2eVSphere.Config.Global.User,
		ClusterFlavor: string(cnstypes.CnsClusterFlavorVanilla),
	}
	var containerClusterArray []cnstypes.CnsContainerCluster
	containerClusterArray = append(containerClusterArray, *containerCluster)
	createSpec := &cnstypes.CnsVolumeCreateSpec{
		Name:       "testFileSharex",
		VolumeType: "FILE",
		Datastores: []types.ManagedObjectReference{datastore},
		BackingObjectDetails: &cnstypes.CnsVsanFileShareBackingDetails{
			CnsFileBackingDetails: cnstypes.CnsFileBackingDetails{
				CnsBackingObjectDetails: cnstypes.CnsBackingObjectDetails{
					CapacityInMb: fileSizeInMb,
				},
			},
		},
		Metadata: cnstypes.CnsVolumeMetadata{
			ContainerCluster:      *containerCluster,
			ContainerClusterArray: containerClusterArray,
		},
		CreateSpec: &cnstypes.CnsVSANFileCreateSpec{
			SoftQuotaInMb: fileSizeInMb,
			Permission:    []vsanfstypes.VsanFileShareNetPermission{netPermissions},
		},
	}
	return createSpec
}
