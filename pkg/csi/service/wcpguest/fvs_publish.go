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

// File-volume ControllerPublish/Unpublish handling for the new vSAN
// FileVolumeService (FVS) workflow on the supervisor.
//
// Background:
// With the legacy CNS file-volume architecture, before a guest pod can mount a
// file volume the supervisor must add the guest VM's IP to the file share ACL.
// CSI in the guest cluster drives that flow with a CnsFileAccessConfig CR (see
// controllerPublishForFileVolume / controllerUnpublishForFileVolume); the
// reconciler resolves the VM IP and updates the share ACL.
//
// The new vSAN FileVolumeService architecture provisions file volumes through
// FileVolume CRs on the supervisor and exposes them on a sticky raw IP that is
// reachable to every VM in the same VPC. No per-VM ACL is required, so the
// guest must not create / delete a CnsFileAccessConfig CR for these volumes.
// The export endpoint and path are returned to CSI on the supervisor PVC's
// "csi.vsphere.exportpath.nfs41" annotation (common.Nfsv4ExportPathAnnotationKey)
// at CreateVolume time.
//
// Identification: the controller must gate on IsVsanFileVolumeServiceEnabled and
// common.IsFVSPersistentVolumeClaim(supervisor PVC) before calling these helpers.
//
// Gating: IsVsanFileVolumeServiceEnabled is set in controller.Init when both the
// PVCSI internal FSS common.VsanFileVolumeServiceSupportFSS and the supervisor
// capability common.VsanFileVolumeService are enabled. If the FSS is on but the
// capability is not yet on, HandleLateEnablementOfCapability triggers a
// controller restart when the capability flips, so the flag does not need to be
// refreshed on each RPC.

package wcpguest

import (
	"context"
	"fmt"

	"github.com/container-storage-interface/spec/lib/go/csi"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	v1 "k8s.io/api/core/v1"

	csifault "sigs.k8s.io/vsphere-csi-driver/v3/pkg/common/fault"
	"sigs.k8s.io/vsphere-csi-driver/v3/pkg/csi/service/common"
	"sigs.k8s.io/vsphere-csi-driver/v3/pkg/csi/service/logger"
)

// IsVsanFileVolumeServiceEnabled is the package-level gate for the new vSAN
// FileVolumeService publish/unpublish workflow on the guest cluster. It is
// true only when BOTH the PVCSI internal FSS
// common.VsanFileVolumeServiceSupportFSS AND the corresponding supervisor
// capability common.VsanFileVolumeService are enabled. Initialized in
// controller.Init.
var IsVsanFileVolumeServiceEnabled bool

// ControllerPublishForFileVolumeServiceVolume handles a ControllerPublishVolume for a
// file volume that has been provisioned by the new vSAN FileVolumeService.
// There is no per-VM ACL to manage, so we simply read the NFSv4.1 export path
// from the supervisor PVC's csi.vsphere.exportpath.nfs41 annotation and
// return it to the node. CnsFileAccessConfig is intentionally not created.
//
// The caller must have verified FVS (e.g. common.IsFVSPersistentVolumeClaim on
// the supervisor PVC) before invoking this function.
func ControllerPublishForFileVolumeServiceVolume(ctx context.Context,
	req *csi.ControllerPublishVolumeRequest, svPVC *v1.PersistentVolumeClaim) (
	*csi.ControllerPublishVolumeResponse, string, error) {
	log := logger.GetLogger(ctx)

	exportPath := svPVC.Annotations[common.Nfsv4ExportPathAnnotationKey]
	if exportPath == "" {
		// Export-path annotation has not been written by the supervisor yet.
		// Surface as a retryable error so the external-attacher will retry.
		msg := fmt.Sprintf("FVS supervisor PVC %q in namespace %q is missing required %q annotation; "+
			"the supervisor has not yet populated the NFSv4.1 export path",
			svPVC.Name, svPVC.Namespace, common.Nfsv4ExportPathAnnotationKey)
		log.Error(msg)
		return nil, csifault.CSIInternalFault, status.Error(codes.Internal, msg)
	}

	publishInfo := map[string]string{
		common.AttributeDiskType: common.DiskTypeFileVolume,
		common.Nfsv4AccessPoint:  exportPath,
	}
	log.Infof("ControllerPublishVolume: FVS file volume %q published to node %q with access point %q; "+
		"skipping CnsFileAccessConfig",
		req.VolumeId, req.NodeId, exportPath)
	return &csi.ControllerPublishVolumeResponse{PublishContext: publishInfo}, "", nil
}

// ControllerUnpublishForFileVolumeServiceVolume handles a ControllerUnpublishVolume for
// an FVS-provisioned file volume. There is no per-VM ACL to revoke, so we
// short-circuit with a success response without touching CnsFileAccessConfig.
// The caller must have verified FVS (e.g. common.IsFVSPersistentVolumeClaim on
// the supervisor PVC) before invoking this function.
func ControllerUnpublishForFileVolumeServiceVolume(ctx context.Context,
	req *csi.ControllerUnpublishVolumeRequest) (
	*csi.ControllerUnpublishVolumeResponse, string, error) {
	log := logger.GetLogger(ctx)
	log.Infof("ControllerUnpublishVolume: FVS file volume %q on node %q; "+
		"no CnsFileAccessConfig to delete, returning success",
		req.VolumeId, req.NodeId)
	return &csi.ControllerUnpublishVolumeResponse{}, "", nil
}
