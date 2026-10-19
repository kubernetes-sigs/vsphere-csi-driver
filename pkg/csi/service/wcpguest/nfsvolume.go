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

package wcpguest

import (
	"context"
	"strings"

	"github.com/container-storage-interface/spec/lib/go/csi"
	"google.golang.org/grpc/codes"

	csifault "sigs.k8s.io/vsphere-csi-driver/v3/pkg/common/fault"
	"sigs.k8s.io/vsphere-csi-driver/v3/pkg/csi/service/common"
	"sigs.k8s.io/vsphere-csi-driver/v3/pkg/csi/service/common/commonco"
	"sigs.k8s.io/vsphere-csi-driver/v3/pkg/csi/service/logger"
	"sigs.k8s.io/vsphere-csi-driver/v3/pkg/csi/service/wcpguest/nfsdriver"
)

// IsGuestNFSVolumeID reports whether id was minted by createGuestNFSVolume.
func IsGuestNFSVolumeID(id string) bool {
	return nfsdriver.IsGuestVolumeID(id)
}

// isGuestNFSEnabled reports whether the guest-nfs-volume pvCSI FSS is enabled. Every guest
// NFS volume operation is gated on it.
func isGuestNFSEnabled(ctx context.Context) bool {
	return commonco.ContainerOrchestratorUtility != nil &&
		commonco.ContainerOrchestratorUtility.IsFSSEnabled(ctx, common.GuestNFSVolume)
}

// hasNFSServerParam reports whether params selects the guest-local NFS driver backend, i.e.
// whether the StorageClass carries a non-empty "server" parameter — the same parameter key
// the vendored nfsdriver package itself looks for (case-insensitive, matching upstream).
func hasNFSServerParam(params map[string]string) bool {
	for k, v := range params {
		if strings.EqualFold(k, "server") && v != "" {
			return true
		}
	}
	return false
}

// createGuestNFSVolume handles CreateVolume entirely locally within the guest cluster via the
// vendored nfsdriver package, when the StorageClass carries a "server" parameter. It never
// creates a Supervisor PVC.
func (c *controller) createGuestNFSVolume(ctx context.Context, req *csi.CreateVolumeRequest) (
	*csi.CreateVolumeResponse, string, error) {
	log := logger.GetLogger(ctx)

	// Without this check a "server" parameter would fall through to the Supervisor path and be
	// rejected there as an unknown parameter, which is a confusing error for the user.
	if !isGuestNFSEnabled(ctx) {
		return nil, csifault.CSIInvalidArgumentFault, logger.LogNewErrorCodef(log, codes.InvalidArgument,
			"StorageClass parameter %q selects guest NFS volumes, but the %q feature state is disabled",
			"server", common.GuestNFSVolume)
	}

	resp, err := nfsdriver.GetOrCreateDriver().ControllerServer().CreateVolume(ctx, req)
	if err != nil {
		log.Errorf("createGuestNFSVolume: CreateVolume failed for %q: %v", req.GetName(), err)
		return nil, csifault.CSIInternalFault, err
	}
	resp.Volume.VolumeId = nfsdriver.VolumeIDPrefix + resp.Volume.VolumeId
	log.Infof("createGuestNFSVolume: volume %q ready", resp.Volume.VolumeId)
	return resp, "", nil
}

// deleteGuestNFSVolume handles DeleteVolume for a VolumeId minted by createGuestNFSVolume.
func (c *controller) deleteGuestNFSVolume(ctx context.Context, req *csi.DeleteVolumeRequest) (
	*csi.DeleteVolumeResponse, string, error) {
	log := logger.GetLogger(ctx)

	// Fail rather than fall through to the Supervisor path, which would not find a PVC and
	// report success, orphaning the folder on the NFS server. The external-provisioner retries
	// until the feature state is enabled again.
	if !isGuestNFSEnabled(ctx) {
		return nil, csifault.CSIInternalFault, logger.LogNewErrorCodef(log, codes.FailedPrecondition,
			"cannot delete guest NFS volume %q: the %q feature state is disabled",
			req.VolumeId, common.GuestNFSVolume)
	}

	req.VolumeId = strings.TrimPrefix(req.VolumeId, nfsdriver.VolumeIDPrefix)
	resp, err := nfsdriver.GetOrCreateDriver().ControllerServer().DeleteVolume(ctx, req)
	if err != nil {
		log.Errorf("deleteGuestNFSVolume: DeleteVolume failed for %q: %v", req.VolumeId, err)
		return nil, csifault.CSIInternalFault, err
	}
	return resp, "", nil
}
