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

	csifault "sigs.k8s.io/vsphere-csi-driver/v3/pkg/common/fault"
	"sigs.k8s.io/vsphere-csi-driver/v3/pkg/csi/service/logger"
	"sigs.k8s.io/vsphere-csi-driver/v3/pkg/csi/service/wcpguest/nfsdriver"
)

// IsGuestNFSVolumeID reports whether id was minted by createGuestNFSVolume.
func IsGuestNFSVolumeID(id string) bool {
	return nfsdriver.IsGuestVolumeID(id)
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

	req.VolumeId = strings.TrimPrefix(req.VolumeId, nfsdriver.VolumeIDPrefix)
	resp, err := nfsdriver.GetOrCreateDriver().ControllerServer().DeleteVolume(ctx, req)
	if err != nil {
		log.Errorf("deleteGuestNFSVolume: DeleteVolume failed for %q: %v", req.VolumeId, err)
		return nil, csifault.CSIInternalFault, err
	}
	return resp, "", nil
}

// expandGuestNFSVolume handles ControllerExpandVolume for a VolumeId minted by
// createGuestNFSVolume. The vendored nfsdriver has no real quota to grow, so this just echoes
// back the requested size (see nfsdriver.ControllerServer.ControllerExpandVolume).
func expandGuestNFSVolume(ctx context.Context, req *csi.ControllerExpandVolumeRequest) (
	*csi.ControllerExpandVolumeResponse, string, error) {
	log := logger.GetLogger(ctx)

	req.VolumeId = strings.TrimPrefix(req.VolumeId, nfsdriver.VolumeIDPrefix)
	resp, err := nfsdriver.GetOrCreateDriver().ControllerServer().ControllerExpandVolume(ctx, req)
	if err != nil {
		log.Errorf("expandGuestNFSVolume: ControllerExpandVolume failed for %q: %v", req.VolumeId, err)
		return nil, csifault.CSIInternalFault, err
	}
	return resp, "", nil
}
