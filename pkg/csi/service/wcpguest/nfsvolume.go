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
	"google.golang.org/grpc/status"

	csifault "sigs.k8s.io/vsphere-csi-driver/v3/pkg/common/fault"
	"sigs.k8s.io/vsphere-csi-driver/v3/pkg/csi/service/common"
	"sigs.k8s.io/vsphere-csi-driver/v3/pkg/csi/service/common/commonco"
	"sigs.k8s.io/vsphere-csi-driver/v3/pkg/csi/service/logger"
	"sigs.k8s.io/vsphere-csi-driver/v3/pkg/csi/service/wcpguest/nfsdriver"
)

// IsGuestNFSVolumeID reports whether id was minted by the vendored nfsdriver package for a guest
// NFS volume.
func IsGuestNFSVolumeID(id string) bool {
	return nfsdriver.IsGuestVolumeID(id)
}

// guestNFSContentSourceID returns the ID of the guest NFS snapshot or volume that src refers to,
// or "" when src is nil or refers to anything else.
func guestNFSContentSourceID(src *csi.VolumeContentSource) string {
	if id := src.GetSnapshot().GetSnapshotId(); IsGuestNFSVolumeID(id) {
		return id
	}
	if id := src.GetVolume().GetVolumeId(); IsGuestNFSVolumeID(id) {
		return id
	}
	return ""
}

// isGuestNFSEnabled reports whether the guest-nfs-volume pvCSI FSS is enabled.
//
// The FSS gates what creates or removes guest NFS state on the provisioning side: CreateVolume,
// DeleteVolume and CreateSnapshot. It deliberately does not gate anything that serves volumes
// that already exist — ControllerPublish/Unpublish, ControllerExpand, DeleteSnapshot and the
// node's mount, unmount and stats. Gating those would turn an FSS that is reset (for example by
// a pvCSI package reconcile re-applying the default "false" during a VKS upgrade) into an
// outage: pods moved to another node could not attach or mount, kubelet's re-mount of running
// pods after a restart would be rejected, and failing a detach or unmount would leave pods
// stuck Terminating and block node drains.
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

// nfsFault maps an error from the vendored driver to the CSI fault type reported in metrics.
func nfsFault(err error) string {
	switch status.Code(err) {
	case codes.InvalidArgument:
		return csifault.CSIInvalidArgumentFault
	case codes.NotFound:
		return csifault.CSINotFoundFault
	default:
		return csifault.CSIInternalFault
	}
}

// failIfGuestNFSDisabled returns an error when the guest-nfs-volume FSS is off. The error is
// a retriable FailedPrecondition so the caller keeps retrying until the feature is enabled
// again, rather than falling through to the Supervisor path, which knows nothing about these
// volumes.
func failIfGuestNFSDisabled(ctx context.Context, op, id string) error {
	if isGuestNFSEnabled(ctx) {
		return nil
	}
	log := logger.GetLogger(ctx)
	return logger.LogNewErrorCodef(log, codes.FailedPrecondition,
		"cannot %s guest NFS volume or snapshot %q: the %q feature state is disabled", op, id, common.GuestNFSVolume)
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

	// This path is selected by one parameter, so validate the whole parameter set up front rather
	// than letting a stray "server" key on an ordinary StorageClass provision NFS on any host.
	if err := nfsdriver.ValidateCreateVolumeParameters(req.GetParameters()); err != nil {
		return nil, csifault.CSIInvalidArgumentFault, logger.LogNewErrorCodef(log, codes.InvalidArgument,
			"invalid StorageClass parameters for guest NFS volume %q: %v", req.GetName(), err)
	}

	resp, err := nfsdriver.GetOrCreateDriver().ControllerServer().CreateVolume(ctx, req)
	if err != nil {
		log.Errorf("createGuestNFSVolume: CreateVolume failed for %q: %v", req.GetName(), err)
		return nil, nfsFault(err), err
	}
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
	if err := failIfGuestNFSDisabled(ctx, "delete", req.GetVolumeId()); err != nil {
		return nil, csifault.CSIInternalFault, err
	}

	resp, err := nfsdriver.GetOrCreateDriver().ControllerServer().DeleteVolume(ctx, req)
	if err != nil {
		log.Errorf("deleteGuestNFSVolume: DeleteVolume failed for %q: %v", req.GetVolumeId(), err)
		return nil, nfsFault(err), err
	}
	return resp, "", nil
}

// publishGuestNFSVolume handles ControllerPublishVolume. An NFS export is mounted directly by
// the node, which reads the mount source from the volume context rather than a publish
// context, so there is nothing to attach: the call only has to succeed so the attach flow can
// complete. See isGuestNFSEnabled for why it is not gated on the feature state.
func publishGuestNFSVolume(ctx context.Context, req *csi.ControllerPublishVolumeRequest) (
	*csi.ControllerPublishVolumeResponse, string, error) {
	logger.GetLogger(ctx).Infof("ControllerPublishVolume: skipping publish for guest NFS volume %q",
		req.GetVolumeId())
	return &csi.ControllerPublishVolumeResponse{}, "", nil
}

// unpublishGuestNFSVolume handles ControllerUnpublishVolume: nothing was attached, so there is
// nothing to detach. See isGuestNFSEnabled for why it is not gated on the feature state.
func unpublishGuestNFSVolume(ctx context.Context, req *csi.ControllerUnpublishVolumeRequest) (
	*csi.ControllerUnpublishVolumeResponse, string, error) {
	logger.GetLogger(ctx).Infof("ControllerUnpublishVolume: skipping unpublish for guest NFS volume %q",
		req.GetVolumeId())
	return &csi.ControllerUnpublishVolumeResponse{}, "", nil
}

// expandGuestNFSVolume handles ControllerExpandVolume. An NFS export has no per-volume size
// limit to change, so the vendored driver just acknowledges the requested size, and the node has
// nothing to resize.
func expandGuestNFSVolume(ctx context.Context, req *csi.ControllerExpandVolumeRequest) (
	*csi.ControllerExpandVolumeResponse, string, error) {
	resp, err := nfsdriver.GetOrCreateDriver().ControllerServer().ControllerExpandVolume(ctx, req)
	if err != nil {
		return nil, nfsFault(err), err
	}
	resp.NodeExpansionRequired = false
	return resp, "", nil
}

// validateGuestNFSVolumeCapabilities handles ValidateVolumeCapabilities.
func validateGuestNFSVolumeCapabilities(ctx context.Context, req *csi.ValidateVolumeCapabilitiesRequest) (
	*csi.ValidateVolumeCapabilitiesResponse, error) {
	return nfsdriver.GetOrCreateDriver().ControllerServer().ValidateVolumeCapabilities(ctx, req)
}

// externalMetadataPrefix prefixes the parameters that the CSI sidecars add to a request with
// --extra-create-metadata (the snapshotter in pvcsi.yaml runs with it). The vendored driver
// rejects parameters it does not know, so they are removed before a snapshot request is passed on.
const externalMetadataPrefix = "csi.storage.k8s.io/"

// withoutExternalMetadata returns a copy of params without the keys the CSI sidecars add.
func withoutExternalMetadata(params map[string]string) map[string]string {
	out := make(map[string]string, len(params))
	for k, v := range params {
		if !strings.HasPrefix(strings.ToLower(k), externalMetadataPrefix) {
			out[k] = v
		}
	}
	return out
}

// createGuestNFSSnapshot handles CreateSnapshot for a source volume minted by createGuestNFSVolume.
// Unlike the block volume path it does not depend on the Supervisor's block volume snapshot
// feature, because the snapshot is an archive that the vendored driver writes on the NFS server.
func createGuestNFSSnapshot(ctx context.Context, req *csi.CreateSnapshotRequest) (
	*csi.CreateSnapshotResponse, error) {
	log := logger.GetLogger(ctx)
	if err := failIfGuestNFSDisabled(ctx, "snapshot", req.GetSourceVolumeId()); err != nil {
		return nil, err
	}
	resp, err := nfsdriver.GetOrCreateDriver().ControllerServer().CreateSnapshot(ctx,
		&csi.CreateSnapshotRequest{
			SourceVolumeId: req.GetSourceVolumeId(),
			Name:           req.GetName(),
			Secrets:        req.GetSecrets(),
			Parameters:     withoutExternalMetadata(req.GetParameters()),
		})
	if err != nil {
		log.Errorf("createGuestNFSSnapshot: CreateSnapshot failed for %q: %v", req.GetName(), err)
		return nil, err
	}
	return resp, nil
}

// deleteGuestNFSSnapshot handles DeleteSnapshot for a snapshot minted by createGuestNFSSnapshot.
// See isGuestNFSEnabled for why it is not gated on the feature state.
func deleteGuestNFSSnapshot(ctx context.Context, req *csi.DeleteSnapshotRequest) (
	*csi.DeleteSnapshotResponse, error) {
	return nfsdriver.GetOrCreateDriver().ControllerServer().DeleteSnapshot(ctx, req)
}

// listGuestNFSSnapshots handles ListSnapshots for a request that names a guest NFS snapshot or
// source volume. The vendored driver keeps no catalogue of snapshots to list, and these never
// exist on the Supervisor, so the answer is an empty list, which is what the CSI spec asks for
// an ID that is not found.
func listGuestNFSSnapshots(_ context.Context, _ *csi.ListSnapshotsRequest) (
	*csi.ListSnapshotsResponse, error) {
	return &csi.ListSnapshotsResponse{}, nil
}
