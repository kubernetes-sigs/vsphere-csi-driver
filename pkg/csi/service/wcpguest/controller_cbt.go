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
	"io"
	"time"

	"github.com/container-storage-interface/spec/lib/go/csi"
	snapshotmetadataapi "github.com/kubernetes-csi/external-snapshot-metadata/pkg/api"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"sigs.k8s.io/vsphere-csi-driver/v3/pkg/common/prometheus"
	"sigs.k8s.io/vsphere-csi-driver/v3/pkg/csi/service/common"
	"sigs.k8s.io/vsphere-csi-driver/v3/pkg/csi/service/common/commonco"
	"sigs.k8s.io/vsphere-csi-driver/v3/pkg/csi/service/logger"
)

// GetMetadataAllocated forwards the request to the Snapshot Metadata Service in the Supervisor
// cluster. The Supervisor sidecar (external-snapshot-metadata) speaks the
// `snapshotmetadata.SnapshotMetadata` proto, which identifies snapshots by
// (namespace, snapshot_name) rather than by the CSI snapshot_id handle used by the CSI spec.
// The guest pvCSI therefore:
//  1. Resolves the incoming CSI snapshot_id to a Supervisor VolumeSnapshot by looking up
//     the Supervisor VolumeSnapshotContent whose Status.SnapshotHandle matches.
//  2. Builds an external-proto request carrying the security token and the resolved
//     (namespace, snapshot_name).
//  3. Streams responses back, translating them into CSI GetMetadataAllocatedResponse messages.
func (c *controller) GetMetadataAllocated(
	req *csi.GetMetadataAllocatedRequest,
	server csi.SnapshotMetadata_GetMetadataAllocatedServer) error {

	ctx := logger.NewContextWithLogger(server.Context())
	log := logger.GetLogger(ctx)
	log.Infof("GetMetadataAllocated: called with args %+v", req)

	// Gate on the pvCSI-side CBT FSS. In production this FSS is wired via
	// common.WCPFeatureStateAssociatedWithPVCSI to the Supervisor's
	// supports_CSI_Backup_API WCP capability, so the effective gate in VKS is
	// the Supervisor's capability — not a guest-local config.
	if !commonco.ContainerOrchestratorUtility.IsFSSEnabled(ctx, common.CSI_Backup_API_FSS) {
		return logger.LogNewErrorCode(log, codes.Unimplemented, "GetMetadataAllocated")
	}

	start := time.Now()
	volumeType := prometheus.PrometheusBlockVolumeType

	getMetadataAllocatedInternal := func() error {
		if req == nil {
			return logger.LogNewErrorCode(log, codes.InvalidArgument, "GetMetadataAllocated request is nil")
		}
		if req.SnapshotId == "" {
			return logger.LogNewErrorCode(log, codes.InvalidArgument, "snapshot ID is required")
		}

		svNamespace, svName, err := c.findSupervisorSnapshotByHandle(ctx, req.SnapshotId)
		if err != nil {
			return logger.LogNewErrorCodef(log, codes.NotFound,
				"failed to resolve CSI snapshot_id %q to a supervisor VolumeSnapshot: %v",
				req.SnapshotId, err)
		}

		client, conn, token, err := c.getSnapshotMetadataClient(ctx)
		if err != nil {
			return logger.LogNewErrorCodef(log, codes.Internal,
				"failed to get Snapshot Metadata client: %v", err)
		}
		defer conn.Close()

		svReq := &snapshotmetadataapi.GetMetadataAllocatedRequest{
			SecurityToken:  token,
			Namespace:      svNamespace,
			SnapshotName:   svName,
			StartingOffset: req.StartingOffset,
			MaxResults:     req.MaxResults,
		}
		log.Infof("Forwarding GetMetadataAllocated to Supervisor for %s/%s (CSI handle=%s)",
			svNamespace, svName, req.SnapshotId)
		stream, err := client.GetMetadataAllocated(ctx, svReq)
		if err != nil {
			return logger.LogNewErrorCodef(log, status.Code(err),
				"failed to get allocated metadata stream from supervisor cluster: %v", err)
		}

		for {
			svResp, err := stream.Recv()
			if err != nil {
				if err == io.EOF || status.Code(err) == codes.OutOfRange {
					break
				}
				if status.Code(err) == codes.Canceled || status.Code(err) == codes.DeadlineExceeded {
					log.Infof("metadata stream stopped: %v", err)
					return err
				}
				return logger.LogNewErrorCodef(log, status.Code(err),
					"error receiving from allocated metadata stream: %v", err)
			}
			csiResp := &csi.GetMetadataAllocatedResponse{
				BlockMetadataType:   csi.BlockMetadataType(svResp.GetBlockMetadataType()),
				VolumeCapacityBytes: svResp.GetVolumeCapacityBytes(),
				BlockMetadata:       convertBlockMetadata(svResp.GetBlockMetadata()),
			}
			if err := server.Send(csiResp); err != nil {
				return logger.LogNewErrorCodef(log, status.Code(err),
					"failed to send allocated metadata to client: %v", err)
			}
		}

		return nil
	}

	err := getMetadataAllocatedInternal()
	if err != nil {
		prometheus.CsiControlOpsHistVec.WithLabelValues(volumeType, "GetMetadataAllocated",
			prometheus.PrometheusFailStatus, "NotComputed").Observe(time.Since(start).Seconds())
	} else {
		prometheus.CsiControlOpsHistVec.WithLabelValues(volumeType, "GetMetadataAllocated",
			prometheus.PrometheusPassStatus, "").Observe(time.Since(start).Seconds())
	}
	return err
}

// GetMetadataDelta forwards the request to the Snapshot Metadata Service in the Supervisor
// cluster. The external-snapshot-metadata proto carries the base snapshot by an opaque
// identifier (base_snapshot_id) and the target snapshot by (namespace, target_snapshot_name).
//
// vSphere semantics for base_snapshot_id: it is the vSphere CBT change-id (the
// `csi.vsphere.volume/change-id` annotation that pvCSI mirrors onto the guest VolumeSnapshot
// at CreateSnapshot time). Backup software is expected to read that annotation and persist
// the change-id in its own catalog, then pass it back in BaseSnapshotId. We deliberately do
// NOT look the change-id back up by snapshot_id here because vSphere best practice is to keep
// snapshots short-lived, so by the time a delta backup runs the base snapshot may have been deleted.
//
// For the target snapshot we still translate the CSI handle to the Supervisor VolumeSnapshot
// (namespace, name) using a namespace-scoped Get on the Supervisor VolumeSnapshot; the
// Supervisor sidecar resolves that to its CSI handle.
func (c *controller) GetMetadataDelta(
	req *csi.GetMetadataDeltaRequest,
	server csi.SnapshotMetadata_GetMetadataDeltaServer) error {

	ctx := logger.NewContextWithLogger(server.Context())
	log := logger.GetLogger(ctx)
	log.Infof("GetMetadataDelta: called with args %+v", req)

	// Same gating as GetMetadataAllocated: the pvCSI FSS resolves to the
	// Supervisor's supports_CSI_Backup_API capability via WCPFeatureStateAssociatedWithPVCSI.
	if !commonco.ContainerOrchestratorUtility.IsFSSEnabled(ctx, common.CSI_Backup_API_FSS) {
		return logger.LogNewErrorCode(log, codes.Unimplemented, "GetMetadataDelta")
	}

	start := time.Now()
	volumeType := prometheus.PrometheusBlockVolumeType

	getMetadataDeltaInternal := func() error {
		if req == nil {
			return logger.LogNewErrorCode(log, codes.InvalidArgument, "GetMetadataDelta request is nil")
		}
		if req.BaseSnapshotId == "" {
			return logger.LogNewErrorCode(log, codes.InvalidArgument,
				"base snapshot ID (vSphere change-id) is required")
		}
		if req.TargetSnapshotId == "" {
			return logger.LogNewErrorCode(log, codes.InvalidArgument, "target snapshot ID is required")
		}

		svNamespace, svTargetName, err := c.findSupervisorSnapshotByHandle(ctx, req.TargetSnapshotId)
		if err != nil {
			return logger.LogNewErrorCodef(log, codes.NotFound,
				"failed to resolve CSI target snapshot_id %q to a supervisor VolumeSnapshot: %v",
				req.TargetSnapshotId, err)
		}

		client, conn, token, err := c.getSnapshotMetadataClient(ctx)
		if err != nil {
			return logger.LogNewErrorCodef(log, codes.Internal,
				"failed to get Snapshot Metadata client: %v", err)
		}
		defer conn.Close()

		// BaseSnapshotId is forwarded verbatim: it is already the vSphere change-id that the
		// Supervisor CSI driver hands directly to VSLM.QueryChangedDiskAreas.
		svReq := &snapshotmetadataapi.GetMetadataDeltaRequest{
			SecurityToken:      token,
			Namespace:          svNamespace,
			BaseSnapshotId:     req.BaseSnapshotId,
			TargetSnapshotName: svTargetName,
			StartingOffset:     req.StartingOffset,
			MaxResults:         req.MaxResults,
		}
		log.Infof("Forwarding GetMetadataDelta to Supervisor: base=<change-id> target=%s/%s",
			svNamespace, svTargetName)
		stream, err := client.GetMetadataDelta(ctx, svReq)
		if err != nil {
			return logger.LogNewErrorCodef(log, status.Code(err),
				"failed to get delta metadata stream from supervisor cluster: %v", err)
		}

		for {
			svResp, err := stream.Recv()
			if err != nil {
				if err == io.EOF || status.Code(err) == codes.OutOfRange {
					break
				}
				if status.Code(err) == codes.Canceled || status.Code(err) == codes.DeadlineExceeded {
					log.Infof("metadata stream stopped: %v", err)
					return err
				}
				return logger.LogNewErrorCodef(log, status.Code(err),
					"error receiving from delta metadata stream: %v", err)
			}
			csiResp := &csi.GetMetadataDeltaResponse{
				BlockMetadataType:   csi.BlockMetadataType(svResp.GetBlockMetadataType()),
				VolumeCapacityBytes: svResp.GetVolumeCapacityBytes(),
				BlockMetadata:       convertBlockMetadata(svResp.GetBlockMetadata()),
			}
			if err := server.Send(csiResp); err != nil {
				return logger.LogNewErrorCodef(log, status.Code(err),
					"failed to send delta metadata to client: %v", err)
			}
		}

		return nil
	}

	err := getMetadataDeltaInternal()
	if err != nil {
		prometheus.CsiControlOpsHistVec.WithLabelValues(volumeType, "GetMetadataDelta",
			prometheus.PrometheusFailStatus, "NotComputed").Observe(time.Since(start).Seconds())
	} else {
		prometheus.CsiControlOpsHistVec.WithLabelValues(volumeType, "GetMetadataDelta",
			prometheus.PrometheusPassStatus, "").Observe(time.Since(start).Seconds())
	}
	return err
}

// convertBlockMetadata maps external-snapshot-metadata BlockMetadata values to their CSI
// equivalents. The two protos have the same field names and semantics, so the conversion is a
// straight copy.
func convertBlockMetadata(in []*snapshotmetadataapi.BlockMetadata) []*csi.BlockMetadata {
	if len(in) == 0 {
		return nil
	}
	out := make([]*csi.BlockMetadata, 0, len(in))
	for _, b := range in {
		if b == nil {
			continue
		}
		out = append(out, &csi.BlockMetadata{
			ByteOffset: b.GetByteOffset(),
			SizeBytes:  b.GetSizeBytes(),
		})
	}
	return out
}
