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

package wcp

import (
	"context"
	"fmt"
	"time"

	"github.com/container-storage-interface/spec/lib/go/csi"
	cnstypes "github.com/vmware/govmomi/cns/types"
	"google.golang.org/grpc/codes"

	"sigs.k8s.io/vsphere-csi-driver/v3/pkg/common/prometheus"
	"sigs.k8s.io/vsphere-csi-driver/v3/pkg/csi/service/common"
	"sigs.k8s.io/vsphere-csi-driver/v3/pkg/csi/service/common/commonco"
	"sigs.k8s.io/vsphere-csi-driver/v3/pkg/csi/service/logger"
)

const (
	// defaultMaxResults defines the default maximum number of blocks to return in a single gRPC stream
	// if the CSI caller (e.g. external-snapshot-metadata) passes 0 (no limit).
	defaultMaxResults = 10000
)

// GetMetadataAllocated returns the allocated blocks for a snapshot using FCD VSLM APIs.
// This implementation uses pure Go through govmomi's VSLM package (no CGO/VDDK required).

func (c *controller) GetMetadataAllocated(req *csi.GetMetadataAllocatedRequest,
	server csi.SnapshotMetadata_GetMetadataAllocatedServer) error {

	ctx := server.Context()
	ctx = logger.NewContextWithLogger(ctx)
	log := logger.GetLogger(ctx)
	log.Infof("GetMetadataAllocated: called with args %+v", req)

	// Check if CBT feature is enabled
	isCBTEnabled := commonco.ContainerOrchestratorUtility.IsFSSEnabled(ctx, common.CBT)
	if !isCBTEnabled {
		return logger.LogNewErrorCode(log, codes.Unimplemented, "GetMetadataAllocated")
	}

	start := time.Now()
	volumeType := prometheus.PrometheusBlockVolumeType

	getMetadataAllocatedInternal := func() (*csi.GetMetadataAllocatedResponse, error) {
		// Validate request
		if err := validateGetMetadataAllocatedRequest(ctx, req); err != nil {
			return nil, logger.LogNewErrorCodef(log, codes.InvalidArgument,
				"validation for GetMetadataAllocated Request: %+v has failed. Error: %v", req, err)
		}

		snapshotID := req.GetSnapshotId()
		startingOffset := req.GetStartingOffset()
		maxResults := req.GetMaxResults()
		if maxResults == 0 {
			// CSI spec: If zero, the Plugin MUST choose a reasonable maximum number of results.
			maxResults = defaultMaxResults
		}

		// Parse snapshot ID to get volume ID and snapshot handle
		// CSI snapshot ID format: "volumeID+snapshotID"
		volumeID, cnsSnapshotID, err := common.ParseCSISnapshotID(snapshotID)
		if err != nil {
			return nil, logger.LogNewErrorCodef(log, codes.InvalidArgument,
				"failed to parse snapshot ID %s: %v", snapshotID, err)
		}

		log.Infof("GetMetadataAllocated: querying allocated blocks for volume %s, snapshot %s, offset %d, max %d",
			volumeID, cnsSnapshotID, startingOffset, maxResults)

		// Query volume details to get volume information
		volumeIds := []cnstypes.CnsVolumeId{{Id: volumeID}}
		cnsQueryFilter := cnstypes.CnsQueryFilter{
			VolumeIds: volumeIds,
		}

		queryResult, err := c.manager.VolumeManager.QueryVolume(ctx, cnsQueryFilter)
		if err != nil {
			return nil, logger.LogNewErrorCodef(log, codes.Internal,
				"failed to query volume %s: %v", volumeID, err)
		}

		if len(queryResult.Volumes) == 0 {
			return nil, logger.LogNewErrorCodef(log, codes.NotFound,
				"volume %s not found", volumeID)
		}

		// Verify volume type
		cnsVolumeType := queryResult.Volumes[0].VolumeType
		if cnsVolumeType != common.BlockVolumeType {
			return nil, logger.LogNewErrorCodef(log, codes.InvalidArgument,
				"GetMetadataAllocated is only supported for block volumes, got volume type: %s",
				cnsVolumeType)
		}

		blockBacking, ok := queryResult.Volumes[0].BackingObjectDetails.(*cnstypes.CnsBlockBackingDetails)
		if !ok {
			return nil, logger.LogNewErrorCodef(log, codes.Internal,
				"volume %s does not have block backing details", volumeID)
		}
		volumeCapacityBytes := blockBacking.CapacityInMb * common.MbInBytes

		allocatedAreas, nextOffset, err := c.manager.VolumeManager.QueryFCDAllocatedBlocks(
			ctx, volumeID, cnsSnapshotID, uint64(startingOffset), uint32(maxResults))
		if err != nil {
			return nil, logger.LogNewErrorCodef(log, codes.Internal,
				"failed to query allocated blocks: %v", err)
		}

		// Convert to CSI response format
		var blockMetadata []*csi.BlockMetadata
		for _, area := range allocatedAreas {
			blockMetadata = append(blockMetadata, &csi.BlockMetadata{
				ByteOffset: int64(area.Offset),
				SizeBytes:  int64(area.Length),
			})
		}

		response := &csi.GetMetadataAllocatedResponse{
			BlockMetadata:       blockMetadata,
			VolumeCapacityBytes: int64(volumeCapacityBytes),
			BlockMetadataType:   csi.BlockMetadataType_VARIABLE_LENGTH,
		}

		// Note: CSI spec doesn't have StartingOffset in response for pagination.
		// Clients should track nextOffset from the number of results returned.
		// If fewer results than maxResults are returned, pagination is complete.

		log.Infof("GetMetadataAllocated succeeded for snapshot %s, returned %d allocated blocks, next offset %d",
			snapshotID, len(blockMetadata), nextOffset)

		return response, nil
	}

	resp, err := getMetadataAllocatedInternal()
	if err != nil {
		prometheus.CsiControlOpsHistVec.WithLabelValues(volumeType, "GetMetadataAllocated",
			prometheus.PrometheusFailStatus, "NotComputed").Observe(time.Since(start).Seconds())
		return err
	} else {
		prometheus.CsiControlOpsHistVec.WithLabelValues(volumeType, "GetMetadataAllocated",
			prometheus.PrometheusPassStatus, "").Observe(time.Since(start).Seconds())
	}
	return server.Send(resp)
}

// GetMetadataDelta returns the changed blocks between two snapshots using FCD VSLM APIs.
// This implementation uses pure Go through govmomi's VSLM package (no CGO/VDDK required).
func (c *controller) GetMetadataDelta(req *csi.GetMetadataDeltaRequest,
	server csi.SnapshotMetadata_GetMetadataDeltaServer) error {

	ctx := server.Context()
	ctx = logger.NewContextWithLogger(ctx)
	log := logger.GetLogger(ctx)
	log.Infof("GetMetadataDelta: called with args %+v", req)

	// Check if CBT feature is enabled
	isCBTEnabled := commonco.ContainerOrchestratorUtility.IsFSSEnabled(ctx, common.CBT)
	if !isCBTEnabled {
		return logger.LogNewErrorCode(log, codes.Unimplemented, "GetMetadataDelta")
	}

	start := time.Now()
	volumeType := prometheus.PrometheusBlockVolumeType

	getMetadataDeltaInternal := func() (*csi.GetMetadataDeltaResponse, error) {
		// Validate request
		if err := validateGetMetadataDeltaRequest(ctx, req); err != nil {
			return nil, logger.LogNewErrorCodef(log, codes.InvalidArgument,
				"validation for GetMetadataDelta Request: %+v has failed. Error: %v", req, err)
		}

		baseSnapshotID := req.GetBaseSnapshotId()
		targetSnapshotID := req.GetTargetSnapshotId()
		startingOffset := req.GetStartingOffset()
		maxResults := req.GetMaxResults()
		if maxResults == 0 {
			// CSI spec: If zero, the Plugin MUST choose a reasonable maximum number of results.
			maxResults = defaultMaxResults
		}

		// Parse snapshot IDs
		baseVolumeID, baseCnsSnapshotID, err := common.ParseCSISnapshotID(baseSnapshotID)
		if err != nil {
			return nil, logger.LogNewErrorCodef(log, codes.InvalidArgument,
				"failed to parse base snapshot ID %s: %v", baseSnapshotID, err)
		}

		targetVolumeID, targetCnsSnapshotID, err := common.ParseCSISnapshotID(targetSnapshotID)
		if err != nil {
			return nil, logger.LogNewErrorCodef(log, codes.InvalidArgument,
				"failed to parse target snapshot ID %s: %v", targetSnapshotID, err)
		}

		// Verify both snapshots are from the same volume
		if baseVolumeID != targetVolumeID {
			return nil, logger.LogNewErrorCodef(log, codes.InvalidArgument,
				"base snapshot and target snapshot must be from the same volume, got %s and %s",
				baseVolumeID, targetVolumeID)
		}

		volumeID := baseVolumeID

		log.Infof("GetMetadataDelta: querying changed blocks for volume %s, "+
			"base snapshot %s, target snapshot %s, offset %d, max %d",
			volumeID, baseCnsSnapshotID, targetCnsSnapshotID, startingOffset, maxResults)

		// Query volume details
		volumeIds := []cnstypes.CnsVolumeId{{Id: volumeID}}
		cnsQueryFilter := cnstypes.CnsQueryFilter{
			VolumeIds: volumeIds,
		}

		queryResult, err := c.manager.VolumeManager.QueryVolume(ctx, cnsQueryFilter)
		if err != nil {
			return nil, logger.LogNewErrorCodef(log, codes.Internal,
				"failed to query volume %s: %v", volumeID, err)
		}

		if len(queryResult.Volumes) == 0 {
			return nil, logger.LogNewErrorCodef(log, codes.NotFound,
				"volume %s not found", volumeID)
		}

		// Verify volume type
		cnsVolumeType := queryResult.Volumes[0].VolumeType
		if cnsVolumeType != common.BlockVolumeType {
			return nil, logger.LogNewErrorCodef(log, codes.InvalidArgument,
				"GetMetadataDelta is only supported for block volumes, got volume type: %s",
				cnsVolumeType)
		}

		blockBacking, ok := queryResult.Volumes[0].BackingObjectDetails.(*cnstypes.CnsBlockBackingDetails)
		if !ok {
			return nil, logger.LogNewErrorCodef(log, codes.Internal,
				"volume %s does not have block backing details", volumeID)
		}
		volumeCapacityBytes := blockBacking.CapacityInMb * common.MbInBytes

		baseChangeID, err := commonco.ContainerOrchestratorUtility.GetVolumeSnapshotChangeIDBySnapshotID(
			ctx, baseSnapshotID)
		if err != nil {
			return nil, logger.LogNewErrorCodef(log, codes.Internal,
				"failed to get base snapshot changeId from annotation: %v", err)
		}

		changedAreas, nextOffset, err := c.manager.VolumeManager.QueryFCDChangedBlocks(
			ctx, volumeID, targetCnsSnapshotID, baseChangeID, uint64(startingOffset), uint32(maxResults))
		if err != nil {
			return nil, logger.LogNewErrorCodef(log, codes.Internal,
				"failed to query changed blocks: %v", err)
		}

		// Convert to CSI response format
		var blockMetadata []*csi.BlockMetadata
		for _, area := range changedAreas {
			blockMetadata = append(blockMetadata, &csi.BlockMetadata{
				ByteOffset: int64(area.Offset),
				SizeBytes:  int64(area.Length),
			})
		}

		response := &csi.GetMetadataDeltaResponse{
			BlockMetadata:       blockMetadata,
			VolumeCapacityBytes: int64(volumeCapacityBytes),
			BlockMetadataType:   csi.BlockMetadataType_VARIABLE_LENGTH,
		}

		// Note: CSI spec doesn't have StartingOffset in response for pagination.
		// Clients should track nextOffset from the number of results returned.
		// If fewer results than maxResults are returned, pagination is complete.

		log.Infof("GetMetadataDelta succeeded for base snapshot %s, target snapshot %s, "+
			"returned %d changed blocks, next offset %d",
			baseSnapshotID, targetSnapshotID, len(blockMetadata), nextOffset)

		return response, nil
	}

	resp, err := getMetadataDeltaInternal()
	if err != nil {
		prometheus.CsiControlOpsHistVec.WithLabelValues(volumeType, "GetMetadataDelta",
			prometheus.PrometheusFailStatus, "NotComputed").Observe(time.Since(start).Seconds())
		return err
	} else {
		prometheus.CsiControlOpsHistVec.WithLabelValues(volumeType, "GetMetadataDelta",
			prometheus.PrometheusPassStatus, "").Observe(time.Since(start).Seconds())
	}
	return server.Send(resp)
}

// getSnapshotChangeIdFromFCD retrieves the changeId from a snapshot using FCD VSLM APIs via the volume manager.
func (c *controller) getSnapshotChangeIdFromFCD(ctx context.Context, volumeID, snapshotID string) (string, error) {
	return c.manager.VolumeManager.GetFCDSnapshotChangeID(ctx, volumeID, snapshotID)
}

// validateGetMetadataAllocatedRequest validates the GetMetadataAllocated request
func validateGetMetadataAllocatedRequest(ctx context.Context, req *csi.GetMetadataAllocatedRequest) error {
	log := logger.GetLogger(ctx)

	if req == nil {
		return fmt.Errorf("GetMetadataAllocated request is nil")
	}

	if req.SnapshotId == "" {
		return fmt.Errorf("snapshot ID is required")
	}

	log.Debugf("GetMetadataAllocated request validation passed")
	return nil
}

// validateGetMetadataDeltaRequest validates the GetMetadataDelta request
func validateGetMetadataDeltaRequest(ctx context.Context, req *csi.GetMetadataDeltaRequest) error {
	log := logger.GetLogger(ctx)

	if req == nil {
		return fmt.Errorf("GetMetadataDelta request is nil")
	}

	if req.BaseSnapshotId == "" {
		return fmt.Errorf("base snapshot ID is required")
	}

	if req.TargetSnapshotId == "" {
		return fmt.Errorf("target snapshot ID is required")
	}

	if req.BaseSnapshotId == req.TargetSnapshotId {
		return fmt.Errorf("base snapshot and target snapshot must be different")
	}

	log.Debugf("GetMetadataDelta request validation passed")
	return nil
}
