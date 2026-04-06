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
	"strings"
	"time"

	"github.com/container-storage-interface/spec/lib/go/csi"
	cnstypes "github.com/vmware/govmomi/cns/types"
	"github.com/vmware/govmomi/vim25/soap"
	"github.com/vmware/govmomi/vim25/types"
	"github.com/vmware/govmomi/vslm"
	"go.uber.org/zap"
	"google.golang.org/grpc/codes"

	cnsvsphere "sigs.k8s.io/vsphere-csi-driver/v3/pkg/common/cns-lib/vsphere"
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

// For unit testing purposes
var queryChangedDiskAreasFunc = func(ctx context.Context, vcenter *cnsvsphere.VirtualCenter,
	volumeID types.ID, snapshotID types.ID, startingOffset int64, changeId string) (*types.DiskChangeInfo, error) {
	vslmClient, err := vslm.NewClient(ctx, vcenter.Client.Client)
	if err != nil {
		return nil, fmt.Errorf("failed to create VSLM client: %v", err)
	}
	globalObjectManager := vslm.NewGlobalObjectManager(vslmClient)
	return globalObjectManager.QueryChangedDiskAreas(ctx, volumeID, snapshotID, startingOffset, changeId)
}

var retrieveSnapshotDetailsFunc = func(ctx context.Context, vcenter *cnsvsphere.VirtualCenter,
	volumeID types.ID, snapshotID types.ID) (*types.VStorageObjectSnapshotDetails, error) {
	vslmClient, err := vslm.NewClient(ctx, vcenter.Client.Client)
	if err != nil {
		return nil, fmt.Errorf("failed to create VSLM client: %v", err)
	}
	globalObjectManager := vslm.NewGlobalObjectManager(vslmClient)
	return globalObjectManager.RetrieveSnapshotDetails(ctx, volumeID, snapshotID)
}

func (c *controller) GetMetadataAllocated(req *csi.GetMetadataAllocatedRequest,
	server csi.SnapshotMetadata_GetMetadataAllocatedServer) error {

	ctx := server.Context()
	ctx = logger.NewContextWithLogger(ctx)
	log := logger.GetLogger(ctx)
	log.Infof("GetMetadataAllocated: called with args %+v", req)

	// Check if CBT feature is enabled.
	//
	// On Supervisor we gate on the CSI_Backup_API WCP capability. The guest
	// pvCSI gates on common.CSI_Backup_API_FSS, which is mapped via
	// common.WCPFeatureStateAssociatedWithPVCSI to this same Supervisor
	// capability, so both ends consult the same source of truth in VKS.
	isCSIBackupAPIEnabled := commonco.ContainerOrchestratorUtility.IsFSSEnabled(ctx, common.CSI_Backup_API)
	if !isCSIBackupAPIEnabled {
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

		// Query allocated blocks using FCD APIs
		allocatedAreas, nextOffset, err := c.queryAllocatedBlocksFromFCD(
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

// GetMetadataDelta returns the metadata for the changed blocks between two snapshots using
// FCD VSLM APIs. This implementation uses Go through govmomi's VSLM package.
//
// vSphere semantics for the request fields:
//   - base_snapshot_id is the vSphere CBT change-id of the base snapshot (the
//     `csi.vsphere.volume/change-id` annotation on the VolumeSnapshot in both guest and
//     supervisor clusters). Backup software is expected to persist that
//     change-id in its own catalog and pass it back here. We do not look it up from the base
//     VolumeSnapshot because the base snapshot may have been deleted (vSphere best practice).
//   - target_snapshot_id is the CSI snapshot handle of the target snapshot ("volID+snapID");
//     the target snapshot must still exist for VSLM to query its blocks.
//
// The volume ID is therefore taken from target_snapshot_id only.
func (c *controller) GetMetadataDelta(req *csi.GetMetadataDeltaRequest,
	server csi.SnapshotMetadata_GetMetadataDeltaServer) error {

	ctx := server.Context()
	ctx = logger.NewContextWithLogger(ctx)
	log := logger.GetLogger(ctx)
	log.Infof("GetMetadataDelta: called with args %+v", req)

	// Check if CBT feature is enabled.
	//
	// See GetMetadataAllocated above for the wcp / wcpguest split: on
	// Supervisor we gate on the CSI_Backup_API WCP capability, while pvCSI
	// gates on common.CSI_Backup_API_FSS which maps to the same capability.
	isCSIBackupAPIEnabled := commonco.ContainerOrchestratorUtility.IsFSSEnabled(ctx, common.CSI_Backup_API)
	if !isCSIBackupAPIEnabled {
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

		// base_snapshot_id is the vSphere change-id, an opaque string. Pass it straight to
		// VSLM.QueryChangedDiskAreas without parsing.
		baseChangeID := req.GetBaseSnapshotId()
		targetSnapshotID := req.GetTargetSnapshotId()
		startingOffset := req.GetStartingOffset()
		maxResults := req.GetMaxResults()
		if maxResults == 0 {
			// CSI spec: If zero, the Plugin MUST choose a reasonable maximum number of results.
			maxResults = defaultMaxResults
		}

		// Only the target snapshot ID is parsed for volume ID lookup; the target snapshot
		// must exist on the Supervisor for VSLM to query its blocks.
		volumeID, targetCnsSnapshotID, err := common.ParseCSISnapshotID(targetSnapshotID)
		if err != nil {
			return nil, logger.LogNewErrorCodef(log, codes.InvalidArgument,
				"failed to parse target snapshot ID %s: %v", targetSnapshotID, err)
		}

		log.Infof("GetMetadataDelta: querying changed blocks for volume %s, "+
			"target snapshot %s, base change-id %s, offset %d, max %d",
			volumeID, targetCnsSnapshotID, baseChangeID, startingOffset, maxResults)

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

		// Query changed blocks using FCD APIs. baseChangeID is the change-id from the caller.
		changedAreas, nextOffset, err := c.queryChangedAreasFromFCD(
			ctx, volumeID, baseChangeID, targetCnsSnapshotID, uint64(startingOffset), uint32(maxResults))
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

		log.Infof("GetMetadataDelta succeeded for target snapshot %s, "+
			"returned %d changed blocks, next offset %d",
			targetSnapshotID, len(blockMetadata), nextOffset)

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

// AllocatedArea represents an allocated area on disk
type AllocatedArea struct {
	Offset uint64
	Length uint64
}

// ChangedArea represents a changed area between two snapshots
type ChangedArea struct {
	Offset uint64
	Length uint64
}

// queryAllocatedBlocksFromFCD queries allocated blocks using FCD VSLM APIs.
// It uses QueryChangedDiskAreas with changeId="*" to get all allocated blocks.
func (c *controller) queryAllocatedBlocksFromFCD(ctx context.Context, volumeID, snapshotID string,
	startingOffset uint64, maxResults uint32) ([]AllocatedArea, uint64, error) {

	log := logger.GetLogger(ctx)
	log.Debugf("queryAllocatedBlocksFromFCD: volume=%s snapshot=%s offset=%d maxResults=%d",
		volumeID, snapshotID, startingOffset, maxResults)

	// Get vCenter connection
	vcenter, err := common.GetVCenter(ctx, c.manager)
	if err != nil {
		return nil, 0, fmt.Errorf("failed to get vCenter instance: %v", err)
	}

	// Convert IDs to VSLM format
	vslmVolumeID := types.ID{Id: volumeID}
	vslmSnapshotID := types.ID{Id: snapshotID}

	// Use "*" as changeId to get all allocated blocks
	changeId := "*"

	// Query changed disk areas (all allocated blocks when changeId="*")
	log.Debugf("Calling VSLM QueryChangedDiskAreas with changeId='*' for all allocated blocks")
	diskChangeInfo, err := queryChangedDiskAreasFunc(
		ctx,
		vcenter,
		vslmVolumeID,
		vslmSnapshotID,
		int64(startingOffset),
		changeId,
	)
	if err != nil {
		return nil, 0, translateVslmError(log, err)
	}

	// Convert to our result format
	var allocatedAreas []AllocatedArea
	nextOffset := startingOffset

	for _, area := range diskChangeInfo.ChangedArea {
		if uint32(len(allocatedAreas)) >= maxResults {
			break
		}

		allocatedArea := AllocatedArea{
			Offset: uint64(area.Start),
			Length: uint64(area.Length),
		}
		allocatedAreas = append(allocatedAreas, allocatedArea)

		// Track the end of this area
		areaEnd := uint64(area.Start) + uint64(area.Length)
		if areaEnd > nextOffset {
			nextOffset = areaEnd
		}
	}

	// If we got fewer results than requested, we're done (set nextOffset to 0)
	if uint32(len(allocatedAreas)) < maxResults {
		nextOffset = 0
	}

	log.Debugf("queryAllocatedBlocksFromFCD: returned %d allocated areas, next offset %d",
		len(allocatedAreas), nextOffset)

	return allocatedAreas, nextOffset, nil
}

// queryChangedAreasFromFCD queries changed blocks using FCD VSLM APIs.
// baseChangeID is the vSphere CBT change-id supplied by the caller (via the CSI
// GetMetadataDelta request's base_snapshot_id field). It is forwarded verbatim to
// VSLM.QueryChangedDiskAreas; the base snapshot itself does not have to exist anymore.
func (c *controller) queryChangedAreasFromFCD(ctx context.Context, volumeID, baseChangeID,
	targetSnapshotID string, startingOffset uint64, maxResults uint32) ([]ChangedArea, uint64, error) {

	log := logger.GetLogger(ctx)
	log.Debugf("queryChangedAreasFromFCD: volume=%s target=%s offset=%d maxResults=%d (base change-id supplied)",
		volumeID, targetSnapshotID, startingOffset, maxResults)

	// Get vCenter connection
	vcenter, err := common.GetVCenter(ctx, c.manager)
	if err != nil {
		return nil, 0, fmt.Errorf("failed to get vCenter instance: %v", err)
	}

	// Convert IDs to VSLM format
	vslmVolumeID := types.ID{Id: volumeID}
	vslmTargetSnapshotID := types.ID{Id: targetSnapshotID}

	// Query changed disk areas
	log.Debugf("Calling VSLM QueryChangedDiskAreas with caller-supplied baseChangeID for delta")
	diskChangeInfo, err := queryChangedDiskAreasFunc(
		ctx,
		vcenter,
		vslmVolumeID,
		vslmTargetSnapshotID,
		int64(startingOffset),
		baseChangeID,
	)
	if err != nil {
		return nil, 0, translateVslmError(log, err)
	}

	// Convert to our result format
	var changedAreas []ChangedArea
	nextOffset := startingOffset

	for _, area := range diskChangeInfo.ChangedArea {
		if uint32(len(changedAreas)) >= maxResults {
			break
		}

		changedArea := ChangedArea{
			Offset: uint64(area.Start),
			Length: uint64(area.Length),
		}
		changedAreas = append(changedAreas, changedArea)

		// Track the end of this area
		areaEnd := uint64(area.Start) + uint64(area.Length)
		if areaEnd > nextOffset {
			nextOffset = areaEnd
		}
	}

	// If we got fewer results than requested, we're done (set nextOffset to 0)
	if uint32(len(changedAreas)) < maxResults {
		nextOffset = 0
	}

	log.Debugf("queryChangedAreasFromFCD: returned %d changed areas, next offset %d",
		len(changedAreas), nextOffset)

	return changedAreas, nextOffset, nil
}

// getSnapshotChangeIdFromFCD retrieves the changeId from a snapshot using FCD VSLM APIs.
func (c *controller) getSnapshotChangeIdFromFCD(ctx context.Context, volumeID, snapshotID string) (string, error) {
	log := logger.GetLogger(ctx)

	// Get vCenter connection
	vcenter, err := common.GetVCenter(ctx, c.manager)
	if err != nil {
		return "", fmt.Errorf("failed to get vCenter instance: %v", err)
	}

	vslmVolumeID := types.ID{Id: volumeID}
	vslmSnapshotID := types.ID{Id: snapshotID}

	// Retrieve snapshot details
	log.Debugf("Retrieving snapshot details for snapshot %s", snapshotID)
	snapshotDetails, err := retrieveSnapshotDetailsFunc(
		ctx,
		vcenter,
		vslmVolumeID,
		vslmSnapshotID,
	)
	if err != nil {
		return "", fmt.Errorf("failed to retrieve snapshot details via VSLM API: %v", err)
	}

	// Extract changeId from snapshot
	changeId := snapshotDetails.ChangedBlockTrackingId
	if changeId == "" {
		return "", fmt.Errorf("changeId is empty in snapshot %s (CBT may not be enabled)", snapshotID)
	}

	log.Debugf("Retrieved changeId %s for snapshot %s", changeId, snapshotID)
	return changeId, nil
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

// validateGetMetadataDeltaRequest validates the GetMetadataDelta request.
//
// base_snapshot_id is the vSphere CBT change-id (opaque string) — we only check that it is
// non-empty. target_snapshot_id is a CSI snapshot handle that the caller is responsible for
// keeping valid (the target snapshot must still exist in vSphere).
func validateGetMetadataDeltaRequest(ctx context.Context, req *csi.GetMetadataDeltaRequest) error {
	log := logger.GetLogger(ctx)

	if req == nil {
		return fmt.Errorf("GetMetadataDelta request is nil")
	}

	if req.BaseSnapshotId == "" {
		return fmt.Errorf("base snapshot ID (vSphere change-id) is required")
	}

	if req.TargetSnapshotId == "" {
		return fmt.Errorf("target snapshot ID is required")
	}

	log.Debugf("GetMetadataDelta request validation passed")
	return nil
}

// translateVslmError maps VSLM and VADP error codes to standard CSI gRPC error codes
func translateVslmError(log *zap.SugaredLogger, err error) error {
	if err == nil {
		return nil
	}

	errMsg := err.Error()

	// Check if it's a SOAP fault from vCenter
	if soap.IsSoapFault(err) {
		fault := soap.ToSoapFault(err).VimFault()

		switch f := fault.(type) {
		case *types.FileFault:
			msgID := ""
			if len(f.FaultMessage) > 0 {
				msgID = f.FaultMessage[0].Key
			} else {
				if strings.Contains(errMsg, "vim.hostd.vmsvc.cbt.noTrack") {
					msgID = "vim.hostd.vmsvc.cbt.noTrack"
				} else if strings.Contains(errMsg, "vim.hostd.vmsvc.cbt.noEpoch") {
					msgID = "vim.hostd.vmsvc.cbt.noEpoch"
				} else if strings.Contains(errMsg, "vim.hostd.vmsvc.cbt.cannotGetChanges") {
					msgID = "vim.hostd.vmsvc.cbt.cannotGetChanges"
				}
			}

			switch msgID {
			case "vim.hostd.vmsvc.cbt.noTrack":
				return logger.LogNewErrorCodef(log, codes.FailedPrecondition,
					"CBT disabled or not enabled. The caller should perform a full backup instead: %v", err)
			case "vim.hostd.vmsvc.cbt.noEpoch":
				return logger.LogNewErrorCodef(log, codes.FailedPrecondition,
					"Cannot get current epoch when changeId=*. The caller should perform a full backup instead: %v", err)
			case "vim.hostd.vmsvc.cbt.cannotGetChanges":
				if strings.Contains(strings.ToLower(errMsg), "corrupt") {
					return logger.LogNewErrorCodef(log, codes.FailedPrecondition,
						"ctk file corrupted. The caller should perform a full backup instead: %v", err)
				}
				return logger.LogNewErrorCodef(log, codes.InvalidArgument,
					"changeID mismatch. The caller should correct the error and resubmit the call: %v", err)
			default:
				return logger.LogNewErrorCodef(log, codes.Internal,
					"Internal errors such ctk disk open fails, FCD disk locked, disk missing, etc. "+
						"CSI driver should retry the operation: %v", err)
			}
		case *types.SystemError:
			return logger.LogNewErrorCodef(log, codes.Internal,
				"Internal system error. CSI driver should retry the operation: %v", err)
		case *types.InvalidArgument:
			if f.InvalidProperty == "startOffset" || strings.Contains(errMsg, "startOffset") {
				return logger.LogNewErrorCodef(log, codes.OutOfRange,
					"start offset specified beyond volume size. The caller should specify a valid offset: %v", err)
			}
			if f.InvalidProperty == "snapshotId" || strings.Contains(errMsg, "snapshotId") {
				return logger.LogNewErrorCodef(log, codes.NotFound,
					"snapshot ID not found for FCD. The caller should re-check that these objects exist: %v", err)
			}
			if f.InvalidProperty == "changeId" || strings.Contains(errMsg, "changeId") {
				return logger.LogNewErrorCodef(log, codes.InvalidArgument,
					"invalid format for changeID. The caller should correct the error and resubmit the call: %v", err)
			}
			if f.InvalidProperty == "deviceKey" || strings.Contains(errMsg, "deviceKey") {
				return logger.LogNewErrorCodef(log, codes.InvalidArgument,
					"Device key doesn't exist, Disk has no backing, or Disk backing "+
						"type doesn't support CBT. The caller should correct the "+
						"error and resubmit the call: %v", err)
			}
			return logger.LogNewErrorCodef(log, codes.InvalidArgument,
				"invalid argument %s: %v", f.InvalidProperty, err)
		case *types.NotFound:
			return logger.LogNewErrorCodef(log, codes.NotFound,
				"FCD not found in VC inventory. The caller should re-check that these objects exist: %v", err)
		}
	}

	// Default fallback
	return logger.LogNewErrorCodef(log, codes.Internal, "failed with error: %v", err)
}
