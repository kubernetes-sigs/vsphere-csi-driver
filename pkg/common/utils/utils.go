/*
Copyright 2021 The Kubernetes Authors.

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

package utils

import (
	"context"
	"math"
	"strconv"

	cnstypes "github.com/vmware/govmomi/cns/types"
	"github.com/vmware/govmomi/vim25/types"
	"google.golang.org/grpc/codes"
	cnsvolume "sigs.k8s.io/vsphere-csi-driver/v3/pkg/common/cns-lib/volume"
	cnsvsphere "sigs.k8s.io/vsphere-csi-driver/v3/pkg/common/cns-lib/vsphere"
	"sigs.k8s.io/vsphere-csi-driver/v3/pkg/csi/service/logger"
)

const (
	// DefaultQuerySnapshotLimit constant is already present in pkg/csi/service/common/constants.go
	// However, using that constant creates an import cycle.
	// TODO: Refactor to move all the constants into a top level directory.
	DefaultQuerySnapshotLimit = int64(128)
)

// QueryVolumeUtil helps to invoke query volume API based on the feature
// state set for using query async volume. If useQueryVolumeAsync is set to
// true, the function invokes CNS QueryVolumeAsync, otherwise it invokes
// synchronous QueryVolume API. The function also take volume manager instance,
// query filters, query selection as params. Returns queryResult when query
// volume succeeds, otherwise returns appropriate errors.
func QueryVolumeUtil(ctx context.Context, m cnsvolume.Manager, queryFilter cnstypes.CnsQueryFilter,
	querySelection *cnstypes.CnsQuerySelection, useQueryVolumeAsync bool) (*cnstypes.CnsQueryResult, error) {
	log := logger.GetLogger(ctx)
	var queryAsyncNotSupported bool
	var queryResult *cnstypes.CnsQueryResult
	var err error
	if useQueryVolumeAsync {
		// AsyncQueryVolume feature switch is enabled.
		queryResult, err = m.QueryVolumeAsync(ctx, queryFilter, querySelection)
		if err != nil {
			if err.Error() == cnsvsphere.ErrNotSupported.Error() {
				log.Warn("QueryVolumeAsync is not supported. Invoking QueryVolume API")
				queryAsyncNotSupported = true
			} else { // Return for any other failures.
				return nil, logger.LogNewErrorCodef(log, codes.Internal,
					"queryVolumeAsync failed for queryFilter: %v. Err=%+v", queryFilter, err.Error())
			}
		}
	}
	if !useQueryVolumeAsync || queryAsyncNotSupported {
		queryResult, err = m.QueryVolume(ctx, queryFilter)
		if err != nil {
			return nil, logger.LogNewErrorCodef(log, codes.Internal,
				"queryVolume failed for queryFilter: %+v. Err=%+v", queryFilter, err.Error())
		}
	}
	return queryResult, nil
}

// QuerySnapshotsUtil helps invoke CNS QuerySnapshot API. The method takes in a snapshotQueryFilter that represents
// the criteria to retrieve the snapshots. The maxEntries represents the max number of results that the caller of this
// method can handle.
func QuerySnapshotsUtil(ctx context.Context, m cnsvolume.Manager, snapshotQueryFilter cnstypes.CnsSnapshotQueryFilter,
	maxEntries int64) ([]cnstypes.CnsSnapshotQueryResultEntry, string, error) {
	log := logger.GetLogger(ctx)
	var allQuerySnapshotResults []cnstypes.CnsSnapshotQueryResultEntry
	var snapshotQuerySpec cnstypes.CnsSnapshotQuerySpec
	var batchSize int64
	maxIteration := int64(1)
	isMaxIterationSet := false
	if snapshotQueryFilter.SnapshotQuerySpecs == nil {
		log.Infof("Attempting to retrieve all the Snapshots available in the vCenter inventory.")
	} else {
		snapshotQuerySpec = snapshotQueryFilter.SnapshotQuerySpecs[0]
		log.Infof("Invoking QuerySnapshots with spec: %+v", snapshotQuerySpec)
	}
	// Check if cursor is specified, if not set a default cursor.
	if snapshotQueryFilter.Cursor == nil {
		// Setting the default limit(128) explicitly.
		snapshotQueryFilter = cnstypes.CnsSnapshotQueryFilter{
			Cursor: &cnstypes.CnsCursor{
				Offset: 0,
				Limit:  DefaultQuerySnapshotLimit,
			},
		}
		batchSize = DefaultQuerySnapshotLimit
	} else {
		batchSize = snapshotQueryFilter.Cursor.Limit
	}
	iteration := int64(1)
	for {
		if iteration > maxIteration {
			// Exceeds the max number of results that can be handled by callers.
			nextToken := strconv.FormatInt(snapshotQueryFilter.Cursor.Offset, 10)
			log.Infof("the number of results: %d approached max-entries: %d for "+
				"limit: %d in iteration: %d, returning with next-token: %s",
				len(allQuerySnapshotResults), maxEntries, batchSize, iteration, nextToken)
			return allQuerySnapshotResults, nextToken, nil
		}
		log.Infof("invoking QuerySnapshots in iteration: %d with offset: %d and limit: %d, current total "+
			"results: %d", iteration, snapshotQueryFilter.Cursor.Offset, snapshotQueryFilter.Cursor.Limit,
			len(allQuerySnapshotResults))
		snapshotQueryResult, err := m.QuerySnapshots(ctx, snapshotQueryFilter)
		if err != nil {
			log.Errorf("querySnapshots failed for snapshotQueryFilter: %v. Err=%+v", snapshotQueryFilter, err)
			return nil, "", err
		}
		if snapshotQueryResult == nil {
			log.Infof("Observed empty SnapshotQueryResult")
			break
		}
		if len(snapshotQueryResult.Entries) == 0 {
			log.Infof("QuerySnapshots retrieved no results for the spec: %+v", snapshotQuerySpec)
		}
		// Update the max iteration.
		// isMaxIterationSet ensures that the max iterations are set only once, this is to ensure that the number of
		// results are lower than the max entries supported by caller in a busy system which has increasing number
		// total records.
		if !isMaxIterationSet {
			if snapshotQueryResult.Cursor.TotalRecords < maxEntries {
				// If the total number of records is less than max entries supported by caller then
				// all results can be retrieved in a loop, when the results are returned no next-token is expected to be set.
				// Example:
				// maxEntries=200, totalRecords=150, batchSize=128
				// maxIteration=2
				// iteration-1: 128 results, iteration-2: 22 results
				// total results returned: 150
				// offset=0
				maxRecords := snapshotQueryResult.Cursor.TotalRecords
				numOfIterationsForAllResults := float64(maxRecords) / float64(batchSize)
				maxIteration = int64(math.Ceil(numOfIterationsForAllResults))
				log.Infof("setting max iteration to %d for total records count: %d", maxIteration, maxRecords)
			} else {
				// All results cannot be returned to caller, in this case the expectation is return as many results with a
				// nextToken.
				// Example:
				// maxEntries=150, totalRecords=200, batchSize=128
				// maxIteration=1
				// iteration-1: 128 results
				// total results returned: 128
				// offset= 1, callers are expected to call with new offset as next token.
				maxRecords := maxEntries
				numOfIterationsForAllResults := float64(maxRecords) / float64(batchSize)
				maxIteration = int64(math.Floor(numOfIterationsForAllResults))
				log.Infof("setting max iteration to %d for total records count: %d and max limit: %d",
					maxIteration, snapshotQueryResult.Cursor.TotalRecords, maxRecords)
			}
			isMaxIterationSet = true
		}

		allQuerySnapshotResults = append(allQuerySnapshotResults, snapshotQueryResult.Entries...)
		log.Infof("%d more snapshots to be queried",
			snapshotQueryResult.Cursor.TotalRecords-snapshotQueryResult.Cursor.Offset)
		if snapshotQueryResult.Cursor.Offset == snapshotQueryResult.Cursor.TotalRecords {
			log.Infof("QuerySnapshots retrieved all records (%d) for the SnapshotQuerySpec: %+v in %d iterations",
				snapshotQueryResult.Cursor.TotalRecords, snapshotQuerySpec, iteration)
			break
		}
		iteration++
		snapshotQueryFilter.Cursor = &snapshotQueryResult.Cursor
	}
	return allQuerySnapshotResults, "", nil
}

type CnsVolumeDetails struct {
	VolumeID     string
	SizeInMB     int64
	DatastoreUrl string
	VolumeType   string
}

// QueryVolumeDetailsUtil queries Capacity in MB and datastore URL for the source volume with expected volume type.
func QueryVolumeDetailsUtil(ctx context.Context, m cnsvolume.Manager, volumeIds []cnstypes.CnsVolumeId) (
	map[string]*CnsVolumeDetails, error) {
	log := logger.GetLogger(ctx)
	volumeDetailsMap := make(map[string]*CnsVolumeDetails)
	// Select only the backing object details, volume type and datastore.
	querySelection := &cnstypes.CnsQuerySelection{
		Names: []string{
			string(cnstypes.QuerySelectionNameTypeBackingObjectDetails),
			string(cnstypes.QuerySelectionNameTypeVolumeType),
			string(cnstypes.QuerySelectionNameTypeDataStoreUrl),
		},
	}
	queryFilter := cnstypes.CnsQueryFilter{
		VolumeIds: volumeIds,
	}
	log.Infof("Invoking QueryAllVolumeUtil with Filter: %+v, Selection: %+v", queryFilter, *querySelection)
	allQueryResults, err := m.QueryAllVolume(ctx, queryFilter, *querySelection)
	if err != nil {
		log.Errorf("failed to retrieve the volume size and datastore, err: %+v", err)
		return volumeDetailsMap, logger.LogNewErrorCodef(log, codes.Internal,
			"failed to retrieve the volume sizes: %+v", err)
	}
	log.Infof("Number of results from QueryAllVolumeUtil: %d", len(allQueryResults.Volumes))
	for _, res := range allQueryResults.Volumes {
		volumeId := res.VolumeId
		datastoreUrl := res.DatastoreUrl
		volumeCapacityInMB := res.BackingObjectDetails.GetCnsBackingObjectDetails().CapacityInMb
		volumeType := res.VolumeType
		log.Debugf("VOLUME: %s, TYPE: %s, DATASTORE: %s, CAPACITY: %d", volumeId, volumeType, datastoreUrl,
			volumeCapacityInMB)
		volumeDetails := &CnsVolumeDetails{
			VolumeID:     volumeId.Id,
			SizeInMB:     volumeCapacityInMB,
			DatastoreUrl: datastoreUrl,
			VolumeType:   volumeType,
		}
		volumeDetailsMap[volumeId.Id] = volumeDetails
	}
	return volumeDetailsMap, nil
}

// GetDatastoreRefByURLFromGivenDatastoreList fetches the datastore reference by datastore URL
// from a list of datastore references.
// If the datastore with dsURL can be found in the same datacenter as the given VC
// and it is also found in the given datastoreList, return the reference of the datastore.
// Otherwise, return error.
func GetDatastoreRefByURLFromGivenDatastoreList(ctx context.Context, vc *cnsvsphere.VirtualCenter,
	datastoreList []types.ManagedObjectReference, dsURL string) (*types.ManagedObjectReference, error) {
	log := logger.GetLogger(ctx)
	// Get all datacenters in the virtualcenter
	datacenters, err := vc.GetDatacenters(ctx)
	if err != nil {
		log.Errorf("failed to find datacenters from VC: %q, Error: %+v", vc.Config.Host, err)
		return nil, err
	}
	var candidateDsObj *cnsvsphere.Datastore
	// traverse each datacenter and find the datastore with the specified dsURL
	for _, datacenter := range datacenters {
		candidateDsInfoObj, err := datacenter.GetDatastoreInfoByURL(ctx, dsURL)
		if err != nil {
			log.Errorf("failed to find datastore with URL %q in datacenter %q from VC %q, Error: %+v",
				dsURL, datacenter.InventoryPath, vc.Config.Host, err)
			continue
		}
		candidateDsObj = candidateDsInfoObj.Datastore
		break
	}
	if candidateDsObj == nil {
		// fail if the candidate datastore is not found in the virtualcenter
		return nil, logger.LogNewErrorf(log,
			"failed to find datastore with URL %q in VC %q", dsURL, vc.Config.Host)
	}

	for _, datastoreRef := range datastoreList {
		if datastoreRef == candidateDsObj.Reference() {
			log.Infof("compatible datastore found, dsURL = %q, dsRef = %v", dsURL, datastoreRef)
			return &datastoreRef, nil
		}
	}
	return nil, logger.LogNewErrorf(log,
		"failed to find datastore with URL %q from the input datastore list, %v", dsURL, datastoreList)
}
