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

	cnstypes "github.com/vmware/govmomi/cns/types"
	"google.golang.org/grpc/codes"
	cnsvolume "sigs.k8s.io/vsphere-csi-driver/pkg/common/cns-lib/volume"
	cnsvsphere "sigs.k8s.io/vsphere-csi-driver/pkg/common/cns-lib/vsphere"
	"sigs.k8s.io/vsphere-csi-driver/pkg/csi/service/logger"
)

// QueryVolumeUtil helps to invoke query volume API based on the feature
// state set for using query async volume. If useQueryVolumeAsync is set to
// true, the function invokes CNS QueryVolumeAsync, otherwise it invokes
// synchronous QueryVolume API. The function also take volume manager instance,
// query filters, query selection as params. Returns queryResult when query
// volume succeeds, otherwise returns appropriate errors.
func QueryVolumeUtil(ctx context.Context, m cnsvolume.Manager, queryFilter cnstypes.CnsQueryFilter,
	querySelection cnstypes.CnsQuerySelection, useQueryVolumeAsync bool) (*cnstypes.CnsQueryResult, error) {
	log := logger.GetLogger(ctx)
	var queryAsyncNotSupported bool
	var queryResult *cnstypes.CnsQueryResult
	var err error
	if useQueryVolumeAsync {
		// AsyncQueryVolume feature switch is disabled.
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

// QueryAllVolumeUtil helps to invoke query volume API based on the feature
// state set for using query async volume. If useQueryVolumeAsync is set to
// true, the function invokes CNS QueryVolumeAsync, otherwise it invokes
// synchronous QueryAllVolume API. The function also take volume manager
// instance, query filters, query selection as params. Returns queryResult
// when query volume succeeds, otherwise returns appropriate errors.
func QueryAllVolumeUtil(ctx context.Context, m cnsvolume.Manager, queryFilter cnstypes.CnsQueryFilter,
	querySelection cnstypes.CnsQuerySelection, useQueryVolumeAsync bool) (*cnstypes.CnsQueryResult, error) {
	log := logger.GetLogger(ctx)
	var queryAsyncNotSupported bool
	var queryResult *cnstypes.CnsQueryResult
	var err error
	if useQueryVolumeAsync {
		// AsyncQueryVolume feature switch is disabled.
		queryResult, err = m.QueryVolumeAsync(ctx, queryFilter, querySelection)
		if err != nil {
			if err.Error() == cnsvsphere.ErrNotSupported.Error() {
				log.Warn("QueryVolumeAsync is not supported. Invoking QueryAllVolume API")
				queryAsyncNotSupported = true
			} else { // Return for any other failures.
				return nil, logger.LogNewErrorCodef(log, codes.Internal,
					"queryVolumeAsync failed for queryFilter: %v. Err=%+v", queryFilter, err.Error())
			}
		}
	}
	if !useQueryVolumeAsync || queryAsyncNotSupported {
		queryResult, err = m.QueryAllVolume(ctx, queryFilter, querySelection)
		if err != nil {
			return nil, logger.LogNewErrorCodef(log, codes.Internal,
				"queryAllVolume failed for queryFilter: %+v. Err=%+v", queryFilter, err.Error())
		}
	}
	return queryResult, nil
}

func QuerySnapshotsUtil(ctx context.Context, m cnsvolume.Manager, snapshotQueryFilter cnstypes.CnsSnapshotQueryFilter) ([]cnstypes.CnsSnapshotQueryResultEntry, error) {
	log := logger.GetLogger(ctx)
	var allQuerySnapshotResults []cnstypes.CnsSnapshotQueryResultEntry
	var snapshotQuerySpec cnstypes.CnsSnapshotQuerySpec
	if snapshotQueryFilter.SnapshotQuerySpecs == nil {
		log.Infof("Attempting to retrieve all the Snapshots available in the vCenter inventory.")
	} else {
		snapshotQuerySpec = snapshotQueryFilter.SnapshotQuerySpecs[0]
		log.Infof("Invoking QuerySnapshots with spec: %+v", snapshotQuerySpec)
	}
	iteration := 1
	for {
		snapshotQueryResult, err := m.QuerySnapshots(ctx, snapshotQueryFilter)
		if err != nil {
			log.Errorf("querySnapshots failed for snapshotQueryFilter: %v. Err=%+v", snapshotQueryFilter, err)
			return nil, err
		}
		if snapshotQueryResult == nil {
			log.Infof("Observed empty SnapshotQueryResult")
			break
		}
		if len(snapshotQueryResult.Entries) == 0 {
			log.Infof("QuerySnapshots retrieved no results for the spec: %+v", snapshotQuerySpec)
		}
		allQuerySnapshotResults = append(allQuerySnapshotResults, snapshotQueryResult.Entries...)
		log.Infof("%v more snapshots to be queried", snapshotQueryResult.Cursor.TotalRecords-snapshotQueryResult.Cursor.Offset)
		if snapshotQueryResult.Cursor.Offset == snapshotQueryResult.Cursor.TotalRecords {
			log.Infof("QuerySnapshots retrieved all records (%d) for the SnapshotQuerySpec: %+v in %d iterations",
				snapshotQueryResult.Cursor.TotalRecords, snapshotQuerySpec, iteration)
			break
		}
		iteration++
		snapshotQueryFilter.Cursor = &snapshotQueryResult.Cursor
	}
	return allQuerySnapshotResults, nil
}
