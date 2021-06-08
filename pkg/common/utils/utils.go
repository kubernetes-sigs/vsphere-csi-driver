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

// QueryVolumeUtil helps to invoke query volume API based on the feature state set for using query async volume.
// If useQueryVolumeAsync is set to true, the function invokes CNS QueryVolumeAsync, otherwise it invokes synchronous QueryVolume API
// The function also take volume manager instance, query filters, query selection as params
// Returns queryResult when query volume succeeds, otherwise returns appropriate errors
func QueryVolumeUtil(ctx context.Context, m cnsvolume.Manager, queryFilter cnstypes.CnsQueryFilter, querySelection cnstypes.CnsQuerySelection, useQueryVolumeAsync bool) (*cnstypes.CnsQueryResult, error) {
	log := logger.GetLogger(ctx)
	var queryAsyncNotSupported bool
	var queryResult *cnstypes.CnsQueryResult
	var err error
	if useQueryVolumeAsync {
		// AsyncQueryVolume feature switch is disabled
		queryResult, err = m.QueryVolumeAsync(ctx, queryFilter, querySelection)
		if err != nil {
			if err.Error() == cnsvsphere.ErrNotSupported.Error() {
				log.Warn("QueryVolumeAsync is not supported. Invoking QueryVolume API")
				queryAsyncNotSupported = true
			} else { // Return for any other failures
				return nil, logger.LogNewErrorCodef(log, codes.Internal,
					"QueryVolumeAsync failed for queryFilter: %v. Err=%+v", queryFilter, err.Error())
			}
		}
	}
	if !useQueryVolumeAsync || queryAsyncNotSupported {
		queryResult, err = m.QueryVolume(ctx, queryFilter)
		if err != nil {
			return nil, logger.LogNewErrorCodef(log, codes.Internal,
				"QueryVolume failed for queryFilter: %+v. Err=%+v", queryFilter, err.Error())
		}
	}
	return queryResult, nil
}

// QueryAllVolumeUtil helps to invoke query volume API based on the feature state set for using query async volume.
// If useQueryVolumeAsync is set to true, the function invokes CNS QueryVolumeAsync, otherwise it invokes synchronous QueryAllVolume API
// The function also take volume manager instance, query filters, query selection as params
// Returns queryResult when query volume succeeds, otherwise returns appropriate errors
func QueryAllVolumeUtil(ctx context.Context, m cnsvolume.Manager, queryFilter cnstypes.CnsQueryFilter, querySelection cnstypes.CnsQuerySelection, useQueryVolumeAsync bool) (*cnstypes.CnsQueryResult, error) {
	log := logger.GetLogger(ctx)
	var queryAsyncNotSupported bool
	var queryResult *cnstypes.CnsQueryResult
	var err error
	if useQueryVolumeAsync {
		// AsyncQueryVolume feature switch is disabled
		queryResult, err = m.QueryVolumeAsync(ctx, queryFilter, querySelection)
		if err != nil {
			if err.Error() == cnsvsphere.ErrNotSupported.Error() {
				log.Warn("QueryVolumeAsync is not supported. Invoking QueryAllVolume API")
				queryAsyncNotSupported = true
			} else { // Return for any other failures
				return nil, logger.LogNewErrorCodef(log, codes.Internal,
					"QueryVolumeAsync failed for queryFilter: %v. Err=%+v", queryFilter, err.Error())
			}
		}
	}
	if !useQueryVolumeAsync || queryAsyncNotSupported {
		queryResult, err = m.QueryAllVolume(ctx, queryFilter, querySelection)
		if err != nil {
			return nil, logger.LogNewErrorCodef(log, codes.Internal,
				"QueryAllVolume failed for queryFilter: %+v. Err=%+v", queryFilter, err.Error())
		}
	}
	return queryResult, nil
}
