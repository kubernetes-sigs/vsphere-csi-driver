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

package volume

import (
	"context"
	"testing"

	cnstypes "github.com/vmware/govmomi/cns/types"
	"github.com/vmware/govmomi/vim25/types"
)

// TestGetCnsVolumeInfoFromTaskResultHostCapture verifies that the host CNS reports in the winning
// placement result (used for host-local volumes) is surfaced on the returned CnsVolumeInfo. The
// placement result also carries clusters so the datastore-lookup branch (which needs a vCenter
// connection) is not exercised.
func TestGetCnsVolumeInfoFromTaskResultHostCapture(t *testing.T) {
	ctx := context.Background()
	hostRef := types.ManagedObjectReference{Type: "HostSystem", Value: "host-42"}
	clusterRef := types.ManagedObjectReference{Type: "ClusterComputeResource", Value: "domain-c1"}
	volumeID := cnstypes.CnsVolumeId{Id: "vol-1"}

	taskResult := &cnstypes.CnsVolumeCreateResult{
		PlacementResults: []cnstypes.CnsPlacementResult{
			{
				Clusters: []types.ManagedObjectReference{clusterRef},
				Host:     &hostRef,
			},
		},
	}

	info, faultType, err := getCnsVolumeInfoFromTaskResult(ctx, nil, "vol", volumeID, taskResult)
	if err != nil {
		t.Fatalf("unexpected error: %v (faultType %q)", err, faultType)
	}
	if info.Host == nil {
		t.Fatalf("expected placement host to be captured, got nil")
	}
	if info.Host.Value != "host-42" {
		t.Errorf("expected host MoRef value %q, got %q", "host-42", info.Host.Value)
	}
}

// TestGetCnsVolumeInfoFromTaskResultNoHost verifies that CnsVolumeInfo.Host stays nil when CNS does
// not report a host in the placement result (the non-host-local case).
func TestGetCnsVolumeInfoFromTaskResultNoHost(t *testing.T) {
	ctx := context.Background()
	clusterRef := types.ManagedObjectReference{Type: "ClusterComputeResource", Value: "domain-c1"}
	volumeID := cnstypes.CnsVolumeId{Id: "vol-2"}

	taskResult := &cnstypes.CnsVolumeCreateResult{
		PlacementResults: []cnstypes.CnsPlacementResult{
			{
				Clusters: []types.ManagedObjectReference{clusterRef},
			},
		},
	}

	info, faultType, err := getCnsVolumeInfoFromTaskResult(ctx, nil, "vol", volumeID, taskResult)
	if err != nil {
		t.Fatalf("unexpected error: %v (faultType %q)", err, faultType)
	}
	if info.Host != nil {
		t.Errorf("expected nil host, got %+v", info.Host)
	}
}
