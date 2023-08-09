/*
Copyright 2023 The Kubernetes Authors.

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

package vsphere

import (
	"context"

	"github.com/vmware/govmomi/object"
	"github.com/vmware/govmomi/vim25/mo"

	"sigs.k8s.io/vsphere-csi-driver/v3/pkg/csi/service/logger"
)

// ClusterComputeResource holds details of a cluster instance.
type ClusterComputeResource struct {
	// ClusterComputeResource represents a vSphere cluster.
	*object.ClusterComputeResource
	// VirtualCenterHost denotes the virtual center host address.
	VirtualCenterHost string
}

// GetHosts fetches the hosts under the ClusterComputeResource.
func (ccr *ClusterComputeResource) GetHosts(ctx context.Context) ([]*HostSystem, error) {
	log := logger.GetLogger(ctx)
	cluster := mo.ClusterComputeResource{}
	err := ccr.Properties(ctx, ccr.Reference(), []string{"host"}, &cluster)
	if err != nil {
		return nil, logger.LogNewErrorf(log,
			"failed to retrieve host property for cluster %+v", ccr.Reference())
	}
	var hostList []*HostSystem
	for _, host := range cluster.Host {
		hostList = append(hostList, &HostSystem{HostSystem: object.NewHostSystem(ccr.Client(), host)})
	}
	return hostList, nil
}
