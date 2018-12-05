/*
Copyright 2018 The Kubernetes Authors.

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

package fcd

import (
	"strconv"
	"time"

	"golang.org/x/net/context"

	"github.com/container-storage-interface/spec/lib/go/csi"
	log "github.com/sirupsen/logrus"
	"github.com/vmware/govmomi/units"
	clientset "k8s.io/client-go/kubernetes"

	vcfg "k8s.io/cloud-provider-vsphere/pkg/common/config"
	cm "k8s.io/cloud-provider-vsphere/pkg/common/connectionmanager"
	k8s "k8s.io/cloud-provider-vsphere/pkg/common/kubernetes"
	"k8s.io/cloud-provider-vsphere/pkg/common/vclib"
)

type Controller interface {
	csi.ControllerServer
}

type controller struct {
	client    *clientset.Interface
	cfg       *vcfg.Config
	connMgr   *cm.ConnectionManager
	informMgr *k8s.InformerManager
}

func noResyncPeriodFunc() time.Duration {
	return 0
}

// New creates a FCD controller
func New(config *vcfg.Config) Controller {
	client, err := k8s.NewClient(config.Global.ServiceAccount)
	if err != nil {
		log.Fatalln("Creating Kubernetes client failed. Err:", err)
	}

	informMgr := k8s.NewInformer(&client)
	connMgr := cm.NewConnectionManager(config, informMgr.GetSecretListener())
	informMgr.Listen()

	return &controller{
		client:    &client,
		cfg:       config,
		connMgr:   connMgr,
		informMgr: informMgr,
	}
}

func (c *controller) CreateVolume(
	ctx context.Context,
	req *csi.CreateVolumeRequest) (
	*csi.CreateVolumeResponse, error) {

	return nil, nil
}

func (c *controller) DeleteVolume(
	ctx context.Context,
	req *csi.DeleteVolumeRequest) (
	*csi.DeleteVolumeResponse, error) {

	return nil, nil
}

func (c *controller) ControllerPublishVolume(
	ctx context.Context,
	req *csi.ControllerPublishVolumeRequest) (
	*csi.ControllerPublishVolumeResponse, error) {

	return nil, nil
}

func (c *controller) ControllerUnpublishVolume(
	ctx context.Context,
	req *csi.ControllerUnpublishVolumeRequest) (
	*csi.ControllerUnpublishVolumeResponse, error) {

	return nil, nil
}

func (c *controller) ValidateVolumeCapabilities(
	ctx context.Context,
	req *csi.ValidateVolumeCapabilitiesRequest) (
	*csi.ValidateVolumeCapabilitiesResponse, error) {

	return nil, nil
}

func (c *controller) ListVolumes(
	ctx context.Context,
	req *csi.ListVolumesRequest) (
	*csi.ListVolumesResponse, error) {

	//TODO: zones are currently unimplemented. supports single VC/DC only.
	zone := "TODO"
	discoveryInfo, err := c.connMgr.WhichVCandDCByZone(zone)
	if err != nil {
		log.Errorf("Failed to retrieve VC/DC based on zone %s. Err: %v", zone, err)
		return nil, err
	}

	firstClassDisks, err := discoveryInfo.DataCenter.GetAllFirstClassDisks(ctx)
	if err != nil {
		log.Errorf("GetAllFirstClassDisks failed. Err: %v", err)
		return nil, err
	}

	total := len(firstClassDisks)

	start := 0
	if req.StartingToken != "" {
		start, err = strconv.Atoi(req.StartingToken)
		if err != nil {
			log.Errorf("Invalid starting token %s. Err: %v", req.StartingToken, err)
			return nil, ErrListInvalidNextToken
		}
	}

	stop := total
	if req.MaxEntries != 0 && stop > int(req.MaxEntries) {
		stop = start + int(req.MaxEntries)
	}

	log.Infof("Start: %d, End: %d, Total: %d", start, stop, total)

	resp := &csi.ListVolumesResponse{}

	subsetFirstClassDisks := firstClassDisks
	if stop >= total {
		subsetFirstClassDisks = firstClassDisks[start:]
	} else if stop < total {
		subsetFirstClassDisks = firstClassDisks[start:(stop - 1)]
	}

	for _, firstClassDisk := range subsetFirstClassDisks {
		attributes := make(map[string]string)
		attributes[AttributeFirstClassDiskType] = FirstClassDiskTypeString
		attributes[AttributeFirstClassDiskName] = firstClassDisk.Config.Name
		attributes[AttributeFirstClassDiskParentType] = string(firstClassDisk.ParentType)
		if firstClassDisk.ParentType == vclib.TypeDatastoreCluster {
			attributes[AttributeFirstClassDiskParentName] = firstClassDisk.StoragePodInfo.Summary.Name
			attributes[AttributeFirstClassDiskOwningDatastore] = firstClassDisk.DatastoreInfo.Info.Name
		} else {
			attributes[AttributeFirstClassDiskParentName] = firstClassDisk.DatastoreInfo.Info.Name
		}

		resp.Entries = append(resp.Entries, &csi.ListVolumesResponse_Entry{
			Volume: &csi.Volume{
				VolumeId:      firstClassDisk.Config.Id.Id,
				CapacityBytes: int64(units.FileSize(firstClassDisk.Config.CapacityInMB * MbInBytes)),
				VolumeContext: attributes,
				//TODO: ContentSource?
				//TODO: AccessibleTopology, needed when we support multiple VC/DC combos
			},
		})
	}

	if stop < total {
		resp.NextToken = strconv.Itoa(stop)
		log.Infoln("Next token is", resp.NextToken)
	}

	return resp, nil
}

func (c *controller) GetCapacity(
	ctx context.Context,
	req *csi.GetCapacityRequest) (
	*csi.GetCapacityResponse, error) {

	return nil, nil
}

func (c *controller) ControllerGetCapabilities(
	ctx context.Context,
	req *csi.ControllerGetCapabilitiesRequest) (
	*csi.ControllerGetCapabilitiesResponse, error) {

	return &csi.ControllerGetCapabilitiesResponse{
		Capabilities: []*csi.ControllerServiceCapability{
			&csi.ControllerServiceCapability{
				Type: &csi.ControllerServiceCapability_Rpc{
					Rpc: &csi.ControllerServiceCapability_RPC{
						Type: csi.ControllerServiceCapability_RPC_LIST_VOLUMES,
					},
				},
			},
		},
	}, nil
}

func (c *controller) CreateSnapshot(
	ctx context.Context,
	req *csi.CreateSnapshotRequest) (
	*csi.CreateSnapshotResponse, error) {

	return nil, nil
}

func (c *controller) DeleteSnapshot(
	ctx context.Context,
	req *csi.DeleteSnapshotRequest) (
	*csi.DeleteSnapshotResponse, error) {

	return nil, nil
}

func (c *controller) ListSnapshots(
	ctx context.Context,
	req *csi.ListSnapshotsRequest) (
	*csi.ListSnapshotsResponse, error) {

	return nil, nil
}
