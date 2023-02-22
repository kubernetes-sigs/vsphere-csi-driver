/*
Copyright 2020 The Kubernetes Authors.

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

package commonco

import (
	"context"
	"fmt"

	cnstypes "github.com/vmware/govmomi/cns/types"
	storagev1 "k8s.io/api/storage/v1"

	cnsvolume "sigs.k8s.io/vsphere-csi-driver/v3/pkg/common/cns-lib/volume"

	"sigs.k8s.io/vsphere-csi-driver/v3/pkg/csi/service/common"
	"sigs.k8s.io/vsphere-csi-driver/v3/pkg/csi/service/common/commonco/k8sorchestrator"
	"sigs.k8s.io/vsphere-csi-driver/v3/pkg/csi/service/common/commonco/types"
	"sigs.k8s.io/vsphere-csi-driver/v3/pkg/csi/service/logger"
)

// ContainerOrchestratorUtility represents the singleton instance of
// container orchestrator interface.
var ContainerOrchestratorUtility COCommonInterface

// COCommonInterface provides functionality to define container orchestrator
// related implementation to read resources/objects.
type COCommonInterface interface {
	// Check if feature state switch is enabled for the given feature indicated
	// by featureName.
	IsFSSEnabled(ctx context.Context, featureName string) bool
	// Check if the passed volume can be fake attached.
	IsFakeAttachAllowed(ctx context.Context, volumeID string, volumeManager cnsvolume.Manager) (bool, error)
	// Mark the volume as fake attached.
	MarkFakeAttached(ctx context.Context, volumeID string) error
	// Check if the volume was fake attached, and unmark it as not fake attached.
	ClearFakeAttached(ctx context.Context, volumeID string) error
	// InitTopologyServiceInController initializes the necessary resources
	// required for topology related functionality in the controller.
	InitTopologyServiceInController(ctx context.Context) (types.ControllerTopologyService, error)
	// InitTopologyServiceInNode initializes the necessary resources
	// required for topology related functionality in the nodes.
	InitTopologyServiceInNode(ctx context.Context) (types.NodeTopologyService, error)
	// GetNodesForVolumes returns a map of volumeID to list of node names
	GetNodesForVolumes(ctx context.Context, volumeIds []string) map[string][]string
	// GetNodeIDtoNameMap returns a map of node ID  to node names
	GetNodeIDtoNameMap(ctx context.Context) map[string]string
	// GetFakeAttachedVolumes returns a map of volumeIDs to a bool, which is set
	// to true if volumeID key is fake attached else false
	GetFakeAttachedVolumes(ctx context.Context, volumeIDs []string) map[string]bool
	//GetVolumeAttachment is used to fetch the VA object from the cluster.
	GetVolumeAttachment(ctx context.Context, volumeId string, nodeName string) (*storagev1.VolumeAttachment, error)
	// GetAllVolumes returns list of volumes in a bound state
	GetAllVolumes() []string
	// GetAllK8sVolumes returns list of volumes in a bound state, in the K8s cluster
	// list Includes Migrated vSphere Volumes VMDK Paths and CSI Volume IDs
	GetAllK8sVolumes() []string
	// AnnotateVolumeSnapshot annotates the volumesnapshot CR in k8s cluster with the snapshot-id and fcd-id
	AnnotateVolumeSnapshot(ctx context.Context, volumeSnapshotName string,
		volumeSnapshotNamespace string, annotations map[string]string) (bool, error)
	// GetConfigMap checks if ConfigMap with given name exists in the given namespace.
	// If it exists, this function returns ConfigMap data, otherwise returns error.
	GetConfigMap(ctx context.Context, name string, namespace string) (map[string]string, error)
	// CreateConfigMap creates the ConfigMap with given name, namespace, data and immutable
	// parameter values.
	CreateConfigMap(ctx context.Context, name string, namespace string, data map[string]string,
		isImmutable bool) error
	// GetCSINodeTopologyInstancesList lists CSINodeTopology instances for a given cluster.
	GetCSINodeTopologyInstancesList() []interface{}
	// GetCSINodeTopologyInstanceByName fetches the CSINodeTopology instance for a given node name in the cluster.
	GetCSINodeTopologyInstanceByName(nodeName string) (item interface{}, exists bool, err error)
}

// GetContainerOrchestratorInterface returns orchestrator object for a given
// container orchestrator type.
func GetContainerOrchestratorInterface(ctx context.Context, orchestratorType int,
	clusterFlavor cnstypes.CnsClusterFlavor, params interface{}) (COCommonInterface, error) {
	log := logger.GetLogger(ctx)
	switch orchestratorType {
	case common.Kubernetes:
		k8sOrchestratorInstance, err := k8sorchestrator.Newk8sOrchestrator(ctx, clusterFlavor, params)
		if err != nil {
			log.Errorf("creating k8sOrchestratorInstance failed. Err: %v", err)
			return nil, err
		}
		return k8sOrchestratorInstance, nil
	default:
		// If type is invalid, return an error.
		return nil, fmt.Errorf("invalid orchestrator type")
	}
}
