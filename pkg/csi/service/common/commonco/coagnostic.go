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
	"errors"

	cnstypes "github.com/vmware/govmomi/cns/types"
	"sigs.k8s.io/vsphere-csi-driver/pkg/csi/service/common"
	"sigs.k8s.io/vsphere-csi-driver/pkg/csi/service/common/commonco/k8sorchestrator"
	"sigs.k8s.io/vsphere-csi-driver/pkg/csi/service/logger"
)

// COCommonInterface provides functionality to define
// container orchestrator related implementation to read resources/objects
type COCommonInterface interface {
	// Check if feature state switch is enabled for the given feature indicated by featureName
	IsFSSEnabled(ctx context.Context, featureName string) bool
}

// GetContainerOrchestratorInterface returns orchestrator object
// for a given container orchestrator type
func GetContainerOrchestratorInterface(ctx context.Context, orchestratorType int, clusterFlavour cnstypes.CnsClusterFlavor) (COCommonInterface, error) {
	log := logger.GetLogger(ctx)
	switch orchestratorType {
	case common.Kubernetes:
		k8sorchestratorInstance, err := k8sorchestrator.Newk8sOrchestrator(ctx, clusterFlavour)
		if err != nil {
			log.Errorf("Creating k8sorchestratorInstance failed. Err: %v", err)
			return nil, err
		}
		return k8sorchestratorInstance, nil
	default:
		//if type is invalid, return an error
		return nil, errors.New("Invalid orchestrator Type")
	}
}
