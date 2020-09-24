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

package unittestcommon

import (
	"context"
	"fmt"
	"strconv"
	"sync"

	"sigs.k8s.io/vsphere-csi-driver/pkg/apis/migration"
	cnsvolume "sigs.k8s.io/vsphere-csi-driver/pkg/common/cns-lib/volume"
	cnsconfig "sigs.k8s.io/vsphere-csi-driver/pkg/common/config"
	"sigs.k8s.io/vsphere-csi-driver/pkg/csi/service/common"
	"sigs.k8s.io/vsphere-csi-driver/pkg/csi/service/common/commonco"
)

var mapVolumePathToID map[string]map[string]string

// GetFakeContainerOrchestratorInterface returns a dummy CO interface based on the CO type
func GetFakeContainerOrchestratorInterface(orchestratorType int) (commonco.COCommonInterface, error) {
	if orchestratorType == common.Kubernetes {
		fakeCO := &FakeK8SOrchestrator{
			featureStates: map[string]string{
				"volume-extend": "true",
				"volume-health": "true",
				"csi-migration": "true",
			},
		}
		return fakeCO, nil
	}
	return nil, fmt.Errorf("Unrecognized CO type")

}

// IsFSSEnabled returns the FSS values for a given feature
func (c *FakeK8SOrchestrator) IsFSSEnabled(ctx context.Context, featureName string) bool {
	var featureState bool
	var err error
	if flag, ok := c.featureStates[featureName]; ok {
		featureState, err = strconv.ParseBool(flag)
		if err != nil {
			return false
		}
		return featureState
	}
	return false
}

// GetFakeVolumeMigrationService returns the mocked VolumeMigrationService
func GetFakeVolumeMigrationService(ctx context.Context, volumeManager *cnsvolume.Manager, cnsConfig *cnsconfig.Config) (MockVolumeMigrationService, error) {
	// fakeVolumeMigrationInstance is a mocked instance of volumeMigration
	fakeVolumeMigrationInstance := &mockVolumeMigration{
		volumePathToVolumeID: sync.Map{},
		volumeManager:        volumeManager,
		cnsConfig:            cnsConfig,
	}
	// Storing a dummy volume migration instance in the mapping
	mapVolumePathToID = make(map[string]map[string]string)
	mapVolumePathToID = map[string]map[string]string{
		"dummy-vms-CR": {
			"VolumePath": "[vsanDatastore] 9c01ed5e/kubernetes-dynamic-pvc-d8698e54.vmdk",
			"VolumeID":   "191c6d51-ed59-4340-9841-638c09f642b7",
		},
	}
	return fakeVolumeMigrationInstance, nil
}

// GetVolumeID mocks the method with returns Volume Id for a given Volume Path
func (dummyInstance *mockVolumeMigration) GetVolumeID(ctx context.Context, volumeSpec *migration.VolumeSpec) (string, error) {
	return mapVolumePathToID["dummy-vms-CR"][volumeSpec.VolumePath], nil
}

// GetVolumePath mocks the method with returns Volume Path for a given Volume ID
func (dummyInstance *mockVolumeMigration) GetVolumePath(ctx context.Context, volumeID string) (string, error) {
	return mapVolumePathToID["dummy-vms-CR"]["VolumePath"], nil
}

// DeleteVolumeInfo mocks the method to delete mapping of volumePath to VolumeID for specified volumeID
func (dummyInstance *mockVolumeMigration) DeleteVolumeInfo(ctx context.Context, volumeID string) error {
	delete(mapVolumePathToID, "dummy-vms-CR")
	return nil
}
