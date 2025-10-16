/*
Copyright 2025 The Kubernetes Authors.

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

	cnstypes "github.com/vmware/govmomi/cns/types"
	"github.com/vmware/govmomi/object"
	vim25types "github.com/vmware/govmomi/vim25/types"
	cnsvsphere "sigs.k8s.io/vsphere-csi-driver/v3/pkg/common/cns-lib/vsphere"
	"sigs.k8s.io/vsphere-csi-driver/v3/pkg/internalapis/cnsvolumeoperationrequest"
)

type MockManager struct {
	// failRequest is used to simulate failure in the mock manager.
	failRequest bool
	// err is used to store the error that should be returned by the mock manager.
	err error
	// faultType is used to store the fault type that should be returned by the mock manager.
	faultType string
}

func NewMockManager(failReq bool, err error, faultType string) *MockManager {
	if !failReq {
		return &MockManager{}
	}

	return &MockManager{
		failRequest: failReq,
		err:         err,
		faultType:   faultType,
	}
}

func (m MockManager) CreateVolume(ctx context.Context, spec *cnstypes.CnsVolumeCreateSpec,
	extraParams interface{}) (*CnsVolumeInfo, string, error) {
	//TODO implement me
	panic("implement me")
}

func (m MockManager) AttachVolume(ctx context.Context, vm *cnsvsphere.VirtualMachine, volumeID string,
	checkNVMeController bool) (string, string, error) {
	//TODO implement me
	panic("implement me")
}

func (m MockManager) DetachVolume(ctx context.Context, vm *cnsvsphere.VirtualMachine,
	volumeID string) (string, error) {
	//TODO implement me
	panic("implement me")
}

func (m MockManager) DeleteVolume(ctx context.Context, volumeID string, deleteDisk bool) (string, error) {
	//TODO implement me
	panic("implement me")
}

func (m MockManager) UpdateVolumeMetadata(ctx context.Context, spec *cnstypes.CnsVolumeMetadataUpdateSpec) error {
	//TODO implement me
	panic("implement me")
}

func (m MockManager) UpdateVolumeCrypto(ctx context.Context, spec *cnstypes.CnsVolumeCryptoUpdateSpec) error {
	//TODO implement me
	panic("implement me")
}

func (m MockManager) QueryVolumeInfo(ctx context.Context,
	volumeIDList []cnstypes.CnsVolumeId) (*cnstypes.CnsQueryVolumeInfoResult, error) {
	//TODO implement me
	panic("implement me")
}

func (m MockManager) QueryAllVolume(ctx context.Context, queryFilter cnstypes.CnsQueryFilter,
	querySelection cnstypes.CnsQuerySelection) (*cnstypes.CnsQueryResult, error) {
	//TODO implement me
	panic("implement me")
}

func (m MockManager) QueryVolumeAsync(ctx context.Context, queryFilter cnstypes.CnsQueryFilter,
	querySelection *cnstypes.CnsQuerySelection) (*cnstypes.CnsQueryResult, error) {
	//TODO implement me
	panic("implement me")
}

func (m MockManager) QueryVolume(ctx context.Context,
	queryFilter cnstypes.CnsQueryFilter) (*cnstypes.CnsQueryResult, error) {
	//TODO implement me
	panic("implement me")
}

func (m MockManager) RelocateVolume(ctx context.Context,
	relocateSpecList ...cnstypes.BaseCnsVolumeRelocateSpec) (*object.Task, error) {
	//TODO implement me
	panic("implement me")
}

func (m MockManager) ExpandVolume(ctx context.Context, volumeID string, size int64,
	extraParams interface{}) (string, error) {
	//TODO implement me
	panic("implement me")
}

func (m MockManager) ResetManager(ctx context.Context, vcenter *cnsvsphere.VirtualCenter) error {
	//TODO implement me
	panic("implement me")
}

func (m MockManager) ConfigureVolumeACLs(ctx context.Context, spec cnstypes.CnsVolumeACLConfigureSpec) error {
	//TODO implement me
	panic("implement me")
}

func (m MockManager) RegisterDisk(ctx context.Context, path string, name string) (string, error) {
	//TODO implement me
	panic("implement me")
}

func (m MockManager) RetrieveVStorageObject(ctx context.Context, volumeID string) (*vim25types.VStorageObject, error) {
	//TODO implement me
	panic("implement me")
}

func (m MockManager) ProtectVolumeFromVMDeletion(ctx context.Context, volumeID string) error {
	//TODO implement me
	panic("implement me")
}

func (m MockManager) CreateSnapshot(ctx context.Context, volumeID string, desc string,
	extraParams interface{}) (*CnsSnapshotInfo, error) {
	//TODO implement me
	panic("implement me")
}

func (m MockManager) DeleteSnapshot(ctx context.Context, volumeID string, snapshotID string,
	extraParams interface{}) (*CnsSnapshotInfo, error) {
	//TODO implement me
	panic("implement me")
}

func (m MockManager) QuerySnapshots(ctx context.Context,
	snapshotQueryFilter cnstypes.CnsSnapshotQueryFilter) (*cnstypes.CnsSnapshotQueryResult, error) {
	//TODO implement me
	panic("implement me")
}

func (m MockManager) MonitorCreateVolumeTask(ctx context.Context,
	details **cnsvolumeoperationrequest.VolumeOperationRequestDetails,
	task *object.Task, volNameFromInputSpec, clusterID string) (*CnsVolumeInfo, string, error) {
	//TODO implement me
	panic("implement me")
}

func (m MockManager) GetOperationStore() cnsvolumeoperationrequest.VolumeOperationRequest {
	//TODO implement me
	panic("implement me")
}

func (m MockManager) IsListViewReady() bool {
	//TODO implement me
	panic("implement me")
}

func (m MockManager) SetListViewNotReady(ctx context.Context) {
	//TODO implement me
	panic("implement me")
}

func (m MockManager) BatchAttachVolumes(ctx context.Context, vm *cnsvsphere.VirtualMachine,
	batchAttachRequest []BatchAttachRequest) ([]BatchAttachResult, string, error) {
	//TODO implement me
	panic("implement me")
}

func (m MockManager) UnregisterVolume(ctx context.Context, volumeID string, unregisterDisk bool) (string, error) {
	if m.failRequest {
		return "", m.err
	}

	return "", nil
}

func (m MockManager) SyncVolume(ctx context.Context, syncVolumeSpecs []cnstypes.CnsSyncVolumeSpec) (string, error) {
	//TODO implement me
	panic("implement me")
}
