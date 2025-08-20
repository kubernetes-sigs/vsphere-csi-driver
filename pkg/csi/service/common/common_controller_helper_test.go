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

package common

import (
	"context"
	"fmt"
	"testing"

	"github.com/container-storage-interface/spec/lib/go/csi"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	cnstypes "github.com/vmware/govmomi/cns/types"
	"github.com/vmware/govmomi/object"
	vim25types "github.com/vmware/govmomi/vim25/types"
	"google.golang.org/grpc/codes"
	cnsvolume "sigs.k8s.io/vsphere-csi-driver/v3/pkg/common/cns-lib/volume"
	cnsvsphere "sigs.k8s.io/vsphere-csi-driver/v3/pkg/common/cns-lib/vsphere"
	"sigs.k8s.io/vsphere-csi-driver/v3/pkg/internalapis/cnsvolumeoperationrequest"
)

// TestUseVslmAPIsFuncForVC67Update3l tests UseVslmAPIs method for VC version 6.7 Update 3l
func TestUseVslmAPIsFuncForVC67Update3l(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Scenario 1: VC version in 6.7 Update 3l ==> UseVslmAPIs, should return true
	aboutInfo := vim25types.AboutInfo{
		Name:                  "VMware vCenter Server",
		FullName:              "VMware vCenter Server 6.7.0 build-17137327",
		Vendor:                "VMware, Inc.",
		Version:               "6.7.0",
		Build:                 "17137327",
		LocaleVersion:         "INTL",
		LocaleBuild:           "000",
		OsType:                "linux-x64",
		ProductLineId:         "vpx",
		ApiType:               "VirtualCenter",
		ApiVersion:            "6.7.3",
		InstanceUuid:          "4394cd59-cd63-43df-bdf5-8a2cee4fe055",
		LicenseProductName:    "VMware VirtualCenter Server",
		LicenseProductVersion: "6.0",
	}
	bUseVslm, err := UseVslmAPIs(ctx, aboutInfo)
	if err == nil {
		if !bUseVslm {
			t.Fatal("UseVslmAPIs returned false, expecting true")
		}
	}
}

// TestUseVslmAPIsFuncForVC67Update3a tests UseVslmAPIs method for 6.7 Update 3a
func TestUseVslmAPIsFuncForVC67Update3a(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	// Scenario 2: VC version in 6.7 Update 3a ==> UseVslmAPIs, should return false
	aboutInfo := vim25types.AboutInfo{
		Name:                  "VMware vCenter Server",
		FullName:              "VMware vCenter Server 6.7.0 build-17137327",
		Vendor:                "VMware, Inc.",
		Version:               "6.7.0",
		Build:                 "17037327",
		LocaleVersion:         "INTL",
		LocaleBuild:           "000",
		OsType:                "linux-x64",
		ProductLineId:         "vpx",
		ApiType:               "VirtualCenter",
		ApiVersion:            "6.7.3",
		InstanceUuid:          "4394cd59-cd63-43df-bdf5-8a2cee4fe055",
		LicenseProductName:    "VMware VirtualCenter Server",
		LicenseProductVersion: "6.0",
	}
	_, err := UseVslmAPIs(ctx, aboutInfo)
	if err == nil {
		t.Errorf("error expected but not received. aboutInfo: %+v", aboutInfo)
	}
	t.Logf("expected err received. err: %v", err)
}

// TestUseVslmAPIsFuncForVC6dot6 tests UseVslmAPIs method for VC version 6.6
func TestUseVslmAPIsFuncForVC6dot6(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	// Scenario 3: VC version in 6.6 ==> UseVslmAPIs, should return false
	aboutInfo := vim25types.AboutInfo{
		Name:                  "VMware vCenter Server",
		FullName:              "VMware vCenter Server 6.6.0 build-18037327",
		Vendor:                "VMware, Inc.",
		Version:               "6.6.0",
		Build:                 "18037327",
		LocaleVersion:         "INTL",
		LocaleBuild:           "000",
		OsType:                "linux-x64",
		ProductLineId:         "vpx",
		ApiType:               "VirtualCenter",
		ApiVersion:            "6.6.0",
		InstanceUuid:          "4394cd59-cd63-43df-bdf5-8a2cee4fe055",
		LicenseProductName:    "VMware VirtualCenter Server",
		LicenseProductVersion: "6.0",
	}
	bUseVslm, err := UseVslmAPIs(ctx, aboutInfo)
	if err == nil {
		if bUseVslm {
			t.Fatal("UseVslmAPIs returned true, expecting false")
		}
	} else {
		t.Fatal("Received error from UseVslmAPIs method")
	}
}

// TestUseVslmAPIsFuncForVC7 tests UseVslmAPIs method for VC version 7.0
func TestUseVslmAPIsFuncForVC7(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	// Scenario 4: VC version in 7.0 ==> UseVslmAPIs, should return true
	aboutInfo := vim25types.AboutInfo{
		Name:                  "VMware vCenter Server",
		FullName:              "VMware vCenter Server 7.0.0.0 build-19037327",
		Vendor:                "VMware, Inc.",
		Version:               "7.0.0.0",
		Build:                 "19037327",
		LocaleVersion:         "INTL",
		LocaleBuild:           "000",
		OsType:                "linux-x64",
		ProductLineId:         "vpx",
		ApiType:               "VirtualCenter",
		ApiVersion:            "7.0.0.0",
		InstanceUuid:          "4394cd59-cd63-43df-bdf5-8a2cee4fe055",
		LicenseProductName:    "VMware VirtualCenter Server",
		LicenseProductVersion: "7.0",
	}
	bUseVslm, err := UseVslmAPIs(ctx, aboutInfo)
	if err == nil {
		if !bUseVslm {
			t.Fatal("UseVslmAPIs returned false, expecting true")
		}
	} else {
		t.Fatal("Received error from UseVslmAPIs method")
	}
}

// TestUseVslmAPIsFuncForVC70u1 tests UseVslmAPIs method for VC version 70u1
func TestUseVslmAPIsFuncForVC70u1(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	// Scenario 5: VC version in 7.0 Update 1 ==> UseVslmAPIs, should return false
	// CSI migration is supported using CNS APIs
	aboutInfo := vim25types.AboutInfo{
		Name:                  "VMware vCenter Server",
		FullName:              "VMware vCenter Server 7.0.1.0 build-19237327",
		Vendor:                "VMware, Inc.",
		Version:               "7.0.1.0",
		Build:                 "19037327",
		LocaleVersion:         "INTL",
		LocaleBuild:           "000",
		OsType:                "linux-x64",
		ProductLineId:         "vpx",
		ApiType:               "VirtualCenter",
		ApiVersion:            "7.0.1.0",
		InstanceUuid:          "4394cd59-cd63-43df-bdf5-8a2cee4fe055",
		LicenseProductName:    "VMware VirtualCenter Server",
		LicenseProductVersion: "7.0",
	}
	bUseVslm, err := UseVslmAPIs(ctx, aboutInfo)
	if err == nil {
		if bUseVslm {
			t.Fatal("UseVslmAPIs returned true, expecting false")
		}
	} else {
		t.Fatal("Received error from UseVslmAPIs method")
	}
}

// TestCheckAPIForVC8 tests CheckAPI method for VC version 8.
func TestCheckAPIForVC8(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	vcVersion := "8.0.0.1.0"
	err := CheckAPI(ctx, vcVersion, MinSupportedVCenterMajor, MinSupportedVCenterMinor,
		MinSupportedVCenterPatch)
	if err != nil {
		t.Fatalf("CheckAPI method failing for VC %q", vcVersion)
	}
}

// TestCheckAPIForVC70u3 tests CheckAPI method for VC version 7.0U3.
func TestCheckAPIForVC70u3(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	vcVersion := "7.0.3.0"
	err := CheckAPI(ctx, vcVersion, MinSupportedVCenterMajor, MinSupportedVCenterMinor,
		MinSupportedVCenterPatch)
	if err != nil {
		t.Fatalf("CheckAPI method failing for VC %q", vcVersion)
	}
}

// TestValidateVolumeCapabilitiesCommon tests the ValidateVolumeCapabilitiesCommon function
func TestValidateVolumeCapabilitiesCommon(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	tests := []struct {
		name           string
		req            *csi.ValidateVolumeCapabilitiesRequest
		validationFunc func(context.Context, []*csi.VolumeCapability) error
		expectedError  bool
		expectedCode   codes.Code
	}{
		{
			name: "Valid request with valid capabilities",
			req: &csi.ValidateVolumeCapabilitiesRequest{
				VolumeId: "test-volume-id",
				VolumeCapabilities: []*csi.VolumeCapability{
					{
						AccessMode: &csi.VolumeCapability_AccessMode{
							Mode: csi.VolumeCapability_AccessMode_SINGLE_NODE_WRITER,
						},
						AccessType: &csi.VolumeCapability_Block{
							Block: &csi.VolumeCapability_BlockVolume{},
						},
					},
				},
			},
			validationFunc: func(ctx context.Context, volCaps []*csi.VolumeCapability) error {
				return nil // Valid capabilities
			},
			expectedError: false,
		},
		{
			name: "Valid request with IsValidVolumeCapabilities function",
			req: &csi.ValidateVolumeCapabilitiesRequest{
				VolumeId: "test-volume-id",
				VolumeCapabilities: []*csi.VolumeCapability{
					{
						AccessMode: &csi.VolumeCapability_AccessMode{
							Mode: csi.VolumeCapability_AccessMode_SINGLE_NODE_WRITER,
						},
						AccessType: &csi.VolumeCapability_Block{
							Block: &csi.VolumeCapability_BlockVolume{},
						},
					},
				},
			},
			validationFunc: IsValidVolumeCapabilities,
			expectedError:  false,
		},
		{
			name: "Empty volume ID",
			req: &csi.ValidateVolumeCapabilitiesRequest{
				VolumeId: "",
				VolumeCapabilities: []*csi.VolumeCapability{
					{
						AccessMode: &csi.VolumeCapability_AccessMode{
							Mode: csi.VolumeCapability_AccessMode_SINGLE_NODE_WRITER,
						},
						AccessType: &csi.VolumeCapability_Block{
							Block: &csi.VolumeCapability_BlockVolume{},
						},
					},
				},
			},
			validationFunc: func(ctx context.Context, volCaps []*csi.VolumeCapability) error {
				return nil
			},
			expectedError: true,
			expectedCode:  codes.InvalidArgument,
		},
		{
			name: "Validation function returns error",
			req: &csi.ValidateVolumeCapabilitiesRequest{
				VolumeId: "test-volume-id",
				VolumeCapabilities: []*csi.VolumeCapability{
					{
						AccessMode: &csi.VolumeCapability_AccessMode{
							Mode: csi.VolumeCapability_AccessMode_MULTI_NODE_MULTI_WRITER,
						},
						AccessType: &csi.VolumeCapability_Block{
							Block: &csi.VolumeCapability_BlockVolume{},
						},
					},
				},
			},
			validationFunc: func(ctx context.Context, volCaps []*csi.VolumeCapability) error {
				return fmt.Errorf("invalid capabilities")
			},
			expectedError: false, // Function should return success but with no confirmed capabilities
		},
		{
			name: "Invalid capabilities using IsValidVolumeCapabilities",
			req: &csi.ValidateVolumeCapabilitiesRequest{
				VolumeId: "test-volume-id",
				VolumeCapabilities: []*csi.VolumeCapability{
					{
						AccessMode: &csi.VolumeCapability_AccessMode{
							Mode: csi.VolumeCapability_AccessMode_MULTI_NODE_MULTI_WRITER,
						},
						AccessType: &csi.VolumeCapability_Block{
							Block: &csi.VolumeCapability_BlockVolume{},
						},
					},
				},
			},
			validationFunc: IsValidVolumeCapabilities,
			expectedError:  false, // Function should return success but with no confirmed capabilities
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			resp, err := ValidateVolumeCapabilitiesCommon(ctx, tt.req, tt.validationFunc)

			if tt.expectedError {
				require.Error(t, err)
				if tt.expectedCode != codes.OK {
					// Check if the error is a gRPC error with the expected code
					grpcErr, ok := err.(interface{ Code() codes.Code })
					if ok {
						assert.Equal(t, tt.expectedCode, grpcErr.Code())
					}
				}
			} else {
				require.NoError(t, err)
				require.NotNil(t, resp)

				// If validation function returns nil, we should have confirmed capabilities
				if tt.validationFunc(ctx, tt.req.VolumeCapabilities) == nil {
					require.NotNil(t, resp.Confirmed)
					assert.Equal(t, tt.req.VolumeCapabilities, resp.Confirmed.VolumeCapabilities)
				} else {
					// If validation function returns error, we should not have confirmed capabilities
					assert.Nil(t, resp.Confirmed)
				}
			}
		})
	}
}

// Tests ValidateVolumeCapabilitiesCommonWithVolumeCheck function
func TestValidateVolumeCapabilitiesCommonWithVolumeCheck(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Save original queryVolumeByIDInternal function
	originalQueryVolumeByID := queryVolumeByIDInternal
	defer func() {
		// Restore original function after test
		queryVolumeByIDInternal = originalQueryVolumeByID
	}()

	tests := []struct {
		name           string
		req            *csi.ValidateVolumeCapabilitiesRequest
		validationFunc func(context.Context, []*csi.VolumeCapability) error
		mockQueryFunc  func(ctx context.Context,
			volManager cnsvolume.Manager, volumeID string,
			querySelection *cnstypes.CnsQuerySelection) (*cnstypes.CnsVolume, error)
		expectedError bool
		expectedCode  codes.Code
	}{
		{
			name: "Valid request with existing volume and valid capabilities",
			req: &csi.ValidateVolumeCapabilitiesRequest{
				VolumeId: "existing-volume-id",
				VolumeCapabilities: []*csi.VolumeCapability{
					{
						AccessMode: &csi.VolumeCapability_AccessMode{
							Mode: csi.VolumeCapability_AccessMode_SINGLE_NODE_WRITER,
						},
						AccessType: &csi.VolumeCapability_Block{
							Block: &csi.VolumeCapability_BlockVolume{},
						},
					},
				},
			},
			validationFunc: func(ctx context.Context, volCaps []*csi.VolumeCapability) error {
				return nil // Valid capabilities
			},
			mockQueryFunc: func(ctx context.Context, volManager cnsvolume.Manager, volumeID string,
				querySelection *cnstypes.CnsQuerySelection) (*cnstypes.CnsVolume, error) {
				return &cnstypes.CnsVolume{
					VolumeId: cnstypes.CnsVolumeId{Id: "existing-volume-id"},
				}, nil
			},
			expectedError: false,
		},
		{
			name: "Empty volume ID",
			req: &csi.ValidateVolumeCapabilitiesRequest{
				VolumeId: "",
				VolumeCapabilities: []*csi.VolumeCapability{
					{
						AccessMode: &csi.VolumeCapability_AccessMode{
							Mode: csi.VolumeCapability_AccessMode_SINGLE_NODE_WRITER,
						},
						AccessType: &csi.VolumeCapability_Block{
							Block: &csi.VolumeCapability_BlockVolume{},
						},
					},
				},
			},
			validationFunc: func(ctx context.Context, volCaps []*csi.VolumeCapability) error {
				return nil
			},
			mockQueryFunc: nil, // Should not be called for empty volume ID
			expectedError: true,
			expectedCode:  codes.InvalidArgument,
		},
		{
			name: "Volume not found",
			req: &csi.ValidateVolumeCapabilitiesRequest{
				VolumeId: "non-existent-volume-id",
				VolumeCapabilities: []*csi.VolumeCapability{
					{
						AccessMode: &csi.VolumeCapability_AccessMode{
							Mode: csi.VolumeCapability_AccessMode_SINGLE_NODE_WRITER,
						},
						AccessType: &csi.VolumeCapability_Block{
							Block: &csi.VolumeCapability_BlockVolume{},
						},
					},
				},
			},
			validationFunc: func(ctx context.Context, volCaps []*csi.VolumeCapability) error {
				return nil
			},
			mockQueryFunc: func(ctx context.Context, volManager cnsvolume.Manager, volumeID string,
				querySelection *cnstypes.CnsQuerySelection) (*cnstypes.CnsVolume, error) {
				return nil, ErrNotFound
			},
			expectedError: true,
			expectedCode:  codes.NotFound,
		},
		{
			name: "Query volume error",
			req: &csi.ValidateVolumeCapabilitiesRequest{
				VolumeId: "error-volume-id",
				VolumeCapabilities: []*csi.VolumeCapability{
					{
						AccessMode: &csi.VolumeCapability_AccessMode{
							Mode: csi.VolumeCapability_AccessMode_SINGLE_NODE_WRITER,
						},
						AccessType: &csi.VolumeCapability_Block{
							Block: &csi.VolumeCapability_BlockVolume{},
						},
					},
				},
			},
			validationFunc: func(ctx context.Context, volCaps []*csi.VolumeCapability) error {
				return nil
			},
			mockQueryFunc: func(ctx context.Context, volManager cnsvolume.Manager, volumeID string,
				querySelection *cnstypes.CnsQuerySelection) (*cnstypes.CnsVolume, error) {
				return nil, fmt.Errorf("query error")
			},
			expectedError: true,
			expectedCode:  codes.Internal,
		},
		{
			name: "Validation function returns error",
			req: &csi.ValidateVolumeCapabilitiesRequest{
				VolumeId: "existing-volume-id",
				VolumeCapabilities: []*csi.VolumeCapability{
					{
						AccessMode: &csi.VolumeCapability_AccessMode{
							Mode: csi.VolumeCapability_AccessMode_MULTI_NODE_MULTI_WRITER,
						},
						AccessType: &csi.VolumeCapability_Block{
							Block: &csi.VolumeCapability_BlockVolume{},
						},
					},
				},
			},
			validationFunc: func(ctx context.Context, volCaps []*csi.VolumeCapability) error {
				return fmt.Errorf("invalid capabilities")
			},
			mockQueryFunc: func(ctx context.Context, volManager cnsvolume.Manager, volumeID string,
				querySelection *cnstypes.CnsQuerySelection) (*cnstypes.CnsVolume, error) {
				return &cnstypes.CnsVolume{
					VolumeId: cnstypes.CnsVolumeId{Id: "existing-volume-id"},
				}, nil
			},
			expectedError: false, // Function should return success but with no confirmed capabilities
		},
		{
			name: "Valid request with IsValidVolumeCapabilities function",
			req: &csi.ValidateVolumeCapabilitiesRequest{
				VolumeId: "existing-volume-id",
				VolumeCapabilities: []*csi.VolumeCapability{
					{
						AccessMode: &csi.VolumeCapability_AccessMode{
							Mode: csi.VolumeCapability_AccessMode_SINGLE_NODE_WRITER,
						},
						AccessType: &csi.VolumeCapability_Block{
							Block: &csi.VolumeCapability_BlockVolume{},
						},
					},
				},
			},
			validationFunc: IsValidVolumeCapabilities,
			mockQueryFunc: func(ctx context.Context, volManager cnsvolume.Manager,
				volumeID string, querySelection *cnstypes.CnsQuerySelection) (*cnstypes.CnsVolume, error) {
				return &cnstypes.CnsVolume{
					VolumeId: cnstypes.CnsVolumeId{Id: "existing-volume-id"},
				}, nil
			},
			expectedError: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Set up the mock function if provided
			if tt.mockQueryFunc != nil {
				queryVolumeByIDInternal = tt.mockQueryFunc
			}

			// Create a dummy volume manager (not used when mocking QueryVolumeByID)
			dummyVolumeManager := &MockVolumeManager{}

			resp, err := ValidateVolumeCapabilitiesCommonWithVolumeCheck(ctx,
				tt.req, tt.validationFunc, dummyVolumeManager)

			if tt.expectedError {
				require.Error(t, err)
				if tt.expectedCode != codes.OK {
					// Check if the error is a gRPC error with the expected code
					grpcErr, ok := err.(interface{ Code() codes.Code })
					if ok {
						assert.Equal(t, tt.expectedCode, grpcErr.Code())
					}
				}
			} else {
				require.NoError(t, err)
				require.NotNil(t, resp)

				// If validation function returns nil, we should have confirmed capabilities
				if tt.validationFunc(ctx, tt.req.VolumeCapabilities) == nil {
					require.NotNil(t, resp.Confirmed)
					assert.Equal(t, tt.req.VolumeCapabilities, resp.Confirmed.VolumeCapabilities)
				} else {
					// If validation function returns error, we should not have confirmed capabilities
					assert.Nil(t, resp.Confirmed)
				}
			}
		})
	}
}

// MockVolumeManager is a minimal mock implementation of cnsvolume.Manager for testing
type MockVolumeManager struct{}

// Implement all required methods from cnsvolume.Manager interface
func (m *MockVolumeManager) CreateVolume(ctx context.Context, spec *cnstypes.CnsVolumeCreateSpec,
	extraParams interface{}) (*cnsvolume.CnsVolumeInfo, string, error) {
	return nil, "", nil
}

func (m *MockVolumeManager) AttachVolume(ctx context.Context, vm *cnsvsphere.VirtualMachine, volumeID string,
	checkNVMeController bool) (string, string, error) {
	return "", "", nil
}

func (m *MockVolumeManager) DetachVolume(ctx context.Context, vm *cnsvsphere.VirtualMachine,
	volumeID string) (string, error) {
	return "", nil
}

func (m *MockVolumeManager) DeleteVolume(ctx context.Context, volumeID string, deleteDisk bool) (string, error) {
	return "", nil
}

func (m *MockVolumeManager) UpdateVolumeMetadata(ctx context.Context,
	spec *cnstypes.CnsVolumeMetadataUpdateSpec) error {
	return nil
}

func (m *MockVolumeManager) UpdateVolumeCrypto(ctx context.Context, spec *cnstypes.CnsVolumeCryptoUpdateSpec) error {
	return nil
}

func (m *MockVolumeManager) QueryVolumeInfo(ctx context.Context,
	volumeIDList []cnstypes.CnsVolumeId) (*cnstypes.CnsQueryVolumeInfoResult, error) {
	return nil, nil
}

func (m *MockVolumeManager) QueryAllVolume(ctx context.Context, queryFilter cnstypes.CnsQueryFilter,
	querySelection cnstypes.CnsQuerySelection) (*cnstypes.CnsQueryResult, error) {
	return &cnstypes.CnsQueryResult{}, nil
}

func (m *MockVolumeManager) QueryVolume(ctx context.Context, queryFilter cnstypes.CnsQueryFilter) (
	*cnstypes.CnsQueryResult, error) {
	return &cnstypes.CnsQueryResult{}, nil
}

func (m *MockVolumeManager) QueryVolumeAsync(ctx context.Context, queryFilter cnstypes.CnsQueryFilter,
	querySelection *cnstypes.CnsQuerySelection) (*cnstypes.CnsQueryResult, error) {
	return nil, nil
}

func (m *MockVolumeManager) RelocateVolume(ctx context.Context,
	relocateSpecList ...cnstypes.BaseCnsVolumeRelocateSpec) (*object.Task, error) {
	return nil, nil
}

func (m *MockVolumeManager) ExpandVolume(ctx context.Context, volumeID string, size int64,
	extraParams interface{}) (string, error) {
	return "", nil
}

func (m *MockVolumeManager) ResetManager(ctx context.Context, vcenter *cnsvsphere.VirtualCenter) error {
	return nil
}

func (m *MockVolumeManager) ConfigureVolumeACLs(ctx context.Context, spec cnstypes.CnsVolumeACLConfigureSpec) error {
	return nil
}

func (m *MockVolumeManager) RegisterDisk(ctx context.Context, path string, name string) (string, error) {
	return "", nil
}

func (m *MockVolumeManager) RetrieveVStorageObject(ctx context.Context,
	volumeID string) (*vim25types.VStorageObject, error) {
	return nil, nil
}

func (m *MockVolumeManager) ProtectVolumeFromVMDeletion(ctx context.Context, volumeID string) error {
	return nil
}

func (m *MockVolumeManager) CreateSnapshot(ctx context.Context, volumeID string,
	desc string, extraParams interface{}) (*cnsvolume.CnsSnapshotInfo, error) {
	return nil, nil
}

func (m *MockVolumeManager) DeleteSnapshot(ctx context.Context, volumeID string, snapshotID string,
	extraParams interface{}) (*cnsvolume.CnsSnapshotInfo, error) {
	return nil, nil
}

func (m *MockVolumeManager) QuerySnapshots(ctx context.Context,
	snapshotQueryFilter cnstypes.CnsSnapshotQueryFilter) (
	*cnstypes.CnsSnapshotQueryResult, error) {
	return nil, nil
}

func (m *MockVolumeManager) MonitorCreateVolumeTask(ctx context.Context,
	volumeOperationDetails **cnsvolumeoperationrequest.VolumeOperationRequestDetails, task *object.Task,
	volNameFromInputSpec, clusterID string) (*cnsvolume.CnsVolumeInfo,
	string, error) {
	return nil, "", nil
}

func (m *MockVolumeManager) GetOperationStore() cnsvolumeoperationrequest.VolumeOperationRequest {
	return nil
}

func (m *MockVolumeManager) IsListViewReady() bool {
	return true
}

func (m *MockVolumeManager) SetListViewNotReady(ctx context.Context) {
}

func (m *MockVolumeManager) BatchAttachVolumes(ctx context.Context,
	vm *cnsvsphere.VirtualMachine,
	batchAttachRequest []cnsvolume.BatchAttachRequest) ([]cnsvolume.BatchAttachResult, string, error) {
	return nil, "", nil
}
