package vsphere

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/vmware/govmomi/object"
	"github.com/vmware/govmomi/vim25/soap"
	"github.com/vmware/govmomi/vim25/types"

	"sigs.k8s.io/vsphere-csi-driver/v3/pkg/common/config"
)

func TestFilterSuspendedDatastoresWhenDatastoreIsSuspended(t *testing.T) {

	customValue := []types.CustomFieldValue{
		{Key: 101},
	}
	CustomFieldStringValue := []types.CustomFieldStringValue{
		{Value: cnsMgrDatastoreSuspended, CustomFieldValue: customValue[0]},
	}
	customValue2 := (types.CustomFieldStringValue)(CustomFieldStringValue[0])
	baseCustomFieldValue := (types.BaseCustomFieldValue)(&customValue2)

	datastoreMoref := types.ManagedObjectReference{Type: "datastore", Value: "datastore-1"}
	datastore := &Datastore{Datastore: object.NewDatastore(nil, datastoreMoref)}
	dsInfo := []*DatastoreInfo{
		{
			Datastore: datastore,
			Info: &types.DatastoreInfo{
				Name: "test-ds",
			},
			CustomValues: []types.BaseCustomFieldValue{baseCustomFieldValue},
		},
	}

	outputDsInfo, err := FilterSuspendedDatastores(context.TODO(), dsInfo)
	assert.NotNil(t, err)
	assert.Equal(t, 0, len(outputDsInfo))
}
func TestFilterSuspendedDatastoresWhenDatastoreIsNotSuspended(t *testing.T) {

	customValue := []types.CustomFieldValue{
		{Key: 101},
	}
	CustomFieldStringValue := []types.CustomFieldStringValue{
		{Value: "randomValue", CustomFieldValue: customValue[0]},
	}
	customValue2 := (types.CustomFieldStringValue)(CustomFieldStringValue[0])
	baseCustomFieldValue := (types.BaseCustomFieldValue)(&customValue2)

	dsInfo := []*DatastoreInfo{
		{
			Info: &types.DatastoreInfo{
				Name: "test-ds",
			},
			CustomValues: []types.BaseCustomFieldValue{baseCustomFieldValue},
		},
	}

	outputDsInfo, err := FilterSuspendedDatastores(context.TODO(), dsInfo)
	assert.Nil(t, err)
	assert.Equal(t, 1, len(outputDsInfo))

}

func TestIsInvalidLoginError(t *testing.T) {
	ctx := context.Background()

	t.Run("WhenNilErr", func(tt *testing.T) {
		// Setup - empty error
		var err error

		// Execute
		result := IsInvalidLoginError(ctx, err)

		// Verify
		assert.False(tt, result)
	})

	t.Run("WhenSoapFaultWithInvalidLogin", func(tt *testing.T) {
		// Setup - soap.soapFaultError containing InvalidLogin
		// This mimics the actual error type returned by govmomi from vCenter
		fault := &soap.Fault{
			Code:   "ServerFaultCode",
			String: "Cannot complete login due to an incorrect user name or password",
			Detail: struct {
				Fault types.AnyType "xml:\",any,typeattr\""
			}{
				Fault: &types.InvalidLogin{
					VimFault: types.VimFault{
						MethodFault: types.MethodFault{
							FaultCause: &types.LocalizedMethodFault{
								Fault:            nil,
								LocalizedMessage: "Cannot complete login due to an incorrect user name or password",
							},
							FaultMessage: []types.LocalizableMessage{},
						},
					},
				},
			},
		}
		soapFault := soap.WrapSoapFault(fault)

		// Execute
		result := IsInvalidLoginError(ctx, soapFault)

		// Verify
		assert.True(tt, result)
	})

	t.Run("WhenSoapFaultWithDifferentVimFault", func(tt *testing.T) {
		// Setup - SoapFault with a different VimFault type that is not InvalidLogin
		fault := &soap.Fault{
			Code:   "ServerFaultCode",
			String: "Invalid locale",
			Detail: struct {
				Fault types.AnyType "xml:\",any,typeattr\""
			}{
				Fault: &types.InvalidLocale{
					VimFault: types.VimFault{
						MethodFault: types.MethodFault{
							FaultCause: &types.LocalizedMethodFault{
								Fault:            nil,
								LocalizedMessage: "invalid locale",
							},
							FaultMessage: []types.LocalizableMessage{},
						},
					},
				},
			},
		}
		soapFault := soap.WrapSoapFault(fault)

		// Execute
		result := IsInvalidLoginError(ctx, soapFault)

		// Verify
		assert.False(tt, result)
	})

	t.Run("WhenErrorMessageContainsInvalidLoginText", func(tt *testing.T) {
		// Setup - error message contains InvalidLogin text (fallback check)
		err := errors.New("ServerFaultCode: Cannot complete login due to an incorrect user name or password")

		// Execute
		result := IsInvalidLoginError(ctx, err)

		// Verify
		assert.True(tt, result)
	})

	t.Run("WhenGenericError", func(tt *testing.T) {
		// Setup - any other error that is not InvalidLogin
		err := errors.New("some random connection error")

		// Execute
		result := IsInvalidLoginError(ctx, err)

		// Verify
		assert.False(tt, result)
	})
}

// TestGetVirtualCenterConfigDatacenterPathNormalization verifies that datacenter entries
// in the config are normalised correctly:
//   - Inventory paths (starting with "/") are preserved as-is.
//   - Bare MoRef values (e.g. "datacenter-3") are preserved as-is.
//   - Entries with the "Datacenter:" type prefix are stripped to a bare MoRef value,
//     enabling users to copy the identifier verbatim from the vSphere API response while
//     still producing a value that is resilient to datacenter renames.
//   - Leading/trailing whitespace is trimmed before prefix stripping.
func TestGetVirtualCenterConfigDatacenterPathNormalization(t *testing.T) {
	ctx := context.Background()

	tests := []struct {
		name        string
		datacenters string
		want        []string
	}{
		{
			name:        "inventory path is preserved",
			datacenters: "/DC1",
			want:        []string{"/DC1"},
		},
		{
			name:        "bare MoRef value is preserved",
			datacenters: "datacenter-3",
			want:        []string{"datacenter-3"},
		},
		{
			name:        "Datacenter: prefix is stripped to bare MoRef",
			datacenters: "Datacenter:datacenter-3",
			want:        []string{"datacenter-3"},
		},
		{
			name:        "whitespace is trimmed before prefix stripping",
			datacenters: "  Datacenter:datacenter-3  ",
			want:        []string{"datacenter-3"},
		},
		{
			name:        "multiple mixed entries are each normalised",
			datacenters: "/DC1, Datacenter:datacenter-3, datacenter-5",
			want:        []string{"/DC1", "datacenter-3", "datacenter-5"},
		},
		{
			name:        "empty datacenters field produces nil DatacenterPaths",
			datacenters: "",
			want:        nil,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			cfg := &config.Config{
				VirtualCenter: map[string]*config.VirtualCenterConfig{
					"test-vc": {
						VCenterPort: "443",
						Datacenters: tc.datacenters,
					},
				},
			}
			vcConfig, err := GetVirtualCenterConfig(ctx, cfg)
			require.NoError(t, err)
			assert.Equal(t, tc.want, vcConfig.DatacenterPaths)
		})
	}
}

// TestGetVirtualCenterConfigsDatacenterPathNormalization is the same coverage for
// GetVirtualCenterConfigs (the multi-VC variant).
func TestGetVirtualCenterConfigsDatacenterPathNormalization(t *testing.T) {
	ctx := context.Background()

	tests := []struct {
		name        string
		datacenters string
		want        []string
	}{
		{
			name:        "inventory path is preserved",
			datacenters: "/DC1",
			want:        []string{"/DC1"},
		},
		{
			name:        "Datacenter: prefix is stripped to bare MoRef",
			datacenters: "Datacenter:datacenter-3",
			want:        []string{"datacenter-3"},
		},
		{
			name:        "multiple mixed entries are each normalised",
			datacenters: "Datacenter:datacenter-3, /DC2",
			want:        []string{"datacenter-3", "/DC2"},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			cfg := &config.Config{
				VirtualCenter: map[string]*config.VirtualCenterConfig{
					"test-vc": {
						VCenterPort: "443",
						Datacenters: tc.datacenters,
					},
				},
			}
			vcConfigs, err := GetVirtualCenterConfigs(ctx, cfg)
			require.NoError(t, err)
			require.Len(t, vcConfigs, 1)
			assert.Equal(t, tc.want, vcConfigs[0].DatacenterPaths)
		})
	}
}
