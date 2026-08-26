package vsphere

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/vmware/govmomi/object"
	"github.com/vmware/govmomi/vim25/soap"
	"github.com/vmware/govmomi/vim25/types"
	"sigs.k8s.io/vsphere-csi-driver/v3/pkg/common/config"
	commontypes "sigs.k8s.io/vsphere-csi-driver/v3/pkg/common/types"
)

// vCenter host and thumbprint are protocol-compliant case-insensitive identifiers, so
// GetVirtualCenterConfigs must normalize them (host -> lowercase, thumbprint -> uppercase)
// regardless of the casing an operator used in vsphere.conf, to avoid case-mismatch failures
// in the map lookups and equality checks performed downstream on VirtualCenterConfig.Host.
func TestGetVirtualCenterConfigsNormalizesHostAndThumbprint(t *testing.T) {
	ctx := context.Background()

	mixedCaseHost := "VC.Example.COM"
	mixedCaseThumbprint := "aa:bb:cc:dd:11:22:33:44:55:66:77:88:99:00:aa:bb:cc:dd:ee:ff"

	cfg := &config.Config{}
	cfg.Global.QueryLimit = 100
	cfg.Global.ListVolumeThreshold = 100
	cfg.VirtualCenter = commontypes.NewCaseInsensitiveMap[*config.VirtualCenterConfig]()
	cfg.VirtualCenter.Set(mixedCaseHost, &config.VirtualCenterConfig{
		User:         "administrator@vsphere.local",
		Password:     "password",
		VCenterPort:  "443",
		InsecureFlag: true,
		Thumbprint:   mixedCaseThumbprint,
	})

	vcConfigs, err := GetVirtualCenterConfigs(ctx, cfg)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(vcConfigs) != 1 {
		t.Fatalf("expected 1 VirtualCenterConfig, got %d", len(vcConfigs))
	}

	assert.Equal(t, "vc.example.com", vcConfigs[0].Host,
		"vCenter host should be normalized to lowercase")
	assert.Equal(t, "AA:BB:CC:DD:11:22:33:44:55:66:77:88:99:00:AA:BB:CC:DD:EE:FF", vcConfigs[0].Thumbprint,
		"thumbprint should be normalized to uppercase to match govmomi's SHA1/SHA256 format")
}

// The Global.Thumbprint fallback path must be normalized the same way as the
// per-VirtualCenter thumbprint.
func TestGetVirtualCenterConfigsNormalizesGlobalThumbprintFallback(t *testing.T) {
	ctx := context.Background()

	cfg := &config.Config{}
	cfg.Global.QueryLimit = 100
	cfg.Global.ListVolumeThreshold = 100
	cfg.Global.Thumbprint = "aa:bb:cc:dd:11:22:33:44:55:66:77:88:99:00:aa:bb:cc:dd:ee:ff"
	cfg.VirtualCenter = commontypes.NewCaseInsensitiveMap[*config.VirtualCenterConfig]()
	cfg.VirtualCenter.Set("vc.example.com", &config.VirtualCenterConfig{
		User:         "administrator@vsphere.local",
		Password:     "password",
		VCenterPort:  "443",
		InsecureFlag: true,
	})

	vcConfigs, err := GetVirtualCenterConfigs(ctx, cfg)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(vcConfigs) != 1 {
		t.Fatalf("expected 1 VirtualCenterConfig, got %d", len(vcConfigs))
	}

	assert.Equal(t, "AA:BB:CC:DD:11:22:33:44:55:66:77:88:99:00:AA:BB:CC:DD:EE:FF", vcConfigs[0].Thumbprint)
}

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
