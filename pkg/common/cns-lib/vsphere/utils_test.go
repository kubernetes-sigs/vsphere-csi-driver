package vsphere

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/vmware/govmomi/object"
	"github.com/vmware/govmomi/vim25/soap"
	"github.com/vmware/govmomi/vim25/types"
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
