package vsphere

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/vmware/govmomi/object"
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
