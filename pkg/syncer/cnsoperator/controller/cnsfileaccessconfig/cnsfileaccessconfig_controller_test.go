package cnsfileaccessconfig

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	vmoperatortypes "github.com/vmware-tanzu/vm-operator/api/v1alpha5"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

func TestValidateVmAndPvc_NoLabels(t *testing.T) {
	ctx := context.Background()

	err := validateVmAndPvc(ctx, nil, "vm-1", "pvc-1", "ns-1", nil, &vmoperatortypes.VirtualMachine{})
	assert.NoError(t, err)
}

func TestValidateVmAndPvc_NotDevOpsUser(t *testing.T) {
	ctx := context.Background()

	labels := map[string]string{
		"other.label": "value",
	}

	err := validateVmAndPvc(ctx, labels, "vm-1", "pvc-1", "ns-1", nil, &vmoperatortypes.VirtualMachine{})
	assert.NoError(t, err)
}

func TestValidateVmAndPvc_DevOpsUser_ValidVM(t *testing.T) {
	ctx := context.Background()

	labels := map[string]string{
		devopsUserLabelKey: "true",
	}

	vm := &vmoperatortypes.VirtualMachine{
		ObjectMeta: metav1.ObjectMeta{
			Name:   "my-vm",
			Labels: map[string]string{"custom.label": "value"},
		},
	}

	err := validateVmAndPvc(ctx, labels, "vm-1", "pvc-1", "ns-1", nil, vm)
	assert.NoError(t, err)
}

func TestValidateVmAndPvc_DevOpsUser_InvalidVM(t *testing.T) {
	ctx := context.Background()

	labels := map[string]string{
		devopsUserLabelKey: "true",
	}

	vm := &vmoperatortypes.VirtualMachine{
		ObjectMeta: metav1.ObjectMeta{
			Name: "my-vm",
			Labels: map[string]string{
				capvVmLabelKey + "/machine": "value", // Contains capvVmLabelKey
			},
		},
	}

	err := validateVmAndPvc(ctx, labels, "vm-1", "pvc-1", "ns-1", nil, vm)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "Invalid combination")
	assert.Contains(t, err.Error(), "my-vm")
}
