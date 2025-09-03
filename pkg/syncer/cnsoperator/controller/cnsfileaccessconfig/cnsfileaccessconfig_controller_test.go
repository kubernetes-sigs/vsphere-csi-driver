package cnsfileaccessconfig

import (
	"context"
	"testing"

	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	vmoperatorv1alpha4 "github.com/vmware-tanzu/vm-operator/api/v1alpha4"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

func TestValidateVmAndPvc(t *testing.T) {
	scheme := runtime.NewScheme()
	_ = v1.AddToScheme(scheme)
	_ = vmoperatorv1alpha4.AddToScheme(scheme)

	ctx := context.TODO()

	tests := []struct {
		name           string
		instanceLabels map[string]string
		vmLabels       map[string]string
		expectError    bool
		setupPVC       bool
	}{
		{
			name:           "no instance labels",
			instanceLabels: nil,
			expectError:    false,
		},
		{
			name: "labels but not devops user",
			instanceLabels: map[string]string{
				"random": "value",
			},
			expectError: false,
		},
		{
			name: "devops user with capv label on VM",
			instanceLabels: map[string]string{
				devopsUserLabelKey: "true",
			},
			vmLabels: map[string]string{
				capvVmLabelKey + "/cluster": "my-cluster",
			},
			expectError: true,
		},
		{
			name: "valid input - no errors",
			instanceLabels: map[string]string{
				devopsUserLabelKey: "true",
			},
			setupPVC:    true,
			expectError: false,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			var objects []runtime.Object

			vm := &vmoperatorv1alpha4.VirtualMachine{
				ObjectMeta: metav1.ObjectMeta{
					Name:   "my-vm",
					Labels: test.vmLabels,
				},
			}

			if test.setupPVC {
				pvc := &v1.PersistentVolumeClaim{
					ObjectMeta: metav1.ObjectMeta{
						Name:      "my-pvc",
						Namespace: "default",
					},
				}
				objects = append(objects, pvc)
			}

			client := fake.NewClientBuilder().WithScheme(scheme).WithRuntimeObjects(objects...).Build()

			err := validateVmAndPvc(ctx, test.instanceLabels, "my-instance", "my-pvc", "default", client, vm)

			if test.expectError && err == nil {
				t.Errorf("expected error but got nil")
			} else if !test.expectError && err != nil {
				t.Errorf("unexpected error: %v", err)
			}
		})
	}
}
