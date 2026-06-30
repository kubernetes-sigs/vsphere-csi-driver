/*
Copyright 2026 The Kubernetes Authors.

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

package cnsfilevolumeclient

import (
	"context"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	fakeclient "sigs.k8s.io/controller-runtime/pkg/client/fake"

	internalapis "sigs.k8s.io/vsphere-csi-driver/v3/pkg/internalapis"
	v1alpha1 "sigs.k8s.io/vsphere-csi-driver/v3/pkg/internalapis/cnsoperator/cnsfilevolumeclient/v1alpha1"
)

func newTestFileVolumeClient(objs ...v1alpha1.CnsFileVolumeClient) *fileVolumeClient {
	scheme := runtime.NewScheme()
	_ = internalapis.SchemeBuilder.AddToScheme(scheme)
	builder := fakeclient.NewClientBuilder().WithScheme(scheme)
	for i := range objs {
		builder = builder.WithObjects(&objs[i])
	}
	return &fileVolumeClient{
		client:     builder.Build(),
		volumeLock: &sync.Map{},
	}
}

// TestGetVMIPFromVMName_InstanceNotFound verifies that when the CnsFileVolumeClient CR does not
// exist, GetVMIPFromVMName returns an empty IP and no error, allowing callers to treat the
// absence as "nothing to clean up".
func TestGetVMIPFromVMName_InstanceNotFound(t *testing.T) {
	ctx := context.Background()
	f := newTestFileVolumeClient() // no CR created

	vmIP, count, err := f.GetVMIPFromVMName(ctx, "ns1/pvc1", "vm1")

	assert.NoError(t, err)
	assert.Empty(t, vmIP)
	assert.Zero(t, count)
}

// TestGetVMIPFromVMName_VMNotRegistered verifies that when the CnsFileVolumeClient CR exists but
// the requested VM name is not in the spec, GetVMIPFromVMName returns an empty IP and no error.
func TestGetVMIPFromVMName_VMNotRegistered(t *testing.T) {
	ctx := context.Background()
	cr := v1alpha1.CnsFileVolumeClient{
		ObjectMeta: metav1.ObjectMeta{Name: "pvc1", Namespace: "ns1"},
		Spec: v1alpha1.CnsFileVolumeClientSpec{
			ExternalIPtoClientVms: map[string][]string{
				"10.0.0.1": {"other-vm"},
			},
		},
	}
	f := newTestFileVolumeClient(cr)

	vmIP, count, err := f.GetVMIPFromVMName(ctx, "ns1/pvc1", "vm1")

	assert.NoError(t, err)
	assert.Empty(t, vmIP)
	assert.Zero(t, count)
}

// TestGetVMIPFromVMName_VMRegistered verifies that when the CnsFileVolumeClient CR exists and
// the requested VM name is present in the spec, GetVMIPFromVMName returns the correct IP and
// the number of VMs sharing that IP.
func TestGetVMIPFromVMName_VMRegistered(t *testing.T) {
	ctx := context.Background()
	cr := v1alpha1.CnsFileVolumeClient{
		ObjectMeta: metav1.ObjectMeta{Name: "pvc1", Namespace: "ns1"},
		Spec: v1alpha1.CnsFileVolumeClientSpec{
			ExternalIPtoClientVms: map[string][]string{
				"10.0.0.1": {"vm1", "vm2"},
			},
		},
	}
	f := newTestFileVolumeClient(cr)

	vmIP, count, err := f.GetVMIPFromVMName(ctx, "ns1/pvc1", "vm1")

	assert.NoError(t, err)
	assert.Equal(t, "10.0.0.1", vmIP)
	assert.Equal(t, 2, count)
}
