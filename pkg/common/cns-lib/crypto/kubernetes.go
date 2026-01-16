/*
Copyright 2024 The Kubernetes Authors.

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

package crypto

import (
	byokv1 "github.com/vmware-tanzu/vm-operator/external/byok/api/v1alpha1"
	"k8s.io/apimachinery/pkg/runtime"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	cnsoperatorapis "sigs.k8s.io/vsphere-csi-driver/v3/pkg/apis/cnsoperator"
)

// NewK8sScheme creates a Kubernetes runtime schema for interacting with EncryptionClass entities
// and CNS operator resources.
func NewK8sScheme() (*runtime.Scheme, error) {
	scheme := runtime.NewScheme()

	if err := clientgoscheme.AddToScheme(scheme); err != nil {
		return nil, err
	}

	if err := byokv1.AddToScheme(scheme); err != nil {
		return nil, err
	}

	// Register CNS operator types (CnsNodeVmAttachment, CnsNodeVMBatchAttachment, etc.)
	if err := cnsoperatorapis.AddToScheme(scheme); err != nil {
		return nil, err
	}

	return scheme, nil
}
