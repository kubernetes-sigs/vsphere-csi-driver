package crypto

import (
	byokv1 "github.com/vmware-tanzu/vm-operator/external/byok/api/v1alpha1"
	"k8s.io/apimachinery/pkg/runtime"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
)

func NewK8sScheme() (*runtime.Scheme, error) {
	scheme := runtime.NewScheme()

	if err := clientgoscheme.AddToScheme(scheme); err != nil {
		return nil, err
	}

	if err := byokv1.AddToScheme(scheme); err != nil {
		return nil, err
	}

	return scheme, nil
}
