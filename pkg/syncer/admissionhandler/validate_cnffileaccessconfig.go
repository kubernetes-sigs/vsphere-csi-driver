package admissionhandler

import (
	"context"
	"encoding/json"
	"fmt"

	admissionv1 "k8s.io/api/admission/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
	cnsoperatorv1alpha1 "sigs.k8s.io/vsphere-csi-driver/v3/pkg/apis/cnsoperator"

	"k8s.io/client-go/rest"
	cnsfileaccessconfigv1alpha1 "sigs.k8s.io/vsphere-csi-driver/v3/pkg/apis/cnsoperator/cnsfileaccessconfig/v1alpha1"
	"sigs.k8s.io/vsphere-csi-driver/v3/pkg/csi/service/logger"
	k8s "sigs.k8s.io/vsphere-csi-driver/v3/pkg/kubernetes"
)

// validateCnsFileAccessConfig validates if a CnsFileAccessConfig CR with the same VM and PVC already exists.
// If it already exists, do not allow creation of another CR.
func validateCnsFileAccessConfig(ctx context.Context, clientConfig *rest.Config,
	req *admissionv1.AdmissionRequest) *admissionv1.AdmissionResponse {
	log := logger.GetLogger(ctx)

	cnsFileAccessConfig := cnsfileaccessconfigv1alpha1.CnsFileAccessConfig{}
	log.Debugf("JSON req.Object.Raw: %v", string(req.Object.Raw))
	if err := json.Unmarshal(req.Object.Raw, &cnsFileAccessConfig); err != nil {
		log.Errorf("error deserializing CnsFileAccessConfig: %v. skipping validation.", err)
		// return AdmissionResponse result
		return &admissionv1.AdmissionResponse{
			Allowed: false,
			Result: &metav1.Status{
				Message: fmt.Sprintf("Failed to serialize CnsFileAccessConfig: %v", err),
			},
		}
	}

	vm := cnsFileAccessConfig.Spec.VMName
	pvc := cnsFileAccessConfig.Spec.PvcName
	namespace := cnsFileAccessConfig.Namespace
	existingCnsFileAccessConfigName, err := cnsFileAccessConfigAlreadyExists(ctx,
		clientConfig, namespace, vm, pvc)
	if err != nil {
		return &admissionv1.AdmissionResponse{
			Allowed: false,
			Result: &metav1.Status{
				Message: fmt.Sprintf("Failed to verify if CnsFileAccessConfig already exists: %v", err),
			},
		}
	}

	// If CR already exists, do not allow this request
	if existingCnsFileAccessConfigName != "" {
		return &admissionv1.AdmissionResponse{
			Allowed: false,
			Result: &metav1.Status{
				Message: fmt.Sprintf("CnsFileAccessConfig %s already exists for VM %s and PVC %s",
					existingCnsFileAccessConfigName, vm, pvc),
			},
		}
	}

	// Existing CR not found. Allow this request.
	return &admissionv1.AdmissionResponse{
		Allowed: true,
	}

}

// cnsFileAccessConfigAlreadyExists lists all CnsFileAccessConfig CRs in the given namespace
// and verifies if any of them has the same VM name and PVC name.
// It returns the name of the CR with the same VM and PVC.
func cnsFileAccessConfigAlreadyExists(ctx context.Context, clientConfig *rest.Config, namespace string,
	vm string, pvc string) (string, error) {
	log := logger.GetLogger(ctx)

	cnsOperatorClient, err := k8s.NewClientForGroup(ctx, clientConfig, cnsoperatorv1alpha1.GroupName)
	if err != nil {
		log.Errorf("failed to create CnsOperator client. Err: %+v", err)
		return "", err
	}

	// Get the list of all CnsFileAccessConfig CRs in the given namespace.
	cnsFileAccessConfigList := &cnsfileaccessconfigv1alpha1.CnsFileAccessConfigList{}
	err = cnsOperatorClient.List(ctx, cnsFileAccessConfigList, &client.ListOptions{Namespace: namespace})
	if err != nil {
		log.Errorf("failed to list CnsFileAccessConfigList CRs from %s namesapace. Error: %+v",
			namespace, err)
		return "", err
	}

	for _, cnsFileAccessConfig := range cnsFileAccessConfigList.Items {
		if cnsFileAccessConfig.Spec.VMName == vm {
			if cnsFileAccessConfig.Spec.PvcName == pvc {
				return cnsFileAccessConfig.Name, nil
			}
		}
	}
	return "", nil
}
