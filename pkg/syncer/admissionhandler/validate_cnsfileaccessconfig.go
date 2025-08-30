package admissionhandler

import (
	"context"
	"encoding/json"
	"fmt"
	"regexp"

	admissionv1 "k8s.io/api/admission/v1"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"sigs.k8s.io/controller-runtime/pkg/client"
	cnsoperatorv1alpha1 "sigs.k8s.io/vsphere-csi-driver/v3/pkg/apis/cnsoperator"

	"k8s.io/client-go/rest"
	cnsfileaccessconfigv1alpha1 "sigs.k8s.io/vsphere-csi-driver/v3/pkg/apis/cnsoperator/cnsfileaccessconfig/v1alpha1"
	"sigs.k8s.io/vsphere-csi-driver/v3/pkg/csi/service/logger"
	k8s "sigs.k8s.io/vsphere-csi-driver/v3/pkg/kubernetes"
)

const (
	KubernetesServiceAccount = "system:serviceaccount:kube-system"
	PvCsiServiceAccountregex = "^system:serviceaccount.*-pvcsi$"
	KubernetesAdmin          = "kubernetes-admin"
)

// validateCreateCnsFileAccessConfig validates if a CnsFileAccessConfig CR with the same VM and PVC already exists.
// If it already exists, do not allow creation of another CR.
func validateCreateCnsFileAccessConfig(ctx context.Context, clientConfig *rest.Config,
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

	// This validation is not required for PVCSI service account.
	isPvCSIServiceAccount, err := validatePvCSIServiceAccount(req.UserInfo.Username)
	if err != nil {
		// return AdmissionResponse result
		return &admissionv1.AdmissionResponse{
			Allowed: false,
			Result: &metav1.Status{
				Message: fmt.Sprintf("failed to validate user information: %s", err),
			},
		}
	}

	// If user is PVCSI service account, allow this request.
	if isPvCSIServiceAccount {
		return &admissionv1.AdmissionResponse{
			Allowed: true,
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

	// List only that CnsFileAccessConfig CRs which has the same VM name and PVC name labels.
	labelSelector := labels.SelectorFromSet(labels.Set{vmNameLabelKey: vm, pvcNameLabelKey: pvc})
	// Get the list of all CnsFileAccessConfig CRs in the given namespace.
	cnsFileAccessConfigList := &cnsfileaccessconfigv1alpha1.CnsFileAccessConfigList{}
	err = cnsOperatorClient.List(ctx, cnsFileAccessConfigList, &client.ListOptions{
		Namespace:     namespace,
		LabelSelector: labelSelector,
	})
	if err != nil {
		log.Errorf("failed to list CnsFileAccessConfigList CRs from %s namesapace. Error: %+v",
			namespace, err)
		return "", err
	}

	if len(cnsFileAccessConfigList.Items) == 1 {
		// There should be only 1 CFC CR with the same VM and PVC
		return cnsFileAccessConfigList.Items[0].Name, nil
	}

	if len(cnsFileAccessConfigList.Items) > 1 {
		// We should never reach here but it's good to have this check.
		return "", fmt.Errorf("invalid case, %d CnsFileAccessConfig instances detected "+
			"with the VM %s and PVC %s", len(cnsFileAccessConfigList.Items), vm, pvc)
	}

	return "", nil
}

// validateDeleteCnsFileAccessConfig allows deletion of a CnsFileAccessConfig instance without
// devops label (indicates that it is a CR being used by guest cluster) if user deleting the instance
// is a CSI or K8s system user or K8s admin.
func validateDeleteCnsFileAccessConfig(ctx context.Context, clientConfig *rest.Config,
	req *admissionv1.AdmissionRequest) *admissionv1.AdmissionResponse {
	log := logger.GetLogger(ctx)

	cnsFileAccessConfig := cnsfileaccessconfigv1alpha1.CnsFileAccessConfig{}
	log.Debugf("JSON req.Object.Raw: %v", string(req.OldObject.Raw))
	if err := json.Unmarshal(req.OldObject.Raw, &cnsFileAccessConfig); err != nil {
		log.Errorf("error deserializing CnsFileAccessConfig: %v. skipping validation.", err)
		// return AdmissionResponse result
		return &admissionv1.AdmissionResponse{
			Allowed: false,
			Result: &metav1.Status{
				Message: fmt.Sprintf("Failed to serialize CnsFileAccessConfig: %v", err),
			},
		}
	}
	// If CR has devops user label, allow this request as
	// it means that it is created by devops user or K8s admin
	// and not by VKS (CSI service account).
	if _, ok := cnsFileAccessConfig.Labels[devopsUserLabelKey]; ok {
		log.Infof("CnsFileAccessConfig %s has devops user label. Allow this reqeust.",
			cnsFileAccessConfig.Name)
		return &admissionv1.AdmissionResponse{
			Allowed: true,
		}
	}

	// Check if user is allowed to delete this CR.
	allowed, err := isUserAllowedForDeletion(req.UserInfo.Username)
	if err != nil {
		return &admissionv1.AdmissionResponse{
			Allowed: false,
			Result: &metav1.Status{
				Message: fmt.Sprintf("Failed to find out if CnsFileAccessConfig can be deleted: %v", err),
			},
		}
	}
	if !allowed {
		return &admissionv1.AdmissionResponse{
			Allowed: false,
			Result: &metav1.Status{
				Message: fmt.Sprintf("User %s is not allowed to delete this CnsFileAccesConfig.",
					req.UserInfo.Username),
			},
		}
	}

	return &admissionv1.AdmissionResponse{
		Allowed: true,
	}
}

// isUserAllowedForDeletion returns true if user is either a PVCSI service account or
// K8s' namespace-cotnroller.
func isUserAllowedForDeletion(username string) (bool, error) {
	pvcCsiServiceAccountRegex, err := regexp.Compile(PvCsiServiceAccountregex)
	if err != nil {
		return false, err
	}

	kubernetesServiceAccount, err := regexp.Compile(KubernetesServiceAccount)
	if err != nil {
		return false, err
	}

	// Allowed users are :
	// 1. PVCSI service account
	// 2. K8s service account (like namespace-controller or generic-garbage-collector)
	// 3. K8s admin
	if pvcCsiServiceAccountRegex.MatchString(username) ||
		kubernetesServiceAccount.MatchString(username) || username == KubernetesAdmin {
		return true, nil

	}

	return false, nil
}

func validatePvCSIServiceAccount(username string) (bool, error) {
	pvcCsiServiceAccountRegex, err := regexp.Compile(PvCsiServiceAccountregex)
	if err != nil {
		return false, err // fail open
	}
	return pvcCsiServiceAccountRegex.MatchString(username), nil
}
