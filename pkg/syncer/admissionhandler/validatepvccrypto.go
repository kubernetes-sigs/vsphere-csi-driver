package admissionhandler

import (
	"context"
	"encoding/json"
	"net/http"
	"strings"

	admissionv1 "k8s.io/api/admission/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/validation/field"
	"sigs.k8s.io/controller-runtime/pkg/webhook/admission"
	"sigs.k8s.io/vsphere-csi-driver/v3/pkg/common/cns-lib/crypto"
	"sigs.k8s.io/vsphere-csi-driver/v3/pkg/csi/service/logger"
)

func validatePVCRequestForCrypto(
	ctx context.Context,
	cryptoClient crypto.Client,
	request admission.Request) admission.Response {

	if request.Operation != admissionv1.Create || request.Operation == admissionv1.Update {
		return admission.Allowed("")
	}

	log := logger.GetLogger(ctx)

	newPVC := &corev1.PersistentVolumeClaim{}
	if err := json.Unmarshal(request.Object.Raw, newPVC); err != nil {
		log.Errorf("error unmarshalling pvc: %v", err)
		reason := "skipped validation when failed to deserialize PVC from new request object"
		log.Warn(reason)
		return admission.Allowed(reason)
	}

	fieldErrs := validatePVCCrypto(ctx, cryptoClient, newPVC)

	validationErrs := make([]string, 0, len(fieldErrs))
	for _, fieldErr := range fieldErrs {
		validationErrs = append(validationErrs, fieldErr.Error())
	}

	// The existence of validation errors means the request is denied.
	if len(fieldErrs) > 0 {
		reason := strings.Join(validationErrs, ", ")
		return admission.Response{
			AdmissionResponse: admissionv1.AdmissionResponse{
				Allowed: false,
				Result: &metav1.Status{
					Reason: metav1.StatusReason(reason),
					Code:   http.StatusUnprocessableEntity,
				},
			},
		}
	}

	return admission.Allowed("")
}

func validatePVCCrypto(
	ctx context.Context,
	cryptoClient crypto.Client,
	pvc *corev1.PersistentVolumeClaim) field.ErrorList {

	if pvc.Spec.StorageClassName == nil {
		return nil
	}

	encClassName := crypto.GetEncryptionClassNameForPVC(pvc)
	if encClassName == "" {
		return nil
	}

	var (
		allErrs          field.ErrorList
		encClassNamePath = field.NewPath("annotations", crypto.PVCEncryptionClassAnnotationName)
	)

	if ok, _, err := cryptoClient.IsEncryptedStorageClass(ctx, *pvc.Spec.StorageClassName); err != nil {
		allErrs = append(allErrs, field.InternalError(encClassNamePath, err))
	} else if !ok {
		allErrs = append(allErrs, field.Invalid(
			encClassNamePath,
			encClassName,
			"requires spec.storageClassName specify an encryption storage class"))
	}

	return allErrs
}
