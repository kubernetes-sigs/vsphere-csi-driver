package admissionhandler

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"

	admissionv1 "k8s.io/api/admission/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
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

	if request.Operation != admissionv1.Create && request.Operation != admissionv1.Update {
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

	var fieldErrs field.ErrorList
	if request.Operation == admissionv1.Create {
		fieldErrs = validatePVCCrypto(ctx, cryptoClient, newPVC)
	} else {
		oldPVC := &corev1.PersistentVolumeClaim{}
		if err := json.Unmarshal(request.OldObject.Raw, oldPVC); err != nil {
			log.Errorf("error unmarshalling old pvc: %v", err)
			reason := "skipped validation when failed to deserialize PVC from old request object"
			log.Warn(reason)
			return admission.Allowed(reason)
		}
		// Only validate updates that add or change the EncryptionClass annotation, or that
		// switch an annotated PVC to a different VolumeAttributesClass, so that unrelated
		// updates (e.g. finalizer or label changes) on existing PVCs are not blocked.
		if crypto.GetEncryptionClassNameForPVC(newPVC) != crypto.GetEncryptionClassNameForPVC(oldPVC) {
			fieldErrs = append(fieldErrs, validatePVCCrypto(ctx, cryptoClient, newPVC)...)
		}
		if featureIsVACPolicyMutabilityEnabled && isVACChanged(oldPVC, newPVC) {
			fieldErrs = append(fieldErrs, validatePVCTargetVACCrypto(ctx, cryptoClient, newPVC)...)
		}
	}

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

	// Use the storage policy currently backing the volume, which is the policy of
	// status.currentVolumeAttributesClassName if set, consistent with the BYOK PVC controller.
	ok, _, source, err := crypto.IsEncryptedStoragePolicyForPVC(
		ctx, cryptoClient, cryptoClient, pvc, featureIsVACPolicyMutabilityEnabled)

	// A VolumeAttributesClass policy comes from status.currentVolumeAttributesClassName, which
	// differs from spec.volumeAttributesClassName until a change to the latter completes (e.g.
	// after it was rejected as Infeasible). Report whichever field names the VAC actually used.
	var (
		vacPath *field.Path
		vacName string
	)
	if source == crypto.StoragePolicySourceVAC {
		vacPath = field.NewPath("spec", "volumeAttributesClassName")
		vacName = *pvc.Status.CurrentVolumeAttributesClassName
		if vacName != *pvc.Spec.VolumeAttributesClassName {
			vacPath = field.NewPath("status", "currentVolumeAttributesClassName")
		}
	}

	switch {
	case err != nil && source == crypto.StoragePolicySourceVAC && apierrors.IsNotFound(err):
		allErrs = append(allErrs, field.NotFound(vacPath, vacName))
	case err != nil:
		allErrs = append(allErrs, field.InternalError(encClassNamePath, err))
	case !ok && source == crypto.StoragePolicySourceVAC:
		allErrs = append(allErrs, field.Invalid(
			encClassNamePath,
			encClassName,
			fmt.Sprintf("requires the effective %s %q to specify an encryption storage policy",
				vacPath, vacName)))
	case !ok:
		allErrs = append(allErrs, field.Invalid(
			encClassNamePath,
			encClassName,
			"requires spec.storageClassName specify an encryption storage class"))
	}

	return allErrs
}

// isVACChanged returns true if newPVC references a VolumeAttributesClass that differs from
// the one referenced by oldPVC.
func isVACChanged(oldPVC, newPVC *corev1.PersistentVolumeClaim) bool {
	if newPVC.Spec.VolumeAttributesClassName == nil {
		return false
	}
	return oldPVC.Spec.VolumeAttributesClassName == nil ||
		*oldPVC.Spec.VolumeAttributesClassName != *newPVC.Spec.VolumeAttributesClassName
}

// validatePVCTargetVACCrypto verifies that a PVC with an EncryptionClass annotation is not
// switched to a VolumeAttributesClass whose storage policy doesn't support encryption. Once
// the ModifyVolume completes, that policy would back the volume and conflict with the
// annotation.
func validatePVCTargetVACCrypto(
	ctx context.Context,
	cryptoClient crypto.Client,
	pvc *corev1.PersistentVolumeClaim) field.ErrorList {

	encClassName := crypto.GetEncryptionClassNameForPVC(pvc)
	if encClassName == "" {
		return nil
	}

	vacName := *pvc.Spec.VolumeAttributesClassName
	vacPath := field.NewPath("spec", "volumeAttributesClassName")

	ok, _, err := crypto.IsEncryptedVAC(ctx, cryptoClient, cryptoClient, vacName)
	switch {
	case meta.IsNoMatchError(err) || apierrors.IsNotFound(err):
		// The VolumeAttributesClass v1 API isn't served, or the VolumeAttributesClass doesn't
		// exist yet; Kubernetes allows referencing one before it is created, and the
		// ModifyVolume stays pending until it is.
		return nil
	case err != nil:
		return field.ErrorList{field.InternalError(vacPath, err)}
	case !ok:
		return field.ErrorList{field.Invalid(
			vacPath,
			vacName,
			fmt.Sprintf("must specify an encryption storage policy while annotation %s is set to %q",
				crypto.PVCEncryptionClassAnnotationName, encClassName))}
	}

	return nil
}
