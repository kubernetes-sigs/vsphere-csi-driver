package admissionhandler

import (
	"context"
	"encoding/json"
	"fmt"
	"regexp"

	admissionv1 "k8s.io/api/admission/v1"
	corev1 "k8s.io/api/core/v1"
	"sigs.k8s.io/controller-runtime/pkg/webhook/admission"

	"sigs.k8s.io/vsphere-csi-driver/v3/pkg/csi/service/common"
	"sigs.k8s.io/vsphere-csi-driver/v3/pkg/csi/service/logger"
)

const (
	NonUpdatablePVCAnnotation = "PVC Annotation %s is not mutable by user %s"
	NonCreatablePVCAnnotation = "PVC Annotation %s cannot be created by user %s"
	CSIServiceAccountPrefix   = "system:serviceaccount:vmware-system-csi"
)

// Disallow editing PVC Volume Health annotation for non cns-csi service account users.
func validatePVCAnnotationForVolumeHealth(ctx context.Context, request admission.Request) admission.Response {
	log := logger.GetLogger(ctx)
	username := request.UserInfo.Username
	isCSIServiceAccount := validateCSIServiceAccount(request.UserInfo.Username)
	log.Debugf("validatePVCAnnotationForVolumeHealth called with the request %v by user: %v", request, username)
	newPVC := corev1.PersistentVolumeClaim{}
	if err := json.Unmarshal(request.Object.Raw, &newPVC); err != nil {
		reason := "skipped validation when failed to deserialize PVC from new request object"
		log.Warn(reason)
		return admission.Allowed(reason)
	}

	if request.Operation == admissionv1.Create {
		_, ok := newPVC.Annotations[common.AnnVolumeHealth]
		if ok && !isCSIServiceAccount {
			return admission.Denied(fmt.Sprintf(NonCreatablePVCAnnotation, common.AnnVolumeHealth, username))
		}
	} else if request.Operation == admissionv1.Update {
		oldPVC := corev1.PersistentVolumeClaim{}
		if err := json.Unmarshal(request.OldObject.Raw, &oldPVC); err != nil {
			reason := "skipped validation when failed to deserialize PVC from old request object"
			log.Warn(reason)
			return admission.Allowed(reason)
		}

		if !isCSIServiceAccount {
			oldAnnVolumeHealthValue, oldOk := oldPVC.Annotations[common.AnnVolumeHealth]
			newAnnVolumeHealthValue, newOk := newPVC.Annotations[common.AnnVolumeHealth]

			// We only need to prevent updates to volume health annotation.
			// Other PVC edit requests should go through.

			if oldOk && newOk {
				// Disallow updating the annotation of AnnVolumeHealth in an existing PVC.
				if oldAnnVolumeHealthValue != newAnnVolumeHealthValue {
					return admission.Denied(fmt.Sprintf(NonUpdatablePVCAnnotation, common.AnnVolumeHealth, username))
				}
			} else if oldOk || newOk {
				// Disallow adding/removing the annotation of AnnVolumeHealth in an existing PVC.
				return admission.Denied(fmt.Sprintf(NonUpdatablePVCAnnotation, common.AnnVolumeHealth, username))
			}
		}
	}

	log.Debugf("validatePVCAnnotationForVolumeHealth completed for the request %v", request)
	return admission.Allowed("")
}

func validateCSIServiceAccount(username string) bool {
	csiServiceAccountRegex, err := regexp.Compile(CSIServiceAccountPrefix)
	if err != nil {
		return true // fail open
	}
	return csiServiceAccountRegex.MatchString(username)
}
