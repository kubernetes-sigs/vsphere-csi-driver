package admissionhandler

import (
	"context"
	"encoding/json"
	"fmt"

	admissionv1 "k8s.io/api/admission/v1"
	corev1 "k8s.io/api/core/v1"
	"sigs.k8s.io/controller-runtime/pkg/webhook/admission"

	"sigs.k8s.io/vsphere-csi-driver/v3/pkg/csi/service/common"
	"sigs.k8s.io/vsphere-csi-driver/v3/pkg/csi/service/logger"
)

const (
	NonCreatablePVCLabel = "PVC Label %s cannot be created by user %s"
	NonUpdatablePVCLabel = "PVC Label %s is not mutable by user %s"
)

// validatePVCLabelForFileStore disallows non cns-csi service account users from setting or editing the
// file store label on a PVC. The label is published by the CSI provisioning flow once the backing
// FileVolume CR is Ready, so a user supplied value would misrepresent which file store actually backs
// the volume.
//
// On CREATE the label is rejected only for dynamically provisioned PVCs. A statically provisioned PVC
// (one that names an existing PV through spec.volumeName) is bound to a volume that CSI did not create
// here, so its labels are the user's to set.
//
// On UPDATE the label is immutable: it may not be added, changed or removed. The CSI service account is
// exempt from both checks, since external-provisioner is what stamps the label in the first place.
func validatePVCLabelForFileStore(ctx context.Context, request admission.Request) admission.Response {
	log := logger.GetLogger(ctx)
	username := request.UserInfo.Username
	log.Debugf("validatePVCLabelForFileStore called with the request %v by user: %v", request, username)
	if request.Operation == admissionv1.Delete {
		// File store label validation is not required for delete PVC calls.
		return admission.Allowed("")
	}
	if validateCSIServiceAccount(username) {
		// The CSI service account owns this label.
		return admission.Allowed("")
	}

	newPVC := corev1.PersistentVolumeClaim{}
	if err := json.Unmarshal(request.Object.Raw, &newPVC); err != nil {
		log.Errorf("error unmarshalling pvc: %v", err)
		reason := "skipped validation when failed to deserialize PVC from new request object"
		log.Warn(reason)
		return admission.Allowed(reason)
	}
	newFileStore, newOk := newPVC.Labels[common.FileStoreLabelKey]

	if request.Operation == admissionv1.Create {
		if newOk && isDynamicallyProvisionedPVC(&newPVC) {
			return admission.Denied(fmt.Sprintf(NonCreatablePVCLabel, common.FileStoreLabelKey, username))
		}
	} else if request.Operation == admissionv1.Update {
		oldPVC := corev1.PersistentVolumeClaim{}
		if err := json.Unmarshal(request.OldObject.Raw, &oldPVC); err != nil {
			log.Errorf("error unmarshalling pvc: %v", err)
			reason := "skipped validation when failed to deserialize PVC from old request object"
			log.Warn(reason)
			return admission.Allowed(reason)
		}

		oldFileStore, oldOk := oldPVC.Labels[common.FileStoreLabelKey]
		// We only need to prevent updates to the file store label. Other PVC edit requests should go through.
		if oldOk && newOk {
			// Disallow changing the value of the file store label on an existing PVC.
			if oldFileStore != newFileStore {
				return admission.Denied(fmt.Sprintf(NonUpdatablePVCLabel, common.FileStoreLabelKey, username))
			}
		} else if oldOk || newOk {
			// Disallow adding/removing the file store label on an existing PVC.
			return admission.Denied(fmt.Sprintf(NonUpdatablePVCLabel, common.FileStoreLabelKey, username))
		}
	}

	log.Debugf("validatePVCLabelForFileStore completed for the request %v", request)
	return admission.Allowed("")
}

// isDynamicallyProvisionedPVC reports whether the PVC, as submitted on CREATE, will be dynamically
// provisioned: it names a storage class and does not pre-bind itself to an existing PV.
func isDynamicallyProvisionedPVC(pvc *corev1.PersistentVolumeClaim) bool {
	return pvc.Spec.StorageClassName != nil && *pvc.Spec.StorageClassName != "" && pvc.Spec.VolumeName == ""
}
