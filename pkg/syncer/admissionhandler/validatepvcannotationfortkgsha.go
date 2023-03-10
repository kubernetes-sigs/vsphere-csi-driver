package admissionhandler

import (
	"context"
	"encoding/json"

	admissionv1 "k8s.io/api/admission/v1"
	corev1 "k8s.io/api/core/v1"
	"sigs.k8s.io/controller-runtime/pkg/webhook/admission"

	"sigs.k8s.io/vsphere-csi-driver/v3/pkg/csi/service/common"
	"sigs.k8s.io/vsphere-csi-driver/v3/pkg/csi/service/logger"
)

const (
	CreatePVCWithInvalidAnnotation = "Create PVC with invalid annotation " + common.AnnGuestClusterRequestedTopology
	AddPVCAnnotation               = "Add a new PVC Annotation"
	UpdatePVCAnnotation            = "Update the PVC Annotation"
	RemovePVCAnnotation            = "Remove the PVC Annotation"
)

func validatePVCAnnotationForTKGSHA(ctx context.Context, request admission.Request) admission.Response {
	log := logger.GetLogger(ctx)
	log.Debugf("validatePVCAnnotationForTKGSHA called with the request %v", request)

	newPVC := corev1.PersistentVolumeClaim{}
	if err := json.Unmarshal(request.Object.Raw, &newPVC); err != nil {
		reason := "skipped validation when failed to deserialize PVC from new request object"
		log.Warn(reason)
		return admission.Allowed(reason)
	}

	if request.Operation == admissionv1.Create {
		// disallow PVC Create with AnnGuestClusterRequestedTopology key but empty value
		annGuestClusterRequestedTopologyValue, ok := newPVC.Annotations[common.AnnGuestClusterRequestedTopology]
		if ok && annGuestClusterRequestedTopologyValue == "" {
			return admission.Denied(CreatePVCWithInvalidAnnotation)
		}
	} else if request.Operation == admissionv1.Update {
		oldPVC := corev1.PersistentVolumeClaim{}
		if err := json.Unmarshal(request.OldObject.Raw, &oldPVC); err != nil {
			reason := "skipped validation when failed to deserialize PVC from old request object"
			log.Warn(reason)
			return admission.Allowed(reason)
		}

		// disallow PVC Update on AnnGuestClusterRequestedTopology
		oldAnnGuestClusterRequestedTopologyValue, oldOk := oldPVC.Annotations[common.AnnGuestClusterRequestedTopology]
		newAnnGuestClusterRequestedTopologyValue, newOk := newPVC.Annotations[common.AnnGuestClusterRequestedTopology]
		if oldOk && newOk {
			// disallow changing AnnGuestClusterRequestedTopology to a different value
			if oldAnnGuestClusterRequestedTopologyValue != newAnnGuestClusterRequestedTopologyValue {
				return admission.Denied(UpdatePVCAnnotation + ", " + common.AnnGuestClusterRequestedTopology)
			}
		} else if oldOk {
			// disallow removing the annotation of AnnGuestClusterRequestedTopology in an existing PVC
			return admission.Denied(RemovePVCAnnotation + ", " + common.AnnGuestClusterRequestedTopology)
		} else if newOk {
			// disallow adding an annotation of AnnGuestClusterRequestedTopology in an existing PVC
			return admission.Denied(AddPVCAnnotation + ", " + common.AnnGuestClusterRequestedTopology)
		}

		// disallow PVC Update on AnnVolumeAccessibleTopology to another non-empty value
		oldAnnVolumeAccessibleTopologyValue, oldOk := oldPVC.Annotations[common.AnnVolumeAccessibleTopology]
		newAnnVolumeAccessibleTopologyValue, newOk := newPVC.Annotations[common.AnnVolumeAccessibleTopology]
		if oldOk && newOk {
			if oldAnnVolumeAccessibleTopologyValue != newAnnVolumeAccessibleTopologyValue {
				// disallow changing AnnVolumeAccessibleTopology to another a different value except
				// only allow changing AnnVolumeAccessibleTopology from an empty value to a non-empty value
				if !(oldAnnVolumeAccessibleTopologyValue == "" && newAnnVolumeAccessibleTopologyValue != "") {
					return admission.Denied(UpdatePVCAnnotation + ", " + common.AnnVolumeAccessibleTopology)
				}
			}
		} else if oldOk {
			// disallow removing the annotation of AnnVolumeAccessibleTopology in an existing PVC
			return admission.Denied(RemovePVCAnnotation + ", " + common.AnnVolumeAccessibleTopology)
		} else if newOk {
			// Note: allow adding an annotation of AnnVolumeAccessibleTopology in an existing PVC for now;
			// We can prevent SV DevOps from doing that by checking request.userInfo if necessary
			log.Debugf("Allowing adding an annotation, %v, in an existing PVC for now. "+
				"Might be denied later if necessary", common.AnnVolumeAccessibleTopology)
		}
	}

	log.Debugf("validatePVCAnnotationForTKGSHA completed for the request %v", request)
	return admission.Allowed("")
}
