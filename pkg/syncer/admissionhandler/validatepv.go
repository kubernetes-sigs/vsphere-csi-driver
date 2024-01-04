package admissionhandler

import (
	"context"
	"encoding/json"
	"fmt"

	admissionv1 "k8s.io/api/admission/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"sigs.k8s.io/vsphere-csi-driver/v3/pkg/csi/service/logger"
)

const (
	// FileVolumeWithNodeAffinityError conveys the message that a file volume cannot have node affinity rules on it.
	FileVolumeWithNodeAffinityError = "Invalid configuration. File volumes cannot have node affinity rules"
)

func validatePv(ctx context.Context, req *admissionv1.AdmissionRequest) *admissionv1.AdmissionResponse {

	log := logger.GetLogger(ctx)

	if !featureGateTopologyAwareFileVolumeEnabled {
		return &admissionv1.AdmissionResponse{
			Allowed: true,
		}
	}

	var (
		allowed bool
		result  *metav1.Status
	)

	switch req.Kind.Kind {
	case "PersistentVolume":
		pv := corev1.PersistentVolume{}
		log.Debugf("JSON req.Object.Raw: %v", string(req.Object.Raw))
		if err := json.Unmarshal(req.Object.Raw, &pv); err != nil {
			log.Errorf("error deserializing PV: %v. skipping validation.", err)
			allowed = false
			result = &metav1.Status{
				Message: fmt.Sprintf("Failed to serialize PV: %+V", err),
			}
			break
		}

		if !isFileVolume(pv.Spec.AccessModes) {
			log.Debugf("Not a file volume. Skipping validation.")
			allowed = true
			break
		}

		allowed = true
		if pv.Spec.NodeAffinity != nil {
			allowed = false
			result = &metav1.Status{
				Message: FileVolumeWithNodeAffinityError,
			}
		}

	default:
		allowed = false
		result = &metav1.Status{
			Message: fmt.Sprintf("Can't validate resource kind: %q using validatePv function", req.Kind.Kind),
		}
		log.Errorf("Can't validate resource kind: %q using validatePv function", req.Kind.Kind)
	}

	// return AdmissionResponse result
	return &admissionv1.AdmissionResponse{
		Allowed: allowed,
		Result:  result,
	}

}

func isFileVolume(accessModes []corev1.PersistentVolumeAccessMode) bool {
	for _, accessMode := range accessModes {
		if accessMode == corev1.ReadWriteMany || accessMode == corev1.ReadOnlyMany {
			return true
		}
	}
	return false
}
