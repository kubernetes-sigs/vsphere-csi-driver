package admissionhandler

import (
	"context"
	"encoding/json"

	"k8s.io/apimachinery/pkg/api/resource"

	snapshotv1 "github.com/kubernetes-csi/external-snapshotter/client/v4/apis/volumesnapshot/v1"
	admissionv1 "k8s.io/api/admission/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"sigs.k8s.io/vsphere-csi-driver/v3/pkg/csi/service/logger"
	k8s "sigs.k8s.io/vsphere-csi-driver/v3/pkg/kubernetes"
)

const (
	ExpandVolumeWithSnapshotErrorMessage = "Expanding volume with snapshots is not allowed"
	DeleteVolumeWithSnapshotErrorMessage = "Deleting volume with snapshots is not allowed"
)

// validatePVC helps validate AdmissionReview requests for PersistentVolumeClaim.
func validatePVC(ctx context.Context, ar *admissionv1.AdmissionReview) *admissionv1.AdmissionResponse {
	if !featureGateBlockVolumeSnapshotEnabled {
		// If CSI block volume snapshot is disabled and webhook is running,
		// skip validation for PersistentVolumeClaim.
		return &admissionv1.AdmissionResponse{
			Allowed: true,
		}
	}

	if ar.Request.Operation != admissionv1.Update && ar.Request.Operation != admissionv1.Delete {
		// If AdmissionReview request operation is out of expectation,
		// skip validation for PersistentVolumeClaim.
		return &admissionv1.AdmissionResponse{
			Allowed: true,
		}
	}

	log := logger.GetLogger(ctx)
	req := ar.Request
	var result *metav1.Status
	allowed := true

	switch req.Kind.Kind {
	case "PersistentVolumeClaim":
		oldPVC := corev1.PersistentVolumeClaim{}
		log.Debugf("JSON req.OldObject.Raw: %v", string(req.OldObject.Raw))
		// req.OldObject is null for CREATE and CONNECT operations.
		if err := json.Unmarshal(req.OldObject.Raw, &oldPVC); err != nil {
			log.Warnf("error deserializing old pvc: %v. skipping validation.", err)
			return &admissionv1.AdmissionResponse{
				// skip validation if there is pvc deserialization error
				Allowed: true,
			}
		}
		oldReq := oldPVC.Spec.Resources.Requests[corev1.ResourceStorage]

		if !isRWOVolumeRequest(oldPVC.Spec.AccessModes) {
			log.Info("the access mode of PVC is not ReadWriteOnce. skipping validation.")
			return &admissionv1.AdmissionResponse{
				// skip validation if the pvc is not RWO
				Allowed: true,
			}
		}

		var newReq resource.Quantity
		if req.Operation != admissionv1.Delete {
			newPVC := corev1.PersistentVolumeClaim{}
			log.Debugf("JSON req.Object.Raw: %v", string(req.Object.Raw))
			// req.Object is null for DELETE operations.
			if err := json.Unmarshal(req.Object.Raw, &newPVC); err != nil {
				log.Warnf("error deserializing old pvc: %v. skipping validation.", err)
				return &admissionv1.AdmissionResponse{
					// skip validation if there is pvc deserialization error
					Allowed: true,
				}
			}

			newReq = newPVC.Spec.Resources.Requests[corev1.ResourceStorage]
		} else {
			reclaimPolicy, err := getPVReclaimPolicyForPVC(ctx, oldPVC)
			if err != nil {
				log.Warnf("error getting reclaim policy for pvc: %v. skipping validation.", err)
				return &admissionv1.AdmissionResponse{
					// skip validation if there is any error in getting reclaim policy for pvc
					Allowed: true,
				}
			}

			if reclaimPolicy != corev1.PersistentVolumeReclaimDelete {
				log.Info("the reclaim policy of PVC is not Delete. skipping validation.")
				return &admissionv1.AdmissionResponse{
					// skip validation if the reclaim policy of PVC is not Delete
					Allowed: true,
				}
			}
		}

		// only admit PVC deletion or expansion events.
		if req.Operation == admissionv1.Delete || req.Operation == admissionv1.Update && newReq.Cmp(oldReq) > 0 {
			snapshots, err := getSnapshotsForPVC(ctx, oldPVC.Namespace, oldPVC.Name)
			if err != nil {
				log.Warnf("error getting snapshots for pvc: %v. skipping validation.", err)
				return &admissionv1.AdmissionResponse{
					// skip validation if there is any error in getting volume snapshots associated with the pvc
					Allowed: true,
				}
			}
			if len(snapshots) != 0 {
				allowed = false
				if req.Operation == admissionv1.Update {
					result = &metav1.Status{
						Reason: ExpandVolumeWithSnapshotErrorMessage,
					}
				} else if req.Operation == admissionv1.Delete {
					result = &metav1.Status{
						Reason: DeleteVolumeWithSnapshotErrorMessage,
					}
				}
			}
		}
	default:
		allowed = false
		log.Errorf("Can't validate resource kind: %q using validatePVC function", req.Kind.Kind)
	}

	// return AdmissionResponse result
	return &admissionv1.AdmissionResponse{
		Allowed: allowed,
		Result:  result,
	}
}

func getPVReclaimPolicyForPVC(ctx context.Context, pvc corev1.PersistentVolumeClaim) (
	corev1.PersistentVolumeReclaimPolicy, error) {
	log := logger.GetLogger(ctx)

	var result corev1.PersistentVolumeReclaimPolicy

	if pvc.Spec.VolumeName == "" {
		return result, logger.LogNewErrorf(log, "No PV is bound to the PVC %s/%s", pvc.Namespace, pvc.Name)
	}

	kubeClient, err := k8s.NewClient(ctx)
	if err != nil {
		return result, logger.LogNewErrorf(log, "failed to get kube client with error: %v. "+
			"Stopping getting reclaim policy for PVC, %s/%s", err, pvc.Namespace, pvc.Name)
	}

	pv, err := kubeClient.CoreV1().PersistentVolumes().Get(ctx, pvc.Spec.VolumeName, metav1.GetOptions{})
	if err != nil {
		return result, logger.LogNewErrorf(log, "failed to get PV %v with error: %v. "+
			"Stopping getting reclaim policy for PVC, %s/%s", err, pvc.Spec.VolumeName, pvc.Namespace, pvc.Name)
	}

	return pv.Spec.PersistentVolumeReclaimPolicy, nil
}

func isRWOVolumeRequest(accessModes []corev1.PersistentVolumeAccessMode) bool {
	for _, accessMode := range accessModes {
		if accessMode != corev1.ReadWriteOnce {
			return false
		}
	}
	return true
}

func getSnapshotsForPVC(ctx context.Context, ns string, name string) ([]snapshotv1.VolumeSnapshot, error) {
	log := logger.GetLogger(ctx)

	var result []snapshotv1.VolumeSnapshot

	snapshotterClient, err := k8s.NewSnapshotterClient(ctx)
	if err != nil {
		log.Errorf("failed to get snapshotterClient with error: %v. "+
			"Stopping getting snapshots for PVC, %s/%s", err, ns, name)
		return result, err
	}

	volumeSnapshotList, err := snapshotterClient.SnapshotV1().VolumeSnapshots(ns).List(ctx, metav1.ListOptions{})
	if err != nil {
		log.Errorf("failed to list VolumeSnapshot with error: %v. "+
			"Stopping getting snapshots for PVC, %s/%s", err, ns, name)
		return result, err
	}

	for _, volumeSnapshot := range volumeSnapshotList.Items {
		if volumeSnapshot.Spec.Source.PersistentVolumeClaimName == nil {
			continue
		}

		pvcName := *volumeSnapshot.Spec.Source.PersistentVolumeClaimName
		if pvcName == name {
			result = append(result, volumeSnapshot)
		}
	}

	return result, nil
}
