package admissionhandler

import (
	"context"
	"encoding/json"
	"fmt"
	"slices"
	"strings"

	"sigs.k8s.io/vsphere-csi-driver/v3/pkg/csi/service/common/commonco/k8sorchestrator"

	"k8s.io/apimachinery/pkg/api/resource"
	"sigs.k8s.io/vsphere-csi-driver/v3/pkg/csi/service/common"

	snapshotv1 "github.com/kubernetes-csi/external-snapshotter/client/v8/apis/volumesnapshot/v1"
	admissionv1 "k8s.io/api/admission/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"sigs.k8s.io/vsphere-csi-driver/v3/pkg/csi/service/logger"
	k8s "sigs.k8s.io/vsphere-csi-driver/v3/pkg/kubernetes"
)

const (
	ExpandVolumeWithSnapshotErrorMessage   = "Expanding volume with snapshots is not allowed"
	ExpandLinkedCloneVolumeErrorMessage    = "Expanding linked clone volume is not allowed"
	UpdateLinkedCloneVolumeAnnErrorMessage = "Cannot update linked clone volume annotations after creation"
	DeleteVolumeWithSnapshotErrorMessage   = "Deleting volume with snapshots is not allowed"
)

// validatePVC helps validate AdmissionReview requests for PersistentVolumeClaim.
func validatePVC(ctx context.Context, req *admissionv1.AdmissionRequest) *admissionv1.AdmissionResponse {
	if !featureGateBlockVolumeSnapshotEnabled {
		// If CSI block volume snapshot is disabled and webhook is running,
		// skip validation for PersistentVolumeClaim.
		return &admissionv1.AdmissionResponse{
			Allowed: true,
		}
	}

	if req.Operation != admissionv1.Update && req.Operation != admissionv1.Delete {
		// If AdmissionReview request operation is out of expectation,
		// skip validation for PersistentVolumeClaim.
		return &admissionv1.AdmissionResponse{
			Allowed: true,
		}
	}

	log := logger.GetLogger(ctx)
	var result *metav1.Status
	allowed := true

	switch req.Kind.Kind {
	case "PersistentVolumeClaim":
		oldPVC := corev1.PersistentVolumeClaim{}
		log.Debugf("JSON req.OldObject.Raw: %v", string(req.OldObject.Raw))
		// req.OldObject is null for CREATE and CONNECT operations.
		if err := json.Unmarshal(req.OldObject.Raw, &oldPVC); err != nil {
			log.Errorf("error deserializing old pvc: %v. skipping validation.", err)
			return &admissionv1.AdmissionResponse{
				// skip validation if there is pvc deserialization error
				Allowed: true,
			}
		}
		oldReq := oldPVC.Spec.Resources.Requests[corev1.ResourceStorage]

		if isFileVolume(oldPVC.Spec.AccessModes, *oldPVC.Spec.VolumeMode) {
			log.Info("PVC is a file volume. skipping validation.")
			return &admissionv1.AdmissionResponse{
				// skip validation if the pvc is not RWO
				Allowed: true,
			}
		}
		var newPVC corev1.PersistentVolumeClaim
		var newReq resource.Quantity
		if req.Operation != admissionv1.Delete {
			newPVC = corev1.PersistentVolumeClaim{}
			log.Debugf("JSON req.Object.Raw: %v", string(req.Object.Raw))
			// req.Object is null for DELETE operations.
			if err := json.Unmarshal(req.Object.Raw, &newPVC); err != nil {
				log.Errorf("error deserializing old pvc: %v. skipping validation.", err)
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
			// Check if the namespace is being deleted for PVC deletion operations
			if req.Operation == admissionv1.Delete && isNamespaceBeingDeleted(ctx, oldPVC.Namespace) {
				log.Infof("Allowing PVC %s/%s deletion as namespace %s is being deleted",
					oldPVC.Namespace, oldPVC.Name, oldPVC.Namespace)
				return &admissionv1.AdmissionResponse{
					Allowed: true,
					Result: &metav1.Status{
						Reason: "Namespace is being deleted",
					},
				}
			}

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
			} else {
				// Determine the state of the linked clone annotation on the old PVC
				oldPVCHasLinkedCloneAnn := metav1.HasAnnotation(oldPVC.ObjectMeta, common.AttributeIsLinkedClone)
				oldPVCLinkedCloneIsFalse := oldPVCHasLinkedCloneAnn && oldPVC.Annotations[common.AnnKeyLinkedClone] == "false"

				// Determine the state of the linked clone annotation on the new PVC
				newPVCHasLinkedCloneAnn := metav1.HasAnnotation(newPVC.ObjectMeta, common.AttributeIsLinkedClone)
				newPVCLinkedCloneIsTrue := newPVCHasLinkedCloneAnn && newPVC.Annotations[common.AnnKeyLinkedClone] == "true"

				if !oldPVCHasLinkedCloneAnn && newPVCHasLinkedCloneAnn {
					// Scenario 1: Adding the linked clone annotation where it didn't exist before
					allowed = false
					result = &metav1.Status{
						Reason: UpdateLinkedCloneVolumeAnnErrorMessage,
					}
				} else if oldPVCLinkedCloneIsFalse && newPVCLinkedCloneIsTrue {
					// Scenario 2: Changing the linked clone annotation from "false" to "true"
					allowed = false
					result = &metav1.Status{
						Reason: UpdateLinkedCloneVolumeAnnErrorMessage,
					}
				}
			}
		}
		if featureIsLinkedCloneSupportEnabled && allowed {
			// If the LinkedClone PVC annotation is being removed
			if req.Operation == admissionv1.Update {
				oldPVCLinkedClone := metav1.HasAnnotation(oldPVC.ObjectMeta, common.AttributeIsLinkedClone) &&
					oldPVC.Annotations[common.AnnKeyLinkedClone] == "true"

				if oldPVCLinkedClone {
					newPVCLinkedClone := metav1.HasAnnotation(newPVC.ObjectMeta, common.AttributeIsLinkedClone) &&
						newPVC.Annotations[common.AnnKeyLinkedClone] == "true"

					if !newPVCLinkedClone {
						allowed = false
						result = &metav1.Status{
							Reason: UpdateLinkedCloneVolumeAnnErrorMessage,
						}
					}
				}
			}
			// If the LinkedClone PVC is being expanded
			if req.Operation == admissionv1.Update && newReq.Cmp(oldReq) > 0 && allowed {
				if metav1.HasAnnotation(oldPVC.ObjectMeta, common.AttributeIsLinkedClone) {
					if oldPVC.Annotations[common.AnnKeyLinkedClone] == "true" {
						allowed = false
						result = &metav1.Status{
							Reason: ExpandLinkedCloneVolumeErrorMessage,
						}
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
			"Stopping getting reclaim policy for PVC, %s/%s", pvc.Spec.VolumeName, err, pvc.Namespace, pvc.Name)
	}

	return pv.Spec.PersistentVolumeReclaimPolicy, nil
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

// validateGuestPVCOperation helps validate AdmissionReview requests for PersistentVolumeClaim.
func validateGuestPVCOperation(ctx context.Context, req *admissionv1.AdmissionRequest) *admissionv1.AdmissionResponse {
	log := logger.GetLogger(ctx)
	isLinkedCLoneRequest := false
	log.Debugf("validateGuestPVCOperation called with the request %s/%s", req.Namespace, req.Name)
	if !featureIsLinkedCloneSupportEnabled {
		return &admissionv1.AdmissionResponse{
			Allowed: true,
		}
	}

	if req.Kind.Kind != "PersistentVolumeClaim" || req.Operation != admissionv1.Create {
		// Skip for non create operations
		return &admissionv1.AdmissionResponse{
			Allowed: true,
		}
	}

	// Unmarshal the object as PVC
	pvc := corev1.PersistentVolumeClaim{}
	err := json.Unmarshal(req.Object.Raw, &pvc)
	if err != nil {
		log.Errorf("error deserializing pvc: %v. failing validation.", err)
		return &admissionv1.AdmissionResponse{
			Allowed: false,
			Result: &metav1.Status{
				Message: fmt.Sprintf("error deserializing pvc. error: %v, failing validation.", err),
			},
		}
	}
	log.Debugf("validateGuestPVCOperation called with the PVC request %+v", pvc)

	if metav1.HasAnnotation(pvc.ObjectMeta, common.AttributeIsLinkedClone) {
		if pvc.Annotations[common.AnnKeyLinkedClone] == "true" {
			isLinkedCLoneRequest = true
		}
	}

	if !isLinkedCLoneRequest {
		return &admissionv1.AdmissionResponse{
			Allowed: true,
		}
	}

	isFileVolumeRequest := slices.Contains(pvc.Spec.AccessModes, corev1.ReadWriteMany) &&
		len(pvc.Spec.AccessModes) == 1

	if isFileVolumeRequest {
		return &admissionv1.AdmissionResponse{
			Allowed: false,
			Result: &metav1.Status{
				Message: "cannot create a linked clone for a file volume",
			},
		}
	}

	// ValidateLinkedCloneRequest validates the various conditions necessary for a valid linkedclone request.
	// 1. The PVC datasource needs to be of type VolumeSnapshot
	// 2. The storageclass associated LinkedClone PVC should be the same the source PVC
	// 3. The size should be the same as the source PVC
	// 4. Should not be a second level LinkedClone
	// 5. VS is not under deletion
	// 6. VS is Ready
	// Note: order does not matter

	dataSource, err := k8sorchestrator.GetPVCDataSource(ctx, &pvc)
	if err != nil {
		return &admissionv1.AdmissionResponse{
			Allowed: false,
			Result: &metav1.Status{
				Message: fmt.Sprintf("failed to get data source for PVC. error: %v, failing validation.", err),
			},
		}
	}
	// Check if the data source field is set at all.
	if dataSource == nil {
		return &admissionv1.AdmissionResponse{
			Allowed: false,
			Result: &metav1.Status{
				Message: "datasource not specified for linked clone PVC request",
			},
		}
	}

	// Validate the APIGroup. It should be "snapshot.storage.k8s.io"
	if dataSource.APIVersion != "snapshot.storage.k8s.io" {
		return &admissionv1.AdmissionResponse{
			Allowed: false,
			Result: &metav1.Status{
				Message: "datasource type is incorrect for linked clone pvc request, epxected snapshot.storage.k8s.io",
			},
		}
	}

	// Validate the Kind. It must be "VolumeSnapshot".
	if dataSource.Kind != "VolumeSnapshot" {
		return &admissionv1.AdmissionResponse{
			Allowed: false,
			Result: &metav1.Status{
				Message: "datasource Kind is incorrect for linked clone request, epxected VolumeSnapshot",
			},
		}
	}

	k8sClient, err := k8s.NewClient(ctx)
	if err != nil {
		return &admissionv1.AdmissionResponse{
			Allowed: false,
			Result: &metav1.Status{
				Message: fmt.Sprintf("failed to get k8s client when validating linkedclone request"+
					" with error: %v", err),
			},
		}
	}

	snapClient, err := k8s.NewSnapshotterClient(ctx)
	if err != nil {
		return &admissionv1.AdmissionResponse{
			Allowed: false,
			Result: &metav1.Status{
				Message: fmt.Sprintf("failed to get snapshotterClient when validating linkedclone request"+
					" with error: %v", err),
			},
		}
	}

	volumeSnapshot, err := snapClient.SnapshotV1().VolumeSnapshots(dataSource.Namespace).Get(ctx, dataSource.Name,
		metav1.GetOptions{})
	if err != nil {
		return &admissionv1.AdmissionResponse{
			Allowed: false,
			Result: &metav1.Status{
				Message: fmt.Sprintf("error getting snapshot %s/%s from api server when validating linked clone "+
					"request, error: %v", dataSource.Namespace, dataSource.Name, err),
			},
		}
	}

	if volumeSnapshot.ObjectMeta.DeletionTimestamp != nil {
		errMsg := fmt.Sprintf("LinkedClone %s/%s source VolumeSnapshot %s/%s is marked for deletion, "+
			"failing linkedclone request", pvc.Namespace, pvc.Name, dataSource.Namespace, dataSource.Name)
		return &admissionv1.AdmissionResponse{
			Allowed: false,
			Result: &metav1.Status{
				Message: errMsg,
			},
		}
	}

	// Validate that the VolumeSnapshot is Ready
	if volumeSnapshot.Status == nil {
		errMsg := fmt.Sprintf("volumesnapshot %s status unavailable, failing linkedclone request",
			volumeSnapshot.Name)
		return &admissionv1.AdmissionResponse{
			Allowed: false,
			Result: &metav1.Status{
				Message: errMsg,
			},
		}
	}

	// Validate that ReadyToUse is set
	if volumeSnapshot.Status != nil && volumeSnapshot.Status.ReadyToUse == nil {
		errMsg := fmt.Sprintf("volumeSnapshot %s is ReadyToUse is unavailable, failing linkedclone request",
			volumeSnapshot.Name)
		return &admissionv1.AdmissionResponse{
			Allowed: false,
			Result: &metav1.Status{
				Message: errMsg,
			},
		}
	}

	// Check if the VolumeSnapshot is ready
	if !*volumeSnapshot.Status.ReadyToUse {
		errMsg := fmt.Sprintf("volumeSnapshot %s is not ready", volumeSnapshot.Name)
		return &admissionv1.AdmissionResponse{
			Allowed: false,
			Result: &metav1.Status{
				Message: errMsg,
			},
		}
	}

	// Retrieve the source PVC from the VolumeSnapshot
	sourcePVCName := volumeSnapshot.Spec.Source.PersistentVolumeClaimName
	// If the VolumeSnapshot is statically provisioned, we don't need to
	// validate based on the source PVC.
	if sourcePVCName != nil {
		sourcePVC, err := k8sClient.CoreV1().PersistentVolumeClaims(volumeSnapshot.Namespace).Get(ctx, *sourcePVCName,
			metav1.GetOptions{})
		if err != nil {
			errMsg := fmt.Sprintf("error getting source PVC %v/%v from api server: %v",
				volumeSnapshot.Namespace, *sourcePVCName, err)
			return &admissionv1.AdmissionResponse{
				Allowed: false,
				Result: &metav1.Status{
					Message: errMsg,
				},
			}
		}

		// The storageclass associated LinkedClone PVC should be the same the source PVC
		sourcePVCStorageClassName := sourcePVC.Spec.StorageClassName
		same := strings.Compare(*pvc.Spec.StorageClassName, *sourcePVCStorageClassName)
		if same != 0 {
			errMsg := fmt.Sprintf("StorageClass mismatch, Namespace: %s, LinkedClone StorageClass: "+
				"%s, source PVC StorageClass: %s", sourcePVC.Namespace, *pvc.Spec.StorageClassName,
				*sourcePVCStorageClassName)
			return &admissionv1.AdmissionResponse{
				Allowed: false,
				Result: &metav1.Status{
					Message: errMsg,
				},
			}
		}

		// the size should be same
		sourcePVCSize, ok1 := sourcePVC.Spec.Resources.Requests[corev1.ResourceStorage]
		linkedClonePVCSize, ok2 := pvc.Spec.Resources.Requests[corev1.ResourceStorage]
		if !ok1 {
			errMsg := fmt.Sprintf("source PVC %s/%s does not have a storage request defined",
				sourcePVC.Namespace, sourcePVC.Name)
			return &admissionv1.AdmissionResponse{
				Allowed: false,
				Result: &metav1.Status{
					Message: errMsg,
				},
			}
		}
		if !ok2 {
			errMsg := fmt.Sprintf("linkedClone PVC %s/%s does not have a storage request defined",
				pvc.Namespace, pvc.Name)
			return &admissionv1.AdmissionResponse{
				Allowed: false,
				Result: &metav1.Status{
					Message: errMsg,
				},
			}
		}
		szResult := sourcePVCSize.Cmp(linkedClonePVCSize)
		if szResult != 0 {
			errMsg := fmt.Sprintf("size mismatch, Source PVC: %s LinkedClone PVC: %s",
				sourcePVCSize.String(), linkedClonePVCSize.String())
			return &admissionv1.AdmissionResponse{
				Allowed: false,
				Result: &metav1.Status{
					Message: errMsg,
				},
			}
		}

		// should not be a second level LinkedClone
		if metav1.HasAnnotation(sourcePVC.ObjectMeta, common.AnnKeyLinkedClone) {
			errMsg := fmt.Sprintf("cannot create a LinkedClone "+
				"from a VolumeSnapshot %s/%s that is created from another LinkedClone %s/%s",
				volumeSnapshot.Namespace, volumeSnapshot.Name, sourcePVC.Namespace, sourcePVC.Name)
			return &admissionv1.AdmissionResponse{
				Allowed: false,
				Result: &metav1.Status{
					Message: errMsg,
				},
			}
		}
	}
	// return AdmissionResponse result
	return &admissionv1.AdmissionResponse{
		Allowed: true,
	}
}
