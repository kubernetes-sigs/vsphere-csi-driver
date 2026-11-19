package admissionhandler

import (
	"context"
	"encoding/json"
	"fmt"

	"k8s.io/apimachinery/pkg/labels"
	"sigs.k8s.io/vsphere-csi-driver/v3/pkg/csi/service/common"

	admissionv1 "k8s.io/api/admission/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/webhook/admission"

	"sigs.k8s.io/vsphere-csi-driver/v3/pkg/csi/service/logger"

	snap "github.com/kubernetes-csi/external-snapshotter/client/v8/apis/volumesnapshot/v1"
	snapshotterClientSet "github.com/kubernetes-csi/external-snapshotter/client/v8/clientset/versioned"
)

const (
	SnapshotOperationNotAllowed = "Snapshot creation initiated directly from the Supervisor cluster " +
		"is not supported. Please initiate snapshot creation from the Guest cluster."
	SnapshotFeatureNotEnabled = "CSI Snapshot feature is not supported for vSphere driver on either " +
		"Supervisor cluster or Guest cluster. Please upgrade the appropriate cluster to get support for CSI snapshot feature."
)

// Validate snapshot operations initiated on guest cluster and disallow if CSI snapshot feature is disabled
// on either of supervisor or guest cluster.
func validateSnapshotOperationGuestRequest(ctx context.Context, req *admissionv1.AdmissionRequest) admission.Response {
	log := logger.GetLogger(ctx)
	log.Debugf("validateSnapshotOperationGuestRequest called with the request name=%q namespace=%q kind=%q",
		req.Name, req.Namespace, req.Kind.Kind)
	if req.Kind.Kind == "VolumeSnapshotClass" {
		vsclass := snap.VolumeSnapshotClass{}
		if err := json.Unmarshal(req.Object.Raw, &vsclass); err != nil {
			reason := "error deserializing volume snapshot class"
			log.Warn(reason)
			return admission.Denied(reason)
		}
		log.Debugf("Validating VolumeSnapshotClass: %q", vsclass.Name)
		// A class names no volume, so it cannot tell a guest NFS snapshot, which does not need the
		// block-volume-snapshot feature, from a block volume one. It is allowed while guest NFS volumes
		// are enabled; the VolumeSnapshot and VolumeSnapshotContent checks below still refuse snapshots
		// of other volumes.
		if vsclass.Driver == "csi.vsphere.vmware.com" && !featureGateBlockVolumeSnapshotEnabled &&
			!featureIsGuestNFSVolumeEnabled {
			// Disallow any operation on VolumeSnapshotClass object if block-volume-snapshot feature is not enabled
			return admission.Denied(SnapshotFeatureNotEnabled)
		}
	} else if req.Kind.Kind == "VolumeSnapshotContent" {
		vsc := snap.VolumeSnapshotContent{}
		if err := json.Unmarshal(req.Object.Raw, &vsc); err != nil {
			reason := "error deserializing volume snapshot content"
			log.Warn(reason)
			return admission.Denied(reason)
		}
		log.Debugf("Validating VolumeSnapshotContent: %q", vsc.Name)
		if vsc.Spec.Driver == "csi.vsphere.vmware.com" && !featureGateBlockVolumeSnapshotEnabled &&
			!isGuestNFSVolumeSnapshotContent(&vsc) {
			// Disallow any operation on VolumeSnapshotContent object if block-volume-snapshot feature is not enabled
			return admission.Denied(SnapshotFeatureNotEnabled)
		}
	} else if req.Kind.Kind == "VolumeSnapshot" {
		vs := snap.VolumeSnapshot{}
		// Handle VolumeSnapshot deletion with Linked Clone support.
		if req.Operation == admissionv1.Delete {
			if !featureIsLinkedCloneSupportEnabled {
				return admission.Allowed("")
			}
			if err := json.Unmarshal(req.OldObject.Raw, &vs); err != nil {
				reason := "error deserializing volume snapshot"
				log.Warn(reason)
				return admission.Denied(reason)
			}
			log.Debugf("Validating VolumeSnapshot LinkedClone count: %s/%s", vs.Namespace, vs.Name)

			// Check if the namespace is being deleted. If so, allow the VolumeSnapshot deletion.
			if isNamespaceBeingDeleted(ctx, vs.Namespace) {
				log.Infof("Allowing VolumeSnapshot %s/%s deletion as namespace %s is being deleted",
					vs.Namespace, vs.Name, vs.Namespace)
				return admission.Allowed("Namespace is being deleted")
			}

			return checkIfLinkedClonesExist(ctx, vs)
		}
		if err := json.Unmarshal(req.Object.Raw, &vs); err != nil {
			reason := "error deserializing volume snapshot"
			log.Warn(reason)
			return admission.Denied(reason)
		}
		log.Debugf("Validating VolumeSnapshot: %q Namespace=%q", vs.Name, vs.Namespace)
		// Disallow any operation on VolumeSnapshot object if block-volume-snapshot feature is not enable
		// If no volume snapshot class mentioned i.e. default volume snapshot class to be used, then
		// following checks are skipped. Currently vSphere driver snapshot class is not marked default.
		if vs.Spec.VolumeSnapshotClassName != nil && *vs.Spec.VolumeSnapshotClassName != "" {
			snapshotterClient, err := newSnapshotterClient(ctx)
			if err != nil {
				reason := fmt.Sprintf("failed to get snapshotterClient for class %s with error: %v",
					*vs.Spec.VolumeSnapshotClassName, err)
				log.Warn(reason)
				return admission.Denied(reason)
			}
			vsclass, err := snapshotterClient.SnapshotV1().VolumeSnapshotClasses().Get(ctx,
				*vs.Spec.VolumeSnapshotClassName, metav1.GetOptions{})
			if err != nil {
				reason := fmt.Sprintf("failed to get VolumeSnapshotClass %s with error: %v",
					*vs.Spec.VolumeSnapshotClassName, err)
				log.Warn(reason)
				return admission.Denied(reason)
			}
			// A VolumeSnapshot that is being deleted is let through: by the time the snapshot
			// controller removes its last finalizer its content is gone, so it can no longer be told
			// apart, and refusing the update would only leave it stuck.
			if vsclass.Driver == "csi.vsphere.vmware.com" && !featureGateBlockVolumeSnapshotEnabled &&
				vs.DeletionTimestamp == nil && !isGuestNFSVolumeSnapshot(ctx, snapshotterClient, &vs, req.Namespace) {
				return admission.Denied(SnapshotFeatureNotEnabled)
			}
		}
	}
	log.Debugf("validateSnapshotOperationGuestRequest completed for the request name=%q namespace=%q",
		req.Name, req.Namespace)
	return admission.Allowed("")
}

// isGuestNFSVolumeSnapshotContent reports whether vsc is a snapshot of a guest NFS volume, dynamic
// (its source is the volume) or pre-provisioned (its source is the snapshot). Such a snapshot is an
// archive on the NFS server and does not depend on the block-volume-snapshot feature.
func isGuestNFSVolumeSnapshotContent(vsc *snap.VolumeSnapshotContent) bool {
	src := vsc.Spec.Source
	return (src.VolumeHandle != nil && common.IsGuestNFSVolumeHandle(*src.VolumeHandle)) ||
		(src.SnapshotHandle != nil && common.IsGuestNFSVolumeHandle(*src.SnapshotHandle))
}

// isGuestNFSVolumeSnapshot reports whether vs is a snapshot of a guest NFS volume: by the content it
// is bound to or names, or, if there is none, by the PV of the PVC it is taken of. namespace is that
// of the request, which a new object need not carry. Any other lookup failure reads as false, which
// keeps the check as strict as it is for every other volume.
func isGuestNFSVolumeSnapshot(ctx context.Context, snapshotterClient snapshotterClientSet.Interface,
	vs *snap.VolumeSnapshot, namespace string) bool {
	log := logger.GetLogger(ctx)
	var contentName string
	if vs.Status != nil && vs.Status.BoundVolumeSnapshotContentName != nil {
		contentName = *vs.Status.BoundVolumeSnapshotContentName
	}
	if contentName == "" && vs.Spec.Source.VolumeSnapshotContentName != nil {
		contentName = *vs.Spec.Source.VolumeSnapshotContentName
	}
	if contentName != "" {
		vsc, err := snapshotterClient.SnapshotV1().VolumeSnapshotContents().Get(ctx, contentName, metav1.GetOptions{})
		if err == nil {
			return isGuestNFSVolumeSnapshotContent(vsc)
		}
		if !apierrors.IsNotFound(err) {
			log.Warnf("failed to get VolumeSnapshotContent %q of VolumeSnapshot %s/%s: %v",
				contentName, namespace, vs.Name, err)
			return false
		}
	}
	if vs.Spec.Source.PersistentVolumeClaimName == nil {
		return false
	}
	pvcName := *vs.Spec.Source.PersistentVolumeClaimName
	k8sClient, err := newK8sClient(ctx)
	if err != nil {
		log.Warnf("failed to get k8s client to look up PVC %s/%s: %v", namespace, pvcName, err)
		return false
	}
	pvc, err := k8sClient.CoreV1().PersistentVolumeClaims(namespace).Get(ctx, pvcName, metav1.GetOptions{})
	if err != nil || pvc.Spec.VolumeName == "" {
		log.Debugf("PVC %s/%s of VolumeSnapshot %s is not found or not bound: %v", namespace, pvcName, vs.Name, err)
		return false
	}
	pv, err := k8sClient.CoreV1().PersistentVolumes().Get(ctx, pvc.Spec.VolumeName, metav1.GetOptions{})
	if err != nil {
		log.Warnf("failed to get PV %q of PVC %s/%s: %v", pvc.Spec.VolumeName, namespace, pvcName, err)
		return false
	}
	return common.IsGuestNFSPersistentVolume(pv)
}

func validateSnapshotOperationSupervisorRequest(ctx context.Context,
	req *admissionv1.AdmissionRequest) admission.Response {
	log := logger.GetLogger(ctx)
	log.Debugf("validateSnapshotOperationSupervisorRequest called with the request name=%q namespace=%q kind=%q",
		req.Name, req.Namespace, req.Kind.Kind)

	if req.Kind.Kind == "VolumeSnapshot" {
		if !featureIsLinkedCloneSupportEnabled {
			return admission.Allowed("")
		}

		// Only apply Linked Clone validation for delete operations.
		if featureIsLinkedCloneSupportEnabled && req.Operation == admissionv1.Delete {
			vs := snap.VolumeSnapshot{}
			if err := json.Unmarshal(req.OldObject.Raw, &vs); err != nil {
				reason := "error deserializing volume snapshot"
				log.Warn(reason)
				return admission.Denied(reason)
			}
			log.Debugf("Validating VolumeSnapshot LinkedClone count:%s/%s", vs.Namespace, vs.Name)

			// Check if the namespace is being deleted. If so, allow the VolumeSnapshot deletion.
			if isNamespaceBeingDeleted(ctx, vs.Namespace) {
				log.Infof("Allowing VolumeSnapshot %s/%s deletion as namespace %s is being deleted",
					vs.Namespace, vs.Name, vs.Namespace)
				return admission.Allowed("Namespace is being deleted")
			}

			return checkIfLinkedClonesExist(ctx, vs)
		}
	}
	log.Debugf("validateSnapshotOperationSupervisorRequest completed for the request name=%q namespace=%q",
		req.Name, req.Namespace)
	return admission.Allowed("")
}

// checkIfLinkedClonesExist checks if there are any LinkedClone volumes created out of the
// VolumeSnapshot
func checkIfLinkedClonesExist(ctx context.Context, vs snap.VolumeSnapshot) admission.Response {
	k8sClient, err := newK8sClient(ctx)
	if err != nil {
		return admission.Denied("failed to get k8s client. Error: " + err.Error())
	}
	vsUID := string(vs.UID)
	// List all the PV with the VolumeSnapshot label.
	// If the label is present on the PV it indicates that a LinkedClone was created from this
	// specific volumesnapshot.
	labelSelector := labels.SelectorFromSet(map[string]string{
		common.VolumeContextAttributeLinkedCloneVolumeSnapshotSourceUID: vsUID,
	})
	pvList, err := k8sClient.CoreV1().PersistentVolumes().List(ctx, metav1.ListOptions{
		LabelSelector: labelSelector.String(),
	})
	if err != nil {
		errMsg := fmt.Sprintf("error when checking if there are linkedclones created from volume snapshot, "+
			"failed to list PVs with error: %v", err)
		return admission.Denied(errMsg)
	}

	if len(pvList.Items) != 0 {
		errMsg := fmt.Sprintf("deleting volumesnapshot from which linked clones are created is not allowed. "+
			"There are %d linked clones created from this volumesnapshot", len(pvList.Items))
		return admission.Denied(errMsg)
	}
	return admission.Allowed("")
}

// isNamespaceBeingDeleted checks if a namespace is being deleted by examining its DeletionTimestamp
// Returns true if:
// 1. Namespace exists and has DeletionTimestamp set (being deleted)
// 2. Namespace is not found (already deleted)
func isNamespaceBeingDeleted(ctx context.Context, namespaceName string) bool {
	log := logger.GetLogger(ctx)
	k8sClient, err := newK8sClient(ctx)
	if err != nil {
		log.Errorf("Failed to get kubernetes client while checking namespace deletion status: %v", err)
		return false
	}

	namespace, err := k8sClient.CoreV1().Namespaces().Get(ctx, namespaceName, metav1.GetOptions{})
	if err != nil {
		if apierrors.IsNotFound(err) {
			log.Infof("Namespace %s not found - assuming it's deleted", namespaceName)
			return true
		}
		log.Errorf("Failed to get namespace %s while checking deletion status: %v", namespaceName, err)
		return false
	}

	return namespace.DeletionTimestamp != nil
}
