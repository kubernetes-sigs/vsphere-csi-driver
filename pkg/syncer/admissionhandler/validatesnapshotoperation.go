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
	k8s "sigs.k8s.io/vsphere-csi-driver/v3/pkg/kubernetes"

	snap "github.com/kubernetes-csi/external-snapshotter/client/v8/apis/volumesnapshot/v1"
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
	log.Debugf("validateSnapshotOperationGuestRequest called with the request %v", req)
	if req.Kind.Kind == "VolumeSnapshotClass" {
		vsclass := snap.VolumeSnapshotClass{}
		log.Debugf("JSON req.Object.Raw: %v", string(req.Object.Raw))
		if err := json.Unmarshal(req.Object.Raw, &vsclass); err != nil {
			reason := "error deserializing volume snapshot class"
			log.Warn(reason)
			return admission.Denied(reason)
		}
		log.Debugf("Validating VolumeSnapshotClass: %q", vsclass.Name)
		if vsclass.Driver == "csi.vsphere.vmware.com" && !featureGateBlockVolumeSnapshotEnabled {
			// Disallow any operation on VolumeSnapshotClass object if block-volume-snapshot feature is not enabled
			return admission.Denied(SnapshotFeatureNotEnabled)
		}
	} else if req.Kind.Kind == "VolumeSnapshotContent" {
		vsc := snap.VolumeSnapshotContent{}
		log.Debugf("JSON req.Object.Raw: %v", string(req.Object.Raw))
		if err := json.Unmarshal(req.Object.Raw, &vsc); err != nil {
			reason := "error deserializing volume snapshot content"
			log.Warn(reason)
			return admission.Denied(reason)
		}
		log.Debugf("Validating VolumeSnapshotContent: %q", vsc.Name)
		if vsc.Spec.Driver == "csi.vsphere.vmware.com" && !featureGateBlockVolumeSnapshotEnabled {
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
		log.Debugf("JSON req.Object.Raw: %v", string(req.Object.Raw))
		if err := json.Unmarshal(req.Object.Raw, &vs); err != nil {
			reason := "error deserializing volume snapshot"
			log.Warn(reason)
			return admission.Denied(reason)
		}
		log.Debugf("Validating VolumeSnapshot: %q", vs.Name)
		// Disallow any operation on VolumeSnapshot object if block-volume-snapshot feature is not enable
		// If no volume snapshot class mentioned i.e. default volume snapshot class to be used, then
		// following checks are skipped. Currently vSphere driver snapshot class is not marked default.
		if *vs.Spec.VolumeSnapshotClassName != "" {
			snapshotterClient, err := k8s.NewSnapshotterClient(ctx)
			if err != nil {
				reason := fmt.Sprintf("failed to get snapshotterClient with error: %v. for class %s",
					err, *vs.Spec.VolumeSnapshotClassName)
				log.Warn(reason)
				return admission.Denied(reason)
			}
			vsclass, err := snapshotterClient.SnapshotV1().VolumeSnapshotClasses().Get(ctx,
				*vs.Spec.VolumeSnapshotClassName, metav1.GetOptions{})
			if err != nil {
				reason := fmt.Sprintf("failed to Get VolumeSnapshotclass %s with error: %v.",
					*vs.Spec.VolumeSnapshotClassName, err)
				log.Warn(reason)
				return admission.Denied(reason)
			}
			if vsclass.Driver == "csi.vsphere.vmware.com" && !featureGateBlockVolumeSnapshotEnabled {
				return admission.Denied(SnapshotFeatureNotEnabled)
			}
		}
	}
	log.Debugf("validateSnapshotOperationGuestRequest completed for the request %v", req)
	return admission.Allowed("")
}

func validateSnapshotOperationSupervisorRequest(ctx context.Context,
	req *admissionv1.AdmissionRequest) admission.Response {
	log := logger.GetLogger(ctx)
	log.Debugf("validateSnapshotOperationSupervisorRequest called with the request %v", req)

	if req.Kind.Kind == "VolumeSnapshot" {
		if !featureIsLinkedCloneSupportEnabled {
			return admission.Allowed("")
		}

		// Only apply Linked Clone validation for delete operations.
		if featureIsLinkedCloneSupportEnabled && req.Operation == admissionv1.Delete {
			vs := snap.VolumeSnapshot{}
			log.Debugf("JSON req.Object.Raw: %v", string(req.Object.Raw))
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
	log.Debugf("validateSnapshotOperationSupervisorRequest completed for the request %v", req)
	return admission.Allowed("")
}

// checkIfLinkedClonesExist checks if there are any LinkedClone volumes created out of the
// VolumeSnapshot
func checkIfLinkedClonesExist(ctx context.Context, vs snap.VolumeSnapshot) admission.Response {
	k8sClient, err := k8s.NewClient(ctx)
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
	k8sClient, err := k8s.NewClient(ctx)
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
