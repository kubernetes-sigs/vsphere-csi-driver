package admissionhandler

import (
	"context"
	"encoding/json"
	"fmt"

	admissionv1 "k8s.io/api/admission/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/webhook/admission"

	"sigs.k8s.io/vsphere-csi-driver/v3/pkg/csi/service/common"
	"sigs.k8s.io/vsphere-csi-driver/v3/pkg/csi/service/logger"
	k8s "sigs.k8s.io/vsphere-csi-driver/v3/pkg/kubernetes"

	snap "github.com/kubernetes-csi/external-snapshotter/client/v6/apis/volumesnapshot/v1"
)

const (
	SnapshotOperationNotAllowed = "Snapshot creation initiated directly from the Supervisor cluster " +
		"is not supported. Please initiate snapshot creation from the Guest cluster."
	SnapshotFeatureNotEnabled = "CSI Snapshot feature is not supported for vSphere driver on either " +
		"Supervisor cluster or Guest cluster. Please upgrade the appropriate cluster to get support for CSI snapshot feature."
)

// Disallow any opertion on volume snapshot initiated by user directly on the supervisor cluster.
// Currently we only allow snapshot operation initiated from the guest cluster.
func validateSnapshotOperationSupervisorRequest(ctx context.Context, request admission.Request) admission.Response {
	log := logger.GetLogger(ctx)
	log.Debugf("validateSnapshotOperationSupervisorRequest called with the request %v", request)
	newVS := snap.VolumeSnapshot{}
	if err := json.Unmarshal(request.Object.Raw, &newVS); err != nil {
		reason := "Failed to deserialize VolumeSnapshot from new request object"
		log.Warn(reason)
		return admission.Denied(reason)
	}

	if request.Operation == admissionv1.Create {
		// NOTE: Change to allow snapshot creation from supervisor directly with StorageQuotaM2 FSS enabled
		//        is temporary & will be reverted before release
		if featureGateStorageQuotaM2Enabled {
			log.Debugf("validateSnapshotOperationSupervisorRequest Allow %v since StorageQuotaM2 FSS enabled", request)
			return admission.Allowed("")
		}
		if _, annotationFound := newVS.Annotations[common.SupervisorVolumeSnapshotAnnotationKey]; !annotationFound {
			return admission.Denied(SnapshotOperationNotAllowed)
		}
	}

	log.Debugf("validateSnapshotOperationSupervisorRequest completed for the request %v", request)
	return admission.Allowed("")
}

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
