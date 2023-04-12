package admissionhandler

import (
	"context"
	"encoding/json"

	admissionv1 "k8s.io/api/admission/v1"
	"sigs.k8s.io/controller-runtime/pkg/webhook/admission"

	"sigs.k8s.io/vsphere-csi-driver/v3/pkg/csi/service/common"
	"sigs.k8s.io/vsphere-csi-driver/v3/pkg/csi/service/logger"

	snap "github.com/kubernetes-csi/external-snapshotter/client/v6/apis/volumesnapshot/v1"
)

const (
	SnapshotOperationNotAllowed = "Snapshot creation initiated directly from the Supervisor cluster " +
		"is not supported. Please initiate snapshot creation from the Guest cluster."
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
		if _, annotationFound := newVS.Annotations[common.SupervisorVolumeSnapshotAnnotationKey]; !annotationFound {
			return admission.Denied(SnapshotOperationNotAllowed)
		}
	}

	log.Debugf("validateSnapshotOperationSupervisorRequest completed for the request %v", request)
	return admission.Allowed("")
}
