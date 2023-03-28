package admissionhandler

import (
	"context"
	"encoding/json"
	"fmt"

	admissionv1 "k8s.io/api/admission/v1"
	"sigs.k8s.io/controller-runtime/pkg/webhook/admission"

	"sigs.k8s.io/vsphere-csi-driver/v3/pkg/csi/service/logger"

	snap "github.com/kubernetes-csi/external-snapshotter/client/v6/apis/volumesnapshot/v1"
)

const (
	SnapshotOperationNotAllowed = "Snapshot operation initiated directly from the Supervisor cluster by user " +
		"%s is not supported. Please initiate snapshot operation from the Guest cluster."
)

// Disallow any opertion on volume snapshot initiated by user directly on the supervisor cluster.
// Currently we only allow snapshot operation initiated from the guest cluster.
func validateSnapshotOperationSupervisorRequest(ctx context.Context, request admission.Request) admission.Response {
	log := logger.GetLogger(ctx)
	username := request.UserInfo.Username
	isCSIServiceAccount := validateCSIServiceAccount(request.UserInfo.Username)
	log.Debugf("validateSnapshotOperationSupervisorRequest called with the request %v by user: %v", request, username)
	newVS := snap.VolumeSnapshot{}
	if err := json.Unmarshal(request.Object.Raw, &newVS); err != nil {
		reason := "Failed to deserialize VolumeSnapshot from new request object"
		log.Warn(reason)
		return admission.Denied(reason)
	}

	if request.Operation == admissionv1.Create || request.Operation == admissionv1.Update ||
		request.Operation == admissionv1.Delete {
		if !isCSIServiceAccount {
			return admission.Denied(fmt.Sprintf(SnapshotOperationNotAllowed, username))
		}
	}

	log.Debugf("validateSnapshotOperationSupervisorRequest completed for the request %v", request)
	return admission.Allowed("")
}
