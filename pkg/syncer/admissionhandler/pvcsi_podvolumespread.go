/*
Copyright 2026 The Kubernetes Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package admissionhandler

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"

	corev1 "k8s.io/api/core/v1"
	"sigs.k8s.io/controller-runtime/pkg/webhook/admission"

	"sigs.k8s.io/vsphere-csi-driver/v3/pkg/csi/service/common"
	"sigs.k8s.io/vsphere-csi-driver/v3/pkg/csi/service/common/commonco"
	"sigs.k8s.io/vsphere-csi-driver/v3/pkg/csi/service/logger"
)

// podVolumeSpreadFeatureState returns the two states the guest webhook needs for pod volume
// spreading: whether the pod-volume-spread pvCSI FSS is on, and whether the feature is
// supported, i.e. the FSS is on and the supervisor has the capability paired with it. The
// capability is only looked up when the FSS is on.
func podVolumeSpreadFeatureState(ctx context.Context,
	containerOrchestratorUtility commonco.COCommonInterface) (fssEnabled, supported bool) {
	fssEnabled = containerOrchestratorUtility.IsPVCSIFSSEnabled(ctx, common.PodVolumeSpreadFSS)
	if fssEnabled {
		supported = containerOrchestratorUtility.IsFSSEnabled(ctx, common.PodVolumeSpreadFSS)
	}
	return fssEnabled, supported
}

// appendPodVolumeSpreadLateEnablement adds the supervisor capability paired with the
// pod-volume-spread FSS to the capabilities to watch for late enablement, when the FSS is on
// but the supervisor does not have the capability yet. The watcher restarts the container once
// the capability appears, so labelled pods stop being rejected without manual action.
func appendPodVolumeSpreadLateEnablement(capabilities []string, fssEnabled, supported bool) []string {
	if fssEnabled && !supported {
		return append(capabilities, common.SharedDiskFss)
	}
	return capabilities
}

// admitPodVolumeSpread handles the creation of a pod in a guest cluster when the
// pod-volume-spread pvCSI FSS is on. A pod that asks for its volumes to be spread across the
// node VM's storage controllers is rejected when the supervisor cannot honor a controller
// chosen by pvCSI: admitting it would let it run with default placement, which is exactly
// what the pod asked not to get. Every other pod is allowed unchanged.
func admitPodVolumeSpread(ctx context.Context, req admission.Request) admission.Response {
	log := logger.GetLogger(ctx)
	pod := &corev1.Pod{}
	if err := json.Unmarshal(req.Object.Raw, pod); err != nil {
		log.Errorf("failed to unmarshal pod %s/%s: %v", req.Namespace, req.Name, err)
		return admission.Errored(http.StatusBadRequest, err)
	}
	if pod.Labels[common.PodVolumePlacementLabel] != common.PodVolumePlacementSpread {
		return admission.Allowed("")
	}
	if !featureIsPodVolumeSpreadSupported {
		msg := fmt.Sprintf("pod has label %s=%s, but volumes cannot be spread across storage controllers "+
			"in this cluster: the supervisor does not have the %q capability",
			common.PodVolumePlacementLabel, common.PodVolumePlacementSpread, common.SharedDiskFss)
		log.Errorf("rejecting pod %s/%s: %s", req.Namespace, req.Name, msg)
		return admission.Denied(msg)
	}
	return admission.Allowed("")
}
