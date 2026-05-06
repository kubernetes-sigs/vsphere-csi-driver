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

// ControllerExpandVolume handling for the new vSAN FileVolumeService (FVS)
// workflow on the guest cluster.
//
// Background:
// For legacy CNS file volumes, volume expansion is not supported (the storage
// backend does not allow it, and there is no node-side filesystem resize step).
//
// For FVS-backed file volumes the supervisor controller drives the resize by
// updating the FileVolume CR when the supervisor PVC's
// spec.resources.requests.storage is increased (see the supervisor-side
// expandFileVolumeViaFVS). The guest cluster's role is:
//   1. Patch spec.resources.requests.storage on the supervisor PVC.
//   2. Wait until status.capacity reflects the new size (set by the supervisor
//      FVS controller once the underlying NFS share has grown).
//   3. Return NodeExpansionRequired=false — NFS shares grow on the server side
//      so no kubelet filesystem resize step is needed on the guest node.
//
// Identification: the controller must gate on IsVsanFileVolumeServiceEnabled
// and common.IsFVSPersistentVolumeClaim(supervisor PVC) before calling the
// helpers in this file.

package wcpguest

import (
	"context"
	"fmt"
	"time"

	"github.com/container-storage-interface/spec/lib/go/csi"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/fields"
	clientset "k8s.io/client-go/kubernetes"
	ctrlclient "sigs.k8s.io/controller-runtime/pkg/client"

	csifault "sigs.k8s.io/vsphere-csi-driver/v3/pkg/common/fault"
	"sigs.k8s.io/vsphere-csi-driver/v3/pkg/csi/service/logger"
	k8s "sigs.k8s.io/vsphere-csi-driver/v3/pkg/kubernetes"
)

// controllerExpandForFVSFileVolume handles a ControllerExpandVolume request for
// a file volume provisioned by the new vSAN FileVolumeService. It patches the
// supervisor PVC's spec.resources.requests.storage to trigger the supervisor-side
// FVS expansion reconciler, then waits for status.capacity to reflect the new
// size before returning success with NodeExpansionRequired=false.
//
// The caller must have verified FVS (IsVsanFileVolumeServiceEnabled &&
// common.IsFVSPersistentVolumeClaim on the supervisor PVC) before invoking this
// function.
func controllerExpandForFVSFileVolume(ctx context.Context,
	req *csi.ControllerExpandVolumeRequest,
	svPVC *corev1.PersistentVolumeClaim,
	c *controller) (*csi.ControllerExpandVolumeResponse, string, error) {

	log := logger.GetLogger(ctx)
	volumeID := req.GetVolumeId()
	volSizeBytes := int64(req.GetCapacityRange().GetRequiredBytes())
	gcPvcRequestSize := resource.NewQuantity(volSizeBytes, resource.BinarySI)
	svPvcRequestSize := svPVC.Spec.Resources.Requests[corev1.ResourceStorage]
	timeout := time.Duration(getResizeTimeoutInMin(ctx)) * time.Minute

	switch gcPvcRequestSize.Cmp(svPvcRequestSize) {
	case 1:
		// Requested size is larger than the current supervisor PVC spec: patch it
		// so the FVS expansion reconciler on the supervisor can act on it.
		original := svPVC.DeepCopy()
		clone := svPVC.DeepCopy()
		clone.Spec.Resources.Requests[corev1.ResourceStorage] = *gcPvcRequestSize

		supervisorRuntimeClient := c.supervisorRuntimeClient
		if supervisorRuntimeClient == nil {
			rc, rcErr := ctrlclient.New(c.restClientConfig, ctrlclient.Options{})
			if rcErr != nil {
				msg := fmt.Sprintf("failed to create controller-runtime client for supervisor cluster. Error: %+v", rcErr)
				log.Error(msg)
				return nil, csifault.CSIInternalFault, status.Error(codes.Internal, msg)
			}
			supervisorRuntimeClient = rc
		}
		log.Infof("ControllerExpandVolume: increasing supervisor PVC %s/%s from %s to %s for FVS file volume",
			c.supervisorNamespace, volumeID, svPvcRequestSize.String(), gcPvcRequestSize.String())
		if patchErr := k8s.PatchObject(ctx, supervisorRuntimeClient, original, clone); patchErr != nil {
			msg := fmt.Sprintf("failed to patch supervisor PVC %q in %q namespace. Error: %+v",
				volumeID, c.supervisorNamespace, patchErr)
			log.Error(msg)
			return nil, csifault.CSIInternalFault, status.Error(codes.Internal, msg)
		}
		// Wait for the FVS controller to reflect the new size in status.capacity.
		if waitErr := checkForSupervisorPVCCapacityReached(ctx, c.supervisorClient, clone,
			gcPvcRequestSize, timeout); waitErr != nil {
			msg := fmt.Sprintf("failed to expand FVS file volume %s: %+v", volumeID, waitErr)
			log.Error(msg)
			return nil, csifault.CSIInternalFault, status.Error(codes.Internal, msg)
		}
	case 0:
		// Supervisor PVC spec already at the requested size. If status.capacity
		// has not caught up yet (e.g. a previous attempt patched the spec but
		// timed out waiting), wait for convergence.
		svPvcCapacity := svPVC.Status.Capacity[corev1.ResourceStorage]
		if gcPvcRequestSize.Cmp(svPvcCapacity) > 0 {
			log.Infof("ControllerExpandVolume: supervisor PVC %s/%s spec already at %s but "+
				"status.capacity is %s; waiting for FVS controller to converge",
				c.supervisorNamespace, volumeID, gcPvcRequestSize.String(), svPvcCapacity.String())
			if err := checkForSupervisorPVCCapacityReached(ctx, c.supervisorClient, svPVC,
				gcPvcRequestSize, timeout); err != nil {
				msg := fmt.Sprintf("failed to expand FVS file volume %s: %+v", volumeID, err)
				log.Error(msg)
				return nil, csifault.CSIInternalFault, status.Error(codes.Internal, msg)
			}
		} else {
			log.Infof("ControllerExpandVolume: FVS file volume %s already at requested size %s; skipping resize",
				volumeID, gcPvcRequestSize.String())
		}
	default:
		// Requested size is smaller than current supervisor PVC spec: reject.
		msg := fmt.Sprintf("the requested size of the Supervisor PVC %s in namespace %s is %s "+
			"which is greater than the requested size of %s",
			volumeID, c.supervisorNamespace, svPvcRequestSize.String(), gcPvcRequestSize.String())
		log.Error(msg)
		return nil, csifault.CSIInternalFault, status.Error(codes.InvalidArgument, msg)
	}

	log.Infof("ControllerExpandVolume: FVS file volume %s expanded to %s; NodeExpansionRequired=false",
		volumeID, gcPvcRequestSize.String())
	return &csi.ControllerExpandVolumeResponse{
		CapacityBytes:         volSizeBytes,
		NodeExpansionRequired: false,
	}, "", nil
}

// checkForSupervisorPVCCapacityReached watches the supervisor PVC and returns
// nil when status.capacity.storage reaches reqSize, or an error if timeout
// elapses first. Unlike checkForSupervisorPVCCondition (which waits for the
// FileSystemResizePending condition used by block volumes), this helper is used
// for FVS file volumes whose NFS share grows on the server side; the supervisor
// FVS controller signals completion by updating status.capacity rather than
// setting a filesystem-resize condition.
func checkForSupervisorPVCCapacityReached(ctx context.Context, svClient clientset.Interface,
	claim *corev1.PersistentVolumeClaim, reqSize *resource.Quantity, timeout time.Duration) error {

	log := logger.GetLogger(ctx)
	pvcName := claim.Name
	ns := claim.Namespace
	timeoutSeconds := int64(timeout.Seconds())

	log.Infof("Waiting up to %d seconds for supervisor PVC %s/%s status.capacity to reach %s",
		timeoutSeconds, ns, pvcName, reqSize.String())

	watchClaim, err := svClient.CoreV1().PersistentVolumeClaims(ns).Watch(
		ctx,
		metav1.ListOptions{
			FieldSelector:  fields.OneTermEqualSelector("metadata.name", pvcName).String(),
			TimeoutSeconds: &timeoutSeconds,
			Watch:          true,
		})
	if err != nil {
		return fmt.Errorf("failed to watch supervisor PVC %s/%s: %w", ns, pvcName, err)
	}
	defer watchClaim.Stop()

	for {
		select {
		case <-ctx.Done():
			return fmt.Errorf("context cancelled waiting for supervisor PVC %s/%s status.capacity to reach %s: %v",
				ns, pvcName, reqSize.String(), ctx.Err())
		case event, ok := <-watchClaim.ResultChan():
			if !ok {
				return fmt.Errorf("supervisor PVC %s/%s status.capacity did not reach %s within %d seconds",
					ns, pvcName, reqSize.String(), timeoutSeconds)
			}
			pvc, ok := event.Object.(*corev1.PersistentVolumeClaim)
			if !ok {
				continue
			}
			cap := pvc.Status.Capacity[corev1.ResourceStorage]
			if cap.Cmp(*reqSize) >= 0 {
				log.Infof("Supervisor PVC %s/%s status.capacity reached %s (requested %s)",
					ns, pvcName, cap.String(), reqSize.String())
				return nil
			}
			log.Debugf("Supervisor PVC %s/%s status.capacity is %s, waiting for %s",
				ns, pvcName, cap.String(), reqSize.String())
		}
	}
}
