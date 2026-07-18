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

package vksregistervolume

import (
	"context"
	"fmt"
	"time"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	clientset "k8s.io/client-go/kubernetes"
	"sigs.k8s.io/controller-runtime/pkg/client"

	vksregistervolumev1alpha1 "sigs.k8s.io/vsphere-csi-driver/v3/pkg/apis/cnsoperator/vksregistervolume/v1alpha1"
	"sigs.k8s.io/vsphere-csi-driver/v3/pkg/csi/service/common"
	"sigs.k8s.io/vsphere-csi-driver/v3/pkg/csi/service/logger"
)

const (
	// pvcMissingTimeout is the bounded window during which a missing guest PVC is treated as transient.
	// ss-metadata-manager may create the PVC and the VKSRegisterVolume CR concurrently; after this
	// window the PVC is assumed to be a permanently broken reference and the CR is Failed.
	pvcMissingTimeout = 10 * time.Minute
)

// validateSpec validates the required spec fields of a VKSRegisterVolume.
// Both reference fields must be non-empty; a missing field is always a terminal failure
// (the CR was created with a bad spec and retrying will not fix it).
func validateSpec(ctx context.Context, spec *vksregistervolumev1alpha1.VKSRegisterVolumeSpec) error {
	if spec.PVCName == "" {
		return fmt.Errorf("spec.pvcName must not be empty")
	}
	if spec.CnsRegisterVolumeName == "" {
		return fmt.Errorf("spec.cnsRegisterVolumeName must not be empty")
	}
	return nil
}

// resolveGuestPVC retrieves the guest PVC referenced by the VKSRegisterVolume and validates it.
//
// Return semantics:
//   - (pvc, false, nil)   – PVC is valid and ready; caller may proceed.
//   - (nil, false, err)   – transient error; caller should requeue with backoff.
//   - (nil, true,  err)   – terminal error; caller should set Phase=Failed with no requeue.
//
// The PVC must satisfy all of the following:
//   - exists in the same namespace as the CR
//   - not yet Bound (Phase == Pending; ss-metadata-manager creates it in Pending)
//   - spec.volumeName is set (the pre-chosen guest PV name)
//   - spec.accessModes is non-empty
//   - spec.resources.requests[storage] > 0
//   - spec.storageClassName is set and that StorageClass exists
//   - the StorageClass carries the svstorageclass (common.AttributeSupervisorStorageClass) parameter
//
// If the PVC is not found and the CR was created within pvcMissingTimeout, the error is transient
// (ss-metadata-manager may not have created the PVC yet). After the timeout it becomes terminal.
func resolveGuestPVC(
	ctx context.Context,
	c client.Client,
	k8sclient clientset.Interface,
	instance *vksregistervolumev1alpha1.VKSRegisterVolume,
) (*corev1.PersistentVolumeClaim, bool, error) {
	log := logger.GetLogger(ctx)

	pvcKey := types.NamespacedName{
		Namespace: instance.Namespace,
		Name:      instance.Spec.PVCName,
	}
	pvc := &corev1.PersistentVolumeClaim{}
	if err := c.Get(ctx, pvcKey, pvc); err != nil {
		if !apierrors.IsNotFound(err) {
			return nil, false, fmt.Errorf("failed to GET guest PVC %s/%s: %w", instance.Namespace, instance.Spec.PVCName, err)
		}
		// PVC not found — transient if within the bounded window, terminal otherwise.
		age := crAge(instance)
		if age <= pvcMissingTimeout {
			log.Infof("Guest PVC %s/%s not found after %v (within %v window); will retry",
				instance.Namespace, instance.Spec.PVCName, age.Truncate(time.Second), pvcMissingTimeout)
			return nil, false, fmt.Errorf("guest PVC %s/%s not found (waited %v)",
				instance.Namespace, instance.Spec.PVCName, age.Truncate(time.Second))
		}
		return nil, true, fmt.Errorf("guest PVC %s/%s not found after %v (exceeded %v timeout); "+
			"ss-metadata-manager must create the PVC before creating the VKSRegisterVolume CR",
			instance.Namespace, instance.Spec.PVCName, age.Truncate(time.Second), pvcMissingTimeout)
	}

	// PVC must not be Bound yet — ss-metadata-manager creates it in Pending.
	// A Bound PVC means another PV already claimed it; fail terminally.
	if pvc.Status.Phase == corev1.ClaimBound {
		return nil, true, fmt.Errorf("guest PVC %s/%s is already Bound (to PV %q); "+
			"cannot import volume into an already-bound PVC",
			instance.Namespace, instance.Spec.PVCName, pvc.Spec.VolumeName)
	}

	// spec.volumeName must be set — this is the pre-chosen guest PV name.
	if pvc.Spec.VolumeName == "" {
		return nil, true, fmt.Errorf("guest PVC %s/%s has empty spec.volumeName; "+
			"ss-metadata-manager must set spec.volumeName to the future PV name",
			instance.Namespace, instance.Spec.PVCName)
	}

	// At least one accessMode is required (sourced from the PVC for the guest PV).
	if len(pvc.Spec.AccessModes) == 0 {
		return nil, true, fmt.Errorf("guest PVC %s/%s has no accessModes",
			instance.Namespace, instance.Spec.PVCName)
	}

	// A positive storage request is required (sourced from the PVC for the guest PV capacity).
	req, ok := pvc.Spec.Resources.Requests[corev1.ResourceStorage]
	if !ok || req.Cmp(resource.MustParse("0")) <= 0 {
		return nil, true, fmt.Errorf("guest PVC %s/%s has no positive storage request",
			instance.Namespace, instance.Spec.PVCName)
	}

	// storageClassName must be set.
	if pvc.Spec.StorageClassName == nil || *pvc.Spec.StorageClassName == "" {
		return nil, true, fmt.Errorf("guest PVC %s/%s has no storageClassName; "+
			"the StorageClass must carry the %q parameter",
			instance.Namespace, instance.Spec.PVCName, common.AttributeSupervisorStorageClass)
	}
	scName := *pvc.Spec.StorageClassName

	// The StorageClass must exist and must carry the svstorageclass parameter.
	sc, err := k8sclient.StorageV1().StorageClasses().Get(ctx, scName, metav1.GetOptions{})
	if err != nil {
		if apierrors.IsNotFound(err) {
			return nil, true, fmt.Errorf("StorageClass %q referenced by PVC %s/%s not found",
				scName, instance.Namespace, instance.Spec.PVCName)
		}
		// API error: treat as transient.
		return nil, false, fmt.Errorf("failed to GET StorageClass %q: %w", scName, err)
	}
	if _, hasSVSC := sc.Parameters[common.AttributeSupervisorStorageClass]; !hasSVSC {
		return nil, true, fmt.Errorf("StorageClass %q referenced by PVC %s/%s does not have %q parameter; "+
			"only guest StorageClasses backed by a Supervisor StorageClass are supported",
			scName, instance.Namespace, instance.Spec.PVCName, common.AttributeSupervisorStorageClass)
	}

	log.Infof("Guest PVC %s/%s resolved: volumeName=%q storageClass=%q accessModes=%v storage=%s",
		instance.Namespace, instance.Spec.PVCName,
		pvc.Spec.VolumeName, scName, pvc.Spec.AccessModes, req.String())
	return pvc, false, nil
}

// defaultVolumeMode returns the effective PersistentVolumeMode for the guest PV,
// defaulting to Filesystem when the PVC's volumeMode is nil or empty.
func defaultVolumeMode(mode *corev1.PersistentVolumeMode) corev1.PersistentVolumeMode {
	if mode == nil || *mode == "" {
		return corev1.PersistentVolumeFilesystem
	}
	return *mode
}

// crAge returns how long ago the VKSRegisterVolume CR was created.
// Returns a large duration if CreationTimestamp is zero (e.g. in unit tests that don't set it).
func crAge(instance *vksregistervolumev1alpha1.VKSRegisterVolume) time.Duration {
	ts := instance.CreationTimestamp
	if ts.IsZero() {
		return pvcMissingTimeout + time.Hour // treat zero timestamp as "very old"
	}
	return time.Since(ts.Time)
}
