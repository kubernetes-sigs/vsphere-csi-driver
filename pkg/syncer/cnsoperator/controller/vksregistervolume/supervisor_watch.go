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
	"encoding/json"
	"fmt"
	"time"

	"github.com/container-storage-interface/spec/lib/go/csi"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	clientset "k8s.io/client-go/kubernetes"
	"sigs.k8s.io/controller-runtime/pkg/client"

	cnsregistervolumev1alpha1 "sigs.k8s.io/vsphere-csi-driver/v3/pkg/apis/cnsoperator/cnsregistervolume/v1alpha1"
	vksregistervolumev1alpha1 "sigs.k8s.io/vsphere-csi-driver/v3/pkg/apis/cnsoperator/vksregistervolume/v1alpha1"
	"sigs.k8s.io/vsphere-csi-driver/v3/pkg/csi/service/common"
	"sigs.k8s.io/vsphere-csi-driver/v3/pkg/csi/service/logger"
)

const (
	// supervisorCRMissingTimeout is the bounded window during which a missing Supervisor
	// CnsRegisterVolume is treated as transient. The caller may create the guest CR and the
	// Supervisor CR concurrently, or the CR may not yet be visible through the local informer
	// cache; after this window the reference is assumed to be permanently broken.
	supervisorCRMissingTimeout = 10 * time.Minute
)

// waitForSupervisorRegistration resolves the Supervisor CnsRegisterVolume referenced by the
// VKSRegisterVolume instance and checks whether it has completed registration.
//
// Return semantics (mirrors resolveGuestPVC):
//   - (pvcName, false, nil) – Supervisor CR reports Registered==true; pvcName is its spec.pvcName,
//     which becomes the guest PV's csi.volumeHandle. Caller may proceed to WaitingForSupervisorBinding.
//   - ("", false, err)      – transient; caller should requeue with backoff.
//   - ("", true,  err)      – terminal; caller should set Phase=Failed with no requeue.
//
// A VKS cluster can only ever access resources in its own Supervisor namespace (r.supervisorNamespace),
// so instance.Spec.CnsRegisterVolumeName is looked up there without a separate namespace field.
func waitForSupervisorRegistration(
	ctx context.Context,
	supervisorCnsOperatorClient client.Client,
	supervisorNamespace string,
	instance *vksregistervolumev1alpha1.VKSRegisterVolume,
) (string, bool, error) {
	log := logger.GetLogger(ctx)

	crKey := types.NamespacedName{
		Namespace: supervisorNamespace,
		Name:      instance.Spec.CnsRegisterVolumeName,
	}
	supervisorCR := &cnsregistervolumev1alpha1.CnsRegisterVolume{}
	if err := supervisorCnsOperatorClient.Get(ctx, crKey, supervisorCR); err != nil {
		if !apierrors.IsNotFound(err) {
			return "", false, fmt.Errorf("failed to GET supervisor CnsRegisterVolume %s/%s: %w",
				supervisorNamespace, instance.Spec.CnsRegisterVolumeName, err)
		}
		// Supervisor CR not found — transient if within the bounded window, terminal otherwise.
		age := crAge(instance)
		if age <= supervisorCRMissingTimeout {
			log.Infof("Supervisor CnsRegisterVolume %s/%s not found after %v (within %v window); will retry",
				supervisorNamespace, instance.Spec.CnsRegisterVolumeName, age.Truncate(time.Second),
				supervisorCRMissingTimeout)
			return "", false, fmt.Errorf("supervisor CnsRegisterVolume %s/%s not found (waited %v)",
				supervisorNamespace, instance.Spec.CnsRegisterVolumeName, age.Truncate(time.Second))
		}
		return "", true, fmt.Errorf("supervisor CnsRegisterVolume %s/%s not found after %v (exceeded %v timeout); "+
			"the referenced CnsRegisterVolume must exist before the VKSRegisterVolume CR is created",
			supervisorNamespace, instance.Spec.CnsRegisterVolumeName, age.Truncate(time.Second),
			supervisorCRMissingTimeout)
	}

	if supervisorCR.Status.Error != "" {
		return "", true, fmt.Errorf("supervisor CnsRegisterVolume %s/%s failed registration: %s",
			supervisorNamespace, instance.Spec.CnsRegisterVolumeName, supervisorCR.Status.Error)
	}

	if !supervisorCR.Status.Registered {
		log.Infof("Supervisor CnsRegisterVolume %s/%s not yet registered; will retry",
			supervisorNamespace, instance.Spec.CnsRegisterVolumeName)
		return "", false, fmt.Errorf("supervisor CnsRegisterVolume %s/%s not yet registered",
			supervisorNamespace, instance.Spec.CnsRegisterVolumeName)
	}

	if supervisorCR.Spec.PvcName == "" {
		return "", true, fmt.Errorf("supervisor CnsRegisterVolume %s/%s reports Registered=true "+
			"but spec.pvcName is empty", supervisorNamespace, instance.Spec.CnsRegisterVolumeName)
	}

	log.Infof("Supervisor CnsRegisterVolume %s/%s registered: supervisorPVCName=%q",
		supervisorNamespace, instance.Spec.CnsRegisterVolumeName, supervisorCR.Spec.PvcName)
	return supervisorCR.Spec.PvcName, false, nil
}

// waitForSupervisorBinding resolves the Supervisor PVC (named supervisorPVCName, the future guest
// PV's csi.volumeHandle) and waits for it to reach Bound. Once Bound it also derives the volume's
// accessible topology, used by T7 to set node affinity on the guest PV it creates.
//
// Return semantics:
//   - (topology, false, nil) – Supervisor PVC is Bound; topology may be nil if the volume is not
//     zone-restricted (single-zone/non-stretched Supervisor) or the annotation has not yet
//     propagated — a nil topology is not an error, since the guest-side fullsync backfill
//     (AddNodeAffinityRulesOnPV) later corrects a guest PV created with no node affinity.
//   - (nil, false, err)      – transient; caller should requeue with backoff.
//   - (nil, true,  err)      – terminal; caller should set Phase=Failed with no requeue.
func waitForSupervisorBinding(
	ctx context.Context,
	supervisorClient clientset.Interface,
	supervisorNamespace string,
	supervisorPVCName string,
) ([]*csi.Topology, bool, error) {
	log := logger.GetLogger(ctx)

	supervisorPVC, err := supervisorClient.CoreV1().PersistentVolumeClaims(supervisorNamespace).
		Get(ctx, supervisorPVCName, metav1.GetOptions{})
	if err != nil {
		if apierrors.IsNotFound(err) {
			// The Supervisor CnsRegisterVolume already reported Registered==true, so its PVC
			// must exist; a NotFound here is unexpected and not a "just hasn't been created yet"
			// race — treat as terminal rather than retrying indefinitely.
			return nil, true, fmt.Errorf("supervisor PVC %s/%s not found even though its "+
				"CnsRegisterVolume reported Registered=true", supervisorNamespace, supervisorPVCName)
		}
		return nil, false, fmt.Errorf("failed to GET supervisor PVC %s/%s: %w",
			supervisorNamespace, supervisorPVCName, err)
	}

	if supervisorPVC.Status.Phase != corev1.ClaimBound {
		log.Infof("Supervisor PVC %s/%s not yet Bound (phase=%q); will retry",
			supervisorNamespace, supervisorPVCName, supervisorPVC.Status.Phase)
		return nil, false, fmt.Errorf("supervisor PVC %s/%s not yet Bound",
			supervisorNamespace, supervisorPVCName)
	}

	topology, err := accessibleTopologyFromSupervisorPVC(ctx, supervisorClient, supervisorPVC)
	if err != nil {
		// Topology is a best-effort read: log and proceed with nil rather than blocking
		// registration on it. AddNodeAffinityRulesOnPV backfills node affinity later.
		log.Warnf("Could not determine accessible topology for supervisor PVC %s/%s; "+
			"guest PV will be created without node affinity and backfilled later. Err: %v",
			supervisorNamespace, supervisorPVCName, err)
		return nil, false, nil
	}

	return topology, false, nil
}

// accessibleTopologyFromSupervisorPVC returns the accessible topology for a Bound Supervisor PVC,
// preferring the csi.vsphere.volume-accessible-topology annotation on the PVC itself and falling
// back to the Supervisor PV's spec.nodeAffinity when the annotation is absent. Returns (nil, nil)
// when neither source has topology information (single-zone/non-stretched Supervisor).
func accessibleTopologyFromSupervisorPVC(
	ctx context.Context,
	supervisorClient clientset.Interface,
	supervisorPVC *corev1.PersistentVolumeClaim,
) ([]*csi.Topology, error) {
	if annotation := supervisorPVC.Annotations[common.AnnVolumeAccessibleTopology]; annotation != "" {
		segments, err := parseVolumeAccessibleTopologyAnnotation(annotation)
		if err != nil {
			return nil, fmt.Errorf("failed to parse %q annotation on supervisor PVC %s/%s: %w",
				common.AnnVolumeAccessibleTopology, supervisorPVC.Namespace, supervisorPVC.Name, err)
		}
		return toCSITopology(segments), nil
	}

	// Annotation absent — fall back to the Supervisor PV's node affinity, if any.
	supervisorPV, err := supervisorClient.CoreV1().PersistentVolumes().
		Get(ctx, supervisorPVC.Spec.VolumeName, metav1.GetOptions{})
	if err != nil {
		return nil, fmt.Errorf("failed to GET supervisor PV %q: %w", supervisorPVC.Spec.VolumeName, err)
	}
	if supervisorPV.Spec.NodeAffinity == nil || supervisorPV.Spec.NodeAffinity.Required == nil {
		return nil, nil
	}

	var topology []*csi.Topology
	for _, term := range supervisorPV.Spec.NodeAffinity.Required.NodeSelectorTerms {
		segments := make(map[string]string, len(term.MatchExpressions))
		for _, expr := range term.MatchExpressions {
			if len(expr.Values) > 0 {
				segments[expr.Key] = expr.Values[0]
			}
		}
		if len(segments) > 0 {
			topology = append(topology, &csi.Topology{Segments: segments})
		}
	}
	return topology, nil
}

// parseVolumeAccessibleTopologyAnnotation unmarshals the csi.vsphere.volume-accessible-topology
// annotation value into its underlying segment-map form.
func parseVolumeAccessibleTopologyAnnotation(annotation string) ([]map[string]string, error) {
	var segments []map[string]string
	if err := json.Unmarshal([]byte(annotation), &segments); err != nil {
		return nil, err
	}
	return segments, nil
}

// toCSITopology converts parsed annotation segments into the []*csi.Topology shape expected by
// GenerateVolumeNodeAffinity.
func toCSITopology(segments []map[string]string) []*csi.Topology {
	var topology []*csi.Topology
	for _, s := range segments {
		topology = append(topology, &csi.Topology{Segments: s})
	}
	return topology
}
