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

// Package cnsnfsvolumeinformation maintains the CnsNfsVolumeInformation CR that
// aggregates every guest-local-NFS-backed volume for a VKS cluster into one object in
// the Supervisor Namespace. Both pvCSI (on CreateVolume/DeleteVolume) and the pvCSI
// syncer (on PVC/PV/Pod events) write to it, so the upsert/remove logic lives here
// rather than in either caller's own package.
package cnsnfsvolumeinformation

import (
	"context"
	"encoding/json"
	"fmt"

	"k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"

	cnsnfsvolumeinformationv1alpha1 "sigs.k8s.io/vsphere-csi-driver/v3/pkg/apis/cnsoperator/cnsnfsvolumeinformation/v1alpha1"
	"sigs.k8s.io/vsphere-csi-driver/v3/pkg/csi/service/logger"
)

// CRName returns the name of the single CnsNfsVolumeInformation CR that aggregates
// every NFS-backed volume for a given VKS cluster, following the
// "<vksClusterName>-<vksClusterID>-nfsvolumes" naming convention.
func CRName(vksClusterName, vksClusterID string) string {
	return fmt.Sprintf("%s-%s-nfsvolumes", vksClusterName, vksClusterID)
}

// UpsertVolumeEntry adds or overwrites a single entry in the CnsNfsVolumeInformation
// CR's Spec.Volumes map, keyed by pvcKey (the guest PVC's UID). It does this with a JSON
// merge patch rather than a Get-then-Update: since Spec.Volumes aggregates every NFS
// volume in the VKS cluster into one shared object, concurrent callers acting on
// different PVCs in the same cluster (pvCSI's CreateVolume and the syncer's PVC/PV
// watch loop both call this) would otherwise race on the same object's
// resourceVersion. A merge patch touching only this one map key is applied by the API
// server against whatever the current object is, so two concurrent patches for two
// different pvcKeys never conflict with each other.
//
// If the CR does not exist yet (this is the first NFS volume for the cluster), it is
// created with this entry as its sole volume. If Create loses a race against another
// concurrent caller doing the same thing, the patch is retried once against the
// now-existing CR.
func UpsertVolumeEntry(ctx context.Context, c client.Client, name, namespace, pvcKey string,
	entry cnsnfsvolumeinformationv1alpha1.NfsVolumeEntry) error {

	log := logger.GetLogger(ctx)

	patchBytes, err := json.Marshal(map[string]interface{}{
		"spec": map[string]interface{}{
			"volumes": map[string]interface{}{pvcKey: entry},
		},
	})
	if err != nil {
		return logger.LogNewErrorf(log, "failed to marshal merge patch for CnsNfsVolumeInformation %s/%s, "+
			"volume key %q. Error: %+v", namespace, name, pvcKey, err)
	}
	patch := client.RawPatch(types.MergePatchType, patchBytes)

	obj := &cnsnfsvolumeinformationv1alpha1.CnsNfsVolumeInformation{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: namespace},
	}
	err = c.Patch(ctx, obj, patch)
	if err == nil {
		log.Infof("Upserted volume key %q on CnsNfsVolumeInformation %s/%s", pvcKey, namespace, name)
		return nil
	}
	if !errors.IsNotFound(err) {
		return logger.LogNewErrorf(log, "failed to patch CnsNfsVolumeInformation %s/%s with volume key %q. "+
			"Error: %+v", namespace, name, pvcKey, err)
	}

	// CR does not exist yet - create it with this volume as the first entry.
	obj.Spec = cnsnfsvolumeinformationv1alpha1.CnsNfsVolumeInformationSpec{
		Volumes: map[string]cnsnfsvolumeinformationv1alpha1.NfsVolumeEntry{pvcKey: entry},
	}
	err = c.Create(ctx, obj)
	if err == nil {
		log.Infof("Created CnsNfsVolumeInformation %s/%s with initial volume key %q", namespace, name, pvcKey)
		return nil
	}
	if !errors.IsAlreadyExists(err) {
		return logger.LogNewErrorf(log, "failed to create CnsNfsVolumeInformation %s/%s with volume key %q. "+
			"Error: %+v", namespace, name, pvcKey, err)
	}

	// Lost the create race against a concurrent caller - the CR exists now, patch it.
	obj = &cnsnfsvolumeinformationv1alpha1.CnsNfsVolumeInformation{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: namespace},
	}
	if err := c.Patch(ctx, obj, patch); err != nil {
		return logger.LogNewErrorf(log, "failed to patch CnsNfsVolumeInformation %s/%s with volume key %q "+
			"after losing create race. Error: %+v", namespace, name, pvcKey, err)
	}
	log.Infof("Upserted volume key %q on CnsNfsVolumeInformation %s/%s after losing create race",
		pvcKey, namespace, name)
	return nil
}

// PatchVolumeEntryHealthAndPods patches only the health, podNames, and lastUpdatedTime
// sub-fields of an existing entry (keyed by pvcKey), leaving every other field
// (claimName, nfsSharePath, labels, capacity, ...) untouched. This is deliberately
// narrower than UpsertVolumeEntry: the caller here (a Pod add/update/delete handler)
// only ever knows about health/podNames, and a full-entry replace built from a
// zero-valued NfsVolumeEntry would silently drop those other fields - or, worse, since
// PodNames has `omitempty`, silently fail to clear podNames back to empty once the last
// pod detaches, since an empty/nil slice would simply be omitted from the patch and the
// stale value would survive. Building the raw patch by hand here sidesteps both
// problems: podNames is always included explicitly, even when empty.
func PatchVolumeEntryHealthAndPods(ctx context.Context, c client.Client, name, namespace, pvcKey, health string,
	podNames []string, lastUpdatedTime metav1.Time) error {

	log := logger.GetLogger(ctx)
	if podNames == nil {
		podNames = []string{}
	}

	patchBytes, err := json.Marshal(map[string]interface{}{
		"spec": map[string]interface{}{
			"volumes": map[string]interface{}{
				pvcKey: map[string]interface{}{
					"health":          health,
					"podNames":        podNames,
					"lastUpdatedTime": lastUpdatedTime,
				},
			},
		},
	})
	if err != nil {
		return logger.LogNewErrorf(log, "failed to marshal health/podNames merge patch for "+
			"CnsNfsVolumeInformation %s/%s, volume key %q. Error: %+v", namespace, name, pvcKey, err)
	}

	obj := &cnsnfsvolumeinformationv1alpha1.CnsNfsVolumeInformation{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: namespace},
	}
	if err := c.Patch(ctx, obj, client.RawPatch(types.MergePatchType, patchBytes)); err != nil {
		if errors.IsNotFound(err) {
			// No entry to refresh yet (the volume-created flow hasn't upserted it, or it
			// was already removed). Nothing to do - this is a best-effort refresh, not
			// the source of truth for the entry's existence.
			return nil
		}
		return logger.LogNewErrorf(log, "failed to patch health/podNames on CnsNfsVolumeInformation %s/%s, "+
			"volume key %q. Error: %+v", namespace, name, pvcKey, err)
	}
	log.Infof("Patched health/podNames for volume key %q on CnsNfsVolumeInformation %s/%s", pvcKey, namespace, name)
	return nil
}

// RemoveVolumeEntry removes a single entry (keyed by pvcKey) from the
// CnsNfsVolumeInformation CR's Spec.Volumes map via a JSON merge patch that nulls out
// that one key, for the same reason UpsertVolumeEntry patches rather than
// Get-then-Updates: removal of unrelated keys by concurrent callers must not conflict.
// If the CR ends up with no volumes left, it is deleted.
func RemoveVolumeEntry(ctx context.Context, c client.Client, name, namespace, pvcKey string) error {
	log := logger.GetLogger(ctx)

	patchBytes, err := json.Marshal(map[string]interface{}{
		"spec": map[string]interface{}{
			"volumes": map[string]interface{}{pvcKey: nil},
		},
	})
	if err != nil {
		return logger.LogNewErrorf(log, "failed to marshal merge patch removing volume key %q from "+
			"CnsNfsVolumeInformation %s/%s. Error: %+v", pvcKey, namespace, name, err)
	}

	obj := &cnsnfsvolumeinformationv1alpha1.CnsNfsVolumeInformation{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: namespace},
	}
	if err := c.Patch(ctx, obj, client.RawPatch(types.MergePatchType, patchBytes)); err != nil {
		if errors.IsNotFound(err) {
			// Nothing to remove - the CR (or the whole VKS cluster's entry) is already gone.
			return nil
		}
		return logger.LogNewErrorf(log, "failed to remove volume key %q from CnsNfsVolumeInformation %s/%s. "+
			"Error: %+v", pvcKey, namespace, name, err)
	}
	log.Infof("Removed volume key %q from CnsNfsVolumeInformation %s/%s", pvcKey, namespace, name)

	current := &cnsnfsvolumeinformationv1alpha1.CnsNfsVolumeInformation{}
	if err := c.Get(ctx, client.ObjectKey{Name: name, Namespace: namespace}, current); err != nil {
		return client.IgnoreNotFound(err)
	}
	if len(current.Spec.Volumes) > 0 {
		return nil
	}
	if err := c.Delete(ctx, current); err != nil {
		return client.IgnoreNotFound(err)
	}
	log.Infof("Deleted empty CnsNfsVolumeInformation %s/%s after removing last volume key %q",
		namespace, name, pvcKey)
	return nil
}
