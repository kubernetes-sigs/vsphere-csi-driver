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

package syncer

import (
	"context"
	"time"

	v1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	clientset "k8s.io/client-go/kubernetes"
	"k8s.io/client-go/util/retry"

	"sigs.k8s.io/vsphere-csi-driver/v3/pkg/csi/service/common"
	"sigs.k8s.io/vsphere-csi-driver/v3/pkg/csi/service/logger"
)

// guestNFSHealthTimeout bounds the probes of one guest NFS volume health pass, so that NFS servers that do not
// answer cannot hold up full sync for longer.
var guestNFSHealthTimeout = 2 * time.Minute

// guestNFSHealthTarget is the PVC of a bound guest NFS volume and the address of the volume's NFS service.
type guestNFSHealthTarget struct {
	pvc     *v1.PersistentVolumeClaim
	pvName  string
	address string
}

// syncGuestNFSVolumeHealth sets the volume health annotation on the PVC of every bound guest NFS volume:
// "accessible" when the volume's NFS service answered probeNFSService from this pod, "inaccessible" when it
// did not. See probeNFSService for what that does and does not prove. Guest NFS volumes have no Supervisor PVC
// or CNS volume, so the volume health reconciler, which copies the health of the Supervisor PVC, cannot cover
// them. Each NFS service is probed once, however many volumes it serves, and a volume whose NFS service was not
// probed keeps its annotation.
//
// It is not gated on the guest-nfs-volume feature state, which gates only provisioning: volumes that exist are
// still served when it is off, so their health is still reported. Without guest NFS volumes it does nothing.
// Errors are logged and never fail full sync.
func syncGuestNFSVolumeHealth(ctx context.Context, metadataSyncer *metadataSyncInformer) {
	log := logger.GetLogger(ctx)
	if err := ctx.Err(); err != nil {
		log.Infof("FullSync: skipping guest NFS volume health: %v", err)
		return
	}
	pvs, err := getBoundPVs(ctx, metadataSyncer)
	if err != nil {
		log.Warnf("FullSync: failed to get PVs for guest NFS volume health. Err: %v", err)
		return
	}
	var targets []guestNFSHealthTarget
	var addresses []string
	seen := make(map[string]bool)
	for _, pv := range pvs {
		if !common.IsGuestNFSPersistentVolume(pv) || pv.Spec.ClaimRef == nil {
			continue
		}
		claim := pv.Spec.ClaimRef
		pvc, err := metadataSyncer.pvcLister.PersistentVolumeClaims(claim.Namespace).Get(claim.Name)
		if err != nil {
			if !apierrors.IsNotFound(err) {
				log.Warnf("FullSync: failed to get PVC %s/%s of guest NFS PV %s. Err: %v",
					claim.Namespace, claim.Name, pv.Name, err)
			}
			continue
		}
		if pvc.Spec.VolumeName != pv.Name || (claim.UID != "" && claim.UID != pvc.UID) {
			continue
		}
		address, err := guestNFSServerAddress(pv)
		if err != nil {
			log.Warnf("FullSync: cannot check the NFS service of guest NFS PVC %s/%s (PV %s): %v",
				pvc.Namespace, pvc.Name, pv.Name, err)
			continue
		}
		targets = append(targets, guestNFSHealthTarget{pvc: pvc, pvName: pv.Name, address: address})
		if !seen[address] {
			seen[address] = true
			addresses = append(addresses, address)
		}
	}
	if len(targets) == 0 {
		return
	}
	k8sClient, err := k8sNewClient(ctx)
	if err != nil {
		log.Warnf("FullSync: failed to create a Kubernetes client for guest NFS volume health. Err: %v", err)
		return
	}

	probeCtx, cancel := context.WithTimeout(ctx, guestNFSHealthTimeout)
	results := probeGuestNFSServers(probeCtx, addresses)
	cancel()
	for _, address := range addresses {
		probeErr, probed := results[address]
		switch {
		case !probed:
			log.Warnf("FullSync: the probe of NFS service %s did not complete before the guest NFS volume health "+
				"pass ended; the volume health of its guest NFS PVCs is left unchanged", address)
		case probeErr != nil:
			log.Warnf("FullSync: the NFS NULL call to %s from this pod failed: %v", address, probeErr)
		default:
			log.Debugf("FullSync: NFS service %s answered an NFS NULL call from this pod", address)
		}
	}
	if err := ctx.Err(); err != nil {
		log.Infof("FullSync: skipping guest NFS volume health updates: %v", err)
		return
	}
	for _, target := range targets {
		probeErr, probed := results[target.address]
		if !probed {
			continue
		}
		status := common.VolHealthStatusAccessible
		if probeErr != nil {
			status = common.VolHealthStatusInaccessible
		}
		if err := setGuestNFSVolumeHealth(ctx, k8sClient, target.pvc, status); err != nil {
			log.Warnf("FullSync: failed to set the volume health of guest NFS PVC %s/%s (PV %s, NFS service %s) "+
				"to %q. Err: %v", target.pvc.Namespace, target.pvc.Name, target.pvName, target.address, status, err)
		}
	}
}

// setGuestNFSVolumeHealth sets the volume health annotation of pvc to status, with a new timestamp, unless it
// already has that status and a timestamp. Like updateVolumeHealthStatus it updates the PVC and, on a conflict,
// retries with the PVC read back from the API server. A PVC that was deleted, or replaced by another one with the
// same name, is left alone.
func setGuestNFSVolumeHealth(ctx context.Context, k8sClient clientset.Interface, pvc *v1.PersistentVolumeClaim,
	status string) error {
	log := logger.GetLogger(ctx)
	attempt := 0
	err := retry.RetryOnConflict(retry.DefaultRetry, func() error {
		current := pvc
		if attempt++; attempt > 1 {
			// The previous update conflicted, so pvc, which comes from the informer cache, is stale.
			latest, err := k8sClient.CoreV1().PersistentVolumeClaims(pvc.Namespace).Get(ctx, pvc.Name,
				metav1.GetOptions{})
			if err != nil {
				return err
			}
			if latest.UID != pvc.UID {
				return nil
			}
			current = latest
		}
		old, found := current.Annotations[annVolumeHealth]
		_, foundTS := current.Annotations[annVolumeHealthTS]
		if found && foundTS && old == status {
			return nil
		}
		updated := current.DeepCopy()
		timeNow := time.Now().Format(time.UnixDate)
		metav1.SetMetaDataAnnotation(&updated.ObjectMeta, annVolumeHealth, status)
		metav1.SetMetaDataAnnotation(&updated.ObjectMeta, annVolumeHealthTS, timeNow)
		if _, err := k8sClient.CoreV1().PersistentVolumeClaims(updated.Namespace).Update(ctx, updated,
			metav1.UpdateOptions{}); err != nil {
			return err
		}
		log.Infof("FullSync: set volumehealth annotation of guest NFS PVC %s/%s from %q to %q and "+
			"volumehealthTS annotation to %s", updated.Namespace, updated.Name, old, status, timeNow)
		return nil
	})
	if apierrors.IsNotFound(err) {
		log.Debugf("FullSync: guest NFS PVC %s/%s was deleted; not setting its volume health", pvc.Namespace, pvc.Name)
		return nil
	}
	return err
}
