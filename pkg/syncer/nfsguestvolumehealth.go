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
	"strings"
	"time"

	clientset "k8s.io/client-go/kubernetes"

	"sigs.k8s.io/vsphere-csi-driver/v3/pkg/common/nfsmount"
	"sigs.k8s.io/vsphere-csi-driver/v3/pkg/csi/service/common"
	"sigs.k8s.io/vsphere-csi-driver/v3/pkg/csi/service/logger"
	"sigs.k8s.io/vsphere-csi-driver/v3/pkg/csi/service/wcpguest/nfsdriver"
)

// nfsGuestVolumeHealthCheckTimeout bounds each individual MOUNT-protocol RPC check, so
// one unreachable NFS server can't stall an entire reconcile pass.
const nfsGuestVolumeHealthCheckTimeout = 10 * time.Second

// csiGetNfsGuestVolumeHealthStatus is the guest-local-NFS-volume analogue of
// csiGetVolumeHealthStatus. That function only runs on Supervisor and derives health
// from a CNS query (or, for FVS volumes, a FileVolume CR's conditions) - neither
// applies here, since guest-local NFS volumes have no Supervisor PVC and no CNS volume
// at all. This instead independently verifies, for every guest-local NFS-backed Bound
// PV, that its server:/share/subdir is currently reachable and mountable via a real
// NFSv3 MOUNT-protocol RPC call (nfsmount.CheckMountable - the same check the server
// itself performs for an actual mount, without requiring the unprivileged syncer
// container to perform a real mount(2) syscall), and reflects the result on the PVC
// using the same volumehealth.storage.kubernetes.io/health annotation convention
// already used for block/vSAN-File volumes.
//
// Detection only: an already-mounted, already-failing volume on a running pod is not
// remediated here - remediation remains a pod restart or node-level intervention, same
// as for every other volume type this annotation is used for.
func csiGetNfsGuestVolumeHealthStatus(ctx context.Context, k8sclient clientset.Interface,
	metadataSyncer *metadataSyncInformer) {
	log := logger.GetLogger(ctx)
	log.Infof("csiGetNfsGuestVolumeHealthStatus: start")

	boundPVs, err := getBoundPVs(ctx, metadataSyncer)
	if err != nil {
		log.Errorf("csiGetNfsGuestVolumeHealthStatus: Failed to get PVs from kubernetes. Err: %+v", err)
		return
	}

	accessibleCount, inaccessibleCount := 0, 0
	for _, pv := range boundPVs {
		if !isGuestNFSVolume(pv) || pv.Spec.ClaimRef == nil {
			continue
		}

		pvc, err := metadataSyncer.pvcLister.PersistentVolumeClaims(pv.Spec.ClaimRef.Namespace).
			Get(pv.Spec.ClaimRef.Name)
		if err != nil {
			log.Warnf("csiGetNfsGuestVolumeHealthStatus: Failed to get pvc for namespace %s and name %s. err=%+v",
				pv.Spec.ClaimRef.Namespace, pv.Spec.ClaimRef.Name, err)
			continue
		}

		healthStatus := nfsGuestVolumeHealth(ctx, pv.Spec.CSI.VolumeHandle, pvc.Namespace, pvc.Name)
		updateVolumeHealthStatus(ctx, k8sclient, pvc, healthStatus)

		switch healthStatus {
		case common.VolHealthStatusAccessible:
			accessibleCount++
		case common.VolHealthStatusInaccessible:
			inaccessibleCount++
		}
	}

	log.Infof("csiGetNfsGuestVolumeHealthStatus: end, %d accessible, %d inaccessible",
		accessibleCount, inaccessibleCount)
}

// nfsGuestVolumeHealth decodes volumeHandle and performs the real MOUNT-protocol check
// against it, returning common.VolHealthStatusAccessible or
// common.VolHealthStatusInaccessible. Split out from csiGetNfsGuestVolumeHealthStatus
// so the decode+check logic is unit-testable without a fake PV/PVC/lister setup.
func nfsGuestVolumeHealth(ctx context.Context, volumeHandle, pvcNamespace, pvcName string) string {
	log := logger.GetLogger(ctx)

	rawID := strings.TrimPrefix(volumeHandle, nfsdriver.VolumeIDPrefix)
	server, share, subDir, err := nfsdriver.DecodeVolumeID(rawID)
	if err != nil {
		log.Errorf("nfsGuestVolumeHealth: Failed to decode NFS volume handle %q for pvc %s/%s: %v",
			volumeHandle, pvcNamespace, pvcName, err)
		return common.VolHealthStatusInaccessible
	}
	dirpath := "/" + strings.Trim(share, "/") + "/" + strings.Trim(subDir, "/")

	if err := nfsmount.CheckMountable(server, dirpath, nfsGuestVolumeHealthCheckTimeout); err != nil {
		log.Warnf("nfsGuestVolumeHealth: %s:%s is not currently mountable for pvc %s/%s: %v",
			server, dirpath, pvcNamespace, pvcName, err)
		return common.VolHealthStatusInaccessible
	}
	return common.VolHealthStatusAccessible
}
