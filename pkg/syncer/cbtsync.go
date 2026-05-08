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
	"errors"
	"fmt"
	"strings"
	"sync"

	cnstypes "github.com/vmware/govmomi/cns/types"
	v1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	apiMeta "k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/dynamic"
	corelisters "k8s.io/client-go/listers/core/v1"
	storagelistersv1 "k8s.io/client-go/listers/storage/v1"
	"sigs.k8s.io/controller-runtime/pkg/client/config"

	cbtconfigv1alpha1 "sigs.k8s.io/vsphere-csi-driver/v3/pkg/apis/cbtconfig/v1alpha1"
	volumes "sigs.k8s.io/vsphere-csi-driver/v3/pkg/common/cns-lib/volume"
	"sigs.k8s.io/vsphere-csi-driver/v3/pkg/csi/service/common"
	"sigs.k8s.io/vsphere-csi-driver/v3/pkg/csi/service/logger"
	"sigs.k8s.io/vsphere-csi-driver/v3/pkg/syncer/cnsoperator/util"
)

var cbtConfigResource = schema.GroupVersionResource{
	Group:    cbtconfigv1alpha1.GroupName,
	Version:  cbtconfigv1alpha1.Version,
	Resource: cbtconfigv1alpha1.CBTConfigResource,
}

const (
	workerThreadsCBTReconcileOpsEnvVar  = "WORKER_THREADS_CBT_RECONCILE_OPS"
	defaultWorkerThreadsCBTReconcileOps = 4

	vmServiceVMAnnotationPrefix = "cns.vmware.com/usedby-vm-"
)

// cbtWorkQueue is a fixed-size buffered channel used as a counting semaphore that
// bounds the number of in-flight ReconcileCBTForNamespace goroutines across all
// namespaces. Sized once on first use from WORKER_THREADS_CBT_RECONCILE_OPS so the
// CBTConfig controller's Reconcile thread is never blocked behind a slow CNS call —
// when the queue is full, Reconcile returns an error and controller-runtime requeues.
var (
	cbtWorkQueueOnce sync.Once
	cbtWorkQueue     chan struct{}
)

func getCBTWorkQueue(ctx context.Context) chan struct{} {
	cbtWorkQueueOnce.Do(func() {
		size := util.GetMaxWorkerThreads(ctx, workerThreadsCBTReconcileOpsEnvVar,
			defaultWorkerThreadsCBTReconcileOps)
		cbtWorkQueue = make(chan struct{}, size)
	})
	return cbtWorkQueue
}

// cbtWork is the cancel handle for one in-flight syncCBTWork invocation, regardless of
// whether it was started by the periodic sync or by the controller reconcile. Pointer
// identity (the *cbtWork stored in cbtWorkMap) is what the owning goroutine compares
// against on exit so it doesn't stomp a successor that has already replaced it.
type cbtWork struct {
	cancel context.CancelFunc
}

var (
	cbtWorkMu sync.Mutex
	// cbtWorkMap tracks the in-flight CBT work per namespace. An entry is inserted when
	// syncCBTForNamespace or ReconcileCBTForNamespace starts work for a namespace, and
	// deleted by the owning goroutine on exit, so the map does not grow without bound
	// on clusters that churn namespaces over time.
	cbtWorkMap = make(map[string]*cbtWork)
)

// runPeriodicCBTSync reconciles PVC CBT states with CNS changed-block-tracking state.
// It is invoked on the interval configured by CBT_SYNC_INTERVAL_MINUTES (see getCBTSyncIntervalInMin).
// InitMetadataSyncer starts the periodic caller only on Supervisor when supports_CSI_Backup_API is
// enabled at startup. This function no-ops if no CBTConfig CR exists in the cluster.
func runPeriodicCBTSync(ctx context.Context, metadataSyncer *metadataSyncInformer) {
	log := logger.GetLogger(ctx)
	// Single cluster-wide List populates both the namespace set and the enable flag per namespace,
	// avoiding a separate per-namespace API call inside the sync loop.
	namespaceCBTState, err := listCBTStatesByNamespace(ctx)
	if err != nil {
		log.Errorf("runPeriodicCBTSync: failed to list CBTConfig CRs: %v", err)
		return
	}
	if len(namespaceCBTState) == 0 {
		log.Debugf("runPeriodicCBTSync: no CBTConfig CR found in cluster, skipping periodic reconciliation")
		return
	}
	log.Infof("runPeriodicCBTSync: starting periodic CBT reconciliation for %d namespace(s)",
		len(namespaceCBTState))
	periodicCBTSync(ctx, metadataSyncer, namespaceCBTState)
}

// listCBTStatesByNamespace returns a map of namespace to the CBT enable flag (true/false)
// for every CBTConfig CR found in the cluster.
func listCBTStatesByNamespace(ctx context.Context) (map[string]bool, error) {
	log := logger.GetLogger(ctx)
	cfg, err := config.GetConfig()
	if err != nil {
		return nil, err
	}
	dynClient, err := dynamic.NewForConfig(cfg)
	if err != nil {
		return nil, err
	}
	unstructuredList, err := dynClient.Resource(cbtConfigResource).
		Namespace(metav1.NamespaceAll).List(ctx, metav1.ListOptions{})
	if err != nil {
		// CRD may be installed after the syncer starts; treat a missing API as "no CBTConfig objects".
		if apiMeta.IsNoMatchError(err) || apierrors.IsNotFound(err) {
			log.Debugf("listCBTStatesByNamespace: CBTConfig CRD not found, skipping")
			return nil, nil
		}
		return nil, err
	}
	var list cbtconfigv1alpha1.CBTConfigList
	if err := runtime.DefaultUnstructuredConverter.FromUnstructured(
		unstructuredList.UnstructuredContent(), &list); err != nil {
		return nil, err
	}
	out := make(map[string]bool, len(list.Items))
	for i := range list.Items {
		item := &list.Items[i]
		if item.Status.Enabled != nil {
			out[item.Namespace] = *item.Status.Enabled
		} else {
			log.Debugf("listCBTStatesByNamespace: namespace %q CBTConfig CR status.Enabled is not set",
				item.Namespace)
		}
	}
	return out, nil
}

func periodicCBTSync(ctx context.Context, metadataSyncer *metadataSyncInformer,
	namespaceCBTState map[string]bool) {
	log := logger.GetLogger(ctx)
	vc := metadataSyncer.configInfo.Cfg.Global.VCenterIP
	// CBT sync is gated to Workload flavor in InitMetadataSyncer, so metadataSyncer.volumeManager
	// is guaranteed initialised by the time the periodic loop runs.
	volManager := metadataSyncer.volumeManager
	for ns, enable := range namespaceCBTState {
		if err := syncCBTForNamespace(ctx, volManager,
			metadataSyncer.pvLister, metadataSyncer.pvcLister, metadataSyncer.vaLister,
			ns, enable); err != nil {
			log.Warnf("periodicCBTSync: namespace %q CBT reconciliation failed: %v"+
				" - continuing with next namespace", ns, err)
		}
	}
	log.Infof("periodicCBTSync: finished periodic CBT reconciliation for VC %s", vc)
}

// pvcWithVolume pairs a PVC with its resolved CNS volume ID.
type pvcWithVolume struct {
	pvc      *v1.PersistentVolumeClaim
	volumeID string
}

// syncCBTForNamespace is the periodic-syncer entry point called by periodicCBTSync.
// If a background controller reconcile is already in flight for the namespace it skips
// immediately. Otherwise it registers its own cancel handle so a concurrent
// ReconcileCBTForNamespace call can abort it mid-flight and take over.
func syncCBTForNamespace(ctx context.Context,
	volManager volumes.Manager,
	pvLister corelisters.PersistentVolumeLister,
	pvcLister corelisters.PersistentVolumeClaimLister,
	vaLister storagelistersv1.VolumeAttachmentLister,
	namespace string,
	enable bool,
) error {
	log := logger.GetLogger(ctx)

	ctx, cancel := context.WithCancel(ctx)
	myWork := &cbtWork{cancel: cancel}
	cbtWorkMu.Lock()
	if cbtWorkMap[namespace] != nil {
		cbtWorkMu.Unlock()
		cancel()
		log.Infof("syncCBTForNamespace: namespace %q has in-flight CBT work; skipping periodic sync",
			namespace)
		return nil
	}
	cbtWorkMap[namespace] = myWork
	cbtWorkMu.Unlock()

	defer func() {
		cancel()
		cbtWorkMu.Lock()
		if cbtWorkMap[namespace] == myWork {
			delete(cbtWorkMap, namespace)
		}
		cbtWorkMu.Unlock()
	}()

	return syncCBTWork(ctx, volManager, pvLister, pvcLister, vaLister, namespace, enable)
}

// ReconcileCBTForNamespace is the controller entry point called by the CBTConfig reconciler.
// It cancels any in-flight CBT work for the namespace (periodic sync or a previous
// background reconcile), then enqueues a fresh reconcile onto the bounded background
// pool returned by getCBTReconcileQueue. The actual syncCBTWork pipeline runs on a
// separate goroutine so a slow CNS call cannot block the controller's Reconcile thread.
//
// Returns nil once the work has been scheduled. Returns a non-nil error only when the
// background pool is fully busy; the caller is expected to requeue and retry, by which
// time slots typically free up. Per-volume failures inside the pipeline are best-effort
// and do not surface here — the periodic sync re-converges on the next interval.
func ReconcileCBTForNamespace(ctx context.Context,
	volManager volumes.Manager,
	pvLister corelisters.PersistentVolumeLister,
	pvcLister corelisters.PersistentVolumeClaimLister,
	vaLister storagelistersv1.VolumeAttachmentLister,
	namespace string,
	enable bool,
) error {
	log := logger.GetLogger(ctx)

	cbtWorkMu.Lock()
	if existing := cbtWorkMap[namespace]; existing != nil {
		log.Infof("ReconcileCBTForNamespace: cancelling in-flight CBT work for namespace %q"+
			" to take over with new reconcile", namespace)
		existing.cancel()
	}
	cbtWorkMu.Unlock()

	queue := getCBTWorkQueue(ctx)
	select {
	case queue <- struct{}{}:
	default:
		return fmt.Errorf("CBT work queue is full for namespace %q; will retry", namespace)
	}

	// Detach from the controller's Reconcile context (which is cancelled when Reconcile
	// returns) but preserve its values — notably the request-scoped logger — so the
	// background goroutine logs with the same trace metadata as its caller.
	workCtx, workCancel := context.WithCancel(context.WithoutCancel(ctx))
	myWork := &cbtWork{cancel: workCancel}
	cbtWorkMu.Lock()
	if existing := cbtWorkMap[namespace]; existing != nil {
		existing.cancel()
	}
	cbtWorkMap[namespace] = myWork
	cbtWorkMu.Unlock()

	go func() {
		defer func() {
			workCancel()
			<-queue
			cbtWorkMu.Lock()
			if cbtWorkMap[namespace] == myWork {
				delete(cbtWorkMap, namespace)
			}
			cbtWorkMu.Unlock()
		}()
		reconcileCBTWork(workCtx, volManager, pvLister, pvcLister, vaLister, namespace, enable)
	}()

	return nil
}

// reconcileCBTWork is the body run by the background goroutine launched from
// ReconcileCBTForNamespace. It invokes the shared syncCBTWork pipeline and only logs
// the outcome — there is no caller to return an error to. Cancellation triggered by a
// newer reconcile taking over is logged at Info level since it's an expected transition,
// not a failure.
func reconcileCBTWork(ctx context.Context,
	volManager volumes.Manager,
	pvLister corelisters.PersistentVolumeLister,
	pvcLister corelisters.PersistentVolumeClaimLister,
	vaLister storagelistersv1.VolumeAttachmentLister,
	namespace string,
	enable bool,
) {
	log := logger.GetLogger(ctx)
	if err := syncCBTWork(ctx, volManager, pvLister, pvcLister, vaLister, namespace, enable); err != nil {
		if errors.Is(err, context.Canceled) {
			log.Infof("reconcileCBTWork: namespace %q background CBT reconcile cancelled (superseded): %v",
				namespace, err)
			return
		}
		log.Errorf("reconcileCBTWork: namespace %q background CBT reconcile failed: %+v", namespace, err)
	}
}

// syncCBTWork drives the three CBT reconcile phases shared by syncCBTForNamespace (periodic
// path) and ReconcileCBTForNamespace (controller path). It honours ctx cancellation at each
// phase boundary so the periodic path can be aborted quickly when the controller takes over.
//
//  1. Build candidates: collect eligible vSphere block PVCs and resolve their volume IDs.
//  2. Filter by CBT state: query CNS and keep only volumes whose state differs from target.
//  3. Apply CBT flags: set/clear CBT flag on each surviving candidate.
func syncCBTWork(ctx context.Context,
	volManager volumes.Manager,
	pvLister corelisters.PersistentVolumeLister,
	pvcLister corelisters.PersistentVolumeClaimLister,
	vaLister storagelistersv1.VolumeAttachmentLister,
	namespace string,
	enable bool,
) error {
	log := logger.GetLogger(ctx)
	log.Infof("syncCBTWork: reconciling CBT(enable=%t) for namespace %q", enable, namespace)

	if err := ctx.Err(); err != nil {
		return err
	}
	candidates, err := buildPVCCandidates(ctx, pvLister, pvcLister, vaLister, namespace)
	if err != nil {
		log.Errorf("syncCBTWork: namespace %q phase 1 (build candidates) failed: %v", namespace, err)
		return err
	}
	log.Infof("syncCBTWork: namespace %q phase 1: %d candidate(s)", namespace, len(candidates))

	if err := ctx.Err(); err != nil {
		log.Infof("syncCBTWork: namespace %q CBT reconcile cancelled at phase 1: %v", namespace, err)
		return err
	}
	candidates, err = filterCandidatesByCBTState(ctx, volManager, namespace, candidates, enable)
	if err != nil {
		log.Errorf("syncCBTWork: namespace %q phase 2 (CNS state query) failed: %v", namespace, err)
		return err
	}
	log.Infof("syncCBTWork: namespace %q phase 2: %d candidate(s) need CBT flip", namespace, len(candidates))

	if err := ctx.Err(); err != nil {
		log.Infof("syncCBTWork: namespace %q CBT reconcile cancelled at phase 2: %v", namespace, err)
		return err
	}
	if err = applyCnsCbtFlags(ctx, volManager, namespace, candidates, enable); err != nil {
		log.Errorf("syncCBTWork: namespace %q phase 3 (apply CBT flags) failed: %v", namespace, err)
		return err
	}

	log.Infof("syncCBTWork: namespace %q CBT reconcile finished (enable=%t)", namespace, enable)
	return nil
}

func loadAttachedPVNames(vaLister storagelistersv1.VolumeAttachmentLister) (map[string]struct{}, error) {
	vas, err := vaLister.List(labels.Everything())
	if err != nil {
		return nil, err
	}
	attached := make(map[string]struct{}, len(vas))
	for _, va := range vas {
		name := va.Spec.Source.PersistentVolumeName
		if name == nil || *name == "" {
			continue
		}
		attached[*name] = struct{}{}
	}
	return attached, nil
}

func buildPVCCandidates(ctx context.Context,
	pvLister corelisters.PersistentVolumeLister,
	pvcLister corelisters.PersistentVolumeClaimLister,
	vaLister storagelistersv1.VolumeAttachmentLister,
	namespace string) ([]pvcWithVolume, error) {
	log := logger.GetLogger(ctx)

	attachedPVs, err := loadAttachedPVNames(vaLister)
	if err != nil {
		log.Errorf("buildPVCCandidates: failed to list VolumeAttachment objects: %v", err)
		return nil, err
	}
	log.Infof("buildPVCCandidates: namespace %q loaded %d attached PV names from VolumeAttachment cache",
		namespace, len(attachedPVs))

	pvcs, err := pvcLister.PersistentVolumeClaims(namespace).List(labels.Everything())
	if err != nil {
		log.Errorf("buildPVCCandidates: failed to list PVCs: %v", err)
		return nil, err
	}

	candidates := make([]pvcWithVolume, 0, len(pvcs))
	for _, pvc := range pvcs {
		if !pvcEligibleForCBTChange(ctx, pvc, attachedPVs) {
			continue
		}
		volumeID, ok := resolvePVCToVolumeID(ctx, pvLister, pvc)
		if !ok {
			log.Debugf("buildPVCCandidates: namespace %q PVC %s skipped: PV %s is not an eligible vSphere block volume",
				namespace, pvc.Name, pvc.Spec.VolumeName)
			continue
		}
		candidates = append(candidates, pvcWithVolume{pvc: pvc, volumeID: volumeID})
	}
	return candidates, nil
}

func resolvePVCToVolumeID(ctx context.Context,
	pvLister corelisters.PersistentVolumeLister,
	pvc *v1.PersistentVolumeClaim) (string, bool) {
	log := logger.GetLogger(ctx)
	pv, err := pvLister.Get(pvc.Spec.VolumeName)
	if err != nil {
		log.Errorf("resolvePVCToVolumeID: failed to get PV %s for PVC %s: %+v",
			pvc.Spec.VolumeName, pvc.Name, err)
		return "", false
	}
	if pv.Spec.CSI == nil || pv.Spec.CSI.Driver != common.VSphereCSIDriverName {
		return "", false
	}
	if pv.Spec.CSI.VolumeAttributes == nil ||
		pv.Spec.CSI.VolumeAttributes[common.AttributeDiskType] != common.DiskTypeBlockVolume {
		return "", false
	}
	if pv.Spec.CSI.VolumeHandle == "" {
		return "", false
	}
	return pv.Spec.CSI.VolumeHandle, true
}

func filterCandidatesByCBTState(ctx context.Context, volManager volumes.Manager,
	namespace string, candidates []pvcWithVolume, enable bool) ([]pvcWithVolume, error) {
	log := logger.GetLogger(ctx)
	if len(candidates) == 0 {
		log.Debugf("filterCandidatesByCBTState: namespace %q has no candidates to filter by CBT state",
			namespace)
		return candidates, nil
	}
	candidateVolumeIDs := make([]string, 0, len(candidates))
	for _, c := range candidates {
		candidateVolumeIDs = append(candidateVolumeIDs, c.volumeID)
	}
	needsAction, err := queryVolumesNeedingFlip(ctx, volManager, candidateVolumeIDs, enable)
	if err != nil {
		log.Errorf("filterCandidatesByCBTState: namespace %q failed to query volumes needing CBT flip: %+v",
			namespace, err)
		return nil, err
	}
	filtered := candidates[:0]
	for _, c := range candidates {
		if _, ok := needsAction[c.volumeID]; ok {
			filtered = append(filtered, c)
		} else {
			log.Debugf("filterCandidatesByCBTState: namespace %q volume %s CBT already in target state; skipping",
				namespace, c.volumeID)
		}
	}
	return filtered, nil
}

// applyCnsCbtFlags applies the CBT enable/disable flag to each candidate volume
// sequentially.
func applyCnsCbtFlags(ctx context.Context, volManager volumes.Manager,
	namespace string, candidates []pvcWithVolume, enable bool) error {
	log := logger.GetLogger(ctx)
	if len(candidates) == 0 {
		log.Debugf("applyCnsCbtFlags: namespace %q has no candidates to apply CBT flag", namespace)
		return nil
	}
	for _, cand := range candidates {
		if err := ctx.Err(); err != nil {
			log.Infof("applyCnsCbtFlags: namespace %q CBT flag changes cancelled after partial progress: %v",
				namespace, err)
			return err
		}
		var opErr error
		if enable {
			opErr = common.SetVolumeCbtFlagsUtil(ctx, volManager, cand.volumeID)
		} else {
			opErr = common.ClearVolumeCbtFlagsUtil(ctx, volManager, cand.volumeID)
		}
		if opErr != nil {
			log.Warnf("applyCnsCbtFlags: namespace %q failed to set CBT=%t for PVC %s (volume %s): %+v",
				namespace, enable, cand.pvc.Name, cand.volumeID, opErr)
			continue
		}
		log.Infof("applyCnsCbtFlags: namespace %q set CBT=%t for PVC %s (volume %s)",
			namespace, enable, cand.pvc.Name, cand.volumeID)
	}
	return ctx.Err()
}

// queryVolumesNeedingFlip queries CNS for volumes in candidateVolumeIDs whose current CBT
// state is opposite to the target. Batched at volumdIDLimitPerQuery so a single
// CnsQueryFilter never exceeds the same input-size envelope used by full sync.
func queryVolumesNeedingFlip(ctx context.Context, volManager volumes.Manager,
	candidateVolumeIDs []string, enable bool) (map[string]struct{}, error) {
	log := logger.GetLogger(ctx)
	nonTargetCBTState := cnstypes.CnsVolumeCBTStatusDisabled
	if !enable {
		nonTargetCBTState = cnstypes.CnsVolumeCBTStatusEnabled
	}
	needsAction := make(map[string]struct{}, len(candidateVolumeIDs))
	for start := 0; start < len(candidateVolumeIDs); start += volumdIDLimitPerQuery {
		end := start + volumdIDLimitPerQuery
		if end > len(candidateVolumeIDs) {
			end = len(candidateVolumeIDs)
		}
		batch := make([]cnstypes.CnsVolumeId, 0, end-start)
		for _, id := range candidateVolumeIDs[start:end] {
			batch = append(batch, cnstypes.CnsVolumeId{Id: id})
		}
		queryFilter := cnstypes.CnsQueryFilter{VolumeIds: batch, ChangedBlockTracking: nonTargetCBTState}
		queryResult, err := volManager.QueryAllVolume(ctx, queryFilter, cnstypes.CnsQuerySelection{})
		if err != nil {
			return nil, err
		}
		if queryResult == nil {
			continue
		}
		log.Debugf("queryVolumesNeedingFlip: QueryAllVolume batch [%d:%d) returned %d volumes needing CBT flip",
			start, end, len(queryResult.Volumes))
		for _, vol := range queryResult.Volumes {
			needsAction[vol.VolumeId.Id] = struct{}{}
		}
	}
	return needsAction, nil
}

// pvcEligibleForCBTChange returns false for PVCs that must be skipped:
//   - not in Bound phase (no backing volume yet)
//   - no backing volume name
//   - currently attached to a PodVM via VolumeAttachment (CBT ops require the volume to be detached)
//   - in use by a VM Service VM (annotation prefix "cns.vmware.com/usedby-vm-"); these attachments
//     go through CnsNodeVMBatchAttachment and do not create VolumeAttachment objects, so they are
//     not covered by the VolumeAttachment check above
func pvcEligibleForCBTChange(ctx context.Context,
	pvc *v1.PersistentVolumeClaim, attachedPVs map[string]struct{}) bool {
	log := logger.GetLogger(ctx)
	if pvc.Status.Phase != v1.ClaimBound {
		return false
	}
	if pvc.Spec.VolumeName == "" {
		return false
	}
	if _, attached := attachedPVs[pvc.Spec.VolumeName]; attached {
		log.Debugf("pvcEligibleForCBTChange: PVC %s/%s is attached to PodVM; skipping",
			pvc.Namespace, pvc.Name)
		return false
	}
	for key := range pvc.Annotations {
		if strings.HasPrefix(key, vmServiceVMAnnotationPrefix) {
			log.Debugf("pvcEligibleForCBTChange: PVC %s/%s is in use by a VM Service VM; skipping",
				pvc.Namespace, pvc.Name)
			return false
		}
	}
	return true
}
