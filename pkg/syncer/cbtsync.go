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
	"encoding/json"
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
	"k8s.io/apimachinery/pkg/types"
	clientset "k8s.io/client-go/kubernetes"
	corelisters "k8s.io/client-go/listers/core/v1"
	storagelistersv1 "k8s.io/client-go/listers/storage/v1"
	ctrlclient "sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/config"

	cbtconfigv1alpha1 "sigs.k8s.io/vsphere-csi-driver/v3/pkg/apis/cbtconfig/v1alpha1"
	volumes "sigs.k8s.io/vsphere-csi-driver/v3/pkg/common/cns-lib/volume"
	"sigs.k8s.io/vsphere-csi-driver/v3/pkg/csi/service/common"
	"sigs.k8s.io/vsphere-csi-driver/v3/pkg/csi/service/logger"
	k8s "sigs.k8s.io/vsphere-csi-driver/v3/pkg/kubernetes"
	cnsoperatortypes "sigs.k8s.io/vsphere-csi-driver/v3/pkg/syncer/cnsoperator/types"
	"sigs.k8s.io/vsphere-csi-driver/v3/pkg/syncer/cnsoperator/util"
)

const (
	workerThreadsCBTReconcileOpsEnvVar  = "WORKER_THREADS_CBT_RECONCILE_OPS"
	defaultWorkerThreadsCBTReconcileOps = 4

	// isCBTActiveLabel is set to "true" on a PVC whose underlying CNS volume currently
	// has ChangedBlockTracking enabled, and is absent when CBT is disabled. The label
	// gives data-protection consumers a cheap label selector to find CBT-eligible PVCs
	// without per-PVC CNS QueryAllVolume calls. Phase 4 of syncCBTWork is responsible
	// for keeping this label in lock-step with the CNS volume state.
	// Label key: cns.vmware.com/cbt-active
	isCBTActiveLabel    = "cns.vmware.com/cbt-active"
	isCBTActiveLabelVal = "true"
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

// CBTSyncer bundles the Kubernetes and CNS dependencies used by the CBT reconcile pipeline.
type CBTSyncer struct {
	kubeClient clientset.Interface
	volManager volumes.Manager
	pvLister   corelisters.PersistentVolumeLister
	pvcLister  corelisters.PersistentVolumeClaimLister
	vaLister   storagelistersv1.VolumeAttachmentLister
}

// NewCBTSyncer creates a CBTSyncer with the supplied dependencies.
func NewCBTSyncer(
	kubeClient clientset.Interface,
	volManager volumes.Manager,
	pvLister corelisters.PersistentVolumeLister,
	pvcLister corelisters.PersistentVolumeClaimLister,
	vaLister storagelistersv1.VolumeAttachmentLister,
) *CBTSyncer {
	return &CBTSyncer{
		kubeClient: kubeClient,
		volManager: volManager,
		pvLister:   pvLister,
		pvcLister:  pvcLister,
		vaLister:   vaLister,
	}
}

// runPeriodicCBTSync reconciles PVC CBT states with CNS changed-block-tracking state.
// It is invoked on the interval configured by CBT_SYNC_INTERVAL_MINUTES (see getCBTSyncIntervalInMin).
// InitMetadataSyncer starts the periodic caller only on Supervisor when supports_CSI_Backup_API is
// enabled at startup. This function no-ops if no CBTConfig CR exists in the cluster.
func runPeriodicCBTSync(ctx context.Context, metadataSyncer *metadataSyncInformer) {
	log := logger.GetLogger(ctx)
	// Single cluster-wide List populates both the namespace set and the active flag per namespace,
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

// listCBTStatesByNamespace returns a map of namespace to the CBT active flag (true/false)
// for every CBTConfig CR found in the cluster.
func listCBTStatesByNamespace(ctx context.Context) (map[string]bool, error) {
	log := logger.GetLogger(ctx)
	cfg, err := config.GetConfig()
	if err != nil {
		return nil, err
	}
	scheme := runtime.NewScheme()
	if err := cbtconfigv1alpha1.AddToScheme(scheme); err != nil {
		return nil, fmt.Errorf("failed to add CBTConfig scheme: %w", err)
	}
	runtimeClient, err := ctrlclient.New(cfg, ctrlclient.Options{Scheme: scheme})
	if err != nil {
		return nil, err
	}
	var list cbtconfigv1alpha1.CBTConfigList
	if err := runtimeClient.List(ctx, &list, ctrlclient.InNamespace(metav1.NamespaceAll)); err != nil {
		// CRD may be installed after the syncer starts; treat a missing API as "no CBTConfig objects".
		if apiMeta.IsNoMatchError(err) || apierrors.IsNotFound(err) {
			log.Debugf("listCBTStatesByNamespace: CBTConfig CRD not found, skipping")
			return nil, nil
		}
		return nil, err
	}
	out := make(map[string]bool, len(list.Items))
	for i := range list.Items {
		item := &list.Items[i]
		if item.Status != nil && item.Status.State != nil {
			out[item.Namespace] = *item.Status.State == cbtconfigv1alpha1.CBTStateActive
		} else {
			log.Debugf("listCBTStatesByNamespace: namespace %q CBTConfig CR status.State is not set",
				item.Namespace)
		}
	}
	return out, nil
}

func periodicCBTSync(ctx context.Context, metadataSyncer *metadataSyncInformer,
	namespaceCBTState map[string]bool) {
	log := logger.GetLogger(ctx)
	vc := metadataSyncer.configInfo.Cfg.Global.VCenterIP
	// Create one kube client and wire all dependencies into a CBTSyncer shared by every
	// namespace sync in this tick. CBT sync is gated to Workload flavor in InitMetadataSyncer,
	// so metadataSyncer.volumeManager is guaranteed initialised by the time the loop runs.
	kubeClient, err := k8s.NewClient(ctx)
	if err != nil {
		log.Errorf("periodicCBTSync: failed to create kube client; skipping CBT label phase for VC %s: %v",
			vc, err)
		return
	}
	s := NewCBTSyncer(kubeClient, metadataSyncer.volumeManager,
		metadataSyncer.pvLister, metadataSyncer.pvcLister, metadataSyncer.vaLister)
	for ns, active := range namespaceCBTState {
		if err := s.syncCBTForNamespace(ctx, ns, active); err != nil {
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
func (s *CBTSyncer) syncCBTForNamespace(ctx context.Context, namespace string, active bool) error {
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

	return s.syncCBTWork(ctx, namespace, active)
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
func (s *CBTSyncer) ReconcileCBTForNamespace(ctx context.Context, namespace string, active bool) error {
	log := logger.GetLogger(ctx)
	log.Infof("ReconcileCBTForNamespace: starting CBT reconciliation for namespace %q (active=%t)",
		namespace, active)

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
		s.reconcileCBTWork(workCtx, namespace, active)
	}()

	return nil
}

// reconcileCBTWork is the body run by the background goroutine launched from
// ReconcileCBTForNamespace. It invokes the shared syncCBTWork pipeline and only logs
// the outcome — there is no caller to return an error to. Cancellation triggered by a
// newer reconcile taking over is logged at Info level since it's an expected transition,
// not a failure.
func (s *CBTSyncer) reconcileCBTWork(ctx context.Context, namespace string, active bool) {
	log := logger.GetLogger(ctx)
	if err := s.syncCBTWork(ctx, namespace, active); err != nil {
		if errors.Is(err, context.Canceled) {
			log.Infof("reconcileCBTWork: namespace %q background CBT reconcile cancelled (superseded): %v",
				namespace, err)
			return
		}
		log.Errorf("reconcileCBTWork: namespace %q background CBT reconcile failed: %+v", namespace, err)
	}
}

// syncCBTWork drives the four CBT reconcile phases shared by syncCBTForNamespace (periodic
// path) and ReconcileCBTForNamespace (controller path). It honours ctx cancellation at each
// phase boundary so the periodic path can be aborted quickly when the controller takes over.
//
//  1. Build candidates: collect eligible vSphere block PVCs and resolve their volume IDs.
//  2. Filter by CBT state: query CNS and keep only volumes whose state differs from target.
//  3. Apply CBT flags: set/clear CBT flag on each surviving candidate.
//  4. Sync PVC labels: re-converge the cns.vmware.com/cbt-active label on every bound block PVC
//     in the namespace so the label tracks the volume's actual CNS CBT state. Best-effort
//     per PVC — failures are logged and skipped; the next sync tick re-converges.
func (s *CBTSyncer) syncCBTWork(ctx context.Context, namespace string, active bool) error {
	log := logger.GetLogger(ctx)
	log.Infof("syncCBTWork: reconciling CBT(active=%t) for namespace %q", active, namespace)

	if err := ctx.Err(); err != nil {
		return err
	}
	candidates, err := s.buildPVCCandidates(ctx, namespace)
	if err != nil {
		log.Errorf("syncCBTWork: namespace %q phase 1 (build candidates) failed: %v", namespace, err)
		return err
	}
	if err := ctx.Err(); err != nil {
		log.Infof("syncCBTWork: namespace %q CBT reconcile cancelled at phase 1: %v", namespace, err)
		return err
	}
	log.Infof("syncCBTWork: namespace %q phase 1: %d candidate(s)", namespace, len(candidates))

	candidates, err = s.filterCandidatesByCBTState(ctx, namespace, candidates, active)
	if err != nil {
		log.Errorf("syncCBTWork: namespace %q phase 2 (CNS state query) failed: %v", namespace, err)
		return err
	}
	if err := ctx.Err(); err != nil {
		log.Infof("syncCBTWork: namespace %q CBT reconcile cancelled at phase 2: %v", namespace, err)
		return err
	}
	log.Infof("syncCBTWork: namespace %q phase 2: %d candidate(s) need CBT flip", namespace, len(candidates))

	if err = s.applyCnsCbtFlags(ctx, namespace, candidates, active); err != nil {
		log.Errorf("syncCBTWork: namespace %q phase 3 (apply CBT flags) failed: %v", namespace, err)
		return err
	}
	if err := ctx.Err(); err != nil {
		log.Infof("syncCBTWork: namespace %q CBT reconcile cancelled at phase 3: %v", namespace, err)
		return err
	}
	log.Infof("syncCBTWork: namespace %q phase 3: %d candidate(s) applied CBT flags", namespace, len(candidates))

	if err := s.syncPvcCBTLabel(ctx, namespace, active); err != nil {
		log.Errorf("syncCBTWork: namespace %q phase 4 (sync CBT label) failed: %v", namespace, err)
		return err
	}
	if err := ctx.Err(); err != nil {
		log.Infof("syncCBTWork: namespace %q CBT reconcile cancelled at phase 4: %v", namespace, err)
		return err
	}

	log.Infof("syncCBTWork: namespace %q CBT reconcile finished (active=%t)", namespace, active)
	return nil
}

// loadAttachedPVNames lists all PV names currently attached to a PodVM.
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

func (s *CBTSyncer) buildPVCCandidates(ctx context.Context, namespace string) ([]pvcWithVolume, error) {
	log := logger.GetLogger(ctx)

	attachedPVs, err := loadAttachedPVNames(s.vaLister)
	if err != nil {
		log.Errorf("buildPVCCandidates: failed to list VolumeAttachment objects: %v", err)
		return nil, err
	}
	log.Infof("buildPVCCandidates: namespace %q loaded %d attached PV names from VolumeAttachment cache",
		namespace, len(attachedPVs))

	pvcs, err := s.pvcLister.PersistentVolumeClaims(namespace).List(labels.Everything())
	if err != nil {
		log.Errorf("buildPVCCandidates: failed to list PVCs: %v", err)
		return nil, err
	}

	candidates := make([]pvcWithVolume, 0, len(pvcs))
	for _, pvc := range pvcs {
		if !pvcEligibleForCBTChange(ctx, pvc, attachedPVs) {
			continue
		}
		volumeID, ok := resolvePVCToVolumeID(ctx, s.pvLister, pvc)
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

func (s *CBTSyncer) filterCandidatesByCBTState(ctx context.Context,
	namespace string, candidates []pvcWithVolume, active bool) ([]pvcWithVolume, error) {
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
	needsAction, err := queryVolumesNeedingFlip(ctx, s.volManager, candidateVolumeIDs, active)
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

// applyCnsCbtFlags applies the CBT active/inactive flag to each candidate volume
// sequentially.
func (s *CBTSyncer) applyCnsCbtFlags(ctx context.Context,
	namespace string, candidates []pvcWithVolume, active bool) error {
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
		if active {
			opErr = common.SetVolumeCbtFlagsUtil(ctx, s.volManager, cand.volumeID)
		} else {
			opErr = common.ClearVolumeCbtFlagsUtil(ctx, s.volManager, cand.volumeID)
		}
		if opErr != nil {
			log.Warnf("applyCnsCbtFlags: namespace %q failed to set CBT=%t for PVC %s (volume %s): %+v",
				namespace, active, cand.pvc.Name, cand.volumeID, opErr)
			continue
		}
		log.Infof("applyCnsCbtFlags: namespace %q set CBT=%t for PVC %s (volume %s)",
			namespace, active, cand.pvc.Name, cand.volumeID)
	}
	return ctx.Err()
}

// queryVolumesByCBTState queries CNS for volumes in candidateVolumeIDs whose current CBT
// state equals filterState, returning their volume IDs as a set. Batched at
// volumdIDLimitPerQuery so a single CnsQueryFilter never exceeds the same input-size
// envelope used by full sync.
func queryVolumesByCBTState(ctx context.Context, volManager volumes.Manager,
	candidateVolumeIDs []string, filterState cnstypes.CnsVolumeCBTStatus) (map[string]struct{}, error) {
	log := logger.GetLogger(ctx)
	matched := make(map[string]struct{}, len(candidateVolumeIDs))
	for start := 0; start < len(candidateVolumeIDs); start += volumdIDLimitPerQuery {
		end := start + volumdIDLimitPerQuery
		if end > len(candidateVolumeIDs) {
			end = len(candidateVolumeIDs)
		}
		batch := make([]cnstypes.CnsVolumeId, 0, end-start)
		for _, id := range candidateVolumeIDs[start:end] {
			batch = append(batch, cnstypes.CnsVolumeId{Id: id})
		}
		queryFilter := cnstypes.CnsQueryFilter{VolumeIds: batch, ChangedBlockTracking: filterState}
		queryResult, err := volManager.QueryAllVolume(ctx, queryFilter, cnstypes.CnsQuerySelection{})
		if err != nil {
			log.Errorf("queryVolumesByCBTState: QueryAllVolume failed: %v", err)
			return nil, err
		}
		if queryResult == nil {
			continue
		}
		log.Debugf("queryVolumesByCBTState: QueryAllVolume batch [%d:%d) filterState=%q returned %d volumes",
			start, end, filterState, len(queryResult.Volumes))
		for _, vol := range queryResult.Volumes {
			matched[vol.VolumeId.Id] = struct{}{}
		}
	}
	return matched, nil
}

// queryVolumesNeedingFlip queries CNS for volumes in candidateVolumeIDs whose current CBT
// state is opposite to active. Thin wrapper around queryVolumesByCBTState that resolves
// the non-target enum so phase 2 callers don't repeat the active→state mapping.
func queryVolumesNeedingFlip(ctx context.Context, volManager volumes.Manager,
	candidateVolumeIDs []string, active bool) (map[string]struct{}, error) {
	nonTargetCBTState := cnstypes.CnsVolumeCBTStatusDisabled
	if !active {
		nonTargetCBTState = cnstypes.CnsVolumeCBTStatusEnabled
	}
	return queryVolumesByCBTState(ctx, volManager, candidateVolumeIDs, nonTargetCBTState)
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
		if strings.HasPrefix(key, cnsoperatortypes.UsedByVMAnnotationPrefix) {
			log.Debugf("pvcEligibleForCBTChange: PVC %s/%s is in use by a VM Service VM; skipping",
				pvc.Namespace, pvc.Name)
			return false
		}
	}
	return true
}

// pvcCBTState carries the per-PVC inputs phase 4 needs to reconcile the
// cns.vmware.com/cbt-active label.
//
//   - hasLabel: PVC currently has cns.vmware.com/cbt-active="true".
//   - isActive: underlying CNS volume currently reports ChangedBlockTracking=Enabled.
//
// A mismatch (hasLabel != isActive) is what phase 4 fixes by patching the PVC label.
type pvcCBTState struct {
	pvc      *v1.PersistentVolumeClaim
	volumeID string
	hasLabel bool
	isActive bool
}

// buildAllBlockPVCs returns every Bound vSphere block-volume PVC in the namespace, paired
// with its resolved CNS volume ID. Unlike buildPVCCandidates this helper does NOT filter
// out attached / VM-Service-bound PVCs: phase 4 only patches labels, which is safe to do
// at any time regardless of attachment state.
func buildAllBlockPVCs(ctx context.Context,
	pvLister corelisters.PersistentVolumeLister,
	pvcLister corelisters.PersistentVolumeClaimLister,
	namespace string) ([]pvcCBTState, error) {
	log := logger.GetLogger(ctx)
	pvcs, err := pvcLister.PersistentVolumeClaims(namespace).List(labels.Everything())
	if err != nil {
		log.Errorf("buildAllBlockPVCs: failed to list PVCs in namespace %q: %v", namespace, err)
		return nil, err
	}
	out := make([]pvcCBTState, 0, len(pvcs))
	for _, pvc := range pvcs {
		if pvc.Status.Phase != v1.ClaimBound || pvc.Spec.VolumeName == "" {
			continue
		}
		volumeID, ok := resolvePVCToVolumeID(ctx, pvLister, pvc)
		if !ok {
			continue
		}
		out = append(out, pvcCBTState{
			pvc:      pvc,
			volumeID: volumeID,
			hasLabel: pvc.Labels[isCBTActiveLabel] == isCBTActiveLabelVal,
		})
	}
	log.Infof("buildAllBlockPVCs: namespace %q found %d block PVC(s)", namespace, len(out))
	return out, nil
}

// syncPvcCBTLabel reconciles the cns.vmware.com/cbt-active label on every Bound block-volume PVC
// in namespace so it mirrors the underlying CNS volume's actual ChangedBlockTracking state.
// Phase 4 is best-effort: per-PVC patch failures are logged and skipped, and a stale CNS
// view that doesn't yet reflect a recent CBT flip is nudged via SyncVolume before patching.
//
// Steps:
//   - Build pvcCBTState for every block PVC in the namespace. Query CNS for all volumes
//     whose CBT is Enabled; volumes not returned are treated as Disabled.
//   - Identify two independent sets: mismatchedPVCs (hasLabel != isActive) and
//     unreconciledPVCs (isActive != active, i.e. CNS hasn't converged to the target yet).
//   - Call SyncVolume on each potentially unreconciled PVC, re-query, and merge any newly discovered
//     label mismatches into mismatchedPVCs.
//   - Patch the cns.vmware.com/cbt-active label on every mismatch (add when isActive, remove when
//     !isActive). Failures are logged at Warn and skipped — the next sync tick re-converges.
func (s *CBTSyncer) syncPvcCBTLabel(ctx context.Context, namespace string, active bool) error {
	log := logger.GetLogger(ctx)

	allBlockPVCs, err := buildAllBlockPVCs(ctx, s.pvLister, s.pvcLister, namespace)
	if err != nil {
		return err
	}
	if len(allBlockPVCs) == 0 {
		log.Debugf("syncPvcCBTLabel: namespace %q has no block PVCs", namespace)
		return nil
	}

	mismatchedPVCs, unreconciledPVCs, err := findUnsyncedPvcCBTState(ctx, s.volManager, namespace, allBlockPVCs, active)
	if err != nil {
		log.Errorf("syncPvcCBTLabel: namespace %q failed to find mismatched or unreconciled PVCs: %v",
			namespace, err)
		return err
	}

	updatedPVCs := refreshUnreconciledPVCs(ctx, s.volManager, s.vaLister, namespace, unreconciledPVCs)
	if err := ctx.Err(); err != nil {
		log.Infof("syncPvcCBTLabel: namespace %q refreshUnreconciledPVCs cancelled: %v", namespace, err)
		return err
	}
	mismatchedPVCs = mergeMismatchedPVCs(ctx, namespace, mismatchedPVCs, updatedPVCs)

	applyCBTLabelPatches(ctx, s.kubeClient, namespace, mismatchedPVCs)
	return nil
}

// findUnsyncedPvcCBTState queries CNS for all volumes in allBlockPVCs whose CBT is currently
// Enabled, then computes isActive for every entry from membership in that set.
//
// Returns:
//   - mismatchedPVCs: PVCs where hasLabel != isActive (label out of sync with CNS state).
//   - unreconciledPVCs: PVCs where isActive != active (CNS state has not yet converged to
//     the CBTConfig target).
func findUnsyncedPvcCBTState(ctx context.Context, volManager volumes.Manager,
	namespace string, allBlockPVCs []pvcCBTState, active bool,
) (mismatchedPVCs, unreconciledPVCs []pvcCBTState, err error) {
	log := logger.GetLogger(ctx)
	volIDs := make([]string, 0, len(allBlockPVCs))
	for _, s := range allBlockPVCs {
		volIDs = append(volIDs, s.volumeID)
	}
	// Querying Enabled makes the logic straightforward to reason about: present in the
	// result set means isActive=true, absent means isActive=false.
	enabled, err := queryVolumesByCBTState(ctx, volManager, volIDs, cnstypes.CnsVolumeCBTStatusEnabled)
	if err != nil {
		log.Errorf("findUnsyncedPvcCBTState: namespace %q failed to query volumes by CBT state: %v", namespace, err)
		return nil, nil, err
	}
	for _, s := range allBlockPVCs {
		_, s.isActive = enabled[s.volumeID]
		if s.hasLabel != s.isActive {
			mismatchedPVCs = append(mismatchedPVCs, s)
		}
		if s.isActive != active {
			unreconciledPVCs = append(unreconciledPVCs, s)
		}
	}
	log.Infof("findUnsyncedPvcCBTState: namespace %q found %d mismatched PVC(s), %d unreconciled PVC(s) out of %d total",
		namespace, len(mismatchedPVCs), len(unreconciledPVCs), len(allBlockPVCs))
	return mismatchedPVCs, unreconciledPVCs, nil
}

// refreshUnreconciledPVCs nudges CNS to refresh its cached view from VC for PVCs whose
// CNS state does NOT yet match the CBTConfig target. Two cases arise:
//  1. CBT was applied on the VM for PVCs attached to it, but CNS has not yet synced
//     the volume state.
//  2. CBT was applied on the PVCs before CSI_Backup_API was enabled, but CNS has not
//     yet synced the volume state.
//
// PVCs backed by PVs currently attached to a PodVM are skipped: CBT state cannot be toggled
// while the volume is in use by a PodVM.
//
// After SyncVolume completes, it re-queries the actual CBT state and returns updated
// PVCs — every unreconciled PVC with its isActive field changed to the post-sync value.
//
// Best-effort: VolumeAttachment listing, SyncVolume, and re-query failures are logged
// and result in nil so the caller skips the merge.
func refreshUnreconciledPVCs(ctx context.Context, volManager volumes.Manager,
	vaLister storagelistersv1.VolumeAttachmentLister,
	namespace string, unreconciled []pvcCBTState) []pvcCBTState {
	log := logger.GetLogger(ctx)
	if len(unreconciled) == 0 {
		log.Debugf("refreshUnreconciledPVCs: namespace %q no unreconciled PVC(s); skipping SyncVolume",
			namespace)
		return nil
	}

	attachedPVs, err := loadAttachedPVNames(vaLister)
	if err != nil {
		log.Warnf("refreshUnreconciledPVCs: namespace %q failed to list VolumeAttachments; "+
			"skipping SyncVolume: %v", namespace, err)
		return nil
	}
	var toSync []pvcCBTState
	for _, s := range unreconciled {
		if _, ok := attachedPVs[s.pvc.Spec.VolumeName]; !ok {
			toSync = append(toSync, s)
		}
	}
	if skipped := len(unreconciled) - len(toSync); skipped > 0 {
		log.Infof("refreshUnreconciledPVCs: namespace %q skipping %d PVC(s) attached to a PodVM",
			namespace, skipped)
	}
	unreconciled = toSync
	if len(unreconciled) == 0 {
		log.Debugf("refreshUnreconciledPVCs: namespace %q all unreconciled PVC(s) are attached; "+
			"skipping SyncVolume", namespace)
		return nil
	}

	log.Infof("refreshUnreconciledPVCs: namespace %q syncing %d volume(s) not yet at target CBT state",
		namespace, len(unreconciled))

	syncIDs := make([]string, 0, len(unreconciled))
	for _, entry := range unreconciled {
		if err := ctx.Err(); err != nil {
			log.Infof("refreshUnreconciledPVCs: namespace %q SyncVolume loop cancelled: %v", namespace, err)
			return nil
		}
		specs := []cnstypes.CnsSyncVolumeSpec{{VolumeId: cnstypes.CnsVolumeId{Id: entry.volumeID}}}
		if _, err := volManager.SyncVolume(ctx, specs); err != nil {
			log.Warnf("refreshUnreconciledPVCs: namespace %q SyncVolume failed for PVC %s (volume %s): %v",
				namespace, entry.pvc.Name, entry.volumeID, err)
		}
		syncIDs = append(syncIDs, entry.volumeID)
	}

	// Re-query the Enabled set after sync so we have the post-sync CBT state.
	nowEnabled, err := queryVolumesByCBTState(ctx, volManager, syncIDs, cnstypes.CnsVolumeCBTStatusEnabled)
	if err != nil {
		log.Warnf("refreshUnreconciledPVCs: namespace %q re-query after SyncVolume failed; "+
			"label patches may use stale CBT state: %v", namespace, err)
		return nil
	}
	log.Debugf("refreshUnreconciledPVCs: namespace %q %d out of %d volume(s) are now CBT enabled after SyncVolume",
		namespace, len(nowEnabled), len(syncIDs))

	// Return only PVCs whose isActive changed after SyncVolume.
	var updatedPVCs []pvcCBTState
	for _, s := range unreconciled {
		wasActive := s.isActive
		_, s.isActive = nowEnabled[s.volumeID]
		if s.isActive != wasActive {
			updatedPVCs = append(updatedPVCs, s)
		}
	}

	log.Infof("refreshUnreconciledPVCs: namespace %q returning %d updated PVC(s)",
		namespace, len(updatedPVCs))
	return updatedPVCs
}

// mergeMismatchedPVCs reconciles updatedPVCs (post-SyncVolume state) into mismatched:
//   - An updated PVC already in mismatched whose label now matches isActive is removed.
//   - An updated PVC not yet in mismatched whose label does not match isActive is added.
//
// PVCs in mismatched that have no corresponding entry in updatedPVCs are left unchanged.
func mergeMismatchedPVCs(ctx context.Context, namespace string,
	mismatched, updatedPVCs []pvcCBTState) []pvcCBTState {
	log := logger.GetLogger(ctx)
	if len(updatedPVCs) == 0 {
		return mismatched
	}
	// Index updatedPVCs by volumeID for O(1) lookup.
	updated := make(map[string]pvcCBTState, len(updatedPVCs))
	for _, s := range updatedPVCs {
		updated[s.volumeID] = s
	}

	// Pass 1: keep existing mismatched entries, dropping any that are now resolved.
	kept := mismatched[:0]
	for _, s := range mismatched {
		if u, ok := updated[s.volumeID]; ok {
			// Use the refreshed isActive; drop if label now matches.
			if u.hasLabel == u.isActive {
				continue
			}
			s = u
		}
		kept = append(kept, s)
	}
	mismatched = kept

	// Pass 2: append updated PVCs that were not already in mismatched and now have a mismatch.
	inMismatched := make(map[string]struct{}, len(mismatched))
	for _, s := range mismatched {
		inMismatched[s.volumeID] = struct{}{}
	}
	for _, u := range updatedPVCs {
		if _, seen := inMismatched[u.volumeID]; seen {
			continue
		}
		if u.hasLabel != u.isActive {
			mismatched = append(mismatched, u)
		}
	}

	log.Infof("syncPvcCBTLabel: namespace %q merged %d updated PVC(s) into %d mismatched PVC(s)",
		namespace, len(updatedPVCs), len(mismatched))
	return mismatched
}

// applyCBTLabelPatches issues one JSON-merge-patch per mismatched PVC to add or remove the
// cns.vmware.com/cbt-active label. Patches are sequential (label updates are tiny, and serialising
// keeps log output tidy) and best-effort: per-PVC errors are logged at Warn and skipped so
// a single misbehaving PVC does not block the rest of the namespace from converging.
func applyCBTLabelPatches(ctx context.Context, kubeClient clientset.Interface,
	namespace string, mismatched []pvcCBTState) {
	log := logger.GetLogger(ctx)
	log.Infof("applyCBTLabelPatches: namespace %q applying %d label patches for PVCs", namespace, len(mismatched))
	for _, s := range mismatched {
		if err := ctx.Err(); err != nil {
			log.Infof("applyCBTLabelPatches: namespace %q label patch process cancelled: %v", namespace, err)
			return
		}
		patchBytes, err := buildCBTLabelPatch(s.isActive)
		if err != nil {
			log.Warnf("applyCBTLabelPatches: namespace %q failed to build label patch for volume %s: %v",
				namespace, s.volumeID, err)
			continue
		}
		_, err = kubeClient.CoreV1().PersistentVolumeClaims(namespace).Patch(ctx, s.pvc.Name,
			types.MergePatchType, patchBytes, metav1.PatchOptions{})
		if err != nil {
			log.Warnf("applyCBTLabelPatches: namespace %q failed to patch CBT label on PVC %s (volume %s): %v",
				namespace, s.pvc.Name, s.volumeID, err)
			continue
		}
		if s.isActive {
			log.Infof("applyCBTLabelPatches: namespace %q PVC %s label %s added for volume %s",
				namespace, s.pvc.Name, isCBTActiveLabel, s.volumeID)
		} else {
			log.Infof("applyCBTLabelPatches: namespace %q PVC %s label %s removed for volume %s",
				namespace, s.pvc.Name, isCBTActiveLabel, s.volumeID)
		}
	}
}

// buildCBTLabelPatch returns a JSON merge patch that either sets cns.vmware.com/cbt-active to
// "true" or removes the label (by patching it to null). Using merge-patch keeps the
// update independent of any concurrent label edit on the same PVC, since other label
// keys are not touched.
func buildCBTLabelPatch(isActive bool) ([]byte, error) {
	var labelValue interface{}
	if isActive {
		labelValue = isCBTActiveLabelVal
	} else {
		// JSON null deletes the key per RFC 7396 (JSON Merge Patch).
		labelValue = nil
	}
	patch := map[string]interface{}{
		"metadata": map[string]interface{}{
			"labels": map[string]interface{}{
				isCBTActiveLabel: labelValue,
			},
		},
	}
	return json.Marshal(patch)
}
