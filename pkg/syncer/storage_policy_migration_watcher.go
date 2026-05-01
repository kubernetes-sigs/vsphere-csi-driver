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

// This file implements the supervisor-side watcher that observes the Mobility
// Operator's storage-policy migration CRs (VirtualMachineInfraMigration for
// attached PVCs, VolumeMigration for detached PVCs) and propagates their
// terminal state into CNSVolumeInfo.Status.MigrationConditions.
//
// Discovery is annotation-driven on the SV PVC: the Mobility Operator marks
// the PVC with cns.vmware.com/migration-cr-kind and cns.vmware.com/migration-cr-name
// after creating the migration CR. There is intentionally NO ownerReference
// between the PVC and the migration CR - the lifecycles are decoupled.
//
// Per-PVC goroutine model: when we observe a PVC with migration annotations,
// we start a goroutine that polls the named migration CR every
// migrationPollInterval. We track active watchers in `activeMigrations` to
// avoid duplicate work. The goroutine exits when:
//   - the migration CR reaches a terminal state and we've propagated it, or
//   - the PVC's migration annotations are removed (e.g. user rolled back), or
//   - the migration CR is deleted, or
//   - the syncer shuts down.
package syncer

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"time"

	v1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/dynamic"
	"sigs.k8s.io/controller-runtime/pkg/client/config"

	cnsoperatorv1alpha1 "sigs.k8s.io/vsphere-csi-driver/v3/pkg/apis/cnsoperator"
	"sigs.k8s.io/vsphere-csi-driver/v3/pkg/csi/service/common"
	"sigs.k8s.io/vsphere-csi-driver/v3/pkg/csi/service/common/commonco"
	"sigs.k8s.io/vsphere-csi-driver/v3/pkg/csi/service/logger"
	"sigs.k8s.io/vsphere-csi-driver/v3/pkg/internalapis/cnsvolumeinfo"
	cnsvolumeinfov1alpha1 "sigs.k8s.io/vsphere-csi-driver/v3/pkg/internalapis/cnsvolumeinfo/v1alpha1"
	k8s "sigs.k8s.io/vsphere-csi-driver/v3/pkg/kubernetes"
)

const (
	// migrationPatchRetries is the number of retries when patching CNSVolumeInfo.
	migrationPatchRetries = 5
)

// migrationPollInterval is how often the per-PVC watcher polls the migration
// CR for status updates. Migrations may take hours, so a relaxed interval is
// fine and keeps API server load low. Declared as a var so unit tests can
// shorten it without spinning fake-time wheels.
var migrationPollInterval = 5 * time.Second

// activeMigration represents a single in-flight per-PVC migration watcher.
type activeMigration struct {
	cancel context.CancelFunc
	// crKind / crName captures the annotations the watcher was started for, so
	// we can detect when the user has changed/removed them mid-flight.
	crKind string
	crName string
}

// activeMigrations tracks per-PVC migration watchers, keyed by "<namespace>/<name>".
// Concurrent access is safe via the mutex.
var (
	activeMigrationsMu sync.Mutex
	activeMigrations   = map[string]*activeMigration{}
)

// Shared dynamic client used by all migration watchers for reading Mobility
// Operator CRs. Lazily initialized via sync.Once so that we pay the cost of
// rest.Config + client construction at most once per syncer process, rather
// than on every poll tick of every active watcher. The Once is held by
// pointer so tests can swap in a fresh one without copying it (which `go
// vet` correctly rejects).
var (
	migrationDynamicClient     dynamic.Interface
	migrationDynamicClientOnce = &sync.Once{}
	migrationDynamicClientErr  error
)

// Indirections through package-level function variables so unit tests can stub
// these out without spinning up a real Kubernetes API. Production code path
// is unchanged. The migrationXxxFn forms are the seams; the underscore-less
// names are the real implementations.
var (
	migrationDynamicClientGetter       = getMigrationDynamicClient
	migrationK8sNewClient              = k8s.NewClient
	migrationInitVolumeInfoService     = cnsvolumeinfo.InitVolumeInfoService
	migrationStartMigrationWatcher     = startMigrationWatcher
	migrationHandleStoragePolicyChange = handleVACChangeForVolumeInfo
)

// getMigrationDynamicClient returns the process-wide shared dynamic client used
// to read migration CRs from the supervisor. Safe for concurrent use.
func getMigrationDynamicClient() (dynamic.Interface, error) {
	migrationDynamicClientOnce.Do(func() {
		cfg, err := config.GetConfig()
		if err != nil {
			migrationDynamicClientErr = fmt.Errorf("failed to get k8s config: %w", err)
			return
		}
		client, err := dynamic.NewForConfig(cfg)
		if err != nil {
			migrationDynamicClientErr = fmt.Errorf("failed to create dynamic client: %w", err)
			return
		}
		migrationDynamicClient = client
	})
	return migrationDynamicClient, migrationDynamicClientErr
}

// handlePvcMigrationAnnotations is invoked from pvcUpdated whenever a PVC
// updates. It is a cheap no-op when:
//   - the VM_PVC_STORAGE_POLICY_MUTABILITY supervisor capability is disabled, or
//   - the PVC carries no migration annotations and no watcher is active for it.
//
// When a new migration appears (annotations present + no active watcher OR
// annotations changed since last seen), it starts a per-PVC watcher goroutine.
// When annotations are removed (rollback) on a PVC with an active watcher, it
// cancels the watcher.
func handlePvcMigrationAnnotations(ctx context.Context, oldPvc, newPvc *v1.PersistentVolumeClaim,
	metadataSyncer *metadataSyncInformer) {

	// Defensive guards: this hook is invoked from pvcUpdated, which is wired up
	// from the syncer's PVC informer. In some test setups the orchestrator
	// utility may not be initialized; in those cases we silently no-op.
	if commonco.ContainerOrchestratorUtility == nil ||
		!commonco.ContainerOrchestratorUtility.IsFSSEnabled(ctx, common.VMPVCStoragePolicyMutability) {
		return
	}

	key := newPvc.Namespace + "/" + newPvc.Name
	newKind := newPvc.Annotations[common.AnnMigrationCRKind]
	newName := newPvc.Annotations[common.AnnMigrationCRName]
	oldKind := ""
	oldName := ""
	if oldPvc != nil {
		oldKind = oldPvc.Annotations[common.AnnMigrationCRKind]
		oldName = oldPvc.Annotations[common.AnnMigrationCRName]
	}

	// Annotations not present on new PVC.
	if newKind == "" || newName == "" {
		// If we had an active watcher (annotations were just removed), cancel it.
		if oldKind != "" || oldName != "" {
			RemoveMigrationWatcher(ctx, key, "PVC migration annotations removed")
		}
		return
	}

	// Validate the kind early.
	if newKind != common.MigrationCRKindVMInfra && newKind != common.MigrationCRKindVolume {
		log := logger.GetLogger(ctx)
		log.Warnf("handlePvcMigrationAnnotations: unsupported %s=%q on PVC %s; ignoring",
			common.AnnMigrationCRKind, newKind, key)
		return
	}

	// If a watcher already exists for this PVC, decide whether to keep / cancel / restart.
	activeMigrationsMu.Lock()
	if existing, ok := activeMigrations[key]; ok {
		if existing.crKind == newKind && existing.crName == newName {
			activeMigrationsMu.Unlock()
			return // already watching
		}
		// Annotation values changed (Mobility re-issued the CR or different kind);
		// cancel the old watcher and fall through to start a new one.
		existing.cancel()
		delete(activeMigrations, key)
	}
	activeMigrationsMu.Unlock()

	migrationStartMigrationWatcher(ctx, newPvc.Namespace, newPvc.Name, newKind, newName, metadataSyncer)
}

// handlePvcDeletedForMigration cancels any in-flight migration watcher when
// the PVC itself is deleted (e.g. cleanup / namespace deletion). Safe to call
// for any PVC.
func handlePvcDeletedForMigration(ctx context.Context, pvc *v1.PersistentVolumeClaim) {
	if pvc == nil {
		return
	}
	RemoveMigrationWatcher(ctx, pvc.Namespace+"/"+pvc.Name, "PVC deleted")
}

func RemoveMigrationWatcher(ctx context.Context, key, reason string) {
	log := logger.GetLogger(ctx)
	activeMigrationsMu.Lock()
	defer activeMigrationsMu.Unlock()
	if existing, ok := activeMigrations[key]; ok {
		log.Infof("RemoveMigrationWatcher: stopping migration watcher for PVC %s (%s)", key, reason)
		existing.cancel()
		delete(activeMigrations, key)
	}
}

// startMigrationWatcher launches a goroutine that polls the named migration CR
// in the PVC's namespace and propagates its terminal state.
func startMigrationWatcher(parentCtx context.Context, pvcNamespace, pvcName, crKind, crName string,
	metadataSyncer *metadataSyncInformer) {

	log := logger.GetLogger(parentCtx)
	key := pvcNamespace + "/" + pvcName

	watchCtx, cancel := context.WithCancel(context.Background())
	activeMigrationsMu.Lock()
	activeMigrations[key] = &activeMigration{cancel: cancel, crKind: crKind, crName: crName}
	activeMigrationsMu.Unlock()

	log.Infof("startMigrationWatcher: starting watcher for PVC %s on %s/%s", key, crKind, crName)

	go func() {
		defer func() {
			activeMigrationsMu.Lock()
			if existing, ok := activeMigrations[key]; ok && existing.crKind == crKind && existing.crName == crName {
				delete(activeMigrations, key)
			}
			activeMigrationsMu.Unlock()
		}()

		ticker := time.NewTicker(migrationPollInterval)
		defer ticker.Stop()

		// On observation (creation already happened by the time we see the
		// annotation), patch InProgress once before entering the poll loop. We
		// best-effort patch and continue regardless of the result.
		if err := patchMigrationConditionsInProgress(watchCtx, pvcNamespace, pvcName, metadataSyncer); err != nil {
			log.Warnf("startMigrationWatcher: failed initial InProgress patch for PVC %s: %v", key, err)
		}

		for {
			select {
			case <-watchCtx.Done():
				log.Infof("startMigrationWatcher: watcher for PVC %s stopped", key)
				return
			case <-ticker.C:
			}

			cr, err := getMigrationCR(watchCtx, pvcNamespace, crKind, crName)
			if err != nil {
				if apierrors.IsNotFound(err) {
					log.Infof("startMigrationWatcher: migration CR %s/%s for PVC %s not found, exiting watcher. "+
						"Mobility Operator will handle cleanup.", crKind, crName, key)
					return
				}
				log.Warnf("startMigrationWatcher: error fetching migration CR %s/%s for PVC %s: %v",
					crKind, crName, key, err)
				continue
			}

			outcome := classifyMigrationCR(cr)
			switch outcome {
			case migrationOutcomeInProgress:
				continue
			case migrationOutcomeComplete:
				if err := propagateMigrationSuccess(watchCtx, metadataSyncer, pvcNamespace, pvcName, cr); err != nil {
					log.Errorf("startMigrationWatcher: failed to propagate success for PVC %s: %v", key, err)
					// Don't return; we want to retry on the next tick rather than
					// leave CNSVolumeInfo in InProgress forever.
					continue
				}
				return
			case migrationOutcomeError:
				if err := patchMigrationConditionsTerminal(watchCtx, pvcNamespace, pvcName,
					cnsvolumeinfov1alpha1.MigrationConditionError, metadataSyncer); err != nil {
					log.Errorf("startMigrationWatcher: failed to patch Error condition for PVC %s: %v", key, err)
					continue
				}
				return
			case migrationOutcomeInfeasible:
				if err := patchMigrationConditionsTerminal(watchCtx, pvcNamespace, pvcName,
					cnsvolumeinfov1alpha1.MigrationConditionInfeasible, metadataSyncer); err != nil {
					log.Errorf("startMigrationWatcher: failed to patch Infeasible condition for PVC %s: %v", key, err)
					continue
				}
				return
			}
		}
	}()
}

// migrationOutcome enumerates the terminal-state classification of a migration CR.
type migrationOutcome int

const (
	migrationOutcomeInProgress migrationOutcome = iota
	migrationOutcomeComplete
	migrationOutcomeError
	migrationOutcomeInfeasible
)

// classifyMigrationCR interprets the migration CR's status into a single
// migrationOutcome. The Mobility Operator (v1alpha4) reports outcome strictly
// through status.conditions[].
//
// Contract (anchored on the "Ready" condition):
//   - Ready.status == "True"  -> Complete
//   - Ready.status == "False":
//   - if Ready.reason is in {"Infeasible","NotSupported","UnsupportedZoneChange"} -> Infeasible
//   - otherwise -> Error
//   - Ready absent, Ready.status == "Unknown", or anything else -> InProgress
//
// Other conditions (Validated, StorageInSync) carry sub-step progress and do
// not on their own indicate a terminal outcome.
func classifyMigrationCR(cr *unstructured.Unstructured) migrationOutcome {
	if cr == nil {
		return migrationOutcomeInProgress
	}
	conditions, _, _ := unstructured.NestedSlice(cr.Object, "status", "conditions")
	for _, c := range conditions {
		cm, ok := c.(map[string]interface{})
		if !ok {
			continue
		}
		if t, _ := cm["type"].(string); t != "Ready" {
			continue
		}
		status, _ := cm["status"].(string)
		switch status {
		case "True":
			return migrationOutcomeComplete
		case "False":
			reason, _ := cm["reason"].(string)
			switch reason {
			case "Infeasible", "NotSupported", "UnsupportedZoneChange":
				return migrationOutcomeInfeasible
			}
			return migrationOutcomeError
		}
		// Ready=Unknown or unset status string -> still in progress.
		return migrationOutcomeInProgress
	}
	return migrationOutcomeInProgress
}

// getMigrationCR fetches the named migration CR from the supervisor namespace.
// Uses the shared dynamic client (one per process) instead of constructing a
// new client on every poll tick.
func getMigrationCR(ctx context.Context, namespace, kind, name string) (*unstructured.Unstructured, error) {
	gvr, err := migrationCRGVR(kind)
	if err != nil {
		return nil, err
	}
	dyn, err := migrationDynamicClientGetter()
	if err != nil {
		return nil, err
	}
	return dyn.Resource(gvr).Namespace(namespace).Get(ctx, name, metav1.GetOptions{})
}

// migrationCRGVR returns the GroupVersionResource for the given migration CR kind.
func migrationCRGVR(kind string) (schema.GroupVersionResource, error) {
	switch kind {
	case common.MigrationCRKindVMInfra:
		return schema.GroupVersionResource{
			Group:    common.MobilityOperatorGroup,
			Version:  common.MobilityOperatorVersion,
			Resource: common.MobilityOperatorVMInfraResource,
		}, nil
	case common.MigrationCRKindVolume:
		return schema.GroupVersionResource{
			Group:    common.MobilityOperatorGroup,
			Version:  common.MobilityOperatorVersion,
			Resource: common.MobilityOperatorVolumeResource,
		}, nil
	}
	return schema.GroupVersionResource{}, fmt.Errorf("unsupported migration CR kind %q", kind)
}

// patchMigrationConditionsInProgress sets MigrationConditions to InProgress on
// the volume's CNSVolumeInfo CR. The volume is identified by the SV PVC name
// (which is also the FCD volumeID per the WCP convention).
func patchMigrationConditionsInProgress(ctx context.Context, pvcNamespace, pvcName string,
	metadataSyncer *metadataSyncInformer) error {
	return patchMigrationConditionsType(ctx, pvcNamespace, pvcName,
		cnsvolumeinfov1alpha1.MigrationConditionInProgress, metadataSyncer)
}

// patchMigrationConditionsTerminal sets MigrationConditions to a terminal type
// (Error or Infeasible). Spec is NOT changed.
func patchMigrationConditionsTerminal(ctx context.Context, pvcNamespace, pvcName, conditionType string,
	metadataSyncer *metadataSyncInformer) error {
	return patchMigrationConditionsType(ctx, pvcNamespace, pvcName, conditionType, metadataSyncer)
}

func patchMigrationConditionsType(ctx context.Context, pvcNamespace, pvcName, conditionType string,
	metadataSyncer *metadataSyncInformer) error {
	volumeID, err := volumeIDForPVC(ctx, pvcNamespace, pvcName, metadataSyncer)
	if err != nil {
		return err
	}
	patch := map[string]interface{}{
		"status": map[string]interface{}{
			"migrationConditions": []map[string]interface{}{
				{
					"type":               conditionType,
					"status":             string(metav1.ConditionTrue),
					"reason":             conditionType,
					"message":            "Set by CSI Syncer based on Mobility Operator migration CR",
					"lastTransitionTime": time.Now().UTC().Format(time.RFC3339),
				},
			},
		},
	}
	patchBytes, err := json.Marshal(patch)
	if err != nil {
		return fmt.Errorf("failed to marshal CNSVolumeInfo status patch: %w", err)
	}
	volumeInfoSvc, err := getVolumeInfoService(ctx)
	if err != nil {
		return err
	}
	return volumeInfoSvc.PatchVolumeInfoStatus(ctx, volumeID, patchBytes, migrationPatchRetries)
}

// propagateMigrationSuccess performs the single-handler PATCH + quota update
// when a migration CR reaches a successful terminal state. It:
//  1. PATCHes CNSVolumeInfo.Spec with the new VolumeAttributeClassName,
//     K8sCompliantName, and StoragePolicyID. StorageClassName is intentionally
//     NOT touched because a PVC's storage class is immutable.
//  2. PATCHes CNSVolumeInfo.Status.MigrationConditions = Complete with a
//     "from -> to" message.
//  3. Calls migrationHandleStoragePolicyChange() directly so that the
//     StoragePolicyUsage CRs (PVC capacity + AggregatedSnapshotSize) are moved
//     from the OLD policy's SPU (keyed by old VAC, or StorageClassName when
//     there was no old VAC) to the NEW policy's SPU (keyed by new VAC).
func propagateMigrationSuccess(ctx context.Context, metadataSyncer *metadataSyncInformer,
	pvcNamespace, pvcName string, migrationCR *unstructured.Unstructured) error {
	log := logger.GetLogger(ctx)

	// Resolve PVC -> volumeID and current CNSVolumeInfo (the OLD state, needed
	// for the quota helper).
	pvc, err := metadataSyncer.pvcLister.PersistentVolumeClaims(pvcNamespace).Get(pvcName)
	if err != nil {
		return fmt.Errorf("failed to get PVC %s/%s from lister: %w", pvcNamespace, pvcName, err)
	}
	volumeID := pvc.Spec.VolumeName // PV name; the volumeID we want is the CSI handle below.
	if pvc.Spec.VolumeName == "" {
		return fmt.Errorf("PVC %s/%s has no bound volume yet", pvcNamespace, pvcName)
	}
	pv, err := metadataSyncer.pvLister.Get(volumeID)
	if err != nil {
		return fmt.Errorf("failed to get PV %s: %w", volumeID, err)
	}
	if pv.Spec.CSI == nil || pv.Spec.CSI.VolumeHandle == "" {
		return fmt.Errorf("PV %s has no CSI volume handle", volumeID)
	}
	csiVolumeID := pv.Spec.CSI.VolumeHandle

	volumeInfoSvc, err := getVolumeInfoService(ctx)
	if err != nil {
		return err
	}
	oldCVI, err := volumeInfoSvc.GetVolumeInfoForVolumeID(ctx, csiVolumeID)
	if err != nil {
		return fmt.Errorf("failed to get CNSVolumeInfo for volume %s: %w", csiVolumeID, err)
	}

	specPatchBytes, statusPatchBytes, newCVI, newVAC, newStoragePolicyID, err :=
		buildMigrationSuccessPatches(oldCVI, pvc, migrationCR, pvcName, time.Now().UTC())
	if err != nil {
		return err
	}
	// Apply spec part first via PatchVolumeInfo, then status via PatchVolumeInfoStatus.
	// PatchVolumeInfoStatus only touches the status subresource; spec lives separately.
	if len(specPatchBytes) > 0 {
		if err := volumeInfoSvc.PatchVolumeInfo(ctx, csiVolumeID, specPatchBytes, migrationPatchRetries); err != nil {
			return fmt.Errorf("failed to patch CNSVolumeInfo spec: %w", err)
		}
	}
	if err := volumeInfoSvc.PatchVolumeInfoStatus(ctx, csiVolumeID, statusPatchBytes,
		migrationPatchRetries); err != nil {
		return fmt.Errorf("failed to patch CNSVolumeInfo status: %w", err)
	}
	log.Infof("propagateMigrationSuccess: Volume %s migration successfully completed by Mobility Operator: "+
		"VAC %q (policy %s) -> VAC %q (policy %s) [StorageClass %q unchanged]",
		csiVolumeID,
		oldCVI.Spec.VolumeAttributeClassName, oldCVI.Spec.StoragePolicyID,
		newVAC, newStoragePolicyID,
		oldCVI.Spec.StorageClassName)

	// Quota update: deduct from OLD SPU (keyed by old VAC, or StorageClassName
	// when no prior VAC) and add to NEW SPU (keyed by new VAC). We invoke the
	// existing helper directly (Option 1 - single handler does both jobs)
	// rather than relying on the cnsVolumeInfoCRUpdated informer cascade.
	//
	// `metadataSyncer.cnsOperatorClient` is only populated in Guest mode; in
	// WCP mode it's nil. We therefore construct a fresh CnsOperator client
	// from the in-cluster config here, mirroring what cnsVolumeInfoCRUpdated
	// does. Failure to build the client is logged but does NOT fail the whole
	// migration - the CNSVolumeInfo spec/status PATCHes have already landed;
	// the quota rebalance is best-effort and full-sync will reconcile it later.
	quotaClient := metadataSyncer.cnsOperatorClient
	if quotaClient == nil {
		restConfig, err := config.GetConfig()
		if err != nil {
			log.Errorf("propagateMigrationSuccess: failed to get kube config "+
				"for quota update on volume %s: %v", csiVolumeID, err)
			return nil
		}
		quotaClient, err = k8s.NewClientForGroup(ctx, restConfig, cnsoperatorv1alpha1.GroupName)
		if err != nil {
			log.Errorf("propagateMigrationSuccess: failed to create CnsOperator client "+
				"for quota update on volume %s: %v", csiVolumeID, err)
			return nil
		}
	}
	migrationHandleStoragePolicyChange(ctx, quotaClient, *oldCVI, newCVI)
	return nil
}

// buildMigrationSuccessPatches is the pure / side-effect-free portion of
// propagateMigrationSuccess. Given the old CNSVolumeInfo, the bound PVC, and
// the Mobility-Operator migration CR, it derives:
//
//   - the spec-only PATCH bytes (or nil if the spec already matches the
//     migration result),
//   - the status-only PATCH bytes carrying MigrationConditions=Complete with
//     a human-readable "from -> to" message,
//   - the projected "new" CNSVolumeInfo used by the quota cascade
//     (handleVACChangeForVolumeInfo only looks at Spec fields), and
//   - the resolved (newVAC, newStoragePolicyID) for logging.
//
// What is patched into CNSVolumeInfo.Spec:
//   - VolumeAttributeClassName = PVC's spec.volumeAttributesClassName
//   - K8sCompliantName         = same as VolumeAttributeClassName (VAC names
//     are created from the immutable SPBM K8sCompliantName, so they are
//     identical by construction)
//   - StoragePolicyID          = policy ID resolved from the migration CR
//
// What is intentionally NOT touched:
//   - StorageClassName: a PVC's storage class is immutable in Kubernetes, so
//     the CNSVolumeInfo's StorageClassName must remain stable across migrations.
//
// The `now` parameter is injected so the test can pin lastTransitionTime.
func buildMigrationSuccessPatches(
	oldCVI *cnsvolumeinfov1alpha1.CNSVolumeInfo,
	pvc *v1.PersistentVolumeClaim,
	migrationCR *unstructured.Unstructured,
	pvcName string,
	now time.Time,
) ([]byte, []byte, cnsvolumeinfov1alpha1.CNSVolumeInfo, string, string, error) {
	// Resolve the new VAC name from the PVC. By the time we reach success,
	// csi-resizer has already validated PVC.Spec.VolumeAttributesClassName.
	newVAC := ""
	if pvc != nil && pvc.Spec.VolumeAttributesClassName != nil {
		newVAC = *pvc.Spec.VolumeAttributesClassName
	}
	newStoragePolicyID := lookupNewStoragePolicyID(migrationCR, pvcName)

	// Build the projected "new" CNSVolumeInfo (used by the quota helper).
	// StorageClassName is preserved verbatim.
	newCVI := *oldCVI
	if newVAC != "" {
		newCVI.Spec.VolumeAttributeClassName = newVAC
		// K8sCompliantName mirrors VAC name (see godoc on this function).
		newCVI.Spec.K8sCompliantName = newVAC
	}
	if newStoragePolicyID != "" {
		newCVI.Spec.StoragePolicyID = newStoragePolicyID
	}

	specPatch := map[string]interface{}{}
	if newVAC != "" && newVAC != oldCVI.Spec.VolumeAttributeClassName {
		specPatch["volumeAttributeClassName"] = newVAC
		specPatch["k8sCompliantName"] = newVAC
	}
	if newStoragePolicyID != "" && newStoragePolicyID != oldCVI.Spec.StoragePolicyID {
		specPatch["storagePolicyID"] = newStoragePolicyID
	}

	statusPatch := map[string]interface{}{
		"status": map[string]interface{}{
			"migrationConditions": []map[string]interface{}{
				{
					"type":   cnsvolumeinfov1alpha1.MigrationConditionComplete,
					"status": string(metav1.ConditionTrue),
					"reason": cnsvolumeinfov1alpha1.MigrationConditionComplete,
					"message": fmt.Sprintf(
						"Migration completed successfully: VAC %q (policy %s) -> VAC %q (policy %s); "+
							"StorageClass %q unchanged",
						oldCVI.Spec.VolumeAttributeClassName, oldCVI.Spec.StoragePolicyID,
						newVAC, newStoragePolicyID,
						oldCVI.Spec.StorageClassName),
					"lastTransitionTime": now.Format(time.RFC3339),
				},
			},
		},
	}

	statusBytes, err := json.Marshal(statusPatch)
	if err != nil {
		return nil, nil, newCVI, newVAC, newStoragePolicyID,
			fmt.Errorf("failed to marshal CNSVolumeInfo status patch: %w", err)
	}

	var specBytes []byte
	if len(specPatch) > 0 {
		specBytes, err = json.Marshal(map[string]interface{}{"spec": specPatch})
		if err != nil {
			return nil, nil, newCVI, newVAC, newStoragePolicyID,
				fmt.Errorf("failed to marshal CNSVolumeInfo spec patch: %w", err)
		}
	}
	return specBytes, statusBytes, newCVI, newVAC, newStoragePolicyID, nil
}

// volumeIDForPVC resolves the CSI volume handle for the given PVC.
//
// Follows the established syncer pattern (see pvcUpdated in metadatasyncer.go):
// read from the informer caches first and only fall back to a direct API
// server call when the lister reports NotFound. This avoids creating a fresh
// k8s client on every poll tick. metadataSyncer may be nil in unit-test paths
// that do not exercise the lister fallback.
func volumeIDForPVC(ctx context.Context, pvcNamespace, pvcName string,
	metadataSyncer *metadataSyncInformer) (string, error) {

	var (
		pvc *v1.PersistentVolumeClaim
		err error
	)
	if metadataSyncer != nil && metadataSyncer.pvcLister != nil {
		pvc, err = metadataSyncer.pvcLister.PersistentVolumeClaims(pvcNamespace).Get(pvcName)
		if err != nil && !apierrors.IsNotFound(err) {
			return "", fmt.Errorf("failed to list PVC %s/%s from cache: %w", pvcNamespace, pvcName, err)
		}
	}
	if pvc == nil {
		k8sClient, cerr := migrationK8sNewClient(ctx)
		if cerr != nil {
			return "", fmt.Errorf("failed to create k8s client: %w", cerr)
		}
		pvc, err = k8sClient.CoreV1().PersistentVolumeClaims(pvcNamespace).
			Get(ctx, pvcName, metav1.GetOptions{})
		if err != nil {
			return "", fmt.Errorf("failed to get PVC %s/%s: %w", pvcNamespace, pvcName, err)
		}
	}
	if pvc.Spec.VolumeName == "" {
		return "", fmt.Errorf("PVC %s/%s has no bound volume", pvcNamespace, pvcName)
	}

	var pv *v1.PersistentVolume
	if metadataSyncer != nil && metadataSyncer.pvLister != nil {
		pv, err = metadataSyncer.pvLister.Get(pvc.Spec.VolumeName)
		if err != nil && !apierrors.IsNotFound(err) {
			return "", fmt.Errorf("failed to list PV %s from cache: %w", pvc.Spec.VolumeName, err)
		}
	}
	if pv == nil {
		k8sClient, cerr := migrationK8sNewClient(ctx)
		if cerr != nil {
			return "", fmt.Errorf("failed to create k8s client: %w", cerr)
		}
		pv, err = k8sClient.CoreV1().PersistentVolumes().
			Get(ctx, pvc.Spec.VolumeName, metav1.GetOptions{})
		if err != nil {
			return "", fmt.Errorf("failed to get PV %s: %w", pvc.Spec.VolumeName, err)
		}
	}
	if pv.Spec.CSI == nil || pv.Spec.CSI.VolumeHandle == "" {
		return "", fmt.Errorf("PV %s has no CSI volume handle", pv.Name)
	}
	return pv.Spec.CSI.VolumeHandle, nil
}

// getVolumeInfoService returns the VolumeInfoService singleton.
//
// The syncer initializes the package-level volumeInfoService at startup (see
// metadatasyncer.go). We prefer that instance to avoid going through
// InitVolumeInfoService on every patch. If for some reason it is not yet
// initialized (e.g. test setup, late enablement), we fall back to
// InitVolumeInfoService which is itself idempotent.
func getVolumeInfoService(ctx context.Context) (cnsvolumeinfo.VolumeInfoService, error) {
	if volumeInfoService != nil {
		return volumeInfoService, nil
	}
	svc, err := migrationInitVolumeInfoService(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to get VolumeInfoService: %w", err)
	}
	return svc, nil
}

// lookupNewStoragePolicyID extracts the target storage policy ID from a
// migration CR. The CR shape differs by kind (Mobility Operator v1alpha4):
//
// VirtualMachineInfraMigration (attached PVCs) - per-volume policy IDs under
// spec.targetStorage.volumes[]:
//
//	spec:
//	  targetStorage:
//	    homeStoragePolicyID: "policy-uuid-vm-home"
//	    volumes:
//	      - pvcName: "pvc-data-1"
//	        storagePolicyID: "policy-uuid-silver"
//	      - pvcName: "pvc-logs-2"
//	        storagePolicyID: "policy-uuid-gold"
//
// VolumeMigration (detached PVCs) - single top-level policy ID in
// spec.storagePolicyID (the CR is per-PVC, so no per-volume list):
//
//	spec:
//	  storagePolicyID: "policy-uuid-gold"
//
// Returns "" if the CR is nil or the policy ID is not found in the expected
// location for the CR's kind.
func lookupNewStoragePolicyID(cr *unstructured.Unstructured, pvcName string) string {
	if cr == nil {
		return ""
	}
	switch cr.GetKind() {
	case common.MigrationCRKindVMInfra:
		// VirtualMachineInfraMigration v1alpha4: spec.targetStorage.volumes[].
		volumes, found, err := unstructured.NestedSlice(cr.Object, "spec", "targetStorage", "volumes")
		if err != nil || !found {
			return ""
		}
		for _, v := range volumes {
			vm, ok := v.(map[string]interface{})
			if !ok {
				continue
			}
			if name, _ := vm["pvcName"].(string); name == pvcName {
				if pid, _ := vm["storagePolicyID"].(string); pid != "" {
					return pid
				}
			}
		}
		return ""
	case common.MigrationCRKindVolume:
		// VolumeMigration v1alpha4: single storage policy ID at spec.storagePolicyID.
		pid, _, _ := unstructured.NestedString(cr.Object, "spec", "storagePolicyID")
		return pid
	default:
		return ""
	}
}
