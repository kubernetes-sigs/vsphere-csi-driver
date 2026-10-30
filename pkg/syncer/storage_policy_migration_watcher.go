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
//   - at start, CNSVolumeInfo already records the CR's current terminal state
//     for the same target policy (a relaunch after the migration finished), or
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
	k8stypes "k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/dynamic"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/config"

	"sigs.k8s.io/vsphere-csi-driver/v3/pkg/csi/service/common"
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
	// recheck is set, under activeMigrationsMu, when the PVC's VAC changes while
	// the watcher is active. The update does not start a new watcher, so the
	// watcher checks this flag before exiting on a terminal result and keeps
	// polling for the new target instead.
	recheck bool
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
	migrationResolveTargetPolicyID     = resolveMigrationTargetPolicyID
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
//   - the VM/PVC storage-policy mutability feature is disabled, or
//   - the PVC carries no migration annotations and no watcher is active for it.
//
// When a new migration appears (annotations present + no active watcher OR
// annotations changed since last seen), it starts a per-PVC watcher goroutine.
// When annotations are removed (rollback) on a PVC with an active watcher, it
// cancels the watcher.
func handlePvcMigrationAnnotations(ctx context.Context, oldPvc, newPvc *v1.PersistentVolumeClaim,
	metadataSyncer *metadataSyncInformer) {

	// pvcUpdated is wired up from the syncer's PVC informer on both the Supervisor and a Guest,
	// so gate on the flavor-resolved value rather than querying a hardcoded FSS/capability name
	// here: the Supervisor capability name is not a key in the guest internal FSS ConfigMap.
	if !IsVMPVCStoragePolicyMutabilityEnabled {
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
			// A retarget or rollback patches the CR in place, so the
			// annotations stay the same while the VAC changes.
			if oldPvc != nil && !equalVACName(oldPvc.Spec.VolumeAttributesClassName,
				newPvc.Spec.VolumeAttributesClassName) {
				existing.recheck = true
			}
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
	self := &activeMigration{cancel: cancel, crKind: crKind, crName: crName}
	activeMigrationsMu.Lock()
	activeMigrations[key] = self
	activeCount := len(activeMigrations)
	activeMigrationsMu.Unlock()

	log.Infof("startMigrationWatcher: launching watcher goroutine for PVC %s on %s/%s (%d active)",
		key, crKind, crName, activeCount)

	// finish is called on a terminal result. It removes this watcher and
	// returns true, unless the PVC's VAC changed while the watcher was active;
	// then it clears the flag and returns false so the watcher keeps polling
	// for the new target. Removing the watcher under the same lock that sets
	// the flag means a later VAC change starts a new watcher instead.
	finish := func() bool {
		activeMigrationsMu.Lock()
		defer activeMigrationsMu.Unlock()
		if activeMigrations[key] == self && self.recheck {
			self.recheck = false
			return false
		}
		if activeMigrations[key] == self {
			delete(activeMigrations, key)
		}
		return true
	}

	go func() {
		defer func() {
			activeMigrationsMu.Lock()
			if activeMigrations[key] == self {
				delete(activeMigrations, key)
			}
			remaining := len(activeMigrations)
			activeMigrationsMu.Unlock()
			log.Infof("startMigrationWatcher: watcher goroutine for PVC %s exited (%d active remaining)",
				key, remaining)
		}()

		ticker := time.NewTicker(migrationPollInterval)
		defer ticker.Stop()

		// On observation (creation already happened by the time we see the
		// annotation), patch InProgress once before entering the poll loop. We
		// best-effort patch and continue regardless of the result. The patch
		// records the target policy so ControllerModifyVolume ignores any
		// terminal condition left over from a previous modify.
		_, lastPatchedTargetPolicyID, err := migrationResolveTargetPolicyID(watchCtx, pvcNamespace, pvcName,
			metadataSyncer)
		if err != nil {
			log.Warnf("startMigrationWatcher: failed to resolve target storage policy for PVC %s: %v", key, err)
		} else if migrationResultAlreadyRecorded(watchCtx, pvcNamespace, pvcName, crKind, crName,
			lastPatchedTargetPolicyID, metadataSyncer) && finish() {
			log.Infof("startMigrationWatcher: CNSVolumeInfo for PVC %s already records the final result "+
				"of %s/%s for target policy %q, exiting watcher", key, crKind, crName, lastPatchedTargetPolicyID)
			return
		}
		if err := patchMigrationConditionsInProgress(watchCtx, pvcNamespace, pvcName, lastPatchedTargetPolicyID,
			metadataSyncer); err != nil {
			log.Warnf("startMigrationWatcher: failed initial InProgress patch for PVC %s: %v", key, err)
		}

		for {
			select {
			case <-watchCtx.Done():
				return
			case <-ticker.C:
				// Re-resolve the target on every tick: a retarget or rollback
				// changes the PVC's VAC while this watcher keeps running. The
				// same PVC snapshot is used for success propagation, so the VAC
				// name written to CNSVolumeInfo always matches targetPolicyID.
				pvc, targetPolicyID, err := migrationResolveTargetPolicyID(watchCtx, pvcNamespace, pvcName,
					metadataSyncer)
				if err != nil {
					log.Warnf("startMigrationWatcher: failed to resolve target storage policy for PVC %s: %v",
						key, err)
					continue
				}
				if targetPolicyID != lastPatchedTargetPolicyID {
					log.Infof("startMigrationWatcher: target storage policy for PVC %s changed from %q to %q",
						key, lastPatchedTargetPolicyID, targetPolicyID)
					if err := patchMigrationConditionsInProgress(watchCtx, pvcNamespace, pvcName, targetPolicyID,
						metadataSyncer); err != nil {
						log.Warnf("startMigrationWatcher: failed InProgress patch for PVC %s: %v", key, err)
					} else {
						lastPatchedTargetPolicyID = targetPolicyID
					}
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

				outcome := classifyMigrationCR(cr, pvcName, targetPolicyID)
				switch outcome {
				case migrationOutcomeInProgress:
					log.Debugf("startMigrationWatcher: migration CR %s/%s for PVC %s (target policy %q) "+
						"is not terminal or its Ready condition is not current; polling again",
						crKind, crName, key, targetPolicyID)
					continue
				case migrationOutcomeComplete:
					if err := propagateMigrationSuccess(watchCtx, metadataSyncer, pvc, cr); err != nil {
						log.Errorf("startMigrationWatcher: failed to propagate success for PVC %s: %v", key, err)
						// Don't return; we want to retry on the next tick rather than
						// leave CNSVolumeInfo in InProgress forever.
						continue
					}
				case migrationOutcomeInfeasible:
					if err := patchMigrationConditionsTerminal(watchCtx, pvcNamespace, pvcName,
						cnsvolumeinfov1alpha1.MigrationConditionInfeasible, targetPolicyID, metadataSyncer); err != nil {
						log.Errorf("startMigrationWatcher: failed to patch Infeasible condition for PVC %s: %v", key, err)
						continue
					}
				}
				if finish() {
					return
				}
				log.Infof("startMigrationWatcher: VAC of PVC %s changed while recording the result for target "+
					"policy %q; polling for the new target", key, targetPolicyID)
			}
		}
	}()
}

// migrationOutcome enumerates the terminal-state classification of a migration CR.
type migrationOutcome int

const (
	migrationOutcomeInProgress migrationOutcome = iota
	migrationOutcomeComplete
	migrationOutcomeInfeasible
)

// classifyMigrationCR interprets the migration CR's status into a single
// migrationOutcome. The Mobility Operator (v1alpha4) reports outcome strictly
// through status.conditions[].
//
// The Ready condition is acted on only when it is current, i.e. both:
//   - Ready.observedGeneration == metadata.generation, and
//   - the CR spec's storagePolicyID for pvcName == expectedPolicyID (the policy
//     CSI is modifying to).
//
// Retarget and rollback patch the CR in place, so until the Mobility Operator
// reconciles again Ready can still carry the previous run's outcome. A Ready
// that is not current -> InProgress.
//
// Contract for a current Ready:
//   - Ready.status == "True" -> Complete
//   - Ready.status == "False" with reason in
//     {"Infeasible","NotSupported","UnsupportedZoneChange"} -> Infeasible
//   - Ready.status == "False" with any other reason -> InProgress (the
//     migration is running or retrying, e.g. during a Storage vMotion)
//   - Ready absent, Ready.status == "Unknown", or anything else -> InProgress
//
// Other conditions (Validated, StorageInSync) carry sub-step progress and do
// not on their own indicate a terminal outcome.
func classifyMigrationCR(cr *unstructured.Unstructured, pvcName, expectedPolicyID string) migrationOutcome {
	if cr == nil {
		return migrationOutcomeInProgress
	}
	if expectedPolicyID == "" || lookupNewStoragePolicyID(cr, pvcName) != expectedPolicyID {
		// The CR does not (yet) target the policy CSI is modifying to.
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
		observedGeneration, found, err := unstructured.NestedInt64(cm, "observedGeneration")
		if err != nil || !found || observedGeneration != cr.GetGeneration() {
			// Ready was written for an older spec.
			return migrationOutcomeInProgress
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
		}
		// Ready=False with a non-terminal reason, Ready=Unknown, or unset
		// status string -> still in progress.
		return migrationOutcomeInProgress
	}
	return migrationOutcomeInProgress
}

// resolveMigrationTargetPolicyID returns the PVC from the lister and the
// storage policy ID that CSI is modifying it to: the storagePolicyID parameter
// of the VAC named in the PVC's spec.volumeAttributesClassName. Callers pass
// the returned PVC to propagateMigrationSuccess so that the VAC name recorded
// on success comes from the same snapshot as the policy ID.
//
// The VAC is read on every call rather than cached: once no PVC references it,
// a VAC can be deleted and recreated with the same name and a different
// policy. One GET per poll tick matches the cost of reading the migration CR.
func resolveMigrationTargetPolicyID(ctx context.Context, pvcNamespace, pvcName string,
	metadataSyncer *metadataSyncInformer) (*v1.PersistentVolumeClaim, string, error) {
	if metadataSyncer == nil || metadataSyncer.pvcLister == nil {
		return nil, "", fmt.Errorf("PVC lister is not initialized")
	}
	pvc, err := metadataSyncer.pvcLister.PersistentVolumeClaims(pvcNamespace).Get(pvcName)
	if err != nil {
		return nil, "", fmt.Errorf("failed to get PVC %s/%s from lister: %w", pvcNamespace, pvcName, err)
	}
	if pvc.Spec.VolumeAttributesClassName == nil || *pvc.Spec.VolumeAttributesClassName == "" {
		return nil, "", fmt.Errorf("PVC %s/%s has no VolumeAttributesClassName", pvcNamespace, pvcName)
	}
	vacName := *pvc.Spec.VolumeAttributesClassName
	k8sClient, err := migrationK8sNewClient(ctx)
	if err != nil {
		return nil, "", fmt.Errorf("failed to create k8s client: %w", err)
	}
	vac, err := k8sClient.StorageV1().VolumeAttributesClasses().Get(ctx, vacName, metav1.GetOptions{})
	if err != nil {
		return nil, "", fmt.Errorf("failed to get VolumeAttributesClass %s: %w", vacName, err)
	}
	policyID := getStoragePolicyIDFromVAC(vac)
	if policyID == "" {
		return nil, "", fmt.Errorf("VolumeAttributesClass %s has no storagePolicyID parameter", vacName)
	}
	return pvc, policyID, nil
}

// equalVACName reports whether two PVC spec.volumeAttributesClassName values
// name the same VAC, treating nil and "" as equal.
func equalVACName(a, b *string) bool {
	derefOrEmpty := func(s *string) string {
		if s == nil {
			return ""
		}
		return *s
	}
	return derefOrEmpty(a) == derefOrEmpty(b)
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
// the volume's CNSVolumeInfo CR, for the given target storage policy. The
// volume is identified by the SV PVC name (which is also the FCD volumeID per
// the WCP convention).
func patchMigrationConditionsInProgress(ctx context.Context, pvcNamespace, pvcName, targetPolicyID string,
	metadataSyncer *metadataSyncInformer) error {
	return patchMigrationConditionsType(ctx, pvcNamespace, pvcName,
		cnsvolumeinfov1alpha1.MigrationConditionInProgress, targetPolicyID, metadataSyncer)
}

// patchMigrationConditionsTerminal sets MigrationConditions to a terminal
// failure type (Infeasible) for the given target storage policy. Spec is NOT
// changed.
func patchMigrationConditionsTerminal(ctx context.Context, pvcNamespace, pvcName, conditionType,
	targetPolicyID string, metadataSyncer *metadataSyncInformer) error {
	return patchMigrationConditionsType(ctx, pvcNamespace, pvcName, conditionType, targetPolicyID, metadataSyncer)
}

// patchMigrationConditionsType replaces MigrationConditions with a single
// condition of conditionType and records targetPolicyID in
// MigrationTargetStoragePolicyID in the same status patch.
func patchMigrationConditionsType(ctx context.Context, pvcNamespace, pvcName, conditionType,
	targetPolicyID string, metadataSyncer *metadataSyncInformer) error {
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
			"migrationTargetStoragePolicyID": targetPolicyID,
		},
	}
	patchBytes, err := json.Marshal(patch)
	if err != nil {
		return fmt.Errorf("failed to marshal CNSVolumeInfo status patch: %w", err)
	}
	cvi := cnsVolumeInfoObj(volumeID)
	return patchCNSVolumeInfoStatusWithRetry(ctx, metadataSyncer.cnsOperatorClient, cvi, patchBytes,
		migrationPatchRetries)
}

// migrationResultAlreadyRecorded reports whether the PVC's CNSVolumeInfo
// already holds the final result that the migration CR currently reports for
// targetPolicyID. The Mobility Operator keeps the PVC migration annotations
// until it deletes the CR, so a finished migration starts a new watcher on
// every PVC update and on syncer restart. Without this check that watcher
// would overwrite the final result with InProgress until its first poll.
// Any lookup error returns false, so the caller falls back to polling.
func migrationResultAlreadyRecorded(ctx context.Context, pvcNamespace, pvcName, crKind, crName,
	targetPolicyID string, metadataSyncer *metadataSyncInformer) bool {
	if targetPolicyID == "" || metadataSyncer.cnsOperatorClient == nil {
		return false
	}
	cr, err := getMigrationCR(ctx, pvcNamespace, crKind, crName)
	if err != nil {
		return false
	}
	var conditionType string
	switch classifyMigrationCR(cr, pvcName, targetPolicyID) {
	case migrationOutcomeComplete:
		conditionType = cnsvolumeinfov1alpha1.MigrationConditionComplete
	case migrationOutcomeInfeasible:
		conditionType = cnsvolumeinfov1alpha1.MigrationConditionInfeasible
	default:
		return false
	}
	volumeID, err := volumeIDForPVC(ctx, pvcNamespace, pvcName, metadataSyncer)
	if err != nil {
		return false
	}
	cvi := &cnsvolumeinfov1alpha1.CNSVolumeInfo{}
	if err := metadataSyncer.cnsOperatorClient.Get(ctx, client.ObjectKeyFromObject(cnsVolumeInfoObj(volumeID)),
		cvi); err != nil {
		return false
	}
	if cvi.Status.MigrationTargetStoragePolicyID != targetPolicyID {
		return false
	}
	// Complete is written after the spec patch, so also require the spec to
	// match before treating the success as fully propagated.
	if conditionType == cnsvolumeinfov1alpha1.MigrationConditionComplete &&
		cvi.Spec.StoragePolicyID != targetPolicyID {
		return false
	}
	for _, c := range cvi.Status.MigrationConditions {
		if c.Type == conditionType && c.Status == metav1.ConditionTrue {
			return true
		}
	}
	return false
}

// propagateMigrationSuccess performs the single-handler PATCH + quota update
// when a migration CR reaches a successful terminal state. It:
//  1. PATCHes CNSVolumeInfo.Spec with the new VolumeAttributeClassName,
//     K8sCompliantName, and StoragePolicyID. StorageClassName is intentionally
//     NOT touched because a PVC's storage class is immutable.
//  2. PATCHes CNSVolumeInfo.Status.MigrationConditions = Complete with a
//     "from -> to" message.
//  3. Calls migrationHandleStoragePolicyChange() directly to move PVC capacity
//     from the old policy's SPU to the new policy's SPU. The CNSVolumeInfo informer
//     update separately schedules recomputation of snapshot Used for both SPUs.
//
// pvc must be the snapshot the watcher validated the CR against (see
// resolveMigrationTargetPolicyID), not a fresh lister read: a retarget in
// between would otherwise record the new VAC name with the old policy ID.
func propagateMigrationSuccess(ctx context.Context, metadataSyncer *metadataSyncInformer,
	pvc *v1.PersistentVolumeClaim, migrationCR *unstructured.Unstructured) error {
	log := logger.GetLogger(ctx)

	if pvc == nil {
		return fmt.Errorf("PVC is nil")
	}
	pvcNamespace, pvcName := pvc.Namespace, pvc.Name
	// Resolve PVC -> volumeID and current CNSVolumeInfo (the OLD state, needed
	// for the quota helper).
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

	cnsClient := metadataSyncer.cnsOperatorClient
	cvi := cnsVolumeInfoObj(csiVolumeID)

	// Apply spec patch first (non-status subresource), then status.
	if len(specPatchBytes) > 0 {
		if err := patchCNSVolumeInfoSpecWithRetry(ctx, cnsClient, cvi, specPatchBytes,
			migrationPatchRetries); err != nil {
			return fmt.Errorf("failed to patch CNSVolumeInfo spec: %w", err)
		}
	}
	if err := patchCNSVolumeInfoStatusWithRetry(ctx, cnsClient, cvi, statusPatchBytes,
		migrationPatchRetries); err != nil {
		return fmt.Errorf("failed to patch CNSVolumeInfo status: %w", err)
	}
	log.Infof("propagateMigrationSuccess: Volume %s migration successfully completed by Mobility Operator: "+
		"VAC %q (policy %s) -> VAC %q (policy %s) [StorageClass %q unchanged]",
		csiVolumeID,
		oldCVI.Spec.VolumeAttributeClassName, oldCVI.Spec.StoragePolicyID,
		newVAC, newStoragePolicyID,
		oldCVI.Spec.StorageClassName)

	// Quota update: rebalance StoragePolicyUsage CRs from the OLD policy's SPU
	// to the NEW policy's SPU.
	migrationHandleStoragePolicyChange(ctx, cnsClient, *oldCVI, newCVI)
	return nil
}

// cnsVolumeInfoObj builds a minimal CNSVolumeInfo object suitable as the
// target of a client.Patch or client.Status().Patch call. For merge
// patches the API server only needs the name, namespace, and GroupVersionKind; the full
// object body is not required.
func cnsVolumeInfoObj(volumeID string) *cnsvolumeinfov1alpha1.CNSVolumeInfo {
	return &cnsvolumeinfov1alpha1.CNSVolumeInfo{
		ObjectMeta: metav1.ObjectMeta{
			Name:      cnsvolumeinfo.GetCnsVolumeInfoCrName(volumeID),
			Namespace: common.GetCSINamespace(),
		},
	}
}

// patchCNSVolumeInfoSpecWithRetry patches the spec of a
// CNSVolumeInfo object, retrying up to maxRetries times on transient failures.
func patchCNSVolumeInfoSpecWithRetry(ctx context.Context, c client.Client,
	obj *cnsvolumeinfov1alpha1.CNSVolumeInfo, patchBytes []byte, maxRetries int) error {
	rawPatch := client.RawPatch(k8stypes.MergePatchType, patchBytes)
	var err error
	for attempt := 1; attempt <= maxRetries; attempt++ {
		if err = c.Patch(ctx, obj, rawPatch); err == nil || attempt == maxRetries {
			break
		}
		time.Sleep(100 * time.Millisecond)
	}
	return err
}

// patchCNSVolumeInfoStatusWithRetry patches the status subresource of a
// CNSVolumeInfo object, retrying up to maxRetries times on transient failures.
func patchCNSVolumeInfoStatusWithRetry(ctx context.Context, c client.Client,
	obj *cnsvolumeinfov1alpha1.CNSVolumeInfo, patchBytes []byte, maxRetries int) error {
	rawPatch := client.RawPatch(k8stypes.MergePatchType, patchBytes)
	var err error
	for attempt := 1; attempt <= maxRetries; attempt++ {
		if err = c.Status().Patch(ctx, obj, rawPatch); err == nil || attempt == maxRetries {
			break
		}
		time.Sleep(100 * time.Millisecond)
	}
	return err
}

// buildMigrationSuccessPatches is the pure / side-effect-free portion of
// propagateMigrationSuccess. Given the old CNSVolumeInfo, the bound PVC, and
// the Mobility-Operator migration CR, it derives:
//
//   - the spec-only PATCH bytes (or nil if the spec already matches the
//     migration result),
//   - the status-only PATCH bytes carrying MigrationConditions=Complete with
//     a human-readable "from -> to" message, and MigrationTargetStoragePolicyID
//     set to the new storage policy ID,
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
			"migrationTargetStoragePolicyID": newStoragePolicyID,
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
