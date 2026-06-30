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

package storagepolicyinfo

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"time"

	cnstypes "github.com/vmware/govmomi/cns/types"
	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/equality"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	apitypes "k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes/scheme"
	typedcorev1 "k8s.io/client-go/kubernetes/typed/core/v1"
	restclient "k8s.io/client-go/rest"
	"k8s.io/client-go/tools/record"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/builder"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller"
	"sigs.k8s.io/controller-runtime/pkg/event"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	"sigs.k8s.io/controller-runtime/pkg/manager"
	"sigs.k8s.io/controller-runtime/pkg/predicate"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
	"sigs.k8s.io/controller-runtime/pkg/source"
	apis "sigs.k8s.io/vsphere-csi-driver/v3/pkg/apis/cnsoperator"
	infraspiv1alpha1 "sigs.k8s.io/vsphere-csi-driver/v3/pkg/apis/cnsoperator/infrastoragepolicyinfo/v1alpha1"
	storagepolicyv1alpha2 "sigs.k8s.io/vsphere-csi-driver/v3/pkg/apis/cnsoperator/storagepolicy/v1alpha2"
	spiv1alpha1 "sigs.k8s.io/vsphere-csi-driver/v3/pkg/apis/cnsoperator/storagepolicyinfo/v1alpha1"
	volumes "sigs.k8s.io/vsphere-csi-driver/v3/pkg/common/cns-lib/volume"
	"sigs.k8s.io/vsphere-csi-driver/v3/pkg/common/config"
	"sigs.k8s.io/vsphere-csi-driver/v3/pkg/csi/service/common"
	"sigs.k8s.io/vsphere-csi-driver/v3/pkg/csi/service/common/commonco"
	"sigs.k8s.io/vsphere-csi-driver/v3/pkg/csi/service/logger"
	k8s "sigs.k8s.io/vsphere-csi-driver/v3/pkg/kubernetes"
	"sigs.k8s.io/vsphere-csi-driver/v3/pkg/syncer/cnsoperator/types"
	"sigs.k8s.io/vsphere-csi-driver/v3/pkg/syncer/cnsoperator/util"
)

// zonesProvider is the narrow slice of COCommonInterface that this controller
// actually needs. Declaring it here lets tests inject a minimal stub instead of
// implementing the entire COCommonInterface.
type zonesProvider interface {
	// StartZonesInformer starts the dynamic informer watching Zone CRs. It must
	// be called before GetZonesForNamespace; otherwise GetZonesForNamespace will
	// dereference a nil informer and panic.
	StartZonesInformer(ctx context.Context, restClientConfig *restclient.Config, namespace string) error
	GetZonesForNamespace(ns string) map[string]struct{}
	// RegisterZoneEventHandler registers a callback invoked on Zone CR
	// add/update/delete, carrying the namespace of the changed Zone. It must be
	// called before StartZonesInformer.
	RegisterZoneEventHandler(handler func(namespace string))
}

const (
	workerThreadsEnvVar     = "WORKER_THREADS_STORAGE_POLICY_INFO"
	defaultMaxWorkerThreads = 4
	// storagePolicyQuotaSuffix is the suffix appended to the K8s-compliant storage
	// policy name to form the corresponding StoragePolicyQuota CR name.
	storagePolicyQuotaSuffix = "-storagepolicyquota"
	// spiNameIndexField is the field index key used to look up StoragePolicyInfo
	// objects by name, enabling efficient cross-namespace lookups in mapInfraSPItoSPI.
	spiNameIndexField = ".metadata.name"
	// zoneEventChannelBuffer is the buffer size of the channel bridging Zone CR
	// events into the controller workqueue. It absorbs short bursts of zone churn
	// without blocking the informer's event-handler goroutine.
	zoneEventChannelBuffer = 64
)

// Add registers the StoragePolicyInfo controller with the Manager (WCP / Workload only).
func Add(mgr manager.Manager, clusterFlavor cnstypes.CnsClusterFlavor,
	configInfo *config.ConfigurationInfo, _ volumes.Manager) error {
	ctx, log := logger.GetNewContextWithLogger()
	if clusterFlavor != cnstypes.CnsClusterFlavorWorkload {
		log.Debug("Not initializing the StoragePolicyInfo Controller: unsupported cluster flavor")
		return nil
	}
	if !commonco.ContainerOrchestratorUtility.IsFSSEnabled(ctx, common.SupportsExposingStoragePolicyAttributes) {
		log.Infof("Not initializing the StoragePolicyInfo Controller: capability %q is not activated",
			common.SupportsExposingStoragePolicyAttributes)
		return nil
	}

	k8sclient, err := k8s.NewClient(ctx)
	if err != nil {
		log.Errorf("creating Kubernetes client failed. Err: %v", err)
		return err
	}

	eventBroadcaster := record.NewBroadcaster()
	eventBroadcaster.StartRecordingToSink(
		&typedcorev1.EventSinkImpl{
			Interface: k8sclient.CoreV1().Events(""),
		},
	)
	recorder := eventBroadcaster.NewRecorder(scheme.Scheme, v1.EventSource{Component: apis.GroupName})
	r := newReconciler(mgr, configInfo, recorder, commonco.ContainerOrchestratorUtility)

	// Register the Zone event handler before starting the informer so it is
	// attached on the informer's first (and only) start. The handler bridges Zone
	// CR changes in a namespace into this controller's workqueue.
	commonco.ContainerOrchestratorUtility.RegisterZoneEventHandler(r.enqueueZoneNamespace)

	if err := commonco.ContainerOrchestratorUtility.StartZonesInformer(
		ctx, nil, metav1.NamespaceAll); err != nil {
		return logger.LogNewErrorf(log, "failed to start zone informer for StoragePolicyInfo controller. Err: %v", err)
	}

	return add(mgr, r)
}

func newReconciler(mgr manager.Manager, configInfo *config.ConfigurationInfo,
	recorder record.EventRecorder, zp zonesProvider) *ReconcileStoragePolicyInfo {

	return &ReconcileStoragePolicyInfo{
		client:          mgr.GetClient(),
		scheme:          mgr.GetScheme(),
		configInfo:      configInfo,
		recorder:        recorder,
		zonesProvider:   zp,
		zoneEventCh:     make(chan event.GenericEvent, zoneEventChannelBuffer),
		backOffDuration: make(map[apitypes.NamespacedName]time.Duration),
	}
}

func add(mgr manager.Manager, r *ReconcileStoragePolicyInfo) error {
	ctx, log := logger.GetNewContextWithLogger()
	maxWorkerThreads := util.GetMaxWorkerThreads(ctx, workerThreadsEnvVar, defaultMaxWorkerThreads)

	// Index StoragePolicyInfo by name so mapInfraSPItoSPI can list only matching
	// objects rather than doing a full cross-namespace list.
	if err := mgr.GetFieldIndexer().IndexField(ctx, &spiv1alpha1.StoragePolicyInfo{},
		spiNameIndexField, func(obj client.Object) []string {
			return []string{obj.GetName()}
		}); err != nil {
		log.Errorf("failed to add field index for StoragePolicyInfo name: %v", err)
		return err
	}

	// Only reconcile on StoragePolicyQuota CREATE events. Updates to the quota
	// spec/status do not affect the StoragePolicyInfo topology data.
	spqPredicates := predicate.Funcs{
		CreateFunc: func(e event.CreateEvent) bool {
			return e.Object != nil
		},
		UpdateFunc: func(e event.UpdateEvent) bool {
			return false
		},
		DeleteFunc: func(_ event.DeleteEvent) bool {
			return false
		},
	}

	// Only re-reconcile when InfraStoragePolicyInfo status actually changes.
	// Generation-based predicates don't work for status-only updates, so we
	// gate updates on resource-version changes.
	infraSPIPredicates := predicate.Funcs{
		CreateFunc: func(_ event.CreateEvent) bool {
			return false
		},
		UpdateFunc: predicate.ResourceVersionChangedPredicate{}.Update,
		DeleteFunc: func(_ event.DeleteEvent) bool {
			return false
		},
	}

	err := ctrl.NewControllerManagedBy(mgr).Named("storagepolicyinfo-controller").
		For(&spiv1alpha1.StoragePolicyInfo{},
			builder.WithPredicates(predicate.GenerationChangedPredicate{})).
		Watches(
			&storagepolicyv1alpha2.StoragePolicyQuota{},
			handler.EnqueueRequestsFromMapFunc(r.mapSPQtoSPI),
			builder.WithPredicates(spqPredicates),
		).
		Watches(
			&infraspiv1alpha1.InfraStoragePolicyInfo{},
			handler.EnqueueRequestsFromMapFunc(r.mapInfraSPItoSPI),
			builder.WithPredicates(infraSPIPredicates),
		).
		WatchesRawSource(
			source.Channel(
				r.zoneEventCh,
				handler.EnqueueRequestsFromMapFunc(r.mapZoneNamespaceToSPIs),
			),
		).
		WithOptions(controller.Options{MaxConcurrentReconciles: maxWorkerThreads}).
		Complete(r)
	if err != nil {
		log.Errorf("failed to build storagepolicyinfo controller. Err: %v", err)
		return err
	}

	return nil
}

// mapSPQtoSPI maps a StoragePolicyQuota object to the reconcile request for
// the corresponding StoragePolicyInfo in the same namespace.
// The StoragePolicyInfo name is derived by stripping the "-storagepolicyquota"
// suffix from the StoragePolicyQuota name.
func (r *ReconcileStoragePolicyInfo) mapSPQtoSPI(ctx context.Context,
	obj client.Object) []reconcile.Request {
	if obj == nil {
		return nil
	}
	spiName := strings.TrimSuffix(obj.GetName(), storagePolicyQuotaSuffix)
	// If the name was unchanged, this object does not follow the expected
	// "<policy>-storagepolicyquota" naming convention; skip it so we don't
	// accidentally reconcile an unrelated StoragePolicyInfo.
	if spiName == obj.GetName() {
		return nil
	}
	return []reconcile.Request{{NamespacedName: apitypes.NamespacedName{
		Namespace: obj.GetNamespace(),
		Name:      spiName,
	}}}
}

// mapInfraSPItoSPI maps an InfraStoragePolicyInfo event to reconcile requests
// for all namespace-scoped StoragePolicyInfo CRs that share the same name.
// This keeps namespace-scoped topology in sync whenever the cluster-scoped
// InfraStoragePolicyInfo status changes (e.g., zones added or removed).
func (r *ReconcileStoragePolicyInfo) mapInfraSPItoSPI(ctx context.Context,
	obj client.Object) []reconcile.Request {
	if obj == nil {
		return nil
	}
	log := logger.GetLogger(ctx)

	// Use the name field index to list only StoragePolicyInfo objects whose name
	// matches the InfraStoragePolicyInfo, avoiding a full cross-namespace scan.
	spiList := &spiv1alpha1.StoragePolicyInfoList{}
	if err := r.client.List(ctx, spiList,
		client.MatchingFields{spiNameIndexField: obj.GetName()}); err != nil {
		log.Errorf("Failed to list StoragePolicyInfo for InfraStoragePolicyInfo %q: %v",
			obj.GetName(), err)
		return nil
	}

	reqs := make([]reconcile.Request, 0, len(spiList.Items))
	for i := range spiList.Items {
		reqs = append(reqs, reconcile.Request{
			NamespacedName: apitypes.NamespacedName{
				Namespace: spiList.Items[i].Namespace,
				Name:      spiList.Items[i].Name,
			},
		})
	}
	return reqs
}

// enqueueZoneNamespace is the Zone event-handler callback registered with the
// commonco layer. It is invoked whenever a Zone CR is added, updated, or deleted
// and pushes a GenericEvent stamped with the affected namespace onto the
// controller's channel. mapZoneNamespaceToSPIs then fans the namespace out to its
// StoragePolicyInfo CRs. The send is best-effort: if the buffered channel is
// full, the event is dropped (a subsequent zone change or any other reconcile
// trigger will recompute the topology), so the informer's event goroutine is
// never blocked.
func (r *ReconcileStoragePolicyInfo) enqueueZoneNamespace(namespace string) {
	if namespace == "" {
		return
	}
	evt := event.GenericEvent{
		Object: &metav1.PartialObjectMetadata{
			ObjectMeta: metav1.ObjectMeta{Namespace: namespace},
		},
	}
	select {
	case r.zoneEventCh <- evt:
	default:
		_, log := logger.GetNewContextWithLogger()
		log.Warnf("Zone event channel full; dropping zone-change event for namespace %q", namespace)
	}
}

// mapZoneNamespaceToSPIs maps a Zone-change GenericEvent (whose object carries
// only the affected namespace) to reconcile requests for every StoragePolicyInfo
// CR in that namespace, so their AccessibleZones are recomputed.
func (r *ReconcileStoragePolicyInfo) mapZoneNamespaceToSPIs(ctx context.Context,
	obj client.Object) []reconcile.Request {
	if obj == nil {
		return nil
	}
	namespace := obj.GetNamespace()
	if namespace == "" {
		return nil
	}
	log := logger.GetLogger(ctx)

	spiList := &spiv1alpha1.StoragePolicyInfoList{}
	if err := r.client.List(ctx, spiList, client.InNamespace(namespace)); err != nil {
		log.Errorf("Failed to list StoragePolicyInfo in namespace %q on zone change: %v",
			namespace, err)
		return nil
	}

	reqs := make([]reconcile.Request, 0, len(spiList.Items))
	for i := range spiList.Items {
		reqs = append(reqs, reconcile.Request{
			NamespacedName: apitypes.NamespacedName{
				Namespace: spiList.Items[i].Namespace,
				Name:      spiList.Items[i].Name,
			},
		})
	}
	log.Infof("Zone change in namespace %q enqueued %d StoragePolicyInfo reconcile request(s)",
		namespace, len(reqs))
	return reqs
}

var _ reconcile.Reconciler = &ReconcileStoragePolicyInfo{}

// ReconcileStoragePolicyInfo reconciles StoragePolicyInfo objects.
type ReconcileStoragePolicyInfo struct {
	client        client.Client
	scheme        *runtime.Scheme
	configInfo    *config.ConfigurationInfo
	recorder      record.EventRecorder
	zonesProvider zonesProvider
	// zoneEventCh receives a GenericEvent (stamped with the affected namespace)
	// whenever a Zone CR in some namespace changes. It bridges the package-level
	// zone informer's events into this controller's workqueue via source.Channel.
	zoneEventCh chan event.GenericEvent
	// backOffDuration tracks per-instance requeue delays, incremented
	// exponentially on failure and reset to 1s on success.
	backOffDuration         map[apitypes.NamespacedName]time.Duration
	backOffDurationMapMutex sync.Mutex
}

// Reconcile creates or updates the StoragePolicyInfo for the given namespace
// and storage policy, deriving topology from the corresponding
// InfraStoragePolicyInfo.
func (r *ReconcileStoragePolicyInfo) Reconcile(ctx context.Context,
	request reconcile.Request) (reconcile.Result, error) {
	ctx = logger.NewContextWithLogger(ctx)
	log := logger.GetLogger(ctx).With("name", request.NamespacedName)

	log.Infof("Reconciling StoragePolicyInfo")

	// Initialize backOffDuration for the instance, if required.
	r.backOffDurationMapMutex.Lock()
	var timeout time.Duration
	if _, exists := r.backOffDuration[request.NamespacedName]; !exists {
		r.backOffDuration[request.NamespacedName] = time.Second
	}
	timeout = r.backOffDuration[request.NamespacedName]
	r.backOffDurationMapMutex.Unlock()

	// Fetch the cluster-scoped InfraStoragePolicyInfo first so we can set the
	// owner reference at SPI creation time.
	infraSPI := &infraspiv1alpha1.InfraStoragePolicyInfo{}
	if err := r.client.Get(ctx,
		apitypes.NamespacedName{Name: request.Name}, infraSPI); err != nil {
		if apierrors.IsNotFound(err) {
			log.Infof("InfraStoragePolicyInfo %q not yet available; will retry",
				request.Name)
		} else {
			log.Errorf("Failed to get InfraStoragePolicyInfo %q: %v", request.Name, err)
		}
		return r.completeReconciliationWithError(ctx, request.NamespacedName, timeout, err)
	}

	// Fetch or create the StoragePolicyInfo instance, stamping the InfraSPI owner
	// reference at creation time so the SPI is deleted automatically when the
	// InfraStoragePolicyInfo is deleted.
	instance, wasCreated, err := r.ensureSPIExists(ctx, request.Namespace, request.Name, infraSPI)
	if err != nil {
		return r.completeReconciliationWithError(ctx, request.NamespacedName, timeout, err)
	}
	if wasCreated {
		log.Infof("StoragePolicyInfo %q was just created; creation event will trigger next reconcile",
			request.NamespacedName)
		return r.completeReconciliationWithSuccess(ctx, request.NamespacedName)
	}

	if instance.DeletionTimestamp != nil {
		log.Infof("StoragePolicyInfo %q is being deleted, skipping reconciliation",
			request.NamespacedName)
		return r.completeReconciliationWithSuccess(ctx, request.NamespacedName)
	}

	// Ensure SPI holds an owner reference to InfraStoragePolicyInfo. This is a
	// no-op for newly created SPIs (owner ref set at creation) but handles SPIs
	// that pre-date this logic or had the ref removed.
	if err := r.ensureInfraSPIOwnerReference(ctx, instance, infraSPI); err != nil {
		log.Errorf("Failed to set owner reference on StoragePolicyInfo %q: %v",
			request.NamespacedName, err)
		return r.completeReconciliationWithError(ctx, request.NamespacedName, timeout, err)
	}

	// Populate TopologyInfo from InfraStoragePolicyInfo.
	if err := r.syncTopologyFromInfraSPI(ctx, instance, infraSPI); err != nil {
		log.Errorf("Failed to sync topology for StoragePolicyInfo %q: %v",
			request.NamespacedName, err)
		if setErr := r.setSPIError(ctx, instance,
			fmt.Sprintf("Failed to sync topology: %v", err)); setErr != nil {
			log.Errorf("Failed to update StoragePolicyInfo %q status: %v",
				request.NamespacedName, setErr)
		}
		return r.completeReconciliationWithError(ctx, request.NamespacedName, timeout, err)
	}

	if setErr := r.setSPISuccess(ctx, instance, "Successfully synced topology"); setErr != nil {
		log.Errorf("Failed to update StoragePolicyInfo %q status: %v",
			request.NamespacedName, setErr)
		return r.completeReconciliationWithError(ctx, request.NamespacedName, timeout, setErr)
	}

	log.Infof("Successfully reconciled StoragePolicyInfo %q", request.NamespacedName)
	return r.completeReconciliationWithSuccess(ctx, request.NamespacedName)
}

// ensureSPIExists returns the existing StoragePolicyInfo for the given
// namespace/name. If absent, it creates the CR with an owner reference to
// infraSPI and returns wasCreated=true.
func (r *ReconcileStoragePolicyInfo) ensureSPIExists(ctx context.Context,
	namespace, name string,
	infraSPI *infraspiv1alpha1.InfraStoragePolicyInfo) (*spiv1alpha1.StoragePolicyInfo, bool, error) {
	log := logger.GetLogger(ctx)

	instance := &spiv1alpha1.StoragePolicyInfo{}
	err := r.client.Get(ctx,
		apitypes.NamespacedName{Namespace: namespace, Name: name}, instance)
	if err == nil {
		return instance, false, nil
	}
	if !apierrors.IsNotFound(err) {
		return nil, false, err
	}

	ownerRef, err := generateOwnerReference(r.scheme, infraSPI)
	if err != nil {
		return nil, false, fmt.Errorf("failed to generate owner reference for InfraStoragePolicyInfo %q: %w",
			infraSPI.Name, err)
	}

	// Create the StoragePolicyInfo CR with the owner reference already set.
	instance = &spiv1alpha1.StoragePolicyInfo{
		TypeMeta: metav1.TypeMeta{
			APIVersion: apis.SchemeGroupVersion.String(),
			Kind:       "StoragePolicyInfo",
		},
		ObjectMeta: metav1.ObjectMeta{
			Name:            name,
			Namespace:       namespace,
			OwnerReferences: []metav1.OwnerReference{ownerRef},
		},
	}
	if err := r.client.Create(ctx, instance); err != nil {
		if !apierrors.IsAlreadyExists(err) {
			log.Errorf("Failed to create StoragePolicyInfo %s/%s: %v", namespace, name, err)
			return nil, false, err
		}
		// Race condition: another reconcile beat us; fetch the existing object.
		if err := r.client.Get(ctx,
			apitypes.NamespacedName{Namespace: namespace, Name: name}, instance); err != nil {
			return nil, false, err
		}
		// The object already existed, so treat it as not newly created.
		return instance, false, nil
	}
	log.Infof("Created StoragePolicyInfo %s/%s with owner reference to InfraStoragePolicyInfo %q",
		namespace, name, infraSPI.Name)
	return instance, true, nil
}

// ensureInfraSPIOwnerReference sets an owner reference on the StoragePolicyInfo
// pointing to the given InfraStoragePolicyInfo. This causes the SPI to be
// garbage-collected when the InfraStoragePolicyInfo is deleted.
func (r *ReconcileStoragePolicyInfo) ensureInfraSPIOwnerReference(ctx context.Context,
	instance *spiv1alpha1.StoragePolicyInfo,
	infraSPI *infraspiv1alpha1.InfraStoragePolicyInfo) error {
	log := logger.GetLogger(ctx)

	ownerRef, err := generateOwnerReference(r.scheme, infraSPI)
	if err != nil {
		return fmt.Errorf("failed to generate owner reference for InfraStoragePolicyInfo %q: %w",
			infraSPI.Name, err)
	}

	updatedRefs := mergeOwnerReference(instance.OwnerReferences, ownerRef)
	if equality.Semantic.DeepEqual(instance.OwnerReferences, updatedRefs) {
		return nil
	}

	base := instance.DeepCopy()
	instance.OwnerReferences = updatedRefs
	if err := r.client.Patch(ctx, instance, client.MergeFrom(base)); err != nil {
		log.Errorf("Failed to patch StoragePolicyInfo %s/%s owner references: %v",
			instance.Namespace, instance.Name, err)
		return err
	}
	log.Infof("Set owner reference on StoragePolicyInfo %s/%s → InfraStoragePolicyInfo %q",
		instance.Namespace, instance.Name, infraSPI.Name)
	return nil
}

// syncTopologyFromInfraSPI copies topology information from the cluster-scoped
// InfraStoragePolicyInfo into the namespace-scoped StoragePolicyInfo status,
// filtering accessible zones down to only those assigned to the namespace.
func (r *ReconcileStoragePolicyInfo) syncTopologyFromInfraSPI(ctx context.Context,
	instance *spiv1alpha1.StoragePolicyInfo,
	infraSPI *infraspiv1alpha1.InfraStoragePolicyInfo) error {
	log := logger.GetLogger(ctx)

	if infraSPI.Status.Topology == nil {
		log.Debugf("InfraStoragePolicyInfo %q has no topology; clearing StoragePolicyInfo topology",
			infraSPI.Name)
		instance.Status.TopologyInfo = nil
		return nil
	}

	accessibleZones := r.namespaceFilteredZones(ctx, instance.Namespace,
		infraSPI.Status.Topology.AccessibleZones)

	if len(accessibleZones) == 0 {
		// No zones are accessible in this namespace (no Zone CRs assigned).
		// Clear TopologyInfo rather than writing an empty accessibleZones slice,
		// which the CRD schema rejects as a required field.
		log.Debugf("StoragePolicyInfo %s/%s has no accessible zones; clearing TopologyInfo",
			instance.Namespace, instance.Name)
		instance.Status.TopologyInfo = nil
		return nil
	}

	instance.Status.TopologyInfo = &spiv1alpha1.Topology{
		TopologyType:    infraSPI.Status.Topology.TopologyType,
		AccessibleZones: accessibleZones,
	}
	log.Debugf("Synced topology for StoragePolicyInfo %s/%s: type=%q zones=%v",
		instance.Namespace, instance.Name,
		instance.Status.TopologyInfo.TopologyType,
		instance.Status.TopologyInfo.AccessibleZones)
	return nil
}

// namespaceFilteredZones returns the subset of clusterZones that are also
// assigned to the given namespace, by consulting the namespace-scoped Zone CRs.
func (r *ReconcileStoragePolicyInfo) namespaceFilteredZones(ctx context.Context,
	namespace string, clusterZones []string) []string {
	log := logger.GetLogger(ctx)

	nsZones := r.zonesProvider.GetZonesForNamespace(namespace)
	if len(nsZones) == 0 {
		// No Zone CRs in this namespace means no zone has been assigned to it yet.
		// Return empty rather than falling back to all cluster zones — exposing all
		// zones for an unassigned namespace would be incorrect in a WDI deployment.
		log.Debugf("Namespace %q has no zone assignments; accessibleZones will be empty",
			namespace)
		return nil
	}

	// Intersect cluster-accessible zones with namespace-assigned zones.
	filtered := make([]string, 0, len(clusterZones))
	for _, zone := range clusterZones {
		if _, assigned := nsZones[zone]; assigned {
			filtered = append(filtered, zone)
		}
	}
	log.Debugf("Namespace %q has zone assignments %v; filtered %d cluster zones → %d namespace-accessible zones",
		namespace, nsZones, len(clusterZones), len(filtered))
	return filtered
}

// setSPIError sets the error status and records a Warning event on the instance.
func (r *ReconcileStoragePolicyInfo) setSPIError(ctx context.Context,
	instance *spiv1alpha1.StoragePolicyInfo, errMsg string) error {
	instance.Status.Error = errMsg
	if err := k8s.UpdateStatus(ctx, r.client, instance); err != nil {
		return err
	}
	r.recorder.Event(instance, v1.EventTypeWarning, "StoragePolicyInfoFailed", errMsg)
	return nil
}

// setSPISuccess clears the error status and records a Normal event on the instance.
func (r *ReconcileStoragePolicyInfo) setSPISuccess(ctx context.Context,
	instance *spiv1alpha1.StoragePolicyInfo, msg string) error {
	instance.Status.Error = ""
	if err := k8s.UpdateStatus(ctx, r.client, instance); err != nil {
		return err
	}
	r.recorder.Event(instance, v1.EventTypeNormal, "StoragePolicyInfoSynced", msg)
	return nil
}

// completeReconciliationWithSuccess resets the backoff duration for the instance
// and records a successful reconciliation.
func (r *ReconcileStoragePolicyInfo) completeReconciliationWithSuccess(ctx context.Context,
	namespacedName apitypes.NamespacedName) (reconcile.Result, error) {
	log := logger.GetLogger(ctx).With("name", namespacedName)

	r.backOffDurationMapMutex.Lock()
	delete(r.backOffDuration, namespacedName)
	r.backOffDurationMapMutex.Unlock()

	log.Infof("Successfully reconciled StoragePolicyInfo")
	return reconcile.Result{}, nil
}

// completeReconciliationWithError updates the backoff duration for the instance
// and schedules a retry after the backoff timeout.
func (r *ReconcileStoragePolicyInfo) completeReconciliationWithError(ctx context.Context,
	namespacedName apitypes.NamespacedName, timeout time.Duration, err error) (reconcile.Result, error) {
	log := logger.GetLogger(ctx).With("name", namespacedName)

	r.backOffDurationMapMutex.Lock()
	r.backOffDuration[namespacedName] = min(r.backOffDuration[namespacedName]*2,
		types.MaxBackOffDurationForReconciler)
	r.backOffDurationMapMutex.Unlock()

	log.Errorf("Failed to reconcile StoragePolicyInfo %q. Err: %v", namespacedName, err)
	return reconcile.Result{RequeueAfter: timeout}, nil
}
