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
	apis "sigs.k8s.io/vsphere-csi-driver/v3/pkg/apis/cnsoperator"
	infraspiv1alpha1 "sigs.k8s.io/vsphere-csi-driver/v3/pkg/apis/cnsoperator/infrastoragepolicyinfo/v1alpha1"
	spiv1alpha1 "sigs.k8s.io/vsphere-csi-driver/v3/pkg/apis/cnsoperator/storagepolicyinfo/v1alpha1"
	storagepolicyv1alpha2 "sigs.k8s.io/vsphere-csi-driver/v3/pkg/apis/cnsoperator/storagepolicy/v1alpha2"
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
}

// backOffDuration is a map of StoragePolicyInfo NamespacedNames to the time after
// which a request for this instance will be requeued.
// Initialized to 1 second for new instances and for instances whose latest
// reconcile operation succeeded.
// If the reconcile fails, backoff is incremented exponentially.
var (
	backOffDuration         map[apitypes.NamespacedName]time.Duration
	backOffDurationMapMutex = sync.Mutex{}
)

const (
	workerThreadsEnvVar     = "WORKER_THREADS_STORAGE_POLICY_INFO"
	defaultMaxWorkerThreads = 4
	// storagePolicyQuotaSuffix is the suffix appended to the K8s-compliant storage
	// policy name to form the corresponding StoragePolicyQuota CR name.
	storagePolicyQuotaSuffix = "-storagepolicyquota"
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

	workloadDomainIsolationEnabled := commonco.ContainerOrchestratorUtility.IsFSSEnabled(ctx,
		common.WorkloadDomainIsolation)
	if workloadDomainIsolationEnabled {
		if err := commonco.ContainerOrchestratorUtility.StartZonesInformer(
			ctx, nil, metav1.NamespaceAll); err != nil {
			return logger.LogNewErrorf(log, "failed to start zone informer for StoragePolicyInfo controller. Err: %v", err)
		}
	}

	eventBroadcaster := record.NewBroadcaster()
	eventBroadcaster.StartRecordingToSink(
		&typedcorev1.EventSinkImpl{
			Interface: k8sclient.CoreV1().Events(""),
		},
	)
	recorder := eventBroadcaster.NewRecorder(scheme.Scheme, v1.EventSource{Component: apis.GroupName})
	return add(mgr, newReconciler(mgr, configInfo, recorder,
		commonco.ContainerOrchestratorUtility, workloadDomainIsolationEnabled))
}

func newReconciler(mgr manager.Manager, configInfo *config.ConfigurationInfo,
	recorder record.EventRecorder, zp zonesProvider,
	workloadDomainIsolationEnabled bool) *ReconcileStoragePolicyInfo {

	return &ReconcileStoragePolicyInfo{
		client:                         mgr.GetClient(),
		scheme:                         mgr.GetScheme(),
		configInfo:                     configInfo,
		recorder:                       recorder,
		zonesProvider:                  zp,
		workloadDomainIsolationEnabled: workloadDomainIsolationEnabled,
	}
}

func add(mgr manager.Manager, r *ReconcileStoragePolicyInfo) error {
	ctx, log := logger.GetNewContextWithLogger()
	maxWorkerThreads := util.GetMaxWorkerThreads(ctx, workerThreadsEnvVar, defaultMaxWorkerThreads)

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
	// compare the resource version instead.
	infraSPIPredicates := predicate.Funcs{
		CreateFunc: func(e event.CreateEvent) bool {
			return e.Object != nil
		},
		UpdateFunc: func(e event.UpdateEvent) bool {
			return e.ObjectOld.GetResourceVersion() != e.ObjectNew.GetResourceVersion()
		},
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
		WithOptions(controller.Options{MaxConcurrentReconciles: maxWorkerThreads}).
		Complete(r)
	if err != nil {
		log.Errorf("failed to build storagepolicyinfo controller. Err: %v", err)
		return err
	}

	// Initialize the backoff duration map
	backOffDuration = make(map[apitypes.NamespacedName]time.Duration)
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

	// List all StoragePolicyInfo instances across all namespaces and filter
	// to those whose name matches the InfraStoragePolicyInfo name. A field
	// indexer would be more efficient at scale, but SPI count per cluster is
	// bounded by (policies × namespaces) which is manageable with a full list.
	spiList := &spiv1alpha1.StoragePolicyInfoList{}
	if err := r.client.List(ctx, spiList); err != nil {
		log.Errorf("Failed to list StoragePolicyInfo for InfraStoragePolicyInfo %q: %v",
			obj.GetName(), err)
		return nil
	}

	reqs := make([]reconcile.Request, 0, len(spiList.Items))
	for i := range spiList.Items {
		if spiList.Items[i].Name == obj.GetName() {
			reqs = append(reqs, reconcile.Request{
				NamespacedName: apitypes.NamespacedName{
					Namespace: spiList.Items[i].Namespace,
					Name:      spiList.Items[i].Name,
				},
			})
		}
	}
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
	// workloadDomainIsolationEnabled gates zone-filtering logic. When false,
	// namespaceFilteredZones skips the GetZonesForNamespace call entirely,
	// which avoids a nil-informer panic on non-zonal deployments.
	workloadDomainIsolationEnabled bool
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
	backOffDurationMapMutex.Lock()
	var timeout time.Duration
	if _, exists := backOffDuration[request.NamespacedName]; !exists {
		backOffDuration[request.NamespacedName] = time.Second
	}
	timeout = backOffDuration[request.NamespacedName]
	backOffDurationMapMutex.Unlock()

	// Fetch or create the StoragePolicyInfo instance. If the CR was just
	// created here, the creation event will trigger another reconcile automatically,
	// so we return early and let the next reconcile populate its fields.
	instance, wasCreated, err := r.ensureSPIExists(ctx, request.Namespace, request.Name)
	if err != nil {
		return r.completeReconciliationWithError(ctx, request.NamespacedName, timeout, err)
	}
	if wasCreated {
		log.Infof("StoragePolicyInfo %q was just created; creation event will trigger next reconcile",
			request.NamespacedName)
		return r.completeReconciliationWithSuccess(ctx, request.NamespacedName)
	}

	if instance != nil && instance.DeletionTimestamp != nil {
		log.Infof("StoragePolicyInfo %q is being deleted, skipping reconciliation",
			request.NamespacedName)
		return r.completeReconciliationWithSuccess(ctx, request.NamespacedName)
	}

	// Fetch the cluster-scoped InfraStoragePolicyInfo (same name as StoragePolicyInfo)
	// to obtain topology data already computed by the ClusterStoragePolicyInfo controller.
	infraSPI := &infraspiv1alpha1.InfraStoragePolicyInfo{}
	if err := r.client.Get(ctx,
		apitypes.NamespacedName{Name: request.Name}, infraSPI); err != nil {
		if apierrors.IsNotFound(err) {
			// InfraStoragePolicyInfo may not be created yet (controller startup ordering).
			// The InfraStoragePolicyInfo watch will re-enqueue this reconcile once InfraSPI
			// is available, so returning success here is safe.
			log.Infof("InfraStoragePolicyInfo %q not yet available; topology will be synced on next reconcile",
				request.Name)
			return r.completeReconciliationWithSuccess(ctx, request.NamespacedName)
		}
		log.Errorf("Failed to get InfraStoragePolicyInfo %q: %v", request.Name, err)
		return r.completeReconciliationWithError(ctx, request.NamespacedName, timeout, err)
	}

	// Ensure SPI holds an owner reference to InfraStoragePolicyInfo so that the SPI
	// is garbage-collected when the InfraStoragePolicyInfo is deleted.
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
// namespace/name. If absent, it creates the CR and returns wasCreated=true.
func (r *ReconcileStoragePolicyInfo) ensureSPIExists(ctx context.Context,
	namespace, name string) (*spiv1alpha1.StoragePolicyInfo, bool, error) {
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

	// Create the StoragePolicyInfo CR.
	instance = &spiv1alpha1.StoragePolicyInfo{
		TypeMeta: metav1.TypeMeta{
			APIVersion: apis.SchemeGroupVersion.String(),
			Kind:       "StoragePolicyInfo",
		},
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: namespace,
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
	log.Infof("Created StoragePolicyInfo %s/%s", namespace, name)
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
// Zone filtering is only performed when WorkloadDomainIsolation is enabled and
// the zones informer has been started; otherwise all cluster zones are returned.
func (r *ReconcileStoragePolicyInfo) namespaceFilteredZones(ctx context.Context,
	namespace string, clusterZones []string) []string {
	log := logger.GetLogger(ctx)

	if !r.workloadDomainIsolationEnabled {
		log.Debugf("WorkloadDomainIsolation disabled; using all %d cluster-accessible zones for namespace %q",
			len(clusterZones), namespace)
		return clusterZones
	}

	nsZones := r.zonesProvider.GetZonesForNamespace(namespace)
	if len(nsZones) == 0 {
		// Non-zonal deployment or namespace has no zone constraints; expose all
		// cluster-accessible zones.
		log.Debugf("Namespace %q has no zone assignments; using all %d cluster-accessible zones",
			namespace, len(clusterZones))
		return clusterZones
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

	backOffDurationMapMutex.Lock()
	delete(backOffDuration, namespacedName)
	backOffDurationMapMutex.Unlock()

	log.Infof("Successfully reconciled StoragePolicyInfo")
	return reconcile.Result{}, nil
}

// completeReconciliationWithError updates the backoff duration for the instance
// and schedules a retry after the backoff timeout.
func (r *ReconcileStoragePolicyInfo) completeReconciliationWithError(ctx context.Context,
	namespacedName apitypes.NamespacedName, timeout time.Duration, err error) (reconcile.Result, error) {
	log := logger.GetLogger(ctx).With("name", namespacedName)

	backOffDurationMapMutex.Lock()
	backOffDuration[namespacedName] = min(backOffDuration[namespacedName]*2,
		types.MaxBackOffDurationForReconciler)
	backOffDurationMapMutex.Unlock()

	log.Errorf("Failed to reconcile StoragePolicyInfo %q. Err: %v", namespacedName, err)
	return reconcile.Result{RequeueAfter: timeout}, nil
}
