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
	vimtypes "github.com/vmware/govmomi/vim25/types"
	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/equality"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	apitypes "k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/kubernetes"
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
	cnsvsphere "sigs.k8s.io/vsphere-csi-driver/v3/pkg/common/cns-lib/vsphere"
	"sigs.k8s.io/vsphere-csi-driver/v3/pkg/common/config"
	"sigs.k8s.io/vsphere-csi-driver/v3/pkg/csi/service/common"
	"sigs.k8s.io/vsphere-csi-driver/v3/pkg/csi/service/common/commonco"
	"sigs.k8s.io/vsphere-csi-driver/v3/pkg/csi/service/logger"
	k8s "sigs.k8s.io/vsphere-csi-driver/v3/pkg/kubernetes"
	"sigs.k8s.io/vsphere-csi-driver/v3/pkg/syncer/cnsoperator/types"
	"sigs.k8s.io/vsphere-csi-driver/v3/pkg/syncer/cnsoperator/util"
	"sigs.k8s.io/vsphere-csi-driver/v3/pkg/syncer/cnsoperator/vsphereinfra"
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
	// GetActiveClustersForNamespaceInRequestedZones returns the cluster morefs the namespace
	// is actually active on within the given zones (from the namespace-scoped Zone CR's
	// spec.namespace.clusterMoIDs), as opposed to GetZonesForNamespace, which only reports
	// zone-name assignment. A zone can span multiple clusters, and a namespace may be active
	// on only a subset of them.
	GetActiveClustersForNamespaceInRequestedZones(ctx context.Context, ns string, zones []string) ([]string, error)
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
	// spiMarkerIndexField is the field index key used to list only marker-policy
	// StoragePolicyInfo objects in mapFVSNamespaceToMarkerSPIs.
	spiMarkerIndexField = "isMarkerPolicy"
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

	cfg, err := restclient.InClusterConfig()
	if err != nil {
		log.Errorf("getting in-cluster config failed. Err: %v", err)
		return err
	}
	dynamicClient, err := dynamic.NewForConfig(cfg)
	if err != nil {
		log.Errorf("creating dynamic client failed. Err: %v", err)
		return err
	}

	if err := commonco.ContainerOrchestratorUtility.StartZonesInformer(
		ctx, nil, metav1.NamespaceAll); err != nil {
		return logger.LogNewErrorf(log, "failed to start zone informer for StoragePolicyInfo controller. Err: %v", err)
	}

	eventBroadcaster := record.NewBroadcaster()
	eventBroadcaster.StartRecordingToSink(
		&typedcorev1.EventSinkImpl{
			Interface: k8sclient.CoreV1().Events(""),
		},
	)
	recorder := eventBroadcaster.NewRecorder(mgr.GetScheme(), v1.EventSource{Component: apis.GroupName})

	// Marker-policy topology support (the marker field index, the FVS-namespace
	// watch, and the syncMarkerPolicyTopology branch) is a distinct feature gated
	// by the VsanFileVolumeService capability. Regular policy topology reconciles
	// regardless of this flag.
	IsVsanFileVolumeService := commonco.ContainerOrchestratorUtility.IsFSSEnabled(ctx, common.VsanFileVolumeService)
	if !IsVsanFileVolumeService {
		log.Infof("StoragePolicyInfo Controller: capability %q is not activated; "+
			"marker-policy topology support is disabled", common.VsanFileVolumeService)
	}

	return add(mgr, newReconciler(mgr, configInfo, recorder,
		commonco.ContainerOrchestratorUtility,
		k8sclient, dynamicClient, IsVsanFileVolumeService))
}

func newReconciler(mgr manager.Manager, configInfo *config.ConfigurationInfo,
	recorder record.EventRecorder, zp zonesProvider,
	k8sClient kubernetes.Interface, dynamicClient dynamic.Interface,
	IsVsanFileVolumeService bool) *ReconcileStoragePolicyInfo {

	return &ReconcileStoragePolicyInfo{
		client:                  mgr.GetClient(),
		scheme:                  mgr.GetScheme(),
		configInfo:              configInfo,
		recorder:                recorder,
		zonesProvider:           zp,
		k8sClient:               k8sClient,
		dynamicClient:           dynamicClient,
		IsVsanFileVolumeService: IsVsanFileVolumeService,
		backOffDuration:         make(map[apitypes.NamespacedName]time.Duration),
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

	// Index StoragePolicyInfo by marker-policy membership so mapFVSNamespaceToMarkerSPIs
	// can list only marker SPIs without a full cross-namespace scan. Only registered
	// when marker-policy support is enabled; the index and the FVS-namespace watch
	// below are wired together since the watch's map func lists by this index.
	if r.IsVsanFileVolumeService {
		if err := mgr.GetFieldIndexer().IndexField(ctx, &spiv1alpha1.StoragePolicyInfo{},
			spiMarkerIndexField, func(obj client.Object) []string {
				if common.IsvSANFileServiceMarkerPolicyName(obj.GetName()) {
					return []string{"true"}
				}
				return nil
			}); err != nil {
			log.Errorf("failed to add field index for marker StoragePolicyInfo: %v", err)
			return err
		}
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

	// Channel for periodic resync; a ticker-driven goroutine lists all
	// StoragePolicyInfo CRs on a configurable interval and pushes them here so
	// the controller re-reconciles each one against the cached InfraStoragePolicyInfo
	// topology.
	// source.Channel drains this into controller-runtime's internal work queue,
	// so a small buffer is sufficient to avoid blocking between sends and reads.
	// If the buffer does fill up (e.g. reconciles are slow), the sender skips the
	// remaining CRs for that tick instead of blocking; see StartPeriodicResync.
	resyncCh := make(chan event.GenericEvent, 256)

	// Trigger re-reconcile of marker-policy SPIs when an FVS instance namespace is
	// created/deleted or its vpc_network_config annotation / fvs_instance_namespace
	// label changes.
	nsFVSPredicate := predicate.Funcs{
		CreateFunc: func(e event.CreateEvent) bool {
			if e.Object == nil {
				return false
			}
			return e.Object.GetLabels()[util.NamespaceLabelFVSInstance] == "true"
		},
		UpdateFunc: func(e event.UpdateEvent) bool {
			if e.ObjectOld == nil || e.ObjectNew == nil {
				return false
			}
			oldAnnotation := e.ObjectOld.GetAnnotations()[util.AnnotationVPCNetworkConfig]
			newAnnotation := e.ObjectNew.GetAnnotations()[util.AnnotationVPCNetworkConfig]
			oldLabel := e.ObjectOld.GetLabels()[util.NamespaceLabelFVSInstance]
			newLabel := e.ObjectNew.GetLabels()[util.NamespaceLabelFVSInstance]
			return oldAnnotation != newAnnotation || oldLabel != newLabel
		},
		DeleteFunc: func(e event.DeleteEvent) bool {
			if e.Object == nil {
				return false
			}
			return e.Object.GetLabels()[util.NamespaceLabelFVSInstance] == "true"
		},
	}

	ctrlBuilder := ctrl.NewControllerManagedBy(mgr).Named("storagepolicyinfo-controller").
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
		WatchesRawSource(source.Channel(resyncCh, &handler.EnqueueRequestForObject{}))

	// The FVS-namespace watch re-triggers marker-policy SPIs on namespace changes.
	// Only wired when marker-policy support is enabled, matching the marker index
	// above (mapFVSNamespaceToMarkerSPIs lists by that index).
	if r.IsVsanFileVolumeService {
		ctrlBuilder = ctrlBuilder.Watches(
			&v1.Namespace{},
			handler.EnqueueRequestsFromMapFunc(r.mapFVSNamespaceToMarkerSPIs),
			builder.WithPredicates(nsFVSPredicate),
		)
	}

	err := ctrlBuilder.
		WithOptions(controller.Options{MaxConcurrentReconciles: maxWorkerThreads}).
		Complete(r)
	if err != nil {
		log.Errorf("failed to build storagepolicyinfo controller. Err: %v", err)
		return err
	}

	interval := getSlowSyncInterval(ctx)
	if err := mgr.Add(manager.RunnableFunc(func(mgrCtx context.Context) error {
		StartPeriodicResync(mgrCtx, r.client, resyncCh, interval, r)
		<-mgrCtx.Done()
		return nil
	})); err != nil {
		log.Errorf("failed to register periodic resync runnable. Err: %v", err)
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

// mapFVSNamespaceToMarkerSPIs maps an FVS instance namespace event to reconcile
// requests for all marker-policy StoragePolicyInfo CRs across all namespaces.
// Marker policy topology depends on FVS instance namespace membership and VPC path,
// so any relevant namespace change must re-trigger all marker SPI reconciliations.
// If the list call below fails, the event is dropped, but the periodic slow-sync
// reconcile independently re-syncs marker-policy topology.
func (r *ReconcileStoragePolicyInfo) mapFVSNamespaceToMarkerSPIs(ctx context.Context,
	_ client.Object) []reconcile.Request {
	log := logger.GetLogger(ctx)

	spiList := &spiv1alpha1.StoragePolicyInfoList{}
	if err := r.client.List(ctx, spiList,
		client.MatchingFields{spiMarkerIndexField: "true"}); err != nil {
		log.Errorf("mapFVSNamespaceToMarkerSPIs: failed to list marker StoragePolicyInfo: %v", err)
		return nil
	}

	if len(spiList.Items) == 0 {
		return nil
	}
	reqs := make([]reconcile.Request, len(spiList.Items))
	for i := range spiList.Items {
		reqs[i] = reconcile.Request{
			NamespacedName: apitypes.NamespacedName{
				Namespace: spiList.Items[i].Namespace,
				Name:      spiList.Items[i].Name,
			},
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
	k8sClient     kubernetes.Interface
	dynamicClient dynamic.Interface
	// IsVsanFileVolumeService mirrors the VsanFileVolumeService capability read at
	// controller start. When false, marker-policy topology support (the marker
	// field index, the FVS-namespace watch, and the syncMarkerPolicyTopology
	// branch) is disabled and only regular policy topology is reconciled.
	IsVsanFileVolumeService bool
	// backOffDuration tracks per-instance requeue delays, incremented
	// exponentially on failure and reset to 1s on success. A value greater than
	// one second means the instance is currently backed off after a failure;
	// slow-sync skips such instances, since they are already scheduled to
	// reconcile via RequeueAfter.
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

	// Snapshot the status so setters below can skip the write if unchanged.
	origStatus := instance.Status.DeepCopy()

	// Populate TopologyInfo from InfraStoragePolicyInfo.
	if err := r.syncTopologyFromInfraSPI(ctx, instance, infraSPI); err != nil {
		log.Errorf("Failed to sync topology for StoragePolicyInfo %q: %v",
			request.NamespacedName, err)
		if setErr := r.setSPIError(ctx, instance, origStatus,
			fmt.Sprintf("Failed to sync topology: %v", err)); setErr != nil {
			log.Errorf("Failed to update StoragePolicyInfo %q status: %v",
				request.NamespacedName, setErr)
		}
		return r.completeReconciliationWithError(ctx, request.NamespacedName, timeout, err)
	}

	// Populate VolumeCapabilities from InfraStoragePolicyInfo.
	if err := syncVolumeCapabilitiesFromInfraSPI(ctx, instance, infraSPI); err != nil {
		log.Errorf("Failed to sync volume capabilities for StoragePolicyInfo %q: %v",
			request.NamespacedName, err)
		if setErr := r.setSPIError(ctx, instance, origStatus,
			fmt.Sprintf("Failed to sync volume capabilities: %v", err)); setErr != nil {
			log.Errorf("Failed to update StoragePolicyInfo %q status: %v",
				request.NamespacedName, setErr)
		}
		return r.completeReconciliationWithError(ctx, request.NamespacedName, timeout, err)
	}

	if setErr := r.setSPISuccess(ctx, instance, origStatus, "Successfully synced topology"); setErr != nil {
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
	clusterSPIRef, err := buildClusterSPIRef(r.scheme, name)
	if err != nil {
		return nil, false, fmt.Errorf("failed to build ClusterStoragePolicyInfoRef for %q: %w", name, err)
	}

	// Create the StoragePolicyInfo CR with the owner reference and spec ref already set.
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
		Spec: spiv1alpha1.StoragePolicyInfoSpec{
			ClusterStoragePolicyInfoRef: clusterSPIRef,
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
// pointing to the given InfraStoragePolicyInfo, and backfills spec.clusterStoragePolicyInfoRef
// if missing. This causes the SPI to be garbage-collected when the InfraStoragePolicyInfo is deleted.
func (r *ReconcileStoragePolicyInfo) ensureInfraSPIOwnerReference(ctx context.Context,
	instance *spiv1alpha1.StoragePolicyInfo,
	infraSPI *infraspiv1alpha1.InfraStoragePolicyInfo) error {
	log := logger.GetLogger(ctx)

	ownerRef, err := generateOwnerReference(r.scheme, infraSPI)
	if err != nil {
		return fmt.Errorf("failed to generate owner reference for InfraStoragePolicyInfo %q: %w",
			infraSPI.Name, err)
	}
	clusterSPIRef, err := buildClusterSPIRef(r.scheme, instance.Name)
	if err != nil {
		return fmt.Errorf("failed to build ClusterStoragePolicyInfoRef for %q: %w", instance.Name, err)
	}

	updatedRefs := mergeOwnerReference(instance.OwnerReferences, ownerRef)
	ownerRefsChanged := !equality.Semantic.DeepEqual(instance.OwnerReferences, updatedRefs)
	specRefChanged := instance.Spec.ClusterStoragePolicyInfoRef != clusterSPIRef
	if !ownerRefsChanged && !specRefChanged {
		return nil
	}

	base := instance.DeepCopy()
	instance.OwnerReferences = updatedRefs
	instance.Spec.ClusterStoragePolicyInfoRef = clusterSPIRef
	if err := r.client.Patch(ctx, instance, client.MergeFrom(base)); err != nil {
		log.Errorf("Failed to patch StoragePolicyInfo %s/%s owner references/spec: %v",
			instance.Namespace, instance.Name, err)
		return err
	}
	log.Infof("Set owner reference and spec ref on StoragePolicyInfo %s/%s → InfraStoragePolicyInfo %q",
		instance.Namespace, instance.Name, infraSPI.Name)
	return nil
}

// syncTopologyFromInfraSPI copies topology information from the cluster-scoped
// InfraStoragePolicyInfo into the namespace-scoped StoragePolicyInfo status,
// filtering accessible zones down to only those assigned to the namespace.
// For marker policies the zone set is derived from FVS instance namespaces that
// share the consumer namespace's VPC path, bypassing the standard namespace filter.
func (r *ReconcileStoragePolicyInfo) syncTopologyFromInfraSPI(ctx context.Context,
	instance *spiv1alpha1.StoragePolicyInfo,
	infraSPI *infraspiv1alpha1.InfraStoragePolicyInfo) error {
	log := logger.GetLogger(ctx)

	// Marker policies derive namespace-level zones from VPC/FVS instance namespace
	// topology rather than from InfraSPI zones filtered by namespace Zone CRs. This
	// path is only taken when marker-policy support is enabled; otherwise a marker
	// policy falls through to the regular namespace-filtered topology path.
	if r.IsVsanFileVolumeService && common.IsvSANFileServiceMarkerPolicyName(instance.Name) {
		return r.syncMarkerPolicyTopology(ctx, instance, infraSPI)
	}

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

// syncVolumeCapabilitiesFromInfraSPI populates the namespace-scoped
// StoragePolicyInfo volume capabilities from the cluster-scoped
// InfraStoragePolicyInfo, with no additional vCenter calls.
// SupportsVolumeModeFilesystem is always true, independent of InfraSPI.
// SupportsVolumeModeBlock and SupportsHostLocal are copied as-is from InfraSPI, since neither
// varies by namespace.
// SupportsLinkedClone and SupportsHighPerformanceLinkedClone are recomputed for just the zones accessible
// to this namespace.
func syncVolumeCapabilitiesFromInfraSPI(ctx context.Context, instance *spiv1alpha1.StoragePolicyInfo,
	infraSPI *infraspiv1alpha1.InfraStoragePolicyInfo) error {
	infraCaps := infraSPI.Status.VolumeCapabilities

	lc, hplc, err := linkedCloneCapabilitiesForNamespace(ctx, instance, infraSPI)
	if err != nil {
		return err
	}

	instance.Status.VolumeCapabilities = map[spiv1alpha1.VolumeCapability]bool{
		spiv1alpha1.SupportsVolumeModeFilesystem:       true,
		spiv1alpha1.SupportsVolumeModeBlock:            infraCaps[infraspiv1alpha1.SupportsVolumeModeBlock],
		spiv1alpha1.SupportsLinkedClone:                lc,
		spiv1alpha1.SupportsHighPerformanceLinkedClone: hplc,
		spiv1alpha1.SupportsHostLocal:                  infraCaps[infraspiv1alpha1.SupportsHostLocal],
	}
	return nil
}

// linkedCloneCapabilitiesForNamespace determines SupportsLinkedClone and
// SupportsHighPerformanceLinkedClone for this namespace's StoragePolicyInfo, recomputed over
// just the namespace's accessible zones (already namespace-filtered by
// syncTopologyFromInfraSPI) via computeLinkedCloneForNamespace/computeHPLCForNamespace.
//
// instance.Status.TopologyInfo is nil only when InfraStoragePolicyInfo itself has no
// Topology, which happens exclusively when the cluster-scoped reconcile failed to resolve the
// policy's StorageClass or StorageTopologyType (see populateTopologyCapabilities) — never as a
// legitimate "non-zonal" state, since a genuinely non-zonal policy still gets a non-nil
// Topology with an empty TopologyType. That's an upstream failure, not an absence of
// applicable zones, so it's surfaced as an error here rather than silently falling back to
// infraCaps (which was computed from that same failed reconcile and can't be trusted either).
func linkedCloneCapabilitiesForNamespace(ctx context.Context, instance *spiv1alpha1.StoragePolicyInfo,
	infraSPI *infraspiv1alpha1.InfraStoragePolicyInfo) (lc bool, hplc bool, err error) {
	if instance.Status.TopologyInfo == nil {
		return false, false, fmt.Errorf(
			"cannot determine LinkedClone capabilities for namespace %q: InfraStoragePolicyInfo %q "+
				"has not resolved its topology yet", instance.Namespace, infraSPI.Name)
	}

	nsZones := instance.Status.TopologyInfo.AccessibleZones
	lc, err = computeLinkedCloneForNamespace(ctx, infraSPI.Name, nsZones)
	if err != nil {
		return false, false, err
	}
	hplc, err = computeHPLCForNamespace(ctx, infraSPI.Name, nsZones, lc)
	if err != nil {
		return false, false, err
	}
	return lc, hplc, nil
}

// computeLinkedCloneForNamespace determines whether the storage policy identified by
// policyName (its K8s-compliant name) supports LinkedClone within nsZones, using
// data already cached by vsphereinfra.StoragePolicyInfoCache.
// LinkedClone is supported for the namespace if any host mounting a
// datastore compatible with policyName within one of nsZones is running ESXi 9.1 or
// above.
func computeLinkedCloneForNamespace(ctx context.Context, policyName string, nsZones []string) (bool, error) {
	return anyQualifyingHostForNamespace(ctx, "LinkedClone", policyName, nsZones, hostSupportsLinkedClone)
}

// computeHPLCForNamespace determines whether SupportsHighPerformanceLinkedClone is true
// for nsZones. HPLC requires LinkedClone support first — if lcSupported is false, HPLC is
// false without evaluating the vSAN-ESA condition. Otherwise it looks for a qualifying
// host (ESXi 9.1+) whose cluster has vSAN-ESA enabled
// (hostSupportsHighPerformanceLinkedClone).
func computeHPLCForNamespace(ctx context.Context, policyName string, nsZones []string,
	lcSupported bool) (bool, error) {
	if !lcSupported {
		return false, nil
	}
	return anyQualifyingHostForNamespace(ctx, "HighPerformanceLinkedClone", policyName, nsZones,
		hostSupportsHighPerformanceLinkedClone)
}

// anyQualifyingHostForNamespace walks every host mounting a datastore compatible with
// policyName within one of nsZones to find out if LC or HPLC are supported or not.
// checkName ("LinkedClone" or "HPLC") identifies which check is running in the logs, since
// both checks walk the same hosts/datastores/zones and would otherwise be indistinguishable.
func anyQualifyingHostForNamespace(ctx context.Context, operation string, policyName string, nsZones []string,
	qualifies func(ctx context.Context, hostID string) (bool, error)) (bool, error) {
	log := logger.GetLogger(ctx)
	for _, zone := range nsZones {
		zoneDS, ok := vsphereinfra.GetCache().GetDatastoresForPolicyZone(ctx, policyName, zone)
		if !ok {
			continue
		}
		log.Debugf("anyQualifyingHostForNamespace[%s]: policy %q has %d compatible datastore(s) cached "+
			"in zone %q", operation, policyName, len(zoneDS), zone)

		for dsID := range zoneDS {
			hosts, ok := vsphereinfra.GetCache().GetDsHosts(dsID)
			if !ok {
				log.Debugf("anyQualifyingHostForNamespace[%s]: no hosts cached yet for datastore %s "+
					"(policy %q, zone %q); skipping", operation, dsID, policyName, zone)
				continue
			}
			log.Infof("anyQualifyingHostForNamespace[%s]: found hosts %+v for datastore %s", operation, hosts, dsID)
			for hostID := range hosts {
				qualified, err := qualifies(ctx, hostID)
				if err != nil {
					return false, fmt.Errorf("failed to check host %s (datastore %s, zone %q): %w",
						hostID, dsID, zone, err)
				}
				if qualified {
					log.Infof("anyQualifyingHostForNamespace[%s]: host %s qualifies for policy %q via "+
						"datastore %s in zone %q", operation, hostID, policyName, dsID, zone)
					return true, nil
				}
				log.Debugf("anyQualifyingHostForNamespace[%s]: host %s does not qualify for policy %q "+
					"(datastore %s, zone %q)", operation, hostID, policyName, dsID, zone)
			}
		}
	}
	log.Infof("anyQualifyingHostForNamespace[%s]: no qualifying host found for policy %q across zones %v",
		operation, policyName, nsZones)
	return false, nil
}

// hostSupportsLinkedClone reports whether a host cached by the inventory watcher is
// running ESXi 9.1 or above. Returns false if the host's version has not been observed
// yet; returns an error if the cached version string can't be parsed.
func hostSupportsLinkedClone(ctx context.Context, hostID string) (bool, error) {
	log := logger.GetLogger(ctx)

	version, found := vsphereinfra.GetCache().GetHostVersion(hostID)
	if !found {
		log.Infof("Version for Host %s not found", hostID)
		return false, nil
	}
	supported, err := cnsvsphere.IsvSphereVersion91orAbove(ctx, vimtypes.AboutInfo{Version: version})
	if err != nil {
		return false, fmt.Errorf("failed to parse ESXi version %q for host %q: %w", version, hostID, err)
	}
	return supported, nil
}

// hostSupportsHighPerformanceLinkedClone reports whether a host cached by the inventory
// watcher both is ESXi 9.1+ and belongs to a cluster with vSAN-ESA enabled
// (ClusterForHost/GetClusterESAEnabled, populated by the clusterstoragepolicyinfo
// controller's isClusterESAEnabled check while reconciling InfraSPI). Returns false if
// the host's cluster, or that cluster's vSAN-ESA state, has not been observed/computed
// yet.
func hostSupportsHighPerformanceLinkedClone(ctx context.Context, hostID string) (bool, error) {
	log := logger.GetLogger(ctx)

	lc, err := hostSupportsLinkedClone(ctx, hostID)
	if err != nil {
		return false, err
	}
	if !lc {
		log.Debugf("hostSupportsHighPerformanceLinkedClone: host %s does not support LinkedClone; HPLC=false",
			hostID)
		return false, nil
	}

	clusterID, ok := vsphereinfra.GetCache().ClusterForHost(hostID)
	if !ok {
		log.Debugf("hostSupportsHighPerformanceLinkedClone: cluster for host %s not yet known; HPLC=false",
			hostID)
		return false, nil
	}

	esa, found := vsphereinfra.GetCache().GetClusterESAEnabled(clusterID)
	if !found {
		log.Debugf("hostSupportsHighPerformanceLinkedClone: vSAN-ESA state for cluster %s (host %s) not "+
			"yet known; HPLC=false", clusterID, hostID)
		return false, nil
	}
	if !esa {
		log.Debugf("hostSupportsHighPerformanceLinkedClone: cluster %s (host %s) does not have vSAN-ESA "+
			"enabled; HPLC=false", clusterID, hostID)
		return false, nil
	}

	log.Debugf("hostSupportsHighPerformanceLinkedClone: host %s in cluster %s qualifies for HPLC",
		hostID, clusterID)
	return true, nil
}

// syncMarkerPolicyTopology populates the namespace-scoped StoragePolicyInfo
// topology for a marker policy by deriving accessible zones from FVS instance
// namespaces that share the consumer namespace's VPC path, bypassing the
// standard InfraSPI-zones-filtered-by-namespace-Zone-CRs path used for
// regular policies.
func (r *ReconcileStoragePolicyInfo) syncMarkerPolicyTopology(ctx context.Context,
	instance *spiv1alpha1.StoragePolicyInfo,
	infraSPI *infraspiv1alpha1.InfraStoragePolicyInfo) error {
	log := logger.GetLogger(ctx)

	log.Infof("syncTopologyFromInfraSPI: %q is a marker policy; deriving zones from FVS instance namespaces",
		instance.Name)
	if infraSPI.Status.Topology == nil || infraSPI.Status.Topology.TopologyType == "" {
		log.Debugf("syncTopologyFromInfraSPI: marker policy %q has no topology type; clearing topology",
			instance.Name)
		instance.Status.TopologyInfo = nil
		return nil
	}

	zonesFn := func(ns string) map[string]struct{} {
		return r.zonesProvider.GetZonesForNamespace(ns)
	}
	zones, err := util.GetZonesForvSANFileServiceMarkerPolicyByNamespace(ctx, r.dynamicClient, r.k8sClient,
		instance.Namespace, zonesFn)
	if err != nil {
		return fmt.Errorf("failed to compute marker-policy zones for namespace %q: %w",
			instance.Namespace, err)
	}
	instance.Status.TopologyInfo = &spiv1alpha1.Topology{
		TopologyType:    infraSPI.Status.Topology.TopologyType,
		AccessibleZones: zones,
	}
	log.Infof("syncTopologyFromInfraSPI: marker policy %q namespace %q zones: %v",
		instance.Name, instance.Namespace, zones)
	return nil
}

// namespaceFilteredZones returns the subset of clusterZones that are also
// assigned to the given namespace, by consulting the namespace-scoped Zone CRs.
func (r *ReconcileStoragePolicyInfo) namespaceFilteredZones(ctx context.Context,
	namespace string, clusterZones []string) []string {
	log := logger.GetLogger(ctx)

	nsZones := r.zonesProvider.GetZonesForNamespace(namespace)
	if len(nsZones) == 0 {
		// Non-zonal deployment or namespace has no zone constraints; expose all
		// cluster-accessible zones.
		log.Debugf("Namespace %q has no zone assignments; using all %d cluster-accessible zones",
			namespace, len(clusterZones))
		return clusterZones
	}

	// Intersect cluster-accessible zones with namespace-assigned zones.
	assignedZones := make([]string, 0, len(clusterZones))
	for _, zone := range clusterZones {
		if _, assigned := nsZones[zone]; assigned {
			assignedZones = append(assignedZones, zone)
		}
	}

	// A zone can span multiple clusters, and this namespace may only be active on a subset
	// of them (or, in an edge case, on none). Confirm each zone-assigned zone actually has an
	// active cluster for this namespace before exposing it — otherwise a policy's datastores
	// attributed to that zone (via PolicyZoneDatastores, computed over every cluster in the
	// zone) could be reported as accessible even though this namespace's volumes could never
	// actually land there.
	// GetActiveClustersForNamespaceInRequestedZones does a full informer-store scan internally,
	// calling it once per zone is fine for typical zone counts.
	filtered := make([]string, 0, len(assignedZones))
	for _, zone := range assignedZones {
		activeClusters, err := r.zonesProvider.GetActiveClustersForNamespaceInRequestedZones(ctx, namespace, []string{zone})
		if err != nil || len(activeClusters) == 0 {
			log.Debugf("Namespace %q is assigned zone %q but has no active cluster within it; excluding: %v",
				namespace, zone, err)
			continue
		}
		filtered = append(filtered, zone)
	}
	log.Infof("Namespace %q has zone assignments %v; filtered %d cluster zones → %d namespace-accessible zones",
		namespace, nsZones, len(clusterZones), len(filtered))
	return filtered
}

// setSPIError sets the error status and records a Warning event on the instance.
// origStatus is the pre-reconcile status snapshot; written only if it changed.
func (r *ReconcileStoragePolicyInfo) setSPIError(ctx context.Context,
	instance *spiv1alpha1.StoragePolicyInfo, origStatus *spiv1alpha1.StoragePolicyInfoStatus, errMsg string) error {
	instance.Status.Error = errMsg
	if !equality.Semantic.DeepEqual(*origStatus, instance.Status) {
		if err := k8s.UpdateStatus(ctx, r.client, instance); err != nil {
			return err
		}
	} else {
		logger.GetLogger(ctx).Debugf("StoragePolicyInfo %q status unchanged, skipping status update", instance.Name)
	}
	r.recorder.Event(instance, v1.EventTypeWarning, "StoragePolicyInfoFailed", errMsg)
	return nil
}

// setSPISuccess clears the error status and records a Normal event on the instance.
// origStatus is the pre-reconcile status snapshot; written only if it changed.
func (r *ReconcileStoragePolicyInfo) setSPISuccess(ctx context.Context,
	instance *spiv1alpha1.StoragePolicyInfo, origStatus *spiv1alpha1.StoragePolicyInfoStatus, msg string) error {
	instance.Status.Error = ""
	if !equality.Semantic.DeepEqual(*origStatus, instance.Status) {
		if err := k8s.UpdateStatus(ctx, r.client, instance); err != nil {
			return err
		}
	} else {
		logger.GetLogger(ctx).Debugf("StoragePolicyInfo %q status unchanged, skipping status update", instance.Name)
	}
	r.recorder.Event(instance, v1.EventTypeNormal, "StoragePolicyInfoSynced", msg)
	return nil
}

// completeReconciliationWithSuccess resets the backoff duration for the instance
// and records a successful reconciliation.
func (r *ReconcileStoragePolicyInfo) completeReconciliationWithSuccess(ctx context.Context,
	namespacedName apitypes.NamespacedName) (reconcile.Result, error) {

	r.backOffDurationMapMutex.Lock()
	delete(r.backOffDuration, namespacedName)
	r.backOffDurationMapMutex.Unlock()

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
