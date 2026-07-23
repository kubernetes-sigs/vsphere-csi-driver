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

package clusterstoragepolicyinfo

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	cnstypes "github.com/vmware/govmomi/cns/types"
	v1 "k8s.io/api/core/v1"
	storagev1 "k8s.io/api/storage/v1"
	"k8s.io/apimachinery/pkg/api/equality"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	apitypes "k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/kubernetes"
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
	clusterspiv1alpha1 "sigs.k8s.io/vsphere-csi-driver/v3/pkg/apis/cnsoperator/clusterstoragepolicyinfo/v1alpha1"
	infraspiv1alpha1 "sigs.k8s.io/vsphere-csi-driver/v3/pkg/apis/cnsoperator/infrastoragepolicyinfo/v1alpha1"
	volumes "sigs.k8s.io/vsphere-csi-driver/v3/pkg/common/cns-lib/volume"
	cnsvsphere "sigs.k8s.io/vsphere-csi-driver/v3/pkg/common/cns-lib/vsphere"
	"sigs.k8s.io/vsphere-csi-driver/v3/pkg/common/config"
	"sigs.k8s.io/vsphere-csi-driver/v3/pkg/csi/service/common"
	"sigs.k8s.io/vsphere-csi-driver/v3/pkg/csi/service/common/commonco"
	commoncotypes "sigs.k8s.io/vsphere-csi-driver/v3/pkg/csi/service/common/commonco/types"
	"sigs.k8s.io/vsphere-csi-driver/v3/pkg/csi/service/logger"
	k8s "sigs.k8s.io/vsphere-csi-driver/v3/pkg/kubernetes"
	"sigs.k8s.io/vsphere-csi-driver/v3/pkg/syncer/cnsoperator/types"
	cnsoperatorutil "sigs.k8s.io/vsphere-csi-driver/v3/pkg/syncer/cnsoperator/util"
	"sigs.k8s.io/vsphere-csi-driver/v3/pkg/syncer/cnsoperator/vsphereinfra"
)

// backOffDuration is a map of ClusterStoragePolicyInfo names to the time after
// which a request for this instance will be requeued.
// Initialized to 1 second for new instances and for instances whose latest
// reconcile operation succeeded.
// If the reconcile fails, backoff is incremented exponentially. A value greater
// than one second therefore means the instance is currently backed off after a
// failure; slow-sync skips such instances, since they are already scheduled to
// reconcile via RequeueAfter.
var (
	backOffDuration         map[apitypes.NamespacedName]time.Duration
	backOffDurationMapMutex = sync.Mutex{}
)

const (
	workerThreadsEnvVar     = "WORKER_THREADS_CLUSTER_STORAGE_POLICY_INFO"
	defaultMaxWorkerThreads = 4
	vsanEncryptionPropID    = "dataAtRestEncryption"
	vsanIopsLimitNs         = "VSAN"
	vsanIopsLimitPropID     = "iopsLimit"
	vmEncryptionNs          = "vmwarevmcrypt"
	vmEncryptionCapID       = "vmwarevmcrypt@ENCRYPTION"
	dataserviceNs           = "com.vmware.storageprofile.dataservice"
)

// Add registers the ClusterStoragePolicyInfo controller with the Manager (WCP / Workload only).
func Add(mgr manager.Manager, clusterFlavor cnstypes.CnsClusterFlavor,
	configInfo *config.ConfigurationInfo, _ volumes.Manager) error {
	ctx, log := logger.GetNewContextWithLogger()
	if clusterFlavor != cnstypes.CnsClusterFlavorWorkload {
		log.Debug("Not initializing the ClusterStoragePolicyInfo Controller: unsupported cluster flavor")
		return nil
	}
	if !commonco.ContainerOrchestratorUtility.IsFSSEnabled(ctx, common.SupportsExposingStoragePolicyAttributes) {
		log.Infof("Not initializing the ClusterStoragePolicyInfo Controller: capability %q is not activated",
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

	// Initialize topology service.
	topologyMgr, err := commonco.ContainerOrchestratorUtility.InitTopologyServiceInController(ctx)
	if err != nil {
		log := logger.GetLogger(ctx)
		log.Warnf("Failed to initialize topology service in ClusterStoragePolicyInfo controller: %v", err)
	}

	eventBroadcaster := record.NewBroadcaster()
	eventBroadcaster.StartRecordingToSink(
		&typedcorev1.EventSinkImpl{
			Interface: k8sclient.CoreV1().Events(""),
		},
	)
	recorder := eventBroadcaster.NewRecorder(scheme.Scheme, v1.EventSource{Component: apis.GroupName})
	return add(mgr, newReconciler(mgr, configInfo, topologyMgr, k8sclient, dynamicClient, recorder))
}

func newReconciler(mgr manager.Manager, configInfo *config.ConfigurationInfo,
	topologyMgr commoncotypes.ControllerTopologyService,
	k8sClient kubernetes.Interface, dynamicClient dynamic.Interface,
	recorder record.EventRecorder) *ReconcileClusterStoragePolicyInfo {

	return &ReconcileClusterStoragePolicyInfo{
		client:        mgr.GetClient(),
		scheme:        mgr.GetScheme(),
		configInfo:    configInfo,
		recorder:      recorder,
		mgr:           mgr,
		topologyMgr:   topologyMgr,
		k8sClient:     k8sClient,
		dynamicClient: dynamicClient,
	}
}

func add(mgr manager.Manager, r *ReconcileClusterStoragePolicyInfo) error {
	ctx, log := logger.GetNewContextWithLogger()
	maxWorkerThreads := cnsoperatorutil.GetMaxWorkerThreads(ctx, workerThreadsEnvVar, defaultMaxWorkerThreads)
	scVacPredicates := predicate.Funcs{
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

	// Trigger re-reconcile of the cluster-wide marker-policy ClusterStoragePolicyInfo when an
	// FVS instance namespace is created/deleted or its vpc_network_config annotation /
	// fvs_instance_namespace label changes.
	nsFVSPredicate := predicate.Funcs{
		CreateFunc: func(e event.CreateEvent) bool {
			if e.Object == nil {
				return false
			}
			return e.Object.GetLabels()[cnsoperatorutil.NamespaceLabelFVSInstance] == "true"
		},
		UpdateFunc: func(e event.UpdateEvent) bool {
			if e.ObjectOld == nil || e.ObjectNew == nil {
				return false
			}
			oldLabel := e.ObjectOld.GetLabels()[cnsoperatorutil.NamespaceLabelFVSInstance]
			newLabel := e.ObjectNew.GetLabels()[cnsoperatorutil.NamespaceLabelFVSInstance]
			if oldLabel != "true" && newLabel != "true" {
				// Not an FVS instance namespace before or after the update; the
				// vpc_network_config annotation is set on every supervisor namespace,
				// so ignore its changes unless this namespace is FVS-relevant.
				return false
			}
			oldAnnotation := e.ObjectOld.GetAnnotations()[cnsoperatorutil.AnnotationVPCNetworkConfig]
			newAnnotation := e.ObjectNew.GetAnnotations()[cnsoperatorutil.AnnotationVPCNetworkConfig]
			return oldAnnotation != newAnnotation || oldLabel != newLabel
		},
		DeleteFunc: func(e event.DeleteEvent) bool {
			if e.Object == nil {
				return false
			}
			return e.Object.GetLabels()[cnsoperatorutil.NamespaceLabelFVSInstance] == "true"
		},
	}

	blder := ctrl.NewControllerManagedBy(mgr).Named("clusterstoragepolicyinfo-controller").
		For(&clusterspiv1alpha1.ClusterStoragePolicyInfo{},
			builder.WithPredicates(predicate.GenerationChangedPredicate{})).
		Watches(
			&storagev1.StorageClass{},
			handler.EnqueueRequestsFromMapFunc(r.mapObjectToClusterSPI),
			builder.WithPredicates(scVacPredicates),
		).
		Watches(
			&v1.Namespace{},
			handler.EnqueueRequestsFromMapFunc(r.mapFVSNamespaceToClusterMarkerSPI),
			builder.WithPredicates(nsFVSPredicate),
		)

	// VolumeAttributesClass API is supported from K8s version 1.34 onwards.
	// Add watch for VolumeAttributesClass only if API is available.
	vacSupported, vacErr := volumeAttributesClassAPIAvailable(mgr)
	if vacErr != nil {
		log.Warnf("Could not discover VolumeAttributesClass API; skipping VAC watch. Err: %v", vacErr)
	} else if !vacSupported {
		log.Infof("VolumeAttributesClass API not registered on this cluster; skipping VAC watch")
	} else {
		log.Infof("VolumeAttributesClass API available; registering VAC watch")
		blder = blder.Watches(
			&storagev1.VolumeAttributesClass{},
			handler.EnqueueRequestsFromMapFunc(r.mapObjectToClusterSPI),
			builder.WithPredicates(scVacPredicates),
		)
	}

	// Channel for periodic resync; a ticker-driven goroutine lists all
	// ClusterStoragePolicyInfo CRs on a configurable interval and pushes them
	// here so the controller re-reconciles each one against the vCenter.
	// source.Channel drains this into controller-runtime's internal work queue,
	// so a small buffer is sufficient to avoid blocking between sends and reads.
	// If the buffer does fill up (e.g. reconciles are slow), the sender skips the
	// remaining CRs for that tick instead of blocking; see StartPeriodicResync.
	resyncCh := make(chan event.GenericEvent, 256)
	blder = blder.WatchesRawSource(source.Channel(resyncCh, &handler.EnqueueRequestForObject{}))

	err := blder.WithOptions(controller.Options{MaxConcurrentReconciles: maxWorkerThreads}).
		Complete(r)
	if err != nil {
		log.Errorf("failed to build clusterstoragepolicyinfo controller. Err: %v", err)
		return err
	}

	// Initialize the backoff duration map
	backOffDuration = make(map[apitypes.NamespacedName]time.Duration)

	interval := getSlowSyncInterval(ctx)
	if err := mgr.Add(manager.RunnableFunc(func(mgrCtx context.Context) error {
		StartPeriodicResync(mgrCtx, r.client, resyncCh, interval)
		<-mgrCtx.Done()
		return nil
	})); err != nil {
		log.Errorf("failed to register periodic resync runnable. Err: %v", err)
		return err
	}

	if err := mgr.Add(manager.RunnableFunc(func(mgrCtx context.Context) error {
		forwardInventoryPolicyChanges(mgrCtx, resyncCh)
		return nil
	})); err != nil {
		log.Errorf("failed to register inventory watcher policy-change forwarder runnable. Err: %v", err)
		return err
	}
	return nil
}

// forwardInventoryPolicyChanges relays storage policy names coalesced by the
// shared PropertyCollector inventory watcher (see vsphereinfra.OnInventoryChange)
// onto ch, so this controller re-reconciles the named ClusterStoragePolicyInfo
// against the vCenter.
func forwardInventoryPolicyChanges(ctx context.Context, ch chan<- event.GenericEvent) {
	forwardPolicyChanges(ctx, vsphereinfra.PolicyChanges(), ch)
}

// forwardPolicyChanges is forwardInventoryPolicyChanges with its source
// channel injected, so tests can drive it without reaching into vsphereinfra's
// process-wide PolicyChanges() channel.
func forwardPolicyChanges(ctx context.Context, src <-chan string, dst chan<- event.GenericEvent) {
	log := logger.GetLogger(ctx)
	for {
		select {
		case <-ctx.Done():
			log.Infof("ClusterStoragePolicyInfo inventory policy-change forwarder stopping")
			return
		case policyName, ok := <-src:
			if !ok {
				log.Infof("ClusterStoragePolicyInfo inventory policy-change forwarder stopping: source channel closed")
				return
			}
			if policyName == "" {
				log.Warnf("ClusterStoragePolicyInfo inventory policy-change forwarder: received empty policy name, skipping")
				continue
			}
			namespacedName := apitypes.NamespacedName{Name: policyName}
			backOffDurationMapMutex.Lock()
			backoff := backOffDuration[namespacedName]
			backOffDurationMapMutex.Unlock()
			if backoff > time.Second {
				// A backoff greater than one second means this instance is already scheduled to
				// reconcile via RequeueAfter after a prior failure. Forwarding
				// it now would just add a redundant, immediate reconcile on top
				// of that already-pending one.
				log.Debugf("ClusterStoragePolicyInfo inventory policy-change forwarder: "+
					"%q is backed off and already scheduled to reconcile, skipping", policyName)
				continue
			}

			obj := &clusterspiv1alpha1.ClusterStoragePolicyInfo{
				ObjectMeta: metav1.ObjectMeta{Name: policyName},
			}
			select {
			case dst <- event.GenericEvent{Object: obj}:
				log.Infof("ClusterStoragePolicyInfo inventory policy-change forwarder: "+
					"queued %q for reconciliation", policyName)
			case <-ctx.Done():
				return
			default:
				// resyncCh is full; do not block this forwarder waiting for a
				// slot. The policy stays out of the coalescing queue at this
				// point (see queuePolicyReconcile), so it will be re-queued by
				// the next inventory event that affects it, or picked up by
				// the periodic slow-sync in the meantime.
				log.Warnf("ClusterStoragePolicyInfo inventory policy-change forwarder: "+
					"resync channel full, dropping reconcile for %q", policyName)
			}
		}
	}
}

// mapObjectToClusterSPI maps any client.Object to a ClusterStoragePolicyInfo reconcile request.
// Used for both StorageClass and VolumeAttributesClass watches.
func (r *ReconcileClusterStoragePolicyInfo) mapObjectToClusterSPI(ctx context.Context,
	obj client.Object) []reconcile.Request {
	if obj == nil {
		return nil
	}

	// If this is a StorageClass, skip WFFC StorageClasses
	if sc, ok := obj.(*storagev1.StorageClass); ok {
		if storageClassIsWaitForFirstConsumer(sc) {
			return nil
		}
	}

	return []reconcile.Request{{NamespacedName: apitypes.NamespacedName{Name: obj.GetName()}}}
}

// mapFVSNamespaceToClusterMarkerSPI maps an FVS instance namespace event to a reconcile
// request for the single cluster-scoped vSAN File Service marker-policy
// ClusterStoragePolicyInfo. Marker-policy topology depends on FVS instance namespace VPC
// membership, so any relevant namespace change must re-trigger recomputation.
func (r *ReconcileClusterStoragePolicyInfo) mapFVSNamespaceToClusterMarkerSPI(ctx context.Context,
	obj client.Object) []reconcile.Request {
	log := logger.GetLogger(ctx)
	nsName := ""
	if obj != nil {
		nsName = obj.GetName()
	}
	log.Infof("FVS instance namespace %q changed; requeuing marker-policy ClusterStoragePolicyInfo %q",
		nsName, common.StorageClassVsanFileServicePolicy)

	return []reconcile.Request{{
		NamespacedName: apitypes.NamespacedName{Name: common.StorageClassVsanFileServicePolicy},
	}}
}

var _ reconcile.Reconciler = &ReconcileClusterStoragePolicyInfo{}

// ReconcileClusterStoragePolicyInfo reconciles ClusterStoragePolicyInfo objects.
type ReconcileClusterStoragePolicyInfo struct {
	client        client.Client
	scheme        *runtime.Scheme
	configInfo    *config.ConfigurationInfo
	recorder      record.EventRecorder
	mgr           manager.Manager
	topologyMgr   commoncotypes.ControllerTopologyService
	k8sClient     kubernetes.Interface
	dynamicClient dynamic.Interface
}

// Reconcile syncs storage policy attributes from the vCenter.
func (r *ReconcileClusterStoragePolicyInfo) Reconcile(ctx context.Context,
	request reconcile.Request) (reconcile.Result, error) {
	ctx = logger.NewContextWithLogger(ctx)
	log := logger.GetLogger(ctx).With("name", request.NamespacedName)

	log.Infof("Reconciling ClusterStoragePolicyInfo")

	// Initialize backOffDuration for the instance, if required.
	backOffDurationMapMutex.Lock()
	var timeout time.Duration
	if _, exists := backOffDuration[request.NamespacedName]; !exists {
		backOffDuration[request.NamespacedName] = time.Second
	}
	timeout = backOffDuration[request.NamespacedName]
	backOffDurationMapMutex.Unlock()

	instance := &clusterspiv1alpha1.ClusterStoragePolicyInfo{}
	err := r.client.Get(ctx, request.NamespacedName, instance)
	if err != nil {
		if !apierrors.IsNotFound(err) {
			log.Errorf("Failed to get ClusterStoragePolicyInfo %q: %v", request.NamespacedName, err)
			return r.completeReconciliationWithError(ctx, request.NamespacedName, timeout, err)
		}
		instance = nil // Instance not found, will be created later
	}

	if instance != nil && instance.DeletionTimestamp != nil {
		log.Infof("Instance %q is being deleted, skipping reconciliation", request.Name)
		return r.completeReconciliationWithSuccess(ctx, request.NamespacedName, timeout)
	}

	// Handle creation and owner reference management
	instance, wasCreated, err := r.ensureClusterSPIExistsInReconcile(ctx, instance, request.Name)
	if err != nil {
		return r.completeReconciliationWithError(ctx, request.NamespacedName, timeout, err)
	}

	// If no instance returned, the CR doesn't exist and neither does a backing SC/VAC - nothing to manage.
	// If the CR already existed (e.g. pre-created by full sync) it is returned even without a backing
	// SC/VAC so InfraStoragePolicyInfo creation below still proceeds.
	if instance == nil {
		return r.completeReconciliationWithSuccess(ctx, request.NamespacedName, timeout)
	}

	// If a new instance was created, the creation event will trigger another reconcile automatically
	if wasCreated {
		log.Infof("Instance was created, creation event will trigger next reconcile")
		return r.completeReconciliationWithSuccess(ctx, request.NamespacedName, timeout)
	}

	// Snapshot the status so setters below can skip the write if unchanged.
	origClusterStatus := instance.Status.DeepCopy()

	// Ensure InfraStoragePolicyInfo CR exists with the same name
	infraSPI, err := r.ensureInfraSPIExists(ctx, instance)
	if err != nil {
		log.Errorf("Failed to ensure InfraStoragePolicyInfo exists for %q: %v", request.Name, err)
		errorMsg := fmt.Sprintf("Failed to ensure InfraStoragePolicyInfo exists: %v", err)
		if setErr := r.setClusterSPIError(ctx, instance, origClusterStatus, errorMsg); setErr != nil {
			log.Errorf("Failed to set error status: %v", setErr)
		}
		return r.completeReconciliationWithError(ctx, request.NamespacedName, timeout, err)
	}

	// Snapshot the status so setters below can skip the write if unchanged.
	origInfraStatus := infraSPI.Status.DeepCopy()

	// Connect to vCenter.
	vc, err := cnsvsphere.GetVirtualCenterInstance(ctx, r.configInfo, false)
	if err != nil {
		log.Errorf("Failed to get vCenter instance for %q: %v", request.Name, err)
		errorMsg := fmt.Sprintf("Failed to get vCenter instance: %v", err)
		if setErr := r.setClusterSPIError(ctx, instance, origClusterStatus, errorMsg); setErr != nil {
			log.Errorf("Failed to set error status: %v", setErr)
		}
		return r.completeReconciliationWithError(ctx, request.NamespacedName, timeout, err)
	}

	// Find storage policy profile by K8s compliant name
	profile, policyDeleted, err := findStoragePolicyProfile(ctx, instance, vc)
	if err != nil {
		errorMsg := fmt.Sprintf("Failed to lookup storage policy: %v", err)
		if setErr := r.setClusterSPIError(ctx, instance, origClusterStatus, errorMsg); setErr != nil {
			log.Errorf("Failed to set error status: %v", setErr)
		}
		return r.completeReconciliationWithError(ctx, request.NamespacedName, timeout, err)
	}
	if policyDeleted {
		// Policy was deleted - update status and return success
		if statusErr := r.setClusterSPISuccess(ctx, instance, origClusterStatus,
			"Storage policy deleted from vCenter"); statusErr != nil {
			log.Errorf("failed to update status for ClusterStoragePolicyInfo %q: %v", request.Name, statusErr)
			return r.completeReconciliationWithError(ctx, request.NamespacedName, timeout, statusErr)
		}
		return r.completeReconciliationWithSuccess(ctx, request.NamespacedName, timeout)
	}

	policyContent, err := vc.PbmRetrieveContent(ctx, []string{profile.ID})
	if err != nil {
		log.Errorf("Failed to retrieve policy content for profile %s: %v", profile.ID, err)
		errorMsg := fmt.Sprintf("Failed to retrieve policy content: %v", err)
		if setErr := r.setClusterSPIError(ctx, instance, origClusterStatus, errorMsg); setErr != nil {
			log.Errorf("Failed to set error status: %v", setErr)
		}
		return r.completeReconciliationWithError(ctx, request.NamespacedName, timeout, err)
	}

	// Sync storage policy attributes for ClusterSPI instance.
	err = r.syncClusterSPIAttributes(ctx, instance, profile, vc, policyContent)
	if err != nil {
		log.Errorf("Failed to sync storage policy attributes for %q: %v.", request.Name, err)
		errorMsg := fmt.Sprintf("Failed to sync storage policy attributes: %v", err)
		if setErr := r.setClusterSPIError(ctx, instance, origClusterStatus, errorMsg); setErr != nil {
			log.Errorf("Failed to set error status: %v", setErr)
		}
		return r.completeReconciliationWithError(ctx, request.NamespacedName, timeout, err)
	}

	statusErr := r.setClusterSPISuccess(ctx, instance, origClusterStatus, "Successfully synced storage policy attributes")
	if statusErr != nil {
		log.Errorf("failed to update status for ClusterStoragePolicyInfo %q: %v", request.Name, statusErr)
		return r.completeReconciliationWithError(ctx, request.NamespacedName, timeout, statusErr)
	}

	// Sync InfraSPI attributes for InfraSPI instance.
	err = r.syncInfraSPIAttributes(ctx, instance, infraSPI, vc, profile, policyContent)
	if err != nil {
		log.Errorf("Failed to sync InfraSPI attributes for %q: %v.", request.Name, err)
		errorMsg := fmt.Sprintf("Failed to sync InfraSPI attributes: %v", err)
		if setErr := r.setInfraSPIError(ctx, infraSPI, origInfraStatus, errorMsg); setErr != nil {
			log.Errorf("Failed to set infraSPI error status: %v", setErr)
		}
		return r.completeReconciliationWithError(ctx, request.NamespacedName, timeout, err)
	}

	infraSPIStatusErr := r.setInfraSPISuccess(ctx, infraSPI, origInfraStatus, "Successfully synced InfraSPI attributes")
	if infraSPIStatusErr != nil {
		log.Errorf("failed to update status for InfraStoragePolicyInfo %q: %v", request.Name, infraSPIStatusErr)
		return r.completeReconciliationWithError(ctx, request.NamespacedName, timeout, infraSPIStatusErr)
	}

	log.Infof("Successfully synced storage policy attributes for %q", request.Name)
	return r.completeReconciliationWithSuccess(ctx, request.NamespacedName, timeout)
}

// ensureClusterSPIExistsInReconcile handles ClusterStoragePolicyInfo creation and owner reference
// management in Reconcile.
// Returns (instance, wasCreated, error).
func (r *ReconcileClusterStoragePolicyInfo) ensureClusterSPIExistsInReconcile(ctx context.Context,
	instance *clusterspiv1alpha1.ClusterStoragePolicyInfo, name string) (
	*clusterspiv1alpha1.ClusterStoragePolicyInfo, bool, error) {
	// Check what resources exist
	sc, err := r.checkStorageClassExists(ctx, name)
	if err != nil {
		return nil, false, err
	}

	vac, err := r.checkVolumeAttributesClassExists(ctx, name)
	if err != nil {
		return nil, false, err
	}

	// Get or create the ClusterStoragePolicyInfo with owner references
	instance, wasCreated, err := r.ensureClusterSPIInstance(ctx, instance, name, sc, vac)
	return instance, wasCreated, err
}

// checkStorageClassExists checks if a StorageClass with the given name exists and is valid.
func (r *ReconcileClusterStoragePolicyInfo) checkStorageClassExists(ctx context.Context,
	name string) (*storagev1.StorageClass, error) {
	sc := &storagev1.StorageClass{}

	if err := r.client.Get(ctx, apitypes.NamespacedName{Name: name}, sc); err == nil {
		return sc, nil
	} else if !apierrors.IsNotFound(err) {
		return nil, err
	}

	return nil, nil
}

// checkVolumeAttributesClassExists checks if a VolumeAttributesClass with the given name exists.
func (r *ReconcileClusterStoragePolicyInfo) checkVolumeAttributesClassExists(ctx context.Context,
	name string) (*storagev1.VolumeAttributesClass, error) {
	log := logger.GetLogger(ctx)
	vac := &storagev1.VolumeAttributesClass{}

	vacSupported, vacErr := volumeAttributesClassAPIAvailable(r.mgr)
	if vacErr != nil {
		log.Warnf("Could not discover VolumeAttributesClass API; skipping VAC lookup. Err: %v", vacErr)
		return nil, nil
	}

	if !vacSupported {
		return nil, nil
	}

	if err := r.client.Get(ctx, apitypes.NamespacedName{Name: name}, vac); err == nil {
		return vac, nil
	} else if !apierrors.IsNotFound(err) {
		return nil, err
	}

	return nil, nil
}

// ensureClusterSPIInstance ensures the ClusterStoragePolicyInfo instance exists when SC or VAC exists.
// Returns (instance, wasCreated, error).
func (r *ReconcileClusterStoragePolicyInfo) ensureClusterSPIInstance(ctx context.Context,
	instance *clusterspiv1alpha1.ClusterStoragePolicyInfo, name string,
	sc *storagev1.StorageClass, vac *storagev1.VolumeAttributesClass) (
	*clusterspiv1alpha1.ClusterStoragePolicyInfo, bool, error) {
	log := logger.GetLogger(ctx)

	// If neither SC nor VAC exists, there are no owner references to manage. If the instance already
	// exists (e.g. pre-created by full sync ahead of any StorageClass), return it as-is so the caller
	// still proceeds to ensure the InfraStoragePolicyInfo CR. If it doesn't exist, there's nothing to do;
	// the reactive SC/VAC create event will trigger creation later.
	if sc == nil && vac == nil {
		if instance != nil {
			log.Debugf("ClusterStoragePolicyInfo %q exists but no matching StorageClass or VolumeAttributesClass "+
				"found yet", name)
			return instance, false, nil
		}
		log.Debugf("No StorageClass or VolumeAttributesClass found for %q, nothing to create", name)
		return nil, false, nil
	}

	// Build owner references
	ownerRefs := buildOwnerReferences(ctx, r.scheme, name, sc, vac)

	// Create SPI if it doesn't exist
	if instance == nil {
		instance = &clusterspiv1alpha1.ClusterStoragePolicyInfo{
			TypeMeta: metav1.TypeMeta{
				APIVersion: apis.SchemeGroupVersion.String(),
				Kind:       "ClusterStoragePolicyInfo",
			},
			ObjectMeta: metav1.ObjectMeta{
				Name:            name,
				OwnerReferences: ownerRefs,
			},
		}
		if err := r.client.Create(ctx, instance); err != nil {
			if !apierrors.IsAlreadyExists(err) {
				log.Errorf("Failed to create ClusterStoragePolicyInfo %q: %v", name, err)
				return nil, false, err
			}
			// Validate and update owner references on the existing instance
			updatedInstance, err := r.validateAndUpdateOwnerReferences(ctx, instance, ownerRefs)
			return updatedInstance, false, err
		} else {
			log.Infof("Created ClusterStoragePolicyInfo %q with owner references", name)
			return instance, true, nil
		}
	}

	// SPI exists, check if owner references need updating
	updatedInstance, err := r.validateAndUpdateOwnerReferences(ctx, instance, ownerRefs)
	return updatedInstance, false, err
}

// ensureInfraSPIExists creates an InfraStoragePolicyInfo CR with the same name as the ClusterSPI
// and sets the ClusterSPI as its owner reference. Returns the InfraSPI instance.
func (r *ReconcileClusterStoragePolicyInfo) ensureInfraSPIExists(ctx context.Context,
	clusterSPI *clusterspiv1alpha1.ClusterStoragePolicyInfo) (*infraspiv1alpha1.InfraStoragePolicyInfo, error) {
	log := logger.GetLogger(ctx)

	// Check if InfraSPI already exists
	infraSPI := &infraspiv1alpha1.InfraStoragePolicyInfo{}
	err := r.client.Get(ctx, apitypes.NamespacedName{Name: clusterSPI.Name}, infraSPI)
	if err == nil {
		// InfraSPI already exists, check if it has the correct owner reference
		err := r.ensureInfraSPIOwnerReference(ctx, infraSPI, clusterSPI)
		return infraSPI, err
	}

	if !apierrors.IsNotFound(err) {
		log.Errorf("Failed to get InfraStoragePolicyInfo %q: %v", clusterSPI.Name, err)
		return nil, err
	}

	// Generate owner reference so InfraSPI is owned by ClusterSPI
	ownerRef, err := generateOwnerReference(r.scheme, clusterSPI)
	if err != nil {
		log.Errorf("Failed to generate owner reference for ClusterSPI %q: %v", clusterSPI.Name, err)
		return nil, err
	}

	// Build spec.clusterStoragePolicyInfoRef pointing at the ClusterSPI
	clusterSPIRef, err := buildClusterSPIRef(r.scheme, clusterSPI)
	if err != nil {
		log.Errorf("Failed to build ClusterStoragePolicyInfoRef for ClusterSPI %q: %v", clusterSPI.Name, err)
		return nil, err
	}

	infraSPI = &infraspiv1alpha1.InfraStoragePolicyInfo{
		TypeMeta: metav1.TypeMeta{
			APIVersion: apis.SchemeGroupVersion.String(),
			Kind:       "InfraStoragePolicyInfo",
		},
		ObjectMeta: metav1.ObjectMeta{
			Name:            clusterSPI.Name,
			OwnerReferences: []metav1.OwnerReference{ownerRef},
		},
		Spec: infraspiv1alpha1.InfraStoragePolicyInfoSpec{
			ClusterStoragePolicyInfoRef: clusterSPIRef,
		},
	}

	err = r.client.Create(ctx, infraSPI)
	if err != nil {
		if apierrors.IsAlreadyExists(err) {
			// If it was created between our check and create, update owner references
			err = r.client.Get(ctx, apitypes.NamespacedName{Name: clusterSPI.Name}, infraSPI)
			if err != nil {
				return nil, err
			}
			err := r.ensureInfraSPIOwnerReference(ctx, infraSPI, clusterSPI)
			return infraSPI, err
		}
		log.Errorf("Failed to create InfraStoragePolicyInfo %q: %v", clusterSPI.Name, err)
		return nil, err
	}

	log.Infof("Created InfraStoragePolicyInfo %q with owner reference to ClusterSPI", clusterSPI.Name)
	return infraSPI, nil
}

// ensureInfraSPIOwnerReference ensures that the InfraSPI has the correct owner reference and
// spec.clusterStoragePolicyInfoRef pointing at the ClusterSPI.
func (r *ReconcileClusterStoragePolicyInfo) ensureInfraSPIOwnerReference(ctx context.Context,
	infraSPI *infraspiv1alpha1.InfraStoragePolicyInfo, clusterSPI *clusterspiv1alpha1.ClusterStoragePolicyInfo) error {
	log := logger.GetLogger(ctx)

	// Generate expected owner reference
	expectedOwnerRef, err := generateOwnerReference(r.scheme, clusterSPI)
	if err != nil {
		log.Errorf("Failed to generate owner reference for ClusterSPI %q: %v", clusterSPI.Name, err)
		return err
	}
	expectedClusterSPIRef, err := buildClusterSPIRef(r.scheme, clusterSPI)
	if err != nil {
		log.Errorf("Failed to build ClusterStoragePolicyInfoRef for ClusterSPI %q: %v", clusterSPI.Name, err)
		return err
	}

	// Check if owner reference needs to be added or updated
	currentOwnerRefs := infraSPI.OwnerReferences
	updatedOwnerRefs := mergeOwnerReference(currentOwnerRefs, expectedOwnerRef)

	ownerRefsChanged := !equality.Semantic.DeepEqual(infraSPI.OwnerReferences, updatedOwnerRefs)
	specRefChanged := infraSPI.Spec.ClusterStoragePolicyInfoRef != expectedClusterSPIRef

	// Update if needed
	if ownerRefsChanged || specRefChanged {
		base := infraSPI.DeepCopy()
		infraSPI.OwnerReferences = updatedOwnerRefs
		infraSPI.Spec.ClusterStoragePolicyInfoRef = expectedClusterSPIRef
		if err := r.client.Patch(ctx, infraSPI, client.MergeFrom(base)); err != nil {
			log.Errorf("Failed to update InfraStoragePolicyInfo %q owner references/spec: %v", infraSPI.Name, err)
			return err
		}
		log.Infof("Updated InfraStoragePolicyInfo %q owner references/spec", infraSPI.Name)
	}

	return nil
}

// validateAndUpdateOwnerReferences validates and updates owner references on existing
// ClusterStoragePolicyInfo instance.
func (r *ReconcileClusterStoragePolicyInfo) validateAndUpdateOwnerReferences(ctx context.Context,
	instance *clusterspiv1alpha1.ClusterStoragePolicyInfo, expectedOwnerRefs []metav1.OwnerReference) (
	*clusterspiv1alpha1.ClusterStoragePolicyInfo, error) {
	log := logger.GetLogger(ctx)

	// Merge expected owner references with existing ones
	currentOwnerRefs := instance.OwnerReferences
	for _, expectedRef := range expectedOwnerRefs {
		currentOwnerRefs = mergeOwnerReference(currentOwnerRefs, expectedRef)
	}

	// Check if update is needed
	if !equality.Semantic.DeepEqual(instance.OwnerReferences, currentOwnerRefs) {
		base := instance.DeepCopy()
		instance.OwnerReferences = currentOwnerRefs
		if err := r.client.Patch(ctx, instance, client.MergeFrom(base)); err != nil {
			log.Errorf("Failed to update ClusterStoragePolicyInfo %q owner references: %v", instance.Name, err)
			return nil, err
		}
		log.Infof("Updated ClusterStoragePolicyInfo %q owner references", instance.Name)
	}

	return instance, nil
}

// completeReconciliationWithSuccess resets the backoff duration for the instance
// and records a successful reconciliation.
func (r *ReconcileClusterStoragePolicyInfo) completeReconciliationWithSuccess(ctx context.Context,
	namespacedName apitypes.NamespacedName, timeout time.Duration) (reconcile.Result, error) {
	log := logger.GetLogger(ctx).With("name", namespacedName)

	// Reset backOff duration to one second on success.
	backOffDurationMapMutex.Lock()
	delete(backOffDuration, namespacedName)
	backOffDurationMapMutex.Unlock()

	log.Infof("Successfully reconciled ClusterStoragePolicyInfo")
	return reconcile.Result{}, nil
}

// completeReconciliationWithError updates the backoff duration for the instance
// and schedules a retry after the backoff timeout.
func (r *ReconcileClusterStoragePolicyInfo) completeReconciliationWithError(ctx context.Context,
	namespacedName apitypes.NamespacedName, timeout time.Duration, err error) (reconcile.Result, error) {
	log := logger.GetLogger(ctx).With("name", namespacedName)

	// Double backOff duration on error, up to the maximum.
	backOffDurationMapMutex.Lock()
	backOffDuration[namespacedName] = min(backOffDuration[namespacedName]*2,
		types.MaxBackOffDurationForReconciler)
	backOffDurationMapMutex.Unlock()

	log.Errorf("Failed to reconcile ClusterStoragePolicyInfo %q. Err: %v",
		namespacedName.Name, err)

	return reconcile.Result{RequeueAfter: timeout}, nil
}

// syncClusterSPIAttributes syncs storage policy attributes from vCenter.
func (r *ReconcileClusterStoragePolicyInfo) syncClusterSPIAttributes(ctx context.Context,
	instance *clusterspiv1alpha1.ClusterStoragePolicyInfo, profile *cnsvsphere.ProfileDetail,
	vc *cnsvsphere.VirtualCenter, policyContent []cnsvsphere.SpbmPolicyContent) error {
	log := logger.GetLogger(ctx)

	log.Infof("Syncing storage policy attributes for ClusterStoragePolicyInfo %q (policy ID: %s, name: %s)",
		instance.Name, profile.ID, profile.Name)

	if len(policyContent) == 0 {
		log.Warnf("No policy content found for profile %s", profile.ID)
		return nil
	}

	var overallErr error

	// Populate encryption capabilities (vSAN and VM).
	if err := populateEncryptionCapabilities(ctx, instance, profile.ID, vc, policyContent); err != nil {
		log.Errorf("Failed to populate encryption capabilities for profile %s: %v", profile.ID, err)
		overallErr = errors.Join(overallErr, err)
	}

	// Populate performance capabilities.
	if err := populatePerformanceCapabilities(ctx, instance, profile.ID, policyContent); err != nil {
		log.Errorf("Failed to populate performance capabilities for profile %s: %v", profile.ID, err)
		overallErr = errors.Join(overallErr, err)
	}

	return overallErr
}

// syncInfraSPIAttributes syncs InfraStoragePolicyInfo attributes from vCenter.
func (r *ReconcileClusterStoragePolicyInfo) syncInfraSPIAttributes(ctx context.Context,
	instance *clusterspiv1alpha1.ClusterStoragePolicyInfo,
	infraSPI *infraspiv1alpha1.InfraStoragePolicyInfo,
	vc *cnsvsphere.VirtualCenter, profile *cnsvsphere.ProfileDetail,
	policyContent []cnsvsphere.SpbmPolicyContent) error {
	log := logger.GetLogger(ctx)

	log.Infof("Syncing InfraSPI attributes for %q (policy ID: %s, name: %s)",
		instance.Name, profile.ID, profile.Name)

	var overallErr error

	// Populate topology capabilities for InfraSPI.
	zoneCompatibleDS, err := r.populateTopologyCapabilities(ctx, instance, infraSPI, profile.ID, vc, policyContent)
	if err != nil {
		log.Errorf("Failed to populate topology capabilities for profile %s: %v", profile.ID, err)
		overallErr = errors.Join(overallErr, err)
	}

	// Populate volume capabilities.
	if err := populateVolumeCapabilities(ctx, infraSPI, vc, profile.ID, zoneCompatibleDS); err != nil {
		log.Errorf("Failed to populate volume capabilities for profile %s: %v", profile.ID, err)
		overallErr = errors.Join(overallErr, err)
	}

	return overallErr
}

// populateTopologyCapabilities populates topology information for the storage policy in InfraSPI,
// and returns the zone->compatible-datastore map computed along the way (nil for marker
// policies).
func (r *ReconcileClusterStoragePolicyInfo) populateTopologyCapabilities(ctx context.Context,
	clusterSPI *clusterspiv1alpha1.ClusterStoragePolicyInfo,
	infraSPI *infraspiv1alpha1.InfraStoragePolicyInfo, profileID string,
	vc *cnsvsphere.VirtualCenter,
	clusterSPIPolicyContent []cnsvsphere.SpbmPolicyContent) (map[string][]*cnsvsphere.DatastoreInfo, error) {
	log := logger.GetLogger(ctx)

	// Marker policies (e.g. vSAN File Service) derive their cluster-wide accessible zones from
	// FVS instance namespaces rather than datastore/PBM compatibility.
	if common.IsvSANFileServiceMarkerPolicyName(clusterSPI.Name) {
		log.Infof("%q is a vSAN File Service marker policy; deriving zones from FVS instance namespaces",
			clusterSPI.Name)
		zonesFn := func(ns string) map[string]struct{} {
			return commonco.ContainerOrchestratorUtility.GetZonesForNamespace(ns)
		}
		zones, err := cnsoperatorutil.GetZonesForvSANFileServiceMarkerPolicy(ctx, r.k8sClient, zonesFn)
		if err != nil {
			return nil, fmt.Errorf("failed to compute cluster zones for vSAN File Service marker policy %q: %w",
				clusterSPI.Name, err)
		}
		infraSPI.Status.Topology = &infraspiv1alpha1.Topology{
			TopologyType:    "zonal",
			AccessibleZones: zones,
		}
		log.Infof("vSAN File Service marker policy %q accessible zones: %v", clusterSPI.Name, zones)
		return nil, nil
	}

	// Determine the topology type for this policy. Prefer the StorageClass's StorageTopologyType
	// parameter when a StorageClass references the policy; otherwise derive it directly from the
	// policy's PBM capabilities. Deriving from PBM lets us populate topology for policies whose CRs
	// were created ahead of any StorageClass (see full sync).
	storageClass, err := getStorageClassForPolicy(ctx, r.client, profileID)
	if err != nil {
		return nil, fmt.Errorf("failed to get StorageClass for policy: %w", err)
	}

	var topologyType string
	if storageClass != nil {
		// Get the StorageTopologyType parameter value from StorageClass
		topologyType, err = getStorageTopologyType(ctx, storageClass)
		if err != nil {
			log.Errorf("Storage policy %s does not have a valid StorageTopologyType parameter: %v",
				profileID, err)
			return nil, err
		}
		log.Infof("Storage policy %s has topology type %q from StorageClass %q", profileID, topologyType,
			storageClass.Name)
	} else {
		// No StorageClass references this policy yet; derive the topology type directly from the
		// policy's PBM capabilities.
		topologyType = getStorageTopologyTypeFromPolicy(ctx, clusterSPIPolicyContent)
		log.Infof("No StorageClass found for storage policy %s; derived topology type %q from PBM policy content",
			profileID, topologyType)
	}

	// Initialize topology status with the topology type
	infraSPI.Status.Topology = &infraspiv1alpha1.Topology{
		TopologyType: topologyType,
	}

	log.Infof("Storage policy %s: populating accessible zones", profileID)

	// GetZoneCompatibleDatastoresForPolicy makes SPBM CheckRequirements call, scoped to
	// every cluster registered to a vSphere AvailabilityZone, to determine both the accessible
	// zones and the compatible datastores within each zone.
	accessibleZones, zoneCompatibleDS, dsIDs, err := cnsoperatorutil.GetZoneCompatibleDatastoresForPolicy(ctx,
		r.topologyMgr, vc, profileID)
	if err != nil {
		return nil, fmt.Errorf("failed to get accessible zones: %w", err)
	}

	// Record which datastores this policy is compatible with, so the shared
	// PropertyCollector inventory watcher (vsphereinfra.OnInventoryChange) can
	// resolve a future datastore/host topology change straight to this policy
	// without a separate vCenter round trip.
	vsphereinfra.GetCache().SetDatastoresForPolicy(clusterSPI.Name, dsIDs)

	// Record, per zone, which datastores are compatible with this policy, so the
	// namespace-scoped storagepolicyinfo controller can determine SupportsLinkedClone/
	// SupportsHighPerformanceLinkedClone for a given zone without any additional vCenter or
	// PBM call.
	zoneDsIDs := make(map[string][]string, len(zoneCompatibleDS))
	for zone, datastores := range zoneCompatibleDS {
		ids := make([]string, 0, len(datastores))
		for _, ds := range datastores {
			ids = append(ids, ds.Reference().Value)
		}
		zoneDsIDs[zone] = ids
	}
	vsphereinfra.GetCache().SetDatastoresForPolicyZones(clusterSPI.Name, zoneDsIDs)

	// Update InfraSPI with topology information including accessible zones
	infraSPI.Status.Topology.AccessibleZones = accessibleZones

	return zoneCompatibleDS, nil
}

// setClusterSPIError sets error and records an event on the ClusterStoragePolicyInfo instance.
// origStatus is the pre-reconcile status snapshot; written only if it changed.
func (r *ReconcileClusterStoragePolicyInfo) setClusterSPIError(ctx context.Context,
	instance *clusterspiv1alpha1.ClusterStoragePolicyInfo,
	origStatus *clusterspiv1alpha1.ClusterStoragePolicyInfoStatus, errMsg string) error {
	instance.Status.Error = errMsg
	if !equality.Semantic.DeepEqual(*origStatus, instance.Status) {
		if err := k8s.UpdateStatus(ctx, r.client, instance); err != nil {
			return err
		}
	} else {
		logger.GetLogger(ctx).Debugf("ClusterStoragePolicyInfo %q status unchanged, skipping status update",
			instance.Name)
	}

	r.recordEvent(ctx, instance, v1.EventTypeWarning, errMsg)
	return nil
}

// setClusterSPISuccess sets instance to success and records an event on the
// ClusterStoragePolicyInfo instance. origStatus is the pre-reconcile status snapshot;
// written only if it changed.
func (r *ReconcileClusterStoragePolicyInfo) setClusterSPISuccess(ctx context.Context,
	instance *clusterspiv1alpha1.ClusterStoragePolicyInfo,
	origStatus *clusterspiv1alpha1.ClusterStoragePolicyInfoStatus, msg string) error {
	// Clear error but preserve other status fields that were set during sync
	instance.Status.Error = ""
	if !equality.Semantic.DeepEqual(*origStatus, instance.Status) {
		if err := k8s.UpdateStatus(ctx, r.client, instance); err != nil {
			return err
		}
	} else {
		logger.GetLogger(ctx).Debugf("ClusterStoragePolicyInfo %q status unchanged, skipping status update",
			instance.Name)
	}

	r.recordEvent(ctx, instance, v1.EventTypeNormal, msg)
	return nil
}

// recordEvent records events for clusterSPI instance.
func (r *ReconcileClusterStoragePolicyInfo) recordEvent(ctx context.Context,
	instance *clusterspiv1alpha1.ClusterStoragePolicyInfo, eventtype string, msg string) {
	log := logger.GetLogger(ctx)
	log.Debugf("Event type is %s", eventtype)
	switch eventtype {
	case v1.EventTypeWarning:
		r.recorder.Event(instance, v1.EventTypeWarning, "ClusterStoragePolicyInfoFailed", msg)
	case v1.EventTypeNormal:
		r.recorder.Event(instance, v1.EventTypeNormal, "ClusterStoragePolicyInfoSynced", msg)
	}
}

// setInfraSPIError sets error and records an event on the InfraStoragePolicyInfo instance.
// origStatus is the pre-reconcile status snapshot; written only if it changed.
func (r *ReconcileClusterStoragePolicyInfo) setInfraSPIError(ctx context.Context,
	infraSPI *infraspiv1alpha1.InfraStoragePolicyInfo,
	origStatus *infraspiv1alpha1.InfraStoragePolicyInfoStatus, errMsg string) error {
	infraSPI.Status.Error = errMsg
	if !equality.Semantic.DeepEqual(*origStatus, infraSPI.Status) {
		if err := k8s.UpdateStatus(ctx, r.client, infraSPI); err != nil {
			return err
		}
	} else {
		logger.GetLogger(ctx).Debugf("InfraStoragePolicyInfo %q status unchanged, skipping status update",
			infraSPI.Name)
	}

	r.recordInfraSPIEvent(ctx, infraSPI, v1.EventTypeWarning, errMsg)
	return nil
}

// setInfraSPISuccess sets instance to success and records an event on the
// InfraStoragePolicyInfo instance. origStatus is the pre-reconcile status snapshot;
// written only if it changed.
func (r *ReconcileClusterStoragePolicyInfo) setInfraSPISuccess(ctx context.Context,
	infraSPI *infraspiv1alpha1.InfraStoragePolicyInfo,
	origStatus *infraspiv1alpha1.InfraStoragePolicyInfoStatus, msg string) error {
	// Clear error but preserve other status fields that were set during sync
	infraSPI.Status.Error = ""
	if !equality.Semantic.DeepEqual(*origStatus, infraSPI.Status) {
		if err := k8s.UpdateStatus(ctx, r.client, infraSPI); err != nil {
			return err
		}
	} else {
		logger.GetLogger(ctx).Debugf("InfraStoragePolicyInfo %q status unchanged, skipping status update",
			infraSPI.Name)
	}

	r.recordInfraSPIEvent(ctx, infraSPI, v1.EventTypeNormal, msg)
	return nil
}

// recordInfraSPIEvent records events for InfraStoragePolicyInfo instances.
func (r *ReconcileClusterStoragePolicyInfo) recordInfraSPIEvent(ctx context.Context,
	infraSPI *infraspiv1alpha1.InfraStoragePolicyInfo, eventtype string, msg string) {
	log := logger.GetLogger(ctx)
	log.Debugf("InfraSPI Event type is %s", eventtype)
	switch eventtype {
	case v1.EventTypeWarning:
		r.recorder.Event(infraSPI, v1.EventTypeWarning, "InfraStoragePolicyInfoFailed", msg)
	case v1.EventTypeNormal:
		r.recorder.Event(infraSPI, v1.EventTypeNormal, "InfraStoragePolicyInfoSynced", msg)
	}
}
