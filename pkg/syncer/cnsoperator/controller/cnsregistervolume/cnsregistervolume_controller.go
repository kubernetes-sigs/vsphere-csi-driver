/*
Copyright 2020-2025 The Kubernetes Authors.

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

package cnsregistervolume

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"sync"
	"time"

	cnstypes "github.com/vmware/govmomi/cns/types"
	v1 "k8s.io/api/core/v1"
	apiequality "k8s.io/apimachinery/pkg/api/equality"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	apitypes "k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/client-go/kubernetes/scheme"
	typedcorev1 "k8s.io/client-go/kubernetes/typed/core/v1"
	"k8s.io/client-go/tools/record"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	"sigs.k8s.io/controller-runtime/pkg/event"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	"sigs.k8s.io/controller-runtime/pkg/manager"
	"sigs.k8s.io/controller-runtime/pkg/predicate"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
	"sigs.k8s.io/controller-runtime/pkg/source"
	"sigs.k8s.io/vsphere-csi-driver/v3/pkg/common/conditions"
	cnsoperatortypes "sigs.k8s.io/vsphere-csi-driver/v3/pkg/syncer/cnsoperator/types"
	"sigs.k8s.io/vsphere-csi-driver/v3/pkg/syncer/cnsoperator/util"

	clientset "k8s.io/client-go/kubernetes"
	clientConfig "sigs.k8s.io/controller-runtime/pkg/client/config"
	apis "sigs.k8s.io/vsphere-csi-driver/v3/pkg/apis/cnsoperator"
	cnsregistervolumev1alpha1 "sigs.k8s.io/vsphere-csi-driver/v3/pkg/apis/cnsoperator/cnsregistervolume/v1alpha1"
	storagepolicyusagev1alpha3 "sigs.k8s.io/vsphere-csi-driver/v3/pkg/apis/cnsoperator/storagepolicy/v1alpha3"
	volumes "sigs.k8s.io/vsphere-csi-driver/v3/pkg/common/cns-lib/volume"
	cnsvsphere "sigs.k8s.io/vsphere-csi-driver/v3/pkg/common/cns-lib/vsphere"
	commonconfig "sigs.k8s.io/vsphere-csi-driver/v3/pkg/common/config"
	"sigs.k8s.io/vsphere-csi-driver/v3/pkg/csi/service/common"
	"sigs.k8s.io/vsphere-csi-driver/v3/pkg/csi/service/common/commonco"
	commoncotypes "sigs.k8s.io/vsphere-csi-driver/v3/pkg/csi/service/common/commonco/types"
	"sigs.k8s.io/vsphere-csi-driver/v3/pkg/csi/service/logger"
	"sigs.k8s.io/vsphere-csi-driver/v3/pkg/internalapis/cnsvolumeinfo"
	"sigs.k8s.io/vsphere-csi-driver/v3/pkg/internalapis/cnsvolumeoperationrequest"
	cnsvolumeoprequestv1alpha1 "sigs.k8s.io/vsphere-csi-driver/v3/pkg/internalapis/cnsvolumeoperationrequest/v1alpha1"
	k8s "sigs.k8s.io/vsphere-csi-driver/v3/pkg/kubernetes"
	"sigs.k8s.io/vsphere-csi-driver/v3/pkg/syncer"
)

const (
	workerThreadsEnvVar     = "WORKER_THREADS_REGISTER_VOLUME"
	defaultMaxWorkerThreads = 40
	// maxInFlightCnsTasksEnvVar caps concurrent CNS CreateVolume tasks. Before
	// the task wait was moved off the reconciler worker, the worker pool itself
	// capped this at defaultMaxWorkerThreads; this replaces that ceiling
	// explicitly so vCenter sees no more concurrency than it does today.
	maxInFlightCnsTasksEnvVar  = "MAX_INFLIGHT_CNS_TASKS_REGISTER_VOLUME"
	defaultMaxInFlightCnsTasks = 40
	staticPvNamePrefix         = "static-pv-"
	// Labels added to PVs created by CnsRegisterVolume for identification and direct lookup
	labelCnsRegisterVolumeCreatedBy      = "cns.vmware.com/created-by"
	labelCnsRegisterVolumeCreatedByValue = "cnsregistervolume"
	labelCnsRegisterVolumeCRNamespace    = "cns.vmware.com/cnsregistervolume-namespace"
	labelCnsRegisterVolumeCRName         = "cns.vmware.com/cnsregistervolume-name"
)

var (
	// backOffDuration is a map of cnsregistervolume name's to the time after which
	// a request for this instance will be requeued.
	// Initialized to 1 second for new instances and for instances whose latest
	// reconcile operation succeeded.
	// If the reconcile fails, backoff is incremented exponentially.
	backOffDuration         map[apitypes.NamespacedName]time.Duration
	backOffDurationMapMutex = sync.Mutex{}

	topologyMgr                 commoncotypes.ControllerTopologyService
	clusterComputeResourceMoIds []string
	// workloadDomainIsolationEnabled determines if the workload domain
	// isolation feature is available on a supervisor cluster.
	workloadDomainIsolationEnabled          bool
	isTKGSHAEnabled                         bool
	isMultipleClustersPerVsphereZoneEnabled bool
	isSharedDiskEnabled                     bool
	// isVSphereDPLPModernAppEnabled caches the VSphereDPLPModernApp
	// WCP capability at controller startup. Set once in Add(). The syncer's
	// InitMetadataSyncer starts the late-enablement watcher for this capability, which
	// exits the container on enablement so this value gets re-evaluated on restart.
	isVSphereDPLPModernAppEnabled bool
	// isSnapServiceLocalProtectionEnabled caches the supports_snap_service_local_protection WCP
	// capability at controller startup. Set once in Add(). The syncer restarts the container when the
	// capability is enabled later, so this value gets re-evaluated on restart.
	isSnapServiceLocalProtectionEnabled bool
)

// Add creates a new CnsRegisterVolume Controller and adds it to the Manager,
// ConfigurationInfo and VirtualCenterTypes. The Manager will set fields on
// the Controller and Start it when the Manager is Started.
func Add(mgr manager.Manager, clusterFlavor cnstypes.CnsClusterFlavor,
	configInfo *commonconfig.ConfigurationInfo, volumeManager volumes.Manager) error {
	ctx, log := logger.GetNewContextWithLogger()
	if clusterFlavor != cnstypes.CnsClusterFlavorWorkload {
		log.Debug("Not initializing the CnsRegisterVolume Controller as its a non-WCP CSI deployment")
		return nil
	}
	workloadDomainIsolationEnabled = commonco.ContainerOrchestratorUtility.IsFSSEnabled(ctx,
		common.WorkloadDomainIsolation)
	isSharedDiskEnabled = commonco.ContainerOrchestratorUtility.IsFSSEnabled(ctx,
		common.SharedDiskFss)
	isTKGSHAEnabled = commonco.ContainerOrchestratorUtility.IsFSSEnabled(ctx, common.TKGsHA)
	isMultipleClustersPerVsphereZoneEnabled = commonco.ContainerOrchestratorUtility.IsFSSEnabled(ctx,
		common.MultipleClustersPerVsphereZone)
	isVSphereDPLPModernAppEnabled = commonco.ContainerOrchestratorUtility.IsFSSEnabled(ctx,
		common.VSphereDPLPModernApp)
	isSnapServiceLocalProtectionEnabled = commonco.ContainerOrchestratorUtility.IsFSSEnabled(ctx,
		common.SupportsSnapServiceLocalProtection)

	var volumeInfoService cnsvolumeinfo.VolumeInfoService
	if commonco.ContainerOrchestratorUtility.IsFSSEnabled(ctx, common.TKGsHA) {
		var err error
		clusterComputeResourceMoIds, _, err = common.GetClusterComputeResourceMoIds(ctx)
		if err != nil {
			log.Errorf("failed to get clusterComputeResourceMoIds. err: %v", err)
			return err
		}
		if syncer.IsPodVMOnStretchSupervisorFSSEnabled {
			topologyMgr, err = commonco.ContainerOrchestratorUtility.InitTopologyServiceInController(ctx)
			if err != nil {
				log.Errorf("failed to init topology manager. err: %v", err)
				return err
			}
			log.Info("Creating CnsVolumeInfo Service to persist mapping for VolumeID to storage policy info")
			volumeInfoService, err = cnsvolumeinfo.InitVolumeInfoService(ctx)
			if err != nil {
				return logger.LogNewErrorf(log, "error initializing volumeInfoService. Error: %+v", err)
			}
			log.Infof("Successfully initialized VolumeInfoService")
		} else {
			if len(clusterComputeResourceMoIds) > 1 {
				log.Infof("Not initializing the CnsRegisterVolume Controller as stretched supervisor is detected.")
				return nil
			}
		}
	}
	if isMultipleClustersPerVsphereZoneEnabled {
		err := commonco.ContainerOrchestratorUtility.StartZonesInformer(ctx, nil, metav1.NamespaceAll)
		if err != nil {
			return logger.LogNewErrorf(log, "failed to start zone informer. Error: %v", err)
		}
	}
	// Initializes kubernetes client.
	k8sclient, err := k8s.NewClient(ctx)
	if err != nil {
		log.Errorf("Creating Kubernetes client failed. Err: %v", err)
		return err
	}

	// eventBroadcaster broadcasts events on cnsregistervolume instances to the
	// event sink.
	eventBroadcaster := record.NewBroadcaster()
	eventBroadcaster.StartRecordingToSink(
		&typedcorev1.EventSinkImpl{
			Interface: k8sclient.CoreV1().Events(""),
		},
	)
	recorder := eventBroadcaster.NewRecorder(scheme.Scheme, v1.EventSource{Component: apis.GroupName})
	reconciler, err := newReconciler(mgr, configInfo, volumeManager, recorder, volumeInfoService)
	if err != nil {
		log.Errorf("Failed to create reconciler. Err: %v", err)
		return err
	}
	return add(mgr, reconciler)
}

// newReconciler returns a new reconcile.Reconciler.
func newReconciler(mgr manager.Manager, configInfo *commonconfig.ConfigurationInfo,
	volumeManager volumes.Manager, recorder record.EventRecorder,
	volumeInfoService cnsvolumeinfo.VolumeInfoService) (reconcile.Reconciler, error) {
	ctx, log := logger.GetNewContextWithLogger()
	k8sclient, err := k8s.NewClient(ctx)
	if err != nil {
		log.Errorf("Failed to create k8s client. Err: %v", err)
		return nil, err
	}
	maxInFlight := util.GetMaxWorkerThreads(ctx, maxInFlightCnsTasksEnvVar, defaultMaxInFlightCnsTasks)
	// Buffered so the tracker's goroutines never block delivering a wake-up.
	taskEvents := make(chan event.GenericEvent, maxInFlight*2)
	return &ReconcileCnsRegisterVolume{client: mgr.GetClient(), apiReader: mgr.GetAPIReader(), scheme: mgr.GetScheme(),
		configInfo: configInfo, volumeManager: volumeManager, recorder: recorder,
		volumeInfoService: volumeInfoService, k8sclient: k8sclient,
		// The listview and vCenter are bound on first use, once a reconcile has
		// a live VC connection.
		taskTracker: newCnsTaskTracker(nil, nil, taskEvents, maxInFlight),
		taskEvents:  taskEvents}, nil
}

// add adds a new Controller to mgr with r as the reconcile.Reconciler.
func add(mgr manager.Manager, r reconcile.Reconciler) error {
	ctx, log := logger.GetNewContextWithLogger()

	maxWorkerThreads := util.GetMaxWorkerThreads(ctx,
		workerThreadsEnvVar, defaultMaxWorkerThreads)
	// Create a new controller.
	builder := ctrl.NewControllerManagedBy(mgr).Named("cnsregistervolume-controller").
		For(&cnsregistervolumev1alpha1.CnsRegisterVolume{}).
		WithEventFilter(predicate.GenerationChangedPredicate{}).
		WithOptions(controller.Options{MaxConcurrentReconciles: maxWorkerThreads})

	// Wake a CR as soon as its CNS task completes, instead of waiting for the
	// safety requeue. GenerationChangedPredicate overrides only Update, so
	// Generic events pass through this filter untouched.
	if reconciler, ok := r.(*ReconcileCnsRegisterVolume); ok && reconciler.taskEvents != nil {
		builder = builder.WatchesRawSource(
			source.Channel(reconciler.taskEvents, &handler.EnqueueRequestForObject{}))
	}

	err := builder.Complete(r)
	if err != nil {
		log.Errorf("Failed to build application controller. Err: %v", err)
		return err
	}

	backOffDuration = make(map[apitypes.NamespacedName]time.Duration)
	return nil
}

// blank assignment to verify that ReconcileCnsRegisterVolume implements
// reconcile.Reconciler.
var _ reconcile.Reconciler = &ReconcileCnsRegisterVolume{}

// ReconcileCnsRegisterVolume reconciles a CnsRegisterVolume object.
type ReconcileCnsRegisterVolume struct {
	// This client, initialized using mgr.Client() above, is a split client
	// that reads objects from the cache and writes to the apiserver.
	client            client.Client
	apiReader         client.Reader // uncached reads for task state and restore reservations
	scheme            *runtime.Scheme
	configInfo        *commonconfig.ConfigurationInfo
	volumeManager     volumes.Manager
	recorder          record.EventRecorder
	volumeInfoService cnsvolumeinfo.VolumeInfoService
	k8sclient         clientset.Interface
	// taskTracker watches in-flight CNS CreateVolume tasks so that no
	// reconciler worker has to block on one.
	taskTracker *cnsTaskTracker
	// taskEvents carries wake-ups from the tracker back into the controller's
	// work queue; it is registered as a source.Channel watch in add().
	taskEvents chan event.GenericEvent
}

// instanceKey is the work-queue identity of a CnsRegisterVolume.
func instanceKey(instance *cnsregistervolumev1alpha1.CnsRegisterVolume) apitypes.NamespacedName {
	return apitypes.NamespacedName{Name: instance.Name, Namespace: instance.Namespace}
}

// Reconcile reads that state of the cluster for a CnsRegisterVolume object
// and makes changes based on the state read and what is in the
// CnsRegisterVolume.Spec.
// Note:
// The Controller will requeue the Request to be processed again if the
// returned error is non-nil or Result.Requeue is true. Otherwise, upon
// completion it will remove the work from the queue.
func (r *ReconcileCnsRegisterVolume) Reconcile(ctx context.Context,
	request reconcile.Request) (reconcile.Result, error) {
	ctx = logger.NewContextWithLogger(ctx)
	log := logger.GetLogger(ctx)
	// Fetch the CnsRegisterVolume instance.
	instance := &cnsregistervolumev1alpha1.CnsRegisterVolume{}
	err := r.executionReader().Get(ctx, request.NamespacedName, instance)
	if err != nil {
		if apierrors.IsNotFound(err) {
			log.With("name", request.Name).With("namespace", request.Namespace).
				Info("CnsRegisterVolume resource not found. Ignoring since object must be deleted.")
			return reconcile.Result{}, nil
		}
		log.Errorf("Error reading the CnsRegisterVolume with name: %q on namespace: %q. Err: %+v",
			request.Name, request.Namespace, err)
		// Error reading the object - return with err.
		return reconcile.Result{}, err
	}
	// Initialize backOffDuration for the instance, if required.
	backOffDurationMapMutex.Lock()
	var timeout time.Duration
	if _, exists := backOffDuration[request.NamespacedName]; !exists {
		backOffDuration[request.NamespacedName] = time.Second
	}
	timeout = backOffDuration[request.NamespacedName]
	backOffDurationMapMutex.Unlock()

	// Handle deletion of the instance.
	// If the CR is being deleted, we need to clean up any resources that were created
	// and ensure no PV is left behind before removing the finalizer.
	if instance.DeletionTimestamp != nil {
		log.With("name", instance.Name).With("namespace", instance.Namespace).
			Info("CnsRegisterVolume instance is marked for deletion")
		return r.reconcileDelete(ctx, instance, request, timeout)
	}

	// If the CnsRegisterVolume instance is already registered, remove the
	// instance from the queue.
	if instance.Status.Registered {
		backOffDurationMapMutex.Lock()
		delete(backOffDuration, request.NamespacedName)
		backOffDurationMapMutex.Unlock()
		return reconcile.Result{}, nil
	}

	// A previously submitted task must settle even if admission prerequisites
	// have since changed. The policy checks below still gate PV/PVC creation.
	if instance.Status.CnsTaskID == "" && instance.Status.CnsVolumeID == "" && r.taskTracker != nil {
		r.taskTracker.mu.Lock()
		pending := r.taskTracker.slots[instanceKey(instance)]
		r.taskTracker.mu.Unlock()
		if pending != "" {
			if err := setInstanceCnsTaskID(ctx, r, instance, pending); err != nil {
				return reconcile.Result{RequeueAfter: timeout}, nil
			}
		}
	}
	if instance.Status.CnsTaskID != "" {
		vc, err := taskVirtualCenterFn(r, ctx)
		if err != nil {
			return reconcile.Result{RequeueAfter: timeout}, nil
		}
		if err := r.taskTracker.ensureInit(ctx, r.volumeManager, vc); err != nil {
			return reconcile.Result{RequeueAfter: timeout}, nil
		}
		name := staticPvNamePrefix + instance.Spec.VolumeID
		if instance.Spec.VolumeID == "" {
			name = staticPvNamePrefix + string(instance.UID)
		}
		_, retry, err := r.pollCreateVolume(ctx, instance, vc, &cnstypes.CnsVolumeCreateSpec{Name: name}, timeout)
		if err != nil {
			return reconcile.Result{}, err
		}
		if retry != nil {
			return *retry, nil
		}
	}

	log.Infof("Reconciling CnsRegisterVolume with instance: %q from namespace: %q. timeout %q seconds",
		instance.Name, request.Namespace, timeout)

	// Restores of one volume run one at a time, and only the first CnsRegisterVolume to stamp the
	// volume restores it. This is decided before the finalizer is added, so that a CnsRegisterVolume
	// that is not processed is not protected again on every resync.
	isRestore := r.isSnapServiceRestore(ctx, instance)
	restoreStamped := false
	if isRestore {
		unlock, stamped, result, proceed := r.startRestore(ctx, instance, request, timeout)
		if !proceed {
			return result, nil
		}
		defer unlock()
		restoreStamped = stamped
	}

	// Protect the instance with a finalizer before performing any operation.
	err = protectInstance(ctx, r.client, instance)
	if err != nil {
		log.Errorf("Failed to add finalizer to CnsRegisterVolume instance: %q. Err: %+v", instance.Name, err)
		setInstanceError(ctx, r, instance, "Failed to add finalizer")
		return reconcile.Result{RequeueAfter: timeout}, nil
	}

	// Validate CnsRegisterVolume spec to check for valid entries.
	err = validateCnsRegisterVolumeSpec(ctx, instance)
	if err != nil {
		log.Errorf(err.Error())
		setInstanceError(ctx, r, instance, err.Error(),
			falseCondition(cnsregistervolumev1alpha1.ConditionSpecValidated,
				cnsregistervolumev1alpha1.ReasonInvalidSpec, "CnsRegisterVolume spec is not valid"))
		return reconcile.Result{RequeueAfter: timeout}, nil
	}

	// If a StorageClassName is specified, verify up front that its storage policy is assigned
	// to this instance's namespace, before any CNS volume is created or modified. Without this,
	// a namespace could get a volume registered with a storage policy it isn't entitled to.
	if instance.Spec.StorageClassName != "" {
		policyID, err := getStoragePolicyIDForStorageClass(ctx, r.k8sclient, instance.Spec.StorageClassName)
		if err != nil {
			log.Error(err.Error())
			setInstanceError(ctx, r, instance, err.Error(),
				falseCondition(cnsregistervolumev1alpha1.ConditionSpecValidated,
					cnsregistervolumev1alpha1.ReasonStoragePolicyLookupFailed,
					"Could not resolve the storage policy for the StorageClass"))
			return reconcile.Result{RequeueAfter: timeout}, nil
		}
		assigned, err := isStoragePolicyAssignedToNamespace(ctx, r.k8sclient, r.client, policyID,
			instance.Spec.StorageClassName, instance.Namespace, syncer.IsPodVMOnStretchSupervisorFSSEnabled)
		if err != nil {
			msg := fmt.Sprintf("Failed to verify StorageClass %q is assigned to namespace %q: %+v",
				instance.Spec.StorageClassName, instance.Namespace, err)
			log.Error(msg)
			setInstanceError(ctx, r, instance, msg,
				falseCondition(cnsregistervolumev1alpha1.ConditionSpecValidated,
					cnsregistervolumev1alpha1.ReasonStoragePolicyLookupFailed,
					"Could not verify the storage policy is assigned to this namespace"))
			return reconcile.Result{RequeueAfter: timeout}, nil
		}
		if !assigned {
			msg := fmt.Sprintf("StorageClass %q's storage policy is not assigned to namespace %q",
				instance.Spec.StorageClassName, instance.Namespace)
			log.Error(msg)
			setInstanceError(ctx, r, instance, msg,
				falseCondition(cnsregistervolumev1alpha1.ConditionSpecValidated,
					cnsregistervolumev1alpha1.ReasonStoragePolicyNotAssignedToNamespace,
					"The namespace is not entitled to the requested storage policy"))
			return reconcile.Result{RequeueAfter: timeout}, nil
		}
	}

	// A SnapService restore requires a deleted volume with a CnsDeletedVolumeInfo. Check it before any
	// CNS volume, PV or PVC is created, so that a broken restore fails visibly on this instance and
	// passes on a later retry once the CnsDeletedVolumeInfo exists. Once an earlier attempt stamped the
	// PV or the CnsVolumeInfo, the restore is underway and the check is skipped: the
	// CnsDeletedVolumeInfo may be gone by then, and the handover copes without it.
	if isRestore && restoreStamped {
		log.Infof("CnsRegisterVolume %s/%s already stamped volume %q; skipping the restore precheck",
			instance.Namespace, instance.Name, instance.Spec.VolumeID)
	}
	if isRestore && !restoreStamped && instance.Status.CnsTaskID == "" {
		err = r.volumeInfoService.PrecheckRestore(ctx, instance.Spec.VolumeID, instance.Namespace,
			string(instance.UID))
		if err != nil {
			log.Errorf("Restore precheck failed for CnsRegisterVolume %s/%s, will retry: %v",
				instance.Namespace, instance.Name, err)
			setInstanceError(ctx, r, instance, err.Error(), falseCondition(
				cnsregistervolumev1alpha1.ConditionRestoreValidated,
				cnsregistervolumev1alpha1.ReasonRestorePrecheckFailed, "Waiting for restore prerequisites"))
			return reconcile.Result{RequeueAfter: timeout}, nil
		}
	}

	restoreReason := cnsregistervolumev1alpha1.ReasonNotRequired
	if isRestore {
		restoreReason = cnsregistervolumev1alpha1.ReasonValidated
	}
	setInstanceConditions(ctx, r, instance, trueCondition(
		cnsregistervolumev1alpha1.ConditionRestoreValidated, restoreReason, "Restore prerequisites satisfied"))

	// Verify if CnsRegisterVolume request is for block volume registration
	// Currently file volume registration is not supported.
	ok := isBlockVolumeRegisterRequest(ctx, instance)
	if !ok {
		// File volumes are not supported, so error out.
		msg := fmt.Sprintf("AccessMode: %s is not supported with volumemode %s",
			instance.Spec.AccessMode, instance.Spec.VolumeMode)
		log.Error(msg)
		setInstanceError(ctx, r, instance, msg,
			falseCondition(cnsregistervolumev1alpha1.ConditionSpecValidated,
				cnsregistervolumev1alpha1.ReasonInvalidSpec,
				"File volume registration is not supported"))
		return reconcile.Result{RequeueAfter: timeout}, nil
	}

	vcconfig, err := cnsvsphere.GetVirtualCenterConfig(ctx, r.configInfo.GetCfg())
	var vc *cnsvsphere.VirtualCenter
	if err == nil {
		vc, err = cnsvsphere.GetVirtualCenterInstanceForVCenterConfig(ctx, vcconfig, false)
	}
	if err != nil {
		msg := fmt.Sprintf("Failed to get virtual center instance with error: %+v", err)
		log.Error(msg)
		setInstanceError(ctx, r, instance, "Unable to connect to VC for volume registration")
		return reconcile.Result{RequeueAfter: timeout}, nil
	}
	var (
		volumeID       string
		pvName         string
		pvNodeAffinity *v1.VolumeNodeAffinity
	)
	// Create Volume for the input CnsRegisterVolume instance.
	createSpec, err := constructCreateSpecForInstance(ctx, r, instance, vc.Config.Host, isTKGSHAEnabled)
	if err != nil {
		msg := fmt.Sprintf("Failed to construct CNS create spec: %+v", err)
		log.Error(msg)
		setInstanceError(ctx, r, instance, msg)
		return reconcile.Result{RequeueAfter: timeout}, nil
	}
	// Every pre-CNS check has passed. Published before the first CNS call so a
	// CR waiting on a task slot is distinguishable from one still being validated.
	setInstanceConditions(ctx, r, instance, trueCondition(cnsregistervolumev1alpha1.ConditionSpecValidated,
		cnsregistervolumev1alpha1.ReasonValidated, "CnsRegisterVolume spec is valid"))

	log.Infof("Creating CNS volume: %+v for CnsRegisterVolume request with name: %q on namespace: %q",
		instance, instance.Name, instance.Namespace)
	// Submit the CNS task and return, or pick up the result of a task submitted
	// by an earlier pass. Either way this does not block on CNS, so the worker
	// is free to start another CnsRegisterVolume while this one's task runs.
	volInfo, taskResult, err := r.submitOrPollCreateVolume(ctx, instance, vc, createSpec, timeout)
	if taskResult != nil {
		return *taskResult, nil
	}
	if err != nil {
		msg := fmt.Sprintf("failed to create CNS volume. Error: %v", err)
		log.Errorf(msg)
		setInstanceError(ctx, r, instance, msg)
		return reconcile.Result{RequeueAfter: timeout}, nil
	}

	if instance.Spec.DiskURLPath != "" {
		// if CNS Register Volume Instance is created with diskURLPath,
		// confirm using CNS Volume ID, if another PV is already present with same volume ID
		// If yes then fail.
		pvName, found := commonco.ContainerOrchestratorUtility.GetPVNameFromCSIVolumeID(volInfo.VolumeID.Id)
		if found {
			if pvName != staticPvNamePrefix+volInfo.VolumeID.Id {
				msg := fmt.Sprintf("PV: %q with the volume ID: %q for volume path: %q"+
					"is already present. Can not create multiple PV with same disk.", pvName, volInfo.VolumeID.Id,
					instance.Spec.DiskURLPath)
				log.Errorf(msg)
				setInstanceError(ctx, r, instance, msg)
				return reconcile.Result{RequeueAfter: timeout}, nil
			}
		}
	}

	volumeID = volInfo.VolumeID.Id
	log.Infof("Created CNS volume with volumeID: %s", volumeID)
	// The CNS volume exists from here on; the remaining work is Kubernetes-side.
	setInstanceConditions(ctx, r, instance, trueCondition(cnsregistervolumev1alpha1.ConditionCnsVolumeRegistered,
		cnsregistervolumev1alpha1.ReasonRegistered, "CNS has registered the disk"))
	r.clearStaleDeleteOperationRecord(ctx, volumeID)

	pvName = staticPvNamePrefix + volumeID
	// Query volume
	log.Infof("Querying volume: %s for CnsRegisterVolume request with name: %q on namespace: %q",
		volumeID, instance.Name, instance.Namespace)
	querySelection := cnstypes.CnsQuerySelection{
		Names: []string{
			string(cnstypes.QuerySelectionNameTypeVolumeType),
			string(cnstypes.QuerySelectionNameTypeDataStoreUrl),
			string(cnstypes.QuerySelectionNameTypePolicyId),
			string(cnstypes.QuerySelectionNameTypeBackingObjectDetails),
		},
	}
	volume, err := common.QueryVolumeByID(ctx, r.volumeManager, volumeID, &querySelection)
	if err != nil {
		if err.Error() == common.ErrNotFound.Error() {
			// Cleanup may have completed before a crash persisted its result.
			// Only an authoritative NotFound clears the checkpoint; transport
			// failures keep it and cannot authorize a new registration task.
			if err := r.persistCnsExecution(ctx, instance, "", ""); err != nil {
				return reconcile.Result{RequeueAfter: timeout}, nil
			}
			msg := fmt.Sprintf("CNS Volume: %s not found", volumeID)
			log.Error(msg)
			setInstanceError(ctx, r, instance, msg)
			return reconcile.Result{RequeueAfter: timeout}, nil
		}
		msg := fmt.Sprintf("Failed to query CNS volume: %s with error: %+v", volumeID, err)
		log.Error(msg)
		setInstanceError(ctx, r, instance, msg)
		return reconcile.Result{RequeueAfter: timeout}, nil
	}

	// Detect whether the volume uses a vSAN host-local storage policy. This is gated on the
	// supports_host_local_storage capability; re-checking IsFSSEnabled here (rather than caching it
	// at startup) picks up late enablement of the capability on the running syncer. A host-local
	// volume lives on the local storage of a single ESX host, so the regular "accessible to all
	// hosts" gate and the intersection-based topology computation are replaced with a node-driven
	// resolution below. When the capability is off or the policy is not host-local, isHostLocal
	// stays false and the existing behavior is preserved.
	isHostLocal := false
	if commonco.ContainerOrchestratorUtility.IsFSSEnabled(ctx, common.HostLocalStorageSupport) {
		isHostLocal, err = vc.IsHostLocalStoragePolicy(ctx, volume.StoragePolicyId)
		if err != nil {
			msg := fmt.Sprintf("failed to determine host-local status for storage policy %s: %+v",
				volume.StoragePolicyId, err)
			log.Error(msg)
			setInstanceError(ctx, r, instance, msg,
				falseCondition(cnsregistervolumev1alpha1.ConditionVolumeAccessible,
					cnsregistervolumev1alpha1.ReasonHostLocalCheckFailed,
					"Could not determine whether the storage policy is host-local"))
			return reconcile.Result{RequeueAfter: timeout}, nil
		}
	}

	// datastoreAccessibleTopology holds the PV/PVC topology segments. For host-local volumes it is
	// populated by the node-driven resolver below; otherwise it is computed later from shared
	// datastores in the IsPodVMOnStretchSupervisorFSSEnabled block.
	var datastoreAccessibleTopology []map[string]string

	if isHostLocal {
		// Bypass the shared-datastore "accessible to all hosts" gate. Build the clusterMoID -> zone
		// map (inverted AvailabilityZone map) used both to discover the candidate clusters that may
		// hold the datastore and to derive the zone from the host's cluster.
		var clusterToZone map[string]string
		if topologyMgr != nil {
			clusterToZone = buildClusterToZoneMap(topologyMgr.GetAZClustersMap(ctx))
		}
		candidateClusterIDs := make([]string, 0, len(clusterToZone))
		for clusterID := range clusterToZone {
			candidateClusterIDs = append(candidateClusterIDs, clusterID)
		}
		if len(candidateClusterIDs) == 0 {
			candidateClusterIDs = clusterComputeResourceMoIds
		}

		// Resolve the datastore object the disk lives on.
		dsInfo, dsErr := cnsvsphere.GetDatastoreInfoByURL(ctx, vc, candidateClusterIDs, volume.DatastoreUrl)
		if dsErr != nil || dsInfo == nil {
			log.Errorf("failed to resolve datastore %s for host-local volume %s: %+v",
				volume.DatastoreUrl, volumeID, dsErr)
			return r.failVolumeNotAccessible(ctx, instance, volumeID,
				cnsregistervolumev1alpha1.ReasonDatastoreNotFound,
				"host-local volume in the spec is on a datastore not found in the namespace's clusters",
				"The host-local volume's datastore is not present in the namespace's clusters", timeout), nil
		}

		// Requirement: the host-local policy must be PBM-compatible with the datastore the disk
		// lives on. Reject registration if it is not.
		compatible, compatErr := checkDatastorePolicyCompatibilityFn(ctx, vc, dsInfo, volume.StoragePolicyId)
		if compatErr != nil {
			// Transient PBM/VC error: requeue without cleanup.
			setInstanceError(ctx, r, instance,
				fmt.Sprintf("failed to verify PBM compatibility for host-local volume %s: %+v", volumeID, compatErr),
				falseCondition(cnsregistervolumev1alpha1.ConditionVolumeAccessible,
					cnsregistervolumev1alpha1.ReasonHostLocalCheckFailed,
					"Could not verify the host-local storage policy against the datastore"))
			return reconcile.Result{RequeueAfter: timeout}, nil
		}
		if !compatible {
			log.Errorf("host-local policy %s is not PBM-compatible with datastore %s for volume %s",
				volume.StoragePolicyId, volume.DatastoreUrl, volumeID)
			return r.failVolumeNotAccessible(ctx, instance, volumeID,
				cnsregistervolumev1alpha1.ReasonHostLocalPolicyIncompatible,
				"host-local storage policy is not compatible with the datastore the disk lives on",
				"The host-local storage policy is not compatible with the datastore the disk lives on", timeout), nil
		}

		// Derive the PV topology from Datastore.host[0] -> {zone (from host's cluster), hostname}.
		datastoreAccessibleTopology, err = getHostLocalAccessibleTopologyFn(ctx, vc, dsInfo, clusterToZone)
		if err != nil {
			log.Errorf("failed to resolve host-local topology for volume %s on datastore %s: %+v",
				volumeID, volume.DatastoreUrl, err)
			return r.failVolumeNotAccessible(ctx, instance, volumeID,
				cnsregistervolumev1alpha1.ReasonTopologyUnresolved,
				fmt.Sprintf("failed to resolve host-local topology for volume %s: %+v", volumeID, err),
				"The accessible topology for the host-local volume could not be resolved", timeout), nil
		}
	} else if syncer.IsPodVMOnStretchSupervisorFSSEnabled {
		if workloadDomainIsolationEnabled || len(clusterComputeResourceMoIds) > 1 {
			if isMultipleClustersPerVsphereZoneEnabled {
				// Get zones assigned to the namespace
				zoneMaps := commonco.ContainerOrchestratorUtility.GetZonesForNamespace(request.Namespace)

				// Convert map keys to slice
				zones := make([]string, 0, len(zoneMaps))
				for zone := range zoneMaps {
					zones = append(zones, zone)
				}
				// Get active clusters in the requested zones
				activeClusters, err := commonco.ContainerOrchestratorUtility.
					GetActiveClustersForNamespaceInRequestedZones(ctx, request.Namespace, zones)
				if err != nil {
					msg := fmt.Sprintf("Failed to get active clusters for CnsRegisterVolume request. error: %+v", err)
					log.Error(msg)
					setInstanceError(ctx, r, instance, msg)
					return reconcile.Result{RequeueAfter: timeout}, nil
				}
				// Check if volume is accessible in any of the active clusters
				if !isDatastoreAccessibleToAZClusters(ctx, vc,
					map[string][]string{"dummy-zone-key": activeClusters}, volume.DatastoreUrl) {
					log.Errorf("Volume: %s present on datastore: %s is not accessible to any of the active "+
						"cluster on the namespace: %v", volumeID, volume.DatastoreUrl, activeClusters)
					return r.failVolumeNotAccessible(ctx, instance, volumeID,
						cnsregistervolumev1alpha1.ReasonDatastoreNotAccessibleToAZClusters,
						"Volume in the spec is not accessible to any of "+
							"the active cluster on the namespace",
						"The volume's datastore is not accessible to any active AZ cluster",
						timeout), nil
				}
			} else {
				azClustersMap := topologyMgr.GetAZClustersMap(ctx)
				isAccessible := isDatastoreAccessibleToAZClusters(ctx, vc, azClustersMap, volume.DatastoreUrl)
				if !isAccessible {
					log.Errorf("Volume: %s present on datastore: %s is not accessible to any of the AZ clusters: %v",
						volumeID, volume.DatastoreUrl, azClustersMap)
					return r.failVolumeNotAccessible(ctx, instance, volumeID,
						cnsregistervolumev1alpha1.ReasonDatastoreNotAccessibleToAZClusters,
						"Volume in the spec is not accessible to any of the AZ clusters",
						"The volume's datastore is not accessible to any active AZ cluster", timeout), nil
				}
			}
		}
	} else if !isDatastoreAccessibleToCluster(
		ctx, vc, r.configInfo.GetCfg().Global.ClusterID, volume.DatastoreUrl) {
		// Verify if the volume is accessible to Supervisor cluster.
		log.Errorf("Volume: %s present on datastore: %s is not accessible to all nodes in the cluster: %s",
			volumeID, volume.DatastoreUrl, r.configInfo.GetCfg().Global.ClusterID)
		return r.failVolumeNotAccessible(ctx, instance, volumeID,
			cnsregistervolumev1alpha1.ReasonDatastoreNotAccessibleToCluster,
			"Volume in the spec is not accessible to all nodes in the cluster",
			"The volume's datastore is not accessible to all nodes in the cluster", timeout), nil
	}
	// Verify if storage policy is empty.
	if volume.StoragePolicyId == "" {
		log.Errorf("Volume: %s doesn't have storage policy associated with it", volumeID)
		setInstanceError(ctx, r, instance,
			"Volume in the spec doesn't have storage policy associated with it",
			falseCondition(cnsregistervolumev1alpha1.ConditionVolumeAccessible,
				cnsregistervolumev1alpha1.ReasonStoragePolicyMissing,
				"The registered volume has no storage policy associated with it"))
		if err = r.cleanupCNSVolume(ctx, instance, volumeID); err != nil {
			log.Errorf("Failed to cleanup CNS volume: %s with error: %+v", volumeID, err)
		}
		return reconcile.Result{RequeueAfter: timeout}, nil
	}

	// Every check against the registered volume has passed, so the volume is
	// usable from this cluster and the Kubernetes-side work can begin.
	setInstanceConditions(ctx, r, instance,
		trueCondition(cnsregistervolumev1alpha1.ConditionVolumeAccessible, cnsregistervolumev1alpha1.ReasonAccessible,
			"The registered volume is accessible from this cluster"),
		falseCondition(cnsregistervolumev1alpha1.ConditionPvcBound, cnsregistervolumev1alpha1.ReasonConfiguringK8sResources,
			"Creating the PV and PVC for the registered volume"))

	// Use cached K8s client for registration operations.
	k8sclient := r.k8sclient

	// Check if PVC already exists and has valid DataSourceRef. Fetch this before resolving
	// storageClassName below: when the PVC already declares a StorageClass, that's the ground
	// truth of what the caller actually asked for, and is preferred over independently
	// re-deriving a name from the volume's storage policy ID -- which is ambiguous whenever more
	// than one StorageClass shares the same storagePolicyID (e.g. after a policy rename leaves an
	// old and a new StorageClass both pointing at the same policy ID).
	// Do this check before creating a PV. Otherwise, PVC will be bound to PV after PV
	// is created even if validation fails.
	pvc, err := checkExistingPVCDataSourceRef(ctx, k8sclient, instance.Spec.PvcName, instance.Namespace)
	if err != nil {
		log.Errorf("Failed to check existing PVC %s/%s with DataSourceRef: %+v", instance.Namespace,
			instance.Spec.PvcName, err)
		setInstanceError(ctx, r, instance, fmt.Sprintf("Failed to check existing PVC %s/%s with DataSourceRef: %+v",
			instance.Namespace, instance.Spec.PvcName, err))
		return reconcile.Result{RequeueAfter: timeout}, nil
	}

	storageClassName, err := resolveStorageClassNameForRegistration(ctx, k8sclient, r.client, instance, pvc,
		volume.StoragePolicyId, request.Namespace, syncer.IsPodVMOnStretchSupervisorFSSEnabled)
	if err != nil {
		msg := fmt.Sprintf("Failed to resolve K8S Storageclass for storagepolicyId: %s in namespace: %s. Error: %+v",
			volume.StoragePolicyId, request.Namespace, err)
		log.Error(msg)
		setInstanceError(ctx, r, instance, msg)
		return reconcile.Result{RequeueAfter: timeout}, nil
	}
	log.Infof("Volume with storagepolicyId: %s is mapping to K8S storage class: %s and assigned to namespace: %s",
		volume.StoragePolicyId, storageClassName, request.Namespace)

	sc, err := k8sclient.StorageV1().StorageClasses().Get(ctx, storageClassName, metav1.GetOptions{})
	if err != nil {
		msg := fmt.Sprintf("Failed to fetch StorageClass: %q with error: %+v", storageClassName, err)
		log.Error(msg)
		setInstanceError(ctx, r, instance, msg)
		return reconcile.Result{RequeueAfter: timeout}, nil
	}

	// Calculate accessible topology for the provisioned volume. datastoreAccessibleTopology is
	// declared earlier and, for host-local volumes, is already populated by the node-driven
	// resolver, so build the PV node affinity from it directly (covering all configurations,
	// including non-stretched supervisors). Otherwise fall back to the shared-datastore topology.
	if isHostLocal {
		pvNodeAffinity = buildNodeAffinityFromSegments(datastoreAccessibleTopology)
	} else if syncer.IsPodVMOnStretchSupervisorFSSEnabled {
		if workloadDomainIsolationEnabled {
			datastoreAccessibleTopology, err = topologyMgr.GetTopologyInfoFromNodes(ctx,
				commoncotypes.WCPRetrieveTopologyInfoParams{
					DatastoreURL:        volume.DatastoreUrl,
					StorageTopologyType: sc.Parameters["StorageTopologyType"],
					TopologyRequirement: nil,
					Vc:                  vc})
		} else if len(clusterComputeResourceMoIds) > 1 {
			// Fetch datastoreAccessibleTopology for the volume in the stretched supervisor cluster
			datastoreAccessibleTopology, err = topologyMgr.GetTopologyInfoFromNodes(ctx,
				commoncotypes.WCPRetrieveTopologyInfoParams{
					DatastoreURL:        volume.DatastoreUrl,
					StorageTopologyType: "zonal",
					TopologyRequirement: nil,
					Vc:                  vc})
		}
		if err != nil {
			msg := fmt.Sprintf("failed to find volume topology. Error: %v", err)
			log.Error(msg)
			setInstanceError(ctx, r, instance, msg)
			return reconcile.Result{RequeueAfter: timeout}, nil
		}

		// Create node affinity terms from datastoreAccessibleTopology.
		var terms []v1.NodeSelectorTerm
		if workloadDomainIsolationEnabled {
			for _, topologyTerms := range datastoreAccessibleTopology {
				var expressions []v1.NodeSelectorRequirement
				for key, value := range topologyTerms {
					expressions = append(expressions, v1.NodeSelectorRequirement{
						Key:      key,
						Operator: v1.NodeSelectorOpIn,
						Values:   []string{value},
					})
				}
				terms = append(terms, v1.NodeSelectorTerm{
					MatchExpressions: expressions,
				})
			}
			pvNodeAffinity = &v1.VolumeNodeAffinity{
				Required: &v1.NodeSelector{
					NodeSelectorTerms: terms,
				},
			}
		} else if len(clusterComputeResourceMoIds) > 1 && len(datastoreAccessibleTopology) == 1 {
			// This is the case when workloadDomainIsolation FSS is disabled and
			// Supervisor is stretched cluster and we have datastoreAccessibleTopology for the Volume.
			matchExpressions := make([]v1.NodeSelectorRequirement, 0)
			for key, value := range datastoreAccessibleTopology[0] {
				matchExpressions = append(matchExpressions, v1.NodeSelectorRequirement{
					Key:      key,
					Operator: v1.NodeSelectorOpIn,
					Values:   []string{value},
				})
			}
			terms = append(terms, v1.NodeSelectorTerm{
				MatchExpressions: matchExpressions,
			})
			pvNodeAffinity = &v1.VolumeNodeAffinity{
				Required: &v1.NodeSelector{
					NodeSelectorTerms: terms,
				},
			}
		}
	}

	// pvc was already fetched above (before storageClassName resolution).

	// If existing PVC has DataSourceRef and volumeMode set, handle volumeMode validation and inheritance
	volumeModeInherited := false
	if pvc != nil && pvc.Spec.DataSourceRef != nil && pvc.Spec.VolumeMode != nil {
		if instance.Spec.VolumeMode == "" {
			// Inherit the volumeMode from the existing PVC
			log.Infof("Existing PVC %s in namespace %s has DataSourceRef and volumeMode set to %s. "+
				"CnsRegisterVolume does not have volumeMode set. Inheriting volumeMode from existing PVC.",
				pvc.Name, pvc.Namespace, *pvc.Spec.VolumeMode)
			instance.Spec.VolumeMode = *pvc.Spec.VolumeMode
			volumeModeInherited = true
		} else if instance.Spec.VolumeMode != *pvc.Spec.VolumeMode {
			// Both are set but don't match - this is an error
			msg := fmt.Sprintf("VolumeMode mismatch: existing PVC %s in namespace %s has volumeMode %s, "+
				"but CnsRegisterVolume specifies volumeMode %s",
				pvc.Name, pvc.Namespace, *pvc.Spec.VolumeMode, instance.Spec.VolumeMode)
			log.Error(msg)
			setInstanceError(ctx, r, instance, msg)
			return reconcile.Result{RequeueAfter: timeout}, nil
		}
	}

	// Persist the inherited volumeMode to the CnsRegisterVolume CR
	if volumeModeInherited {
		err = updateCnsRegisterVolume(ctx, r.client, instance)
		if err != nil {
			msg := fmt.Sprintf("Failed to update CnsRegisterVolume with inherited volumeMode. Error: %+v", err)
			log.Error(msg)
			setInstanceError(ctx, r, instance, msg)
			return reconcile.Result{RequeueAfter: timeout}, nil
		}
		log.Infof("Successfully updated CnsRegisterVolume %s with inherited volumeMode: %s",
			instance.Name, instance.Spec.VolumeMode)
	}

	// Do this check before creating a PV. Otherwise, PVC will be bound to PV after PV
	// is created even if validation fails
	if pvc != nil {
		log.Infof("PVC: %s already exists. Validate if there is topology annotation on PVC",
			instance.Spec.PvcName)

		// Validate that existing PVC's storage class matches the one found by storage policy mapping
		if pvc.Spec.StorageClassName != nil && *pvc.Spec.StorageClassName != storageClassName {
			msg := fmt.Sprintf("PVC %s has storage class %s, but volume maps to storage class %s",
				instance.Spec.PvcName, *pvc.Spec.StorageClassName, storageClassName)
			log.Error(msg)
			setInstanceError(ctx, r, instance, msg)
			if delErr := r.cleanupCNSVolume(ctx, instance, volumeID); delErr != nil {
				log.Errorf("Failed to cleanup CNS volume: %s with error: %+v", volumeID, delErr)
			}
			return reconcile.Result{RequeueAfter: timeout}, nil
		} else if pvc.Spec.StorageClassName == nil {
			msg := fmt.Sprintf("PVC %s has no storage class specified, but requires storage class %s",
				instance.Spec.PvcName, storageClassName)
			log.Error(msg)
			setInstanceError(ctx, r, instance, msg)
			if delErr := r.cleanupCNSVolume(ctx, instance, volumeID); delErr != nil {
				log.Errorf("Failed to cleanup CNS volume: %s with error: %+v", volumeID, delErr)
			}
			return reconcile.Result{RequeueAfter: timeout}, nil
		}

		// Validate topology compatibility if PVC exists and can be reused. For host-local volumes the
		// topology is already resolved (datastoreAccessibleTopology is non-empty) and topologyMgr is
		// not consulted, so run the validation even when topologyMgr is nil (e.g. non-stretched
		// supervisor) to ensure the reused PVC gets the {zone, hostname} annotation.
		if topologyMgr != nil || isHostLocal {
			err = validatePVCTopologyCompatibility(ctx, k8sclient, pvc, volume.DatastoreUrl, topologyMgr, vc,
				datastoreAccessibleTopology, isHostLocal)
			if err != nil {
				msg := fmt.Sprintf("PVC topology validation failed: %v", err)
				log.Error(msg)
				setInstanceError(ctx, r, instance, msg)
				if delErr := r.cleanupCNSVolume(ctx, instance, volumeID); delErr != nil {
					log.Errorf("Failed to cleanup CNS volume: %s with error: %+v", volumeID, delErr)
				}
				return reconcile.Result{RequeueAfter: timeout}, nil
			}
		}
	}

	capacityInMb := volume.BackingObjectDetails.GetCnsBackingObjectDetails().CapacityInMb
	// pvCapacity is used for PV spec, CNSVolumeInfo and validateAndFixPVVolumeMode;
	// When registering a disk with VM as the datasource ref, we expect VM Op to create the PVC with DataSourceRef
	// while CSI registers the disk. So, we check  if a PVC exists and use the capacity from the PVC when
	// creating the PV.
	// When a PVC exists with DataSourceRef (e.g. vmoperator.vmware.com/ VirtualMachine)
	// and spec.resources.requests.storage set, use that value for the PV capacity so PV and PVC sizes
	// match and they can bind.
	// When PVC not present, query the volume size from the backend and applies queried size to
	// both the PV and the PVC.
	var pvCapacity resource.Quantity
	if pvc != nil && pvc.Spec.DataSourceRef != nil && pvc.Spec.Resources.Requests != nil {
		if requestStorage, ok := pvc.Spec.Resources.Requests[v1.ResourceStorage]; ok {
			pvCapacity = requestStorage.DeepCopy()
			log.Infof("Using PVC requested size %s for PV capacity as DataSourceRef is set on PVC", requestStorage.String())
			log.Infof("PVC %s/%s size in MB: %v", pvc.Namespace, pvc.Name, common.RoundUpSize(pvCapacity.Value(), 1024*1024))
			log.Infof("FCD size in MB: %v", capacityInMb)
			if err := validatePVCCapacityMatchesBackendFCD(pvc, capacityInMb); err != nil {
				log.Error(err.Error())
				setInstanceError(ctx, r, instance, err.Error())
				return reconcile.Result{RequeueAfter: timeout}, nil
			}
		} else {
			log.Errorf("PVC %s has no storage request and DataSourceRef is not supported", pvc.Namespace+"/"+pvc.Name)
			setInstanceError(ctx, r, instance,
				fmt.Sprintf("PVC %s has no storage request and DataSourceRef is not supported", pvc.Namespace+"/"+pvc.Name))
			return reconcile.Result{RequeueAfter: timeout}, nil
		}
	} else {
		log.Infof("Using backend capacity for PV: %d Mi", capacityInMb)
		pvCapacity = *resource.NewQuantity(capacityInMb*common.MbInBytes, resource.BinarySI)
	}
	accessMode := instance.Spec.AccessMode
	// Set accessMode to ReadWriteOnce if DiskURLPath is used for import.
	if accessMode == "" && instance.Spec.DiskURLPath != "" {
		accessMode = v1.ReadWriteOnce
	}
	pv, err := k8sclient.CoreV1().PersistentVolumes().Get(ctx, pvName, metav1.GetOptions{})
	if err != nil {
		if apierrors.IsNotFound(err) {
			log.Infof("PV: %s not found. Creating a new PV", pvName)
			// Create Persistent volume with claimRef.
			claimRef := &v1.ObjectReference{
				Kind:       "PersistentVolumeClaim",
				APIVersion: "v1",
				Namespace:  instance.Namespace,
				Name:       instance.Spec.PvcName,
			}
			pvSpec := getPersistentVolumeSpec(pvName, volumeID, pvCapacity,
				accessMode, instance.Spec.VolumeMode, storageClassName, claimRef,
				instance.Namespace, instance.Name)
			pvSpec.Spec.NodeAffinity = pvNodeAffinity
			if isRestore {
				setRestoreAnnotations(pvSpec, instance)
			}
			log.Debugf("PV spec is: %+v", pvSpec)
			pv, err = k8sclient.CoreV1().PersistentVolumes().Create(ctx, pvSpec, metav1.CreateOptions{})
			if err != nil {
				log.Errorf("Failed to create PV with spec: %+v. Error: %+v", pvSpec, err)
				setInstanceError(ctx, r, instance,
					fmt.Sprintf("Failed to create PV: %s for volume with err: %+v", pvName, err))
				return reconcile.Result{RequeueAfter: timeout}, nil
			}
			log.Infof("PV: %s is created successfully", pvName)
		} else {
			msg := fmt.Sprintf("Failed to get PV: %s with error: %+v", pvName, err)
			log.Error(msg)
			setInstanceError(ctx, r, instance, msg)
			return reconcile.Result{RequeueAfter: timeout}, nil
		}
	} else {
		// A restore annotates the PV first, which fails for a PV restored by another
		// CnsRegisterVolume, so such a PV is never recreated below.
		if isRestore {
			pv, err = ensureRestoreAnnotationsOnPV(ctx, k8sclient, pv, instance)
			if err != nil {
				log.Error(err.Error())
				setInstanceError(ctx, r, instance, err.Error())
				return reconcile.Result{RequeueAfter: timeout}, nil
			}
		}
		// PV exists - check if volumeMode needs correction
		pv, err = validateAndFixPVVolumeMode(ctx, k8sclient, r, instance, pv, pvName, volumeID, pvCapacity,
			accessMode, storageClassName, pvNodeAffinity, timeout, isRestore)
		if err != nil {
			return reconcile.Result{RequeueAfter: timeout}, nil
		}
	}
	// If PV is already bound to a different PVC at this point, then its a
	// duplicate request.
	if pv.Status.Phase == v1.VolumeBound && isPVClaimedByOtherPVC(pv, instance) {
		log.Errorf("Duplicate Request. There already exists a PV: %s which is bound", pvName)
		setInstanceError(ctx, r, instance, "Duplicate Request")
		return reconcile.Result{RequeueAfter: timeout}, nil
	}

	IsPVCCreatedByCNSRegisterVolumeReconciler := false

	if pvc != nil {
		if pvc.Status.Phase == v1.ClaimBound && pvc.Spec.VolumeName != pvName {
			// This is handle cases where PVC with this name already exists and
			// is bound. This happens when a new CnsRegisterVolume instance is
			// created to import a new volume with PVC name which is already
			// created and is bound.
			msg := fmt.Sprintf("Another PVC: %s already exists in namespace: %s which is Bound to a different PV",
				instance.Spec.PvcName, instance.Namespace)
			log.Errorf(msg)
			setInstanceError(ctx, r, instance, msg)
			if err = r.cleanupCNSVolume(ctx, instance, volumeID); err != nil {
				log.Errorf("Failed to cleanup CNS volume: %s with error: %+v", volumeID, err)
			} else {
				// Delete PV created above.
				err = k8sclient.CoreV1().PersistentVolumes().Delete(ctx, pvName, *metav1.NewDeleteOptions(0))
				if err != nil {
					log.Errorf("Failed to delete PV: %s with error: %+v", pvName, err)
				}
			}
			return reconcile.Result{RequeueAfter: timeout}, nil
		}

		if pvc.Spec.DataSourceRef != nil {
			apiGroup := ""
			if pvc.Spec.DataSourceRef.APIGroup != nil {
				apiGroup = *pvc.Spec.DataSourceRef.APIGroup
			}
			log.Infof("PVC %s in namespace %s has valid DataSourceRef with apiGroup: %s, kind: %s, name: %s",
				pvc.Name, pvc.Namespace, apiGroup, pvc.Spec.DataSourceRef.Kind, pvc.Spec.DataSourceRef.Name)
		}
	} else {
		// Create PVC mapping to above created PV.
		log.Infof("Creating PVC: %s", instance.Spec.PvcName)
		pvcSpec, err := getPersistentVolumeClaimSpec(ctx, instance.Spec.PvcName, instance.Namespace, capacityInMb,
			storageClassName, accessMode, *pv.Spec.VolumeMode, pvName, datastoreAccessibleTopology, instance)
		if err != nil {
			msg := fmt.Sprintf("Failed to create spec for PVC: %q. Error: %v", instance.Spec.PvcName, err)
			log.Errorf(msg)
			setInstanceError(ctx, r, instance, msg)
			return reconcile.Result{RequeueAfter: timeout}, nil
		}
		log.Debugf("PVC spec is: %+v", pvcSpec)
		pvc, err = k8sclient.CoreV1().PersistentVolumeClaims(instance.Namespace).Create(ctx,
			pvcSpec, metav1.CreateOptions{})
		if err != nil {
			log.Errorf("Failed to create PVC with spec: %+v. Error: %+v", pvcSpec, err)
			errMsg := fmt.Sprintf("Failed to create PVC: %s for volume with err: %+v", instance.Spec.PvcName, err)
			setInstanceError(ctx, r, instance, errMsg)
			// Delete PV created above.
			err = k8sclient.CoreV1().PersistentVolumes().Delete(ctx, pvName, *metav1.NewDeleteOptions(0))
			if err != nil {
				log.Errorf("Delete PV %s failed with error: %+v", pvName, err)
				combinedErrMsg := fmt.Sprintf("%s. Additionally, cleanup failed: Delete PV %s failed with error: %+v",
					errMsg, pvName, err)
				setInstanceError(ctx, r, instance, combinedErrMsg)
			}
			return reconcile.Result{RequeueAfter: timeout}, nil
		} else {
			log.Infof("PVC: %s is created successfully", instance.Spec.PvcName)
			IsPVCCreatedByCNSRegisterVolumeReconciler = true
		}
	}
	// Check whether the PVC is bound, without blocking on it. The previous
	// implementation opened a per-CR watch and held this worker for up to 60s;
	// now an unbound PVC simply requeues and the worker moves on.
	setInstanceConditions(ctx, r, instance, falseCondition(cnsregistervolumev1alpha1.ConditionPvcBound,
		cnsregistervolumev1alpha1.ReasonWaitingForBind, "Waiting for the PVC to be bound"))
	isBound, err := isPVCBoundNow(ctx, k8sclient, pvc)
	if err != nil {
		log.Errorf("failed to check bind state of PVC %s: %v", instance.Spec.PvcName, err)
		return reconcile.Result{RequeueAfter: timeout}, nil
	}
	if !isBound {
		log.Infof("PVC %s is not bound yet; will re-check", instance.Spec.PvcName)
		return reconcile.Result{RequeueAfter: pvcBindPollInterval}, nil
	}
	{
		log.Infof("PVC: %s is bound", instance.Spec.PvcName)
		setInstanceConditions(ctx, r, instance,
			trueCondition(cnsregistervolumev1alpha1.ConditionPvcBound, cnsregistervolumev1alpha1.ReasonBound, "PVC is bound"))
		// Allow binding before retrying VSLM, but do not declare registration
		// complete until the flag clear is durable. Keep this before quota writes:
		// failed clear attempts must not repeatedly add this volume's usage.
		var protectionRetry *reconcile.Result
		pvc, protectionRetry = r.reconcileVMDeleteProtection(ctx, instance, pvc, volumeID)
		if protectionRetry != nil {
			return *protectionRetry, nil
		}
		if !isRestore || !syncer.IsPodVMOnStretchSupervisorFSSEnabled {
			setInstanceConditions(ctx, r, instance, trueCondition(
				cnsregistervolumev1alpha1.ConditionRestoreMetadataConfigured,
				cnsregistervolumev1alpha1.ReasonNotRequired, "Restore metadata handover is not required"))
		}
		setInstanceConditions(ctx, r, instance,
			falseCondition(cnsregistervolumev1alpha1.ConditionReady, cnsregistervolumev1alpha1.ReasonFinalizing,
				"Applying CnsVolumeInfo and storage quota accounting"))
		if syncer.IsPodVMOnStretchSupervisorFSSEnabled {
			if isRestore {
				// The restored CnsVolumeInfo is created already carrying the snapshot size of the deleted
				// volume, so the snapshot usage of the namespace does not drop during the restore.
				err = r.reconcileRestoreMetadata(ctx, instance, pvName, cnsvolumeinfo.RestoredVolumeInfo{
					VolumeID:         volumeID,
					Namespace:        instance.Namespace,
					StoragePolicyID:  volume.StoragePolicyId,
					StorageClassName: storageClassName,
					VCenter:          vc.Config.Host.String(),
					Capacity:         &pvCapacity,
					RestoredBy:       string(instance.UID),
				})
				if err != nil {
					msg := fmt.Sprintf("failed to create the CnsVolumeInfo of restored volume %s: %v", volumeID, err)
					log.Error(msg)
					setInstanceError(ctx, r, instance, msg)
					return reconcile.Result{RequeueAfter: timeout}, nil
				}
			} else {
				// Create CNSVolumeInfo CR for static pv (pvCapacity set earlier from PVC or backend)
				err = r.volumeInfoService.CreateVolumeInfoWithPolicyInfo(ctx, volumeID, instance.Namespace,
					volume.StoragePolicyId, storageClassName, vc.Config.Host.String(), &pvCapacity, false)
				if err != nil {
					log.Errorf("failed to store volumeID %q namespace %s StoragePolicyID %q StorageClassName %q "+
						"and vCenter %q in CNSVolumeInfo CR. Error: %+v", volumeID, instance.Namespace,
						volume.StoragePolicyId, storageClassName, vc.Config.Host, err)
					return reconcile.Result{RequeueAfter: timeout}, nil
				}
			}

			restConfig, err := clientConfig.GetConfig()
			if err != nil {
				log.Errorf("failed to get Kubernetes config. Err: %+v", err)
				return reconcile.Result{RequeueAfter: timeout}, nil
			}
			cnsOperatorClient, err := k8s.NewClientForGroup(ctx,
				restConfig, apis.GroupName)
			if err != nil {
				log.Errorf("failed to create cns operator client. Err: %v", err)
				return reconcile.Result{RequeueAfter: timeout}, nil
			}

			namespace := instance.Namespace
			storagePolicyUsageCRName := storageClassName + "-" +
				storagepolicyusagev1alpha3.NameSuffixForPVC
			storagePolicyUsageCR := &storagepolicyusagev1alpha3.StoragePolicyUsage{}
			err = cnsOperatorClient.Get(ctx, apitypes.NamespacedName{
				Namespace: namespace,
				Name:      storagePolicyUsageCRName},
				storagePolicyUsageCR)
			if err != nil {
				log.Errorf("failed to fetch %s instance with name %q from supervisor namespace %q. Error: %+v",
					storagepolicyusagev1alpha3.CRDSingular, storagePolicyUsageCRName,
					namespace, err)
				return reconcile.Result{RequeueAfter: timeout}, nil
			}

			// Patch an increase of "reserved" in storagePolicyUsageCR.
			patchedStoragePolicyUsageCR := storagePolicyUsageCR.DeepCopy()
			if storagePolicyUsageCR.Status.ResourceTypeLevelQuotaUsage != nil &&
				storagePolicyUsageCR.Status.ResourceTypeLevelQuotaUsage.Reserved != nil {
				patchedStoragePolicyUsageCR.Status.ResourceTypeLevelQuotaUsage.Reserved.Add(pvCapacity)
			} else {
				var (
					usedQty     resource.Quantity
					reservedQty resource.Quantity
				)
				reservedQty = *resource.NewQuantity(pvCapacity.Value(), pvCapacity.Format)
				patchedStoragePolicyUsageCR.Status = storagepolicyusagev1alpha3.StoragePolicyUsageStatus{
					ResourceTypeLevelQuotaUsage: &storagepolicyusagev1alpha3.QuotaUsageDetails{
						Reserved: &reservedQty,
						Used:     &usedQty,
					},
				}
			}
			err = syncer.PatchStoragePolicyUsage(ctx, cnsOperatorClient, storagePolicyUsageCR, patchedStoragePolicyUsageCR)
			if err != nil {
				log.Errorf("patching operation failed for StoragePolicyUsage CR: %q in namespace: %q. err: %v",
					storagePolicyUsageCR.Name, storagePolicyUsageCR.Namespace, err)
			}
			// Retrieve the CR
			currentStoragePolicyUsageCR := &storagepolicyusagev1alpha3.StoragePolicyUsage{}
			finalStoragePolicyUsageCR := &storagepolicyusagev1alpha3.StoragePolicyUsage{}
			key := apitypes.NamespacedName{Namespace: storagePolicyUsageCR.Namespace,
				Name: storagePolicyUsageCR.Name}
			err = cnsOperatorClient.Get(ctx, key, currentStoragePolicyUsageCR)
			if err != nil {
				log.Errorf("failed to get %s CR from supervisor namespace %q. Error: %+v",
					storagePolicyUsageCR.Name, storagePolicyUsageCR.Namespace, err)
				return reconcile.Result{RequeueAfter: timeout}, nil
			}
			finalStoragePolicyUsageCR = currentStoragePolicyUsageCR.DeepCopy()
			// Decrease the Reserved field, and increase the Used field, by this PVC's own
			// capacity only. Reserved is a pool shared across every disk in the same (e.g.
			// multi-disk VM import) batch, so it may transiently hold other PVCs'
			// still-outstanding reservations at the moment of this Get; draining the CR's
			// entire current Reserved value here (instead of just this PVC's pvCapacity
			// share) would steal those sibling reservations and double-count them into Used.
			reservedBeforeTransfer := finalStoragePolicyUsageCR.Status.ResourceTypeLevelQuotaUsage.Reserved
			if reservedBeforeTransfer.Cmp(pvCapacity) < 0 {
				// Reserved should always be at least this PVC's own pvCapacity, since it was
				// added there earlier in this same reconcile. If it isn't, the shared pool is
				// already inconsistent for reasons outside this PVC's transition; clamp to
				// zero instead of underflowing Reserved negative, and surface it loudly so the
				// inconsistency is visible rather than silently compounding.
				log.Errorf("StoragePolicyUsage CR %q in namespace %q has Reserved (%s) less than "+
					"this PVC's own capacity (%s) being transferred to Used; the Reserved pool is "+
					"inconsistent. Clamping Reserved to zero instead of underflowing negative.",
					currentStoragePolicyUsageCR.Name, currentStoragePolicyUsageCR.Namespace,
					reservedBeforeTransfer.String(), pvCapacity.String())
				*finalStoragePolicyUsageCR.Status.ResourceTypeLevelQuotaUsage.Reserved = resource.Quantity{}
			} else {
				finalStoragePolicyUsageCR.Status.ResourceTypeLevelQuotaUsage.Reserved.Sub(pvCapacity)
			}
			finalStoragePolicyUsageCR.Status.ResourceTypeLevelQuotaUsage.Used.Add(pvCapacity)
			err = syncer.PatchStoragePolicyUsage(ctx, cnsOperatorClient, currentStoragePolicyUsageCR,
				finalStoragePolicyUsageCR)
			if err != nil {
				log.Errorf("patching operation failed for StoragePolicyUsage CR: %q in namespace: %q. err: %v",
					currentStoragePolicyUsageCR.Name, currentStoragePolicyUsageCR.Namespace, err)
			} else {
				log.Infof("Successfully decreased the reserved field by %s "+
					"for storagepolicyusage CR: %q in namespace: %q",
					pvCapacity.String(), finalStoragePolicyUsageCR.Name, finalStoragePolicyUsageCR.Namespace)
				log.Infof("Successfully increased the used field by %s "+
					"for storagepolicyusage CR: %q in namespace: %q",
					pvCapacity.String(), finalStoragePolicyUsageCR.Name, finalStoragePolicyUsageCR.Namespace)
			}
		}
	}

	if isSharedDiskEnabled && !IsPVCCreatedByCNSRegisterVolumeReconciler {
		// Ensure that the PVC has the correct backing disk annotation if PVC was already present on the cluster.
		if pvc != nil {
			pvc, err = setBackingDiskAnnotation(ctx, k8sclient, instance, pvc)
			if err != nil {
				msg := fmt.Sprintf("failed to update bakcing disk type on PVC. Error: %s", err)
				setInstanceError(ctx, r, instance, msg)
				return reconcile.Result{RequeueAfter: timeout}, nil
			}
		}
	}

	// Update the instance to indicate the volume registration is successful.
	msg := fmt.Sprintf("Successfully registered the volume on namespace: %s", instance.Namespace)
	err = setInstanceSuccess(ctx, r, instance, instance.Spec.PvcName, pvc.UID, msg)
	if err != nil {
		msg := fmt.Sprintf("Failed to update CnsRegistered instance with error: %+v", err)
		log.Error(msg)
		setInstanceError(ctx, r, instance, msg)
		return reconcile.Result{RequeueAfter: timeout}, nil
	}
	backOffDurationMapMutex.Lock()
	delete(backOffDuration, request.NamespacedName)
	backOffDurationMapMutex.Unlock()
	log.Info(msg)
	return reconcile.Result{}, nil
}

// validatePVCTopologyCompatibility checks if the existing PVC's topology annotation is compatible
// with the volume's actual placement zone.
func validatePVCTopologyCompatibility(ctx context.Context, k8sclient clientset.Interface, pvc *v1.PersistentVolumeClaim,
	volumeDatastoreURL string, topologyMgr commoncotypes.ControllerTopologyService,
	vc *cnsvsphere.VirtualCenter, datastoreAccessibleTopology []map[string]string, isHostLocal bool) error {
	log := logger.GetLogger(ctx)

	// Check if PVC has topology annotation
	topologyAnnotation, exists := pvc.Annotations[common.AnnVolumeAccessibleTopology]
	if !exists || topologyAnnotation == "" {
		// No topology annotation on PVC, add topology annotation and skip validation
		log.Debugf("PVC %s/%s has no topology annotation, adding topology annotation",
			pvc.Namespace, pvc.Name)
		var segmentsArray []string
		for _, topologyTerm := range datastoreAccessibleTopology {
			jsonSegment, err := json.Marshal(topologyTerm)
			if err != nil {
				return logger.LogNewErrorf(log,
					"failed to marshal topology segment: %+v to json. Error: %+v", topologyTerm, err)
			}
			segmentsArray = append(segmentsArray, string(jsonSegment))
		}
		topologyAnnotation = "[" + strings.Join(segmentsArray, ",") + "]"

		if pvc.Annotations == nil {
			pvc.Annotations = make(map[string]string)
		}
		pvc.Annotations[common.AnnVolumeAccessibleTopology] = topologyAnnotation

		// Persist the topology annotation via a merge patch (resourceVersion-agnostic) instead of a
		// full-object Update, so this write can't 409 if the PVC was already mutated earlier in this
		// same reconcile pass or by another controller.
		patchAnnotations := make(map[string]interface{})
		for k, v := range pvc.Annotations {
			patchAnnotations[k] = v
		}
		patch := map[string]interface{}{
			"metadata": map[string]interface{}{
				"annotations": patchAnnotations,
			},
		}
		patchBytes, err := json.Marshal(patch)
		if err != nil {
			return logger.LogNewErrorf(log, "failed to marshal topology annotation patch for PVC %s/%s: %+v",
				pvc.Namespace, pvc.Name, err)
		}
		patchedPVC, err := k8sclient.CoreV1().PersistentVolumeClaims(pvc.Namespace).Patch(ctx, pvc.Name,
			apitypes.MergePatchType, patchBytes, metav1.PatchOptions{})
		if err != nil {
			return logger.LogNewErrorf(log, "failed to patch PVC %s/%s with topology annotation. PVC: %+v, err: %+v",
				pvc.Namespace, pvc.Name, pvc, err)
		}
		log.Debugf("Patched PVC: %+v", patchedPVC)
		pvc = patchedPVC

		log.Infof("Successfully added topology annotation %s to PVC %s/%s",
			topologyAnnotation, pvc.Namespace, pvc.Name)
		// Return nil as we just added the topology annotation based on actual volume placement
		return nil
	}

	// Parse PVC topology annotation
	var pvcTopologySegments []map[string]string
	err := json.Unmarshal([]byte(topologyAnnotation), &pvcTopologySegments)
	if err != nil {
		return fmt.Errorf("failed to parse topology annotation on PVC %s/%s: %v",
			pvc.Namespace, pvc.Name, err)
	}

	// Determine the volume's actual topology. For host-local volumes the caller already resolved the
	// node-driven {zone, hostname} topology; reuse it directly and skip the shared-datastore
	// intersection recompute (GetTopologyInfoFromNodes), which would incorrectly drop the single-host
	// datastore and reject the PVC.
	var volumeTopologySegments []map[string]string
	if isHostLocal && len(datastoreAccessibleTopology) > 0 {
		volumeTopologySegments = datastoreAccessibleTopology
	} else {
		volumeTopologySegments, err = topologyMgr.GetTopologyInfoFromNodes(ctx,
			commoncotypes.WCPRetrieveTopologyInfoParams{
				DatastoreURL:        volumeDatastoreURL,
				StorageTopologyType: "",
				TopologyRequirement: nil,
				Vc:                  vc,
			})
		if err != nil {
			return fmt.Errorf("failed to get topology for volume datastore %s: %v", volumeDatastoreURL, err)
		}
	}

	// Check if volume topology segments are compatible with PVC topology segments
	if isTopologyCompatible(pvcTopologySegments, volumeTopologySegments) {
		log.Infof("PVC %s/%s topology annotation is compatible with volume placement",
			pvc.Namespace, pvc.Name)
		return nil
	}

	// No compatible topology found
	return fmt.Errorf("PVC %s/%s topology annotation %s is not compatible with volume placement in zones %+v",
		pvc.Namespace, pvc.Name, topologyAnnotation, volumeTopologySegments)
}

// isTopologyCompatible checks if volume topology segments are compatible with PVC topology segments.
// Returns true if at least one volume topology segment is satisfied by the PVC topology segments.
func isTopologyCompatible(pvcTopologySegments, volumeTopologySegments []map[string]string) bool {
	// If PVC does not have any topology segments, any topology associated with the volume
	// would satisfy the requirements
	if len(pvcTopologySegments) == 0 {
		return true
	}

	// For each volume topology segment, check if it exists in PVC topology segments
	for _, volumeSegment := range volumeTopologySegments {
		for _, pvcSegment := range pvcTopologySegments {
			// Check if the volume segment satisfies the PVC's topology requirements
			segmentCompatible := false
			for key, volumeValue := range volumeSegment {
				if pvcValue, exists := pvcSegment[key]; exists && pvcValue == volumeValue {
					segmentCompatible = true
					break
				}
			}
			if segmentCompatible {
				return true // Found at least one compatible segment
			}
		}
	}
	return false // No compatible segments found
}

// checkExistingPVCDataSourceRef checks if a PVC already exists and validates its DataSourceRef.
// Returns the PVC if it exists with no DataSourceRef or it exists with a supported DataSourceRef.
// If PVC exists but has an unsupported DataSourceRef, return (nil, error).
// If PVC does not exist, return (nil, nil) so that it will be created later.
func checkExistingPVCDataSourceRef(ctx context.Context, k8sclient clientset.Interface,
	pvcName, namespace string) (*v1.PersistentVolumeClaim, error) {
	log := logger.GetLogger(ctx)

	// Try to get the existing PVC
	existingPVC, err := k8sclient.CoreV1().PersistentVolumeClaims(namespace).Get(ctx, pvcName, metav1.GetOptions{})
	if err != nil {
		if apierrors.IsNotFound(err) {
			// PVC doesn't exist, return nil (we'll create a new one)
			return nil, nil
		}
		// Some other error occurred
		return nil, fmt.Errorf("failed to check existing PVC %s in namespace %s: %+v", pvcName, namespace, err)
	}

	// PVC exists, check if it has DataSourceRef
	if existingPVC.Spec.DataSourceRef == nil {
		log.Infof("Existing PVC %s in namespace %s has no DataSourceRef, can reuse", pvcName, namespace)
		return existingPVC, nil
	}

	// Check if DataSourceRef matches supported types
	apiGroup := ""
	if existingPVC.Spec.DataSourceRef.APIGroup != nil {
		apiGroup = *existingPVC.Spec.DataSourceRef.APIGroup
	}

	for _, supportedType := range supportedDataSourceTypes {
		if supportedType.apiGroup == apiGroup && supportedType.kind == existingPVC.Spec.DataSourceRef.Kind {
			log.Infof("Existing PVC %s in namespace %s has valid DataSourceRef (apiGroup: %s, kind: %s), can reuse",
				pvcName, namespace, apiGroup, existingPVC.Spec.DataSourceRef.Kind)
			return existingPVC, nil
		}
	}

	// Check if DataSourceRef is VolumeSnapshot
	if existingPVC.Spec.DataSourceRef.Kind == "VolumeSnapshot" &&
		existingPVC.Spec.DataSourceRef.APIGroup != nil &&
		*existingPVC.Spec.DataSourceRef.APIGroup == "snapshot.storage.k8s.io" {
		log.Infof("WARNING: Existing PVC %s in namespace %s has valid VolumeSnapshot DataSourceRef, "+
			"however, it is not supported for CNSRegisterVolume", pvcName, namespace)
	}

	// DataSourceRef is not supported
	return nil, fmt.Errorf("existing PVC %s in namespace %s has unsupported DataSourceRef for CNSRegisterVolume. "+
		"APIGroup: %s, Kind: %s is not supported. Supported types: %+v",
		pvcName, namespace, apiGroup, existingPVC.Spec.DataSourceRef.Kind, supportedDataSourceTypes)
}

// validateCnsRegisterVolumeSpec validates the input params of
// CnsRegisterVolume instance.
func validateCnsRegisterVolumeSpec(ctx context.Context, instance *cnsregistervolumev1alpha1.CnsRegisterVolume) error {
	var msg string
	// Both VolumeID and DiskURLPath may be set by ss-metadata-manager for the VKSRegisterVolume
	// restore path: CNS locates the VMDK by URL and stamps the provided FCD UUID onto it.
	// This combination is only permitted when the VSphereDPLPModernApp WCP capability
	// is active on the Supervisor; on older Supervisors the original rejection is preserved.
	if instance.Spec.VolumeID != "" && instance.Spec.DiskURLPath != "" {
		if !isVSphereDPLPModernAppEnabled {
			return errors.New("VolumeID and DiskURLPath cannot be specified together")
		}
		// Capability is active: both fields are valid. The single-field checks below
		// (AccessMode requirement, duplicate-PV lookup) do not apply to this path.
		return nil
	}
	if instance.Spec.DiskURLPath != "" && instance.Spec.AccessMode != "" &&
		instance.Spec.AccessMode != v1.ReadWriteOnce {
		if isSharedDiskEnabled {
			if instance.Spec.AccessMode == v1.ReadWriteMany &&
				instance.Spec.VolumeMode == v1.PersistentVolumeFilesystem {
				// File volume is not support for disk URL path.
				msg = fmt.Sprintf("DiskURLPath cannot be used with accessMode: %s and volumeMode: %s",
					instance.Spec.AccessMode, instance.Spec.VolumeMode)
			}
		} else {
			msg = fmt.Sprintf("DiskURLPath cannot be used with accessMode: %q", instance.Spec.AccessMode)
		}
	}
	if instance.Spec.VolumeID != "" {
		// Accessmode is required if volumeID is specified by the user.
		if instance.Spec.AccessMode == "" {
			msg = "AccessMode cannot be empty when volumeID is specified"
		} else {
			pvName, found := commonco.ContainerOrchestratorUtility.GetPVNameFromCSIVolumeID(instance.Spec.VolumeID)
			if found {
				if pvName != staticPvNamePrefix+instance.Spec.VolumeID {
					msg = fmt.Sprintf("PV: %q with the volume ID: %q "+
						"is already present. Can not create multiple PV with same volume Id.", pvName, instance.Spec.VolumeID)
				}
			}
		}
	}
	if msg != "" {
		return errors.New(msg)
	}
	return nil
}

// isBlockVolumeRegisterRequest verifies if block volume register is requested
// via CnsRegisterVolume instance.
func isBlockVolumeRegisterRequest(ctx context.Context, instance *cnsregistervolumev1alpha1.CnsRegisterVolume) bool {
	if instance.Spec.AccessMode != "" {
		if instance.Spec.AccessMode == v1.ReadWriteOnce {
			return true
		}
		if isSharedDiskEnabled {
			// Shared block volume
			if instance.Spec.AccessMode == v1.ReadWriteMany &&
				instance.Spec.VolumeMode == v1.PersistentVolumeBlock {
				return true
			}
		}
	} else {
		if instance.Spec.DiskURLPath != "" {
			return true
		}
	}
	return false
}

// isPVClaimedByOtherPVC returns true if the PV's claimRef points to a PVC other
// than the one requested by the CnsRegisterVolume instance. Both name and
// namespace are compared since the PV is cluster-scoped.
func isPVClaimedByOtherPVC(pv *v1.PersistentVolume,
	instance *cnsregistervolumev1alpha1.CnsRegisterVolume) bool {
	if pv.Spec.ClaimRef == nil {
		return false
	}
	return pv.Spec.ClaimRef.Name != instance.Spec.PvcName ||
		pv.Spec.ClaimRef.Namespace != instance.Namespace
}

// validateAndFixPVVolumeMode checks if an existing PV has the correct volumeMode.
// If the volumeMode doesn't match what's expected, it untags the CNS volume, deletes
// and recreates the PV with the correct volumeMode since volumeMode is immutable on PVs.
func validateAndFixPVVolumeMode(ctx context.Context, k8sclient clientset.Interface,
	r *ReconcileCnsRegisterVolume, instance *cnsregistervolumev1alpha1.CnsRegisterVolume,
	pv *v1.PersistentVolume, pvName, volumeID string, pvCapacity resource.Quantity,
	accessMode v1.PersistentVolumeAccessMode, storageClassName string,
	pvNodeAffinity *v1.VolumeNodeAffinity, timeout time.Duration, isRestore bool) (*v1.PersistentVolume, error) {
	log := logger.GetLogger(ctx)

	// Determine expected volumeMode
	expectedVolumeMode := instance.Spec.VolumeMode
	if expectedVolumeMode == "" {
		expectedVolumeMode = v1.PersistentVolumeFilesystem
	}

	// Get actual volumeMode from PV
	pvVolumeMode := v1.PersistentVolumeFilesystem
	if pv.Spec.VolumeMode != nil {
		pvVolumeMode = *pv.Spec.VolumeMode
	}

	// Check if volumeMode matches
	if expectedVolumeMode != pvVolumeMode {
		// Never untag/delete/recreate a PV that is claimed by a different PVC,
		// e.g. a PVC with the same name in another namespace.
		if isPVClaimedByOtherPVC(pv, instance) {
			msg := fmt.Sprintf("PV %s is claimed by PVC %s/%s, not %s/%s. Refusing to recreate it "+
				"with volumeMode %s", pvName, pv.Spec.ClaimRef.Namespace, pv.Spec.ClaimRef.Name,
				instance.Namespace, instance.Spec.PvcName, expectedVolumeMode)
			log.Error(msg)
			setInstanceError(ctx, r, instance, msg)
			return nil, errors.New(msg)
		}

		log.Warnf("PV %s exists but has incorrect volumeMode. Expected: %s, Actual: %s. "+
			"Untagging CNS volume and recreating PV with correct volumeMode.", pvName, expectedVolumeMode, pvVolumeMode)

		// Untag the CNS volume before deleting PV to prevent underlying volume deletion.
		// deleteDisk=false ensures the underlying vSphere disk is preserved.
		log.Infof("Untagging CNS volume %s to preserve underlying disk before PV deletion", volumeID)
		_, err := common.DeleteVolumeUtil(ctx, r.volumeManager, volumeID, false)
		if err != nil {
			msg := fmt.Sprintf("Failed to untag CNS volume %s. Error: %+v", volumeID, err)
			log.Error(msg)
			setInstanceError(ctx, r, instance, msg)
			return nil, err
		}
		log.Infof("Successfully untagged CNS volume %s", volumeID)

		// Delete the existing PV (underlying volume is safe due to CNS untag with deleteDisk=false)
		err = k8sclient.CoreV1().PersistentVolumes().Delete(ctx, pvName, *metav1.NewDeleteOptions(0))
		if err != nil {
			msg := fmt.Sprintf("Failed to delete PV %s with incorrect volumeMode. Error: %+v", pvName, err)
			log.Error(msg)
			setInstanceError(ctx, r, instance, msg)
			return nil, err
		}
		log.Infof("Successfully deleted PV %s with incorrect volumeMode", pvName)

		// Wait for PV to be fully deleted before recreating
		log.Infof("Waiting for PV %s to be fully deleted", pvName)
		waitErr := wait.PollUntilContextTimeout(ctx, time.Second, timeout, true, func(ctx context.Context) (bool, error) {
			_, err := k8sclient.CoreV1().PersistentVolumes().Get(ctx, pvName, metav1.GetOptions{})
			if err != nil {
				if apierrors.IsNotFound(err) {
					// PV is fully deleted
					return true, nil
				}
				// Unexpected error
				log.Warnf("Error checking PV deletion status: %+v", err)
				return false, err
			}
			// PV still exists, continue waiting
			return false, nil
		})
		if waitErr != nil {
			msg := fmt.Sprintf("Timeout waiting for PV %s to be deleted. Error: %+v", pvName, waitErr)
			log.Error(msg)
			setInstanceError(ctx, r, instance, msg)
			return nil, waitErr
		}
		log.Infof("PV %s has been fully deleted", pvName)

		// Recreate PV with correct volumeMode
		claimRef := &v1.ObjectReference{
			Kind:       "PersistentVolumeClaim",
			APIVersion: "v1",
			Namespace:  instance.Namespace,
			Name:       instance.Spec.PvcName,
		}
		pvSpec := getPersistentVolumeSpec(pvName, volumeID, pvCapacity,
			accessMode, instance.Spec.VolumeMode, storageClassName, claimRef,
			instance.Namespace, instance.Name)
		pvSpec.Spec.NodeAffinity = pvNodeAffinity
		if isRestore {
			setRestoreAnnotations(pvSpec, instance)
		}
		log.Debugf("Recreating PV with spec: %+v", pvSpec)
		pv, err = k8sclient.CoreV1().PersistentVolumes().Create(ctx, pvSpec, metav1.CreateOptions{})
		if err != nil {
			log.Errorf("Failed to recreate PV with spec: %+v. Error: %+v", pvSpec, err)
			setInstanceError(ctx, r, instance,
				fmt.Sprintf("Failed to recreate PV: %s with correct volumeMode. Error: %+v", pvName, err))
			return nil, err
		}
		log.Infof("Successfully recreated PV %s with correct volumeMode: %s", pvName, expectedVolumeMode)
	}

	return pv, nil
}

// validatePVCCapacityMatchesBackendFCD checks that the PVC's requested storage
// size (rounded up to MB) matches the backend FCD capacity in MB, allowing
// capacityInMb or capacityInMb+1 (for rounding). Returns an error if they do not match.
func validatePVCCapacityMatchesBackendFCD(pvc *v1.PersistentVolumeClaim, capacityInMb int64) error {
	requestStorage := pvc.Spec.Resources.Requests[v1.ResourceStorage]
	pvcCapacityInMB := common.RoundUpSize(requestStorage.Value(), 1024*1024)
	if pvcCapacityInMB == capacityInMb || pvcCapacityInMB == capacityInMb+1 {
		return nil
	}
	return fmt.Errorf("PVC %s size is not matching with backend FCD size", pvc.Namespace+"/"+pvc.Name)
}

// clearStaleDeleteOperationRecord drops any "delete-<volumeID>" CnsVolumeOperationRequest left
// behind by an earlier CSI DeleteVolume for this same volume ID. CNS reuses FCD IDs across
// register/unregister cycles, and the CSI driver treats a terminal record for that key as proof
// that the volume is already gone. A surviving record would therefore turn the next genuine delete
// of this newly registered volume into a silent no-op and leak the FCD. This reconciler runs in the
// syncer, whose volume manager has no operation store, so it deletes the CR directly.
// Only Delete is used (never Get or List): reads go through the manager cache and would start an
// informer over every CnsVolumeOperationRequest. Block FCD IDs contain no ':', so no file-volume
// name normalisation is needed. It is best effort: registration must not fail because of this.
func (r *ReconcileCnsRegisterVolume) clearStaleDeleteOperationRecord(ctx context.Context, volumeID string) {
	log := logger.GetLogger(ctx)
	name := "delete-" + volumeID
	namespace := cnsvolumeoperationrequest.GetCSINamespace()
	err := r.client.Delete(ctx, &cnsvolumeoprequestv1alpha1.CnsVolumeOperationRequest{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: namespace},
	})
	switch {
	case err == nil:
		log.Infof("Deleted stale CnsVolumeOperationRequest %s/%s before registering volume %q",
			namespace, name, volumeID)
	case apierrors.IsNotFound(err):
		log.Debugf("No stale CnsVolumeOperationRequest %s/%s found for volume %q", namespace, name, volumeID)
	default:
		log.Warnf("Failed to delete stale CnsVolumeOperationRequest %s/%s for volume %q. Err: %v",
			namespace, name, volumeID, err)
	}
}

// setInstanceError sets error and records an event on the CnsRegisterVolume
// instance.
// Any conditions passed are published in the same status write as the error, so
// that no observer can see the error without the condition describing which step
// failed. Callers that have no condition to report pass none.
func setInstanceError(ctx context.Context, r *ReconcileCnsRegisterVolume,
	instance *cnsregistervolumev1alpha1.CnsRegisterVolume, errMsg string,
	conds ...*metav1.Condition) {
	log := logger.GetLogger(ctx)
	origInstance := instance.DeepCopy()
	instance.Status.Registered = false
	instance.Status.Error = errMsg
	applyConditions(instance, conds...)
	err := patchCnsRegisterVolumeStatus(ctx, r.client, origInstance, instance)
	if err != nil {
		log.Errorf("patchCnsRegisterVolumeStatus failed. err: %v", err)
		return
	}
	recordEvent(ctx, r, instance, v1.EventTypeWarning, errMsg)
}

// failVolumeNotAccessible publishes a post-registration accessibility failure
// and cleans up the CNS volume this registration created.
//
// These checks run against the volume CNS returned, so a failure means a volume
// exists that must not be kept. Cleanup therefore has to succeed before the
// terminal reason is published: while it has not, the outcome is retried and
// reported as CleanupPending. Once the volume is gone, CnsVolumeRegistered is
// republished as False, because the registration has been undone rather than
// left half-made.
func (r *ReconcileCnsRegisterVolume) failVolumeNotAccessible(ctx context.Context,
	instance *cnsregistervolumev1alpha1.CnsRegisterVolume, volumeID, reason, errMsg,
	message string, timeout time.Duration) reconcile.Result {
	log := logger.GetLogger(ctx)
	if err := r.cleanupCNSVolume(ctx, instance, volumeID); err != nil {
		log.Errorf("Failed to cleanup CNS volume: %s with error: %+v", volumeID, err)
		setInstanceError(ctx, r, instance, errMsg,
			falseCondition(cnsregistervolumev1alpha1.ConditionVolumeAccessible,
				cnsregistervolumev1alpha1.ReasonCleanupPending,
				"Waiting for the CNS volume created by this registration to be deleted"))
		return reconcile.Result{RequeueAfter: timeout}
	}
	setInstanceError(ctx, r, instance, errMsg,
		falseCondition(cnsregistervolumev1alpha1.ConditionVolumeAccessible, reason, message),
		falseCondition(cnsregistervolumev1alpha1.ConditionCnsVolumeRegistered,
			cnsregistervolumev1alpha1.ReasonVolumeCleanedUp,
			"The CNS volume was deleted because the registered volume is not usable from this cluster"),
		// Ready is set explicitly here rather than mirrored. CnsVolumeRegistered
		// is an earlier step, so mirroring would surface VolumeCleanedUp -- the
		// consequence -- where the operator needs the cause.
		falseCondition(cnsregistervolumev1alpha1.ConditionReady, reason, message))
	// permanent failure and not requeue.
	return reconcile.Result{}
}

// setInstanceConditions publishes one or more conditions on the
// CnsRegisterVolume instance.
//
// It is purely observational: it leaves Registered and Error untouched, and a
// failure to publish is logged rather than returned, so that reporting progress
// can never fail a registration that is otherwise succeeding. Callers therefore
// do not need to handle an error from it.
//
// Nothing is written when no condition actually changes. conditions.Set only
// moves LastTransitionTime when Status, Reason or Message differs, so a pass
// that re-states the same conditions is a no-op and the age of a condition
// stays a measure of how long the CR has been in that state.
//
// Publishing status does not re-trigger this controller: it filters events with
// predicate.GenerationChangedPredicate, and a status-subresource write does not
// bump metadata.generation.
func setInstanceConditions(ctx context.Context, r *ReconcileCnsRegisterVolume,
	instance *cnsregistervolumev1alpha1.CnsRegisterVolume, conds ...*metav1.Condition) {
	log := logger.GetLogger(ctx)
	origInstance := instance.DeepCopy()
	applyConditions(instance, conds...)
	if apiequality.Semantic.DeepEqual(origInstance.Status.Conditions, instance.Status.Conditions) {
		return
	}
	if err := patchCnsRegisterVolumeStatus(ctx, r.client, origInstance, instance); err != nil {
		// Roll the in-memory conditions back so they keep matching the server;
		// the next transition will try to publish again.
		instance.Status.Conditions = origInstance.Status.Conditions
		log.Errorf("Failed to publish conditions on CnsRegisterVolume instance: %q on namespace: %q. err: %v",
			instance.Name, instance.Namespace, err)
	}
}

// registrationSteps are the step conditions, in the order a registration passes
// through them. Ready mirrors the first of these that is not yet satisfied.
var registrationSteps = []string{
	cnsregistervolumev1alpha1.ConditionSpecValidated,
	cnsregistervolumev1alpha1.ConditionRestoreValidated,
	cnsregistervolumev1alpha1.ConditionCnsVolumeRegistered,
	cnsregistervolumev1alpha1.ConditionVolumeAccessible,
	cnsregistervolumev1alpha1.ConditionPvcBound,
	cnsregistervolumev1alpha1.ConditionVMDeleteProtectionConfigured,
	cnsregistervolumev1alpha1.ConditionRestoreMetadataConfigured,
}

// applyConditions stamps observedGeneration and sets each condition on the
// instance, without writing to the API server.
//
// Ready is kept in step with the rest: unless the caller sets it explicitly, it
// mirrors the reason of the leftmost step that is not True. That is what makes
// the Ready condition, and the kubectl column that reads it, answer "why is this
// CR not registered yet" without the reader having to walk the array.
func applyConditions(instance *cnsregistervolumev1alpha1.CnsRegisterVolume,
	conds ...*metav1.Condition) {
	explicitReady := false
	for _, c := range conds {
		if c == nil {
			continue
		}
		if c.Type == cnsregistervolumev1alpha1.ConditionReady {
			explicitReady = true
		}
		c.ObservedGeneration = instance.Generation
		conditions.Set(instance, c)
	}
	if !explicitReady {
		mirrorReadyCondition(instance)
	}
}

// mirrorReadyCondition republishes Ready from the step conditions.
//
// It deliberately does nothing when no step is blocking: at that point either
// the registration has succeeded, and setInstanceSuccess owns Ready, or the
// remaining work is the final bookkeeping, whose Finalizing reason was set
// explicitly and must not be overwritten.
func mirrorReadyCondition(instance *cnsregistervolumev1alpha1.CnsRegisterVolume) {
	if instance.Status.Registered {
		return
	}
	for _, step := range registrationSteps {
		cond := conditions.Get(instance, step)
		if cond == nil || cond.Status == metav1.ConditionTrue {
			continue
		}
		ready := falseCondition(cnsregistervolumev1alpha1.ConditionReady, cond.Reason, cond.Message)
		ready.ObservedGeneration = instance.Generation
		conditions.Set(instance, ready)
		return
	}
}

// trueCondition builds a True condition with a meaningful reason.
//
// conditions.MarkTrue is deliberately not used: it hard-codes Reason to the
// literal "True", which carries no information, so every True condition here is
// set explicitly instead.
func trueCondition(conditionType, reason, message string) *metav1.Condition {
	return &metav1.Condition{
		Type:    conditionType,
		Status:  metav1.ConditionTrue,
		Reason:  reason,
		Message: message,
	}
}

// falseCondition builds a False condition.
//
// The message must be stable for a given state: it participates in the
// comparison conditions.Set makes before moving LastTransitionTime, so a
// message carrying a retry count or an elapsed time would move the timestamp on
// every pass and destroy the "time in state" signal. Volatile detail belongs in
// logs and events.
func falseCondition(conditionType, reason, message string) *metav1.Condition {
	return &metav1.Condition{
		Type:    conditionType,
		Status:  metav1.ConditionFalse,
		Reason:  reason,
		Message: message,
	}
}

// setInstanceCnsTaskID persists the in-flight CNS task reference, or clears it
// by passing an empty string. Returning the error matters here, unlike for the
// phase: if the task ID cannot be stored, the submitted task has no durable
// reference and the caller must decide what to do about it.
func setInstanceCnsTaskID(ctx context.Context, r *ReconcileCnsRegisterVolume,
	instance *cnsregistervolumev1alpha1.CnsRegisterVolume, taskID string) error {
	if instance.Status.CnsTaskID == taskID {
		return nil
	}
	return r.persistCnsExecution(ctx, instance, taskID, instance.Status.CnsVolumeID)
}

// setInstanceSuccess sets instance to success and records an event on the
// CnsRegisterVolume instance.
func setInstanceSuccess(ctx context.Context, r *ReconcileCnsRegisterVolume,
	instance *cnsregistervolumev1alpha1.CnsRegisterVolume, pvcName string, pvcUID apitypes.UID, msg string) error {
	log := logger.GetLogger(ctx)
	setInstanceOwnerRef(instance, pvcName, pvcUID)
	// Update the metadata (ownerReferences) first
	err := updateCnsRegisterVolume(ctx, r.client, instance)
	if err != nil {
		return err
	}
	// Fetch the latest version of the instance after updating metadata
	// to avoid conflict when updating status
	namespacedName := apitypes.NamespacedName{
		Name:      instance.Name,
		Namespace: instance.Namespace,
	}
	err = r.client.Get(ctx, namespacedName, instance)
	if err != nil {
		log.Errorf("Failed to fetch latest CnsRegisterVolume instance after metadata update. err: %v", err)
		return err
	}
	// Now update the status subresource with the fresh object
	origInstance := instance.DeepCopy()
	instance.Status.Registered = true
	instance.Status.Error = ""
	// Registered=true and Ready=True are published in the same write, so no
	// observer can ever see one without the other.
	applyConditions(instance, trueCondition(cnsregistervolumev1alpha1.ConditionReady,
		cnsregistervolumev1alpha1.ReasonRegistered, "Volume is registered"))
	err = patchCnsRegisterVolumeStatus(ctx, r.client, origInstance, instance)
	if err != nil {
		log.Errorf("patchCnsRegisterVolumeStatus failed. err: %v", err)
		return err
	}
	recordEvent(ctx, r, instance, v1.EventTypeNormal, msg)
	return nil
}

// setInstanceOwnerRef sets instance ownerRef to PVC instance that it created.
func setInstanceOwnerRef(instance *cnsregistervolumev1alpha1.CnsRegisterVolume, pvcName string,
	pvcUID apitypes.UID) {
	bController := true
	bOwnerDeletion := true
	kind := reflect.TypeOf(v1.PersistentVolumeClaim{}).Name()
	instance.OwnerReferences = []metav1.OwnerReference{
		{
			APIVersion:         "v1",
			Controller:         &bController,
			BlockOwnerDeletion: &bOwnerDeletion,
			Kind:               kind,
			Name:               pvcName,
			UID:                pvcUID,
		},
	}
}

// recordEvent records the event, sets the backOffDuration for the instance
// appropriately and logs the message.
// backOffDuration is reset to 1 second on success and doubled on failure.
func recordEvent(ctx context.Context, r *ReconcileCnsRegisterVolume,
	instance *cnsregistervolumev1alpha1.CnsRegisterVolume, eventtype string, msg string) {
	log := logger.GetLogger(ctx)
	log.Debugf("Event type is %s", eventtype)
	namespacedName := apitypes.NamespacedName{
		Name:      instance.Name,
		Namespace: instance.Namespace,
	}
	switch eventtype {
	case v1.EventTypeWarning:
		// Double backOff duration.
		backOffDurationMapMutex.Lock()
		backOffDuration[namespacedName] = min(backOffDuration[namespacedName]*2,
			cnsoperatortypes.MaxBackOffDurationForReconciler)
		r.recorder.Event(instance, v1.EventTypeWarning, "CnsRegisterVolumeFailed", msg)
		backOffDurationMapMutex.Unlock()
	case v1.EventTypeNormal:
		// Reset backOff duration to one second.
		backOffDurationMapMutex.Lock()
		backOffDuration[namespacedName] = time.Second
		r.recorder.Event(instance, v1.EventTypeNormal, "CnsRegisterVolumeSucceeded", msg)
		backOffDurationMapMutex.Unlock()
	}
}

// updateCnsRegisterVolume updates the CnsRegisterVolume instance in K8S.
func updateCnsRegisterVolume(ctx context.Context, client client.Client,
	instance *cnsregistervolumev1alpha1.CnsRegisterVolume) error {
	log := logger.GetLogger(ctx)
	err := client.Update(ctx, instance)
	if err != nil {
		log.Errorf("Failed to update CnsRegisterVolume instance: %q on namespace: %q. Error: %+v",
			instance.Name, instance.Namespace, err)
	}
	return err
}

// patchCnsRegisterVolumeStatus patches status field of CnsRegisterVolume instance in K8S.
// For status subresources with required fields, we need to ensure those fields are always
// included in the patch, even if they haven't changed. This is because Kubernetes validates
// the patch against the CRD schema which marks 'registered' as required.
func patchCnsRegisterVolumeStatus(ctx context.Context, cnsOperatorClient client.Client,
	oldObj *cnsregistervolumev1alpha1.CnsRegisterVolume,
	newObj *cnsregistervolumev1alpha1.CnsRegisterVolume) error {
	log := logger.GetLogger(ctx)

	// For status patches with required fields, we must always include 'registered' field
	// to satisfy CRD validation. For the 'error' field (optional with omitempty):
	// - Include it if it has a value
	// - Include it as empty string only if we're explicitly clearing it (oldObj had error, newObj doesn't)
	// - Omit it if both oldObj and newObj have no error (to avoid showing error:"" in status)
	statusMap := map[string]interface{}{
		"registered": newObj.Status.Registered,
	}

	// Include error field if:
	// 1. newObj has an error message, OR
	// 2. we're clearing an error (oldObj has error, newObj doesn't)
	if newObj.Status.Error != "" || (oldObj.Status.Error != "" && newObj.Status.Error == "") {
		statusMap["error"] = newObj.Status.Error
	}

	// Conditions are carried as a whole array, because a JSON merge patch
	// replaces arrays wholesale. The key is included only when a condition
	// actually changed; omitting it leaves the stored conditions untouched,
	// which is what keeps a write that is only about registered/error from
	// wiping conditions published by an earlier write.
	if !apiequality.Semantic.DeepEqual(oldObj.Status.Conditions, newObj.Status.Conditions) {
		statusMap["conditions"] = newObj.Status.Conditions
	}

	// cnsTaskID follows the same include-or-clear rule. Clearing it matters:
	// a stale task ID left behind would make the next reconcile poll a task
	// that is already finished with.
	if newObj.Status.CnsTaskID != oldObj.Status.CnsTaskID {
		statusMap["cnsTaskID"] = newObj.Status.CnsTaskID
	}

	if newObj.Status.CnsVolumeID != oldObj.Status.CnsVolumeID {
		statusMap["cnsVolumeID"] = newObj.Status.CnsVolumeID
	}
	statusPatch := map[string]interface{}{
		"status": statusMap,
	}

	patchBytes, err := json.Marshal(statusPatch)
	if err != nil {
		log.Errorf("failed to marshal status patch. err: %v", err)
		return err
	}

	rawPatch := client.RawPatch(apitypes.MergePatchType, patchBytes)

	// Try to patch CnsRegisterVolume CR for 3 times
	allowedRetries := 3
	attempt := 0
	for {
		attempt++
		err := cnsOperatorClient.Status().Patch(ctx, oldObj, rawPatch)
		if err != nil && attempt >= allowedRetries {
			log.Errorf("failed to patch CnsRegisterVolume instance %q on namespace %q, Error: %+v",
				oldObj.Name, oldObj.Namespace, err)
			return err
		} else if err == nil {
			// Patch decodes the API response into oldObj. Carry its version
			// forward so later metadata updates in this reconcile do not
			// conflict with our own condition/status writes.
			newObj.ResourceVersion = oldObj.ResourceVersion
			log.Debugf("Successfully patched CnsRegisterVolume instance %q on namespace %q",
				oldObj.Name, oldObj.Namespace)
			return nil
		}
		log.Warnf("attempt %d, failed to patch CnsRegisterVolume instance %q on namespace %q with error %+v, "+
			"will retry...", attempt, oldObj.Name, oldObj.Namespace, err)
		time.Sleep(100 * time.Millisecond)
	}
}

// reconcileDelete handles deletion of a CnsRegisterVolume instance.
// This function ensures that:
//  1. If registration hasn't started or failed early, we can safely delete without cleanup
//  2. If only PV was created, we can remove PV before allowing deletion
//  3. If CNS volume was created, we untag it (deleteDisk=false) before allowing deletion
//  4. If PVC and PV are bound, we can safely remove the finalizer and let
//     CNSUnregisterVolume workflow handle deletion of PVC and PV
//
// The finalizer is only removed after all cleanup is complete.
func (r *ReconcileCnsRegisterVolume) reconcileDelete(ctx context.Context,
	instance *cnsregistervolumev1alpha1.CnsRegisterVolume,
	request reconcile.Request, timeout time.Duration) (reconcile.Result, error) {
	log := logger.GetLogger(ctx).With("name", instance.Name).With("namespace", instance.Namespace)
	log.Info("Handling deletion of CnsRegisterVolume instance")

	// If already registered successfully, the PVC owns the lifecycle via ownerReference.
	// We can safely remove the finalizer and let Kubernetes garbage collection handle it.
	if instance.Status.Registered {
		log.Info("Instance already registered. Removing finalizer")
		err := removeFinalizer(ctx, r.client, instance)
		if err != nil {
			log.With("error", err).Error("Failed to remove finalizer")
			return reconcile.Result{RequeueAfter: timeout}, nil
		}
		backOffDurationMapMutex.Lock()
		delete(backOffDuration, request.NamespacedName)
		backOffDurationMapMutex.Unlock()
		return reconcile.Result{}, nil
	}

	// Recover a submission whose status write failed in this process before
	// deciding there is no outstanding work. The task must settle on deletion.
	if instance.Status.CnsTaskID == "" && instance.Status.CnsVolumeID == "" && r.taskTracker != nil {
		r.taskTracker.mu.Lock()
		pending := r.taskTracker.slots[instanceKey(instance)]
		r.taskTracker.mu.Unlock()
		if pending != "" {
			if err := setInstanceCnsTaskID(ctx, r, instance, pending); err != nil {
				return reconcile.Result{RequeueAfter: timeout}, nil
			}
		}
	}

	// If a CNS task is still in flight, the finalizer must stay: the task may
	// be about to create a volume, and dropping the finalizer now would delete
	// the CR and leave that volume behind with nothing referencing it.
	// Starting a second CNS operation on the same disk is also not an option,
	// so the only correct move is to wait for the task to settle.
	if instance.Status.CnsTaskID != "" || instance.Status.CnsVolumeID != "" {
		if res := r.resolveInFlightTaskForDelete(ctx, instance, request, timeout); res != nil {
			return *res, nil
		}
	}

	// If not registered, we need to clean up any partially created resources.

	// Step 1: Find the PV (if it exists) based on VolumeID or DiskURLPath
	var pv *v1.PersistentVolume
	var err error

	if instance.Spec.VolumeID != "" {
		pv, err = r.findPVByVolumeID(ctx, instance)
	} else if instance.Spec.DiskURLPath != "" {
		pv, err = r.findPVByDiskURLPath(ctx, instance)
	}

	if err != nil {
		log.With("error", err).Error("Failed to find PV")
		return reconcile.Result{RequeueAfter: timeout}, nil
	}

	// Step 2: If no PV found (or PV/PVC is bound), remove finalizer and complete.
	// A PV restored by another CnsRegisterVolume belongs to that registration and is left alone too.
	if pv == nil || isPVRestoredByOther(ctx, pv, instance) {
		log.Info("No unbound PV of this instance found. Removing finalizer")
		if err := removeFinalizer(ctx, r.client, instance); err != nil {
			log.With("error", err).Error("Failed to remove finalizer")
			return reconcile.Result{RequeueAfter: timeout}, nil
		}
		// Centralized backOffDuration cleanup
		backOffDurationMapMutex.Lock()
		delete(backOffDuration, request.NamespacedName)
		backOffDurationMapMutex.Unlock()
		log.Info("Successfully handled deletion")
		return reconcile.Result{}, nil
	}

	// Step 3: Clean up unbound PV (pv is guaranteed to be unbound at this point)
	if err := r.cleanupUnboundPV(ctx, pv); err != nil {
		log.With("error", err).Error("Failed to cleanup unbound PV")
		return reconcile.Result{RequeueAfter: timeout}, nil
	}

	// Step 4: Cleanup CNS volume registration
	// Extract volumeID from instance spec or PV spec
	volumeID := instance.Spec.VolumeID
	if volumeID == "" && pv.Spec.CSI != nil {
		volumeID = pv.Spec.CSI.VolumeHandle
	}

	if volumeID != "" {
		err := r.cleanupCNSVolume(ctx, instance, volumeID)
		if err != nil {
			log.With("error", err).Error("Failed to cleanup CNS volume")
			return reconcile.Result{RequeueAfter: timeout}, nil
		}
	}

	// All cleanup complete, remove the finalizer.
	log.Info("Cleanup complete. Removing finalizer")
	if err := removeFinalizer(ctx, r.client, instance); err != nil {
		log.With("error", err).Error("Failed to remove finalizer")
		return reconcile.Result{RequeueAfter: timeout}, nil
	}

	backOffDurationMapMutex.Lock()
	delete(backOffDuration, request.NamespacedName)
	backOffDurationMapMutex.Unlock()
	log.Info("Successfully handled deletion")
	return reconcile.Result{}, nil
}

// protectInstance adds finalizer to the CnsRegisterVolume instance to handle deletion.
func protectInstance(ctx context.Context, c client.Client, obj *cnsregistervolumev1alpha1.CnsRegisterVolume) error {
	log := logger.GetLogger(ctx).With("name", obj.Name).With("namespace", obj.Namespace)

	if controllerutil.ContainsFinalizer(obj, cnsoperatortypes.CNSRegisterVolumeFinalizer) {
		log.With("finalizer", cnsoperatortypes.CNSRegisterVolumeFinalizer).
			Debug("Finalizer already exists")
		return nil
	}

	log.With("finalizer", cnsoperatortypes.CNSRegisterVolumeFinalizer).Info("Adding finalizer")
	controllerutil.AddFinalizer(obj, cnsoperatortypes.CNSRegisterVolumeFinalizer)
	return c.Update(ctx, obj)
}

// removeFinalizer removes finalizer from the CnsRegisterVolume instance to allow deletion.
func removeFinalizer(ctx context.Context, c client.Client, obj *cnsregistervolumev1alpha1.CnsRegisterVolume) error {
	log := logger.GetLogger(ctx).With("name", obj.Name).With("namespace", obj.Namespace)

	if !controllerutil.ContainsFinalizer(obj, cnsoperatortypes.CNSRegisterVolumeFinalizer) {
		log.With("finalizer", cnsoperatortypes.CNSRegisterVolumeFinalizer).
			Debug("Finalizer does not exist")
		return nil
	}

	log.With("finalizer", cnsoperatortypes.CNSRegisterVolumeFinalizer).Info("Removing finalizer")
	controllerutil.RemoveFinalizer(obj, cnsoperatortypes.CNSRegisterVolumeFinalizer)
	return c.Update(ctx, obj)
}

// findPVByVolumeID finds a PV by constructing its name from the VolumeID.
// Returns: pv (unbound PV if found, nil otherwise), error
// - pv=nil, err=nil: no unbound PV found (PV not found or bound), caller should remove finalizer
// - pv!=nil, err=nil: unbound PV found, caller should clean it up
// - err!=nil: error occurred, caller should requeue
func (r *ReconcileCnsRegisterVolume) findPVByVolumeID(
	ctx context.Context,
	instance *cnsregistervolumev1alpha1.CnsRegisterVolume,
) (*v1.PersistentVolume, error) {
	log := logger.GetLogger(ctx)
	pvName := staticPvNamePrefix + instance.Spec.VolumeID

	pv, err := k8s.GetPersistentVolume(ctx, r.k8sclient, pvName)
	if err != nil {
		return nil, err
	}
	if pv == nil {
		// PV doesn't exist, nothing to clean up
		return nil, nil
	}

	// PV exists - check if it's bound
	if pv.Status.Phase == v1.VolumeBound {
		// PV is bound, return nil to indicate no cleanup needed (caller will remove finalizer)
		log.With("pvName", pvName).Info("PV is bound, no cleanup needed")
		return nil, nil
	}

	return pv, nil
}

// findPVByDiskURLPath finds a PV by checking PVC binding status or searching by labels.
// Returns: pv (unbound PV if found, nil otherwise), error
// - pv=nil, err=nil: no unbound PV found (PVC/PV bound or not found), caller should remove finalizer
// - pv!=nil, err=nil: unbound PV found, caller should clean it up
// - err!=nil: error occurred, caller should requeue
func (r *ReconcileCnsRegisterVolume) findPVByDiskURLPath(ctx context.Context,
	instance *cnsregistervolumev1alpha1.CnsRegisterVolume,
) (*v1.PersistentVolume, error) {

	log := logger.GetLogger(ctx)

	// Check if PVC exists and is bound
	pvc, err := r.k8sclient.CoreV1().PersistentVolumeClaims(instance.Namespace).Get(ctx,
		instance.Spec.PvcName, metav1.GetOptions{})
	if err != nil && !apierrors.IsNotFound(err) {
		// Error getting PVC (other than NotFound)
		log.With("pvcName", instance.Spec.PvcName).With("error", err).Error("Failed to get PVC")
		return nil, err
	}

	if err == nil && pvc.Status.Phase == v1.ClaimBound && pvc.Spec.VolumeName != "" {
		// PVC is bound - registration was successful, no cleanup needed
		log.With("pvcName", instance.Spec.PvcName).With("pvName", pvc.Spec.VolumeName).
			Info("PVC is bound to PV. Registration completed successfully")
		return nil, nil
	}

	// PVC doesn't exist or is not bound - search for PV by labels
	return r.searchPVByLabels(ctx, instance)
}

// searchPVByLabels searches for a PV created by this CR using label selectors.
// Returns: pv (unbound PV if found, nil otherwise), error
// - pv=nil, err=nil: no unbound PV found (PV bound or not found), caller should remove finalizer
// - pv!=nil, err=nil: unbound PV found, caller should clean it up
// - err!=nil: error occurred, caller should requeue
func (r *ReconcileCnsRegisterVolume) searchPVByLabels(ctx context.Context,
	instance *cnsregistervolumev1alpha1.CnsRegisterVolume,
) (*v1.PersistentVolume, error) {

	log := logger.GetLogger(ctx)

	listOptions := metav1.ListOptions{
		LabelSelector: fmt.Sprintf("%s=%s,%s=%s,%s=%s",
			labelCnsRegisterVolumeCreatedBy, labelCnsRegisterVolumeCreatedByValue,
			labelCnsRegisterVolumeCRNamespace, instance.Namespace,
			labelCnsRegisterVolumeCRName, instance.Name),
	}

	pvList, err := k8s.ListPersistentVolumes(ctx, r.k8sclient, listOptions)
	if err != nil {
		return nil, err
	}

	if len(pvList.Items) == 0 {
		return nil, nil
	}

	candidatePV := &pvList.Items[0]

	// Verify it matches our expected PVC (defense in depth)
	if candidatePV.Spec.ClaimRef == nil ||
		candidatePV.Spec.ClaimRef.Name != instance.Spec.PvcName ||
		candidatePV.Spec.ClaimRef.Namespace != instance.Namespace ||
		candidatePV.Spec.CSI == nil ||
		candidatePV.Spec.CSI.Driver != cnsoperatortypes.VSphereCSIDriverName {
		log.With("pvName", candidatePV.Name).With("expectedPVC", instance.Spec.PvcName).
			Warn("Found PV with CR labels but ClaimRef mismatch")
		return nil, nil
	}

	// Check if PV is bound
	if candidatePV.Status.Phase == v1.VolumeBound {
		log.With("pvName", candidatePV.Name).With("pvcName", instance.Spec.PvcName).
			Info("Found bound PV. Registration completed successfully")
		return nil, nil
	}

	// PV is unbound - return for cleanup
	log.With("pvName", candidatePV.Name).With("phase", candidatePV.Status.Phase).
		Info("Found unbound PV")
	return candidatePV, nil
}

// cleanupUnboundPV sets the PV reclaim policy to Retain and deletes the PV.
// Returns: error (nil on success)
func (r *ReconcileCnsRegisterVolume) cleanupUnboundPV(ctx context.Context,
	pv *v1.PersistentVolume) error {

	log := logger.GetLogger(ctx)
	pvName := pv.Name

	// Set reclaim policy to Retain to prevent deletion of the underlying CNS volume
	log.With("pvName", pvName).With("currentReclaimPolicy", pv.Spec.PersistentVolumeReclaimPolicy).
		Info("Setting PV reclaim policy to Retain")
	if err := k8s.RetainPersistentVolume(ctx, r.k8sclient, pvName); err != nil && !apierrors.IsNotFound(err) {
		return err
	}

	// Delete the unbound PV
	log.With("pvName", pvName).With("phase", pv.Status.Phase).Info("Deleting unbound PV")
	if err := k8s.DeletePersistentVolume(ctx, r.k8sclient, pvName); err != nil && !apierrors.IsNotFound(err) {
		return err
	}
	return nil
}

// untagCNSVolume untags the CNS volume (deleteDisk=false to preserve the underlying disk).
// Returns: error (nil on success)
func (r *ReconcileCnsRegisterVolume) untagCNSVolume(ctx context.Context, volumeID string) error {
	log := logger.GetLogger(ctx)

	log.With("volumeID", volumeID).Info("Untagging CNS volume (deleteDisk=false)")
	if _, err := common.DeleteVolumeUtil(ctx, r.volumeManager, volumeID, false); err != nil {
		log.With("volumeID", volumeID).With("error", err).Error("Failed to untag CNS volume")
		return err
	}
	log.With("volumeID", volumeID).Info("Successfully untagged CNS volume")
	return nil
}

// cleanupCNSVolume cleans up a CNS volume created during a failed registration.
// For DiskURLPath registrations (legacy VMDK promoted to FCD), it unregisters the
// FCD to convert the disk back to a legacy VMDK. For VolumeID registrations (existing
// FCD), it only untags the CNS record, preserving the underlying FCD.
func (r *ReconcileCnsRegisterVolume) cleanupCNSVolume(
	ctx context.Context,
	instance *cnsregistervolumev1alpha1.CnsRegisterVolume,
	volumeID string,
) error {
	if isSnapServiceLocalProtectionEnabled && r.volumeInfoService != nil {
		owner, err := r.restoreOwner(ctx, volumeID)
		if err != nil {
			return err
		}
		if owner != "" && owner != string(instance.UID) {
			return nil
		}
	}
	var err error
	if instance.Spec.DiskURLPath != "" {
		err = r.unregisterFCD(ctx, volumeID)
	} else {
		err = r.untagCNSVolume(ctx, volumeID)
	}
	if err != nil {
		return err
	}
	if instance.Status.CnsVolumeID != "" {
		if err := r.persistCnsExecution(ctx, instance, "", ""); err != nil {
			return err
		}
	}
	return r.releaseRestoreReservation(ctx, instance)
}

// unregisterFCD de-registers a volume as FCD, converting it back to a legacy disk.
// This is used when the volume was originally registered from a DiskURLPath.
func (r *ReconcileCnsRegisterVolume) unregisterFCD(ctx context.Context, volumeID string) error {
	log := logger.GetLogger(ctx)

	log.With("volumeID", volumeID).Info("Unregistering FCD (unregDisk=true)")
	if _, err := r.volumeManager.UnregisterVolume(ctx, volumeID, true); err != nil {
		log.With("volumeID", volumeID).With("error", err).Error("Failed to unregister FCD")
		return err
	}
	log.With("volumeID", volumeID).Info("Successfully unregistered FCD")
	return nil
}
