/*
Copyright 2025 The Kubernetes Authors.

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

package virtualmachinesnapshot

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	vmoperatorv1alpha4 "github.com/vmware-tanzu/vm-operator/api/v1alpha4"
	cnstypes "github.com/vmware/govmomi/cns/types"
	"go.uber.org/zap"
	corev1 "k8s.io/api/core/v1"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/runtime"
	apitypes "k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes/scheme"
	typedcorev1 "k8s.io/client-go/kubernetes/typed/core/v1"
	"k8s.io/client-go/tools/record"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	"sigs.k8s.io/controller-runtime/pkg/manager"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
	"sigs.k8s.io/controller-runtime/pkg/source"
	apis "sigs.k8s.io/vsphere-csi-driver/v3/pkg/apis/cnsoperator"
	volumes "sigs.k8s.io/vsphere-csi-driver/v3/pkg/common/cns-lib/volume"
	commonconfig "sigs.k8s.io/vsphere-csi-driver/v3/pkg/common/config"
	"sigs.k8s.io/vsphere-csi-driver/v3/pkg/common/utils"
	"sigs.k8s.io/vsphere-csi-driver/v3/pkg/csi/service/common"
	"sigs.k8s.io/vsphere-csi-driver/v3/pkg/csi/service/common/commonco"
	"sigs.k8s.io/vsphere-csi-driver/v3/pkg/csi/service/logger"
	"sigs.k8s.io/vsphere-csi-driver/v3/pkg/internalapis/cnsvolumeinfo"
	k8s "sigs.k8s.io/vsphere-csi-driver/v3/pkg/kubernetes"
	"sigs.k8s.io/vsphere-csi-driver/v3/pkg/syncer"
)

const (
	defaultMaxWorkerThreadsForVirtualMachineSnapshot = 10
	allowedRetriesToPatchCNSVolumeInfo               = 5
	SyncVolumeFinalizer                              = "cns.vmware.com/syncvolume"
	VMSnapshotFinalizer                              = "vmoperator.vmware.com/virtualmachinesnapshot"
)

var (
	// backOffDuration is a map of virtualmachinesnapshot name's to the time after which
	// a request for this instance will be requeued.
	// Initialized to 1 second for new instances and for instances whose latest
	// reconcile operation succeeded.
	// If the reconcile fails, backoff is incremented exponentially.
	backOffDuration         map[apitypes.NamespacedName]time.Duration
	backOffDurationMapMutex = sync.Mutex{}
)

// Add creates a new VirtualMachineSnapshot Controller and adds it to the Manager,
// ConfigurationInfo and VirtualCenterTypes. The Manager will set fields on the
// Controller and start it when the Manager is Started.
func Add(mgr manager.Manager, clusterFlavor cnstypes.CnsClusterFlavor,
	configInfo *commonconfig.ConfigurationInfo, volumeManager volumes.Manager) error {
	ctx, log := logger.GetNewContextWithLogger()

	var coCommonInterface commonco.COCommonInterface
	var err error
	var volumeInfoService cnsvolumeinfo.VolumeInfoService
	// VirtualMachineSnapshot quota validation is only supported on WCP.
	if clusterFlavor == cnstypes.CnsClusterFlavorWorkload {
		coCommonInterface, err = commonco.GetContainerOrchestratorInterface(ctx,
			common.Kubernetes, clusterFlavor, &syncer.COInitParams)
		if err != nil {
			log.Errorf("failed to create CO agnostic interface. error: %v", err)
			return err
		}
		var err error
		if !coCommonInterface.IsFSSEnabled(ctx, common.WCPVMServiceVMSnapshots) {
			log.Info("Not initializing the VirtualMachineSnapshot Controller as " +
				"this feature is disabled on the cluster")
			return nil
		}
		log.Info("Creating CnsVolumeInfo Service to persist mapping for VolumeID to storage policy info")
		volumeInfoService, err = cnsvolumeinfo.InitVolumeInfoService(ctx)
		if err != nil {
			return logger.LogNewErrorf(log, "error initializing volumeInfoService. error: %+v", err)
		}
		log.Info("Successfully initialized VolumeInfoService")
	} else {
		log.Info("Not initializing VirtualMachineSnapshot Controller as guest/vanilla cluster is detected.")
		return nil
	}
	// Initializes kubernetes client.
	k8sclient, err := k8s.NewClient(ctx)
	if err != nil {
		log.Errorf("Creating Kubernetes client failed. error: %v", err)
		return err
	}
	restClientConfig, err := k8s.GetKubeConfig(ctx)
	if err != nil {
		msg := fmt.Sprintf("Failed to initialize rest clientconfig. error: %+v", err)
		log.Error(msg)
		return err
	}
	vmOperatorClient, err := k8s.NewClientForGroup(ctx, restClientConfig, vmoperatorv1alpha4.GroupName)
	if err != nil {
		msg := fmt.Sprintf("Failed to initialize vmOperatorClient. error: %+v", err)
		log.Error(msg)
		return err
	}

	// eventBroadcaster broadcasts events on virtualmachinesnapshot instances to the
	// event sink.
	eventBroadcaster := record.NewBroadcaster()
	eventBroadcaster.StartRecordingToSink(
		&typedcorev1.EventSinkImpl{
			Interface: k8sclient.CoreV1().Events(""),
		},
	)
	recorder := eventBroadcaster.NewRecorder(scheme.Scheme, corev1.EventSource{Component: apis.GroupName})
	return add(mgr, newReconciler(mgr, configInfo, volumeManager,
		recorder, vmOperatorClient, volumeInfoService))
}

// newReconciler returns a new reconcile.Reconciler.
func newReconciler(mgr manager.Manager, configInfo *commonconfig.ConfigurationInfo,
	volumeManager volumes.Manager, recorder record.EventRecorder, vmOperatorClient client.Client,
	volumeInfoService cnsvolumeinfo.VolumeInfoService) reconcile.Reconciler {
	return &ReconcileVirtualMachineSnapshot{
		client:            mgr.GetClient(),
		scheme:            mgr.GetScheme(),
		configInfo:        configInfo,
		volumeManager:     volumeManager,
		recorder:          recorder,
		vmOperatorClient:  vmOperatorClient,
		volumeInfoService: volumeInfoService,
	}
}

// add adds a new Controller to mgr with r as the reconcile.Reconciler.
func add(mgr manager.Manager, r reconcile.Reconciler) error {
	ctx, log := logger.GetNewContextWithLogger()
	maxWorkerThreads := getMaxWorkerThreadsToReconcileVirtualMachineSnapshot(ctx)
	// Create a new controller.
	c, err := controller.New("virtualmachinesnapshot-controller", mgr,
		controller.Options{Reconciler: r, MaxConcurrentReconciles: maxWorkerThreads})
	if err != nil {
		log.Errorf("Failed to create new VirtualMachineSnapshot controller with error: %+v", err)
		return err
	}
	backOffDuration = make(map[apitypes.NamespacedName]time.Duration)
	// Watch for changes to primary resource VirtualMachineSnapshot.
	err = c.Watch(source.Kind(mgr.GetCache(),
		&vmoperatorv1alpha4.VirtualMachineSnapshot{},
		&handler.TypedEnqueueRequestForObject[*vmoperatorv1alpha4.VirtualMachineSnapshot]{}))
	if err != nil {
		log.Errorf("Failed to watch for changes to VirtualMachineSnapshot resource with error: %+v", err)
		return err
	}
	return nil
}

// blank assignment to verify that ReconcileVirtualMachineSnapshot implements
// reconcile.Reconciler.
var _ reconcile.Reconciler = &ReconcileVirtualMachineSnapshot{}

// ReconcileVirtualMachineSnapshot reconciles a VirtualMachineSnapshot object.
type ReconcileVirtualMachineSnapshot struct {
	// This client, initialized using mgr.Client() above, is a split client
	// that reads objects from the cache and writes to the apiserver.
	client            client.Client
	scheme            *runtime.Scheme
	configInfo        *commonconfig.ConfigurationInfo
	volumeManager     volumes.Manager
	recorder          record.EventRecorder
	volumeInfoService cnsvolumeinfo.VolumeInfoService
	vmOperatorClient  client.Client
}

// Reconcile reads that state of the cluster for a VirtualMachineSnapshot object and
// makes changes based on the state read and what is in VirtualMachineSnapshot.Spec.
// Note:
// The Controller will requeue the Request to be processed again if the returned
// error is non-nil or Result.Requeue is true. Otherwise, upon completion it
// will remove the work from the queue.
func (r *ReconcileVirtualMachineSnapshot) Reconcile(ctx context.Context,
	request reconcile.Request) (reconcile.Result, error) {
	_, log := logger.GetNewContextWithLogger()
	now := time.Now()
	log.Infof("Reconcile Started for VirtualMachineSnapshot %s/%s", request.Namespace, request.Name)
	defer func() {
		log.Infof("Reconcile Completed for virtualmachinesnapshot %s/%s Time Taken %v",
			request.Namespace, request.Name, time.Since(now))
	}()
	// Fetch the VirtualMachineSnapshot instance.
	vmSnapshot := &vmoperatorv1alpha4.VirtualMachineSnapshot{}
	err := r.client.Get(ctx, request.NamespacedName, vmSnapshot)
	if err != nil {
		if apierrors.IsNotFound(err) {
			log.Infof("resource not found. Ignoring since object must be deleted for vmsnapshot %s/%s",
				request.Namespace, request.Name)
			return reconcile.Result{}, nil
		}
		log.Errorf("error while fetch the virtualmachinesnapshot %s/%s. error: %v",
			request.Namespace, request.Name, err)
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

	log.Infof("Reconciling virtualmachinesnapshot %s/%s",
		request.Namespace, request.Name)
	err = r.reconcileNormal(ctx, log, vmSnapshot)
	if err != nil {
		return reconcile.Result{RequeueAfter: timeout}, err
	}
	backOffDurationMapMutex.Lock()
	delete(backOffDuration, request.NamespacedName)
	backOffDurationMapMutex.Unlock()
	return reconcile.Result{}, nil
}
func (r *ReconcileVirtualMachineSnapshot) reconcileNormal(ctx context.Context, log *zap.SugaredLogger,
	vmsnapshot *vmoperatorv1alpha4.VirtualMachineSnapshot) error {
	deleteVMSnapshot := false
	if vmsnapshot.DeletionTimestamp.IsZero() {
		// If the finalizer is not present, add it.
		log.Infof("reconcileNormal: Adding finalizer %s on virtualmachinesnapshot cr %s/%s",
			SyncVolumeFinalizer, vmsnapshot.Namespace, vmsnapshot.Name)
		vmSnapshotPatch := client.MergeFrom(vmsnapshot.DeepCopy())
		if controllerutil.AddFinalizer(vmsnapshot, SyncVolumeFinalizer) {
			err := r.client.Patch(ctx, vmsnapshot, vmSnapshotPatch)
			if err != nil {
				log.Errorf("reconcileNormal: error while add finalizer to "+
					"virtualmachinesnapshot %s/%s. error: %v", vmsnapshot.Name, vmsnapshot.Name, err)
				return err
			}
			return nil
		}
	}
	if !vmsnapshot.DeletionTimestamp.IsZero() &&
		controllerutil.ContainsFinalizer(vmsnapshot, SyncVolumeFinalizer) {
		if !controllerutil.ContainsFinalizer(vmsnapshot, VMSnapshotFinalizer) {
			log.Infof("reconcileNormal: virtualmachinesnapshot %s/%s is set to delete",
				vmsnapshot.Namespace, vmsnapshot.Name)
			deleteVMSnapshot = true
		} else {
			log.Infof("reconcileNormal: virtualmachinesnapshot %s/%s is set to delete, "+
				"expecting to remove %s first", vmsnapshot.Namespace, vmsnapshot.Name,
				VMSnapshotFinalizer)
			return nil
		}
	}
	// Check for the annotation "csi.vsphere.volume.sync: Requested"
	syncVolumeAnnotation := strings.ToLower(vmsnapshot.Annotations["csi.vsphere.volume.sync"])
	// process quota validation if annotation value is "Requested"
	// annotation value is set to "Requested" by vm-service when snapshot is completed successfully.
	if syncVolumeAnnotation == "requested" || deleteVMSnapshot {
		// if found fetch vmsnapshot and pvcs and pvs
		vmKey := apitypes.NamespacedName{
			Namespace: vmsnapshot.Namespace,
			Name:      vmsnapshot.Spec.VMRef.Name,
		}
		log.Infof("reconcileNormal: get virtulal machine %s/%s", vmKey.Namespace, vmKey.Name)
		virtualMachine, _, err := utils.GetVirtualMachineAllApiVersions(ctx, vmKey,
			r.vmOperatorClient)
		if err != nil {
			log.Errorf("reconcileNormal: could not get VirtualMachine %s/%s. error: %v",
				vmKey.Namespace, vmKey.Name, err)
			return err
		}
		log.Infof("reconcileNormal: sync and update storage quota for vmsnapshot %s/%s",
			vmsnapshot.Namespace, vmsnapshot.Name)
		err = r.syncVolumesAndUpdateCNSVolumeInfo(ctx, log, virtualMachine)
		if err != nil {
			log.Errorf("reconcileNormal: failed to validate VirtualMachineSnapshot %s/%s. error: %v",
				vmsnapshot.Namespace, vmsnapshot.Name, err)
			return err
		}
		log.Infof("reconcileNormal: successfully synced and updated storage quota for vmsnapshot %s/%s",
			vmsnapshot.Namespace, vmsnapshot.Name)
		if deleteVMSnapshot {
			log.Infof("reconcileNormal: remove finalizer %s for virtualmachinesnapshot %s/%s",
				SyncVolumeFinalizer, vmsnapshot.Namespace, vmsnapshot.Name)
			vmSnapshotPatch := client.MergeFrom(vmsnapshot.DeepCopy())
			if controllerutil.RemoveFinalizer(vmsnapshot, SyncVolumeFinalizer) {
				err = r.client.Patch(ctx, vmsnapshot, vmSnapshotPatch)
				if err != nil {
					log.Errorf("reconcileNormal: failed to remove finalizer for "+
						"virtualmachinesnapshot %s/%s. error: %v", vmsnapshot.Namespace,
						vmsnapshot.Name, err)
					return err
				}
				return nil
			}
		}
		// Update VMSnapshot CR annotation to "csi.vsphere.volume.sync: completed"
		log.Infof("reconcileNormal: update annotation value  for vmsnapshot %s/%s to 'completed'",
			vmsnapshot.Namespace, vmsnapshot.Name)
		vmSnapshotPatch := client.MergeFrom(vmsnapshot.DeepCopy())
		vmsnapshot.Annotations["csi.vsphere.volume.sync"] = "completed"
		err = r.client.Patch(ctx, vmsnapshot, vmSnapshotPatch)
		if err != nil {
			log.Errorf("reconcileNormal: could not update virtualmachinesnapshot %s/%s. error: %v",
				vmsnapshot.Namespace, vmsnapshot.Name, err)
			return err
		}
		log.Infof("reconcileNormal: successfully updated vmsnapshot %s/%s",
			vmsnapshot.Namespace, vmsnapshot.Name)
	}
	return nil
}

// getMaxWorkerThreadsToReconcileVirtualMachineSnapshot returns the maximum number
// of worker threads which can be run to reconcile VirtualMachineSnapshot instances.
// If environment variable WORKER_THREADS_VIRTUAL_MACHINE_SNAPSHOT is set and valid,
// return the value read from environment variable. Otherwise, use the default
// value.
func getMaxWorkerThreadsToReconcileVirtualMachineSnapshot(ctx context.Context) int {
	log := logger.GetLogger(ctx)
	workerThreads := defaultMaxWorkerThreadsForVirtualMachineSnapshot
	envVal := os.Getenv("WORKER_THREADS_VIRTUAL_MACHINE_SNAPSHOT")
	if envVal == "" {
		log.Debugf("WORKER_THREADS_VIRTUAL_MACHINE_SNAPSHOT is not set. Picking the default value %d",
			defaultMaxWorkerThreadsForVirtualMachineSnapshot)
		return workerThreads
	}
	value, err := strconv.Atoi(envVal)
	if err != nil {
		log.Warnf("Invalid value for WORKER_THREADS_VIRTUAL_MACHINE_SNAPSHOT: %s. Using default value %d",
			envVal, defaultMaxWorkerThreadsForVirtualMachineSnapshot)
		return workerThreads
	}
	switch {
	case value <= 0 || value > defaultMaxWorkerThreadsForVirtualMachineSnapshot:
		log.Warnf("Value %s for WORKER_THREADS_VIRTUAL_MACHINE_SNAPSHOT is invalid. Using default value %d",
			envVal, defaultMaxWorkerThreadsForVirtualMachineSnapshot)
	default:
		workerThreads = value
		log.Debugf("Maximum number of worker threads to reconcile VirtualMachineSnapshot is set to %d",
			workerThreads)
	}
	return workerThreads
}

// syncVolumesAndUpdateCNSVolumeInfo will fetch the volume-ids attached to virtualmachine
// will call SyncVolume API with sync mode SPACE_USAGE and volume-id list
// after volume sync is successful it will fetch the aggregated size of all related volumes
// will update the relevant CNSVolumeInfo for each volume which will update the storage policy usage.
func (r *ReconcileVirtualMachineSnapshot) syncVolumesAndUpdateCNSVolumeInfo(ctx context.Context,
	log *zap.SugaredLogger, vm *vmoperatorv1alpha4.VirtualMachine) error {
	var err error
	cnsVolumeIds := []cnstypes.CnsVolumeId{}
	syncMode := []string{string(cnstypes.CnsSyncVolumeModeSPACE_USAGE)}
	for _, vmVolume := range vm.Spec.Volumes {
		pvcKey := apitypes.NamespacedName{
			Namespace: vm.Namespace,
			Name:      vmVolume.Name,
		}
		pvc := &corev1.PersistentVolumeClaim{}
		err = r.client.Get(ctx, pvcKey, pvc, &client.GetOptions{})
		if err != nil {
			log.Errorf("syncVolumesAndUpdateCNSVolumeInfo: failed get pvc %s/%s. error: %v",
				vm.Namespace, vmVolume.Name, err)
			return err
		}
		if pvc.Spec.VolumeName != "" {
			pvKey := apitypes.NamespacedName{
				Name: pvc.Spec.VolumeName,
			}
			pv := &corev1.PersistentVolume{}
			err = r.client.Get(ctx, pvKey, pv, &client.GetOptions{})
			if err != nil {
				log.Errorf("syncVolumesAndUpdateCNSVolumeInfo: could not get the volume "+
					"for pvc %s/%s error: %v", pvc.Namespace, vm.Name, err)
				return err
			}
			if pv.Spec.CSI != nil && pv.Spec.CSI.VolumeHandle != "" {
				cnsVolId := cnstypes.CnsVolumeId{Id: pv.Spec.CSI.VolumeHandle}
				cnsVolumeIds = append(cnsVolumeIds, cnsVolId)
				syncVolumeSpecs := []cnstypes.CnsSyncVolumeSpec{
					{
						VolumeId: cnsVolId,
						SyncMode: syncMode,
					},
				}
				// Trigger CNS VolumeSync API for identified volume-lds and Fetch Latest Aggregated snapshot size
				log.Infof("syncVolumesAndUpdateCNSVolumeInfo: Trigger CNS VolumeSync API for volume %s",
					cnsVolId)
				syncVolumeFaultType, err := r.volumeManager.SyncVolume(ctx, syncVolumeSpecs)
				if err != nil {
					log.Errorf("syncVolumesAndUpdateCNSVolumeInfo: error while sync volume %s "+
						"cnsfault %s. error: %v", cnsVolId, syncVolumeFaultType, err)
					return err
				}
			}
		} else {
			err = fmt.Errorf("could not find the PV associated with PVC %s/%s",
				vm.Namespace, vmVolume.Name)
			log.Errorf("syncVolumesAndUpdateCNSVolumeInfo: pv not found error: %v", err)
			return err
		}
	}
	// fetch updated cns volumes
	queryFilter := cnstypes.CnsQueryFilter{
		VolumeIds: cnsVolumeIds,
	}
	queryResult, err := r.volumeManager.QueryVolume(ctx, queryFilter)
	if err != nil {
		log.Errorf("syncVolumesAndUpdateCNSVolumeInfo: error while query volumes from cns. error: %v", err)
		return err
	}
	if queryResult != nil && len(queryResult.Volumes) > 0 {
		for _, cnsvolume := range queryResult.Volumes {
			val, ok := cnsvolume.BackingObjectDetails.(*cnstypes.CnsBlockBackingDetails)
			if ok {
				log.Infof("syncVolumesAndUpdateCNSVolumeInfo: fetched aggregated capacity for volume %s "+
					"AggregatedSnapshotCapacityInMb %s", cnsvolume.VolumeId.Id, val.AggregatedSnapshotCapacityInMb)

				//  Update CNSVolumeInfo with latest aggregated Size and Update SPU used value.
				patch, err := common.GetCNSVolumeInfoPatch(ctx, val.AggregatedSnapshotCapacityInMb,
					cnsvolume.VolumeId.Id) // TODO: UDPATE to value returned
				if err != nil {
					log.Errorf("syncVolumesAndUpdateCNSVolumeInfo: failed to get cnsvolumeinfo patch for "+
						"volume %s, error: %v", cnsvolume.VolumeId.Id, err)
					return err
				}
				patchBytes, err := json.Marshal(patch)
				if err != nil {
					log.Errorf("syncVolumesAndUpdateCNSVolumeInfo: error while json marshal. error: %v", err)
					return err
				}
				err = r.volumeInfoService.PatchVolumeInfo(ctx, cnsvolume.VolumeId.Id,
					patchBytes, allowedRetriesToPatchCNSVolumeInfo)
				if err != nil {
					log.Errorf("syncVolumesAndUpdateCNSVolumeInfo: failed to patch cnsvolumeinfo for volume "+
						"volume %s, error: %v", cnsvolume.VolumeId.Id, err)
					return err
				}
			} else {
				err = fmt.Errorf("unable to retrieve CnsBlockBackingDetails for volumeID %s",
					cnsvolume.VolumeId.Id)
				log.Errorf("syncVolumesAndUpdateCNSVolumeInfo: could not retrieve CnsBlockBackingDetails. "+
					"error: %v", err)
				return err
			}
		}
	}
	return nil
}
