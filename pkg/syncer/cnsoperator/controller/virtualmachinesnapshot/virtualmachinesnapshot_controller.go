/*
Copyright 2019 The Kubernetes Authors.

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
	"errors"
	"fmt"
	"os"
	"strconv"
	"sync"
	"time"

	vmoperatorv1alpha4 "github.com/vmware-tanzu/vm-operator/api/v1alpha4"
	cnstypes "github.com/vmware/govmomi/cns/types"
	v1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	apitypes "k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes/scheme"
	typedcorev1 "k8s.io/client-go/kubernetes/typed/core/v1"
	"k8s.io/client-go/tools/record"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	"sigs.k8s.io/controller-runtime/pkg/manager"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
	"sigs.k8s.io/controller-runtime/pkg/source"
	apis "sigs.k8s.io/vsphere-csi-driver/v3/pkg/apis/cnsoperator"
	volumes "sigs.k8s.io/vsphere-csi-driver/v3/pkg/common/cns-lib/volume"
	"sigs.k8s.io/vsphere-csi-driver/v3/pkg/common/config"
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
	defaultMaxWorkerThreadsForVirtualMachineSnapshot = 1
	allowedRetriesToPatchCNSVolumeInfo               = 5
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
	configInfo *config.ConfigurationInfo, volumeManager volumes.Manager) error {
	ctx, log := logger.GetNewContextWithLogger()

	var coCommonInterface commonco.COCommonInterface
	var err error
	if clusterFlavor == cnstypes.CnsClusterFlavorWorkload {
		coCommonInterface, err = commonco.GetContainerOrchestratorInterface(ctx,
			common.Kubernetes, clusterFlavor, &syncer.COInitParams)
		if err != nil {
			log.Errorf("failed to create CO agnostic interface. Err: %v", err)
			return err
		}
	} else {
		log.Infof("Not initializing the VirtualMachineSnapshot Controller as stretched supervisor is detected.")
		return nil
	}

	var volumeInfoService cnsvolumeinfo.VolumeInfoService
	if clusterFlavor == cnstypes.CnsClusterFlavorWorkload {
		if commonco.ContainerOrchestratorUtility.IsFSSEnabled(ctx, common.TKGsHA) {
			var err error
			clusterComputeResourceMoIds, _, err := common.GetClusterComputeResourceMoIds(ctx)
			if err != nil {
				log.Errorf("failed to get clusterComputeResourceMoIds. err: %v", err)
				return err
			}
			if syncer.IsPodVMOnStretchSupervisorFSSEnabled {
				if !coCommonInterface.IsFSSEnabled(ctx, common.WCPVMServiceVMSnapshots) {
					log.Infof("Not initializing the VirtualMachineSnapshot Controller as this feature is disabled on the cluster")
					return nil
				}
				log.Info("Creating CnsVolumeInfo Service to persist mapping for VolumeID to storage policy info")
				volumeInfoService, err = cnsvolumeinfo.InitVolumeInfoService(ctx)
				if err != nil {
					return logger.LogNewErrorf(log, "error initializing volumeInfoService. Error: %+v", err)
				}
				log.Infof("Successfully initialized VolumeInfoService")
			} else {
				if len(clusterComputeResourceMoIds) > 1 {
					log.Infof("Not initializing the VirtualMachineSnapshot Controller as stretched supervisor is detected.")
					return nil
				}
			}
		}
	}
	// Initializes kubernetes client.
	k8sclient, err := k8s.NewClient(ctx)
	if err != nil {
		log.Errorf("Creating Kubernetes client failed. Err: %v", err)
		return err
	}
	restClientConfig, err := k8s.GetKubeConfig(ctx)
	if err != nil {
		msg := fmt.Sprintf("Failed to initialize rest clientconfig. Error: %+v", err)
		log.Error(msg)
		return err
	}
	vmOperatorClient, err := k8s.NewClientForGroup(ctx, restClientConfig, vmoperatorv1alpha4.GroupName)
	if err != nil {
		msg := fmt.Sprintf("Failed to initialize vmOperatorClient. Error: %+v", err)
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
	recorder := eventBroadcaster.NewRecorder(scheme.Scheme, v1.EventSource{Component: apis.GroupName})
	return add(mgr, newReconciler(mgr, configInfo, volumeManager, recorder, vmOperatorClient, volumeInfoService))
}

// newReconciler returns a new reconcile.Reconciler.
func newReconciler(mgr manager.Manager, configInfo *commonconfig.ConfigurationInfo,
	volumeManager volumes.Manager, recorder record.EventRecorder, vmOperatorClient client.Client,
	volumeInfoService cnsvolumeinfo.VolumeInfoService) reconcile.Reconciler {
	return &ReconcileVirtualMachineSnapshot{client: mgr.GetClient(), scheme: mgr.GetScheme(),
		configInfo: configInfo, volumeManager: volumeManager, recorder: recorder, vmOperatorClient: vmOperatorClient, volumeInfoService: volumeInfoService}
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
	log := logger.GetLogger(ctx)
	log.Info("Reconciling CR VirtualMachineSnapshot")
	// Fetch the VirtualMachineSnapshot instance.
	vms := &vmoperatorv1alpha4.VirtualMachineSnapshot{}
	err := r.client.Get(ctx, request.NamespacedName, vms)
	if err != nil {
		if apierrors.IsNotFound(err) {
			log.Infof("VirtualMachineSnapshot resource not found. Ignoring since object must be deleted.")
			return reconcile.Result{}, nil
		}
		log.Errorf("Error reading the VirtualMachineSnapshot with name: %q. Err: %+v",
			request.Name, err)
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

	log.Info("Reconciling virtualmachinesnapshot")

	// Check for the annotation "csi.vsphere.volume.sync: Requested"
	annotationValue := vms.Annotations["csi.vsphere.volume.sync"]
	if annotationValue == "Requested" {
		// if found fetch vmsnapshot and pvcs and pvs
		vmKey := types.NamespacedName{
			Namespace: vms.Namespace,
			Name:      vms.Spec.VMRef.Name,
		}
		virtualMachine, _, err := utils.GetVirtualMachineAllApiVersions(ctx, vmKey,
			r.vmOperatorClient)
		if err != nil {
			log.Errorf("ReconcileVirtualMachineSnapshot: "+
				" Could not get VirtualMachine with name: %s namespace: %s err: %v",
				virtualMachine.Namespace, virtualMachine.Name, err)
		}
		err = r.validateStorageQuotaforVMSnapshot(ctx, virtualMachine)
		if err != nil {
			log.Errorf("ReconcileVirtualMachineSnapshot: Failed to validate VirtualMachineSnapshot for %s/%s err: %v",
				vms.Namespace, vms.Name, err)
			return reconcile.Result{RequeueAfter: timeout}, err
		}
		// Update VMSnapshot CR annotation to "csi.vsphere.volume.sync: Completed"
		vms.Annotations["csi.vsphere.volume.sync"] = "Completed"
		err = utils.UpdateVirtualMachineSnapshot(ctx, r.vmOperatorClient, vms)
		if err != nil {
			log.Errorf("ReconcileVirtualMachineSnapshot: Could not update VirtualMachineSnapshot CR for %s/%s err: %v",
				vms.Namespace, vms.Name, err)
			return reconcile.Result{RequeueAfter: timeout}, err
		}
	}
	backOffDurationMapMutex.Lock()
	delete(backOffDuration, request.NamespacedName)
	backOffDurationMapMutex.Unlock()
	return reconcile.Result{}, nil
}

// getMaxWorkerThreadsToReconcileVirtualMachineSnapshot returns the maximum number
// of worker threads which can be run to reconcile VirtualMachineSnapshot instances.
// If environment variable WORKER_THREADS_VIRTUAL_MACHINE_SNAPSHOT is set and valid,
// return the value read from environment variable. Otherwise, use the default
// value.
func getMaxWorkerThreadsToReconcileVirtualMachineSnapshot(ctx context.Context) int {
	log := logger.GetLogger(ctx)
	workerThreads := defaultMaxWorkerThreadsForVirtualMachineSnapshot
	// Maximum number of worker threads to run.
	if v := os.Getenv("WORKER_THREADS_VIRTUAL_MACHINE_SNAPSHOT"); v != "" {
		if value, err := strconv.Atoi(v); err == nil {
			if value <= 0 {
				log.Warnf("Env variable WORKER_THREADS_VIRTUAL_MACHINE_SNAPSHOT %s is less than 1, use the default %d",
					v, defaultMaxWorkerThreadsForVirtualMachineSnapshot)
			} else if value > defaultMaxWorkerThreadsForVirtualMachineSnapshot {
				log.Warnf("Env variable WORKER_THREADS_VIRTUAL_MACHINE_SNAPSHOT %s is greater than %d, use the default %d",
					v, defaultMaxWorkerThreadsForVirtualMachineSnapshot, defaultMaxWorkerThreadsForVirtualMachineSnapshot)
			} else {
				workerThreads = value
				log.Debugf("Maximum #worker to reconcile VirtualMachineSnapshot instances is set to %d",
					workerThreads)
			}
		} else {
			log.Warnf("Env variable WORKER_THREADS_VIRTUAL_MACHINE_SNAPSHOT %s is invalid, use the default %d",
				v, defaultMaxWorkerThreadsForVirtualMachineSnapshot)
		}
	} else {
		log.Debugf("WORKER_THREADS_VIRTUAL_MACHINE_SNAPSHOT is not set. Use the default value %d",
			defaultMaxWorkerThreadsForVirtualMachineSnapshot)
	}
	return workerThreads
}

func (r *ReconcileVirtualMachineSnapshot) validateStorageQuotaforVMSnapshot(ctx context.Context, vm *vmoperatorv1alpha4.VirtualMachine) error {
	var err error
	log := logger.GetLogger(ctx)
	syncVolumeSpecs := []cnstypes.CnsSyncVolumeSpec{}
	cnsVolumeIds := []cnstypes.CnsVolumeId{}
	syncMode := []string{string(cnstypes.CnsSyncVolumeModeSPACE_USAGE)}
	var pvc *v1.PersistentVolumeClaim
	var pv *v1.PersistentVolume
	k8sclient, err := k8s.NewClient(ctx)
	if err != nil {
		log.Errorf("ValidateStorageQuotaforVMSnapshot2: Failed to initialize K8S client error: %v", err)

		return err
	}
	for _, vmVolume := range vm.Spec.Volumes {
		pvc, err = k8sclient.CoreV1().PersistentVolumeClaims(vm.Namespace).Get(ctx, vmVolume.Name, metav1.GetOptions{})
		if err != nil {
			log.Errorf("validateStorageQuotaforVMSnapshot: get pvc %s/%s failed: %+v", vm.Namespace, vmVolume.Name, err)
			return err
		}
		if pvc.Spec.VolumeName != "" {
			pv, err = k8sclient.CoreV1().PersistentVolumes().Get(ctx, pvc.Spec.VolumeName, metav1.GetOptions{})
			if err != nil {
				log.Errorf("validateStorageQuotaforVMSnapshot: could not get the volume for pvc %s", pvc.Name)
				return err
			}
			if pv.Spec.CSI != nil && pv.Spec.CSI.VolumeHandle != "" {
				cnsVolId := cnstypes.CnsVolumeId{Id: pv.Spec.CSI.VolumeHandle}
				cnsVolumeIds = append(cnsVolumeIds, cnsVolId)
				syncVolumeSpecs = append(syncVolumeSpecs, cnstypes.CnsSyncVolumeSpec{VolumeId: cnsVolId, SyncMode: syncMode})
			}
		} else {
			msg := fmt.Sprintf("validateStorageQuotaforVMSnapshot: could not find the PV associated with PVC %s/%s", vm.Namespace, vmVolume.Name)
			log.Error(msg)
			return errors.New(msg)
		}
	}
	if len(syncVolumeSpecs) == 0 {
		log.Info("no volumes found for virtual machine %+v/%+v", vm.Name, vm.Namespace)
		return nil
	}

	// Trigger CNS VolumeSync API for identified volume-lds and Fetch Latest Aggregated snapshot size
	syncVolumeFaultType, err := r.volumeManager.SyncVolume(ctx, syncVolumeSpecs)
	if err != nil {
		log.Errorf("validateStorageQuotaforVMSnapshot: failed to sync volumes opertion failed with cnsfault: %q error %+v", syncVolumeFaultType, err)
		return err
	}
	// fetch updated cns volumes
	queryFilter := cnstypes.CnsQueryFilter{
		VolumeIds: cnsVolumeIds,
	}
	queryResult, err := r.volumeManager.QueryVolume(ctx, queryFilter)
	if err != nil {
		log.Errorf("validateStorageQuotaforVMSnapshot: failed to query volume from cns with err: %v.", err)
		return err
	}
	if queryResult != nil && len(queryResult.Volumes) > 0 {
		for _, cnsvolume := range queryResult.Volumes {
			val, ok := cnsvolume.BackingObjectDetails.(*cnstypes.CnsBlockBackingDetails)
			if ok {
				//  Update CNSVolumeInfo with latest aggregated Size and Update SPU used value.
				patch, err := common.GetCNSVolumeInfoPatch(ctx, val.AggregatedSnapshotCapacityInMb, cnsvolume.VolumeId.Id) // TODO: UDPATE to value returned
				if err != nil {
					log.Errorf("validateStorageQuotaforVMSnapshot: failed to create cnsvolumeinfo patch error %+v", err)
					return err
				}
				patchBytes, err := json.Marshal(patch)
				if err != nil {
					return err
				}
				err = r.volumeInfoService.PatchVolumeInfo(ctx, cnsvolume.VolumeId.Id,
					patchBytes, allowedRetriesToPatchCNSVolumeInfo)
				if err != nil {
					return err
				}
				log.Infof("validateStorageQuotaforVMSnapshot: received aggregated capacity %d for volumeID %q",
					val.AggregatedSnapshotCapacityInMb, cnsvolume.VolumeId.Id)
			} else {
				return logger.LogNewErrorf(log, "validateStorageQuotaforVMSnapshot: unable to retrieve"+
					" CnsBlockBackingDetails for volumeID %q", cnsvolume.VolumeId.Id)
			}
		}
	}
	return nil
}
