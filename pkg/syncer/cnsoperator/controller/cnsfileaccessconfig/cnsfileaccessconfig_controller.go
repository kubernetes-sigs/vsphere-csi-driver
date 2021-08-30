/*
Copyright 2021 The Kubernetes Authors.

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

package cnsfileaccessconfig

import (
	"context"
	"fmt"
	"reflect"
	"sync"
	"time"

	cnstypes "github.com/vmware/govmomi/cns/types"
	v1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/kubernetes/scheme"
	typedcorev1 "k8s.io/client-go/kubernetes/typed/core/v1"
	"k8s.io/client-go/tools/record"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/config"
	"sigs.k8s.io/controller-runtime/pkg/controller"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	"sigs.k8s.io/controller-runtime/pkg/manager"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
	"sigs.k8s.io/controller-runtime/pkg/source"

	vmoperatortypes "github.com/vmware-tanzu/vm-operator-api/api/v1alpha1"
	vsanfstypes "github.com/vmware/govmomi/vsan/vsanfs/types"
	cnsoperatorapis "sigs.k8s.io/vsphere-csi-driver/v2/pkg/apis/cnsoperator"
	cnsfileaccessconfigv1alpha1 "sigs.k8s.io/vsphere-csi-driver/v2/pkg/apis/cnsoperator/cnsfileaccessconfig/v1alpha1"
	volumes "sigs.k8s.io/vsphere-csi-driver/v2/pkg/common/cns-lib/volume"
	commonconfig "sigs.k8s.io/vsphere-csi-driver/v2/pkg/common/config"
	"sigs.k8s.io/vsphere-csi-driver/v2/pkg/csi/service/common"
	"sigs.k8s.io/vsphere-csi-driver/v2/pkg/csi/service/common/commonco"
	"sigs.k8s.io/vsphere-csi-driver/v2/pkg/csi/service/logger"
	"sigs.k8s.io/vsphere-csi-driver/v2/pkg/internalapis/cnsoperator/cnsfilevolumeclient"
	k8s "sigs.k8s.io/vsphere-csi-driver/v2/pkg/kubernetes"
	"sigs.k8s.io/vsphere-csi-driver/v2/pkg/syncer"
	cnsoperatortypes "sigs.k8s.io/vsphere-csi-driver/v2/pkg/syncer/cnsoperator/types"
	cnsoperatorutil "sigs.k8s.io/vsphere-csi-driver/v2/pkg/syncer/cnsoperator/util"
)

const (
	defaultMaxWorkerThreadsForFileAccessConfig = 10
)

// backOffDuration is a map of cnsfileaccessconfig name's to the time after
// which a request for this instance will be requeued. Initialized to 1 second
// for new instances and for instances whose latest reconcile operation
// succeeded. If the reconcile fails, backoff is incremented exponentially.
var (
	backOffDuration         map[string]time.Duration
	backOffDurationMapMutex = sync.Mutex{}
)

// Add creates a new CnsFileAccessConfig Controller and adds it to the Manager.
// The Manager will set fields on the Controller and Start it when the Manager
// is Started.
func Add(mgr manager.Manager, clusterFlavor cnstypes.CnsClusterFlavor,
	configInfo *commonconfig.ConfigurationInfo, volumeManager volumes.Manager) error {
	ctx, log := logger.GetNewContextWithLogger()
	if clusterFlavor != cnstypes.CnsClusterFlavorWorkload {
		log.Debug("Not initializing the CnsFileAccessConfig Controller as its a non-WCP CSI deployment")
		return nil
	}
	// Initialize the k8s orchestrator interface.
	coCommonInterface, err := commonco.GetContainerOrchestratorInterface(ctx, common.Kubernetes,
		cnstypes.CnsClusterFlavorWorkload, &syncer.COInitParams)
	if err != nil {
		log.Errorf("failed to create CO agnostic interface. Err: %v", err)
		return err
	}
	if !coCommonInterface.IsFSSEnabled(ctx, common.FileVolume) {
		log.Infof("Not initializing the CnsFileAccessConfig Controller as File volume feature is disabled on the cluster")
		return nil
	}
	// Initializes kubernetes client.
	k8sclient, err := k8s.NewClient(ctx)
	if err != nil {
		log.Errorf("Creating Kubernetes client failed. Err: %v", err)
		return err
	}

	// eventBroadcaster broadcasts events on cnsfileaccessconfig instances to
	// the event sink.
	eventBroadcaster := record.NewBroadcaster()
	eventBroadcaster.StartRecordingToSink(
		&typedcorev1.EventSinkImpl{
			Interface: k8sclient.CoreV1().Events(""),
		},
	)
	restClientConfig, err := k8s.GetKubeConfig(ctx)
	if err != nil {
		msg := fmt.Sprintf("Failed to initialize rest clientconfig. Error: %+v", err)
		log.Error(msg)
		return err
	}

	vmOperatorClient, err := k8s.NewClientForGroup(ctx, restClientConfig, vmoperatortypes.GroupName)
	if err != nil {
		msg := fmt.Sprintf("Failed to initialize vmOperatorClient. Error: %+v", err)
		log.Error(msg)
		return err
	}

	cfg, err := config.GetConfig()
	if err != nil {
		msg := fmt.Sprintf("Failed to get config. Err: %+v", err)
		log.Error(msg)
		return err
	}

	// create a new dynamic client for config.
	dynamicClient, err := dynamic.NewForConfig(cfg)
	if err != nil {
		msg := fmt.Sprintf("Failed to create client using config. Err: %+v", err)
		log.Error(msg)
		return err
	}
	recorder := eventBroadcaster.NewRecorder(scheme.Scheme, v1.EventSource{Component: cnsoperatorapis.GroupName})
	return add(mgr, newReconciler(mgr, configInfo, volumeManager, vmOperatorClient, dynamicClient, recorder))
}

// newReconciler returns a new reconcile.Reconciler.
func newReconciler(mgr manager.Manager, configInfo *commonconfig.ConfigurationInfo,
	volumeManager volumes.Manager, vmOperatorClient client.Client, dynamicClient dynamic.Interface,
	recorder record.EventRecorder) reconcile.Reconciler {
	return &ReconcileCnsFileAccessConfig{client: mgr.GetClient(), scheme: mgr.GetScheme(),
		configInfo: configInfo, volumeManager: volumeManager, vmOperatorClient: vmOperatorClient,
		dynamicClient: dynamicClient, recorder: recorder}
}

// add adds a new Controller to mgr with r as the reconcile.Reconciler.
func add(mgr manager.Manager, r reconcile.Reconciler) error {
	ctx, log := logger.GetNewContextWithLogger()

	maxWorkerThreads := getMaxWorkerThreadsToReconcileCnsFileAccessConfig(ctx)

	// Create a new controller.
	c, err := controller.New("cnsfileaccessconfig-controller", mgr,
		controller.Options{Reconciler: r, MaxConcurrentReconciles: maxWorkerThreads})
	if err != nil {
		log.Errorf("Failed to create new CnsFileAccessConfig controller with error: %+v", err)
		return err
	}

	backOffDuration = make(map[string]time.Duration)

	// Watch for changes to primary resource CnsFileAccessConfig.
	err = c.Watch(&source.Kind{Type: &cnsfileaccessconfigv1alpha1.CnsFileAccessConfig{}},
		&handler.EnqueueRequestForObject{})
	if err != nil {
		log.Errorf("Failed to watch for changes to CnsFileAccessConfig resource with error: %+v", err)
		return err
	}
	return nil
}

// Blank assignment to verify that ReconcileCnsFileAccessConfig implements
// reconcile.Reconciler.
var _ reconcile.Reconciler = &ReconcileCnsFileAccessConfig{}

// ReconcileCnsFileAccessConfig reconciles a CnsFileAccessConfig object.
type ReconcileCnsFileAccessConfig struct {
	// This client, initialized using mgr.Client() above, is a split client
	// that reads objects from the cache and writes to the apiserver.
	client           client.Client
	scheme           *runtime.Scheme
	configInfo       *commonconfig.ConfigurationInfo
	volumeManager    volumes.Manager
	vmOperatorClient client.Client
	dynamicClient    dynamic.Interface
	recorder         record.EventRecorder
}

// Reconcile reads that cluster state for a CnsFileAccessConfig object and makes
// changes based on the state read and what is in the CnsFileAccessConfig.Spec.
// Note:
// The Controller will requeue the Request to be processed again if the returned
// error is non-nil or Result.Requeue is true, otherwise upon completion it will
// remove the work from the queue.
func (r *ReconcileCnsFileAccessConfig) Reconcile(ctx context.Context,
	request reconcile.Request) (reconcile.Result, error) {
	log := logger.GetLogger(ctx)
	// Fetch the CnsFileAccessConfig instance.
	instance := &cnsfileaccessconfigv1alpha1.CnsFileAccessConfig{}
	err := r.client.Get(ctx, request.NamespacedName, instance)
	if err != nil {
		if apierrors.IsNotFound(err) {
			log.Infof("CnsFileAccessConfig resource not found. Ignoring since object must be deleted.")
			return reconcile.Result{}, nil
		}
		log.Errorf("Error reading the CnsFileAccessConfig with name: %q on namespace: %q. Err: %+v",
			request.Name, request.Namespace, err)
		// Error reading the object - requeue the request.
		return reconcile.Result{}, err
	}

	// Initialize backOffDuration for the instance, if required.
	backOffDurationMapMutex.Lock()
	var timeout time.Duration
	if _, exists := backOffDuration[instance.Name]; !exists {
		backOffDuration[instance.Name] = time.Second
	}
	timeout = backOffDuration[instance.Name]
	backOffDurationMapMutex.Unlock()

	// Get the virtualmachine instance
	vm, err := getVirtualMachine(ctx, r.vmOperatorClient, instance.Spec.VMName, instance.Namespace)
	if err != nil {
		msg := fmt.Sprintf("Failed to get virtualmachine instance for the VM with name: %q. Error: %+v",
			instance.Spec.VMName, err)
		log.Error(msg)
		setInstanceError(ctx, r, instance, msg)
		return reconcile.Result{RequeueAfter: timeout}, nil
	}
	log.Debugf("Found virtualMachine instance for VM: %q/%q: +%v", instance.Namespace, instance.Spec.VMName, vm)

	if instance.DeletionTimestamp != nil {
		volumeID, err := cnsoperatorutil.GetVolumeID(ctx, r.client, instance.Spec.PvcName, instance.Namespace)
		if err != nil {
			msg := fmt.Sprintf("Failed to get volumeID from pvcName: %q. Error: %+v", instance.Spec.PvcName, err)
			log.Error(msg)
			setInstanceError(ctx, r, instance, msg)
			return reconcile.Result{RequeueAfter: timeout}, nil
		}
		err = r.configureNetPermissionsForFileVolume(ctx, volumeID, vm, instance, true)
		if err != nil {
			msg := fmt.Sprintf("Failed to configure CnsFileAccessConfig instance with error: %+v", err)
			log.Error(msg)
			setInstanceError(ctx, r, instance, msg)
			return reconcile.Result{RequeueAfter: timeout}, nil
		}
		removeFinalizerFromCRDInstance(ctx, instance)
		err = updateCnsFileAccessConfig(ctx, r.client, instance)
		if err != nil {
			msg := fmt.Sprintf("failed to update CnsFileAccessConfig instance: %q on namespace: %q. Error: %+v",
				instance.Name, instance.Namespace, err)
			recordEvent(ctx, r, instance, v1.EventTypeWarning, msg)
			return reconcile.Result{RequeueAfter: timeout}, nil
		}
		// Cleanup instance entry from backOffDuration map.
		backOffDurationMapMutex.Lock()
		delete(backOffDuration, instance.Name)
		backOffDurationMapMutex.Unlock()
		return reconcile.Result{}, nil
	}

	// If the CnsFileAccessConfig instance is already successful,
	// and not deleted by the user, remove the instance from the queue.
	if instance.Status.Done {
		// Cleanup instance entry from backOffDuration map.
		backOffDurationMapMutex.Lock()
		delete(backOffDuration, instance.Name)
		backOffDurationMapMutex.Unlock()
		return reconcile.Result{}, nil
	}
	cnsFinalizerExists := false
	// Check if finalizer already exists.
	for _, finalizer := range instance.Finalizers {
		if finalizer == cnsoperatortypes.CNSFinalizer {
			cnsFinalizerExists = true
			break
		}
	}
	if !cnsFinalizerExists {
		// Add finalizer.
		instance.Finalizers = append(instance.Finalizers, cnsoperatortypes.CNSFinalizer)
		err = updateCnsFileAccessConfig(ctx, r.client, instance)
		if err != nil {
			msg := fmt.Sprintf("failed to update CnsFileAccessConfig instance: %q on namespace: %q. Error: %+v",
				instance.Name, instance.Namespace, err)
			recordEvent(ctx, r, instance, v1.EventTypeWarning, msg)
			return reconcile.Result{RequeueAfter: timeout}, nil
		}
	}

	vmOwnerRefExists := false
	if len(instance.OwnerReferences) != 0 {
		for _, ownerRef := range instance.OwnerReferences {
			if ownerRef.Kind == reflect.TypeOf(vmoperatortypes.VirtualMachine{}).Name() &&
				ownerRef.Name == instance.Spec.VMName && ownerRef.UID == vm.UID {
				vmOwnerRefExists = true
				break
			}
		}
	}
	if !vmOwnerRefExists {
		// Set ownerRef on CnsFileAccessConfig instance (in-memory) to VM instance.
		setInstanceOwnerRef(instance, instance.Spec.VMName, vm.UID)
		err = updateCnsFileAccessConfig(ctx, r.client, instance)
		if err != nil {
			msg := fmt.Sprintf("failed to update CnsFileAccessConfig instance: %q on namespace: %q. Error: %+v",
				instance.Name, instance.Namespace, err)
			recordEvent(ctx, r, instance, v1.EventTypeWarning, msg)
			return reconcile.Result{RequeueAfter: timeout}, nil
		}
	}
	log.Infof("Reconciling CnsFileAccessConfig with instance: %q from namespace: %q. timeout %q seconds",
		instance.Name, instance.Namespace, timeout)
	if !instance.Status.Done {
		volumeID, err := cnsoperatorutil.GetVolumeID(ctx, r.client, instance.Spec.PvcName, instance.Namespace)
		if err != nil {
			msg := fmt.Sprintf("Failed to get volumeID from pvcName: %q. Error: %+v", instance.Spec.PvcName, err)
			log.Error(msg)
			setInstanceError(ctx, r, instance, msg)
			return reconcile.Result{RequeueAfter: timeout}, nil
		}

		// Query volume.
		log.Debugf("Querying volume: %s for CnsFileAccessConfig request with name: %q on namespace: %q",
			volumeID, instance.Name, instance.Namespace)
		volume, err := common.QueryVolumeByID(ctx, r.volumeManager, volumeID)
		if err != nil {
			if err.Error() == common.ErrNotFound.Error() {
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

		vSANFileBackingDetails := volume.BackingObjectDetails.(*cnstypes.CnsVsanFileShareBackingDetails)
		accessPoints := make(map[string]string)
		for _, kv := range vSANFileBackingDetails.AccessPoints {
			accessPoints[kv.Key] = kv.Value
		}
		if len(accessPoints) == 0 {
			msg := fmt.Sprintf("No access points found for volume: %q", volumeID)
			log.Error(msg)
			setInstanceError(ctx, r, instance, msg)
			return reconcile.Result{RequeueAfter: timeout}, nil
		}
		err = r.configureNetPermissionsForFileVolume(ctx, volumeID, vm, instance, false)
		if err != nil {
			msg := fmt.Sprintf("Failed to configure CnsFileAccessConfig instance with error: %+v", err)
			log.Error(msg)
			setInstanceError(ctx, r, instance, msg)
			return reconcile.Result{RequeueAfter: timeout}, nil
		}
		// Update the instance to indicate the volume registration is successful.
		msg := fmt.Sprintf("Successfully configured access points of VM: %q on the volume: %q",
			instance.Spec.VMName, instance.Spec.PvcName)
		instance.Status.AccessPoints = accessPoints
		err = setInstanceSuccess(ctx, r, instance, msg)
		if err != nil {
			msg := fmt.Sprintf("Failed to update CnsFileAccessConfig instance with error: %+v", err)
			log.Error(msg)
			setInstanceError(ctx, r, instance, msg)
			return reconcile.Result{RequeueAfter: timeout}, nil
		}
		log.Info(msg)
	}

	backOffDurationMapMutex.Lock()
	delete(backOffDuration, instance.Name)
	backOffDurationMapMutex.Unlock()
	return reconcile.Result{}, nil
}

// configureNetPermissionsForFileVolume helps to add or remove net permissions
// for a given file volume. The callers of this method can remove or add net
// permissions by setting the parameter removePermission to true or false
// respectively. Returns error if any operation fails.
func (r *ReconcileCnsFileAccessConfig) configureNetPermissionsForFileVolume(ctx context.Context,
	volumeID string, vm *vmoperatortypes.VirtualMachine, instance *cnsfileaccessconfigv1alpha1.CnsFileAccessConfig,
	removePermission bool) error {
	log := logger.GetLogger(ctx)
	tkgVMIP, err := r.getVMExternalIP(ctx, vm)
	if err != nil {
		return logger.LogNewErrorf(log, "Failed to get external facing IP address for VM: %s/%s instance. Error: %+v",
			vm.Namespace, vm.Name, err)
	}
	cnsFileVolumeClientInstance, err := cnsfilevolumeclient.GetFileVolumeClientInstance(ctx)
	if err != nil {
		return logger.LogNewErrorf(log, "Failed to get CNSFileVolumeClient instance. Error: %+v", err)
	}
	clientVms, err := cnsFileVolumeClientInstance.GetClientVMsFromIPList(ctx,
		instance.Namespace+"/"+instance.Spec.PvcName, tkgVMIP)
	if err != nil {
		return logger.LogNewErrorf(log, "Failed to get the list of clients VMs for IP %q. Error: %+v", tkgVMIP, err)
	}
	if !removePermission {
		if len(clientVms) == 0 {
			err = r.configureVolumeACLs(ctx, volumeID, tkgVMIP, false)
			if err != nil {
				return logger.LogNewErrorf(log, "Failed to add net permissions for file volume %q. Error: %+v",
					volumeID, err)
			}
		}
		err = cnsFileVolumeClientInstance.AddClientVMToIPList(ctx,
			instance.Namespace+"/"+instance.Spec.PvcName, instance.Spec.VMName, tkgVMIP)
		if err != nil {
			return logger.LogNewErrorf(log, "Failed to add VM %q with IP %q to IPList. Error: %+v",
				vm.Name, tkgVMIP, err)
		}
		log.Debugf("Successfully added VM IP %q to IPList for CnsFileAccessConfig request with name: %q on namespace: %q",
			tkgVMIP, instance.Name, instance.Namespace)
		return nil
	}
	// RemovePermission is set to true.
	if len(clientVms) == 1 && clientVms[0] == vm.Name {
		err = r.configureVolumeACLs(ctx, volumeID, tkgVMIP, true)
		if err != nil {
			return logger.LogNewErrorf(log, "Failed to remove net permissions for file volume %q. Error: %+v",
				volumeID, err)
		}
	}
	err = cnsFileVolumeClientInstance.RemoveClientVMFromIPList(ctx,
		instance.Namespace+"/"+instance.Spec.PvcName, instance.Spec.VMName, tkgVMIP)
	if err != nil {
		return logger.LogNewErrorf(log, "Failed to remove VM %q with IP %q to IPList. Error: %+v", vm.Name, tkgVMIP, err)
	}
	log.Debugf("Successfully removed VM IP %q to IPList for CnsFileAccessConfig request with name: %q on namespace: %q",
		tkgVMIP, instance.Name, instance.Namespace)
	return nil
}

// configureVolumeACLs helps to prepare the CnsVolumeACLConfigureSpec
// for a given TKG VM IP address and volumeID and invoke CNS API.
func (r *ReconcileCnsFileAccessConfig) configureVolumeACLs(ctx context.Context,
	volumeID string, tkgVMIP string, delete bool) error {
	log := logger.GetLogger(ctx)
	cnsVolumeID := cnstypes.CnsVolumeId{
		Id: volumeID,
	}
	vSanFileShareNetPermissions := make([]vsanfstypes.VsanFileShareNetPermission, 0)
	vsanFileShareAccessType := vsanfstypes.VsanFileShareAccessTypeREAD_WRITE
	vSanFileShareNetPermissions = append(vSanFileShareNetPermissions, vsanfstypes.VsanFileShareNetPermission{
		Ips:         tkgVMIP,
		Permissions: vsanFileShareAccessType,
		AllowRoot:   true,
	})

	cnsNFSAccessControlSpecList := make([]cnstypes.CnsNFSAccessControlSpec, 0)
	cnsNFSAccessControlSpecList = append(cnsNFSAccessControlSpecList, cnstypes.CnsNFSAccessControlSpec{
		Permission: vSanFileShareNetPermissions,
		Delete:     delete,
	})

	cnsVolumeACLConfigSpec := cnstypes.CnsVolumeACLConfigureSpec{
		VolumeId:              cnsVolumeID,
		AccessControlSpecList: cnsNFSAccessControlSpecList,
	}
	log.Debugf("CnsVolumeACLConfigSpec : %v", cnsVolumeACLConfigSpec)
	err := r.volumeManager.ConfigureVolumeACLs(ctx, cnsVolumeACLConfigSpec)
	if err != nil {
		return logger.LogNewErrorf(log, "Failed to configure ACLs for volume: %q. Error: %+v", volumeID, err)
	}
	log.Debugf("Successfully configured ACLs for volume %q", volumeID)
	return nil
}

// getVMExternalIP helps to fetch the external facing IP for a given TKG VM.
func (r *ReconcileCnsFileAccessConfig) getVMExternalIP(ctx context.Context,
	vm *vmoperatortypes.VirtualMachine) (string, error) {
	log := logger.GetLogger(ctx)
	networkProvider, err := cnsoperatorutil.GetNetworkProvider(ctx)
	if err != nil {
		return "", logger.LogNewErrorf(log, "Failed to identify the network provider. Error: %+v", err)
	}
	var nsxConfiguration bool
	if networkProvider == "" {
		return "", logger.LogNewError(log, "unable to find network provider information")
	}
	if networkProvider == cnsoperatorutil.NSXTNetworkProvider {
		nsxConfiguration = true
	} else if networkProvider == cnsoperatorutil.VDSNetworkProvider {
		nsxConfiguration = false
	} else {
		return "", logger.LogNewErrorf(log, "Unknown network provider. Error: %+v", err)
	}

	tkgVMIP, err := cnsoperatorutil.GetTKGVMIP(ctx, r.vmOperatorClient,
		r.dynamicClient, vm.Namespace, vm.Name, nsxConfiguration)
	if err != nil {
		return "", logger.LogNewErrorf(log, "Failed to get external facing IP address for VM %q/%q. Err: %+v",
			vm.Namespace, vm.Name, err)
	}
	log.Debugf("Found tkg VMIP %q for VM %q in namespace %q", tkgVMIP, vm.Name, vm.Namespace)
	return tkgVMIP, nil
}

// setInstanceSuccess sets instance to success and records an event on the
// CnsFileAccessConfig instance.
func setInstanceSuccess(ctx context.Context, r *ReconcileCnsFileAccessConfig,
	instance *cnsfileaccessconfigv1alpha1.CnsFileAccessConfig, msg string) error {
	instance.Status.Done = true
	instance.Status.Error = ""
	err := updateCnsFileAccessConfig(ctx, r.client, instance)
	if err != nil {
		return err
	}
	recordEvent(ctx, r, instance, v1.EventTypeNormal, msg)
	return nil
}

// setInstanceError sets error and records an event on the CnsFileAccessConfig
// instance.
func setInstanceError(ctx context.Context, r *ReconcileCnsFileAccessConfig,
	instance *cnsfileaccessconfigv1alpha1.CnsFileAccessConfig, errMsg string) {
	log := logger.GetLogger(ctx)
	instance.Status.Error = errMsg
	err := updateCnsFileAccessConfig(ctx, r.client, instance)
	if err != nil {
		log.Errorf("updateCnsFileAccessConfig failed. err: %v", err)
	}
	recordEvent(ctx, r, instance, v1.EventTypeWarning, errMsg)
}

func updateCnsFileAccessConfig(ctx context.Context, client client.Client,
	instance *cnsfileaccessconfigv1alpha1.CnsFileAccessConfig) error {
	log := logger.GetLogger(ctx)
	err := client.Update(ctx, instance)
	if err != nil {
		log.Errorf("failed to update CnsFileAccessConfig instance: %q on namespace: %q. Error: %+v",
			instance.Name, instance.Namespace, err)
	}
	return err
}

// recordEvent records the event, sets the backOffDuration for the instance
// appropriately and logs the message.
// backOffDuration is reset to 1 second on success and doubled on failure.
func recordEvent(ctx context.Context, r *ReconcileCnsFileAccessConfig,
	instance *cnsfileaccessconfigv1alpha1.CnsFileAccessConfig, eventtype string, msg string) {
	log := logger.GetLogger(ctx)
	log.Debugf("Event type is %s", eventtype)
	switch eventtype {
	case v1.EventTypeWarning:
		// Double backOff duration.
		backOffDurationMapMutex.Lock()
		backOffDuration[instance.Name] = backOffDuration[instance.Name] * 2
		r.recorder.Event(instance, v1.EventTypeWarning, "CnsFileAccessConfigFailed", msg)
		backOffDurationMapMutex.Unlock()
	case v1.EventTypeNormal:
		// Reset backOff duration to one second.
		backOffDurationMapMutex.Lock()
		backOffDuration[instance.Name] = time.Second
		r.recorder.Event(instance, v1.EventTypeNormal, "CnsFileAccessConfigSucceeded", msg)
		backOffDurationMapMutex.Unlock()
	}
}

// removeFinalizerFromCRDInstance will remove the CNS Finalizer = cns.vmware.com,
// from a given CnsFileAccessConfig instance.
func removeFinalizerFromCRDInstance(ctx context.Context, instance *cnsfileaccessconfigv1alpha1.CnsFileAccessConfig) {
	log := logger.GetLogger(ctx)
	for i, finalizer := range instance.Finalizers {
		if finalizer == cnsoperatortypes.CNSFinalizer {
			log.Debugf("Removing %q finalizer from CnsFileAccessConfig instance with name: %q on namespace: %q",
				cnsoperatortypes.CNSFinalizer, instance.Name, instance.Namespace)
			instance.Finalizers = append(instance.Finalizers[:i], instance.Finalizers[i+1:]...)
			break
		}
	}
}
