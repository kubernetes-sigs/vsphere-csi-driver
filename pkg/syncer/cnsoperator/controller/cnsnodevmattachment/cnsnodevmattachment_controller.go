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

package cnsnodevmattachment

import (
	"context"
	"errors"
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"

	"sigs.k8s.io/vsphere-csi-driver/pkg/csi/service/logger"

	"gitlab.eng.vmware.com/hatchway/govmomi/object"
	vimtypes "gitlab.eng.vmware.com/hatchway/govmomi/vim25/types"
	v1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/runtime"
	k8stypes "k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes/scheme"
	"k8s.io/client-go/tools/record"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	"sigs.k8s.io/controller-runtime/pkg/manager"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
	"sigs.k8s.io/controller-runtime/pkg/source"
	k8s "sigs.k8s.io/vsphere-csi-driver/pkg/kubernetes"

	typedcorev1 "k8s.io/client-go/kubernetes/typed/core/v1"
	cnsnode "sigs.k8s.io/vsphere-csi-driver/pkg/common/cns-lib/node"
	volumes "sigs.k8s.io/vsphere-csi-driver/pkg/common/cns-lib/volume"
	"sigs.k8s.io/vsphere-csi-driver/pkg/common/cns-lib/vsphere"
	"sigs.k8s.io/vsphere-csi-driver/pkg/common/config"
	cnsnodevmattachmentv1alpha1 "sigs.k8s.io/vsphere-csi-driver/pkg/syncer/cnsoperator/apis/cnsnodevmattachment/v1alpha1"
	cnsoperatortypes "sigs.k8s.io/vsphere-csi-driver/pkg/syncer/cnsoperator/types"
	"sigs.k8s.io/vsphere-csi-driver/pkg/syncer/types"
)

const (
	defaultMaxWorkerThreadsForNodeVMAttach = 10
)

// backOffDuration is a map of cnsnodevmattachment name's to the time after which a request
// for this instance will be requeued.
// Initialized to 1 second for new instances and for instances whose latest reconcile
// operation succeeded.
// If the reconcile fails, backoff is incremented exponentially.
var backOffDuration map[string]time.Duration

// Add creates a new CnsNodeVmAttachment Controller and adds it to the Manager, ConfigInfo
// and VirtualCenterTypes. The Manager will set fields on the Controller
// and Start it when the Manager is Started.
func Add(mgr manager.Manager, configInfo *types.ConfigInfo, volumeManager volumes.Manager) error {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	ctx = logger.NewContextWithLogger(ctx)
	log := logger.GetLogger(ctx)
	// Initializes kubernetes client
	k8sclient, err := k8s.NewClient(ctx)
	if err != nil {
		log.Errorf("Creating Kubernetes client failed. Err: %v", err)
		return err
	}

	// eventBroadcaster broadcasts events on cnsnodevmattachment instances to the event sink
	eventBroadcaster := record.NewBroadcaster()
	eventBroadcaster.StartRecordingToSink(
		&typedcorev1.EventSinkImpl{
			Interface: k8sclient.CoreV1().Events(""),
		},
	)
	recorder := eventBroadcaster.NewRecorder(scheme.Scheme, v1.EventSource{Component: "cns.vmware.com"})
	return add(mgr, newReconciler(mgr, configInfo, volumeManager, recorder))
}

// newReconciler returns a new reconcile.Reconciler
func newReconciler(mgr manager.Manager, configInfo *types.ConfigInfo, volumeManager volumes.Manager, recorder record.EventRecorder) reconcile.Reconciler {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	ctx = logger.NewContextWithLogger(ctx)
	return &ReconcileCnsNodeVMAttachment{client: mgr.GetClient(), scheme: mgr.GetScheme(), configInfo: configInfo, volumeManager: volumeManager, nodeManager: cnsnode.GetManager(ctx), recorder: recorder}
}

// add adds a new Controller to mgr with r as the reconcile.Reconciler
func add(mgr manager.Manager, r reconcile.Reconciler) error {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	ctx = logger.NewContextWithLogger(ctx)
	log := logger.GetLogger(ctx)

	maxWorkerThreads := getMaxWorkerThreadsToReconcileCnsNodeVmAttachment(ctx)
	// Create a new controller
	c, err := controller.New("cnsnodevmattachment-controller", mgr, controller.Options{Reconciler: r, MaxConcurrentReconciles: maxWorkerThreads})
	if err != nil {
		log.Errorf("Failed to create new CnsNodeVmAttachment controller with error: %+v", err)
		return err
	}

	backOffDuration = make(map[string]time.Duration)

	// Watch for changes to primary resource CnsNodeVmAttachment
	err = c.Watch(&source.Kind{Type: &cnsnodevmattachmentv1alpha1.CnsNodeVmAttachment{}}, &handler.EnqueueRequestForObject{})
	if err != nil {
		log.Errorf("Failed to watch for changes to CnsNodeVmAttachment resource with error: %+v", err)
		return err
	}
	return nil
}

// blank assignment to verify that ReconcileCnsNodeVMAttachment implements reconcile.Reconciler
var _ reconcile.Reconciler = &ReconcileCnsNodeVMAttachment{}

// ReconcileCnsNodeVMAttachment reconciles a CnsNodeVmAttachment object
type ReconcileCnsNodeVMAttachment struct {
	// This client, initialized using mgr.Client() above, is a split client
	// that reads objects from the cache and writes to the apiserver
	client        client.Client
	scheme        *runtime.Scheme
	configInfo    *types.ConfigInfo
	volumeManager volumes.Manager
	nodeManager   cnsnode.Manager
	recorder      record.EventRecorder
}

// Reconcile reads that state of the cluster for a CnsNodeVMAttachment object and makes changes based on the state read
// and what is in the CnsNodeVMAttachment.Spec
// Note:
// The Controller will requeue the Request to be processed again if the returned error is non-nil or
// Result.Requeue is true, otherwise upon completion it will remove the work from the queue.
func (r *ReconcileCnsNodeVMAttachment) Reconcile(request reconcile.Request) (reconcile.Result, error) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	ctx = logger.NewContextWithLogger(ctx)
	log := logger.GetLogger(ctx)

	// Fetch the CnsNodeVmAttachment instance
	instance := &cnsnodevmattachmentv1alpha1.CnsNodeVmAttachment{}
	err := r.client.Get(ctx, request.NamespacedName, instance)
	if err != nil {
		if apierrors.IsNotFound(err) {
			log.Error("CnsNodeVmAttachment resource not found. Ignoring since object must be deleted.")
			return reconcile.Result{}, nil
		}
		log.Errorf("Error reading the CnsNodeVmAttachment with name: %q on namespace: %q. Err: %+v",
			request.Name, request.Namespace, err)
		// Error reading the object - return with err
		return reconcile.Result{}, err
	}

	// Initialize backOffDuration for the instance, if required.
	var timeout time.Duration
	if _, exists := backOffDuration[instance.Name]; !exists {
		backOffDuration[instance.Name] = time.Second
	}
	timeout = backOffDuration[instance.Name]
	log.Infof("Reconciling CnsNodeVmAttachment with Request.Name: %q instance %q timeout %q seconds", request.Name, instance.Name, timeout)

	// If the CnsNodeVMAttachment instance is already attached and
	// not deleted by the user, remove the instance from the queue.
	if instance.Status.Attached && instance.DeletionTimestamp == nil {
		// Cleanup instance entry from backOffDuration map
		delete(backOffDuration, instance.Name)
		return reconcile.Result{}, nil
	}

	vcdcMap, err := getVCDatacentersFromConfig(r.configInfo.Cfg)
	if err != nil {
		msg := fmt.Sprintf("Failed to find datacenter moref from config for CnsNodeVmAttachment request with name: %q on namespace: %q. Err: %+v",
			request.Name, request.Namespace, err)
		instance.Status.Error = err.Error()
		updateCnsNodeVMAttachment(ctx, r.client, instance)
		recordEvent(ctx, r, instance, v1.EventTypeWarning, msg)
		return reconcile.Result{RequeueAfter: timeout}, nil
	}
	var host, dcMoref string
	for key, value := range vcdcMap {
		host = key
		dcMoref = value[0]
	}
	// Get node VM by nodeUUID
	var dc *vsphere.Datacenter
	vcenter, err := types.GetVirtualCenterInstance(ctx, r.configInfo)
	if err != nil {
		msg := fmt.Sprintf("Failed to get virtual center instance with error: %v", err)
		instance.Status.Error = err.Error()
		updateCnsNodeVMAttachment(ctx, r.client, instance)
		recordEvent(ctx, r, instance, v1.EventTypeWarning, msg)
		return reconcile.Result{RequeueAfter: timeout}, nil
	}
	dc = &vsphere.Datacenter{
		Datacenter: object.NewDatacenter(vcenter.Client.Client,
			vimtypes.ManagedObjectReference{
				Type:  "Datacenter",
				Value: dcMoref,
			}),
		VirtualCenterHost: host,
	}
	nodeUUID := instance.Spec.NodeUUID
	if !instance.Status.Attached && instance.DeletionTimestamp == nil {
		nodeVM, err := dc.GetVirtualMachineByUUID(ctx, nodeUUID, false)
		if err != nil {
			msg := fmt.Sprintf("Failed to find the VM with UUID: %q for CnsNodeVmAttachment request with name: %q on namespace: %q. Err: %+v",
				nodeUUID, request.Name, request.Namespace, err)
			instance.Status.Error = fmt.Sprintf("Failed to find the VM with UUID: %q", nodeUUID)
			updateCnsNodeVMAttachment(ctx, r.client, instance)
			recordEvent(ctx, r, instance, v1.EventTypeWarning, msg)
			return reconcile.Result{RequeueAfter: timeout}, nil
		}
		volumeID, err := getVolumeID(ctx, r.client, instance.Spec.VolumeName, instance.Namespace)
		if err != nil {
			msg := fmt.Sprintf("Failed to get volumeID from volumeName: %q for CnsNodeVmAttachment request with name: %q on namespace: %q. Error: %+v",
				instance.Spec.VolumeName, request.Name, request.Namespace, err)
			instance.Status.Error = err.Error()
			updateCnsNodeVMAttachment(ctx, r.client, instance)
			recordEvent(ctx, r, instance, v1.EventTypeWarning, msg)
			return reconcile.Result{RequeueAfter: timeout}, nil
		}
		cnsFinalizerExists := false
		// Check if finalizer already exists.
		for _, finalizer := range instance.Finalizers {
			if finalizer == cnsoperatortypes.CNSFinalizer {
				cnsFinalizerExists = true
				break
			}
		}
		// Update finalizer and attachmentMetadata together in CnsNodeVMAttachment.
		if !cnsFinalizerExists {
			// Add finalizer.
			instance.Finalizers = append(instance.Finalizers, cnsoperatortypes.CNSFinalizer)
			/*
				Add the CNS volume ID in the attachment metadata. This is used later to detach the CNS volume on
				deletion of CnsNodeVMAttachment instance. Note that the supervisor PVC can be deleted due to following:
				1. Bug in external provisioner(https://github.com/kubernetes/kubernetes/issues/84226) where DeleteVolume
				   could be invoked in pvcsi before ControllerUnpublishVolume. This causes supervisor PVC to be deleted.
				2. Supervisor namespace user deletes PVC used by a guest cluster.
				3. Supervisor namespace is deleted
				Basically, we cannot rely on the existence of PVC in supervisor cluster for detaching the volume from
				guest cluster VM. So, the logic stores the CNS volume ID in attachmentMetadata itself which is used
				during detach.
			*/
			attachmentMetadata := make(map[string]string)
			attachmentMetadata[cnsnodevmattachmentv1alpha1.AttributeCnsVolumeID] = volumeID
			instance.Status.AttachmentMetadata = attachmentMetadata
			err = updateCnsNodeVMAttachment(ctx, r.client, instance)
			if err != nil {
				msg := fmt.Sprintf("Failed to update CnsNodeVmAttachment instance: %q on namespace: %q. Error: %+v",
					request.Name, request.Namespace, err)
				recordEvent(ctx, r, instance, v1.EventTypeWarning, msg)
				return reconcile.Result{RequeueAfter: timeout}, nil
			}
		}

		log.Debugf("vSphere CNS driver is attaching volume: %q to nodevm: %+v for CnsNodeVmAttachment request with name: %q on namespace: %q",
			volumeID, nodeVM, request.Name, request.Namespace)
		diskUUID, attachErr := volumes.GetManager(ctx, vcenter).AttachVolume(ctx, nodeVM, volumeID)

		if attachErr != nil {
			log.Errorf("Failed to attach disk: %q to nodevm: %+v for CnsNodeVmAttachment request with name: %q on namespace: %q. Err: %+v",
				volumeID, nodeVM, request.Name, request.Namespace, attachErr)
		}

		if !cnsFinalizerExists {
			// Read the CnsNodeVMAttachment instance again because the instance is already modified
			err = r.client.Get(ctx, request.NamespacedName, instance)
			if err != nil {
				msg := fmt.Sprintf("Error reading the CnsNodeVmAttachment with name: %q on namespace: %q. Err: %+v",
					request.Name, request.Namespace, err)
				// Error reading the object - requeue the request.
				recordEvent(ctx, r, instance, v1.EventTypeWarning, msg)
				return reconcile.Result{RequeueAfter: timeout}, nil
			}
		}

		if attachErr != nil {
			// Update CnsNodeVMAttachment instance with attach error message
			instance.Status.Error = attachErr.Error()
		} else {
			// Update CnsNodeVMAttachment instance with attached status set to true
			// and attachment metadata
			instance.Status.AttachmentMetadata[cnsnodevmattachmentv1alpha1.AttributeFirstClassDiskUUID] = diskUUID
			instance.Status.Attached = true
			// Clear the error message
			instance.Status.Error = ""
		}

		err = updateCnsNodeVMAttachment(ctx, r.client, instance)
		if err != nil {
			msg := fmt.Sprintf("Failed to update attach status on CnsNodeVmAttachment instance: %q on namespace: %q. Error: %+v",
				request.Name, request.Namespace, err)
			recordEvent(ctx, r, instance, v1.EventTypeWarning, msg)
			return reconcile.Result{RequeueAfter: timeout}, nil
		}

		if attachErr != nil {
			recordEvent(ctx, r, instance, v1.EventTypeWarning, "")
			return reconcile.Result{RequeueAfter: timeout}, nil
		}

		msg := fmt.Sprintf("ReconcileCnsNodeVMAttachment: Successfully updated entry in CNS for instance "+
			"with name %q and namespace %q.", request.Name, request.Namespace)
		recordEvent(ctx, r, instance, v1.EventTypeNormal, msg)
		// Cleanup instance entry from backOffDuration map
		delete(backOffDuration, instance.Name)
		return reconcile.Result{}, nil
	}

	if instance.DeletionTimestamp != nil {
		nodeVM, err := dc.GetVirtualMachineByUUID(ctx, nodeUUID, false)
		if err != nil {
			msg := fmt.Sprintf("Failed to find the VM with UUID: %q for CnsNodeVmAttachment request with name: %q on namespace: %q. Err: %+v",
				nodeUUID, request.Name, request.Namespace, err)
			// TODO : Need to check for VirtualMachine CRD instance existence.
			// This check is needed in scenarios where VC inventory is stale due to upgrade or back-up and restore
			removeFinalizerFromCRDInstance(ctx, instance, request)
			updateCnsNodeVMAttachment(ctx, r.client, instance)
			recordEvent(ctx, r, instance, v1.EventTypeNormal, msg)
			return reconcile.Result{}, nil

		}
		var cnsVolumeID string
		var ok bool
		if cnsVolumeID, ok = instance.Status.AttachmentMetadata[cnsnodevmattachmentv1alpha1.AttributeCnsVolumeID]; !ok {
			log.Debugf("CnsNodeVmAttachment does not have CNS volume ID. AttachmentMetadata: %+v", instance.Status.AttachmentMetadata)
			msg := "CnsNodeVmAttachment does not have CNS volume ID."
			recordEvent(ctx, r, instance, v1.EventTypeWarning, msg)
			return reconcile.Result{RequeueAfter: timeout}, nil
		}
		log.Debugf("vSphere CNS driver is detaching volume: %q to nodevm: %+v for CnsNodeVmAttachment request with name: %q on namespace: %q",
			cnsVolumeID, nodeVM, request.Name, request.Namespace)
		detachErr := volumes.GetManager(ctx, vcenter).DetachVolume(ctx, nodeVM, cnsVolumeID)
		if detachErr != nil {
			if vsphere.IsManagedObjectNotFound(detachErr) {
				msg := fmt.Sprintf("Found a managed object not found fault for vm: %+v", nodeVM)
				removeFinalizerFromCRDInstance(ctx, instance, request)
				updateCnsNodeVMAttachment(ctx, r.client, instance)
				recordEvent(ctx, r, instance, v1.EventTypeNormal, msg)
				// Cleanup instance entry from backOffDuration map
				delete(backOffDuration, instance.Name)
				return reconcile.Result{}, nil
			}
			// Update CnsNodeVMAttachment instance with detach error message
			log.Errorf("Failed to detach disk: %q to nodevm: %+v for CnsNodeVmAttachment request with name: %q on namespace: %q. Err: %+v",
				cnsVolumeID, nodeVM, request.Name, request.Namespace, detachErr)
			instance.Status.Error = detachErr.Error()
		} else {
			removeFinalizerFromCRDInstance(ctx, instance, request)
		}
		err = updateCnsNodeVMAttachment(ctx, r.client, instance)
		if err != nil {
			msg := fmt.Sprintf("Failed to update detach status on CnsNodeVmAttachment instance: %q on namespace: %q. Error: %+v",
				request.Name, request.Namespace, err)
			recordEvent(ctx, r, instance, v1.EventTypeWarning, msg)
			return reconcile.Result{RequeueAfter: timeout}, nil
		}
		if detachErr != nil {
			recordEvent(ctx, r, instance, v1.EventTypeWarning, "")
			return reconcile.Result{RequeueAfter: timeout}, nil
		}
		msg := fmt.Sprintf("ReconcileCnsNodeVMAttachment: Successfully updated entry in CNS for instance "+
			"with name %q and namespace %q.", request.Name, request.Namespace)
		recordEvent(ctx, r, instance, v1.EventTypeNormal, msg)
	}
	// Cleanup instance entry from backOffDuration map
	delete(backOffDuration, instance.Name)
	return reconcile.Result{}, nil
}

// removeFinalizerFromCRDInstance will remove the CNS Finalizer = cns.vmware.com, from a given nodevmattachment instance
func removeFinalizerFromCRDInstance(ctx context.Context, instance *cnsnodevmattachmentv1alpha1.CnsNodeVmAttachment, request reconcile.Request) {
	log := logger.GetLogger(ctx)
	for i, finalizer := range instance.Finalizers {
		if finalizer == cnsoperatortypes.CNSFinalizer {
			log.Debugf("Removing %q finalizer from CnsNodeVmAttachment instance with name: %q on namespace: %q",
				cnsoperatortypes.CNSFinalizer, request.Name, request.Namespace)
			instance.Finalizers = append(instance.Finalizers[:i], instance.Finalizers[i+1:]...)
		}
	}
}

// getVCDatacenterFromConfig returns datacenter registered for each vCenter
func getVCDatacentersFromConfig(cfg *config.Config) (map[string][]string, error) {
	var err error
	vcdcMap := make(map[string][]string)
	for key, value := range cfg.VirtualCenter {
		dcList := strings.Split(value.Datacenters, ",")
		for _, dc := range dcList {
			dcMoID := strings.TrimSpace(dc)
			if dcMoID != "" {
				vcdcMap[key] = append(vcdcMap[key], dcMoID)
			}
		}
	}
	if len(vcdcMap) == 0 {
		err = errors.New("Unable get vCenter datacenters from vsphere config")
	}
	return vcdcMap, err
}

// getVolumeID gets the volume ID from the PV that is bound to PVC by pvcName
func getVolumeID(ctx context.Context, client client.Client, pvcName string, namespace string) (string, error) {
	log := logger.GetLogger(ctx)
	// Get PVC by pvcName from namespace
	pvc := &v1.PersistentVolumeClaim{}
	err := client.Get(ctx, k8stypes.NamespacedName{Name: pvcName, Namespace: namespace}, pvc)
	if err != nil {
		log.Errorf("Failed to get PVC with volumename: %q on namespace: %q. Err: %+v",
			pvcName, namespace, err)
		return "", err
	}

	// Get PV by name
	pv := &v1.PersistentVolume{}
	err = client.Get(ctx, k8stypes.NamespacedName{Name: pvc.Spec.VolumeName, Namespace: ""}, pv)
	if err != nil {
		log.Errorf("Failed to get PV with name: %q for PVC: %q. Err: %+v",
			pvc.Spec.VolumeName, pvcName, err)
		return "", err
	}
	return pv.Spec.CSI.VolumeHandle, nil
}

func updateCnsNodeVMAttachment(ctx context.Context, client client.Client, instance *cnsnodevmattachmentv1alpha1.CnsNodeVmAttachment) error {
	log := logger.GetLogger(ctx)
	err := client.Update(ctx, instance)
	if err != nil {
		log.Errorf("Failed to update CnsNodeVmAttachment instance: %q on namespace: %q. Error: %+v",
			instance.Name, instance.Namespace, err)
	}
	return err
}

// getMaxWorkerThreadsToReconcileCnsNodeVmAttachment returns the maximum
// number of worker threads which can be run to reconcile CnsNodeVmAttachment instances.
// If environment variable WORKER_THREADS_NODEVM_ATTACH is set and valid,
// return the value read from enviroment variable otherwise, use the default value
func getMaxWorkerThreadsToReconcileCnsNodeVmAttachment(ctx context.Context) int {
	log := logger.GetLogger(ctx)
	workerThreads := defaultMaxWorkerThreadsForNodeVMAttach
	if v := os.Getenv("WORKER_THREADS_NODEVM_ATTACH"); v != "" {
		if value, err := strconv.Atoi(v); err == nil {
			if value <= 0 {
				log.Warnf("Maximum number of worker threads to run set in env variable WORKER_THREADS_NODEVM_ATTACH %s is less than 1, will use the default value %d", v, defaultMaxWorkerThreadsForNodeVMAttach)
			} else if value > defaultMaxWorkerThreadsForNodeVMAttach {
				log.Warnf("Maximum number of worker threads to run set in env variable WORKER_THREADS_NODEVM_ATTACH %s is greater than %d, will use the default value %d",
					v, defaultMaxWorkerThreadsForNodeVMAttach, defaultMaxWorkerThreadsForNodeVMAttach)
			} else {
				workerThreads = value
				log.Debugf("Maximum number of worker threads to run to reconcile CnsNodeVmAttachment instances is set to %d", workerThreads)
			}
		} else {
			log.Warnf("Maximum number of worker threads to run set in env variable WORKER_THREADS_NODEVM_ATTACH %s is invalid, will use the default value %d", v, defaultMaxWorkerThreadsForNodeVMAttach)
		}
	} else {
		log.Debugf("WORKER_THREADS_NODEVM_ATTACH is not set. Picking the default value %d", defaultMaxWorkerThreadsForNodeVMAttach)
	}
	return workerThreads
}

// recordEvent records the event, sets the backOffDuration for the instance appropriately
// and logs the message.
// backOffDuration is reset to 1 second on success and doubled on failure.
func recordEvent(ctx context.Context, r *ReconcileCnsNodeVMAttachment, instance *cnsnodevmattachmentv1alpha1.CnsNodeVmAttachment, eventtype string, msg string) {
	log := logger.GetLogger(ctx)
	switch eventtype {
	case v1.EventTypeWarning:
		// Double backOff duration
		backOffDuration[instance.Name] = backOffDuration[instance.Name] * 2
		r.recorder.Event(instance, v1.EventTypeWarning, "NodeVMAttachFailed", msg)
		log.Error(msg)
	case v1.EventTypeNormal:
		// Reset backOff duration to one second
		backOffDuration[instance.Name] = time.Second
		r.recorder.Event(instance, v1.EventTypeNormal, "NodeVMAttachSucceeded", msg)
		log.Info(msg)
	}
}
