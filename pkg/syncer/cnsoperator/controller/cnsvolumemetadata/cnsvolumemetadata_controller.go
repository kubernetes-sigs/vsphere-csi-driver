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

package cnsvolumemetadata

import (
	"context"
	"fmt"
	"reflect"
	"time"

	"github.com/davecgh/go-spew/spew"
	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/kubernetes/scheme"
	"k8s.io/client-go/tools/record"
	"k8s.io/klog"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller"
	"sigs.k8s.io/controller-runtime/pkg/event"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	"sigs.k8s.io/controller-runtime/pkg/manager"
	"sigs.k8s.io/controller-runtime/pkg/predicate"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
	"sigs.k8s.io/controller-runtime/pkg/source"

	cnstypes "gitlab.eng.vmware.com/hatchway/govmomi/cns/types"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	typedcorev1 "k8s.io/client-go/kubernetes/typed/core/v1"
	volumes "sigs.k8s.io/vsphere-csi-driver/pkg/common/cns-lib/volume"
	cnsvsphere "sigs.k8s.io/vsphere-csi-driver/pkg/common/cns-lib/vsphere"
	csitypes "sigs.k8s.io/vsphere-csi-driver/pkg/csi/types"
	k8s "sigs.k8s.io/vsphere-csi-driver/pkg/kubernetes"
	cnsv1alpha1 "sigs.k8s.io/vsphere-csi-driver/pkg/syncer/cnsoperator/apis/cnsvolumemetadata/v1alpha1"
	cnsoperatortypes "sigs.k8s.io/vsphere-csi-driver/pkg/syncer/cnsoperator/types"
	"sigs.k8s.io/vsphere-csi-driver/pkg/syncer/types"
)

// backOffDuration is a map of cnsvolumemetadata name's to the time after which a request
// for this instance will be requeued.
// Initialized to 1 second for new instances and for instances whose latest reconcile
// operation succeeded.
// If the reconcile fails, backoff is incremented exponentially.
var backOffDuration map[string]time.Duration

// Add creates a new CnsVolumeMetadata Controller and adds it to the Manager, ConfigInfo,
// volumeManager and k8sclient. The Manager will set fields on the Controller
// and Start it when the Manager is Started.
func Add(mgr manager.Manager, configInfo *types.ConfigInfo, volumeManager volumes.Manager) error {
	// Initializes kubernetes client
	k8sclient, err := k8s.NewClient()
	if err != nil {
		klog.Errorf("Creating Kubernetes client failed. Err: %v", err)
		return err
	}

	// eventBroadcaster broadcasts events on cnsvolumemetadata instances to the event sink
	eventBroadcaster := record.NewBroadcaster()
	eventBroadcaster.StartRecordingToSink(
		&typedcorev1.EventSinkImpl{
			Interface: k8sclient.CoreV1().Events(""),
		},
	)
	recorder := eventBroadcaster.NewRecorder(scheme.Scheme, v1.EventSource{Component: "cns.vmware.com"})
	return add(mgr, newReconciler(mgr, configInfo, volumeManager, k8sclient, recorder))
}

// newReconciler returns a new reconcile.Reconciler
func newReconciler(mgr manager.Manager, configInfo *types.ConfigInfo, volumeManager volumes.Manager, k8sclient kubernetes.Interface, recorder record.EventRecorder) reconcile.Reconciler {
	return &ReconcileCnsVolumeMetadata{client: mgr.GetClient(), scheme: mgr.GetScheme(), configInfo: configInfo, volumeManager: volumeManager, k8sclient: k8sclient, recorder: recorder}
}

// add adds a new Controller to mgr with r as the reconcile.Reconciler
func add(mgr manager.Manager, r reconcile.Reconciler) error {
	// Create a new controller
	c, err := controller.New("cnsvolumemetadata-controller", mgr, controller.Options{Reconciler: r})
	if err != nil {
		return err
	}
	backOffDuration = make(map[string]time.Duration)
	src := &source.Kind{Type: &cnsv1alpha1.CnsVolumeMetadata{}}
	h := &handler.EnqueueRequestForObject{}
	// Predicates are used to determine under which conditions
	// the reconcile callback will be made for an instance.
	pred := predicate.Funcs{
		CreateFunc: func(e event.CreateEvent) bool {
			return true
		},
		UpdateFunc: func(e event.UpdateEvent) bool {
			oldObj, ok := e.ObjectOld.(*cnsv1alpha1.CnsVolumeMetadata)
			if oldObj == nil || !ok {
				return false
			}
			newObj, ok := e.ObjectNew.(*cnsv1alpha1.CnsVolumeMetadata)
			if newObj == nil || !ok {
				return false
			}
			// Return true if finalizer or spec has changed.
			// Return true if deletion timestamp is non-nil and the finalizer is still set.
			// Return false for updates to any other fields.
			// Finalizer is added and removed by CNS Operator.
			return !(reflect.DeepEqual(oldObj.Finalizers, newObj.Finalizers) && reflect.DeepEqual(oldObj.Spec, newObj.Spec)) || (newObj.DeletionTimestamp != nil && newObj.Finalizers != nil)
		},
		DeleteFunc: func(e event.DeleteEvent) bool {
			// Instances are deleted only after CNS Operator has removed its finalizer from that instance.
			// No reconcile operations need to take place after the finalizer is removed.
			return false
		},
	}

	// Watch for changes to primary resource CnsVolumeMetadata
	err = c.Watch(src, h, pred)
	if err != nil {
		return err
	}

	return nil
}

// blank assignment to verify that ReconcileCnsVolumeMetadata implements reconcile.Reconciler
var _ reconcile.Reconciler = &ReconcileCnsVolumeMetadata{}

// ReconcileCnsVolumeMetadata reconciles a CnsVolumeMetadata object
type ReconcileCnsVolumeMetadata struct {
	client        client.Client
	scheme        *runtime.Scheme
	configInfo    *types.ConfigInfo
	volumeManager volumes.Manager
	k8sclient     kubernetes.Interface
	recorder      record.EventRecorder
}

// Reconcile reads that state of the cluster for a CnsVolumeMetadata object and makes changes on CNS
// based on the state read in the CnsVolumeMetadata.Spec
// The Controller will requeue the Request to be processed again if the returned error is non-nil or
// Result.Requeue is true, otherwise upon completion it will remove the work from the queue.
func (r *ReconcileCnsVolumeMetadata) Reconcile(request reconcile.Request) (reconcile.Result, error) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	instance := &cnsv1alpha1.CnsVolumeMetadata{}
	err := r.client.Get(ctx, request.NamespacedName, instance)
	if err != nil {
		if errors.IsNotFound(err) {
			klog.V(2).Infof("ReconcileCnsVolumeMetadata: Failed to get CnsVolumeMetadata instance %q. Ignoring request.", request.Name)
			return reconcile.Result{}, nil
		}
		// Error reading the object - requeue the request.
		klog.Errorf("ReconcileCnsVolumeMetadata: Error reading the CnsVolumeMetadata instance with name: %q on namespace: %q. Err: %+v",
			request.Name, request.Namespace, err)
		return reconcile.Result{}, err
	}

	klog.V(2).Infof("ReconcileCnsVolumeMetadata: Received request for instance %q and type %q", instance.Name, instance.Spec.EntityType)

	// Initialize backOffDuration for the instance, if required.
	var timeout time.Duration
	if _, exists := backOffDuration[instance.Name]; !exists {
		backOffDuration[instance.Name] = time.Second
	}
	timeout = backOffDuration[instance.Name]

	// Validate input instance fields
	if err = validateReconileRequest(instance); err != nil {
		msg := fmt.Sprintf("ReconcileCnsVolumeMetadata: Failed to validate reconcile request with error: %v", err)
		recordEvent(r, instance, v1.EventTypeWarning, msg)
		return reconcile.Result{RequeueAfter: timeout}, nil
	}

	// If deletion timestamp is set, instance is marked for deletion by k8s.
	// Remove corresponding metadata from CNS.
	// If the operation succeeds, remove the finalizer.
	// If the operation fails, requeue the request.
	if instance.DeletionTimestamp != nil {
		if !r.updateCnsMetadata(ctx, instance, true) {
			// Failed to update CNS
			msg := fmt.Sprintf("ReconcileCnsVolumeMetadata: Failed to delete entry in CNS for instance "+
				"with name %q and entity type %q in the guest cluster. Requeuing request.",
				instance.Spec.EntityName, instance.Spec.EntityType)
			recordEvent(r, instance, v1.EventTypeWarning, msg)
			// Update instance.status fields with the errors per volume.
			if err = r.client.Update(ctx, instance); err != nil {
				msg := fmt.Sprintf("ReconcileCnsVolumeMetadata: Failed to update status for %q. "+
					"Err: %v.", instance.Name, err)
				recordEvent(r, instance, v1.EventTypeWarning, msg)
			}
			// updateCnsMetadata failed, so the request will be requeued.
			return reconcile.Result{RequeueAfter: timeout}, err
		}

		// Remove finalizer as update on CNS was successful.
		for index, finalizer := range instance.Finalizers {
			if finalizer == cnsoperatortypes.CNSFinalizer {
				klog.V(4).Infof("ReconcileCnsVolumeMetadata: Removing finalizer %q for instance %q", finalizer, instance.Name)
				instance.Finalizers = append(instance.Finalizers[:index], instance.Finalizers[index+1:]...)
				if err = r.client.Update(ctx, instance); err != nil {
					msg := fmt.Sprintf("ReconcileCnsVolumeMetadata: Failed to remove finalizer %q for %q. "+
						"Err: %v. Requeueing request.", finalizer, instance.Name, err)
					recordEvent(r, instance, v1.EventTypeWarning, msg)
					return reconcile.Result{RequeueAfter: timeout}, err
				}
				klog.V(4).Infof("ReconcileCnsVolumeMetadata: Successfully removed finalizer %q for instance %q", finalizer, instance.Name)
			}
		}
		// Cleanup instance entry from backOffDuration map
		delete(backOffDuration, instance.Name)
		return reconcile.Result{}, nil
	}

	// Deletion timestamp was not set.
	// Instance was either created or updated on the supervisor API server.
	isFinalizerSet := false
	for _, finalizer := range instance.Finalizers {
		if finalizer == cnsoperatortypes.CNSFinalizer {
			isFinalizerSet = true
			break
		}
	}

	// Set finalizer if it was not set already on this instance
	if !isFinalizerSet {
		instance.Finalizers = append(instance.Finalizers, cnsoperatortypes.CNSFinalizer)
		if err = r.client.Update(ctx, instance); err != nil {
			msg := fmt.Sprintf("ReconcileCnsVolumeMetadata: Failed to add finalizer %q for %q. "+
				"Err: %v. Requeueing request.", cnsoperatortypes.CNSFinalizer, instance.Name, err)
			recordEvent(r, instance, v1.EventTypeWarning, msg)
			return reconcile.Result{RequeueAfter: timeout}, err
		}
	} else {
		// Update CNS volume entry with instance's metadata
		if !r.updateCnsMetadata(ctx, instance, false) {
			// Failed to update CNS.
			msg := fmt.Sprintf("ReconcileCnsVolumeMetadata: Failed to update entry in CNS for instance "+
				"with name %q and entity type %q in the guest cluster. Requeueing request.",
				instance.Spec.EntityName, instance.Spec.EntityType)
			recordEvent(r, instance, v1.EventTypeWarning, msg)
			// Update instance.status fields on supervisor API server and requeue the request.
			_ = r.client.Update(ctx, instance)
			return reconcile.Result{RequeueAfter: timeout}, nil
		}
		// Successfully updated CNS.
		msg := fmt.Sprintf("ReconcileCnsVolumeMetadata: Successfully updated entry in CNS for instance "+
			"with name %q and entity type %q in the guest cluster.",
			instance.Spec.EntityName, instance.Spec.EntityType)
		recordEvent(r, instance, v1.EventTypeNormal, msg)
		// Update instance.status fields on supervisor API server.
		if err = r.client.Update(ctx, instance); err != nil {
			msg := fmt.Sprintf("ReconcileCnsVolumeMetadata: Failed to update status for %q. "+
				"Err: %v. Requeueing request.", instance.Name, err)
			recordEvent(r, instance, v1.EventTypeWarning, msg)
			return reconcile.Result{RequeueAfter: timeout}, err
		}
	}
	return reconcile.Result{}, nil
}

// updateCnsMetadata updates the volume entry on CNS.
// If deleteFlag is true, metadata is deleted for the given instance.
// Returns true if all updates on CNS succeeded, otherwise return false.
func (r *ReconcileCnsVolumeMetadata) updateCnsMetadata(ctx context.Context, instance *cnsv1alpha1.CnsVolumeMetadata, deleteFlag bool) bool {
	klog.V(4).Infof("ReconcileCnsVolumeMetadata: Calling updateCnsMetadata for instance %q with delete flag %v", instance.Name, deleteFlag)
	vCenter, err := types.GetVirtualCenterInstance(r.configInfo)
	if err != nil {
		klog.Errorf("ReconcileCnsVolumeMetadata: Failed to get virtual center instance. Err: %v", err)
		return false
	}
	if vCenter.Config == nil {
		klog.Errorf("ReconcileCnsVolumeMetadata: vcenter config is empty")
		return false
	}
	host := vCenter.Config.Host

	var entityReferences []cnstypes.CnsKubernetesEntityReference
	for _, reference := range instance.Spec.EntityReferences {
		clusterid := reference.ClusterID
		if instance.Spec.EntityType == cnsv1alpha1.CnsOperatorEntityTypePV {
			clusterid = r.configInfo.Cfg.Global.ClusterID
		}
		entityReferences = append(entityReferences, cnsvsphere.CreateCnsKuberenetesEntityReference(reference.EntityType, reference.EntityName, reference.Namespace, clusterid))
	}

	var metadataList []cnstypes.BaseCnsEntityMetadata
	metadata := cnsvsphere.GetCnsKubernetesEntityMetaData(instance.Spec.EntityName, instance.Spec.Labels, deleteFlag, string(instance.Spec.EntityType), instance.Spec.Namespace, instance.Spec.GuestClusterID, entityReferences)
	metadataList = append(metadataList, cnstypes.BaseCnsEntityMetadata(metadata))

	var volumeStatus []*cnsv1alpha1.CnsVolumeMetadataVolumeStatus
	success := true
	for _, volume := range instance.Spec.VolumeNames {
		status := cnsv1alpha1.GetCnsOperatorVolumeStatus(volume, "")
		status.Updated = true
		volumeStatus = append(volumeStatus, &status)

		// Get pvc object in the supervisor cluster that this instance refers to.
		pvc, err := r.k8sclient.CoreV1().PersistentVolumeClaims(instance.Namespace).Get(volume, metav1.GetOptions{})
		if err != nil {
			klog.Errorf("ReconcileCnsVolumeMetadata: Failed to get PVC %q in namespace %q. Err: %v", volume, instance.Namespace, err)
			if errors.IsNotFound(err) && deleteFlag {
				klog.V(2).Info("Assuming volume entry is deleted from CNS.")
				continue
			} else {
				status.ErrorMessage = err.Error()
				status.Updated = false
				success = false
				continue
			}
		}

		// Get the corresponding pv object bound to the pvc.
		pv, err := r.k8sclient.CoreV1().PersistentVolumes().Get(pvc.Spec.VolumeName, metav1.GetOptions{})
		if err != nil {
			klog.Errorf("ReconcileCnsVolumeMetadata: Failed to get PV %q. Err: %v", pvc.Spec.VolumeName, err)
			status.ErrorMessage = err.Error()
			status.Updated = false
			success = false
			continue
		}
		if pv.Spec.CSI == nil || pv.Spec.CSI.Driver != csitypes.Name {
			continue
		}

		cluster := cnsvsphere.GetContainerCluster(instance.Spec.GuestClusterID, r.configInfo.Cfg.VirtualCenter[host].User, cnstypes.CnsClusterFlavorGuest)
		updateSpec := &cnstypes.CnsVolumeMetadataUpdateSpec{
			VolumeId: cnstypes.CnsVolumeId{
				Id: pv.Spec.CSI.VolumeHandle,
			},
			Metadata: cnstypes.CnsVolumeMetadata{
				ContainerCluster:      cluster,
				ContainerClusterArray: []cnstypes.CnsContainerCluster{cluster},
				EntityMetadata:        metadataList,
			},
		}
		klog.V(4).Infof("ReconcileCnsVolumeMetadata: Calling UpdateVolumeMetadata for "+
			"volume %q of instance %q with updateSpec: %+v", volume, instance.Name, spew.Sdump(updateSpec))
		if err := r.volumeManager.UpdateVolumeMetadata(updateSpec); err != nil {
			klog.Errorf("ReconcileCnsVolumeMetadata: UpdateVolumeMetadata failed with err %v", err)
			status.ErrorMessage = err.Error()
			status.Updated = false
			success = false
		}
	}

	// Modify status field of instance
	// Update on API server will be made by the calling function
	instance.Status.VolumeStatus = nil
	for _, status := range volumeStatus {
		instance.Status.VolumeStatus = append(instance.Status.VolumeStatus, *status)
	}
	return success
}

// validateReconileRequest validates the fields of the request against the
// cnsvolumemetadata API.
// Returns an error if any validation fails.
func validateReconileRequest(req *cnsv1alpha1.CnsVolumeMetadata) error {
	var err error

	if req.Spec.EntityName == "" || req.Spec.EntityType == "" || req.Spec.GuestClusterID == "" {
		return errors.NewBadRequest("EntityName, EntityType and GuestClusterID are required parameters.")
	}
	switch req.Spec.EntityType {
	case cnsv1alpha1.CnsOperatorEntityTypePV:
		if req.Spec.Namespace != "" {
			err = errors.NewBadRequest("Namespace cannot be set for PERSISTENT_VOLUME instances")
		}
		if len(req.Spec.VolumeNames) != 1 || len(req.Spec.EntityReferences) != 1 {
			err = errors.NewBadRequest("VolumeNames and EntityReferences should have length 1 for PERSISTENT_VOLUME instances")
		}
		for _, reference := range req.Spec.EntityReferences {
			if reference.EntityType != string(cnsv1alpha1.CnsOperatorEntityTypePVC) {
				err = errors.NewBadRequest("PERSISTENT_VOLUME instances can only refer to PERSISTENT_VOLUME_CLAIM instances")
			}
			if reference.ClusterID != "" {
				err = errors.NewBadRequest("EntityReferences.ClusterID should be empty for PERSISTENT_VOLUME instances")
			}
		}
	case cnsv1alpha1.CnsOperatorEntityTypePVC:
		if req.Spec.Namespace == "" {
			err = errors.NewBadRequest("Namespace should be set for PERSISTENT_VOLUME_CLAIM instances")
		}
		if len(req.Spec.VolumeNames) != 1 || len(req.Spec.EntityReferences) != 1 {
			err = errors.NewBadRequest("VolumeNames and EntityReferences should have length 1 for PERSISTENT_VOLUME_CLAIM instances")
		}
		for _, reference := range req.Spec.EntityReferences {
			if reference.EntityType != string(cnsv1alpha1.CnsOperatorEntityTypePV) {
				err = errors.NewBadRequest("PERSISTENT_VOLUME_CLAIM instances can only refer to PERSISTENT_VOLUME instances")
			}
			if reference.ClusterID == "" {
				err = errors.NewBadRequest("EntityReferences.ClusterID should not be empty for PERSISTENT_VOLUME_CLAIM instances")
			}
		}
	case cnsv1alpha1.CnsOperatorEntityTypePOD:
		if req.Spec.Namespace == "" {
			err = errors.NewBadRequest("Namespace should be set for POD instances")
		}
		if req.Spec.Labels != nil {
			err = errors.NewBadRequest("Labels cannot be set for POD instances")
		}
		if len(req.Spec.VolumeNames) == 0 || len(req.Spec.EntityReferences) == 0 {
			err = errors.NewBadRequest("VolumeNames and EntityReferences should have length greater than 0 for POD instances")
		}
		for _, reference := range req.Spec.EntityReferences {
			if reference.EntityType != string(cnsv1alpha1.CnsOperatorEntityTypePVC) {
				err = errors.NewBadRequest("POD instances can only refer to PERSISTENT_VOLUME_CLAIM instances")
			}
			if reference.ClusterID == "" {
				err = errors.NewBadRequest("EntityReferences.ClusterID should not be empty for POD instances")
			}
		}
	default:
		err = errors.NewBadRequest(fmt.Sprintf("Invalid entity type %q", req.Spec.EntityType))
	}
	return err

}

// recordEvent records the event, sets the backOffDuration for the instance appropriately
// and logs the message.
// backOffDuration is reset to 1 second on success and doubled on failure.
func recordEvent(r *ReconcileCnsVolumeMetadata, instance *cnsv1alpha1.CnsVolumeMetadata, eventtype string, msg string) {
	switch eventtype {
	case v1.EventTypeWarning:
		// Double backOff duration
		backOffDuration[instance.Name] = backOffDuration[instance.Name] * 2
		r.recorder.Event(instance, v1.EventTypeWarning, "UpdateFailed", msg)
		klog.Error(msg)
	case v1.EventTypeNormal:
		// Reset backOff duration to one second
		backOffDuration[instance.Name] = time.Second
		r.recorder.Event(instance, v1.EventTypeNormal, "UpdateSucceeded", msg)
		klog.V(2).Info(msg)
	}
}
