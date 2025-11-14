/*
Copyright 2024 The Kubernetes Authors.

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

package cnsunregistervolume

import (
	"context"
	"fmt"
	"sync"
	"time"

	vmoperatortypes "github.com/vmware-tanzu/vm-operator/api/v1alpha2"
	cnstypes "github.com/vmware/govmomi/cns/types"
	v1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	clientset "k8s.io/client-go/kubernetes"
	"k8s.io/client-go/kubernetes/scheme"
	typedcorev1 "k8s.io/client-go/kubernetes/typed/core/v1"
	"k8s.io/client-go/tools/record"
	"k8s.io/client-go/util/retry"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"sigs.k8s.io/controller-runtime/pkg/controller"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	"sigs.k8s.io/controller-runtime/pkg/manager"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
	"sigs.k8s.io/controller-runtime/pkg/source"

	apis "sigs.k8s.io/vsphere-csi-driver/v3/pkg/apis/cnsoperator"
	cnsunregistervolumev1alpha1 "sigs.k8s.io/vsphere-csi-driver/v3/pkg/apis/cnsoperator/cnsunregistervolume/v1alpha1"
	volumes "sigs.k8s.io/vsphere-csi-driver/v3/pkg/common/cns-lib/volume"
	cnsvsphere "sigs.k8s.io/vsphere-csi-driver/v3/pkg/common/cns-lib/vsphere"
	commonconfig "sigs.k8s.io/vsphere-csi-driver/v3/pkg/common/config"
	"sigs.k8s.io/vsphere-csi-driver/v3/pkg/common/utils"
	"sigs.k8s.io/vsphere-csi-driver/v3/pkg/csi/service/common"
	"sigs.k8s.io/vsphere-csi-driver/v3/pkg/csi/service/common/commonco"
	"sigs.k8s.io/vsphere-csi-driver/v3/pkg/csi/service/logger"
	k8s "sigs.k8s.io/vsphere-csi-driver/v3/pkg/kubernetes"
	"sigs.k8s.io/vsphere-csi-driver/v3/pkg/syncer"
)

const (
	defaultMaxWorkerThreadsForUnregisterVolume = 40
	metadata                                   = "VOLUME_METADATA"
)

var (
	// backOffDuration is a map of cnsunregistervolume name's to the time after which
	// a request for this instance will be requeued.
	// Initialized to 1 second for new instances and for instances whose latest
	// reconcile operation succeeded.
	// If the reconcile fails, backoff is incremented exponentially.
	backOffDuration         map[string]time.Duration
	backOffDurationMapMutex = sync.Mutex{}
)

// Add creates a new CnsUnregisterVolume Controller and adds it to the Manager,
// ConfigurationInfo and VirtualCenterTypes. The Manager will set fields on
// the Controller and Start it when the Manager is Started.
func Add(mgr manager.Manager, clusterFlavor cnstypes.CnsClusterFlavor,
	configInfo *commonconfig.ConfigurationInfo, volumeManager volumes.Manager) error {
	ctx, log := logger.GetNewContextWithLogger()
	if clusterFlavor != cnstypes.CnsClusterFlavorWorkload {
		log.Debug("Not initializing the CnsUnregisterVolume Controller as its a non-WCP CSI deployment")
		return nil
	}

	var coCommonInterface commonco.COCommonInterface
	var err error
	coCommonInterface, err = commonco.GetContainerOrchestratorInterface(ctx,
		common.Kubernetes, clusterFlavor, &syncer.COInitParams)
	if err != nil {
		log.Errorf("failed to create CO agnostic interface. Err: %v", err)
		return err
	}
	if !coCommonInterface.IsFSSEnabled(ctx, common.CnsUnregisterVolume) {
		log.Infof("Not initializing the CnsUnregisterVolume Controller as this feature is disabled on the cluster")
		return nil
	}

	// Initializes kubernetes client.
	k8sclient, err := k8s.NewClient(ctx)
	if err != nil {
		log.Errorf("Creating Kubernetes client failed. Err: %v", err)
		return err
	}

	// eventBroadcaster broadcasts events on cnsunregistervolume instances to the
	// event sink.
	eventBroadcaster := record.NewBroadcaster()
	eventBroadcaster.StartRecordingToSink(
		&typedcorev1.EventSinkImpl{
			Interface: k8sclient.CoreV1().Events(""),
		},
	)
	recorder := eventBroadcaster.NewRecorder(scheme.Scheme, v1.EventSource{Component: apis.GroupName})
	return add(mgr, newReconciler(mgr, configInfo, volumeManager, recorder))
}

// newReconciler returns a new reconcile.Reconciler.
func newReconciler(mgr manager.Manager, configInfo *commonconfig.ConfigurationInfo,
	volumeManager volumes.Manager, recorder record.EventRecorder) reconcile.Reconciler {
	return &ReconcileCnsUnregisterVolume{client: mgr.GetClient(), scheme: mgr.GetScheme(),
		configInfo: configInfo, volumeManager: volumeManager, recorder: recorder}
}

// add adds a new Controller to mgr with r as the reconcile.Reconciler.
func add(mgr manager.Manager, r reconcile.Reconciler) error {
	ctx, log := logger.GetNewContextWithLogger()

	maxWorkerThreads := getMaxWorkerThreadsToReconcileCnsUnregisterVolume(ctx)
	// Create a new controller.
	c, err := controller.New("cnsunregistervolume-controller", mgr,
		controller.Options{Reconciler: r, MaxConcurrentReconciles: maxWorkerThreads})
	if err != nil {
		log.Errorf("Failed to create new CnsUnregisterVolume controller with error: %+v", err)
		return err
	}

	backOffDuration = make(map[string]time.Duration)

	// Watch for changes to primary resource CnsUnregisterVolume.
	err = c.Watch(source.Kind(
		mgr.GetCache(),
		&cnsunregistervolumev1alpha1.CnsUnregisterVolume{},
		&handler.TypedEnqueueRequestForObject[*cnsunregistervolumev1alpha1.CnsUnregisterVolume]{},
	))
	if err != nil {
		log.Errorf("Failed to watch for changes to CnsUnregisterVolume resource with error: %+v", err)
		return err
	}
	return nil
}

// blank assignment to verify that ReconcileCnsUnregisterVolume implements
// reconcile.Reconciler.
var _ reconcile.Reconciler = &ReconcileCnsUnregisterVolume{}

// ReconcileCnsUnregisterVolume reconciles a CnsUnregisterVolume object.
type ReconcileCnsUnregisterVolume struct {
	// This client, initialized using mgr.Client() above, is a split client
	// that reads objects from the cache and writes to the apiserver.
	client        client.Client
	scheme        *runtime.Scheme
	configInfo    *commonconfig.ConfigurationInfo
	volumeManager volumes.Manager
	recorder      record.EventRecorder
}

// Reconcile reads that state of the cluster for a ReconcileCnsUnregisterVolume object
// and makes changes based on the state read and what is in the
// ReconcileCnsUnregisterVolume.Spec.
// Note:
// The Controller will requeue the Request to be processed again if the
// returned error is non-nil or Result.Requeue is true. Otherwise, upon
// completion it will remove the work from the queue.
func (r *ReconcileCnsUnregisterVolume) Reconcile(ctx context.Context,
	request reconcile.Request) (reconcile.Result, error) {
	log := logger.GetLogger(ctx)

	// Fetch the CnsUnregisterVolume instance.
	instance := &cnsunregistervolumev1alpha1.CnsUnregisterVolume{}
	err := r.client.Get(ctx, request.NamespacedName, instance)
	if err != nil {
		if apierrors.IsNotFound(err) {
			log.Infof("CnsUnregisterVolume resource not found. Ignoring since object must be deleted.")
			return reconcile.Result{}, nil
		}
		log.Errorf("Error reading the CnsUnregisterVolume with name: %q on namespace: %q. Err: %+v",
			request.Name, request.Namespace, err)
		// Error reading the object - return with err.
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
	// If the CnsUnregistereVolume instance is already unregistered, remove the
	// instance from the queue.
	if instance.Status.Unregistered {
		backOffDurationMapMutex.Lock()
		delete(backOffDuration, instance.Name)
		backOffDurationMapMutex.Unlock()
		return reconcile.Result{}, nil
	}
	log.Infof("Reconciling CnsUnregisterVolume instance %q from namespace %q. timeout %q seconds",
		instance.Name, request.Namespace, timeout)

	// 1. Perform all the necessary validations.
	// 2. Fetch the PV corresponding to the volume and set on it the ReclaimPolicy to Retain.
	// 3. Delete PVC, wait for it to get deleted.
	// 4. Delete PV.
	// 5. Invoke CNS DeleteVolume API with deleteDisk set to false.
	// 6. Set the CnsUnregisterVolumeStatus.Unregistered to true.

	// TODO - Add validations whether the volume is not in use in a TKC or by a VM service VM.

	queryFilter := cnstypes.CnsQueryFilter{
		VolumeIds: []cnstypes.CnsVolumeId{{Id: instance.Spec.VolumeID}},
	}
	querySelection := cnstypes.CnsQuerySelection{
		Names: []string{
			metadata,
		},
	}

	queryResult, err := r.volumeManager.QueryVolumeAsync(ctx, queryFilter, &querySelection)
	if err != nil {
		msg := fmt.Sprintf("Unable to query volume %q . Error: %+v", instance.Spec.VolumeID, err)
		log.Error(msg)
		setInstanceError(ctx, r, instance, msg)
		return reconcile.Result{RequeueAfter: timeout}, nil
	}
	if len(queryResult.Volumes) == 0 {
		msg := fmt.Sprintf("Volume: %q not found while querying CNS. It may have already been unregistered.",
			instance.Spec.VolumeID)
		err = setInstanceSuccess(ctx, r, instance, msg)
		if err != nil {
			msg := fmt.Sprintf("Failed to update CnsUnregistered instance with error: %+v", err)
			log.Error(msg)
			setInstanceError(ctx, r, instance, msg)
			return reconcile.Result{RequeueAfter: timeout}, nil
		}
		backOffDurationMapMutex.Lock()
		delete(backOffDuration, instance.Name)
		backOffDurationMapMutex.Unlock()
		log.Info(msg)
		return reconcile.Result{}, nil
	}

	cnsVol := queryResult.Volumes[0]

	var pvName, pvcName, pvcNamespace string
	for _, entity := range cnsVol.Metadata.EntityMetadata {
		if k8sEntityMetadata, ok := entity.(*cnstypes.CnsKubernetesEntityMetadata); ok {
			entityType := k8sEntityMetadata.EntityType

			if entityType == string(cnstypes.CnsKubernetesEntityTypePV) {
				pvName = entity.(*cnstypes.CnsKubernetesEntityMetadata).EntityName
			}

			if entityType == string(cnstypes.CnsKubernetesEntityTypePVC) {
				pvcName = entity.(*cnstypes.CnsKubernetesEntityMetadata).EntityName
				pvcNamespace = entity.(*cnstypes.CnsKubernetesEntityMetadata).Namespace
			}
		}
	}

	k8sclient, err := k8s.NewClient(ctx)
	if err != nil {
		log.Errorf("Failed to initialize K8S client when reconciling CnsUnregisterVolume "+
			"instance: %s on namespace: %s. Error: %+v", instance.Name, instance.Namespace, err)
		setInstanceError(ctx, r, instance, "Failed to init K8S client for volume unregistration")
		return reconcile.Result{RequeueAfter: timeout}, nil
	}

	err = validateVolumeNotInUse(ctx, cnsVol, pvcName, pvcNamespace, k8sclient)
	if err != nil {
		log.Error(err)
		setInstanceError(ctx, r, instance, err.Error())
		return reconcile.Result{RequeueAfter: timeout}, nil
	}

	if pvName != "" {
		//Change PV ReclaimPolicy to retain so that underlying FCD doesn't get deleted when deleting PV,PVC
		pv, err := k8sclient.CoreV1().PersistentVolumes().Get(ctx, pvName, metav1.GetOptions{})
		if err != nil {
			if !apierrors.IsNotFound(err) {
				log.Errorf("Unable to get PV %q", pvName)
				return reconcile.Result{}, err
			}
		}

		if pv.Spec.PersistentVolumeReclaimPolicy != v1.PersistentVolumeReclaimRetain {
			pv.Spec.PersistentVolumeReclaimPolicy = v1.PersistentVolumeReclaimRetain
			retryErr := retry.RetryOnConflict(retry.DefaultRetry, func() error {
				_, updateErr := k8sclient.CoreV1().PersistentVolumes().Update(context.TODO(), pv, metav1.UpdateOptions{})
				return updateErr
			})
			if retryErr != nil {
				log.Errorf("Unable to update ReclaimPolicy on PV %q", pvName)
				return reconcile.Result{}, err
			}
			log.Infof("Updated ReclaimPolicy on PV %q to %q", pvName, v1.PersistentVolumeReclaimRetain)
		}
	} else {
		log.Infof("CNS metadata for volume %s has missing pvName."+
			"PV may have already been deleted. Continuing with other operations..", instance.Spec.VolumeID)
	}

	// Delete PVC.
	if pvcName != "" && pvcNamespace != "" {
		err = k8sclient.CoreV1().PersistentVolumeClaims(pvcNamespace).Delete(ctx,
			pvcName, *metav1.NewDeleteOptions(0))
		if err != nil {
			if apierrors.IsNotFound(err) {
				log.Infof("PVC %q not found in namespace %q. It may have already been deleted."+
					"Continuing with other operations..", pvcName, pvcNamespace)
			} else {
				log.Errorf("Failed to delete PVC %q in namespace %q with error - %s",
					pvcName, pvcNamespace, err.Error())
				return reconcile.Result{}, err
			}
		} else {
			log.Infof("Deleted PVC %q in namespace %q", pvcName, pvcNamespace)
		}
	} else {
		log.Infof("CNS metadata for volume %s has missing pvcName or namespace."+
			"PVC may have already been deleted. Continuing with other operations..", instance.Spec.VolumeID)
	}

	if pvName != "" {
		// Delete PV.
		// Since reclaimPolicy was set to Retain, we need to explicitly delete it.
		err = k8sclient.CoreV1().PersistentVolumes().Delete(ctx, pvName, *metav1.NewDeleteOptions(0))
		if err != nil {
			if apierrors.IsNotFound(err) {
				log.Infof("PV %q not found. It may have already been deleted."+
					"Continuing with other operations..", pvName)
			} else {
				log.Errorf("Failed to delete PV %q with error %s", pvName, err.Error())
				return reconcile.Result{}, err
			}
		} else {
			log.Infof("Deleted PV %q", pvName)
		}
	}

	// Invoke CNS DeleteVolume API with deleteDisk flag set to false.
	_, err = r.volumeManager.DeleteVolume(ctx, instance.Spec.VolumeID, false)
	if err != nil {
		if cnsvsphere.IsNotFoundError(err) {
			log.Infof("VolumeID %q not found in CNS. It may have already been deleted."+
				"Marking the operation as success.", instance.Spec.VolumeID)
		} else {
			log.Errorf("Failed to delete volume %q in CNS with error %+v.",
				instance.Spec.VolumeID, err)
			return reconcile.Result{}, err
		}
	} else {
		log.Infof("Deleted CNS volume %q with deleteDisk set to false", instance.Spec.VolumeID)
	}

	// Update the instance to indicate the volume unregistration is successful.
	msg := fmt.Sprintf("Successfully unregistered the volume on namespace: %s", instance.Namespace)
	err = setInstanceSuccess(ctx, r, instance, msg)
	if err != nil {
		msg := fmt.Sprintf("Failed to update CnsUnregistered instance with error: %+v", err)
		log.Error(msg)
		setInstanceError(ctx, r, instance, msg)
		return reconcile.Result{RequeueAfter: timeout}, nil
	}
	backOffDurationMapMutex.Lock()
	delete(backOffDuration, instance.Name)
	backOffDurationMapMutex.Unlock()
	log.Info(msg)
	return reconcile.Result{}, nil
}

// validateVolumeNotInUse validates whether the volume to be unregistered is not in use by
// either PodVM, TKG cluster or Volume service VM.
func validateVolumeNotInUse(ctx context.Context, cnsVol cnstypes.CnsVolume, pvcName string,
	pvcNamespace string, k8sClient clientset.Interface) error {

	log := logger.GetLogger(ctx)

	// Check if the Supervisor volume is not in use by any pods (PodVMs) in the namespace.
	pods, err := k8sClient.CoreV1().Pods(pvcNamespace).List(ctx, metav1.ListOptions{})
	if err != nil {
		log.Errorf("Failed to list pods in namespace %s with error - %s",
			pvcNamespace, err.Error())
		return err
	}

	for _, pod := range pods.Items {
		for _, podVol := range pod.Spec.Volumes {
			if podVol.PersistentVolumeClaim != nil &&
				podVol.PersistentVolumeClaim.ClaimName == pvcName {
				log.Debugf("Volume %s is in use by pod %s in namespace %s", cnsVol.VolumeId.Id,
					pod.Name, pvcNamespace)
				return fmt.Errorf("cannot unregister the volume %s as it's in use by pod %s in namespace %s",
					cnsVol.VolumeId.Id, pod.Name, pvcNamespace)
			}
		}
	}

	// Check if the Supervisor volume is not used in any TKGs cluster.
	// For volumes created from TKGs Cluster, CNS metadata will have two entries for containerClusterArray.
	// One for clusterFlavor: "WORKLOAD" & clusterDistribution "SupervisorCluster",
	// another for clusterFlavor: "GUEST_CLUSTER" & clusterDistribution: "TKGService".
	for _, containerCluster := range cnsVol.Metadata.ContainerClusterArray {
		if containerCluster.ClusterFlavor == "GUEST_CLUSTER" {
			log.Debugf("Volume %s is in use by guest cluster with CNS clusterId %s", cnsVol.VolumeId.Id,
				containerCluster.ClusterId)
			return fmt.Errorf("cannot unregister the volume %s as it's in use by guest cluster with CNS clusterId %s",
				cnsVol.VolumeId.Id, containerCluster.ClusterId)
		}
	}

	// Check if the Supervisor volume is not used by a volume service VM.
	// If the volume is specified in the VirtualMachine's spec, then it intends
	// to be attached to the VM. We will check for the presence of volume in VM's spec.
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

	vmList, err := utils.GetVirtualMachineList(ctx, pvcNamespace, vmOperatorClient)
	if err != nil {
		msg := fmt.Sprintf("failed to list virtualmachines with error: %+v", err)
		log.Error(msg)
		return err
	}

	for _, vmInstance := range vmList.Items {
		for _, vmVol := range vmInstance.Spec.Volumes {
			if vmVol.PersistentVolumeClaim != nil &&
				vmVol.PersistentVolumeClaim.ClaimName == pvcName {
				log.Debugf("Volume %s is in use by VirtualMachine %s in namespace %s", cnsVol.VolumeId.Id,
					vmInstance.Name, pvcNamespace)
				return fmt.Errorf("cannot unregister the volume %s as it's in use by VirtualMachine %s in namespace %s",
					cnsVol.VolumeId.Id, vmInstance.Name, pvcNamespace)
			}
		}
	}
	return nil
}

// setInstanceError sets error and records an event on the CnsUnregisterVolume
// instance.
func setInstanceError(ctx context.Context, r *ReconcileCnsUnregisterVolume,
	instance *cnsunregistervolumev1alpha1.CnsUnregisterVolume, errMsg string) {
	log := logger.GetLogger(ctx)
	instance.Status.Error = errMsg
	err := updateCnsUnregisterVolume(ctx, r.client, instance)
	if err != nil {
		log.Errorf("updateCnsUnregisterVolume failed. err: %v", err)
	}
	recordEvent(ctx, r, instance, v1.EventTypeWarning, errMsg)
}

// setInstanceSuccess sets instance to success and records an event on the
// CnsUnregisterVolume instance.
func setInstanceSuccess(ctx context.Context, r *ReconcileCnsUnregisterVolume,
	instance *cnsunregistervolumev1alpha1.CnsUnregisterVolume, msg string) error {
	instance.Status.Unregistered = true
	instance.Status.Error = ""
	err := updateCnsUnregisterVolume(ctx, r.client, instance)
	if err != nil {
		return err
	}
	recordEvent(ctx, r, instance, v1.EventTypeNormal, msg)
	return nil
}

// recordEvent records the event, sets the backOffDuration for the instance
// appropriately and logs the message.
// backOffDuration is reset to 1 second on success and doubled on failure.
func recordEvent(ctx context.Context, r *ReconcileCnsUnregisterVolume,
	instance *cnsunregistervolumev1alpha1.CnsUnregisterVolume, eventtype string, msg string) {
	log := logger.GetLogger(ctx)
	log.Debugf("Event type is %s", eventtype)
	switch eventtype {
	case v1.EventTypeWarning:
		// Double backOff duration.
		backOffDurationMapMutex.Lock()
		backOffDuration[instance.Name] = backOffDuration[instance.Name] * 2
		r.recorder.Event(instance, v1.EventTypeWarning, "CnsUnregisterVolumeFailed", msg)
		backOffDurationMapMutex.Unlock()
	case v1.EventTypeNormal:
		// Reset backOff duration to one second.
		backOffDurationMapMutex.Lock()
		backOffDuration[instance.Name] = time.Second
		r.recorder.Event(instance, v1.EventTypeNormal, "CnsUnregisterVolumeSucceeded", msg)
		backOffDurationMapMutex.Unlock()
	}
}

// updateCnsUnregisterVolume updates the CnsUnregisterVolume instance in K8S.
func updateCnsUnregisterVolume(ctx context.Context, client client.Client,
	instance *cnsunregistervolumev1alpha1.CnsUnregisterVolume) error {
	log := logger.GetLogger(ctx)
	err := client.Update(ctx, instance)
	if err != nil {
		log.Errorf("Failed to update CnsUnregisterVolume instance: %q on namespace: %q. Error: %+v",
			instance.Name, instance.Namespace, err)
	}
	return err
}
