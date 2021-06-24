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

package csinodetopology

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	cnstypes "github.com/vmware/govmomi/cns/types"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/kubernetes/scheme"
	typedcorev1 "k8s.io/client-go/kubernetes/typed/core/v1"
	"k8s.io/client-go/tools/record"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller"
	"sigs.k8s.io/controller-runtime/pkg/event"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	"sigs.k8s.io/controller-runtime/pkg/manager"
	"sigs.k8s.io/controller-runtime/pkg/predicate"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
	"sigs.k8s.io/controller-runtime/pkg/source"

	"sigs.k8s.io/vsphere-csi-driver/pkg/common/cns-lib/node"
	volumes "sigs.k8s.io/vsphere-csi-driver/pkg/common/cns-lib/volume"
	cnsvsphere "sigs.k8s.io/vsphere-csi-driver/pkg/common/cns-lib/vsphere"
	cnsconfig "sigs.k8s.io/vsphere-csi-driver/pkg/common/config"
	"sigs.k8s.io/vsphere-csi-driver/pkg/csi/service/common"
	"sigs.k8s.io/vsphere-csi-driver/pkg/csi/service/common/commonco"
	"sigs.k8s.io/vsphere-csi-driver/pkg/csi/service/logger"
	csinodetopologyv1alpha1 "sigs.k8s.io/vsphere-csi-driver/pkg/internalapis/csinodetopology/v1alpha1"
	k8s "sigs.k8s.io/vsphere-csi-driver/pkg/kubernetes"
	"sigs.k8s.io/vsphere-csi-driver/pkg/syncer"
)

const defaultMaxWorkerThreadsForCSINodeTopology = 1

// backOffDuration is a map of csinodetopology instance name to the time after which a request
// for this instance will be requeued.
// Initialized to 1 second for new instances and for instances whose latest reconcile
// operation succeeded.
// If the reconcile fails, backoff is incremented exponentially.
var (
	backOffDuration         map[string]time.Duration
	backOffDurationMapMutex = sync.Mutex{}
)

// Add creates a new CSINodeTopology Controller and adds it to the Manager, ConfigurationInfo
// and VirtualCenterTypes. The Manager will set fields on the Controller
// and start it when the Manager is started.
func Add(mgr manager.Manager, clusterFlavor cnstypes.CnsClusterFlavor,
	configInfo *cnsconfig.ConfigurationInfo, volumeManager volumes.Manager) error {
	ctx, log := logger.GetNewContextWithLogger()
	if clusterFlavor != cnstypes.CnsClusterFlavorVanilla {
		log.Debug("Not initializing the CSINodetopology Controller as it is not a Vanilla CSI deployment")
		return nil
	}

	coCommonInterface, err := commonco.GetContainerOrchestratorInterface(ctx, common.Kubernetes, clusterFlavor, &syncer.COInitParams)
	if err != nil {
		log.Errorf("failed to create CO agnostic interface. Err: %v", err)
		return err
	}
	if !coCommonInterface.IsFSSEnabled(ctx, common.ImprovedVolumeTopology) {
		log.Infof("Not initializing the CSINodetopology Controller as %s FSS is disabled", common.ImprovedVolumeTopology)
		return nil
	}

	// Initialize kubernetes client.
	k8sclient, err := k8s.NewClient(ctx)
	if err != nil {
		log.Errorf("creating Kubernetes client failed. Err: %v", err)
		return err
	}
	// eventBroadcaster broadcasts events on csinodetopology instances to the event sink.
	eventBroadcaster := record.NewBroadcaster()
	eventBroadcaster.StartRecordingToSink(
		&typedcorev1.EventSinkImpl{
			Interface: k8sclient.CoreV1().Events(""),
		},
	)
	recorder := eventBroadcaster.NewRecorder(scheme.Scheme, corev1.EventSource{Component: csinodetopologyv1alpha1.GroupName})
	return add(mgr, newReconciler(mgr, configInfo, recorder))
}

// newReconciler returns a new `reconcile.Reconciler`.
func newReconciler(mgr manager.Manager, configInfo *cnsconfig.ConfigurationInfo,
	recorder record.EventRecorder) reconcile.Reconciler {
	return &ReconcileCSINodeTopology{client: mgr.GetClient(), scheme: mgr.GetScheme(),
		configInfo: configInfo, recorder: recorder}
}

// add adds a new Controller to mgr with r as the `reconcile.Reconciler`.
func add(mgr manager.Manager, r reconcile.Reconciler) error {
	log := logger.GetLoggerWithNoContext()

	// Create a new controller.
	c, err := controller.New("csinodetopology-controller", mgr, controller.Options{Reconciler: r,
		MaxConcurrentReconciles: defaultMaxWorkerThreadsForCSINodeTopology})
	if err != nil {
		log.Errorf("failed to create new CSINodetopology controller with error: %+v", err)
		return err
	}

	// Initialize backoff duration map.
	backOffDuration = make(map[string]time.Duration)

	// Predicates are used to determine under which conditions
	// the reconcile callback will be made for an instance.
	pred := predicate.Funcs{
		CreateFunc: func(e event.CreateEvent) bool {
			return true
		},
		UpdateFunc: func(e event.UpdateEvent) bool {
			// The CO calls NodeGetInfo API just once during the node registration,
			// therefore we do not support updates to the spec after the CR has been reconciled.
			log.Debug("Ignoring CSINodeTopology reconciliation on update event")
			return false
		},
		DeleteFunc: func(e event.DeleteEvent) bool {
			// Instances are deleted by the garbage collector automatically after the corresponding NodeVM is deleted.
			// No reconcile operations are required.
			log.Debug("Ignoring CSINodeTopology reconciliation on delete event")
			return false
		},
	}

	// Watch for changes to primary resource CSINodeTopology.
	err = c.Watch(&source.Kind{Type: &csinodetopologyv1alpha1.CSINodeTopology{}}, &handler.EnqueueRequestForObject{}, pred)
	if err != nil {
		log.Errorf("Failed to watch for changes to CSINodeTopology resource with error: %+v", err)
		return err
	}
	log.Info("Started watching on CSINodeTopology resources")
	return nil
}

// blank assignment to verify that ReconcileCSINodeTopology implements `reconcile.Reconciler`.
var _ reconcile.Reconciler = &ReconcileCSINodeTopology{}

// ReconcileCSINodeTopology reconciles a CSINodeTopology object.
type ReconcileCSINodeTopology struct {
	// This client, initialized using mgr.Client() above, is a split client
	// that reads objects from the cache and writes to the apiserver.
	client     client.Client
	scheme     *runtime.Scheme
	configInfo *cnsconfig.ConfigurationInfo
	recorder   record.EventRecorder
}

// Reconcile is part of the main kubernetes reconciliation loop which aims to
// move the current state of the cluster closer to the desired state.
// Note: The Controller will requeue the Request to be processed again if the returned error is non-nil or
// Result.Requeue is true, otherwise upon completion it will remove the work from the queue.
func (r *ReconcileCSINodeTopology) Reconcile(ctx context.Context, request reconcile.Request) (reconcile.Result, error) {
	log := logger.GetLogger(ctx)

	// Fetch the CSINodeTopology instance.
	instance := &csinodetopologyv1alpha1.CSINodeTopology{}
	err := r.client.Get(ctx, request.NamespacedName, instance)
	if err != nil {
		if apierrors.IsNotFound(err) {
			log.Infof("CSINodeTopology resource with name %q not found. Ignoring since object must have been deleted.", request.Name)
			return reconcile.Result{}, nil
		}
		log.Errorf("Failed to fetch the CSINodeTopology instance with name: %q. Error: %+v", request.Name, err)
		// Error reading the object - return with err.
		return reconcile.Result{}, err
	}

	// Get NodeVM instance.
	nodeID := instance.Spec.NodeID
	nodeManager := node.GetManager(ctx)
	// NOTE: NodeID is set to NodeName for now. Will be changed to NodeUUID in future.
	nodeVM, err := nodeManager.GetNodeByName(ctx, nodeID)
	if err != nil {
		// If nodeVM not found, ignore reconcile call.
		log.Warnf("Ignoring update to CSINodeTopology CR with name %s as corresponding NodeVM seems to be deleted. "+
			"Error: %v", nodeID, err)
		return reconcile.Result{}, nil
	}

	// Initialize backOffDuration for the instance, if required.
	backOffDurationMapMutex.Lock()
	var timeout time.Duration
	if _, exists := backOffDuration[instance.Name]; !exists {
		backOffDuration[instance.Name] = time.Second
	}
	timeout = backOffDuration[instance.Name]
	backOffDurationMapMutex.Unlock()

	// TODO: Make this check generic by checking for the length of topologyCategories in future
	// Retrieve topology labels for nodeVM.
	if r.configInfo.Cfg.Labels.Zone == "" && r.configInfo.Cfg.Labels.Region == "" {
		// Not a topology aware setup. No need to check for labels on nodeVM.
		// Set the Status to Success and return.
		instance.Status.TopologyLabels = make([]csinodetopologyv1alpha1.TopologyLabel, 0)
		err = updateCRStatus(ctx, r, instance, csinodetopologyv1alpha1.CSINodeTopologySuccess,
			"Not a topology aware cluster.")
		if err != nil {
			return reconcile.Result{RequeueAfter: timeout}, nil
		}
	} else if r.configInfo.Cfg.Labels.Zone != "" && r.configInfo.Cfg.Labels.Region != "" {
		log.Infof("Detected a topology aware cluster")

		// Fetch topology labels for nodeVM.
		topologyLabels, err := getNodeTopologyInfo(ctx, nodeVM, r.configInfo.Cfg)
		if err != nil {
			msg := fmt.Sprintf("failed to fetch topology information for the nodeVM with ID %q", nodeID)
			log.Errorf("%s. Error: %v", msg, err)
			_ = updateCRStatus(ctx, r, instance, csinodetopologyv1alpha1.CSINodeTopologyError, msg)
			return reconcile.Result{RequeueAfter: timeout}, nil
		}

		// Raise error if nodeVM does not have a topology label associated with each category in the
		// vSphere config secret `Labels` section. For now, as we support only zone, region, hardcoded the value to 2.
		// TODO: Count the number of topology categories given and verify against that number.
		if len(topologyLabels) != 2 {
			msg := fmt.Sprintf("Detected a topology aware cluster. However, nodeVM with ID %q does not have a "+
				"topology label for each category mentioned under the vSphere CSI config secret `Labels` section.", nodeID)
			log.Error(msg)
			_ = updateCRStatus(ctx, r, instance, csinodetopologyv1alpha1.CSINodeTopologyError, msg)
			return reconcile.Result{RequeueAfter: timeout}, nil
		}

		// Update CSINodeTopology instance.
		instance.Status.TopologyLabels = topologyLabels
		err = updateCRStatus(ctx, r, instance, csinodetopologyv1alpha1.CSINodeTopologySuccess,
			fmt.Sprintf("Topology labels successfully updated for nodeVM %q", nodeID))
		if err != nil {
			return reconcile.Result{RequeueAfter: timeout}, nil
		}
	} else {
		msg := "missing zone or region information in vSphere CSI config `Labels` section"
		log.Error(msg)
		_ = updateCRStatus(ctx, r, instance, csinodetopologyv1alpha1.CSINodeTopologyError, msg)
		return reconcile.Result{}, errors.New(msg)
	}

	// On successful event, remove instance from backOffDuration.
	backOffDurationMapMutex.Lock()
	delete(backOffDuration, instance.Name)
	backOffDurationMapMutex.Unlock()
	log.Infof("Successfully updated topology labels for nodeVM with ID %q", nodeID)
	return reconcile.Result{}, nil
}

func updateCRStatus(ctx context.Context, r *ReconcileCSINodeTopology, instance *csinodetopologyv1alpha1.CSINodeTopology,
	status csinodetopologyv1alpha1.CRDStatus, eventMessage string) error {
	log := logger.GetLogger(ctx)

	instance.Status.Status = status
	switch status {
	case csinodetopologyv1alpha1.CSINodeTopologySuccess:
		// Reset error message if previously populated.
		instance.Status.ErrorMessage = ""
		// Record an event on the CR.
		r.recorder.Event(instance, corev1.EventTypeNormal, "CSINodeTopologySucceeded", eventMessage)
	case csinodetopologyv1alpha1.CSINodeTopologyError:
		// Increase backoff duration for the instance.
		backOffDurationMapMutex.Lock()
		backOffDuration[instance.Name] = backOffDuration[instance.Name] * 2
		backOffDurationMapMutex.Unlock()

		// Record an event on the CR.
		instance.Status.ErrorMessage = eventMessage
		r.recorder.Event(instance, corev1.EventTypeWarning, "CSINodeTopologyFailed", eventMessage)
	}

	// Update CSINodeTopology instance.
	err := r.client.Update(ctx, instance)
	if err != nil {
		msg := fmt.Sprintf("Failed to update the CSINodeTopology instance with name %q.", instance.Spec.NodeID)
		log.Errorf("%s. Error: %+v", msg, err)
		r.recorder.Event(instance, corev1.EventTypeWarning, "CSINodeTopologyFailed", msg)
		return err
	}
	return nil
}

func getNodeTopologyInfo(ctx context.Context, nodeVM *cnsvsphere.VirtualMachine, cfg *cnsconfig.Config) (
	[]csinodetopologyv1alpha1.TopologyLabel, error) {
	log := logger.GetLogger(ctx)

	// Get VC instance.
	vcenter, err := cnsvsphere.GetVirtualCenterInstance(ctx, &cnsconfig.ConfigurationInfo{Cfg: cfg}, false)
	if err != nil {
		log.Errorf("failed to get virtual center instance with error: %v", err)
		return nil, err
	}

	// Get tag manager instance.
	tagManager, err := cnsvsphere.GetTagManager(ctx, vcenter)
	if err != nil {
		log.Errorf("failed to create tagManager. Error: %v", err)
		return nil, err
	}
	defer func() {
		err := tagManager.Logout(ctx)
		if err != nil {
			log.Errorf("failed to logout tagManager. Error: %v", err)
		}
	}()

	// Get zone, region info for NodeVM.
	zone, region, err := nodeVM.GetZoneRegion(ctx, cfg.Labels.Zone, cfg.Labels.Region, tagManager)
	if err != nil {
		log.Errorf("failed to get accessibleTopology for nodeVM: %v, Error: %v", nodeVM.Reference(), err)
		return nil, err
	}
	log.Infof("NodeVM %q belongs to zone: %q and region: %q", nodeVM.Reference(), zone, region)
	topologyLabels := []csinodetopologyv1alpha1.TopologyLabel{
		{Key: corev1.LabelTopologyZone, Value: zone},
		{Key: corev1.LabelTopologyRegion, Value: region},
	}
	return topologyLabels, nil
}
