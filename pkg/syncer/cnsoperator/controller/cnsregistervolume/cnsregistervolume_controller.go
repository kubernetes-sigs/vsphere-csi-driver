/*
Copyright 2020 The Kubernetes Authors.

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
	"errors"
	"fmt"
	"reflect"
	"sync"
	"time"

	cnstypes "github.com/vmware/govmomi/cns/types"
	v1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
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

	clientConfig "sigs.k8s.io/controller-runtime/pkg/client/config"

	apis "sigs.k8s.io/vsphere-csi-driver/v3/pkg/apis/cnsoperator"
	cnsregistervolumev1alpha1 "sigs.k8s.io/vsphere-csi-driver/v3/pkg/apis/cnsoperator/cnsregistervolume/v1alpha1"
	storagepolicyusagev1alpha2 "sigs.k8s.io/vsphere-csi-driver/v3/pkg/apis/cnsoperator/storagepolicy/v1alpha2"
	volumes "sigs.k8s.io/vsphere-csi-driver/v3/pkg/common/cns-lib/volume"
	cnsvsphere "sigs.k8s.io/vsphere-csi-driver/v3/pkg/common/cns-lib/vsphere"
	commonconfig "sigs.k8s.io/vsphere-csi-driver/v3/pkg/common/config"
	"sigs.k8s.io/vsphere-csi-driver/v3/pkg/csi/service/common"
	"sigs.k8s.io/vsphere-csi-driver/v3/pkg/csi/service/common/commonco"
	commoncotypes "sigs.k8s.io/vsphere-csi-driver/v3/pkg/csi/service/common/commonco/types"
	"sigs.k8s.io/vsphere-csi-driver/v3/pkg/csi/service/logger"
	"sigs.k8s.io/vsphere-csi-driver/v3/pkg/internalapis/cnsvolumeinfo"
	k8s "sigs.k8s.io/vsphere-csi-driver/v3/pkg/kubernetes"
	"sigs.k8s.io/vsphere-csi-driver/v3/pkg/syncer"
)

const (
	defaultMaxWorkerThreadsForRegisterVolume = 40
	staticPvNamePrefix                       = "static-pv-"
)

var (
	// backOffDuration is a map of cnsregistervolume name's to the time after which
	// a request for this instance will be requeued.
	// Initialized to 1 second for new instances and for instances whose latest
	// reconcile operation succeeded.
	// If the reconcile fails, backoff is incremented exponentially.
	backOffDuration         map[string]time.Duration
	backOffDurationMapMutex = sync.Mutex{}

	topologyMgr                 commoncotypes.ControllerTopologyService
	clusterComputeResourceMoIds []string
	// workloadDomainIsolationEnabled determines if the workload domain
	// isolation feature is available on a supervisor cluster.
	workloadDomainIsolationEnabled bool
)

// Add creates a new CnsRegisterVolume Controller and adds it to the Manager,
// ConfigurationInfo and VirtualCenterTypes. The Manager will set fields on
// the Controller and Start it when the Manager is Started.
func Add(mgr manager.Manager, clusterFlavor cnstypes.CnsClusterFlavor,
	configInfo *commonconfig.ConfigurationInfo, volumeManager volumes.Manager) error {
	ctx, log := logger.GetNewContextWithLogger()
	if clusterFlavor != cnstypes.CnsClusterFlavorWorkload {
		log.Debug("Not initializing the CnsRegisterVolume Controller as it's not a WCP CSI deployment")
		return nil
	}

	workloadDomainIsolationEnabled = commonco.ContainerOrchestratorUtility.IsFSSEnabled(ctx,
		common.WorkloadDomainIsolation)

	var volumeInfoService cnsvolumeinfo.VolumeInfoService
	if commonco.ContainerOrchestratorUtility.IsFSSEnabled(ctx, common.TKGsHA) {
		var err error
		clusterComputeResourceMoIds, err = common.GetClusterComputeResourceMoIds(ctx)
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
		} else if len(clusterComputeResourceMoIds) > 1 {
			log.Infof("Stretched Supervisor detected but PodVM is not enabled. " +
				"Not initializing the CnsRegisterVolume controller.")
			return nil
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
	return add(mgr, newReconciler(mgr, configInfo, volumeManager, recorder, volumeInfoService))
}

// newReconciler returns a new reconcile.Reconciler.
func newReconciler(mgr manager.Manager, configInfo *commonconfig.ConfigurationInfo,
	volumeManager volumes.Manager, recorder record.EventRecorder,
	volumeInfoService cnsvolumeinfo.VolumeInfoService) reconcile.Reconciler {
	return &ReconcileCnsRegisterVolume{client: mgr.GetClient(), scheme: mgr.GetScheme(),
		configInfo: configInfo, volumeManager: volumeManager, recorder: recorder, volumeInfoService: volumeInfoService}
}

// add adds a new Controller to mgr with r as the reconcile.Reconciler.
func add(mgr manager.Manager, r reconcile.Reconciler) error {
	ctx, log := logger.GetNewContextWithLogger()

	maxWorkerThreads := getMaxWorkerThreadsToReconcileCnsRegisterVolume(ctx)
	// Create a new controller.
	c, err := controller.New("cnsregistervolume-controller", mgr,
		controller.Options{Reconciler: r, MaxConcurrentReconciles: maxWorkerThreads})
	if err != nil {
		log.Errorf("Failed to create new CnsRegisterVolume controller with error: %+v", err)
		return err
	}

	backOffDuration = make(map[string]time.Duration)

	// Watch for changes to primary resource CnsRegisterVolume.
	err = c.Watch(source.Kind(
		mgr.GetCache(),
		&cnsregistervolumev1alpha1.CnsRegisterVolume{},
		&handler.TypedEnqueueRequestForObject[*cnsregistervolumev1alpha1.CnsRegisterVolume]{},
	))
	if err != nil {
		log.Errorf("Failed to watch for changes to CnsRegisterVolume resource with error: %+v", err)
		return err
	}
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
	scheme            *runtime.Scheme
	configInfo        *commonconfig.ConfigurationInfo
	volumeManager     volumes.Manager
	recorder          record.EventRecorder
	volumeInfoService cnsvolumeinfo.VolumeInfoService
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
	log := logger.GetLogger(ctx)
	// Fetch the CnsRegisterVolume instance.
	instance := &cnsregistervolumev1alpha1.CnsRegisterVolume{}
	err := r.client.Get(ctx, request.NamespacedName, instance)
	if err != nil {
		if apierrors.IsNotFound(err) {
			log.Infof("CnsRegisterVolume resource not found. Ignoring since object must be deleted.")
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
	if _, exists := backOffDuration[instance.Name]; !exists {
		backOffDuration[instance.Name] = time.Second
	}
	timeout = backOffDuration[instance.Name]
	backOffDurationMapMutex.Unlock()

	// If the CnsRegisterVolume instance is already registered, remove the
	// instance from the queue.
	if instance.Status.Registered {
		backOffDurationMapMutex.Lock()
		delete(backOffDuration, instance.Name)
		backOffDurationMapMutex.Unlock()
		return reconcile.Result{}, nil
	}

	log.Infof("Reconciling CnsRegisterVolume with instance: %q from namespace: %q. timeout %q seconds",
		instance.Name, request.Namespace, timeout)
	// Validate CnsRegisterVolume spec to check for valid entries.
	err = validateCnsRegisterVolumeSpec(ctx, instance)
	if err != nil {
		log.Errorf(err.Error())
		setInstanceError(ctx, r, instance, err.Error())
		backOffDurationMapMutex.Lock()
		delete(backOffDuration, instance.Name)
		backOffDurationMapMutex.Unlock()
		return reconcile.Result{}, nil
	}

	// Verify if CnsRegisterVolume request is for block volume registration
	// Currently file volume registration is not supported.
	if !isBlockVolumeRegisterRequest(ctx, instance) {
		msg := fmt.Sprintf("AccessMode: %s is not supported", instance.Spec.AccessMode)
		log.Error(msg)
		setInstanceError(ctx, r, instance, msg)
		backOffDurationMapMutex.Lock()
		delete(backOffDuration, instance.Name)
		backOffDurationMapMutex.Unlock()
		return reconcile.Result{}, nil
	}

	vc, err := cnsvsphere.GetVirtualCenterInstance(ctx, r.configInfo, false)
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
	isTKGSHAEnabled := commonco.ContainerOrchestratorUtility.IsFSSEnabled(ctx, common.TKGsHA)
	createSpec := constructCreateSpecForInstance(r, instance, vc.Config.Host, isTKGSHAEnabled)
	log.Infof("Creating CNS volume: %+v for CnsRegisterVolume request with name: %q in namespace: %q",
		instance, instance.Name, instance.Namespace)
	log.Debugf("CNS Volume create spec is: %+v", createSpec)
	volInfo, _, err := r.volumeManager.CreateVolume(ctx, createSpec, nil)
	if err != nil {
		msg := "failed to create CNS volume"
		log.Errorf(msg)
		setInstanceError(ctx, r, instance, msg)
		return reconcile.Result{RequeueAfter: timeout}, nil
	}

	if instance.Spec.DiskURLPath != "" {
		// if CNS Register Volume Instance is created with diskURLPath,
		// confirm using CNS Volume ID, if another PV is already present with same volume ID
		// If yes then fail.
		pvName, found := commonco.ContainerOrchestratorUtility.GetPVNameFromCSIVolumeID(volInfo.VolumeID.Id)
		if found && pvName != staticPvNamePrefix+volInfo.VolumeID.Id {
			msg := fmt.Sprintf("PV: %q with the volume ID: %q for volume path: %q"+
				"is already present. Can not create multiple PV with same disk.", pvName, volInfo.VolumeID.Id,
				instance.Spec.DiskURLPath)
			log.Errorf(msg)
			setInstanceError(ctx, r, instance, msg)
			delete(backOffDuration, instance.Name)
			backOffDurationMapMutex.Unlock()
			return reconcile.Result{}, nil
		}
	}

	volumeID = volInfo.VolumeID.Id
	log.Infof("Created CNS volume with volumeID: %s", volumeID)

	pvName = staticPvNamePrefix + volumeID
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

	if syncer.IsPodVMOnStretchSupervisorFSSEnabled &&
		(workloadDomainIsolationEnabled || len(clusterComputeResourceMoIds) > 1) {
		azClustersMap := topologyMgr.GetAZClustersMap(ctx)
		isAccessible := isDatastoreAccessibleToAZClusters(ctx, vc, azClustersMap, volume.DatastoreUrl)
		if !isAccessible {
			log.Errorf("Volume: %s present on datastore: %s is not accessible to any of the AZ clusters: %v",
				volumeID, volume.DatastoreUrl, azClustersMap)
			setInstanceError(ctx, r, instance, "Volume in the spec is not accessible to any of the AZ clusters")
			_, err = common.DeleteVolumeUtil(ctx, r.volumeManager, volumeID, false)
			if err != nil {
				log.Errorf("Failed to untag CNS volume: %s with error: %+v", volumeID, err)
			}
			return reconcile.Result{RequeueAfter: timeout}, nil
		}
	} else {
		// Verify if the volume is accessible to Supervisor cluster.
		isAccessible := isDatastoreAccessibleToCluster(ctx, vc, r.configInfo.Cfg.Global.ClusterID, volume.DatastoreUrl)
		if !isAccessible {
			log.Errorf("Volume: %s present on datastore: %s is not accessible to all nodes in the cluster: %s",
				volumeID, volume.DatastoreUrl, r.configInfo.Cfg.Global.ClusterID)
			setInstanceError(ctx, r, instance, "Volume in the spec is not accessible to all nodes in the cluster")
			// Untag the CNS volume which was created previously.
			_, err = common.DeleteVolumeUtil(ctx, r.volumeManager, volumeID, false)
			if err != nil {
				log.Errorf("Failed to untag CNS volume: %s with error: %+v", volumeID, err)
			}
			return reconcile.Result{RequeueAfter: timeout}, nil
		}
	}

	// Verify if storage policy is empty.
	if volume.StoragePolicyId == "" {
		log.Errorf("Volume: %s doesn't have storage policy associated with it", volumeID)
		setInstanceError(ctx, r, instance, "Volume in the spec doesn't have storage policy associated with it")
		// Untag the CNS volume which was created previously.
		_, err = common.DeleteVolumeUtil(ctx, r.volumeManager, volumeID, false)
		if err != nil {
			log.Errorf("Failed to untag CNS volume: %s with error: %+v", volumeID, err)
		}
		return reconcile.Result{RequeueAfter: timeout}, nil
	}

	k8sclient, err := k8s.NewClient(ctx)
	if err != nil {
		log.Errorf("Failed to initialize K8S client when registering the CnsRegisterVolume "+
			"instance: %s on namespace: %s. Error: %+v", instance.Name, instance.Namespace, err)
		setInstanceError(ctx, r, instance, "Failed to init K8S client for volume registration")
		return reconcile.Result{RequeueAfter: timeout}, nil
	}

	// Get K8S storageclass name mapping the storagepolicy id with Immediate volume binding mode
	storageClassName, err := getK8sStorageClassNameWithImmediateBindingModeForPolicy(ctx, k8sclient, r.client,
		volume.StoragePolicyId, request.Namespace, syncer.IsPodVMOnStretchSupervisorFSSEnabled)
	if err != nil {
		msg := fmt.Sprintf("Failed to find K8S Storageclass mapping storagepolicyId: %s and assigned to namespace: %s",
			volume.StoragePolicyId, request.Namespace)
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

	// Calculate accessible topology for the provisioned volume.
	var datastoreAccessibleTopology []map[string]string
	if syncer.IsPodVMOnStretchSupervisorFSSEnabled {
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

	capacityInMb := volume.BackingObjectDetails.GetCnsBackingObjectDetails().CapacityInMb
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
			pvSpec := getPersistentVolumeSpec(pvName, volumeID, capacityInMb,
				accessMode, storageClassName, claimRef)
			pvSpec.Spec.NodeAffinity = pvNodeAffinity
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
	}
	// If PV is already bound to a different PVC at this point, then its a
	// duplicate request.
	if pv.Status.Phase == v1.VolumeBound && pv.Spec.ClaimRef.Name != instance.Spec.PvcName {
		log.Errorf("Duplicate Request. There already exists a PV: %s which is bound", pvName)
		setInstanceError(ctx, r, instance, "Duplicate Request")
		return reconcile.Result{RequeueAfter: timeout}, nil
	}
	// Create PVC mapping to above created PV.
	log.Infof("Creating PVC: %s", instance.Spec.PvcName)
	pvcSpec, err := getPersistentVolumeClaimSpec(ctx, instance.Spec.PvcName, instance.Namespace, capacityInMb,
		storageClassName, accessMode, pvName, datastoreAccessibleTopology)
	if err != nil {
		msg := fmt.Sprintf("Failed to create spec for PVC: %q. Error: %v", instance.Spec.PvcName, err)
		log.Errorf(msg)
		setInstanceError(ctx, r, instance, msg)
		return reconcile.Result{RequeueAfter: timeout}, nil
	}
	log.Debugf("PVC spec is: %+v", pvcSpec)
	pvc, err := k8sclient.CoreV1().PersistentVolumeClaims(instance.Namespace).Create(ctx,
		pvcSpec, metav1.CreateOptions{})
	if err != nil {
		if apierrors.IsAlreadyExists(err) {
			log.Infof("PVC: %s already exists", instance.Spec.PvcName)
			pvc, err = k8sclient.CoreV1().PersistentVolumeClaims(instance.Namespace).Get(ctx,
				instance.Spec.PvcName, metav1.GetOptions{})
			if err != nil {
				msg := fmt.Sprintf("Failed to get PVC: %s on namespace: %s", instance.Spec.PvcName, instance.Namespace)
				log.Errorf(msg)
				setInstanceError(ctx, r, instance, msg)
				return reconcile.Result{RequeueAfter: timeout}, nil
			}
			if pvc.Status.Phase == v1.ClaimBound && pvc.Spec.VolumeName != pvName {
				// This is handle cases where PVC with this name already exists and
				// is bound. This happens when a new CnsRegisterVolume instance is
				// created to import a new volume with PVC name which is already
				// created and is bound.
				msg := fmt.Sprintf("Another PVC: %s already exists in namespace: %s which is Bound to a different PV",
					instance.Spec.PvcName, instance.Namespace)
				log.Errorf(msg)
				setInstanceError(ctx, r, instance, msg)
				// Untag the CNS volume which was created previously.
				_, err = common.DeleteVolumeUtil(ctx, r.volumeManager, volumeID, false)
				if err != nil {
					log.Errorf("Failed to untag CNS volume: %s with error: %+v", volumeID, err)
				} else {
					// Delete PV created above.
					err = k8sclient.CoreV1().PersistentVolumes().Delete(ctx, pvName, *metav1.NewDeleteOptions(0))
					if err != nil {
						log.Errorf("Failed to delete PV: %s with error: %+v", pvName, err)
					}
				}
				return reconcile.Result{RequeueAfter: timeout}, nil
			}
		} else {
			log.Errorf("Failed to create PVC with spec: %+v. Error: %+v", pvcSpec, err)
			setInstanceError(ctx, r, instance,
				fmt.Sprintf("Failed to create PVC: %s for volume with err: %+v", instance.Spec.PvcName, err))
			// Delete PV created above.
			err = k8sclient.CoreV1().PersistentVolumes().Delete(ctx, pvName, *metav1.NewDeleteOptions(0))
			if err != nil {
				log.Errorf("Delete PV %s failed with error: %+v", pvName, err)
			}
			setInstanceError(ctx, r, instance,
				fmt.Sprintf("Delete PV %s failed with error: %+v", pvName, err))
			return reconcile.Result{RequeueAfter: timeout}, nil
		}
	} else {
		log.Infof("PVC: %s is created successfully", instance.Spec.PvcName)
	}
	// Watch for PVC to be bound.
	isBound, err := isPVCBound(ctx, k8sclient, pvc, time.Duration(1*time.Minute))
	if isBound {
		log.Infof("PVC: %s is bound", instance.Spec.PvcName)
		if syncer.IsPodVMOnStretchSupervisorFSSEnabled {
			// Create CNSVolumeInfo CR for static pv
			capacityInBytes := capacityInMb * common.MbInBytes
			capacity := resource.NewQuantity(capacityInBytes, resource.BinarySI)
			err = r.volumeInfoService.CreateVolumeInfoWithPolicyInfo(ctx, volumeID, instance.Namespace,
				volume.StoragePolicyId, storageClassName, vc.Config.Host, capacity)
			if err != nil {
				log.Errorf("failed to store volumeID %q namespace %s StoragePolicyID %q StorageClassName %q and vCenter %q "+
					"in CNSVolumeInfo CR. Error: %+v", volumeID, instance.Namespace, volume.StoragePolicyId,
					storageClassName, vc.Config.Host, err)
				return reconcile.Result{RequeueAfter: timeout}, nil
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
				storagepolicyusagev1alpha2.NameSuffixForPVC
			storagePolicyUsageCR := &storagepolicyusagev1alpha2.StoragePolicyUsage{}
			err = cnsOperatorClient.Get(ctx, apitypes.NamespacedName{
				Namespace: namespace,
				Name:      storagePolicyUsageCRName},
				storagePolicyUsageCR)
			if err != nil {
				log.Errorf("failed to fetch %s instance with name %q from supervisor namespace %q. Error: %+v",
					storagepolicyusagev1alpha2.CRDSingular, storagePolicyUsageCRName,
					namespace, err)
				return reconcile.Result{RequeueAfter: timeout}, nil
			}

			// Patch an increase of "reserved" in storagePolicyUsageCR.
			patchedStoragePolicyUsageCR := storagePolicyUsageCR.DeepCopy()
			if storagePolicyUsageCR.Status.ResourceTypeLevelQuotaUsage != nil &&
				storagePolicyUsageCR.Status.ResourceTypeLevelQuotaUsage.Reserved != nil {
				patchedStoragePolicyUsageCR.Status.ResourceTypeLevelQuotaUsage.Reserved.Add(*capacity)
			} else {
				var (
					usedQty     resource.Quantity
					reservedQty resource.Quantity
				)
				reservedQty = *resource.NewQuantity(capacity.Value(), capacity.Format)
				patchedStoragePolicyUsageCR.Status = storagepolicyusagev1alpha2.StoragePolicyUsageStatus{
					ResourceTypeLevelQuotaUsage: &storagepolicyusagev1alpha2.QuotaUsageDetails{
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
			currentStoragePolicyUsageCR := &storagepolicyusagev1alpha2.StoragePolicyUsage{}
			finalStoragePolicyUsageCR := &storagepolicyusagev1alpha2.StoragePolicyUsage{}
			key := apitypes.NamespacedName{Namespace: storagePolicyUsageCR.Namespace,
				Name: storagePolicyUsageCR.Name}
			err = cnsOperatorClient.Get(ctx, key, currentStoragePolicyUsageCR)
			if err != nil {
				log.Errorf("failed to get %s CR from supervisor namespace %q. Error: %+v",
					storagePolicyUsageCR.Name, storagePolicyUsageCR.Namespace, err)
				return reconcile.Result{RequeueAfter: timeout}, nil
			}
			finalStoragePolicyUsageCR = currentStoragePolicyUsageCR.DeepCopy()
			// Decrease the Reserved field for StoragePolicyUsageCR
			finalStoragePolicyUsageCR.Status.ResourceTypeLevelQuotaUsage.Reserved.Sub(
				*resource.NewQuantity(currentStoragePolicyUsageCR.Status.ResourceTypeLevelQuotaUsage.Reserved.Value(),
					currentStoragePolicyUsageCR.Status.ResourceTypeLevelQuotaUsage.Reserved.Format))
			// Increase the Used field for StoragePolicyUsageCR
			finalStoragePolicyUsageCR.Status.ResourceTypeLevelQuotaUsage.Used.Add(
				*currentStoragePolicyUsageCR.Status.ResourceTypeLevelQuotaUsage.Reserved)
			err = syncer.PatchStoragePolicyUsage(ctx, cnsOperatorClient, storagePolicyUsageCR, finalStoragePolicyUsageCR)
			if err != nil {
				log.Errorf("patching operation failed for StoragePolicyUsage CR: %q in namespace: %q. err: %v",
					currentStoragePolicyUsageCR.Name, currentStoragePolicyUsageCR.Namespace, err)
			} else {
				log.Infof("Successfully decreased the reserved field by %v Mb "+
					"for storagepolicyusage CR: %q in namespace: %q",
					currentStoragePolicyUsageCR.Status.ResourceTypeLevelQuotaUsage.Reserved.Value(), finalStoragePolicyUsageCR.Name,
					finalStoragePolicyUsageCR.Namespace)
				log.Infof("Successfully increased the used field by %v Mb "+
					"for storagepolicyusage CR: %q in namespace: %q",
					currentStoragePolicyUsageCR.Status.ResourceTypeLevelQuotaUsage.Reserved.Value(), finalStoragePolicyUsageCR.Name,
					finalStoragePolicyUsageCR.Namespace)
			}
		}
	} else {
		log.Errorf("PVC: %s is not bound. Error: %+v", instance.Spec.PvcName, err)
		setInstanceError(ctx, r, instance, fmt.Sprintf("PVC: %s is not bound", instance.Spec.PvcName))
		return reconcile.Result{RequeueAfter: timeout}, nil
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
	delete(backOffDuration, instance.Name)
	backOffDurationMapMutex.Unlock()
	log.Info(msg)
	return reconcile.Result{}, nil
}

// validateCnsRegisterVolumeSpec validates the input params of
// CnsRegisterVolume instance.
func validateCnsRegisterVolumeSpec(ctx context.Context, instance *cnsregistervolumev1alpha1.CnsRegisterVolume) error {
	if instance.Spec.VolumeID == "" && instance.Spec.DiskURLPath == "" {
		return errors.New("VolumeID or DiskURLPath must be specified")
	}

	if instance.Spec.VolumeID != "" && instance.Spec.DiskURLPath != "" {
		return errors.New("VolumeID and DiskURLPath cannot be specified together")
	}

	// DiskURLPath is only supported for block volumes i.e, ReadWriteOnce
	if instance.Spec.DiskURLPath != "" && instance.Spec.AccessMode != v1.ReadWriteOnce {
		return fmt.Errorf("DiskURLPath cannot be used with accessMode: %q", instance.Spec.AccessMode)
	}

	if instance.Spec.VolumeID != "" {
		pvName, found := commonco.ContainerOrchestratorUtility.GetPVNameFromCSIVolumeID(instance.Spec.VolumeID)
		if found && pvName != staticPvNamePrefix+instance.Spec.VolumeID {
			return fmt.Errorf("PV: %q with the volume ID: %q "+
				"is already present. Can not create multiple PV with same volume Id", pvName, instance.Spec.VolumeID)
		}
	}

	return nil
}

// isBlockVolumeRegisterRequest verifies if block volume register is requested
// via CnsRegisterVolume instance.
func isBlockVolumeRegisterRequest(ctx context.Context, instance *cnsregistervolumev1alpha1.CnsRegisterVolume) bool {
	return instance.Spec.AccessMode == v1.ReadWriteOnce || instance.Spec.DiskURLPath != ""
}

// setInstanceError sets error and records an event on the CnsRegisterVolume
// instance.
func setInstanceError(ctx context.Context, r *ReconcileCnsRegisterVolume,
	instance *cnsregistervolumev1alpha1.CnsRegisterVolume, errMsg string) {
	log := logger.GetLogger(ctx)
	instance.Status.Error = errMsg
	err := updateCnsRegisterVolume(ctx, r.client, instance)
	if err != nil {
		log.Errorf("updateCnsRegisterVolume failed. err: %v", err)
	}
	recordEvent(ctx, r, instance, v1.EventTypeWarning, errMsg)
}

// setInstanceSuccess sets instance to success and records an event on the
// CnsRegisterVolume instance.
func setInstanceSuccess(ctx context.Context, r *ReconcileCnsRegisterVolume,
	instance *cnsregistervolumev1alpha1.CnsRegisterVolume, pvcName string, pvcUID apitypes.UID, msg string) error {
	instance.Status.Registered = true
	instance.Status.Error = ""
	setInstanceOwnerRef(instance, pvcName, pvcUID)
	err := updateCnsRegisterVolume(ctx, r.client, instance)
	if err != nil {
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
	instance *cnsregistervolumev1alpha1.CnsRegisterVolume, eventType string, message string) {
	log := logger.GetLogger(ctx)
	log.Debugf("Event type is %s", eventType)
	switch eventType {
	case v1.EventTypeWarning:
		// Double backOff duration.
		backOffDurationMapMutex.Lock()
		backOffDuration[instance.Name] = backOffDuration[instance.Name] * 2
		r.recorder.Event(instance, v1.EventTypeWarning, "CnsRegisterVolumeFailed", message)
		backOffDurationMapMutex.Unlock()
	case v1.EventTypeNormal:
		// Reset backOff duration to one second.
		backOffDurationMapMutex.Lock()
		backOffDuration[instance.Name] = time.Second
		r.recorder.Event(instance, v1.EventTypeNormal, "CnsRegisterVolumeSucceeded", message)
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
