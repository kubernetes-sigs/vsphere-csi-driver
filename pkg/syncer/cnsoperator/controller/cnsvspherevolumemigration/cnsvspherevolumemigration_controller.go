package cnsvspherevolumemigration

import (
	"context"
	"fmt"
	"regexp"
	"strings"
	"time"

	cnstypes "github.com/vmware/govmomi/cns/types"
	v1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/kubernetes/scheme"
	typedcorev1 "k8s.io/client-go/kubernetes/typed/core/v1"
	"k8s.io/client-go/tools/record"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	"sigs.k8s.io/controller-runtime/pkg/manager"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
	"sigs.k8s.io/controller-runtime/pkg/source"

	"sigs.k8s.io/vsphere-csi-driver/pkg/common/cns-lib/vsphere"
	"sigs.k8s.io/vsphere-csi-driver/pkg/csi/service/logger"
	k8s "sigs.k8s.io/vsphere-csi-driver/pkg/kubernetes"
	syncertypes "sigs.k8s.io/vsphere-csi-driver/pkg/syncer/types"

	volumes "sigs.k8s.io/vsphere-csi-driver/pkg/common/cns-lib/volume"
	cnsvspherevolumemigrationv1alpha1 "sigs.k8s.io/vsphere-csi-driver/pkg/syncer/cnsoperator/apis/cnsvspherevolumemigration/v1alpha1"
)

// backOffDuration is a map of instance name to the time after which a request
// for this instance will be requeued.
// Initialized to 1 second for new instances and for instances whose latest reconcile
// operation succeeded.
// If the reconcile fails, backoff is incremented exponentially.
var backOffDuration map[string]time.Duration

// Add creates a new CnsvSphereVolumeMigration Controller and adds it to the Manager. The Manager will set fields on the Controller
// and Start it when the Manager is Started.
func Add(mgr manager.Manager, configInfo *syncertypes.ConfigInfo, volumeManager volumes.Manager) error {
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
func newReconciler(mgr manager.Manager, configInfo *syncertypes.ConfigInfo, volumeManager volumes.Manager, recorder record.EventRecorder) reconcile.Reconciler {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	ctx = logger.NewContextWithLogger(ctx)
	return &ReconcileCnsvSphereVolumeMigration{client: mgr.GetClient(), scheme: mgr.GetScheme(), configInfo: configInfo, volumeManager: volumeManager, recorder: recorder}
}

// add adds a new Controller to mgr with r as the reconcile.Reconciler
func add(mgr manager.Manager, r reconcile.Reconciler) error {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	ctx = logger.NewContextWithLogger(ctx)
	log := logger.GetLogger(ctx)
	// Create a new controller
	c, err := controller.New("cnsvspherevolumemigration-controller", mgr, controller.Options{Reconciler: r})
	if err != nil {
		return err
	}

	// Watch for changes to primary resource CnsvSphereVolumeMigration
	err = c.Watch(&source.Kind{Type: &cnsvspherevolumemigrationv1alpha1.CnsvSphereVolumeMigration{}}, &handler.EnqueueRequestForObject{})
	if err != nil {
		return err
	}

	backOffDuration = make(map[string]time.Duration)

	// Watch for changes to primary resource CnsvSphereVolumeMigration
	err = c.Watch(&source.Kind{Type: &cnsvspherevolumemigrationv1alpha1.CnsvSphereVolumeMigration{}}, &handler.EnqueueRequestForObject{})
	if err != nil {
		log.Errorf("failed to watch for changes to CnsvSphereVolumeMigration resource with error: %+v", err)
		return err
	}
	return nil
}

// blank assignment to verify that ReconcileCnsvSphereVolumeMigration implements reconcile.Reconciler
var _ reconcile.Reconciler = &ReconcileCnsvSphereVolumeMigration{}

// ReconcileCnsvSphereVolumeMigration reconciles a CnsvSphereVolumeMigration object
type ReconcileCnsvSphereVolumeMigration struct {
	// This client, initialized using mgr.Client() above, is a split client
	// that reads objects from the cache and writes to the apiserver
	client        client.Client
	scheme        *runtime.Scheme
	configInfo    *syncertypes.ConfigInfo
	volumeManager volumes.Manager
	recorder      record.EventRecorder
}

// Reconcile reads that state of the cluster for a CnsvSphereVolumeMigration object and makes changes based on the state read
// and what is in the CnsvSphereVolumeMigration.Spec
// Note:
// The Controller will requeue the Request to be processed again if the returned error is non-nil or
// Result.Requeue is true, otherwise upon completion it will remove the work from the queue.
func (r *ReconcileCnsvSphereVolumeMigration) Reconcile(request reconcile.Request) (reconcile.Result, error) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	ctx = logger.NewContextWithLogger(ctx)
	log := logger.GetLogger(ctx)

	// Fetch the CnsNodeVmAttachment instance
	instance := &cnsvspherevolumemigrationv1alpha1.CnsvSphereVolumeMigration{}
	err := r.client.Get(ctx, request.NamespacedName, instance)
	if err != nil {
		if apierrors.IsNotFound(err) {
			log.Error("CnsvSphereVolumeMigration resource not found. Ignoring since object must be deleted.")
			return reconcile.Result{}, nil
		}
		log.Errorf("Error reading the CnsvSphereVolumeMigration with name: %q on namespace: %q. Err: %+v",
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
	log.Infof("Reconciling CnsvSphereVolumeMigration with Request.Name: %q instance %q timeout %q seconds", request.Name, instance.Name, timeout)

	// If the CnsvSphereVolumeMigration instance is already registered and
	// not deleted by the user, remove the instance from the queue.
	if instance.Status.Registered && instance.DeletionTimestamp == nil {
		// Cleanup instance entry from backOffDuration map
		delete(backOffDuration, instance.Name)
		return reconcile.Result{}, nil
	}

	vcenter, err := syncertypes.GetVirtualCenterInstance(ctx, r.configInfo)
	if err != nil {
		msg := fmt.Sprintf("failed to get virtual center instance with error: %v", err)
		log.Errorf(msg)
		instance.Status.Error = err.Error()
		updateCnsvSphereVolumeMigration(ctx, r.client, instance)
		recordEvent(ctx, r, instance, v1.EventTypeWarning, msg)
		return reconcile.Result{RequeueAfter: timeout}, nil
	}

	vcHost := vcenter.Config.Host

	var containerClusterArray []cnstypes.CnsContainerCluster
	containerCluster := vsphere.GetContainerCluster(r.configInfo.Cfg.Global.ClusterID, r.configInfo.Cfg.VirtualCenter[vcHost].User, cnstypes.CnsClusterFlavorVanilla)
	containerClusterArray = append(containerClusterArray, containerCluster)

	re := regexp.MustCompile(`\[([^\[\]]*)\]`)
	if !re.MatchString(instance.Spec.VolumePath) {
		msg := fmt.Sprintf("failed to extract datastore name from in-tree volume path: %q", instance.Spec.VolumePath)
		log.Errorf(msg)
		instance.Status.Error = err.Error()
		updateCnsvSphereVolumeMigration(ctx, r.client, instance)
		recordEvent(ctx, r, instance, v1.EventTypeWarning, msg)
		return reconcile.Result{RequeueAfter: timeout}, nil
	}
	datastoreName := re.FindAllString(instance.Spec.VolumePath, -1)[0]
	vmdkPath := strings.TrimSpace(strings.Trim(instance.Spec.VolumePath, datastoreName))
	datastoreName = strings.Trim(strings.Trim(datastoreName, "["), "]")
	// backingDiskURLPath example: https://<vc_ip>/folder/<vm_vmdk_path>?dcPath=<datacenterName>&dsName=<datastoreName>
	backingDiskURLPath := "https://" + vcHost + "/folder/" +
		vmdkPath + "?dcPath=" + vcenter.Config.DatacenterPaths[0] + "&dsName=" + datastoreName

	createSpec := &cnstypes.CnsVolumeCreateSpec{
		Name:       instance.Spec.VolumeName,
		VolumeType: string(cnstypes.CnsVolumeTypeBlock),
		Metadata: cnstypes.CnsVolumeMetadata{
			ContainerCluster:      containerCluster,
			ContainerClusterArray: containerClusterArray,
		},
		BackingObjectDetails: &cnstypes.CnsBlockBackingDetails{
			BackingDiskUrlPath: backingDiskURLPath,
		},
	}

	log.Debugf("vSphere CNS driver is registering volume: %q for CnsvSphereVolumeMigration instance: %q using CreateSpec: %+v",
		instance.Spec.VolumePath, instance.Name, *createSpec)

	volumeID, registerErr := volumes.GetManager(ctx, vcenter).CreateVolume(ctx, createSpec)
	if registerErr != nil {
		msg := fmt.Sprintf("failed to register disk %s with error %+v", backingDiskURLPath, registerErr)
		log.Errorf(msg)
		instance.Status.Error = msg
	} else {
		log.Infof("successfully registered volume: %q with cns as Volume Id: %q", backingDiskURLPath, volumeID.Id)
		instance.Status.VolumeID = volumeID.Id
		instance.Status.Registered = true
		// Clear the error message
		instance.Status.Error = ""
	}

	err = updateCnsvSphereVolumeMigration(ctx, r.client, instance)
	if err != nil {
		msg := fmt.Sprintf("failed to update registration status on CnsvSphereVolumeMigration instance: %q on namespace: %q. Error: %+v",
			request.Name, request.Namespace, err)
		recordEvent(ctx, r, instance, v1.EventTypeWarning, msg)
		return reconcile.Result{RequeueAfter: timeout}, nil
	}

	if registerErr != nil {
		recordEvent(ctx, r, instance, v1.EventTypeWarning, "")
		return reconcile.Result{RequeueAfter: timeout}, nil
	}

	msg := fmt.Sprintf("ReconcileCnsvSphereVolumeMigration: Successfully registered volume %q in CNS with id: %q", instance.Spec.VolumePath, volumeID.Id)
	recordEvent(ctx, r, instance, v1.EventTypeNormal, msg)
	// Cleanup instance entry from backOffDuration map
	delete(backOffDuration, instance.Name)
	return reconcile.Result{}, nil
}

func updateCnsvSphereVolumeMigration(ctx context.Context, client client.Client, instance *cnsvspherevolumemigrationv1alpha1.CnsvSphereVolumeMigration) error {
	log := logger.GetLogger(ctx)
	err := client.Update(ctx, instance)
	if err != nil {
		log.Errorf("failed to update CnsvSphereVolumeMigration instance: %q on namespace: %q. Error: %+v",
			instance.Name, instance.Namespace, err)
	}
	return err
}

// recordEvent records the event, sets the backOffDuration for the instance appropriately
// and logs the message.
// backOffDuration is reset to 1 second on success and doubled on failure.
func recordEvent(ctx context.Context, r *ReconcileCnsvSphereVolumeMigration, instance *cnsvspherevolumemigrationv1alpha1.CnsvSphereVolumeMigration, eventtype string, msg string) {
	log := logger.GetLogger(ctx)
	switch eventtype {
	case v1.EventTypeWarning:
		// Double backOff duration
		backOffDuration[instance.Name] = backOffDuration[instance.Name] * 2
		r.recorder.Event(instance, v1.EventTypeWarning, "RegisterVolumeFailed", msg)
		log.Error(msg)
	case v1.EventTypeNormal:
		// Reset backOff duration to one second
		backOffDuration[instance.Name] = time.Second
		r.recorder.Event(instance, v1.EventTypeNormal, "RegisterVolumeSucceeded", msg)
		log.Info(msg)
	}
}
