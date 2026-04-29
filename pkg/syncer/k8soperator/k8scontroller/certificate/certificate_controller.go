/*
Copyright 2026 The Kubernetes Authors.

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

package certificate

import (
	"context"
	"fmt"
	"reflect"
	"sync"
	"time"

	certmanagerv1 "github.com/cert-manager/cert-manager/pkg/apis/certmanager/v1"
	cmmeta "github.com/cert-manager/cert-manager/pkg/apis/meta/v1"
	snapshotmetadatav1beta1 "github.com/kubernetes-csi/external-snapshot-metadata/client/apis/snapshotmetadataservice/v1beta1"
	cnstypes "github.com/vmware/govmomi/cns/types"
	v1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/runtime"
	k8stypes "k8s.io/apimachinery/pkg/types"
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

	cnsoperatorapis "sigs.k8s.io/vsphere-csi-driver/v3/pkg/apis/cnsoperator"
	csifault "sigs.k8s.io/vsphere-csi-driver/v3/pkg/common/fault"
	"sigs.k8s.io/vsphere-csi-driver/v3/pkg/csi/service/common"
	"sigs.k8s.io/vsphere-csi-driver/v3/pkg/csi/service/common/commonco"
	"sigs.k8s.io/vsphere-csi-driver/v3/pkg/csi/service/logger"
	k8s "sigs.k8s.io/vsphere-csi-driver/v3/pkg/kubernetes"
)

const (
	maxWorkerThreads = 1

	targetCertificateName = "vmware-system-csi-snapshot-metadata-service-cert"
	targetSMSName         = "csi.vsphere.vmware.com"
	targetNamespace       = "vmware-system-csi"
)

// backOffDuration is a map of certificate name's to the time after
// which a request for this instance will be requeued.
// Initialized to 1 second for new instances and for instances whose latest
// reconcile operation succeeded.
// If the reconcile fails, backoff is incremented exponentially.
var (
	backOffDuration         map[string]time.Duration
	backOffDurationMapMutex = sync.Mutex{}

	// getContainerOrchestratorInterface is a package-level indirection over
	// commonco.GetContainerOrchestratorInterface so unit tests can inject a
	// fake CO interface without requiring a real Kubernetes client.
	getContainerOrchestratorInterface = commonco.GetContainerOrchestratorInterface
)

// Add creates a new certificate Controller and adds it to the Manager,
// The Manager will set fields on the Controller and Start it when the Manager is Started.
func Add(mgr manager.Manager, clusterFlavor cnstypes.CnsClusterFlavor) error {
	ctx, log := logger.GetNewContextWithLogger()

	// TODO: remove this check once we add changes for Guest cluster
	if clusterFlavor != cnstypes.CnsClusterFlavorWorkload {
		log.Debug("Not initializing the Certificate Controller as its a non-WCP CSI deployment")
		return nil
	}

	// Check if CSI_Backup_API FSS is enabled
	coCommonInterface, err := getContainerOrchestratorInterface(ctx,
		common.Kubernetes, clusterFlavor, nil)
	if err != nil {
		log.Errorf("Failed to get CO common interface. Err: %v", err)
		return err
	}

	isCSIBackupAPIEnabled := coCommonInterface.IsFSSEnabled(ctx, common.CSI_Backup_API)
	if !isCSIBackupAPIEnabled {
		log.Infof("CSI_Backup_API FSS is not enabled. Skipping Certificate Controller initialization")
		return nil
	}

	log.Infof("CSI_Backup_API FSS is enabled. Initializing Certificate Controller")

	// Initializes kubernetes client.
	k8sclient, err := k8s.NewClient(ctx)
	if err != nil {
		log.Errorf("Creating Kubernetes client failed. Err: %v", err)
		return err
	}
	// eventBroadcaster broadcasts events on certificate instances to
	// the event sink.
	eventBroadcaster := record.NewBroadcaster()
	eventBroadcaster.StartRecordingToSink(
		&typedcorev1.EventSinkImpl{
			Interface: k8sclient.CoreV1().Events(""),
		},
	)

	recorder := eventBroadcaster.NewRecorder(scheme.Scheme, v1.EventSource{Component: cnsoperatorapis.GroupName})
	return add(mgr, newReconciler(mgr, recorder))
}

// newReconciler returns a new reconcile.Reconciler
func newReconciler(mgr manager.Manager, recorder record.EventRecorder) reconcile.Reconciler {
	return &ReconcileCertificate{
		client:   mgr.GetClient(),
		scheme:   mgr.GetScheme(),
		recorder: recorder,
	}
}

// add adds a new Controller to mgr with r as the reconcile.Reconciler.
func add(mgr manager.Manager, r reconcile.Reconciler) error {
	_, log := logger.GetNewContextWithLogger()

	// Create a new controller.
	c, err := controller.New("certificate-controller", mgr,
		controller.Options{Reconciler: r, MaxConcurrentReconciles: maxWorkerThreads})
	if err != nil {
		log.Errorf("failed to create new Certificate controller with error: %+v", err)
		return err
	}

	backOffDuration = make(map[string]time.Duration)

	// Watch for changes to Certificate resource.
	pred := predicate.TypedFuncs[*certmanagerv1.Certificate]{
		CreateFunc: func(e event.TypedCreateEvent[*certmanagerv1.Certificate]) bool {
			return shouldProcessCertificate(e.Object)
		},
		UpdateFunc: func(e event.TypedUpdateEvent[*certmanagerv1.Certificate]) bool {
			if !shouldProcessCertificate(e.ObjectNew) {
				return false
			}
			return !reflect.DeepEqual(e.ObjectOld.Status, e.ObjectNew.Status)
		},
		DeleteFunc: func(e event.TypedDeleteEvent[*certmanagerv1.Certificate]) bool {
			log.Info("Ignoring Certificate reconciliation on delete event")
			return false
		},
	}

	err = c.Watch(source.Kind(
		mgr.GetCache(),
		&certmanagerv1.Certificate{},
		&handler.TypedEnqueueRequestForObject[*certmanagerv1.Certificate]{}, pred))
	if err != nil {
		log.Errorf("failed to watch for changes to Certificate resource with error: %+v", err)
		return err
	}
	return nil
}

// shouldProcessCertificate checks if the certificate should be processed
func shouldProcessCertificate(cert *certmanagerv1.Certificate) bool {
	return cert.Name == targetCertificateName && cert.Namespace == targetNamespace
}

// blank assignment to verify that ReconcileCertificate implements
// reconcile.Reconciler.
var _ reconcile.Reconciler = &ReconcileCertificate{}

// ReconcileCertificate reconciles a Certificate object.
type ReconcileCertificate struct {
	client   client.Client
	scheme   *runtime.Scheme
	recorder record.EventRecorder
}

// Reconcile reads that state of the cluster for a Certificate object
// and makes changes based on the state read and what is in the
// Certificate.Spec.
// Note:
// The Controller will requeue the Request to be processed again if the returned
// error is non-nil or Result.Requeue is true. Otherwise, upon completion it
// will remove the work from the queue.
func (r *ReconcileCertificate) Reconcile(ctx context.Context,
	request reconcile.Request) (reconcile.Result, error) {
	ctx = logger.NewContextWithLogger(ctx)
	reconcileLog := logger.GetLogger(ctx)
	reconcileLog.Infof("Received Reconcile for request: %q", request.NamespacedName)

	go func() {
		<-ctx.Done()
		reconcileLog.Infof("context canceled for reconcile for Certificate request: %q, error: %v",
			request.NamespacedName, ctx.Err())
	}()

	reconcileLog.Infof("Started Reconcile for Certificate request: %q", request.NamespacedName)

	// Fetch the Certificate instance
	instance := &certmanagerv1.Certificate{}
	err := r.client.Get(ctx, request.NamespacedName, instance)
	if err != nil {
		if apierrors.IsNotFound(err) {
			reconcileLog.Info("Certificate resource not found. Ignoring since object must be deleted.")
			return reconcile.Result{}, nil
		}
		reconcileLog.Errorf("Error reading the Certificate with name: %q. Err: %+v",
			request.Name, err)
		reconcileLog.Errorf("Operation failed, reporting failure status to Prometheus."+
			" Fault Type: %q", csifault.CSIInternalFault)
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

	reconcileLog.Infof("Reconciling Certificate with Request.Name: %q timeout %q seconds",
		request.Name, timeout)

	// Check if Certificate is ready
	if !isCertificateReady(instance) {
		reconcileLog.Infof("Certificate %s/%s is not ready yet, requeuing",
			instance.Namespace, instance.Name)
		return reconcile.Result{RequeueAfter: timeout}, nil
	}

	reconcileLog.Infof("Certificate %s/%s is ready (revision: %d), syncing CA certificate",
		instance.Namespace, instance.Name, instance.Status.Revision)

	// Get the Secret name from Certificate spec
	secretName := instance.Spec.SecretName
	if secretName == "" {
		msg := fmt.Sprintf("Certificate %s/%s has no secretName in spec",
			instance.Namespace, instance.Name)
		recordEvent(ctx, r, instance, v1.EventTypeWarning, msg)
		reconcileLog.Errorf("Operation failed, reporting failure status to Prometheus."+
			" Fault Type: %q", csifault.CSIInternalFault)
		return reconcile.Result{RequeueAfter: timeout}, nil
	}

	// Get the Secret using controller-runtime client
	secret := &v1.Secret{}
	err = r.client.Get(ctx, k8stypes.NamespacedName{
		Name:      secretName,
		Namespace: instance.Namespace,
	}, secret)
	if err != nil {
		if apierrors.IsNotFound(err) {
			msg := fmt.Sprintf("Secret %s/%s not found yet",
				instance.Namespace, secretName)
			recordEvent(ctx, r, instance, v1.EventTypeWarning, msg)
			reconcileLog.Errorf("Operation failed, reporting failure status to Prometheus."+
				" Fault Type: %q", csifault.CSIApiServerOperationFault)
			return reconcile.Result{RequeueAfter: timeout}, nil
		}
		msg := fmt.Sprintf("Failed to get secret %s/%s. Err: %+v",
			instance.Namespace, secretName, err)
		recordEvent(ctx, r, instance, v1.EventTypeWarning, msg)
		reconcileLog.Errorf("Operation failed, reporting failure status to Prometheus."+
			" Fault Type: %q", csifault.CSIApiServerOperationFault)
		return reconcile.Result{RequeueAfter: timeout}, nil
	}

	// Extract CA certificate from Secret
	caCertBytes, ok := secret.Data["ca.crt"]
	if !ok || len(caCertBytes) == 0 {
		msg := fmt.Sprintf("Secret %s/%s does not have ca.crt field or it is empty",
			instance.Namespace, secretName)
		recordEvent(ctx, r, instance, v1.EventTypeWarning, msg)
		reconcileLog.Errorf("Operation failed, reporting failure status to Prometheus."+
			" Fault Type: %q", csifault.CSIInternalFault)
		return reconcile.Result{RequeueAfter: timeout}, nil
	}

	reconcileLog.Infof("Found CA certificate in secret %s/%s (length: %d bytes)",
		instance.Namespace, secretName, len(caCertBytes))

	// Get the SnapshotMetadataService CR
	sms := &snapshotmetadatav1beta1.SnapshotMetadataService{}
	err = r.client.Get(ctx, k8stypes.NamespacedName{Name: targetSMSName}, sms)
	if err != nil {
		if apierrors.IsNotFound(err) {
			reconcileLog.Infof("SnapshotMetadataService %s not found, skipping sync", targetSMSName)
			return reconcile.Result{}, nil
		}
		msg := fmt.Sprintf("Failed to get SnapshotMetadataService %s. Err: %+v", targetSMSName, err)
		recordEvent(ctx, r, instance, v1.EventTypeWarning, msg)
		reconcileLog.Errorf("Operation failed, reporting failure status to Prometheus."+
			" Fault Type: %q", csifault.CSIApiServerOperationFault)
		return reconcile.Result{RequeueAfter: timeout}, nil
	}

	// Compare current CA certificate with the one in Secret
	if reflect.DeepEqual(sms.Spec.CACert, caCertBytes) {
		reconcileLog.Infof("CA certificate in SnapshotMetadataService is already up-to-date, no action needed")
		msg := fmt.Sprintf("CA certificate is up-to-date for Certificate %s/%s (revision: %d)",
			instance.Namespace, instance.Name, instance.Status.Revision)
		recordEvent(ctx, r, instance, v1.EventTypeNormal, msg)
		// Cleanup instance entry from backOffDuration map.
		backOffDurationMapMutex.Lock()
		delete(backOffDuration, instance.Name)
		backOffDurationMapMutex.Unlock()
		reconcileLog.Infof("Finished Reconcile for Certificate request: %q", request.NamespacedName)
		return reconcile.Result{}, nil
	}

	reconcileLog.Infof("CA certificate mismatch detected, patching SnapshotMetadataService CR")

	// Patch the SnapshotMetadataService CR with new CA certificate
	patch := client.MergeFrom(sms.DeepCopy())
	sms.Spec.CACert = caCertBytes

	err = r.client.Patch(ctx, sms, patch)
	if err != nil {
		msg := fmt.Sprintf("Failed to patch SnapshotMetadataService %s. Err: %+v", targetSMSName, err)
		recordEvent(ctx, r, instance, v1.EventTypeWarning, msg)
		reconcileLog.Errorf("Operation failed, reporting failure status to Prometheus."+
			" Fault Type: %q", csifault.CSIApiServerOperationFault)
		return reconcile.Result{RequeueAfter: timeout}, nil
	}

	msg := fmt.Sprintf("Successfully patched SnapshotMetadataService %s with new CA certificate (revision: %d)",
		targetSMSName, instance.Status.Revision)
	recordEvent(ctx, r, instance, v1.EventTypeNormal, msg)

	// Cleanup instance entry from backOffDuration map.
	backOffDurationMapMutex.Lock()
	delete(backOffDuration, instance.Name)
	backOffDurationMapMutex.Unlock()

	reconcileLog.Infof("Finished Reconcile for Certificate request: %q", request.NamespacedName)
	return reconcile.Result{}, nil
}

// isCertificateReady checks if the Certificate is in Ready state
func isCertificateReady(cert *certmanagerv1.Certificate) bool {
	for _, condition := range cert.Status.Conditions {
		if condition.Type == certmanagerv1.CertificateConditionReady {
			return condition.Status == cmmeta.ConditionTrue
		}
	}
	return false
}

// recordEvent records the event, sets the backOffDuration for the instance
// appropriately and logs the message.
// backOffDuration is reset to 1 second on success and doubled on failure.
func recordEvent(ctx context.Context, r *ReconcileCertificate,
	instance *certmanagerv1.Certificate, eventtype string, msg string) {
	log := logger.GetLogger(ctx)
	switch eventtype {
	case v1.EventTypeWarning:
		// Double backOff duration.
		backOffDurationMapMutex.Lock()
		backOffDuration[instance.Name] = backOffDuration[instance.Name] * 2
		backOffDurationMapMutex.Unlock()
		r.recorder.Event(instance, v1.EventTypeWarning, "CertificateSyncFailed", msg)
		log.Error(msg)
	case v1.EventTypeNormal:
		// Reset backOff duration to one second.
		backOffDurationMapMutex.Lock()
		backOffDuration[instance.Name] = time.Second
		backOffDurationMapMutex.Unlock()
		r.recorder.Event(instance, v1.EventTypeNormal, "CertificateSyncSucceeded", msg)
		log.Info(msg)
	}
}
