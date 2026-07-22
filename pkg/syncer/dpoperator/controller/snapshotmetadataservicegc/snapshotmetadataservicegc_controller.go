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

// Package snapshotmetadataservicegc runs in the guest (VKS) cluster. It keeps
// the guest-local SnapshotMetadataService CR's spec.caCert in sync with the
// targetsecretname TLS secret that backs the guest cluster's own csi-snapshot-metadata
// sidecar (the one guest workloads dial directly for GetMetadataAllocated /
// GetMetadataDelta). That secret is a copy of a supervisor-issued leaf
// certificate (see docs/cbt-api-guest-cluster-deployment.md); its ca.crt field
// holds the CA that signed the leaf, which is exactly what spec.caCert must
// contain for the guest's csi-snapshot-metadata sidecar's serving cert to be
// trusted.
package snapshotmetadataservicegc

import (
	"context"
	"fmt"
	"reflect"
	"sync"
	"time"

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
	cnsconfig "sigs.k8s.io/vsphere-csi-driver/v3/pkg/common/config"
	csifault "sigs.k8s.io/vsphere-csi-driver/v3/pkg/common/fault"
	"sigs.k8s.io/vsphere-csi-driver/v3/pkg/csi/service/logger"
	k8s "sigs.k8s.io/vsphere-csi-driver/v3/pkg/kubernetes"
)

const (
	maxWorkerThreads = 1

	// targetSecretName is the guest-local copy of the supervisor-issued
	// leaf certificate that secures the guest cluster's own csi-snapshot-metadata
	// sidecar. Its ca.crt field is the CA that must be published in the guest
	// SnapshotMetadataService CR's spec.caCert.
	targetSecretName = "vmware-system-csi-snapshot-metadata-service-cert-gc"

	// targetNamespace is the namespace of targetSecretName in the guest cluster.
	targetNamespace = "vmware-system-csi"

	// targetSMSName is the (cluster-scoped) guest-local SnapshotMetadataService
	// CR that this controller owns.
	targetSMSName = "csi.vsphere.vmware.com"
)

// backOffDuration is a map of SnapshotMetadataService CR name's to the time
// after which a request for this instance will be requeued.
// Initialized to 1 second for new instances and for instances whose latest
// reconcile operation succeeded. If the reconcile fails, backoff is incremented
// exponentially.
var (
	backOffDuration         map[string]time.Duration
	backOffDurationMapMutex = sync.Mutex{}
)

// Add creates a new SnapshotMetadataServiceGC Controller and adds it to the
// Manager. The Manager will set fields on the Controller and Start it when the
// Manager is Started.
//
// This controller only runs in the guest (VKS) cluster's pvCSI syncer — it is
// a no-op in the supervisor, which owns its own SnapshotMetadataService CR via
// the sibling snapshotmetadataservice controller.
func Add(mgr manager.Manager, clusterFlavor cnstypes.CnsClusterFlavor,
	_ *cnsconfig.ConfigurationInfo) error {
	ctx, log := logger.GetNewContextWithLogger()

	if clusterFlavor != cnstypes.CnsClusterFlavorGuest {
		log.Debug("Not initializing the SnapshotMetadataServiceGC Controller as its not a Guest cluster CSI deployment")
		return nil
	}

	log.Infof("Initializing SnapshotMetadataServiceGC Controller")

	// Initializes kubernetes client.
	k8sclient, err := k8s.NewClient(ctx)
	if err != nil {
		log.Errorf("Creating Kubernetes client failed. Err: %v", err)
		return err
	}
	// eventBroadcaster broadcasts events on SnapshotMetadataService instances to
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
	return &ReconcileSnapshotMetadataServiceGC{
		client:   mgr.GetClient(),
		scheme:   mgr.GetScheme(),
		recorder: recorder,
	}
}

// enqueueSMSRequest returns the single reconcile.Request for the owned,
// cluster-scoped SnapshotMetadataService CR. The Secret watch maps its events
// onto this request so the controller always reconciles its owned object
// regardless of which input changed.
func enqueueSMSRequest() []reconcile.Request {
	return []reconcile.Request{{NamespacedName: k8stypes.NamespacedName{Name: targetSMSName}}}
}

// add adds a new Controller to mgr with r as the reconcile.Reconciler.
//
// The controller is keyed on the guest-local SnapshotMetadataService CR (the
// resource whose desired state it maintains). The targetsecretname TLS Secret is watched
// as the input; only changes to its ca.crt field are interesting, so the rest
// are filtered out before they ever reach Reconcile.
func add(mgr manager.Manager, r reconcile.Reconciler) error {
	_, log := logger.GetNewContextWithLogger()

	// Create a new controller.
	c, err := controller.New("snapshotmetadataservicegc-controller", mgr,
		controller.Options{Reconciler: r, MaxConcurrentReconciles: maxWorkerThreads})
	if err != nil {
		log.Errorf("failed to create new SnapshotMetadataServiceGC controller with error: %+v", err)
		return err
	}

	backOffDuration = make(map[string]time.Duration)

	// Primary watch: the owned SnapshotMetadataService CR. Reconcile on create,
	// and on updates only when spec.caCert actually changed (drift correction) —
	// ignore status/metadata-only churn and our own no-op writes.
	smsPred := predicate.TypedFuncs[*snapshotmetadatav1beta1.SnapshotMetadataService]{
		CreateFunc: func(e event.TypedCreateEvent[*snapshotmetadatav1beta1.SnapshotMetadataService]) bool {
			return e.Object.GetName() == targetSMSName
		},
		UpdateFunc: func(e event.TypedUpdateEvent[*snapshotmetadatav1beta1.SnapshotMetadataService]) bool {
			if e.ObjectNew.GetName() != targetSMSName {
				return false
			}
			return !reflect.DeepEqual(e.ObjectOld.Spec.CACert, e.ObjectNew.Spec.CACert)
		},
		DeleteFunc: func(e event.TypedDeleteEvent[*snapshotmetadatav1beta1.SnapshotMetadataService]) bool {
			log.Debug("Ignoring SnapshotMetadataService reconciliation on delete event")
			return false
		},
	}
	err = c.Watch(source.Kind(
		mgr.GetCache(),
		&snapshotmetadatav1beta1.SnapshotMetadataService{},
		&handler.TypedEnqueueRequestForObject[*snapshotmetadatav1beta1.SnapshotMetadataService]{},
		smsPred))
	if err != nil {
		log.Errorf("failed to watch for changes to SnapshotMetadataService resource with error: %+v", err)
		return err
	}

	// Secondary watch: the targetsecretname TLS Secret. Only the target Secret, and only
	// changes to its ca.crt field, are interesting.
	secretPred := predicate.TypedFuncs[*v1.Secret]{
		CreateFunc: func(e event.TypedCreateEvent[*v1.Secret]) bool {
			return shouldProcessSecret(e.Object)
		},
		UpdateFunc: func(e event.TypedUpdateEvent[*v1.Secret]) bool {
			if !shouldProcessSecret(e.ObjectNew) {
				return false
			}
			return !reflect.DeepEqual(e.ObjectOld.Data["ca.crt"], e.ObjectNew.Data["ca.crt"])
		},
		DeleteFunc: func(e event.TypedDeleteEvent[*v1.Secret]) bool {
			return false
		},
		GenericFunc: func(e event.TypedGenericEvent[*v1.Secret]) bool {
			return false
		},
	}
	err = c.Watch(source.Kind(
		mgr.GetCache(),
		&v1.Secret{},
		handler.TypedEnqueueRequestsFromMapFunc(func(_ context.Context, _ *v1.Secret) []reconcile.Request {
			return enqueueSMSRequest()
		}),
		secretPred,
	))
	if err != nil {
		log.Errorf("failed to watch for changes to the %q Secret with error: %+v", targetSecretName, err)
		return err
	}
	return nil
}

// shouldProcessSecret checks if the Secret should be processed
func shouldProcessSecret(secret *v1.Secret) bool {
	return secret.Name == targetSecretName && secret.Namespace == targetNamespace
}

// blank assignment to verify that ReconcileSnapshotMetadataServiceGC implements
// reconcile.Reconciler.
var _ reconcile.Reconciler = &ReconcileSnapshotMetadataServiceGC{}

// ReconcileSnapshotMetadataServiceGC reconciles the guest-local
// SnapshotMetadataService CR, keeping its spec.caCert in sync with the ca.crt
// field of the targetsecretname TLS Secret.
type ReconcileSnapshotMetadataServiceGC struct {
	client   client.Client
	scheme   *runtime.Scheme
	recorder record.EventRecorder
}

// Reconcile brings the guest-local SnapshotMetadataService CR's spec.caCert in
// sync with the current ca.crt of the targetsecretname Secret.
//
// Note:
// The Controller will requeue the Request to be processed again if the returned
// error is non-nil or Result.Requeue is true. Otherwise, upon completion it
// will remove the work from the queue.
func (r *ReconcileSnapshotMetadataServiceGC) Reconcile(ctx context.Context,
	request reconcile.Request) (reconcile.Result, error) {
	ctx = logger.NewContextWithLogger(ctx)
	reconcileLog := logger.GetLogger(ctx)
	reconcileLog.Infof("Received Reconcile for request: %q", request.NamespacedName)
	reconcileLog.Infof("Started Reconcile for SnapshotMetadataServiceGC request: %q", request.NamespacedName)

	// Fetch the owned SnapshotMetadataService CR (the primary object).
	sms := &snapshotmetadatav1beta1.SnapshotMetadataService{}
	if err := r.client.Get(ctx, k8stypes.NamespacedName{Name: targetSMSName}, sms); err != nil {
		if apierrors.IsNotFound(err) {
			reconcileLog.Infof("SnapshotMetadataService %s not found. Nothing to reconcile.", targetSMSName)
			return reconcile.Result{}, nil
		}
		reconcileLog.Errorf("Error reading SnapshotMetadataService %s. Err: %+v", targetSMSName, err)
		reconcileLog.Errorf("Operation failed, reporting failure status to Prometheus."+
			" Fault Type: %q", csifault.CSIInternalFault)
		return reconcile.Result{}, err
	}

	// Initialize backOffDuration for the instance, if required.
	backOffDurationMapMutex.Lock()
	var timeout time.Duration
	if _, exists := backOffDuration[sms.Name]; !exists {
		backOffDuration[sms.Name] = time.Second
	}
	timeout = backOffDuration[sms.Name]
	backOffDurationMapMutex.Unlock()

	// Fetch the targetsecretname Secret and read its ca.crt.
	secret := &v1.Secret{}
	if err := r.client.Get(ctx, k8stypes.NamespacedName{
		Name:      targetSecretName,
		Namespace: targetNamespace,
	}, secret); err != nil {
		if apierrors.IsNotFound(err) {
			msg := fmt.Sprintf("Secret %s/%s not found yet", targetNamespace, targetSecretName)
			recordEvent(ctx, r, sms, v1.EventTypeWarning, msg)
			reconcileLog.Errorf("Operation failed, reporting failure status to Prometheus."+
				" Fault Type: %q", csifault.CSIApiServerOperationFault)
			return reconcile.Result{RequeueAfter: timeout}, nil
		}
		msg := fmt.Sprintf("Failed to get secret %s/%s. Err: %+v", targetNamespace, targetSecretName, err)
		recordEvent(ctx, r, sms, v1.EventTypeWarning, msg)
		reconcileLog.Errorf("Operation failed, reporting failure status to Prometheus."+
			" Fault Type: %q", csifault.CSIApiServerOperationFault)
		return reconcile.Result{RequeueAfter: timeout}, nil
	}

	caCertBytes, ok := secret.Data["ca.crt"]
	if !ok || len(caCertBytes) == 0 {
		msg := fmt.Sprintf("Secret %s/%s does not have ca.crt field or it is empty",
			targetNamespace, targetSecretName)
		recordEvent(ctx, r, sms, v1.EventTypeWarning, msg)
		reconcileLog.Errorf("Operation failed, reporting failure status to Prometheus."+
			" Fault Type: %q", csifault.CSIInternalFault)
		return reconcile.Result{RequeueAfter: timeout}, nil
	}

	// Publish the CA cert to the SnapshotMetadataService CR if it changed.
	if !reflect.DeepEqual(sms.Spec.CACert, caCertBytes) {
		reconcileLog.Infof("Updating SnapshotMetadataService %s caCert from Secret %s/%s",
			targetSMSName, targetNamespace, targetSecretName)
		patch := client.MergeFrom(sms.DeepCopy())
		sms.Spec.CACert = caCertBytes
		if err := r.client.Patch(ctx, sms, patch); err != nil {
			msg := fmt.Sprintf("Failed to patch SnapshotMetadataService %s. Err: %+v", targetSMSName, err)
			recordEvent(ctx, r, sms, v1.EventTypeWarning, msg)
			reconcileLog.Errorf("Operation failed, reporting failure status to Prometheus."+
				" Fault Type: %q", csifault.CSIApiServerOperationFault)
			return reconcile.Result{RequeueAfter: timeout}, nil
		}
		msg := fmt.Sprintf("Successfully synced SnapshotMetadataService %s caCert from Secret %s/%s",
			targetSMSName, targetNamespace, targetSecretName)
		recordEvent(ctx, r, sms, v1.EventTypeNormal, msg)
	} else {
		reconcileLog.Infof("SnapshotMetadataService %s caCert already up-to-date, no action needed", targetSMSName)
	}

	// Cleanup instance entry from backOffDuration map.
	backOffDurationMapMutex.Lock()
	delete(backOffDuration, sms.Name)
	backOffDurationMapMutex.Unlock()

	reconcileLog.Infof("Finished Reconcile for SnapshotMetadataServiceGC request: %q", request.NamespacedName)
	return reconcile.Result{}, nil
}

// recordEvent records the event on the SnapshotMetadataService instance, sets the
// backOffDuration for the instance appropriately and logs the message.
// backOffDuration is reset to 1 second on success and doubled on failure.
func recordEvent(ctx context.Context, r *ReconcileSnapshotMetadataServiceGC,
	instance *snapshotmetadatav1beta1.SnapshotMetadataService, eventtype string, msg string) {
	log := logger.GetLogger(ctx)
	switch eventtype {
	case v1.EventTypeWarning:
		// Double backOff duration.
		backOffDurationMapMutex.Lock()
		backOffDuration[instance.Name] = backOffDuration[instance.Name] * 2
		backOffDurationMapMutex.Unlock()
		r.recorder.Event(instance, v1.EventTypeWarning, "SnapshotMetadataServiceGCSyncFailed", msg)
		log.Error(msg)
	case v1.EventTypeNormal:
		// Reset backOff duration to one second.
		backOffDurationMapMutex.Lock()
		backOffDuration[instance.Name] = time.Second
		backOffDurationMapMutex.Unlock()
		r.recorder.Event(instance, v1.EventTypeNormal, "SnapshotMetadataServiceGCSyncSucceeded", msg)
		log.Info(msg)
	}
}
