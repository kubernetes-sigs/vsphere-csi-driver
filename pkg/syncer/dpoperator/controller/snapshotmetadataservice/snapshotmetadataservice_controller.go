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

package snapshotmetadataservice

import (
	"context"
	"crypto/x509"
	"encoding/pem"
	"fmt"
	"net"
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
	cnsconfig "sigs.k8s.io/vsphere-csi-driver/v3/pkg/common/config"
	csifault "sigs.k8s.io/vsphere-csi-driver/v3/pkg/common/fault"
	"sigs.k8s.io/vsphere-csi-driver/v3/pkg/csi/service/logger"
	k8s "sigs.k8s.io/vsphere-csi-driver/v3/pkg/kubernetes"
)

const (
	maxWorkerThreads = 1

	// targetCertificateName is the cert-manager Certificate that issues the
	// self-signed serving certificate for the Snapshot Metadata gRPC endpoint.
	// In the self-signed single-CA model, the Secret's ca.crt IS this served
	// certificate, so the certificate must itself carry the LB IP in its IP SANs.
	targetCertificateName = "vmware-system-csi-snapshot-metadata-service-cert"

	// targetSMSName is the (cluster-scoped) SnapshotMetadataService CR that
	// this controller owns. It is the primary reconciled object: the controller
	// keeps its spec.caCert and spec.address in sync with the served certificate
	// and the LoadBalancer endpoint.
	targetSMSName = "csi.vsphere.vmware.com"

	// targetNamespace is the namespace for targetCertificate and targetService
	targetNamespace = "vmware-system-csi"

	// targetServiceName is the LoadBalancer Service that exposes the Snapshot
	// Metadata gRPC port. Its external ingress IP is the address advertised in
	// the SnapshotMetadataService CR and the IP that must appear in the served
	// certificate's IP SANs.
	targetServiceName = "vmware-system-csi-snapshot-metadata-service"
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

// Add creates a new SnapshotMetadataService Controller and adds it to the
// Manager. The Manager will set fields on the Controller and Start it when the
// Manager is Started.
func Add(mgr manager.Manager, clusterFlavor cnstypes.CnsClusterFlavor,
	_ *cnsconfig.ConfigurationInfo) error {
	ctx, log := logger.GetNewContextWithLogger()

	if clusterFlavor != cnstypes.CnsClusterFlavorWorkload {
		log.Debug("Not initializing the SnapshotMetadataService Controller as its a non-WCP CSI deployment")
		return nil
	}

	log.Infof("Initializing SnapshotMetadataService Controller")

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
	return &ReconcileSnapshotMetadataService{
		client:   mgr.GetClient(),
		scheme:   mgr.GetScheme(),
		recorder: recorder,
	}
}

// enqueueSMSRequest returns the single reconcile.Request for the owned,
// cluster-scoped SnapshotMetadataService CR. All secondary watches (Certificate,
// Service) map their events onto this request so the controller always reconciles
// its owned object regardless of which input changed.
func enqueueSMSRequest() []reconcile.Request {
	return []reconcile.Request{{NamespacedName: k8stypes.NamespacedName{Name: targetSMSName}}}
}

// add adds a new Controller to mgr with r as the reconcile.Reconciler.
//
// The controller is keyed on the SnapshotMetadataService CR (the resource whose
// desired state it maintains). The cert-manager Certificate and the LoadBalancer
// Service are watched as inputs; their relevant events are mapped onto the single
// SnapshotMetadataService reconcile request. Predicates filter out every event
// that cannot change the desired state, so the reconcile only runs when the CR,
// the served certificate, or the LB endpoint actually changes.
func add(mgr manager.Manager, r reconcile.Reconciler) error {
	_, log := logger.GetNewContextWithLogger()

	// Create a new controller.
	c, err := controller.New("snapshotmetadataservice-controller", mgr,
		controller.Options{Reconciler: r, MaxConcurrentReconciles: maxWorkerThreads})
	if err != nil {
		log.Errorf("failed to create new SnapshotMetadataService controller with error: %+v", err)
		return err
	}

	backOffDuration = make(map[string]time.Duration)

	// Primary watch: the owned SnapshotMetadataService CR. Reconcile on create,
	// and on updates only when spec.caCert or spec.address actually changed (drift
	// correction) — ignore status/metadata-only churn and our own no-op writes.
	smsPred := predicate.TypedFuncs[*snapshotmetadatav1beta1.SnapshotMetadataService]{
		CreateFunc: func(e event.TypedCreateEvent[*snapshotmetadatav1beta1.SnapshotMetadataService]) bool {
			return e.Object.GetName() == targetSMSName
		},
		UpdateFunc: func(e event.TypedUpdateEvent[*snapshotmetadatav1beta1.SnapshotMetadataService]) bool {
			if e.ObjectNew.GetName() != targetSMSName {
				return false
			}
			return !reflect.DeepEqual(e.ObjectOld.Spec.CACert, e.ObjectNew.Spec.CACert) ||
				e.ObjectOld.Spec.Address != e.ObjectNew.Spec.Address
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

	// Secondary watch: the served Certificate. A reissue (e.g. after we add the
	// LB IP to its SANs) surfaces as a Status change, which re-triggers the
	// reconcile so the new ca.crt is republished. Only the target Certificate
	// and only Status changes are interesting.
	certPred := predicate.TypedFuncs[*certmanagerv1.Certificate]{
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
			return false
		},
	}
	err = c.Watch(source.Kind(
		mgr.GetCache(),
		&certmanagerv1.Certificate{},
		handler.TypedEnqueueRequestsFromMapFunc(func(_ context.Context, _ *certmanagerv1.Certificate) []reconcile.Request {
			return enqueueSMSRequest()
		}),
		certPred))
	if err != nil {
		log.Errorf("failed to watch for changes to Certificate resource with error: %+v", err)
		return err
	}

	// Secondary watch: the LoadBalancer Service. Only the target Service and only
	// LoadBalancer ingress (external IP) changes are interesting.
	svcPred := predicate.TypedFuncs[*v1.Service]{
		CreateFunc: func(e event.TypedCreateEvent[*v1.Service]) bool {
			return e.Object.GetName() == targetServiceName &&
				e.Object.GetNamespace() == targetNamespace
		},
		UpdateFunc: func(e event.TypedUpdateEvent[*v1.Service]) bool {
			if e.ObjectNew.GetName() != targetServiceName ||
				e.ObjectNew.GetNamespace() != targetNamespace {
				return false
			}
			return !reflect.DeepEqual(
				e.ObjectOld.Status.LoadBalancer.Ingress,
				e.ObjectNew.Status.LoadBalancer.Ingress,
			)
		},
		DeleteFunc: func(e event.TypedDeleteEvent[*v1.Service]) bool {
			return false
		},
		GenericFunc: func(e event.TypedGenericEvent[*v1.Service]) bool {
			return false
		},
	}
	err = c.Watch(source.Kind(
		mgr.GetCache(),
		&v1.Service{},
		handler.TypedEnqueueRequestsFromMapFunc(func(_ context.Context, _ *v1.Service) []reconcile.Request {
			return enqueueSMSRequest()
		}),
		svcPred,
	))
	if err != nil {
		log.Errorf("failed to watch for changes to SMS Service resource with error: %+v", err)
		return err
	}
	return nil
}

// shouldProcessCertificate checks if the certificate should be processed
func shouldProcessCertificate(cert *certmanagerv1.Certificate) bool {
	return cert.Name == targetCertificateName && cert.Namespace == targetNamespace
}

// blank assignment to verify that ReconcileSnapshotMetadataService implements
// reconcile.Reconciler.
var _ reconcile.Reconciler = &ReconcileSnapshotMetadataService{}

// ReconcileSnapshotMetadataService reconciles the SnapshotMetadataService CR,
// keeping its spec.caCert and spec.address in sync with the served certificate
// and the LoadBalancer endpoint.
type ReconcileSnapshotMetadataService struct {
	client   client.Client
	scheme   *runtime.Scheme
	recorder record.EventRecorder
}

// Reconcile brings the SnapshotMetadataService CR to its desired state:
//   - the LoadBalancer IP is present in the served certificate's IP SANs (so
//     clients dialing the external IP pass TLS verification),
//   - spec.caCert holds the current served CA certificate, and
//   - spec.address holds the current LoadBalancer "IP:port" endpoint.
//
// In the self-signed single-CA model the served certificate IS the CA (ca.crt),
// so adding the LB IP reissues it and changes ca.crt. To avoid ever advertising
// a (caCert, address) pair the server cannot satisfy, the caCert/address publish
// is gated on the current ca.crt actually covering the LB IP; until cert-manager
// finishes reissuing, the reconcile requeues and the CR keeps its previous,
// consistent values.
//
// Note:
// The Controller will requeue the Request to be processed again if the returned
// error is non-nil or Result.Requeue is true. Otherwise, upon completion it
// will remove the work from the queue.
func (r *ReconcileSnapshotMetadataService) Reconcile(ctx context.Context,
	request reconcile.Request) (reconcile.Result, error) {
	ctx = logger.NewContextWithLogger(ctx)
	reconcileLog := logger.GetLogger(ctx)
	reconcileLog.Infof("Received Reconcile for request: %q", request.NamespacedName)
	reconcileLog.Infof("Started Reconcile for SnapshotMetadataService request: %q", request.NamespacedName)

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

	// Fetch the served Certificate and ensure it is ready before reading its Secret.
	cert := &certmanagerv1.Certificate{}
	if err := r.client.Get(ctx, k8stypes.NamespacedName{
		Name:      targetCertificateName,
		Namespace: targetNamespace,
	}, cert); err != nil {
		if apierrors.IsNotFound(err) {
			reconcileLog.Infof("Certificate %s/%s not found yet; requeuing",
				targetNamespace, targetCertificateName)
			return reconcile.Result{RequeueAfter: timeout}, nil
		}
		msg := fmt.Sprintf("Failed to get Certificate %s/%s. Err: %+v",
			targetNamespace, targetCertificateName, err)
		recordEvent(ctx, r, sms, v1.EventTypeWarning, msg)
		reconcileLog.Errorf("Operation failed, reporting failure status to Prometheus."+
			" Fault Type: %q", csifault.CSIApiServerOperationFault)
		return reconcile.Result{RequeueAfter: timeout}, nil
	}

	if !isCertificateReady(cert) {
		reconcileLog.Infof("Certificate %s/%s is not ready yet, requeuing",
			cert.Namespace, cert.Name)
		return reconcile.Result{RequeueAfter: timeout}, nil
	}

	// Read the served CA certificate from the Certificate's Secret.
	secretName := cert.Spec.SecretName
	if secretName == "" {
		msg := fmt.Sprintf("Certificate %s/%s has no secretName in spec", cert.Namespace, cert.Name)
		recordEvent(ctx, r, sms, v1.EventTypeWarning, msg)
		reconcileLog.Errorf("Operation failed, reporting failure status to Prometheus."+
			" Fault Type: %q", csifault.CSIInternalFault)
		return reconcile.Result{RequeueAfter: timeout}, nil
	}

	secret := &v1.Secret{}
	if err := r.client.Get(ctx, k8stypes.NamespacedName{
		Name:      secretName,
		Namespace: cert.Namespace,
	}, secret); err != nil {
		if apierrors.IsNotFound(err) {
			msg := fmt.Sprintf("Secret %s/%s not found yet", cert.Namespace, secretName)
			recordEvent(ctx, r, sms, v1.EventTypeWarning, msg)
			reconcileLog.Errorf("Operation failed, reporting failure status to Prometheus."+
				" Fault Type: %q", csifault.CSIApiServerOperationFault)
			return reconcile.Result{RequeueAfter: timeout}, nil
		}
		msg := fmt.Sprintf("Failed to get secret %s/%s. Err: %+v", cert.Namespace, secretName, err)
		recordEvent(ctx, r, sms, v1.EventTypeWarning, msg)
		reconcileLog.Errorf("Operation failed, reporting failure status to Prometheus."+
			" Fault Type: %q", csifault.CSIApiServerOperationFault)
		return reconcile.Result{RequeueAfter: timeout}, nil
	}

	caCertBytes, ok := secret.Data["ca.crt"]
	if !ok || len(caCertBytes) == 0 {
		msg := fmt.Sprintf("Secret %s/%s does not have ca.crt field or it is empty",
			cert.Namespace, secretName)
		recordEvent(ctx, r, sms, v1.EventTypeWarning, msg)
		reconcileLog.Errorf("Operation failed, reporting failure status to Prometheus."+
			" Fault Type: %q", csifault.CSIInternalFault)
		return reconcile.Result{RequeueAfter: timeout}, nil
	}

	// Read the LoadBalancer Service to learn the external endpoint.
	lbSvc, err := r.getSMSService(ctx)
	if err != nil {
		reconcileLog.Warnf("Failed to look up SMS Service %s/%s: %v; will retry",
			targetNamespace, targetServiceName, err)
		return reconcile.Result{RequeueAfter: timeout}, nil
	}
	lbIP := smsServiceLBIP(lbSvc)

	// Desired endpoint address. When there is no LB IP yet (LB still pending),
	// leave spec.address untouched (in-cluster DNS access still works) and skip
	// the IP-SAN handling below.
	desiredAddress := sms.Spec.Address
	if lbIP != "" {
		// Ensure the served certificate carries the LB IP in its SANs. If we had
		// to add it, cert-manager will reissue; requeue and let the Certificate
		// Status-change watch re-trigger us once the new ca.crt is available.
		changed, ipErr := r.ensureLeafCoversLBIP(ctx, cert, lbIP)
		if ipErr != nil {
			msg := fmt.Sprintf("Failed to set LB IP %s on Certificate %s/%s. Err: %+v",
				lbIP, cert.Namespace, cert.Name, ipErr)
			recordEvent(ctx, r, sms, v1.EventTypeWarning, msg)
			reconcileLog.Errorf("Operation failed, reporting failure status to Prometheus."+
				" Fault Type: %q", csifault.CSIApiServerOperationFault)
			return reconcile.Result{RequeueAfter: timeout}, nil
		}
		if changed {
			reconcileLog.Infof("Set Certificate %s/%s ipAddresses to LB IP %s; waiting for cert-manager to reissue",
				cert.Namespace, cert.Name, lbIP)
			return reconcile.Result{RequeueAfter: timeout}, nil
		}

		// Consistency gate: only publish once the served certificate actually
		// covers the LB IP. In the self-signed single-CA model ca.crt IS the
		// served certificate, so this also guarantees spec.caCert will match a
		// certificate valid for the advertised address.
		if !certPEMCoversIP(caCertBytes, lbIP) {
			reconcileLog.Infof("Served certificate does not yet cover LB IP %s (reissue pending); requeuing", lbIP)
			return reconcile.Result{RequeueAfter: timeout}, nil
		}

		if len(lbSvc.Spec.Ports) == 0 {
			reconcileLog.Warnf("SMS Service %s/%s has no ports; cannot compute address; requeuing",
				targetNamespace, targetServiceName)
			return reconcile.Result{RequeueAfter: timeout}, nil
		}
		desiredAddress = fmt.Sprintf("%s:%d", lbIP, lbSvc.Spec.Ports[0].Port)
	}

	// Publish the desired (caCert, address) pair to the SnapshotMetadataService CR
	// in a single patch so the two never diverge.
	needCACert := !reflect.DeepEqual(sms.Spec.CACert, caCertBytes)
	needAddress := sms.Spec.Address != desiredAddress
	if needCACert || needAddress {
		reconcileLog.Infof("Updating SnapshotMetadataService %s (caCert changed=%t, address changed=%t; address=%q)",
			targetSMSName, needCACert, needAddress, desiredAddress)
		patch := client.MergeFrom(sms.DeepCopy())
		sms.Spec.CACert = caCertBytes
		sms.Spec.Address = desiredAddress
		if err := r.client.Patch(ctx, sms, patch); err != nil {
			msg := fmt.Sprintf("Failed to patch SnapshotMetadataService %s. Err: %+v", targetSMSName, err)
			recordEvent(ctx, r, sms, v1.EventTypeWarning, msg)
			reconcileLog.Errorf("Operation failed, reporting failure status to Prometheus."+
				" Fault Type: %q", csifault.CSIApiServerOperationFault)
			return reconcile.Result{RequeueAfter: timeout}, nil
		}
		msg := fmt.Sprintf("Successfully synced SnapshotMetadataService %s (caCert, address=%q)",
			targetSMSName, desiredAddress)
		recordEvent(ctx, r, sms, v1.EventTypeNormal, msg)
	} else {
		reconcileLog.Infof("SnapshotMetadataService %s already up-to-date, no action needed", targetSMSName)
	}

	// Cleanup instance entry from backOffDuration map.
	backOffDurationMapMutex.Lock()
	delete(backOffDuration, sms.Name)
	backOffDurationMapMutex.Unlock()

	reconcileLog.Infof("Finished Reconcile for SnapshotMetadataService request: %q", request.NamespacedName)
	return reconcile.Result{}, nil
}

// getSMSService fetches the Snapshot Metadata LoadBalancer Service from the
// API server. A nil return (with nil error) means the Service does not exist
// yet; callers should treat that as "no LB IP yet".
func (r *ReconcileSnapshotMetadataService) getSMSService(ctx context.Context) (*v1.Service, error) {
	svc := &v1.Service{}
	if err := r.client.Get(ctx, k8stypes.NamespacedName{
		Name:      targetServiceName,
		Namespace: targetNamespace,
	}, svc); err != nil {
		if apierrors.IsNotFound(err) {
			return nil, nil
		}
		return nil, fmt.Errorf("failed to get SMS Service %s/%s: %w", targetNamespace, targetServiceName, err)
	}
	return svc, nil
}

// smsServiceLBIP returns the first LoadBalancer ingress IP from svc, or ""
// if the LB IP has not been assigned yet (or svc is nil).
func smsServiceLBIP(svc *v1.Service) string {
	if svc == nil || len(svc.Status.LoadBalancer.Ingress) == 0 {
		return ""
	}
	return svc.Status.LoadBalancer.Ingress[0].IP
}

// ensureLeafCoversLBIP ensures Certificate.Spec.IPAddresses contains exactly
// the current LoadBalancer IP, replacing whatever was there before (e.g. a
// stale IP left over from a previous LB assignment). It returns (true, nil) if
// it changed the IP SAN (the caller should requeue and wait for cert-manager
// to reissue), (false, nil) if the LB IP was already the sole entry, and
// (_, err) on patch failure.
func (r *ReconcileSnapshotMetadataService) ensureLeafCoversLBIP(
	ctx context.Context, cert *certmanagerv1.Certificate, lbIP string) (bool, error) {
	log := logger.GetLogger(ctx)

	if len(cert.Spec.IPAddresses) == 1 && cert.Spec.IPAddresses[0] == lbIP {
		return false, nil
	}

	log.Infof("ensureLeafCoversLBIP: setting Certificate %s/%s ipAddresses to current LB IP %s (was %v)",
		cert.Namespace, cert.Name, lbIP, cert.Spec.IPAddresses)
	patch := client.MergeFrom(cert.DeepCopy())
	cert.Spec.IPAddresses = []string{lbIP}
	if err := r.client.Patch(ctx, cert, patch); err != nil {
		return false, fmt.Errorf("failed to patch Certificate %s/%s with IP SAN %s: %w",
			cert.Namespace, cert.Name, lbIP, err)
	}
	return true, nil
}

// certPEMCoversIP reports whether any CERTIFICATE block in pemBytes has ip in its
// IP SANs. Used to gate publication of the SnapshotMetadataService endpoint on
// the served certificate actually being valid for the LoadBalancer IP.
func certPEMCoversIP(pemBytes []byte, ip string) bool {
	target := net.ParseIP(ip)
	if target == nil {
		return false
	}
	rest := pemBytes
	for {
		var block *pem.Block
		block, rest = pem.Decode(rest)
		if block == nil {
			break
		}
		if block.Type != "CERTIFICATE" {
			continue
		}
		cert, err := x509.ParseCertificate(block.Bytes)
		if err != nil {
			continue
		}
		for _, ipSAN := range cert.IPAddresses {
			if ipSAN.Equal(target) {
				return true
			}
		}
	}
	return false
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

// recordEvent records the event on the SnapshotMetadataService instance, sets the
// backOffDuration for the instance appropriately and logs the message.
// backOffDuration is reset to 1 second on success and doubled on failure.
func recordEvent(ctx context.Context, r *ReconcileSnapshotMetadataService,
	instance *snapshotmetadatav1beta1.SnapshotMetadataService, eventtype string, msg string) {
	log := logger.GetLogger(ctx)
	switch eventtype {
	case v1.EventTypeWarning:
		// Double backOff duration.
		backOffDurationMapMutex.Lock()
		backOffDuration[instance.Name] = backOffDuration[instance.Name] * 2
		backOffDurationMapMutex.Unlock()
		r.recorder.Event(instance, v1.EventTypeWarning, "SnapshotMetadataServiceSyncFailed", msg)
		log.Error(msg)
	case v1.EventTypeNormal:
		// Reset backOff duration to one second.
		backOffDurationMapMutex.Lock()
		backOffDuration[instance.Name] = time.Second
		backOffDurationMapMutex.Unlock()
		r.recorder.Event(instance, v1.EventTypeNormal, "SnapshotMetadataServiceSyncSucceeded", msg)
		log.Info(msg)
	}
}
