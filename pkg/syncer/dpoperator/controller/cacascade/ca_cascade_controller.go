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

// Package cacascade guards against a specific cert-manager gap: when a CA
// Certificate is reissued (routine renewal or otherwise), cert-manager does
// not cascade that to Certificates it signs - each leaf only reissues on its
// own independent schedule. Two leaves signed by the same CA, on two
// different schedules, can therefore end up trusting two different CA
// generations of each other for as long as either leaf's own renewal is not
// yet due - which, for the K8sCloudOperator client/server pair, is up to 90
// days. During that window every mTLS handshake between vsphere-syncer and
// csi-provisioner fails with "unknown authority"/"bad certificate", and
// since PlacePersistenceVolumeClaim's failure mode is to fail the whole
// CreateVolume call, every PVC creation on the Supervisor fails - silently,
// since neither cert-manager nor cainjector logs anything: both do exactly
// what they are configured to do.
//
// This is not hypothetical: the same gap, on the same shared-CA-plus-two-
// independently-scheduled-leaves shape, caused exactly this outage for
// storage-quota-webhook and cns-storage-quota-extension in production
// (2026-08-19 to 2026-08-24). That incident's own fix only re-syncs the two
// sides during an explicit component upgrade, which leaves the same gap
// open between upgrades - potentially for months. This controller instead
// watches the CA continuously and force-reissues both leaves the moment it
// detects a new CA generation, closing the window to the time it takes
// cert-manager to reissue plus kubelet to propagate the new Secret content
// (observed empirically in the low tens of seconds), rather than leaving it
// open until either leaf's own schedule or the next upgrade happens to
// catch up.
package cacascade

import (
	"context"
	"fmt"
	"strconv"

	certmanagerv1 "github.com/cert-manager/cert-manager/pkg/apis/certmanager/v1"
	cmmeta "github.com/cert-manager/cert-manager/pkg/apis/meta/v1"
	cnstypes "github.com/vmware/govmomi/cns/types"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	k8stypes "k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller"
	"sigs.k8s.io/controller-runtime/pkg/event"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	"sigs.k8s.io/controller-runtime/pkg/manager"
	"sigs.k8s.io/controller-runtime/pkg/predicate"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
	"sigs.k8s.io/controller-runtime/pkg/source"

	cnsconfig "sigs.k8s.io/vsphere-csi-driver/v3/pkg/common/config"
	"sigs.k8s.io/vsphere-csi-driver/v3/pkg/csi/service/logger"
)

const (
	maxWorkerThreads = 1

	targetNamespace = "vmware-system-csi"

	// caCertName is the K8sCloudOperator root CA. It signs both leaves below
	// and is the sole thing this controller watches.
	caCertName = "vmware-system-csi-k8scloudoperator-ca-cert"

	// The two leaves that must never trust a different CA generation than
	// each other.
	serverCertName = "vmware-system-csi-k8scloudoperator-server-cert"
	clientCertName = "vmware-system-csi-k8scloudoperator-client-cert"

	// lastCascadedCARevisionAnnotation records, on each leaf, the CA
	// Certificate revision this leaf was last force-reissued against. Without
	// it every reconcile of the CA - including ones triggered by something
	// other than a reissue - would re-trigger both leaves every time.
	lastCascadedCARevisionAnnotation = "cns.vmware.com/last-cascaded-ca-revision"
)

// Add creates a new CA cascade controller and adds it to the Manager. The
// Manager will set fields on the Controller and Start it when the Manager is
// Started.
func Add(mgr manager.Manager, clusterFlavor cnstypes.CnsClusterFlavor,
	_ *cnsconfig.ConfigurationInfo) error {
	_, log := logger.GetNewContextWithLogger()

	if clusterFlavor != cnstypes.CnsClusterFlavorWorkload {
		log.Debug("Not initializing the CA cascade controller as its a non-WCP CSI deployment")
		return nil
	}

	log.Infof("Initializing CA cascade controller")
	return add(mgr, newReconciler(mgr))
}

// newReconciler returns a new reconcile.Reconciler.
func newReconciler(mgr manager.Manager) reconcile.Reconciler {
	return &ReconcileCACascade{
		client: mgr.GetClient(),
	}
}

// add adds a new Controller to mgr with r as the reconcile.Reconciler.
//
// The only watch is on the CA Certificate itself, filtered to its Status
// changing (a Status change is what a reissue looks like; Spec/metadata-only
// churn is not interesting). request.NamespacedName is therefore always the
// CA's own identity - there is no separate owned object to map onto.
func add(mgr manager.Manager, r reconcile.Reconciler) error {
	_, log := logger.GetNewContextWithLogger()

	c, err := controller.New("ca-cascade-controller", mgr,
		controller.Options{Reconciler: r, MaxConcurrentReconciles: maxWorkerThreads})
	if err != nil {
		log.Errorf("failed to create new CA cascade controller with error: %+v", err)
		return err
	}

	caPred := predicate.TypedFuncs[*certmanagerv1.Certificate]{
		CreateFunc: func(e event.TypedCreateEvent[*certmanagerv1.Certificate]) bool {
			return isTargetCA(e.Object)
		},
		UpdateFunc: func(e event.TypedUpdateEvent[*certmanagerv1.Certificate]) bool {
			if !isTargetCA(e.ObjectNew) {
				return false
			}
			return !revisionEqual(e.ObjectOld.Status.Revision, e.ObjectNew.Status.Revision)
		},
		DeleteFunc: func(e event.TypedDeleteEvent[*certmanagerv1.Certificate]) bool {
			return false
		},
	}
	err = c.Watch(source.Kind(
		mgr.GetCache(),
		&certmanagerv1.Certificate{},
		&handler.TypedEnqueueRequestForObject[*certmanagerv1.Certificate]{},
		caPred))
	if err != nil {
		log.Errorf("failed to watch for changes to the K8sCloudOperator CA Certificate with error: %+v", err)
		return err
	}
	return nil
}

func isTargetCA(cert *certmanagerv1.Certificate) bool {
	return cert.Name == caCertName && cert.Namespace == targetNamespace
}

func revisionEqual(a, b *int) bool {
	if a == nil || b == nil {
		return a == b
	}
	return *a == *b
}

// blank assignment to verify that ReconcileCACascade implements
// reconcile.Reconciler.
var _ reconcile.Reconciler = &ReconcileCACascade{}

// ReconcileCACascade keeps the K8sCloudOperator server and client leaf
// Certificates from ever trusting a different CA generation than the CA
// Certificate that signs both of them.
type ReconcileCACascade struct {
	client client.Client
}

// Reconcile is invoked whenever the K8sCloudOperator CA Certificate's Status
// changes. It force-reissues both leaves unless each has already been
// cascaded to the CA's current revision.
func (r *ReconcileCACascade) Reconcile(ctx context.Context,
	request reconcile.Request) (reconcile.Result, error) {
	ctx = logger.NewContextWithLogger(ctx)
	log := logger.GetLogger(ctx)

	ca := &certmanagerv1.Certificate{}
	if err := r.client.Get(ctx, request.NamespacedName, ca); err != nil {
		if apierrors.IsNotFound(err) {
			return reconcile.Result{}, nil
		}
		log.Errorf("Failed to get CA Certificate %s. Err: %+v", request.NamespacedName, err)
		return reconcile.Result{}, err
	}

	if ca.Status.Revision == nil {
		log.Debugf("CA Certificate %s has not been issued yet; nothing to cascade", request.NamespacedName)
		return reconcile.Result{}, nil
	}
	caRevision := *ca.Status.Revision

	for _, leafName := range []string{serverCertName, clientCertName} {
		if err := r.forceReissueIfStale(ctx, leafName, caRevision); err != nil {
			log.Errorf("Failed to cascade CA revision %d to leaf %s/%s. Err: %+v",
				caRevision, targetNamespace, leafName, err)
			return reconcile.Result{}, err
		}
	}
	return reconcile.Result{}, nil
}

// forceReissueIfStale reissues the named leaf Certificate unless it has
// already been cascaded to caRevision. Reissuance is triggered by setting
// the Issuing condition - the same mechanism cmctl renew uses - so it is
// picked up by the leaf's existing CertWatcher with no pod restart.
func (r *ReconcileCACascade) forceReissueIfStale(ctx context.Context, name string, caRevision int) error {
	log := logger.GetLogger(ctx)

	leaf := &certmanagerv1.Certificate{}
	key := k8stypes.NamespacedName{Name: name, Namespace: targetNamespace}
	if err := r.client.Get(ctx, key, leaf); err != nil {
		if apierrors.IsNotFound(err) {
			log.Debugf("Leaf Certificate %s not found yet; skipping", key)
			return nil
		}
		return err
	}

	if leaf.Annotations[lastCascadedCARevisionAnnotation] == strconv.Itoa(caRevision) {
		return nil // already cascaded to this CA generation
	}

	log.Infof("CA Certificate %s/%s is now at revision %d; forcing reissuance of leaf %s to stay in sync",
		targetNamespace, caCertName, caRevision, name)

	now := metav1.Now()
	leaf.Status.Conditions = append(leaf.Status.Conditions, certmanagerv1.CertificateCondition{
		Type:               certmanagerv1.CertificateConditionIssuing,
		Status:             cmmeta.ConditionTrue,
		Reason:             "CACascade",
		Message:            fmt.Sprintf("signing CA reissued (revision %d); forcing reissuance to stay in sync", caRevision),
		LastTransitionTime: &now,
	})
	if err := r.client.Status().Update(ctx, leaf); err != nil {
		return fmt.Errorf("failed to set Issuing condition on %s: %w", key, err)
	}

	if leaf.Annotations == nil {
		leaf.Annotations = map[string]string{}
	}
	leaf.Annotations[lastCascadedCARevisionAnnotation] = strconv.Itoa(caRevision)
	if err := r.client.Update(ctx, leaf); err != nil {
		return fmt.Errorf("failed to record cascaded CA revision on %s: %w", key, err)
	}
	return nil
}
