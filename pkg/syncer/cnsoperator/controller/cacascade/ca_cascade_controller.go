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
// csi-provisioner fails with "unknown authority"/"bad certificate".
//
// As a fallback for drift the revision watch alone can miss - an
// out-of-band CA rotation that doesn't advance Status.Revision the way
// expected, or a reissue that silently didn't converge - this controller
// also requeues itself on a timer and, on every check, compares each leaf's
// actual cached CA (read from its own Secret) against the CA's live Secret
// content directly, rather than trusting its own cascade-annotation
// bookkeeping alone.
package cacascade

import (
	"bytes"
	"context"
	"fmt"
	"strconv"
	"time"

	certmanagerv1 "github.com/cert-manager/cert-manager/pkg/apis/certmanager/v1"
	cmmeta "github.com/cert-manager/cert-manager/pkg/apis/meta/v1"
	cnstypes "github.com/vmware/govmomi/cns/types"
	corev1 "k8s.io/api/core/v1"
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

	volumes "sigs.k8s.io/vsphere-csi-driver/v3/pkg/common/cns-lib/volume"
	cnsconfig "sigs.k8s.io/vsphere-csi-driver/v3/pkg/common/config"
	"sigs.k8s.io/vsphere-csi-driver/v3/pkg/csi/service/logger"
	"sigs.k8s.io/vsphere-csi-driver/v3/pkg/syncer/k8scloudoperator"
)

const (
	maxWorkerThreads = 1

	// lastCascadedCARevisionAnnotation records, on each leaf, the CA
	// Certificate revision this leaf was last force-reissued against.
	lastCascadedCARevisionAnnotation = "cns.vmware.com/last-cascaded-ca-revision"

	// driftCheckInterval is how often Reconcile re-runs even without a new
	// watch event, to actively re-verify each leaf still trusts the live CA.
	driftCheckInterval = 120 * time.Minute
)

// Add creates a new CA cascade controller and adds it to the Manager.
func Add(mgr manager.Manager, clusterFlavor cnstypes.CnsClusterFlavor,
	_ *cnsconfig.ConfigurationInfo, _ volumes.Manager) error {
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
	return cert.Name == k8scloudoperator.K8sCloudOperatorCACertName && cert.Namespace == cnsconfig.DefaultCSINamespace
}

func revisionEqual(a, b *int) bool {
	if a == nil || b == nil {
		return a == b
	}
	return *a == *b
}

var _ reconcile.Reconciler = &ReconcileCACascade{}

type ReconcileCACascade struct {
	client client.Client
}

// Reconcile is invoked whenever the K8sCloudOperator CA Certificate's Status
// changes, or periodically via driftCheckInterval. It force-reissues each
// leaf that has not yet been cascaded to the CA's current revision, and
// otherwise actively verifies the leaf still trusts the CA's live content.
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

	if ca.DeletionTimestamp != nil {
		log.Debugf("CA Certificate %s is being deleted; nothing to cascade", request.NamespacedName)
		return reconcile.Result{}, nil
	}

	if ca.Status.Revision == nil {
		log.Debugf("CA Certificate %s has not been issued yet; nothing to cascade", request.NamespacedName)
		return reconcile.Result{RequeueAfter: driftCheckInterval}, nil
	}
	caRevision := *ca.Status.Revision

	leafNames := []string{k8scloudoperator.K8sCloudOperatorServerCertName, k8scloudoperator.K8sCloudOperatorClientCertName}
	for _, leafName := range leafNames {
		if err := r.cascadeToLeaf(ctx, ca, leafName, caRevision); err != nil {
			log.Errorf("Failed to cascade CA revision %d to leaf %s/%s. Err: %+v",
				caRevision, cnsconfig.DefaultCSINamespace, leafName, err)
			return reconcile.Result{}, err
		}
	}
	return reconcile.Result{RequeueAfter: driftCheckInterval}, nil
}

// cascadeToLeaf reissues the named leaf Certificate if it is stale against
// caRevision. If a reissue was not needed (the leaf's cascade annotation
// already matches caRevision), it actively re-verifies, independent of that
// annotation, that the leaf's cached CA still matches the CA's live Secret
// content, and forces a reissue if they've drifted apart regardless of what
// the annotation claims.
func (r *ReconcileCACascade) cascadeToLeaf(ctx context.Context, ca *certmanagerv1.Certificate,
	leafName string, caRevision int) error {
	log := logger.GetLogger(ctx)

	key := k8stypes.NamespacedName{Name: leafName, Namespace: cnsconfig.DefaultCSINamespace}
	leaf := &certmanagerv1.Certificate{}
	if err := r.client.Get(ctx, key, leaf); err != nil {
		if apierrors.IsNotFound(err) {
			log.Debugf("Leaf Certificate %s not found yet; skipping", key)
			return nil
		}
		return err
	}

	reissued, err := r.forceReissueIfStale(ctx, ca, leaf, caRevision)
	if err != nil {
		return err
	}

	// Already reissued above - nothing more to do. Skip verifying now:
	// cert-manager reissues asynchronously, so the Secret can't have
	// converged yet and would always look stale.
	if reissued {
		return nil
	}

	known, trusted, err := r.leafTrustsLiveCA(ctx, ca, leaf)
	if err != nil {
		return err
	}
	if known && !trusted {
		log.Infof("Leaf Certificate %s's cached CA no longer matches the live CA despite matching "+
			"cascade annotation; forcing reissue", key)
		return r.reissueLeaf(ctx, leaf, caRevision, "CADriftDetected",
			"leaf's cached CA no longer matches the live CA; forcing reissue")
	}
	return nil
}

// forceReissueIfStale reissues leaf unless it has already been cascaded to
// caRevision. Before reissuing, it checks whether the leaf's actual issued
// Secret already trusts the CA's actual live Secret content despite a stale or
// missing cascade annotation.
func (r *ReconcileCACascade) forceReissueIfStale(ctx context.Context, ca, leaf *certmanagerv1.Certificate,
	caRevision int) (bool, error) {
	log := logger.GetLogger(ctx)
	key := k8stypes.NamespacedName{Name: leaf.Name, Namespace: leaf.Namespace}

	if leaf.Annotations[lastCascadedCARevisionAnnotation] == strconv.Itoa(caRevision) {
		return false, nil // already cascaded to this CA generation
	}

	known, trusted, err := r.leafTrustsLiveCA(ctx, ca, leaf)
	if err != nil {
		return false, err
	}
	if known && trusted {
		log.Infof("Leaf Certificate %s already trusts the live CA despite a stale or missing cascade "+
			"annotation; backfilling the annotation without forcing a reissue", key)
		return false, r.recordCascadedRevision(ctx, leaf, caRevision)
	}

	// Either confirmed stale, or unknown (e.g. the leaf hasn't been issued
	// for the first time yet) - fall back to forcing a reissue rather than
	// assuming it's fine.
	if err := r.reissueLeaf(ctx, leaf, caRevision, "CACascade",
		fmt.Sprintf("signing CA reissued (revision %d); forcing reissuance to stay in sync", caRevision)); err != nil {
		return false, err
	}
	return true, nil
}

// leafTrustsLiveCA reports whether it was able to determine that leaf's
// actual cached CA (its own Secret's ca.crt) matches ca's actual live
// Secret content (tls.crt). known is false whenever either Secret - or the
// specific key read from it - isn't there yet, since bytes.Equal(nil, nil)
// would otherwise report two absent values as trusting each other.
func (r *ReconcileCACascade) leafTrustsLiveCA(ctx context.Context,
	ca, leaf *certmanagerv1.Certificate) (known, trusted bool, err error) {
	caSecret := &corev1.Secret{}
	caSecretKey := k8stypes.NamespacedName{Name: ca.Spec.SecretName, Namespace: ca.Namespace}
	if err := r.client.Get(ctx, caSecretKey, caSecret); err != nil {
		if apierrors.IsNotFound(err) {
			return false, false, nil // CA not issued yet; nothing to compare
		}
		return false, false, fmt.Errorf("failed to get CA Secret %s: %w", caSecretKey, err)
	}
	caCert := caSecret.Data["tls.crt"]
	if len(caCert) == 0 {
		return false, false, nil // CA Secret exists but has no tls.crt yet; nothing to compare
	}

	leafSecret := &corev1.Secret{}
	leafSecretKey := k8stypes.NamespacedName{Name: leaf.Spec.SecretName, Namespace: leaf.Namespace}
	if err := r.client.Get(ctx, leafSecretKey, leafSecret); err != nil {
		if apierrors.IsNotFound(err) {
			return false, false, nil // leaf not issued yet; nothing to compare
		}
		return false, false, fmt.Errorf("failed to get leaf Secret %s: %w", leafSecretKey, err)
	}
	leafCACert := leafSecret.Data["ca.crt"]
	if len(leafCACert) == 0 {
		return false, false, nil // leaf Secret exists but has no ca.crt yet; nothing to compare
	}

	return true, bytes.Equal(caCert, leafCACert), nil
}

// reissueLeaf sets the Issuing condition on leaf - the same mechanism cmctl
// renew uses.
func (r *ReconcileCACascade) reissueLeaf(ctx context.Context, leaf *certmanagerv1.Certificate,
	caRevision int, reason, message string) error {
	log := logger.GetLogger(ctx)
	key := k8stypes.NamespacedName{Name: leaf.Name, Namespace: leaf.Namespace}
	log.Infof("CA Certificate %s/%s is now at revision %d; forcing reissuance of leaf %s to stay in sync",
		cnsconfig.DefaultCSINamespace, k8scloudoperator.K8sCloudOperatorCACertName, caRevision, key)

	now := metav1.Now()
	issuing := certmanagerv1.CertificateCondition{
		Type:               certmanagerv1.CertificateConditionIssuing,
		Status:             cmmeta.ConditionTrue,
		Reason:             reason,
		Message:            message,
		LastTransitionTime: &now,
	}
	// Status.Conditions is a listType=map keyed by Type: appending
	// unconditionally duplicates the entry and the API server rejects the
	// whole update whenever an Issuing condition already exists (e.g. left
	// over from a prior cascade, or from cert-manager's own processing).
	// Replace it in place if present instead.
	if idx := issuingConditionIndex(leaf.Status.Conditions); idx >= 0 {
		leaf.Status.Conditions[idx] = issuing
	} else {
		leaf.Status.Conditions = append(leaf.Status.Conditions, issuing)
	}
	if err := r.client.Status().Update(ctx, leaf); err != nil {
		return fmt.Errorf("failed to set Issuing condition on %s: %w", key, err)
	}

	return r.recordCascadedRevision(ctx, leaf, caRevision)
}

// recordCascadedRevision patches leaf's annotation to record caRevision as
// the CA generation it has been reconciled against, without touching its
// Status.
func (r *ReconcileCACascade) recordCascadedRevision(ctx context.Context, leaf *certmanagerv1.Certificate,
	caRevision int) error {
	key := k8stypes.NamespacedName{Name: leaf.Name, Namespace: leaf.Namespace}
	patch := client.MergeFrom(leaf.DeepCopy())
	if leaf.Annotations == nil {
		leaf.Annotations = map[string]string{}
	}
	leaf.Annotations[lastCascadedCARevisionAnnotation] = strconv.Itoa(caRevision)
	if err := r.client.Patch(ctx, leaf, patch); err != nil {
		return fmt.Errorf("failed to record cascaded CA revision on %s: %w", key, err)
	}
	return nil
}

// issuingConditionIndex returns the index of the existing Issuing condition
// in conditions, or -1 if none is present.
func issuingConditionIndex(conditions []certmanagerv1.CertificateCondition) int {
	for i, c := range conditions {
		if c.Type == certmanagerv1.CertificateConditionIssuing {
			return i
		}
	}
	return -1
}
