package syncer

import (
	"context"
	"fmt"
	"time"

	"k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	apitypes "k8s.io/apimachinery/pkg/types"
	restclient "k8s.io/client-go/rest"
	"k8s.io/client-go/tools/cache"
	"k8s.io/client-go/util/workqueue"
	"sigs.k8s.io/controller-runtime/pkg/client"

	cnsoperatorv1alpha1 "sigs.k8s.io/vsphere-csi-driver/v3/pkg/apis/cnsoperator"
	storagepolicyv1alpha3 "sigs.k8s.io/vsphere-csi-driver/v3/pkg/apis/cnsoperator/storagepolicy/v1alpha3"
	"sigs.k8s.io/vsphere-csi-driver/v3/pkg/csi/service/common"
	"sigs.k8s.io/vsphere-csi-driver/v3/pkg/csi/service/logger"
	"sigs.k8s.io/vsphere-csi-driver/v3/pkg/internalapis/cnsvolumeinfo"
	k8s "sigs.k8s.io/vsphere-csi-driver/v3/pkg/kubernetes"
)

const (
	// spuSnapshotWorkers is the number of workers recomputing snapshot StoragePolicyUsage CRs.
	// The same StoragePolicyUsage is never processed by two workers at once.
	spuSnapshotWorkers = 4
	// spuSnapshotRecomputeAttempts is the number of times a run re-reads, re-sums and re-patches
	// before the key is handed back to the rate limited work queue.
	spuSnapshotRecomputeAttempts = 3
	// spuSnapshotRetryDelay is multiplied by the attempt number to get the sleep between attempts.
	spuSnapshotRetryDelay = 100 * time.Millisecond
)

// spuSnapshotQueue is the work queue of the snapshot StoragePolicyUsage reconciler. It is nil until
// the reconciler is started. Keys are "<namespace>/<StoragePolicyUsage name>".
var spuSnapshotQueue workqueue.TypedRateLimitingInterface[string]

// enqueueSnapshotSPU adds the given StoragePolicyUsage key to the snapshot reconciler queue, if started.
func enqueueSnapshotSPU(key string) {
	if spuSnapshotQueue != nil {
		spuSnapshotQueue.Add(key)
	}
}

// spuSnapshotReconciler is the only writer of the Used value of snapshot StoragePolicyUsage CRs.
// CnsVolumeInfo event handlers and the full sync only add the StoragePolicyUsage key to the queue.
// The worker reads the StoragePolicyUsage by name, sums AggregatedSnapshotSize of the CnsVolumeInfo CRs
// of that StoragePolicyUsage from the informer index, and patches the total.
type spuSnapshotReconciler struct {
	client   client.Client
	informer cache.SharedIndexInformer
	queue    workqueue.TypedRateLimitingInterface[string]
}

// newSPUSnapshotClient creates a client with its own rate limiter for the reconciler, so that its
// requests do not use up the request budget of other syncer clients and vice versa.
func newSPUSnapshotClient(ctx context.Context) (client.Client, error) {
	log := logger.GetLogger(ctx)
	cfg, err := k8s.GetKubeConfig(ctx)
	if err != nil {
		return nil, err
	}
	cfg = restclient.CopyConfig(cfg)
	log.Infof("Snapshot StoragePolicyUsage client uses QPS: %v, Burst: %d", cfg.QPS, cfg.Burst)
	return k8s.NewClientForGroup(ctx, cfg, cnsoperatorv1alpha1.GroupName)
}

// newSPUSnapshotReconciler creates the reconciler and registers the event handlers on the informer.
func newSPUSnapshotReconciler(c client.Client, informer cache.SharedIndexInformer,
	queue workqueue.TypedRateLimitingInterface[string]) (*spuSnapshotReconciler, error) {
	r := &spuSnapshotReconciler{client: c, informer: informer, queue: queue}
	_, err := informer.AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc: func(obj interface{}) {
			// After the initial sync, a new CR has no snapshot size yet. Nothing to do.
			// Before it, every CR is added once, which enqueues every StoragePolicyUsage.
			if informer.HasSynced() && !hasSnapshotInfo(obj) {
				return
			}
			r.enqueue(obj)
		},
		UpdateFunc: func(oldObj, newObj interface{}) {
			if !snapshotFieldsChanged(oldObj, newObj) {
				return
			}
			r.enqueue(oldObj)
			r.enqueue(newObj)
		},
		DeleteFunc: func(obj interface{}) {
			r.enqueue(obj)
		},
	})
	if err != nil {
		return nil, err
	}
	return r, nil
}

// enqueue adds the StoragePolicyUsage key of the CnsVolumeInfo object to the queue.
func (r *spuSnapshotReconciler) enqueue(obj interface{}) {
	if tombstone, ok := obj.(cache.DeletedFinalStateUnknown); ok {
		obj = tombstone.Obj
	}
	u, ok := obj.(*unstructured.Unstructured)
	if !ok {
		return
	}
	if key, ok := cnsvolumeinfo.SPUSnapshotKey(u); ok {
		r.queue.Add(key)
	}
}

// snapshotInfoFields are the fields of a CnsVolumeInfo spec which decide its contribution to the
// snapshot StoragePolicyUsage, besides the StoragePolicyUsage key itself.
var snapshotInfoFields = []string{"aggregatedsnapshotsize", "validaggregatedsnapshotsize",
	"snapshotlatestoperationcompletetime", "storagePolicyID"}

func specField(obj interface{}, field string) interface{} {
	u, ok := obj.(*unstructured.Unstructured)
	if !ok {
		return nil
	}
	v, _, _ := unstructured.NestedFieldNoCopy(u.Object, "spec", field)
	return v
}

func hasSnapshotInfo(obj interface{}) bool {
	return specField(obj, "aggregatedsnapshotsize") != nil ||
		specField(obj, "snapshotlatestoperationcompletetime") != nil
}

// snapshotFieldsChanged returns true if the update can change a snapshot StoragePolicyUsage.
func snapshotFieldsChanged(oldObj, newObj interface{}) bool {
	oldU, ok1 := oldObj.(*unstructured.Unstructured)
	newU, ok2 := newObj.(*unstructured.Unstructured)
	if !ok1 || !ok2 {
		return false
	}
	oldKey, _ := cnsvolumeinfo.SPUSnapshotKey(oldU)
	newKey, _ := cnsvolumeinfo.SPUSnapshotKey(newU)
	if oldKey != newKey {
		return true
	}
	for _, f := range snapshotInfoFields {
		if fmt.Sprint(specField(oldObj, f)) != fmt.Sprint(specField(newObj, f)) {
			return true
		}
	}
	return false
}

// run waits for the informer cache to sync and then processes the queue until ctx is done.
// Workers start only after the sync, otherwise a restart would write partial sums.
func (r *spuSnapshotReconciler) run(ctx context.Context, workers int) {
	log := logger.GetLogger(ctx)
	if !cache.WaitForCacheSync(ctx.Done(), r.informer.HasSynced) {
		log.Errorf("snapshot StoragePolicyUsage reconciler: CnsVolumeInfo cache did not sync")
		return
	}
	go func() {
		<-ctx.Done()
		r.queue.ShutDown()
	}()
	for i := 0; i < workers; i++ {
		go func() {
			for r.processNext(ctx) {
			}
		}()
	}
}

func (r *spuSnapshotReconciler) processNext(ctx context.Context) bool {
	key, shutdown := r.queue.Get()
	if shutdown {
		return false
	}
	defer r.queue.Done(key)
	log := logger.GetLogger(ctx)
	start := time.Now()
	if err := r.reconcile(ctx, key); err != nil {
		log.Errorf("snapshot StoragePolicyUsage %q: recompute failed, requeuing with backoff. Err: %v", key, err)
		r.queue.AddRateLimited(key)
		return true
	}
	r.queue.Forget(key)
	log.Debugf("snapshot StoragePolicyUsage %q: recomputed in %v, queue depth: %d", key, time.Since(start),
		r.queue.Len())
	return true
}

// reconcile retries the recompute a few times, re-reading and re-summing every time.
func (r *spuSnapshotReconciler) reconcile(ctx context.Context, key string) error {
	log := logger.GetLogger(ctx)
	ns, name, err := cache.SplitMetaNamespaceKey(key)
	if err != nil {
		// A malformed key can never succeed, drop it.
		log.Errorf("snapshot StoragePolicyUsage: dropping invalid key %q. Err: %v", key, err)
		return nil
	}
	for attempt := 1; attempt <= spuSnapshotRecomputeAttempts; attempt++ {
		if err = r.recomputeOnce(ctx, ns, name, key); err == nil {
			return nil
		}
		log.Warnf("snapshot StoragePolicyUsage %q: attempt %d failed. Err: %v", key, attempt, err)
		if attempt < spuSnapshotRecomputeAttempts {
			time.Sleep(spuSnapshotRetryDelay * time.Duration(attempt))
		}
	}
	return err
}

// recomputeOnce reads the StoragePolicyUsage, sums the AggregatedSnapshotSize of its CnsVolumeInfo CRs
// and patches Used (and the missing capacity annotation) if they differ.
func (r *spuSnapshotReconciler) recomputeOnce(ctx context.Context, ns, name, key string) error {
	log := logger.GetLogger(ctx)
	// 1. Read the current StoragePolicyUsage by name.
	spu := &storagepolicyv1alpha3.StoragePolicyUsage{}
	if err := r.client.Get(ctx, apitypes.NamespacedName{Namespace: ns, Name: name}, spu); err != nil {
		if errors.IsNotFound(err) {
			// The full sync enqueues it again once it is created.
			return nil
		}
		return err
	}

	// 2. Sum only the CnsVolumeInfo CRs of this StoragePolicyUsage, from the index.
	objs, err := r.informer.GetIndexer().ByIndex(cnsvolumeinfo.SPUSnapshotIndex, key)
	if err != nil {
		return err
	}
	total := resource.NewQuantity(0, resource.BinarySI)
	missing := false
	for _, o := range objs {
		u, ok := o.(*unstructured.Unstructured)
		if !ok {
			continue
		}
		if policyID, _, _ := unstructured.NestedString(u.Object, "spec", "storagePolicyID"); policyID !=
			spu.Spec.StoragePolicyId {
			continue
		}
		if s, found, _ := unstructured.NestedString(u.Object, "spec", "aggregatedsnapshotsize"); found {
			q, err := resource.ParseQuantity(s)
			if err != nil {
				log.Errorf("snapshot StoragePolicyUsage %q: invalid aggregatedsnapshotsize %q in CnsVolumeInfo %q. "+
					"Err: %v", key, s, u.GetName(), err)
				continue
			}
			total.Add(q)
		}
		if valid, _, _ := unstructured.NestedBool(u.Object, "spec", "validaggregatedsnapshotsize"); !valid {
			if t, _, _ := unstructured.NestedString(u.Object, "spec", "snapshotlatestoperationcompletetime"); t != "" &&
				t != "0001-01-01T00:00:00Z" {
				missing = true
			}
		}
	}

	// 3. Write only if something changed.
	usedChanged := spu.Status.ResourceTypeLevelQuotaUsage == nil ||
		spu.Status.ResourceTypeLevelQuotaUsage.Used == nil ||
		spu.Status.ResourceTypeLevelQuotaUsage.Used.Cmp(*total) != 0
	_, hasAnnotation := spu.Annotations[common.MissingSnapshotAggregatedCapacity]
	annotationChanged := missing != hasAnnotation

	if usedChanged {
		patched := spu.DeepCopy()
		if patched.Status.ResourceTypeLevelQuotaUsage == nil {
			patched.Status.ResourceTypeLevelQuotaUsage = &storagepolicyv1alpha3.QuotaUsageDetails{}
		}
		patched.Status.ResourceTypeLevelQuotaUsage.Used = total
		if err := patchStoragePolicyUsageStatusOnce(ctx, r.client, spu, patched); err != nil {
			return err
		}
		log.Infof("snapshot StoragePolicyUsage %q: set Used to %s", key, total.String())
	}
	if annotationChanged {
		if err := patchMissingSnapshotCapacityAnnotation(ctx, r.client, spu, missing); err != nil {
			return err
		}
		log.Infof("snapshot StoragePolicyUsage %q: set %s annotation to %v", key,
			common.MissingSnapshotAggregatedCapacity, missing)
	}
	return nil
}
