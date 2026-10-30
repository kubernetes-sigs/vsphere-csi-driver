package syncer

import (
	"context"
	"fmt"
	"math"
	"reflect"
	"strings"
	"sync"
	"time"

	"k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	apitypes "k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/wait"
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
	// Bound each recomputation so an API outage does not indefinitely occupy a worker.
	spuSnapshotRecomputeTimeout = 30 * time.Second
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

// spuSnapshotReconciler owns CSI updates to Used in snapshot StoragePolicyUsage CRs.
// Inventory event handlers and the full sync only add the StoragePolicyUsage key to the queue.
// The worker sums CVI snapshot capacity and, when enabled, DVI capacity from the informer indexes.
type spuSnapshotReconciler struct {
	client      client.Client
	informer    cache.SharedIndexInformer
	dviInformer cache.SharedIndexInformer
	queue       workqueue.TypedRateLimitingInterface[string]
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
	cfg.RateLimiter = nil // Construct a limiter for this client from the configured QPS and Burst.
	log.Infof("Snapshot StoragePolicyUsage client uses QPS: %v, Burst: %d", cfg.QPS, cfg.Burst)
	return k8s.NewClientForGroup(ctx, cfg, cnsoperatorv1alpha1.GroupName)
}

// newSPUSnapshotReconciler creates the reconciler and registers the event handlers on the informer.
func newSPUSnapshotReconciler(c client.Client, informer cache.SharedIndexInformer,
	queue workqueue.TypedRateLimitingInterface[string],
	dviInformer cache.SharedIndexInformer) (*spuSnapshotReconciler, error) {
	r := &spuSnapshotReconciler{client: c, informer: informer, queue: queue, dviInformer: dviInformer}
	if dviInformer != nil {
		// The volume-info service may already be running. AddIndexers backfills its store
		// under the cache lock before returning, so existing CVIs participate in the handoff.
		cviIndexer := informer.GetIndexer()
		if _, exists := cviIndexer.GetIndexers()[snapshotVolumeIDIndex]; !exists {
			if err := cviIndexer.AddIndexers(cache.Indexers{snapshotVolumeIDIndex: snapshotVolumeIDIndexFunc}); err != nil {
				return nil, err
			}
		}
		if _, err := dviInformer.AddEventHandler(cache.ResourceEventHandlerFuncs{
			AddFunc: r.enqueueDeletedVolumeInfo,
			UpdateFunc: func(oldObj, newObj interface{}) {
				if deletedVolumeInfoFieldsChanged(oldObj, newObj) {
					r.enqueueDeletedVolumeInfo(oldObj)
					r.enqueueDeletedVolumeInfo(newObj)
				}
			},
			DeleteFunc: r.enqueueDeletedVolumeInfo,
		}); err != nil {
			return nil, err
		}
	}
	_, err := informer.AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc: func(obj interface{}) {
			if dviInformer == nil && informer.HasSynced() && !hasSnapshotInfo(obj) {
				return
			}
			r.enqueue(obj)
		},
		UpdateFunc: func(oldObj, newObj interface{}) {
			if snapshotFieldsChanged(oldObj, newObj) {
				r.enqueue(oldObj)
				r.enqueue(newObj)
			}
		},
		DeleteFunc: r.enqueue,
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
	r.enqueueDeletedVolumeInfoForCVI(u)
	if key, ok := cnsvolumeinfo.SPUSnapshotKey(u); ok {
		r.queue.Add(key)
	} else if cnsvolumeinfo.HasSnapshotInfo(u) {
		logger.GetLogger(context.Background()).Errorf("Cannot identify snapshot StoragePolicyUsage for CnsVolumeInfo %q",
			u.GetName())
	}
}

// snapshotInfoFields are the fields of a CnsVolumeInfo spec which decide its contribution to the
// snapshot StoragePolicyUsage, besides the StoragePolicyUsage key itself.
var snapshotInfoFields = []string{"aggregatedsnapshotsize", "validaggregatedsnapshotsize",
	"snapshotlatestoperationcompletetime", "storagePolicyID", "volumeID"}

func hasSnapshotInfo(obj interface{}) bool {
	u, ok := obj.(*unstructured.Unstructured)
	return ok && cnsvolumeinfo.HasSnapshotInfo(u)
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
		oldValue, oldFound, _ := unstructured.NestedFieldNoCopy(oldU.Object, "spec", f)
		newValue, newFound, _ := unstructured.NestedFieldNoCopy(newU.Object, "spec", f)
		if oldFound != newFound || !reflect.DeepEqual(oldValue, newValue) {
			return true
		}
	}
	return false
}

// run waits for the informer cache to sync and then processes the queue until ctx is done.
// Workers start only after the sync, otherwise a restart would write partial sums.
func (r *spuSnapshotReconciler) run(ctx context.Context, workers int) {
	log := logger.GetLogger(ctx)
	defer r.queue.ShutDown()
	synced := []cache.InformerSynced{r.informer.HasSynced}
	if r.dviInformer != nil {
		go r.dviInformer.Run(ctx.Done())
		synced = append(synced, r.dviInformer.HasSynced)
		log.Info("snapshot StoragePolicyUsage reconciler: waiting for CnsVolumeInfo and CnsDeletedVolumeInfo inventories")
	}
	if !cache.WaitForCacheSync(ctx.Done(), synced...) {
		log.Errorf("snapshot StoragePolicyUsage reconciler: inventory caches did not sync")
		return
	}
	var workerGroup sync.WaitGroup
	for i := 0; i < workers; i++ {
		workerGroup.Add(1)
		go func() {
			defer workerGroup.Done()
			for r.processNext(ctx) {
			}
		}()
	}
	// CVI Add events cannot schedule an SPU whose last contributor was deleted while
	// the syncer was stopped. Enumerate all SPUs, retrying without blocking other keys.
	_ = wait.PollUntilContextCancel(ctx, 5*time.Second, true, func(ctx context.Context) (bool, error) {
		if err := r.enqueueAllSnapshotSPUs(ctx); err != nil {
			log.Errorf("snapshot StoragePolicyUsage reconciler: startup enumeration failed, retrying: %v", err)
			return false, nil
		}
		return true, nil
	})
	<-ctx.Done()
	r.queue.ShutDown()
	workerGroup.Wait()
}

func isCSISnapshotSPU(spu *storagepolicyv1alpha3.StoragePolicyUsage) bool {
	return spu.Spec.ResourceKind == ResourceKindSnapshot &&
		spu.Spec.ResourceExtensionName == SnapQuotaExtensionServiceName &&
		spu.Spec.ResourceAPIgroup != nil && *spu.Spec.ResourceAPIgroup == ResourceAPIgroupSnapshot
}

func (r *spuSnapshotReconciler) enqueueAllSnapshotSPUs(ctx context.Context) error {
	list := &storagepolicyv1alpha3.StoragePolicyUsageList{}
	if err := r.client.List(ctx, list); err != nil {
		return err
	}
	for i := range list.Items {
		spu := &list.Items[i]
		if isCSISnapshotSPU(spu) {
			r.queue.Add(spu.Namespace + "/" + spu.Name)
		}
	}
	return nil
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
	if key == unassignedDeletedVolumeInfoKey {
		return r.enqueueAllSnapshotSPUs(ctx)
	}
	if strings.HasPrefix(key, malformedDeletedVolumeInfoNamespaceKeyPrefix) {
		return r.enqueueSnapshotSPUsInNamespace(ctx, strings.TrimPrefix(key, malformedDeletedVolumeInfoNamespaceKeyPrefix))
	}
	ns, name, err := cache.SplitMetaNamespaceKey(key)
	if err != nil || ns == "" || name == "" {
		// A malformed key can never succeed, drop it.
		log.Errorf("snapshot StoragePolicyUsage: dropping invalid key %q. Err: %v", key, err)
		return nil
	}
	for attempt := 1; attempt <= spuSnapshotRecomputeAttempts; attempt++ {
		if err := ctx.Err(); err != nil {
			return err
		}
		if err = r.recomputeOnce(ctx, ns, name, key); err == nil {
			return nil
		}
		log.Warnf("snapshot StoragePolicyUsage %q: attempt %d failed. Err: %v", key, attempt, err)
		if attempt < spuSnapshotRecomputeAttempts {
			timer := time.NewTimer(spuSnapshotRetryDelay * time.Duration(attempt))
			select {
			case <-ctx.Done():
				timer.Stop()
				return ctx.Err()
			case <-timer.C:
			}
		}
	}
	return err
}

// recomputeOnce reads the StoragePolicyUsage, sums the AggregatedSnapshotSize of its CnsVolumeInfo CRs
// and patches Used (and the missing capacity annotation) if they differ.
func (r *spuSnapshotReconciler) recomputeOnce(ctx context.Context, ns, name, key string) error {
	ctx, cancel := context.WithTimeout(ctx, spuSnapshotRecomputeTimeout)
	defer cancel()
	log := logger.GetLogger(ctx)
	spu := &storagepolicyv1alpha3.StoragePolicyUsage{}
	if err := r.client.Get(ctx, apitypes.NamespacedName{Namespace: ns, Name: name}, spu); err != nil {
		if errors.IsNotFound(err) {
			objs, indexErr := r.informer.GetIndexer().ByIndex(cnsvolumeinfo.SPUSnapshotIndex, key)
			if indexErr != nil {
				return indexErr
			}
			dvis, indexErr := r.deletedVolumeInfosForSPU(key)
			if indexErr != nil {
				return indexErr
			}
			if len(objs) == 0 && len(dvis) == 0 {
				return nil // The association no longer exists; nothing remains to report.
			}
		}
		return err // Retry while a contributing CVI or DVI is waiting for SPU creation.
	}
	if !isCSISnapshotSPU(spu) {
		return nil
	}
	if spu.Spec.StoragePolicyId == "" {
		return fmt.Errorf("snapshot StoragePolicyUsage %q has no storage policy ID", key)
	}

	objs, err := r.informer.GetIndexer().ByIndex(cnsvolumeinfo.SPUSnapshotIndex, key)
	if err != nil {
		return err
	}
	total, missing, capacityErr := sumSnapshotCapacity(objs, spu.Spec.StoragePolicyId)
	if capacityErr == nil && r.dviInformer != nil {
		var dvis []interface{}
		dvis, capacityErr = r.deletedVolumeInfosForSPU(key)
		if capacityErr == nil {
			var deletedTotal *resource.Quantity
			deletedTotal, capacityErr = r.sumDeletedVolumeInfoCapacity(dvis, objs, spu.Spec.StoragePolicyId)
			if capacityErr == nil {
				total.Add(*deletedTotal)
			}
		}
	}
	_, hasAnnotation := spu.Annotations[common.MissingSnapshotAggregatedCapacity]
	if capacityErr != nil {
		// Never turn a malformed inventory into a downward correction of Used.
		if !hasAnnotation {
			if err := patchMissingSnapshotCapacityAnnotation(ctx, r.client, spu, true); err != nil {
				return fmt.Errorf("invalid snapshot inventory (%v), marking capacity missing: %w", capacityErr, err)
			}
		}
		return capacityErr
	}

	usage := spu.Status.ResourceTypeLevelQuotaUsage
	if usage == nil || usage.Used == nil || usage.Used.Cmp(*total) != 0 {
		if err := patchSnapshotUsedOnce(ctx, r.client, spu, total); err != nil {
			return err
		}
		log.Infof("snapshot StoragePolicyUsage %q: set Used to %s", key, total.String())
	}
	if missing != hasAnnotation {
		return patchMissingSnapshotCapacityAnnotation(ctx, r.client, spu, missing)
	}
	return nil
}

// sumSnapshotCapacity returns the snapshot total for policyID and whether any contributor's
// capacity is unknown. A contributor with validaggregatedsnapshotsize=false (CNS reported -1)
// is counted at its last known size, or zero if none, matching the previous delta behaviour.
// Malformed inventory still returns an error; the caller must not publish a sum in that case.
func sumSnapshotCapacity(objs []interface{}, policyID string) (*resource.Quantity, bool, error) {
	total := resource.NewQuantity(0, resource.BinarySI)
	missing := false
	for _, obj := range objs {
		u, ok := obj.(*unstructured.Unstructured)
		if !ok {
			return nil, false, fmt.Errorf("unexpected CnsVolumeInfo cache object %T", obj)
		}
		policy, _, err := unstructured.NestedString(u.Object, "spec", "storagePolicyID")
		if err != nil || policy == "" {
			return nil, false, fmt.Errorf("CnsVolumeInfo %q has no valid storage policy ID", u.GetName())
		}
		if policy != policyID || !cnsvolumeinfo.HasSnapshotInfo(u) {
			continue
		}
		valid, found, err := unstructured.NestedBool(u.Object, "spec", "validaggregatedsnapshotsize")
		if err != nil || !found {
			return nil, false, fmt.Errorf("CnsVolumeInfo %q snapshot capacity flag is missing or malformed",
				u.GetName())
		}
		if !valid {
			missing = true
			if _, hasSize, _ := unstructured.NestedFieldNoCopy(u.Object, "spec", "aggregatedsnapshotsize"); !hasSize {
				continue // No last known size; contributes zero.
			}
		}
		q, err := snapshotCapacityQuantity(u)
		if err != nil {
			return nil, false, fmt.Errorf("CnsVolumeInfo %q has invalid snapshot capacity: %w", u.GetName(), err)
		}
		total.Add(q)
	}
	return total, missing, nil
}

// snapshotCapacityQuantity parses spec.aggregatedsnapshotsize. The CRD declares the field as
// int-or-string, so besides the usual quantity string a bare integer is also valid.
func snapshotCapacityQuantity(u *unstructured.Unstructured) (resource.Quantity, error) {
	raw, found, err := unstructured.NestedFieldNoCopy(u.Object, "spec", "aggregatedsnapshotsize")
	if err != nil || !found {
		return resource.Quantity{}, fmt.Errorf("missing or malformed")
	}
	var q resource.Quantity
	switch v := raw.(type) {
	case string:
		q, err = resource.ParseQuantity(v)
		if err != nil {
			return resource.Quantity{}, fmt.Errorf("%q: %w", v, err)
		}
	case int64:
		q = *resource.NewQuantity(v, resource.BinarySI)
	case int32:
		q = *resource.NewQuantity(int64(v), resource.BinarySI)
	case float64:
		if v != math.Trunc(v) || v > math.MaxInt64 || v < math.MinInt64 {
			return resource.Quantity{}, fmt.Errorf("non-integer value %v", v)
		}
		q = *resource.NewQuantity(int64(v), resource.BinarySI)
	default:
		return resource.Quantity{}, fmt.Errorf("unsupported type %T", raw)
	}
	if q.Sign() < 0 {
		return resource.Quantity{}, fmt.Errorf("negative value %s", q.String())
	}
	return q, nil
}
