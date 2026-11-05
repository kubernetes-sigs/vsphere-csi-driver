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

package clusterstoragepolicyinfo

import (
	"context"
	"fmt"
	"maps"
	"slices"
	"sort"
	"time"

	"github.com/vmware/govmomi/property"
	"github.com/vmware/govmomi/vim25"
	"github.com/vmware/govmomi/vim25/mo"
	vimtypes "github.com/vmware/govmomi/vim25/types"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	apitypes "k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/util/retry"
	"sigs.k8s.io/controller-runtime/pkg/client"

	infraspiv1alpha1 "sigs.k8s.io/vsphere-csi-driver/v3/pkg/apis/cnsoperator/infrastoragepolicyinfo/v1alpha1"
	cnsvsphere "sigs.k8s.io/vsphere-csi-driver/v3/pkg/common/cns-lib/vsphere"
	"sigs.k8s.io/vsphere-csi-driver/v3/pkg/csi/service/logger"
	"sigs.k8s.io/vsphere-csi-driver/v3/pkg/syncer/cnsoperator/util"
	"sigs.k8s.io/vsphere-csi-driver/v3/pkg/syncer/cnsoperator/vsphereinfra"
)

const (
	// capacitySyncInterval is how often the per-zone capacity of every InfraStoragePolicyInfo is
	// refreshed from vCenter.
	capacitySyncInterval = time.Hour
	// capacitySyncLogPrefix namespaces capacity-sync log lines.
	capacitySyncLogPrefix = "InfraStoragePolicyInfo capacity"
)

// datastoreSummary is the subset of a datastore's summary that capacity is computed from.
type datastoreSummary struct {
	capacity  int64
	freeSpace int64
	// usable is false for datastores that are inaccessible or in maintenance mode; they do not
	// contribute to capacity, since their free space can't actually be used.
	usable bool
}

// newDatastoreSummary converts a vCenter datastore summary. The check is type-neutral: it only
// looks at accessibility and maintenance mode, never at VMFS/vSAN specifics.
func newDatastoreSummary(s vimtypes.DatastoreSummary) datastoreSummary {
	// MaintenanceMode is unset for datastores that don't support it; treat that as normal.
	inMaintenance := s.MaintenanceMode != "" &&
		s.MaintenanceMode != string(vimtypes.DatastoreSummaryMaintenanceModeStateNormal)
	return datastoreSummary{
		capacity:  s.Capacity,
		freeSpace: s.FreeSpace,
		usable:    s.Accessible && !inMaintenance,
	}
}

// computeZoneCapacity sums capacity and free space per zone over the usable datastores in
// zoneDatastores (zone -> datastore moref set). A datastore shared by several zones is counted in
// each of them, so per-zone totals are right but must not be added up across zones. Values are
// bytes, in decimal SI form. Zones with no usable datastore, including datastores missing
// from summaries, are omitted. The result is sorted by zone.
func computeZoneCapacity(zoneDatastores map[string]map[string]struct{},
	summaries map[string]datastoreSummary) []infraspiv1alpha1.ZoneCapacity {
	result := make([]infraspiv1alpha1.ZoneCapacity, 0, len(zoneDatastores))
	for zone, dsIDs := range zoneDatastores {
		var capacity, free int64
		usable := 0
		for dsID := range dsIDs {
			s, ok := summaries[dsID]
			if !ok || !s.usable {
				continue
			}
			capacity += s.capacity
			free += s.freeSpace
			usable++
		}
		if usable == 0 {
			continue
		}
		result = append(result, infraspiv1alpha1.ZoneCapacity{
			Zone:      zone,
			Capacity:  *resource.NewQuantity(capacity, resource.DecimalSI),
			FreeSpace: *resource.NewQuantity(free, resource.DecimalSI),
		})
	}
	sort.Slice(result, func(i, j int) bool { return result[i].Zone < result[j].Zone })
	return result
}

// retrieveDatastoreSummaries reads the summary of every datastore in dsIDs in one call. dsIDs may
// contain duplicates (a datastore shared by several zones or policies); each datastore is read once.
// Any error, including a datastore deleted since the cache was written, fails the whole read, so the
// caller keeps the previous values rather than publishing a partial result.
func retrieveDatastoreSummaries(ctx context.Context, c *vim25.Client,
	dsIDs []string) (map[string]datastoreSummary, error) {
	ids := slices.Compact(slices.Sorted(slices.Values(dsIDs)))
	refs := make([]vimtypes.ManagedObjectReference, 0, len(ids))
	for _, id := range ids {
		refs = append(refs, vimtypes.ManagedObjectReference{Type: "Datastore", Value: id})
	}

	var dss []mo.Datastore
	if err := property.DefaultCollector(c).Retrieve(ctx, refs, []string{"summary"}, &dss); err != nil {
		return nil, fmt.Errorf("failed to read summary of %d datastores: %w", len(refs), err)
	}

	summaries := make(map[string]datastoreSummary, len(dss))
	for _, ds := range dss {
		summaries[ds.Reference().Value] = newDatastoreSummary(ds.Summary)
	}
	return summaries, nil
}

// zoneCapacityForPolicy computes the per-zone capacity of policyName from the cache. found is false
// when the policy has no cache entry yet (not reconciled since startup)
// A policy that is cached with no compatible datastores yields found=true and an empty slice.
func zoneCapacityForPolicy(ctx context.Context, c *vim25.Client, policyName string) (
	zc []infraspiv1alpha1.ZoneCapacity, found bool, err error) {
	zoneDatastores, found := vsphereinfra.GetCache().GetPolicyZoneDatastores(policyName)
	if !found {
		return nil, false, nil
	}
	var datastoreIDs []string
	for _, dsIDs := range zoneDatastores {
		datastoreIDs = slices.AppendSeq(datastoreIDs, maps.Keys(dsIDs))
	}
	var datastoreSummaries map[string]datastoreSummary
	if len(datastoreIDs) > 0 {
		if c == nil {
			return nil, true, fmt.Errorf("no vCenter client available to read datastore sizes for policy %q",
				policyName)
		}
		datastoreSummaries, err = retrieveDatastoreSummaries(ctx, c, datastoreIDs)
		if err != nil {
			return nil, true, err
		}
	}
	return computeZoneCapacity(zoneDatastores, datastoreSummaries), true, nil
}

// populateCapacity refreshes infraSPI.Status.ZoneCapacity/CapacityObservedTime.
// If the policy is not in the cache there is nothing trustworthy to compute from,
// so it returns nil and leaves the published values alone.
func (r *ReconcileClusterStoragePolicyInfo) populateCapacity(ctx context.Context,
	infraSPI *infraspiv1alpha1.InfraStoragePolicyInfo, vc *cnsvsphere.VirtualCenter) error {
	log := logger.GetLogger(ctx)
	zc, found, err := zoneCapacityForPolicy(ctx, vc.Client.Client, infraSPI.Name)
	if !found {
		log.Debugf("%s: policy %q is not in the cache, leaving capacity unchanged", capacitySyncLogPrefix,
			infraSPI.Name)
		return nil
	}
	if err != nil {
		return fmt.Errorf("failed to compute capacity for policy %q: %w", infraSPI.Name, err)
	}
	now := metav1.Now()
	infraSPI.Status.ZoneCapacity = zc
	infraSPI.Status.CapacityObservedTime = &now
	return nil
}

// startCapacitySync refreshes the capacity of every InfraStoragePolicyInfo every
// capacitySyncInterval. Like the periodic resync, the first run happens one interval after start,
// not at startup: the reconcile already populates both the cache and capacity for each policy at
// startup. Blocks until ctx is cancelled.
func (r *ReconcileClusterStoragePolicyInfo) startCapacitySync(ctx context.Context) {
	util.RunPeriodicResync(ctx, capacitySyncLogPrefix, capacitySyncInterval,
		int(capacitySyncInterval/time.Minute), r.syncAllCapacity)
}

// policyZoneDatastores is a cached policy together with its zone -> datastore moref set.
type policyZoneDatastores struct {
	policyName     string
	zoneDatastores map[string]map[string]struct{}
}

// syncAllCapacity reads the size of every datastore referenced by the cache in one batched call,
// and publishes per-zone capacity to each cached InfraStoragePolicyInfo. If vCenter can't be read,
// nothing is written: the previous values and capacityObservedTime remain, showing their age.
func (r *ReconcileClusterStoragePolicyInfo) syncAllCapacity(ctx context.Context) {
	log := logger.GetLogger(ctx)

	var list infraspiv1alpha1.InfraStoragePolicyInfoList
	if err := r.client.List(ctx, &list); err != nil {
		log.Errorf("%s sync: failed to list InfraStoragePolicyInfo: %v", capacitySyncLogPrefix, err)
		return
	}

	// Snapshot the cache for every policy, and the union of their datastores.
	policies := make([]policyZoneDatastores, 0, len(list.Items))
	var allDS []string
	for i := range list.Items {
		name := list.Items[i].Name
		zoneDatastores, found := vsphereinfra.GetCache().GetPolicyZoneDatastores(name)
		if !found {
			// Not reconciled since startup: nothing trustworthy to publish yet.
			log.Debugf("%s sync: policy %q is not in the cache yet, skipping", capacitySyncLogPrefix, name)
			continue
		}
		policies = append(policies, policyZoneDatastores{policyName: name, zoneDatastores: zoneDatastores})
		for _, dsIDs := range zoneDatastores {
			allDS = slices.AppendSeq(allDS, maps.Keys(dsIDs))
		}
	}
	if len(policies) == 0 {
		log.Infof("%s sync: no cached policies, nothing to do", capacitySyncLogPrefix)
		return
	}

	vcconfig, err := cnsvsphere.GetVirtualCenterConfig(ctx, r.configInfo.GetCfg())
	var vc *cnsvsphere.VirtualCenter
	if err == nil {
		vc, err = cnsvsphere.GetVirtualCenterInstanceForVCenterConfig(ctx, vcconfig, false)
	}
	if err != nil {
		log.Errorf("%s sync: failed to get vCenter instance, keeping previous values: %v",
			capacitySyncLogPrefix, err)
		return
	}
	summaries, err := retrieveDatastoreSummaries(ctx, vc.Client.Client, allDS)
	if err != nil {
		log.Errorf("%s sync: failed to read datastore sizes, keeping previous values: %v",
			capacitySyncLogPrefix, err)
		return
	}

	observed := metav1.Now()
	published := 0
	for _, p := range policies {
		zc := computeZoneCapacity(p.zoneDatastores, summaries)
		if err := r.publishCapacity(ctx, p.policyName, zc, observed); err != nil {
			log.Errorf("%s sync: failed to publish capacity for policy %q: %v", capacitySyncLogPrefix,
				p.policyName, err)
			continue
		}
		published++
	}
	log.Infof("%s sync: published capacity for %d/%d policies from %d datastores", capacitySyncLogPrefix,
		published, len(policies), len(summaries))
}

// publishCapacity writes only zoneCapacity and capacityObservedTime to the InfraStoragePolicyInfo
// status with a merge patch, so it can't clobber the topology/volumeCapabilities/error fields the
// reconcile owns. The reconcile also writes capacity, so a value read earlier must never replace a
// newer one: if the published capacityObservedTime is not older than observed, nothing is written.
func (r *ReconcileClusterStoragePolicyInfo) publishCapacity(ctx context.Context, name string,
	zc []infraspiv1alpha1.ZoneCapacity, observed metav1.Time) error {
	return retry.RetryOnConflict(retry.DefaultRetry, func() error {
		// Read fresh so a policy deleted since the listing isn't resurrected or patched blindly.
		infraSPI := &infraspiv1alpha1.InfraStoragePolicyInfo{}
		if err := r.client.Get(ctx, apitypes.NamespacedName{Name: name}, infraSPI); err != nil {
			if apierrors.IsNotFound(err) {
				return nil
			}
			return err
		}
		if published := infraSPI.Status.CapacityObservedTime; published != nil && !observed.After(published.Time) {
			logger.GetLogger(ctx).Debugf("%s: policy %q already has capacity observed at %s, not older than %s; "+
				"skipping", capacitySyncLogPrefix, name, published.Time, observed.Time)
			return nil
		}
		base := infraSPI.DeepCopy()
		infraSPI.Status.ZoneCapacity = zc
		infraSPI.Status.CapacityObservedTime = &observed
		return r.client.Status().Patch(ctx, infraSPI, client.MergeFromWithOptions(base, client.MergeFromWithOptimisticLock{}))
	})
}
