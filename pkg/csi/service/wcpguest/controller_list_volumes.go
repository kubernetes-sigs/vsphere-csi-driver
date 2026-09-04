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

package wcpguest

import (
	"context"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/container-storage-interface/spec/lib/go/csi"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	csifault "sigs.k8s.io/vsphere-csi-driver/v3/pkg/common/fault"
	"sigs.k8s.io/vsphere-csi-driver/v3/pkg/common/prometheus"
	"sigs.k8s.io/vsphere-csi-driver/v3/pkg/common/utils"
	"sigs.k8s.io/vsphere-csi-driver/v3/pkg/csi/service/common"
	"sigs.k8s.io/vsphere-csi-driver/v3/pkg/csi/service/common/commonco"
	"sigs.k8s.io/vsphere-csi-driver/v3/pkg/csi/service/logger"
	csitypes "sigs.k8s.io/vsphere-csi-driver/v3/pkg/csi/types"
)

var (
	// listVolumesListTimeout bounds each LIST issued while the pagination lock is held.
	// Without a deadline, a hung API server would block every subsequent ListVolumes call
	// behind it. Declared as a var so it can be shortened in tests.
	listVolumesListTimeout = 30 * time.Second

	// listVolumesGeneration is bumped once per rebuild, so successive caches never share a
	// token namespace. Guarded by controller.listVolumesMu, same as the cache itself.
	listVolumesGeneration int64
)

// ownedVolume represents a single entry in the ownership set, built by scanning guest
// PersistentVolumes. Only volume handles that make it into this set are eligible to appear in
// the response.
//
// Block volumes only: file volumes are filtered out earlier, in listOwnedVolumes, rather than
// being included and marked unpublished. That's because file volumes never show up in
// VirtualMachine.Status.Volumes, so treating them as unpublished would incorrectly imply
// they'd been detached.
type ownedVolume struct {
	capacityBytes int64
	// nodes accumulates published node names found by the block pass, deduplicated and
	// sorted at flatten time.
	nodes []string
}

// listVolumesCache is built once and served page by page for one pagination sequence.
// Guarded by controller.listVolumesMu. A rebuild always creates a brand new cache and swaps it
// in for controller.listVolumesCache; it never edits the old cache's data in place. That's
// because a response we already sent back may still be holding onto (i.e. sharing the same
// underlying array as) that old cache's PublishedNodeIds slice, even after we've released the
// lock. Editing the old cache in place could corrupt data a caller is still reading.
type listVolumesCache struct {
	// generation is embedded in every token so a token minted against a superseded cache
	// can't collide with a coincidentally equal cursor in the current one.
	generation int64
	entries    []*csi.ListVolumesResponse_Entry
	// cursor is the only starting_token value this cache will accept.
	cursor int
}

// ListVolumes implements the CSI ListVolumes RPC for the guest (pvCSI) flavor, for block
// volumes only. It queries neither CNS nor vCenter for this RPC, reading instead the same
// Kubernetes objects the attach path already trusts.
func (c *controller) ListVolumes(ctx context.Context, req *csi.ListVolumesRequest) (
	*csi.ListVolumesResponse, error) {

	ctx = logger.NewContextWithLogger(ctx)
	log := logger.GetLogger(ctx)
	log.Infof("ListVolumes: called with args %+v", req)

	if !commonco.ContainerOrchestratorUtility.IsFSSEnabled(ctx, common.ListVolumes) {
		return nil, status.Error(codes.Unimplemented, "list volumes FSS disabled")
	}

	start := time.Now()
	volumeType := prometheus.PrometheusBlockVolumeType
	resp, faultType, err := c.listVolumesInternal(ctx, req)
	if err != nil {
		if csifault.IsNonStorageFault(faultType) {
			faultType = csifault.AddCsiNonStoragePrefix(ctx, faultType)
		}
		log.Errorf("Operation failed, reporting failure status to Prometheus."+
			" Operation Type: %q, Volume Type: %q, Fault Type: %q",
			prometheus.PrometheusListVolumeOpType, volumeType, faultType)
		prometheus.CsiControlOpsHistVec.WithLabelValues(volumeType, prometheus.PrometheusListVolumeOpType,
			prometheus.PrometheusFailStatus, faultType).Observe(time.Since(start).Seconds())
	} else {
		prometheus.CsiControlOpsHistVec.WithLabelValues(volumeType, prometheus.PrometheusListVolumeOpType,
			prometheus.PrometheusPassStatus, faultType).Observe(time.Since(start).Seconds())
	}
	return resp, err
}

// listVolumesInternal holds the pagination lock for the whole check, build and page
// sequence, so exactly one caller is ever inside it at a time.
func (c *controller) listVolumesInternal(ctx context.Context, req *csi.ListVolumesRequest) (
	*csi.ListVolumesResponse, string, error) {
	log := logger.GetLogger(ctx)

	if req.MaxEntries < 0 {
		return nil, csifault.CSIInvalidArgumentFault, status.Error(codes.InvalidArgument,
			"MaxEntries must not be negative")
	}

	c.listVolumesMu.Lock()
	defer c.listVolumesMu.Unlock()

	startIdx, aborted := parseListVolumesToken(req.StartingToken, c.listVolumesCache)
	if aborted {
		log.Warnf("ListVolumes: starting token %q does not continue any in-progress listing, "+
			"discarding cache and returning ABORTED", req.StartingToken)
		c.listVolumesCache = nil
		return nil, csifault.CSIInternalFault, status.Error(codes.Aborted,
			"starting_token does not continue the current listing; restart with an empty starting_token")
	}

	if req.StartingToken == "" {
		cache, faultType, err := c.buildListVolumesCache(ctx)
		if err != nil {
			return nil, faultType, err
		}
		c.listVolumesCache = cache
		startIdx = 0
	}

	cache := c.listVolumesCache
	entries := cache.entries
	end := len(entries)
	if req.MaxEntries > 0 && startIdx+int(req.MaxEntries) < end {
		end = startIdx + int(req.MaxEntries)
	}

	page := entries[startIdx:end]
	resp := &csi.ListVolumesResponse{Entries: page}
	if end < len(entries) {
		cache.cursor = end
		resp.NextToken = listVolumesToken(cache.generation, end)
	} else {
		// Discard the cache on the final page, so that a replayed token is rejected as
		// non-continuing rather than served a stale page.
		c.listVolumesCache = nil
	}
	return resp, "", nil
}

// parseListVolumesToken implements the token state machine. An empty token starts a new
// sequence. Any other value must exactly equal the cache's cursor, or the listing is
// aborted; it never rebuilds and pages from the supplied index, since that would silently
// skip or duplicate entries.
func parseListVolumesToken(token string, cache *listVolumesCache) (idx int, aborted bool) {
	if token == "" {
		return 0, false
	}
	gen, idx, err := splitListVolumesToken(token)
	if err != nil || idx < 0 {
		return 0, true
	}
	if cache == nil || cache.generation != gen || cache.cursor != idx {
		return 0, true
	}
	return idx, false
}

func listVolumesToken(generation int64, idx int) string {
	return fmt.Sprintf("%d:%d", generation, idx)
}

func splitListVolumesToken(token string) (generation int64, idx int, err error) {
	parts := strings.SplitN(token, ":", 2)
	if len(parts) != 2 {
		return 0, 0, fmt.Errorf("malformed starting_token %q", token)
	}
	gen, err := strconv.ParseInt(parts[0], 10, 64)
	if err != nil {
		return 0, 0, err
	}
	i, err := strconv.Atoi(parts[1])
	if err != nil {
		return 0, 0, err
	}
	return gen, i, nil
}

// buildListVolumesCache runs the ownership and block passes for block volumes and returns a
// freshly allocated cache. Never mutates any previous cache.
func (c *controller) buildListVolumesCache(ctx context.Context) (*listVolumesCache, string, error) {
	log := logger.GetLogger(ctx)

	owned, faultType, err := c.listOwnedVolumes(ctx)
	if err != nil {
		return nil, faultType, err
	}

	vmCount, faultType, err := c.addPublishedNodesFromVMs(ctx, owned)
	if err != nil {
		return nil, faultType, err
	}

	// A healthy cluster that owns volumes necessarily has nodes, so an empty VirtualMachine
	// list here would be reported as an empty response, which reads to external-attacher as
	// everything being detached and would force-sync every owned volume. Fail instead.
	if vmCount == 0 && len(owned) > 0 {
		return nil, csifault.CSIInternalFault, status.Error(codes.FailedPrecondition,
			"no VirtualMachine objects found in the Supervisor namespace while this cluster owns "+
				"provisioned volumes; refusing to report an empty listing")
	}

	entries, pairCount := flattenListVolumesEntries(owned)

	// Unlike vanilla and wcp, there's no guard here against a drop in published-pair count:
	// those flavors compare a rebuild against a threshold from the same pass and self-heal on
	// the next call. Here, both lists are either read to completion or the whole rebuild fails,
	// so a successful rebuild is never partial. Adding a guard would only ever latch onto a
	// real drop (e.g. a scale-down) and never recover.

	listVolumesGeneration++
	log.Infof("ListVolumes: rebuilt cache generation %d: owned=%d vms=%d entries=%d publishedPairs=%d",
		listVolumesGeneration, len(owned), vmCount, len(entries), pairCount)

	return &listVolumesCache{
		generation: listVolumesGeneration,
		entries:    entries,
		cursor:     0,
	}, "", nil
}

// listOwnedVolumes scans guest PersistentVolumes to build the ownership set, seeding one entry
// per owned block handle with its capacity. File volumes are filtered out here rather than
// admitted and failed later. Since nothing outside this set can ever enter the response,
// a foreign cluster's volumes are excluded automatically, without needing to know which VMs
// belong to which cluster.
func (c *controller) listOwnedVolumes(ctx context.Context) (
	owned map[string]*ownedVolume, faultType string, err error) {
	log := logger.GetLogger(ctx)
	listCtx, cancel := context.WithTimeout(ctx, listVolumesListTimeout)
	defer cancel()

	owned = make(map[string]*ownedVolume)
	excludedFileVolumes := 0
	continueToken := ""
	for {
		pvList, err := c.guestClient.CoreV1().PersistentVolumes().List(listCtx, metav1.ListOptions{
			Limit:    500,
			Continue: continueToken,
		})
		if err != nil {
			msg := fmt.Sprintf("failed to list guest PersistentVolumes: %v", err)
			log.Error(msg)
			return nil, csifault.CSIInternalFault, status.Error(codes.Internal, msg)
		}
		for i := range pvList.Items {
			pv := &pvList.Items[i]
			if pv.Spec.CSI == nil || pv.Spec.CSI.Driver != csitypes.Name || pv.Spec.CSI.VolumeHandle == "" {
				continue
			}
			if isFileVolumePV(pv) {
				excludedFileVolumes++
				continue
			}
			handle := pv.Spec.CSI.VolumeHandle
			if _, exists := owned[handle]; exists {
				log.Warnf("ListVolumes: volume handle %q owned by more than one guest PersistentVolume; "+
					"keeping the first one seen", handle)
				continue
			}
			capacity := pv.Spec.Capacity[corev1.ResourceStorage]
			owned[handle] = &ownedVolume{
				capacityBytes: capacity.Value(),
			}
		}
		continueToken = pvList.Continue
		if continueToken == "" {
			break
		}
	}
	if excludedFileVolumes > 0 {
		log.Infof("ListVolumes: excluded %d file volume(s); this increment reports block volumes only",
			excludedFileVolumes)
	}
	return owned, "", nil
}

// isFileVolumePV checks for a ReadWriteMany or ReadOnlyMany access mode, the same test
// ControllerUnpublishVolume and DeleteVolume use to distinguish block from file volumes.
func isFileVolumePV(pv *corev1.PersistentVolume) bool {
	for _, mode := range pv.Spec.AccessModes {
		if mode == corev1.ReadWriteMany || mode == corev1.ReadOnlyMany {
			return true
		}
	}
	return false
}

// addPublishedNodesFromVMs lists Supervisor VirtualMachines and, for each Status.Volumes
// entry satisfying Attached && DiskUUID != "" (the same predicate ControllerPublishVolume
// waits on), adds the VM name to that volume's node set. Returns the VM count, used by the
// empty-list guard in buildListVolumesCache.
func (c *controller) addPublishedNodesFromVMs(ctx context.Context, owned map[string]*ownedVolume) (
	int, string, error) {
	log := logger.GetLogger(ctx)
	listCtx, cancel := context.WithTimeout(ctx, listVolumesListTimeout)
	defer cancel()

	vmList, err := utils.ListVirtualMachines(listCtx, c.vmOperatorClient, c.supervisorNamespace)
	if err != nil {
		msg := fmt.Sprintf("failed to list Supervisor VirtualMachines in namespace %q: %v",
			c.supervisorNamespace, err)
		log.Error(msg)
		return 0, csifault.CSIInternalFault, status.Error(codes.Internal, msg)
	}

	seenNotOwned := 0
	for i := range vmList.Items {
		vm := &vmList.Items[i]
		for _, vs := range vm.Status.Volumes {
			if !(vs.Attached && vs.DiskUUID != "") {
				continue
			}
			// vm-operator appends ":detaching" to the volume name while a detach is in
			// flight. Strip it and report the volume as published rather than skipping it,
			// since reporting it unpublished would race a fresh attach against the detach
			// already underway.
			name := removeDetachingSuffixFromVolumeName(vs.Name)
			ov, ok := owned[name]
			if !ok {
				seenNotOwned++
				continue
			}
			ov.nodes = appendUniqueNode(ov.nodes, vm.Name)
		}
	}
	if seenNotOwned > 0 {
		log.Infof("ListVolumes: saw %d attached volume(s) on Supervisor VirtualMachines that this "+
			"cluster does not own (foreign cluster or InstanceVolumeClaim volumes); excluded", seenNotOwned)
	}
	return len(vmList.Items), "", nil
}

// appendUniqueNode appends nodeName if not already present. This avoids double-counting a
// volume seen as both "foo" and "foo:detaching" on the same VM.
func appendUniqueNode(nodes []string, nodeName string) []string {
	for _, n := range nodes {
		if n == nodeName {
			return nodes
		}
	}
	return append(nodes, nodeName)
}

// flattenListVolumesEntries sorts handles and, within each, node IDs, so page boundaries are
// deterministic across rebuilds since map iteration order is not. Status is always non-nil,
// even when PublishedNodeIds is empty, because the attacher's lister silently drops any
// entry whose Status is nil. Returns the entries plus the total published pair count, logged
// by buildListVolumesCache.
func flattenListVolumesEntries(owned map[string]*ownedVolume) ([]*csi.ListVolumesResponse_Entry, int) {
	handles := make([]string, 0, len(owned))
	for h := range owned {
		handles = append(handles, h)
	}
	sort.Strings(handles)

	entries := make([]*csi.ListVolumesResponse_Entry, 0, len(handles))
	pairCount := 0
	for _, h := range handles {
		ov := owned[h]
		nodes := append([]string(nil), ov.nodes...)
		sort.Strings(nodes)
		pairCount += len(nodes)
		entries = append(entries, &csi.ListVolumesResponse_Entry{
			Volume: &csi.Volume{
				VolumeId:      h,
				CapacityBytes: ov.capacityBytes,
			},
			Status: &csi.ListVolumesResponse_VolumeStatus{
				PublishedNodeIds: nodes,
			},
		})
	}
	return entries, pairCount
}
