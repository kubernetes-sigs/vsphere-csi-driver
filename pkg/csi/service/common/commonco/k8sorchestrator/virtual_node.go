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

package k8sorchestrator

import (
	"context"
	"fmt"
	"sync"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/kubernetes"
	"sigs.k8s.io/controller-runtime/pkg/client/config"
	"sigs.k8s.io/vsphere-csi-driver/v3/pkg/csi/service/common"
	"sigs.k8s.io/vsphere-csi-driver/v3/pkg/csi/service/logger"
	"sigs.k8s.io/vsphere-csi-driver/v3/pkg/internalapis/podvmv2"
)

// hostInventory resolves which Kubernetes Node currently owns each physical
// ESXi host. Dedicated ownership comes from core Node annotations; VirtualNode
// ownership comes from the platform's NodeMapping resource, which publishes
// the hosts a VirtualNode represents rather than raw vCenter cluster membership.
//
// A Node of either kind owns its hosts until the object is deleted. A
// terminating Node has not gone away: it still exists and the VolumeAttachments
// on its hosts still name it, so withholding its hosts would read as a detach.
//
// Host identity is a bare MoID, unique within one vCenter but not across
// vCenters, and NodeMapping does not name its vCenter. That is unambiguous
// because a Supervisor is backed by a single vCenter: GetVirtualCenterConfig
// takes the first configured entry and wcp/controller.go registers only that
// one, so CSI resolves every MoID against it. Supporting several vCenters here
// would need vCenter-qualified host identity from the platform.
//
// Live reads avoid treating an unsynchronized informer as evidence of
// ownership. No previous mapping is reused after a lookup fails, and an
// unresolved host is never published rather than guessed.
type hostInventory struct {
	client       kubernetes.Interface
	nodeMappings func(context.Context) ([]podvmv2.NodeMapping, error)
}

var (
	dynamicClientMu sync.Mutex
	dynamicClient   dynamic.Interface
	// newDynamicClient builds the NodeMapping client. It is a variable so tests
	// can exercise sharedDynamicClient without an API server.
	newDynamicClient = func() (dynamic.Interface, error) {
		cfg, err := config.GetConfig()
		if err != nil {
			return nil, err
		}
		return dynamic.NewForConfig(cfg)
	}
)

// sharedDynamicClient builds the client once and reuses it: an ownership
// snapshot runs on every ListVolumes, and rebuilding a client per call is pure
// overhead.
//
// Only a successful client is cached. A failure is returned without being
// remembered, so a config or client build that fails at first use, such as a
// service account token not yet projected into the container, does not disable
// every later snapshot until the container restarts.
func sharedDynamicClient() (dynamic.Interface, error) {
	dynamicClientMu.Lock()
	defer dynamicClientMu.Unlock()
	if dynamicClient != nil {
		return dynamicClient, nil
	}
	client, err := newDynamicClient()
	if err != nil {
		return nil, err
	}
	dynamicClient = client
	return dynamicClient, nil
}

func listNodeMappings(ctx context.Context) ([]podvmv2.NodeMapping, error) {
	client, err := sharedDynamicClient()
	if err != nil {
		return nil, err
	}
	return podvmv2.ListNodeMappings(ctx, client)
}

// snapshot resolves current host ownership. An error means the view is incomplete, and every
// caller must fail rather than publish what was resolved: a host that resolves to no Node looks
// exactly like a host holding no volumes, which ListVolumes would report as detached.
func (h *hostInventory) snapshot(ctx context.Context) (map[string]string, error) {
	ctx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	log := logger.GetLogger(ctx)
	owners := make(map[string]string)
	nodes, err := h.client.CoreV1().Nodes().List(ctx, metav1.ListOptions{})
	if err != nil {
		log.Errorf("Cannot list Nodes for physical host ownership: %v", err)
		return nil, fmt.Errorf("cannot list Nodes for physical host ownership: %w", err)
	}
	conflicts := make(map[string]string)
	reserved := make(map[string]string)
	add := func(host, node string) {
		if host == "" {
			return
		}
		if previous, exists := owners[host]; exists && previous != node {
			conflicts[host] = fmt.Sprintf("claimed by Nodes %q and %q", previous, node)
		}
		owners[host] = node
	}
	var virtualNodes []*corev1.Node
	for i := range nodes.Items {
		node := &nodes.Items[i]
		if podvmv2.IsVirtualNode(node) {
			// A deleting VirtualNode still owns its hosts until it is gone.
			// Zone must be defined before its hosts can carry usable topology.
			if node.Labels[corev1.LabelTopologyZone] != "" {
				virtualNodes = append(virtualNodes, node)
			}
			continue // Ignore any legacy masqueraded single-host annotation.
		}
		host := node.Annotations[common.HostMoidAnnotationKey]
		if host == "" {
			continue
		}
		reserved[host] = node.Name
		if podvmv2.IsDedicatedHostNode(node) {
			add(host, node.Name)
		} else {
			// An unsupported node type is never read as an ESXi host Node, and
			// nothing else can be said to own the host it names either.
			conflicts[host] = fmt.Sprintf("held by Node %q, whose node type %q is not a supported ESXi host Node",
				node.Name, node.Annotations[podvmv2.NodeTypeAnnotation])
		}
	}
	if len(virtualNodes) > 0 {
		if err := h.addVirtualNodeHosts(ctx, virtualNodes, reserved, conflicts, add); err != nil {
			return nil, err
		}
	}
	for host, reason := range conflicts {
		// Say why once per host. Withholding one drops every volume attached on
		// it from ListVolumes, which a consumer reads as those volumes having
		// been detached, so this must never happen silently.
		log.Errorf("Host %q has no resolvable owning Node and is withheld from the inventory: %s. "+
			"Volumes attached on it are omitted from ListVolumes", host, reason)
		delete(owners, host)
	}
	return owners, nil
}

// addVirtualNodeHosts publishes each VirtualNode's hosts from its NodeMapping.
// The platform scopes that list to hosts without a dedicated Node, so CSI
// performs no handoff inference of its own. A host listed by two VirtualNodes
// gets no owner. A host listed by a VirtualNode that a dedicated Node still
// holds stays with the dedicated Node, which is the live owner until it is
// deleted; dropping both claimants would hide that host's volumes.
//
// A NodeMapping read failure here fails the whole snapshot. Returning the dedicated owners alone
// would be a plausible-looking view of a mixed cluster in which every VirtualNode has silently
// lost its volumes, which is worse than no answer at all.
func (h *hostInventory) addVirtualNodeHosts(ctx context.Context, virtualNodes []*corev1.Node,
	reserved, conflicts map[string]string, add func(host, node string)) error {
	log := logger.GetLogger(ctx)
	mappings, err := h.nodeMappings(ctx)
	if err != nil {
		log.Errorf("Cannot list NodeMapping resources: %v", err)
		return fmt.Errorf("cannot list NodeMapping resources: %w", err)
	}
	byName := make(map[string]*podvmv2.NodeMapping, len(mappings))
	for i := range mappings {
		byName[mappings[i].Name] = &mappings[i]
	}
	for _, node := range virtualNodes {
		mapping := byName[node.Name]
		if mapping == nil {
			// Normal while a VirtualNode exists but its mapping has not been
			// published yet. Publish nothing for it rather than guessing.
			log.Infof("VirtualNode %q has no NodeMapping yet; publishing no hosts for it", node.Name)
			continue
		}
		if err := mapping.ValidateFor(node); err != nil {
			log.Errorf("Ignoring NodeMapping for VirtualNode %q: %v", node.Name, err)
			continue
		}
		hosts := mapping.HostMoIDs()
		if len(hosts) == 0 {
			// Expected before the first ESXi host upgrade: every host still has
			// a dedicated Node, so this VirtualNode represents none yet.
			log.Debugf("VirtualNode %q currently represents no hosts", node.Name)
			continue
		}
		for _, host := range hosts {
			// The platform scopes status.hosts to hosts without a dedicated
			// Node, so an overlap means the mapping and live Node state
			// disagree: producer lag, a reverse transition or a bug.
			if holder, held := reserved[host]; held {
				if _, unresolvable := conflicts[host]; unresolvable {
					log.Errorf("Host %q is published by VirtualNode %q and is also held by Node %q",
						host, node.Name, holder)
				} else {
					log.Errorf("Host %q is published by VirtualNode %q but is still held by dedicated Node %q; "+
						"keeping the dedicated Node as its owner", host, node.Name, holder)
				}
				continue
			}
			// add marks the host as conflicting if another VirtualNode already
			// claimed it in this snapshot.
			add(host, node.Name)
		}
	}
	return nil
}
