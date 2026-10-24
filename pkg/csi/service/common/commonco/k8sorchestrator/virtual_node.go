/*
Copyright 2020 The Kubernetes Authors.

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
	"sigs.k8s.io/vsphere-csi-driver/v3/pkg/common/cns-lib/vsphere"
	"sigs.k8s.io/vsphere-csi-driver/v3/pkg/csi/service/common"
	"sigs.k8s.io/vsphere-csi-driver/v3/pkg/csi/service/logger"
	"sigs.k8s.io/vsphere-csi-driver/v3/pkg/internalapis/podvmv2"
)

// hostInventory resolves which Kubernetes Node currently owns each physical
// ESXi host. Dedicated ownership comes from core Node annotations; VirtualNode
// ownership comes from the platform's HostMapping resource, which publishes
// the hosts a VirtualNode owns rather than raw vCenter cluster membership.
//
// Live reads avoid treating an unsynchronized informer as evidence of
// ownership. No previous mapping is reused after a lookup fails, and an
// unresolved host is never published rather than guessed.
type hostInventory struct {
	client       kubernetes.Interface
	hostMappings func(context.Context) ([]podvmv2.HostMapping, error)
	vcenterUUID  func(context.Context) (string, error)
}

var (
	dynamicClientOnce sync.Once
	dynamicClient     dynamic.Interface
	dynamicClientErr  error
)

// sharedDynamicClient builds the client once: an ownership snapshot runs on
// every ListVolumes, and rebuilding a client per call is pure overhead.
func sharedDynamicClient() (dynamic.Interface, error) {
	dynamicClientOnce.Do(func() {
		cfg, err := config.GetConfig()
		if err != nil {
			dynamicClientErr = err
			return
		}
		dynamicClient, dynamicClientErr = dynamic.NewForConfig(cfg)
	})
	return dynamicClient, dynamicClientErr
}

func listHostMappings(ctx context.Context) ([]podvmv2.HostMapping, error) {
	client, err := sharedDynamicClient()
	if err != nil {
		return nil, err
	}
	return podvmv2.ListHostMappings(ctx, client)
}

// configuredVCenterUUID resolves the vCenter lazily: its client may not be
// registered yet when the orchestrator initializes.
func configuredVCenterUUID(ctx context.Context) (string, error) {
	return vcenterUUIDFrom(ctx, vsphere.GetVirtualCenterManager(ctx).GetAllVirtualCenters())
}

func vcenterUUIDFrom(ctx context.Context, vcs []*vsphere.VirtualCenter) (string, error) {
	// Supervisor host topology uses bare MoIDs. They are unique within a
	// vCenter but not across vCenters, so a second configured vCenter could
	// contribute a colliding host ID to this namespace. Refuse to resolve
	// VirtualNode ownership until host identity is vCenter-qualified.
	if len(vcs) != 1 {
		return "", fmt.Errorf("host ownership requires exactly one configured vCenter, found %d", len(vcs))
	}
	vc := vcs[0]
	if err := vc.Connect(ctx); err != nil {
		return "", err
	}
	uuid := vc.Client.ServiceContent.About.InstanceUuid
	if uuid == "" {
		return "", fmt.Errorf("configured vCenter reports no instance UUID")
	}
	return uuid, nil
}

func (h *hostInventory) snapshot(ctx context.Context) map[string]string {
	ctx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	log := logger.GetLogger(ctx)
	owners := make(map[string]string)
	nodes, err := h.client.CoreV1().Nodes().List(ctx, metav1.ListOptions{})
	if err != nil {
		log.Errorf("Cannot list Nodes for physical host ownership: %v", err)
		return owners
	}
	conflicts := make(map[string]bool)
	reserved := make(map[string]bool)
	add := func(host, node string) {
		if host == "" {
			return
		}
		if previous, exists := owners[host]; exists && previous != node {
			conflicts[host] = true
		}
		owners[host] = node
	}
	var virtualNodes []*corev1.Node
	for i := range nodes.Items {
		node := &nodes.Items[i]
		if podvmv2.IsVirtualNode(node) {
			// A deleting VirtualNode is not published. Zone must be defined
			// before its hosts can carry usable topology.
			if node.DeletionTimestamp == nil && node.Labels[corev1.LabelTopologyZone] != "" {
				virtualNodes = append(virtualNodes, node)
			}
			continue // Ignore any legacy masqueraded single-host annotation.
		}
		host := node.Annotations[common.HostMoidAnnotationKey]
		if host == "" {
			continue
		}
		reserved[host] = true
		if podvmv2.IsDedicatedHostNode(node) && node.DeletionTimestamp == nil {
			add(host, node.Name)
		} else {
			// Neither an unsupported node type nor a terminating Node can be
			// interpreted as evidence that this host has joined a VirtualNode.
			conflicts[host] = true
		}
	}
	if len(virtualNodes) > 0 {
		h.addVirtualNodeHosts(ctx, virtualNodes, reserved, conflicts, add)
	}
	for host := range conflicts {
		delete(owners, host)
	}
	return owners
}

// addVirtualNodeHosts publishes each VirtualNode's owned hosts from its
// HostMapping. The platform scopes that list to hosts without a dedicated
// Node, so CSI performs no handoff inference of its own.
func (h *hostInventory) addVirtualNodeHosts(ctx context.Context, virtualNodes []*corev1.Node,
	reserved, conflicts map[string]bool, add func(host, node string)) {
	log := logger.GetLogger(ctx)
	vcUUID, err := h.vcenterUUID(ctx)
	if err != nil {
		log.Errorf("Cannot resolve the configured vCenter for VirtualNode host ownership: %v", err)
		return
	}
	mappings, err := h.hostMappings(ctx)
	if err != nil {
		// Dedicated owners resolved above remain valid; only VirtualNode hosts
		// are withheld. No previous mapping is reused.
		log.Errorf("Cannot list HostMapping resources: %v", err)
		return
	}
	byName := make(map[string]*podvmv2.HostMapping, len(mappings))
	for i := range mappings {
		byName[mappings[i].Name] = &mappings[i]
	}
	type clusterIdentity struct{ vcUUID, clusterMoID string }
	valid := make(map[string]*podvmv2.HostMapping, len(virtualNodes))
	claims := make(map[clusterIdentity][]string)
	for _, node := range virtualNodes {
		mapping := byName[node.Name]
		if mapping == nil {
			// Normal while a VirtualNode exists but its mapping has not been
			// published yet. Publish nothing for it rather than guessing.
			log.Infof("VirtualNode %q has no HostMapping yet; publishing no hosts for it", node.Name)
			continue
		}
		if err := mapping.ValidateFor(node, vcUUID); err != nil {
			log.Errorf("Ignoring HostMapping for VirtualNode %q: %v", node.Name, err)
			continue
		}
		vc, cluster := mapping.ClusterIdentity()
		identity := clusterIdentity{vc, cluster}
		claims[identity] = append(claims[identity], node.Name)
		valid[node.Name] = mapping
	}
	for _, node := range virtualNodes {
		mapping := valid[node.Name]
		if mapping == nil {
			continue
		}
		vc, cluster := mapping.ClusterIdentity()
		if owners := claims[clusterIdentity{vc, cluster}]; len(owners) != 1 {
			log.Errorf("Cluster %q is claimed by %d VirtualNodes %v; publishing no hosts for it",
				cluster, len(owners), owners)
			continue
		}
		hosts := mapping.HostMoIDs()
		if len(hosts) == 0 {
			// Expected before the first ESXi host upgrade: every host still has
			// a dedicated Node, so this VirtualNode owns none yet.
			log.Debugf("VirtualNode %q currently owns no hosts", node.Name)
			continue
		}
		for _, host := range hosts {
			if reserved[host] {
				// The platform scopes spec.hosts to hosts without a dedicated
				// Node, so an overlap means the mapping and live Node state
				// disagree: producer lag, a reverse transition or a bug.
				log.Errorf("Host %q is published by VirtualNode %q but is still held by a dedicated Node; "+
					"publishing no owner for it", host, node.Name)
				conflicts[host] = true
				continue
			}
			add(host, node.Name)
		}
	}
}
