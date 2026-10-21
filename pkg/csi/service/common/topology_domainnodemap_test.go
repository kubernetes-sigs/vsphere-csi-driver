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

package common

import (
	"context"
	"fmt"
	"sync"
	"testing"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"sigs.k8s.io/vsphere-csi-driver/v3/pkg/csi/service/logger"
	csinodetopologyv1alpha1 "sigs.k8s.io/vsphere-csi-driver/v3/pkg/internalapis/csinodetopology/v1alpha1"
)

// TestVerifyAllNodesInTopologyAccessibleToDatastoreConcurrent exercises
// VerifyAllNodesInTopologyAccessibleToDatastore concurrently with node
// additions/removals. Run with -race: without the read lock this fails with a
// data race (or "concurrent map iteration and map write").
func TestVerifyAllNodesInTopologyAccessibleToDatastoreConcurrent(t *testing.T) {
	ctx := context.Background()
	// Initialise the lazily-created global logger up front so the race detector
	// only reports on domainNodeMap access.
	_ = logger.GetLogger(ctx)
	// Use a fresh domainNodeMap so this test doesn't leak state into others.
	origDomainNodeMap := domainNodeMap
	domainNodeMap = make(map[string]map[string]struct{})
	t.Cleanup(func() { domainNodeMap = origDomainNodeMap })
	segments := []map[string]string{{"zone": "z1", "region": "r1"}}
	nodeNames := []string{"node-0"}

	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		defer wg.Done()
		for i := 0; i < 500; i++ {
			node := csinodetopologyv1alpha1.CSINodeTopology{
				ObjectMeta: metav1.ObjectMeta{Name: fmt.Sprintf("node-%d", i%50)},
				Status: csinodetopologyv1alpha1.CSINodeTopologyStatus{
					TopologyLabels: []csinodetopologyv1alpha1.TopologyLabel{
						{Key: "zone", Value: "z1"}, {Key: "region", Value: "r1"},
					},
				},
			}
			AddNodeToDomainNodeMapNew(ctx, node)
			RemoveNodeFromDomainNodeMapNew(ctx, node)
		}
	}()
	go func() {
		defer wg.Done()
		for i := 0; i < 500; i++ {
			_, err := VerifyAllNodesInTopologyAccessibleToDatastore(ctx, nodeNames, "ds:///vmfs/volumes/ds1/", segments)
			if err != nil {
				t.Errorf("unexpected error: %v", err)
				return
			}
		}
	}()
	wg.Wait()
}
