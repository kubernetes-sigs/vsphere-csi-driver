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

package commonco

import (
	"context"
	"reflect"
	"testing"

	"sigs.k8s.io/vsphere-csi-driver/v3/pkg/csi/service/common"
)

// ownershipStub implements only the methods GetNodeNameToHostMoIDsMap uses;
// any other call panics through the nil embedded interface.
type ownershipStub struct {
	COCommonInterface
	podVMV2    bool
	hostToNode map[string]string
	nodeToHost map[string]string
	inventory  int
}

func (s *ownershipStub) IsFSSEnabled(_ context.Context, feature string) bool {
	return feature == common.PodVMV2Support && s.podVMV2
}

func (s *ownershipStub) GetNodeIDtoNameMap(context.Context) map[string]string {
	s.inventory++
	return s.hostToNode
}

func (s *ownershipStub) GetNodeNameToHostMoIDMap(context.Context) map[string]string {
	return s.nodeToHost
}

func TestGetNodeNameToHostMoIDsMap(t *testing.T) {
	ctx := context.Background()
	t.Run("PodVM v2 disabled uses the legacy one-host map", func(t *testing.T) {
		s := &ownershipStub{
			nodeToHost: map[string]string{"esx-1": "host-1", "esx-2": "host-2"},
			// Deliberately inconsistent: must not be consulted.
			hostToNode: map[string]string{"host-9": "esx-1"},
		}
		got := GetNodeNameToHostMoIDsMap(ctx, s)
		want := map[string][]string{"esx-1": {"host-1"}, "esx-2": {"host-2"}}
		if !reflect.DeepEqual(got, want) {
			t.Fatalf("got %v, want %v", got, want)
		}
		if s.inventory != 0 {
			t.Fatal("host inventory must not be read while PodVM v2 is disabled")
		}
	})
	t.Run("PodVM v2 enabled expands cluster-backed Nodes", func(t *testing.T) {
		s := &ownershipStub{podVMV2: true, hostToNode: map[string]string{
			"host-3": "virtual", "host-1": "virtual", "host-2": "esx-2",
		}}
		got := GetNodeNameToHostMoIDsMap(ctx, s)
		want := map[string][]string{"virtual": {"host-1", "host-3"}, "esx-2": {"host-2"}}
		if !reflect.DeepEqual(got, want) {
			t.Fatalf("got %v, want %v", got, want)
		}
	})
	t.Run("empty inputs", func(t *testing.T) {
		for _, enabled := range []bool{false, true} {
			if got := GetNodeNameToHostMoIDsMap(ctx, &ownershipStub{podVMV2: enabled}); len(got) != 0 {
				t.Fatalf("podVMV2=%t: got %v, want empty", enabled, got)
			}
		}
	})
}
