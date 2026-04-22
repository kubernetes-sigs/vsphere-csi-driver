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

package node

import (
	"context"
	"errors"
	"testing"

	v1 "k8s.io/api/core/v1"
	storagev1 "k8s.io/api/storage/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	clientset "k8s.io/client-go/kubernetes"

	cnsvsphere "sigs.k8s.io/vsphere-csi-driver/v3/pkg/common/cns-lib/vsphere"
	csitypes "sigs.k8s.io/vsphere-csi-driver/v3/pkg/csi/types"
)

// stubNodeManager implements Manager for CSINode handler tests.
type stubNodeManager struct {
	registerErr error
	k8sNode     *v1.Node
	k8sErr      error
}

func (s *stubNodeManager) SetKubernetesClient(client clientset.Interface) {}

func (s *stubNodeManager) RegisterNode(ctx context.Context, nodeUUID string, nodeName string) error {
	return s.registerErr
}

func (s *stubNodeManager) DiscoverNode(ctx context.Context, nodeUUID string) error {
	return nil
}

func (s *stubNodeManager) GetK8sNode(ctx context.Context, nodename string) (*v1.Node, error) {
	return s.k8sNode, s.k8sErr
}

func (s *stubNodeManager) GetNodeVMAndUpdateCache(ctx context.Context,
	nodeUUID string, dc *cnsvsphere.Datacenter) (*cnsvsphere.VirtualMachine, error) {
	return nil, ErrNodeNotFound
}

func (s *stubNodeManager) GetNodeVMByUuid(ctx context.Context,
	nodeUUID string) (*cnsvsphere.VirtualMachine, error) {
	return nil, ErrNodeNotFound
}

func (s *stubNodeManager) GetNodeVMByNameAndUpdateCache(ctx context.Context,
	nodeName string) (*cnsvsphere.VirtualMachine, error) {
	return nil, ErrNodeNotFound
}

func (s *stubNodeManager) GetNodeVMByNameOrUUID(
	ctx context.Context, nodeNameOrUUID string) (*cnsvsphere.VirtualMachine, error) {
	return nil, ErrNodeNotFound
}

func (s *stubNodeManager) GetNodeNameByUUID(ctx context.Context, nodeUUID string) (string, error) {
	return "", ErrNodeNotFound
}

func (s *stubNodeManager) GetAllNodes(ctx context.Context) ([]*cnsvsphere.VirtualMachine, error) {
	return nil, nil
}

func (s *stubNodeManager) GetAllNodesByVC(ctx context.Context, vcHost string) ([]*cnsvsphere.VirtualMachine, error) {
	return nil, nil
}

func (s *stubNodeManager) UnregisterNode(ctx context.Context, nodeName string) error {
	return nil
}

func (s *stubNodeManager) UnregisterAllNodes(ctx context.Context) error {
	return nil
}

func csiNodeWithNodeID(name, nodeID string) *storagev1.CSINode {
	return &storagev1.CSINode{
		ObjectMeta: metav1.ObjectMeta{Name: name},
		Spec: storagev1.CSINodeSpec{
			Drivers: []storagev1.CSINodeDriver{
				{Name: csitypes.Name, NodeID: nodeID},
			},
		},
	}
}

func TestCsiNodeAdd_ExitsAfterRegisterNodeAndProviderIDFallbackFail(t *testing.T) {
	regErr := errors.New("simulated vcenter/dns failure")
	stub := &stubNodeManager{
		registerErr: regErr,
		k8sNode: &v1.Node{
			ObjectMeta: metav1.ObjectMeta{Name: "worker-1"},
			Spec: v1.NodeSpec{
				ProviderID: "vsphere://fallback-uuid-1234",
			},
		},
	}
	nodes := &Nodes{cnsNodeManager: stub}

	var exitCode int
	exitCalled := false
	prevExit := osExit
	osExit = func(code int) {
		exitCalled = true
		exitCode = code
	}
	defer func() { osExit = prevExit }()

	nodes.csiNodeAdd(csiNodeWithNodeID("worker-1", "csinode-uuid-1"))

	if !exitCalled {
		t.Fatal("expected process exit after failed RegisterNode (including provider ID fallback)")
	}
	if exitCode != 1 {
		t.Fatalf("expected exit code 1, got %d", exitCode)
	}
}

func TestCsiNodeAdd_DoesNotExitWhenGetK8sNodeFailsAfterRegisterFail(t *testing.T) {
	stub := &stubNodeManager{
		registerErr: errors.New("discover failed"),
		k8sErr:      errors.New("apiserver unavailable"),
	}
	nodes := &Nodes{cnsNodeManager: stub}

	exitCalled := false
	prevExit := osExit
	osExit = func(code int) {
		exitCalled = true
	}
	defer func() { osExit = prevExit }()

	nodes.csiNodeAdd(csiNodeWithNodeID("worker-2", "uuid-2"))

	if exitCalled {
		t.Fatal("did not expect process exit when GetK8sNode fails after RegisterNode failure")
	}
}

func TestCsiNodeAdd_DoesNotExitWhenRegisterNodeSucceeds(t *testing.T) {
	stub := &stubNodeManager{registerErr: nil}
	nodes := &Nodes{cnsNodeManager: stub}

	exitCalled := false
	prevExit := osExit
	osExit = func(code int) {
		exitCalled = true
	}
	defer func() { osExit = prevExit }()

	nodes.csiNodeAdd(csiNodeWithNodeID("worker-ok", "uuid-ok"))

	if exitCalled {
		t.Fatal("did not expect process exit when RegisterNode succeeds on CSINode add")
	}
}

func TestCsiNodeUpdate_ExitsWhenRegisterNodeFails(t *testing.T) {
	stub := &stubNodeManager{registerErr: errors.New("register on uuid change failed")}
	nodes := &Nodes{cnsNodeManager: stub}

	var exitCode int
	exitCalled := false
	prevExit := osExit
	osExit = func(code int) {
		exitCalled = true
		exitCode = code
	}
	defer func() { osExit = prevExit }()

	oldNode := csiNodeWithNodeID("worker-3", "")
	newNode := csiNodeWithNodeID("worker-3", "new-uuid-abc")
	nodes.csiNodeUpdate(oldNode, newNode)

	if !exitCalled {
		t.Fatal("expected process exit when RegisterNode fails on CSINode UUID update")
	}
	if exitCode != 1 {
		t.Fatalf("expected exit code 1, got %d", exitCode)
	}
}

func TestCsiNodeUpdate_DoesNotExitWhenRegisterNodeSucceeds(t *testing.T) {
	stub := &stubNodeManager{registerErr: nil}
	nodes := &Nodes{cnsNodeManager: stub}

	exitCalled := false
	prevExit := osExit
	osExit = func(code int) {
		exitCalled = true
	}
	defer func() { osExit = prevExit }()

	oldNode := csiNodeWithNodeID("worker-ok", "")
	newNode := csiNodeWithNodeID("worker-ok", "fresh-uuid")
	nodes.csiNodeUpdate(oldNode, newNode)

	if exitCalled {
		t.Fatal("did not expect process exit when RegisterNode succeeds on CSINode UUID update")
	}
}
