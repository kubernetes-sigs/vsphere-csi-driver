package node

import (
	"context"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	clientset "k8s.io/client-go/kubernetes"
	"k8s.io/client-go/kubernetes/fake"
)

func TestDefaultManager_GetNodeByName(t *testing.T) {
	nodeName := "foobar.dev.lan"
	m := defaultManager{
		nodeVMs:        sync.Map{},
		nodeNameToUUID: sync.Map{},
		k8sClient:      nil,
		useNodeUuid:    false,
	}

	k8sClient := k8sClientWithNodes(nodeName)
	m.SetKubernetesClient(k8sClient)

	vm, _ := m.GetNodeByName(context.TODO(), nodeName)
	if vm != nil {
		t.Errorf("Unexpected vm found:%v", vm)
	}

	nodeUUID, ok := m.nodeNameToUUID.Load(nodeName)
	if !ok {
		t.Errorf("node name should be loaded into nodeUUID map")
	}
	assert.Equal(t, "foobar", nodeUUID)
}

func k8sClientWithNodes(nodeName string) clientset.Interface {
	node := &v1.Node{
		ObjectMeta: metav1.ObjectMeta{
			Name: nodeName,
		},
		Spec: v1.NodeSpec{
			ProviderID: "vsphere://foobar",
		},
	}
	client := fake.NewSimpleClientset(node)
	return client
}
