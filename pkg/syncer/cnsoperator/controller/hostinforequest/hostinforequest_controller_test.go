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

package hostinforequest

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes/fake"
	"k8s.io/client-go/kubernetes/scheme"
	"k8s.io/client-go/tools/record"
	ctrlfake "sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	hostinforequestv1alpha1 "sigs.k8s.io/vsphere-csi-driver/v3/pkg/internalapis/hostinforequest/v1alpha1"
)

func TestHostInfoRequestReconcile(t *testing.T) {
	const (
		testNamespace = "test-supervisor-namespace"
		testName      = "test-guest-node"
		testBufSize   = 1024
	)

	matchingNode := &corev1.Node{
		ObjectMeta: metav1.ObjectMeta{
			Name:        "dhcp-10-144-64-66.dvm.lvn.broadcom.net",
			Annotations: map[string]string{"vmware-system-esxi-node-moid": "host-37"},
		},
		Status: corev1.NodeStatus{
			Addresses: []corev1.NodeAddress{
				{Type: corev1.NodeInternalIP, Address: "10.144.64.66"},
			},
		},
	}

	tests := []struct {
		name               string
		nodeIP             string
		nodes              []runtime.Object
		expectedStatus     hostinforequestv1alpha1.CRDStatus
		expectedHostname   string
		expectedMoid       string
		expectRequeueAfter bool
	}{
		{
			name:             "resolves successfully when Node matches",
			nodeIP:           "10.144.64.66",
			nodes:            []runtime.Object{matchingNode},
			expectedStatus:   hostinforequestv1alpha1.HostInfoRequestSuccess,
			expectedHostname: "dhcp-10-144-64-66.dvm.lvn.broadcom.net",
			expectedMoid:     "host-37",
		},
		{
			name:               "sets Error and requests requeue when no Node matches",
			nodeIP:             "1.2.3.4",
			nodes:              []runtime.Object{matchingNode},
			expectedStatus:     hostinforequestv1alpha1.HostInfoRequestError,
			expectRequeueAfter: true,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			instance := &hostinforequestv1alpha1.HostInfoRequest{
				ObjectMeta: metav1.ObjectMeta{
					Name:      testName,
					Namespace: testNamespace,
				},
				Spec: hostinforequestv1alpha1.HostInfoRequestSpec{
					NodeIP: test.nodeIP,
				},
			}

			s := scheme.Scheme
			s.AddKnownTypes(hostinforequestv1alpha1.SchemeGroupVersion, instance)
			fakeClient := ctrlfake.NewClientBuilder().
				WithScheme(s).
				WithStatusSubresource(&hostinforequestv1alpha1.HostInfoRequest{}).
				WithRuntimeObjects(instance).
				Build()

			k8sClientset := fake.NewSimpleClientset(test.nodes...)

			r := &ReconcileHostInfoRequest{
				client:       fakeClient,
				recorder:     record.NewFakeRecorder(testBufSize),
				k8sClientset: k8sClientset,
			}
			backOffDuration = make(map[types.NamespacedName]time.Duration)

			req := reconcile.Request{
				NamespacedName: types.NamespacedName{Name: testName, Namespace: testNamespace},
			}
			res, err := r.Reconcile(context.TODO(), req)
			assert.NoError(t, err)
			assert.Equal(t, test.expectRequeueAfter, res.RequeueAfter > 0)

			updated := &hostinforequestv1alpha1.HostInfoRequest{}
			assert.NoError(t, fakeClient.Get(context.TODO(), req.NamespacedName, updated))
			assert.Equal(t, test.expectedStatus, updated.Status.Status)
			assert.Equal(t, test.expectedHostname, updated.Status.Hostname)
			assert.Equal(t, test.expectedMoid, updated.Status.EsxiHostMoid)
		})
	}
}
