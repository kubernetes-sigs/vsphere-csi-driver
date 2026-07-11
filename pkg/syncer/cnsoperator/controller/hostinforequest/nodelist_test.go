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

	"github.com/stretchr/testify/assert"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes/fake"

	"sigs.k8s.io/vsphere-csi-driver/v3/pkg/csi/service/common"
)

func TestGetESXiHostnameFromNodeList(t *testing.T) {
	matchingNode := &corev1.Node{
		ObjectMeta: metav1.ObjectMeta{
			Name:        "dhcp-10-144-64-66.dvm.lvn.broadcom.net",
			Annotations: map[string]string{common.HostMoidAnnotationKey: "host-37"},
		},
		Status: corev1.NodeStatus{
			Addresses: []corev1.NodeAddress{
				{Type: corev1.NodeHostName, Address: "dhcp-10-144-64-66.dvm.lvn.broadcom.net"},
				{Type: corev1.NodeInternalIP, Address: "10.144.64.66"},
			},
		},
	}
	otherNode := &corev1.Node{
		ObjectMeta: metav1.ObjectMeta{
			Name:        "423e1874cfdde9c61ad93a30609c4feb",
			Annotations: map[string]string{common.HostMoidAnnotationKey: "host-12"},
		},
		Status: corev1.NodeStatus{
			Addresses: []corev1.NodeAddress{
				{Type: corev1.NodeInternalIP, Address: "166.168.16.0"},
			},
		},
	}

	tests := []struct {
		name             string
		nodeIP           string
		expectedHostname string
		expectedMoid     string
		expectError      bool
	}{
		{
			name:             "matching InternalIP resolves to hostname and moid",
			nodeIP:           "10.144.64.66",
			expectedHostname: "dhcp-10-144-64-66.dvm.lvn.broadcom.net",
			expectedMoid:     "host-37",
		},
		{
			name:             "second node also resolves",
			nodeIP:           "166.168.16.0",
			expectedHostname: "423e1874cfdde9c61ad93a30609c4feb",
			expectedMoid:     "host-12",
		},
		{
			name:        "no matching node returns error",
			nodeIP:      "1.2.3.4",
			expectError: true,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			k8sClient := fake.NewSimpleClientset(matchingNode, otherNode)

			hostname, moid, err := getESXiHostnameFromNodeList(context.TODO(), k8sClient, test.nodeIP)

			if test.expectError {
				assert.Error(t, err)
				return
			}
			assert.NoError(t, err)
			assert.Equal(t, test.expectedHostname, hostname)
			assert.Equal(t, test.expectedMoid, moid)
		})
	}
}
