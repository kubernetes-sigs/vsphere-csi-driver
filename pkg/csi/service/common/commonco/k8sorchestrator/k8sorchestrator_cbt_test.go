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
	"testing"

	snapshotv1 "github.com/kubernetes-csi/external-snapshotter/client/v8/apis/volumesnapshot/v1"
	fakesnapshot "github.com/kubernetes-csi/external-snapshotter/client/v8/clientset/versioned/fake"
	"github.com/stretchr/testify/assert"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

func TestGetVolumeSnapshotChangeIDBySnapshotID(t *testing.T) {
	ctx := context.Background()

	tests := []struct {
		name          string
		snapshotID    string
		vsc           *snapshotv1.VolumeSnapshotContent
		vs            *snapshotv1.VolumeSnapshot
		nilClient     bool
		expectedID    string
		expectedError string
	}{
		{
			name:       "success",
			snapshotID: "test-snapshot-handle",
			vsc: &snapshotv1.VolumeSnapshotContent{
				ObjectMeta: metav1.ObjectMeta{
					Name: "test-vsc",
				},
				Spec: snapshotv1.VolumeSnapshotContentSpec{
					VolumeSnapshotRef: corev1.ObjectReference{
						Name:      "test-vs",
						Namespace: "default",
					},
				},
				Status: &snapshotv1.VolumeSnapshotContentStatus{
					SnapshotHandle: func() *string { s := "test-snapshot-handle"; return &s }(),
				},
			},
			vs: &snapshotv1.VolumeSnapshot{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "test-vs",
					Namespace: "default",
					Annotations: map[string]string{
						"csi.vsphere.volume/change-id": "test-change-id/1",
					},
				},
			},
			expectedID:    "test-change-id/1",
			expectedError: "",
		},
		{
			name:          "snapshotterClient is nil",
			snapshotID:    "test-snapshot-handle",
			vsc:           nil,
			vs:            nil,
			nilClient:     true,
			expectedID:    "",
			expectedError: "snapshotterClient is not initialized",
		},
		{
			name:       "vsc empty ref name",
			snapshotID: "test-snapshot-handle",
			vsc: &snapshotv1.VolumeSnapshotContent{
				ObjectMeta: metav1.ObjectMeta{
					Name: "test-vsc",
				},
				Spec: snapshotv1.VolumeSnapshotContentSpec{
					VolumeSnapshotRef: corev1.ObjectReference{
						Namespace: "default",
					},
				},
				Status: &snapshotv1.VolumeSnapshotContentStatus{
					SnapshotHandle: func() *string { s := "test-snapshot-handle"; return &s }(),
				},
			},
			vs:            nil,
			expectedID:    "",
			expectedError: "VolumeSnapshotRef is missing in VolumeSnapshotContent test-vsc",
		},
		{
			name:       "vs get error",
			snapshotID: "test-snapshot-handle",
			vsc: &snapshotv1.VolumeSnapshotContent{
				ObjectMeta: metav1.ObjectMeta{
					Name: "test-vsc",
				},
				Spec: snapshotv1.VolumeSnapshotContentSpec{
					VolumeSnapshotRef: corev1.ObjectReference{
						Name:      "test-vs",
						Namespace: "default",
					},
				},
				Status: &snapshotv1.VolumeSnapshotContentStatus{
					SnapshotHandle: func() *string { s := "test-snapshot-handle"; return &s }(),
				},
			},
			vs:            nil, // vs doesn't exist, will cause Get() to fail
			expectedID:    "",
			expectedError: "failed to get VolumeSnapshot default/test-vs",
		},
		{
			name:       "vsc not found",
			snapshotID: "non-existent-handle",
			vsc: &snapshotv1.VolumeSnapshotContent{
				ObjectMeta: metav1.ObjectMeta{
					Name: "test-vsc",
				},
				Spec: snapshotv1.VolumeSnapshotContentSpec{
					VolumeSnapshotRef: corev1.ObjectReference{
						Name:      "test-vs",
						Namespace: "default",
					},
				},
				Status: &snapshotv1.VolumeSnapshotContentStatus{
					SnapshotHandle: func() *string { s := "test-snapshot-handle"; return &s }(),
				},
			},
			vs:            nil,
			expectedID:    "",
			expectedError: "VolumeSnapshotContent not found for SnapshotHandle",
		},
		{
			name:       "annotation missing",
			snapshotID: "test-snapshot-handle",
			vsc: &snapshotv1.VolumeSnapshotContent{
				ObjectMeta: metav1.ObjectMeta{
					Name: "test-vsc",
				},
				Spec: snapshotv1.VolumeSnapshotContentSpec{
					VolumeSnapshotRef: corev1.ObjectReference{
						Name:      "test-vs",
						Namespace: "default",
					},
				},
				Status: &snapshotv1.VolumeSnapshotContentStatus{
					SnapshotHandle: func() *string { s := "test-snapshot-handle"; return &s }(),
				},
			},
			vs: &snapshotv1.VolumeSnapshot{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "test-vs",
					Namespace: "default",
					Annotations: map[string]string{
						"other-annotation": "value",
					},
				},
			},
			expectedID:    "",
			expectedError: "annotation csi.vsphere.volume/change-id not found",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			fakeClient := fakesnapshot.NewSimpleClientset()
			if tt.vsc != nil {
				_, err := fakeClient.SnapshotV1().VolumeSnapshotContents().Create(ctx, tt.vsc, metav1.CreateOptions{})
				assert.NoError(t, err)
			}
			if tt.vs != nil {
				_, err := fakeClient.SnapshotV1().VolumeSnapshots(tt.vs.Namespace).Create(ctx, tt.vs, metav1.CreateOptions{})
				assert.NoError(t, err)
			}

			k8sOrch := &K8sOrchestrator{
				snapshotterClient: fakeClient,
			}
			if tt.nilClient {
				k8sOrch.snapshotterClient = nil
			}

			changeID, err := k8sOrch.GetVolumeSnapshotChangeIDBySnapshotID(ctx, tt.snapshotID)

			if tt.expectedError != "" {
				assert.Error(t, err)
				assert.Contains(t, err.Error(), tt.expectedError)
			} else {
				assert.NoError(t, err)
				assert.Equal(t, tt.expectedID, changeID)
			}
		})
	}
}
