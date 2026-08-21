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
	"testing"

	snapshotv1 "github.com/kubernetes-csi/external-snapshotter/client/v8/apis/volumesnapshot/v1"
	"github.com/stretchr/testify/assert"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	k8sfake "k8s.io/client-go/kubernetes/fake"

	"sigs.k8s.io/vsphere-csi-driver/v3/pkg/common/fakesnapshot"
)

// TestGetVolumeSnapshotPVCSource covers how GetVolumeSnapshotPVCSource resolves (or
// declines to resolve) the source PVC behind a VolumeSnapshot.
//
// VolumeSnapshotSource is a union: exactly one of PersistentVolumeClaimName (dynamic
// snapshot) or VolumeSnapshotContentName (pre-provisioned snapshot) is set. The
// pre-provisioned case must return an error rather than dereferencing the nil
// PersistentVolumeClaimName pointer, since the CNS-CSI mutating webhook calls this for
// every VolumeSnapshot-backed PVC and a panic there fails the admission request.
func TestGetVolumeSnapshotPVCSource(t *testing.T) {
	const namespace = "test-ns"

	sourcePVC := &corev1.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "src-pvc",
			Namespace: namespace,
		},
	}

	// A dynamic snapshot: taken from a PVC that lives in this cluster.
	dynamicSnapshot := &snapshotv1.VolumeSnapshot{
		ObjectMeta: metav1.ObjectMeta{Name: "dynamic-snap", Namespace: namespace},
		Spec: snapshotv1.VolumeSnapshotSpec{
			Source: snapshotv1.VolumeSnapshotSource{
				PersistentVolumeClaimName: &[]string{"src-pvc"}[0],
			},
		},
	}
	// A pre-provisioned snapshot: bound to a VolumeSnapshotContent, so there is no
	// source PVC and PersistentVolumeClaimName is nil.
	preProvisionedSnapshot := &snapshotv1.VolumeSnapshot{
		ObjectMeta: metav1.ObjectMeta{Name: "preprovisioned-snap", Namespace: namespace},
		Spec: snapshotv1.VolumeSnapshotSpec{
			Source: snapshotv1.VolumeSnapshotSource{
				VolumeSnapshotContentName: &[]string{"snapcontent-1"}[0],
			},
		},
	}
	// Defensive: an explicitly empty source PVC name is as unusable as a nil one.
	emptyNameSnapshot := &snapshotv1.VolumeSnapshot{
		ObjectMeta: metav1.ObjectMeta{Name: "empty-name-snap", Namespace: namespace},
		Spec: snapshotv1.VolumeSnapshotSpec{
			Source: snapshotv1.VolumeSnapshotSource{
				PersistentVolumeClaimName: &[]string{""}[0],
			},
		},
	}

	orchestrator := &K8sOrchestrator{
		k8sClient: k8sfake.NewSimpleClientset(sourcePVC),
		snapshotterClient: fakesnapshot.NewClientset(
			dynamicSnapshot, preProvisionedSnapshot, emptyNameSnapshot),
	}

	tests := []struct {
		name              string
		snapshotNamespace string
		snapshotName      string
		expectErr         bool
		expectPVCName     string
	}{
		{
			name:              "dynamic snapshot resolves its source PVC",
			snapshotNamespace: namespace,
			snapshotName:      "dynamic-snap",
			expectPVCName:     "src-pvc",
		},
		{
			name:              "pre-provisioned snapshot errors instead of panicking",
			snapshotNamespace: namespace,
			snapshotName:      "preprovisioned-snap",
			expectErr:         true,
		},
		{
			name:              "empty source PVC name errors instead of a bogus lookup",
			snapshotNamespace: namespace,
			snapshotName:      "empty-name-snap",
			expectErr:         true,
		},
		{
			name:              "missing snapshot errors",
			snapshotNamespace: namespace,
			snapshotName:      "does-not-exist",
			expectErr:         true,
		},
		{
			name:              "empty snapshot name errors",
			snapshotNamespace: namespace,
			snapshotName:      "",
			expectErr:         true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// The call must never panic, whatever shape the snapshot source has.
			assert.NotPanics(t, func() {
				pvc, err := orchestrator.GetVolumeSnapshotPVCSource(ctx, tt.snapshotNamespace, tt.snapshotName)
				if tt.expectErr {
					assert.Error(t, err)
					assert.Nil(t, pvc)
					return
				}
				assert.NoError(t, err)
				assert.NotNil(t, pvc)
				assert.Equal(t, tt.expectPVCName, pvc.Name)
			})
		})
	}
}
