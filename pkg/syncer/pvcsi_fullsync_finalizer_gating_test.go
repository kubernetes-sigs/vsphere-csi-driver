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

package syncer

import (
	"context"
	"fmt"
	"testing"

	snapv1 "github.com/kubernetes-csi/external-snapshotter/client/v8/apis/volumesnapshot/v1"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	restclient "k8s.io/client-go/rest"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	cnsconfig "sigs.k8s.io/vsphere-csi-driver/v3/pkg/common/config"
	"sigs.k8s.io/vsphere-csi-driver/v3/pkg/csi/service/common"
	cnsoperatortypes "sigs.k8s.io/vsphere-csi-driver/v3/pkg/syncer/cnsoperator/types"
)

// TestSetGuestClusterDetailsOnSupervisorSnapshot_FinalizerGating calls the real function with
// pre-populated snapshot clients and verifies that the guest cluster ownership label is always
// added, while the CNS snapshot finalizer is added only when addFinalizer is true (the supervisor
// removes it on guest cluster deletion). Existing finalizers are never removed by this function.
func TestSetGuestClusterDetailsOnSupervisorSnapshot_FinalizerGating(t *testing.T) {
	const (
		svNamespace = "vmware-system-csi"
		svVSName    = "sv-snap-1"
		guestVSName = "guest-snap-1"
		guestVSNs   = "guest-ns"
		vscName     = "snapcontent-guest-1"
		clusterName = "my-tkc"
		clusterDist = "TKGService"
		clusterUID  = "tkc-uid-abc"
	)
	labelKey := fmt.Sprintf("%s/%s", clusterName, clusterDist)

	tests := []struct {
		name               string
		addFinalizer       bool
		existingFinalizers []string
		wantFinalizers     []string
	}{
		{name: "addFinalizer true adds finalizer", addFinalizer: true,
			wantFinalizers: []string{cnsoperatortypes.CNSSnapshotFinalizer}},
		{name: "addFinalizer false adds no finalizer", addFinalizer: false, wantFinalizers: nil},
		{name: "addFinalizer true keeps other finalizers", addFinalizer: true,
			existingFinalizers: []string{"other"},
			wantFinalizers:     []string{"other", cnsoperatortypes.CNSSnapshotFinalizer}},
		{name: "addFinalizer false keeps already present CNS finalizer", addFinalizer: false,
			existingFinalizers: []string{cnsoperatortypes.CNSSnapshotFinalizer},
			wantFinalizers:     []string{cnsoperatortypes.CNSSnapshotFinalizer}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx := context.Background()

			readyToUse := true
			handle := svVSName
			guestVSC := &snapv1.VolumeSnapshotContent{
				ObjectMeta: metav1.ObjectMeta{Name: vscName},
				Spec: snapv1.VolumeSnapshotContentSpec{
					Driver:            common.VSphereCSIDriverName,
					VolumeSnapshotRef: corev1.ObjectReference{Name: guestVSName, Namespace: guestVSNs},
				},
				Status: &snapv1.VolumeSnapshotContentStatus{SnapshotHandle: &handle, ReadyToUse: &readyToUse},
			}
			svVS := &snapv1.VolumeSnapshot{ObjectMeta: metav1.ObjectMeta{
				Name: svVSName, Namespace: svNamespace, Finalizers: tt.existingFinalizers,
			}}

			scheme := runtime.NewScheme()
			require.NoError(t, snapv1.AddToScheme(scheme))
			svRuntimeClient := fake.NewClientBuilder().WithScheme(scheme).WithObjects(svVS).Build()

			cfg := &cnsconfig.Config{}
			cfg.GC.TanzuKubernetesClusterName = clusterName
			cfg.GC.ClusterDistribution = clusterDist
			cfg.GC.TanzuKubernetesClusterUID = clusterUID
			metadataSyncer := &metadataSyncInformer{
				configInfo:                  cnsconfig.NewConfigurationInfo(cfg),
				guestSnapClient:             newFakeSnapshotClientset(guestVSC),
				supervisorSnapClient:        newFakeSnapshotClientset(svVS.DeepCopy()),
				supervisorSnapRestConf:      &restclient.Config{},
				supervisorSnapRuntimeClient: svRuntimeClient,
			}

			require.NoError(t, setGuestClusterDetailsOnSupervisorSnapshot(ctx, metadataSyncer,
				svNamespace, tt.addFinalizer))

			got := &snapv1.VolumeSnapshot{}
			require.NoError(t, svRuntimeClient.Get(ctx,
				client.ObjectKey{Namespace: svNamespace, Name: svVSName}, got))
			assert.Equal(t, clusterUID, got.Labels[labelKey], "ownership label must always be added")
			assert.ElementsMatch(t, tt.wantFinalizers, got.Finalizers)
		})
	}
}
