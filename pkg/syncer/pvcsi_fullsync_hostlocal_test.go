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
	"reflect"
	"testing"

	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	clientset "k8s.io/client-go/kubernetes"
	k8sfake "k8s.io/client-go/kubernetes/fake"

	cnsconfig "sigs.k8s.io/vsphere-csi-driver/v3/pkg/common/config"
	"sigs.k8s.io/vsphere-csi-driver/v3/pkg/csi/service/common"
)

const (
	hostLocalFullSyncZoneValue = "az1"
	hostLocalFullSyncHostValue = "lvn-dvm-10-161-28-26.dvm.lvn.broadcom.net"
)

func TestTranslateTopologyHostKey_Syncer(t *testing.T) {
	tests := []struct {
		name        string
		segments    map[string]string
		fromKey     string
		toKey       string
		keepHostKey bool
		want        map[string]string
	}{
		{
			name: "key present, keepHostKey true renames the key",
			segments: map[string]string{
				v1.LabelHostname:              hostLocalFullSyncHostValue,
				"topology.kubernetes.io/zone": hostLocalFullSyncZoneValue,
			},
			fromKey:     v1.LabelHostname,
			toKey:       common.GuestClusterTopologyLabelHost,
			keepHostKey: true,
			want: map[string]string{
				common.GuestClusterTopologyLabelHost: hostLocalFullSyncHostValue,
				"topology.kubernetes.io/zone":        hostLocalFullSyncZoneValue,
			},
		},
		{
			name: "key present, keepHostKey false drops the key",
			segments: map[string]string{
				v1.LabelHostname:              hostLocalFullSyncHostValue,
				"topology.kubernetes.io/zone": hostLocalFullSyncZoneValue,
			},
			fromKey:     v1.LabelHostname,
			toKey:       common.GuestClusterTopologyLabelHost,
			keepHostKey: false,
			want: map[string]string{
				"topology.kubernetes.io/zone": hostLocalFullSyncZoneValue,
			},
		},
		{
			name: "key absent is a no-op regardless of keepHostKey",
			segments: map[string]string{
				"topology.kubernetes.io/zone": hostLocalFullSyncZoneValue,
			},
			fromKey:     v1.LabelHostname,
			toKey:       common.GuestClusterTopologyLabelHost,
			keepHostKey: true,
			want: map[string]string{
				"topology.kubernetes.io/zone": hostLocalFullSyncZoneValue,
			},
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := translateTopologyHostKey(tc.segments, tc.fromKey, tc.toKey, tc.keepHostKey)
			if !reflect.DeepEqual(got, tc.want) {
				t.Fatalf("translateTopologyHostKey() = %+v, want %+v", got, tc.want)
			}
		})
	}
}

// TestAddNodeAffinityRulesOnPV_HostLocal_TranslatesHostKey verifies that when the supervisor
// PVC's csi.vsphere.volume-accessible-topology annotation carries kubernetes.io/hostname (set by
// the Supervisor CSI controller for a host-local volume), AddNodeAffinityRulesOnPV translates it
// back to the VKS host key (common.GuestClusterTopologyLabelHost) on the Guest PV's NodeAffinity.
// This translation is unconditional (not gated on the host-local-storage-support FSS): by the
// time full-sync runs, the volume's host pinning in CNS is already a committed fact, so the
// FSS/capability's current state is irrelevant here (see controller.go's CreateVolume for the
// only gate in this feature, applied at request time).
func TestAddNodeAffinityRulesOnPV_HostLocal_TranslatesHostKey(t *testing.T) {
	ctx := context.Background()

	supPVC := &v1.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "volume-1",
			Namespace: "sv-namespace",
			Annotations: map[string]string{
				common.AnnVolumeAccessibleTopology: `[{"kubernetes.io/hostname":"` + hostLocalFullSyncHostValue +
					`","topology.kubernetes.io/zone":"` + hostLocalFullSyncZoneValue + `"}]`,
			},
		},
	}
	guestPV := &v1.PersistentVolume{
		ObjectMeta: metav1.ObjectMeta{
			Name: "pv-1",
		},
		Spec: v1.PersistentVolumeSpec{
			PersistentVolumeSource: v1.PersistentVolumeSource{
				CSI: &v1.CSIPersistentVolumeSource{
					VolumeHandle: "volume-1",
				},
			},
		},
	}

	supervisorClient := k8sfake.NewClientset(supPVC)
	guestClient := k8sfake.NewClientset(guestPV)

	metadataSyncer := &metadataSyncInformer{
		supervisorClient: supervisorClient,
	}
	metadataSyncer.configInfo = &cnsconfig.ConfigurationInfo{
		Cfg: &cnsconfig.Config{
			GC: cnsconfig.GCConfig{
				Endpoint: "endpoint",
				Port:     "443",
			},
		},
	}

	origK8sClient := k8sNewClient
	defer func() { k8sNewClient = origK8sClient }()
	k8sNewClient = func(ctx context.Context) (clientset.Interface, error) {
		return guestClient, nil
	}

	origGetPVs := getPVsInBoundAvailableOrReleased
	defer func() { getPVsInBoundAvailableOrReleased = origGetPVs }()
	getPVsInBoundAvailableOrReleased = func(ctx context.Context,
		syncer *metadataSyncInformer) ([]*v1.PersistentVolume, error) {
		return []*v1.PersistentVolume{guestPV}, nil
	}

	origGetSuperNS := cnsconfigGetSupervisorNamespace
	defer func() { cnsconfigGetSupervisorNamespace = origGetSuperNS }()
	cnsconfigGetSupervisorNamespace = func(ctx context.Context) (string, error) {
		return "sv-namespace", nil
	}

	AddNodeAffinityRulesOnPV(ctx, metadataSyncer)

	gotPV, err := guestClient.CoreV1().PersistentVolumes().Get(ctx, "pv-1", metav1.GetOptions{})
	if err != nil {
		t.Fatalf("PV not found: %v", err)
	}
	if gotPV.Spec.NodeAffinity == nil {
		t.Fatalf("expected node affinity to be set on PV")
	}
	gotKeys := map[string]string{}
	for _, term := range gotPV.Spec.NodeAffinity.Required.NodeSelectorTerms {
		for _, expr := range term.MatchExpressions {
			if len(expr.Values) != 1 {
				t.Fatalf("expected exactly one value for key %q, got %+v", expr.Key, expr.Values)
			}
			gotKeys[expr.Key] = expr.Values[0]
		}
	}
	want := map[string]string{
		common.GuestClusterTopologyLabelHost: hostLocalFullSyncHostValue,
		"topology.kubernetes.io/zone":        hostLocalFullSyncZoneValue,
	}
	if !reflect.DeepEqual(gotKeys, want) {
		t.Fatalf("PV NodeAffinity keys/values = %+v, want %+v", gotKeys, want)
	}
	if _, hasHostname := gotKeys[v1.LabelHostname]; hasHostname {
		t.Fatalf("expected kubernetes.io/hostname to be translated away, but it is still present: %+v", gotKeys)
	}
}
