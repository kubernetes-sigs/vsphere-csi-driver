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

package wcpguest

import (
	"context"
	"reflect"
	"testing"

	"github.com/container-storage-interface/spec/lib/go/csi"
	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"sigs.k8s.io/vsphere-csi-driver/v3/pkg/common/unittestcommon"
	"sigs.k8s.io/vsphere-csi-driver/v3/pkg/csi/service/common"
	"sigs.k8s.io/vsphere-csi-driver/v3/pkg/csi/service/common/commonco"
)

// Tests in this file cover PVCSI host-local behavior when HostLocalStorage and PodVM v2 are
// both enabled. controller_hostlocal_test.go covers HostLocalStorage alone.

const podVMV2HostMoID = "host-45"

// newHostLocalFeatureCO returns a fake CO with the HostLocalStorage PVCSI FSS enabled and,
// when podVMV2 is true, the PodVM v2 PVCSI FSS enabled too. The fake CO reports a PVCSI FSS
// and its Supervisor capability as one switch.
func newHostLocalFeatureCO(t *testing.T, podVMV2 bool) commonco.COCommonInterface {
	t.Helper()
	co, err := unittestcommon.GetFakeContainerOrchestratorInterface(common.Kubernetes)
	if err != nil {
		t.Fatalf("failed to build fake CO: %v", err)
	}
	if err := co.EnableFSS(context.Background(), common.HostLocalStorageSupportFSS); err != nil {
		t.Fatalf("failed to enable %s: %v", common.HostLocalStorageSupportFSS, err)
	}
	if podVMV2 {
		if err := co.EnableFSS(context.Background(), common.PodVMV2SupportFSS); err != nil {
			t.Fatalf("failed to enable %s: %v", common.PodVMV2SupportFSS, err)
		}
	} else if err := co.DisableFSS(context.Background(), common.PodVMV2SupportFSS); err != nil {
		t.Fatalf("failed to disable %s: %v", common.PodVMV2SupportFSS, err)
	}
	return co
}

// runCreateVolumeAndBindSupervisorPVCWithAnnotations binds the Supervisor PVC with the given
// annotations, as the Supervisor provisioner would, and returns the CreateVolume result.
func runCreateVolumeAndBindSupervisorPVCWithAnnotations(t *testing.T, c *controller,
	req *csi.CreateVolumeRequest, annotations map[string]string) (*csi.CreateVolumeResponse, error) {
	t.Helper()
	respCh := make(chan *csi.CreateVolumeResponse, 1)
	errCh := make(chan error, 1)
	go func() {
		resp, err := c.CreateVolume(context.Background(), req)
		respCh <- resp
		errCh <- err
	}()
	pvc, earlyResp, earlyErr := waitForSupervisorPVCCreated(t, c, respCh, errCh)
	if pvc == nil {
		return earlyResp, earlyErr
	}
	if pvc.Annotations == nil {
		pvc.Annotations = map[string]string{}
	}
	for key, value := range annotations {
		pvc.Annotations[key] = value
	}
	pvc.Status.Phase = v1.ClaimBound
	if _, err := c.supervisorClient.CoreV1().PersistentVolumeClaims(c.supervisorNamespace).Update(
		context.Background(), pvc, metav1.UpdateOptions{}); err != nil {
		t.Fatalf("failed to mark supervisor PVC bound: %v", err)
	}
	return <-respCh, <-errCh
}

// TestCreateVolume_HostLocalRequestedTopologyByFeature checks the Supervisor requested-topology
// annotation for a guest host-local request:
//   - HostLocalStorage only: the VKS host key is renamed to kubernetes.io/hostname;
//   - HostLocalStorage + PodVM v2: the VKS host key (host MoID) is forwarded unchanged.
func TestCreateVolume_HostLocalRequestedTopologyByFeature(t *testing.T) {
	original := commonco.ContainerOrchestratorUtility
	t.Cleanup(func() { commonco.ContainerOrchestratorUtility = original })
	for _, tc := range []struct {
		name    string
		podVMV2 bool
		wantKey string
	}{
		{name: "HostLocalStorage_only", podVMV2: false, wantKey: v1.LabelHostname},
		{name: "HostLocalStorage_and_PodVMV2", podVMV2: true, wantKey: common.GuestClusterTopologyLabelHost},
	} {
		t.Run(tc.name, func(t *testing.T) {
			c := newFVSCreateController(t)
			commonco.ContainerOrchestratorUtility = newHostLocalFeatureCO(t, tc.podVMV2)
			seedHostLocalWorkerNode(t, c)

			req := hostLocalCreateRequest(hostLocalAccessMode, hostLocalZoneValue, hostLocalHostValue)
			pvc, _, err := runCreateVolumeAndBindSupervisorPVC(t, c, req)
			if err != nil {
				t.Fatalf("CreateVolume failed: %v", err)
			}
			expectSupervisorAnnotation(t, pvc, hostLocalWantAnnotation(tc.wantKey))
		})
	}
}

// TestCreateVolume_HostLocalAccessibleTopologyByFeature checks the guest accessible topology
// built from the bound Supervisor PVC:
//   - a HostLocalStorage volume (zone + hostname, no host mirror) returns the hostname under
//     the VKS host key;
//   - a HostLocalStorage + PodVM v2 volume (zone only plus the host mirror) returns the host
//     MoID under the VKS host key.
func TestCreateVolume_HostLocalAccessibleTopologyByFeature(t *testing.T) {
	original := commonco.ContainerOrchestratorUtility
	t.Cleanup(func() { commonco.ContainerOrchestratorUtility = original })
	for _, tc := range []struct {
		name        string
		podVMV2     bool
		annotations map[string]string
		wantHost    string
	}{
		{
			name:    "HostLocalStorage_only",
			podVMV2: false,
			annotations: map[string]string{
				common.AnnVolumeAccessibleTopology: hostLocalWantAnnotation(v1.LabelHostname),
			},
			wantHost: hostLocalHostValue,
		},
		{
			name:    "HostLocalStorage_and_PodVMV2",
			podVMV2: true,
			annotations: map[string]string{
				common.AnnVolumeAccessibleTopology: `[{"topology.kubernetes.io/zone":"` + hostLocalZoneValue + `"}]`,
				common.AttributeESXiHostMoID:       podVMV2HostMoID,
			},
			wantHost: podVMV2HostMoID,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			c := newFVSCreateController(t)
			commonco.ContainerOrchestratorUtility = newHostLocalFeatureCO(t, tc.podVMV2)
			seedHostLocalWorkerNode(t, c)

			req := hostLocalCreateRequest(hostLocalAccessMode, hostLocalZoneValue, hostLocalHostValue)
			resp, err := runCreateVolumeAndBindSupervisorPVCWithAnnotations(t, c, req, tc.annotations)
			if err != nil {
				t.Fatalf("CreateVolume failed: %v", err)
			}
			if len(resp.Volume.AccessibleTopology) != 1 {
				t.Fatalf("expected one accessible topology segment, got %+v", resp.Volume.AccessibleTopology)
			}
			want := map[string]string{
				common.GuestClusterTopologyLabelHost: tc.wantHost,
				v1.LabelTopologyZone:                 hostLocalZoneValue,
			}
			if got := resp.Volume.AccessibleTopology[0].Segments; !reflect.DeepEqual(got, want) {
				t.Fatalf("AccessibleTopology segments = %+v, want %+v", got, want)
			}
		})
	}
}

// TestGenerateVolumeAccessibleTopologyFromPVCAnnotationByFeature checks that only a Supervisor
// PVC carrying the HostLocalStorage + PodVM v2 host mirror is converted to the host MoID;
// every other PVC is returned as on master.
func TestGenerateVolumeAccessibleTopologyFromPVCAnnotationByFeature(t *testing.T) {
	pvc := func(annotations map[string]string) *v1.PersistentVolumeClaim {
		return &v1.PersistentVolumeClaim{ObjectMeta: metav1.ObjectMeta{
			Name: "pvc", Namespace: "ns", Annotations: annotations}}
	}
	t.Run("HostLocalStorage_volume_returned_unchanged", func(t *testing.T) {
		got, err := generateVolumeAccessibleTopologyFromPVCAnnotation(pvc(map[string]string{
			common.AnnVolumeAccessibleTopology: `[{"kubernetes.io/hostname":"esx-1","topology.kubernetes.io/zone":"az1"}]`,
		}))
		if err != nil {
			t.Fatal(err)
		}
		want := []map[string]string{{v1.LabelHostname: "esx-1", v1.LabelTopologyZone: "az1"}}
		if !reflect.DeepEqual(got, want) {
			t.Fatalf("got %+v, want %+v", got, want)
		}
	})
	t.Run("HostLocalStorage_and_PodVMV2_volume_uses_host_mirror", func(t *testing.T) {
		got, err := generateVolumeAccessibleTopologyFromPVCAnnotation(pvc(map[string]string{
			common.AnnVolumeAccessibleTopology: `[{"topology.kubernetes.io/zone":"az1"}]`,
			common.AttributeESXiHostMoID:       podVMV2HostMoID,
		}))
		if err != nil {
			t.Fatal(err)
		}
		want := []map[string]string{{common.GuestClusterTopologyLabelHost: podVMV2HostMoID, v1.LabelTopologyZone: "az1"}}
		if !reflect.DeepEqual(got, want) {
			t.Fatalf("got %+v, want %+v", got, want)
		}
	})
	t.Run("missing_annotation_errors_as_on_master", func(t *testing.T) {
		if _, err := generateVolumeAccessibleTopologyFromPVCAnnotation(pvc(nil)); err == nil {
			t.Fatal("expected an error for a PVC without accessible topology")
		}
	})
}
